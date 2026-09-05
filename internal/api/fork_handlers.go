/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/agentruntimepolicy"
	"github.com/orka-agents/orka/internal/events"
	forkcontext "github.com/orka-agents/orka/internal/fork"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tracing"
)

type ForkTaskRequest struct {
	AfterSeq    *int64                        `json:"afterSeq,omitempty"`
	NewTaskName string                        `json:"newTaskName,omitempty"`
	AgentRef    *corev1alpha1.AgentReference  `json:"agentRef,omitempty"`
	Prompt      string                        `json:"prompt,omitempty"`
	Workspace   *corev1alpha1.WorkspaceConfig `json:"workspace,omitempty"`
}

type ForkTaskResponse struct {
	Namespace      string              `json:"namespace"`
	SourceTaskName string              `json:"sourceTaskName"`
	NewTaskName    string              `json:"newTaskName"`
	AfterSeq       int64               `json:"afterSeq"`
	ForkContext    forkcontext.Context `json:"forkContext"`
}

// ForkTask handles POST /api/v1/tasks/{id}/fork.
func (h *Handlers) ForkTask(c fiber.Ctx) error {
	sourceName := c.Params("id")
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	access := h.taskAccess()
	if err := access.authorizeReadable(c, "forkTask", namespace, sourceName); err != nil {
		return err
	}
	if h.executionEventStore == nil {
		return fiber.NewError(fiber.StatusNotImplemented, "execution event storage not enabled")
	}

	var req ForkTaskRequest
	if len(c.Body()) > 0 {
		if err := c.Bind().JSON(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
	}

	source, err := access.loadAuthorized(c, "forkTask", namespace, sourceName)
	if err != nil {
		return err
	}

	latest, err := h.executionEventStore.GetLatestExecutionEventSeq(
		c.Context(),
		namespace,
		events.ExecutionEventStreamTypeTask,
		sourceName,
	)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get latest execution event sequence: %v", err))
	}
	afterSeq := latest
	if req.AfterSeq != nil {
		afterSeq = *req.AfterSeq
	}
	if afterSeq < 0 {
		return fiber.NewError(fiber.StatusBadRequest, "afterSeq must be non-negative")
	}
	if afterSeq > latest {
		return fiber.NewError(fiber.StatusBadRequest, "afterSeq must be 0, latest, or an existing event sequence")
	}
	validAfterSeq, err := taskEventSeqExists(c.Context(), h.executionEventStore, namespace, sourceName, afterSeq, latest)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to validate execution event sequence: %v", err))
	}
	if !validAfterSeq {
		return fiber.NewError(fiber.StatusBadRequest, "afterSeq must be 0, latest, or an existing event sequence")
	}
	eventsBefore, scanTruncated, err := listTaskEventsThrough(c.Context(), h.executionEventStore, namespace, sourceName, afterSeq)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list execution events: %v", err))
	}

	forkCtx := forkcontext.BuildContext(namespace, sourceName, afterSeq, eventsBefore, forkcontext.DefaultMaxEvents)
	forkCtx.Truncated = forkCtx.Truncated || scanTruncated
	idempotencyKey := strings.TrimSpace(c.Get("Idempotency-Key"))
	newName := strings.TrimSpace(req.NewTaskName)
	// Idempotent recovery is opt-in via an explicit Idempotency-Key. We do NOT
	// infer idempotency from (source, afterSeq, requester) alone, because that
	// tuple omits the divergent overrides (prompt/agentRef/workspace) — forking
	// one checkpoint into several distinct branches is the primary use case, so
	// the default must mint a unique name. With a key, the caller is explicitly
	// asserting "same logical fork", so retries collapse onto one object.
	idempotent := newName == "" && idempotencyKey != ""
	if newName == "" {
		if idempotent {
			newName = deterministicForkTaskName(sourceName, afterSeq, forkRequesterIdentity(c), idempotencyKey)
		} else {
			newName = generatedForkTaskName(sourceName)
		}
	}

	spec := *source.Spec.DeepCopy()
	applyForkRequestOverrides(&spec, req, namespace)
	if err := applyForkContextToSpec(&spec, forkCtx); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to encode fork context: %v", err))
	}

	forked := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newName,
			Namespace: namespace,
			Labels: map[string]string{
				labels.LabelParentTask: labels.SelectorValue(sourceName),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName:                sourceName,
				labels.AnnotationForkSourceTask:                sourceName,
				labels.AnnotationForkSourceSeq:                 strconv.FormatInt(afterSeq, 10),
				labels.AnnotationForkContextTruncated:          strconv.FormatBool(forkCtx.Truncated),
				labels.AnnotationDisableCoordinationToolInject: strconv.FormatBool(true),
			},
		},
		Spec: spec,
	}
	if err := agentruntimepolicy.ResolveAndReplaceTaskRuntimeRefAllowedTools(
		c.Context(), h.contextTokenAuthorizationReader(), forked,
	); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("invalid fork AgentRuntime policy: %v", err))
	}
	tracing.StampTaskTraceContext(c.Context(), forked)
	sourceSessionName, forkedSessionName, err := h.resolveForkSessionNames(c.Context(), namespace, source, forked)
	if err != nil {
		return err
	}
	stampTaskRequesterFromUserInfo(forked, GetUserInfo(c))
	if err := h.authorizeContextTokenTaskCreate(c, createTaskRequestFromTask(forked), namespace); err != nil {
		return err
	}
	if err := h.authorizeTaskCreate(c.Context(), c, forked); err != nil {
		return err
	}

	// Reject oversize objects BEFORE any write. The apiserver/etcd object-size
	// limit is enforced only on the real persisted write (a DryRunAll create does
	// not catch it), so guard with an explicit serialized-size budget here and
	// surface 413 rather than a generic 500 with an orphaned request event.
	if size := forkedTaskSerializedSize(forked); size > maxForkedTaskSerializedBytes {
		return fiber.NewError(fiber.StatusRequestEntityTooLarge,
			fmt.Sprintf("forked task is too large (%d bytes > %d limit); fork from an earlier checkpoint or shorten the prompt", size, maxForkedTaskSerializedBytes))
	}

	// Create the Task FIRST (it is the authoritative object). Only after a
	// confirmed-successful create do we append fork timeline events, so a failed
	// create can never orphan a TaskForkRequested/Created event (invariant 1 & 4).
	if err := h.client.Create(c.Context(), forked); err != nil {
		switch {
		case apierrors.IsAlreadyExists(err):
			// Idempotent recovery only when the caller opted in with an
			// Idempotency-Key: the existing object is the same logical fork (same
			// source + checkpoint), so return it instead of creating a duplicate.
			// Without a key (default unique auto-name, or an explicit user-supplied
			// name), a collision is a genuine conflict — we never silently alias a
			// divergent fork onto a pre-existing Task whose spec differs.
			if idempotent {
				if existing, ok := h.matchingExistingFork(c.Context(), namespace, newName, sourceName, afterSeq); ok {
					return c.Status(fiber.StatusOK).JSON(ForkTaskResponse{Namespace: namespace, SourceTaskName: sourceName, NewTaskName: existing.Name, AfterSeq: afterSeq, ForkContext: forkCtx})
				}
			}
			return fiber.NewError(fiber.StatusConflict, "forked task already exists")
		case apierrors.IsRequestEntityTooLargeError(err):
			return fiber.NewError(fiber.StatusRequestEntityTooLarge, "forked task is too large; fork from an earlier checkpoint or shorten the prompt")
		default:
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create forked task: %v", err))
		}
	}

	// Timeline events are best-effort AFTER the authoritative create. A failure
	// here must not fail the fork (the Task already exists and is self-describing
	// via its parent labels/annotations); log and continue (invariant 1).
	h.appendForkTimelineEvents(c.Context(), namespace, sourceName, newName, afterSeq, sourceSessionName, forkedSessionName)

	return c.Status(fiber.StatusCreated).JSON(ForkTaskResponse{Namespace: namespace, SourceTaskName: sourceName, NewTaskName: newName, AfterSeq: afterSeq, ForkContext: forkCtx})
}

// maxForkedTaskSerializedBytes bounds the serialized forked Task well under the
// kube-apiserver/etcd ~1.5 MiB object-size limit, leaving headroom for
// server-side managed fields and status.
const maxForkedTaskSerializedBytes = 1024 * 1024

func forkedTaskSerializedSize(task *corev1alpha1.Task) int {
	if task == nil {
		return 0
	}
	data, err := json.Marshal(task)
	if err != nil {
		// Treat an unmarshalable object as oversize (fail-closed).
		return maxForkedTaskSerializedBytes + 1
	}
	return len(data)
}

// forkRequesterIdentity returns a stable identifier for the caller used to scope
// deterministic fork names, so different users forking the same checkpoint do not
// collide. Falls back to empty when unauthenticated.
func forkRequesterIdentity(c fiber.Ctx) string {
	if info := GetUserInfo(c); info != nil {
		if u := strings.TrimSpace(info.Username); u != "" {
			return u
		}
	}
	return ""
}

// deterministicForkTaskName derives a stable fork name so retries of the same
// logical fork (identified by an explicit Idempotency-Key) resolve to the same
// object. Used only on the opt-in idempotent path.
func deterministicForkTaskName(sourceName string, afterSeq int64, requester, idempotencyKey string) string {
	seed := idempotencyKey
	if seed == "" {
		seed = fmt.Sprintf("%s\x00%d\x00%s", sourceName, afterSeq, requester)
	}
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("%s-fork-%s", forkcontext.SanitizeTaskNamePrefix(sourceName), hex.EncodeToString(sum[:4]))
}

// generatedForkTaskName mints a unique fork name (random suffix). This is the
// default for auto-named forks so that forking one checkpoint into several
// distinct branches always creates distinct Tasks; it never aliases divergent
// forks. Retry-safety for the same logical fork is opt-in via an Idempotency-Key
// (see deterministicForkTaskName); the create-before-events ordering already
// removes the transient-failure retry that previously caused duplicates.
func generatedForkTaskName(sourceName string) string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-fork-%d", forkcontext.SanitizeTaskNamePrefix(sourceName), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-fork-%s", forkcontext.SanitizeTaskNamePrefix(sourceName), hex.EncodeToString(suffix[:]))
}

// matchingExistingFork returns the existing Task at newName if it is the same
// logical fork (same source task and checkpoint seq), enabling idempotent
// recovery on retry.
func (h *Handlers) matchingExistingFork(ctx context.Context, namespace, newName, sourceName string, afterSeq int64) (*corev1alpha1.Task, bool) {
	existing := &corev1alpha1.Task{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: newName}, existing); err != nil {
		return nil, false
	}
	if existing.Annotations[labels.AnnotationForkSourceTask] != sourceName {
		return nil, false
	}
	if existing.Annotations[labels.AnnotationForkSourceSeq] != strconv.FormatInt(afterSeq, 10) {
		return nil, false
	}
	return existing, true
}

// appendForkTimelineEvents records the fork request + created events on the
// source and forked streams. It is best-effort: the authoritative Task already
// exists, so an append failure is logged but does not fail the request.
func (h *Handlers) appendForkTimelineEvents(ctx context.Context, namespace, sourceName, newName string, afterSeq int64, sourceSessionName, forkedSessionName string) {
	requestContent, err := marshalForkEventContent(sourceName, newName, afterSeq, "request")
	if err != nil {
		log.Info("fork: failed to encode request event content", "error", err, "task", sourceName)
		return
	}
	if _, err := h.executionEventStore.AppendExecutionEvent(ctx, &store.ExecutionEvent{
		Namespace:   namespace,
		StreamType:  events.ExecutionEventStreamTypeTask,
		StreamID:    sourceName,
		TaskName:    sourceName,
		SessionName: sourceSessionName,
		Type:        events.ExecutionEventTypeTaskForkRequested,
		Severity:    events.ExecutionEventSeverityInfo,
		Summary:     "task fork requested",
		Content:     requestContent,
	}); err != nil {
		log.Info("fork: failed to append request event", "error", err, "task", sourceName)
	}

	createdContent, err := marshalForkEventContent(sourceName, newName, afterSeq, "created")
	if err != nil {
		log.Info("fork: failed to encode created event content", "error", err, "task", newName)
		return
	}
	for _, event := range []struct {
		streamID    string
		sessionName string
	}{
		{streamID: sourceName, sessionName: sourceSessionName},
		{streamID: newName, sessionName: forkedSessionName},
	} {
		if _, err := h.executionEventStore.AppendExecutionEvent(ctx, &store.ExecutionEvent{
			Namespace:   namespace,
			StreamType:  events.ExecutionEventStreamTypeTask,
			StreamID:    event.streamID,
			TaskName:    event.streamID,
			SessionName: event.sessionName,
			Type:        events.ExecutionEventTypeTaskForkCreated,
			Severity:    events.ExecutionEventSeverityInfo,
			Summary:     "task fork created",
			Content:     createdContent,
		}); err != nil {
			log.Info("fork: failed to append created event", "error", err, "stream", event.streamID)
		}
	}
}

func applyForkRequestOverrides(spec *corev1alpha1.TaskSpec, req ForkTaskRequest, taskNamespace string) {
	if spec == nil {
		return
	}
	spec.RequestedBy = nil
	spec.Transaction = nil
	if req.AgentRef != nil {
		if !sameForkAgentReference(spec.AgentRef, req.AgentRef, taskNamespace) {
			spec.SessionRef = nil
		}
		spec.AgentRef = req.AgentRef
	}
	if prompt := strings.TrimSpace(req.Prompt); prompt != "" {
		spec.Prompt = prompt
		if spec.AI != nil {
			spec.AI.Prompt = prompt
		}
	}
	if req.Workspace != nil {
		spec.Workspace = req.Workspace
	}
	clearForkSchedule(spec)
}

func sameForkAgentReference(left, right *corev1alpha1.AgentReference, taskNamespace string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	normalizeNamespace := func(namespace string) string {
		namespace = strings.TrimSpace(namespace)
		if namespace == "" {
			return taskNamespace
		}
		return namespace
	}
	return strings.TrimSpace(left.Name) == strings.TrimSpace(right.Name) &&
		normalizeNamespace(left.Namespace) == normalizeNamespace(right.Namespace)
}

func clearForkSchedule(spec *corev1alpha1.TaskSpec) {
	if spec == nil {
		return
	}
	spec.Schedule = ""
	spec.TimeZone = nil
	spec.ConcurrencyPolicy = ""
	spec.StartingDeadlineSeconds = nil
	spec.SuccessfulRunsHistoryLimit = nil
	spec.FailedRunsHistoryLimit = nil
	spec.Suspend = nil
}

func applyForkContextToSpec(spec *corev1alpha1.TaskSpec, forkCtx forkcontext.Context) error {
	if spec == nil || len(forkCtx.Events) == 0 {
		return nil
	}
	encoded, err := json.Marshal(forkCtx)
	if err != nil {
		return err
	}
	contextBlock := "Fork context through execution event checkpoint:\n" + string(encoded)
	if spec.AI != nil {
		spec.AI.Prompt = joinForkContextPrompt(contextBlock, spec.AI.Prompt)
		if spec.Type == corev1alpha1.TaskTypeAI && strings.TrimSpace(spec.Prompt) == "" {
			return nil
		}
	}
	if spec.Type == corev1alpha1.TaskTypeAgent || strings.TrimSpace(spec.Prompt) != "" {
		spec.Prompt = joinForkContextPrompt(contextBlock, spec.Prompt)
	}
	return nil
}

func joinForkContextPrompt(contextBlock, prompt string) string {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return contextBlock
	}
	return contextBlock + "\n\nUser continuation prompt:\n" + prompt
}

func (h *Handlers) resolveForkSessionNames(
	ctx context.Context,
	namespace string,
	source *corev1alpha1.Task,
	forked *corev1alpha1.Task,
) (string, string, error) {
	sourceSessionName := sessionNameForTask(source)
	forkedSessionName := sessionNameForTask(forked)
	gatewayOwned, err := h.gatewayOwnedForkSource(ctx, source)
	if err != nil {
		return "", "", err
	}
	if gatewayOwned {
		detachGatewayFork(forked)
	}
	if sourceSessionName == "" || h.sessionStore == nil {
		if gatewayOwned {
			return sourceSessionName, "", nil
		}
		return sourceSessionName, forkedSessionName, nil
	}

	sessionType, err := transcriptSessionType(ctx, h.sessionStore, namespace, sourceSessionName)
	if errors.Is(err, store.ErrNotFound) {
		forked.Spec.SessionRef = nil
		if gatewayOwned {
			return sourceSessionName, "", nil
		}
		return "", "", nil
	}
	if err != nil {
		return "", "", fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get source session: %v", err))
	}
	if sessionType == store.SessionTypeGateway {
		detachGatewayFork(forked)
		return sourceSessionName, "", nil
	}
	return sourceSessionName, forkedSessionName, nil
}

func (h *Handlers) gatewayOwnedForkSource(ctx context.Context, source *corev1alpha1.Task) (bool, error) {
	if _, owned := gatewayruntime.TaskIdentity(source); owned {
		return true, nil
	}
	if h.gatewayEventStore == nil || source == nil || source.UID == "" {
		return false, nil
	}
	if _, err := h.gatewayEventStore.GetGatewayEventForTask(ctx, source.Namespace, source.Name, string(source.UID)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fiber.NewError(fiber.StatusInternalServerError, "failed to resolve source gateway ownership")
	}
	return true, nil
}

func detachGatewayFork(forked *corev1alpha1.Task) {
	if forked == nil {
		return
	}
	forked.Spec.SessionRef = nil
	forked.Spec.RequestedBy = nil
	forked.Spec.Transaction = nil
	for _, key := range []string{
		gatewayruntime.TaskGatewayNameLabel, gatewayruntime.TaskGatewayBindingLabel,
		gatewayruntime.TaskGatewayEventLabel,
	} {
		delete(forked.Labels, key)
	}
	for _, key := range []string{
		gatewayruntime.TaskGatewayEventAnnotation, gatewayruntime.TaskGatewayExternalEvent,
		gatewayruntime.TaskGatewaySession, gatewayruntime.TaskGatewayNameAnnotation,
		gatewayruntime.TaskGatewayBindingAnnotation, gatewayruntime.TaskGatewayDelivery,
		gatewayruntime.TaskGatewayProviderMessage,
	} {
		delete(forked.Annotations, key)
	}
}

func marshalForkEventContent(sourceName, newName string, afterSeq int64, eventKind string) (json.RawMessage, error) {
	content, err := json.Marshal(map[string]any{"sourceTaskName": sourceName, "newTaskName": newName, "afterSeq": afterSeq})
	if err != nil {
		return nil, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to encode fork %s event: %v", eventKind, err))
	}
	return content, nil
}

func taskEventSeqExists(
	ctx context.Context,
	eventStore store.ExecutionEventStore,
	namespace,
	taskName string,
	seq,
	latestSeq int64,
) (bool, error) {
	return newTaskTimelineReader(eventStore, namespace, taskName).seqExists(ctx, seq, latestSeq)
}

func listTaskEventsThrough(
	ctx context.Context,
	eventStore store.ExecutionEventStore,
	namespace,
	taskName string,
	throughSeq int64,
) ([]store.ExecutionEvent, bool, error) {
	return newTaskTimelineReader(eventStore, namespace, taskName).listRecentContextThrough(ctx, throughSeq, forkcontext.DefaultMaxEvents+1)
}
