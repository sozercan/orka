package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

type acpTaskSession struct {
	Turn             *ACPSessionTurn
	Binding          ACPRuntimeSessionBinding
	Bootstrap        *ACPBootstrapTranscript
	UserPrompt       string
	VerifiedBaseline *store.VerifiedBranchBaseline
	Reused           bool
	LeaseGeneration  int64
	finalized        bool
	requeued         bool
}

const acpPreSubmissionCleanupTimeout = 30 * time.Second

func promptAttemptSessionBound(attempt *store.PromptAttempt) (bool, error) {
	if attempt == nil {
		return false, fmt.Errorf("%w: PromptAttempt is required", store.ErrConflict)
	}
	sessionUID := strings.TrimSpace(attempt.SessionUID)
	switch {
	case sessionUID == "" && attempt.SessionLeaseGeneration == 0:
		return false, nil
	case sessionUID == "" || attempt.SessionLeaseGeneration < 1:
		return false, fmt.Errorf("%w: prompt attempt %s has an incomplete SessionTurn binding", store.ErrConflict, attempt.ID)
	default:
		return true, nil
	}
}

func (d *ACPDispatcher) finalizedSessionTurnKnown(taskUID types.UID, turnID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	knownTurnID, known := d.finalizedTurns[taskUID]
	return known && knownTurnID == turnID
}

func (d *ACPDispatcher) rememberFinalizedSessionTurn(taskUID types.UID, turnID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finalizedTurns == nil {
		d.finalizedTurns = map[types.UID]string{}
	}
	d.finalizedTurns[taskUID] = turnID
}

// pruneFinalizedSessionTurns bounds the lookup cache to Tasks returned by the
// current cluster-wide scan. A Task deleted during the scan can leave one
// entry until the next pass, but cumulative historical throughput cannot grow
// the map indefinitely.
func (d *ACPDispatcher) pruneFinalizedSessionTurns(tasks []corev1alpha1.Task) {
	live := make(map[types.UID]struct{}, len(tasks))
	for i := range tasks {
		if tasks[i].UID != "" {
			live[tasks[i].UID] = struct{}{}
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	for taskUID := range d.finalizedTurns {
		if _, ok := live[taskUID]; !ok {
			delete(d.finalizedTurns, taskUID)
		}
	}
}

// sessionTurnRequiresTerminalRecovery reports a session-bound Task whose
// attempt settled in any terminal state while its SessionTurn is still open.
// Inline settle finalization silently skips when its in-memory turn is
// missing, and the recovery sweep otherwise assumes a complete projection
// implies a finalized turn - leaving artifact retirement blocked on
// "SessionTurn is not finalized" until the Task deadline fails it. Succeeded
// additionally waits for terminal delivery because publication recovery owns
// the open turn until then; Failed, Cancelled, and OutcomeUnknown attempts
// have no delivery to wait for. The check runs for Finalizing AND settled
// terminal phases: a finalizer that silently skipped its missing in-memory
// turn still terminalizes the Task, so a terminal phase alone is never proof
// of a finalized turn.
func (d *ACPDispatcher) sessionTurnRequiresTerminalRecovery(
	ctx context.Context,
	task *corev1alpha1.Task,
	attempt *store.PromptAttempt,
) (bool, error) {
	if task == nil || attempt == nil ||
		!store.IsTerminalPromptExecutionState(attempt.ExecutionState) ||
		(attempt.ExecutionState == store.PromptExecutionSucceeded &&
			!store.IsTerminalPromptDeliveryState(attempt.DeliveryState)) {
		return false, nil
	}
	// A settled Task phase is NOT proof of a finalized turn: a finalizer that
	// silently skipped its missing in-memory turn still terminalizes the Task
	// (for example deletion-driven cancellation), so every terminal phase is
	// inspected alongside Finalizing.
	switch task.Status.Phase {
	case corev1alpha1.TaskPhaseFinalizing, corev1alpha1.TaskPhaseSucceeded,
		corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
	default:
		return false, nil
	}
	bound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return false, err
	}
	if !bound {
		return false, nil
	}
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		return false, err
	}
	settled := task.Status.Phase != corev1alpha1.TaskPhaseFinalizing
	if d.finalizedSessionTurnKnown(task.UID, turnID) {
		return false, nil
	}
	turn, err := d.Store.GetSessionTurn(ctx, turnID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			if settled {
				d.rememberFinalizedSessionTurn(task.UID, turnID)
			}
			return false, nil
		}
		return false, err
	}
	if turn.State != store.SessionTurnFinalized {
		return true, nil
	}
	// SessionTurnFinalized proves only the durable turn commit, not the
	// cross-store activation tail (lease release, status projection, outbox
	// activation). The Task controller can terminalize independently after
	// that commit, so Task phase is never durable tail-completion proof.
	// ResumeSessionTurnFinalization is idempotent, and its successful recovery
	// records the immutable turn ID in the cache above.
	return true, nil
}

func (d *ACPDispatcher) reconcileUnfinalizedTaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	cause error,
) error {
	if session == nil || session.Turn == nil || session.finalized || session.requeued {
		return nil
	}
	attemptID := session.Turn.Turn.PromptAttemptID
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	switch attempt.ExecutionState {
	case store.PromptExecutionReserved, store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
		if cause == nil {
			cause = errors.New("ACP task left pre-submission execution without completing")
		}
		return d.requeuePreSubmissionTask(ctx, task, attemptID, fence, cause)
	case store.PromptExecutionSubmitting, store.PromptExecutionSubmittedUnknown,
		store.PromptExecutionAccepted, store.PromptExecutionRunning, store.PromptExecutionSettling:
		if err := d.persistOutcomeUnknown(ctx, attemptID, fence, "RuntimeLost", "task exited before prompt settlement could be proven"); err != nil {
			return err
		}
		attempt, err = d.Store.GetPromptAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
	case store.PromptExecutionSucceeded:
		if !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			// Publication recovery owns the still-open SessionTurn and lease until
			// delivery reaches a durable terminal receipt.
			return nil
		}
	case store.PromptExecutionFailed, store.PromptExecutionCancelled, store.PromptExecutionOutcomeUnknown:
	default:
		return fmt.Errorf("unsupported unfinalized ACP SessionTurn attempt state %s", attempt.ExecutionState)
	}
	return d.finalizeRecoveredTerminalSession(ctx, task, attempt, fence)
}

type acpTaskSessionPreparation struct {
	control          *store.SessionControl
	current          *ACPRuntimeSessionBinding
	plan             ACPRuntimeSessionPlan
	bootstrap        *ACPBootstrapTranscript
	userPrompt       string
	verifiedBaseline *store.VerifiedBranchBaseline
}

type acpSessionTranscriptAppendPolicy struct {
	skipTranscriptAppend bool
	skipUserPromptAppend bool
}

func acpSessionTranscriptAppendPolicyForTask(task *corev1alpha1.Task) acpSessionTranscriptAppendPolicy {
	if task == nil || task.Spec.SessionRef == nil {
		return acpSessionTranscriptAppendPolicy{}
	}
	if !task.Spec.SessionRef.Append {
		return acpSessionTranscriptAppendPolicy{skipTranscriptAppend: true}
	}
	return acpSessionTranscriptAppendPolicy{skipUserPromptAppend: task.Spec.SessionRef.PromptIncluded}
}

// acpSessionLineageIdentity carries the protocol/runtime lineage identity
// claimed atomically with the Session mutation lease.
type acpSessionLineageIdentity struct {
	NamespaceUID    string
	RuntimeIdentity string
	ConfigDigest    string
	// WorkspaceSessionUID is the immutable SessionControl identity frozen into
	// a session-reused execution-workspace binding.
	WorkspaceSessionUID string
}

func acpSessionLineageConfigDigest(plan ACPRuntimePlan) (string, error) {
	if plan.Workspace == nil || plan.Workspace.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
		if err := harnessv2.ValidateProfileDigest(plan.Digest); err != nil {
			return "", fmt.Errorf("session lineage runtime profile digest: %w", err)
		}
		return string(plan.Digest), nil
	}
	return acpSessionLineageConfigurationDigest(
		string(plan.Digest),
		plan.Image,
		plan.Workspace.BindingDigest,
	)
}

func acpSessionLineageConfigurationDigest(
	runtimeProfileDigest string,
	runtimeImage string,
	workspaceBindingDigest string,
) (string, error) {
	if err := harnessv2.ValidateProfileDigest(harnessv2.ProfileDigest(runtimeProfileDigest)); err != nil {
		return "", fmt.Errorf("session lineage runtime profile digest: %w", err)
	}
	runtimeImage = strings.TrimSpace(runtimeImage)
	if !digestPinnedImagePattern.MatchString(runtimeImage) {
		return "", fmt.Errorf("session lineage runtime image must be pinned by sha256 digest")
	}
	workspaceBindingDigest = strings.TrimSpace(workspaceBindingDigest)
	if err := store.ValidateCanonicalDigest("session lineage workspace binding digest", workspaceBindingDigest); err != nil {
		return "", err
	}
	// This is the provider-backed execution-workspace binding frozen into the
	// RuntimePool plan. It is deliberately distinct from the harness
	// RuntimeSession WorkspaceDigest, whose repo-less baseline may rotate after
	// the Session lease is acquired without changing protocol lineage.
	return acpDomainDigest("runtime-session-lineage-configuration/v1", struct {
		RuntimeProfileDigest   string `json:"runtimeProfileDigest"`
		RuntimeImage           string `json:"runtimeImage"`
		WorkspaceBindingDigest string `json:"workspaceBindingDigest"`
	}{
		RuntimeProfileDigest:   runtimeProfileDigest,
		RuntimeImage:           runtimeImage,
		WorkspaceBindingDigest: workspaceBindingDigest,
	})
}

func (d *ACPDispatcher) prepareTaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	profileDigest harnessv2.ProfileDigest,
	mcpBindingDigest string,
	runtimeInstanceID harnessv2.RuntimeInstanceID,
	supervisorBootID harnessv2.SupervisorBootID,
	lineage acpSessionLineageIdentity,
) (*acpTaskSession, error) {
	if task.Spec.SessionRef == nil {
		return nil, nil
	}
	if d.Sessions == nil {
		return nil, fmt.Errorf("ACP Session continuity is not configured")
	}
	if lineage.ConfigDigest == "" {
		lineage.ConfigDigest = string(profileDigest)
	}
	preparation, err := d.planTaskSession(
		ctx, task, fence, profileDigest, mcpBindingDigest, runtimeInstanceID, supervisorBootID, lineage.WorkspaceSessionUID,
	)
	if err != nil {
		return nil, err
	}
	lease, turn, err := d.bindAndOpenTaskSessionTurn(
		ctx, task, fence, runtimeInstanceID, preparation.control, preparation.userPrompt,
		acpSessionTranscriptAppendPolicyForTask(task), lineage,
	)
	if err != nil {
		return nil, err
	}
	if preparation.current == nil && lease.Session.LeaseGeneration > 1 {
		preparation.plan.Binding.Generation = max(
			preparation.plan.Binding.Generation,
			uint64(lease.Session.LeaseGeneration),
		)
		preparation.plan.Recreate = true
		preparation.plan.BootstrapRequired = true
		preparation.plan.Reason = "controller-restarted"
	}
	if preparation.plan.Binding.Generation < uint64(lease.Session.LeaseGeneration) && preparation.plan.Recreate {
		preparation.plan.Binding.Generation = uint64(lease.Session.LeaseGeneration)
	}
	if task.Status.Execution != nil && task.Status.Execution.RuntimeSessionGeneration > 0 {
		if task.Status.Execution.RuntimeSessionUID != "" && task.Status.Execution.RuntimeSessionUID != preparation.control.SessionUID {
			return nil, fmt.Errorf("%w: persisted Task RuntimeSession UID does not match durable Session UID", store.ErrConflict)
		}
		generationFloor := uint64(task.Status.Execution.RuntimeSessionGeneration)
		if task.Status.Execution.RuntimeInstanceID != "" &&
			task.Status.Execution.RuntimeInstanceID != string(runtimeInstanceID) {
			if generationFloor >= maxControllerRuntimeSessionGeneration {
				return nil, store.ValidationErrorf("ACP runtime session generation is exhausted")
			}
			generationFloor++
		}
		if preparation.plan.Binding.Generation < generationFloor {
			preparation.plan.Binding.Generation = generationFloor
			preparation.plan.Binding.RecreationRequired = true
			preparation.plan.Recreate = true
			preparation.plan.BootstrapRequired = true
			preparation.plan.Reason = "persisted-runtime-session-recreation"
		}
	}
	logf.FromContext(ctx).Info("ACP runtime session planned",
		"namespace", task.Namespace, "task", task.Name, "sessionUID", preparation.control.SessionUID,
		"reason", preparation.plan.Reason, "generation", preparation.plan.Binding.Generation,
		"recreate", preparation.plan.Recreate, "bootstrap", preparation.plan.BootstrapRequired,
		"currentBindingKnown", preparation.current != nil, "leaseGeneration", lease.Session.LeaseGeneration,
		"durableGeneration", preparation.control.RuntimeSessionGeneration,
	)
	return &acpTaskSession{
		Turn: turn, Binding: preparation.plan.Binding, Bootstrap: preparation.bootstrap,
		UserPrompt:       preparation.userPrompt,
		VerifiedBaseline: preparation.verifiedBaseline, Reused: !preparation.plan.Recreate,
		LeaseGeneration: lease.Key.LeaseGeneration,
	}, nil
}

func (d *ACPDispatcher) planTaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	profileDigest harnessv2.ProfileDigest,
	mcpBindingDigest string,
	runtimeInstanceID harnessv2.RuntimeInstanceID,
	supervisorBootID harnessv2.SupervisorBootID,
	expectedSessionUID string,
) (*acpTaskSessionPreparation, error) {
	name := strings.TrimSpace(task.Spec.SessionRef.Name)
	if name == "" {
		return nil, fmt.Errorf("SessionRef name is required")
	}
	transcriptBackedPrompt := task.Spec.SessionRef.PromptIncluded &&
		strings.TrimSpace(task.Spec.SessionRef.ThroughMessageID) != ""
	if !task.Spec.SessionRef.Create && !transcriptBackedPrompt {
		if _, err := d.Store.GetSessionControl(ctx, task.Namespace, name); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("session %s/%s does not exist and create=false", task.Namespace, name)
			}
			return nil, err
		}
	}
	sessionType := defaultACPSessionType
	if task.Spec.SessionRef.PromptIncluded && strings.HasPrefix(task.Spec.SessionRef.ThroughMessageID, "gateway:") {
		sessionType = store.SessionTypeGateway
	}
	control, err := d.Sessions.EnsureSession(ctx, ACPEnsureSessionRequest{
		Namespace: task.Namespace, SessionName: name, SessionType: sessionType,
		ExpectedSessionUID:        expectedSessionUID,
		RequireExistingTranscript: transcriptBackedPrompt && !task.Spec.SessionRef.Create,
		Fence:                     fence, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}

	// Complete every fallible transcript/runtime planning read before acquiring
	// the mutation Lease. Once the Lease is held, the only next durable action is
	// opening the exact SessionTurn.
	current := d.currentRuntimeSessionBinding(control.SessionUID)
	if current == nil {
		current, err = runtimeSessionBindingFromTaskStatus(
			task, control.SessionUID, profileDigest, runtimeInstanceID, supervisorBootID,
		)
		if err != nil {
			return nil, err
		}
	}
	if control.RuntimeSessionGeneration < 0 {
		return nil, store.ValidationErrorf("durable Session RuntimeSession generation must not be negative")
	}
	generationFloor := uint64(control.RuntimeSessionGeneration)
	workspaceGenerationFloor, err := d.taskRuntimeSessionGenerationFloor(ctx, task)
	if err != nil {
		return nil, err
	}
	generationFloor = max(generationFloor, workspaceGenerationFloor)
	plan, err := PlanACPRuntimeSession(*control, current, profileDigest, mcpBindingDigest, runtimeInstanceID, supervisorBootID)
	if err != nil {
		return nil, err
	}
	plan, err = enforceACPRuntimeSessionGenerationFloor(plan, generationFloor)
	if err != nil {
		return nil, err
	}
	var userPrompt string
	var bootstrap *ACPBootstrapTranscript
	if plan.BootstrapRequired {
		bootstrap, userPrompt, err = d.resolveTaskSessionBootstrap(ctx, task, control)
	} else {
		userPrompt, err = d.resolveTaskSessionPrompt(ctx, task, control)
	}
	if err != nil {
		return nil, err
	}
	var verifiedBaseline *store.VerifiedBranchBaseline
	if control.VerifiedBaseline != nil {
		copyBaseline := *control.VerifiedBaseline
		verifiedBaseline = &copyBaseline
	}
	return &acpTaskSessionPreparation{
		control: control, current: current, plan: plan, bootstrap: bootstrap, userPrompt: userPrompt,
		verifiedBaseline: verifiedBaseline,
	}, nil
}

func (d *ACPDispatcher) resolveTaskSessionPrompt(
	ctx context.Context, task *corev1alpha1.Task, control *store.SessionControl,
) (string, error) {
	userPrompt := task.Spec.Prompt
	ref := task.Spec.SessionRef
	if ref == nil || !ref.PromptIncluded {
		return userPrompt, nil
	}
	throughMessageID := strings.TrimSpace(ref.ThroughMessageID)
	if throughMessageID == "" {
		return "", store.ValidationErrorf("prompt-included ACP session requires a through-message ID")
	}
	current, err := d.Sessions.controls.GetSessionControl(ctx, control.Namespace, control.SessionName)
	if err != nil {
		return "", fmt.Errorf("load ACP session for prompt resolution: %w", err)
	}
	if current.SessionUID != control.SessionUID {
		return "", fmt.Errorf(
			"%w: ACP prompt session UID changed from %q to %q",
			store.ErrConflict, control.SessionUID, current.SessionUID,
		)
	}
	if current.Availability != store.SessionAvailable {
		return "", fmt.Errorf(
			"%w: ACP session %s/%s is reconciliation-blocked",
			store.ErrConflict, current.Namespace, current.SessionName,
		)
	}
	messages, err := d.Sessions.transcripts.LoadTranscriptThrough(
		ctx, current.Namespace, current.SessionName, throughMessageID, 1,
	)
	if err != nil {
		return "", fmt.Errorf("load canonical ACP session prompt: %w", err)
	}
	if len(messages) != 1 || messages[0].ID != throughMessageID {
		return "", fmt.Errorf(
			"%w: bounded ACP transcript does not end at message %q",
			store.ErrConflict, throughMessageID,
		)
	}
	throughMessage := messages[0]
	if throughMessage.Role != acpBootstrapRoleUser || strings.TrimSpace(throughMessage.Content) == "" {
		return "", store.ValidationErrorf("prompt-included ACP session cutoff must end at a non-empty user message")
	}
	return throughMessage.Content, nil
}

func (d *ACPDispatcher) resolveTaskSessionBootstrap(
	ctx context.Context, task *corev1alpha1.Task, control *store.SessionControl,
) (*ACPBootstrapTranscript, string, error) {
	userPrompt := task.Spec.Prompt
	ref := task.Spec.SessionRef
	if ref == nil || (!ref.Append && !ref.PromptIncluded) {
		return nil, userPrompt, nil
	}
	if strings.TrimSpace(ref.ThroughMessageID) == "" {
		if ref.PromptIncluded {
			return nil, "", store.ValidationErrorf("prompt-included ACP session requires a through-message ID")
		}
		bootstrap, err := d.Sessions.BuildBootstrapTranscriptWithLimit(ctx, *control, acpSessionReferenceMaxMessages(ref))
		return bootstrap, userPrompt, err
	}
	bootstrap, throughMessage, err := d.Sessions.BuildBootstrapTranscriptThrough(
		ctx, *control, ref.ThroughMessageID, acpSessionReferenceMaxMessages(ref), ref.PromptIncluded,
	)
	if err != nil {
		return nil, "", err
	}
	if ref.PromptIncluded {
		if throughMessage == nil || throughMessage.Role != acpBootstrapRoleUser || strings.TrimSpace(throughMessage.Content) == "" {
			return nil, "", store.ValidationErrorf("prompt-included ACP session cutoff must end at a non-empty user message")
		}
		userPrompt = throughMessage.Content
	}
	return bootstrap, userPrompt, nil
}

func acpSessionReferenceMaxMessages(ref *corev1alpha1.SessionReference) int {
	if ref != nil && ref.MaxMessages > 0 {
		return int(ref.MaxMessages)
	}
	return 50
}

func runtimeSessionBindingFromTaskStatus(
	task *corev1alpha1.Task,
	sessionUID string,
	profileDigest harnessv2.ProfileDigest,
	runtimeInstanceID harnessv2.RuntimeInstanceID,
	supervisorBootID harnessv2.SupervisorBootID,
) (*ACPRuntimeSessionBinding, error) {
	if task == nil || task.Status.Execution == nil || task.Status.Execution.RuntimeSessionGeneration <= 0 {
		return nil, nil
	}
	execution := task.Status.Execution
	if execution.RuntimeSessionUID == "" {
		return nil, nil
	}
	if execution.RuntimeSessionUID != sessionUID {
		return nil, fmt.Errorf("%w: persisted Task RuntimeSession UID does not match durable Session UID", store.ErrConflict)
	}
	workspaceDigest := strings.TrimSpace(execution.RuntimeSessionWorkspaceDigest)
	if workspaceDigest != "" {
		if err := store.ValidateCanonicalDigest("persisted RuntimeSession workspace digest", workspaceDigest); err != nil {
			return nil, err
		}
	}
	persistedRuntimeInstanceID := harnessv2.RuntimeInstanceID(strings.TrimSpace(execution.RuntimeInstanceID))
	if persistedRuntimeInstanceID == "" {
		persistedRuntimeInstanceID = runtimeInstanceID
	}
	persistedSupervisorBootID := harnessv2.SupervisorBootID(strings.TrimSpace(execution.RuntimeSessionSupervisorBootID))
	if persistedSupervisorBootID == "" {
		persistedSupervisorBootID = supervisorBootID
	}
	persistedProfileDigest := harnessv2.ProfileDigest(strings.TrimSpace(execution.RuntimeSessionProfileDigest))
	if persistedProfileDigest == "" {
		persistedProfileDigest = profileDigest
	} else if err := harnessv2.ValidateProfileDigest(persistedProfileDigest); err != nil {
		return nil, fmt.Errorf("persisted RuntimeSession profile digest: %w", err)
	}
	persistedMCPDigest := strings.TrimSpace(execution.RuntimeSessionMCPDigest)
	if persistedMCPDigest != "" {
		if err := store.ValidateCanonicalDigest("persisted RuntimeSession MCP digest", persistedMCPDigest); err != nil {
			return nil, err
		}
	}
	return &ACPRuntimeSessionBinding{
		SessionUID: sessionUID, Generation: uint64(execution.RuntimeSessionGeneration), ProfileDigest: persistedProfileDigest,
		RuntimeInstanceID: persistedRuntimeInstanceID, SupervisorBootID: persistedSupervisorBootID,
		WorkspaceDigest: workspaceDigest, MCPDigest: persistedMCPDigest,
		RecreationRequired: execution.RuntimeSessionRecreationPending,
	}, nil
}

func (d *ACPDispatcher) bindAndOpenTaskSessionTurn(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	runtimeInstanceID harnessv2.RuntimeInstanceID,
	control *store.SessionControl,
	userPrompt string,
	appendPolicy acpSessionTranscriptAppendPolicy,
	lineage acpSessionLineageIdentity,
) (*ACPSessionLease, *ACPSessionTurn, error) {
	attemptID, err := promptAttemptIDFromTask(task)
	if err != nil {
		return nil, nil, err
	}
	existingLease, _, err := matchingTaskSessionLease(control, task)
	if err != nil {
		return nil, nil, err
	}
	leaseGeneration := control.LeaseGeneration + 1
	if existingLease != nil {
		leaseGeneration = existingLease.Generation
	}
	if err := d.transitionAttempt(ctx, attemptID, fence, store.PromptExecutionReserved, store.PromptExecutionSessionStarting, "session-starting", &attemptRuntimeBinding{
		RuntimeInstanceID: string(runtimeInstanceID), SessionUID: control.SessionUID, SessionGeneration: leaseGeneration,
	}); err != nil {
		return nil, nil, err
	}

	// Re-enter exact lease acquisition even when the Lease already exists. This
	// is idempotent and completes a lineage payload projection that may have
	// failed after the Kubernetes-authoritative status CAS.
	lease, err := d.acquireTaskSessionLease(ctx, task, fence, control, lineage)
	if err != nil {
		requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err)
		return nil, nil, errors.Join(err, requeueErr)
	}
	if lease.Key.LeaseGeneration != leaseGeneration {
		err := fmt.Errorf("%w: acquired ACP Session lease generation changed after PromptAttempt binding", store.ErrConflict)
		requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, err)
		return nil, nil, errors.Join(err, requeueErr)
	}
	turn, err := d.Sessions.OpenTurn(ctx, ACPOpenSessionTurnRequest{
		Lease: *lease, Fence: fence, PromptAttemptID: attemptID,
		PromptRequestDigest: task.Status.Execution.RequestDigest, UserPrompt: userPrompt,
		SkipTranscriptAppend: appendPolicy.skipTranscriptAppend,
		SkipUserPromptAppend: appendPolicy.skipUserPromptAppend,
		OpenedAt:             time.Now().UTC(),
	})
	if err != nil {
		recovered, recoveryErr := d.handleSessionTurnOpenFailure(
			ctx, task, fence, attemptID, lease, userPrompt, appendPolicy, err,
		)
		if recoveryErr != nil {
			return nil, nil, recoveryErr
		}
		return lease, recovered, nil
	}
	return lease, turn, nil
}

func (d *ACPDispatcher) quiesceInterruptedTaskSessionPreparation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (*acpTaskSession, error) {
	if task == nil || task.Spec.SessionRef == nil || d.Sessions == nil {
		return nil, nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acpPreSubmissionCleanupTimeout)
	defer cancel()

	attempt, err := d.Store.GetPromptAttempt(cleanupCtx, attemptID)
	if err != nil {
		return nil, err
	}
	bound, err := promptAttemptSessionBound(attempt)
	if err != nil {
		return nil, err
	}
	if !bound {
		switch attempt.ExecutionState {
		case store.PromptExecutionReserved, store.PromptExecutionCancelled:
			return nil, nil
		case store.PromptExecutionSessionStarting, store.PromptExecutionPlanned:
			if err := d.requeuePreSubmissionTask(cleanupCtx, task, attemptID, fence, context.DeadlineExceeded); err != nil {
				return nil, err
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("%w: interrupted session preparation left prompt attempt %s in state %s", store.ErrConflict, attempt.ID, attempt.ExecutionState)
		}
	}

	session, err := d.recoveredTaskSession(cleanupCtx, task, attempt)
	if err == nil {
		if session == nil || session.Turn == nil || session.Turn.Turn.State != store.SessionTurnOpen {
			return nil, fmt.Errorf("%w: interrupted session preparation did not recover an open SessionTurn", store.ErrConflict)
		}
		return session, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	control, controlErr := d.Store.GetSessionControl(cleanupCtx, task.Namespace, task.Spec.SessionRef.Name)
	if controlErr != nil {
		return nil, errors.Join(err, controlErr)
	}
	if control.SessionUID != attempt.SessionUID {
		return nil, fmt.Errorf("%w: interrupted SessionControl UID does not match PromptAttempt", store.ErrConflict)
	}
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	if control.Lease != nil {
		lease := ACPSessionLease{Session: *control, Key: key}
		if validateErr := validateACPSessionLease(control, key); validateErr != nil {
			return nil, errors.Join(err, validateErr)
		}
		if _, releaseErr := d.Sessions.ReleaseMutationLease(cleanupCtx, ACPReleaseSessionLeaseRequest{
			Lease: lease, Fence: fence, ReleasedAt: time.Now().UTC(),
		}); releaseErr != nil {
			return nil, errors.Join(err, releaseErr)
		}
	}
	if requeueErr := d.requeuePreSubmissionTask(cleanupCtx, task, attemptID, fence, context.DeadlineExceeded); requeueErr != nil {
		return nil, errors.Join(err, requeueErr)
	}
	return nil, nil
}

func (d *ACPDispatcher) handleSessionTurnOpenFailure(
	ctx context.Context, task *corev1alpha1.Task, fence store.ControllerEpochFence,
	attemptID string, lease *ACPSessionLease, userPrompt string,
	appendPolicy acpSessionTranscriptAppendPolicy, openErr error,
) (*ACPSessionTurn, error) {
	turnID, err := lease.Key.CanonicalID()
	if err != nil {
		return nil, errors.Join(openErr, err)
	}
	persisted, getErr := d.Store.GetSessionTurn(ctx, turnID)
	if getErr == nil {
		effectivePolicy := appendPolicy
		expectedDigest, digestErr := acpSessionTurnDigest(
			turnID, attemptID, task.Status.Execution.RequestDigest, userPrompt,
			effectivePolicy.skipTranscriptAppend, effectivePolicy.skipUserPromptAppend,
		)
		if digestErr == nil && persisted.RequestDigest != expectedDigest && (effectivePolicy.skipTranscriptAppend || effectivePolicy.skipUserPromptAppend) {
			legacyDigest, legacyErr := acpSessionTurnDigest(
				turnID, attemptID, task.Status.Execution.RequestDigest, userPrompt, false, false,
			)
			if legacyErr == nil && persisted.RequestDigest == legacyDigest {
				effectivePolicy = acpSessionTranscriptAppendPolicy{}
				expectedDigest = legacyDigest
			}
		}
		if digestErr == nil && persisted.State == store.SessionTurnOpen && persisted.Key == lease.Key &&
			persisted.PromptAttemptID == attemptID && persisted.RequestDigest == expectedDigest && persisted.UserPrompt == userPrompt {
			return &ACPSessionTurn{
				Lease: *lease, Turn: *persisted,
				SkipTranscriptAppend: effectivePolicy.skipTranscriptAppend,
				SkipUserPromptAppend: effectivePolicy.skipUserPromptAppend,
			}, nil
		}
		return nil, errors.Join(openErr, fmt.Errorf("%w: persisted SessionTurn does not match failed open request", store.ErrConflict))
	}
	if !errors.Is(getErr, store.ErrNotFound) {
		_ = d.requeuePreSubmissionTask(ctx, task, attemptID, fence, openErr)
		return nil, errors.Join(openErr, getErr)
	}
	_, releaseErr := d.Sessions.ReleaseMutationLease(ctx, ACPReleaseSessionLeaseRequest{Lease: *lease, Fence: fence, ReleasedAt: time.Now().UTC()})
	requeueErr := d.requeuePreSubmissionTask(ctx, task, attemptID, fence, openErr)
	return nil, errors.Join(openErr, releaseErr, requeueErr)
}

func matchingTaskSessionLease(control *store.SessionControl, task *corev1alpha1.Task) (*store.SessionMutationLease, string, error) {
	existing := control.Lease
	if existing == nil {
		return nil, "", nil
	}
	expectedDigest, err := acpSessionMutationLeaseDigest(
		control.SessionUID, existing.Generation, string(task.UID), int64(task.Status.Execution.Attempt),
		task.Status.Execution.PromptID, task.Status.Execution.RequestDigest,
	)
	if err != nil {
		return nil, "", err
	}
	if existing.TaskUID != string(task.UID) || existing.Attempt != int64(task.Status.Execution.Attempt) ||
		existing.PromptID != task.Status.Execution.PromptID || existing.RequestDigest != expectedDigest {
		return nil, expectedDigest, nil
	}
	return existing, expectedDigest, nil
}

func (d *ACPDispatcher) acquireTaskSessionLease(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	control *store.SessionControl,
	lineage acpSessionLineageIdentity,
) (*ACPSessionLease, error) {
	expires := time.Now().UTC().Add(30 * time.Minute)
	if task.Spec.Timeout != nil && task.Spec.Timeout.Duration > 0 {
		expires = time.Now().UTC().Add(task.Spec.Timeout.Duration + time.Minute)
	}
	return d.Sessions.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
		Session: *control, Fence: fence, TaskName: task.Name, TaskUID: string(task.UID), Attempt: int64(task.Status.Execution.Attempt),
		PromptID: task.Status.Execution.PromptID, PromptRequestDigest: task.Status.Execution.RequestDigest,
		AcquiredAt: time.Now().UTC(), ExpiresAt: &expires,
		NamespaceUID: lineage.NamespaceUID, RuntimeIdentity: lineage.RuntimeIdentity, ConfigDigest: lineage.ConfigDigest,
	})
}

func (d *ACPDispatcher) openTaskSessionTurn(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
) error {
	if session == nil {
		return nil
	}
	if session.Turn != nil {
		return nil
	}
	control, err := d.Store.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		return err
	}
	lease := ACPSessionLease{Session: *control, Key: store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: string(task.UID), Attempt: int64(task.Status.Execution.Attempt), PromptID: task.Status.Execution.PromptID,
	}}
	userPrompt := session.UserPrompt
	if strings.TrimSpace(userPrompt) == "" {
		userPrompt = task.Spec.Prompt
	}
	appendPolicy := acpSessionTranscriptAppendPolicyForTask(task)
	turn, err := d.Sessions.OpenTurn(ctx, ACPOpenSessionTurnRequest{
		Lease: lease, Fence: fence, PromptAttemptID: attemptID,
		PromptRequestDigest: task.Status.Execution.RequestDigest, UserPrompt: userPrompt,
		SkipTranscriptAppend: appendPolicy.skipTranscriptAppend,
		SkipUserPromptAppend: appendPolicy.skipUserPromptAppend,
		OpenedAt:             time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	session.Turn = turn
	return nil
}

func (d *ACPDispatcher) finalizeTaskSessionResult(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	result, publicationID string,
	phase corev1alpha1.TaskPhase,
	delivery corev1alpha1.TaskDeliveryStatus,
) error {
	if session == nil || session.Turn == nil {
		// The turn-aware recovery sweep converges the still-open SessionTurn;
		// log so the skip is attributable when it happens.
		logf.FromContext(ctx).Info("skipping inline ACP session finalization without an open in-memory turn",
			"namespace", task.Namespace, "task", task.Name)
		return nil
	}
	execution, err := taskSessionProjectionExecution(task, corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
		Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID,
	})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: task.Status.Execution.Attempt,
		Phase: phase, Execution: execution, Delivery: &delivery,
	})
	if err != nil {
		return err
	}
	_, err = d.Sessions.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
		SessionTurn: *session.Turn, Fence: fence, AssistantResult: result,
		PublicationID: publicationID,
		Projection:    ACPFinalizationProjection{ProjectionKind: "TaskTerminalStatus", Payload: payload, AvailableAt: time.Now().UTC()},
		FinalizedAt:   time.Now().UTC(),
	})
	if err == nil {
		session.finalized = true
		// Only a complete binding may replace the live one: a recovered
		// session that could not rebuild its generation must not clobber
		// the binding the live path recorded.
		if session.Binding.Generation > 0 {
			d.setRuntimeSessionBinding(session.Binding)
		}
	}
	return err
}

func (d *ACPDispatcher) finalizeTaskSessionUnknown(ctx context.Context, task *corev1alpha1.Task, fence store.ControllerEpochFence, session *acpTaskSession, reason string) error {
	if session == nil || session.Turn == nil || session.finalized {
		return nil
	}
	execution, err := taskSessionProjectionExecution(task, corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateOutcomeUnknown, Outcome: corev1alpha1.TaskExecutionOutcomeOutcomeUnknown,
		Attempt: task.Status.Execution.Attempt, PromptID: task.Status.Execution.PromptID,
		Reason: "RuntimeLost", Message: reason,
	})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: task.Status.Execution.Attempt,
		Phase: corev1alpha1.TaskPhaseFailed, Execution: execution, Delivery: task.Status.Delivery,
	})
	if err != nil {
		return err
	}
	_, err = d.Sessions.FinalizeOutcomeUnknown(ctx, ACPFinalizeOutcomeUnknownRequest{
		SessionTurn: *session.Turn, Fence: fence, Reason: reason,
		Projection:  ACPFinalizationProjection{ProjectionKind: "TaskTerminalStatus", Payload: payload, AvailableAt: time.Now().UTC()},
		FinalizedAt: time.Now().UTC(),
	})
	if err == nil {
		session.finalized = true
		d.removeRuntimeSessionBinding(session.Binding.SessionUID)
	}
	return err
}

func (d *ACPDispatcher) finalizeTaskSessionMarker(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	kind, reason string,
	phase corev1alpha1.TaskPhase,
	execution corev1alpha1.TaskExecutionStatus,
) error {
	if session == nil || session.Turn == nil || session.finalized {
		return nil
	}
	var err error
	execution, err = taskSessionProjectionExecution(task, execution)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: execution.Attempt,
		Phase: phase, Message: reason, Execution: execution, Delivery: task.Status.Delivery,
	})
	if err != nil {
		return err
	}
	_, err = d.Sessions.FinalizeOutcomeMarker(ctx, ACPFinalizeOutcomeMarkerRequest{
		SessionTurn: *session.Turn, Fence: fence, Kind: kind, Reason: reason,
		Projection:  ACPFinalizationProjection{ProjectionKind: "TaskTerminalStatus", Payload: payload, AvailableAt: time.Now().UTC()},
		FinalizedAt: time.Now().UTC(),
	})
	if err == nil {
		session.finalized = true
		d.removeRuntimeSessionBinding(session.Binding.SessionUID)
	}
	return err
}

// taskSessionProjectionExecution overlays only the terminal classification on
// the Task's frozen execution identity. Session finalization projections are
// immutable reclamation evidence, so omitting the request digest or runtime
// identity would make the source PromptAttempt impossible to retire safely.
func taskSessionProjectionExecution(
	task *corev1alpha1.Task,
	terminal corev1alpha1.TaskExecutionStatus,
) (corev1alpha1.TaskExecutionStatus, error) {
	if task == nil || task.Status.Execution == nil {
		return corev1alpha1.TaskExecutionStatus{}, fmt.Errorf("task execution status is required for Session terminal projection")
	}
	if terminal.State == "" || terminal.Outcome == "" {
		return corev1alpha1.TaskExecutionStatus{}, fmt.Errorf("session terminal projection requires execution state and outcome")
	}
	current := task.Status.Execution
	if terminal.Attempt != 0 && terminal.Attempt != current.Attempt {
		return corev1alpha1.TaskExecutionStatus{}, fmt.Errorf("session terminal projection attempt does not match the Task")
	}
	if terminal.PromptID != "" && terminal.PromptID != current.PromptID {
		return corev1alpha1.TaskExecutionStatus{}, fmt.Errorf("session terminal projection prompt ID does not match the Task")
	}
	execution := *current.DeepCopy()
	execution.State = terminal.State
	execution.Outcome = terminal.Outcome
	execution.Reason = terminal.Reason
	execution.Message = terminal.Message
	return execution, nil
}

func (d *ACPDispatcher) currentRuntimeSessionBinding(sessionUID string) *ACPRuntimeSessionBinding {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.runtimeSessions == nil {
		return nil
	}
	binding, ok := d.runtimeSessions[sessionUID]
	if !ok {
		return nil
	}
	copy := binding
	return &copy
}

func (d *ACPDispatcher) setRuntimeSessionBinding(binding ACPRuntimeSessionBinding) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.runtimeSessions == nil {
		d.runtimeSessions = make(map[string]ACPRuntimeSessionBinding)
	}
	d.runtimeSessions[binding.SessionUID] = binding
}

func (d *ACPDispatcher) forgetRuntimeSessionBinding(sessionUID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.runtimeSessions, strings.TrimSpace(sessionUID))
}

func (d *ACPDispatcher) removeRuntimeSessionBinding(sessionUID string) {
	d.mu.Lock()
	delete(d.runtimeSessions, strings.TrimSpace(sessionUID))
	d.mu.Unlock()
}

// retireRecoveredRuntimeSessionBinding drops the in-memory RuntimeSession
// binding after recovered terminal settlement unless the RuntimeSession is
// known to stay live: a session-bound read RuntimeSession whose prompt
// succeeded remains resident on the supervisor after its turn finalizes, and
// its binding must survive so the next continuation Task plans a reuse
// instead of a controller-restart style recreation at a new generation with
// a full transcript bootstrap. Task-scoped sessions (no durable Session, or a
// write workspace) are retired with the Task, and any non-successful
// settlement (failed, cancelled, outcome unknown) schedules supervisor
// cleanup of the RuntimeSession, so those bindings are dropped exactly as the
// normal failure path does.
//
// Retention additionally requires a reusable terminal delivery state (a read
// prompt that succeeded but whose delivery failed, for example a modified
// read-only workspace, is cleaned up by the live failure paths) and a
// complete binding: a recovered session may carry only the SessionUID, and
// retaining a binding without a generation would make the next continuation
// fail planning instead of recreating the runtime.
func (d *ACPDispatcher) retireRecoveredRuntimeSessionBinding(task *corev1alpha1.Task, attempt *store.PromptAttempt, binding ACPRuntimeSessionBinding) {
	sessionUID := strings.TrimSpace(binding.SessionUID)
	reusable := task != nil && attempt != nil && sessionUID != "" &&
		attempt.ExecutionState == store.PromptExecutionSucceeded &&
		(attempt.DeliveryState == store.PromptDeliveryNotRequested || attempt.DeliveryState == store.PromptDeliveryReadValidated) &&
		task.Spec.SessionRef != nil &&
		(task.Spec.Workspace == nil || task.Spec.Workspace.Intent != corev1alpha1.WorkspaceIntentWrite)
	if reusable {
		if binding.Generation > 0 {
			return
		}
		// The recovered binding could not be rebuilt; the live binding, if
		// complete, is still authoritative and stays.
		if current := d.currentRuntimeSessionBinding(sessionUID); current != nil && current.Generation > 0 {
			return
		}
	}
	d.removeRuntimeSessionBinding(sessionUID)
}

func bootstrapPromptText(bootstrap *ACPBootstrapTranscript) string {
	if bootstrap == nil || bootstrap.MessageCount == 0 || len(bootstrap.Artifact) == 0 {
		return ""
	}
	return "Orka canonical session transcript (JSONL; provider-native history is non-authoritative):\n" + string(bootstrap.Artifact)
}
