package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

// permanentHarnessV1PreSubmitSessionError marks a Session identity conflict
// that immutable control state proves cannot succeed on retry. Storage,
// controller-epoch, and failed re-read conflicts intentionally remain untyped
// so the Prepared attempt can be retried.
type permanentHarnessV1PreSubmitSessionError struct{ err error }

func (e *permanentHarnessV1PreSubmitSessionError) Error() string { return e.err.Error() }
func (e *permanentHarnessV1PreSubmitSessionError) Unwrap() error { return e.err }

func permanentHarnessV1PreSubmitSession(err error) error {
	if err == nil || isPermanentHarnessV1PreSubmitSessionError(err) {
		return err
	}
	return &permanentHarnessV1PreSubmitSessionError{err: err}
}

func isPermanentHarnessV1PreSubmitSessionError(err error) bool {
	var permanent *permanentHarnessV1PreSubmitSessionError
	return errors.As(err, &permanent)
}

func harnessV1SessionAttemptReference(attempt *store.HarnessV1Attempt) string {
	if attempt == nil {
		return ""
	}
	return harnessV1AttemptKey(attempt).SessionReferenceID()
}

func harnessV1SessionRuntimeIdentity(binding *corev1alpha1.AgentExecutionBinding) string {
	if binding == nil {
		return ""
	}
	if binding.RuntimeRef != nil && binding.RuntimeRef.UID != "" {
		return string(binding.RuntimeRef.UID)
	}
	return string(binding.RuntimeType)
}

func harnessV1SessionLineageConfigDigest(binding *corev1alpha1.AgentExecutionBinding) (string, error) {
	if binding == nil {
		return "", errors.New("execution binding is required for Session lineage")
	}
	if store.ValidateCanonicalDigest("runtime profile digest", binding.RuntimeProfileDigest) == nil {
		return binding.RuntimeProfileDigest, nil
	}
	// Harness v1 has no managed RuntimeProfile. Commit to the immutable route,
	// runtime, and Agent identities while excluding turn-local prompt fields.
	return acpDomainDigest("agent-execution-session-lineage-config/v1", struct {
		Contract   corev1alpha1.AgentRuntimeContractVersion `json:"contract"`
		Backend    corev1alpha1.AgentExecutionBackend       `json:"backend"`
		Runtime    corev1alpha1.AgentRuntimeType            `json:"runtime,omitempty"`
		RuntimeRef *corev1alpha1.AgentExecutionRuntimeRef   `json:"runtimeRef,omitempty"`
		Agent      *corev1alpha1.AgentExecutionAgentRef     `json:"agent,omitempty"`
	}{binding.ContractVersion, binding.Backend, binding.RuntimeType, binding.RuntimeRef, binding.Agent})
}

func (d *HarnessV1Dispatcher) prepareHarnessV1TaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	verified *verifiedHarnessV1Execution,
	attempt *store.HarnessV1Attempt,
	fence store.ControllerEpochFence,
) error {
	if verified == nil || verified.frozenTask == nil || verified.frozenTask.Spec.SessionRef == nil {
		return nil
	}
	if d.Sessions == nil {
		return errors.New("harness v1 Session continuity is not configured")
	}
	ref := verified.frozenTask.Spec.SessionRef
	name := strings.TrimSpace(ref.Name)
	if name == "" {
		return store.ValidationErrorf("harness v1 SessionRef name is required")
	}
	if verified.body.HarnessV1 == nil || verified.body.HarnessV1.SessionName != name ||
		verified.body.HarnessV1.SessionBootstrap == nil {
		return fmt.Errorf("%w: frozen harness v1 Session identity is incomplete", store.ErrConflict)
	}
	bootstrap := verified.body.HarnessV1.SessionBootstrap
	transcriptBackedPrompt := ref.PromptIncluded && strings.TrimSpace(ref.ThroughMessageID) != ""
	if !ref.Create && !transcriptBackedPrompt {
		if _, err := d.Sessions.controls.GetSessionControl(ctx, task.Namespace, name); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("session %s/%s does not exist and create=false: %w", task.Namespace, name, store.ErrNotFound)
			}
			return err
		}
	}
	sessionType := defaultACPSessionType
	if ref.PromptIncluded && strings.HasPrefix(ref.ThroughMessageID, "gateway:") {
		sessionType = store.SessionTypeGateway
	}
	control, err := d.Sessions.EnsureSession(ctx, ACPEnsureSessionRequest{
		Namespace:                 task.Namespace,
		SessionName:               name,
		SessionType:               sessionType,
		ExpectedSessionUID:        bootstrap.SessionUID,
		RequireExistingTranscript: transcriptBackedPrompt && !ref.Create,
		Fence:                     fence,
		CreatedAt:                 time.Now().UTC(),
	})
	if err != nil {
		return d.classifyHarnessV1PreSubmitSessionConflict(
			ctx, task.Namespace, name, bootstrap, task, attempt, err,
		)
	}
	if err := validateFrozenHarnessV1SessionControl(bootstrap, control, task, attempt); err != nil {
		return err
	}
	expiresAt := time.Now().UTC().Add(defaultHarnessV1TurnTimeout + time.Minute)
	if verified.frozenTask.Spec.Timeout != nil && verified.frozenTask.Spec.Timeout.Duration > 0 {
		expiresAt = time.Now().UTC().Add(verified.frozenTask.Spec.Timeout.Duration + time.Minute)
	}
	lineageConfigDigest, err := harnessV1SessionLineageConfigDigest(verified.binding)
	if err != nil {
		return err
	}
	control, err = d.Sessions.controls.GetSessionControl(ctx, task.Namespace, name)
	if err != nil {
		return fmt.Errorf("recheck frozen harness v1 Session control before lease acquisition: %w", err)
	}
	if err := validateFrozenHarnessV1SessionControl(bootstrap, control, task, attempt); err != nil {
		return err
	}
	lease, err := d.Sessions.AcquireMutationLease(ctx, ACPAcquireSessionLeaseRequest{
		Session:             *control,
		Fence:               fence,
		TaskName:            task.Name,
		TaskUID:             string(task.UID),
		Attempt:             int64(attempt.Attempt),
		PromptID:            attempt.TurnID,
		PromptRequestDigest: attempt.RequestDigest,
		AcquiredAt:          time.Now().UTC(),
		ExpiresAt:           &expiresAt,
		NamespaceUID:        string(verified.binding.Task.NamespaceUID),
		ContractVersion:     corev1alpha1.AgentRuntimeContractHarnessV1,
		LineageGeneration:   1,
		RuntimeIdentity:     harnessV1SessionRuntimeIdentity(verified.binding),
		ConfigDigest:        lineageConfigDigest,
	})
	if err != nil {
		return d.classifyHarnessV1PreSubmitSessionConflict(
			ctx, task.Namespace, name, bootstrap, task, attempt, err,
		)
	}
	appendPolicy := acpSessionTranscriptAppendPolicyForTask(verified.frozenTask)
	_, err = d.Sessions.OpenTurn(ctx, ACPOpenSessionTurnRequest{
		Lease:                *lease,
		Fence:                fence,
		PromptAttemptID:      harnessV1SessionAttemptReference(attempt),
		PromptRequestDigest:  attempt.RequestDigest,
		UserPrompt:           verified.frozenTask.Spec.Prompt,
		SkipTranscriptAppend: appendPolicy.skipTranscriptAppend,
		SkipUserPromptAppend: appendPolicy.skipUserPromptAppend,
		OpenedAt:             time.Now().UTC(),
	})
	return err
}

// classifyHarnessV1PreSubmitSessionConflict promotes only a conflict whose
// current authoritative control proves that the frozen Session identity was
// lost. In particular, an unchanged control after a lease-acquisition conflict
// can be an API or controller-epoch race and must remain retryable.
func (d *HarnessV1Dispatcher) classifyHarnessV1PreSubmitSessionConflict(
	ctx context.Context,
	namespace, name string,
	bootstrap *agentExecutionSnapshotHarnessV1SessionBootstrap,
	task *corev1alpha1.Task,
	attempt *store.HarnessV1Attempt,
	cause error,
) error {
	if cause == nil || !errors.Is(cause, store.ErrConflict) {
		return cause
	}
	current, err := d.Sessions.controls.GetSessionControl(ctx, namespace, name)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("recheck harness v1 Session control after conflict: %w", err))
	}
	if err := validateFrozenHarnessV1SessionControl(bootstrap, current, task, attempt); err != nil {
		return err
	}
	return cause
}

func validateFrozenHarnessV1SessionControl(
	bootstrap *agentExecutionSnapshotHarnessV1SessionBootstrap,
	control *store.SessionControl,
	task *corev1alpha1.Task,
	attempt *store.HarnessV1Attempt,
) error {
	if bootstrap == nil || control == nil || task == nil || attempt == nil ||
		control.SessionUID != bootstrap.SessionUID || control.Availability != store.SessionAvailable {
		return permanentHarnessV1PreSubmitSession(fmt.Errorf(
			"%w: current harness v1 Session control does not match the frozen available identity", store.ErrConflict,
		))
	}
	if attempt.Attempt < 1 {
		return permanentHarnessV1PreSubmitSession(fmt.Errorf(
			"%w: harness v1 Session attempt number must be positive", store.ErrConflict,
		))
	}
	initialVersion := bootstrap.ControlVersion
	if bootstrap.ControlVersion == 0 {
		initialVersion = 1
	}
	priorAttempts := int64(attempt.Attempt - 1)
	expectedVersion := initialVersion + 2*priorAttempts
	expectedGeneration := bootstrap.LeaseGeneration + priorAttempts
	if priorAttempts > 0 {
		priorTurnID, err := store.SessionTurnKey{
			SessionUID:      bootstrap.SessionUID,
			LeaseGeneration: expectedGeneration,
			TaskUID:         string(task.UID),
			Attempt:         priorAttempts,
			PromptID:        string(harnessV1TurnID(task, attempt.Attempt-1)),
		}.CanonicalID()
		if err != nil {
			return err
		}
		if control.LastOperationID != "finalize:"+priorTurnID {
			return permanentHarnessV1PreSubmitSession(fmt.Errorf(
				"%w: harness v1 Session retry does not follow its own finalized turn", store.ErrConflict,
			))
		}
	}
	if control.Lease == nil {
		if control.Version == expectedVersion && control.LeaseGeneration == expectedGeneration {
			return nil
		}
		return permanentHarnessV1PreSubmitSession(fmt.Errorf(
			"%w: harness v1 Session control advanced after transcript freeze", store.ErrConflict,
		))
	}
	expectedGeneration++
	expectedDigest, err := acpSessionMutationLeaseDigest(
		bootstrap.SessionUID, expectedGeneration, string(task.UID), int64(attempt.Attempt),
		attempt.TurnID, attempt.RequestDigest,
	)
	if err != nil {
		return err
	}
	lease := control.Lease
	if control.Version != expectedVersion+1 || control.LeaseGeneration != expectedGeneration ||
		lease.Generation != expectedGeneration || lease.TaskUID != string(task.UID) ||
		lease.Attempt != int64(attempt.Attempt) || lease.PromptID != attempt.TurnID ||
		lease.RequestDigest != expectedDigest {
		return permanentHarnessV1PreSubmitSession(fmt.Errorf(
			"%w: harness v1 Session is leased by stale or foreign work", store.ErrConflict,
		))
	}
	return nil
}

func (d *HarnessV1Dispatcher) recoverHarnessV1TaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	verified *verifiedHarnessV1Execution,
	attempt *store.HarnessV1Attempt,
) (*ACPSessionTurn, error) {
	if verified == nil || verified.frozenTask == nil || verified.frozenTask.Spec.SessionRef == nil {
		return nil, nil
	}
	if d.Sessions == nil {
		return nil, errors.New("harness v1 Session continuity is not configured")
	}
	name := strings.TrimSpace(verified.frozenTask.Spec.SessionRef.Name)
	control, err := d.Sessions.controls.GetSessionControl(ctx, task.Namespace, name)
	if err != nil {
		return nil, err
	}
	lineageConfigDigest, err := harnessV1SessionLineageConfigDigest(verified.binding)
	if err != nil {
		return nil, err
	}
	lineage := control.Lineage
	if lineage == nil || lineage.ContractVersion != string(corev1alpha1.AgentRuntimeContractHarnessV1) ||
		lineage.NamespaceUID != string(verified.binding.Task.NamespaceUID) ||
		lineage.RuntimeIdentity != harnessV1SessionRuntimeIdentity(verified.binding) ||
		lineage.ConfigDigest != lineageConfigDigest {
		return nil, fmt.Errorf("%w: harness v1 Session lineage does not match the immutable Task binding", store.ErrConflict)
	}
	key := store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: string(task.UID), Attempt: int64(attempt.Attempt), PromptID: attempt.TurnID,
	}
	turnID, err := key.CanonicalID()
	if err != nil {
		return nil, err
	}
	turn, err := d.Sessions.controls.GetSessionTurn(ctx, turnID)
	if err != nil {
		return nil, err
	}
	if turn.PromptAttemptID != harnessV1SessionAttemptReference(attempt) || turn.Key != key {
		return nil, fmt.Errorf("%w: harness v1 SessionTurn identity does not match the durable attempt", store.ErrConflict)
	}
	lease := ACPSessionLease{Session: *control, Key: key}
	if turn.State == store.SessionTurnOpen {
		if err := validateACPSessionLease(control, key); err != nil {
			return nil, err
		}
	}
	appendPolicy := acpSessionTranscriptAppendPolicyForTask(verified.frozenTask)
	return &ACPSessionTurn{
		Lease: lease, Turn: *turn,
		SkipTranscriptAppend: appendPolicy.skipTranscriptAppend,
		SkipUserPromptAppend: appendPolicy.skipUserPromptAppend,
	}, nil
}

func harnessV1SessionCanBeAbsent(attempt *store.HarnessV1Attempt) bool {
	if attempt == nil || attempt.TerminalReceiptDigest != "" {
		return false
	}
	if attempt.State == store.HarnessV1AttemptCancelled {
		return true
	}
	if attempt.State != store.HarnessV1AttemptRejected {
		return false
	}
	switch attempt.TerminalReason {
	case harnessV1ReasonBackendDisabled, harnessV1ReasonCredentialChanged, harnessV1ReasonInvalidBinding,
		harnessV1ReasonSessionConflict:
		return true
	default:
		return false
	}
}

func harnessV1RuntimeProjection(
	task *corev1alpha1.Task,
	attempt *store.HarnessV1Attempt,
	message string,
) *corev1alpha1.HarnessRuntimeStatus {
	projected := &corev1alpha1.HarnessRuntimeStatus{}
	if task.Status.HarnessRuntime != nil {
		projected = task.Status.HarnessRuntime.DeepCopy()
	}
	state, outcome, _ := harnessV1TaskProjection(attempt.State)
	projected.Attempt = attempt.Attempt
	projected.TurnID = attempt.TurnID
	projected.RuntimeSessionID = attempt.RuntimeSessionID
	projected.State = state
	projected.Outcome = outcome
	projected.Reason = attempt.TerminalReason
	projected.TerminalReceiptDigest = attempt.TerminalReceiptDigest
	projected.RequestDigest = attempt.RequestDigest
	projected.ControllerEpoch = attempt.ControllerEpoch
	projected.LastEventSeq = attempt.LastEventSeq
	projected.Message = message
	if attempt.CancelRequestedAt != nil {
		when := metav1.NewTime(attempt.CancelRequestedAt.UTC())
		projected.CancelRequestedAt = &when
	} else {
		projected.CancelRequestedAt = nil
	}
	return projected
}

func (d *HarnessV1Dispatcher) finalizeHarnessV1TaskSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	verified *verifiedHarnessV1Execution,
	attempt *store.HarnessV1Attempt,
	fence store.ControllerEpochFence,
	retrying bool,
) (bool, error) {
	if verified == nil || verified.frozenTask == nil || verified.frozenTask.Spec.SessionRef == nil {
		return false, nil
	}
	// SessionConflict is emitted only when preparation proves the frozen
	// identity was lost before this attempt acquired a lease or opened a turn.
	// Recovery against the winning control would itself conflict, so skip it.
	if attempt != nil && attempt.State == store.HarnessV1AttemptRejected &&
		attempt.TerminalReason == harnessV1ReasonSessionConflict && attempt.TerminalReceiptDigest == "" {
		return false, nil
	}
	sessionTurn, err := d.recoverHarnessV1TaskSession(ctx, task, verified, attempt)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) && harnessV1SessionCanBeAbsent(attempt) {
			return false, nil
		}
		return false, err
	}
	if sessionTurn == nil {
		return false, nil
	}
	if sessionTurn.Turn.State == store.SessionTurnFinalized {
		_, err := d.Sessions.ResumeSessionTurnFinalization(ctx, ACPResumeSessionTurnFinalizationRequest{
			SessionTurn: *sessionTurn, Fence: fence,
		})
		return true, err
	}
	message := d.terminalMessage(attempt)
	_, _, phase := harnessV1TaskProjection(attempt.State)
	if retrying {
		phase = corev1alpha1.TaskPhaseRunning
		sessionTurn.SkipTranscriptAppend = true
		sessionTurn.SkipUserPromptAppend = false
		message = "harness v1 attempt settled; a safe retry is pending"
	}
	payload, err := json.Marshal(taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: attempt.Attempt,
		Phase: phase, Message: message, BindingDigest: attempt.BindingDigest,
		HarnessRuntime: harnessV1RuntimeProjection(task, attempt, message), ResultRef: task.Status.ResultRef.DeepCopy(),
	})
	if err != nil {
		return false, err
	}
	projection := ACPFinalizationProjection{
		ProjectionKind: "TaskTerminalStatus", Payload: payload, AvailableAt: time.Now().UTC(),
	}
	finalizedAt := time.Now().UTC()
	switch attempt.State {
	case store.HarnessV1AttemptSucceeded:
		result, err := d.ResultStore.GetResult(ctx, task.Namespace, task.Name)
		if err != nil {
			return false, err
		}
		_, err = d.Sessions.FinalizeAssistantResult(ctx, ACPFinalizeAssistantRequest{
			SessionTurn: *sessionTurn, Fence: fence, AssistantResult: string(result),
			Projection: projection, FinalizedAt: finalizedAt,
		})
		return true, err
	case store.HarnessV1AttemptOutcomeUnknown:
		reason := strings.TrimSpace(attempt.TerminalReason)
		if reason == "" {
			reason = harnessV1OutcomeUnknownMessage
		}
		_, err = d.Sessions.FinalizeOutcomeUnknown(ctx, ACPFinalizeOutcomeUnknownRequest{
			SessionTurn: *sessionTurn, Fence: fence, Reason: reason,
			Projection: projection, FinalizedAt: finalizedAt,
		})
		return true, err
	default:
		reason := strings.TrimSpace(attempt.TerminalReason)
		if reason == "" {
			reason = string(attempt.State)
		}
		_, err = d.Sessions.FinalizeOutcomeMarker(ctx, ACPFinalizeOutcomeMarkerRequest{
			SessionTurn: *sessionTurn, Fence: fence, Kind: string(attempt.State), Reason: reason,
			Projection: projection, FinalizedAt: finalizedAt,
		})
		return true, err
	}
}
