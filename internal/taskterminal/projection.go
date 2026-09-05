package taskterminal

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

const (
	// ProjectionKind is the durable outbox projection for terminal Task status.
	ProjectionKind = "TaskTerminalStatus"

	// NoWorkspaceRevision is the protocol-only revision harness v2 records for
	// Tasks without a repository workspace. It is not a Git object ID: the
	// outbox projector strips it from delivery evidence before the
	// schema-validated Task status, while immutable projection payloads keep it.
	NoWorkspaceRevision = "empty"

	restoreIdentityChangedReason = corev1alpha1.TaskExecutionReason("RestoreIdentityChanged")
)

// Projection is the canonical payload of a terminal Task outbox projection.
type Projection struct {
	Namespace      string                             `json:"namespace"`
	Task           string                             `json:"task"`
	TaskUID        string                             `json:"taskUID"`
	Attempt        int32                              `json:"attempt"`
	Phase          corev1alpha1.TaskPhase             `json:"phase"`
	Message        string                             `json:"message,omitempty"`
	BindingDigest  string                             `json:"bindingDigest,omitempty"`
	HarnessRuntime *corev1alpha1.HarnessRuntimeStatus `json:"harnessRuntime,omitempty"`
	ResultRef      *corev1alpha1.ResultReference      `json:"resultRef,omitempty"`
	Execution      corev1alpha1.TaskExecutionStatus   `json:"execution"`
	Delivery       *corev1alpha1.TaskDeliveryStatus   `json:"delivery,omitempty"`
}

// ValidateRestoredProjection decodes and proves that one immutable source
// terminal projection is compatible with the restored Task incarnation and
// its final source PromptAttempt. Restore-only status text, controller epoch,
// and transition timestamps are deliberately not compared with the restored
// Task because the restored incarnation records a new settlement event.
func ValidateRestoredProjection(
	payload []byte,
	task *corev1alpha1.Task,
	sourceTaskUID string,
	attempt *store.PromptAttempt,
) (*Projection, error) {
	projection, err := decodeProjection(payload)
	if err != nil {
		return nil, err
	}
	return validateProjection(projection, task, sourceTaskUID, attempt)
}

// ValidateFinalizedSessionProjection validates a Session-backed terminal
// projection and its immutable SessionTurn. It accepts the one legacy payload
// shape produced before Session projections copied the frozen execution
// identity: every execution field other than the terminal classification was
// omitted. That compatibility path is authorized only by an exact finalized
// SessionTurn which pins this payload digest and the same PromptAttempt.
func ValidateFinalizedSessionProjection(
	payload []byte,
	task *corev1alpha1.Task,
	sourceTaskUID string,
	attempt *store.PromptAttempt,
	turn *store.SessionTurn,
) (*Projection, error) {
	projection, err := decodeProjection(payload)
	if err != nil {
		return nil, err
	}
	if err := validateFinalizedSessionTurn(payload, task, sourceTaskUID, attempt, turn); err != nil {
		return nil, err
	}
	if legacySessionExecutionIdentityOmitted(projection.Execution) {
		execution := *task.Status.Execution.DeepCopy()
		execution.State = projection.Execution.State
		execution.Outcome = projection.Execution.Outcome
		execution.Reason = projection.Execution.Reason
		execution.Attempt = projection.Execution.Attempt
		execution.PromptID = projection.Execution.PromptID
		execution.Message = projection.Execution.Message
		normalized := *projection
		normalized.Execution = execution
		projection = &normalized
	}
	return validateProjection(projection, task, sourceTaskUID, attempt)
}

//nolint:gocyclo // Keep the complete fail-closed terminal-payload contract in one auditable boundary.
func validateProjection(
	projection *Projection,
	task *corev1alpha1.Task,
	sourceTaskUID string,
	attempt *store.PromptAttempt,
) (*Projection, error) {
	if task == nil || task.Status.Execution == nil || attempt == nil {
		return nil, conflict("restored terminal projection evidence is incomplete")
	}
	sourceTaskUID = strings.TrimSpace(sourceTaskUID)
	binding := task.Status.AgentExecutionBinding
	if sourceTaskUID == "" {
		return nil, conflict("restored terminal projection source binding is invalid")
	}
	if string(task.UID) != sourceTaskUID {
		if binding == nil || binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
			string(binding.Task.UID) != sourceTaskUID {
			return nil, conflict("restored terminal projection source binding is invalid")
		}
	}
	if projection.HarnessRuntime != nil || projection.ResultRef != nil {
		return nil, conflict("restored harness v2 terminal projection contains harness v1 payload")
	}
	if projection.Namespace != task.Namespace || projection.Task != task.Name || projection.TaskUID != sourceTaskUID ||
		projection.Attempt < 1 || projection.Attempt != task.Status.Execution.Attempt ||
		int64(projection.Attempt) != attempt.Key.Attempt || attempt.Key.Namespace != task.Namespace ||
		attempt.Key.TaskUID != sourceTaskUID {
		return nil, conflict("restored terminal projection Task identity does not match its source attempt")
	}
	if projection.BindingDigest != "" && (binding == nil || projection.BindingDigest != binding.BindingDigest) {
		return nil, conflict("restored terminal projection binding digest does not match its Task")
	}
	if projection.Execution.Attempt != projection.Attempt ||
		projection.Execution.PromptID == "" || projection.Execution.PromptID != task.Status.Execution.PromptID ||
		projection.Execution.PromptID != attempt.Key.PromptID {
		return nil, conflict("restored terminal projection execution identity does not match its source attempt")
	}
	if attempt.RequestDigest != "" && projection.Execution.RequestDigest != attempt.RequestDigest {
		return nil, conflict("restored terminal projection request digest does not match its source attempt")
	}
	if task.Status.Execution.RequestDigest != "" && projection.Execution.RequestDigest != task.Status.Execution.RequestDigest {
		return nil, conflict("restored terminal projection request digest does not match its Task")
	}
	wantState, wantOutcome, ok := terminalExecution(attempt.ExecutionState)
	if !ok || projection.Execution.State != wantState || projection.Execution.Outcome != wantOutcome {
		return nil, conflict("restored terminal projection execution outcome does not match its source attempt")
	}
	if projection.Delivery == nil || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) ||
		store.PromptDeliveryState(projection.Delivery.State) != attempt.DeliveryState ||
		string(projection.Delivery.Outcome) != string(attempt.DeliveryState) {
		return nil, conflict("restored terminal projection delivery outcome does not match its source attempt")
	}
	if projection.Phase != terminalPhase(wantState, attempt.DeliveryState) {
		return nil, conflict("restored terminal projection phase does not match its terminal outcome")
	}
	if task.Status.Delivery == nil || !equalDeliveryEvidence(task, projection.Delivery, task.Status.Delivery) {
		return nil, conflict("restored terminal projection delivery evidence does not match its Task")
	}
	if err := validateRuntimeIdentity(projection.Execution, *task.Status.Execution, *attempt); err != nil {
		return nil, err
	}
	projectionRestoreReason := projection.Execution.Reason == restoreIdentityChangedReason
	attemptRestoreReason := attempt.TerminalReason == string(restoreIdentityChangedReason)
	if projectionRestoreReason != attemptRestoreReason {
		return nil, conflict("restored terminal projection restore marker does not match its source attempt")
	}
	if projectionRestoreReason && (strings.TrimSpace(attempt.OutcomeMarker) == "" ||
		projection.Execution.Message != attempt.OutcomeMarker || projection.Message != attempt.OutcomeMarker) {
		return nil, conflict("restored terminal projection restore message does not match its source attempt")
	}
	return projection, nil
}

func validateFinalizedSessionTurn(
	payload []byte,
	task *corev1alpha1.Task,
	sourceTaskUID string,
	attempt *store.PromptAttempt,
	turn *store.SessionTurn,
) error {
	if task == nil || task.Status.Execution == nil || attempt == nil || turn == nil {
		return conflict("finalized Session terminal projection evidence is incomplete")
	}
	if task.Spec.SessionRef == nil || turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil ||
		turn.ProjectionID == "" || turn.ProjectionKind != ProjectionKind ||
		turn.ProjectionDigest != fmt.Sprintf("sha256:%x", sha256.Sum256(payload)) {
		return conflict("finalized Session terminal projection is not pinned by its SessionTurn")
	}
	canonicalID, err := turn.Key.CanonicalID()
	if err != nil || canonicalID != turn.ID {
		return conflict("finalized SessionTurn identity is invalid")
	}
	sourceTaskUID = strings.TrimSpace(sourceTaskUID)
	// turn.Key.LeaseGeneration is the per-prompt Session mutation-lease
	// generation and is fenced against the PromptAttempt's recorded lease.
	// Task.Status.Execution.RuntimeSessionGeneration is the RuntimeSession
	// incarnation generation — a different counter that only coincides with
	// the lease generation for a session's first prompt — so the turn binds to
	// the Task through the session UID, never through that generation.
	if turn.Key.TaskUID != sourceTaskUID || turn.Key.Attempt != attempt.Key.Attempt ||
		turn.Key.PromptID != attempt.Key.PromptID || turn.PromptAttemptID != attempt.ID ||
		turn.Key.SessionUID != attempt.SessionUID || turn.Key.LeaseGeneration != attempt.SessionLeaseGeneration ||
		turn.Key.SessionUID != task.Status.Execution.RuntimeSessionUID {
		return conflict("finalized SessionTurn does not match its Task and PromptAttempt")
	}
	wantTerminalKind := store.SessionTurnOutcomeMarker
	if attempt.ExecutionState == store.PromptExecutionSucceeded {
		wantTerminalKind = store.SessionTurnAssistantResult
	}
	if turn.TerminalKind != wantTerminalKind {
		return conflict("finalized SessionTurn terminal kind does not match its PromptAttempt")
	}
	return nil
}

func legacySessionExecutionIdentityOmitted(execution corev1alpha1.TaskExecutionStatus) bool {
	execution.State = ""
	execution.Outcome = ""
	execution.Reason = ""
	execution.Attempt = 0
	execution.PromptID = ""
	execution.Message = ""
	return reflect.DeepEqual(execution, corev1alpha1.TaskExecutionStatus{})
}

func decodeProjection(payload []byte) (*Projection, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	projection := &Projection{}
	if err := decoder.Decode(projection); err != nil {
		return nil, conflict("decode restored terminal projection: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, conflict("restored terminal projection contains trailing data")
	}
	return projection, nil
}

func terminalExecution(state store.PromptExecutionState) (corev1alpha1.TaskExecutionState, corev1alpha1.TaskExecutionOutcome, bool) {
	switch state {
	case store.PromptExecutionSucceeded:
		return corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded, true
	case store.PromptExecutionFailed:
		return corev1alpha1.TaskExecutionStateFailed, corev1alpha1.TaskExecutionOutcomeFailed, true
	case store.PromptExecutionCancelled:
		return corev1alpha1.TaskExecutionStateCancelled, corev1alpha1.TaskExecutionOutcomeCancelled, true
	case store.PromptExecutionOutcomeUnknown:
		return corev1alpha1.TaskExecutionStateOutcomeUnknown, corev1alpha1.TaskExecutionOutcomeOutcomeUnknown, true
	default:
		return "", "", false
	}
}

func terminalPhase(state corev1alpha1.TaskExecutionState, delivery store.PromptDeliveryState) corev1alpha1.TaskPhase {
	if state == corev1alpha1.TaskExecutionStateCancelled {
		return corev1alpha1.TaskPhaseCancelled
	}
	if state == corev1alpha1.TaskExecutionStateSucceeded {
		switch delivery {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated, store.PromptDeliveryNoChange,
			store.PromptDeliveryVerifiedExact, store.PromptDeliveryDeliveredSuperseded:
			return corev1alpha1.TaskPhaseSucceeded
		case store.PromptDeliveryCancelledBeforePublish:
			return corev1alpha1.TaskPhaseCancelled
		}
	}
	return corev1alpha1.TaskPhaseFailed
}

func equalDeliveryEvidence(task *corev1alpha1.Task, left, right *corev1alpha1.TaskDeliveryStatus) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftCopy := left.DeepCopy()
	rightCopy := right.DeepCopy()
	leftCopy.State, rightCopy.State = "", ""
	leftCopy.Outcome, rightCopy.Outcome = "", ""
	leftCopy.LastTransitionTime, rightCopy.LastTransitionTime = nil, nil
	// The projector strips the protocol-only no-workspace revision before the
	// schema-validated Task status while the immutable payload keeps it, so
	// evidence is compared through that same normalization. Workspace intent
	// and options alone do not establish a repository baseline.
	if task != nil && (task.Spec.Workspace == nil || strings.TrimSpace(task.Spec.Workspace.GitRepo) == "") {
		if leftCopy.StartingSHA == NoWorkspaceRevision {
			leftCopy.StartingSHA = ""
		}
		if rightCopy.StartingSHA == NoWorkspaceRevision {
			rightCopy.StartingSHA = ""
		}
	}
	return reflect.DeepEqual(leftCopy, rightCopy)
}

func validateRuntimeIdentity(projected, task corev1alpha1.TaskExecutionStatus, attempt store.PromptAttempt) error {
	if projected.RuntimePoolName != task.RuntimePoolName || projected.RuntimePoolUID != task.RuntimePoolUID ||
		projected.AgentRuntimeName != task.AgentRuntimeName || projected.AgentRuntimeUID != task.AgentRuntimeUID ||
		projected.RuntimeInstanceID != task.RuntimeInstanceID || projected.RuntimeSessionUID != task.RuntimeSessionUID ||
		projected.RuntimeSessionGeneration != task.RuntimeSessionGeneration ||
		projected.RuntimeSessionSupervisorBootID != task.RuntimeSessionSupervisorBootID ||
		projected.RuntimeSessionProfileDigest != task.RuntimeSessionProfileDigest ||
		projected.RuntimeSessionMCPDigest != task.RuntimeSessionMCPDigest ||
		projected.RuntimeSessionWorkspaceDigest != task.RuntimeSessionWorkspaceDigest {
		return conflict("restored terminal projection runtime identity does not match its Task")
	}
	if attempt.RuntimeInstanceID != "" && projected.RuntimeInstanceID != attempt.RuntimeInstanceID {
		return conflict("restored terminal projection runtime instance does not match its source attempt")
	}
	// The projection records the RuntimeSession incarnation generation while
	// the attempt records the per-prompt Session mutation-lease generation;
	// they only coincide for a session's first prompt, so the attempt binding
	// is fenced by session UID here and by lease generation on the finalized
	// SessionTurn.
	if attempt.SessionUID != "" && projected.RuntimeSessionUID != attempt.SessionUID {
		return conflict("restored terminal projection runtime Session does not match its source attempt")
	}
	return nil
}

func conflict(format string, args ...any) error {
	return fmt.Errorf("%w: %s", store.ErrConflict, fmt.Sprintf(format, args...))
}
