package taskterminal

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestValidateRestoredProjectionAcceptsExactSourceEvidence(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()

	if _, err := ValidateRestoredProjection(marshalProjection(t, projection), task, sourceUID, attempt); err != nil {
		t.Fatalf("ValidateRestoredProjection() error = %v", err)
	}
}

func TestValidateRestoredProjectionAcceptsDocumentedDeliverySettlementRewrite(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	attempt.DeliveryState = store.PromptDeliveryPublicationOutcomeUnknown
	projection.Phase = corev1alpha1.TaskPhaseFailed
	projection.Delivery.State = corev1alpha1.TaskDeliveryStatePublicationOutcomeUnknown
	projection.Delivery.Outcome = corev1alpha1.TaskDeliveryOutcomePublicationOutcomeUnknown
	projection.Delivery.LastTransitionTime = timePtr(time.Date(2026, 8, 11, 12, 5, 0, 0, time.UTC))
	task.Status.Delivery.State = corev1alpha1.TaskDeliveryStatePreparing
	task.Status.Delivery.Outcome = ""
	task.Status.Delivery.LastTransitionTime = timePtr(time.Date(2026, 8, 11, 12, 4, 0, 0, time.UTC))

	if _, err := ValidateRestoredProjection(marshalProjection(t, projection), task, sourceUID, attempt); err != nil {
		t.Fatalf("ValidateRestoredProjection(documented settlement rewrite) error = %v", err)
	}
}

func TestValidateRestoredProjectionAcceptsExactRestoreMarker(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	const message = "Task incarnation changed during restore after source execution reached a durable terminal state; execution was preserved and was not replayed"
	attempt.TerminalReason = string(restoreIdentityChangedReason)
	attempt.OutcomeMarker = message
	projection.Execution.Reason = restoreIdentityChangedReason
	projection.Execution.Message = message
	projection.Message = message

	if _, err := ValidateRestoredProjection(marshalProjection(t, projection), task, sourceUID, attempt); err != nil {
		t.Fatalf("ValidateRestoredProjection(restore marker) error = %v", err)
	}
}

func TestValidateFinalizedSessionProjectionAcceptsPinnedLegacySparseExecution(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-transcript", Create: true}
	legacyExecution := corev1alpha1.TaskExecutionStatus{
		State: projection.Execution.State, Outcome: projection.Execution.Outcome,
		Reason: projection.Execution.Reason, Attempt: projection.Execution.Attempt,
		PromptID: projection.Execution.PromptID, Message: projection.Execution.Message,
	}
	projection.Execution = legacyExecution
	payload := marshalProjection(t, projection)
	turn := finalizedSessionProjectionTurn(t, payload, attempt)

	validated, err := ValidateFinalizedSessionProjection(payload, task, sourceUID, attempt, turn)
	if err != nil {
		t.Fatalf("ValidateFinalizedSessionProjection() error = %v", err)
	}
	if validated.Execution.RequestDigest != attempt.RequestDigest ||
		validated.Execution.RuntimeSessionUID != attempt.SessionUID ||
		validated.Execution.RuntimeSessionGeneration != attempt.SessionLeaseGeneration {
		t.Fatalf("legacy Session projection did not recover frozen execution identity: %#v", validated.Execution)
	}
}

func TestValidateFinalizedSessionProjectionAcceptsContinuationLeaseGeneration(t *testing.T) {
	// A session continuation prompt runs under a later mutation-lease
	// generation than the RuntimeSession incarnation generation frozen in the
	// Task execution identity. Reclamation must still validate, or every
	// continuation Task becomes undeletable.
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-transcript", Create: false}
	attempt.SessionLeaseGeneration = task.Status.Execution.RuntimeSessionGeneration + 1
	payload := marshalProjection(t, projection)
	turn := finalizedSessionProjectionTurn(t, payload, attempt)

	if _, err := ValidateFinalizedSessionProjection(payload, task, sourceUID, attempt, turn); err != nil {
		t.Fatalf("ValidateFinalizedSessionProjection(continuation lease) error = %v", err)
	}
}

func TestValidateFinalizedSessionProjectionAcceptsStrippedNoWorkspaceRevision(t *testing.T) {
	// A Task without a repository workspace freezes the protocol-only "empty"
	// revision in its projected delivery evidence, while the outbox projector
	// strips that value before the schema-validated Task status. Reclamation
	// must compare through the same normalization, or every no-workspace Task
	// with delivery evidence becomes undeletable.
	for _, tt := range []struct {
		name      string
		workspace *corev1alpha1.WorkspaceConfig
	}{
		{name: "omitted workspace"},
		{name: "empty workspace", workspace: &corev1alpha1.WorkspaceConfig{}},
		{name: "read workspace without repository", workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task, sourceUID, attempt, projection := restoredProjectionFixture()
			task.Spec.Workspace = tt.workspace
			task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-transcript", Create: true}
			attempt.DeliveryState = store.PromptDeliveryReadValidated
			projection.Delivery = &corev1alpha1.TaskDeliveryStatus{
				State:       corev1alpha1.TaskDeliveryStateReadValidated,
				Outcome:     corev1alpha1.TaskDeliveryOutcomeReadValidated,
				StartingSHA: NoWorkspaceRevision,
			}
			task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{
				State:   corev1alpha1.TaskDeliveryStateReadValidated,
				Outcome: corev1alpha1.TaskDeliveryOutcomeReadValidated,
			}
			payload := marshalProjection(t, projection)
			turn := finalizedSessionProjectionTurn(t, payload, attempt)

			if _, err := ValidateFinalizedSessionProjection(payload, task, sourceUID, attempt, turn); err != nil {
				t.Fatalf("ValidateFinalizedSessionProjection(no-repository revision) error = %v", err)
			}
			if _, err := ValidateRestoredProjection(payload, task, sourceUID, attempt); err != nil {
				t.Fatalf("ValidateRestoredProjection(no-repository revision) error = %v", err)
			}
			if projection.Delivery.StartingSHA != NoWorkspaceRevision || task.Status.Delivery.StartingSHA != "" {
				t.Fatal("projection validation changed immutable delivery evidence")
			}
		})
	}
}

func TestValidateRestoredProjectionRejectsNoWorkspaceRevisionForWorkspaceTask(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{GitRepo: "https://github.com/example/repo.git"}
	projection.Delivery.StartingSHA = NoWorkspaceRevision
	task.Status.Delivery.StartingSHA = ""

	if _, err := ValidateRestoredProjection(marshalProjection(t, projection), task, sourceUID, attempt); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("ValidateRestoredProjection(workspace task with protocol revision) error = %v, want ErrConflict", err)
	}
}

func TestValidateFinalizedSessionProjectionRejectsLeaseGenerationDrift(t *testing.T) {
	task, sourceUID, attempt, projection := restoredProjectionFixture()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-transcript", Create: false}
	payload := marshalProjection(t, projection)
	turn := finalizedSessionProjectionTurn(t, payload, attempt)
	// Drift between the finalized turn and its attempt must stay fenced even
	// though the Task's incarnation generation is no longer compared.
	attempt.SessionLeaseGeneration++

	if _, err := ValidateFinalizedSessionProjection(payload, task, sourceUID, attempt, turn); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("ValidateFinalizedSessionProjection(lease drift) error = %v, want ErrConflict", err)
	}
}

func TestValidateFinalizedSessionProjectionRejectsUnpinnedOrPartialLegacyPayload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1alpha1.Task, *store.PromptAttempt, *Projection, *store.SessionTurn)
	}{
		{name: "no Session reference", mutate: func(task *corev1alpha1.Task, _ *store.PromptAttempt, _ *Projection, _ *store.SessionTurn) {
			task.Spec.SessionRef = nil
		}},
		{name: "projection digest", mutate: func(_ *corev1alpha1.Task, _ *store.PromptAttempt, _ *Projection, turn *store.SessionTurn) {
			turn.ProjectionDigest = digest("different-payload")
		}},
		{name: "prompt attempt identity", mutate: func(_ *corev1alpha1.Task, _ *store.PromptAttempt, _ *Projection, turn *store.SessionTurn) {
			turn.PromptAttemptID = "different-attempt"
		}},
		{name: "Session identity", mutate: func(_ *corev1alpha1.Task, _ *store.PromptAttempt, _ *Projection, turn *store.SessionTurn) {
			turn.Key.SessionUID = "different-session"
		}},
		{name: "terminal kind", mutate: func(_ *corev1alpha1.Task, _ *store.PromptAttempt, _ *Projection, turn *store.SessionTurn) {
			turn.TerminalKind = store.SessionTurnOutcomeMarker
		}},
		{name: "partial request identity", mutate: func(_ *corev1alpha1.Task, _ *store.PromptAttempt, projection *Projection, _ *store.SessionTurn) {
			projection.Execution.RequestDigest = digest("different-request")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, sourceUID, attempt, projection := restoredProjectionFixture()
			task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-transcript", Create: true}
			projection.Execution = corev1alpha1.TaskExecutionStatus{
				State: projection.Execution.State, Outcome: projection.Execution.Outcome,
				Reason: projection.Execution.Reason, Attempt: projection.Execution.Attempt,
				PromptID: projection.Execution.PromptID, Message: projection.Execution.Message,
			}
			payload := marshalProjection(t, projection)
			turn := finalizedSessionProjectionTurn(t, payload, attempt)
			tt.mutate(task, attempt, &projection, turn)
			payload = marshalProjection(t, projection)
			if tt.name != "partial request identity" {
				// Mutations above target authoritative evidence, not the payload.
				turn.ProjectionDigest = fmt.Sprintf("sha256:%x", sha256.Sum256(payload))
				if tt.name == "projection digest" {
					turn.ProjectionDigest = digest("different-payload")
				}
			}
			if _, err := ValidateFinalizedSessionProjection(payload, task, sourceUID, attempt, turn); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("ValidateFinalizedSessionProjection() error = %v, want ErrConflict", err)
			}
		})
	}
}

//nolint:gocyclo // One table deliberately covers every security-relevant terminal payload field.
func TestValidateRestoredProjectionRejectsForgedOrIncompletePayload(t *testing.T) {
	const forgedValue = "other"

	tests := []struct {
		name   string
		mutate func(*Projection)
	}{
		{name: "namespace", mutate: func(p *Projection) { p.Namespace = forgedValue }},
		{name: "task name", mutate: func(p *Projection) { p.Task = forgedValue }},
		{name: "source UID", mutate: func(p *Projection) { p.TaskUID = forgedValue }},
		{name: "attempt", mutate: func(p *Projection) { p.Attempt++ }},
		{name: "execution attempt", mutate: func(p *Projection) { p.Execution.Attempt++ }},
		{name: "prompt ID", mutate: func(p *Projection) { p.Execution.PromptID = forgedValue }},
		{name: "request digest", mutate: func(p *Projection) { p.Execution.RequestDigest = digest("other-request") }},
		{name: "binding digest", mutate: func(p *Projection) { p.BindingDigest = digest("other-binding") }},
		{name: "execution state", mutate: func(p *Projection) { p.Execution.State = corev1alpha1.TaskExecutionStateFailed }},
		{name: "execution outcome", mutate: func(p *Projection) { p.Execution.Outcome = corev1alpha1.TaskExecutionOutcomeFailed }},
		{name: "phase", mutate: func(p *Projection) { p.Phase = corev1alpha1.TaskPhaseFailed }},
		{name: "runtime pool", mutate: func(p *Projection) { p.Execution.RuntimePoolName = forgedValue }},
		{name: "runtime pool UID", mutate: func(p *Projection) { p.Execution.RuntimePoolUID = forgedValue }},
		{name: "runtime instance", mutate: func(p *Projection) { p.Execution.RuntimeInstanceID = forgedValue }},
		{name: "runtime Session", mutate: func(p *Projection) { p.Execution.RuntimeSessionUID = forgedValue }},
		{name: "runtime Session generation", mutate: func(p *Projection) { p.Execution.RuntimeSessionGeneration++ }},
		{name: "runtime supervisor boot", mutate: func(p *Projection) { p.Execution.RuntimeSessionSupervisorBootID = forgedValue }},
		{name: "runtime profile digest", mutate: func(p *Projection) { p.Execution.RuntimeSessionProfileDigest = digest("other-profile") }},
		{name: "runtime MCP digest", mutate: func(p *Projection) { p.Execution.RuntimeSessionMCPDigest = digest("other-mcp") }},
		{name: "runtime workspace digest", mutate: func(p *Projection) { p.Execution.RuntimeSessionWorkspaceDigest = digest("other-workspace") }},
		{name: "delivery omitted", mutate: func(p *Projection) { p.Delivery = nil }},
		{name: "delivery state", mutate: func(p *Projection) { p.Delivery.State = corev1alpha1.TaskDeliveryStateDeliveryConflict }},
		{name: "delivery outcome", mutate: func(p *Projection) { p.Delivery.Outcome = corev1alpha1.TaskDeliveryOutcomeDeliveryConflict }},
		{name: "delivery reason", mutate: func(p *Projection) { p.Delivery.Reason = "Other" }},
		{name: "publication ID", mutate: func(p *Projection) { p.Delivery.PublicationID = forgedValue }},
		{name: "source repository", mutate: func(p *Projection) { p.Delivery.SourceRepository.ID = "github.com/other/source" }},
		{name: "publication repository", mutate: func(p *Projection) { p.Delivery.PublicationRepository.ID = "github.com/other/target" }},
		{name: "branch", mutate: func(p *Projection) { p.Delivery.Branch = forgedValue }},
		{name: "starting SHA", mutate: func(p *Projection) { p.Delivery.StartingSHA = strings.Repeat("a", 40) }},
		{name: "remote before SHA", mutate: func(p *Projection) { value := strings.Repeat("b", 40); p.Delivery.RemoteBeforeSHA = &value }},
		{name: "tree SHA", mutate: func(p *Projection) { p.Delivery.TreeSHA = strings.Repeat("c", 40) }},
		{name: "expected commit SHA", mutate: func(p *Projection) { p.Delivery.ExpectedCommitSHA = strings.Repeat("d", 40) }},
		{name: "verified remote SHA", mutate: func(p *Projection) { p.Delivery.VerifiedRemoteSHA = strings.Repeat("e", 40) }},
		{name: "superseding remote SHA", mutate: func(p *Projection) { p.Delivery.SupersedingRemoteSHA = strings.Repeat("f", 40) }},
		{name: "artifact digest", mutate: func(p *Projection) { p.Delivery.ArtifactDigest = digest("other-artifact") }},
		{name: "pull request receipt", mutate: func(p *Projection) { p.Delivery.PRReceipt.ID = forgedValue }},
		{name: "delivery message", mutate: func(p *Projection) { p.Delivery.Message = forgedValue }},
		{name: "harness v1 runtime", mutate: func(p *Projection) { p.HarnessRuntime = &corev1alpha1.HarnessRuntimeStatus{} }},
		{name: "harness v1 result", mutate: func(p *Projection) { p.ResultRef = &corev1alpha1.ResultReference{} }},
		{name: "unproved restore marker", mutate: func(p *Projection) { p.Execution.Reason = restoreIdentityChangedReason }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, sourceUID, attempt, projection := restoredProjectionFixture()
			tt.mutate(&projection)
			if _, err := ValidateRestoredProjection(marshalProjection(t, projection), task, sourceUID, attempt); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("ValidateRestoredProjection() error = %v, want ErrConflict", err)
			}
		})
	}

	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{name: "empty object", payload: []byte(`{}`)},
		{name: "legacy phase only", payload: []byte(`{"phase":"Succeeded"}`)},
		{name: "unknown field", payload: []byte(`{"unknown":true}`)},
		{name: "trailing object", payload: []byte(`{} {}`)},
		{name: "not JSON", payload: []byte(`not-json`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task, sourceUID, attempt, _ := restoredProjectionFixture()
			if _, err := ValidateRestoredProjection(tt.payload, task, sourceUID, attempt); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("ValidateRestoredProjection() error = %v, want ErrConflict", err)
			}
		})
	}
}

func restoredProjectionFixture() (*corev1alpha1.Task, string, *store.PromptAttempt, Projection) {
	const sourceUID = "11111111-1111-1111-1111-111111111111"
	remoteBefore := strings.Repeat("1", 40)
	transition := metav1.NewTime(time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC))
	delivery := &corev1alpha1.TaskDeliveryStatus{
		State: corev1alpha1.TaskDeliveryStateVerifiedExact, Outcome: corev1alpha1.TaskDeliveryOutcomeVerifiedExact,
		Reason: "Verified", PublicationID: "publication-source", Branch: "restore-proof",
		SourceRepository:      &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/example/source"},
		PublicationRepository: &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/example/target"},
		StartingSHA:           strings.Repeat("2", 40), RemoteBeforeSHA: &remoteBefore, TreeSHA: strings.Repeat("3", 40),
		ExpectedCommitSHA: strings.Repeat("4", 40), VerifiedRemoteSHA: strings.Repeat("4", 40),
		SupersedingRemoteSHA: strings.Repeat("5", 40), ArtifactDigest: digest("artifact"),
		PRReceipt: &corev1alpha1.TaskPullRequestReceipt{ID: "pr-1", Number: 1, URL: "https://github.com/example/target/pull/1", State: "open"},
		Message:   "verified exact", LastTransitionTime: &transition,
	}
	execution := &corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateSucceeded, Outcome: corev1alpha1.TaskExecutionOutcomeSucceeded,
		Reason: restoreIdentityChangedReason, Message: "restored status", Attempt: 2, PromptID: "prompt-final",
		RuntimePoolName: "pool", RuntimePoolUID: "pool-uid", RuntimeInstanceID: "runtime-instance",
		RuntimeSessionUID: "session-uid", RuntimeSessionGeneration: 3, RuntimeSessionSupervisorBootID: "boot-id",
		RuntimeSessionProfileDigest: digest("profile"), RuntimeSessionMCPDigest: digest("mcp"),
		RuntimeSessionWorkspaceDigest: digest("workspace"), RequestDigest: digest("request"), ControllerEpoch: 9,
		LastTransitionTime: &transition,
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "restored-task", UID: types.UID("22222222-2222-2222-2222-222222222222")},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded, Message: "restored status", Execution: execution, Delivery: delivery.DeepCopy(),
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				SchemaVersion: 1, ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
				BindingDigest: digest("binding"), Task: corev1alpha1.AgentExecutionBindingTaskRef{UID: types.UID(sourceUID)},
				Snapshot: corev1alpha1.AgentExecutionSnapshotRef{ID: sourceUID + "/" + digest("snapshot"), Digest: digest("snapshot"), SchemaVersion: 1},
			},
		},
	}
	attempt := &store.PromptAttempt{
		ID:            "prompt-attempt-final",
		Key:           store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: sourceUID, Attempt: 2, PromptID: execution.PromptID},
		RequestDigest: execution.RequestDigest, BindingDigest: task.Status.AgentExecutionBinding.BindingDigest,
		RuntimeInstanceID: execution.RuntimeInstanceID, SessionUID: execution.RuntimeSessionUID,
		SessionLeaseGeneration: execution.RuntimeSessionGeneration, ExecutionState: store.PromptExecutionSucceeded,
		DeliveryState: store.PromptDeliveryVerifiedExact, ControllerEpoch: 7,
	}
	projectionExecution := *execution.DeepCopy()
	projectionExecution.Reason = "SourceCompleted"
	projectionExecution.Message = "source terminal message"
	projectionExecution.ControllerEpoch = attempt.ControllerEpoch
	projectionExecution.LastTransitionTime = timePtr(time.Date(2026, 8, 11, 11, 59, 0, 0, time.UTC))
	projection := Projection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: sourceUID, Attempt: execution.Attempt,
		Phase: corev1alpha1.TaskPhaseSucceeded, Message: "source completed",
		BindingDigest: task.Status.AgentExecutionBinding.BindingDigest, Execution: projectionExecution,
		Delivery: delivery.DeepCopy(),
	}
	projection.Delivery.LastTransitionTime = timePtr(time.Date(2026, 8, 11, 11, 58, 0, 0, time.UTC))
	return task, sourceUID, attempt, projection
}

func marshalProjection(t *testing.T, projection Projection) []byte {
	t.Helper()
	payload, err := json.Marshal(projection)
	if err != nil {
		t.Fatalf("json.Marshal(): %v", err)
	}
	return payload
}

func finalizedSessionProjectionTurn(t *testing.T, payload []byte, attempt *store.PromptAttempt) *store.SessionTurn {
	t.Helper()
	key := store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}
	id, err := key.CanonicalID()
	if err != nil {
		t.Fatalf("SessionTurnKey.CanonicalID(): %v", err)
	}
	finalizedAt := time.Date(2026, 8, 11, 12, 10, 0, 0, time.UTC)
	return &store.SessionTurn{
		ID: id, Key: key, PromptAttemptID: attempt.ID, State: store.SessionTurnFinalized,
		TerminalKind: store.SessionTurnAssistantResult, ProjectionID: "outbox:session-terminal",
		ProjectionKind: ProjectionKind, ProjectionDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(payload)),
		FinalizedAt: &finalizedAt,
	}
}

func digest(value string) string {
	if len(value) > 64 {
		value = value[:64]
	}
	return "sha256:" + value + strings.Repeat("0", 64-len(value))
}

func timePtr(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}
