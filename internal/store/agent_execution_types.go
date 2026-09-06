/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// AgentExecutionSnapshotSchemaVersion is the current immutable execution
// snapshot schema version.
const AgentExecutionSnapshotSchemaVersion int32 = 1

// AgentExecutionSnapshotKey identifies one immutable, content-addressed
// execution snapshot.
type AgentExecutionSnapshotKey struct {
	TaskUID string
	// Digest is the canonical sha256 digest of the plaintext snapshot body.
	Digest string
}

// ID returns the canonical snapshot identity <task-uid>/sha256:<digest>.
func (k AgentExecutionSnapshotKey) ID() string {
	return k.TaskUID + "/" + k.Digest
}

// Validate rejects incomplete snapshot keys.
func (k AgentExecutionSnapshotKey) Validate() error {
	if strings.TrimSpace(k.TaskUID) == "" {
		return ValidationErrorf("snapshot task UID is required")
	}
	if err := ValidateCanonicalDigest("snapshot digest", k.Digest); err != nil {
		return err
	}
	return nil
}

// AgentExecutionSnapshot is the immutable non-secret executable input record
// for one Task binding. The body is sensitive (resolved prompts, Skill
// content, repository identities, endpoint metadata, policy configuration)
// even though raw credentials and TxTokens are prohibited; it is encrypted at
// rest and must never enter logs, events, metrics, or ordinary API output.
type AgentExecutionSnapshot struct {
	TaskUID       string
	Digest        string
	SchemaVersion int32
	// Body is the canonical JSON plaintext. Digest must equal
	// CanonicalAgentExecutionSnapshotDigest(Body).
	Body      []byte
	CreatedAt time.Time
}

// CanonicalAgentExecutionSnapshotDigest returns the canonical digest of a
// plaintext snapshot body.
func CanonicalAgentExecutionSnapshotDigest(body []byte) string {
	return CanonicalBytesDigest(body)
}

// AgentExecutionSnapshotStore persists immutable execution snapshots.
// Implementations must encrypt snapshot bodies at rest and fail closed when no
// encryption key is configured.
type AgentExecutionSnapshotStore interface {
	// PersistAgentExecutionSnapshot idempotently stores an immutable snapshot
	// keyed by Task UID and digest. An existing identical snapshot succeeds; an
	// existing snapshot with the same key but different content returns
	// ErrDuplicateMismatch. The snapshot digest must match the body.
	PersistAgentExecutionSnapshot(ctx context.Context, snapshot AgentExecutionSnapshot) error

	// GetAgentExecutionSnapshot decrypts and returns one snapshot, verifying
	// body integrity against the stored digest.
	GetAgentExecutionSnapshot(ctx context.Context, key AgentExecutionSnapshotKey) (*AgentExecutionSnapshot, error)

	// DeleteAgentExecutionSnapshots removes every snapshot for a Task UID. The
	// caller is responsible for proving all binding, attempt, lineage,
	// finalizer, and retention references are released first.
	DeleteAgentExecutionSnapshots(ctx context.Context, taskUID string) error
}

// AgentExecutionSnapshotMetadata is the non-secret lifecycle view of one
// encrypted snapshot. It intentionally excludes the nonce and ciphertext as
// well as the decrypted body.
type AgentExecutionSnapshotMetadata struct {
	Key           AgentExecutionSnapshotKey
	SchemaVersion int32
	CreatedAt     time.Time
}

// Validate rejects corrupt lifecycle metadata before retention code acts on
// it. Stored snapshot keys remain integrity references, not authorization.
func (m AgentExecutionSnapshotMetadata) Validate() error {
	if err := m.Key.Validate(); err != nil {
		return err
	}
	if m.SchemaVersion < 1 {
		return ValidationErrorf("snapshot metadata schema version must be positive")
	}
	if m.CreatedAt.IsZero() {
		return ValidationErrorf("snapshot metadata creation time is required")
	}
	return nil
}

// AgentExecutionSnapshotReferenceCounts reports every durable SQLite
// reference that can retain a snapshot. SessionTurns retain every snapshot
// scoped to their immutable Task UID because they do not store a snapshot
// digest themselves.
type AgentExecutionSnapshotReferenceCounts struct {
	HarnessV1Attempts int64
	PromptAttempts    int64
	SessionTurns      int64
}

// Total returns the number of durable references across all known sources.
func (c AgentExecutionSnapshotReferenceCounts) Total() int64 {
	return c.HarnessV1Attempts + c.PromptAttempts + c.SessionTurns
}

// AgentExecutionSnapshotLifecycleStore is an optional retention/GC extension
// to AgentExecutionSnapshotStore. It exposes metadata only, reports durable
// references, and deletes one exact key without changing the existing broad
// Task-UID deletion contract.
type AgentExecutionSnapshotLifecycleStore interface {
	// ListAgentExecutionSnapshotMetadataBefore returns snapshots created
	// strictly before cutoff, ordered by creation time, Task UID, and digest.
	ListAgentExecutionSnapshotMetadataBefore(ctx context.Context, cutoff time.Time) ([]AgentExecutionSnapshotMetadata, error)

	// CountAgentExecutionSnapshotReferences returns a consistent count across
	// v1 attempts, v2 prompt attempts, and SessionTurns. Attempt sources match
	// Task UID and digest; SessionTurns match the immutable Task UID because they
	// do not carry a snapshot digest.
	CountAgentExecutionSnapshotReferences(ctx context.Context, key AgentExecutionSnapshotKey) (AgentExecutionSnapshotReferenceCounts, error)

	// DeleteAgentExecutionSnapshot idempotently deletes one exact Task
	// UID/digest key. The caller must first prove that all references and the
	// configured retention interval have cleared.
	DeleteAgentExecutionSnapshot(ctx context.Context, key AgentExecutionSnapshotKey) error
}

// SessionLineage durably records the execution protocol and runtime identity
// of one conversation Session. LineageGeneration is independent of the Session
// mutation-lease generation and any v2 RuntimeSession generation.
type SessionLineage struct {
	Namespace    string
	SessionName  string
	NamespaceUID string
	SessionUID   string
	// ContractVersion is orka.harness.v1 or orka.harness.v2.
	ContractVersion   string
	LineageGeneration int64
	// RuntimeIdentity is the built-in runtime type or the AgentRuntime UID.
	RuntimeIdentity string
	// ConfigDigest is the configuration/snapshot digest recorded when the
	// lineage was established.
	ConfigDigest string
	Version      int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Validate rejects an incomplete authoritative Session lineage.
func (l SessionLineage) Validate() error {
	claim := ClaimSessionLineageRequest{
		Namespace: l.Namespace, SessionName: l.SessionName, NamespaceUID: l.NamespaceUID,
		SessionUID: l.SessionUID, ContractVersion: l.ContractVersion,
		LineageGeneration: l.LineageGeneration, RuntimeIdentity: l.RuntimeIdentity, ConfigDigest: l.ConfigDigest,
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if l.LineageGeneration < 1 {
		return ValidationErrorf("session lineage generation must be at least 1")
	}
	if l.Version < 1 {
		return ValidationErrorf("session lineage projection version must be at least 1")
	}
	if l.CreatedAt.IsZero() || l.UpdatedAt.IsZero() {
		return ValidationErrorf("session lineage creation and update times are required")
	}
	return nil
}

// ClaimSessionLineageRequest atomically establishes or verifies a Session
// lineage. Callers must invoke it under the same serialization that acquires
// the Session mutation lease so two concurrent first-use Tasks cannot
// establish different protocols.
type ClaimSessionLineageRequest struct {
	Namespace       string
	SessionName     string
	NamespaceUID    string
	SessionUID      string
	ContractVersion string
	// LineageGeneration is independent of mutation-lease and runtime-session
	// generations. Ordinary first use establishes generation 1.
	LineageGeneration int64
	RuntimeIdentity   string
	ConfigDigest      string

	// EstablishIfAbsent permits creating the lineage row. It must be true only
	// when the caller has proven the Session is genuinely fresh: a non-empty
	// pre-existing Session is never silently treated as unclaimed.
	EstablishIfAbsent bool
}

// Validate rejects incomplete lineage claims.
func (r ClaimSessionLineageRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.Namespace) == "":
		return ValidationErrorf("session lineage namespace is required")
	case strings.TrimSpace(r.SessionName) == "":
		return ValidationErrorf("session lineage session name is required")
	case strings.TrimSpace(r.NamespaceUID) == "":
		return ValidationErrorf("session lineage namespace UID is required")
	case strings.TrimSpace(r.SessionUID) == "":
		return ValidationErrorf("session lineage session UID is required")
	case r.ContractVersion != "orka.harness.v1" && r.ContractVersion != "orka.harness.v2":
		return ValidationErrorf("session lineage contract version %q must be orka.harness.v1 or orka.harness.v2", r.ContractVersion)
	case r.LineageGeneration < 1:
		return ValidationErrorf("session lineage generation must be at least 1")
	case strings.TrimSpace(r.RuntimeIdentity) == "":
		return ValidationErrorf("session lineage runtime identity is required")
	}
	if err := ValidateCanonicalDigest("session lineage config digest", r.ConfigDigest); err != nil {
		return err
	}
	return nil
}

// SessionLineageStore persists a payload projection of Kubernetes-authoritative
// Session protocol/runtime lineage. Projection failure may block dispatch but
// never changes or releases the Kubernetes lineage/Lease authority.
type SessionLineageStore interface {
	// ProjectSessionLineage idempotently stores the exact authoritative record.
	// Any mismatch returns ErrConflict and must be repaired explicitly; SQLite
	// never adjudicates which lineage owns a Session.
	ProjectSessionLineage(ctx context.Context, lineage SessionLineage) (*SessionLineage, error)

	// GetSessionLineage returns the lineage for one Session, or ErrNotFound.
	GetSessionLineage(ctx context.Context, namespace, sessionName string) (*SessionLineage, error)
}

// HarnessV1AttemptState is the durable harness v1 attempt state machine.
type HarnessV1AttemptState string

const (
	HarnessV1AttemptPrepared         HarnessV1AttemptState = "Prepared"
	HarnessV1AttemptSubmitting       HarnessV1AttemptState = "Submitting"
	HarnessV1AttemptRejected         HarnessV1AttemptState = "Rejected"
	HarnessV1AttemptSubmittedUnknown HarnessV1AttemptState = "SubmittedUnknown"
	HarnessV1AttemptAccepted         HarnessV1AttemptState = "Accepted"
	HarnessV1AttemptRunning          HarnessV1AttemptState = "Running"
	HarnessV1AttemptCancelRequested  HarnessV1AttemptState = "CancelRequested"
	HarnessV1AttemptSettling         HarnessV1AttemptState = "Settling"
	HarnessV1AttemptSucceeded        HarnessV1AttemptState = "Succeeded"
	HarnessV1AttemptFailed           HarnessV1AttemptState = "Failed"
	HarnessV1AttemptCancelled        HarnessV1AttemptState = "Cancelled"
	HarnessV1AttemptOutcomeUnknown   HarnessV1AttemptState = "OutcomeUnknown"
)

var harnessV1AttemptTransitions = map[HarnessV1AttemptState][]HarnessV1AttemptState{
	HarnessV1AttemptPrepared:   {HarnessV1AttemptSubmitting, HarnessV1AttemptRejected, HarnessV1AttemptCancelled},
	HarnessV1AttemptSubmitting: {HarnessV1AttemptRejected, HarnessV1AttemptSubmittedUnknown, HarnessV1AttemptAccepted},
	HarnessV1AttemptSubmittedUnknown: {
		HarnessV1AttemptRejected, HarnessV1AttemptAccepted, HarnessV1AttemptOutcomeUnknown,
	},
	HarnessV1AttemptAccepted: {
		HarnessV1AttemptRunning, HarnessV1AttemptCancelRequested, HarnessV1AttemptSettling,
		HarnessV1AttemptSucceeded, HarnessV1AttemptFailed, HarnessV1AttemptCancelled, HarnessV1AttemptOutcomeUnknown,
	},
	HarnessV1AttemptRunning: {
		HarnessV1AttemptCancelRequested, HarnessV1AttemptSettling,
		HarnessV1AttemptSucceeded, HarnessV1AttemptFailed, HarnessV1AttemptCancelled, HarnessV1AttemptOutcomeUnknown,
	},
	HarnessV1AttemptCancelRequested: {
		HarnessV1AttemptSettling, HarnessV1AttemptSucceeded, HarnessV1AttemptFailed,
		HarnessV1AttemptCancelled, HarnessV1AttemptOutcomeUnknown,
	},
	HarnessV1AttemptSettling: {
		HarnessV1AttemptSucceeded, HarnessV1AttemptFailed, HarnessV1AttemptCancelled, HarnessV1AttemptOutcomeUnknown,
	},
}

// IsTerminalHarnessV1AttemptState reports whether a state admits no further
// transitions. Rejected is terminal for the attempt and is the only
// submission-state path eligible for a safe resend through a new attempt.
func IsTerminalHarnessV1AttemptState(state HarnessV1AttemptState) bool {
	switch state {
	case HarnessV1AttemptRejected, HarnessV1AttemptSucceeded, HarnessV1AttemptFailed,
		HarnessV1AttemptCancelled, HarnessV1AttemptOutcomeUnknown:
		return true
	default:
		return false
	}
}

// ValidateHarnessV1AttemptTransition rejects illegal state transitions.
func ValidateHarnessV1AttemptTransition(from, to HarnessV1AttemptState) error {
	if slices.Contains(harnessV1AttemptTransitions[from], to) {
		return nil
	}
	return ValidationErrorf("harness v1 attempt transition %s -> %s is not allowed", from, to)
}

// HarnessV1AttemptKey identifies one durable v1 attempt.
type HarnessV1AttemptKey struct {
	Namespace string
	TaskUID   string
	Attempt   int32
}

// CanonicalID returns the canonical attempt record identity.
func (k HarnessV1AttemptKey) CanonicalID() string {
	return fmt.Sprintf("%s/%s/%d", k.Namespace, k.TaskUID, k.Attempt)
}

// SessionReferenceID returns the bounded content-derived identity stored in a
// protocol-neutral SessionTurn. The attempt store continues to use the
// human-readable CanonicalID as its primary key; this separate identifier
// prevents a v1 attempt from being confused with a v2 PromptAttempt.
func (k HarnessV1AttemptKey) SessionReferenceID() string {
	return CanonicalControlID("harness-v1-attempt", k.Namespace, k.TaskUID, fmt.Sprint(k.Attempt))
}

// Validate rejects incomplete attempt keys.
func (k HarnessV1AttemptKey) Validate() error {
	switch {
	case strings.TrimSpace(k.Namespace) == "":
		return ValidationErrorf("harness v1 attempt namespace is required")
	case strings.TrimSpace(k.TaskUID) == "":
		return ValidationErrorf("harness v1 attempt task UID is required")
	case k.Attempt < 1:
		return ValidationErrorf("harness v1 attempt number must be positive")
	}
	return nil
}

// HarnessV1AttemptRetryClass bounds retry eligibility recorded from the
// immutable snapshot classification.
type HarnessV1AttemptRetryClass string

const (
	// HarnessV1RetryClassNone forbids retry regardless of failure shape.
	HarnessV1RetryClassNone HarnessV1AttemptRetryClass = "none"
	// HarnessV1RetryClassDuplicateSafe permits retry only after a definitive
	// pre-submission rejection or definitive retryable terminal failure.
	HarnessV1RetryClassDuplicateSafe HarnessV1AttemptRetryClass = "duplicate-safe"
)

// HarnessV1Attempt is the durable v1 attempt aggregate. Attempt-specific state
// stays separate from the lifetime Task binding; every record carries the
// binding digest.
type HarnessV1Attempt struct {
	Namespace string
	TaskName  string
	TaskUID   string
	Attempt   int32

	BindingDigest  string
	SnapshotDigest string
	RequestDigest  string

	TurnID           string
	RuntimeSessionID string
	CorrelationID    string

	// Backend identifies the executor: built-in wrapper or external endpoint.
	Backend string
	// BackendEndpoint is the non-secret endpoint identity selected at dispatch.
	BackendEndpoint string

	AuthSecretNamespace       string
	AuthSecretName            string
	AuthSecretKey             string
	AuthSecretUID             string
	AuthSecretResourceVersion string

	State HarnessV1AttemptState
	// LastEventSeq is the highest persisted frame sequence.
	LastEventSeq int64
	// CancelRequestedAt is set when cancellation was requested; CancelAccepted
	// remains nonterminal until a terminal frame or settlement receipt.
	CancelRequestedAt *time.Time
	// TerminalReceiptDigest digests the authoritative terminal or
	// OutcomeUnknown receipt.
	TerminalReceiptDigest string
	// TerminalReason is a bounded reason code for terminal states.
	TerminalReason string

	DuplicateSafe bool
	RetryClass    HarnessV1AttemptRetryClass

	ControllerEpochName string
	ControllerEpoch     int64
	LastOperationID     string
	LastOperationDigest string
	Version             int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Validate rejects incomplete attempt records at creation.
func (a *HarnessV1Attempt) Validate() error {
	if a == nil {
		return ValidationErrorf("harness v1 attempt is required")
	}
	key := HarnessV1AttemptKey{Namespace: a.Namespace, TaskUID: a.TaskUID, Attempt: a.Attempt}
	if err := key.Validate(); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("harness v1 attempt binding digest", a.BindingDigest); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("harness v1 attempt snapshot digest", a.SnapshotDigest); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("harness v1 attempt request digest", a.RequestDigest); err != nil {
		return err
	}
	if strings.TrimSpace(a.TurnID) == "" {
		return ValidationErrorf("harness v1 attempt turn ID is required")
	}
	if a.State != HarnessV1AttemptPrepared {
		return ValidationErrorf("harness v1 attempts are created in Prepared state, got %q", string(a.State))
	}
	switch a.RetryClass {
	case HarnessV1RetryClassNone, HarnessV1RetryClassDuplicateSafe:
	default:
		return ValidationErrorf("harness v1 attempt retry class %q is not supported", string(a.RetryClass))
	}
	return nil
}

// HarnessV1AttemptUpdates carries optional field updates applied atomically
// with a state transition.
type HarnessV1AttemptUpdates struct {
	RuntimeSessionID      *string
	CorrelationID         *string
	BackendEndpoint       *string
	LastEventSeq          *int64
	CancelRequestedAt     *time.Time
	TerminalReceiptDigest *string
	TerminalReason        *string
}

// HarnessV1AttemptTransition is a fenced, idempotent CAS state transition.
type HarnessV1AttemptTransition struct {
	Key             HarnessV1AttemptKey
	ExpectedVersion int64
	ExpectedState   HarnessV1AttemptState
	TargetState     HarnessV1AttemptState
	OperationID     string
	OperationDigest string
	Fence           ControllerEpochFence
	Updates         HarnessV1AttemptUpdates
}

// ReclaimHarnessV1AttemptsRequest removes Task-owned v1 attempt aggregates
// only after terminal state and, for continuity Tasks, durable SessionTurn and
// delivered outbox barriers have been re-proven in the same transaction.
type ReclaimHarnessV1AttemptsRequest struct {
	Namespace       string
	TaskUID         string
	BindingDigest   string
	SessionRequired bool
	Fence           ControllerEpochFence
}

// HarnessV1AttemptStore persists durable v1 attempt aggregates.
type HarnessV1AttemptStore interface {
	// CreateHarnessV1Attempt persists a new Prepared attempt exactly once. A
	// duplicate identical create is idempotent; a duplicate with different
	// content returns ErrDuplicateMismatch.
	CreateHarnessV1Attempt(ctx context.Context, attempt *HarnessV1Attempt, fence ControllerEpochFence) error

	// GetHarnessV1Attempt returns one attempt or ErrNotFound.
	GetHarnessV1Attempt(ctx context.Context, key HarnessV1AttemptKey) (*HarnessV1Attempt, error)

	// ListHarnessV1AttemptsByTask returns every attempt for one Task UID.
	ListHarnessV1AttemptsByTask(ctx context.Context, namespace, taskUID string) ([]HarnessV1Attempt, error)

	// TransitionHarnessV1Attempt applies a fenced CAS transition. A replay with
	// the same operation ID and digest against the already-applied state is
	// idempotent; conflicting expectations return ErrConflict.
	TransitionHarnessV1Attempt(ctx context.Context, transition HarnessV1AttemptTransition) (*HarnessV1Attempt, error)

	// ReclaimHarnessV1Attempts deletes all terminal attempts for one immutable
	// Task. Active attempts or incomplete Session/outbox settlement fail closed.
	ReclaimHarnessV1Attempts(ctx context.Context, request ReclaimHarnessV1AttemptsRequest) (int, error)
}
