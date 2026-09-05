package store

import (
	"context"
	"time"
)

// SessionCleanupStore coordinates deletion across the Kubernetes-authoritative
// ACP Session records and the SQLite-owned transcript state.
type SessionCleanupStore interface {
	ReclaimSession(ctx context.Context, request ReclaimSessionRequest) error
}

// SessionCleanupRecoveryStore resumes durable cleanup intents after controller
// restart without requiring the original DELETE caller to retry.
type SessionCleanupRecoveryStore interface {
	ResumeSessionCleanups(ctx context.Context, fence ControllerEpochFence) error
}

// SessionCleanupPersistenceStore is the SQLite half of the cross-store Session
// cleanup protocol. The durable intent closes the crash window between deleting
// Kubernetes control records and deleting the transcript.
type SessionCleanupPersistenceStore interface {
	SessionTurnCleanupReceiptStore
	GetSessionCleanupIntent(ctx context.Context, namespace, sessionName string) (*SessionCleanupIntent, error)
	ListSessionCleanupIntents(ctx context.Context) ([]SessionCleanupIntent, error)
	ListSessionCleanupTurns(ctx context.Context, intent SessionCleanupIntent) ([]SessionTurn, error)
	GetSessionCleanupCompletion(ctx context.Context, namespace, sessionName string) (*SessionCleanupCompletion, error)
	HasSessionCleanupFenceForUID(ctx context.Context, sessionUID string) (bool, error)
	GetSessionCleanupIdentity(ctx context.Context, namespace, sessionName string) (string, error)
	BindSessionCleanupIdentity(ctx context.Context, namespace, sessionName, sessionUID string) error
	HasSessionCleanupIntent(ctx context.Context, namespace, sessionName string) (bool, error)
	PrepareSessionCleanup(ctx context.Context, intent SessionCleanupIntent) (*SessionCleanupIntent, error)
	CompleteSessionCleanup(ctx context.Context, request CompleteSessionCleanupRequest) error
}

// SessionTurnCleanupReceiptStore reads Task-owned finalization evidence after
// Session deletion. Ordinary turn and outbox reads never consult these receipts.
type SessionTurnCleanupReceiptStore interface {
	GetSessionTurnCleanupReceipt(ctx context.Context, namespace, sessionName, promptAttemptID string) (*SessionTurnCleanupReceipt, error)
}

// SessionRuntimeCleanupFunc retires resident runtime state after the durable
// intent has fenced new work and before Session authority is removed.
// It runs under the controller-epoch mutation lock and must not acquire that
// lock again through another control-store mutation.
type SessionRuntimeCleanupFunc func(context.Context, SessionCleanupIntent, ControllerEpochFence) error

// ReclaimSessionRequest identifies one idempotent user-requested Session
// deletion under the current controller epoch.
type ReclaimSessionRequest struct {
	Namespace       string               `json:"namespace"`
	SessionName     string               `json:"sessionName"`
	Fence           ControllerEpochFence `json:"fence"`
	OperationID     string               `json:"operationId"`
	OperationDigest string               `json:"operationDigest"`
	RequestedAt     time.Time            `json:"requestedAt"`
}

// SessionCleanupBranchClaim freezes one exact Session-owned BranchClaim.
type SessionCleanupBranchClaim struct {
	ID                    string                  `json:"id"`
	ObjectUID             string                  `json:"objectUid"`
	ExpectedVersion       int64                   `json:"expectedVersion"`
	ExpectedGeneration    int64                   `json:"expectedGeneration"`
	ExpectedRepositoryID  string                  `json:"expectedRepositoryId"`
	ExpectedRef           string                  `json:"expectedRef"`
	ExpectedOwnerUID      string                  `json:"expectedOwnerUid"`
	ExpectedLastVerified  RemoteRefState          `json:"expectedLastVerified"`
	ExpectedAvailability  BranchClaimAvailability `json:"expectedAvailability"`
	ExpectedBlockedReason string                  `json:"expectedBlockedReason,omitempty"`
	ExpectedPublicationID string                  `json:"expectedPublicationId,omitempty"`
	ExpectedRequestDigest string                  `json:"expectedRequestDigest"`
}

// SessionCleanupIntent is the durable cross-store deletion plan. An empty
// SessionUID denotes a transcript-only, non-ACP Session; SQLite refuses that
// form when ACP SessionTurns exist.
type SessionCleanupIntent struct {
	Namespace                      string                      `json:"namespace"`
	SessionName                    string                      `json:"sessionName"`
	SessionUID                     string                      `json:"sessionUid,omitempty"`
	ControlObjectUID               string                      `json:"controlObjectUid,omitempty"`
	ControlRequestDigest           string                      `json:"controlRequestDigest,omitempty"`
	ExpectedControlVersion         int64                       `json:"expectedControlVersion,omitempty"`
	ExpectedLeaseGeneration        int64                       `json:"expectedLeaseGeneration,omitempty"`
	ExpectedControlLastOperationID string                      `json:"expectedControlLastOperationId,omitempty"`
	ExpectedControlLastDigest      string                      `json:"expectedControlLastDigest,omitempty"`
	ExpectedVerifiedBaseline       *VerifiedBranchBaseline     `json:"expectedVerifiedBaseline,omitempty"`
	LeaseName                      string                      `json:"leaseName,omitempty"`
	LeaseObjectUID                 string                      `json:"leaseObjectUid,omitempty"`
	BranchClaims                   []SessionCleanupBranchClaim `json:"branchClaims,omitempty"`
	OperationID                    string                      `json:"operationId"`
	OperationDigest                string                      `json:"operationDigest"`
	PreparedAt                     time.Time                   `json:"preparedAt"`
}

// CompleteSessionCleanupRequest deletes SQLite-owned state only after the
// authoritative Kubernetes plan has been reclaimed.
type CompleteSessionCleanupRequest struct {
	Namespace       string `json:"namespace"`
	SessionName     string `json:"sessionName"`
	OperationID     string `json:"operationId"`
	OperationDigest string `json:"operationDigest"`
}

// SessionCleanupCompletion is the durable deletion tombstone retained after
// the Session row and cleanup intent are atomically removed. Session names are
// not reusable because stale DELETE retries cannot otherwise distinguish a new
// Session instance from the deleted one.
type SessionCleanupCompletion struct {
	Namespace       string    `json:"namespace"`
	SessionName     string    `json:"sessionName"`
	SessionUID      string    `json:"sessionUid,omitempty"`
	OperationID     string    `json:"operationId"`
	OperationDigest string    `json:"operationDigest"`
	CompletedAt     time.Time `json:"completedAt"`
}
