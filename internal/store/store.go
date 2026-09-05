package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a resource cannot be updated because it changed concurrently.
var ErrConflict = errors.New("conflict")

// ErrNotReady is returned when a durable prerequisite is expected to become ready shortly.
var ErrNotReady = errors.New("not ready")

// ErrDuplicateMismatch is returned when a stable external identifier is reused with a different payload.
var ErrDuplicateMismatch = errors.New("duplicate payload mismatch")

// ErrCapacity is returned when a bounded durable store quota is full.
var ErrCapacity = errors.New("capacity exceeded")

// ErrValidation is returned when supplied input fails store-level validation.
var ErrValidation = errors.New("validation error")

// ErrGatewayOwnedSession is returned when a generic deletion targets canonical gateway history.
var ErrGatewayOwnedSession = errors.New("gateway-owned session")

// ValidationError carries a client-safe validation message while remaining
// comparable with ErrValidation via errors.Is.
type ValidationError struct {
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}

func (e ValidationError) Unwrap() error {
	return ErrValidation
}

// ValidationErrorf formats a store validation error.
func ValidationErrorf(format string, args ...any) error {
	return ValidationError{Message: fmt.Sprintf(format, args...)}
}

// HealthChecker can verify its underlying storage is reachable.
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

// ResultStore handles task result persistence.
type ResultStore interface {
	SaveResult(ctx context.Context, namespace, taskName string, data []byte) error
	GetResult(ctx context.Context, namespace, taskName string) ([]byte, error)
	DeleteResult(ctx context.Context, namespace, taskName string) error
}

// PromptResultReceipt preserves the exact result bytes behind a fenced prompt
// settling transition. The receipt is immutable for one PromptAttempt so a
// controller takeover can repair a result write interrupted after the durable
// control-plane transition.
type PromptResultReceipt struct {
	AttemptID       string
	Namespace       string
	TaskName        string
	OperationID     string
	OperationDigest string
	Data            []byte
}

// PromptResultReceiptStore persists attempt-bound result receipts before the
// corresponding PromptAttempt enters Settling.
type PromptResultReceiptStore interface {
	SavePromptResultReceipt(ctx context.Context, receipt PromptResultReceipt) error
	GetPromptResultReceipt(ctx context.Context, attemptID string) (*PromptResultReceipt, error)
}

// SessionStore handles session transcript persistence.
type SessionStore interface {
	CreateSession(ctx context.Context, session *SessionRecord) error
	GetSession(ctx context.Context, namespace, name string) (*SessionRecord, error)
	ListSessions(ctx context.Context, namespace string) ([]SessionMetadata, error)
	// ListSessionsPage returns up to limit sessions of the namespace in name
	// order, starting strictly after afterName and skipping excludeType
	// (empty = no exclusion). more reports whether further rows exist.
	ListSessionsPage(ctx context.Context, namespace, afterName string, limit int, excludeType string) (items []SessionMetadata, more bool, err error)
	DeleteSession(ctx context.Context, namespace, name string) error

	// Locking
	AcquireLock(ctx context.Context, namespace, name, taskName, taskUID string) error
	ReleaseLock(ctx context.Context, namespace, name, taskName, taskUID string) error
	IsLocked(ctx context.Context, namespace, name, currentTask, currentTaskUID string) (bool, error)

	// Transcript
	AppendMessages(ctx context.Context, namespace, name string, messages []SessionMessage) error
	LoadTranscript(ctx context.Context, namespace, name string, maxMessages int) ([]SessionMessage, error)
	LoadTranscriptThrough(ctx context.Context, namespace, name, throughMessageID string, maxMessages int) ([]SessionMessage, error)
	SearchTranscript(ctx context.Context, filter TranscriptSearchFilter) ([]TranscriptSearchResult, error)

	// Token tracking
	UpdateTokenCounts(ctx context.Context, namespace, name string, inputTokens, outputTokens int) error
}

// ExpiringSessionLockStore supports crash-recoverable transient locks. Durable
// Task locks continue to use SessionStore.AcquireLock without an expiry.
type ExpiringSessionLockStore interface {
	AcquireLockUntil(ctx context.Context, namespace, name, ownerName, ownerUID string, expiresAt time.Time) error
}

// FencedSessionWriteStore binds transcript and token writes to the exact active
// transient lock owner so an expired request cannot write after takeover.
type FencedSessionWriteStore interface {
	AppendMessagesWithLock(ctx context.Context, namespace, name, ownerName, ownerUID string, messages []SessionMessage) error
	UpdateTokenCountsWithLock(ctx context.Context, namespace, name, ownerName, ownerUID string, inputTokens, outputTokens int) error
}

// GatewayEventStore handles durable normalized ingress records and atomic Session projection.
type GatewayEventStore interface {
	AdmitGatewayEvent(ctx context.Context, admission GatewayEventAdmission) (*GatewayEvent, bool, error)
	GetGatewayEvent(ctx context.Context, namespace, id string) (*GatewayEvent, error)
	GetGatewayEventDuplicate(ctx context.Context, candidate *GatewayEvent, now time.Time) (*GatewayEvent, error)
	GetGatewayEventForTask(ctx context.Context, namespace, taskName, taskUID string) (*GatewayEvent, error)
	HasGatewayTaskTombstone(ctx context.Context, namespace, taskName, taskUID string) (bool, error)
	ListGatewayEvents(ctx context.Context, filter GatewayEventFilter) ([]GatewayEvent, error)
	ClaimNextGatewayEvent(ctx context.Context, namespace, owner string, now time.Time, lease time.Duration) (*GatewayEvent, error)
	RenewGatewayEventClaim(ctx context.Context, namespace, id, owner string, now time.Time, lease time.Duration) (*GatewayEvent, error)
	FreezeGatewayEventTaskRuntimeAllowedTools(ctx context.Context, namespace, id, owner string, allowedTools []string, now time.Time) (*GatewayEvent, error)
	MarkGatewayEventTaskCreated(ctx context.Context, namespace, id, taskName, taskUID, owner string, now time.Time) error
	RetryGatewayEvent(ctx context.Context, namespace, id, owner, reason string, nextAttemptAt time.Time) error
	DeferGatewayEventProjection(ctx context.Context, namespace, id string, nextAttemptAt time.Time) error
	ExpireGatewayEvent(ctx context.Context, namespace, id, owner, reason string, now time.Time) error
	ExpireGatewayEventWithDelivery(ctx context.Context, projection GatewayExpiryProjection) (*GatewayDelivery, bool, error)
	MarkExpiredGatewayEventDeadLettered(ctx context.Context, namespace, id, reason string, now time.Time) error
	ProjectGatewayTerminal(ctx context.Context, projection GatewayTerminalProjection) (*GatewayDelivery, bool, error)
	GetGatewayQueueStats(ctx context.Context, namespace string) (GatewayQueueStats, error)
}

// GatewayDeliveryStore handles durable adapter outbox records.
type GatewayDeliveryStore interface {
	CreateGatewayDelivery(ctx context.Context, delivery *GatewayDelivery) (*GatewayDelivery, bool, error)
	GetGatewayDelivery(ctx context.Context, namespace, id string) (*GatewayDelivery, error)
	ListGatewayDeliveries(ctx context.Context, filter GatewayDeliveryFilter) ([]GatewayDelivery, error)
	ClaimNextGatewayDelivery(ctx context.Context, namespace, owner string, now time.Time, lease time.Duration) (*GatewayDelivery, error)
	MarkGatewayDeliveryDelivered(ctx context.Context, namespace, id, owner, providerMessageID string, now time.Time) error
	ScheduleGatewayDeliveryRetry(ctx context.Context, namespace, id, owner, reason string, nextAttemptAt time.Time) error
	MarkGatewayDeliveryTerminal(ctx context.Context, namespace, id, owner string, state GatewayDeliveryState, reason string, now time.Time) error
	RetryGatewayDelivery(ctx context.Context, namespace, id string, now, expiresAt time.Time) (*GatewayDelivery, error)
	MaintainGatewayRecords(ctx context.Context, namespace string, now, terminalCutoff time.Time) (GatewayMaintenanceResult, error)
}

// MemoryStore handles durable namespace-scoped memory persistence.
type MemoryStore interface {
	CreateMemory(ctx context.Context, memory *Memory) error
	GetMemory(ctx context.Context, namespace, id string) (*Memory, error)
	ListMemories(ctx context.Context, filter MemoryFilter) ([]Memory, error)
	UpdateMemory(ctx context.Context, memory *Memory) error
	DeleteMemory(ctx context.Context, namespace, id string) error
	SetMemoryDisabled(ctx context.Context, namespace, id string, disabled bool) error
}

// MemoryProposalStore handles governance for worker-proposed memory/skill changes.
type MemoryProposalStore interface {
	CreateMemoryProposal(ctx context.Context, proposal *MemoryProposal) error
	GetMemoryProposal(ctx context.Context, namespace, id string) (*MemoryProposal, error)
	ListMemoryProposals(ctx context.Context, filter MemoryProposalFilter) ([]MemoryProposal, error)
	ReviewMemoryProposal(ctx context.Context, review MemoryProposalReview) error
	ApplyMemoryProposal(ctx context.Context, apply MemoryProposalApply) (*Memory, error)
	ArchiveMemoryProposal(ctx context.Context, namespace, id string) error
}

// PlanStore handles autonomous plan state persistence.
type PlanStore interface {
	SavePlan(ctx context.Context, namespace, taskName string, plan *PlanState) error
	GetPlan(ctx context.Context, namespace, taskName string) (*PlanState, error)
	DeletePlan(ctx context.Context, namespace, taskName string) error
}

// ArtifactStore handles task artifact persistence.
type ArtifactStore interface {
	SaveArtifact(ctx context.Context, namespace, taskName, filename, contentType string, data []byte) error
	GetArtifact(ctx context.Context, namespace, taskName, filename string) ([]byte, string, error)
	ListArtifacts(ctx context.Context, namespace, taskName string) ([]ArtifactMetadata, error)
	DeleteArtifacts(ctx context.Context, namespace, taskName string) error
}

// SecurityStore handles repository security scanning persistence.
type SecurityStore interface {
	CreateScanRun(ctx context.Context, run *ScanRun) error
	UpdateScanRun(ctx context.Context, run *ScanRun) error
	GetScanRun(ctx context.Context, namespace, id string) (*ScanRun, error)
	ListScanRuns(ctx context.Context, namespace, repositoryScan string, limit int, cursor string) ([]ScanRun, string, error)

	UpsertReviewSlice(ctx context.Context, slice *ReviewSlice) error
	ListReviewSlices(ctx context.Context, filter ReviewSliceFilter) ([]ReviewSlice, string, error)
	GetReviewSlice(ctx context.Context, namespace, repositoryScan, id string) (*ReviewSlice, error)
	UpdateReviewSliceStatus(ctx context.Context, namespace, repositoryScan, id, lastScanRunID, status string) error

	GetLatestThreatModel(ctx context.Context, namespace, repositoryScan string) (*ThreatModel, error)
	SaveThreatModel(ctx context.Context, model *ThreatModel) error

	UpsertFinding(ctx context.Context, finding *Finding) error
	GetFinding(ctx context.Context, namespace, id string) (*Finding, error)
	ListFindings(ctx context.Context, filter FindingFilter) ([]Finding, string, error)
	GetFindingCounts(ctx context.Context, namespace, repositoryScan string) (FindingCounts, error)
	UpdateFindingState(ctx context.Context, namespace, id, state string) error

	CreatePatchProposal(ctx context.Context, proposal *PatchProposal) error
	UpdatePatchProposal(ctx context.Context, proposal *PatchProposal) error
	BindPatchProposalPublicationEvidence(ctx context.Context, proposal *PatchProposal) error
	ListPatchProposals(ctx context.Context, namespace, findingID string) ([]PatchProposal, error)

	CreateDroppedFinding(ctx context.Context, dropped *DroppedFinding) error
	ListDroppedFindings(ctx context.Context, filter DroppedFindingFilter) ([]DroppedFinding, string, error)
}

// RepositoryMonitorStore handles durable repository monitor state.
type RepositoryMonitorStore interface {
	UpsertRepositoryMonitor(ctx context.Context, monitor *RepositoryMonitorRecord) error
	GetRepositoryMonitor(ctx context.Context, namespace, name string) (*RepositoryMonitorRecord, error)
	DeleteRepositoryMonitor(ctx context.Context, namespace, name string) error

	CreateMonitorRun(ctx context.Context, run *MonitorRun) error
	UpdateMonitorRun(ctx context.Context, run *MonitorRun) error
	GetMonitorRun(ctx context.Context, namespace, id string) (*MonitorRun, error)
	ListMonitorRuns(ctx context.Context, filter MonitorRunFilter) ([]MonitorRun, string, error)

	UpsertMonitorItem(ctx context.Context, item *MonitorItem) error
	GetMonitorItem(ctx context.Context, namespace, monitorName, kind, itemKey string) (*MonitorItem, error)
	ListMonitorItems(ctx context.Context, filter MonitorItemFilter) ([]MonitorItem, string, error)

	CreateWorkAction(ctx context.Context, action *WorkAction) error
	UpdateWorkAction(ctx context.Context, action *WorkAction) error
	GetWorkAction(ctx context.Context, namespace, id string) (*WorkAction, error)
	ListWorkActions(ctx context.Context, filter WorkActionFilter) ([]WorkAction, string, error)
	CancelWorkActions(ctx context.Context, namespace, monitorName, targetKind string, targetNumber int64, reason string) (int, error)

	CreateActionRecord(ctx context.Context, record *ActionRecord) error
	UpdateActionRecord(ctx context.Context, record *ActionRecord) error
	GetActionRecord(ctx context.Context, namespace, id string) (*ActionRecord, error)
	ListActionRecords(ctx context.Context, filter ActionRecordFilter) ([]ActionRecord, string, error)

	CreateReviewRecord(ctx context.Context, record *ReviewRecord) error
	GetReviewRecord(ctx context.Context, namespace, id string) (*ReviewRecord, error)
	ListReviewRecords(ctx context.Context, filter ReviewRecordFilter) ([]ReviewRecord, string, error)

	CreateReviewPublishRecord(ctx context.Context, record *ReviewPublishRecord) error
	UpdateReviewPublishRecord(ctx context.Context, record *ReviewPublishRecord) error
	GetReviewPublishRecord(ctx context.Context, namespace, id string) (*ReviewPublishRecord, error)
	ListReviewPublishRecords(ctx context.Context, filter ReviewPublishRecordFilter) ([]ReviewPublishRecord, string, error)

	CreateCommandEvent(ctx context.Context, event *CommandEvent) error
	UpdateCommandEvent(ctx context.Context, event *CommandEvent) error
	GetCommandEvent(ctx context.Context, namespace, id string) (*CommandEvent, error)
	ListCommandEvents(ctx context.Context, filter CommandEventFilter) ([]CommandEvent, string, error)

	CreateImplementationJob(ctx context.Context, job *ImplementationJob) error
	UpdateImplementationJob(ctx context.Context, job *ImplementationJob) error
	GetImplementationJob(ctx context.Context, namespace, id string) (*ImplementationJob, error)
	ListImplementationJobs(ctx context.Context, filter ImplementationJobFilter) ([]ImplementationJob, string, error)
	CountImplementationJobs(ctx context.Context, filter ImplementationJobFilter) (int, error)

	CreateGitHubMutationRecord(ctx context.Context, record *GitHubMutationRecord) error
	UpdateGitHubMutationRecord(ctx context.Context, record *GitHubMutationRecord) error
	GetGitHubMutationRecord(ctx context.Context, namespace, id string) (*GitHubMutationRecord, error)
	ListGitHubMutationRecords(ctx context.Context, filter GitHubMutationRecordFilter) ([]GitHubMutationRecord, string, error)

	CreateRepairJob(ctx context.Context, job *RepairJob) error
	UpdateRepairJob(ctx context.Context, job *RepairJob) error
	GetRepairJob(ctx context.Context, namespace, id string) (*RepairJob, error)
	ListRepairJobs(ctx context.Context, filter RepairJobFilter) ([]RepairJob, string, error)

	CreateMonitorEvent(ctx context.Context, event *MonitorEvent) error
	ListMonitorEvents(ctx context.Context, filter MonitorEventFilter) ([]MonitorEvent, string, error)
}

// Message represents an inter-agent message.
type Message struct {
	ID         int64  `json:"id"`
	Namespace  string `json:"namespace"`
	FromTask   string `json:"fromTask"`
	ToTask     string `json:"toTask"` // "*" for broadcast to all siblings
	ParentTask string `json:"parentTask"`
	Content    string `json:"content"`
	Read       bool   `json:"read"`
	CreatedAt  string `json:"createdAt"`
}

// MessageStore handles inter-agent message persistence.
type MessageStore interface {
	// SendMessage stores a new message. Use toTask="*" for broadcast.
	SendMessage(ctx context.Context, msg *Message) error

	// GetMessages returns unread messages for a task, optionally marking them as read.
	GetMessages(ctx context.Context, namespace, taskName, parentTask string, markRead bool) ([]Message, error)

	// DeleteTaskMessages deletes all messages involving a task (sent or received).
	DeleteTaskMessages(ctx context.Context, namespace, taskName string) error

	// DeleteParentMessages deletes all messages for children of a parent task.
	DeleteParentMessages(ctx context.Context, namespace, parentTask string) error
}
