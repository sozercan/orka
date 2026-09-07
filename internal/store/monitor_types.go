package store

import "time"

// RepositoryMonitorRecord stores normalized RepositoryMonitor metadata.
type RepositoryMonitorRecord struct {
	Namespace  string    `json:"namespace"`
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	RepoURL    string    `json:"repoURL"`
	Owner      string    `json:"owner"`
	Repository string    `json:"repository"`
	Branch     string    `json:"branch"`
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// MonitorRun represents one scheduled, manual, or exact-event repository monitor run.
type MonitorRun struct {
	ID               string     `json:"id"`
	MonitorNamespace string     `json:"monitorNamespace"`
	MonitorName      string     `json:"monitorName"`
	Trigger          string     `json:"trigger"`
	TargetKind       string     `json:"targetKind,omitempty"`
	TargetNumber     int64      `json:"targetNumber,omitempty"`
	TargetSHA        string     `json:"targetSHA,omitempty"`
	CommandEventID   string     `json:"commandEventID,omitempty"`
	Phase            string     `json:"phase"`
	StartedAt        time.Time  `json:"startedAt"`
	CompletedAt      *time.Time `json:"completedAt,omitempty"`
	SelectedCount    int        `json:"selectedCount"`
	CreatedTaskCount int        `json:"createdTaskCount"`
	SkippedCount     int        `json:"skippedCount"`
	Error            string     `json:"error,omitempty"`
}

// MonitorRunFilter constrains monitor run list queries.
type MonitorRunFilter struct {
	Namespace    string
	MonitorName  string
	Trigger      string
	TargetKind   string
	TargetNumber int64
	TargetSHA    string
	Phase        string
	OldestFirst  bool
	Limit        int
	Cursor       string
}

// MonitorItem stores the latest state for one monitored issue, pull request, or commit.
type MonitorItem struct {
	MonitorNamespace    string    `json:"monitorNamespace"`
	MonitorName         string    `json:"monitorName"`
	Kind                string    `json:"kind"`
	ItemKey             string    `json:"itemKey"`
	Number              int64     `json:"number,omitempty"`
	SHA                 string    `json:"sha,omitempty"`
	Title               string    `json:"title,omitempty"`
	Body                string    `json:"body,omitempty"`
	HTMLURL             string    `json:"htmlURL,omitempty"`
	Author              string    `json:"author,omitempty"`
	State               string    `json:"state,omitempty"`
	LabelsJSON          string    `json:"labelsJSON,omitempty"`
	SnapshotDigest      string    `json:"snapshotDigest,omitempty"`
	GitHubUpdatedAt     time.Time `json:"githubUpdatedAt,omitempty"`
	WorkflowPhase       string    `json:"workflowPhase,omitempty"`
	LinkedPRNumber      int64     `json:"linkedPRNumber,omitempty"`
	LastCommandID       string    `json:"lastCommandID,omitempty"`
	LastCommandIntent   string    `json:"lastCommandIntent,omitempty"`
	LastActionID        string    `json:"lastActionID,omitempty"`
	LastActionKind      string    `json:"lastActionKind,omitempty"`
	LastActionTaskName  string    `json:"lastActionTaskName,omitempty"`
	BaseBranch          string    `json:"baseBranch,omitempty"`
	HeadBranch          string    `json:"headBranch,omitempty"`
	HeadSHA             string    `json:"headSHA,omitempty"`
	BaseSHA             string    `json:"baseSHA,omitempty"`
	Draft               bool      `json:"draft,omitempty"`
	MergeableState      string    `json:"mergeableState,omitempty"`
	CIState             string    `json:"ciState,omitempty"`
	SkipReason          string    `json:"skipReason,omitempty"`
	LastReviewID        string    `json:"lastReviewID,omitempty"`
	LastReviewedHeadSHA string    `json:"lastReviewedHeadSHA,omitempty"`
	LastVerdict         string    `json:"lastVerdict,omitempty"`
	RepairState         string    `json:"repairState,omitempty"`
	AutomergeState      string    `json:"automergeState,omitempty"`
	StatusCommentID     string    `json:"statusCommentID,omitempty"`
	StatusCommentURL    string    `json:"statusCommentURL,omitempty"`
	LastPublishID       string    `json:"lastPublishID,omitempty"`
	LastPublishPhase    string    `json:"lastPublishPhase,omitempty"`
	LastPublishReason   string    `json:"lastPublishReason,omitempty"`
	LastPublishURL      string    `json:"lastPublishURL,omitempty"`
	UpdatedAt           time.Time `json:"updatedAt"`
	LastSeenAt          time.Time `json:"lastSeenAt"`
}

// MonitorItemFilter constrains monitor item list queries.
type MonitorItemFilter struct {
	Namespace      string
	MonitorName    string
	Kind           string
	Number         int64
	State          string
	ReviewVerdict  string
	RepairState    string
	AutomergeState string
	Limit          int
	Cursor         string
}

// WorkAction stores one durable workflow action/lease for monitor-owned automation.
type WorkAction struct {
	ID                   string     `json:"id"`
	MonitorNamespace     string     `json:"monitorNamespace"`
	MonitorName          string     `json:"monitorName"`
	RunID                string     `json:"runID,omitempty"`
	CommandEventID       string     `json:"commandEventID,omitempty"`
	MonitorGeneration    int64      `json:"monitorGeneration,omitempty"`
	TargetKind           string     `json:"targetKind,omitempty"`
	TargetNumber         int64      `json:"targetNumber,omitempty"`
	TargetSHA            string     `json:"targetSHA,omitempty"`
	TargetSnapshotDigest string     `json:"targetSnapshotDigest,omitempty"`
	Intent               string     `json:"intent,omitempty"`
	DesiredAction        string     `json:"desiredAction,omitempty"`
	DependsOnActionID    string     `json:"dependsOnActionID,omitempty"`
	DedupeKey            string     `json:"dedupeKey,omitempty"`
	IdempotencyKey       string     `json:"idempotencyKey,omitempty"`
	Status               string     `json:"status"`
	Phase                string     `json:"phase,omitempty"`
	Attempt              int        `json:"attempt"`
	LeaseOwner           string     `json:"leaseOwner,omitempty"`
	LeaseExpiresAt       *time.Time `json:"leaseExpiresAt,omitempty"`
	TaskName             string     `json:"taskName,omitempty"`
	BlockedReason        string     `json:"blockedReason,omitempty"`
	Error                string     `json:"error,omitempty"`
	ArtifactIDs          string     `json:"artifactIDs,omitempty"`
	PayloadDigest        string     `json:"payloadDigest,omitempty"`
	MetadataJSON         string     `json:"metadataJSON,omitempty"`
	CreatedAt            time.Time  `json:"createdAt"`
	UpdatedAt            time.Time  `json:"updatedAt"`
	CompletedAt          *time.Time `json:"completedAt,omitempty"`
}

// WorkActionFilter constrains workflow action list queries.
type WorkActionFilter struct {
	Namespace      string
	MonitorName    string
	TargetKind     string
	TargetNumber   int64
	TargetSHA      string
	Intent         string
	DesiredAction  string
	Status         string
	RunID          string
	CommandEventID string
	TaskName       string
	DedupeKey      string
	Limit          int
	Cursor         string
}

// ImplementationJob tracks issue coding attempts and patch validation state.
type ImplementationJob struct {
	ID                string     `json:"id"`
	MonitorNamespace  string     `json:"monitorNamespace"`
	MonitorName       string     `json:"monitorName"`
	Repo              string     `json:"repo,omitempty"`
	IssueNumber       int64      `json:"issueNumber,omitempty"`
	PlanID            string     `json:"planID,omitempty"`
	SnapshotDigest    string     `json:"snapshotDigest,omitempty"`
	Phase             string     `json:"phase,omitempty"`
	Attempt           int        `json:"attempt"`
	Branch            string     `json:"branch,omitempty"`
	PatchArtifactID   string     `json:"patchArtifactID,omitempty"`
	PRNumber          int64      `json:"prNumber,omitempty"`
	ValidationState   string     `json:"validationState,omitempty"`
	TaskName          string     `json:"taskName,omitempty"`
	MutationTaskName  string     `json:"mutationTaskName,omitempty"`
	CommandEventID    string     `json:"commandEventID,omitempty"`
	WorkActionID      string     `json:"workActionID,omitempty"`
	MonitorGeneration int64      `json:"monitorGeneration,omitempty"`
	Error             string     `json:"error,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	CompletedAt       *time.Time `json:"completedAt,omitempty"`
}

// ImplementationJobFilter constrains implementation job list queries.
type ImplementationJobFilter struct {
	Namespace                 string
	MonitorName               string
	Repo                      string
	IssueNumber               int64
	Phase                     string
	Phases                    []string
	TaskName                  string
	ExcludeWorkActionStatuses []string
	Limit                     int
	Cursor                    string
}

// GitHubMutationRecord stores one controller-owned GitHub write audit record.
type GitHubMutationRecord struct {
	ID                string     `json:"id"`
	MonitorNamespace  string     `json:"monitorNamespace"`
	MonitorName       string     `json:"monitorName"`
	RunID             string     `json:"runID,omitempty"`
	CommandEventID    string     `json:"commandEventID,omitempty"`
	WorkActionID      string     `json:"workActionID,omitempty"`
	MonitorGeneration int64      `json:"monitorGeneration,omitempty"`
	Operation         string     `json:"operation"`
	TargetKind        string     `json:"targetKind,omitempty"`
	TargetNumber      int64      `json:"targetNumber,omitempty"`
	TargetSHA         string     `json:"targetSHA,omitempty"`
	Actor             string     `json:"actor,omitempty"`
	Reason            string     `json:"reason,omitempty"`
	RequestDigest     string     `json:"requestDigest,omitempty"`
	GitHubURL         string     `json:"githubURL,omitempty"`
	GitHubRequestID   string     `json:"githubRequestID,omitempty"`
	ExternalID        string     `json:"externalID,omitempty"`
	Status            string     `json:"status,omitempty"`
	Error             string     `json:"error,omitempty"`
	PendingAt         *time.Time `json:"pendingAt,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
}

// GitHubMutationRecordFilter constrains mutation audit list queries.
type GitHubMutationRecordFilter struct {
	Namespace    string
	MonitorName  string
	Operation    string
	TargetKind   string
	TargetNumber int64
	Status       string
	Limit        int
	Cursor       string
}

// ActionRecord stores one generic typed result from an agent or deterministic controller action.
type ActionRecord struct {
	ID                string    `json:"id"`
	MonitorNamespace  string    `json:"monitorNamespace"`
	MonitorName       string    `json:"monitorName"`
	Kind              string    `json:"kind"`
	Number            int64     `json:"number,omitempty"`
	ActionKind        string    `json:"actionKind"`
	SnapshotDigest    string    `json:"snapshotDigest,omitempty"`
	HeadSHA           string    `json:"headSHA,omitempty"`
	TaskName          string    `json:"taskName,omitempty"`
	CommandEventID    string    `json:"commandEventID,omitempty"`
	WorkActionID      string    `json:"workActionID,omitempty"`
	MonitorGeneration int64     `json:"monitorGeneration,omitempty"`
	Verdict           string    `json:"verdict,omitempty"`
	Confidence        string    `json:"confidence,omitempty"`
	Summary           string    `json:"summary,omitempty"`
	PayloadJSON       string    `json:"payloadJSON,omitempty"`
	PayloadDigest     string    `json:"payloadDigest,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
}

// ActionRecordFilter constrains action record list queries.
type ActionRecordFilter struct {
	Namespace   string
	MonitorName string
	Kind        string
	Number      int64
	ActionKind  string
	TaskName    string
	Limit       int
	Cursor      string
}

// ReviewRecord stores one immutable typed review result.
type ReviewRecord struct {
	ID                      string    `json:"id"`
	MonitorNamespace        string    `json:"monitorNamespace"`
	MonitorName             string    `json:"monitorName"`
	Kind                    string    `json:"kind"`
	Number                  int64     `json:"number,omitempty"`
	HeadSHA                 string    `json:"headSHA,omitempty"`
	TaskName                string    `json:"taskName,omitempty"`
	TaskNamespace           string    `json:"taskNamespace,omitempty"`
	Verdict                 string    `json:"verdict,omitempty"`
	Confidence              string    `json:"confidence,omitempty"`
	Repairable              bool      `json:"repairable,omitempty"`
	SecurityStatus          string    `json:"securityStatus,omitempty"`
	FindingsJSON            string    `json:"findingsJSON,omitempty"`
	Summary                 string    `json:"summary,omitempty"`
	SuggestedComment        string    `json:"suggestedComment,omitempty"`
	ValidationTask          string    `json:"validationTask,omitempty"`
	ValidationImage         string    `json:"validationImage,omitempty"`
	ValidationCommandDigest string    `json:"validationCommandDigest,omitempty"`
	ValidationStatus        string    `json:"validationStatus,omitempty"`
	ValidationEvidence      string    `json:"validationEvidence,omitempty"`
	RenderedComment         string    `json:"renderedComment,omitempty"`
	Marker                  string    `json:"marker,omitempty"`
	GitHubReviewID          string    `json:"githubReviewID,omitempty"`
	GitHubCommentID         string    `json:"githubCommentID,omitempty"`
	GitHubCommentURL        string    `json:"githubCommentURL,omitempty"`
	CreatedAt               time.Time `json:"createdAt"`
}

// ReviewRecordFilter constrains review record list queries.
type ReviewRecordFilter struct {
	Namespace   string
	MonitorName string
	Kind        string
	Number      int64
	HeadSHA     string
	Verdict     string
	Limit       int
	Cursor      string
}

// ReviewPublishRecord stores one GitHub review publish attempt/outcome.
type ReviewPublishRecord struct {
	ID                 string    `json:"id"`
	MonitorNamespace   string    `json:"monitorNamespace"`
	MonitorName        string    `json:"monitorName"`
	ItemKind           string    `json:"itemKind"`
	ItemNumber         int64     `json:"itemNumber,omitempty"`
	HeadSHA            string    `json:"headSHA,omitempty"`
	RunID              string    `json:"runID,omitempty"`
	ReviewTaskName     string    `json:"reviewTaskName,omitempty"`
	ReviewRecordID     string    `json:"reviewRecordID,omitempty"`
	Phase              string    `json:"phase"`
	Event              string    `json:"event,omitempty"`
	GitHubReviewID     string    `json:"githubReviewID,omitempty"`
	GitHubReviewURL    string    `json:"githubReviewURL,omitempty"`
	BodyDigest         string    `json:"bodyDigest,omitempty"`
	InlineCommentCount int       `json:"inlineCommentCount"`
	SkipReason         string    `json:"skipReason,omitempty"`
	Error              string    `json:"error,omitempty"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// ReviewPublishRecordFilter constrains review publish record list queries.
type ReviewPublishRecordFilter struct {
	Namespace      string
	MonitorName    string
	ItemKind       string
	ItemNumber     int64
	HeadSHA        string
	ReviewRecordID string
	Phase          string
	Limit          int
	Cursor         string
}

// CommandEventFilter constrains command event list queries.
type CommandEventFilter struct {
	Namespace   string
	MonitorName string
	Kind        string
	Number      int64
	Intent      string
	Status      string
	Limit       int
	Cursor      string
}

// CommandEvent stores one maintainer command intake event.
type CommandEvent struct {
	ID                  string     `json:"id"`
	MonitorNamespace    string     `json:"monitorNamespace"`
	MonitorName         string     `json:"monitorName"`
	Repo                string     `json:"repo,omitempty"`
	Kind                string     `json:"kind,omitempty"`
	Number              int64      `json:"number,omitempty"`
	Source              string     `json:"source,omitempty"`
	DeliveryID          string     `json:"deliveryID,omitempty"`
	Label               string     `json:"label,omitempty"`
	MonitorGeneration   int64      `json:"monitorGeneration,omitempty"`
	DedupeKey           string     `json:"dedupeKey,omitempty"`
	IdempotencyKey      string     `json:"idempotencyKey,omitempty"`
	CommentID           string     `json:"commentID,omitempty"`
	CommentURL          string     `json:"commentURL,omitempty"`
	Author              string     `json:"author,omitempty"`
	AuthorAssociation   string     `json:"authorAssociation,omitempty"`
	Permission          string     `json:"permission,omitempty"`
	Command             string     `json:"command,omitempty"`
	Intent              string     `json:"intent,omitempty"`
	HeadSHA             string     `json:"headSHA,omitempty"`
	IssueSnapshotDigest string     `json:"issueSnapshotDigest,omitempty"`
	Status              string     `json:"status,omitempty"`
	StatusCommentID     string     `json:"statusCommentID,omitempty"`
	CreatedRepairJobID  string     `json:"createdRepairJobID,omitempty"`
	CreatedAt           time.Time  `json:"createdAt"`
	ProcessedAt         *time.Time `json:"processedAt,omitempty"`
	Error               string     `json:"error,omitempty"`
}

// RepairJob stores durable repair and automerge state.
type RepairJob struct {
	ID                 string     `json:"id"`
	MonitorNamespace   string     `json:"monitorNamespace"`
	MonitorName        string     `json:"monitorName"`
	Repo               string     `json:"repo,omitempty"`
	PRNumber           int64      `json:"prNumber,omitempty"`
	Intent             string     `json:"intent,omitempty"`
	Source             string     `json:"source,omitempty"`
	HeadSHA            string     `json:"headSHA,omitempty"`
	BaseSHA            string     `json:"baseSHA,omitempty"`
	BaseBranch         string     `json:"baseBranch,omitempty"`
	Phase              string     `json:"phase,omitempty"`
	RepairCountPR      int        `json:"repairCountPR"`
	RepairCountHead    int        `json:"repairCountHead"`
	ValidationAttempts int        `json:"validationAttempts"`
	ReviewFixAttempts  int        `json:"reviewFixAttempts"`
	TaskName           string     `json:"taskName,omitempty"`
	Branch             string     `json:"branch,omitempty"`
	PushedSHA          string     `json:"pushedSHA,omitempty"`
	LastError          string     `json:"lastError,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	CompletedAt        *time.Time `json:"completedAt,omitempty"`
}

// RepairJobFilter constrains repair job list queries.
type RepairJobFilter struct {
	Namespace   string
	MonitorName string
	Repo        string
	PRNumber    int64
	Intent      string
	Phase       string
	Limit       int
	Cursor      string
}

// MonitorEvent stores an append-only monitor audit event.
type MonitorEvent struct {
	ID               string    `json:"id"`
	MonitorNamespace string    `json:"monitorNamespace"`
	MonitorName      string    `json:"monitorName"`
	RunID            string    `json:"runID,omitempty"`
	ItemKind         string    `json:"itemKind,omitempty"`
	ItemNumber       int64     `json:"itemNumber,omitempty"`
	ItemSHA          string    `json:"itemSHA,omitempty"`
	EventType        string    `json:"eventType"`
	Actor            string    `json:"actor,omitempty"`
	Summary          string    `json:"summary,omitempty"`
	MetadataJSON     string    `json:"metadataJSON,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
}

// MonitorEventFilter constrains monitor event list queries.
type MonitorEventFilter struct {
	Namespace   string
	ID          string
	MonitorName string
	RunID       string
	ItemKind    string
	ItemNumber  int64
	EventType   string
	Limit       int
	Cursor      string
}
