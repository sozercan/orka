package store

import (
	"context"
	"time"
)

// SessionTypeGateway identifies canonical Sessions exclusively owned by gateway admission/projection.
const SessionTypeGateway = "gateway"

// SessionRecord represents a full session.
type SessionRecord struct {
	Namespace     string
	Name          string
	SessionType   string // "task", "chat", or "gateway"
	ActiveTask    string
	ActiveTaskUID string
	MessageCount  int
	InputTokens   int
	OutputTokens  int
	Cancelled     bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Messages      []SessionMessage
}

// SessionMetadata is the lightweight listing representation.
type SessionMetadata struct {
	Name          string
	SessionType   string
	MessageCount  int
	InputTokens   int
	OutputTokens  int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ActiveTask    string
	ActiveTaskUID string
}

// SessionMessage is a single transcript entry.
type SessionMessage struct {
	ID         string            `json:"id"`
	Order      int64             `json:"order,omitempty"`
	Role       string            `json:"role"`
	Content    string            `json:"content"`
	Name       string            `json:"name,omitempty"`
	Input      map[string]any    `json:"input,omitempty"`
	ToolCalls  any               `json:"toolCalls,omitempty"`
	ToolCallID string            `json:"toolCallID,omitempty"`
	SourceType string            `json:"sourceType,omitempty"`
	SourceRef  string            `json:"sourceRef,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Timestamp  time.Time         `json:"ts"`
}

// ArtifactMetadata describes a stored artifact file.
type ArtifactMetadata struct {
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size"`
	CreatedAt   string `json:"createdAt"`
}

// PlanState represents the autonomous plan state for a coordinator task.
type PlanState struct {
	TaskName     string
	Namespace    string
	Iteration    int
	Summary      string // Human-readable progress summary
	ProgressPct  int    // 0-100 progress estimate
	GoalComplete bool   // LLM determined goal is met
	PlanDocument string // Freeform markdown plan managed by LLM
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Memory represents durable namespace-scoped memory captured from tasks,
// sessions, or explicit worker calls.
type Memory struct {
	ID               string     `json:"id"`
	Namespace        string     `json:"namespace"`
	SessionName      string     `json:"sessionName,omitempty"`
	AgentName        string     `json:"agentName,omitempty"`
	TaskName         string     `json:"taskName,omitempty"`
	ParentTask       string     `json:"parentTask,omitempty"`
	Source           string     `json:"source"`
	SourceProposalID string     `json:"sourceProposalId,omitempty"`
	Content          string     `json:"content"`
	Tags             []string   `json:"tags,omitempty"`
	Disabled         bool       `json:"disabled"`
	Deleted          bool       `json:"deleted"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
	LastRecalledAt   *time.Time `json:"lastRecalledAt,omitempty"`
	RecalledCount    int        `json:"recalledCount"`
}

// MemoryFilter constrains memory list and recall queries.
type MemoryFilter struct {
	Namespace       string
	Query           string
	SessionName     string
	AgentName       string
	TaskName        string
	ParentTask      string
	Source          string
	Tags            []string
	IDs             []string
	IncludeDisabled bool
	IncludeDeleted  bool
	Limit           int
}

// TranscriptSearchFilter constrains transcript-backed recall/search.
type TranscriptSearchFilter struct {
	Namespace          string
	Query              string
	SessionName        string
	ExcludeSessionName string
	Roles              []string
	Limit              int
	MaxSnippetLength   int
}

// TranscriptSearchResult is a compact prior transcript hit.
type TranscriptSearchResult struct {
	SessionName     string    `json:"sessionName"`
	MessageID       int64     `json:"messageId"`
	StableMessageID string    `json:"stableMessageId,omitempty"`
	Role            string    `json:"role"`
	Name            string    `json:"name,omitempty"`
	Snippet         string    `json:"snippet"`
	CreatedAt       time.Time `json:"createdAt"`
}

// MemoryProposal represents a proposed memory-adjacent change such as a reusable skill.
type MemoryProposal struct {
	ID              string     `json:"id"`
	Namespace       string     `json:"namespace"`
	TaskName        string     `json:"taskName,omitempty"`
	AgentName       string     `json:"agentName,omitempty"`
	Type            string     `json:"type"`
	SkillName       string     `json:"skillName,omitempty"`
	Title           string     `json:"title"`
	Description     string     `json:"description,omitempty"`
	Content         string     `json:"content,omitempty"`
	Patch           string     `json:"patch,omitempty"`
	Status          string     `json:"status"`
	Reviewer        string     `json:"reviewer,omitempty"`
	ReviewNote      string     `json:"reviewNote,omitempty"`
	AppliedMemoryID string     `json:"appliedMemoryId,omitempty"`
	AppliedBy       string     `json:"appliedBy,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
	ReviewedAt      *time.Time `json:"reviewedAt,omitempty"`
	AppliedAt       *time.Time `json:"appliedAt,omitempty"`
}

// MemoryProposalFilter constrains proposal list queries.
type MemoryProposalFilter struct {
	Namespace string
	TaskName  string
	AgentName string
	Type      string
	Status    string
	Query     string
	Limit     int
}

// MemoryProposalReview records governance review decisions for proposals.
type MemoryProposalReview struct {
	Namespace  string
	ID         string
	Status     string
	Reviewer   string
	ReviewNote string
}

// MemoryProposalApply records an explicit proposal application request.
type MemoryProposalApply struct {
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
	AppliedBy string `json:"appliedBy"`
	// AuthorizeExistingMemory runs inside the apply transaction before returning
	// an existing memory or changing the proposal to reference it. New memories
	// are covered by the caller's separate create permission.
	AuthorizeExistingMemory func(context.Context, string, string) error `json:"-"`
}
