/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

// Package workspace defines Orka's internal durable-workspace execution boundary.
//
// The package intentionally models the small set of operations Orka needs from a
// workspace backend without importing agent-sandbox client or CRD types. Real
// agent-sandbox integration should live behind an adapter that implements
// WorkspaceExecutor.
package workspace

import (
	"context"
	"time"
)

// WorkspaceExecutor is the backend-neutral contract for durable execution
// workspaces. Implementations must honor context cancellation and timeouts on
// every operation.
type WorkspaceExecutor interface {
	Claim(ctx context.Context, req ClaimRequest) (*ClaimResult, error)
	WaitReady(ctx context.Context, req WaitReadyRequest) (*ReadyResult, error)
	Exec(ctx context.Context, req ExecRequest) (*ExecResult, error)
	Upload(ctx context.Context, req UploadRequest) (*UploadResult, error)
	Download(ctx context.Context, req DownloadRequest) (*DownloadResult, error)
	Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error)
	Delete(ctx context.Context, req DeleteRequest) (*DeleteResult, error)
	Describe(ctx context.Context, req DescribeRequest) (*Description, error)
}

// Phase is Orka's internal, backend-neutral view of workspace lifecycle state.
type Phase string

const (
	// PhasePending means a workspace claim exists but the execution environment is not ready yet.
	PhasePending Phase = "Pending"
	// PhaseReady means a workspace can accept commands and artifact operations.
	PhaseReady Phase = "Ready"
	// PhaseReleased means the active task has disconnected but the workspace was not explicitly retained.
	PhaseReleased Phase = "Released"
	// PhaseRetained means the workspace should survive task completion and may be reused by policy.
	PhaseRetained Phase = "Retained"
	// PhaseDeleted means the workspace was destroyed or is no longer usable.
	PhaseDeleted Phase = "Deleted"
	// PhaseFailed means the workspace reached a terminal setup or runtime failure.
	PhaseFailed Phase = "Failed"
)

// WorkspaceRef identifies a claimed workspace without exposing backend-native
// object types. ClaimName is the stable Orka-facing identity; SandboxName and ID
// are optional backend adapter details.
type WorkspaceRef struct {
	Namespace   string `json:"namespace,omitempty"`
	ClaimName   string `json:"claimName,omitempty"`
	SandboxName string `json:"sandboxName,omitempty"`
	ID          string `json:"id,omitempty"`
}

// IsZero returns true when the reference has no usable identity.
func (r WorkspaceRef) IsZero() bool {
	return r.Namespace == "" && r.ClaimName == "" && r.SandboxName == "" && r.ID == ""
}

// TemplateRef identifies a platform-managed workspace template.
type TemplateRef struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
}

// Placement captures non-secret runtime placement metadata for a workspace.
type Placement struct {
	WorkerNamespace string `json:"workerNamespace,omitempty"`
	WorkerPool      string `json:"workerPool,omitempty"`
	WorkerPodName   string `json:"workerPodName,omitempty"`
	PodIP           string `json:"podIP,omitempty"`
}

// IsZero reports whether no placement metadata is available.
func (p Placement) IsZero() bool {
	return p.WorkerNamespace == "" && p.WorkerPool == "" && p.WorkerPodName == "" && p.PodIP == ""
}

// Density captures non-secret actor/worker density metadata for a workspace provider.
type Density struct {
	WorkerCount         int    `json:"workerCount,omitempty"`
	ActorCount          int    `json:"actorCount,omitempty"`
	RunningActorCount   int    `json:"runningActorCount,omitempty"`
	SuspendedActorCount int    `json:"suspendedActorCount,omitempty"`
	ActorsPerWorker     string `json:"actorsPerWorker,omitempty"`
}

// IsZero reports whether no density metadata is available.
func (d Density) IsZero() bool {
	return d.WorkerCount == 0 &&
		d.ActorCount == 0 &&
		d.RunningActorCount == 0 &&
		d.SuspendedActorCount == 0 &&
		d.ActorsPerWorker == ""
}

// ClaimRequest asks the executor to create or reuse a workspace claim.
type ClaimRequest struct {
	Namespace string
	TaskName  string
	ClaimName string
	// CreateIfMissing allows executors that support named claims to create
	// ClaimName when reattach misses.
	CreateIfMissing bool
	Template        TemplateRef
	ReuseKey        string
	// WarmPoolPolicy is retained for the legacy agent-sandbox worker environment
	// contract. With agent-sandbox v1, Template.Name identifies the
	// SandboxWarmPool to claim.
	WarmPoolPolicy string
	Labels         map[string]string
	Annotations    map[string]string
	Timeout        time.Duration
	// MaxRequestTimeout is the longest expected single SDK request against
	// the claim's HTTP transport (e.g. a long-running Exec call). Executors
	// that build a per-claim HTTP client may use this to widen transport
	// timeouts (RequestTimeout / PerAttemptTimeout / ResponseHeaderTimeout)
	// while keeping Timeout as the deadline for the claim operation itself.
	// When zero, executors fall back to Timeout.
	MaxRequestTimeout time.Duration
}

// ClaimResult describes the outcome of claiming a workspace.
type ClaimResult struct {
	Ref       WorkspaceRef
	Template  TemplateRef
	ReuseKey  string
	Created   bool
	Reused    bool
	Phase     Phase
	Message   string
	ClaimedAt time.Time
	Placement Placement
}

// WaitReadyRequest waits until a workspace can execute commands.
type WaitReadyRequest struct {
	Ref                   WorkspaceRef
	Timeout               time.Duration
	Boot                  bool
	SnapshotRestoreURI    string
	SkipDaemonHealthCheck bool
}

// ReadyResult describes a workspace that became ready.
type ReadyResult struct {
	Ref           WorkspaceRef
	Phase         Phase
	Message       string
	ReadyAt       time.Time
	Placement     Placement
	Density       Density
	ResumeLatency time.Duration
}

// ExecRequest executes a command inside a ready workspace.
type ExecRequest struct {
	Ref            WorkspaceRef
	Command        []string
	Env            map[string]string
	WorkDir        string
	Stdin          []byte
	Timeout        time.Duration
	MaxOutputBytes int64
	Resident       bool
	ResidentKey    string
}

// ExecResult captures command execution output and metadata.
type ExecResult struct {
	Ref             WorkspaceRef
	Command         []string
	Stdout          string
	Stderr          string
	ExitCode        int
	StartedAt       time.Time
	FinishedAt      time.Time
	Artifacts       []Artifact
	StdoutTruncated bool
	StderrTruncated bool
}

// Duration returns the elapsed wall-clock time recorded for the command.
func (r ExecResult) Duration() time.Duration {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		return 0
	}
	return r.FinishedAt.Sub(r.StartedAt)
}

// Succeeded returns true when the command exited successfully.
func (r ExecResult) Succeeded() bool {
	return r.ExitCode == 0
}

// UploadRequest writes artifacts into a workspace.
type UploadRequest struct {
	Ref       WorkspaceRef
	Artifacts []UploadArtifact
	Timeout   time.Duration
	// BootstrapHandoff asks backends with a separate daemon bootstrap credential
	// to use it for the initial per-task handoff token upload.
	BootstrapHandoff bool
}

// UploadArtifact is an artifact payload to write into a workspace.
type UploadArtifact struct {
	Path    string
	Data    []byte
	Mode    uint32
	ModTime time.Time
}

// Artifact is backend-neutral artifact metadata.
type Artifact struct {
	Path    string
	Size    int64
	Digest  string
	Mode    uint32
	ModTime time.Time
}

// UploadResult lists artifacts written into a workspace.
type UploadResult struct {
	Ref       WorkspaceRef
	Artifacts []Artifact
}

// DownloadRequest reads artifacts from a workspace. When Paths is empty, all
// available artifacts are downloaded.
type DownloadRequest struct {
	Ref     WorkspaceRef
	Paths   []string
	Timeout time.Duration
}

// DownloadedArtifact is an artifact payload read from a workspace.
type DownloadedArtifact struct {
	Artifact
	Data []byte
}

// DownloadResult lists artifacts read from a workspace.
type DownloadResult struct {
	Ref       WorkspaceRef
	Artifacts []DownloadedArtifact
}

// ReleaseRequest disconnects the active task from a workspace. Retain marks the
// workspace as intentionally preserved for future reuse or operator inspection.
type ReleaseRequest struct {
	Ref     WorkspaceRef
	Retain  bool
	Reason  string
	Timeout time.Duration
	// SkipScrub bypasses backend daemon scrubbing when the caller knows the
	// current handoff credential is unavailable or not yet installed.
	SkipScrub bool
	// SnapshotCheckpointURI asks backends that support explicit snapshot
	// addressing to checkpoint to this provider-native URI before releasing.
	SnapshotCheckpointURI string
}

// ReleaseResult describes the release/retain decision.
type ReleaseResult struct {
	Ref      WorkspaceRef
	Released bool
	Retained bool
	Phase    Phase
	Message  string
}

// DeleteRequest destroys a workspace.
type DeleteRequest struct {
	Ref       WorkspaceRef
	Reason    string
	Timeout   time.Duration
	SkipScrub bool
}

// DeleteResult describes workspace deletion.
type DeleteResult struct {
	Ref     WorkspaceRef
	Deleted bool
	Phase   Phase
	Message string
}

// DescribeRequest reads the current executor view of a workspace.
type DescribeRequest struct {
	Ref WorkspaceRef
}

// Description is a backend-neutral workspace snapshot.
type Description struct {
	Ref         WorkspaceRef
	Template    TemplateRef
	ReuseKey    string
	Phase       Phase
	Retained    bool
	Message     string
	CreatedAt   time.Time
	ReadyAt     time.Time
	ReleasedAt  time.Time
	DeletedAt   time.Time
	Placement   Placement
	Density     Density
	Labels      map[string]string
	Annotations map[string]string
	Artifacts   []Artifact
}
