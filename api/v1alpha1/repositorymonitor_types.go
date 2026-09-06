/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RepositoryMonitorSpec defines the desired state of RepositoryMonitor.
type RepositoryMonitorSpec struct {
	// Provider is the source control provider. GitHub is the only supported v1 provider.
	// +kubebuilder:validation:Enum=github
	// +kubebuilder:default=github
	// +optional
	Provider string `json:"provider,omitempty"`

	// RepoURL is the repository URL to monitor.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(https://github[.]com/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+([.]git)?/?|git@github[.]com:[A-Za-z0-9._-]+/[A-Za-z0-9._-]+([.]git)?)$`
	RepoURL string `json:"repoURL"`

	// Owner is the repository owner or organization.
	// +optional
	Owner string `json:"owner,omitempty"`

	// Repository is the repository name.
	// +optional
	Repository string `json:"repository,omitempty"`

	// Branch is the default base branch for repository-wide monitoring decisions.
	// +kubebuilder:default=main
	// +optional
	Branch string `json:"branch,omitempty"`

	// GitSecretRef is the backward-compatible source-read credential reference.
	// ReadCredentialRef takes precedence when both are set. GitSecretRef is never
	// used for publication writes or forge mutations.
	// +optional
	GitSecretRef *corev1.LocalObjectReference `json:"gitSecretRef,omitempty"`

	// ReadCredentialRef references the source-repository clone/read credential.
	// It is resolved only by the clean-room workspace boundary. When omitted,
	// GitSecretRef remains the backward-compatible read-only fallback.
	// +optional
	ReadCredentialRef *corev1.LocalObjectReference `json:"readCredentialRef,omitempty"`

	// PublicationReadCredentialRef references the target-repository read
	// credential used only for publication preflight and independent verification.
	// Write workflows require this explicit reference.
	// +optional
	PublicationReadCredentialRef *corev1.LocalObjectReference `json:"publicationReadCredentialRef,omitempty"`

	// PublicationCredentialRef references the target-repository write credential
	// used only for exact compare-and-swap publication. Write workflows require
	// this explicit reference.
	// +optional
	PublicationCredentialRef *corev1.LocalObjectReference `json:"publicationCredentialRef,omitempty"`

	// ForgeCredentialRef references the GitHub API credential used only for
	// controller-owned forge reads and mutations. Write workflows and GitHub label
	// triggers require this explicit reference.
	// +optional
	ForgeCredentialRef *corev1.LocalObjectReference `json:"forgeCredentialRef,omitempty"`

	// Schedule is the cron expression for background monitor runs.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// TimeZone is the IANA time zone for the schedule.
	// +optional
	TimeZone *string `json:"timeZone,omitempty"`

	// Suspend pauses scheduled monitor runs.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// Targets selects the repository item types covered by this monitor.
	// +optional
	Targets RepositoryMonitorTargets `json:"targets,omitempty"`

	// Triggers configures external events that create durable monitor commands.
	// +optional
	Triggers RepositoryMonitorTriggers `json:"triggers,omitempty"`

	// Agents configures the agents used by monitor review, issue, and repair tasks.
	// +optional
	Agents RepositoryMonitorAgents `json:"agents,omitempty"`

	// IssueWorkflow controls issue triage, research, planning, and implementation behavior.
	// +optional
	IssueWorkflow RepositoryMonitorIssueWorkflowSpec `json:"issueWorkflow,omitempty"`

	// Review controls pull-request review behavior.
	// +optional
	Review RepositoryMonitorReviewSpec `json:"review,omitempty"`

	// Repair controls bounded repair behavior.
	// +optional
	Repair RepositoryMonitorRepairSpec `json:"repair,omitempty"`

	// Automerge controls deterministic merge behavior.
	// +optional
	Automerge RepositoryMonitorAutomergeSpec `json:"automerge,omitempty"`

	// Policy contains authorization and safety policy for monitor operations.
	// +optional
	Policy RepositoryMonitorPolicySpec `json:"policy,omitempty"`

	// Validation configures isolated validation for pull request reviews.
	// +optional
	Validation RepositoryMonitorValidationSpec `json:"validation,omitempty"`
}

// RepositoryMonitorTargets configures monitored repository item types.
type RepositoryMonitorTargets struct {
	// PullRequests configures pull request monitoring.
	// +optional
	PullRequests RepositoryMonitorPullRequestTarget `json:"pullRequests,omitempty"`

	// Issues configures issue monitoring.
	// +optional
	Issues RepositoryMonitorIssueTarget `json:"issues,omitempty"`

	// Commits configures commit monitoring.
	// +optional
	Commits RepositoryMonitorCommitTarget `json:"commits,omitempty"`
}

// RepositoryMonitorPullRequestTarget configures pull request monitoring.
type RepositoryMonitorPullRequestTarget struct {
	// Enabled enables pull request monitoring.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// IncludeDrafts allows draft pull requests to be selected for review.
	// +optional
	IncludeDrafts bool `json:"includeDrafts,omitempty"`

	// MaxPerRun limits pull requests selected by one background run.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MaxPerRun *int32 `json:"maxPerRun,omitempty"`
}

// RepositoryMonitorIssueTarget configures issue monitoring.
type RepositoryMonitorIssueTarget struct {
	// Enabled enables issue monitoring.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MaxPerRun limits issues selected by one background run.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxPerRun *int32 `json:"maxPerRun,omitempty"`

	// IncludeLabels optionally restricts issue inventory to issues with any of these labels.
	// +listType=set
	// +optional
	IncludeLabels []string `json:"includeLabels,omitempty"`

	// ExcludeLabels excludes matching issues from actionable inventory.
	// +listType=set
	// +optional
	ExcludeLabels []string `json:"excludeLabels,omitempty"`
}

// RepositoryMonitorCommitTarget configures commit monitoring.
type RepositoryMonitorCommitTarget struct {
	// Enabled enables commit monitoring.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MaxPerRun limits commits selected by one background run.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxPerRun *int32 `json:"maxPerRun,omitempty"`
}

// RepositoryMonitorTriggers configures external RepositoryMonitor triggers.
type RepositoryMonitorTriggers struct {
	// GitHub configures GitHub webhook triggers.
	// +optional
	GitHub RepositoryMonitorGitHubTriggers `json:"github,omitempty"`
}

// RepositoryMonitorGitHubTriggers configures GitHub-specific monitor triggers.
type RepositoryMonitorGitHubTriggers struct {
	// Labels maps GitHub labels to durable RepositoryMonitor commands.
	// +optional
	Labels RepositoryMonitorGitHubLabelTriggers `json:"labels,omitempty"`
}

// RepositoryMonitorGitHubLabelTriggers configures orka:* label command intake.
type RepositoryMonitorGitHubLabelTriggers struct {
	// Enabled enables durable command intake for configured GitHub labels.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ConsumeCommandLabels removes accepted one-shot command labels after durable intake.
	// +optional
	ConsumeCommandLabels bool `json:"consumeCommandLabels,omitempty"`

	// RequireActorPermission is the minimum GitHub permission for mutating/code-executing commands.
	// Supported values are write, maintain, and admin. Defaults to write.
	// +kubebuilder:validation:Enum=write;maintain;admin
	// +kubebuilder:default=write
	// +optional
	RequireActorPermission string `json:"requireActorPermission,omitempty"`

	// Issues maps issue command intents to label names. Empty fields use the default orka:* labels.
	// +optional
	Issues RepositoryMonitorIssueCommandLabels `json:"issues,omitempty"`

	// PullRequests maps pull-request command intents to label names. Empty fields use the default orka:* labels.
	// +optional
	PullRequests RepositoryMonitorPullRequestCommandLabels `json:"pullRequests,omitempty"`
}

// RepositoryMonitorIssueCommandLabels configures issue command label names.
type RepositoryMonitorIssueCommandLabels struct {
	// +optional
	Triage string `json:"triage,omitempty"`
	// +optional
	Research string `json:"research,omitempty"`
	// +optional
	Plan string `json:"plan,omitempty"`
	// +optional
	ApprovePlan string `json:"approvePlan,omitempty"`
	// +optional
	Implement string `json:"implement,omitempty"`
	// +optional
	Decompose string `json:"decompose,omitempty"`
	// +optional
	Stop string `json:"stop,omitempty"`
	// +optional
	Resume string `json:"resume,omitempty"`
}

// RepositoryMonitorPullRequestCommandLabels configures pull-request command label names.
type RepositoryMonitorPullRequestCommandLabels struct {
	// +optional
	Review string `json:"review,omitempty"`
	// +optional
	Fix string `json:"fix,omitempty"`
	// +optional
	FixCI string `json:"fixCI,omitempty"`
	// +optional
	UpdateBranch string `json:"updateBranch,omitempty"`
	// +optional
	Automerge string `json:"automerge,omitempty"`
	// +optional
	Stop string `json:"stop,omitempty"`
	// +optional
	Resume string `json:"resume,omitempty"`
}

// RepositoryMonitorAgents configures task agents for monitor workflows.
type RepositoryMonitorAgents struct {
	// Reviewer is the agent used for pull-request review tasks.
	// +optional
	Reviewer *AgentReference `json:"reviewer,omitempty"`

	// Triager is the agent used for issue triage tasks.
	// +optional
	Triager *AgentReference `json:"triager,omitempty"`

	// Researcher is the agent used for issue research tasks.
	// +optional
	Researcher *AgentReference `json:"researcher,omitempty"`

	// Planner is the agent used for issue planning tasks.
	// +optional
	Planner *AgentReference `json:"planner,omitempty"`

	// Repairer is the agent used for repair tasks.
	// +optional
	Repairer *AgentReference `json:"repairer,omitempty"`

	// Implementer is the agent used for guarded issue implementation tasks.
	// +optional
	Implementer *AgentReference `json:"implementer,omitempty"`
}

// RepositoryMonitorIssueWorkflowSpec configures issue workflow phases.
type RepositoryMonitorIssueWorkflowSpec struct {
	// Triage controls read-only issue classification.
	// +optional
	Triage RepositoryMonitorIssueWorkflowPhaseSpec `json:"triage,omitempty"`

	// Research controls read-only issue research.
	// +optional
	Research RepositoryMonitorIssueWorkflowPhaseSpec `json:"research,omitempty"`

	// Planning controls read-only implementation plan generation.
	// +optional
	Planning RepositoryMonitorIssuePlanningSpec `json:"planning,omitempty"`

	// Implementation controls bounded implementation tasks.
	// +optional
	Implementation RepositoryMonitorIssueImplementationSpec `json:"implementation,omitempty"`
}

// RepositoryMonitorIssueWorkflowPhaseSpec configures a read-only issue phase.
type RepositoryMonitorIssueWorkflowPhaseSpec struct {
	// Enabled enables this phase. Defaults to true when the corresponding agent is configured and a command requests it.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// RepositoryMonitorIssuePlanningSpec configures planning behavior.
type RepositoryMonitorIssuePlanningSpec struct {
	// Enabled enables planning. Defaults to true when a planner agent is configured and a command requests it.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// RequireHumanApprovalFor names risk levels or plan categories that require explicit approval.
	// +listType=set
	// +optional
	RequireHumanApprovalFor []string `json:"requireHumanApprovalFor,omitempty"`
}

// RepositoryMonitorIssueImplementationSpec configures implementation behavior.
type RepositoryMonitorIssueImplementationSpec struct {
	// Enabled enables implementation. Defaults to true when an implementer agent is configured and a command requests it.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// RequireApprovedPlan blocks implementation unless the latest plan was approved.
	// +optional
	RequireApprovedPlan *bool `json:"requireApprovedPlan,omitempty"`

	// BranchPrefix is the branch prefix for implementation push branches. Defaults to orka/issue.
	// +optional
	BranchPrefix string `json:"branchPrefix,omitempty"`

	// MaxActive bounds concurrently active issue implementation/mutation jobs per monitor. Defaults to 2.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxActive *int32 `json:"maxActive,omitempty"`

	// MaxAttemptsPerIssue bounds implementation attempts for one issue. Defaults to 2.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxAttemptsPerIssue *int32 `json:"maxAttemptsPerIssue,omitempty"`

	// MaxChangedFiles bounds changed files in an implementation patch. Defaults to 12.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxChangedFiles *int32 `json:"maxChangedFiles,omitempty"`

	// AllowedPaths optionally restricts implementation patch files to these path globs/prefixes.
	// Examples: api/**, internal/**, docs/**.
	// +listType=set
	// +optional
	AllowedPaths []string `json:"allowedPaths,omitempty"`
}

// RepositoryMonitorReviewSpec configures review behavior.
type RepositoryMonitorReviewSpec struct {
	// Event is the legacy/default GitHub review event value included in review task input.
	// It does not control RepositoryMonitor GitHub publishing; use Publish.Event.
	// +kubebuilder:validation:Enum=COMMENT;APPROVE;REQUEST_CHANGES
	// +kubebuilder:default=COMMENT
	// +optional
	Event string `json:"event,omitempty"`

	// RequireGreenCI requires acceptable CI before background review selection.
	// +optional
	RequireGreenCI bool `json:"requireGreenCI,omitempty"`

	// StaleReviewTTL bounds how long an unchanged head review remains fresh.
	// +optional
	StaleReviewTTL *metav1.Duration `json:"staleReviewTTL,omitempty"`

	// ExactEventEnabled enables exact-head review from repository events.
	// +optional
	ExactEventEnabled bool `json:"exactEventEnabled,omitempty"`

	// Publish controls deterministic GitHub pull request review publishing after review ingestion.
	// Publishing is disabled by default and V1 only supports neutral COMMENT reviews.
	// +optional
	Publish RepositoryMonitorReviewPublishSpec `json:"publish,omitempty"`
}

// RepositoryMonitorReviewPublishSpec configures safe GitHub review publishing.
type RepositoryMonitorReviewPublishSpec struct {
	// Enabled enables GitHub pull request review publishing. Defaults to false.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Mode selects whether Orka publishes only a deterministic summary or also eligible inline findings.
	// +kubebuilder:validation:Enum=summary_only;summary_with_inline_findings
	// +kubebuilder:default=summary_only
	// +optional
	Mode string `json:"mode,omitempty"`

	// Event is the GitHub review event to submit. V1 only supports COMMENT.
	// +kubebuilder:validation:Enum=COMMENT
	// +kubebuilder:default=COMMENT
	// +optional
	Event string `json:"event,omitempty"`

	// PostPassed controls whether clean/passed reviews are posted. Defaults to false.
	// +optional
	PostPassed *bool `json:"postPassed,omitempty"`

	// PostNeedsChanges controls whether needs_changes reviews are posted. Defaults to true.
	// +optional
	PostNeedsChanges *bool `json:"postNeedsChanges,omitempty"`

	// PostNeedsHuman controls whether needs_human reviews are posted. Defaults to true.
	// +optional
	PostNeedsHuman *bool `json:"postNeedsHuman,omitempty"`

	// PostSecuritySensitive allows public publishing of security_sensitive findings when true.
	// Defaults to false.
	// +optional
	PostSecuritySensitive bool `json:"postSecuritySensitive,omitempty"`

	// SameHeadPolicy controls duplicate handling for one monitor, PR, and exact head SHA.
	// V1 only supports skip.
	// +kubebuilder:validation:Enum=skip
	// +kubebuilder:default=skip
	// +optional
	SameHeadPolicy string `json:"sameHeadPolicy,omitempty"`

	// Inline controls optional inline review comments for eligible findings.
	// +optional
	Inline RepositoryMonitorReviewPublishInlineSpec `json:"inline,omitempty"`
}

// RepositoryMonitorReviewPublishInlineSpec configures optional inline GitHub review comments.
type RepositoryMonitorReviewPublishInlineSpec struct {
	// Enabled enables inline comments when Mode is summary_with_inline_findings.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MinPriority is the lowest finding priority eligible for inline comments. Defaults to P2.
	// +kubebuilder:validation:Enum=P0;P1;P2;P3
	// +kubebuilder:default=P2
	// +optional
	MinPriority string `json:"minPriority,omitempty"`

	// MaxComments caps inline comments per review. Defaults to 10.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +optional
	MaxComments *int32 `json:"maxComments,omitempty"`
}

// RepositoryMonitorRepairSpec configures bounded repair behavior.
type RepositoryMonitorRepairSpec struct {
	// Enabled enables repair jobs.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MaxRepairsPerPR bounds total automated repairs per PR.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRepairsPerPR *int32 `json:"maxRepairsPerPR,omitempty"`

	// MaxRepairsPerHead bounds automated repairs per PR head SHA.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRepairsPerHead *int32 `json:"maxRepairsPerHead,omitempty"`

	// MaxValidationRetries bounds validation retries for one repair job.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxValidationRetries *int32 `json:"maxValidationRetries,omitempty"`
}

// RepositoryMonitorAutomergeSpec configures automerge behavior.
type RepositoryMonitorAutomergeSpec struct {
	// Enabled enables automerge jobs.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// RequireGlobalMergeGate requires the controller-wide merge gate.
	// +kubebuilder:default=true
	// +optional
	RequireGlobalMergeGate *bool `json:"requireGlobalMergeGate,omitempty"`

	// AllowedMergeMethods lists merge methods allowed by policy.
	// +kubebuilder:validation:items:Enum=merge;squash;rebase
	// +listType=set
	// +optional
	AllowedMergeMethods []string `json:"allowedMergeMethods,omitempty"`
}

// RepositoryMonitorPolicySpec configures monitor safety policy.
type RepositoryMonitorPolicySpec struct {
	// ProtectedLabels block automated review, repair, or merge according to policy.
	// +listType=set
	// +optional
	ProtectedLabels []string `json:"protectedLabels,omitempty"`

	// PauseLabels block further automation while present.
	// +listType=set
	// +optional
	PauseLabels []string `json:"pauseLabels,omitempty"`

	// AllowedRepositoryPermissions lists GitHub permissions allowed to issue write commands.
	// +kubebuilder:validation:items:Enum=admin;maintain;write
	// +listType=set
	// +optional
	AllowedRepositoryPermissions []string `json:"allowedRepositoryPermissions,omitempty"`
}

// RepositoryMonitorValidationSpec configures isolated validation.
type RepositoryMonitorValidationSpec struct {
	// Image is the digest-pinned repository-specific container image used for validation.
	// The reviewer chooses the validation command from the checked-out code.
	// The image must contain /bin/sh and every tool the repository expects the
	// reviewer to run.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[^\s@]+@sha256:[a-f0-9]{64}$`
	// +optional
	Image string `json:"image,omitempty"`

	// Mode is retained only so upgraded clusters preserve the legacy field.
	// Deprecated: validation scope is selected by the reviewer from repository evidence.
	// +kubebuilder:validation:Enum=off;changed;full
	// +optional
	Mode string `json:"mode,omitempty"`

	// Commands is retained only so upgraded clusters can detect and reject legacy
	// validation policies instead of silently treating them as disabled.
	// Deprecated: configure a digest-pinned image and let the reviewer select one command.
	// +optional
	Commands []string `json:"commands,omitempty"`
}

// RepositoryMonitorStatus defines the observed state of RepositoryMonitor.
type RepositoryMonitorStatus struct {
	// Phase describes the high-level monitor lifecycle state.
	// +optional
	Phase string `json:"phase,omitempty"`

	// LastRunID is the most recent monitor run identifier stored in SQLite.
	// +optional
	LastRunID string `json:"lastRunID,omitempty"`

	// LastRunTime is the completion time of the most recent run, regardless of success.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`

	// LastSuccessfulRunTime is the completion time of the most recent successful run.
	// +optional
	LastSuccessfulRunTime *metav1.Time `json:"lastSuccessfulRunTime,omitempty"`

	// ObservedGeneration is the latest spec generation reflected in status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// OpenPullRequests is the current count of open pull requests seen by the monitor.
	// +optional
	OpenPullRequests int32 `json:"openPullRequests,omitempty"`

	// PendingReviews is the count of items waiting for review.
	// +optional
	PendingReviews int32 `json:"pendingReviews,omitempty"`

	// ActiveRepairs is the count of repair jobs currently active.
	// +optional
	ActiveRepairs int32 `json:"activeRepairs,omitempty"`

	// BlockedItems is the count of items blocked by policy, failures, or human action.
	// +optional
	BlockedItems int32 `json:"blockedItems,omitempty"`

	// MergeReadyItems is the count of items ready for merge.
	// +optional
	MergeReadyItems int32 `json:"mergeReadyItems,omitempty"`

	// OpenIssues is the current count of open issues seen by the monitor.
	// +optional
	OpenIssues int32 `json:"openIssues,omitempty"`

	// PendingIssueActions is the count of issues waiting for a queued workflow action.
	// +optional
	PendingIssueActions int32 `json:"pendingIssueActions,omitempty"`

	// BlockedIssues is the count of issues blocked by guard labels or workflow policy.
	// +optional
	BlockedIssues int32 `json:"blockedIssues,omitempty"`

	// Conditions represent the current state of the repository monitor.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repository`
// +kubebuilder:printcolumn:name="Branch",type=string,JSONPath=`.spec.branch`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.pendingReviews`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RepositoryMonitor is the Schema for repository maintainer automation.
type RepositoryMonitor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RepositoryMonitorSpec   `json:"spec,omitempty"`
	Status RepositoryMonitorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RepositoryMonitorList contains a list of RepositoryMonitor resources.
type RepositoryMonitorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RepositoryMonitor `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RepositoryMonitor{}, &RepositoryMonitorList{})
}
