/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyConfigMapKeyRef references a key in a same-namespace ConfigMap that contains scanner policy text.
type PolicyConfigMapKeyRef struct {
	// Name is the ConfigMap name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is the ConfigMap data key. Defaults to "policy" when omitted.
	// +optional
	Key string `json:"key,omitempty"`
}

// RepositoryScanSpec defines the desired state of RepositoryScan.
// +kubebuilder:validation:XValidation:rule="self.repoURL == oldSelf.repoURL",message="repoURL is immutable; create a new RepositoryScan for a different repository"
type RepositoryScanSpec struct {
	// Provider is the source control provider. GitHub is the only supported v1 provider.
	// +kubebuilder:validation:Enum=github
	// +kubebuilder:default=github
	// +optional
	Provider string `json:"provider,omitempty"`

	// RepoURL is the repository URL to scan.
	// +kubebuilder:validation:Required
	RepoURL string `json:"repoURL"`

	// Owner is the repository owner or organization.
	// +optional
	Owner string `json:"owner,omitempty"`

	// Repository is the repository name.
	// +optional
	Repository string `json:"repository,omitempty"`

	// Branch is the base branch to scan. Defaults to the literal main branch when omitted.
	// +optional
	Branch string `json:"branch,omitempty"`

	// Ref is a specific git ref, tag, or commit SHA to checkout for scan tasks.
	// +optional
	Ref string `json:"ref,omitempty"`

	// SubPath scopes scanning to a subdirectory in a monorepo.
	// +optional
	SubPath string `json:"subPath,omitempty"`

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
	// Patch workflows require this explicit reference.
	// +optional
	PublicationReadCredentialRef *corev1.LocalObjectReference `json:"publicationReadCredentialRef,omitempty"`

	// PublicationCredentialRef references the target-repository write credential
	// used only for exact compare-and-swap publication. Patch workflows require
	// this explicit reference.
	// +optional
	PublicationCredentialRef *corev1.LocalObjectReference `json:"publicationCredentialRef,omitempty"`

	// ForgeCredentialRef references the GitHub API credential used only for
	// controller-owned pull request reconciliation. Patch workflows require this
	// explicit reference.
	// +optional
	ForgeCredentialRef *corev1.LocalObjectReference `json:"forgeCredentialRef,omitempty"`

	// ForkRepo is the writable fork repository URL used for patch proposals.
	// +optional
	ForkRepo string `json:"forkRepo,omitempty"`

	// PRBaseBranch is the pull request base branch for remediation.
	// +optional
	PRBaseBranch string `json:"prBaseBranch,omitempty"`

	// Schedule is the cron expression for incremental scans.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// TimeZone is the IANA time zone for the schedule.
	// +optional
	TimeZone *string `json:"timeZone,omitempty"`

	// HistoryDays controls how far back the initial scan should inspect repository history.
	// +optional
	HistoryDays *int32 `json:"historyDays,omitempty"`

	// ValidationMode controls how aggressively findings are validated.
	// +kubebuilder:validation:Enum=off;light;full
	// +kubebuilder:default=light
	// +optional
	ValidationMode string `json:"validationMode,omitempty"`

	// ValidationMaxFindingsPerRun bounds automatic validation tasks for light mode.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ValidationMaxFindingsPerRun *int32 `json:"validationMaxFindingsPerRun,omitempty"`

	// ValidationMinSeverity is the minimum severity eligible for automatic validation.
	// +kubebuilder:validation:Enum=critical;high;medium;low
	// +optional
	ValidationMinSeverity string `json:"validationMinSeverity,omitempty"`

	// ValidationMinConfidence is the minimum confidence eligible for automatic validation.
	// +kubebuilder:validation:Enum=high;medium;low
	// +optional
	ValidationMinConfidence string `json:"validationMinConfidence,omitempty"`

	// CustomScanInstructionsRef references additive scanner instructions in a same-namespace ConfigMap.
	// +optional
	CustomScanInstructionsRef *PolicyConfigMapKeyRef `json:"customScanInstructionsRef,omitempty"`

	// FalsePositivePolicyRef references additive false-positive policy in a same-namespace ConfigMap.
	// +optional
	FalsePositivePolicyRef *PolicyConfigMapKeyRef `json:"falsePositivePolicyRef,omitempty"`

	// AnalysisAgentRef is the agent used for scan runs.
	AnalysisAgentRef AgentReference `json:"analysisAgentRef"`

	// PatchAgentRef is the agent used for patch proposal runs.
	// +optional
	PatchAgentRef *AgentReference `json:"patchAgentRef,omitempty"`

	// MaxFindingsPerRun bounds scan output volume.
	// +optional
	MaxFindingsPerRun *int32 `json:"maxFindingsPerRun,omitempty"`

	// Suspend pauses scheduled incremental scans.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`
}

// FindingCountsStatus summarizes open findings by severity.
type FindingCountsStatus struct {
	Total    int32 `json:"total,omitempty"`
	Critical int32 `json:"critical,omitempty"`
	High     int32 `json:"high,omitempty"`
	Medium   int32 `json:"medium,omitempty"`
	Low      int32 `json:"low,omitempty"`
}

// RepositoryScanStatus defines the observed state of RepositoryScan.
type RepositoryScanStatus struct {
	// Phase describes the high-level repository scan lifecycle state.
	// +optional
	Phase string `json:"phase,omitempty"`

	// LastScanID is the most recent scan run identifier stored in SQLite.
	// +optional
	LastScanID string `json:"lastScanID,omitempty"`

	// LastScanTaskName is the most recent scan task name.
	// +optional
	LastScanTaskName string `json:"lastScanTaskName,omitempty"`

	// LastScanAt is the completion time of the most recent scan run, regardless of success or failure.
	// +optional
	LastScanAt *metav1.Time `json:"lastScanAt,omitempty"`

	// LastSuccessfulScanAt is the completion time of the most recent successful scan.
	// +optional
	LastSuccessfulScanAt *metav1.Time `json:"lastSuccessfulScanAt,omitempty"`

	// LastObservedHeadSHA is the latest repository head SHA seen by a completed scan.
	// +optional
	LastObservedHeadSHA string `json:"lastObservedHeadSHA,omitempty"`

	// LastProcessedCommit is the latest commit fully processed by a completed scan.
	// +optional
	LastProcessedCommit string `json:"lastProcessedCommit,omitempty"`

	// ThreatModelVersion is the latest persisted threat model version.
	// +optional
	ThreatModelVersion int64 `json:"threatModelVersion,omitempty"`

	// FindingCounts summarizes open findings.
	// +optional
	FindingCounts FindingCountsStatus `json:"findingCounts,omitempty"`

	// Conditions represent the current state of the repository scan.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repository`
// +kubebuilder:printcolumn:name="Branch",type=string,JSONPath=`.spec.branch`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Findings",type=integer,JSONPath=`.status.findingCounts.total`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RepositoryScan is the Schema for the repository security scanning API.
type RepositoryScan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RepositoryScanSpec   `json:"spec,omitempty"`
	Status RepositoryScanStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RepositoryScanList contains a list of RepositoryScan resources.
type RepositoryScanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RepositoryScan `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RepositoryScan{}, &RepositoryScanList{})
}
