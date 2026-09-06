/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SkillSpec defines the desired state of Skill
type SkillSpec struct {
	// DisplayName is the human-readable name of the skill
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// Description is a short description of the skill shown to users and LLMs
	// +kubebuilder:validation:Required
	Description string `json:"description"`

	// Version is the semantic version of the skill
	// +optional
	Version string `json:"version,omitempty"`

	// Author is the author or maintainer of the skill
	// +optional
	Author string `json:"author,omitempty"`

	// Tags are labels for categorization and discovery
	// +optional
	Tags []string `json:"tags,omitempty"`

	// Content defines the skill content (Agent Skills standard)
	// +kubebuilder:validation:Required
	Content SkillContent `json:"content"`

	// Source tracks where this skill was imported from (for updates)
	// +optional
	Source *SkillSource `json:"source,omitempty"`
}

// SkillContent holds the skill content following the Agent Skills standard
type SkillContent struct {
	// Inline is the SKILL.md content injected into the system prompt
	// +kubebuilder:validation:Required
	Inline string `json:"inline"`

	// Files is a map of additional files (templates, examples) mounted alongside the skill
	// Keys are relative paths (e.g. "templates/review-checklist.md")
	// +optional
	Files map[string]string `json:"files,omitempty"`
}

// SkillSource tracks the origin of an imported skill
type SkillSource struct {
	// GitHub is the GitHub repo path (e.g. "/anthropics/skills")
	// +optional
	GitHub string `json:"github,omitempty"`

	// SkillName is the skill name within the source repo
	// +optional
	SkillName string `json:"skillName,omitempty"`
}

// SkillStatus defines the observed state of Skill
type SkillStatus struct {
	// Phase indicates the current state of the skill: Ready or Error
	// +optional
	Phase string `json:"phase,omitempty"`

	// ContentHash is the SHA-256 hash of the skill content for change detection
	// +optional
	ContentHash string `json:"contentHash,omitempty"`

	// Conditions represent the current state of the Skill
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Display Name",type=string,JSONPath=`.spec.displayName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Skill is the Schema for the skills API
type Skill struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SkillSpec   `json:"spec,omitempty"`
	Status SkillStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SkillList contains a list of Skill
type SkillList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Skill `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Skill{}, &SkillList{})
}
