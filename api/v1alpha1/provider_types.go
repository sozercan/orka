/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderType defines the type of LLM provider
// +kubebuilder:validation:Enum=anthropic;openai;azure-openai
type ProviderType string

const (
	// ProviderTypeAnthropic is the Anthropic Claude provider
	ProviderTypeAnthropic ProviderType = "anthropic"
	// ProviderTypeOpenAI is the OpenAI provider
	ProviderTypeOpenAI ProviderType = "openai"
	// ProviderTypeAzureOpenAI is the Azure OpenAI provider
	ProviderTypeAzureOpenAI ProviderType = "azure-openai"
)

// ProviderSpec defines the desired state of Provider
type ProviderSpec struct {
	// Type is the LLM provider type (anthropic, openai, azure-openai)
	// +kubebuilder:validation:Required
	Type ProviderType `json:"type"`

	// SecretRef references the Secret containing API credentials
	// +kubebuilder:validation:Required
	SecretRef ProviderSecretRef `json:"secretRef"`

	// BaseURL is an optional custom API endpoint (for proxies or self-hosted)
	// +optional
	BaseURL string `json:"baseURL,omitempty"`

	// DefaultModel is the default model to use if not specified in Task/Agent
	// +optional
	DefaultModel string `json:"defaultModel,omitempty"`

	// Azure contains Azure-specific configuration
	// +optional
	Azure *AzureConfig `json:"azure,omitempty"`
}

// ProviderSecretRef references a Secret containing API credentials
type ProviderSecretRef struct {
	// Name is the name of the Secret
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key is the key within the Secret containing the API key
	// +kubebuilder:default="api-key"
	// +optional
	Key string `json:"key,omitempty"`
}

// AzureConfig contains Azure OpenAI specific configuration
type AzureConfig struct {
	// DeploymentName is the Azure OpenAI deployment name
	// +kubebuilder:validation:Required
	DeploymentName string `json:"deploymentName"`

	// APIVersion is the Azure OpenAI API version
	// +kubebuilder:default="2024-02-15-preview"
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`
}

// ProviderStatus defines the observed state of Provider
type ProviderStatus struct {
	// Ready indicates whether the provider is configured and validated
	// +optional
	Ready bool `json:"ready,omitempty"`

	// LastValidated is the timestamp of the last successful validation
	// +optional
	LastValidated *metav1.Time `json:"lastValidated,omitempty"`

	// Message provides additional status information
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the current state of the Provider
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Default Model",type=string,JSONPath=`.spec.defaultModel`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Provider is the Schema for the providers API
type Provider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProviderSpec   `json:"spec,omitempty"`
	Status ProviderStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProviderList contains a list of Provider
type ProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Provider `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Provider{}, &ProviderList{})
}
