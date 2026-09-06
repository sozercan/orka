/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentSpec defines the desired state of Agent
// +kubebuilder:validation:XValidation:rule="!has(self.execution) || !has(self.execution.workspace) || !has(self.execution.workspace.classRef)",message="execution.workspace.classRef is only supported on Task specs"
// +kubebuilder:validation:XValidation:rule="!(has(self.runtime) && has(self.runtime.type) && self.runtime.type == 'opencode' && has(self.runtime.contractVersion) && self.runtime.contractVersion == 'orka.harness.v2' && has(self.systemPrompt) && ((has(self.systemPrompt.inline) && self.systemPrompt.inline.size() > 0) || has(self.systemPrompt.configMapRef)))",message="opencode orka.harness.v2 runtime does not support spec.systemPrompt"
type AgentSpec struct {
	// ProviderRef references a Provider CRD for LLM configuration
	// If set, model.provider is optional (inherited from Provider)
	// +optional
	ProviderRef *ProviderReference `json:"providerRef,omitempty"`

	// Model defines the LLM model configuration
	// Provider field is optional if providerRef is set
	// +optional
	Model *ModelConfig `json:"model,omitempty"`

	// SystemPrompt defines the system prompt configuration
	// +optional
	SystemPrompt *PromptSource `json:"systemPrompt,omitempty"`

	// Tools lists the default tools available to this agent
	// +optional
	Tools []ToolReference `json:"tools,omitempty"`

	// Skills lists the default skills for this agent
	// +optional
	Skills []SkillReference `json:"skills,omitempty"`

	// Resources defines the resource limits for tasks using this agent
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Execution defines default worker pod runtime and placement settings.
	// +optional
	Execution *ExecutionSpec `json:"execution,omitempty"`

	// SecretRef references a Secret containing LLM API keys
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// Session defines session configuration defaults
	// +optional
	Session *SessionConfig `json:"session,omitempty"`

	// Coordination enables agent-to-agent delegation
	// +optional
	Coordination *CoordinationConfig `json:"coordination,omitempty"`

	// Runtime configures this Agent for external CLI runtimes (type: agent tasks).
	// When set, this Agent is for type: agent tasks only (mutually exclusive with providerRef).
	// +optional
	Runtime *AgentCLIRuntime `json:"runtime,omitempty"`

	// TTLAfterLastTask defines how long the agent persists after its last task completes.
	// When set and no tasks are active, the agent is deleted after this duration.
	// Zero means the agent is never auto-deleted (permanent). Default is no TTL (permanent).
	// +optional
	TTLAfterLastTask *metav1.Duration `json:"ttlAfterLastTask,omitempty"`
}

// AgentCLIRuntime defines agent CLI runtime configuration for an Agent.
// +kubebuilder:validation:XValidation:rule="has(self.type) != has(self.runtimeRef)",message="exactly one of type or runtimeRef is required"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.contractVersion) || (has(self.contractVersion) && self.contractVersion == oldSelf.contractVersion)",message="runtime.contractVersion is immutable once set"
// +kubebuilder:validation:XValidation:rule="!has(self.contractVersion) || has(self.type)",message="runtime.contractVersion applies only to built-in runtime types; runtimeRef derives the protocol from the referenced AgentRuntime"
type AgentCLIRuntime struct {
	// Type specifies which built-in CLI runtime to use. Use runtimeRef for admin-registered custom runtimes.
	// +optional
	Type AgentRuntimeType `json:"type,omitempty"`

	// ContractVersion is the immutable harness protocol selector for built-in
	// runtime types. There is no default: a missing selector is never
	// interpreted as either protocol, and fail-closed admission requires an
	// explicit value on new built-in Agents. runtime.type alone (including
	// opencode, which exists in both protocols) is never protocol evidence.
	// +optional
	ContractVersion *AgentRuntimeContractVersion `json:"contractVersion,omitempty"`

	// RuntimeRef selects an admin-governed AgentRuntime for custom/BYO harness runtimes.
	// +optional
	RuntimeRef *AgentRuntimeReference `json:"runtimeRef,omitempty"`

	// DefaultMaxTurns is the default maximum agent loop iterations for tasks using this Agent
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=50
	// +optional
	DefaultMaxTurns *int32 `json:"defaultMaxTurns,omitempty"`

	// DefaultAllowedTools lists the default tools allowed for tasks using this Agent
	// +optional
	DefaultAllowedTools []string `json:"defaultAllowedTools,omitempty"`

	// DefaultAllowBash controls whether bash is allowed by default for tasks using this Agent.
	// Defaults to true if not specified.
	// +optional
	DefaultAllowBash *bool `json:"defaultAllowBash,omitempty"`

	// DefaultReasoningEffort configures the CLI runtime reasoning effort for tasks using this Agent.
	// Runtime adapters reject values they do not support (for example, Codex does not support max).
	// +kubebuilder:validation:Enum=low;medium;high;xhigh;max
	// +optional
	DefaultReasoningEffort string `json:"defaultReasoningEffort,omitempty"`
}

// MarshalJSON preserves the distinction between an omitted tool allowlist and
// an explicitly empty deny-all allowlist. The standard omitempty handling for
// slices would otherwise serialize both states as omission.
func (in AgentCLIRuntime) MarshalJSON() ([]byte, error) {
	type agentCLIRuntimeJSON struct {
		Type                   AgentRuntimeType             `json:"type,omitempty"`
		ContractVersion        *AgentRuntimeContractVersion `json:"contractVersion,omitempty"`
		RuntimeRef             *AgentRuntimeReference       `json:"runtimeRef,omitempty"`
		DefaultMaxTurns        *int32                       `json:"defaultMaxTurns,omitempty"`
		DefaultAllowedTools    *[]string                    `json:"defaultAllowedTools,omitempty"`
		DefaultAllowBash       *bool                        `json:"defaultAllowBash,omitempty"`
		DefaultReasoningEffort string                       `json:"defaultReasoningEffort,omitempty"`
	}
	var defaultAllowedTools *[]string
	if in.DefaultAllowedTools != nil {
		tools := append([]string{}, in.DefaultAllowedTools...)
		defaultAllowedTools = &tools
	}
	return json.Marshal(agentCLIRuntimeJSON{
		Type:                   in.Type,
		ContractVersion:        in.ContractVersion,
		RuntimeRef:             in.RuntimeRef,
		DefaultMaxTurns:        in.DefaultMaxTurns,
		DefaultAllowedTools:    defaultAllowedTools,
		DefaultAllowBash:       in.DefaultAllowBash,
		DefaultReasoningEffort: in.DefaultReasoningEffort,
	})
}

// BuiltInContractVersion returns the Agent's explicit built-in harness
// protocol selector, or empty when unclassified. Callers must treat empty as
// neither protocol and fail closed; runtime.type alone is never protocol
// evidence.
func (in *Agent) BuiltInContractVersion() AgentRuntimeContractVersion {
	if in == nil || in.Spec.Runtime == nil || in.Spec.Runtime.ContractVersion == nil {
		return ""
	}
	return *in.Spec.Runtime.ContractVersion
}

// ModelFallback defines a fallback provider configuration
type ModelFallback struct {
	// ProviderRef is the name of a Provider CRD to fall back to
	// +kubebuilder:validation:Required
	ProviderRef string `json:"providerRef"`

	// Model to use with this provider (optional, uses provider's defaultModel if empty)
	// +optional
	Model string `json:"model,omitempty"`
}

// ModelConfig defines LLM model configuration
type ModelConfig struct {
	// Provider is the LLM provider (anthropic, openai)
	// Optional if providerRef is set on the Agent
	// +kubebuilder:validation:Enum=anthropic;openai
	// +optional
	Provider string `json:"provider,omitempty"`

	// Name is the model identifier
	// Optional if providerRef is set and Provider has defaultModel
	// +optional
	Name string `json:"name,omitempty"`

	// Temperature controls randomness in generation
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	// +optional
	Temperature *float64 `json:"temperature,omitempty"`

	// ContextWindow is the reviewed model context capacity in tokens. Built-in
	// runtimes that manage their own compaction require this value explicitly.
	// +kubebuilder:validation:Minimum=1
	// +optional
	ContextWindow *int32 `json:"contextWindow,omitempty"`

	// MaxTokens limits the response length. OpenCode validates positive reviewed
	// limits at its runtime-specific admission boundary; existing Agent objects
	// may retain the legacy zero value.
	// +optional
	MaxTokens *int32 `json:"maxTokens,omitempty"`

	// Fallbacks defines alternative providers to try when the primary fails.
	// Each fallback specifies a Provider CRD and optional model override.
	// +optional
	Fallbacks []ModelFallback `json:"fallbacks,omitempty"`
}

// PromptSource defines where to get a prompt from
// +kubebuilder:validation:XValidation:rule="!(has(self.inline) && self.inline.size() > 0 && has(self.configMapRef))",message="system prompt must use only one of inline or configMapRef"
type PromptSource struct {
	// Inline is the inline prompt text
	// +optional
	Inline string `json:"inline,omitempty"`

	// ConfigMapRef references a ConfigMap containing the prompt
	// +optional
	ConfigMapRef *ConfigMapKeySelector `json:"configMapRef,omitempty"`
}

// ConfigMapKeySelector selects a key from a ConfigMap
type ConfigMapKeySelector struct {
	// Name is the name of the ConfigMap
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key is the key within the ConfigMap
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// ToolReference references a tool for an agent
type ToolReference struct {
	// Name is the tool name (built-in or Tool CRD name)
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Enabled indicates if the tool is enabled (default: true)
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// SessionConfig defines session behavior defaults
type SessionConfig struct {
	// MaxMessages is the maximum messages to load from session
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=50
	// +optional
	MaxMessages int32 `json:"maxMessages,omitempty"`
}

// CoordinationConfig enables agent-to-agent delegation
type CoordinationConfig struct {
	// Enabled indicates if coordination is enabled
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// AllowedAgents lists agents this agent can delegate to
	// +optional
	AllowedAgents []AllowedAgent `json:"allowedAgents,omitempty"`

	// MaxConcurrentChildren limits concurrent child tasks
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	// +optional
	MaxConcurrentChildren int32 `json:"maxConcurrentChildren,omitempty"`

	// MaxDepth limits delegation depth to prevent infinite loops
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=3
	// +optional
	MaxDepth int32 `json:"maxDepth,omitempty"`

	// Autonomous enables autonomous loop mode for coordinator agents using this config.
	// When enabled, the controller re-creates Jobs in a loop instead of marking the task as Succeeded.
	// +optional
	Autonomous bool `json:"autonomous,omitempty"`

	// MaxIterations limits the number of autonomous loop iterations (0 = unlimited).
	// Only used when Autonomous is true.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxIterations int32 `json:"maxIterations,omitempty"`

	// ApprovalRequiredTools lists custom tool names that require a human approval before execution.
	// This field is only honored when coordination is enabled and autonomous mode is true.
	// Built-in tools such as request_approval, delegate_task, and web_search are rejected.
	// +optional
	ApprovalRequiredTools []string `json:"approvalRequiredTools,omitempty"`
}

// AllowedAgent defines an agent that can be delegated to
type AllowedAgent struct {
	// Name is the name of the agent
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace is the namespace of the agent (defaults to same namespace)
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// AgentStatus defines the observed state of Agent
type AgentStatus struct {
	// Ready indicates whether the agent configuration is valid and usable
	// +optional
	Ready bool `json:"ready,omitempty"`

	// ActiveTasks is the number of active tasks using this agent
	ActiveTasks int32 `json:"activeTasks"`

	// LastUsed is the timestamp of when this agent was last used
	// +optional
	LastUsed *metav1.Time `json:"lastUsed,omitempty"`

	// Conditions represent the current state of the Agent
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.model.provider`
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.spec.model.name`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeTasks`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Agent is the Schema for the agents API
type Agent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentSpec   `json:"spec,omitempty"`
	Status AgentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AgentList contains a list of Agent
type AgentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Agent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Agent{}, &AgentList{})
}
