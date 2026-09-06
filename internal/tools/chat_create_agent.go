/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/tracing"
)

// ChatCreateAgentTool creates an Agent CRD from the chat context.
type ChatCreateAgentTool struct{}

func (t *ChatCreateAgentTool) Name() string { return createAgentToolName }

func (t *ChatCreateAgentTool) Description() string {
	return "Create an agent with model, tools, and optional runtime and coordination configuration. Enable coordination to allow this agent to delegate tasks to other agents. Provide initialPrompt to also create and start a Task immediately."
}

func (t *ChatCreateAgentTool) Parameters() json.RawMessage {
	return mustMarshalSchema(map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaPropertiesField: map[string]any{nameField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: agentNameDescription}, namespaceField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: namespaceDescription}, providerRefField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional Provider CRD reference name. Omit for runtime Agents; provide explicitly for AI/coordinator Agents when a specific provider is required."}, systemPromptField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "System prompt for the agent. Omit for OpenCode runtime Agents because OpenCode cannot enforce Agent system prompts; use initialPrompt or Task prompts instead."}, toolsField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeArray, itemsField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString}, jsonSchemaDescriptionField: "Tool names to attach"}, "resources": map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaDescriptionField: `Optional Kubernetes resource requests/limits for tasks using this agent, e.g. {requests:{cpu:"100m",memory:"512Mi"},limits:{cpu:"1",memory:"2Gi"}}`, jsonSchemaPropertiesField: map[string]any{
		"requests": map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, "additionalProperties": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString}},
		"limits":   map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, "additionalProperties": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString}},
	},
	}, modelField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaDescriptionField: "Model configuration; OpenCode requires a literal provider/model name plus reviewed contextWindow and maxTokens", jsonSchemaPropertiesField: map[string]any{
		"provider": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Model provider (e.g. anthropic, openai)"}, nameField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Model name; OpenCode requires a literal provider/model ID"}, "temperature": map[string]any{jsonSchemaTypeField: "number", "minimum": 0, "maximum": 2, jsonSchemaDescriptionField: "Sampling temperature. OpenCode only accepts the legacy default 0.7."},
		"contextWindow": map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, "minimum": 1, jsonSchemaDescriptionField: "Reviewed model context capacity; required for OpenCode"},
		"maxTokens":     map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, "minimum": 1, jsonSchemaDescriptionField: "Reviewed maximum output tokens; required for OpenCode"},
	},
	}, runtimeField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaDescriptionField: "CLI runtime configuration. OpenCode uses the built-in ACP RuntimePool profile and controller provider proxy.", jsonSchemaPropertiesField: map[string]any{jsonSchemaTypeField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Runtime type: copilot, claude, codex, or opencode"}, "defaultMaxTurns": map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, jsonSchemaDescriptionField: "Default max agent loop iterations"},
		"defaultAllowedTools": map[string]any{jsonSchemaTypeField: jsonSchemaTypeArray, itemsField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString}, jsonSchemaDescriptionField: "Default CLI tools allowed for tasks using this runtime agent. OpenCode defaults to Read, Write, Edit, Bash, Glob, and Grep when omitted."},
		"defaultAllowBash":    map[string]any{jsonSchemaTypeField: jsonSchemaTypeBoolean, jsonSchemaDescriptionField: "Whether bash is allowed by default for tasks using this runtime agent. OpenCode defaults to true when omitted."}, secretRefField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Deprecated legacy field. Built-in ACP runtimes are credential-free at the Agent boundary; OpenCode rejects it."},
	},
	}, "initialPrompt": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "When provided, automatically create and start a Task using this agent with this prompt. One tool call = agent + task. Leave empty to only create the agent config without running it."},
		"coordination": map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaDescriptionField: "Enable multi-agent coordination so this agent can delegate tasks to other agents via delegate_task/wait_for_tasks tools", jsonSchemaPropertiesField: map[string]any{
			enabledString:           map[string]any{jsonSchemaTypeField: jsonSchemaTypeBoolean, jsonSchemaDescriptionField: "Enable coordination (delegate_task/wait_for_tasks tools)"},
			"maxConcurrentChildren": map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, jsonSchemaDescriptionField: "Max concurrent child tasks (default 5)"},
			"maxDepth":              map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, jsonSchemaDescriptionField: "Max delegation depth (default 3)"},
			"allowedAgents":         map[string]any{jsonSchemaTypeField: jsonSchemaTypeArray, jsonSchemaDescriptionField: "List of agent names this agent can delegate to. If empty, can delegate to any agent.", itemsField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaPropertiesField: map[string]any{nameField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: agentNameDescription}, namespaceField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Agent namespace (defaults to same namespace)"}}, jsonSchemaRequiredField: []string{nameField}}},
		},
		},
	}, jsonSchemaRequiredField: []string{nameField},
	})
}

func (t *ChatCreateAgentTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	tc := GetToolContext(ctx)
	if tc == nil {
		return ChatToolErrorResult(internalErrorType, "missing tool context", "")
	}

	var a map[string]any
	if err := json.Unmarshal(args, &a); err != nil {
		return ChatToolErrorResult("invalid_arguments", fmt.Sprintf("failed to parse arguments: %v", err), "Ensure arguments are valid JSON")
	}

	name := chatGetStringArg(a, nameField)
	if name == "" {
		return ChatToolErrorResult("invalid_arguments", "name is required", "Provide a name for the agent")
	}

	namespace := chatGetStringArgDefault(a, namespaceField, tc.Namespace)
	if r, ok := checkChatNamespaceScope(tc, namespace); !ok {
		return r, nil
	}

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labels.LabelCreatedBy: "chat",
			},
		},
		Spec: corev1alpha1.AgentSpec{
			TTLAfterLastTask: &metav1.Duration{Duration: time.Hour},
		},
	}

	// Model configuration
	if modelObj, ok := a[modelField]; ok {
		switch m := modelObj.(type) {
		case map[string]any:
			name := chatGetStringArg(m, nameField)
			provider := chatGetStringArg(m, "provider")
			agent.Spec.Model = &corev1alpha1.ModelConfig{Name: name, Provider: provider}
			if temp, ok := m["temperature"]; ok {
				if tempF, ok := temp.(float64); ok {
					agent.Spec.Model.Temperature = &tempF
				}
			}
			if raw, supplied := m["contextWindow"]; supplied {
				contextWindow, err := parsePositiveAgentModelInt32("model.contextWindow", raw)
				if err != nil {
					return ChatToolErrorResult(
						"invalid_arguments",
						err.Error(),
						"Use a positive whole-number model.contextWindow within the 32-bit integer range.",
					)
				}
				agent.Spec.Model.ContextWindow = &contextWindow
			}
			if raw, supplied := m["maxTokens"]; supplied {
				maxTokens, err := parsePositiveAgentModelInt32("model.maxTokens", raw)
				if err != nil {
					return ChatToolErrorResult(
						"invalid_arguments",
						err.Error(),
						"Use a positive whole-number model.maxTokens within the 32-bit integer range.",
					)
				}
				agent.Spec.Model.MaxTokens = &maxTokens
			}
		case string:
			provider, modelName := splitModelString(m)
			if provider != "" {
				agent.Spec.Model = &corev1alpha1.ModelConfig{Provider: provider, Name: modelName}
			} else {
				agent.Spec.Model = &corev1alpha1.ModelConfig{Name: modelName}
			}
		}
	}

	// Set providerRef only when explicitly provided. Runtime Agents clear it below.
	if providerRefName := chatGetStringArg(a, providerRefField); providerRefName != "" {
		agent.Spec.ProviderRef = &corev1alpha1.ProviderReference{Name: providerRefName}
	}

	if agent.Spec.ProviderRef != nil && agent.Spec.Model != nil && agent.Spec.Model.Provider != "" {
		agent.Spec.Model.Provider = ""
	}

	if systemPrompt := chatGetStringArg(a, systemPromptField); systemPrompt != "" {
		agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{
			Inline: systemPrompt,
		}
	}

	if toolNames := chatGetStringSliceArg(a, toolsField); len(toolNames) > 0 {
		for _, tn := range toolNames {
			agent.Spec.Tools = append(agent.Spec.Tools, corev1alpha1.ToolReference{Name: tn})
		}
	}

	if resources, errResult, ok := parseResourceRequirementsArg(a); !ok {
		return errResult, nil
	} else {
		agent.Spec.Resources = resources
	}

	if errResult, ok := parseRuntimeConfig(a, agent, tc.ExecutionMode); !ok {
		return errResult, nil
	}
	if err := executionmode.DefaultBuiltInAgentContract(agent, tc.ExecutionMode); err != nil {
		return ChatToolErrorResult(
			"invalid_arguments",
			err.Error(),
			"Use the built-in runtime contract selected by this Orka installation.",
		)
	}
	parseCoordinationConfig(a, agent)

	if chatGetStringArg(a, "initialPrompt") != "" && tc.AuthorizeAgentInitialTask != nil {
		if err := tc.AuthorizeAgentInitialTask(ctx, agent); err != nil {
			return ChatToolErrorResult(err.Type, err.Message, err.Suggestion)
		}
	}
	if result, ok := authorizeAgentCreate(ctx, tc, agent); !ok {
		return result, nil
	}
	if err := tc.Client.Create(ctx, agent); err != nil {
		return classifyChatK8sErr(err)
	}

	// Handle initialPrompt: create a task for the agent
	if initialPrompt := chatGetStringArg(a, "initialPrompt"); initialPrompt != "" {
		return t.handleInitialPrompt(ctx, tc, agent, namespace, initialPrompt)
	}

	return ChatToolSuccess(map[string]any{nameField: agent.Name, namespaceField: agent.Namespace, messageField: "Agent created"})
}

func (t *ChatCreateAgentTool) handleInitialPrompt(ctx context.Context, tc *ToolContext, agent *corev1alpha1.Agent, namespace, initialPrompt string) (string, error) {
	if limitErr := tc.CheckTaskLimit(); limitErr != nil {
		return ChatToolSuccess(map[string]any{nameField: agent.Name, namespaceField: agent.Namespace, messageField: "Agent created, but task creation skipped: task limit reached"})
	}

	taskType := corev1alpha1.TaskTypeAI
	if agent.Spec.Runtime != nil {
		taskType = corev1alpha1.TaskTypeAgent
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tc.GenerateTaskName(),
			Namespace: namespace,
			Labels:    tc.TaskLabels(),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   taskType,
			Prompt: initialPrompt,
			AgentRef: &corev1alpha1.AgentReference{
				Name: agent.Name,
			},
		},
	}

	if agent.Spec.ProviderRef != nil && taskType == corev1alpha1.TaskTypeAI {
		if task.Spec.AI == nil {
			task.Spec.AI = &corev1alpha1.AISpec{}
		}
		task.Spec.AI.ProviderRef = agent.Spec.ProviderRef
	}

	if result, ok := authorizeTaskCreate(ctx, tc, task); !ok {
		if err := tc.Client.Delete(ctx, agent); err != nil && !apierrors.IsNotFound(err) {
			return ChatToolErrorResult("cleanup_failed", fmt.Sprintf("initial task authorization failed and agent cleanup failed: %v", err), "Delete the agent manually before retrying.")
		}
		return result, nil
	}
	tracing.StampTaskTraceContext(ctx, task)
	if err := tc.Client.Create(ctx, task); err != nil {
		return ChatToolSuccess(map[string]any{
			"agentName":      agent.Name,
			"agentNamespace": agent.Namespace, messageField: fmt.Sprintf("Agent created, but task creation failed: %v", err),
		})
	}

	tc.IncrementTasks()
	return ChatToolSuccess(map[string]any{
		"agentName":      agent.Name,
		"agentNamespace": agent.Namespace,
		"taskName":       task.Name,
		"taskNamespace":  task.Namespace, messageField: "Agent created and task started",
	})
}

func parseResourceRequirementsArg(args map[string]any) (corev1.ResourceRequirements, string, bool) {
	var requirements corev1.ResourceRequirements
	value, ok := args["resources"]
	if !ok || value == nil {
		return requirements, "", true
	}

	resourcesMap, ok := value.(map[string]any)
	if !ok {
		result, _ := ChatToolErrorResult("invalid_arguments", "resources must be an object", "Use resources.requests and/or resources.limits maps with Kubernetes quantity strings")
		return requirements, result, false
	}

	requests, errResult, ok := parseResourceListArg(resourcesMap, "requests")
	if !ok {
		return requirements, errResult, false
	}
	limits, errResult, ok := parseResourceListArg(resourcesMap, "limits")
	if !ok {
		return requirements, errResult, false
	}

	if len(requests) > 0 {
		requirements.Requests = requests
	}
	if len(limits) > 0 {
		requirements.Limits = limits
	}
	return requirements, "", true
}

func parseResourceListArg(resourcesMap map[string]any, key string) (corev1.ResourceList, string, bool) {
	value, ok := resourcesMap[key]
	if !ok || value == nil {
		return nil, "", true
	}

	values, ok := value.(map[string]any)
	if !ok {
		result, _ := ChatToolErrorResult("invalid_arguments", fmt.Sprintf("resources.%s must be an object", key), "Use resource names mapped to Kubernetes quantity strings")
		return nil, result, false
	}

	resourceList := corev1.ResourceList{}
	for name, raw := range values {
		quantityText := chatGetStringArg(values, name)
		if quantityText == "" || raw == nil {
			continue
		}
		quantity, err := resource.ParseQuantity(quantityText)
		if err != nil {
			result, _ := ChatToolErrorResult("invalid_arguments", fmt.Sprintf("invalid resources.%s.%s: %v", key, name, err), "Use Kubernetes quantity strings such as 100m, 512Mi, 1, or 2Gi")
			return nil, result, false
		}
		resourceList[corev1.ResourceName(name)] = quantity
	}
	return resourceList, "", true
}

// parseRuntimeConfig extracts runtime configuration from chat args into the agent spec.
func parseRuntimeConfig(a map[string]any, agent *corev1alpha1.Agent, mode executionmode.Mode) (string, bool) {
	if coord, ok := a["coordination"].(map[string]any); ok {
		if enabled, ok := coord[enabledString].(bool); ok && enabled {
			return "", true
		}
	}
	rt, ok := a[runtimeField]
	if !ok {
		return "", true
	}
	rtMap, ok := rt.(map[string]any)
	if !ok {
		return "", true
	}
	runtimeType := strings.TrimSpace(chatGetStringArg(rtMap, jsonSchemaTypeField))
	if runtimeType == "" {
		result, _ := ChatToolErrorResult(
			"invalid_arguments",
			"runtime.type is required when runtime is provided",
			"Use one of the built-in ACP runtimes: copilot, claude, codex, or opencode.",
		)
		return result, false
	}
	resolvedRuntimeType := corev1alpha1.AgentRuntimeType(runtimeType)
	if !isBuiltInACPRuntime(resolvedRuntimeType) {
		result, _ := ChatToolErrorResult(
			"invalid_arguments",
			fmt.Sprintf("unsupported runtime type %q", runtimeType),
			"Use one of the built-in ACP runtimes: copilot, claude, codex, or opencode.",
		)
		return result, false
	}
	if err := validateBuiltInRuntimeModelLimits(resolvedRuntimeType, agent.Spec.Model); err != nil {
		result, _ := ChatToolErrorResult(
			"invalid_arguments",
			err.Error(),
			"Remove contextWindow/maxTokens for Codex, Claude, or Copilot; those reviewed limits are OpenCode-only.",
		)
		return result, false
	}
	if mode == executionmode.HarnessV2 && resolvedRuntimeType != corev1alpha1.AgentRuntimeOpencode &&
		(agent.Spec.Model == nil || strings.TrimSpace(agent.Spec.Model.Name) == "") {
		// Admission rejects a harness-v2 built-in runtime Agent without
		// spec.model.name; surface the requirement here instead of after a
		// denied create.
		result, _ := ChatToolErrorResult(
			"invalid_arguments",
			fmt.Sprintf("model.name is required for %s runtime Agents: the ACP runtime session has no default model", resolvedRuntimeType),
			"Set model.name to the model this runtime should use.",
		)
		return result, false
	}
	if resolvedRuntimeType == corev1alpha1.AgentRuntimeOpencode {
		if agent.Spec.SystemPrompt != nil && strings.TrimSpace(agent.Spec.SystemPrompt.Inline) != "" {
			result, _ := ChatToolErrorResult(
				"invalid_arguments",
				"opencode runtime does not support systemPrompt",
				"Omit systemPrompt for OpenCode Agents and use initialPrompt or Task prompts for instructions.",
			)
			return result, false
		}
		if secretRef := strings.TrimSpace(chatGetStringArg(rtMap, secretRefField)); secretRef != "" {
			result, _ := ChatToolErrorResult(
				"invalid_arguments",
				"opencode runtime does not accept runtime.secretRef; provider access is controller-proxied",
				"Remove runtime.secretRef for OpenCode Agents.",
			)
			return result, false
		}
		if result, ok := normalizeChatOpenCodeModel(agent); !ok {
			return result, false
		}
	}
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: resolvedRuntimeType}
	if agent.Spec.Model != nil {
		agent.Spec.Model.Provider = ""
	}
	if maxTurns, ok := rtMap["defaultMaxTurns"].(float64); ok && maxTurns > 0 {
		maxTurnsInt := int32(maxTurns)
		agent.Spec.Runtime.DefaultMaxTurns = &maxTurnsInt
	}
	_, allowedToolsSupplied := rtMap["defaultAllowedTools"]
	if allowedToolsSupplied {
		agent.Spec.Runtime.DefaultAllowedTools = chatGetStringSliceArg(rtMap, "defaultAllowedTools")
	}
	_, allowBashSupplied := rtMap["defaultAllowBash"]
	if allowBash, ok := rtMap["defaultAllowBash"].(bool); ok {
		agent.Spec.Runtime.DefaultAllowBash = &allowBash
	}
	applyOpencodeRuntimeDefaults(agent.Spec.Runtime, allowedToolsSupplied, allowBashSupplied)
	agent.Spec.SecretRef = nil
	agent.Spec.ProviderRef = nil
	return "", true
}

func normalizeChatOpenCodeModel(agent *corev1alpha1.Agent) (string, bool) {
	providerID, modelID, specErr := validateOpenCodeModelSpec(agent.Spec.Model)
	if specErr != nil {
		result, _ := ChatToolErrorResult("invalid_arguments", specErr.message, specErr.hint)
		return result, false
	}
	agent.Spec.Model.Name = providerID + "/" + modelID
	agent.Spec.Model.Provider = ""
	return "", true
}

// parseCoordinationConfig extracts coordination configuration from chat args into the agent spec.
func parseCoordinationConfig(a map[string]any, agent *corev1alpha1.Agent) {
	coord, ok := a["coordination"]
	if !ok {
		return
	}
	coordMap, ok := coord.(map[string]any)
	if !ok {
		return
	}
	coordCfg := &corev1alpha1.CoordinationConfig{}
	if enabled, ok := coordMap[enabledString].(bool); ok {
		coordCfg.Enabled = enabled
	}
	if maxCC, ok := coordMap["maxConcurrentChildren"].(float64); ok {
		coordCfg.MaxConcurrentChildren = int32(maxCC)
	}
	if maxD, ok := coordMap["maxDepth"].(float64); ok {
		coordCfg.MaxDepth = int32(maxD)
	}
	if allowed, ok := coordMap["allowedAgents"].([]any); ok {
		for _, item := range allowed {
			aMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			aa := corev1alpha1.AllowedAgent{
				Name:      chatGetStringArg(aMap, nameField),
				Namespace: chatGetStringArg(aMap, namespaceField),
			}
			if aa.Name != "" {
				coordCfg.AllowedAgents = append(coordCfg.AllowedAgents, aa)
			}
		}
	}
	agent.Spec.Coordination = coordCfg
	if coordCfg.Enabled {
		agent.Spec.Runtime = nil
		agent.Spec.SecretRef = nil
	}
}
