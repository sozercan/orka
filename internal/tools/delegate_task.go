/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
	"go.opentelemetry.io/otel/trace"
)

const (
	delegateRequestOperationField  = "operation"
	delegateRequestParentTaskField = "parentTask"
)

// DelegateTaskSubjectTokenResolver resolves the exact subject token authorized
// for one authenticated parent Task. Implementations must not fall back to
// process-global worker credentials for brokered requests.
type DelegateTaskSubjectTokenResolver func(context.Context, *corev1alpha1.Task, string) (string, error)

// BrokeredDelegateTaskTransactionExchangeConfig carries controller-parsed TTS
// settings and request-scoped dependencies for brokered child-token exchange.
type BrokeredDelegateTaskTransactionExchangeConfig struct {
	TTS                 contexttoken.TTSConfig
	Exchanger           contexttoken.Exchanger
	SubjectTokenType    string
	ChildScope          string
	ResolveSubjectToken DelegateTaskSubjectTokenResolver
}

// DelegateTaskTool implements multi-agent task delegation.
type DelegateTaskTool struct {
	k8sClient                   client.Client
	brokeredTransactionExchange *BrokeredDelegateTaskTransactionExchangeConfig
}

// WorkspaceArgs specifies a git workspace for agent runtime tasks
type WorkspaceArgs struct {
	Intent                       string `json:"intent,omitempty"`
	GitRepo                      string `json:"gitRepo,omitempty"`
	Branch                       string `json:"branch,omitempty"`
	Ref                          string `json:"ref,omitempty"`
	ReadCredentialRef            string `json:"readCredentialRef,omitempty"`
	PublicationGitRepo           string `json:"publicationGitRepo,omitempty"`
	PublicationReadCredentialRef string `json:"publicationReadCredentialRef,omitempty"`
	PublicationCredentialRef     string `json:"publicationCredentialRef,omitempty"`
	ForgeCredentialRef           string `json:"forgeCredentialRef,omitempty"`
	PushBranch                   string `json:"pushBranch,omitempty"`
	PRBaseBranch                 string `json:"prBaseBranch,omitempty"`
	CreatePR                     bool   `json:"createPR,omitempty"`
}

// DelegateTaskArgs are the arguments for the delegate_task tool
type DelegateTaskArgs struct {
	Agent          string         `json:"agent"`
	Prompt         string         `json:"prompt"`
	Namespace      string         `json:"namespace,omitempty"`
	AgentNamespace string         `json:"agentNamespace,omitempty"`
	TaskNamespace  string         `json:"taskNamespace,omitempty"`
	Priority       *int32         `json:"priority,omitempty"`
	Timeout        string         `json:"timeout,omitempty"`
	Workspace      *WorkspaceArgs `json:"workspace,omitempty"`
	MaxTurns       *int32         `json:"maxTurns,omitempty"`
	AllowBash      *bool          `json:"allowBash,omitempty"`

	// PriorTask references a previously completed task whose diff should be
	// applied to the workspace before this task begins. Optional.
	PriorTask string `json:"prior_task,omitempty"`

	// Feedback provides review feedback to include in the task prompt.
	// Used with prior_task for iterative code review workflows. Optional.
	Feedback string `json:"feedback,omitempty"`

	// AutoRetry marks this task for structured failure reporting.
	// When enabled, wait_for_tasks includes retry metadata (attempt count,
	// max retries, error message) in the failure result so the coordinator
	// can make informed retry decisions. Does NOT automatically re-create tasks.
	AutoRetry bool `json:"auto_retry,omitempty"`

	// MaxRetries is the maximum retry budget for coordinator reference (default: 2).
	// Only used when auto_retry is true.
	MaxRetries *int `json:"max_retries,omitempty"`
}

// DelegateTaskResult represents the delegation result
type DelegateTaskResult struct {
	TaskName      string `json:"taskName"`
	TaskUID       string `json:"taskUID,omitempty"`
	ParentTaskUID string `json:"parentTaskUID,omitempty"`
	Status        string `json:"status"`
}

// NewDelegateTaskTool creates a new delegate task tool
func NewDelegateTaskTool(k8sClient client.Client) *DelegateTaskTool {
	return &DelegateTaskTool{
		k8sClient: k8sClient,
	}
}

// NewBrokeredDelegateTaskTool creates a delegate_task implementation that uses
// controller-parsed TTS settings and request-scoped subject-token resolution.
// The configuration is copied so later caller mutation cannot change the live
// broker policy.
func NewBrokeredDelegateTaskTool(
	k8sClient client.Client,
	config BrokeredDelegateTaskTransactionExchangeConfig,
) (*DelegateTaskTool, error) {
	if k8sClient == nil {
		return nil, fmt.Errorf("brokered delegate_task requires a Kubernetes client")
	}
	if config.TTS.Enabled() {
		if config.Exchanger == nil {
			return nil, fmt.Errorf("brokered delegate_task requires a transaction-token exchanger when TTS is enabled")
		}
		if config.ResolveSubjectToken == nil {
			return nil, fmt.Errorf("brokered delegate_task requires request-scoped subject-token resolution when TTS is enabled")
		}
	}
	copyConfig := config
	return &DelegateTaskTool{
		k8sClient:                   k8sClient,
		brokeredTransactionExchange: &copyConfig,
	}, nil
}

// RegisterBrokeredDelegateTaskTool replaces the generic delegate_task
// registration with the controller-configured broker implementation.
func RegisterBrokeredDelegateTaskTool(
	registry *Registry,
	k8sClient client.Client,
	config BrokeredDelegateTaskTransactionExchangeConfig,
) error {
	if registry == nil {
		return fmt.Errorf("brokered coordination tool registry is required")
	}
	tool, err := NewBrokeredDelegateTaskTool(k8sClient, config)
	if err != nil {
		return err
	}
	registry.Register(tool)
	return nil
}

// Name returns the tool name
func (t *DelegateTaskTool) Name() string {
	return delegateTaskToolName
}

// Description returns the tool description
func (t *DelegateTaskTool) Description() string {
	return "Delegate a task to another agent. Creates a child Task CR that will be picked up by the specified agent. Supports iterative workflows via prior_task (applies previous diff) and feedback (prepends review feedback to prompt)."
}

// Parameters returns the JSON Schema for parameters
func (t *DelegateTaskTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"agent": {
				"type": "string",
				"description": "Name of the agent to delegate to"
			},
			"prompt": {
				"type": "string",
				"description": "The task prompt for the agent"
			},
			"namespace": {
				"type": "string",
				"description": "Legacy namespace for both child task and agent lookup. Prefer taskNamespace/agentNamespace for cross-namespace delegation."
			},
			"agentNamespace": {
				"type": "string",
				"description": "Namespace containing the target Agent. Defaults to taskNamespace/current namespace."
			},
			"taskNamespace": {
				"type": "string",
				"description": "Namespace where the child Task should be created. Defaults to the parent task namespace."
			},
			"priority": {
				"type": "integer",
				"description": "Priority 0-1000 (defaults to parent priority)"
			},
			"timeout": {
				"type": "string",
				"description": "Task timeout duration, e.g. \"20m\""
			},
			"workspace": {
				"type": "object",
				"description": "Git workspace configuration for agent runtime tasks",
				"properties": {
					"gitRepo": {
						"type": "string",
						"description": "Git repository URL as a credential-free HTTPS URL, e.g. https://github.com/owner/repo (GitHub SSH roots are converted automatically). Required for write intent."
					},
					"branch": {
						"type": "string",
						"description": "Git branch name (short name or refs/heads/... ref). Omit with ref to resolve and freeze the repository's advertised default branch."
					},
					"ref": {
						"type": "string",
						"description": "Exact source selector: a full commit SHA, refs/heads/... branch, refs/tags/... tag, or short ref name. Other refs/ namespaces are rejected."
					},
					"intent": {
						"type": "string",
						"enum": ["read", "write"],
						"description": "Workspace intent. Defaults to read; publication fields require write."
					},
					"readCredentialRef": {
						"type": "string",
						"description": "Optional Secret name for clone/read credentials. Omit to auto-discover a read credential when available. Requires gitRepo."
					},
					"publicationGitRepo": {
						"type": "string",
						"description": "Publication repository URL for write Tasks as a credential-free HTTPS URL."
					},
					"publicationReadCredentialRef": {
						"type": "string",
						"description": "Optional Secret name for target-repository preflight and verification credentials."
					},
					"publicationCredentialRef": {
						"type": "string",
						"description": "Secret name for target-repository write credentials. Required for write intent."
					},
					"forgeCredentialRef": {
						"type": "string",
						"description": "Secret name for forge API credentials used to reconcile pull requests. Required when createPR is true."
					},
					"pushBranch": {
						"type": "string",
						"description": "Publication branch name for a write Task."
					},
					"prBaseBranch": {
						"type": "string",
						"description": "Pull request base branch. Required when createPR is true."
					},
					"createPR": {
						"type": "boolean",
						"description": "Reconcile a pull request after publication. Requires prBaseBranch and forgeCredentialRef."
					}
				}
			},
			"maxTurns": {
				"type": "integer",
				"description": "Maximum number of turns for the agent"
			},
			"allowBash": {
				"type": "` + jsonSchemaTypeBoolean + `",
				"description": "Whether to allow bash execution in the agent"
			},
			"prior_task": {
				"type": "string",
				"description": "Name of a previously completed task whose diff should be applied to the workspace before this task starts. Used for iterative workflows."
			},
			"feedback": {
				"type": "string",
				"description": "Review feedback to prepend to the task prompt. Used with prior_task for iterative code review workflows."
			},
			"auto_retry": {
				"type": "` + jsonSchemaTypeBoolean + `",
				"description": "Include structured retry metadata in failure reports. The coordinator decides whether to retry — wait_for_tasks does not auto-retry."
			},
			"max_retries": {
				"type": "integer",
				"description": "Maximum retry budget for coordinator reference (default: 2). Only used when auto_retry is true."
			}
		},
		"required": ["agent", "prompt"]
	}`)
}

// delegationContext holds validated delegation parameters.
type delegationContext struct {
	args         DelegateTaskArgs
	parentName   string
	currentDepth int
	namespace    string
	agentNS      string
	parentTask   *corev1alpha1.Task
	targetAgent  *corev1alpha1.Agent
	priority     *int32
}

func delegatedAgentAllowed(agentName, agentNamespace, taskNamespace, allowedAgents string) bool {
	agentName = strings.TrimSpace(agentName)
	agentNamespace = strings.TrimSpace(agentNamespace)
	taskNamespace = strings.TrimSpace(taskNamespace)
	for allowed := range strings.SplitSeq(allowedAgents, ",") {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		parts := strings.SplitN(allowed, "/", 2)
		if len(parts) == 2 {
			if strings.TrimSpace(parts[0]) == agentNamespace && strings.TrimSpace(parts[1]) == agentName {
				return true
			}
			continue
		}
		if allowed == agentName && (agentNamespace == "" || agentNamespace == taskNamespace) {
			return true
		}
	}
	return false
}

func namespacedDelegateAgent(namespace, name string) string {
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func validateDelegateNamespaceScope(toolCtx *ToolContext, resourceKind, namespace string) error {
	if toolCtx == nil {
		return nil
	}
	namespace = strings.TrimSpace(namespace)
	if watchNamespace := strings.TrimSpace(toolCtx.WatchNamespace); watchNamespace != "" && namespace != watchNamespace {
		return fmt.Errorf("%s namespace %q is outside watched namespace %q", resourceKind, namespace, watchNamespace)
	}
	if toolCtx.EnforceNamespaceIsolation {
		requestNamespace := strings.TrimSpace(toolCtx.Namespace)
		if namespace != requestNamespace {
			return fmt.Errorf("%s namespace %q is outside request namespace %q", resourceKind, namespace, requestNamespace)
		}
	}
	return nil
}

// parseDelegateArgs parses and validates delegation arguments against either
// worker environment or an authenticated broker request context.
//
//nolint:gocyclo // Delegation keeps worker and authenticated broker policy checks auditable in one path.
func (t *DelegateTaskTool) parseDelegateArgs(ctx context.Context, args json.RawMessage) (*delegationContext, error) {
	parentName := os.Getenv(envOrkaTaskName)
	parentNamespace := os.Getenv(envOrkaTaskNamespace)
	depthStr := os.Getenv(envOrkaCoordinationDepth)
	allowedAgents := os.Getenv(envOrkaCoordinationAllowedAgents)
	maxDepthStr := os.Getenv(envOrkaCoordinationMaxDepth)
	toolCtx := GetToolContext(ctx)
	brokered := toolCtx != nil && toolCtx.Brokered
	if brokered {
		parentName = strings.TrimSpace(toolCtx.TaskID)
		parentNamespace = strings.TrimSpace(toolCtx.Namespace)
		depthStr = ""
		allowedAgents = ""
		maxDepthStr = ""
		if parentName == "" || parentNamespace == "" {
			return nil, fmt.Errorf("brokered delegation requires request-scoped task and namespace context")
		}
	}

	var delegateArgs DelegateTaskArgs
	if err := json.Unmarshal(args, &delegateArgs); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}

	if delegateArgs.Agent == "" {
		return nil, fmt.Errorf("agent is required")
	}
	if delegateArgs.Prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}

	// Determine child task and target agent namespaces. The legacy namespace
	// field remains compatible: when no split fields are supplied it targets both
	// the child task and agent. New callers should prefer taskNamespace and
	// agentNamespace so cross-namespace agent catalogs do not move child tasks out
	// of the parent namespace by accident.
	taskNS := strings.TrimSpace(delegateArgs.TaskNamespace)
	agentNS := strings.TrimSpace(delegateArgs.AgentNamespace)
	legacyNS := strings.TrimSpace(delegateArgs.Namespace)
	if taskNS == "" {
		if legacyNS != "" && agentNS == "" {
			taskNS = legacyNS
		} else {
			taskNS = parentNamespace
		}
	}
	if taskNS == "" {
		taskNS = defaultNamespace
	}
	if agentNS == "" {
		if legacyNS != "" {
			agentNS = legacyNS
		} else {
			agentNS = taskNS
		}
	}

	// Enforce the authenticated request's namespace boundary before any target
	// Agent lookup or child creation. This keeps brokered execution at least as
	// narrow as the controller's watch and namespace-isolation configuration.
	if err := validateDelegateNamespaceScope(toolCtx, "child Task", taskNS); err != nil {
		return nil, err
	}
	if err := validateDelegateNamespaceScope(toolCtx, "target Agent", agentNS); err != nil {
		return nil, err
	}

	// Fetch parent Task for owner reference. Brokered calls always require the
	// exact authenticated Task; worker calls retain the legacy empty-parent path.
	parentTask := &corev1alpha1.Task{}
	if parentName != "" {
		parentLookupNS := parentNamespace
		if parentLookupNS == "" {
			parentLookupNS = taskNS
		}
		if err := t.k8sClient.Get(ctx, types.NamespacedName{Name: parentName, Namespace: parentLookupNS}, parentTask); err != nil {
			return nil, fmt.Errorf("failed to get parent task: %w", err)
		}
	}

	if brokered {
		authenticatedUID := strings.TrimSpace(toolCtx.TaskUID)
		if authenticatedUID == "" || string(parentTask.UID) != authenticatedUID {
			return nil, fmt.Errorf("brokered delegation parent Task identity changed")
		}
		depthStr = strings.TrimSpace(parentTask.Annotations[labels.AnnotationCoordinationDepth])
		if depthStr == "" {
			depthStr = "0"
		}
		if parentTask.Spec.AgentRef == nil || strings.TrimSpace(parentTask.Spec.AgentRef.Name) == "" {
			return nil, fmt.Errorf("brokered delegation requires a parent Task with agentRef")
		}
		parentAgentNamespace := parentTask.Namespace
		if value := strings.TrimSpace(parentTask.Spec.AgentRef.Namespace); value != "" {
			parentAgentNamespace = value
		}
		parentAgent := &corev1alpha1.Agent{}
		if err := t.k8sClient.Get(ctx, types.NamespacedName{
			Name: parentTask.Spec.AgentRef.Name, Namespace: parentAgentNamespace,
		}, parentAgent); err != nil {
			return nil, fmt.Errorf("failed to get parent agent: %w", err)
		}
		coordination := parentAgent.Spec.Coordination
		if coordination == nil || !coordination.Enabled {
			return nil, fmt.Errorf("parent agent does not have coordination enabled")
		}
		if coordination.MaxDepth > 0 {
			maxDepthStr = strconv.FormatInt(int64(coordination.MaxDepth), 10)
		}
		allowed := make([]string, 0, len(coordination.AllowedAgents))
		for _, agent := range coordination.AllowedAgents {
			name := strings.TrimSpace(agent.Name)
			if name == "" {
				continue
			}
			if namespace := strings.TrimSpace(agent.Namespace); namespace != "" {
				allowed = append(allowed, namespacedDelegateAgent(namespace, name))
			} else {
				allowed = append(allowed, name)
			}
		}
		allowedAgents = strings.Join(allowed, ",")
	}

	// Validate depth.
	currentDepth := 0
	if depthStr != "" {
		var err error
		currentDepth, err = strconv.Atoi(depthStr)
		if err != nil {
			return nil, fmt.Errorf("invalid coordination depth %q: %w", depthStr, err)
		}
	}

	maxDepth := 3
	if maxDepthStr != "" {
		var err error
		maxDepth, err = strconv.Atoi(maxDepthStr)
		if err != nil {
			return nil, fmt.Errorf("invalid max coordination depth %q: %w", maxDepthStr, err)
		}
	}

	if currentDepth+1 > maxDepth {
		return nil, fmt.Errorf("coordination depth exceeded: current depth %d, max depth %d", currentDepth, maxDepth)
	}

	// Validate agent is allowed, including namespace when the allowlist entry is
	// namespaced as namespace/name. Bare names retain existing same-name behavior.
	if (brokered || allowedAgents != "") && !delegatedAgentAllowed(delegateArgs.Agent, agentNS, taskNS, allowedAgents) {
		return nil, fmt.Errorf("agent %q is not in the allowed agents list", namespacedDelegateAgent(agentNS, delegateArgs.Agent))
	}

	// Determine priority.
	var priority *int32
	if delegateArgs.Priority != nil {
		priority = delegateArgs.Priority
	} else if parentTask.Spec.Priority != nil {
		priority = parentTask.Spec.Priority
	}

	// Look up the target Agent to determine task type.
	targetAgent := &corev1alpha1.Agent{}
	if err := t.k8sClient.Get(ctx, types.NamespacedName{
		Name: delegateArgs.Agent, Namespace: agentNS,
	}, targetAgent); err != nil {
		return nil, fmt.Errorf("failed to get agent %q: %w", namespacedDelegateAgent(agentNS, delegateArgs.Agent), err)
	}

	return &delegationContext{
		args:         delegateArgs,
		parentName:   parentName,
		currentDepth: currentDepth,
		namespace:    taskNS,
		agentNS:      agentNS,
		parentTask:   parentTask,
		targetAgent:  targetAgent,
		priority:     priority,
	}, nil
}

// buildDelegatedTask creates the child Task object from a validated delegation context.
func (t *DelegateTaskTool) buildDelegatedTask(ctx context.Context, dc *delegationContext) (*corev1alpha1.Task, error) {
	// Auto-detect task type based on agent configuration
	taskType := corev1alpha1.TaskTypeAI
	if dc.targetAgent.Spec.Runtime != nil {
		taskType = corev1alpha1.TaskTypeAgent
	}

	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: dc.parentName + "-child-",
			Namespace:    dc.namespace,
			Labels: map[string]string{
				labels.LabelParentTask:     labels.SelectorValue(dc.parentName),
				labels.LabelCoordinator:    trueStr,
				labels.LabelDelegatedAgent: dc.args.Agent,
			},
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: strconv.Itoa(dc.currentDepth + 1),
				labels.AnnotationParentTaskName:    dc.parentName,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: taskType,
			AgentRef: &corev1alpha1.AgentReference{
				Name: dc.args.Agent,
			},
			Prompt:   dc.args.Prompt,
			Priority: dc.priority,
		},
	}
	if dc.agentNS != "" && dc.agentNS != dc.namespace {
		childTask.Spec.AgentRef.Namespace = dc.agentNS
	}

	if dc.args.Timeout != "" {
		timeout, err := time.ParseDuration(dc.args.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout %q: %w", dc.args.Timeout, err)
		}
		if timeout > 0 {
			childTask.Spec.Timeout = &metav1.Duration{Duration: timeout}
		}
	}

	// Store auto-retry config as annotations
	if dc.args.AutoRetry {
		childTask.Annotations[labels.AnnotationAutoRetry] = trueStr
		maxRetries := 2
		if dc.args.MaxRetries != nil && *dc.args.MaxRetries >= 0 {
			maxRetries = *dc.args.MaxRetries
		}
		childTask.Annotations[labels.AnnotationMaxRetries] = strconv.Itoa(maxRetries)
		childTask.Annotations[labels.AnnotationRetryCount] = "0"
		childTask.Annotations[labels.AnnotationOriginalPrompt] = dc.args.Prompt
	}

	// Set agent runtime config for agent-type tasks
	if taskType == corev1alpha1.TaskTypeAgent {
		if err := t.applyAgentRuntimeConfig(ctx, childTask, dc); err != nil {
			return nil, err
		}
	}

	// Prepend feedback to prompt if provided
	if dc.args.Feedback != "" {
		childTask.Spec.Prompt = fmt.Sprintf("FEEDBACK FROM REVIEW:\n%s\n\nTASK:\n%s", dc.args.Feedback, childTask.Spec.Prompt)
	}
	if taskType == corev1alpha1.TaskTypeAI {
		// Native AI admission requires spec.ai. Keep Agent-owned provider,
		// model, prompt defaults, skills, and tools authoritative; the child
		// contributes only its task-specific prompt.
		childTask.Spec.AI = &corev1alpha1.AISpec{Prompt: childTask.Spec.Prompt}
	}

	// Handle prior task reference for iterative workflows
	if dc.args.PriorTask != "" {
		t.applyPriorTaskConfig(ctx, childTask, dc)
	}

	inheritTaskProvenance(childTask, dc.parentTask)
	if toolCtx := GetToolContext(ctx); toolCtx != nil && toolCtx.Brokered {
		if dc.parentTask.UID == "" {
			return nil, fmt.Errorf("brokered delegation requires an immutable parent Task UID")
		}
		childTask.Annotations[labels.AnnotationParentTaskUID] = string(dc.parentTask.UID)
		if toolCtx.ExternalEffects != nil || strings.TrimSpace(toolCtx.SessionID) != "" || strings.TrimSpace(toolCtx.OperationID) != "" {
			effectID, err := brokeredDelegationEffectID(toolCtx)
			if err != nil {
				return nil, err
			}
			childTask.Annotations[labels.AnnotationDelegationEffectID] = effectID
		}
	}

	// Set owner reference only for same-namespace children. Kubernetes treats
	// cross-namespace owner references for namespaced objects as invalid and may
	// garbage-collect the child; labels/annotations still preserve lineage. Do
	// not request foreground-deletion blocking: workers can create Tasks but do
	// not have permission to update the parent Task's finalizers.
	if dc.parentTask.UID != "" && dc.parentTask.Namespace == dc.namespace {
		isController := true
		childTask.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: corev1alpha1.GroupVersion.String(),
				Kind:       taskKindString,
				Name:       dc.parentTask.Name,
				UID:        dc.parentTask.UID,
				Controller: &isController,
			},
		}
	}

	return childTask, nil
}

// applyAgentRuntimeConfig sets agent runtime configuration on the child task.
func (t *DelegateTaskTool) applyAgentRuntimeConfig(ctx context.Context, childTask *corev1alpha1.Task, dc *delegationContext) error {
	childTask.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{}

	if dc.args.Workspace != nil {
		intent := corev1alpha1.WorkspaceIntent(strings.ToLower(strings.TrimSpace(dc.args.Workspace.Intent)))
		if intent == "" {
			intent = corev1alpha1.WorkspaceIntentRead
		}
		if intent != corev1alpha1.WorkspaceIntentRead && intent != corev1alpha1.WorkspaceIntentWrite {
			return fmt.Errorf("workspace intent must be read or write")
		}
		gitRepo, repoErr := canonicalAgentWorkspaceRepositoryArg("gitRepo", dc.args.Workspace.GitRepo)
		if repoErr != nil {
			return fmt.Errorf("%s (%s)", repoErr.Message, repoErr.Suggestion)
		}
		publicationGitRepo, repoErr := canonicalAgentWorkspaceRepositoryArg("publicationGitRepo", dc.args.Workspace.PublicationGitRepo)
		if repoErr != nil {
			return fmt.Errorf("%s (%s)", repoErr.Message, repoErr.Suggestion)
		}
		workspace := &corev1alpha1.WorkspaceConfig{
			Intent:             intent,
			GitRepo:            gitRepo,
			Branch:             dc.args.Workspace.Branch,
			Ref:                dc.args.Workspace.Ref,
			PublicationGitRepo: publicationGitRepo,
			PushBranch:         dc.args.Workspace.PushBranch,
			PRBaseBranch:       dc.args.Workspace.PRBaseBranch,
			CreatePR:           dc.args.Workspace.CreatePR,
		}
		if workspaceRequestsPublication(workspace) {
			workspace.Intent = corev1alpha1.WorkspaceIntentWrite
		}
		readCredential := strings.TrimSpace(dc.args.Workspace.ReadCredentialRef)
		publicationReadCredential := strings.TrimSpace(dc.args.Workspace.PublicationReadCredentialRef)
		publicationCredential := strings.TrimSpace(dc.args.Workspace.PublicationCredentialRef)
		forgeCredential := strings.TrimSpace(dc.args.Workspace.ForgeCredentialRef)
		// Mirror the controller's workspace preflight before creating the child
		// Task so a doomed configuration fails here instead of after creation.
		if wsErr := agentWorkspaceArgError(workspace, readCredential, publicationReadCredential, publicationCredential, forgeCredential); wsErr != nil {
			return fmt.Errorf("%s (%s)", wsErr.Message, wsErr.Suggestion)
		}
		// Only attach read credentials alongside a gitRepo: the controller
		// workspace preflight rejects readCredentialRef without gitRepo, so
		// auto-discovery must not doom a repository-free workspace.
		if strings.TrimSpace(workspace.GitRepo) != "" {
			readRef, err := resolveWorkspaceCredentialRef(ctx, t.k8sClient, dc.namespace, dc.targetAgent, readCredential)
			if err != nil {
				return err
			}
			workspace.ReadCredentialRef = readRef
		}
		if publicationReadCredential != "" {
			workspace.PublicationReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: publicationReadCredential}
		}
		if publicationCredential != "" {
			workspace.PublicationCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: publicationCredential}
		}
		if forgeCredential != "" {
			workspace.ForgeCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: forgeCredential}
		}
		childTask.Spec.Workspace = workspace
	}

	if dc.args.MaxTurns != nil {
		childTask.Spec.AgentRuntime.MaxTurns = dc.args.MaxTurns
	}
	if dc.args.AllowBash != nil {
		childTask.Spec.AgentRuntime.AllowBash = dc.args.AllowBash
	}
	return nil
}

// applyPriorTaskConfig sets prior task reference and copies workspace config from the prior task.
func (t *DelegateTaskTool) applyPriorTaskConfig(ctx context.Context, childTask *corev1alpha1.Task, dc *delegationContext) {
	childTask.Spec.PriorTaskRef = &corev1alpha1.PriorTaskReference{
		Name:      dc.args.PriorTask,
		Namespace: dc.namespace,
	}

	priorTask := &corev1alpha1.Task{}
	if err := t.k8sClient.Get(ctx, types.NamespacedName{Name: dc.args.PriorTask, Namespace: dc.namespace}, priorTask); err == nil {
		// Copy workspace from prior task if not explicitly provided
		if dc.args.Workspace == nil {
			if priorWorkspace := taskWorkspace(priorTask); priorWorkspace != nil {
				childTask.Spec.Workspace = priorWorkspace.DeepCopy()
			}
		}

		// Increment iteration count
		iteration := 1
		if iterStr, ok := priorTask.Labels[labels.LabelIteration]; ok {
			if iter, err := strconv.Atoi(iterStr); err == nil {
				iteration = iter + 1
			}
		}
		childTask.Labels[labels.LabelIteration] = strconv.Itoa(iteration)

		// Copy or generate iteration group
		if group, ok := priorTask.Labels[labels.LabelIterationGroup]; ok {
			childTask.Labels[labels.LabelIterationGroup] = group
		} else {
			childTask.Labels[labels.LabelIterationGroup] = string(priorTask.UID)
		}
	}
}

func (t *DelegateTaskTool) shouldPrepareChildTransactionToken(
	ctx context.Context,
	parentTask *corev1alpha1.Task,
) (bool, error) {
	toolCtx := GetToolContext(ctx)
	if toolCtx == nil || !toolCtx.Brokered {
		return shouldPrepareChildTransactionToken(parentTask)
	}
	if parentTask == nil || parentTask.Spec.Transaction == nil {
		return false, nil
	}
	if t.brokeredTransactionExchange == nil {
		return false, fmt.Errorf("brokered delegate_task transaction-token exchange configuration is not registered")
	}
	return t.brokeredTransactionExchange.TTS.Enabled(), nil
}

func (t *DelegateTaskTool) prepareChildTransactionToken(
	ctx context.Context,
	parentTask, childTask *corev1alpha1.Task,
	operation, agent string,
) error {
	toolCtx := GetToolContext(ctx)
	if toolCtx == nil || !toolCtx.Brokered {
		return prepareChildTransactionToken(ctx, t.k8sClient, parentTask, childTask, operation, agent)
	}
	return t.prepareBrokeredChildTransactionToken(ctx, parentTask, childTask, operation, agent)
}

func (t *DelegateTaskTool) prepareBrokeredChildTransactionToken(
	ctx context.Context,
	parentTask, childTask *corev1alpha1.Task,
	operation, agent string,
) error {
	config := t.brokeredTransactionExchange
	if config == nil {
		return fmt.Errorf("brokered delegate_task transaction-token exchange configuration is not registered")
	}
	if !config.TTS.Enabled() || parentTask == nil || parentTask.Spec.Transaction == nil {
		return nil
	}
	if parentTask.UID == "" {
		return fmt.Errorf("parent task UID is required for child transaction token exchange")
	}
	if err := requireSameNamespaceChildTokenExchange(parentTask, childTask); err != nil {
		return err
	}
	if config.Exchanger == nil {
		return fmt.Errorf("brokered delegate_task transaction-token exchanger is not configured")
	}
	if config.ResolveSubjectToken == nil {
		return fmt.Errorf("brokered delegate_task request-scoped subject-token resolver is not configured")
	}

	scope := strings.TrimSpace(config.ChildScope)
	if scope == "" {
		return fmt.Errorf("context-token child scope is required when brokered TTS is enabled for child task tokens")
	}
	if err := validateChildTransactionScope(parentTask, scope); err != nil {
		return err
	}
	subjectToken, err := config.ResolveSubjectToken(ctx, parentTask, config.TTS.TokenSource)
	if err != nil {
		return fmt.Errorf("resolving brokered child transaction subject token: %w", err)
	}
	if strings.TrimSpace(subjectToken) == "" {
		return fmt.Errorf("resolving brokered child transaction subject token: resolved token is empty")
	}
	subjectTokenType := strings.TrimSpace(config.SubjectTokenType)
	if subjectTokenType == "" {
		subjectTokenType = contexttoken.SubjectTokenTypeForSource(config.TTS.TokenSource)
	}

	requestDetails := map[string]any{
		delegateRequestOperationField:  operation,
		delegateRequestParentTaskField: parentTask.Name,
		namespaceField:                 childTask.Namespace,
	}
	if agent != "" {
		requestDetails["agent"] = agent
	}
	if parentTask.Spec.Transaction.ID != "" {
		requestDetails["txn"] = parentTask.Spec.Transaction.ID
	}
	token, err := config.Exchanger.Exchange(ctx, contexttoken.ExchangeRequest{
		SubjectToken:     subjectToken,
		SubjectTokenType: subjectTokenType,
		Scope:            scope,
		RequestedTTL:     config.TTS.ChildTokenTTL,
		RequestDetails:   requestDetails,
	})
	if err != nil {
		return fmt.Errorf("exchanging child transaction token: %w", err)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("exchanging child transaction token: TTS returned an empty token")
	}

	secretName, err := childTransactionTokenSecretName(parentTask.Name)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            secretName,
			Namespace:       childTask.Namespace,
			OwnerReferences: childTokenSecretOwnerReferences(parentTask, childTask),
			Labels: map[string]string{
				labels.LabelParentTask: labels.SelectorValue(parentTask.Name),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName: parentTask.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
	if err := t.k8sClient.Create(ctx, secret); err != nil {
		return fmt.Errorf("creating child transaction token secret: %w", err)
	}
	stampChildTransactionScope(childTask, scope)
	if childTask.Annotations == nil {
		childTask.Annotations = map[string]string{}
	}
	childTask.Annotations[labels.AnnotationTransactionTokenSecret] = secretName
	return nil
}

// Execute delegates a task to another agent
func (t *DelegateTaskTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	dc, err := t.parseDelegateArgs(ctx, args)
	if err != nil {
		return "", err
	}

	childTask, err := t.buildDelegatedTask(ctx, dc)
	if err != nil {
		return "", err
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(orkatracing.DelegateAttributes(dc.parentName, "")...)
	orkatracing.StampTaskTraceContext(ctx, childTask)
	if err := validateChildTaskAgainstParentTransaction(ctx, t.k8sClient, dc.parentTask, childTask, dc.args.Agent); err != nil {
		return "", err
	}

	childTokenExchangeEnabled, err := t.shouldPrepareChildTransactionToken(ctx, dc.parentTask)
	if err != nil {
		return "", err
	}
	if childTokenExchangeEnabled {
		markChildTransactionTokenPending(childTask)
		if err := t.prepareChildTransactionToken(ctx, dc.parentTask, childTask, "delegateTask", dc.args.Agent); err != nil {
			return "", err
		}
	}

	if err := t.k8sClient.Create(ctx, childTask); err != nil {
		if childTokenExchangeEnabled {
			cleanupChildTransactionTokenSecret(ctx, t.k8sClient, childTask)
		}
		return "", fmt.Errorf("failed to create child task: %w", err)
	}
	span.SetAttributes(orkatracing.DelegateAttributes("", childTask.Name)...)

	if childTokenExchangeEnabled {
		if err := adoptChildTransactionTokenSecret(ctx, t.k8sClient, childTask); err != nil {
			cleanupChildTaskAfterTokenAdoptionFailure(ctx, t.k8sClient, childTask)
			return "", err
		}
		if err := patchPreparedChildTransactionToken(ctx, t.k8sClient, childTask); err != nil {
			cleanupChildTaskAfterTokenAdoptionFailure(ctx, t.k8sClient, childTask)
			return "", err
		}
	}

	result := DelegateTaskResult{
		TaskName:      childTask.Name,
		TaskUID:       string(childTask.UID),
		ParentTaskUID: string(dc.parentTask.UID),
		Status:        GitHubPullRequestStatusCreated,
	}

	output, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(output), nil
}

func brokeredDelegationEffectID(toolCtx *ToolContext) (string, error) {
	if toolCtx == nil || !toolCtx.Brokered {
		return "", nil
	}
	identity := store.ExternalEffectIdentity{
		Kind:        "acp-mcp-tool",
		Namespace:   strings.TrimSpace(toolCtx.Namespace),
		AggregateID: strings.TrimSpace(toolCtx.SessionID),
		OperationID: strings.TrimSpace(toolCtx.OperationID),
	}
	id, err := identity.CanonicalID()
	if err != nil {
		return "", fmt.Errorf("bind brokered delegation to its durable effect: %w", err)
	}
	return id, nil
}

func markChildTransactionTokenPending(childTask *corev1alpha1.Task) {
	if childTask.Annotations == nil {
		childTask.Annotations = map[string]string{}
	}
	childTask.Annotations[labels.AnnotationTransactionTokenPending] = trueStr
	childTask.Annotations[labels.AnnotationTransactionTokenPendingSince] = time.Now().Format(time.RFC3339Nano)
}

func patchPreparedChildTransactionToken(ctx context.Context, k8sClient client.Client, childTask *corev1alpha1.Task) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				labels.AnnotationTransactionTokenPending:      nil,
				labels.AnnotationTransactionTokenPendingSince: nil,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("encoding child task transaction token metadata patch: %w", err)
	}
	target := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childTask.Name,
			Namespace: childTask.Namespace,
		},
	}
	if err := k8sClient.Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("updating child task transaction token metadata: %w", err)
	}
	if childTask.Annotations != nil {
		delete(childTask.Annotations, labels.AnnotationTransactionTokenPending)
		delete(childTask.Annotations, labels.AnnotationTransactionTokenPendingSince)
	}
	return nil
}

// Ensure DelegateTaskTool implements Tool
var _ Tool = (*DelegateTaskTool)(nil)
