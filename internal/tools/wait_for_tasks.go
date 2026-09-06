/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/workers/common"
)

// WaitForTasksTool implements waiting for child tasks to complete
type WaitForTasksTool struct {
	k8sClient client.Client
}

// WaitForTasksArgs are the arguments for the wait_for_tasks tool
type WaitForTasksArgs struct {
	Tasks   []string `json:"tasks"`
	Timeout string   `json:"timeout,omitempty"`
}

// WaitForTasksResult represents the aggregated result
type WaitForTasksResult struct {
	Completed bool             `json:"completed"`
	Results   []TaskResultInfo `json:"results"`
}

// TaskResultInfo holds individual task result information
type TaskResultInfo struct {
	Task             string                                     `json:"task"`
	Agent            string                                     `json:"agent,omitempty"`
	Phase            string                                     `json:"phase"`
	Result           string                                     `json:"result,omitempty"`
	Summary          string                                     `json:"summary,omitempty"`
	Verdict          string                                     `json:"verdict,omitempty"`
	Feedback         string                                     `json:"feedback,omitempty"`
	Files            []string                                   `json:"files,omitempty"`
	Data             map[string]any                             `json:"data,omitempty"`
	Artifacts        []common.ArtifactRef                       `json:"artifacts,omitempty"`
	BaseSHA          string                                     `json:"baseSHA,omitempty"`
	HeadSHA          string                                     `json:"headSHA,omitempty"`
	PushBranch       string                                     `json:"pushBranch,omitempty"`
	WorkspaceRef     string                                     `json:"workspaceRef,omitempty"`
	WorkspaceBranch  string                                     `json:"workspaceBranch,omitempty"`
	Iteration        string                                     `json:"iteration,omitempty"`
	FailureDetails   *FailureDetails                            `json:"failureDetails,omitempty"`
	Retried          bool                                       `json:"retried,omitempty"`
	RetryTaskName    string                                     `json:"retryTaskName,omitempty"`
	ExecutionOutcome *corev1alpha1.TaskWorkloadExecutionOutcome `json:"executionOutcome,omitempty"`
	WorkspaceStatus  *corev1alpha1.ExecutionWorkspaceStatus     `json:"workspaceStatus,omitempty"`
}

func waitTaskTerminal(phase corev1alpha1.TaskPhase) bool {
	switch phase {
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
		return true
	default:
		return false
	}
}

// FailureDetails provides structured information about a failed task
type FailureDetails struct {
	Message    string `json:"message"`
	RetryCount int    `json:"retryCount"`
	MaxRetries int    `json:"maxRetries"`
}

const (
	maxWaitTaskSummaryChars = 4096
	maxWaitTaskDataBytes    = 32 * 1024
)

// NewWaitForTasksTool creates a new wait_for_tasks tool
func NewWaitForTasksTool(k8sClient client.Client) *WaitForTasksTool {
	return &WaitForTasksTool{
		k8sClient: k8sClient,
	}
}

// Name returns the tool name
func (t *WaitForTasksTool) Name() string {
	return waitForTasksToolName
}

// Description returns the tool description
func (t *WaitForTasksTool) Description() string {
	return "Wait for one or more child tasks to complete and return their results. Use after delegating tasks to check completion status."
}

// Parameters returns the JSON Schema for parameters
func (t *WaitForTasksTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"tasks": {
				"type": "` + jsonSchemaTypeArray + `",
				"items": {"type": "string"},
				"description": "Child task names to wait for"
			},
			"timeout": {
				"type": "string",
				"description": "Max wait duration, e.g. '5m' (default: '10m')"
			}
		},
		"required": ["tasks"]
	}`)
}

// Execute waits for the specified tasks to complete and returns their results
func (t *WaitForTasksTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var waitArgs WaitForTasksArgs
	if err := json.Unmarshal(args, &waitArgs); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}

	if len(waitArgs.Tasks) == 0 {
		return "", fmt.Errorf("at least one task name is required")
	}

	// Parse timeout
	timeoutStr := waitArgs.Timeout
	if timeoutStr == "" {
		timeoutStr = "10m"
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return "", fmt.Errorf("invalid timeout %q: %w", timeoutStr, err)
	}

	ns := ""
	toolCtx := GetToolContext(ctx)
	brokered := toolCtx != nil && toolCtx.Brokered
	if toolCtx != nil {
		ns = strings.TrimSpace(toolCtx.Namespace)
	}
	if ns == "" {
		ns = os.Getenv(envOrkaTaskNamespace)
	}
	if ns == "" {
		return "", fmt.Errorf("%s environment variable is not set", envOrkaTaskNamespace)
	}
	parent, err := t.validateBrokeredCaller(ctx, toolCtx, ns)
	if err != nil {
		return "", err
	}

	deadline := time.Now().Add(timeout)
	pollInterval := 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	results := make(map[string]*TaskResultInfo)
	for _, taskName := range waitArgs.Tasks {
		results[taskName] = &TaskResultInfo{
			Task:  taskName,
			Phase: "Unknown",
		}
	}

	allTerminal := false
	for {
		allTerminal = true
		for _, taskName := range waitArgs.Tasks {
			var task corev1alpha1.Task
			err := t.k8sClient.Get(ctx, types.NamespacedName{Name: taskName, Namespace: ns}, &task)
			if err != nil {
				allTerminal = false
				results[taskName].Phase = taskPhaseErrorString
				results[taskName].Result = fmt.Sprintf("error: %v", err)
				continue
			}
			if err := validateBrokeredWaitTarget(ctx, toolCtx, parent, &task); err != nil {
				return "", err
			}

			phase := task.Status.Phase

			// Don't overwrite a task already marked as Retried
			if results[taskName].Phase != "Retried" {
				results[taskName].Phase = string(phase)
			}

			if task.Spec.AgentRef != nil {
				results[taskName].Agent = task.Spec.AgentRef.Name
			}
			if ws := taskWorkspace(&task); ws != nil {
				results[taskName].WorkspaceRef = ws.Ref
				results[taskName].WorkspaceBranch = ws.Branch
			}

			results[taskName].ExecutionOutcome = task.Status.ExecutionOutcome
			results[taskName].WorkspaceStatus = task.Status.ExecutionWorkspace

			if !waitTaskTerminal(phase) {
				allTerminal = false
				continue
			}

			// Handle failed tasks — report failure details but do NOT auto-retry.
			// Retry logic is handled by the coordinator LLM which can make informed
			// decisions about whether and how to retry.
			if phase == corev1alpha1.TaskPhaseFailed {
				if task.Annotations[labels.AnnotationAutoRetry] == trueStr {
					retryCount, maxRetries := getRetryInfo(&task)
					results[taskName].FailureDetails = &FailureDetails{
						Message:    task.Status.Message,
						RetryCount: retryCount,
						MaxRetries: maxRetries,
					}
				}
			}

			// Fetch result if available. Controller-side brokered coordination calls
			// provide ResultStore through ToolContext; worker calls fall back to the
			// internal HTTP result endpoint.
			if task.Status.ResultRef != nil && task.Status.ResultRef.Available {
				resultStr, fetchErr := fetchTaskResultForNamespace(ctx, ns, taskName)
				if fetchErr == nil {
					// Parse structured result and strip diff to avoid context bloat.
					sr := common.ParseStructuredResult(resultStr)
					summaryText := sr.Summary
					if brokered {
						// Redact before truncation so a token crossing the summary
						// boundary cannot leak as an unmatched prefix.
						summaryText = redact.SensitiveText(summaryText)
					}
					summary := truncateWaitTaskSummary(summaryText)
					results[taskName].Summary = summary
					results[taskName].Verdict = sr.Verdict
					results[taskName].Feedback = sr.Feedback
					results[taskName].Files = sr.Files
					results[taskName].Data = boundWaitTaskData(sr.Data)
					results[taskName].Artifacts = sr.Artifacts
					results[taskName].BaseSHA = sr.BaseSHA
					results[taskName].HeadSHA = sr.HeadSHA
					results[taskName].PushBranch = sr.PushBranch
					// Set Result to summary only (never include raw diff).
					results[taskName].Result = summary
				} else {
					results[taskName].Result = fmt.Sprintf("error reading result: %v", fetchErr)
				}
			} else if task.Status.Message != "" {
				results[taskName].Result = task.Status.Message
			}

			// Add iteration label if present
			if iterStr, ok := task.Labels[labels.LabelIteration]; ok {
				results[taskName].Iteration = iterStr
			}
		}

		if allTerminal {
			break
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		// Wait for the shorter of poll interval or remaining time
		wait := min(remaining, pollInterval)

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(wait):
		}
	}

	// Build ordered results
	resultList := make([]TaskResultInfo, 0, len(waitArgs.Tasks))
	for _, taskName := range waitArgs.Tasks {
		result := *results[taskName]
		if brokered {
			redactBrokeredWaitTaskResult(&result)
		}
		resultList = append(resultList, result)
	}

	output := WaitForTasksResult{
		Completed: allTerminal,
		Results:   resultList,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	return string(data), nil
}

func (t *WaitForTasksTool) validateBrokeredCaller(ctx context.Context, toolCtx *ToolContext, namespace string) (*corev1alpha1.Task, error) {
	if toolCtx == nil || !toolCtx.Brokered {
		return nil, nil
	}
	if t == nil || t.k8sClient == nil {
		return nil, fmt.Errorf("brokered wait requires a Kubernetes client")
	}
	parentName := strings.TrimSpace(toolCtx.TaskID)
	parentUID := strings.TrimSpace(toolCtx.TaskUID)
	if parentName == "" || parentUID == "" || namespace != strings.TrimSpace(toolCtx.Namespace) {
		return nil, fmt.Errorf("brokered wait requires authenticated task identity")
	}
	parent := &corev1alpha1.Task{}
	if err := t.k8sClient.Get(ctx, types.NamespacedName{Name: parentName, Namespace: namespace}, parent); err != nil {
		return nil, fmt.Errorf("load authenticated parent task: %w", err)
	}
	if string(parent.UID) != parentUID {
		return nil, fmt.Errorf("authenticated parent task identity no longer matches the current Task")
	}
	return parent, nil
}

func validateBrokeredWaitTarget(ctx context.Context, toolCtx *ToolContext, parent, task *corev1alpha1.Task) error {
	if toolCtx == nil || !toolCtx.Brokered {
		return nil
	}
	if parent == nil || task == nil || task.Namespace != parent.Namespace ||
		parent.Namespace != strings.TrimSpace(toolCtx.Namespace) {
		return fmt.Errorf("task is not an authorized child of the authenticated parent task")
	}
	owner := metav1.GetControllerOf(task)
	if toolCtx.TaskProvenanceProtected &&
		strings.TrimSpace(task.Annotations[labels.AnnotationParentTaskUID]) == string(parent.UID) &&
		owner != nil && owner.APIVersion == corev1alpha1.GroupVersion.String() &&
		owner.Kind == taskKindString && owner.Name == parent.Name && owner.UID == parent.UID {
		return nil
	}
	if authorized, err := brokeredDelegationReceiptAuthorizes(ctx, toolCtx, parent, task); err != nil {
		return err
	} else if authorized {
		return nil
	}
	if task.Name != RepositoryValidationTaskName(parent) {
		return fmt.Errorf("task is not an authorized child of the authenticated parent task")
	}

	binding, err := FindRepositoryValidationCommandBinding(ctx, toolCtx.RepositoryValidationBindings, task.Namespace, task.Name)
	if err != nil {
		return fmt.Errorf("verify durable repository validation child binding: %w", err)
	}
	if binding == nil || binding.MonitorNamespace != task.Namespace ||
		binding.ReviewTaskName != parent.Name || binding.ReviewTaskUID != string(parent.UID) ||
		binding.ValidationTaskName != task.Name {
		return fmt.Errorf("task is not an authorized child of the authenticated parent task")
	}
	return nil
}

func brokeredDelegationReceiptAuthorizes(
	ctx context.Context,
	toolCtx *ToolContext,
	parent, task *corev1alpha1.Task,
) (bool, error) {
	if toolCtx == nil || toolCtx.ExternalEffects == nil || parent == nil || task == nil {
		return false, nil
	}
	effectID := strings.TrimSpace(task.Annotations[labels.AnnotationDelegationEffectID])
	if effectID == "" {
		return false, nil
	}
	effect, err := toolCtx.ExternalEffects.GetExternalEffect(ctx, effectID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("verify durable delegated child receipt: %w", err)
	}
	if effect.ID != effectID || effect.State != store.ExternalEffectSucceeded ||
		effect.Identity.Kind != "acp-mcp-tool" || effect.Identity.Namespace != parent.Namespace {
		return false, nil
	}
	var receipt DelegateTaskResult
	if len(effect.Response) == 0 || json.Unmarshal(effect.Response, &receipt) != nil {
		return false, nil
	}
	return receipt.TaskName == task.Name && receipt.TaskUID == string(task.UID) &&
		receipt.ParentTaskUID == string(parent.UID), nil
}

func fetchTaskResultForNamespace(ctx context.Context, namespace, taskName string) (string, error) {
	if toolCtx := GetToolContext(ctx); toolCtx != nil && toolCtx.ResultStore != nil {
		result, err := toolCtx.ResultStore.GetResult(ctx, namespace, taskName)
		if err != nil {
			return "", err
		}
		return string(result), nil
	}
	return fetchTaskResult(ctx, taskName)
}

func boundWaitTaskData(data map[string]any) map[string]any {
	if len(data) == 0 {
		return nil
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return map[string]any{"error": "data payload could not be encoded"}
	}
	if len(encoded) <= maxWaitTaskDataBytes {
		return data
	}
	return map[string]any{
		"truncated":     true,
		"originalBytes": len(encoded),
		"message":       "structured data payload exceeded wait_for_tasks inline limit; use artifact references for large outputs",
	}
}

func redactBrokeredWaitTaskResult(result *TaskResultInfo) {
	if result == nil {
		return
	}
	result.Result = redact.SensitiveText(result.Result)
	result.Summary = redact.SensitiveText(result.Summary)
	result.Verdict = redact.SensitiveText(result.Verdict)
	result.Feedback = redact.SensitiveText(result.Feedback)
	for i := range result.Files {
		result.Files[i] = redact.SensitiveText(result.Files[i])
	}
	result.Data = redactBrokeredWaitTaskData(result.Data)
	for i := range result.Artifacts {
		result.Artifacts[i].Filename = redact.SensitiveText(result.Artifacts[i].Filename)
		result.Artifacts[i].ContentType = redact.SensitiveText(result.Artifacts[i].ContentType)
		result.Artifacts[i].Description = redact.SensitiveText(result.Artifacts[i].Description)
	}
	result.BaseSHA = redact.SensitiveText(result.BaseSHA)
	result.HeadSHA = redact.SensitiveText(result.HeadSHA)
	result.PushBranch = redact.SensitiveText(result.PushBranch)
	result.WorkspaceRef = redact.SensitiveText(result.WorkspaceRef)
	result.WorkspaceBranch = redact.SensitiveText(result.WorkspaceBranch)
	result.Iteration = redact.SensitiveText(result.Iteration)
	result.RetryTaskName = redact.SensitiveText(result.RetryTaskName)
	if result.FailureDetails != nil {
		failure := *result.FailureDetails
		failure.Message = redact.SensitiveText(failure.Message)
		result.FailureDetails = &failure
	}
	if result.ExecutionOutcome != nil {
		outcome := result.ExecutionOutcome.DeepCopy()
		outcome.Message = redact.SensitiveText(outcome.Message)
		result.ExecutionOutcome = outcome
	}
	if result.WorkspaceStatus != nil {
		workspace := result.WorkspaceStatus.DeepCopy()
		workspace.Message = redact.SensitiveText(workspace.Message)
		for i := range workspace.Conditions {
			workspace.Conditions[i].Message = redact.SensitiveText(workspace.Conditions[i].Message)
		}
		result.WorkspaceStatus = workspace
	}
}

func redactBrokeredWaitTaskData(data map[string]any) map[string]any {
	if len(data) == 0 {
		return data
	}
	redacted := make(map[string]any, len(data))
	for key, value := range data {
		redacted[redact.SensitiveText(key)] = redactBrokeredWaitTaskValue(key, value)
	}
	return redacted
}

func redactBrokeredWaitTaskValue(key string, value any) any {
	if brokeredWaitTaskValueIsSensitive(key, value) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case string:
		return redact.SensitiveText(typed)
	case map[string]any:
		return redactBrokeredWaitTaskData(typed)
	case []any:
		redacted := make([]any, len(typed))
		for i := range typed {
			redacted[i] = redactBrokeredWaitTaskValue("", typed[i])
		}
		return redacted
	default:
		return value
	}
}

func brokeredWaitTaskValueIsSensitive(key string, value any) bool {
	if strings.TrimSpace(key) == "" {
		return false
	}
	probe := fmt.Sprintf("%s=%v", key, value)
	return redact.SensitiveText(probe) != probe
}

// Ensure WaitForTasksTool implements Tool
var _ Tool = (*WaitForTasksTool)(nil)

func truncateWaitTaskSummary(summary string) string {
	if len(summary) <= maxWaitTaskSummaryChars {
		return summary
	}
	return summary[:maxWaitTaskSummaryChars] + fmt.Sprintf(
		"\n[summary truncated, full summary: %d chars]",
		len(summary),
	)
}

// getRetryInfo extracts retry count and max retries from task annotations.
func getRetryInfo(task *corev1alpha1.Task) (retryCount, maxRetries int) {
	if countStr, ok := task.Annotations[labels.AnnotationRetryCount]; ok {
		retryCount, _ = strconv.Atoi(countStr)
	}
	maxRetries = 2 // default
	if maxStr, ok := task.Annotations[labels.AnnotationMaxRetries]; ok {
		maxRetries, _ = strconv.Atoi(maxStr)
	}
	return
}

const saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// fetchTaskResult fetches a task result from the controller via HTTP GET.
func fetchTaskResult(ctx context.Context, taskName string) (string, error) {
	controllerURL := os.Getenv(envOrkaControllerURL)
	if controllerURL == "" {
		return "", fmt.Errorf("%s is not set", envOrkaControllerURL)
	}

	controllerURL = strings.TrimRight(controllerURL, "/")
	url := fmt.Sprintf("%s/api/v1/tasks/%s/result", controllerURL, taskName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add SA token for auth
	if token, readErr := os.ReadFile(saTokenPath); readErr == nil {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10MB limit
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// The public endpoint returns JSON: {"result": "..."}
	var result struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("failed to parse result JSON: %w", err)
	}

	return result.Result, nil
}
