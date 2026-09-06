/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	distributionref "github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	// RunValidationToolName is the brokered tool exposed only to controller-owned
	// repository monitor review Tasks with a configured validation image.
	RunValidationToolName = "run_validation"
	// RepositoryValidationTimeout is the fixed execution timeout for repository
	// validation Tasks.
	RepositoryValidationTimeout = 45 * time.Minute
	// RepositoryValidationWaitTimeout leaves scheduling and reconciliation slack
	// beyond the validation Task's execution deadline.
	RepositoryValidationWaitTimeout = time.Hour

	repositoryValidationPurpose         = "repository-validation"
	repositoryValidationCreatedBy       = "repository-monitor"
	repositoryValidationMaxImage        = 2048
	repositoryValidationTaskPlaceholder = "exit 125"
	runValidationCommandField           = "command"
	runValidationTaskField              = "task"
)

var repositoryValidationImagePattern = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

// ValidRepositoryValidationImage reports whether image satisfies the public
// RepositoryMonitor validation-image contract.
func ValidRepositoryValidationImage(image string) bool {
	if len(image) > repositoryValidationMaxImage || !repositoryValidationImagePattern.MatchString(image) {
		return false
	}
	named, err := distributionref.ParseNormalizedNamed(image)
	if err != nil {
		return false
	}
	_, ok := named.(distributionref.Canonical)
	return ok
}

// RunValidationTool creates one tightly scoped validation Task for a
// controller-owned repository monitor review. The caller chooses the shell
// command, while the controller fixes the image and exact source checkout.
type RunValidationTool struct {
	k8sClient client.Client
}

func NewRunValidationTool(k8sClient client.Client) *RunValidationTool {
	return &RunValidationTool{k8sClient: k8sClient}
}

func (t *RunValidationTool) Name() string { return RunValidationToolName }

func (t *RunValidationTool) Description() string {
	return fmt.Sprintf("Run one offline repository validation command in the configured image against this exact pull request head. The checkout is mounted read-only and the command has no network access. Call wait_for_tasks with the returned child task name and timeout %q before reporting the review result.", RepositoryValidationWaitTimeout.String())
}

func (t *RunValidationTool) Parameters() json.RawMessage {
	return mustMarshalSchema(map[string]any{
		jsonSchemaTypeField: jsonSchemaTypeObject,
		jsonSchemaPropertiesField: map[string]any{
			runValidationCommandField: map[string]any{
				jsonSchemaTypeField:        jsonSchemaTypeString,
				jsonSchemaDescriptionField: "Offline shell command selected from the checked-out repository, for example 'go test ./...' or 'terraform validate'. The workspace is read-only and the image must already contain all tools and dependencies. Combine related checks in one command when needed.",
				"minLength":                1,
				"maxLength":                workerenv.RepositoryValidationMaxCommandBytes,
			},
		},
		jsonSchemaRequiredField: []string{runValidationCommandField},
	})
}

func (t *RunValidationTool) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	if t == nil || t.k8sClient == nil {
		return ChatToolErrorResult(internalErrorType, "repository validation is not configured", "")
	}
	toolCtx := GetToolContext(ctx)
	if toolCtx == nil || !toolCtx.Brokered || strings.TrimSpace(toolCtx.Namespace) == "" || strings.TrimSpace(toolCtx.TaskID) == "" || strings.TrimSpace(toolCtx.TaskUID) == "" {
		return ChatToolErrorResult(internalErrorType, "authenticated repository review context is unavailable", "")
	}
	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return ChatToolErrorResult("invalid_arguments", fmt.Sprintf("failed to parse arguments: %v", err), "Provide one shell command string")
	}
	command := strings.TrimSpace(args.Command)
	if command == "" {
		return ChatToolErrorResult("invalid_arguments", "command is required", "Choose the smallest relevant validation command from the repository")
	}
	if len(command) > workerenv.RepositoryValidationMaxCommandBytes || !utf8.ValidString(command) || strings.IndexByte(command, 0) >= 0 {
		return ChatToolErrorResult("invalid_arguments", fmt.Sprintf("command must be valid UTF-8 without NUL bytes and no longer than %d bytes", workerenv.RepositoryValidationMaxCommandBytes), "Use a shorter validation command")
	}
	if redact.SensitiveText(command) != command {
		return ChatToolErrorResult("invalid_arguments", "command must not contain credential-like values", "Use repository files and environment-independent validation commands without credentials")
	}

	parent := &corev1alpha1.Task{}
	parentKey := types.NamespacedName{Namespace: toolCtx.Namespace, Name: toolCtx.TaskID}
	if err := t.k8sClient.Get(ctx, parentKey, parent); err != nil {
		return classifyChatK8sErr(err)
	}
	monitor, image, headSHA, err := t.validateParent(ctx, toolCtx, parent)
	if err != nil {
		return ChatToolErrorResult("validation_not_authorized", err.Error(), "Run validation only from the RepositoryMonitor review task that exposed this tool")
	}

	validationTask := buildRepositoryValidationTask(parent, monitor, image, headSHA)
	validationTask.Annotations[labels.AnnotationRepositoryValidationCommandDigest] = RepositoryValidationCommandDigest(command)
	if err := validateChildTaskAgainstParentTransaction(ctx, t.k8sClient, parent, validationTask, ""); err != nil {
		return ChatToolErrorResult("validation_not_authorized", err.Error(), "The validation task must remain within the parent task transaction policy")
	}
	bindingEvent, err := RepositoryValidationCommandBindingEvent(parent, monitor, validationTask, image, headSHA, command)
	if err != nil {
		return ChatToolErrorResult(internalErrorType, "repository validation command binding is invalid", "Report validation as unavailable")
	}
	if err := ensureRepositoryValidationCommandBinding(ctx, toolCtx.RepositoryValidationBindings, bindingEvent); err != nil {
		if errors.Is(err, errRepositoryValidationBindingConflict) {
			return ChatToolErrorResult("validation_task_conflict", "the validation command is already bound to a different request", "Do not retry with a different command; report validation as unavailable")
		}
		return ChatToolErrorResult(internalErrorType, "failed to persist the repository validation command binding", "Report validation as unavailable")
	}
	commandSecret := buildRepositoryValidationCommandSecret(parent, validationTask, command)
	if err := ensureRepositoryValidationCommandSecret(ctx, t.k8sClient, commandSecret); err != nil {
		if errors.Is(err, errRepositoryValidationBindingConflict) {
			return ChatToolErrorResult("validation_task_conflict", "the validation command Secret does not match this review", "Do not retry with a different command; report validation as unavailable")
		}
		return ChatToolErrorResult(internalErrorType, "failed to persist the repository validation command Secret", "Report validation as unavailable")
	}
	if err := t.k8sClient.Create(ctx, validationTask); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return classifyChatK8sErr(err)
		}
		existing := &corev1alpha1.Task{}
		if getErr := t.k8sClient.Get(ctx, types.NamespacedName{Namespace: validationTask.Namespace, Name: validationTask.Name}, existing); getErr != nil {
			return classifyChatK8sErr(getErr)
		}
		if !repositoryValidationTaskMatches(existing, validationTask) {
			return ChatToolErrorResult("validation_task_conflict", "the existing validation task does not match this review, image, checkout, and command", "Do not retry with a different command; report validation as unavailable")
		}
		validationTask = existing
	}

	return ChatToolSuccess(map[string]any{
		runValidationTaskField: validationTask.Name,
		"phase":                validationTask.Status.Phase,
		"image":                image,
		"headSHA":              headSHA,
	})
}

func (t *RunValidationTool) validateParent(ctx context.Context, toolCtx *ToolContext, parent *corev1alpha1.Task) (*corev1alpha1.RepositoryMonitor, string, string, error) {
	if parent == nil || string(parent.UID) != toolCtx.TaskUID || parent.Namespace != toolCtx.Namespace {
		return nil, "", "", fmt.Errorf("authenticated review task identity does not match the current Task")
	}
	if parent.Spec.Type != corev1alpha1.TaskTypeAgent || parent.Annotations[labels.AnnotationAgentReadOnly] != trueStr || parent.Labels[labels.LabelCreatedBy] != repositoryValidationCreatedBy {
		return nil, "", "", fmt.Errorf("task is not a read-only repository monitor review")
	}
	if parent.Spec.AgentRuntime == nil || !slices.Contains(parent.Spec.AgentRuntime.AllowedTools, RunValidationToolName) {
		return nil, "", "", fmt.Errorf("task was not authorized to run repository validation")
	}
	monitorName := strings.TrimSpace(parent.Annotations[labels.AnnotationRepositoryMonitorName])
	if monitorName == "" {
		return nil, "", "", fmt.Errorf("repository monitor identity is missing")
	}
	monitor := &corev1alpha1.RepositoryMonitor{}
	if err := t.k8sClient.Get(ctx, types.NamespacedName{Namespace: parent.Namespace, Name: monitorName}, monitor); err != nil {
		return nil, "", "", fmt.Errorf("load repository monitor: %w", err)
	}
	if !metav1.IsControlledBy(parent, monitor) {
		return nil, "", "", fmt.Errorf("review task is not controlled by repository monitor %s/%s", monitor.Namespace, monitor.Name)
	}
	image := strings.TrimSpace(parent.Annotations[labels.AnnotationRepositoryValidationImage])
	if image == "" || image != strings.TrimSpace(monitor.Spec.Validation.Image) {
		return nil, "", "", fmt.Errorf("review task validation image does not match the repository monitor")
	}
	if !ValidRepositoryValidationImage(image) {
		return nil, "", "", fmt.Errorf("repository validation image must be digest-pinned")
	}
	headSHA := strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorHeadSHA])
	workspace := parent.Spec.Workspace
	if headSHA == "" || workspace == nil || workspace.Intent != corev1alpha1.WorkspaceIntentRead || strings.TrimSpace(workspace.GitRepo) == "" || strings.TrimSpace(workspace.Ref) != headSHA {
		return nil, "", "", fmt.Errorf("review task is not bound to an exact read-only repository head")
	}
	if strings.TrimSpace(workspace.PublicationGitRepo) != "" || workspace.PublicationReadCredentialRef != nil || workspace.PublicationCredentialRef != nil || workspace.ForgeCredentialRef != nil || strings.TrimSpace(workspace.PushBranch) != "" || workspace.CreatePR {
		return nil, "", "", fmt.Errorf("review task workspace contains publication capabilities")
	}
	if err := ValidateRepositoryValidationReviewBinding(ctx, toolCtx.RepositoryValidationBindings, parent, monitor); err != nil {
		return nil, "", "", fmt.Errorf("review task workspace does not match the controller binding")
	}
	return monitor, image, headSHA, nil
}

func buildRepositoryValidationTask(parent *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor, image, headSHA string) *corev1alpha1.Task {
	workspace := parent.Spec.Workspace.DeepCopy()
	workspace.Intent = corev1alpha1.WorkspaceIntentRead
	workspace.Branch = ""
	workspace.Ref = headSHA
	workspace.PublicationGitRepo = ""
	workspace.PublicationReadCredentialRef = nil
	workspace.PublicationCredentialRef = nil
	workspace.ForgeCredentialRef = nil
	workspace.PRBaseBranch = ""
	workspace.PushBranch = ""
	workspace.ExpectedRemoteSHA = ""
	workspace.CreatePR = false
	workspace.MaxChangedFiles = nil
	workspace.AllowedPaths = nil
	workspace.DenyRepositoryControlPaths = false
	workspace.RejectBinaryFiles = false
	workspace.RejectSecretLikeContent = false

	timeout := metav1.Duration{Duration: RepositoryValidationTimeout}
	annotations := map[string]string{
		labels.AnnotationParentTaskName:            parent.Name,
		labels.AnnotationParentTaskUID:             string(parent.UID),
		labels.AnnotationRepositoryMonitorName:     monitor.Name,
		labels.AnnotationMonitorRunID:              parent.Annotations[labels.AnnotationMonitorRunID],
		labels.AnnotationMonitorItemKind:           parent.Annotations[labels.AnnotationMonitorItemKind],
		labels.AnnotationMonitorItemNumber:         parent.Annotations[labels.AnnotationMonitorItemNumber],
		labels.AnnotationMonitorHeadSHA:            headSHA,
		labels.AnnotationGitHubRepository:          parent.Annotations[labels.AnnotationGitHubRepository],
		labels.AnnotationRepositoryValidationImage: image,
		labels.AnnotationWorkspaceInitContainer:    trueStr,
	}
	validationTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RepositoryValidationTaskName(parent),
			Namespace: parent.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:           trueStr,
				labels.LabelCreatedBy:         repositoryValidationCreatedBy,
				labels.LabelPurpose:           repositoryValidationPurpose,
				labels.LabelParentTask:        labels.SelectorValue(parent.Name),
				labels.LabelRepositoryMonitor: labels.SelectorValue(monitor.Name),
				labels.LabelMonitorRun:        parent.Labels[labels.LabelMonitorRun],
				labels.LabelGitHubRepository:  parent.Labels[labels.LabelGitHubRepository],
				labels.LabelGitHubTarget:      parent.Labels[labels.LabelGitHubTarget],
				labels.LabelGitHubNumber:      parent.Labels[labels.LabelGitHubNumber],
			},
			Annotations: annotations,
			// The exact review incarnation owns its UID-versioned validation Task.
			// Replacing or deleting the review therefore garbage-collects stale
			// validation work instead of leaving it under the long-lived monitor.
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(parent, corev1alpha1.GroupVersion.WithKind("Task")),
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeContainer,
			Image:     image,
			Command:   []string{"/bin/sh", "-c"},
			Args:      []string{repositoryValidationTaskPlaceholder},
			Timeout:   &timeout,
			Workspace: workspace,
		},
	}
	if parent.Spec.Priority != nil {
		priority := *parent.Spec.Priority
		validationTask.Spec.Priority = &priority
	}
	inheritTaskProvenance(validationTask, parent)
	return validationTask
}

func buildRepositoryValidationCommandSecret(parent, validationTask *corev1alpha1.Task, command string) *corev1.Secret {
	immutable := true
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RepositoryValidationCommandSecretName(validationTask.Name),
			Namespace: validationTask.Namespace,
			Labels: map[string]string{
				labels.LabelManaged:    trueStr,
				labels.LabelCreatedBy:  repositoryValidationCreatedBy,
				labels.LabelPurpose:    repositoryValidationPurpose,
				labels.LabelParentTask: labels.SelectorValue(parent.Name),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName:            parent.Name,
				labels.AnnotationParentTaskUID:             string(parent.UID),
				labels.AnnotationRepositoryMonitorName:     validationTask.Annotations[labels.AnnotationRepositoryMonitorName],
				labels.AnnotationMonitorHeadSHA:            validationTask.Annotations[labels.AnnotationMonitorHeadSHA],
				labels.AnnotationRepositoryValidationImage: validationTask.Annotations[labels.AnnotationRepositoryValidationImage],
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(parent, corev1alpha1.GroupVersion.WithKind("Task")),
			},
		},
		Immutable: &immutable,
		Type:      corev1.SecretTypeOpaque,
		Data:      map[string][]byte{RepositoryValidationCommandSecretKey: []byte(command)},
	}
}

func ensureRepositoryValidationCommandSecret(ctx context.Context, k8sClient client.Client, expected *corev1.Secret) error {
	if k8sClient == nil || expected == nil {
		return fmt.Errorf("repository validation command Secret client is unavailable")
	}
	if err := k8sClient.Create(ctx, expected); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create repository validation command Secret: %w", err)
	}
	existing := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(expected), existing); err != nil {
		return fmt.Errorf("load repository validation command Secret: %w", err)
	}
	if existing.Type != expected.Type || existing.Immutable == nil || !*existing.Immutable ||
		!reflect.DeepEqual(existing.Labels, expected.Labels) ||
		!reflect.DeepEqual(existing.Annotations, expected.Annotations) ||
		!reflect.DeepEqual(existing.OwnerReferences, expected.OwnerReferences) ||
		!reflect.DeepEqual(existing.Data, expected.Data) {
		return errRepositoryValidationBindingConflict
	}
	return nil
}

// RepositoryValidationTaskName returns the deterministic child Task name for
// one exact review Task incarnation. Including the UID prevents a recreated
// review with the same Kubernetes name from adopting an earlier validation.
func RepositoryValidationTaskName(parent *corev1alpha1.Task) string {
	if parent == nil {
		return ""
	}
	parentName := strings.Trim(strings.TrimSpace(parent.Name), "-")
	parentUID := strings.TrimSpace(string(parent.UID))
	if parentName == "" || parentUID == "" {
		return ""
	}
	const suffix = "-validation"
	digest := sha256.Sum256([]byte(parentName + "\x00" + parentUID))
	hash := hex.EncodeToString(digest[:])[:12]
	prefixLength := 63 - len(hash) - len(suffix) - 1
	prefix := strings.Trim(parentName[:min(len(parentName), prefixLength)], "-")
	if prefix == "" {
		prefix = "review"
	}
	return prefix + "-" + hash + suffix
}

func repositoryValidationTaskMatches(existing, expected *corev1alpha1.Task) bool {
	if existing == nil || expected == nil || existing.Name != expected.Name || existing.Namespace != expected.Namespace {
		return false
	}
	if !reflect.DeepEqual(existing.Labels, expected.Labels) || !reflect.DeepEqual(existing.Annotations, expected.Annotations) || !reflect.DeepEqual(existing.OwnerReferences, expected.OwnerReferences) {
		return false
	}
	return repositoryValidationTaskSpecsEqual(existing.Spec, expected.Spec)
}

// RepositoryValidationTaskSpecMatches reports whether a validation Task has
// the exact spec the run_validation tool would create for the review.
func RepositoryValidationTaskSpecMatches(parent *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor, validationTask *corev1alpha1.Task, image, headSHA string) bool {
	if parent == nil || monitor == nil || validationTask == nil || parent.Spec.Workspace == nil {
		return false
	}
	expected := buildRepositoryValidationTask(parent, monitor, image, headSHA)
	return repositoryValidationTaskSpecsEqual(validationTask.Spec, expected.Spec)
}

func repositoryValidationTaskSpecsEqual(existingSpec, expectedSpec corev1alpha1.TaskSpec) bool {
	existingSpec.ConcurrencyPolicy, expectedSpec.ConcurrencyPolicy = "", ""
	existingSpec.StartingDeadlineSeconds, expectedSpec.StartingDeadlineSeconds = nil, nil
	existingSpec.SuccessfulRunsHistoryLimit, expectedSpec.SuccessfulRunsHistoryLimit = nil, nil
	existingSpec.FailedRunsHistoryLimit, expectedSpec.FailedRunsHistoryLimit = nil, nil
	return reflect.DeepEqual(existingSpec, expectedSpec)
}

var _ Tool = (*RunValidationTool)(nil)
