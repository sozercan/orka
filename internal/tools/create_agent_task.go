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
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tracing"
)

// CreateAgentTaskTool creates an agent-runtime Task CR.
type CreateAgentTaskTool struct{}

func (t *CreateAgentTaskTool) Name() string { return createAgentTaskToolName }

func (t *CreateAgentTaskTool) Description() string {
	return "Create a task using an external CLI runtime (Copilot, Claude Code, Codex, OpenCode) for code changes in a git repo. Do NOT use for simple container commands or direct LLM reasoning."
}

func (t *CreateAgentTaskTool) Parameters() json.RawMessage {
	return mustMarshalSchema(map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaPropertiesField: map[string]any{nameField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: taskNameDescription}, promptField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "The prompt/instruction for the agent"}, agentRefField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Agent name with runtime configured"}, namespaceField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: namespaceDescription}, timeoutField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: timeoutDescription}, "maxTurns": map[string]any{jsonSchemaTypeField: jsonSchemaTypeInteger, jsonSchemaMinimumField: minMaxTurns, jsonSchemaMaximumField: maxMaxTurns, jsonSchemaDescriptionField: "Maximum agent loop iterations"}, workspaceField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeObject, jsonSchemaPropertiesField: map[string]any{
		"intent":                       map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaEnumField: []string{"read", "write"}, jsonSchemaDescriptionField: "Workspace intent. Defaults to read; publication fields require write. Write intent requires gitRepo and publicationCredentialRef."},
		"gitRepo":                      map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Source Git repository URL as a credential-free HTTPS URL, e.g. https://github.com/owner/repo. GitHub SSH roots (git@github.com:owner/repo) are converted automatically; other schemes and embedded credentials are rejected. Required for write intent."},
		"branch":                       map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Source branch to clone from (must exist), as a short branch name or refs/heads/... ref. Omit with ref to resolve and freeze the repository's advertised default branch. Requires gitRepo."},
		"ref":                          map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Exact source selector: a full commit SHA, refs/heads/... branch, refs/tags/... tag, or short ref name. Other refs/ namespaces (e.g. refs/remotes/...) are rejected. Requires gitRepo."},
		"readCredentialRef":            map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional Secret name for clone/read credentials. Omit to auto-discover a read credential when available. Requires gitRepo."},
		"publicationGitRepo":           map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Publication repository URL for write Tasks as a credential-free HTTPS URL, e.g. https://github.com/owner/repo. GitHub SSH roots (git@github.com:owner/repo) are converted automatically; other schemes and embedded credentials are rejected."},
		"publicationReadCredentialRef": map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional Secret name for target-repository preflight and verification credentials. Write intent only."},
		"publicationCredentialRef":     map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Secret name for target-repository write credentials. Required for write intent; write intent only."},
		"forgeCredentialRef":           map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Optional Secret name for forge API credentials used to reconcile pull requests. Required when createPR is true; write intent only."},
		"pushBranch":                   map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Publication branch name (write intent)"},
		"prBaseBranch":                 map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Pull request base branch. Required when createPR is true."},
		"createPR":                     map[string]any{jsonSchemaTypeField: jsonSchemaTypeBoolean, jsonSchemaDescriptionField: "Reconcile a pull request after publication. Requires prBaseBranch and forgeCredentialRef."},
		"subPath":                      map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: "Sub-path within the repo as a relative slash-separated path (no leading /, no . or .. segments, max 1024 bytes). Requires gitRepo."},
	},
	}, scheduleField: map[string]any{jsonSchemaTypeField: jsonSchemaTypeString, jsonSchemaDescriptionField: cronScheduleDescription},
	}, jsonSchemaRequiredField: []string{nameField, promptField, agentRefField},
	})
}

func (t *CreateAgentTaskTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	tc := GetToolContext(ctx)
	if tc == nil {
		return ChatToolErrorResult(internalErrorType, "missing tool context", "")
	}

	var a map[string]any
	if err := json.Unmarshal(args, &a); err != nil {
		return ChatToolErrorResult("invalid_arguments", fmt.Sprintf("failed to parse arguments: %v", err), "Ensure arguments are valid JSON")
	}

	if limitErr := tc.CheckTaskLimit(); limitErr != nil {
		return ChatToolErrorResult(limitErr.Type, limitErr.Message, limitErr.Suggestion)
	}

	prompt := chatGetStringArg(a, promptField)
	if prompt == "" {
		return ChatToolErrorResult("invalid_arguments", "prompt is required", "Provide a prompt for the agent task")
	}

	agentRef := chatGetStringArg(a, agentRefField)
	if agentRef == "" {
		return ChatToolErrorResult("invalid_arguments", "agentRef is required", "Provide an agent reference for the agent task")
	}

	namespace := chatGetStringArgDefault(a, namespaceField, tc.Namespace)
	if r, ok := checkChatNamespaceScope(tc, namespace); !ok {
		return r, nil
	}
	policyReader := toolPolicyReader(ctx, tc.Client)
	agent, err := loadAgent(ctx, policyReader, namespace, agentRef)
	if err != nil {
		result, _ := ChatToolErrorResult(internalErrorType, err.Error(), "")
		return result, nil
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tc.GenerateTaskName(),
			Namespace: namespace,
			Labels:    tc.TaskLabels(),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAgent,
			Prompt: prompt,
			AgentRef: &corev1alpha1.AgentReference{
				Name: agentRef,
			},
		},
	}

	if d, errResult, ok := parseTimeoutArg(a); !ok {
		return errResult, nil
	} else if d > 0 {
		task.Spec.Timeout = &metav1.Duration{Duration: d}
	}

	var agentRuntime *corev1alpha1.AgentRuntimeSpec

	if turns, errResult, ok := parseMaxTurnsArg(a); !ok {
		return errResult, nil
	} else if turns != nil {
		agentRuntime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: turns}
	}

	if ws, ok := a[workspaceField]; ok {
		wsMap, ok := ws.(map[string]any)
		if !ok {
			return ChatToolErrorResult("invalid_arguments", "workspace must be an object", "Provide workspace as a JSON object or omit it")
		}
		wsCfg := &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentRead}
		if intent := strings.ToLower(strings.TrimSpace(chatGetStringArg(wsMap, "intent"))); intent != "" {
			switch corev1alpha1.WorkspaceIntent(intent) {
			case corev1alpha1.WorkspaceIntentRead, corev1alpha1.WorkspaceIntentWrite:
				wsCfg.Intent = corev1alpha1.WorkspaceIntent(intent)
			default:
				return ChatToolErrorResult("invalid_arguments", "workspace.intent must be read or write", "Use read for inspection or write for publication")
			}
		}
		gitRepo, repoErr := canonicalAgentWorkspaceRepositoryArg("gitRepo", chatGetStringArg(wsMap, "gitRepo"))
		if repoErr != nil {
			return ChatToolErrorResult(repoErr.Type, repoErr.Message, repoErr.Suggestion)
		}
		wsCfg.GitRepo = gitRepo
		wsCfg.Branch = chatGetStringArg(wsMap, "branch")
		wsCfg.Ref = chatGetStringArg(wsMap, "ref")
		wsCfg.SubPath = chatGetStringArg(wsMap, "subPath")
		publicationGitRepo, repoErr := canonicalAgentWorkspaceRepositoryArg("publicationGitRepo", chatGetStringArg(wsMap, "publicationGitRepo"))
		if repoErr != nil {
			return ChatToolErrorResult(repoErr.Type, repoErr.Message, repoErr.Suggestion)
		}
		wsCfg.PublicationGitRepo = publicationGitRepo
		wsCfg.PushBranch = chatGetStringArg(wsMap, "pushBranch")
		wsCfg.PRBaseBranch = chatGetStringArg(wsMap, "prBaseBranch")
		createPR, errResult, ok := parseCreatePRArg(wsMap)
		if !ok {
			return errResult, nil
		}
		wsCfg.CreatePR = createPR
		if workspaceRequestsPublication(wsCfg) {
			wsCfg.Intent = corev1alpha1.WorkspaceIntentWrite
		}
		readCredential := strings.TrimSpace(chatGetStringArg(wsMap, "readCredentialRef"))
		publicationReadCredential := strings.TrimSpace(chatGetStringArg(wsMap, "publicationReadCredentialRef"))
		publicationCredential := strings.TrimSpace(chatGetStringArg(wsMap, "publicationCredentialRef"))
		forgeCredential := strings.TrimSpace(chatGetStringArg(wsMap, "forgeCredentialRef"))
		if wsErr := agentWorkspaceArgError(wsCfg, readCredential, publicationReadCredential, publicationCredential, forgeCredential); wsErr != nil {
			return ChatToolErrorResult(wsErr.Type, wsErr.Message, wsErr.Suggestion)
		}
		// Only attach read credentials alongside a gitRepo: the controller
		// workspace preflight rejects readCredentialRef without gitRepo, so
		// auto-discovery must not doom a repository-free workspace.
		if strings.TrimSpace(wsCfg.GitRepo) != "" {
			readRef, err := resolveWorkspaceCredentialRef(ctx, tc.Client, namespace, agent, readCredential)
			if err != nil {
				result, _ := ChatToolErrorResult(internalErrorType, err.Error(), "")
				return result, nil
			}
			wsCfg.ReadCredentialRef = readRef
		}
		if publicationReadCredential != "" {
			wsCfg.PublicationReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: publicationReadCredential}
		}
		if publicationCredential != "" {
			wsCfg.PublicationCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: publicationCredential}
		}
		if forgeCredential != "" {
			wsCfg.ForgeCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: forgeCredential}
		}
		task.Spec.Workspace = wsCfg
	}

	task.Spec.AgentRuntime = agentRuntime
	if err := materializeRuntimeRefAllowedTools(ctx, policyReader, task, agent); err != nil {
		result, _ := ChatToolErrorResult(internalErrorType, err.Error(), "")
		return result, nil
	}

	schedule := chatGetStringArg(a, scheduleField)
	if schedule != "" {
		task.Spec.Schedule = schedule
	}

	if result, ok := authorizeTaskCreate(ctx, tc, task); !ok {
		return result, nil
	}
	tracing.StampTaskTraceContext(ctx, task)
	if err := tc.Client.Create(ctx, task); err != nil {
		return classifyChatK8sErr(err)
	}

	tc.IncrementTasks()
	return ChatToolSuccess(map[string]any{nameField: task.Name, namespaceField: task.Namespace, phaseField: taskPhasePendingString, messageField: taskCreatedMsg(schedule)})
}

// agentWorkspaceArgError runs the full workspace-argument preflight mirror:
// credential/publication prerequisites, canonical source selectors, and the
// harness-v2 subPath rule.
func agentWorkspaceArgError(wsCfg *corev1alpha1.WorkspaceConfig, readCredential, publicationReadCredential, publicationCredential, forgeCredential string) *ChatToolError {
	if wsErr := agentWorkspacePreflightError(wsCfg, readCredential, publicationReadCredential, publicationCredential, forgeCredential); wsErr != nil {
		return wsErr
	}
	if wsErr := agentWorkspaceSourceSelectorError(wsCfg); wsErr != nil {
		return wsErr
	}
	return agentWorkspaceSubPathError(wsCfg.SubPath)
}

// agentWorkspacePreflightError mirrors the tool-expressible subset of the
// controller's validateACPWorkspacePreflight rules so obviously doomed Tasks
// are rejected before consuming the session task budget. The controller-side
// preflight remains authoritative; keep the mirrored conditions in exact
// behavior parity with it (not stricter and not looser).
func agentWorkspacePreflightError(wsCfg *corev1alpha1.WorkspaceConfig, readCredential, publicationReadCredential, publicationCredential, forgeCredential string) *ChatToolError {
	invalidArgs := func(message, suggestion string) *ChatToolError {
		return &ChatToolError{Type: "invalid_arguments", Message: message, Suggestion: suggestion}
	}
	if strings.TrimSpace(wsCfg.GitRepo) == "" {
		switch {
		case strings.TrimSpace(wsCfg.Branch) != "":
			return invalidArgs("workspace.branch requires workspace.gitRepo", "Provide workspace.gitRepo or omit branch")
		case strings.TrimSpace(wsCfg.Ref) != "":
			return invalidArgs("workspace.ref requires workspace.gitRepo", "Provide workspace.gitRepo or omit ref")
		case strings.TrimSpace(wsCfg.SubPath) != "":
			return invalidArgs("workspace.subPath requires workspace.gitRepo", "Provide workspace.gitRepo or omit subPath")
		case readCredential != "":
			return invalidArgs("workspace.readCredentialRef requires workspace.gitRepo", "Provide workspace.gitRepo or omit readCredentialRef")
		}
	}
	if wsCfg.Intent != corev1alpha1.WorkspaceIntentWrite {
		switch {
		case publicationReadCredential != "":
			return invalidArgs("workspace.publicationReadCredentialRef requires write workspace intent", "Set workspace.intent to write or omit publicationReadCredentialRef")
		case publicationCredential != "":
			return invalidArgs("workspace.publicationCredentialRef requires write workspace intent", "Set workspace.intent to write or omit publicationCredentialRef")
		case forgeCredential != "":
			return invalidArgs("workspace.forgeCredentialRef requires write workspace intent", "Set workspace.intent to write or omit forgeCredentialRef")
		}
		return nil
	}
	if publicationCredential == "" {
		return invalidArgs("workspace.publicationCredentialRef is required for write intent", "Provide a dedicated target-repository write credential")
	}
	if strings.TrimSpace(wsCfg.GitRepo) == "" {
		return invalidArgs("workspace.gitRepo is required for write intent", "Provide the source repository URL for the publication workspace")
	}
	if pushBranch := strings.TrimSpace(wsCfg.PushBranch); pushBranch != "" {
		if err := validateWorkspaceBranchArg(pushBranch); err != nil {
			return invalidArgs(fmt.Sprintf("workspace.pushBranch is invalid: %v", err), "Provide a valid Git branch name")
		}
	}
	if wsCfg.CreatePR {
		if strings.TrimSpace(wsCfg.PRBaseBranch) == "" {
			return invalidArgs("workspace.createPR requires workspace.prBaseBranch", "Provide the pull request base branch")
		}
		if err := validateWorkspaceBranchArg(strings.TrimSpace(wsCfg.PRBaseBranch)); err != nil {
			return invalidArgs(fmt.Sprintf("workspace.prBaseBranch is invalid: %v", err), "Provide a valid pull request base branch name")
		}
		if forgeCredential == "" {
			return invalidArgs("workspace.createPR requires workspace.forgeCredentialRef", "Provide a forge API credential Secret for pull request reconciliation")
		}
	}
	return nil
}

// canonicalAgentWorkspaceRepositoryArg canonicalizes a workspace repository
// argument through security.CanonicalWorkspaceRepositoryCloneURL, the shared
// mirror of the controller's canonicalWorkspaceRepositoryURL rule, and wraps
// rejections into tool errors carrying the offending field name.
func canonicalAgentWorkspaceRepositoryArg(field, raw string) (string, *ChatToolError) {
	canonical, err := security.CanonicalWorkspaceRepositoryCloneURL(raw)
	if err != nil {
		return "", &ChatToolError{
			Type:    "invalid_arguments",
			Message: fmt.Sprintf("workspace.%s %s", field, err.Error()),
			Suggestion: "Use a credential-free HTTPS URL such as https://github.com/owner/repo" +
				" (GitHub SSH roots like git@github.com:owner/repo are converted automatically)",
		}
	}
	return canonical, nil
}

// agentWorkspaceSourceSelectorError mirrors the controller's
// runtimeWorkspaceSourceRef selector validation with the same canonical
// source-ref validator, so a Task with a malformed branch or ref selector is
// rejected before creation instead of failing preflight afterwards. Keep the
// mirrored conditions in exact behavior parity with the controller (not
// stricter and not looser).
func agentWorkspaceSourceSelectorError(wsCfg *corev1alpha1.WorkspaceConfig) *ChatToolError {
	if ref := strings.TrimSpace(wsCfg.Ref); ref != "" {
		if _, err := publisherservice.CanonicalWorkspaceSourceRef(ref); err != nil {
			return &ChatToolError{
				Type:       "invalid_arguments",
				Message:    fmt.Sprintf("workspace.ref is invalid: %v", err),
				Suggestion: "Use a full commit SHA, refs/heads/... branch, refs/tags/... tag, or short ref name",
			}
		}
	}
	if branch := strings.TrimSpace(wsCfg.Branch); branch != "" {
		candidate := branch
		if !strings.HasPrefix(candidate, "refs/") {
			candidate = "refs/heads/" + candidate
		}
		if _, err := publisherservice.CanonicalWorkspaceSourceRef(candidate); err != nil {
			return &ChatToolError{
				Type:       "invalid_arguments",
				Message:    fmt.Sprintf("workspace.branch is invalid: %v", err),
				Suggestion: "Use a valid Git branch name or refs/heads/... ref",
			}
		}
	}
	return nil
}

// agentWorkspaceSubPathError mirrors the harness-v2 workspace relative-root
// validation (validateWorkspaceRelativeRoot) so an unsafe subPath is rejected
// before the Task is created instead of failing RuntimeSession creation. Keep
// the mirrored conditions in exact behavior parity (not stricter and not
// looser).
func agentWorkspaceSubPathError(subPath string) *ChatToolError {
	root := strings.TrimSpace(subPath)
	if root == "" || root == "." {
		return nil
	}
	invalid := func(detail string) *ChatToolError {
		return &ChatToolError{
			Type:       "invalid_arguments",
			Message:    "workspace.subPath " + detail,
			Suggestion: "Use a relative slash-separated path inside the repository, such as services/api",
		}
	}
	if !utf8.ValidString(root) {
		return invalid("contains invalid UTF-8")
	}
	if len(root) > 1024 {
		return invalid("exceeds 1024 bytes")
	}
	if strings.HasPrefix(root, "/") || strings.Contains(root, `\`) {
		return invalid("must be a relative slash-separated path")
	}
	for segment := range strings.SplitSeq(root, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return invalid("contains an unsafe segment")
		}
	}
	return nil
}

// validateWorkspaceBranchArg mirrors the controller's canonicalWorkspaceBranchRef
// check with the same canonical branch-ref validator.
func validateWorkspaceBranchArg(branch string) error {
	ref := branch
	if !strings.HasPrefix(ref, "refs/heads/") {
		ref = "refs/heads/" + ref
	}
	return store.ValidateFullBranchRef(ref)
}
