/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import "github.com/orka-agents/orka/internal/workerenv"

const (
	envOrkaTaskName                  = workerenv.TaskName
	envOrkaTaskNamespace             = workerenv.TaskNamespace
	envOrkaParentTask                = workerenv.ParentTask
	envOrkaControllerURL             = workerenv.ControllerURL
	envOrkaCoordinationDepth         = workerenv.CoordinationDepth
	envOrkaCoordinationAllowedAgents = workerenv.CoordinationAllowedAgents
	envOrkaCoordinationMaxDepth      = workerenv.CoordinationMaxDepth

	noNewMessagesText               = "No new messages"
	agentNameDescription            = "Agent name"
	namespaceDescription            = "Namespace"
	taskNameDescription             = "Task name"
	cronScheduleDescription         = "Cron schedule for recurring tasks (e.g., '0 */6 * * *' for every 6 hours, '0 9 * * 1-5' for weekdays at 9am, '*/5 * * * *' for every 5 minutes). Leave empty for one-time tasks."
	workspaceTaskDescription        = "Name of a task whose workspace config has the repo and git credentials"
	childWorkspaceTaskDescription   = "Name of the child task whose workspace config has the repo and git credentials"
	timeoutDescription              = "Timeout duration, e.g. \"5m\""
	missingControllerTaskEnvMessage = workerenv.ControllerURL + ", " + workerenv.TaskName + ", and " + workerenv.TaskNamespace + " are required"
	taskPhasePendingString          = "Pending"
	taskPhaseErrorString            = "Error"
	reviewEventApprove              = "APPROVE"
	reviewEventRequestChanges       = "REQUEST_CHANGES"
	reviewEventComment              = "COMMENT"
	jsonSchemaTypeArray             = "array"
	jsonSchemaTypeBoolean           = "boolean"
	codeLanguageBash                = "bash"
	codeLanguageShell               = "sh"
	githubBodyField                 = "body"
	githubCommitTitleField          = "commit_title"
	githubCommitMessageField        = "commit_message"
	agentRefField                   = "agentRef"
	taskKindString                  = "Task"
	cancelledStatusString           = "cancelled"
	createAgentToolName             = "create_agent"
	checkMessagesToolName           = "check_messages"
	checkPullRequestCIToolName      = "check_pull_request_ci"
	checkPRReviewMarkerToolName     = "check_pr_review_marker"
	checkTaskProgressToolName       = "check_task_progress"
	cancelTaskToolName              = "cancel_task"
	autoMergePullRequestToolName    = "auto_merge_pull_request"
	pullRequestMergedMessage        = "Pull request merged successfully"
	httpMethodPostString            = "POST"
	defaultWorkspacePath            = "/workspace"
	tempDirPath                     = "/tmp"
	cUTF8Locale                     = "C.UTF-8"

	jsonSchemaDescriptionField  = "description"
	jsonSchemaEnumField         = "enum"
	enabledString               = "enabled"
	failedStatusString          = "failed"
	deletedStatusString         = "deleted"
	createAITaskToolName        = "create_ai_task"
	createPRMonitorToolName     = "create_pr_monitor"
	createAgentTaskToolName     = "create_agent_task"
	createContainerTaskToolName = "create_container_task"
	createPullRequestToolName   = "create_pull_request"
	listPullRequestsToolName    = "list_pull_requests"
	createToolCRDToolName       = "create_tool"
	deleteToolToolName          = "delete_tool"
	deleteSessionToolName       = "delete_session"
	delegateTaskToolName        = "delegate_task"
	githubReviewEventField      = "event"
)

const (
	jsonSchemaTypeField       = "type"
	jsonSchemaTypeString      = "string"
	jsonSchemaTypeObject      = "object"
	jsonSchemaTypeInteger     = "integer"
	jsonSchemaPropertiesField = "properties"
	jsonSchemaRequiredField   = "required"
	jsonSchemaDefaultField    = "default"
	jsonSchemaMinimumField    = "minimum"
	jsonSchemaMaximumField    = "maximum"
	nameField                 = "name"
	namespaceField            = "namespace"
	taskNameField             = "task_name"
	tokenKey                  = "token"
	messageField              = "message"
	timeoutField              = "timeout"
	githubPRNumberField       = "pr_number"
	headSHAField              = "head_sha"
	urlField                  = "url"
	passwordKey               = "password"
	repoURLField              = "repo_url"
	phaseField                = "phase"
	githubIssueNumberField    = "issue_number"
	promptField               = "prompt"
	sessionIDField            = "sessionId"
	itemsField                = "items"
	runtimeField              = "runtime"
	statusField               = "status"
	scheduleField             = "schedule"
	modelField                = "model"
	systemPromptField         = "systemPrompt"
	mergeMethodField          = "merge_method"
	methodField               = "method"
	pageField                 = "page"
	perPageField              = "per_page"
	secretRefField            = "secretRef"
	workspaceField            = "workspace"
	priorTaskField            = "prior_task"
	providerRefField          = "providerRef"
	jobField                  = "job"
	limitField                = "limit"
	titleField                = "title"
	toolsField                = "tools"
	priorityField             = "priority"
)

const (
	codeLanguagePython     = "python"
	codeLanguageNode       = "node"
	codeLanguageJavaScript = "javascript"
	python3BinaryName      = "python3"
	falseStr               = "false"
	mergeMethodMerge       = "merge"
	mergeMethodRebase      = "rebase"
	errTypeNotFound        = "not_found"
	internalErrorType      = "internal_error"
)

const (
	fileReadToolName          = "file_read"
	fileWriteToolName         = "file_write"
	fetchTaskOutputToolName   = "fetch_task_output"
	listAgentsToolName        = "list_agents"
	listTasksToolName         = "list_tasks"
	listToolsToolName         = "list_tools"
	mergePullRequestToolName  = "merge_pull_request"
	postReviewCommentToolName = "post_review_comment"
	reviewPullRequestToolName = "review_pull_request"
	updateAgentToolName       = "update_agent"
	updatePlanToolName        = "update_plan"
	waitForTaskToolName       = "wait_for_task"
	waitForTasksToolName      = "wait_for_tasks"
	webFetchToolName          = "web_fetch"
)

const (
	passedStatusString  = "passed"
	mergedStatusString  = "merged"
	successStatusString = "success"
)
