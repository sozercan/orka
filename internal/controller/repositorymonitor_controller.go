/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	cron "github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	repositoryMonitorPhasePending   = "Pending"
	repositoryMonitorPhaseReady     = "Ready"
	repositoryMonitorPhaseError     = "Error"
	repositoryMonitorPhaseSuspended = "Suspended"

	repositoryMonitorRunPhaseQueued            = "queued"
	repositoryMonitorRunPhaseRunning           = "running"
	repositoryMonitorRunPhaseSucceeded         = "succeeded"
	repositoryMonitorRunPhaseFailed            = "failed"
	repositoryMonitorRunRetryScheduled         = "retry_scheduled"
	repositoryMonitorRunFailurePermanent       = "run_failed"
	repositoryMonitorCommandIntentReview       = "review"
	repositoryMonitorCommandIntentFixCI        = "fix_ci"
	repositoryMonitorCommandIntentUpdateBranch = "update_branch"
	repositoryMonitorCommandIntentDecompose    = "decompose"

	repositoryMonitorRunningRunTimeout = 30 * time.Minute
	repositoryMonitorValidationRetry   = time.Minute
	repositoryMonitorStaleRunError     = "[retry_scheduled] repository monitor run did not complete within "

	repositoryMonitorReasonReviewerCredentialsInvalid = "ReviewerCredentialsInvalid"
	repositoryMonitorReasonGitSecretInvalid           = "GitSecretInvalid"
	repositoryMonitorReasonLegacyValidationCommands   = "LegacyValidationCommandsUnsupported"
	repositoryMonitorReasonValidationImageInvalid     = "InvalidValidationImage"
)

// RepositoryMonitorReconciler reconciles RepositoryMonitor resources.
type RepositoryMonitorReconciler struct {
	client.Client
	Scheme                    *runtime.Scheme
	Store                     store.RepositoryMonitorStore
	ResultStore               store.ResultStore
	ArtifactStore             store.ArtifactStore
	HTTPClient                *http.Client
	GitHubAPIBaseURL          string
	EnforceNamespaceIsolation bool
}

// +kubebuilder:rbac:groups=core.orka.ai,resources=repositorymonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.orka.ai,resources=repositorymonitors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.orka.ai,resources=repositorymonitors/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.orka.ai,resources=tasks,verbs=get;list;watch;create
// Secret write/list verbs are already present in generated and Helm RBAC for Task reconciliation;
// keep this marker explicit because RepositoryMonitor also manages per-task runtime auth snapshots.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;create;delete

// Reconcile keeps monitor metadata durable and publishes basic status.
func (r *RepositoryMonitorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("repositorymonitor")

	monitor, found, err := r.getRepositoryMonitor(ctx, req)
	if err != nil {
		logger.Error(err, "failed to get repository monitor")
		return ctrl.Result{}, err
	}
	if !found {
		return ctrl.Result{}, nil
	}

	state, handled, result, err := r.prepareRepositoryMonitor(ctx, monitor)
	if err != nil || handled {
		return result, err
	}

	if err := r.Store.UpsertRepositoryMonitor(ctx, &store.RepositoryMonitorRecord{
		Namespace:  monitor.Namespace,
		Name:       monitor.Name,
		UID:        string(monitor.UID),
		RepoURL:    monitor.Spec.RepoURL,
		Owner:      state.owner,
		Repository: state.repository,
		Branch:     effectiveRepositoryMonitorBranch(monitor),
		Generation: monitor.Generation,
		CreatedAt:  monitor.CreationTimestamp.Time,
	}); err != nil {
		logger.Error(err, "failed to upsert repository monitor metadata")
		return ctrl.Result{}, err
	}

	return r.reconcileRepositoryMonitorRuns(ctx, monitor, state)
}

type repositoryMonitorReconcileState struct {
	owner       string
	repository  string
	schedule    cron.Schedule
	scheduleErr error
	suspended   bool
}

func (r *RepositoryMonitorReconciler) getRepositoryMonitor(ctx context.Context, req ctrl.Request) (*corev1alpha1.RepositoryMonitor, bool, error) {
	monitor := &corev1alpha1.RepositoryMonitor{}
	if err := r.Get(ctx, req.NamespacedName, monitor); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, false, err
		}
		if r.Store != nil {
			if deleteErr := r.Store.DeleteRepositoryMonitor(ctx, req.Namespace, req.Name); deleteErr != nil && !errorsIsStoreNotFound(deleteErr) {
				return nil, false, deleteErr
			}
		}
		return nil, false, nil
	}
	return monitor, true, nil
}

func (r *RepositoryMonitorReconciler) prepareRepositoryMonitor(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (repositoryMonitorReconcileState, bool, ctrl.Result, error) {
	suspended := repositoryMonitorSuspended(monitor)
	if r.Store == nil {
		if suspended {
			err := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseSuspended, "Suspended", "Repository monitor scheduled runs are suspended")
			return repositoryMonitorReconcileState{}, true, ctrl.Result{}, err
		}
		err := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "StoreUnavailable", "Repository monitor store is not configured")
		return repositoryMonitorReconcileState{}, true, ctrl.Result{RequeueAfter: time.Minute}, err
	}

	owner, repository, handled, validationRetryAfter, err := r.validateRepositoryMonitorSpec(ctx, monitor)
	if handled || err != nil {
		return repositoryMonitorReconcileState{}, handled, ctrl.Result{RequeueAfter: validationRetryAfter}, err
	}

	var schedule cron.Schedule
	var scheduleErr error
	if !suspended {
		schedule, scheduleErr = parseRepositoryMonitorSchedule(monitor)
	}
	return repositoryMonitorReconcileState{owner: owner, repository: repository, schedule: schedule, scheduleErr: scheduleErr, suspended: suspended}, false, ctrl.Result{}, nil
}

func repositoryMonitorSuspended(monitor *corev1alpha1.RepositoryMonitor) bool {
	return monitor.Spec.Suspend != nil && *monitor.Spec.Suspend
}

func parseRepositoryMonitorSchedule(monitor *corev1alpha1.RepositoryMonitor) (cron.Schedule, error) {
	schedule := repositoryMonitorSchedule(monitor)
	if schedule == "" {
		return nil, nil
	}
	parsed, err := cron.ParseStandard(schedule)
	if err == nil {
		return parsed, nil
	}
	return nil, err
}

const repositoryMonitorReasonUnsupportedReviewerAgent = "UnsupportedReviewerAgent"

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorSpec(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (string, string, bool, time.Duration, error) {
	owner, repository, err := security.ParseGitHubRepositoryURL(monitor.Spec.RepoURL)
	if err != nil {
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "InvalidRepositoryURL", repositoryScanConditionMessage(err.Error(), "invalid repository URL"))
		return "", "", true, 0, updateErr
	}
	if err := validateRepositoryMonitorSupportedTargets(monitor.Spec); err != nil {
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "UnsupportedTarget", repositoryScanConditionMessage(err.Error(), "unsupported repository monitor target"))
		return "", "", true, 0, updateErr
	}
	if strings.TrimSpace(monitor.Spec.Validation.Mode) != "" || len(monitor.Spec.Validation.Commands) > 0 {
		message := "spec.validation.mode and spec.validation.commands are no longer supported; replace them with a digest-pinned spec.validation.image"
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, repositoryMonitorReasonLegacyValidationCommands, message)
		return "", "", true, 0, updateErr
	}
	if image := monitor.Spec.Validation.Image; image != "" && !tools.ValidRepositoryValidationImage(image) {
		message := "spec.validation.image must be a valid digest-pinned OCI image reference with sha256"
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, repositoryMonitorReasonValidationImageInvalid, message)
		return "", "", true, 0, updateErr
	}
	if repositoryMonitorPullRequestsEnabled(monitor.Spec) && (monitor.Spec.Agents.Reviewer == nil || strings.TrimSpace(monitor.Spec.Agents.Reviewer.Name) == "") {
		message := "spec.agents.reviewer.name is required when pull request monitoring is enabled"
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "MissingReviewerAgent", message)
		return "", "", true, 0, updateErr
	}
	if reason, message, err := r.validateRepositoryMonitorReviewerAgent(ctx, monitor); reason != "" || err != nil {
		if err != nil {
			return "", "", false, 0, err
		}
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, reason, message)
		return "", "", true, repositoryMonitorValidationRetry, updateErr
	}
	if err := validateRepositoryMonitorCommandLabels(monitor.Spec); err != nil {
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "InvalidCommandLabels", err.Error())
		return "", "", true, repositoryMonitorValidationRetry, updateErr
	}
	if reason, message, err := r.validateRepositoryMonitorIssueReadOnlyAgents(ctx, monitor); reason != "" || err != nil {
		if err != nil {
			return "", "", false, 0, err
		}
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, reason, message)
		return "", "", true, repositoryMonitorValidationRetry, updateErr
	}
	if reason, message, err := r.validateRepositoryMonitorImplementerAgent(ctx, monitor); reason != "" || err != nil {
		if err != nil {
			return "", "", false, 0, err
		}
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, reason, message)
		return "", "", true, repositoryMonitorValidationRetry, updateErr
	}
	if monitor.Spec.Repair.Enabled && monitor.Spec.Agents.Repairer != nil && strings.TrimSpace(monitor.Spec.Agents.Repairer.Name) != "" {
		repairMonitor := monitor.DeepCopy()
		repairMonitor.Spec.Targets.Issues.Enabled = true
		repairMonitor.Spec.IssueWorkflow.Implementation.Enabled = nil
		repairMonitor.Spec.Agents.Implementer = monitor.Spec.Agents.Repairer
		if reason, message, err := r.validateRepositoryMonitorImplementerAgent(ctx, repairMonitor); reason != "" || err != nil {
			if err != nil {
				return "", "", false, 0, err
			}
			reason = strings.ReplaceAll(reason, "Implementer", "Repairer")
			message = strings.ReplaceAll(message, "implementer", "repairer")
			updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, reason, message)
			return "", "", true, repositoryMonitorValidationRetry, updateErr
		}
	}
	if reason, message, err := r.validateRepositoryMonitorGitSecret(ctx, monitor); reason != "" || err != nil {
		if err != nil {
			return "", "", false, 0, err
		}
		updateErr := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, reason, message)
		return "", "", true, repositoryMonitorValidationRetry, updateErr
	}
	return owner, repository, false, 0, nil
}

func validateRepositoryMonitorCommandLabels(spec corev1alpha1.RepositoryMonitorSpec) error {
	labels := spec.Triggers.GitHub.Labels
	groups := [][]struct{ intent, label string }{
		{{"triage", labels.Issues.Triage}, {"research", labels.Issues.Research}, {"plan", labels.Issues.Plan}, {"approve_plan", labels.Issues.ApprovePlan}, {"implement", labels.Issues.Implement}, {repositoryMonitorCommandIntentDecompose, labels.Issues.Decompose}, {"stop", labels.Issues.Stop}, {"resume", labels.Issues.Resume}},
		{{repositoryMonitorCommandIntentReview, labels.PullRequests.Review}, {"fix", labels.PullRequests.Fix}, {repositoryMonitorCommandIntentFixCI, labels.PullRequests.FixCI}, {repositoryMonitorCommandIntentUpdateBranch, labels.PullRequests.UpdateBranch}, {repositoryMonitorCommandIntentAutomerge, labels.PullRequests.Automerge}, {"stop", labels.PullRequests.Stop}, {"resume", labels.PullRequests.Resume}},
	}
	for _, group := range groups {
		seen := map[string]string{}
		for _, entry := range group {
			label := strings.ToLower(strings.TrimSpace(entry.label))
			if label == "" {
				label = defaultRepositoryMonitorCommandLabel(entry.intent)
			}
			if previous := seen[label]; previous != "" {
				return fmt.Errorf("command label %q is configured for both %s and %s", label, previous, entry.intent)
			}
			seen[label] = entry.intent
		}
	}
	return nil
}

func defaultRepositoryMonitorCommandLabel(intent string) string {
	switch intent {
	case "approve_plan":
		return "orka:approve-plan"
	case repositoryMonitorCommandIntentFixCI:
		return "orka:fix-ci"
	case repositoryMonitorCommandIntentUpdateBranch:
		return "orka:update-branch"
	case repositoryMonitorCommandIntentDecompose:
		return "orka:to-issues"
	default:
		return "orka:" + strings.ReplaceAll(intent, "_", "-")
	}
}

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorReviewerAgent(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (string, string, error) {
	if !repositoryMonitorPullRequestsEnabled(monitor.Spec) {
		return "", "", nil
	}
	reviewer := monitor.Spec.Agents.Reviewer
	if reviewer == nil || strings.TrimSpace(reviewer.Name) == "" {
		return "", "", nil
	}
	agentNamespace := reviewer.Namespace
	if agentNamespace == "" {
		agentNamespace = monitor.Namespace
	}
	if r.EnforceNamespaceIsolation && agentNamespace != monitor.Namespace {
		return "ReviewerNamespaceInvalid", fmt.Sprintf("spec.agents.reviewer namespace %q must match monitor namespace %q when namespace isolation is enforced", agentNamespace, monitor.Namespace), nil
	}

	var agent corev1alpha1.Agent
	if err := r.Get(ctx, types.NamespacedName{Name: reviewer.Name, Namespace: agentNamespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return "ReviewerAgentNotFound", fmt.Sprintf("spec.agents.reviewer %q not found in namespace %q", reviewer.Name, agentNamespace), nil
		}
		return "", "", err
	}
	if agent.Spec.Runtime == nil {
		return repositoryMonitorReasonUnsupportedReviewerAgent, fmt.Sprintf("spec.agents.reviewer %q must use a built-in claude, codex, or opencode runtime for read-only repository monitor reviews", reviewer.Name), nil
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return repositoryMonitorReasonUnsupportedReviewerAgent, fmt.Sprintf("spec.agents.reviewer %q cannot use runtimeRef because external runtimes cannot enforce read-only credential and tool isolation; use built-in claude, codex, or opencode", reviewer.Name), nil
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeClaude, corev1alpha1.AgentRuntimeCodex:
	case corev1alpha1.AgentRuntimeOpencode:
		if err := ValidateOpenCodeAgentSpec(&agent); err != nil {
			return repositoryMonitorReasonUnsupportedReviewerAgent, fmt.Sprintf("spec.agents.reviewer %q has an invalid OpenCode configuration: %v", reviewer.Name, err), nil
		}
	default:
		return repositoryMonitorReasonUnsupportedReviewerAgent, fmt.Sprintf("spec.agents.reviewer %q runtime %q is not supported for read-only repository monitor reviews; use claude, codex, or opencode", reviewer.Name, agent.Spec.Runtime.Type), nil
	}
	if err := validateBuiltInACPAgentCredentialSecretRef(&agent); err != nil {
		return repositoryMonitorReasonReviewerCredentialsInvalid, fmt.Sprintf("spec.agents.reviewer %q must omit spec.secretRef; provider credentials are supplied by the controller-managed runtime proxy", reviewer.Name), nil
	}
	return "", "", nil
}

const repositoryMonitorReasonImplementerAuthInvalid = "ImplementerCredentialsInvalid"

const repositoryMonitorReasonUnsupportedImplementerAgent = "UnsupportedImplementerAgent"

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorImplementerAgent(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (string, string, error) {
	if monitor == nil || !monitor.Spec.Targets.Issues.Enabled || (monitor.Spec.IssueWorkflow.Implementation.Enabled != nil && !*monitor.Spec.IssueWorkflow.Implementation.Enabled) {
		return "", "", nil
	}
	ref := monitor.Spec.Agents.Implementer
	if ref == nil || strings.TrimSpace(ref.Name) == "" {
		return "", "", nil
	}
	agentNamespace := strings.TrimSpace(ref.Namespace)
	if agentNamespace == "" {
		agentNamespace = monitor.Namespace
	}
	if r.EnforceNamespaceIsolation && agentNamespace != monitor.Namespace {
		return "ImplementerNamespaceInvalid", fmt.Sprintf("spec.agents.implementer namespace %q must match monitor namespace %q when namespace isolation is enforced", agentNamespace, monitor.Namespace), nil
	}
	var agent corev1alpha1.Agent
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: agentNamespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return "ImplementerAgentNotFound", fmt.Sprintf("spec.agents.implementer %q not found in namespace %q", ref.Name, agentNamespace), nil
		}
		return "", "", err
	}
	if agent.Spec.Runtime == nil {
		return repositoryMonitorReasonUnsupportedImplementerAgent, fmt.Sprintf("spec.agents.implementer %q must configure a CLI runtime", ref.Name), nil
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return repositoryMonitorReasonUnsupportedImplementerAgent, fmt.Sprintf("spec.agents.implementer %q cannot use runtimeRef because external runtimes cannot enforce implementation credential isolation; use built-in codex or claude", ref.Name), nil
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeClaude:
		if err := validateBuiltInACPAgentCredentialSecretRef(&agent); err != nil {
			return repositoryMonitorReasonImplementerAuthInvalid, fmt.Sprintf("spec.agents.implementer %q must omit spec.secretRef; provider credentials are supplied by the controller-managed runtime proxy", ref.Name), nil
		}
		return "", "", nil
	case corev1alpha1.AgentRuntimeCopilot:
		return repositoryMonitorReasonUnsupportedImplementerAgent, fmt.Sprintf("spec.agents.implementer %q cannot use copilot because its runtime credential can mutate GitHub; use codex or claude", ref.Name), nil
	default:
		return repositoryMonitorReasonUnsupportedImplementerAgent, fmt.Sprintf("spec.agents.implementer %q runtime %q is not supported; use built-in codex or claude", ref.Name, agent.Spec.Runtime.Type), nil
	}
}

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorIssueReadOnlyAgents(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (string, string, error) {
	if monitor == nil || !monitor.Spec.Targets.Issues.Enabled {
		return "", "", nil
	}
	candidates := []struct {
		role    string
		ref     *corev1alpha1.AgentReference
		enabled bool
	}{
		{role: "triager", ref: monitor.Spec.Agents.Triager, enabled: monitor.Spec.IssueWorkflow.Triage.Enabled == nil || *monitor.Spec.IssueWorkflow.Triage.Enabled},
		{role: "researcher", ref: monitor.Spec.Agents.Researcher, enabled: monitor.Spec.IssueWorkflow.Research.Enabled == nil || *monitor.Spec.IssueWorkflow.Research.Enabled},
		{role: "planner", ref: monitor.Spec.Agents.Planner, enabled: monitor.Spec.IssueWorkflow.Planning.Enabled == nil || *monitor.Spec.IssueWorkflow.Planning.Enabled},
	}
	for _, candidate := range candidates {
		if !candidate.enabled || candidate.ref == nil || strings.TrimSpace(candidate.ref.Name) == "" {
			continue
		}
		reason, message, err := r.validateRepositoryMonitorIssueReadOnlyAgent(ctx, monitor, candidate.role, candidate.ref)
		if reason != "" || err != nil {
			return reason, message, err
		}
	}
	return "", "", nil
}

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorIssueReadOnlyAgent(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, role string, ref *corev1alpha1.AgentReference) (string, string, error) {
	field := "spec.agents." + role
	reasonPrefix := strings.ToUpper(role[:1]) + role[1:]
	agentNamespace := strings.TrimSpace(ref.Namespace)
	if agentNamespace == "" {
		agentNamespace = monitor.Namespace
	}
	if r.EnforceNamespaceIsolation && agentNamespace != monitor.Namespace {
		return reasonPrefix + "NamespaceInvalid", fmt.Sprintf("%s namespace %q must match monitor namespace %q when namespace isolation is enforced", field, agentNamespace, monitor.Namespace), nil
	}
	var agent corev1alpha1.Agent
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: agentNamespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return reasonPrefix + "AgentNotFound", fmt.Sprintf("%s %q not found in namespace %q", field, ref.Name, agentNamespace), nil
		}
		return "", "", err
	}
	if agent.Spec.Runtime == nil {
		runtimeType := corev1alpha1.AgentRuntimeType("")
		return "Unsupported" + reasonPrefix + "Agent", fmt.Sprintf("%s %q runtime %q is not supported for read-only repository monitor tasks; use claude or opencode", field, ref.Name, runtimeType), nil
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return "Unsupported" + reasonPrefix + "Agent", fmt.Sprintf("%s %q cannot use runtimeRef because external runtimes cannot enforce read-only credential and tool isolation; use built-in claude or opencode", field, ref.Name), nil
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeOpencode:
		if err := ValidateOpenCodeAgentSpec(&agent); err != nil {
			return "Unsupported" + reasonPrefix + "Agent", fmt.Sprintf("%s %q has an invalid OpenCode configuration: %v", field, ref.Name, err), nil
		}
	case corev1alpha1.AgentRuntimeClaude:
	default:
		return "Unsupported" + reasonPrefix + "Agent", fmt.Sprintf("%s %q runtime %q is not supported for read-only repository monitor tasks; use claude or opencode", field, ref.Name, agent.Spec.Runtime.Type), nil
	}
	if err := validateBuiltInACPAgentCredentialSecretRef(&agent); err != nil {
		return reasonPrefix + "CredentialsInvalid", fmt.Sprintf("%s %q must omit spec.secretRef; provider credentials are supplied by the controller-managed runtime proxy", field, ref.Name), nil
	}
	return "", "", nil
}

func (r *RepositoryMonitorReconciler) validateRepositoryMonitorGitSecret(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (string, string, error) {
	return r.validateRepositoryMonitorCredentialRefs(ctx, monitor)
}

func repositoryMonitorGitSecretHasToken(secret *corev1.Secret) bool {
	if secret == nil {
		return false
	}
	for _, key := range []string{"token", "password", workerenv.GitHubToken} {
		if value := strings.TrimSpace(string(secret.Data[key])); value != "" {
			return true
		}
	}
	return false
}

//nolint:gocyclo // Reconcile run processing is intentionally linear across durable queues.
func (r *RepositoryMonitorReconciler) reconcileRepositoryMonitorRuns(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, state repositoryMonitorReconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("repositorymonitor")

	ingestedRepairs, err := r.ingestCompletedRepositoryMonitorRepairTasks(ctx, monitor)
	if err != nil {
		logger.Error(err, "failed to ingest completed repository monitor repair task")
		return ctrl.Result{}, err
	}
	ingestedIssueActions, err := r.ingestCompletedRepositoryMonitorIssueTasks(ctx, monitor)
	if err != nil {
		logger.Error(err, "failed to ingest completed repository monitor issue task")
		return ctrl.Result{}, err
	}
	ingestedReviews, pendingReviews, err := r.ingestCompletedRepositoryMonitorReviewTasks(ctx, monitor)
	if err != nil {
		logger.Error(err, "failed to ingest completed repository monitor review task")
		return ctrl.Result{}, err
	}
	publishedReviews, err := r.publishPendingRepositoryMonitorReviewRecords(ctx, monitor)
	if err != nil {
		logger.Error(err, "failed to publish pending repository monitor review")
		return ctrl.Result{}, err
	}
	queuedCommands, err := r.enqueueAcceptedRepositoryMonitorCommands(ctx, monitor)
	if err != nil {
		logger.Error(err, "failed to enqueue accepted repository monitor commands")
		return ctrl.Result{}, err
	}

	processedRun, runningRunRequeueAfter, err := r.processNextQueuedMonitorRun(ctx, monitor, state.owner, state.repository)
	if err != nil {
		logger.Error(err, "failed to process queued repository monitor run")
		return ctrl.Result{}, err
	}
	if processedRun != nil {
		if err := r.updateStatusAfterMonitorRun(ctx, monitor, processedRun); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if runningRunRequeueAfter = minimumRepositoryMonitorRequeueAfter(runningRunRequeueAfter); pendingReviews &&
		(runningRunRequeueAfter == 0 || repositoryMonitorValidationRetry < runningRunRequeueAfter) {
		runningRunRequeueAfter = repositoryMonitorValidationRetry
	}
	if runningRunRequeueAfter > 0 {
		return ctrl.Result{RequeueAfter: runningRunRequeueAfter}, nil
	}

	var queuedRun *store.MonitorRun
	requeueAfter := time.Duration(0)
	if pendingReviews {
		requeueAfter = repositoryMonitorValidationRetry
	}
	if state.suspended {
		err := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseSuspended, "Suspended", "Repository monitor scheduled runs are suspended")
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	if state.scheduleErr != nil {
		err := r.updateRepositoryMonitorNotReadyCondition(ctx, monitor, repositoryMonitorPhaseError, "InvalidSchedule", repositoryScanConditionMessage(state.scheduleErr.Error(), "invalid monitor schedule"))
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	if state.schedule != nil {
		run, next, err := r.enqueueScheduledRunIfDue(ctx, monitor, state.schedule)
		if err != nil {
			logger.Error(err, "failed to enqueue scheduled repository monitor run")
			return ctrl.Result{}, err
		}
		queuedRun = run
		if next > 0 && (requeueAfter == 0 || next < requeueAfter) {
			requeueAfter = next
		}
	}

	if queuedCommands || ingestedRepairs || ingestedIssueActions || ingestedReviews || publishedReviews {
		latestRun, err := r.latestCompletedMonitorRun(ctx, monitor)
		if err != nil {
			return ctrl.Result{}, err
		}
		if latestRun != nil {
			if err := r.updateStatusAfterMonitorRun(ctx, monitor, latestRun); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if queuedRun == nil {
		latestRun, err := r.latestCompletedMonitorRun(ctx, monitor)
		if err != nil {
			return ctrl.Result{}, err
		}
		if latestRun != nil {
			if err := r.updateStatusAfterMonitorRun(ctx, monitor, latestRun); err != nil {
				return ctrl.Result{}, err
			}
			if requeueAfter > 0 {
				if requeueAfter < time.Second {
					requeueAfter = time.Second
				}
				return ctrl.Result{RequeueAfter: requeueAfter}, nil
			}
			return ctrl.Result{}, nil
		}
	}

	if err := r.updateStatusWithRetry(ctx, monitor, func(m *corev1alpha1.RepositoryMonitor) {
		m.Status.Phase = repositoryMonitorPhaseReady
		reason := "MetadataRecorded"
		message := "Repository monitor metadata is recorded"
		if queuedRun != nil {
			m.Status.LastRunID = queuedRun.ID
			reason = "RunQueued"
			message = "Scheduled repository monitor run queued"
		}
		m.Status.ObservedGeneration = m.Generation
		meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: m.Generation,
		})
	}); err != nil {
		return ctrl.Result{}, err
	}

	if queuedRun != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if requeueAfter = minimumRepositoryMonitorRequeueAfter(requeueAfter); requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	return ctrl.Result{}, nil
}

func minimumRepositoryMonitorRequeueAfter(requeueAfter time.Duration) time.Duration {
	if requeueAfter <= 0 {
		return 0
	}
	if requeueAfter < time.Second {
		return time.Second
	}
	return requeueAfter
}

func (r *RepositoryMonitorReconciler) updateRepositoryMonitorNotReadyCondition(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, phase, reason, message string) error {
	return r.updateStatusWithRetry(ctx, monitor, func(m *corev1alpha1.RepositoryMonitor) {
		m.Status.Phase = phase
		m.Status.ObservedGeneration = m.Generation
		meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: m.Generation,
		})
	})
}

func (r *RepositoryMonitorReconciler) latestCompletedMonitorRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (*store.MonitorRun, error) {
	var latest *store.MonitorRun
	for _, phase := range []string{repositoryMonitorRunPhaseSucceeded, repositoryMonitorRunPhaseFailed} {
		runs, _, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
			Namespace:   monitor.Namespace,
			MonitorName: monitor.Name,
			Phase:       phase,
			Limit:       1,
		})
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			continue
		}
		candidate := runs[0]
		if latest == nil || monitorRunCompletionTime(candidate).After(monitorRunCompletionTime(*latest)) {
			latest = &candidate
		}
	}
	return latest, nil
}

func monitorRunCompletionTime(run store.MonitorRun) time.Time {
	if run.CompletedAt != nil {
		return *run.CompletedAt
	}
	return run.StartedAt
}

func repositoryMonitorSchedule(monitor *corev1alpha1.RepositoryMonitor) string {
	schedule := strings.TrimSpace(monitor.Spec.Schedule)
	if schedule == "" {
		return ""
	}
	if monitor.Spec.TimeZone != nil && strings.TrimSpace(*monitor.Spec.TimeZone) != "" {
		return "CRON_TZ=" + strings.TrimSpace(*monitor.Spec.TimeZone) + " " + schedule
	}
	return schedule
}

func effectiveRepositoryMonitorBranch(monitor *corev1alpha1.RepositoryMonitor) string {
	if monitor.Spec.Branch != "" {
		return monitor.Spec.Branch
	}
	return defaultACPSourceBranch
}

func (r *RepositoryMonitorReconciler) enqueueScheduledRunIfDue(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, schedule cron.Schedule) (*store.MonitorRun, time.Duration, error) {
	activeRun, err := r.activeMonitorRunExists(ctx, monitor)
	if err != nil {
		return nil, 0, err
	}
	if activeRun {
		return nil, 15 * time.Second, nil
	}

	runs, _, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
		Namespace:   monitor.Namespace,
		MonitorName: monitor.Name,
		Trigger:     "schedule",
		Limit:       1,
	})
	if err != nil {
		return nil, 0, err
	}

	base := monitor.CreationTimestamp.Time
	if base.IsZero() {
		base = time.Now()
	}
	if len(runs) > 0 && runs[0].StartedAt.After(base) {
		base = runs[0].StartedAt
	}

	now := time.Now()
	nextRun := schedule.Next(base)
	if now.Before(nextRun) {
		return nil, time.Until(nextRun), nil
	}

	run := &store.MonitorRun{
		ID:               fmt.Sprintf("monrun-%d", now.UTC().UnixNano()),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		Trigger:          "schedule",
		Phase:            repositoryMonitorRunPhaseQueued,
		StartedAt:        now,
	}
	if err := r.Store.CreateMonitorRun(ctx, run); err != nil {
		return nil, 0, err
	}
	return run, 15 * time.Second, nil
}

func (r *RepositoryMonitorReconciler) activeMonitorRunExists(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (bool, error) {
	for _, phase := range []string{repositoryMonitorRunPhaseQueued, repositoryMonitorRunPhaseRunning} {
		runs, _, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
			Namespace:   monitor.Namespace,
			MonitorName: monitor.Name,
			Phase:       phase,
			Limit:       1,
		})
		if err != nil {
			return false, err
		}
		if len(runs) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (r *RepositoryMonitorReconciler) processNextQueuedMonitorRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, owner, repository string) (*store.MonitorRun, time.Duration, error) {
	staleRun, requeueAfter, err := r.failStaleRunningMonitorRun(ctx, monitor)
	if err != nil || staleRun != nil || requeueAfter > 0 {
		return staleRun, requeueAfter, err
	}

	runs, _, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
		Namespace:   monitor.Namespace,
		MonitorName: monitor.Name,
		Phase:       repositoryMonitorRunPhaseQueued,
		OldestFirst: true,
		Limit:       1,
	})
	if err != nil || len(runs) == 0 {
		return nil, 0, err
	}

	run := runs[0]
	if requeueAfter := time.Until(run.StartedAt); requeueAfter > 0 {
		return nil, requeueAfter, nil
	}
	run.Phase = repositoryMonitorRunPhaseRunning
	if err := r.Store.UpdateMonitorRun(ctx, &run); err != nil {
		return nil, 0, err
	}
	if err := r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "run_started", "Repository monitor run started", nil); err != nil {
		completedAt := time.Now()
		run.CompletedAt = &completedAt
		run.Phase = repositoryMonitorRunPhaseFailed
		run.Error = err.Error()
		if updateErr := r.Store.UpdateMonitorRun(ctx, &run); updateErr != nil {
			return nil, 0, fmt.Errorf("failed to record repository monitor run start: %w; additionally failed to mark run failed: %v", err, updateErr)
		}
		return &run, 0, nil
	}

	selected, createdTasks, skipped, processErr := r.processRepositoryMonitorInventoryRun(ctx, monitor, &run, owner, repository)
	completedAt := time.Now()
	run.CompletedAt = &completedAt
	run.SelectedCount = selected
	run.CreatedTaskCount = createdTasks
	run.SkippedCount = skipped
	if processErr != nil {
		failureState := repositoryMonitorRunFailureState(processErr)
		if strings.TrimSpace(run.CommandEventID) == "" && repositoryMonitorFailedCommandRunRetryable("["+failureState+"]") {
			events, _, listErr := r.Store.ListMonitorEvents(ctx, store.MonitorEventFilter{Namespace: monitor.Namespace, MonitorName: monitor.Name, RunID: run.ID, EventType: "run_failed", Limit: repositoryMonitorCommandMaxRetries})
			if listErr != nil {
				return nil, 0, listErr
			}
			if len(events) < repositoryMonitorCommandMaxRetries {
				run.Phase = repositoryMonitorRunPhaseQueued
				run.StartedAt = time.Now().Add(repositoryMonitorCommandRetryDelay)
				run.CompletedAt = nil
				run.Error = ""
				if err := r.Store.UpdateMonitorRun(ctx, &run); err != nil {
					return nil, 0, err
				}
				if eventErr := r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "run_failed", repositoryScanConditionMessage(processErr.Error(), "repository monitor run failed; retry scheduled"), map[string]any{"state": failureState}); eventErr != nil {
					return nil, 0, eventErr
				}
				return &run, repositoryMonitorCommandRetryDelay, nil
			}
			failureState = repositoryMonitorRunFailurePermanent
			processErr = fmt.Errorf("retry_attempts_exhausted: %w", processErr)
		}
		run.Phase = repositoryMonitorRunPhaseFailed
		run.Error = fmt.Sprintf("[%s] %s", failureState, processErr.Error())
		if err := r.Store.UpdateMonitorRun(ctx, &run); err != nil {
			return nil, 0, err
		}
		metrics.RecordRepositoryMonitorBlock(failureState)
		if eventErr := r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "run_failed", repositoryScanConditionMessage(processErr.Error(), "repository monitor run failed"), map[string]any{"state": failureState}); eventErr != nil {
			return nil, 0, eventErr
		}
		return &run, 0, nil
	}

	run.Phase = repositoryMonitorRunPhaseSucceeded
	run.Error = ""
	if err := r.Store.UpdateMonitorRun(ctx, &run); err != nil {
		return nil, 0, err
	}
	if err := r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "run_succeeded", "Repository monitor run completed", map[string]any{
		"selected":     selected,
		"createdTasks": createdTasks,
		"skipped":      skipped,
	}); err != nil {
		return nil, 0, err
	}
	return &run, 0, nil
}

func (r *RepositoryMonitorReconciler) failStaleRunningMonitorRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (*store.MonitorRun, time.Duration, error) {
	runs, _, err := r.Store.ListMonitorRuns(ctx, store.MonitorRunFilter{
		Namespace:   monitor.Namespace,
		MonitorName: monitor.Name,
		Phase:       repositoryMonitorRunPhaseRunning,
		Limit:       1,
	})
	if err != nil || len(runs) == 0 {
		return nil, 0, err
	}

	run := runs[0]
	now := time.Now()
	runningFor := now.Sub(run.StartedAt)
	if runningFor < repositoryMonitorRunningRunTimeout {
		return nil, repositoryMonitorRunningRunTimeout - runningFor, nil
	}

	run.Phase = repositoryMonitorRunPhaseFailed
	run.CompletedAt = &now
	run.Error = fmt.Sprintf("%s%s and was marked failed", repositoryMonitorStaleRunError, repositoryMonitorRunningRunTimeout)
	if err := r.Store.UpdateMonitorRun(ctx, &run); err != nil {
		return nil, 0, err
	}
	if err := r.createMonitorEvent(ctx, monitor, run.ID, "", 0, "", "run_failed", run.Error, map[string]any{
		"reason":  "stale_running_run",
		"timeout": repositoryMonitorRunningRunTimeout.String(),
	}); err != nil {
		run.Error = fmt.Sprintf("%s; additionally failed to record recovery event: %v", run.Error, err)
	}
	return &run, 0, nil
}

func repositoryMonitorRunFailureState(err error) string {
	if err == nil {
		return ""
	}
	if _, ok := errors.AsType[*repositoryMonitorPendingMutationProjectionError](err); ok {
		return repositoryMonitorRunRetryScheduled
	}
	if ghErr, ok := errors.AsType[*repositoryMonitorGitHubAPIError](err); ok {
		if ghErr.StatusCode == http.StatusTooManyRequests || (ghErr.StatusCode == http.StatusForbidden && repositoryMonitorGitHubErrorLooksRateLimited(ghErr.Body)) {
			return "github_rate_limited"
		}
		if ghErr.StatusCode == http.StatusRequestTimeout {
			return repositoryMonitorRunRetryScheduled
		}
		if ghErr.StatusCode == http.StatusConflict || ghErr.StatusCode == http.StatusUnprocessableEntity {
			return repositoryMonitorRunRetryScheduled
		}
		if ghErr.StatusCode >= 500 {
			return repositoryMonitorRunRetryScheduled
		}
		if ghErr.StatusCode >= 400 && ghErr.StatusCode < 500 {
			return "run_failed"
		}
	}
	if errors.Is(err, io.EOF) {
		return repositoryMonitorRunRetryScheduled
	}
	lower := strings.ToLower(err.Error())
	if apierrors.IsTooManyRequests(err) || strings.Contains(lower, "insufficient quota") || strings.Contains(lower, "cluster capacity") {
		return "cluster_capacity_blocked"
	}
	if strings.Contains(lower, "llm_rate_limited") || strings.Contains(lower, "llm rate limited") {
		return "llm_rate_limited"
	}
	for _, marker := range []string{"timeout", "connection refused", "connection reset", "temporarily unavailable", "unexpected eof"} {
		if strings.Contains(lower, marker) {
			return repositoryMonitorRunRetryScheduled
		}
	}
	return repositoryMonitorRunFailurePermanent
}

func (r *RepositoryMonitorReconciler) updateStatusAfterMonitorRun(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun) error {
	counts, err := r.repositoryMonitorStatusCounts(ctx, monitor)
	if err != nil {
		return err
	}
	return r.updateStatusWithRetry(ctx, monitor, func(m *corev1alpha1.RepositoryMonitor) {
		m.Status.LastRunID = run.ID
		if run.CompletedAt != nil {
			completedAt := metav1.NewTime(*run.CompletedAt)
			m.Status.LastRunTime = &completedAt
			if run.Phase == repositoryMonitorRunPhaseSucceeded {
				successAt := completedAt
				m.Status.LastSuccessfulRunTime = &successAt
			}
		}
		m.Status.OpenPullRequests = counts.openPullRequests
		m.Status.PendingReviews = counts.pendingReviews
		m.Status.BlockedItems = counts.blockedItems
		m.Status.OpenIssues = counts.openIssues
		m.Status.PendingIssueActions = counts.pendingIssueActions
		m.Status.BlockedIssues = counts.blockedIssues
		m.Status.ActiveRepairs = counts.activeRepairs
		m.Status.MergeReadyItems = counts.mergeReadyItems
		m.Status.ObservedGeneration = m.Generation

		condition := metav1.Condition{
			Type:               "Ready",
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: m.Generation,
		}
		if run.Phase == repositoryMonitorRunPhaseFailed {
			m.Status.Phase = repositoryMonitorPhaseError
			condition.Status = metav1.ConditionFalse
			condition.Reason = "RunFailed"
			condition.Message = repositoryScanConditionMessage(run.Error, "repository monitor run failed")
		} else {
			m.Status.Phase = repositoryMonitorPhaseReady
			condition.Status = metav1.ConditionTrue
			condition.Reason = "RunSucceeded"
			condition.Message = "Repository monitor run completed"
		}
		meta.SetStatusCondition(&m.Status.Conditions, condition)
	})
}

type repositoryMonitorStatusCounts struct {
	openPullRequests    int32
	pendingReviews      int32
	blockedItems        int32
	activeRepairs       int32
	mergeReadyItems     int32
	openIssues          int32
	pendingIssueActions int32
	blockedIssues       int32
}

func (r *RepositoryMonitorReconciler) repositoryMonitorStatusCounts(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor) (repositoryMonitorStatusCounts, error) {
	prItems, err := r.listRepositoryMonitorPullRequestItems(ctx, monitor)
	if err != nil {
		return repositoryMonitorStatusCounts{}, err
	}
	issueItems, err := r.listRepositoryMonitorIssueItems(ctx, monitor)
	if err != nil {
		return repositoryMonitorStatusCounts{}, err
	}
	var counts repositoryMonitorStatusCounts
	for _, item := range prItems {
		if item.State != repositoryMonitorItemStateOpen {
			continue
		}
		counts.openPullRequests++
		if item.LastVerdict == repositoryMonitorReviewVerdictPassed && item.LastReviewedHeadSHA == item.HeadSHA && !repositoryMonitorAutomergeRepairStateBlocks(item.RepairState) && item.SkipReason == "" {
			counts.mergeReadyItems++
		}
		if item.RepairState == repositoryMonitorRepairPhaseQueued {
			counts.activeRepairs++
		}
		switch item.LastVerdict {
		case repositoryMonitorRunPhaseQueued:
			counts.pendingReviews++
		default:
			if repositoryMonitorItemVerdictBlocked(item.LastVerdict) {
				counts.blockedItems++
			}
		}
	}
	for _, item := range issueItems {
		if item.State != repositoryMonitorItemStateOpen {
			continue
		}
		counts.openIssues++
		switch item.WorkflowPhase {
		case "triage_queued", "research_queued", "plan_queued", "implementation_queued", "mutation_queued":
			counts.pendingIssueActions++
		case repositoryMonitorIssuePhaseBlocked, repositoryMonitorIssuePhaseApprovalRequired:
			counts.blockedIssues++
		default:
			if repositoryMonitorItemVerdictBlocked(item.LastVerdict) {
				counts.blockedIssues++
			}
		}
	}
	return counts, nil
}

func repositoryMonitorItemVerdictBlocked(verdict string) bool {
	switch strings.TrimSpace(verdict) {
	case repositoryMonitorVerdictSkipped,
		repositoryMonitorReviewVerdictFailed,
		repositoryMonitorReviewVerdictStale,
		repositoryMonitorReviewVerdictNeedsChanges,
		repositoryMonitorReviewVerdictNeedsHuman,
		repositoryMonitorReviewVerdictSecuritySensitive:
		return true
	default:
		return false
	}
}

func errorsIsStoreNotFound(err error) bool {
	return err == store.ErrNotFound
}

func (r *RepositoryMonitorReconciler) updateStatusWithRetry(ctx context.Context, monitor *corev1alpha1.RepositoryMonitor, mutate func(*corev1alpha1.RepositoryMonitor)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &corev1alpha1.RepositoryMonitor{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(monitor), current); err != nil {
			return err
		}
		patch := current.DeepCopy()
		mutate(patch)
		if reflect.DeepEqual(current.Status, patch.Status) {
			return nil
		}
		return r.Status().Patch(ctx, patch, client.MergeFrom(current))
	})
}

// SetupWithManager sets up the controller with the manager.
func (r *RepositoryMonitorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.RepositoryMonitor{}).
		Owns(&corev1alpha1.Task{}).
		Named("repositorymonitor").
		Complete(r)
}
