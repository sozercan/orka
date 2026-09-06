package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

type CreateRepositoryMonitorRequest struct {
	Name      string                             `json:"name"`
	Namespace string                             `json:"namespace"`
	Metadata  MetadataRequest                    `json:"metadata"`
	Spec      corev1alpha1.RepositoryMonitorSpec `json:"spec"`
}

type UpdateRepositoryMonitorRequest struct {
	Spec corev1alpha1.RepositoryMonitorSpec `json:"spec"`
}

type CreateRepositoryMonitorRunRequest struct {
	TargetKind   string `json:"targetKind,omitempty"`
	TargetNumber int64  `json:"targetNumber,omitempty"`
	TargetSHA    string `json:"targetSHA,omitempty"`
}

// CreateRepositoryMonitorCommandRequest records an explicit monitor command from the Orka API.
type CreateRepositoryMonitorCommandRequest struct {
	Kind      string `json:"kind"`
	Number    int64  `json:"number"`
	Intent    string `json:"intent"`
	TargetSHA string `json:"targetSHA,omitempty"`
}

const (
	repositoryMonitorRunRequestAnnotation  = "orka.ai/repository-monitor-run-requested-at"
	repositoryMonitorTargetKindPullRequest = "pull_request"
	repositoryMonitorTargetKindIssue       = "issue"
	repositoryMonitorRunPhaseQueued        = "queued"
	repositoryMonitorRunPhaseRunning       = "running"
	repositoryMonitorRunPhaseFailed        = "failed"
	repositoryMonitorIntentAutomerge       = "automerge"
	repositoryMonitorDefaultBranch         = "main"
)

func (h *Handlers) ensureRepositoryMonitorStore() error {
	if h.repositoryMonitorStore == nil {
		return fiber.NewError(fiber.StatusNotImplemented, "repository monitor store not configured")
	}
	return nil
}

func (h *Handlers) normalizeRepositoryMonitorSpec(spec *corev1alpha1.RepositoryMonitorSpec) {
	if spec.Provider == "" {
		spec.Provider = sourceProviderGitHub
	}
	if spec.Branch == "" {
		spec.Branch = repositoryMonitorDefaultBranch
	}
	if owner, repo, err := parseRepositoryMonitorGitHubURL(spec.RepoURL); err == nil {
		spec.Owner = owner
		spec.Repository = repo
	} else if spec.Owner == "" || spec.Repository == "" {
		owner, repo := security.ParseRepositoryURL(spec.RepoURL)
		if spec.Owner == "" {
			spec.Owner = owner
		}
		if spec.Repository == "" {
			spec.Repository = repo
		}
	}
	if spec.Targets.PullRequests.Enabled == nil && !spec.Targets.Issues.Enabled && !spec.Targets.Commits.Enabled {
		enabled := true
		spec.Targets.PullRequests.Enabled = &enabled
	}
	if spec.Targets.PullRequests.MaxPerRun == nil {
		maxPerRun := int32(20)
		spec.Targets.PullRequests.MaxPerRun = &maxPerRun
	}
	if spec.Review.Event == "" {
		spec.Review.Event = "COMMENT"
	}
}

func validateRepositoryMonitorSpec(spec corev1alpha1.RepositoryMonitorSpec) error {
	if strings.TrimSpace(spec.RepoURL) == "" {
		return fiber.NewError(fiber.StatusBadRequest, "spec.repoURL is required")
	}
	if spec.Provider != "" && spec.Provider != sourceProviderGitHub {
		return fiber.NewError(fiber.StatusBadRequest, "spec.provider must be github")
	}
	if _, _, err := parseRepositoryMonitorGitHubURL(spec.RepoURL); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	if err := validateRepositoryMonitorSupportedTargets(spec); err != nil {
		return err
	}
	if err := validateRepositoryMonitorReviewPublishSpec(spec.Review.Publish); err != nil {
		return err
	}
	if err := validateRepositoryMonitorCommandLabels(spec); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Validation.Mode) != "" || len(spec.Validation.Commands) > 0 {
		return fiber.NewError(fiber.StatusBadRequest, "spec.validation.mode and spec.validation.commands are no longer supported; replace them with spec.validation.image")
	}
	if image := spec.Validation.Image; image != "" && !tools.ValidRepositoryValidationImage(image) {
		return fiber.NewError(fiber.StatusBadRequest, "spec.validation.image must be digest-pinned with sha256")
	}
	if repositoryMonitorPullRequestsEnabled(spec) && (spec.Agents.Reviewer == nil || strings.TrimSpace(spec.Agents.Reviewer.Name) == "") {
		return fiber.NewError(fiber.StatusBadRequest, "spec.agents.reviewer.name is required when pull request monitoring is enabled")
	}
	if spec.Triggers.GitHub.Labels.Enabled && (spec.ForgeCredentialRef == nil || strings.TrimSpace(spec.ForgeCredentialRef.Name) == "") {
		return fiber.NewError(fiber.StatusBadRequest, "spec.forgeCredentialRef is required when GitHub label triggers are enabled")
	}
	return nil
}

func validateRepositoryMonitorCommandLabels(spec corev1alpha1.RepositoryMonitorSpec) error {
	labels := spec.Triggers.GitHub.Labels
	groups := [][]struct{ intent, label string }{
		{{"triage", labels.Issues.Triage}, {"research", labels.Issues.Research}, {"plan", labels.Issues.Plan}, {commandIntentApprovePlan, labels.Issues.ApprovePlan}, {"implement", labels.Issues.Implement}, {commandIntentDecompose, labels.Issues.Decompose}, {commandIntentStop, labels.Issues.Stop}, {commandIntentResume, labels.Issues.Resume}},
		{{"review", labels.PullRequests.Review}, {"fix", labels.PullRequests.Fix}, {commandIntentFixCI, labels.PullRequests.FixCI}, {commandIntentUpdateBranch, labels.PullRequests.UpdateBranch}, {"automerge", labels.PullRequests.Automerge}, {commandIntentStop, labels.PullRequests.Stop}, {commandIntentResume, labels.PullRequests.Resume}},
	}
	for _, group := range groups {
		seen := map[string]string{}
		for _, entry := range group {
			label := strings.ToLower(strings.TrimSpace(entry.label))
			if label == "" {
				label = repositoryMonitorDefaultCommandLabel(entry.intent)
			}
			if previous := seen[label]; previous != "" {
				return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("command label %q is configured for both %s and %s", label, previous, entry.intent))
			}
			seen[label] = entry.intent
		}
	}
	return nil
}

func validateRepositoryMonitorReviewPublishSpec(publish corev1alpha1.RepositoryMonitorReviewPublishSpec) error {
	if mode := strings.TrimSpace(publish.Mode); mode != "" && mode != "summary_only" && mode != "summary_with_inline_findings" {
		return fiber.NewError(fiber.StatusBadRequest, "spec.review.publish.mode must be summary_only or summary_with_inline_findings")
	}
	if event := strings.TrimSpace(publish.Event); event != "" && event != "COMMENT" {
		return fiber.NewError(fiber.StatusBadRequest, "spec.review.publish.event only supports COMMENT in v1")
	}
	if policy := strings.TrimSpace(publish.SameHeadPolicy); policy != "" && policy != "skip" {
		return fiber.NewError(fiber.StatusBadRequest, "spec.review.publish.sameHeadPolicy only supports skip in v1")
	}
	if priority := strings.TrimSpace(publish.Inline.MinPriority); priority != "" {
		switch priority {
		case "P0", "P1", "P2", "P3":
		default:
			return fiber.NewError(fiber.StatusBadRequest, "spec.review.publish.inline.minPriority must be one of P0, P1, P2, or P3")
		}
	}
	if publish.Inline.MaxComments != nil {
		if *publish.Inline.MaxComments < 0 || *publish.Inline.MaxComments > 50 {
			return fiber.NewError(fiber.StatusBadRequest, "spec.review.publish.inline.maxComments must be between 0 and 50")
		}
	}
	return nil
}

func (h *Handlers) validateRepositoryMonitorImplementerAgent(c fiber.Ctx, namespace string, spec corev1alpha1.RepositoryMonitorSpec) error {
	if !spec.Targets.Issues.Enabled || (spec.IssueWorkflow.Implementation.Enabled != nil && !*spec.IssueWorkflow.Implementation.Enabled) {
		return nil
	}
	ref := spec.Agents.Implementer
	if ref == nil || strings.TrimSpace(ref.Name) == "" {
		return nil
	}
	agentNamespace := strings.TrimSpace(ref.Namespace)
	if agentNamespace == "" {
		agentNamespace = namespace
	}
	if h.enforceNamespaceIsolation && agentNamespace != namespace {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer namespace %q must match monitor namespace %q when namespace isolation is enforced", agentNamespace, namespace))
	}
	if err := authorizeKubernetesResourceAction(c.Context(), h.clientset, GetUserInfo(c),
		agentNamespace, "get", corev1alpha1.GroupVersion.Group, "agents", ref.Name); err != nil {
		return err
	}
	var agent corev1alpha1.Agent
	if err := h.client.Get(c.Context(), types.NamespacedName{Name: ref.Name, Namespace: agentNamespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q not found in namespace %q", ref.Name, agentNamespace))
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get implementer agent %q: %v", ref.Name, err))
	}
	if agent.Spec.Runtime == nil {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q must configure a CLI runtime", ref.Name))
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q cannot use runtimeRef because external runtimes cannot enforce implementation credential isolation; use built-in codex or claude", ref.Name))
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeClaude:
		if agent.Spec.SecretRef != nil && strings.TrimSpace(agent.Spec.SecretRef.Name) != "" {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q must omit spec.secretRef; provider credentials are supplied by the controller-managed runtime proxy", ref.Name))
		}
		return nil
	case corev1alpha1.AgentRuntimeCopilot:
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q cannot use copilot because its runtime credential can mutate GitHub; use codex or claude", ref.Name))
	default:
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("spec.agents.implementer %q runtime %q is not supported; use built-in codex or claude", ref.Name, agent.Spec.Runtime.Type))
	}
}

func (h *Handlers) validateRepositoryMonitorReadOnlyAgents(c fiber.Ctx, namespace string, spec corev1alpha1.RepositoryMonitorSpec) error {
	readOnlyAgents := []struct {
		role    string
		ref     *corev1alpha1.AgentReference
		enabled bool
	}{
		{role: "reviewer", ref: spec.Agents.Reviewer, enabled: repositoryMonitorPullRequestsEnabled(spec)},
		{role: "triager", ref: spec.Agents.Triager, enabled: spec.Targets.Issues.Enabled && (spec.IssueWorkflow.Triage.Enabled == nil || *spec.IssueWorkflow.Triage.Enabled)},
		{role: "researcher", ref: spec.Agents.Researcher, enabled: spec.Targets.Issues.Enabled && (spec.IssueWorkflow.Research.Enabled == nil || *spec.IssueWorkflow.Research.Enabled)},
		{role: "planner", ref: spec.Agents.Planner, enabled: spec.Targets.Issues.Enabled && (spec.IssueWorkflow.Planning.Enabled == nil || *spec.IssueWorkflow.Planning.Enabled)},
	}
	for _, candidate := range readOnlyAgents {
		if !candidate.enabled || candidate.ref == nil || strings.TrimSpace(candidate.ref.Name) == "" {
			continue
		}
		if err := h.validateRepositoryMonitorReadOnlyAgent(c, namespace, candidate.role, candidate.ref); err != nil {
			return err
		}
	}
	if err := h.validateRepositoryMonitorImplementerAgent(c, namespace, spec); err != nil {
		return err
	}
	if spec.Repair.Enabled && spec.Agents.Repairer != nil && strings.TrimSpace(spec.Agents.Repairer.Name) != "" {
		repairSpec := spec
		repairSpec.Targets.Issues.Enabled = true
		repairSpec.IssueWorkflow.Implementation.Enabled = nil
		repairSpec.Agents.Implementer = spec.Agents.Repairer
		if err := h.validateRepositoryMonitorImplementerAgent(c, namespace, repairSpec); err != nil {
			if fiberErr, ok := errors.AsType[*fiber.Error](err); ok {
				return fiber.NewError(fiberErr.Code, strings.ReplaceAll(fiberErr.Message, "implementer", "repairer"))
			}
			return err
		}
	}
	return nil
}

func (h *Handlers) validateRepositoryMonitorReadOnlyAgent(c fiber.Ctx, namespace, role string, ref *corev1alpha1.AgentReference) error {
	field := "spec.agents." + role
	agentNamespace := strings.TrimSpace(ref.Namespace)
	if agentNamespace == "" {
		agentNamespace = namespace
	}
	if h.enforceNamespaceIsolation && agentNamespace != namespace {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s namespace %q must match monitor namespace %q when namespace isolation is enforced", field, agentNamespace, namespace))
	}
	if err := authorizeKubernetesResourceAction(c.Context(), h.clientset, GetUserInfo(c),
		agentNamespace, "get", corev1alpha1.GroupVersion.Group, "agents", ref.Name); err != nil {
		return err
	}
	var agent corev1alpha1.Agent
	if err := h.client.Get(c.Context(), types.NamespacedName{Name: ref.Name, Namespace: agentNamespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q not found in namespace %q", field, ref.Name, agentNamespace))
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get %s agent %q: %v", role, ref.Name, err))
	}
	allowedRuntimes := "claude or opencode"
	if role == "reviewer" {
		allowedRuntimes = "claude, codex, or opencode"
	}
	if agent.Spec.Runtime == nil {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q must use a built-in %s runtime for read-only repository monitor tasks", field, ref.Name, allowedRuntimes))
	}
	if agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q cannot use runtimeRef because external runtimes cannot enforce read-only credential and tool isolation; use built-in %s", field, ref.Name, allowedRuntimes))
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeOpencode:
		if err := controller.ValidateOpenCodeAgentSpec(&agent); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q has an invalid OpenCode configuration: %v", field, ref.Name, err))
		}
	case corev1alpha1.AgentRuntimeClaude:
	case corev1alpha1.AgentRuntimeCodex:
		// Codex reviewers run inside the RuntimeSession boundary with
		// controller-rejected elevation requests and read-intent workspace
		// delta classification failing any modifying turn.
		if role != "reviewer" {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q runtime %q is not supported for read-only repository monitor tasks; use %s", field, ref.Name, agent.Spec.Runtime.Type, allowedRuntimes))
		}
	default:
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q runtime %q is not supported for read-only repository monitor tasks; use %s", field, ref.Name, agent.Spec.Runtime.Type, allowedRuntimes))
	}
	if agent.Spec.SecretRef != nil && strings.TrimSpace(agent.Spec.SecretRef.Name) != "" {
		return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q must omit spec.secretRef; provider credentials are supplied by the controller-managed runtime proxy", field, ref.Name))
	}
	return nil
}

func validateRepositoryMonitorCredentialRoleRefs(spec corev1alpha1.RepositoryMonitorSpec) error {
	implementationEnabled := spec.Targets.Issues.Enabled &&
		(spec.IssueWorkflow.Implementation.Enabled == nil || *spec.IssueWorkflow.Implementation.Enabled) &&
		spec.Agents.Implementer != nil && strings.TrimSpace(spec.Agents.Implementer.Name) != ""
	repairEnabled := spec.Repair.Enabled && spec.Agents.Repairer != nil && strings.TrimSpace(spec.Agents.Repairer.Name) != ""
	if !implementationEnabled && !repairEnabled {
		return nil
	}
	refs := []struct {
		field string
		ref   *corev1.LocalObjectReference
	}{
		{field: "spec.readCredentialRef", ref: spec.ReadCredentialRef},
		{field: "spec.publicationReadCredentialRef", ref: spec.PublicationReadCredentialRef},
		{field: "spec.publicationCredentialRef", ref: spec.PublicationCredentialRef},
		{field: "spec.forgeCredentialRef", ref: spec.ForgeCredentialRef},
	}
	seen := make(map[string]string, len(refs))
	for _, credential := range refs {
		name := repositoryMonitorCredentialRefName(credential.ref)
		if name == "" {
			return fiber.NewError(fiber.StatusBadRequest, credential.field+" is required for repository monitor write workflows")
		}
		if previous := seen[name]; previous != "" {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s and %s must reference distinct Secrets", previous, credential.field))
		}
		seen[name] = credential.field
	}
	return nil
}

func repositoryMonitorCredentialRefName(ref *corev1.LocalObjectReference) string {
	if ref == nil {
		return ""
	}
	return strings.TrimSpace(ref.Name)
}

func repositoryMonitorEffectiveReadCredential(spec corev1alpha1.RepositoryMonitorSpec) (string, *corev1.LocalObjectReference) {
	if repositoryMonitorCredentialRefName(spec.ReadCredentialRef) != "" {
		return "spec.readCredentialRef", spec.ReadCredentialRef
	}
	return "spec.gitSecretRef", spec.GitSecretRef
}

func (h *Handlers) validateRepositoryMonitorCredentialSecrets(c fiber.Ctx, namespace string, spec corev1alpha1.RepositoryMonitorSpec) error {
	readField, readRef := repositoryMonitorEffectiveReadCredential(spec)
	refs := []struct {
		field string
		ref   *corev1.LocalObjectReference
	}{
		{field: readField, ref: readRef},
		{field: "spec.publicationReadCredentialRef", ref: spec.PublicationReadCredentialRef},
		{field: "spec.publicationCredentialRef", ref: spec.PublicationCredentialRef},
		{field: "spec.forgeCredentialRef", ref: spec.ForgeCredentialRef},
	}
	for _, credential := range refs {
		secretName := repositoryMonitorCredentialRefName(credential.ref)
		if secretName == "" {
			continue
		}
		if err := authorizeKubernetesResourceAction(c.Context(), h.clientset, GetUserInfo(c),
			namespace, "get", "", "secrets", secretName); err != nil {
			return err
		}
		var secret corev1.Secret
		if err := h.client.Get(c.Context(), types.NamespacedName{Name: secretName, Namespace: namespace}, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q not found in namespace %q", credential.field, secretName, namespace))
			}
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get %s %q: %v", credential.field, secretName, err))
		}
		if !repositoryMonitorGitSecretHasToken(&secret) {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("%s %q must contain a non-empty token, password, or %s key", credential.field, secretName, workerenv.GitHubToken))
		}
	}
	return nil
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

func validateRepositoryMonitorSupportedTargets(spec corev1alpha1.RepositoryMonitorSpec) error {
	if spec.Targets.Commits.Enabled {
		return fiber.NewError(fiber.StatusBadRequest, "spec.targets.commits is not supported; only pull request and issue monitoring are supported")
	}
	if !repositoryMonitorPullRequestsEnabled(spec) && !spec.Targets.Issues.Enabled {
		return fiber.NewError(fiber.StatusBadRequest, "at least one repository monitor target must be enabled")
	}
	return nil
}

func validateRepositoryMonitorRunRequest(req CreateRepositoryMonitorRunRequest) error {
	switch strings.TrimSpace(req.TargetKind) {
	case "", repositoryMonitorTargetKindPullRequest, repositoryMonitorTargetKindIssue:
		return nil
	default:
		return fiber.NewError(fiber.StatusBadRequest, "targetKind must be pull_request or issue")
	}
}

func parseRepositoryMonitorGitHubURL(repoURL string) (string, string, error) {
	owner, repo, err := security.ParseGitHubRepositoryURL(repoURL)
	if err != nil {
		return "", "", repositoryMonitorRepoURLError(err)
	}
	return owner, repo, nil
}

func repositoryMonitorRepoURLError(err error) error {
	message := strings.TrimPrefix(err.Error(), "repository URL ")
	return fmt.Errorf("spec.repoURL %s", message)
}

func repositoryMonitorPullRequestsEnabled(spec corev1alpha1.RepositoryMonitorSpec) bool {
	return spec.Targets.PullRequests.Enabled == nil || *spec.Targets.PullRequests.Enabled
}

func (h *Handlers) fetchRepositoryMonitor(ctx fiber.Ctx, namespace, name string) (*corev1alpha1.RepositoryMonitor, error) {
	monitor := &corev1alpha1.RepositoryMonitor{}
	if err := h.client.Get(ctx.Context(), types.NamespacedName{Name: name, Namespace: namespace}, monitor); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fiber.NewError(fiber.StatusNotFound, "repository monitor not found")
		}
		return nil, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get repository monitor: %v", err))
	}
	return monitor, nil
}

func effectiveRepositoryMonitorBranch(monitor *corev1alpha1.RepositoryMonitor) string {
	if monitor.Spec.Branch != "" {
		return monitor.Spec.Branch
	}
	return "main"
}

func repositoryMonitorAgentRefs(monitor *corev1alpha1.RepositoryMonitor) []corev1alpha1.AgentReference {
	if monitor == nil {
		return nil
	}
	candidates := []*corev1alpha1.AgentReference{
		monitor.Spec.Agents.Reviewer,
		monitor.Spec.Agents.Triager,
		monitor.Spec.Agents.Researcher,
		monitor.Spec.Agents.Planner,
		monitor.Spec.Agents.Repairer,
		monitor.Spec.Agents.Implementer,
	}
	refs := make([]corev1alpha1.AgentReference, 0, len(candidates))
	for _, ref := range candidates {
		if ref != nil && strings.TrimSpace(ref.Name) != "" {
			refs = append(refs, *ref)
		}
	}
	return refs
}

func contextTokenRepositoryMonitorFailures(token *ContextToken, monitor *corev1alpha1.RepositoryMonitor) []string {
	failures := []string{}
	if want, ok := contextString(token.TransactionContext, "namespace"); ok && monitor.Namespace != want {
		failures = append(failures, fmt.Sprintf("namespace %q does not match token context %q", monitor.Namespace, want))
	}
	if want, ok := contextString(token.TransactionContext, "repo"); ok && monitor.Spec.RepoURL != want {
		failures = append(failures, fmt.Sprintf("repository %q does not match token context %q", monitor.Spec.RepoURL, want))
	}
	if want, ok := contextString(token.TransactionContext, "branch"); ok && effectiveRepositoryMonitorBranch(monitor) != want {
		failures = append(failures, fmt.Sprintf("workspace branch %q does not match token context %q", effectiveRepositoryMonitorBranch(monitor), want))
	}

	refs := repositoryMonitorAgentRefs(monitor)
	allowed, hasAllowed := contextStringList(token.TransactionContext, "allowedAgents")
	if want, ok := contextString(token.TransactionContext, "agent"); ok {
		matched := false
		for _, ref := range refs {
			agentNamespace := ref.Namespace
			if agentNamespace == "" {
				agentNamespace = monitor.Namespace
			}
			if agentMatches(ref.Name, agentNamespace, want) {
				matched = true
				continue
			}
			if !hasAllowed {
				failures = append(failures, fmt.Sprintf("agent %q does not match token context %q", namespacedNameString(agentNamespace, ref.Name), want))
			}
		}
		if !matched {
			failures = append(failures, fmt.Sprintf("no repository monitor agent matches token context %q", want))
		}
	}
	if hasAllowed {
		for _, ref := range refs {
			agentNamespace := ref.Namespace
			if agentNamespace == "" {
				agentNamespace = monitor.Namespace
			}
			if !agentAllowed(ref.Name, agentNamespace, allowed) {
				failures = append(failures, fmt.Sprintf("agent %q is not allowed by token context", namespacedNameString(agentNamespace, ref.Name)))
			}
		}
	}
	return failures
}

func (h *Handlers) contextTokenRepositoryMonitorAllowed(c fiber.Ctx, monitor *corev1alpha1.RepositoryMonitor) bool {
	if !h.contextTokenAuthorization.Enabled() {
		return true
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return true
	}
	failures := contextTokenRepositoryMonitorFailures(ui.ContextToken, monitor)
	if len(failures) == 0 {
		return true
	}
	if h.contextTokenAuthorization.enforcing() {
		return false
	}
	_ = h.handleContextTokenAuthorizationFailures(ui.ContextToken, "listRepositoryMonitors", failures)
	return true
}

func (h *Handlers) authorizeContextTokenRepositoryMonitor(c fiber.Ctx, action string, monitor *corev1alpha1.RepositoryMonitor) error {
	if !h.contextTokenAuthorization.Enabled() {
		return nil
	}
	ui := GetUserInfo(c)
	if ui == nil || ui.AuthType != AuthTypeContextToken || ui.ContextToken == nil {
		return nil
	}
	failures := contextTokenRepositoryMonitorFailures(ui.ContextToken, monitor)
	if len(failures) == 0 {
		metrics.RecordContextTokenAuthorization(action, "allowed", "ok")
		return nil
	}
	return h.handleContextTokenAuthorizationFailures(ui.ContextToken, action, failures)
}

func (h *Handlers) authorizeContextTokenRepositoryMonitorCredentialSecrets(c fiber.Ctx, action, namespace string, spec corev1alpha1.RepositoryMonitorSpec) error {
	readField, readRef := repositoryMonitorEffectiveReadCredential(spec)
	refs := []struct {
		role  string
		field string
		ref   *corev1.LocalObjectReference
	}{
		{role: "sourceRead", field: readField, ref: readRef},
		{role: "publicationRead", field: "spec.publicationReadCredentialRef", ref: spec.PublicationReadCredentialRef},
		{role: "publication", field: "spec.publicationCredentialRef", ref: spec.PublicationCredentialRef},
		{role: "forge", field: "spec.forgeCredentialRef", ref: spec.ForgeCredentialRef},
	}
	for _, credential := range refs {
		name := repositoryMonitorCredentialRefName(credential.ref)
		if name == "" {
			continue
		}
		if err := h.authorizeContextTokenGitCredentialSecretName(c, action+credential.role+"Credential", namespace, name); err != nil {
			return fiber.NewError(fiber.StatusForbidden, fmt.Sprintf("%s is not authorized: %v", credential.field, err))
		}
	}
	return nil
}

// CreateRepositoryMonitor creates a new durable repository monitor.
func (h *Handlers) CreateRepositoryMonitor(c fiber.Ctx) error {
	var req CreateRepositoryMonitorRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	name := req.Name
	if name == "" {
		name = req.Metadata.Name
	}
	if name == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name is required")
	}
	namespaceValue := req.Namespace
	if namespaceValue == "" {
		namespaceValue = req.Metadata.Namespace
	}
	namespace, err := h.resolveNamespace(c, namespaceValue)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "createRepositoryMonitor", h.contextTokenAuthorization.MonitorWriteScopes); err != nil {
		return err
	}
	h.normalizeRepositoryMonitorSpec(&req.Spec)
	if err := validateRepositoryMonitorSpec(req.Spec); err != nil {
		return err
	}

	monitor := &corev1alpha1.RepositoryMonitor{
		ObjectMeta: objectMetaFromRequest(name, namespace, req.Metadata),
		Spec:       req.Spec,
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "createRepositoryMonitor", monitor); err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitorCredentialSecrets(c, "createRepositoryMonitor", namespace, req.Spec); err != nil {
		return err
	}
	if err := h.validateRepositoryMonitorReadOnlyAgents(c, namespace, req.Spec); err != nil {
		return err
	}
	if err := validateRepositoryMonitorCredentialRoleRefs(req.Spec); err != nil {
		return err
	}
	if err := h.validateRepositoryMonitorCredentialSecrets(c, namespace, req.Spec); err != nil {
		return err
	}
	if err := h.client.Create(c.Context(), monitor); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fiber.NewError(fiber.StatusConflict, "repository monitor already exists")
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create repository monitor: %v", err))
	}
	return c.Status(fiber.StatusCreated).JSON(monitor)
}

// ListRepositoryMonitors lists configured repository monitors.
func (h *Handlers) ListRepositoryMonitors(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitors", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}

	pagination, err := ParsePagination(c.Query("limit", "100"), c.Query("continue", ""))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	filteredList := false
	var remainingItemCount *int64
	items, continueToken, err := collectAuthorizedPages(pagination.Limit, pagination.Continue, func(continueToken string, pageLimit int64) ([]corev1alpha1.RepositoryMonitor, string, error) {
		list := &corev1alpha1.RepositoryMonitorList{}
		opts := &client.ListOptions{Namespace: namespace, Limit: pageLimit, Continue: continueToken}
		if err := h.listPage(c.Context(), list, opts, "repository monitors"); err != nil {
			return nil, "", err
		}
		remainingItemCount = list.RemainingItemCount
		items := list.Items
		if h.contextTokenAuthorization.Enabled() {
			filtered := make([]corev1alpha1.RepositoryMonitor, 0, len(items))
			for i := range items {
				if h.contextTokenRepositoryMonitorAllowed(c, &items[i]) {
					filtered = append(filtered, items[i])
				}
			}
			if len(filtered) != len(items) {
				filteredList = true
			}
			items = filtered
		}
		return items, list.Continue, nil
	})
	if err != nil {
		return err
	}
	if filteredList {
		// The raw count describes monitors the caller is not allowed to see.
		remainingItemCount = nil
	}
	return c.JSON(ListResponse{
		Items: items,
		Metadata: ListMeta{
			Continue:           NormalizeListContinue(continueToken),
			RemainingItemCount: remainingItemCount,
		},
	})
}

// GetRepositoryMonitor returns a repository monitor configuration.
func (h *Handlers) GetRepositoryMonitor(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitor", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitor", monitor); err != nil {
		return err
	}
	return c.JSON(monitor)
}

// UpdateRepositoryMonitor updates an existing repository monitor.
func (h *Handlers) UpdateRepositoryMonitor(c fiber.Ctx) error {
	var req UpdateRepositoryMonitorRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "updateRepositoryMonitor", h.contextTokenAuthorization.MonitorWriteScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "updateRepositoryMonitor", monitor); err != nil {
		return err
	}

	h.normalizeRepositoryMonitorSpec(&req.Spec)
	if err := validateRepositoryMonitorSpec(req.Spec); err != nil {
		return err
	}
	updated := monitor.DeepCopy()
	updated.Spec = req.Spec
	if err := h.authorizeContextTokenRepositoryMonitor(c, "updateRepositoryMonitor", updated); err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitorCredentialSecrets(c, "updateRepositoryMonitor", namespace, req.Spec); err != nil {
		return err
	}
	if err := h.validateRepositoryMonitorReadOnlyAgents(c, namespace, req.Spec); err != nil {
		return err
	}
	if err := validateRepositoryMonitorCredentialRoleRefs(req.Spec); err != nil {
		return err
	}
	if err := h.validateRepositoryMonitorCredentialSecrets(c, namespace, req.Spec); err != nil {
		return err
	}
	if err := h.client.Update(c.Context(), updated); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to update repository monitor: %v", err))
	}
	return c.JSON(updated)
}

// DeleteRepositoryMonitor deletes a repository monitor configuration.
func (h *Handlers) DeleteRepositoryMonitor(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "deleteRepositoryMonitor", h.contextTokenAuthorization.MonitorWriteScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "deleteRepositoryMonitor", monitor); err != nil {
		return err
	}
	if err := h.client.Delete(c.Context(), monitor); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to delete repository monitor: %v", err))
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// CreateRepositoryMonitorRun records a manual monitor run request.
func (h *Handlers) CreateRepositoryMonitorRun(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	var req CreateRepositoryMonitorRunRequest
	if len(c.Body()) > 0 {
		if err := c.Bind().JSON(&req); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "createRepositoryMonitorRun", h.contextTokenAuthorization.MonitorOperateScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "createRepositoryMonitorRun", monitor); err != nil {
		return err
	}
	if err := validateRepositoryMonitorRunRequest(req); err != nil {
		return err
	}
	if err := h.ensureNoActiveRepositoryMonitorRun(c, namespace, monitor.Name); err != nil {
		return err
	}

	run := &store.MonitorRun{
		ID:               fmt.Sprintf("monrun-%d", time.Now().UTC().UnixNano()),
		MonitorNamespace: namespace,
		MonitorName:      monitor.Name,
		Trigger:          "manual",
		TargetKind:       req.TargetKind,
		TargetNumber:     req.TargetNumber,
		TargetSHA:        req.TargetSHA,
		Phase:            repositoryMonitorRunPhaseQueued,
		StartedAt:        time.Now(),
	}
	if err := h.repositoryMonitorStore.CreateMonitorRun(c.Context(), run); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return fiber.NewError(fiber.StatusConflict, "repository monitor already has an active run")
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create monitor run: %v", err))
	}
	if err := h.annotateRepositoryMonitorRunRequest(c, monitor, run); err != nil {
		if failErr := h.markRepositoryMonitorRunSignalFailed(c, run, err); failErr != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("%v; additionally failed to mark monitor run failed: %v", err, failErr))
		}
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(run)
}

func (h *Handlers) markRepositoryMonitorRunSignalFailed(c fiber.Ctx, run *store.MonitorRun, signalErr error) error {
	completedAt := time.Now()
	run.Phase = repositoryMonitorRunPhaseFailed
	run.CompletedAt = &completedAt
	run.Error = signalErr.Error()
	return h.repositoryMonitorStore.UpdateMonitorRun(c.Context(), run)
}

func (h *Handlers) annotateRepositoryMonitorRunRequest(c fiber.Ctx, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun) error {
	updated := monitor.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[repositoryMonitorRunRequestAnnotation] = run.ID
	if err := h.client.Patch(c.Context(), updated, client.MergeFrom(monitor)); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to signal repository monitor run: %v", err))
	}
	return nil
}

func (h *Handlers) ensureNoActiveRepositoryMonitorRun(c fiber.Ctx, namespace, monitorName string) error {
	for _, phase := range []string{repositoryMonitorRunPhaseQueued, repositoryMonitorRunPhaseRunning} {
		runs, _, err := h.repositoryMonitorStore.ListMonitorRuns(c.Context(), store.MonitorRunFilter{
			Namespace:   namespace,
			MonitorName: monitorName,
			Phase:       phase,
			Limit:       1,
		})
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list active monitor runs: %v", err))
		}
		if len(runs) > 0 {
			return fiber.NewError(fiber.StatusConflict, "repository monitor already has an active run")
		}
	}
	return nil
}

// ListRepositoryMonitorRuns lists durable runs for a repository monitor.
func (h *Handlers) ListRepositoryMonitorRuns(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorRuns", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorRuns", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "20"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	runs, next, err := h.repositoryMonitorStore.ListMonitorRuns(c.Context(), store.MonitorRunFilter{
		Namespace:   namespace,
		MonitorName: monitor.Name,
		Trigger:     c.Query("trigger"),
		TargetKind:  c.Query("targetKind"),
		Limit:       limit,
		Cursor:      monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor runs: %v", err))
	}
	return c.JSON(fiber.Map{"items": runs, "metadata": fiber.Map{"continue": next}})
}

// ListRepositoryMonitorItems lists current items for a repository monitor.
func (h *Handlers) ListRepositoryMonitorItems(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorItems", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorItems", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	number, err := parseOptionalInt64Query(c.Query("number"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid number")
	}
	items, next, err := h.repositoryMonitorStore.ListMonitorItems(c.Context(), store.MonitorItemFilter{
		Namespace:      namespace,
		MonitorName:    monitor.Name,
		Kind:           c.Query("kind"),
		Number:         number,
		State:          c.Query("state"),
		ReviewVerdict:  c.Query("verdict"),
		RepairState:    c.Query("repairState"),
		AutomergeState: c.Query("automergeState"),
		Limit:          limit,
		Cursor:         monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor items: %v", err))
	}
	return c.JSON(fiber.Map{"items": items, "metadata": fiber.Map{"continue": next}})
}

// ListRepositoryMonitorEvents lists audit events for a repository monitor.
func (h *Handlers) ListRepositoryMonitorEvents(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorEvents", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorEvents", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	itemNumber, err := parseOptionalInt64Query(c.Query("itemNumber"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid itemNumber")
	}
	events, next, err := h.repositoryMonitorStore.ListMonitorEvents(c.Context(), store.MonitorEventFilter{
		Namespace:   namespace,
		MonitorName: monitor.Name,
		RunID:       c.Query("runID"),
		ItemKind:    c.Query("itemKind"),
		ItemNumber:  itemNumber,
		EventType:   c.Query("eventType"),
		Limit:       limit,
		Cursor:      monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor events: %v", err))
	}
	return c.JSON(fiber.Map{"items": events, "metadata": fiber.Map{"continue": next}})
}

func monitorListCursor(c fiber.Ctx) string {
	if cursor := c.Query("continue"); cursor != "" {
		return cursor
	}
	return c.Query("cursor")
}

func parseOptionalInt64Query(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

// CreateRepositoryMonitorCommandEvent records an API-created monitor command and best-effort queues its run.
//
//nolint:gocyclo // Command validation is intentionally linear and auditable.
func (h *Handlers) CreateRepositoryMonitorCommandEvent(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	var req CreateRepositoryMonitorCommandRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "createRepositoryMonitorCommandEvent", h.contextTokenAuthorization.MonitorOperateScopes); err != nil {
		return err
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, c.Params("name"))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "createRepositoryMonitorCommandEvent", monitor); err != nil {
		return err
	}
	req.Intent = strings.TrimSpace(req.Intent)
	req.TargetSHA = strings.TrimSpace(req.TargetSHA)
	if req.Kind == repositoryMonitorTargetKindIssue && strings.TrimSpace(req.TargetSHA) != "" {
		return fiber.NewError(fiber.StatusBadRequest, "targetSHA is only supported for pull_request commands")
	}
	if err := validateRepositoryMonitorCommandRequest(req); err != nil {
		return err
	}
	if repositoryMonitorCommandRequiresWrite(req) || (req.Kind == repositoryMonitorTargetKindPullRequest && req.Intent == githubActionReview && monitor.Spec.Review.Publish.Enabled) {
		if err := h.authorizeContextTokenAction(c, "createRepositoryMonitorMutatingCommand", h.contextTokenAuthorization.MonitorWriteScopes); err != nil {
			return err
		}
	}
	if err := validateRepositoryMonitorCommandTargetEnabled(monitor.Spec, req.Kind); err != nil {
		return err
	}
	item, err := h.repositoryMonitorStore.GetMonitorItem(c.Context(), namespace, monitor.Name, req.Kind, strconv.FormatInt(req.Number, 10))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to inspect monitor target: %v", err))
	}
	if item == nil && repositoryMonitorCommandRequiresInventoriedTarget(req) {
		return fiber.NewError(fiber.StatusBadRequest, "command target must be present in monitor inventory before this intent can be queued")
	}
	if repositoryMonitorPullRequestCommandRequiresTargetSHA(req) && req.TargetSHA == "" {
		return fiber.NewError(fiber.StatusBadRequest, "targetSHA is required for head-bound pull_request commands")
	}
	if item != nil && !repositoryMonitorControlCommandIntent(req.Intent) && !strings.EqualFold(strings.TrimSpace(item.State), "open") {
		return fiber.NewError(fiber.StatusBadRequest, "command target must be open")
	}
	if item != nil && req.Kind == repositoryMonitorTargetKindIssue && !repositoryMonitorControlCommandIntent(req.Intent) && !repositoryMonitorWebhookIssueTargetLabelsAllowed(monitor.Spec, repositoryMonitorLabelsFromItem(item)) {
		return fiber.NewError(fiber.StatusBadRequest, "command target is outside issue label scope")
	}
	if item != nil && req.Kind == repositoryMonitorTargetKindPullRequest && req.TargetSHA != "" && req.TargetSHA != item.HeadSHA {
		return fiber.NewError(fiber.StatusBadRequest, "targetSHA must match current pull request head")
	}
	snapshot := ""
	if item != nil && req.Kind == repositoryMonitorTargetKindIssue {
		snapshot = item.SnapshotDigest
	}
	status := githubCommandStatusAccepted
	errorMessage := ""
	if item != nil && !repositoryMonitorControlCommandIntent(req.Intent) {
		if guard := repositoryMonitorCommandGuardLabel(monitor, repositoryMonitorLabelsFromItem(item)); guard != "" {
			status = githubCommandStatusBlocked
			errorMessage = fmt.Sprintf("target has guard label %q", guard)
		}
	}
	repo := monitor.Spec.Owner + "/" + monitor.Spec.Repository
	if repo == "/" {
		if owner, repository, err := parseRepositoryMonitorGitHubURL(monitor.Spec.RepoURL); err == nil {
			repo = owner + "/" + repository
		}
	}
	now := time.Now()
	id := fmt.Sprintf("cmd-api-%d", now.UTC().UnixNano())
	event := &store.CommandEvent{
		ID:                  id,
		MonitorNamespace:    namespace,
		MonitorName:         monitor.Name,
		Repo:                repo,
		Kind:                req.Kind,
		Number:              req.Number,
		Source:              "api",
		MonitorGeneration:   monitor.Generation,
		DedupeKey:           id,
		IdempotencyKey:      id,
		Author:              "orka-api",
		Permission:          repositoryMonitorAPICommandPermission(req),
		Command:             req.Intent,
		Intent:              req.Intent,
		HeadSHA:             req.TargetSHA,
		IssueSnapshotDigest: snapshot,
		Status:              status,
		CreatedAt:           now,
		ProcessedAt:         &now,
		Error:               errorMessage,
	}
	if err := h.repositoryMonitorStore.CreateCommandEvent(c.Context(), event); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create monitor command: %v", err))
	}
	metrics.RecordRepositoryMonitorCommand(event.Intent, event.Status)
	if event.Status != githubCommandStatusAccepted {
		if err := h.upsertRepositoryMonitorCommandWorkAction(c.Context(), monitor, event, ""); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create monitor workflow action: %v", err))
		}
		return c.Status(fiber.StatusCreated).JSON(event)
	}
	target := githubLabelTarget{Kind: req.Kind, Number: int(req.Number), HeadSHA: req.TargetSHA}
	if _, _, err := h.queueRepositoryMonitorCommandRun(c, monitor, event, target); err != nil {
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(event)
}

func repositoryMonitorCommandRequiresInventoriedTarget(req CreateRepositoryMonitorCommandRequest) bool {
	return !repositoryMonitorControlCommandIntent(req.Intent)
}

func repositoryMonitorPullRequestCommandRequiresTargetSHA(req CreateRepositoryMonitorCommandRequest) bool {
	if req.Kind != repositoryMonitorTargetKindPullRequest {
		return false
	}
	switch strings.TrimSpace(req.Intent) {
	case finishReasonStop, commandIntentResume:
		return false
	default:
		return true
	}
}

func repositoryMonitorLabelsFromItem(item *store.MonitorItem) []string {
	if item == nil || strings.TrimSpace(item.LabelsJSON) == "" {
		return nil
	}
	var labels []string
	if err := json.Unmarshal([]byte(item.LabelsJSON), &labels); err == nil {
		return labels
	}
	return nil
}

func repositoryMonitorCommandRequiresWrite(req CreateRepositoryMonitorCommandRequest) bool {
	switch req.Intent {
	case commandIntentApprovePlan, commandIntentStop, commandIntentResume, "implement", commandIntentDecompose, "fix", commandIntentFixCI, commandIntentUpdateBranch, repositoryMonitorIntentAutomerge:
		return true
	default:
		return false
	}
}

func repositoryMonitorAPICommandPermission(req CreateRepositoryMonitorCommandRequest) string {
	if req.Kind == repositoryMonitorTargetKindPullRequest && req.Intent == repositoryMonitorIntentAutomerge {
		return "orka:monitors:write"
	}
	return "orka:monitors:operate"
}

func validateRepositoryMonitorCommandTargetEnabled(spec corev1alpha1.RepositoryMonitorSpec, kind string) error {
	switch kind {
	case repositoryMonitorTargetKindIssue:
		if spec.Targets.Issues.Enabled {
			return nil
		}
	case repositoryMonitorTargetKindPullRequest:
		if repositoryMonitorPullRequestsEnabled(spec) {
			return nil
		}
	}
	return fiber.NewError(fiber.StatusBadRequest, "command target kind is not enabled for this monitor")
}

func validateRepositoryMonitorCommandRequest(req CreateRepositoryMonitorCommandRequest) error {
	if req.Kind != repositoryMonitorTargetKindIssue && req.Kind != repositoryMonitorTargetKindPullRequest {
		return fiber.NewError(fiber.StatusBadRequest, "kind must be issue or pull_request")
	}
	if req.Number <= 0 {
		return fiber.NewError(fiber.StatusBadRequest, "number is required")
	}
	intent := strings.TrimSpace(req.Intent)
	switch req.Kind {
	case repositoryMonitorTargetKindIssue:
		switch intent {
		case "triage", "research", "plan", "approve_plan", "implement", commandIntentDecompose, finishReasonStop, "resume":
			return nil
		}
	case repositoryMonitorTargetKindPullRequest:
		switch intent {
		case "review", "fix", commandIntentFixCI, commandIntentUpdateBranch, repositoryMonitorIntentAutomerge, finishReasonStop, "resume":
			return nil
		}
	}
	return fiber.NewError(fiber.StatusBadRequest, "unsupported command intent for target kind")
}

// ListRepositoryMonitorCommandEvents lists durable label/API command events.
func (h *Handlers) ListRepositoryMonitorCommandEvents(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorCommandEvents", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorCommandEvents", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	number, err := parseOptionalInt64Query(c.Query("number"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid number")
	}
	events, next, err := h.repositoryMonitorStore.ListCommandEvents(c.Context(), store.CommandEventFilter{
		Namespace:   namespace,
		MonitorName: monitor.Name,
		Kind:        c.Query("kind"),
		Number:      number,
		Intent:      c.Query("intent"),
		Status:      c.Query("status"),
		Limit:       limit,
		Cursor:      monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor commands: %v", err))
	}
	return c.JSON(fiber.Map{"items": events, "metadata": fiber.Map{"continue": next}})
}

// GetRepositoryMonitorCommandEvent fetches one durable command event.
func (h *Handlers) GetRepositoryMonitorCommandEvent(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorCommandEvent", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	event, err := h.repositoryMonitorStore.GetCommandEvent(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor command not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor command: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, event.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorCommandEvent", monitor); err != nil {
		return err
	}
	return c.JSON(event)
}

// ListRepositoryMonitorActionRecords lists durable monitor action records.
func (h *Handlers) ListRepositoryMonitorActionRecords(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorActionRecords", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorActionRecords", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	number, err := parseOptionalInt64Query(c.Query("number"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid number")
	}
	records, next, err := h.repositoryMonitorStore.ListActionRecords(c.Context(), store.ActionRecordFilter{
		Namespace:   namespace,
		MonitorName: monitor.Name,
		Kind:        c.Query("kind"),
		Number:      number,
		ActionKind:  c.Query("actionKind"),
		TaskName:    c.Query("taskName"),
		Limit:       limit,
		Cursor:      monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor actions: %v", err))
	}
	return c.JSON(fiber.Map{"items": records, "metadata": fiber.Map{"continue": next}})
}

// GetRepositoryMonitorActionRecord fetches one durable action record.
func (h *Handlers) GetRepositoryMonitorActionRecord(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorActionRecord", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	record, err := h.repositoryMonitorStore.GetActionRecord(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor action not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor action: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, record.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorActionRecord", monitor); err != nil {
		return err
	}
	return c.JSON(record)
}

// ListRepositoryMonitorWorkActions lists durable workflow queue actions.
func (h *Handlers) ListRepositoryMonitorWorkActions(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorWorkActions", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorWorkActions", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	number, err := parseOptionalInt64Query(c.Query("number"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid number")
	}
	actions, next, err := h.repositoryMonitorStore.ListWorkActions(c.Context(), store.WorkActionFilter{
		Namespace:      namespace,
		MonitorName:    monitor.Name,
		TargetKind:     c.Query("kind"),
		TargetNumber:   number,
		TargetSHA:      c.Query("targetSHA"),
		Intent:         c.Query("intent"),
		DesiredAction:  c.Query("desiredAction"),
		Status:         c.Query("status"),
		RunID:          c.Query("runID"),
		CommandEventID: c.Query("commandEventID"),
		TaskName:       c.Query("taskName"),
		Limit:          limit,
		Cursor:         monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor workflow actions: %v", err))
	}
	return c.JSON(fiber.Map{"items": actions, "metadata": fiber.Map{"continue": next}})
}

// GetRepositoryMonitorWorkAction fetches one durable workflow action.
func (h *Handlers) GetRepositoryMonitorWorkAction(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorWorkAction", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	action, err := h.repositoryMonitorStore.GetWorkAction(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor workflow action not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor workflow action: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, action.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorWorkAction", monitor); err != nil {
		return err
	}
	return c.JSON(action)
}

// ListRepositoryMonitorImplementationJobs lists durable issue implementation jobs.
func (h *Handlers) ListRepositoryMonitorImplementationJobs(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorImplementationJobs", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorImplementationJobs", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	issueNumber, err := parseOptionalInt64Query(c.Query("issueNumber", c.Query("number")))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid issue number")
	}
	jobs, next, err := h.repositoryMonitorStore.ListImplementationJobs(c.Context(), store.ImplementationJobFilter{
		Namespace:   namespace,
		MonitorName: monitor.Name,
		Repo:        c.Query("repo"),
		IssueNumber: issueNumber,
		Phase:       c.Query("phase"),
		TaskName:    c.Query("taskName"),
		Limit:       limit,
		Cursor:      monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor implementation jobs: %v", err))
	}
	return c.JSON(fiber.Map{"items": jobs, "metadata": fiber.Map{"continue": next}})
}

// GetRepositoryMonitorImplementationJob fetches one implementation job.
func (h *Handlers) GetRepositoryMonitorImplementationJob(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorImplementationJob", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	job, err := h.repositoryMonitorStore.GetImplementationJob(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor implementation job not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor implementation job: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, job.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorImplementationJob", monitor); err != nil {
		return err
	}
	return c.JSON(job)
}

// ListRepositoryMonitorGitHubMutations lists controller-owned GitHub mutation audit records.
func (h *Handlers) ListRepositoryMonitorGitHubMutations(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listRepositoryMonitorGitHubMutations", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	monitorName := c.Query("name")
	if monitorName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name query parameter is required")
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, monitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "listRepositoryMonitorGitHubMutations", monitor); err != nil {
		return err
	}
	limit, err := strconv.Atoi(c.Query("limit", "50"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	number, err := parseOptionalInt64Query(c.Query("number"))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid number")
	}
	records, next, err := h.repositoryMonitorStore.ListGitHubMutationRecords(c.Context(), store.GitHubMutationRecordFilter{
		Namespace:    namespace,
		MonitorName:  monitor.Name,
		Operation:    c.Query("operation"),
		TargetKind:   c.Query("kind"),
		TargetNumber: number,
		Status:       c.Query("status"),
		Limit:        limit,
		Cursor:       monitorListCursor(c),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list monitor GitHub mutations: %v", err))
	}
	return c.JSON(fiber.Map{"items": records, "metadata": fiber.Map{"continue": next}})
}

// GetRepositoryMonitorGitHubMutation fetches one mutation audit record.
func (h *Handlers) GetRepositoryMonitorGitHubMutation(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorGitHubMutation", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	record, err := h.repositoryMonitorStore.GetGitHubMutationRecord(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor GitHub mutation not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor GitHub mutation: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, record.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorGitHubMutation", monitor); err != nil {
		return err
	}
	return c.JSON(record)
}

// GetRepositoryMonitorImplementationPatchPreview returns safe patch artifact metadata for an implementation job.
func (h *Handlers) GetRepositoryMonitorImplementationPatchPreview(c fiber.Ctx) error {
	if err := h.ensureRepositoryMonitorStore(); err != nil {
		return err
	}
	if h.artifactStore == nil {
		return fiber.NewError(fiber.StatusNotImplemented, "artifact store not configured")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getRepositoryMonitorImplementationPatchPreview", h.contextTokenAuthorization.MonitorReadScopes); err != nil {
		return err
	}
	job, err := h.repositoryMonitorStore.GetImplementationJob(c.Context(), namespace, c.Params("id"))
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor implementation job not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get monitor implementation job: %v", err))
	}
	monitor, err := h.fetchRepositoryMonitor(c, namespace, job.MonitorName)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenRepositoryMonitor(c, "getRepositoryMonitorImplementationPatchPreview", monitor); err != nil {
		return err
	}
	if strings.TrimSpace(job.TaskName) == "" || strings.TrimSpace(job.PatchArtifactID) == "" {
		return fiber.NewError(fiber.StatusNotFound, "monitor implementation job has no patch artifact")
	}
	data, contentType, err := h.artifactStore.GetArtifact(c.Context(), namespace, job.TaskName, job.PatchArtifactID)
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, "monitor implementation patch artifact not found")
	}
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to read monitor implementation patch artifact: %v", err))
	}
	var patch any = string(data)
	if json.Valid(data) {
		var parsed any
		if err := json.Unmarshal(data, &parsed); err == nil {
			patch = parsed
		}
	}
	return c.JSON(fiber.Map{"job": job, "patch": patch, "contentType": contentType})
}
