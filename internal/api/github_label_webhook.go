/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/agentruntimepolicy"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	githubWebhookSecretEnv                     = "ORKA_GITHUB_WEBHOOK_SECRET"
	githubLabelTriggerAgentEnv                 = "ORKA_GITHUB_LABEL_TRIGGER_AGENT"
	githubLabelTriggerNamespaceEnv             = "ORKA_GITHUB_LABEL_TRIGGER_NAMESPACE"
	githubLabelTriggerGitSecretEnv             = "ORKA_GITHUB_LABEL_TRIGGER_GIT_SECRET"
	githubLabelTriggerPrefixEnv                = "ORKA_GITHUB_LABEL_TRIGGER_PREFIX"
	githubLabelTriggerTimeoutEnv               = "ORKA_GITHUB_LABEL_TRIGGER_TIMEOUT"
	githubLabelTriggerMaxTurnsEnv              = "ORKA_GITHUB_LABEL_TRIGGER_MAX_TURNS"
	githubLabelTriggerDefaultPrefix            = "agent:"
	githubDeliveryHeader                       = "X-GitHub-Delivery"
	githubEventHeader                          = "X-GitHub-Event"
	githubSignature256Header                   = "X-Hub-Signature-256"
	githubSignature256Prefix                   = "sha256="
	githubEventIssues                          = "issues"
	githubEventPullRequest                     = "pull_request"
	githubEventPing                            = "ping"
	githubWebhookCreatedBy                     = "github-label"
	githubActionImplement                      = "implement"
	githubActionReview                         = "review"
	githubActionToIssues                       = "to-issues"
	githubActionUpdateBranch                   = "update-branch"
	githubMonitorTriggerPullRequestEvent       = "pull_request_event"
	githubMonitorTriggerLabelCommand           = "github_label_command"
	githubCommandEventSourceLabel              = "github_label"
	githubCommandStatusAccepted                = "accepted"
	githubCommandStatusRejected                = "rejected"
	githubCommandStatusBlocked                 = "blocked"
	githubCommandStatusCompleted               = "completed"
	githubCommandStatusProcessed               = "processed"
	githubMonitorEventTypeExactRunQueued       = "exact_event_run_queued"
	githubWebhookDefaultTimeout                = 30 * time.Minute
	githubWebhookDefaultMaxTurns         int32 = 100
)

var nonDNSNameCharRE = regexp.MustCompile(`[^a-z0-9-]+`)

type githubLabelWebhookPayload struct {
	Action      string                    `json:"action"`
	Label       githubWebhookLabel        `json:"label"`
	Repository  githubWebhookRepository   `json:"repository"`
	Issue       *githubWebhookIssue       `json:"issue,omitempty"`
	PullRequest *githubWebhookPullRequest `json:"pull_request,omitempty"`
	Sender      githubWebhookUser         `json:"sender"`
}

type githubWebhookLabel struct {
	Name string `json:"name"`
}

type githubWebhookUser struct {
	Login string `json:"login"`
}

type githubWebhookRepository struct {
	FullName      string `json:"full_name"`
	HTMLURL       string `json:"html_url"`
	CloneURL      string `json:"clone_url"`
	DefaultBranch string `json:"default_branch"`
}

type githubWebhookIssue struct {
	Number      int                       `json:"number"`
	Title       string                    `json:"title"`
	Body        string                    `json:"body"`
	HTMLURL     string                    `json:"html_url"`
	State       string                    `json:"state"`
	UpdatedAt   time.Time                 `json:"updated_at"`
	PullRequest *githubIssuePullRequestID `json:"pull_request,omitempty"`
	Labels      []githubWebhookLabel      `json:"labels"`
}

type githubIssuePullRequestID struct {
	HTMLURL string `json:"html_url"`
}

type githubWebhookPullRequest struct {
	Number  int    `json:"number"`
	Title   string `json:"title"`
	Body    string `json:"body"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Draft   bool   `json:"draft"`
	Base    struct {
		Ref  string                  `json:"ref"`
		SHA  string                  `json:"sha"`
		Repo githubWebhookRepository `json:"repo"`
	} `json:"base"`
	Head struct {
		Ref  string                  `json:"ref"`
		SHA  string                  `json:"sha"`
		Repo githubWebhookRepository `json:"repo"`
	} `json:"head"`
	Labels []githubWebhookLabel `json:"labels"`
}

type githubLabelTarget struct {
	Kind         string
	Number       int
	Title        string
	Body         string
	HTMLURL      string
	State        string
	IsPR         bool
	IncompletePR bool
	Draft        bool
	BaseBranch   string
	BaseSHA      string
	HeadBranch   string
	HeadSHA      string
	Repo         githubWebhookRepository
	BaseRepo     githubWebhookRepository
	HeadRepo     githubWebhookRepository
	Labels       []string
	UpdatedAt    time.Time
}

type githubRepositoryMonitorEventResult struct {
	Matched       int
	Queued        int
	Duplicate     int
	SkippedActive int
	RunIDs        []string
	CommandIDs    []string
}

//nolint:gocyclo // GitHub webhook routing is intentionally linear across event families.
func (h *Handlers) HandleGitHubWebhook(c fiber.Ctx) error {
	body := append([]byte(nil), c.Body()...)
	secret := strings.TrimSpace(os.Getenv(githubWebhookSecretEnv))
	if secret == "" {
		return fiber.NewError(fiber.StatusServiceUnavailable, "GitHub webhook secret is not configured")
	}
	if !validGitHubSignature(body, c.Get(githubSignature256Header), secret) {
		return fiber.NewError(fiber.StatusUnauthorized, "invalid GitHub webhook signature")
	}

	event := c.Get(githubEventHeader)
	if event == githubEventPing {
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
			"status":  "ok",
			"message": "GitHub webhook signature verified",
		})
	}
	if event != githubEventIssues && event != githubEventPullRequest {
		return githubWebhookIgnored(c, fmt.Sprintf("unsupported GitHub event %q", event))
	}

	var payload githubLabelWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid GitHub webhook payload")
	}

	var monitorResult githubRepositoryMonitorEventResult
	if payload.Action != "labeled" {
		if event == githubEventPullRequest {
			var err error
			delivery := strings.TrimSpace(c.Get(githubDeliveryHeader))
			monitorResult, err = h.enqueueRepositoryMonitorPullRequestEventRuns(c, body, delivery, payload)
			if err != nil {
				return err
			}
		}
		if monitorResult.Matched > 0 {
			return githubRepositoryMonitorEventResponse(c, monitorResult)
		}
		return githubWebhookIgnored(c, fmt.Sprintf("ignored action %q", payload.Action))
	}

	target, ok := payload.target()
	if !ok {
		return fiber.NewError(fiber.StatusBadRequest, "GitHub webhook payload has no issue or pull request target")
	}
	if target.IncompletePR {
		return githubWebhookIgnored(c, "issues webhook payload for pull request lacks base/head details; configure pull_request events for PR labels")
	}

	commandResult, handledCommand, err := h.handleRepositoryMonitorLabelCommand(c, body, strings.TrimSpace(c.Get(githubDeliveryHeader)), payload, target)
	if err != nil {
		return err
	}
	if handledCommand {
		if event == githubEventPullRequest {
			var err error
			delivery := strings.TrimSpace(c.Get(githubDeliveryHeader))
			monitorResult, err = h.enqueueRepositoryMonitorPullRequestEventRuns(c, body, delivery, payload)
			if err != nil {
				return err
			}
		}
		return githubRepositoryMonitorEventResponse(c, mergeGitHubRepositoryMonitorEventResults(monitorResult, commandResult))
	}
	if event == githubEventPullRequest && !handledCommand {
		var err error
		delivery := strings.TrimSpace(c.Get(githubDeliveryHeader))
		monitorResult, err = h.enqueueRepositoryMonitorPullRequestEventRuns(c, body, delivery, payload)
		if err != nil {
			return err
		}
	}

	action, ok := githubLabelAction(payload.Label.Name)
	if !ok {
		if monitorResult.Matched > 0 {
			return githubRepositoryMonitorEventResponse(c, monitorResult)
		}
		return githubWebhookIgnored(c, "label is not an Orka agent trigger")
	}
	if actionRequiresPullRequest(action) && !target.IsPR {
		return githubWebhookIgnored(c, fmt.Sprintf("agent:%s requires a pull request target", action))
	}

	namespace := h.githubWebhookNamespace()
	agentName := githubAgentForAction(action)
	if agentName == "" {
		if monitorResult.Matched > 0 {
			return githubRepositoryMonitorEventResponse(c, monitorResult)
		}
		return fiber.NewError(fiber.StatusServiceUnavailable, "GitHub label trigger agent is not configured")
	}
	runtimePolicy, err := h.ensureAgentExists(c, namespace, agentName)
	if err != nil {
		if monitorResult.Matched > 0 && githubWebhookAgentNotFound(err) {
			return githubRepositoryMonitorEventResponse(c, monitorResult)
		}
		return err
	}
	if err := h.validateGitHubLabelTaskGitSecret(c, namespace, target); err != nil {
		return err
	}

	replayKey := githubWebhookReplayKey(body)
	delivery := strings.TrimSpace(c.Get(githubDeliveryHeader))
	if delivery == "" {
		delivery = githubReplayKeySuffix(replayKey)
	}

	maxTurns := githubMaxTurns()
	if runtimePolicy != nil {
		maxTurns = nil
	}
	task := buildGitHubLabelTask(namespace, agentName, action, replayKey, delivery, event, payload, target, maxTurns)
	if err := agentruntimepolicy.MaterializeRuntimeRefAllowedTools(task, runtimePolicy); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to apply AgentRuntime policy: %v", err))
	}
	if err := h.client.Create(c.Context(), task); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
				"status":    "duplicate",
				"message":   "task already exists for this GitHub webhook payload",
				"taskName":  task.Name,
				"namespace": task.Namespace,
				"action":    action,
			})
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create task: %v", err))
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status":             "created",
		"taskName":           task.Name,
		"namespace":          task.Namespace,
		"action":             action,
		"label":              payload.Label.Name,
		"monitorRunsQueued":  monitorResult.Queued,
		"monitorRunsSkipped": monitorResult.SkippedActive,
	})
}

func (h *Handlers) enqueueRepositoryMonitorPullRequestEventRuns(c fiber.Ctx, body []byte, delivery string, payload githubLabelWebhookPayload) (githubRepositoryMonitorEventResult, error) {
	var result githubRepositoryMonitorEventResult
	if h.repositoryMonitorStore == nil || !repositoryMonitorExactPullRequestAction(payload.Action) || payload.PullRequest == nil {
		return result, nil
	}
	target, ok := payload.target()
	if !ok || !target.IsPR || target.IncompletePR || target.HeadSHA == "" {
		return result, nil
	}

	monitors := &corev1alpha1.RepositoryMonitorList{}
	if err := h.client.List(c.Context(), monitors, h.githubWebhookMonitorListOptions()...); err != nil {
		return result, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to list repository monitors: %v", err))
	}

	deliveryKey := strings.TrimSpace(delivery)
	if deliveryKey == "" {
		deliveryKey = githubWebhookReplayKey(body)
	}
	for i := range monitors.Items {
		monitor := &monitors.Items[i]
		if intent, isCommand := repositoryMonitorCommandIntentForLabel(monitor, target, payload.Label.Name); isCommand && repositoryMonitorAcceptsLabelCommand(monitor, payload.Repository, target, intent) {
			continue
		}
		if !repositoryMonitorAcceptsPullRequestEvent(monitor, payload.Repository, target) {
			continue
		}
		result.Matched++
		runID := githubRepositoryMonitorExactRunID(monitor, deliveryKey)
		run := &store.MonitorRun{
			ID:               runID,
			MonitorNamespace: monitor.Namespace,
			MonitorName:      monitor.Name,
			Trigger:          githubMonitorTriggerPullRequestEvent,
			TargetKind:       repositoryMonitorTargetKindPullRequest,
			TargetNumber:     int64(target.Number),
			TargetSHA:        target.HeadSHA,
			Phase:            repositoryMonitorRunPhaseQueued,
			StartedAt:        time.Now(),
		}
		duplicate, err := h.queuedRepositoryMonitorPullRequestEventRunExists(c, monitor, run)
		if err != nil {
			return result, err
		}
		if duplicate {
			result.Duplicate++
			continue
		}
		if err := h.repositoryMonitorStore.CreateMonitorRun(c.Context(), run); err != nil {
			if errors.Is(err, store.ErrConflict) {
				retryQueued, retryErr := h.requeueFailedRepositoryMonitorEventRun(c, run)
				if retryErr != nil {
					return result, retryErr
				}
				if !retryQueued {
					result.SkippedActive++
					continue
				}
			} else {
				return result, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to create repository monitor event run: %v", err))
			}
		}
		if err := h.annotateRepositoryMonitorRunRequest(c, monitor, run); err != nil {
			if failErr := h.markRepositoryMonitorRunSignalFailed(c, run, err); failErr != nil {
				return result, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("%v; additionally failed to mark monitor run failed: %v", err, failErr))
			}
			return result, err
		}
		if err := h.createRepositoryMonitorEventRunAudit(c, monitor, run, payload, target, deliveryKey, githubMonitorEventTypeExactRunQueued); err != nil {
			return result, err
		}
		result.Queued++
		result.RunIDs = append(result.RunIDs, runID)
	}
	return result, nil
}

func (h *Handlers) githubWebhookMonitorListOptions() []crclient.ListOption {
	if h.watchNamespace == "" {
		return nil
	}
	return []crclient.ListOption{crclient.InNamespace(h.watchNamespace)}
}

func (h *Handlers) requeueFailedRepositoryMonitorEventRun(c fiber.Ctx, next *store.MonitorRun) (bool, error) {
	existing, err := h.repositoryMonitorStore.GetMonitorRun(c.Context(), next.MonitorNamespace, next.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to inspect repository monitor event run: %v", err))
	}
	if existing.Phase != repositoryMonitorRunPhaseFailed {
		return false, nil
	}
	events, _, err := h.repositoryMonitorStore.ListMonitorEvents(c.Context(), store.MonitorEventFilter{
		Namespace:   next.MonitorNamespace,
		MonitorName: next.MonitorName,
		RunID:       next.ID,
		EventType:   githubMonitorEventTypeExactRunQueued,
		Limit:       1,
	})
	if err != nil {
		return false, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to inspect repository monitor event run audit: %v", err))
	}
	if len(events) > 0 {
		return false, nil
	}
	existing.Trigger = next.Trigger
	existing.TargetKind = next.TargetKind
	existing.TargetNumber = next.TargetNumber
	existing.TargetSHA = next.TargetSHA
	existing.Phase = repositoryMonitorRunPhaseQueued
	existing.StartedAt = next.StartedAt
	existing.CompletedAt = nil
	existing.Error = ""
	if err := h.repositoryMonitorStore.UpdateMonitorRun(c.Context(), existing); err != nil {
		return false, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to requeue repository monitor event run: %v", err))
	}
	return true, nil
}

func (h *Handlers) queuedRepositoryMonitorPullRequestEventRunExists(c fiber.Ctx, monitor *corev1alpha1.RepositoryMonitor, next *store.MonitorRun) (bool, error) {
	runs, _, err := h.repositoryMonitorStore.ListMonitorRuns(c.Context(), store.MonitorRunFilter{
		Namespace:    monitor.Namespace,
		MonitorName:  monitor.Name,
		Trigger:      githubMonitorTriggerPullRequestEvent,
		TargetKind:   repositoryMonitorTargetKindPullRequest,
		TargetNumber: next.TargetNumber,
		TargetSHA:    next.TargetSHA,
		Phase:        repositoryMonitorRunPhaseQueued,
		Limit:        1,
	})
	if err != nil {
		return false, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to inspect active repository monitor run: %v", err))
	}
	return len(runs) > 0, nil
}

func repositoryMonitorExactPullRequestAction(action string) bool {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "opened", "reopened", "synchronize", "ready_for_review", "labeled", "unlabeled":
		return true
	default:
		return false
	}
}

func repositoryMonitorAcceptsPullRequestEvent(monitor *corev1alpha1.RepositoryMonitor, repo githubWebhookRepository, target githubLabelTarget) bool {
	if monitor == nil || repositoryMonitorWebhookSuspended(monitor) || !monitor.Spec.Review.ExactEventEnabled || !repositoryMonitorPullRequestsEnabled(monitor.Spec) {
		return false
	}
	if target.BaseBranch != "" && !strings.EqualFold(target.BaseBranch, effectiveRepositoryMonitorBranch(monitor)) {
		return false
	}
	owner, repository, err := parseRepositoryMonitorGitHubURL(monitor.Spec.RepoURL)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(repo.FullName), owner+"/"+repository)
}

func repositoryMonitorWebhookSuspended(monitor *corev1alpha1.RepositoryMonitor) bool {
	return monitor.Spec.Suspend != nil && *monitor.Spec.Suspend
}

func githubRepositoryMonitorExactRunID(monitor *corev1alpha1.RepositoryMonitor, delivery string) string {
	monitorKey := monitor.Namespace + "/" + monitor.Name + "/" + delivery
	return fmt.Sprintf("monrun-%s-%s", dnsNamePart(monitor.Name), githubReplayKeySuffix(githubWebhookReplayKey([]byte(monitorKey))))
}

func (h *Handlers) createRepositoryMonitorEventRunAudit(c fiber.Ctx, monitor *corev1alpha1.RepositoryMonitor, run *store.MonitorRun, payload githubLabelWebhookPayload, target githubLabelTarget, delivery, eventType string) error {
	eventKey := githubWebhookReplayKey([]byte(eventType + "-" + delivery))
	metadataJSON, err := json.Marshal(map[string]any{
		"action":     payload.Action,
		"delivery":   delivery,
		"repository": payload.Repository.FullName,
		"sender":     payload.Sender.Login,
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to encode monitor event metadata: %v", err))
	}
	if err := h.repositoryMonitorStore.CreateMonitorEvent(c.Context(), &store.MonitorEvent{
		ID:               fmt.Sprintf("mevt-%s-%s", run.ID, githubReplayKeySuffix(eventKey)),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		RunID:            run.ID,
		ItemKind:         repositoryMonitorTargetKindPullRequest,
		ItemNumber:       int64(target.Number),
		ItemSHA:          target.HeadSHA,
		EventType:        eventType,
		Actor:            "github-webhook",
		Summary:          fmt.Sprintf("Exact pull request event queued repository monitor run for PR #%d", target.Number),
		MetadataJSON:     string(metadataJSON),
	}); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to record repository monitor event run audit: %v", err))
	}
	return nil
}

func githubRepositoryMonitorEventResponse(c fiber.Ctx, result githubRepositoryMonitorEventResult) error {
	status := fiber.StatusAccepted
	responseStatus := "accepted"
	if result.Queued > 0 {
		status = fiber.StatusCreated
		responseStatus = "created"
	}
	return c.Status(status).JSON(fiber.Map{
		"status":             responseStatus,
		"monitorRunsQueued":  result.Queued,
		"monitorRunsCached":  result.Duplicate,
		"monitorRunsSkipped": result.SkippedActive,
		"matchedMonitors":    result.Matched,
		"runIDs":             result.RunIDs,
		"commandIDs":         result.CommandIDs,
	})
}

func mergeGitHubRepositoryMonitorEventResults(a, b githubRepositoryMonitorEventResult) githubRepositoryMonitorEventResult {
	return githubRepositoryMonitorEventResult{
		Matched:       a.Matched + b.Matched,
		Queued:        a.Queued + b.Queued,
		Duplicate:     a.Duplicate + b.Duplicate,
		SkippedActive: a.SkippedActive + b.SkippedActive,
		RunIDs:        append(append([]string{}, a.RunIDs...), b.RunIDs...),
		CommandIDs:    append(append([]string{}, a.CommandIDs...), b.CommandIDs...),
	}
}

func validGitHubSignature(body []byte, signatureHeader, secret string) bool {
	if !strings.HasPrefix(signatureHeader, githubSignature256Prefix) {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(signatureHeader, githubSignature256Prefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	want := mac.Sum(nil)
	return hmac.Equal(got, want)
}

func githubHash(body []byte) []byte {
	sum := sha256.Sum256(body)
	return sum[:]
}

func githubWebhookReplayKey(body []byte) string {
	return hex.EncodeToString(githubHash(body))
}

func githubWebhookIgnored(c fiber.Ctx, reason string) error {
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status": "ignored",
		"reason": reason,
	})
}

func githubLabelAction(labelName string) (string, bool) {
	prefix := strings.TrimSpace(os.Getenv(githubLabelTriggerPrefixEnv))
	if prefix == "" {
		prefix = githubLabelTriggerDefaultPrefix
	}

	labelName = strings.TrimSpace(labelName)
	if !strings.HasPrefix(strings.ToLower(labelName), strings.ToLower(prefix)) {
		return "", false
	}
	action := strings.TrimSpace(labelName[len(prefix):])
	action = strings.ToLower(action)
	if action == "" {
		return "", false
	}
	return action, true
}

func actionRequiresPullRequest(action string) bool {
	switch action {
	case githubActionReview, githubActionUpdateBranch:
		return true
	default:
		return false
	}
}

func (p githubLabelWebhookPayload) target() (githubLabelTarget, bool) {
	if p.PullRequest != nil {
		pr := p.PullRequest
		repo := p.Repository
		headRepo := pr.Head.Repo
		baseRepo := pr.Base.Repo
		if baseRepo.CloneURL == "" && baseRepo.HTMLURL == "" {
			baseRepo = repo
		}
		return githubLabelTarget{
			Kind:       "pull_request",
			Number:     pr.Number,
			Title:      pr.Title,
			Body:       pr.Body,
			HTMLURL:    pr.HTMLURL,
			State:      pr.State,
			IsPR:       true,
			Draft:      pr.Draft,
			BaseBranch: pr.Base.Ref,
			BaseSHA:    pr.Base.SHA,
			HeadBranch: pr.Head.Ref,
			HeadSHA:    pr.Head.SHA,
			Repo:       repo,
			BaseRepo:   baseRepo,
			HeadRepo:   headRepo,
			Labels:     githubWebhookLabelNames(pr.Labels),
		}, true
	}

	if p.Issue != nil {
		htmlURL := p.Issue.HTMLURL
		if p.Issue.PullRequest != nil {
			if p.Issue.PullRequest.HTMLURL != "" {
				htmlURL = p.Issue.PullRequest.HTMLURL
			}
			return githubLabelTarget{
				Kind:         "pull_request",
				Number:       p.Issue.Number,
				Title:        p.Issue.Title,
				Body:         p.Issue.Body,
				HTMLURL:      htmlURL,
				State:        p.Issue.State,
				IsPR:         true,
				IncompletePR: true,
				Repo:         p.Repository,
				BaseRepo:     p.Repository,
				HeadRepo:     p.Repository,
				Labels:       githubWebhookLabelNames(p.Issue.Labels),
				UpdatedAt:    p.Issue.UpdatedAt,
			}, true
		}
		return githubLabelTarget{
			Kind:      "issue",
			Number:    p.Issue.Number,
			Title:     p.Issue.Title,
			Body:      p.Issue.Body,
			HTMLURL:   htmlURL,
			State:     p.Issue.State,
			Repo:      p.Repository,
			BaseRepo:  p.Repository,
			HeadRepo:  p.Repository,
			Labels:    githubWebhookLabelNames(p.Issue.Labels),
			UpdatedAt: p.Issue.UpdatedAt,
		}, true
	}

	return githubLabelTarget{}, false
}

func githubWebhookLabelNames(webhookLabels []githubWebhookLabel) []string {
	names := make([]string, 0, len(webhookLabels))
	for _, label := range webhookLabels {
		if strings.TrimSpace(label.Name) != "" {
			names = append(names, label.Name)
		}
	}
	return names
}

func (h *Handlers) githubWebhookNamespace() string {
	if ns := strings.TrimSpace(os.Getenv(githubLabelTriggerNamespaceEnv)); ns != "" {
		return ns
	}
	if h.watchNamespace != "" {
		return h.watchNamespace
	}
	return defaultNamespace
}

func githubAgentForAction(action string) string {
	if actionAgent := strings.TrimSpace(os.Getenv(githubActionAgentEnv(action))); actionAgent != "" {
		return actionAgent
	}
	return strings.TrimSpace(os.Getenv(githubLabelTriggerAgentEnv))
}

func githubActionAgentEnv(action string) string {
	action = strings.ToUpper(action)
	var b strings.Builder
	for _, r := range action {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return "ORKA_GITHUB_LABEL_AGENT_" + strings.Trim(b.String(), "_")
}

func (h *Handlers) ensureAgentExists(c fiber.Ctx, namespace, agentName string) (*agentruntimepolicy.RuntimeRefPolicy, error) {
	reader := h.apiReader
	if reader == nil {
		reader = h.client
	}

	var agent corev1alpha1.Agent
	if err := reader.Get(c.Context(), types.NamespacedName{Name: agentName, Namespace: namespace}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("agent %q not found in namespace %q", agentName, namespace))
		}
		return nil, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get agent: %v", err))
	}
	if agent.Spec.Runtime == nil {
		return nil, fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("agent %q must have runtime configured", agentName))
	}
	if agent.Spec.Runtime.RuntimeRef == nil {
		return nil, nil
	}

	runtimeName := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
	if runtimeName == "" {
		return nil, fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("agent %q runtimeRef.name is required", agentName))
	}
	var registered corev1alpha1.AgentRuntime
	if err := reader.Get(c.Context(), types.NamespacedName{Name: runtimeName, Namespace: namespace}, &registered); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("AgentRuntime %q referenced by agent %q not found in namespace %q", runtimeName, agentName, namespace))
		}
		return nil, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get AgentRuntime %q: %v", runtimeName, err))
	}
	switch registered.RegisteredContractVersion() {
	case corev1alpha1.AgentRuntimeContractHarnessV1:
		return nil, nil
	case corev1alpha1.AgentRuntimeContractHarnessV2:
		policy, err := agentruntimepolicy.PolicyForRuntime(&registered)
		if err != nil {
			return nil, fiber.NewError(fiber.StatusBadRequest, err.Error())
		}
		return policy, nil
	default:
		return nil, fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("AgentRuntime %q referenced by agent %q has no supported contractVersion", runtimeName, agentName))
	}
}

func githubWebhookAgentNotFound(err error) bool {
	var fiberErr *fiber.Error
	return errors.As(err, &fiberErr) &&
		fiberErr.Code == fiber.StatusBadRequest &&
		strings.HasPrefix(fiberErr.Message, "agent ") &&
		strings.Contains(fiberErr.Message, " not found in namespace ")
}

func buildGitHubLabelTask(namespace, agentName, action, replayKey, delivery, event string, payload githubLabelWebhookPayload, target githubLabelTarget, maxTurns *int32) *corev1alpha1.Task {
	workspace := githubWorkspace(action, target, replayKey)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      githubTaskName(action, target.Number, replayKey),
			Namespace: namespace,
			Labels: map[string]string{
				labels.LabelCreatedBy:        githubWebhookCreatedBy,
				labels.LabelGitHubEvent:      labels.SelectorValue(event),
				labels.LabelGitHubAction:     labels.SelectorValue(action),
				labels.LabelGitHubRepository: labels.SelectorValue(payload.Repository.FullName),
				labels.LabelGitHubTarget:     labels.SelectorValue(target.Kind),
			},
			Annotations: map[string]string{
				labels.AnnotationGitHubDelivery:   delivery,
				labels.AnnotationGitHubLabel:      payload.Label.Name,
				labels.AnnotationGitHubAction:     action,
				labels.AnnotationGitHubRepository: payload.Repository.FullName,
				labels.AnnotationGitHubURL:        target.HTMLURL,
				labels.AnnotationGitHubSender:     payload.Sender.Login,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAgent,
			Prompt: buildGitHubActionPrompt(action, payload, target, workspace),
			AgentRef: &corev1alpha1.AgentReference{
				Name: agentName,
			},
			Workspace: workspace,
			Timeout:   githubTimeout(),
		},
	}
	if maxTurns != nil {
		task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: maxTurns}
	}
	if action == githubActionReview && workspace != nil && workspace.ReadCredentialRef != nil {
		task.Annotations[labels.AnnotationWorkspaceInitContainer] = queryTrue
	}
	if target.Number > 0 {
		task.Labels[labels.LabelGitHubNumber] = labels.SelectorValue(strconv.Itoa(target.Number))
		task.Annotations[labels.AnnotationGitHubNumber] = strconv.Itoa(target.Number)
	}
	return task
}

func githubTaskName(action string, number int, replayKey string) string {
	action = dnsNamePart(action)
	if action == "" {
		action = "action"
	}
	deliveryHash := hex.EncodeToString(githubHash([]byte(replayKey)))[:12]
	base := fmt.Sprintf("github-%s-%d", action, number)
	maxBaseLen := 63 - len(deliveryHash) - 1
	if len(base) > maxBaseLen {
		base = strings.Trim(base[:maxBaseLen], "-")
	}
	if base == "" {
		base = sourceProviderGitHub
	}
	return base + "-" + deliveryHash
}

func dnsNamePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, ":", "-")
	value = nonDNSNameCharRE.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if len(value) > 40 {
		value = strings.Trim(value[:40], "-")
	}
	return value
}

func githubWorkspace(action string, target githubLabelTarget, replayKey string) *corev1alpha1.WorkspaceConfig {
	repo := target.Repo
	if target.IsPR && repoURL(target.HeadRepo) != "" {
		repo = target.HeadRepo
	}
	ws := &corev1alpha1.WorkspaceConfig{
		Intent:  corev1alpha1.WorkspaceIntentRead,
		GitRepo: repoURL(repo),
	}

	switch {
	case target.IsPR && target.HeadBranch != "":
		ws.Branch = target.HeadBranch
		if target.HeadSHA != "" {
			ws.Ref = target.HeadSHA
		}
	case target.Repo.DefaultBranch != "":
		ws.Branch = target.Repo.DefaultBranch
	}

	gitSecret := githubLabelTaskGitSecret(target)
	canPush := gitSecret != ""
	if action == githubActionImplement && canPush {
		ws.PushBranch = fmt.Sprintf("orka/implement-%s-%d", target.Kind, target.Number)
		if target.IsPR && target.HeadBranch != "" {
			ws.PushBranch = target.HeadBranch
		} else if replayKey != "" {
			ws.PushBranch = fmt.Sprintf("%s-%s", ws.PushBranch, githubReplayKeySuffix(replayKey))
		}
	}
	if action == githubActionUpdateBranch && target.HeadBranch != "" && canPush {
		ws.PushBranch = target.HeadBranch
	}
	if action != githubActionReview && action != githubActionToIssues && action != githubActionImplement && action != githubActionUpdateBranch && canPush {
		ws.PushBranch = fmt.Sprintf("orka/%s-%s-%d", dnsNamePart(action), target.Kind, target.Number)
	}

	if gitSecret != "" {
		ws.ReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: gitSecret}
	}
	if ws.PushBranch != "" {
		ws.Intent = corev1alpha1.WorkspaceIntentWrite
		ws.PublicationGitRepo = ws.GitRepo
		if gitSecret != "" {
			ws.PublicationReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: gitSecret}
			ws.PublicationCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: gitSecret}
		}
	}
	// prBaseBranch is a publication field: the ACP workspace preflight rejects it
	// on non-write intents, and read flows carry base-branch context in the prompt.
	if target.IsPR && target.BaseBranch != "" && ws.Intent == corev1alpha1.WorkspaceIntentWrite {
		ws.PRBaseBranch = target.BaseBranch
	}
	return ws
}

func githubLabelTaskGitSecret(target githubLabelTarget) string {
	gitSecret := strings.TrimSpace(os.Getenv(githubLabelTriggerGitSecretEnv))
	if gitSecret == "" || !githubLabelTaskCanUseGitSecret(target) {
		return ""
	}
	return gitSecret
}

func githubLabelTaskCanUseGitSecret(target githubLabelTarget) bool {
	if !target.IsPR {
		return true
	}
	return githubSameRepository(target.HeadRepo, target.BaseRepo)
}

func (h *Handlers) validateGitHubLabelTaskGitSecret(c fiber.Ctx, namespace string, target githubLabelTarget) error {
	gitSecret := strings.TrimSpace(os.Getenv(githubLabelTriggerGitSecretEnv))
	if gitSecret == "" || !githubLabelTaskCanUseGitSecret(target) {
		return nil
	}
	var secret corev1.Secret
	if err := h.client.Get(c.Context(), types.NamespacedName{Name: gitSecret, Namespace: namespace}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusServiceUnavailable, fmt.Sprintf("GitHub label trigger git secret %q not found in namespace %q", gitSecret, namespace))
		}
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get GitHub label trigger git secret %q: %v", gitSecret, err))
	}
	if !repositoryMonitorGitSecretHasToken(&secret) {
		return fiber.NewError(fiber.StatusServiceUnavailable, fmt.Sprintf("GitHub label trigger git secret %q must contain a non-empty token, password, or %s key", gitSecret, workerenv.GitHubToken))
	}
	return nil
}

func githubSameRepository(a, b githubWebhookRepository) bool {
	return a.FullName != "" && b.FullName != "" && strings.EqualFold(a.FullName, b.FullName)
}

func githubReplayKeySuffix(replayKey string) string {
	replayKey = strings.TrimSpace(replayKey)
	if len(replayKey) >= 12 {
		return replayKey[:12]
	}
	return hex.EncodeToString(githubHash([]byte(replayKey)))[:12]
}

func repoURL(repo githubWebhookRepository) string {
	if repo.CloneURL != "" {
		return repo.CloneURL
	}
	if repo.HTMLURL != "" {
		return strings.TrimSuffix(repo.HTMLURL, ".git") + ".git"
	}
	return ""
}

func githubMaxTurns() *int32 {
	maxTurns := githubWebhookDefaultMaxTurns
	if value := strings.TrimSpace(os.Getenv(githubLabelTriggerMaxTurnsEnv)); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 32); err == nil && parsed > 0 {
			maxTurns = int32(parsed)
		}
	}
	return &maxTurns
}

func githubTimeout() *metav1.Duration {
	if value := strings.TrimSpace(os.Getenv(githubLabelTriggerTimeoutEnv)); value != "" {
		if d, err := time.ParseDuration(value); err == nil && d > 0 {
			return &metav1.Duration{Duration: d}
		}
	}
	return &metav1.Duration{Duration: githubWebhookDefaultTimeout}
}

func buildGitHubActionPrompt(action string, payload githubLabelWebhookPayload, target githubLabelTarget, workspace *corev1alpha1.WorkspaceConfig) string {
	var b strings.Builder
	b.WriteString("You are an Orka agent task triggered by a GitHub label.\n\n")
	b.WriteString("Trigger details:\n")
	fmt.Fprintf(&b, "- Label: %s\n", payload.Label.Name)
	fmt.Fprintf(&b, "- Action: %s\n", action)
	fmt.Fprintf(&b, "- Repository: %s\n", payload.Repository.FullName)
	fmt.Fprintf(&b, "- Target: %s #%d\n", target.Kind, target.Number)
	fmt.Fprintf(&b, "- URL: %s\n", target.HTMLURL)
	if payload.Sender.Login != "" {
		fmt.Fprintf(&b, "- Triggered by: %s\n", payload.Sender.Login)
	}
	if target.IsPR {
		fmt.Fprintf(&b, "- Base branch: %s\n", target.BaseBranch)
		fmt.Fprintf(&b, "- Head branch: %s\n", target.HeadBranch)
		if target.HeadSHA != "" {
			fmt.Fprintf(&b, "- Head SHA: %s\n", target.HeadSHA)
		}
	}
	if workspace != nil {
		fmt.Fprintf(&b, "- Workspace repo: %s\n", workspace.GitRepo)
		if workspace.Branch != "" {
			fmt.Fprintf(&b, "- Workspace branch: %s\n", workspace.Branch)
		}
		if workspace.PushBranch != "" {
			fmt.Fprintf(&b, "- Push branch: %s\n", workspace.PushBranch)
			b.WriteString("- Push handling: do not commit or push yourself; leave final workspace changes uncommitted so Orka can commit and push them.\n")
		}
	}

	b.WriteString("\nTitle:\n")
	b.WriteString(target.Title)
	b.WriteString("\n\nBody:\n")
	body := strings.TrimSpace(target.Body)
	if body == "" {
		body = "(empty)"
	}
	b.WriteString(body)
	b.WriteString("\n\nInstructions:\n")

	switch action {
	case githubActionImplement:
		if workspace != nil && workspace.PushBranch != "" {
			b.WriteString("Implement the requested change. Keep the scope limited to the GitHub issue or PR request. Run relevant tests. Leave final changes uncommitted for Orka to commit and push. Summarize changes and test results.\n")
		} else {
			b.WriteString("Implement the requested change. Keep the scope limited to the GitHub issue or PR request. Run relevant tests. Leave final changes in the workspace for Orka to capture in the task result; Orka will not push them automatically. Summarize changes and test results.\n")
		}
	case githubActionUpdateBranch:
		if workspace != nil && workspace.PushBranch != "" {
			b.WriteString("Update the pull request branch with the latest base branch changes using a no-commit merge workflow. Resolve conflicts if needed, run relevant tests, and leave final changes uncommitted for Orka to commit and push. Do not merge the pull request.\n")
		} else {
			b.WriteString("Update the pull request branch with the latest base branch changes using a no-commit merge workflow. Resolve conflicts if needed, run relevant tests, and leave final changes in the workspace for Orka to capture in the task result; Orka will not push them automatically. Do not merge the pull request.\n")
		}
	case githubActionReview:
		b.WriteString("Review the pull request for correctness, tests, security, maintainability, and regressions. Do not change code. Produce a concise review with blocking findings first and include file/line references when available.\n")
	case githubActionToIssues:
		b.WriteString("Break the request into small, independently implementable GitHub issues. Prefer tracer-bullet vertical slices with acceptance criteria. If you can create issues with available GitHub credentials, do so; otherwise return issue drafts with titles, bodies, and labels.\n")
	default:
		fmt.Fprintf(&b, "Perform the requested %q action for this GitHub target. Keep changes scoped, run relevant verification, and summarize the outcome.\n", action)
	}

	b.WriteString("\nSafety constraints:\n")
	b.WriteString("- Do not print or commit secrets or credentials.\n")
	b.WriteString("- Do not merge pull requests unless the prompt explicitly says to merge.\n")
	b.WriteString("- If required credentials or permissions are missing, explain exactly what is missing.\n")
	return b.String()
}
