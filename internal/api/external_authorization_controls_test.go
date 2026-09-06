package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/events"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
)

func (f *externalAuthorizationFixture) allowOnly(t *testing.T, permissions ...authorizationv1.ResourceAttributes) {
	t.Helper()
	f.reviews = nil
	f.review = func(review *authorizationv1.SubjectAccessReview) error {
		f.requireIdentity(t, review.Spec)
		review.Status.Allowed = slices.ContainsFunc(permissions, func(p authorizationv1.ResourceAttributes) bool {
			return p == *review.Spec.ResourceAttributes
		})
		return nil
	}
}

func (f *externalAuthorizationFixture) allowRoute(t *testing.T, route string, additional ...authorizationv1.ResourceAttributes) {
	t.Helper()
	for _, tc := range externalAuthorizationCases(t) {
		if tc.route == route {
			f.allowOnly(t, append(tc.permissions, additional...)...)
			return
		}
	}
	t.Fatalf("no independent permission fixture for %s", route)
}

// CRUD controls use only the action's exact grant. A get or list grant is not
// required for the handler's internal read during an update or deletion.
// Monitor validation also needs access to the referenced reviewer Agent.
func TestExternalAPIAuthorizedKubernetesCRUD(t *testing.T) {
	for _, tc := range []struct {
		path, spec, marker string
		object             client.Object
	}{
		{"tasks", `{"type":"container","image":"example.invalid/test:fixture"}`, "example.invalid/test:fixture", &corev1alpha1.Task{}},
		{"providers", `{"type":"openai","defaultModel":"authorized-change"}`, "authorized-change", &corev1alpha1.Provider{}},
		{"tools", `{"type":"http","description":"authorized-change","http":{"url":"https://example.com"}}`, "authorized-change", &corev1alpha1.Tool{}},
		{"agent-runtimes", `{"contractVersion":"orka.harness.v2"}`, "orka.harness.v2", &corev1alpha1.AgentRuntime{}},
		{"agents", `{"systemPrompt":{"inline":"authorized-change"}}`, "authorized-change", &corev1alpha1.Agent{}},
		{"skills", `{"description":"authorized-change","content":{"inline":"fixture skill"}}`, "authorized-change", &corev1alpha1.Skill{}},
		{"security/repositories", `{"repoURL":"https://github.com/orka-agents/orka","analysisAgentRef":{"name":"reviewer"},"branch":"authorized-change"}`, "authorized-change", &corev1alpha1.RepositoryScan{}},
		{"monitors/repositories", `{"repoURL":"https://github.com/orka-agents/orka","agents":{"reviewer":{"name":"reviewer"}},"branch":"authorized-change"}`, "authorized-change", &corev1alpha1.RepositoryMonitor{}},
		{"substrate-actor-pools", `{"templateRef":{"name":"authorized-change"},"targetActors":1}`, "authorized-change", &corev1alpha1.SubstrateActorPool{}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			reviewer := repositoryMonitorHandlerTestAgent("reviewer", corev1alpha1.AgentRuntimeClaude)
			reviewer.Namespace = "default"
			require.NoError(t, f.kube.Create(t.Context(), reviewer))
			base := "/api/v1/" + tc.path
			var referencePermissions []authorizationv1.ResourceAttributes
			if tc.path == "monitors/repositories" {
				referencePermissions = []authorizationv1.ResourceAttributes{{Namespace: "default", Group: corev1alpha1.GroupVersion.Group, Resource: "agents", Verb: "get", Name: "reviewer"}}
			}
			f.allowRoute(t, "POST "+base, referencePermissions...)
			status, body := f.request(t, http.MethodPost, base, `{"name":"created","metadata":{"name":"created","labels":{"authorization-test":"created"}},"spec":`+tc.spec+`}`)
			require.Equal(t, http.StatusCreated, status, body)
			created := tc.object.DeepCopyObject().(client.Object)
			require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "created"}, created))
			require.Equal(t, "created", created.GetLabels()["authorization-test"])
			encoded, err := json.Marshal(created)
			require.NoError(t, err)
			require.Contains(t, string(encoded), tc.marker)
			require.NotEmpty(t, f.reviews)

			parameter := ":name"
			if tc.path == "tasks" {
				parameter = ":id"
			} else {
				f.allowRoute(t, "PUT "+base+"/"+parameter, referencePermissions...)
				status, body = f.request(t, http.MethodPut, base+"/protected", `{"metadata":{"name":"wrong-target","namespace":"other"},"spec":`+tc.spec+`}`)
				require.Equal(t, http.StatusOK, status, body)
				updated := tc.object.DeepCopyObject().(client.Object)
				require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, updated))
				encoded, err = json.Marshal(updated)
				require.NoError(t, err)
				require.Contains(t, string(encoded), tc.marker)
			}

			f.allowRoute(t, "GET "+base+"/"+parameter)
			status, body = f.request(t, http.MethodGet, base+"/protected", "")
			require.Equal(t, http.StatusOK, status, body)
			require.Contains(t, body, "protected")
			f.allowRoute(t, "GET "+base)
			status, body = f.request(t, http.MethodGet, base, "")
			require.Equal(t, http.StatusOK, status, body)
			require.Contains(t, body, "created")

			f.allowRoute(t, "DELETE "+base+"/"+parameter)
			status, body = f.request(t, http.MethodDelete, base+"/protected", "")
			require.Equal(t, http.StatusNoContent, status, body)
			require.True(t, apierrors.IsNotFound(f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, tc.object.DeepCopyObject().(client.Object))))
		})
	}
}

func TestExternalAPIAuthorizedMemoryLifecycle(t *testing.T) {
	f := newExternalAuthorizationFixture(t)
	f.allowRoute(t, "POST /api/v1/memory-proposals")
	status, body := f.request(t, http.MethodPost, "/api/v1/memory-proposals", `{"id":"created","type":"memory","title":"learned","content":"new memory"}`)
	require.Equal(t, http.StatusCreated, status, body)
	proposal, err := f.store.GetMemoryProposal(t.Context(), "default", "created")
	require.NoError(t, err)
	require.Equal(t, "new memory", proposal.Content)

	f.allowRoute(t, "POST /api/v1/memory-proposals/:id/review")
	status, body = f.request(t, http.MethodPost, "/api/v1/memory-proposals/protected/review", `{"status":"accepted"}`)
	require.Equal(t, http.StatusNoContent, status, body)
	proposal, err = f.store.GetMemoryProposal(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.Equal(t, "accepted", proposal.Status)
	require.Equal(t, f.user.Username, proposal.Reviewer)

	f.allowRoute(t, "POST /api/v1/memory-proposals/:id/apply")
	status, body = f.request(t, http.MethodPost, "/api/v1/memory-proposals/protected/apply", `{}`)
	require.Equal(t, http.StatusOK, status, body)
	proposal, err = f.store.GetMemoryProposal(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.NotEmpty(t, proposal.AppliedMemoryID)
	memory, err := f.store.GetMemory(t.Context(), "default", proposal.AppliedMemoryID)
	require.NoError(t, err)
	require.Equal(t, proposal.Content, memory.Content)

	require.NoError(t, f.store.CreateMemoryProposal(t.Context(), &store.MemoryProposal{ID: "archivable", Namespace: "default", Type: "memory", Title: "archive", Content: "archive"}))
	f.allowOnly(t, authorizationv1.ResourceAttributes{Namespace: "default", Group: "core.orka.ai", Resource: "memoryproposals", Verb: "update", Name: "archivable"})
	status, body = f.request(t, http.MethodPost, "/api/v1/memory-proposals/archivable/archive", `{}`)
	require.Equal(t, http.StatusNoContent, status, body)
	proposal, err = f.store.GetMemoryProposal(t.Context(), "default", "archivable")
	require.NoError(t, err)
	require.Equal(t, "archived", proposal.Status)

	f.allowRoute(t, "POST /api/v1/memories")
	status, body = f.request(t, http.MethodPost, "/api/v1/memories", `{"id":"created","content":"new memory"}`)
	require.Equal(t, http.StatusCreated, status, body)
	memory, err = f.store.GetMemory(t.Context(), "default", "created")
	require.NoError(t, err)
	require.Equal(t, "new memory", memory.Content)

	f.allowRoute(t, "PUT /api/v1/memories/:id")
	status, body = f.request(t, http.MethodPut, "/api/v1/memories/protected", `{"content":"updated memory"}`)
	require.Equal(t, http.StatusOK, status, body)
	memory, err = f.store.GetMemory(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.Equal(t, "updated memory", memory.Content)
	for _, action := range []string{"disable", "enable"} {
		f.allowRoute(t, "POST /api/v1/memories/:id/"+action)
		status, body = f.request(t, http.MethodPost, "/api/v1/memories/protected/"+action, "")
		require.Equal(t, http.StatusNoContent, status, body)
		memory, err = f.store.GetMemory(t.Context(), "default", "protected")
		require.NoError(t, err)
		require.Equal(t, action == "disable", memory.Disabled)
	}
	f.allowRoute(t, "DELETE /api/v1/memories/:id")
	status, body = f.request(t, http.MethodDelete, "/api/v1/memories/protected", "")
	require.Equal(t, http.StatusNoContent, status, body)
	memory, err = f.store.GetMemory(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.True(t, memory.Deleted)
}

func TestExternalAPIAuthorizedCompoundActions(t *testing.T) {
	t.Run("approval", func(t *testing.T) {
		f := newExternalAuthorizationFixture(t)
		task := &corev1alpha1.Task{}
		require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, task))
		task.Status.Phase = "Running"
		require.NoError(t, f.kube.Update(t.Context(), task))
		_, err := f.store.AppendExecutionEvent(t.Context(), &store.ExecutionEvent{Namespace: "default", StreamType: store.ExecutionEventStreamTypeTask, StreamID: "protected", TaskName: "protected", Type: events.ExecutionEventTypeApprovalRequested, Content: json.RawMessage(`{"approvalID":"protected","action":"create_pr"}`)})
		require.NoError(t, err)
		f.allowRoute(t, "POST /api/v1/tasks/:id/approvals/:approvalID/decision")
		status, body := f.request(t, http.MethodPost, "/api/v1/tasks/protected/approvals/protected/decision", `{"decision":"approve"}`)
		require.Equal(t, http.StatusOK, status, body)
		require.NoError(t, f.kube.Get(t.Context(), client.ObjectKeyFromObject(task), task))
		require.Equal(t, "approved", task.Annotations[labels.AnnotationApprovalDecisionStatus])
		entries, err := f.server.handlers.listTaskApprovalEvents(t.Context(), "default", "protected")
		require.NoError(t, err)
		require.Len(t, entries, 2)
	})
	t.Run("fork", func(t *testing.T) {
		f := newExternalAuthorizationFixture(t)
		f.allowRoute(t, "POST /api/v1/tasks/:id/fork")
		status, body := f.request(t, http.MethodPost, "/api/v1/tasks/protected/fork", `{"newTaskName":"forked"}`)
		require.Equal(t, http.StatusCreated, status, body)
		var result ForkTaskResponse
		require.NoError(t, json.Unmarshal([]byte(body), &result))
		task := &corev1alpha1.Task{}
		require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: result.NewTaskName}, task))
		require.NotEqual(t, "protected", task.Name)
	})
	t.Run("monitor run", func(t *testing.T) {
		f := newExternalAuthorizationFixture(t)
		f.allowRoute(t, "POST /api/v1/monitors/repositories/:name/runs")
		status, body := f.request(t, http.MethodPost, "/api/v1/monitors/repositories/protected/runs", `{"targetKind":"pull_request","targetNumber":42}`)
		require.Equal(t, http.StatusCreated, status, body)
		runs, _, err := f.store.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "protected"})
		require.NoError(t, err)
		require.Len(t, runs, 1)
		require.Equal(t, "queued", runs[0].Phase)
		monitor := &corev1alpha1.RepositoryMonitor{}
		require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, monitor))
		require.Equal(t, runs[0].ID, monitor.Annotations[repositoryMonitorRunRequestAnnotation])
	})
	t.Run("session delete", func(t *testing.T) {
		f := newExternalAuthorizationFixture(t)
		require.NoError(t, f.store.CreateSession(t.Context(), &store.SessionRecord{Namespace: "default", Name: "protected"}))
		f.allowRoute(t, "DELETE /api/v1/sessions/:id")
		status, body := f.request(t, http.MethodDelete, "/api/v1/sessions/protected", "")
		require.Equal(t, http.StatusNoContent, status, body)
		_, err := f.store.GetSession(t.Context(), "default", "protected")
		require.ErrorIs(t, err, store.ErrNotFound)
	})
}

func TestExternalAPIIdentityContracts(t *testing.T) {
	provider := newTestOIDCProvider(t)
	for _, identity := range []string{"oidc", "transaction-write", "transaction-read"} {
		t.Run(identity, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
			require.NoError(t, err)
			cfg := ServerConfig{WatchNamespace: "default", EnforceNamespaceIsolation: true, Clientset: f.clientset, MemoryStore: f.store, ContextTokenAuthorization: authz}
			header := "Authorization"
			var credential string
			want := http.StatusCreated
			if identity == "oidc" {
				cfg.OIDC = provider.config()
				credential = "Bearer " + provider.issueToken(t, testOIDCTokenOptions{Namespace: "default"})
			} else {
				cfg.ContextTokens = testContextTokenConfig(t, provider, "")
				header = TransactionTokenHeaderName
				scope := ContextTokenScopeMemoryWrite
				if identity == "transaction-read" {
					scope = ContextTokenScopeMemoryRead
					want = http.StatusForbidden
				}
				credential = issueTestContextToken(t, provider, nil, map[string]any{"scope": scope, "tctx": map[string]any{"namespace": "default"}})
			}
			server := NewServer(f.kube, nil, cfg)
			before := f.changes(t)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/memories", strings.NewReader(`{"id":"created","content":"allowed by the identity contract"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(header, credential)
			response, err := server.app.Test(request)
			require.NoError(t, err)
			defer response.Body.Close() //nolint:errcheck
			require.Equal(t, want, response.StatusCode)
			require.Zero(t, f.tokenReviews)
			require.Empty(t, f.reviews, "non-TokenReview identities must not acquire Kubernetes RBAC semantics")
			if want == http.StatusCreated {
				memory, err := f.store.GetMemory(t.Context(), "default", "created")
				require.NoError(t, err)
				require.Equal(t, "allowed by the identity contract", memory.Content)
			} else {
				require.Equal(t, before, f.changes(t))
			}
		})
	}
}

func TestExternalAPIProposalAppliedMemoryRequiresNamedRead(t *testing.T) {
	for _, link := range []string{"applied-memory-id", "source-proposal-id"} {
		t.Run(link, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			proposal := &store.MemoryProposal{ID: "reference", Namespace: "default", Type: "memory", Title: "reference", Content: "proposal", Status: "accepted"}
			if link == "applied-memory-id" {
				proposal.AppliedMemoryID = "protected"
			} else {
				memory, err := f.store.GetMemory(t.Context(), "default", "protected")
				require.NoError(t, err)
				memory.SourceProposalID = proposal.ID
				require.NoError(t, f.store.UpdateMemory(t.Context(), memory))
			}
			require.NoError(t, f.store.CreateMemoryProposal(t.Context(), proposal))
			permissions := make([]authorizationv1.ResourceAttributes, 0, 3)
			permissions = append(permissions,
				authorizationv1.ResourceAttributes{Namespace: "default", Group: "core.orka.ai", Resource: "memoryproposals", Verb: "apply", Name: "reference"},
				authorizationv1.ResourceAttributes{Namespace: "default", Group: "core.orka.ai", Resource: "memories", Verb: "create"},
			)
			f.allowOnly(t, permissions...)
			before := f.changes(t)
			status, body := f.request(t, http.MethodPost, "/api/v1/memory-proposals/reference/apply", `{}`)
			require.Equal(t, http.StatusForbidden, status, body)
			require.NotContains(t, body, "protected-content")
			require.Equal(t, before, f.changes(t))
			read := authorizationv1.ResourceAttributes{Namespace: "default", Group: "core.orka.ai", Resource: "memories", Verb: "get", Name: "protected"}
			require.Equal(t, read, *f.reviews[len(f.reviews)-1].ResourceAttributes)
			f.allowOnly(t, append(permissions, read)...)
			status, body = f.request(t, http.MethodPost, "/api/v1/memory-proposals/reference/apply", `{}`)
			require.Equal(t, http.StatusOK, status, body)
			require.Contains(t, body, "protected-content")
		})
	}
}

func TestExternalAPIAuthorizedSecurityActions(t *testing.T) {
	f := newExternalAuthorizationFixture(t)
	scan := &corev1alpha1.RepositoryScan{}
	require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, scan))
	scan.Spec = corev1alpha1.RepositoryScanSpec{
		RepoURL: "https://github.com/orka-agents/orka", Branch: "main", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "reviewer"},
		ReadCredentialRef: &corev1.LocalObjectReference{Name: "source-read"}, PublicationReadCredentialRef: &corev1.LocalObjectReference{Name: "publication-read"},
		PublicationCredentialRef: &corev1.LocalObjectReference{Name: "publication-write"}, ForgeCredentialRef: &corev1.LocalObjectReference{Name: "forge"},
	}
	require.NoError(t, f.kube.Update(t.Context(), scan))
	require.NoError(t, f.store.UpsertFinding(t.Context(), &store.Finding{ID: "protected", Namespace: "default", RepositoryScan: "protected", Title: "fixture", State: "open"}))

	f.allowRoute(t, "PUT /api/v1/security/repositories/:name/threat-model")
	status, body := f.request(t, http.MethodPut, "/api/v1/security/repositories/protected/threat-model", `{"content":"updated threat model"}`)
	require.Equal(t, http.StatusOK, status, body)
	model, err := f.store.GetLatestThreatModel(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.Equal(t, "updated threat model", model.Content)

	f.allowRoute(t, "POST /api/v1/security/repositories/:name/scans")
	status, body = f.request(t, http.MethodPost, "/api/v1/security/repositories/protected/scans", `{}`)
	require.Equal(t, http.StatusCreated, status, body)
	runs, _, err := f.store.ListScanRuns(t.Context(), "default", "protected", 10, "")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: runs[0].TaskName}, &corev1alpha1.Task{}))

	for _, action := range []string{"dismiss", "reopen"} {
		f.allowRoute(t, "POST /api/v1/security/findings/:id/"+action)
		status, body = f.request(t, http.MethodPost, "/api/v1/security/findings/protected/"+action, `{}`)
		require.Equal(t, http.StatusNoContent, status, body)
		finding, err := f.store.GetFinding(t.Context(), "default", "protected")
		require.NoError(t, err)
		if action == "dismiss" {
			require.Equal(t, "dismissed", finding.State)
		} else {
			require.Equal(t, "open", finding.State)
		}
	}
	f.allowRoute(t, "POST /api/v1/security/findings/:id/validate")
	status, body = f.request(t, http.MethodPost, "/api/v1/security/findings/protected/validate", `{}`)
	require.Equal(t, http.StatusAccepted, status, body)
	finding, err := f.store.GetFinding(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.Equal(t, "pending", finding.ValidationStatus)

	f.allowRoute(t, "POST /api/v1/security/findings/:id/patch")
	status, body = f.request(t, http.MethodPost, "/api/v1/security/findings/protected/patch", `{}`)
	require.Equal(t, http.StatusCreated, status, body)
	proposals, err := f.store.ListPatchProposals(t.Context(), "default", "protected")
	require.NoError(t, err)
	require.Len(t, proposals, 1)
	require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: proposals[0].TaskName}, &corev1alpha1.Task{}))
	prNumber := 42
	require.NoError(t, f.store.CreatePatchProposal(t.Context(), &store.PatchProposal{ID: "receipt", Namespace: "default", RepositoryScan: "protected", FindingID: "protected", Status: "pr_opened", PRNumber: &prNumber, PRURL: "https://github.com/orka-agents/orka/pull/42"}))
	f.allowRoute(t, "POST /api/v1/security/findings/:id/pull-request")
	before := f.changes(t)
	status, body = f.request(t, http.MethodPost, "/api/v1/security/findings/protected/pull-request", `{}`)
	require.Equal(t, http.StatusOK, status, body)
	require.Contains(t, body, `"prNumber":42`)
	require.Equal(t, before, f.changes(t), "receipt access must not publish or change stored data")
	require.Zero(t, f.externalCalls.Load())
}

func TestExternalAPIGatewayRetryPreservesOwnershipChecks(t *testing.T) {
	for _, denied := range []string{"get", "update", ""} {
		t.Run("denied-"+denied, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			ss, delivery := newGatewayHandlerDeliveryStore(t)
			f.server.handlers.gatewayDeliveryStore = ss
			f.server.handlers.apiReader = newGatewayIdentityClient(t, delivery.NamespaceUID, delivery.GatewayUID)
			cfg := gatewayruntime.DefaultConfig()
			cfg.Enabled = true
			f.server.handlers.gatewayService = gatewayruntime.NewService(nil, nil, ss, nil, cfg)
			permissions := []authorizationv1.ResourceAttributes{{Namespace: "default", Group: "gateway.orka.ai", Resource: "gatewaydeliveries", Verb: "update", Name: delivery.ID}}
			for _, verb := range []string{"get", "update"} {
				if verb != denied {
					permissions = append(permissions, authorizationv1.ResourceAttributes{Namespace: "default", Group: "gateway.orka.ai", Resource: "gateways", Verb: verb, Name: delivery.GatewayName})
				}
			}
			f.allowOnly(t, permissions...)
			status, body := f.request(t, http.MethodPost, "/api/v1/gateway-deliveries/"+delivery.ID+"/retry", `{}`)
			current, err := ss.GetGatewayDelivery(t.Context(), "default", delivery.ID)
			require.NoError(t, err)
			if denied != "" {
				require.Equal(t, http.StatusForbidden, status, body)
				require.Equal(t, delivery, current)
			} else {
				require.Equal(t, http.StatusAccepted, status, body)
				require.Equal(t, 1, current.ManualRetryCount)
				require.NotEqual(t, store.GatewayDeliveryDeadLettered, current.State)
			}
		})
	}
}

func TestExternalAPIMonitorCommandPreflightsRunCreation(t *testing.T) {
	for _, test := range []struct{ name, runGrantName string }{
		{name: "missing-run-grant"},
		{name: "other-monitor-run-grant", runGrantName: "other"},
		{name: "authorized", runGrantName: "protected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			monitor := &corev1alpha1.RepositoryMonitor{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "protected"}}
			require.NoError(t, f.kube.Get(t.Context(), client.ObjectKeyFromObject(monitor), monitor))
			monitor.Spec.RepoURL = "https://github.com/orka-agents/orka"
			f.server.handlers.normalizeRepositoryMonitorSpec(&monitor.Spec)
			require.NoError(t, f.kube.Update(t.Context(), monitor))
			require.NoError(t, f.store.UpsertMonitorItem(t.Context(), &store.MonitorItem{MonitorNamespace: "default", MonitorName: "protected", Kind: "pull_request", ItemKey: "42", Number: 42, State: "open", HeadSHA: "fixture-sha", UpdatedAt: time.Now()}))
			permissions := []authorizationv1.ResourceAttributes{
				{Namespace: "default", Group: "core.orka.ai", Resource: "repositorymonitors", Subresource: "commands", Verb: "create", Name: "protected"},
				{Namespace: "default", Group: "core.orka.ai", Resource: "repositorymonitors", Verb: "patch", Name: "protected"},
			}
			if test.runGrantName != "" {
				permissions = append(permissions, authorizationv1.ResourceAttributes{
					Namespace: "default", Group: "core.orka.ai", Resource: "repositorymonitors", Subresource: "runs", Verb: "create", Name: test.runGrantName,
				})
			}
			f.allowOnly(t, permissions...)
			before := f.changes(t)
			f.kubeCalls = 0
			status, body := f.request(t, http.MethodPost, "/api/v1/monitors/repositories/protected/commands", `{"kind":"pull_request","number":42,"intent":"review","targetSHA":"fixture-sha"}`)
			require.Zero(t, f.externalCalls.Load())
			if test.runGrantName != "protected" {
				require.Equal(t, http.StatusForbidden, status, "body=%s; Kubernetes calls=%d; SQLite changes=%d", body, f.kubeCalls, f.changes(t)-before)
				require.Zero(t, f.kubeCalls)
				require.Equal(t, before, f.changes(t), "denied command changed a record or queued work")
				return
			}
			require.Equal(t, http.StatusCreated, status, body)
			commands, _, err := f.store.ListCommandEvents(t.Context(), store.CommandEventFilter{Namespace: "default", MonitorName: "protected"})
			require.NoError(t, err)
			require.Len(t, commands, 1)
			require.Equal(t, "review", commands[0].Intent)
			runs, _, err := f.store.ListMonitorRuns(t.Context(), store.MonitorRunFilter{Namespace: "default", MonitorName: "protected"})
			require.NoError(t, err)
			require.Len(t, runs, 1)
			require.Equal(t, commands[0].ID, runs[0].CommandEventID)
			require.Equal(t, repositoryMonitorRunPhaseQueued, runs[0].Phase)
			require.NoError(t, f.kube.Get(t.Context(), client.ObjectKeyFromObject(monitor), monitor))
			require.Equal(t, runs[0].ID, monitor.Annotations[repositoryMonitorRunRequestAnnotation])
		})
	}
}

func TestExternalAPIAuthorizedChatProviderInvocation(t *testing.T) {
	for _, path := range []string{"/api/v1/chat", "/openai/v1/chat/completions", "/anthropic/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			f.allowRoute(t, "POST "+path)
			before := f.changes(t)
			status, body := f.request(t, http.MethodPost, path, `{"message":"hello","model":"protected/fixture","max_tokens":128,"messages":[{"role":"user","content":"hello"}]}`)
			require.Equal(t, http.StatusOK, status, body)
			require.Contains(t, body, "Done.")
			require.Equal(t, int64(1), f.externalCalls.Load())
			require.Equal(t, 1, f.tokenReviews)
			if path == "/api/v1/chat" {
				var response ChatResponse
				require.NoError(t, json.Unmarshal([]byte(body), &response))
				messages, err := f.store.LoadTranscript(t.Context(), "default", response.SessionID, 0)
				require.NoError(t, err)
				require.Len(t, messages, 2)
				require.Equal(t, "hello", messages[0].Content)
			} else {
				require.Equal(t, before, f.changes(t))
			}
		})
	}
}
