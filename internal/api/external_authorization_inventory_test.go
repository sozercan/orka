package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

// This fixture is independent of the production policy map. Each row is an
// actual HTTP request and its expected resource authorization, including every
// write in compound operations. An omitted route fails the inventory check.
const externalAuthorizationInventory = `
POST /api/v1/tasks | create core.orka.ai:tasks
GET /api/v1/tasks | list core.orka.ai:tasks
GET /api/v1/tasks/:id | get core.orka.ai:tasks protected
DELETE /api/v1/tasks/:id | delete core.orka.ai:tasks protected
GET /api/v1/tasks/:id/logs | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/events | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/stream | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/trace | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/approvals | get core.orka.ai:tasks protected
POST /api/v1/tasks/:id/approvals/:approvalID/decision | update core.orka.ai:tasks/approvals protected; patch core.orka.ai:tasks protected
POST /api/v1/tasks/:id/fork | get core.orka.ai:tasks protected; create core.orka.ai:tasks
GET /api/v1/tasks/:id/result | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/plan | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/children | get core.orka.ai:tasks protected; list core.orka.ai:tasks
GET /api/v1/tasks/:id/artifacts | get core.orka.ai:tasks protected
GET /api/v1/tasks/:id/artifacts/:filename | get core.orka.ai:tasks protected
GET /api/v1/sessions | list core.orka.ai:sessions
GET /api/v1/sessions/:id | get core.orka.ai:sessions protected
GET /api/v1/sessions/:id/events | get core.orka.ai:sessions protected
GET /api/v1/sessions/:id/stream | get core.orka.ai:sessions protected
DELETE /api/v1/sessions/:id | delete core.orka.ai:sessions protected
GET /api/v1/gatewayclasses | list gateway.orka.ai:gatewayclasses
GET /api/v1/gatewayclasses/:name | get gateway.orka.ai:gatewayclasses protected
GET /api/v1/gateways | list gateway.orka.ai:gateways
GET /api/v1/gateways/:name | get gateway.orka.ai:gateways protected
GET /api/v1/gatewaybindings | list gateway.orka.ai:gatewaybindings
GET /api/v1/gatewaybindings/:name | get gateway.orka.ai:gatewaybindings protected
GET /api/v1/gateway-events | list gateway.orka.ai:gatewayevents
GET /api/v1/gateway-events/:id | get gateway.orka.ai:gatewayevents protected
GET /api/v1/gateway-deliveries | list gateway.orka.ai:gatewaydeliveries
GET /api/v1/gateway-deliveries/:id | get gateway.orka.ai:gatewaydeliveries protected
POST /api/v1/gateway-deliveries/:id/retry | update gateway.orka.ai:gatewaydeliveries protected
GET /api/v1/memories | list core.orka.ai:memories
POST /api/v1/memories | create core.orka.ai:memories
GET /api/v1/memories/:id | get core.orka.ai:memories protected
PUT /api/v1/memories/:id | update core.orka.ai:memories protected
DELETE /api/v1/memories/:id | delete core.orka.ai:memories protected
POST /api/v1/memories/:id/disable | update core.orka.ai:memories protected
POST /api/v1/memories/:id/enable | update core.orka.ai:memories protected
GET /api/v1/memory-proposals | list core.orka.ai:memoryproposals
POST /api/v1/memory-proposals | create core.orka.ai:memoryproposals
GET /api/v1/memory-proposals/:id | get core.orka.ai:memoryproposals protected
POST /api/v1/memory-proposals/:id/review | review core.orka.ai:memoryproposals protected
POST /api/v1/memory-proposals/:id/apply | apply core.orka.ai:memoryproposals protected; create core.orka.ai:memories
POST /api/v1/memory-proposals/:id/archive | update core.orka.ai:memoryproposals protected
GET /api/v1/providers | list core.orka.ai:providers
POST /api/v1/providers | create core.orka.ai:providers
GET /api/v1/providers/:name | get core.orka.ai:providers protected
PUT /api/v1/providers/:name | update core.orka.ai:providers protected
DELETE /api/v1/providers/:name | delete core.orka.ai:providers protected
GET /api/v1/tools | list core.orka.ai:tools
POST /api/v1/tools | create core.orka.ai:tools
GET /api/v1/tools/:name | get core.orka.ai:tools protected
PUT /api/v1/tools/:name | update core.orka.ai:tools protected
DELETE /api/v1/tools/:name | delete core.orka.ai:tools protected
GET /api/v1/runtime-pools | list core.orka.ai:runtimepools
GET /api/v1/runtime-pools/:name | get core.orka.ai:runtimepools protected
GET /api/v1/agent-runtimes | list core.orka.ai:agentruntimes
POST /api/v1/agent-runtimes | create core.orka.ai:agentruntimes
GET /api/v1/agent-runtimes/:name | get core.orka.ai:agentruntimes protected
PUT /api/v1/agent-runtimes/:name | update core.orka.ai:agentruntimes protected
DELETE /api/v1/agent-runtimes/:name | delete core.orka.ai:agentruntimes protected
POST /api/v1/agents | create core.orka.ai:agents
GET /api/v1/agents | list core.orka.ai:agents
GET /api/v1/agents/:name | get core.orka.ai:agents protected
PUT /api/v1/agents/:name | patch core.orka.ai:agents protected
DELETE /api/v1/agents/:name | delete core.orka.ai:agents protected
POST /api/v1/skills | create core.orka.ai:skills
GET /api/v1/skills | list core.orka.ai:skills
GET /api/v1/skills/:name | get core.orka.ai:skills protected
GET /api/v1/skills/:name/content | get core.orka.ai:skills protected
PUT /api/v1/skills/:name | update core.orka.ai:skills protected
DELETE /api/v1/skills/:name | delete core.orka.ai:skills protected
POST /api/v1/security/repositories | create core.orka.ai:repositoryscans
GET /api/v1/security/repositories | list core.orka.ai:repositoryscans
GET /api/v1/security/repositories/:name | get core.orka.ai:repositoryscans protected
PUT /api/v1/security/repositories/:name | update core.orka.ai:repositoryscans protected
DELETE /api/v1/security/repositories/:name | delete core.orka.ai:repositoryscans protected
GET /api/v1/security/repositories/:name/threat-model | get core.orka.ai:repositoryscans/threatmodel protected
PUT /api/v1/security/repositories/:name/threat-model | update core.orka.ai:repositoryscans/threatmodel protected
GET /api/v1/security/repositories/:name/scans | list core.orka.ai:repositoryscans/scans protected
POST /api/v1/security/repositories/:name/scans | create core.orka.ai:repositoryscans/scans protected; list core.orka.ai:tasks; create core.orka.ai:tasks; patch core.orka.ai:repositoryscans/status protected
GET /api/v1/security/repositories/:name/slices | list core.orka.ai:repositoryscans/slices protected
GET /api/v1/security/repositories/:name/slices/:sliceID | get core.orka.ai:repositoryscans/slices protected
GET /api/v1/security/repositories/:name/dropped-findings | list core.orka.ai:repositoryscans/droppedfindings protected
GET /api/v1/security/repositories/:name/findings | list core.orka.ai:repositoryscans/findings protected
GET /api/v1/security/findings/:id | get core.orka.ai:securityfindings protected
POST /api/v1/security/findings/:id/dismiss | update core.orka.ai:securityfindings protected
POST /api/v1/security/findings/:id/reopen | update core.orka.ai:securityfindings protected
POST /api/v1/security/findings/:id/validate | create core.orka.ai:securityfindings/validation protected; create core.orka.ai:tasks
POST /api/v1/security/findings/:id/patch | create core.orka.ai:securityfindings/patches protected; create core.orka.ai:tasks
GET /api/v1/security/findings/:id/patches | list core.orka.ai:securityfindings/patches protected
POST /api/v1/security/findings/:id/pull-request | get core.orka.ai:securityfindings/pullrequest protected
POST /api/v1/monitors/repositories | create core.orka.ai:repositorymonitors
GET /api/v1/monitors/repositories | list core.orka.ai:repositorymonitors
GET /api/v1/monitors/repositories/:name | get core.orka.ai:repositorymonitors protected
PUT /api/v1/monitors/repositories/:name | update core.orka.ai:repositorymonitors protected
DELETE /api/v1/monitors/repositories/:name | delete core.orka.ai:repositorymonitors protected
POST /api/v1/monitors/repositories/:name/runs | create core.orka.ai:repositorymonitors/runs protected; patch core.orka.ai:repositorymonitors protected
GET /api/v1/monitors/repositories/:name/runs | list core.orka.ai:repositorymonitors/runs protected
GET /api/v1/monitors/repositories/:name/items | list core.orka.ai:repositorymonitors/items protected
POST /api/v1/monitors/repositories/:name/commands | create core.orka.ai:repositorymonitors/commands protected; create core.orka.ai:repositorymonitors/runs protected; patch core.orka.ai:repositorymonitors protected
GET /api/v1/monitors/commands | list core.orka.ai:monitorcommands
GET /api/v1/monitors/commands/:id | get core.orka.ai:monitorcommands protected
GET /api/v1/monitors/actions | list core.orka.ai:monitoractions
GET /api/v1/monitors/actions/:id | get core.orka.ai:monitoractions protected
GET /api/v1/monitors/work-actions | list core.orka.ai:monitorworkactions
GET /api/v1/monitors/work-actions/:id | get core.orka.ai:monitorworkactions protected
GET /api/v1/monitors/implementation-jobs | list core.orka.ai:monitorimplementationjobs
GET /api/v1/monitors/implementation-jobs/:id | get core.orka.ai:monitorimplementationjobs protected
GET /api/v1/monitors/implementation-jobs/:id/patch-preview | get core.orka.ai:monitorimplementationjobs protected
GET /api/v1/monitors/mutations | list core.orka.ai:monitormutations
GET /api/v1/monitors/mutations/:id | get core.orka.ai:monitormutations protected
GET /api/v1/monitors/events | list core.orka.ai:monitorevents
GET /api/v1/substrate-actor-pools | list core.orka.ai:substrateactorpools
POST /api/v1/substrate-actor-pools | create core.orka.ai:substrateactorpools
GET /api/v1/substrate-actor-pools/:name | get core.orka.ai:substrateactorpools protected
PUT /api/v1/substrate-actor-pools/:name | update core.orka.ai:substrateactorpools protected
DELETE /api/v1/substrate-actor-pools/:name | delete core.orka.ai:substrateactorpools protected
GET /api/v1/auth/validate | identity
GET /api/v1/auth/whoami | identity
GET /api/v1/secrets | list :secrets
POST /api/v1/chat | create core.orka.ai:chats
GET /api/v1/chat/config | get core.orka.ai:chats/config
DELETE /api/v1/chat/:sessionId | delete core.orka.ai:sessions protected
POST /openai/v1/chat/completions | create core.orka.ai:chats
GET /openai/v1/models | list core.orka.ai:providers
POST /anthropic/v1/messages | create core.orka.ai:chats
GET /anthropic/v1/models | list core.orka.ai:providers
`

type externalAuthorizationCase struct {
	route       string
	permissions []authorizationv1.ResourceAttributes
}

func externalAuthorizationCases(t *testing.T) []externalAuthorizationCase {
	t.Helper()
	var cases []externalAuthorizationCase
	for line := range strings.SplitSeq(strings.TrimSpace(externalAuthorizationInventory), "\n") {
		route, expected, ok := strings.Cut(line, " | ")
		require.True(t, ok)
		c := externalAuthorizationCase{route: route}
		if expected != "identity" {
			for permission := range strings.SplitSeq(expected, "; ") {
				fields := strings.Fields(permission)
				require.GreaterOrEqual(t, len(fields), 2)
				group, resource, found := strings.Cut(fields[1], ":")
				require.True(t, found)
				resource, subresource, _ := strings.Cut(resource, "/")
				attributes := authorizationv1.ResourceAttributes{Namespace: "default", Group: group, Resource: resource, Subresource: subresource, Verb: fields[0]}
				if len(fields) == 3 {
					attributes.Name = fields[2]
				}
				if resource == "gatewayclasses" {
					attributes.Namespace = ""
				}
				c.permissions = append(c.permissions, attributes)
			}
		}
		cases = append(cases, c)
	}
	return cases
}

type externalAuthorizationFixture struct {
	server        *Server
	kube          client.Client
	clientset     *kubefake.Clientset
	store         *sqlite.Store
	db            *sql.DB
	user          authenticationv1.UserInfo
	tokenReviews  int
	kubeCalls     int
	externalCalls atomic.Int64
	reviews       []authorizationv1.SubjectAccessReviewSpec
	review        func(*authorizationv1.SubjectAccessReview) error
	sequence      int
}

func newExternalAuthorizationFixture(t *testing.T) *externalAuthorizationFixture {
	t.Helper()
	f := &externalAuthorizationFixture{user: authenticationv1.UserInfo{
		Username: "system:serviceaccount:default:viewer", UID: "viewer-uid",
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
		Extra:  map[string]authenticationv1.ExtraValue{"example.com/claims": {"a", "b"}},
	}}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.externalCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/responses"):
			_, _ = io.WriteString(w, `{"id":"fixture","object":"response","status":"completed","model":"fixture","output":[{"id":"message","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Done. <!-- GOAL_STATE:SATISFIED -->"}]}]}`)
		case strings.HasSuffix(r.URL.Path, "/chat/completions"):
			_, _ = io.WriteString(w, `{"id":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"Done. <!-- GOAL_STATE:SATISFIED -->"},"finish_reason":"stop"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(provider.Close)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1alpha1.AddToScheme, gatewayv1alpha1.AddToScheme, corev1.AddToScheme, authenticationv1.AddToScheme} {
		require.NoError(t, add(scheme))
	}
	meta := metav1.ObjectMeta{Name: "protected", Namespace: "default"}
	f.kube = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.RepositoryScan{}).WithObjects(
		&corev1alpha1.Task{ObjectMeta: meta, Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer}, Status: corev1alpha1.TaskStatus{Phase: "Succeeded"}},
		&corev1alpha1.Tool{ObjectMeta: meta, Spec: corev1alpha1.ToolSpec{Description: "protected-content"}},
		&corev1alpha1.Skill{ObjectMeta: meta},
		&corev1alpha1.Agent{ObjectMeta: meta},
		&corev1alpha1.AgentRuntime{ObjectMeta: meta},
		&corev1alpha1.RuntimePool{ObjectMeta: meta},
		&corev1alpha1.SubstrateActorPool{ObjectMeta: meta},
		&corev1alpha1.RepositoryScan{ObjectMeta: meta},
		&corev1alpha1.RepositoryMonitor{ObjectMeta: meta},
		&corev1alpha1.Provider{ObjectMeta: meta, Spec: corev1alpha1.ProviderSpec{Type: "openai", BaseURL: provider.URL, DefaultModel: "fixture", SecretRef: corev1alpha1.ProviderSecretRef{Name: "provider-key", Key: "key"}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "provider-key"}, Data: map[string][]byte{"key": []byte("fixture-only")}},
	).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if review, ok := obj.(*authenticationv1.TokenReview); ok {
				f.tokenReviews++
				review.Status = authenticationv1.TokenReviewStatus{Authenticated: true, User: f.user}
				return nil
			}
			f.kubeCalls++
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			f.kubeCalls++
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			f.kubeCalls++
			return c.List(ctx, list, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			f.kubeCalls++
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			f.kubeCalls++
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			f.kubeCalls++
			return c.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subresource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			f.kubeCalls++
			return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	f.clientset = kubefake.NewClientset()
	f.clientset.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		f.reviews = append(f.reviews, *review.Spec.DeepCopy())
		if f.review != nil {
			if err := f.review(review); err != nil {
				return true, nil, err
			}
		}
		return true, review, nil
	})
	var err error
	f.db, err = sqlite.NewDB(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.db.Close()) })
	f.store = sqlite.NewStore(f.db, ":memory:")
	require.NoError(t, f.store.CreateMemory(t.Context(), &store.Memory{ID: "protected", Namespace: "default", Content: "protected-content"}))
	require.NoError(t, f.store.CreateMemoryProposal(t.Context(), &store.MemoryProposal{ID: "protected", Namespace: "default", Type: "memory", Title: "protected-content", Content: "protected-content"}))
	f.server = NewServer(f.kube, nil, ServerConfig{
		WatchNamespace: "default", EnforceNamespaceIsolation: true, Clientset: f.clientset, APIReader: f.kube,
		MemoryStore: f.store, MemoryProposalStore: f.store, SecurityStore: f.store, RepositoryMonitorStore: f.store,
		SessionStore: f.store, ResultStore: f.store, PlanStore: f.store, ArtifactStore: f.store,
		ExecutionEventStore: f.store, GatewayEventStore: f.store, GatewayDeliveryStore: f.store,
		Chat: ChatConfig{Enabled: true, Provider: "protected", Model: "fixture", MaxDuration: time.Second, MaxIterations: 1, MaxConcurrent: 2},
	})
	return f
}

func (f *externalAuthorizationFixture) request(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	f.sequence++
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Bearer fixture-%s-%d", t.Name(), f.sequence))
	response, err := f.server.app.Test(request, fiber.TestConfig{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, string(data)
}

func (f *externalAuthorizationFixture) changes(t *testing.T) int64 {
	t.Helper()
	var count int64
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT total_changes()").Scan(&count))
	return count
}

func (f *externalAuthorizationFixture) requireIdentity(t *testing.T, review authorizationv1.SubjectAccessReviewSpec) {
	t.Helper()
	require.Equal(t, f.user.Username, review.User)
	require.Equal(t, f.user.UID, review.UID)
	require.Equal(t, f.user.Groups, review.Groups)
	require.Equal(t, map[string]authorizationv1.ExtraValue{"example.com/claims": {"a", "b"}}, review.Extra)
	require.Nil(t, review.NonResourceAttributes)
}

func TestExternalAPIRouteInventory(t *testing.T) {
	f := newExternalAuthorizationFixture(t)
	cases := map[string]bool{}
	for _, tc := range externalAuthorizationCases(t) {
		require.False(t, cases[tc.route], "duplicate test case: %s", tc.route)
		cases[tc.route] = true
	}
	registered := map[string]bool{}
	for _, route := range f.server.app.GetRoutes(true) {
		if route.Method == http.MethodHead || (!strings.HasPrefix(route.Path, "/api/v1/") && !strings.HasPrefix(route.Path, "/openai/v1/") && !strings.HasPrefix(route.Path, "/anthropic/v1/")) {
			continue
		}
		key := route.Method + " " + route.Path
		if key == "POST /api/v1/gateways/:namespace/:name/events" {
			continue // Bound-secret ingress has a separate tested contract.
		}
		require.Contains(t, externalAPIPolicies, key, "external route lacks an explicit policy")
		require.True(t, cases[key], "external route lacks an HTTP authorization test: %s", key)
		registered[key] = true
	}
	require.Equal(t, cases, registered)
	require.Len(t, externalAPIPolicies, len(cases))
}

func TestExternalAPIEveryPermissionDeniesBeforeEffects(t *testing.T) {
	for _, mode := range []string{"denied", "review-error", "evaluation-error", "explicit-denied", "missing-client", "typed-nil-client"} {
		t.Run(mode, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			for _, tc := range externalAuthorizationCases(t) {
				for deniedIndex := range tc.permissions {
					t.Run(fmt.Sprintf("%s/permission-%d", tc.route, deniedIndex), func(t *testing.T) {
						f.reviews = nil
						f.kubeCalls = 0
						f.clientset.ClearActions()
						before := f.changes(t)
						beforeTokenReviews := f.tokenReviews
						f.server.handlers.clientset = f.clientset
						if mode == "missing-client" {
							f.server.handlers.clientset = nil
						}
						if mode == "typed-nil-client" {
							var missing *kubernetes.Clientset
							f.server.handlers.clientset = missing
						}
						f.review = func(review *authorizationv1.SubjectAccessReview) error {
							index := len(f.reviews) - 1
							require.LessOrEqual(t, index, deniedIndex)
							f.requireIdentity(t, review.Spec)
							require.Equal(t, tc.permissions[index], *review.Spec.ResourceAttributes)
							review.Status.Allowed = index < deniedIndex
							if index == deniedIndex {
								switch mode {
								case "review-error":
									return errors.New("authorization is unavailable")
								case "evaluation-error":
									review.Status.Allowed = true
									review.Status.EvaluationError = "authorizer failed"
								case "explicit-denied":
									review.Status.Allowed = true
									review.Status.Denied = true
								}
							}
							return nil
						}
						method, path, _ := strings.Cut(tc.route, " ")
						for _, parameter := range []string{":approvalID", ":filename", ":sessionId", ":sliceID", ":name", ":id"} {
							path = strings.ReplaceAll(path, parameter, "protected")
						}
						status, body := f.request(t, method, path, `{"name":"created","metadata":{"name":"created"},"content":"new content","message":"hello","model":"protected/fixture","messages":[{"role":"user","content":"hello"}]}`)
						require.Equal(t, http.StatusForbidden, status, body)
						require.NotContains(t, body, "protected-content")
						require.Equal(t, beforeTokenReviews+1, f.tokenReviews, "request must pass the real TokenReview middleware")
						require.Zero(t, f.kubeCalls, "denied request touched a Kubernetes resource")
						require.Equal(t, before, f.changes(t), "denied request changed a record or queued work")
						require.Zero(t, f.externalCalls.Load(), "denied request contacted a provider")
						require.Empty(t, f.server.chatHandler.activeChats)
						if mode == "missing-client" || mode == "typed-nil-client" {
							require.Empty(t, f.reviews)
						} else {
							require.Len(t, f.reviews, deniedIndex+1)
							require.Len(t, f.clientset.Actions(), len(f.reviews), "unexpected Kubernetes side effect")
						}
						if strings.HasPrefix(path, "/openai/") || strings.HasPrefix(path, "/anthropic/") {
							require.Contains(t, body, `"type":"permission_error"`)
						}
					})
				}
			}
		})
	}
}
