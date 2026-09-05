/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const testWatchNamespace = "prod"

func TestHandlers_ProviderReadsRequireKubernetesRBACForTokenReviewUser(t *testing.T) {
	// TokenReview-authenticated reads run with the controller's credentials,
	// so the Provider list and get handlers must pass a SubjectAccessReview
	// for the caller before the orka-client Role grant means anything.
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)

	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: "default"},
		Spec:       corev1alpha1.ProviderSpec{Type: "openai", DefaultModel: "gpt-4o"},
	}
	tests := []struct {
		name       string
		method     string
		path       string
		wantVerb   string
		wantName   string
		allowed    bool
		wantStatus int
	}{
		{"list denied", http.MethodGet, "/providers?namespace=default", "list", "", false, http.StatusForbidden},
		{"list allowed", http.MethodGet, "/providers?namespace=default", "list", "", true, http.StatusOK},
		{"get denied", http.MethodGet, "/providers/openai?namespace=default", "get", "openai", false, http.StatusForbidden},
		{"get allowed", http.MethodGet, "/providers/openai?namespace=default", "get", "openai", true, http.StatusOK},
		// Authorization runs before the read, so a denied caller cannot
		// enumerate names through a 403-vs-404 difference.
		{"get denied unknown name", http.MethodGet, "/providers/missing?namespace=default", "get", "missing", false, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(provider.DeepCopy()).Build()
			kubeClient := kubefake.NewSimpleClientset()
			reviewed := false
			kubeClient.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
				require.Equal(t, "system:serviceaccount:default:limited", review.Spec.User)
				require.NotNil(t, review.Spec.ResourceAttributes)
				require.Equal(t, "default", review.Spec.ResourceAttributes.Namespace)
				require.Equal(t, tt.wantVerb, review.Spec.ResourceAttributes.Verb)
				require.Equal(t, corev1alpha1.GroupVersion.Group, review.Spec.ResourceAttributes.Group)
				require.Equal(t, "providers", review.Spec.ResourceAttributes.Resource)
				require.Equal(t, tt.wantName, review.Spec.ResourceAttributes.Name)
				reviewed = true
				review.Status.Allowed = tt.allowed
				return true, review, nil
			})

			handlers := NewHandlers(HandlersConfig{Client: fakeClient, KubeClient: kubeClient})
			app := fiber.New()
			app.Use(func(c fiber.Ctx) error {
				c.Locals(UserInfoContextKey, &UserInfo{
					Username: "system:serviceaccount:default:limited",
					Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:default"},
					AuthType: AuthTypeTokenReview,
				})
				return c.Next()
			})
			app.Get("/providers", handlers.ListProviders)
			app.Get("/providers/:name", handlers.GetProvider)

			resp, err := app.Test(httptest.NewRequest(tt.method, tt.path, nil))
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, tt.wantStatus, resp.StatusCode)
			require.True(t, reviewed, "expected a SubjectAccessReview")
		})
	}
}

func TestHandlers_CreateTaskRequiresKubernetesRBACForTokenReviewUser(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	kubeClient := kubefake.NewSimpleClientset()
	kubeClient.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		review := createAction.GetObject().(*authorizationv1.SubjectAccessReview)
		require.Equal(t, "system:serviceaccount:default:limited", review.Spec.User)
		require.Equal(t, []string{"system:serviceaccounts", "system:serviceaccounts:default"}, review.Spec.Groups)
		require.NotNil(t, review.Spec.ResourceAttributes)
		require.Equal(t, "default", review.Spec.ResourceAttributes.Namespace)
		require.Equal(t, "create", review.Spec.ResourceAttributes.Verb)
		require.Equal(t, corev1alpha1.GroupVersion.Group, review.Spec.ResourceAttributes.Group)
		require.Equal(t, "tasks", review.Spec.ResourceAttributes.Resource)
		review.Status.Allowed = false
		return true, review, nil
	})

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, KubeClient: kubeClient})
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{
			Username: "system:serviceaccount:default:limited",
			Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:default"},
			AuthType: AuthTypeTokenReview,
		})
		return c.Next()
	})
	app.Post("/tasks", handlers.CreateTask)

	resp := testJSONRequest(t, app, http.MethodPost, "/tasks", map[string]any{
		"name":  "denied-task",
		"type":  "container",
		"image": "alpine:3.20",
	})
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var created corev1alpha1.Task
	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "denied-task", Namespace: "default"}, &created)
	require.True(t, apierrors.IsNotFound(err), "denied task should not be created")
}

func TestHandlers_CreateTaskAllowsKubernetesRBACAuthorizedTokenReviewUser(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	kubeClient := kubefake.NewSimpleClientset()
	kubeClient.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		review := createAction.GetObject().(*authorizationv1.SubjectAccessReview)
		review.Status.Allowed = true
		return true, review, nil
	})

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, KubeClient: kubeClient})
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{
			Username: "system:serviceaccount:default:editor",
			AuthType: AuthTypeTokenReview,
		})
		return c.Next()
	})
	app.Post("/tasks", handlers.CreateTask)

	resp := testJSONRequest(t, app, http.MethodPost, "/tasks", map[string]any{
		"name":  "allowed-task",
		"type":  "container",
		"image": "alpine:3.20",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func setupTestHandlers() (*Handlers, *fiber.App) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	return handlers, app
}

func setupTestHandlersWithAuthz(t *testing.T, ctxTokenConfig ContextTokenConfig, mode string, objs ...runtime.Object) *fiber.App {
	t.Helper()
	app, _ := setupTestHandlersWithAuthzStore(t, ctxTokenConfig, mode, objs...)
	return app
}

func setupTestHandlersWithAuthzStore(
	t *testing.T,
	ctxTokenConfig ContextTokenConfig,
	mode string,
	objs ...runtime.Object,
) (*fiber.App, *sqlite.Store) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{
		Mode: mode,
	})
	require.NoError(t, err)
	handlers := NewHandlers(HandlersConfig{
		Client:                    fakeClient,
		SessionStore:              ss,
		ResultStore:               ss,
		PlanStore:                 ss,
		ArtifactStore:             ss,
		MemoryStore:               ss,
		MemoryProposalStore:       ss,
		ContextTokenAuthorization: authz,
	})

	app := fiber.New()
	app.Use(NewAuthMiddleware(handlers.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/tasks", handlers.CreateTask)
	app.Get("/tasks", handlers.ListTasks)
	app.Get("/tasks/:id", handlers.GetTask)
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)
	app.Get("/tasks/:id/result", handlers.GetTaskResult)
	app.Get("/tasks/:id/plan", handlers.GetTaskPlan)
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)
	app.Get("/tasks/:id/artifacts", handlers.ListTaskArtifacts)
	app.Get("/tasks/:id/artifacts/:filename", handlers.DownloadTaskArtifact)
	app.Delete("/tasks/:id", handlers.DeleteTask)
	app.Get("/tools", handlers.ListTools)
	app.Get("/tools/:name", handlers.GetTool)
	app.Get("/secrets", handlers.ListSecretNames)
	app.Post("/agents", handlers.CreateAgent)
	app.Get("/agents", handlers.ListAgents)
	app.Get("/agents/:name", handlers.GetAgent)
	app.Put("/agents/:name", handlers.UpdateAgent)
	app.Delete("/agents/:name", handlers.DeleteAgent)
	app.Get("/memories", handlers.ListMemories)
	app.Post("/memories", handlers.CreateMemory)
	app.Get("/memories/:id", handlers.GetMemory)
	app.Put("/memories/:id", handlers.UpdateMemory)
	app.Delete("/memories/:id", handlers.DeleteMemory)
	app.Post("/memory-proposals/:id/apply", handlers.ApplyMemoryProposal)
	app.Get("/sessions", handlers.ListSessions)
	app.Get("/sessions/:id", handlers.GetSession)
	app.Delete("/sessions/:id", handlers.DeleteSession)
	app.Post("/skills", handlers.CreateSkill)
	app.Get("/skills", handlers.ListSkills)
	app.Get("/skills/:name", handlers.GetSkill)
	app.Get("/skills/:name/content", handlers.GetSkillContent)
	app.Put("/skills/:name", handlers.UpdateSkill)
	app.Delete("/skills/:name", handlers.DeleteSkill)
	return app, ss
}

func setupTestCreateTaskHandlerWithAuthzReaders(
	t *testing.T,
	ctxTokenConfig ContextTokenConfig,
	mode string,
	cachedClient client.Client,
	apiReader client.Reader,
) *fiber.App {
	t.Helper()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	ss := sqlite.NewStore(db, ":memory:")
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: mode})
	require.NoError(t, err)
	handlers := NewHandlers(HandlersConfig{
		Client:                    cachedClient,
		APIReader:                 apiReader,
		SessionStore:              ss,
		ResultStore:               ss,
		ContextTokenAuthorization: authz,
	})

	app := fiber.New()
	app.Use(NewAuthMiddleware(cachedClient, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/tasks", handlers.CreateTask)
	return app
}

func setupTestHandlersWithObjects(objs ...runtime.Object) (*Handlers, *fiber.App) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	return handlers, app
}

func TestHandlers_Healthz(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/healthz", handlers.Healthz)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_Readyz(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/readyz", handlers.Readyz)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_Readyz_UsesUncachedNamedNamespaceRead(t *testing.T) {
	tests := []struct {
		name       string
		apiObjects []client.Object
		wantStatus int
	}{
		{
			name: "namespace exists",
			apiObjects: []client.Object{&corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: testWatchNamespace},
			}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "namespace read fails",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))

			cachedReads := 0
			cachedClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testWatchNamespace}}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						cachedReads++
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						cachedReads++
						return c.List(ctx, list, opts...)
					},
				}).
				Build()

			apiGets := 0
			apiLists := 0
			apiReader := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.apiObjects...).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						apiGets++
						require.Equal(t, client.ObjectKey{Name: testWatchNamespace}, key)
						require.IsType(t, &corev1.Namespace{}, obj)
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						apiLists++
						return c.List(ctx, list, opts...)
					},
				}).
				Build()

			handlers := NewHandlers(HandlersConfig{
				Client:         cachedClient,
				APIReader:      apiReader,
				WatchNamespace: testWatchNamespace,
			})
			app := fiber.New()
			app.Get("/readyz", handlers.Readyz)

			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", nil))
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, resp.StatusCode)
			require.Zero(t, cachedReads)
			require.Equal(t, 1, apiGets)
			require.Zero(t, apiLists)
		})
	}
}

func TestHandlers_CreateTask_Valid(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name:        "test-task",
		Namespace:   "default",
		Annotations: map[string]string{"example.com/purpose": "smoke"},
		Type:        corev1alpha1.TaskTypeContainer,
		Image:       "busybox",
		Command:     []string{"echo", "hello"},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	created := &corev1alpha1.Task{}
	if err := handlers.client.Get(context.Background(), types.NamespacedName{Name: "test-task", Namespace: "default"}, created); err != nil {
		t.Fatalf("failed to fetch created task: %v", err)
	}
	if created.Annotations["example.com/purpose"] != "smoke" {
		t.Fatalf("annotation example.com/purpose = %q, want smoke", created.Annotations["example.com/purpose"])
	}
}

func TestHandlers_CreateTask_MissingName(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Type: corev1alpha1.TaskTypeContainer,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_CreateTask_MissingType(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name: "test-task",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_CreateTask_DefaultNamespace(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name: "test-task",
		Type: corev1alpha1.TaskTypeContainer,
		// No namespace - should default to "default"
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestHandlers_CreateTask_StampsRequestedByFromOIDC(t *testing.T) {
	provider := newTestOIDCProvider(t)
	handlers, app := setupTestHandlers()
	app.Use(NewAuthMiddleware(handlers.client, AuthConfig{OIDC: provider.config()}))
	app.Post("/tasks", handlers.CreateTask)

	token := provider.issueToken(t, testOIDCTokenOptions{
		Subject:    "subject-456",
		Username:   "alex",
		Email:      "alex@example.test",
		Groups:     []string{"developers", "operators"},
		Roles:      []string{"creator"},
		RealmRoles: []string{"approver"},
	})
	body := CreateTaskRequest{
		Name:      "oidc-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
		Image:     "busybox",
		Command:   []string{"echo", "hello"},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	created := &corev1alpha1.Task{}
	if err := handlers.client.Get(context.Background(), types.NamespacedName{Name: "oidc-task", Namespace: "default"}, created); err != nil {
		t.Fatalf("failed to fetch created task: %v", err)
	}
	if created.Spec.RequestedBy == nil {
		t.Fatal("expected requestedBy to be stamped")
	}
	if created.Spec.RequestedBy.Subject != "subject-456" {
		t.Fatalf("requestedBy.subject = %q, want %q", created.Spec.RequestedBy.Subject, "subject-456")
	}
	if created.Spec.RequestedBy.Issuer != provider.server.URL {
		t.Fatalf("requestedBy.issuer = %q, want %q", created.Spec.RequestedBy.Issuer, provider.server.URL)
	}
	if created.Spec.RequestedBy.Username != "alex" {
		t.Fatalf("requestedBy.username = %q, want %q", created.Spec.RequestedBy.Username, "alex")
	}
	if created.Spec.RequestedBy.Email != "alex@example.test" {
		t.Fatalf("requestedBy.email = %q, want %q", created.Spec.RequestedBy.Email, "alex@example.test")
	}
	if strings.Join(created.Spec.RequestedBy.Groups, ",") != "developers,operators" {
		t.Fatalf("requestedBy.groups = %#v, want [developers operators]", created.Spec.RequestedBy.Groups)
	}
	if strings.Join(created.Spec.RequestedBy.Roles, ",") != "creator,approver" {
		t.Fatalf("requestedBy.roles = %#v, want [creator approver]", created.Spec.RequestedBy.Roles)
	}
}

func TestHandlers_CreateTask_StampsRequestedByFromContextToken(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	handlers, app := setupTestHandlers()
	app.Use(NewAuthMiddleware(handlers.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/tasks", handlers.CreateTask)

	token := issueTestContextToken(t, provider, nil, nil)
	body := CreateTaskRequest{
		Name:      "context-token-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
		Image:     "busybox",
		Command:   []string{"echo", "hello"},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	created := &corev1alpha1.Task{}
	if err := handlers.client.Get(context.Background(), types.NamespacedName{Name: "context-token-task", Namespace: "default"}, created); err != nil {
		t.Fatalf("failed to fetch created task: %v", err)
	}
	if created.Spec.RequestedBy == nil {
		t.Fatal("expected requestedBy to be stamped")
	}
	if created.Spec.RequestedBy.Subject != "workload-subject" {
		t.Fatalf("requestedBy.subject = %q, want %q", created.Spec.RequestedBy.Subject, "workload-subject")
	}
	if created.Spec.RequestedBy.Issuer != provider.server.URL {
		t.Fatalf("requestedBy.issuer = %q, want %q", created.Spec.RequestedBy.Issuer, provider.server.URL)
	}
	if strings.Join(created.Spec.RequestedBy.Roles, ",") != "read,write" {
		t.Fatalf("requestedBy.roles = %#v, want [read write]", created.Spec.RequestedBy.Roles)
	}
	if created.Spec.Transaction == nil {
		t.Fatal("expected transaction metadata to be stamped")
	}
	if created.Spec.Transaction.Profile != ContextTokenProfileTransactionToken {
		t.Fatalf("transaction.profile = %q, want %q", created.Spec.Transaction.Profile, ContextTokenProfileTransactionToken)
	}
	if created.Spec.Transaction.ID != testContextTokenTransactionID {
		t.Fatalf("transaction.id = %q, want txn-123", created.Spec.Transaction.ID)
	}
	if created.Spec.Transaction.RequestingWorkload != "spiffe://example.test/ns/default/sa/client" {
		t.Fatalf("transaction.requestingWorkload = %q", created.Spec.Transaction.RequestingWorkload)
	}
	if strings.Join(created.Spec.Transaction.Scopes, ",") != "read,write" {
		t.Fatalf("transaction.scopes = %#v, want [read write]", created.Spec.Transaction.Scopes)
	}
	if !strings.HasPrefix(created.Spec.Transaction.ContextDigest, "sha256:") {
		t.Fatalf("transaction.contextDigest = %q, want sha256 digest", created.Spec.Transaction.ContextDigest)
	}
	if !strings.HasPrefix(created.Spec.Transaction.RequesterContextDigest, "sha256:") {
		t.Fatalf("transaction.requesterContextDigest = %q, want sha256 digest", created.Spec.Transaction.RequesterContextDigest)
	}
	if created.Spec.Transaction.Context["trace_id"] != "trace-123" {
		t.Fatalf("transaction.context = %#v, want trace_id", created.Spec.Transaction.Context)
	}
	if created.Labels[labels.LabelTransactionID] != labels.SelectorValue(testContextTokenTransactionID) {
		t.Fatalf("transaction label = %q, want txn-123", created.Labels[labels.LabelTransactionID])
	}
	if created.Annotations[labels.AnnotationTransactionID] != testContextTokenTransactionID {
		t.Fatalf("transaction annotation = %q, want txn-123", created.Annotations[labels.AnnotationTransactionID])
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationEnforceAllowsMatchingToken(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate + " orka:agents:run " + ContextTokenScopeSecretsCredentialsRead,
		"tctx": map[string]any{
			"namespace":    "default",
			"taskType":     "agent",
			"agent":        "reviewer",
			"repo":         "https://github.com/orka-agents/orka.git",
			"branch":       "feature-branch",
			"allowedTools": []string{"file_read", "code_exec", "Bash"},
		},
	})
	body := CreateTaskRequest{
		Name:         "authorized-context-token-task",
		Namespace:    "default",
		Type:         corev1alpha1.TaskTypeAgent,
		AgentRef:     &corev1alpha1.AgentReference{Name: "reviewer"},
		AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"file_read"}},
		Workspace: &corev1alpha1.WorkspaceConfig{
			GitRepo: "https://github.com/orka-agents/orka.git",
			Branch:  "feature-branch",
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationEnforceRejectsMissingScope(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": "orka:tasks:get",
	})
	body := CreateTaskRequest{
		Name:      "unauthorized-context-token-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
		Image:     "busybox",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationEnforceRejectsContextMismatch(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace": "restricted",
		},
	})
	body := CreateTaskRequest{
		Name:      "context-mismatch-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
		Image:     "busybox",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationAuditAllowsFailures(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeAudit)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": "orka:tasks:get",
	})
	body := CreateTaskRequest{
		Name:      "audit-context-token-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
		Image:     "busybox",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationUsesAuthoritativeExternalRuntimePolicy(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	tests := []struct {
		name               string
		mode               string
		cachedAllowedTools []string
		apiAllowedTools    []string
		requestAllowed     []string
		tokenAllowed       []string
		nilAPIReader       bool
		failAPIReader      bool
		wantStatus         int
	}{
		{
			name:               "rejects cached match after authoritative policy expansion",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{"Read"},
			apiAllowedTools:    []string{"Read", "Write"},
			requestAllowed:     []string{"Read"},
			tokenAllowed:       []string{"Read"},
			wantStatus:         http.StatusForbidden,
		},
		{
			name:               "allows exact authoritative policy",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{"Read"},
			apiAllowedTools:    []string{"Read", "Write"},
			requestAllowed:     []string{"Write", "Read", "Read"},
			tokenAllowed:       []string{"Read", "Write"},
			wantStatus:         http.StatusCreated,
		},
		{
			name:               "allows explicit empty deny all policy",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{},
			apiAllowedTools:    []string{},
			requestAllowed:     []string{},
			tokenAllowed:       []string{},
			wantStatus:         http.StatusCreated,
		},
		{
			name:               "rejects omitted external allowed tools",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{},
			apiAllowedTools:    []string{},
			requestAllowed:     nil,
			tokenAllowed:       []string{},
			wantStatus:         http.StatusForbidden,
		},
		{
			name:               "uses cached client when API reader is absent",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{"Read"},
			requestAllowed:     []string{"Read"},
			tokenAllowed:       []string{"Read"},
			nilAPIReader:       true,
			wantStatus:         http.StatusCreated,
		},
		{
			name:               "does not fall back after authoritative read error",
			mode:               ContextTokenAuthorizationModeEnforce,
			cachedAllowedTools: []string{"Read"},
			requestAllowed:     []string{"Read"},
			tokenAllowed:       []string{"Read"},
			failAPIReader:      true,
			wantStatus:         http.StatusInternalServerError,
		},
		{
			name:               "audit mode records mismatch without blocking",
			mode:               ContextTokenAuthorizationModeAudit,
			cachedAllowedTools: []string{"Read"},
			apiAllowedTools:    []string{"Read", "Write"},
			requestAllowed:     []string{"Read"},
			tokenAllowed:       []string{"Read"},
			wantStatus:         http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cachedAgent, cachedRuntime := testExternalRuntimeAuthorizationObjects(tt.cachedAllowedTools)
			cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cachedAgent, cachedRuntime).Build()

			var apiReader client.Reader
			switch {
			case tt.nilAPIReader:
			case tt.failAPIReader:
				apiReader = fake.NewClientBuilder().
					WithScheme(scheme).
					WithInterceptorFuncs(interceptor.Funcs{
						Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
							return errors.New("authoritative API unavailable")
						},
					}).
					Build()
			default:
				apiAgent, apiRuntime := testExternalRuntimeAuthorizationObjects(tt.apiAllowedTools)
				apiReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(apiAgent, apiRuntime).Build()
			}

			app := setupTestCreateTaskHandlerWithAuthzReaders(t, ctxTokenConfig, tt.mode, cachedClient, apiReader)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeTaskCreate,
				"tctx": map[string]any{
					"namespace":    "default",
					"taskType":     "agent",
					"agent":        "agentkit",
					"allowedTools": tt.tokenAllowed,
				},
			})
			body := CreateTaskRequest{
				Name:         strings.ReplaceAll(tt.name, " ", "-"),
				Namespace:    "default",
				Type:         corev1alpha1.TaskTypeAgent,
				AgentRef:     &corev1alpha1.AgentReference{Name: "agentkit"},
				AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: tt.requestAllowed},
			}
			bodyBytes, err := json.Marshal(body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equalf(t, tt.wantStatus, resp.StatusCode, "response body: %s", respBody)

			created := &corev1alpha1.Task{}
			err = cachedClient.Get(context.Background(), client.ObjectKey{Name: body.Name, Namespace: body.Namespace}, created)
			if tt.wantStatus == http.StatusCreated {
				require.NoError(t, err)
			} else {
				require.True(t, apierrors.IsNotFound(err), "task must not be created after authorization failure: %v", err)
			}
		})
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationRejectsProviderModelResolvedFromAgentProviderCRD(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	providerCRD := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "anthropic-main", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "llm-key"},
			DefaultModel: "claude-3-5-sonnet",
		},
	}
	agent := testAgentFromJSON(t, `{
		"metadata": {
			"name": "reviewer",
			"namespace": "default"
		},
		"spec": {
			"providerRef": {
				"name": "anthropic-main"
			},
			"model": {
				"provider": "openai",
				"name": "claude-3-opus"
			}
		}
	}`)

	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, providerCRD, agent)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace":        "default",
			"provider":         "openai",
			"model":            "claude-3-5-sonnet",
			"allowedProviders": []string{"openai"},
			"allowedModels":    []string{"openai/claude-3-5-sonnet"},
		},
	})

	resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
		Name:      "agent-provider-model-denied",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeAgent,
		AgentRef:  &corev1alpha1.AgentReference{Name: "reviewer"},
		Prompt:    "review this change",
	})

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_CreateTask_ContextTokenAuthorizationUsesOpenCodeAgentIdentityOverAIOverrides(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	agent := testAgentFromJSON(t, `{
		"metadata": {"name": "opencode-agent", "namespace": "default"},
		"spec": {
			"model": {"name": "openai/gpt-5.4", "contextWindow": 32768, "maxTokens": 4096},
			"runtime": {"type": "opencode"}
		}
	}`)
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, agent)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace":        "default",
			"allowedProviders": []string{string(corev1alpha1.ProviderTypeAnthropic)},
			"allowedModels":    []string{"claude-sonnet-4"},
		},
	})

	resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
		Name:      "opencode-ai-override-denied",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeAgent,
		AgentRef:  &corev1alpha1.AgentReference{Name: "opencode-agent"},
		Prompt:    "review this change",
		AI: &corev1alpha1.AISpec{
			Provider: string(corev1alpha1.ProviderTypeAnthropic),
			Model:    "claude-sonnet-4",
		},
	})

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_CreateTask_ContextTokenAuthorizationRejectsCrossNamespaceProviderRefMatches(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	privilegedProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: "privileged"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeOpenAI,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "llm-key"},
			DefaultModel: "gpt-4o-mini",
		},
	}
	defaultProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeOpenAI,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "llm-key"},
			DefaultModel: "gpt-4o-mini",
		},
	}

	tests := []struct {
		name               string
		taskName           string
		transactionContext map[string]any
		providerRef        *corev1alpha1.ProviderReference
		wantStatus         int
	}{
		{
			name:     "denies cross-namespace providerRef despite bare allowed provider",
			taskName: "cross-ns-provider-denied",
			transactionContext: map[string]any{
				"namespace":        "default",
				"allowedProviders": []string{"llm"},
			},
			providerRef: &corev1alpha1.ProviderReference{
				Name:      "llm",
				Namespace: "privileged",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:     "denies cross-namespace providerRef via ambiguous allowedModels",
			taskName: "cross-ns-model-denied",
			transactionContext: map[string]any{
				"namespace":     "default",
				"allowedModels": []string{"llm/gpt-4o-mini"},
			},
			providerRef: &corev1alpha1.ProviderReference{
				Name:      "llm",
				Namespace: "privileged",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:     "denies unresolved cross-namespace providerRef",
			taskName: "missing-cross-ns-provider-denied",
			transactionContext: map[string]any{
				"namespace": "default",
			},
			providerRef: &corev1alpha1.ProviderReference{
				Name:      "missing-llm",
				Namespace: "privileged",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:     "allows bare provider match in token namespace",
			taskName: "same-ns-provider-allowed",
			transactionContext: map[string]any{
				"namespace":        "default",
				"allowedProviders": []string{"llm"},
			},
			providerRef: &corev1alpha1.ProviderReference{
				Name: "llm",
			},
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, privilegedProvider.DeepCopyObject(), defaultProvider.DeepCopyObject())
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeTaskCreate,
				"tctx":  tt.transactionContext,
			})

			resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
				Name:      tt.taskName,
				Namespace: "default",
				Type:      corev1alpha1.TaskTypeAI,
				Prompt:    "review this change",
				AI: &corev1alpha1.AISpec{
					ProviderRef: tt.providerRef,
				},
			})

			require.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

func TestHandlers_CreateTask_ContextTokenAuthorizationRejectsEffectiveAITools(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	agent := testAgentFromJSON(t, `{
		"metadata": {
			"name": "reviewer",
			"namespace": "default"
		},
		"spec": {
			"tools": [
				{
					"name": "web_search",
					"enabled": true
				},
				{
					"name": "disabled_tool",
					"enabled": false
				}
			]
		}
	}`)

	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, agent)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace":    "default",
			"allowedTools": []string{"file_read"},
		},
	})

	resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
		Name:      "ai-tools-denied",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeAI,
		AgentRef:  &corev1alpha1.AgentReference{Name: "reviewer"},
		Prompt:    "review this change",
		AI: &corev1alpha1.AISpec{
			Tools: []string{"file_write"},
		},
	})

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_CreateTask_ContextTokenAuthorizationRejectsCrossNamespaceAgentRef(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace": "default",
		},
	})

	resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
		Name:      "cross-namespace-agent-denied",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeAgent,
		AgentRef: &corev1alpha1.AgentReference{
			Name:      "reviewer",
			Namespace: "team-a",
		},
		Prompt: "review this change",
	})

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_CreateTask_ContextTokenAuthorizationRejectsAgentRuntimeDefaultAllowedTools(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	agent := testAgentFromJSON(t, `{
		"metadata": {
			"name": "claude-agent",
			"namespace": "default"
		},
		"spec": {
			"runtime": {
				"defaultAllowedTools": [
					"bash"
				]
			}
		}
	}`)

	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, agent)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskCreate,
		"tctx": map[string]any{
			"namespace":    "default",
			"allowedTools": []string{"file_read"},
		},
	})

	resp := postCreateTaskWithContextToken(t, app, token, CreateTaskRequest{
		Name:      "runtime-default-tool-denied",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeAgent,
		AgentRef:  &corev1alpha1.AgentReference{Name: "claude-agent"},
		Prompt:    "make a change",
	})

	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func postCreateTaskWithContextToken(t *testing.T, app *fiber.App, token string, body CreateTaskRequest) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testAgentFromJSON(t *testing.T, raw string) *corev1alpha1.Agent {
	t.Helper()

	agent := &corev1alpha1.Agent{}
	require.NoError(t, json.Unmarshal([]byte(raw), agent))
	return agent
}

func TestHandlers_TaskActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name   string
		method string
		path   string
		scope  string
		want   int
	}{
		{name: "list allowed", method: http.MethodGet, path: "/tasks", scope: ContextTokenScopeTaskList, want: http.StatusOK},
		{name: "list denied by read scope", method: http.MethodGet, path: "/tasks", scope: ContextTokenScopeTaskGet, want: http.StatusForbidden},
		{name: "get allowed", method: http.MethodGet, path: "/tasks/authz-task", scope: ContextTokenScopeTaskGet, want: http.StatusOK},
		{name: "get denied by list scope", method: http.MethodGet, path: "/tasks/authz-task", scope: ContextTokenScopeTaskList, want: http.StatusForbidden},
		{name: "delete allowed", method: http.MethodDelete, path: "/tasks/authz-task", scope: ContextTokenScopeTaskDelete, want: http.StatusNoContent},
		{name: "delete denied by read scope", method: http.MethodDelete, path: "/tasks/authz-task", scope: ContextTokenScopeTaskGet, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "authz-task", Namespace: "default"},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
			}
			app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, task)
			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.want {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHandlers_TaskReadActions_ContextTokenAuthorizationEnforcesTaskNameContext(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	const taskName = "authz-task"

	tests := []struct {
		name    string
		path    string
		prepare func(t *testing.T, ss *sqlite.Store)
	}{
		{
			name: "get task",
			path: "/tasks/" + taskName,
		},
		{
			name: "get task logs",
			path: "/tasks/" + taskName + "/logs",
			prepare: func(t *testing.T, ss *sqlite.Store) {
				t.Helper()
				require.NoError(t, ss.SaveResult(context.Background(), "default", taskName, []byte("logs")))
			},
		},
		{
			name: "get task result",
			path: "/tasks/" + taskName + "/result",
			prepare: func(t *testing.T, ss *sqlite.Store) {
				t.Helper()
				require.NoError(t, ss.SaveResult(context.Background(), "default", taskName, []byte("result")))
			},
		},
		{
			name: "get task plan",
			path: "/tasks/" + taskName + "/plan",
			prepare: func(t *testing.T, ss *sqlite.Store) {
				t.Helper()
				require.NoError(t, ss.SavePlan(context.Background(), "default", taskName, &store.PlanState{
					TaskName:    taskName,
					Namespace:   "default",
					Summary:     "in progress",
					ProgressPct: 50,
				}))
			},
		},
		{
			name: "get task children",
			path: "/tasks/" + taskName + "/children",
		},
		{
			name: "list task artifacts",
			path: "/tasks/" + taskName + "/artifacts",
		},
		{
			name: "download task artifact",
			path: "/tasks/" + taskName + "/artifacts/report.txt",
			prepare: func(t *testing.T, ss *sqlite.Store) {
				t.Helper()
				require.NoError(t, ss.SaveArtifact(context.Background(), "default", taskName, "report.txt", "text/plain", []byte("artifact")))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: "default"},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
				Status: corev1alpha1.TaskStatus{
					ResultRef: &corev1alpha1.ResultReference{Available: true},
				},
			}
			child := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "authz-child",
					Namespace: "default",
					Labels: map[string]string{
						labels.LabelParentTask: labels.SelectorValue(taskName),
					},
				},
				Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
			}

			t.Run("allows matching taskName", func(t *testing.T) {
				app, ss := setupTestHandlersWithAuthzStore(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, task.DeepCopyObject(), child.DeepCopyObject())
				if tt.prepare != nil {
					tt.prepare(t, ss)
				}

				token := issueTestContextToken(t, provider, nil, map[string]any{
					"scope": ContextTokenScopeTaskGet,
					"tctx": map[string]any{
						"namespace": "default",
						"taskName":  taskName,
					},
				})
				req := httptest.NewRequest(http.MethodGet, tt.path, nil)
				req.Header.Set(TransactionTokenHeaderName, token)
				resp, err := app.Test(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
			})

			t.Run("denies mismatched taskName", func(t *testing.T) {
				app, ss := setupTestHandlersWithAuthzStore(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, task.DeepCopyObject(), child.DeepCopyObject())
				if tt.prepare != nil {
					tt.prepare(t, ss)
				}

				token := issueTestContextToken(t, provider, nil, map[string]any{
					"scope": ContextTokenScopeTaskGet,
					"tctx": map[string]any{
						"namespace": "default",
						"taskName":  "other-task",
					},
				})
				req := httptest.NewRequest(http.MethodGet, tt.path, nil)
				req.Header.Set(TransactionTokenHeaderName, token)
				resp, err := app.Test(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusForbidden, resp.StatusCode)
			})
		})
	}
}

func TestHandlers_GetTask_ContextTokenAuthorizationEnforcesLoadedTaskRepoContext(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/allowed.git",
			},
		},
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, task)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskGet,
		"tctx": map[string]any{
			"repo": "https://github.com/acme/other.git",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/tasks/repo-task", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_ListTasks_ContextTokenAuthorizationFiltersLoadedTaskContext(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	allowedTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "allowed-repo-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/allowed.git",
			},
		},
	}
	otherTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "other-repo-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/other.git",
			},
		},
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, allowedTask, otherTask)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskList,
		"tctx": map[string]any{
			"repo":     "https://github.com/acme/allowed.git",
			"agent":    "reviewer",
			"taskType": string(corev1alpha1.TaskTypeAgent),
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Items []corev1alpha1.Task `json:"items"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Len(t, body.Items, 1)
	require.Equal(t, "allowed-repo-task", body.Items[0].Name)
}

func TestHandlers_GetTaskChildren_ContextTokenAuthorizationFiltersLoadedTaskContext(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/allowed.git",
			},
		},
	}
	allowedChild := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "allowed-child",
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelParentTask: labels.SelectorValue("parent-task"),
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/allowed.git",
			},
		},
	}
	otherChild := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-child",
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelParentTask: labels.SelectorValue("parent-task"),
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/other.git",
			},
		},
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, parentTask, allowedChild, otherChild)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskGet,
		"tctx": map[string]any{
			"namespace": "default",
			"taskName":  "parent-task",
			"repo":      "https://github.com/acme/allowed.git",
			"agent":     "reviewer",
			"taskType":  string(corev1alpha1.TaskTypeAgent),
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/tasks/parent-task/children", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Items []corev1alpha1.Task `json:"items"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Len(t, body.Items, 1)
	require.Equal(t, "allowed-child", body.Items[0].Name)
}

func TestHandlers_DeleteTask_ContextTokenAuthorizationEnforcesLoadedTaskRepoContext(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/acme/allowed.git",
			},
		},
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, task)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeTaskDelete,
		"tctx": map[string]any{
			"repo": "https://github.com/acme/other.git",
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/tasks/repo-task", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_GenericActions_ContextTokenAuthorizationEnforceRejectsNamespaceMismatch(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		scope  string
	}{
		{
			name:   "task list",
			method: http.MethodGet,
			path:   "/tasks?namespace=other",
			scope:  ContextTokenScopeTaskList,
		},
		{
			name:   "task get",
			method: http.MethodGet,
			path:   "/tasks/authz-task?namespace=other",
			scope:  ContextTokenScopeTaskGet,
		},
		{
			name:   "task delete",
			method: http.MethodDelete,
			path:   "/tasks/authz-task?namespace=other",
			scope:  ContextTokenScopeTaskDelete,
		},
		{
			name:   "agent create",
			method: http.MethodPost,
			path:   "/agents",
			body:   `{"name":"created-agent","namespace":"other","spec":{}}`,
			scope:  ContextTokenScopeAgentsWrite,
		},
		{
			name:   "skill create",
			method: http.MethodPost,
			path:   "/skills",
			body:   `{"name":"created-skill","namespace":"other","spec":{"description":"create","content":{"inline":"# Create"}}}`,
			scope:  ContextTokenScopeSkillsWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": tt.scope,
				"tctx":  map[string]any{"namespace": "default"},
			})
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}

func TestHandlers_ToolAndAgentActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "authz-agent", Namespace: "default"},
		Spec:       corev1alpha1.AgentSpec{},
	}

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		scope  string
		want   int
	}{
		{name: "list tools allowed", method: http.MethodGet, path: "/tools", scope: ContextTokenScopeToolsRead, want: http.StatusOK},
		{name: "list tools denied", method: http.MethodGet, path: "/tools", scope: ContextTokenScopeAgentsRead, want: http.StatusForbidden},
		{name: "list agents allowed", method: http.MethodGet, path: "/agents", scope: ContextTokenScopeAgentsRead, want: http.StatusOK},
		{name: "list agents denied", method: http.MethodGet, path: "/agents", scope: ContextTokenScopeToolsRead, want: http.StatusForbidden},
		{name: "create agent allowed", method: http.MethodPost, path: "/agents", body: `{"name":"new-agent","spec":{}}`, scope: ContextTokenScopeAgentsWrite, want: http.StatusCreated},
		{name: "create agent denied", method: http.MethodPost, path: "/agents", body: `{"name":"new-agent","spec":{}}`, scope: ContextTokenScopeAgentsRead, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, agent.DeepCopyObject())
			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.want {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHandlers_MemoryActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		scope  string
		want   int
	}{
		{name: "list memories allowed", method: http.MethodGet, path: "/memories", scope: ContextTokenScopeMemoryRead, want: http.StatusOK},
		{name: "list memories denied", method: http.MethodGet, path: "/memories", scope: ContextTokenScopeMemoryWrite, want: http.StatusForbidden},
		{name: "create memory allowed", method: http.MethodPost, path: "/memories", body: `{"namespace":"default","content":"remember this"}`, scope: ContextTokenScopeMemoryWrite, want: http.StatusCreated},
		{name: "create memory denied", method: http.MethodPost, path: "/memories", body: `{"namespace":"default","content":"remember this"}`, scope: ContextTokenScopeMemoryRead, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)
			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.want {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHandlers_SessionActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	const authzSessionName = "authz-session"
	const authzSessionsPath = "/sessions"
	authzSessionPath := authzSessionsPath + "/" + authzSessionName

	tests := []struct {
		name        string
		mode        string
		method      string
		path        string
		scope       string
		seedSession bool
		want        int
	}{
		{
			name:        "session read denied without read scope",
			mode:        ContextTokenAuthorizationModeEnforce,
			method:      http.MethodGet,
			path:        authzSessionPath,
			scope:       ContextTokenScopeSkillsRead,
			seedSession: true,
			want:        http.StatusForbidden,
		},
		{
			name:        "session read allowed with read scope",
			mode:        ContextTokenAuthorizationModeEnforce,
			method:      http.MethodGet,
			path:        authzSessionPath,
			scope:       ContextTokenScopeSessionsRead,
			seedSession: true,
			want:        http.StatusOK,
		},
		{
			name:        "session delete denied without write scope",
			mode:        ContextTokenAuthorizationModeEnforce,
			method:      http.MethodDelete,
			path:        authzSessionPath,
			scope:       ContextTokenScopeSessionsRead,
			seedSession: true,
			want:        http.StatusForbidden,
		},
		{
			name:        "session delete allowed with write scope",
			mode:        ContextTokenAuthorizationModeEnforce,
			method:      http.MethodDelete,
			path:        authzSessionPath,
			scope:       ContextTokenScopeSessionsWrite,
			seedSession: true,
			want:        http.StatusNoContent,
		},
		{
			name:   "session audit mode allows missing scope",
			mode:   ContextTokenAuthorizationModeAudit,
			method: http.MethodGet,
			path:   authzSessionsPath,
			scope:  ContextTokenScopeSkillsRead,
			want:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, ss := setupTestHandlersWithAuthzStore(t, ctxTokenConfig, tt.mode)
			if tt.seedSession {
				require.NoError(t, ss.CreateSession(context.Background(), &store.SessionRecord{
					Namespace:   "default",
					Name:        authzSessionName,
					SessionType: "task",
				}))
			}

			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestHandlers_SkillActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	const authzSkillName = "authz-skill"
	const authzSkillsPath = "/skills"
	authzSkillPath := authzSkillsPath + "/" + authzSkillName
	writeBody := `{"name":"created-skill","spec":{"description":"create","content":{"inline":"# Create"}}}`
	updateBody := `{"spec":{"description":"update","content":{"inline":"# Update"}}}`
	seedSkill := func() *corev1alpha1.Skill {
		return &corev1alpha1.Skill{
			ObjectMeta: metav1.ObjectMeta{Name: authzSkillName, Namespace: "default"},
			Spec: corev1alpha1.SkillSpec{
				Description: "authz skill",
				Content: corev1alpha1.SkillContent{
					Inline: "# Authz",
				},
			},
		}
	}

	tests := []struct {
		name   string
		mode   string
		method string
		path   string
		body   string
		scope  string
		seed   bool
		want   int
	}{
		{
			name:   "skill list denied without read scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodGet,
			path:   authzSkillsPath,
			scope:  ContextTokenScopeSkillsWrite,
			want:   http.StatusForbidden,
		},
		{
			name:   "skill list allowed with read scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodGet,
			path:   authzSkillsPath,
			scope:  ContextTokenScopeSkillsRead,
			want:   http.StatusOK,
		},
		{
			name:   "skill get allowed with read scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodGet,
			path:   authzSkillPath,
			scope:  ContextTokenScopeSkillsRead,
			seed:   true,
			want:   http.StatusOK,
		},
		{
			name:   "skill content denied without read scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodGet,
			path:   authzSkillPath + "/content",
			scope:  ContextTokenScopeSkillsWrite,
			seed:   true,
			want:   http.StatusForbidden,
		},
		{
			name:   "skill content allowed with read scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodGet,
			path:   authzSkillPath + "/content",
			scope:  ContextTokenScopeSkillsRead,
			seed:   true,
			want:   http.StatusOK,
		},
		{
			name:   "skill create denied without write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodPost,
			path:   authzSkillsPath,
			body:   writeBody,
			scope:  ContextTokenScopeSkillsRead,
			want:   http.StatusForbidden,
		},
		{
			name:   "skill create allowed with write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodPost,
			path:   authzSkillsPath,
			body:   writeBody,
			scope:  ContextTokenScopeSkillsWrite,
			want:   http.StatusCreated,
		},
		{
			name:   "skill update denied without write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodPut,
			path:   authzSkillPath,
			body:   updateBody,
			scope:  ContextTokenScopeSkillsRead,
			seed:   true,
			want:   http.StatusForbidden,
		},
		{
			name:   "skill update allowed with write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodPut,
			path:   authzSkillPath,
			body:   updateBody,
			scope:  ContextTokenScopeSkillsWrite,
			seed:   true,
			want:   http.StatusOK,
		},
		{
			name:   "skill delete denied without write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodDelete,
			path:   authzSkillPath,
			scope:  ContextTokenScopeSkillsRead,
			seed:   true,
			want:   http.StatusForbidden,
		},
		{
			name:   "skill delete allowed with write scope",
			mode:   ContextTokenAuthorizationModeEnforce,
			method: http.MethodDelete,
			path:   authzSkillPath,
			scope:  ContextTokenScopeSkillsWrite,
			seed:   true,
			want:   http.StatusNoContent,
		},
		{
			name:   "skill audit mode allows missing scope",
			mode:   ContextTokenAuthorizationModeAudit,
			method: http.MethodGet,
			path:   authzSkillsPath,
			scope:  ContextTokenScopeSessionsRead,
			want:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []runtime.Object{}
			if tt.seed {
				objs = append(objs, seedSkill())
			}
			app := setupTestHandlersWithAuthz(t, ctxTokenConfig, tt.mode, objs...)
			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestHandlers_CreateTask_RejectsTopLevelRequestedBy(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := map[string]any{
		"name":        "tampered-task",
		"namespace":   "default",
		"type":        corev1alpha1.TaskTypeContainer,
		"requestedBy": map[string]any{"subject": "spoofed"},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_CreateTask_RejectsNestedSpecRequestedBy(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := map[string]any{
		"name":      "tampered-task",
		"namespace": "default",
		"type":      corev1alpha1.TaskTypeContainer,
		"spec": map[string]any{
			"requestedBy": map[string]any{"subject": "spoofed"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_CreateTask_RejectsClientSuppliedTransaction(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{
			name: "top-level transaction",
			body: map[string]any{
				"name":        "tampered-transaction",
				"namespace":   "default",
				"type":        corev1alpha1.TaskTypeContainer,
				"transaction": map[string]any{"id": "spoofed"},
			},
		},
		{
			name: "nested spec transaction",
			body: map[string]any{
				"name":      "tampered-spec-transaction",
				"namespace": "default",
				"type":      corev1alpha1.TaskTypeContainer,
				"spec": map[string]any{
					"transaction": map[string]any{"id": "spoofed"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlers, app := setupTestHandlers()
			app.Post("/tasks", handlers.CreateTask)

			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestHandlers_CreateTask_RejectsReservedLabels(t *testing.T) {
	for _, labelName := range []string{labels.LabelParentTask, labels.LabelCoordinator} {
		t.Run(labelName, func(t *testing.T) {
			handlers, app := setupTestHandlers()
			app.Post("/tasks", handlers.CreateTask)

			body := CreateTaskRequest{
				Name:      "reserved-label-task",
				Namespace: "default",
				Metadata: MetadataRequest{Labels: map[string]string{
					labelName: "reserved-value",
				}},
				Type:  corev1alpha1.TaskTypeContainer,
				Image: "busybox",
			}
			bodyBytes, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestHandlers_CreateTask_RejectsReservedAnnotations(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
	}{
		{name: "transaction token secret", annotation: labels.AnnotationTransactionTokenSecret},
		{name: "coordination injection control", annotation: labels.AnnotationDisableCoordinationToolInject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlers, app := setupTestHandlers()
			app.Post("/tasks", handlers.CreateTask)

			body := CreateTaskRequest{
				Name:        "reserved-annotation-task",
				Namespace:   "default",
				Annotations: map[string]string{tt.annotation: "reserved-value"},
				Type:        corev1alpha1.TaskTypeContainer,
				Image:       "busybox",
			}
			bodyBytes, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestHandlers_CreateTask_NamespaceScoped(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "allowed-ns"})

	app := fiber.New()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name:      "test-task",
		Namespace: "other-ns", // Different from watchNamespace
		Type:      corev1alpha1.TaskTypeContainer,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestHandlers_CreateTask_WithTimeout(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name:    "test-task",
		Type:    corev1alpha1.TaskTypeContainer,
		Timeout: "5m",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}

func TestHandlers_CreateTask_WithExecution(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name: "test-task",
		Type: corev1alpha1.TaskTypeContainer,
		Execution: &corev1alpha1.ExecutionSpec{
			RuntimeClassName: "gvisor",
			NodeSelector: map[string]string{
				"kubernetes.io/os": "linux",
			},
			Tolerations: []corev1.Toleration{
				{Key: "sandboxed", Operator: corev1.TolerationOpExists},
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{},
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	created := &corev1alpha1.Task{}
	err = handlers.client.Get(context.Background(), types.NamespacedName{Name: "test-task", Namespace: "default"}, created)
	if err != nil {
		t.Fatalf("failed to fetch created task: %v", err)
	}
	if created.Spec.Execution == nil {
		t.Fatal("expected execution to be set on created task")
	}
	if created.Spec.Execution.RuntimeClassName != "gvisor" {
		t.Fatalf("RuntimeClassName = %q, want %q", created.Spec.Execution.RuntimeClassName, "gvisor")
	}
	if got := created.Spec.Execution.NodeSelector["kubernetes.io/os"]; got != "linux" {
		t.Fatalf("NodeSelector[kubernetes.io/os] = %q, want %q", got, "linux")
	}
	if len(created.Spec.Execution.Tolerations) != 1 || created.Spec.Execution.Tolerations[0].Key != "sandboxed" {
		t.Fatalf("unexpected tolerations: %#v", created.Spec.Execution.Tolerations)
	}
	if created.Spec.Execution.Affinity == nil || created.Spec.Execution.Affinity.NodeAffinity == nil {
		t.Fatal("expected affinity to be preserved on created task")
	}
}

func TestHandlers_CreateTask_InvalidTimeout(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name:    "test-task",
		Type:    corev1alpha1.TaskTypeContainer,
		Timeout: "invalid",
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_CreateTask_AlreadyExists(t *testing.T) {
	existingTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	handlers, app := setupTestHandlersWithObjects(existingTask)
	app.Post("/tasks", handlers.CreateTask)

	body := CreateTaskRequest{
		Name:      "existing-task",
		Namespace: "default",
		Type:      corev1alpha1.TaskTypeContainer,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}

func TestHandlers_ListTasks(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks", handlers.ListTasks)

	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_ListTasks_WithPagination(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks", handlers.ListTasks)

	req := httptest.NewRequest(http.MethodGet, "/tasks?limit=10", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_ListTasks_LimitZeroDisablesPagination(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks", handlers.ListTasks)

	req := httptest.NewRequest(http.MethodGet, "/tasks?limit=0", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_ListTasks_LimitZeroRejectsContinue(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks", handlers.ListTasks)

	req := httptest.NewRequest(http.MethodGet, "/tasks?limit=0&continue=next", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlers_ListTasks_WithNamespaceFilter(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks", handlers.ListTasks)

	req := httptest.NewRequest(http.MethodGet, "/tasks?namespace=custom-ns", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTask_Found(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id", handlers.GetTask)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTask_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id", handlers.GetTask)

	req := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_DeleteTask_Success(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Delete("/tasks/:id", handlers.DeleteTask)

	req := httptest.NewRequest(http.MethodDelete, "/tasks/test-task", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestHandlers_DeleteTask_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Delete("/tasks/:id", handlers.DeleteTask)

	req := httptest.NewRequest(http.MethodDelete, "/tasks/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskLogs_NoJob(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			JobName: "", // No job
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/logs", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskLogs_WithJob(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			JobName: "test-job",
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/logs", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	// Returns OK with placeholder message
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTaskResult_Success(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	// Save result to store
	require.NoError(t, handlers.resultStore.SaveResult(context.Background(), "default", "test-task", []byte("task result content")))
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTaskResult_NoResult(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: nil, // No result
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskResult_ResultNotFound(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_ListTools(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tools", handlers.ListTools)

	req := httptest.NewRequest(http.MethodGet, "/tools", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTool_Builtin(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tools/:name", handlers.GetTool)

	builtinTools := []string{"web_search", "code_exec", "file_read"}
	for _, toolName := range builtinTools {
		t.Run(toolName, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tools/"+toolName, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
			}

			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			params, ok := body["parameters"].(map[string]any)
			if !ok {
				t.Fatalf("parameters missing or invalid type: %T", body["parameters"])
			}
			if got := params["type"]; got != "object" {
				t.Fatalf("parameters.type = %v, want object", got)
			}
		})
	}
}

func TestHandlers_GetTool_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tools/:name", handlers.GetTool)

	req := httptest.NewRequest(http.MethodGet, "/tools/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_ListAgents(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/agents", handlers.ListAgents)

	req := httptest.NewRequest(http.MethodGet, "/agents", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetAgent_Found(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(agent)
	app.Get("/agents/:name", handlers.GetAgent)

	req := httptest.NewRequest(http.MethodGet, "/agents/test-agent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetAgent_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/agents/:name", handlers.GetAgent)

	req := httptest.NewRequest(http.MethodGet, "/agents/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"seconds", "300s", false},
		{"minutes", "5m", false},
		{"hours", "1h", false},
		{"combined", "1h30m", false},
		{"invalid", "invalid", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDuration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Error("parseDuration() returned nil for valid input")
			}
		})
	}
}

func TestNewHandlers(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "test-ns"})
	if handlers == nil {
		t.Fatal("NewHandlers returned nil")
	}
	if handlers.watchNamespace != "test-ns" {
		t.Errorf("watchNamespace = %s, want test-ns", handlers.watchNamespace)
	}
}

type metadataOnlySessionStore struct {
	store.SessionStore
	typeReader transcriptSessionTypeReader
	getCalls   int
}

type sessionAccessRecordingStore struct {
	store.SessionStore
	typeReader  transcriptSessionTypeReader
	listCalls   int
	readCalls   int
	deleteCalls int
}

func (s *sessionAccessRecordingStore) ListSessions(ctx context.Context, namespace string) ([]store.SessionMetadata, error) {
	s.listCalls++
	return s.SessionStore.ListSessions(ctx, namespace)
}

func (s *sessionAccessRecordingStore) ListSessionsPage(ctx context.Context, namespace, afterName string, limit int, excludeType string) ([]store.SessionMetadata, bool, error) {
	s.listCalls++
	return s.SessionStore.ListSessionsPage(ctx, namespace, afterName, limit, excludeType)
}

func (s *metadataOnlySessionStore) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	s.getCalls++
	return s.SessionStore.GetSession(ctx, namespace, name)
}

func (s *metadataOnlySessionStore) GetSessionType(ctx context.Context, namespace, name string) (string, error) {
	return s.typeReader.GetSessionType(ctx, namespace, name)
}

func (s *sessionAccessRecordingStore) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	s.readCalls++
	return s.SessionStore.GetSession(ctx, namespace, name)
}

func (s *sessionAccessRecordingStore) GetSessionType(ctx context.Context, namespace, name string) (string, error) {
	s.readCalls++
	return s.typeReader.GetSessionType(ctx, namespace, name)
}

func (s *sessionAccessRecordingStore) DeleteSession(ctx context.Context, namespace, name string) error {
	s.deleteCalls++
	return s.SessionStore.DeleteSession(ctx, namespace, name)
}

func setupTestHandlersWithSessionManager() (*Handlers, *fiber.App, *sqlite.Store) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	return handlers, app, ss
}

// --- ListSessions tests ---

func TestHandlers_ListSessions_Success(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	ss.CreateSession(ctx, &store.SessionRecord{ //nolint:errcheck
		Namespace:    "default",
		Name:         "my-session",
		SessionType:  "task",
		MessageCount: 5,
		InputTokens:  100,
		OutputTokens: 200,
	})

	app.Get("/sessions", handlers.ListSessions)

	req := httptest.NewRequest(http.MethodGet, "/sessions?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result ListResponse
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items, ok := result.Items.([]any)
	if !ok || len(items) != 1 {
		t.Errorf("Expected 1 session, got %v", result.Items)
	}
}

func TestHandlers_ListSessions_HidesGatewaySessions(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "ordinary-session", SessionType: "task",
	}))
	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "gateway-session", SessionType: store.SessionTypeGateway,
	}))

	app.Get("/sessions", handlers.ListSessions)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	items, ok := result.Items.([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "ordinary-session", item["name"])
}

func TestHandlers_ListSessions_HonorsLimitAndContinue(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	for _, name := range []string{"session-a", "session-b", "session-c"} {
		require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
			Namespace: "default", Name: name, SessionType: "task",
		}))
	}

	app.Get("/sessions", handlers.ListSessions)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default&limit=2", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page1 ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page1))
	items, ok := page1.Items.([]any)
	require.True(t, ok)
	require.Len(t, items, 2)
	require.Equal(t, "session-b", page1.Metadata.Continue)

	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default&limit=2&continue="+page1.Metadata.Continue, nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page2 ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page2))
	items, ok = page2.Items.([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "session-c", item["name"])
	require.Empty(t, page2.Metadata.Continue)

	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default&limit=bogus", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_ListSessions_CursorEqualToCacheSentinelResumes(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	// Sorted by name: "continue-not-supported" < "session-z". A page that
	// ends on the sentinel-named session must still resume after it.
	for _, name := range []string{"continue-not-supported", "session-z"} {
		require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
			Namespace: "default", Name: name, SessionType: "task",
		}))
	}
	app.Get("/sessions", handlers.ListSessions)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default&limit=1", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page1 ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page1))
	require.Equal(t, "continue-not-supported", page1.Metadata.Continue)

	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/sessions?namespace=default&limit=1&continue="+page1.Metadata.Continue, nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page2 ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page2))
	items, ok := page2.Items.([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "session-z", item["name"])
	require.Empty(t, page2.Metadata.Continue)
}

func TestHandlers_ListSessions_Empty(t *testing.T) {
	handlers, app, _ := setupTestHandlersWithSessionManager()
	app.Get("/sessions", handlers.ListSessions)

	req := httptest.NewRequest(http.MethodGet, "/sessions?namespace=default", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result ListResponse
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items, ok := result.Items.([]any)
	if !ok || len(items) != 0 {
		t.Errorf("Expected 0 sessions, got %v", result.Items)
	}
}

func TestHandlers_ListSessions_DefaultNamespace(t *testing.T) {
	handlers, app, _ := setupTestHandlersWithSessionManager()
	app.Get("/sessions", handlers.ListSessions)

	// No namespace query param - should default to "default"
	req := httptest.NewRequest(http.MethodGet, "/sessions", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_ListSessions_WatchNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns", SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/sessions", handlers.ListSessions)

	// No namespace provided - should use watchNamespace
	req := httptest.NewRequest(http.MethodGet, "/sessions", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// --- GetSession tests ---

func TestHandlers_SessionEndpointsRequireKubernetesRBACForTokenReviewUser(t *testing.T) {
	const (
		protectedSessionName = "protected-session"
		sessionsPath         = "/sessions"
		protectedSessionPath = sessionsPath + "/" + protectedSessionName
	)
	tests := []struct {
		name         string
		method       string
		path         string
		verb         string
		resourceName string
		allowed      bool
		wantStatus   int
	}{
		{name: "list denied", method: http.MethodGet, path: sessionsPath, verb: gatewayVerbList, wantStatus: http.StatusForbidden},
		{name: "list allowed", method: http.MethodGet, path: sessionsPath, verb: gatewayVerbList, allowed: true, wantStatus: http.StatusOK},
		{name: "get denied", method: http.MethodGet, path: protectedSessionPath, verb: gatewayVerbGet, resourceName: protectedSessionName, wantStatus: http.StatusForbidden},
		{name: "get allowed", method: http.MethodGet, path: protectedSessionPath, verb: gatewayVerbGet, resourceName: protectedSessionName, allowed: true, wantStatus: http.StatusOK},
		{name: "events denied", method: http.MethodGet, path: protectedSessionPath + "/events", verb: gatewayVerbGet, resourceName: protectedSessionName, wantStatus: http.StatusForbidden},
		{name: "stream denied", method: http.MethodGet, path: protectedSessionPath + "/stream", verb: gatewayVerbGet, resourceName: protectedSessionName, wantStatus: http.StatusForbidden},
		{name: "delete denied", method: http.MethodDelete, path: protectedSessionPath, verb: "delete", resourceName: protectedSessionName, wantStatus: http.StatusForbidden},
		{name: "delete allowed", method: http.MethodDelete, path: protectedSessionPath, verb: "delete", resourceName: protectedSessionName, allowed: true, wantStatus: http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1alpha1.AddToScheme(scheme))
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			db, err := sqlite.NewDB(":memory:")
			require.NoError(t, err)
			ss := sqlite.NewStore(db, ":memory:")
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			require.NoError(t, ss.CreateSession(context.Background(), &store.SessionRecord{
				Namespace: "default", Name: protectedSessionName, SessionType: "task",
			}))
			require.NoError(t, ss.AppendMessages(context.Background(), "default", protectedSessionName, []store.SessionMessage{{
				Role: testRoleUser, Content: "private prompt",
			}}))
			recordingStore := &sessionAccessRecordingStore{SessionStore: ss, typeReader: ss}
			kubeClient := kubefake.NewSimpleClientset()
			kubeClient.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				createAction := action.(k8stesting.CreateAction)
				review := createAction.GetObject().(*authorizationv1.SubjectAccessReview)
				require.Equal(t, "system:serviceaccount:default:limited", review.Spec.User)
				require.Equal(t, []string{"system:serviceaccounts", "system:serviceaccounts:default"}, review.Spec.Groups)
				require.NotNil(t, review.Spec.ResourceAttributes)
				require.Equal(t, "default", review.Spec.ResourceAttributes.Namespace)
				require.Equal(t, test.verb, review.Spec.ResourceAttributes.Verb)
				require.Equal(t, corev1alpha1.GroupVersion.Group, review.Spec.ResourceAttributes.Group)
				require.Equal(t, "sessions", review.Spec.ResourceAttributes.Resource)
				require.Equal(t, test.resourceName, review.Spec.ResourceAttributes.Name)
				review.Status.Allowed = test.allowed
				return true, review, nil
			})

			handlers := NewHandlers(HandlersConfig{
				Client: fakeClient, KubeClient: kubeClient, SessionStore: recordingStore, ResultStore: ss,
			})
			app := fiber.New()
			app.Use(tokenReviewUserMiddleware(limitedTokenReviewUser("default")))
			app.Get("/sessions", handlers.ListSessions)
			app.Get("/sessions/:id", handlers.GetSession)
			app.Get("/sessions/:id/events", handlers.ListSessionEvents)
			app.Get("/sessions/:id/stream", handlers.StreamSessionEvents)
			app.Delete("/sessions/:id", handlers.DeleteSession)

			resp, err := app.Test(httptest.NewRequest(test.method, test.path, nil))
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, resp.StatusCode)
			if !test.allowed {
				require.Zero(t, recordingStore.listCalls, "denied request must not list Session state")
				require.Zero(t, recordingStore.readCalls, "denied request must not read Session state")
				require.Zero(t, recordingStore.deleteCalls, "denied request must not delete Session state")
			}
			_, getErr := ss.GetSession(context.Background(), "default", protectedSessionName)
			if test.method == http.MethodDelete && test.allowed {
				require.ErrorIs(t, getErr, store.ErrNotFound)
			} else {
				require.NoError(t, getErr)
			}
		})
	}
}

func TestHandlers_GetSession_Success(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	ss.CreateSession(ctx, &store.SessionRecord{ //nolint:errcheck
		Namespace:    "default",
		Name:         "my-session",
		SessionType:  "task",
		MessageCount: 3,
		InputTokens:  50,
		OutputTokens: 100,
	})

	app.Get("/sessions/:id", handlers.GetSession)

	req := httptest.NewRequest(http.MethodGet, "/sessions/my-session", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	if result["name"] != "my-session" {
		t.Errorf("name = %v, want my-session", result["name"])
	}
	if result["messageCount"] != float64(3) {
		t.Errorf("messageCount = %v, want 3", result["messageCount"])
	}
}

func TestHandlers_GetSessionProjectsSafeExecutionControl(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	const sessionName = "lineage-session"
	control := &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Name:            storekube.RuntimeSessionControlObjectName(sessionName),
			Namespace:       "default",
			ResourceVersion: "42",
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName:          sessionName,
			SessionUID:           "session-uid",
			RuntimePoolRef:       "opencode-read",
			RuntimeProfileDigest: "sha256:" + strings.Repeat("a", 64),
		},
		Status: corev1alpha1.RuntimeSessionControlStatus{
			Generation:              3,
			Lifecycle:               corev1alpha1.RuntimeSessionControlLifecycle("Finalizing"),
			Availability:            corev1alpha1.RuntimeSessionControlAvailability("ReconciliationBlocked"),
			MutationLeaseGeneration: 8,
			BlockedReason:           "publication outcome unknown",
			RelatedPublicationID:    "publication-1",
			Lineage: &corev1alpha1.RuntimeSessionLineageStatus{
				NamespaceUID:    "namespace-uid",
				SessionUID:      "session-uid",
				ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
				Generation:      1,
				RuntimeIdentity: "opencode",
				ConfigDigest:    "sha256:" + strings.Repeat("b", 64),
				EstablishedAt:   metav1.NewTime(time.Date(2026, time.August, 6, 0, 0, 0, 0, time.UTC)),
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(control).Build()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	ss := sqlite.NewStore(db, ":memory:")
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, ss.CreateSession(context.Background(), &store.SessionRecord{
		Namespace: "default", Name: sessionName, SessionType: "task",
	}))

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, APIReader: fakeClient, SessionStore: ss})
	app := fiber.New()
	app.Get("/sessions/:id", handlers.GetSession)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions/"+sessionName, nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	projected := result["executionControl"].(map[string]any)
	require.Equal(t, "session-uid", projected["sessionUID"])
	require.Equal(t, "ReconciliationBlocked", projected["availability"])
	require.Equal(t, "publication outcome unknown", projected["blockedReason"])
	lineage := projected["lineage"].(map[string]any)
	require.Equal(t, "orka.harness.v2", lineage["contractVersion"])
	require.Equal(t, "opencode", lineage["runtimeIdentity"])
	require.NotContains(t, projected, "mutationLease")
	require.NotContains(t, projected, "controllerEpoch")
}

func TestHandlers_GetSession_HidesGatewaySession(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "gateway-session", SessionType: store.SessionTypeGateway,
	}))
	require.NoError(t, ss.AppendMessages(ctx, "default", "gateway-session", []store.SessionMessage{{
		Role: "user", Content: "private gateway message",
	}}))

	app.Get("/sessions/:id", handlers.GetSession)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions/gateway-session", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlers_GetSessionRejectsGatewayBeforeLoadingTranscript(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "gateway-metadata-only", SessionType: store.SessionTypeGateway,
	}))
	require.NoError(t, ss.AppendMessages(ctx, "default", "gateway-metadata-only", []store.SessionMessage{{
		Role: "user", Content: strings.Repeat("private", 1024),
	}}))
	wrapped := &metadataOnlySessionStore{SessionStore: ss, typeReader: ss}
	handlers.sessionStore = wrapped
	app.Get("/sessions/:id", handlers.GetSession)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/sessions/gateway-metadata-only", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Zero(t, wrapped.getCalls)
}

func TestHandlers_GetSession_NotFound(t *testing.T) {
	handlers, app, _ := setupTestHandlersWithSessionManager()
	app.Get("/sessions/:id", handlers.GetSession)

	req := httptest.NewRequest(http.MethodGet, "/sessions/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetSession_WatchNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns", SessionStore: ss, ResultStore: ss})

	ctx := context.Background()
	ss.CreateSession(ctx, &store.SessionRecord{ //nolint:errcheck
		Namespace:   "watched-ns",
		Name:        "my-session",
		SessionType: "task",
	})

	app := fiber.New()
	app.Get("/sessions/:id", handlers.GetSession)

	// explicit namespace that doesn't match watchNamespace should be rejected
	req := httptest.NewRequest(http.MethodGet, "/sessions/my-session?namespace=other", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

// --- DeleteSession tests ---

func TestHandlers_DeleteSession_Success(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	ss.CreateSession(ctx, &store.SessionRecord{ //nolint:errcheck
		Namespace:   "default",
		Name:        "my-session",
		SessionType: "task",
	})

	app.Delete("/sessions/:id", handlers.DeleteSession)

	req := httptest.NewRequest(http.MethodDelete, "/sessions/my-session", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestHandlers_DeleteSession_HidesAndPreservesGatewaySession(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()
	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "gateway-session", SessionType: store.SessionTypeGateway,
	}))

	app.Delete("/sessions/:id", handlers.DeleteSession)
	resp, err := app.Test(httptest.NewRequest(http.MethodDelete, "/sessions/gateway-session", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	session, err := ss.GetSession(ctx, "default", "gateway-session")
	require.NoError(t, err)
	require.Equal(t, store.SessionTypeGateway, session.SessionType)
}

func TestHandlers_DeleteSession_NotFound(t *testing.T) {
	handlers, app, _ := setupTestHandlersWithSessionManager()
	app.Delete("/sessions/:id", handlers.DeleteSession)

	req := httptest.NewRequest(http.MethodDelete, "/sessions/nonexistent", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	// SQLite DELETE is a no-op when not found, not an error
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestHandlers_DeleteSession_WatchNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns", SessionStore: ss, ResultStore: ss})

	ctx := context.Background()
	ss.CreateSession(ctx, &store.SessionRecord{ //nolint:errcheck
		Namespace:   "watched-ns",
		Name:        "my-session",
		SessionType: "task",
	})

	app := fiber.New()
	app.Delete("/sessions/:id", handlers.DeleteSession)

	req := httptest.NewRequest(http.MethodDelete, "/sessions/my-session", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// --- GetTaskLogs additional tests ---

func TestHandlers_GetTaskLogs_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent/logs", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskLogs_WatchNamespace(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "watched-ns",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			JobName: "test-job",
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns"})

	app := fiber.New()
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/logs", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// --- GetTaskResult additional tests ---

func TestHandlers_GetTaskResult_TaskNotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskResult_MissingKey(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	resultCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task-result",
			Namespace: "default",
		},
		Data: map[string]string{
			"output": "task result content",
		},
	}

	handlers, app := setupTestHandlersWithObjects(task, resultCM)
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHandlers_GetTaskResult_DefaultKey(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	handlers.resultStore.SaveResult(context.Background(), "default", "test-task", []byte("default key content")) //nolint:errcheck
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandlers_GetTaskResult_WatchNamespace(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "watched-ns",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	ss.SaveResult(context.Background(), "watched-ns", "test-task", []byte("task result content")) //nolint:errcheck
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns", SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/tasks/:id/result", handlers.GetTaskResult)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test-task/result", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// --- DeleteTask additional tests ---

func TestHandlers_DeleteTask_WatchNamespace(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-task",
			Namespace: "watched-ns",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns"})

	app := fiber.New()
	app.Delete("/tasks/:id", handlers.DeleteTask)

	req := httptest.NewRequest(http.MethodDelete, "/tasks/test-task", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// readLines is a test helper that reads lines from a reader into a channel.
func readLines(r io.Reader) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			ch <- scanner.Text()
		}
	}()
	return ch
}

// --- readLines tests ---

func TestReadLines(t *testing.T) {
	input := "line1\nline2\nline3\n"
	r := strings.NewReader(input)
	ch := readLines(r)

	lines := make([]string, 0, 3)
	for line := range ch {
		lines = append(lines, line)
	}

	if len(lines) != 3 {
		t.Fatalf("Expected 3 lines, got %d", len(lines))
	}
	if lines[0] != "line1" || lines[1] != "line2" || lines[2] != "line3" {
		t.Errorf("Unexpected lines: %v", lines)
	}
}

func TestReadLines_Empty(t *testing.T) {
	r := strings.NewReader("")
	ch := readLines(r)

	lines := make([]string, 0, 1)
	for line := range ch {
		lines = append(lines, line)
	}

	if len(lines) != 0 {
		t.Errorf("Expected 0 lines, got %d", len(lines))
	}
}

func TestReadLines_SingleLine(t *testing.T) {
	r := strings.NewReader("single line")
	ch := readLines(r)

	lines := make([]string, 0, 1)
	for line := range ch {
		lines = append(lines, line)
	}

	if len(lines) != 1 {
		t.Fatalf("Expected 1 line, got %d", len(lines))
	}
	if lines[0] != "single line" {
		t.Errorf("line = %q, want %q", lines[0], "single line")
	}
}

func TestReadLines_MultipleLines_NoTrailingNewline(t *testing.T) {
	r := strings.NewReader("line1\nline2")
	ch := readLines(r)

	lines := make([]string, 0, 2)
	for line := range ch {
		lines = append(lines, line)
	}

	if len(lines) != 2 {
		t.Fatalf("Expected 2 lines, got %d", len(lines))
	}
	if lines[0] != "line1" || lines[1] != "line2" {
		t.Errorf("Unexpected lines: %v", lines)
	}
}

func TestGetTaskChildren(t *testing.T) {
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	childTask1 := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-1",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-task"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	childTask2 := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-2",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-task"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	unrelatedTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unrelated",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}

	handlers, app := setupTestHandlersWithObjects(parentTask, childTask1, childTask2, unrelatedTask)
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)

	req := httptest.NewRequest(http.MethodGet, "/tasks/parent-task/children", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	items, ok := result.Items.([]any)
	if !ok {
		t.Fatalf("Items is not a slice")
	}

	if len(items) != 2 {
		t.Errorf("Expected 2 children, got %d", len(items))
	}
}

func TestGetTaskChildren_Empty(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)

	req := httptest.NewRequest(http.MethodGet, "/tasks/no-parent/children", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	items, ok := result.Items.([]any)
	if !ok {
		t.Fatalf("Items is not a slice")
	}

	if len(items) != 0 {
		t.Errorf("Expected 0 children, got %d", len(items))
	}
}

func TestGetTaskPlan(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plan-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss, PlanStore: ss})

	app := fiber.New()
	app.Get("/api/v1/tasks/:id/plan", handlers.GetTaskPlan)

	// Pre-save a plan into the store
	err := ss.SavePlan(context.Background(), "default", "plan-task", &store.PlanState{
		TaskName:     "plan-task",
		Namespace:    "default",
		Summary:      "working on it",
		ProgressPct:  75,
		GoalComplete: false,
		PlanDocument: "# My Plan\n- item 1",
	})
	require.NoError(t, err)

	t.Run("plan exists", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/plan-task/plan?namespace=default", nil)
		resp, err := app.Test(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var plan store.PlanState
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&plan))
		require.Equal(t, "working on it", plan.Summary)
		require.Equal(t, 75, plan.ProgressPct)
		require.Equal(t, "# My Plan\n- item 1", plan.PlanDocument)
	})

	t.Run("no plan", func(t *testing.T) {
		taskNoPlan := &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-plan-task",
				Namespace: "default",
			},
			Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAI,
			},
		}

		scheme2 := runtime.NewScheme()
		_ = corev1alpha1.AddToScheme(scheme2)
		_ = corev1.AddToScheme(scheme2)
		fakeClient2 := fake.NewClientBuilder().WithScheme(scheme2).WithRuntimeObjects(taskNoPlan).Build()
		db2, _ := sqlite.NewDB(":memory:")
		ss2 := sqlite.NewStore(db2, ":memory:")
		handlers2 := NewHandlers(HandlersConfig{Client: fakeClient2, SessionStore: ss2, ResultStore: ss2, PlanStore: ss2})

		app2 := fiber.New()
		app2.Get("/api/v1/tasks/:id/plan", handlers2.GetTaskPlan)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/no-plan-task/plan?namespace=default", nil)
		resp, err := app2.Test(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func TestResolveNamespace_IsolationEnforced(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, EnforceNamespaceIsolation: true, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/test", func(c fiber.Ctx) error {
		// Set user info in context (simulating auth middleware)
		c.Locals(UserInfoContextKey, &UserInfo{
			Username:  "system:serviceaccount:team-a:default",
			Namespace: "team-a",
		})
		ns, err := handlers.resolveNamespace(c, "team-b")
		if err != nil {
			return err
		}
		return c.SendString(ns)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestResolveNamespace_IsolationRejectsNamespaceLessAuthenticatedUser(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, EnforceNamespaceIsolation: true, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/test", func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{Username: "oidc-user", AuthType: AuthTypeOIDC})
		ns, err := handlers.resolveNamespace(c, "team-a")
		if err != nil {
			return err
		}
		return c.SendString(ns)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestResolveNamespace_IsolationAllowsSameNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, EnforceNamespaceIsolation: true, SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/test", func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, &UserInfo{
			Username:  "system:serviceaccount:team-a:default",
			Namespace: "team-a",
		})
		ns, err := handlers.resolveNamespace(c, "team-a")
		if err != nil {
			return err
		}
		return c.SendString(ns)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestResolveNamespace_WatchNamespaceMismatch(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	// Set watchNamespace to "production"
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "production", SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Get("/test", func(c fiber.Ctx) error {
		ns, err := handlers.resolveNamespace(c, "staging")
		if err != nil {
			return err
		}
		return c.SendString(ns)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// --- CreateAgent tests ---

func TestHandlers_CreateAgent_Success(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/agents", handlers.CreateAgent)

	body := CreateAgentRequest{
		Name:      "test-agent",
		Namespace: "default",
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestHandlers_CreateAgent_DefaultsBuiltInContractFromExecutionMode(t *testing.T) {
	tests := []struct {
		name       string
		mode       executionmode.Mode
		explicit   corev1alpha1.AgentRuntimeContractVersion
		want       corev1alpha1.AgentRuntimeContractVersion
		wantStatus int
	}{
		{
			name:       "harness v1",
			mode:       executionmode.HarnessV1,
			want:       corev1alpha1.AgentRuntimeContractHarnessV1,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "harness v2",
			mode:       executionmode.HarnessV2,
			want:       corev1alpha1.AgentRuntimeContractHarnessV2,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "opposite explicit contract",
			mode:       executionmode.HarnessV1,
			explicit:   corev1alpha1.AgentRuntimeContractHarnessV2,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1alpha1.AddToScheme(scheme))
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			handlers := NewHandlers(HandlersConfig{Client: fakeClient, ExecutionMode: test.mode})
			app := fiber.New()
			app.Post("/agents", handlers.CreateAgent)

			var explicit *corev1alpha1.AgentRuntimeContractVersion
			if test.explicit != "" {
				value := test.explicit
				explicit = &value
			}
			bodyBytes, err := json.Marshal(CreateAgentRequest{
				Name:      "runtime-agent",
				Namespace: "default",
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					Type:            corev1alpha1.AgentRuntimeCodex,
					ContractVersion: explicit,
				}},
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, resp.StatusCode)

			created := &corev1alpha1.Agent{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "runtime-agent", Namespace: "default"}, created)
			if test.wantStatus != http.StatusCreated {
				require.True(t, apierrors.IsNotFound(err), "rejected Agent should not be created")
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, created.BuiltInContractVersion())
		})
	}
}

func TestHandlers_CreateAgent_MetadataStyle(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/agents", handlers.CreateAgent)

	body := CreateAgentRequest{
		Metadata: MetadataRequest{
			Name:      "meta-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestHandlers_CreateAgent_MissingName(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/agents", handlers.CreateAgent)

	body := CreateAgentRequest{
		Spec: corev1alpha1.AgentSpec{},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_CreateAgent_InvalidBody(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/agents", handlers.CreateAgent)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_CreateAgent_AlreadyExists(t *testing.T) {
	existing := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{},
	}
	handlers, app := setupTestHandlersWithObjects(existing)
	app.Post("/agents", handlers.CreateAgent)

	body := CreateAgentRequest{
		Name:      "existing-agent",
		Namespace: "default",
		Spec:      corev1alpha1.AgentSpec{},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestHandlers_CreateAgent_NamespaceForbidden(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "allowed-ns"})

	app := fiber.New()
	app.Post("/agents", handlers.CreateAgent)

	body := CreateAgentRequest{
		Name:      "test-agent",
		Namespace: "other-ns",
		Spec:      corev1alpha1.AgentSpec{},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_CreateAgent_ContextTokenAuthorizationRejectsDisallowedAgentSpec(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeAgentsWrite,
		"tctx": map[string]any{
			"allowedProviders": []string{"openai"},
			"allowedModels":    []string{"openai/gpt-4o"},
			"allowedTools":     []string{"file_read"},
		},
	})

	bodyBytes, _ := json.Marshal(CreateAgentRequest{
		Name:      "test-agent",
		Namespace: "default",
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
			Tools: []corev1alpha1.ToolReference{{Name: "web_search"}},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// --- UpdateAgent tests ---

func TestHandlers_UpdateAgent_Success(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(agent)
	app.Put("/agents/:name", handlers.UpdateAgent)

	body := UpdateAgentRequest{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPut, "/agents/test-agent", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	spec := result["spec"].(map[string]any)
	model := spec["model"].(map[string]any)
	require.Equal(t, "gpt-4", model["name"])
}

func TestHandlers_UpdateAgent_ContextTokenAuthorizationRejectsDisallowedAgentSpec(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "default"},
		Spec:       corev1alpha1.AgentSpec{},
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, agent)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeAgentsWrite,
		"tctx": map[string]any{
			"allowedAgents":    []string{"test-agent"},
			"allowedProviders": []string{"openai"},
			"allowedModels":    []string{"openai/gpt-4o"},
		},
	})

	bodyBytes, _ := json.Marshal(UpdateAgentRequest{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
		},
	})
	req := httptest.NewRequest(http.MethodPut, "/agents/test-agent", bytes.NewReader(bodyBytes))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_UpdateAgent_PatchesSpecWithoutFullObjectUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Name: "gpt-4o-mini",
			},
		},
	}

	updateCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(agent).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updateCalled = true
				return fmt.Errorf("unexpected full object update")
			},
		}).
		Build()

	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss})
	app := fiber.New()
	app.Put("/agents/:name", handlers.UpdateAgent)

	body := UpdateAgentRequest{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Name: "gpt-4.1",
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPut, "/agents/test-agent", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, updateCalled)

	updated := &corev1alpha1.Agent{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-agent", Namespace: "default"}, updated)
	require.NoError(t, err)
	require.Equal(t, "gpt-4.1", updated.Spec.Model.Name)
}

func TestHandlers_UpdateAgent_RetriesConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Name: "gpt-4o-mini",
			},
		},
	}

	patchCalls := 0
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(agent).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				patchCalls++
				if patchCalls == 1 {
					return apierrors.NewConflict(
						schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "agents"},
						obj.GetName(),
						fmt.Errorf("changed"),
					)
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss})
	app := fiber.New()
	app.Put("/agents/:name", handlers.UpdateAgent)

	body := UpdateAgentRequest{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Name: "gpt-4.1",
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPut, "/agents/test-agent", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, patchCalls)

	updated := &corev1alpha1.Agent{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-agent", Namespace: "default"}, updated)
	require.NoError(t, err)
	require.Equal(t, "gpt-4.1", updated.Spec.Model.Name)
}

func TestHandlers_UpdateAgent_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Put("/agents/:name", handlers.UpdateAgent)

	body := UpdateAgentRequest{
		Spec: corev1alpha1.AgentSpec{},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPut, "/agents/nonexistent", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlers_UpdateAgent_InvalidBody(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{},
	}

	handlers, app := setupTestHandlersWithObjects(agent)
	app.Put("/agents/:name", handlers.UpdateAgent)

	req := httptest.NewRequest(http.MethodPut, "/agents/test-agent", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- DeleteAgent tests ---

func TestHandlers_DeleteAgent_Success(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-agent",
			Namespace: "default",
		},
		Spec: corev1alpha1.AgentSpec{},
	}

	handlers, app := setupTestHandlersWithObjects(agent)
	app.Delete("/agents/:name", handlers.DeleteAgent)

	req := httptest.NewRequest(http.MethodDelete, "/agents/test-agent", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestHandlers_DeleteAgent_NotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Delete("/agents/:name", handlers.DeleteAgent)

	req := httptest.NewRequest(http.MethodDelete, "/agents/nonexistent", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- ListSecretNames tests ---

func TestHandlers_ListSecretNames_Success(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
	}

	handlers, app := setupTestHandlersWithObjects(secret)
	app.Get("/secrets", handlers.ListSecretNames)

	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items := result["items"].([]any)
	require.Len(t, items, 1)
	item := items[0].(map[string]any)
	require.Equal(t, "my-secret", item["name"])
}

func TestHandlers_ListSecretNames_FiltersServiceAccountTokens(t *testing.T) {
	opaqueSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
	}
	saTokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sa-token",
			Namespace: "default",
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}

	handlers, app := setupTestHandlersWithObjects(opaqueSecret, saTokenSecret)
	app.Get("/secrets", handlers.ListSecretNames)

	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items := result["items"].([]any)
	require.Len(t, items, 1)
	item := items[0].(map[string]any)
	require.Equal(t, "my-secret", item["name"])
}

func TestHandlers_ListSecretNames_Empty(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/secrets", handlers.ListSecretNames)

	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items := result["items"].([]any)
	require.Len(t, items, 0)
}

func TestHandlers_ListSecretNames_ContextTokenAuthorizationEnforceAllowsReadScope(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "default",
		},
		Type: corev1.SecretTypeOpaque,
	}
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, secret)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecretsRead,
		"tctx": map[string]any{
			"namespace": "default",
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	items := result["items"].([]any)
	require.Len(t, items, 1)
}

func TestHandlers_ListSecretNames_ContextTokenAuthorizationEnforceRejectsMissingReadScope(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeProvidersUse,
	})
	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestHandlers_ListSecretNames_ContextTokenAuthorizationEnforceRejectsNamespaceMismatch(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupTestHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecretsRead,
		"tctx": map[string]any{
			"namespace": "restricted",
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/secrets?namespace=default", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// --- StreamPodLogs tests ---

func TestStreamPodLogs_WithFakeClientset(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	clientset := kubefake.NewSimpleClientset(pod) //nolint:staticcheck
	ctx := context.Background()

	// StreamPodLogs should return a stream (or error) — with fake clientset
	// it returns a stream even without real logs, but we verify the function is callable.
	stream, err := StreamPodLogs(ctx, clientset, "default", "test-pod", "worker")
	if err == nil && stream != nil {
		defer stream.Close() //nolint:errcheck
	}
	// The fake clientset may or may not error — we just verify the function doesn't panic
	// and correctly calls the K8s API.
	_ = err
}

func TestStreamPodLogs_CancelledContext(t *testing.T) {
	clientset := kubefake.NewSimpleClientset() //nolint:staticcheck
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := StreamPodLogs(ctx, clientset, "default", "test-pod", "worker")
	// With a cancelled context, the stream call may or may not error depending
	// on the fake implementation — we verify no panic.
	_ = err
}

// --- handleAuthValidate tests ---

func TestHandlers_HandleAuthValidate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	server := &Server{
		app:    fiber.New(),
		client: fakeClient,
	}
	server.app.Get("/auth/validate", server.handleAuthValidate)

	req := httptest.NewRequest(http.MethodGet, "/auth/validate", nil)
	resp, err := server.app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	require.Equal(t, true, result["authenticated"])
}

// --- GetTaskLogs additional branch tests ---

func TestHandlers_GetTaskLogs_ResultStoreAvailable(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "done-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	require.NoError(t, handlers.resultStore.SaveResult(context.Background(), "default", "done-task", []byte("log output here")))
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/done-task/logs", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	require.Equal(t, "log output here", result["logs"])
}

func TestHandlers_GetTaskLogs_ResultStoreNotFound(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "done-task-no-data",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{
				Available: true,
			},
		},
	}

	handlers, app := setupTestHandlersWithObjects(task)
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/done-task-no-data/logs", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlers_GetTaskLogs_NamespaceForbidden(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "allowed-ns"})

	app := fiber.New()
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/test/logs?namespace=other-ns", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// --- GetSession transcript generation test ---

func TestHandlers_GetSession_WithTranscript(t *testing.T) {
	handlers, app, ss := setupTestHandlersWithSessionManager()
	ctx := context.Background()

	require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
		Namespace:   "default",
		Name:        "transcript-session",
		SessionType: "chat",
	}))

	require.NoError(t, ss.AppendMessages(ctx, "default", "transcript-session", []store.SessionMessage{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "hi there"},
	}))

	app.Get("/sessions/:id", handlers.GetSession)

	req := httptest.NewRequest(http.MethodGet, "/sessions/transcript-session", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	transcript, ok := result["transcript"].(string)
	require.True(t, ok)
	require.NotEmpty(t, transcript)
	require.Contains(t, transcript, "hello")
	require.Contains(t, transcript, "hi there")
	// Transcript should be JSONL (newline-separated)
	lines := strings.Split(transcript, "\n")
	require.Len(t, lines, 2)
}

// --- DeleteSession error path tests ---

func TestHandlers_DeleteSession_NamespaceForbidden(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, WatchNamespace: "watched-ns", SessionStore: ss, ResultStore: ss})

	app := fiber.New()
	app.Delete("/sessions/:id", handlers.DeleteSession)

	req := httptest.NewRequest(http.MethodDelete, "/sessions/my-session?namespace=wrong-ns", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// --- GetTaskPlan additional tests ---

func TestHandlers_GetTaskPlan_TaskNotFound(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id/plan", handlers.GetTaskPlan)

	req := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent/plan", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlers_GetTaskPlan_NoPlanStore(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plan-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()
	// planStore is nil
	handlers := NewHandlers(HandlersConfig{Client: fakeClient})

	app := fiber.New()
	app.Get("/tasks/:id/plan", handlers.GetTaskPlan)

	req := httptest.NewRequest(http.MethodGet, "/tasks/plan-task/plan", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// --- GetTask plan enrichment test ---

func TestHandlers_GetTask_WithPlanEnrichment(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "enriched-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
		Status: corev1alpha1.TaskStatus{
			Iteration: 0,
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).WithStatusSubresource(task).Build()
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss, PlanStore: ss})

	require.NoError(t, ss.SavePlan(context.Background(), "default", "enriched-task", &store.PlanState{
		TaskName:     "enriched-task",
		Namespace:    "default",
		Summary:      "almost done",
		ProgressPct:  90,
		GoalComplete: false,
		PlanDocument: "# Plan\n- step 1 done",
		Iteration:    0,
	}))

	app := fiber.New()
	app.Get("/tasks/:id", handlers.GetTask)

	req := httptest.NewRequest(http.MethodGet, "/tasks/enriched-task", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	plan, ok := result["plan"].(map[string]any)
	require.True(t, ok, "response should contain plan field")
	require.Equal(t, "almost done", plan["summary"])
	require.Equal(t, float64(90), plan["progressPct"])
	require.Equal(t, false, plan["goalComplete"])
}

func TestHandlers_GetTask_NoPlanStoreNoEnrichment(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "no-plan-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
		Status: corev1alpha1.TaskStatus{
			Iteration: 2,
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).WithStatusSubresource(task).Build()
	// planStore is nil
	handlers := NewHandlers(HandlersConfig{Client: fakeClient})

	app := fiber.New()
	app.Get("/tasks/:id", handlers.GetTask)

	req := httptest.NewRequest(http.MethodGet, "/tasks/no-plan-task", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	_, hasPlan := result["plan"]
	require.False(t, hasPlan, "response should not contain plan field when planStore is nil")
}

// --- Tests: GetTaskChildren ---

func TestHandlers_GetTaskChildren_Success(t *testing.T) {
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "parent-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-task",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-task"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	handlers, app := setupTestHandlersWithObjects(parent, child)
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)

	req := httptest.NewRequest(http.MethodGet, "/tasks/parent-task/children", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result ListResponse
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
}

func TestHandlers_GetTaskChildren_NoChildren(t *testing.T) {
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lonely-task",
			Namespace: "default",
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	handlers, app := setupTestHandlersWithObjects(parent)
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)

	req := httptest.NewRequest(http.MethodGet, "/tasks/lonely-task/children", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHandlers_GetTaskChildren_WithNamespace(t *testing.T) {
	handlers, app := setupTestHandlers()
	handlers.watchNamespace = testWatchNamespace
	app.Get("/tasks/:id/children", handlers.GetTaskChildren)

	req := httptest.NewRequest(http.MethodGet, "/tasks/parent/children?namespace=prod", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- Tests: Readyz with health checker ---

type fakeHealthChecker struct {
	err error
}

func (f *fakeHealthChecker) HealthCheck(_ context.Context) error {
	return f.err
}

func TestHandlers_Readyz_HealthCheckFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, HealthChecker: &fakeHealthChecker{err: fmt.Errorf("db down")}})
	app := fiber.New()
	app.Get("/readyz", handlers.Readyz)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body) //nolint:errcheck
	require.Equal(t, "not ready", body["status"])
}

func TestHandlers_Readyz_HealthCheckSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	handlers := NewHandlers(HandlersConfig{Client: fakeClient, HealthChecker: &fakeHealthChecker{err: nil}})
	app := fiber.New()
	app.Get("/readyz", handlers.Readyz)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- Tests: GetTaskLogs with clientset ---

func TestHandlers_GetTaskLogs_TaskNotFoundReturns404(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Get("/tasks/:id/logs", handlers.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent/logs", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlers_GetTaskLogs_WithClientsetNoPods(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cs-task",
			Namespace: "default",
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{JobName: "test-job"},
	}

	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(task).Build()

	fakeCS := kubefake.NewSimpleClientset() //nolint:staticcheck // no pods
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	h := NewHandlers(HandlersConfig{Client: fakeClient, SessionStore: ss, ResultStore: ss, KubeClient: fakeCS})

	app := fiber.New()
	app.Get("/tasks/:id/logs", h.GetTaskLogs)

	req := httptest.NewRequest(http.MethodGet, "/tasks/cs-task/logs", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- Tests: DeleteTask with watch namespace ---

func TestHandlers_DeleteTask_WatchNamespaceScoped(t *testing.T) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ns-task",
			Namespace: testWatchNamespace,
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	handlers, app := setupTestHandlersWithObjects(task)
	handlers.watchNamespace = testWatchNamespace
	app.Delete("/tasks/:id", handlers.DeleteTask)

	req := httptest.NewRequest(http.MethodDelete, "/tasks/ns-task", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

type conflictApplyMemoryProposalStore struct{}

var _ store.MemoryProposalStore = conflictApplyMemoryProposalStore{}

func (conflictApplyMemoryProposalStore) CreateMemoryProposal(context.Context, *store.MemoryProposal) error {
	return nil
}

func (conflictApplyMemoryProposalStore) GetMemoryProposal(context.Context, string, string) (*store.MemoryProposal, error) {
	return nil, store.ErrNotFound
}

func (conflictApplyMemoryProposalStore) ListMemoryProposals(context.Context, store.MemoryProposalFilter) ([]store.MemoryProposal, error) {
	return nil, nil
}

func (conflictApplyMemoryProposalStore) ReviewMemoryProposal(context.Context, store.MemoryProposalReview) error {
	return nil
}

func (conflictApplyMemoryProposalStore) ApplyMemoryProposal(context.Context, store.MemoryProposalApply) (*store.Memory, error) {
	return nil, fmt.Errorf("%w: changed", store.ErrConflict)
}

func (conflictApplyMemoryProposalStore) ArchiveMemoryProposal(context.Context, string, string) error {
	return nil
}

func TestHandlers_ApplyMemoryProposal_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name  string
		scope string
		want  int
	}{
		{name: "allowed with memory write", scope: ContextTokenScopeMemoryWrite, want: http.StatusOK},
		{name: "denied with only memory read", scope: ContextTokenScopeMemoryRead, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, ss := setupTestHandlersWithAuthzStore(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)
			proposal := &store.MemoryProposal{
				Namespace: "default",
				Title:     "Remember project preference",
				Type:      "memory",
				Content:   "User prefers compact task summaries.",
			}
			require.NoError(t, ss.CreateMemoryProposal(context.Background(), proposal))
			require.NoError(t, ss.ReviewMemoryProposal(context.Background(), store.MemoryProposalReview{
				Namespace: "default",
				ID:        proposal.ID,
				Status:    "accepted",
				Reviewer:  "reviewer",
			}))

			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": tt.scope})
			body, _ := json.Marshal(map[string]any{"appliedBy": "api-user"})
			req := httptest.NewRequest(http.MethodPost, "/memory-proposals/"+proposal.ID+"/apply?namespace=default", bytes.NewReader(body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestHandlers_ApplyMemoryProposal(t *testing.T) {
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	ss := sqlite.NewStore(db, ":memory:")
	h := NewHandlers(HandlersConfig{MemoryStore: ss, MemoryProposalStore: ss})

	app := fiber.New()
	app.Post("/api/v1/memory-proposals/:id/apply", h.ApplyMemoryProposal)

	proposal := &store.MemoryProposal{
		Namespace:   "default",
		Title:       "Remember project preference",
		Type:        "memory",
		Description: "Tags: preference, summary",
		Content:     "User prefers compact task summaries.",
	}
	require.NoError(t, ss.CreateMemoryProposal(context.Background(), proposal))
	require.NoError(t, ss.ReviewMemoryProposal(context.Background(), store.MemoryProposalReview{
		Namespace: "default",
		ID:        proposal.ID,
		Status:    "accepted",
		Reviewer:  "reviewer",
	}))

	body, _ := json.Marshal(map[string]any{"appliedBy": "api-user"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/memory-proposals/"+proposal.ID+"/apply?namespace=default", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var memory store.Memory
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&memory))
	require.Equal(t, "default", memory.Namespace)
	require.Equal(t, proposal.ID, memory.SourceProposalID)
	require.Equal(t, "memory_proposal", memory.Source)
	require.Equal(t, proposal.Content, memory.Content)
	require.ElementsMatch(t, []string{"preference", "summary"}, memory.Tags)

	updated, err := ss.GetMemoryProposal(context.Background(), "default", proposal.ID)
	require.NoError(t, err)
	require.Equal(t, memory.ID, updated.AppliedMemoryID)
	require.Equal(t, "api-user", updated.AppliedBy)
	require.NotNil(t, updated.AppliedAt)
	require.False(t, updated.AppliedAt.IsZero())
}

func TestHandlers_ApplyMemoryProposalConflict(t *testing.T) {
	h := NewHandlers(HandlersConfig{MemoryProposalStore: conflictApplyMemoryProposalStore{}})

	app := fiber.New()
	app.Post("/api/v1/memory-proposals/:id/apply", h.ApplyMemoryProposal)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/memory-proposals/mprop-conflict/apply?namespace=default", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "changed")
}

func TestHandlers_ApplyMemoryProposalRejectedOrArchived(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(context.Context, *sqlite.Store, *store.MemoryProposal)
	}{
		{
			name: "rejected",
			prepare: func(ctx context.Context, ss *sqlite.Store, proposal *store.MemoryProposal) {
				require.NoError(t, ss.ReviewMemoryProposal(ctx, store.MemoryProposalReview{
					Namespace: proposal.Namespace,
					ID:        proposal.ID,
					Status:    "rejected",
					Reviewer:  "reviewer",
				}))
			},
		},
		{
			name: "archived",
			prepare: func(ctx context.Context, ss *sqlite.Store, proposal *store.MemoryProposal) {
				require.NoError(t, ss.ArchiveMemoryProposal(ctx, proposal.Namespace, proposal.ID))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sqlite.NewDB(":memory:")
			require.NoError(t, err)
			ss := sqlite.NewStore(db, ":memory:")
			h := NewHandlers(HandlersConfig{MemoryStore: ss, MemoryProposalStore: ss})

			app := fiber.New()
			app.Post("/api/v1/memory-proposals/:id/apply", h.ApplyMemoryProposal)

			ctx := context.Background()
			proposal := &store.MemoryProposal{
				Namespace:   "default",
				Title:       "Remember project preference",
				Type:        "memory",
				Description: "Reusable preference",
				Content:     "User prefers compact task summaries.",
			}
			require.NoError(t, ss.CreateMemoryProposal(ctx, proposal))
			tt.prepare(ctx, ss, proposal)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/memory-proposals/"+proposal.ID+"/apply?namespace=default", nil)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "cannot be applied")
		})
	}
}

// --- Tests: CreateAgent already exists (conflict) ---

func TestHandlers_CreateAgent_Conflict(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-agent",
			Namespace: "default",
		},
	}
	handlers, app := setupTestHandlersWithObjects(agent)
	app.Post("/agents", handlers.CreateAgent)

	body, _ := json.Marshal(map[string]any{
		"name": "existing-agent",
	})
	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

// --- Tests: ListTools & GetTool with CRDs ---

func TestHandlers_GetTool_CRD(t *testing.T) {
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-tool",
			Namespace: "default",
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "A custom tool",
		},
	}
	handlers, app := setupTestHandlersWithObjects(tool)
	app.Get("/tools/:name", handlers.GetTool)

	req := httptest.NewRequest(http.MethodGet, "/tools/custom-tool", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- Tests: UpdateAgent invalid body ---

func TestHandlers_UpdateAgent_WatchNamespace(t *testing.T) {
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "upd-agent",
			Namespace: testWatchNamespace,
		},
		Spec: corev1alpha1.AgentSpec{},
	}
	handlers, app := setupTestHandlersWithObjects(agent)
	handlers.watchNamespace = testWatchNamespace
	app.Put("/agents/:name", handlers.UpdateAgent)

	body, _ := json.Marshal(map[string]any{"spec": map[string]any{}})
	req := httptest.NewRequest(http.MethodPut, "/agents/upd-agent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- Tests: CreateAgent with metadata format ---

func TestHandlers_CreateAgent_MetadataFormat(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/agents", handlers.CreateAgent)

	body, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"name": "meta-agent",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/agents", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestHandlers_CreateTask_RejectsServerOwnedSpecCaseVariants(t *testing.T) {
	handlers, app := setupTestHandlers()
	app.Post("/tasks", handlers.CreateTask)
	body := map[string]any{
		"metadata": map[string]any{"name": "bad-task"},
		"Spec": map[string]any{
			"type":        "container",
			"RequestedBy": map[string]any{"username": "forged"},
		},
	}
	resp := testJSONRequest(t, app, http.MethodPost, "/tasks", body)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
