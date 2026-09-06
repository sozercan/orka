package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

// Run against a disposable cluster with the Task, Tool and Skill CRDs installed.
// The explicit kubeconfig is required so normal unit tests never use a current
// Kubernetes context. All test records live in a namespace removed on cleanup.
func TestExternalAPILiveTokenReviewAuthorization(t *testing.T) {
	kubeconfig := os.Getenv("ORKA_AUTHORIZATION_E2E_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("set ORKA_AUTHORIZATION_E2E_KUBECONFIG to a disposable cluster's scoped kubeconfig")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(t, err)
	cfg.Timeout = 15 * time.Second
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1alpha1.AddToScheme, corev1.AddToScheme, authenticationv1.AddToScheme} {
		require.NoError(t, add(scheme))
	}
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	namespace := fmt.Sprintf("orka-authz-%x", time.Now().UnixNano())
	_, err = clientset.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, clientset.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{}))
	})

	credentials := map[string]string{}
	callerClients := map[string]client.Client{}
	for _, caller := range []string{"viewer", "editor"} {
		_, err = clientset.CoreV1().ServiceAccounts(namespace).Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: caller}}, metav1.CreateOptions{})
		require.NoError(t, err)
		rules := []rbacv1.PolicyRule{{APIGroups: []string{"core.orka.ai"}, Resources: []string{"tasks", "agents", "repositoryscans"}, Verbs: []string{"get", "list", "watch"}}}
		if caller == "editor" {
			rules = []rbacv1.PolicyRule{
				{APIGroups: []string{"core.orka.ai"}, Resources: []string{"tasks", "tools", "skills"}, Verbs: []string{"create", "delete"}},
				{APIGroups: []string{"core.orka.ai"}, Resources: []string{"memories"}, Verbs: []string{"create"}},
			}
		}
		_, err = clientset.RbacV1().Roles(namespace).Create(t.Context(), &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: caller}, Rules: rules}, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = clientset.RbacV1().RoleBindings(namespace).Create(t.Context(), &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: caller},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: caller},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: caller, Namespace: namespace}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		issued, err := clientset.CoreV1().ServiceAccounts(namespace).CreateToken(t.Context(), caller, &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
		require.NoError(t, err)
		// Tokens remain in memory. Never include requests or credential values
		// in assertion messages, fixture files or logs.
		credentials[caller] = issued.Status.Token
		callerConfig := rest.AnonymousClientConfig(cfg)
		callerConfig.BearerToken = issued.Status.Token
		callerClients[caller], err = client.New(callerConfig, client.Options{Scheme: scheme})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		review, err := clientset.AuthorizationV1().SubjectAccessReviews().Create(t.Context(), &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               "system:serviceaccount:" + namespace + ":editor",
			ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: namespace, Verb: "delete", Group: "core.orka.ai", Resource: "tasks", Name: "protected"},
		}}, metav1.CreateOptions{})
		return err == nil && review.Status.Allowed
	}, 10*time.Second, 100*time.Millisecond)

	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "protected", Namespace: namespace}, Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer, Image: "example.invalid/authz:fixture"}}
	require.NoError(t, admin.Create(t.Context(), task))
	require.True(t, apierrors.IsForbidden(callerClients["viewer"].Delete(t.Context(), task.DeepCopy())), "direct Kubernetes Task deletion must be denied")
	require.True(t, apierrors.IsForbidden(callerClients["viewer"].Create(t.Context(), &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "direct-denied", Namespace: namespace},
		Spec:       corev1alpha1.ToolSpec{Description: "fixture", HTTP: &corev1alpha1.HTTPExecution{URL: "https://example.com"}},
	})), "direct Kubernetes Tool creation must be denied")
	require.True(t, apierrors.IsForbidden(callerClients["viewer"].List(t.Context(), &corev1alpha1.ToolList{}, client.InNamespace(namespace))), "direct Kubernetes Tool listing must be denied")

	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ss := sqlite.NewStore(db, ":memory:")
	server := NewServer(admin, nil, ServerConfig{WatchNamespace: namespace, EnforceNamespaceIsolation: true, Clientset: clientset, MemoryStore: ss})
	request := func(caller, method, path, body string) int {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+credentials[caller])
		resp, err := server.app.Test(req, fiber.TestConfig{Timeout: 20 * time.Second})
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		return resp.StatusCode
	}
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodDelete, "/api/v1/tasks/protected", ""},
		{http.MethodPost, "/api/v1/tools", `{"name":"created","spec":{"type":"http","description":"fixture","http":{"url":"https://example.com"}}}`},
		{http.MethodPost, "/api/v1/skills", `{"name":"created","spec":{"description":"fixture","content":{"inline":"fixture skill"}}}`},
		{http.MethodPost, "/api/v1/memories", `{"id":"created","content":"fixture memory"}`},
		{http.MethodGet, "/api/v1/tools", ""},
	} {
		require.Equal(t, http.StatusForbidden, request("viewer", tc.method, tc.path, tc.body), "%s %s", tc.method, tc.path)
	}
	require.NoError(t, admin.Get(t.Context(), client.ObjectKeyFromObject(task), &corev1alpha1.Task{}))
	for _, object := range []client.Object{&corev1alpha1.Tool{}, &corev1alpha1.Skill{}} {
		require.True(t, apierrors.IsNotFound(admin.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: "created"}, object)))
	}
	memories, err := ss.ListMemories(t.Context(), store.MemoryFilter{Namespace: namespace})
	require.NoError(t, err)
	require.Empty(t, memories)

	require.Equal(t, http.StatusNoContent, request("editor", http.MethodDelete, "/api/v1/tasks/protected", ""))
	require.True(t, apierrors.IsNotFound(admin.Get(t.Context(), client.ObjectKeyFromObject(task), &corev1alpha1.Task{})))
	require.Equal(t, http.StatusCreated, request("editor", http.MethodPost, "/api/v1/tools", `{"name":"created","spec":{"type":"http","description":"fixture","http":{"url":"https://example.com"}}}`))
	require.NoError(t, admin.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: "created"}, &corev1alpha1.Tool{}))
	require.Equal(t, http.StatusCreated, request("editor", http.MethodPost, "/api/v1/skills", `{"name":"created","spec":{"description":"fixture","content":{"inline":"fixture skill"}}}`))
	require.NoError(t, admin.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: "created"}, &corev1alpha1.Skill{}))
	require.Equal(t, http.StatusCreated, request("editor", http.MethodPost, "/api/v1/memories", `{"id":"created","content":"fixture memory"}`))
	memory, err := ss.GetMemory(t.Context(), namespace, "created")
	require.NoError(t, err)
	require.Equal(t, "fixture memory", memory.Content)
	t.Log("restricted ServiceAccount: direct Kubernetes and Orka denials preserved state; exact editor grants allowed persisted changes")
}
