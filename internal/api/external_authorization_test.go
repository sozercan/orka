package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

func TestExternalAPIReportedMutationsRequireAuthorization(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodDelete, "/api/v1/tasks/protected", ""},
		{http.MethodPost, "/api/v1/tools", `{"name":"new-tool","spec":{"type":"http","http":{"url":"https://example.com"}}}`},
		{http.MethodPost, "/api/v1/memories", `{"id":"new-memory","content":"protected content"}`},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, authenticationv1.AddToScheme(scheme))
			task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "protected", Namespace: "default"}}
			var tokenReviews, writes int
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if review, ok := obj.(*authenticationv1.TokenReview); ok {
						tokenReviews++
						review.Status = authenticationv1.TokenReviewStatus{Authenticated: true, User: authenticationv1.UserInfo{
							Username: "system:serviceaccount:default:viewer", UID: "viewer-uid",
							Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default"},
							Extra:  map[string]authenticationv1.ExtraValue{"example.com/claim": {"value"}},
						}}
						return nil
					}
					writes++
					return c.Create(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					writes++
					return c.Delete(ctx, obj, opts...)
				},
			}).Build()
			clientset := kubefake.NewClientset()
			var reviews []authorizationv1.SubjectAccessReviewSpec
			clientset.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
				reviews = append(reviews, review.Spec)
				review.Status.Allowed = false
				return true, review, nil
			})
			db, err := sqlite.NewDB(":memory:")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			ss := sqlite.NewStore(db, ":memory:")
			server := NewServer(kube, nil, ServerConfig{WatchNamespace: "default", EnforceNamespaceIsolation: true, Clientset: clientset, MemoryStore: ss})
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+fmt.Sprintf("fixture-%s", t.Name()))
			response, err := server.app.Test(request)
			require.NoError(t, err)
			defer response.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, 1, tokenReviews)
			assertDenied := response.StatusCode == http.StatusForbidden && writes == 0 && len(reviews) > 0
			require.True(t, assertDenied, "status=%d, writes=%d, reviews=%d, body=%s", response.StatusCode, writes, len(reviews), body)
			require.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(task), &corev1alpha1.Task{}))
			memories, err := ss.ListMemories(t.Context(), store.MemoryFilter{Namespace: "default"})
			require.NoError(t, err)
			require.Empty(t, memories)
		})
	}
}
