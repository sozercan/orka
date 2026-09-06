package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestExternalAPIAuthorizesResolvedNamespace(t *testing.T) {
	for _, tc := range []struct {
		name, route, path, body string
		status                  int
	}{
		{"flat wins over metadata and query", "POST /api/v1/tools", "/api/v1/tools?namespace=other", `{"name":"created","namespace":"default","metadata":{"namespace":"other"}}`, http.StatusCreated},
		{"metadata fallback ignores query", "POST /api/v1/tools", "/api/v1/tools?namespace=other", `{"name":"created","metadata":{"namespace":"default"}}`, http.StatusCreated},
		{"cross namespace flat wins", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","namespace":"other","metadata":{"namespace":"default"}}`, http.StatusForbidden},
		{"cross namespace metadata fallback", "POST /api/v1/tools", "/api/v1/tools?namespace=default", `{"name":"created","metadata":{"namespace":"other"}}`, http.StatusForbidden},
		{"case insensitive flat", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","NaMeSpAcE":"other"}`, http.StatusForbidden},
		{"case insensitive metadata", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","METADATA":{"NAMESPACE":"other"}}`, http.StatusForbidden},
		{"last duplicate key wins", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","namespace":"other","namespace":"default"}`, http.StatusCreated},
		{"last case folded key wins", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","namespace":"default","NAMESPACE":"other"}`, http.StatusForbidden},
		{"null string retains earlier value", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","namespace":"other","namespace":null}`, http.StatusForbidden},
		{"null metadata retains earlier value", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","metadata":{"namespace":"other"},"metadata":null}`, http.StatusForbidden},
		{"duplicate metadata merges fields", "POST /api/v1/tools", "/api/v1/tools", `{"name":"created","metadata":{"namespace":"other"},"metadata":{"name":"created"}}`, http.StatusForbidden},
		{"runtime ignores flat namespace", "POST /api/v1/agent-runtimes", "/api/v1/agent-runtimes?namespace=other", `{"namespace":"other","metadata":{"name":"created","namespace":"default"}}`, http.StatusCreated},
		{"runtime ignores wrong typed flat namespace", "POST /api/v1/agent-runtimes", "/api/v1/agent-runtimes", `{"namespace":42,"metadata":{"name":"created"}}`, http.StatusCreated},
		{"runtime uses cross namespace metadata", "POST /api/v1/agent-runtimes", "/api/v1/agent-runtimes", `{"namespace":"default","metadata":{"name":"created","namespace":"other"}}`, http.StatusForbidden},
		{"memory ignores metadata and query", "POST /api/v1/memories", "/api/v1/memories?namespace=other", `{"id":"created","content":"allowed","metadata":"ignored"}`, http.StatusCreated},
		{"memory uses body namespace", "POST /api/v1/memories", "/api/v1/memories?namespace=default", `{"id":"created","content":"denied","namespace":"other"}`, http.StatusForbidden},
		{"memory update query wins", "PUT /api/v1/memories/:id", "/api/v1/memories/protected?namespace=default", `{"namespace":"other","content":"updated"}`, http.StatusOK},
		{"memory update query denies", "PUT /api/v1/memories/:id", "/api/v1/memories/protected?namespace=other", `{"namespace":"default","content":"denied"}`, http.StatusForbidden},
		{"proposal review body wins", "POST /api/v1/memory-proposals/:id/review", "/api/v1/memory-proposals/protected/review?namespace=other", `{"namespace":"default","status":"accepted"}`, http.StatusNoContent},
		{"proposal review body denies", "POST /api/v1/memory-proposals/:id/review", "/api/v1/memory-proposals/protected/review?namespace=default", `{"namespace":"other","status":"accepted"}`, http.StatusForbidden},
		{"proposal rejects namespace trimming", "POST /api/v1/memory-proposals/:id/review", "/api/v1/memory-proposals/protected/review", `{"namespace":" default ","status":"accepted"}`, http.StatusForbidden},
		{"named update ignores body identity", "PUT /api/v1/tools/:name", "/api/v1/tools/protected?namespace=default", `{"namespace":"other","metadata":{"namespace":"other","name":"other"},"spec":{"description":"updated"}}`, http.StatusOK},
		{"named deletion uses query", "DELETE /api/v1/tasks/:id", "/api/v1/tasks/protected?namespace=other", `{}`, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			f.allowRoute(t, tc.route)
			before := f.changes(t)
			f.kubeCalls = 0
			method, _, _ := strings.Cut(tc.route, " ")
			status, body := f.request(t, method, tc.path, tc.body)
			require.Equal(t, tc.status, status, body)
			if tc.status == http.StatusForbidden {
				require.Empty(t, f.reviews)
				require.Zero(t, f.kubeCalls)
				require.Equal(t, before, f.changes(t))
				return
			}
			require.NotEmpty(t, f.reviews)
			for _, review := range f.reviews {
				require.Equal(t, "default", review.ResourceAttributes.Namespace)
			}
			switch method + " " + tc.path {
			case "PUT /api/v1/tools/protected?namespace=default":
				tool := &corev1alpha1.Tool{}
				require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, tool))
				require.Equal(t, "updated", tool.Spec.Description)
			case "PUT /api/v1/memories/protected?namespace=default":
				memory, err := f.store.GetMemory(t.Context(), "default", "protected")
				require.NoError(t, err)
				require.Equal(t, "updated", memory.Content)
			}
		})
	}
}
