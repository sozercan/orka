package api

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestExternalAPIRepositoryMonitorReferencedReads(t *testing.T) {
	const monitorSpec = `{
		"repoURL":"https://github.com/orka-agents/orka",
		"branch":"authorized-reference",
		"targets":{"pullRequests":{"enabled":true},"issues":{"enabled":true}},
		"agents":{
			"reviewer":{"name":"reviewer","namespace":" default "},
			"triager":{"name":"triager"},
			"researcher":{"name":"researcher"},
			"planner":{"name":"planner"},
			"implementer":{"name":"implementer"},
			"repairer":{"name":"repairer"}
		},
		"repair":{"enabled":true},
		"readCredentialRef":{"name":" read-credential "},
		"publicationReadCredentialRef":{"name":"publication-read-credential"},
		"publicationCredentialRef":{"name":"publication-credential"},
		"forgeCredentialRef":{"name":"forge-credential"}
	}`
	agentNames := []string{"reviewer", "triager", "researcher", "planner", "implementer", "repairer"}
	secretNames := []string{"read-credential", "publication-read-credential", "publication-credential", "forge-credential"}
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		for _, reference := range []struct {
			field, resource, name string
			legacyRead            bool
		}{
			{field: "reviewer", resource: "agents", name: "reviewer"},
			{field: "triager", resource: "agents", name: "triager"},
			{field: "researcher", resource: "agents", name: "researcher"},
			{field: "planner", resource: "agents", name: "planner"},
			{field: "implementer", resource: "agents", name: "implementer"},
			{field: "repairer", resource: "agents", name: "repairer"},
			{field: "readCredentialRef", resource: "secrets", name: "read-credential"},
			{field: "publicationReadCredentialRef", resource: "secrets", name: "publication-read-credential"},
			{field: "publicationCredentialRef", resource: "secrets", name: "publication-credential"},
			{field: "forgeCredentialRef", resource: "secrets", name: "forge-credential"},
			{field: "gitSecretRef", resource: "secrets", name: "read-credential", legacyRead: true},
		} {
			t.Run(method+"/"+reference.field, func(t *testing.T) {
				f := newExternalAuthorizationFixture(t)
				path, targetName, verb, permissionName := "/api/v1/monitors/repositories", "created", "create", ""
				wantStatus := http.StatusCreated
				if method == http.MethodPut {
					path += "/protected"
					targetName, verb, permissionName, wantStatus = "protected", "update", "protected", http.StatusOK
				}
				permissions := []authorizationv1.ResourceAttributes{{
					Namespace: "default", Group: corev1alpha1.GroupVersion.Group,
					Resource: "repositorymonitors", Verb: verb, Name: permissionName,
				}}
				for _, name := range agentNames {
					agent := repositoryMonitorHandlerTestAgent(name, corev1alpha1.AgentRuntimeClaude)
					agent.Namespace = "default"
					require.NoError(t, f.kube.Create(t.Context(), agent))
					permissions = append(permissions, authorizationv1.ResourceAttributes{
						Namespace: "default", Group: corev1alpha1.GroupVersion.Group, Resource: "agents", Verb: "get", Name: name,
					})
				}
				for _, name := range secretNames {
					require.NoError(t, f.kube.Create(t.Context(), &corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
						Data:       map[string][]byte{"token": []byte("fixture-only")},
					}))
					permissions = append(permissions, authorizationv1.ResourceAttributes{
						Namespace: "default", Resource: "secrets", Verb: "get", Name: name,
					})
				}
				denied := authorizationv1.ResourceAttributes{Namespace: "default", Resource: reference.resource, Verb: "get", Name: reference.name}
				if reference.resource == "agents" {
					denied.Group = corev1alpha1.GroupVersion.Group
				}
				referencedReads := 0
				f.server.handlers.client = interceptor.NewClient(f.kube.(client.WithWatch), interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						resource := ""
						switch obj.(type) {
						case *corev1alpha1.Agent:
							resource = "agents"
						case *corev1.Secret:
							resource = "secrets"
						}
						if resource == denied.Resource && key.Namespace == denied.Namespace && key.Name == denied.Name {
							referencedReads++
						}
						return c.Get(ctx, key, obj, opts...)
					},
				})
				spec := monitorSpec
				if reference.legacyRead {
					// Legacy credentials are effective only when readCredentialRef is absent.
					spec = strings.ReplaceAll(spec, "\"readCredentialRef\"", "\"gitSecretRef\"")
					// Write workflows require the explicit read credential field.
					spec = strings.ReplaceAll(spec, "\"issues\":{\"enabled\":true}", "\"issues\":{\"enabled\":false}")
					spec = strings.ReplaceAll(spec, "\"repair\":{\"enabled\":true}", "\"repair\":{\"enabled\":false}")
					permissions = slices.DeleteFunc(permissions, func(p authorizationv1.ResourceAttributes) bool {
						return p.Resource == "agents" && p.Name != "reviewer"
					})
				}
				requestBody := `{"name":"created","spec":` + spec + `}`
				before := &corev1alpha1.RepositoryMonitorList{}
				require.NoError(t, f.kube.List(t.Context(), before, client.InNamespace("default")))
				storeChanges := f.changes(t)
				f.allowOnly(t, slices.DeleteFunc(slices.Clone(permissions), func(p authorizationv1.ResourceAttributes) bool { return p == denied })...)
				status, body := f.request(t, method, path, requestBody)
				require.Equal(t, http.StatusForbidden, status, body)
				require.Zero(t, referencedReads, "denied reference reached the Kubernetes client")
				require.NotEmpty(t, f.reviews)
				require.Equal(t, denied, *f.reviews[len(f.reviews)-1].ResourceAttributes)
				after := &corev1alpha1.RepositoryMonitorList{}
				require.NoError(t, f.kube.List(t.Context(), after, client.InNamespace("default")))
				require.Equal(t, before.Items, after.Items)
				require.Equal(t, storeChanges, f.changes(t))
				require.Zero(t, f.externalCalls.Load())

				f.allowOnly(t, permissions...)
				status, body = f.request(t, method, path, requestBody)
				require.Equal(t, wantStatus, status, body)
				require.Equal(t, 1, referencedReads)
				persisted := &corev1alpha1.RepositoryMonitor{}
				require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: targetName}, persisted))
				require.Equal(t, "authorized-reference", persisted.Spec.Branch)
				var response corev1alpha1.RepositoryMonitor
				require.NoError(t, json.Unmarshal([]byte(body), &response))
				require.Equal(t, persisted.Spec, response.Spec)
			})
		}
	}
}
