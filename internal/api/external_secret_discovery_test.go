package api

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestExternalSecretAutoDiscoveryUsesAuthorizedCandidates(t *testing.T) {
	const first, second, last = "git-credentials", "github-credentials", "git-token"
	for _, toolName := range []string{"create_agent_task", "create_pr_monitor"} {
		for _, test := range []struct {
			name                    string
			allowed                 []string
			wantSecret              string
			missingFirst, failFirst bool
			explicit, fail          bool
		}{
			{name: "first", allowed: []string{first}, wantSecret: first},
			{name: "later", allowed: []string{second}, wantSecret: second},
			{name: "last", allowed: []string{last}, wantSecret: last},
			{name: "missing-first", allowed: []string{first, second}, wantSecret: second, missingFirst: true},
			{name: "backend-error", allowed: []string{first, second}, failFirst: true, fail: true},
			{name: "none-visible"},
			{name: "explicit-denied", allowed: []string{second}, explicit: true, fail: true},
		} {
			if test.explicit && toolName != "create_pr_monitor" {
				continue
			}
			t.Run(toolName+"/"+test.name, func(t *testing.T) {
				clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					a := *spec.ResourceAttributes
					require.Equal(t, externalToolNamespace, a.Namespace)
					agentRead := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Group: "core.orka.ai", Resource: "agents", Verb: "get", Name: "agent"}
					taskCreate := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Group: "core.orka.ai", Resource: "tasks", Verb: "create"}
					secretRead := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Resource: "secrets", Verb: "get", Name: a.Name}
					return externalToolReview(a == agentRead || a == taskCreate || a == secretRead && slices.Contains(test.allowed, a.Name)), nil
				})
				agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: externalToolNamespace}}
				if toolName == "create_agent_task" {
					agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex}
				} else {
					agent.Spec.Coordination = &corev1alpha1.CoordinationConfig{Enabled: true}
				}
				objects := make([]client.Object, 1, 4)
				objects[0] = agent
				for _, name := range []string{first, second, last} {
					if name == first && test.missingFirst {
						continue
					}
					objects = append(objects, &corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: externalToolNamespace},
						Data:       map[string][]byte{"token": []byte("fixture-credential")},
					})
				}
				executor, backend, _, _ := newExternalToolExecutor(clientset, objects...)
				secretReads := map[string]int{}
				backend.Client = interceptor.NewClient(backend.Client.(client.WithWatch), interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*corev1.Secret); ok {
							require.Equal(t, externalToolNamespace, key.Namespace)
							require.Contains(t, test.allowed, key.Name, "denied Secret reached the backend")
							secretReads[key.Name]++
							if test.failFirst && key.Name == first {
								return errors.New("fixture Secret lookup failed")
							}
						}
						return c.Get(ctx, key, obj, opts...)
					},
				})
				args := map[string]any{"agentRef": "agent", "namespace": externalToolNamespace}
				if toolName == "create_agent_task" {
					args["prompt"] = "Inspect the repository"
					args["workspace"] = map[string]any{"gitRepo": "https://github.com/orka-agents/orka"}
				} else {
					args["name"], args["schedule"], args["repo_url"] = "credential-discovery", "0 * * * *", "https://github.com/orka-agents/orka"
					if test.explicit {
						args["readCredentialRef"] = first
					}
				}
				encoded, err := json.Marshal(args)
				require.NoError(t, err)
				result := executeExternalTool(t, executor, toolName, string(encoded))
				wantSuccess := !test.fail && (test.wantSecret != "" || toolName == "create_agent_task")
				require.Equal(t, wantSuccess, result.Success, result.Error)
				tasks := &corev1alpha1.TaskList{}
				require.NoError(t, backend.Client.List(t.Context(), tasks, client.InNamespace(externalToolNamespace)))
				if wantSuccess {
					require.Equal(t, 1, backend.writes)
					require.Len(t, tasks.Items, 1)
					workspace := tasks.Items[0].Spec.Workspace
					require.NotNil(t, workspace)
					if test.wantSecret == "" {
						require.Nil(t, workspace.ReadCredentialRef)
						require.Empty(t, secretReads)
					} else {
						require.NotNil(t, workspace.ReadCredentialRef)
						require.Equal(t, test.wantSecret, workspace.ReadCredentialRef.Name)
						require.Positive(t, secretReads[test.wantSecret])
					}
				} else {
					require.Zero(t, backend.writes)
					require.Empty(t, tasks.Items)
					if test.failFirst {
						require.Contains(t, result.Error, "fixture Secret lookup failed")
						require.Equal(t, map[string]int{first: 1}, secretReads)
					} else {
						require.Empty(t, secretReads)
					}
				}
				require.NotEmpty(t, *reviews)
			})
		}
	}
}
