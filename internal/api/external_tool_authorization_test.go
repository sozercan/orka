/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	kubernetestesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

const externalToolNamespace = "tool-target"

func TestExternalToolCodeExecPreflightsLifecyclePermissions(t *testing.T) {
	type testCase struct {
		name           string
		deniedVerb     string
		deniedResource string
		networkPolicy  bool
		rejectResource string
		wantCreates    int
	}
	tests := make([]testCase, 0, 12)
	tests = append(tests,
		testCase{name: "completed", networkPolicy: true, wantCreates: 4},
		testCase{name: "partial-setup", networkPolicy: true, rejectResource: "serviceaccounts", wantCreates: 2},
		testCase{name: "job-rejected", networkPolicy: true, rejectResource: "jobs", wantCreates: 4},
		testCase{name: "network-policy-disabled", deniedVerb: "delete", deniedResource: "networkpolicies", wantCreates: 3},
	)
	for _, resource := range []string{"secrets", "serviceaccounts", "networkpolicies", "jobs"} {
		for _, verb := range []string{"create", "delete"} {
			tests = append(tests, testCase{name: "denied-" + verb + "-" + resource, deniedVerb: verb, deniedResource: resource, networkPolicy: true})
		}
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(workerenv.CodeExecKubernetesNetworkPolicy, boolString(test.networkPolicy))
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				a := spec.ResourceAttributes
				require.Equal(t, externalToolNamespace, a.Namespace)
				return externalToolReview(a.Verb != test.deniedVerb || a.Resource != test.deniedResource), nil
			})
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, networkingv1.AddToScheme} {
				require.NoError(t, add(scheme))
			}
			creates := 0
			backend := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if creates == 0 {
						permissions := []authorizationv1.ResourceAttributes{
							{Resource: "secrets"}, {Resource: "serviceaccounts"}, {Group: "batch", Resource: "jobs"},
							{Group: "networking.k8s.io", Resource: "networkpolicies"},
						}
						if !test.networkPolicy {
							permissions = permissions[:3]
						}
						for _, permission := range permissions {
							permission.Namespace = externalToolNamespace
							for _, verb := range []string{"create", "delete"} {
								permission.Verb = verb
								permission.Name = ""
								if verb == "delete" {
									permission.Name = obj.GetName()
								}
								require.True(t, slices.ContainsFunc(*reviews, func(spec authorizationv1.SubjectAccessReviewSpec) bool {
									return reflect.DeepEqual(*spec.ResourceAttributes, permission)
								}), "missing %s %s preflight before the first create", verb, permission.Resource)
							}
						}
					}
					creates++
					if test.rejectResource == "serviceaccounts" {
						if _, ok := obj.(*corev1.ServiceAccount); ok {
							return errors.New("fixture admission rejected")
						}
					}
					if test.rejectResource == "jobs" {
						if _, ok := obj.(*batchv1.Job); ok {
							return errors.New("fixture admission rejected")
						}
					}
					return c.Create(ctx, obj, opts...)
				},
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					err := c.Get(ctx, key, obj, opts...)
					if job, ok := obj.(*batchv1.Job); ok && err == nil {
						job.Status.Succeeded = 1
					}
					return err
				},
			}).Build()
			tc := &tools.ToolContext{Client: backend, KubeClient: clientset, Namespace: externalToolNamespace}
			authorizeExternalToolContext(tc, externalToolUser(), nil)
			executor := &tools.KubernetesJobCodeExecutor{}
			result := executor.Execute(tools.WithToolContext(t.Context(), tc), tools.CodeExecutionRequest{
				Language: "python", Code: "pass", Timeout: time.Second,
			})
			for _, list := range []client.ObjectList{&corev1.SecretList{}, &corev1.ServiceAccountList{}, &networkingv1.NetworkPolicyList{}, &batchv1.JobList{}} {
				require.NoError(t, backend.List(t.Context(), list, client.InNamespace(externalToolNamespace)))
				items, err := meta.ExtractList(list)
				require.NoError(t, err)
				require.Zero(t, len(items), "temporary %T resources survived", list)
			}
			require.Equal(t, test.wantCreates, creates)
			switch {
			case test.wantCreates == 0:
				require.Contains(t, result.Error, "not authorized")
				require.True(t, slices.ContainsFunc(*reviews, func(spec authorizationv1.SubjectAccessReviewSpec) bool {
					return spec.ResourceAttributes.Resource == test.deniedResource && spec.ResourceAttributes.Verb == test.deniedVerb
				}))
			case test.rejectResource != "":
				require.Contains(t, result.Error, "fixture admission rejected")
			default:
				require.Empty(t, result.Error)
				require.Zero(t, result.ExitCode)
			}
		})
	}
}

func TestExternalToolExecutorEditorAgentUpdateRole(t *testing.T) {
	data, err := os.ReadFile("../../config/rbac/api_editor_role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatal(err)
	}
	clientset, _ := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
		a := spec.ResourceAttributes
		allowed := a.Namespace == externalToolNamespace && a.Subresource == "" && slices.ContainsFunc(role.Rules, func(rule rbacv1.PolicyRule) bool {
			return slices.Contains(rule.APIGroups, a.Group) && slices.Contains(rule.Resources, a.Resource) &&
				slices.Contains(rule.Verbs, a.Verb) && (len(rule.ResourceNames) == 0 || slices.Contains(rule.ResourceNames, a.Name))
		})
		return externalToolReview(allowed), nil
	})
	executor, backend, _, _ := newExternalToolExecutor(clientset,
		&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: externalToolNamespace}},
	)
	result := executeExternalTool(t, executor, "update_agent", `{"name":"agent","namespace":"tool-target","systemPrompt":"updated"}`)
	if !result.Success || backend.writes != 1 {
		t.Fatalf("API editor cannot update an Agent through chat: success=%v writes=%d error=%s", result.Success, backend.writes, result.Error)
	}
}

func TestExternalToolExecutorResourcePermissions(t *testing.T) {
	tests := []struct {
		tool     string
		args     string
		verb     string
		resource string
		name     string
		mutation bool
	}{
		{"check_task_progress", `{"name":"task","namespace":"tool-target"}`, "get", "tasks", "task", false},
		{"wait_for_task", `{"name":"task","namespace":"tool-target"}`, "get", "tasks", "task", false},
		{"fetch_task_output", `{"name":"task","namespace":"tool-target"}`, "get", "tasks", "task", false},
		{"list_tasks", `{"namespace":"tool-target"}`, "list", "tasks", "", false},
		{"list_agents", `{"namespace":"tool-target"}`, "list", "agents", "", false},
		{"list_tools", `{"namespace":"tool-target"}`, "list", "tools", "", false},
		{"create_ai_task", `{"name":"new-task","prompt":"do work","namespace":"tool-target"}`, "create", "tasks", "", true},
		{"cancel_task", `{"name":"task","namespace":"tool-target"}`, "delete", "tasks", "task", true},
		{"create_agent", `{"name":"new-agent","namespace":"tool-target"}`, "create", "agents", "", true},
		{"update_agent", `{"name":"agent","namespace":"tool-target","systemPrompt":"updated"}`, "update", "agents", "agent", true},
		{"delete_agent", `{"name":"agent","namespace":"tool-target"}`, "delete", "agents", "agent", true},
		{"create_tool", `{"name":"new-tool","namespace":"tool-target","description":"test","url":"https://example.test/tool"}`, "create", "tools", "", true},
		{"delete_tool", `{"name":"tool","namespace":"tool-target"}`, "delete", "tools", "tool", true},
		{"delete_session", `{"sessionId":"session","namespace":"tool-target"}`, "delete", "sessions", "session", true},
	}
	for _, test := range tests {
		for _, allowed := range []bool{false, true} {
			t.Run(test.tool+"/allowed="+boolString(allowed), func(t *testing.T) {
				clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					a := spec.ResourceAttributes
					isTarget := a.Verb == test.verb && a.Resource == test.resource
					prerequisite := test.mutation && a.Verb == "get" && a.Resource == test.resource
					return externalToolReview((isTarget && allowed) || prerequisite), nil
				})
				executor, backend, results, sessions := newExternalToolExecutor(clientset,
					&corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: externalToolNamespace, UID: "task-uid"}, Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded, ResultRef: &corev1alpha1.ResultReference{Available: true}}},
					&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: externalToolNamespace}},
					&corev1alpha1.Tool{ObjectMeta: metav1.ObjectMeta{Name: "tool", Namespace: externalToolNamespace}},
				)
				result := executeExternalTool(t, executor, test.tool, test.args)
				if result.Success != allowed {
					t.Fatalf("success = %v, allowed = %v, error = %s", result.Success, allowed, result.Error)
				}
				want := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Verb: test.verb, Group: corev1alpha1.GroupVersion.Group, Resource: test.resource, Name: test.name}
				found := false
				for _, review := range *reviews {
					if reflect.DeepEqual(*review.ResourceAttributes, want) {
						found = true
					}
					if review.ResourceAttributes.Namespace != externalToolNamespace {
						t.Fatalf("authorized namespace = %q, want final tool namespace", review.ResourceAttributes.Namespace)
					}
				}
				if !found {
					t.Fatalf("missing exact authorization for %#v", want)
				}
				if !allowed {
					if backend.writes != 0 || results.reads != 0 || len(sessions.deleted) != 0 {
						t.Fatalf("denied call changed state or read output: writes=%d outputReads=%d sessions=%d", backend.writes, results.reads, len(sessions.deleted))
					}
					if !test.mutation && backend.reads != 0 {
						t.Fatalf("denied read reached controller client %d times", backend.reads)
					}
				} else if test.mutation && backend.writes+len(sessions.deleted) == 0 {
					t.Fatal("authorized mutation did not reach storage")
				}
			})
		}
	}
}

func TestExternalToolExecutorCreateAgentPreflightsInitialTask(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(boolString(allowed), func(t *testing.T) {
			clientset, _ := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				a := spec.ResourceAttributes
				return externalToolReview(a.Verb == "create" && (a.Resource == "agents" || allowed && a.Resource == "tasks")), nil
			})
			executor, backend, _, _ := newExternalToolExecutor(clientset)
			result := executeExternalTool(t, executor, "create_agent", `{"name":"combined","namespace":"tool-target","initialPrompt":"run this"}`)
			if result.Success != allowed {
				t.Fatalf("success = %v, allowed = %v, error = %s", result.Success, allowed, result.Error)
			}
			wantWrites := 0
			if allowed {
				wantWrites = 2
			}
			if backend.writes != wantWrites {
				t.Fatalf("writes = %d, want %d", backend.writes, wantWrites)
			}
		})
	}
}

func TestExternalToolExecutorAuthorizationFailsClosed(t *testing.T) {
	for _, failure := range []string{"missing-client", "typed-nil-client", "denied", "nil-review", "review-error", "explicit-denied", "evaluation-error"} {
		t.Run(failure, func(t *testing.T) {
			var clientset kubernetes.Interface
			if failure == "typed-nil-client" {
				clientset = (*kubernetesfake.Clientset)(nil)
			} else if failure != "missing-client" {
				clientset, _ = externalToolReviews(t, func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					switch failure {
					case "nil-review":
						return nil, nil
					case "review-error":
						return nil, errors.New("authorization service unavailable")
					case "explicit-denied":
						return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true, Denied: true}}, nil
					case "evaluation-error":
						return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true, EvaluationError: "failed evaluation"}}, nil
					default:
						return externalToolReview(false), nil
					}
				})
			}
			executor, backend, _, _ := newExternalToolExecutor(clientset)
			result := executeExternalTool(t, executor, "create_tool", `{"name":"blocked","description":"test","url":"https://example.test/tool"}`)
			if result.Success || backend.writes != 0 || result.ErrorType != "permission_denied" {
				t.Fatalf("authorization failure escaped: success=%v writes=%d errorType=%s", result.Success, backend.writes, result.ErrorType)
			}
		})
	}
}

func TestExternalToolExecutorNamespaceLimits(t *testing.T) {
	for _, mode := range []string{"watch", "isolation", "blocked"} {
		for _, tool := range []string{"check_task_progress", "list_tasks", "update_agent", "delete_session"} {
			t.Run(mode+"/"+tool, func(t *testing.T) {
				clientset, reviews := externalToolReviews(t, func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					return externalToolReview(true), nil
				})
				executor, backend, _, sessions := newExternalToolExecutor(clientset)
				namespace := "outside"
				switch mode {
				case "watch":
					executor.watchNamespace = executor.namespace
				case "isolation":
					executor.enforceNamespaceIsolation = true
				case "blocked":
					namespace = "kube-system"
				}
				args, err := json.Marshal(map[string]string{"namespace": namespace, "name": "task", "sessionId": "session"})
				if err != nil {
					t.Fatal(err)
				}
				result := executeExternalTool(t, executor, tool, string(args))
				if result.Success || backend.reads != 0 || backend.writes != 0 || len(sessions.deleted) != 0 || len(*reviews) != 0 {
					t.Fatalf("namespace restriction escaped: success=%v reads=%d writes=%d sessions=%d reviews=%d", result.Success, backend.reads, backend.writes, len(sessions.deleted), len(*reviews))
				}
			})
		}
	}
}

func TestExternalToolExecutorAuthorizesStringifiedTarget(t *testing.T) {
	clientset, reviews := externalToolReviews(t, func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
		return externalToolReview(false), nil
	})
	executor, backend, _, _ := newExternalToolExecutor(clientset)
	result := executeExternalTool(t, executor, "check_task_progress", `{"name":464,"namespace":123}`)
	if result.Success || backend.reads != 0 || len(*reviews) != 1 {
		t.Fatalf("unexpected result: success=%v reads=%d reviews=%d", result.Success, backend.reads, len(*reviews))
	}
	if a := (*reviews)[0].ResourceAttributes; a.Namespace != "123" || a.Name != "464" {
		t.Fatalf("authorization target = %s/%s, want actual stringified tool target 123/464", a.Namespace, a.Name)
	}
}

func TestExternalToolExecutorProtectsGatewayTasks(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run("durable="+boolString(durable), func(t *testing.T) {
			clientset, _ := externalToolReviews(t, func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				return externalToolReview(true), nil
			})
			gatewayTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "gateway-task", Namespace: externalToolNamespace, UID: "gateway-task-uid"}, Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded, ResultRef: &corev1alpha1.ResultReference{Available: true}}}
			if !durable {
				gatewayTask.Spec.RequestedBy = &corev1alpha1.RequestedBy{Issuer: "gateway.orka.ai/tool-target/ns-uid/gateway/gateway-uid"}
			}
			executor, backend, results, _ := newExternalToolExecutor(clientset, gatewayTask,
				&corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "visible-task", Namespace: externalToolNamespace, UID: "visible-task-uid"}},
			)
			if durable {
				executor.gatewayEventStore = &externalToolGatewayStore{event: &store.GatewayEvent{Namespace: externalToolNamespace, NamespaceUID: "ns-uid", GatewayName: "gateway", GatewayUID: "gateway-uid", TaskName: gatewayTask.Name, TaskUID: string(gatewayTask.UID)}}
			}
			for _, tool := range []string{"check_task_progress", "fetch_task_output", "cancel_task"} {
				result := executeExternalTool(t, executor, tool, `{"name":"gateway-task","namespace":"tool-target"}`)
				if result.Success {
					t.Fatalf("%s accessed a gateway Task", tool)
				}
			}
			result := executeExternalTool(t, executor, "list_tasks", `{"namespace":"tool-target"}`)
			data, err := json.Marshal(result.Data)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Success || strings.Contains(string(data), "gateway-task") || !strings.Contains(string(data), "visible-task") {
				t.Fatalf("Task list did not filter gateway Tasks: %s, error=%s", data, result.Error)
			}
			if backend.writes != 0 || results.reads != 0 {
				t.Fatalf("gateway access reached backing state: writes=%d results=%d", backend.writes, results.reads)
			}
		})
	}
}

func TestCompatCoordinatorResourceAuthorization(t *testing.T) {
	for _, profile := range []compatProxyToolContextProfile{openAICompatProxyToolContextProfile, anthropicCompatProxyToolContextProfile} {
		t.Run(profile.SourceLabel, func(t *testing.T) {
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				return externalToolReview(spec.ResourceAttributes.Verb == "get"), nil
			})
			_, backend, _, _ := newExternalToolExecutor(clientset, &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: externalToolNamespace}})
			toolCtx := newCompatProxyToolContext(compatProxyToolContextConfig{
				Client: backend, KubeClient: clientset, Namespace: externalToolNamespace, UserInfo: externalToolUser(), Profile: profile,
			})
			for _, tool := range []string{"list_tasks", "cancel_task"} {
				result := executeToolCall(context.Background(), llm.ToolCall{Name: tool, Arguments: json.RawMessage(`{"name":"task"}`)}, time.Second, toolCtx)
				var decoded ToolResult
				if err := json.Unmarshal([]byte(result), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded.Success {
					t.Fatalf("%s escaped nested authorization", tool)
				}
			}
			if backend.writes != 0 || len(*reviews) != 3 {
				t.Fatalf("writes=%d reviews=%d, want zero writes and list/get/delete checks", backend.writes, len(*reviews))
			}
		})
	}
}

func TestCompatCoordinatorPRToolsUseRequestClient(t *testing.T) {
	for _, tool := range []tools.Tool{tools.NewCreatePullRequestTool(nil), tools.NewCheckPullRequestCITool(nil)} {
		for _, denyResource := range []string{"tasks", "secrets"} {
			t.Run(tool.Name()+"/"+denyResource, func(t *testing.T) {
				previous := tools.DefaultRegistry
				t.Cleanup(func() { tools.DefaultRegistry = previous })
				tools.DefaultRegistry = tools.NewRegistry()
				tools.DefaultRegistry.Register(tool)
				clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					return externalToolReview(spec.ResourceAttributes.Resource == "tasks" && denyResource != "tasks"), nil
				})
				_, backend, _, _ := newExternalToolExecutor(clientset, &corev1alpha1.Task{
					ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: externalToolNamespace},
					Spec:       corev1alpha1.TaskSpec{Workspace: &corev1alpha1.WorkspaceConfig{GitRepo: "https://github.com/example/repo", ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "git-read"}, ForgeCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "git-forge"}}},
				})
				toolCtx := newCompatProxyToolContext(compatProxyToolContextConfig{Client: backend, KubeClient: clientset, Namespace: externalToolNamespace, UserInfo: externalToolUser()})
				result := executeToolCall(context.Background(), llm.ToolCall{Name: tool.Name(), Arguments: json.RawMessage(`{"task_name":"task","head_branch":"feature","base_branch":"main","title":"test","pr_number":1}`)}, time.Second, toolCtx)
				wantReviews := 1
				if denyResource == "secrets" {
					wantReviews = 2
				}
				if !strings.Contains(result, "not authorized") || len(*reviews) != wantReviews || backend.reads != wantReviews-1 {
					t.Fatalf("PR tool did not use request authorization: result=%s reviews=%d reads=%d", result, len(*reviews), backend.reads)
				}
				a := (*reviews)[wantReviews-1].ResourceAttributes
				if a.Resource != denyResource || a.Verb != "get" || a.Namespace != externalToolNamespace {
					t.Fatalf("unexpected PR resource authorization: %#v", a)
				}
				if denyResource == "tasks" && a.Name != "task" {
					t.Fatalf("wrong Task name: %q", a.Name)
				}
				if denyResource == "secrets" && (a.Group != "" || a.Name != "git-read" && a.Name != "git-forge") {
					t.Fatalf("wrong Secret authorization: %#v", a)
				}
			})
		}
	}
}

func TestExternalToolPodLogsAuthorization(t *testing.T) {
	clientset, reviews := externalToolReviews(t, func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
		return externalToolReview(false), nil
	})
	toolCtx := newCompatProxyToolContext(compatProxyToolContextConfig{KubeClient: clientset, Namespace: externalToolNamespace, UserInfo: externalToolUser()})
	if err := toolCtx.AuthorizePodLogs(context.Background(), externalToolNamespace, "sandbox-pod"); !apierrors.IsForbidden(err) {
		t.Fatalf("pod log authorization = %v, want forbidden", err)
	}
	want := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Verb: "get", Resource: "pods", Subresource: "log", Name: "sandbox-pod"}
	if len(*reviews) != 1 || !reflect.DeepEqual(*(*reviews)[0].ResourceAttributes, want) {
		t.Fatalf("missing exact pod log authorization: %#v", reviews)
	}
}

func TestExternalToolChatReferencesAreAuthorizedBeforeStateAccess(t *testing.T) {
	for _, denied := range []string{"agent", "session-get", "session-update"} {
		t.Run(denied, func(t *testing.T) {
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				return externalToolReview(denied == "session-update" && spec.ResourceAttributes.Resource == "sessions" && spec.ResourceAttributes.Verb == "get"), nil
			})
			_, backend, results, _ := newExternalToolExecutor(clientset)
			sessions := &externalToolSessionAccessStore{SessionStore: newTestSessionStore(t)}
			cfg := DefaultChatConfig()
			handler := NewChatHandler(backend, nil, cfg, "", false, sessions, results, NewProviderResolver(backend, cfg), clientset)
			app := fiber.New()
			app.Post("/api/v1/chat", func(c fiber.Ctx) error {
				c.Locals(UserInfoContextKey, externalToolUser())
				return handler.HandleChat(c)
			})
			request := ChatRequest{Message: "continue", Namespace: externalToolNamespace}
			resource, target := "sessions", "selected-session"
			if denied == "agent" {
				request.AgentRef = "selected-agent"
				resource, target = "agents", "selected-agent"
			} else {
				request.SessionID = target
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			httpRequest := httptest.NewRequest(http.MethodPost, "/api/v1/chat", bytes.NewReader(body))
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Accept", "application/json")
			response, err := app.Test(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusForbidden || backend.reads != 0 || backend.writes != 0 || sessions.calls != 0 {
				t.Fatalf("reference preflight escaped: status=%d reads=%d writes=%d sessionCalls=%d", response.StatusCode, backend.reads, backend.writes, sessions.calls)
			}
			wantReviews := 1
			if denied == "session-update" {
				wantReviews = 2
			}
			if len(*reviews) != wantReviews {
				t.Fatalf("reviews=%d, want %d", len(*reviews), wantReviews)
			}
			for _, review := range *reviews {
				if a := review.ResourceAttributes; a.Namespace != externalToolNamespace || a.Resource != resource || a.Name != target {
					t.Fatalf("authorization target did not match handler target: %#v", a)
				}
			}
		})
	}
}

func TestExternalToolChatNamedSessionExactPermissions(t *testing.T) {
	const sessionID, providerType = "selected-session", "external-tool-session-test"
	for _, existing := range []bool{false, true} {
		t.Run("existing="+boolString(existing), func(t *testing.T) {
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				a := spec.ResourceAttributes
				return externalToolReview(a.Group == corev1alpha1.GroupVersion.Group && a.Resource == "sessions" && a.Namespace == externalToolNamespace && a.Name == sessionID && (a.Verb == "get" || a.Verb == "update")), nil
			})
			provider := &chatMockProvider{name: providerType, responses: []*llm.CompletionResponse{{Content: "continued"}}}
			llm.RegisterProvider(providerType, func(llm.ProviderConfig) (llm.Provider, error) { return provider, nil })
			backend := fake.NewClientBuilder().WithScheme(newTestScheme()).WithRuntimeObjects(providerCRD("chat-provider", externalToolNamespace, providerType, "test-model")...).Build()
			sessions := newTestSessionStore(t)
			if existing {
				if err := sessions.CreateSession(context.Background(), &store.SessionRecord{Namespace: externalToolNamespace, Name: sessionID, SessionType: "chat"}); err != nil {
					t.Fatal(err)
				}
				if err := sessions.AppendMessages(context.Background(), externalToolNamespace, sessionID, []store.SessionMessage{{Role: "assistant", Content: "earlier message"}}); err != nil {
					t.Fatal(err)
				}
			}
			cfg := DefaultChatConfig()
			cfg.Provider = "chat-provider"
			handler := NewChatHandler(backend, nil, cfg, "", false, sessions, newTestResultStore(t), NewProviderResolver(backend, cfg), clientset)
			app := fiber.New()
			app.Post("/api/v1/chat", func(c fiber.Ctx) error {
				c.Locals(UserInfoContextKey, externalToolUser())
				return handler.HandleChat(c)
			})
			body, err := json.Marshal(ChatRequest{Message: "continue", Namespace: externalToolNamespace, SessionID: sessionID})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/chat", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			response, err := app.Test(request)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			var result ChatResponse
			if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK || result.SessionID != sessionID || result.Message != "continued" || provider.callCount != 1 {
				t.Fatalf("named-session grant failed: status=%d session=%q message=%q providerCalls=%d", response.StatusCode, result.SessionID, result.Message, provider.callCount)
			}
			messages, err := sessions.LoadTranscript(context.Background(), externalToolNamespace, sessionID, 0)
			if err != nil {
				t.Fatal(err)
			}
			wantMessages := 2
			if existing {
				wantMessages++
			}
			if len(messages) != wantMessages || messages[len(messages)-1].Content != "continued" || existing && messages[0].Content != "earlier message" {
				t.Fatalf("authorized continuation did not preserve and append transcript: %#v", messages)
			}
			for i, verb := range []string{"get", "update"} {
				want := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Verb: verb, Group: corev1alpha1.GroupVersion.Group, Resource: "sessions", Name: sessionID}
				if len(*reviews) <= i || !reflect.DeepEqual(*(*reviews)[i].ResourceAttributes, want) {
					t.Fatalf("missing named sessions/%s preflight", verb)
				}
			}
		})
	}
}

func TestExternalToolChatRunningTaskCheckUsesCallerPermissions(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		for _, gatewayOwned := range []bool{false, true} {
			t.Run("allowed="+boolString(allowed)+"/gateway="+boolString(gatewayOwned), func(t *testing.T) {
				clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
					a := spec.ResourceAttributes
					return externalToolReview(allowed && a.Resource == "tasks" && a.Verb == "list" && a.Namespace == externalToolNamespace), nil
				})
				task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "running-task", Namespace: externalToolNamespace, Labels: map[string]string{labels.LabelChatSession: "chat-session"}}, Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning}}
				if gatewayOwned {
					task.Spec.RequestedBy = &corev1alpha1.RequestedBy{Issuer: "gateway.orka.ai/tool-target/ns-uid/gateway/gateway-uid"}
				}
				executor, backend, results, _ := newExternalToolExecutor(clientset, task)
				executor.tasksCreated = 1
				cfg := DefaultChatConfig()
				cfg.MaxIterations = 1
				handler := newTestChatHandler(t, backend, newTestSessionStore(t), results, cfg)
				provider := &chatMockProvider{}
				_, _, _, err := handler.runToolLoop(context.Background(), provider, []llm.Message{{Role: "user", Content: "continue"}}, "", nil, executor, "chat-session", externalToolNamespace, "test-model", 0, 100, 0, nil)
				if err != nil {
					t.Fatal(err)
				}
				wantCalls, wantReads := 1, 0
				if allowed {
					wantReads = 1
					if !gatewayOwned {
						wantCalls++
					}
				}
				if len(*reviews) != 1 || backend.reads != wantReads || provider.callCount != wantCalls {
					t.Fatalf("running Task check bypassed caller authorization: reviews=%d reads=%d providerCalls=%d", len(*reviews), backend.reads, provider.callCount)
				}
			})
		}
	}
}

func TestExternalToolChatDiscoveryOmitsForbiddenMetadata(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(boolString(allowed), func(t *testing.T) {
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				return externalToolReview(allowed && spec.ResourceAttributes.Verb == "list"), nil
			})
			_, backend, _, _ := newExternalToolExecutor(clientset,
				&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "private-agent", Namespace: externalToolNamespace}},
				&corev1alpha1.Tool{ObjectMeta: metav1.ObjectMeta{Name: "private-tool", Namespace: externalToolNamespace}, Spec: corev1alpha1.ToolSpec{Description: "private tool description"}},
				&corev1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "private-provider", Namespace: externalToolNamespace}},
				&corev1alpha1.Skill{ObjectMeta: metav1.ObjectMeta{Name: "private-skill", Namespace: externalToolNamespace}},
			)
			requestClient := newExternalToolClient(backend, clientset, externalToolUser(), externalToolNamespace, "", false, nil)
			builder := NewSystemPromptBuilder(externalToolDiscoveryClient{Client: requestClient}, externalToolNamespace)
			prompt, err := builder.BuildSystemPrompt(context.Background(), "", PromptModeFull)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(prompt, "private-") != allowed || len(*reviews) != 4 {
				t.Fatalf("discovery visibility differs from permission: containsPrivate=%v allowed=%v reviews=%d", strings.Contains(prompt, "private-"), allowed, len(*reviews))
			}
			if !allowed && backend.reads != 0 {
				t.Fatalf("forbidden discovery performed %d backing reads", backend.reads)
			}
		})
	}
}

func externalToolUser() *UserInfo {
	return &UserInfo{Username: "system:serviceaccount:caller:chat-user", UID: "caller-uid", Groups: []string{"system:serviceaccounts", "system:serviceaccounts:caller", "system:authenticated"}, Extra: map[string]authenticationv1.ExtraValue{"example.test/identity": {"verified"}}, Namespace: "caller", AuthType: AuthTypeTokenReview}
}

func externalToolReviews(t *testing.T, decide func(authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error)) (*kubernetesfake.Clientset, *[]authorizationv1.SubjectAccessReviewSpec) {
	t.Helper()
	clientset := kubernetesfake.NewClientset()
	reviews := []authorizationv1.SubjectAccessReviewSpec{}
	clientset.PrependReactor("create", "subjectaccessreviews", func(action kubernetestesting.Action) (bool, runtime.Object, error) {
		review := action.(kubernetestesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		wantUser := externalToolUser()
		if review.Spec.User != wantUser.Username || review.Spec.UID != wantUser.UID || !reflect.DeepEqual(review.Spec.Groups, wantUser.Groups) || !reflect.DeepEqual(review.Spec.Extra["example.test/identity"], authorizationv1.ExtraValue{"verified"}) {
			t.Errorf("SubjectAccessReview did not preserve authenticated identity")
		}
		reviews = append(reviews, *review.Spec.DeepCopy())
		result, err := decide(review.Spec)
		return true, result, err
	})
	return clientset, &reviews
}

func externalToolReview(allowed bool) *authorizationv1.SubjectAccessReview {
	return &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: allowed}}
}

func newExternalToolExecutor(clientset kubernetes.Interface, objects ...client.Object) (*ToolExecutor, *externalToolCountingClient, *externalToolCountingResults, *fakeSessionStore) {
	backend := &externalToolCountingClient{Client: fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objects...).Build()}
	results := &externalToolCountingResults{}
	sessions := &fakeSessionStore{}
	executor := NewToolExecutor(backend, controller.NewSessionManager(sessions), "caller", "chat-session", "", false, 5, time.Second, results, clientset)
	executor.userInfo = externalToolUser()
	return executor, backend, results, sessions
}

func executeExternalTool(t *testing.T, executor *ToolExecutor, name, args string) ToolResult {
	t.Helper()
	result, err := executor.Execute(context.Background(), llm.ToolCall{Name: name, Arguments: json.RawMessage(args)})
	if err != nil {
		t.Fatal(err)
	}
	var decoded ToolResult
	if err := json.Unmarshal([]byte(result), &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

type externalToolCountingClient struct {
	client.Client
	reads  int
	writes int
}

func (c *externalToolCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.reads++
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *externalToolCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.reads++
	return c.Client.List(ctx, list, opts...)
}

func (c *externalToolCountingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.writes++
	return c.Client.Create(ctx, obj, opts...)
}

func (c *externalToolCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.writes++
	return c.Client.Update(ctx, obj, opts...)
}

func (c *externalToolCountingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.writes++
	return c.Client.Delete(ctx, obj, opts...)
}

type externalToolCountingResults struct {
	store.ResultStore
	reads int
}

func (r *externalToolCountingResults) GetResult(context.Context, string, string) ([]byte, error) {
	r.reads++
	return []byte("task output"), nil
}

type externalToolGatewayStore struct {
	store.GatewayEventStore
	event *store.GatewayEvent
}

type externalToolSessionAccessStore struct {
	store.SessionStore
	calls int
}

func (s *externalToolSessionAccessStore) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	s.calls++
	return s.SessionStore.GetSession(ctx, namespace, name)
}

func (s *externalToolSessionAccessStore) CreateSession(ctx context.Context, session *store.SessionRecord) error {
	s.calls++
	return s.SessionStore.CreateSession(ctx, session)
}

func (s *externalToolSessionAccessStore) AcquireLock(ctx context.Context, namespace, name, taskName, taskUID string) error {
	s.calls++
	return s.SessionStore.AcquireLock(ctx, namespace, name, taskName, taskUID)
}

func (s *externalToolGatewayStore) GetGatewayEventForTask(_ context.Context, namespace, taskName, taskUID string) (*store.GatewayEvent, error) {
	if s.event.Namespace == namespace && s.event.TaskName == taskName && s.event.TaskUID == taskUID {
		return s.event, nil
	}
	return nil, store.ErrNotFound
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

var _ client.Client = (*externalToolCountingClient)(nil)
