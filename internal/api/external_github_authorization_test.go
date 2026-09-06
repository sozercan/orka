package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

func TestExternalGitHubToolsRequireTaskCredentials(t *testing.T) {
	previousRegistry := tools.DefaultRegistry
	tools.DefaultRegistry = tools.NewRegistry()
	tools.DefaultRegistry.Register(tools.NewCheckPullRequestCITool(nil))
	tools.DefaultRegistry.Register(tools.NewCreatePullRequestTool(nil))
	t.Cleanup(func() { tools.DefaultRegistry = previousRegistry })
	for _, test := range []struct {
		name, taskName, currentTask string
		credential, allowTask       bool
		allowSecret, permitted      bool
		oidc, trusted, createPR     bool
		wantReads                   int
	}{
		{name: "no-task"},
		{name: "whitespace-task", taskName: " "},
		{name: "whitespace-create-pr", taskName: " ", createPR: true},
		{name: "task-without-credential", taskName: "workspace-task", allowTask: true, wantReads: 1},
		{name: "task-read-denied", taskName: "workspace-task", credential: true, allowSecret: true},
		{name: "secret-read-denied", taskName: "workspace-task", credential: true, allowTask: true, wantReads: 1},
		{name: "task-credentials", taskName: "workspace-task", credential: true, allowTask: true, allowSecret: true, permitted: true, wantReads: 2},
		{name: "forge-credentials", taskName: "workspace-task", credential: true, allowTask: true, allowSecret: true, permitted: true, createPR: true, wantReads: 2},
		{name: "forge-secret-denied", taskName: "workspace-task", credential: true, allowTask: true, createPR: true, wantReads: 1},
		{name: "current-task-credentials", currentTask: "workspace-task", credential: true, allowTask: true, allowSecret: true, permitted: true, wantReads: 2},
		{name: "oidc-contract", oidc: true, permitted: true},
		{name: "trusted-worker-contract", trusted: true, permitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(workerenv.GitRepo, "https://github.com/controller/private")
			t.Setenv(workerenv.GitHubToken, "fixture-global")
			secretName := "repo-read"
			if test.createPR {
				secretName = "repo-forge"
			}
			clientset, reviews := externalToolReviews(t, func(spec authorizationv1.SubjectAccessReviewSpec) (runtime.Object, error) {
				a := *spec.ResourceAttributes
				taskRead := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Group: "core.orka.ai", Resource: "tasks", Verb: "get", Name: "workspace-task"}
				secretRead := authorizationv1.ResourceAttributes{Namespace: externalToolNamespace, Resource: "secrets", Verb: "get", Name: secretName}
				return externalToolReview(test.allowTask && a == taskRead || test.allowSecret && a == secretRead), nil
			})
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "workspace-task", Namespace: externalToolNamespace},
				Spec: corev1alpha1.TaskSpec{Workspace: &corev1alpha1.WorkspaceConfig{
					GitRepo: "https://github.com/allowed/repository",
				}},
			}
			if test.credential {
				task.Spec.Workspace.ReadCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "repo-read", Key: "token"}
				if test.createPR {
					task.Spec.Workspace.ForgeCredentialRef = &corev1alpha1.WorkspaceCredentialReference{Name: "repo-forge", Key: "token"}
				}
			}
			backend := &externalToolCountingClient{Client: fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(task,
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "repo-read", Namespace: externalToolNamespace}, Data: map[string][]byte{"token": []byte("fixture-task")}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "repo-forge", Namespace: externalToolNamespace}, Data: map[string][]byte{"token": []byte("fixture-forge")}},
			).Build()}
			tc := &tools.ToolContext{Client: backend, KubeClient: clientset, Namespace: externalToolNamespace, TaskID: test.currentTask}
			if !test.trusted {
				user := externalToolUser()
				if test.oidc {
					user.AuthType = AuthTypeOIDC
				}
				authorizeExternalToolContext(tc, user, nil)
			}
			httpCalls := 0
			previousTransport := http.DefaultTransport
			http.DefaultTransport = externalGitHubRoundTripper(func(req *http.Request) (*http.Response, error) {
				httpCalls++
				if req.URL.Host != "api.github.com" {
					return nil, errors.New("unexpected fixture HTTP host")
				}
				if test.permitted {
					wantToken, wantRepo := "fixture-task", "/repos/allowed/repository/"
					if test.createPR {
						wantToken = "fixture-forge"
					}
					if test.oidc || test.trusted {
						wantToken, wantRepo = "fixture-global", "/repos/controller/private/"
					}
					// Do not include credential values in assertion failures.
					require.True(t, req.Header.Get("Authorization") == "Bearer "+wantToken, "unexpected GitHub credential source")
					require.True(t, strings.HasPrefix(req.URL.Path, wantRepo), "unexpected GitHub repository scope")
				}
				body := ""
				switch {
				case strings.Contains(req.URL.Path, "/pulls"):
					body = `{"number":42,"state":"open","head":{"sha":"fixture-head"},"html_url":"https://github.com/allowed/repository/pull/42"}`
				case strings.HasSuffix(req.URL.Path, "/check-runs"):
					body = `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"success"}]}`
				case strings.HasSuffix(req.URL.Path, "/status"):
					body = `{"state":"success","total_count":1,"statuses":[{"context":"unit","state":"success"}]}`
				default:
					return nil, errors.New("unexpected fixture GitHub endpoint")
				}
				status := http.StatusOK
				if req.Method == http.MethodPost {
					status = http.StatusCreated
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = previousTransport })
			name := "check_pull_request_ci"
			arguments, err := json.Marshal(tools.CheckPullRequestCIArgs{TaskName: test.taskName, PRNumber: 42})
			require.NoError(t, err)
			if test.createPR {
				name = "create_pull_request"
				arguments, err = json.Marshal(tools.CreatePullRequestArgs{TaskName: test.taskName, HeadBranch: "feature", BaseBranch: "main", Title: "fixture"})
				require.NoError(t, err)
			}
			result := executeToolCall(t.Context(), llm.ToolCall{Name: name, Arguments: arguments}, time.Second, tc)
			require.Equal(t, test.wantReads, backend.reads)
			require.Zero(t, backend.writes)
			if test.permitted {
				if test.createPR {
					require.Contains(t, result, `"status":"created"`)
				} else {
					require.Contains(t, result, `"status":"passed"`)
				}
				require.Positive(t, httpCalls)
			} else {
				require.Zero(t, httpCalls, "unauthorized GitHub call reached HTTP")
				require.Contains(t, result, `"success":false`)
			}
			if test.oidc || test.trusted {
				require.Empty(t, *reviews)
			}
		})
	}
}

type externalGitHubRoundTripper func(*http.Request) (*http.Response, error)

func (f externalGitHubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
