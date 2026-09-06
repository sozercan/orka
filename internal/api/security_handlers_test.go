/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const (
	securityTestRepoURL   = "https://github.com/sozercan/actions-test"
	securityTestRepoPRURL = securityTestRepoURL + "/pull/99"
)

func TestSecurityRepositoryActions_ContextTokenAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	createBody := fmt.Sprintf(`{
		"name":"scan-create",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		scope  string
		want   int
	}{
		{
			name:   "list allowed with security read scope",
			method: http.MethodGet,
			path:   "/security/repositories?namespace=demo",
			scope:  ContextTokenScopeSecurityRead,
			want:   http.StatusOK,
		},
		{
			name:   "list denied without security read scope",
			method: http.MethodGet,
			path:   "/security/repositories?namespace=demo",
			scope:  ContextTokenScopeSecurityWrite,
			want:   http.StatusForbidden,
		},
		{
			name:   "create allowed with security write scope",
			method: http.MethodPost,
			path:   "/security/repositories",
			body:   createBody,
			scope:  ContextTokenScopeSecurityWrite,
			want:   http.StatusCreated,
		},
		{
			name:   "create denied without security write scope",
			method: http.MethodPost,
			path:   "/security/repositories",
			body:   createBody,
			scope:  ContextTokenScopeSecurityRead,
			want:   http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := setupSecurityHandlersWithAuthz(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce)
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

func setupSecurityHandlersWithAuthz(t *testing.T, ctxTokenConfig ContextTokenConfig, mode string, objs ...runtime.Object) *fiber.App {
	app, _ := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, mode, objs...)
	return app
}

func setupSecurityHandlersWithAuthzFixture(t *testing.T, ctxTokenConfig ContextTokenConfig, mode string, objs ...runtime.Object) (*fiber.App, *Handlers) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RepositoryScan{}).
		WithRuntimeObjects(objs...).
		Build()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	securityStore := sqlite.NewStore(db, ":memory:")
	authz, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{
		Mode: mode,
	})
	require.NoError(t, err)

	handlers := NewHandlers(HandlersConfig{
		Client:                    fakeClient,
		SecurityStore:             securityStore,
		ContextTokenAuthorization: authz,
	})

	app := fiber.New()
	app.Use(NewAuthMiddleware(handlers.client, AuthConfig{ContextTokens: ctxTokenConfig}))
	app.Post("/security/repositories", handlers.CreateRepositoryScan)
	app.Get("/security/repositories", handlers.ListRepositoryScans)
	app.Get("/security/repositories/:name", handlers.GetRepositoryScan)
	app.Put("/security/repositories/:name", handlers.UpdateRepositoryScan)
	app.Delete("/security/repositories/:name", handlers.DeleteRepositoryScan)
	app.Get("/security/repositories/:name/threat-model", handlers.GetThreatModel)
	app.Put("/security/repositories/:name/threat-model", handlers.UpdateThreatModel)
	app.Get("/security/repositories/:name/scans", handlers.ListSecurityScanRuns)
	app.Post("/security/repositories/:name/scans", handlers.CreateManualSecurityScan)
	app.Get("/security/repositories/:name/findings", handlers.ListSecurityFindings)
	app.Get("/security/findings/:id", handlers.GetSecurityFinding)
	app.Get("/security/findings/:id/patches", handlers.ListSecurityPatchProposals)
	app.Post("/security/findings/:id/dismiss", handlers.DismissSecurityFinding)
	app.Post("/security/findings/:id/reopen", handlers.ReopenSecurityFinding)
	app.Post("/security/findings/:id/validate", handlers.ValidateSecurityFinding)
	app.Post("/security/findings/:id/patch", handlers.GenerateSecurityPatch)
	app.Post("/security/findings/:id/pull-request", handlers.CreateSecurityPullRequest)
	return app, handlers
}

func TestGenerateSecurityPatch_ContextTokenTransactionContextAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL

	tests := []struct {
		name string
		tctx map[string]any
		want int
	}{
		{
			name: "matching repo branch and agent creates governed patch",
			tctx: map[string]any{
				"namespace":     "demo",
				"repo":          repoURL,
				"branch":        "main",
				"agent":         "demo/patch",
				"allowedAgents": []any{"demo/patch"},
			},
			want: http.StatusCreated,
		},
		{
			name: "mismatched repo denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      "https://github.com/sozercan/other",
				"branch":    "main",
				"agent":     "demo/patch",
			},
			want: http.StatusForbidden,
		},
		{
			name: "mismatched branch denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "release",
				"agent":     "demo/patch",
			},
			want: http.StatusForbidden,
		},
		{
			name: "mismatched agent denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "main",
				"agent":     "demo/analysis",
			},
			want: http.StatusForbidden,
		},
		{
			name: "disallowed allowed agents denied",
			tctx: map[string]any{
				"namespace":     "demo",
				"repo":          repoURL,
				"branch":        "main",
				"allowedAgents": []any{"demo/analysis"},
			},
			want: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patchAgent := corev1alpha1.AgentReference{Name: "patch"}
			scan := &corev1alpha1.RepositoryScan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "scan-1",
					Namespace: "demo",
				},
				Spec: corev1alpha1.RepositoryScanSpec{
					RepoURL:                      repoURL,
					Branch:                       "main",
					ReadCredentialRef:            &corev1.LocalObjectReference{Name: "source-read"},
					PublicationReadCredentialRef: &corev1.LocalObjectReference{Name: "target-read"},
					PublicationCredentialRef:     &corev1.LocalObjectReference{Name: "target-write"},
					ForgeCredentialRef:           &corev1.LocalObjectReference{Name: "forge"},
					AnalysisAgentRef:             corev1alpha1.AgentReference{Name: "analysis"},
					PatchAgentRef:                &patchAgent,
				},
			}
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)

			ctx := context.Background()
			require.NoError(t, handlers.securityStore.UpsertFinding(ctx, &store.Finding{
				ID:             "finding-1",
				Namespace:      "demo",
				RepositoryScan: "scan-1",
				ScanRunID:      "scan-run-1",
				Fingerprint:    "fp-1",
				Title:          "Command injection",
				Summary:        "Unsanitized user input reaches shell execution.",
				Severity:       "critical",
				Confidence:     "high",
				State:          "validated",
				RootCause:      "Shell command arguments are concatenated directly.",
				Remediation:    "Use argument arrays and validate inputs.",
			}))

			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeSecretsCredentialsRead,
				"tctx":  tt.tctx,
			})
			req := httptest.NewRequest(http.MethodPost, "/security/findings/finding-1/patch?namespace=demo", nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.StatusCode)

			if tt.want == http.StatusCreated {
				var tasks corev1alpha1.TaskList
				require.NoError(t, handlers.client.List(ctx, &tasks, client.InNamespace("demo")))
				require.Len(t, tasks.Items, 1)
				proposals, err := handlers.securityStore.ListPatchProposals(ctx, "demo", "finding-1")
				require.NoError(t, err)
				require.Len(t, proposals, 1)
			}
		})
	}
}

func TestCreateManualSecurityScan_ContextTokenTransactionContextAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL

	tests := []struct {
		name    string
		scanRef string
		tctx    map[string]any
	}{
		{
			name: "mismatched repo denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      "https://github.com/sozercan/other",
				"branch":    "main",
				"agent":     "demo/analysis",
			},
		},
		{
			name: "mismatched branch denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "release",
				"agent":     "demo/analysis",
			},
		},
		{
			name:    "mismatched ref denied",
			scanRef: "refs/tags/allowed",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "main",
				"ref":       "refs/tags/disallowed",
				"agent":     "demo/analysis",
			},
		},
		{
			name:    "branch-only token denies ref checkout",
			scanRef: "refs/tags/allowed",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "main",
				"agent":     "demo/analysis",
			},
		},
		{
			name: "mismatched agent denied",
			tctx: map[string]any{
				"namespace": "demo",
				"repo":      repoURL,
				"branch":    "main",
				"agent":     "demo/other",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := &corev1alpha1.RepositoryScan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "scan-1",
					Namespace: "demo",
				},
				Spec: corev1alpha1.RepositoryScanSpec{
					RepoURL:          repoURL,
					Branch:           "main",
					Ref:              tt.scanRef,
					AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
				},
			}
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite,
				"tctx":  tt.tctx,
			})
			req := httptest.NewRequest(http.MethodPost, "/security/repositories/scan-1/scans?namespace=demo", nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			var tasks corev1alpha1.TaskList
			require.NoError(t, handlers.client.List(context.Background(), &tasks, client.InNamespace("demo")))
			require.Empty(t, tasks.Items)
		})
	}
}

func TestCreateManualSecurityScan_ContextTokenAllowsRefOnlyWorkspaceWithBranchAndRef(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          repoURL,
			Ref:              "refs/tags/v1.0.0",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityWrite,
		"tctx": map[string]any{
			"namespace": "demo",
			"repo":      repoURL,
			"branch":    "release",
			"ref":       "refs/tags/v1.0.0",
			"agent":     "demo/analysis",
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/security/repositories/scan-1/scans?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var run store.ScanRun
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&run))
	task := &corev1alpha1.Task{}
	require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey(run.TaskName), task))
	require.NotNil(t, task.Spec.Workspace)
	require.Empty(t, task.Spec.Workspace.Branch)
	require.Equal(t, "refs/tags/v1.0.0", task.Spec.Workspace.Ref)
}

func TestRepositoryScanMutations_ContextTokenTransactionContextAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL
	createBody := fmt.Sprintf(`{
		"name":"scan-create",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)
	updateBody := fmt.Sprintf(`{
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		objs   []runtime.Object
	}{
		{
			name:   "create repository scan mismatched repo denied",
			method: http.MethodPost,
			path:   "/security/repositories",
			body:   createBody,
		},
		{
			name:   "update repository scan mismatched repo denied",
			method: http.MethodPut,
			path:   "/security/repositories/scan-1?namespace=demo",
			body:   updateBody,
			objs: []runtime.Object{
				&corev1alpha1.RepositoryScan{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "scan-1",
						Namespace: "demo",
					},
					Spec: corev1alpha1.RepositoryScanSpec{
						RepoURL:          repoURL,
						Branch:           "main",
						AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, tt.objs...)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite,
				"tctx": map[string]any{
					"namespace": "demo",
					"repo":      "https://github.com/sozercan/other",
					"branch":    "main",
					"agent":     "demo/analysis",
				},
			})
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			var got corev1alpha1.RepositoryScan
			err = handlers.client.Get(context.Background(), clientObjectKey("scan-create"), &got)
			require.Error(t, err)
		})
	}
}

func TestCreateRepositoryScanPolicyRefs(t *testing.T) {
	ctx := context.Background()
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	policy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: "demo", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}},
		Data:       map[string]string{"scan": "Focus on operator repositories.", "fp": "Ignore public docs examples."},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, policy)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeConfigMapsRead})
	body := fmt.Sprintf(`{
		"name":"scan-policy-test",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"},
			"customScanInstructionsRef":{"name":"scan-policy","key":"scan"},
			"falsePositivePolicyRef":{"name":"scan-policy","key":"fp"}
		}
	}`, securityTestRepoURL)
	req := httptest.NewRequest(http.MethodPost, "/security/repositories", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var got corev1alpha1.RepositoryScan
	require.NoError(t, handlers.client.Get(ctx, client.ObjectKey{Namespace: "demo", Name: "scan-policy-test"}, &got))
	require.Equal(t, "scan-policy", got.Spec.CustomScanInstructionsRef.Name)
	require.Equal(t, "fp", got.Spec.FalsePositivePolicyRef.Key)
}

func TestCreateRepositoryScanPolicyRefRequiresConfigMapReadScope(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: "demo", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": "policy"}}
	app, _ := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, policy)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})
	body := fmt.Sprintf(`{
		"name":"scan-policy-test",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"},
			"customScanInstructionsRef":{"name":"scan-policy"}
		}
	}`, securityTestRepoURL)
	req := httptest.NewRequest(http.MethodPost, "/security/repositories", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestCreateRepositoryScanPolicyRefMissingKeyFails(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: "demo", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"other": "policy"}}
	app, _ := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, policy)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeConfigMapsRead})
	body := fmt.Sprintf(`{
		"name":"scan-policy-test",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"},
			"customScanInstructionsRef":{"name":"scan-policy","key":"missing"}
		}
	}`, securityTestRepoURL)
	req := httptest.NewRequest(http.MethodPost, "/security/repositories", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCreateRepositoryScanPolicyRefOversizedFails(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: "demo", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": strings.Repeat("a", security.MaxCustomPolicyBytes+1)}}
	app, _ := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, policy)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeConfigMapsRead})
	body := fmt.Sprintf(`{
		"name":"scan-policy-test",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"},
			"customScanInstructionsRef":{"name":"scan-policy"}
		}
	}`, securityTestRepoURL)
	req := httptest.NewRequest(http.MethodPost, "/security/repositories", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCreateRepositoryScanPolicyRefOtherNamespaceFails(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "scan-policy", Namespace: "other", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}}, Data: map[string]string{"policy": "policy"}}
	app, _ := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, policy)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeConfigMapsRead})
	body := fmt.Sprintf(`{
		"name":"scan-policy-test",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"analysisAgentRef":{"name":"analysis"},
			"customScanInstructionsRef":{"name":"scan-policy"}
		}
	}`, securityTestRepoURL)
	req := httptest.NewRequest(http.MethodPost, "/security/repositories", strings.NewReader(body))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRepositoryScanMutations_ContextTokenRefAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL
	createBody := fmt.Sprintf(`{
		"name":"scan-create",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"ref":"refs/tags/disallowed",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)
	updateBody := fmt.Sprintf(`{
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"ref":"refs/tags/disallowed",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "create repository scan mismatched ref denied",
			method: http.MethodPost,
			path:   "/security/repositories",
			body:   createBody,
		},
		{
			name:   "update repository scan mismatched ref denied",
			method: http.MethodPut,
			path:   "/security/repositories/scan-1?namespace=demo",
			body:   updateBody,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := securityAuthzTestRepositoryScan("scan-1", repoURL)
			existing.Spec.Ref = "refs/tags/allowed"
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, existing)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite,
				"tctx": map[string]any{
					"namespace": "demo",
					"repo":      repoURL,
					"branch":    "main",
					"ref":       "refs/tags/allowed",
					"agent":     "demo/analysis",
				},
			})

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			var created corev1alpha1.RepositoryScan
			err = handlers.client.Get(context.Background(), clientObjectKey("scan-create"), &created)
			require.Error(t, err)

			var updated corev1alpha1.RepositoryScan
			require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey("scan-1"), &updated))
			require.Equal(t, "refs/tags/allowed", updated.Spec.Ref)
		})
	}
}

func TestRepositoryScanMutations_ContextTokenBranchOnlyDeniesRef(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	repoURL := securityTestRepoURL
	createBody := fmt.Sprintf(`{
		"name":"scan-create",
		"namespace":"demo",
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"ref":"refs/tags/disallowed",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)
	updateBody := fmt.Sprintf(`{
		"spec":{
			"repoURL":%q,
			"branch":"main",
			"ref":"refs/tags/disallowed",
			"analysisAgentRef":{"name":"analysis"}
		}
	}`, securityTestRepoURL)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "create repository scan ref denied by branch-only token",
			method: http.MethodPost,
			path:   "/security/repositories",
			body:   createBody,
		},
		{
			name:   "update repository scan ref denied by branch-only token",
			method: http.MethodPut,
			path:   "/security/repositories/scan-1?namespace=demo",
			body:   updateBody,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := securityAuthzTestRepositoryScan("scan-1", repoURL)
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, existing)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite,
				"tctx": map[string]any{
					"namespace": "demo",
					"repo":      repoURL,
					"branch":    "main",
					"agent":     "demo/analysis",
				},
			})

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			var created corev1alpha1.RepositoryScan
			err = handlers.client.Get(context.Background(), clientObjectKey("scan-create"), &created)
			require.Error(t, err)

			var updated corev1alpha1.RepositoryScan
			require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey("scan-1"), &updated))
			require.Empty(t, updated.Spec.Ref)
		})
	}
}

func securityAuthzTestRepositoryScan(name, repoURL string) *corev1alpha1.RepositoryScan {
	return &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          repoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
}

func securityAuthzTestTctx(repoURL string) map[string]any {
	return map[string]any{
		"namespace": "demo",
		"repo":      repoURL,
		"branch":    "main",
		"agent":     "demo/analysis",
	}
}

func TestUpdateRepositoryScan_ContextTokenAuthorizesExistingScanBeforeRequestBody(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	existing := securityAuthzTestRepositoryScan("scan-1", "https://github.com/sozercan/other")
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, existing)

	bodyBytes, err := json.Marshal(UpdateRepositoryScanRequest{
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	})
	require.NoError(t, err)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityWrite,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})

	req := httptest.NewRequest(http.MethodPut, "/security/repositories/scan-1?namespace=demo", strings.NewReader(string(bodyBytes)))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var got corev1alpha1.RepositoryScan
	require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey("scan-1"), &got))
	require.Equal(t, "https://github.com/sozercan/other", got.Spec.RepoURL)
}

func TestUpdateRepositoryScanRejectsRepositoryChange(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	existing := securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL)
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeOff, existing)

	bodyBytes, err := json.Marshal(UpdateRepositoryScanRequest{
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          "https://github.com/sozercan/other",
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	})
	require.NoError(t, err)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})
	req := httptest.NewRequest(http.MethodPut, "/security/repositories/scan-1?namespace=demo", strings.NewReader(string(bodyBytes)))
	req.Header.Set(TransactionTokenHeaderName, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	var got corev1alpha1.RepositoryScan
	require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey("scan-1"), &got))
	require.Equal(t, securityTestRepoURL, got.Spec.RepoURL)
}

func TestListRepositoryScans_ContextTokenFiltersMismatchedScansInEnforceMode(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app := setupSecurityHandlersWithAuthz(
		t,
		ctxTokenConfig,
		ContextTokenAuthorizationModeEnforce,
		securityAuthzTestRepositoryScan("scan-match", securityTestRepoURL),
		securityAuthzTestRepositoryScan("scan-mismatch", "https://github.com/sozercan/other"),
	)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityRead,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})

	req := httptest.NewRequest(http.MethodGet, "/security/repositories?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got struct {
		Items    []corev1alpha1.RepositoryScan `json:"items"`
		Metadata ListMeta                      `json:"metadata"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got.Items, 1)
	require.Equal(t, "scan-match", got.Items[0].Name)
	require.Equal(t, securityTestRepoURL, got.Items[0].Spec.RepoURL)
}

func TestRepositoryScanReadDelete_ContextTokenObjectAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name   string
		method string
		scope  string
	}{
		{
			name:   "get repository scan mismatched repo denied",
			method: http.MethodGet,
			scope:  ContextTokenScopeSecurityRead,
		},
		{
			name:   "delete repository scan mismatched repo denied",
			method: http.MethodDelete,
			scope:  ContextTokenScopeSecurityWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, handlers := setupSecurityHandlersWithAuthzFixture(
				t,
				ctxTokenConfig,
				ContextTokenAuthorizationModeEnforce,
				securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
			)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": tt.scope,
				"tctx":  securityAuthzTestTctx("https://github.com/sozercan/other"),
			})

			req := httptest.NewRequest(tt.method, "/security/repositories/scan-1?namespace=demo", nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			var got corev1alpha1.RepositoryScan
			require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey("scan-1"), &got))
		})
	}
}

func TestThreatModelRedactsEditedContent(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app, _ := setupSecurityHandlersWithAuthzFixture(
		t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce,
		securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
	)
	credential := "ghp_" + strings.Repeat("a", 36)
	body, err := json.Marshal(UpdateThreatModelRequest{
		Content: "Notes for `config/auth.go:12`.\n\n\t" + credential[:2] + "\u200b" + credential[2:] + "\n\nRotate keys.",
	})
	require.NoError(t, err)
	const want = "Notes for `config/auth.go:12`.\n\n\t[REDACTED]\n\nRotate keys."
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityRead + " " + ContextTokenScopeSecurityWrite,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		req := httptest.NewRequest(method, "/security/repositories/scan-1/threat-model?namespace=demo", strings.NewReader(string(body)))
		req.Header.Set(TransactionTokenHeaderName, token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var model store.ThreatModel
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&model))
		require.NoError(t, resp.Body.Close())
		if model.Content != want {
			t.Fatalf("%s returned unsanitized threat-model content", method)
		}
		require.Equal(t, "edited", model.Source)
		require.Equal(t, int64(1), model.Version)
	}
}

func TestThreatModelRejectsBlankSanitizedEdit(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app, handlers := setupSecurityHandlersWithAuthzFixture(
		t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce,
		securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
	)
	ctx := context.Background()
	const content = "Existing threat-model notes."
	require.NoError(t, handlers.securityStore.SaveThreatModel(ctx, &store.ThreatModel{
		Namespace: "demo", RepositoryScan: "scan-1", Content: content, Source: "edited",
	}))
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityWrite,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})
	for name, input := range map[string]string{
		"empty":         "",
		"whitespace":    " \n\t",
		"format runes":  "\u200b\u202e",
		"control runes": "\x00\x1b",
		"mixed":         " \n\t\u200b\x00",
	} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(UpdateThreatModelRequest{Content: input})
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPut, "/security/repositories/scan-1/threat-model?namespace=demo", strings.NewReader(string(body)))
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			model, err := handlers.securityStore.GetLatestThreatModel(ctx, "demo", "scan-1")
			require.NoError(t, err)
			require.Equal(t, content, model.Content)
			require.Equal(t, int64(1), model.Version)
		})
	}
}

func TestThreatModel_ContextTokenRepositoryScanAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name   string
		method string
		scope  string
		body   string
	}{
		{
			name:   "get threat model mismatched repo denied",
			method: http.MethodGet,
			scope:  ContextTokenScopeSecurityRead,
		},
		{
			name:   "update threat model mismatched repo denied",
			method: http.MethodPut,
			scope:  ContextTokenScopeSecurityWrite,
			body:   `{"content":"updated","source":"edited"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, handlers := setupSecurityHandlersWithAuthzFixture(
				t,
				ctxTokenConfig,
				ContextTokenAuthorizationModeEnforce,
				securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
			)
			ctx := context.Background()
			require.NoError(t, handlers.securityStore.SaveThreatModel(ctx, &store.ThreatModel{
				Namespace:      "demo",
				RepositoryScan: "scan-1",
				Content:        "model",
				Source:         "generated",
			}))
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": tt.scope,
				"tctx":  securityAuthzTestTctx("https://github.com/sozercan/other"),
			})

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}
			req := httptest.NewRequest(tt.method, "/security/repositories/scan-1/threat-model?namespace=demo", body)
			req.Header.Set(TransactionTokenHeaderName, token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			model, err := handlers.securityStore.GetLatestThreatModel(ctx, "demo", "scan-1")
			require.NoError(t, err)
			require.Equal(t, "model", model.Content)
		})
	}
}

func TestSecurityScanRunAndFindingLists_ContextTokenRepositoryScanAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	tests := []struct {
		name string
		path string
		seed func(t *testing.T, handlers *Handlers)
	}{
		{
			name: "list scan runs mismatched repo denied",
			path: "/security/repositories/scan-1/scans?namespace=demo",
			seed: func(t *testing.T, handlers *Handlers) {
				t.Helper()
				require.NoError(t, handlers.securityStore.CreateScanRun(context.Background(), &store.ScanRun{
					ID:             "run-1",
					Namespace:      "demo",
					RepositoryScan: "scan-1",
					Mode:           "manual",
					Phase:          "completed",
				}))
			},
		},
		{
			name: "list findings mismatched repo denied",
			path: "/security/repositories/scan-1/findings?namespace=demo",
			seed: func(t *testing.T, handlers *Handlers) {
				t.Helper()
				require.NoError(t, handlers.securityStore.UpsertFinding(context.Background(), &store.Finding{
					ID:             "finding-1",
					Namespace:      "demo",
					RepositoryScan: "scan-1",
					Fingerprint:    "fp-1",
					Title:          "Command injection",
					Summary:        "Unsanitized user input reaches shell execution.",
					Severity:       "critical",
					Confidence:     "high",
					State:          "open",
				}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, handlers := setupSecurityHandlersWithAuthzFixture(
				t,
				ctxTokenConfig,
				ContextTokenAuthorizationModeEnforce,
				securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
			)
			tt.seed(t, handlers)
			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityRead,
				"tctx":  securityAuthzTestTctx("https://github.com/sozercan/other"),
			})

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}

func TestListSecurityFindingsReturnsEmptyItemsArray(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app, _ := setupSecurityHandlersWithAuthzFixture(
		t,
		ctxTokenConfig,
		ContextTokenAuthorizationModeEnforce,
		securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
	)
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityRead,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})

	req := httptest.NewRequest(http.MethodGet, "/security/repositories/scan-1/findings?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.JSONEq(t, "[]", string(body["items"]))
}

func TestGetSecurityFinding_ContextTokenAuthorizesFindingRepositoryScan(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	app, handlers := setupSecurityHandlersWithAuthzFixture(
		t,
		ctxTokenConfig,
		ContextTokenAuthorizationModeEnforce,
		securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL),
	)
	ctx := context.Background()
	require.NoError(t, handlers.securityStore.UpsertFinding(ctx, &store.Finding{
		ID:             "finding-1",
		Namespace:      "demo",
		RepositoryScan: "scan-1",
		Fingerprint:    "fp-1",
		Title:          "Command injection",
		Summary:        "Unsanitized user input reaches shell execution.",
		Severity:       "critical",
		Confidence:     "high",
		State:          "open",
	}))
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityRead,
		"tctx":  securityAuthzTestTctx("https://github.com/sozercan/other"),
	})

	req := httptest.NewRequest(http.MethodGet, "/security/findings/finding-1?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestListSecurityPatchProposals_ContextTokenUsesPatchAgentAuthorization(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scan := securityAuthzTestRepositoryScan("scan-1", securityTestRepoURL)
	scan.Spec.PatchAgentRef = &corev1alpha1.AgentReference{Name: "patch"}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)

	ctx := context.Background()
	require.NoError(t, handlers.securityStore.UpsertFinding(ctx, &store.Finding{
		ID:             "finding-1",
		Namespace:      "demo",
		RepositoryScan: "scan-1",
		Fingerprint:    "fp-1",
		Title:          "Command injection",
		Summary:        "Unsanitized user input reaches shell execution.",
		Severity:       "critical",
		Confidence:     "high",
		State:          "open",
	}))
	require.NoError(t, handlers.securityStore.CreatePatchProposal(ctx, &store.PatchProposal{
		ID:             "proposal-1",
		Namespace:      "demo",
		RepositoryScan: "scan-1",
		FindingID:      "finding-1",
		Status:         "ready",
	}))
	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityRead,
		"tctx":  securityAuthzTestTctx(securityTestRepoURL),
	})

	req := httptest.NewRequest(http.MethodGet, "/security/findings/finding-1/patches?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestSecurityFindingMutations_ContextTokenTransactionContextAuthorizationDenials(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}

	tests := []struct {
		name    string
		path    string
		initial string
	}{
		{
			name:    "dismiss finding mismatched repo denied",
			path:    "/security/findings/finding-1/dismiss?namespace=demo",
			initial: "open",
		},
		{
			name:    "reopen finding mismatched repo denied",
			path:    "/security/findings/finding-1/reopen?namespace=demo",
			initial: "dismissed",
		},
		{
			name:    "validate finding mismatched repo denied",
			path:    "/security/findings/finding-1/validate?namespace=demo",
			initial: "open",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan.DeepCopyObject())
			ctx := context.Background()
			require.NoError(t, handlers.securityStore.UpsertFinding(ctx, &store.Finding{
				ID:             "finding-1",
				Namespace:      "demo",
				RepositoryScan: "scan-1",
				ScanRunID:      "scan-run-1",
				Fingerprint:    "fp-1",
				Title:          "Command injection",
				Summary:        "Unsanitized user input reaches shell execution.",
				Severity:       "critical",
				Confidence:     "high",
				State:          tt.initial,
			}))

			token := issueTestContextToken(t, provider, nil, map[string]any{
				"scope": ContextTokenScopeSecurityWrite,
				"tctx": map[string]any{
					"namespace": "demo",
					"repo":      "https://github.com/sozercan/other",
					"branch":    "main",
					"agent":     "demo/analysis",
				},
			})
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusForbidden, resp.StatusCode)

			finding, err := handlers.securityStore.GetFinding(ctx, "demo", "finding-1")
			require.NoError(t, err)
			require.Equal(t, tt.initial, finding.State)
			var tasks corev1alpha1.TaskList
			require.NoError(t, handlers.client.List(ctx, &tasks, client.InNamespace("demo")))
			require.Empty(t, tasks.Items)
		})
	}
}

func TestSecurityFindingMutationsResolveCanonicalAlias(t *testing.T) {
	setup := func(t *testing.T, canonicalState, aliasState string) (*fiber.App, *Handlers) {
		t.Helper()

		scheme := runtime.NewScheme()
		require.NoError(t, corev1alpha1.AddToScheme(scheme))
		require.NoError(t, corev1.AddToScheme(scheme))
		scan := &corev1alpha1.RepositoryScan{
			ObjectMeta: metav1.ObjectMeta{Name: "scan-1", Namespace: "demo"},
			Spec: corev1alpha1.RepositoryScanSpec{
				RepoURL:                      securityTestRepoURL,
				Branch:                       "main",
				ReadCredentialRef:            &corev1.LocalObjectReference{Name: "source-read"},
				PublicationReadCredentialRef: &corev1.LocalObjectReference{Name: "target-read"},
				PublicationCredentialRef:     &corev1.LocalObjectReference{Name: "target-write"},
				ForgeCredentialRef:           &corev1.LocalObjectReference{Name: "forge"},
				AnalysisAgentRef:             corev1alpha1.AgentReference{Name: "analysis"},
				PatchAgentRef:                &corev1alpha1.AgentReference{Name: "patch"},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan).Build()
		db, err := sqlite.NewDB(":memory:")
		require.NoError(t, err)
		securityStore := sqlite.NewStore(db, ":memory:")
		handlers := NewHandlers(HandlersConfig{Client: fakeClient, SecurityStore: securityStore})

		ctx := context.Background()
		require.NoError(t, securityStore.UpsertFinding(ctx, &store.Finding{
			ID:             "finding-1",
			Namespace:      "demo",
			RepositoryScan: "scan-1",
			ScanRunID:      "scan-run-1",
			Fingerprint:    "fp-1",
			Title:          "Command injection",
			Summary:        "Unsanitized user input reaches shell execution.",
			Severity:       "critical",
			Confidence:     "high",
			State:          canonicalState,
		}))
		require.NoError(t, securityStore.UpsertFinding(ctx, &store.Finding{
			ID:               "finding-alias",
			Namespace:        "demo",
			RepositoryScan:   "scan-1",
			ScanRunID:        "scan-run-2",
			Fingerprint:      "fp-alias",
			Title:            "Command injection alias",
			Summary:          "A duplicate observation of the command injection finding.",
			Severity:         "critical",
			Confidence:       "high",
			State:            aliasState,
			DuplicateOf:      "finding-1",
			ValidationStatus: "failed",
		}))

		app := fiber.New()
		app.Post("/security/findings/:id/dismiss", handlers.DismissSecurityFinding)
		app.Post("/security/findings/:id/reopen", handlers.ReopenSecurityFinding)
		app.Post("/security/findings/:id/validate", handlers.ValidateSecurityFinding)
		app.Post("/security/findings/:id/patch", handlers.GenerateSecurityPatch)
		return app, handlers
	}

	t.Run("dismiss", func(t *testing.T) {
		app, handlers := setup(t, "open", "open")
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/dismiss?namespace=demo", nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode)

		canonical, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-1")
		require.NoError(t, err)
		require.Equal(t, "dismissed", canonical.State)
		alias, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-alias")
		require.NoError(t, err)
		require.Equal(t, "open", alias.State)
	})

	t.Run("reopen", func(t *testing.T) {
		app, handlers := setup(t, "dismissed", "dismissed")
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/reopen?namespace=demo", nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode)

		canonical, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-1")
		require.NoError(t, err)
		require.Equal(t, "open", canonical.State)
		alias, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-alias")
		require.NoError(t, err)
		require.Equal(t, "dismissed", alias.State)
	})

	t.Run("validate", func(t *testing.T) {
		app, handlers := setup(t, "open", "open")
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/validate?namespace=demo", nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)

		canonical, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-1")
		require.NoError(t, err)
		require.Equal(t, "pending", canonical.ValidationStatus)
		alias, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-alias")
		require.NoError(t, err)
		require.Equal(t, "failed", alias.ValidationStatus)
		var tasks corev1alpha1.TaskList
		require.NoError(t, handlers.client.List(context.Background(), &tasks, client.InNamespace("demo")))
		require.Len(t, tasks.Items, 1)
		require.Equal(t, "finding-1", tasks.Items[0].Labels[labels.LabelSecurityFindingID])

		canonical.ScanRunID = "scan-run-3"
		require.NoError(t, handlers.securityStore.UpsertFinding(context.Background(), canonical))
		resp, err = app.Test(httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/validate?namespace=demo", nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		require.NoError(t, handlers.client.List(context.Background(), &tasks, client.InNamespace("demo")))
		require.Len(t, tasks.Items, 2)
		names := map[string]bool{}
		for i := range tasks.Items {
			names[tasks.Items[i].Name] = true
		}
		require.True(t, names[security.ScanStageTaskName("scan-1", "validation", security.StageValidation, "finding-1-scan-run-1")])
		require.True(t, names[security.ScanStageTaskName("scan-1", "validation", security.StageValidation, "finding-1-scan-run-3")])
	})

	t.Run("generate patch", func(t *testing.T) {
		app, handlers := setup(t, "validated", "open")
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/patch?namespace=demo", nil))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode)

		canonical, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-1")
		require.NoError(t, err)
		require.Equal(t, "patch_pending", canonical.State)
		require.NotEmpty(t, canonical.PatchProposalID)
		alias, err := handlers.securityStore.GetFinding(context.Background(), "demo", "finding-alias")
		require.NoError(t, err)
		require.Equal(t, "open", alias.State)
		require.Empty(t, alias.PatchProposalID)
		proposals, err := handlers.securityStore.ListPatchProposals(context.Background(), "demo", "finding-1")
		require.NoError(t, err)
		require.Len(t, proposals, 1)
		require.Equal(t, "finding-1", proposals[0].FindingID)
	})
}

func TestCreateSecurityPullRequest_ContextTokenTransactionContextAuthorizationDenied(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
			PatchAgentRef:    &corev1alpha1.AgentReference{Name: "patch"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)
	ctx := context.Background()
	require.NoError(t, handlers.securityStore.UpsertFinding(ctx, &store.Finding{
		ID:             "finding-1",
		Namespace:      "demo",
		RepositoryScan: "scan-1",
		ScanRunID:      "scan-run-1",
		Fingerprint:    "fp-1",
		Title:          "Command injection",
		Summary:        "Unsanitized user input reaches shell execution.",
		Severity:       "critical",
		Confidence:     "high",
		State:          "validated",
	}))

	token := issueTestContextToken(t, provider, nil, map[string]any{
		"scope": ContextTokenScopeSecurityWrite,
		"tctx": map[string]any{
			"namespace": "demo",
			"repo":      "https://github.com/sozercan/other",
			"branch":    "main",
			"agent":     "demo/patch",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/security/findings/finding-1/pull-request?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	finding, err := handlers.securityStore.GetFinding(ctx, "demo", "finding-1")
	require.NoError(t, err)
	require.Equal(t, "validated", finding.State)
}

func TestCreateManualSecurityScan_ContextTokenStampsTaskRequesterAndTransaction(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")

	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Ref:              "refs/tags/v1.0.0",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)

	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite + " " + ContextTokenScopeConfigMapsRead})
	req := httptest.NewRequest(http.MethodPost, "/security/repositories/scan-1/scans?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var run store.ScanRun
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&run))
	require.NotEmpty(t, run.TaskName)

	task := &corev1alpha1.Task{}
	require.NoError(t, handlers.client.Get(context.Background(), clientObjectKey(run.TaskName), task))
	require.NotNil(t, task.Spec.RequestedBy)
	require.Equal(t, testContextTokenSubject, task.Spec.RequestedBy.Subject)
	require.NotNil(t, task.Spec.Transaction)
	require.Equal(t, testContextTokenTransactionID, task.Spec.Transaction.ID)
	require.Equal(t, labels.SelectorValue(testContextTokenTransactionID), task.Labels[labels.LabelTransactionID])
	require.Equal(t, testContextTokenTransactionID, task.Annotations[labels.AnnotationTransactionID])
	require.Empty(t, task.Spec.Env)
	require.Contains(t, task.Spec.Prompt, `"kind":"`+security.AgentResultKindThreatModel+`"`)
	require.Contains(t, task.Spec.Prompt, `"repositoryScan":"scan-1"`)
	require.Contains(t, task.Spec.Prompt, `"scanId":"`+run.ID+`"`)
	require.Contains(t, task.Spec.Prompt, `"policyDigest":"`+run.PolicyDigest+`"`)
	require.NotNil(t, task.Spec.Workspace)
	require.Empty(t, task.Spec.Workspace.Branch)
	require.Equal(t, "refs/tags/v1.0.0", task.Spec.Workspace.Ref)
}

func TestCreateManualSecurityScanConcurrentRequestsReturnCreatedAndConflict(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "scan-concurrent", Namespace: "demo"},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})

	type response struct {
		status int
		body   string
		err    error
	}
	start := make(chan struct{})
	responses := make(chan response, 2)
	for range 2 {
		go func() {
			<-start
			req := httptest.NewRequest(http.MethodPost, "/security/repositories/scan-concurrent/scans?namespace=demo", nil)
			req.Header.Set(TransactionTokenHeaderName, token)
			resp, err := app.Test(req)
			if err != nil {
				responses <- response{err: err}
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			responses <- response{status: resp.StatusCode, body: string(body), err: readErr}
		}()
	}
	close(start)

	created := 0
	conflicts := 0
	for range 2 {
		result := <-responses
		require.NoError(t, result.err)
		switch result.status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
			require.Contains(t, result.body, "a security scan is already running for this repository")
		default:
			t.Fatalf("concurrent scan status = %d body=%q", result.status, result.body)
		}
	}
	require.Equal(t, 1, created)
	require.Equal(t, 1, conflicts)

	var tasks corev1alpha1.TaskList
	require.NoError(t, handlers.client.List(context.Background(), &tasks, client.InNamespace("demo")))
	require.Len(t, tasks.Items, 1)
	runs, _, err := handlers.securityStore.ListScanRuns(context.Background(), "demo", "scan-concurrent", 10, "")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "pending", runs[0].Phase)
}

func TestCreateManualSecurityScanReleasesAdmissionWhenTaskCreationFails(t *testing.T) {
	provider := newTestOIDCProvider(t)
	ctxTokenConfig := testContextTokenConfig(t, provider, "")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "scan-create-failure", Namespace: "demo"},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			Branch:           "main",
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, ctxTokenConfig, ContextTokenAuthorizationModeEnforce, scan)
	baseClient := handlers.client
	baseWithWatch, ok := baseClient.(client.WithWatch)
	require.True(t, ok)
	handlers.client = interceptor.NewClient(baseWithWatch, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1alpha1.Task); ok {
				return errors.New("injected task create failure")
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})
	req := httptest.NewRequest(http.MethodPost, "/security/repositories/scan-create-failure/scans?namespace=demo", nil)
	req.Header.Set(TransactionTokenHeaderName, token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	ctx := context.Background()
	runs, _, err := handlers.securityStore.ListScanRuns(ctx, "demo", scan.Name, 10, "")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "failed", runs[0].Phase)
	require.NotNil(t, runs[0].CompletedAt)
	require.Equal(t, "scan task creation failed", runs[0].ErrorMessage)
	var tasks corev1alpha1.TaskList
	require.NoError(t, baseClient.List(ctx, &tasks, client.InNamespace("demo")))
	require.Empty(t, tasks.Items)

	require.NoError(t, handlers.securityStore.CreateScanRun(ctx, &store.ScanRun{
		ID:             "scan-create-failure-retry",
		Namespace:      "demo",
		RepositoryScan: scan.Name,
		TaskName:       "scan-create-failure-retry-task",
		Mode:           "manual",
		Phase:          "pending",
		IdempotencyKey: "scanidem:create-failure-retry",
		StartedAt:      time.Now(),
	}))
}

func TestCreateSecurityPullRequestReturnsGovernedReceiptWithoutGitHubMutation(t *testing.T) {
	githubCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		githubCalled = true
		t.Fatalf("receipt lookup must not call the repository forge: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: server.URL,
			Branch:  "main",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan).Build()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	securityStore := sqlite.NewStore(db, ":memory:")

	handlers := NewHandlers(HandlersConfig{
		Client:        fakeClient,
		SecurityStore: securityStore,
	})

	ctx := context.Background()
	prNumber := 99
	prURL := server.URL + "/pull/99"
	require.NoError(t, securityStore.UpsertFinding(ctx, &store.Finding{
		ID:              "finding-1",
		Namespace:       "demo",
		RepositoryScan:  "scan-1",
		ScanRunID:       "scan-run-1",
		Fingerprint:     "fp-1",
		Title:           "Command injection",
		Summary:         "Unsanitized user input reaches shell execution.",
		Severity:        "critical",
		Confidence:      "high",
		State:           "pr_open",
		RootCause:       "Shell command arguments are concatenated directly.",
		Remediation:     "Use argument arrays and validate inputs.",
		PatchProposalID: "patch-1",
		PRNumber:        &prNumber,
		PRURL:           prURL,
	}))
	require.NoError(t, securityStore.CreatePatchProposal(ctx, &store.PatchProposal{
		ID:             "patch-1",
		Namespace:      "demo",
		RepositoryScan: "scan-1",
		FindingID:      "finding-1",
		TaskName:       "patch-task-1",
		Branch:         "orka/security/fnd-123",
		Status:         "pr_opened",
		PRNumber:       &prNumber,
		PRURL:          prURL,
	}))
	require.NoError(t, securityStore.UpsertFinding(ctx, &store.Finding{
		ID:               "finding-alias",
		Namespace:        "demo",
		RepositoryScan:   "scan-1",
		ScanRunID:        "scan-run-2",
		Fingerprint:      "fp-alias",
		Title:            "Command injection alias",
		Summary:          "A duplicate observation of the command injection finding.",
		Severity:         "critical",
		Confidence:       "high",
		State:            "open",
		DuplicateOf:      "finding-1",
		PatchProposalID:  "",
		ValidationStatus: "validated",
	}))

	app := fiber.New()
	app.Post("/security/findings/:id/pull-request", handlers.CreateSecurityPullRequest)

	req := httptest.NewRequest(http.MethodPost, "/security/findings/finding-1/pull-request?namespace=demo", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body struct {
		PRNumber int    `json:"prNumber"`
		PRURL    string `json:"prURL"`
		Status   string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, prNumber, body.PRNumber)
	require.Equal(t, prURL, body.PRURL)
	require.Equal(t, "Open", body.Status)
	require.False(t, githubCalled)

	aliasReq := httptest.NewRequest(http.MethodPost, "/security/findings/finding-alias/pull-request?namespace=demo", nil)
	aliasResp, err := app.Test(aliasReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, aliasResp.StatusCode)
	var aliasBody struct {
		PRNumber int    `json:"prNumber"`
		PRURL    string `json:"prURL"`
		Status   string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(aliasResp.Body).Decode(&aliasBody))
	require.Equal(t, prNumber, aliasBody.PRNumber)
	require.Equal(t, prURL, aliasBody.PRURL)
	require.Equal(t, "Open", aliasBody.Status)

	proposals, err := securityStore.ListPatchProposals(ctx, "demo", "finding-1")
	require.NoError(t, err)
	require.Len(t, proposals, 1)
	require.Equal(t, "pr_opened", proposals[0].Status)
	require.Equal(t, prURL, proposals[0].PRURL)
	require.Equal(t, prNumber, *proposals[0].PRNumber)

	finding, err := securityStore.GetFinding(ctx, "demo", "finding-1")
	require.NoError(t, err)
	require.Equal(t, "pr_open", finding.State)
	require.Equal(t, "patch-1", finding.PatchProposalID)
	require.Equal(t, prURL, finding.PRURL)
	require.Equal(t, prNumber, *finding.PRNumber)

	require.NoError(t, securityStore.UpdateFindingState(ctx, "demo", finding.ID, "resolved"))
	reobserved, err := securityStore.GetFinding(ctx, "demo", finding.ID)
	require.NoError(t, err)
	reobserved.State = "open"
	reobserved.PatchProposalID = ""
	reobserved.PRNumber = nil
	reobserved.PRURL = ""
	require.NoError(t, securityStore.UpsertObservedFinding(ctx, reobserved))

	staleReq := httptest.NewRequest(http.MethodPost, "/security/findings/finding-1/pull-request?namespace=demo", nil)
	staleResp, err := app.Test(staleReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, staleResp.StatusCode)
	reopened, err := securityStore.GetFinding(ctx, "demo", finding.ID)
	require.NoError(t, err)
	require.Equal(t, "open", reopened.State)
	require.Empty(t, reopened.PatchProposalID)
	require.Nil(t, reopened.PRNumber)
	require.Empty(t, reopened.PRURL)
}

func TestCreateSecurityPatchTaskRequestsGovernedPublication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scan-1",
			Namespace: "demo",
		},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:                      securityTestRepoURL,
			ForkRepo:                     securityTestRepoURL,
			Branch:                       "main",
			Ref:                          "f00dbabe",
			PRBaseBranch:                 "main",
			ReadCredentialRef:            &corev1.LocalObjectReference{Name: "source-read"},
			PublicationReadCredentialRef: &corev1.LocalObjectReference{Name: "target-read"},
			PublicationCredentialRef:     &corev1.LocalObjectReference{Name: "target-write"},
			ForgeCredentialRef:           &corev1.LocalObjectReference{Name: "forge"},
			AnalysisAgentRef:             corev1alpha1.AgentReference{Name: "analysis"},
			PatchAgentRef:                &corev1alpha1.AgentReference{Name: "patch"},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan).Build()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	securityStore := sqlite.NewStore(db, ":memory:")

	handlers := NewHandlers(HandlersConfig{
		Client:        fakeClient,
		SecurityStore: securityStore,
	})

	finding := &store.Finding{
		ID:         "fnd_123",
		Namespace:  "demo",
		ScanRunID:  "scan-run-123",
		Title:      "Command injection",
		Severity:   "high",
		Confidence: "high",
	}

	proposal, err := handlers.createSecurityPatchTask(context.Background(), nil, scan, finding)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.Equal(t, "pending", proposal.Status)
	require.NotEmpty(t, proposal.Branch)

	var tasks corev1alpha1.TaskList
	require.NoError(t, fakeClient.List(context.Background(), &tasks, client.InNamespace("demo")))
	require.Len(t, tasks.Items, 1)
	task := tasks.Items[0]
	require.Equal(t, proposal.TaskName, task.Name)
	require.Equal(t, finding.ScanRunID, task.Labels[labels.LabelSecurityScanID])
	require.Equal(t, corev1alpha1.TaskTypeAgent, task.Spec.Type)
	require.Equal(t, "patch", task.Spec.AgentRef.Name)
	require.Empty(t, task.Spec.Env)
	require.Contains(t, task.Spec.Prompt, proposal.Branch)
	require.NotNil(t, task.Spec.Workspace)
	workspace := task.Spec.Workspace
	require.Equal(t, corev1alpha1.WorkspaceIntentWrite, workspace.Intent)
	require.Equal(t, securityTestRepoURL, workspace.GitRepo)
	require.Equal(t, securityTestRepoURL, workspace.PublicationGitRepo)
	require.Equal(t, "main", workspace.Branch)
	require.Equal(t, "f00dbabe", workspace.Ref)
	require.Equal(t, proposal.Branch, workspace.PushBranch)
	require.Equal(t, "main", workspace.PRBaseBranch)
	require.True(t, workspace.CreatePR)
	for name, tc := range map[string]struct {
		ref  *corev1alpha1.WorkspaceCredentialReference
		want string
	}{
		"read":             {ref: workspace.ReadCredentialRef, want: "source-read"},
		"publication read": {ref: workspace.PublicationReadCredentialRef, want: "target-read"},
		"publication":      {ref: workspace.PublicationCredentialRef, want: "target-write"},
		"forge":            {ref: workspace.ForgeCredentialRef, want: "forge"},
	} {
		require.NotNil(t, tc.ref, name)
		require.Equal(t, tc.want, tc.ref.Name, name)
	}
	proposals, err := securityStore.ListPatchProposals(context.Background(), "demo", finding.ID)
	require.NoError(t, err)
	require.Len(t, proposals, 1)
	require.Equal(t, proposal.ID, proposals[0].ID)
	require.Equal(t, proposal.Branch, proposals[0].Branch)
}

func TestCreateSecurityPatchTaskRejectsLegacyCredentialReuse(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "scan-legacy", Namespace: "demo"},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL:          securityTestRepoURL,
			GitSecretRef:     &corev1.LocalObjectReference{Name: "legacy-all-powerful"},
			AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(scan).Build()
	db, err := sqlite.NewDB(":memory:")
	require.NoError(t, err)
	securityStore := sqlite.NewStore(db, ":memory:")
	handlers := NewHandlers(HandlersConfig{Client: fakeClient, SecurityStore: securityStore})
	finding := &store.Finding{ID: "fnd_legacy", Namespace: "demo", Title: "Legacy authority reuse"}

	proposal, err := handlers.createSecurityPatchTask(context.Background(), nil, scan, finding)
	require.Nil(t, proposal)
	var fiberErr *fiber.Error
	require.ErrorAs(t, err, &fiberErr)
	require.Equal(t, http.StatusBadRequest, fiberErr.Code)
	require.Contains(t, fiberErr.Message, "spec.readCredentialRef is required")

	var tasks corev1alpha1.TaskList
	require.NoError(t, fakeClient.List(context.Background(), &tasks, client.InNamespace("demo")))
	require.Empty(t, tasks.Items)
	proposals, err := securityStore.ListPatchProposals(context.Background(), "demo", finding.ID)
	require.NoError(t, err)
	require.Empty(t, proposals)
}

func clientObjectKey(name string) client.ObjectKey {
	return client.ObjectKey{Namespace: "demo", Name: name}
}
