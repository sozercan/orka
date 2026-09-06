package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

func TestExternalAPIScanAdmissionRequiresTaskList(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run("active="+boolString(active), func(t *testing.T) {
			f := newExternalAuthorizationFixture(t)
			scan := &corev1alpha1.RepositoryScan{}
			require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, scan))
			scan.Spec = corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/orka-agents/orka", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "reviewer"}}
			require.NoError(t, f.kube.Update(t.Context(), scan))
			if active {
				require.NoError(t, f.kube.Create(t.Context(), &corev1alpha1.Task{
					ObjectMeta: metav1.ObjectMeta{Name: "active-scan", Namespace: "default", Labels: map[string]string{
						labels.LabelSecurityTarget: labels.SelectorValue("protected"), labels.LabelSecurityStage: security.StageThreatModel,
					}},
					Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
				}))
			}
			list := authorizationv1.ResourceAttributes{Namespace: "default", Group: "core.orka.ai", Resource: "tasks", Verb: "list"}
			permissions := []authorizationv1.ResourceAttributes{
				{Namespace: "default", Group: "core.orka.ai", Resource: "repositoryscans", Subresource: "scans", Verb: "create", Name: "protected"},
				{Namespace: "default", Group: "core.orka.ai", Resource: "tasks", Verb: "create"},
				{Namespace: "default", Group: "core.orka.ai", Resource: "repositoryscans", Subresource: "status", Verb: "patch", Name: "protected"},
				list,
			}
			f.allowOnly(t, permissions[:3]...)
			f.kubeCalls = 0
			before := f.changes(t)
			status, body := f.request(t, http.MethodPost, "/api/v1/security/repositories/protected/scans", "{}")
			require.Equal(t, http.StatusForbidden, status, body)
			require.Zero(t, f.kubeCalls, "scan admission read or changed Kubernetes state without Task list")
			require.Equal(t, before, f.changes(t))
			require.Zero(t, f.externalCalls.Load())
			require.Equal(t, list, *f.reviews[len(f.reviews)-1].ResourceAttributes)
			f.allowOnly(t, permissions...)
			status, body = f.request(t, http.MethodPost, "/api/v1/security/repositories/protected/scans", "{}")
			if active {
				require.Equal(t, http.StatusConflict, status, body)
				require.Equal(t, before, f.changes(t))
			} else {
				require.Equal(t, http.StatusCreated, status, body)
				runs, _, err := f.store.ListScanRuns(t.Context(), "default", "protected", 10, "")
				require.NoError(t, err)
				require.Len(t, runs, 1)
				require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: runs[0].TaskName}, &corev1alpha1.Task{}))
			}
		})
	}
}

func TestExternalAPIScannerPolicyConfigMapReads(t *testing.T) {
	for _, action := range []struct {
		name, method, route, path string
		configuration             bool
		wantStatus                int
	}{
		{"create", http.MethodPost, "POST /api/v1/security/repositories", "/api/v1/security/repositories", true, http.StatusCreated},
		{"update", http.MethodPut, "PUT /api/v1/security/repositories/:name", "/api/v1/security/repositories/protected", true, http.StatusOK},
		{"scan", http.MethodPost, "POST /api/v1/security/repositories/:name/scans", "/api/v1/security/repositories/protected/scans", false, http.StatusCreated},
		{"validate", http.MethodPost, "POST /api/v1/security/findings/:id/validate", "/api/v1/security/findings/protected/validate", false, http.StatusAccepted},
	} {
		for _, deniedName := range []string{"scan-policy", "fp-policy"} {
			t.Run(action.name+"/"+deniedName, func(t *testing.T) {
				f := newExternalAuthorizationFixture(t)
				policies := map[string]string{
					"scan-policy": "Inspect parser bounds.",
					"fp-policy":   "Exclude documented test fixtures.",
				}
				for name, content := range policies {
					require.NoError(t, f.kube.Create(t.Context(), &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{security.PolicyConfigMapAllowedLabel: "true"}},
						Data:       map[string]string{"policy": content},
					}))
				}
				scan := &corev1alpha1.RepositoryScan{}
				require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "protected"}, scan))
				scan.Spec = corev1alpha1.RepositoryScanSpec{
					RepoURL: "https://github.com/orka-agents/orka", Branch: "main", AnalysisAgentRef: corev1alpha1.AgentReference{Name: "reviewer"},
					CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: " scan-policy ", Key: "policy"},
					FalsePositivePolicyRef:    &corev1alpha1.PolicyConfigMapKeyRef{Name: " fp-policy ", Key: "policy"},
				}
				require.NoError(t, f.kube.Update(t.Context(), scan))
				require.NoError(t, f.store.UpsertFinding(t.Context(), &store.Finding{ID: "protected", Namespace: "default", RepositoryScan: "protected", Title: "fixture", State: "open"}))
				policyReads := map[client.ObjectKey]int{}
				f.server.handlers.client = interceptor.NewClient(f.kube.(client.WithWatch), interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*corev1.ConfigMap); ok {
							policyReads[key]++
						}
						return c.Get(ctx, key, obj, opts...)
					},
				})
				requestBody := "{}"
				if action.configuration {
					encoded, err := json.Marshal(CreateRepositoryScanRequest{Name: "created", Spec: scan.Spec})
					require.NoError(t, err)
					requestBody = string(encoded)
				}
				beforeScans, beforeTasks := &corev1alpha1.RepositoryScanList{}, &corev1alpha1.TaskList{}
				require.NoError(t, f.kube.List(t.Context(), beforeScans, client.InNamespace("default")))
				require.NoError(t, f.kube.List(t.Context(), beforeTasks, client.InNamespace("default")))
				beforeStore := f.changes(t)
				permissions := make([]authorizationv1.ResourceAttributes, 0, len(policies))
				for name := range policies {
					if name != deniedName {
						permissions = append(permissions, authorizationv1.ResourceAttributes{Namespace: "default", Resource: "configmaps", Verb: "get", Name: name})
					}
				}
				denied := authorizationv1.ResourceAttributes{Namespace: "default", Resource: "configmaps", Verb: "get", Name: deniedName}
				f.allowRoute(t, action.route, permissions...)
				status, body := f.request(t, action.method, action.path, requestBody)
				require.Equal(t, http.StatusForbidden, status, body)
				require.Zero(t, policyReads[client.ObjectKey{Namespace: "default", Name: deniedName}])
				require.NotEmpty(t, f.reviews)
				require.Equal(t, denied, *f.reviews[len(f.reviews)-1].ResourceAttributes)
				afterScans, afterTasks := &corev1alpha1.RepositoryScanList{}, &corev1alpha1.TaskList{}
				require.NoError(t, f.kube.List(t.Context(), afterScans, client.InNamespace("default")))
				require.NoError(t, f.kube.List(t.Context(), afterTasks, client.InNamespace("default")))
				require.Equal(t, beforeScans.Items, afterScans.Items)
				require.Equal(t, beforeTasks.Items, afterTasks.Items)
				require.Equal(t, beforeStore, f.changes(t))
				require.Zero(t, f.externalCalls.Load())

				f.allowRoute(t, action.route, append(permissions, denied)...)
				status, body = f.request(t, action.method, action.path, requestBody)
				require.Equal(t, action.wantStatus, status, body)
				require.Equal(t, 1, policyReads[client.ObjectKey{Namespace: "default", Name: deniedName}])
				if action.configuration {
					target := "protected"
					if action.name == "create" {
						target = "created"
					}
					persisted := &corev1alpha1.RepositoryScan{}
					require.NoError(t, f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: target}, persisted))
					require.Equal(t, scan.Spec.CustomScanInstructionsRef, persisted.Spec.CustomScanInstructionsRef)
					require.Equal(t, scan.Spec.FalsePositivePolicyRef, persisted.Spec.FalsePositivePolicyRef)
				} else {
					require.NoError(t, f.kube.List(t.Context(), afterTasks, client.InNamespace("default")))
					require.Len(t, afterTasks.Items, len(beforeTasks.Items)+1)
					for _, task := range afterTasks.Items {
						if task.Name == "protected" {
							continue
						}
						for _, content := range policies {
							require.Contains(t, task.Spec.Prompt, content)
						}
					}
				}
			})
		}
	}
}
