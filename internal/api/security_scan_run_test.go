package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
)

func TestCreateManualSecurityScanReplacesStaleRunIdentity(t *testing.T) {
	provider := newTestOIDCProvider(t)
	config := testContextTokenConfig(t, provider, "")
	for _, tt := range []struct {
		name       string
		uid        string
		generation int64
	}{
		{name: "edited", uid: "current-uid", generation: 1},
		{name: "recreated", uid: "previous-uid", generation: 2},
		{name: "legacy"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			scan := &corev1alpha1.RepositoryScan{
				ObjectMeta: metav1.ObjectMeta{Name: "identity-scan", Namespace: "demo", UID: "current-uid", Generation: 2},
				Spec: corev1alpha1.RepositoryScanSpec{
					RepoURL: securityTestRepoURL, AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
				},
				Status: corev1alpha1.RepositoryScanStatus{LastScanID: "scan_old", LastProcessedCommit: "old-base"},
			}
			oldOwner := scan.DeepCopy()
			oldOwner.UID = types.UID(tt.uid)
			oldTask := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name: "old-mapper", Namespace: scan.Namespace,
					Labels: map[string]string{
						labels.LabelSecurityTarget: scan.Name, labels.LabelSecurityScanID: "scan_old", labels.LabelSecurityStage: security.StageMapper,
					},
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(oldOwner, corev1alpha1.GroupVersion.WithKind("RepositoryScan"))},
				},
				Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
			}
			app, handlers := setupSecurityHandlersWithAuthzFixture(t, config, ContextTokenAuthorizationModeEnforce, scan, oldTask)
			require.NoError(t, handlers.securityStore.CreateScanRun(ctx, &store.ScanRun{
				ID: "scan_old", Namespace: scan.Namespace, RepositoryScan: scan.Name,
				RepositoryScanUID: tt.uid, RepositoryScanGeneration: tt.generation, Phase: "running",
			}))
			token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})
			request := httptest.NewRequest(http.MethodPost, "/security/repositories/identity-scan/scans?namespace=demo", nil)
			request.Header.Set(TransactionTokenHeaderName, token)
			response, err := app.Test(request)
			require.NoError(t, err)
			t.Cleanup(func() { _ = response.Body.Close() })
			require.Equal(t, http.StatusCreated, response.StatusCode)
			var run store.ScanRun
			require.NoError(t, json.NewDecoder(response.Body).Decode(&run))
			require.True(t, security.ScanRunMatchesRepositoryScan(&run, scan))
			require.NotEqual(t, "scan_old", run.ID)
			require.Empty(t, run.BaseCommit)
			oldRun, err := handlers.securityStore.GetScanRun(ctx, scan.Namespace, "scan_old")
			require.NoError(t, err)
			require.Equal(t, "failed", oldRun.Phase)
			require.Equal(t, tt.uid, oldRun.RepositoryScanUID)
			current := &corev1alpha1.RepositoryScan{}
			require.NoError(t, handlers.client.Get(ctx, client.ObjectKeyFromObject(scan), current))
			require.Equal(t, run.ID, current.Status.LastScanID)
			require.Empty(t, current.Status.LastProcessedCommit)
			require.Contains(t, current.Finalizers, security.RepositoryScanRunFinalizer)
			task := &corev1alpha1.Task{}
			require.NoError(t, handlers.client.Get(ctx, client.ObjectKey{Namespace: scan.Namespace, Name: run.TaskName}, task))
			require.True(t, metav1.IsControlledBy(task, scan))
			require.Contains(t, task.Spec.Prompt, `"scanId":"`+run.ID+`"`)
		})
	}
}

func TestCreateManualSecurityScanDoesNotProjectStatusOntoEditedScan(t *testing.T) {
	provider := newTestOIDCProvider(t)
	config := testContextTokenConfig(t, provider, "")
	scan := &corev1alpha1.RepositoryScan{
		ObjectMeta: metav1.ObjectMeta{Name: "edited-scan", Namespace: "demo", UID: "scan-uid", Generation: 1},
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: securityTestRepoURL, AnalysisAgentRef: corev1alpha1.AgentReference{Name: "analysis"},
		},
	}
	app, handlers := setupSecurityHandlersWithAuthzFixture(t, config, ContextTokenAuthorizationModeEnforce, scan)
	base, ok := handlers.client.(client.WithWatch)
	require.True(t, ok)
	handlers.client = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if err := c.Create(ctx, object, opts...); err != nil {
				return err
			}
			if _, ok := object.(*corev1alpha1.Task); ok {
				current := &corev1alpha1.RepositoryScan{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(scan), current); err != nil {
					return err
				}
				current.Generation++
				current.Spec.SubPath = "edited"
				return c.Update(ctx, current)
			}
			return nil
		},
	})
	token := issueTestContextToken(t, provider, nil, map[string]any{"scope": ContextTokenScopeSecurityWrite})
	request := httptest.NewRequest(http.MethodPost, "/security/repositories/edited-scan/scans?namespace=demo", nil)
	request.Header.Set(TransactionTokenHeaderName, token)
	response, err := app.Test(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	require.Equal(t, http.StatusConflict, response.StatusCode)
	current := &corev1alpha1.RepositoryScan{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(scan), current))
	require.Equal(t, int64(2), current.Generation)
	require.Empty(t, current.Status.LastScanID)
	require.Empty(t, current.Status.Phase)
}
