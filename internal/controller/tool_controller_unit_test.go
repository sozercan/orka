/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/workspace"
)

const (
	oldMCPActorID        = "orka-tool-old"
	testPooledMCPActorID = "orka-p-pool-00001"
	testMCPPoolName      = "mcp-pool"
	testMCPPath          = "/mcp"
)

func newToolScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = coordinationv1.AddToScheme(s)
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "ate.dev",
		Version: "v1alpha1",
		Kind:    "ActorTemplate",
	}, &unstructured.Unstructured{})
	return s
}

// ---------- validateTool ----------

func TestValidateTool(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "auth-secret", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("t")},
	}

	tests := []struct {
		name      string
		tool      *corev1alpha1.Tool
		objects   []runtime.Object
		wantErr   bool
		errSubstr string
	}{
		{
			name: "valid tool without auth",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api"},
				},
			},
		},
		{
			name: "valid non-credential token-like query parameters",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "token-like-query", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api?max_tokens=10&tokenizer=model&pagination-token=next"},
				},
			},
		},
		{
			name: "missing URL",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t2", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: ""},
				},
			},
			wantErr:   true,
			errSubstr: "http.url is required",
		},
		{
			name: "invalid URL",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t3", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "not-a-url"},
				},
			},
			wantErr:   true,
			errSubstr: "invalid http.url",
		},
		{
			name: "credential query parameter",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "credential-query", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api?api-key=value"},
				},
			},
			wantErr:   true,
			errSubstr: "credential query parameters",
		},
		{
			name: "signed URL query parameter",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "signed-query", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api?sig=credential"},
				},
			},
			wantErr:   true,
			errSubstr: "credential query parameters",
		},
		{
			name: "malformed credential query parameter",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "malformed-query", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api?safe=value;api-key=secret"},
				},
			},
			wantErr:   true,
			errSubstr: "invalid http.url query",
		},
		{
			name: "missing description",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t4", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com/api"},
				},
			},
			wantErr:   true,
			errSubstr: "description is required",
		},
		{
			name: "auth secret not found",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t5", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP: &corev1alpha1.HTTPExecution{
						URL:           "http://example.com/api",
						AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: "missing", Key: "k"},
					},
				},
			},
			wantErr:   true,
			errSubstr: "referenced auth secret",
		},
		{
			name: "auth secret key not found",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t6", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP: &corev1alpha1.HTTPExecution{
						URL:           "http://example.com/api",
						AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: "auth-secret", Key: "wrong-key"},
					},
				},
			},
			wantErr:   true,
			errSubstr: "key \"wrong-key\" not found",
		},
		{
			name: "valid tool with auth secret",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t7", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP: &corev1alpha1.HTTPExecution{
						URL:           "http://example.com/api",
						AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: "auth-secret", Key: "token"},
					},
				},
			},
		},
		{
			name: "authInject body without authBodyKey",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t8", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP: &corev1alpha1.HTTPExecution{
						URL:        "http://example.com/api",
						AuthInject: "body",
					},
				},
			},
			wantErr:   true,
			errSubstr: "authBodyKey is required",
		},
		{
			name: "authInject body with authBodyKey is valid",
			tool: &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "t9", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "A tool",
					HTTP: &corev1alpha1.HTTPExecution{
						URL:         "http://example.com/api",
						AuthInject:  "body",
						AuthBodyKey: "api_key",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newToolScheme()
			cb := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret.DeepCopy())
			r := &ToolReconciler{Client: cb.Build(), Scheme: scheme}

			err := r.validateTool(context.Background(), tt.tool)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errSubstr != "" && !contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errSubstr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestToolReconcilerMCPSubstrateActorPublishesEndpoint(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	var gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: "default"},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreateds: []bool{true, false}}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want finalizer-only first reconcile", executor.claimName, executor.waitReadyCalled)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after finalizer: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not added before actor claim")
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	if executor.claimName != wantActorID || !executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want actor claim and resume", executor.claimName, executor.waitReadyCalled)
	}
	if !executor.waitReadyBoot {
		t.Fatal("WaitReady Boot = false, want true for newly-created MCP actor")
	}
	if !executor.waitReadySkipDaemonHealthCheck {
		t.Fatal("WaitReady SkipDaemonHealthCheck = false, want true for MCP actor readiness")
	}
	if !executor.closeCalled {
		t.Fatal("Substrate MCP executor was not closed after reconcile")
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	wantRouteHost := wantActorID + ".actors.resources.substrate.ate.dev"
	if !got.Status.Available ||
		got.Status.Endpoint != srv.URL+testMCPPath ||
		got.Status.Actor == nil ||
		got.Status.Actor.ActorID != wantActorID ||
		got.Status.Actor.RouteHost != wantRouteHost {
		t.Fatalf("status = %#v, want ready MCP actor endpoint", got.Status)
	}
	if gotHost != wantRouteHost {
		t.Fatalf("MCP readiness Host = %q, want %q", gotHost, wantRouteHost)
	}
}

func TestToolReconcilerMCPSubstrateActorPollsEndpointReadiness(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		if probes.Add(1) == 1 {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreateds: []bool{true, false}}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   time.Second,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() readiness retry error = %v", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if !got.Status.Available {
		t.Fatalf("Available = false, want true after transient MCP readiness failure: %#v", got.Status)
	}
	if probes.Load() < 2 {
		t.Fatalf("MCP endpoint probes = %d, want retry after transient readiness failure", probes.Load())
	}
}

func TestToolReconcilerMCPSubstrateActorBootsRecreatedActor(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	actorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:  actorID,
				substrateMCPToolBootedIDAnno: actorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreated: true}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(executor.waitReadyBoots) != 1 || !executor.waitReadyBoots[0] {
		t.Fatalf("WaitReady boot history = %#v, want [true] for recreated actor", executor.waitReadyBoots)
	}
}

func TestToolReconcilerMCPSubstrateActorRetriesBootAfterWaitReadyFailure(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	actorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: actorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{
		claimCreateds: []bool{true, false},
		waitReadyErrs: []error{errors.New("timed out waiting for actor readiness")},
	}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() wait ready failure error = %v", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after wait ready failure: %v", err)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != "" {
		t.Fatalf("booted actor annotation after wait ready failure = %q, want empty", got.Annotations[substrateMCPToolBootedIDAnno])
	}
	if len(executor.waitReadyBoots) != 1 || !executor.waitReadyBoots[0] {
		t.Fatalf("WaitReady boot history after failure = %#v, want [true]", executor.waitReadyBoots)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() retry error = %v", err)
	}
	if len(executor.waitReadyBoots) != 2 || !executor.waitReadyBoots[0] || !executor.waitReadyBoots[1] {
		t.Fatalf("WaitReady boot history after retry = %#v, want [true true]", executor.waitReadyBoots)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after wait ready retry: %v", err)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != actorID {
		t.Fatalf("booted actor annotation after successful retry = %q, want %q", got.Annotations[substrateMCPToolBootedIDAnno], actorID)
	}
}

func TestToolReconcilerRecordSubstrateMCPBootedRequeuesWhenSpecChanged(t *testing.T) {
	scheme := newToolScheme()
	actorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	local := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS, Generation: 1},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
	}
	latest := local.DeepCopy()
	latest.Generation = 2
	latest.Spec.MCP.Path = "/changed"
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(latest).Build(),
		Scheme: scheme,
	}

	canContinue, err := r.recordSubstrateMCPToolActorBooted(context.Background(), local, actorID)
	if err != nil {
		t.Fatalf("recordSubstrateMCPToolActorBooted() error = %v", err)
	}
	if canContinue {
		t.Fatal("recordSubstrateMCPToolActorBooted() canContinue = true, want false after spec change")
	}
	if local.Annotations[substrateMCPToolBootedIDAnno] != "" {
		t.Fatalf("local booted annotation = %q, want empty after spec change", local.Annotations[substrateMCPToolBootedIDAnno])
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get latest tool: %v", err)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != "" {
		t.Fatalf("latest booted annotation = %q, want empty after spec change", got.Annotations[substrateMCPToolBootedIDAnno])
	}
}

func TestToolReconcilerMCPSubstrateActorSeedsBootedAnnotationWithoutReboot(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	actorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: actorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
		Status: corev1alpha1.ToolStatus{
			Available: true,
			Endpoint:  "http://old-router/mcp",
			Actor: &corev1alpha1.ToolActorStatus{
				Provider:  corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:   actorID,
				RouteHost: actorID + ".actors.resources.substrate.ate.dev",
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(executor.waitReadyBoots) != 1 || executor.waitReadyBoots[0] {
		t.Fatalf("WaitReady boot history = %#v, want [false] for existing actor", executor.waitReadyBoots)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != actorID {
		t.Fatalf("booted actor annotation = %q, want %q", got.Annotations[substrateMCPToolBootedIDAnno], actorID)
	}
}

func TestToolReconcilerMCPSubstrateActorRequiresEndpointReadiness(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: "/missing",
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreateds: []bool{true, false}}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() add finalizer error = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() readiness error = %v", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Status.Available {
		t.Fatal("Available = true, want false while MCP endpoint is not ready")
	}
	if got.Status.Endpoint != srv.URL+"/missing" {
		t.Fatalf("endpoint = %q, want resolved MCP endpoint", got.Status.Endpoint)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID == "" {
		t.Fatalf("actor status = %#v, want preserved actor metadata", got.Status.Actor)
	}
	if !contains(got.Status.Error, "MCP endpoint returned HTTP 404") {
		t.Fatalf("status error = %q, want MCP readiness failure", got.Status.Error)
	}
}

func TestToolReconcilerMCPSubstrateActorReplacementIgnoresNonPooledAnnotationOwnership(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: oldMCPActorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreated: true}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want metadata-only first reconcile", executor.claimName, executor.waitReadyCalled)
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after metadata update: %v", err)
	}
	if got.Annotations[substrateMCPToolActorIDAnno] != wantActorID {
		t.Fatalf("actor ownership annotation = %q, want %q", got.Annotations[substrateMCPToolActorIDAnno], wantActorID)
	}
	if _, ok := got.Annotations[substrateMCPToolCleanupActorIDAnno]; ok {
		t.Fatalf("cleanup annotations = %#v, want no cleanup from untrusted non-pooled annotation", got.Annotations)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() replacement error = %v", err)
	}
	if executor.claimName != wantActorID || !executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want new actor claim", executor.claimName, executor.waitReadyCalled)
	}
	if len(executor.deletedActorIDs) != 0 {
		t.Fatalf("deleted actors = %#v, want no deletion from untrusted non-pooled annotation", executor.deletedActorIDs)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after replacement: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != wantActorID {
		t.Fatalf("status actor = %#v, want replacement actor %q", got.Status.Actor, wantActorID)
	}
	if _, ok := got.Annotations[substrateMCPToolCleanupActorIDAnno]; ok {
		t.Fatalf("cleanup annotations were not cleared after replacement: %#v", got.Annotations)
	}
}

func TestToolReconcilerMCPSubstrateActorReplacementRetriesNonPooledCleanupAfterDeleteFailure(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: oldMCPActorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
		Status: corev1alpha1.ToolStatus{
			Available: true,
			Endpoint:  "http://old-router/mcp",
			Actor: &corev1alpha1.ToolActorStatus{
				Provider:  corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:   oldMCPActorID,
				RouteHost: oldMCPActorID + ".actors.resources.substrate.ate.dev",
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{
		deleteErrs: []error{errors.New("temporary delete failure")},
	}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err == nil {
		t.Fatal("Reconcile() cleanup error = nil, want transient delete failure")
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after failed cleanup: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != oldMCPActorID {
		t.Fatalf("actor status after failed cleanup = %#v, want previous actor", got.Status.Actor)
	}
	if got.Annotations[substrateMCPToolCleanupActorIDAnno] != oldMCPActorID ||
		got.Annotations[substrateMCPToolCleanupPoolNameAnno] != substrateMCPToolCleanupNonPooledValueAnno {
		t.Fatalf("cleanup annotations after failed cleanup = %#v, want old non-pooled actor cleanup", got.Annotations)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() cleanup retry error = %v", err)
	}
	if len(executor.deletedActorIDs) != 2 ||
		executor.deletedActorIDs[0] != oldMCPActorID ||
		executor.deletedActorIDs[1] != oldMCPActorID {
		t.Fatalf("deleted actors = %#v, want two attempts for old actor %q", executor.deletedActorIDs, oldMCPActorID)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after cleanup retry: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != wantActorID {
		t.Fatalf("actor status after cleanup retry = %#v, want replacement actor %q", got.Status.Actor, wantActorID)
	}
	if _, ok := got.Annotations[substrateMCPToolCleanupActorIDAnno]; ok {
		t.Fatalf("cleanup annotations after retry = %#v, want cleared", got.Annotations)
	}
}

func TestToolReconcilerMCPSubstrateActorReplacementDeletesAnnotatedPooledActorBeforeLeaseRelease(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	oldActorID := "orka-p-old-pool-00001"
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            oldActorID,
				substrateMCPToolActorPoolNameAnno:      "old-pool",
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	lease := newSubstrateMCPPoolActorLease(tool, defaultNS, oldActorID, oldActorID)
	executor := &recordingToolWorkspaceExecutor{claimCreateds: []bool{true, false}}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template, lease).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after metadata update: %v", err)
	}
	if got.Annotations[substrateMCPToolActorIDAnno] != wantActorID {
		t.Fatalf("actor ownership annotation = %q, want %q", got.Annotations[substrateMCPToolActorIDAnno], wantActorID)
	}
	if got.Annotations[substrateMCPToolCleanupActorIDAnno] != oldActorID ||
		got.Annotations[substrateMCPToolCleanupPoolNameAnno] != "old-pool" ||
		got.Annotations[substrateMCPToolCleanupPoolNamespaceAnno] != defaultNS {
		t.Fatalf("cleanup annotations = %#v, want old pooled actor cleanup", got.Annotations)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() replacement error = %v", err)
	}
	if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != oldActorID {
		t.Fatalf("deleted actors = %#v, want old pooled actor %q", executor.deletedActorIDs, oldActorID)
	}
	assertToolDeleteRequest(t, executor, oldActorID, "MCP pooled tool actor replaced")
	if err := r.Get(context.Background(), types.NamespacedName{Name: oldActorID, Namespace: defaultNS}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old pooled actor lease error = %v, want not found", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after replacement: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != wantActorID || got.Status.Actor.PoolRef != nil {
		t.Fatalf("status actor = %#v, want non-pooled replacement actor %q", got.Status.Actor, wantActorID)
	}
	if _, ok := got.Annotations[substrateMCPToolCleanupActorIDAnno]; ok {
		t.Fatalf("cleanup annotations were not cleared after replacement: %#v", got.Annotations)
	}
}

func TestToolReconcilerMCPSubstrateActorFailedReplacementPreservesPreviousEndpoint(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: oldMCPActorID,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					Boot:        true,
				},
			},
		},
		Status: corev1alpha1.ToolStatus{
			Available: true,
			Endpoint:  "http://old-router/mcp",
			Actor: &corev1alpha1.ToolActorStatus{
				Provider:  corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:   oldMCPActorID,
				RouteHost: oldMCPActorID + ".actors.resources.substrate.ate.dev",
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreateds: []bool{true, false}}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() failed replacement error = %v", err)
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after failed replacement: %v", err)
	}
	if got.Status.Available {
		t.Fatal("Available = true, want false for failed replacement readiness")
	}
	if got.Status.Endpoint != "http://old-router/mcp" {
		t.Fatalf("endpoint = %q, want previous endpoint", got.Status.Endpoint)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != oldMCPActorID {
		t.Fatalf("actor status = %#v, want previous actor", got.Status.Actor)
	}
	if len(executor.deletedActorIDs) != 0 {
		t.Fatalf("deleted actors = %#v, want no cleanup before replacement readiness", executor.deletedActorIDs)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != wantActorID {
		t.Fatalf("booted actor annotation = %q, want replacement actor %q", got.Annotations[substrateMCPToolBootedIDAnno], wantActorID)
	}
	if len(executor.waitReadyBoots) != 1 || !executor.waitReadyBoots[0] {
		t.Fatalf("WaitReady boot history after first retry = %#v, want [true]", executor.waitReadyBoots)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() repeated failed replacement error = %v", err)
	}
	if len(executor.waitReadyBoots) != 2 || !executor.waitReadyBoots[0] || executor.waitReadyBoots[1] {
		t.Fatalf("WaitReady boot history after second retry = %#v, want [true false]", executor.waitReadyBoots)
	}
}

func TestToolReconcilerMCPSubstrateActorUsesPoolRef(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: testMCPPoolName, Namespace: defaultNS},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors: 5,
		},
	}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
					Boot:        true,
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{claimCreated: true}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template, pool).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want ownership-only first reconcile", executor.claimName, executor.waitReadyCalled)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after ownership update: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not added before pool actor claim")
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() claim error = %v", err)
	}
	prefix := deterministicSubstratePoolActorPrefix(defaultNS, testMCPPoolName)
	if !strings.HasPrefix(executor.claimName, prefix+"-") || !executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want pooled actor claim and resume", executor.claimName, executor.waitReadyCalled)
	}
	if !executor.waitReadyBoot {
		t.Fatal("WaitReady Boot = false, want true for booting pooled MCP actor")
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != executor.claimName {
		t.Fatalf("actor status = %#v, want pooled actor id %q", got.Status.Actor, executor.claimName)
	}
	if got.Status.Actor.PoolRef == nil ||
		got.Status.Actor.PoolRef.Name != testMCPPoolName ||
		got.Status.Actor.PoolRef.Namespace != defaultNS {
		t.Fatalf("actor poolRef = %#v, want default/mcp-pool", got.Status.Actor.PoolRef)
	}
	var gotPool corev1alpha1.SubstrateActorPool
	if err := r.Get(context.Background(), types.NamespacedName{Name: testMCPPoolName, Namespace: defaultNS}, &gotPool); err != nil {
		t.Fatalf("Get pool: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&gotPool, substrateActorPoolFinalizer) {
		t.Fatal("pool finalizer was not persisted during MCP poolRef resolution")
	}
	var gotLease coordinationv1.Lease
	if err := r.Get(context.Background(), types.NamespacedName{Name: executor.claimName, Namespace: defaultNS}, &gotLease); err != nil {
		t.Fatalf("Get pool actor lease: %v", err)
	}
	if !substratePoolActorLeaseHeldByTool(&gotLease, &got) {
		t.Fatalf("lease annotations = %#v, want held by mcp-tool", gotLease.Annotations)
	}

	busy, err := substratePoolActorLeaseHasActiveHolder(context.Background(), r.Client, &gotLease)
	if err != nil {
		t.Fatalf("substratePoolActorLeaseHasActiveHolder() error = %v", err)
	}
	if !busy {
		t.Fatal("MCP tool-held pool actor lease reported idle, want busy")
	}
}

func TestToolReconcilerMCPSubstrateActorWaitsForLegacyTaskCleanup(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: testMCPPoolName, Namespace: defaultNS},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors: 1,
		},
	}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS, UID: types.UID("new-tool-uid")},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
				},
			},
		},
	}
	actorID := deterministicSubstratePoolActorID(deterministicSubstratePoolActorPrefix(defaultNS, testMCPPoolName), 0)
	legacyTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "old-task", Namespace: defaultNS, UID: types.UID("old-task-uid")},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseFailed,
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				Provider:      corev1alpha1.WorkspaceProviderSubstrate,
				CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
				Phase:         corev1alpha1.ExecutionWorkspacePhaseFailed,
				Reason:        corev1alpha1.ExecutionWorkspaceReasonCleanupFailed,
			},
		},
	}
	legacyLease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name:      substratePoolActorLeaseName(actorID),
		Namespace: defaultNS,
		Labels: map[string]string{
			labels.LabelManaged:                   managedLabelValue,
			labels.LabelPurpose:                   substratePoolActorLeasePurpose,
			substratePoolActorLeaseActorIDLabel:   actorID,
			substratePoolActorLeaseHolderUIDLabel: string(legacyTask.UID),
		},
		Annotations: map[string]string{
			legacySubstratePoolActorLeaseTaskNSAnno:   legacyTask.Namespace,
			legacySubstratePoolActorLeaseTaskNameAnno: legacyTask.Name,
			legacySubstratePoolActorLeaseTaskUIDAnno:  string(legacyTask.UID),
		},
	}}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}, &corev1alpha1.Task{}).
			WithObjects(tool, template, pool, legacyTask, legacyLease).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   time.Second,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	result, err := r.Reconcile(ctx, mcpToolRequest())
	if err != nil {
		t.Fatalf("Reconcile() while legacy cleanup failed: %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled || len(executor.deletedActorIDs) != 0 {
		t.Fatalf("executor claimed=%q waited=%t deleted=%v, want legacy actor untouched while cleanup failed", executor.claimName, executor.waitReadyCalled, executor.deletedActorIDs)
	}
	var got corev1alpha1.Tool
	if err := r.Get(ctx, mcpToolRequest().NamespacedName, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Status.Available || !strings.Contains(got.Status.Error, "are leased") || result.RequeueAfter <= 0 {
		t.Fatalf("available=%t error=%q requeueAfter=%v, want unavailable tool retrying a busy pool", got.Status.Available, got.Status.Error, result.RequeueAfter)
	}
	var gotLease coordinationv1.Lease
	leaseKey := types.NamespacedName{Name: legacyLease.Name, Namespace: legacyLease.Namespace}
	if err := r.Get(ctx, leaseKey, &gotLease); err != nil {
		t.Fatalf("Get legacy lease: %v", err)
	}
	if substratePoolActorLeaseHeldByTool(&gotLease, tool) || gotLease.Labels[substratePoolActorLeaseHolderUIDLabel] != string(legacyTask.UID) {
		t.Fatalf("lease = %#v, want legacy Task ownership preserved", gotLease)
	}

	// Once the legacy owner confirms deletion, the Tool may claim a fresh actor.
	if err := r.Get(ctx, types.NamespacedName{Name: legacyTask.Name, Namespace: legacyTask.Namespace}, legacyTask); err != nil {
		t.Fatalf("Get legacy Task: %v", err)
	}
	legacyTask.Status.ExecutionWorkspace.Phase = corev1alpha1.ExecutionWorkspacePhaseDeleted
	legacyTask.Status.ExecutionWorkspace.Reason = corev1alpha1.ExecutionWorkspaceReasonDeleted
	if err := r.Status().Update(ctx, legacyTask); err != nil {
		t.Fatalf("Update legacy cleanup status: %v", err)
	}
	executor.claimCreated = true
	if _, err := r.Reconcile(ctx, mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() after legacy cleanup succeeded: %v", err)
	}
	if executor.claimName != actorID || !executor.waitReadyCalled {
		t.Fatalf("executor claimed=%q waited=%t, want actor %q after cleanup", executor.claimName, executor.waitReadyCalled, actorID)
	}
	if err := r.Get(ctx, mcpToolRequest().NamespacedName, &got); err != nil {
		t.Fatalf("Get tool after cleanup: %v", err)
	}
	if !got.Status.Available || got.Status.Actor == nil || got.Status.Actor.ActorID != actorID {
		t.Fatalf("tool status = %#v, want available Tool using cleaned-up actor", got.Status)
	}
	if err := r.Get(ctx, leaseKey, &gotLease); err != nil {
		t.Fatalf("Get lease after cleanup: %v", err)
	}
	if !substratePoolActorLeaseHeldByTool(&gotLease, &got) {
		t.Fatalf("lease annotations = %#v, want Tool ownership after cleanup", gotLease.Annotations)
	}
}

func TestToolReconcilerMCPSubstrateActorBootsPrecreatedPooledActor(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: testMCPPoolName, Namespace: defaultNS},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors: 5,
		},
	}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
					Boot:        true,
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template, pool).Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want ownership-only first reconcile", executor.claimName, executor.waitReadyCalled)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() claim error = %v", err)
	}
	if len(executor.waitReadyBoots) != 1 || !executor.waitReadyBoots[0] {
		t.Fatalf("WaitReady boot history = %#v, want [true] for first claim of precreated pooled actor", executor.waitReadyBoots)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Annotations[substrateMCPToolBootedIDAnno] != got.Status.Actor.ActorID {
		t.Fatalf("booted actor annotation = %q, want claimed actor %q", got.Annotations[substrateMCPToolBootedIDAnno], got.Status.Actor.ActorID)
	}
}

func TestToolReconcilerMCPSubstrateActorMigratesPooledLeaseOutsideTarget(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: testMCPPoolName, Namespace: defaultNS},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors: 3,
		},
	}
	prefix := deterministicSubstratePoolActorPrefix(defaultNS, testMCPPoolName)
	oldActorID := deterministicSubstratePoolActorID(prefix, 4)
	wantActorID := deterministicSubstratePoolActorID(prefix, deterministicSubstratePoolActorOrdinal(
		pool.Spec.TargetActors,
		prefix,
		defaultNS,
		"mcp-tool",
		"ate-demo",
		"mcp-template",
	))
	if oldActorID == wantActorID {
		t.Fatalf("old actor %q unexpectedly equals in-range actor %q", oldActorID, wantActorID)
	}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            oldActorID,
				substrateMCPToolActorPoolNameAnno:      testMCPPoolName,
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
				},
			},
		},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  oldActorID,
				PoolRef:  &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName, Namespace: defaultNS},
			},
		},
	}
	oldLease := newSubstrateMCPPoolActorLease(tool, defaultNS, oldActorID, oldActorID)
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool, template, pool, oldLease).
			Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() migration metadata error = %v", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after migration metadata: %v", err)
	}
	if got.Annotations[substrateMCPToolActorIDAnno] != wantActorID {
		t.Fatalf("actor annotation = %q, want in-range actor %q", got.Annotations[substrateMCPToolActorIDAnno], wantActorID)
	}
	if got.Annotations[substrateMCPToolCleanupActorIDAnno] != oldActorID ||
		got.Annotations[substrateMCPToolCleanupPoolNameAnno] != testMCPPoolName ||
		got.Annotations[substrateMCPToolCleanupPoolNamespaceAnno] != defaultNS {
		t.Fatalf("cleanup annotations = %#v, want old out-of-range actor cleanup", got.Annotations)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() migration claim error = %v", err)
	}
	if executor.claimName != wantActorID || !executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want in-range actor %q", executor.claimName, executor.waitReadyCalled, wantActorID)
	}
	assertToolDeleteRequest(t, executor, oldActorID, "MCP pooled tool actor replaced")
	if err := r.Get(context.Background(), types.NamespacedName{Name: oldActorID, Namespace: defaultNS}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old out-of-range lease error = %v, want not found", err)
	}
	var gotLease coordinationv1.Lease
	if err := r.Get(context.Background(), types.NamespacedName{Name: wantActorID, Namespace: defaultNS}, &gotLease); err != nil {
		t.Fatalf("Get new in-range lease: %v", err)
	}
	if !substratePoolActorLeaseHeldByTool(&gotLease, &got) {
		t.Fatalf("new lease annotations = %#v, want held by mcp-tool", gotLease.Annotations)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after migration: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != wantActorID {
		t.Fatalf("status actor = %#v, want migrated actor %q", got.Status.Actor, wantActorID)
	}
	if _, ok := got.Annotations[substrateMCPToolCleanupActorIDAnno]; ok {
		t.Fatalf("cleanup annotations after migration = %#v, want cleared", got.Annotations)
	}
}

func TestToolReconcilerMCPSubstrateActorProbesPoolOnLeaseCollision(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testMCPPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := &corev1alpha1.SubstrateActorPool{
		ObjectMeta: metav1.ObjectMeta{Name: testMCPPoolName, Namespace: defaultNS},
		Spec: corev1alpha1.SubstrateActorPoolSpec{
			TemplateRef:  corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
			TargetActors: 3,
		},
	}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: defaultNS, UID: "mcp-tool-uid"},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: testMCPPath,
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
				},
			},
		},
	}
	prefix := deterministicSubstratePoolActorPrefix(defaultNS, testMCPPoolName)
	startOrdinal := deterministicSubstratePoolActorOrdinal(
		pool.Spec.TargetActors,
		prefix,
		tool.Namespace,
		tool.Name,
		"ate-demo",
		"mcp-template",
	)
	startActorID := deterministicSubstratePoolActorID(prefix, startOrdinal)
	wantActorID := deterministicSubstratePoolActorID(prefix, (startOrdinal+1)%int(pool.Spec.TargetActors))

	holder := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "other-tool", Namespace: defaultNS, UID: "other-tool-uid"},
		Spec:       corev1alpha1.ToolSpec{Description: "other MCP tool"},
	}
	busyLease := newSubstrateMCPPoolActorLease(holder, defaultNS, startActorID, startActorID)
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool, template, pool, holder, busyLease).
			Build(),
		Scheme:           scheme,
		HTTPClient:       srv.Client(),
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      srv.URL,
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() ownership update error = %v", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after ownership update: %v", err)
	}
	if got.Annotations[substrateMCPToolActorIDAnno] != startActorID {
		t.Fatalf("actor annotation = %q, want hashed actor %q", got.Annotations[substrateMCPToolActorIDAnno], startActorID)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() collision probe error = %v", err)
	}
	if executor.claimName != "" || executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want lease-only probe before claim", executor.claimName, executor.waitReadyCalled)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after probe: %v", err)
	}
	if got.Annotations[substrateMCPToolActorIDAnno] != wantActorID {
		t.Fatalf("actor annotation after probe = %q, want %q", got.Annotations[substrateMCPToolActorIDAnno], wantActorID)
	}
	var gotLease coordinationv1.Lease
	if err := r.Get(context.Background(), types.NamespacedName{Name: wantActorID, Namespace: defaultNS}, &gotLease); err != nil {
		t.Fatalf("Get probed pool actor lease: %v", err)
	}
	if !substratePoolActorLeaseHeldByTool(&gotLease, &got) {
		t.Fatalf("probed lease annotations = %#v, want held by mcp-tool", gotLease.Annotations)
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() claim error = %v", err)
	}
	if executor.claimName != wantActorID || !executor.waitReadyCalled {
		t.Fatalf("executor claimName=%q waitReady=%t, want pinned actor %q", executor.claimName, executor.waitReadyCalled, wantActorID)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after claim: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != wantActorID {
		t.Fatalf("status actor = %#v, want %q", got.Status.Actor, wantActorID)
	}
	if got.Status.Actor.PoolRef == nil ||
		got.Status.Actor.PoolRef.Name != testMCPPoolName ||
		got.Status.Actor.PoolRef.Namespace != defaultNS {
		t.Fatalf("status poolRef = %#v, want default/mcp-pool", got.Status.Actor.PoolRef)
	}
}

func TestToolReconcilerFinalizesNonPooledMCPSubstrateActor(t *testing.T) {
	scheme := newToolScheme()
	tests := []struct {
		name           string
		actor          *corev1alpha1.ToolActorStatus
		wantDeleteID   string
		wantReason     string
		wantFactoryHit bool
	}{
		{
			name: "non-pooled actor",
			actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  oldMCPActorID,
			},
			wantDeleteID:   oldMCPActorID,
			wantReason:     "MCP tool deleted",
			wantFactoryHit: true,
		},
		{
			name: "pooled actor without lease",
			actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  testPooledMCPActorID,
				PoolRef:  &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName, Namespace: defaultNS},
			},
			wantDeleteID:   "",
			wantReason:     "",
			wantFactoryHit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "mcp-tool",
					Namespace:  defaultNS,
					Finalizers: []string{substrateMCPToolActorFinalizer},
				},
				Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
				Status: corev1alpha1.ToolStatus{
					Actor: tt.actor,
				},
			}
			executor := &recordingToolWorkspaceExecutor{}
			var factoryHit bool
			r := &ToolReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithStatusSubresource(&corev1alpha1.Tool{}).
					WithObjects(tool).
					Build(),
				Scheme: scheme,
				SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
					factoryHit = true
					return executor, nil
				},
			}

			if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
				t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
			}
			if factoryHit != tt.wantFactoryHit {
				t.Fatalf("executor factory called=%t, want %t", factoryHit, tt.wantFactoryHit)
			}
			if tt.wantDeleteID == "" {
				if len(executor.deletedActorIDs) != 0 {
					t.Fatalf("deleted actors = %#v, want none", executor.deletedActorIDs)
				}
			} else if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != tt.wantDeleteID {
				t.Fatalf("deleted actors = %#v, want [%s]", executor.deletedActorIDs, tt.wantDeleteID)
			} else {
				assertToolDeleteRequest(t, executor, tt.wantDeleteID, tt.wantReason)
			}
			var got corev1alpha1.Tool
			if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
				t.Fatalf("Get tool: %v", err)
			}
			if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
				t.Fatal("MCP tool actor finalizer was not removed")
			}
		})
	}
}

func TestToolReconcilerFinalizerIgnoresUntrustedNonPooledMCPActorAnnotation(t *testing.T) {
	scheme := newToolScheme()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno: "victim-actor",
			},
		},
		Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
	}
	executor := &recordingToolWorkspaceExecutor{}
	var factoryHit bool
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			factoryHit = true
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	if factoryHit {
		t.Fatal("executor factory was called for untrusted non-pooled annotation")
	}
	if len(executor.deletedActorIDs) != 0 {
		t.Fatalf("deleted actors = %#v, want no deletion from untrusted non-pooled annotation", executor.deletedActorIDs)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
	if got.Annotations != nil {
		t.Fatalf("annotations = %#v, want untrusted actor annotation cleared", got.Annotations)
	}
}

func TestToolReconcilerFinalizesMCPReplacementWithStaleStatusDeletesOwnedActor(t *testing.T) {
	scheme := newToolScheme()
	newActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:         newActorID,
				substrateMCPToolCleanupActorIDAnno:  oldMCPActorID,
				substrateMCPToolCleanupPoolNameAnno: substrateMCPToolCleanupNonPooledValueAnno,
			},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  oldMCPActorID,
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	assertToolDeleteRequests(t, executor, []toolDeleteRequestExpectation{
		{actorID: oldMCPActorID, reason: "MCP tool deleted"},
		{actorID: newActorID, reason: "MCP tool deleted"},
	})
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
}

func TestToolReconcilerFinalizesPooledMCPSubstrateActorDeletesActorBeforeLeaseRelease(t *testing.T) {
	scheme := newToolScheme()
	actorID := testPooledMCPActorID
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            actorID,
				substrateMCPToolActorPoolNameAnno:      testMCPPoolName,
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  actorID,
				PoolRef:  &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName, Namespace: defaultNS},
			},
		},
	}
	lease := newSubstrateMCPPoolActorLease(tool, defaultNS, actorID, actorID)
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool, lease).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != actorID {
		t.Fatalf("deleted actors = %#v, want pooled actor %q", executor.deletedActorIDs, actorID)
	}
	assertToolDeleteRequest(t, executor, actorID, "MCP pooled tool actor deleted")
	if err := r.Get(context.Background(), types.NamespacedName{Name: actorID, Namespace: defaultNS}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("pool actor lease error = %v, want not found", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
}

func TestToolReconcilerFinalizesAnnotatedPooledMCPSubstrateActorWithoutStatus(t *testing.T) {
	scheme := newToolScheme()
	actorID := testPooledMCPActorID
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            actorID,
				substrateMCPToolActorPoolNameAnno:      testMCPPoolName,
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
	}
	lease := newSubstrateMCPPoolActorLease(tool, defaultNS, actorID, actorID)
	executor := &recordingToolWorkspaceExecutor{}
	var factoryHit bool
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool, lease).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			factoryHit = true
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	if !factoryHit {
		t.Fatal("executor factory was not called for annotated pooled actor cleanup")
	}
	if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != actorID {
		t.Fatalf("deleted actors = %#v, want pooled actor %q", executor.deletedActorIDs, actorID)
	}
	assertToolDeleteRequest(t, executor, actorID, "MCP pooled tool actor deleted")
	if err := r.Get(context.Background(), types.NamespacedName{Name: actorID, Namespace: defaultNS}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("pool actor lease error = %v, want not found", err)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
}

func TestToolReconcilerFinalizerIgnoresAnnotatedPooledMCPActorWithoutLease(t *testing.T) {
	scheme := newToolScheme()
	actorID := testPooledMCPActorID
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            actorID,
				substrateMCPToolActorPoolNameAnno:      testMCPPoolName,
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
	}
	executor := &recordingToolWorkspaceExecutor{}
	var factoryHit bool
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			factoryHit = true
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	if factoryHit {
		t.Fatal("executor factory was called for stale annotated pooled actor")
	}
	if len(executor.deletedActorIDs) != 0 {
		t.Fatalf("deleted actors = %#v, want no deletion without Tool-held lease", executor.deletedActorIDs)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
	if got.Annotations != nil {
		t.Fatalf("annotations = %#v, want untrusted pooled actor annotations cleared", got.Annotations)
	}
}

func TestToolReconcilerFinalizerIgnoresPooledMCPActorHeldByAnotherTool(t *testing.T) {
	scheme := newToolScheme()
	actorID := testPooledMCPActorID
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			UID:        "mcp-tool-uid",
			Finalizers: []string{substrateMCPToolActorFinalizer},
			Annotations: map[string]string{
				substrateMCPToolActorIDAnno:            actorID,
				substrateMCPToolActorPoolNameAnno:      testMCPPoolName,
				substrateMCPToolActorPoolNamespaceAnno: defaultNS,
			},
		},
		Spec: corev1alpha1.ToolSpec{Description: "MCP tool"},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  actorID,
				PoolRef:  &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName, Namespace: defaultNS},
			},
		},
	}
	otherTool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "other-tool", Namespace: defaultNS, UID: "other-tool-uid"},
		Spec:       corev1alpha1.ToolSpec{Description: "Other MCP tool"},
	}
	lease := newSubstrateMCPPoolActorLease(otherTool, defaultNS, actorID, actorID)
	executor := &recordingToolWorkspaceExecutor{}
	var factoryHit bool
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool, otherTool, lease).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			factoryHit = true
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	if factoryHit {
		t.Fatal("executor factory was called for pooled actor held by another Tool")
	}
	if len(executor.deletedActorIDs) != 0 {
		t.Fatalf("deleted actors = %#v, want no deletion without Tool-held lease", executor.deletedActorIDs)
	}
	var gotLease coordinationv1.Lease
	if err := r.Get(context.Background(), types.NamespacedName{Name: actorID, Namespace: defaultNS}, &gotLease); err != nil {
		t.Fatalf("Get lease: %v", err)
	}
	if !substratePoolActorLeaseHeldByTool(&gotLease, otherTool) {
		t.Fatalf("lease annotations = %#v, want held by other Tool", gotLease.Annotations)
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
	if got.Annotations != nil {
		t.Fatalf("annotations = %#v, want stale pooled actor annotations cleared", got.Annotations)
	}
}

func TestToolReconcilerFinalizesMCPActorFromSpecWhenStatusMissing(t *testing.T) {
	scheme := newToolScheme()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.finalizeSubstrateMCPTool(context.Background(), tool); err != nil {
		t.Fatalf("finalizeSubstrateMCPTool() error = %v", err)
	}
	wantActorID := deterministicSubstrateToolActorID(defaultNS, "mcp-tool", "ate-demo", "mcp-template")
	if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != wantActorID {
		t.Fatalf("deleted actors = %#v, want deterministic actor %q", executor.deletedActorIDs, wantActorID)
	}
	assertToolDeleteRequest(t, executor, wantActorID, "MCP tool deleted")
}

func TestToolReconcilerFinalizesMCPActorWhenToolBecomesHTTP(t *testing.T) {
	scheme := newToolScheme()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mcp-tool",
			Namespace:  defaultNS,
			Finalizers: []string{substrateMCPToolActorFinalizer},
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "HTTP tool",
			HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com"},
		},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  oldMCPActorID,
			},
		},
	}
	executor := &recordingToolWorkspaceExecutor{}
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1alpha1.Tool{}).
			WithObjects(tool).
			Build(),
		Scheme: scheme,
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			return executor, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(executor.deletedActorIDs) != 1 || executor.deletedActorIDs[0] != oldMCPActorID {
		t.Fatalf("deleted actors = %#v, want old MCP actor deleted", executor.deletedActorIDs)
	}
	assertToolDeleteRequest(t, executor, oldMCPActorID, "MCP tool deleted")
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if controllerutil.ContainsFinalizer(&got, substrateMCPToolActorFinalizer) {
		t.Fatal("MCP tool actor finalizer was not removed")
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsCrossNamespaceTemplateWhenIsolationEnforced(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: "default"},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: "/mcp",
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
					PoolRef:     &corev1alpha1.SubstrateActorPoolReference{Name: testMCPPoolName},
				},
			},
		},
	}
	r := &ToolReconciler{
		Client:                    fake.NewClientBuilder().WithScheme(scheme).WithObjects(tool, template).Build(),
		Scheme:                    scheme,
		SubstrateEnabled:          true,
		EnforceNamespaceIsolation: true,
	}

	err := r.validateTool(context.Background(), tool)
	if err == nil {
		t.Fatal("validateTool() error = nil, want cross-namespace templateRef rejection")
	}
	if !contains(err.Error(), "cross-namespace MCP substrate actor templateRef not allowed") {
		t.Fatalf("validateTool() error = %q, want cross-namespace templateRef rejection", err.Error())
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsUnapprovedTemplate(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	labels := template.GetLabels()
	delete(labels, "orka.ai/execution-workspace")
	template.SetLabels(labels)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: "default"},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			MCP: &corev1alpha1.MCPToolServer{
				Path: "/mcp",
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	var executorFactoryCalled bool
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      "http://atenet-router.ate-system.svc",
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			executorFactoryCalled = true
			return &recordingToolWorkspaceExecutor{}, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executorFactoryCalled {
		t.Fatal("executor factory called before ActorTemplate approval validation")
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: "default"}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Status.Available {
		t.Fatalf("Available = true, want false for unapproved template")
	}
	if !contains(got.Status.Error, "missing label orka.ai/execution-workspace=true") {
		t.Fatalf("status error = %q, want missing approval label", got.Status.Error)
	}
}

func TestToolReconcilerMCPSubstrateActorAllowsMCPOnlyTemplate(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	if err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request); err != nil {
		t.Fatalf("validateSubstrateMCPActorTemplateResource() error = %v", err)
	}
	if err := validateSubstrateActorTemplateResource(context.Background(), k8sClient, request); err == nil {
		t.Fatal("validateSubstrateActorTemplateResource() error = nil, want workspace template validation to reject MCP-only template")
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsRoutePortMismatch(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "80"
	template.SetAnnotations(annotations)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request)
	if err == nil {
		t.Fatal("validateSubstrateMCPActorTemplateResource() error = nil, want route port mismatch")
	}
	if !contains(err.Error(), "requires a matching containerPort") {
		t.Fatalf("error = %q, want matching containerPort context", err.Error())
	}
}

func TestToolReconcilerMCPSubstrateActorAcceptsExplicitRoutePort(t *testing.T) {
	scheme := newToolScheme()
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "mcp",
			"command": []any{"/mcp-server"},
			"ports": []any{
				map[string]any{"containerPort": int64(9090)},
			},
		},
	})
	template.SetName("mcp-template")
	template.SetNamespace("ate-demo")
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "9090"
	delete(annotations, "orka.ai/workspace-staging-root")
	template.SetAnnotations(annotations)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	if err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request); err != nil {
		t.Fatalf("validateSubstrateMCPActorTemplateResource() error = %v", err)
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsListenEnvRoutePortMismatch(t *testing.T) {
	scheme := newToolScheme()
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "mcp",
			"command": []any{"/mcp-server"},
			"env": []any{
				map[string]any{
					"name":  substrateWorkspaceDaemonListenEnv,
					"value": ":9090",
				},
			},
		},
	})
	template.SetName("mcp-template")
	template.SetNamespace("ate-demo")
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "8080"
	delete(annotations, "orka.ai/workspace-staging-root")
	template.SetAnnotations(annotations)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request)
	if err == nil {
		t.Fatal("validateSubstrateMCPActorTemplateResource() error = nil, want listen env route port mismatch")
	}
	if !contains(err.Error(), "no container listen env matches") {
		t.Fatalf("error = %q, want listen env mismatch context", err.Error())
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsListenEnvMismatchDespiteMatchingContainerPort(t *testing.T) {
	scheme := newToolScheme()
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "mcp",
			"command": []any{"/mcp-server"},
			"env": []any{
				map[string]any{
					"name":  substrateWorkspaceDaemonListenEnv,
					"value": ":9090",
				},
			},
			"ports": []any{
				map[string]any{"containerPort": int64(8080)},
			},
		},
	})
	template.SetName("mcp-template")
	template.SetNamespace("ate-demo")
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "8080"
	delete(annotations, "orka.ai/workspace-staging-root")
	template.SetAnnotations(annotations)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request)
	if err == nil {
		t.Fatal("validateSubstrateMCPActorTemplateResource() error = nil, want listen env mismatch despite matching containerPort")
	}
	if !contains(err.Error(), "no container listen env matches") {
		t.Fatalf("error = %q, want listen env mismatch context", err.Error())
	}
}

func TestToolReconcilerMCPSubstrateActorAcceptsExplicitListenEnvRoutePort(t *testing.T) {
	scheme := newToolScheme()
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "mcp",
			"command": []any{"/mcp-server"},
			"env": []any{
				map[string]any{
					"name":  substrateWorkspaceDaemonListenEnv,
					"value": ":9090",
				},
			},
		},
	})
	template.SetName("mcp-template")
	template.SetNamespace("ate-demo")
	annotations := template.GetAnnotations()
	annotations["orka.ai/workspace-daemon-port"] = "9090"
	delete(annotations, "orka.ai/workspace-staging-root")
	template.SetAnnotations(annotations)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(template).Build()
	request := &ExecutionWorkspaceRequest{
		TemplateName:      "mcp-template",
		TemplateNamespace: "ate-demo",
	}

	if err := validateSubstrateMCPActorTemplateResource(context.Background(), k8sClient, request); err != nil {
		t.Fatalf("validateSubstrateMCPActorTemplateResource() error = %v", err)
	}
}

func TestToolReconcilerMCPSubstrateActorRejectsInvalidAuthConfig(t *testing.T) {
	scheme := newToolScheme()
	template := approvedMCPActorTemplateForTest()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-tool", Namespace: "default"},
		Spec: corev1alpha1.ToolSpec{
			Description: "MCP tool",
			HTTP: &corev1alpha1.HTTPExecution{
				AuthInject: "body",
			},
			MCP: &corev1alpha1.MCPToolServer{
				Path: "/mcp",
				SubstrateActor: &corev1alpha1.SubstrateMCPActor{
					TemplateRef: corev1alpha1.WorkspaceTemplateReference{Name: "mcp-template", Namespace: "ate-demo"},
				},
			},
		},
	}
	var executorFactoryCalled bool
	r := &ToolReconciler{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1alpha1.Tool{}).WithObjects(tool, template).Build(),
		Scheme:           scheme,
		SubstrateEnabled: true,
		SubstrateConfig: SubstrateConfig{
			RouterURL:      "http://atenet-router.ate-system.svc",
			ActorDNSSuffix: "actors.resources.substrate.ate.dev",
			ClaimTimeout:   1,
		},
		SubstrateExecutorFactory: func(SubstrateConfig) (workspace.WorkspaceExecutor, error) {
			executorFactoryCalled = true
			return &recordingToolWorkspaceExecutor{}, nil
		},
	}

	if _, err := r.Reconcile(context.Background(), mcpToolRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if executorFactoryCalled {
		t.Fatal("executor factory called before MCP auth validation")
	}
	var got corev1alpha1.Tool
	if err := r.Get(context.Background(), types.NamespacedName{Name: "mcp-tool", Namespace: "default"}, &got); err != nil {
		t.Fatalf("Get tool: %v", err)
	}
	if got.Status.Available {
		t.Fatalf("Available = true, want false for invalid auth config")
	}
	if !contains(got.Status.Error, "authBodyKey is required") {
		t.Fatalf("status error = %q, want authBodyKey validation error", got.Status.Error)
	}
}

func approvedMCPActorTemplateForTest() *unstructured.Unstructured {
	template := readySubstrateActorTemplateWithContainersForTest([]any{
		map[string]any{
			"name":    "mcp",
			"command": []any{"/mcp-server"},
		},
	})
	template.SetName("mcp-template")
	template.SetNamespace("ate-demo")
	annotations := template.GetAnnotations()
	delete(annotations, "orka.ai/workspace-staging-root")
	template.SetAnnotations(annotations)
	return template
}

// ---------- healthCheck ----------

func TestHealthCheck(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr bool
	}{
		{
			name:    "reachable 200",
			handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
		},
		{
			name:    "reachable 500 still succeeds",
			handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
		},
		{
			name:    "reachable 404 still succeeds",
			handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			scheme := newToolScheme()
			r := &ToolReconciler{
				Client:     fake.NewClientBuilder().WithScheme(scheme).Build(),
				Scheme:     scheme,
				HTTPClient: srv.Client(),
			}

			tool := &corev1alpha1.Tool{
				Spec: corev1alpha1.ToolSpec{
					HTTP: &corev1alpha1.HTTPExecution{URL: srv.URL},
				},
			}

			err := r.healthCheck(context.Background(), tool)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestHealthCheck_Unreachable(t *testing.T) {
	scheme := newToolScheme()
	r := &ToolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
		HTTPClient: &http.Client{
			Transport: &http.Transport{},
		},
	}
	tool := &corev1alpha1.Tool{
		Spec: corev1alpha1.ToolSpec{
			HTTP: &corev1alpha1.HTTPExecution{URL: "http://127.0.0.1:1/unreachable"},
		},
	}

	err := r.healthCheck(context.Background(), tool)
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
	if !contains(err.Error(), "endpoint unreachable") {
		t.Errorf("error %q should contain 'endpoint unreachable'", err.Error())
	}
}

// ---------- getHTTPClient ----------

func TestGetHTTPClient(t *testing.T) {
	t.Run("returns injected client", func(t *testing.T) {
		custom := &http.Client{}
		r := &ToolReconciler{HTTPClient: custom}
		got := r.getHTTPClient()
		if got != custom {
			t.Error("expected injected client to be returned")
		}
	})

	t.Run("returns default client when nil", func(t *testing.T) {
		r := &ToolReconciler{}
		got := r.getHTTPClient()
		if got == nil {
			t.Fatal("expected non-nil default client")
		}
		if got.Timeout != toolHealthCheckTimeout {
			t.Errorf("timeout = %v, want %v", got.Timeout, toolHealthCheckTimeout)
		}
	})
}

// ---------- updateStatus ----------

func TestToolUpdateStatus(t *testing.T) {
	tests := []struct {
		name       string
		available  bool
		errMsg     string
		wantReason string
	}{
		{"available", true, "", "EndpointReachable"},
		{"unavailable", false, "endpoint unreachable", "EndpointUnreachable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newToolScheme()
			tool := &corev1alpha1.Tool{
				ObjectMeta: metav1.ObjectMeta{Name: "st", Namespace: "default"},
				Spec: corev1alpha1.ToolSpec{
					Description: "test",
					HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com"},
				},
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(tool).
				WithStatusSubresource(&corev1alpha1.Tool{}).Build()
			r := &ToolReconciler{Client: cl, Scheme: scheme}

			result, err := r.updateStatus(context.Background(), tool, tt.available, tt.errMsg)
			if err != nil {
				t.Fatalf("updateStatus error: %v", err)
			}
			if result.RequeueAfter != toolHealthCheckInterval {
				t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, toolHealthCheckInterval)
			}

			got := &corev1alpha1.Tool{}
			if err := cl.Get(context.Background(), types.NamespacedName{Name: "st", Namespace: "default"}, got); err != nil {
				t.Fatalf("get error: %v", err)
			}
			if got.Status.Available != tt.available {
				t.Errorf("Available = %v, want %v", got.Status.Available, tt.available)
			}
			if got.Status.Error != tt.errMsg {
				t.Errorf("Error = %q, want %q", got.Status.Error, tt.errMsg)
			}
			if got.Status.LastCheck == nil {
				t.Error("LastCheck should be set")
			}
			if len(got.Status.Conditions) == 0 {
				t.Fatal("expected at least one condition")
			}
			if got.Status.Conditions[0].Reason != tt.wantReason {
				t.Errorf("condition reason = %q, want %q", got.Status.Conditions[0].Reason, tt.wantReason)
			}
		})
	}
}

func TestToolUpdateStatusPreservesActorOnFailure(t *testing.T) {
	scheme := newToolScheme()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "st", Namespace: defaultNS},
		Spec: corev1alpha1.ToolSpec{
			Description: "test",
			HTTP:        &corev1alpha1.HTTPExecution{URL: "http://example.com"},
		},
		Status: corev1alpha1.ToolStatus{
			Actor: &corev1alpha1.ToolActorStatus{
				Provider: corev1alpha1.WorkspaceProviderSubstrate,
				ActorID:  oldMCPActorID,
			},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tool).
		WithStatusSubresource(&corev1alpha1.Tool{}).
		Build()
	r := &ToolReconciler{Client: cl, Scheme: scheme}

	if _, err := r.updateStatus(context.Background(), tool, false, "temporary failure"); err != nil {
		t.Fatalf("updateStatus failure error: %v", err)
	}
	var got corev1alpha1.Tool
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "st", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after failure: %v", err)
	}
	if got.Status.Actor == nil || got.Status.Actor.ActorID != oldMCPActorID {
		t.Fatalf("actor after failure = %#v, want preserved actor", got.Status.Actor)
	}

	if _, err := r.updateStatus(context.Background(), &got, true, ""); err != nil {
		t.Fatalf("updateStatus success error: %v", err)
	}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "st", Namespace: defaultNS}, &got); err != nil {
		t.Fatalf("Get tool after success: %v", err)
	}
	if got.Status.Actor != nil {
		t.Fatalf("actor after success = %#v, want cleared actor for non-actor status", got.Status.Actor)
	}
}

func mcpToolRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: defaultNS, Name: "mcp-tool"}}
}

type recordingToolWorkspaceExecutor struct {
	claimName                      string
	claimCreated                   bool
	claimCreateds                  []bool
	waitReadyCalled                bool
	waitReadyBoot                  bool
	waitReadyBoots                 []bool
	waitReadySkipDaemonHealthCheck bool
	waitReadyErrs                  []error
	closeCalled                    bool
	deletedActorIDs                []string
	deleteReqs                     []workspace.DeleteRequest
	deleteErrs                     []error
}

func (e *recordingToolWorkspaceExecutor) Claim(ctx context.Context, req workspace.ClaimRequest) (*workspace.ClaimResult, error) {
	e.claimName = req.ClaimName
	created := e.claimCreated
	if len(e.claimCreateds) > 0 {
		created = e.claimCreateds[0]
		e.claimCreateds = e.claimCreateds[1:]
	}
	return &workspace.ClaimResult{
		Ref: workspace.WorkspaceRef{
			Namespace: req.Namespace,
			ClaimName: req.ClaimName,
			ID:        req.ClaimName,
		},
		Template: req.Template,
		Created:  created,
		Phase:    workspace.PhasePending,
	}, nil
}

func (e *recordingToolWorkspaceExecutor) WaitReady(ctx context.Context, req workspace.WaitReadyRequest) (*workspace.ReadyResult, error) {
	e.waitReadyCalled = true
	e.waitReadyBoot = req.Boot
	e.waitReadyBoots = append(e.waitReadyBoots, req.Boot)
	e.waitReadySkipDaemonHealthCheck = req.SkipDaemonHealthCheck
	if len(e.waitReadyErrs) > 0 {
		err := e.waitReadyErrs[0]
		e.waitReadyErrs = e.waitReadyErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &workspace.ReadyResult{Ref: req.Ref, Phase: workspace.PhaseReady}, nil
}

func (e *recordingToolWorkspaceExecutor) Exec(ctx context.Context, req workspace.ExecRequest) (*workspace.ExecResult, error) {
	return &workspace.ExecResult{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Upload(ctx context.Context, req workspace.UploadRequest) (*workspace.UploadResult, error) {
	return &workspace.UploadResult{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Download(ctx context.Context, req workspace.DownloadRequest) (*workspace.DownloadResult, error) {
	return &workspace.DownloadResult{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Release(ctx context.Context, req workspace.ReleaseRequest) (*workspace.ReleaseResult, error) {
	return &workspace.ReleaseResult{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Delete(ctx context.Context, req workspace.DeleteRequest) (*workspace.DeleteResult, error) {
	e.deletedActorIDs = append(e.deletedActorIDs, req.Ref.ID)
	e.deleteReqs = append(e.deleteReqs, req)
	if len(e.deleteErrs) > 0 {
		err := e.deleteErrs[0]
		e.deleteErrs = e.deleteErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &workspace.DeleteResult{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Describe(ctx context.Context, req workspace.DescribeRequest) (*workspace.Description, error) {
	return &workspace.Description{Ref: req.Ref}, nil
}

func (e *recordingToolWorkspaceExecutor) Close() error {
	e.closeCalled = true
	return nil
}

func assertToolDeleteRequest(t *testing.T, executor *recordingToolWorkspaceExecutor, actorID, reason string) {
	t.Helper()
	assertToolDeleteRequests(t, executor, []toolDeleteRequestExpectation{{actorID: actorID, reason: reason}})
}

type toolDeleteRequestExpectation struct {
	actorID string
	reason  string
}

func assertToolDeleteRequests(t *testing.T, executor *recordingToolWorkspaceExecutor, want []toolDeleteRequestExpectation) {
	t.Helper()
	if len(executor.deleteReqs) != len(want) {
		t.Fatalf("delete requests = %#v, want %d request(s)", executor.deleteReqs, len(want))
	}
	for i, req := range executor.deleteReqs {
		if req.Ref.ID != want[i].actorID || req.Ref.ClaimName != want[i].actorID {
			t.Fatalf("delete request %d ref = %#v, want actor %q", i, req.Ref, want[i].actorID)
		}
		if req.Reason != want[i].reason {
			t.Fatalf("delete request %d reason = %q, want %q", i, req.Reason, want[i].reason)
		}
		if !req.SkipScrub {
			t.Fatalf("delete request %d SkipScrub = false, want true for MCP tool actor deletion", i)
		}
	}
}

func TestToolWorkspaceRequiresWorkspaceProviderAPI(t *testing.T) {
	tool := &corev1alpha1.Tool{Spec: corev1alpha1.ToolSpec{
		Description: "workspace-backed MCP tool",
		MCP: &corev1alpha1.MCPToolServer{Workspace: &corev1alpha1.MCPWorkspace{
			ClassRef: corev1alpha1.WorkspaceClassReference{Name: "service-v1"},
			Port:     8080,
		}},
	}}
	r := &ToolReconciler{}
	if err := r.validateTool(context.Background(), tool); err == nil ||
		!strings.Contains(err.Error(), "workspace provider API") {
		t.Fatalf("disabled workspace API validation error = %v", err)
	}
	r.WorkspaceProviderAPIEnabled = true
	if err := r.validateTool(context.Background(), tool); err == nil ||
		!strings.Contains(err.Error(), "controller-first Tool workspace integration") {
		t.Fatalf("enabled workspace API validation error = %v", err)
	}
}
