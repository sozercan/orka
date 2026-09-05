package controller

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/gateway/referenceadapter"
)

func TestGatewayReconcilerProbesReferenceAdapter(t *testing.T) {
	adapter := referenceadapter.New("outbound-token")
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	class := &gatewayv1alpha1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "generic-chat", Generation: 1},
		Spec: gatewayv1alpha1.GatewayClassSpec{
			ContractVersion: gatewayv1alpha1.ContractVersionV1,
			Category:        gatewayv1alpha1.GatewayCategoryChat,
			Capabilities: gatewayv1alpha1.GatewayCapabilities{
				InboundText: true, OutboundText: true, IdempotentDelivery: true,
			},
		},
		Status: gatewayv1alpha1.GatewayClassStatus{Accepted: true, ObservedGeneration: 1},
	}
	object := &gatewayv1alpha1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default", Generation: 1},
		Spec: gatewayv1alpha1.GatewaySpec{
			GatewayClassName: "generic-chat",
			Adapter:          gatewayv1alpha1.GatewayAdapterLocation{Endpoint: server.URL},
			InboundAuthRef:   gatewayv1alpha1.GatewayBearerAuthReference{Name: "inbound", Key: "token"},
			OutboundAuthRef:  gatewayv1alpha1.GatewayBearerAuthReference{Name: "outbound", Key: "token"},
		},
	}
	inbound := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "inbound", Namespace: "default", Labels: map[string]string{
			gatewayruntime.GatewayInboundAuthLabel: gatewayruntime.GatewayAuthEnabledValue,
			gatewayruntime.GatewayAuthNameLabel:    "chat",
		}, Annotations: map[string]string{gatewayruntime.GatewayAuthNameAnnotation: "chat"}},
		Data: map[string][]byte{"token": []byte("inbound-token")},
	}
	outbound := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "outbound", Namespace: "default", Labels: map[string]string{
			gatewayruntime.GatewayOutboundAuthLabel: gatewayruntime.GatewayAuthEnabledValue,
			gatewayruntime.GatewayAuthNameLabel:     "chat",
		}, Annotations: map[string]string{
			gatewayruntime.GatewayAuthNameAnnotation:     "chat",
			gatewayruntime.GatewayAuthEndpointAnnotation: server.URL,
		}},
		Data: map[string][]byte{"token": []byte("outbound-token")},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&gatewayv1alpha1.Gateway{}).
		WithObjects(class, object, inbound, outbound).Build()
	reconciler := &GatewayReconciler{Client: client, Scheme: scheme, HTTPClient: server.Client(), AllowInsecureLoopback: true}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "chat"}}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	updated := &gatewayv1alpha1.Gateway{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "chat"}, updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Status.Ready || !updated.Status.Connected || updated.Status.ObservedCapabilities == nil {
		t.Fatalf("Gateway status = %+v", updated.Status)
	}
	if updated.Status.ResolvedEndpoint != server.URL {
		t.Fatalf("resolved endpoint = %q, want %q", updated.Status.ResolvedEndpoint, server.URL)
	}
}

func TestValidateGatewaySpecRejectsNonCanonicalAuthSecretNames(t *testing.T) {
	tests := []struct {
		name         string
		inboundName  string
		outboundName string
		wantError    string
	}{
		{
			name:         "trim-equivalent names",
			inboundName:  "shared",
			outboundName: " shared ",
			wantError:    "must use separate Secrets",
		},
		{
			name:         "non-canonical inbound name",
			inboundName:  " inbound ",
			outboundName: "outbound",
			wantError:    "inboundAuthRef name must not contain surrounding whitespace",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := &gatewayv1alpha1.Gateway{
				Spec: gatewayv1alpha1.GatewaySpec{
					GatewayClassName: "generic-chat",
					InboundAuthRef: gatewayv1alpha1.GatewayBearerAuthReference{
						Name: test.inboundName, Key: "token",
					},
					OutboundAuthRef: gatewayv1alpha1.GatewayBearerAuthReference{
						Name: test.outboundName, Key: "token",
					},
				},
			}
			if err := validateGatewaySpec(object); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validateGatewaySpec() = %v, want error containing %q", err, test.wantError)
			}
		})
	}
}

func TestValidateGatewayBindingFailsClosed(t *testing.T) {
	binding := &gatewayv1alpha1.GatewayBinding{
		Spec: gatewayv1alpha1.GatewayBindingSpec{
			GatewayRef: gatewayv1alpha1.GatewayBindingReference{Name: "chat"},
			AgentRef:   gatewayv1alpha1.GatewayBindingReference{Name: "agent"},
			Match:      gatewayv1alpha1.GatewayBindingMatch{AccountID: "acct", ContextID: "room"},
		},
	}
	if err := validateGatewayBindingSpec(binding); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("validateGatewayBindingSpec() = %v, want default-deny allowlist error", err)
	}
	binding.Spec.Match.SenderID = "sender"
	binding.Spec.Session.Mode = gatewayv1alpha1.GatewaySessionThread
	if err := validateGatewayBindingSpec(binding); err != nil {
		t.Fatalf("validateGatewayBindingSpec() error = %v", err)
	}
	if err := validateBindingCapabilities(binding, gatewayv1alpha1.GatewayCapabilities{InboundText: true, OutboundText: true, IdempotentDelivery: true}); err == nil {
		t.Fatal("validateBindingCapabilities() accepted thread mode without threads capability")
	}
	binding.Spec.Session.Mode = gatewayv1alpha1.GatewaySessionContext
	if err := validateBindingCapabilities(binding, gatewayv1alpha1.GatewayCapabilities{InboundText: true, OutboundText: true, IdempotentDelivery: true}); err == nil || !strings.Contains(err.Error(), "senderIdentity") {
		t.Fatalf("validateBindingCapabilities() = %v, want senderIdentity requirement for exact sender matching", err)
	}
}

func TestGatewayBindingsOverlapAtEqualPriority(t *testing.T) {
	left := &gatewayv1alpha1.GatewayBinding{Spec: gatewayv1alpha1.GatewayBindingSpec{
		GatewayRef:   gatewayv1alpha1.GatewayBindingReference{Name: "chat"},
		Match:        gatewayv1alpha1.GatewayBindingMatch{AccountID: "acct", ContextID: "room"},
		SenderPolicy: gatewayv1alpha1.GatewaySenderPolicy{AllowedSenderIDs: []string{"same"}},
	}}
	right := left.DeepCopy()
	right.Spec.Match.ThreadID = "thread"
	if !gatewayBindingsOverlap(left, right) {
		t.Fatal("broad and thread-specific bindings should overlap")
	}
	right.Spec.SenderPolicy.AllowedSenderIDs = []string{"different"}
	if gatewayBindingsOverlap(left, right) {
		t.Fatal("disjoint allowlists should not overlap")
	}

	left.Spec.Match.SenderID = "left"
	left.Spec.SenderPolicy = gatewayv1alpha1.GatewaySenderPolicy{Mode: gatewayv1alpha1.GatewaySenderPolicyAll}
	right.Spec.Match.SenderID = "right"
	right.Spec.SenderPolicy = gatewayv1alpha1.GatewaySenderPolicy{Mode: gatewayv1alpha1.GatewaySenderPolicyAll}
	if gatewayBindingsOverlap(left, right) {
		t.Fatal("disjoint exact-sender bindings should not overlap even with senderPolicy all")
	}
	right.Spec.Match.SenderID = "left"
	if !gatewayBindingsOverlap(left, right) {
		t.Fatal("matching exact-sender bindings should overlap")
	}
}

func TestGatewayBindingReconcilerIgnoresUnprogrammableOverlappingPeer(t *testing.T) {
	scheme := newGatewayBindingTestScheme(t)
	current := gatewayBindingTestObject("current", "current-agent")
	peer := gatewayBindingTestObject("peer", "missing-agent")
	peer.Status = gatewayv1alpha1.GatewayBindingStatus{
		Accepted: true, ObservedGeneration: peer.Generation,
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&gatewayv1alpha1.GatewayBinding{}).
		WithObjects(
			gatewayBindingTestGateway(),
			&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "current-agent", Namespace: "default"}},
			current,
			peer,
		).Build()
	reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: current.Name}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &gatewayv1alpha1.GatewayBinding{}
	if err := fakeClient.Get(context.Background(), request.NamespacedName, updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Status.Accepted || !updated.Status.ResolvedRefs || !updated.Status.Programmed || !updated.Status.Ready {
		t.Fatalf("GatewayBinding status = %+v, want ready despite unprogrammable peer", updated.Status)
	}
}

func TestGatewayBindingReconcilerRejectsProgrammablePeerRegardlessOfStatusOrder(t *testing.T) {
	orders := []struct {
		name  string
		names []string
	}{
		{name: "current first", names: []string{"current", "peer"}},
		{name: "peer first", names: []string{"peer", "current"}},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			scheme := newGatewayBindingTestScheme(t)
			current := gatewayBindingTestObject("current", "current-agent")
			peer := gatewayBindingTestObject("peer", "peer-agent")
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&gatewayv1alpha1.GatewayBinding{}).
				WithObjects(
					gatewayBindingTestGateway(),
					&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "current-agent", Namespace: "default"}},
					&corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "peer-agent", Namespace: "default"}},
					current,
					peer,
				).Build()
			reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
			for _, name := range order.names {
				request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}
				if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
					t.Fatalf("Reconcile(%q) error = %v", name, err)
				}
			}

			for _, name := range []string{"current", "peer"} {
				updated := &gatewayv1alpha1.GatewayBinding{}
				key := types.NamespacedName{Namespace: "default", Name: name}
				if err := fakeClient.Get(context.Background(), key, updated); err != nil {
					t.Fatal(err)
				}
				if !updated.Status.Accepted || !updated.Status.ResolvedRefs || updated.Status.Programmed || updated.Status.Ready {
					t.Fatalf("GatewayBinding %q status = %+v, want independently valid but ambiguous", name, updated.Status)
				}
			}
		})
	}
}

func TestGatewayBindingReconcilerAgentChangeEnqueuesOverlappingPeers(t *testing.T) {
	scheme := newGatewayBindingTestScheme(t)
	direct := gatewayBindingTestObject("direct", "changed-agent")
	overlap := gatewayBindingTestObject("overlap", "other-agent")
	differentPriority := gatewayBindingTestObject("different-priority", "other-agent")
	differentPriority.Spec.Priority = 1
	differentContext := gatewayBindingTestObject("different-context", "other-agent")
	differentContext.Spec.Match.ContextID = "elsewhere"
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(direct, overlap, differentPriority, differentContext).
		Build()
	reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
	requests := reconciler.bindingsForAgent(context.Background(), &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "changed-agent", Namespace: "default"},
	})
	got := make(map[string]bool, len(requests))
	for _, request := range requests {
		got[request.Name] = true
	}
	if len(got) != 2 || !got[direct.Name] || !got[overlap.Name] {
		t.Fatalf("bindingsForAgent() = %v, want direct and overlapping peer", got)
	}
}

func TestGatewayBindingReconcilerValidatesRuntimeMaxTurnsCompatibility(t *testing.T) {
	for _, test := range []struct {
		name        string
		contract    corev1alpha1.AgentRuntimeContractVersion
		external    bool
		wantReady   bool
		wantMessage string
	}{
		{
			name:      "built-in harness v2",
			contract:  corev1alpha1.AgentRuntimeContractHarnessV2,
			wantReady: true,
		},
		{
			name:      "external harness v1",
			contract:  corev1alpha1.AgentRuntimeContractHarnessV1,
			external:  true,
			wantReady: true,
		},
		{
			name:        "external harness v2",
			contract:    corev1alpha1.AgentRuntimeContractHarnessV2,
			external:    true,
			wantMessage: "taskDefaults.agentRuntimeMaxTurns is not supported by external orka.harness.v2 AgentRuntime",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := newGatewayBindingTestScheme(t)
			maxTurns := int32(12)
			binding := gatewayBindingTestObject("binding", "assistant")
			binding.Spec.TaskDefaults.AgentRuntimeMaxTurns = &maxTurns
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "assistant", Namespace: "default"},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					Type:            corev1alpha1.AgentRuntimeCodex,
					ContractVersion: &test.contract,
				}},
			}
			objects := []client.Object{gatewayBindingTestGateway(), agent, binding}
			if test.external {
				agent.Spec.Runtime.Type = ""
				agent.Spec.Runtime.ContractVersion = nil
				agent.Spec.Runtime.RuntimeRef = &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"}
				objects = append(objects, &corev1alpha1.AgentRuntime{
					ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default"},
					Spec:       corev1alpha1.AgentRuntimeRegistrySpec{ContractVersion: &test.contract},
				})
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&gatewayv1alpha1.GatewayBinding{}).
				WithObjects(objects...).Build()
			reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: binding.Name}}
			if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			updated := &gatewayv1alpha1.GatewayBinding{}
			if err := fakeClient.Get(context.Background(), request.NamespacedName, updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Ready != test.wantReady || updated.Status.Programmed != test.wantReady {
				t.Fatalf("GatewayBinding status = %+v, want ready=%t", updated.Status, test.wantReady)
			}
			if !updated.Status.Accepted || !updated.Status.ResolvedRefs {
				t.Fatalf("GatewayBinding status = %+v, want accepted and resolved", updated.Status)
			}
			if test.wantMessage != "" && !strings.Contains(updated.Status.Message, test.wantMessage) {
				t.Fatalf("GatewayBinding message = %q, want substring %q", updated.Status.Message, test.wantMessage)
			}
		})
	}
}

func TestGatewayBindingReconcilerValidatesExternalRuntimeDefaultsAndPolicy(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, test := range []struct {
		name         string
		capabilities *corev1alpha1.AgentRuntimeCapabilitiesSpec
		retryPolicy  *gatewayv1alpha1.GatewayTaskRetryPolicy
		wantReady    bool
		wantMessage  string
	}{
		{
			name: "valid registered policy",
			capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{"read_evidence"},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
			wantReady: true,
		},
		{
			name: "missing registered policy",
			capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
			},
			wantMessage: "missing capabilities.mcpPolicy",
		},
		{
			name: "retry defaults",
			capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{AllowedTools: []string{}, DisallowedTools: []string{}},
			},
			retryPolicy: &gatewayv1alpha1.GatewayTaskRetryPolicy{MaxRetries: 1},
			wantMessage: "taskDefaults.retryPolicy.maxRetries must be 0 for external orka.harness.v2 AgentRuntime",
		},
		{
			name: "write-pinned runtime",
			capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentWrite,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{AllowedTools: []string{}, DisallowedTools: []string{}},
			},
			wantMessage: `profile workspace intent "write" does not match Gateway Task intent "read"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := newGatewayBindingTestScheme(t)
			binding := gatewayBindingTestObject("binding", "assistant")
			binding.Spec.TaskDefaults.RetryPolicy = test.retryPolicy
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "assistant", Namespace: "default"},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
				}},
			}
			runtimeObject := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default"},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities:    test.capabilities,
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&gatewayv1alpha1.GatewayBinding{}).
				WithObjects(gatewayBindingTestGateway(), agent, runtimeObject, binding).Build()
			reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: binding.Name}}
			if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			updated := &gatewayv1alpha1.GatewayBinding{}
			if err := fakeClient.Get(context.Background(), request.NamespacedName, updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Ready != test.wantReady || updated.Status.Programmed != test.wantReady {
				t.Fatalf("GatewayBinding status = %+v, want ready=%t", updated.Status, test.wantReady)
			}
			if test.wantMessage != "" && !strings.Contains(updated.Status.Message, test.wantMessage) {
				t.Fatalf("GatewayBinding message = %q, want substring %q", updated.Status.Message, test.wantMessage)
			}
		})
	}
}

func TestGatewayBindingReconcilerAgentRuntimeChangeEnqueuesOverlappingPeers(t *testing.T) {
	scheme := newGatewayBindingTestScheme(t)
	direct := gatewayBindingTestObject("direct", "changed-agent")
	overlap := gatewayBindingTestObject("overlap", "other-agent")
	differentPriority := gatewayBindingTestObject("different-priority", "other-agent")
	differentPriority.Spec.Priority = 1
	differentContext := gatewayBindingTestObject("different-context", "other-agent")
	differentContext.Spec.Match.ContextID = "elsewhere"
	changedAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "changed-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: " changed-runtime "},
		}},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(changedAgent, direct, overlap, differentPriority, differentContext).
		Build()
	reconciler := &GatewayBindingReconciler{Client: fakeClient, Scheme: scheme}
	requests := reconciler.bindingsForAgentRuntime(context.Background(), &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "changed-runtime", Namespace: "default"},
	})
	got := make(map[string]bool, len(requests))
	for _, request := range requests {
		got[request.Name] = true
	}
	if len(got) != 2 || !got[direct.Name] || !got[overlap.Name] {
		t.Fatalf("bindingsForAgentRuntime() = %v, want direct and overlapping peer", got)
	}
}

func newGatewayBindingTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func gatewayBindingTestGateway() *gatewayv1alpha1.Gateway {
	return &gatewayv1alpha1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default", Generation: 1},
		Status: gatewayv1alpha1.GatewayStatus{
			Ready:              true,
			ObservedGeneration: 1,
			ObservedCapabilities: &gatewayv1alpha1.GatewayObservedCapabilities{
				Capabilities: gatewayv1alpha1.GatewayCapabilities{
					InboundText: true, OutboundText: true, IdempotentDelivery: true,
				},
			},
		},
	}
}

func gatewayBindingTestObject(name, agentName string) *gatewayv1alpha1.GatewayBinding {
	return &gatewayv1alpha1.GatewayBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: gatewayv1alpha1.GatewayBindingSpec{
			GatewayRef:   gatewayv1alpha1.GatewayBindingReference{Name: "chat"},
			AgentRef:     gatewayv1alpha1.GatewayBindingReference{Name: agentName},
			Match:        gatewayv1alpha1.GatewayBindingMatch{AccountID: "acct", ContextID: "room"},
			SenderPolicy: gatewayv1alpha1.GatewaySenderPolicy{Mode: gatewayv1alpha1.GatewaySenderPolicyAll},
		},
	}
}
