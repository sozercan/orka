package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	executionevents "github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/gateway/protocol"
	"github.com/orka-agents/orka/internal/gateway/referenceadapter"
	orkalabels "github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
)

const replacementGatewayUID = "replacement-gateway-uid"

func TestGatewayServiceRequiresLeaderElection(t *testing.T) {
	if !(&Service{}).NeedLeaderElection() {
		t.Fatal("gateway service must run under controller leader election")
	}
}

func newGatewayServiceFixture(t *testing.T) (*Service, *sqlite.Store, *referenceadapter.Server) {
	t.Helper()
	ctx := context.Background()
	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sqliteStore := sqlite.NewStore(db, ":memory:")
	adapter := referenceadapter.New("outbound-token")
	server := httptest.NewServer(adapter.Handler())
	t.Cleanup(server.Close)

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	namespaceObject := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "namespace-uid"}}
	class := &gatewayv1alpha1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "generic-chat", Generation: 1},
		Spec: gatewayv1alpha1.GatewayClassSpec{
			ContractVersion: gatewayv1alpha1.ContractVersionV1, Category: gatewayv1alpha1.GatewayCategoryChat,
			AllowedMetadataKeys: []string{"fixture", "fixtureDelayMs"},
		},
		Status: gatewayv1alpha1.GatewayClassStatus{Accepted: true, ObservedGeneration: 1},
	}
	gatewayObject := &gatewayv1alpha1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default", UID: "gateway-uid", Generation: 1},
		Spec: gatewayv1alpha1.GatewaySpec{
			GatewayClassName: "generic-chat",
			Adapter:          gatewayv1alpha1.GatewayAdapterLocation{Endpoint: server.URL},
			InboundAuthRef:   gatewayv1alpha1.GatewayBearerAuthReference{Name: "inbound", Key: "token"},
			OutboundAuthRef:  gatewayv1alpha1.GatewayBearerAuthReference{Name: "outbound", Key: "token"},
		},
		Status: gatewayv1alpha1.GatewayStatus{
			Ready: true, Connected: true, Accepted: true, ResolvedRefs: true, ObservedGeneration: 1,
			ObservedInboundAuthRefVersion: "1", ObservedOutboundAuthRefVersion: "1",
		},
	}
	binding := &gatewayv1alpha1.GatewayBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "room", Namespace: "default", UID: "binding-uid", Generation: 1},
		Spec: gatewayv1alpha1.GatewayBindingSpec{
			GatewayRef: gatewayv1alpha1.GatewayBindingReference{Name: "chat"},
			AgentRef:   gatewayv1alpha1.GatewayBindingReference{Name: "assistant"},
			Match:      gatewayv1alpha1.GatewayBindingMatch{AccountID: "acct", ContextID: "room"},
			SenderPolicy: gatewayv1alpha1.GatewaySenderPolicy{
				Mode: gatewayv1alpha1.GatewaySenderPolicyAllowlist, AllowedSenderIDs: []string{"user-1"},
			},
			Session: gatewayv1alpha1.GatewaySessionSpec{Mode: gatewayv1alpha1.GatewaySessionContext},
		},
		Status: gatewayv1alpha1.GatewayBindingStatus{Ready: true, Programmed: true, Accepted: true, ResolvedRefs: true, ObservedGeneration: 1},
	}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "assistant", Namespace: "default", UID: "agent-uid"}}
	inbound := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "inbound", Namespace: "default", ResourceVersion: "1", Labels: map[string]string{
			GatewayInboundAuthLabel: GatewayAuthEnabledValue, GatewayAuthNameLabel: "chat",
		}, Annotations: map[string]string{GatewayAuthNameAnnotation: "chat"}},
		Data: map[string][]byte{"token": []byte("inbound-token")},
	}
	outbound := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "outbound", Namespace: "default", ResourceVersion: "1", Labels: map[string]string{
			GatewayOutboundAuthLabel: GatewayAuthEnabledValue, GatewayAuthNameLabel: "chat",
		}, Annotations: map[string]string{
			GatewayAuthNameAnnotation: "chat", GatewayAuthEndpointAnnotation: server.URL,
		}},
		Data: map[string][]byte{"token": []byte("outbound-token")},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &gatewayv1alpha1.Gateway{}, &gatewayv1alpha1.GatewayBinding{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, underlying client.WithWatch, object client.Object, opts ...client.CreateOption) error {
				if task, ok := object.(*corev1alpha1.Task); ok && task.UID == "" {
					task.UID = types.UID("uid-" + task.Name)
				}
				return underlying.Create(ctx, object, opts...)
			},
		}).
		WithObjects(namespaceObject, class, gatewayObject, binding, agent, inbound, outbound).Build()
	config := DefaultConfig()
	config.AllowInsecureLoopback = true
	config.PollInterval = time.Millisecond
	service := NewService(kubeClient, sqliteStore, sqliteStore, sqliteStore, config)
	if service == nil {
		t.Fatal("NewService returned nil")
	}
	service.HTTPClient = server.Client()
	_ = ctx
	return service, sqliteStore, adapter
}

func configureGatewayExternalRuntime(t *testing.T, service *Service, allowedTools []string) {
	t.Helper()
	ctx := context.Background()
	agent := &corev1alpha1.Agent{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "assistant"}, agent); err != nil {
		t.Fatal(err)
	}
	agent.Spec.Runtime = &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
	}
	if err := service.Client.Update(ctx, agent); err != nil {
		t.Fatal(err)
	}
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default", UID: "external-runtime-uid", Generation: 1},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          append([]string{}, allowedTools...),
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
	if err := service.Client.Create(ctx, runtimeObject); err != nil {
		t.Fatal(err)
	}
}

func gatewayEventBody(t *testing.T, externalID, sender string) []byte {
	t.Helper()
	return gatewayEnvelopeBody(t, protocol.EventEnvelope{
		ProtocolVersion: protocol.Version, ExternalEventID: externalID, EventType: protocol.EventTypeText,
		AccountID: "acct", ContextID: "room", Sender: protocol.Sender{ID: sender},
		Text: "hello", ReplyTarget: "room",
	})
}

func gatewayEnvelopeBody(t *testing.T, event protocol.EventEnvelope) []byte {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func attachDeliveredExpiryDelivery(
	t *testing.T, s *sqlite.Store, ctx context.Context, event store.GatewayEvent, completedAt time.Time,
) {
	t.Helper()
	deliveredAt := completedAt
	deliveryID := "gdl-" + event.ID
	if _, _, err := s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID, Reason: "expired", CompletedAt: completedAt,
		Delivery: store.GatewayDelivery{
			ID: deliveryID, IdempotencyID: deliveryID, Namespace: event.Namespace,
			NamespaceUID: event.NamespaceUID, GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
			GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
			TaskName: event.TaskName, SessionName: event.SessionName, Kind: protocol.DeliveryKindError,
			State: store.GatewayDeliveryDelivered, AccountID: event.AccountID, ContextID: event.ContextID,
			ReplyTarget: event.ReplyTarget, Text: "expired", MaxAttempts: 10,
			NextAttemptAt: completedAt, ExpiresAt: completedAt.Add(time.Hour),
			ProviderMessageID: "provider-" + event.ID, CreatedAt: completedAt, UpdatedAt: completedAt, DeliveredAt: &deliveredAt,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestServiceEndToEndAndDuplicateSafety(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "event-1", "user-1"))
	if err != nil || response.Status != ingressStatusAccepted {
		t.Fatalf("AdmitEvent() = (%+v, %v)", response, err)
	}
	duplicate, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "event-1", "user-1"))
	if err != nil || duplicate.Status != ingressStatusDuplicate || duplicate.EventID != response.EventID {
		t.Fatalf("duplicate AdmitEvent() = (%+v, %v)", duplicate, err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", response.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatalf("get Task: %v", err)
	}
	if task.Spec.SessionRef == nil || task.Spec.SessionRef.Create || task.Spec.SessionRef.Append {
		t.Fatalf("gateway Task SessionRef = %+v", task.Spec.SessionRef)
	}
	if task.Spec.Prompt != "" || !task.Spec.SessionRef.PromptIncluded || task.Spec.SessionRef.ThroughMessageID == "" {
		t.Fatalf("gateway Task leaked prompt or lacks transcript-backed prompt policy: prompt=%q sessionRef=%+v", task.Spec.Prompt, task.Spec.SessionRef)
	}
	if task.Spec.RequestedBy == nil || task.Spec.RequestedBy.Subject != "user-1" {
		t.Fatalf("gateway Task requestedBy = %+v", task.Spec.RequestedBy)
	}
	if err := sqliteStore.SaveResult(ctx, "default", task.Name, []byte("final response")); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatalf("update Task status: %v", err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatalf("ProjectTerminals() error = %v", err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() error = %v", err)
	}
	if got := len(adapter.Deliveries()); got != 1 {
		t.Fatalf("adapter sends = %d, want 1", got)
	}
	if err := service.DeliverOnce(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second DeliverOnce() error = %v, want ErrNotFound", err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Messages) != 2 || session.Messages[0].Role != "user" || session.Messages[1].Content != "final response" {
		t.Fatalf("session transcript = %#v", session.Messages)
	}
}

func TestGatewayDispatchMaterializesExternalRuntimeAllowedTools(t *testing.T) {
	for _, test := range []struct {
		name         string
		allowedTools []string
	}{
		{name: "registered tools", allowedTools: []string{"read_evidence"}},
		{name: "explicit deny all", allowedTools: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, sqliteStore, _ := newGatewayServiceFixture(t)
			configureGatewayExternalRuntime(t, service, test.allowedTools)
			ctx := context.Background()
			accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "external-runtime-"+strings.ReplaceAll(test.name, " ", "-"), "user-1"))
			if err != nil {
				t.Fatal(err)
			}
			if err := service.DispatchOnce(ctx); err != nil {
				t.Fatalf("DispatchOnce() error = %v", err)
			}
			event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
			if err != nil {
				t.Fatal(err)
			}
			task := &corev1alpha1.Task{}
			if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
				t.Fatal(err)
			}
			if task.Spec.AgentRuntime == nil || task.Spec.AgentRuntime.MaxTurns != nil {
				t.Fatalf("agentRuntime = %#v, want allowedTools without maxTurns", task.Spec.AgentRuntime)
			}
			if task.Spec.AgentRuntime.AllowedTools == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, test.allowedTools) {
				t.Fatalf("allowedTools = %#v, want explicit %#v", task.Spec.AgentRuntime.AllowedTools, test.allowedTools)
			}
		})
	}
}

func TestGatewayDispatchRecoveryMatchesMaterializedExternalRuntimePolicy(t *testing.T) {
	for _, test := range []struct {
		name         string
		allowedTools []string
		nextTools    []string
	}{
		{name: "registered tools", allowedTools: []string{"read_evidence"}, nextTools: []string{"search_evidence"}},
		{name: "explicit deny all", allowedTools: []string{}, nextTools: []string{"search_evidence"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const recoveryClaimLease = time.Second
			service, sqliteStore, _ := newGatewayServiceFixture(t)
			configureGatewayExternalRuntime(t, service, test.allowedTools)
			service.Config.ClaimLease = recoveryClaimLease
			service.EventStore = conflictMarkGatewayEventStore{GatewayEventStore: sqliteStore}
			ctx := context.Background()
			accepted, err := service.AdmitEvent(
				ctx, "default", "chat", "Bearer inbound-token",
				gatewayEventBody(t, "external-runtime-recovery-"+strings.ReplaceAll(test.name, " ", "-"), "user-1"),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.DispatchOnce(ctx); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("DispatchOnce() simulated lost-link error = %v, want ErrConflict", err)
			}
			event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
			if err != nil {
				t.Fatal(err)
			}
			if event.State != store.GatewayEventDispatching || !event.TaskPolicyFrozen ||
				event.TaskAllowedTools == nil || !slices.Equal(event.TaskAllowedTools, test.allowedTools) {
				t.Fatalf("event after lost link = %+v", event)
			}
			existing := &corev1alpha1.Task{}
			if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, existing); err != nil {
				t.Fatal(err)
			}
			runtimeObject := &corev1alpha1.AgentRuntime{}
			if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "external-runtime"}, runtimeObject); err != nil {
				t.Fatal(err)
			}
			runtimeObject.Spec.Capabilities.MCPPolicy.AllowedTools = append([]string{}, test.nextTools...)
			if err := service.Client.Update(ctx, runtimeObject); err != nil {
				t.Fatal(err)
			}
			service.EventStore = sqliteStore
			time.Sleep(recoveryClaimLease + 10*time.Millisecond)
			if err := service.DispatchOnce(ctx); err != nil {
				t.Fatalf("DispatchOnce() recovery error = %v", err)
			}
			recovered, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
			if err != nil || recovered.State != store.GatewayEventTaskCreated || recovered.TaskUID != string(existing.UID) {
				t.Fatalf("event after external runtime recovery = (%+v, %v)", recovered, err)
			}
		})
	}
}

func TestFailedGatewayTaskProjectsSafeActionableError(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	response, err := service.AdmitEvent(
		ctx,
		"default",
		"chat",
		"Bearer inbound-token",
		gatewayEventBody(t, "failed-runtime-config", "user-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", response.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseFailed
	task.Status.Message = "codex runtime requires ORKA_ALLOW_BASH=true because the Codex CLI cannot disable shell execution"
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
		Namespace: "default",
		EventID:   event.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("projected deliveries = %#v, want one", deliveries)
	}
	if got, want := deliveries[0].Text, "The AI agent is temporarily misconfigured. An operator needs to correct it."; got != want {
		t.Fatalf("delivery text = %q, want %q", got, want)
	}
	if strings.Contains(deliveries[0].Text, "ORKA_ALLOW_BASH") {
		t.Fatalf("delivery leaked internal runtime details: %q", deliveries[0].Text)
	}
}

func TestTerminalErrorTextUsesSafeFailureCategories(t *testing.T) {
	tests := []struct {
		name    string
		phase   corev1alpha1.TaskPhase
		message string
		want    string
	}{
		{
			name:  "cancelled",
			phase: corev1alpha1.TaskPhaseCancelled,
			want:  "The task was cancelled before a response was produced.",
		},
		{
			name:    "timeout",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "task timed out",
			want:    "The task timed out before a response was produced. Please try again.",
		},
		{
			name:    "rate limit",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "provider returned HTTP 429: rate limit exceeded",
			want:    "The AI service is temporarily rate-limited. Please try again shortly.",
		},
		{
			name:    "exhausted quota",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "provider returned HTTP 429: You exceeded your current quota",
			want:    "The AI service quota is unavailable. An operator needs to restore capacity.",
		},
		{
			name:    "status-only 429 remains generic",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "provider returned HTTP 429",
			want:    "The task could not be completed.",
		},
		{
			name:    "context length",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "maximum context length exceeded",
			want:    "The conversation is too long for the configured model. Please shorten the request and try again.",
		},
		{
			name:    "runtime configuration",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "codex runtime requires ORKA_ALLOW_BASH=true",
			want:    "The AI agent is temporarily misconfigured. An operator needs to correct it.",
		},
		{
			name:    "runtime configuration wins over context keyword",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "runtime configuration is invalid: context window must be positive",
			want:    "The AI agent is temporarily misconfigured. An operator needs to correct it.",
		},
		{
			name:    "unknown",
			phase:   corev1alpha1.TaskPhaseFailed,
			message: "opaque provider failure containing internal details",
			want:    "The task could not be completed.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := terminalErrorText(tt.phase, tt.message); got != tt.want {
				t.Fatalf("terminalErrorText() = %q, want %q", got, tt.want)
			}
		})
	}
}

//nolint:gocyclo // this test deliberately verifies the complete durable correlation chain in one flow
func TestServicePreservesTraceAndDeliveryCorrelation(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previousTracer := gatewayTracer
	previousPropagator := otel.GetTextMapPropagator()
	gatewayTracer = provider.Tracer("gateway-test")
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		gatewayTracer = previousTracer
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})

	ctx, root := provider.Tracer("adapter-test").Start(context.Background(), "adapter.receive")
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "trace-event", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	root.End()
	if err := service.DispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(context.Background(), "default", response.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(context.Background(), "default", task.Name, []byte("traced response")); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	if err := service.Client.Status().Update(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(context.Background()); err != nil {
		t.Fatal(err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(context.Background(), store.GatewayDeliveryFilter{
		Namespace: "default", EventID: event.ID,
	})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("projected deliveries = (%#v, %v)", deliveries, err)
	}
	if err := service.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	event, err = sqliteStore.GetGatewayEvent(context.Background(), "default", event.ID)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := sqliteStore.GetGatewayDelivery(context.Background(), "default", deliveries[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	providerMessageID := "reference:" + delivery.ID
	if event.DeliveryID != delivery.ID || event.ProviderMessageID != providerMessageID || delivery.ProviderMessageID != providerMessageID {
		t.Fatalf("durable correlation event=%+v delivery=%+v", event, delivery)
	}
	session, err := sqliteStore.GetSession(context.Background(), "default", event.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	terminal := session.Messages[len(session.Messages)-1]
	if terminal.Metadata["deliveryId"] != delivery.ID || terminal.Metadata["providerMessageId"] != providerMessageID {
		t.Fatalf("terminal message metadata = %#v", terminal.Metadata)
	}
	if err := service.Client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	if task.Annotations[TaskGatewayDelivery] != delivery.ID || task.Annotations[TaskGatewayProviderMessage] != providerMessageID {
		t.Fatalf("Task delivery annotations = %#v", task.Annotations)
	}
	timeline, err := sqliteStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace: "default", StreamType: store.ExecutionEventStreamTypeTask, StreamID: task.Name,
		EventTypes: []string{executionevents.ExecutionEventTypeGatewayDeliveryCompleted},
	})
	if err != nil || len(timeline) != 1 || !strings.Contains(string(timeline[0].Content), delivery.ID) {
		t.Fatalf("delivery timeline = (%#v, %v)", timeline, err)
	}

	traceIDs := []trace.TraceID{
		traceIDFromCarrier(t, event.TraceParent, event.TraceState),
		traceIDFromCarrier(t, task.Annotations[orkalabels.AnnotationTraceParent], task.Annotations[orkalabels.AnnotationTraceState]),
		traceIDFromCarrier(t, delivery.TraceParent, delivery.TraceState),
	}
	for _, id := range traceIDs[1:] {
		if id != traceIDs[0] {
			t.Fatalf("trace correlation IDs = %v", traceIDs)
		}
	}
	wantSpans := map[string]bool{"gateway.ingress": false, "gateway.dispatch": false, "gateway.project_terminal": false, "gateway.deliver": false}
	for _, span := range recorder.Ended() {
		if _, ok := wantSpans[span.Name()]; ok {
			wantSpans[span.Name()] = true
			if span.SpanContext().TraceID() != traceIDs[0] {
				t.Fatalf("span %s trace = %s, want %s", span.Name(), span.SpanContext().TraceID(), traceIDs[0])
			}
		}
	}
	for name, found := range wantSpans {
		if !found {
			t.Fatalf("missing correlated span %s", name)
		}
	}
}

func traceIDFromCarrier(t *testing.T, traceParent, traceState string) trace.TraceID {
	t.Helper()
	ctx := orkatracing.ExtractContext(context.Background(), orkatracing.MapCarrier{
		"traceparent": traceParent,
		"tracestate":  traceState,
	})
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		t.Fatalf("invalid trace carrier traceparent=%q tracestate=%q", traceParent, traceState)
	}
	return spanContext.TraceID()
}

func TestBoundedTextPreservesUTF8WhenAddingSuffix(t *testing.T) {
	value := strings.Repeat("界", protocol.MaxTextBytes/3+100)
	got := boundedText(value)
	if !utf8.ValidString(got) {
		t.Fatal("boundedText returned invalid UTF-8")
	}
	if len(got) > protocol.MaxTextBytes {
		t.Fatalf("boundedText length = %d, want <= %d", len(got), protocol.MaxTextBytes)
	}
	if !strings.HasSuffix(got, "[response truncated by gateway]") {
		t.Fatalf("boundedText suffix missing: %q", got[len(got)-64:])
	}
}

func TestServiceQueuesBusySessionFIFO(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	first, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "event-1", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "event-2", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	firstEventForTask, err := sqliteStore.GetGatewayEvent(ctx, "default", first.EventID)
	if err != nil {
		t.Fatal(err)
	}
	firstTask := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: firstEventForTask.TaskName}, firstTask); err != nil {
		t.Fatal(err)
	}
	if firstTask.Spec.SessionRef == nil || firstTask.Spec.SessionRef.ThroughMessageID != "gateway:"+first.EventID+":user" {
		t.Fatalf("first Task transcript cutoff = %+v", firstTask.Spec.SessionRef)
	}
	if err := service.DispatchOnce(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("busy session dispatch error = %v, want ErrNotFound", err)
	}
	firstEvent, _ := sqliteStore.GetGatewayEvent(ctx, "default", first.EventID)
	secondEvent, _ := sqliteStore.GetGatewayEvent(ctx, "default", second.EventID)
	if firstEvent.State != store.GatewayEventTaskCreated || secondEvent.State != store.GatewayEventQueued {
		t.Fatalf("event states = %s, %s", firstEvent.State, secondEvent.State)
	}
}

func TestServiceRejectsUnauthorizedSenderDurably(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "unauthorized", "intruder"))
	if err != nil || response.Status != ingressStatusRejected {
		t.Fatalf("AdmitEvent() = (%+v, %v)", response, err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", response.EventID)
	if err != nil || event.State != store.GatewayEventRejected || event.SessionName != "" || event.TaskName != "" {
		t.Fatalf("rejected event = (%+v, %v)", event, err)
	}
}

func TestRejectedEventFallsBackToContextForDenialDelivery(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	var envelope map[string]any
	if err := json.Unmarshal(gatewayEventBody(t, "denial-context-fallback", "intruder"), &envelope); err != nil {
		t.Fatal(err)
	}
	delete(envelope, "replyTarget")
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || response.Status != ingressStatusRejected {
		t.Fatalf("AdmitEvent() = (%+v, %v)", response, err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
		Namespace: "default", EventID: response.EventID,
	})
	if err != nil || len(deliveries) != 1 || deliveries[0].ReplyTarget != "room" {
		t.Fatalf("denial deliveries = (%+v, %v), want context fallback", deliveries, err)
	}
}

func TestServiceRejectsBadInboundAuthentication(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	_, err := service.AdmitEvent(context.Background(), "default", "chat", "Bearer wrong", gatewayEventBody(t, "bad-auth", "user-1"))
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != 401 {
		t.Fatalf("AdmitEvent() error = %v, want HTTP 401", err)
	}
}

func TestDuplicateAcceptedEventDoesNotCreateDenialDelivery(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	body := gatewayEventBody(t, "duplicate-no-denial", "user-1")
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || accepted.Status != ingressStatusAccepted {
		t.Fatalf("first AdmitEvent() = (%+v, %v)", accepted, err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "room"}, binding); err != nil {
		t.Fatal(err)
	}
	binding.Status.Ready = false
	if err := service.Client.Status().Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || duplicate.Status != ingressStatusDuplicate || duplicate.State != string(store.GatewayEventQueued) {
		t.Fatalf("duplicate AdmitEvent() = (%+v, %v)", duplicate, err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("duplicate accepted event created denial deliveries: %#v", deliveries)
	}
}

func TestTombstoneDuplicateAcknowledgementPrecedesReadinessChecks(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	body := gatewayEventBody(t, "tombstone-duplicate", "user-1")
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-31 * 24 * time.Hour)
	if err := sqliteStore.ExpireGatewayEvent(ctx, "default", accepted.EventID, "", "expired", old); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := sqliteStore.MaintainGatewayRecords(ctx, "", now, now.Add(-30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	gatewayObject := &gatewayv1alpha1.Gateway{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "chat"}, gatewayObject); err != nil {
		t.Fatal(err)
	}
	gatewayObject.Status.Ready = false
	if err := service.Client.Status().Update(ctx, gatewayObject); err != nil {
		t.Fatal(err)
	}

	duplicate, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || duplicate.Status != ingressStatusDuplicate || duplicate.EventID != accepted.EventID {
		t.Fatalf("tombstone duplicate = (%+v, %v)", duplicate, err)
	}
}

func TestDuplicateAcknowledgementPrecedesMutableReadinessChecks(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	body := gatewayEnvelopeBody(t, protocol.EventEnvelope{
		ProtocolVersion: protocol.Version, ExternalEventID: "duplicate-after-policy-change", EventType: protocol.EventTypeText,
		AccountID: "acct", ContextID: "room", ThreadID: "thread-1",
		Sender: protocol.Sender{ID: "user-1", DisplayName: "User One"},
		Text:   "hello", ReplyTarget: "room", Metadata: map[string]string{"fixture": "allowed"},
	})
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || accepted.Status != ingressStatusAccepted {
		t.Fatalf("first AdmitEvent() = (%+v, %v)", accepted, err)
	}

	class := &gatewayv1alpha1.GatewayClass{}
	if err := service.Client.Get(ctx, client.ObjectKey{Name: "generic-chat"}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.AllowedMetadataKeys = nil
	if err := service.Client.Update(ctx, class); err != nil {
		t.Fatal(err)
	}
	duplicate, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || duplicate.Status != ingressStatusDuplicate || duplicate.EventID != accepted.EventID {
		t.Fatalf("duplicate after metadata policy change = (%+v, %v)", duplicate, err)
	}

	gatewayObject := &gatewayv1alpha1.Gateway{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "chat"}, gatewayObject); err != nil {
		t.Fatal(err)
	}
	gatewayObject.Status.Ready = false
	if err := service.Client.Status().Update(ctx, gatewayObject); err != nil {
		t.Fatal(err)
	}
	duplicate, err = service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || duplicate.Status != ingressStatusDuplicate || duplicate.EventID != accepted.EventID {
		t.Fatalf("duplicate while Gateway not ready = (%+v, %v)", duplicate, err)
	}
}

func TestDuplicateExternalEventIDRejectsDifferentEnvelope(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	body := gatewayEventBody(t, "payload-conflict", "user-1")
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || accepted.Status != ingressStatusAccepted {
		t.Fatalf("first AdmitEvent() = (%+v, %v)", accepted, err)
	}

	conflicting := protocol.EventEnvelope{
		ProtocolVersion: protocol.Version, ExternalEventID: "payload-conflict", EventType: protocol.EventTypeText,
		AccountID: "acct", ContextID: "room", Sender: protocol.Sender{ID: "user-1"},
		Text: "different content", ReplyTarget: "room",
	}
	_, err = service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEnvelopeBody(t, conflicting))
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != http.StatusConflict {
		t.Fatalf("conflicting duplicate error = %v, want HTTP 409", err)
	}
	events, listErr := sqliteStore.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default"})
	if listErr != nil || len(events) != 1 || events[0].Text != "hello" {
		t.Fatalf("events after conflicting duplicate = (%#v, %v)", events, listErr)
	}
}

func TestRetryDeliveryRejectsWhenProcessingDisabled(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	service.Config.Enabled = false
	_, err := service.RetryDelivery(context.Background(), "default", "delivery-id")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("RetryDelivery() error = %v, want HTTP 503", err)
	}
}

func TestMissingLinkedTaskTerminalizesWithoutReexecution(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "missing-task", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: event.TaskName, Namespace: "default"}}
	if err := service.Client.Delete(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("terminalized event = (%+v, %v)", event, err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil || session.ActiveTask != "" || len(session.Messages) != 2 {
		t.Fatalf("Session after linked Task deletion = (%+v, %v)", session, err)
	}
	if err := service.DispatchOnce(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DispatchOnce() error = %v, want no re-execution", err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() error = %v", err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("missing-task deliveries = %#v", deliveries)
	} else if deliveries[0].TaskRef != nil {
		t.Fatalf("missing-task expiry leaked taskRef: %+v", deliveries[0].TaskRef)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted Task was recreated: %v", err)
	}
}

func TestNamespaceTaskCapacityAvailableCountsFinalizingButNotTerminalTasks(t *testing.T) {
	tests := []struct {
		phase         corev1alpha1.TaskPhase
		wantAvailable bool
	}{
		{phase: "", wantAvailable: false},
		{phase: corev1alpha1.TaskPhasePending, wantAvailable: false},
		{phase: corev1alpha1.TaskPhaseRunning, wantAvailable: false},
		{phase: corev1alpha1.TaskPhaseFinalizing, wantAvailable: false},
		{phase: corev1alpha1.TaskPhaseSucceeded, wantAvailable: true},
		{phase: corev1alpha1.TaskPhaseFailed, wantAvailable: true},
		{phase: corev1alpha1.TaskPhaseCancelled, wantAvailable: true},
	}

	for _, tt := range tests {
		name := string(tt.phase)
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			service, _, _ := newGatewayServiceFixture(t)
			service.Config.MaxTasksPerNamespace = 1
			ctx := context.Background()
			existing := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "existing-task", Namespace: "default"},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
			}
			if err := service.Client.Create(ctx, existing); err != nil {
				t.Fatal(err)
			}
			if tt.phase != "" {
				existing.Status.Phase = tt.phase
				if err := service.Client.Status().Update(ctx, existing); err != nil {
					t.Fatal(err)
				}
			}

			available, err := service.namespaceTaskCapacityAvailable(ctx, "default", "new-task")
			if err != nil {
				t.Fatalf("namespaceTaskCapacityAvailable() error = %v", err)
			}
			if available != tt.wantAvailable {
				t.Fatalf("namespaceTaskCapacityAvailable() = %t, want %t", available, tt.wantAvailable)
			}
		})
	}
}

func TestDispatchOnceCountsFinalizingTaskTowardNamespaceLimitAcrossSessions(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Config.MaxTasksPerNamespace = 1
	ctx := context.Background()

	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "room"}, binding); err != nil {
		t.Fatal(err)
	}
	binding.Spec.Session.Mode = gatewayv1alpha1.GatewaySessionThread
	if err := service.Client.Update(ctx, binding); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		body := gatewayEnvelopeBody(t, protocol.EventEnvelope{
			ProtocolVersion: protocol.Version, ExternalEventID: fmt.Sprintf("limited-%d", i), EventType: protocol.EventTypeText,
			AccountID: "acct", ContextID: "room", ThreadID: fmt.Sprintf("thread-%d", i),
			Sender: protocol.Sender{ID: "user-1"}, Text: "hello", ReplyTarget: "room",
		})
		if response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body); err != nil || response.Status != ingressStatusAccepted {
			t.Fatalf("AdmitEvent(%d) = (%+v, %v)", i, response, err)
		}
	}

	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce(1) error = %v", err)
	}
	var tasks corev1alpha1.TaskList
	if err := service.freshReader().List(ctx, &tasks, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 1 {
		t.Fatalf("gateway Tasks after first dispatch = %d, want 1", len(tasks.Items))
	}
	created := tasks.Items[0].DeepCopy()
	created.Status.Phase = corev1alpha1.TaskPhaseFinalizing
	if err := service.Client.Status().Update(ctx, created); err != nil {
		t.Fatalf("mark first gateway Task Finalizing: %v", err)
	}

	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce(2) error = %v", err)
	}
	if err := service.freshReader().List(ctx, &tasks, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 1 {
		t.Fatalf("gateway Tasks = %d, want 1 at namespace limit", len(tasks.Items))
	}
	events, err := sqliteStore.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	states := map[store.GatewayEventState]int{}
	for i := range events {
		states[events[i].State]++
		if events[i].State == store.GatewayEventQueued {
			if events[i].StateMessage != "namespace task limit reached" {
				t.Fatalf("queued event state message = %q", events[i].StateMessage)
			}
			session, getErr := sqliteStore.GetSession(ctx, events[i].Namespace, events[i].SessionName)
			if getErr != nil || session.ActiveTask != "" {
				t.Fatalf("queued event session reservation = (%+v, %v)", session, getErr)
			}
		}
	}
	if states[store.GatewayEventTaskCreated] != 1 || states[store.GatewayEventQueued] != 1 {
		t.Fatalf("gateway event states = %#v, want one TaskCreated and one Queued", states)
	}
}

func TestDeliveryWaitsForCurrentGatewayReadiness(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-readiness", IdempotencyID: "gdl-readiness", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "gev-readiness",
		Kind: protocol.DeliveryKindFinal, AccountID: "acct", ContextID: "room", ReplyTarget: "room",
		Text: "ready check", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	gatewayObject := &gatewayv1alpha1.Gateway{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "chat"}, gatewayObject); err != nil {
		t.Fatal(err)
	}
	gatewayObject.Status.Ready = false
	if err := service.Client.Status().Update(ctx, gatewayObject); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(adapter.Deliveries()) != 0 {
		t.Fatal("delivery was sent through a non-ready Gateway")
	}
	stored, err := sqliteStore.GetGatewayDelivery(ctx, "default", delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryRetryScheduled {
		t.Fatalf("delivery after readiness gate = (%+v, %v)", stored, err)
	}
}

func TestGatewayDeliveryIDIncludesAdmissionIncarnation(t *testing.T) {
	base := &store.GatewayEvent{ID: "gev-stable", TaskName: "gw-first", CreatedAt: time.Unix(100, 1)}
	first := gatewayDeliveryID(base, protocol.DeliveryKindFinal)
	secondEvent := *base
	secondEvent.TaskName = "gw-second"
	second := gatewayDeliveryID(&secondEvent, protocol.DeliveryKindFinal)
	if first == second {
		t.Fatalf("delivery IDs reused across event incarnations: %q", first)
	}
	if first != gatewayDeliveryID(base, protocol.DeliveryKindFinal) {
		t.Fatal("delivery ID is not deterministic within one event incarnation")
	}
}

func TestGatewayTaskNameChangesAcrossAdmissionWindows(t *testing.T) {
	first := gatewayTaskName("gateway-uid", "external-id", time.Unix(100, 1))
	second := gatewayTaskName("gateway-uid", "external-id", time.Unix(100, 2))
	if first == second {
		t.Fatalf("gateway Task names are identical across admission windows: %q", first)
	}
	if first != gatewayTaskName("gateway-uid", "external-id", time.Unix(100, 1)) {
		t.Fatal("gateway Task name is not deterministic within one admission")
	}
}

func TestCleanupRetainedGatewayTasksDeletesOnlyOrphansPastCutoff(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	cutoff := now.Add(-30 * 24 * time.Hour)
	old := metav1.NewTime(cutoff.Add(-2 * time.Hour))
	recent := metav1.NewTime(cutoff.Add(time.Hour))
	oldTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old-orphan", Namespace: "default", CreationTimestamp: old,
			Labels: map[string]string{TaskGatewayEventLabel: "gev-old-orphan"},
			Annotations: map[string]string{
				TaskGatewayEventAnnotation: "gev-old-orphan", TaskGatewayNameAnnotation: "chat",
			},
		},
		Spec: corev1alpha1.TaskSpec{RequestedBy: &corev1alpha1.RequestedBy{
			Issuer: "gateway.orka.ai/default/namespace-uid/chat/gateway-uid",
		}},
	}
	recentTask := oldTask.DeepCopy()
	recentTask.Name = "recent-orphan"
	recentTask.CreationTimestamp = recent
	recentTask.Labels[TaskGatewayEventLabel] = "gev-recent-orphan"
	recentTask.Annotations[TaskGatewayEventAnnotation] = "gev-recent-orphan"
	forgedTask := oldTask.DeepCopy()
	forgedTask.Name = "forged-old-task"
	forgedTask.Spec.RequestedBy = nil
	forgedTask.Labels[TaskGatewayEventLabel] = "gev-forged"
	forgedTask.Annotations[TaskGatewayEventAnnotation] = "gev-forged"
	for _, task := range []*corev1alpha1.Task{oldTask, recentTask, forgedTask} {
		if err := service.Client.Create(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	event := store.GatewayEvent{
		ID: "gev-old-orphan", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
		BindingName: "room", BindingUID: "binding-uid", BindingGeneration: 1,
		AgentName: "assistant", AgentUID: "agent-uid", ExternalEventID: "external-old-orphan",
		ProtocolVersion: protocol.Version, EventType: protocol.EventTypeText, AccountID: "acct", ContextID: "room",
		SenderID: "user-1", Text: "old prompt", ReplyTarget: "room", SessionName: "cleanup-session",
		TaskName: oldTask.Name, TaskUID: string(oldTask.UID), ReceivedAt: old.Time, NextAttemptAt: old.Time,
		ExpiresAt: old.Add(time.Hour), CreatedAt: old.Time, UpdatedAt: old.Time,
	}
	if _, _, err := sqliteStore.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	attachDeliveredExpiryDelivery(t, sqliteStore, ctx, event, cutoff.Add(-time.Hour))
	if _, err := sqliteStore.MaintainGatewayRecords(ctx, "", now, cutoff); err != nil {
		t.Fatal(err)
	}
	if tombstoned, err := sqliteStore.HasGatewayTaskTombstone(ctx, event.Namespace, event.TaskName, event.TaskUID); err != nil || !tombstoned {
		t.Fatalf("task tombstone = (%v, %v), want true", tombstoned, err)
	}

	if err := service.cleanupRetainedGatewayTasks(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: oldTask.Name}, &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old tombstoned Task still present: %v", err)
	}
	for _, task := range []*corev1alpha1.Task{recentTask, forgedTask} {
		if err := service.Client.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Name}, &corev1alpha1.Task{}); err != nil {
			t.Fatalf("Task %s was deleted without immutable tombstone ownership: %v", task.Name, err)
		}
	}
}

func TestGatewayTaskUsesSafeLabelsAndBoundedDefaultTimeout(t *testing.T) {
	longGateway := strings.Repeat("g", 120)
	longBinding := strings.Repeat("b", 120)
	event := &store.GatewayEvent{
		ID: "gev-labels", Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "uid", GatewayGeneration: 1, GatewayName: longGateway,
		BindingName: longBinding, ExternalEventID: "external", SenderID: "sender", Text: "hello",
		SessionName: "session", TaskName: "gw-task", ExpiresAt: time.Now().Add(2 * time.Hour),
	}
	binding := &gatewayv1alpha1.GatewayBinding{Spec: gatewayv1alpha1.GatewayBindingSpec{
		AgentRef: gatewayv1alpha1.GatewayBindingReference{Name: "assistant"},
	}}
	task := taskForGatewayEvent(event, binding, time.Now())
	if len(task.Labels[TaskGatewayNameLabel]) > 63 || len(task.Labels[TaskGatewayBindingLabel]) > 63 {
		t.Fatalf("unsafe gateway labels: %#v", task.Labels)
	}
	if task.Annotations[TaskGatewayNameAnnotation] != longGateway || task.Annotations[TaskGatewayBindingAnnotation] != longBinding {
		t.Fatalf("full names missing from annotations: %#v", task.Annotations)
	}
	if task.Spec.Timeout == nil || task.Spec.Timeout.Duration <= 0 || task.Spec.Timeout.Duration > defaultGatewayTaskTimeout {
		t.Fatalf("gateway default timeout = %+v", task.Spec.Timeout)
	}
}

func TestDeterministicTaskCollisionRequiresFullExpectedTask(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "squatted-task", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	squatted := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: event.TaskName, Namespace: event.Namespace,
			Annotations: map[string]string{TaskGatewayEventAnnotation: event.ID},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "attacker-agent"},
			Prompt:   "attacker-controlled prompt",
		},
	}
	if err := service.Client.Create(ctx, squatted); err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventQueued {
		t.Fatalf("event after collision = (%+v, %v)", event, err)
	}
}

func TestQueuedEventDoesNotCrossBindingGeneration(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "binding-change", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "room"}, binding); err != nil {
		t.Fatal(err)
	}
	binding.Generation = 2
	binding.Spec.AgentRef.Name = "different-agent"
	if err := service.Client.Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	binding.Status.Ready = true
	binding.Status.ObservedGeneration = 2
	if err := service.Client.Status().Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("event after binding change = (%+v, %v)", event, err)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected Task after binding change: %v", err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() error = %v", err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("binding-change deliveries = %#v", deliveries)
	} else if deliveries[0].TaskRef != nil {
		t.Fatalf("pre-creation expiry leaked taskRef: %+v", deliveries[0].TaskRef)
	}
}

func TestQueuedEventDoesNotCrossAgentUID(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "agent-recreate", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	agent := &corev1alpha1.Agent{}
	key := client.ObjectKey{Namespace: "default", Name: "assistant"}
	if err := service.Client.Get(ctx, key, agent); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Delete(ctx, agent); err != nil {
		t.Fatal(err)
	}
	recreated := agent.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = "replacement-agent-uid"
	if err := service.Client.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("event after Agent replacement = (%+v, %v)", event, err)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, &corev1alpha1.Task{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected Task after Agent replacement: %v", err)
	}
}

func TestQueuedEventDoesNotCrossGatewayUID(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "gateway-recreate", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	original := &gatewayv1alpha1.Gateway{}
	key := client.ObjectKey{Namespace: "default", Name: "chat"}
	if err := service.Client.Get(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Delete(ctx, original); err != nil {
		t.Fatal(err)
	}
	recreated := original.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = replacementGatewayUID
	recreated.Status.Ready = true
	recreated.Status.ObservedGeneration = recreated.Generation
	if err := service.Client.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("event after Gateway replacement = (%+v, %v)", event, err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil || session.ActiveTask != "" || len(session.Messages) != 2 {
		t.Fatalf("Session after Gateway replacement = (%+v, %v)", session, err)
	}
}

func TestDeliveryDoesNotCrossGatewayUID(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-gateway-recreate", IdempotencyID: "gdl-gateway-recreate", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event-gateway-recreate",
		Kind: protocol.DeliveryKindFinal, AccountID: "acct", ContextID: "room", ReplyTarget: "room",
		Text: "do not cross identity", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	original := &gatewayv1alpha1.Gateway{}
	key := client.ObjectKey{Namespace: "default", Name: "chat"}
	if err := service.Client.Get(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Delete(ctx, original); err != nil {
		t.Fatal(err)
	}
	recreated := original.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = replacementGatewayUID
	if err := service.Client.Create(ctx, recreated); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := sqliteStore.GetGatewayDelivery(ctx, delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryDeadLettered {
		t.Fatalf("delivery after Gateway replacement = (%+v, %v)", stored, err)
	}
	if len(adapter.Deliveries()) != 0 {
		t.Fatal("delivery crossed immutable Gateway identity")
	}
}

func TestDeliveryDoesNotCrossGatewayGeneration(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-gateway-generation", IdempotencyID: "gdl-gateway-generation", Namespace: "default",
		NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1,
		GatewayName: "chat", EventID: "event-gateway-generation", Kind: protocol.DeliveryKindFinal,
		AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: "do not cross generation",
		MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	object := &gatewayv1alpha1.Gateway{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: "chat"}, object); err != nil {
		t.Fatal(err)
	}
	object.Generation = 2
	object.Status.Ready = true
	object.Status.ObservedGeneration = 2
	if err := service.Client.Update(ctx, object); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Status().Update(ctx, object); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := sqliteStore.GetGatewayDelivery(ctx, delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryDeadLettered {
		t.Fatalf("delivery after Gateway generation change = (%+v, %v)", stored, err)
	}
	if len(adapter.Deliveries()) != 0 {
		t.Fatal("delivery crossed admitted Gateway generation")
	}
}

func TestExpiredLinkedTaskReleasesSessionWithVisibleError(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 25 * time.Millisecond
	service.Config.PollInterval = time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "task-expiry", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("expired event = (%+v, %v)", event, err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil || session.ActiveTask != "" || len(session.Messages) != 2 || session.Messages[1].Role != "assistant" {
		t.Fatalf("Session after expiry = (%+v, %v)", session, err)
	}
	projected, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{Namespace: "default", EventID: event.ID})
	if err != nil || len(projected) != 1 {
		t.Fatalf("expired-task outbox = (%#v, %v)", projected, err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() error = %v", err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("expired-task deliveries = %#v", deliveries)
	}
}

func TestDeleteUnlinkedGatewayTaskStopsWaitingAfterCleanupGrace(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "stuck-unlinked-task", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := sqliteStore.ClaimNextGatewayEvent(ctx, "default", "stale-owner", time.Now().UTC(), time.Minute)
	if err != nil || claimed.ID != accepted.EventID {
		t.Fatalf("claimed event = (%+v, %v)", claimed, err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: claimed.BindingName}, binding); err != nil {
		t.Fatal(err)
	}
	task := taskForGatewayEvent(claimed, binding, time.Now().UTC())
	task.Finalizers = []string{"test.invalid/stuck"}
	if err := service.Client.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	deletedAt := metav1.NewTime(time.Now().Add(-gatewayTaskCleanupGrace - time.Second))
	service.Client = &staleDeletingTaskClient{Client: service.Client, taskName: task.Name, deletedAt: deletedAt}
	if err := service.deleteUnlinkedGatewayTask(ctx, claimed); err != nil {
		t.Fatalf("deleteUnlinkedGatewayTask() after grace = %v", err)
	}
}

type staleDeletingTaskClient struct {
	client.Client
	taskName  string
	deletedAt metav1.Time
}

func (c *staleDeletingTaskClient) Get(
	ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption,
) error {
	if err := c.Client.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	if task, ok := object.(*corev1alpha1.Task); ok && task.Name == c.taskName {
		task.DeletionTimestamp = c.deletedAt.DeepCopy()
	}
	return nil
}

func TestGatewayTaskTimeoutUsesRemainingEventLifetime(t *testing.T) {
	now := time.Now().UTC()
	event := &store.GatewayEvent{ReceivedAt: now.Add(-time.Hour), ExpiresAt: now.Add(5 * time.Minute)}
	timeout := gatewayTaskTimeout(event, nil, now)
	if timeout.Duration <= 0 || timeout.Duration > 5*time.Minute {
		t.Fatalf("timeout = %s, want remaining event lifetime", timeout.Duration)
	}
}

type failOnceDeliveryStore struct {
	store.GatewayDeliveryStore
	failed bool
}

func (s *failOnceDeliveryStore) CreateGatewayDelivery(
	ctx context.Context, delivery *store.GatewayDelivery,
) (*store.GatewayDelivery, bool, error) {
	if !s.failed {
		s.failed = true
		return nil, false, errors.New("injected delivery write failure")
	}
	return s.GatewayDeliveryStore.CreateGatewayDelivery(ctx, delivery)
}

func TestRejectedEventRetryRepairsMissingDenialDelivery(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	flaky := &failOnceDeliveryStore{GatewayDeliveryStore: sqliteStore}
	service.DeliveryStore = flaky
	ctx := context.Background()
	body := gatewayEventBody(t, "repair-denial", "intruder")
	if _, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body); err == nil {
		t.Fatal("first rejection unexpectedly succeeded despite injected outbox failure")
	}
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || response.Status != ingressStatusDuplicate || response.State != string(store.GatewayEventRejected) {
		t.Fatalf("retry AdmitEvent() = (%+v, %v)", response, err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{Namespace: "default"})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("repaired deliveries = (%#v, %v)", deliveries, err)
	}
}

func TestExpiredDispatchCrashLinksExistingTaskBeforeCleanup(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 1100 * time.Millisecond
	service.Config.ClaimLease = time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "dispatch-crash", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claimed, err := sqliteStore.ClaimNextGatewayEvent(ctx, "", "crashed-owner", now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: claimed.BindingName}, binding); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Create(ctx, taskForGatewayEvent(claimed, binding, now)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated {
		t.Fatalf("event after crash recovery = (%+v, %v)", event, err)
	}
}

func TestExpiredDispatchClaimRejectsNoncanonicalExistingTask(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 1100 * time.Millisecond
	service.Config.ClaimLease = time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "dispatch-crash-conflict", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claimed, err := sqliteStore.ClaimNextGatewayEvent(ctx, "", "crashed-owner", now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: claimed.BindingName}, binding); err != nil {
		t.Fatal(err)
	}
	noncanonical := taskForGatewayEvent(claimed, binding, now)
	noncanonical.Spec.RequestedBy.Issuer = "foreign.example/not-the-gateway"
	if err := service.Client.Create(ctx, noncanonical); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired || event.TaskUID != "" {
		t.Fatalf("event after noncanonical crash recovery = (%+v, %v)", event, err)
	}
}

func TestExpiredDispatchClaimRejectsRecreatedDependencies(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		replace func(*testing.T, *Service, context.Context)
	}{
		{
			name: "Gateway",
			replace: func(t *testing.T, service *Service, ctx context.Context) {
				t.Helper()
				object := &gatewayv1alpha1.Gateway{}
				key := client.ObjectKey{Namespace: "default", Name: "chat"}
				if err := service.Client.Get(ctx, key, object); err != nil {
					t.Fatal(err)
				}
				if err := service.Client.Delete(ctx, object); err != nil {
					t.Fatal(err)
				}
				replacement := object.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = replacementGatewayUID
				if err := service.Client.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Binding",
			replace: func(t *testing.T, service *Service, ctx context.Context) {
				t.Helper()
				object := &gatewayv1alpha1.GatewayBinding{}
				key := client.ObjectKey{Namespace: "default", Name: "room"}
				if err := service.Client.Get(ctx, key, object); err != nil {
					t.Fatal(err)
				}
				object.Generation++
				if err := service.Client.Update(ctx, object); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "Agent",
			replace: func(t *testing.T, service *Service, ctx context.Context) {
				t.Helper()
				object := &corev1alpha1.Agent{}
				key := client.ObjectKey{Namespace: "default", Name: "assistant"}
				if err := service.Client.Get(ctx, key, object); err != nil {
					t.Fatal(err)
				}
				if err := service.Client.Delete(ctx, object); err != nil {
					t.Fatal(err)
				}
				replacement := object.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = "replacement-agent-uid"
				if err := service.Client.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service, sqliteStore, _ := newGatewayServiceFixture(t)
			service.Config.EventExpiry = 1100 * time.Millisecond
			service.Config.ClaimLease = time.Millisecond
			ctx := context.Background()
			accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "dispatch-recreated-"+strings.ToLower(testCase.name), "user-1"))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			claimed, err := sqliteStore.ClaimNextGatewayEvent(ctx, "", "crashed-owner", now, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			binding := &gatewayv1alpha1.GatewayBinding{}
			if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: claimed.BindingName}, binding); err != nil {
				t.Fatal(err)
			}
			if err := service.Client.Create(ctx, taskForGatewayEvent(claimed, binding, now)); err != nil {
				t.Fatal(err)
			}
			testCase.replace(t, service, ctx)
			time.Sleep(1200 * time.Millisecond)
			if err := service.DispatchOnce(ctx); err != nil {
				t.Fatal(err)
			}
			event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
			if err != nil || event.State != store.GatewayEventExpired || event.TaskUID != "" {
				t.Fatalf("event after %s replacement = (%+v, %v)", testCase.name, event, err)
			}
			remaining := &corev1alpha1.Task{}
			err = service.Client.Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, remaining)
			if !apierrors.IsNotFound(err) {
				t.Fatalf("unlinked Task after %s replacement = (%+v, %v), want deleted", testCase.name, remaining, err)
			}
		})
	}
}

type delayFirstGatewayRead struct {
	client.Reader
	delay   time.Duration
	delayed bool
}

func (r *delayFirstGatewayRead) Get(
	ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption,
) error {
	if _, ok := object.(*gatewayv1alpha1.Gateway); ok && !r.delayed {
		r.delayed = true
		time.Sleep(r.delay)
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestExpiredDispatchClaimUsesTimeAfterDependencyResolution(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 2 * time.Second
	service.Config.ClaimLease = time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "dispatch-resolution-window", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claimed, err := sqliteStore.ClaimNextGatewayEvent(ctx, "", "crashed-owner", now, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	binding := &gatewayv1alpha1.GatewayBinding{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: claimed.BindingName}, binding); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Create(ctx, taskForGatewayEvent(claimed, binding, now)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	service.APIReader = &delayFirstGatewayRead{Reader: service.Client, delay: 1200 * time.Millisecond}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated || event.TaskUID == "" {
		t.Fatalf("event after dependency resolution crossed execution window = (%+v, %v)", event, err)
	}
}

func TestTerminalResultIsProjectedAfterEventDeadline(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 1100 * time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "late-projection", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(ctx, "default", task.Name, []byte("completed before deadline")); err != nil {
		t.Fatal(err)
	}
	completed := metav1.Now()
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	task.Status.CompletionTime = &completed
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventCompleted {
		t.Fatalf("late-projected event = (%+v, %v)", event, err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil || len(session.Messages) != 2 || session.Messages[1].Content != "completed before deadline" {
		t.Fatalf("late-projected Session = (%+v, %v)", session, err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() after late projection = %v", err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Text != "completed before deadline" {
		t.Fatalf("late-projected adapter deliveries = %#v", deliveries)
	}
}

func TestSucceededTaskWithoutResultDoesNotBlockSession(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	first, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "missing-result-first", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "missing-result-second", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	firstEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", first.EventID)
	if err != nil {
		t.Fatal(err)
	}
	firstTask := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: firstEvent.TaskName}, firstTask); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Now().Add(-gatewayResultProjectionGrace - time.Second))
	firstTask.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	firstTask.Status.CompletionTime = &completed
	if err := service.Client.Status().Update(ctx, firstTask); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("missing-result deliveries = %#v", deliveries)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	secondEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", second.EventID)
	if err != nil || secondEvent.State != store.GatewayEventTaskCreated {
		t.Fatalf("second event after missing result = (%+v, %v)", secondEvent, err)
	}
}

func TestSucceededTaskWithoutResultRefUsesPersistedResult(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "result-without-ref", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(ctx, "default", event.TaskName, []byte("persisted despite missing ref")); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Now().Add(-gatewayResultProjectionGrace - time.Second))
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.CompletionTime = &completed
	task.Status.ResultRef = nil
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindFinal || deliveries[0].Text != "persisted despite missing ref" {
		t.Fatalf("persisted-result deliveries = %#v", deliveries)
	}
}

func TestExpiredQueuedEventsDeliverInAdmissionOrder(t *testing.T) {
	service, _, adapter := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 25 * time.Millisecond
	service.Config.BatchSize = 2
	ctx := context.Background()
	first, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "expiry-order-first", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "expiry-order-second", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := service.ExpireQueuedEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.ExpireQueuedEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	deliveries := adapter.Deliveries()
	if len(deliveries) != 2 || deliveries[0].OriginatingEvent != first.EventID || deliveries[1].OriginatingEvent != second.EventID {
		t.Fatalf("expiry delivery order = %#v", deliveries)
	}
}

func TestRepairExpiredEventsCreatesVisibleDelivery(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "repair-expired", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	const reason = "The message expired before execution could start."
	if err := sqliteStore.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", reason, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := service.RepairExpiredEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError || deliveries[0].Text != reason {
		t.Fatalf("repaired deliveries = %#v", deliveries)
	}
}

func TestExpiredMissingDeliveryBlocksLaterSessionExpiryUntilRepaired(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 25 * time.Millisecond
	service.Config.BatchSize = 2
	ctx := context.Background()
	first, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "repair-order-first", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "repair-order-second", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	firstEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", first.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.ExpireGatewayEvent(ctx, firstEvent.Namespace, firstEvent.ID, "", "legacy expiry", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := service.ExpireQueuedEvents(ctx); err != nil {
		t.Fatal(err)
	}
	secondEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", second.EventID)
	if err != nil || secondEvent.State != store.GatewayEventQueued {
		t.Fatalf("later event before repair = (%+v, %v), want Queued", secondEvent, err)
	}
	if err := service.RepairExpiredEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.ExpireQueuedEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	deliveries := adapter.Deliveries()
	if len(deliveries) != 2 || deliveries[0].OriginatingEvent != first.EventID || deliveries[1].OriginatingEvent != second.EventID {
		t.Fatalf("repair delivery order = %#v", deliveries)
	}
}

func TestGatewayDeliveryExpiryIncludesStaleClaimRecoveryWindow(t *testing.T) {
	now := time.Now().UTC()
	got := gatewayDeliveryExpiresAt(now.Add(time.Second), now, Config{
		EventExpiry: time.Second,
		ClaimLease:  time.Minute,
	})
	if got.Sub(now) < 2*time.Minute {
		t.Fatalf("delivery window = %s, want at least %s", got.Sub(now), 2*time.Minute)
	}
}

func TestSucceededTaskResultGraceSurvivesEventDeadline(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 1100 * time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "result-grace", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	completed := metav1.Now()
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.CompletionTime = &completed
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated {
		t.Fatalf("event during result grace = (%+v, %v)", event, err)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: "default", Name: event.TaskName}, task); err != nil {
		t.Fatalf("Task was removed during result grace: %v", err)
	}
	completed = metav1.NewTime(time.Now().Add(-gatewayResultProjectionGrace - time.Second))
	task.Status.CompletionTime = &completed
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("result-grace deliveries = %#v", deliveries)
	}
}

func TestMatchingUnreadyBindingIsRetryableNotDurablyRejected(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	binding := &gatewayv1alpha1.GatewayBinding{}
	key := client.ObjectKey{Namespace: "default", Name: "room"}
	if err := service.Client.Get(ctx, key, binding); err != nil {
		t.Fatal(err)
	}
	binding.Status.Ready = false
	if err := service.Client.Status().Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	body := gatewayEventBody(t, "temporarily-unready", "user-1")
	if _, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body); err == nil {
		t.Fatal("AdmitEvent() unexpectedly accepted an unready binding")
	} else {
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.Code != http.StatusServiceUnavailable {
			t.Fatalf("AdmitEvent() error = %v, want HTTP 503", err)
		}
	}
	if err := service.Client.Get(ctx, key, binding); err != nil {
		t.Fatal(err)
	}
	binding.Status.Ready = true
	binding.Status.ObservedGeneration = binding.Generation
	if err := service.Client.Status().Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", body)
	if err != nil || response.Status != ingressStatusAccepted {
		t.Fatalf("AdmitEvent() after readiness = (%+v, %v)", response, err)
	}
}

func TestReadyBindingWinsOverHigherPriorityUnreadyBinding(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	unready := &gatewayv1alpha1.GatewayBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "future-room", Namespace: "default", UID: "future-binding", Generation: 1},
		Spec: gatewayv1alpha1.GatewayBindingSpec{
			GatewayRef: gatewayv1alpha1.GatewayBindingReference{Name: "chat"},
			AgentRef:   gatewayv1alpha1.GatewayBindingReference{Name: "assistant"},
			Match:      gatewayv1alpha1.GatewayBindingMatch{AccountID: "acct", ContextID: "room"},
			SenderPolicy: gatewayv1alpha1.GatewaySenderPolicy{
				Mode: gatewayv1alpha1.GatewaySenderPolicyAllowlist, AllowedSenderIDs: []string{"user-1"},
			},
			Priority: 500,
		},
		Status: gatewayv1alpha1.GatewayBindingStatus{Ready: false, ObservedGeneration: 1},
	}
	if err := service.Client.Create(ctx, unready); err != nil {
		t.Fatal(err)
	}
	response, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "ready-wins", "user-1"))
	if err != nil || response.Status != ingressStatusAccepted {
		t.Fatalf("AdmitEvent() = (%+v, %v), want ready binding acceptance", response, err)
	}
}

func TestProcessDeliveryBatchHandlesIndependentSessionsConcurrently(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.Config.BatchSize = 2
	ctx := context.Background()
	now := time.Now().UTC()
	for i, sessionName := range []string{"session-a", "session-b"} {
		id := fmt.Sprintf("gdl-batch-%d", i)
		if _, _, err := sqliteStore.CreateGatewayDelivery(ctx, &store.GatewayDelivery{
			ID: id, IdempotencyID: id, Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
			EventID: "event-" + id, SessionName: sessionName, Kind: protocol.DeliveryKindFinal,
			AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: id,
			MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.processDeliveryBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if len(adapter.Deliveries()) != 2 {
		t.Fatalf("batch delivered %d records, want 2", len(adapter.Deliveries()))
	}
}

func TestTerminalProjectionRejectsReplacementTaskUID(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "task-replacement", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.TaskUID == "" {
		t.Fatalf("linked event = (%+v, %v)", event, err)
	}
	original := &corev1alpha1.Task{}
	key := client.ObjectKey{Namespace: "default", Name: event.TaskName}
	if err := service.Client.Get(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Delete(ctx, original); err != nil {
		t.Fatal(err)
	}
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = "replacement-task-uid"
	replacement.Status = corev1alpha1.TaskStatus{
		Phase:     corev1alpha1.TaskPhaseSucceeded,
		ResultRef: &corev1alpha1.ResultReference{Available: true},
	}
	if err := service.Client.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(ctx, "default", replacement.Name, []byte("replacement result")); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err == nil {
		t.Fatal("ProjectTerminals() accepted replacement Task UID")
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated {
		t.Fatalf("event after replacement = (%+v, %v)", event, err)
	}
	session, err := sqliteStore.GetSession(ctx, "default", event.SessionName)
	if err != nil || len(session.Messages) != 1 {
		t.Fatalf("replacement result entered Session: (%+v, %v)", session, err)
	}
}

func TestReplacementTaskIsNotDeletedWhenEventExpires(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Config.EventExpiry = 1100 * time.Millisecond
	service.Config.PollInterval = time.Millisecond
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "replacement-expiry", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	original := &corev1alpha1.Task{}
	key := client.ObjectKey{Namespace: "default", Name: event.TaskName}
	if err := service.Client.Get(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	if err := service.Client.Delete(ctx, original); err != nil {
		t.Fatal(err)
	}
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = "foreign-task-uid"
	if err := service.Client.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	event, err = sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventExpired {
		t.Fatalf("expired identity-conflict event = (%+v, %v)", event, err)
	}
	stillPresent := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, key, stillPresent); err != nil || stillPresent.UID != replacement.UID {
		t.Fatalf("foreign replacement Task was deleted or changed: (%+v, %v)", stillPresent, err)
	}
}

func TestIngressRejectsStaleGatewayClassStatus(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	class := &gatewayv1alpha1.GatewayClass{}
	if err := service.Client.Get(ctx, client.ObjectKey{Name: "generic-chat"}, class); err != nil {
		t.Fatal(err)
	}
	class.Generation = 2
	class.Status.Accepted = true
	class.Status.ObservedGeneration = 1
	if err := service.Client.Update(ctx, class); err != nil {
		t.Fatal(err)
	}
	_, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "stale-class", "user-1"))
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != 503 {
		t.Fatalf("AdmitEvent() error = %v, want stale-class 503", err)
	}
}

type failingGatewayBindingListClient struct{ client.Client }

func (c *failingGatewayBindingListClient) List(
	ctx context.Context, list client.ObjectList, opts ...client.ListOption,
) error {
	if _, ok := list.(*gatewayv1alpha1.GatewayBindingList); ok {
		return errors.New("injected binding list failure")
	}
	return c.Client.List(ctx, list, opts...)
}

type taskMissingClient struct{ client.Client }

func (c *taskMissingClient) Get(
	ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption,
) error {
	if _, ok := object.(*corev1alpha1.Task); ok {
		return apierrors.NewNotFound(corev1alpha1.GroupVersion.WithResource("tasks").GroupResource(), key.Name)
	}
	return c.Client.Get(ctx, key, object, opts...)
}

type secretOverrideReader struct {
	client.Reader
	tokens   map[string]string
	versions map[string]string
}

func (r *secretOverrideReader) Get(
	ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption,
) error {
	if err := r.Reader.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	secret, ok := object.(*corev1.Secret)
	if !ok {
		return nil
	}
	token, ok := r.tokens[key.Name]
	if !ok {
		return nil
	}
	data := make(map[string][]byte, len(secret.Data)+1)
	for name, value := range secret.Data {
		data[name] = append([]byte(nil), value...)
	}
	data["token"] = []byte(token)
	secret.Data = data
	if version := r.versions[key.Name]; version != "" {
		secret.ResourceVersion = version
	}
	return nil
}

func TestIngressUsesFreshRotatedBearerSecret(t *testing.T) {
	service, _, _ := newGatewayServiceFixture(t)
	service.APIReader = &secretOverrideReader{
		Reader: service.Client,
		tokens: map[string]string{"inbound": "test-auth-token"},
	}
	body := gatewayEventBody(t, "rotated-inbound", "user-1")
	_, err := service.AdmitEvent(context.Background(), "default", "chat", "Bearer inbound-token", body)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != http.StatusUnauthorized {
		t.Fatalf("revoked bearer error = %v, want HTTP 401", err)
	}
	response, err := service.AdmitEvent(context.Background(), "default", "chat", "Bearer test-auth-token", body)
	if err != nil || response.Status != ingressStatusAccepted {
		t.Fatalf("rotated bearer AdmitEvent() = (%+v, %v)", response, err)
	}
}

func TestDeliveryUsesFreshRotatedBearerSecret(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.APIReader = &secretOverrideReader{
		Reader: service.Client,
		tokens: map[string]string{"outbound": "gateway-token"},
	}
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if got := request.Header.Get("Authorization"); got != "Bearer gateway-token" {
			t.Fatalf("Authorization = %q, want rotated outbound bearer", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"status":"delivered","providerMessageId":"rotated-provider-id"}`,
			)),
		}, nil
	})}
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-rotated-secret", IdempotencyID: "gdl-rotated-secret", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event-rotated-secret",
		Kind: protocol.DeliveryKindFinal, AccountID: "acct", ContextID: "room", ReplyTarget: "room",
		Text: "rotated secret", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := sqliteStore.GetGatewayDelivery(context.Background(), delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryDelivered || stored.ProviderMessageID != "rotated-provider-id" {
		t.Fatalf("delivery after rotation = (%+v, %v)", stored, err)
	}
}

func TestDeliveryWaitsForCurrentOutboundSecretProbe(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.APIReader = &secretOverrideReader{
		Reader:   service.Client,
		tokens:   map[string]string{"outbound": "gateway-token"},
		versions: map[string]string{"outbound": "2"},
	}
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-unprobed-secret", IdempotencyID: "gdl-unprobed-secret", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event-unprobed-secret",
		Kind: protocol.DeliveryKindFinal, AccountID: "acct", ContextID: "room", ReplyTarget: "room",
		Text: "wait for probe", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, err := sqliteStore.GetGatewayDelivery(context.Background(), delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryRetryScheduled || !strings.Contains(stored.LastError, "has not been probed") {
		t.Fatalf("unprobed Secret delivery = (%+v, %v)", stored, err)
	}
	if len(adapter.Deliveries()) != 0 {
		t.Fatal("delivery used an outbound Secret before its probe completed")
	}
}

type conflictMarkGatewayEventStore struct {
	store.GatewayEventStore
}

func (s conflictMarkGatewayEventStore) MarkGatewayEventTaskCreated(
	context.Context, string, string, string, string, string, time.Time,
) error {
	return store.ErrConflict
}

type commitTaskThenErrorClient struct {
	client.Client
	failed bool
}

func (c *commitTaskThenErrorClient) Create(
	ctx context.Context, object client.Object, opts ...client.CreateOption,
) error {
	if _, ok := object.(*corev1alpha1.Task); ok && !c.failed {
		c.failed = true
		if err := c.Client.Create(ctx, object, opts...); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	return c.Client.Create(ctx, object, opts...)
}

type invalidTaskCreateOnceClient struct {
	client.Client
	failed bool
}

func (c *invalidTaskCreateOnceClient) Create(
	ctx context.Context, object client.Object, opts ...client.CreateOption,
) error {
	if task, ok := object.(*corev1alpha1.Task); ok && !c.failed {
		c.failed = true
		return apierrors.NewInvalid(
			corev1alpha1.GroupVersion.WithKind("Task").GroupKind(), task.Name,
			field.ErrorList{field.Invalid(field.NewPath("spec", "prompt"), task.Spec.Prompt, "invalid fixture")},
		)
	}
	return c.Client.Create(ctx, object, opts...)
}

type failTaskCreateClient struct{ client.Client }

func (c *failTaskCreateClient) Create(
	ctx context.Context, object client.Object, opts ...client.CreateOption,
) error {
	if _, ok := object.(*corev1alpha1.Task); ok {
		return context.DeadlineExceeded
	}
	return c.Client.Create(ctx, object, opts...)
}

type deleteTaskBeforePatchClient struct {
	client.Client
	deleted bool
}

func (c *deleteTaskBeforePatchClient) Patch(
	ctx context.Context, object client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	task, ok := object.(*corev1alpha1.Task)
	if !ok || c.deleted {
		return c.Client.Patch(ctx, object, patch, opts...)
	}
	c.deleted = true
	current := &corev1alpha1.Task{}
	key := client.ObjectKeyFromObject(task)
	if err := c.Get(ctx, key, current); err != nil {
		return err
	}
	if err := c.Delete(ctx, current); err != nil {
		return err
	}
	return apierrors.NewNotFound(corev1alpha1.GroupVersion.WithResource("tasks").GroupResource(), task.Name)
}

type replaceTaskBeforePatchClient struct {
	client.Client
	patchCount     int
	replaced       bool
	optimisticLock bool
}

func (c *replaceTaskBeforePatchClient) Patch(
	ctx context.Context, object client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	task, ok := object.(*corev1alpha1.Task)
	if !ok || c.replaced {
		return c.Client.Patch(ctx, object, patch, opts...)
	}
	c.patchCount++
	if c.patchCount == 1 {
		return apierrors.NewConflict(
			corev1alpha1.GroupVersion.WithResource("tasks").GroupResource(), task.Name, errors.New("concurrent Task update"),
		)
	}
	current := &corev1alpha1.Task{}
	key := client.ObjectKeyFromObject(task)
	if err := c.Get(ctx, key, current); err != nil {
		return err
	}
	patchData, err := patch.Data(task)
	if err != nil {
		return err
	}
	var patchDocument struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patchData, &patchDocument); err != nil {
		return err
	}
	c.optimisticLock = patchDocument.Metadata.ResourceVersion != "" &&
		patchDocument.Metadata.ResourceVersion == current.ResourceVersion
	if err := c.Delete(ctx, current); err != nil {
		return err
	}
	replacement := current.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = types.UID("replacement-" + task.Name)
	replacement.Status = corev1alpha1.TaskStatus{}
	if err := c.Create(ctx, replacement); err != nil {
		return err
	}
	c.replaced = true
	if !c.optimisticLock {
		replacement.Annotations = make(map[string]string, len(task.Annotations))
		maps.Copy(replacement.Annotations, task.Annotations)
		return c.Update(ctx, replacement)
	}
	return apierrors.NewConflict(
		corev1alpha1.GroupVersion.WithResource("tasks").GroupResource(), task.Name, errors.New("Task was replaced"),
	)
}

type deleteOptionsCaptureClient struct {
	client.Client
	preconditionUID types.UID
}

func (c *deleteOptionsCaptureClient) Delete(
	ctx context.Context, object client.Object, opts ...client.DeleteOption,
) error {
	options := (&client.DeleteOptions{}).ApplyOptions(opts)
	if options.Preconditions != nil && options.Preconditions.UID != nil {
		c.preconditionUID = *options.Preconditions.UID
	}
	return c.Client.Delete(ctx, object, opts...)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestDefinitiveTaskCreateFailureExpiresEventAndUnblocksSession(t *testing.T) {
	service, sqliteStore, adapter := newGatewayServiceFixture(t)
	service.APIReader = service.Client
	service.Client = &invalidTaskCreateOnceClient{Client: service.Client}
	ctx := context.Background()
	first, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "invalid-task", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "after-invalid-task", "user-1"))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("first DispatchOnce() error = %v", err)
	}
	firstEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", first.EventID)
	if err != nil || firstEvent.State != store.GatewayEventExpired || !strings.Contains(firstEvent.StateMessage, "configured task is invalid") {
		t.Fatalf("first event = (%+v, %v), want terminal invalid-task expiry", firstEvent, err)
	}
	projected, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{Namespace: "default", EventID: firstEvent.ID})
	if err != nil || len(projected) != 1 {
		t.Fatalf("invalid-task deliveries = (%#v, %v), want one", projected, err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries := adapter.Deliveries(); len(deliveries) != 1 || deliveries[0].Kind != protocol.DeliveryKindError {
		t.Fatalf("invalid-task adapter deliveries = %#v", deliveries)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("second DispatchOnce() error = %v", err)
	}
	secondEvent, err := sqliteStore.GetGatewayEvent(ctx, "default", second.EventID)
	if err != nil || secondEvent.State != store.GatewayEventTaskCreated {
		t.Fatalf("second event = (%+v, %v), want TaskCreated", secondEvent, err)
	}
}

func TestDeleteGatewayTaskWithUIDUsesPrecondition(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Name: "delete-with-uid", Namespace: "default", UID: types.UID("task-uid"),
	}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).Build()
	capture := &deleteOptionsCaptureClient{Client: base}
	if err := deleteGatewayTaskWithUID(context.Background(), capture, task); err != nil {
		t.Fatal(err)
	}
	if capture.preconditionUID != task.UID {
		t.Fatalf("delete UID precondition = %q, want %q", capture.preconditionUID, task.UID)
	}
}

func TestLinkGatewayTaskLeavesTaskAfterClaimLoss(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.EventStore = conflictMarkGatewayEventStore{GatewayEventStore: sqliteStore}
	ctx := context.Background()
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Name: "claim-loss-task", Namespace: "default", UID: types.UID("claim-loss-uid"),
	}}
	if err := service.Client.Create(ctx, task); err != nil {
		t.Fatal(err)
	}
	event := &store.GatewayEvent{ID: "claim-loss-event", Namespace: "default", TaskName: task.Name}
	if err := service.linkGatewayTask(ctx, event, task, time.Now().UTC()); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("linkGatewayTask() error = %v, want ErrConflict", err)
	}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Name}, &corev1alpha1.Task{}); err != nil {
		t.Fatalf("Task was deleted after claim loss: %v", err)
	}
}

func TestAmbiguousTaskCreateLinksCommittedTask(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	freshReader := service.Client
	service.APIReader = freshReader
	service.Client = &commitTaskThenErrorClient{Client: service.Client}
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "ambiguous-create-committed", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatalf("DispatchOnce() error = %v", err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated || event.TaskUID == "" {
		t.Fatalf("event after ambiguous committed create = (%+v, %v)", event, err)
	}
}

func TestAmbiguousTaskCreateKeepsDispatchClaim(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.APIReader = service.Client
	service.Client = &failTaskCreateClient{Client: service.Client}
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "ambiguous-create-missing", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err == nil {
		t.Fatal("DispatchOnce() unexpectedly resolved an ambiguous missing Task")
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventDispatching || event.ClaimOwner != service.Owner {
		t.Fatalf("event after ambiguous missing create = (%+v, %v), want retained Dispatching claim", event, err)
	}
}

func TestDeliveredResultDoesNotPatchMissingOrReplacementTask(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		createReplacement bool
	}{
		{name: "missing"},
		{name: "replacement", createReplacement: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service, sqliteStore, _ := newGatewayServiceFixture(t)
			ctx := context.Background()
			accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "delivery-task-"+testCase.name, "user-1"))
			if err != nil {
				t.Fatal(err)
			}
			if err := service.DispatchOnce(ctx); err != nil {
				t.Fatal(err)
			}
			event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
			if err != nil {
				t.Fatal(err)
			}
			task := &corev1alpha1.Task{}
			key := client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}
			if err := service.Client.Get(ctx, key, task); err != nil {
				t.Fatal(err)
			}
			if err := sqliteStore.SaveResult(ctx, task.Namespace, task.Name, []byte("delivered response")); err != nil {
				t.Fatal(err)
			}
			task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
			task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
			if err := service.Client.Status().Update(ctx, task); err != nil {
				t.Fatal(err)
			}
			if err := service.ProjectTerminals(ctx); err != nil {
				t.Fatal(err)
			}
			deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
				Namespace: event.Namespace, EventID: event.ID,
			})
			if err != nil || len(deliveries) != 1 {
				t.Fatalf("projected deliveries = (%#v, %v)", deliveries, err)
			}
			if err := service.Client.Delete(ctx, task); err != nil {
				t.Fatal(err)
			}
			if testCase.createReplacement {
				replacement := task.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = types.UID("replacement-" + task.Name)
				replacement.Status = corev1alpha1.TaskStatus{}
				if err := service.Client.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if err := service.DeliverOnce(ctx); err != nil {
				t.Fatal(err)
			}
			delivery, err := sqliteStore.GetGatewayDelivery(ctx, event.Namespace, deliveries[0].ID)
			if err != nil || delivery.State != store.GatewayDeliveryDelivered {
				t.Fatalf("delivery after Task %s = (%+v, %v)", testCase.name, delivery, err)
			}
			if testCase.createReplacement {
				replacement := &corev1alpha1.Task{}
				if err := service.Client.Get(ctx, key, replacement); err != nil {
					t.Fatal(err)
				}
				if replacement.Annotations[TaskGatewayDelivery] != "" || replacement.Annotations[TaskGatewayProviderMessage] != "" {
					t.Fatalf("replacement Task received delivery correlation: %#v", replacement.Annotations)
				}
			}
		})
	}
}

func TestDeliveredResultCommitsWhenTaskDisappearsDuringCorrelationPatch(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "delivery-task-patch-race", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	if err := service.Client.Get(ctx, client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}, task); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(ctx, task.Namespace, task.Name, []byte("delivered response")); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
		Namespace: event.Namespace, EventID: event.ID,
	})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("projected deliveries = (%#v, %v)", deliveries, err)
	}
	underlying := service.Client
	service.APIReader = underlying
	racingClient := &deleteTaskBeforePatchClient{Client: underlying}
	service.Client = racingClient
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !racingClient.deleted {
		t.Fatal("Task was not deleted during the correlation patch race")
	}
	delivery, err := sqliteStore.GetGatewayDelivery(ctx, event.Namespace, deliveries[0].ID)
	if err != nil || delivery.State != store.GatewayDeliveryDelivered {
		t.Fatalf("delivery after patch-time Task deletion = (%+v, %v)", delivery, err)
	}
}

func TestDeliveredResultCommitsWhenTaskIsReplacedDuringCorrelationPatch(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "delivery-task-patch-conflict", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{}
	key := client.ObjectKey{Namespace: event.Namespace, Name: event.TaskName}
	if err := service.Client.Get(ctx, key, task); err != nil {
		t.Fatal(err)
	}
	if err := sqliteStore.SaveResult(ctx, task.Namespace, task.Name, []byte("delivered response")); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = corev1alpha1.TaskPhaseSucceeded
	task.Status.ResultRef = &corev1alpha1.ResultReference{Available: true}
	if err := service.Client.Status().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatal(err)
	}
	deliveries, err := sqliteStore.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
		Namespace: event.Namespace, EventID: event.ID,
	})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("projected deliveries = (%#v, %v)", deliveries, err)
	}
	underlying := service.Client
	service.APIReader = underlying
	racingClient := &replaceTaskBeforePatchClient{Client: underlying}
	service.Client = racingClient
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !racingClient.replaced || racingClient.patchCount != 2 {
		t.Fatalf("Task replacement race = (replaced=%v, patchCount=%d), want replacement on final patch", racingClient.replaced, racingClient.patchCount)
	}
	if !racingClient.optimisticLock {
		t.Fatal("correlation patch did not include the original Task resourceVersion")
	}
	delivery, err := sqliteStore.GetGatewayDelivery(ctx, event.Namespace, deliveries[0].ID)
	if err != nil || delivery.State != store.GatewayDeliveryDelivered {
		t.Fatalf("delivery after patch-time Task replacement = (%+v, %v)", delivery, err)
	}
	replacement := &corev1alpha1.Task{}
	if err := underlying.Get(ctx, key, replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.Annotations[TaskGatewayDelivery] != "" || replacement.Annotations[TaskGatewayProviderMessage] != "" {
		t.Fatalf("replacement Task received delivery correlation: %#v", replacement.Annotations)
	}
}

func TestHTTP408DeliveryIsRetried(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	var respondedAt time.Time
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		time.Sleep(75 * time.Millisecond)
		respondedAt = time.Now().UTC()
		return &http.Response{
			StatusCode: http.StatusRequestTimeout,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"error":"timeout"}`)),
		}, nil
	})}
	ctx := context.Background()
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "gdl-http-408", IdempotencyID: "gdl-http-408", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", BindingName: "room",
		EventID: "event-http-408", SessionName: "session-http-408", Kind: protocol.DeliveryKindFinal,
		AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: "reply",
		MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if err := service.DeliverOnce(ctx); err != nil {
		t.Fatalf("DeliverOnce() error = %v", err)
	}
	stored, err := sqliteStore.GetGatewayDelivery(ctx, delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryRetryScheduled || stored.AttemptCount != 1 {
		t.Fatalf("HTTP 408 delivery = (%+v, %v), want RetryScheduled attempt 1", stored, err)
	}
	if stored.UpdatedAt.Before(respondedAt) || !stored.NextAttemptAt.After(respondedAt) {
		t.Fatalf("HTTP 408 timestamps = updated %s next %s, want after response %s", stored.UpdatedAt, stored.NextAttemptAt, respondedAt)
	}
}

func TestTerminalProjectionUsesUncachedTaskReader(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	ctx := context.Background()
	accepted, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "fresh-task-read", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	freshReader := service.Client
	service.APIReader = freshReader
	service.Client = &taskMissingClient{Client: service.Client}
	if err := service.ProjectTerminals(ctx); err != nil {
		t.Fatalf("ProjectTerminals() error = %v", err)
	}
	event, err := sqliteStore.GetGatewayEvent(ctx, "default", accepted.EventID)
	if err != nil || event.State != store.GatewayEventTaskCreated {
		t.Fatalf("event after cache-stale projection = (%+v, %v), want TaskCreated", event, err)
	}
}

func TestBindingLookupFailureIsRetryableAndNotPersisted(t *testing.T) {
	service, sqliteStore, _ := newGatewayServiceFixture(t)
	service.Client = &failingGatewayBindingListClient{Client: service.Client}
	ctx := context.Background()
	_, err := service.AdmitEvent(ctx, "default", "chat", "Bearer inbound-token", gatewayEventBody(t, "binding-list-failure", "user-1"))
	if err == nil {
		t.Fatal("AdmitEvent() unexpectedly durably rejected a binding lookup failure")
	}
	events, listErr := sqliteStore.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default"})
	if listErr != nil || len(events) != 0 {
		t.Fatalf("persisted events after lookup failure = (%#v, %v)", events, listErr)
	}
}
