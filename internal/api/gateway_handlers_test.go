package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/gateway/protocol"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const gatewayResourceName = "gateways"

func TestGatewayIngressUsesGatewayAuthRoute(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	server := NewServer(fake.NewClientBuilder().WithScheme(scheme).Build(), nil, ServerConfig{})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/gateways/default/chat/events", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := server.app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 from gateway route rather than external-auth 401", response.StatusCode)
	}
}

func TestGatewayIngressRejectsWatchNamespaceMismatch(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		WatchNamespace: "allowed",
		GatewayService: &gatewayruntime.Service{Config: gatewayruntime.DefaultConfig()},
	})
	app := fiber.New()
	app.Post("/api/v1/gateways/:namespace/:name/events", h.HandleGatewayEvent)
	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/api/v1/gateways/other/chat/events", strings.NewReader(`{}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for watch-namespace mismatch", response.StatusCode)
	}
}

func TestGatewayIngressRateLimitsBeforeServiceAdmission(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		GatewayService: &gatewayruntime.Service{Config: gatewayruntime.Config{Enabled: false}},
	})
	h.gatewayIngressLimiter = newGatewayIngressLimiterWithLimits(100, 10, 1, 1, 10, time.Minute)
	app := fiber.New()
	app.Post("/api/v1/gateways/:namespace/:name/events", h.HandleGatewayEvent)
	request := func() *http.Response {
		response, err := app.Test(httptest.NewRequest(
			http.MethodPost, "/api/v1/gateways/default/chat/events", strings.NewReader(`{}`),
		))
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	if response := request(); response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d, want service 503 after limiter allowance", response.StatusCode)
	}
	if response := request(); response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want pre-admission 429", response.StatusCode)
	}
}

func TestGatewayIngressRateLimitCannotRotateGatewayNames(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		GatewayService: &gatewayruntime.Service{Config: gatewayruntime.Config{Enabled: false}},
	})
	h.gatewayIngressLimiter = newGatewayIngressLimiterWithLimits(100, 10, 1, 1, 10, time.Minute)
	app := fiber.New()
	app.Post("/api/v1/gateways/:namespace/:name/events", h.HandleGatewayEvent)
	for i, name := range []string{"chat-a", "chat-b"} {
		response, err := app.Test(httptest.NewRequest(
			http.MethodPost, "/api/v1/gateways/default/"+name+"/events", strings.NewReader(`{}`),
		))
		if err != nil {
			t.Fatal(err)
		}
		want := http.StatusServiceUnavailable
		if i == 1 {
			want = http.StatusTooManyRequests
		}
		if response.StatusCode != want {
			t.Fatalf("request %d status = %d, want %d", i+1, response.StatusCode, want)
		}
	}
}

func TestGatewayIngressRejectsOversizedBodyAtTransport(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		GatewayService: &gatewayruntime.Service{Config: gatewayruntime.Config{Enabled: false}},
	})
	app := fiber.New(fiber.Config{BodyLimit: 15 << 20})
	app.Server().HeaderReceived = requestBodyConfig
	app.Post("/api/v1/gateways/:namespace/:name/events", h.HandleGatewayEvent)
	request := httptest.NewRequest(
		http.MethodPost,
		"/API/V1/GATEWAYS/default/chat/EVENTS",
		bytes.NewReader(bytes.Repeat([]byte{'x'}, protocol.MaxHTTPBodyBytes+1)),
	)
	response, err := app.Test(request)
	if errors.Is(err, fasthttp.ErrBodyTooLarge) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.StatusCode)
	}
}

func TestGatewayRequestBodyLimitPreservesEscapedSlash(t *testing.T) {
	header := &fasthttp.RequestHeader{}
	header.SetMethod(http.MethodPost)
	header.SetRequestURI("/api/v1/gateways/default/x%2Fy/events")
	config := requestBodyConfig(header)
	if config.MaxRequestBodySize != protocol.MaxHTTPBodyBytes {
		t.Fatalf("MaxRequestBodySize = %d, want %d", config.MaxRequestBodySize, protocol.MaxHTTPBodyBytes)
	}
}

func TestGatewayRequestBodyLimitMatchesAbsoluteFormURI(t *testing.T) {
	header := &fasthttp.RequestHeader{}
	header.SetMethod(http.MethodPost)
	header.SetRequestURI("http://example.com/API/V1/GATEWAYS/default/chat/EVENTS?source=test")
	config := requestBodyConfig(header)
	if config.MaxRequestBodySize != protocol.MaxHTTPBodyBytes {
		t.Fatalf("MaxRequestBodySize = %d, want %d", config.MaxRequestBodySize, protocol.MaxHTTPBodyBytes)
	}
}

type listFailingClient struct{ client.Client }

func (c listFailingClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("cached list must not be used")
}

func TestGatewayResourceListsUseAPIReader(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&gatewayv1alpha1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "class"}},
		&gatewayv1alpha1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default"}},
		&gatewayv1alpha1.GatewayBinding{ObjectMeta: metav1.ObjectMeta{Name: "room", Namespace: "default"}},
	).Build()
	cached := listFailingClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	h := NewHandlers(HandlersConfig{Client: cached, APIReader: reader})
	app := fiber.New()
	app.Get("/gatewayclasses", h.ListGatewayClasses)
	app.Get("/gateways", h.ListGateways)
	app.Get("/gatewaybindings", h.ListGatewayBindings)
	for _, target := range []string{
		"/gatewayclasses?limit=1",
		"/gateways?namespace=default&limit=1",
		"/gatewaybindings?namespace=default&limit=1",
	} {
		response, err := app.Test(httptest.NewRequest(http.MethodGet, target, nil))
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", target, response.StatusCode)
		}
	}
}

// continueRejectingReader answers every list the way the API server answers
// an unusable continue token.
type continueRejectingReader struct {
	client.Reader
	err error
}

func (r continueRejectingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return r.err
}

func TestGatewayResourceListsPreserveContinueTokenErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"expired", apierrors.NewResourceExpired("too old resource version"), http.StatusGone},
		{"malformed", apierrors.NewBadRequest("invalid continue token"), http.StatusBadRequest},
		{"other", errors.New("boom"), http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := continueRejectingReader{Reader: fake.NewClientBuilder().WithScheme(scheme).Build(), err: tc.err}
			h := NewHandlers(HandlersConfig{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), APIReader: reader})
			app := fiber.New()
			app.Get("/gatewayclasses", h.ListGatewayClasses)
			app.Get("/gateways", h.ListGateways)
			app.Get("/gatewaybindings", h.ListGatewayBindings)
			for _, target := range []string{
				"/gatewayclasses?limit=1&continue=stale",
				"/gateways?namespace=default&limit=1&continue=stale",
				"/gatewaybindings?namespace=default&limit=1&continue=stale",
			} {
				response, err := app.Test(httptest.NewRequest(http.MethodGet, target, nil))
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != tc.want {
					t.Fatalf("GET %s status = %d, want %d", target, response.StatusCode, tc.want)
				}
			}
		})
	}
}

func TestGatewayResourceListsFallBackToUnlimitedCachedList(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cached := &cacheEmulatingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&gatewayv1alpha1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "class-a"}},
		&gatewayv1alpha1.GatewayClass{ObjectMeta: metav1.ObjectMeta{Name: "class-b"}},
		&gatewayv1alpha1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "chat-a", Namespace: "default"}},
		&gatewayv1alpha1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "chat-b", Namespace: "default"}},
		&gatewayv1alpha1.GatewayBinding{ObjectMeta: metav1.ObjectMeta{Name: "room-a", Namespace: "default"}},
		&gatewayv1alpha1.GatewayBinding{ObjectMeta: metav1.ObjectMeta{Name: "room-b", Namespace: "default"}},
	).Build()}
	h := NewHandlers(HandlersConfig{Client: cached})
	app := fiber.New()
	app.Get("/gatewayclasses", h.ListGatewayClasses)
	app.Get("/gateways", h.ListGateways)
	app.Get("/gatewaybindings", h.ListGatewayBindings)
	for _, target := range []string{
		"/gatewayclasses?limit=1",
		"/gateways?namespace=default&limit=1",
		"/gatewaybindings?namespace=default&limit=1",
	} {
		response, err := app.Test(httptest.NewRequest(http.MethodGet, target, nil))
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", target, response.StatusCode)
		}
		var body struct {
			Items    []json.RawMessage `json:"items"`
			Metadata ListMeta          `json:"metadata"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if len(body.Items) != 2 || body.Metadata.Continue != "" || body.Metadata.RemainingItemCount != nil {
			t.Fatalf("GET %s = %d items continue=%q remaining=%v, want the full cached collection with no cursor", target, len(body.Items), body.Metadata.Continue, body.Metadata.RemainingItemCount)
		}
		response, err = app.Test(httptest.NewRequest(http.MethodGet, target+"&continue=opaque", nil))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("GET %s with continue status = %d, want 400 without an API reader", target, response.StatusCode)
		}
	}
	if cached.limitedLists != 0 {
		t.Fatalf("cached client served %d limited lists, want 0", cached.limitedLists)
	}
}

func TestGatewayOperatorRoutesRequireExternalAuth(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = gatewayv1alpha1.AddToScheme(scheme)
	server := NewServer(fake.NewClientBuilder().WithScheme(scheme).Build(), nil, ServerConfig{})

	for _, path := range []string{
		"/api/v1/gatewayclasses", "/api/v1/gateways", "/api/v1/gatewaybindings",
		"/api/v1/gateway-events", "/api/v1/gateway-deliveries",
	} {
		response, err := server.app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s status = %d, want 401", path, response.StatusCode)
		}
	}
}

func TestGatewayContextTokenScopesDefaultFailClosed(t *testing.T) {
	cfg, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: ContextTokenAuthorizationModeEnforce})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.GatewayReadScopes) != 1 || cfg.GatewayReadScopes[0] != ContextScopeGatewaysRead {
		t.Fatalf("GatewayReadScopes = %v", cfg.GatewayReadScopes)
	}
	if len(cfg.GatewayOperateScopes) != 1 || cfg.GatewayOperateScopes[0] != ContextScopeGatewaysOperate {
		t.Fatalf("GatewayOperateScopes = %v", cfg.GatewayOperateScopes)
	}
}

func TestGatewayLedgerCursorRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	events, cursor := paginateGatewayEvents([]store.GatewayEvent{
		{ID: "third", CreatedAt: now.Add(2 * time.Second)},
		{ID: "second", CreatedAt: now.Add(time.Second)},
		{ID: "first", CreatedAt: now},
	}, 2)
	if len(events) != 2 || cursor == "" {
		t.Fatalf("paginated events = %#v, cursor = %q", events, cursor)
	}
	decoded, err := decodeGatewayListCursor(cursor)
	if err != nil || decoded.CreatedAt == nil || decoded.ID != "second" || !decoded.CreatedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("decoded cursor = (%+v, %v)", decoded, err)
	}
	if _, err := decodeGatewayListCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
	if got := gatewayPageLimit(25); got != 25 {
		t.Fatalf("bounded page limit = %d, want 25", got)
	}
	if got := gatewayPageLimit(int64(MaxLimit) + 1); got != MaxLimit {
		t.Fatalf("oversized page limit = %d, want %d", got, MaxLimit)
	}
	if got := gatewayStoreListLimit(0); got != MaxLimit+1 {
		t.Fatalf("zero-limit store request = %d, want %d", got, MaxLimit+1)
	}
	unbounded, next := paginateGatewayEvents([]store.GatewayEvent{{ID: "one"}, {ID: "two"}}, 0)
	if len(unbounded) != 2 || next != "" {
		t.Fatalf("zero-limit events = %#v, next = %q", unbounded, next)
	}
	unboundedDeliveries, next := paginateGatewayDeliveries([]store.GatewayDelivery{{ID: "one"}, {ID: "two"}}, 0)
	if len(unboundedDeliveries) != 2 || next != "" {
		t.Fatalf("zero-limit deliveries = %#v, next = %q", unboundedDeliveries, next)
	}
}

func TestGatewayLedgerKubernetesAuthorizationUsesSubjectAccessReview(t *testing.T) {
	clientset := denyingSubjectAccessReviewClient(t, nil, func(review *authorizationv1.SubjectAccessReview) {
		attrs := review.Spec.ResourceAttributes
		if attrs == nil || attrs.Namespace != defaultNamespace || attrs.Verb != gatewayVerbList ||
			attrs.Group != gatewayv1alpha1.GroupVersion.Group || attrs.Resource != gatewayResourceName {
			t.Fatalf("unexpected SubjectAccessReview: %#v", review.Spec)
		}
	})
	err := authorizeKubernetesResourceAction(
		context.Background(), clientset,
		&UserInfo{AuthType: AuthTypeTokenReview, Username: "system:serviceaccount:default:viewer"},
		defaultNamespace, gatewayVerbList, gatewayv1alpha1.GroupVersion.Group, gatewayResourceName, "",
	)
	var fiberErr *fiber.Error
	if !errors.As(err, &fiberErr) || fiberErr.Code != http.StatusForbidden {
		t.Fatalf("authorization error = %v, want forbidden", err)
	}
}

func TestGatewayStoreHTTPErrorPreservesServiceAvailabilityStatus(t *testing.T) {
	err := gatewayStoreHTTPError(&gatewayruntime.HTTPError{
		Code: http.StatusServiceUnavailable, Message: "gateway delivery processing is disabled",
	}, "retry gateway delivery")
	var fiberErr *fiber.Error
	if !errors.As(err, &fiberErr) || fiberErr.Code != http.StatusServiceUnavailable ||
		fiberErr.Message != "gateway delivery processing is disabled" {
		t.Fatalf("gatewayStoreHTTPError() = %v, want gateway service 503", err)
	}
}

func TestRetryGatewayDeliveryRequiresContextTokenReadScope(t *testing.T) {
	cfg, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{
		Mode: ContextTokenAuthorizationModeEnforce,
	})
	if err != nil {
		t.Fatal(err)
	}
	storeFixture, delivery := newGatewayHandlerDeliveryStore(t)
	h := NewHandlers(HandlersConfig{
		ContextTokenAuthorization: cfg,
		GatewayDeliveryStore:      storeFixture,
		GatewayService:            gatewayruntime.NewService(nil, nil, storeFixture, nil, gatewayruntime.DefaultConfig()),
	})
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.Locals(UserInfoContextKey, contextScopedTestUser(ContextScopeGatewaysOperate))
		return c.Next()
	})
	app.Post("/gateway-deliveries/:id/retry", h.RetryGatewayDelivery)

	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/gateway-deliveries/"+delivery.ID+"/retry?namespace=default", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for operate-only context token", response.StatusCode)
	}
}

func TestRetryGatewayDeliveryRequiresKubernetesReadPermission(t *testing.T) {
	storeFixture, delivery := newGatewayHandlerDeliveryStore(t)
	h := NewHandlers(HandlersConfig{
		Client:    newGatewayIdentityClient(t, "namespace-uid", delivery.GatewayUID),
		APIReader: newGatewayIdentityClient(t, "namespace-uid", delivery.GatewayUID),
		KubeClient: denyingSubjectAccessReviewClient(t, nil, func(review *authorizationv1.SubjectAccessReview) {
			attrs := review.Spec.ResourceAttributes
			if attrs == nil || attrs.Verb != gatewayVerbGet || attrs.Resource != gatewayResourceName ||
				attrs.Namespace != "default" || attrs.Name != delivery.GatewayName {
				t.Fatalf("unexpected SubjectAccessReview: %#v", review.Spec)
			}
		}),
		GatewayDeliveryStore: storeFixture,
		GatewayService:       gatewayruntime.NewService(nil, nil, storeFixture, nil, gatewayruntime.DefaultConfig()),
	})
	app := fiber.New()
	app.Use(tokenReviewUserMiddleware(limitedTokenReviewUser("default")))
	app.Post("/gateway-deliveries/:id/retry", h.RetryGatewayDelivery)

	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/gateway-deliveries/"+delivery.ID+"/retry?namespace=default", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 without gateway read permission", response.StatusCode)
	}
}

func TestGetGatewayEventAuthorizesAssociatedGatewayName(t *testing.T) {
	storeFixture := newGatewayHandlerStore(t)
	now := time.Now().UTC()
	event := store.GatewayEvent{
		ID: "event-authz", Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
		ExternalEventID: "external-authz", ProtocolVersion: "orka.gateway.v1", EventType: "text",
		State: store.GatewayEventRejected, AccountID: "acct", ContextID: "room", SenderID: "sender",
		Text: "hello", ReplyTarget: "room", ReceivedAt: now, NextAttemptAt: now,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(HandlersConfig{
		Client:            newGatewayIdentityClient(t, event.NamespaceUID, event.GatewayUID),
		APIReader:         newGatewayIdentityClient(t, event.NamespaceUID, event.GatewayUID),
		GatewayEventStore: storeFixture,
		KubeClient: denyingSubjectAccessReviewClient(t, nil, func(review *authorizationv1.SubjectAccessReview) {
			attrs := review.Spec.ResourceAttributes
			if attrs == nil || attrs.Verb != gatewayVerbGet || attrs.Resource != gatewayResourceName || attrs.Name != event.GatewayName {
				t.Fatalf("unexpected SubjectAccessReview: %#v", review.Spec)
			}
		}),
	})
	app := fiber.New()
	app.Use(tokenReviewUserMiddleware(limitedTokenReviewUser("default")))
	app.Get("/gateway-events/:id", h.GetGatewayEvent)
	response, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/gateway-events/"+event.ID+"?namespace=default", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", response.StatusCode)
	}
}

func TestGatewayLedgerListsSupportZeroLimit(t *testing.T) {
	storeFixture, delivery := newGatewayHandlerDeliveryStore(t)
	now := time.Now().UTC()
	event := store.GatewayEvent{
		ID: "event-zero-limit", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", ExternalEventID: "external-zero-limit",
		ProtocolVersion: "orka.gateway.v1", EventType: "text", State: store.GatewayEventRejected,
		AccountID: "acct", ContextID: "room", SenderID: "sender", Text: "bounded", ReplyTarget: "room",
		ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
		t.Fatal(err)
	}
	identityClient := newGatewayIdentityClient(t, "namespace-uid", "gateway-uid")
	h := NewHandlers(HandlersConfig{
		Client: identityClient, APIReader: identityClient,
		GatewayEventStore: storeFixture, GatewayDeliveryStore: storeFixture,
	})
	app := fiber.New()
	app.Get("/gateway-events", h.ListGatewayEvents)
	app.Get("/gateway-deliveries", h.ListGatewayDeliveries)

	eventResponse, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/gateway-events?namespace=default&gateway=chat&limit=0", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if eventResponse.StatusCode != http.StatusOK {
		t.Fatalf("event status = %d", eventResponse.StatusCode)
	}
	var eventList struct {
		Items []store.GatewayEvent `json:"items"`
	}
	if err := json.NewDecoder(eventResponse.Body).Decode(&eventList); err != nil {
		t.Fatal(err)
	}
	if len(eventList.Items) != 1 || eventList.Items[0].ID != event.ID {
		t.Fatalf("zero-limit events = %#v", eventList.Items)
	}

	deliveryResponse, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/gateway-deliveries?namespace=default&gateway=chat&limit=0", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if deliveryResponse.StatusCode != http.StatusOK {
		t.Fatalf("delivery status = %d", deliveryResponse.StatusCode)
	}
	var deliveryList struct {
		Items []store.GatewayDelivery `json:"items"`
	}
	if err := json.NewDecoder(deliveryResponse.Body).Decode(&deliveryList); err != nil {
		t.Fatal(err)
	}
	if len(deliveryList.Items) != 1 || deliveryList.Items[0].ID != delivery.ID {
		t.Fatalf("zero-limit deliveries = %#v", deliveryList.Items)
	}
}

func TestFilteredGatewayEventListUsesNamedAuthorization(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		Client:            newGatewayIdentityClient(t, "namespace-uid", "gateway-uid"),
		APIReader:         newGatewayIdentityClient(t, "namespace-uid", "gateway-uid"),
		GatewayEventStore: newGatewayHandlerStore(t),
		KubeClient: denyingSubjectAccessReviewClient(t, nil, func(review *authorizationv1.SubjectAccessReview) {
			attrs := review.Spec.ResourceAttributes
			if attrs == nil || attrs.Verb != gatewayVerbGet || attrs.Resource != gatewayResourceName || attrs.Name != "chat" {
				t.Fatalf("unexpected SubjectAccessReview: %#v", review.Spec)
			}
		}),
	})
	app := fiber.New()
	app.Use(tokenReviewUserMiddleware(limitedTokenReviewUser("default")))
	app.Get("/gateway-events", h.ListGatewayEvents)
	response, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/gateway-events?namespace=default&gateway=chat", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", response.StatusCode)
	}
}

func TestGatewayEventRejectsRecreatedResourceIdentity(t *testing.T) {
	for _, test := range []struct {
		name         string
		namespaceUID string
		gatewayUID   string
	}{
		{name: "Gateway replacement", namespaceUID: "namespace-uid", gatewayUID: "replacement-gateway-uid"},
		{name: "namespace replacement", namespaceUID: "replacement-namespace-uid", gatewayUID: "gateway-uid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			storeFixture := newGatewayHandlerStore(t)
			now := time.Now().UTC()
			event := store.GatewayEvent{
				ID: "event-recreated", Namespace: "default", NamespaceUID: "namespace-uid",
				GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", ExternalEventID: "external-recreated",
				ProtocolVersion: "orka.gateway.v1", EventType: "text", State: store.GatewayEventRejected,
				AccountID: "acct", ContextID: "room", SenderID: "sender", Text: "private",
				ReplyTarget: "room", ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
				CreatedAt: now, UpdatedAt: now,
			}
			if _, _, err := storeFixture.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
				t.Fatal(err)
			}
			identityClient := newGatewayIdentityClient(t, test.namespaceUID, test.gatewayUID)
			h := NewHandlers(HandlersConfig{
				Client: identityClient, APIReader: identityClient, GatewayEventStore: storeFixture,
			})
			app := fiber.New()
			app.Get("/gateway-events/:id", h.GetGatewayEvent)
			response, err := app.Test(httptest.NewRequest(
				http.MethodGet, "/gateway-events/"+event.ID+"?namespace=default", nil,
			))
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 for stale immutable identity", response.StatusCode)
			}
		})
	}
}

func TestUnfilteredGatewayLedgersExcludeRecreatedGatewayRecords(t *testing.T) {
	storeFixture := newGatewayHandlerStore(t)
	now := time.Now().UTC()
	event := store.GatewayEvent{
		ID: "event-stale-list", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "old-gateway-uid", GatewayGeneration: 1, GatewayName: "chat", ExternalEventID: "external-stale-list",
		ProtocolVersion: "orka.gateway.v1", EventType: "text", State: store.GatewayEventRejected,
		AccountID: "acct", ContextID: "room", SenderID: "sender", Text: "private stale event",
		ReplyTarget: "room", ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
		t.Fatal(err)
	}
	delivery := &store.GatewayDelivery{
		ID: "delivery-stale-list", IdempotencyID: "delivery-stale-list", Namespace: "default",
		NamespaceUID: "namespace-uid", GatewayUID: "old-gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
		EventID: event.ID, Kind: "final", AccountID: "acct", ContextID: "room", ReplyTarget: "room",
		Text: "private stale delivery", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.CreateGatewayDelivery(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	identityClient := newGatewayIdentityClient(t, "namespace-uid", "replacement-gateway-uid")
	h := NewHandlers(HandlersConfig{
		Client: identityClient, APIReader: identityClient,
		GatewayEventStore: storeFixture, GatewayDeliveryStore: storeFixture,
	})
	app := fiber.New()
	app.Get("/gateway-events", h.ListGatewayEvents)
	app.Get("/gateway-deliveries", h.ListGatewayDeliveries)

	eventResponse, err := app.Test(httptest.NewRequest(http.MethodGet, "/gateway-events?namespace=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	var eventList struct {
		Items []store.GatewayEvent `json:"items"`
	}
	if err := json.NewDecoder(eventResponse.Body).Decode(&eventList); err != nil {
		t.Fatal(err)
	}
	if len(eventList.Items) != 0 {
		t.Fatalf("stale events leaked through unfiltered list: %#v", eventList.Items)
	}

	deliveryResponse, err := app.Test(httptest.NewRequest(http.MethodGet, "/gateway-deliveries?namespace=default", nil))
	if err != nil {
		t.Fatal(err)
	}
	var deliveryList struct {
		Items []store.GatewayDelivery `json:"items"`
	}
	if err := json.NewDecoder(deliveryResponse.Body).Decode(&deliveryList); err != nil {
		t.Fatal(err)
	}
	if len(deliveryList.Items) != 0 {
		t.Fatalf("stale deliveries leaked through unfiltered list: %#v", deliveryList.Items)
	}
}

type failingGatewayIdentityReader struct{ err error }

func (r failingGatewayIdentityReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return r.err
}

func (r failingGatewayIdentityReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return r.err
}

func TestGatewayIdentityLookupErrorsPropagate(t *testing.T) {
	storeFixture := newGatewayHandlerStore(t)
	now := time.Now().UTC()
	event := store.GatewayEvent{
		ID: "event-identity-error", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", ExternalEventID: "external-identity-error",
		ProtocolVersion: "orka.gateway.v1", EventType: "text", State: store.GatewayEventRejected,
		AccountID: "acct", ContextID: "room", SenderID: "sender", Text: "private",
		ReplyTarget: "room", ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(HandlersConfig{
		APIReader: failingGatewayIdentityReader{err: errors.New("API unavailable")}, GatewayEventStore: storeFixture,
	})
	app := fiber.New()
	app.Get("/gateway-events/:id", h.GetGatewayEvent)
	response, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/gateway-events/"+event.ID+"?namespace=default", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 for identity backend failure", response.StatusCode)
	}
}

func newGatewayHandlerStore(t *testing.T) *sqlite.Store {
	t.Helper()
	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return sqlite.NewStore(db, ":memory:")
}

func newGatewayHandlerDeliveryStore(t *testing.T) (*sqlite.Store, *store.GatewayDelivery) {
	t.Helper()
	storeFixture := newGatewayHandlerStore(t)
	now := time.Now().UTC()
	delivery := &store.GatewayDelivery{
		ID: "delivery-authz", IdempotencyID: "delivery-authz", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event-authz",
		Kind: "final", State: store.GatewayDeliveryDeadLettered, AccountID: "acct", ContextID: "room",
		ReplyTarget: "room", Text: "reply", MaxAttempts: 10, NextAttemptAt: now,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := storeFixture.CreateGatewayDelivery(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	return storeFixture, delivery
}

func newGatewayIdentityClient(t *testing.T, namespaceUID, gatewayUID string) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := gatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID(namespaceUID)}},
		&gatewayv1alpha1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "default", UID: types.UID(gatewayUID)}},
	).Build()
}
