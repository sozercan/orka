/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/outboundaccess"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/tokenexchange"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/genai"
	"github.com/orka-agents/orka/internal/transactiontoken"
	"github.com/orka-agents/orka/internal/workerenv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	mcpJSONRPCVersion                = "2.0"
	mcpProtocolVersion               = "2025-06-18"
	mcpProtocolVersionHeader         = "MCP-Protocol-Version"
	mcpSessionIDHeader               = "Mcp-Session-Id"
	mcpToolCallRequestID             = "1"
	mcpInitializeRequestID           = "initialize"
	mcpSessionTerminateTimeout       = 10 * time.Second
	maxToolHTTPRedirects             = 10
	mcpToolsCallMethod               = "tools/call"
	mcpInitializeMethod              = "initialize"
	mcpInitializedNotificationMethod = "notifications/initialized"
	toolIdempotencyKeyHeader         = "Idempotency-Key"
	toolHTTPResponseBodyLimit        = 10 << 20
	toolHTTPErrorBodyLimit           = 4096
	toolAuthInjectHeader             = "header"
)

var (
	executeToolOperationAttr = attribute.String(genai.AttrOperationName, genai.OperationExecuteTool)
	extensionToolTypeAttr    = attribute.String(genai.AttrToolType, genai.ToolTypeExtension)

	executeToolDurationOptions = []metric.Float64HistogramOption{
		metric.WithUnit(genai.UnitSeconds),
		metric.WithExplicitBucketBoundaries(genai.ToolDurationBuckets...),
	}
)

type toolCallIDContextKey struct{}

// WithToolCallID records the model provider's tool-call id for Tool CRD span metadata.
func WithToolCallID(ctx context.Context, toolCallID string) context.Context {
	if strings.TrimSpace(toolCallID) == "" {
		return ctx
	}
	return context.WithValue(ctx, toolCallIDContextKey{}, toolCallID)
}

func toolCallIDFromContext(ctx context.Context) string {
	toolCallID, _ := ctx.Value(toolCallIDContextKey{}).(string)
	return strings.TrimSpace(toolCallID)
}

// ToolExecutor handles execution of custom Tool CRDs via HTTP or MCP-over-HTTP.
type ToolExecutor struct {
	client                     *http.Client
	secretPath                 string
	namespace                  string
	k8sClient                  kubernetes.Interface
	outboundResolver           outboundaccess.Resolver
	skipDirectPublicValidation bool

	transactionAuthoritySet     bool
	transactionToken            string
	transactionScopes           []string
	credentialAuthorityEnforced bool
	credentialScopeAllowed      bool
	credentialSecret            string
	transactionExchange         *TransactionExchangeConfig
	authSecretValues            map[string]string

	ttsMu        sync.Mutex
	ttsClient    *contexttoken.TTSClient
	ttsClientKey string
}

// TransactionExchangeConfig carries controller-resolved TTS settings for
// brokered Tool execution, where worker environment variables are unavailable.
type TransactionExchangeConfig struct {
	TTS              contexttoken.TTSConfig
	Exchanger        contexttoken.Exchanger
	SubjectTokenType string
	OutboundScope    string
}

// SetTransactionAuthority sets task-scoped transaction authority. Calling it
// with an empty token intentionally disables fallback to process-global files.
func (e *ToolExecutor) SetTransactionAuthority(token string, scopes []string) {
	if e == nil {
		return
	}
	e.transactionAuthoritySet = true
	e.transactionToken = strings.TrimSpace(token)
	e.transactionScopes = append([]string(nil), scopes...)
}

// SetTransactionCredentialAuthority sets immutable Task authorization for outbound credentials.
func (e *ToolExecutor) SetTransactionCredentialAuthority(enforced, allowed bool, secret string) {
	if e == nil {
		return
	}
	e.credentialAuthorityEnforced = enforced
	e.credentialScopeAllowed = allowed
	e.credentialSecret = strings.TrimSpace(secret)
}

// SetTransactionExchangeConfig supplies controller-resolved TTS settings.
func (e *ToolExecutor) SetTransactionExchangeConfig(config *TransactionExchangeConfig) {
	if e == nil {
		return
	}
	if config == nil {
		e.transactionExchange = nil
		return
	}
	copyConfig := *config
	e.transactionExchange = &copyConfig
}

// SetAuthSecretValue supplies an exact task-scoped Tool auth Secret snapshot.
func (e *ToolExecutor) SetAuthSecretValue(name, key, value string) {
	if e == nil || strings.TrimSpace(name) == "" || strings.TrimSpace(key) == "" {
		return
	}
	if e.authSecretValues == nil {
		e.authSecretValues = map[string]string{}
	}
	e.authSecretValues[name+"\x00"+key] = strings.TrimSpace(value)
}

// NewToolExecutor creates a new tool executor
func NewToolExecutor() *ToolExecutor {
	namespace := os.Getenv(workerenv.TaskNamespace)
	if namespace == "" {
		namespace = "default"
	}

	// Create Kubernetes clients for Secret, policy, Service, and TokenRequest access.
	var k8sClient kubernetes.Interface
	var resolver outboundaccess.Resolver
	if config, err := rest.InClusterConfig(); err == nil {
		k8sClient, _ = kubernetes.NewForConfig(config)
		scheme := runtime.NewScheme()
		if clientgoscheme.AddToScheme(scheme) == nil && corev1alpha1.AddToScheme(scheme) == nil {
			if resourceClient, clientErr := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme}); clientErr == nil {
				trustedGateways, gatewayErr := outboundaccess.ParseTrustedServiceReferences(os.Getenv(workerenv.OutboundAccessTrustedGatewayServices))
				trustedEndpoints, endpointErr := outboundaccess.ParseTrustedServiceReferences(os.Getenv(workerenv.OutboundAccessTrustedTokenEndpointServices))
				if gatewayErr == nil && endpointErr == nil {
					resolver = &outboundaccess.KubernetesResolver{
						Reader:     resourceClient,
						KubeClient: k8sClient,
						Exchanger:  tokenexchange.NewClient(tokenexchange.ClientOptions{}),
						Trust: outboundaccess.TrustConfig{
							Gateways:       trustedGateways,
							TokenEndpoints: trustedEndpoints,
						},
					}
				}
			}
		}
	}

	credentialScopes := workerenv.SplitCSV(os.Getenv(workerenv.TransactionScopes))
	if len(credentialScopes) == 0 {
		credentialScopes = strings.Fields(os.Getenv(workerenv.TransactionScope))
	}
	requiredCredentialScopes := workerenv.SplitCSV(os.Getenv(workerenv.TransactionCredentialReadScopes))
	if len(requiredCredentialScopes) == 0 {
		requiredCredentialScopes = []string{outboundaccess.DefaultCredentialReadScope}
	}
	authorityConstraint := strings.TrimSpace(os.Getenv(workerenv.TransactionCredentialSecret))
	return &ToolExecutor{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		secretPath:                  "/secrets/tools",
		namespace:                   namespace,
		k8sClient:                   k8sClient,
		outboundResolver:            resolver,
		credentialAuthorityEnforced: workerenv.IsTrue(os.Getenv(workerenv.TransactionCredentialAuthorizationEnforced)),
		credentialScopeAllowed:      containsAnyString(credentialScopes, requiredCredentialScopes),
		credentialSecret:            authorityConstraint,
	}
}

// NewToolExecutorForNamespace creates a Tool CRD executor with explicit
// dependencies. Controller-side brokered AgentRuntime tool execution uses this
// path so Orka, not the remote backend, resolves tool credentials.
func NewToolExecutorForNamespace(namespace string, k8sClient kubernetes.Interface, httpClient *http.Client, resolvers ...outboundaccess.Resolver) *ToolExecutor {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = "default"
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	var resolver outboundaccess.Resolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}
	return &ToolExecutor{
		client:           httpClient,
		secretPath:       "/secrets/tools",
		namespace:        namespace,
		k8sClient:        k8sClient,
		outboundResolver: resolver,
	}
}

// Execute executes a Tool CRD by making an HTTP request.
func (e *ToolExecutor) Execute(ctx context.Context, tool *corev1alpha1.Tool, args json.RawMessage) (result string, err error) {
	if tool != nil && tool.Spec.HTTP != nil && tool.Spec.HTTP.Timeout != nil && tool.Spec.HTTP.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tool.Spec.HTTP.Timeout.Duration)
		defer cancel()
	}
	if toolExecutorTelemetryDisabled() {
		return e.executeToolRequest(ctx, tool, args)
	}

	toolName := toolTelemetryName(tool)
	meterActive := tracing.GlobalMeterProviderActive()
	var start time.Time
	if meterActive {
		start = time.Now()
	}

	tracer := tracing.GenAITracer(genai.InstrumentationName)
	spanName := genai.OperationExecuteTool + " " + toolName
	deferStartAttributes := tracing.IsDefaultGlobalTracerProvider(tracing.GlobalTracerProvider())
	var span trace.Span
	if deferStartAttributes {
		ctx, span = tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	} else {
		attrs := toolExecutorSpanAttributes(ctx, e.namespace, toolName)
		ctx, span = tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	}
	spanRecording := span.IsRecording()
	if spanRecording && deferStartAttributes {
		span.SetAttributes(toolExecutorSpanAttributes(ctx, e.namespace, toolName)...)
	}
	if !spanRecording && !meterActive {
		result, err = e.executeToolRequest(ctx, tool, args)
		span.End()
		return result, err
	}

	defer func() {
		resultSize := len(result)
		if err != nil {
			errType := toolExecutionErrorType(err)
			if spanRecording {
				span.SetStatus(codes.Error, errType)
				span.SetAttributes(
					attribute.Int(tracing.AttrToolResultSizeBytes, resultSize),
					attribute.String(genai.AttrErrorType, errType),
				)
			}
			if meterActive {
				recordExternalToolDuration(ctx, time.Since(start).Seconds(), executeToolOperationAttr, attribute.String(genai.AttrToolName, toolName), extensionToolTypeAttr, attribute.String(genai.AttrErrorType, errType))
			}
		} else {
			if spanRecording {
				span.SetAttributes(attribute.Int(tracing.AttrToolResultSizeBytes, resultSize))
			}
			if meterActive {
				recordExternalToolDuration(ctx, time.Since(start).Seconds(), executeToolOperationAttr, attribute.String(genai.AttrToolName, toolName), extensionToolTypeAttr)
			}
		}
		span.End()
	}()

	prepared, err := e.prepareRequest(ctx, tool, args)
	if err != nil {
		return "", err
	}
	if prepared.request != nil {
		if spanRecording {
			span.SetAttributes(httpToolRequestAttributes(prepared.request)...)
		}
		injectTraceHeaders(ctx, prepared.request.Header)
	}

	return e.executePreparedToolRequest(ctx, prepared)
}

func toolExecutorSpanAttributes(ctx context.Context, namespace, toolName string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 9)
	attrs = append(attrs,
		executeToolOperationAttr,
		attribute.String(genai.AttrToolName, toolName),
		extensionToolTypeAttr,
		attribute.String(tracing.AttrToolName, toolName),
		attribute.String(tracing.AttrToolKind, tracing.ToolKindHTTP),
	)
	if taskName := os.Getenv(workerenv.TaskName); taskName != "" {
		attrs = append(attrs, attribute.String(tracing.AttrTaskID, taskName))
	}
	if namespace != "" {
		attrs = append(attrs,
			attribute.String(tracing.AttrTaskNamespace, namespace),
			attribute.String(tracing.AttrTenant, namespace),
		)
	}
	if toolCallID := toolCallIDFromContext(ctx); toolCallID != "" {
		attrs = append(attrs, attribute.String(genai.AttrToolCallID, toolCallID))
	}
	return attrs
}

func (e *ToolExecutor) executeToolRequest(ctx context.Context, tool *corev1alpha1.Tool, args json.RawMessage) (string, error) {
	prepared, err := e.prepareRequest(ctx, tool, args)
	if err != nil {
		return "", err
	}
	if prepared.request != nil {
		injectTraceHeaders(ctx, prepared.request.Header)
	}
	return e.executePreparedToolRequest(ctx, prepared)
}

func (e *ToolExecutor) executePreparedToolRequest(ctx context.Context, prepared preparedToolRequest) (string, error) {
	httpClient, err := toolHTTPClient(e.client, prepared.httpConfig.Timeout, prepared.gatewayTLS, prepared.gateway)
	if err != nil {
		return "", err
	}

	if prepared.direct && !e.skipDirectPublicValidation {
		dialContext := tokenexchange.PublicEndpointDialContext
		if prepared.trustedActorRoute {
			dialContext = exactEndpointDialContext(prepared.request.URL)
		}
		httpClient, err = directCredentialHTTPClient(httpClient, dialContext)
		if err != nil {
			return "", err
		}
	}

	if prepared.mcp {
		result, err := e.executeMCPToolCall(ctx, httpClient, prepared)
		if err != nil && prepared.gateway {
			return "", gatewayMCPError{Err: err}
		}
		return result, err
	}

	respBody, err := executeToolHTTPRequest(httpClient, prepared.request, prepared.gateway, prepared.redactionSecrets...)
	if err != nil {
		return "", ToolRequestAttemptedError{Err: err}
	}

	return string(respBody), nil
}

func directCredentialHTTPClient(
	base *http.Client,
	dialContext func(context.Context, string, string) (net.Conn, error),
) (*http.Client, error) {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	transport := base.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		return nil, errors.New("direct outbound access requires an *http.Transport")
	}
	if dialContext == nil {
		return nil, errors.New("direct outbound access requires a dial context")
	}
	clone := httpTransport.Clone()
	clone.Proxy = nil
	clone.DisableKeepAlives = true
	clone.DialTLS = nil //nolint:staticcheck
	clone.DialTLSContext = nil
	clone.DialContext = dialContext
	if clone.TLSClientConfig != nil {
		clone.TLSClientConfig = clone.TLSClientConfig.Clone()
	} else {
		clone.TLSClientConfig = &tls.Config{}
	}
	clone.TLSClientConfig.MinVersion = max(clone.TLSClientConfig.MinVersion, tls.VersionTLS12)
	clone.TLSClientConfig.InsecureSkipVerify = false
	client.Transport = clone
	return &client, nil
}

func exactEndpointDialContext(endpoint *neturl.URL) func(context.Context, string, string) (net.Conn, error) {
	expectedAuthority := normalizedHTTPHost(endpoint)
	scheme := ""
	if endpoint != nil {
		scheme = strings.ToLower(endpoint.Scheme)
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		actual := &neturl.URL{Scheme: scheme, Host: address}
		if expectedAuthority == "" || normalizedHTTPHost(actual) != expectedAuthority {
			return nil, errors.New("direct outbound access refused an unexpected endpoint authority")
		}
		dialer := &net.Dialer{}
		return dialer.DialContext(ctx, network, address)
	}
}

func toolHTTPClient(base *http.Client, timeout *metav1.Duration, gatewayTLS tokenexchange.TLSConfig, gateway bool) (*http.Client, error) {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	if timeout != nil {
		client.Timeout = timeout.Duration
	}

	configureGatewayTLS := len(gatewayTLS.CAPEM) > 0 || strings.TrimSpace(gatewayTLS.ServerName) != ""
	if gateway || configureGatewayTLS {
		transport := base.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		httpTransport, ok := transport.(*http.Transport)
		if !ok {
			return nil, errors.New("gateway routing requires an *http.Transport")
		}
		clone := httpTransport.Clone()
		if gateway {
			clone.Proxy = nil
			clone.DisableKeepAlives = true
		}
		if configureGatewayTLS {
			tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(gatewayTLS.ServerName)}
			if len(gatewayTLS.CAPEM) > 0 {
				roots, err := x509.SystemCertPool()
				if err != nil || roots == nil {
					roots = x509.NewCertPool()
				}
				if !roots.AppendCertsFromPEM(gatewayTLS.CAPEM) {
					return nil, errors.New("gateway CA Secret does not contain a valid PEM certificate")
				}
				tlsConfig.RootCAs = roots
			}
			clone.TLSClientConfig = tlsConfig
		}
		client.Transport = clone
	}

	previousCheckRedirect := base.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if previousCheckRedirect != nil {
			if err := previousCheckRedirect(req, via); err != nil {
				return err
			}
		}
		if gateway {
			return http.ErrUseLastResponse
		}
		if len(via) == 0 {
			return nil
		}
		if !sameHTTPOrigin(via[len(via)-1].URL, req.URL) {
			return http.ErrUseLastResponse
		}
		if len(via) >= maxToolHTTPRedirects {
			return fmt.Errorf("stopped after %d redirects", maxToolHTTPRedirects)
		}
		// Transaction authority is single-hop even when an authenticated
		// resource credential is allowed to follow a same-origin canonical redirect.
		req.Header.Del(transactiontoken.HeaderName)
		return nil
	}
	return &client, nil
}

func sameHTTPOrigin(a, b *neturl.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Scheme, b.Scheme) && normalizedHTTPHost(a) == normalizedHTTPHost(b)
}

func normalizedHTTPHost(u *neturl.URL) string {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	if port == "" {
		return host
	}
	return net.JoinHostPort(host, port)
}

type gatewayMCPError struct{ Err error }

func (e gatewayMCPError) Error() string { return "gateway MCP request failed" }
func (e gatewayMCPError) Unwrap() error { return e.Err }

// ToolRequestAttemptedError wraps errors that occur after a custom tool request
// may already have reached the remote endpoint.
type ToolRequestAttemptedError struct {
	Err error
}

func (e ToolRequestAttemptedError) Error() string { return e.Err.Error() }
func (e ToolRequestAttemptedError) Unwrap() error { return e.Err }

// ToolExecutionError reports a failure returned by an admitted tool call.
// Authorization, transport, cancellation, and malformed-response errors do not
// carry this marker. Callers must not expose its upstream diagnostic as an MCP
// result without applying their own disclosure policy.
type ToolExecutionError struct {
	Err error
}

func (e ToolExecutionError) Error() string { return e.Err.Error() }
func (e ToolExecutionError) Unwrap() error { return e.Err }

// ToolRequestWasAttempted reports whether err happened after the custom tool send path began.
func ToolRequestWasAttempted(err error) bool {
	var attempted ToolRequestAttemptedError
	return errors.As(err, &attempted)
}

func toolExecutorTelemetryDisabled() bool {
	return tracing.GlobalTracerProviderExplicitNoop() && !tracing.GlobalMeterProviderActive()
}

func recordExternalToolDuration(ctx context.Context, seconds float64, attrs ...attribute.KeyValue) {
	meter := tracing.GenAIMeter(genai.InstrumentationName)
	histogram, err := meter.Float64Histogram(
		genai.MetricExecuteToolDuration,
		executeToolDurationOptions...,
	)
	if err != nil {
		return
	}
	histogram.Record(ctx, seconds, metric.WithAttributes(attrs...))
}

func toolExecutionErrorType(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	const httpPrefix = "tool returned HTTP "
	if _, after, ok := strings.Cut(message, httpPrefix); ok {
		statusText := after
		var code strings.Builder
		for _, r := range statusText {
			if r < '0' || r > '9' {
				break
			}
			code.WriteString(string(r))
		}
		if code.String() != "" {
			return "http_status_" + code.String()
		}
	}
	return fmt.Sprintf("%T", err)
}

func toolTelemetryName(tool *corev1alpha1.Tool) string {
	if tool != nil {
		if name := strings.TrimSpace(tool.Name); name != "" {
			return name
		}
	}
	return "http_tool"
}

func httpToolRequestAttributes(req *http.Request) []attribute.KeyValue {
	if req == nil {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, 3)
	if req.Method != "" {
		attrs = append(attrs, attribute.String("http.request.method", req.Method))
	}
	if req.URL != nil {
		if host := req.URL.Hostname(); host != "" {
			attrs = append(attrs, attribute.String("server.address", host))
		}
		if req.URL.Scheme != "" {
			attrs = append(attrs, attribute.String("url.scheme", req.URL.Scheme))
		}
	}
	return attrs
}

func injectTraceHeaders(ctx context.Context, header http.Header) {
	if header == nil {
		return
	}
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(header))
}

func executeToolHTTPRequest(httpClient *http.Client, req *http.Request, suppressErrorBody bool, secrets ...string) ([]byte, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, safeToolTransportError(req.Context(), err, secrets...)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := readLimitedHTTPResponseBody(resp.Body, resp.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if suppressErrorBody {
			return nil, fmt.Errorf("gateway returned HTTP %d", resp.StatusCode)
		}
		err := fmt.Errorf("tool returned HTTP %d: %s", resp.StatusCode, redactToolHTTPErrorBody(string(respBody), secrets...))
		// A completed API failure can be delivered to the model as a tool
		// result. Redirect and authentication failures remain protocol errors.
		if resp.StatusCode >= 400 && resp.StatusCode < 600 && resp.StatusCode != http.StatusUnauthorized &&
			resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusProxyAuthRequired {
			return nil, ToolExecutionError{Err: err}
		}
		return nil, err
	}
	return []byte(redactToolSensitiveText(string(respBody), secrets...)), nil
}

func readLimitedHTTPResponseBody(body io.Reader, contentLength int64) ([]byte, error) {
	limited := io.LimitReader(body, toolHTTPResponseBodyLimit+1)
	var respBody bytes.Buffer
	if contentLength > 0 && contentLength <= toolHTTPResponseBodyLimit {
		respBody.Grow(int(contentLength))
	}
	if _, err := respBody.ReadFrom(limited); err != nil {
		return nil, err
	}
	if respBody.Len() > toolHTTPResponseBodyLimit {
		return nil, fmt.Errorf("response body exceeds %d-byte limit", toolHTTPResponseBodyLimit)
	}
	return respBody.Bytes(), nil
}

type toolIdempotencyKeyContextKey struct{}

// WithToolIdempotencyKey attaches a trusted idempotency key for HTTP tool execution.
func WithToolIdempotencyKey(ctx context.Context, key string) context.Context {
	key = strings.TrimSpace(key)
	if key == "" {
		return ctx
	}
	return context.WithValue(ctx, toolIdempotencyKeyContextKey{}, key)
}

func toolIdempotencyKeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	key, _ := ctx.Value(toolIdempotencyKeyContextKey{}).(string)
	return strings.TrimSpace(key)
}

type preparedToolRequest struct {
	httpConfig        corev1alpha1.HTTPExecution
	request           *http.Request
	authToken         string
	transactionToken  string
	redactionSecrets  []string
	gatewayTLS        tokenexchange.TLSConfig
	gateway           bool
	direct            bool
	mcp               bool
	trustedActorRoute bool
}

func (p preparedToolRequest) secrets(extra ...string) []string {
	values := append([]string(nil), p.redactionSecrets...)
	values = append(values, extra...)
	return values
}

func decodeToolArguments(args json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(args)) == 0 {
		return make(map[string]any), nil
	}
	var params map[string]any
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.UseNumber()
	if err := dec.Decode(&params); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing data")
	}
	return params, nil
}

//nolint:gocyclo // Request preparation centralizes auth, protocol, transaction, and outbound policy invariants.
func (e *ToolExecutor) prepareRequest(ctx context.Context, tool *corev1alpha1.Tool, args json.RawMessage) (preparedToolRequest, error) {
	params, err := decodeToolArguments(args)
	if err != nil {
		return preparedToolRequest{}, fmt.Errorf("failed to parse tool arguments: %w", err)
	}

	httpConfig, routeHost, err := toolHTTPConfig(tool)
	if err != nil {
		return preparedToolRequest{}, err
	}
	isMCP := isMCPSubstrateActorTool(tool)

	var authToken string
	if httpConfig.AuthSecretRef != nil {
		token, err := e.getSecretKey(ctx, httpConfig.AuthSecretRef.Name, httpConfig.AuthSecretRef.Key)
		if err != nil {
			return preparedToolRequest{}, fmt.Errorf("failed to get auth secret: %w", err)
		}
		authToken = token
	}

	authInject := toolAuthInject(httpConfig)
	if isMCP && authInject == "body" && authToken != "" {
		return preparedToolRequest{}, fmt.Errorf("MCP tools do not support authInject=body")
	}
	authInject, err = applyToolBodyAuth(params, httpConfig, authToken)
	if err != nil {
		return preparedToolRequest{}, err
	}

	url := httpConfig.URL
	method := httpConfig.Method
	if method == "" {
		method = http.MethodPost
	}
	var body []byte
	if isMCP {
		method = http.MethodPost
		body, err = marshalMCPToolCallRequest(tool, params)
		if err != nil {
			return preparedToolRequest{}, err
		}
	} else {
		url = interpolateToolURL(url, params)
		body, err = json.Marshal(params)
		if err != nil {
			return preparedToolRequest{}, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return preparedToolRequest{}, fmt.Errorf("failed to create request: %w", err)
	}
	if routeHost != "" {
		req.Host = routeHost
	}

	req.Header.Set("Content-Type", "application/json")
	if isMCP {
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(mcpProtocolVersionHeader, mcpProtocolVersion)
	}
	configuredSecrets := []string{}
	for k, v := range httpConfig.Headers {
		req.Header.Set(k, v)
		if sensitiveToolHeader(k) {
			configuredSecrets = append(configuredSecrets, sensitiveHeaderValues(v)...)
		}
	}
	approvalIdempotencyKey := toolIdempotencyKeyFromContext(ctx)
	if approvalIdempotencyKey != "" {
		if existing := strings.TrimSpace(req.Header.Get(toolIdempotencyKeyHeader)); existing != "" && existing != approvalIdempotencyKey {
			return preparedToolRequest{}, fmt.Errorf("tool configured reserved header %q while approval idempotency is enabled", toolIdempotencyKeyHeader)
		}
		if isMCP {
			req.Header.Del(toolIdempotencyKeyHeader)
		} else {
			req.Header.Set(toolIdempotencyKeyHeader, approvalIdempotencyKey)
		}
	}
	if authInject == toolAuthInjectHeader && authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	if values, ok := req.Header[http.CanonicalHeaderKey(transactiontoken.HeaderName)]; ok && len(values) > 0 {
		return preparedToolRequest{}, fmt.Errorf("tool configured reserved header %q", transactiontoken.HeaderName)
	}

	transactionToken := ""
	policyBacked := tool != nil && tool.Spec.HTTP != nil && tool.Spec.HTTP.OutboundAccessPolicyRef != nil
	if !policyBacked {
		transactionToken, err = e.outboundTransactionToken(ctx, tool)
		if err != nil {
			return preparedToolRequest{}, err
		}
		if transactionToken != "" {
			req.Header.Set(transactiontoken.HeaderName, transactionToken)
		}
	}

	prepared := preparedToolRequest{
		httpConfig:        httpConfig,
		request:           req,
		authToken:         authToken,
		transactionToken:  transactionToken,
		redactionSecrets:  compactToolSecrets(append(configuredSecrets, authToken, transactionToken)...),
		mcp:               isMCP,
		trustedActorRoute: isMCP && routeHost != "",
	}
	if tool != nil && tool.Spec.HTTP != nil && tool.Spec.HTTP.OutboundAccessPolicyRef != nil {
		if err := e.applyOutboundAccessPolicy(ctx, tool, &prepared); err != nil {
			return preparedToolRequest{}, err
		}
	}
	return prepared, nil
}

func (e *ToolExecutor) applyOutboundAccessPolicy(ctx context.Context, tool *corev1alpha1.Tool, prepared *preparedToolRequest) error {
	if e.outboundResolver == nil {
		return errors.New("outbound access policy is configured but the resolver is unavailable")
	}
	ref := tool.Spec.HTTP.OutboundAccessPolicyRef
	if ref == nil || strings.TrimSpace(ref.Name) == "" {
		return errors.New("outbound access policy reference name is required")
	}
	parentScopes := e.currentParentTransactionScopes()
	transactionTokenSource := func() (string, error) {
		if strings.TrimSpace(prepared.transactionToken) != "" {
			return prepared.transactionToken, nil
		}
		token, err := e.outboundTransactionToken(ctx, tool)
		if err != nil {
			return "", err
		}
		prepared.transactionToken = token
		prepared.redactionSecrets = compactToolSecrets(append(prepared.redactionSecrets, token)...)
		if token != "" {
			prepared.request.Header.Set(transactiontoken.HeaderName, token)
		}
		return token, nil
	}
	targetScheme := ""
	if prepared.request != nil && prepared.request.URL != nil {
		targetScheme = prepared.request.URL.Scheme
	}
	resolution, err := e.outboundResolver.Resolve(ctx, outboundaccess.ResolveRequest{
		Namespace:                   e.namespace,
		PolicyName:                  ref.Name,
		TransactionToken:            prepared.transactionToken,
		TransactionTokenSource:      transactionTokenSource,
		ParentTransactionScopes:     parentScopes,
		HasAuthSecretRef:            prepared.httpConfig.AuthSecretRef != nil,
		TargetScheme:                targetScheme,
		CredentialAuthorityEnforced: e.credentialAuthorityEnforced,
		CredentialScopeAllowed:      e.credentialScopeAllowed,
		CredentialSecret:            e.credentialSecret,
	})
	if err != nil {
		return fmt.Errorf("resolve outbound access policy: %w", err)
	}
	prepared.redactionSecrets = compactToolSecrets(append(prepared.redactionSecrets, resolution.SensitiveValues...)...)
	switch resolution.Adapter {
	case outboundaccess.AdapterDirect:
		prepared.direct = true
		if prepared.request == nil || prepared.request.URL == nil || !strings.EqualFold(prepared.request.URL.Scheme, "https") {
			return errors.New("direct outbound access requires an HTTPS Tool URL")
		}
		if prepared.httpConfig.AuthSecretRef != nil {
			return errors.New("direct outbound access cannot coexist with authSecretRef")
		}
		header := http.CanonicalHeaderKey(strings.TrimSpace(resolution.CredentialHeader))
		if header == "" || strings.EqualFold(header, transactiontoken.HeaderName) {
			return errors.New("direct outbound access returned an invalid credential header")
		}
		if values, ok := prepared.request.Header[header]; ok && len(values) > 0 {
			return fmt.Errorf("outbound credential header %q collides with an existing Tool header", header)
		}
		if strings.TrimSpace(resolution.CredentialValue) == "" {
			return errors.New("direct outbound access returned an empty credential")
		}
		prepared.request.Header.Del(transactiontoken.HeaderName)
		prepared.request.Header.Set(header, resolution.CredentialValue)
	case outboundaccess.AdapterGateway:
		return e.applyGatewayOutboundAccess(ctx, prepared, resolution, parentScopes, transactionTokenSource)
	default:
		return fmt.Errorf("unsupported outbound access adapter %q", resolution.Adapter)
	}
	return nil
}

func (e *ToolExecutor) applyGatewayOutboundAccess(
	ctx context.Context,
	prepared *preparedToolRequest,
	resolution outboundaccess.Resolution,
	parentScopes []string,
	transactionTokenSource func() (string, error),
) error {
	if prepared.request == nil || prepared.request.URL == nil {
		return errors.New("gateway outbound access requires a prepared Tool request")
	}
	originalAuthority := strings.TrimSpace(prepared.request.Host)
	if originalAuthority == "" {
		originalAuthority = prepared.request.URL.Host
	}
	if originalAuthority == "" || strings.TrimSpace(resolution.GatewayHost) == "" {
		return errors.New("gateway outbound access could not resolve request authorities")
	}
	if !e.skipDirectPublicValidation {
		trustedActorRoute := prepared.mcp && strings.TrimSpace(prepared.request.Host) != ""
		if err := validateGatewayOriginalTarget(
			ctx,
			prepared.request.URL,
			originalAuthority,
			trustedActorRoute,
		); err != nil {
			return err
		}
	}
	if strings.TrimSpace(prepared.transactionToken) == "" {
		if _, err := transactionTokenSource(); err != nil {
			return err
		}
	}
	if len(parentScopes) > 0 && strings.TrimSpace(prepared.transactionToken) == "" {
		return errors.New("gateway outbound access requires task-scoped transaction authority")
	}
	prepared.request.URL.Scheme = resolution.GatewayScheme
	prepared.request.URL.Host = resolution.GatewayHost
	prepared.request.Host = originalAuthority
	prepared.gatewayTLS = resolution.GatewayTLS
	prepared.gateway = true
	return nil
}

func validateGatewayOriginalTarget(
	ctx context.Context,
	requestURL *neturl.URL,
	authority string,
	trustedActorRoute bool,
) error {
	if requestURL == nil {
		return errors.New("gateway outbound access requires a prepared Tool request")
	}
	if !strings.EqualFold(requestURL.Scheme, "http") && !strings.EqualFold(requestURL.Scheme, "https") {
		return errors.New("gateway outbound access requires an HTTP or HTTPS Tool URL")
	}
	if requestURL.User != nil {
		return errors.New("gateway outbound access Tool URL must not contain embedded credentials")
	}
	authorityURL, err := neturl.Parse(requestURL.Scheme + "://" + strings.TrimSpace(authority))
	if err != nil || authorityURL.Hostname() == "" || authorityURL.Path != "" || authorityURL.RawQuery != "" || authorityURL.Fragment != "" {
		return errors.New("gateway outbound access original Tool authority is invalid")
	}
	if authorityURL.User != nil {
		return errors.New("gateway outbound access original Tool authority must not contain embedded credentials")
	}
	if trustedActorRoute {
		// Actor route hosts and router endpoints are controller-resolved Tool status,
		// not caller-selected public Tool URLs. The trusted gateway owns that route.
		return nil
	}
	if err := tokenexchange.ValidatePublicEndpointHost(ctx, authorityURL.Hostname()); err != nil {
		return fmt.Errorf("gateway outbound access requires a public original Tool URL: %w", err)
	}
	return nil
}

func sensitiveHeaderValues(value string) []string {
	values := []string{value}
	fields := strings.Fields(value)
	if len(fields) == 2 && (strings.EqualFold(fields[0], "Bearer") || strings.EqualFold(fields[0], "Basic")) {
		values = append(values, fields[1])
	}
	return values
}

func sensitiveToolHeader(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	return normalized == "authorization" || normalized == "proxy-authorization" ||
		normalized == "cookie" || normalized == "x-api-key" ||
		strings.Contains(normalized, "token") || strings.Contains(normalized, "secret") ||
		strings.Contains(normalized, "credential")
}

func compactToolSecrets(values ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func toolHTTPConfig(tool *corev1alpha1.Tool) (corev1alpha1.HTTPExecution, string, error) {
	var httpConfig corev1alpha1.HTTPExecution
	if tool != nil && tool.Spec.HTTP != nil {
		httpConfig = *tool.Spec.HTTP
	}
	if !isMCPSubstrateActorTool(tool) {
		if tool == nil || tool.Spec.HTTP == nil {
			return corev1alpha1.HTTPExecution{}, "", fmt.Errorf("http tool endpoint is not configured")
		}
		return httpConfig, "", nil
	}

	httpConfig.URL = strings.TrimSpace(tool.Status.Endpoint)
	if httpConfig.Method == "" {
		httpConfig.Method = http.MethodPost
	}
	routeHost := ""
	if tool.Status.Actor != nil {
		routeHost = strings.TrimSpace(tool.Status.Actor.RouteHost)
	}
	if httpConfig.URL == "" || routeHost == "" {
		return corev1alpha1.HTTPExecution{}, "", fmt.Errorf("MCP tool actor endpoint is not ready")
	}
	return httpConfig, routeHost, nil
}

func isMCPSubstrateActorTool(tool *corev1alpha1.Tool) bool {
	return tool != nil && tool.Spec.MCP != nil && tool.Spec.MCP.SubstrateActor != nil
}

type mcpToolCallRequest struct {
	JSONRPC string                `json:"jsonrpc"`
	ID      string                `json:"id"`
	Method  string                `json:"method"`
	Params  mcpToolCallParameters `json:"params"`
}

type mcpToolCallParameters struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

type mcpInitializeRequest struct {
	JSONRPC string                  `json:"jsonrpc"`
	ID      string                  `json:"id"`
	Method  string                  `json:"method"`
	Params  mcpInitializeParameters `json:"params"`
}

type mcpInitializeParameters struct {
	ProtocolVersion string                  `json:"protocolVersion"`
	Capabilities    map[string]any          `json:"capabilities"`
	ClientInfo      mcpInitializeClientInfo `json:"clientInfo"`
}

type mcpInitializeClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type mcpNotificationRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
}

func marshalMCPToolCallRequest(tool *corev1alpha1.Tool, arguments map[string]any) ([]byte, error) {
	toolName := strings.TrimSpace(tool.Name)
	if toolName == "" {
		return nil, fmt.Errorf("MCP tool name is required")
	}
	body, err := json.Marshal(mcpToolCallRequest{
		JSONRPC: mcpJSONRPCVersion,
		ID:      mcpToolCallRequestID,
		Method:  mcpToolsCallMethod,
		Params: mcpToolCallParameters{
			Name:      toolName,
			Arguments: arguments,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal MCP tool call request: %w", err)
	}
	return body, nil
}

func marshalMCPInitializeRequest() ([]byte, error) {
	body, err := json.Marshal(mcpInitializeRequest{
		JSONRPC: mcpJSONRPCVersion,
		ID:      mcpInitializeRequestID,
		Method:  mcpInitializeMethod,
		Params: mcpInitializeParameters{
			ProtocolVersion: mcpProtocolVersion,
			Capabilities:    map[string]any{},
			ClientInfo: mcpInitializeClientInfo{
				Name:    "orka",
				Version: "dev",
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal MCP initialize request: %w", err)
	}
	return body, nil
}

func marshalMCPInitializedNotification() ([]byte, error) {
	body, err := json.Marshal(mcpNotificationRequest{
		JSONRPC: mcpJSONRPCVersion,
		Method:  mcpInitializedNotificationMethod,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal MCP initialized notification: %w", err)
	}
	return body, nil
}

type mcpToolCallResponse struct {
	JSONRPC string           `json:"jsonrpc,omitempty"`
	ID      any              `json:"id,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *mcpJSONRPCError `json:"error,omitempty"`
}

type mcpJSONRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type mcpInitializeResult struct {
	ProtocolVersion string `json:"protocolVersion,omitempty"`
}

type mcpToolCallResult struct {
	Content []mcpToolContent `json:"content,omitempty"`
	IsError bool             `json:"isError,omitempty"`
}

type mcpToolContent struct {
	Type string          `json:"type,omitempty"`
	Text string          `json:"text,omitempty"`
	Raw  json.RawMessage `json:"-"`
}

func (e *ToolExecutor) executeMCPToolCall(ctx context.Context, httpClient *http.Client, prepared preparedToolRequest) (result string, err error) {
	sessionID, protocolVersion, err := e.initializeMCP(ctx, httpClient, prepared)
	if err != nil {
		return "", err
	}
	if sessionID != "" {
		defer func() {
			_ = e.terminateMCPSession(ctx, httpClient, prepared, sessionID, protocolVersion)
		}()
		prepared.request.Header.Set(mcpSessionIDHeader, sessionID)
	}
	prepared.request.Header.Set(mcpProtocolVersionHeader, protocolVersion)
	if err := e.sendMCPInitializedNotification(ctx, httpClient, prepared, sessionID, protocolVersion); err != nil {
		return "", err
	}
	if key := toolIdempotencyKeyFromContext(prepared.request.Context()); key != "" {
		prepared.request.Header.Set(toolIdempotencyKeyHeader, key)
	}
	respBody, err := executeMCPHTTPRequest(httpClient, prepared.request, mcpToolCallRequestID, prepared.secrets(sessionID)...)
	if err != nil {
		return "", ToolRequestAttemptedError{Err: err}
	}
	result, err = decodeMCPToolCallResponse(respBody, prepared.secrets(sessionID)...)
	if err != nil {
		return "", ToolRequestAttemptedError{Err: err}
	}
	return result, nil
}

func (e *ToolExecutor) initializeMCP(ctx context.Context, httpClient *http.Client, prepared preparedToolRequest) (string, string, error) {
	body, err := marshalMCPInitializeRequest()
	if err != nil {
		return "", "", err
	}
	req, err := newMCPRequest(ctx, prepared, body, mcpProtocolVersion)
	if err != nil {
		return "", "", fmt.Errorf("failed to create MCP initialize request: %w", err)
	}
	resp, err := executeMCPHTTPRequestWithResponse(httpClient, req, mcpInitializeRequestID, prepared.secrets()...)
	sessionID := strings.TrimSpace(resp.header.Get(mcpSessionIDHeader))
	if err != nil {
		if sessionID != "" {
			_ = e.terminateMCPSession(ctx, httpClient, prepared, sessionID, mcpProtocolVersion)
		}
		return "", "", fmt.Errorf("MCP initialize failed: %w", err)
	}
	protocolVersion, err := mcpInitializedProtocolVersion(resp.body, prepared.secrets(sessionID)...)
	if err != nil {
		if sessionID != "" {
			_ = e.terminateMCPSession(ctx, httpClient, prepared, sessionID, mcpProtocolVersion)
		}
		return "", "", fmt.Errorf("MCP initialize failed: %w", err)
	}
	return sessionID, protocolVersion, nil
}

func (e *ToolExecutor) sendMCPInitializedNotification(ctx context.Context, httpClient *http.Client, prepared preparedToolRequest, sessionID string, protocolVersion string) error {
	body, err := marshalMCPInitializedNotification()
	if err != nil {
		return err
	}
	req, err := newMCPRequest(ctx, prepared, body, protocolVersion)
	if err != nil {
		return fmt.Errorf("failed to create MCP initialized notification: %w", err)
	}
	if sessionID != "" {
		req.Header.Set(mcpSessionIDHeader, sessionID)
	}
	if _, err := executeMCPHTTPRequestWithResponse(httpClient, req, "", prepared.secrets(sessionID)...); err != nil {
		return fmt.Errorf("MCP initialized notification failed: %w", err)
	}
	return nil
}

func (e *ToolExecutor) terminateMCPSession(ctx context.Context, httpClient *http.Client, prepared preparedToolRequest, sessionID, protocolVersion string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mcpSessionTerminateTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, prepared.request.URL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create MCP session termination request: %w", err)
	}
	req.Host = prepared.request.Host
	req.Header = prepared.request.Header.Clone()
	if toolIdempotencyKeyFromContext(prepared.request.Context()) != "" {
		req.Header.Del(toolIdempotencyKeyHeader)
	}
	req.Header.Del("Content-Type")
	req.Header.Set(mcpSessionIDHeader, sessionID)
	req.Header.Set(mcpProtocolVersionHeader, mcpProtocolHeaderValue(protocolVersion))
	injectTraceHeaders(cleanupCtx, req.Header)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("MCP session termination request failed: %w", safeToolTransportError(req.Context(), err, prepared.secrets(sessionID)...))
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := readLimitedHTTPResponseBody(resp.Body, resp.ContentLength)
	if err != nil {
		return fmt.Errorf("failed to read MCP session termination response: %w", err)
	}
	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return nil
	}
	return fmt.Errorf(
		"MCP session termination returned HTTP %d: %s",
		resp.StatusCode,
		redactToolHTTPErrorBody(string(respBody), prepared.secrets(sessionID)...),
	)
}

func newMCPRequest(ctx context.Context, prepared preparedToolRequest, body []byte, protocolVersion string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, prepared.request.URL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Host = prepared.request.Host
	req.Header = prepared.request.Header.Clone()
	if toolIdempotencyKeyFromContext(prepared.request.Context()) != "" {
		req.Header.Del(toolIdempotencyKeyHeader)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(mcpProtocolVersionHeader, mcpProtocolHeaderValue(protocolVersion))
	injectTraceHeaders(ctx, req.Header)
	return req, nil
}

type mcpHTTPResponse struct {
	body   []byte
	header http.Header
}

func executeMCPHTTPRequest(httpClient *http.Client, req *http.Request, expectedResponseID string, secrets ...string) ([]byte, error) {
	resp, err := executeMCPHTTPRequestWithResponse(httpClient, req, expectedResponseID, secrets...)
	if err != nil {
		return nil, err
	}
	return resp.body, nil
}

func executeMCPHTTPRequestWithResponse(httpClient *http.Client, req *http.Request, expectedResponseID string, secrets ...string) (mcpHTTPResponse, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		return mcpHTTPResponse{}, safeToolTransportError(req.Context(), err, secrets...)
	}
	defer resp.Body.Close() //nolint:errcheck
	mcpResp := mcpHTTPResponse{header: resp.Header.Clone()}

	respBody, err := readMCPHTTPResponseBody(resp, expectedResponseID)
	if err != nil {
		return mcpResp, fmt.Errorf("failed to read response: %w", err)
	}
	mcpResp.body = respBody
	redactionSecrets := append([]string{}, secrets...)
	redactionSecrets = append(redactionSecrets, resp.Header.Get(mcpSessionIDHeader))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return mcpResp, fmt.Errorf("tool returned HTTP %d: %s", resp.StatusCode, redactToolHTTPErrorBody(string(respBody), redactionSecrets...))
	}
	if err := validateMCPExpectedResponseID(respBody, expectedResponseID); err != nil {
		return mcpResp, err
	}
	return mcpResp, nil
}

func readMCPHTTPResponseBody(resp *http.Response, expectedResponseID string) ([]byte, error) {
	if isMCPEventStream(resp.Header.Get("Content-Type")) && strings.TrimSpace(expectedResponseID) != "" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return readMCPEventStreamResponse(resp.Body, expectedResponseID)
	}
	respBody, err := readLimitedHTTPResponseBody(resp.Body, resp.ContentLength)
	if err != nil {
		return nil, err
	}
	return normalizeMCPResponseBody(resp.Header.Get("Content-Type"), respBody), nil
}

func mcpInitializedProtocolVersion(body []byte, secrets ...string) (string, error) {
	response, err := parseMCPResponse(body, mcpInitializeRequestID, secrets...)
	if err != nil {
		return "", err
	}
	var result mcpInitializeResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return "", fmt.Errorf("failed to parse MCP initialize result: %w", err)
	}
	protocolVersion := strings.TrimSpace(result.ProtocolVersion)
	if protocolVersion == "" {
		return "", fmt.Errorf("MCP initialize result missing protocolVersion")
	}
	return protocolVersion, nil
}

func parseMCPResponse(body []byte, expectedID string, secrets ...string) (mcpToolCallResponse, error) {
	var response mcpToolCallResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return mcpToolCallResponse{}, fmt.Errorf("failed to parse MCP response: %w", err)
	}
	if err := validateMCPResponseID(response, expectedID); err != nil {
		return mcpToolCallResponse{}, err
	}
	if response.Error != nil {
		message := response.Error.Message
		if len(response.Error.Data) > 0 {
			message = strings.TrimSpace(message + ": " + string(response.Error.Data))
		}
		return mcpToolCallResponse{}, fmt.Errorf("MCP returned error %d: %s", response.Error.Code, redactToolHTTPErrorBody(message, secrets...))
	}
	if len(response.Result) == 0 {
		return mcpToolCallResponse{}, fmt.Errorf("MCP response missing result")
	}
	return response, nil
}

func validateMCPExpectedResponseID(body []byte, expectedID string) error {
	expectedID = strings.TrimSpace(expectedID)
	if expectedID == "" {
		return nil
	}
	var response mcpToolCallResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse MCP response: %w", err)
	}
	return validateMCPResponseID(response, expectedID)
}

func validateMCPResponseID(response mcpToolCallResponse, expectedID string) error {
	expectedID = strings.TrimSpace(expectedID)
	if expectedID == "" || mcpJSONRPCResponseMatchesID(response, expectedID) {
		return nil
	}
	return fmt.Errorf("MCP response id did not match expected %q", expectedID)
}

func mcpProtocolHeaderValue(protocolVersion string) string {
	protocolVersion = strings.TrimSpace(protocolVersion)
	if protocolVersion == "" {
		return mcpProtocolVersion
	}
	return protocolVersion
}

func normalizeMCPResponseBody(contentType string, body []byte) []byte {
	if !isMCPEventStream(contentType) {
		return body
	}
	if data := firstSSEData(body); len(data) > 0 {
		return data
	}
	return body
}

func isMCPEventStream(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/event-stream")
}

func readMCPEventStreamResponse(body io.Reader, expectedResponseID string) ([]byte, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), toolHTTPResponseBodyLimit)
	totalDataBytes := 0
	var data []string
	flush := func() []byte {
		if len(data) == 0 {
			return nil
		}
		joined := strings.TrimSpace(strings.Join(data, "\n"))
		data = nil
		if joined == "" || joined == "[DONE]" {
			return nil
		}
		eventBody := []byte(joined)
		if !isMCPJSONRPCResponse(eventBody, expectedResponseID) {
			return nil
		}
		return eventBody
	}
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if event := flush(); len(event) > 0 {
				return event, nil
			}
			continue
		}
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			after = strings.TrimPrefix(after, " ")
			totalDataBytes += len(after)
			if totalDataBytes > toolHTTPResponseBodyLimit {
				return nil, fmt.Errorf("MCP event stream response exceeded 10MB limit")
			}
			data = append(data, after)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if event := flush(); len(event) > 0 {
		return event, nil
	}
	return nil, fmt.Errorf("MCP event stream ended before response %q", expectedResponseID)
}

func firstSSEData(body []byte) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), toolHTTPResponseBodyLimit)
	var data []string
	flush := func() []byte {
		if len(data) == 0 {
			return nil
		}
		joined := strings.TrimSpace(strings.Join(data, "\n"))
		data = nil
		if joined == "" || joined == "[DONE]" {
			return nil
		}
		eventBody := []byte(joined)
		if !isMCPJSONRPCResponse(eventBody, "") {
			return nil
		}
		return eventBody
	}
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if event := flush(); len(event) > 0 {
				return event
			}
			continue
		}
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimSpace(after))
		}
	}
	return flush()
}

func isMCPJSONRPCResponse(body []byte, expectedID string) bool {
	var response mcpToolCallResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}
	if response.Error == nil && len(response.Result) == 0 {
		return false
	}
	return mcpJSONRPCResponseMatchesID(response, expectedID)
}

func mcpJSONRPCResponseMatchesID(response mcpToolCallResponse, expectedID string) bool {
	if strings.TrimSpace(expectedID) == "" {
		return true
	}
	if response.ID == nil {
		return response.Error != nil
	}
	return strings.TrimSpace(fmt.Sprint(response.ID)) == strings.TrimSpace(expectedID)
}

func (c *mcpToolContent) UnmarshalJSON(data []byte) error {
	type mcpToolContentAlias struct {
		Type string `json:"type,omitempty"`
		Text string `json:"text,omitempty"`
	}
	var parsed mcpToolContentAlias
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	c.Type = parsed.Type
	c.Text = parsed.Text
	c.Raw = append(json.RawMessage(nil), data...)
	return nil
}

func decodeMCPToolCallResponse(body []byte, secrets ...string) (string, error) {
	var response mcpToolCallResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("failed to parse MCP response: %w", err)
	}
	if response.Error != nil {
		message := response.Error.Message
		if len(response.Error.Data) > 0 {
			message = strings.TrimSpace(message + ": " + string(response.Error.Data))
		}
		return "", fmt.Errorf("MCP tool returned error %d: %s", response.Error.Code, redactToolHTTPErrorBody(message, secrets...))
	}
	if len(response.Result) == 0 {
		return "", fmt.Errorf("MCP response missing result")
	}

	var result mcpToolCallResult
	if err := json.Unmarshal(response.Result, &result); err == nil && (len(result.Content) > 0 || result.IsError) {
		text := mcpToolContentText(result.Content)
		if result.IsError {
			if response.JSONRPC != mcpJSONRPCVersion || result.Content == nil {
				return "", errors.New("MCP tool returned an invalid error result")
			}
			if text == "" {
				text = string(response.Result)
			}
			return "", ToolExecutionError{Err: fmt.Errorf("MCP tool reported error: %s", redactToolHTTPErrorBody(text, secrets...))}
		}
		if text != "" {
			return redactToolSensitiveText(text, secrets...), nil
		}
	}

	return redactToolSensitiveText(string(response.Result), secrets...), nil
}

func mcpToolContentText(content []mcpToolContent) string {
	parts := make([]string, 0, len(content))
	for _, item := range content {
		if item.Type == "text" && item.Text != "" {
			parts = append(parts, item.Text)
			continue
		}
		if len(item.Raw) > 0 {
			parts = append(parts, string(item.Raw))
			continue
		}
		raw, err := json.Marshal(item)
		if err == nil && string(raw) != "{}" {
			parts = append(parts, string(raw))
		}
	}
	return strings.Join(parts, "\n")
}

func applyToolBodyAuth(params map[string]any, httpConfig corev1alpha1.HTTPExecution, authToken string) (string, error) {
	authInject := toolAuthInject(httpConfig)
	if authInject != "body" || authToken == "" {
		return authInject, nil
	}
	bodyKey := httpConfig.AuthBodyKey
	if bodyKey == "" {
		return "", fmt.Errorf("authBodyKey is required when authInject=body")
	}
	params[bodyKey] = authToken
	return authInject, nil
}

func toolAuthInject(httpConfig corev1alpha1.HTTPExecution) string {
	if httpConfig.AuthInject == "" {
		return toolAuthInjectHeader
	}
	return httpConfig.AuthInject
}

func interpolateToolURL(url string, params map[string]any) string {
	if !strings.Contains(url, "{{") {
		return url
	}
	var interpolatedKeys []string
	for key, val := range params {
		placeholder := "{{" + key + "}}"
		if strings.Contains(url, placeholder) {
			url = strings.ReplaceAll(url, placeholder, neturl.PathEscape(fmt.Sprintf("%v", val)))
			interpolatedKeys = append(interpolatedKeys, key)
		}
	}
	for _, key := range interpolatedKeys {
		delete(params, key)
	}
	return url
}

func safeToolTransportError(ctx context.Context, err error, secrets ...string) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("tool request failed: " + redactToolHTTPErrorBody(err.Error(), secrets...))
}

func redactToolSensitiveText(body string, secrets ...string) string {
	redacted := body
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		redacted = strings.ReplaceAll(redacted, secret, "[REDACTED]")
	}
	return redact.SensitiveText(redacted)
}

func redactToolHTTPErrorBody(body string, secrets ...string) string {
	redacted := redactToolSensitiveText(body, secrets...)
	if len(redacted) > toolHTTPErrorBodyLimit {
		redacted = redacted[:toolHTTPErrorBodyLimit] + "...[truncated]"
	}
	return redacted
}

// getSecretKey reads a key from a secret (mounted path or Kubernetes API)
func (e *ToolExecutor) getSecretKey(ctx context.Context, secretName, key string) (string, error) {
	if e != nil {
		if err := outboundaccess.ValidateCredentialAuthority(
			e.credentialAuthorityEnforced,
			e.credentialScopeAllowed,
			e.credentialSecret,
			[]string{secretName},
			false,
		); err != nil {
			return "", err
		}
	}
	if e != nil && e.authSecretValues != nil {
		if value, ok := e.authSecretValues[secretName+"\x00"+key]; ok {
			if value == "" {
				return "", fmt.Errorf("secret %s/%s is empty", secretName, key)
			}
			return value, nil
		}
	}
	// Try mounted secret paths first
	paths := []string{
		fmt.Sprintf("%s/%s/%s", e.secretPath, secretName, key),
		fmt.Sprintf("/secrets/task/%s", key),
		fmt.Sprintf("/secrets/agent/%s", key),
	}

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(data)), nil
		}
	}

	// Fall back to Kubernetes API
	if e.k8sClient != nil {
		secret, err := e.k8sClient.CoreV1().Secrets(e.namespace).Get(ctx, secretName, metav1.GetOptions{})
		if err == nil {
			if data, ok := secret.Data[key]; ok {
				return strings.TrimSpace(string(data)), nil
			}
		}
	}

	return "", fmt.Errorf("secret %s/%s not found", secretName, key)
}

func (e *ToolExecutor) outboundTransactionToken(ctx context.Context, tool *corev1alpha1.Tool) (string, error) {
	ttsConfig, subjectTokenTypeOverride, outboundScopeOverride, err := e.toolTransactionExchangeConfig()
	if err != nil {
		return "", err
	}
	if !ttsConfig.Enabled() {
		return e.currentTransactionToken()
	}
	subjectToken, err := e.outboundTTSSubjectToken(ttsConfig.TokenSource)
	if err != nil {
		return "", err
	}
	scope := strings.TrimSpace(outboundScopeOverride)
	if scope == "" {
		scope = strings.TrimSpace(os.Getenv(workerenv.ContextTokenOutboundScope))
	}
	if scope == "" && e != nil && e.transactionAuthoritySet {
		scope = strings.Join(e.currentParentTransactionScopes(), " ")
	}
	if scope == "" {
		scope = strings.TrimSpace(os.Getenv(workerenv.TransactionScope))
	}
	if scope == "" {
		return "", fmt.Errorf("%s or %s is required when %s is set", workerenv.ContextTokenOutboundScope, workerenv.TransactionScope, workerenv.ContextTokenTTSEndpoint)
	}
	if err := e.validateOutboundTransactionScope(scope); err != nil {
		return "", err
	}
	subjectTokenType := strings.TrimSpace(subjectTokenTypeOverride)
	if subjectTokenType == "" {
		subjectTokenType = strings.TrimSpace(os.Getenv(workerenv.ContextTokenSubjectTokenType))
	}
	if subjectTokenType == "" {
		subjectTokenType = contexttoken.SubjectTokenTypeForSource(ttsConfig.TokenSource)
	}
	requestDetails := map[string]any{
		"operation": "httpTool",
		"tool":      tool.Name,
		"namespace": e.namespace,
	}
	if taskName := strings.TrimSpace(os.Getenv(workerenv.TaskName)); taskName != "" {
		requestDetails["task"] = taskName
	}
	exchanger, err := e.transactionTokenExchanger(ttsConfig)
	if err != nil {
		return "", err
	}
	token, err := exchanger.Exchange(ctx, contexttoken.ExchangeRequest{
		SubjectToken:     subjectToken,
		SubjectTokenType: subjectTokenType,
		Scope:            scope,
		RequestedTTL:     ttsConfig.ToolTokenTTL,
		RequestDetails:   requestDetails,
	})
	if err != nil {
		return "", fmt.Errorf("token exchange failed: %w", err)
	}
	return token, nil
}

func (e *ToolExecutor) toolTransactionExchangeConfig() (contexttoken.TTSConfig, string, string, error) {
	if e != nil && e.transactionExchange != nil {
		return e.transactionExchange.TTS, e.transactionExchange.SubjectTokenType, e.transactionExchange.OutboundScope, nil
	}
	ttsEndpoint := strings.TrimSpace(os.Getenv(workerenv.ContextTokenTTSEndpoint))
	if ttsEndpoint == "" {
		return contexttoken.TTSConfig{TokenSource: contexttoken.TTSTokenSourceNone}, "", "", nil
	}
	config, err := contexttoken.NewTTSConfig(
		ttsEndpoint,
		os.Getenv(workerenv.ContextTokenTTSAudience),
		os.Getenv(workerenv.ContextTokenTTSTimeout),
		os.Getenv(workerenv.ContextTokenTTSTokenSource),
		"",
		os.Getenv(workerenv.ContextTokenToolTokenTTL),
	)
	return config, "", "", err
}

func containsAnyString(actual, required []string) bool {
	for _, value := range actual {
		if slices.Contains(required, value) {
			return true
		}
	}
	return false
}

func (e *ToolExecutor) currentParentTransactionScopes() []string {
	if e != nil && e.transactionAuthoritySet {
		return normalizeToolScopes(e.transactionScopes)
	}
	if parentScope := strings.TrimSpace(os.Getenv(workerenv.TransactionScope)); parentScope != "" {
		return normalizeToolScopes([]string{parentScope})
	}
	return normalizeToolScopes(workerenv.SplitCSV(os.Getenv(workerenv.TransactionScopes)))
}

func normalizeToolScopes(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		for scope := range strings.FieldsSeq(value) {
			if _, ok := seen[scope]; ok {
				continue
			}
			seen[scope] = struct{}{}
			out = append(out, scope)
		}
	}
	return out
}

func (e *ToolExecutor) currentTransactionToken() (string, error) {
	if e != nil && e.transactionAuthoritySet {
		return e.transactionToken, nil
	}
	if token, ok, err := workerenv.ReadTokenFileEnv(workerenv.TransactionTokenFile, "transaction token"); ok || err != nil {
		return token, err
	}
	return "", nil
}

func (e *ToolExecutor) validateOutboundTransactionScope(scope string) error {
	requested := strings.Fields(scope)
	if len(requested) == 0 {
		return fmt.Errorf("outbound transaction scope is required")
	}
	parent := e.currentParentTransactionScopes()
	if len(parent) == 0 {
		return fmt.Errorf("parent transaction scopes are required for outbound token exchange")
	}
	for _, child := range requested {
		if !slices.Contains(parent, child) {
			return fmt.Errorf("outbound transaction scope %q is not present in parent transaction scopes", child)
		}
	}
	return nil
}

func (e *ToolExecutor) outboundTTSSubjectToken(tokenSource string) (string, error) {
	switch tokenSource {
	case contexttoken.TTSTokenSourceIncoming:
		if e != nil && e.transactionAuthoritySet {
			if e.transactionToken == "" {
				return "", errors.New("task-scoped incoming transaction authority is unavailable")
			}
			return e.transactionToken, nil
		}
		if token, ok, err := workerenv.ReadTokenFileEnv(workerenv.ContextTokenSubjectTokenFile, "context token subject token"); ok || err != nil {
			return token, err
		}
		if token, ok, err := workerenv.ReadTokenFileEnv(workerenv.TransactionTokenFile, "transaction token"); ok || err != nil {
			return token, err
		}
		return "", fmt.Errorf("%s or %s is required when %s uses %q", workerenv.ContextTokenSubjectTokenFile, workerenv.TransactionTokenFile, workerenv.ContextTokenTTSTokenSource, tokenSource)
	case contexttoken.TTSTokenSourceServiceAccount:
		if e != nil && e.transactionAuthoritySet {
			return "", errors.New("task-scoped transaction authority cannot use a service account subject token")
		}
		return serviceAccountSubjectToken()
	case contexttoken.TTSTokenSourceNone:
		return "", fmt.Errorf("context token TTS token source %q does not provide a subject token", tokenSource)
	default:
		return "", fmt.Errorf("unsupported context token TTS token source %q", tokenSource)
	}
}

func serviceAccountSubjectToken() (string, error) {
	if token := strings.TrimSpace(os.Getenv(workerenv.ServiceAccountToken)); token != "" {
		return token, nil
	}
	return workerenv.ReadTokenFile(workerenv.ServiceAccountTokenFile, "service account token")
}

func (e *ToolExecutor) transactionTokenExchanger(cfg contexttoken.TTSConfig) (contexttoken.Exchanger, error) {
	if e != nil && e.transactionExchange != nil && e.transactionExchange.Exchanger != nil {
		return e.transactionExchange.Exchanger, nil
	}
	return e.outboundTTSClient(cfg)
}

func (e *ToolExecutor) outboundTTSClient(cfg contexttoken.TTSConfig) (*contexttoken.TTSClient, error) {
	key := outboundTTSClientKey(cfg)

	e.ttsMu.Lock()
	defer e.ttsMu.Unlock()

	if e.ttsClient != nil && e.ttsClientKey == key {
		return e.ttsClient, nil
	}

	ttsClient, err := contexttoken.NewTTSClient(cfg)
	if err != nil {
		return nil, err
	}
	e.ttsClient = ttsClient
	e.ttsClientKey = key
	return ttsClient, nil
}

func outboundTTSClientKey(cfg contexttoken.TTSConfig) string {
	return strings.Join([]string{
		strings.TrimSpace(cfg.Endpoint),
		strings.TrimSpace(cfg.Audience),
		cfg.Timeout.String(),
		strings.TrimSpace(cfg.TokenSource),
		cfg.ChildTokenTTL.String(),
		cfg.ToolTokenTTL.String(),
	}, "\x00")
}
