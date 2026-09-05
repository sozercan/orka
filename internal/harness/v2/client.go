package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OperationCapabilityHeader = "X-Orka-Operation-Capability"

	defaultControlTimeout       = 30 * time.Second
	defaultMaxJSONResponseBytes = int64(MaxCanonicalJSONBytes)
	defaultMaxErrorBodyBytes    = int64(64 << 10)
)

type ClientOption func(*Client) error

// Client is a strict controller-side HTTP client for orka.harness.v2. It never
// follows redirects, never logs credentials, and never retries mutating
// requests. The same operation identity may be resent only by its caller after
// inspecting RequestWriteEvidence and durable attempt state.
type Client struct {
	baseURL    *url.URL
	basePrefix string
	httpClient *http.Client

	controlTimeout       time.Duration
	controllerBearer     string
	capabilitySecret     []byte
	statusBinding        StatusCapabilityBinding
	maxJSONResponseBytes int64
	maxErrorBodyBytes    int64
	traceReliable        bool
	beforeMutation       func(context.Context, string) error

	limitsMu sync.RWMutex
	limits   ProtocolLimits
}

// WithBeforeMutation installs a fail-closed check that runs immediately before
// every mutating request is encoded, capability-signed, and sent. The check's
// context reserves half of the effective remaining window, bounded by both the
// request metadata expiry and caller deadline, for the mutation itself. It is
// intended for callers that must revalidate external authority between
// otherwise independent mutations.
func WithBeforeMutation(validate func(context.Context, string) error) ClientOption {
	return func(c *Client) error {
		if validate == nil {
			return fmt.Errorf("before-mutation validator is required")
		}
		c.beforeMutation = validate
		return nil
	}
}

func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) error {
		if httpClient == nil {
			return fmt.Errorf("HTTP client is required")
		}
		clone := *httpClient
		// Redirects would either leak the operation capability or silently route a
		// fenced operation to a different instance. Return the redirect response
		// to the caller instead.
		clone.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
		c.httpClient = &clone
		c.traceReliable = transportSupportsHTTPTrace(clone.Transport)
		return nil
	}
}

func WithControlTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) error {
		if timeout <= 0 {
			return fmt.Errorf("control timeout must be positive")
		}
		c.controlTimeout = timeout
		return nil
	}
}

func WithControllerBearerToken(token string) ClientOption {
	return func(c *Client) error {
		token = strings.TrimSpace(token)
		if len(token) < 32 {
			return fmt.Errorf("controller bearer token must be at least 32 bytes")
		}
		if !validHeaderCredential(token) {
			return fmt.Errorf("controller bearer token contains invalid header bytes")
		}
		c.controllerBearer = token
		return nil
	}
}

// WithStatusCapabilityBinding sets the profile digest the client binds into
// every status capability. Required before Status can be called.
func WithStatusCapabilityBinding(binding StatusCapabilityBinding) ClientOption {
	return func(c *Client) error {
		if strings.TrimSpace(string(binding.RuntimeProfileDigest)) == "" {
			return fmt.Errorf("status capability binding requires a profile digest")
		}
		c.statusBinding = binding
		return nil
	}
}

func WithOperationCapabilitySecret(secret []byte) ClientOption {
	return func(c *Client) error {
		if len(secret) < MinCapabilitySecretBytes {
			return fmt.Errorf("operation capability secret must be at least %d bytes", MinCapabilitySecretBytes)
		}
		c.capabilitySecret = append(c.capabilitySecret[:0], secret...)
		return nil
	}
}

// WithProtocolLimits configures negotiated limits before Capabilities has been
// called. A later successful Capabilities call atomically replaces these values
// with the supervisor-advertised limits.
func WithProtocolLimits(limits ProtocolLimits) ClientOption {
	return func(c *Client) error {
		if err := limits.Validate(); err != nil {
			return err
		}
		c.limits = limits
		return nil
	}
}

// NewProxylessTransport returns a clone of http.DefaultTransport with
// environment proxy resolution disabled. Supervisor control traffic targets
// exact in-cluster Pod endpoints; routing it through an inherited HTTP(S)_PROXY
// would either expose authenticated control headers to an intermediary or
// leave pools unreachable when the proxy cannot reach Pod IPs.
func NewProxylessTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return transport
}

func NewClient(baseURL string, options ...ClientOption) (*Client, error) {
	parsed, prefix, err := parseClientBaseURL(baseURL)
	if err != nil {
		return nil, clientError("new_client", ClientErrorConfiguration, err.Error(), err)
	}
	defaultHTTPClient := *http.DefaultClient
	defaultHTTPClient.Transport = NewProxylessTransport()
	defaultHTTPClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client := &Client{
		baseURL:              parsed,
		basePrefix:           prefix,
		httpClient:           &defaultHTTPClient,
		controlTimeout:       defaultControlTimeout,
		maxJSONResponseBytes: defaultMaxJSONResponseBytes,
		maxErrorBodyBytes:    defaultMaxErrorBodyBytes,
		traceReliable:        true,
		limits:               DefaultProtocolLimits(),
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(client); err != nil {
			return nil, clientError("new_client", ClientErrorConfiguration, err.Error(), err)
		}
	}
	return client, nil
}

func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	const operation = "health"
	var response HealthResponse
	if err := c.getJSON(ctx, operation, HealthPath, false, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) Capabilities(ctx context.Context) (*CapabilitiesResponse, error) {
	const operation = "capabilities"
	var response CapabilitiesResponse
	if err := c.getJSON(ctx, operation, CapabilitiesPath, false, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	c.limitsMu.Lock()
	c.limits = response.Limits
	c.limitsMu.Unlock()
	return &response, nil
}

func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	const operation = "status"
	if err := c.requireControllerAuth(operation); err != nil {
		return nil, err
	}
	if len(c.capabilitySecret) < MinCapabilitySecretBytes {
		return nil, clientError(operation, ClientErrorConfiguration, "status requires the operation capability secret", ErrClientConfiguration)
	}
	if strings.TrimSpace(string(c.statusBinding.RuntimeProfileDigest)) == "" {
		return nil, clientError(operation, ClientErrorConfiguration, "status requires the profile binding", ErrClientConfiguration)
	}
	nonce, err := NewCapabilityNonce()
	if err != nil {
		return nil, clientError(operation, ClientErrorConfiguration, "generate status nonce", err)
	}
	capability, err := SignStatusCapability(c.capabilitySecret, NewStatusCapabilityClaims(c.statusBinding, nonce, time.Now().UTC().Add(DefaultStatusCapabilityTTL)))
	if err != nil {
		return nil, clientError(operation, ClientErrorConfiguration, "sign status capability", err)
	}
	var response StatusResponse
	if err := c.getJSONWithCapability(ctx, operation, StatusPath, true, capability, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) CreateRuntimeSession(ctx context.Context, request CreateRuntimeSessionRequest) (*CreateRuntimeSessionResponse, error) {
	const operation = "create_runtime_session"
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := RuntimeSessionPath(request.RuntimeSessionID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response CreateRuntimeSessionResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) FinalizeRuntimeSessionPublication(ctx context.Context, sessionID RuntimeSessionID, request FinalizeRuntimeSessionPublicationRequest) (*FinalizeRuntimeSessionPublicationResponse, error) {
	const operation = "finalize_runtime_session_publication"
	if err := request.ValidateAt(time.Now().UTC()); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := RuntimeSessionPublicationFinalizationPath(sessionID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response FinalizeRuntimeSessionPublicationResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) DeleteRuntimeSession(ctx context.Context, sessionID RuntimeSessionID, request DeleteRuntimeSessionRequest) (*DeleteRuntimeSessionResponse, error) {
	const operation = "delete_runtime_session"
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := RuntimeSessionPath(sessionID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response DeleteRuntimeSessionResponse
	if err := c.mutateJSON(ctx, operation, http.MethodDelete, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) RenewPromptLease(ctx context.Context, sessionID RuntimeSessionID, request RenewPromptLeaseRequest) (*PromptLeaseResponse, error) {
	const operation = "renew_prompt_lease"
	limits := c.protocolLimits()
	now := time.Now().UTC()
	maxLease := time.Duration(limits.MaxPromptLeaseMillis) * time.Millisecond
	if err := request.ValidateAt(now, maxLease); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := PromptLeasePath(sessionID, request.Metadata.PromptID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response PromptLeaseResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.ValidateAt(time.Now().UTC(), maxLease); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if response.Lease.Generation != request.Lease.Generation ||
		!response.Lease.IssuedAt.Equal(request.Lease.IssuedAt) ||
		!response.Lease.ExpiresAt.Equal(request.Lease.ExpiresAt) {
		return nil, c.protocolError(operation, 0, fmt.Errorf("lease response does not match proposed lease"))
	}
	return &response, nil
}

func (c *Client) ResolvePermission(ctx context.Context, sessionID RuntimeSessionID, request ResolvePermissionRequest) (*PermissionResolutionResponse, error) {
	const operation = "resolve_permission"
	if err := request.ValidateAt(time.Now().UTC()); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := PromptPermissionPath(sessionID, request.Metadata.PromptID, request.RequestID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response PermissionResolutionResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if response.State != PermissionResolutionCancelledByPrompt && response.Decision != request.Decision {
		return nil, c.protocolError(operation, 0, fmt.Errorf("permission response decision does not match request"))
	}
	return &response, nil
}

func (c *Client) CancelPrompt(ctx context.Context, sessionID RuntimeSessionID, request CancelPromptRequest) (*CancelPromptResponse, error) {
	const operation = "cancel_prompt"
	if err := request.ValidateAt(time.Now().UTC()); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := PromptCancelPath(sessionID, request.Metadata.PromptID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response CancelPromptResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate, RequestClassificationSettled); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if response.Classification.Class == RequestClassificationSettled && response.Classification.TerminalEvent != response.Settlement.TerminalEvent {
		return nil, c.protocolError(operation, 0, fmt.Errorf("settled cancellation classification does not match settlement"))
	}
	return &response, nil
}

func (c *Client) CreateWorkspaceDelta(ctx context.Context, sessionID RuntimeSessionID, request CreateWorkspaceDeltaRequest) (*CreateWorkspaceDeltaResponse, error) {
	const operation = "create_workspace_delta"
	if err := request.ValidateAt(time.Now().UTC()); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := WorkspaceDeltaPath(sessionID, request.DeltaID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	var response CreateWorkspaceDeltaResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, relative, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) Drain(ctx context.Context, request DrainRequest) (*DrainResponse, error) {
	const operation = "drain"
	if err := request.ValidateAt(time.Now().UTC()); err != nil {
		return nil, c.validationError(operation, err)
	}
	var response DrainResponse
	if err := c.mutateJSON(ctx, operation, http.MethodPut, DrainPath, request.Metadata, request, &response); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	if err := validateSuccessClassification(operation, response.Classification, RequestClassificationFresh, RequestClassificationDuplicate); err != nil {
		return nil, c.protocolError(operation, 0, err)
	}
	return &response, nil
}

func (c *Client) getJSON(ctx context.Context, operation, relative string, authenticated bool, out any) error {
	return c.getJSONWithCapability(ctx, operation, relative, authenticated, "", out)
}

func (c *Client) getJSONWithCapability(ctx context.Context, operation, relative string, authenticated bool, capability string, out any) error {
	if c == nil {
		return clientError(operation, ClientErrorConfiguration, "client is nil", ErrClientConfiguration)
	}
	endpoint, err := c.endpoint(relative)
	if err != nil {
		return c.validationError(operation, err)
	}
	requestCtx, cancel := c.controlContext(ctx, time.Time{})
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return c.validationError(operation, err)
	}
	setCommonHeaders(req, "application/json")
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+c.controllerBearer)
	}
	if capability != "" {
		req.Header.Set(OperationCapabilityHeader, capability)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.transportError(operation, requestCtx, err, nil, "")
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return c.decodeHTTPError(operation, resp, nil, "")
	}
	if err := requireMediaType(resp.Header.Get("Content-Type"), "application/json"); err != nil {
		return c.protocolError(operation, resp.StatusCode, err)
	}
	body, err := readBoundedResponseBody(resp, c.maxJSONResponseBytes)
	if err != nil {
		return c.protocolError(operation, resp.StatusCode, err)
	}
	if protocolErr := decodeSuccessJSON(body, out); protocolErr != nil {
		return c.protocolError(operation, resp.StatusCode, protocolErr)
	}
	return nil
}

func (c *Client) mutateJSON(
	ctx context.Context,
	operation, method, relative string,
	metadata MutationMetadata,
	input, output any,
) error {
	if err := c.requireMutationAuth(operation); err != nil {
		return err
	}
	if err := c.validateBeforeMutation(ctx, operation, metadata.ExpiresAt); err != nil {
		if clientErr, ok := errors.AsType[*ClientError](err); ok {
			copy := *clientErr
			copy.WriteEvidence = RequestWriteEvidence{State: RequestWriteZeroBytes}
			err = &copy
		}
		return err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return c.validationError(operation, fmt.Errorf("encode request: %w", err))
	}
	limits := c.protocolLimits()
	if len(payload) > limits.MaxRequestBytes {
		return c.validationError(operation, fmt.Errorf("request body is %d bytes, limit %d", len(payload), limits.MaxRequestBytes))
	}
	capability, err := SignOperationCapability(c.capabilitySecret, ClaimsForMutation(metadata))
	if err != nil {
		return c.validationError(operation, fmt.Errorf("sign operation capability: %w", err))
	}
	endpoint, err := c.endpoint(relative)
	if err != nil {
		return c.validationError(operation, err)
	}
	requestCtx, cancel := c.controlContext(ctx, metadata.ExpiresAt)
	defer cancel()
	tracker := newRequestTrace(c.traceReliable)
	req, err := newTrackedRequest(requestCtx, method, endpoint.String(), payload, tracker)
	if err != nil {
		return c.validationError(operation, err)
	}
	setCommonHeaders(req, "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.controllerBearer)
	req.Header.Set(OperationCapabilityHeader, capability)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.transportError(operation, requestCtx, err, tracker, capability)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return c.decodeHTTPError(operation, resp, tracker, capability)
	}
	if err := requireMediaType(resp.Header.Get("Content-Type"), "application/json"); err != nil {
		return c.protocolErrorWithEvidence(operation, resp.StatusCode, err, tracker.evidence(), capability)
	}
	body, err := readBoundedResponseBody(resp, c.maxJSONResponseBytes)
	if err != nil {
		return c.protocolErrorWithEvidence(operation, resp.StatusCode, err, tracker.evidence(), capability)
	}
	if envelope, ok, envelopeErr := decodeErrorEnvelope(body); envelopeErr != nil {
		return c.protocolErrorWithEvidence(operation, resp.StatusCode, envelopeErr, tracker.evidence(), capability)
	} else if ok {
		return c.httpError(operation, resp.StatusCode, envelope, tracker.evidence(), capability)
	}
	if err := decodeSuccessJSON(body, output); err != nil {
		return c.protocolErrorWithEvidence(operation, resp.StatusCode, err, tracker.evidence(), capability)
	}
	return nil
}

func (c *Client) validateBeforeMutation(ctx context.Context, operation string, expiresAt time.Time) error {
	if c == nil || c.beforeMutation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	effectiveDeadline := expiresAt
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(effectiveDeadline) {
		effectiveDeadline = callerDeadline
	}
	now := time.Now()
	remaining := effectiveDeadline.Sub(now)
	if remaining <= 0 {
		return c.preMutationValidationError(operation, fmt.Errorf("pre-mutation authority check: mutation deadline elapsed before revalidation"), false)
	}
	validationCtx, cancel := context.WithDeadline(ctx, now.Add(remaining/2))
	defer cancel()
	if err := c.beforeMutation(validationCtx, operation); err != nil {
		retryable := preMutationRetryable(err) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		return c.preMutationValidationError(operation, fmt.Errorf("pre-mutation authority check: %w", err), retryable)
	}
	if err := validationCtx.Err(); err != nil {
		retryable := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		return c.preMutationValidationError(operation, fmt.Errorf("pre-mutation authority check exceeded its reserved deadline: %w", err), retryable)
	}
	return nil
}

func (c *Client) preMutationValidationError(operation string, err error, retryable bool) error {
	message := c.redact(err.Error(), "")
	return &ClientError{
		Operation: operation, Kind: ClientErrorValidation, Retryable: retryable,
		Message: message, cause: ErrClientValidation,
	}
}

func (c *Client) decodeHTTPError(operation string, response *http.Response, tracker *requestTrace, capability string) error {
	evidence := RequestWriteEvidence{State: RequestWriteUnknown}
	if tracker != nil {
		evidence = tracker.evidence()
	}
	if err := requireMediaType(response.Header.Get("Content-Type"), "application/json"); err != nil {
		return c.protocolErrorWithEvidence(operation, response.StatusCode, err, evidence, capability)
	}
	body, err := readBoundedResponseBody(response, c.maxErrorBodyBytes)
	if err != nil {
		return c.protocolErrorWithEvidence(operation, response.StatusCode, err, evidence, capability)
	}
	errorResponse, ok, err := decodeErrorEnvelope(body)
	if err != nil {
		return c.protocolErrorWithEvidence(operation, response.StatusCode, err, evidence, capability)
	}
	if !ok {
		return c.protocolErrorWithEvidence(operation, response.StatusCode, fmt.Errorf("HTTP error response is not a v2 ErrorResponse"), evidence, capability)
	}
	return c.httpError(operation, response.StatusCode, errorResponse, evidence, capability)
}

func (c *Client) httpError(operation string, status int, response ErrorResponse, evidence RequestWriteEvidence, capability string) error {
	if err := validateHTTPErrorMapping(status, response); err != nil {
		return c.protocolErrorWithEvidence(operation, status, err, evidence, capability)
	}
	message := c.redact(response.Message, capability)
	clientErr := &ClientError{
		Operation:     operation,
		Kind:          ClientErrorHTTP,
		StatusCode:    status,
		Code:          response.Code,
		Retryable:     response.Retryable,
		Message:       message,
		WriteEvidence: evidence,
	}
	if response.Classification != nil {
		classification := *response.Classification
		clientErr.Classification = &classification
	}
	return clientErr
}

func (c *Client) requireControllerAuth(operation string) error {
	if c == nil {
		return clientError(operation, ClientErrorConfiguration, "client is nil", ErrClientConfiguration)
	}
	if c.controllerBearer == "" {
		return clientError(operation, ClientErrorConfiguration, "controller bearer token is required", ErrClientConfiguration)
	}
	return nil
}

func (c *Client) requireMutationAuth(operation string) error {
	if err := c.requireControllerAuth(operation); err != nil {
		return err
	}
	if len(c.capabilitySecret) < MinCapabilitySecretBytes {
		return clientError(operation, ClientErrorConfiguration, "operation capability secret is required", ErrClientConfiguration)
	}
	return nil
}

func (c *Client) protocolLimits() ProtocolLimits {
	c.limitsMu.RLock()
	defer c.limitsMu.RUnlock()
	return c.limits
}

func (c *Client) controlContext(ctx context.Context, expiry time.Time) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(c.controlTimeout)
	if existing, ok := ctx.Deadline(); ok && existing.Before(deadline) {
		deadline = existing
	}
	if !expiry.IsZero() && expiry.Before(deadline) {
		deadline = expiry
	}
	return context.WithDeadline(ctx, deadline)
}

func (c *Client) endpoint(relative string) (*url.URL, error) {
	if c == nil || c.baseURL == nil {
		return nil, fmt.Errorf("client base URL is not configured")
	}
	if err := validateRelativeEndpoint(relative); err != nil {
		return nil, err
	}
	resolved := *c.baseURL
	resolved.Path = c.basePrefix + relative
	resolved.RawPath = ""
	resolved.RawQuery = ""
	resolved.Fragment = ""
	return &resolved, nil
}

func (c *Client) validationError(operation string, err error) error {
	message := c.redact(err.Error(), "")
	return clientError(operation, ClientErrorValidation, message, ErrClientValidation)
}

func (c *Client) protocolError(operation string, status int, err error) error {
	return c.protocolErrorWithEvidence(operation, status, err, RequestWriteEvidence{State: RequestWriteUnknown}, "")
}

func (c *Client) protocolErrorWithEvidence(operation string, status int, err error, evidence RequestWriteEvidence, capability string) error {
	message := c.redact(err.Error(), capability)
	cause := err
	if !errors.Is(err, ErrResponseBodyTooLarge) {
		cause = ErrClientProtocol
	}
	return &ClientError{
		Operation: operation, Kind: ClientErrorProtocol, StatusCode: status,
		Message: message, WriteEvidence: evidence, cause: cause,
	}
}

func (c *Client) transportError(operation string, requestCtx context.Context, transportErr error, tracker *requestTrace, capability string) error {
	evidence := RequestWriteEvidence{State: RequestWriteUnknown}
	if tracker != nil {
		evidence = tracker.evidence()
	}
	cause := transportErr
	if requestCtx != nil && requestCtx.Err() != nil {
		cause = requestCtx.Err()
	}
	message := c.redact(cause.Error(), capability)
	// Do not retain an arbitrary RoundTripper error that may include request
	// headers. Preserve only context cancellation sentinels; otherwise unwrap to
	// the stable transport sentinel.
	if !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		cause = ErrClientTransport
	}
	return &ClientError{
		Operation: operation, Kind: ClientErrorTransport, Message: message,
		WriteEvidence: evidence, cause: cause,
	}
}

func (c *Client) redact(message, capability string) string {
	for _, secret := range []string{c.controllerBearer, capability} {
		if secret == "" {
			continue
		}
		message = strings.ReplaceAll(message, secret, "[REDACTED]")
	}
	return message
}

func parseClientBaseURL(raw string) (*url.URL, string, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("parse base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, "", fmt.Errorf("base URL scheme must be http or https")
	}
	if parsed.Host == "" {
		return nil, "", fmt.Errorf("base URL must include a host")
	}
	if parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, "", fmt.Errorf("base URL must not contain userinfo, opaque data, query, or fragment")
	}
	if parsed.RawPath != "" || strings.Contains(parsed.Path, "\\") {
		return nil, "", fmt.Errorf("base URL path must not contain escaped or backslash path bytes")
	}
	prefix := strings.TrimSuffix(parsed.Path, "/")
	if prefix == "/" {
		prefix = ""
	}
	if prefix != "" {
		if !strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix {
			return nil, "", fmt.Errorf("base URL path must be canonical and traversal-free")
		}
		for segment := range strings.SplitSeq(strings.TrimPrefix(prefix, "/"), "/") {
			if segment == "" || segment == "." || segment == ".." {
				return nil, "", fmt.Errorf("base URL path contains an unsafe segment")
			}
		}
	}
	parsed.Path = prefix
	return parsed, prefix, nil
}

func validateRelativeEndpoint(relative string) error {
	if relative == "" || !strings.HasPrefix(relative, "/v2/") {
		return fmt.Errorf("endpoint path must be an absolute /v2 path")
	}
	if strings.ContainsAny(relative, "?#\\\r\n") || strings.Contains(relative, "//") || path.Clean(relative) != relative {
		return fmt.Errorf("endpoint path is not canonical")
	}
	for segment := range strings.SplitSeq(strings.TrimPrefix(relative, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("endpoint path contains an unsafe segment")
		}
	}
	return nil
}

func validHeaderCredential(value string) bool {
	if strings.TrimSpace(value) != value || value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] <= 0x20 || value[i] >= 0x7f {
			return false
		}
	}
	return true
}

func setCommonHeaders(request *http.Request, accept string) {
	request.Header.Set("Accept", accept)
	request.Header.Set("Accept-Encoding", "identity")
}

func requireMediaType(header, expected string) error {
	if strings.TrimSpace(header) == "" {
		return fmt.Errorf("response Content-Type is required; want %s", expected)
	}
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return fmt.Errorf("parse response Content-Type: %w", err)
	}
	if !strings.EqualFold(mediaType, expected) {
		return fmt.Errorf("response Content-Type %q is unsupported; want %q", mediaType, expected)
	}
	return nil
}

func readBoundedResponseBody(response *http.Response, limit int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("response body is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("response body limit must be positive")
	}
	if response.ContentLength > limit {
		return nil, fmt.Errorf("%w: content length %d exceeds limit %d", ErrResponseBodyTooLarge, response.ContentLength, limit)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w: body exceeds limit %d", ErrResponseBodyTooLarge, limit)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("response body is empty")
	}
	return body, nil
}

func decodeSuccessJSON(body []byte, output any) error {
	if output == nil {
		return fmt.Errorf("response target is required")
	}
	if _, err := parseCanonicalJSON(body); err != nil {
		return fmt.Errorf("invalid JSON response: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode JSON response: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode JSON response: %w", err)
	}
	return nil
}

func decodeErrorEnvelope(body []byte) (ErrorResponse, bool, error) {
	if _, err := parseCanonicalJSON(body); err != nil {
		return ErrorResponse{}, false, fmt.Errorf("invalid JSON error response: %w", err)
	}
	var discriminator struct {
		Code ErrorCode `json:"code"`
	}
	if err := json.Unmarshal(body, &discriminator); err != nil {
		return ErrorResponse{}, false, fmt.Errorf("decode error discriminator: %w", err)
	}
	if discriminator.Code == "" {
		return ErrorResponse{}, false, nil
	}
	var response ErrorResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return ErrorResponse{}, false, fmt.Errorf("decode v2 ErrorResponse: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ErrorResponse{}, false, fmt.Errorf("decode v2 ErrorResponse: %w", err)
	}
	if err := response.Validate(); err != nil {
		return ErrorResponse{}, false, fmt.Errorf("invalid v2 ErrorResponse: %w", err)
	}
	return response, true, nil
}

func validateHTTPErrorMapping(status int, response ErrorResponse) error {
	if err := response.Validate(); err != nil {
		return err
	}
	if response.Classification != nil {
		classification := *response.Classification
		if classification.Class == RequestClassificationFresh {
			return fmt.Errorf("error response must not carry fresh classification")
		}
		if response.Code != classificationCode(classification.Class) {
			return fmt.Errorf("error code %q does not match classification %q", response.Code, classification.Class)
		}
		expectedStatus := classification.HTTPStatus()
		if classification.Class == RequestClassificationDuplicate {
			expectedStatus = http.StatusConflict
		}
		if expectedStatus != status {
			return fmt.Errorf("HTTP status %d does not match classification %q status %d", status, classification.Class, expectedStatus)
		}
		return nil
	}
	allowed := map[ErrorCode]map[int]struct{}{
		ErrorCodeInvalidRequest:      {http.StatusBadRequest: {}, http.StatusNotFound: {}, http.StatusConflict: {}, http.StatusNotImplemented: {}},
		ErrorCodeUnauthenticated:     {http.StatusUnauthorized: {}},
		ErrorCodeForbidden:           {http.StatusForbidden: {}},
		ErrorCodeExpired:             {http.StatusGone: {}},
		ErrorCodeStaleFence:          {http.StatusGone: {}},
		ErrorCodeDigestConflict:      {http.StatusConflict: {}},
		ErrorCodeAlreadyAccepted:     {http.StatusConflict: {}},
		ErrorCodeSettled:             {http.StatusGone: {}},
		ErrorCodeRateLimited:         {http.StatusTooManyRequests: {}},
		ErrorCodeSessionPoisoned:     {http.StatusConflict: {}, http.StatusBadGateway: {}, http.StatusInternalServerError: {}},
		ErrorCodeWorkspaceResumeLost: {http.StatusConflict: {}},
		ErrorCodeOutcomeUnknown:      {http.StatusInternalServerError: {}},
	}
	statuses, ok := allowed[response.Code]
	if !ok {
		return fmt.Errorf("unsupported error code %q", response.Code)
	}
	if _, ok := statuses[status]; !ok {
		return fmt.Errorf("HTTP status %d is invalid for error code %q", status, response.Code)
	}
	return nil
}

func validateSuccessClassification(operation string, classification Classification, allowed ...RequestClassification) error {
	if err := classification.Validate(); err != nil {
		return fmt.Errorf("%s classification: %w", operation, err)
	}
	if slices.Contains(allowed, classification.Class) {
		return nil
	}
	return fmt.Errorf("%s returned unsupported success classification %q", operation, classification.Class)
}

type requestTrace struct {
	traceReliable bool
	bodyBytesRead atomic.Int64
	wroteHeaders  atomic.Bool
	wroteRequest  atomic.Bool
	wroteReqError atomic.Bool
	gotFirstByte  atomic.Bool
}

func newRequestTrace(reliable bool) *requestTrace {
	return &requestTrace{traceReliable: reliable}
}

func (t *requestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		WroteHeaders: func() {
			t.wroteHeaders.Store(true)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.wroteRequest.Store(info.Err == nil)
			t.wroteReqError.Store(info.Err != nil)
		},
		GotFirstResponseByte: func() {
			t.gotFirstByte.Store(true)
		},
	}
}

func (t *requestTrace) evidence() RequestWriteEvidence {
	if t == nil {
		return RequestWriteEvidence{State: RequestWriteUnknown}
	}
	evidence := RequestWriteEvidence{
		State:                RequestWriteAmbiguous,
		RequestBodyBytesRead: t.bodyBytesRead.Load(),
		WroteHeaders:         t.wroteHeaders.Load(),
		WroteRequest:         t.wroteRequest.Load(),
		WroteRequestError:    t.wroteReqError.Load(),
		GotFirstResponseByte: t.gotFirstByte.Load(),
	}
	switch {
	case evidence.WroteRequest:
		evidence.State = RequestWriteComplete
	case t.traceReliable && !evidence.WroteHeaders && evidence.RequestBodyBytesRead == 0 && !evidence.GotFirstResponseByte:
		evidence.State = RequestWriteZeroBytes
	default:
		evidence.State = RequestWriteAmbiguous
	}
	return evidence
}

type trackedBody struct {
	reader  *bytes.Reader
	tracker *requestTrace
}

func (b *trackedBody) Read(buffer []byte) (int, error) {
	n, err := b.reader.Read(buffer)
	if n > 0 {
		b.tracker.bodyBytesRead.Add(int64(n))
	}
	return n, err
}

func (b *trackedBody) Close() error { return nil }

func newTrackedRequest(ctx context.Context, method, endpoint string, payload []byte, tracker *requestTrace) (*http.Request, error) {
	body := &trackedBody{reader: bytes.NewReader(payload), tracker: tracker}
	tracedContext := httptrace.WithClientTrace(ctx, tracker.clientTrace())
	request, err := http.NewRequestWithContext(tracedContext, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("build HTTP request: %w", err)
	}
	request.ContentLength = int64(len(payload))
	// A nil GetBody prevents net/http from automatically replaying an idempotent
	// PUT/DELETE after a connection failure. Replay classification belongs to the
	// durable controller state machine, never the transport.
	request.GetBody = nil
	return request, nil
}

func transportSupportsHTTPTrace(roundTripper http.RoundTripper) bool {
	if roundTripper == nil {
		return true
	}
	_, ok := roundTripper.(*http.Transport)
	return ok
}
