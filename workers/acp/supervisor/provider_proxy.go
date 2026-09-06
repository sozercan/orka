package supervisor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/providerproxy"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/security"
)

const (
	providerProxyPathPrefix               = "/_orka/provider/"
	providerProxyScheme                   = "http"
	providerProxyTLSScheme                = "https"
	providerAuthorizationHeader           = "Authorization"
	providerAPIKeyHeader                  = "X-Api-Key"
	providerLegacyAPIKeyHeader            = "Api-Key"
	providerProxyAuthorizationHeader      = "Proxy-Authorization"
	providerCookieHeader                  = "Cookie"
	providerForwardedForHeader            = "X-Forwarded-For"
	providerContentEncodingHeader         = "Content-Encoding"
	providerOpenAIResponsesV1Path         = "/v1/responses"
	providerOpenAIChatCompletionsPath     = "/chat/completions"
	providerOpenAIChatCompletionsV1Path   = "/v1/chat/completions"
	providerModelsV1Path                  = "/v1/models"
	providerMaxTokensField                = "max_tokens"
	providerMaxCompletionTokensField      = "max_completion_tokens"
	providerMaxOutputTokensField          = "max_output_tokens"
	providerReasoningEffortField          = "reasoning_effort"
	providerToolsField                    = "tools"
	providerVerbosityField                = "verbosity"
	defaultProviderProxyMaxRequestBytes   = 32 << 20
	defaultProviderProxyMaxResponseBytes  = 64 << 20
	defaultProviderProxyHeaderTimeout     = 2 * time.Minute
	defaultProviderProxyReadHeaderTimeout = 5 * time.Second
	defaultProviderProxyReadTimeout       = 30 * time.Second
	defaultProviderProxySessionRequests   = 2
	defaultProviderProxyGlobalRequests    = 8
	providerUpstreamDetailProbeBytes      = 4 << 10
	providerUpstreamDetailMaxBytes        = 256
	providerUpstreamTransportFailure      = "provider upstream request failed"
)

type ProviderProxyConfig struct {
	UpstreamBaseURL       string
	UpstreamBearerToken   string
	ProviderKind          string
	Model                 string
	MaxRequestBytes       int64
	MaxResponseBytes      int64
	ResponseHeaderTimeout time.Duration
	ModelOutputLimit      int64
}

type ProviderProxyBinding struct {
	BaseURL    string
	Credential string
}

func (b ProviderProxyBinding) String() string {
	return fmt.Sprintf("{BaseURL:%q Credential:[redacted]}", b.BaseURL)
}

func (b ProviderProxyBinding) GoString() string { return b.String() }

func (c ProviderProxyConfig) String() string {
	return fmt.Sprintf("{UpstreamBaseURL:%q UpstreamBearerToken:[redacted] MaxRequestBytes:%d MaxResponseBytes:%d ResponseHeaderTimeout:%s}", c.UpstreamBaseURL, c.MaxRequestBytes, c.MaxResponseBytes, c.ResponseHeaderTimeout)
}

func (c ProviderProxyConfig) GoString() string { return c.String() }

type providerProxy struct {
	upstreamBase     *url.URL
	upstreamToken    []byte
	providerKind     string
	model            string
	maxRequestBytes  int64
	maxResponseBytes int64
	modelOutputLimit int64
	client           *http.Client
	listener         net.Listener
	server           *http.Server
	requestSlots     chan struct{}

	mu        sync.RWMutex
	sessions  map[string]*providerProxySession
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

type providerProxySession struct {
	proxy      *providerProxy
	route      string
	credential []byte
	baseURL    string
	basePath   string

	mu                sync.Mutex
	activePromptID    string
	leaseExpiresAt    time.Time
	leaseVersion      uint64
	leaseTimer        *time.Timer
	gateContext       context.Context
	gateCancel        context.CancelFunc
	turnPromptID      string
	maxTurns          int32
	inferenceRequests int32
	// inflightInference counts inference requests between admission and
	// handler completion, separately from the session-wide inflight counter:
	// prompt settlement must only fail closed on an unresolved *inference*
	// outcome, not on a stalled metadata read such as GET /models.
	inflightInference int
	drainedInference  chan struct{}
	turnLimitExceeded bool
	// Per-prompt upstream inference response accounting. ACP agents such as
	// Codex and Copilot report provider errors as ordinary assistant text and
	// end their turn, so the supervisor needs its own evidence that the
	// prompt's final inference call succeeded before it trusts an end_turn
	// settlement. Outcomes are ordered by issuance, not completion: with two
	// concurrent slots a later-issued request can fail before an earlier one
	// succeeds, and the prompt's final outcome is the highest-sequence one.
	inferenceSuccesses int32
	inferenceFailures  int32
	issuedInference    uint64
	// firstInferenceResponseStartedAt is when the first non-error inference
	// response of the active prompt began relaying to the child. Model output
	// can only be derived from those bytes, so assistant text the child
	// emitted before that instant cannot be model output.
	firstInferenceResponseStartedAt time.Time
	lastSuccessSeq                  uint64
	lastFailureSeq                  uint64
	lastUpstreamStatus              int
	lastUpstreamDetail              string
	// admissionClosed rejects new requests for the active prompt once the
	// ACP child has settled its turn, while in-flight relays keep their
	// gate context and drain normally.
	admissionClosed bool
	inflight        int
	drained         chan struct{}
	closed          bool
	requestSlots    chan struct{}
}

type providerProxyAuthorization struct {
	upstreamBase *url.URL
	gateContext  context.Context
	promptID     string
	// inferenceSeq is the issuance order assigned atomically at admission
	// for inference-route requests (0 for metadata). Allocating it here —
	// not after body validation — keeps "final request" meaning final by
	// admission order even when concurrent bodies validate out of order.
	inferenceSeq uint64
	release      func()
}

type providerRequestClass uint8

const (
	providerRequestMetadata providerRequestClass = iota
	providerRequestInference
)

var errProviderTurnLimitExceeded = errors.New("provider inference request limit exceeded")

func (c ProviderProxyConfig) normalized() (ProviderProxyConfig, *url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(c.UpstreamBaseURL))
	if err != nil ||
		(parsed.Scheme != providerProxyScheme && parsed.Scheme != providerProxyTLSScheme) ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy upstream URL is invalid")
	}
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	if strings.TrimSpace(c.UpstreamBearerToken) == "" {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy upstream bearer token is required")
	}
	c.ProviderKind = strings.TrimSpace(c.ProviderKind)
	c.Model = strings.TrimSpace(c.Model)
	if c.ProviderKind == "" || c.Model == "" {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy kind and model are required")
	}
	c.UpstreamBaseURL = parsed.String()
	c.UpstreamBearerToken = normalizeBearerToken(c.UpstreamBearerToken)
	if c.UpstreamBearerToken == "" || strings.IndexFunc(c.UpstreamBearerToken, func(value rune) bool { return value <= ' ' || value == 0x7f }) >= 0 {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy upstream bearer token is required")
	}
	if providerproxy.HasUnsafePathSegment(parsed.Path) {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy upstream URL is invalid")
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = defaultProviderProxyMaxRequestBytes
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = defaultProviderProxyMaxResponseBytes
	}
	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = defaultProviderProxyHeaderTimeout
	}
	if c.ModelOutputLimit < 0 {
		return ProviderProxyConfig{}, nil, fmt.Errorf("provider proxy model output limit must be positive")
	}
	if c.ProviderKind == providerKindOpencode && c.ModelOutputLimit == 0 {
		return ProviderProxyConfig{}, nil, fmt.Errorf("OpenCode provider proxy model output limit is required")
	}
	return c, parsed, nil
}

func normalizeBearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= len("Bearer ") && strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		return strings.TrimSpace(value[len("Bearer "):])
	}
	return value
}

func newProviderProxy(cfg ProviderProxyConfig) (*providerProxy, error) {
	normalized, upstream, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on provider proxy loopback: %w", err)
	}
	transport := &http.Transport{
		Proxy:       nil,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		// A provider request owns its upstream connection so revoking one
		// session lease cannot interfere with another session sharing an HTTP/2
		// connection, and cancellation can close the exact request transport.
		ForceAttemptHTTP2:      false,
		DisableKeepAlives:      true,
		MaxIdleConns:           16,
		MaxIdleConnsPerHost:    8,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  normalized.ResponseHeaderTimeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		DisableCompression:     true,
	}
	proxy := &providerProxy{
		upstreamBase:     upstream,
		upstreamToken:    []byte(normalized.UpstreamBearerToken),
		providerKind:     normalized.ProviderKind,
		model:            normalized.Model,
		maxRequestBytes:  normalized.MaxRequestBytes,
		maxResponseBytes: normalized.MaxResponseBytes,
		modelOutputLimit: normalized.ModelOutputLimit,
		client: &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		listener:     listener,
		sessions:     make(map[string]*providerProxySession),
		requestSlots: make(chan struct{}, defaultProviderProxyGlobalRequests),
	}
	proxy.server = &http.Server{
		Handler:           http.HandlerFunc(proxy.serveHTTP),
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: defaultProviderProxyReadHeaderTimeout,
		ReadTimeout:       defaultProviderProxyReadTimeout,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	go func() {
		_ = proxy.server.Serve(listener)
	}()
	return proxy, nil
}

func (p *providerProxy) newSession() (*providerProxySession, ProviderProxyBinding, error) {
	if p == nil {
		return nil, ProviderProxyBinding{}, fmt.Errorf("provider proxy is required")
	}
	credential, err := randomProxySecret(32)
	if err != nil {
		return nil, ProviderProxyBinding{}, err
	}
	for range 8 {
		route, routeErr := randomProxySecret(24)
		if routeErr != nil {
			return nil, ProviderProxyBinding{}, routeErr
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ProviderProxyBinding{}, fmt.Errorf("provider proxy is closed")
		}
		if _, exists := p.sessions[route]; exists {
			p.mu.Unlock()
			continue
		}
		basePath := providerProxyPathPrefix + route
		if upstreamPath := strings.TrimSuffix(p.upstreamBase.Path, "/"); upstreamPath != "" {
			basePath += upstreamPath
		}
		baseURL := providerProxyScheme + "://" + p.listener.Addr().String() + basePath
		drained := make(chan struct{})
		close(drained)
		session := &providerProxySession{
			proxy:        p,
			route:        route,
			credential:   []byte(credential),
			baseURL:      baseURL,
			basePath:     basePath,
			drained:      drained,
			requestSlots: make(chan struct{}, defaultProviderProxySessionRequests),
		}
		p.sessions[route] = session
		p.mu.Unlock()
		return session, ProviderProxyBinding{BaseURL: baseURL, Credential: credential}, nil
	}
	return nil, ProviderProxyBinding{}, fmt.Errorf("allocate unique provider proxy route")
}

func randomProxySecret(size int) (string, error) {
	if size < 16 {
		return "", fmt.Errorf("provider proxy secret size is too small")
	}
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate provider proxy secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (s *providerProxySession) activateWithMaxTurns(promptID string, maxTurns int32, expiresAt, now time.Time) error {
	promptID = strings.TrimSpace(promptID)
	if promptID == "" || maxTurns <= 0 || !expiresAt.After(now) {
		return fmt.Errorf("active prompt identity, positive max turns, and future lease expiry are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("provider proxy session is closed")
	}
	if s.activePromptID != "" {
		return fmt.Errorf("provider proxy session already has an active prompt")
	}
	s.activePromptID = promptID
	s.leaseExpiresAt = expiresAt
	s.turnPromptID = promptID
	s.maxTurns = maxTurns
	s.inferenceRequests = 0
	s.turnLimitExceeded = false
	s.admissionClosed = false
	s.inferenceSuccesses = 0
	s.inferenceFailures = 0
	s.issuedInference = 0
	s.firstInferenceResponseStartedAt = time.Time{}
	s.lastSuccessSeq = 0
	s.lastFailureSeq = 0
	s.lastUpstreamStatus = 0
	s.lastUpstreamDetail = ""
	s.leaseVersion++
	version := s.leaseVersion
	s.gateContext, s.gateCancel = context.WithCancel(context.Background())
	s.leaseTimer = time.AfterFunc(time.Until(expiresAt), func() {
		s.expire(promptID, version)
	})
	return nil
}

func (s *providerProxySession) renew(promptID string, expiresAt, now time.Time) error {
	if !expiresAt.After(now) {
		return fmt.Errorf("provider proxy lease expiry must be in the future")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.activePromptID != promptID || s.gateCancel == nil || !now.Before(s.leaseExpiresAt) {
		return fmt.Errorf("provider proxy prompt lease is no longer active")
	}
	s.leaseExpiresAt = expiresAt
	s.leaseVersion++
	version := s.leaseVersion
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	s.leaseTimer = time.AfterFunc(time.Until(expiresAt), func() {
		s.expire(promptID, version)
	})
	return nil
}

// closeAdmission stops admitting new provider requests for promptID once the
// ACP child has settled its turn. Requests already authorized keep relaying
// under the prompt's gate context so the bounded drain before deactivate can
// finish accounting them; a child cannot launch further inference calls
// against the settled prompt's quota during that window.
func (s *providerProxySession) closeAdmission(promptID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activePromptID != strings.TrimSpace(promptID) {
		return
	}
	s.admissionClosed = true
}

func (s *providerProxySession) deactivate(promptID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activePromptID != promptID {
		return
	}
	s.revokeLocked()
}

func (s *providerProxySession) revoke() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revokeLocked()
}

func (s *providerProxySession) revokeLocked() {
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
		s.leaseTimer = nil
	}
	if s.gateCancel != nil {
		s.gateCancel()
		s.gateCancel = nil
	}
	s.gateContext = nil
	s.activePromptID = ""
	s.leaseExpiresAt = time.Time{}
	s.leaseVersion++
}

func (s *providerProxySession) expire(promptID string, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.activePromptID != promptID || s.leaseVersion != version {
		return
	}
	if remaining := time.Until(s.leaseExpiresAt); remaining > 0 {
		s.leaseTimer = time.AfterFunc(remaining, func() { s.expire(promptID, version) })
		return
	}
	slog.Warn("ACP provider proxy prompt lease expired without renewal; revoking provider access",
		"promptID", promptID, "leaseVersion", version, "expiredAt", s.leaseExpiresAt.UTC().Format(time.RFC3339))
	s.revokeLocked()
}

func (s *providerProxySession) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.revokeLocked()
	for i := range s.credential {
		s.credential[i] = 0
	}
	s.mu.Unlock()
	if s.proxy != nil {
		s.proxy.mu.Lock()
		if s.proxy.sessions[s.route] == s {
			delete(s.proxy.sessions, s.route)
		}
		s.proxy.mu.Unlock()
	}
}

func (s *providerProxySession) wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	drained := s.drained
	s.mu.Unlock()
	if drained == nil {
		// No request was ever authorized on this session: nothing to drain.
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// authorize admits one request and, atomically under the same lock, starts
// the in-flight accounting the settlement drain depends on. The route class
// is registered here — not after body validation — so there is no window in
// which an authorized inference request exists that the drain cannot see.
func (s *providerProxySession) authorize(r *http.Request, class providerRequestClass, now time.Time) (providerProxyAuthorization, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.activePromptID == "" || s.gateContext == nil || !now.Before(s.leaseExpiresAt) {
		if s.activePromptID != "" && !now.Before(s.leaseExpiresAt) {
			s.revokeLocked()
		}
		return providerProxyAuthorization{}, false
	}
	if s.admissionClosed {
		return providerProxyAuthorization{}, false
	}
	if !requestHasCredential(r, s.credential) {
		return providerProxyAuthorization{}, false
	}
	if s.inflight == 0 {
		s.drained = make(chan struct{})
	}
	s.inflight++
	var inferenceSeq uint64
	if class == providerRequestInference {
		if s.inflightInference == 0 {
			s.drainedInference = make(chan struct{})
		}
		s.inflightInference++
		s.issuedInference++
		inferenceSeq = s.issuedInference
	}
	var releaseOnce sync.Once
	target := *s.proxy.upstreamBase
	return providerProxyAuthorization{
		upstreamBase: &target,
		gateContext:  s.gateContext,
		promptID:     s.activePromptID,
		inferenceSeq: inferenceSeq,
		release: func() {
			releaseOnce.Do(func() {
				s.releaseRequest()
				s.releaseInferenceRequest(class)
			})
		},
	}, true
}

func (s *providerProxySession) releaseRequest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight <= 0 {
		return
	}
	s.inflight--
	if s.inflight == 0 {
		close(s.drained)
	}
}

// consumeInferenceRequest admits one inference request against the prompt's
// turn budget. The issuance sequence is assigned earlier, atomically at
// route admission in authorize; metadata requests consume nothing.
func (s *providerProxySession) consumeInferenceRequest(promptID string, class providerRequestClass, now time.Time) error {
	if class != providerRequestInference {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.activePromptID != promptID || s.gateContext == nil || !now.Before(s.leaseExpiresAt) {
		return context.Canceled
	}
	if s.inferenceRequests >= s.maxTurns {
		s.turnLimitExceeded = true
		return errProviderTurnLimitExceeded
	}
	s.inferenceRequests++
	return nil
}

// releaseInferenceRequest ends the in-flight accounting for one authorized
// inference-route request; metadata classes are a no-op.
func (s *providerProxySession) releaseInferenceRequest(class providerRequestClass) {
	if s == nil || class != providerRequestInference {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflightInference <= 0 {
		return
	}
	s.inflightInference--
	if s.inflightInference == 0 {
		close(s.drainedInference)
	}
}

// waitInference waits for the in-flight inference requests to finish. Unlike
// wait, it ignores metadata requests: their outcomes never feed prompt
// classification, so they must not convert a completed prompt into a
// provider failure.
func (s *providerProxySession) waitInference(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	drained := s.drainedInference
	s.mu.Unlock()
	if drained == nil {
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *providerProxySession) maxTurnsExceeded(promptID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnPromptID == strings.TrimSpace(promptID) && s.turnLimitExceeded
}

// recordInferenceOutcome accounts one upstream inference response, issued
// with sequence seq, for the prompt that owns the current turn. Status codes
// below 400 count as successes; everything else is a failure whose bounded,
// sanitized detail is kept for the terminal failure message. The outcome
// that decides the prompt is the one with the highest issuance sequence,
// regardless of the order in which responses complete. Metadata requests and
// responses for other prompts are ignored.
func (s *providerProxySession) recordInferenceOutcome(promptID string, class providerRequestClass, seq uint64, statusCode int, detail string) {
	if s == nil || class != providerRequestInference || seq == 0 {
		return
	}
	promptID = strings.TrimSpace(promptID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if promptID == "" || s.turnPromptID != promptID {
		return
	}
	s.recordInferenceOutcomeLocked(seq, statusCode, detail)
}

// recordRejectedInferenceRequest accounts an inference request the proxy
// itself refused (capacity, profile, or lifecycle rejection) as a failed
// inference in issuance order, without consuming turn budget. ACP agents can
// render such a rejection as ordinary assistant text and end their turn, so
// without this evidence the prompt would settle Completed even though its
// final inference attempt never reached the provider.
func (s *providerProxySession) recordRejectedInferenceRequest(promptID string, class providerRequestClass, seq uint64, statusCode int, detail string) {
	if s == nil || class != providerRequestInference {
		return
	}
	promptID = strings.TrimSpace(promptID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if promptID == "" || s.turnPromptID != promptID {
		return
	}
	if seq == 0 {
		// A reject with no admission-allocated sequence (rejected before
		// authorization completed) still gets ordered at issuance.
		s.issuedInference++
		seq = s.issuedInference
	}
	s.recordInferenceOutcomeLocked(seq, statusCode, detail)
}

func (s *providerProxySession) recordInferenceOutcomeLocked(seq uint64, statusCode int, detail string) {
	if statusCode < http.StatusBadRequest {
		s.inferenceSuccesses++
		s.lastSuccessSeq = max(s.lastSuccessSeq, seq)
		return
	}
	s.inferenceFailures++
	if seq < s.lastFailureSeq {
		return
	}
	s.lastFailureSeq = seq
	s.lastUpstreamStatus = statusCode
	s.lastUpstreamDetail = sanitizeProviderUpstreamDetail(detail)
}

// attachInferenceFailureDetail fills in the sanitized detail for the failure
// recorded by recordInferenceResponse once the relayed error body prefix has
// been observed. It is a no-op when a later inference already succeeded or a
// different failure has been recorded since.
func (s *providerProxySession) attachInferenceFailureDetail(promptID string, class providerRequestClass, seq uint64, detail string) {
	if s == nil || class != providerRequestInference || seq == 0 || detail == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnPromptID != strings.TrimSpace(promptID) || s.lastFailureSeq != seq || s.lastUpstreamDetail != "" {
		return
	}
	s.lastUpstreamDetail = sanitizeProviderUpstreamDetail(detail)
}

// upstreamFailureUnrecovered reports whether the latest-issued accounted
// inference response for promptID failed and no later-issued inference
// succeeded. It is false when the prompt made no inference requests or when
// the latest-issued response succeeded. An earlier success (for example the
// tool-call round before a 429 on the follow-up request) does not mask a
// final failure: the agent may render that error as assistant text and settle
// end_turn, which must not become a successful Task. Completion order is
// irrelevant: a later-issued failure stays unrecovered even if an
// earlier-issued request succeeds afterwards.
// markInferenceResponseStarted records that a non-error inference response
// for the active prompt is about to be relayed to the child; only the first
// one per prompt is timestamped.
func (s *providerProxySession) markInferenceResponseStarted(promptID string, class providerRequestClass, now time.Time) {
	if s == nil || class != providerRequestInference {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnPromptID != strings.TrimSpace(promptID) || !s.firstInferenceResponseStartedAt.IsZero() {
		return
	}
	s.firstInferenceResponseStartedAt = now
}

// modelOutputPossibleAt reports whether a non-error inference response for
// the active prompt had begun relaying to the child by at: whether assistant
// text the child emitted at that instant could be model output. Comparing
// against the instant an ACP event was received, rather than the current
// state, keeps the answer correct when the prompt stream consumer drains a
// queued event only after the proxy has moved on.
func (s *providerProxySession) modelOutputPossibleAt(promptID string, at time.Time) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnPromptID == strings.TrimSpace(promptID) &&
		!s.firstInferenceResponseStartedAt.IsZero() && !at.Before(s.firstInferenceResponseStartedAt)
}

func (s *providerProxySession) upstreamFailureUnrecovered(promptID string) (bool, int, string) {
	if s == nil {
		return false, 0, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnPromptID != strings.TrimSpace(promptID) || s.lastFailureSeq == 0 || s.lastFailureSeq < s.lastSuccessSeq {
		return false, 0, ""
	}
	return true, s.lastUpstreamStatus, s.lastUpstreamDetail
}

// sanitizeProviderUpstreamDetail bounds an upstream error detail to printable
// text of at most providerUpstreamDetailMaxBytes, strips any private
// provider-proxy route path that an upstream might echo back, and redacts
// credential-shaped values (API keys, bearer/JWT tokens, signed or
// credentialed URLs). The result is persisted in the terminal Failed event,
// the PromptAttempt, and Task status, so it must never carry a secret the
// upstream echoed into its error message. Non-printable runes are dropped
// before redaction so a control character cannot split a token past the
// redactor.
// providerUpstreamDetailWithheld replaces an upstream detail that still
// matches the secret policy after redaction.
const providerUpstreamDetailWithheld = "upstream error detail withheld: credential-shaped content"

func sanitizeProviderUpstreamDetail(detail string) string {
	// Drop non-printable runes first: U+0085 and friends are both controls
	// and Unicode whitespace, so splitting into fields before removing them
	// would fragment a credential (or the private proxy path) past both the
	// path filter and the redactor.
	var printable strings.Builder
	for _, r := range detail {
		switch {
		// Separators are dropped, not spaced: a credential wrapped across a
		// line break or tab must reassemble into one contiguous token for
		// the redactor, matching the other ACP sanitizers.
		case r == utf8.RuneError || !unicode.IsPrint(r):
			continue
		default:
			printable.WriteRune(r)
		}
	}
	fields := strings.Fields(printable.String())
	kept := fields[:0]
	for _, field := range fields {
		if strings.Contains(field, providerProxyPathPrefix) {
			continue
		}
		kept = append(kept, field)
	}
	sanitized := strings.TrimSpace(redact.SensitiveText(strings.Join(kept, " ")))
	limit := providerUpstreamDetailMaxBytes
	if len(sanitized) > limit {
		for limit > 0 && !utf8.RuneStart(sanitized[limit]) {
			limit--
		}
		sanitized = strings.TrimSpace(sanitized[:limit])
	}
	// The redactor knows a fixed set of credential shapes; the broader
	// secret policy recognizes more (a bare AWS access-key ID, for one).
	// A detail that still looks like a credential after redaction is
	// withheld entirely rather than persisted in durable Task state.
	if sanitized != "" && security.LooksLikeSecret(sanitized) {
		return providerUpstreamDetailWithheld
	}
	return sanitized
}

// providerUpstreamErrorDetail extracts a human-readable detail from a
// buffered upstream error body prefix: the JSON error.message when present,
// otherwise the trimmed raw prefix. The result is not display-bounded here:
// sanitizeProviderUpstreamDetail redacts the whole captured prefix first and
// bounds afterwards, so a credential whose recognizer needs trailing syntax
// (such as the "@" of a URL userinfo) is never cut away before redaction.
func providerUpstreamErrorDetail(prefix []byte) string {
	var payload struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(prefix, &payload); err == nil && len(payload.Error) > 0 {
		var nested struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(payload.Error, &nested); err == nil && strings.TrimSpace(nested.Message) != "" {
			return nested.Message
		}
		var message string
		if err := json.Unmarshal(payload.Error, &message); err == nil && strings.TrimSpace(message) != "" {
			return message
		}
	}
	return strings.TrimSpace(string(prefix))
}

// countingWriter counts the bytes that reached the downstream client.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// deliveredSSEWriter scans only the prefix accepted by the downstream
// client. A ResponseWriter may return both n > 0 and an error, so
// io.MultiWriter cannot preserve this delivery boundary.
type deliveredSSEWriter struct {
	w       io.Writer
	scanner *sseTerminalErrorScanner
}

func (w *deliveredSSEWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		_, _ = w.scanner.Write(p[:n])
	}
	return n, err
}

// prefixCapture retains the first limit bytes written through it so an
// upstream error body can be relayed to the ACP child as it arrives while a
// bounded prefix is kept for the failure detail.
type prefixCapture struct {
	limit  int
	buffer []byte
}

func (c *prefixCapture) Write(p []byte) (int, error) {
	if room := c.limit - len(c.buffer); room > 0 {
		c.buffer = append(c.buffer, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func requestHasCredential(r *http.Request, expected []byte) bool {
	for _, value := range r.Header.Values(providerAuthorizationHeader) {
		value = strings.TrimSpace(value)
		if len(value) < len("Bearer ") || !strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
			continue
		}
		if constantTimeStringMatch(strings.TrimSpace(value[len("Bearer "):]), expected) {
			return true
		}
	}
	for _, name := range []string{providerAPIKeyHeader, providerLegacyAPIKeyHeader} {
		for _, value := range r.Header.Values(name) {
			if constantTimeStringMatch(strings.TrimSpace(value), expected) {
				return true
			}
		}
	}
	return false
}

func constantTimeStringMatch(value string, expected []byte) bool {
	if len(value) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(value), expected) == 1
}

func (p *providerProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	route, ok := splitProviderProxyRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	p.mu.RLock()
	session := p.sessions[route]
	p.mu.RUnlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}
	suffix, ok := session.requestSuffix(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	// The route class (path + method) is known before authorization, so the
	// drain accounting can start atomically with admission inside authorize.
	_, _, routeClass := providerRequestRoute(p.providerKind, suffix, r.Method)
	authorization, ok := session.authorize(r, routeClass, time.Now().UTC())
	if !ok {
		providerproxy.WriteError(w, http.StatusForbidden, "provider access is not active")
		return
	}
	defer authorization.release()
	reject := func(statusCode int, message string) {
		session.recordRejectedInferenceRequest(authorization.promptID, routeClass, authorization.inferenceSeq, statusCode, message)
		providerproxy.WriteError(w, statusCode, message)
	}
	if !providerproxy.TryAcquireSlot(session.requestSlots) {
		reject(http.StatusTooManyRequests, "provider session request capacity is exhausted")
		return
	}
	defer providerproxy.ReleaseSlot(session.requestSlots)
	if !providerproxy.TryAcquireSlot(p.requestSlots) {
		reject(http.StatusTooManyRequests, "provider proxy request capacity is exhausted")
		return
	}
	defer providerproxy.ReleaseSlot(p.requestSlots)
	if r.Method == http.MethodConnect || r.Method == http.MethodTrace {
		reject(http.StatusMethodNotAllowed, "provider request method is not allowed")
		return
	}
	if providerproxy.HasUnsafePathSegment(suffix) {
		reject(http.StatusBadRequest, "provider request path is invalid")
		return
	}
	if providerproxy.HasDisallowedContentEncoding(r.Header) {
		reject(http.StatusUnsupportedMediaType, "compressed provider requests are forbidden")
		return
	}

	requestContext, cancel := context.WithCancel(r.Context())
	stopGate := context.AfterFunc(authorization.gateContext, cancel)
	stopBody := context.AfterFunc(authorization.gateContext, func() { _ = r.Body.Close() })
	var connectionMu sync.Mutex
	var upstreamConnection net.Conn
	connectionRevoked := false
	closeUpstreamConnection := func() {
		connectionMu.Lock()
		connectionRevoked = true
		if upstreamConnection != nil {
			_ = upstreamConnection.Close()
		}
		connectionMu.Unlock()
	}
	stopConnection := context.AfterFunc(authorization.gateContext, closeUpstreamConnection)
	defer func() {
		stopGate()
		stopBody()
		stopConnection()
		cancel()
	}()
	body, err := readBoundedProviderBody(requestContext, r.Body, p.maxRequestBytes)
	if err != nil {
		switch {
		case errors.Is(err, errProviderBodyTooLarge):
			reject(http.StatusRequestEntityTooLarge, "provider request body exceeds limit")
		case requestContext.Err() != nil && authorization.gateContext.Err() == nil:
			// The ACP child abandoned its own request mid-body; that is
			// not provider evidence and must not outrank an earlier success.
			providerproxy.WriteError(w, http.StatusForbidden, "provider request is no longer active")
		default:
			reject(http.StatusForbidden, "provider request is no longer active")
		}
		return
	}
	requestClass, err := validateProviderRequest(p.providerKind, p.model, suffix, r.Method, body)
	if err != nil {
		reject(http.StatusForbidden, "provider request is outside the immutable profile")
		return
	}
	body, err = normalizeProviderRequestBody(p.providerKind, p.model, suffix, p.modelOutputLimit, body)
	if err != nil {
		reject(http.StatusForbidden, "provider request is outside the immutable profile")
		return
	}
	select {
	case <-authorization.gateContext.Done():
		reject(http.StatusForbidden, "provider request is no longer active")
		return
	default:
	}
	inferenceSeq := authorization.inferenceSeq
	if err := session.consumeInferenceRequest(authorization.promptID, requestClass, time.Now().UTC()); err != nil {
		if errors.Is(err, errProviderTurnLimitExceeded) {
			session.recordRejectedInferenceRequest(authorization.promptID, requestClass, inferenceSeq, http.StatusTooManyRequests, "maximum provider inference requests reached for active prompt")
			writeProviderTurnLimitError(w, p.providerKind)
		} else {
			reject(http.StatusForbidden, "provider request is no longer active")
		}
		return
	}

	requestContext = httptrace.WithClientTrace(requestContext, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		connectionMu.Lock()
		upstreamConnection = info.Conn
		if connectionRevoked {
			_ = info.Conn.Close()
		}
		connectionMu.Unlock()
	}})
	target := providerproxy.Target(authorization.upstreamBase, suffix, r.URL.RawQuery)
	upstreamRequest, err := http.NewRequestWithContext(requestContext, r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		providerproxy.WriteError(w, http.StatusBadGateway, "provider request could not be prepared")
		return
	}
	providerproxy.CopyRequestHeaders(upstreamRequest.Header, r.Header)
	upstreamRequest.Header.Set(providerAuthorizationHeader, "Bearer "+string(p.upstreamToken))
	upstreamRequest.Header.Set("Accept-Encoding", "identity")

	response, err := p.client.Do(upstreamRequest)
	if err != nil {
		// A transport failure is upstream evidence only while the child's
		// request is still wanted. When the child (or the prompt gate)
		// cancelled before headers arrived, nothing about the upstream was
		// learned, so the sequence is left unaccounted rather than turning a
		// speculative request the child abandoned into a prompt failure.
		if requestContext.Err() == nil {
			session.recordInferenceOutcome(authorization.promptID, requestClass, inferenceSeq, http.StatusBadGateway, providerUpstreamTransportFailure)
		}
		providerproxy.WriteError(w, http.StatusBadGateway, providerUpstreamTransportFailure)
		return
	}
	defer response.Body.Close() //nolint:errcheck
	p.relayUpstreamResponse(requestContext, w, session, authorization.promptID, requestClass, inferenceSeq, response)
}

// relayUpstreamResponse forwards an upstream response to the ACP child and
// accounts the inference outcome for the owning prompt. Successful responses
// stream through untouched; error responses have a bounded prefix probed for
// a detail message before the identical bytes are relayed.
func (p *providerProxy) relayUpstreamResponse(
	ctx context.Context,
	w http.ResponseWriter,
	session *providerProxySession,
	promptID string,
	requestClass providerRequestClass,
	seq uint64,
	response *http.Response,
) {
	rejectUpstream := func(message string) {
		session.recordInferenceOutcome(promptID, requestClass, seq, http.StatusBadGateway, message)
		providerproxy.WriteError(w, http.StatusBadGateway, message)
	}
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
		rejectUpstream("provider upstream redirects are forbidden")
		return
	}
	if providerproxy.HasDisallowedContentEncoding(response.Header) {
		rejectUpstream("compressed provider responses are forbidden")
		return
	}
	if response.ContentLength > p.maxResponseBytes {
		rejectUpstream("provider upstream response exceeds limit")
		return
	}
	var body io.Reader = response.Body
	upstreamFailed := response.StatusCode >= http.StatusBadRequest
	var capture *prefixCapture
	if upstreamFailed {
		// Upstream errors are accounted before the relay so the failure
		// survives even when the child abandons the error body. The detail
		// is captured from a bounded prefix as the body is relayed rather
		// than read ahead: a chunked 4xx that stalls after a short payload
		// must not hold the child's request until the lease expires.
		session.recordInferenceOutcome(promptID, requestClass, seq, response.StatusCode, "")
		capture = &prefixCapture{limit: providerUpstreamDetailProbeBytes}
		body = io.TeeReader(response.Body, capture)
	}
	// A 200 status line does not prove a streamed inference succeeded:
	// providers report terminal errors inside the SSE body. Scan the relayed
	// stream for explicit error events so they are accounted as failures.
	var streamScanner *sseTerminalErrorScanner
	if !upstreamFailed && strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		streamScanner = &sseTerminalErrorScanner{}
		// The scanner attaches on the WRITE side below, so it sees only
		// bytes the child actually received. A marker read upstream but
		// never delivered must not count as delivered.
	}
	if !upstreamFailed {
		session.markInferenceResponseStarted(promptID, requestClass, time.Now().UTC())
	}
	providerproxy.CopyResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	// Flushing after every chunk keeps streamed provider responses (SSE)
	// flowing to the ACP child without buffering delays.
	flusher, _ := w.(http.Flusher)
	relayed := &countingWriter{w: w}
	var destination io.Writer = relayed
	if streamScanner != nil {
		destination = &deliveredSSEWriter{w: relayed, scanner: streamScanner}
	}
	err := providerproxy.StreamBoundedResponse(destination, body, p.maxResponseBytes, flusher)
	streamFailed := func() bool {
		if streamScanner == nil {
			return false
		}
		// A stream can end on an unterminated line (no trailing newline);
		// the residual buffer must be scanned before the verdict.
		streamScanner.flush()
		return streamScanner.failed
	}
	recordStreamFailure := func() {
		session.recordInferenceOutcome(promptID, requestClass, seq, http.StatusBadGateway, "provider stream reported a terminal error: "+streamScanner.detail)
	}
	recordIncompleteStream := func() {
		session.recordInferenceOutcome(promptID, requestClass, seq, http.StatusBadGateway, "provider stream ended before a terminal success event")
	}
	if upstreamFailed {
		session.attachInferenceFailureDetail(promptID, requestClass, seq, providerUpstreamErrorDetail(capture.buffer))
	}
	if err != nil {
		if !upstreamFailed {
			if errors.Is(err, providerproxy.ErrDestinationWrite) || ctx.Err() != nil {
				// A child disconnect is not upstream evidence by itself. A
				// partially delivered SSE response still needs a terminal
				// marker, while a zero-byte relay remains unaccounted and a
				// partially delivered non-SSE response stays unaccounted.
				if streamFailed() {
					recordStreamFailure()
				} else if relayed.n > 0 && streamScanner != nil {
					if !streamScanner.completed {
						recordIncompleteStream()
					} else {
						session.recordInferenceOutcome(promptID, requestClass, seq, response.StatusCode, "")
					}
				}
				// A partially delivered non-SSE body is left unaccounted:
				// it proves nothing about the inference outcome, and an
				// unaccounted request can neither mask an earlier failure
				// nor certify success.
			} else {
				// A 2xx whose body overran the response limit or broke
				// mid-stream on the upstream side never delivered a usable
				// inference result; count it as a failure so
				// upstreamFailureUnrecovered does not mistake it for a
				// success.
				session.recordInferenceOutcome(promptID, requestClass, seq, http.StatusBadGateway, "provider upstream stream failed")
			}
		}
		panic(http.ErrAbortHandler)
	}
	if !upstreamFailed {
		if streamFailed() {
			recordStreamFailure()
			return
		}
		if streamScanner != nil && !streamScanner.completed {
			recordIncompleteStream()
			return
		}
		// A success is only accounted once the whole body reached the
		// child; a streamed response must also carry a provider terminal
		// success marker so a truncated 2xx cannot mask an earlier failure.
		session.recordInferenceOutcome(promptID, requestClass, seq, response.StatusCode, "")
	}
}

// sseTerminalErrorScanner watches a relayed text/event-stream body for an
// explicit terminal result. Providers can fail after a 200 status line, and
// a clean EOF without a success marker is only evidence of a truncated
// stream. Marker matching uses a bounded rolling window of compacted line
// bytes: model-generated content carries embedded markers JSON-escaped and
// cannot spoof them.
type sseTerminalErrorScanner struct {
	linePrefix []byte
	lineWindow []byte
	compactLen int
	failed     bool
	// pendingMarker is the error payload marker matched on the current
	// line. The failure latches once the whole line has been seen so the
	// recorded detail carries the complete error payload rather than the
	// prefix up to the marker.
	pendingMarker []byte
	// awaitingErrorData is set after an error event line (`event: error`):
	// the failure latches on the following data line, whose payload is the
	// detail, or on the blank line/end of stream that closes the event.
	awaitingErrorData bool
	eventMarker       []byte
	completed         bool
	detail            string
}

const (
	sseScannerDetailPrefixBytes = 1024
	sseScannerWindowBytes       = 64
)

// Markers are matched against a whitespace-stripped copy of each line, so
// valid spaced JSON ({"type": "response.failed"}) and unspaced JSON match
// identically. Content deltas still cannot spoof them: quotes inside model
// text arrive JSON-escaped (\"), and stripping whitespace does not unescape.
var sseTerminalErrorEventMarkers = [][]byte{
	[]byte("event:error"),
	[]byte("event:response.failed"),
}

var sseTerminalErrorPayloadMarkers = [][]byte{
	[]byte(`"type":"error"`),
	[]byte(`"type":"response.failed"`),
	[]byte(`data:{"error"`),
}

var sseTerminalSuccessEventMarkers = [][]byte{
	[]byte("event:message_stop"),
	[]byte("event:response.completed"),
	[]byte("event:response.incomplete"),
}

var sseTerminalSuccessPayloadMarkers = [][]byte{
	[]byte(`"type":"message_stop"`),
	[]byte(`"type":"response.completed"`),
	[]byte(`"type":"response.incomplete"`),
}

var sseDoneMarker = []byte("data:[DONE]")

// sseDataFieldPrefix is the whitespace-stripped SSE data field name.
var sseDataFieldPrefix = []byte("data:")

func (c *sseTerminalErrorScanner) Write(p []byte) (int, error) {
	if c.failed {
		return len(p), nil
	}
	for _, b := range p {
		if b == '\n' {
			c.finishLine()
			if c.failed {
				return len(p), nil
			}
			c.resetLine()
			continue
		}
		if len(c.linePrefix) < sseScannerDetailPrefixBytes {
			c.linePrefix = append(c.linePrefix, b)
		}
		if b == ' ' || b == '\t' || b == '\r' {
			continue
		}
		c.compactLen++
		if c.pendingMarker != nil {
			// The verdict is already known; keep buffering the rest of the
			// line so the detail is the full payload.
			continue
		}
		if len(c.lineWindow) < sseScannerWindowBytes {
			c.lineWindow = append(c.lineWindow, b)
		} else {
			copy(c.lineWindow, c.lineWindow[1:])
			c.lineWindow[len(c.lineWindow)-1] = b
		}
		c.scanWindow()
	}
	return len(p), nil
}

// flush scans any residual unterminated line at end of stream and settles a
// failure that was still waiting for the end of its line or data payload.
func (c *sseTerminalErrorScanner) flush() {
	if c.failed {
		return
	}
	if c.compactLen > 0 || c.pendingMarker != nil || c.awaitingErrorData {
		c.finishLine()
		c.resetLine()
	}
}

func (c *sseTerminalErrorScanner) scanWindow() {
	for _, marker := range sseTerminalErrorPayloadMarkers {
		if bytes.HasSuffix(c.lineWindow, marker) {
			c.pendingMarker = marker
			return
		}
	}
	for _, marker := range sseTerminalSuccessPayloadMarkers {
		if bytes.HasSuffix(c.lineWindow, marker) {
			c.completed = true
			return
		}
	}
}

func (c *sseTerminalErrorScanner) finishLine() {
	if c.pendingMarker != nil {
		c.markFailure(c.pendingMarker)
		return
	}
	if c.awaitingErrorData {
		switch {
		case c.compactLen == 0:
			// Blank line: the error event carried no data payload.
			c.markFailure(c.eventMarker)
		case bytes.HasPrefix(c.lineWindow, sseDataFieldPrefix):
			c.markFailure(c.eventMarker)
		default:
			// Another field of the same event (id:, retry:): keep waiting
			// for its data line.
		}
		return
	}
	for _, marker := range sseTerminalErrorEventMarkers {
		if c.compactLen == len(marker) && bytes.Equal(c.lineWindow, marker) {
			c.awaitingErrorData = true
			c.eventMarker = marker
			return
		}
	}
	if c.compactLen == len(sseDoneMarker) && bytes.Equal(c.lineWindow, sseDoneMarker) {
		c.completed = true
		return
	}
	for _, marker := range sseTerminalSuccessEventMarkers {
		if c.compactLen == len(marker) && bytes.Equal(c.lineWindow, marker) {
			c.completed = true
			return
		}
	}
}

func (c *sseTerminalErrorScanner) markFailure(marker []byte) {
	c.failed = true
	c.pendingMarker = nil
	c.awaitingErrorData = false
	if len(c.linePrefix) < sseScannerDetailPrefixBytes {
		payload := bytes.TrimSpace(bytes.TrimSuffix(c.linePrefix, []byte{'\r'}))
		// Strip the SSE field name so a JSON payload parses and yields the
		// provider's message instead of the raw `data: {...}` line.
		if rest, ok := bytes.CutPrefix(payload, sseDataFieldPrefix); ok {
			payload = bytes.TrimSpace(rest)
		}
		c.detail = providerUpstreamErrorDetail(payload)
	}
	if c.detail == "" {
		c.detail = string(marker)
	}
}

func (c *sseTerminalErrorScanner) resetLine() {
	c.linePrefix = c.linePrefix[:0]
	c.lineWindow = c.lineWindow[:0]
	c.compactLen = 0
}

func normalizeProviderRequestBody(providerKind, model, requestPath string, modelOutputLimit int64, body []byte) ([]byte, error) {
	if providerKind != providerKindOpencode ||
		(requestPath != providerOpenAIChatCompletionsPath && requestPath != providerOpenAIChatCompletionsV1Path) {
		return clampProviderOutputLimit(providerKind, requestPath, modelOutputLimit, body)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode OpenCode provider request: %w", err)
	}
	if err := ensureProviderJSONEOF(decoder); err != nil {
		return nil, err
	}
	if modelOutputLimit <= 0 {
		return nil, fmt.Errorf("OpenCode model output limit is required")
	}
	providerID, upstreamModel, ok := strings.Cut(model, "/")
	providerID = strings.TrimSpace(providerID)
	upstreamModel = strings.TrimSpace(upstreamModel)
	if !ok || providerID == "" || upstreamModel == "" {
		return nil, fmt.Errorf("OpenCode model must use provider/model form")
	}
	maxTokens, hasMaxTokens, err := positiveProviderOutputLimit(payload, providerMaxTokensField)
	if err != nil {
		return nil, err
	}
	maxCompletionTokens, hasMaxCompletionTokens, err := positiveProviderOutputLimit(payload, providerMaxCompletionTokensField)
	if err != nil {
		return nil, err
	}
	outputLimit := modelOutputLimit
	outputField := providerMaxTokensField
	if strings.EqualFold(providerID, "openai") {
		outputField = providerMaxCompletionTokensField
		delete(payload, providerVerbosityField)
		if tools, ok := payload[providerToolsField].([]any); ok && len(tools) > 0 {
			delete(payload, providerReasoningEffortField)
		}
	}
	if hasMaxTokens && maxTokens < outputLimit {
		outputLimit = maxTokens
	}
	if hasMaxCompletionTokens {
		outputField = providerMaxCompletionTokensField
		if maxCompletionTokens < outputLimit {
			outputLimit = maxCompletionTokens
		}
	}
	delete(payload, providerMaxTokensField)
	delete(payload, providerMaxCompletionTokensField)
	payload[outputField] = outputLimit
	payload["model"] = upstreamModel
	return json.Marshal(payload)
}

// providerOutputLimitFields maps a provider inference path to the request
// fields that bound generated output, plus the canonical field to force-set
// when the caller omitted one.
func providerOutputLimitFields(providerKind, requestPath string) (fields []string, canonical string) {
	switch providerKind {
	case providerKindCodex, providerKindCopilot:
		switch requestPath {
		case "/responses", providerOpenAIResponsesV1Path, "/responses/compact", "/v1/responses/compact":
			return []string{providerMaxOutputTokensField}, providerMaxOutputTokensField
		case providerOpenAIChatCompletionsPath, providerOpenAIChatCompletionsV1Path:
			return []string{providerMaxTokensField, providerMaxCompletionTokensField}, providerMaxTokensField
		}
	case "claude":
		if requestPath == "/v1/messages" {
			return []string{providerMaxTokensField}, providerMaxTokensField
		}
	}
	return nil, ""
}

// clampProviderOutputLimit enforces the immutable profile's model output
// limit on Codex, Claude, and Copilot inference requests: an untrusted ACP
// child holding a prompt-scoped proxy credential must not exceed the reviewed
// token bound by inflating or omitting the output-limit field. OpenCode
// requests are normalized separately with the same enforcement.
func clampProviderOutputLimit(providerKind, requestPath string, modelOutputLimit int64, body []byte) ([]byte, error) {
	if modelOutputLimit <= 0 {
		return body, nil
	}
	fields, canonical := providerOutputLimitFields(providerKind, requestPath)
	if len(fields) == 0 {
		return body, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode provider request: %w", err)
	}
	if err := ensureProviderJSONEOF(decoder); err != nil {
		return nil, err
	}
	outputLimit := modelOutputLimit
	outputField := canonical
	for _, field := range fields {
		value, present, err := positiveProviderOutputLimit(payload, field)
		if err != nil {
			return nil, err
		}
		if present {
			outputField = field
			if value < outputLimit {
				outputLimit = value
			}
		}
	}
	for _, field := range fields {
		delete(payload, field)
	}
	payload[outputField] = outputLimit
	return json.Marshal(payload)
}

func positiveProviderOutputLimit(payload map[string]any, name string) (int64, bool, error) {
	value, ok := payload[name]
	if !ok {
		return 0, false, nil
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, false, fmt.Errorf("provider %s must be a positive integer", name)
	}
	parsed, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || parsed <= 0 {
		return 0, false, fmt.Errorf("provider %s must be a positive integer", name)
	}
	return parsed, true, nil
}

func ensureProviderJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("provider request contains trailing JSON data")
	}
	return fmt.Errorf("decode provider request trailer: %w", err)
}

// providerRequestRoute classifies a provider request by path and method
// alone, before the body is read, so proxy-side rejections of inference
// requests can be accounted in issuance order.
func providerRequestRoute(providerKind, requestPath, method string) (allowed, requiresModel bool, class providerRequestClass) {
	class = providerRequestMetadata
	switch providerKind {
	case providerKindCodex, providerKindCopilot:
		switch requestPath {
		case "/responses", providerOpenAIResponsesV1Path, "/responses/compact", "/v1/responses/compact", providerOpenAIChatCompletionsPath, providerOpenAIChatCompletionsV1Path:
			allowed, requiresModel, class = method == http.MethodPost, true, providerRequestInference
		case "/models", providerModelsV1Path:
			allowed = method == http.MethodGet
		}
	case providerKindOpencode:
		switch requestPath {
		case providerOpenAIChatCompletionsPath, providerOpenAIChatCompletionsV1Path:
			allowed, requiresModel, class = method == http.MethodPost, true, providerRequestInference
		}
	case "claude":
		switch requestPath {
		case "/v1/messages":
			allowed, requiresModel, class = method == http.MethodPost, true, providerRequestInference
		case "/v1/messages/count_tokens":
			allowed, requiresModel = method == http.MethodPost, true
		case providerModelsV1Path:
			allowed = method == http.MethodGet
		}
	}
	return allowed, requiresModel, class
}

func validateProviderRequest(providerKind, model, requestPath, method string, body []byte) (providerRequestClass, error) {
	allowed, requiresModel, class := providerRequestRoute(providerKind, requestPath, method)
	if !allowed {
		return providerRequestMetadata, fmt.Errorf("provider path or method is not allowed")
	}
	if !requiresModel {
		return class, nil
	}
	var envelope struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || strings.TrimSpace(envelope.Model) != model {
		return providerRequestMetadata, fmt.Errorf("provider model does not match immutable profile")
	}
	return class, nil
}

var errProviderBodyTooLarge = errors.New("provider body too large")

func readBoundedProviderBody(ctx context.Context, body io.ReadCloser, limit int64) ([]byte, error) {
	defer body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if int64(len(data)) > limit {
		return nil, errProviderBodyTooLarge
	}
	return data, nil
}

func splitProviderProxyRoute(path string) (route string, ok bool) {
	if !strings.HasPrefix(path, providerProxyPathPrefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(path, providerProxyPathPrefix)
	route, _, _ = strings.Cut(remainder, "/")
	if route == "" {
		return "", false
	}
	return route, true
}

func (s *providerProxySession) requestSuffix(path string) (string, bool) {
	if path == s.basePath {
		return "/", true
	}
	if !strings.HasPrefix(path, s.basePath+"/") {
		return "", false
	}
	return strings.TrimPrefix(path, s.basePath), true
}

func writeProviderTurnLimitError(w http.ResponseWriter, providerKind string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusBadRequest)
	if providerKind == providerKindClaude {
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"maximum provider inference requests reached for active prompt"}}`+"\n")
		return
	}
	_, _ = io.WriteString(w, `{"error":{"message":"maximum provider inference requests reached for active prompt","type":"invalid_request_error","code":"max_turn_requests"}}`+"\n")
}

func (p *providerProxy) close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closeDone != nil {
		done := p.closeDone
		p.mu.Unlock()
		select {
		case <-done:
			p.mu.RLock()
			err := p.closeErr
			p.mu.RUnlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.closed = true
	p.closeDone = make(chan struct{})
	done := p.closeDone
	sessions := make([]*providerProxySession, 0, len(p.sessions))
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.mu.Unlock()
	for _, session := range sessions {
		session.close()
	}
	var errs []error
	for _, session := range sessions {
		if err := session.wait(ctx); err != nil {
			errs = append(errs, fmt.Errorf("wait for provider proxy session requests: %w", err))
		}
	}
	if err := p.server.Shutdown(ctx); err != nil {
		errs = append(errs, err, p.server.Close())
	}
	if transport, ok := p.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	closeErr := errors.Join(errs...)
	p.mu.Lock()
	p.closeErr = closeErr
	close(done)
	p.mu.Unlock()
	return closeErr
}
