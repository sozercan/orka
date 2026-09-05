package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testUpstreamToken          = "upstream-token"
	testConnectionSecretHeader = "X-Connection-Secret"
)

const testUnreachableUpstreamURL = "http://upstream.invalid"

func TestProviderProxyConfigValidationAndRedaction(t *testing.T) {
	const secret = "do-not-print-this-provider-token"
	cfg := ProviderProxyConfig{UpstreamBaseURL: "http://vekil.example/v1", UpstreamBearerToken: secret}
	if rendered := fmt.Sprintf("%#v", cfg); strings.Contains(rendered, secret) {
		t.Fatalf("provider proxy config formatting leaked its bearer token: %s", rendered)
	}
	binding := ProviderProxyBinding{BaseURL: "http://127.0.0.1/private", Credential: secret}
	if rendered := fmt.Sprintf("%#v", binding); strings.Contains(rendered, secret) {
		t.Fatalf("provider proxy binding formatting leaked its local credential: %s", rendered)
	}
	for _, invalid := range []ProviderProxyConfig{
		{UpstreamBaseURL: "http://vekil.example/v1?token=query", UpstreamBearerToken: secret},
		{UpstreamBaseURL: "http://vekil.example/../admin", UpstreamBearerToken: secret},
		{UpstreamBaseURL: "http://vekil.example/v1", UpstreamBearerToken: "bad\nheader"},
	} {
		if _, _, err := invalid.normalized(); err == nil {
			t.Fatalf("unsafe provider proxy config unexpectedly validated: %#v", invalid)
		}
	}
	normalized, _, err := (ProviderProxyConfig{
		UpstreamBaseURL: "http://vekil.example/v1", UpstreamBearerToken: secret,
		ProviderKind: providerKindCodex, Model: "codex-timeout-model",
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.ResponseHeaderTimeout != 2*time.Minute {
		t.Fatalf("provider proxy response header timeout = %s, want 2m", normalized.ResponseHeaderTimeout)
	}
}

func TestProviderProxyGatesSessionsAndInjectsSupervisorBearer(t *testing.T) {
	const upstreamToken = "supervisor-upstream-token-canary"
	type observedRequest struct {
		method string
		path   string
		query  string
		header http.Header
		body   string
	}
	observed := make(chan observedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- observedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: string(body)}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Safe", "yes")
		w.Header().Set("Set-Cookie", "upstream-secret=1")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	proxy := newTestProviderProxy(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL + "/v1", UpstreamBearerToken: upstreamToken,
		MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20,
	})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	second, secondBinding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	if binding.BaseURL == secondBinding.BaseURL || binding.Credential == secondBinding.Credential {
		t.Fatal("runtime sessions reused a provider proxy route or credential")
	}
	cleanupNow := time.Now().UTC()
	if err := second.activateWithMaxTurns("cleanup-prompt", 50, cleanupNow.Add(time.Minute), cleanupNow); err != nil {
		t.Fatal(err)
	}
	second.close()
	assertProviderProxyStatus(t, secondBinding.BaseURL+"/responses", secondBinding.Credential, http.StatusNotFound)
	parsed, err := url.Parse(binding.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != providerProxyScheme || parsed.Hostname() != "127.0.0.1" || !strings.HasPrefix(parsed.Path, providerProxyPathPrefix) || len(binding.Credential) < 40 {
		t.Fatalf("provider proxy binding is not private and unguessable: %#v", binding)
	}

	response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses?stream=true", binding.Credential, []byte(`{"model":"test-model","prompt":"idle"}`), nil)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("idle request status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
	_ = response.Body.Close()
	select {
	case request := <-observed:
		t.Fatalf("idle request reached upstream: %#v", request)
	default:
	}

	now := time.Now().UTC()
	if err := session.activateWithMaxTurns(testPromptOneID, 50, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	response = doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", "wrong-local-credential", []byte(`{"model":"test-model"}`), nil)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong credential status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
	_ = response.Body.Close()

	headers := http.Header{
		"Content-Type":                   []string{"application/json"},
		providerAPIKeyHeader:             []string{binding.Credential},
		providerCookieHeader:             []string{"local-secret=1"},
		providerProxyAuthorizationHeader: []string{"Bearer proxy-secret"},
		providerForwardedForHeader:       []string{"203.0.113.1"},
		"Connection":                     []string{testConnectionSecretHeader},
		testConnectionSecretHeader:       []string{"remove-me"},
		"X-Safe-Request":                 []string{"preserve-me"},
	}
	response = doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses?stream=true", binding.Credential, []byte(`{"model":"test-model","prompt":"active"}`), headers)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("active request status = %d body=%s", response.StatusCode, data)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"ok":true}` || response.Header.Get("X-Upstream-Safe") != "yes" || len(response.Cookies()) != 0 {
		t.Fatalf("unexpected proxied response: body=%q header=%v", data, response.Header)
	}
	request := <-observed
	if request.method != http.MethodPost || request.path != providerOpenAIResponsesV1Path || request.query != "stream=true" || request.body != `{"model":"test-model","prompt":"active"}` {
		t.Fatalf("unexpected upstream request: %#v", request)
	}
	if got := request.header.Get(providerAuthorizationHeader); got != "Bearer "+upstreamToken {
		t.Fatalf("upstream authorization = %q", got)
	}
	for _, name := range []string{
		providerAPIKeyHeader, providerLegacyAPIKeyHeader, providerCookieHeader,
		providerProxyAuthorizationHeader, providerForwardedForHeader, testConnectionSecretHeader,
	} {
		if value := request.header.Get(name); value != "" {
			t.Fatalf("sensitive inbound header %s reached upstream: %q", name, value)
		}
	}
	if request.header.Get("X-Safe-Request") != "preserve-me" {
		t.Fatalf("safe request header was not preserved: %v", request.header)
	}

	session.deactivate(testPromptOneID)
	assertProviderProxyStatus(t, binding.BaseURL+"/responses", binding.Credential, http.StatusForbidden)

	session.close()
	assertProviderProxyStatus(t, binding.BaseURL+"/responses", binding.Credential, http.StatusNotFound)
}

func TestProviderProxyAcceptsAnthropicAPIKeyCredential(t *testing.T) {
	observed := make(chan *http.Request, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Clone(r.Context())
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	proxy, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL + "/anthropic", UpstreamBearerToken: testUpstreamToken,
		ProviderKind: "claude", Model: "test-model",
	})
	_ = proxy
	defer session.close()
	request, err := http.NewRequest(http.MethodPost, binding.BaseURL+"/v1/messages", strings.NewReader(`{"model":"test-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(providerAPIKeyHeader, binding.Credential)
	request.Header.Set("Anthropic-Version", "2023-06-01")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Anthropic proxy status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	upstreamRequest := <-observed
	if upstreamRequest.URL.Path != "/anthropic/v1/messages" ||
		upstreamRequest.Header.Get(providerAuthorizationHeader) != "Bearer "+testUpstreamToken ||
		upstreamRequest.Header.Get(providerAPIKeyHeader) != "" ||
		upstreamRequest.Header.Get("Anthropic-Version") != "2023-06-01" {
		t.Fatalf("unexpected Anthropic upstream request: path=%s headers=%v", upstreamRequest.URL.Path, upstreamRequest.Header)
	}
}

func TestProviderProxyAcceptsVersionedOpenAIPath(t *testing.T) {
	observed := make(chan *http.Request, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Clone(r.Context())
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
		ProviderKind: providerKindCodex, Model: "test-model",
	})
	defer session.close()
	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
		[]byte(`{"model":"test-model"}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("versioned OpenAI request status = %d body=%s", response.StatusCode, body)
	}
	request := <-observed
	if request.URL.Path != providerOpenAIResponsesV1Path || request.Method != http.MethodPost {
		t.Fatalf("versioned OpenAI upstream request = %s %s", request.Method, request.URL.Path)
	}
}

func TestProviderProxyRejectsVersionedAgentKitChatCompletions(t *testing.T) {
	var reached atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL + "/v1", UpstreamBearerToken: testUpstreamToken,
		ProviderKind: providerKindAgentKit, Model: "gpt-test",
	})
	defer session.close()
	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIChatCompletionsV1Path, binding.Credential,
		[]byte(`{"model":"gpt-test"}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("versioned AgentKit chat request status = %d body=%s", response.StatusCode, body)
	}
	if got := reached.Load(); got != 0 {
		t.Fatalf("versioned AgentKit chat request reached upstream %d times", got)
	}
}

func TestProviderProxyLeaseExpiryCancelsInflightAndDeniesLateRequests(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: started\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(started)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				close(cancelled)
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, "data: waiting\n\n"); err != nil {
					close(cancelled)
					return
				}
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		}
	}))
	defer upstream.Close()
	proxy := newTestProviderProxy(t, ProviderProxyConfig{UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-expiring", 50, now.Add(150*time.Millisecond), now); err != nil {
		t.Fatal(err)
	}

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		request, _ := http.NewRequest(http.MethodPost, binding.BaseURL+"/responses", bytes.NewReader([]byte(`{"model":"test-model"}`)))
		request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr == nil {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("provider request did not reach upstream while lease was active")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("lease expiry did not cancel the in-flight upstream request")
	}
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("local proxy request did not terminate after lease expiry")
	}
	response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte(`{"model":"test-model"}`), nil)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("late request status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
	_ = response.Body.Close()
}

func TestProviderProxyLeaseRenewalExtendsAccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	proxy := newTestProviderProxy(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-renew", 50, now.Add(75*time.Millisecond), now); err != nil {
		t.Fatal(err)
	}
	if err := session.renew("prompt-renew", now.Add(time.Second), now.Add(25*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(125 * time.Millisecond)
	response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte(`{"model":"test-model"}`), nil)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("renewed provider request status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	session.deactivate("prompt-renew")
}

func TestProviderProxyMaxTurnsCountsOnlyInferenceRequests(t *testing.T) {
	t.Run("OpenAI-compatible", func(t *testing.T) {
		var reached atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			reached.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
		defer upstream.Close()

		proxy := newTestProviderProxy(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: "test-auth-token",
		})
		session, binding, err := proxy.newSession()
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		if err := session.activateWithMaxTurns(testPromptOneID, 3, now.Add(time.Minute), now); err != nil {
			t.Fatal(err)
		}
		defer session.close()

		for _, request := range []struct {
			method string
			path   string
			body   []byte
		}{
			{method: http.MethodGet, path: "/models"},
			{method: http.MethodPost, path: "/responses", body: []byte(`{"model":"test-model"}`)},
			{method: http.MethodPost, path: "/v1/chat/completions", body: []byte(`{"model":"test-model"}`)},
			{method: http.MethodPost, path: "/responses/compact", body: []byte(`{"model":"test-model"}`)},
			{method: http.MethodGet, path: providerModelsV1Path},
		} {
			response := doProviderProxyRequest(t, request.method, binding.BaseURL+request.path, binding.Credential, request.body, nil)
			if response.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				t.Fatalf("%s %s status = %d body=%s", request.method, request.path, response.StatusCode, body)
			}
			_ = response.Body.Close()
		}

		blocked := doProviderProxyRequest(
			t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
			[]byte(`{"model":"test-model"}`), nil,
		)
		defer func() { _ = blocked.Body.Close() }()
		if blocked.StatusCode != http.StatusBadRequest {
			body, _ := io.ReadAll(blocked.Body)
			t.Fatalf("N+1 OpenAI request status = %d body=%s", blocked.StatusCode, body)
		}
		body, _ := io.ReadAll(blocked.Body)
		if !strings.Contains(string(body), `"code":"max_turn_requests"`) {
			t.Fatalf("OpenAI turn-limit body = %s", body)
		}
		if got := reached.Load(); got != 5 {
			t.Fatalf("OpenAI upstream requests = %d, want 5", got)
		}
		if !session.maxTurnsExceeded(testPromptOneID) {
			t.Fatal("OpenAI prompt was not marked turn-limit exhausted")
		}
	})

	t.Run("Anthropic", func(t *testing.T) {
		var reached atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			reached.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
		defer upstream.Close()

		proxy := newTestProviderProxy(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: "test-auth-token", ProviderKind: providerKindClaude,
		})
		session, binding, err := proxy.newSession()
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		if err := session.activateWithMaxTurns(testPromptOneID, 1, now.Add(time.Minute), now); err != nil {
			t.Fatal(err)
		}
		defer session.close()

		for _, request := range []struct {
			method string
			path   string
			body   []byte
		}{
			{method: http.MethodPost, path: "/v1/messages/count_tokens", body: []byte(`{"model":"test-model"}`)},
			{method: http.MethodGet, path: providerModelsV1Path},
			{method: http.MethodPost, path: "/v1/messages", body: []byte(`{"model":"test-model"}`)},
		} {
			response := doProviderProxyRequest(t, request.method, binding.BaseURL+request.path, binding.Credential, request.body, nil)
			if response.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				t.Fatalf("%s %s status = %d body=%s", request.method, request.path, response.StatusCode, body)
			}
			_ = response.Body.Close()
		}

		blocked := doProviderProxyRequest(
			t, http.MethodPost, binding.BaseURL+"/v1/messages", binding.Credential,
			[]byte(`{"model":"test-model"}`), nil,
		)
		defer func() { _ = blocked.Body.Close() }()
		if blocked.StatusCode != http.StatusBadRequest {
			body, _ := io.ReadAll(blocked.Body)
			t.Fatalf("N+1 Anthropic request status = %d body=%s", blocked.StatusCode, body)
		}
		body, _ := io.ReadAll(blocked.Body)
		if !strings.Contains(string(body), `"type":"error"`) {
			t.Fatalf("Anthropic turn-limit body = %s", body)
		}
		if got := reached.Load(); got != 3 {
			t.Fatalf("Anthropic upstream requests = %d, want 3", got)
		}
		if !session.maxTurnsExceeded(testPromptOneID) {
			t.Fatal("Anthropic prompt was not marked turn-limit exhausted")
		}
	})
}

func TestProviderProxyMaxTurnsIsAtomicAcrossConcurrentRequests(t *testing.T) {
	var reached atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseUpstream()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	proxy := newTestProviderProxy(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: "test-auth-token",
	})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns(testPromptOneID, 1, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	defer session.close()

	type result struct {
		status int
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	request := func() {
		<-start
		httpRequest, requestErr := http.NewRequest(
			http.MethodPost, binding.BaseURL+"/responses",
			bytes.NewReader([]byte(`{"model":"test-model"}`)),
		)
		if requestErr != nil {
			results <- result{err: requestErr}
			return
		}
		httpRequest.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
		response, requestErr := http.DefaultClient.Do(httpRequest)
		if requestErr != nil {
			results <- result{err: requestErr}
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		results <- result{status: response.StatusCode}
	}
	go request()
	go request()
	close(start)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("allowed inference request did not reach upstream")
	}
	select {
	case got := <-results:
		if got.err != nil || got.status != http.StatusBadRequest {
			t.Fatalf("concurrent N+1 result = %#v, want status %d", got, http.StatusBadRequest)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent N+1 request was not blocked")
	}
	if got := reached.Load(); got != 1 {
		t.Fatalf("concurrent upstream requests = %d, want 1", got)
	}
	releaseUpstream()
	select {
	case got := <-results:
		if got.err != nil || got.status != http.StatusOK {
			t.Fatalf("allowed concurrent request result = %#v, want status %d", got, http.StatusOK)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("allowed concurrent request did not complete")
	}
	if !session.maxTurnsExceeded(testPromptOneID) {
		t.Fatal("concurrent prompt was not marked turn-limit exhausted")
	}
}

func TestProviderProxyMaxTurnsResetsOnActivationButNotRenewal(t *testing.T) {
	var reached atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	proxy := newTestProviderProxy(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: "test-auth-token",
	})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-one", 1, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}

	requestInference := func(want int) {
		t.Helper()
		response := doProviderProxyRequest(
			t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential,
			[]byte(`{"model":"test-model"}`), nil,
		)
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != want {
			body, _ := io.ReadAll(response.Body)
			t.Fatalf("provider response status = %d, want %d body=%s", response.StatusCode, want, body)
		}
	}

	requestInference(http.StatusOK)
	if err := session.renew("prompt-one", now.Add(2*time.Minute), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	requestInference(http.StatusBadRequest)
	if !session.maxTurnsExceeded("prompt-one") {
		t.Fatal("lease renewal reset the prompt turn-limit state")
	}

	session.deactivate("prompt-one")
	secondNow := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-two", 1, secondNow.Add(time.Minute), secondNow); err != nil {
		t.Fatal(err)
	}
	if session.maxTurnsExceeded("prompt-one") || session.maxTurnsExceeded("prompt-two") {
		t.Fatal("new prompt activation did not reset turn-limit state")
	}
	requestInference(http.StatusOK)
	requestInference(http.StatusBadRequest)
	if !session.maxTurnsExceeded("prompt-two") {
		t.Fatal("second prompt was not marked turn-limit exhausted")
	}
	if got := reached.Load(); got != 2 {
		t.Fatalf("upstream requests across prompt resets = %d, want 2", got)
	}
}

func TestProviderProxySessionCleanupWaitsForInflightRequest(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(started)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				close(cancelled)
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, "data: waiting\n\n"); err != nil {
					close(cancelled)
					return
				}
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		}
	}))
	defer upstream.Close()
	proxy, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	_ = proxy
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		request, _ := http.NewRequest(http.MethodPost, binding.BaseURL+"/responses", bytes.NewReader([]byte(`{"model":"test-model"}`)))
		request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr == nil {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("provider request did not become in-flight")
	}
	session.close()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.wait(waitCtx); err != nil {
		t.Fatalf("wait for provider request cleanup: %v", err)
	}
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("local provider request remained active after session cleanup")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream provider request remained active after session cleanup")
	}
}

func TestProviderProxyForbidsRedirectsAndBoundsBodies(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		var redirected atomic.Bool
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
		defer target.Close()
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Redirect(w, &http.Request{}, target.URL, http.StatusTemporaryRedirect)
		}))
		defer upstream.Close()
		proxy, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken})
		_ = proxy
		defer session.close()
		response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte(`{"model":"test-model"}`), nil)
		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("redirect status = %d, want %d", response.StatusCode, http.StatusBadGateway)
		}
		_ = response.Body.Close()
		if redirected.Load() {
			t.Fatal("provider proxy followed an upstream redirect")
		}
	})

	t.Run("request body", func(t *testing.T) {
		var reached atomic.Bool
		upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Store(true) }))
		defer upstream.Close()
		_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken, MaxRequestBytes: 4,
		})
		defer session.close()
		response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte("12345"), nil)
		if response.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized request status = %d, want %d", response.StatusCode, http.StatusRequestEntityTooLarge)
		}
		_ = response.Body.Close()
		if reached.Load() {
			t.Fatal("oversized request reached upstream")
		}
	})

	t.Run("response body", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "5")
			_, _ = io.WriteString(w, "12345")
		}))
		defer upstream.Close()
		_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken, MaxResponseBytes: 4,
		})
		defer session.close()
		response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte(`{"model":"test-model"}`), nil)
		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("oversized response status = %d, want %d", response.StatusCode, http.StatusBadGateway)
		}
		_ = response.Body.Close()
	})

	t.Run("compressed request", func(t *testing.T) {
		var reached atomic.Bool
		upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Store(true) }))
		defer upstream.Close()
		_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
		})
		defer session.close()
		response := doProviderProxyRequest(
			t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte("compressed"),
			http.Header{providerContentEncodingHeader: []string{"gzip"}},
		)
		if response.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("compressed request status = %d, want %d", response.StatusCode, http.StatusUnsupportedMediaType)
		}
		_ = response.Body.Close()
		if reached.Load() {
			t.Fatal("compressed request reached upstream")
		}
	})

	t.Run("compressed response", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(providerContentEncodingHeader, "gzip")
			_, _ = io.WriteString(w, "compressed")
		}))
		defer upstream.Close()
		_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
			UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
		})
		defer session.close()
		response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+"/responses", binding.Credential, []byte(`{"model":"test-model"}`), nil)
		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("compressed response status = %d, want %d", response.StatusCode, http.StatusBadGateway)
		}
		_ = response.Body.Close()
	})
}

func newTestProviderProxy(t *testing.T, cfg ProviderProxyConfig) *providerProxy {
	t.Helper()
	if cfg.ProviderKind == "" {
		cfg.ProviderKind = providerKindCodex
	}
	if cfg.Model == "" {
		cfg.Model = "test-model"
	}
	proxy, err := newProviderProxy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := proxy.close(ctx); err != nil {
			t.Errorf("close provider proxy: %v", err)
		}
	})
	return proxy
}

//nolint:unparam // Stable test helper signatures keep related cases uniform.
func activeTestProviderProxySession(t *testing.T, cfg ProviderProxyConfig) (*providerProxy, *providerProxySession, ProviderProxyBinding) {
	t.Helper()
	proxy := newTestProviderProxy(t, cfg)
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns(testPromptOneID, 50, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	return proxy, session, binding
}

//nolint:unparam // Stable test helper signatures keep related cases uniform.
func doProviderProxyRequest(t *testing.T, method, endpoint, credential string, body []byte, headers http.Header) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	request.Header.Set(providerAuthorizationHeader, "Bearer "+credential)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertProviderProxyStatus(t *testing.T, endpoint, credential string, want int) {
	t.Helper()
	response := doProviderProxyRequest(t, http.MethodPost, endpoint, credential, []byte(`{"model":"test-model"}`), nil)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != want {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		t.Fatalf("provider proxy status = %d, want %d body=%s", response.StatusCode, want, body)
	}
}

func TestProviderProxyUpstreamFailureAccountingPassesErrorThrough(t *testing.T) {
	const quotaBody = `{"error":{"message":"You have exceeded your monthly quota","type":"quota_exceeded"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Marker", "quota")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = io.WriteString(w, quotaBody)
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
		[]byte(`{"model":"test-model"}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusPaymentRequired || string(body) != quotaBody {
		t.Fatalf("passed-through upstream error = %d %s", response.StatusCode, body)
	}
	if got := response.Header.Get("X-Upstream-Marker"); got != "quota" {
		t.Fatalf("passed-through upstream header = %q", got)
	}
	waitProviderProxyIdle(t, session)
	failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
	if !failed || status != http.StatusPaymentRequired || detail != "You have exceeded your monthly quota" {
		t.Fatalf("upstreamFailureUnrecovered = %v/%d/%q", failed, status, detail)
	}
	if failed, _, _ := session.upstreamFailureUnrecovered("other-prompt"); failed {
		t.Fatal("upstream failure accounting leaked to another prompt")
	}
}

func TestProviderProxyUpstreamFailureAccountingClearsAfterTerminalStream(t *testing.T) {
	for _, terminalEvent := range []string{"response.completed", "response.incomplete"} {
		t.Run(terminalEvent, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusPaymentRequired)
					_, _ = io.WriteString(w, `{"error":{"message":"You have exceeded your monthly quota"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				flusher := w.(http.Flusher)
				for _, chunk := range []string{"event: response.created\ndata: {}\n\n", "event: " + terminalEvent + "\ndata: {}\n\n"} {
					_, _ = io.WriteString(w, chunk)
					flusher.Flush()
				}
			}))
			defer upstream.Close()

			_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
				UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
			})
			defer session.close()

			assertProviderProxyStatus(t, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, http.StatusPaymentRequired)
			if failed, _, _ := session.upstreamFailureUnrecovered(testPromptOneID); !failed {
				t.Fatal("first failed inference response was not accounted")
			}

			response := doProviderProxyRequest(
				t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
				[]byte(`{"model":"test-model","stream":true}`), nil,
			)
			defer func() { _ = response.Body.Close() }()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK || !strings.Contains(string(body), terminalEvent) {
				t.Fatalf("terminal stream = %d %s", response.StatusCode, body)
			}
			if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); failed || status != 0 || detail != "" {
				t.Fatalf("upstreamFailureUnrecovered after %s = %v/%d/%q, want false", terminalEvent, failed, status, detail)
			}
		})
	}
}

// Model output can only be derived from a non-error inference response, so
// the proxy records when the prompt's first such response begins relaying.
func TestProviderProxyModelOutputPossibleAfterFirstResponseStarts(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.created\ndata: {}\n\n")
		flusher.Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	if session.modelOutputPossibleAt(testPromptOneID, time.Now().UTC()) {
		t.Fatal("model output reported as possible before any request")
	}
	before := time.Now().UTC()

	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
		[]byte(`{"model":"test-model","stream":true}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d", response.StatusCode)
	}
	if !session.modelOutputPossibleAt(testPromptOneID, time.Now().UTC()) {
		t.Fatal("model output not reported as possible once headers were relayed")
	}
	if session.modelOutputPossibleAt(testPromptOneID, before) {
		t.Fatal("model output reported as possible for an instant before the response started")
	}
	if session.modelOutputPossibleAt("other-prompt", time.Now().UTC()) {
		t.Fatal("another prompt's response start leaked")
	}
	close(release)
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatalf("drain stream: %v", err)
	}
}

func TestProviderProxyIncompleteStreamIsAccountedAsFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: response.created\ndata: {}\n\n")
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
		[]byte(`{"model":"test-model","stream":true}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	waitProviderProxyIdle(t, session)
	failure, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
	if !failure || status != http.StatusBadGateway || detail != "provider stream ended before a terminal success event" {
		t.Fatalf("incomplete stream accounting = %v/%d/%q, want terminal-marker failure", failure, status, detail)
	}
}

func TestProviderProxyUpstreamFailureAccountingFailsWhenFinalInferenceFails(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"id":"resp_1","output":[]}`)
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"rate limit exceeded","type":"rate_limit"}}`)
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	assertProviderProxyStatus(t, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, http.StatusOK)
	if failed, _, _ := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatal("successful inference response was accounted as a failure")
	}
	assertProviderProxyStatus(t, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, http.StatusTooManyRequests)
	waitProviderProxyIdle(t, session)
	failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
	if !failed || status != http.StatusTooManyRequests || detail != "rate limit exceeded" {
		t.Fatalf("upstreamFailureUnrecovered after success then failure = %v/%d/%q, want the final failure", failed, status, detail)
	}
	if successes, failures := session.inferenceSuccesses, session.inferenceFailures; successes != 1 || failures != 1 {
		t.Fatalf("inference accounting = %d successes / %d failures, want 1/1", successes, failures)
	}
}

func TestProviderProxyCloseAdmissionRejectsNewRequestsButDrainsInFlight(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_1","output":[]}`)
	}))
	defer upstream.Close()
	// Runs before upstream.Close so a failed assertion cannot leave the
	// upstream handler blocked.
	defer releaseUpstream()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	inflightDone := make(chan int, 1)
	go func() {
		response := doProviderProxyRequest(t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, []byte(`{"model":"test-model"}`), nil)
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		inflightDone <- response.StatusCode
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		session.mu.Lock()
		inflight := session.inflight
		session.mu.Unlock()
		if inflight == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("in-flight request was never admitted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	session.closeAdmission(testPromptOneID)
	assertProviderProxyStatus(t, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, http.StatusForbidden)

	releaseUpstream()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := session.wait(waitCtx); err != nil {
		t.Fatalf("in-flight request did not drain after admission closed: %v", err)
	}
	if status := <-inflightDone; status != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200 relayed after admission closed", status)
	}
	if failed, _, _ := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatal("drained in-flight success was not accounted")
	}
}

func TestProviderProxyUpstreamFailureAccountingResetsOnActivation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "upstream\x00unavailable\n at /_orka/provider/secret-route/v1/responses")
	}))
	defer upstream.Close()

	proxy := newTestProviderProxy(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	session, binding, err := proxy.newSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	now := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-one", 50, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	assertProviderProxyStatus(t, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential, http.StatusServiceUnavailable)
	// The failure detail is attached once the relayed error body has been
	// observed, which completes after the status line is available.
	waitProviderProxyIdle(t, session)
	failed, status, detail := session.upstreamFailureUnrecovered("prompt-one")
	if !failed || status != http.StatusServiceUnavailable {
		t.Fatalf("upstreamFailureUnrecovered = %v/%d/%q", failed, status, detail)
	}
	if detail != "upstreamunavailable at" || strings.Contains(detail, providerProxyPathPrefix) {
		t.Fatalf("sanitized upstream detail = %q", detail)
	}

	session.deactivate("prompt-one")
	if failed, _, _ := session.upstreamFailureUnrecovered("prompt-one"); !failed {
		t.Fatal("deactivation cleared upstream failure accounting before settlement")
	}
	secondNow := time.Now().UTC()
	if err := session.activateWithMaxTurns("prompt-two", 50, secondNow.Add(time.Minute), secondNow); err != nil {
		t.Fatal(err)
	}
	for _, promptID := range []string{"prompt-one", "prompt-two"} {
		if failed, status, detail := session.upstreamFailureUnrecovered(promptID); failed || status != 0 || detail != "" {
			t.Fatalf("upstreamFailureUnrecovered(%q) after activation = %v/%d/%q, want reset", promptID, failed, status, detail)
		}
	}
	session.mu.Lock()
	successes, failures := session.inferenceSuccesses, session.inferenceFailures
	session.mu.Unlock()
	if successes != 0 || failures != 0 {
		t.Fatalf("inference accounting after activation = %d/%d, want 0/0", successes, failures)
	}
}

func TestRecordRejectedInferenceRequestCountsAsFinalFailure(t *testing.T) {
	const promptID = "prompt-rejected"
	now := time.Now().UTC()
	proxy := &providerProxySession{}
	if err := proxy.activateWithMaxTurns(promptID, 8, now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	seq := testAllocateInferenceSeq(proxy)
	err := proxy.consumeInferenceRequest(promptID, providerRequestInference, now)
	if err != nil {
		t.Fatal(err)
	}
	proxy.recordInferenceOutcome(promptID, providerRequestInference, seq, http.StatusOK, "")
	// Metadata-route rejections carry no inference evidence.
	proxy.recordRejectedInferenceRequest(promptID, providerRequestMetadata, 0, http.StatusTooManyRequests, "capacity")
	if failed, _, _ := proxy.upstreamFailureUnrecovered(promptID); failed {
		t.Fatal("metadata rejection was accounted as an inference failure")
	}
	// A proxy-side capacity rejection of an inference request is the
	// latest-issued outcome, so an end_turn settlement must not be trusted.
	proxy.recordRejectedInferenceRequest(promptID, providerRequestInference, 0, http.StatusTooManyRequests, "provider session request capacity is exhausted")
	failed, status, detail := proxy.upstreamFailureUnrecovered(promptID)
	if !failed || status != http.StatusTooManyRequests || !strings.Contains(detail, "capacity") {
		t.Fatalf("upstreamFailureUnrecovered() = (%v, %d, %q), want a 429 capacity failure", failed, status, detail)
	}
	// The rejection consumed no turn budget, and a later successful inference
	// recovers the prompt.
	seq = testAllocateInferenceSeq(proxy)
	if err := proxy.consumeInferenceRequest(promptID, providerRequestInference, now); err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("issuance sequence after a rejection = %d, want 3", seq)
	}
	proxy.recordInferenceOutcome(promptID, providerRequestInference, seq, http.StatusOK, "")
	if failed, _, _ := proxy.upstreamFailureUnrecovered(promptID); failed {
		t.Fatal("later successful inference did not recover the prompt")
	}
	proxy.mu.Lock()
	requests := proxy.inferenceRequests
	proxy.mu.Unlock()
	if requests != 2 {
		t.Fatalf("inferenceRequests = %d, want 2 (rejections consume no turn budget)", requests)
	}
}

func TestSanitizeProviderUpstreamDetailIsBounded(t *testing.T) {
	long := strings.Repeat("y", providerUpstreamDetailMaxBytes+10) + "界"
	if got := sanitizeProviderUpstreamDetail(long); len(got) > providerUpstreamDetailMaxBytes {
		t.Fatalf("sanitized detail length = %d, want <= %d", len(got), providerUpstreamDetailMaxBytes)
	}
	// Separators are dropped, not spaced (a wrapped credential must
	// reassemble for the redactor), so prose joins across them.
	if got := sanitizeProviderUpstreamDetail("quota\tex\x1bceeded\n"); got != "quotaexceeded" {
		t.Fatalf("sanitized control characters = %q", got)
	}
	if got := providerUpstreamErrorDetail([]byte(`{"error":"plain string error"}`)); got != "plain string error" {
		t.Fatalf("string error detail = %q", got)
	}
	if got := providerUpstreamErrorDetail([]byte("  <html>bad gateway</html>  ")); got != "<html>bad gateway</html>" {
		t.Fatalf("raw error detail = %q", got)
	}
}

func TestSanitizeProviderUpstreamDetailDropsC1ControlsBeforeSplitting(t *testing.T) {
	const key = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	// U+0085 is both a control and Unicode whitespace: splitting into fields
	// first would fragment the token past the redactor and the path filter.
	got := sanitizeProviderUpstreamDetail("rejected api_key=" + key[:14] + "\u0085" + key[14:] + " via " + providerProxyPathPrefix[:len(providerProxyPathPrefix)/2] + "\u0085" + providerProxyPathPrefix[len(providerProxyPathPrefix)/2:] + "abc")
	for _, fragment := range []string{key[:14], key[14:], providerProxyPathPrefix} {
		if strings.Contains(got, fragment) {
			t.Fatalf("sanitized detail leaked %q: %q", fragment, got)
		}
	}
	if !strings.Contains(got, "rejected api_key=[REDACTED]") {
		t.Fatalf("sanitized detail = %q, want the whole token redacted", got)
	}
}

func TestProviderUpstreamErrorDetailRedactsBeforeDisplayBound(t *testing.T) {
	// The URL password is longer than the display bound, so a pre-redaction
	// cut would remove the "@" the URL-credential recognizer relies on.
	password := strings.Repeat("p", providerUpstreamDetailMaxBytes+16)
	raw := "upstream rejected https://svc:" + password + "@provider.example.com/v1/responses"
	got := sanitizeProviderUpstreamDetail(providerUpstreamErrorDetail([]byte(raw)))
	if strings.Contains(got, strings.Repeat("p", 16)) {
		t.Fatalf("sanitized detail leaked the URL credential: %q", got)
	}
	if len(got) > providerUpstreamDetailMaxBytes || !strings.Contains(got, "upstream rejected") {
		t.Fatalf("sanitized detail = %q (%d bytes), want bounded text that keeps the prose", got, len(got))
	}
}

// credentialShapedUpstreamErrorBody is an upstream error payload that echoes
// several credential shapes back into error.message; none of them may reach
// the persisted failure detail.
const (
	testLeakedAPIKey    = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	testLeakedJWT       = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvcmthLXRlc3QifQ.c2lnbmF0dXJlLXRoYXQtbXVzdC1ub3QtbGVhaw"
	testLeakedSignature = "deadbeefcafef00d1234567890abcdef"
	testLeakedSignedURL = "https://bucket.example.com/object?X-Amz-Credential=AKIAEXAMPLEKEYID&X-Amz-Signature=" + testLeakedSignature
)

var credentialShapedUpstreamErrorBody = `{"error":{"message":"request rejected: api_key=` + testLeakedAPIKey +
	` Authorization: Bearer ` + testLeakedJWT + ` presigned ` + testLeakedSignedURL + ` denied","type":"invalid_request_error"}}`

func assertNoLeakedCredential(t *testing.T, label, text string) {
	t.Helper()
	for _, leaked := range []string{testLeakedAPIKey, testLeakedJWT, testLeakedSignature, "AKIAEXAMPLEKEYID"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("%s leaked credential-shaped value %q: %q", label, leaked, text)
		}
	}
	if len(text) > providerUpstreamDetailMaxBytes+64 {
		t.Fatalf("%s is unbounded (%d bytes): %q", label, len(text), text)
	}
}

func TestProviderProxyUpstreamFailureDetailRedactsCredentials(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, credentialShapedUpstreamErrorBody)
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, binding.Credential,
		[]byte(`{"model":"test-model"}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	// The child still receives the identical upstream bytes; only the
	// persisted accounting detail is redacted.
	if response.StatusCode != http.StatusBadRequest || string(body) != credentialShapedUpstreamErrorBody {
		t.Fatalf("passed-through upstream error = %d %s", response.StatusCode, body)
	}
	waitProviderProxyIdle(t, session)
	failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
	if !failed || status != http.StatusBadRequest {
		t.Fatalf("upstreamFailureUnrecovered = %v/%d/%q", failed, status, detail)
	}
	assertNoLeakedCredential(t, "upstreamFailureUnrecovered detail", detail)
	if !strings.HasPrefix(detail, "request rejected:") || !strings.Contains(detail, "[REDACTED]") {
		t.Fatalf("redacted detail lost its surrounding prose: %q", detail)
	}
	assertNoLeakedCredential(t, "providerUpstreamFailureError", (&providerUpstreamFailureError{Status: status, Detail: detail}).Error())
}

func TestSanitizeProviderUpstreamDetailRedactsSplitTokens(t *testing.T) {
	// A control character inside a token must not let the token slip past
	// the redactor once the control character is stripped.
	split := "key sk-proj\x00-abcdefghijklmnopqrstuvwxyz0123456789 rejected"
	if got := sanitizeProviderUpstreamDetail(split); strings.Contains(got, "abcdefghijklmnopqrstuvwxyz0123456789") {
		t.Fatalf("control-split token survived sanitization: %q", got)
	}
	if got := sanitizeProviderUpstreamDetail("quota exceeded for model"); got != "quota exceeded for model" {
		t.Fatalf("benign detail was altered: %q", got)
	}
}

// childDisconnectedWriter emulates the ACP child closing its side of the
// relay after the upstream already answered 2xx.
type childDisconnectedWriter struct {
	header http.Header
}

func (w *childDisconnectedWriter) Header() http.Header { return w.header }
func (w *childDisconnectedWriter) WriteHeader(int)     {}

// The first chunk reaches the child before its side of the relay breaks.
func (w *childDisconnectedWriter) Write(p []byte) (int, error) {
	return len(p), errors.New("write: broken pipe")
}

// zeroByteDisconnectedWriter emulates a child that abandons the relay before
// any response byte was delivered.
type zeroByteDisconnectedWriter struct{ header http.Header }

func (w *zeroByteDisconnectedWriter) Header() http.Header { return w.header }
func (w *zeroByteDisconnectedWriter) WriteHeader(int)     {}
func (w *zeroByteDisconnectedWriter) Write([]byte) (int, error) {
	return 0, errors.New("write: broken pipe")
}

// prefixDisconnectedWriter accepts a fixed prefix from one response write,
// then reports the child disconnect. This exercises Writer's valid n > 0,
// err != nil result without claiming the unwritten suffix was delivered.
type prefixDisconnectedWriter struct {
	header http.Header
	limit  int
}

func (w *prefixDisconnectedWriter) Header() http.Header { return w.header }
func (w *prefixDisconnectedWriter) WriteHeader(int)     {}
func (w *prefixDisconnectedWriter) Write(p []byte) (int, error) {
	n := min(w.limit, len(p))
	w.limit -= n
	return n, errors.New("write: broken pipe")
}

func TestProviderProxyChildAbandonBeforeAnyByteIsUnaccounted(t *testing.T) {
	proxy, session, _ := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: testUnreachableUpstreamURL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	// An earlier request failed; a later-issued request the child abandoned
	// before receiving a single body byte must not mask that failure.
	session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusTooManyRequests, "rate limit exceeded")
	seq := testAllocateInferenceSeq(session)
	err := session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: {}\n\n")),
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != http.ErrAbortHandler {
				t.Fatalf("relay panic = %v, want http.ErrAbortHandler", recovered)
			}
		}()
		proxy.relayUpstreamResponse(context.Background(), &zeroByteDisconnectedWriter{header: http.Header{}}, session, testPromptOneID, providerRequestInference, seq, response)
	}()
	if failed, status, _ := session.upstreamFailureUnrecovered(testPromptOneID); !failed || status != http.StatusTooManyRequests {
		t.Fatalf("upstreamFailureUnrecovered = %v/%d, want the earlier failure still unrecovered", failed, status)
	}
	if successes := session.inferenceSuccesses; successes != 0 {
		t.Fatalf("inferenceSuccesses = %d, want 0 for a zero-byte abandoned relay", successes)
	}
}

func TestProviderProxyChildDisconnectOnIncompleteStreamCountsAsFailure(t *testing.T) {
	proxy, session, _ := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: testUnreachableUpstreamURL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusTooManyRequests, "rate limit exceeded")

	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: {}\n\n")),
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != http.ErrAbortHandler {
				t.Fatalf("relay panic = %v, want http.ErrAbortHandler", recovered)
			}
		}()
		seq := testAllocateInferenceSeq(session)
		err := session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		proxy.relayUpstreamResponse(context.Background(), &childDisconnectedWriter{header: http.Header{}}, session, testPromptOneID, providerRequestInference, seq, response)
	}()
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); !failed || status != http.StatusBadGateway || detail != "provider stream ended before a terminal success event" {
		t.Fatalf("upstreamFailureUnrecovered after child disconnect on incomplete stream = %v/%d/%q, want terminal-marker failure", failed, status, detail)
	}
	if successes, failures := session.inferenceSuccesses, session.inferenceFailures; successes != 0 || failures != 2 {
		t.Fatalf("inference accounting = %d successes / %d failures, want 0/2", successes, failures)
	}
}

func TestProviderProxyChildDisconnectAccountsOnlyDeliveredStreamBytes(t *testing.T) {
	const incompleteStreamDetail = "provider stream ended before a terminal success event"
	tests := []struct {
		name          string
		delivered     string
		undelivered   string
		wantFailure   bool
		wantDetail    string
		wantSuccesses int32
		wantFailures  int32
	}{
		{
			name:          "terminal marker is not delivered",
			delivered:     "data: {\"type\":\"response.created\"}\n\n",
			undelivered:   "event: response.completed\n\n",
			wantFailure:   true,
			wantDetail:    incompleteStreamDetail,
			wantSuccesses: 0,
			wantFailures:  2,
		},
		{
			name:          "terminal marker is delivered before disconnect",
			delivered:     "event: response.completed\n\n",
			undelivered:   "data: trailing\n\n",
			wantFailure:   false,
			wantSuccesses: 1,
			wantFailures:  1,
		},
		{
			// The error marker sits in the unwritten tail of the short write:
			// it is not upstream evidence the child received, so the stream
			// is accounted as incomplete, exactly once.
			name:          "terminal error is not delivered",
			delivered:     "data: {\"type\":\"response.created\"}\n\n",
			undelivered:   "data: {\"type\":\"response.failed\"}\n\n",
			wantFailure:   true,
			wantDetail:    incompleteStreamDetail,
			wantSuccesses: 0,
			wantFailures:  2,
		},
		{
			name:          "terminal error is delivered before disconnect",
			delivered:     "data: {\"type\":\"response.failed\",\"error\":\"quota\"}\n\n",
			undelivered:   "data: trailing\n\n",
			wantFailure:   true,
			wantDetail:    "provider stream reported a terminal error: quota",
			wantSuccesses: 0,
			wantFailures:  2,
		},
		{
			// The child took the whole error line except its completing
			// newline: the delivered bytes still settle as that terminal
			// error at stream end, exactly once.
			name:          "terminal error line is delivered without its newline",
			delivered:     "data: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.failed\",\"error\":\"quota\"}",
			undelivered:   "\n\n",
			wantFailure:   true,
			wantDetail:    "provider stream reported a terminal error: quota",
			wantSuccesses: 0,
			wantFailures:  2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy, session, _ := activeTestProviderProxySession(t, ProviderProxyConfig{
				UpstreamBaseURL: testUnreachableUpstreamURL, UpstreamBearerToken: testUpstreamToken,
			})
			defer session.close()
			session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusTooManyRequests, "rate limit exceeded")
			seq := testAllocateInferenceSeq(session)
			if err := session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now()); err != nil {
				t.Fatal(err)
			}
			response := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(tt.delivered + tt.undelivered)),
			}
			func() {
				defer func() {
					if recovered := recover(); recovered != http.ErrAbortHandler {
						t.Fatalf("relay panic = %v, want http.ErrAbortHandler", recovered)
					}
				}()
				proxy.relayUpstreamResponse(context.Background(), &prefixDisconnectedWriter{
					header: http.Header{},
					limit:  len(tt.delivered),
				}, session, testPromptOneID, providerRequestInference, seq, response)
			}()
			failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
			if failed != tt.wantFailure {
				t.Fatalf("upstreamFailureUnrecovered = %v/%d/%q, want failed=%v", failed, status, detail, tt.wantFailure)
			}
			if tt.wantFailure && (status != http.StatusBadGateway || detail != tt.wantDetail) {
				t.Fatalf("stream failure = %d/%q, want %d/%q", status, detail, http.StatusBadGateway, tt.wantDetail)
			}
			if successes, failures := session.inferenceSuccesses, session.inferenceFailures; successes != tt.wantSuccesses || failures != tt.wantFailures {
				t.Fatalf("inference accounting = %d successes / %d failures, want %d/%d", successes, failures, tt.wantSuccesses, tt.wantFailures)
			}
		})
	}
}

func TestProviderProxyChildDisconnectOnNonStreamedResponseStaysUnaccounted(t *testing.T) {
	proxy, session, _ := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: testUnreachableUpstreamURL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusTooManyRequests, "rate limit exceeded")

	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"response-1"}`)),
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != http.ErrAbortHandler {
				t.Fatalf("relay panic = %v, want http.ErrAbortHandler", recovered)
			}
		}()
		seq := testAllocateInferenceSeq(session)
		if err := session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now()); err != nil {
			t.Fatal(err)
		}
		proxy.relayUpstreamResponse(context.Background(), &childDisconnectedWriter{header: http.Header{}}, session, testPromptOneID, providerRequestInference, seq, response)
	}()
	// A partially delivered JSON body proves nothing about the inference:
	// it must stay unaccounted, leaving the earlier recorded failure as the
	// unrecovered final outcome instead of masking it as a success.
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); !failed || status != http.StatusTooManyRequests || !strings.Contains(detail, "rate limit") {
		t.Fatalf("upstreamFailureUnrecovered after child disconnect on partial non-streamed 2xx = %v/%d/%q, want the earlier 429 unrecovered", failed, status, detail)
	}
	if successes, failures := session.inferenceSuccesses, session.inferenceFailures; successes != 0 || failures != 1 {
		t.Fatalf("inference accounting = %d successes / %d failures, want 0/1 (partial delivery unaccounted)", successes, failures)
	}
}

func TestProviderProxyChildCancelDuringStreamedSuccessCountsAsSuccess(t *testing.T) {
	firstChunkSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the request body so the server's background read can
		// observe the proxy closing the upstream connection.
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.completed\ndata: {}\n\n")
		flusher.Flush()
		close(firstChunkSent)
		// Keep the upstream stream open until the proxy's upstream request
		// is cancelled by the child's disconnect.
		<-r.Context().Done()
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, strings.NewReader(`{"model":"test-model","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	<-firstChunkSent
	chunk := make([]byte, 64)
	if _, err := response.Body.Read(chunk); err != nil {
		t.Fatalf("read first chunk: %v", err)
	}
	// The child got what it needed and drops the stream.
	cancel()
	_ = response.Body.Close()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := session.wait(waitCtx); err != nil {
		t.Fatalf("relay did not finish after child cancel: %v", err)
	}
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatalf("child cancel of a healthy 2xx stream accounted as failure: %d %q", status, detail)
	}
	if successes, failures := session.inferenceSuccesses, session.inferenceFailures; successes != 1 || failures != 0 {
		t.Fatalf("inference accounting = %d successes / %d failures, want 1/0", successes, failures)
	}
}

func TestProviderProxyChildCancelBeforeHeadersIsNotAnUpstreamFailure(t *testing.T) {
	requestSeen := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(requestSeen)
		// Never write headers: the child abandons the request first.
		<-r.Context().Done()
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	// An earlier request already succeeded; the abandoned one is later-issued.
	session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusOK, "")

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, strings.NewReader(`{"model":"test-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	<-requestSeen
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled request unexpectedly completed")
	}
	waitProviderProxyIdle(t, session)
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatalf("pre-header child cancel accounted as upstream failure: %d %q", status, detail)
	}
	if failures := session.inferenceFailures; failures != 0 {
		t.Fatalf("inferenceFailures = %d, want 0", failures)
	}
}

func TestProviderProxyChildCancelDuringBodyIsNotAnUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be reached when the child abandons its request body")
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	// An earlier request already succeeded; the abandoned one is later-issued.
	session.recordInferenceResponse(testPromptOneID, providerRequestInference, http.StatusOK, "")

	bodyReader, bodyWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	// Deliver a partial body so the proxy is blocked inside the body read,
	// then abandon the request from the child side.
	if _, err := bodyWriter.Write([]byte(`{"model":`)); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = bodyWriter.CloseWithError(context.Canceled)
	<-done
	waitProviderProxyIdle(t, session)
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatalf("mid-body child cancel accounted as upstream failure: %d %q", status, detail)
	}
	if failures := session.inferenceFailures; failures != 0 {
		t.Fatalf("inferenceFailures = %d, want 0", failures)
	}
}

func TestProviderProxyStreamedSuccessOverLimitCountsAsFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// No Content-Length: the body is chunked, so the size is unknown
		// until the relay reads past the limit.
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for _, chunk := range []string{"data: {}\n\n", "data: {\"done\":true}\n\n"} {
			_, _ = io.WriteString(w, chunk)
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL, UpstreamBearerToken: testUpstreamToken, MaxResponseBytes: 4,
	})
	defer session.close()

	request, err := http.NewRequest(http.MethodPost, binding.BaseURL+providerOpenAIResponsesV1Path, strings.NewReader(`{"model":"test-model","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(providerAuthorizationHeader, "Bearer "+binding.Credential)
	// The relay aborts the connection once the body overruns the limit; the
	// client observes either a transport error or a truncated body.
	response, err := http.DefaultClient.Do(request)
	if err == nil {
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr == nil && len(body) > 4 {
			t.Fatalf("oversized streamed body was relayed in full: %q", body)
		}
	}

	waitProviderProxyIdle(t, session)
	failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID)
	if !failed || status != http.StatusBadGateway || detail != "provider upstream stream failed" {
		t.Fatalf("upstreamFailureUnrecovered after oversized 2xx stream = %v/%d/%q, want failure", failed, status, detail)
	}
	session.mu.Lock()
	successes, failures := session.inferenceSuccesses, session.inferenceFailures
	session.mu.Unlock()
	if successes != 0 || failures != 1 {
		t.Fatalf("inference accounting after oversized 2xx stream = %d/%d, want 0/1", successes, failures)
	}
}

// recordInferenceResponse is a test convenience that issues a fresh sequence
// for one accounted response so fixtures can record outcomes in order.
func (s *providerProxySession) recordInferenceResponse(promptID string, class providerRequestClass, statusCode int, detail string) {
	s.mu.Lock()
	s.issuedInference++
	seq := s.issuedInference
	s.mu.Unlock()
	s.recordInferenceOutcome(promptID, class, seq, statusCode, detail)
}

func TestProviderProxyInferenceAccountingOrdersByIssuance(t *testing.T) {
	_, session, _ := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: testUnreachableUpstreamURL, UpstreamBearerToken: testUpstreamToken,
	})
	defer session.close()
	first := testAllocateInferenceSeq(session)
	err := session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second := testAllocateInferenceSeq(session)
	err = session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// The later-issued request fails fast; the earlier one succeeds afterwards.
	session.recordInferenceOutcome(testPromptOneID, providerRequestInference, second, http.StatusTooManyRequests, "rate limit exceeded")
	session.recordInferenceOutcome(testPromptOneID, providerRequestInference, first, http.StatusOK, "")
	if failed, status, detail := session.upstreamFailureUnrecovered(testPromptOneID); !failed || status != http.StatusTooManyRequests || detail != "rate limit exceeded" {
		t.Fatalf("upstreamFailureUnrecovered = %v/%d/%q, want the later-issued failure to decide", failed, status, detail)
	}
	// A failure issued before a success that completes later is recovered.
	third := testAllocateInferenceSeq(session)
	_ = session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
	fourth := testAllocateInferenceSeq(session)
	_ = session.consumeInferenceRequest(testPromptOneID, providerRequestInference, time.Now())
	session.recordInferenceOutcome(testPromptOneID, providerRequestInference, fourth, http.StatusOK, "")
	session.recordInferenceOutcome(testPromptOneID, providerRequestInference, third, http.StatusBadGateway, "late failure")
	if failed, _, _ := session.upstreamFailureUnrecovered(testPromptOneID); failed {
		t.Fatal("earlier-issued failure was not recovered by the later-issued success")
	}
}

// waitProviderProxyIdle mirrors prompt settlement, which lets in-flight relays
// drain before the accounting is read: the failure detail is attached once
// the relayed error body has been observed, after the client may already
// have read a Content-Length-delimited body to EOF.
func waitProviderProxyIdle(t *testing.T, session *providerProxySession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := session.wait(ctx); err != nil {
		t.Fatalf("provider proxy did not drain: %v", err)
	}
}
