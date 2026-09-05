package supervisor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderProxyAcceptsAgentKitChatCompletions(t *testing.T) {
	type observedRequest struct {
		path          string
		authorization string
		body          map[string]any
	}
	observed := make(chan observedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode AgentKit upstream body: %v", err)
		}
		observed <- observedRequest{
			path: request.URL.Path, authorization: request.Header.Get(providerAuthorizationHeader), body: body,
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
	}))
	defer upstream.Close()

	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: upstream.URL + "/v1", UpstreamBearerToken: testUpstreamToken,
		ProviderKind: providerKindAgentKit, Model: "gpt-test", ModelOutputLimit: 2048,
	})
	defer session.close()
	response := doProviderProxyRequest(
		t, http.MethodPost, binding.BaseURL+providerOpenAIChatCompletionsPath, binding.Credential,
		[]byte(`{"model":"gpt-test","messages":[{"role":"user","content":"hello"}],"max_tokens":9000}`), nil,
	)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("AgentKit proxy status = %d body=%s", response.StatusCode, body)
	}
	request := <-observed
	if request.path != "/v1/chat/completions" || request.authorization != "Bearer "+testUpstreamToken {
		t.Fatalf("AgentKit upstream request = path %q authorization %q", request.path, request.authorization)
	}
	if request.body["model"] != "gpt-test" || request.body[providerMaxTokensField] != float64(2048) {
		t.Fatalf("AgentKit upstream body = %#v", request.body)
	}
}

func TestProviderProxyRejectsAgentKitNonChatRoutes(t *testing.T) {
	_, session, binding := activeTestProviderProxySession(t, ProviderProxyConfig{
		UpstreamBaseURL: testUnreachableUpstreamURL + "/v1", UpstreamBearerToken: testUpstreamToken,
		ProviderKind: providerKindAgentKit, Model: "gpt-test",
	})
	defer session.close()
	for _, path := range []string{providerOpenAIResponsesV1Path, providerModelsV1Path, "/v1/messages"} {
		response := doProviderProxyRequest(
			t, http.MethodPost, binding.BaseURL+path, binding.Credential, []byte(`{"model":"gpt-test"}`), nil,
		)
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("AgentKit route %s status = %d, want %d", path, response.StatusCode, http.StatusForbidden)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
}
