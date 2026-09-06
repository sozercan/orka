/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/tools"
)

func TestCompatCoordinatorToolsExcludeKubernetesTools(t *testing.T) {
	for _, api := range []string{"openai", "anthropic"} {
		t.Run(api, func(t *testing.T) {
			file, err := os.CreateTemp("/tmp", "orka-compat-tool-*")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.Remove(file.Name()) })
			const fileContent = "built-in file_read result"
			_, err = file.WriteString(fileContent)
			require.NoError(t, err)
			require.NoError(t, file.Close())

			var endpointCalls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				endpointCalls.Add(1)
				_, _ = io.WriteString(w, `{"content":"custom tool result"}`)
			}))
			t.Cleanup(endpoint.Close)

			captured := make(chan AnthropicRequest, 2)
			var modelCalls atomic.Int32
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request AnthropicRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				select {
				case captured <- request:
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				switch modelCalls.Add(1) {
				case 1:
					// Also attempt an unadvertised custom tool to verify rejection.
					_, _ = fmt.Fprintf(w, `{"id":"msg-tools","type":"message","role":"assistant","model":"test-model","content":[{"type":"tool_use","id":"custom-call","name":"example-lookup","input":{"query":"test"}},{"type":"tool_use","id":"builtin-call","name":"file_read","input":{"path":%q}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`, file.Name())
				case 2:
					_, _ = fmt.Fprintf(w, `{"id":"msg-result","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":%q}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`, goalStateSentinel+"\n"+fileContent)
				default:
					http.Error(w, "unexpected model call", http.StatusBadRequest)
				}
			}))
			t.Cleanup(model.Close)

			app, path := setupCompatCustomToolsApp(t, api, model.URL, endpoint.URL)
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"anthropic/test-model","messages":[{"role":"user","content":"read the file"}],"max_tokens":1024}`))
			request.Header.Set("Content-Type", "application/json")
			response, err := app.Test(request, fiber.TestConfig{Timeout: 10 * time.Second})
			require.NoError(t, err)
			defer func() { _ = response.Body.Close() }()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode, string(body))
			require.Contains(t, string(body), fileContent)
			require.EqualValues(t, 0, endpointCalls.Load(), "custom HTTP tools must not execute")
			require.EqualValues(t, 2, modelCalls.Load())
			require.Len(t, captured, 2)

			allowed := append(slices.Clone(builtinProxyTools), coordinatorProxyTools...)
			requests := []AnthropicRequest{<-captured, <-captured}
			for _, upstream := range requests {
				names := make(map[string]bool)
				for _, definition := range upstream.Tools {
					require.Contains(t, allowed, definition.Name, "only supported tools may be advertised")
					require.NotContains(t, names, definition.Name, "tool definitions must be unique")
					names[definition.Name] = true
					registered, ok := tools.DefaultRegistry.Get(definition.Name)
					require.True(t, ok, "advertised tool %q must be registered", definition.Name)
					require.Equal(t, registered.Description(), definition.Description)
					require.JSONEq(t, string(registered.Parameters()), string(definition.InputSchema))
				}
				require.Contains(t, names, "file_read")
				require.NotContains(t, names, "example-lookup")
			}
			results := compatCustomToolResults(t, requests[1].Messages)
			require.JSONEq(t, `{"success":false,"error":"tool \"example-lookup\" is not available in this request"}`, results["custom-call"])
			var result tools.FileReadResult
			require.NoError(t, json.Unmarshal([]byte(results["builtin-call"]), &result))
			require.Equal(t, fileContent, result.Content)
			require.Equal(t, file.Name(), result.Path)
		})
	}
}

func TestCompatToolsDisabledPreservesClientDefinitions(t *testing.T) {
	for _, api := range []string{"openai", "anthropic"} {
		t.Run(api, func(t *testing.T) {
			clientTools := []AnthropicTool{
				{Name: "example-lookup", Description: "Client lookup", InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","enum":["first","second"]}},"required":["query"]}`)},
				{Name: "file_read", Description: "Client file reader", InputSchema: json.RawMessage(`{"type":"object","properties":{"filename":{"type":"string"},"options":{"type":"object","properties":{"lines":{"type":"integer","minimum":1,"maximum":5}}}},"required":["filename"]}`)},
			}
			captured := make(chan AnthropicRequest, 1)
			var modelCalls atomic.Int32
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request AnthropicRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if modelCalls.Add(1) != 1 {
					http.Error(w, "server-side tool loop must be disabled", http.StatusBadRequest)
					return
				}
				captured <- request
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"msg-client","type":"message","role":"assistant","model":"test-model","content":[{"type":"tool_use","id":"client-call","name":"file_read","input":{"filename":"client.txt"}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`)
			}))
			t.Cleanup(model.Close)

			app, path := setupCompatCustomToolsApp(t, api, model.URL, "http://unused.invalid/tool")
			requestBody := map[string]any{
				"model": "anthropic/test-model", "max_tokens": 1024,
				"messages": []map[string]string{{"role": "user", "content": "read a client file"}},
				"tools":    clientTools,
			}
			if api == "openai" {
				functions := make([]map[string]any, 0, len(clientTools))
				for _, tool := range clientTools {
					functions = append(functions, map[string]any{"type": "function", "function": map[string]any{
						"name": tool.Name, "description": tool.Description, "parameters": tool.InputSchema,
					}})
				}
				requestBody["tools"] = functions
			}
			encoded, err := json.Marshal(requestBody)
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(encoded)))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Orka-Tools", "disabled")
			response, err := app.Test(request, fiber.TestConfig{Timeout: 10 * time.Second})
			require.NoError(t, err)
			defer func() { _ = response.Body.Close() }()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode, string(body))
			require.Contains(t, string(body), "client-call")
			require.Contains(t, string(body), "file_read")
			require.EqualValues(t, 1, modelCalls.Load())
			require.Len(t, captured, 1)
			upstream := <-captured
			require.Len(t, upstream.Tools, len(clientTools))
			for i, expected := range clientTools {
				require.Equal(t, expected.Name, upstream.Tools[i].Name)
				require.Equal(t, expected.Description, upstream.Tools[i].Description)
				require.JSONEq(t, string(expected.InputSchema), string(upstream.Tools[i].InputSchema))
			}
		})
	}
}

func setupCompatCustomToolsApp(t *testing.T, api, modelURL, toolURL string) (*fiber.App, string) {
	t.Helper()
	objects := make([]runtime.Object, 0, 4)
	objects = append(objects,
		&corev1alpha1.Provider{
			ObjectMeta: metav1.ObjectMeta{Name: "anthropic", Namespace: "default"},
			Spec: corev1alpha1.ProviderSpec{
				Type: corev1alpha1.ProviderTypeAnthropic, DefaultModel: "test-model", BaseURL: modelURL,
				SecretRef: corev1alpha1.ProviderSecretRef{Name: "anthropic-secret", Key: "api-key"},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "anthropic-secret", Namespace: "default"},
			Data:       map[string][]byte{"api-key": []byte("test-key")},
		},
	)
	for _, name := range []string{"example-lookup", "file_read"} {
		objects = append(objects, &corev1alpha1.Tool{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1alpha1.ToolSpec{
				Description: "Kubernetes custom tool",
				Parameters:  &apiextensionsv1.JSON{Raw: []byte(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`)},
				HTTP:        &corev1alpha1.HTTPExecution{URL: toolURL, Method: http.MethodPost},
			},
		})
	}
	if api == "openai" {
		handler, app := setupTestOpenAIHandler(objects...)
		path := "/openai/v1/chat/completions"
		app.Post(path, handler.HandleChatCompletions)
		return app, path
	}
	handler, app := setupTestAnthropicHandler(objects...)
	path := "/anthropic/v1/messages"
	app.Post(path, handler.HandleMessages)
	return app, path
}

func compatCustomToolResults(t *testing.T, messages []AnthropicMessage) map[string]string {
	t.Helper()
	results := make(map[string]string)
	for _, message := range messages {
		blocks, err := parseAnthropicContent(message.Content)
		require.NoError(t, err)
		for _, block := range blocks {
			if block.Type != "tool_result" {
				continue
			}
			content, err := parseAnthropicContent(block.Content)
			require.NoError(t, err)
			require.Len(t, content, 1)
			results[block.ToolUseID] = content[0].Text
		}
	}
	return results
}
