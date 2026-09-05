package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

const mcpUpstreamPrivateDiagnostic = "private-upstream-diagnostic-must-not-reach-agent"

func TestACPMCPBrokerCustomToolFailureAllowsRecovery(t *testing.T) {
	for _, effect := range []harnessv2.MCPToolEffect{harnessv2.MCPToolEffectReadOnly, harnessv2.MCPToolEffectConsequential} {
		t.Run(string(effect), func(t *testing.T) {
			var calls atomic.Int32
			broker, request := newMCPExecutionErrorTestBroker(t, effect, func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(mcpUpstreamPrivateDiagnostic))
					return
				}
				_, _ = w.Write([]byte(acpMCPTestOKBody))
			}, nil)
			first := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			if first.Code != http.StatusOK {
				t.Fatalf("tool failure returned HTTP %d, want a valid MCP result: %s", first.Code, first.Body.String())
			}
			var response harnessv2.MCPBrokerCallResponse
			if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Validate() != nil || !response.IsError || response.CallID != request.Call.CallID || response.Replayed {
				t.Fatalf("failed tool response = %#v", response)
			}
			if string(response.Result) != `{"isError":true,"error":"MCP tool execution failed"}` ||
				strings.Contains(first.Body.String(), mcpUpstreamPrivateDiagnostic) {
				t.Fatalf("tool failure was not a bounded generic result: %s", first.Body.String())
			}
			if effect == harnessv2.MCPToolEffectConsequential {
				replay := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
				if err := json.Unmarshal(replay.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if replay.Code != http.StatusOK || !response.IsError || !response.Replayed || calls.Load() != 1 {
					t.Fatalf("failed consequential replay = %#v status=%d calls=%d", response, replay.Code, calls.Load())
				}
				identity := store.ExternalEffectIdentity{
					Kind: "acp-mcp-tool", Namespace: request.Namespace,
					AggregateID: string(request.Authorization.RuntimeSessionUID), OperationID: string(request.Metadata.OperationID),
				}
				id, err := identity.CanonicalID()
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := broker.Effects.GetExternalEffect(context.Background(), id)
				if err != nil || receipt.State != store.ExternalEffectSucceeded ||
					strings.Contains(string(receipt.Response), mcpUpstreamPrivateDiagnostic) {
					t.Fatalf("tool error did not commit a redacted replay receipt: %#v err=%v", receipt, err)
				}
			}
			request.Call.CallID = "recovery-call"
			request.Metadata.OperationID = "recovery-operation"
			var err error
			request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
			if err != nil {
				t.Fatal(err)
			}
			second := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			var recovery harnessv2.MCPBrokerCallResponse
			if err := json.Unmarshal(second.Body.Bytes(), &recovery); err != nil {
				t.Fatal(err)
			}
			if second.Code != http.StatusOK || recovery.IsError || recovery.Replayed ||
				string(recovery.Result) != acpMCPTestOKBody || calls.Load() != 2 {
				t.Fatalf("recovery response = %#v status=%d calls=%d", recovery, second.Code, calls.Load())
			}
		})
	}
}

func TestACPMCPBrokerDoesNotRecoverInfrastructureFailures(t *testing.T) {
	for _, name := range []string{"unauthorized", "forbidden", "malformed response", "outbound policy", "descriptor drift"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			broker, request := newMCPExecutionErrorTestBroker(t, harnessv2.MCPToolEffectReadOnly, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				switch name {
				case "unauthorized":
					w.WriteHeader(http.StatusUnauthorized)
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
				case "malformed response":
					w.Header().Set("Content-Length", "1000")
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				_, _ = w.Write([]byte(mcpUpstreamPrivateDiagnostic))
			}, func(tool *corev1alpha1.Tool) {
				if name == "outbound policy" {
					tool.Spec.HTTP.OutboundAccessPolicyRef = &corev1alpha1.LocalObjectReference{Name: "missing-policy"}
				}
			})
			if name == "descriptor drift" {
				request.Authorization.ToolPolicy.Tools[0].Description = "different authorized definition"
				var err error
				request.Authorization.ToolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(request.Authorization.ToolPolicy.Tools)
				if err != nil {
					t.Fatal(err)
				}
				request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
				if err != nil {
					t.Fatal(err)
				}
			}
			response := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), mcpUpstreamPrivateDiagnostic) {
				t.Fatalf("infrastructure failure returned HTTP %d body=%s", response.Code, response.Body.String())
			}
			if (name == "outbound policy" || name == "descriptor drift") && calls.Load() != 0 {
				t.Fatal("rejected tool policy reached the endpoint")
			}
		})
	}
}

func TestACPMCPBrokerToolDeadlineRecoveryIsReadOnly(t *testing.T) {
	for _, effect := range []harnessv2.MCPToolEffect{harnessv2.MCPToolEffectReadOnly, harnessv2.MCPToolEffectConsequential} {
		t.Run(string(effect), func(t *testing.T) {
			var calls atomic.Int32
			cancelled := make(chan struct{})
			broker, request := newMCPExecutionErrorTestBroker(t, effect, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if calls.Add(1) == 1 {
					<-r.Context().Done()
					close(cancelled)
					return
				}
				_, _ = w.Write([]byte(acpMCPTestOKBody))
			}, func(tool *corev1alpha1.Tool) {
				tool.Spec.HTTP.Timeout = &metav1.Duration{Duration: 30 * time.Millisecond}
			})
			first := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("Tool request deadline did not cancel downstream execution")
			}
			if effect == harnessv2.MCPToolEffectConsequential {
				if first.Code != http.StatusBadGateway {
					t.Fatalf("consequential timeout status=%d, want fatal broker error", first.Code)
				}
				identity := store.ExternalEffectIdentity{
					Kind: "acp-mcp-tool", Namespace: request.Namespace,
					AggregateID: string(request.Authorization.RuntimeSessionUID), OperationID: string(request.Metadata.OperationID),
				}
				id, err := identity.CanonicalID()
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := broker.Effects.GetExternalEffect(context.Background(), id)
				if err != nil || receipt.State != store.ExternalEffectOutcomeUnknown || len(receipt.Response) != 0 {
					t.Fatalf("consequential timeout receipt=%#v err=%v", receipt, err)
				}
				replay := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
				if replay.Code != http.StatusBadGateway || calls.Load() != 1 {
					t.Fatalf("unknown consequential call replay status=%d calls=%d", replay.Code, calls.Load())
				}
				return
			}
			var failed harnessv2.MCPBrokerCallResponse
			if err := json.Unmarshal(first.Body.Bytes(), &failed); err != nil {
				t.Fatal(err)
			}
			if first.Code != http.StatusOK || !failed.IsError || failed.CallID != request.Call.CallID ||
				string(failed.Result) != `{"isError":true,"error":"MCP tool execution failed"}` {
				t.Fatalf("read-only timeout response=%#v status=%d", failed, first.Code)
			}
			request.Call.CallID = "after-tool-timeout"
			request.Metadata.OperationID = "after-tool-timeout-operation"
			var err error
			request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
			if err != nil {
				t.Fatal(err)
			}
			second := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			var recovered harnessv2.MCPBrokerCallResponse
			if err := json.Unmarshal(second.Body.Bytes(), &recovered); err != nil {
				t.Fatal(err)
			}
			if second.Code != http.StatusOK || recovered.IsError || calls.Load() != 2 || string(recovered.Result) != acpMCPTestOKBody {
				t.Fatalf("read-only timeout recovery=%#v status=%d calls=%d", recovered, second.Code, calls.Load())
			}
		})
	}
}

func TestACPMCPBrokerPromptCancellationDoesNotBecomeToolError(t *testing.T) {
	for _, name := range []string{"deadline", "cancellation"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			var stopPrompt context.CancelFunc
			broker, request := newMCPExecutionErrorTestBroker(t, harnessv2.MCPToolEffectReadOnly, func(_ http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if name == "cancellation" {
					stopPrompt()
				}
				<-r.Context().Done()
			}, func(tool *corev1alpha1.Tool) {
				tool.Spec.HTTP.Timeout = &metav1.Duration{Duration: time.Second}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			stopPrompt = cancel
			defer cancel()
			bounded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { broker.ServeHTTP(w, r.WithContext(ctx)) })
			response := performMCPBrokerCall(t, bounded, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			if response.Code != http.StatusBadGateway || calls.Load() != 1 {
				t.Fatalf("prompt %s returned HTTP %d calls=%d", name, response.Code, calls.Load())
			}
		})
	}
}

func TestACPMCPBrokerToolFailureStillRequiresPromptAuthority(t *testing.T) {
	for _, effect := range []harnessv2.MCPToolEffect{harnessv2.MCPToolEffectReadOnly, harnessv2.MCPToolEffectConsequential} {
		t.Run(string(effect), func(t *testing.T) {
			var calls atomic.Int32
			broker, request := newMCPExecutionErrorTestBroker(t, effect, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}, nil)
			broker.Prompts = ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error {
				if calls.Load() != 0 {
					return errors.New("prompt settled during tool execution")
				}
				return nil
			})
			response := performMCPBrokerCall(t, broker, request, strings.Repeat("b", 32), []byte(strings.Repeat("c", 32)))
			if response.Code != http.StatusBadGateway || calls.Load() != 1 {
				t.Fatalf("revoked prompt returned HTTP %d calls=%d", response.Code, calls.Load())
			}
		})
	}
}

func newMCPExecutionErrorTestBroker(
	t *testing.T,
	effect harnessv2.MCPToolEffect,
	upstream http.HandlerFunc,
	configure func(*corev1alpha1.Tool),
) (*ACPMCPBroker, harnessv2.MCPBrokerCallRequest) {
	t.Helper()
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	request, profile := testMCPBrokerRequest(t, effect)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: request.Call.ToolName, Namespace: request.Namespace},
		Spec: corev1alpha1.ToolSpec{
			Description: "controlled test tool", Parameters: &apiextensionsJSONForMCPTest,
			BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
			HTTP:              &corev1alpha1.HTTPExecution{URL: server.URL, Method: http.MethodPost},
		},
	}
	if effect == harnessv2.MCPToolEffectConsequential {
		tool.Spec.BrokeredToolClass = corev1alpha1.AgentRuntimeBrokeredToolClassWrite
	}
	if configure != nil {
		configure(tool)
	}
	descriptor, err := customACPMCPToolDescriptor(tool)
	if err != nil {
		t.Fatal(err)
	}
	request.Authorization.ToolPolicy.Tools = []harnessv2.MCPToolDescriptor{descriptor}
	request.Authorization.ToolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(request.Authorization.ToolPolicy.Tools)
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: acpDispatcherTestTaskName, Namespace: request.Namespace, UID: acpDispatcherTaskUID}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tool, task).Build()
	effects, fence := newMCPBrokerControlStore(t)
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: strings.Repeat("b", 32), CapabilitySecret: []byte(strings.Repeat("c", 32)),
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
				Task: ACPMCPAuthenticatedTask{Name: task.Name, Namespace: task.Namespace, UID: string(task.UID)},
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error { return nil }),
		Executor: RegistryACPMCPToolExecutor{
			Reader: reader, KubeClient: k8sfake.NewSimpleClientset(), HTTPClient: server.Client(),
		},
		Effects: effects,
	}
	return broker, request
}
