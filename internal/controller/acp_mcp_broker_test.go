package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	storesqlite "github.com/orka-agents/orka/internal/store/sqlite"
	workerexecutor "github.com/orka-agents/orka/internal/worker"
	"github.com/orka-agents/orka/internal/workerenv"
)

var apiextensionsJSONForMCPTest = apiextensionsv1.JSON{Raw: json.RawMessage(`{"type":"object"}`)}

const (
	acpMCPTestOKBody         = `{"ok":true}`
	acpMCPTestOtherNamespace = "other-namespace"
	acpMCPTestTTSEndpoint    = "https://tts.example.test/token"
	acpMCPTestToolName       = "mutate"
	brokeredTestReportScope  = "reports.read"
)

type recordingContextTokenExchanger struct {
	calls   atomic.Int32
	request contexttoken.ExchangeRequest
}

func (e *recordingContextTokenExchanger) Exchange(_ context.Context, request contexttoken.ExchangeRequest) (string, error) {
	e.calls.Add(1)
	e.request = request
	return "exchanged-transaction-token", nil
}

func assertContextTokenExchange(t *testing.T, exchanger *recordingContextTokenExchanger, subjectToken, scope string) {
	t.Helper()
	if calls := exchanger.calls.Load(); calls != 1 {
		t.Fatalf("TTS exchanger calls = %d, want 1", calls)
	}
	if exchanger.request.SubjectToken != subjectToken || exchanger.request.Scope != scope {
		t.Fatalf("TTS exchange subject = %q scope = %q", exchanger.request.SubjectToken, exchanger.request.Scope)
	}
}

func TestRegistryACPMCPToolExecutorReusesCustomToolExecutorWithIdempotency(t *testing.T) {
	request, _ := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	request.Call.ToolName = "custom_tool"
	request.Call.Arguments = json.RawMessage(`{"value":"x"}`)
	request.Metadata.OperationID = "mcp-custom-operation"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != string(request.Metadata.OperationID) {
			t.Fatalf("Idempotency-Key = %q", r.Header.Get("Idempotency-Key"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["value"] != "x" {
			t.Fatalf("custom tool body = %#v err=%v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(acpMCPTestOKBody))
	}))
	defer upstream.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "custom_tool", Namespace: request.Namespace},
		Spec: corev1alpha1.ToolSpec{
			Description: "custom write", BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassWrite,
			Parameters: &apiextensionsJSONForMCPTest,
			HTTP:       &corev1alpha1.HTTPExecution{URL: upstream.URL, Method: http.MethodPost},
		},
	}
	descriptor, err := customACPMCPToolDescriptor(tool)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Name: acpDispatcherTestTaskName, Namespace: request.Namespace, UID: acpDispatcherTaskUID,
	}}
	authenticated := ACPMCPAuthenticatedTask{
		Name: task.Name, Namespace: task.Namespace, UID: string(task.UID),
	}
	ctx := withACPMCPAuthenticatedTask(context.Background(), authenticated)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tool, task).Build()
	executor := RegistryACPMCPToolExecutor{
		Reader: reader, KubeClient: k8sfake.NewSimpleClientset(), HTTPClient: upstream.Client(),
	}
	result, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != acpMCPTestOKBody {
		t.Fatalf("custom tool result = %s", result)
	}

	changed := tool.DeepCopy()
	changed.Spec.Description = "changed after authorization"
	reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(changed).Build()
	executor.Reader = reader
	if _, err := executor.ExecuteACPMCPTool(ctx, request, descriptor); err == nil ||
		!strings.Contains(err.Error(), "changed after prompt authorization") {
		t.Fatalf("descriptor drift error = %v", err)
	}
}

func TestRegistryACPMCPToolExecutorBindsTaskTransactionAuthority(t *testing.T) {
	request, _ := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	request.Call.ToolName = "custom_tool"
	request.Call.Arguments = json.RawMessage(`{"value":"x"}`)
	request.Metadata.OperationID = "mcp-custom-operation"
	var lastTxnToken atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastTxnToken.Store(r.Header.Get("Txn-Token"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(acpMCPTestOKBody))
	}))
	defer upstream.Close()
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: "custom_tool", Namespace: request.Namespace},
		Spec: corev1alpha1.ToolSpec{
			Description: "custom write", BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassWrite,
			Parameters: &apiextensionsJSONForMCPTest,
			HTTP: &corev1alpha1.HTTPExecution{
				URL: upstream.URL, Method: http.MethodPost,
				AuthSecretRef: &corev1alpha1.SecretKeySelector{Name: "tool-credential", Key: "token"},
			},
		},
	}
	descriptor, err := customACPMCPToolDescriptor(tool)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	authenticated := ACPMCPAuthenticatedTask{Name: acpDispatcherTestTaskName, Namespace: request.Namespace, UID: string(request.Metadata.TaskUID)}
	newTask := func(scopes []string, constraint string, tokenSecret string) *corev1alpha1.Task {
		task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
			Name: authenticated.Name, Namespace: authenticated.Namespace, UID: acpDispatcherTaskUID,
		}}
		if tokenSecret != "" {
			task.Annotations = map[string]string{"orka.ai/transaction-token-secret": tokenSecret}
		}
		task.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "txn-1", Scopes: scopes}
		if constraint != "" {
			task.Spec.Transaction.Context = map[string]string{"secret": constraint}
		}
		return task
	}
	toolCredential := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tool-credential", Namespace: request.Namespace},
		Data:       map[string][]byte{"token": []byte("tool-token")},
	}
	newExecutor := func(enforce bool, objects ...client.Object) RegistryACPMCPToolExecutor {
		objects = append(objects, tool.DeepCopy())
		return RegistryACPMCPToolExecutor{
			Reader:     fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
			KubeClient: k8sfake.NewSimpleClientset(toolCredential.DeepCopy()), HTTPClient: upstream.Client(),
			EnforceTransactionCredentialAuth: enforce,
		}
	}
	authorizedTask := newTask([]string{"orka:secrets:credentials:read"}, "tool-credential", "")
	ctx := withACPMCPAuthenticatedTask(context.Background(), authenticated)

	t.Run("enforcement off still binds task transaction authority", func(t *testing.T) {
		t.Setenv(workerenv.ContextTokenOutboundScope, "")
		t.Setenv(workerenv.TransactionScope, "controller.scope")
		t.Setenv(workerenv.TransactionScopes, "controller.scope")
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "task-txn-token-off", Namespace: request.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: corev1alpha1.GroupVersion.String(), Kind: taskResourceKind,
					Name: authenticated.Name, UID: acpDispatcherTaskUID,
				}},
			},
			Data: map[string][]byte{"token": []byte("task-scoped-token-off")},
		}
		exchanger := &recordingContextTokenExchanger{}
		executor := newExecutor(false,
			newTask([]string{brokeredTestReportScope}, "other-credential", tokenSecret.Name), tokenSecret)
		executor.TransactionExchange = &workerexecutor.TransactionExchangeConfig{
			TTS: contexttoken.TTSConfig{
				Endpoint:    acpMCPTestTTSEndpoint,
				TokenSource: contexttoken.TTSTokenSourceIncoming,
			},
			Exchanger: exchanger,
		}
		_, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
		if err != nil {
			t.Fatalf("enforcement-off execution error = %v", err)
		}
		if got, _ := lastTxnToken.Load().(string); got != "exchanged-transaction-token" {
			t.Fatalf("enforcement-off Txn-Token header = %q, want exchanged task authority", got)
		}
		assertContextTokenExchange(t, exchanger, "task-scoped-token-off", brokeredTestReportScope)
	})
	t.Run("enforcement off blanks controller ambient transaction authority", func(t *testing.T) {
		ambientTokenFile := filepath.Join(t.TempDir(), "controller-transaction-token")
		if err := os.WriteFile(ambientTokenFile, []byte("controller-ambient-token"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(workerenv.TransactionTokenFile, ambientTokenFile)
		t.Setenv(workerenv.TransactionScope, "controller.scope")
		t.Setenv(workerenv.TransactionScopes, "controller.scope")
		task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
			Name: authenticated.Name, Namespace: authenticated.Namespace, UID: acpDispatcherTaskUID,
		}}
		executor := newExecutor(false, task)
		_, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
		if err != nil {
			t.Fatalf("enforcement-off execution error = %v", err)
		}
		if got, _ := lastTxnToken.Load().(string); got != "" {
			t.Fatalf("enforcement-off Txn-Token header = %q, want no controller ambient token", got)
		}
	})
	t.Run("transactionless task skips injected incoming TTS", func(t *testing.T) {
		ambientTokenFile := filepath.Join(t.TempDir(), "controller-transaction-token")
		if err := os.WriteFile(ambientTokenFile, []byte("controller-ambient-token"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(workerenv.TransactionTokenFile, ambientTokenFile)
		t.Setenv(workerenv.ContextTokenTTSEndpoint, acpMCPTestTTSEndpoint)
		t.Setenv(workerenv.ContextTokenTTSTokenSource, contexttoken.TTSTokenSourceIncoming)
		task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
			Name: authenticated.Name, Namespace: authenticated.Namespace, UID: acpDispatcherTaskUID,
		}}
		exchanger := &recordingContextTokenExchanger{}
		executor := newExecutor(true, task)
		executor.TransactionExchange = &workerexecutor.TransactionExchangeConfig{
			TTS: contexttoken.TTSConfig{
				Endpoint:    acpMCPTestTTSEndpoint,
				TokenSource: contexttoken.TTSTokenSourceIncoming,
			},
			Exchanger: exchanger,
		}
		result, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
		if err != nil {
			t.Fatalf("transactionless execution error = %v", err)
		}
		if string(result) != acpMCPTestOKBody {
			t.Fatalf("transactionless result = %s", result)
		}
		if calls := exchanger.calls.Load(); calls != 0 {
			t.Fatalf("transactionless task reached TTS exchanger %d times: %#v", calls, exchanger.request)
		}
		if got, _ := lastTxnToken.Load().(string); got != "" {
			t.Fatalf("transactionless Txn-Token header = %q, want no controller ambient token", got)
		}
	})
	t.Run("enforcement off refuses controller service account despite matching task scope", func(t *testing.T) {
		t.Setenv(workerenv.TransactionScope, "controller.scope")
		t.Setenv(workerenv.TransactionScopes, "controller.scope")
		t.Setenv(workerenv.ContextTokenOutboundScope, brokeredTestReportScope)
		t.Setenv(workerenv.ServiceAccountToken, "controller-service-account-token")
		exchanger := &recordingContextTokenExchanger{}
		executor := newExecutor(false, newTask([]string{brokeredTestReportScope}, "", ""))
		executor.TransactionExchange = &workerexecutor.TransactionExchangeConfig{
			TTS: contexttoken.TTSConfig{
				Endpoint:    acpMCPTestTTSEndpoint,
				TokenSource: contexttoken.TTSTokenSourceServiceAccount,
			},
			Exchanger: exchanger,
		}
		requireACPMCPToolErrorContains(
			t, executor, ctx, request, descriptor, "cannot use a service account subject token",
		)
		if calls := exchanger.calls.Load(); calls != 0 {
			t.Fatalf("controller service account subject reached TTS exchanger %d times: %#v", calls, exchanger.request)
		}
	})
	t.Run("missing authenticated task fails closed without enforcement", func(t *testing.T) {
		executor := newExecutor(false, authorizedTask.DeepCopy())
		requireACPMCPToolErrorContains(
			t, executor, context.Background(), request, descriptor, "task authority is unavailable",
		)
	})
	t.Run("authenticated namespace mismatch fails closed without enforcement", func(t *testing.T) {
		executor := newExecutor(false, authorizedTask.DeepCopy())
		mismatched := withACPMCPAuthenticatedTask(context.Background(), ACPMCPAuthenticatedTask{
			Name: authenticated.Name, Namespace: acpMCPTestOtherNamespace, UID: authenticated.UID,
		})
		requireACPMCPToolErrorContains(t, executor, mismatched, request, descriptor, "task authority is unavailable")
	})
	t.Run("authenticated UID mismatch fails closed without enforcement", func(t *testing.T) {
		executor := newExecutor(false, authorizedTask.DeepCopy())
		mismatched := withACPMCPAuthenticatedTask(context.Background(), ACPMCPAuthenticatedTask{
			Name: authenticated.Name, Namespace: authenticated.Namespace, UID: "other-task-uid",
		})
		requireACPMCPToolErrorContains(t, executor, mismatched, request, descriptor, "task authority is unavailable")
	})
	t.Run("replaced task fails closed without enforcement", func(t *testing.T) {
		replaced := authorizedTask.DeepCopy()
		replaced.UID = "replaced-task-uid"
		executor := newExecutor(false, replaced)
		requireACPMCPToolErrorContains(t, executor, ctx, request, descriptor, "task identity changed")
	})
	t.Run("missing authenticated task fails closed under enforcement", func(t *testing.T) {
		executor := newExecutor(true, authorizedTask.DeepCopy())
		requireACPMCPToolErrorContains(
			t, executor, context.Background(), request, descriptor, "task authority is unavailable",
		)
	})
	t.Run("missing task object fails closed under enforcement", func(t *testing.T) {
		executor := newExecutor(true)
		requireACPMCPToolErrorContains(
			t, executor, ctx, request, descriptor, "load authenticated ACP MCP task authority",
		)
	})
	t.Run("task without credential-read scope is refused", func(t *testing.T) {
		executor := newExecutor(true, newTask([]string{brokeredTestReportScope}, "", ""))
		requireACPMCPToolErrorContains(
			t, executor, ctx, request, descriptor, "not authorized by task transaction authority",
		)
	})
	t.Run("task secret constraint must match the tool credential", func(t *testing.T) {
		executor := newExecutor(true, newTask([]string{"orka:secrets:credentials:read"}, "other-credential", ""))
		requireACPMCPToolErrorContains(
			t, executor, ctx, request, descriptor, "does not match task transaction authority",
		)
	})
	t.Run("authorized task executes and attaches its owner-referenced token", func(t *testing.T) {
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "task-txn-token", Namespace: request.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: corev1alpha1.GroupVersion.String(), Kind: taskResourceKind,
					Name: authenticated.Name, UID: acpDispatcherTaskUID,
				}},
			},
			Data: map[string][]byte{"token": []byte("task-scoped-token")},
		}
		executor := newExecutor(true,
			newTask([]string{"orka:secrets:credentials:read"}, "tool-credential", tokenSecret.Name), tokenSecret)
		result, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
		if err != nil {
			t.Fatalf("authorized execution error = %v", err)
		}
		if string(result) != acpMCPTestOKBody {
			t.Fatalf("authorized result = %s", result)
		}
		if got, _ := lastTxnToken.Load().(string); got != "task-scoped-token" {
			t.Fatalf("Txn-Token header = %q, want the task's owner-referenced token", got)
		}
	})
	t.Run("token secret not owned by the task fails closed", func(t *testing.T) {
		unowned := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "task-txn-token", Namespace: request.Namespace},
			Data:       map[string][]byte{"token": []byte("task-scoped-token")},
		}
		executor := newExecutor(true,
			newTask([]string{"orka:secrets:credentials:read"}, "tool-credential", unowned.Name), unowned)
		requireACPMCPToolErrorContains(
			t, executor, ctx, request, descriptor, "not owned by the authenticated Task",
		)
	})
}

func requireACPMCPToolErrorContains(
	t *testing.T,
	executor RegistryACPMCPToolExecutor,
	ctx context.Context,
	request harnessv2.MCPBrokerCallRequest,
	descriptor harnessv2.MCPToolDescriptor,
	want string,
) {
	t.Helper()
	_, err := executor.ExecuteACPMCPTool(ctx, request, descriptor)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("ExecuteACPMCPTool() error = %v, want containing %q", err, want)
	}
}

func TestACPMCPBrokerPassesResolvedTaskIdentityToExecutor(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	expectedTask := ACPMCPAuthenticatedTask{
		Name: acpDispatcherTestTaskName, Namespace: request.Namespace, UID: string(request.Metadata.TaskUID),
		ParentTaskID: "parent-task", AgentName: "authority-agent",
	}
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
				Task: expectedTask,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error { return nil }),
		Executor: ACPMCPToolExecutorFunc(func(ctx context.Context, _ harnessv2.MCPBrokerCallRequest, _ harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			got, ok := ACPMCPAuthenticatedTaskFromContext(ctx)
			if !ok || got != expectedTask {
				t.Fatalf("executor task identity = %#v, %v; want %#v, true", got, ok, expectedTask)
			}
			return json.RawMessage(acpMCPTestOKBody), nil
		}),
		Effects: effects,
	}

	response := performMCPBrokerCall(t, broker, request, bearer, capability)
	if response.Code != http.StatusOK {
		t.Fatalf("broker status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestACPMCPBrokerConsequentialCallUsesDurableReplay(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	var executions atomic.Int32
	var authorizations atomic.Int32
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) error {
			authorizations.Add(1)
			if got.Metadata.TaskUID != request.Metadata.TaskUID || got.Metadata.PromptID != request.Metadata.PromptID ||
				got.Lease.Generation != request.Lease.Generation {
				t.Fatalf("prompt authorizer received wrong identity: %#v", got)
			}
			return nil
		}),
		Executor: ACPMCPToolExecutorFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest, descriptor harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			executions.Add(1)
			if descriptor.Name != acpMCPTestToolName || got.Call.CallID != acpDispatcherToolCallID {
				t.Fatalf("executor received wrong call: descriptor=%#v call=%#v", descriptor, got.Call)
			}
			return json.RawMessage(`{"changed":true}`), nil
		}),
		Effects: effects,
	}

	first := performMCPBrokerCall(t, broker, request, bearer, capability)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d body=%s", first.Code, first.Body.String())
	}
	second := performMCPBrokerCall(t, broker, request, bearer, capability)
	if second.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s", second.Code, second.Body.String())
	}
	if executions.Load() != 1 {
		t.Fatalf("executor calls = %d, want one durable consequential execution", executions.Load())
	}
	if authorizations.Load() != 3 {
		t.Fatalf("prompt authorizer calls = %d, want pre-call plus post-execution authorization", authorizations.Load())
	}
	var response harnessv2.MCPBrokerCallResponse
	if err := json.Unmarshal(second.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if string(response.Result) != `{"changed":true}` || !response.Replayed {
		t.Fatalf("replayed response = %#v", response)
	}
}

func TestACPMCPBrokerConsequentialConcurrentDuplicateDoesNotDoubleExecute(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	entered := make(chan struct{})
	release := make(chan struct{})
	var executions atomic.Int32
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error { return nil }),
		Executor: ACPMCPToolExecutorFunc(func(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			if executions.Add(1) == 1 {
				close(entered)
			}
			<-release
			return json.RawMessage(`{"changed":true}`), nil
		}),
		Effects: effects,
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- performMCPBrokerCall(t, broker, request, bearer, capability) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first consequential execution did not start")
	}
	duplicate := performMCPBrokerCall(t, broker, request, bearer, capability)
	if duplicate.Code == http.StatusOK {
		t.Fatalf("concurrent duplicate unexpectedly succeeded: %s", duplicate.Body.String())
	}
	if executions.Load() != 1 {
		t.Fatalf("concurrent executor calls = %d, want 1", executions.Load())
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first consequential status = %d body=%s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first consequential execution did not finish")
	}
	replay := performMCPBrokerCall(t, broker, request, bearer, capability)
	if replay.Code != http.StatusOK || executions.Load() != 1 {
		t.Fatalf("committed replay status=%d calls=%d body=%s", replay.Code, executions.Load(), replay.Body.String())
	}
}

func TestACPMCPBrokerReadOnlyCallExecutesWithoutLedgerReplay(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	var executions atomic.Int32
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error { return nil }),
		Executor: ACPMCPToolExecutorFunc(func(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			executions.Add(1)
			return json.RawMessage(`{"value":"fresh"}`), nil
		}),
		Effects: effects,
	}
	for range 2 {
		response := performMCPBrokerCall(t, broker, request, bearer, capability)
		if response.Code != http.StatusOK {
			t.Fatalf("read-only status = %d body=%s", response.Code, response.Body.String())
		}
	}
	if executions.Load() != 2 {
		t.Fatalf("read-only executor calls = %d, want 2", executions.Load())
	}
}

func TestACPMCPBrokerRejectsAuthFenceProfileAndInactivePrompt(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	promptAllowed := true
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			expected := got.Metadata.Fence
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: expected, RuntimeProfile: profile, ControllerFence: fence,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error {
			if !promptAllowed {
				return store.ErrConflict
			}
			return nil
		}),
		Executor: ACPMCPToolExecutorFunc(func(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			return json.RawMessage(acpMCPTestOKBody), nil
		}),
		Effects: effects,
	}

	wrongBearer := performMCPBrokerCall(t, broker, request, strings.Repeat("x", 32), capability)
	if wrongBearer.Code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer status = %d", wrongBearer.Code)
	}
	wrongCapability := performMCPBrokerCall(t, broker, request, bearer, []byte(strings.Repeat("x", 32)))
	if wrongCapability.Code != http.StatusForbidden {
		t.Fatalf("wrong capability status = %d", wrongCapability.Code)
	}
	promptAllowed = false
	inactive := performMCPBrokerCall(t, broker, request, bearer, capability)
	if inactive.Code != http.StatusForbidden {
		t.Fatalf("inactive prompt status = %d", inactive.Code)
	}

	promptAllowed = true
	staleProfile := profile
	staleProfile.ToolPolicyDigest = testControllerMCPDigest("stale")
	broker.Credentials = ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
		return ACPMCPBrokerCredentials{
			ControllerBearerToken: bearer, CapabilitySecret: capability,
			ExpectedFence: got.Metadata.Fence, RuntimeProfile: staleProfile, ControllerFence: fence,
		}, nil
	})
	profileResponse := performMCPBrokerCall(t, broker, request, bearer, capability)
	if profileResponse.Code != http.StatusGone {
		t.Fatalf("stale profile status = %d", profileResponse.Code)
	}
}

func TestACPMCPBrokerRejectsDescriptorDriftFromFrozenMCPConfiguration(t *testing.T) {
	effects, fence := newMCPBrokerControlStore(t)
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	frozen := request.Authorization.Configuration()
	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	var executions atomic.Int32
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile,
				ExpectedMCPConfiguration: &frozen, ControllerFence: fence,
			}, nil
		}),
		Prompts: ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error {
			t.Fatal("descriptor drift reached prompt authorization")
			return nil
		}),
		Executor: ACPMCPToolExecutorFunc(func(context.Context, harnessv2.MCPBrokerCallRequest, harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
			executions.Add(1)
			return json.RawMessage(acpMCPTestOKBody), nil
		}),
		Effects: effects,
	}

	forged := request
	forged.Authorization.ToolPolicy.Tools = append(
		[]harnessv2.MCPToolDescriptor(nil), request.Authorization.ToolPolicy.Tools...,
	)
	forged.Authorization.ToolPolicy.Tools[0].Effect = harnessv2.MCPToolEffectReadOnly
	forged.Authorization.ToolPolicy.DescriptorDigest, _ = harnessv2.CanonicalMCPToolDescriptorDigest(
		forged.Authorization.ToolPolicy.Tools,
	)
	forged.Metadata.RequestDigest, _ = harnessv2.CanonicalRequestDigest(forged)
	if err := forged.Authorization.ValidateProfile(profile); err != nil {
		t.Fatalf("forged descriptor no longer demonstrates profile-only validation: %v", err)
	}

	response := performMCPBrokerCall(t, broker, forged, bearer, capability)
	if response.Code != http.StatusGone {
		t.Fatalf("descriptor drift status = %d body=%s", response.Code, response.Body.String())
	}
	if executions.Load() != 0 {
		t.Fatalf("descriptor drift executed %d tools, want 0", executions.Load())
	}
}

func TestRuntimePoolAuthSecretForEpochSelectsActiveInstanceSecretDuringRollover(t *testing.T) {
	secrets := []corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: "pool-auth-e1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pool-auth-e2"}},
	}
	selected, err := runtimePoolAuthSecretForEpoch(secrets, 1)
	if err != nil || selected.Name != "pool-auth-e1" {
		t.Fatalf("epoch 1 selection = %v, %v; want pool-auth-e1", selected, err)
	}
	selected, err = runtimePoolAuthSecretForEpoch(secrets, 2)
	if err != nil || selected.Name != "pool-auth-e2" {
		t.Fatalf("epoch 2 selection = %v, %v; want pool-auth-e2", selected, err)
	}
	if _, err := runtimePoolAuthSecretForEpoch(secrets, 3); err == nil {
		t.Fatal("missing epoch secret unexpectedly selected")
	}
	if _, err := runtimePoolAuthSecretForEpoch([]corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: "pool-auth-e11"}},
	}, 1); err == nil {
		t.Fatal("suffix must be anchored: auth-e11 must not satisfy epoch 1")
	}
}

func TestResolveRuntimePoolAuthSecretUsesPrivateWorkspaceBinding(t *testing.T) {
	const (
		runtimeNamespace = "runtime-system"
		testPoolName     = "pool"
		testPoolUID      = "pool-uid"
	)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceDefault, Name: testPoolName, UID: testPoolUID},
		Spec: corev1alpha1.RuntimePoolSpec{
			RuntimeNamespace: runtimeNamespace,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
			},
		},
	}
	secretName := runtimePoolChildName(
		runtimePoolResourceName(pool.Namespace, pool.Name),
		"auth-e1-"+strings.Repeat("a", 24),
	)
	boundUID := types.UID("bound-secret-uid")
	pool.Annotations = map[string]string{
		runtimePoolPrivateAuthSecretBindingAnnotation(1): secretName + "/" + string(boundUID),
	}
	immutable := true
	labels := map[string]string{
		runtimePoolManagedByLabel:       runtimePoolManagedByLabelValue,
		runtimePoolApplicationLabel:     runtimePoolApplicationLabelValue,
		runtimePoolKeyLabel:             runtimePoolKey(pool.Namespace, pool.Name),
		runtimePoolNameLabel:            pool.Name,
		runtimePoolNamespaceLabel:       pool.Namespace,
		runtimePoolUIDLabel:             string(pool.UID),
		runtimePoolNetworkRoleLabel:     "provider-client",
		runtimePoolAuthLabel:            booleanTrueValue,
		runtimePoolCredentialEpochLabel: "1",
	}
	bound := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: runtimeNamespace, Name: secretName, UID: boundUID, Labels: labels},
		Immutable:  &immutable,
		Data: map[string][]byte{
			runtimePoolControllerTokenKey:      []byte(strings.Repeat("t", 32)),
			runtimePoolCapabilitySecretKey:     []byte(strings.Repeat("c", 32)),
			runtimePoolBootstrapNonceKey:       []byte(strings.Repeat("n", 32)),
			runtimePoolBootstrapSigningSeedKey: []byte(strings.Repeat("s", 32)),
		},
	}
	decoy := bound.DeepCopy()
	decoy.Name = runtimePoolChildName(
		runtimePoolResourceName(pool.Namespace, pool.Name),
		"auth-e1-"+strings.Repeat("b", 24),
	)
	decoy.UID = "decoy-secret-uid"
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bound, decoy).Build()
	selected, err := resolveRuntimePoolAuthSecret(context.Background(), reader, pool, runtimeNamespace, 1)
	if err != nil || selected.Name != bound.Name || selected.UID != boundUID {
		t.Fatalf("private auth Secret = %s/%s, error=%v; want bound %s/%s", selected.Name, selected.UID, err, bound.Name, boundUID)
	}

	replacement := bound.DeepCopy()
	replacement.UID = "replacement-secret-uid"
	replacementReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(replacement).Build()
	if _, err := resolveRuntimePoolAuthSecret(context.Background(), replacementReader, pool, runtimeNamespace, 1); err == nil ||
		!strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("recreated private auth Secret error = %v, want immutable UID rejection", err)
	}
}

func TestKubernetesACPMCPBrokerCredentialResolverChecksTaskSessionGeneration(t *testing.T) {
	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: request.Namespace, UID: "pool-uid", Generation: 2},
		Spec: corev1alpha1.RuntimePoolSpec{
			RuntimeNamespace: "runtime-system",
			Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Profile: RuntimePoolProfileFromPlan(ACPRuntimePlan{Profile: profile, Digest: request.Metadata.Fence.RuntimeProfileDigest}),
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
			PodNamespace: "runtime-system", PodName: "runtime-pod", PodAddress: "10.0.0.2", PodUID: "pod-uid",
			BootID: string(request.Metadata.Fence.SupervisorBootID), RuntimeInstanceID: string(request.Metadata.Fence.RuntimeInstanceID),
			ControllerEpoch: 1, ProfileDigest: string(request.Metadata.Fence.RuntimeProfileDigest),
			ProfileDigestSchemaVersion: strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10), ProtocolVersion: corev1alpha1.RuntimePoolProtocolHarnessV2,
		}},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: acpDispatcherTestTaskName, Namespace: request.Namespace, UID: acpDispatcherTaskUID},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: "prompt-1",
			RuntimePoolName: pool.Name, RuntimePoolUID: string(pool.UID),
			RuntimeInstanceID:        string(request.Metadata.Fence.RuntimeInstanceID),
			RuntimeSessionUID:        string(request.Authorization.RuntimeSessionUID),
			RuntimeSessionGeneration: int64(request.Authorization.SessionGeneration), ControllerEpoch: 1,
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-auth-e1", Namespace: "runtime-system", Labels: map[string]string{
			runtimePoolAuthLabel: "true", runtimePoolUIDLabel: string(pool.UID),
		}},
		Data: map[string][]byte{
			runtimePoolControllerTokenKey:  []byte(strings.Repeat("b", 32)),
			runtimePoolCapabilitySecretKey: []byte(strings.Repeat("c", 32)),
		},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, task, secret).Build()
	epochs := NewControllerEpochManager(nil, "controller-a")
	epochs.current = &store.ControllerEpoch{Name: store.DefaultControllerEpochName, Epoch: 1, HolderID: "controller-a"}
	close(epochs.ready)
	resolver := KubernetesACPMCPBrokerCredentialResolver{Reader: reader}

	// Header pre-authentication accepts the pool bearer and rejects a wrong
	// bearer or missing pool identity before any body is read.
	if err := resolver.PreAuthenticateACPMCPBroker(context.Background(), request.Namespace, string(pool.UID), "Bearer "+strings.Repeat("b", 32)); err != nil {
		t.Fatalf("valid pre-auth error = %v", err)
	}
	if err := resolver.PreAuthenticateACPMCPBroker(context.Background(), request.Namespace, string(pool.UID), "Bearer "+strings.Repeat("z", 32)); err == nil {
		t.Fatal("wrong-bearer pre-auth unexpectedly accepted")
	}
	if err := resolver.PreAuthenticateACPMCPBroker(context.Background(), request.Namespace, "", "Bearer "+strings.Repeat("b", 32)); err == nil {
		t.Fatal("missing pool identity pre-auth unexpectedly accepted")
	}
	resolver.Epochs = epochs

	credentials, err := resolver.ResolveACPMCPBrokerCredentials(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.ExpectedFence.RuntimeSessionGeneration != request.Authorization.SessionGeneration ||
		credentials.ExpectedFence.RuntimeSessionUID != request.Authorization.RuntimeSessionUID {
		t.Fatalf("resolved fence = %#v", credentials.ExpectedFence)
	}
	if credentials.Task.Name != task.Name || credentials.Task.Namespace != task.Namespace ||
		credentials.Task.UID != string(task.UID) {
		t.Fatalf("resolved task identity = %#v", credentials.Task)
	}

	task.Status.Execution.RuntimeSessionGeneration++
	reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, task, secret).Build()
	resolver.Reader = reader
	if _, err := resolver.ResolveACPMCPBrokerCredentials(context.Background(), request); err == nil {
		t.Fatal("mismatched Task RuntimeSession generation remained authorized")
	}
}

func TestKubernetesACPMCPBrokerCredentialResolverSupportsExternalRuntime(t *testing.T) {
	fixture, task, request, resolver := newSnapshotBackedExternalMCPResolver(t)
	bearer := strings.Repeat("t", 32)
	if err := resolver.PreAuthenticateACPMCPBroker(
		fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+bearer,
	); err != nil {
		t.Fatalf("valid external pre-auth error = %v", err)
	}
	if err := resolver.PreAuthenticateACPMCPBroker(
		fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+strings.Repeat("z", 32),
	); err == nil {
		t.Fatal("wrong external bearer unexpectedly passed pre-authentication")
	}
	credentials, err := resolver.ResolveACPMCPBrokerCredentials(fixture.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.ExpectedFence != request.Metadata.Fence || credentials.RuntimeProfile.Model != acpTestModel {
		t.Fatalf("external credentials = %#v", credentials)
	}
	if credentials.ExpectedMCPConfiguration == nil ||
		credentials.ExpectedMCPConfiguration.ToolPolicy.DescriptorDigest != fixture.runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest {
		t.Fatalf("external frozen MCP configuration = %#v", credentials.ExpectedMCPConfiguration)
	}
	if credentials.Task.Name != task.Name || credentials.Task.Namespace != task.Namespace ||
		credentials.Task.UID != string(task.UID) {
		t.Fatalf("resolved external task identity = %#v", credentials.Task)
	}

	runtimeObject := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
		t.Fatal(err)
	}
	runtimeObject.Status.Ready = false
	if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
		t.Fatal(err)
	}
	if err := resolver.PreAuthenticateACPMCPBroker(
		fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+bearer,
	); err != nil {
		t.Fatalf("draining external pre-auth error = %v", err)
	}
	if _, err := resolver.ResolveACPMCPBrokerCredentials(fixture.ctx, request); err != nil {
		t.Fatalf("draining external credential resolution error = %v", err)
	}
}

func TestKubernetesACPMCPBrokerCredentialResolverRequiresExternalTaskSupervisorBoot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *externalACPDispatchFixture, *corev1alpha1.Task, *harnessv2.MCPBrokerCallRequest)
	}{
		{
			name: "missing Task boot",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture, task *corev1alpha1.Task, _ *harnessv2.MCPBrokerCallRequest) {
				t.Helper()
				task.Status.Execution.RuntimeSessionSupervisorBootID = ""
				if err := fixture.client.Status().Update(fixture.ctx, task); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "restarted supervisor",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture, _ *corev1alpha1.Task, request *harnessv2.MCPBrokerCallRequest) {
				t.Helper()
				runtimeObject := &corev1alpha1.AgentRuntime{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
					t.Fatal(err)
				}
				runtimeObject.Status.ObservedCapabilities.SupervisorBootID = "replacement-boot"
				if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
					t.Fatal(err)
				}
				request.Metadata.Fence.SupervisorBootID = "replacement-boot"
				requestDigest, err := harnessv2.CanonicalRequestDigest(*request)
				if err != nil {
					t.Fatal(err)
				}
				request.Metadata.RequestDigest = requestDigest
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, task, request, resolver := newSnapshotBackedExternalMCPResolver(t)
			test.mutate(t, fixture, task, &request)
			if err := resolver.PreAuthenticateACPMCPBroker(
				fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+strings.Repeat("t", 32),
			); err != nil {
				t.Fatalf("external pre-auth error = %v", err)
			}
			if _, err := resolver.ResolveACPMCPBrokerCredentials(fixture.ctx, request); err == nil ||
				!strings.Contains(err.Error(), "supervisor boot does not match") {
				t.Fatalf("external supervisor boot resolution error = %v", err)
			}
		})
	}
}

func TestKubernetesACPMCPBrokerCredentialResolverRejectsAmbiguousRuntimeIdentity(t *testing.T) {
	fixture, _, request, resolver := newSnapshotBackedExternalMCPResolver(t)
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "colliding-pool",
			Namespace: request.Namespace,
			UID:       types.UID(request.Metadata.Fence.RuntimePoolUID),
		},
	}
	if err := fixture.client.Create(fixture.ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, bearer := range []string{strings.Repeat("t", 32), strings.Repeat("b", 32)} {
		err := resolver.PreAuthenticateACPMCPBroker(
			fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+bearer,
		)
		if err == nil || !strings.Contains(err.Error(), "missing or ambiguous") {
			t.Fatalf("ambiguous identity pre-auth error = %v", err)
		}
	}
}

func TestKubernetesACPMCPBrokerCredentialResolverRejectsReconformedExternalAuthorityDrift(t *testing.T) {
	t.Run("runtime generation", func(t *testing.T) {
		fixture, _, request, resolver := newSnapshotBackedExternalMCPResolver(t)
		runtimeObject := &corev1alpha1.AgentRuntime{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
			t.Fatal(err)
		}
		runtimeObject.Spec.Deployment.Endpoint += "/reconfigured"
		runtimeObject.Generation++
		if err := fixture.client.Update(fixture.ctx, runtimeObject); err != nil {
			t.Fatal(err)
		}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
			t.Fatal(err)
		}
		runtimeObject.Status.ObservedGeneration = runtimeObject.Generation
		if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
			t.Fatal(err)
		}
		if err := resolver.PreAuthenticateACPMCPBroker(
			fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+strings.Repeat("t", 32),
		); err != nil {
			t.Fatalf("reconformed runtime pre-auth error = %v", err)
		}
		if _, err := resolver.ResolveACPMCPBrokerCredentials(fixture.ctx, request); err == nil ||
			!strings.Contains(err.Error(), "identity or generation changed after binding") {
			t.Fatalf("reconformed runtime resolution error = %v", err)
		}
	})

	t.Run("auth Secret rotation", func(t *testing.T) {
		fixture, _, request, resolver := newSnapshotBackedExternalMCPResolver(t)
		runtimeObject := &corev1alpha1.AgentRuntime{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
			t.Fatal(err)
		}
		controllerRef := *runtimeObject.Spec.ClientAuth.ControllerBearerTokenSecretRef
		capabilityRef := *runtimeObject.Spec.ClientAuth.OperationCapabilitySecretRef
		secret := &corev1.Secret{}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKey{Namespace: runtimeObject.Namespace, Name: controllerRef.Name}, secret); err != nil {
			t.Fatal(err)
		}
		secret.Data[controllerRef.Key] = []byte(strings.Repeat("n", 32))
		secret.Data[capabilityRef.Key] = []byte(strings.Repeat("q", 32))
		if err := fixture.client.Update(fixture.ctx, secret); err != nil {
			t.Fatal(err)
		}
		if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(secret), secret); err != nil {
			t.Fatal(err)
		}
		runtimeObject.Status.ObservedControllerAuthRefResourceVersion = secret.ResourceVersion
		runtimeObject.Status.ObservedOperationCapabilityRefResourceVersion = secret.ResourceVersion
		if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
			t.Fatal(err)
		}
		if err := resolver.PreAuthenticateACPMCPBroker(
			fixture.ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID), "Bearer "+strings.Repeat("n", 32),
		); err != nil {
			t.Fatalf("rotated Secret pre-auth error = %v", err)
		}
		if _, err := resolver.ResolveACPMCPBrokerCredentials(fixture.ctx, request); err == nil ||
			!strings.Contains(err.Error(), "authentication authority changed after binding") {
			t.Fatalf("rotated Secret resolution error = %v", err)
		}
	})
}

func newSnapshotBackedExternalMCPResolver(
	t *testing.T,
) (*externalACPDispatchFixture, *corev1alpha1.Task, harnessv2.MCPBrokerCallRequest, KubernetesACPMCPBrokerCredentialResolver) {
	t.Helper()
	fixture := newExternalACPDispatchFixture(t)
	task := fixture.queueTask(t, "external-mcp", types.UID("external-mcp-task-uid"), "broker a tool", nil)
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	observed := fixture.runtime.Status.ObservedCapabilities
	current.Status.Execution.State = corev1alpha1.TaskExecutionStateRunning
	current.Status.Execution.RuntimeInstanceID = observed.RuntimeInstanceID
	current.Status.Execution.RuntimeSessionUID = "external-mcp-session-uid"
	current.Status.Execution.RuntimeSessionGeneration = 3
	current.Status.Execution.RuntimeSessionSupervisorBootID = observed.SupervisorBootID
	current.Status.Execution.ControllerEpoch = observed.ControllerEpoch
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	request, _ := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	request.Namespace = current.Namespace
	request.Metadata.TaskUID = harnessv2.TaskUID(current.UID)
	request.Metadata.TaskAttempt = uint32(current.Status.Execution.Attempt)
	request.Metadata.PromptID = harnessv2.PromptID(current.Status.Execution.PromptID)
	request.Metadata.Fence = harnessv2.Fence{
		RuntimeInstanceID:          harnessv2.RuntimeInstanceID(observed.RuntimeInstanceID),
		SupervisorBootID:           harnessv2.SupervisorBootID(observed.SupervisorBootID),
		ControllerEpoch:            uint64(observed.ControllerEpoch),
		RuntimePoolUID:             harnessv2.RuntimePoolUID(observed.RuntimePoolUID),
		RuntimePoolGeneration:      uint64(observed.RuntimePoolGeneration),
		RuntimeSessionUID:          harnessv2.RuntimeSessionUID(current.Status.Execution.RuntimeSessionUID),
		RuntimeSessionGeneration:   uint64(current.Status.Execution.RuntimeSessionGeneration),
		RuntimeProfileDigest:       harnessv2.ProfileDigest(current.Status.AgentExecutionBinding.RuntimeProfileDigest),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	request.Authorization.RuntimeSessionUID = request.Metadata.Fence.RuntimeSessionUID
	request.Authorization.SessionGeneration = request.Metadata.Fence.RuntimeSessionGeneration
	request.Authorization.TaskUID = request.Metadata.TaskUID
	request.Authorization.TaskAttempt = request.Metadata.TaskAttempt
	request.Authorization.PromptID = request.Metadata.PromptID
	request.Metadata.RequestDigest, _ = harnessv2.CanonicalRequestDigest(request)
	resolver := KubernetesACPMCPBrokerCredentialResolver{
		Reader: fixture.client, Epochs: fixture.epochs, AgentExecutionSnapshots: fixture.persistence,
	}
	return fixture, current, request, resolver
}

func TestDurableACPMCPPromptAuthorizerRequiresActiveExactAttempt(t *testing.T) {
	request, _ := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	attempt := &store.PromptAttempt{
		Key: store.PromptAttemptKey{
			Namespace: request.Namespace, TaskUID: string(request.Metadata.TaskUID),
			Attempt: int64(request.Metadata.TaskAttempt), PromptID: string(request.Metadata.PromptID),
		},
		SessionUID: string(request.Authorization.RuntimeSessionUID), RuntimeInstanceID: string(request.Metadata.Fence.RuntimeInstanceID),
		ControllerEpoch: int64(request.Metadata.Fence.ControllerEpoch), ExecutionState: store.PromptExecutionRunning,
	}
	attempt.ID, _ = attempt.Key.CanonicalID()
	authorizer := DurableACPMCPPromptAuthorizer{Attempts: staticPromptAttemptStore{attempt: attempt}}
	if err := authorizer.AuthorizeACPMCPPrompt(context.Background(), request); err != nil {
		t.Fatalf("AuthorizeACPMCPPrompt() error = %v", err)
	}
	settling := *attempt
	settling.ExecutionState = store.PromptExecutionSettling
	authorizer.Attempts = staticPromptAttemptStore{attempt: &settling}
	if err := authorizer.AuthorizeACPMCPPrompt(context.Background(), request); err == nil {
		t.Fatal("settling prompt remained authorized")
	}
	wrongSession := *attempt
	wrongSession.SessionUID = "other-session"
	authorizer.Attempts = staticPromptAttemptStore{attempt: &wrongSession}
	if err := authorizer.AuthorizeACPMCPPrompt(context.Background(), request); err == nil {
		t.Fatal("mismatched RuntimeSession remained authorized")
	}
}

func TestDurableACPMCPPromptAuthorizerRequiresAcceptanceForConsequentialCalls(t *testing.T) {
	tests := []struct {
		name    string
		effect  harnessv2.MCPToolEffect
		state   store.PromptExecutionState
		allowed bool
	}{
		{name: "read-only submitting", effect: harnessv2.MCPToolEffectReadOnly, state: store.PromptExecutionSubmitting, allowed: true},
		{name: "consequential submitting", effect: harnessv2.MCPToolEffectConsequential, state: store.PromptExecutionSubmitting},
		{name: "read-only accepted", effect: harnessv2.MCPToolEffectReadOnly, state: store.PromptExecutionAccepted, allowed: true},
		{name: "consequential accepted", effect: harnessv2.MCPToolEffectConsequential, state: store.PromptExecutionAccepted, allowed: true},
		{name: "read-only running", effect: harnessv2.MCPToolEffectReadOnly, state: store.PromptExecutionRunning, allowed: true},
		{name: "consequential running", effect: harnessv2.MCPToolEffectConsequential, state: store.PromptExecutionRunning, allowed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, _ := testMCPBrokerRequest(t, test.effect)
			attempt := &store.PromptAttempt{
				Key: store.PromptAttemptKey{
					Namespace: request.Namespace, TaskUID: string(request.Metadata.TaskUID),
					Attempt: int64(request.Metadata.TaskAttempt), PromptID: string(request.Metadata.PromptID),
				},
				SessionUID: string(request.Authorization.RuntimeSessionUID), RuntimeInstanceID: string(request.Metadata.Fence.RuntimeInstanceID),
				ControllerEpoch: int64(request.Metadata.Fence.ControllerEpoch), ExecutionState: test.state,
			}
			attempt.ID, _ = attempt.Key.CanonicalID()
			authorizer := DurableACPMCPPromptAuthorizer{Attempts: staticPromptAttemptStore{attempt: attempt}}
			err := authorizer.AuthorizeACPMCPPrompt(context.Background(), request)
			if test.allowed && err != nil {
				t.Fatalf("AuthorizeACPMCPPrompt() error = %v", err)
			}
			if !test.allowed && err == nil {
				t.Fatal("AuthorizeACPMCPPrompt() allowed a consequential call before prompt acceptance")
			}
		})
	}
}

func TestDurableACPMCPPromptAuthorizerRejectsApprovalRequiredCalls(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	request, _ := testMCPBrokerRequest(t, harnessv2.MCPToolEffectConsequential)
	request.Authorization.ApprovalPolicy = harnessv2.MCPApprovalPolicy{RequiredTools: []string{request.Call.ToolName}}
	request.Call.Approval = &harnessv2.MCPApprovalEvidence{
		PermissionRequestID: "permission-1",
		ToolCallID:          request.Call.CallID,
		ToolName:            request.Call.ToolName,
		GrantedAt:           now.Add(-time.Minute),
		ExpiresAt:           now.Add(time.Minute),
	}
	attempt := &store.PromptAttempt{
		Key: store.PromptAttemptKey{
			Namespace: request.Namespace, TaskUID: string(request.Metadata.TaskUID),
			Attempt: int64(request.Metadata.TaskAttempt), PromptID: string(request.Metadata.PromptID),
		},
		SessionUID: string(request.Authorization.RuntimeSessionUID), RuntimeInstanceID: string(request.Metadata.Fence.RuntimeInstanceID),
		ControllerEpoch: int64(request.Metadata.Fence.ControllerEpoch), ExecutionState: store.PromptExecutionRunning,
	}
	attempt.ID, _ = attempt.Key.CanonicalID()
	authorizer := DurableACPMCPPromptAuthorizer{Attempts: staticPromptAttemptStore{attempt: attempt}}
	err := authorizer.AuthorizeACPMCPPrompt(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "controller-owned permission review") {
		t.Fatalf("approval-required call error = %v", err)
	}
}

type staticPromptAttemptStore struct {
	attempt *store.PromptAttempt
}

func (s staticPromptAttemptStore) CreatePromptAttempt(context.Context, *store.PromptAttempt, store.ControllerEpochFence) (*store.PromptAttempt, error) {
	return nil, store.ErrConflict
}
func (s staticPromptAttemptStore) GetPromptAttempt(context.Context, string) (*store.PromptAttempt, error) {
	if s.attempt == nil {
		return nil, store.ErrNotFound
	}
	copy := *s.attempt
	return &copy, nil
}
func (s staticPromptAttemptStore) TransitionPromptAttemptExecution(context.Context, store.PromptAttemptExecutionTransition) (*store.PromptAttempt, error) {
	return nil, store.ErrConflict
}
func (s staticPromptAttemptStore) RecoverPromptAttemptPreSubmission(context.Context, store.PromptAttemptPreSubmissionRecovery) (*store.PromptAttempt, error) {
	return nil, store.ErrConflict
}
func (s staticPromptAttemptStore) TransitionPromptAttemptDelivery(context.Context, store.PromptAttemptDeliveryTransition) (*store.PromptAttempt, error) {
	return nil, store.ErrConflict
}

func testMCPBrokerRequest(t *testing.T, effect harnessv2.MCPToolEffect) (harnessv2.MCPBrokerCallRequest, harnessv2.RuntimeProfile) {
	t.Helper()
	now := time.Now().UTC()
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot", ControllerEpoch: 1,
		RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 2,
		RuntimeSessionUID: "session-uid", RuntimeSessionGeneration: 3,
		RuntimeProfileDigest:       harnessv2.ProfileDigest(testControllerMCPDigest("profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	descriptor := harnessv2.MCPToolDescriptor{
		Name: acpMCPTestToolName, Description: "test broker tool", InputSchema: json.RawMessage(`{"type":"object"}`),
		Source: harnessv2.MCPToolSourceBrokeredBuiltin, Effect: effect,
	}
	descriptorDigest, err := harnessv2.CanonicalMCPToolDescriptorDigest([]harnessv2.MCPToolDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	toolPolicy := harnessv2.MCPToolPolicy{
		AllowedToolNames: []string{acpMCPTestToolName}, AllowBash: true,
		Tools: []harnessv2.MCPToolDescriptor{descriptor}, DescriptorDigest: descriptorDigest,
	}
	approvalPolicy := harnessv2.MCPApprovalPolicy{}
	toolDigest, _ := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	approvalDigest, _ := harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	mcpDigest, _ := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	profile := harnessv2.RuntimeProfile{
		ACPProfile: harnessv2.ACPProfileV1, AdapterDigests: map[string]string{"adapter": testControllerMCPDigest("adapter")},
		ProviderKind: "codex", Model: "model", AgentConfigurationDigest: testControllerMCPDigest("agent"),
		ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest,
		MCPConfigurationDigest: mcpDigest, WorkspaceIntent: harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole: "provider", ProxyCredentialScope: "model:model", ResourceClass: "standard",
	}
	lease := harnessv2.PromptLease{Generation: 4, IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(2 * time.Minute)}
	metadata := harnessv2.MutationMetadata{
		Fence: fence, TaskUID: acpDispatcherTaskUID, TaskAttempt: 1, PromptID: "prompt-1", OperationID: "mcp-operation-1",
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion, ExpiresAt: now.Add(30 * time.Second),
	}
	authorization := harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
		TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		LeaseGeneration: lease.Generation, ToolPolicyDigest: profile.ToolPolicyDigest,
		ApprovalPolicyDigest: profile.ApprovalPolicyDigest, MCPConfigurationDigest: profile.MCPConfigurationDigest,
		ToolPolicy: toolPolicy, ApprovalPolicy: approvalPolicy, ExpiresAt: now.Add(time.Minute),
	}
	request := harnessv2.MCPBrokerCallRequest{
		Protocol: harnessv2.ProtocolVersion, Namespace: "default", SessionState: harnessv2.RuntimeSessionStatePromptRunning,
		Metadata: metadata, Lease: lease, Authorization: authorization,
		Call: harnessv2.MCPToolCall{CallID: acpDispatcherToolCallID, ToolName: acpMCPTestToolName, Arguments: json.RawMessage(`{"value":"x"}`)},
	}
	digest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata.RequestDigest = digest
	return request, profile
}

func performMCPBrokerCall(
	t *testing.T,
	broker http.Handler,
	request harnessv2.MCPBrokerCallRequest,
	bearer string,
	capabilitySecret []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := harnessv2.SignOperationCapability(capabilitySecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, harnessv2.MCPBrokerCallPath, bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+bearer)
	httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, httpRequest)
	return response
}

func newMCPBrokerControlStore(t *testing.T) (*storesqlite.Store, store.ControllerEpochFence) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	db, err := storesqlite.NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	control := storesqlite.NewStore(db, path)
	epoch, err := control.CompareAndSwapControllerEpoch(context.Background(), store.ControllerEpochCAS{
		ExpectedVersion: 0, ExpectedEpoch: 0, NewEpoch: 1, HolderID: "controller-a",
		RequestDigest: testControllerMCPDigest("epoch"), UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return control, store.ControllerEpochFence{Name: epoch.Name, Epoch: epoch.Epoch, HolderID: epoch.HolderID}
}

func testControllerMCPDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
