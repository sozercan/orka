package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/tools"
)

type externalACPDispatchFixture struct {
	ctx            context.Context
	client         client.Client
	controlStore   store.DurableControlStore
	persistence    *sqlite.Store
	epochs         *ControllerEpochManager
	reconciler     *TaskReconciler
	dispatcher     *ACPDispatcher
	agent          *corev1alpha1.Agent
	runtime        *corev1alpha1.AgentRuntime
	createCalls    *atomic.Int32
	createRequests chan harnessv2.CreateRuntimeSessionRequest
	deleteCalls    *atomic.Int32
	deleteRequests chan harnessv2.DeleteRuntimeSessionRequest
	mcpPolicy      corev1alpha1.AgentRuntimeMCPPolicySpec
}

type externalACPDispatchFixtureOptions struct {
	statusTransform                 func(*harnessv2.StatusResponse)
	profileTransform                func(*harnessv2.RuntimeProfile)
	terminalEvents                  map[harnessv2.PromptID]harnessv2.EventType
	promptObserver                  func(harnessv2.StartPromptRequest)
	workspaceDeltaObserver          func(harnessv2.CreateWorkspaceDeltaRequest)
	supportsPublicationFinalization bool
}

type failAgentRuntimeReadWhileTaskSubmitting struct {
	client.Reader
	taskKey  client.ObjectKey
	failures atomic.Int32
}

type failAgentRuntimeReadWhileTaskPlanned struct {
	client.Reader
	taskKey  client.ObjectKey
	failures atomic.Int32
}

type failAgentRuntimeReadWhileTaskSettling struct {
	client.Reader
	taskKey  client.ObjectKey
	failures atomic.Int32
}

func (r *failAgentRuntimeReadWhileTaskSubmitting) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, ok := object.(*corev1alpha1.AgentRuntime); ok && r.failures.Load() == 0 {
		task := &corev1alpha1.Task{}
		if err := r.Reader.Get(ctx, r.taskKey, task); err != nil {
			return err
		}
		if task.Status.Execution != nil && task.Status.Execution.State == corev1alpha1.TaskExecutionStateSubmitting &&
			r.failures.CompareAndSwap(0, 1) {
			return apierrors.NewServiceUnavailable("transient AgentRuntime read failure")
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *failAgentRuntimeReadWhileTaskPlanned) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, ok := object.(*corev1alpha1.AgentRuntime); ok && r.failures.Load() == 0 {
		task := &corev1alpha1.Task{}
		if err := r.Reader.Get(ctx, r.taskKey, task); err != nil {
			return err
		}
		if task.Status.Execution != nil && task.Status.Execution.State == corev1alpha1.TaskExecutionStatePlanned &&
			r.failures.CompareAndSwap(0, 1) {
			return apierrors.NewServiceUnavailable("transient AgentRuntime read failure")
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *failAgentRuntimeReadWhileTaskSettling) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, ok := object.(*corev1alpha1.AgentRuntime); ok && r.failures.Load() == 0 {
		task := &corev1alpha1.Task{}
		if err := r.Reader.Get(ctx, r.taskKey, task); err != nil {
			return err
		}
		if task.Status.Execution != nil && task.Status.Execution.State == corev1alpha1.TaskExecutionStateSettling &&
			r.failures.CompareAndSwap(0, 1) {
			return apierrors.NewServiceUnavailable("transient AgentRuntime read failure")
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func newExternalACPDispatchFixture(t *testing.T) *externalACPDispatchFixture {
	t.Helper()
	return newExternalACPDispatchFixtureWithPolicy(t, "external-v2", testAgentRuntimeMCPPolicy())
}

func newExternalACPDispatchFixtureWithRuntimeName(t *testing.T, runtimeName string) *externalACPDispatchFixture {
	t.Helper()
	return newExternalACPDispatchFixtureWithPolicy(t, runtimeName, testAgentRuntimeMCPPolicy())
}

func newExternalACPDispatchFixtureWithPolicy(
	t *testing.T,
	runtimeName string,
	policy corev1alpha1.AgentRuntimeMCPPolicySpec,
	extraObjects ...client.Object,
) *externalACPDispatchFixture {
	t.Helper()
	return newExternalACPDispatchFixtureWithOptions(
		t, runtimeName, policy, externalACPDispatchFixtureOptions{}, extraObjects...,
	)
}

func newExternalACPDispatchFixtureWithOptions(
	t *testing.T,
	runtimeName string,
	policy corev1alpha1.AgentRuntimeMCPPolicySpec,
	options externalACPDispatchFixtureOptions,
	extraObjects ...client.Object,
) *externalACPDispatchFixture {
	t.Helper()
	allowAgentRuntimeLoopback(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	profile, governance, limits := testAgentRuntimeProfileClaimsAndLimits()
	if options.profileTransform != nil {
		options.profileTransform(&profile)
	}
	toolPolicyDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(policy.AllowedTools, policy.DisallowedTools, policy.AllowBash)
	if err != nil {
		t.Fatal(err)
	}
	profile.ToolPolicyDigest = toolPolicyDigest
	approvalPolicyDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(agentRuntimeMCPApprovalPolicy(&policy))
	if err != nil {
		t.Fatal(err)
	}
	profile.ApprovalPolicyDigest = approvalPolicyDigest
	mcpConfigurationDigest, err := harnessv2.CanonicalMCPConfigurationDigest(policy.AllowedTools)
	if err != nil {
		t.Fatal(err)
	}
	profile.MCPConfigurationDigest = mcpConfigurationDigest
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	createCalls := &atomic.Int32{}
	createRequests := make(chan harnessv2.CreateRuntimeSessionRequest, 8)
	deleteCalls := &atomic.Int32{}
	deleteRequests := make(chan harnessv2.DeleteRuntimeSessionRequest, 8)
	server := newDispatcherRuntimeServerForPoolWithOptions(
		t, profile, profileDigest, acpDispatcherTestPoolUID, dispatcherRuntimeServerOptions{
			disableAgentSessionConfiguration: true,
			disablePermissions:               true,
			terminalEvents:                   options.terminalEvents,
			onDelete: func(request harnessv2.DeleteRuntimeSessionRequest) {
				deleteCalls.Add(1)
				deleteRequests <- request
			},
		},
		func(request harnessv2.CreateRuntimeSessionRequest) {
			createCalls.Add(1)
			createRequests <- request
		},
	)
	t.Cleanup(server.Close)
	runtimeEndpoint := server.URL
	if options.statusTransform != nil {
		statusProxy := newExternalRuntimeStatusProxy(t, server.URL, options.statusTransform)
		runtimeEndpoint = statusProxy.URL
	}
	if options.supportsPublicationFinalization {
		capabilitiesProxy := newExternalRuntimeCapabilitiesProxy(t, runtimeEndpoint, func(capabilities *harnessv2.CapabilitiesResponse) {
			capabilities.SupportsPublicationFinalization = true
		})
		runtimeEndpoint = capabilitiesProxy.URL
	}
	if options.promptObserver != nil {
		promptProxy := newExternalRuntimePromptProxy(t, runtimeEndpoint, options.promptObserver)
		runtimeEndpoint = promptProxy.URL
	}
	if options.workspaceDeltaObserver != nil {
		deltaProxy := newExternalRuntimeWorkspaceDeltaProxy(t, runtimeEndpoint, options.workspaceDeltaObserver)
		runtimeEndpoint = deltaProxy.URL
	}

	config := conformancetest.Config{
		ControllerBearerToken:           strings.Repeat("t", 32),
		OperationCapabilitySecret:       []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:               "pod-uid.boot-id",
		SupervisorBootID:                "boot-id",
		RuntimePoolUID:                  "pool-uid",
		Profile:                         profile,
		Limits:                          limits,
		SupportsDrain:                   true,
		SupportsPublicationFinalization: options.supportsPublicationFinalization,
		WorkspaceGovernance:             governance,
	}
	externalRuntime, authSecret := testAgentRuntimeAndSecret(t, runtimeEndpoint, config)
	externalRuntime.Spec.Capabilities.MCPPolicy = &policy
	externalRuntime.Name = runtimeName
	externalRuntime.UID = types.UID("external-runtime-uid")
	authSecret.UID = types.UID("external-runtime-auth-uid")
	authSecret.ResourceVersion = "1"
	if len(k8svalidation.IsValidLabelValue(externalRuntime.Name)) == 0 {
		authSecret.Labels[agentRuntimeAuthRefNameLabel] = externalRuntime.Name
	} else {
		delete(authSecret.Labels, agentRuntimeAuthRefNameLabel)
	}
	limitsSpec := *externalRuntime.Spec.Capabilities.Limits
	governanceSpec := *externalRuntime.Spec.Capabilities.WorkspaceGovernance
	profileSpec := externalRuntime.Spec.Capabilities.Profile
	externalRuntime.Status = corev1alpha1.AgentRuntimeStatus{
		Ready:                                    true,
		ObservedGeneration:                       externalRuntime.Generation,
		ObservedControllerAuthRefResourceVersion: authSecret.ResourceVersion,
		ObservedOperationCapabilityRefResourceVersion: authSecret.ResourceVersion,
		ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
			ProtocolVersion:                 harnessv2.ProtocolVersion,
			Transport:                       "http+ndjson",
			ACPVersion:                      harnessv2.ACPProfileV1,
			RuntimeInstanceID:               string(config.RuntimeInstanceID),
			SupervisorBootID:                string(config.SupervisorBootID),
			ControllerEpoch:                 1,
			RuntimePoolUID:                  string(config.RuntimePoolUID),
			RuntimePoolGeneration:           1,
			RuntimeProfileDigest:            profileSpec.Digest,
			ProfileDigestSchemaVersion:      profileSpec.DigestSchemaVersion,
			AdapterName:                     profileSpec.AdapterName,
			AdapterDigest:                   profileSpec.AdapterDigest,
			ProviderKind:                    profileSpec.ProviderKind,
			Model:                           profileSpec.Model,
			Limits:                          &limitsSpec,
			SupportsDrain:                   true,
			SupportsPublicationFinalization: options.supportsPublicationFinalization,
			WorkspaceGovernance:             &governanceSpec,
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNS, Name: "external-agent", UID: types.UID("external-agent-uid"), Generation: 1,
		},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: externalRuntime.Name},
		}},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultNS, UID: types.UID("default-namespace-uid")}}
	scheme := newTestScheme()
	objects := make([]client.Object, 0, 4+len(extraObjects))
	objects = append(objects, agent, externalRuntime, authSecret, namespace)
	objects = append(objects, extraObjects...)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&corev1alpha1.Task{}, &corev1alpha1.AgentRuntime{}, &corev1alpha1.ControllerEpoch{},
			&corev1alpha1.PromptAttempt{}, &corev1alpha1.RuntimeSessionControl{},
			&corev1alpha1.BranchClaim{}, &corev1alpha1.Publication{}, &corev1alpha1.ExternalEffect{},
		).
		WithObjects(objects...).Build()
	mcpConfiguration, err := buildAgentRuntimeMCPConfigurationWithRegistry(
		ctx, kubeClient, externalRuntime, profile, tools.DefaultRegistry,
	)
	if err != nil {
		t.Fatal(err)
	}
	storedRuntime := &corev1alpha1.AgentRuntime{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(externalRuntime), storedRuntime); err != nil {
		t.Fatal(err)
	}
	storedRuntime.Status.ObservedCapabilities.MCPToolDescriptorDigest = mcpConfiguration.ToolPolicy.DescriptorDigest
	if err := kubeClient.Status().Update(ctx, storedRuntime); err != nil {
		t.Fatal(err)
	}
	externalRuntime = storedRuntime.DeepCopy()
	kubeClient = withControllerEpochLeaseUIDs(t, kubeClient)

	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "external-dispatch.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	persistence := sqlite.NewStore(db, "external-dispatch-test")
	cipher, err := sqlite.NewAgentExecutionSnapshotCipher(bytes.Repeat([]byte{0x58}, sqlite.AgentExecutionSnapshotKeyBytes))
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.SetAgentExecutionSnapshotCipher(cipher); err != nil {
		t.Fatal(err)
	}
	controlStore, err := storekube.NewComposite(kubeClient, defaultNS, persistence, storekube.WithAPIReader(kubeClient))
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, "external-dispatch-controller").WithMirror(persistence)
	epochCtx, cancelEpoch := context.WithCancel(ctx)
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	t.Cleanup(func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("stop external dispatch epoch manager: %v", err)
		}
	})
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fence.Epoch != externalRuntime.Status.ObservedCapabilities.ControllerEpoch {
		t.Fatalf("controller epoch = %d, fixture runtime epoch = %d", fence.Epoch, externalRuntime.Status.ObservedCapabilities.ControllerEpoch)
	}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: controlStore,
		Transcripts:     persistence,
		Publications:    controlStore,
		BranchClaims:    controlStore,
		Lineages:        persistence,
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionManager := NewSessionManager(persistence)
	sessionManager.SetGatewayEventStore(persistence)
	reconciler := &TaskReconciler{
		Client: kubeClient, APIReader: kubeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(32),
		DurableControlStore: controlStore, ControllerEpochManager: epochs, AgentExecutionSnapshots: persistence,
		ResultStore: persistence, MessageStore: persistence, PlanStore: persistence, ExecutionEventStore: persistence,
		SessionManager: sessionManager, ACPRuntimeEnabled: true,
	}
	dispatcher := &ACPDispatcher{
		Client: kubeClient, APIReader: kubeClient, Store: controlStore, ResultStore: persistence,
		EventStore: persistence, PlanStore: persistence, Snapshots: persistence, Epochs: epochs, Sessions: continuity,
	}
	return &externalACPDispatchFixture{
		ctx: ctx, client: kubeClient, controlStore: controlStore, persistence: persistence, epochs: epochs,
		reconciler: reconciler, dispatcher: dispatcher,
		agent: agent, runtime: externalRuntime, createCalls: createCalls, createRequests: createRequests, mcpPolicy: policy,
		deleteCalls: deleteCalls, deleteRequests: deleteRequests,
	}
}

func newExternalRuntimeStatusProxy(
	t *testing.T,
	upstream string,
	transform func(*harnessv2.StatusResponse),
) *httptest.Server {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method != http.MethodGet || response.Request.URL.Path != harnessv2.StatusPath {
			return nil
		}
		defer response.Body.Close() //nolint:errcheck
		var status harnessv2.StatusResponse
		if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
			return err
		}
		transform(&status)
		body, err := json.Marshal(status)
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = -1
		response.Header.Del("Content-Length")
		return nil
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	return server
}

func newExternalRuntimeCapabilitiesProxy(
	t *testing.T,
	upstream string,
	transform func(*harnessv2.CapabilitiesResponse),
) *httptest.Server {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method != http.MethodGet || response.Request.URL.Path != harnessv2.CapabilitiesPath {
			return nil
		}
		defer response.Body.Close() //nolint:errcheck
		var capabilities harnessv2.CapabilitiesResponse
		if err := json.NewDecoder(response.Body).Decode(&capabilities); err != nil {
			return err
		}
		transform(&capabilities)
		body, err := json.Marshal(capabilities)
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = -1
		response.Header.Del("Content-Length")
		return nil
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	return server
}

func newExternalRuntimePromptProxy(
	t *testing.T,
	upstream string,
	observe func(harnessv2.StartPromptRequest),
) *httptest.Server {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut && strings.Contains(request.URL.Path, "/prompts/") {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(w, "read prompt request", http.StatusBadRequest)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			var prompt harnessv2.StartPromptRequest
			if err := json.Unmarshal(body, &prompt); err != nil {
				http.Error(w, "decode prompt request", http.StatusBadRequest)
				return
			}
			observe(prompt)
		}
		proxy.ServeHTTP(w, request)
	}))
	t.Cleanup(server.Close)
	return server
}

func newExternalRuntimeWorkspaceDeltaProxy(
	t *testing.T,
	upstream string,
	observe func(harnessv2.CreateWorkspaceDeltaRequest),
) *httptest.Server {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut && strings.Contains(request.URL.Path, "/workspace-deltas/") {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(w, "read workspace delta request", http.StatusBadRequest)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			var delta harnessv2.CreateWorkspaceDeltaRequest
			if err := json.Unmarshal(body, &delta); err != nil {
				http.Error(w, "decode workspace delta request", http.StatusBadRequest)
				return
			}
			observe(delta)
		}
		proxy.ServeHTTP(w, request)
	}))
	t.Cleanup(server.Close)
	return server
}

func testExternalACPCustomTool(name string) *corev1alpha1.Tool {
	return &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNS, Name: name, UID: types.UID(name + "-uid"), Generation: 1,
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "look up a value", BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
			HTTP: &corev1alpha1.HTTPExecution{URL: "https://tool.example.invalid", Method: http.MethodPost},
		},
	}
}

func updateExternalACPObservedDescriptorDigest(t *testing.T, fixture *externalACPDispatchFixture) string {
	t.Helper()
	runtimeObject := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
		t.Fatal(err)
	}
	profile, err := agentRuntimeProfile(*runtimeObject.Spec.Capabilities.Profile)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := buildAgentRuntimeMCPConfigurationWithRegistry(
		fixture.ctx, fixture.client, runtimeObject, profile, tools.DefaultRegistry,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeObject.Status.ObservedCapabilities.MCPToolDescriptorDigest = configuration.ToolPolicy.DescriptorDigest
	if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
		t.Fatal(err)
	}
	fixture.runtime = runtimeObject.DeepCopy()
	return configuration.ToolPolicy.DescriptorDigest
}

func (f *externalACPDispatchFixture) queueTask(
	t *testing.T,
	name string,
	uid types.UID,
	prompt string,
	sessionRef *corev1alpha1.SessionReference,
) *corev1alpha1.Task {
	t.Helper()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNS, Name: name, UID: uid, Generation: 1, Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: f.agent.Name},
			Prompt: prompt, SessionRef: sessionRef,
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{
		AllowedTools: append([]string{}, f.mcpPolicy.AllowedTools...),
	}
	if err := f.client.Create(f.ctx, task); err != nil {
		t.Fatal(err)
	}
	current := &corev1alpha1.Task{}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	result, err := f.reconciler.handlePending(f.ctx, current)
	if err != nil {
		t.Fatalf("handlePending: %v", err)
	}
	if result.RequeueAfter != time.Second {
		_, candidateErr := f.reconciler.resolveAgentExecutionCandidate(f.ctx, current, f.agent)
		t.Fatalf("RequeueAfter = %v, want %v; candidate error: %v", result.RequeueAfter, time.Second, candidateErr)
	}
	queued := &corev1alpha1.Task{}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(task), queued); err != nil {
		t.Fatal(err)
	}
	if queued.Status.AgentExecutionBinding == nil ||
		queued.Status.AgentExecutionBinding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint ||
		queued.Status.AgentExecutionBinding.RuntimeRef == nil ||
		queued.Status.AgentExecutionBinding.RuntimeRef.UID != f.runtime.UID {
		t.Fatalf("external execution binding = %#v; Task status = %#v", queued.Status.AgentExecutionBinding, queued.Status)
	}
	if queued.Status.Execution == nil || queued.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued ||
		queued.Status.Execution.AgentRuntimeName != f.runtime.Name ||
		queued.Status.Execution.AgentRuntimeUID != string(f.runtime.UID) ||
		queued.Status.Execution.RuntimePoolName != "" || queued.Status.Execution.RuntimePoolUID != "" {
		t.Fatalf("external queued execution = %#v", queued.Status.Execution)
	}
	if queued.Annotations[acpExternalRuntimeTaskAnnotation] != f.runtime.Name ||
		queued.Labels[acpExternalRuntimeTaskAnnotation] != "" || queued.Labels[acpRuntimeTaskPoolLabel] != "" {
		t.Fatalf("external queue metadata = labels %#v annotations %#v", queued.Labels, queued.Annotations)
	}
	attemptID, err := promptAttemptIDFromTask(queued)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := f.controlStore.GetPromptAttempt(f.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionQueued ||
		attempt.BindingDigest != queued.Status.AgentExecutionBinding.BindingDigest ||
		attempt.SnapshotDigest != queued.Status.AgentExecutionBinding.Snapshot.Digest ||
		attempt.RequestDigest != queued.Status.Execution.RequestDigest {
		t.Fatalf("external durable PromptAttempt = %#v", attempt)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := f.client.List(f.ctx, &pools, client.InNamespace(defaultNS)); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 0 {
		t.Fatalf("external queue created %d RuntimePools", len(pools.Items))
	}
	return queued
}

func (f *externalACPDispatchFixture) dispatch(t *testing.T, queued *corev1alpha1.Task) *corev1alpha1.Task {
	t.Helper()
	dispatchQueuedTask(f.ctx, t, f.dispatcher, queued.DeepCopy())
	completed := &corev1alpha1.Task{}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(queued), completed); err != nil {
		t.Fatal(err)
	}
	return completed
}

func createExternalRuntimeSessionForRecovery(
	t *testing.T,
	fixture *externalACPDispatchFixture,
	queued *corev1alpha1.Task,
	operation string,
) (*corev1alpha1.Task, *verifiedAgentExecution, harnessv2.Fence) {
	t.Helper()
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(
		runtimeFence, bound.frozenTask, operation, false, time.Now().UTC().Add(30*time.Second),
	)
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RuntimeInstanceID = string(metadata.Fence.RuntimeInstanceID)
	current.Status.Execution.RuntimeSessionUID = string(metadata.Fence.RuntimeSessionUID)
	current.Status.Execution.RuntimeSessionGeneration = int64(metadata.Fence.RuntimeSessionGeneration)
	current.Status.Execution.RuntimeSessionSupervisorBootID = string(metadata.Fence.SupervisorBootID)
	current.Status.Execution.RuntimeSessionProfileDigest = string(metadata.Fence.RuntimeProfileDigest)
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	return current, bound, metadata.Fence
}

func rotateExternalRuntimeCredentials(
	t *testing.T,
	fixture *externalACPDispatchFixture,
	markConformed bool,
) {
	t.Helper()
	secret := &corev1.Secret{}
	controllerRef := fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	capabilityRef := fixture.runtime.Spec.ClientAuth.OperationCapabilitySecretRef
	if err := fixture.client.Get(
		fixture.ctx,
		client.ObjectKey{Namespace: fixture.runtime.Namespace, Name: controllerRef.Name},
		secret,
	); err != nil {
		t.Fatal(err)
	}
	originalUID := secret.UID
	originalVersion := secret.ResourceVersion
	secret.Data[controllerRef.Key] = []byte(strings.Repeat("u", 32))
	secret.Data[capabilityRef.Key] = []byte(strings.Repeat("v", 32))
	if err := fixture.client.Update(fixture.ctx, secret); err != nil {
		t.Fatal(err)
	}
	if secret.UID != originalUID || secret.ResourceVersion == originalVersion {
		t.Fatalf(
			"in-place credential rotation identity = uid:%q resourceVersion:%q, want uid %q and a version after %q",
			secret.UID, secret.ResourceVersion, originalUID, originalVersion,
		)
	}
	if !markConformed {
		return
	}
	runtimeObject := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
		t.Fatal(err)
	}
	runtimeObject.Status.Ready = true
	runtimeObject.Status.ObservedGeneration = runtimeObject.Generation
	runtimeObject.Status.ObservedControllerAuthRefResourceVersion = secret.ResourceVersion
	runtimeObject.Status.ObservedOperationCapabilityRefResourceVersion = secret.ResourceVersion
	if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
		t.Fatal(err)
	}
	fixture.runtime = runtimeObject.DeepCopy()
}

func TestACPDispatcherExecutesExternalRuntimeTask(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-task", types.UID("external-task-uid"), "do work", nil)
	completed := fixture.dispatch(t, queued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("completed external Task status = %#v", completed.Status)
	}
	result, err := fixture.persistence.GetResult(fixture.ctx, completed.Namespace, completed.Name)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "from runtime" {
		t.Fatalf("result = %q, want external runtime result", result)
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external CreateRuntimeSession calls = %d, want 1", fixture.createCalls.Load())
	}
	select {
	case request := <-fixture.createRequests:
		if request.AgentConfiguration != nil {
			t.Fatalf("external runtime received controller-owned Agent configuration: %#v", request.AgentConfiguration)
		}
		if request.Metadata.Fence.RuntimeInstanceID != "pod-uid.boot-id" ||
			request.Metadata.Fence.RuntimePoolUID != "pool-uid" || request.Profile.Model != acpTestModel {
			t.Fatalf("external CreateRuntimeSession request = %#v", request)
		}
	default:
		t.Fatal("external runtime did not receive CreateRuntimeSession")
	}
}

func TestACPDispatcherRetryableUnsentExternalRuntimeSessionCreationRequeues(t *testing.T) {
	tests := []struct {
		name        string
		sessionRef  *corev1alpha1.SessionReference
		wantDeletes int32
	}{
		{name: "task scoped", wantDeletes: 1},
		{
			name: "named session",
			sessionRef: &corev1alpha1.SessionReference{
				Name: "external-create-retry", Create: true, Append: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExternalACPDispatchFixture(t)
			queued := fixture.queueTask(
				t, "external-create-retry-"+strings.ReplaceAll(test.name, " ", "-"),
				types.UID("external-create-retry-"+strings.ReplaceAll(test.name, " ", "-")+"-uid"),
				"retry runtime session creation", test.sessionRef,
			)
			failingReader := &failAgentRuntimeReadWhileTaskPlanned{
				Reader: fixture.client, taskKey: client.ObjectKeyFromObject(queued),
			}
			fixture.dispatcher.APIReader = failingReader

			requeued := fixture.dispatch(t, queued)
			if requeued.Status.Phase != corev1alpha1.TaskPhasePending || requeued.Status.Execution == nil ||
				requeued.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
				taskExecutionStateTerminal(requeued.Status.Execution.State) {
				t.Fatalf("retryable unsent RuntimeSession creation status = %#v, want nonterminal Reserved", requeued.Status)
			}
			if requeued.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
				requeued.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest {
				t.Fatalf("retryable RuntimeSession creation changed sealed prompt identity: before=%#v after=%#v", queued.Status.Execution, requeued.Status.Execution)
			}
			if fixture.createCalls.Load() != 0 || fixture.deleteCalls.Load() != 0 || failingReader.failures.Load() != 1 {
				t.Fatalf(
					"RuntimeSession calls after preflight requeue = create:%d delete:%d injected-read-failures:%d, want 0/0/1",
					fixture.createCalls.Load(), fixture.deleteCalls.Load(), failingReader.failures.Load(),
				)
			}

			completed := fixture.dispatch(t, requeued)
			if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
				completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
				t.Fatalf("retried external Task status = %#v", completed.Status)
			}
			if completed.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
				completed.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest {
				t.Fatalf("successful RuntimeSession retry changed sealed prompt identity: before=%#v after=%#v", requeued.Status.Execution, completed.Status.Execution)
			}
			if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != test.wantDeletes {
				t.Fatalf(
					"RuntimeSession calls after successful retry = create:%d delete:%d, want 1/%d",
					fixture.createCalls.Load(), fixture.deleteCalls.Load(), test.wantDeletes,
				)
			}
		})
	}
}

func TestACPDispatcherPersistsExternalTaskScopedSupervisorBootBeforePrompt(t *testing.T) {
	var fixture *externalACPDispatchFixture
	var taskKey client.ObjectKey
	promptExecutions := make(chan corev1alpha1.TaskExecutionStatus, 1)
	promptErrors := make(chan error, 1)
	fixture = newExternalACPDispatchFixtureWithOptions(
		t,
		"external-v2",
		testAgentRuntimeMCPPolicy(),
		externalACPDispatchFixtureOptions{promptObserver: func(_ harnessv2.StartPromptRequest) {
			current := &corev1alpha1.Task{}
			if err := fixture.client.Get(fixture.ctx, taskKey, current); err != nil {
				promptErrors <- err
				return
			}
			if current.Status.Execution == nil {
				promptErrors <- errors.New("prompt Task execution status is missing")
				return
			}
			promptExecutions <- *current.Status.Execution
		}},
	)
	queued := fixture.queueTask(
		t,
		"external-task-scoped-boot",
		types.UID("external-task-scoped-boot-uid"),
		"use a brokered tool",
		nil,
	)
	taskKey = client.ObjectKeyFromObject(queued)
	completed := fixture.dispatch(t, queued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("completed external Task status = %#v", completed.Status)
	}
	select {
	case err := <-promptErrors:
		t.Fatal(err)
	case execution := <-promptExecutions:
		if execution.State != corev1alpha1.TaskExecutionStateSubmitting ||
			execution.RuntimeSessionSupervisorBootID != "boot-id" ||
			execution.RuntimeSessionUID == "" || execution.RuntimeSessionGeneration < 1 {
			t.Fatalf("external task-scoped execution at prompt submission = %#v", execution)
		}
	default:
		t.Fatal("external runtime did not receive a prompt request")
	}
}

//nolint:gocyclo // The end-to-end retry scenario keeps every identity, lifecycle, and cleanup assertion together.
func TestACPDispatcherRetryableUnsentExternalPromptRequeuesSameSessionTurn(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(
		t,
		"external-retryable-unsent",
		types.UID("external-retryable-unsent-uid"),
		"retry the same prompt",
		&corev1alpha1.SessionReference{Name: "external-retryable-unsent", Create: true, Append: true},
	)
	attemptID, err := promptAttemptIDFromTask(queued)
	if err != nil {
		t.Fatal(err)
	}
	failingReader := &failAgentRuntimeReadWhileTaskSubmitting{
		Reader:  fixture.client,
		taskKey: client.ObjectKeyFromObject(queued),
	}
	fixture.dispatcher.APIReader = failingReader

	requeued := fixture.dispatch(t, queued)
	if requeued.Status.Phase != corev1alpha1.TaskPhasePending || requeued.Status.Execution == nil ||
		requeued.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		taskExecutionStateTerminal(requeued.Status.Execution.State) {
		t.Fatalf("retryable unsent Task status = %#v, want nonterminal Reserved", requeued.Status)
	}
	if requeued.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
		requeued.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest ||
		requeued.Status.Execution.RuntimeSessionUID == "" || requeued.Status.Execution.RuntimeSessionGeneration < 1 {
		t.Fatalf("retryable unsent Task lost its sealed identity or RuntimeSession binding: %#v", requeued.Status.Execution)
	}
	if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 0 {
		t.Fatalf("runtime session calls after requeue = create:%d delete:%d, want 1/0", fixture.createCalls.Load(), fixture.deleteCalls.Load())
	}
	if failingReader.failures.Load() != 1 {
		t.Fatalf("AgentRuntime read failures = %d, want one failure during start_prompt revalidation", failingReader.failures.Load())
	}

	attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionReserved || attempt.RuntimeInstanceID == "" ||
		attempt.SessionUID == "" || attempt.SessionLeaseGeneration < 1 {
		t.Fatalf("retryable unsent PromptAttempt lost its bindings: %#v", attempt)
	}
	turnID, err := (store.SessionTurnKey{
		SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
		TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	turn, err := fixture.controlStore.GetSessionTurn(fixture.ctx, turnID)
	if err != nil {
		t.Fatal(err)
	}
	if turn.State != store.SessionTurnOpen {
		t.Fatalf("SessionTurn state after retryable unsent prompt = %s, want Open", turn.State)
	}
	events, err := fixture.persistence.ListExecutionEvents(fixture.ctx, store.ExecutionEventFilter{
		Namespace: queued.Namespace, StreamType: store.ExecutionEventStreamTypeTask, StreamID: queued.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		switch event.Type {
		case "model.request.started", "model.request.completed", "model.request.failed":
			t.Fatalf("retryable unsent prompt recorded terminal lifecycle event %#v", event)
		}
	}

	completed := fixture.dispatch(t, requeued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("retried external Task status = %#v", completed.Status)
	}
	if completed.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
		completed.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest ||
		completed.Status.Execution.RuntimeSessionUID != requeued.Status.Execution.RuntimeSessionUID ||
		completed.Status.Execution.RuntimeSessionGeneration != requeued.Status.Execution.RuntimeSessionGeneration {
		t.Fatalf("successful retry changed sealed identity or RuntimeSession binding: before=%#v after=%#v", requeued.Status.Execution, completed.Status.Execution)
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external CreateRuntimeSession calls after successful retry = %d, want reuse of one live session", fixture.createCalls.Load())
	}
	finalAttempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if finalAttempt.ExecutionState != store.PromptExecutionSucceeded || finalAttempt.SessionUID != attempt.SessionUID ||
		finalAttempt.SessionLeaseGeneration != attempt.SessionLeaseGeneration {
		t.Fatalf("successful retry PromptAttempt = %#v, want same session binding", finalAttempt)
	}
	turn, err = fixture.controlStore.GetSessionTurn(fixture.ctx, turnID)
	if err != nil {
		t.Fatal(err)
	}
	if turn.State != store.SessionTurnFinalized {
		t.Fatalf("SessionTurn state after successful retry = %s, want Finalized", turn.State)
	}
}

func TestACPDispatcherRetryableUnsentExternalWorkspaceDeltaRetriesSameOperation(t *testing.T) {
	var deltaCalls atomic.Int32
	deltaRequests := make(chan harnessv2.CreateWorkspaceDeltaRequest, 1)
	fixture := newExternalACPDispatchFixtureWithOptions(
		t,
		"external-v2",
		testAgentRuntimeMCPPolicy(),
		externalACPDispatchFixtureOptions{workspaceDeltaObserver: func(request harnessv2.CreateWorkspaceDeltaRequest) {
			deltaCalls.Add(1)
			deltaRequests <- request
		}},
	)
	queued := fixture.queueTask(
		t,
		"external-retryable-unsent-delta",
		types.UID("external-retryable-unsent-delta-uid"),
		"retry the same workspace delta",
		nil,
	)
	failingReader := &failAgentRuntimeReadWhileTaskSettling{
		Reader: fixture.client, taskKey: client.ObjectKeyFromObject(queued),
	}
	fixture.dispatcher.APIReader = failingReader

	completed := fixture.dispatch(t, queued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("retried external Task status = %#v", completed.Status)
	}
	if failingReader.failures.Load() != 1 || deltaCalls.Load() != 1 ||
		fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 1 {
		t.Fatalf(
			"workspace delta retry calls = read-failures:%d deltas:%d creates:%d deletes:%d, want 1/1/1/1",
			failingReader.failures.Load(), deltaCalls.Load(), fixture.createCalls.Load(), fixture.deleteCalls.Load(),
		)
	}
	select {
	case request := <-deltaRequests:
		wantOperationID := harnessv2.OperationID("workspace-delta-" + completed.Status.Execution.PromptID)
		if request.Metadata.OperationID != wantOperationID || request.Metadata.RequestDigest == "" {
			t.Fatalf("retried workspace delta request = %#v", request)
		}
	default:
		t.Fatal("external runtime did not receive the retried workspace delta")
	}
	attemptID, err := promptAttemptIDFromTask(completed)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionSucceeded || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		t.Fatalf("retried workspace delta PromptAttempt = %#v", attempt)
	}
}

func TestACPDispatcherRetryableUnsentExternalTaskScopedPromptReusesRuntimeSession(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(
		t,
		"external-task-scoped-retryable-unsent",
		types.UID("external-task-scoped-retryable-unsent-uid"),
		"retry the same task-scoped prompt",
		nil,
	)
	failingReader := &failAgentRuntimeReadWhileTaskSubmitting{
		Reader:  fixture.client,
		taskKey: client.ObjectKeyFromObject(queued),
	}
	fixture.dispatcher.APIReader = failingReader

	requeued := fixture.dispatch(t, queued)
	if requeued.Status.Phase != corev1alpha1.TaskPhasePending || requeued.Status.Execution == nil ||
		requeued.Status.Execution.State != corev1alpha1.TaskExecutionStateReserved ||
		requeued.Status.Execution.RuntimeSessionUID == "" || requeued.Status.Execution.RuntimeSessionGeneration < 1 {
		t.Fatalf("retryable unsent task-scoped Task status = %#v, want bound nonterminal Reserved", requeued.Status)
	}
	if requeued.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
		requeued.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest {
		t.Fatalf("retryable unsent task-scoped Task changed sealed prompt identity: before=%#v after=%#v", queued.Status.Execution, requeued.Status.Execution)
	}
	if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 0 || failingReader.failures.Load() != 1 {
		t.Fatalf(
			"task-scoped calls after requeue = create:%d delete:%d injected-read-failures:%d, want 1/0/1",
			fixture.createCalls.Load(), fixture.deleteCalls.Load(), failingReader.failures.Load(),
		)
	}

	completed := fixture.dispatch(t, requeued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded || completed.Status.Execution == nil ||
		completed.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeSucceeded {
		t.Fatalf("retried task-scoped external Task status = %#v", completed.Status)
	}
	if completed.Status.Execution.PromptID != queued.Status.Execution.PromptID ||
		completed.Status.Execution.RequestDigest != queued.Status.Execution.RequestDigest ||
		completed.Status.Execution.RuntimeSessionUID != requeued.Status.Execution.RuntimeSessionUID ||
		completed.Status.Execution.RuntimeSessionGeneration != requeued.Status.Execution.RuntimeSessionGeneration {
		t.Fatalf("successful task-scoped retry changed sealed identity or RuntimeSession binding: before=%#v after=%#v", requeued.Status.Execution, completed.Status.Execution)
	}
	if fixture.createCalls.Load() != 1 || fixture.deleteCalls.Load() != 1 {
		t.Fatalf(
			"task-scoped RuntimeSession calls after successful retry = create:%d delete:%d, want one reused session and terminal cleanup",
			fixture.createCalls.Load(), fixture.deleteCalls.Load(),
		)
	}
}

func TestACPDispatcherExternalReservationRechecksAdmissionGate(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-drain-race", types.UID("external-drain-race-uid"), "do not reserve", nil)
	fixture.dispatcher.AdmissionGate = NewACPAdmissionGate()
	fixture.dispatcher.AdmissionGate.Close("planned drain", time.Now().UTC())

	// reserveTask runs after dispatchOnce has scanned the queue and checked the
	// gate once. Closing it here models a drain that starts between that scan and
	// the per-runtime reservation barrier.
	reserved, target, err := fixture.dispatcher.reserveTask(fixture.ctx, queued.DeepCopy())
	if !errors.Is(err, ErrACPAdmissionClosed) {
		t.Fatalf("reserveTask() error = %v, want ErrACPAdmissionClosed", err)
	}
	if reserved != nil || target.pool != nil || target.external != nil || target.reservation != nil {
		t.Fatalf("reserveTask() = (%#v, %#v), want no reservation", reserved, target)
	}
	attemptID, err := promptAttemptIDFromTask(queued)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionQueued {
		t.Fatalf("PromptAttempt state = %s, want %s", attempt.ExecutionState, store.PromptExecutionQueued)
	}
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Execution == nil || current.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued {
		t.Fatalf("Task execution = %#v, want queued", current.Status.Execution)
	}
	if fixture.createCalls.Load() != 0 {
		t.Fatalf("external runtime received %d CreateRuntimeSession requests", fixture.createCalls.Load())
	}
}

func TestValidateExternalRuntimeStatusRequiresReadyAdmission(t *testing.T) {
	runtimeObject := &corev1alpha1.AgentRuntime{
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
			RuntimeInstanceID: "runtime-instance",
			Profile:           &corev1alpha1.AgentRuntimeProfileSpec{Digest: "profile-digest"},
		}},
		Status: corev1alpha1.AgentRuntimeStatus{ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
			RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
			RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: "profile-digest",
			ProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
		}},
	}
	baseline := harnessv2.StatusResponse{
		Fence: harnessv2.Fence{
			RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
			RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: "profile-digest",
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		Lifecycle: harnessv2.SupervisorLifecycleReady,
		Drain:     harnessv2.DrainStatus{AcceptingNewSessions: true},
	}
	controllerFence := store.ControllerEpochFence{Epoch: 1}
	if err := validateExternalRuntimeStatus(runtimeObject, controllerFence, &baseline, true); err != nil {
		t.Fatalf("ready status rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*harnessv2.StatusResponse)
	}{
		{name: "booting", mutate: func(status *harnessv2.StatusResponse) {
			status.Lifecycle = harnessv2.SupervisorLifecycleBooting
		}},
		{name: "draining", mutate: func(status *harnessv2.StatusResponse) {
			status.Lifecycle = harnessv2.SupervisorLifecycleDraining
			status.Drain = harnessv2.DrainStatus{Requested: true, RequestedAt: time.Now().UTC()}
		}},
		{name: "not accepting", mutate: func(status *harnessv2.StatusResponse) {
			status.Drain.AcceptingNewSessions = false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := baseline
			test.mutate(&status)
			if err := validateExternalRuntimeStatus(runtimeObject, controllerFence, &status, true); err == nil ||
				!strings.Contains(err.Error(), "not ready to accept new sessions") {
				t.Fatalf("validateExternalRuntimeStatus() error = %v, want admission rejection", err)
			}
		})
	}
	draining := baseline
	draining.Lifecycle = harnessv2.SupervisorLifecycleDraining
	draining.Drain = harnessv2.DrainStatus{Requested: true, RequestedAt: time.Now().UTC()}
	if err := validateExternalRuntimeStatus(runtimeObject, controllerFence, &draining, false); err != nil {
		t.Fatalf("exact-fenced draining settlement rejected: %v", err)
	}
	draining.Fence.SupervisorBootID = "replacement-boot"
	if err := validateExternalRuntimeStatus(runtimeObject, controllerFence, &draining, false); err == nil ||
		!strings.Contains(err.Error(), "authenticated status fence drifted") {
		t.Fatalf("draining settlement fence error = %v, want exact-fence rejection", err)
	}
}

func TestExternalRuntimeMutationRequiresAdmission(t *testing.T) {
	for _, test := range []struct {
		operation string
		want      bool
	}{
		{operation: "create_runtime_session", want: true},
		{operation: "start_prompt", want: true},
		{operation: "renew_prompt_lease", want: false},
		{operation: "resolve_permission", want: false},
		{operation: "cancel_prompt", want: false},
		{operation: "create_workspace_delta", want: false},
		{operation: "finalize_runtime_session_publication", want: false},
		{operation: "delete_runtime_session", want: false},
		{operation: "drain", want: false},
		{operation: "future_mutation", want: true},
	} {
		t.Run(test.operation, func(t *testing.T) {
			if got := externalRuntimeMutationRequiresAdmission(test.operation); got != test.want {
				t.Fatalf("externalRuntimeMutationRequiresAdmission(%q) = %t, want %t", test.operation, got, test.want)
			}
		})
	}
}

func TestACPDispatcherExternalRuntimeClientKeepsFrozenCleanupAuthorityAfterRegistrationDrift(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-live-cleanup-drift", types.UID("external-live-cleanup-drift-uid"), "recover", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "live-cleanup-drift", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}

	var replacementCalls atomic.Int32
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		replacementCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer replacement.Close()
	current := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Deployment.Endpoint = replacement.URL
	current.Generation++
	if err := fixture.client.Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}

	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err == nil {
		t.Fatal("new admission succeeded after registration drift")
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external runtime received %d CreateRuntimeSession calls, want only the pre-drift admission", fixture.createCalls.Load())
	}
	if err := fixture.dispatcher.deleteRuntimeSession(
		fixture.ctx, runtimeClient, request.RuntimeSessionID, bound.frozenTask, metadata.Fence, "test cleanup",
	); err != nil {
		t.Fatalf("delete resident RuntimeSession after registration drift: %v", err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("external runtime received %d DeleteRuntimeSession calls, want 1", fixture.deleteCalls.Load())
	}
	if replacementCalls.Load() != 0 {
		t.Fatalf("cleanup contacted replacement endpoint %d times, want 0", replacementCalls.Load())
	}
}

func TestExternalRuntimeRecoverySettlesExactSessionWhileDraining(t *testing.T) {
	var draining atomic.Bool
	fixture := newExternalACPDispatchFixtureWithOptions(
		t,
		"external-v2",
		testAgentRuntimeMCPPolicy(),
		externalACPDispatchFixtureOptions{statusTransform: func(status *harnessv2.StatusResponse) {
			if !draining.Load() {
				return
			}
			status.Lifecycle = harnessv2.SupervisorLifecycleDraining
			status.Drain = harnessv2.DrainStatus{
				Requested: true, RequestedAt: time.Now().UTC(), Reason: "test drain",
			}
		}},
	)
	queued := fixture.queueTask(t, "external-draining-recovery", types.UID("external-draining-recovery-uid"), "recover", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "draining-recovery", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}

	currentTask := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), currentTask); err != nil {
		t.Fatal(err)
	}
	currentTask.Status.Execution.RuntimeInstanceID = string(metadata.Fence.RuntimeInstanceID)
	currentTask.Status.Execution.RuntimeSessionUID = string(metadata.Fence.RuntimeSessionUID)
	currentTask.Status.Execution.RuntimeSessionGeneration = int64(metadata.Fence.RuntimeSessionGeneration)
	currentTask.Status.Execution.RuntimeSessionSupervisorBootID = string(metadata.Fence.SupervisorBootID)
	currentTask.Status.Execution.RuntimeSessionProfileDigest = string(metadata.Fence.RuntimeProfileDigest)
	if err := fixture.client.Status().Update(fixture.ctx, currentTask); err != nil {
		t.Fatal(err)
	}

	draining.Store(true)
	if _, _, _, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	); err == nil || !strings.Contains(err.Error(), "not ready to accept new sessions") {
		t.Fatalf("new admission while draining error = %v, want admission rejection", err)
	}
	currentRuntime := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), currentRuntime); err != nil {
		t.Fatal(err)
	}
	currentRuntime.Status.Ready = false
	if err := fixture.client.Status().Update(fixture.ctx, currentRuntime); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err == nil ||
		!strings.Contains(err.Error(), "has not passed current-generation v2 conformance") {
		t.Fatalf("existing client admission while draining error = %v, want readiness rejection", err)
	}
	if err := fixture.dispatcher.deleteRuntimeSession(
		fixture.ctx, runtimeClient, request.RuntimeSessionID, bound.frozenTask, metadata.Fence, "test settlement",
	); err != nil {
		t.Fatalf("exact-fenced deletion after readiness transition: %v", err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(currentTask), currentTask); err != nil {
		t.Fatal(err)
	}

	complete, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, currentTask)
	if err != nil || !complete {
		t.Fatalf("draining recovery cleanup = complete:%t err:%v, want complete", complete, err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("draining recovery DELETE calls = %d, want 1", fixture.deleteCalls.Load())
	}
}

func TestExternalActiveSessionKeepsFrozenAuthorityAfterFailedSameIdentityProbe(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-active-probe-failure", types.UID("external-active-probe-failure-uid"), "continue", nil)
	originalDescriptorDigest := fixture.runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest

	active := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), active); err != nil {
		t.Fatal(err)
	}
	active.Status.Phase = corev1alpha1.TaskPhaseRunning
	active.Status.Execution.State = corev1alpha1.TaskExecutionStateRunning
	if err := fixture.client.Status().Update(fixture.ctx, active); err != nil {
		t.Fatal(err)
	}

	runtimeObject := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtimeObject); err != nil {
		t.Fatal(err)
	}
	failedObservation := runtimeObject.Status.ObservedCapabilities.DeepCopy()
	failedObservation.MCPToolDescriptorDigest = testControllerDigest("unproven-mcp-descriptors")
	runtimeObject.Status.Ready = false
	runtimeObject.Status.ObservedCapabilities = retainedAgentRuntimeObservation(
		runtimeObject,
		false,
		failedObservation,
		runtimeObject.Status.ObservedControllerAuthRefResourceVersion,
		runtimeObject.Status.ObservedOperationCapabilityRefResourceVersion,
	)
	if runtimeObject.Status.ObservedCapabilities.MCPToolDescriptorDigest != originalDescriptorDigest {
		t.Fatalf("failed same-identity probe replaced conformed descriptor digest %q with %q",
			originalDescriptorDigest, runtimeObject.Status.ObservedCapabilities.MCPToolDescriptorDigest)
	}
	if err := fixture.client.Status().Update(fixture.ctx, runtimeObject); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(active), active); err != nil {
		t.Fatal(err)
	}

	verified, err := fixture.reconciler.loadVerifiedBoundExecutionForActiveSession(
		fixture.ctx, active, active.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatalf("active frozen execution after failed same-identity probe: %v", err)
	}
	if verified.mcpConfiguration.ToolPolicy.DescriptorDigest != originalDescriptorDigest {
		t.Fatalf("active frozen descriptor digest = %q, want %q",
			verified.mcpConfiguration.ToolPolicy.DescriptorDigest, originalDescriptorDigest)
	}
	if _, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, active, active.Status.AgentExecutionBinding,
	); err == nil || !strings.Contains(err.Error(), "has not passed current-generation v2 conformance") {
		t.Fatalf("new admission after failed same-identity probe error = %v, want readiness rejection", err)
	}
}

func TestExternalRuntimeBindingRejectsReadyRuntimeFromPreviousControllerEpoch(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	takeover := NewControllerEpochManager(fixture.controlStore, "external-binding-takeover-controller")
	takeoverCtx, cancelTakeover := context.WithCancel(fixture.ctx)
	takeoverDone := make(chan error, 1)
	go func() { takeoverDone <- takeover.Start(takeoverCtx) }()
	t.Cleanup(func() {
		cancelTakeover()
		if err := <-takeoverDone; err != nil {
			t.Errorf("stop takeover epoch manager: %v", err)
		}
	})
	fence, err := takeover.CurrentFence(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fence.Epoch != 2 {
		t.Fatalf("takeover controller epoch = %d, want 2", fence.Epoch)
	}
	fixture.reconciler.ControllerEpochManager = takeover
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: defaultNS, Name: "stale-binding", UID: types.UID("stale-binding-uid"), Generation: 1},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: fixture.agent.Name}, Prompt: "do not bind",
		},
	}

	candidate, err := fixture.reconciler.resolveAgentExecutionCandidate(fixture.ctx, task, fixture.agent)
	if err == nil || candidate != nil || !strings.Contains(err.Error(), "fenced to controller epoch 1, current epoch is 2") {
		t.Fatalf("resolveAgentExecutionCandidate() = (%#v, %v), want stale controller epoch rejection", candidate, err)
	}
}

func TestExternalRuntimeBindingRequiresPublicationFinalizationForWriteProfile(t *testing.T) {
	for _, supportsPublicationFinalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("supports publication finalization=%t", supportsPublicationFinalization), func(t *testing.T) {
			fixture := newExternalACPDispatchFixtureWithOptions(
				t,
				"external-write",
				testAgentRuntimeMCPPolicy(),
				externalACPDispatchFixtureOptions{
					profileTransform: func(profile *harnessv2.RuntimeProfile) {
						profile.WorkspaceIntent = harnessv2.WorkspaceIntentWrite
					},
					supportsPublicationFinalization: supportsPublicationFinalization,
				},
			)
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: defaultNS, Name: "external-write", UID: types.UID("external-write-uid"), Generation: 1,
				},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: fixture.agent.Name},
					Prompt: "update the repository", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{}},
					Workspace: &corev1alpha1.WorkspaceConfig{Intent: corev1alpha1.WorkspaceIntentWrite},
				},
			}

			candidate, err := fixture.reconciler.resolveExternalAgentExecutionCandidate(fixture.ctx, task, fixture.agent)
			if !supportsPublicationFinalization {
				if err == nil || candidate != nil || !isPermanentACPAgentConfigurationError(err) ||
					!strings.Contains(err.Error(), "publication finalization") {
					t.Fatalf("resolveExternalAgentExecutionCandidate() = (%#v, %v), want permanent publication-finalization rejection", candidate, err)
				}
				return
			}
			if err != nil || candidate == nil {
				t.Fatalf("resolveExternalAgentExecutionCandidate() = (%#v, %v), want write candidate", candidate, err)
			}
		})
	}
}

func TestACPDispatcherUsesRegisteredExternalMCPPolicy(t *testing.T) {
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{"web_search"}
	fixture := newExternalACPDispatchFixtureWithPolicy(t, "external-v2", policy)
	queued := fixture.queueTask(t, "external-policy", types.UID("external-policy-task-uid"), "search", nil)
	completed := fixture.dispatch(t, queued)
	if completed.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("completed external Task status = %#v", completed.Status)
	}
	select {
	case request := <-fixture.createRequests:
		got := request.MCPConfiguration.ToolPolicy
		if !slices.Equal(got.AllowedToolNames, policy.AllowedTools) ||
			!slices.Equal(got.DisallowedToolNames, policy.DisallowedTools) || got.AllowBash != policy.AllowBash {
			t.Fatalf("external MCP policy = %#v, want %#v", got, policy)
		}
		if len(got.Tools) != 1 || got.Tools[0].Name != "web_search" || !got.Tools[0].Source.Brokered() {
			t.Fatalf("external MCP descriptors = %#v", got.Tools)
		}
		if request.MCPConfiguration.ToolPolicyDigest != fixture.runtime.Spec.Capabilities.Profile.ToolPolicyDigest {
			t.Fatalf("external tool policy digest = %q, want %q", request.MCPConfiguration.ToolPolicyDigest, fixture.runtime.Spec.Capabilities.Profile.ToolPolicyDigest)
		}
	default:
		t.Fatal("external runtime did not receive CreateRuntimeSession")
	}
}

func TestExternalRuntimeBindingRejectsUnconformedMCPToolDescriptorChange(t *testing.T) {
	const toolName = "external_lookup"
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{toolName}
	fixture := newExternalACPDispatchFixtureWithPolicy(t, "external-v2", policy, testExternalACPCustomTool(toolName))
	conformedDigest := fixture.runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest

	tool := &corev1alpha1.Tool{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKey{Namespace: defaultNS, Name: toolName}, tool); err != nil {
		t.Fatal(err)
	}
	tool.Spec.Description = "look up a changed value"
	tool.Generation++
	if err := fixture.client.Update(fixture.ctx, tool); err != nil {
		t.Fatal(err)
	}

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNS, Name: "external-unconformed-descriptor", UID: types.UID("external-unconformed-descriptor-uid"), Generation: 1,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: fixture.agent.Name},
			Prompt: "look up a value", AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{toolName}},
		},
	}
	_, err := fixture.reconciler.resolveExternalAgentExecutionCandidate(fixture.ctx, task, fixture.agent)
	if err == nil || !strings.Contains(err.Error(), "MCP tool descriptors have not passed current conformance") {
		t.Fatalf("resolveExternalAgentExecutionCandidate() error = %v, want unconformed descriptor rejection", err)
	}
	if fixture.runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest != conformedDigest {
		t.Fatal("binding test unexpectedly changed the last conformed descriptor digest")
	}
}

func TestACPDispatcherRejectsFrozenMCPToolDescriptorsAfterReprobe(t *testing.T) {
	const toolName = "external_lookup"
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{toolName}
	fixture := newExternalACPDispatchFixtureWithPolicy(t, "external-v2", policy, testExternalACPCustomTool(toolName))
	queued := fixture.queueTask(t, "external-descriptor-race", types.UID("external-descriptor-race-uid"), "look up", nil)
	frozenDigest := fixture.runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest

	tool := &corev1alpha1.Tool{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKey{Namespace: defaultNS, Name: toolName}, tool); err != nil {
		t.Fatal(err)
	}
	tool.Spec.Description = "look up a changed value"
	tool.Generation++
	if err := fixture.client.Update(fixture.ctx, tool); err != nil {
		t.Fatal(err)
	}
	if currentDigest := updateExternalACPObservedDescriptorDigest(t, fixture); currentDigest == frozenDigest {
		t.Fatal("changed Tool did not produce a new conformed descriptor digest")
	}

	reserved, target, err := fixture.dispatcher.reserveTask(fixture.ctx, queued.DeepCopy())
	if err != nil {
		t.Fatalf("reserveTask() error = %v, want terminal settlement", err)
	}
	if reserved != nil {
		t.Fatalf("reserveTask() returned reserved Task %#v after descriptor conformance drift", reserved)
	}
	if target.pool != nil || target.external != nil || target.reservation != nil {
		t.Fatalf("reserveTask() target = %#v, want empty target after terminal settlement", target)
	}
	if fixture.createCalls.Load() != 0 {
		t.Fatalf("external runtime received %d CreateRuntimeSession requests with stale frozen descriptors", fixture.createCalls.Load())
	}

	attemptID, err := promptAttemptIDFromTask(queued)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.ExecutionState != store.PromptExecutionFailed || attempt.TerminalReason != "InvalidRuntimeProfile" ||
		!strings.HasPrefix(attempt.LastOperationID, "external-runtime-binding-drift-") ||
		!strings.Contains(attempt.OutcomeMarker, "MCP tool descriptors do not match current conformance") {
		t.Fatalf("PromptAttempt settlement = %#v", attempt)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseFailed || current.Status.Execution == nil ||
		current.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		current.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeFailed ||
		current.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") ||
		!strings.Contains(current.Status.Execution.Message, "MCP tool descriptors do not match current conformance") {
		t.Fatalf("Task settlement = %#v", current.Status)
	}
}

func TestACPQueueStoresLongExternalRuntimeNameInAnnotation(t *testing.T) {
	runtimeName := strings.Repeat("a", 32) + "." + strings.Repeat("b", 32)
	if errs := k8svalidation.IsDNS1123Subdomain(runtimeName); len(errs) != 0 {
		t.Fatalf("test AgentRuntime name %q is not a valid DNS subdomain: %v", runtimeName, errs)
	}
	if errs := k8svalidation.IsValidLabelValue(runtimeName); len(errs) == 0 {
		t.Fatalf("test AgentRuntime name %q unexpectedly fits in a label value", runtimeName)
	}
	fixture := newExternalACPDispatchFixtureWithRuntimeName(t, runtimeName)
	queued := fixture.queueTask(t, "external-long-runtime", types.UID("external-long-runtime-task-uid"), "queue", nil)
	if queued.Status.Execution == nil || queued.Status.Execution.AgentRuntimeName != runtimeName {
		t.Fatalf("queued external runtime identity = %#v, want name %q", queued.Status.Execution, runtimeName)
	}
}

func TestExternalRuntimeHTTPClientLeavesPromptStreamDeadlineToContext(t *testing.T) {
	transport := http.DefaultTransport
	httpClient := externalRuntimeHTTPClient(transport)
	if httpClient.Timeout != 0 {
		t.Fatalf("external runtime HTTP client timeout = %v, want no whole-stream timeout", httpClient.Timeout)
	}
	if httpClient.Transport != transport {
		t.Fatalf("external runtime HTTP transport = %T, want supplied dial-controlled transport", httpClient.Transport)
	}
	if httpClient.CheckRedirect == nil || httpClient.CheckRedirect(&http.Request{}, nil) != http.ErrUseLastResponse {
		t.Fatal("external runtime HTTP client did not reject redirects")
	}
}

func TestBuildPromptRequestHonorsExternalLeaseBounds(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-lease-bounds", types.UID("external-lease-bounds-uid"), "bounded", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	observed := fixture.runtime.Status.ObservedCapabilities
	fence := harnessv2.Fence{
		RuntimeInstanceID:          harnessv2.RuntimeInstanceID(observed.RuntimeInstanceID),
		SupervisorBootID:           harnessv2.SupervisorBootID(observed.SupervisorBootID),
		ControllerEpoch:            uint64(observed.ControllerEpoch),
		RuntimePoolUID:             harnessv2.RuntimePoolUID(observed.RuntimePoolUID),
		RuntimePoolGeneration:      uint64(observed.RuntimePoolGeneration),
		RuntimeProfileDigest:       bound.plan.Digest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		RuntimeSessionUID:          "external-lease-bounds-session",
		RuntimeSessionGeneration:   1,
	}
	tests := []struct {
		name     string
		minimum  int64
		maximum  int64
		expected time.Duration
	}{
		{name: "maximum below preferred duration", minimum: 5_000, maximum: 30_000, expected: 30 * time.Second},
		{name: "minimum above preferred duration", minimum: 100_000, maximum: 120_000, expected: 100 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := harnessv2.DefaultProtocolLimits()
			limits.MinPromptLeaseMillis = test.minimum
			limits.MaxPromptLeaseMillis = test.maximum
			request, err := fixture.dispatcher.buildPromptRequest(
				bound.frozenTask, fence, bound.plan.Profile, bound.mcpConfiguration, "", "bounded", limits, 0,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := request.Lease.ExpiresAt.Sub(request.Lease.IssuedAt); got != test.expected {
				t.Fatalf("prompt lease duration = %s, want %s", got, test.expected)
			}
			if request.Metadata.ExpiresAt.After(request.Lease.ExpiresAt) ||
				request.MCPAuthorization.ExpiresAt.After(request.Lease.ExpiresAt) {
				t.Fatal("prompt request authority outlived the bounded lease")
			}
		})
	}
}

func TestValidateExternalRuntimeCapabilitiesAcceptsProviderAndModelSupersets(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	runtimeClient, err := harnessv2.NewClient(fixture.runtime.Spec.Deployment.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := runtimeClient.Capabilities(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	capabilities.Provider.ProviderKinds = append(capabilities.Provider.ProviderKinds, "another-provider")
	capabilities.Provider.Models = append(capabilities.Provider.Models, "another-model")
	if _, _, err := validateExternalRuntimeCapabilities(fixture.runtime, capabilities, false); err != nil {
		t.Fatalf("provider/model capability supersets were rejected: %v", err)
	}
}

func TestValidateExternalRuntimeCapabilitiesRequiresPermissionsOnlyWhenPolicyNeedsThem(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	runtimeClient, err := harnessv2.NewClient(fixture.runtime.Spec.Deployment.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := runtimeClient.Capabilities(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Provider.SupportsPermissions {
		t.Fatal("test runtime unexpectedly advertises permission support")
	}
	if _, _, err := validateExternalRuntimeCapabilities(fixture.runtime, capabilities, false); err != nil {
		t.Fatalf("approval-free runtime without permission support was rejected: %v", err)
	}
	if _, _, err := validateExternalRuntimeCapabilities(fixture.runtime, capabilities, true); err == nil ||
		!strings.Contains(err.Error(), "provider capability drifted") {
		t.Fatalf("permission-required runtime validation error = %v", err)
	}
}

func TestValidateExternalRuntimeCapabilitiesRejectsAgentSessionConfiguration(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	runtimeClient, err := harnessv2.NewClient(fixture.runtime.Spec.Deployment.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := runtimeClient.Capabilities(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	capabilities.SupportsAgentSessionConfiguration = true
	if _, _, err := validateExternalRuntimeCapabilities(fixture.runtime, capabilities, false); err == nil ||
		!strings.Contains(err.Error(), "Agent session configuration capability drifted") {
		t.Fatalf("validateExternalRuntimeCapabilities() error = %v, want Agent session configuration drift rejection", err)
	}
}

func TestACPDispatcherExternalRecoveryUsesReconformedCredentialsAfterSecretRotation(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-recovery-credential-rotation", types.UID("external-recovery-credential-rotation-uid"), "recover", nil)
	_, _, runtimeFence := createExternalRuntimeSessionForRecovery(t, fixture, queued, "credential-rotation")
	rotateExternalRuntimeCredentials(t, fixture, true)

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	complete, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err != nil || !complete {
		t.Fatalf("cleanup after credential reconformance = complete:%t err:%v, want complete", complete, err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("external recovery DELETE calls = %d, want 1", fixture.deleteCalls.Load())
	}
	select {
	case deleted := <-fixture.deleteRequests:
		if mismatch := harnessv2.CompareFence(runtimeFence, deleted.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
			t.Fatalf("external recovery DELETE fence mismatch = %s; got %#v want %#v", mismatch, deleted.Metadata.Fence, runtimeFence)
		}
	default:
		t.Fatal("external recovery did not capture its DELETE request")
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if !taskScopedRuntimeSessionCleanupComplete(current) {
		t.Fatalf("external recovery cleanup receipt = %q, want complete", current.Status.Execution.RuntimeSessionCleanupDigest)
	}
}

func TestACPDispatcherExternalRecoveryRejectsUnconformedCredentialRotation(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-recovery-unconformed-rotation", types.UID("external-recovery-unconformed-rotation-uid"), "recover", nil)
	createExternalRuntimeSessionForRecovery(t, fixture, queued, "unconformed-credential-rotation")
	rotateExternalRuntimeCredentials(t, fixture, false)

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	complete, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err == nil || complete || !strings.Contains(err.Error(), "rotated cleanup authentication has not been observed") {
		t.Fatalf("cleanup before credential reconformance = complete:%t err:%v, want fail-closed rejection", complete, err)
	}
	if fixture.deleteCalls.Load() != 0 {
		t.Fatalf("external recovery DELETE calls = %d, want 0", fixture.deleteCalls.Load())
	}
	if current.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf("external recovery cleanup receipt = %q, want empty", current.Status.Execution.RuntimeSessionCleanupDigest)
	}
}

func TestACPDispatcherExternalReconformedCredentialCleanupRevalidatesBeforeMutation(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-recovery-credential-race", types.UID("external-recovery-credential-race-uid"), "recover", nil)
	currentTask, bound, runtimeFence := createExternalRuntimeSessionForRecovery(t, fixture, queued, "credential-race")
	rotateExternalRuntimeCredentials(t, fixture, true)

	currentRuntime := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), currentRuntime); err != nil {
		t.Fatal(err)
	}
	cleanupClient, cleanupFence, err := fixture.dispatcher.externalRuntimeCleanupClient(
		fixture.ctx, currentRuntime, bound.body.ExternalRuntime, bound.plan.Digest, bound.body.ExternalRuntime.Limits,
		runtimeFence.RuntimeInstanceID, runtimeFence.SupervisorBootID, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	cleanupFence.RuntimeSessionUID = runtimeFence.RuntimeSessionUID
	cleanupFence.RuntimeSessionGeneration = runtimeFence.RuntimeSessionGeneration

	currentRuntime.Status.Ready = false
	if err := fixture.client.Status().Update(fixture.ctx, currentRuntime); err != nil {
		t.Fatal(err)
	}
	err = fixture.dispatcher.deleteRuntimeSession(
		fixture.ctx, cleanupClient, harnessv2.RuntimeSessionID(runtimeSessionID(cleanupFence)),
		currentTask, cleanupFence, "credential_revalidation_race",
	)
	if err == nil || !strings.Contains(err.Error(), "rotated cleanup authentication has not been observed") {
		t.Fatalf("cleanup after conformance proof changed error = %v, want pre-mutation rejection", err)
	}
	if fixture.deleteCalls.Load() != 0 {
		t.Fatalf("external runtime received %d DELETE calls after conformance proof changed, want 0", fixture.deleteCalls.Load())
	}
}

func TestACPDispatcherExternalRecoveryDeletesFrozenSessionAfterDescriptorReprobe(t *testing.T) {
	const toolName = "external_lookup"
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{toolName}
	fixture := newExternalACPDispatchFixtureWithPolicy(t, "external-v2", policy, testExternalACPCustomTool(toolName))
	queued := fixture.queueTask(t, "external-recovery-reprobe", types.UID("external-recovery-reprobe-uid"), "recover", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "recovery-reprobe", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RuntimeInstanceID = string(metadata.Fence.RuntimeInstanceID)
	current.Status.Execution.RuntimeSessionUID = string(metadata.Fence.RuntimeSessionUID)
	current.Status.Execution.RuntimeSessionGeneration = int64(metadata.Fence.RuntimeSessionGeneration)
	current.Status.Execution.RuntimeSessionSupervisorBootID = string(metadata.Fence.SupervisorBootID)
	current.Status.Execution.RuntimeSessionProfileDigest = string(metadata.Fence.RuntimeProfileDigest)
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}

	frozenDigest := bound.mcpConfiguration.ToolPolicy.DescriptorDigest
	tool := &corev1alpha1.Tool{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKey{Namespace: defaultNS, Name: toolName}, tool); err != nil {
		t.Fatal(err)
	}
	tool.Spec.Description = "look up a changed value"
	tool.Generation++
	if err := fixture.client.Update(fixture.ctx, tool); err != nil {
		t.Fatal(err)
	}
	if currentDigest := updateExternalACPObservedDescriptorDigest(t, fixture); currentDigest == frozenDigest {
		t.Fatal("changed Tool did not produce a new conformed descriptor digest")
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}

	ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err != nil || !ready {
		t.Fatalf("cleanup after descriptor reprobe = ready %t, error %v", ready, err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("external recovery DELETE calls = %d, want 1", fixture.deleteCalls.Load())
	}
	select {
	case deleted := <-fixture.deleteRequests:
		if mismatch := harnessv2.CompareFence(metadata.Fence, deleted.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
			t.Fatalf("external recovery DELETE fence mismatch = %s; got %#v want %#v", mismatch, deleted.Metadata.Fence, metadata.Fence)
		}
	default:
		t.Fatal("external recovery did not capture its DELETE request")
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if !taskScopedRuntimeSessionCleanupComplete(current) {
		t.Fatalf("external recovery cleanup receipt = %q, want complete", current.Status.Execution.RuntimeSessionCleanupDigest)
	}
}

func TestACPDispatcherExternalRecoveryWaitsForCompleteObservedIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1alpha1.AgentRuntime)
	}{
		{
			name: "missing observation",
			mutate: func(runtime *corev1alpha1.AgentRuntime) {
				runtime.Status.ObservedCapabilities = nil
			},
		},
		{
			name: "capabilities observed before runtime status",
			mutate: func(runtime *corev1alpha1.AgentRuntime) {
				runtime.Status.ObservedCapabilities.RuntimeInstanceID = ""
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExternalACPDispatchFixture(t)
			task := fixture.queueTask(t, "external-recovery", types.UID("external-recovery-task-uid"), "recover", nil)

			current := &corev1alpha1.Task{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
				t.Fatal(err)
			}
			current.Status.Execution.RuntimeInstanceID = fixture.runtime.Status.ObservedCapabilities.RuntimeInstanceID
			current.Status.Execution.RuntimeSessionUID = "external-recovery-session-uid"
			current.Status.Execution.RuntimeSessionGeneration = 1
			if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
				t.Fatal(err)
			}

			runtime := &corev1alpha1.AgentRuntime{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtime); err != nil {
				t.Fatal(err)
			}
			runtime.Status.Ready = false
			test.mutate(runtime)
			if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
				t.Fatal(err)
			}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
				t.Fatal(err)
			}

			ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				t.Fatal("external recovery reported cleanup complete without a complete authenticated runtime identity")
			}
			if fixture.deleteCalls.Load() != 0 {
				t.Fatalf("external recovery issued %d DELETE calls without a complete authenticated runtime identity", fixture.deleteCalls.Load())
			}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Execution.RuntimeSessionCleanupDigest != "" {
				t.Fatalf("external recovery recorded cleanup digest %q without a complete authenticated runtime identity", current.Status.Execution.RuntimeSessionCleanupDigest)
			}
		})
	}
}

func TestACPDispatcherExternalRecoveryWaitsForExactBootCleanupProof(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(
		t, "external-recovery-boot-drift", types.UID("external-recovery-boot-drift-uid"), "recover", nil,
	)
	current, _, _ := createExternalRuntimeSessionForRecovery(t, fixture, queued, "recovery-boot-drift")

	runtime := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Status.ObservedCapabilities.SupervisorBootID = "replacement-boot"
	if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}

	ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("external recovery reported cleanup complete after same-runtime supervisor boot drift")
	}
	if fixture.deleteCalls.Load() != 0 {
		t.Fatalf("external recovery issued %d DELETE calls against the replacement boot", fixture.deleteCalls.Load())
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf(
			"external recovery recorded cleanup digest %q from supervisor boot drift",
			current.Status.Execution.RuntimeSessionCleanupDigest,
		)
	}
}

func TestACPDispatcherExternalRecoveryUsesFrozenAuthorityAfterObservedEndpointRotation(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	task := fixture.queueTask(t, "external-recovery-drift", types.UID("external-recovery-drift-uid"), "recover", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, task, task.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "recovery-cleanup-drift", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RuntimeInstanceID = string(metadata.Fence.RuntimeInstanceID)
	current.Status.Execution.RuntimeSessionUID = string(metadata.Fence.RuntimeSessionUID)
	current.Status.Execution.RuntimeSessionGeneration = int64(metadata.Fence.RuntimeSessionGeneration)
	current.Status.Execution.RuntimeSessionSupervisorBootID = string(metadata.Fence.SupervisorBootID)
	current.Status.Execution.RuntimeSessionProfileDigest = string(metadata.Fence.RuntimeProfileDigest)
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}

	var replacementCalls atomic.Int32
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		replacementCalls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer replacement.Close()
	runtime := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Spec.Deployment.Endpoint = replacement.URL
	runtime.Generation++
	if err := fixture.client.Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Status.ObservedGeneration = runtime.Generation
	runtime.Status.ObservedCapabilities.RuntimeInstanceID = "replacement-runtime-instance"
	runtime.Status.ObservedCapabilities.SupervisorBootID = "replacement-supervisor-boot"
	runtime.Status.ObservedCapabilities.RuntimePoolUID = "replacement-runtime-pool"
	runtime.Status.ObservedCapabilities.RuntimePoolGeneration++
	if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}

	ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err != nil || !ready {
		t.Fatalf("recovery with drifted live registration = ready %t, error %v, want complete", ready, err)
	}
	if replacementCalls.Load() != 0 {
		t.Fatalf("recovery contacted replacement endpoint %d times, want 0", replacementCalls.Load())
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("recovery issued %d DeleteRuntimeSession calls through frozen endpoint, want 1", fixture.deleteCalls.Load())
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if !taskScopedRuntimeSessionCleanupComplete(current) {
		t.Fatalf("recovery cleanup receipt = %q, want complete", current.Status.Execution.RuntimeSessionCleanupDigest)
	}
}

func TestValidateExternalRuntimeRotatedEndpointCleanupStatusRejectsFenceDrift(t *testing.T) {
	expected := harnessv2.Fence{
		RuntimeInstanceID: "frozen-runtime-instance", SupervisorBootID: "frozen-supervisor-boot",
		ControllerEpoch: 11, RuntimePoolUID: "frozen-runtime-pool",
		RuntimePoolGeneration: 7, RuntimeProfileDigest: "frozen-profile-digest",
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	baseline := harnessv2.StatusResponse{Fence: expected}
	if err := validateExternalRuntimeRotatedEndpointCleanupStatus(expected, &baseline); err != nil {
		t.Fatalf("exact frozen cleanup status rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*harnessv2.StatusResponse)
	}{
		{name: "runtime instance", mutate: func(status *harnessv2.StatusResponse) {
			status.Fence.RuntimeInstanceID = "replacement-runtime-instance"
		}},
		{name: "supervisor boot", mutate: func(status *harnessv2.StatusResponse) { status.Fence.SupervisorBootID = "replacement-supervisor-boot" }},
		{name: "controller epoch", mutate: func(status *harnessv2.StatusResponse) { status.Fence.ControllerEpoch++ }},
		{name: "runtime pool UID", mutate: func(status *harnessv2.StatusResponse) { status.Fence.RuntimePoolUID = "replacement-runtime-pool" }},
		{name: "runtime pool generation", mutate: func(status *harnessv2.StatusResponse) { status.Fence.RuntimePoolGeneration++ }},
		{name: "runtime profile", mutate: func(status *harnessv2.StatusResponse) {
			status.Fence.RuntimeProfileDigest = "replacement-profile-digest"
		}},
		{name: "profile digest schema", mutate: func(status *harnessv2.StatusResponse) { status.Fence.ProfileDigestSchemaVersion++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := baseline
			test.mutate(&status)
			if err := validateExternalRuntimeRotatedEndpointCleanupStatus(expected, &status); err == nil {
				t.Fatal("drifted frozen cleanup status was accepted")
			}
		})
	}
}

func TestACPDispatcherExternalRecoveryFailsClosedWhenAgentRuntimeIsMissing(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	task := fixture.queueTask(t, "external-recovery-missing", types.UID("external-recovery-missing-uid"), "recover", nil)
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RuntimeInstanceID = fixture.runtime.Status.ObservedCapabilities.RuntimeInstanceID
	current.Status.Execution.RuntimeSessionUID = "external-recovery-missing-session"
	current.Status.Execution.RuntimeSessionGeneration = 1
	current.Status.Execution.RuntimeSessionSupervisorBootID = fixture.runtime.Status.ObservedCapabilities.SupervisorBootID
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Delete(fixture.ctx, fixture.runtime); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}

	ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err == nil || ready || !strings.Contains(err.Error(), "load external AgentRuntime for RuntimeSession cleanup") {
		t.Fatalf("cleanup with missing AgentRuntime = ready %t, error %v, want fail-closed read error", ready, err)
	}
	if fixture.deleteCalls.Load() != 0 {
		t.Fatalf("cleanup issued %d DeleteRuntimeSession calls without AgentRuntime authority, want 0", fixture.deleteCalls.Load())
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf("cleanup recorded receipt %q without AgentRuntime authority", current.Status.Execution.RuntimeSessionCleanupDigest)
	}
}

func TestACPDispatcherExternalRecoveryHandlesReplacementWithoutObservedCapabilities(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	task := fixture.queueTask(t, "external-recovery-replacement", types.UID("external-recovery-replacement-uid"), "recover", nil)
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Execution.RuntimeInstanceID = fixture.runtime.Status.ObservedCapabilities.RuntimeInstanceID
	current.Status.Execution.RuntimeSessionUID = "external-recovery-replacement-session"
	current.Status.Execution.RuntimeSessionGeneration = 1
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}

	replacement := fixture.runtime.DeepCopy()
	if err := fixture.client.Delete(fixture.ctx, fixture.runtime); err != nil {
		t.Fatal(err)
	}
	replacement.ResourceVersion = ""
	replacement.UID = types.UID("external-runtime-replacement-uid")
	replacement.Status = corev1alpha1.AgentRuntimeStatus{}
	if err := fixture.client.Create(fixture.ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}

	ready, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("replacement AgentRuntime without observed capabilities did not complete obsolete cleanup")
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(current), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Execution.RuntimeSessionCleanupDigest == "" {
		t.Fatal("replacement AgentRuntime cleanup did not record its completion receipt")
	}
}

func TestACPDispatcherExternalRuntimeDriftFailsBeforeRuntimeMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *externalACPDispatchFixture)
	}{
		{name: "runtime UID", mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
			current := &corev1alpha1.AgentRuntime{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
				t.Fatal(err)
			}
			current.UID = types.UID("replacement-runtime-uid")
			if err := fixture.client.Update(fixture.ctx, current); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "runtime generation", mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
			current := &corev1alpha1.AgentRuntime{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
				t.Fatal(err)
			}
			current.Generation++
			if err := fixture.client.Update(fixture.ctx, current); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "observed profile", mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
			current := &corev1alpha1.AgentRuntime{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
				t.Fatal(err)
			}
			current.Status.ObservedCapabilities.RuntimeProfileDigest = testControllerDigest("drifted-profile")
			if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "authentication", mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
			secret := &corev1.Secret{}
			key := client.ObjectKey{Namespace: defaultNS, Name: fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name}
			if err := fixture.client.Get(fixture.ctx, key, secret); err != nil {
				t.Fatal(err)
			}
			secret.Data[fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Key] = []byte(strings.Repeat("r", 32))
			if err := fixture.client.Update(fixture.ctx, secret); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "controller fence", mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
			current := &corev1alpha1.AgentRuntime{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
				t.Fatal(err)
			}
			current.Status.ObservedCapabilities.ControllerEpoch++
			if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExternalACPDispatchFixture(t)
			queued := fixture.queueTask(t, "external-drift", types.UID("external-drift-uid"), "do not mutate", nil)
			test.mutate(t, fixture)
			reserved, target, err := fixture.dispatcher.reserveTask(fixture.ctx, queued.DeepCopy())
			if err == nil && reserved != nil {
				_ = fixture.dispatcher.executeReservedTask(fixture.ctx, reserved, target)
			}
			if fixture.createCalls.Load() != 0 {
				t.Fatalf("external runtime received %d mutating requests after %s drift", fixture.createCalls.Load(), test.name)
			}
		})
	}
}

func TestACPDispatcherFrozenExternalRuntimeBindingDriftIsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *externalACPDispatchFixture)
		want   string
	}{
		{
			name: "runtime generation",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
				current := &corev1alpha1.AgentRuntime{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
					t.Fatal(err)
				}
				current.Generation++
				if err := fixture.client.Update(fixture.ctx, current); err != nil {
					t.Fatal(err)
				}
			},
			want: "identity or generation changed after binding",
		},
		{
			name: "authentication Secret resource version",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
				secret := &corev1.Secret{}
				key := client.ObjectKey{Namespace: defaultNS, Name: fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name}
				if err := fixture.client.Get(fixture.ctx, key, secret); err != nil {
					t.Fatal(err)
				}
				if err := fixture.client.Delete(fixture.ctx, secret); err != nil {
					t.Fatal(err)
				}
				secret.ResourceVersion = ""
				secret.UID = types.UID("replacement-runtime-auth-uid")
				if err := fixture.client.Create(fixture.ctx, secret); err != nil {
					t.Fatal(err)
				}
			},
			want: "authentication authority changed after binding",
		},
		{
			name: "observed capabilities cleared",
			mutate: func(t *testing.T, fixture *externalACPDispatchFixture) {
				current := &corev1alpha1.AgentRuntime{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
					t.Fatal(err)
				}
				current.Status.ObservedCapabilities = nil
				if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
					t.Fatal(err)
				}
			},
			want: "MCP tool descriptors do not match current conformance",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExternalACPDispatchFixture(t)
			queued := fixture.queueTask(t, "external-terminal-drift", types.UID("external-terminal-drift-uid"), "do not mutate", nil)
			test.mutate(t, fixture)

			reserved, _, err := fixture.dispatcher.reserveTask(fixture.ctx, queued.DeepCopy())
			if err != nil {
				t.Fatalf("reserveTask() error = %v, want terminal settlement", err)
			}
			if reserved != nil {
				t.Fatalf("reserveTask() returned reserved Task %#v after immutable binding drift", reserved)
			}
			if fixture.createCalls.Load() != 0 {
				t.Fatalf("external runtime received %d mutating requests", fixture.createCalls.Load())
			}

			attemptID, err := promptAttemptIDFromTask(queued)
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if attempt.ExecutionState != store.PromptExecutionFailed || attempt.TerminalReason != "InvalidRuntimeProfile" ||
				!strings.Contains(attempt.OutcomeMarker, test.want) {
				t.Fatalf("PromptAttempt settlement = %#v", attempt)
			}

			current := &corev1alpha1.Task{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase != corev1alpha1.TaskPhaseFailed || current.Status.Execution == nil ||
				current.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
				current.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") ||
				!strings.Contains(current.Status.Execution.Message, test.want) {
				t.Fatalf("Task settlement = %#v", current.Status)
			}
		})
	}
}

func TestACPDispatcherExternalRuntimeAuthorityDriftAfterInitialReadsFailsBeforeMutation(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-between-mutation-drift", types.UID("external-between-mutation-drift-uid"), "do not mutate", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}

	current := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
		t.Fatal(err)
	}
	current.Status.ObservedCapabilities.AdapterName = "drifted-adapter"
	if err := fixture.client.Status().Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}

	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "authority-drift", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}

	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err == nil ||
		!strings.Contains(err.Error(), "registration or observed authority changed before mutation") {
		t.Fatalf("CreateRuntimeSession() error = %v, want mutation-authority drift rejection", err)
	}
	if fixture.createCalls.Load() != 0 {
		t.Fatalf("external runtime received %d mutations after authority drift", fixture.createCalls.Load())
	}
}

func TestACPDispatcherDeletingExternalRuntimeRejectsAdmissionAndAllowsCleanup(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	queued := fixture.queueTask(t, "external-runtime-deleting", types.UID("external-runtime-deleting-uid"), "do not mutate", nil)
	bound, err := fixture.reconciler.loadVerifiedBoundExecution(
		fixture.ctx, queued, queued.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient, runtimeFence, profile, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, fixture.runtime.DeepCopy(), bound.mcpConfiguration, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, workspace, err := emptyRuntimeWorkspace(bound.frozenTask, "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := mutationMetadata(runtimeFence, bound.frozenTask, "deleting-runtime", false, time.Now().UTC().Add(30*time.Second))
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
		RuntimeSessionID: harnessv2.RuntimeSessionID(runtimeSessionID(metadata.Fence)),
		Profile:          profile, MCPConfiguration: bound.mcpConfiguration, Workspace: workspace,
	}
	if err := sealMutation(&request.Metadata.RequestDigest, request); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err != nil {
		t.Fatal(err)
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external runtime session creation calls = %d, want 1", fixture.createCalls.Load())
	}

	active := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(queued), active); err != nil {
		t.Fatal(err)
	}
	active.Status.Phase = corev1alpha1.TaskPhaseRunning
	active.Status.Execution.State = corev1alpha1.TaskExecutionStateRunning
	active.Status.Execution.RuntimeInstanceID = string(metadata.Fence.RuntimeInstanceID)
	active.Status.Execution.RuntimeSessionUID = string(metadata.Fence.RuntimeSessionUID)
	active.Status.Execution.RuntimeSessionGeneration = int64(metadata.Fence.RuntimeSessionGeneration)
	active.Status.Execution.RuntimeSessionSupervisorBootID = string(metadata.Fence.SupervisorBootID)
	active.Status.Execution.RuntimeSessionProfileDigest = string(metadata.Fence.RuntimeProfileDigest)
	if err := fixture.client.Status().Update(fixture.ctx, active); err != nil {
		t.Fatal(err)
	}

	current := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
		t.Fatal(err)
	}
	current.Finalizers = append(current.Finalizers, "test.orka.ai/hold-deletion")
	if err := fixture.client.Update(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Delete(fixture.ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), current); err != nil {
		t.Fatal(err)
	}
	if current.DeletionTimestamp.IsZero() {
		t.Fatal("AgentRuntime did not enter finalizer-held deletion")
	}
	if _, err := fixture.reconciler.loadVerifiedBoundExecutionForActiveSession(
		fixture.ctx, active, active.Status.AgentExecutionBinding,
	); err != nil {
		t.Fatalf("active-session binding verification for deleting AgentRuntime: %v", err)
	}
	if _, _, _, _, err := fixture.dispatcher.externalRuntimeClient(
		fixture.ctx, current.DeepCopy(), bound.mcpConfiguration, true,
	); err == nil || !strings.Contains(err.Error(), "is deleting and cannot accept new sessions") {
		t.Fatalf("new admission for deleting AgentRuntime error = %v, want deletion rejection", err)
	}

	if _, err := runtimeClient.CreateRuntimeSession(fixture.ctx, request); err == nil ||
		!strings.Contains(err.Error(), "is deleting and cannot accept new sessions") {
		t.Fatalf("pre-mutation admission for deleting AgentRuntime error = %v, want deletion rejection", err)
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external runtime received %d total session creations, want only the pre-deletion session", fixture.createCalls.Load())
	}
	complete, err := fixture.dispatcher.cleanupRecoveredTaskScopedRuntimeSession(fixture.ctx, active)
	if err != nil || !complete {
		t.Fatalf("active-session recovery cleanup = complete:%t err:%v, want complete", complete, err)
	}
	if fixture.deleteCalls.Load() != 1 {
		t.Fatalf("external runtime cleanup calls = %d, want 1", fixture.deleteCalls.Load())
	}
	select {
	case deleted := <-fixture.deleteRequests:
		if mismatch := harnessv2.CompareFence(metadata.Fence, deleted.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
			t.Fatalf("active-session recovery DELETE fence mismatch = %s; got %#v want %#v", mismatch, deleted.Metadata.Fence, metadata.Fence)
		}
	default:
		t.Fatal("active-session recovery did not capture its DELETE request")
	}
}

func TestACPDispatcherExternalSessionContinuationUsesRuntimeRefLineage(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	first := fixture.queueTask(t, "external-session-1", types.UID("external-session-task-1"), "first", &corev1alpha1.SessionReference{
		Name: "conversation", Create: true, Append: true,
	})
	firstCompleted := fixture.dispatch(t, first)
	second := fixture.queueTask(t, "external-session-2", types.UID("external-session-task-2"), "second", &corev1alpha1.SessionReference{
		Name: "conversation", Append: true,
	})
	secondCompleted := fixture.dispatch(t, second)
	if firstCompleted.Status.Execution == nil || secondCompleted.Status.Execution == nil ||
		firstCompleted.Status.Execution.RuntimeSessionUID == "" ||
		firstCompleted.Status.Execution.RuntimeSessionUID != secondCompleted.Status.Execution.RuntimeSessionUID ||
		firstCompleted.Status.Execution.RuntimeSessionGeneration != secondCompleted.Status.Execution.RuntimeSessionGeneration {
		t.Fatalf("external continuation did not reuse RuntimeSession: first=%#v second=%#v", firstCompleted.Status.Execution, secondCompleted.Status.Execution)
	}
	if fixture.createCalls.Load() != 1 {
		t.Fatalf("external continuation CreateRuntimeSession calls = %d, want 1", fixture.createCalls.Load())
	}
	control, err := fixture.controlStore.GetSessionControl(fixture.ctx, defaultNS, "conversation")
	if err != nil {
		t.Fatal(err)
	}
	wantRuntimeIdentity := "runtimeRef:" + string(fixture.runtime.UID)
	if control.Lineage == nil || control.Lineage.ContractVersion != string(corev1alpha1.AgentRuntimeContractHarnessV2) ||
		control.Lineage.RuntimeIdentity != wantRuntimeIdentity {
		t.Fatalf("external Session lineage = %#v, want runtime identity %q", control.Lineage, wantRuntimeIdentity)
	}
}

func TestExternalRuntimeFrozenCapabilityEnvelopeRejectsEveryLiveDriftClass(t *testing.T) {
	profile, governance, limits := testAgentRuntimeProfileClaimsAndLimits()
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	base := func() harnessv2.CapabilitiesResponse {
		return harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests: map[string]string{profile.ProviderKind: profile.AdapterDigests[profile.ProviderKind]},
			Limits:         limits,
			Provider: harnessv2.ProviderCapabilities{
				ProviderKinds:  []string{profile.ProviderKind, "another-provider"},
				Models:         []string{profile.Model, "another-model"},
				SupportsCancel: true, SupportsPermissions: true, SupportsTools: true,
			},
			WorkspaceGovernance:               governance,
			SupportsDrain:                     true,
			SupportsPublicationFinalization:   true,
			SupportsAgentSessionConfiguration: false,
		}
	}
	baseline := base()
	expected, err := canonicalExternalRuntimeCapabilities(&baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFrozenExternalRuntimeCapabilities(expected, &baseline); err != nil {
		t.Fatalf("unchanged capability envelope rejected: %v", err)
	}
	reordered := base()
	slices.Reverse(reordered.Provider.ProviderKinds)
	slices.Reverse(reordered.Provider.Models)
	if err := validateFrozenExternalRuntimeCapabilities(expected, &reordered); err != nil {
		t.Fatalf("reordered provider capability sets were rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*harnessv2.CapabilitiesResponse)
	}{
		{name: "adapter digests", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.AdapterDigests[profile.ProviderKind] = testControllerDigest("adapter-drift")
		}},
		{name: "limits", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.Limits.MaxTerminalResultBytes--
		}},
		{name: "provider", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.Provider.SupportsImages = true
		}},
		{name: "provider kinds", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.Provider.ProviderKinds[1] = "changed-provider"
		}},
		{name: "models", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.Provider.Models[1] = "changed-model"
		}},
		{name: "governance", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.WorkspaceGovernance.CancellationSettlement = false
		}},
		{name: "supports drain", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.SupportsDrain = false
		}},
		{name: "supports publication finalization", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.SupportsPublicationFinalization = false
		}},
		{name: "profile schema", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.ProfileDigestSchemaVersion++
		}},
		{name: "supports Agent session configuration", mutate: func(value *harnessv2.CapabilitiesResponse) {
			value.SupportsAgentSessionConfiguration = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := base()
			test.mutate(&current)
			if err := validateFrozenExternalRuntimeCapabilities(expected, &current); err == nil {
				t.Fatal("drifted capability envelope was accepted")
			}
		})
	}
}
