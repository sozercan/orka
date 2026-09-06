package tools

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/transactiontoken"
)

const (
	brokerTestNamespace            = "team-a"
	brokerTestAgentName            = "agent-a"
	brokerMemoryProposalType       = "memory"
	brokerSkillProposalType        = "skill"
	brokerProposalStatusPending    = "pending"
	brokerDelegateTransactionScope = "orka:agents:delegate"
)

type brokerMemoryStore struct {
	memoryFilter     store.MemoryFilter
	memories         []store.Memory
	proposals        []store.MemoryProposal
	transcriptFilter store.TranscriptSearchFilter
	transcript       []store.TranscriptSearchResult
}

func (s *brokerMemoryStore) ListMemories(_ context.Context, filter store.MemoryFilter) ([]store.Memory, error) {
	s.memoryFilter = filter
	return append([]store.Memory(nil), s.memories...), nil
}

func (s *brokerMemoryStore) CreateMemoryProposal(_ context.Context, proposal *store.MemoryProposal) error {
	s.proposals = append(s.proposals, *proposal)
	proposal.ID = "proposal-id"
	proposal.Status = brokerProposalStatusPending
	return nil
}

func (s *brokerMemoryStore) SearchTranscript(_ context.Context, filter store.TranscriptSearchFilter) ([]store.TranscriptSearchResult, error) {
	s.transcriptFilter = filter
	return append([]store.TranscriptSearchResult(nil), s.transcript...), nil
}

func TestRegisterBrokeredCoordinationToolsIsIdempotentAndBounded(t *testing.T) {
	registry := NewRegistry()
	k8sClient := newFakeClient()

	if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
		t.Fatalf("RegisterBrokeredCoordinationTools() error = %v", err)
	}
	first := registry.Names()
	if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
		t.Fatalf("second RegisterBrokeredCoordinationTools() error = %v", err)
	}
	if got := registry.Names(); !slices.Equal(got, first) {
		t.Fatalf("second registration changed names: first=%v second=%v", first, got)
	}

	want := []string{
		checkMessagesToolName,
		delegateTaskToolName,
		"propose_memory",
		"recall_memory",
		"remember",
		RunValidationToolName,
		"search_transcript",
		sendMessageToolName,
		waitForTasksToolName,
	}
	slices.Sort(want)
	if !slices.Equal(first, want) {
		t.Fatalf("registered broker tools = %v, want %v", first, want)
	}

	for _, rejected := range []string{
		codeExecToolName,
		fileReadToolName,
		fileWriteToolName,
		createContainerTaskToolName,
		cancelTaskToolName,
		mergePullRequestToolName,
		autoMergePullRequestToolName,
		reviewPullRequestToolName,
		postReviewCommentToolName,
		createAgentToolName,
		deleteAgentToolName,
		updatePlanToolName,
	} {
		if _, ok := registry.Get(rejected); ok {
			t.Fatalf("unsafe or unsupported broker tool %q was registered", rejected)
		}
	}
}

func TestBrokeredMemoryToolsUseRequestScopedStores(t *testing.T) {
	t.Setenv(envOrkaControllerURL, "http://controller-env-must-not-be-used.invalid")
	t.Setenv(envOrkaTaskNamespace, "wrong-namespace")
	t.Setenv(envOrkaTaskName, "wrong-task")

	memoryStore := &brokerMemoryStore{
		memories: []store.Memory{{ID: "memory-1", Namespace: brokerTestNamespace, Content: "bounded context"}},
		transcript: []store.TranscriptSearchResult{{
			SessionName: "older-session", MessageID: 7, Role: "assistant", Snippet: "bounded context",
		}},
	}
	ctx := WithToolContext(context.Background(), &ToolContext{
		Brokered:             true,
		Namespace:            brokerTestNamespace,
		TaskID:               testTaskAName,
		AgentName:            brokerTestAgentName,
		MemoryReader:         memoryStore,
		MemoryProposalWriter: memoryStore,
		TranscriptSearcher:   memoryStore,
	})

	recalled, err := NewRecallMemoryTool().Execute(ctx, json.RawMessage(`{"query":"bounded","tags":["policy"]}`))
	if err != nil {
		t.Fatalf("recall_memory Execute() error = %v", err)
	}
	if !strings.Contains(recalled, `"id":"memory-1"`) {
		t.Fatalf("recall_memory result = %s", recalled)
	}
	if memoryStore.memoryFilter.Namespace != brokerTestNamespace || memoryStore.memoryFilter.Query != "bounded" || !slices.Equal(memoryStore.memoryFilter.Tags, []string{"policy"}) {
		t.Fatalf("recall filter = %#v", memoryStore.memoryFilter)
	}

	if _, err := NewRememberMemoryTool().Execute(ctx, json.RawMessage(`{"content":"Remember request-scoped stores."}`)); err != nil {
		t.Fatalf("remember Execute() error = %v", err)
	}
	if _, err := NewProposeMemoryTool().Execute(ctx, json.RawMessage(`{"title":"Add broker guidance","content":"Use request context."}`)); err != nil {
		t.Fatalf("propose_memory Execute() error = %v", err)
	}
	if len(memoryStore.proposals) != 2 {
		t.Fatalf("proposal count = %d, want 2", len(memoryStore.proposals))
	}
	for _, proposal := range memoryStore.proposals {
		if proposal.Namespace != brokerTestNamespace || proposal.TaskName != testTaskAName || proposal.AgentName != brokerTestAgentName {
			t.Fatalf("proposal provenance = %#v", proposal)
		}
	}
	if memoryStore.proposals[0].Type != brokerMemoryProposalType || memoryStore.proposals[1].Type != brokerSkillProposalType {
		t.Fatalf("proposal types = %q, %q", memoryStore.proposals[0].Type, memoryStore.proposals[1].Type)
	}

	searched, err := NewSearchTranscriptTool().Execute(ctx, json.RawMessage(`{"query":"bounded","roles":["assistant"]}`))
	if err != nil {
		t.Fatalf("search_transcript Execute() error = %v", err)
	}
	if !strings.Contains(searched, `"sessionName":"older-session"`) {
		t.Fatalf("search_transcript result = %s", searched)
	}
	if memoryStore.transcriptFilter.Namespace != brokerTestNamespace || memoryStore.transcriptFilter.ExcludeSessionName != testTaskAName || !slices.Equal(memoryStore.transcriptFilter.Roles, []string{"assistant"}) {
		t.Fatalf("transcript filter = %#v", memoryStore.transcriptFilter)
	}
}

func TestBrokeredMemoryToolFailsClosedWithoutRequestStore(t *testing.T) {
	t.Setenv(envOrkaControllerURL, "http://controller-env-must-not-be-used.invalid")
	t.Setenv(envOrkaTaskNamespace, "wrong-namespace")
	t.Setenv(envOrkaTaskName, "wrong-task")

	ctx := WithToolContext(context.Background(), &ToolContext{
		Brokered:  true,
		Namespace: brokerTestNamespace,
		TaskID:    testTaskAName,
	})
	_, err := NewRecallMemoryTool().Execute(ctx, json.RawMessage(`{"query":"anything"}`))
	if err == nil || !strings.Contains(err.Error(), "brokered memory reader is not configured") {
		t.Fatalf("recall_memory error = %v", err)
	}
}

func TestBrokeredDelegateTaskUsesRequestScopedParentContext(t *testing.T) {
	t.Setenv(envOrkaTaskName, "wrong-parent")
	t.Setenv(envOrkaTaskNamespace, "wrong-namespace")
	t.Setenv(envOrkaCoordinationDepth, "9")
	t.Setenv(envOrkaCoordinationAllowedAgents, "wrong-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "1")

	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "parent-task", Namespace: brokerTestNamespace, UID: types.UID("parent-uid"),
			Annotations: map[string]string{labels.AnnotationCoordinationDepth: "1"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: testCoordinatorTaskName},
		},
	}
	coordinator := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: testCoordinatorTaskName, Namespace: brokerTestNamespace},
		Spec: corev1alpha1.AgentSpec{Coordination: &corev1alpha1.CoordinationConfig{
			Enabled: true, MaxDepth: 3,
			AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "researcher"}},
		}},
	}
	researcher := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "researcher", Namespace: brokerTestNamespace}}
	baseClient := newFakeClient(parent, coordinator, researcher)
	baseWithWatch, ok := baseClient.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	k8sClient := interceptor.NewClient(baseWithWatch, interceptor.Funcs{
		Create: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if child, ok := object.(*corev1alpha1.Task); ok && child.UID == "" {
				child.UID = types.UID("delegated-child-uid")
			}
			return delegate.Create(ctx, object, options...)
		},
	})
	registry := NewRegistry()
	if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
		t.Fatal(err)
	}

	ctx := WithToolContext(context.Background(), &ToolContext{
		Brokered:    true,
		Namespace:   brokerTestNamespace,
		TaskID:      "parent-task",
		TaskUID:     "parent-uid",
		SessionID:   "runtime-session-uid",
		OperationID: "delegate-operation",
	})
	result, err := registry.Execute(ctx, delegateTaskToolName, json.RawMessage(`{"agent":"researcher","prompt":"Investigate"}`))
	if err != nil {
		t.Fatalf("delegate_task Execute() error = %v", err)
	}
	var delegated DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegated); err != nil {
		t.Fatalf("decode delegate result: %v", err)
	}
	child := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: brokerTestNamespace, Name: delegated.TaskName}, child); err != nil {
		t.Fatalf("get child task: %v", err)
	}
	if child.Annotations[labels.AnnotationCoordinationDepth] != "2" {
		t.Fatalf("child depth = %q, want 2", child.Annotations[labels.AnnotationCoordinationDepth])
	}
	if got := labels.ParentTaskName(child.Labels, child.Annotations); got != "parent-task" {
		t.Fatalf("child parent = %q, want parent-task", got)
	}
	if got := child.Annotations[labels.AnnotationParentTaskUID]; got != string(parent.UID) {
		t.Fatalf("child authenticated parent UID = %q, want %q", got, parent.UID)
	}
	effectID, err := (store.ExternalEffectIdentity{
		Kind: "acp-mcp-tool", Namespace: brokerTestNamespace,
		AggregateID: "runtime-session-uid", OperationID: "delegate-operation",
	}).CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if got := child.Annotations[labels.AnnotationDelegationEffectID]; got != effectID {
		t.Fatalf("child delegation effect = %q, want %q", got, effectID)
	}
	if delegated.TaskUID != string(child.UID) || delegated.ParentTaskUID != string(parent.UID) {
		t.Fatalf("delegation receipt identity = child %q parent %q, want %q/%q", delegated.TaskUID, delegated.ParentTaskUID, child.UID, parent.UID)
	}
}

type recordingBrokeredDelegateExchanger struct {
	requests []contexttoken.ExchangeRequest
	token    string
	err      error
}

func (e *recordingBrokeredDelegateExchanger) Exchange(_ context.Context, request contexttoken.ExchangeRequest) (string, error) {
	e.requests = append(e.requests, request)
	return e.token, e.err
}

func brokeredDelegateFixtures(transactional bool) (*corev1alpha1.Task, *corev1alpha1.Agent, *corev1alpha1.Agent) {
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "parent-task", Namespace: brokerTestNamespace, UID: types.UID("parent-uid"),
			Annotations: map[string]string{labels.AnnotationCoordinationDepth: "0"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: testCoordinatorTaskName},
		},
	}
	if transactional {
		parent.Spec.Transaction = &corev1alpha1.TaskTransaction{
			Profile: transactiontoken.ProfileName,
			ID:      parentTransactionID,
			Scope:   brokerDelegateTransactionScope + " " + childTransactionScope,
			Scopes:  []string{brokerDelegateTransactionScope, childTransactionScope},
		}
	}
	coordinator := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: testCoordinatorTaskName, Namespace: brokerTestNamespace},
		Spec: corev1alpha1.AgentSpec{Coordination: &corev1alpha1.CoordinationConfig{
			Enabled: true, MaxDepth: 3,
			AllowedAgents: []corev1alpha1.AllowedAgent{
				{Name: "researcher"},
				{Name: "researcher", Namespace: "other-team"},
			},
		}},
	}
	researcher := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "researcher", Namespace: brokerTestNamespace}}
	return parent, coordinator, researcher
}

func TestBrokeredDelegateTaskEnforcesNamespaceScopeBeforeResourceAccess(t *testing.T) {
	tests := []struct {
		name          string
		toolContext   ToolContext
		arguments     string
		wantErrorPart string
	}{
		{
			name: "watch namespace rejects child Task namespace",
			toolContext: ToolContext{
				WatchNamespace: brokerTestNamespace,
			},
			arguments:     `{"agent":"researcher","agentNamespace":"team-a","taskNamespace":"other-team","prompt":"Investigate"}`,
			wantErrorPart: `child Task namespace "other-team" is outside watched namespace "team-a"`,
		},
		{
			name: "watch namespace rejects target Agent namespace",
			toolContext: ToolContext{
				WatchNamespace: brokerTestNamespace,
			},
			arguments:     `{"agent":"researcher","agentNamespace":"other-team","taskNamespace":"team-a","prompt":"Investigate"}`,
			wantErrorPart: `target Agent namespace "other-team" is outside watched namespace "team-a"`,
		},
		{
			name: "namespace isolation rejects child Task namespace",
			toolContext: ToolContext{
				EnforceNamespaceIsolation: true,
			},
			arguments:     `{"agent":"researcher","agentNamespace":"team-a","taskNamespace":"other-team","prompt":"Investigate"}`,
			wantErrorPart: `child Task namespace "other-team" is outside request namespace "team-a"`,
		},
		{
			name: "namespace isolation rejects target Agent namespace",
			toolContext: ToolContext{
				EnforceNamespaceIsolation: true,
			},
			arguments:     `{"agent":"researcher","agentNamespace":"other-team","taskNamespace":"team-a","prompt":"Investigate"}`,
			wantErrorPart: `target Agent namespace "other-team" is outside request namespace "team-a"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, coordinator, _ := brokeredDelegateFixtures(false)
			k8sClient := newFakeClient(parent, coordinator)
			registry := NewRegistry()
			if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
				t.Fatal(err)
			}
			tt.toolContext.Brokered = true
			tt.toolContext.Namespace = brokerTestNamespace
			tt.toolContext.TaskID = parent.Name
			tt.toolContext.TaskUID = string(parent.UID)
			ctx := WithToolContext(context.Background(), &tt.toolContext)

			_, err := registry.Execute(ctx, delegateTaskToolName, json.RawMessage(tt.arguments))
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorPart) {
				t.Fatalf("delegate_task error = %v, want containing %q", err, tt.wantErrorPart)
			}
			if strings.Contains(err.Error(), "failed to get agent") {
				t.Fatalf("delegate_task loaded the target Agent before enforcing namespace scope: %v", err)
			}
			tasks := &corev1alpha1.TaskList{}
			if err := k8sClient.List(context.Background(), tasks); err != nil {
				t.Fatal(err)
			}
			if len(tasks.Items) != 1 || tasks.Items[0].Name != parent.Name {
				t.Fatalf("unexpected Task mutation after namespace rejection: %#v", tasks.Items)
			}
		})
	}
}

func TestBrokeredDelegateTaskUsesRegisteredControllerTTSConfiguration(t *testing.T) {
	// Worker-only environment must not influence brokered execution.
	t.Setenv("ORKA_CONTEXT_TOKEN_TTS_ENDPOINT", "http://worker-env-must-not-be-used.invalid/token")
	t.Setenv("ORKA_CONTEXT_TOKEN_CHILD_SCOPE", "orka:admin")

	parent, coordinator, researcher := brokeredDelegateFixtures(true)
	k8sClient := newFakeClient(parent, coordinator, researcher)
	registry := NewRegistry()
	if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
		t.Fatal(err)
	}
	exchanger := &recordingBrokeredDelegateExchanger{token: "child-transaction-token"}
	resolverCalls := 0
	if err := RegisterBrokeredDelegateTaskTool(registry, k8sClient, BrokeredDelegateTaskTransactionExchangeConfig{
		TTS: contexttoken.TTSConfig{
			Endpoint: "https://controller-tts.example.test/oauth/token", TokenSource: contexttoken.TTSTokenSourceIncoming,
			ChildTokenTTL: 2 * time.Minute,
		},
		Exchanger:        exchanger,
		SubjectTokenType: transactiontoken.SubjectTokenTypeTransactionToken,
		ChildScope:       childTransactionScope,
		ResolveSubjectToken: func(_ context.Context, gotParent *corev1alpha1.Task, tokenSource string) (string, error) {
			resolverCalls++
			if gotParent.Name != parent.Name || gotParent.UID != parent.UID {
				t.Fatalf("resolver parent = %s/%s uid=%s, want %s/%s uid=%s", gotParent.Namespace, gotParent.Name, gotParent.UID, parent.Namespace, parent.Name, parent.UID)
			}
			if tokenSource != contexttoken.TTSTokenSourceIncoming {
				t.Fatalf("resolver token source = %q, want incoming", tokenSource)
			}
			return parentTransactionToken, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx := WithToolContext(context.Background(), &ToolContext{
		Brokered: true, Namespace: brokerTestNamespace, TaskID: parent.Name, TaskUID: string(parent.UID),
	})
	result, err := registry.Execute(ctx, delegateTaskToolName, json.RawMessage(`{"agent":"researcher","prompt":"Investigate"}`))
	if err != nil {
		t.Fatalf("delegate_task Execute() error = %v", err)
	}
	if resolverCalls != 1 {
		t.Fatalf("subject-token resolver calls = %d, want 1", resolverCalls)
	}
	if len(exchanger.requests) != 1 {
		t.Fatalf("TTS exchange requests = %d, want 1", len(exchanger.requests))
	}
	request := exchanger.requests[0]
	if request.SubjectToken != parentTransactionToken || request.Scope != childTransactionScope || request.RequestedTTL != 2*time.Minute {
		t.Fatalf("TTS exchange request = %#v", request)
	}
	if request.SubjectTokenType != transactiontoken.SubjectTokenTypeTransactionToken {
		t.Fatalf("subject token type = %q", request.SubjectTokenType)
	}

	var delegated DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegated); err != nil {
		t.Fatal(err)
	}
	child := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: brokerTestNamespace, Name: delegated.TaskName}, child); err != nil {
		t.Fatal(err)
	}
	secretName := child.Annotations[labels.AnnotationTransactionTokenSecret]
	if secretName == "" {
		t.Fatal("delegated child is missing transaction-token Secret annotation")
	}
	secret := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: brokerTestNamespace, Name: secretName}, secret); err != nil {
		t.Fatal(err)
	}
	if got := string(secret.Data["token"]); got != "child-transaction-token" {
		t.Fatalf("child transaction token = %q, want exchanged token", got)
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != child.UID {
		t.Fatalf("child transaction-token Secret owner = %#v, want child uid %s", secret.OwnerReferences, child.UID)
	}
}

func TestBrokeredDelegateTaskFailsClosedWithoutRegisteredTTSConfiguration(t *testing.T) {
	parent, coordinator, researcher := brokeredDelegateFixtures(true)
	k8sClient := newFakeClient(parent, coordinator, researcher)
	registry := NewRegistry()
	if err := RegisterBrokeredCoordinationTools(registry, k8sClient); err != nil {
		t.Fatal(err)
	}
	ctx := WithToolContext(context.Background(), &ToolContext{
		Brokered: true, Namespace: brokerTestNamespace, TaskID: parent.Name, TaskUID: string(parent.UID),
	})

	_, err := registry.Execute(ctx, delegateTaskToolName, json.RawMessage(`{"agent":"researcher","prompt":"Investigate"}`))
	if err == nil || !strings.Contains(err.Error(), "transaction-token exchange configuration is not registered") {
		t.Fatalf("delegate_task error = %v, want missing brokered TTS configuration", err)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 1 || tasks.Items[0].Name != parent.Name {
		t.Fatalf("unexpected Task mutation after brokered TTS failure: %#v", tasks.Items)
	}
}
