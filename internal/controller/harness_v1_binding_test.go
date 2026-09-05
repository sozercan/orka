package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

type harnessV1CandidateErrorReader struct {
	client.Reader
	get func(client.ObjectKey, client.Object) error
}

type harnessV1SessionControlSequenceStore struct {
	store.DurableControlStore
	controls []*store.SessionControl
	reads    int
}

type harnessV1LineageControlStore struct {
	store.DurableControlStore
	lineages map[string]*store.SessionLineage
}

func (s *harnessV1LineageControlStore) GetSessionControl(
	ctx context.Context,
	namespace, sessionName string,
) (*store.SessionControl, error) {
	control, err := s.DurableControlStore.GetSessionControl(ctx, namespace, sessionName)
	if err != nil {
		return nil, err
	}
	if lineage := s.lineages[namespace+"\x00"+sessionName]; lineage != nil {
		copyLineage := *lineage
		control.Lineage = &copyLineage
	}
	return control, nil
}

func (s *harnessV1SessionControlSequenceStore) GetSessionControl(
	ctx context.Context,
	namespace, sessionName string,
) (*store.SessionControl, error) {
	if s.reads >= len(s.controls) {
		return s.DurableControlStore.GetSessionControl(ctx, namespace, sessionName)
	}
	control := s.controls[s.reads]
	s.reads++
	if control == nil {
		return nil, store.ErrNotFound
	}
	copyControl := *control
	if control.Lease != nil {
		copyLease := *control.Lease
		copyControl.Lease = &copyLease
	}
	return &copyControl, nil
}

type harnessV1BindingResponseLostClient struct {
	client.Client
	lost bool
}

func (c *harnessV1BindingResponseLostClient) Status() client.SubResourceWriter {
	return &harnessV1BindingResponseLostWriter{
		SubResourceWriter: c.Client.Status(),
		parent:            c,
	}
}

type harnessV1BindingResponseLostWriter struct {
	client.SubResourceWriter
	parent *harnessV1BindingResponseLostClient
}

func (w *harnessV1BindingResponseLostWriter) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.SubResourcePatchOption,
) error {
	if err := w.SubResourceWriter.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	task, ok := object.(*corev1alpha1.Task)
	if !ok || task.Status.AgentExecutionBinding == nil || w.parent.lost {
		return nil
	}
	w.parent.lost = true
	return errors.New("simulated binding status response loss")
}

func (r *harnessV1CandidateErrorReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if r.get != nil {
		if err := r.get(key, object); err != nil {
			return err
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func TestHarnessV1TaskMatchesBindingSnapshot(t *testing.T) {
	deletionTime := metav1.Now()
	boundSpec := corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent, Prompt: "bound prompt",
		Transaction: &corev1alpha1.TaskTransaction{
			ID: "txn-1", Scopes: []string{"orka:secrets:credentials:read"},
		},
	}
	boundDigest, err := harnessV1TaskSpecDigest(boundSpec)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name              string
		generation        int64
		deleting          bool
		allowDeleting     bool
		legacySnapshot    bool
		mutatePrompt      bool
		mutateTransaction bool
		want              bool
	}{
		{name: "bound generation", generation: 7, want: true},
		{name: "bound generation with legacy snapshot", generation: 7, legacySnapshot: true, want: true},
		{name: "non-deleting generation change", generation: 8},
		{name: "deletion transition not authorized", generation: 8, deleting: true},
		{name: "single deletion generation increment", generation: 8, deleting: true, allowDeleting: true, want: true},
		{name: "deletion transition with changed prompt", generation: 8, deleting: true, allowDeleting: true, mutatePrompt: true},
		{name: "deletion transition with changed transaction", generation: 8, deleting: true, allowDeleting: true, mutateTransaction: true},
		{name: "deletion transition with legacy snapshot", generation: 8, deleting: true, allowDeleting: true, legacySnapshot: true},
		{name: "multiple generation increments", generation: 9, deleting: true, allowDeleting: true},
		{name: "stale generation", generation: 6, deleting: true, allowDeleting: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Generation: test.generation},
				Spec:       *boundSpec.DeepCopy(),
			}
			if test.deleting {
				task.DeletionTimestamp = &deletionTime
			}
			if test.mutatePrompt {
				task.Spec.Prompt = "changed prompt"
			}
			if test.mutateTransaction {
				task.Spec.Transaction.ID = "txn-2"
			}
			binding := &corev1alpha1.AgentExecutionBinding{
				Task: corev1alpha1.AgentExecutionBindingTaskRef{BoundSpecGeneration: 7},
			}
			body := agentExecutionSnapshotBody{HarnessV1: &agentExecutionSnapshotHarnessV1{TaskSpecDigest: boundDigest}}
			if test.legacySnapshot {
				body.HarnessV1.TaskSpecDigest = ""
			}
			if got := harnessV1TaskMatchesBindingSnapshot(task, binding, body, test.allowDeleting); got != test.want {
				t.Fatalf("binding snapshot match = %t, want %t", got, test.want)
			}
		})
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsTransactionOutsideBrokeredMode(t *testing.T) {
	fixture := newHarnessV1CandidateFixture(t)
	task := fixture.task.DeepCopy()
	task.Spec.Transaction = &corev1alpha1.TaskTransaction{
		ID: "txn-1", Scopes: []string{"orka:secrets:credentials:read"},
	}
	_, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(context.Background(), task, fixture.agent)
	if err == nil || !isPermanentHarnessV1CandidateError(err) ||
		!strings.Contains(err.Error(), "requires brokered tool execution") {
		t.Fatalf("non-brokered transaction error = %v, want permanent brokered-only rejection", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateDefaultsMaxTurnsTo50(t *testing.T) {
	fixture := newHarnessV1CandidateFixture(t)
	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(
		context.Background(), fixture.task.DeepCopy(), fixture.agent,
	)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	if body.Configuration.MaxTurns != 50 {
		t.Fatalf("MaxTurns = %d, want built-in harness v1 default 50", body.Configuration.MaxTurns)
	}
}

func TestEnsureHarnessV1ExecutionBindingRequeuesTransientCandidateErrors(t *testing.T) {
	tests := []struct {
		name      string
		intercept func(client.Object) bool
	}{
		{
			name: "wrapper auth Secret API read",
			intercept: func(object client.Object) bool {
				_, ok := object.(*corev1.Secret)
				return ok
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newHarnessV1CandidateFixture(t)
			transientErr := errors.New("temporary Kubernetes API outage")
			fixture.reconciler.APIReader = &harnessV1CandidateErrorReader{
				Reader: fixture.reconciler.Client,
				get: func(_ client.ObjectKey, object client.Object) error {
					if test.intercept(object) {
						return transientErr
					}
					return nil
				},
			}

			result, err, handled := fixture.reconciler.ensureHarnessV1ExecutionBinding(
				ctx, fixture.task.DeepCopy(), fixture.agent,
			)
			if err != nil || !handled || result.RequeueAfter != 5*time.Second {
				t.Fatalf("ensure binding = result=%#v handled=%v err=%v, want five-second requeue", result, handled, err)
			}
			current := &corev1alpha1.Task{}
			if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase == corev1alpha1.TaskPhaseFailed || current.Status.AgentExecutionBinding != nil {
				t.Fatalf("transient resolution error terminalized or bound Task: %#v", current.Status)
			}
		})
	}
}

func TestEnsureHarnessV1ExecutionBindingRecoversCommittedPatchAfterResponseLoss(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	baseClient := fixture.reconciler.Client
	lostClient := &harnessV1BindingResponseLostClient{Client: baseClient}
	fixture.reconciler.Client = lostClient

	_, err, handled := fixture.reconciler.ensureHarnessV1ExecutionBinding(
		ctx, fixture.task.DeepCopy(), fixture.agent,
	)
	if err != nil || handled {
		t.Fatalf("ensure binding ambiguity recovery = handled=%v err=%v", handled, err)
	}
	if !lostClient.lost {
		t.Fatal("test client did not simulate a committed status patch with a lost response")
	}
	bound := &corev1alpha1.Task{}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(fixture.task), bound); err != nil {
		t.Fatal(err)
	}
	if bound.Status.AgentExecutionBinding == nil {
		t.Fatal("committed binding was not recovered after the response was lost")
	}
}

func TestResolveHarnessV1ExecutionCandidateMarksSpecViolationPermanent(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	task := fixture.task.DeepCopy()
	task.Spec.Workspace = &corev1alpha1.WorkspaceConfig{}

	_, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if err == nil || !isPermanentHarnessV1CandidateError(err) {
		t.Fatalf("workspace violation error = %v, permanent=%v", err, isPermanentHarnessV1CandidateError(err))
	}
}

func TestResolveHarnessV1ExecutionCandidateFreezesBoundedSessionTranscript(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const sessionName = "continued-session"
	seedHarnessV1BindingTranscript(t, ctx, fixture, sessionName, defaultACPSessionType, []store.SessionMessage{
		{ID: "message-1", Role: "user", Content: "old request"},
		{ID: "message-2", Role: "assistant", Content: "old response"},
		{ID: "message-3", Role: "user", Content: "recent request"},
	})
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: sessionName, Append: true, MaxMessages: 2,
	}
	control, err := fixture.controls.GetSessionControl(ctx, task.Namespace, sessionName)
	if err != nil {
		t.Fatal(err)
	}
	sequence := &harnessV1SessionControlSequenceStore{
		DurableControlStore: fixture.controls,
		controls:            []*store.SessionControl{control, control},
	}
	fixture.reconciler.DurableControlStore = sequence

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	bootstrap := body.HarnessV1.SessionBootstrap
	if bootstrap == nil || bootstrap.SchemaVersion != harnessV1SessionBootstrapSchemaVersion ||
		bootstrap.SessionUID != control.SessionUID || bootstrap.ControlVersion != control.Version ||
		bootstrap.LeaseGeneration != control.LeaseGeneration || bootstrap.MessageCount != 2 ||
		bootstrap.TotalMessages != 2 || sequence.reads != 2 {
		t.Fatalf("frozen Session bootstrap = %#v, want bounded two-message suffix", bootstrap)
	}
	if body.Prompt != task.Spec.Prompt || strings.Contains(bootstrap.Artifact, "old request") ||
		!strings.Contains(bootstrap.Artifact, "old response") || !strings.Contains(bootstrap.Artifact, "recent request") {
		t.Fatalf("frozen Session input = prompt %q bootstrap %q", body.Prompt, bootstrap.Artifact)
	}
	if err := validateFrozenHarnessV1SessionInput(body); err != nil {
		t.Fatalf("validate frozen Session input: %v", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsSessionControlRace(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const sessionName = "racing-session"
	seedHarnessV1BindingTranscript(t, ctx, fixture, sessionName, defaultACPSessionType, []store.SessionMessage{
		{ID: "message-1", Role: "user", Content: "stable request"},
	})
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Append: true}
	control, err := fixture.controls.GetSessionControl(ctx, task.Namespace, sessionName)
	if err != nil {
		t.Fatal(err)
	}
	advanced := *control
	advanced.Version++
	fixture.reconciler.DurableControlStore = &harnessV1SessionControlSequenceStore{
		DurableControlStore: fixture.controls,
		controls:            []*store.SessionControl{control, &advanced},
	}

	if _, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent); err == nil ||
		!errors.Is(err, store.ErrConflict) {
		t.Fatalf("Session control race error = %v, want ErrConflict", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateKeepsReplacementRaceTransient(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const sessionName = "replaced-session"
	seedHarnessV1BindingTranscript(t, ctx, fixture, sessionName, defaultACPSessionType, []store.SessionMessage{
		{ID: "message-1", Role: "user", Content: "stable request"},
	})
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Append: true}
	control, err := fixture.controls.GetSessionControl(ctx, task.Namespace, sessionName)
	if err != nil {
		t.Fatal(err)
	}
	replacement := *control
	replacement.SessionUID = "replacement-session-uid"
	replacement.Lineage = nil
	fixture.reconciler.DurableControlStore = &harnessV1SessionControlSequenceStore{
		DurableControlStore: fixture.controls,
		controls:            []*store.SessionControl{control, &replacement},
	}

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if candidate != nil || err == nil || !errors.Is(err, store.ErrConflict) || isPermanentHarnessV1CandidateError(err) {
		t.Fatalf("replacement race candidate = %#v, error = %v, want transient ErrConflict", candidate, err)
	}
}

func TestResolveHarnessV1ExecutionCandidateFreezesNewSessionUID(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: "new-session", Create: true, Append: true,
	}

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	bootstrap := body.HarnessV1.SessionBootstrap
	if bootstrap == nil || bootstrap.SchemaVersion != harnessV1SessionBootstrapSchemaVersion ||
		strings.TrimSpace(bootstrap.SessionUID) == "" || bootstrap.ControlVersion != 0 ||
		bootstrap.LeaseGeneration != 0 || bootstrap.MessageCount != 0 || bootstrap.TotalMessages != 0 ||
		bootstrap.Artifact != "" {
		t.Fatalf("new Session bootstrap = %#v, want frozen UID at V0/G0 with empty transcript", bootstrap)
	}
	if err := validateFrozenHarnessV1SessionInput(body); err != nil {
		t.Fatalf("validate new Session bootstrap: %v", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsControlLessTranscriptAdoption(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const sessionName = "legacy-transcript"
	transcripts := fixture.reconciler.SessionManager.store
	now := time.Now().UTC()
	if err := transcripts.CreateSession(ctx, &store.SessionRecord{
		Namespace: fixture.task.Namespace, Name: sessionName, SessionType: defaultACPSessionType,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := transcripts.AppendMessages(ctx, fixture.task.Namespace, sessionName, []store.SessionMessage{
		{ID: "legacy-message", Role: "user", Content: "unclassified history", Timestamp: now},
	}); err != nil {
		t.Fatal(err)
	}
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Create: true, Append: true}

	if _, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent); err == nil ||
		!errors.Is(err, store.ErrConflict) {
		t.Fatalf("control-less transcript adoption error = %v, want ErrConflict", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsUnclassifiedExistingTranscript(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const sessionName = "unclassified-session"
	transcripts := fixture.reconciler.SessionManager.store
	now := time.Now().UTC()
	if err := transcripts.CreateSession(ctx, &store.SessionRecord{
		Namespace: fixture.task.Namespace, Name: sessionName, SessionType: defaultACPSessionType,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controls.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: fixture.task.Namespace, SessionName: sessionName,
		SessionUID:    "unclassified-session-uid",
		RequestDigest: store.CanonicalAgentExecutionSnapshotDigest([]byte("unclassified-session-control")),
		Availability:  store.SessionAvailable, CreatedAt: now, UpdatedAt: now,
	}, fixture.fence); err != nil {
		t.Fatal(err)
	}
	if err := transcripts.AppendMessages(ctx, fixture.task.Namespace, sessionName, []store.SessionMessage{{
		ID: "legacy-message", Role: "user", Content: "unclassified history", Timestamp: now,
	}}); err != nil {
		t.Fatal(err)
	}
	task := fixture.task.DeepCopy()
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Append: true}

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if candidate != nil || err == nil || !errors.Is(err, store.ErrConflict) || !isPermanentHarnessV1CandidateError(err) {
		t.Fatalf("unclassified Session candidate = %#v, error = %v, want permanent ErrConflict", candidate, err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsIncompatibleExistingLineage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*store.SessionLineage)
	}{
		{
			name: "v2 contract",
			mutate: func(lineage *store.SessionLineage) {
				lineage.ContractVersion = string(corev1alpha1.AgentRuntimeContractHarnessV2)
			},
		},
		{
			name: "different v1 configuration",
			mutate: func(lineage *store.SessionLineage) {
				lineage.ConfigDigest = store.CanonicalAgentExecutionSnapshotDigest([]byte("different-v1-configuration"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newHarnessV1CandidateFixture(t)
			const sessionName = "incompatible-lineage"
			seedHarnessV1BindingTranscript(t, ctx, fixture, sessionName, defaultACPSessionType, []store.SessionMessage{
				{ID: "message-1", Role: "user", Content: "existing request"},
			})
			lineage := fixture.lineageControls.lineages[fixture.task.Namespace+"\x00"+sessionName]
			test.mutate(lineage)
			task := fixture.task.DeepCopy()
			task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: sessionName, Append: true}

			candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
			if candidate != nil || err == nil || !errors.Is(err, store.ErrConflict) || !isPermanentHarnessV1CandidateError(err) {
				t.Fatalf("incompatible-lineage candidate = %#v, error = %v, want permanent ErrConflict", candidate, err)
			}
		})
	}
}

func TestResolveHarnessV1ExecutionCandidateFreezesPromptIncludedCutoff(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const (
		sessionName   = "gateway-session"
		currentPrompt = "answer the canonical gateway message"
	)
	throughMessageID := store.GatewayUserMessageID("event-current")
	seedHarnessV1BindingTranscript(t, ctx, fixture, sessionName, store.SessionTypeGateway, []store.SessionMessage{
		{ID: "gateway:prior:user", Role: "user", Content: "earlier request"},
		{ID: "gateway:prior:assistant", Role: "assistant", Content: "earlier response"},
		{ID: throughMessageID, Role: "user", Content: currentPrompt},
		{ID: store.GatewayUserMessageID("event-later"), Role: "user", Content: "later queued request"},
	})
	task := fixture.task.DeepCopy()
	task.Spec.Prompt = ""
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: sessionName, Append: false, MaxMessages: int32(store.GatewayTranscriptMessageLimit),
		ThroughMessageID: throughMessageID, PromptIncluded: true,
	}

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	bootstrap := body.HarnessV1.SessionBootstrap
	if body.Prompt != currentPrompt || bootstrap == nil || bootstrap.MessageCount != 2 || bootstrap.TotalMessages != 3 {
		t.Fatalf("frozen prompt-included Session input = prompt %q bootstrap %#v", body.Prompt, bootstrap)
	}
	if strings.Contains(bootstrap.Artifact, currentPrompt) || strings.Contains(bootstrap.Artifact, "later queued request") ||
		!strings.Contains(bootstrap.Artifact, "earlier response") {
		t.Fatalf("prompt-included bootstrap crossed its cutoff or duplicated the current prompt: %q", bootstrap.Artifact)
	}
	if err := validateFrozenHarnessV1SessionInput(body); err != nil {
		t.Fatalf("validate frozen prompt-included Session input: %v", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsMissingPromptCutoff(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	seedHarnessV1BindingTranscript(t, ctx, fixture, "gateway-session", store.SessionTypeGateway, nil)
	task := fixture.task.DeepCopy()
	task.Spec.Prompt = ""
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: "gateway-session", Append: false, MaxMessages: int32(store.GatewayTranscriptMessageLimit),
		ThroughMessageID: store.GatewayUserMessageID("missing"), PromptIncluded: true,
	}

	if _, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent); err == nil ||
		!errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing canonical prompt cutoff error = %v, want ErrNotFound", err)
	}
}

func TestResolveHarnessV1ExecutionCandidateRejectsCopilotPermanently(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	agent := fixture.agent.DeepCopy()
	agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeCopilot

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, fixture.task, agent)
	if candidate != nil || err == nil || !isPermanentHarnessV1CandidateError(err) ||
		!strings.Contains(err.Error(), "GitHub mutation-capable credential") {
		t.Fatalf("Copilot candidate = %#v, error = %v, want permanent safe-credential rejection", candidate, err)
	}
}

func TestResolveHarnessV1TargetRequiresSupportedToolMode(t *testing.T) {
	const (
		endpoint   = "https://runtime.example.invalid"
		runtimeKey = "token"
	)
	tests := []struct {
		name         string
		capabilities *corev1alpha1.AgentRuntimeObservedCapabilities
		wantError    string
	}{
		{name: "missing capabilities", wantError: "supported tool execution mode"},
		{
			name:         "missing modes",
			capabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{},
			wantError:    "supported tool execution mode",
		},
		{
			name: "incomplete brokered mode",
			capabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeBrokered,
				},
			},
			wantError: "requires continuation and brokered tool classes",
		},
		{
			name: "observed only",
			capabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeObserved,
				},
			},
		},
		{
			name: "brokered only",
			capabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeBrokered,
				},
				SupportsContinuation: true,
				BrokeredToolClasses: []corev1alpha1.AgentRuntimeBrokeredToolClass{
					corev1alpha1.AgentRuntimeBrokeredToolClassRead,
				},
			},
		},
		{
			name: "observed and brokered",
			capabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeObserved,
					corev1alpha1.AgentRuntimeToolExecutionModeBrokered,
				},
				SupportsContinuation: true,
				BrokeredToolClasses: []corev1alpha1.AgentRuntimeBrokeredToolClass{
					corev1alpha1.AgentRuntimeBrokeredToolClassRead,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default", Name: "external-v1", UID: types.UID("external-v1-uid"), Generation: 1,
				},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
					Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{
						Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: endpoint,
					},
					ClientAuth: corev1alpha1.AgentRuntimeClientAuth{
						BearerAuthRef: &corev1alpha1.AgentRuntimeBearerAuthReference{Name: "external-v1-auth", Key: runtimeKey},
					},
				},
				Status: corev1alpha1.AgentRuntimeStatus{
					Ready: true, ObservedGeneration: 1, ObservedAuthRefResourceVersion: "1",
					ObservedCapabilities: test.capabilities,
				},
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: runtime.Namespace, Name: runtime.Spec.ClientAuth.BearerAuthRef.Name,
					UID: types.UID("external-v1-auth-uid"), ResourceVersion: "1",
					Labels: map[string]string{
						agentRuntimeAuthUseLabel: scheduledRunLabelValue, agentRuntimeAuthRefNameLabel: runtime.Name,
					},
					Annotations: map[string]string{agentRuntimeAuthEndpointAnnotation: endpoint},
				},
				Data: map[string][]byte{runtimeKey: []byte("runtime-auth-value")},
			}
			reconciler, _ := newBindingTestReconciler(t, runtime, secret)
			task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: runtime.Namespace}}
			agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtime.Name},
			}}}

			target, err := reconciler.resolveHarnessV1Target(t.Context(), reconciler.Client, task, agent)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("resolve target = %#v, error = %v, want rejection containing %q", target, err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if target.runtimeRef == nil || target.runtimeRef.Name != runtime.Name ||
				target.backend != corev1alpha1.AgentExecutionBackendExternalEndpoint {
				t.Fatalf("resolved target = %#v, want external AgentRuntime %q", target, runtime.Name)
			}
			if !slices.Equal(target.toolExecutionModes, test.capabilities.ToolExecutionModes) ||
				target.supportsContinuation != test.capabilities.SupportsContinuation ||
				!slices.Equal(target.brokeredToolClasses, test.capabilities.BrokeredToolClasses) {
				t.Fatalf("resolved target capabilities = modes=%v continuation=%v classes=%v, want %#v",
					target.toolExecutionModes, target.supportsContinuation, target.brokeredToolClasses, test.capabilities)
			}
		})
	}
}

func TestResolveHarnessV1ExecutionCandidateFreezesBrokeredToolAuthority(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	const (
		runtimeName     = "external-brokered-v1"
		runtimeEndpoint = "https://runtime.example.invalid"
		runtimeAuthKey  = "token"
		toolName        = "lookup"
		toolEndpoint    = "https://tools.example.invalid/lookup"
	)

	runtimeAuth := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fixture.task.Namespace, Name: runtimeName + "-auth",
			UID: types.UID(runtimeName + "-auth-uid"),
			Labels: map[string]string{
				agentRuntimeAuthUseLabel: scheduledRunLabelValue, agentRuntimeAuthRefNameLabel: runtimeName,
			},
			Annotations: map[string]string{agentRuntimeAuthEndpointAnnotation: runtimeEndpoint},
		},
		Data: map[string][]byte{runtimeAuthKey: []byte("external-runtime-auth-value")},
	}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fixture.task.Namespace, Name: runtimeName,
			UID: types.UID(runtimeName + "-uid"), Generation: 1,
		},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
			Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{
				Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: runtimeEndpoint,
			},
			ClientAuth: corev1alpha1.AgentRuntimeClientAuth{BearerAuthRef: &corev1alpha1.AgentRuntimeBearerAuthReference{
				Name: runtimeAuth.Name, Key: runtimeAuthKey,
			}},
		},
		Status: corev1alpha1.AgentRuntimeStatus{
			Ready: true, ObservedGeneration: 1,
			ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				RuntimeName: runtimeName,
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeBrokered,
				},
				SupportsContinuation: true,
				BrokeredToolClasses: []corev1alpha1.AgentRuntimeBrokeredToolClass{
					corev1alpha1.AgentRuntimeBrokeredToolClassRead,
				},
			},
		},
	}
	parameters := []byte(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`)
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fixture.task.Namespace, Name: toolName,
			UID: types.UID(toolName + "-uid"), Generation: 1,
		},
		Spec: corev1alpha1.ToolSpec{
			Description: "look up a value", BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
			Parameters: &apiextensionsv1.JSON{Raw: parameters},
			HTTP:       &corev1alpha1.HTTPExecution{URL: toolEndpoint, Method: "POST"},
		},
	}
	if err := fixture.reconciler.Create(ctx, runtimeAuth); err != nil {
		t.Fatal(err)
	}
	currentRuntimeAuth := &corev1.Secret{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(runtimeAuth), currentRuntimeAuth); err != nil {
		t.Fatal(err)
	}
	runtime.Status.ObservedAuthRefResourceVersion = currentRuntimeAuth.ResourceVersion
	for _, object := range []client.Object{runtime, tool} {
		if err := fixture.reconciler.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}

	task := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), task); err != nil {
		t.Fatal(err)
	}
	task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{toolName}}
	task.Spec.Transaction = &corev1alpha1.TaskTransaction{
		ID: "brokered-txn", Scopes: []string{"orka:secrets:credentials:read"},
		Context: map[string]string{"secret": "tool-credential"},
	}
	if err := fixture.reconciler.Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	agent := fixture.agent.DeepCopy()
	agent.Spec.Runtime.RuntimeRef = &corev1alpha1.AgentRuntimeReference{Name: runtime.Name}
	agent.Spec.Runtime.DefaultAllowedTools = nil
	agent.Spec.Runtime.DefaultAllowBash = nil

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	wantTaskSpecDigest, err := harnessV1TaskSpecDigest(task.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if body.HarnessV1 == nil || body.HarnessV1.TaskSpecDigest != wantTaskSpecDigest {
		t.Fatalf("frozen Task spec digest = %#v, want %q", body.HarnessV1, wantTaskSpecDigest)
	}
	if bytes.Contains(candidate.snapshotBody, []byte(task.Spec.Transaction.ID)) {
		t.Fatal("snapshot exposed transaction metadata instead of only its Task spec digest")
	}
	currentTool := &corev1alpha1.Tool{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(tool), currentTool); err != nil {
		t.Fatal(err)
	}
	wantDefinitionDigest, err := harnessV1BrokeredToolDefinitionDigest(currentTool)
	if err != nil {
		t.Fatal(err)
	}
	assertHarnessV1BrokeredCandidateRoute(t, candidate, runtime)
	frozenTool := assertHarnessV1BrokeredSnapshot(
		t, candidate.snapshotBody, body, currentTool, parameters, wantDefinitionDigest, toolEndpoint,
	)

	if result, err, handled := fixture.reconciler.ensureHarnessV1ExecutionBinding(ctx, task.DeepCopy(), agent); err != nil || handled {
		t.Fatalf("ensure brokered binding = result=%#v handled=%v err=%v", result, handled, err)
	}
	bound := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(task), bound); err != nil {
		t.Fatal(err)
	}
	if bound.Status.AgentExecutionBinding == nil || bound.Status.AgentExecutionBinding.BindingDigest == "" {
		t.Fatalf("persisted brokered binding = %#v", bound.Status.AgentExecutionBinding)
	}
	verified, err := fixture.reconciler.loadVerifiedHarnessV1Execution(
		ctx, bound, bound.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatal(err)
	}
	attempt := &store.HarnessV1Attempt{
		Namespace: bound.Namespace, TaskName: bound.Name, TaskUID: string(bound.UID), Attempt: 1,
		BindingDigest:  bound.Status.AgentExecutionBinding.BindingDigest,
		SnapshotDigest: bound.Status.AgentExecutionBinding.Snapshot.Digest,
		TurnID:         "brokered-turn", RuntimeSessionID: "brokered-runtime-session",
		CorrelationID: string(bound.UID),
	}
	request, err := buildHarnessV1StartTurnRequest(bound, verified, attempt, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertHarnessV1BrokeredStartTurn(t, request, frozenTool)
}

func assertHarnessV1BrokeredCandidateRoute(
	t *testing.T,
	candidate *agentExecutionCandidate,
	runtime *corev1alpha1.AgentRuntime,
) {
	t.Helper()
	if candidate.binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint ||
		candidate.binding.RuntimeRef == nil || candidate.binding.RuntimeRef.Name != runtime.Name ||
		candidate.binding.RuntimeRef.UID != runtime.UID || candidate.binding.RuntimeRef.Generation != runtime.Generation {
		t.Fatalf("brokered candidate binding route = %#v", candidate.binding)
	}
}

func assertHarnessV1BrokeredSnapshot(
	t *testing.T,
	snapshotBody []byte,
	body agentExecutionSnapshotBody,
	currentTool *corev1alpha1.Tool,
	parameters []byte,
	wantDefinitionDigest string,
	toolEndpoint string,
) agentExecutionSnapshotHarnessV1BrokeredTool {
	t.Helper()
	if body.HarnessV1 == nil || body.HarnessV1.ToolExecutionMode != string(harness.ToolExecutionModeBrokered) ||
		!slices.Equal(body.HarnessV1.BrokeredToolClasses, []corev1alpha1.AgentRuntimeBrokeredToolClass{
			corev1alpha1.AgentRuntimeBrokeredToolClassRead,
		}) || len(body.HarnessV1.BrokeredTools) != 1 {
		t.Fatalf("frozen brokered governance = %#v", body.HarnessV1)
	}
	frozenTool := body.HarnessV1.BrokeredTools[0]
	if frozenTool.Name != currentTool.Name || frozenTool.Description != currentTool.Spec.Description ||
		frozenTool.BrokeredClass != currentTool.Spec.BrokeredToolClass ||
		!jsonEqual(frozenTool.Parameters, parameters) || frozenTool.UID != string(currentTool.UID) ||
		frozenTool.Generation != currentTool.Generation || frozenTool.DefinitionDigest != wantDefinitionDigest {
		t.Fatalf("frozen Tool authority = %#v, want Tool %s/%s at %s", frozenTool, currentTool.Namespace, currentTool.Name, wantDefinitionDigest)
	}
	if strings.Contains(string(snapshotBody), toolEndpoint) ||
		strings.Contains(string(snapshotBody), "external-runtime-auth-value") {
		t.Fatal("brokered snapshot exposed Tool execution or runtime credential material")
	}
	return frozenTool
}

func assertHarnessV1BrokeredStartTurn(
	t *testing.T,
	request harness.StartTurnRequest,
	frozenTool agentExecutionSnapshotHarnessV1BrokeredTool,
) {
	t.Helper()
	if request.ToolExecutionMode != harness.ToolExecutionModeBrokered || len(request.Input.Tools) != 1 ||
		request.Metadata["brokeredToolClasses"] != string(corev1alpha1.AgentRuntimeBrokeredToolClassRead) {
		t.Fatalf("brokered StartTurn = %#v", request)
	}
	definition := request.Input.Tools[0]
	if definition.Name != frozenTool.Name || definition.Description != frozenTool.Description ||
		definition.BrokeredClass != harness.BrokeredToolClass(frozenTool.BrokeredClass) ||
		!jsonEqual(definition.Parameters, frozenTool.Parameters) {
		t.Fatalf("brokered StartTurn Tool definition = %#v, want frozen %#v", definition, frozenTool)
	}
}

func jsonEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil &&
		fmt.Sprint(leftValue) == fmt.Sprint(rightValue)
}

func TestResolveHarnessV1TargetRevalidatesTLSForReadyRuntime(t *testing.T) {
	const (
		endpoint   = "http://runtime.default.svc:8080"
		runtimeKey = "token"
	)
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "stale-ready-v1", UID: types.UID("stale-ready-v1-uid"), Generation: 1,
		},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
			Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{
				Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: endpoint,
			},
			ClientAuth: corev1alpha1.AgentRuntimeClientAuth{
				BearerAuthRef: &corev1alpha1.AgentRuntimeBearerAuthReference{Name: "stale-ready-v1-auth", Key: runtimeKey},
			},
		},
		Status: corev1alpha1.AgentRuntimeStatus{
			Ready: true, ObservedGeneration: 1, ObservedAuthRefResourceVersion: "1",
			ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeObserved,
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: runtime.Namespace, Name: runtime.Spec.ClientAuth.BearerAuthRef.Name,
			UID: types.UID("stale-ready-v1-auth-uid"), ResourceVersion: "1",
		},
		Data: map[string][]byte{runtimeKey: []byte("runtime-auth-value")},
	}
	reconciler, _ := newBindingTestReconciler(t, runtime, secret)
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: runtime.Namespace}}
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtime.Name},
	}}}

	target, err := reconciler.resolveHarnessV1Target(t.Context(), reconciler.Client, task, agent)
	if err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("resolve target = %#v, error = %v, want stale Ready cleartext rejection", target, err)
	}
}

func TestResolveHarnessV1ExecutionCandidateFreezesRuntimeAuthOnly(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	credential := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fixture.task.Namespace, Name: "runtime-auth-credentials",
			UID: types.UID("runtime-auth-credentials-uid"),
		},
		Data: map[string][]byte{"OPENAI_API_KEY": []byte("runtime-auth-secret-value")},
	}
	if err := fixture.reconciler.Create(ctx, credential); err != nil {
		t.Fatal(err)
	}
	fixture.agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: credential.Name}
	task := fixture.task.DeepCopy()
	task.Annotations = map[string]string{labels.AnnotationAgentRuntimeAuthOnly: scheduledRunLabelValue}

	candidate, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, task, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	var body agentExecutionSnapshotBody
	if err := json.Unmarshal(candidate.snapshotBody, &body); err != nil {
		t.Fatal(err)
	}
	if body.HarnessV1 == nil || !body.HarnessV1.RuntimeAuthOnly {
		t.Fatalf("frozen harness v1 metadata = %#v, want runtimeAuthOnly", body.HarnessV1)
	}
	if strings.Contains(string(candidate.snapshotBody), "runtime-auth-secret-value") {
		t.Fatal("encrypted snapshot plaintext body retained a raw provider credential")
	}

	unprotectedTask := task.DeepCopy()
	delete(unprotectedTask.Annotations, labels.AnnotationAgentRuntimeAuthOnly)
	unprotected, err := fixture.reconciler.resolveHarnessV1ExecutionCandidate(ctx, unprotectedTask, fixture.agent)
	if err != nil {
		t.Fatal(err)
	}
	if unprotected.binding.Snapshot.Digest == candidate.binding.Snapshot.Digest {
		t.Fatal("runtime-auth-only annotation did not change the immutable snapshot digest")
	}
}

func TestResolveHarnessV1CredentialRefsAllowlistsRuntimeProviderKeys(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	tests := []struct {
		name        string
		runtimeType corev1alpha1.AgentRuntimeType
		data        map[string][]byte
		wantKeys    []string
	}{
		{
			name: "codex", runtimeType: corev1alpha1.AgentRuntimeCodex,
			data: map[string][]byte{
				"OPENAI_API_KEY":  []byte("codex-provider-key"),
				"OPENAI_BASE_URL": []byte("https://provider.example.invalid"),
			},
			wantKeys: []string{"OPENAI_API_KEY", "OPENAI_BASE_URL"},
		},
		{
			name: "claude", runtimeType: corev1alpha1.AgentRuntimeClaude,
			data: map[string][]byte{
				"ANTHROPIC_API_KEY":  []byte("claude-provider-key"),
				"ANTHROPIC_BASE_URL": []byte("https://provider.example.invalid"),
			},
			wantKeys: []string{"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: fixture.task.Namespace, Name: "provider-" + tt.name,
					UID: types.UID("provider-" + tt.name + "-uid"),
				},
				Data: tt.data,
			}
			if err := fixture.reconciler.Create(ctx, secret); err != nil {
				t.Fatal(err)
			}
			agent := fixture.agent.DeepCopy()
			agent.Spec.Runtime.Type = tt.runtimeType
			agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: secret.Name}
			refs, err := resolveHarnessV1CredentialRefs(
				ctx, fixture.reconciler.Client, agent, resolvedHarnessV1Target{}, false,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(refs) != 1 || !slices.Equal(refs[0].Keys, tt.wantKeys) {
				t.Fatalf("credential refs = %#v, want keys %v", refs, tt.wantKeys)
			}
		})
	}
}

func TestResolveHarnessV1CredentialRefsRejectsFoundryOnlyForRuntimeAuthOnly(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	tests := []struct {
		name string
		data map[string][]byte
	}{
		{
			name: "Foundry flag",
			data: map[string][]byte{
				"CLAUDE_CODE_USE_FOUNDRY": []byte("true"),
				"ANTHROPIC_API_KEY":       []byte("direct-provider-key"),
			},
		},
		{
			name: "Foundry key",
			data: map[string][]byte{
				"ANTHROPIC_FOUNDRY_API_KEY": []byte("foundry-provider-key"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: fixture.task.Namespace, Name: "provider-" + strings.ReplaceAll(strings.ToLower(test.name), " ", "-"),
					UID: types.UID("provider-" + strings.ReplaceAll(strings.ToLower(test.name), " ", "-") + "-uid"),
				},
				Data: test.data,
			}
			if err := fixture.reconciler.Create(ctx, secret); err != nil {
				t.Fatal(err)
			}
			agent := fixture.agent.DeepCopy()
			agent.Spec.Runtime.Type = corev1alpha1.AgentRuntimeClaude
			agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: secret.Name}

			if _, err := resolveHarnessV1CredentialRefs(
				ctx, fixture.reconciler.Client, agent, resolvedHarnessV1Target{}, true,
			); err == nil || !isPermanentHarnessV1CandidateError(err) || !strings.Contains(err.Error(), "does not support Azure AI Foundry") {
				t.Fatalf("runtime-auth-only Foundry error = %v, want permanent early rejection", err)
			}
			refs, err := resolveHarnessV1CredentialRefs(
				ctx, fixture.reconciler.Client, agent, resolvedHarnessV1Target{}, false,
			)
			if err != nil || len(refs) != 1 {
				t.Fatalf("non-proxied Foundry refs = %#v, error = %v, want preserved support", refs, err)
			}
		})
	}
}

func TestResolveHarnessV1CredentialRefsRejectsUnrelatedKeys(t *testing.T) {
	ctx := context.Background()
	fixture := newHarnessV1CandidateFixture(t)
	for _, prohibitedKey := range []string{"GITHUB_TOKEN", "GIT_TOKEN", "KONTXT_TXN_TOKEN"} {
		t.Run(prohibitedKey, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: fixture.task.Namespace, Name: "provider-" + strings.ToLower(prohibitedKey),
					UID: types.UID("provider-" + strings.ToLower(prohibitedKey) + "-uid"),
				},
				Data: map[string][]byte{
					"OPENAI_API_KEY": []byte("provider-key"),
					prohibitedKey:    []byte("unrelated-sensitive-value"),
				},
			}
			if err := fixture.reconciler.Create(ctx, secret); err != nil {
				t.Fatal(err)
			}
			agent := fixture.agent.DeepCopy()
			agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: secret.Name}
			_, err := resolveHarnessV1CredentialRefs(
				ctx, fixture.reconciler.Client, agent, resolvedHarnessV1Target{}, false,
			)
			if err == nil || !isPermanentHarnessV1CandidateError(err) || !strings.Contains(err.Error(), prohibitedKey) {
				t.Fatalf("unrelated credential key error = %v, want permanent rejection mentioning %s", err, prohibitedKey)
			}
		})
	}
}

func TestHarnessV1SessionLineageDigestExcludesTurnSnapshot(t *testing.T) {
	binding := &corev1alpha1.AgentExecutionBinding{
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
		Backend:         corev1alpha1.AgentExecutionBackendHarnessWrapper,
		RuntimeType:     corev1alpha1.AgentRuntimeCodex,
		Agent: &corev1alpha1.AgentExecutionAgentRef{
			Namespace: "default", Name: "agent", UID: types.UID("agent-uid"), Generation: 1,
		},
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{Digest: "sha256:" + strings.Repeat("b", 64)},
	}
	first, err := harnessV1SessionLineageConfigDigest(binding)
	if err != nil {
		t.Fatal(err)
	}
	secondTurn := *binding
	secondTurn.Snapshot.Digest = "sha256:" + strings.Repeat("c", 64)
	second, err := harnessV1SessionLineageConfigDigest(&secondTurn)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("turn-specific snapshots changed v1 Session lineage digest: %s != %s", first, second)
	}
	otherRuntime := secondTurn
	otherRuntime.RuntimeType = corev1alpha1.AgentRuntimeClaude
	third, err := harnessV1SessionLineageConfigDigest(&otherRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("runtime identity change did not change v1 Session lineage digest")
	}
}

func TestValidateHarnessV1RuntimeAuthOnlyRejectsUnsupportedRoute(t *testing.T) {
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{labels.AnnotationAgentRuntimeAuthOnly: scheduledRunLabelValue},
	}}
	tests := []struct {
		name       string
		runtime    corev1alpha1.AgentRuntimeType
		target     resolvedHarnessV1Target
		wantReason string
	}{
		{
			name: "external endpoint", runtime: corev1alpha1.AgentRuntimeCodex,
			target: resolvedHarnessV1Target{
				backend:    corev1alpha1.AgentExecutionBackendExternalEndpoint,
				runtimeRef: &corev1alpha1.AgentRuntime{},
			},
			wantReason: "built-in wrapper",
		},
		{
			name: "unsupported built-in runtime", runtime: corev1alpha1.AgentRuntimeCopilot,
			target:     resolvedHarnessV1Target{backend: corev1alpha1.AgentExecutionBackendHarnessWrapper},
			wantReason: "does not support runtime",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{
				Runtime: &corev1alpha1.AgentCLIRuntime{Type: test.runtime},
			}}
			_, err := validateHarnessV1RuntimeAuthOnly(task, agent, test.target)
			if err == nil || !isPermanentHarnessV1CandidateError(err) || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("validate runtime-auth-only route error = %v, want permanent %q failure", err, test.wantReason)
			}
		})
	}
}

func TestHandlePendingRoutesExpiredHarnessV1BindingBeforeACPTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := newHarnessV1CandidateFixture(t)

	current := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), current); err != nil {
		t.Fatal(err)
	}
	timeout := metav1.Duration{Duration: time.Minute}
	current.Spec.Timeout = &timeout
	current.CreationTimestamp = metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute))
	if err := fixture.reconciler.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = corev1alpha1.TaskPhasePending
	if err := fixture.reconciler.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), current); err != nil {
		t.Fatal(err)
	}
	fixture.task = current.DeepCopy()
	if result, err, handled := fixture.reconciler.ensureHarnessV1ExecutionBinding(
		ctx, current, fixture.agent,
	); err != nil || handled {
		t.Fatalf("establish harness v1 binding: result=%#v handled=%v err=%v", result, handled, err)
	}
	bound := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), bound); err != nil {
		t.Fatal(err)
	}

	attempts, ok := fixture.reconciler.AgentExecutionSnapshots.(*sqlite.Store)
	if !ok {
		t.Fatalf("snapshot store type = %T, want *sqlite.Store", fixture.reconciler.AgentExecutionSnapshots)
	}
	epochs, stopEpochs := startACPRecoveryEpochManager(t, ctx, attempts, "harness-v1-expired-routing")
	defer stopEpochs()
	fixture.reconciler.HarnessV1Attempts = attempts
	fixture.reconciler.ControllerEpochManager = epochs

	result, err := fixture.reconciler.handlePending(ctx, bound)
	if err != nil {
		t.Fatalf("handlePending: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("handlePending result = %#v, want one-second harness v1 dispatch requeue", result)
	}
	updated := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(bound), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Execution != nil {
		t.Fatalf("expired bound harness v1 Task received ACP execution status: %#v", updated.Status.Execution)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseRunning || updated.Status.HarnessRuntime == nil ||
		updated.Status.HarnessRuntime.State != corev1alpha1.TaskExecutionStateQueued {
		t.Fatalf("expired bound harness v1 Task was not routed to its immutable plane: %#v", updated.Status)
	}
}

type harnessV1CandidateFixture struct {
	reconciler      *TaskReconciler
	controls        store.DurableControlStore
	lineageControls *harnessV1LineageControlStore
	fence           store.ControllerEpochFence
	task            *corev1alpha1.Task
	agent           *corev1alpha1.Agent
}

func seedHarnessV1BindingTranscript(
	t *testing.T,
	ctx context.Context,
	fixture *harnessV1CandidateFixture,
	sessionName string,
	sessionType string,
	messages []store.SessionMessage,
) {
	t.Helper()
	if fixture == nil || fixture.reconciler == nil || fixture.reconciler.SessionManager == nil {
		t.Fatal("harness v1 binding fixture is missing its Session transcript store")
	}
	transcripts := fixture.reconciler.SessionManager.store
	if err := transcripts.CreateSession(ctx, &store.SessionRecord{
		Namespace: fixture.task.Namespace, Name: sessionName, SessionType: sessionType,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	control, err := fixture.controls.CreateSessionControl(ctx, &store.SessionControl{
		Namespace: fixture.task.Namespace, SessionName: sessionName,
		SessionUID:    "frozen-" + sessionName + "-uid",
		RequestDigest: store.CanonicalAgentExecutionSnapshotDigest([]byte("session-control:" + sessionName)),
		Availability:  store.SessionAvailable, CreatedAt: now, UpdatedAt: now,
	}, fixture.fence)
	if err != nil {
		t.Fatal(err)
	}
	lineageConfigDigest, err := harnessV1SessionLineageConfigDigest(&corev1alpha1.AgentExecutionBinding{
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
		Backend:         corev1alpha1.AgentExecutionBackendHarnessWrapper,
		RuntimeType:     fixture.agent.Spec.Runtime.Type,
		Agent: &corev1alpha1.AgentExecutionAgentRef{
			Namespace: fixture.agent.Namespace, Name: fixture.agent.Name,
			UID: fixture.agent.UID, Generation: fixture.agent.Generation,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.lineageControls.lineages[fixture.task.Namespace+"\x00"+sessionName] = &store.SessionLineage{
		Namespace: fixture.task.Namespace, SessionName: sessionName,
		NamespaceUID: "harness-v1-recovery-namespace-uid", SessionUID: control.SessionUID,
		ContractVersion: string(corev1alpha1.AgentRuntimeContractHarnessV1), LineageGeneration: 1,
		RuntimeIdentity: string(fixture.agent.Spec.Runtime.Type), ConfigDigest: lineageConfigDigest,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if len(messages) == 0 {
		return
	}
	for index := range messages {
		if messages[index].Timestamp.IsZero() {
			messages[index].Timestamp = now.Add(time.Duration(index) * time.Millisecond)
		}
	}
	if err := transcripts.AppendMessages(ctx, fixture.task.Namespace, sessionName, messages); err != nil {
		t.Fatal(err)
	}
}

func newHarnessV1CandidateFixture(
	t *testing.T,
) *harnessV1CandidateFixture {
	t.Helper()
	const authSecretKey = "harness-auth"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "harness-v1-recovery", UID: types.UID("harness-v1-recovery-task-uid"), Generation: 1,
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "continue admitted work",
			AgentRef: &corev1alpha1.AgentReference{Name: "harness-v1-agent"},
		},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: task.Namespace, UID: types.UID("harness-v1-recovery-namespace-uid"),
	}}
	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: task.Namespace, Name: "harness-v1-auth",
			UID: types.UID("harness-v1-auth-secret-uid"), ResourceVersion: "1",
		},
		Data: map[string][]byte{authSecretKey: []byte(strings.Repeat("t", 32))},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: task.Namespace, Name: task.Spec.AgentRef.Name,
			UID: types.UID("harness-v1-recovery-agent-uid"), Generation: 1,
		},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Name: "test-model"},
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
				DefaultAllowedTools: []string{}, DefaultAllowBash: new(false),
			},
		},
	}

	reconciler, durable := newBindingTestReconciler(t, task, namespace, authSecret)
	fence := seedHarnessV1AttemptEpoch(t, durable)
	reconciler.HarnessV1Endpoint = "http://harness-v1.default.svc:8080"
	reconciler.HarnessV1AuthSecretNamespace = authSecret.Namespace
	reconciler.HarnessV1AuthSecretName = authSecret.Name
	reconciler.HarnessV1AuthSecretKey = authSecretKey
	reconciler.SessionManager = NewSessionManager(durable)
	lineageControls := &harnessV1LineageControlStore{
		DurableControlStore: durable,
		lineages:            make(map[string]*store.SessionLineage),
	}
	reconciler.DurableControlStore = lineageControls
	return &harnessV1CandidateFixture{
		reconciler:      reconciler,
		controls:        lineageControls,
		lineageControls: lineageControls,
		fence:           fence,
		task:            task,
		agent:           agent,
	}
}

func newHarnessV1RecoveryBindingFixture(
	t *testing.T,
	ctx context.Context,
) (*TaskReconciler, *corev1alpha1.Task) {
	t.Helper()
	fixture := newHarnessV1CandidateFixture(t)
	if result, err, handled := fixture.reconciler.ensureHarnessV1ExecutionBinding(
		ctx, fixture.task.DeepCopy(), fixture.agent,
	); err != nil || handled {
		t.Fatalf("establish harness v1 recovery fixture binding: result=%#v handled=%v err=%v", result, handled, err)
	}
	bound := &corev1alpha1.Task{}
	if err := fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.task), bound); err != nil {
		t.Fatal(err)
	}
	return fixture.reconciler, bound
}
