package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSessionCleanupAuthorityRejectsLostLeadership(t *testing.T) {
	for _, route := range []string{"same endpoint", "rotated endpoint"} {
		for _, timing := range []string{"before construction", "after construction", "during status probe"} {
			t.Run(route+"/"+timing, func(t *testing.T) {
				var armed atomic.Bool
				advanced := make(chan error, 1)
				var fixture *sessionCleanupAuthorityClientFixture
				fixture = newSessionCleanupAuthorityClientFixture(t, route == "rotated endpoint", func(*harnessv2.StatusResponse) {
					if armed.CompareAndSwap(true, false) {
						advanced <- fixture.advanceEpoch()
					}
				})
				if timing == "before construction" {
					if err := fixture.advanceEpoch(); err != nil {
						t.Fatal(err)
					}
				}
				cleanupClient, fence, err := fixture.cleanupClient(fixture.scope())
				if timing == "before construction" {
					if cleanupClient != nil || !errors.Is(err, store.ErrConflict) || !strings.Contains(err.Error(), "lost authority") {
						t.Fatalf("stale cached owner constructed cleanup client: client=%v err=%v", cleanupClient, err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if timing == "after construction" {
						if err := fixture.advanceEpoch(); err != nil {
							t.Fatal(err)
						}
					} else {
						armed.Store(true)
					}
					request, err := newDeleteRuntimeSessionRequestForTaskUID(
						fixture.task, fixture.task.UID, fence, "session_deleted", time.Now().UTC().Add(30*time.Second),
					)
					if err != nil {
						t.Fatal(err)
					}
					_, err = cleanupClient.DeleteRuntimeSession(fixture.base.ctx, harnessv2.RuntimeSessionID(runtimeSessionID(fence)), request)
					assertSessionCleanupClientRejected(t, err, "delete_runtime_session", "lost authority")
					if timing == "during status probe" {
						select {
						case err := <-advanced:
							if err != nil {
								t.Fatalf("takeover during authenticated status failed: %v", err)
							}
						default:
							t.Fatal("cleanup did not reach the armed authenticated status probe")
						}
					}
				}
				cached, err := fixture.base.dispatcher.Epochs.CurrentFence(fixture.base.ctx)
				if err != nil || cached != fixture.owner {
					t.Fatalf("fixture changed cached ownership instead of authoritative ownership: %#v, %v", cached, err)
				}
				if fixture.base.deleteCalls.Load() != 0 {
					t.Fatal("stale controller sent a runtime DELETE")
				}
			})
		}
	}
}

func TestSessionCleanupAuthorityCannotPerformOtherMutations(t *testing.T) {
	for _, rotated := range []bool{false, true} {
		name := "same endpoint"
		if rotated {
			name = "rotated endpoint"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newSessionCleanupAuthorityClientFixture(t, rotated, nil)
			if ordinary, _, err := fixture.cleanupClient(nil); err == nil || ordinary != nil {
				t.Fatal("ordinary cleanup client accepted a previous controller epoch without Session scope")
			}
			cleanupClient, fence, err := fixture.cleanupClient(fixture.scope())
			if err != nil {
				t.Fatal(err)
			}
			sessionID := harnessv2.RuntimeSessionID(runtimeSessionID(fence))
			now := time.Now().UTC()
			baseline, workspace, err := emptyRuntimeWorkspace(fixture.bound.frozenTask, "")
			if err != nil {
				t.Fatal(err)
			}
			create := harnessv2.CreateRuntimeSessionRequest{
				Protocol:         harnessv2.ProtocolVersion,
				Metadata:         mutationMetadata(fence, fixture.task, "cleanup-create-denied", false, now.Add(45*time.Second)),
				RuntimeSessionID: sessionID, Profile: fixture.bound.plan.Profile,
				MCPConfiguration: fixture.bound.mcpConfiguration, Workspace: workspace,
			}
			if err := sealMutation(&create.Metadata.RequestDigest, create); err != nil {
				t.Fatal(err)
			}
			prompt, err := fixture.base.dispatcher.buildPromptRequest(
				fixture.task, fence, fixture.bound.plan.Profile, fixture.bound.mcpConfiguration,
				"", "must not run", fixture.bound.body.ExternalRuntime.Limits, 0,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := sealMutation(&prompt.Metadata.RequestDigest, prompt); err != nil {
				t.Fatal(err)
			}
			renew := harnessv2.RenewPromptLeaseRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: prompt.Metadata,
				ExpectedLeaseGeneration: prompt.Lease.Generation, Lease: prompt.Lease, MCPAuthorization: prompt.MCPAuthorization,
			}
			renew.Metadata.OperationID = "cleanup-renew-denied"
			renew.Lease.Generation++
			renew.MCPAuthorization.LeaseGeneration = renew.Lease.Generation
			if err := sealMutation(&renew.Metadata.RequestDigest, renew); err != nil {
				t.Fatal(err)
			}
			cancel := harnessv2.CancelPromptRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: prompt.Metadata,
				Reason: harnessv2.CancelReasonUserRequested, SettlementDeadline: now.Add(30 * time.Second),
			}
			cancel.Metadata.OperationID = "cleanup-cancel-denied"
			if err := sealMutation(&cancel.Metadata.RequestDigest, cancel); err != nil {
				t.Fatal(err)
			}
			delta := harnessv2.CreateWorkspaceDeltaRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: prompt.Metadata, DeltaID: "cleanup-delta-denied",
				Intent: harnessv2.WorkspaceIntentWrite, VerifiedBaseline: baseline,
				PromptSettlementDigest: store.CanonicalBytesDigest([]byte("settlement")),
				Limits:                 harnessv2.WorkspaceDeltaLimits{MaxBytes: 1024, MaxEntries: 1},
			}
			delta.Metadata.OperationID = "cleanup-delta-denied"
			if err := sealMutation(&delta.Metadata.RequestDigest, delta); err != nil {
				t.Fatal(err)
			}
			publication := harnessv2.FinalizeRuntimeSessionPublicationRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: prompt.Metadata,
				WorkspaceDeltaID: "cleanup-delta-denied", PublicationID: "cleanup-publication-denied",
				PublicationGeneration: 1, PublicationVersion: 1, TerminalState: harnessv2.PublicationTerminalVerifiedExact,
				TerminalReceiptDigest: store.CanonicalBytesDigest([]byte("publication-receipt")),
			}
			publication.Metadata.OperationID = "cleanup-publication-denied"
			if err := sealMutation(&publication.Metadata.RequestDigest, publication); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				operation string
				call      func() error
			}{
				{"create_runtime_session", func() error { _, err := cleanupClient.CreateRuntimeSession(fixture.base.ctx, create); return err }},
				{"start_prompt", func() error {
					stream, err := cleanupClient.StartPrompt(fixture.base.ctx, sessionID, prompt)
					if stream != nil {
						_ = stream.Close()
					}
					return err
				}},
				{"renew_prompt_lease", func() error { _, err := cleanupClient.RenewPromptLease(fixture.base.ctx, sessionID, renew); return err }},
				{"cancel_prompt", func() error { _, err := cleanupClient.CancelPrompt(fixture.base.ctx, sessionID, cancel); return err }},
				{"create_workspace_delta", func() error {
					_, err := cleanupClient.CreateWorkspaceDelta(fixture.base.ctx, sessionID, delta)
					return err
				}},
				{"finalize_runtime_session_publication", func() error {
					_, err := cleanupClient.FinalizeRuntimeSessionPublication(fixture.base.ctx, sessionID, publication)
					return err
				}},
			} {
				t.Run(test.operation, func(t *testing.T) {
					assertSessionCleanupClientRejected(t, test.call(), test.operation, "cannot perform admission or non-cleanup mutations")
				})
			}
			if fixture.base.createCalls.Load() != 1 || fixture.base.deleteCalls.Load() != 0 {
				t.Fatal("rejected scoped operations changed resident runtime state")
			}
			request, err := newDeleteRuntimeSessionRequestForTaskUID(fixture.task, fixture.task.UID, fence, "session_deleted", now.Add(30*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cleanupClient.DeleteRuntimeSession(fixture.base.ctx, sessionID, request); err != nil {
				t.Fatalf("delete-only client rejected its authorized exact DELETE: %v", err)
			}
			if fixture.base.deleteCalls.Load() != 1 {
				t.Fatal("delete-only client did not delete exactly one resident session")
			}
			deleted := <-fixture.base.deleteRequests
			if harnessv2.CompareFence(fixture.originalFence, deleted.Metadata.Fence, true) != harnessv2.FenceMatch {
				t.Fatal("authorized DELETE changed the original runtime fence")
			}
		})
	}
}

func TestSessionCleanupAuthorityRejectsUnfrozenProjectionEpoch(t *testing.T) {
	fixture, tasks := newContinuedSessionCleanupFixture(t)
	turn, projection := sessionRuntimeCleanupTurnProjection(t, fixture, tasks[1])
	epochs, stop := startACPRecoveryEpochManager(t, fixture.ctx, fixture.controlStore, "projection-cleanup-successor")
	defer stop()
	owner, err := epochs.CurrentFence(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	intent := store.SessionCleanupIntent{Namespace: defaultNS, SessionName: "cleanup-conversation", SessionUID: turn.Key.SessionUID}
	for _, test := range []struct {
		name       string
		epoch      string
		legacy     bool
		keepDigest bool
	}{
		{name: "missing epoch"},
		{name: "null epoch", epoch: "null"},
		{name: "zero epoch", epoch: "0"},
		{name: "negative epoch", epoch: "-1"},
		{name: "future epoch", epoch: "3"},
		{name: "runtime epoch mismatch", epoch: "2"},
		{name: "legacy identity from mutable Task", legacy: true},
		{name: "epoch changed without original digest", epoch: "2", keepDigest: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(projection.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			var execution map[string]json.RawMessage
			if err := json.Unmarshal(payload["execution"], &execution); err != nil {
				t.Fatal(err)
			}
			if test.legacy {
				execution = map[string]json.RawMessage{
					"state": execution["state"], "outcome": execution["outcome"],
					"attempt": execution["attempt"], "promptID": execution["promptID"],
				}
			} else if test.epoch == "" {
				delete(execution, "controllerEpoch")
			} else {
				execution["controllerEpoch"] = json.RawMessage(test.epoch)
			}
			payload["execution"], err = json.Marshal(execution)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			changedProjection, changedTurn := *projection, *turn
			changedProjection.Payload = body
			if !test.keepDigest {
				// Re-pin malformed fixtures to exercise authority validation, not
				// merely the separate immutable-payload integrity check.
				changedProjection.PayloadDigest = store.CanonicalBytesDigest(body)
				changedTurn.ProjectionDigest = changedProjection.PayloadDigest
			}
			control := &sessionCleanupAuthorityProjectionStore{
				DurableControlStore: fixture.controlStore, projection: &changedProjection,
			}
			dispatcher := &ACPDispatcher{
				Client: fixture.client, APIReader: fixture.client, Store: control,
				ResultStore: &sessionCleanupAuthorityTurns{ResultStore: fixture.persistence, turn: changedTurn},
				Snapshots:   fixture.persistence, Epochs: epochs,
			}
			if err := dispatcher.CleanupSessionRuntime(fixture.ctx, intent, owner); err == nil {
				t.Fatal("unfrozen or invalid projection epoch authorized previous-epoch cleanup")
			}
			if fixture.deleteCalls.Load() != 0 {
				t.Fatal("invalid projection authority sent a runtime DELETE")
			}
			if _, err := fixture.persistence.GetSession(fixture.ctx, defaultNS, intent.SessionName); err != nil {
				t.Fatalf("rejected cleanup removed the transcript: %v", err)
			}
			if _, err := fixture.controlStore.GetSessionControl(fixture.ctx, defaultNS, intent.SessionName); err != nil {
				t.Fatalf("rejected cleanup removed Session control: %v", err)
			}
			for _, task := range tasks {
				current := &corev1alpha1.Task{}
				if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
					t.Fatal(err)
				}
				if current.Status.Execution.RuntimeSessionCleanupDigest != "" {
					t.Fatal("invalid projection authority minted a runtime cleanup receipt")
				}
			}
		})
	}
}

type sessionCleanupAuthorityClientFixture struct {
	base          *externalACPDispatchFixture
	task          *corev1alpha1.Task
	bound         *verifiedAgentExecution
	originalFence harnessv2.Fence
	owner         store.ControllerEpochFence
	rotated       bool
}

func newSessionCleanupAuthorityClientFixture(t *testing.T, rotated bool, statusTransform func(*harnessv2.StatusResponse)) *sessionCleanupAuthorityClientFixture {
	t.Helper()
	base := newExternalACPDispatchFixtureWithOptions(t, "cleanup-authority", testAgentRuntimeMCPPolicy(), externalACPDispatchFixtureOptions{
		statusTransform: statusTransform,
	})
	queued := base.queueTask(t, "cleanup-authority-task", types.UID("cleanup-authority-task-uid"), "idle", nil)
	task, bound, original := createExternalRuntimeSessionForRecovery(t, base, queued, "cleanup-authority-create")
	epochs, stop := startACPRecoveryEpochManager(t, base.ctx, base.controlStore, "cleanup-authority-successor")
	t.Cleanup(stop)
	base.dispatcher.Epochs = epochs
	owner, err := epochs.CurrentFence(base.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rotated {
		var calls atomic.Int32
		replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(replacement.Close)
		t.Cleanup(func() {
			if calls.Load() != 0 {
				t.Error("cleanup contacted the replacement endpoint instead of its frozen authority")
			}
		})
		runtime := base.runtime.DeepCopy()
		runtime.Spec.Deployment.Endpoint = replacement.URL
		runtime.Generation++
		if err := base.client.Update(base.ctx, runtime); err != nil {
			t.Fatal(err)
		}
	}
	return &sessionCleanupAuthorityClientFixture{
		base: base, task: task, bound: bound, originalFence: original, owner: owner, rotated: rotated,
	}
}

func (f *sessionCleanupAuthorityClientFixture) scope() *sessionRuntimeCleanupFence {
	return &sessionRuntimeCleanupFence{controller: f.owner, runtimeEpoch: f.originalFence.ControllerEpoch}
}

func (f *sessionCleanupAuthorityClientFixture) cleanupClient(scope *sessionRuntimeCleanupFence) (*harnessv2.Client, harnessv2.Fence, error) {
	runtime := &corev1alpha1.AgentRuntime{}
	if err := f.base.client.Get(f.base.ctx, client.ObjectKeyFromObject(f.base.runtime), runtime); err != nil {
		return nil, harnessv2.Fence{}, err
	}
	constructor := f.base.dispatcher.externalRuntimeCleanupClient
	if f.rotated {
		constructor = f.base.dispatcher.externalRuntimeRotatedEndpointCleanupClient
	}
	cleanupClient, fence, err := constructor(
		f.base.ctx, runtime, f.bound.body.ExternalRuntime, f.bound.plan.Digest, f.bound.body.ExternalRuntime.Limits,
		f.originalFence.RuntimeInstanceID, f.originalFence.SupervisorBootID, scope,
	)
	fence.RuntimeSessionUID = f.originalFence.RuntimeSessionUID
	fence.RuntimeSessionGeneration = f.originalFence.RuntimeSessionGeneration
	return cleanupClient, fence, err
}

func (f *sessionCleanupAuthorityClientFixture) advanceEpoch() error {
	current, err := f.base.controlStore.GetControllerEpoch(f.base.ctx, f.owner.Name)
	if err != nil {
		return err
	}
	holder := "cleanup-authority-takeover"
	_, err = f.base.controlStore.CompareAndSwapControllerEpoch(f.base.ctx, store.ControllerEpochCAS{
		Name: current.Name, ExpectedVersion: current.Version, ExpectedEpoch: current.Epoch, NewEpoch: current.Epoch + 1,
		HolderID: holder, UpdatedAt: time.Now().UTC(),
		RequestDigest: controllerEpochDigest(current.Name, holder, current.Version, current.Epoch, current.Epoch+1),
	})
	return err
}

func assertSessionCleanupClientRejected(t *testing.T, err error, operation, message string) {
	t.Helper()
	var rejected *harnessv2.ClientError
	if !errors.As(err, &rejected) || rejected.Kind != harnessv2.ClientErrorValidation || rejected.Operation != operation ||
		rejected.StatusCode != 0 || !strings.Contains(rejected.Message, message) {
		t.Fatalf("%s was not rejected by the pre-mutation authority check: %v", operation, err)
	}
}

type sessionCleanupAuthorityProjectionStore struct {
	store.DurableControlStore
	projection *store.OutboxProjection
}

func (s *sessionCleanupAuthorityProjectionStore) GetOutboxProjection(ctx context.Context, id string) (*store.OutboxProjection, error) {
	if id == s.projection.ID {
		return s.projection, nil
	}
	return s.DurableControlStore.GetOutboxProjection(ctx, id)
}

func (s *sessionCleanupAuthorityProjectionStore) GetControllerEpochFence(ctx context.Context, name string) (store.ControllerEpochFence, error) {
	return s.DurableControlStore.(interface {
		GetControllerEpochFence(context.Context, string) (store.ControllerEpochFence, error)
	}).GetControllerEpochFence(ctx, name)
}

type sessionCleanupAuthorityTurns struct {
	store.ResultStore
	turn store.SessionTurn
}

func (s *sessionCleanupAuthorityTurns) ListSessionCleanupTurns(context.Context, store.SessionCleanupIntent) ([]store.SessionTurn, error) {
	return []store.SessionTurn{s.turn}, nil
}
