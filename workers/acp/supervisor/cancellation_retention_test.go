package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestCancelPromptAfterDisconnectedSessionCleanup(t *testing.T) {
	if runtime.GOOS != linuxGOOS {
		t.Skip("UID descendant cleanup proof requires Linux")
	}
	server, cfg, profile := newTestServer(t, "wait")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d", created.Code)
	}
	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveMutationAsync(server.Handler(), mutationHTTPRequest(
		t, http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg,
	).WithContext(ctx))
	deadline := time.Now().Add(5 * time.Second)
	for {
		server.mu.Lock()
		state := server.sessions[create.RuntimeSessionID]
		accepted := state != nil && state.prompt != nil && !state.prompt.acceptedAt.IsZero()
		server.mu.Unlock()
		if accepted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prompt was not accepted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Reproduce the Task-deadline ordering: disconnect first, let the
	// supervisor retire the child, then request the exact cancellation proof.
	cancel()
	awaitRecorder(t, done, "disconnected prompt did not settle")
	awaitDeletedRuntimeSession(t, server, create.RuntimeSessionID)
	request := testLateCancellation(t, create.Metadata.Fence)
	response := performMutation(t, server.Handler(), http.MethodPut,
		"/v2/runtime-sessions/session-1/prompts/prompt-1/cancel", request, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("late cancellation status = %d body=%s", response.Code, response.Body.String())
	}
	var settled harnessv2.CancelPromptResponse
	decodeResponse(t, response, &settled)
	if err := settled.Validate(); err != nil {
		t.Fatal(err)
	}
	if !settled.SettlementProven || settled.Settlement.TerminalEvent != harnessv2.EventCancelled {
		t.Fatalf("late cancellation lost settlement: %#v", settled)
	}
}

func newCancellationRetentionFixture(t *testing.T, settlement harnessv2.PromptSettlement) (*Server, Config, *sessionState) {
	t.Helper()
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	// Renewed prompts outlive their admission capability. The final proof
	// must survive pruning that operation from the replay journal.
	prompt.Metadata.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	sealRequest(t, &prompt.Metadata.RequestDigest, prompt)
	state := &sessionState{
		id: create.RuntimeSessionID,
		descriptor: harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: create.RuntimeSessionID, RuntimeSessionUID: create.Metadata.Fence.RuntimeSessionUID,
			Generation: create.Metadata.Fence.RuntimeSessionGeneration, RuntimeProfileDigest: cfg.Fence.RuntimeProfileDigest,
		},
		prompt: &promptState{request: prompt, settlement: &settlement, acceptedAt: time.Now().UTC().Add(-2 * time.Minute)},
	}
	server.mu.Lock()
	recordPromptAdmissionLocked(state, state.prompt)
	server.tombstoneSessionLocked(state, time.Now().UTC())
	server.mu.Unlock()
	return server, cfg, state
}

func cancelledRetentionSettlement() harnessv2.PromptSettlement {
	return harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeCancelled,
		StopReason: harnessv2.ACPStopReasonCancelled, SettledAt: time.Now().UTC(),
	}
}

func TestRetiredCancellationPreservesSettlementAndReplay(t *testing.T) {
	for _, settlement := range []harnessv2.PromptSettlement{
		cancelledRetentionSettlement(),
		{TerminalEvent: harnessv2.EventCompleted, Outcome: harnessv2.PromptOutcomeSucceeded, StopReason: harnessv2.ACPStopReasonEndTurn, SettledAt: time.Now().UTC()},
		{TerminalEvent: harnessv2.EventFailed, Outcome: harnessv2.PromptOutcomeFailed, StopReason: harnessv2.ACPStopReasonMaxTurnRequests, SettledAt: time.Now().UTC()},
		{TerminalEvent: harnessv2.EventOutcomeUnknown, Outcome: harnessv2.PromptOutcomeUnknown, SettledAt: time.Now().UTC()},
	} {
		t.Run(string(settlement.TerminalEvent), func(t *testing.T) {
			server, cfg, state := newCancellationRetentionFixture(t, settlement)
			tombstone := server.tombstones[state.descriptor.RuntimeSessionUID]
			if len(tombstone.Operations) != 0 {
				t.Fatal("fixture admission operation did not expire")
			}
			request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
			path := "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel"
			first := performMutation(t, server.Handler(), http.MethodPut, path, request, cfg)
			if first.Code != http.StatusOK {
				t.Fatalf("fresh cancellation status = %d body=%s", first.Code, first.Body.String())
			}
			var got harnessv2.CancelPromptResponse
			decodeResponse(t, first, &got)
			if err := got.Validate(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Settlement, settlement) || got.SettlementProven != (settlement.TerminalEvent != harnessv2.EventOutcomeUnknown) {
				t.Fatalf("recorded outcome changed: %#v", got)
			}
			repeated := performMutation(t, server.Handler(), http.MethodPut, path, request, cfg)
			if repeated.Code != http.StatusOK {
				t.Fatalf("repeated cancellation status = %d", repeated.Code)
			}
			var replayed harnessv2.CancelPromptResponse
			decodeResponse(t, repeated, &replayed)
			got.Classification = harnessv2.Classification{Class: harnessv2.RequestClassificationSettled, Phase: harnessv2.OperationPhaseSettled, TerminalEvent: settlement.TerminalEvent}
			if !reflect.DeepEqual(got, replayed) {
				t.Fatalf("replay changed cancellation response: %#v", replayed)
			}
			request.Reason = harnessv2.CancelReasonUserRequested
			sealRequest(t, &request.Metadata.RequestDigest, request)
			conflict := performMutation(t, server.Handler(), http.MethodPut, path, request, cfg)
			if conflict.Code != http.StatusConflict {
				t.Fatalf("conflicting digest status = %d", conflict.Code)
			}
			retired := server.tombstones[state.descriptor.RuntimeSessionUID]
			if len(server.sessions) != 0 || len(retired.cancellationOperations) != 1 {
				t.Fatal("late cancellation resurrected the session or repeated the operation")
			}
			if !reflect.DeepEqual(tombstone.RuntimeSessionTombstone, retired.RuntimeSessionTombstone) {
				t.Fatal("late cancellation changed the deletion replay tombstone")
			}
		})
	}
}

func TestRetiredCancellationRequiresExactIdentityAndAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*harnessv2.CancelPromptRequest)
	}{
		{"runtime instance", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.RuntimeInstanceID += "-other" }},
		{"supervisor boot", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.SupervisorBootID += "-other" }},
		{"controller epoch", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.ControllerEpoch++ }},
		{"pool uid", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.RuntimePoolUID += "-other" }},
		{"pool generation", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.RuntimePoolGeneration++ }},
		{"session uid", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.RuntimeSessionUID += "-other" }},
		{"session generation", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.RuntimeSessionGeneration++ }},
		{"profile digest", func(r *harnessv2.CancelPromptRequest) {
			r.Metadata.Fence.RuntimeProfileDigest = harnessv2.ProfileDigest(testDigest("other"))
		}},
		{"profile schema", func(r *harnessv2.CancelPromptRequest) { r.Metadata.Fence.ProfileDigestSchemaVersion++ }},
		{"task uid", func(r *harnessv2.CancelPromptRequest) { r.Metadata.TaskUID += "-other" }},
		{"task attempt", func(r *harnessv2.CancelPromptRequest) { r.Metadata.TaskAttempt++ }},
		{"prompt id", func(r *harnessv2.CancelPromptRequest) { r.Metadata.PromptID += "-other" }},
		{"expired", func(r *harnessv2.CancelPromptRequest) { r.Metadata.ExpiresAt = time.Now().UTC().Add(-time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, cfg, state := newCancellationRetentionFixture(t, cancelledRetentionSettlement())
			request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
			test.mutate(&request)
			sealRequest(t, &request.Metadata.RequestDigest, request)
			path := fmt.Sprintf("/v2/runtime-sessions/session-1/prompts/%s/cancel", request.Metadata.PromptID)
			var req *http.Request
			if test.name == "profile schema" || test.name == "expired" {
				// Invalid metadata cannot be signed; it must fail request
				// validation before operation-capability authorization.
				body, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				req = httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+cfg.ControllerBearerToken)
			} else {
				req = mutationHTTPRequest(t, http.MethodPut, path, request, cfg)
			}
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, req)
			if response.Code < 400 {
				t.Fatalf("mismatched identity returned proof: status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	for _, test := range []string{"session path", "controller authentication", "operation capability", "request digest"} {
		t.Run(test, func(t *testing.T) {
			server, cfg, state := newCancellationRetentionFixture(t, cancelledRetentionSettlement())
			request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
			if test == "request digest" {
				request.Reason = harnessv2.CancelReasonUserRequested
			}
			req := mutationHTTPRequest(t, http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel", request, cfg)
			switch test {
			case "session path":
				req.URL.Path = "/v2/runtime-sessions/other-session/prompts/prompt-1/cancel"
			case "controller authentication":
				req.Header.Del("Authorization")
			case "operation capability":
				req.Header.Del(OperationCapabilityHeader)
			}
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, req)
			if response.Code < 400 {
				t.Fatalf("invalid request returned proof: status=%d", response.Code)
			}
		})
	}
}

func TestRetiredCancellationRejectsMissingOrExpiredProof(t *testing.T) {
	for _, test := range []string{"missing tombstone", "missing settlement", "expired tombstone", "request outlives retention", "full journal"} {
		t.Run(test, func(t *testing.T) {
			server, cfg, state := newCancellationRetentionFixture(t, cancelledRetentionSettlement())
			uid := state.descriptor.RuntimeSessionUID
			tombstone := server.tombstones[uid]
			switch test {
			case "missing tombstone":
				delete(server.tombstones, uid)
			case "missing settlement":
				tombstone.prompt = nil
			case "expired tombstone":
				tombstone.DeletedAt = time.Now().UTC().Add(-2 * tombstoneRetention)
			case "request outlives retention":
				tombstone.DeletedAt = time.Now().UTC().Add(-tombstoneRetention + 10*time.Second)
			case "full journal":
				tombstone.Operations = make([]harnessv2.OperationRecord, harnessv2.MaxRuntimeSessionTombstoneOperations)
			}
			if test != "missing tombstone" {
				server.tombstones[uid] = tombstone
			}
			request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
			response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel", request, cfg)
			if response.Code < 400 {
				t.Fatalf("unavailable proof accepted: status=%d", response.Code)
			}
		})
	}
	server, cfg, state := newCancellationRetentionFixture(t, harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCancelled, Outcome: harnessv2.PromptOutcomeSucceeded, SettledAt: time.Now().UTC(),
	})
	request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel", request, cfg)
	if response.Code != http.StatusGone {
		t.Fatalf("malformed proof accepted: status=%d", response.Code)
	}
}

func TestLateCancellationCannotCancelContinuation(t *testing.T) {
	server, cfg, state := newCancellationRetentionFixture(t, cancelledRetentionSettlement())
	request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
	mutations := &recordingPromptMutator{}
	state.promptMutations = mutations
	state.prompt.request.Metadata.PromptID = testPromptTwoID
	state.prompt.request.Metadata.TaskUID = "continuation-task-uid"
	state.prompt.settlement = nil
	server.mu.Lock()
	delete(server.tombstones, state.descriptor.RuntimeSessionUID)
	server.sessions[state.id] = state
	server.mu.Unlock()
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel", request, cfg)
	if response.Code != http.StatusConflict || mutations.cancelCalls != 0 || state.prompt.settlement != nil {
		t.Fatalf("late cancellation affected continuation: status=%d calls=%d", response.Code, mutations.cancelCalls)
	}
}

func TestRetiredCancellationReplaysInFlightOwner(t *testing.T) {
	settlement := cancelledRetentionSettlement()
	server, cfg, state := newCancellationRetentionFixture(t, settlement)
	mutations := newBlockingPromptMutator()
	mutations.cancelResult = acp.PromptResult{Outcome: acp.PromptOutcomeCancelled, StopReason: acp.StopReasonCancelled, Accepted: true, SettledAt: settlement.SettledAt}
	state.prompt.settlement = nil
	state.promptMutations = mutations
	state.permissions = map[harnessv2.PermissionRequestID]permissionState{"permission-1": {requestID: "permission-1"}}
	server.mu.Lock()
	delete(server.tombstones, state.descriptor.RuntimeSessionUID)
	server.sessions[state.id] = state
	server.mu.Unlock()
	request := testLateCancellation(t, state.prompt.request.Metadata.Fence)
	path := "/v2/runtime-sessions/session-1/prompts/prompt-1/cancel"
	first := serveMutationAsync(server.Handler(), mutationHTTPRequest(t, http.MethodPut, path, request, cfg))
	awaitSignal(t, mutations.cancelEntered, "owner did not start cancelling")
	server.mu.Lock()
	settlePromptLocked(state.prompt, settlement)
	server.tombstoneSessionLocked(state, time.Now().UTC())
	server.mu.Unlock()
	replay := serveMutationAsync(server.Handler(), mutationHTTPRequest(t, http.MethodPut, path, request, cfg))
	assertStillWaiting(t, replay, "replay bypassed its in-flight owner")
	close(mutations.cancelRelease)
	var original, repeated harnessv2.CancelPromptResponse
	decodeResponse(t, awaitRecorder(t, first, "owner did not finish"), &original)
	decodeResponse(t, awaitRecorder(t, replay, "replay did not finish"), &repeated)
	original.Classification = harnessv2.Classification{Class: harnessv2.RequestClassificationSettled, Phase: harnessv2.OperationPhaseSettled, TerminalEvent: settlement.TerminalEvent}
	if !reflect.DeepEqual(original, repeated) || repeated.InvalidatedPermissionRequests != 1 || mutations.cancelCalls.Load() != 1 {
		t.Fatalf("cleanup lost the cancellation replay: %#v", repeated)
	}
}

func testLateCancellation(t *testing.T, fence harnessv2.Fence) harnessv2.CancelPromptRequest {
	t.Helper()
	request := harnessv2.CancelPromptRequest{
		Protocol:           harnessv2.ProtocolVersion,
		Metadata:           testMetadata(fence, "late-cancel", true),
		Reason:             harnessv2.CancelReasonTaskTimeout,
		SettlementDeadline: time.Now().UTC().Add(30 * time.Second),
	}
	sealRequest(t, &request.Metadata.RequestDigest, request)
	return request
}

func awaitDeletedRuntimeSession(t *testing.T, server *Server, sessionID harnessv2.RuntimeSessionID) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		server.mu.Lock()
		deleted := server.sessions[sessionID] == nil
		server.mu.Unlock()
		if deleted {
			return
		}
		if time.Now().After(deadline) {
			server.mu.Lock()
			state := server.sessions[sessionID]
			var settlement *harnessv2.PromptSettlement
			if state != nil && state.prompt != nil {
				settlement = state.prompt.settlement
			}
			server.mu.Unlock()
			t.Fatalf("runtime session was not cleaned up: state=%s scheduled=%v settlement=%#v", state.descriptor.State, state.drainCleanupScheduled, settlement)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
