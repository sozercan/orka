package supervisor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const supervisorUpstreamTokenCanary = "supervisor-only-upstream-token-canary"
const testJSONRPCVersion = "2.0"
const testJSONRPCKey = "jsonrpc"
const linuxGOOS = "linux"

const providerProxyCanaryMode = "provider-proxy-canary"
const providerProjectionCanaryMode = "provider-projection-canary"
const providerProjectionCanaryValue = "projected"
const assistantBurstMode = "assistant-burst"
const toolBurstRPCErrorMode = "tool-burst-rpc-error"
const testPromptOneID = "prompt-1"
const testPromptTwoID = "prompt-2"
const testPromptOperationTwo = "prompt-operation-2"

func TestSafeErrorExposesOnlySessionCreationStage(t *testing.T) {
	t.Parallel()
	const secret = "provider-secret-must-not-leak"
	staged := sessionCreationFailed("workspace materialization", errors.New(secret))
	if stage := sessionCreationStage(staged); stage != "workspace materialization" {
		t.Fatalf("session creation stage = %q", stage)
	}
	if isSessionCreationResumeLost(staged) {
		t.Fatal("ordinary session creation failure was classified as resume loss")
	}
	resumeLost := sessionCreationResumeLost(errors.New(secret))
	if !isSessionCreationResumeLost(resumeLost) ||
		sessionCreationStage(resumeLost) != sessionCreationStageDurableResumeVerification {
		t.Fatalf("resume loss classification = %#v", resumeLost)
	}
	got := safeError(staged)
	if got != "runtime session failed during workspace materialization" {
		t.Fatalf("safe staged error = %q", got)
	}
	if strings.Contains(got, secret) {
		t.Fatalf("safe staged error leaked raw cause: %q", got)
	}
	generic := safeError(errors.New(secret))
	if generic != "runtime operation failed; consult bounded supervisor diagnostics" || strings.Contains(generic, secret) {
		t.Fatalf("generic safe error = %q", generic)
	}
}

func TestCreateSessionRejectsStaleTransitionOnlyDurableResume(t *testing.T) {
	cfg, profile := newTestConfigWithUpstream(
		t,
		"immediate",
		"http://127.0.0.1:1",
		strings.Repeat("p", 32),
	)
	cfg.DurableWorkspaceDir = t.TempDir()
	request := testCreateSessionRequest(t, cfg, profile)
	request.Workspace.ExpectDurableResume = true
	request.Workspace.ExpectDurableResumeMinGeneration = 5
	if _, _, err := cfg.UIDAllocator.AllocateAboveReserve(0); err != nil {
		t.Fatalf("allocate session identity: %v", err)
	}
	if err := acp.MarkDurableWorkspaceTransitionAuthorized(
		cfg.DurableWorkspaceDir,
		string(request.Metadata.Fence.RuntimeSessionUID),
		acp.DurableWorkspaceBinding{
			RepositoryIdentity:       request.Workspace.Baseline.RepositoryIdentity,
			Revision:                 request.Workspace.Baseline.Revision,
			SessionIdentityHighWater: 1,
			SessionGeneration:        4,
		},
	); err != nil {
		t.Fatalf("stage transition: %v", err)
	}

	server := &Server{cfg: cfg}
	_, _, _, _, _, _, _, err := server.createSession(
		context.Background(), request, time.Now().UTC(), os.Getuid(), os.Getgid(),
	)
	if err == nil || !isSessionCreationResumeLost(err) ||
		sessionCreationStage(err) != sessionCreationStageDurableResumeVerification ||
		!strings.Contains(err.Error(), "older than the controller's floor") {
		t.Fatalf("createSession error = %v, want stale transition generation refusal", err)
	}
}

func TestSupervisorClassifiesAndReplaysDurableResumeLoss(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, string, harnessv2.CreateRuntimeSessionRequest)
	}{
		{name: "missing checkpoint"},
		{
			name: "marker without workspace tree",
			prepare: func(t *testing.T, durableRoot string, request harnessv2.CreateRuntimeSessionRequest) {
				t.Helper()
				workspaceDir, _, err := acp.PrepareDurableSessionWorkspace(
					durableRoot, string(request.Metadata.Fence.RuntimeSessionUID),
					1,
				)
				if err != nil {
					t.Fatalf("prepare durable checkpoint: %v", err)
				}
				if err := acp.CommitDurableSessionWorkspace(
					durableRoot,
					string(request.Metadata.Fence.RuntimeSessionUID),
					acp.DurableWorkspaceBinding{
						RepositoryIdentity:       request.Workspace.Baseline.RepositoryIdentity,
						Revision:                 request.Workspace.Baseline.Revision,
						SessionIdentityHighWater: 1,
						SessionGeneration:        request.Workspace.ExpectDurableResumeMinGeneration,
					},
				); err != nil {
					t.Fatalf("commit durable checkpoint: %v", err)
				}
				if err := os.RemoveAll(workspaceDir); err != nil {
					t.Fatalf("remove durable workspace tree: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, cfg, profile := newTestServer(t, "immediate")
			server.cfg.DurableWorkspaceDir = t.TempDir()
			request := testCreateSessionRequest(t, cfg, profile)
			request.Workspace.ExpectDurableResume = true
			request.Workspace.ExpectDurableResumeMinGeneration = 1
			if test.prepare != nil {
				test.prepare(t, server.cfg.DurableWorkspaceDir, request)
			}
			request.Metadata.RequestDigest = ""
			sealRequest(t, &request.Metadata.RequestDigest, request)

			assertResumeLost := func(response *httptest.ResponseRecorder) {
				t.Helper()
				if response.Code != http.StatusConflict {
					t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
				}
				var envelope harnessv2.ErrorResponse
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("decode error response: %v", err)
				}
				if envelope.Code != harnessv2.ErrorCodeWorkspaceResumeLost || envelope.Retryable ||
					envelope.Message != "runtime session failed during "+sessionCreationStageDurableResumeVerification {
					t.Fatalf("durable resume error = %#v", envelope)
				}
			}

			first := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", request, cfg)
			assertResumeLost(first)
			remaining := server.cfg.UIDAllocator.Remaining()
			replay := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", request, cfg)
			assertResumeLost(replay)
			if got := server.cfg.UIDAllocator.Remaining(); got != remaining {
				t.Fatalf("duplicate durable-loss create consumed identity capacity: remaining=%d want=%d", got, remaining)
			}
		})
	}
}

func TestCreateSessionStagesCurrentGenerationForDurableTransitionRetry(t *testing.T) {
	cfg, profile := newTestConfigWithUpstream(
		t,
		"immediate",
		"http://127.0.0.1:1",
		strings.Repeat("p", 32),
	)
	cfg.DurableWorkspaceDir = t.TempDir()
	request := testCreateSessionRequest(t, cfg, profile)
	request.Metadata.Fence.RuntimeSessionGeneration = 6
	request.Workspace.ExpectDurableResume = true
	request.Workspace.ExpectDurableResumeMinGeneration = 5
	if _, _, err := cfg.UIDAllocator.AllocateAboveReserve(0); err != nil {
		t.Fatalf("allocate session identity: %v", err)
	}
	const (
		priorRepository = "github.com/orka-agents/prior"
		priorRevision   = "fedcba9876543210"
	)
	request.Workspace.ExpectDurableResumeFrom = acp.StableDurableWorkspaceIdentity(priorRepository, priorRevision)
	sessionUID := string(request.Metadata.Fence.RuntimeSessionUID)
	if _, _, err := acp.PrepareDurableSessionWorkspace(cfg.DurableWorkspaceDir, sessionUID, 1); err != nil {
		t.Fatalf("prepare prior checkpoint: %v", err)
	}
	if err := acp.CommitDurableSessionWorkspace(
		cfg.DurableWorkspaceDir,
		sessionUID,
		acp.DurableWorkspaceBinding{
			RepositoryIdentity:       priorRepository,
			Revision:                 priorRevision,
			SessionIdentityHighWater: 1,
			SessionGeneration:        request.Workspace.ExpectDurableResumeMinGeneration,
		},
	); err != nil {
		t.Fatalf("commit prior checkpoint: %v", err)
	}
	injected := errors.New("injected materialization failure")
	cfg.WorkspaceMaterializer = WorkspaceMaterializerFunc(func(
		context.Context,
		harnessv2.CreateRuntimeSessionRequest,
		string,
	) error {
		return injected
	})

	server := &Server{cfg: cfg}
	_, _, _, _, _, _, _, err := server.createSession(
		context.Background(), request, time.Now().UTC(), os.Getuid(), os.Getgid(),
	)
	if !errors.Is(err, injected) || sessionCreationStage(err) != "workspace materialization" {
		t.Fatalf("createSession error = %v, want injected materialization failure", err)
	}
	transition, err := acp.DurableWorkspaceTransitionTarget(cfg.DurableWorkspaceDir, sessionUID)
	if err != nil {
		t.Fatalf("read staged transition: %v", err)
	}
	if transition == nil || transition.SessionGeneration != request.Metadata.Fence.RuntimeSessionGeneration ||
		transition.SessionIdentityHighWater != 1 {
		t.Fatalf("staged transition = %+v, want session generation %d and identity high-water 1", transition, request.Metadata.Fence.RuntimeSessionGeneration)
	}
}

func TestSupervisorTombstonesFailedCreateToPreventIdentityExhaustion(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	now := time.Now().UTC()

	// Simulate a create that allocated its identity and then failed during
	// session initialization.
	server.mu.Lock()
	server.sessions[create.RuntimeSessionID] = &sessionState{id: create.RuntimeSessionID, creating: true}
	server.tombstoneFailedCreateLocked(create.RuntimeSessionID, create.Metadata, now, nil)
	_, resident := server.sessions[create.RuntimeSessionID]
	tombstone, tombstoned := server.tombstones[create.Metadata.Fence.RuntimeSessionUID]
	server.mu.Unlock()
	if resident {
		t.Fatal("failed create left the session resident")
	}
	if !tombstoned || tombstone.RuntimeSessionGeneration != create.Metadata.Fence.RuntimeSessionGeneration {
		t.Fatalf("failed create did not record a matching tombstone: %#v", tombstone)
	}

	// Replaying the same create must not allocate another identity; it is
	// classified against the tombstone rather than recreated.
	replay := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if replay.Code == http.StatusCreated {
		t.Fatalf("replayed failed create allocated a new session: body=%s", replay.Body.String())
	}
	server.mu.Lock()
	_, residentAfterReplay := server.sessions[create.RuntimeSessionID]
	server.mu.Unlock()
	if residentAfterReplay {
		t.Fatal("replayed failed create became resident")
	}
}

func TestDeleteSessionDefersCleanupUntilCancellationSettles(t *testing.T) {
	tests := []struct {
		name            string
		state           harnessv2.RuntimeSessionState
		creating        bool
		prompt          bool
		wantCancelCalls int
		wantState       harnessv2.RuntimeSessionState
	}{
		{name: "creating", state: harnessv2.RuntimeSessionStateCreating, creating: true, wantState: harnessv2.RuntimeSessionStateCreating},
		{name: "prompt before acceptance", state: harnessv2.RuntimeSessionStateIdle, prompt: true, wantCancelCalls: 1, wantState: harnessv2.RuntimeSessionStateIdle},
		{name: "prompt running", state: harnessv2.RuntimeSessionStatePromptRunning, prompt: true, wantCancelCalls: 1, wantState: harnessv2.RuntimeSessionStateCancelling},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, cfg, profile := newTestServer(t, "immediate")
			create := testCreateSessionRequest(t, cfg, profile)
			now := time.Now().UTC()
			state := &sessionState{
				id: create.RuntimeSessionID, creating: test.creating,
				descriptor: harnessv2.RuntimeSessionDescriptor{
					RuntimeSessionID: create.RuntimeSessionID, RuntimeSessionUID: create.Metadata.Fence.RuntimeSessionUID,
					Generation: create.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: cfg.Fence.RuntimeInstanceID,
					SupervisorBootID: cfg.Fence.SupervisorBootID, RuntimeProfileDigest: cfg.Fence.RuntimeProfileDigest,
					State: test.state, CreatedAt: now, LastTransitionAt: now,
				},
				operations: map[harnessv2.OperationID]harnessv2.OperationRecord{},
			}
			mutations := &recordingPromptMutator{}
			if test.prompt {
				promptMetadata := create.Metadata
				promptMetadata.PromptID = "prompt-1"
				state.prompt = &promptState{request: harnessv2.StartPromptRequest{Metadata: promptMetadata}}
				state.promptMutations = mutations
			}
			server.mu.Lock()
			server.sessions[create.RuntimeSessionID] = state
			server.mu.Unlock()

			metadata := create.Metadata
			metadata.OperationID = harnessv2.OperationID("delete-" + strings.ReplaceAll(test.name, " ", "-"))
			metadata.ExpiresAt = now.Add(time.Minute)
			request := harnessv2.DeleteRuntimeSessionRequest{
				Protocol: harnessv2.ProtocolVersion, Metadata: metadata, Reason: "race cleanup",
			}
			sealRequest(t, &request.Metadata.RequestDigest, request)
			response := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", request, cfg)
			if response.Code != http.StatusConflict {
				t.Fatalf("delete status=%d body=%s", response.Code, response.Body.String())
			}
			var apiError harnessv2.ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &apiError); err != nil {
				t.Fatal(err)
			}
			if apiError.Code != harnessv2.ErrorCodeAlreadyAccepted || !apiError.Retryable {
				t.Fatalf("delete error=%#v, want retryable already_accepted", apiError)
			}
			server.mu.Lock()
			resident := server.sessions[create.RuntimeSessionID]
			server.mu.Unlock()
			if resident != state {
				t.Fatal("delete removed the unsettled runtime session")
			}
			if state.descriptor.State != test.wantState {
				t.Fatalf("session state=%s, want %s", state.descriptor.State, test.wantState)
			}
			if got := mutations.cancelCalls; got != test.wantCancelCalls {
				t.Fatalf("CancelPrompt calls=%d, want %d", got, test.wantCancelCalls)
			}
		})
	}
}

type recordingPromptMutator struct {
	cancelCalls int
}

func (*recordingPromptMutator) ResolvePermission(string, string, acp.RequestPermissionOutcome) error {
	return nil
}

func (m *recordingPromptMutator) CancelPrompt(context.Context, string) (acp.PromptResult, error) {
	m.cancelCalls++
	return acp.PromptResult{Outcome: acp.PromptOutcomeCancelled}, nil
}

func TestSupervisorCreateAndPrompt(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}
	var created harnessv2.CreateRuntimeSessionResponse
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if err := created.ValidateFor(create); err != nil {
		t.Fatalf("create response validation: %v", err)
	}

	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	response = performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("prompt status = %d body=%s", response.Code, response.Body.String())
	}
	decoder, err := harnessv2.NewEventDecoder(bytes.NewReader(response.Body.Bytes()), eventLimits(cfg.Capabilities.Limits), harnessv2.EventExpectationFromMetadata(prompt.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("decode prompt events: %v\n%s", err, response.Body.String())
	}
	if len(events) != 3 || events[0].Type != harnessv2.EventAccepted || events[1].Type != harnessv2.EventUpdate || events[2].Type != harnessv2.EventCompleted {
		t.Fatalf("unexpected event sequence: %#v", events)
	}
	if got := events[2].Completed.Result.Content[0].Text; got != "hello from ACP" {
		t.Fatalf("result text = %q", got)
	}

	statusReq := httptest.NewRequest(http.MethodGet, harnessv2.StatusPath, nil)
	statusReq.Header.Set("Authorization", "Bearer "+cfg.ControllerBearerToken)
	statusNonce, err := harnessv2.NewCapabilityNonce()
	if err != nil {
		t.Fatal(err)
	}
	statusBinding := harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: cfg.Fence.RuntimeProfileDigest}
	statusCapability, err := harnessv2.SignStatusCapability(cfg.CapabilitySecret, harnessv2.NewStatusCapabilityClaims(statusBinding, statusNonce, time.Now().UTC().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	statusReq.Header.Set(OperationCapabilityHeader, statusCapability)
	statusResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(statusResponse, statusReq)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	var status harnessv2.StatusResponse
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("status response validation: %v", err)
	}
	if len(status.Sessions) != 1 || status.Sessions[0].State != harnessv2.RuntimeSessionStateValidating {
		t.Fatalf("unexpected session status: %#v", status.Sessions)
	}
}

func TestSupervisorCompactsAssistantBurstBeforeHarnessRateLimit(t *testing.T) {
	server, cfg, profile := newTestServer(t, assistantBurstMode)
	server.cfg.Capabilities.Limits.MaxBufferedEvents = 2048
	cfg.Capabilities.Limits.MaxBufferedEvents = 2048
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}

	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	response = performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("prompt status = %d body=%s", response.Code, response.Body.String())
	}
	decoder, err := harnessv2.NewEventDecoder(
		bytes.NewReader(response.Body.Bytes()), eventLimits(cfg.Capabilities.Limits),
		harnessv2.EventExpectationFromMetadata(prompt.Metadata),
	)
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("decode compacted prompt events: %v", err)
	}
	if len(events) < 3 || events[0].Type != harnessv2.EventAccepted ||
		events[len(events)-1].Type != harnessv2.EventCompleted {
		t.Fatalf("unexpected compacted event sequence: %#v", events)
	}
	var streamed strings.Builder
	for index, event := range events {
		if event.Identity.Sequence != uint64(index+1) {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Identity.Sequence, index+1)
		}
		if event.Type == harnessv2.EventUpdate && event.Update != nil && event.Update.AssistantMessage != nil {
			streamed.WriteString(event.Update.AssistantMessage.Text)
		}
	}
	want := strings.Repeat("x", runtimeMaxUpdateEventsPerSecond+1)
	if streamed.String() != want {
		t.Fatalf("streamed assistant bytes = %d, want %d exact bytes", streamed.Len(), len(want))
	}
	terminal := events[len(events)-1].Completed
	if terminal == nil || len(terminal.Result.Content) != 1 || terminal.Result.Content[0].Text != want {
		t.Fatalf("terminal result did not preserve the exact assistant burst")
	}
}

func TestSupervisorProjectsRPCFailureAfterToolOutputBurst(t *testing.T) {
	server, cfg, profile := newTestServer(t, toolBurstRPCErrorMode)
	server.cfg.Capabilities.Limits.MaxBufferedEvents = 2048
	server.cfg.Capabilities.Limits.MaxUpdateEventsPerSecond = 2
	cfg.Capabilities.Limits.MaxBufferedEvents = 2048
	cfg.Capabilities.Limits.MaxUpdateEventsPerSecond = 2
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}

	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	response = performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("prompt status = %d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "provider-secret-must-not-leak") {
		t.Fatalf("prompt response leaked provider error detail: %s", response.Body.String())
	}
	decoder, err := harnessv2.NewEventDecoder(
		bytes.NewReader(response.Body.Bytes()), eventLimits(cfg.Capabilities.Limits),
		harnessv2.EventExpectationFromMetadata(prompt.Metadata),
	)
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("decode failed prompt events: %v\n%s", err, response.Body.String())
	}
	if len(events) != 4 || events[0].Type != harnessv2.EventAccepted ||
		events[1].Type != harnessv2.EventUpdate || events[1].Update == nil ||
		events[1].Update.Kind != harnessv2.UpdateToolCall ||
		events[2].Type != harnessv2.EventUpdate || events[2].Update == nil ||
		events[2].Update.Kind != harnessv2.UpdateToolCallUpdate ||
		events[3].Type != harnessv2.EventFailed || events[3].Failed == nil {
		t.Fatalf("unexpected failed prompt event sequence: %#v", events)
	}
	for index, event := range events {
		if event.Identity.Sequence != uint64(index+1) {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Identity.Sequence, index+1)
		}
	}
	if events[1].Update.ToolCall == nil ||
		events[1].Update.ToolCall.Status != harnessv2.ToolCallStatusPending ||
		events[2].Update.ToolCall == nil ||
		events[2].Update.ToolCall.Status != harnessv2.ToolCallStatusCompleted {
		t.Fatalf("tool lifecycle events = %#v / %#v", events[1].Update, events[2].Update)
	}
	if events[3].Failed.Code != "acp_prompt_failed" || events[3].Failed.Retryable {
		t.Fatalf("RPC failure terminal = %#v", events[3].Failed)
	}
}

func TestSupervisorWaitsForAgentConfigurationCapableController(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	create.AgentConfiguration = nil
	create.Metadata.RequestDigest = ""
	sealRequest(t, &create.Metadata.RequestDigest, create)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusTooManyRequests || !strings.Contains(response.Body.String(), string(harnessv2.ErrorCodeRateLimited)) {
		t.Fatalf("legacy controller create status = %d body=%s", response.Code, response.Body.String())
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.sessions) != 0 {
		t.Fatalf("legacy controller request created sessions: %#v", server.sessions)
	}
}

func TestSupervisorProviderSessionProjection(t *testing.T) {
	server, cfg, profile := newTestServer(t, providerProjectionCanaryMode)
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestSupervisorProviderProxyCanary(t *testing.T) {
	type observedRequest struct {
		path   string
		query  string
		header http.Header
		body   string
	}
	observed := make(chan observedRequest, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- observedRequest{path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: string(body)}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	server, cfg, profile := newTestServerWithUpstream(t, providerProxyCanaryMode, upstream.URL+"/v1", supervisorUpstreamTokenCanary)
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}

	server.mu.Lock()
	state := server.sessions[create.RuntimeSessionID]
	if state == nil || state.providerProxy == nil {
		server.mu.Unlock()
		t.Fatal("runtime session omitted its provider proxy binding")
	}
	baseURL := state.providerProxy.baseURL
	localCredential := string(append([]byte(nil), state.providerProxy.credential...))
	server.mu.Unlock()

	idle := doProviderProxyRequest(t, http.MethodPost, baseURL+"/responses", localCredential, []byte(`{}`), nil)
	if idle.StatusCode != http.StatusForbidden {
		t.Fatalf("idle provider request status = %d, want %d", idle.StatusCode, http.StatusForbidden)
	}
	_ = idle.Body.Close()
	select {
	case request := <-observed:
		t.Fatalf("idle ACP child provider request reached upstream: %#v", request)
	default:
	}

	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	promptRequest := mutationHTTPRequest(t, http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	promptResponse := httptest.NewRecorder()
	promptDone := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(promptResponse, promptRequest)
		close(promptDone)
	}()

	var upstreamRequest observedRequest
	select {
	case upstreamRequest = <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("ACP child did not reach provider upstream during active prompt")
	}
	if upstreamRequest.path != providerOpenAIResponsesV1Path || upstreamRequest.query != "from=acp-child" || upstreamRequest.body != `{"model":"test-model","canary":true}` {
		t.Fatalf("unexpected provider upstream request: %#v", upstreamRequest)
	}
	if got := upstreamRequest.header.Get(providerAuthorizationHeader); got != "Bearer "+supervisorUpstreamTokenCanary {
		t.Fatalf("upstream authorization = %q", got)
	}
	for _, name := range []string{
		providerAPIKeyHeader, providerCookieHeader, providerProxyAuthorizationHeader,
		providerForwardedForHeader, "X-Child-Secret",
	} {
		if value := upstreamRequest.header.Get(name); value != "" {
			t.Fatalf("child-local sensitive header %s reached upstream: %q", name, value)
		}
	}
	if upstreamRequest.header.Get("X-Canary-Safe") != "preserved" {
		t.Fatalf("safe child header missing upstream: %v", upstreamRequest.header)
	}
	close(release)
	select {
	case <-promptDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not settle after provider response")
	}
	if promptResponse.Code != http.StatusOK {
		t.Fatalf("prompt status = %d body=%s", promptResponse.Code, promptResponse.Body.String())
	}

	server.mu.Lock()
	if state.descriptor.State != harnessv2.RuntimeSessionStateValidating || state.prompt == nil || state.prompt.settlement == nil {
		server.mu.Unlock()
		t.Fatalf("unexpected post-terminal session state: %#v", state.descriptor)
	}
	server.mu.Unlock()
	late := doProviderProxyRequest(t, http.MethodPost, baseURL+"/responses", localCredential, []byte(`{}`), nil)
	if late.StatusCode != http.StatusForbidden {
		t.Fatalf("post-terminal provider request status = %d, want %d", late.StatusCode, http.StatusForbidden)
	}
	_ = late.Body.Close()

	server.poisonSession(state, "test")
	poisoned := doProviderProxyRequest(t, http.MethodPost, baseURL+"/responses", localCredential, []byte(`{}`), nil)
	if poisoned.StatusCode != http.StatusForbidden && poisoned.StatusCode != http.StatusNotFound {
		t.Fatalf("poisoned provider request status = %d, want %d or %d", poisoned.StatusCode, http.StatusForbidden, http.StatusNotFound)
	}
	_ = poisoned.Body.Close()
	select {
	case request := <-observed:
		t.Fatalf("post-terminal or poisoned request reached upstream: %#v", request)
	default:
	}
}

func TestSupervisorConcurrentLeaseRenewalHasSingleWinner(t *testing.T) {
	server, cfg, profile := newTestServer(t, "wait")
	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", response.Code, response.Body.String())
	}
	server.mu.Lock()
	providerProxy := server.sessions[create.RuntimeSessionID].providerProxy
	baseURL := providerProxy.baseURL
	localCredential := string(append([]byte(nil), providerProxy.credential...))
	server.mu.Unlock()
	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	promptContext, cancelPrompt := context.WithCancel(context.Background())
	promptRequest := mutationHTTPRequest(
		t, http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg,
	).WithContext(promptContext)
	promptResponse := httptest.NewRecorder()
	promptDone := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(promptResponse, promptRequest)
		close(promptDone)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-promptDone:
			t.Fatalf("prompt ended before renewal: status=%d body=%s", promptResponse.Code, promptResponse.Body.String())
		default:
		}
		server.mu.Lock()
		state := server.sessions[create.RuntimeSessionID]
		running := state != nil && state.descriptor.State == harnessv2.RuntimeSessionStatePromptRunning
		server.mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			cancelPrompt()
			t.Fatal("prompt did not become active")
		}
		time.Sleep(5 * time.Millisecond)
	}

	issuedAt := time.Now().UTC()
	lease := harnessv2.PromptLease{
		Generation: 2, IssuedAt: issuedAt, ExpiresAt: prompt.Lease.ExpiresAt.Add(30 * time.Second),
	}
	requests := make([]*http.Request, 0, 2)
	for _, operationID := range []string{"renew-a", "renew-b"} {
		metadata := testMetadata(create.Metadata.Fence, operationID, true)
		metadata.PromptID = prompt.Metadata.PromptID
		metadata.ExpiresAt = issuedAt.Add(30 * time.Second)
		authorization := prompt.MCPAuthorization
		authorization.LeaseGeneration = lease.Generation
		authorization.ExpiresAt = metadata.ExpiresAt
		renew := harnessv2.RenewPromptLeaseRequest{
			Protocol: harnessv2.ProtocolVersion, Metadata: metadata,
			ExpectedLeaseGeneration: 1, Lease: lease, MCPAuthorization: authorization,
		}
		sealRequest(t, &renew.Metadata.RequestDigest, renew)
		requests = append(requests, mutationHTTPRequest(
			t, http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1/lease", renew, cfg,
		))
	}
	statuses := make(chan int, len(requests))
	for _, request := range requests {
		go func() {
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			statuses <- recorder.Code
		}()
	}
	winners := 0
	for range requests {
		if <-statuses == http.StatusOK {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent lease renewal winners = %d, want 1", winners)
	}
	cancelPrompt()
	select {
	case <-promptDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not settle after request cancellation")
	}
	providerResponse := doProviderProxyRequest(t, http.MethodPost, baseURL+"/responses", localCredential, []byte(`{}`), nil)
	defer providerResponse.Body.Close() //nolint:errcheck
	if providerResponse.StatusCode != http.StatusForbidden && providerResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("post-cancellation provider proxy status = %d, want %d or %d", providerResponse.StatusCode, http.StatusForbidden, http.StatusNotFound)
	}
}

func TestSupervisorRejectsMissingAuthAndTamperedCapability(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	data, _ := json.Marshal(create)
	req := httptest.NewRequest(http.MethodPut, "/v2/runtime-sessions/session-1", bytes.NewReader(data))
	resp := httptest.NewRecorder()
	server.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", resp.Code)
	}

	req = httptest.NewRequest(http.MethodPut, "/v2/runtime-sessions/session-1", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+cfg.ControllerBearerToken)
	req.Header.Set(OperationCapabilityHeader, "tampered")
	resp = httptest.NewRecorder()
	server.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("tampered capability status = %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestSupervisorMarksAggregateAssistantOverflowForTerminalFailure(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	server.cfg.Capabilities.Limits.MaxTerminalResultBytes = 10
	fence := cfg.Fence
	fence.RuntimeSessionUID = "session-uid"
	fence.RuntimeSessionGeneration = 1
	prompt := &promptState{request: testStartPromptRequest(t, cfg, fence)}
	state := &sessionState{descriptor: harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionUID: fence.RuntimeSessionUID,
		Generation:        fence.RuntimeSessionGeneration,
	}}
	event := func(text string) acp.PromptEvent {
		update, err := json.Marshal(map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": text},
		})
		if err != nil {
			t.Fatal(err)
		}
		return acp.PromptEvent{
			Type:      acp.PromptEventUpdate,
			Timestamp: time.Now().UTC(),
			Update:    &acp.SessionNotification{SessionID: "provider-session", Update: update},
		}
	}

	for _, chunk := range []string{"12345", "67890"} {
		if _, err := server.mapRuntimeEvent(state, prompt, event(chunk)); err != nil {
			t.Fatalf("map chunk %q: %v", chunk, err)
		}
	}
	if _, err := server.mapRuntimeEvent(state, prompt, event("x")); err != nil {
		t.Fatalf("map overflow chunk: %v", err)
	}
	if got := prompt.assistant.String(); got != "1234567890" {
		t.Fatalf("assistant result = %q, want bounded aggregate", got)
	}
	if !prompt.assistantOverflow {
		t.Fatal("aggregate overflow was not retained for terminal failure")
	}
	if prompt.sequence != 3 {
		t.Fatalf("sequence = %d, want 3 emitted updates", prompt.sequence)
	}
}

func TestSupervisorUsesCodexFinalAnswerAsTerminalResult(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	server.cfg.Capabilities.Limits.MaxTerminalResultBytes = 4096
	fence := cfg.Fence
	fence.RuntimeSessionUID = "phase-session-uid"
	fence.RuntimeSessionGeneration = 1
	prompt := &promptState{request: testStartPromptRequest(t, cfg, fence)}
	state := &sessionState{descriptor: harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionUID: fence.RuntimeSessionUID,
		Generation:        fence.RuntimeSessionGeneration,
	}}
	now := time.Now().UTC()
	events := []acp.PromptEvent{
		testAssistantMessagePromptEventWithPhase(
			t, 1, now, "commentary-message", acpAssistantPhaseCommentary, strings.Repeat("x", 4097),
		),
		testAssistantMessagePromptEventWithPhase(
			t, 2, now.Add(time.Millisecond), "final-message", acpAssistantPhaseFinalAnswer, `{"schemaVersion":1,"ok":true}`,
		),
	}
	for _, event := range events {
		mapped, err := server.mapRuntimeEvent(state, prompt, event)
		if err != nil {
			t.Fatalf("map phased assistant event: %v", err)
		}
		if mapped == nil || mapped.Update == nil || mapped.Update.AssistantMessage == nil {
			t.Fatalf("phased assistant event was not visible: %#v", mapped)
		}
	}
	if !prompt.assistantOverflow || !prompt.finalAnswerSeen || prompt.finalAnswerOverflow {
		t.Fatalf(
			"prompt aggregation = assistantOverflow=%v finalSeen=%v finalOverflow=%v",
			prompt.assistantOverflow, prompt.finalAnswerSeen, prompt.finalAnswerOverflow,
		)
	}
	terminal, result, err := server.terminalEvent(state, prompt, acp.PromptResult{
		Outcome: acp.PromptOutcomeCompleted, StopReason: acp.StopReasonEndTurn,
		Accepted: true, SettledAt: now.Add(2 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("build phased terminal result: %v", err)
	}
	if result.Outcome != acp.PromptOutcomeCompleted || terminal.Type != harnessv2.EventCompleted ||
		terminal.Completed == nil || len(terminal.Completed.Result.Content) != 1 ||
		terminal.Completed.Result.Content[0].Text != `{"schemaVersion":1,"ok":true}` {
		t.Fatalf("phased terminal result = %#v settled=%#v", terminal, result)
	}
}

func TestSupervisorDrainIsIdempotent(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	request := harnessv2.DrainRequest{Protocol: harnessv2.ProtocolVersion, Metadata: harnessv2.MutationMetadata{
		Fence: cfg.Fence, OperationID: "drain-1", RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
		ExpiresAt: time.Now().UTC().Add(time.Minute),
	}, Reason: "test"}
	sealRequest(t, &request.Metadata.RequestDigest, request)
	first := performMutation(t, server.Handler(), http.MethodPut, harnessv2.DrainPath, request, cfg)
	if first.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", first.Code, first.Body.String())
	}
	second := performMutation(t, server.Handler(), http.MethodPut, harnessv2.DrainPath, request, cfg)
	if second.Code != http.StatusOK {
		t.Fatalf("duplicate drain status=%d body=%s", second.Code, second.Body.String())
	}
	var response harnessv2.DrainResponse
	if err := json.Unmarshal(second.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Classification.Class != harnessv2.RequestClassificationDuplicate || response.Drain.AcceptingNewSessions {
		t.Fatalf("unexpected duplicate drain response: %#v", response)
	}
}

func TestSupervisorPreservesIdentityReserveAndClosesOnlySessionAdmission(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65534, 65534
	}
	allocator, err := acp.NewUIDAllocator(uid, uid+1, gid)
	if err != nil {
		t.Fatal(err)
	}
	server.cfg.UIDAllocator = allocator

	create := testCreateSessionRequest(t, cfg, profile)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	status := server.status()
	if status.SessionIdentityCapacity == nil {
		t.Fatal("session identity capacity was not reported")
	}
	if got := *status.SessionIdentityCapacity; got.Total != 2 || got.Remaining != 1 || got.ExhaustionReserve != 1 {
		t.Fatalf("identity capacity = %#v, want total=2 remaining=1 reserve=1", got)
	}
	if status.Drain.AcceptingNewSessions || status.Drain.Requested || status.Lifecycle != harnessv2.SupervisorLifecycleReady {
		t.Fatalf("capacity watermark status = lifecycle=%s drain=%#v, want ready with only new-session admission closed", status.Lifecycle, status.Drain)
	}

	healthRequest := httptest.NewRequest(http.MethodGet, harnessv2.HealthPath, nil)
	healthResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthResponse, healthRequest)
	var health harnessv2.HealthResponse
	if err := json.Unmarshal(healthResponse.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if healthResponse.Code != http.StatusOK || health.Status != harnessv2.HealthStatusDegraded {
		t.Fatalf("health = HTTP %d / %s, want 200 / degraded", healthResponse.Code, health.Status)
	}

	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	response = performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("prompt for admitted session status=%d body=%s", response.Code, response.Body.String())
	}

	second := testCreateSessionRequest(t, cfg, profile)
	second.RuntimeSessionID = "session-2"
	second.Metadata.Fence.RuntimeSessionUID = "session-uid-2"
	second.Metadata.OperationID = "create-session-2"
	second.Metadata.RequestDigest = ""
	sealRequest(t, &second.Metadata.RequestDigest, second)
	response = performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-2", second, cfg)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("create after identity watermark status=%d body=%s", response.Code, response.Body.String())
	}
	if allocator.Remaining() != 1 {
		t.Fatalf("remaining identities = %d, want protected reserve of 1", allocator.Remaining())
	}
}

func TestSupervisorStatusReportsCreatingRuntimeSession(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	entered := make(chan struct{})
	release := make(chan struct{})
	server.cfg.Provider.PrepareSession = func(acp.SessionPaths) error {
		close(entered)
		<-release
		return nil
	}
	create := testCreateSessionRequest(t, cfg, profile)
	request := mutationHTTPRequest(t, http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(response, request)
		close(done)
	}()
	<-entered
	status := server.status()
	if len(status.Sessions) != 1 || status.Sessions[0].RuntimeSessionID != create.RuntimeSessionID ||
		status.Sessions[0].RuntimeSessionUID != create.Metadata.Fence.RuntimeSessionUID ||
		status.Sessions[0].Generation != create.Metadata.Fence.RuntimeSessionGeneration ||
		status.Sessions[0].State != harnessv2.RuntimeSessionStateCreating {
		t.Fatalf("creating RuntimeSession status = %#v", status.Sessions)
	}
	if status.Pressure.ResidentSessions != 1 {
		t.Fatalf("resident pressure = %d, want 1", status.Pressure.ResidentSessions)
	}
	close(release)
	<-done
	if response.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
}

func newTestServer(t *testing.T, mode string) (*Server, Config, harnessv2.RuntimeProfile) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	t.Cleanup(upstream.Close)
	return newTestServerWithUpstream(t, mode, upstream.URL, strings.Repeat("p", 32))
}

func newTestServerWithUpstream(t *testing.T, mode, upstreamURL, upstreamToken string) (*Server, Config, harnessv2.RuntimeProfile) {
	t.Helper()
	cfg, profile := newTestConfigWithUpstream(t, mode, upstreamURL, upstreamToken)
	if mode == providerProjectionCanaryMode {
		cfg.Provider.ProjectSession = func(_ harnessv2.CreateRuntimeSessionRequest, _ acp.SessionPaths, _ ProviderProxyBinding) (ProviderSessionProjection, error) {
			return ProviderSessionProjection{
				Environment:    map[string]string{"ORKA_TEST_PROJECTION": providerProjectionCanaryValue},
				NewSessionMeta: acp.Meta{"provider.canary": providerProjectionCanaryValue},
			}, nil
		}
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Close(ctx)
	})
	return server, cfg, profile
}

func newTestConfigWithUpstream(t *testing.T, mode, upstreamURL, upstreamToken string) (Config, harnessv2.RuntimeProfile) {
	t.Helper()
	if runtime.GOOS == "linux" && os.Geteuid() != 0 {
		groups, err := os.Getgroups()
		if err != nil {
			t.Fatal(err)
		}
		if len(groups) != 0 {
			t.Skipf("non-root test environment cannot clear supplementary groups %v without weakening the production fence", groups)
		}
	}
	policyConfiguration := testEmptyMCPPolicyConfiguration(t)
	agentConfiguration := harnessv2.AgentSessionConfiguration{
		AgentUID: testProjectionAgentUID, AgentGeneration: 1, ProviderKind: providerKindCodex, Model: "test-model", MaxTurns: 7,
	}
	agentDigest, err := harnessv2.CanonicalAgentConfigurationDigest(agentConfiguration)
	if err != nil {
		t.Fatal(err)
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile: harnessv2.ACPProfileV1, AdapterDigests: map[string]string{"fake-acp": testDigest("adapter")},
		ProviderKind: providerKindCodex, Model: "test-model", AgentConfigurationDigest: agentDigest,
		ToolPolicyDigest: policyConfiguration.ToolPolicyDigest, ApprovalPolicyDigest: policyConfiguration.ApprovalPolicyDigest,
		MCPConfigurationDigest: policyConfiguration.MCPConfigurationDigest,
		WorkspaceIntent:        harnessv2.WorkspaceIntentRead, ProxyCredentialRole: "provider", ProxyCredentialScope: "model:test-model", ResourceClass: "standard",
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65534, 65534
	}
	allocator, err := acp.NewUIDAllocator(uid, uid+10, gid)
	if err != nil {
		t.Fatal(err)
	}
	command, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	provider := ProviderProfile{
		Kind: "codex", Model: "test-model", Command: command, Args: []string{"-test.run=TestSupervisorACPHelper"},
		Environment: map[string]string{"GO_WANT_SUPERVISOR_ACP_HELPER": "1", "SUPERVISOR_ACP_HELPER_MODE": mode},
		AdapterName: "fake-acp", AdapterDigest: testDigest("adapter"),
	}
	if mode == providerProxyCanaryMode {
		provider.EnvironmentForSession = func(_ harnessv2.CreateRuntimeSessionRequest, _ acp.SessionPaths, proxy ProviderProxyBinding) (map[string]string, error) {
			config, err := json.Marshal(map[string]string{"openai_base_url": proxy.BaseURL})
			if err != nil {
				return nil, err
			}
			return map[string]string{"CODEX_API_KEY": proxy.Credential, "CODEX_CONFIG": string(config)}, nil
		}
	}
	cfg := Config{
		ListenAddress: ":0",
		Fence: harnessv2.Fence{
			RuntimeInstanceID: "runtime-instance", SupervisorBootID: "boot-id", ControllerEpoch: 1,
			RuntimePoolUID: "pool-uid", RuntimePoolGeneration: 1, RuntimeProfileDigest: profileDigest,
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		Capabilities: harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests: map[string]string{"fake-acp": testDigest("adapter")},
			Limits: harnessv2.ProtocolLimits{
				MaxResidentSessions: 10, MaxConcurrentPrompts: 4, MaxRequestBytes: 1 << 20,
				MaxEventLineBytes: 1 << 20, MaxTerminalResultBytes: 1 << 20, MaxBufferedEvents: 64,
				MaxUpdateEventsPerSecond: 1000, MinPromptLeaseMillis: 1000, MaxPromptLeaseMillis: 120000,
				MaxPendingPermissions: 16, MaxWorkspaceDeltaBytes: 1 << 20,
			},
			Provider:                          harnessv2.ProviderCapabilities{ProviderKinds: []string{"codex"}, Models: []string{"test-model"}, SupportsPermissions: true, SupportsCancel: true},
			WorkspaceGovernance:               harnessv2.StrictWorkspaceGovernanceCapabilities(),
			SupportsDrain:                     true,
			SupportsPublicationFinalization:   true,
			SupportsAgentSessionConfiguration: true,
		},
		Provider:              provider,
		ControllerBearerToken: strings.Repeat("t", 32), CapabilitySecret: []byte(strings.Repeat("s", 32)), RequireCapabilities: true,
		SessionBaseDir: filepath.Join(t.TempDir(), "sessions"), UIDAllocator: allocator,
		ProviderProxy: ProviderProxyConfig{
			UpstreamBaseURL: upstreamURL, UpstreamBearerToken: upstreamToken,
			ProviderKind: providerKindCodex, Model: "test-model",
		},
		MCPBroker: MCPBrokerFunc(func(_ context.Context, request harnessv2.MCPBrokerCallRequest) (harnessv2.MCPBrokerCallResponse, error) {
			return harnessv2.MCPBrokerCallResponse{
				Protocol: harnessv2.ProtocolVersion, CallID: request.Call.CallID, Result: json.RawMessage(`{"ok":true}`),
			}, nil
		}),
		WorkspaceMaterializer: EmptyWorkspaceMaterializer(),
		InitializeTimeout:     5 * time.Second, PermissionTimeout: 5 * time.Second, CancelGrace: time.Second,
	}
	return cfg, profile
}

func testCreateSessionRequest(t *testing.T, cfg Config, profile harnessv2.RuntimeProfile) harnessv2.CreateRuntimeSessionRequest {
	t.Helper()
	fence := cfg.Fence
	fence.RuntimeSessionUID = "session-uid-1"
	fence.RuntimeSessionGeneration = 1
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: testMetadata(fence, "create-session-1", false), RuntimeSessionID: "session-1", Profile: profile,
		AgentConfiguration: testServerAgentConfiguration(profile),
		MCPConfiguration:   testServerMCPPolicyConfiguration(profile),
		Workspace: harnessv2.WorkspaceSpec{Intent: harnessv2.WorkspaceIntentRead, Baseline: harnessv2.WorkspaceBaseline{
			RepositoryIdentity: "github.com/orka-agents/orka", Revision: "0123456789abcdef", TreeDigest: testDigest("tree"),
		}},
	}
	sealRequest(t, &request.Metadata.RequestDigest, request)
	return request
}

func testServerAgentConfiguration(profile harnessv2.RuntimeProfile) *harnessv2.AgentSessionConfiguration {
	return &harnessv2.AgentSessionConfiguration{
		AgentUID: testProjectionAgentUID, AgentGeneration: 1, ProviderKind: profile.ProviderKind, Model: profile.Model,
		MaxTurns: 7,
	}
}

func TestCreateSessionPreservesWorkspaceRelativeRootForDelta(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	create.Workspace.RelativeRoot = testWorkspaceRelativeRoot
	create.Metadata.RequestDigest = ""
	sealRequest(t, &create.Metadata.RequestDigest, create)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", created.Code, created.Body.String())
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if got := server.sessions[create.RuntimeSessionID].workspaceRelativeRoot; got != testWorkspaceRelativeRoot {
		t.Fatalf("workspace relative root = %q, want services/app", got)
	}
}

//nolint:unparam // The stable parameter keeps call sites explicit across related test cases.
func testStartPromptRequest(t *testing.T, cfg Config, fence harnessv2.Fence) harnessv2.StartPromptRequest {
	t.Helper()
	now := time.Now().UTC()
	metadata := testMetadata(fence, "prompt-operation-1", true)
	metadata.PromptID = testPromptOneID
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	metadata.ExpiresAt = now.Add(30 * time.Second)
	policyConfiguration := testEmptyMCPPolicyConfiguration(t)
	request := harnessv2.StartPromptRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata, Lease: lease,
		MCPAuthorization: harnessv2.PromptMCPAuthorization{
			RuntimeSessionUID: fence.RuntimeSessionUID, SessionGeneration: fence.RuntimeSessionGeneration,
			TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID, LeaseGeneration: 1,
			ToolPolicyDigest: policyConfiguration.ToolPolicyDigest, ApprovalPolicyDigest: policyConfiguration.ApprovalPolicyDigest,
			MCPConfigurationDigest: policyConfiguration.MCPConfigurationDigest, ToolPolicy: policyConfiguration.ToolPolicy,
			ApprovalPolicy: policyConfiguration.ApprovalPolicy, ExpiresAt: now.Add(30 * time.Second),
		},
		Input: harnessv2.PromptInput{Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "hello"}}},
	}
	sealRequest(t, &request.Metadata.RequestDigest, request)
	return request
}

func testEmptyMCPPolicyConfiguration(t *testing.T) harnessv2.MCPPolicyConfiguration {
	t.Helper()
	toolPolicy := testEmptyMCPToolPolicy()
	approvalPolicy := harnessv2.MCPApprovalPolicy{}
	toolDigest, err := harnessv2.CanonicalRuntimeToolPolicyDigest(toolPolicy.AllowedToolNames, toolPolicy.DisallowedToolNames, toolPolicy.AllowBash)
	if err != nil {
		t.Fatal(err)
	}
	approvalDigest, err := harnessv2.CanonicalMCPApprovalPolicyDigest(approvalPolicy)
	if err != nil {
		t.Fatal(err)
	}
	mcpDigest, err := harnessv2.CanonicalMCPConfigurationDigest(toolPolicy.AllowedToolNames)
	if err != nil {
		t.Fatal(err)
	}
	return harnessv2.MCPPolicyConfiguration{
		ToolPolicyDigest: toolDigest, ApprovalPolicyDigest: approvalDigest, MCPConfigurationDigest: mcpDigest,
		ToolPolicy: toolPolicy, ApprovalPolicy: approvalPolicy,
	}
}

func testServerMCPPolicyConfiguration(profile harnessv2.RuntimeProfile) harnessv2.MCPPolicyConfiguration {
	return harnessv2.MCPPolicyConfiguration{
		ToolPolicyDigest: profile.ToolPolicyDigest, ApprovalPolicyDigest: profile.ApprovalPolicyDigest,
		MCPConfigurationDigest: profile.MCPConfigurationDigest,
		ToolPolicy:             testEmptyMCPToolPolicy(), ApprovalPolicy: harnessv2.MCPApprovalPolicy{},
	}
}

func testEmptyMCPToolPolicy() harnessv2.MCPToolPolicy {
	digest, _ := harnessv2.CanonicalMCPToolDescriptorDigest([]harnessv2.MCPToolDescriptor{})
	return harnessv2.MCPToolPolicy{AllowedToolNames: []string{}, Tools: []harnessv2.MCPToolDescriptor{}, DescriptorDigest: digest}
}

func testMetadata(fence harnessv2.Fence, operation string, prompt bool) harnessv2.MutationMetadata {
	metadata := harnessv2.MutationMetadata{
		Fence: fence, TaskUID: "task-uid", TaskAttempt: 1, OperationID: harnessv2.OperationID(operation),
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	if prompt {
		metadata.PromptID = testPromptOneID
	}
	return metadata
}

func sealRequest(t *testing.T, target *harnessv2.RequestDigest, request any) {
	t.Helper()
	digest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	*target = digest
}

//nolint:unparam // Stable test helper signatures keep related cases uniform.
func performMutation(t *testing.T, handler http.Handler, method, path string, request any, cfg Config) *httptest.ResponseRecorder {
	t.Helper()
	req := mutationHTTPRequest(t, method, path, request, cfg)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}

func mutationHTTPRequest(t *testing.T, method, path string, request any, cfg Config) *http.Request {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var metadata harnessv2.MutationMetadata
	raw, _ := json.Marshal(request)
	var envelope struct {
		Metadata harnessv2.MutationMetadata `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	metadata = envelope.Metadata
	token, err := harnessv2.SignOperationCapability(cfg.CapabilitySecret, harnessv2.ClaimsForMutation(metadata))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+cfg.ControllerBearerToken)
	req.Header.Set(OperationCapabilityHeader, token)
	return req
}

func testDigest(label string) string {
	sum := sha256.Sum256([]byte(label))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestSupervisorACPHelper(t *testing.T) {
	if os.Getenv("GO_WANT_SUPERVISOR_ACP_HELPER") != "1" {
		return
	}
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush() //nolint:errcheck
	mode := os.Getenv("SUPERVISOR_ACP_HELPER_MODE")
	if mode == providerProxyCanaryMode {
		if err := validateProviderProxyChildIsolation(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if mode == providerProjectionCanaryMode && os.Getenv("ORKA_TEST_PROJECTION") != providerProjectionCanaryValue {
		fmt.Fprintln(os.Stderr, "provider session arguments or environment were not projected")
		os.Exit(2)
	}
	var sessionID = "provider-session"
	var promptID json.RawMessage
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			os.Exit(0)
		}
		var message struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(line, &message); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		switch message.Method {
		case acp.MethodInitialize:
			writeHelperMessage(writer, map[string]any{
				testJSONRPCKey: testJSONRPCVersion, "id": rawID(message.ID),
				"result": map[string]any{
					"protocolVersion":   acp.ProtocolVersion,
					"agentCapabilities": map[string]any{"mcpCapabilities": map[string]any{"http": true}},
				},
			})
		case acp.MethodSessionNew:
			if mode == providerProjectionCanaryMode {
				var request acp.NewSessionRequest
				if err := json.Unmarshal(message.Params, &request); err != nil || request.Meta["provider.canary"] != providerProjectionCanaryValue ||
					request.Meta["orka.runtimeSessionID"] != "session-1" || request.Meta["orka.profileDigest"] == "" {
					writeHelperMessage(writer, map[string]any{
						testJSONRPCKey: testJSONRPCVersion, "id": rawID(message.ID),
						"error": map[string]any{"code": -32602, "message": "provider session metadata was not projected"},
					})
					continue
				}
			}
			writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "id": rawID(message.ID), "result": map[string]any{"sessionId": sessionID}})
		case acp.MethodSessionPrompt:
			promptID = append(promptID[:0], message.ID...)
			if mode == providerProxyCanaryMode {
				if err := exerciseProviderProxyFromChild(); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(3)
				}
				if err := validateProviderProxyChildIsolation(); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(4)
				}
			}
			if mode == toolBurstRPCErrorMode {
				writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "method": acp.MethodSessionUpdate, "params": map[string]any{
					"sessionId": sessionID, "update": map[string]any{
						"sessionUpdate": "tool_call", "toolCallId": "provider-call-1", "title": "Read repository", "kind": "read",
					},
				}})
				for range runtimeMaxUpdateEventsPerSecond + 1 {
					writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "method": acp.MethodSessionUpdate, "params": map[string]any{
						"sessionId": sessionID, "update": map[string]any{
							"sessionUpdate": "tool_call_update", "toolCallId": "provider-call-1",
							"_meta": map[string]any{"terminal_output_delta": map[string]any{"terminal_id": "provider-call-1", "data": "x"}},
						},
					}})
				}
				writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "method": acp.MethodSessionUpdate, "params": map[string]any{
					"sessionId": sessionID, "update": map[string]any{
						"sessionUpdate": "tool_call_update", "toolCallId": "provider-call-1", "status": "completed",
					},
				}})
				writeHelperMessage(writer, map[string]any{
					testJSONRPCKey: testJSONRPCVersion, "id": rawID(promptID),
					"error": map[string]any{
						"code": -32603, "message": "provider-secret-must-not-leak",
						"data": map[string]any{"service": "session", "errorName": "APIError", "detail": "provider-secret-must-not-leak"},
					},
				})
				promptID = nil
				continue
			}
			assistantUpdates := []string{"hello from ACP"}
			if mode == assistantBurstMode {
				assistantUpdates = make([]string, runtimeMaxUpdateEventsPerSecond+1)
				for index := range assistantUpdates {
					assistantUpdates[index] = "x"
				}
			}
			for _, text := range assistantUpdates {
				writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "method": acp.MethodSessionUpdate, "params": map[string]any{
					"sessionId": sessionID, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": text}},
				}})
			}
			if mode != "wait" {
				writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "id": rawID(promptID), "result": map[string]any{"stopReason": acp.StopReasonEndTurn}})
				promptID = nil
			}
		case acp.MethodSessionCancel:
			if len(promptID) > 0 {
				writeHelperMessage(writer, map[string]any{testJSONRPCKey: testJSONRPCVersion, "id": rawID(promptID), "result": map[string]any{"stopReason": acp.StopReasonCancelled}})
				promptID = nil
			}
		}
	}
}

func writeHelperMessage(writer *bufio.Writer, value any) {
	data, _ := json.Marshal(value)
	_, _ = writer.Write(append(data, '\n'))
	_ = writer.Flush()
}

func rawID(raw json.RawMessage) any {
	if value, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
		return value
	}
	var value any
	_ = json.Unmarshal(raw, &value)
	return value
}

func validateProviderProxyChildIsolation() error {
	for _, entry := range os.Environ() {
		if strings.Contains(entry, supervisorUpstreamTokenCanary) {
			return fmt.Errorf("supervisor upstream token leaked into ACP child environment")
		}
	}
	for _, argument := range os.Args {
		if strings.Contains(argument, supervisorUpstreamTokenCanary) {
			return fmt.Errorf("supervisor upstream token leaked into ACP child arguments")
		}
	}
	credential := strings.TrimSpace(os.Getenv("CODEX_API_KEY"))
	if len(credential) < 40 || credential == supervisorUpstreamTokenCanary {
		return fmt.Errorf("ACP child did not receive a distinct local provider credential")
	}
	baseURL, err := providerProxyBaseURLFromChild()
	if err != nil {
		return err
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != providerProxyScheme || parsed.Hostname() != "127.0.0.1" || !strings.HasPrefix(parsed.Path, providerProxyPathPrefix) {
		return fmt.Errorf("ACP child provider base URL is not an unguessable loopback route")
	}
	root := filepath.Dir(strings.TrimSpace(os.Getenv("HOME")))
	if root == "." || !filepath.IsAbs(root) {
		return fmt.Errorf("ACP child home is not rooted in a private session tree")
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > 1<<20 {
			return fmt.Errorf("unexpected large file in ACP canary session tree")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(supervisorUpstreamTokenCanary)) {
			return fmt.Errorf("supervisor upstream token leaked into ACP child session files")
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func exerciseProviderProxyFromChild() error {
	baseURL, err := providerProxyBaseURLFromChild()
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/responses?from=acp-child", strings.NewReader(`{"model":"test-model","canary":true}`))
	if err != nil {
		return err
	}
	credential := os.Getenv("CODEX_API_KEY")
	request.Header.Set(providerAuthorizationHeader, "Bearer "+credential)
	request.Header.Set(providerAPIKeyHeader, credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(providerCookieHeader, "child-secret=1")
	request.Header.Set(providerProxyAuthorizationHeader, "Bearer child-proxy-secret")
	request.Header.Set(providerForwardedForHeader, "203.0.113.10")
	request.Header.Set("Connection", "X-Child-Secret")
	request.Header.Set("X-Child-Secret", "remove-me")
	request.Header.Set("X-Canary-Safe", "preserved")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("ACP child provider request failed: %w", err)
	}
	defer response.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK || string(body) != `{"ok":true}` {
		return fmt.Errorf("ACP child provider response was rejected")
	}
	return nil
}

func providerProxyBaseURLFromChild() (string, error) {
	var config map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("CODEX_CONFIG")), &config); err != nil {
		return "", fmt.Errorf("decode ACP child Codex config: %w", err)
	}
	baseURL := strings.TrimSuffix(strings.TrimSpace(config["openai_base_url"]), "/")
	if baseURL == "" {
		return "", fmt.Errorf("ACP child local provider base URL is missing")
	}
	return baseURL, nil
}

func TestSupervisorStatusReservesOnlyAcceptedPublicationFinalization(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}

	now := time.Now().UTC()
	server.mu.Lock()
	state := server.sessions[create.RuntimeSessionID]
	state.descriptor.State = harnessv2.RuntimeSessionStateFinalizing
	state.descriptor.LastTransitionAt = now
	state.publicationFinalization = &harnessv2.PublicationFinalizationReceipt{
		WorkspaceDeltaID: "delta-1", PublicationID: "publication-1", PublicationGeneration: 1, PublicationVersion: 1,
		TerminalState: harnessv2.PublicationTerminalVerifiedExact, TerminalReceiptDigest: testDigest("publication-receipt"), AppliedAt: now,
	}
	server.mu.Unlock()

	status := server.status()
	if len(status.Sessions) != 1 || !status.Sessions[0].ReservedForFinalization {
		t.Fatalf("finalizing status=%#v, want publication-finalization reservation", status.Sessions)
	}
	if err := status.Sessions[0].Validate(); err != nil {
		t.Fatalf("finalizing RuntimeSessionStatus.Validate() error = %v", err)
	}

	server.mu.Lock()
	state.descriptor.State = harnessv2.RuntimeSessionStateDeleting
	state.descriptor.LastTransitionAt = time.Now().UTC()
	server.mu.Unlock()
	status = server.status()
	if len(status.Sessions) != 1 || status.Sessions[0].ReservedForFinalization {
		t.Fatalf("deleting status=%#v, want reservation cleared", status.Sessions)
	}
	if err := status.Sessions[0].Validate(); err != nil {
		t.Fatalf("deleting RuntimeSessionStatus.Validate() error = %v", err)
	}
}

func TestSupervisorFinalizesPreparedPublicationBeforeDeletion(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	now := time.Now().UTC()
	settlement := harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCompleted, Outcome: harnessv2.PromptOutcomeSucceeded,
		StopReason: harnessv2.ACPStopReasonEndTurn, SettledAt: now,
	}
	server.mu.Lock()
	state := server.sessions[create.RuntimeSessionID]
	state.workspaceIntent = harnessv2.WorkspaceIntentWrite
	state.prompt = &promptState{request: prompt, settlement: &settlement}
	state.descriptor.State = harnessv2.RuntimeSessionStatePublicationPrepared
	state.deltas["delta-1"] = harnessv2.CreateWorkspaceDeltaResponse{Delta: harnessv2.WorkspaceDeltaDescriptor{
		DeltaID: "delta-1", RuntimeSessionUID: state.descriptor.RuntimeSessionUID, SessionGeneration: state.descriptor.Generation,
		State: harnessv2.WorkspaceDeltaPrepared, PublicationSafe: true,
	}}
	server.mu.Unlock()

	metadata := prompt.Metadata
	metadata.OperationID = "finalize-publication-1"
	metadata.ExpiresAt = now.Add(time.Minute)
	finalize := harnessv2.FinalizeRuntimeSessionPublicationRequest{
		Protocol: harnessv2.ProtocolVersion, Metadata: metadata, WorkspaceDeltaID: "delta-1",
		PublicationID: "publication-1", PublicationGeneration: 1, PublicationVersion: 7,
		TerminalState: harnessv2.PublicationTerminalVerifiedExact, TerminalReceiptDigest: testDigest("publication-receipt"),
	}
	sealRequest(t, &finalize.Metadata.RequestDigest, finalize)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/publication-finalization", finalize, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("finalize status=%d body=%s", response.Code, response.Body.String())
	}
	var finalized harnessv2.FinalizeRuntimeSessionPublicationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &finalized); err != nil {
		t.Fatal(err)
	}
	if err := finalized.ValidateFor(finalize); err != nil || finalized.Session.State != harnessv2.RuntimeSessionStateFinalizing {
		t.Fatalf("finalize response=%#v validation=%v", finalized, err)
	}
	duplicate := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/publication-finalization", finalize, cfg)
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate finalize status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	if err := json.Unmarshal(duplicate.Body.Bytes(), &finalized); err != nil {
		t.Fatal(err)
	}
	if finalized.Classification.Class != harnessv2.RequestClassificationDuplicate || finalized.Classification.Phase != harnessv2.OperationPhaseApplied {
		t.Fatalf("duplicate finalization classification=%#v", finalized.Classification)
	}
	appliedAt := finalized.Finalization.AppliedAt
	recovered := finalize
	recovered.Metadata.OperationID = "finalize-publication-recovered"
	recovered.Metadata.ExpiresAt = now.Add(2 * time.Minute)
	sealRequest(t, &recovered.Metadata.RequestDigest, recovered)
	recoveryResponse := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/publication-finalization", recovered, cfg)
	if recoveryResponse.Code != http.StatusOK {
		t.Fatalf("recovered finalization status=%d body=%s", recoveryResponse.Code, recoveryResponse.Body.String())
	}
	if err := json.Unmarshal(recoveryResponse.Body.Bytes(), &finalized); err != nil {
		t.Fatal(err)
	}
	if finalized.Classification.Class != harnessv2.RequestClassificationFresh || !finalized.Finalization.AppliedAt.Equal(appliedAt) {
		t.Fatalf("recovered finalization=%#v, want fresh with stable receipt", finalized)
	}
	conflicting := finalize
	conflicting.Metadata.OperationID = "finalize-publication-conflict"
	conflicting.Metadata.ExpiresAt = now.Add(3 * time.Minute)
	conflicting.TerminalReceiptDigest = testDigest("other-publication-receipt")
	sealRequest(t, &conflicting.Metadata.RequestDigest, conflicting)
	conflictResponse := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/publication-finalization", conflicting, cfg)
	if conflictResponse.Code != http.StatusConflict {
		t.Fatalf("conflicting finalization status=%d body=%s", conflictResponse.Code, conflictResponse.Body.String())
	}
	var conflictError harnessv2.ErrorResponse
	if err := json.Unmarshal(conflictResponse.Body.Bytes(), &conflictError); err != nil {
		t.Fatal(err)
	}
	if conflictError.Code != harnessv2.ErrorCodeDigestConflict {
		t.Fatalf("conflicting finalization error=%#v", conflictError)
	}
	afterFinalizePrompt := prompt
	afterFinalizePrompt.Metadata.OperationID = "prompt-after-publication-finalization"
	sealRequest(t, &afterFinalizePrompt.Metadata.RequestDigest, afterFinalizePrompt)
	promptResponse := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", afterFinalizePrompt, cfg)
	if promptResponse.Code != http.StatusConflict {
		t.Fatalf("prompt after finalization status=%d body=%s", promptResponse.Code, promptResponse.Body.String())
	}
	if runtime.GOOS != linuxGOOS {
		return
	}

	deleteMetadata := prompt.Metadata
	deleteMetadata.OperationID = "delete-finalized-session-1"
	deleteMetadata.ExpiresAt = now.Add(time.Minute)
	deleteRequest := harnessv2.DeleteRuntimeSessionRequest{Protocol: harnessv2.ProtocolVersion, Metadata: deleteMetadata, Reason: "finalized"}
	sealRequest(t, &deleteRequest.Metadata.RequestDigest, deleteRequest)
	deleted := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", deleteRequest, cfg)
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete finalized session status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	replayedDelete := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", deleteRequest, cfg)
	if replayedDelete.Code != http.StatusOK {
		t.Fatalf("replayed delete status=%d body=%s", replayedDelete.Code, replayedDelete.Body.String())
	}
	var deleteResponse harnessv2.DeleteRuntimeSessionResponse
	if err := json.Unmarshal(replayedDelete.Body.Bytes(), &deleteResponse); err != nil {
		t.Fatal(err)
	}
	if deleteResponse.Classification.Class != harnessv2.RequestClassificationDuplicate || deleteResponse.Classification.Phase != harnessv2.OperationPhaseDeleted {
		t.Fatalf("replayed delete classification=%#v", deleteResponse.Classification)
	}
	freshDelete := deleteRequest
	freshDelete.Metadata.OperationID = "delete-finalized-session-new-operation"
	freshDelete.Metadata.ExpiresAt = now.Add(2 * time.Minute)
	sealRequest(t, &freshDelete.Metadata.RequestDigest, freshDelete)
	freshResponse := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", freshDelete, cfg)
	if freshResponse.Code != http.StatusNotFound {
		t.Fatalf("fresh delete after tombstone status=%d body=%s", freshResponse.Code, freshResponse.Body.String())
	}
	var freshError harnessv2.ErrorResponse
	if err := json.Unmarshal(freshResponse.Body.Bytes(), &freshError); err != nil {
		t.Fatal(err)
	}
	if freshError.Classification != nil {
		t.Fatalf("fresh delete after tombstone carried classification=%#v", freshError.Classification)
	}

	// Replaying the original still-valid create after deletion must not
	// resurrect the session and re-run its prompt; the deletion tombstone
	// classifies the create as a duplicate.
	replayedCreate := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if replayedCreate.Code == http.StatusOK {
		t.Fatalf("replayed create after deletion recreated the session: body=%s", replayedCreate.Body.String())
	}
	if _, resident := server.sessions["session-1"]; resident {
		t.Fatal("replayed create after deletion made the session resident again")
	}
}

func TestCleanupDrainedSessionClearsDeferredSchedule(t *testing.T) {
	server := &Server{sessions: make(map[harnessv2.RuntimeSessionID]*sessionState)}
	state := &sessionState{
		id: "session-1", drainCleanupScheduled: true,
		descriptor: harnessv2.RuntimeSessionDescriptor{State: harnessv2.RuntimeSessionStatePublicationPrepared},
		prompt:     &promptState{},
	}
	server.sessions[state.id] = state
	server.cleanupDrainedSession(state.id, state)
	server.mu.Lock()
	defer server.mu.Unlock()
	if state.drainCleanupScheduled {
		t.Fatal("deferred drain cleanup remained scheduled after an ineligible observation")
	}
}

func TestSupervisorRejectsConcurrentDeleteWhileCleanupIsRunning(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	server.mu.Lock()
	server.sessions[create.RuntimeSessionID].descriptor.State = harnessv2.RuntimeSessionStateDeleting
	server.mu.Unlock()
	staleMetadata := testMetadata(create.Metadata.Fence, "stale-delete-while-cleaning", false)
	staleMetadata.Fence.RuntimeSessionGeneration++
	staleRequest := harnessv2.DeleteRuntimeSessionRequest{Protocol: harnessv2.ProtocolVersion, Metadata: staleMetadata, Reason: "stale duplicate cleanup"}
	sealRequest(t, &staleRequest.Metadata.RequestDigest, staleRequest)
	staleResponse := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", staleRequest, cfg)
	if staleResponse.Code != http.StatusGone {
		t.Fatalf("stale concurrent delete status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}

	metadata := testMetadata(create.Metadata.Fence, "delete-while-cleaning", false)
	request := harnessv2.DeleteRuntimeSessionRequest{Protocol: harnessv2.ProtocolVersion, Metadata: metadata, Reason: "duplicate cleanup"}
	sealRequest(t, &request.Metadata.RequestDigest, request)
	response := performMutation(t, server.Handler(), http.MethodDelete, "/v2/runtime-sessions/session-1", request, cfg)
	if response.Code != http.StatusConflict {
		t.Fatalf("concurrent delete status=%d body=%s", response.Code, response.Body.String())
	}
	var decoded harnessv2.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Code != harnessv2.ErrorCodeAlreadyAccepted {
		t.Fatalf("concurrent delete error=%#v", decoded)
	}
}

func TestSupervisorRetiresPoisonedSessionWithoutPoolDrain(t *testing.T) {
	if runtime.GOOS != linuxGOOS {
		t.Skip("descendant cleanup proof is Linux-specific")
	}
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	server.mu.Lock()
	state := server.sessions[create.RuntimeSessionID]
	server.mu.Unlock()
	server.poisonSession(state, "test")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		_, resident := server.sessions[create.RuntimeSessionID]
		_, tombstoned := server.tombstones[create.Metadata.Fence.RuntimeSessionUID]
		server.mu.Unlock()
		if !resident && tombstoned {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("poisoned RuntimeSession remained resident without an enclosing pool drain")
}

func TestSupervisorDrainSchedulesPublicationPreparedSessionBeforeSettlement(t *testing.T) {
	server, cfg, _ := newTestServer(t, "immediate")
	now := time.Now().UTC()
	sessionID := harnessv2.RuntimeSessionID("publication-session")
	state := &sessionState{
		id: sessionID,
		descriptor: harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: sessionID,
			State:            harnessv2.RuntimeSessionStatePublicationPrepared,
		},
		prompt: &promptState{},
	}
	server.mu.Lock()
	server.sessions[sessionID] = state
	server.mu.Unlock()

	drain := harnessv2.DrainRequest{Protocol: harnessv2.ProtocolVersion, Metadata: harnessv2.MutationMetadata{
		Fence: cfg.Fence, OperationID: "drain-publication-prepared", RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
		ExpiresAt: now.Add(time.Minute),
	}, Reason: "rollout"}
	sealRequest(t, &drain.Metadata.RequestDigest, drain)
	response := performMutation(t, server.Handler(), http.MethodPut, harnessv2.DrainPath, drain, cfg)
	if response.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", response.Code, response.Body.String())
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.sessions[sessionID] != state || !state.drainCleanupScheduled {
		t.Fatalf("publication-prepared RuntimeSession was not scheduled before settlement: resident=%t scheduled=%t", server.sessions[sessionID] == state, state.drainCleanupScheduled)
	}
}

func TestSupervisorDrainRetiresEligibleResidentSessions(t *testing.T) {
	if runtime.GOOS != linuxGOOS {
		t.Skip("descendant cleanup proof is Linux-specific")
	}
	for _, test := range []struct {
		name          string
		state         harnessv2.RuntimeSessionState
		settledPrompt bool
	}{
		{name: "idle", state: harnessv2.RuntimeSessionStateIdle},
		{name: "publication-prepared", state: harnessv2.RuntimeSessionStatePublicationPrepared, settledPrompt: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, cfg, profile := newTestServer(t, "immediate")
			create := testCreateSessionRequest(t, cfg, profile)
			created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
			if created.Code != http.StatusCreated {
				t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
			}
			server.mu.Lock()
			state := server.sessions[create.RuntimeSessionID]
			state.descriptor.State = test.state
			if test.settledPrompt {
				now := time.Now().UTC()
				prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
				settlement := harnessv2.PromptSettlement{
					TerminalEvent: harnessv2.EventCompleted,
					Outcome:       harnessv2.PromptOutcomeSucceeded,
					StopReason:    harnessv2.ACPStopReasonEndTurn,
					SettledAt:     now,
				}
				state.prompt = &promptState{request: prompt, settlement: &settlement}
				state.descriptor.LastTransitionAt = now
			}
			server.mu.Unlock()

			drain := harnessv2.DrainRequest{Protocol: harnessv2.ProtocolVersion, Metadata: harnessv2.MutationMetadata{
				Fence: cfg.Fence, OperationID: harnessv2.OperationID("drain-" + test.name), RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
				ExpiresAt: time.Now().UTC().Add(time.Minute),
			}, Reason: "rollout"}
			sealRequest(t, &drain.Metadata.RequestDigest, drain)
			response := performMutation(t, server.Handler(), http.MethodPut, harnessv2.DrainPath, drain, cfg)
			if response.Code != http.StatusOK {
				t.Fatalf("drain status=%d body=%s", response.Code, response.Body.String())
			}
			server.mu.Lock()
			residentState, resident := server.sessions[create.RuntimeSessionID]
			if resident && !residentState.drainCleanupScheduled {
				server.mu.Unlock()
				t.Fatalf("%s RuntimeSession was not scheduled for drain cleanup", test.name)
			}
			server.mu.Unlock()

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				server.mu.Lock()
				_, resident = server.sessions[create.RuntimeSessionID]
				_, tombstoned := server.tombstones[create.Metadata.Fence.RuntimeSessionUID]
				server.mu.Unlock()
				if !resident && tombstoned {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			states := make([]harnessv2.RuntimeSessionState, 0, len(server.sessions))
			for _, state := range server.sessions {
				states = append(states, state.descriptor.State)
			}
			_, tombstoned := server.tombstones[create.Metadata.Fence.RuntimeSessionUID]
			t.Fatalf("%s RuntimeSession drain cleanup did not complete: lifecycle=%s states=%v tombstoned=%t", test.name, server.lifecycle, states, tombstoned)
		})
	}
}

func TestSupervisorRejectsFreshPromptWhenDrainCleanupIsScheduled(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	created := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", create, cfg)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	server.mu.Lock()
	state := server.sessions[create.RuntimeSessionID]
	server.drain = harnessv2.DrainStatus{Requested: true, AcceptingNewSessions: false, RequestedAt: time.Now().UTC(), Reason: "rollout"}
	server.lifecycle = harnessv2.SupervisorLifecycleDraining
	state.drainCleanupScheduled = true
	server.mu.Unlock()
	prompt := testStartPromptRequest(t, cfg, create.Metadata.Fence)
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1/prompts/prompt-1", prompt, cfg)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("prompt during drain status=%d body=%s", response.Code, response.Body.String())
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if state.prompt != nil {
		t.Fatal("fresh prompt was admitted while drain cleanup was scheduled")
	}
}

func TestPruneTombstonesLockedRetainsEveryUnexpiredReplay(t *testing.T) {
	server := &Server{tombstones: map[harnessv2.RuntimeSessionUID]harnessv2.RuntimeSessionTombstone{}}
	now := time.Now().UTC()
	server.tombstones["fresh"] = harnessv2.RuntimeSessionTombstone{RuntimeSessionUID: "fresh", DeletedAt: now.Add(-time.Minute)}
	server.tombstones["stale"] = harnessv2.RuntimeSessionTombstone{RuntimeSessionUID: "stale", DeletedAt: now.Add(-2 * tombstoneRetention)}
	server.pruneTombstonesLocked(now)
	if _, ok := server.tombstones["stale"]; ok {
		t.Fatal("tombstone older than the retention window was retained")
	}
	if _, ok := server.tombstones["fresh"]; !ok {
		t.Fatal("in-window tombstone was dropped")
	}

	// Sustained churn within the capability window must not evict an older
	// tombstone while its create operation can still be replayed.
	const inWindowTombstones = 4352
	for i := range inWindowTombstones {
		uid := harnessv2.RuntimeSessionUID(fmt.Sprintf("session-%05d", i))
		server.tombstones[uid] = harnessv2.RuntimeSessionTombstone{
			RuntimeSessionUID: uid, DeletedAt: now.Add(-time.Duration(i) * time.Millisecond),
		}
	}
	server.pruneTombstonesLocked(now)
	if got, want := len(server.tombstones), inWindowTombstones+1; got != want {
		t.Fatalf("tombstone count = %d, want %d unexpired records", got, want)
	}
	if _, ok := server.tombstones["session-04351"]; !ok {
		t.Fatal("oldest in-window tombstone was evicted")
	}
}

// A create request rebuilt by the controller for the same attempt (fresh
// expiry, fresh workspace capability) reaches a supervisor that already created
// the session from an earlier send. The supervisor must answer digest_conflict
// with the recorded phase and keep the resident session in status so the
// controller can adopt it.
func TestCreateSessionRebuiltRequestConflictsWhileSessionStaysResident(t *testing.T) {
	server, cfg, profile := newTestServer(t, "immediate")
	create := testCreateSessionRequest(t, cfg, profile)
	now := time.Now().UTC()
	state := &sessionState{
		id: create.RuntimeSessionID,
		descriptor: harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: create.RuntimeSessionID, RuntimeSessionUID: create.Metadata.Fence.RuntimeSessionUID,
			Generation: create.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: cfg.Fence.RuntimeInstanceID,
			SupervisorBootID: cfg.Fence.SupervisorBootID, RuntimeProfileDigest: cfg.Fence.RuntimeProfileDigest,
			State: harnessv2.RuntimeSessionStateIdle, CreatedAt: now, LastTransitionAt: now,
		},
	}
	recordSessionOperationLocked(state, create.Metadata, harnessv2.OperationPhaseApplied, "", now)
	server.mu.Lock()
	server.sessions[create.RuntimeSessionID] = state
	server.mu.Unlock()

	rebuilt := create
	rebuilt.Metadata.ExpiresAt = create.Metadata.ExpiresAt.Add(time.Minute)
	sealRequest(t, &rebuilt.Metadata.RequestDigest, rebuilt)
	if rebuilt.Metadata.RequestDigest == create.Metadata.RequestDigest {
		t.Fatal("rebuilt create request kept the original digest")
	}
	response := performMutation(t, server.Handler(), http.MethodPut, "/v2/runtime-sessions/session-1", rebuilt, cfg)
	if response.Code != http.StatusConflict {
		t.Fatalf("rebuilt create status=%d body=%s", response.Code, response.Body.String())
	}
	var apiError harnessv2.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &apiError); err != nil {
		t.Fatal(err)
	}
	if apiError.Code != harnessv2.ErrorCodeDigestConflict || apiError.Classification == nil ||
		apiError.Classification.Class != harnessv2.RequestClassificationDigestConflict ||
		apiError.Classification.Phase != harnessv2.OperationPhaseApplied {
		t.Fatalf("rebuilt create error = %#v, want digest_conflict with the applied phase", apiError)
	}

	statusReq := httptest.NewRequest(http.MethodGet, harnessv2.StatusPath, nil)
	statusReq.Header.Set("Authorization", "Bearer "+cfg.ControllerBearerToken)
	statusNonce, err := harnessv2.NewCapabilityNonce()
	if err != nil {
		t.Fatal(err)
	}
	statusBinding := harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: cfg.Fence.RuntimeProfileDigest}
	statusCapability, err := harnessv2.SignStatusCapability(cfg.CapabilitySecret, harnessv2.NewStatusCapabilityClaims(statusBinding, statusNonce, time.Now().UTC().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	statusReq.Header.Set(OperationCapabilityHeader, statusCapability)
	statusResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(statusResponse, statusReq)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	var status harnessv2.StatusResponse
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Sessions) != 1 || status.Sessions[0].RuntimeSessionID != create.RuntimeSessionID ||
		status.Sessions[0].Generation != create.Metadata.Fence.RuntimeSessionGeneration ||
		status.Sessions[0].State != harnessv2.RuntimeSessionStateIdle {
		t.Fatalf("resident session was not reported admissible after the rejected rebuild: %#v", status.Sessions)
	}
}
