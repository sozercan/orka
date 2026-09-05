package conformancetest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance"
)

type Config struct {
	ListenAddress                     string
	ControllerBearerToken             string
	OperationCapabilitySecret         []byte
	ControllerEpoch                   uint64
	RuntimeInstanceID                 harnessv2.RuntimeInstanceID
	SupervisorBootID                  harnessv2.SupervisorBootID
	RuntimePoolUID                    harnessv2.RuntimePoolUID
	Profile                           harnessv2.RuntimeProfile
	ProviderKinds                     []string
	Models                            []string
	Limits                            harnessv2.ProtocolLimits
	SupportsDrain                     bool
	SupportsPublicationFinalization   bool
	SupportsAgentSessionConfiguration bool
	SupportsPermissions               *bool
	WorkspaceGovernance               conformance.WorkspaceGovernanceClaims
	AllowUnauthenticatedStatus        bool
	OmitStatusControllerEpoch         bool
	DisconnectPromptAfterAccepted     bool
	CompleteNonConformancePrompts     bool
	PromptResultText                  string
	CompletePromptBeforeReplay        bool
	CompletePromptBeforeConflict      bool

	// These test-only faults prove that the conformance cycle rejects runtimes
	// which advertise duplicate-safe mutations without honoring replay semantics.
	BreakDuplicateSafeMutations       bool
	BreakDigestConflictClassification bool

	// These faults prove that publication-finalization recovery is exact even
	// when the controller must allocate a fresh operation identity.
	BreakFreshPublicationFinalizationAppliedAt     bool
	BreakFreshPublicationFinalizationConflictGuard bool
	FailFirstSessionDeleteBeforeApply              bool
}

type Counts struct {
	SessionCreates        int64
	PromptStarts          int64
	PromptCancels         int64
	WorkspaceDeltas       int64
	SessionDeletes        int64
	SessionDeleteAttempts int64
	ReplayClassifications int64
	DigestConflicts       int64
}

type Server struct {
	server *httptest.Server
	config Config

	sessionCreates        atomic.Int64
	promptStarts          atomic.Int64
	promptCancels         atomic.Int64
	workspaceDeltas       atomic.Int64
	sessionDeletes        atomic.Int64
	sessionDeleteAttempts atomic.Int64
	replayClassifications atomic.Int64
	digestConflicts       atomic.Int64

	mu                    sync.Mutex
	fence                 harnessv2.Fence
	drain                 harnessv2.DrainStatus
	operations            map[harnessv2.OperationID]harnessv2.OperationRecord
	sessions              map[harnessv2.RuntimeSessionID]sessionState
	prompts               map[harnessv2.PromptID]*promptState
	workspaceResponses    map[harnessv2.OperationID]harnessv2.CreateWorkspaceDeltaResponse
	finalizationResponses map[harnessv2.OperationID]harnessv2.FinalizeRuntimeSessionPublicationResponse
	deleteResponses       map[harnessv2.OperationID]harnessv2.DeleteRuntimeSessionResponse
	drainResponses        map[harnessv2.OperationID]harnessv2.DrainResponse
}

type sessionState struct {
	descriptor   harnessv2.RuntimeSessionDescriptor
	relativeRoot string
	deltas       map[harnessv2.WorkspaceDeltaID]harnessv2.WorkspaceDeltaDescriptor
	finalization *harnessv2.PublicationFinalizationReceipt
}

type promptState struct {
	request      harnessv2.StartPromptRequest
	acceptedAt   time.Time
	cancelled    chan struct{}
	complete     chan struct{}
	settled      chan struct{}
	cancelOnce   sync.Once
	completeOnce sync.Once
	settleOnce   sync.Once
	settlement   *harnessv2.PromptSettlement
}

func NewServer(config Config) (*Server, error) {
	if config.ControllerBearerToken == "" {
		config.ControllerBearerToken = "controller-token-0123456789abcdef"
	}
	if len(config.OperationCapabilitySecret) == 0 {
		config.OperationCapabilitySecret = []byte("capability-secret-0123456789abcdef")
	}
	if config.ControllerEpoch == 0 {
		config.ControllerEpoch = 1
	}
	if config.RuntimeInstanceID == "" {
		config.RuntimeInstanceID = "runtime-instance-1"
	}
	if config.SupervisorBootID == "" {
		config.SupervisorBootID = "boot-1"
	}
	if config.RuntimePoolUID == "" {
		config.RuntimePoolUID = "external-pool-1"
	}
	if config.Limits.MaxResidentSessions == 0 {
		config.Limits = harnessv2.DefaultProtocolLimits()
	}
	if err := config.Profile.Validate(); err != nil {
		return nil, fmt.Errorf("profile: %w", err)
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(config.Profile)
	if err != nil {
		return nil, err
	}
	s := &Server{
		config: config,
		fence: harnessv2.Fence{
			RuntimeInstanceID:          config.RuntimeInstanceID,
			SupervisorBootID:           config.SupervisorBootID,
			ControllerEpoch:            config.ControllerEpoch,
			RuntimePoolUID:             config.RuntimePoolUID,
			RuntimePoolGeneration:      1,
			RuntimeProfileDigest:       profileDigest,
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		drain:                 harnessv2.DrainStatus{AcceptingNewSessions: true},
		operations:            map[harnessv2.OperationID]harnessv2.OperationRecord{},
		sessions:              map[harnessv2.RuntimeSessionID]sessionState{},
		prompts:               map[harnessv2.PromptID]*promptState{},
		workspaceResponses:    map[harnessv2.OperationID]harnessv2.CreateWorkspaceDeltaResponse{},
		finalizationResponses: map[harnessv2.OperationID]harnessv2.FinalizeRuntimeSessionPublicationResponse{},
		deleteResponses:       map[harnessv2.OperationID]harnessv2.DeleteRuntimeSessionResponse{},
		drainResponses:        map[harnessv2.OperationID]harnessv2.DrainResponse{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+harnessv2.HealthPath, s.handleHealth)
	mux.HandleFunc("GET "+harnessv2.CapabilitiesPath, s.handleCapabilities)
	mux.HandleFunc("GET "+harnessv2.StatusPath, s.handleStatus)
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}", s.handleCreateSession)
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}", s.handlePrompt)
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}/cancel", s.handleCancel)
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/workspace-deltas/{deltaID}", s.handleWorkspaceDelta)
	mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/publication-finalization", s.handlePublicationFinalization)
	mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", s.handleDeleteSession)
	if config.SupportsDrain {
		mux.HandleFunc("PUT "+harnessv2.DrainPath, s.handleDrain)
	}
	server := httptest.NewUnstartedServer(mux)
	if address := strings.TrimSpace(config.ListenAddress); address != "" {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("listen on %q: %w", address, err)
		}
		_ = server.Listener.Close()
		server.Listener = listener
	}
	server.Start()
	s.server = server
	return s, nil
}

func (s *Server) URL() string { return s.server.URL }
func (s *Server) Close()      { s.server.Close() }

func (s *Server) Fence() harnessv2.Fence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fence
}

func (s *Server) SetFence(fence harnessv2.Fence) error {
	if fence.RuntimeSessionUID != "" || fence.RuntimeSessionGeneration != 0 {
		return fmt.Errorf("test server fence must be pool-scoped")
	}
	if err := fence.Validate(false); err != nil {
		return err
	}
	s.mu.Lock()
	s.fence = fence
	s.mu.Unlock()
	return nil
}

func (s *Server) Counts() Counts {
	return Counts{
		SessionCreates:        s.sessionCreates.Load(),
		PromptStarts:          s.promptStarts.Load(),
		PromptCancels:         s.promptCancels.Load(),
		WorkspaceDeltas:       s.workspaceDeltas.Load(),
		SessionDeletes:        s.sessionDeletes.Load(),
		SessionDeleteAttempts: s.sessionDeleteAttempts.Load(),
		ReplayClassifications: s.replayClassifications.Load(),
		DigestConflicts:       s.digestConflicts.Load(),
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, harnessv2.HealthResponse{Protocol: harnessv2.ProtocolVersion, Status: harnessv2.HealthStatusOK, Timestamp: time.Now().UTC()})
}

func (s *Server) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	fence := s.Fence()
	providerKinds := s.config.ProviderKinds
	if providerKinds == nil {
		providerKinds = []string{s.config.Profile.ProviderKind}
	}
	models := s.config.Models
	if models == nil {
		models = []string{s.config.Profile.Model}
	}
	supportsPermissions := true
	if s.config.SupportsPermissions != nil {
		supportsPermissions = *s.config.SupportsPermissions
	}
	writeJSON(w, http.StatusOK, conformance.CapabilitiesResponse{
		CapabilitiesResponse: harnessv2.CapabilitiesResponse{
			Protocol:                   harnessv2.ProtocolVersion,
			Transport:                  "http+ndjson",
			ACPVersion:                 s.config.Profile.ACPProfile,
			RuntimeProfileDigest:       fence.RuntimeProfileDigest,
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests:             s.config.Profile.AdapterDigests,
			Limits:                     s.config.Limits,
			Provider: harnessv2.ProviderCapabilities{
				ProviderKinds:       providerKinds,
				Models:              models,
				SupportsPermissions: supportsPermissions,
				SupportsCancel:      true,
				SupportsTools:       true,
			},
			WorkspaceGovernance:               s.config.WorkspaceGovernance,
			SupportsDrain:                     s.config.SupportsDrain,
			SupportsPublicationFinalization:   s.config.SupportsPublicationFinalization,
			SupportsAgentSessionConfiguration: s.config.SupportsAgentSessionConfiguration,
		},
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !s.config.AllowUnauthenticatedStatus && !s.authorizeBearer(w, r) {
		return
	}
	// Mirror the production supervisor: status requires proof of the
	// operation capability secret in addition to the controller bearer.
	if !s.config.AllowUnauthenticatedStatus {
		binding := harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: s.Fence().RuntimeProfileDigest,
			RuntimeInstanceID:    s.Fence().RuntimeInstanceID,
		}
		if _, err := harnessv2.VerifyStatusCapability(s.config.OperationCapabilitySecret, r.Header.Get(harnessv2.OperationCapabilityHeader), binding, time.Now().UTC()); err != nil {
			writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "status authorization failed", nil)
			return
		}
	}
	s.mu.Lock()
	fence := s.fence
	drain := s.drain
	s.mu.Unlock()
	if s.config.OmitStatusControllerEpoch {
		fence.ControllerEpoch = 0
	}
	lifecycle := harnessv2.SupervisorLifecycleReady
	if drain.Requested {
		lifecycle = harnessv2.SupervisorLifecycleDraining
	}
	writeJSON(w, http.StatusOK, harnessv2.StatusResponse{
		Protocol:           harnessv2.ProtocolVersion,
		Fence:              fence,
		Lifecycle:          lifecycle,
		Drain:              drain,
		Sessions:           []harnessv2.RuntimeSessionStatus{},
		ActivePrompts:      []harnessv2.ActivePromptStatus{},
		PendingPermissions: []harnessv2.PendingPermissionStatus{},
		Pressure:           harnessv2.PressureMetadata{},
		Timestamp:          time.Now().UTC(),
	})
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.DrainRequest
	if !decodeJSON(w, r, &request) || !s.verifyPoolCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}

	s.mu.Lock()
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	classification, err := s.classifyPoolOperation(s.fence, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		response, ok := s.drainResponses[request.Metadata.OperationID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && ok {
			response.Classification = classification
			writeJSON(w, http.StatusOK, response)
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	drain := harnessv2.DrainStatus{
		AcceptingNewSessions: false,
		Requested:            true,
		RequestedAt:          now,
		Reason:               request.Reason,
	}
	response := harnessv2.DrainResponse{
		Protocol:       harnessv2.ProtocolVersion,
		Classification: classification,
		Drain:          drain,
	}
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	s.drainResponses[request.Metadata.OperationID] = response
	s.drain = drain
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.CreateRuntimeSessionRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if string(request.RuntimeSessionID) != r.PathValue("sessionID") {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "runtime session path does not match request", nil)
		return
	}

	s.mu.Lock()
	fence := s.fence
	expected := fence
	expected.RuntimeSessionUID = request.Metadata.Fence.RuntimeSessionUID
	expected.RuntimeSessionGeneration = request.Metadata.Fence.RuntimeSessionGeneration
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		state, ok := s.sessions[request.RuntimeSessionID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate || classification.Class == harnessv2.RequestClassificationFresh {
			if !ok {
				writeError(w, http.StatusConflict, harnessv2.ErrorCodeInvalidRequest, "session replay state is unavailable", nil)
				return
			}
			writeJSON(w, http.StatusOK, harnessv2.CreateRuntimeSessionResponse{
				Protocol: harnessv2.ProtocolVersion, Classification: classification, Session: state.descriptor,
			})
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	if _, exists := s.sessions[request.RuntimeSessionID]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "runtime session identity already exists", nil)
		return
	}
	s.sessionCreates.Add(1)
	descriptor := harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionID:     request.RuntimeSessionID,
		RuntimeSessionUID:    request.Metadata.Fence.RuntimeSessionUID,
		Generation:           request.Metadata.Fence.RuntimeSessionGeneration,
		RuntimeInstanceID:    fence.RuntimeInstanceID,
		SupervisorBootID:     fence.SupervisorBootID,
		RuntimeProfileDigest: fence.RuntimeProfileDigest,
		State:                harnessv2.RuntimeSessionStateIdle,
		ProviderSessionID:    "provider-session-" + string(request.RuntimeSessionID),
		WorkspaceBaseline:    request.Workspace.Baseline,
		CreatedAt:            now,
		LastTransitionAt:     now,
	}
	s.sessions[request.RuntimeSessionID] = sessionState{
		descriptor: descriptor, relativeRoot: strings.TrimSpace(request.Workspace.RelativeRoot),
		deltas: make(map[harnessv2.WorkspaceDeltaID]harnessv2.WorkspaceDeltaDescriptor),
	}
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, harnessv2.CreateRuntimeSessionResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, Session: descriptor,
	})
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.StartPromptRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now, time.Duration(s.config.Limits.MinPromptLeaseMillis)*time.Millisecond, time.Duration(s.config.Limits.MaxPromptLeaseMillis)*time.Millisecond); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}

	s.mu.Lock()
	expected := s.fence
	expected.RuntimeSessionUID = request.Metadata.Fence.RuntimeSessionUID
	expected.RuntimeSessionGeneration = request.Metadata.Fence.RuntimeSessionGeneration
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	if existing != nil && existing.Phase == harnessv2.OperationPhaseAccepted {
		state := s.prompts[request.Metadata.PromptID]
		duplicate := existing.RequestDigest == request.Metadata.RequestDigest
		if state != nil && state.request.Input.Metadata["orka.conformance"] == "cancel-after-accept" &&
			((duplicate && s.config.CompletePromptBeforeReplay) || (!duplicate && s.config.CompletePromptBeforeConflict)) {
			state.completeOnce.Do(func() { close(state.complete) })
			s.mu.Unlock()
			select {
			case <-state.settled:
			case <-r.Context().Done():
				return
			}
			s.mu.Lock()
			existing = operationPtr(s.operations, request.Metadata.OperationID)
		}
	}
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		state := s.prompts[request.Metadata.PromptID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationAlreadyAccepted && state != nil {
			writeJSON(w, http.StatusOK, harnessv2.PromptAdmissionResponse{
				Protocol: harnessv2.ProtocolVersion, Classification: classification, AcceptedAt: state.acceptedAt,
			})
			return
		}
		if classification.Class == harnessv2.RequestClassificationSettled && state != nil && state.settlement != nil {
			settlement := *state.settlement
			writeJSON(w, http.StatusOK, harnessv2.PromptAdmissionResponse{
				Protocol: harnessv2.ProtocolVersion, Classification: classification, AcceptedAt: state.acceptedAt, Settlement: &settlement,
			})
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	if _, ok := s.sessions[harnessv2.RuntimeSessionID(r.PathValue("sessionID"))]; !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil)
		return
	}
	state := &promptState{
		request: request, acceptedAt: now,
		cancelled: make(chan struct{}), complete: make(chan struct{}), settled: make(chan struct{}),
	}
	s.prompts[request.Metadata.PromptID] = state
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseAccepted, "", now)
	s.promptStarts.Add(1)
	s.mu.Unlock()

	w.Header().Set("Content-Type", harnessv2.NDJSONMediaType)
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	limits := harnessv2.EventStreamLimits{
		MaxLineBytes:             s.config.Limits.MaxEventLineBytes,
		MaxTerminalResultBytes:   s.config.Limits.MaxTerminalResultBytes,
		MaxBufferedEvents:        s.config.Limits.MaxBufferedEvents,
		MaxUpdateEventsPerSecond: s.config.Limits.MaxUpdateEventsPerSecond,
	}
	encoder, err := harnessv2.NewEventEncoder(w, limits, harnessv2.EventExpectationFromMetadata(request.Metadata))
	if err != nil {
		return
	}
	_ = encoder.Encode(harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventAccepted,
		Identity: eventIdentity(request.Metadata, 1, now),
		Accepted: &harnessv2.AcceptedEvent{AcceptedAt: now, Lease: request.Lease, ACPVersion: harnessv2.ACPProfileV1},
	})
	if flusher != nil {
		flusher.Flush()
	}
	if s.config.DisconnectPromptAfterAccepted {
		return
	}
	complete := request.Input.Metadata["orka.conformance"] == "complete-for-workspace" ||
		(s.config.CompleteNonConformancePrompts && request.Input.Metadata["orka.conformance"] != "cancel-after-accept")
	if !complete {
		select {
		case <-state.complete:
			complete = true
		case <-state.cancelled:
		case <-r.Context().Done():
			return
		case <-time.After(20 * time.Second):
			return
		}
	}
	if complete {
		settledAt := time.Now().UTC()
		resultText := strings.TrimSpace(s.config.PromptResultText)
		if resultText == "" {
			resultText = "deterministic external runtime result"
		}
		settlement := harnessv2.PromptSettlement{
			TerminalEvent: harnessv2.EventCompleted,
			Outcome:       harnessv2.PromptOutcomeSucceeded,
			StopReason:    harnessv2.ACPStopReasonEndTurn,
			SettledAt:     settledAt,
		}
		s.mu.Lock()
		state.settlement = &settlement
		s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settledAt)
		s.mu.Unlock()
		_ = encoder.Encode(harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion,
			Type:     harnessv2.EventCompleted,
			Identity: eventIdentity(request.Metadata, 2, settledAt),
			Completed: &harnessv2.CompletedEvent{
				StopReason: harnessv2.ACPStopReasonEndTurn,
				Result: harnessv2.PromptResult{
					Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: resultText}},
					Model:   s.config.Profile.Model,
				},
			},
		})
		_ = encoder.Close()
		if flusher != nil {
			flusher.Flush()
		}
		state.settleOnce.Do(func() { close(state.settled) })
		return
	}
	settledAt := time.Now().UTC()
	settlement := harnessv2.PromptSettlement{
		TerminalEvent: harnessv2.EventCancelled,
		Outcome:       harnessv2.PromptOutcomeCancelled,
		StopReason:    harnessv2.ACPStopReasonCancelled,
		SettledAt:     settledAt,
	}
	s.mu.Lock()
	state.settlement = &settlement
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settledAt)
	s.mu.Unlock()
	_ = encoder.Encode(harnessv2.Event{
		Protocol:  harnessv2.ProtocolVersion,
		Type:      harnessv2.EventCancelled,
		Identity:  eventIdentity(request.Metadata, 2, settledAt),
		Cancelled: &harnessv2.CancelledEvent{StopReason: harnessv2.ACPStopReasonCancelled, Reason: "conformance cancellation"},
	})
	_ = encoder.Close()
	if flusher != nil {
		flusher.Flush()
	}
	state.settleOnce.Do(func() { close(state.settled) })
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.CancelPromptRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}

	s.mu.Lock()
	state := s.prompts[request.Metadata.PromptID]
	if state == nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "prompt not found", nil)
		return
	}
	expected := s.fence
	expected.RuntimeSessionUID = request.Metadata.Fence.RuntimeSessionUID
	expected.RuntimeSessionGeneration = request.Metadata.Fence.RuntimeSessionGeneration
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		settlement := state.settlement
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationSettled && settlement != nil {
			writeJSON(w, http.StatusOK, cancellationResponse(classification, *settlement))
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseRecorded, "", now)
	s.promptCancels.Add(1)
	s.mu.Unlock()

	state.cancelOnce.Do(func() { close(state.cancelled) })
	if s.config.DisconnectPromptAfterAccepted {
		settlement := harnessv2.PromptSettlement{
			TerminalEvent: harnessv2.EventOutcomeUnknown,
			Outcome:       harnessv2.PromptOutcomeUnknown,
			SettledAt:     time.Now().UTC(),
		}
		s.mu.Lock()
		state.settlement = &settlement
		s.operations[state.request.Metadata.OperationID] = operationRecord(state.request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settlement.SettledAt)
		s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settlement.SettledAt)
		s.mu.Unlock()
		state.settleOnce.Do(func() { close(state.settled) })
		writeJSON(w, http.StatusOK, cancellationResponse(harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, settlement))
		return
	}
	select {
	case <-state.settled:
	case <-r.Context().Done():
		return
	}
	s.mu.Lock()
	settlement := *state.settlement
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settlement.SettledAt)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, cancellationResponse(harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, settlement))
}

func cancellationResponse(classification harnessv2.Classification, settlement harnessv2.PromptSettlement) harnessv2.CancelPromptResponse {
	proven := settlement.TerminalEvent != harnessv2.EventOutcomeUnknown
	barrier := harnessv2.CancellationBarrierSettled
	if !proven {
		barrier = harnessv2.CancellationBarrierOutcomeUnknown
	}
	return harnessv2.CancelPromptResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: classification,
		BarrierState: barrier, SettlementProven: proven, Settlement: settlement,
	}
}

func (s *Server) handleWorkspaceDelta(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.CreateWorkspaceDeltaRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}

	s.mu.Lock()
	expected := s.fence
	expected.RuntimeSessionUID = request.Metadata.Fence.RuntimeSessionUID
	expected.RuntimeSessionGeneration = request.Metadata.Fence.RuntimeSessionGeneration
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		response, ok := s.workspaceResponses[request.Metadata.OperationID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && ok {
			response.Classification = classification
			writeJSON(w, http.StatusOK, response)
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	prompt := s.prompts[request.Metadata.PromptID]
	if prompt == nil || prompt.settlement == nil || prompt.settlement.TerminalEvent != harnessv2.EventCompleted {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace validation requires a successfully completed prompt", nil)
		return
	}
	settlementDigest, err := harnessv2.CanonicalPromptSettlementDigest(*prompt.settlement)
	if err != nil || request.PromptSettlementDigest != settlementDigest {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "prompt settlement digest does not match", nil)
		return
	}
	s.workspaceDeltas.Add(1)
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	state := s.sessions[sessionID]
	delta := harnessv2.WorkspaceDeltaDescriptor{
		DeltaID: request.DeltaID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
		SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
		State:             harnessv2.WorkspaceDeltaNoChange, Intent: request.Intent, VerifiedBaseline: request.VerifiedBaseline,
		RelativeRoot:     state.relativeRoot,
		NoFollowVerified: true, PublicationSafe: true, FrozenAt: now,
	}
	if s.config.SupportsPublicationFinalization && request.Intent == harnessv2.WorkspaceIntentWrite {
		delta.State = harnessv2.WorkspaceDeltaPrepared
		delta.ManifestDigest = conformanceDigest("manifest-" + string(request.DeltaID))
		delta.Artifact = &harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID("artifact-" + string(request.DeltaID)),
			Digest:     conformanceDigest("artifact-" + string(request.DeltaID)), SizeBytes: 1,
			MediaType: "application/vnd.orka.workspace-delta.v1+tar",
		}
		delta.EntryCount = 1
		delta.ChangedFileCount = 1
	}
	response := harnessv2.CreateWorkspaceDeltaResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, Delta: delta,
	}
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	s.workspaceResponses[request.Metadata.OperationID] = response
	state.deltas[request.DeltaID] = delta
	state.descriptor.State = map[bool]harnessv2.RuntimeSessionState{true: harnessv2.RuntimeSessionStatePublicationPrepared, false: harnessv2.RuntimeSessionStateIdle}[delta.State == harnessv2.WorkspaceDeltaPrepared]
	state.descriptor.LastTransitionAt = now
	s.sessions[sessionID] = state
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handlePublicationFinalization(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.FinalizeRuntimeSessionPublicationRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	s.mu.Lock()
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	state, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil)
		return
	}
	expected := s.fence
	expected.RuntimeSessionUID = state.descriptor.RuntimeSessionUID
	expected.RuntimeSessionGeneration = state.descriptor.Generation
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		response, ok := s.finalizationResponses[request.Metadata.OperationID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && ok {
			response.Classification = classification
			writeJSON(w, http.StatusOK, response)
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	delta, deltaOK := state.deltas[request.WorkspaceDeltaID]
	if !deltaOK || delta.State != harnessv2.WorkspaceDeltaPrepared {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "publication finalization does not match a prepared session", nil)
		return
	}
	requestedReceipt := harnessv2.PublicationFinalizationReceipt{
		WorkspaceDeltaID: request.WorkspaceDeltaID, PublicationID: request.PublicationID,
		PublicationGeneration: request.PublicationGeneration, PublicationVersion: request.PublicationVersion,
		TerminalState: request.TerminalState, TerminalReceiptDigest: request.TerminalReceiptDigest, AppliedAt: now,
	}
	if state.finalization != nil {
		if state.descriptor.State != harnessv2.RuntimeSessionStateFinalizing {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "accepted publication finalization is not in finalizing state", nil)
			return
		}
		if publicationFinalizationMatches(*state.finalization, requestedReceipt) {
			receipt := *state.finalization
			if s.config.BreakFreshPublicationFinalizationAppliedAt {
				receipt.AppliedAt = receipt.AppliedAt.Add(time.Second)
			}
			record := operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
			response := harnessv2.FinalizeRuntimeSessionPublicationResponse{
				Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
				Session: state.descriptor, Finalization: receipt,
			}
			s.operations[request.Metadata.OperationID] = record
			s.finalizationResponses[request.Metadata.OperationID] = response
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, response)
			return
		}
		if !s.config.BreakFreshPublicationFinalizationConflictGuard {
			s.digestConflicts.Add(1)
			s.mu.Unlock()
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "publication finalization receipt conflicts with the accepted receipt", nil)
			return
		}
	} else if state.descriptor.State != harnessv2.RuntimeSessionStatePublicationPrepared {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "runtime session is not awaiting publication finalization", nil)
		return
	}

	receipt := requestedReceipt
	state.descriptor.State = harnessv2.RuntimeSessionStateFinalizing
	state.descriptor.LastTransitionAt = now
	state.finalization = &receipt
	s.sessions[sessionID] = state
	s.operations[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	response := harnessv2.FinalizeRuntimeSessionPublicationResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		Session: state.descriptor, Finalization: receipt,
	}
	s.finalizationResponses[request.Metadata.OperationID] = response
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func publicationFinalizationMatches(left, right harnessv2.PublicationFinalizationReceipt) bool {
	return left.WorkspaceDeltaID == right.WorkspaceDeltaID &&
		left.PublicationID == right.PublicationID &&
		left.PublicationGeneration == right.PublicationGeneration &&
		left.PublicationVersion == right.PublicationVersion &&
		left.TerminalState == right.TerminalState &&
		left.TerminalReceiptDigest == right.TerminalReceiptDigest
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeMutationHeaders(w, r) {
		return
	}
	var request harnessv2.DeleteRuntimeSessionRequest
	if !decodeJSON(w, r, &request) || !s.verifyCapability(w, r, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}

	s.mu.Lock()
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	state, sessionPresent := s.sessions[sessionID]
	existing := operationPtr(s.operations, request.Metadata.OperationID)
	expected := s.fence
	if sessionPresent {
		expected.RuntimeSessionUID = state.descriptor.RuntimeSessionUID
		expected.RuntimeSessionGeneration = state.descriptor.Generation
	} else if response, ok := s.deleteResponses[request.Metadata.OperationID]; ok {
		expected.RuntimeSessionUID = response.Tombstone.RuntimeSessionUID
		expected.RuntimeSessionGeneration = response.Tombstone.RuntimeSessionGeneration
	} else {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil)
		return
	}
	classification, err := s.classifyOperation(expected, request.Metadata, existing, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil)
		return
	}
	if existing != nil {
		response, ok := s.deleteResponses[request.Metadata.OperationID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && ok {
			response.Classification = classification
			writeJSON(w, http.StatusOK, response)
			return
		}
		writeClassificationError(w, classification)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	if !sessionPresent {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil)
		return
	}
	publicationPrepared := false
	for _, delta := range state.deltas {
		if delta.State == harnessv2.WorkspaceDeltaPrepared {
			publicationPrepared = true
			break
		}
	}
	if publicationPrepared &&
		(state.descriptor.State != harnessv2.RuntimeSessionStateFinalizing || state.finalization == nil) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "write session publication is not finalized", nil)
		return
	}
	deleteAttempt := s.sessionDeleteAttempts.Add(1)
	if s.config.FailFirstSessionDeleteBeforeApply && deleteAttempt == 1 {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "injected delete failure before apply", nil)
		return
	}
	s.sessionDeletes.Add(1)
	deleteRecord := operationRecord(request.Metadata, harnessv2.OperationPhaseDeleted, "", now)
	tombstone := harnessv2.RuntimeSessionTombstone{
		RuntimeSessionUID: state.descriptor.RuntimeSessionUID, RuntimeSessionGeneration: state.descriptor.Generation,
		RuntimeProfileDigest: state.descriptor.RuntimeProfileDigest, DeletedAt: now, Operations: []harnessv2.OperationRecord{deleteRecord},
	}
	response := harnessv2.DeleteRuntimeSessionResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		State: harnessv2.RuntimeSessionStateDeleted, Tombstone: tombstone,
	}
	s.operations[request.Metadata.OperationID] = deleteRecord
	s.deleteResponses[request.Metadata.OperationID] = response
	delete(s.sessions, sessionID)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) classifyOperation(
	expected harnessv2.Fence,
	incoming harnessv2.MutationMetadata,
	existing *harnessv2.OperationRecord,
	now time.Time,
) (harnessv2.Classification, error) {
	return s.classifyOperationForScope(expected, incoming, existing, true, now)
}

func (s *Server) classifyPoolOperation(
	expected harnessv2.Fence,
	incoming harnessv2.MutationMetadata,
	existing *harnessv2.OperationRecord,
	now time.Time,
) (harnessv2.Classification, error) {
	return s.classifyOperationForScope(expected, incoming, existing, false, now)
}

func (s *Server) classifyOperationForScope(
	expected harnessv2.Fence,
	incoming harnessv2.MutationMetadata,
	existing *harnessv2.OperationRecord,
	requireSession bool,
	now time.Time,
) (harnessv2.Classification, error) {
	classification, err := harnessv2.ClassifyOperation(expected, incoming, existing, requireSession, now)
	if err != nil {
		return harnessv2.Classification{}, err
	}
	if existing != nil {
		if s.config.BreakDuplicateSafeMutations && existing.RequestDigest == incoming.RequestDigest {
			classification = harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}
		}
		if s.config.BreakDigestConflictClassification && existing.RequestDigest != incoming.RequestDigest {
			classification = harnessv2.Classification{Class: harnessv2.RequestClassificationDuplicate, Phase: existing.Phase}
		}
	}
	switch classification.Class {
	case harnessv2.RequestClassificationDuplicate, harnessv2.RequestClassificationAlreadyAccepted, harnessv2.RequestClassificationSettled:
		s.replayClassifications.Add(1)
	case harnessv2.RequestClassificationDigestConflict:
		s.digestConflicts.Add(1)
	}
	return classification, nil
}

func (s *Server) authorizeBearer(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "Bearer "+s.config.ControllerBearerToken {
		writeError(w, http.StatusUnauthorized, harnessv2.ErrorCodeUnauthenticated, "authentication required", nil)
		return false
	}
	return true
}

func (s *Server) authorizeMutationHeaders(w http.ResponseWriter, r *http.Request) bool {
	if !s.authorizeBearer(w, r) {
		return false
	}
	if r.Header.Get(harnessv2.OperationCapabilityHeader) == "" {
		writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "operation capability required", nil)
		return false
	}
	return true
}

func (s *Server) verifyCapability(w http.ResponseWriter, r *http.Request, metadata harnessv2.MutationMetadata) bool {
	if err := harnessv2.VerifyOperationCapability(s.config.OperationCapabilitySecret, r.Header.Get(harnessv2.OperationCapabilityHeader), metadata, true, time.Now().UTC()); err != nil {
		writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "invalid operation capability", nil)
		return false
	}
	return true
}

func (s *Server) verifyPoolCapability(w http.ResponseWriter, r *http.Request, metadata harnessv2.MutationMetadata) bool {
	if err := harnessv2.VerifyOperationCapability(s.config.OperationCapabilitySecret, r.Header.Get(harnessv2.OperationCapabilityHeader), metadata, false, time.Now().UTC()); err != nil {
		writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "invalid operation capability", nil)
		return false
	}
	return true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, output any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "invalid request", nil)
		return false
	}
	return true
}

func writeClassificationError(w http.ResponseWriter, classification harnessv2.Classification) {
	status := classification.HTTPStatus()
	code := harnessv2.ErrorCodeInvalidRequest
	switch classification.Class {
	case harnessv2.RequestClassificationDigestConflict:
		code = harnessv2.ErrorCodeDigestConflict
	case harnessv2.RequestClassificationStaleFence:
		code = harnessv2.ErrorCodeStaleFence
	case harnessv2.RequestClassificationExpired:
		code = harnessv2.ErrorCodeExpired
	case harnessv2.RequestClassificationAlreadyAccepted:
		code = harnessv2.ErrorCodeAlreadyAccepted
	case harnessv2.RequestClassificationSettled:
		code = harnessv2.ErrorCodeSettled
	case harnessv2.RequestClassificationDuplicate:
		status = http.StatusConflict
	}
	if status == 0 {
		status = http.StatusConflict
	}
	writeError(w, status, code, string(classification.Class), &classification)
}

func writeError(w http.ResponseWriter, status int, code harnessv2.ErrorCode, message string, classification *harnessv2.Classification) {
	writeJSON(w, status, harnessv2.ErrorResponse{
		Protocol: harnessv2.ProtocolVersion, Code: code, Message: message, Classification: classification, Retryable: false,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func conformanceDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func operationRecord(metadata harnessv2.MutationMetadata, phase harnessv2.OperationPhase, terminal harnessv2.EventType, now time.Time) harnessv2.OperationRecord {
	return harnessv2.OperationRecord{
		OperationID: metadata.OperationID, RequestDigest: metadata.RequestDigest, Phase: phase,
		TerminalEvent: terminal, RecordedAt: now, UpdatedAt: now,
	}
}

func operationPtr(records map[harnessv2.OperationID]harnessv2.OperationRecord, id harnessv2.OperationID) *harnessv2.OperationRecord {
	record, ok := records[id]
	if !ok {
		return nil
	}
	return &record
}

func eventIdentity(metadata harnessv2.MutationMetadata, sequence uint64, at time.Time) harnessv2.EventIdentity {
	return harnessv2.EventIdentity{
		RuntimeInstanceID: metadata.Fence.RuntimeInstanceID, SupervisorBootID: metadata.Fence.SupervisorBootID,
		RuntimeSessionUID: metadata.Fence.RuntimeSessionUID, RuntimeSessionGeneration: metadata.Fence.RuntimeSessionGeneration,
		TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		Sequence: sequence, RequestDigest: metadata.RequestDigest, Timestamp: at,
	}
}
