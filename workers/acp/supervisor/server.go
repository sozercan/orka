package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

type Server struct {
	cfg                    Config
	mux                    *http.ServeMux
	providerProxy          *providerProxy
	mcpProxy               *mcpProxy
	identityLock           io.Closer
	e2ePromptWriteFaultDir string
	e2ePromptWriteRecorder E2EPromptWriteFaultRecorder

	mu            sync.Mutex
	lifecycle     harnessv2.SupervisorLifecycle
	drain         harnessv2.DrainStatus
	sessions      map[harnessv2.RuntimeSessionID]*sessionState
	tombstones    map[harnessv2.RuntimeSessionUID]sessionTombstone
	failedCreates map[harnessv2.RuntimeSessionUID]failedCreateReplay
	poolOps       map[harnessv2.OperationID]harnessv2.OperationRecord
	statusNonces  map[string]time.Time
	promptSlots   chan struct{}
}

const e2ePromptWriteAmbiguityLedgerDir = ".orka-e2e-prompt-write-ambiguity"

func prepareE2EPromptWriteAmbiguityLedger(identityStateDir string) (string, error) {
	dir := filepath.Join(identityStateDir, e2ePromptWriteAmbiguityLedgerDir)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create ledger directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", fmt.Errorf("inspect ledger directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("ledger directory must be a real mode 0700 directory")
	}
	return dir, nil
}

// consumeE2EPromptWriteFaultLocked records the test fault before the handler
// aborts its connection. Durable workspace pools use one exclusive-create file
// per operation; direct pools use a controller-owned record. The caller must
// hold s.mu.
func (s *Server) consumeE2EPromptWriteFaultLocked(
	ctx context.Context,
	metadata harnessv2.MutationMetadata,
) (bool, error) {
	if s.e2ePromptWriteFaultDir == "" {
		if s.e2ePromptWriteRecorder == nil {
			return false, fmt.Errorf("E2E prompt write fault recorder is unavailable")
		}
		return s.e2ePromptWriteRecorder.Consume(ctx, metadata)
	}

	digest := sha256.Sum256([]byte(metadata.OperationID))
	recordPath := filepath.Join(s.e2ePromptWriteFaultDir, "operation-"+hex.EncodeToString(digest[:]))
	record, err := os.OpenFile(recordPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(recordPath)
		if statErr != nil {
			return false, fmt.Errorf("inspect existing operation record: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() != 0 {
			return false, fmt.Errorf("existing operation record must be an empty mode 0600 regular file")
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("create operation record: %w", err)
	}
	removeRecord := func() {
		_ = record.Close()
		_ = os.Remove(recordPath)
	}
	if err := record.Chmod(0o600); err != nil {
		removeRecord()
		return false, fmt.Errorf("chmod operation record: %w", err)
	}
	if err := record.Sync(); err != nil {
		removeRecord()
		return false, fmt.Errorf("sync operation record: %w", err)
	}
	if err := record.Close(); err != nil {
		_ = os.Remove(recordPath)
		return false, fmt.Errorf("close operation record: %w", err)
	}
	directory, err := os.Open(s.e2ePromptWriteFaultDir)
	if err != nil {
		_ = os.Remove(recordPath)
		return false, fmt.Errorf("open ledger directory for sync: %w", err)
	}
	defer directory.Close() //nolint:errcheck
	if err := directory.Sync(); err != nil {
		_ = os.Remove(recordPath)
		return false, fmt.Errorf("sync ledger directory: %w", err)
	}
	return true, nil
}

// statusNonceRetentionSlack keeps a consumed status nonce past its capability
// TTL by a clock-skew margin so a token cannot be replayed at the edge of
// expiry.
const statusNonceRetentionSlack = 30 * time.Second

// operationReplayRetentionSlack keeps a session operation and any stored
// response briefly past the request capability expiry. This preserves replay
// classification across the allowed clock-skew edge without retaining expired
// capabilities for the lifetime of a resident session.
const operationReplayRetentionSlack = 30 * time.Second

const sessionDeletionOperationReserve = 1

// tombstoneRetention bounds how long a deleted or failed session's replay
// tombstone is retained. It must exceed the maximum operation-capability
// lifetime plus clock skew so a legitimate replay whose capability is still
// valid is still classified against its tombstone; past this window the
// capability itself has expired and the replay is rejected at the capability
// layer, so the tombstone is no longer needed. Retain every tombstone inside
// this window: the finite, never-reused UID/GID allocator already bounds their
// count, while count-based eviction would reopen valid create capabilities for
// replay.
const tombstoneRetention = time.Hour

type sessionTombstone struct {
	harnessv2.RuntimeSessionTombstone
	sessionID     harnessv2.RuntimeSessionID
	prompt        *retiredPromptSettlement
	cancellations map[harnessv2.OperationID]*operationReplay
	// Late cancellation records stay separate so explicit deletion replays
	// continue to return the original immutable wire tombstone.
	cancellationOperations map[harnessv2.OperationID]harnessv2.OperationRecord
}

// A retired prompt retains proof, never its input, output, or capabilities.
// Its lifetime follows the session tombstone, independently of the admission
// capability, which can expire while a renewed prompt is still running.
type retiredPromptSettlement struct {
	metadata   harnessv2.MutationMetadata
	settlement harnessv2.PromptSettlement
}

// pruneTombstonesLocked drops only records older than tombstoneRetention. It
// must be called with s.mu held, before a new tombstone is inserted.
func (s *Server) pruneTombstonesLocked(now time.Time) {
	for uid, tombstone := range s.tombstones {
		if now.Sub(tombstone.DeletedAt) > tombstoneRetention {
			delete(s.tombstones, uid)
			delete(s.failedCreates, uid)
		}
	}
}

type sessionCreationError struct {
	stage               string
	cause               error
	workspaceResumeLost bool
}

const sessionCreationStageDurableResumeVerification = "durable resume verification"

type failedCreateReplay struct {
	operationID   harnessv2.OperationID
	requestDigest harnessv2.RequestDigest
	statusCode    int
	code          harnessv2.ErrorCode
	message       string
	retryable     bool
}

func (e *sessionCreationError) Error() string {
	return e.stage + ": " + e.cause.Error()
}

func (e *sessionCreationError) Unwrap() error { return e.cause }

func sessionCreationFailed(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &sessionCreationError{stage: stage, cause: err}
}

func sessionCreationResumeLost(err error) error {
	if err == nil {
		return nil
	}
	return &sessionCreationError{
		stage:               sessionCreationStageDurableResumeVerification,
		cause:               err,
		workspaceResumeLost: true,
	}
}

func isSessionCreationResumeLost(err error) bool {
	creation, ok := errors.AsType[*sessionCreationError](err)
	return ok && creation.workspaceResumeLost
}

func durableWorkspacePreparationFailed(expectResume bool, err error) error {
	if expectResume && errors.Is(err, acp.ErrDurableWorkspaceCheckpointUnusable) {
		return sessionCreationResumeLost(err)
	}
	return sessionCreationFailed("durable workspace preparation", err)
}

func sessionCreationStage(err error) string {
	if creation, ok := errors.AsType[*sessionCreationError](err); ok {
		return creation.stage
	}
	return "unclassified"
}

type sessionState struct {
	id                      harnessv2.RuntimeSessionID
	runtime                 *acp.RuntimeSession
	promptMutations         promptMutationExecutor
	descriptor              harnessv2.RuntimeSessionDescriptor
	operations              map[harnessv2.OperationID]harnessv2.OperationRecord
	operationRetention      map[harnessv2.OperationID]time.Time
	operationReplays        map[harnessv2.OperationID]*operationReplay
	prompt                  *promptState
	permissions             map[harnessv2.PermissionRequestID]permissionState
	paths                   acp.SessionPaths
	baseline                *workspacedelta.Snapshot
	workspaceIntent         harnessv2.WorkspaceIntent
	workspaceRelativeRoot   string
	deltas                  map[harnessv2.WorkspaceDeltaID]harnessv2.CreateWorkspaceDeltaResponse
	providerProxy           *providerProxySession
	mcpProxy                *mcpProxySession
	profile                 harnessv2.RuntimeProfile
	agentConfiguration      harnessv2.AgentSessionConfiguration
	agentDiagnosticFilter   *AgentDiagnosticFilter
	creating                bool
	drainCleanupScheduled   bool
	publicationFinalization *harnessv2.PublicationFinalizationReceipt
}

type promptState struct {
	request             harnessv2.StartPromptRequest
	operation           harnessv2.OperationRecord
	lease               harnessv2.PromptLease
	startedAt           time.Time
	acceptedAt          time.Time
	sequence            uint64
	assistant           strings.Builder
	assistantOverflow   bool
	finalAnswer         strings.Builder
	finalAnswerSeen     bool
	finalAnswerOverflow bool
	settlement          *harnessv2.PromptSettlement
	settlementDigest    string
	// providerDrainTimedOut records that an admitted inference request was
	// still in flight when the child settled and did not finish within the
	// cancel grace, so the prompt's inference accounting is incomplete.
	providerDrainTimedOut bool
	permissionRequestIDs  map[harnessv2.PermissionRequestID]struct{}
}

type promptMutationExecutor interface {
	ResolvePermission(promptID, requestID string, outcome acp.RequestPermissionOutcome) error
	CancelPrompt(context.Context, string) (acp.PromptResult, error)
}

type operationReplay struct {
	done           chan struct{}
	isCancellation bool
	admission      *harnessv2.PromptAdmissionResponse
	permission     *harnessv2.PermissionResolutionResponse
	cancellation   *harnessv2.CancelPromptResponse
	lease          *harnessv2.PromptLeaseResponse
	failure        *operationFailure
}

type operationFailure struct {
	status    int
	code      harnessv2.ErrorCode
	message   string
	retryable bool
}

// recordSessionOperationLocked records a session operation and the deadline
// after which neither its classification record nor response replay is useful.
// The caller must hold s.mu when the state belongs to a live Server.
func recordSessionOperationLocked(
	state *sessionState,
	metadata harnessv2.MutationMetadata,
	phase harnessv2.OperationPhase,
	terminal harnessv2.EventType,
	at time.Time,
) {
	if state.operations == nil {
		state.operations = make(map[harnessv2.OperationID]harnessv2.OperationRecord)
	}
	if state.operationRetention == nil {
		state.operationRetention = make(map[harnessv2.OperationID]time.Time)
	}
	state.operations[metadata.OperationID] = operationRecord(metadata, phase, terminal, at)
	state.operationRetention[metadata.OperationID] = metadata.ExpiresAt.Add(operationReplayRetentionSlack)
}

// pruneSessionOperationsLocked drops operations whose capability expiry and
// replay-skew window have elapsed. The caller must hold s.mu when the state
// belongs to a live Server.
func pruneSessionOperationsLocked(state *sessionState, now time.Time) {
	if state == nil {
		return
	}
	for operationID, retainUntil := range state.operationRetention {
		if retainUntil.After(now) {
			continue
		}
		delete(state.operationRetention, operationID)
		delete(state.operations, operationID)
		delete(state.operationReplays, operationID)
	}
}

func sessionOperationPtrLocked(state *sessionState, operationID harnessv2.OperationID, now time.Time) *harnessv2.OperationRecord {
	pruneSessionOperationsLocked(state, now)
	return operationPtr(state.operations, operationID)
}

// ensureSessionOperationCapacityLocked preserves one final journal slot for
// explicit deletion. Tombstones retain every still-replayable operation, so a
// live session must stop accepting fresh mutations before the protocol limit
// is reached rather than evicting valid replay records.
func ensureSessionOperationCapacityLocked(state *sessionState, reservedSlots int) error {
	if state == nil || reservedSlots < 0 || reservedSlots >= harnessv2.MaxRuntimeSessionTombstoneOperations {
		return fmt.Errorf("invalid runtime session operation capacity request")
	}
	if len(state.operations) >= harnessv2.MaxRuntimeSessionTombstoneOperations-reservedSlots {
		return fmt.Errorf("runtime session operation journal is full and the session must be retired")
	}
	return nil
}

type permissionState struct {
	requestID   harnessv2.PermissionRequestID
	toolCallID  string
	toolName    string
	title       string
	requestedAt time.Time
	expiresAt   time.Time
	options     map[string]harnessv2.PermissionOptionKind
}

type drainCleanupCandidate struct {
	id    harnessv2.RuntimeSessionID
	state *sessionState
}

func New(cfg Config) (*Server, error) {
	return newServer(cfg, prepareSessionIdentityState)
}

func newServer(cfg Config, prepareIdentityState func(string, *acp.UIDAllocator) (io.Closer, error)) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.UIDAllocator.Remaining() != cfg.UIDAllocator.Capacity() {
		return nil, fmt.Errorf("UID allocator must be fresh when the supervisor starts")
	}
	if prepareIdentityState == nil {
		return nil, fmt.Errorf("session identity state preparation is required")
	}
	identityStateDir := cfg.SessionBaseDir
	if strings.TrimSpace(cfg.DurableWorkspaceDir) != "" {
		// The session base directory dies with a data-only cold suspension
		// while the durable workspace root survives it. The allocator
		// high-water mark must live with the surviving data, or a cold-booted
		// supervisor would restart allocation at zero and hand the
		// continuation the same UID/GID the pre-suspension session used.
		identityStateDir = filepath.Join(cfg.DurableWorkspaceDir, ".session-identity")
		if err := validateDurableCheckpointIdentityState(cfg.DurableWorkspaceDir, identityStateDir); err != nil {
			return nil, fmt.Errorf("inspect durable session identity state: %w", err)
		}
	}
	identityLock, err := prepareIdentityState(identityStateDir, cfg.UIDAllocator)
	if err != nil {
		return nil, fmt.Errorf("prepare session identity state: %w", err)
	}
	keepIdentityLock := false
	defer func() {
		if !keepIdentityLock {
			_ = identityLock.Close()
		}
	}()
	e2ePromptWriteFaultDir := ""
	if cfg.E2EPromptWriteAmbiguityMarker != "" && cfg.DurableWorkspaceDir != "" {
		e2ePromptWriteFaultDir, err = prepareE2EPromptWriteAmbiguityLedger(identityStateDir)
		if err != nil {
			return nil, fmt.Errorf("prepare E2E prompt write ambiguity ledger: %w", err)
		}
	}
	proxy, err := newProviderProxy(cfg.ProviderProxy)
	if err != nil {
		return nil, err
	}
	mcp, err := newMCPProxy(cfg.MCPBroker)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = proxy.close(closeCtx)
		return nil, err
	}
	cfg.ProviderProxy.UpstreamBearerToken = ""
	cfg.MCPBroker = nil
	server := &Server{
		cfg:                    cfg,
		mux:                    http.NewServeMux(),
		providerProxy:          proxy,
		mcpProxy:               mcp,
		identityLock:           identityLock,
		e2ePromptWriteFaultDir: e2ePromptWriteFaultDir,
		e2ePromptWriteRecorder: cfg.E2EPromptWriteFaultRecorder,
		lifecycle:              harnessv2.SupervisorLifecycleReady,
		drain:                  harnessv2.DrainStatus{AcceptingNewSessions: true},
		sessions:               make(map[harnessv2.RuntimeSessionID]*sessionState),
		tombstones:             make(map[harnessv2.RuntimeSessionUID]sessionTombstone),
		failedCreates:          make(map[harnessv2.RuntimeSessionUID]failedCreateReplay),
		poolOps:                make(map[harnessv2.OperationID]harnessv2.OperationRecord),
		promptSlots:            make(chan struct{}, cfg.Capabilities.Limits.MaxConcurrentPrompts),
	}
	if server.sessionIdentityCapacity().RotationRequired() {
		server.drain.AcceptingNewSessions = false
	}
	server.registerRoutes()
	keepIdentityLock = true
	return server, nil
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET "+harnessv2.HealthPath, s.handleHealth)
	s.mux.HandleFunc("GET "+harnessv2.CapabilitiesPath, s.handleCapabilities)
	s.mux.HandleFunc("GET "+harnessv2.StatusPath, s.handleStatus)
	s.mux.HandleFunc("PUT "+harnessv2.DrainPath, s.handleDrain)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}", s.handleCreateSession)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/publication-finalization", s.handleFinalizeSessionPublication)
	s.mux.HandleFunc("DELETE /v2/runtime-sessions/{sessionID}", s.handleDeleteSession)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}", s.handleStartPrompt)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}/lease", s.handleRenewLease)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}/permissions/{requestID}", s.handleResolvePermission)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/prompts/{promptID}/cancel", s.handleCancelPrompt)
	s.mux.HandleFunc("PUT /v2/runtime-sessions/{sessionID}/workspace-deltas/{deltaID}", s.handleWorkspaceDelta)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	status := harnessv2.HealthStatusOK
	switch s.lifecycle {
	case harnessv2.SupervisorLifecycleUnhealthy:
		status = harnessv2.HealthStatusUnhealthy
	case harnessv2.SupervisorLifecycleDraining, harnessv2.SupervisorLifecycleTerminating:
		status = harnessv2.HealthStatusDegraded
	}
	if status == harnessv2.HealthStatusOK && !s.drain.AcceptingNewSessions {
		status = harnessv2.HealthStatusDegraded
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, harnessv2.HealthResponse{Protocol: harnessv2.ProtocolVersion, Status: status, Timestamp: time.Now().UTC()})
}

func (s *Server) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.cfg.Capabilities)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeController(w, r) {
		return
	}
	// Status discloses Task, prompt, permission, and fence identifiers, so it
	// requires proof of the operation-capability secret in addition to the
	// controller bearer; a disclosed bearer alone must not read it. The
	// capability is bound to this runtime's profile and carries a single-use
	// nonce so a captured token cannot be replayed within its TTL.
	if s.cfg.RequireCapabilities {
		binding := harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: s.cfg.Fence.RuntimeProfileDigest,
			RuntimeInstanceID:    s.cfg.Fence.RuntimeInstanceID,
		}
		nonce, err := harnessv2.VerifyStatusCapability(s.cfg.CapabilitySecret, r.Header.Get(OperationCapabilityHeader), binding, time.Now().UTC())
		if err != nil || !s.consumeStatusNonce(nonce) {
			writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "status authorization failed", nil, false)
			return
		}
	}
	writeJSON(w, http.StatusOK, s.status())
}

// tombstoneFailedCreateLocked records a tombstone for a create that failed
// after identity allocation so a replay of the same request is classified as
// a duplicate rather than allocating another non-reused identity. It must be
// called with s.mu held.
func (s *Server) tombstoneFailedCreateLocked(
	sessionID harnessv2.RuntimeSessionID,
	metadata harnessv2.MutationMetadata,
	recordedAt time.Time,
	replay *failedCreateReplay,
) {
	state, ok := s.sessions[sessionID]
	if !ok || !state.creating {
		return
	}
	delete(s.sessions, sessionID)
	s.pruneTombstonesLocked(time.Now().UTC())
	s.tombstones[metadata.Fence.RuntimeSessionUID] = sessionTombstone{RuntimeSessionTombstone: harnessv2.RuntimeSessionTombstone{
		RuntimeSessionUID:        metadata.Fence.RuntimeSessionUID,
		RuntimeSessionGeneration: metadata.Fence.RuntimeSessionGeneration,
		RuntimeProfileDigest:     s.cfg.Fence.RuntimeProfileDigest,
		DeletedAt:                time.Now().UTC(),
		Operations:               []harnessv2.OperationRecord{operationRecord(metadata, harnessv2.OperationPhaseRecorded, "", recordedAt)},
	}}
	if replay == nil {
		delete(s.failedCreates, metadata.Fence.RuntimeSessionUID)
		return
	}
	if s.failedCreates == nil {
		s.failedCreates = make(map[harnessv2.RuntimeSessionUID]failedCreateReplay)
	}
	s.failedCreates[metadata.Fence.RuntimeSessionUID] = *replay
}

// rejectTombstonedSessionCreateLocked classifies a create against the deletion
// tombstone for its session UID/generation. It must be called with s.mu held;
// when it handles the request it unlocks s.mu, writes the response, and
// returns true, so the caller must return immediately.
func (s *Server) rejectTombstonedSessionCreateLocked(
	w http.ResponseWriter,
	metadata harnessv2.MutationMetadata,
	expected harnessv2.Fence,
	now time.Time,
) bool {
	tombstone, ok := s.tombstones[metadata.Fence.RuntimeSessionUID]
	if !ok || tombstone.RuntimeSessionGeneration < metadata.Fence.RuntimeSessionGeneration {
		return false
	}
	operations := make(map[harnessv2.OperationID]harnessv2.OperationRecord, len(tombstone.Operations))
	for i := range tombstone.Operations {
		operations[tombstone.Operations[i].OperationID] = tombstone.Operations[i]
	}
	classification, classifyErr := harnessv2.ClassifyOperation(expected, metadata, operationPtr(operations, metadata.OperationID), true, now)
	replay, replayExists := s.failedCreates[metadata.Fence.RuntimeSessionUID]
	s.mu.Unlock()
	if classifyErr != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, classifyErr.Error(), nil, false)
		return true
	}
	slog.Info("ACP runtime session create rejected by deletion tombstone",
		"runtimeSessionUID", metadata.Fence.RuntimeSessionUID, "runtimeSessionGeneration", metadata.Fence.RuntimeSessionGeneration,
		"tombstonedGeneration", tombstone.RuntimeSessionGeneration, "operationID", metadata.OperationID,
		"classification", classification.Class, "existingPhase", classification.Phase)
	if classification.Class == harnessv2.RequestClassificationFresh {
		// The session UID/generation is tombstoned but this operation was never
		// recorded on it: fail closed rather than resurrect it.
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, "runtime session was deleted", nil, false)
		return true
	}
	if classification.Class == harnessv2.RequestClassificationDuplicate {
		if replayExists &&
			replay.operationID == metadata.OperationID && replay.requestDigest == metadata.RequestDigest {
			writeError(w, replay.statusCode, replay.code, replay.message, nil, replay.retryable)
			return true
		}
	}
	writeClassificationError(w, classification)
	return true
}

// consumeStatusNonce records a status capability nonce and reports whether it
// was previously unseen. Expired nonces are pruned opportunistically; the map
// stays bounded because nonces live only for the capability TTL.
func (s *Server) consumeStatusNonce(nonce string) bool {
	if nonce == "" {
		return false
	}
	now := time.Now().UTC()
	expiry := now.Add(harnessv2.DefaultStatusCapabilityTTL + statusNonceRetentionSlack)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statusNonces == nil {
		s.statusNonces = make(map[string]time.Time)
	}
	for seen, seenExpiry := range s.statusNonces {
		if !seenExpiry.After(now) {
			delete(s.statusNonces, seen)
		}
	}
	if _, replayed := s.statusNonces[nonce]; replayed {
		return false
	}
	s.statusNonces[nonce] = expiry
	return true
}

func (s *Server) status() harnessv2.StatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	response := harnessv2.StatusResponse{
		Protocol:                harnessv2.ProtocolVersion,
		Fence:                   s.cfg.Fence,
		Lifecycle:               s.lifecycle,
		Drain:                   s.drain,
		SessionIdentityCapacity: s.sessionIdentityCapacity(),
		Timestamp:               now,
	}
	for _, state := range s.sessions {
		pruneSessionOperationsLocked(state, now)
		status := harnessv2.RuntimeSessionStatus{
			RuntimeSessionID:        state.descriptor.RuntimeSessionID,
			RuntimeSessionUID:       state.descriptor.RuntimeSessionUID,
			Generation:              state.descriptor.Generation,
			State:                   state.descriptor.State,
			PendingPermissionCount:  uint32(len(state.permissions)),
			ReservedForFinalization: state.descriptor.State == harnessv2.RuntimeSessionStateFinalizing && state.publicationFinalization != nil,
			LiveDescendantCount:     liveDescendantCount(state.runtime),
			LastTransitionAt:        state.descriptor.LastTransitionAt,
		}
		if state.prompt != nil && state.prompt.settlement == nil {
			status.ActivePromptID = state.prompt.request.Metadata.PromptID
			response.ActivePrompts = append(response.ActivePrompts, harnessv2.ActivePromptStatus{
				RuntimeSessionUID:  state.descriptor.RuntimeSessionUID,
				SessionGeneration:  state.descriptor.Generation,
				TaskUID:            state.prompt.request.Metadata.TaskUID,
				TaskAttempt:        state.prompt.request.Metadata.TaskAttempt,
				PromptID:           state.prompt.request.Metadata.PromptID,
				LeaseExpiresAt:     state.prompt.lease.ExpiresAt,
				FrameSequence:      max(state.prompt.sequence, 1),
				PendingPermissions: uint32(len(state.permissions)),
				StartedAt:          state.prompt.startedAt,
			})
		}
		response.Sessions = append(response.Sessions, status)
		for _, permission := range state.permissions {
			response.PendingPermissions = append(response.PendingPermissions, harnessv2.PendingPermissionStatus{
				RuntimeSessionUID: state.descriptor.RuntimeSessionUID,
				PromptID:          state.prompt.request.Metadata.PromptID,
				RequestID:         permission.requestID,
				RequestedAt:       permission.requestedAt,
				ExpiresAt:         permission.expiresAt,
			})
		}
	}
	response.Pressure = harnessv2.PressureMetadata{
		ResidentSessions:   uint32(len(response.Sessions)),
		ActivePrompts:      uint32(len(response.ActivePrompts)),
		PendingPermissions: uint32(len(response.PendingPermissions)),
		LiveDescendants:    totalLiveDescendants(response.Sessions),
	}
	return response
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.DrainRequest
	if !s.decodeMutation(w, r, &request, false, request.Metadata) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, false) {
		return
	}
	s.mu.Lock()
	classification, err := harnessv2.ClassifyOperation(s.cfg.Fence, request.Metadata, operationPtr(s.poolOps, request.Metadata.OperationID), false, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	var cleanup []drainCleanupCandidate
	if classification.Class == harnessv2.RequestClassificationFresh {
		s.poolOps[request.Metadata.OperationID] = operationRecord(request.Metadata, harnessv2.OperationPhaseApplied, "", now)
		cleanup = s.beginDrainLocked(request.Reason, now)
	}
	response := harnessv2.DrainResponse{Protocol: harnessv2.ProtocolVersion, Classification: classification, Drain: s.drain}
	s.mu.Unlock()
	s.startDrainCleanup(cleanup)
	status := classification.HTTPStatus()
	if status == 0 {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.CreateRuntimeSessionRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if request.AgentConfiguration == nil && s.cfg.Provider.Kind != providerKindAgentKit {
		writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime is waiting for a controller that supports Agent session configuration", nil, true)
		return
	}
	if string(request.RuntimeSessionID) != r.PathValue("sessionID") {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "runtime session path does not match request", nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(request.Profile)
	if err != nil || profileDigest != s.cfg.Fence.RuntimeProfileDigest || request.Metadata.Fence.RuntimeProfileDigest != profileDigest {
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, "runtime profile does not match this supervisor instance", nil, false)
		return
	}
	if request.Profile.ProviderKind != s.cfg.Provider.Kind || request.Profile.Model != s.cfg.Provider.Model {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "requested provider profile is not available in this image", nil, false)
		return
	}
	expected := s.expectedFence(request.Metadata.Fence.RuntimeSessionUID, request.Metadata.Fence.RuntimeSessionGeneration)
	// Fresh creates have no operation record or tombstone to classify, but
	// must still match the current supervisor before allocating an identity.
	if mismatch := harnessv2.CompareFence(expected, request.Metadata.Fence, true); mismatch != harnessv2.FenceMatch {
		writeClassificationError(w, harnessv2.Classification{Class: harnessv2.RequestClassificationStaleFence, FenceMismatch: mismatch})
		return
	}

	s.mu.Lock()
	if existing := s.sessions[request.RuntimeSessionID]; existing != nil {
		classification, classifyErr := harnessv2.ClassifyOperation(expected, request.Metadata, sessionOperationPtrLocked(existing, request.Metadata.OperationID, now), true, now)
		if classifyErr != nil {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, classifyErr.Error(), nil, false)
			return
		}
		if classification.Class == harnessv2.RequestClassificationDuplicate && !existing.creating {
			response := harnessv2.CreateRuntimeSessionResponse{Protocol: harnessv2.ProtocolVersion, Classification: classification, Session: existing.descriptor}
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, response)
			return
		}
		existingState, existingCreating := existing.descriptor.State, existing.creating
		s.mu.Unlock()
		// The controller only sees the classification code; record the resident
		// session's state so a rejected create can be reconstructed from the
		// supervisor log without exposing request content.
		slog.Info("ACP runtime session create rejected by replay classification",
			"runtimeSessionID", request.RuntimeSessionID, "operationID", request.Metadata.OperationID,
			"classification", classification.Class, "existingPhase", classification.Phase,
			"sessionState", existingState, "creating", existingCreating)
		writeClassificationError(w, classification)
		return
	}
	// A create replayed after the session was deleted must not recreate it and
	// re-run the prompt: consult the deletion tombstone before admitting a new
	// OS identity.
	if handled := s.rejectTombstonedSessionCreateLocked(w, request.Metadata, expected, now); handled {
		return
	}
	if !s.drain.AcceptingNewSessions || s.lifecycle != harnessv2.SupervisorLifecycleReady {
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime pool is not accepting new sessions", nil, true)
		return
	}
	if len(s.sessions) >= int(s.cfg.Capabilities.Limits.MaxResidentSessions) {
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime pool is at resident-session capacity", nil, true)
		return
	}
	uid, gid, err := s.cfg.UIDAllocator.AllocateAboveReserve(sessionIdentityExhaustionReserve)
	if err != nil {
		s.drain.AcceptingNewSessions = false
		s.mu.Unlock()
		if errors.Is(err, acp.ErrUIDReserveReached) || errors.Is(err, acp.ErrUIDRangeExhausted) {
			writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime pool is rotating before session identity exhaustion", nil, true)
			return
		}
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "runtime session identity allocation failed", nil, true)
		return
	}
	state := &sessionState{
		id:       request.RuntimeSessionID,
		creating: true,
		descriptor: harnessv2.RuntimeSessionDescriptor{
			RuntimeSessionID: request.RuntimeSessionID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
			Generation: request.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: s.cfg.Fence.RuntimeInstanceID,
			SupervisorBootID: s.cfg.Fence.SupervisorBootID, RuntimeProfileDigest: s.cfg.Fence.RuntimeProfileDigest,
			State: harnessv2.RuntimeSessionStateCreating, WorkspaceBaseline: request.Workspace.Baseline,
			CreatedAt: now, LastTransitionAt: now,
		},
		operations:         make(map[harnessv2.OperationID]harnessv2.OperationRecord),
		operationRetention: make(map[harnessv2.OperationID]time.Time),
		operationReplays:   make(map[harnessv2.OperationID]*operationReplay),
		permissions:        make(map[harnessv2.PermissionRequestID]permissionState),
		deltas:             make(map[harnessv2.WorkspaceDeltaID]harnessv2.CreateWorkspaceDeltaResponse),
	}
	delete(s.failedCreates, request.Metadata.Fence.RuntimeSessionUID)
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseRecorded, "", now)
	s.sessions[request.RuntimeSessionID] = state
	if s.sessionIdentityCapacity().RotationRequired() {
		s.drain.AcceptingNewSessions = false
	}
	s.mu.Unlock()

	runtimeSession, descriptor, paths, baseline, providerProxy, mcpProxy, diagnosticFilter, createErr := s.createSession(r.Context(), request, now, uid, gid)
	if createErr != nil {
		// The allocated UID/GID is permanently consumed (identities are never
		// reused). Tombstone the failed create so a replay of the same request
		// is classified as a duplicate instead of allocating another identity
		// on every retry and exhausting the pool's identity range; a genuine
		// new attempt advances the session generation and is not blocked.
		statusCode := http.StatusInternalServerError
		code := harnessv2.ErrorCodeSessionPoisoned
		retryable := true
		message := safeError(createErr)
		var replay *failedCreateReplay
		if isSessionCreationResumeLost(createErr) {
			statusCode = http.StatusConflict
			code = harnessv2.ErrorCodeWorkspaceResumeLost
			retryable = false
			replay = &failedCreateReplay{
				operationID: request.Metadata.OperationID, requestDigest: request.Metadata.RequestDigest,
				statusCode: statusCode, code: code, message: message, retryable: retryable,
			}
		}
		s.mu.Lock()
		s.tombstoneFailedCreateLocked(request.RuntimeSessionID, request.Metadata, now, replay)
		s.mu.Unlock()
		slog.Error("ACP runtime session creation failed", "stage", sessionCreationStage(createErr))
		writeError(w, statusCode, code, message, nil, retryable)
		return
	}
	s.mu.Lock()
	state.runtime = runtimeSession
	state.promptMutations = runtimeSession
	state.descriptor = descriptor
	state.paths = paths
	state.baseline = baseline
	state.workspaceIntent = request.Workspace.Intent
	state.workspaceRelativeRoot = strings.TrimSpace(request.Workspace.RelativeRoot)
	state.providerProxy = providerProxy
	state.mcpProxy = mcpProxy
	state.profile = request.Profile
	if request.AgentConfiguration == nil {
		state.agentConfiguration.MaxTurns = defaultProviderProxyMaxTurns
	} else {
		state.agentConfiguration = *request.AgentConfiguration
	}
	state.agentDiagnosticFilter = diagnosticFilter
	state.creating = false
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	drainCleanup := s.drain.Requested && !state.drainCleanupScheduled && isDrainCleanupState(state)
	if drainCleanup {
		state.drainCleanupScheduled = true
	}
	s.mu.Unlock()
	if drainCleanup {
		go s.cleanupDrainedSession(request.RuntimeSessionID, state)
	}
	writeJSON(w, http.StatusCreated, harnessv2.CreateRuntimeSessionResponse{
		Protocol:       harnessv2.ProtocolVersion,
		Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		Session:        descriptor,
	})
}

func (s *Server) createSession(
	ctx context.Context,
	request harnessv2.CreateRuntimeSessionRequest,
	now time.Time,
	uid int,
	gid int,
) (*acp.RuntimeSession, harnessv2.RuntimeSessionDescriptor, acp.SessionPaths, *workspacedelta.Snapshot, *providerProxySession, *mcpProxySession, *AgentDiagnosticFilter, error) {
	pathID := sessionPathID(request.Metadata.Fence.RuntimeSessionUID, request.Metadata.Fence.RuntimeSessionGeneration)
	paths, err := acp.PrepareSessionPaths(s.cfg.SessionBaseDir, pathID)
	if err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("path preparation", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(paths.Root)
		}
	}()
	// Under a durable workspace root, the repository workspace of one logical
	// session lives on the provider's durable data volume so a data-only cold
	// suspension preserves exactly it; every other session path stays in the
	// ephemeral tree that dies with this process. Committed content resumes
	// without re-materialization, and the recorded repository binding must
	// match the declared baseline so continuation never silently switches
	// source content.
	materialize := true
	resumedFromCheckpoint := false
	if request.Workspace.ExpectDurableResume && s.cfg.DurableWorkspaceDir == "" {
		// The controller asserts this session resumes a committed durable
		// checkpoint; a runtime without a durable root cannot possibly hold
		// it and must fail closed instead of running on a fresh tree.
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationResumeLost(
			errors.New("controller expects a committed durable checkpoint, but this runtime has no durable workspace root"))
	}
	if s.cfg.DurableWorkspaceDir != "" {
		sessionComponent := string(request.Metadata.Fence.RuntimeSessionUID)
		sessionIdentityHighWater := s.cfg.UIDAllocator.Capacity() - s.cfg.UIDAllocator.Remaining()
		workspaceDir, committed, durableErr := acp.PrepareDurableSessionWorkspace(
			s.cfg.DurableWorkspaceDir, sessionComponent, sessionIdentityHighWater,
		)
		if durableErr != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil,
				durableWorkspacePreparationFailed(request.Workspace.ExpectDurableResume, durableErr)
		}
		if request.Workspace.ExpectDurableResume && committed == nil {
			// The provider returned an empty or replacement volume after
			// snapshot loss: silently materializing the verified baseline
			// would let the continuation run cleanly while every
			// checkpoint-only change has vanished. One authorized exception
			// exists: a repository-identity transition staged its record
			// durably before wiping the old checkpoint, and a transient
			// failure before the recommit leaves exactly this shape - the
			// retry may materialize the SAME staged target fresh.
			transition, transitionErr := acp.DurableWorkspaceTransitionTarget(s.cfg.DurableWorkspaceDir, sessionComponent)
			if transitionErr != nil {
				return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace transition record", transitionErr)
			}
			if transition == nil || !acp.SameDurableWorkspaceIdentity(
				acp.StableDurableWorkspaceIdentity(transition.RepositoryIdentity, transition.Revision),
				acp.StableDurableWorkspaceIdentity(request.Workspace.Baseline.RepositoryIdentity, request.Workspace.Baseline.Revision),
			) {
				return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationResumeLost(
					errors.New("controller expects a committed durable checkpoint for this session, but none exists on the durable volume"))
			}
			if transition.SessionGeneration < request.Workspace.ExpectDurableResumeMinGeneration {
				return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationResumeLost(
					fmt.Errorf(
						"authorized durable transition records session generation %d, older than the controller's floor %d; a stale snapshot restore is refused",
						transition.SessionGeneration, request.Workspace.ExpectDurableResumeMinGeneration))
			}
		}
		paths.Workspace = workspaceDir
		if committed != nil {
			// Continuity is judged on the stable session-level identity, not
			// the raw Task-scoped baseline: a no-repository continuation
			// carries a fresh Task UID in the protocol identity, and a
			// verified publication legitimately advances the revision the
			// controller validated before requesting this session.
			if request.Workspace.ExpectDurableResume &&
				committed.SessionGeneration < request.Workspace.ExpectDurableResumeMinGeneration {
				// Same volume, valid marker, OLDER recorded generation: the
				// provider restored a stale data snapshot of this repository.
				// Diffing it against the newest verified baseline would let
				// the next publication silently drop or revert newer
				// checkpoint-only edits; fail closed instead.
				return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationResumeLost(
					fmt.Errorf(
						"committed durable checkpoint records session generation %d, older than the controller's floor %d; a stale snapshot restore is refused",
						committed.SessionGeneration, request.Workspace.ExpectDurableResumeMinGeneration))
			}
			if acp.SameDurableWorkspaceIdentity(
				acp.StableDurableWorkspaceIdentity(committed.RepositoryIdentity, committed.Revision),
				acp.StableDurableWorkspaceIdentity(request.Workspace.Baseline.RepositoryIdentity, request.Workspace.Baseline.Revision),
			) {
				// The preserved tree still carries the previous session
				// child's ownership and 0700 modes; without DAC_OVERRIDE the
				// supervisor cannot capture the resumed baseline until the
				// tree is reclaimed. Finalization below reassigns it to this
				// session's fresh child identity.
				if err := acp.ReclaimSessionOwnership(paths.Workspace); err != nil {
					return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace ownership reclaim", err)
				}
				materialize = false
				resumedFromCheckpoint = true
			} else {
				if request.Workspace.ExpectDurableResume {
					if !durableResumeTransitionAuthorized(
						committed.RepositoryIdentity, committed.Revision, request.Workspace.ExpectDurableResumeFrom,
					) {
						// The controller asserts continuity with this session's
						// preserved lineage, but the committed checkpoint binds a
						// DIFFERENT repository identity than both the resumed
						// lineage and any controller-authorized prior identity:
						// the provider restored a wrong or stale snapshot.
						// Wiping it would silently destroy someone's preserved
						// data and run the continuation on a clean baseline;
						// fail closed instead.
						return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationResumeLost(
							errors.New("committed durable checkpoint binds a different repository identity than the resumed lineage; refusing to wipe it"))
					}
					// The checkpoint binds exactly the controller-asserted
					// PRIOR identity of a verified publication transition:
					// the wipe below re-materializes from the authenticated
					// new baseline rather than poisoning the continuation.
				}
				// A verified publication transition can move the session to a
				// new repository identity (for example the fork a PR
				// publishes to); the authenticated controller validated that
				// transition before requesting this session, so the stale
				// durable tree is wiped and the workspace re-materializes
				// from the newly declared baseline instead of poisoning the
				// continuation. The authorization is staged DURABLY before
				// the wipe: a transient failure between this wipe and the
				// commit would otherwise leave a resumed lineage with no
				// committed marker, and the retry would fail closed forever.
				if err := acp.MarkDurableWorkspaceTransitionAuthorized(
					s.cfg.DurableWorkspaceDir, sessionComponent,
					acp.DurableWorkspaceBinding{
						RepositoryIdentity: request.Workspace.Baseline.RepositoryIdentity,
						Revision:           request.Workspace.Baseline.Revision,
						SessionIdentityHighWater: s.cfg.UIDAllocator.Capacity() -
							s.cfg.UIDAllocator.Remaining(),
						SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
					},
				); err != nil {
					return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace transition staging", err)
				}
				if err := acp.WipeDurableSessionWorkspace(s.cfg.DurableWorkspaceDir, sessionComponent); err != nil {
					return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace transition", err)
				}
				if workspaceDir, _, durableErr = acp.PrepareDurableSessionWorkspace(
					s.cfg.DurableWorkspaceDir, sessionComponent, sessionIdentityHighWater,
				); durableErr != nil {
					return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil,
						durableWorkspacePreparationFailed(request.Workspace.ExpectDurableResume, durableErr)
				}
				paths.Workspace = workspaceDir
			}
		}
	}
	if materialize {
		if err := s.cfg.WorkspaceMaterializer.Materialize(ctx, request, paths.Workspace); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("workspace materialization", err)
		}
	}
	var baseline *workspacedelta.Snapshot
	if resumedFromCheckpoint {
		// The preserved checkpoint may carry unpublished pre-suspension
		// edits (a failed or cancelled Task detaches with Suspend without
		// publishing its delta). Capturing the checkpoint tree as the
		// baseline would silently drop those edits from the next
		// publication, so the baseline is reconstructed from the
		// controller-verified repository baseline instead: the next delta
		// then expresses everything not yet published, pre-suspension edits
		// included.
		baselineDir := filepath.Join(paths.Root, "baseline-reconstruction")
		if err = os.MkdirAll(baselineDir, 0o700); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("baseline reconstruction root", err)
		}
		if err = s.cfg.WorkspaceMaterializer.Materialize(ctx, request, baselineDir); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("baseline reconstruction materialization", err)
		}
		baseline, err = workspacedelta.CaptureContext(ctx, baselineDir, s.baselineCaptureOptions())
		if err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("baseline reconstruction capture", err)
		}
		if err = os.RemoveAll(baselineDir); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("baseline reconstruction cleanup", err)
		}
	} else {
		baseline, err = workspacedelta.CaptureContext(ctx, paths.Workspace, s.baselineCaptureOptions())
		if err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("workspace baseline capture", err)
		}
	}
	if s.cfg.Provider.PrepareSession != nil {
		if err := s.cfg.Provider.PrepareSession(paths); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider home preparation", err)
		}
	}
	providerProxy, proxyBinding, err := s.providerProxy.newSession()
	if err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider proxy setup", err)
	}
	cleanupProviderProxy := true
	defer func() {
		if cleanupProviderProxy {
			providerProxy.close()
		}
	}()
	mcpProxy, mcpServer, err := s.mcpProxy.newSession(request.Metadata.Fence, request.MCPConfiguration)
	if err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("MCP proxy setup", err)
	}
	cleanupMCPProxy := true
	defer func() {
		if cleanupMCPProxy {
			mcpProxy.close()
		}
	}()
	projection := ProviderSessionProjection{}
	if s.cfg.Provider.ProjectSession != nil {
		projection, err = s.cfg.Provider.ProjectSession(request, paths, proxyBinding)
		if err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider session projection", err)
		}
	}
	envValues := cloneStringMap(s.cfg.Provider.Environment)
	if s.cfg.Provider.EnvironmentForSession != nil {
		values, envErr := s.cfg.Provider.EnvironmentForSession(request, paths, proxyBinding)
		if envErr != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider environment setup", envErr)
		}
		maps.Copy(envValues, values)
	}
	maps.Copy(envValues, projection.Environment)
	if err := acp.FinalizeSessionOwnership(paths.Root, uid, gid); err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("ownership finalization", err)
	}
	if s.cfg.DurableWorkspaceDir != "" {
		// The durable workspace lives outside the session root, and each cold
		// resume allocates a fresh non-reused child identity, so the preserved
		// tree is re-assigned to exactly this session's UID.
		if err := acp.FinalizeSessionOwnership(paths.Workspace, uid, gid); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace ownership finalization", err)
		}
	}
	environment, err := acp.BuildChildEnvironment(paths, acp.EnvironmentConfig{Values: envValues})
	if err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider environment setup", err)
	}
	if resumedFromCheckpoint {
		// Invalidate the committed marker only NOW, immediately before the
		// provider child (the only writer of the durable tree) can spawn: a
		// failure in any earlier stage - baseline reconstruction, provider
		// home preparation, proxy setup - leaves the committed marker intact
		// so the untouched checkpoint is reused on retry instead of being
		// wiped as a partial session. A failure after this point wipes, as
		// it must: the child may have modified the repository. The
		// successful commit below restores the marker.
		if err := acp.MarkDurableSessionWorkspaceResumePending(
			s.cfg.DurableWorkspaceDir, string(request.Metadata.Fence.RuntimeSessionUID),
		); err != nil {
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace pending mark", err)
		}
	}
	runtimeSession, err := acp.NewRuntimeSession(ctx, acp.RuntimeSessionConfig{
		ID:            string(request.RuntimeSessionID),
		Generation:    int64(request.Metadata.Fence.RuntimeSessionGeneration),
		ProfileDigest: string(request.Metadata.Fence.RuntimeProfileDigest),
		Process: acp.ProcessConfig{
			Command:       s.cfg.Provider.Command,
			Args:          append(append([]string(nil), s.cfg.Provider.Args...), projection.AdditionalArgs...),
			Environment:   environment,
			Paths:         paths,
			UID:           uid,
			GID:           gid,
			ClientOptions: acp.Options{MaxMessageBytes: s.cfg.Capabilities.Limits.MaxRequestBytes},
		},
		MCPServers:            []acp.MCPServer{mcpServer},
		NewSessionMeta:        projection.NewSessionMeta,
		AuthMethodID:          s.cfg.Provider.AuthMethodID,
		InitializeTimeout:     defaultDuration(s.cfg.InitializeTimeout, acp.DefaultInitializeTimeout),
		PromptLease:           time.Duration(s.cfg.Capabilities.Limits.MaxPromptLeaseMillis) * time.Millisecond,
		PermissionTimeout:     defaultDuration(s.cfg.PermissionTimeout, acp.DefaultPermissionTimeout),
		CancelGrace:           defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace),
		MaxBufferedEvents:     s.cfg.Capabilities.Limits.MaxBufferedEvents,
		MaxBufferedEventBytes: supervisorMaxBufferedPromptEventBytes,
	})
	if err != nil {
		return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("provider adapter initialization", err)
	}
	if s.cfg.DurableWorkspaceDir != "" {
		// The marker commits only after the session is fully initialized: a
		// creation that fails part-way leaves its binding pending, so the next
		// creation wipes the uncommitted durable tree and
		// materializes clean instead of reusing state a failed create — or a
		// partially started provider — may have modified. A resume recommits
		// here, retiring its pending record.
		if commitErr := acp.CommitDurableSessionWorkspace(
			s.cfg.DurableWorkspaceDir,
			string(request.Metadata.Fence.RuntimeSessionUID),
			acp.DurableWorkspaceBinding{
				RepositoryIdentity: request.Workspace.Baseline.RepositoryIdentity,
				Revision:           request.Workspace.Baseline.Revision,
				SessionIdentityHighWater: s.cfg.UIDAllocator.Capacity() -
					s.cfg.UIDAllocator.Remaining(),
				SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
			},
		); commitErr != nil {
			// The credential-bearing child is already running; its removal
			// must be PROVEN before this creation is abandoned, or the
			// surviving descendant would be untracked by any session and
			// later pool lifecycle decisions would proceed over it.
			cleanupResult, deleteErr := runtimeSession.Delete(ctx)
			if deleteErr != nil || !cleanupResult.Proven {
				s.poisonPool("durable_commit_session_cleanup_unproven")
			}
			return nil, harnessv2.RuntimeSessionDescriptor{}, acp.SessionPaths{}, nil, nil, nil, nil, sessionCreationFailed("durable workspace commit", commitErr)
		}
	}
	cleanup = false
	cleanupProviderProxy = false
	cleanupMCPProxy = false
	descriptor := harnessv2.RuntimeSessionDescriptor{
		RuntimeSessionID:     request.RuntimeSessionID,
		RuntimeSessionUID:    request.Metadata.Fence.RuntimeSessionUID,
		Generation:           request.Metadata.Fence.RuntimeSessionGeneration,
		RuntimeInstanceID:    s.cfg.Fence.RuntimeInstanceID,
		SupervisorBootID:     s.cfg.Fence.SupervisorBootID,
		RuntimeProfileDigest: s.cfg.Fence.RuntimeProfileDigest,
		State:                harnessv2.RuntimeSessionStateIdle,
		ProviderSessionID:    runtimeSession.ProviderSessionID(),
		WorkspaceBaseline:    request.Workspace.Baseline,
		CreatedAt:            now,
		LastTransitionAt:     now,
	}
	return runtimeSession, descriptor, paths, baseline, providerProxy, mcpProxy, projection.AgentDiagnosticFilter, nil
}

func (s *Server) authorizeController(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.bearerMatches(r.Header.Get("Authorization")) {
		writeError(w, http.StatusUnauthorized, harnessv2.ErrorCodeUnauthenticated, "controller authentication failed", nil, false)
		return false
	}
	return true
}

func (s *Server) authorizeMutation(w http.ResponseWriter, r *http.Request, metadata harnessv2.MutationMetadata, requireSession bool) bool {
	if !s.authorizeController(w, r) {
		return false
	}
	if !s.cfg.RequireCapabilities {
		return true
	}
	if err := harnessv2.VerifyOperationCapability(s.cfg.CapabilitySecret, r.Header.Get(OperationCapabilityHeader), metadata, requireSession, time.Now().UTC()); err != nil {
		writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "operation capability authorization failed", nil, false)
		return false
	}
	return true
}

func (s *Server) expectedFence(sessionUID harnessv2.RuntimeSessionUID, generation uint64) harnessv2.Fence {
	fence := s.cfg.Fence
	fence.RuntimeSessionUID = sessionUID
	fence.RuntimeSessionGeneration = generation
	return fence
}

func (s *Server) decodeMutation(w http.ResponseWriter, r *http.Request, target any, requireSession bool, _ harnessv2.MutationMetadata) bool {
	_ = requireSession
	return s.decodeAuthenticatedJSON(w, r, target)
}

// decodeAuthenticatedJSON verifies the controller bearer token before reading
// the request body. An untrusted ACP child shares the Pod network namespace
// and can reach this listener, so unauthenticated peers must be rejected on
// headers alone instead of being allowed to drip-feed mutation bodies. The
// operation-capability check still runs after decoding because it needs the
// mutation metadata from the body.
func (s *Server) decodeAuthenticatedJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if !s.authorizeController(w, r) {
		return false
	}
	return decodeJSON(w, r, s.cfg.Capabilities.Limits.MaxRequestBytes, target)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int, target any) bool {
	body := http.MaxBytesReader(w, r.Body, int64(limit))
	defer body.Close() //nolint:errcheck
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "invalid JSON request", nil, false)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "request contains trailing or invalid JSON", nil, false)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code harnessv2.ErrorCode, message string, classification *harnessv2.Classification, retryable bool) {
	writeJSON(w, status, harnessv2.ErrorResponse{
		Protocol: harnessv2.ProtocolVersion, Code: code, Message: message, Classification: classification, Retryable: retryable,
	})
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
	writeError(w, status, code, string(classification.Class), &classification, false)
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

func sessionPathID(uid harnessv2.RuntimeSessionUID, generation uint64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%d", uid, generation))
	return "session-" + hex.EncodeToString(sum[:])
}

func cloneStringMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	maps.Copy(result, input)
	return result
}

func defaultDuration(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func safeError(err error) string {
	if creation, ok := errors.AsType[*sessionCreationError](err); ok {
		return "runtime session failed during " + creation.stage
	}
	return "runtime operation failed; consult bounded supervisor diagnostics"
}

func liveDescendantCount(runtime *acp.RuntimeSession) uint32 {
	if runtime == nil || runtime.Process() == nil || runtime.Process().PID() <= 0 {
		return 0
	}
	select {
	case <-runtime.Process().Done():
		return 0
	default:
		return 1
	}
}

func totalLiveDescendants(sessions []harnessv2.RuntimeSessionStatus) uint32 {
	var total uint32
	for _, session := range sessions {
		total += session.LiveDescendantCount
	}
	return total
}

func (s *Server) sessionIdentityCapacity() *harnessv2.SessionIdentityCapacity {
	return &harnessv2.SessionIdentityCapacity{
		Total:             uint64(s.cfg.UIDAllocator.Capacity()),
		Remaining:         uint64(s.cfg.UIDAllocator.Remaining()),
		ExhaustionReserve: sessionIdentityExhaustionReserve,
	}
}

func (s *Server) beginDrainLocked(reason string, now time.Time) []drainCleanupCandidate {
	if !s.drain.Requested {
		s.drain = harnessv2.DrainStatus{
			AcceptingNewSessions: false,
			Requested:            true,
			RequestedAt:          now,
			Reason:               reason,
		}
		s.lifecycle = harnessv2.SupervisorLifecycleDraining
	}
	cleanup := make([]drainCleanupCandidate, 0, len(s.sessions))
	for id, state := range s.sessions {
		if state.creating || state.drainCleanupScheduled || !isDrainCleanupState(state) {
			continue
		}
		state.drainCleanupScheduled = true
		cleanup = append(cleanup, drainCleanupCandidate{id: id, state: state})
	}
	return cleanup
}

func (s *Server) startDrainCleanup(cleanup []drainCleanupCandidate) {
	for _, candidate := range cleanup {
		go s.cleanupDrainedSession(candidate.id, candidate.state)
	}
}

func isDrainCleanupState(state *sessionState) bool {
	if state == nil {
		return false
	}
	switch state.descriptor.State {
	case harnessv2.RuntimeSessionStateIdle, harnessv2.RuntimeSessionStatePoisoned, harnessv2.RuntimeSessionStatePublicationPrepared:
		return true
	case harnessv2.RuntimeSessionStateFinalizing:
		return state.publicationFinalization != nil
	default:
		return false
	}
}

func (s *Server) cleanupDrainedSession(sessionID harnessv2.RuntimeSessionID, state *sessionState) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.mu.Lock()
	if s.sessions[sessionID] != state || !state.drainCleanupScheduled {
		s.mu.Unlock()
		return
	}
	settled := state.prompt == nil || state.prompt.settlement != nil
	if state.creating || state.runtime == nil || !settled || !isDrainCleanupState(state) {
		state.drainCleanupScheduled = false
		s.mu.Unlock()
		return
	}
	state.descriptor.State = harnessv2.RuntimeSessionStateDeleting
	state.descriptor.LastTransitionAt = time.Now().UTC()
	s.mu.Unlock()
	if state.mcpProxy != nil {
		state.mcpProxy.close()
	}
	var proxyErr error
	if state.providerProxy != nil {
		state.providerProxy.close()
		proxyErr = state.providerProxy.wait(ctx)
	}
	cleanup, deleteErr := state.runtime.Delete(ctx)
	if proxyErr != nil || deleteErr != nil || !cleanup.Proven {
		s.poisonPool("drain_session_cleanup_unproven")
		return
	}
	if err := acp.ReclaimSessionOwnership(state.paths.Root); err != nil {
		slog.Error("ACP drained runtime session cleanup failed", "stage", "ownership reclaim")
		s.poisonPool("drain_session_root_ownership_reclaim_unproven")
		return
	}
	if err := os.RemoveAll(state.paths.Root); err != nil {
		s.poisonPool("drain_session_root_cleanup_unproven")
		return
	}
	deletedAt := time.Now().UTC()
	s.mu.Lock()
	if s.sessions[sessionID] == state {
		s.tombstoneSessionLocked(state, deletedAt)
	}
	s.mu.Unlock()
}

// tombstoneSessionLocked preserves replay and terminal proof after either
// explicit deletion or automatic cleanup. The caller must hold s.mu.
func (s *Server) tombstoneSessionLocked(state *sessionState, deletedAt time.Time) sessionTombstone {
	pruneSessionOperationsLocked(state, deletedAt)
	operations := make([]harnessv2.OperationRecord, 0, len(state.operations))
	for _, operation := range state.operations {
		operations = append(operations, operation)
	}
	tombstone := sessionTombstone{
		RuntimeSessionTombstone: harnessv2.RuntimeSessionTombstone{
			RuntimeSessionUID: state.descriptor.RuntimeSessionUID, RuntimeSessionGeneration: state.descriptor.Generation,
			RuntimeProfileDigest: state.descriptor.RuntimeProfileDigest, DeletedAt: deletedAt, Operations: operations,
		},
		sessionID:              state.id,
		cancellations:          make(map[harnessv2.OperationID]*operationReplay),
		cancellationOperations: make(map[harnessv2.OperationID]harnessv2.OperationRecord),
	}
	if state.prompt != nil && state.prompt.settlement != nil && state.prompt.settlement.Validate() == nil {
		tombstone.prompt = &retiredPromptSettlement{metadata: state.prompt.request.Metadata, settlement: *state.prompt.settlement}
	}
	for operationID, replay := range state.operationReplays {
		if replay.isCancellation {
			// Keep the shared replay until its owner finishes, even when cleanup
			// overtakes an in-flight cancellation handler.
			tombstone.cancellations[operationID] = replay
		}
	}
	delete(s.sessions, state.id)
	s.pruneTombstonesLocked(deletedAt)
	delete(s.failedCreates, tombstone.RuntimeSessionUID)
	s.tombstones[tombstone.RuntimeSessionUID] = tombstone
	return tombstone
}

// Close stops admission and tears down all resident runtime sessions. A cleanup
// failure is returned so the caller can poison/restart the enclosing RuntimePool.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.lifecycle = harnessv2.SupervisorLifecycleTerminating
	s.drain.AcceptingNewSessions = false
	sessions := make([]*sessionState, 0, len(s.sessions))
	for _, state := range s.sessions {
		if state.runtime != nil || state.providerProxy != nil || state.mcpProxy != nil {
			sessions = append(sessions, state)
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, state := range sessions {
		if state.mcpProxy != nil {
			state.mcpProxy.close()
		}
		if state.providerProxy != nil {
			state.providerProxy.close()
			if err := state.providerProxy.wait(ctx); err != nil {
				errs = append(errs, fmt.Errorf("provider proxy session cleanup: %w", err))
			}
		}
		if state.runtime != nil {
			cleanup, err := state.runtime.Delete(ctx)
			if err != nil || !cleanup.Proven {
				errs = append(errs, fmt.Errorf("runtime session cleanup unproven: %w", err))
			} else if err := acp.ReclaimSessionOwnership(state.paths.Root); err != nil {
				errs = append(errs, fmt.Errorf("runtime session filesystem ownership reclaim: %w", err))
			} else if err := os.RemoveAll(state.paths.Root); err != nil {
				errs = append(errs, fmt.Errorf("runtime session filesystem cleanup: %w", err))
			}
		}
	}
	if err := s.providerProxy.close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("provider proxy shutdown: %w", err))
	}
	if err := s.mcpProxy.close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("MCP proxy shutdown: %w", err))
	}
	if s.identityLock != nil {
		if err := s.identityLock.Close(); err != nil {
			errs = append(errs, fmt.Errorf("session identity lock shutdown: %w", err))
		}
		s.identityLock = nil
	}
	return errors.Join(errs...)
}

func (s *Server) poisonPool(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.sessions {
		if state.providerProxy != nil {
			state.providerProxy.revoke()
		}
		if state.mcpProxy != nil {
			state.mcpProxy.revoke(harnessv2.RuntimeSessionStatePoisoned)
		}
	}
	s.lifecycle = harnessv2.SupervisorLifecycleUnhealthy
	s.drain = harnessv2.DrainStatus{AcceptingNewSessions: false, Requested: true, RequestedAt: time.Now().UTC(), Reason: reason}
}

func (s *Server) BeginDrain(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drain.Requested {
		return
	}
	s.drain = harnessv2.DrainStatus{
		AcceptingNewSessions: false,
		Requested:            true,
		RequestedAt:          time.Now().UTC(),
		Reason:               reason,
	}
	s.lifecycle = harnessv2.SupervisorLifecycleDraining
}

// durableResumeTransitionAuthorized reports whether a committed durable
// checkpoint that mismatches the resumed lineage's declared baseline may be
// wiped and re-materialized: only when the controller asserted the exact
// PRIOR repository identity of a verified publication transition and the
// checkpoint binds precisely that identity.
func durableResumeTransitionAuthorized(committedIdentity, committedRevision, expectedPrior string) bool {
	expectedPrior = strings.TrimSpace(expectedPrior)
	if expectedPrior == "" {
		return false
	}
	return acp.SameDurableWorkspaceIdentity(
		acp.StableDurableWorkspaceIdentity(committedIdentity, committedRevision),
		expectedPrior,
	)
}
