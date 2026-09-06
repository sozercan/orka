package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	DefaultPromptLease        = 2 * time.Minute
	DefaultPermissionTimeout  = 5 * time.Minute
	DefaultBufferedEvents     = 256
	DefaultBufferedEventBytes = 32 << 20
	DefaultInitializeTimeout  = 60 * time.Second
)

type RuntimeSessionConfig struct {
	ID             string
	Generation     int64
	ProfileDigest  string
	Process        ProcessConfig
	MCPServers     []MCPServer
	NewSessionMeta Meta
	AuthMethodID   string
	ClientInfo     Implementation

	InitializeTimeout time.Duration
	PromptLease       time.Duration
	PermissionTimeout time.Duration
	CancelGrace       time.Duration
	MaxBufferedEvents int
	// MaxBufferedEventBytes bounds the aggregate size of buffered, not yet
	// consumed prompt events (measured as the raw notification payload) so a
	// burst of large valid events cannot exhaust the runtime's memory before
	// the event-count limit fires. Consumers release bytes through
	// PromptRun.Release.
	MaxBufferedEventBytes int
}

type RuntimeSession struct {
	id                string
	generation        int64
	profileDigest     string
	providerSessionID string
	process           *Process
	config            RuntimeSessionConfig

	mu           sync.Mutex
	active       *activePrompt
	tombstones   map[string]PromptTombstone
	deleted      bool
	deleteDone   chan struct{}
	deleteStatus CleanupStatus
	deleteErr    error
}

type PromptEventType string

const (
	PromptEventAccepted            PromptEventType = "accepted"
	PromptEventUpdate              PromptEventType = "update"
	PromptEventPermissionRequested PromptEventType = "permission_requested"
)

type PromptEvent struct {
	Type     PromptEventType
	Sequence int64
	// Timestamp is assigned when the event is enqueued for the consumer.
	Timestamp time.Time
	// ReceivedAt is when the session received the notification from the
	// child, stamped before any pre-acceptance buffering, so it preserves
	// the phase the child emitted the event in even when the event is
	// enqueued later.
	ReceivedAt time.Time
	Update     *SessionNotification
	Permission *PermissionRequestEvent
	// Size is the raw notification payload size counted against the prompt's
	// buffered-bytes budget until the consumer releases the event.
	Size int
}

type PermissionRequestEvent struct {
	RequestID string
	Request   RequestPermissionRequest
}

type PromptOutcome string

const (
	PromptOutcomeCompleted      PromptOutcome = "completed"
	PromptOutcomeCancelled      PromptOutcome = "cancelled"
	PromptOutcomeFailed         PromptOutcome = "failed"
	PromptOutcomeOutcomeUnknown PromptOutcome = "outcome_unknown"
)

type PromptResult struct {
	Outcome    PromptOutcome
	StopReason StopReason
	Err        error
	Accepted   bool
	SettledAt  time.Time
}

type PromptRun struct {
	Events <-chan PromptEvent
	// Release returns an event's bytes to the buffered-bytes budget; call it
	// as soon as the event has been received from Events.
	Release func(PromptEvent)
	Result  <-chan PromptResult
}

type PromptTombstone struct {
	PromptID      string
	RequestDigest string
	Result        PromptResult
}

type DuplicatePromptError struct {
	PromptID string
	Active   bool
	Result   *PromptResult
}

func (e *DuplicatePromptError) Error() string {
	if e.Active {
		return fmt.Sprintf("ACP prompt %s was already accepted and remains active", e.PromptID)
	}
	return fmt.Sprintf("ACP prompt %s already settled", e.PromptID)
}

type DigestConflictError struct{ PromptID string }

func (e *DigestConflictError) Error() string {
	return fmt.Sprintf("ACP prompt %s identity was reused with a different request digest", e.PromptID)
}

type StalePromptError struct{ PromptID string }

func (e *StalePromptError) Error() string {
	return fmt.Sprintf("ACP prompt %s is no longer active", e.PromptID)
}

type activePrompt struct {
	id              string
	requestDigest   string
	request         PromptRequest
	events          chan PromptEvent
	result          chan PromptResult
	done            chan struct{}
	seq             int64
	accepted        bool
	settled         bool
	overflowed      bool
	bufferedBytes   int
	cancelRequested bool
	lease           *time.Timer
	permissions     map[string]*pendingPermission
	preAccepted     []PromptEvent
}

type pendingPermission struct {
	options map[string]struct{}
	result  chan RequestPermissionOutcome
}

func NewRuntimeSession(ctx context.Context, cfg RuntimeSessionConfig) (*RuntimeSession, error) {
	cfg.ID = strings.TrimSpace(cfg.ID)
	cfg.ProfileDigest = strings.TrimSpace(cfg.ProfileDigest)
	if cfg.ID == "" || cfg.Generation <= 0 || cfg.ProfileDigest == "" {
		return nil, fmt.Errorf("runtime session ID, positive generation, and profile digest are required")
	}
	newSessionMeta, err := MergeNewSessionMeta(cfg.NewSessionMeta, Meta{
		sessionMetaRuntimeSessionID:     cfg.ID,
		sessionMetaGeneration:           cfg.Generation,
		sessionMetaRuntimeProfileDigest: cfg.ProfileDigest,
	})
	if err != nil {
		return nil, err
	}
	if cfg.InitializeTimeout <= 0 {
		cfg.InitializeTimeout = DefaultInitializeTimeout
	}
	if cfg.PromptLease <= 0 {
		cfg.PromptLease = DefaultPromptLease
	}
	if cfg.PermissionTimeout <= 0 {
		cfg.PermissionTimeout = DefaultPermissionTimeout
	}
	if cfg.CancelGrace <= 0 {
		cfg.CancelGrace = DefaultStopGrace
	}
	if cfg.MaxBufferedEvents <= 0 {
		cfg.MaxBufferedEvents = DefaultBufferedEvents
	}
	if cfg.MaxBufferedEventBytes <= 0 {
		cfg.MaxBufferedEventBytes = DefaultBufferedEventBytes
	}
	session := &RuntimeSession{
		id:            cfg.ID,
		generation:    cfg.Generation,
		profileDigest: cfg.ProfileDigest,
		config:        cfg,
		tombstones:    make(map[string]PromptTombstone),
	}
	cfg.Process.ClientOptions.RequestHandler = session.handleRequest
	cfg.Process.ClientOptions.NotificationHandler = session.handleNotification
	process, err := StartProcess(cfg.Process)
	if err != nil {
		return nil, err
	}
	session.process = process

	initCtx, cancel := context.WithTimeout(ctx, cfg.InitializeTimeout)
	defer cancel()
	clientInfo := cfg.ClientInfo
	if clientInfo.Name == "" {
		clientInfo = Implementation{Name: "orka-acp-runtime", Version: "development"}
	}
	initialized, err := process.Client().Initialize(initCtx, InitializeRequest{
		ProtocolVersion: ProtocolVersion,
		ClientInfo:      &clientInfo,
		ClientCapabilities: ClientCapabilities{
			FS:       FileSystemCapabilities{},
			Terminal: false,
		},
	})
	if err != nil {
		_ = stopProcessBestEffort(process, cfg.CancelGrace)
		return nil, fmt.Errorf("initialize ACP adapter: %w", err)
	}
	if len(cfg.MCPServers) > 0 && !acpMCPCapabilityEnabled(initialized.AgentCapabilities.MCPCapabilities, "http") {
		_ = stopProcessBestEffort(process, cfg.CancelGrace)
		return nil, fmt.Errorf("ACP adapter did not advertise HTTP MCP server support")
	}
	if cfg.AuthMethodID != "" {
		if !containsAuthMethod(initialized.AuthMethods, cfg.AuthMethodID) {
			_ = stopProcessBestEffort(process, cfg.CancelGrace)
			return nil, fmt.Errorf("ACP adapter did not advertise authentication method %q", cfg.AuthMethodID)
		}
		if err := process.Client().Authenticate(initCtx, cfg.AuthMethodID); err != nil {
			_ = stopProcessBestEffort(process, cfg.CancelGrace)
			return nil, fmt.Errorf("authenticate ACP adapter: %w", err)
		}
	}
	newSession, err := process.Client().NewSession(initCtx, NewSessionRequest{
		CWD:        cfg.Process.Paths.Workspace,
		MCPServers: append([]MCPServer{}, cfg.MCPServers...),
		Meta:       newSessionMeta,
	})
	if err != nil {
		_ = stopProcessBestEffort(process, cfg.CancelGrace)
		return nil, fmt.Errorf("create ACP provider session: %w", err)
	}
	session.providerSessionID = newSession.SessionID
	return session, nil
}

func (s *RuntimeSession) ID() string                { return s.id }
func (s *RuntimeSession) Generation() int64         { return s.generation }
func (s *RuntimeSession) ProviderSessionID() string { return s.providerSessionID }
func (s *RuntimeSession) Process() *Process         { return s.process }

func (s *RuntimeSession) StartPrompt(ctx context.Context, promptID, requestDigest string, prompt []ContentBlock) (PromptRun, error) {
	return s.StartPromptWithLease(ctx, promptID, requestDigest, prompt, s.config.PromptLease)
}

func (s *RuntimeSession) StartPromptWithLease(ctx context.Context, promptID, requestDigest string, prompt []ContentBlock, leaseDuration time.Duration) (PromptRun, error) {
	promptID = strings.TrimSpace(promptID)
	requestDigest = strings.TrimSpace(requestDigest)
	if promptID == "" || requestDigest == "" || len(prompt) == 0 {
		return PromptRun{}, fmt.Errorf("prompt ID, request digest, and content are required")
	}
	if leaseDuration <= 0 {
		return PromptRun{}, fmt.Errorf("prompt lease duration must be positive")
	}
	s.mu.Lock()
	if s.deleted {
		s.mu.Unlock()
		return PromptRun{}, fmt.Errorf("runtime session is deleted")
	}
	if active := s.active; active != nil {
		if active.id != promptID {
			s.mu.Unlock()
			return PromptRun{}, fmt.Errorf("runtime session already has active prompt %s", active.id)
		}
		if active.requestDigest != requestDigest {
			s.mu.Unlock()
			return PromptRun{}, &DigestConflictError{PromptID: promptID}
		}
		s.mu.Unlock()
		return PromptRun{}, &DuplicatePromptError{PromptID: promptID, Active: true}
	}
	if tombstone, ok := s.tombstones[promptID]; ok {
		if tombstone.RequestDigest != requestDigest {
			s.mu.Unlock()
			return PromptRun{}, &DigestConflictError{PromptID: promptID}
		}
		result := tombstone.Result
		s.mu.Unlock()
		return PromptRun{}, &DuplicatePromptError{PromptID: promptID, Result: &result}
	}
	active := &activePrompt{
		id:            promptID,
		requestDigest: requestDigest,
		request:       PromptRequest{SessionID: s.providerSessionID, Prompt: append([]ContentBlock(nil), prompt...)},
		events:        make(chan PromptEvent, s.config.MaxBufferedEvents),
		result:        make(chan PromptResult, 1),
		done:          make(chan struct{}),
		permissions:   make(map[string]*pendingPermission),
	}
	active.lease = time.AfterFunc(leaseDuration, func() { s.expirePrompt(promptID) })
	s.active = active
	s.mu.Unlock()

	go s.runPrompt(active)
	go func() {
		select {
		case <-ctx.Done():
			cancelCtx, cancel := context.WithTimeout(context.Background(), s.config.CancelGrace*2)
			defer cancel()
			_, _ = s.CancelPrompt(cancelCtx, promptID)
		case <-active.done:
		}
	}()
	return PromptRun{Events: active.events, Result: active.result, Release: func(event PromptEvent) { s.releaseBufferedEvent(active, event) }}, nil
}

func (s *RuntimeSession) RenewPromptLease(promptID string) error {
	return s.RenewPromptLeaseFor(promptID, s.config.PromptLease)
}

func (s *RuntimeSession) RenewPromptLeaseFor(promptID string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		return fmt.Errorf("prompt lease duration must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil || s.active.id != promptID || s.active.settled {
		return &StalePromptError{PromptID: promptID}
	}
	if !s.active.lease.Stop() {
		return &StalePromptError{PromptID: promptID}
	}
	s.active.lease.Reset(leaseDuration)
	return nil
}

func (s *RuntimeSession) ResolvePermission(promptID, requestID string, outcome RequestPermissionOutcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil || s.active.id != promptID || s.active.settled {
		return &StalePromptError{PromptID: promptID}
	}
	pending, ok := s.active.permissions[requestID]
	if !ok {
		return fmt.Errorf("permission request %s is not pending", requestID)
	}
	if outcome.Outcome == permissionOutcomeSelected {
		if _, ok := pending.options[outcome.OptionID]; !ok {
			return fmt.Errorf("permission option %q was not offered", outcome.OptionID)
		}
	} else if outcome.Outcome != permissionOutcomeCancelled {
		return fmt.Errorf("unsupported permission outcome %q", outcome.Outcome)
	}
	delete(s.active.permissions, requestID)
	pending.result <- outcome
	return nil
}

func (s *RuntimeSession) CancelPrompt(ctx context.Context, promptID string) (PromptResult, error) {
	s.mu.Lock()
	active := s.active
	if active == nil || active.id != promptID || active.settled {
		tombstone, settled := s.tombstones[promptID]
		s.mu.Unlock()
		if settled {
			return tombstone.Result, nil
		}
		return PromptResult{}, &StalePromptError{PromptID: promptID}
	}
	active.cancelRequested = true
	cancelPendingPermissions(active)
	done := active.done
	s.mu.Unlock()

	// Best-effort courtesy cancel: the notification write can block when the
	// adapter stops reading stdin, and cancellation must reach the bounded
	// grace/stop escalation below regardless. A healthy adapter settles the
	// prompt (closing done); a dead or wedged transport is escalated to the
	// bounded process stop after the grace window.
	go func() {
		_ = s.process.Client().Cancel(ctx, s.providerSessionID)
	}()
	timer := time.NewTimer(s.config.CancelGrace)
	defer timer.Stop()
	select {
	case <-done:
		return s.tombstoneResult(promptID)
	case <-timer.C:
		_, _ = s.process.Stop(ctx, s.config.CancelGrace)
		select {
		case <-done:
			return s.tombstoneResult(promptID)
		case <-ctx.Done():
			return PromptResult{}, ctx.Err()
		}
	case <-ctx.Done():
		return PromptResult{}, ctx.Err()
	}
}

func (s *RuntimeSession) Delete(ctx context.Context) (CleanupStatus, error) {
	s.mu.Lock()
	if s.deleteDone != nil {
		done := s.deleteDone
		s.mu.Unlock()
		select {
		case <-done:
			s.mu.Lock()
			status, err := s.deleteStatus, s.deleteErr
			s.mu.Unlock()
			return status, err
		case <-ctx.Done():
			return CleanupStatus{}, ctx.Err()
		}
	}
	s.deleted = true
	s.deleteDone = make(chan struct{})
	active := s.active
	if active != nil && !active.settled {
		active.cancelRequested = true
		cancelPendingPermissions(active)
	}
	s.mu.Unlock()
	if active != nil {
		// Best-effort courtesy cancel: the notification is a blocking pipe write,
		// and a wedged adapter that stopped reading stdin would otherwise block
		// Delete forever before the bounded process stop. Adapter exit closes
		// stdin, which unblocks the write and ends the goroutine.
		go func() {
			_ = s.process.Client().Cancel(context.Background(), s.providerSessionID)
		}()
	}
	status, err := s.process.Stop(ctx, s.config.CancelGrace)
	s.mu.Lock()
	s.deleteStatus = status
	s.deleteErr = err
	close(s.deleteDone)
	s.mu.Unlock()
	return status, err
}

func (s *RuntimeSession) Tombstone(promptID string) (PromptTombstone, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.tombstones[promptID]
	return value, ok
}

func (s *RuntimeSession) runPrompt(active *activePrompt) {
	var written bool
	response, err := s.process.Client().PromptWithWritten(context.Background(), active.request, func() {
		s.mu.Lock()
		written = true
		active.accepted = true
		s.emitLocked(active, PromptEvent{Type: PromptEventAccepted})
		queued := append([]PromptEvent(nil), active.preAccepted...)
		active.preAccepted = nil
		for _, event := range queued {
			// Bytes were counted when the event was parked pre-acceptance;
			// hand them back before emitLocked counts the enqueue.
			active.bufferedBytes -= event.Size
			s.emitLocked(active, event)
		}
		s.mu.Unlock()
	})
	result := PromptResult{Accepted: written, SettledAt: time.Now().UTC()}
	switch {
	case err != nil:
		result.Outcome = classifyPromptErrorOutcome(err, written)
		result.Err = err
	case response.StopReason == StopReasonEndTurn:
		result.Outcome = PromptOutcomeCompleted
		result.StopReason = response.StopReason
	case response.StopReason == StopReasonCancelled:
		result.Outcome = PromptOutcomeCancelled
		result.StopReason = response.StopReason
	default:
		result.Outcome = PromptOutcomeFailed
		result.StopReason = response.StopReason
	}
	s.finishPrompt(active, result)
}

func classifyPromptErrorOutcome(err error, written bool) PromptOutcome {
	if !written {
		return PromptOutcomeFailed
	}
	if _, ok := errors.AsType[*RPCError](err); ok {
		// A structured JSON-RPC error proves that the adapter received and
		// conclusively rejected or failed the prompt. Only loss of the response
		// after the request write leaves the outcome ambiguous.
		return PromptOutcomeFailed
	}
	return PromptOutcomeOutcomeUnknown
}

func (s *RuntimeSession) finishPrompt(active *activePrompt, result PromptResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active.settled {
		return
	}
	active.settled = true
	if active.lease != nil {
		active.lease.Stop()
	}
	cancelPendingPermissions(active)
	if active.overflowed && result.Outcome != PromptOutcomeOutcomeUnknown {
		result.Outcome = PromptOutcomeFailed
		result.Err = ErrPromptEventBufferOverflow
	}
	s.tombstones[active.id] = PromptTombstone{PromptID: active.id, RequestDigest: active.requestDigest, Result: result}
	if s.active == active {
		s.active = nil
	}
	active.result <- result
	close(active.result)
	close(active.events)
	close(active.done)
}

func (s *RuntimeSession) handleNotification(_ context.Context, notification IncomingNotification) {
	if notification.Method != MethodSessionUpdate {
		return
	}
	var update SessionNotification
	if err := json.Unmarshal(notification.Params, &update); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	active := s.active
	if active == nil || active.settled || update.SessionID != s.providerSessionID {
		return
	}
	s.emitLocked(active, PromptEvent{Type: PromptEventUpdate, Update: &update, Size: len(notification.Params)})
}

func (s *RuntimeSession) handleRequest(ctx context.Context, request IncomingRequest) (any, *RPCError) {
	if request.Method != MethodRequestPermission {
		return nil, &RPCError{Code: -32601, Message: "client method is not supported"}
	}
	var permission RequestPermissionRequest
	if err := json.Unmarshal(request.Params, &permission); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid permission request"}
	}
	requestID := canonicalID(request.ID)
	s.mu.Lock()
	active := s.active
	if active == nil || active.settled || permission.SessionID != s.providerSessionID {
		s.mu.Unlock()
		return RequestPermissionResponse{Outcome: CancelledPermissionOutcome()}, nil
	}
	if _, exists := active.permissions[requestID]; exists {
		s.mu.Unlock()
		return nil, &RPCError{Code: -32600, Message: "duplicate permission request"}
	}
	pending := &pendingPermission{options: make(map[string]struct{}), result: make(chan RequestPermissionOutcome, 1)}
	for _, option := range permission.Options {
		pending.options[option.OptionID] = struct{}{}
	}
	active.permissions[requestID] = pending
	s.emitLocked(active, PromptEvent{Type: PromptEventPermissionRequested, Permission: &PermissionRequestEvent{RequestID: requestID, Request: permission}, Size: len(request.Params)})
	done := active.done
	s.mu.Unlock()

	timer := time.NewTimer(s.config.PermissionTimeout)
	defer timer.Stop()
	select {
	case outcome := <-pending.result:
		return RequestPermissionResponse{Outcome: outcome}, nil
	case <-done:
		return RequestPermissionResponse{Outcome: CancelledPermissionOutcome()}, nil
	case <-timer.C:
		s.removePermission(requestID, pending)
		return RequestPermissionResponse{Outcome: CancelledPermissionOutcome()}, nil
	case <-ctx.Done():
		s.removePermission(requestID, pending)
		return RequestPermissionResponse{Outcome: CancelledPermissionOutcome()}, nil
	}
}

func (s *RuntimeSession) emitLocked(active *activePrompt, event PromptEvent) {
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now().UTC()
	}
	if !active.accepted && event.Type != PromptEventAccepted {
		if len(active.preAccepted) >= s.config.MaxBufferedEvents || active.bufferedBytes+event.Size > s.config.MaxBufferedEventBytes {
			s.markOverflowedLocked(active)
			return
		}
		active.bufferedBytes += event.Size
		active.preAccepted = append(active.preAccepted, event)
		return
	}
	if event.Size > 0 && active.bufferedBytes+event.Size > s.config.MaxBufferedEventBytes {
		s.markOverflowedLocked(active)
		return
	}
	active.seq++
	event.Sequence = active.seq
	event.Timestamp = time.Now().UTC()
	select {
	case active.events <- event:
		active.bufferedBytes += event.Size
	default:
		s.markOverflowedLocked(active)
	}
}

// releaseBufferedEvent returns a consumed event's bytes to the prompt's
// buffered-bytes budget.
func (s *RuntimeSession) releaseBufferedEvent(active *activePrompt, event PromptEvent) {
	if active == nil || event.Size <= 0 {
		return
	}
	s.mu.Lock()
	active.bufferedBytes -= event.Size
	if active.bufferedBytes < 0 {
		active.bufferedBytes = 0
	}
	s.mu.Unlock()
}

// markOverflowedLocked records event loss and schedules the bounded prompt
// cancellation exactly once. Pre-acceptance overflow must escalate the same
// way as post-acceptance overflow: a prompt that permanently lost events must
// not keep occupying a global prompt slot until settlement or lease expiry.
func (s *RuntimeSession) markOverflowedLocked(active *activePrompt) {
	if active.overflowed {
		return
	}
	active.overflowed = true
	slog.Warn("ACP prompt event buffer overflowed; cancelling the prompt",
		"promptID", active.id, "bufferedEvents", s.config.MaxBufferedEvents, "bufferedBytes", active.bufferedBytes,
		"maxBufferedEventBytes", s.config.MaxBufferedEventBytes, "lastSequence", active.seq, "accepted", active.accepted)
	go func(promptID string) {
		ctx, cancel := context.WithTimeout(context.Background(), s.config.CancelGrace*2)
		defer cancel()
		_, _ = s.CancelPrompt(ctx, promptID)
	}(active.id)
}

func (s *RuntimeSession) expirePrompt(promptID string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.CancelGrace*2)
	defer cancel()
	_, _ = s.CancelPrompt(ctx, promptID)
}

func (s *RuntimeSession) removePermission(requestID string, pending *pendingPermission) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.permissions[requestID] == pending {
		delete(s.active.permissions, requestID)
	}
}

func (s *RuntimeSession) tombstoneResult(promptID string) (PromptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tombstone, ok := s.tombstones[promptID]
	if !ok {
		return PromptResult{}, fmt.Errorf("prompt %s settled without tombstone", promptID)
	}
	return tombstone.Result, nil
}

func cancelPendingPermissions(active *activePrompt) {
	for id, pending := range active.permissions {
		delete(active.permissions, id)
		select {
		case pending.result <- CancelledPermissionOutcome():
		default:
		}
	}
}

func acpMCPCapabilityEnabled(capabilities map[string]any, name string) bool {
	value, ok := capabilities[name]
	if !ok {
		return false
	}
	enabled, ok := value.(bool)
	return ok && enabled
}

func containsAuthMethod(methods []AuthMethod, id string) bool {
	for _, method := range methods {
		if method.ID == id {
			return true
		}
	}
	return false
}

func stopProcessBestEffort(process *Process, grace time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), grace*2)
	defer cancel()
	_, err := process.Stop(ctx, grace)
	return err
}

func (s *RuntimeSession) Freeze(ctx context.Context) error {
	s.mu.Lock()
	if s.deleted || s.active != nil {
		s.mu.Unlock()
		return fmt.Errorf("runtime session must be idle before workspace freeze")
	}
	process := s.process
	s.mu.Unlock()
	return process.Freeze(ctx)
}

// ChildIdentity returns the immutable UID/GID assigned to the provider process.
func (s *RuntimeSession) ChildIdentity() (int, int) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config.Process.UID, s.config.Process.GID
}

func (s *RuntimeSession) Thaw() error {
	s.mu.Lock()
	if s.deleted || s.active != nil {
		s.mu.Unlock()
		return fmt.Errorf("runtime session cannot thaw while deleted or prompt-active")
	}
	process := s.process
	s.mu.Unlock()
	return process.Thaw()
}
