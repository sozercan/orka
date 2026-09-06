package cliwrapper

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/harness/ledger"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/workerenv"
	"github.com/orka-agents/orka/workers/common"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxTerminalResultBytes           = 512 * 1024
	localOutputRef                   = "cliwrapper-result-v1"
	terminalLedgerPersistFailed      = "persist-failed"
	admissionLedgerReconcileFailed   = "reconcile-failed"
	childCredentialCleanupFailed     = "cleanup-failed"
	durableAdmissionReconcileTimeout = 5 * time.Second
	durableLedgerReclaimTimeout      = 5 * time.Second
	durableLedgerReclaimBatch        = 128
	durableAcceptanceRejectionReason = "wrapper-durable-acceptance-failed"
	durableLocalRejectionReason      = "wrapper-local-admission-rejected"
	failedArtifactRetentionPrefix    = "orka-harness-artifact-upload-failed-"
	maxFailedArtifactRetentions      = 8
)

var failedArtifactRetentionMu sync.Mutex

type Server struct {
	config                         Config
	adapter                        RuntimeAdapter
	runner                         commandRunner
	now                            func() time.Time
	configuredExactRedactionValues []string

	turnRegistry *turnRegistry
	ledgerMu     sync.RWMutex
	ledger       *ledger.Ledger

	healthMu           sync.RWMutex
	terminalLedgerErr  error
	admissionLedgerErr error
	// admissionLedgerErrRetryable is true only for bounded ledger reclamation
	// failures. Ambiguous admission reconciliation failures remain fail-closed.
	admissionLedgerErrRetryable bool
	childCredentialProcessErr   error
}

type commandRunner interface {
	Run(context.Context, *CommandSpec) (CommandResult, error)
}

type RuntimeSupportProvider interface {
	SupportedRuntimes() []string
}

func NewServer(cfg Config, adapter RuntimeAdapter) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if adapter == nil {
		var err error
		adapter, err = NewRuntimeAdapter(cfg)
		if err != nil {
			return nil, err
		}
	}
	s := &Server{
		config:                         cfg,
		adapter:                        adapter,
		runner:                         NewCommandRunner(cfg),
		now:                            time.Now,
		configuredExactRedactionValues: exactConfiguredEnvValues(cfg.CommandEnv),
		turnRegistry:                   newTurnRegistry(),
	}
	if ledgerPath := strings.TrimSpace(cfg.AdmissionLedgerPath); ledgerPath != "" {
		admissionLedger, err := ledger.OpenWithGeneration(ledgerPath, cfg.LedgerGeneration)
		if err != nil {
			return nil, err
		}
		s.ledger = admissionLedger
		if err := s.reconcileOrphanedDurableTurns(context.Background()); err != nil {
			_ = admissionLedger.Close()
			s.ledger = nil
			return nil, err
		}
		if err := admissionLedger.ActivateGeneration(context.Background(), cfg.LedgerGeneration); err != nil {
			_ = admissionLedger.Close()
			s.ledger = nil
			return nil, fmt.Errorf("activate wrapper ledger generation: %w", err)
		}
		if err := s.reclaimSettledTurns(context.Background()); err != nil {
			_ = admissionLedger.Close()
			s.ledger = nil
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) reconcileOrphanedDurableTurns(ctx context.Context) error {
	records, err := s.ledger.ListUnsettledTurns(ctx)
	if err != nil {
		return fmt.Errorf("list orphaned durable turns: %w", err)
	}
	for i := range records {
		record := &records[i]
		switch record.State {
		case ledger.TurnAdmitted:
			if err := s.ledger.MarkTurnRejected(ctx, record.TurnID, "wrapper-restarted-before-acceptance"); err != nil {
				return fmt.Errorf("reject orphaned admitted turn: %w", err)
			}
		case ledger.TurnAccepted:
			runtimeSessionID := strings.TrimSpace(record.RuntimeSessionID)
			if runtimeSessionID == "" {
				runtimeSessionID = "unknown-after-wrapper-restart"
			}
			correlationID := strings.TrimSpace(record.CorrelationID)
			if correlationID == "" {
				correlationID = "unknown-after-wrapper-restart"
			}
			receipt, marshalErr := harness.MarshalDurableTurnTerminalReceipt(harness.DurableTurnTerminalReceipt{
				Version:          harness.ProtocolVersion,
				Kind:             harness.DurableTurnTerminalOutcomeUnknown,
				RuntimeSessionID: harness.RuntimeSessionID(runtimeSessionID),
				TurnID:           harness.HarnessTurnID(record.TurnID),
				CorrelationID:    correlationID,
				OutcomeUnknown: &harness.DurableTurnOutcomeUnknownReceipt{
					Reason: "wrapper-restarted-after-acceptance",
				},
			})
			if marshalErr != nil {
				return fmt.Errorf("encode orphaned accepted turn receipt: %w", marshalErr)
			}
			if err := s.ledger.RecordTurnTerminal(ctx, record.TurnID, receipt, true); err != nil {
				return fmt.Errorf("settle orphaned accepted turn: %w", err)
			}
		}
	}
	return nil
}

func (s *Server) reclaimSettledTurns(ctx context.Context) error {
	if s == nil || s.ledger == nil {
		return nil
	}
	reclaimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), durableLedgerReclaimTimeout)
	defer cancel()
	cutoff := s.now().UTC().Add(-s.config.LedgerRetention)
	if _, err := s.ledger.ReclaimSettledTurnsBefore(reclaimCtx, cutoff, durableLedgerReclaimBatch); err != nil {
		reclaimErr := fmt.Errorf("reclaim settled wrapper ledger turns: %w", err)
		s.setRetryableAdmissionLedgerError(reclaimErr)
		return reclaimErr
	}
	s.clearRetryableAdmissionLedgerError()
	return nil
}

// Close releases the durable wrapper admission ledger. The HTTP server must be
// stopped before calling Close.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.ledgerMu.Lock()
	defer s.ledgerMu.Unlock()
	if s.ledger == nil {
		return nil
	}
	err := s.ledger.Close()
	s.ledger = nil
	return err
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(harness.HealthPath, s.handleHealth)
	mux.HandleFunc(harness.ReadinessPath, s.handleReadiness)
	mux.HandleFunc(harness.CapabilitiesPath, s.handleCapabilities)
	mux.HandleFunc(harness.TurnsPath, s.handleStartTurn)
	mux.HandleFunc(harness.TurnsPath+"/", s.handleTurn)
	mux.HandleFunc(harness.AdminTurnsPath+"/", s.handleAdminTurn)
	mux.HandleFunc(harness.AdminDrainPath, s.handleAdminDrain)
	mux.HandleFunc(harness.AdminClosePath, s.handleAdminClose)
	mux.HandleFunc(harness.AdminRolloverPath, s.handleAdminRollover)
	mux.HandleFunc(harness.AdminAbortRolloverPath, s.handleAdminAbortRollover)
	return mux
}

func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	if s.config.AllowUnauthenticated {
		return true
	}
	want, err := s.currentAuthValue()
	if err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "wrapper auth token is not configured")
		return false
	}
	if want == "" {
		writeSafeError(w, http.StatusServiceUnavailable, "wrapper auth token is not configured")
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		writeSafeError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func (s *Server) finishTurn(turn *turnState) {
	defer turn.clearExactRedactionValues()
	if !s.childCredentialProcessesHealthy() {
		return
	}
	if !s.persistTurnTerminal(turn) {
		return
	}
	turn.close()
	s.turnRegistry.finishActive()
	s.scheduleTurnEviction(turn)
}

func (s *Server) persistTurnTerminal(turn *turnState) bool {
	s.ledgerMu.RLock()
	defer s.ledgerMu.RUnlock()
	if s.ledger != nil {
		receipt, outcomeUnknown := s.durableTerminalReceipt(turn)
		var durableOutput *ledger.TurnOutput
		if !outcomeUnknown && turn.terminalOutputRef() == localOutputRef {
			data, ok, err := turn.output()
			if err != nil || !ok {
				if err == nil {
					err = errors.New("terminal output payload is missing")
				}
				s.setTerminalLedgerError(err)
				return false
			}
			durableOutput = &ledger.TurnOutput{Ref: localOutputRef, Data: data}
		}
		if err := s.ledger.RecordTurnTerminalWithOutput(
			context.Background(), string(turn.id()), receipt, outcomeUnknown, durableOutput,
		); err != nil {
			// Do not expose stream completion, release capacity, or schedule
			// eviction until the authoritative terminal receipt is durable. The
			// unhealthy readiness response surfaces the failure without leaking
			// ledger details; an operator can restart into conservative recovery.
			s.setTerminalLedgerError(err)
			return false
		}
		s.setTerminalLedgerError(nil)
	}
	return true
}

func (s *Server) markDurableTurnAccepted(ctx context.Context, turn *turnState) error {
	acceptErr := s.ledger.MarkTurnAccepted(ctx, string(turn.id()))
	if acceptErr == nil {
		return nil
	}

	// AdmitTurn has already committed. Reconcile that durable admission before
	// releasing the wrapper-local reservation, even if the request was canceled.
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		durableAdmissionReconcileTimeout,
	)
	rejectErr := s.ledger.MarkTurnRejected(
		cleanupCtx,
		string(turn.id()),
		durableAcceptanceRejectionReason,
	)
	cancel()
	if rejectErr != nil {
		combinedErr := errors.Join(acceptErr, fmt.Errorf("reconcile durable turn admission: %w", rejectErr))
		s.setAdmissionLedgerError(combinedErr)
		return combinedErr
	}

	s.turnRegistry.reject(turn)
	return acceptErr
}

func (s *Server) markDurableLocalAdmissionRejected(ctx context.Context, turnID harness.HarnessTurnID) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		durableAdmissionReconcileTimeout,
	)
	defer cancel()
	if err := s.ledger.MarkTurnRejected(cleanupCtx, string(turnID), durableLocalRejectionReason); err != nil {
		reconcileErr := fmt.Errorf("reconcile durable local turn rejection: %w", err)
		s.setAdmissionLedgerError(reconcileErr)
		return reconcileErr
	}
	return nil
}

func (s *Server) setTerminalLedgerError(err error) {
	s.healthMu.Lock()
	s.terminalLedgerErr = err
	s.healthMu.Unlock()
}

func (s *Server) terminalLedgerHealthy() bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.terminalLedgerErr == nil
}

func (s *Server) setAdmissionLedgerError(err error) {
	s.healthMu.Lock()
	s.admissionLedgerErr = err
	s.admissionLedgerErrRetryable = false
	s.healthMu.Unlock()
}

func (s *Server) setRetryableAdmissionLedgerError(err error) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if s.admissionLedgerErr != nil && !s.admissionLedgerErrRetryable {
		return
	}
	s.admissionLedgerErr = err
	s.admissionLedgerErrRetryable = err != nil
}

func (s *Server) clearRetryableAdmissionLedgerError() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if !s.admissionLedgerErrRetryable {
		return
	}
	s.admissionLedgerErr = nil
	s.admissionLedgerErrRetryable = false
}

func (s *Server) admissionLedgerReclaimRetryable() bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.admissionLedgerErr == nil || s.admissionLedgerErrRetryable
}

func (s *Server) admissionLedgerHealthy() bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.admissionLedgerErr == nil
}

func (s *Server) setChildCredentialProcessError(err error) {
	s.healthMu.Lock()
	s.childCredentialProcessErr = err
	s.healthMu.Unlock()
}

func (s *Server) latchChildCredentialProcessCleanupFailure(err error) bool {
	if !errors.Is(err, errChildCredentialProcessCleanupUnproven) {
		return false
	}
	s.setChildCredentialProcessError(err)
	return true
}

func (s *Server) childCredentialProcessesHealthy() bool {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.childCredentialProcessErr == nil
}

func (s *Server) scheduleTurnEviction(turn *turnState) {
	retention := s.config.TurnRetention
	if retention <= 0 {
		s.evictTurn(turn)
		return
	}
	time.AfterFunc(retention, func() { s.evictTurn(turn) })
}

func (s *Server) evictTurn(turn *turnState) {
	if turn == nil {
		return
	}
	if s.config.TurnRetention > 0 && turn.hasUnfetchedOutput() && turn.outputRetentionActive() {
		s.scheduleTurnEviction(turn)
		return
	}
	s.turnRegistry.evict(turn)
	turn.cleanupOutput()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, s.healthResponse())
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	response := s.healthResponse()
	statusCode := http.StatusOK
	if !response.Ready {
		statusCode = http.StatusServiceUnavailable
	}
	harness.WriteJSON(w, statusCode, response)
}

func (s *Server) healthResponse() harness.HealthResponse {
	status := harness.HealthStatusOK
	ready := true
	metadata := map[string]string{
		"runtime": s.adapter.Name(),
		"mode":    "observed",
	}
	if !s.terminalLedgerHealthy() {
		status = harness.HealthStatusUnhealthy
		ready = false
		metadata["terminalLedger"] = terminalLedgerPersistFailed
	}
	if !s.admissionLedgerHealthy() {
		status = harness.HealthStatusUnhealthy
		ready = false
		metadata["admissionLedger"] = admissionLedgerReconcileFailed
	}
	if !s.childCredentialProcessesHealthy() {
		status = harness.HealthStatusUnhealthy
		ready = false
		metadata["childCredentialProcesses"] = childCredentialCleanupFailed
	}
	return harness.HealthResponse{
		Version:   harness.ProtocolVersion,
		Status:    status,
		Ready:     ready,
		CheckedAt: s.now().UTC(),
		Metadata:  metadata,
	}
}

func (s *Server) currentAuthValue() (string, error) {
	if s.config.AllowUnauthenticated {
		return "", nil
	}
	var value string
	if file := strings.TrimSpace(s.config.AuthValueFile); file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		value = string(data)
	} else {
		value = s.config.AuthValue
	}
	value = strings.TrimSpace(value)
	if err := validateAuthValue(value); err != nil {
		return "", err
	}
	return value, nil
}

func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.CapabilitiesResponse{
		Version:                 harness.ProtocolVersion,
		ProtocolVersion:         harness.ProtocolVersion,
		Transport:               harness.HTTPTransport,
		RuntimeName:             s.adapter.Name(),
		ProviderKind:            harness.ProviderKindKubernetesService,
		ToolExecutionModes:      []harness.ToolExecutionMode{harness.ToolExecutionModeObserved},
		SupportsCancel:          true,
		SupportsRuntimeSessions: true,
		MaxConcurrentTurns:      1,
		Metadata:                s.capabilitiesMetadata(),
	})
}

func (s *Server) capabilitiesMetadata() map[string]string {
	metadata := map[string]string{
		"wrapper": "cli",
		"mode":    "observed",
	}
	if provider, ok := s.adapter.(RuntimeSupportProvider); ok {
		if runtimes := provider.SupportedRuntimes(); len(runtimes) > 0 {
			metadata["supportedRuntimes"] = strings.Join(runtimes, ",")
		}
	}
	return metadata
}

type durableTurnAdmission struct {
	taskUID       string
	attempt       int32
	requestDigest string
}

func validateDurableTurnAdmission(request harness.StartTurnRequest) (*durableTurnAdmission, error) {
	metadata := request.Metadata
	taskUID := strings.TrimSpace(metadata[harness.MetadataTaskUID])
	if taskUID == "" {
		return nil, fmt.Errorf("durable turn admission requires task UID metadata")
	}
	attemptValue := strings.TrimSpace(metadata[harness.MetadataAttempt])
	attempt, err := strconv.ParseInt(attemptValue, 10, 32)
	if err != nil || attempt < 1 {
		return nil, fmt.Errorf("durable turn admission requires a positive attempt")
	}
	for label, value := range map[string]string{
		"binding":  metadata[harness.MetadataBindingDigest],
		"snapshot": metadata[harness.MetadataSnapshotDigest],
	} {
		if !validSHA256Digest(value) {
			return nil, fmt.Errorf("durable turn admission requires a canonical %s digest", label)
		}
	}
	requestDigest := strings.TrimSpace(metadata[harness.MetadataRequestDigest])
	if !validSHA256Digest(requestDigest) {
		return nil, fmt.Errorf("durable turn admission requires a canonical request digest")
	}
	computed, err := harness.CanonicalStartTurnRequestDigest(request)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(requestDigest), []byte(computed)) != 1 {
		return nil, fmt.Errorf("durable turn request digest does not match the submitted request")
	}
	return &durableTurnAdmission{taskUID: taskUID, attempt: int32(attempt), requestDigest: requestDigest}, nil
}

func validSHA256Digest(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func safeAdminTurnStatus(record *ledger.TurnRecord) (harness.DurableTurnStatus, error) {
	if record == nil {
		return harness.DurableTurnStatus{}, fmt.Errorf("durable turn record is required")
	}
	status := harness.DurableTurnStatus{
		TurnID: record.TurnID, TaskUID: record.TaskUID, Attempt: record.Attempt,
		RequestDigest: record.RequestDigest, State: harness.DurableTurnAdmissionState(record.State),
		TerminalReceiptDigest: record.TerminalReceiptDigest, UpdatedAt: record.UpdatedAt,
	}
	switch record.State {
	case ledger.TurnTerminal, ledger.TurnOutcomeUnknown:
		var receipt harness.DurableTurnTerminalReceipt
		if err := json.Unmarshal(record.TerminalReceipt, &receipt); err != nil {
			return harness.DurableTurnStatus{}, fmt.Errorf("decode durable terminal receipt: %w", err)
		}
		if err := receipt.Validate(); err != nil {
			return harness.DurableTurnStatus{}, err
		}
		digest, err := harness.DurableTurnTerminalReceiptDigest(receipt)
		if err != nil {
			return harness.DurableTurnStatus{}, err
		}
		if digest != record.TerminalReceiptDigest {
			return harness.DurableTurnStatus{}, fmt.Errorf("durable terminal receipt digest mismatch")
		}
		if record.State == ledger.TurnTerminal && receipt.Kind == harness.DurableTurnTerminalOutcomeUnknown {
			return harness.DurableTurnStatus{}, fmt.Errorf("terminal ledger state contains outcome-unknown receipt")
		}
		if record.State == ledger.TurnOutcomeUnknown && receipt.Kind != harness.DurableTurnTerminalOutcomeUnknown {
			return harness.DurableTurnStatus{}, fmt.Errorf("outcome-unknown ledger state contains terminal receipt")
		}
		status.TerminalReceipt = &receipt
	default:
		if len(record.TerminalReceipt) != 0 || record.TerminalReceiptDigest != "" {
			return harness.DurableTurnStatus{}, fmt.Errorf("nonterminal ledger state contains terminal receipt")
		}
	}
	return status, nil
}

// requireAdmin applies the shared admin-endpoint guard ladder: authorization,
// method check, then durable-ledger availability. It writes the error response
// and returns false when the request must not proceed.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request, method string) bool {
	if !s.authorized(w, r) {
		return false
	}
	if r.Method != method {
		writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	if s.ledger == nil {
		writeSafeError(w, http.StatusServiceUnavailable, "durable admission ledger is not configured")
		return false
	}
	return true
}

func (s *Server) handleAdminTurn(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r, http.MethodGet) {
		return
	}
	turnID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, harness.AdminTurnsPath+"/"))
	if turnID == "" || strings.Contains(turnID, "/") {
		writeSafeError(w, http.StatusBadRequest, "turn ID is required")
		return
	}
	record, err := s.ledger.GetTurn(r.Context(), turnID)
	if err != nil {
		if errors.Is(err, ledger.ErrNotFound) {
			writeSafeError(w, http.StatusNotFound, "turn not found")
			return
		}
		writeSafeError(w, http.StatusServiceUnavailable, "read durable turn status failed")
		return
	}
	status, err := safeAdminTurnStatus(record)
	if err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "durable turn receipt validation failed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, status)
}

func (s *Server) handleAdminClose(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r, http.MethodPost) {
		return
	}
	if err := s.ledger.CloseAdmission(r.Context()); err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "close durable admission failed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.DurableAdmissionCloseResponse{AdmissionClosed: true})
}

func (s *Server) handleAdminDrain(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r, http.MethodGet) {
		return
	}
	closed, closedAt, err := s.ledger.AdmissionClosed(r.Context())
	if err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "read durable admission state failed")
		return
	}
	records, err := s.ledger.ListUnsettledTurns(r.Context())
	if err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "read durable drain inventory failed")
		return
	}
	statuses := make([]harness.DurableTurnStatus, 0, len(records))
	for i := range records {
		status, statusErr := safeAdminTurnStatus(&records[i])
		if statusErr != nil {
			writeSafeError(w, http.StatusServiceUnavailable, "durable drain receipt validation failed")
			return
		}
		statuses = append(statuses, status)
	}
	harness.WriteJSON(w, http.StatusOK, harness.DurableDrainStatus{
		AdmissionClosed: closed, AdmissionClosedAt: closedAt,
		Completed: closed && len(statuses) == 0, Unsettled: statuses,
	})
}

func (s *Server) handleAdminRollover(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r, http.MethodPost) {
		return
	}
	var request harness.DurableRolloverPrepareRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	nextGeneration := strings.TrimSpace(request.NextGeneration)
	currentGeneration, err := s.ledger.PrepareRollover(r.Context(), nextGeneration)
	if err != nil {
		writeSafeError(w, http.StatusConflict, "durable rollover preparation failed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.DurableRolloverPrepareResponse{
		CurrentGeneration: currentGeneration,
		NextGeneration:    nextGeneration,
		Prepared:          true,
	})
}

func (s *Server) handleAdminAbortRollover(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r, http.MethodPost) {
		return
	}
	var request harness.DurableRolloverAbortRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	expectedGeneration := strings.TrimSpace(request.ExpectedGeneration)
	if err := s.ledger.AbortRollover(r.Context(), expectedGeneration); err != nil {
		writeSafeError(w, http.StatusConflict, "durable rollover abort failed")
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.DurableRolloverAbortResponse{
		CurrentGeneration: expectedGeneration,
		AdmissionReopened: true,
	})
}

func (s *Server) handleStartTurn(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.childCredentialProcessesHealthy() {
		writeSafeError(w, http.StatusServiceUnavailable, "turn process isolation is unavailable")
		return
	}
	if !s.admissionLedgerReclaimRetryable() {
		writeSafeError(w, http.StatusServiceUnavailable, "durable turn admission is unavailable")
		return
	}
	if err := s.reclaimSettledTurns(r.Context()); err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "durable turn admission is unavailable")
		return
	}
	if !s.admissionLedgerHealthy() {
		writeSafeError(w, http.StatusServiceUnavailable, "durable turn admission is unavailable")
		return
	}
	var request harness.StartTurnRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := request.Validate(); err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}
	eventStreamPath, err := harness.EventStreamPath(request.TurnID)
	if err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var durable *durableTurnAdmission
	if s.ledger != nil {
		durable, err = validateDurableTurnAdmission(request)
		if err != nil {
			writeSafeError(w, http.StatusBadRequest, err.Error())
			return
		}
		outcome, existing, admitErr := s.ledger.AdmitTurn(
			r.Context(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
			string(request.RuntimeSessionID), request.CorrelationID,
		)
		if admitErr != nil {
			switch {
			case errors.Is(admitErr, ledger.ErrAdmissionClosed):
				writeSafeError(w, http.StatusConflict, "wrapper admission is closed")
			case errors.Is(admitErr, ledger.ErrDigestMismatch):
				writeSafeError(w, http.StatusConflict, "turn identity was already admitted with a different request")
			default:
				writeSafeError(w, http.StatusServiceUnavailable, "durable turn admission failed")
			}
			return
		}
		if outcome == ledger.AdmitOutcomeDuplicate {
			if existing != nil &&
				(existing.State == ledger.TurnAdmitted || existing.State == ledger.TurnAccepted) &&
				s.turnRegistry.active(request.TurnID) {
				writeAcceptedTurn(w, request, eventStreamPath)
				return
			}
			writeSafeError(
				w,
				http.StatusConflict,
				"turn was already durably admitted; inspect authenticated turn status before reconciliation",
			)
			return
		}
	}

	state, err := s.turnRegistry.admit(request, s.now)
	if err != nil {
		if s.ledger != nil && durable != nil {
			if rejectErr := s.markDurableLocalAdmissionRejected(r.Context(), request.TurnID); rejectErr != nil {
				writeSafeError(w, http.StatusServiceUnavailable, "durable turn rejection failed")
				return
			}
		}
		switch {
		case errors.Is(err, errTurnAlreadyExists):
			writeSafeError(w, http.StatusConflict, "turn already exists")
		case errors.Is(err, errTurnAlreadyCompleted):
			// This turn ID was already accepted and run to completion (then evicted).
			// Re-accepting it would duplicate external side effects (branch push, PR
			// creation, token spend), so reject deterministically.
			writeSafeError(w, http.StatusConflict, "turn already completed")
		case errors.Is(err, errMaximumConcurrentTurns):
			writeSafeError(w, http.StatusConflict, "maximum concurrent turns reached")
		default:
			writeSafeError(w, http.StatusInternalServerError, "failed to admit turn")
		}
		return
	}
	for _, value := range s.configuredExactRedactionValues {
		state.addExactRedactionValue(value)
	}
	if s.ledger != nil {
		if err := s.markDurableTurnAccepted(r.Context(), state); err != nil {
			writeSafeError(w, http.StatusServiceUnavailable, "durable turn acceptance failed")
			return
		}
	}

	go s.runTurn(state)
	writeAcceptedTurn(w, request, eventStreamPath)
}

func writeAcceptedTurn(w http.ResponseWriter, request harness.StartTurnRequest, eventStreamPath string) {
	harness.WriteJSON(w, http.StatusAccepted, harness.StartTurnResponse{
		Version:          harness.ProtocolVersion,
		Accepted:         true,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		EventStreamPath:  eventStreamPath,
	})
}

func (s *Server) handleTurn(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	turnID, resource, err := harness.ParseTurnResourcePath(r.URL.EscapedPath())
	if err != nil {
		if errors.Is(err, harness.ErrTurnPathNotFound) {
			writeSafeError(w, http.StatusNotFound, "not found")
		} else {
			writeSafeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	if resource == harness.TurnResourceOutputAcknowledgement {
		if r.Method != http.MethodPost {
			writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if s.ledger == nil {
			writeSafeError(w, http.StatusServiceUnavailable, "durable turn output acknowledgement is unavailable")
			return
		}
		s.handleOutputAcknowledgement(w, r, turnID)
		return
	}
	if resource == harness.TurnResourceSettlementAcknowledgement {
		if r.Method != http.MethodPost {
			writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if s.ledger == nil {
			writeSafeError(w, http.StatusServiceUnavailable, "durable turn settlement acknowledgement is unavailable")
			return
		}
		s.handleSettlementAcknowledgement(w, r, turnID)
		return
	}
	turn := s.turnRegistry.lookup(turnID)
	if turn == nil && resource == harness.TurnResourceOutput && r.Method == http.MethodGet && s.ledger != nil {
		s.handleDurableOutput(w, r, turnID)
		return
	}
	if turn == nil {
		writeSafeError(w, http.StatusNotFound, "turn not found")
		return
	}
	switch resource {
	case harness.TurnResourceEvents:
		if r.Method != http.MethodGet {
			writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleEvents(w, r, turn)
	case harness.TurnResourceCancel:
		if r.Method != http.MethodPost {
			writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleCancel(w, r, turn)
	case harness.TurnResourceOutput:
		if r.Method != http.MethodGet {
			writeSafeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleOutput(w, r, turn)
	default:
		writeSafeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request, turn *turnState) {
	var request harness.CancelTurnRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := request.Validate(); err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := turn.matchesCancel(request); err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}
	turn.cancel()
	harness.WriteJSON(w, http.StatusAccepted, harness.CancelTurnResponse{
		Version:          harness.ProtocolVersion,
		Accepted:         true,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		Message:          "cancel accepted",
	})
}

func (s *Server) handleOutput(w http.ResponseWriter, r *http.Request, turn *turnState) {
	if ref := strings.TrimSpace(r.URL.Query().Get("ref")); ref != localOutputRef {
		writeSafeError(w, http.StatusNotFound, "output not found")
		return
	}
	data, ok, err := turn.output()
	if err != nil {
		writeSafeError(w, http.StatusInternalServerError, "failed to read turn output")
		return
	}
	if !ok {
		writeSafeError(w, http.StatusNotFound, "output not found")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := w.Write(data); err == nil {
		turn.markOutputFetched()
	}
}

func (s *Server) handleDurableOutput(w http.ResponseWriter, r *http.Request, turnID harness.HarnessTurnID) {
	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref != localOutputRef {
		writeSafeError(w, http.StatusNotFound, "output not found")
		return
	}
	data, err := s.ledger.GetTurnOutput(r.Context(), string(turnID), ref)
	if err != nil {
		if errors.Is(err, ledger.ErrNotFound) {
			writeSafeError(w, http.StatusNotFound, "output not found")
			return
		}
		writeSafeError(w, http.StatusServiceUnavailable, "failed to read durable turn output")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (s *Server) handleOutputAcknowledgement(w http.ResponseWriter, r *http.Request, turnID harness.HarnessTurnID) {
	var request harness.TurnOutputAcknowledgementRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := request.ValidateFor(turnID); err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.ledger.AcknowledgeTurnOutput(
		r.Context(), string(turnID), request.OutputRef, request.TerminalReceiptDigest,
	); err != nil {
		switch {
		case errors.Is(err, ledger.ErrNotFound):
			writeSafeError(w, http.StatusNotFound, "output not found")
		case errors.Is(err, ledger.ErrOutputAcknowledgementMismatch):
			writeSafeError(w, http.StatusConflict, "output acknowledgement does not match durable receipt")
		default:
			writeSafeError(w, http.StatusServiceUnavailable, "failed to acknowledge durable turn output")
		}
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.TurnOutputAcknowledgementResponse{
		Version:               harness.ProtocolVersion,
		TurnID:                request.TurnID,
		OutputRef:             request.OutputRef,
		TerminalReceiptDigest: request.TerminalReceiptDigest,
		Acknowledged:          true,
	})
}

func (s *Server) handleSettlementAcknowledgement(w http.ResponseWriter, r *http.Request, turnID harness.HarnessTurnID) {
	var request harness.TurnSettlementAcknowledgementRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeSafeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := request.ValidateFor(turnID); err != nil {
		writeSafeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.ledger.AcknowledgeTurnSettlement(
		r.Context(), string(turnID), request.RequestDigest, request.TerminalReceiptDigest,
	); err != nil {
		switch {
		case errors.Is(err, ledger.ErrNotFound):
			writeSafeError(w, http.StatusNotFound, "turn not found")
		case errors.Is(err, ledger.ErrSettlementAcknowledgementMismatch):
			writeSafeError(w, http.StatusConflict, "settlement acknowledgement does not match durable evidence")
		default:
			writeSafeError(w, http.StatusServiceUnavailable, "failed to acknowledge durable turn settlement")
		}
		return
	}
	if err := s.reclaimSettledTurns(r.Context()); err != nil {
		writeSafeError(w, http.StatusServiceUnavailable, "failed to reclaim settled wrapper turns")
		return
	}
	harness.WriteJSON(w, http.StatusOK, harness.TurnSettlementAcknowledgementResponse{
		Version:               harness.ProtocolVersion,
		TurnID:                request.TurnID,
		RequestDigest:         request.RequestDigest,
		TerminalReceiptDigest: request.TerminalReceiptDigest,
		Acknowledged:          true,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request, turn *turnState) {
	afterSeq, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("afterSeq")), 10, 64)
	if err != nil && strings.TrimSpace(r.URL.Query().Get("afterSeq")) != "" {
		writeSafeError(w, http.StatusBadRequest, "afterSeq must be an integer")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	nextSeq := afterSeq + 1
	for {
		frames, closed := turn.framesFrom(nextSeq)
		for _, frame := range frames {
			if err := harness.WriteSSEFrame(w, frame); err != nil {
				return
			}
			nextSeq = frame.Seq + 1
		}
		if closed {
			_ = harness.WriteSSEDone(w)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Server) runTurn(turn *turnState) { //nolint:gocyclo
	defer s.finishTurn(turn)
	ctx := extractHarnessTurnTraceContext(turn.ctx, turn.request)
	if deadline := turn.deadline(); !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	agentName := strings.TrimSpace(turn.request.Metadata["agentName"])
	if agentName == "" {
		agentName = s.adapter.Name()
	}
	ctx, taskSpan := tracing.Tracer("orka.harness").Start(ctx, "task.run", trace.WithAttributes(
		tracing.TaskAttributes(turn.request.TaskName, turn.request.Namespace, turn.request.Namespace, agentName, "")...,
	))
	defer func() {
		if failed, errType := turnTerminalFailure(turn); failed {
			taskSpan.SetStatus(codes.Error, errType)
			taskSpan.SetAttributes(attribute.String("error.type", errType))
		}
		taskSpan.End()
	}()
	turnCtx := turn.materializeContext(s.adapter.Name(), s.config)
	if eventing, ok := s.adapter.(EventingAdapter); ok {
		_, err := eventing.RunTurn(ctx, turnCtx, func(frame harness.HarnessEventFrame) error {
			turn.appendFrame(s.normalizeFrame(turn, frame))
			return nil
		})
		if err != nil && !turn.hasTerminal() {
			turn.appendFrame(s.failedFrame(turn, "adapter_error", err.Error(), false))
		}
		return
	}

	turn.appendFrame(s.frame(turn, harness.FrameTurnStarted, "turn started", nil))
	ClearTurnArtifacts()
	restoreWorkspaceEnv := setTemporaryEnvEntries(turnCtx.Env)
	preparedWorkspace, err := prepareTurnWorkspace(ctx, turnCtx)
	restoreWorkspaceEnv()
	if err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	cleanupWorkspace := true
	defer func() {
		if cleanupWorkspace {
			preparedWorkspace.cleanup()
		}
	}()
	turnCtx.WorkDir = preparedWorkspace.workDir
	turnCtx.RootDir = preparedWorkspace.rootDir
	turnArtifactsDir := turnArtifactDir(preparedWorkspace)
	cleanupArtifacts := true
	defer func() {
		if cleanupArtifacts {
			ClearTurnArtifacts(turnArtifactsDir)
		}
	}()
	if err := prepareTurnArtifactsDirForWrapper(turnArtifactsDir); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	if preparedWorkspace.baseDir != "" {
		turnCtx.SkillsRoot = filepath.Join(preparedWorkspace.baseDir, "skills")
	}
	restoreChildIdentity := suspendChildIdentity()
	agentCfg, err := PrepareTurnContext(ctx, &turnCtx, preparedWorkspace.rootDir, turnArtifactsDir)
	restoreChildIdentity()
	if err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	if err := ensureWorkspaceArtifactsLinkForTurn(
		preparedWorkspace.rootDir,
		turnCtx.WorkDir,
		turnArtifactsDir,
	); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	turnHomeRoot, err := os.MkdirTemp("/tmp", "orka-harness-home-*")
	if err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	if _, _, ok := childCredentialIDs(); ok {
		if err := os.Chmod(turnHomeRoot, 0o711); err != nil {
			turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
			return
		}
	}
	turnHome := filepath.Join(turnHomeRoot, "home")
	defer func() { _ = cleanupTurnWorkspacePath(turnHomeRoot) }()
	if err := os.MkdirAll(turnHome, 0o700); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	if err := prepareHomeForChild(turnHome); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	turnCtx.HomeDir = turnHome
	turnCtx.Env = setEnv(turnCtx.Env, "HOME", turnHome)
	stripGitCredentials := strings.EqualFold(strings.TrimSpace(turnCtx.Metadata["readOnly"]), "true") ||
		strings.EqualFold(strings.TrimSpace(turnCtx.Metadata["runtimeAuthOnly"]), "true")
	if stripGitCredentials {
		turnCtx.Env = removeTurnEnv(
			turnCtx.Env,
			workerenv.GitToken,
			workerenv.GitHubToken,
			workerenv.GitAskpass,
			workerenv.GitUsername,
		)
	}
	turnCtx, runtimeAuthProxyToken, closeRuntimeAuthProxy, err := protectRuntimeAuthTurn(turnCtx)
	if err != nil {
		turn.appendFrame(s.failedFrame(turn, "runtime_auth_proxy_failed", err.Error(), false))
		return
	}
	turn.addExactRedactionValue(runtimeAuthProxyToken)
	defer closeRuntimeAuthProxy()
	spec, err := s.adapter.BuildCommand(ctx, turnCtx)
	if err != nil {
		turn.appendFrame(s.failedFrame(turn, "build_command_failed", err.Error(), false))
		return
	}
	if stripGitCredentials {
		spec.UnsetEnv = append(spec.UnsetEnv,
			workerenv.GitToken,
			workerenv.GitHubToken,
			workerenv.GitAskpass,
			workerenv.GitUsername,
		)
	}
	defer removeTempFiles(spec.TempFiles)
	if spec.Dir != "" {
		turnCtx.WorkDir = spec.Dir
	}
	if preparedWorkspace.baseDir != "" &&
		(preparedWorkspace.baseDir != preparedWorkspace.rootDir || preparedWorkspace.ownedBaseDir) {
		if err := ensureDirectoryTraversable(preparedWorkspace.baseDir); err != nil {
			turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
			return
		}
	}
	if err := chownTreeForChild(preparedWorkspace.rootDir, turnArtifactsDir); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	if err := prepareArtifactsForChild(turnArtifactsDir); err != nil {
		turn.appendFrame(s.failedFrame(turn, "workspace_prepare_failed", err.Error(), false))
		return
	}
	turn.appendFrame(s.runtimeLogFrame(turn, "runtime command started", map[string]any{
		"runtime": s.adapter.Name(),
		"command": path.Base(spec.Path),
	}))
	run, runErr := s.runner.Run(ctx, spec)
	if s.latchChildCredentialProcessCleanupFailure(runErr) {
		return
	}
	if run.FullStdoutTruncated && strings.TrimSpace(spec.ResultFile) == "" {
		turn.appendFrame(s.failedFrame(turn, "result_too_large", "runtime stdout exceeded harness storage limit", false))
		return
	}
	if strings.TrimSpace(run.Stdout) != "" {
		turn.appendFrame(s.outputFrame(turn, "stdout", run.Stdout))
	}
	if strings.TrimSpace(run.Stderr) != "" {
		turn.appendFrame(s.runtimeLogTextFrame(turn, "stderr", run.Stderr))
	}
	finalizedWorkDir := ""
	switch {
	case run.Cancelled:
		turn.appendFrame(s.frame(turn, harness.FrameTurnCancelled, "turn cancelled", nil))
	case run.TimedOut:
		turn.appendFrame(s.failedFrame(turn, "timeout", "runtime command timed out", true))
	case runErr != nil:
		msg := runErr.Error()
		if strings.TrimSpace(run.Stderr) != "" {
			msg = run.Stderr
		}
		partial, hasAdapterResult := s.failedTurnPartialResult(ctx, turnCtx, run)
		removeControlFiles(turnCtx.WorkDir, append(spec.TempFiles, spec.ResultFile)...)
		if run.FullStdoutTruncated && !hasAdapterResult {
			turn.appendFrame(s.failedFrame(turn, "result_too_large", "runtime stdout exceeded harness storage limit", false))
			return
		}
		finalizeWorkDir := turnCtx.WorkDir
		if preparedWorkspace.rootDir != "" {
			finalizeWorkDir = preparedWorkspace.rootDir
		}
		shouldFinalize := false
		if !envEntryIsTrue(turnCtx.Env, workerenv.ResultStdout) {
			var shouldFinalizeErr error
			shouldFinalize, shouldFinalizeErr = ShouldFinalizeWorkDir(ctx, finalizeWorkDir)
			if shouldFinalizeErr != nil {
				turn.appendFrame(s.runtimeLogTextFrame(turn, "result-finalize", shouldFinalizeErr.Error()))
			}
		}
		if shouldFinalize {
			restoreTurnEnv := setTemporaryEnvEntries(turnCtx.Env)
			if finalized, finalizeErr := FinalizeTurnResult(ctx, finalizeWorkDir, partial); finalizeErr != nil {
				turn.appendFrame(s.runtimeLogTextFrame(turn, "result-finalize", finalizeErr.Error()))
			} else {
				partial = string(finalized)
				finalizedWorkDir = finalizeWorkDir
			}
			restoreTurnEnv()
		}
		if artifactErr := UploadTurnArtifacts(turnCtx, turnArtifactsDir); artifactErr != nil {
			turn.appendFrame(s.runtimeLogTextFrame(
				turn,
				"artifact-upload",
				artifactErr.Error(),
			))
		}
		if finalizedWorkDir != "" {
			if cleanErr := CleanFinalizedWorkDir(ctx, finalizedWorkDir); cleanErr != nil {
				turn.appendFrame(s.runtimeLogTextFrame(turn, "workdir-cleanup", cleanErr.Error()))
			}
		}
		if len([]byte(partial)) > maxStoredResultBytes {
			turn.appendFrame(s.failedFrame(turn, "result_too_large", "runtime result exceeded harness storage limit", false))
			return
		}
		turn.appendFrame(s.failedFrameWithResult(turn, "command_failed", msg, partial, false))
	default:
		restoreTurnEnv := setTemporaryEnvEntries(turnCtx.Env)
		defer restoreTurnEnv()
		parsed, parseErr := s.adapter.ParseResult(ctx, turnCtx, run)
		if parseErr != nil {
			if strings.Contains(parseErr.Error(), "terminal frame limit") {
				turn.appendFrame(s.failedFrame(turn, "result_too_large", parseErr.Error(), false))
				return
			}
			turn.appendFrame(s.failedFrame(turn, "result_parse_failed", parseErr.Error(), false))
			return
		}
		if result, artifactErr := EnsureTurnRequiredSecurityArtifacts(
			ctx,
			agentCfg,
			parsed.Result,
			s.securityArtifactFollowUp(turn, turnCtx),
			turnArtifactsDir,
		); artifactErr != nil {
			if errors.Is(artifactErr, errChildCredentialProcessCleanupUnproven) {
				return
			}
			turn.appendFrame(s.failedFrame(turn, "required_security_artifacts_missing", artifactErr.Error(), false))
			return
		} else {
			parsed.Result = result
		}
		removeControlFiles(turnCtx.WorkDir, append(spec.TempFiles, spec.ResultFile)...)
		finalizeWorkDir := turnCtx.WorkDir
		if preparedWorkspace.rootDir != "" {
			finalizeWorkDir = preparedWorkspace.rootDir
		}
		shouldFinalize := false
		if !envEntryIsTrue(turnCtx.Env, workerenv.ResultStdout) {
			var shouldFinalizeErr error
			shouldFinalize, shouldFinalizeErr = ShouldFinalizeWorkDir(ctx, finalizeWorkDir)
			if shouldFinalizeErr != nil {
				turn.appendFrame(s.failedFrame(turn, "result_finalize_failed", shouldFinalizeErr.Error(), false))
				return
			}
		}
		if shouldFinalize {
			finalized, finalizeErr := FinalizeTurnResult(ctx, finalizeWorkDir, parsed.Result)
			if finalizeErr != nil {
				turn.appendFrame(s.failedFrame(turn, "result_finalize_failed", finalizeErr.Error(), false))
				return
			}
			parsed.Result = string(finalized)
			finalizedWorkDir = finalizeWorkDir
		}
		if len([]byte(parsed.Result)) > maxStoredResultBytes {
			turn.appendFrame(s.failedFrame(
				turn,
				"result_too_large",
				"runtime result exceeded harness storage limit",
				false,
			))
			return
		}
		if artifactErr := UploadTurnArtifacts(turnCtx, turnArtifactsDir); artifactErr != nil {
			retainedArtifactsDir, retainErr := retainFailedTurnArtifacts(turnArtifactsDir)
			if retainErr != nil {
				// If the isolated move fails, retain the original workspace rather
				// than deleting the only copy of the failed-upload evidence.
				cleanupArtifacts = false
				cleanupWorkspace = false
				retainedArtifactsDir = turnArtifactsDir
			}
			message := fmt.Sprintf(
				"artifact upload failed: %v; artifacts retained at %s",
				artifactErr,
				retainedArtifactsDir,
			)
			if retainErr != nil {
				message = fmt.Sprintf("%s (isolated retention failed: %v)", message, retainErr)
			}
			failed := s.failedFrame(turn, "artifact_upload_failed", message, true)
			failed.Metadata["retainedArtifactsPath"] = retainedArtifactsDir
			turn.appendFrame(failed)
			return
		}
		if finalizedWorkDir != "" {
			if cleanErr := CleanFinalizedWorkDir(ctx, finalizedWorkDir); cleanErr != nil {
				turn.appendFrame(s.runtimeLogTextFrame(
					turn,
					"workdir-cleanup",
					cleanErr.Error(),
				))
			}
		}
		if frameErr := s.appendCompletedFrame(turn, parsed); frameErr != nil {
			turn.appendFrame(s.failedFrame(turn, "result_store_failed", frameErr.Error(), false))
			return
		}
	}
}

func extractHarnessTurnTraceContext(ctx context.Context, request harness.StartTurnRequest) context.Context {
	carrier := tracing.MapCarrier{}
	if request.Metadata != nil {
		carrier["traceparent"] = request.Metadata["traceparent"]
		carrier["tracestate"] = request.Metadata["tracestate"]
	}
	if carrier["traceparent"] == "" {
		return ctx
	}
	return tracing.ExtractContext(ctx, carrier)
}

func turnTerminalFailure(turn *turnState) (bool, string) {
	if turn == nil {
		return false, ""
	}
	turn.mu.Lock()
	defer turn.mu.Unlock()
	for _, v := range slices.Backward(turn.frames) {
		if v.Type == harness.FrameTurnFailed {
			return true, "turn_failed"
		}
	}
	return false, ""
}

func (s *Server) failedTurnPartialResult(ctx context.Context, turnCtx TurnContext, run CommandResult) (string, bool) {
	partial := strings.TrimSpace(run.ExactStdout())
	hasAdapterResult := false
	if resultPath := strings.TrimSpace(run.ResultFile); resultPath != "" {
		if data, err := readBoundedResultFile(resultPath, turnCtx.WorkDir); err == nil &&
			!resultFileUnwritten(data.info) && strings.TrimSpace(data.contents) != "" {
			partial = strings.TrimSpace(data.contents)
			hasAdapterResult = true
		}
	}
	restoreTurnEnv := setTemporaryEnvEntries(turnCtx.Env)
	defer restoreTurnEnv()
	if parsed, err := s.adapter.ParseResult(ctx, turnCtx, run); err == nil && strings.TrimSpace(parsed.Result) != "" {
		partial = strings.TrimSpace(parsed.Result)
		hasAdapterResult = true
	}
	return partial, hasAdapterResult
}

func (s *Server) securityArtifactFollowUp(turn *turnState, base TurnContext) common.SecurityArtifactFollowUp {
	return func(ctx context.Context, prompt string) (string, error) {
		followTurn := base
		followTurn.Prompt = prompt
		followTurn.Env = setEnv(followTurn.Env, workerenv.Prompt, prompt)
		restoreFollowEnv := setTemporaryEnvEntries(followTurn.Env)
		defer restoreFollowEnv()
		spec, err := s.adapter.BuildCommand(ctx, followTurn)
		if err != nil {
			return "", err
		}
		defer removeTempFiles(spec.TempFiles)
		if spec.Dir != "" {
			followTurn.WorkDir = spec.Dir
		}
		turn.appendFrame(s.runtimeLogFrame(turn, "security artifact follow-up started", map[string]any{
			"runtime": s.adapter.Name(),
			"command": path.Base(spec.Path),
		}))
		run, runErr := s.runner.Run(ctx, spec)
		if s.latchChildCredentialProcessCleanupFailure(runErr) {
			return "", runErr
		}
		if strings.TrimSpace(run.Stdout) != "" {
			turn.appendFrame(s.outputFrame(turn, "stdout", run.Stdout))
		}
		if strings.TrimSpace(run.Stderr) != "" {
			turn.appendFrame(s.runtimeLogTextFrame(turn, "stderr", run.Stderr))
		}
		if runErr != nil {
			return strings.TrimSpace(run.Stdout), runErr
		}
		parsed, parseErr := s.adapter.ParseResult(ctx, followTurn, run)
		if parseErr != nil {
			return strings.TrimSpace(run.Stdout), parseErr
		}
		return parsed.Result, nil
	}
}

func turnArtifactDir(workspace preparedWorkspace) string {
	if workspace.baseDir != "" {
		return filepath.Join(workspace.baseDir, "artifacts")
	}
	if workspace.rootDir != "" {
		return filepath.Join(workspace.rootDir, ".orka-runtime-artifacts")
	}
	return filepath.Join(os.TempDir(), "orka-runtime-artifacts")
}

func retainFailedTurnArtifacts(artifactDir string) (string, error) {
	return retainFailedTurnArtifactsIn(artifactDir, os.TempDir())
}

func retainFailedTurnArtifactsIn(artifactDir, retentionParent string) (string, error) {
	failedArtifactRetentionMu.Lock()
	defer failedArtifactRetentionMu.Unlock()

	artifactDir = strings.TrimSpace(artifactDir)
	if artifactDir == "" {
		return "", errors.New("artifact directory is empty")
	}
	artifactDir = filepath.Clean(artifactDir)
	info, err := os.Lstat(artifactDir)
	if err != nil {
		return "", fmt.Errorf("inspect artifact directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("artifact path is not a regular directory")
	}
	retentionParent = strings.TrimSpace(retentionParent)
	if retentionParent == "" {
		return "", errors.New("artifact retention parent is empty")
	}
	retentionParent = filepath.Clean(retentionParent)
	// Prune before moving the current artifacts so a successful retention never
	// takes the cross-turn on-disk set above the configured bound. Scanning the
	// parent also reclaims directories left by an earlier wrapper process.
	if err := pruneFailedTurnArtifactRetentionsLocked(
		retentionParent,
		maxFailedArtifactRetentions-1,
	); err != nil {
		return "", err
	}
	retentionRoot, err := os.MkdirTemp(retentionParent, failedArtifactRetentionPrefix+"*")
	if err != nil {
		return "", fmt.Errorf("create artifact retention directory: %w", err)
	}
	retainedArtifactsDir := filepath.Join(retentionRoot, "artifacts")
	if err := os.Rename(artifactDir, retainedArtifactsDir); err != nil {
		_ = os.Remove(retentionRoot)
		return "", fmt.Errorf("move artifacts to retention directory: %w", err)
	}
	return retainedArtifactsDir, nil
}

func pruneFailedTurnArtifactRetentionsLocked(retentionParent string, keep int) error {
	entries, err := os.ReadDir(retentionParent)
	if err != nil {
		return fmt.Errorf("list artifact retention directories: %w", err)
	}
	type retainedEntry struct {
		name    string
		path    string
		modTime time.Time
	}
	retained := make([]retainedEntry, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), failedArtifactRetentionPrefix) {
			continue
		}
		entryPath := filepath.Join(retentionParent, entry.Name())
		info, infoErr := os.Lstat(entryPath)
		if errors.Is(infoErr, os.ErrNotExist) {
			continue
		}
		if infoErr != nil {
			return fmt.Errorf("inspect artifact retention directory: %w", infoErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			continue
		}
		retained = append(retained, retainedEntry{
			name: entry.Name(), path: entryPath, modTime: info.ModTime(),
		})
	}
	sort.Slice(retained, func(i, j int) bool {
		if retained[i].modTime.Equal(retained[j].modTime) {
			return retained[i].name < retained[j].name
		}
		return retained[i].modTime.Before(retained[j].modTime)
	})
	removeCount := len(retained) - max(keep, 0)
	for i := range removeCount {
		if err := os.RemoveAll(retained[i].path); err != nil {
			return fmt.Errorf("remove old artifact retention directory: %w", err)
		}
	}
	return nil
}

// prepareTurnArtifactsDirForWrapper creates the per-turn artifact directory for
// root-side workspace prep. Child ownership is applied later after the workspace
// tree has been chowned for the child.
func prepareTurnArtifactsDirForWrapper(artifactDir string) error {
	if err := os.MkdirAll(artifactDir, 0o770); err != nil {
		return err
	}
	if os.Geteuid() == 0 {
		if err := os.Lchown(artifactDir, 0, 0); err != nil {
			return err
		}
	}
	return os.Chmod(artifactDir, 0o770)
}

func ensureDirectoryTraversable(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	mode := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	return os.Chmod(dir, mode|0o111)
}

func ensureWorkspaceArtifactsLinkForTurn(rootDir, workDir, artifactDir string) error {
	if workDir == "" || rootDir == "" || workDir == rootDir {
		return nil
	}
	restoreArtifactDir := setTemporaryEnv("ORKA_ARTIFACTS_DIR", artifactDir)
	defer restoreArtifactDir()
	return common.EnsureWorkspaceArtifactsLink(workDir)
}

func (s *Server) frame(turn *turnState, typ harness.FrameType, summary string, terminal any) harness.HarnessEventFrame {
	frame := harness.HarnessEventFrame{
		Version:          harness.ProtocolVersion,
		Type:             typ,
		RuntimeSessionID: turn.runtimeSessionID(),
		TurnID:           turn.id(),
		CorrelationID:    turn.correlationID(),
		CreatedAt:        s.now().UTC(),
		Severity:         events.ExecutionEventSeverityInfo,
		Summary:          events.RedactExecutionEventText(summary),
		Metadata: map[string]string{
			"runtime": s.adapter.Name(),
			"mode":    "observed",
		},
	}
	switch value := terminal.(type) {
	case *harness.TurnCompleted:
		frame.Completed = value
	case *harness.TurnFailed:
		frame.Failed = value
		frame.Severity = events.ExecutionEventSeverityError
	}
	return s.sanitizeTerminalFrame(frame)
}

func (s *Server) normalizeFrame(turn *turnState, frame harness.HarnessEventFrame) harness.HarnessEventFrame {
	if frame.Version == "" {
		frame.Version = harness.ProtocolVersion
	}
	if frame.RuntimeSessionID == "" {
		frame.RuntimeSessionID = turn.runtimeSessionID()
	}
	if frame.TurnID == "" {
		frame.TurnID = turn.id()
	}
	if frame.CorrelationID == "" {
		frame.CorrelationID = turn.correlationID()
	}
	if frame.CreatedAt.IsZero() {
		frame.CreatedAt = s.now().UTC()
	}
	if frame.Severity == "" {
		frame.Severity = events.ExecutionEventSeverityInfo
	}
	frame.Summary = events.RedactExecutionEventText(frame.Summary)
	frame.ContentText = redactAndTruncate(frame.ContentText, events.MaxExecutionEventContentTextChars)
	if len(frame.Metadata) > 0 {
		metadata := make(map[string]string, len(frame.Metadata))
		for key, value := range frame.Metadata {
			metadata[key] = events.RedactExecutionEventText(value)
		}
		frame.Metadata = metadata
	}
	if frame.Completed != nil {
		completed := *frame.Completed
		completed.Result = redactAndTruncateBytes(completed.Result, maxTerminalResultBytes)
		completed.OutputRef = events.RedactExecutionEventText(completed.OutputRef)
		frame.Completed = &completed
	}
	if frame.Failed != nil {
		failed := *frame.Failed
		failed.Reason = events.RedactExecutionEventText(failed.Reason)
		failed.Message = redactAndTruncate(failed.Message, events.MaxExecutionEventSummaryChars)
		failed.Result = redactAndTruncateBytes(failed.Result, 64<<10)
		failed.OutputRef = events.RedactExecutionEventText(failed.OutputRef)
		frame.Failed = &failed
	}
	if frame.Error != nil {
		errorInfo := *frame.Error
		errorInfo.Code = events.RedactExecutionEventText(errorInfo.Code)
		errorInfo.Message = redactAndTruncate(errorInfo.Message, events.MaxExecutionEventSummaryChars)
		frame.Error = &errorInfo
	}
	if len(frame.Content) > 0 {
		if sanitized, _, err := events.SanitizeExecutionEventJSON(frame.Content); err == nil {
			frame.Content = sanitized
		} else {
			frame.Content = nil
		}
	}
	frame = s.sanitizeTerminalFrame(frame)
	return redactHarnessFrameExactValues(frame, turn.exactRedactionValuesSnapshot())
}

func (s *Server) sanitizeTerminalFrame(frame harness.HarnessEventFrame) harness.HarnessEventFrame {
	authValue, authErr := s.currentAuthValue()
	sanitize := func(value string, maxBytes int) string {
		if authErr != nil {
			return ""
		}
		value = events.RedactExecutionEventText(value)
		value = harness.RedactExactBearerValue(value, authValue)
		return truncateBytes(value, maxBytes)
	}
	if frame.Completed != nil {
		completed := *frame.Completed
		completed.Result = sanitize(completed.Result, maxTerminalResultBytes)
		completed.OutputRef = sanitize(completed.OutputRef, 4096)
		frame.Completed = &completed
	}
	if frame.Failed != nil {
		failed := *frame.Failed
		failed.Reason = sanitize(failed.Reason, 64<<10)
		failed.Message = sanitize(failed.Message, 64<<10)
		failed.Result = sanitize(failed.Result, 64<<10)
		failed.OutputRef = sanitize(failed.OutputRef, 4096)
		frame.Failed = &failed
	}
	return frame
}

func (s *Server) runtimeLogFrame(turn *turnState, summary string, content map[string]any) harness.HarnessEventFrame {
	encoded, _ := json.Marshal(content)
	frame := s.frame(turn, harness.FrameRuntimeLog, summary, nil)
	frame.Content = encoded
	return frame
}

func (s *Server) runtimeLogTextFrame(turn *turnState, stream, text string) harness.HarnessEventFrame {
	frame := s.runtimeLogFrame(turn, "runtime "+stream, map[string]any{"stream": stream})
	frame.ContentText = redactAndTruncate(text, events.MaxExecutionEventContentTextChars)
	frame.Severity = events.ExecutionEventSeverityWarning
	return frame
}

func (s *Server) outputFrame(turn *turnState, stream, text string) harness.HarnessEventFrame {
	frame := s.frame(turn, harness.FrameRuntimeOutput, "runtime "+stream, nil)
	frame.ContentText = redactAndTruncate(text, events.MaxExecutionEventContentTextChars)
	encoded, _ := json.Marshal(map[string]any{"stream": stream, "preview": frame.ContentText})
	frame.Content = encoded
	return frame
}

func (s *Server) appendCompletedFrame(turn *turnState, result TurnResult) error {
	completed, err := s.completedFrame(turn, result)
	if err != nil {
		return err
	}
	turn.appendFrame(completed)
	return nil
}

func (s *Server) completedFrame(turn *turnState, result TurnResult) (harness.HarnessEventFrame, error) {
	outputRef := strings.TrimSpace(result.OutputRef)
	if result.Result != "" && outputRef == "" {
		var err error
		outputRef, err = turn.storeOutput(result.Result)
		if err != nil {
			return harness.HarnessEventFrame{}, err
		}
	}
	previewLimit := maxTerminalResultBytes
	if outputRef != "" {
		previewLimit = 64 << 10
	}
	preview := redactAndTruncateBytes(result.Result, previewLimit)
	frame := s.frame(turn, harness.FrameTurnCompleted, "turn completed", &harness.TurnCompleted{
		Result:        preview,
		OutputRef:     events.RedactExecutionEventText(outputRef),
		RetainSession: false,
	})
	if len(result.Metadata) > 0 {
		exactValues := turn.exactRedactionValuesSnapshot()
		for k, v := range result.Metadata {
			frame.Metadata[redactExactValues(k, exactValues)] = redactExactValues(
				events.RedactExecutionEventText(v),
				exactValues,
			)
		}
	}
	return frame, nil
}

func (s *Server) failedFrame(turn *turnState, reason, message string, retryable bool) harness.HarnessEventFrame {
	return s.frame(turn, harness.FrameTurnFailed, "turn failed", &harness.TurnFailed{
		Reason:    events.RedactExecutionEventText(reason),
		Message:   redactAndTruncate(message, events.MaxExecutionEventSummaryChars),
		Retryable: retryable,
	})
}

func (s *Server) failedFrameWithResult(
	turn *turnState,
	reason string,
	message string,
	result string,
	retryable bool,
) harness.HarnessEventFrame {
	frame := s.failedFrame(
		turn,
		reason,
		redactExactValues(message, turn.exactRedactionValuesSnapshot()),
		retryable,
	)
	if strings.TrimSpace(result) == "" || frame.Failed == nil {
		return frame
	}
	outputRef, err := turn.storeOutput(result)
	if err != nil {
		frame.Metadata["outputRefError"] = events.RedactExecutionEventText(err.Error())
		return frame
	}
	frame.Failed.Result = redactAndTruncateBytes(result, 64<<10)
	frame.Failed.OutputRef = events.RedactExecutionEventText(outputRef)
	return s.sanitizeTerminalFrame(frame)
}

func redactAndTruncate(value string, maxChars int) string {
	out, _, _ := events.RedactAndTruncateExecutionEventText(value, maxChars)
	return out
}

func redactAndTruncateBytes(value string, maxBytes int) string {
	return truncateBytes(events.RedactExecutionEventText(value), maxBytes)
}

func truncateBytes(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(value)) <= maxBytes {
		return value
	}
	if maxBytes <= utf8.RuneLen('…') {
		return "…"
	}
	limit := maxBytes - utf8.RuneLen('…')
	var out strings.Builder
	out.Grow(limit + utf8.RuneLen('…'))
	for _, r := range value {
		w := utf8.RuneLen(r)
		if w < 0 {
			w = len(string(r))
		}
		if out.Len()+w > limit {
			break
		}
		out.WriteRune(r)
	}
	out.WriteRune('…')
	return out.String()
}

func removeControlFiles(workDir string, controlFiles ...string) {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		return
	}
	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return
	}
	for _, controlFile := range controlFiles {
		if strings.TrimSpace(controlFile) == "" {
			continue
		}
		absControlFile, err := filepath.Abs(controlFile)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(absWorkDir, absControlFile)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			continue
		}
		_ = os.Remove(absControlFile)
	}
}

func removeTurnEnv(env []string, names ...string) []string {
	if len(env) == 0 || len(names) == 0 {
		return env
	}
	remove := make(map[string]struct{}, len(names))
	for _, name := range names {
		remove[strings.TrimSpace(name)] = struct{}{}
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, ok := strings.Cut(entry, "=")
		if !ok {
			out = append(out, entry)
			continue
		}
		if _, shouldRemove := remove[name]; shouldRemove {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func writeSafeError(w http.ResponseWriter, status int, message string) {
	harness.WriteError(w, status, events.RedactExecutionEventText(message))
}

type turnIdentity struct {
	namespace        string
	taskName         string
	sessionName      string
	runtimeSessionID harness.RuntimeSessionID
	turnID           harness.HarnessTurnID
	correlationID    string
	deadline         time.Time
}

type turnState struct {
	request  harness.StartTurnRequest
	identity turnIdentity
	ctx      context.Context
	cancel   context.CancelFunc
	now      func() time.Time

	mu                   sync.Mutex
	frames               []harness.HarnessEventFrame
	terminal             bool
	closed               bool
	resultPath           string
	resultRead           bool
	resultKeepUntil      time.Time
	exactRedactionValues []string
}

func newTurnState(request harness.StartTurnRequest, now func() time.Time) *turnState {
	ctx, cancel := context.WithCancel(context.Background())
	return &turnState{
		request:              request,
		identity:             identityFromStartTurnRequest(request),
		ctx:                  ctx,
		cancel:               cancel,
		now:                  now,
		exactRedactionValues: exactTurnInputValues(request.Input.Env),
	}
}

func exactTurnInputValues(env []harness.TurnEnvVar) []string {
	seen := make(map[string]struct{}, len(env))
	values := make([]string, 0, len(env))
	for _, item := range env {
		if item.Value == "" || (isKnownNonCredentialTurnEnv(item.Name) && !hasCredentialURLUserinfo(item.Value)) {
			continue
		}
		if _, ok := seen[item.Value]; ok {
			continue
		}
		seen[item.Value] = struct{}{}
		values = append(values, item.Value)
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return values
}

func exactConfiguredEnvValues(env []string) []string {
	configured := make([]harness.TurnEnvVar, 0, len(env))
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || (!isCredentialTurnEnvName(name) && !hasCredentialURLUserinfo(value)) {
			continue
		}
		configured = append(configured, harness.TurnEnvVar{Name: name, Value: value})
	}
	return exactTurnInputValues(configured)
}

func isCredentialTurnEnvName(name string) bool {
	normalized := strings.NewReplacer("-", "_", ".", "_").Replace(strings.ToUpper(strings.TrimSpace(name)))
	padded := "_" + normalized + "_"
	for _, marker := range []string{
		"_API_KEY_",
		"_APIKEY_",
		"_ACCESS_KEY_",
		"_PAT_",
		"_BASIC_AUTH_",
		"_TOKEN_",
		"_SECRET_",
		"_PASSWORD_",
		"_PASSWD_",
		"_CREDENTIAL_",
		"_PRIVATE_KEY_",
		"_DSN_",
		"_CONNECTION_STRING_",
		"_CONNECTIONSTRING_",
		"_CONN_STRING_",
	} {
		if strings.Contains(padded, marker) {
			return true
		}
	}
	return false
}

func hasCredentialURLUserinfo(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User == nil {
		return false
	}
	if parsed.User.Username() != "" {
		return true
	}
	_, hasPassword := parsed.User.Password()
	return hasPassword
}

func isKnownNonCredentialTurnEnv(name string) bool {
	switch strings.TrimSpace(name) {
	case workerenv.OpenAIBaseURL,
		workerenv.AnthropicBaseURL,
		"CLAUDE_CODE_USE_FOUNDRY",
		workerenv.AnthropicFoundryBaseURL,
		"ANTHROPIC_FOUNDRY_RESOURCE",
		"ANTHROPIC_DEFAULT_SONNET_MODEL",
		"ANTHROPIC_DEFAULT_HAIKU_MODEL",
		"ANTHROPIC_DEFAULT_OPUS_MODEL":
		return true
	default:
		return false
	}
}

func identityFromStartTurnRequest(request harness.StartTurnRequest) turnIdentity {
	return turnIdentity{
		namespace:        request.Namespace,
		taskName:         request.TaskName,
		sessionName:      request.SessionName,
		runtimeSessionID: request.RuntimeSessionID,
		turnID:           request.TurnID,
		correlationID:    request.CorrelationID,
		deadline:         request.Deadline,
	}
}

func (t *turnState) id() harness.HarnessTurnID {
	return t.identity.turnID
}

func (t *turnState) runtimeSessionID() harness.RuntimeSessionID {
	return t.identity.runtimeSessionID
}

func (t *turnState) correlationID() string {
	return t.identity.correlationID
}

func (t *turnState) deadline() time.Time {
	return t.identity.deadline
}

func (t *turnState) materializeContext(runtimeName string, cfg Config) TurnContext {
	t.mu.Lock()
	request := t.request
	t.request.Input.Env = nil
	t.mu.Unlock()
	return turnContextFromRequest(runtimeName, cfg, request)
}

func (t *turnState) storeOutput(result string) (string, error) {
	result = redactExactValues(result, t.exactRedactionValuesSnapshot())
	file, err := os.CreateTemp("", "harness-turn-output-*")
	if err != nil {
		return "", fmt.Errorf("create turn output file: %w", err)
	}
	outputPath := file.Name()
	if _, err := file.WriteString(result); err != nil {
		_ = file.Close()
		_ = os.Remove(outputPath)
		return "", fmt.Errorf("write turn output file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(outputPath)
		return "", fmt.Errorf("close turn output file: %w", err)
	}
	t.mu.Lock()
	oldPath := t.resultPath
	t.resultPath = outputPath
	t.resultRead = false
	t.resultKeepUntil = t.now().Add(max(30*time.Minute, 6*DefaultTurnRetention))
	t.mu.Unlock()
	if oldPath != "" {
		_ = os.Remove(oldPath)
	}
	return localOutputRef, nil
}

func (t *turnState) output() ([]byte, bool, error) {
	t.mu.Lock()
	outputPath := t.resultPath
	t.mu.Unlock()
	if outputPath == "" {
		return nil, false, nil
	}
	file, err := os.Open(outputPath)
	if err != nil {
		return nil, false, fmt.Errorf("open turn output file: %w", err)
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, int64(maxStoredResultBytes)+1))
	if err != nil {
		return nil, false, fmt.Errorf("read turn output file: %w", err)
	}
	if len(data) > maxStoredResultBytes {
		return nil, false, fmt.Errorf("turn output exceeds harness storage limit")
	}
	return data, true, nil
}

func (t *turnState) hasUnfetchedOutput() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resultPath != "" && !t.resultRead
}

func (t *turnState) outputRetentionActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.resultKeepUntil.IsZero() && t.now().Before(t.resultKeepUntil)
}

func (t *turnState) markOutputFetched() {
	t.mu.Lock()
	t.resultRead = true
	t.mu.Unlock()
}

func (t *turnState) cleanupOutput() {
	t.mu.Lock()
	outputPath := t.resultPath
	t.resultPath = ""
	t.mu.Unlock()
	if outputPath != "" {
		_ = os.Remove(outputPath)
	}
}

func (t *turnState) appendFrame(frame harness.HarnessEventFrame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	frame = redactHarnessFrameOutputValues(frame, t.exactRedactionValues)
	if frame.Seq <= 0 {
		frame.Seq = int64(len(t.frames) + 1)
	}
	if frame.Completed != nil && frame.Completed.FinalEventSeq == 0 {
		frame.Completed.FinalEventSeq = frame.Seq
	}
	t.frames = append(t.frames, frame)
	switch frame.Type {
	case harness.FrameTurnCompleted, harness.FrameTurnFailed, harness.FrameTurnCancelled:
		t.terminal = true
	}
}

func (t *turnState) exactRedactionValuesSnapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.exactRedactionValues...)
}

func (t *turnState) addExactRedactionValue(value string) {
	if value == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if slices.Contains(t.exactRedactionValues, value) {
		return
	}
	t.exactRedactionValues = append(t.exactRedactionValues, value)
	sort.Slice(t.exactRedactionValues, func(i, j int) bool {
		return len(t.exactRedactionValues[i]) > len(t.exactRedactionValues[j])
	})
}

func (t *turnState) clearExactRedactionValues() {
	t.mu.Lock()
	for i := range t.exactRedactionValues {
		t.exactRedactionValues[i] = ""
	}
	t.exactRedactionValues = nil
	t.mu.Unlock()
}

func redactExactValues(value string, exactValues []string) string {
	for _, exact := range exactValues {
		value = harness.RedactExactBearerValue(value, exact)
	}
	return value
}

func redactHarnessFrameExactValues(
	frame harness.HarnessEventFrame,
	exactValues []string,
) harness.HarnessEventFrame {
	if len(exactValues) == 0 {
		return frame
	}
	frame = redactHarnessFrameOutputValues(frame, exactValues)
	frame.Summary = redactExactValues(frame.Summary, exactValues)
	frame.ToolName = redactExactValues(frame.ToolName, exactValues)
	frame.ToolCallID = redactExactValues(frame.ToolCallID, exactValues)
	frame.ApprovalID = redactExactValues(frame.ApprovalID, exactValues)
	if len(frame.Metadata) > 0 {
		metadata := make(map[string]string, len(frame.Metadata))
		for key, value := range frame.Metadata {
			metadata[redactExactValues(key, exactValues)] = redactExactValues(value, exactValues)
		}
		frame.Metadata = metadata
	}
	if frame.Completed != nil {
		completed := *frame.Completed
		completed.OutputRef = redactExactValues(completed.OutputRef, exactValues)
		frame.Completed = &completed
	}
	if frame.Failed != nil {
		failed := *frame.Failed
		failed.Reason = redactExactValues(failed.Reason, exactValues)
		failed.Message = redactExactValues(failed.Message, exactValues)
		failed.OutputRef = redactExactValues(failed.OutputRef, exactValues)
		frame.Failed = &failed
	}
	if frame.Error != nil {
		errorInfo := *frame.Error
		errorInfo.Code = redactExactValues(errorInfo.Code, exactValues)
		errorInfo.Message = redactExactValues(errorInfo.Message, exactValues)
		frame.Error = &errorInfo
	}
	return frame
}

// redactHarnessFrameOutputValues scrubs unambiguously child-controlled text and
// result bodies while preserving wrapper-owned protocol text and structural
// fields. Eventing-adapter frames pass through the stricter helper above before
// this sink, because all of their fields are runtime-controlled.
func redactHarnessFrameOutputValues(
	frame harness.HarnessEventFrame,
	exactValues []string,
) harness.HarnessEventFrame {
	if len(exactValues) == 0 {
		return frame
	}
	frame.ContentText = redactExactValues(frame.ContentText, exactValues)
	frame.Content = redactExactJSON(frame.Content, exactValues)
	if frame.Completed != nil {
		completed := *frame.Completed
		completed.Result = redactExactValues(completed.Result, exactValues)
		completed.Data = redactExactData(completed.Data, exactValues)
		completed.Artifacts = redactExactArtifacts(completed.Artifacts, exactValues)
		frame.Completed = &completed
	}
	if frame.Failed != nil {
		failed := *frame.Failed
		failed.Result = redactExactValues(failed.Result, exactValues)
		failed.Data = redactExactData(failed.Data, exactValues)
		failed.Artifacts = redactExactArtifacts(failed.Artifacts, exactValues)
		frame.Failed = &failed
	}
	return frame
}

func redactExactJSON(value json.RawMessage, exactValues []string) json.RawMessage {
	if len(value) == 0 {
		return nil
	}
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil
	}
	redacted := redactExactJSONValue(decoded, exactValues)
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return nil
	}
	return encoded
}

func redactExactJSONValue(value any, exactValues []string) any {
	switch typed := value.(type) {
	case string:
		return redactExactValues(typed, exactValues)
	case []any:
		for i := range typed {
			typed[i] = redactExactJSONValue(typed[i], exactValues)
		}
		return typed
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		for key, item := range typed {
			redacted[redactExactValues(key, exactValues)] = redactExactJSONValue(item, exactValues)
		}
		return redacted
	case json.Number:
		text := typed.String()
		if redacted := redactExactValues(text, exactValues); redacted != text {
			return redacted
		}
		return typed
	case bool:
		text := strconv.FormatBool(typed)
		if redacted := redactExactValues(text, exactValues); redacted != text {
			return redacted
		}
		return typed
	case nil:
		if redacted := redactExactValues("null", exactValues); redacted != "null" {
			return redacted
		}
		return nil
	default:
		return value
	}
}

func redactExactData(value map[string]any, exactValues []string) map[string]any {
	if len(value) == 0 {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	redacted := redactExactJSON(encoded, exactValues)
	var out map[string]any
	if len(redacted) == 0 || json.Unmarshal(redacted, &out) != nil {
		return nil
	}
	return out
}

func redactExactArtifacts(
	artifacts []harness.ArtifactRef,
	exactValues []string,
) []harness.ArtifactRef {
	if len(artifacts) == 0 {
		return nil
	}
	out := append([]harness.ArtifactRef(nil), artifacts...)
	for i := range out {
		out[i].Filename = redactExactValues(out[i].Filename, exactValues)
		out[i].ContentType = redactExactValues(out[i].ContentType, exactValues)
		out[i].Description = redactExactValues(out[i].Description, exactValues)
	}
	return out
}

func (t *turnState) framesFrom(seq int64) ([]harness.HarnessEventFrame, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	frames := make([]harness.HarnessEventFrame, 0)
	for _, frame := range t.frames {
		if frame.Seq >= seq {
			frames = append(frames, frame)
		}
	}
	return frames, t.closed
}

func (t *turnState) close() {
	t.mu.Lock()
	t.closed = true
	for i := range t.exactRedactionValues {
		t.exactRedactionValues[i] = ""
	}
	t.exactRedactionValues = nil
	t.mu.Unlock()
}

func (t *turnState) hasTerminal() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.terminal
}

func (t *turnState) terminalFrame() (harness.HarnessEventFrame, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, v := range slices.Backward(t.frames) {
		switch v.Type {
		case harness.FrameTurnCompleted, harness.FrameTurnFailed, harness.FrameTurnCancelled:
			return v, true
		}
	}
	return harness.HarnessEventFrame{}, false
}

func (t *turnState) terminalOutputRef() string {
	frame, found := t.terminalFrame()
	if !found {
		return ""
	}
	if frame.Completed != nil {
		return strings.TrimSpace(frame.Completed.OutputRef)
	}
	if frame.Failed != nil {
		return strings.TrimSpace(frame.Failed.OutputRef)
	}
	return ""
}

func (s *Server) durableTerminalReceipt(turn *turnState) ([]byte, bool) {
	frame, found := turn.terminalFrame()
	receipt := harness.DurableTurnTerminalReceipt{
		Version: harness.ProtocolVersion, RuntimeSessionID: turn.runtimeSessionID(),
		TurnID: turn.id(), CorrelationID: turn.correlationID(),
	}
	outcomeUnknown := true
	if found {
		receipt.Seq = frame.Seq
		if canonical, err := harness.DurableTurnTerminalReceiptFromFrame(frame); err == nil {
			if encoded, err := harness.MarshalDurableTurnTerminalReceipt(canonical); err == nil {
				return encoded, false
			}
		}
	}
	reason := "turn-closed-without-terminal-frame"
	if found {
		reason = "invalid-terminal-frame"
	}
	receipt.Kind = harness.DurableTurnTerminalOutcomeUnknown
	receipt.OutcomeUnknown = &harness.DurableTurnOutcomeUnknownReceipt{Reason: reason}
	encoded, err := harness.MarshalDurableTurnTerminalReceipt(receipt)
	if err == nil {
		return encoded, outcomeUnknown
	}
	// StartTurn validation guarantees the receipt identities. Keep the fallback
	// fail-closed and free of runtime-controlled terminal content.
	receipt.Kind = harness.DurableTurnTerminalOutcomeUnknown
	receipt.Completed = nil
	receipt.Failed = nil
	receipt.Cancelled = nil
	receipt.OutcomeUnknown = &harness.DurableTurnOutcomeUnknownReceipt{Reason: "terminal-receipt-encoding-failed"}
	encoded, _ = harness.MarshalDurableTurnTerminalReceipt(receipt)
	return encoded, true
}

func (t *turnState) matchesCancel(request harness.CancelTurnRequest) error {
	if request.RuntimeSessionID != t.identity.runtimeSessionID {
		return fmt.Errorf("cancel runtime session %q does not match turn runtime session", request.RuntimeSessionID)
	}
	if request.TurnID != t.identity.turnID {
		return fmt.Errorf("cancel turn %q does not match started turn", request.TurnID)
	}
	if request.Namespace != t.identity.namespace ||
		request.TaskName != t.identity.taskName ||
		request.SessionName != t.identity.sessionName {
		return fmt.Errorf("cancel request does not match started turn owner")
	}
	return nil
}
