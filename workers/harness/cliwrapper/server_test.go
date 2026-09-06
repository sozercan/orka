package cliwrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/harness/ledger"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/testutil"
	"github.com/orka-agents/orka/internal/workerenv"
)

const wrapperTestShellPath = "/bin/sh"

type commandRunnerFunc func(context.Context, *CommandSpec) (CommandResult, error)

func (run commandRunnerFunc) Run(ctx context.Context, spec *CommandSpec) (CommandResult, error) {
	return run(ctx, spec)
}

func TestServerHealthCapabilitiesAndAfterSeq(t *testing.T) {
	baseURL, cleanup := startWrapperServer(t, NewFakeAdapter(FakeBehaviorSuccess))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	if health, err := client.Health(context.Background()); err != nil || !health.Ready {
		t.Fatalf("Health() = %#v, %v", health, err)
	}
	caps, err := client.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities(): %v", err)
	}
	if !caps.SupportsCancel || caps.RuntimeName == "" {
		t.Fatalf("Capabilities = %#v, want cancel and runtime", caps)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 2)
	if len(frames) != 3 {
		t.Fatalf("frames after seq 2 = %d, want 3 (%#v)", len(frames), frames)
	}
	if frames[0].Seq != 3 {
		t.Fatalf("first seq = %d, want 3", frames[0].Seq)
	}
}

func TestServerReceiptPersistenceFailureRetainsTurnAndClosesReadiness(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	cfg.TurnRetention = 5 * time.Millisecond
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	request := validWrapperStartTurnRequest()
	durable, err := validateDurableTurnAdmission(request)
	if err != nil {
		t.Fatalf("validate durable admission: %v", err)
	}
	if _, _, err := server.ledger.AdmitTurn(
		context.Background(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
		string(request.RuntimeSessionID), request.CorrelationID,
	); err != nil {
		t.Fatalf("admit durable turn: %v", err)
	}
	if err := server.ledger.MarkTurnAccepted(context.Background(), string(request.TurnID)); err != nil {
		t.Fatalf("mark durable turn accepted: %v", err)
	}
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}
	turn.appendFrame(server.frame(
		turn, harness.FrameTurnCompleted, "turn completed", &harness.TurnCompleted{Result: "ok"},
	))
	if err := server.ledger.Close(); err != nil {
		t.Fatalf("close ledger before terminal receipt: %v", err)
	}

	server.finishTurn(turn)

	if !server.turnRegistry.active(request.TurnID) {
		t.Fatal("receipt persistence failure released the active turn")
	}
	if _, closed := turn.framesFrom(1); closed {
		t.Fatal("receipt persistence failure exposed stream completion")
	}
	time.Sleep(3 * cfg.TurnRetention)
	if !server.turnRegistry.active(request.TurnID) {
		t.Fatal("receipt persistence failure scheduled turn eviction")
	}
	healthRecorder := httptest.NewRecorder()
	healthRequest := httptest.NewRequest(http.MethodGet, harness.HealthPath, nil)
	server.Handler().ServeHTTP(healthRecorder, healthRequest)
	if healthRecorder.Code != http.StatusOK {
		t.Fatalf("health status code = %d, want 200", healthRecorder.Code)
	}
	var health harness.HealthResponse
	if err := json.Unmarshal(healthRecorder.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if health.Ready || health.Status != harness.HealthStatusUnhealthy ||
		health.Metadata["terminalLedger"] != terminalLedgerPersistFailed {
		t.Fatalf("health after receipt persistence failure = %#v", health)
	}
	readinessRecorder := httptest.NewRecorder()
	readinessRequest := httptest.NewRequest(http.MethodGet, harness.ReadinessPath, nil)
	server.Handler().ServeHTTP(readinessRecorder, readinessRequest)
	if readinessRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status code = %d, want 503", readinessRecorder.Code)
	}
	var readiness harness.HealthResponse
	if err := json.Unmarshal(readinessRecorder.Body.Bytes(), &readiness); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if readiness.Ready || readiness.Status != harness.HealthStatusUnhealthy ||
		readiness.Metadata["terminalLedger"] != terminalLedgerPersistFailed {
		t.Fatalf("readiness after receipt persistence failure = %#v", readiness)
	}
}

func TestServerCloseWaitsForTerminalReceiptPersistence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	request := validWrapperStartTurnRequest()
	durable, err := validateDurableTurnAdmission(request)
	if err != nil {
		t.Fatalf("validate durable admission: %v", err)
	}
	if _, _, err := server.ledger.AdmitTurn(
		context.Background(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
		string(request.RuntimeSessionID), request.CorrelationID,
	); err != nil {
		t.Fatalf("admit durable turn: %v", err)
	}
	if err := server.ledger.MarkTurnAccepted(context.Background(), string(request.TurnID)); err != nil {
		t.Fatalf("mark durable turn accepted: %v", err)
	}
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}
	turn.appendFrame(server.frame(
		turn, harness.FrameTurnCompleted, "turn completed", &harness.TurnCompleted{Result: "ok"},
	))

	turn.mu.Lock()
	turnLocked := true
	defer func() {
		if turnLocked {
			turn.mu.Unlock()
		}
	}()
	finishDone := make(chan struct{})
	go func() {
		defer close(finishDone)
		server.finishTurn(turn)
	}()
	eventually(t, 2*time.Second, func() bool {
		if server.ledgerMu.TryLock() {
			server.ledgerMu.Unlock()
			return false
		}
		return true
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- server.Close()
	}()
	eventually(t, 2*time.Second, func() bool {
		if server.ledgerMu.TryRLock() {
			server.ledgerMu.RUnlock()
			return false
		}
		return true
	})
	select {
	case err := <-closeDone:
		t.Fatalf("Close completed before terminal persistence was released: %v", err)
	default:
	}

	turn.mu.Unlock()
	turnLocked = false
	select {
	case <-finishDone:
	case <-time.After(2 * time.Second):
		t.Fatal("finishTurn did not complete after terminal persistence was released")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete after terminal persistence finished")
	}
	if server.ledger != nil {
		t.Fatal("Close retained the admission ledger")
	}
}

func TestServerAcceptanceFailureReconcilesAdmissionWithIndependentContext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	request := validWrapperStartTurnRequest()
	durable, err := validateDurableTurnAdmission(request)
	if err != nil {
		t.Fatalf("validate durable admission: %v", err)
	}
	if _, _, err := server.ledger.AdmitTurn(
		context.Background(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
		string(request.RuntimeSessionID), request.CorrelationID,
	); err != nil {
		t.Fatalf("admit durable turn: %v", err)
	}
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.markDurableTurnAccepted(canceledCtx, turn); err == nil {
		t.Fatal("markDurableTurnAccepted() error = nil, want canceled acceptance failure")
	}

	record, err := server.ledger.GetTurn(context.Background(), string(request.TurnID))
	if err != nil {
		t.Fatalf("GetTurn: %v", err)
	}
	if record.State != ledger.TurnRejected || record.RejectReason != durableAcceptanceRejectionReason {
		t.Fatalf("durable record = %#v, want reconciled rejection", record)
	}
	if server.turnRegistry.active(request.TurnID) {
		t.Fatal("reconciled acceptance failure retained the local turn")
	}
	if !server.admissionLedgerHealthy() {
		t.Fatal("successful admission reconciliation closed readiness")
	}
}

func TestServerCanceledStartTurnReclaimDoesNotPoisonAdmissionLedger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, harness.TurnsPath, strings.NewReader("{"))
	request = request.WithContext(canceledCtx)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("canceled StartTurn status = %d, want 400 after independent reclamation", recorder.Code)
	}
	if !server.admissionLedgerHealthy() {
		t.Fatal("canceled reclamation caller poisoned durable admission")
	}

	retryRecorder := httptest.NewRecorder()
	retryRequest := httptest.NewRequest(http.MethodPost, harness.TurnsPath, strings.NewReader("{"))
	server.Handler().ServeHTTP(retryRecorder, retryRequest)
	if retryRecorder.Code != http.StatusBadRequest {
		t.Fatalf("StartTurn after canceled caller = %d, want 400", retryRecorder.Code)
	}
}

func TestServerChildProcessCleanupFailureRetainsTurnAndClosesAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	request := validWrapperStartTurnRequest()
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}
	server.setChildCredentialProcessError(errChildCredentialProcessCleanupUnproven)
	server.finishTurn(turn)

	if !server.turnRegistry.active(request.TurnID) {
		t.Fatal("child process cleanup failure released the active turn")
	}
	if _, closed := turn.framesFrom(1); closed {
		t.Fatal("child process cleanup failure closed the active turn stream")
	}
	readinessRecorder := httptest.NewRecorder()
	readinessRequest := httptest.NewRequest(http.MethodGet, harness.ReadinessPath, nil)
	server.Handler().ServeHTTP(readinessRecorder, readinessRequest)
	if readinessRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d, want 503", readinessRecorder.Code)
	}
	var readiness harness.HealthResponse
	if err := json.Unmarshal(readinessRecorder.Body.Bytes(), &readiness); err != nil {
		t.Fatalf("decode readiness: %v", err)
	}
	if readiness.Metadata["childCredentialProcesses"] != childCredentialCleanupFailed {
		t.Fatalf("readiness after child cleanup failure = %#v", readiness)
	}

	next := validWrapperStartTurnRequest()
	next.TurnID = "turn-after-child-cleanup-failure"
	next.CorrelationID = "corr-after-child-cleanup-failure"
	nextBody, err := json.Marshal(next)
	if err != nil {
		t.Fatalf("marshal next StartTurn: %v", err)
	}
	startRecorder := httptest.NewRecorder()
	startRequest := httptest.NewRequest(http.MethodPost, harness.TurnsPath, bytes.NewReader(nextBody))
	server.Handler().ServeHTTP(startRecorder, startRequest)
	if startRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("StartTurn after child cleanup failure = %d, want 503", startRecorder.Code)
	}
}

func TestServerSecurityArtifactFollowUpChildCleanupFailureFailsClosed(t *testing.T) {
	t.Setenv(EnvChildUID, "")
	t.Setenv(EnvChildGID, "")
	t.Setenv("ORKA_ARTIFACTS_DIR", filepath.Join(t.TempDir(), "artifacts"))

	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeStdout,
	}
	server, err := NewServer(cfg, NewGenericAdapter(cfg.Generic))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	runnerCalls := 0
	server.runner = commandRunnerFunc(func(_ context.Context, spec *CommandSpec) (CommandResult, error) {
		if spec == nil {
			t.Fatal("command spec is nil")
		}
		runnerCalls++
		switch runnerCalls {
		case 1:
			return CommandResult{Stdout: "initial result", FullStdout: "initial result"}, nil
		case 2:
			return CommandResult{Stdout: "follow-up output must not be exposed"}, fmt.Errorf(
				"%w: test child process drain failure",
				errChildCredentialProcessCleanupUnproven,
			)
		default:
			t.Fatalf("runner calls = %d, want at most 2", runnerCalls)
			return CommandResult{}, nil
		}
	})

	request := validWrapperStartTurnRequest()
	request.Input.Prompt = "REQUIRED_SECURITY_ARTIFACTS: security-findings.v2.json\nreview the repository"
	request = sealDurableWrapperRequest(request)
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}

	server.runTurn(turn)

	if runnerCalls != 2 {
		t.Fatalf("runner calls = %d, want initial run plus security follow-up", runnerCalls)
	}
	if server.childCredentialProcessesHealthy() {
		t.Fatal("follow-up child process cleanup failure did not close readiness")
	}
	if !server.turnRegistry.active(request.TurnID) {
		t.Fatal("follow-up child process cleanup failure released the active turn")
	}
	frames, closed := turn.framesFrom(1)
	if closed {
		t.Fatal("follow-up child process cleanup failure closed the active turn stream")
	}
	for _, frame := range frames {
		switch frame.Type {
		case harness.FrameTurnCompleted, harness.FrameTurnFailed, harness.FrameTurnCancelled:
			t.Fatalf("follow-up child process cleanup failure exposed terminal frame %#v", frame)
		}
		if strings.Contains(frame.ContentText, "follow-up output must not be exposed") {
			t.Fatalf("follow-up child process cleanup failure exposed subprocess output %#v", frame)
		}
	}

	readiness := server.healthResponse()
	if readiness.Ready || readiness.Metadata["childCredentialProcesses"] != childCredentialCleanupFailed {
		t.Fatalf("readiness after follow-up child cleanup failure = %#v", readiness)
	}

	next := validWrapperStartTurnRequest()
	next.TurnID = "turn-after-follow-up-child-cleanup-failure"
	next.CorrelationID = "corr-after-follow-up-child-cleanup-failure"
	next = sealDurableWrapperRequest(next)
	nextBody, err := json.Marshal(next)
	if err != nil {
		t.Fatalf("marshal next StartTurn: %v", err)
	}
	startRecorder := httptest.NewRecorder()
	startRequest := httptest.NewRequest(http.MethodPost, harness.TurnsPath, bytes.NewReader(nextBody))
	server.Handler().ServeHTTP(startRecorder, startRequest)
	if startRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("StartTurn after follow-up child cleanup failure = %d, want 503", startRecorder.Code)
	}
}

func TestServerArtifactUploadFailureFailsTurnAndRetainsEvidence(t *testing.T) {
	t.Setenv(EnvChildUID, "")
	t.Setenv(EnvChildGID, "")
	t.Setenv(workerenv.ControllerURL, "")

	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeStdout,
	}
	server, err := NewServer(cfg, NewGenericAdapter(cfg.Generic))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	const (
		artifactName = "security-findings.v2.json"
		artifactBody = `{"findings":[{"severity":"high"}]}`
	)
	server.runner = commandRunnerFunc(func(_ context.Context, spec *CommandSpec) (CommandResult, error) {
		if spec == nil {
			t.Fatal("command spec is nil")
		}
		artifactDir := ""
		for _, entry := range spec.Env {
			key, value, ok := strings.Cut(entry, "=")
			if ok && key == "ORKA_ARTIFACTS_DIR" {
				artifactDir = value
				break
			}
		}
		if artifactDir == "" {
			t.Fatal("command env is missing ORKA_ARTIFACTS_DIR")
		}
		if err := os.WriteFile(filepath.Join(artifactDir, artifactName), []byte(artifactBody), 0o640); err != nil {
			t.Fatalf("write test artifact: %v", err)
		}
		return CommandResult{Stdout: "runtime succeeded", FullStdout: "runtime succeeded"}, nil
	})

	request := validWrapperStartTurnRequest()
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}
	server.runTurn(turn)

	frames, closed := turn.framesFrom(1)
	if !closed {
		t.Fatal("artifact upload failure did not close the turn")
	}
	var terminal harness.HarnessEventFrame
	for _, frame := range frames {
		if frame.Type == harness.FrameTurnCompleted {
			t.Fatalf("artifact upload failure emitted TurnCompleted: %#v", frame)
		}
		if frame.Type == harness.FrameTurnFailed {
			terminal = frame
		}
	}
	if terminal.Failed == nil || terminal.Failed.Reason != "artifact_upload_failed" {
		t.Fatalf("terminal frame = %#v, want artifact_upload_failed", terminal)
	}
	retainedArtifactsDir := terminal.Metadata["retainedArtifactsPath"]
	if retainedArtifactsDir == "" {
		t.Fatalf("terminal metadata = %#v, want retained artifact path", terminal.Metadata)
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(retainedArtifactsDir)) })
	retained, err := os.ReadFile(filepath.Join(retainedArtifactsDir, artifactName))
	if err != nil {
		t.Fatalf("read retained artifact: %v", err)
	}
	if string(retained) != artifactBody {
		t.Fatalf("retained artifact = %q, want %q", retained, artifactBody)
	}
}

func TestRetainFailedTurnArtifactsBoundsConcurrentRetentions(t *testing.T) {
	retentionParent := t.TempDir()
	sourceParent := t.TempDir()
	if err := os.Mkdir(filepath.Join(retentionParent, "unrelated"), 0o700); err != nil {
		t.Fatalf("create unrelated directory: %v", err)
	}

	const extraRetentions = 5
	total := maxFailedArtifactRetentions + extraRetentions
	errorsCh := make(chan error, total)
	var wg sync.WaitGroup
	for i := range total {
		artifactDir := filepath.Join(sourceParent, fmt.Sprintf("turn-%02d", i), "artifacts")
		if err := os.MkdirAll(artifactDir, 0o700); err != nil {
			t.Fatalf("create source artifact directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(artifactDir, "evidence.txt"),
			[]byte(strconv.Itoa(i)),
			0o600,
		); err != nil {
			t.Fatalf("write source artifact: %v", err)
		}
		wg.Go(func() {
			_, err := retainFailedTurnArtifactsIn(artifactDir, retentionParent)
			errorsCh <- err
		})
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("retain failed turn artifacts: %v", err)
		}
	}

	entries, err := os.ReadDir(retentionParent)
	if err != nil {
		t.Fatalf("list retained artifacts: %v", err)
	}
	retained := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), failedArtifactRetentionPrefix) {
			continue
		}
		retained++
		artifactPath := filepath.Join(retentionParent, entry.Name(), "artifacts", "evidence.txt")
		if _, err := os.ReadFile(artifactPath); err != nil {
			t.Fatalf("read retained evidence %q: %v", artifactPath, err)
		}
	}
	if retained != maxFailedArtifactRetentions {
		t.Fatalf("retained failure directories = %d, want %d", retained, maxFailedArtifactRetentions)
	}
	if _, err := os.Stat(filepath.Join(retentionParent, "unrelated")); err != nil {
		t.Fatalf("unrelated directory was removed: %v", err)
	}
}

func TestServerRetriesTransientAdmissionLedgerReclaimError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	server.setRetryableAdmissionLedgerError(errors.New("temporary reclamation failure"))
	if server.admissionLedgerHealthy() {
		t.Fatal("transient reclamation failure did not close readiness")
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, harness.TurnsPath, strings.NewReader("{"))
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("StartTurn reclaim retry status = %d, want 400", recorder.Code)
	}
	if !server.admissionLedgerHealthy() {
		t.Fatal("successful reclamation retry did not restore readiness")
	}
}

func TestServerAcceptanceReconcileFailureRetainsTurnAndClosesReadiness(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	request := validWrapperStartTurnRequest()
	durable, err := validateDurableTurnAdmission(request)
	if err != nil {
		t.Fatalf("validate durable admission: %v", err)
	}
	if _, _, err := server.ledger.AdmitTurn(
		context.Background(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
		string(request.RuntimeSessionID), request.CorrelationID,
	); err != nil {
		t.Fatalf("admit durable turn: %v", err)
	}
	if err := server.ledger.MarkTurnAccepted(context.Background(), string(request.TurnID)); err != nil {
		t.Fatalf("seed accepted turn: %v", err)
	}
	turn, err := server.turnRegistry.admit(request, server.now)
	if err != nil {
		t.Fatalf("admit active turn: %v", err)
	}
	defer server.turnRegistry.reject(turn)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.markDurableTurnAccepted(canceledCtx, turn); err == nil {
		t.Fatal("markDurableTurnAccepted() error = nil, want ambiguous acceptance failure")
	}

	if !server.turnRegistry.active(request.TurnID) {
		t.Fatal("unreconciled acceptance failure released the local turn")
	}
	readinessRecorder := httptest.NewRecorder()
	readinessRequest := httptest.NewRequest(http.MethodGet, harness.ReadinessPath, nil)
	server.Handler().ServeHTTP(readinessRecorder, readinessRequest)
	if readinessRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status code = %d, want 503", readinessRecorder.Code)
	}
	var readiness harness.HealthResponse
	if err := json.Unmarshal(readinessRecorder.Body.Bytes(), &readiness); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if readiness.Ready || readiness.Status != harness.HealthStatusUnhealthy ||
		readiness.Metadata["admissionLedger"] != admissionLedgerReconcileFailed {
		t.Fatalf("readiness after admission reconciliation failure = %#v", readiness)
	}

	next := validWrapperStartTurnRequest()
	next.TurnID = "turn-after-admission-reconcile-failure"
	next.CorrelationID = "corr-after-admission-reconcile-failure"
	next = sealDurableWrapperRequest(next)
	body, err := json.Marshal(next)
	if err != nil {
		t.Fatalf("marshal next request: %v", err)
	}
	startRecorder := httptest.NewRecorder()
	startRequest := httptest.NewRequest(http.MethodPost, harness.TurnsPath, bytes.NewReader(body))
	server.Handler().ServeHTTP(startRecorder, startRequest)
	if startRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("StartTurn while admission ledger is unhealthy = %d, want 503", startRecorder.Code)
	}
}

func TestServerLocalAdmissionRejectionReconcilesWithIndependentContext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	request := validWrapperStartTurnRequest()
	durable, err := validateDurableTurnAdmission(request)
	if err != nil {
		t.Fatalf("validate durable admission: %v", err)
	}
	if _, _, err := server.ledger.AdmitTurn(
		context.Background(), string(request.TurnID), durable.taskUID, durable.attempt, durable.requestDigest,
		string(request.RuntimeSessionID), request.CorrelationID,
	); err != nil {
		t.Fatalf("admit durable turn: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.markDurableLocalAdmissionRejected(canceledCtx, request.TurnID); err != nil {
		t.Fatalf("markDurableLocalAdmissionRejected(): %v", err)
	}

	record, err := server.ledger.GetTurn(context.Background(), string(request.TurnID))
	if err != nil {
		t.Fatalf("GetTurn: %v", err)
	}
	if record.State != ledger.TurnRejected || record.RejectReason != durableLocalRejectionReason {
		t.Fatalf("durable record = %#v, want local admission rejection", record)
	}
	if !server.admissionLedgerHealthy() {
		t.Fatal("successful local admission reconciliation closed readiness")
	}
}

func TestServerLocalAdmissionReconcileFailureReturnsUnavailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	request := validWrapperStartTurnRequest()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal StartTurn request: %v", err)
	}

	// Hold local admission after the durable AdmitTurn commit, then make the
	// rejection write fail. This deterministically exercises the handler's
	// post-commit reconciliation failure instead of only calling the helper.
	server.turnRegistry.mu.Lock()
	registryLocked := true
	defer func() {
		if registryLocked {
			server.turnRegistry.mu.Unlock()
		}
	}()
	server.turnRegistry.activeTurns = 1

	startRecorder := httptest.NewRecorder()
	startRequest := httptest.NewRequest(http.MethodPost, harness.TurnsPath, bytes.NewReader(body))
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		server.Handler().ServeHTTP(startRecorder, startRequest)
	}()

	eventually(t, 2*time.Second, func() bool {
		record, getErr := server.ledger.GetTurn(context.Background(), string(request.TurnID))
		return getErr == nil && record.State == ledger.TurnAdmitted
	})
	if err := server.ledger.Close(); err != nil {
		t.Fatalf("close admission ledger: %v", err)
	}
	server.turnRegistry.mu.Unlock()
	registryLocked = false

	select {
	case <-startDone:
	case <-time.After(2 * time.Second):
		t.Fatal("StartTurn did not finish after local admission was released")
	}
	server.ledger = nil

	if startRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("StartTurn after durable rejection failure = %d, want 503", startRecorder.Code)
	}
	readinessRecorder := httptest.NewRecorder()
	readinessRequest := httptest.NewRequest(http.MethodGet, harness.ReadinessPath, nil)
	server.Handler().ServeHTTP(readinessRecorder, readinessRequest)
	if readinessRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status code = %d, want 503", readinessRecorder.Code)
	}
	var readiness harness.HealthResponse
	if err := json.Unmarshal(readinessRecorder.Body.Bytes(), &readiness); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if readiness.Ready || readiness.Status != harness.HealthStatusUnhealthy ||
		readiness.Metadata["admissionLedger"] != admissionLedgerReconcileFailed {
		t.Fatalf("readiness after local admission reconciliation failure = %#v", readiness)
	}
}

func TestServerRequiresBearerTokenForTurnEndpoints(t *testing.T) {
	cfg := DefaultConfig()
	const authValue = "auth-value-0123456789abcdef012345"
	cfg.AuthValue = authValue
	cfg.Generic.Command = testEchoCommand
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewFakeAdapter(FakeBehaviorSuccess))
	defer cleanup()

	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unauthenticated StartTurn error = %v, want 401", err)
	}

	authed, err := harness.NewClient(baseURL, harness.WithBearerToken(authValue))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authed.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("authenticated StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, authed, request.TurnID, 0)
	if frames[len(frames)-1].Type != harness.FrameTurnCompleted {
		t.Fatalf("last frame = %#v, want completed", frames[len(frames)-1])
	}
}

func TestServerReloadsBearerTokenFile(t *testing.T) {
	const (
		oldToken = "old-token-0123456789abcdef01234567"
		newToken = "new-token-0123456789abcdef01234567"
	)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(oldToken), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.AuthValueFile = tokenFile
	cfg.AuthValue = oldToken
	cfg.Generic.Command = testEchoCommand
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewFakeAdapter(FakeBehaviorSuccess))
	defer cleanup()

	request := validWrapperStartTurnRequest()
	oldClient, err := harness.NewClient(baseURL, harness.WithBearerToken(oldToken))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldClient.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("old token StartTurn: %v", err)
	}
	collectWrapperFrames(t, oldClient, request.TurnID, 0)

	if err := os.WriteFile(tokenFile, []byte("short-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	stale := validWrapperStartTurnRequest()
	stale.TurnID = harness.HarnessTurnID(string(stale.TurnID) + "-rotated")
	stale = sealDurableWrapperRequest(stale)
	shortClient, err := harness.NewClient(baseURL, harness.WithBearerToken("short-token"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shortClient.StartTurn(context.Background(), stale); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("weak rotated token StartTurn error = %v, want 503", err)
	}

	if err := os.WriteFile(tokenFile, []byte(newToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := oldClient.StartTurn(context.Background(), stale); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("stale token StartTurn error = %v, want 401", err)
	}
	newClient, err := harness.NewClient(baseURL, harness.WithBearerToken(newToken))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newClient.StartTurn(context.Background(), stale); err != nil {
		t.Fatalf("new token StartTurn: %v", err)
	}
}

func TestServerEnforcesSingleConcurrentTurn(t *testing.T) {
	baseURL, cleanup := startWrapperServer(t, NewFakeAdapter(FakeBehaviorCancellation))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	first := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), first); err != nil {
		t.Fatalf("StartTurn(first): %v", err)
	}
	second := validWrapperStartTurnRequest()
	second.TurnID = "turn-b"
	second.CorrelationID = "corr-b"
	if _, err := client.StartTurn(context.Background(), second); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("StartTurn(second) error = %v, want concurrency conflict", err)
	}
	if _, err := client.CancelTurn(context.Background(), harness.CancelTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        first.Namespace,
		TaskName:         first.TaskName,
		SessionName:      first.SessionName,
		RuntimeSessionID: first.RuntimeSessionID,
		TurnID:           first.TurnID,
		CorrelationID:    first.CorrelationID,
		Reason:           "cleanup",
	}); err != nil {
		t.Fatalf("CancelTurn cleanup: %v", err)
	}
}

func TestServerCancelEmitsCancellation(t *testing.T) {
	baseURL, cleanup := startWrapperServer(t, NewFakeAdapter(FakeBehaviorCancellation))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	framesCh := make(chan []harness.HarnessEventFrame, 1)
	errCh := make(chan error, 1)
	go func() {
		frames := []harness.HarnessEventFrame{}
		err := client.StreamFrames(context.Background(), request.TurnID, 0, func(frame harness.HarnessEventFrame) error {
			frames = append(frames, frame)
			return nil
		})
		framesCh <- frames
		errCh <- err
	}()
	time.Sleep(25 * time.Millisecond)
	if _, err := client.CancelTurn(context.Background(), harness.CancelTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        request.Namespace,
		TaskName:         request.TaskName,
		SessionName:      request.SessionName,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		Reason:           "test",
	}); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	select {
	case frames := <-framesCh:
		if err := <-errCh; err != nil {
			t.Fatalf("StreamFrames: %v", err)
		}
		if len(frames) < 2 || frames[len(frames)-1].Type != harness.FrameTurnCancelled {
			t.Fatalf("frames = %#v, want cancelled terminal", frames)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cancel frame")
	}
}

func TestServerRejectsUnsafeTurnPath(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatal(err)
	}
	req := validWrapperStartTurnRequest()
	req.TurnID = "../bad"
	body, _ := json.Marshal(req)
	resp := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, harness.TurnsPath, bytes.NewReader(body))
	server.Handler().ServeHTTP(resp, httpReq)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.Code)
	}
}

func TestServerEvictsCompletedTurnsAfterRetention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = testEchoCommand
	cfg.TurnRetention = 20 * time.Millisecond
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	client, err := harness.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	_ = collectWrapperFrames(t, client, request.TurnID, 0)
	eventually(t, time.Second, func() bool {
		return !server.turnRegistry.active(request.TurnID)
	})
}

// After a turn completes and is evicted, re-issuing StartTurn with the same turn
// ID must be rejected (409 "turn already completed") rather than re-running the
// CLI, so a controller retry cannot duplicate external side effects.
func TestServerRejectsReacceptOfEvictedTurn(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = testEchoCommand
	cfg.TurnRetention = 20 * time.Millisecond
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	client, err := harness.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	_ = collectWrapperFrames(t, client, request.TurnID, 0)
	// Wait until the completed turn is evicted from the active map.
	eventually(t, time.Second, func() bool {
		return !server.turnRegistry.active(request.TurnID)
	})

	// Re-issuing the same turn ID must now be a deterministic conflict, not a new run.
	_, err = client.StartTurn(context.Background(), request)
	if err == nil {
		t.Fatal("re-StartTurn of an evicted turn succeeded, want conflict")
	}
	if !strings.Contains(err.Error(), "turn already completed") {
		t.Fatalf("re-StartTurn error = %v, want 'turn already completed'", err)
	}
	// The turn must NOT have been re-admitted to the active map.
	if server.turnRegistry.active(request.TurnID) {
		t.Fatal("evicted turn was re-admitted to the active map")
	}
}

func TestServerDropsRetainedInputEnvAfterMaterializingContext(t *testing.T) {
	adapter := &envCapturingAdapter{envCh: make(chan []string, 1)}
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = testEchoCommand
	server, err := NewServer(cfg, adapter)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	client, err := harness.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	request := validWrapperStartTurnRequest()
	request.Input.Env = []harness.TurnEnvVar{{Name: "FAKE_SECRET", Value: "secret-value"}}
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	_ = collectWrapperFrames(t, client, request.TurnID, 0)

	select {
	case env := <-adapter.envCh:
		if !containsEnv(env, "FAKE_SECRET=secret-value") {
			t.Fatalf("materialized env = %#v, want delivered request env", env)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not observe materialized turn env")
	}
	turn := server.turnRegistry.lookup(request.TurnID)
	if turn == nil {
		t.Fatal("turn was evicted before retention check")
	}
	if got := retainedInputEnvLen(turn); got != 0 {
		t.Fatalf("retained Input.Env len = %d, want 0 after materialization", got)
	}
}

func TestServerGenericCommandSuccessAndResultFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		Args:       []string{"-c", "cat > prompt.txt; printf result-from-file > result.txt"},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	adapter := NewGenericAdapter(cfg.Generic)
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, adapter)
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Input.Prompt = "prompt value"
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
	if !strings.Contains(last.Completed.Result, "result-from-file") {
		t.Fatalf("completed result = %q, want result file content", last.Completed.Result)
	}
}

func TestServerFailedCommandPreservesAdapterResultFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		Args:       []string{"-c", "printf partial-from-file > result.txt; exit 7"},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	req := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), req); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, req.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Failed == nil || !strings.Contains(last.Failed.Result, "partial-from-file") {
		t.Fatalf("failed frame = %#v, want result file content", last.Failed)
	}
}

func TestServerPreservesCompletedResultBytes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	sensitiveResult := "Authorization: Bearer " + "token-shaped-result-1234567890"
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		Args:       []string{"-c", fmt.Sprintf("printf %q > result.txt", sensitiveResult)},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	adapter := NewGenericAdapter(cfg.Generic)
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, adapter)
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
	if strings.Contains(last.Completed.Result, "token-shaped-result-1234567890") {
		t.Fatalf("completed frame leaked exact token-shaped result: %q", last.Completed.Result)
	}
	data, err := client.FetchTurnOutput(context.Background(), request.TurnID, last.Completed.OutputRef)
	if err != nil {
		t.Fatalf("FetchTurnOutput: %v", err)
	}
	if string(data) != sensitiveResult {
		t.Fatalf("fetched output = %q, want exact result", string(data))
	}
}

func TestServerRedactsCommandOutputFrames(t *testing.T) {
	assertCommandFramesRedacted(t, "printf '"+testBearerHeaderValue()+"'", "frames")
}

func TestServerRedactsConfiguredCommandEnvironment(t *testing.T) {
	const (
		privateName  = "WRAPPER_PRIVATE_SECRET"
		privateValue = "wrapper-config-value-4f739d28b61c"
		publicValue  = "https://public-provider.example.test/v1"
	)
	configuredEnv := []string{
		privateName + "=" + privateValue,
		workerenv.OpenAIBaseURL + "=" + publicValue,
		"FEATURE=true",
		"RETRY_COUNT=1",
	}
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.CommandEnv = append([]string(nil), configuredEnv...)
	cfg.Generic = GenericAdapterConfig{
		Command: wrapperTestShellPath,
		Args: []string{"-c", fmt.Sprintf(
			"printf '%%s %%s %%s %%s' \"$%s\" \"$%s\" \"$FEATURE\" \"$RETRY_COUNT\"; printf '%%s' \"$%s\" >&2; exit 7",
			privateName,
			workerenv.OpenAIBaseURL,
			privateName,
		)},
		Env:        append([]string(nil), configuredEnv...),
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeStdout,
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	encoded, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal frames: %v", err)
	}
	if strings.Contains(string(encoded), privateValue) || !strings.Contains(string(encoded), "[REDACTED]") {
		t.Fatalf("configured private environment leaked or was not redacted: %s", encoded)
	}
	if !strings.Contains(string(encoded), publicValue) {
		t.Fatalf("known public configuration was redacted: %s", encoded)
	}
	if !strings.Contains(string(encoded), "true 1") {
		t.Fatalf("ordinary short configuration values were corrupted: %s", encoded)
	}
}

func TestServerRedactsConfiguredConnectionStringsFromFramesAndOutput(t *testing.T) {
	postgresURL, redisURL, reportingDSN, _ := configuredConnectionStringFixtures()
	const publicURL = "https://public-provider.example.test/v1"
	configuredEnv := []string{
		"DATABASE_URL=" + postgresURL,
		"REDIS_URL=" + redisURL,
		"REPORTING_DSN=" + reportingDSN,
		workerenv.OpenAIBaseURL + "=" + publicURL,
	}
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.CommandEnv = append([]string(nil), configuredEnv...)
	cfg.Generic = GenericAdapterConfig{
		Command: wrapperTestShellPath,
		Args: []string{
			"-c",
			`printf '%s\n%s\n%s\n%s\n' "$DATABASE_URL" "$REDIS_URL" "$REPORTING_DSN" "$OPENAI_BASE_URL"`,
		},
		Env:        append([]string(nil), configuredEnv...),
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeStdout,
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	encoded, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal frames: %v", err)
	}
	for label, sensitive := range map[string]string{
		"Postgres URL":  postgresURL,
		"Redis URL":     redisURL,
		"reporting DSN": reportingDSN,
	} {
		if strings.Contains(string(encoded), sensitive) {
			t.Fatalf("frames leaked configured %s: %s", label, encoded)
		}
	}
	if !strings.Contains(string(encoded), "[REDACTED]") || !strings.Contains(string(encoded), publicURL) {
		t.Fatalf("frames missed redaction or corrupted public configuration: %s", encoded)
	}

	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil || last.Completed.OutputRef == "" {
		t.Fatalf("last frame = %#v, want completed output reference", last)
	}
	output, err := client.FetchTurnOutput(context.Background(), request.TurnID, last.Completed.OutputRef)
	if err != nil {
		t.Fatalf("FetchTurnOutput: %v", err)
	}
	for label, sensitive := range map[string]string{
		"Postgres URL":  postgresURL,
		"Redis URL":     redisURL,
		"reporting DSN": reportingDSN,
	} {
		if bytes.Contains(output, []byte(sensitive)) {
			t.Fatalf("stored output leaked configured %s: %q", label, output)
		}
	}
	if !bytes.Contains(output, []byte("[REDACTED]")) || !bytes.Contains(output, []byte(publicURL)) {
		t.Fatalf("stored output missed redaction or corrupted public configuration: %q", output)
	}
}

func TestExactConfiguredEnvValuesSelectsOnlyCredentialNames(t *testing.T) {
	postgresURL, redisURL, reportingDSN, legacyConnectionString := configuredConnectionStringFixtures()
	values := exactConfiguredEnvValues([]string{
		"FEATURE=true",
		"RETRY_COUNT=1",
		"TOKENIZER_MODEL=tokenizer-v1",
		"AUTH_URL=https://auth.example.test",
		workerenv.OpenAIBaseURL + "=https://public-provider.example.test/v1",
		"PATTERN=not-a-credential",
		"DATABASE_URL=" + postgresURL,
		"REDIS_URL=" + redisURL,
		"REPORTING_DSN=" + reportingDSN,
		"LEGACY_CONNECTION_STRING=" + legacyConnectionString,
		"OPENAI_API_KEY=openai-secret",
		"WRAPPER_PRIVATE_SECRET=private-secret",
		"DB_PASSWORD=password-secret",
		"AWS_ACCESS_KEY_ID=aws-access-key-id-secret",
		"GITHUB_PAT=github-pat-secret",
		"UPSTREAM_BASIC_AUTH=basic-auth-secret-value",
	})
	want := []string{
		"aws-access-key-id-secret",
		"basic-auth-secret-value",
		"github-pat-secret",
		"password-secret",
		"private-secret",
		"openai-secret",
		postgresURL,
		redisURL,
		reportingDSN,
		legacyConnectionString,
	}
	if len(values) != len(want) {
		t.Fatalf("configured exact redaction values = %v, want %v", values, want)
	}
	for _, value := range want {
		if !slices.Contains(values, value) {
			t.Fatalf("configured exact redaction values = %v, missing %q", values, value)
		}
	}
}

func configuredConnectionStringFixtures() (string, string, string, string) {
	fixtureValue := func(name string) string {
		return "synthetic-" + name + "-fixture"
	}
	postgresURL := (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("orka", fixtureValue("postgres")),
		Host:   "postgres.example.test",
		Path:   "/orka",
	}).String()
	redisURL := (&url.URL{
		Scheme: "redis",
		User:   url.UserPassword("", fixtureValue("redis")),
		Host:   "redis.example.test",
		Path:   "/0",
	}).String()
	reportingDSN := strings.Join([]string{
		"host=reporting.example.test",
		"user=orka",
		"password=" + fixtureValue("reporting"),
	}, " ")
	legacyConnectionString := strings.Join([]string{
		"Server=legacy.example.test",
		"User=orka",
		"Password=" + fixtureValue("legacy"),
	}, ";")
	return postgresURL, redisURL, reportingDSN, legacyConnectionString
}

func assertCommandFramesRedacted(t *testing.T, script, label string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = wrapperTestShellPath
	cfg.Generic.Args = []string{"-c", script}
	adapter := NewGenericAdapter(cfg.Generic)
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, adapter)
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	encoded, _ := json.Marshal(frames)
	encodedText := string(encoded)
	if strings.Contains(encodedText, redactionLeakMarker()) ||
		(strings.Contains(encodedText, "Authorization") && !strings.Contains(encodedText, "[REDACTED]")) {
		t.Fatalf("%s leaked secret or missed redaction: %s", label, encoded)
	}
}

func startWrapperServer(t *testing.T, adapter RuntimeAdapter) (string, func()) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = testEchoCommand
	return startWrapperServerWithConfig(t, cfg, adapter)
}

func configureSuccessfulArtifactUpload(t *testing.T) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)
	t.Setenv(workerenv.ControllerURL, server.URL)
}

func startWrapperServerWithConfig(t *testing.T, cfg Config, adapter RuntimeAdapter) (string, func()) {
	t.Helper()
	if !cfg.AllowUnauthenticated && strings.TrimSpace(cfg.AdmissionLedgerPath) == "" {
		cfg.AdmissionLedgerPath = filepath.Join(t.TempDir(), "admission-ledger.db")
	}
	server, err := NewServer(cfg, adapter)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(server.Handler())
	return srv.URL, func() {
		srv.Close()
		if err := server.Close(); err != nil {
			t.Errorf("close wrapper server: %v", err)
		}
	}
}

func eventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok() {
		t.Fatal("condition did not become true before timeout")
	}
}

func collectWrapperFrames(
	t *testing.T,
	client *harness.Client,
	turnID harness.HarnessTurnID,
	afterSeq int64,
) []harness.HarnessEventFrame {
	t.Helper()
	frames := []harness.HarnessEventFrame{}
	if err := client.StreamFrames(context.Background(), turnID, afterSeq, func(frame harness.HarnessEventFrame) error {
		frames = append(frames, frame)
		return nil
	}); err != nil {
		t.Fatalf("StreamFrames: %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("no frames")
	}
	return frames
}

func retainedInputEnvLen(turn *turnState) int {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	return len(turn.request.Input.Env)
}

type envCapturingAdapter struct {
	envCh chan []string
}

func (a *envCapturingAdapter) Name() string { return "env-capturing" }
func (a *envCapturingAdapter) BuildCommand(context.Context, TurnContext) (*CommandSpec, error) {
	return nil, nil
}
func (a *envCapturingAdapter) ParseResult(context.Context, TurnContext, CommandResult) (TurnResult, error) {
	return TurnResult{}, nil
}
func (a *envCapturingAdapter) RunTurn(
	_ context.Context,
	turn TurnContext,
	emit func(harness.HarnessEventFrame) error,
) (TurnResult, error) {
	a.envCh <- append([]string(nil), turn.Env...)
	return TurnResult{}, emit(harness.HarnessEventFrame{
		Type:      harness.FrameTurnCompleted,
		Summary:   "done",
		Completed: &harness.TurnCompleted{Result: "ok"},
	})
}

type eventingSecretAdapter struct{}

func (eventingSecretAdapter) Name() string { return "eventing-secret" }
func (eventingSecretAdapter) BuildCommand(context.Context, TurnContext) (*CommandSpec, error) {
	return nil, nil
}
func (eventingSecretAdapter) ParseResult(context.Context, TurnContext, CommandResult) (TurnResult, error) {
	return TurnResult{}, nil
}
func (eventingSecretAdapter) RunTurn(
	_ context.Context,
	_ TurnContext,
	emit func(harness.HarnessEventFrame) error,
) (TurnResult, error) {
	return TurnResult{}, emit(harness.HarnessEventFrame{
		Type:        harness.FrameTurnCompleted,
		Summary:     "done",
		Completed:   &harness.TurnCompleted{Result: testBearerHeaderValue()},
		Metadata:    map[string]string{"note": testBearerHeaderValue()},
		ContentText: testBearerHeaderValue(),
	})
}

type runtimeAuthTokenEchoAdapter struct{}

func (runtimeAuthTokenEchoAdapter) Name() string { return RuntimeCodex }
func (runtimeAuthTokenEchoAdapter) BuildCommand(_ context.Context, turn TurnContext) (*CommandSpec, error) {
	return &CommandSpec{Path: "test-runtime", Env: append([]string(nil), turn.Env...)}, nil
}
func (runtimeAuthTokenEchoAdapter) ParseResult(
	_ context.Context,
	_ TurnContext,
	run CommandResult,
) (TurnResult, error) {
	return TurnResult{Result: run.ExactStdout()}, nil
}

func TestServerRedactsGeneratedRuntimeAuthProxyTokenAtOutputSinks(t *testing.T) {
	previousBoundary := runtimeAuthChildBoundaryAvailable
	runtimeAuthChildBoundaryAvailable = func() bool { return true }
	t.Cleanup(func() { runtimeAuthChildBoundaryAvailable = previousBoundary })

	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	server, err := NewServer(cfg, runtimeAuthTokenEchoAdapter{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	generatedToken := make(chan string, 1)
	server.runner = commandRunnerFunc(func(_ context.Context, spec *CommandSpec) (CommandResult, error) {
		token := envEntryValue(spec.Env, workerenv.OpenAIAPIKey)
		if token == "" {
			return CommandResult{}, errors.New("child command is missing the runtime-auth proxy credential")
		}
		generatedToken <- token
		output := "generated runtime-auth proxy credential: " + token
		return CommandResult{Stdout: output, FullStdout: output, Stderr: output}, nil
	})
	srv := httptest.NewServer(server.Handler())
	defer func() {
		srv.Close()
		if err := server.Close(); err != nil {
			t.Errorf("close wrapper server: %v", err)
		}
	}()
	client, err := harness.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Metadata["runtimeAuthOnly"] = "true"
	request.Input.Env = []harness.TurnEnvVar{{Name: workerenv.OpenAIAPIKey, Value: "upstream-runtime-credential"}}
	request = sealDurableWrapperRequest(request)
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	var token string
	select {
	case token = <-generatedToken:
	default:
		t.Fatal("runner did not receive the generated runtime-auth proxy credential")
	}
	encoded, err := json.Marshal(frames)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(token)) {
		t.Fatal("frames leaked the generated runtime-auth proxy credential")
	}
	if !bytes.Contains(encoded, []byte("[REDACTED]")) {
		t.Fatal("frames did not redact the generated runtime-auth proxy credential")
	}
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil || last.Completed.OutputRef == "" {
		t.Fatal("turn did not complete with an output reference")
	}
	output, err := client.FetchTurnOutput(context.Background(), request.TurnID, last.Completed.OutputRef)
	if err != nil {
		t.Fatalf("FetchTurnOutput: %v", err)
	}
	if bytes.Contains(output, []byte(token)) {
		t.Fatal("stored output leaked the generated runtime-auth proxy credential")
	}
	if !bytes.Contains(output, []byte("[REDACTED]")) {
		t.Fatal("stored output did not redact the generated runtime-auth proxy credential")
	}
}

func TestServerRedactsEventingAdapterTerminalPayloads(t *testing.T) {
	baseURL, cleanup := startWrapperServer(t, eventingSecretAdapter{})
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	encoded, _ := json.Marshal(frames)
	if strings.Contains(string(encoded), redactionLeakMarker()) || !strings.Contains(string(encoded), "[REDACTED]") {
		t.Fatalf("eventing frames leaked secret or missed redaction: %s", encoded)
	}
}

func TestTurnStateExactRedactsProviderValuesAtFrameAndOutputSinks(t *testing.T) {
	exactValue := "q7Zp4vN8m2L6s0D3f5H9j1K7w4X8c2V6"
	request := validWrapperStartTurnRequest()
	request.Input.Env = []harness.TurnEnvVar{
		{Name: "OPENAI_API_KEY", Value: exactValue},
		{Name: "OPENAI_API_KEY_DUPLICATE", Value: exactValue},
		{Name: "CLAUDE_CODE_USE_FOUNDRY", Value: "1"},
		{Name: "EMPTY_VALUE", Value: ""},
	}
	turn := newTurnState(request, time.Now)
	t.Cleanup(turn.cleanupOutput)

	frame := harness.HarnessEventFrame{
		Type:        harness.FrameRuntimeLog,
		Summary:     "summary " + exactValue,
		ContentText: "stdout 1 " + exactValue,
		Content:     json.RawMessage(`{"ordinary":1,"nested":{"value":"` + exactValue + `"},"items":["` + exactValue + `"]}`),
		ToolName:    "tool-" + exactValue,
		ToolCallID:  "call-" + exactValue,
		ApprovalID:  "approval-" + exactValue,
		Metadata:    map[string]string{"note": "metadata " + exactValue},
		Error:       &harness.ErrorInfo{Code: "error-" + exactValue, Message: "message " + exactValue},
	}
	turn.appendFrame(redactHarnessFrameExactValues(frame, turn.exactRedactionValuesSnapshot()))
	turn.appendFrame(harness.HarnessEventFrame{
		Type: harness.FrameTurnCompleted,
		Completed: &harness.TurnCompleted{
			Result: "result " + exactValue,
			Data: map[string]any{
				"nested": map[string]any{"value": exactValue},
			},
			Artifacts: []harness.ArtifactRef{{
				Filename: "artifact-" + exactValue, ContentType: "type/" + exactValue,
				Description: "description " + exactValue,
			}},
		},
	})
	failedTurn := newTurnState(request, time.Now)
	failedFrame := harness.HarnessEventFrame{
		Type: harness.FrameTurnFailed,
		Failed: &harness.TurnFailed{
			Reason: "reason-" + exactValue, Message: "failed " + exactValue,
			Result: "partial " + exactValue,
			Data:   map[string]any{"value": exactValue},
			Artifacts: []harness.ArtifactRef{{
				Filename: "failed-" + exactValue, Description: "failed description " + exactValue,
			}},
		},
	}
	failedTurn.appendFrame(redactHarnessFrameExactValues(
		failedFrame,
		failedTurn.exactRedactionValuesSnapshot(),
	))

	frames, _ := turn.framesFrom(1)
	failedFrames, _ := failedTurn.framesFrom(1)
	encoded, err := json.Marshal(append(frames, failedFrames...))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), exactValue) || !strings.Contains(string(encoded), "[REDACTED]") {
		t.Fatalf("exact-redacted frames = %s", encoded)
	}
	if !strings.Contains(string(encoded), `stdout 1 [REDACTED]`) || !strings.Contains(string(encoded), `"ordinary":1`) {
		t.Fatalf("exact redaction corrupted non-credential configuration value: %s", encoded)
	}

	if _, err := turn.storeOutput("durable output 1 " + exactValue); err != nil {
		t.Fatal(err)
	}
	output, ok, err := turn.output()
	if err != nil || !ok {
		t.Fatalf("stored output = %q, ok=%v, err=%v", output, ok, err)
	}
	if strings.Contains(string(output), exactValue) || !strings.Contains(string(output), "[REDACTED]") {
		t.Fatalf("exact-redacted durable output = %q", output)
	}
	if string(output) != "durable output 1 [REDACTED]" {
		t.Fatalf("exact redaction corrupted non-credential configuration value: %q", output)
	}

	turn.clearExactRedactionValues()
	if values := turn.exactRedactionValuesSnapshot(); len(values) != 0 {
		t.Fatalf("exact redaction values retained after clear: %v", values)
	}
}

func TestExactTurnInputValuesExcludesOnlyKnownProviderConfiguration(t *testing.T) {
	values := exactTurnInputValues([]harness.TurnEnvVar{
		{Name: workerenv.OpenAIAPIKey, Value: "openai-secret"},
		{Name: "ANTHROPIC_FOUNDRY_API_KEY", Value: "foundry-secret"},
		{Name: "FAKE_SECRET", Value: "opaque-secret"},
		{Name: workerenv.OpenAIBaseURL, Value: "https://openai.example.test"},
		{Name: workerenv.AnthropicBaseURL, Value: "https://anthropic.example.test"},
		{Name: "CLAUDE_CODE_USE_FOUNDRY", Value: "1"},
		{Name: workerenv.AnthropicFoundryBaseURL, Value: "https://foundry.example.test"},
		{Name: "ANTHROPIC_FOUNDRY_RESOURCE", Value: "resource"},
		{Name: "ANTHROPIC_DEFAULT_SONNET_MODEL", Value: "sonnet"},
		{Name: "ANTHROPIC_DEFAULT_HAIKU_MODEL", Value: "haiku"},
		{Name: "ANTHROPIC_DEFAULT_OPUS_MODEL", Value: "opus"},
	})
	want := map[string]bool{
		"openai-secret":  true,
		"foundry-secret": true,
		"opaque-secret":  true,
	}
	if len(values) != len(want) {
		t.Fatalf("exact turn input values = %v, want credential values only", values)
	}
	for _, value := range values {
		if !want[value] {
			t.Fatalf("exact turn input values unexpectedly include %q: %v", value, values)
		}
	}
}

func TestServerStoresOversizedCompletedResultOutOfBand(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	largeResultScript := strings.Join([]string{
		"python3 - <<'PY'",
		"from pathlib import Path",
		"Path('result.txt').write_text('x' * (600 * 1024))",
		"PY",
	}, "\n")
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		Args:       []string{"-c", largeResultScript},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
	if len([]byte(last.Completed.Result)) > maxTerminalResultBytes {
		t.Fatalf("completed preview length = %d, want <= %d", len([]byte(last.Completed.Result)), maxTerminalResultBytes)
	}
	data, err := client.FetchTurnOutput(context.Background(), request.TurnID, last.Completed.OutputRef)
	if err != nil {
		t.Fatalf("FetchTurnOutput: %v", err)
	}
	if len(data) != 600*1024 {
		t.Fatalf("fetched output length = %d, want %d", len(data), 600*1024)
	}
}

func TestServerHandleOutputUsesSafeErrorForReadFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	turn := newTurnState(validWrapperStartTurnRequest(), time.Now)
	turn.resultPath = filepath.Join(t.TempDir(), "sensitive-output-path-token")
	req := httptest.NewRequest(http.MethodGet, "/output?ref="+localOutputRef, nil)
	rec := httptest.NewRecorder()

	server.handleOutput(rec, req, turn)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "sensitive-output-path-token") || strings.Contains(body, turn.resultPath) {
		t.Fatalf("output error leaked internal path: %q", body)
	}
	if !strings.Contains(body, "failed to read turn output") {
		t.Fatalf("output error body = %q, want safe generic message", body)
	}
}

func TestServerRedactsCommandStderrFrames(t *testing.T) {
	assertCommandFramesRedacted(t, "printf '"+testBearerHeaderValue()+"' >&2; exit 7", "stderr frames")
}

func TestServerClassifiesCancelBeforeResultFileParsing(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.WorkDir = dir
	cfg.Generic = GenericAdapterConfig{
		Command:    wrapperTestShellPath,
		Args:       []string{"-c", "dd if=/dev/zero bs=1024 count=600 2>/dev/null | tr '\\000' x > result.txt; sleep 10"},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := client.CancelTurn(context.Background(), harness.CancelTurnRequest{
		Version:          harness.ProtocolVersion,
		Namespace:        request.Namespace,
		TaskName:         request.TaskName,
		SessionName:      request.SessionName,
		RuntimeSessionID: request.RuntimeSessionID,
		TurnID:           request.TurnID,
		CorrelationID:    request.CorrelationID,
		Reason:           "test",
	}); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCancelled {
		t.Fatalf("last frame = %#v, want cancelled before result file parse", last)
	}
}

func TestServerPassesSecurityStageEnvToCodexAdapter(t *testing.T) {
	configureSuccessfulArtifactUpload(t)
	artifactDir := "/tmp/artifacts"
	_ = os.RemoveAll(artifactDir)
	t.Cleanup(func() { _ = os.RemoveAll(artifactDir) })
	dir := t.TempDir()
	fakeCodex := filepath.Join(dir, "codex-security.sh")
	script := `#!/bin/sh
set -eu
mkdir -p /tmp/artifacts
if [ "${ORKA_SECURITY_STAGE:-}" = "threat-model" ]; then
  printf '# threat model\n' > /tmp/artifacts/security-threat-model.md
fi
out=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--output-last-message" ]; then out="$arg"; fi
  prev="$arg"
done
if [ -n "$out" ]; then printf 'done' > "$out"; fi
printf 'done'
`
	if err := os.WriteFile(fakeCodex, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(workerenv.AllowBash, "true")
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Runtime = RuntimeCodex
	adapter := NewCodexAdapter(CodexAdapterConfig{Path: fakeCodex, WorkDir: dir})
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, adapter)
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Input.Prompt = "REQUIRED_SECURITY_ARTIFACTS: security-threat-model.md\nwrite artifact"
	request.Input.Env = []harness.TurnEnvVar{{Name: "ORKA_SECURITY_STAGE", Value: "threat-model"}}
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
}

func TestServerCreatesWorkspaceArtifactLinkAndEnforcesRequiredArtifacts(t *testing.T) {
	configureSuccessfulArtifactUpload(t)
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic = GenericAdapterConfig{
		Command: wrapperTestShellPath,
		Args: []string{"-c", strings.Join([]string{
			"printf 'artifact body' > .orka-artifacts/security-threat-model.md",
			"printf 'done' > result.txt",
		}, "; ")},
		PromptMode: PromptModeStdin,
		ResultMode: ResultModeFile,
		ResultFile: "result.txt",
	}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Input.Prompt = "REQUIRED_SECURITY_ARTIFACTS: security-threat-model.md\nwrite artifact"
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
	if !strings.Contains(last.Completed.Result, "done") {
		t.Fatalf("completed result = %q, want done", last.Completed.Result)
	}
}

func testBearerHeaderValue() string {
	return "Authorization: " + "Bearer " + strings.Join([]string{"redaction", "value", "1234567890"}, "-")
}

func redactionLeakMarker() string {
	return strings.Join([]string{"redaction", "value"}, "-")
}

func TestServerStripsGitCredentialsFromReadOnlyCommandEnv(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = wrapperTestShellPath
	cfg.Generic.Args = []string{"-c", "printf 'github=%s git=%s' \"$GITHUB_TOKEN\" \"$GIT_TOKEN\""}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Metadata = map[string]string{"readOnly": "true"}
	request.Input.Env = []harness.TurnEnvVar{
		{Name: workerenv.GitHubToken, Value: "github-token"},
		{Name: workerenv.GitToken, Value: "git-token"},
	}
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnCompleted || last.Completed == nil {
		t.Fatalf("last frame = %#v, want completed", last)
	}
	if strings.Contains(last.Completed.Result, "token") {
		t.Fatalf("read-only command received git credentials: %q", last.Completed.Result)
	}
}

func TestServerRunTurnEmitsTaskRunSpanFromTraceparentMetadata(t *testing.T) {
	if _, err := tracing.Init("test", false); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	spans := testutil.NewSpanHarness(t)
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	server, err := NewServer(cfg, NewFakeAdapter(FakeBehaviorSuccess))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	client, err := harness.NewClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	parentCtx, parentSpan := tracing.Tracer("test").Start(context.Background(), "controller")
	carrier := tracing.InjectContext(parentCtx)
	forgedCtx, forgedSpan := tracing.Tracer("test").Start(context.Background(), "forged")
	forgedCarrier := tracing.InjectContext(forgedCtx)
	forgedSpan.End()
	request := validWrapperStartTurnRequest()
	request.Metadata = map[string]string{
		"traceparent": carrier.Get("traceparent"),
		"agentName":   "agent-a",
	}
	request.Input.Env = append(request.Input.Env, harness.TurnEnvVar{
		Name:  workerenv.TraceParent,
		Value: forgedCarrier.Get("traceparent"),
	})
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	_ = collectWrapperFrames(t, client, request.TurnID, 0)
	parentSpan.End()

	eventually(t, time.Second, func() bool {
		return testutil.SpanNamed(spans.Recorder.Ended(), "task.run") != nil
	})
	taskRun := testutil.SpanNamed(spans.Recorder.Ended(), "task.run")
	if taskRun == nil {
		t.Fatal("missing task.run span")
	}
	if got, want := taskRun.Parent().SpanID(), parentSpan.SpanContext().SpanID(); got != want {
		t.Fatalf("task.run parent = %s, want controller %s", got, want)
	}
	if got, forged := taskRun.Parent().SpanID(), forgedSpan.SpanContext().SpanID(); got == forged {
		t.Fatalf("task.run used task-supplied forged traceparent %s", forged)
	}
	attrs := testutil.AttributeMap(taskRun)
	if got := attrs[tracing.AttrTaskID].AsString(); got != request.TaskName {
		t.Fatalf("%s = %q", tracing.AttrTaskID, got)
	}
	if got := attrs[tracing.AttrAgentName].AsString(); got != "agent-a" {
		t.Fatalf("%s = %q", tracing.AttrAgentName, got)
	}
}

func TestServerRejectsUnsupportedRuntimeAuthOnlyCommand(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowUnauthenticated = true
	cfg.Generic.Command = wrapperTestShellPath
	cfg.Generic.Args = []string{"-c", "printf 'github=%s git=%s' \"$GITHUB_TOKEN\" \"$GIT_TOKEN\""}
	baseURL, cleanup := startWrapperServerWithConfig(t, cfg, NewGenericAdapter(cfg.Generic))
	defer cleanup()
	client, err := harness.NewClient(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	request := validWrapperStartTurnRequest()
	request.Metadata = map[string]string{"runtimeAuthOnly": "true"}
	request.Input.Env = []harness.TurnEnvVar{
		{Name: workerenv.GitHubToken, Value: "x"},
		{Name: workerenv.GitToken, Value: "y"},
	}
	if _, err := client.StartTurn(context.Background(), request); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	frames := collectWrapperFrames(t, client, request.TurnID, 0)
	last := frames[len(frames)-1]
	if last.Type != harness.FrameTurnFailed || last.Failed == nil || last.Failed.Reason != "runtime_auth_proxy_failed" {
		t.Fatalf("last frame = %#v failed = %#v, want runtime auth proxy failure", last, last.Failed)
	}
	wantMessage := `runtime-auth-only credential proxy does not support runtime "generic"`
	if got, want := last.Failed.Message, wantMessage; got != want {
		t.Fatalf("failed message = %q, want %q", got, want)
	}
}
