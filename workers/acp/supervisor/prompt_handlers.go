package supervisor

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func (s *Server) handleStartPrompt(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.StartPromptRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	minLease := time.Duration(s.cfg.Capabilities.Limits.MinPromptLeaseMillis) * time.Millisecond
	maxLease := time.Duration(s.cfg.Capabilities.Limits.MaxPromptLeaseMillis) * time.Millisecond
	if err := request.ValidateAt(now, minLease, maxLease); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if string(request.Metadata.PromptID) != r.PathValue("promptID") {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "prompt path does not match request", nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	slotHeld := false
	defer func() {
		if slotHeld {
			<-s.promptSlots
		}
	}()

	s.mu.Lock()
	state := s.sessions[sessionID]
	if state == nil || state.creating {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil, false)
		return
	}
	if err := s.validateSessionFence(state, request.Metadata); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	if err := request.MCPAuthorization.ValidateProfile(state.profile); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata,
		sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		writeClassificationError(w, classification)
		return
	}
	if s.drain.Requested || state.drainCleanupScheduled {
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime pool is draining", nil, true)
		return
	}
	if state.descriptor.State != harnessv2.RuntimeSessionStateIdle || (state.prompt != nil && state.prompt.settlement == nil) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeAlreadyAccepted, "runtime session is not idle", nil, false)
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	content, err := promptContentToACP(request.Input.Content)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	injectWriteAmbiguity, faultErr := s.consumeE2EPromptWriteAmbiguityLocked(r.Context(), request, s.cfg.E2EPromptWriteAmbiguityMarker)
	if faultErr != nil {
		s.mu.Unlock()
		slog.Error("record E2E prompt write ambiguity failed", "promptID", request.Metadata.PromptID, "error", faultErr)
		writeError(
			w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned,
			"E2E prompt write ambiguity state failed", nil, false,
		)
		return
	}
	if injectWriteAmbiguity {
		s.mu.Unlock()
		slog.Warn("injecting E2E prompt write ambiguity", "promptID", request.Metadata.PromptID)
		panic(http.ErrAbortHandler)
	}
	select {
	case s.promptSlots <- struct{}{}:
		slotHeld = true
	default:
		s.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, harnessv2.ErrorCodeRateLimited, "runtime pool is at prompt capacity", nil, true)
		return
	}
	prompt := &promptState{
		request:              request,
		operation:            operationRecord(request.Metadata, harnessv2.OperationPhaseRecorded, "", now),
		lease:                request.Lease,
		startedAt:            now,
		permissionRequestIDs: make(map[harnessv2.PermissionRequestID]struct{}),
	}
	state.prompt = prompt
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseRecorded, "", now)
	runtimeSession := state.runtime
	providerProxy := state.providerProxy
	mcpProxy := state.mcpProxy
	if providerProxy == nil ||
		providerProxy.activateWithMaxTurns(string(request.Metadata.PromptID), state.agentConfiguration.MaxTurns, request.Lease.ExpiresAt, now) != nil {
		state.prompt = nil
		delete(state.operations, request.Metadata.OperationID)
		s.mu.Unlock()
		writeError(
			w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned,
			"provider proxy activation failed", nil, false,
		)
		return
	}
	if mcpProxy == nil || mcpProxy.activate(request.MCPAuthorization, request.Lease, now) != nil {
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
		state.prompt = nil
		delete(state.operations, request.Metadata.OperationID)
		s.mu.Unlock()
		writeError(
			w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned,
			"MCP proxy activation failed", nil, false,
		)
		return
	}
	stopRequestGate := context.AfterFunc(r.Context(), func() {
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
	})
	defer stopRequestGate()
	s.mu.Unlock()

	run, err := runtimeSession.StartPromptWithLease(
		r.Context(), string(request.Metadata.PromptID), string(request.Metadata.RequestDigest),
		content, request.Lease.ExpiresAt.Sub(now),
	)
	if err != nil {
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
		s.mu.Lock()
		if state.prompt == prompt {
			state.prompt = nil
			delete(state.operations, request.Metadata.OperationID)
		}
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeAlreadyAccepted, safeError(err), nil, false)
		return
	}

	first, ok := <-run.Events
	if !ok {
		result := providerTurnLimitResult(state, prompt, <-run.Result)
		s.finishPrompt(state, prompt, result, time.Now().UTC())
		if result.Accepted {
			writeError(
				w, http.StatusInternalServerError, harnessv2.ErrorCodeOutcomeUnknown,
				"accepted ACP prompt settled without an event stream", nil, false,
			)
		} else {
			writeError(
				w, http.StatusBadGateway, harnessv2.ErrorCodeSessionPoisoned,
				"ACP prompt failed before acceptance", nil, true,
			)
		}
		return
	}

	limits := eventLimits(s.cfg.Capabilities.Limits)
	encoder, err := harnessv2.NewEventEncoder(w, limits, harnessv2.EventExpectationFromMetadata(request.Metadata))
	if err != nil {
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
		_, _ = runtimeSession.CancelPrompt(context.Background(), string(request.Metadata.PromptID))
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, safeError(err), nil, false)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	streamBroken := false
	markStreamBroken := func(stage, eventType, updateKind string, sequence int64, err error) {
		if streamBroken {
			return
		}
		streamBroken = true
		slog.Error(
			"ACP prompt stream broken",
			"stage", stage,
			"promptID", request.Metadata.PromptID,
			"eventType", eventType,
			"updateKind", updateKind,
			"sequence", sequence,
			"errorClass", promptStreamErrorClass(err),
			"validationClass", promptStreamMalformedValidationClass(err),
			"errorDetail", promptStreamErrorDetail(err),
		)
	}
	encodeEvent := func(event harnessv2.Event) {
		if streamBroken {
			return
		}
		if err := encoder.Encode(event); err != nil {
			updateKind := ""
			if event.Update != nil {
				updateKind = string(event.Update.Kind)
			}
			markStreamBroken("event-encode", string(event.Type), updateKind, int64(event.Identity.Sequence), err)
			deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
			cancelCtx, cancel := context.WithTimeout(
				context.Background(), defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace)*2,
			)
			defer cancel()
			_, _ = runtimeSession.CancelPrompt(cancelCtx, string(request.Metadata.PromptID))
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if first.Type != acp.PromptEventAccepted {
		markStreamBroken("first-event", string(first.Type), "", first.Sequence, nil)
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
		_, _ = runtimeSession.CancelPrompt(context.Background(), string(request.Metadata.PromptID))
	}
	mapAndEncode := func(event acp.PromptEvent) {
		mapped, mapErr := s.mapRuntimeEvent(state, prompt, event)
		if mapErr != nil {
			markStreamBroken("event-map", string(event.Type), "", event.Sequence, mapErr)
			deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
			_, _ = runtimeSession.CancelPrompt(context.Background(), string(request.Metadata.PromptID))
			return
		}
		if mapped != nil {
			encodeEvent(*mapped)
		}
	}
	mapAndEncode(first)
	compactor := newAssistantMessageCompactor()
	defer compactor.close()
	events := run.Events
	for events != nil {
		select {
		case event, ok := <-events:
			if !ok {
				for _, pending := range compactor.flushPending() {
					mapAndEncode(pending)
				}
				events = nil
				continue
			}
			if run.Release != nil {
				run.Release(event)
			}
			if withholdAgentDiagnostic(state, prompt, event) {
				continue
			}
			for _, ready := range compactor.push(event, time.Now()) {
				mapAndEncode(ready)
			}
		case <-compactor.timerChannel():
			for _, pending := range compactor.flushPending() {
				mapAndEncode(pending)
			}
		}
	}
	result := <-run.Result
	if result.Err != nil {
		stage, rpcCode, rpcService, rpcErrorName := promptExecutionDiagnostic(result.Err)
		slog.Error(
			"ACP prompt execution failed",
			"stage", stage,
			"rpcCode", rpcCode,
			"rpcService", rpcService,
			"rpcErrorName", rpcErrorName,
			"accepted", result.Accepted,
			"resultOutcome", string(result.Outcome),
			"resultStopReason", string(result.StopReason),
			"errorType", fmt.Sprintf("%T", result.Err),
			// Credential-redacted before bounding (a truncation could cut a
			// credential ahead of the text its recognizer needs), then
			// bounded: the text is the ACP transport/client diagnostic.
			"errorDetail", redactedPromptErrorDetail(result.Err),
		)
	}
	// The ACP child can settle its turn while the provider proxy is still
	// relaying the final bytes of the last inference response. A 2xx is only
	// accounted once its body has been relayed, so let in-flight proxy
	// requests drain (bounded) before revoking the prompt's capabilities;
	// otherwise the upstream-failure classification could read a snapshot
	// with an earlier failure and no success yet, and turn a successfully
	// retried prompt into provider_upstream_error.
	// Capabilities close before the drain: a settled child must not start
	// MCP/tool side effects or launch further inference calls while the
	// last provider relay finishes. Only the provider proxy's deactivation
	// waits, and only for responses that were already admitted.
	if state.mcpProxy != nil {
		state.mcpProxy.deactivate(request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
	}
	if state.providerProxy != nil {
		state.providerProxy.closeAdmission(string(request.Metadata.PromptID))
	}
	s.waitProviderProxyDrained(state, prompt)
	if state.providerProxy != nil {
		state.providerProxy.deactivate(string(request.Metadata.PromptID))
	}
	terminal, settledResult, terminalErr := s.terminalEvent(state, prompt, result)
	if settledResult.Outcome == acp.PromptOutcomeFailed {
		outcome, stopReason := promptTerminalDiagnostic(settledResult)
		slog.Error(
			"ACP prompt settled failed",
			"outcome", outcome,
			"stopReason", stopReason,
			"accepted", settledResult.Accepted,
			"errorPresent", settledResult.Err != nil,
		)
	}
	if terminalErr != nil {
		markStreamBroken("terminal-build", string(terminal.Type), "", int64(terminal.Identity.Sequence), terminalErr)
	} else {
		encodeEvent(terminal)
	}
	if !streamBroken {
		if err := encoder.Close(); err != nil {
			slog.Error(
				"ACP prompt stream close failed",
				"promptID", request.Metadata.PromptID,
				"errorClass", promptStreamErrorClass(err),
			)
		}
	}
	s.finishPrompt(state, prompt, settledResult, terminal.Identity.Timestamp)
	slotHeld = false
	<-s.promptSlots
}

func promptContainsE2EWriteAmbiguityMarker(input harnessv2.PromptInput, marker string) bool {
	if marker == "" {
		return false
	}
	for _, block := range input.Content {
		if block.Type == harnessv2.ContentBlockText && strings.Contains(block.Text, marker) {
			return true
		}
	}
	return false
}

// consumeE2EPromptWriteAmbiguityLocked injects the test fault once for each
// operation. A retry of the same HTTP mutation must proceed to the provider so
// live conformance detects it through the provider request count. The caller
// must hold s.mu.
func (s *Server) consumeE2EPromptWriteAmbiguityLocked(
	ctx context.Context,
	request harnessv2.StartPromptRequest,
	marker string,
) (bool, error) {
	if !promptContainsE2EWriteAmbiguityMarker(request.Input, marker) {
		return false, nil
	}
	return s.consumeE2EPromptWriteFaultLocked(ctx, request.Metadata)
}

func promptTerminalDiagnostic(result acp.PromptResult) (string, string) {
	const promptDiagnosticOther = "Other"

	outcome := promptDiagnosticOther
	switch result.Outcome {
	case acp.PromptOutcomeCompleted, acp.PromptOutcomeCancelled, acp.PromptOutcomeFailed, acp.PromptOutcomeOutcomeUnknown:
		outcome = string(result.Outcome)
	}
	stopReason := promptDiagnosticOther
	switch result.StopReason {
	case acp.StopReasonEndTurn, acp.StopReasonMaxTokens, acp.StopReasonMaxTurnRequests,
		acp.StopReasonRefusal, acp.StopReasonCancelled:
		stopReason = string(result.StopReason)
	case "":
		stopReason = ""
	}
	return outcome, stopReason
}

// redactedPromptErrorDetail removes control and format runes before redacting
// the complete error text and bounding it for a log field. Dropping every
// separator reassembles credentials split across lines or tabs so the
// redactor can recognize the complete value.
func redactedPromptErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsControl(r):
			return -1
		case unicode.Is(unicode.Cf, r):
			return -1
		}
		return r
	}, strings.ToValidUTF8(err.Error(), ""))
	return promptStreamErrorDetail(errors.New(redact.SensitiveText(cleaned)))
}

func promptStreamErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	detail := strings.NewReplacer("\n", " ", "\r", " ").Replace(strings.TrimSpace(err.Error()))
	limit := 512
	if len(detail) <= limit {
		return detail
	}
	for limit > 0 && !utf8.RuneStart(detail[limit]) {
		limit--
	}
	return detail[:limit] + "..."
}

func promptStreamErrorClass(err error) string {
	switch {
	case err == nil:
		return "unexpected-event"
	case errors.Is(err, harnessv2.ErrEventRateExceeded):
		return "event-rate-exceeded"
	case errors.Is(err, harnessv2.ErrEventByteRateExceeded):
		return "event-byte-rate-exceeded"
	case errors.Is(err, harnessv2.ErrEventLineTooLarge):
		return "event-line-too-large"
	case errors.Is(err, harnessv2.ErrMalformedEvent):
		return "malformed-event"
	case errors.Is(err, harnessv2.ErrEventIdentityMismatch):
		return "event-identity-mismatch"
	case errors.Is(err, harnessv2.ErrEventSequence):
		return "event-sequence"
	case errors.Is(err, harnessv2.ErrEventAfterTerminal):
		return "event-after-terminal"
	case errors.Is(err, harnessv2.ErrMissingAcceptedEvent):
		return "missing-accepted-event"
	case strings.Contains(err.Error(), "write event"):
		return "event-write"
	case strings.Contains(err.Error(), "omitted toolCallId"):
		return "missing-tool-call-id"
	case strings.Contains(err.Error(), "decode ACP session update"),
		strings.Contains(err.Error(), "decode ACP agent message content"):
		return "update-decode"
	case strings.Contains(err.Error(), "permission"):
		return "permission-map"
	case strings.Contains(err.Error(), "unsupported runtime prompt event"):
		return "unsupported-runtime-event"
	case strings.Contains(err.Error(), "activate prompt-scoped MCP authority"):
		return "mcp-authority"
	case strings.Contains(err.Error(), "terminal") && strings.Contains(err.Error(), "size"):
		return "terminal-size"
	default:
		return "unclassified"
	}
}

func promptStreamMalformedValidationClass(err error) string {
	const promptDiagnosticOther = "Other"

	if !errors.Is(err, harnessv2.ErrMalformedEvent) {
		return ""
	}
	detail := err.Error()
	switch {
	case strings.Contains(detail, "assistant message chunk is required"):
		return "assistant-empty"
	case strings.Contains(detail, "assistant message chunk exceeds"):
		return "assistant-too-large"
	case strings.Contains(detail, "assistant message chunk contains invalid UTF-8"):
		return "assistant-invalid-utf8"
	case strings.Contains(detail, "update event must carry exactly one typed payload"),
		strings.Contains(detail, "update requires"):
		return "update-payload"
	case strings.Contains(detail, "tool call ID"):
		return "tool-call-id"
	case strings.Contains(detail, "tool call title"):
		return "tool-call-title"
	case strings.Contains(detail, "tool call kind"):
		return "tool-call-kind"
	case strings.Contains(detail, "unsupported tool call status"):
		return "tool-call-status"
	case strings.Contains(detail, "tool call content"):
		return "tool-call-content"
	case strings.Contains(detail, "plan entry count"):
		return "plan-count"
	case strings.Contains(detail, "plan entry content"):
		return "plan-content"
	case strings.Contains(detail, "plan entry priority"):
		return "plan-priority"
	case strings.Contains(detail, "plan entry") && strings.Contains(detail, "unsupported status"):
		return "plan-status"
	case strings.Contains(detail, "diagnostic code"):
		return "diagnostic-code"
	case strings.Contains(detail, "diagnostic message"):
		return "diagnostic-message"
	default:
		return promptDiagnosticOther
	}
}

const promptExecutionStageJSONRPCError = "json-rpc-error"

func promptExecutionDiagnostic(err error) (string, int, string, string) {
	var rpcErr *acp.RPCError
	switch {
	case errors.As(err, &rpcErr):
		var data struct {
			Service   string `json:"service"`
			ErrorName string `json:"errorName"`
		}
		if len(rpcErr.Data) > 0 && json.Unmarshal(rpcErr.Data, &data) == nil {
			return promptExecutionStageJSONRPCError, rpcErr.Code,
				promptExecutionDiagnosticIdentifier(data.Service),
				promptExecutionDiagnosticIdentifier(data.ErrorName)
		}
		return promptExecutionStageJSONRPCError, rpcErr.Code, "", ""
	case errors.Is(err, acp.ErrClosed):
		return "transport-closed", 0, "", ""
	case errors.Is(err, acp.ErrPromptEventBufferOverflow):
		return "event-buffer-overflow", 0, "", ""
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline-exceeded", 0, "", ""
	case errors.Is(err, context.Canceled):
		return "context-canceled", 0, "", ""
	default:
		return "client-error", 0, "", ""
	}
}

func promptExecutionDiagnosticIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 64 {
		return ""
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-' {
			continue
		}
		return ""
	}
	return value
}

func (s *Server) handleRenewLease(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.RenewPromptLeaseRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	maxLease := time.Duration(s.cfg.Capabilities.Limits.MaxPromptLeaseMillis) * time.Millisecond
	if err := request.ValidateAt(now, maxLease); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !pathMatchesPrompt(r, request.Metadata) || !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	s.mu.Lock()
	state := s.sessions[harnessv2.RuntimeSessionID(r.PathValue("sessionID"))]
	if state == nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt is no longer active", nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata,
		sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		replay := state.operationReplays[request.Metadata.OperationID]
		s.mu.Unlock()
		if replay != nil && classification.Class == harnessv2.RequestClassificationDuplicate {
			writeLeaseOperationReplay(w, r, replay, classification)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if state.prompt == nil || state.prompt.settlement != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt is no longer active", nil, false)
		return
	}
	if !promptMetadataMatches(request.Metadata, state.prompt.request.Metadata) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "lease renewal prompt identity does not match the active prompt", nil, false)
		return
	}
	if err := request.MCPAuthorization.ValidateProfile(state.profile); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	if err := harnessv2.ValidatePromptLeaseRenewal(state.prompt.lease, request.Lease, request.ExpectedLeaseGeneration, now, maxLease); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	runtimeSession := state.runtime
	providerProxy := state.providerProxy
	mcpProxy := state.mcpProxy
	if err := runtimeSession.RenewPromptLeaseFor(string(request.Metadata.PromptID), request.Lease.ExpiresAt.Sub(now)); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, safeError(err), nil, false)
		return
	}
	var providerRenewErr, mcpRenewErr error
	if providerProxy != nil {
		providerRenewErr = providerProxy.renew(string(request.Metadata.PromptID), request.Lease.ExpiresAt, now)
	}
	if mcpProxy != nil && providerRenewErr == nil {
		mcpRenewErr = mcpProxy.renew(request.MCPAuthorization, request.Lease, now)
	}
	if providerProxy == nil || mcpProxy == nil || providerRenewErr != nil || mcpRenewErr != nil {
		// The renewal is rejected fail-closed and the prompt is cancelled;
		// record which capability refused so a cancelled prompt can be
		// traced to its cause. Both messages are supervisor-generated.
		slog.Error(
			"ACP prompt lease renewal rejected; cancelling the prompt",
			"promptID", request.Metadata.PromptID,
			"leaseGeneration", request.Lease.Generation,
			"providerProxyPresent", providerProxy != nil,
			"mcpProxyPresent", mcpProxy != nil,
			"providerRenewError", errorString(providerRenewErr),
			"mcpRenewError", errorString(mcpRenewErr),
		)
		if providerProxy != nil {
			providerProxy.revoke()
		}
		if mcpProxy != nil {
			mcpProxy.revoke(harnessv2.RuntimeSessionStateCancelling)
		}
		s.mu.Unlock()
		cancelCtx, cancel := context.WithTimeout(context.Background(), defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace)*2)
		defer cancel()
		_, _ = runtimeSession.CancelPrompt(cancelCtx, string(request.Metadata.PromptID))
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt provider lease is no longer active", nil, false)
		return
	}
	state.prompt.lease = request.Lease
	state.prompt.request.MCPAuthorization = request.MCPAuthorization
	response := harnessv2.PromptLeaseResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, Lease: request.Lease,
	}
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	if state.operationReplays == nil {
		state.operationReplays = make(map[harnessv2.OperationID]*operationReplay)
	}
	done := make(chan struct{})
	close(done)
	state.operationReplays[request.Metadata.OperationID] = &operationReplay{done: done, lease: &response}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolvePermission(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.ResolvePermissionRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !pathMatchesPermission(r, request) || !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	s.mu.Lock()
	state := s.sessions[harnessv2.RuntimeSessionID(r.PathValue("sessionID"))]
	if state == nil || state.prompt == nil || state.prompt.settlement != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt is no longer active", nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata,
		sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	permission, pending := state.permissions[request.RequestID]
	if classification.Class != harnessv2.RequestClassificationFresh {
		replay := state.operationReplays[request.Metadata.OperationID]
		s.mu.Unlock()
		if replay != nil && classification.Class == harnessv2.RequestClassificationDuplicate {
			writePermissionOperationReplay(w, r, replay, classification)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	if !pending {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "permission request is no longer pending", nil, false)
		return
	}

	outcome := acp.CancelledPermissionOutcome()
	var approval *harnessv2.MCPApprovalEvidence
	if request.Decision.Outcome == harnessv2.PermissionDecisionSelected {
		outcome = acp.SelectedPermissionOutcome(request.Decision.OptionID)
		optionKind := permission.options[request.Decision.OptionID]
		if optionKind == harnessv2.PermissionOptionAllowOnce || optionKind == harnessv2.PermissionOptionAllowAlways {
			if state.mcpProxy == nil {
				s.mu.Unlock()
				writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "permission cannot authorize an MCP tool", nil, false)
				return
			}
			toolName, resolveErr := state.mcpProxy.resolveApprovalToolName(permission.toolName, permission.title)
			if resolveErr != nil {
				s.mu.Unlock()
				writeError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "permission cannot authorize an MCP tool", nil, false)
				return
			}
			approval = &harnessv2.MCPApprovalEvidence{
				PermissionRequestID: request.RequestID, ToolCallID: permission.toolCallID, ToolName: toolName,
				GrantedAt: now, ExpiresAt: permission.expiresAt, Reusable: optionKind == harnessv2.PermissionOptionAllowAlways,
			}
		}
	}
	replay := reserveOperationReplayLocked(state, request.Metadata, now)
	mutations := state.promptMutations
	mcpProxy := state.mcpProxy
	s.mu.Unlock()

	if mutations == nil {
		failure := operationFailure{status: http.StatusInternalServerError, code: harnessv2.ErrorCodeSessionPoisoned, message: "runtime permission resolver is unavailable"}
		s.completeOperationFailure(replay, failure)
		writeError(w, failure.status, failure.code, failure.message, nil, failure.retryable)
		return
	}
	if approval != nil {
		if err := mcpProxy.grantApproval(request.Metadata.PromptID, *approval); err != nil {
			failure := operationFailure{status: http.StatusForbidden, code: harnessv2.ErrorCodeForbidden, message: "permission cannot authorize an MCP tool"}
			s.completeOperationFailure(replay, failure)
			writeError(w, failure.status, failure.code, failure.message, nil, failure.retryable)
			return
		}
	}
	if err := mutations.ResolvePermission(string(request.Metadata.PromptID), string(request.RequestID), outcome); err != nil {
		if mcpProxy != nil {
			mcpProxy.revoke(harnessv2.RuntimeSessionStatePoisoned)
		}
		failure := operationFailure{status: http.StatusGone, code: harnessv2.ErrorCodeSettled, message: safeError(err)}
		s.completeOperationFailure(replay, failure)
		writeError(w, failure.status, failure.code, failure.message, nil, failure.retryable)
		return
	}

	resolvedAt := time.Now().UTC()
	response := harnessv2.PermissionResolutionResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		State: harnessv2.PermissionResolutionApplied, Decision: request.Decision, ResolvedAt: resolvedAt,
	}
	s.mu.Lock()
	delete(state.permissions, request.RequestID)
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseApplied, "", resolvedAt)
	replay.permission = &response
	close(replay.done)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelPrompt(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.CancelPromptRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !pathMatchesPrompt(r, request.Metadata) || !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	s.mu.Lock()
	state := s.sessions[harnessv2.RuntimeSessionID(r.PathValue("sessionID"))]
	if state == nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt is not active", nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata,
		sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		replay := state.operationReplays[request.Metadata.OperationID]
		s.mu.Unlock()
		if replay != nil && (classification.Class == harnessv2.RequestClassificationDuplicate || classification.Class == harnessv2.RequestClassificationSettled) {
			writeCancellationOperationReplay(w, r, replay, classification)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if state.prompt == nil {
		s.mu.Unlock()
		slog.Info("ACP prompt cancellation rejected: no active prompt", "promptID", request.Metadata.PromptID, "reason", request.Reason)
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt is not active", nil, false)
		return
	}
	if !promptMetadataMatches(request.Metadata, state.prompt.request.Metadata) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "cancellation prompt identity does not match the active prompt", nil, false)
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	invalidated := uint32(len(state.permissions))
	replay := reserveOperationReplayLocked(state, request.Metadata, now)
	mutations := state.promptMutations
	providerProxy := state.providerProxy
	mcpProxy := state.mcpProxy
	prompt := state.prompt
	s.mu.Unlock()
	if mutations == nil {
		failure := operationFailure{status: http.StatusInternalServerError, code: harnessv2.ErrorCodeSessionPoisoned, message: "runtime cancellation is unavailable"}
		s.completeOperationFailure(replay, failure)
		writeError(w, failure.status, failure.code, failure.message, nil, failure.retryable)
		return
	}
	if providerProxy != nil || mcpProxy != nil {
		deactivatePromptCapabilities(state, request.Metadata.PromptID, harnessv2.RuntimeSessionStateCancelling)
	}
	cancelCtx, cancel := context.WithDeadline(r.Context(), request.SettlementDeadline)
	defer cancel()
	result, cancelErr := mutations.CancelPrompt(cancelCtx, string(request.Metadata.PromptID))
	if cancelErr != nil && result.Outcome == "" {
		result = acp.PromptResult{Outcome: acp.PromptOutcomeOutcomeUnknown, Accepted: true, Err: cancelErr, SettledAt: time.Now().UTC()}
	}
	if cancelErr != nil || result.Outcome == acp.PromptOutcomeOutcomeUnknown {
		slog.Warn("ACP prompt cancellation did not settle cleanly",
			"promptID", request.Metadata.PromptID, "reason", request.Reason, "outcome", result.Outcome,
			"errorClass", promptStreamErrorClass(cancelErr), "deadlineExceeded", errors.Is(cancelErr, context.DeadlineExceeded))
	}
	settlement := settlementFromResult(result, time.Now().UTC())
	forced := cancelErr != nil && result.Outcome == acp.PromptOutcomeOutcomeUnknown

	s.mu.Lock()
	settlement = settlePromptLocked(prompt, settlement)
	if settlement.TerminalEvent != harnessv2.EventOutcomeUnknown {
		forced = false
	}
	if state.prompt == prompt {
		state.permissions = make(map[harnessv2.PermissionRequestID]permissionState)
	}
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settlement.SettledAt)
	response := cancellationResponse(harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, settlement, invalidated, forced)
	replay.cancellation = &response
	close(replay.done)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}

func reserveOperationReplayLocked(
	state *sessionState,
	metadata harnessv2.MutationMetadata,
	at time.Time,
) *operationReplay {
	if state.operationReplays == nil {
		state.operationReplays = make(map[harnessv2.OperationID]*operationReplay)
	}
	replay := &operationReplay{done: make(chan struct{})}
	state.operationReplays[metadata.OperationID] = replay
	recordSessionOperationLocked(state, metadata, harnessv2.OperationPhaseRecorded, "", at)
	return replay
}

func (s *Server) completeOperationFailure(replay *operationReplay, failure operationFailure) {
	s.mu.Lock()
	replay.failure = &failure
	close(replay.done)
	s.mu.Unlock()
}

// writeOperationReplay waits for a reserved operation replay to settle, then
// writes the recorded failure or the replayed response with a
// duplicate-classification stamp applied. extract selects which stored replay
// response applies; stamp applies the endpoint-specific classification/state
// to the copied response before it is written.
func writeOperationReplay[T any](
	w http.ResponseWriter,
	r *http.Request,
	replay *operationReplay,
	classification harnessv2.Classification,
	extract func(*operationReplay) *T,
	stamp func(*T),
) {
	select {
	case <-replay.done:
	case <-r.Context().Done():
		return
	}
	if replay.failure != nil {
		writeError(w, replay.failure.status, replay.failure.code, replay.failure.message, &classification, replay.failure.retryable)
		return
	}
	stored := extract(replay)
	if stored == nil {
		writeClassificationError(w, classification)
		return
	}
	response := *stored
	stamp(&response)
	writeJSON(w, http.StatusOK, response)
}

func writePermissionOperationReplay(
	w http.ResponseWriter,
	r *http.Request,
	replay *operationReplay,
	classification harnessv2.Classification,
) {
	writeOperationReplay(w, r, replay, classification,
		func(replay *operationReplay) *harnessv2.PermissionResolutionResponse { return replay.permission },
		func(response *harnessv2.PermissionResolutionResponse) {
			response.Classification = harnessv2.Classification{Class: harnessv2.RequestClassificationDuplicate, Phase: harnessv2.OperationPhaseApplied}
			response.State = harnessv2.PermissionResolutionAlreadyResolved
		})
}

func writeLeaseOperationReplay(
	w http.ResponseWriter,
	r *http.Request,
	replay *operationReplay,
	classification harnessv2.Classification,
) {
	writeOperationReplay(w, r, replay, classification,
		func(replay *operationReplay) *harnessv2.PromptLeaseResponse { return replay.lease },
		func(response *harnessv2.PromptLeaseResponse) {
			response.Classification = harnessv2.Classification{Class: harnessv2.RequestClassificationDuplicate, Phase: harnessv2.OperationPhaseApplied}
		})
}

func writeCancellationOperationReplay(
	w http.ResponseWriter,
	r *http.Request,
	replay *operationReplay,
	classification harnessv2.Classification,
) {
	writeOperationReplay(w, r, replay, classification,
		func(replay *operationReplay) *harnessv2.CancelPromptResponse { return replay.cancellation },
		func(response *harnessv2.CancelPromptResponse) {
			response.Classification = harnessv2.Classification{
				Class:         harnessv2.RequestClassificationSettled,
				Phase:         harnessv2.OperationPhaseSettled,
				TerminalEvent: response.Settlement.TerminalEvent,
			}
		})
}

func (s *Server) handleFinalizeSessionPublication(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.FinalizeRuntimeSessionPublicationRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	s.mu.Lock()
	state := s.sessions[sessionID]
	if state == nil || state.creating {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil, false)
		return
	}
	if err := s.validateSessionFence(state, request.Metadata); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata,
		sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		descriptor, receipt := state.descriptor, state.publicationFinalization
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && descriptor.State == harnessv2.RuntimeSessionStateFinalizing && receipt != nil {
			writeJSON(w, http.StatusOK, harnessv2.FinalizeRuntimeSessionPublicationResponse{
				Protocol: harnessv2.ProtocolVersion, Classification: classification, Session: descriptor, Finalization: *receipt,
			})
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if failure := publicationFinalizationPreconditionFailure(state, request); failure != nil {
		s.mu.Unlock()
		writeError(w, failure.status, failure.code, failure.message, nil, failure.retryable)
		return
	}
	receipt := harnessv2.PublicationFinalizationReceipt{
		WorkspaceDeltaID: request.WorkspaceDeltaID, PublicationID: request.PublicationID,
		PublicationGeneration: request.PublicationGeneration, PublicationVersion: request.PublicationVersion,
		TerminalState: request.TerminalState, TerminalReceiptDigest: request.TerminalReceiptDigest, AppliedAt: now,
	}
	if state.descriptor.State == harnessv2.RuntimeSessionStateFinalizing {
		if state.publicationFinalization == nil || !publicationFinalizationMatches(*state.publicationFinalization, receipt) {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "publication finalization receipt conflicts with the accepted receipt", nil, false)
			return
		}
		receipt = *state.publicationFinalization
	} else {
		if state.descriptor.State != harnessv2.RuntimeSessionStatePublicationPrepared {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "runtime session is not awaiting publication finalization", nil, false)
			return
		}
		if err := harnessv2.ValidateRuntimeSessionTransition(state.descriptor.State, harnessv2.RuntimeSessionStateFinalizing); err != nil {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
			return
		}
		state.descriptor.State = harnessv2.RuntimeSessionStateFinalizing
		state.publicationFinalization = &receipt
	}
	state.descriptor.LastTransitionAt = now
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseApplied, "", now)
	descriptor := state.descriptor
	mcpProxy := state.mcpProxy
	drainCleanup := s.drain.Requested && !state.drainCleanupScheduled
	if drainCleanup {
		state.drainCleanupScheduled = true
	}
	s.mu.Unlock()
	if mcpProxy != nil {
		mcpProxy.revoke(harnessv2.RuntimeSessionStateFinalizing)
	}
	writeJSON(w, http.StatusOK, harnessv2.FinalizeRuntimeSessionPublicationResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		Session: descriptor, Finalization: receipt,
	})
	if drainCleanup {
		go s.cleanupDrainedSession(sessionID, state)
	}
}

func publicationFinalizationPreconditionFailure(
	state *sessionState,
	request harnessv2.FinalizeRuntimeSessionPublicationRequest,
) *operationFailure {
	if state.workspaceIntent != harnessv2.WorkspaceIntentWrite || state.prompt == nil || state.prompt.settlement == nil ||
		!promptMetadataMatches(request.Metadata, state.prompt.request.Metadata) {
		return &operationFailure{
			status: http.StatusConflict, code: harnessv2.ErrorCodeSessionPoisoned,
			message: "runtime session is not ready for publication finalization",
		}
	}
	deltaResponse, ok := state.deltas[request.WorkspaceDeltaID]
	if !ok || deltaResponse.Delta.State != harnessv2.WorkspaceDeltaPrepared || !deltaResponse.Delta.PublicationSafe ||
		deltaResponse.Delta.RuntimeSessionUID != state.descriptor.RuntimeSessionUID ||
		deltaResponse.Delta.SessionGeneration != state.descriptor.Generation {
		return &operationFailure{
			status: http.StatusConflict, code: harnessv2.ErrorCodeDigestConflict,
			message: "publication finalization does not match the prepared workspace delta",
		}
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		return &operationFailure{status: http.StatusConflict, code: harnessv2.ErrorCodeSessionPoisoned, message: err.Error()}
	}
	return nil
}

func publicationFinalizationMatches(a, b harnessv2.PublicationFinalizationReceipt) bool {
	return a.WorkspaceDeltaID == b.WorkspaceDeltaID && a.PublicationID == b.PublicationID &&
		a.PublicationGeneration == b.PublicationGeneration && a.PublicationVersion == b.PublicationVersion &&
		a.TerminalState == b.TerminalState && a.TerminalReceiptDigest == b.TerminalReceiptDigest
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.DeleteRuntimeSessionRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	s.mu.Lock()
	state := s.sessions[sessionID]
	if state == nil {
		tombstone, ok := s.tombstones[request.Metadata.Fence.RuntimeSessionUID]
		if !ok || tombstone.RuntimeSessionGeneration != request.Metadata.Fence.RuntimeSessionGeneration ||
			tombstone.RuntimeProfileDigest != request.Metadata.Fence.RuntimeProfileDigest {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil, false)
			return
		}
		record := tombstoneOperation(tombstone, request.Metadata.OperationID)
		if record == nil {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "runtime session not found", nil, false)
			return
		}
		classification, classifyErr := harnessv2.ClassifyOperation(
			s.expectedFence(tombstone.RuntimeSessionUID, tombstone.RuntimeSessionGeneration), request.Metadata, record, true, now,
		)
		s.mu.Unlock()
		if classifyErr != nil {
			writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, classifyErr.Error(), nil, false)
			return
		}
		if classification.Class != harnessv2.RequestClassificationDuplicate || classification.Phase != harnessv2.OperationPhaseDeleted {
			writeClassificationError(w, classification)
			return
		}
		writeJSON(w, http.StatusOK, harnessv2.DeleteRuntimeSessionResponse{
			Protocol: harnessv2.ProtocolVersion, Classification: classification, State: harnessv2.RuntimeSessionStateDeleted, Tombstone: tombstone,
		})
		return
	}
	if err := s.validateSessionFence(state, request.Metadata); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	if state.descriptor.State == harnessv2.RuntimeSessionStateDeleting {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeAlreadyAccepted, "runtime session cleanup is already in progress", nil, true)
		return
	}
	publicationFinalized := state.descriptor.State == harnessv2.RuntimeSessionStateFinalizing && state.publicationFinalization != nil
	classification, err := harnessv2.ClassifyOperation(s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata, sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now)
	if err != nil || classification.Class != harnessv2.RequestClassificationFresh {
		s.mu.Unlock()
		if err != nil {
			writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, 0); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	ready, transitionErr := prepareSessionDeletionLocked(state, publicationFinalized, now)
	if transitionErr != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, transitionErr.Error(), nil, false)
		return
	}
	promptCancellation, cancellationErr := promptCancellationForDeletionLocked(state)
	if cancellationErr != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, cancellationErr.Error(), nil, false)
		return
	}
	if !ready {
		s.mu.Unlock()
		s.cancelPromptForDeletion(r.Context(), promptCancellation)
		writeError(
			w, http.StatusConflict, harnessv2.ErrorCodeAlreadyAccepted,
			"runtime session cancellation must settle before deletion; retry after settlement", nil, true,
		)
		return
	}
	runtimeSession := state.runtime
	providerProxy := state.providerProxy
	s.mu.Unlock()
	var providerCleanupErr error
	if state.mcpProxy != nil {
		state.mcpProxy.close()
	}
	if providerProxy != nil {
		providerProxy.close()
		providerCleanupErr = providerProxy.wait(r.Context())
	}
	cleanup, deleteErr := runtimeSession.Delete(r.Context())
	if providerCleanupErr != nil || deleteErr != nil || !cleanup.Proven {
		s.poisonPool("descendant_cleanup_unproven")
		s.mu.Lock()
		state.descriptor.State = harnessv2.RuntimeSessionStatePoisoned
		state.descriptor.LastTransitionAt = time.Now().UTC()
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "runtime descendant cleanup could not be proven", nil, false)
		return
	}
	if err := acp.ReclaimSessionOwnership(state.paths.Root); err != nil {
		slog.Error("ACP runtime session deletion failed", "stage", "ownership reclaim")
		s.poisonPool("session_root_ownership_reclaim_unproven")
		s.mu.Lock()
		state.descriptor.State = harnessv2.RuntimeSessionStatePoisoned
		state.descriptor.LastTransitionAt = time.Now().UTC()
		s.mu.Unlock()
		writeError(
			w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned,
			"runtime session filesystem ownership reclaim could not be proven", nil, false,
		)
		return
	}
	if err := os.RemoveAll(state.paths.Root); err != nil {
		s.poisonPool("session_root_cleanup_unproven")
		s.mu.Lock()
		state.descriptor.State = harnessv2.RuntimeSessionStatePoisoned
		state.descriptor.LastTransitionAt = time.Now().UTC()
		s.mu.Unlock()
		writeError(
			w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned,
			"runtime session filesystem cleanup could not be proven", nil, false,
		)
		return
	}
	deletedAt := time.Now().UTC()
	s.mu.Lock()
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseDeleted, "", deletedAt)
	pruneSessionOperationsLocked(state, deletedAt)
	operations := make([]harnessv2.OperationRecord, 0, len(state.operations))
	for _, operation := range state.operations {
		operations = append(operations, operation)
	}
	tombstone := harnessv2.RuntimeSessionTombstone{
		RuntimeSessionUID: state.descriptor.RuntimeSessionUID, RuntimeSessionGeneration: state.descriptor.Generation,
		RuntimeProfileDigest: state.descriptor.RuntimeProfileDigest, DeletedAt: deletedAt, Operations: operations,
	}
	delete(s.sessions, sessionID)
	s.pruneTombstonesLocked(deletedAt)
	delete(s.failedCreates, tombstone.RuntimeSessionUID)
	s.tombstones[tombstone.RuntimeSessionUID] = tombstone
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, harnessv2.DeleteRuntimeSessionResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		State: harnessv2.RuntimeSessionStateDeleted, Tombstone: tombstone,
	})
}

type promptDeletionCancellation struct {
	state     *sessionState
	mutations promptMutationExecutor
	promptID  harnessv2.PromptID
}

// promptCancellationForDeletionLocked returns the active prompt cancellation
// surface while the caller holds s.mu. Runtime settlement remains owned by the
// prompt handler; deletion requests cancellation, then leaves that owner to
// advance the session before a retry can perform destructive cleanup.
func promptCancellationForDeletionLocked(state *sessionState) (*promptDeletionCancellation, error) {
	if state == nil || state.prompt == nil || state.prompt.settlement != nil {
		return nil, nil
	}
	if state.promptMutations == nil {
		return nil, fmt.Errorf("runtime cancellation is unavailable")
	}
	return &promptDeletionCancellation{
		state: state, mutations: state.promptMutations, promptID: state.prompt.request.Metadata.PromptID,
	}, nil
}

func (s *Server) cancelPromptForDeletion(ctx context.Context, cancellation *promptDeletionCancellation) {
	if cancellation == nil {
		return
	}
	deactivatePromptCapabilities(cancellation.state, cancellation.promptID, harnessv2.RuntimeSessionStateCancelling)
	cancelCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace)*2,
	)
	defer cancel()
	_, _ = cancellation.mutations.CancelPrompt(cancelCtx, string(cancellation.promptID))
}

// prepareSessionDeletionLocked advances only to a state where destructive
// cleanup is safe. Creating sessions have no runtime cancellation surface yet,
// and an active prompt must settle through CancelPrompt before descendant
// cleanup can be proven. The caller must hold s.mu.
func prepareSessionDeletionLocked(state *sessionState, publicationFinalized bool, now time.Time) (bool, error) {
	nextState := harnessv2.RuntimeSessionStateDeleting
	if !publicationFinalized {
		var err error
		nextState, err = harnessv2.DeletionTransition(state.descriptor.State)
		if err != nil {
			return false, err
		}
	}
	promptSettled := state.prompt == nil || state.prompt.settlement != nil
	if state.creating || state.runtime == nil || !promptSettled || nextState != harnessv2.RuntimeSessionStateDeleting {
		if !state.creating && nextState == harnessv2.RuntimeSessionStateCancelling && state.descriptor.State != nextState {
			state.descriptor.State = nextState
			state.descriptor.LastTransitionAt = now
		}
		return false, nil
	}
	state.descriptor.State = nextState
	state.descriptor.LastTransitionAt = now
	return true, nil
}

func tombstoneOperation(tombstone harnessv2.RuntimeSessionTombstone, operationID harnessv2.OperationID) *harnessv2.OperationRecord {
	for i := range tombstone.Operations {
		if tombstone.Operations[i].OperationID == operationID {
			record := tombstone.Operations[i]
			return &record
		}
	}
	return nil
}

// boundedWorkspaceValidationMessage carries the workspace delta build failure
// back to the controller in a bounded form so live diagnostics name the real
// cause instead of a generic validation error.
// boundedWorkspaceValidationMessage returns a categorized diagnostic for a
// workspace delta build failure. Workspace paths and raw OS error strings are
// agent-controlled (a file can be named after a credential) and the returned
// message is forwarded into controller structured logs, so it carries only
// the failing operation and the safety category - never the path or the
// underlying error text.
func boundedWorkspaceValidationMessage(buildErr error) string {
	const message = "workspace validation failed"
	if buildErr == nil {
		return message
	}
	category := ""
	for _, sentinel := range []error{
		workspacedelta.ErrInvalidRoot, workspacedelta.ErrInvalidBaseline, workspacedelta.ErrPathTraversal,
		workspacedelta.ErrReservedPath, workspacedelta.ErrExcludedPathModified, workspacedelta.ErrUnsafeFileType,
		workspacedelta.ErrHardlinkAmbiguous, workspacedelta.ErrUnsafeSymlink, workspacedelta.ErrLimitExceeded,
		workspacedelta.ErrUnsupportedFilesystem,
	} {
		if errors.Is(buildErr, sentinel) {
			category = sentinel.Error()
			break
		}
	}
	if pathErr, ok := errors.AsType[*workspacedelta.PathError](buildErr); ok {
		if category == "" {
			category = "unsafe workspace entry"
		}
		if pathErr.Op != "" {
			return message + ": " + pathErr.Op + ": " + category
		}
	}
	if category != "" {
		return message + ": " + category
	}
	return message
}

func workspaceDeltaChangedPaths(result workspacedelta.Result) []string {
	paths := make(map[string]struct{}, len(result.Changes)+len(result.Deletions))
	for _, change := range result.Changes {
		paths[change.Path] = struct{}{}
	}
	for _, deletion := range result.Deletions {
		paths[deletion.Path] = struct{}{}
	}
	resultPaths := make([]string, 0, len(paths))
	for changedPath := range paths {
		resultPaths = append(resultPaths, changedPath)
	}
	slices.Sort(resultPaths)
	return resultPaths
}

func workspaceDeltaPathAllowed(changedPath string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, patternValue := range patterns {
		patternValue = strings.TrimPrefix(strings.TrimSpace(patternValue), "./")
		if strings.HasSuffix(patternValue, "/**") && strings.HasPrefix(changedPath, strings.TrimSuffix(patternValue, "**")) {
			return true
		}
		if workspaceDeltaPatternMatches(patternValue, changedPath) {
			return true
		}
		if strings.TrimSuffix(patternValue, "/") == strings.TrimSuffix(changedPath, "/") {
			return true
		}
	}
	return false
}

// workspaceDeltaPatternMatches applies gitignore-style glob semantics: a `**`
// segment matches zero or more whole path segments, while every other segment
// keeps path.Match single-segment semantics.
func workspaceDeltaPatternMatches(patternValue, changedPath string) bool {
	if !strings.Contains(patternValue, "**") {
		matched, err := path.Match(patternValue, changedPath)
		return err == nil && matched
	}
	return workspaceDeltaSegmentsMatch(strings.Split(patternValue, "/"), strings.Split(changedPath, "/"))
}

// workspaceDeltaSegmentsMatch uses the classic greedy wildcard algorithm at
// segment granularity — backtracking only to the most recent `**` — so
// matching stays O(pattern × path) even for agent-controlled paths against
// patterns with many `**` segments.
func workspaceDeltaSegmentsMatch(patternSegments, pathSegments []string) bool {
	patternIndex, pathIndex := 0, 0
	starPattern, starPath := -1, 0
	for pathIndex < len(pathSegments) {
		switch {
		case patternIndex < len(patternSegments) && patternSegments[patternIndex] == "**":
			starPattern, starPath = patternIndex, pathIndex
			patternIndex++
		case patternIndex < len(patternSegments) && workspaceDeltaSegmentMatches(patternSegments[patternIndex], pathSegments[pathIndex]):
			patternIndex++
			pathIndex++
		case starPattern >= 0:
			starPath++
			pathIndex = starPath
			patternIndex = starPattern + 1
		default:
			return false
		}
	}
	for patternIndex < len(patternSegments) && patternSegments[patternIndex] == "**" {
		patternIndex++
	}
	return patternIndex == len(patternSegments)
}

func workspaceDeltaSegmentMatches(pattern, segment string) bool {
	matched, err := path.Match(pattern, segment)
	return err == nil && matched
}

func workspaceDeltaRepositoryControlPathForWorkspace(workspaceRelativeRoot, changedPath string) bool {
	return workspaceDeltaRepositoryControlPath(path.Join(strings.TrimSpace(workspaceRelativeRoot), changedPath))
}

func workspaceDeltaRepositoryControlPath(changedPath string) bool {
	lower := strings.ToLower(strings.TrimPrefix(changedPath, "./"))
	return strings.HasPrefix(lower, ".github/workflows/") ||
		strings.HasPrefix(lower, "config/rbac/") ||
		(strings.HasPrefix(lower, "charts/") && strings.Contains(lower, "secret"))
}

func buildWorkspaceDeltaContext(
	ctx context.Context,
	baseline *workspacedelta.Snapshot,
	workspace string,
	intent workspacedelta.Intent,
	limits harnessv2.WorkspaceDeltaLimits,
) (workspacedelta.Result, error) {
	return workspacedelta.BuildWithLimitsContext(
		ctx,
		baseline,
		workspace,
		intent,
		workspacedelta.BuildLimits{MaxArtifactBytes: limits.MaxBytes},
	)
}

// baselineCaptureOptions returns the delta options for trusted baseline
// captures. The ContentFlagger records which baseline files already carry
// secret-like content before any agent execution, so the delta content policy
// can exempt pre-existing repository content (a vulnerable app's hardcoded
// demo credential) while still rejecting secrets a prompt introduced.
func (s *Server) baselineCaptureOptions() workspacedelta.Options {
	options := s.cfg.DeltaOptions
	options.ContentFlagger = func(content []byte) bool {
		return security.LooksLikeSecret(string(content))
	}
	options.ContentFingerprinter = func(content []byte) []string {
		return security.SecretLikeLineDigests(string(content))
	}
	return options
}

// workspaceDeltaBaselineExempts reports whether the secret-like content of
// the changed file at path is entirely pre-existing. Every secret-like line
// must match a baseline fingerprint (as a multiset) that covers the line's
// code block together with the previous and next code blocks and every
// blank or comment-only line in between — the only places an expression
// continuation could still reach the credential — and once
// those known lines are removed nothing secret-like may remain. Appending,
// replacing, continuing, or relocating a credential is rejected, and so is
// any edit in the neighbouring code blocks (fail closed); an untouched demo
// credential with edits elsewhere in the file stays publishable.
func workspaceDeltaBaselineExempts(baseline *workspacedelta.Snapshot, changedPath string, content []byte) bool {
	if baseline == nil || !baseline.BaselineContentFlagged(changedPath) {
		return false
	}
	// Fingerprints are a multiset: a known block copied to a second place in
	// the file reproduces its digest but exceeds the baseline count, which
	// rejects the relocated credential.
	budget := map[string]int{}
	for _, digest := range baseline.BaselineContentFingerprints(changedPath) {
		budget[digest]++
	}
	if len(budget) == 0 {
		return false
	}
	text := string(content)
	for _, digest := range security.SecretLikeLineDigests(text) {
		if budget[digest] == 0 {
			return false
		}
		budget[digest]--
	}
	known := make(map[string]struct{}, len(budget))
	for digest := range budget {
		known[digest] = struct{}{}
	}
	return !security.LooksLikeSecret(security.StripLinesByDigest(text, known))
}

// workspaceDeltaContentPolicyViolationContext scans the delta artifact for
// policy violations. baselineExempts, when non-nil, reports whether the
// secret-like content of the named workspace-relative file is entirely
// pre-existing in the trusted pre-prompt baseline (see
// workspaceDeltaBaselineExempts); only then is the file exempt from the
// secret-like rejection.
func workspaceDeltaContentPolicyViolationContext(ctx context.Context, artifact []byte, limits harnessv2.WorkspaceDeltaLimits, baselineExempts func(changedPath string, content []byte) bool) (string, error) {
	if len(artifact) == 0 || (!limits.RejectBinaryFiles && !limits.RejectSecretLikeContent) {
		return "", nil
	}
	reader := tar.NewReader(bytes.NewReader(artifact))
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		fileContent := strings.HasPrefix(header.Name, "files/")
		symlinkManifest := header.Name == "meta/symlinks.json"
		if !fileContent && !symlinkManifest {
			continue
		}
		if header.Size < 0 || header.Size > limits.MaxBytes {
			return "", fmt.Errorf("workspace delta file size is invalid")
		}
		content, err := io.ReadAll(io.LimitReader(&workspaceDeltaContextReader{ctx: ctx, reader: reader}, header.Size+1))
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			return "", fmt.Errorf("workspace delta file content is incomplete")
		}
		if int64(len(content)) != header.Size {
			return "", fmt.Errorf("workspace delta file content is incomplete")
		}
		// The violating path is safe to surface: it names the file, never
		// the content, and lets the operator or agent fix the right file.
		if fileContent && limits.RejectBinaryFiles && (bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content)) {
			return "workspace delta contains binary file content: " + strings.TrimPrefix(header.Name, "files/"), nil
		}
		if limits.RejectSecretLikeContent && security.LooksLikeSecret(string(content)) {
			changedPath := strings.TrimPrefix(header.Name, "files/")
			if fileContent && baselineExempts != nil && baselineExempts(changedPath, content) {
				continue
			}
			return "workspace delta contains secret-like file content: " + changedPath, nil
		}
	}
}

type workspaceDeltaContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *workspaceDeltaContextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}

func workspaceDeltaContainsSessionCredential(artifact []byte, state *sessionState) bool {
	if len(artifact) == 0 || state == nil {
		return false
	}
	credentials := [][]byte{}
	if state.providerProxy != nil {
		credentials = append(credentials, state.providerProxy.credential)
	}
	if state.mcpProxy != nil {
		credentials = append(credentials, state.mcpProxy.credential)
	}
	for _, credential := range credentials {
		if len(credential) >= 8 && bytes.Contains(artifact, credential) {
			return true
		}
	}
	return false
}

//nolint:gocyclo // Workspace validation keeps security invariants in one auditable boundary.
func (s *Server) handleWorkspaceDelta(w http.ResponseWriter, r *http.Request) {
	var request harnessv2.CreateWorkspaceDeltaRequest
	if !s.decodeAuthenticatedJSON(w, r, &request) {
		return
	}
	now := time.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if r.PathValue("deltaID") != string(request.DeltaID) {
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "workspace delta path does not match request", nil, false)
		return
	}
	if !s.authorizeMutation(w, r, request.Metadata, true) {
		return
	}
	sessionID := harnessv2.RuntimeSessionID(r.PathValue("sessionID"))
	s.mu.Lock()
	state := s.sessions[sessionID]
	if state == nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "runtime session not found", nil, false)
		return
	}
	if err := s.validateSessionFence(state, request.Metadata); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, err.Error(), nil, false)
		return
	}
	classification, err := harnessv2.ClassifyOperation(s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), request.Metadata, sessionOperationPtrLocked(state, request.Metadata.OperationID, now), true, now)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		response, ok := state.deltas[request.DeltaID]
		s.mu.Unlock()
		if classification.Class == harnessv2.RequestClassificationDuplicate && ok {
			response.Classification = classification
			writeJSON(w, http.StatusOK, response)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if state.prompt == nil || state.prompt.settlement == nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "prompt has not settled", nil, false)
		return
	}
	if !promptMetadataMatches(request.Metadata, state.prompt.request.Metadata) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "workspace delta prompt identity does not match the settled prompt", nil, false)
		return
	}
	if state.descriptor.State != harnessv2.RuntimeSessionStateValidating {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "runtime session is not awaiting workspace validation", nil, false)
		return
	}
	if state.prompt.settlementDigest == "" || request.PromptSettlementDigest != state.prompt.settlementDigest {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "prompt settlement digest does not match", nil, false)
		return
	}
	if request.Intent != state.workspaceIntent || !reflect.DeepEqual(request.VerifiedBaseline, state.descriptor.WorkspaceBaseline) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "workspace intent or verified baseline does not match the runtime session", nil, false)
		return
	}
	if err := ensureSessionOperationCapacityLocked(state, sessionDeletionOperationReserve); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, err.Error(), nil, false)
		return
	}
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseRecorded, "", now)
	runtimeSession, baseline, paths := state.runtime, state.baseline, state.paths
	s.mu.Unlock()

	freezeCtx, cancel := context.WithTimeout(r.Context(), defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace))
	defer cancel()
	if err := runtimeSession.Freeze(freezeCtx); err != nil {
		s.poisonSession(state, "workspace freeze could not be proven")
		s.poisonPool("workspace_freeze_unproven")
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace freeze could not be proven", nil, false)
		return
	}
	frozenAt := time.Now().UTC()
	intent := workspacedelta.IntentRead
	if request.Intent == harnessv2.WorkspaceIntentWrite {
		intent = workspacedelta.IntentWrite
	}
	uid, gid := runtimeSession.ChildIdentity()
	// A durable workspace lives outside the session root, so the reclaim and
	// restore below must cover it independently or the supervisor (which has
	// no DAC_OVERRIDE) cannot read the child-owned tree it is validating.
	ownershipRoots := []string{paths.Root}
	if sessionWorkspaceOutsideRoot(paths) {
		ownershipRoots = append(ownershipRoots, paths.Workspace)
	}
	for _, root := range ownershipRoots {
		if err := acp.ReclaimSessionOwnership(root); err != nil {
			slog.Error("ACP workspace validation failed", "stage", "ownership reclaim")
			s.poisonSession(state, "workspace ownership reclaim failed")
			writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace ownership reclaim failed", nil, false)
			return
		}
	}
	result, buildErr := buildWorkspaceDeltaContext(r.Context(), baseline, paths.Workspace, intent, request.Limits)
	for _, root := range ownershipRoots {
		if err := acp.FinalizeSessionOwnership(root, uid, gid); err != nil {
			slog.Error("ACP workspace validation failed", "stage", "ownership restore")
			s.poisonSession(state, "workspace ownership restore failed")
			writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace ownership restore failed", nil, false)
			return
		}
	}
	if buildErr != nil {
		slog.Error("ACP workspace validation failed", "stage", "delta construction")
		if errors.Is(buildErr, workspacedelta.ErrLimitExceeded) {
			s.rejectWorkspaceDeltaLimit(w, state)
			return
		}
		s.poisonSession(state, "workspace validation failed")
		// Return only the categorized diagnostic. The raw build error may
		// contain an agent-controlled path or OS error text.
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned,
			boundedWorkspaceValidationMessage(buildErr), nil, false)
		return
	}
	entryCount := len(result.Changes) + len(result.Deletions)
	changedPaths := workspaceDeltaChangedPaths(result)
	if request.Limits.MaxChangedFiles > 0 && len(changedPaths) > int(request.Limits.MaxChangedFiles) {
		s.poisonSession(state, "workspace delta exceeds changed-file limit")
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta exceeds changed-file limit", nil, false)
		return
	}
	for _, changedPath := range changedPaths {
		if !workspaceDeltaPathAllowed(changedPath, request.Limits.AllowedPaths) {
			s.poisonSession(state, "workspace delta contains a disallowed path")
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta contains a disallowed path", nil, false)
			return
		}
		if request.Limits.DenyRepositoryControlPaths && workspaceDeltaRepositoryControlPathForWorkspace(state.workspaceRelativeRoot, changedPath) {
			s.poisonSession(state, "workspace delta contains a protected repository-control path")
			writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta contains a protected repository-control path", nil, false)
			return
		}
	}
	if request.Limits.RejectSecretLikeContent && security.LooksLikeSecret(strings.Join(changedPaths, "\n")) {
		s.poisonSession(state, "workspace delta path looks secret-like")
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta path looks secret-like", nil, false)
		return
	}
	// Check exact prompt-scoped credentials before content policy builds a
	// diagnostic containing an agent-controlled file path.
	if workspaceDeltaContainsSessionCredential(result.Artifact, state) {
		s.poisonSession(state, "workspace delta contains a session credential")
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta contains a session credential", nil, false)
		return
	}
	if violation, policyErr := workspaceDeltaContentPolicyViolationContext(r.Context(), result.Artifact, request.Limits, func(changedPath string, content []byte) bool {
		return workspaceDeltaBaselineExempts(state.baseline, changedPath, content)
	}); policyErr != nil {
		s.poisonSession(state, "workspace delta content policy could not be verified")
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta content policy could not be verified", nil, false)
		return
	} else if violation != "" {
		s.poisonSession(state, violation)
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, violation, nil, false)
		return
	}
	if entryCount > int(request.Limits.MaxEntries) || int64(len(result.Artifact)) > request.Limits.MaxBytes {
		s.rejectWorkspaceDeltaLimit(w, state)
		return
	}
	descriptor := harnessv2.WorkspaceDeltaDescriptor{
		DeltaID: request.DeltaID, RuntimeSessionUID: state.descriptor.RuntimeSessionUID, SessionGeneration: state.descriptor.Generation,
		Intent: request.Intent, VerifiedBaseline: request.VerifiedBaseline, RelativeRoot: state.workspaceRelativeRoot,
		EntryCount:        uint32(entryCount),
		DeletedEntryCount: uint32(len(result.Deletions)), SymlinkEntryCount: uint32(len(result.Symlinks)),
		NoFollowVerified: true, FrozenAt: frozenAt,
	}
	for _, change := range result.Changes {
		if change.Kind == workspacedelta.EntryFile {
			descriptor.ChangedFileCount++
		}
	}
	switch result.Classification {
	case workspacedelta.ClassificationNoChange:
		descriptor.State = harnessv2.WorkspaceDeltaNoChange
		descriptor.PublicationSafe = true
		if err := runtimeSession.Thaw(); err != nil {
			s.poisonSession(state, "workspace thaw failed")
			writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace thaw failed", nil, false)
			return
		}
	case workspacedelta.ClassificationReadOnlyModified:
		descriptor.State = harnessv2.WorkspaceDeltaReadOnlyModified
		descriptor.PublicationSafe = false
	case workspacedelta.ClassificationWriteDelta:
		if s.cfg.ArtifactUploader == nil {
			s.poisonSession(state, "durable workspace artifact upload is unavailable")
			writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "durable workspace artifact upload is unavailable", nil, false)
			return
		}
		artifact, err := s.cfg.ArtifactUploader.UploadWorkspaceDelta(r.Context(), request, result.Artifact, result.ArtifactDigest)
		if err != nil || artifact.Digest != result.ArtifactDigest || artifact.SizeBytes != int64(len(result.Artifact)) {
			s.poisonSession(state, "workspace artifact persistence failed")
			writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace artifact persistence failed", nil, false)
			return
		}
		descriptor.State = harnessv2.WorkspaceDeltaPrepared
		descriptor.ManifestDigest = result.ManifestDigest
		descriptor.Artifact = &artifact
		descriptor.PublicationSafe = true
	default:
		s.poisonSession(state, "unsupported workspace delta classification")
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "unsupported workspace delta classification", nil, false)
		return
	}
	response := harnessv2.CreateWorkspaceDeltaResponse{Protocol: harnessv2.ProtocolVersion, Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, Delta: descriptor}
	if err := response.ValidateFor(request); err != nil {
		s.poisonSession(state, "workspace delta response invariant failed")
		writeError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "workspace delta response invariant failed", nil, false)
		return
	}
	s.mu.Lock()
	state.deltas[request.DeltaID] = response
	recordSessionOperationLocked(state, request.Metadata, harnessv2.OperationPhaseApplied, "", time.Now().UTC())
	nextMCPState := harnessv2.RuntimeSessionStatePoisoned
	switch descriptor.State {
	case harnessv2.WorkspaceDeltaNoChange:
		state.descriptor.State = harnessv2.RuntimeSessionStateIdle
		nextMCPState = harnessv2.RuntimeSessionStateIdle
	case harnessv2.WorkspaceDeltaPrepared:
		state.descriptor.State = harnessv2.RuntimeSessionStatePublicationPrepared
		nextMCPState = harnessv2.RuntimeSessionStatePublicationPrepared
	case harnessv2.WorkspaceDeltaReadOnlyModified:
		state.descriptor.State = harnessv2.RuntimeSessionStatePoisoned
	}
	state.descriptor.LastTransitionAt = time.Now().UTC()
	mcpProxy := state.mcpProxy
	sessionCleanup := (s.drain.Requested || state.descriptor.State == harnessv2.RuntimeSessionStatePoisoned) && !state.drainCleanupScheduled
	if sessionCleanup {
		state.drainCleanupScheduled = true
	}
	s.mu.Unlock()
	if mcpProxy != nil {
		mcpProxy.revoke(nextMCPState)
	}
	writeJSON(w, http.StatusOK, response)
	if sessionCleanup {
		go s.cleanupDrainedSession(sessionID, state)
	}
}

func (s *Server) rejectWorkspaceDeltaLimit(w http.ResponseWriter, state *sessionState) {
	s.poisonSession(state, "workspace delta exceeds request limits")
	writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "workspace delta exceeds request limits", nil, false)
}

func (s *Server) poisonSession(state *sessionState, _ string) {
	if state.providerProxy != nil {
		state.providerProxy.revoke()
	}
	if state.mcpProxy != nil {
		state.mcpProxy.revoke(harnessv2.RuntimeSessionStatePoisoned)
	}
	s.mu.Lock()
	state.descriptor.State = harnessv2.RuntimeSessionStatePoisoned
	state.descriptor.LastTransitionAt = time.Now().UTC()
	cleanup := !state.creating && state.runtime != nil && !state.drainCleanupScheduled
	if cleanup {
		state.drainCleanupScheduled = true
	}
	s.mu.Unlock()
	if cleanup {
		go s.cleanupDrainedSession(state.id, state)
	}
}

func (s *Server) mapRuntimeEvent(state *sessionState, prompt *promptState, event acp.PromptEvent) (*harnessv2.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prompt.sequence++
	identity := eventIdentity(s.cfg.Fence, state.descriptor, prompt.request.Metadata, prompt.sequence, event.Timestamp)
	switch event.Type {
	case acp.PromptEventAccepted:
		if state.mcpProxy == nil || state.mcpProxy.markRunning(prompt.request.Metadata.PromptID, event.Timestamp) != nil {
			return nil, fmt.Errorf("activate prompt-scoped MCP authority")
		}
		prompt.acceptedAt = event.Timestamp
		prompt.operation = operationRecord(prompt.request.Metadata, harnessv2.OperationPhaseAccepted, "", event.Timestamp)
		recordSessionOperationLocked(state, prompt.request.Metadata, harnessv2.OperationPhaseAccepted, "", event.Timestamp)
		state.descriptor.State = harnessv2.RuntimeSessionStatePromptRunning
		state.descriptor.LastTransitionAt = event.Timestamp
		return &harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventAccepted, Identity: identity, Accepted: &harnessv2.AcceptedEvent{AcceptedAt: event.Timestamp, Lease: prompt.lease, ACPVersion: harnessv2.ACPProfileV1}}, nil
	case acp.PromptEventUpdate:
		update, text, ok, err := mapACPUpdate(event.Update)
		if err != nil {
			prompt.sequence--
			return nil, err
		}
		if text != "" {
			prompt.appendAssistantText(text, acpAssistantMessagePhase(event.Update), s.cfg.Capabilities.Limits.MaxTerminalResultBytes)
		}
		if !ok {
			prompt.sequence--
			return nil, nil
		}
		mapped := &harnessv2.Event{
			Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventUpdate, Identity: identity, Update: update,
		}
		if err := boundACPToolContentToEventLine(mapped, s.cfg.Capabilities.Limits.MaxEventLineBytes); err != nil {
			prompt.sequence--
			return nil, err
		}
		return mapped, nil
	case acp.PromptEventPermissionRequested:
		permission, err := mapPermission(event.Permission, event.Timestamp, defaultDuration(s.cfg.PermissionTimeout, acp.DefaultPermissionTimeout))
		if err != nil {
			return nil, err
		}
		if prompt.permissionRequestIDs == nil {
			prompt.permissionRequestIDs = make(map[harnessv2.PermissionRequestID]struct{})
		}
		if _, exists := prompt.permissionRequestIDs[permission.RequestID]; exists {
			return nil, fmt.Errorf("permission request %q was already used by this prompt", permission.RequestID)
		}
		if len(prompt.permissionRequestIDs) >= harnessv2.MaxRuntimeSessionTombstoneOperations-sessionDeletionOperationReserve {
			return nil, fmt.Errorf("permission request limit %d exceeded", harnessv2.MaxRuntimeSessionTombstoneOperations-sessionDeletionOperationReserve)
		}
		if uint32(len(state.permissions)) >= s.cfg.Capabilities.Limits.MaxPendingPermissions {
			return nil, fmt.Errorf("pending permission limit %d exceeded", s.cfg.Capabilities.Limits.MaxPendingPermissions)
		}
		options := make(map[string]harnessv2.PermissionOptionKind, len(permission.Options))
		for _, option := range permission.Options {
			options[option.OptionID] = option.Kind
		}
		prompt.permissionRequestIDs[permission.RequestID] = struct{}{}
		state.permissions[permission.RequestID] = permissionState{
			requestID: permission.RequestID, toolCallID: permission.ToolCallID, toolName: permission.ToolName,
			title: permission.Title, requestedAt: event.Timestamp, expiresAt: permission.ExpiresAt, options: options,
		}
		return &harnessv2.Event{Protocol: harnessv2.ProtocolVersion, Type: harnessv2.EventPermissionRequested, Identity: identity, PermissionRequested: permission}, nil
	default:
		return nil, fmt.Errorf("unsupported runtime prompt event %q", event.Type)
	}
}

func (s *Server) terminalEvent(
	state *sessionState,
	prompt *promptState,
	result acp.PromptResult,
) (harnessv2.Event, acp.PromptResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prompt.sequence++
	effective := result
	if prompt.settlement != nil {
		effective = promptResultFromSettlement(*prompt.settlement)
	} else {
		effective = providerTurnLimitResult(state, prompt, effective)
		// Drain-timeout is checked before recorded upstream failures: when a
		// later-issued request never resolved, blaming an earlier recorded
		// failure as "the final request" would be a false diagnosis — the
		// actually-final request's outcome is unknown.
		effective = providerDrainFailureResult(prompt, effective)
		effective = providerUpstreamFailureResult(state, prompt, effective)
		// The durable settlement is derived from the same result the Failed
		// event is built from: a failed result that still carries the child's
		// end_turn or cancelled stop reason would otherwise settle as
		// Completed or Cancelled while the controller received Failed.
		if effective.Outcome == acp.PromptOutcomeFailed {
			effective.StopReason = acp.StopReason(failedEventStopReason(effective.StopReason))
		}
	}
	now := effective.SettledAt
	if now.IsZero() {
		now = time.Now().UTC()
		effective.SettledAt = now
	}
	event := s.buildTerminalEventLocked(state, prompt, effective, now)
	limit := s.cfg.Capabilities.Limits.MaxTerminalResultBytes
	_, overflow := prompt.terminalResultText()
	if !overflow && serializedEventWithinLimit(event, limit) {
		return event, effective, nil
	}

	effective = acp.PromptResult{
		Outcome: acp.PromptOutcomeFailed, StopReason: acp.StopReasonRefusal,
		Accepted: true, SettledAt: now, Err: fmt.Errorf("terminal result exceeds configured size limit"),
	}
	event = harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Type:     harnessv2.EventFailed,
		Identity: eventIdentity(s.cfg.Fence, state.descriptor, prompt.request.Metadata, prompt.sequence, now),
		Failed: &harnessv2.FailedEvent{
			StopReason: harnessv2.ACPStopReasonRefusal,
			Code:       "terminal_result_too_large",
			Message:    "terminal result exceeded configured size limit",
			Retryable:  false,
		},
	}
	if !serializedEventWithinLimit(event, limit) {
		return event, effective, fmt.Errorf("bounded terminal failure event exceeds %d bytes", limit)
	}
	return event, effective, nil
}

func (s *Server) buildTerminalEventLocked(
	state *sessionState,
	prompt *promptState,
	result acp.PromptResult,
	at time.Time,
) harnessv2.Event {
	event := harnessv2.Event{
		Protocol: harnessv2.ProtocolVersion,
		Identity: eventIdentity(s.cfg.Fence, state.descriptor, prompt.request.Metadata, prompt.sequence, at),
	}
	switch result.Outcome {
	case acp.PromptOutcomeCompleted:
		text, _ := prompt.terminalResultText()
		if strings.TrimSpace(text) == "" {
			text = "Prompt completed without textual output."
		}
		event.Type = harnessv2.EventCompleted
		event.Completed = &harnessv2.CompletedEvent{
			StopReason: harnessv2.ACPStopReasonEndTurn,
			Result: harnessv2.PromptResult{
				Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: text}},
				Model:   s.cfg.Provider.Model,
			},
		}
	case acp.PromptOutcomeCancelled:
		event.Type = harnessv2.EventCancelled
		event.Cancelled = &harnessv2.CancelledEvent{StopReason: harnessv2.ACPStopReasonCancelled, Reason: "prompt cancelled"}
	case acp.PromptOutcomeOutcomeUnknown:
		event.Type = harnessv2.EventOutcomeUnknown
		event.OutcomeUnknown = &harnessv2.OutcomeUnknownEvent{Code: "acp_settlement_unproven", Message: "ACP prompt outcome could not be proven", Retryable: false}
	default:
		event.Type = harnessv2.EventFailed
		code, message := "acp_prompt_failed", "ACP prompt failed"
		if result.StopReason == acp.StopReasonMaxTurnRequests {
			code = "turn_limit"
			message = "ACP prompt exceeded maximum provider inference requests"
		} else if upstreamFailure, ok := errors.AsType[*providerUpstreamFailureError](result.Err); ok {
			code = providerUpstreamErrorCode
			message = promptStreamErrorDetail(upstreamFailure)
		} else if drainFailure, ok := errors.AsType[*providerDrainTimeoutError](result.Err); ok {
			code = providerUpstreamErrorCode
			message = drainFailure.Error()
		} else if detail := promptFailureErrorDetail(result.Err); detail != "" {
			// Keep the generic code but carry the agent's own error text
			// (JSON-RPC error message and service/errorName data) so a
			// provider or session failure is diagnosable from Task status.
			message = "ACP prompt failed: " + detail
		}
		event.Failed = &harnessv2.FailedEvent{
			StopReason: failedEventStopReason(result.StopReason),
			Code:       code,
			Message:    message,
			Retryable:  false,
		}
	}
	return event
}

func (p *promptState) appendAssistantText(text, phase string, limit int) {
	appendBoundedPromptText(&p.assistant, &p.assistantOverflow, text, limit)
	if phase != acpAssistantPhaseFinalAnswer {
		return
	}
	p.finalAnswerSeen = true
	appendBoundedPromptText(&p.finalAnswer, &p.finalAnswerOverflow, text, limit)
}

func (p *promptState) terminalResultText() (string, bool) {
	if p.finalAnswerSeen {
		return p.finalAnswer.String(), p.finalAnswerOverflow
	}
	return p.assistant.String(), p.assistantOverflow
}

func appendBoundedPromptText(builder *strings.Builder, overflow *bool, text string, limit int) {
	if *overflow {
		return
	}
	if limit < 1 || len(text) > limit || builder.Len() > limit-len(text) {
		*overflow = true
		return
	}
	_, _ = builder.WriteString(text)
}

func providerTurnLimitResult(state *sessionState, prompt *promptState, result acp.PromptResult) acp.PromptResult {
	if state == nil || prompt == nil || state.providerProxy == nil ||
		!state.providerProxy.maxTurnsExceeded(string(prompt.request.Metadata.PromptID)) {
		return result
	}
	result.Outcome = acp.PromptOutcomeFailed
	result.StopReason = acp.StopReasonMaxTurnRequests
	result.Accepted = true
	return result
}

// providerUpstreamErrorCode is the terminal Failed event code for a prompt whose
// final inference request failed upstream.
const providerUpstreamErrorCode = "provider_upstream_error"

// providerUpstreamFailureError records that the final provider inference request
// made during a prompt failed upstream, even though the ACP agent reported the
// provider error as ordinary assistant text and ended its turn.
type providerUpstreamFailureError struct {
	Status int
	Detail string
}

func (e providerUpstreamFailureError) Error() string {
	message := fmt.Sprintf("provider upstream returned HTTP %d for the final inference request", e.Status)
	if detail := sanitizeProviderUpstreamDetail(e.Detail); detail != "" {
		message += ": " + detail
	}
	return message
}

// providerDrainTimeoutError records that an admitted inference request was
// still unresolved when the child settled and did not finish within the
// cancel grace, so the prompt's final inference outcome is unknown.
type providerDrainTimeoutError struct{}

func (providerDrainTimeoutError) Error() string {
	return "a provider inference request was still in flight when the prompt settled and did not complete within the cancel grace"
}

// providerDrainFailureResult converts a Completed prompt whose inference
// accounting is incomplete (an in-flight request never resolved) into a
// Failed settlement: a successful Task must rest on accounted evidence, and
// an unresolved request could still be a final failure that a child-reported
// end_turn would otherwise mask.
func providerDrainFailureResult(prompt *promptState, result acp.PromptResult) acp.PromptResult {
	if prompt == nil || !prompt.providerDrainTimedOut || result.Outcome != acp.PromptOutcomeCompleted {
		return result
	}
	slog.Error("ACP prompt settled as failed: an inference request did not drain before settlement", "promptID", string(prompt.request.Metadata.PromptID))
	result.Outcome = acp.PromptOutcomeFailed
	result.StopReason = acp.StopReasonRefusal
	result.Accepted = true
	result.Err = &providerDrainTimeoutError{}
	return result
}

// providerUpstreamFailureResult converts a Completed prompt whose final
// inference request failed upstream into a Failed settlement so a provider
// quota or outage never surfaces as a successful Task result, even when an
// earlier inference round in the same prompt succeeded.
func providerUpstreamFailureResult(state *sessionState, prompt *promptState, result acp.PromptResult) acp.PromptResult {
	if state == nil || prompt == nil || state.providerProxy == nil || result.Outcome != acp.PromptOutcomeCompleted {
		return result
	}
	promptID := string(prompt.request.Metadata.PromptID)
	failed, status, detail := state.providerProxy.upstreamFailureUnrecovered(promptID)
	if !failed {
		return result
	}
	slog.Error(
		"ACP prompt settled as failed: the final provider inference request failed upstream",
		"promptID", promptID, "upstreamStatus", status,
	)
	result.Outcome = acp.PromptOutcomeFailed
	result.StopReason = acp.StopReasonRefusal
	result.Accepted = true
	result.Err = &providerUpstreamFailureError{Status: status, Detail: detail}
	return result
}

// promptFailureErrorDetail renders a bounded, low-cardinality description of
// the error that failed a prompt. Free-text error messages are never copied:
// the ACP child (and the provider behind it) can echo credentials or private
// routes into them, and the terminal event is persisted and projected onto
// Task status. Only the validated JSON-RPC code plus the identifier-shaped
// service/errorName data the agent attached, or the supervisor's own
// classification stage, are exposed.
func promptFailureErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	stage, rpcCode, service, errorName := promptExecutionDiagnostic(err)
	if stage != promptExecutionStageJSONRPCError {
		return stage
	}
	detail := fmt.Sprintf("json-rpc error %d", rpcCode)
	switch {
	case service != "" && errorName != "":
		detail += " " + service + "/" + errorName
	case errorName != "":
		detail += " " + errorName
	case service != "":
		detail += " " + service
	}
	return detail
}

// failedEventStopReason maps a failed prompt's ACP stop reason onto one the
// harness v2 Failed event accepts. An agent that errors while reporting
// end_turn or cancelled (for example a provider error surfaced after the
// child was interrupted) would otherwise produce a malformed terminal event,
// break the prompt stream, and leave the Task with an unknown settlement.
func failedEventStopReason(reason acp.StopReason) harnessv2.ACPStopReason {
	switch reason {
	case acp.StopReasonEndTurn, acp.StopReasonCancelled:
		return harnessv2.ACPStopReasonRefusal
	default:
		return harnessv2.ACPStopReason(reason)
	}
}

func serializedEventWithinLimit(event harnessv2.Event, limit int) bool {
	encoded, err := json.Marshal(event)
	return err == nil && len(encoded) <= limit
}

func promptResultFromSettlement(settlement harnessv2.PromptSettlement) acp.PromptResult {
	result := acp.PromptResult{Accepted: true, SettledAt: settlement.SettledAt, StopReason: acp.StopReason(settlement.StopReason)}
	switch settlement.TerminalEvent {
	case harnessv2.EventCompleted:
		result.Outcome = acp.PromptOutcomeCompleted
		result.StopReason = acp.StopReasonEndTurn
	case harnessv2.EventCancelled:
		result.Outcome = acp.PromptOutcomeCancelled
		result.StopReason = acp.StopReasonCancelled
	case harnessv2.EventOutcomeUnknown:
		result.Outcome = acp.PromptOutcomeOutcomeUnknown
	default:
		result.Outcome = acp.PromptOutcomeFailed
		if result.StopReason == "" {
			result.StopReason = acp.StopReasonRefusal
		}
	}
	return result
}

func (s *Server) finishPrompt(state *sessionState, prompt *promptState, result acp.PromptResult, settledAt time.Time) {
	settlement := settlementFromResult(result, settledAt)
	s.mu.Lock()
	settlement = settlePromptLocked(prompt, settlement)
	recordSessionOperationLocked(state, prompt.request.Metadata, harnessv2.OperationPhaseSettled, settlement.TerminalEvent, settlement.SettledAt)
	state.permissions = make(map[harnessv2.PermissionRequestID]permissionState)
	next := harnessv2.RuntimeSessionStatePoisoned
	if settlement.TerminalEvent == harnessv2.EventCompleted {
		next = harnessv2.RuntimeSessionStateValidating
	}
	state.descriptor.State = next
	state.descriptor.LastTransitionAt = settlement.SettledAt
	sessionCleanup := settlement.TerminalEvent != harnessv2.EventCompleted && !state.drainCleanupScheduled
	if sessionCleanup {
		state.drainCleanupScheduled = true
	}
	s.mu.Unlock()
	deactivatePromptCapabilities(state, prompt.request.Metadata.PromptID, next)
	if sessionCleanup {
		go s.cleanupDrainedSession(state.id, state)
	}
}

func settlePromptLocked(prompt *promptState, settlement harnessv2.PromptSettlement) harnessv2.PromptSettlement {
	if prompt.settlement != nil {
		return *prompt.settlement
	}
	prompt.settlement = &settlement
	digest, err := harnessv2.CanonicalPromptSettlementDigest(settlement)
	if err == nil {
		prompt.settlementDigest = digest
	}
	return settlement
}

// waitProviderProxyDrained waits, bounded by the cancel grace, for the
// session's in-flight *inference* requests to finish so their outcomes are
// accounted before the terminal result is classified. An inference request
// that does not finish in time leaves the accounting incomplete: the prompt
// is marked so a child-reported Completed result settles fail-closed instead
// of trusting evidence that never arrived. Metadata requests (model listings,
// token counting) never feed classification and are not waited on: a stalled
// GET /models must not convert a completed prompt into a provider failure.
func (s *Server) waitProviderProxyDrained(state *sessionState, prompt *promptState) {
	if state == nil || state.providerProxy == nil {
		return
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), defaultDuration(s.cfg.CancelGrace, acp.DefaultStopGrace))
	defer cancel()
	if err := state.providerProxy.waitInference(waitCtx); err != nil {
		slog.Warn("ACP provider proxy did not drain before prompt settlement; settling fail-closed", "errorClass", promptStreamErrorClass(err))
		if prompt != nil {
			s.mu.Lock()
			prompt.providerDrainTimedOut = true
			s.mu.Unlock()
		}
	}
}

func deactivatePromptCapabilities(state *sessionState, promptID harnessv2.PromptID, next harnessv2.RuntimeSessionState) {
	if state == nil {
		return
	}
	if state.providerProxy != nil {
		state.providerProxy.deactivate(string(promptID))
	}
	if state.mcpProxy != nil {
		state.mcpProxy.deactivate(promptID, next)
	}
}

func settlementFromResult(result acp.PromptResult, at time.Time) harnessv2.PromptSettlement {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	settlement := harnessv2.PromptSettlement{SettledAt: at}
	switch result.Outcome {
	case acp.PromptOutcomeCompleted:
		settlement.TerminalEvent, settlement.Outcome, settlement.StopReason = harnessv2.EventCompleted, harnessv2.PromptOutcomeSucceeded, harnessv2.ACPStopReasonEndTurn
	case acp.PromptOutcomeCancelled:
		settlement.TerminalEvent, settlement.Outcome, settlement.StopReason = harnessv2.EventCancelled, harnessv2.PromptOutcomeCancelled, harnessv2.ACPStopReasonCancelled
	case acp.PromptOutcomeOutcomeUnknown:
		settlement.TerminalEvent, settlement.Outcome = harnessv2.EventOutcomeUnknown, harnessv2.PromptOutcomeUnknown
	default:
		mapping := harnessv2.MapACPStopReason(harnessv2.ACPStopReason(result.StopReason), true)
		settlement.TerminalEvent, settlement.Outcome, settlement.StopReason = mapping.EventType, mapping.Outcome, harnessv2.ACPStopReason(result.StopReason)
	}
	return settlement
}

func cancellationResponse(classification harnessv2.Classification, settlement harnessv2.PromptSettlement, invalidated uint32, forced bool) harnessv2.CancelPromptResponse {
	barrier := harnessv2.CancellationBarrierSettled
	proven := settlement.TerminalEvent != harnessv2.EventOutcomeUnknown
	if !proven {
		barrier = harnessv2.CancellationBarrierOutcomeUnknown
	} else if forced {
		barrier = harnessv2.CancellationBarrierForcedTerminated
	}
	return harnessv2.CancelPromptResponse{
		Protocol: harnessv2.ProtocolVersion, Classification: classification, BarrierState: barrier,
		SettlementProven: proven, Settlement: settlement, InvalidatedPermissionRequests: invalidated,
		ForcedTermination: forced,
	}
}

func (s *Server) validateSessionFence(state *sessionState, metadata harnessv2.MutationMetadata) error {
	mismatch := harnessv2.CompareFence(s.expectedFence(state.descriptor.RuntimeSessionUID, state.descriptor.Generation), metadata.Fence, true)
	if mismatch != harnessv2.FenceMatch {
		return fmt.Errorf("stale runtime fence: %s", mismatch)
	}
	return nil
}

func eventIdentity(base harnessv2.Fence, descriptor harnessv2.RuntimeSessionDescriptor, metadata harnessv2.MutationMetadata, sequence uint64, at time.Time) harnessv2.EventIdentity {
	return harnessv2.EventIdentity{
		RuntimeInstanceID: base.RuntimeInstanceID, SupervisorBootID: base.SupervisorBootID,
		RuntimeSessionUID: descriptor.RuntimeSessionUID, RuntimeSessionGeneration: descriptor.Generation,
		TaskUID: metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		Sequence: sequence, RequestDigest: metadata.RequestDigest, Timestamp: at,
	}
}

func eventLimits(limits harnessv2.ProtocolLimits) harnessv2.EventStreamLimits {
	return harnessv2.EventStreamLimits{
		MaxLineBytes: limits.MaxEventLineBytes, MaxTerminalResultBytes: limits.MaxTerminalResultBytes,
		MaxBufferedEvents: limits.MaxBufferedEvents, MaxUpdateEventsPerSecond: limits.MaxUpdateEventsPerSecond,
	}
}

func promptMetadataMatches(incoming, current harnessv2.MutationMetadata) bool {
	return incoming.TaskUID == current.TaskUID && incoming.TaskAttempt == current.TaskAttempt && incoming.PromptID == current.PromptID
}

func pathMatchesPrompt(r *http.Request, metadata harnessv2.MutationMetadata) bool {
	return r.PathValue("promptID") == string(metadata.PromptID)
}

func pathMatchesPermission(r *http.Request, request harnessv2.ResolvePermissionRequest) bool {
	return pathMatchesPrompt(r, request.Metadata) && r.PathValue("requestID") == string(request.RequestID)
}

// sessionWorkspaceOutsideRoot reports whether the session's repository
// workspace lives outside the ephemeral session root, as it does when a
// durable workspace directory hosts it on the provider data volume.
func sessionWorkspaceOutsideRoot(paths acp.SessionPaths) bool {
	root := strings.TrimSuffix(paths.Root, "/")
	return paths.Workspace != root && !strings.HasPrefix(paths.Workspace, root+"/")
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
