package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func (s *lifecycleProbeState) probeCreateSessionReplay(
	ctx context.Context,
	request harnessv2.CreateRuntimeSessionRequest,
	original *harnessv2.CreateRuntimeSessionResponse,
) error {
	duplicate, err := s.client.CreateRuntimeSession(ctx, request)
	if err != nil {
		return fmt.Errorf("replay identical runtime-session creation: %w", err)
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationDuplicate ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseApplied {
		return fmt.Errorf("identical runtime-session creation classified as %#v, want duplicate/applied", duplicate.Classification)
	}
	if original == nil || !reflect.DeepEqual(duplicate.Session, original.Session) {
		return fmt.Errorf("identical runtime-session creation did not replay the original descriptor")
	}

	conflicting := request
	conflicting.Workspace.Baseline.Revision += "-digest-conflict"
	conflicting.Workspace.Baseline.TreeDigest = digestString(conflicting.Workspace.Baseline.TreeDigest + "-digest-conflict")
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting runtime-session creation: %w", err)
	}
	_, err = s.client.CreateRuntimeSession(ctx, conflicting)
	return expectClassificationError("conflicting runtime-session creation", err, harnessv2.OperationPhaseApplied, "")
}

type promptReplayObservation struct {
	settlement       *harnessv2.PromptSettlement
	conflictTerminal harnessv2.EventType
}

func (s *lifecycleProbeState) probeAcceptedPromptReplay(
	ctx context.Context,
	request harnessv2.StartPromptRequest,
	acceptedAt time.Time,
) (promptReplayObservation, error) {
	var observed promptReplayObservation
	duplicate, err := s.promptAdmissionReplay(ctx, request)
	if err != nil {
		return observed, fmt.Errorf("replay identical accepted prompt admission: %w", err)
	}
	if !duplicate.AcceptedAt.Equal(acceptedAt) {
		return observed, fmt.Errorf("identical accepted prompt admission did not preserve the original acceptance timestamp")
	}
	// Completion can win either round trip. Retain any settled observation
	// and compare it with the original stream's cancellation settlement.
	switch duplicate.Classification.Class {
	case harnessv2.RequestClassificationAlreadyAccepted:
		if duplicate.Classification.Phase != harnessv2.OperationPhaseAccepted {
			return observed, fmt.Errorf("identical accepted prompt admission has phase %q, want accepted", duplicate.Classification.Phase)
		}
	case harnessv2.RequestClassificationSettled:
		if duplicate.Classification.TerminalEvent != duplicate.Settlement.TerminalEvent {
			return observed, fmt.Errorf("identical accepted prompt admission classification disagrees with its settlement terminal event")
		}
		observed.settlement = duplicate.Settlement
	default:
		return observed, fmt.Errorf("identical accepted prompt admission classified as %q, want already_accepted or settled", duplicate.Classification.Class)
	}

	conflicting := conflictingPromptRequest(request)
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return observed, fmt.Errorf("build conflicting accepted prompt admission: %w", err)
	}
	stream, err := s.client.StartPrompt(ctx, s.sessionID, conflicting)
	if stream != nil {
		_ = stream.Close()
	}
	phase := harnessv2.OperationPhaseAccepted
	var clientErr *harnessv2.ClientError
	if errors.As(err, &clientErr) && clientErr.Classification != nil && clientErr.Classification.Phase == harnessv2.OperationPhaseSettled {
		phase = harnessv2.OperationPhaseSettled
		observed.conflictTerminal = clientErr.Classification.TerminalEvent
		if !observed.conflictTerminal.IsTerminal() {
			return observed, fmt.Errorf("conflicting accepted prompt admission omitted its settled terminal event")
		}
	}
	if err := expectClassificationError("conflicting accepted prompt admission", err, phase, observed.conflictTerminal); err != nil {
		return observed, err
	}
	if observed.settlement != nil && phase != harnessv2.OperationPhaseSettled {
		return observed, fmt.Errorf("conflicting accepted prompt admission regressed from settled to accepted")
	}
	return observed, nil
}

func (o promptReplayObservation) validateSettlement(settlement harnessv2.PromptSettlement) error {
	if o.settlement != nil && !reflect.DeepEqual(*o.settlement, settlement) {
		return fmt.Errorf("identical accepted prompt admission did not replay the original settlement")
	}
	if o.conflictTerminal != "" && o.conflictTerminal != settlement.TerminalEvent {
		return fmt.Errorf("conflicting accepted prompt admission terminal event does not match the original settlement")
	}
	return nil
}

func (s *lifecycleProbeState) probeSettledPromptReplay(
	ctx context.Context,
	request harnessv2.StartPromptRequest,
	settlement harnessv2.PromptSettlement,
) error {
	duplicate, err := s.promptAdmissionReplay(ctx, request)
	if err != nil {
		return fmt.Errorf("replay identical settled prompt admission: %w", err)
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationSettled ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseSettled ||
		duplicate.Classification.TerminalEvent != settlement.TerminalEvent ||
		duplicate.Settlement == nil || !reflect.DeepEqual(*duplicate.Settlement, settlement) {
		return fmt.Errorf("identical settled prompt admission did not replay the original settlement")
	}

	conflicting := conflictingPromptRequest(request)
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting settled prompt admission: %w", err)
	}
	stream, err := s.client.StartPrompt(ctx, s.sessionID, conflicting)
	if stream != nil {
		_ = stream.Close()
	}
	return expectClassificationError(
		"conflicting settled prompt admission", err,
		harnessv2.OperationPhaseSettled, settlement.TerminalEvent,
	)
}

func (s *lifecycleProbeState) promptAdmissionReplay(
	ctx context.Context,
	request harnessv2.StartPromptRequest,
) (*harnessv2.PromptAdmissionResponse, error) {
	capability, err := harnessv2.SignOperationCapability(s.target.OperationCapabilitySecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		return nil, fmt.Errorf("sign prompt replay capability: %w", err)
	}
	path, err := harnessv2.PromptPath(s.sessionID, request.Metadata.PromptID)
	if err != nil {
		return nil, err
	}
	endpoint, err := endpointURL(s.target.BaseURL, path)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Accept-Encoding", "identity")
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+s.target.ControllerBearerToken)
	httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("prompt replay returned HTTP %d, want 200", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, fmt.Errorf("prompt replay returned unsupported Content-Type")
	}
	limited := &io.LimitedReader{R: response.Body, N: int64(harnessv2.MaxCanonicalJSONBytes) + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var admission harnessv2.PromptAdmissionResponse
	if err := decoder.Decode(&admission); err != nil {
		return nil, fmt.Errorf("decode prompt replay admission: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("prompt replay admission contains trailing JSON")
	}
	if limited.N == 0 {
		return nil, fmt.Errorf("prompt replay admission exceeds the canonical JSON limit")
	}
	if err := admission.Validate(); err != nil {
		return nil, fmt.Errorf("validate prompt replay admission: %w", err)
	}
	return &admission, nil
}

func conflictingPromptRequest(request harnessv2.StartPromptRequest) harnessv2.StartPromptRequest {
	conflicting := request
	conflicting.Input.Content = append([]harnessv2.ContentBlock(nil), request.Input.Content...)
	if len(conflicting.Input.Content) > 0 {
		conflicting.Input.Content[0].Text += " Digest-conflict probe."
	}
	return conflicting
}

func (s *lifecycleProbeState) probeCancellationReplay(
	ctx context.Context,
	request harnessv2.CancelPromptRequest,
	original *harnessv2.CancelPromptResponse,
) error {
	duplicate, err := s.client.CancelPrompt(ctx, s.sessionID, request)
	if err != nil {
		return fmt.Errorf("replay identical prompt cancellation: %w", err)
	}
	if original == nil {
		return fmt.Errorf("original prompt cancellation response is required")
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationSettled ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseSettled ||
		duplicate.Classification.TerminalEvent != original.Settlement.TerminalEvent {
		return fmt.Errorf("identical prompt cancellation classified as %#v, want settled replay", duplicate.Classification)
	}
	duplicateReplay := *duplicate
	originalReplay := *original
	duplicateReplay.Classification = harnessv2.Classification{}
	originalReplay.Classification = harnessv2.Classification{}
	if !reflect.DeepEqual(duplicateReplay, originalReplay) {
		return fmt.Errorf("identical prompt cancellation did not replay the original response")
	}

	conflicting := request
	conflicting.Reason = harnessv2.CancelReasonTaskTimeout
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting prompt cancellation: %w", err)
	}
	_, err = s.client.CancelPrompt(ctx, s.sessionID, conflicting)
	return expectClassificationError(
		"conflicting prompt cancellation", err,
		harnessv2.OperationPhaseSettled, original.Settlement.TerminalEvent,
	)
}

func (s *lifecycleProbeState) probeWorkspaceDeltaReplay(
	ctx context.Context,
	request harnessv2.CreateWorkspaceDeltaRequest,
	original *harnessv2.CreateWorkspaceDeltaResponse,
) error {
	duplicate, err := s.client.CreateWorkspaceDelta(ctx, s.sessionID, request)
	if err != nil {
		return fmt.Errorf("replay identical workspace delta: %w", err)
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationDuplicate ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseApplied {
		return fmt.Errorf("identical workspace delta classified as %#v, want duplicate/applied", duplicate.Classification)
	}
	if original == nil || !reflect.DeepEqual(duplicate.Delta, original.Delta) {
		return fmt.Errorf("identical workspace delta did not replay the original descriptor")
	}

	conflicting := request
	if conflicting.Limits.MaxEntries > 1 {
		conflicting.Limits.MaxEntries--
	} else {
		conflicting.Limits.MaxEntries++
	}
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting workspace delta: %w", err)
	}
	_, err = s.client.CreateWorkspaceDelta(ctx, s.sessionID, conflicting)
	return expectClassificationError(
		"conflicting workspace delta", err,
		harnessv2.OperationPhaseApplied, "",
	)
}

func (s *lifecycleProbeState) probePublicationFinalizationReplay(
	ctx context.Context,
	request harnessv2.FinalizeRuntimeSessionPublicationRequest,
	original *harnessv2.FinalizeRuntimeSessionPublicationResponse,
) error {
	duplicate, err := s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, request)
	if err != nil {
		return fmt.Errorf("replay identical publication finalization: %w", err)
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationDuplicate ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseApplied {
		return fmt.Errorf("identical publication finalization classified as %#v, want duplicate/applied", duplicate.Classification)
	}
	if original == nil || !reflect.DeepEqual(duplicate.Finalization, original.Finalization) {
		return fmt.Errorf("identical publication finalization did not replay the original receipt")
	}
	conflicting := request
	conflicting.TerminalReceiptDigest = digestString("conflicting-publication-finalization")
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting publication finalization: %w", err)
	}
	_, err = s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, conflicting)
	return expectClassificationError("conflicting publication finalization", err, harnessv2.OperationPhaseApplied, "")
}

func (s *lifecycleProbeState) probePublicationFinalizationRecovery(
	ctx context.Context,
	request harnessv2.FinalizeRuntimeSessionPublicationRequest,
	original *harnessv2.FinalizeRuntimeSessionPublicationResponse,
) error {
	if original == nil {
		return fmt.Errorf("publication finalization recovery requires the original response")
	}

	stale := request
	stale.Metadata.OperationID = harnessv2.OperationID(string(request.Metadata.OperationID) + "-stale-generation")
	stale.Metadata.Fence.RuntimeSessionGeneration++
	stale.Metadata.ExpiresAt = time.Now().UTC().Add(30 * time.Second)
	if err := setRequestDigest(&stale, &stale.Metadata); err != nil {
		return fmt.Errorf("build stale-generation publication finalization: %w", err)
	}
	_, err := s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, stale)
	if err := expectStaleSessionGeneration("publication finalization", err); err != nil {
		return err
	}

	conflicting := request
	conflicting.Metadata.OperationID = harnessv2.OperationID(string(request.Metadata.OperationID) + "-fresh-conflict")
	conflicting.Metadata.ExpiresAt = time.Now().UTC().Add(30 * time.Second)
	conflicting.TerminalReceiptDigest = digestString("conflicting-fresh-publication-finalization")
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting fresh publication finalization: %w", err)
	}
	_, err = s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, conflicting)
	if err == nil {
		return fmt.Errorf("conflicting fresh publication finalization succeeded, want digest_conflict")
	}
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != harnessv2.ErrorCodeDigestConflict || clientErr.Retryable {
		return fmt.Errorf("conflicting fresh publication finalization returned %v, want non-retryable digest_conflict", err)
	}

	recovery := request
	recovery.Metadata.OperationID = harnessv2.OperationID(string(request.Metadata.OperationID) + "-fresh-recovery")
	recovery.Metadata.ExpiresAt = time.Now().UTC().Add(30 * time.Second)
	if err := setRequestDigest(&recovery, &recovery.Metadata); err != nil {
		return fmt.Errorf("build fresh publication finalization recovery: %w", err)
	}
	recovered, err := s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, recovery)
	if err != nil {
		return fmt.Errorf("recover publication finalization with fresh operation: %w", err)
	}
	if recovered.Classification.Class != harnessv2.RequestClassificationFresh ||
		recovered.Session.State != harnessv2.RuntimeSessionStateFinalizing {
		return fmt.Errorf("fresh publication finalization recovery returned %#v", recovered)
	}
	if !reflect.DeepEqual(recovered.Finalization, original.Finalization) {
		return fmt.Errorf("fresh publication finalization recovery did not preserve the original receipt")
	}
	return nil
}

func (s *lifecycleProbeState) probeStaleSessionDeletion(
	ctx context.Context, request harnessv2.DeleteRuntimeSessionRequest,
) error {
	stale := request
	stale.Metadata.OperationID = harnessv2.OperationID(string(request.Metadata.OperationID) + "-stale-generation")
	stale.Metadata.Fence.RuntimeSessionGeneration++
	stale.Metadata.ExpiresAt = time.Now().UTC().Add(30 * time.Second)
	if err := setRequestDigest(&stale, &stale.Metadata); err != nil {
		return fmt.Errorf("build stale-generation runtime-session deletion: %w", err)
	}
	_, err := s.client.DeleteRuntimeSession(ctx, s.sessionID, stale)
	return expectStaleSessionGeneration("runtime-session deletion", err)
}

func expectStaleSessionGeneration(operation string, err error) error {
	if err == nil {
		return fmt.Errorf("%s accepted a stale RuntimeSession generation", operation)
	}
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) || clientErr.StatusCode != http.StatusGone || clientErr.Classification == nil ||
		clientErr.Classification.Class != harnessv2.RequestClassificationStaleFence ||
		clientErr.Classification.FenceMismatch != harnessv2.FenceMismatchRuntimeSessionGeneration {
		return fmt.Errorf("stale-generation %s returned %v, want HTTP 410 stale_fence/runtime_session_generation", operation, err)
	}
	return nil
}

func (s *lifecycleProbeState) probeDeletionReplay(
	ctx context.Context,
	request harnessv2.DeleteRuntimeSessionRequest,
	original *harnessv2.DeleteRuntimeSessionResponse,
) error {
	duplicate, err := s.client.DeleteRuntimeSession(ctx, s.sessionID, request)
	if err != nil {
		return fmt.Errorf("replay identical runtime-session deletion: %w", err)
	}
	if duplicate.Classification.Class != harnessv2.RequestClassificationDuplicate ||
		duplicate.Classification.Phase != harnessv2.OperationPhaseDeleted {
		return fmt.Errorf("identical runtime-session deletion classified as %#v, want duplicate/deleted", duplicate.Classification)
	}
	if original == nil || !reflect.DeepEqual(duplicate.Tombstone, original.Tombstone) {
		return fmt.Errorf("identical runtime-session deletion did not replay the original tombstone")
	}

	conflicting := request
	conflicting.Reason += " with conflicting digest"
	if err := setRequestDigest(&conflicting, &conflicting.Metadata); err != nil {
		return fmt.Errorf("build conflicting runtime-session deletion: %w", err)
	}
	_, err = s.client.DeleteRuntimeSession(ctx, s.sessionID, conflicting)
	return expectClassificationError(
		"conflicting runtime-session deletion", err,
		harnessv2.OperationPhaseDeleted, "",
	)
}

func expectClassificationError(
	operation string,
	err error,
	phase harnessv2.OperationPhase,
	terminal harnessv2.EventType,
) error {
	const class = harnessv2.RequestClassificationDigestConflict
	if err == nil {
		return fmt.Errorf("%s succeeded, want %s classification", operation, class)
	}
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) || clientErr.Classification == nil {
		return fmt.Errorf("%s returned %v, want typed %s classification", operation, err, class)
	}
	classification := *clientErr.Classification
	if classification.Class != class || classification.Phase != phase || classification.TerminalEvent != terminal {
		return fmt.Errorf("%s classified as %#v, want class=%q phase=%q terminal=%q", operation, classification, class, phase, terminal)
	}
	return nil
}
