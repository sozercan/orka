package supervisor

import (
	"net/http"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

// cancelRetiredPromptLocked returns recorded proof after automatic cleanup has
// overtaken a controller cancellation. Authentication and canonical request
// validation have already run. It always releases s.mu before returning.
func (s *Server) cancelRetiredPromptLocked(w http.ResponseWriter, r *http.Request, request harnessv2.CancelPromptRequest, now time.Time) {
	s.pruneTombstonesLocked(now)
	tombstone, ok := s.tombstones[request.Metadata.Fence.RuntimeSessionUID]
	if !ok || tombstone.sessionID != harnessv2.RuntimeSessionID(r.PathValue("sessionID")) || tombstone.prompt == nil {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "prompt settlement is unavailable", nil, false)
		return
	}
	record := operationPtr(tombstone.cancellationOperations, request.Metadata.OperationID)
	if record == nil {
		record = tombstoneOperation(tombstone.RuntimeSessionTombstone, request.Metadata.OperationID)
	}
	classification, err := harnessv2.ClassifyOperation(
		s.expectedFence(tombstone.RuntimeSessionUID, tombstone.RuntimeSessionGeneration), request.Metadata,
		record, true, now,
	)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, err.Error(), nil, false)
		return
	}
	if classification.Class != harnessv2.RequestClassificationFresh {
		replay := tombstone.cancellations[request.Metadata.OperationID]
		s.mu.Unlock()
		if replay != nil && (classification.Class == harnessv2.RequestClassificationDuplicate || classification.Class == harnessv2.RequestClassificationSettled) {
			writeCancellationOperationReplay(w, r, replay, classification)
		} else {
			writeClassificationError(w, classification)
		}
		return
	}
	if !promptMetadataMatches(request.Metadata, tombstone.prompt.metadata) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeDigestConflict, "cancellation prompt identity does not match the retired prompt", nil, false)
		return
	}
	if request.Metadata.ExpiresAt.After(tombstone.DeletedAt.Add(tombstoneRetention)) {
		s.mu.Unlock()
		writeError(w, http.StatusGone, harnessv2.ErrorCodeSettled, "request outlives retired prompt retention", nil, false)
		return
	}
	if len(tombstone.Operations)+len(tombstone.cancellationOperations) >= harnessv2.MaxRuntimeSessionTombstoneOperations {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, harnessv2.ErrorCodeSessionPoisoned, "runtime session operation journal is full", nil, false)
		return
	}
	// Cleanup proves that the child is gone; only the retained settlement
	// proves its outcome. In particular, OutcomeUnknown stays unproven.
	response := cancellationResponse(harnessv2.Classification{Class: harnessv2.RequestClassificationFresh}, tombstone.prompt.settlement, 0, false)
	tombstone.cancellationOperations[request.Metadata.OperationID] = operationRecord(
		request.Metadata, harnessv2.OperationPhaseSettled, response.Settlement.TerminalEvent, now,
	)
	done := make(chan struct{})
	close(done)
	tombstone.cancellations[request.Metadata.OperationID] = &operationReplay{done: done, isCancellation: true, cancellation: &response}
	s.tombstones[tombstone.RuntimeSessionUID] = tombstone
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, response)
}
