package store

import (
	"encoding/json"
	"time"
)

// SessionTurnCleanupReceipt retains only the finalization evidence needed by
// Task reclamation. Prompt and transcript content are deliberately absent.
// Payload is bytes rather than RawMessage so encoding preserves the exact
// immutable projection bytes, including any whitespace.
type SessionTurnCleanupReceipt struct {
	Namespace        string                  `json:"namespace"`
	SessionName      string                  `json:"sessionName"`
	OperationID      string                  `json:"operationId"`
	OperationDigest  string                  `json:"operationDigest"`
	TurnID           string                  `json:"turnId"`
	Key              SessionTurnKey          `json:"key"`
	PromptAttemptID  string                  `json:"promptAttemptId"`
	TerminalKind     SessionTurnTerminalKind `json:"terminalKind"`
	FinalizedAt      time.Time               `json:"finalizedAt"`
	ProjectionID     string                  `json:"projectionId"`
	ProjectionKind   string                  `json:"projectionKind"`
	ProjectionDigest string                  `json:"projectionDigest"`
	AggregateKind    string                  `json:"aggregateKind"`
	AggregateID      string                  `json:"aggregateId"`
	Payload          []byte                  `json:"payload"`
	PayloadDigest    string                  `json:"payloadDigest"`
	ProjectionState  OutboxProjectionState   `json:"projectionState"`
	DeliveryDigest   string                  `json:"deliveryDigest,omitempty"`
	DeliveredAt      *time.Time              `json:"deliveredAt,omitempty"`
}

// Validate proves the archived metadata still pins one exact terminal
// projection. DeadLetter receipts are retained as evidence, but callers must
// still require Delivered before authorizing Task reclamation.
func (r *SessionTurnCleanupReceipt) Validate(namespace, sessionName, turnID string) error {
	if r == nil || r.Namespace != namespace || r.SessionName != sessionName || r.TurnID != turnID {
		return ConflictErrorf("Session cleanup receipt does not match its requested identity")
	}
	for field, value := range map[string]string{
		"namespace": r.Namespace, "session name": r.SessionName, "cleanup operation ID": r.OperationID,
		"prompt attempt ID": r.PromptAttemptID,
	} {
		if err := ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := ValidateCanonicalDigest("cleanup operation digest", r.OperationDigest); err != nil {
		return err
	}
	canonicalID, err := r.Key.CanonicalID()
	if err != nil || canonicalID != r.TurnID || r.FinalizedAt.IsZero() ||
		(r.TerminalKind != SessionTurnAssistantResult && r.TerminalKind != SessionTurnOutcomeMarker) {
		return ConflictErrorf("Session cleanup receipt has invalid finalized turn evidence")
	}
	if r.ProjectionKind != "TaskTerminalStatus" || r.AggregateKind != "SessionTurn" || r.AggregateID != r.TurnID ||
		r.ProjectionID != CanonicalControlID("outbox", r.TurnID, r.ProjectionKind) ||
		!json.Valid(r.Payload) || r.PayloadDigest != CanonicalBytesDigest(r.Payload) || r.ProjectionDigest != r.PayloadDigest {
		return ConflictErrorf("Session cleanup receipt does not pin its terminal projection")
	}
	switch r.ProjectionState {
	case OutboxProjectionDelivered:
		if r.DeliveredAt == nil || r.DeliveredAt.IsZero() {
			return ConflictErrorf("Session cleanup receipt has no projection delivery timestamp")
		}
		return ValidateCanonicalDigest("projection delivery digest", r.DeliveryDigest)
	case OutboxProjectionDeadLetter:
		if r.DeliveredAt != nil || r.DeliveryDigest != "" {
			return ConflictErrorf("dead-letter Session cleanup receipt contains delivery proof")
		}
		return nil
	default:
		return ConflictErrorf("Session cleanup receipt projection is not terminal")
	}
}

// SessionTurn returns only the archived metadata used by finalization checks.
func (r *SessionTurnCleanupReceipt) SessionTurn() *SessionTurn {
	return &SessionTurn{
		ID: r.TurnID, Key: r.Key, PromptAttemptID: r.PromptAttemptID,
		State: SessionTurnFinalized, TerminalKind: r.TerminalKind, FinalizedAt: &r.FinalizedAt,
		ProjectionID: r.ProjectionID, ProjectionKind: r.ProjectionKind, ProjectionDigest: r.ProjectionDigest,
	}
}

// OutboxProjection returns the exact archived terminal delivery evidence.
func (r *SessionTurnCleanupReceipt) OutboxProjection() *OutboxProjection {
	return &OutboxProjection{
		ID: r.ProjectionID, AggregateKind: r.AggregateKind, AggregateID: r.AggregateID,
		ProjectionKind: r.ProjectionKind, PayloadDigest: r.PayloadDigest,
		Payload: append([]byte(nil), r.Payload...), State: r.ProjectionState,
		DeliveryDigest: r.DeliveryDigest, DeliveredAt: r.DeliveredAt,
	}
}
