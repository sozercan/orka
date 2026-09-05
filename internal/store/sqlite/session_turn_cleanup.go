package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/orka-agents/orka/internal/store"
)

// GetSessionTurnCleanupReceipt reads evidence only after the exact Session
// cleanup transaction committed. It never revives an ordinary turn or outbox row.
func (s *Store) GetSessionTurnCleanupReceipt(ctx context.Context, namespace, sessionName, promptAttemptID string) (*store.SessionTurnCleanupReceipt, error) {
	for field, value := range map[string]string{sessionControlFieldNamespace: namespace, sessionControlFieldName: sessionName, "prompt attempt ID": promptAttemptID} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	var encoded []byte
	var digest, turnID, sessionUID, operationID, operationDigest string
	err := s.db.QueryRowContext(ctx, `SELECT receipts.receipt, receipts.receipt_digest, receipts.session_uid,
		receipts.turn_id, completions.operation_id, completions.operation_digest
		FROM session_turn_cleanup_receipts AS receipts
		JOIN session_cleanup_completions AS completions
		  ON completions.namespace = receipts.namespace AND completions.session_name = receipts.session_name
		 AND completions.session_uid = receipts.session_uid
		WHERE receipts.namespace = ? AND receipts.session_name = ? AND receipts.prompt_attempt_id = ?
		  AND NOT EXISTS (SELECT 1 FROM sessions WHERE namespace = ? AND name = ?)
		  AND NOT EXISTS (SELECT 1 FROM session_turns WHERE id = receipts.turn_id)
		  AND NOT EXISTS (SELECT 1 FROM outbox_projections WHERE aggregate_kind = 'SessionTurn' AND aggregate_id = receipts.turn_id)
		  AND NOT EXISTS (SELECT 1 FROM session_cleanup_intents WHERE namespace = receipts.namespace AND session_name = receipts.session_name)
		  AND NOT EXISTS (SELECT 1 FROM session_controls WHERE namespace = receipts.namespace AND session_name = receipts.session_name)`,
		namespace, sessionName, promptAttemptID, namespace, sessionName,
	).Scan(&encoded, &digest, &sessionUID, &turnID, &operationID, &operationDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read Session turn cleanup receipt: %w", err)
	}
	if store.CanonicalBytesDigest(encoded) != digest {
		return nil, store.ConflictErrorf("Session turn cleanup receipt digest does not match its content")
	}
	var receipt store.SessionTurnCleanupReceipt
	if err := json.Unmarshal(encoded, &receipt); err != nil {
		return nil, store.ConflictErrorf("Session turn cleanup receipt is malformed")
	}
	if receipt.Key.SessionUID != sessionUID || receipt.OperationID != operationID || receipt.OperationDigest != operationDigest ||
		receipt.PromptAttemptID != promptAttemptID {
		return nil, store.ConflictErrorf("Session turn cleanup receipt does not match its completed deletion")
	}
	if err := receipt.Validate(namespace, sessionName, turnID); err != nil {
		return nil, err
	}
	return &receipt, nil
}

// ListSessionCleanupTurns returns finalized metadata only under the exact
// durable deletion intent. It never returns user prompts or transcript content.
func (s *Store) ListSessionCleanupTurns(ctx context.Context, intent store.SessionCleanupIntent) ([]store.SessionTurn, error) {
	if err := normalizeSessionCleanupIntent(&intent); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getSessionCleanupIntentTx(ctx, tx, intent.Namespace, intent.SessionName)
	if err != nil {
		return nil, err
	}
	if current.SessionUID != intent.SessionUID || current.OperationID != intent.OperationID || current.OperationDigest != intent.OperationDigest {
		return nil, store.ConflictErrorf("Session cleanup turn lookup does not match its durable intent")
	}
	turns, err := listSessionCleanupTurns(ctx, tx, *current)
	if err != nil {
		return nil, err
	}
	return turns, tx.Commit()
}

func listSessionCleanupTurns(ctx context.Context, tx *sql.Tx, intent store.SessionCleanupIntent) ([]store.SessionTurn, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM session_turns WHERE namespace = ? AND session_name = ? ORDER BY id`,
		intent.Namespace, intent.SessionName,
	)
	if err != nil {
		return nil, err
	}
	var turnIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		turnIDs = append(turnIDs, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	turns := make([]store.SessionTurn, 0, len(turnIDs))
	for _, turnID := range turnIDs {
		turn, err := getSessionTurn(ctx, tx, turnID)
		if err != nil {
			return nil, err
		}
		canonicalID, err := turn.Key.CanonicalID()
		if err != nil || canonicalID != turn.ID || turn.State != store.SessionTurnFinalized ||
			turn.FinalizedAt == nil || turn.FinalizedAt.IsZero() || turn.Key.SessionUID != intent.SessionUID {
			return nil, store.ConflictErrorf("Session turn %q is not finalized under the cleanup identity", turnID)
		}
		turn.UserPrompt, turn.TerminalContent = "", ""
		turns = append(turns, turn)
	}
	return turns, nil
}

func archiveSessionTurnCleanupReceipts(ctx context.Context, tx *sql.Tx, intent store.SessionCleanupIntent) error {
	turns, err := listSessionCleanupTurns(ctx, tx, intent)
	if err != nil {
		return err
	}
	for _, turn := range turns {
		projection, err := getOutboxProjection(ctx, tx, turn.ProjectionID)
		if err != nil {
			return fmt.Errorf("archive Session turn %q terminal projection: %w", turn.ID, err)
		}
		if turn.ProjectionKind != projection.ProjectionKind {
			return store.ConflictErrorf("Session turn %q does not match its terminal projection kind", turn.ID)
		}
		receipt := store.SessionTurnCleanupReceipt{
			Namespace: intent.Namespace, SessionName: intent.SessionName,
			OperationID: intent.OperationID, OperationDigest: intent.OperationDigest,
			TurnID: turn.ID, Key: turn.Key, PromptAttemptID: turn.PromptAttemptID,
			TerminalKind: turn.TerminalKind, FinalizedAt: *turn.FinalizedAt,
			ProjectionID: turn.ProjectionID, ProjectionKind: turn.ProjectionKind, ProjectionDigest: turn.ProjectionDigest,
			AggregateKind: projection.AggregateKind, AggregateID: projection.AggregateID,
			Payload: []byte(projection.Payload), PayloadDigest: projection.PayloadDigest,
			ProjectionState: projection.State, DeliveryDigest: projection.DeliveryDigest, DeliveredAt: projection.DeliveredAt,
		}
		if err := receipt.Validate(intent.Namespace, intent.SessionName, turn.ID); err != nil {
			return err
		}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_turn_cleanup_receipts
			(turn_id, prompt_attempt_id, namespace, session_name, session_uid, receipt_digest, receipt) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			turn.ID, turn.PromptAttemptID, intent.Namespace, intent.SessionName, intent.SessionUID, store.CanonicalBytesDigest(encoded), encoded,
		); err != nil {
			return fmt.Errorf("archive Session turn cleanup receipt: %w", err)
		}
	}
	return nil
}
