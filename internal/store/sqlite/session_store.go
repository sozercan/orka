package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

// CreateSession inserts a new session record.
func (s *Store) CreateSession(ctx context.Context, session *store.SessionRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Transcript-only completions are an idempotency receipt until the name is
	// deliberately reused. Kubernetes-backed completions retain a SessionUID and
	// permanently reserve the deleted identity.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM session_cleanup_completions
		 WHERE namespace = ? AND session_name = ? AND session_uid = ''`,
		session.Namespace, session.Name,
	); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		`INSERT INTO sessions (namespace, name, session_type, active_task, active_task_uid, message_count, input_tokens, output_tokens, cancelled, created_at, updated_at)
		 SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		 WHERE NOT EXISTS (
		   SELECT 1 FROM session_cleanup_intents
		   WHERE namespace = ? AND session_name = ?
		 )
		 AND NOT EXISTS (
		   SELECT 1 FROM session_cleanup_completions
		   WHERE namespace = ? AND session_name = ?
		 )`,
		session.Namespace, session.Name, session.SessionType, session.ActiveTask, session.ActiveTaskUID,
		session.MessageCount, session.InputTokens, session.OutputTokens, session.Cancelled,
		session.CreatedAt, session.UpdatedAt,
		session.Namespace, session.Name, session.Namespace, session.Name,
	)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted != 1 {
		return fmt.Errorf("%w: session %s/%s is being deleted or its Kubernetes identity was deleted", store.ErrConflict, session.Namespace, session.Name)
	}
	return tx.Commit()
}

// GetSession loads a session with all its messages.
func (s *Store) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	session := &store.SessionRecord{}
	err := s.db.QueryRowContext(ctx,
		`SELECT namespace, name, session_type, active_task, active_task_uid, message_count, input_tokens, output_tokens, cancelled, created_at, updated_at
		 FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name,
	).Scan(
		&session.Namespace, &session.Name, &session.SessionType, &session.ActiveTask, &session.ActiveTaskUID,
		&session.MessageCount, &session.InputTokens, &session.OutputTokens, &session.Cancelled,
		&session.CreatedAt, &session.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	messages, err := s.LoadTranscript(ctx, namespace, name, 0)
	if err != nil {
		return nil, err
	}
	session.Messages = messages

	return session, nil
}

// GetSessionType returns the stored Session type without loading transcript messages.
func (s *Store) GetSessionType(ctx context.Context, namespace, name string) (string, error) {
	var sessionType string
	err := s.db.QueryRowContext(ctx,
		`SELECT session_type FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name,
	).Scan(&sessionType)
	if errors.Is(err, sql.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return sessionType, err
}

// ListSessions returns metadata for all sessions in a namespace.
func (s *Store) ListSessions(ctx context.Context, namespace string) ([]store.SessionMetadata, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, session_type, message_count, input_tokens, output_tokens, created_at, updated_at, active_task, active_task_uid
		 FROM sessions WHERE namespace = ?`,
		namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var sessions []store.SessionMetadata
	for rows.Next() {
		var m store.SessionMetadata
		if err := rows.Scan(&m.Name, &m.SessionType, &m.MessageCount, &m.InputTokens, &m.OutputTokens, &m.CreatedAt, &m.UpdatedAt, &m.ActiveTask, &m.ActiveTaskUID); err != nil {
			return nil, err
		}
		sessions = append(sessions, m)
	}
	return sessions, rows.Err()
}

// ListSessionsPage pushes the name cursor, type filter, ordering, and LIMIT
// into SQL so a continuation request reads only its own page.
func (s *Store) ListSessionsPage(ctx context.Context, namespace, afterName string, limit int, excludeType string) ([]store.SessionMetadata, bool, error) {
	if limit <= 0 {
		return nil, false, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, session_type, message_count, input_tokens, output_tokens, created_at, updated_at, active_task, active_task_uid
		 FROM sessions
		 WHERE namespace = ? AND name > ? AND (? = '' OR session_type <> ?)
		 ORDER BY name ASC
		 LIMIT ?`,
		namespace, afterName, excludeType, excludeType, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close() //nolint:errcheck

	var sessions []store.SessionMetadata
	for rows.Next() {
		var m store.SessionMetadata
		if err := rows.Scan(&m.Name, &m.SessionType, &m.MessageCount, &m.InputTokens, &m.OutputTokens, &m.CreatedAt, &m.UpdatedAt, &m.ActiveTask, &m.ActiveTaskUID); err != nil {
			return nil, false, err
		}
		sessions = append(sessions, m)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	more := len(sessions) > limit
	if more {
		sessions = sessions[:limit]
	}
	return sessions, more, nil
}

// DeleteSession removes a quiescent session, its settled durable turn state,
// its messages (via CASCADE), and its session-scoped execution event read
// model. Open turns, active mutation leases, reconciliation-blocked controls,
// and undelivered projections keep the session durable for recovery.
func (s *Store) DeleteSession(ctx context.Context, namespace, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var sessionType, activeTask, activeTaskUID string
	var activeTaskExpiresAt sql.NullTime
	if err := tx.QueryRowContext(ctx,
		`SELECT session_type, active_task, active_task_uid, active_task_expires_at
		 FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name,
	).Scan(&sessionType, &activeTask, &activeTaskUID, &activeTaskExpiresAt); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := ensureSessionLockQuiescentTx(ctx, tx, namespace, name, activeTask, activeTaskUID, activeTaskExpiresAt); err != nil {
		return err
	}
	if err := ensureNoSessionCleanupIntentTx(ctx, tx, namespace, name); err != nil {
		return err
	}
	var pendingGatewayEvents int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM gateway_events
		WHERE namespace = ? AND session_name = ? AND state IN (?, ?, ?, ?)`,
		namespace, name, store.GatewayEventAccepted, store.GatewayEventQueued,
		store.GatewayEventDispatching, store.GatewayEventTaskCreated,
	).Scan(&pendingGatewayEvents); err != nil {
		return err
	}
	if pendingGatewayEvents > 0 {
		if sessionType == store.SessionTypeGateway {
			return errors.Join(store.ErrConflict, store.ErrGatewayOwnedSession)
		}
		return store.ErrConflict
	}
	if sessionType == store.SessionTypeGateway {
		return store.ErrGatewayOwnedSession
	}
	var controlAvailability, controlLeaseTaskUID string
	if err := tx.QueryRowContext(ctx,
		`SELECT availability, lease_task_uid FROM session_controls WHERE namespace = ? AND session_name = ?`,
		namespace, name,
	).Scan(&controlAvailability, &controlLeaseTaskUID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if controlLeaseTaskUID != "" || (controlAvailability != "" && controlAvailability != string(store.SessionAvailable)) {
		return store.ErrConflict
	}
	var unsettledTurns int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_turns WHERE namespace = ? AND session_name = ? AND state <> ?`,
		namespace, name, store.SessionTurnFinalized,
	).Scan(&unsettledTurns); err != nil {
		return err
	}
	if unsettledTurns > 0 {
		return store.ErrConflict
	}
	var publicationBackedTurns int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_turns WHERE namespace = ? AND session_name = ? AND publication_id <> ''`,
		namespace, name,
	).Scan(&publicationBackedTurns); err != nil {
		return err
	}
	if publicationBackedTurns > 0 {
		// A publication-backed Session may still own its continuation
		// BranchClaim in the Kubernetes control store. Generic transcript
		// deletion cannot safely reclaim that cross-store ownership fence.
		return store.ErrConflict
	}
	var unsettledProjections int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*)
		 FROM outbox_projections AS projections
		 JOIN session_turns AS turns ON projections.aggregate_kind = ? AND projections.aggregate_id = turns.id
		 WHERE turns.namespace = ? AND turns.session_name = ?
		   AND projections.state NOT IN (?, ?)`,
		sessionTurnAggregateKind, namespace, name, store.OutboxProjectionDelivered, store.OutboxProjectionDeadLetter,
	).Scan(&unsettledProjections); err != nil {
		return err
	}
	if unsettledProjections > 0 {
		return store.ErrConflict
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM outbox_projections
		 WHERE aggregate_kind = ?
		   AND aggregate_id IN (SELECT id FROM session_turns WHERE namespace = ? AND session_name = ?)`,
		sessionTurnAggregateKind, namespace, name,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM session_turns WHERE namespace = ? AND session_name = ?`, namespace, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM session_controls WHERE namespace = ? AND session_name = ?`, namespace, name); err != nil {
		return err
	}
	deleteResult, err := tx.ExecContext(ctx,
		`DELETE FROM sessions WHERE namespace = ? AND name = ? AND session_type <> ?`,
		namespace, name, store.SessionTypeGateway,
	)
	if err != nil {
		return err
	}
	deleted, err := deleteResult.RowsAffected()
	if err != nil {
		return err
	}
	if sessionType != "" && deleted == 0 {
		return store.ErrConflict
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE execution_events SET session_name = '', session_seq = 0 WHERE namespace = ? AND session_name = ?`,
		namespace,
		name,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM execution_event_session_sequences WHERE namespace = ? AND session_name = ?`, namespace, name); err != nil {
		return err
	}
	return tx.Commit()
}

func ensureSessionLockQuiescentTx(
	ctx context.Context,
	tx *sql.Tx,
	namespace, name, activeTask, activeTaskUID string,
	activeTaskExpiresAt sql.NullTime,
) error {
	if activeTask == "" {
		return nil
	}
	if !activeTaskExpiresAt.Valid || activeTaskExpiresAt.Time.After(time.Now().UTC()) {
		return store.ErrConflict
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE sessions SET active_task = '', active_task_uid = '', active_task_expires_at = NULL
		 WHERE namespace = ? AND name = ? AND active_task = ? AND active_task_uid = ?
		   AND active_task_expires_at = ?`,
		namespace, name, activeTask, activeTaskUID, activeTaskExpiresAt.Time,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return store.ErrConflict
	}
	return nil
}

func ensureNoSessionCleanupIntentTx(ctx context.Context, tx *sql.Tx, namespace, name string) error {
	var cleanupPending int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_cleanup_intents WHERE namespace = ? AND session_name = ?`,
		namespace, name,
	).Scan(&cleanupPending); err != nil {
		return err
	}
	if cleanupPending > 0 {
		return store.ErrConflict
	}
	return nil
}

// AcquireLock atomically sets a durable Task lock for a session.
// Returns store.ErrNotFound if the session does not exist, or an error if already locked.
func (s *Store) AcquireLock(ctx context.Context, namespace, name, taskName, taskUID string) error {
	return s.acquireLock(ctx, namespace, name, taskName, taskUID, nil)
}

// AcquireLockUntil atomically sets a crash-recoverable transient lock.
func (s *Store) AcquireLockUntil(ctx context.Context, namespace, name, ownerName, ownerUID string, expiresAt time.Time) error {
	expiresAt = expiresAt.UTC()
	if !expiresAt.After(time.Now().UTC()) {
		return store.ValidationErrorf("session lock expiration must be in the future")
	}
	return s.acquireLock(ctx, namespace, name, ownerName, ownerUID, &expiresAt)
}

func (s *Store) acquireLock(ctx context.Context, namespace, name, taskName, taskUID string, expiresAt *time.Time) error {
	// Check if session exists
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name,
	).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return store.ErrNotFound
	}
	var ownerType string
	if err := s.db.QueryRowContext(ctx, `SELECT owner_type FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name).Scan(&ownerType); err != nil {
		return err
	}
	if ownerType == gatewaySessionOwnerType {
		if taskUID == "" {
			return store.ValidationErrorf("gateway-owned session %s/%s requires an admitted Task identity", namespace, name)
		}
		var eventState store.GatewayEventState
		var storedTaskUID string
		err := s.db.QueryRowContext(ctx, `SELECT state, task_uid FROM gateway_events
			WHERE namespace = ? AND session_name = ? AND task_name = ?
			ORDER BY created_at DESC, id DESC LIMIT 1`,
			namespace, name, taskName,
		).Scan(&eventState, &storedTaskUID)
		if errors.Is(err, sql.ErrNoRows) {
			return store.ValidationErrorf(
				"task %s/%s is not the admitted owner of gateway session %s/%s",
				namespace, taskName, namespace, name,
			)
		}
		if err != nil {
			return err
		}
		if eventState == store.GatewayEventDispatching && storedTaskUID == "" {
			return fmt.Errorf("%w: gateway Task ownership linkage is pending", store.ErrNotReady)
		}
		if eventState != store.GatewayEventTaskCreated || storedTaskUID != taskUID {
			return store.ValidationErrorf(
				"task %s/%s is not the admitted owner of gateway session %s/%s",
				namespace, taskName, namespace, name,
			)
		}
	}

	// Try to acquire the exact owner incarnation. Empty stored UIDs are adopted only
	// for compatibility with locks created before the fencing column existed. Expired
	// transient locks are reclaimable after a controller crash.
	var expiresValue any
	if expiresAt != nil {
		expiresValue = *expiresAt
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET active_task = ?, active_task_uid = ?, active_task_expires_at = ?
		 WHERE namespace = ? AND name = ? AND (active_task = '' OR
		   (active_task_expires_at IS NOT NULL AND active_task_expires_at <= ?) OR
		   (active_task = ? AND (active_task_uid = '' OR active_task_uid = ?)))
		   AND NOT EXISTS (
		     SELECT 1 FROM session_cleanup_intents
		     WHERE namespace = ? AND session_name = ?
		   )`,
		taskName, taskUID, expiresValue, namespace, name, now, taskName, taskUID, namespace, name,
	)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		if pending, pendingErr := s.HasSessionCleanupIntent(ctx, namespace, name); pendingErr != nil {
			return pendingErr
		} else if pending {
			return fmt.Errorf("%w: session %s/%s is being deleted", store.ErrConflict, namespace, name)
		}
		return fmt.Errorf("%w: session %s/%s is already locked", store.ErrConflict, namespace, name)
	}
	return nil
}

// ReleaseLock clears the lock only for the exact Task incarnation.
func (s *Store) ReleaseLock(ctx context.Context, namespace, name, taskName, taskUID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET active_task = '', active_task_uid = '', active_task_expires_at = NULL
		 WHERE namespace = ? AND name = ? AND active_task = ?
		   AND active_task_uid = ?`,
		namespace, name, taskName, taskUID,
	)
	return err
}

// IsLocked returns true if the session is locked by another Task incarnation.
func (s *Store) IsLocked(ctx context.Context, namespace, name, currentTask, currentTaskUID string) (bool, error) {
	for range 3 {
		var activeTask, activeTaskUID string
		var activeTaskExpiresAt sql.NullTime
		err := s.db.QueryRowContext(ctx,
			`SELECT active_task, active_task_uid, active_task_expires_at FROM sessions WHERE namespace = ? AND name = ?`,
			namespace, name,
		).Scan(&activeTask, &activeTaskUID, &activeTaskExpiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return false, store.ErrNotFound
		}
		if err != nil {
			return false, err
		}
		if activeTask == "" {
			return false, nil
		}
		if !activeTaskExpiresAt.Valid || activeTaskExpiresAt.Time.After(time.Now().UTC()) {
			return activeTask != currentTask || (activeTaskUID != "" && activeTaskUID != currentTaskUID), nil
		}
		result, err := s.db.ExecContext(ctx,
			`UPDATE sessions SET active_task = '', active_task_uid = '', active_task_expires_at = NULL
			 WHERE namespace = ? AND name = ? AND active_task = ? AND active_task_uid = ?
			   AND active_task_expires_at = ?`,
			namespace, name, activeTask, activeTaskUID, activeTaskExpiresAt.Time,
		)
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if rows == 1 {
			return false, nil
		}
	}
	// Rapid lock churn is conservatively reported as locked.
	return true, nil
}

// AppendMessages inserts messages into a session's logical transcript and updates session metadata.
// Stable message IDs make retries idempotent; even-numbered logical orders leave a gap for
// gateway terminal projections that arrive after later user messages were durably admitted.
func (s *Store) AppendMessages(ctx context.Context, namespace, name string, messages []store.SessionMessage) error {
	return s.appendMessages(ctx, namespace, name, "", "", messages)
}

// AppendMessagesWithLock requires the exact transient lock owner through the
// whole transaction, preventing writes after lease expiry and takeover.
func (s *Store) AppendMessagesWithLock(
	ctx context.Context,
	namespace, name, ownerName, ownerUID string,
	messages []store.SessionMessage,
) error {
	return s.appendMessages(ctx, namespace, name, ownerName, ownerUID, messages)
}

func (s *Store) appendMessages(
	ctx context.Context,
	namespace, name, ownerName, ownerUID string,
	messages []store.SessionMessage,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var ownerType string
	if err := tx.QueryRowContext(ctx, `SELECT owner_type FROM sessions WHERE namespace = ? AND name = ?`,
		namespace, name).Scan(&ownerType); errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	if ownerType == gatewaySessionOwnerType {
		return store.ErrConflict
	}
	if ownerName != "" || ownerUID != "" {
		if err := verifySessionWriteLockTx(ctx, tx, namespace, name, ownerName, ownerUID); err != nil {
			return err
		}
	}
	var cleanupPending int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_cleanup_intents WHERE namespace = ? AND session_name = ?`,
		namespace, name,
	).Scan(&cleanupPending); err != nil {
		return err
	}
	if cleanupPending > 0 {
		return store.ErrConflict
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO session_messages
		 (namespace, session_name, message_id, sort_order, role, content, name, input, tool_calls, tool_call_id,
		  source_type, source_ref, metadata_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(namespace, session_name, message_id) WHERE message_id <> '' DO NOTHING`,
	)
	if err != nil {
		return err
	}
	defer stmt.Close() //nolint:errcheck

	inserted := 0
	for _, msg := range messages {
		inputJSON, toolCallsJSON, metadataJSON, err := encodeSessionMessagePayload(msg)
		if err != nil {
			return err
		}
		ts := msg.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		messageID := msg.ID
		if messageID == "" {
			messageID, err = newSessionMessageID()
			if err != nil {
				return err
			}
		}
		logicalOrder := msg.Order
		if logicalOrder <= 0 {
			logicalOrder, err = nextSessionMessageOrderTx(ctx, tx, namespace, name)
			if err != nil {
				return err
			}
		}
		result, err := stmt.ExecContext(ctx,
			namespace, name, messageID, logicalOrder, msg.Role, msg.Content, nilIfEmpty(msg.Name), inputJSON,
			toolCallsJSON, nilIfEmpty(msg.ToolCallID), msg.SourceType, msg.SourceRef, metadataJSON, ts,
		)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			matches, matchErr := sessionMessageMatchesTx(
				ctx, tx, namespace, name, messageID, logicalOrder, msg.Order > 0, msg, inputJSON, toolCallsJSON, metadataJSON,
			)
			if matchErr != nil {
				return matchErr
			}
			if !matches {
				return store.ErrDuplicateMismatch
			}
		}
		inserted += int(rows)
	}

	if inserted > 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE sessions SET message_count = message_count + ?, updated_at = CURRENT_TIMESTAMP WHERE namespace = ? AND name = ?`,
			inserted, namespace, name,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func verifySessionWriteLockTx(ctx context.Context, tx *sql.Tx, namespace, name, ownerName, ownerUID string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE sessions SET updated_at = updated_at
		 WHERE namespace = ? AND name = ? AND active_task = ? AND active_task_uid = ?
		   AND (active_task_expires_at IS NULL OR active_task_expires_at > ?)
		   AND NOT EXISTS (
		     SELECT 1 FROM session_cleanup_intents
		     WHERE namespace = ? AND session_name = ?
		   )`,
		namespace, name, ownerName, ownerUID, time.Now().UTC(), namespace, name,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("%w: session %s/%s lock owner changed or expired", store.ErrConflict, namespace, name)
	}
	return nil
}

// LoadTranscript retrieves messages in logical conversation order.
func (s *Store) LoadTranscript(ctx context.Context, namespace, name string, maxMessages int) ([]store.SessionMessage, error) {
	return s.loadTranscript(ctx, namespace, name, 0, maxMessages)
}

// LoadTranscriptThrough retrieves logical history through one stable message ID.
func (s *Store) LoadTranscriptThrough(
	ctx context.Context,
	namespace, name, throughMessageID string,
	maxMessages int,
) ([]store.SessionMessage, error) {
	var throughOrder int64
	if err := s.db.QueryRowContext(ctx, `SELECT sort_order FROM session_messages
		WHERE namespace = ? AND session_name = ? AND message_id = ?`,
		namespace, name, throughMessageID,
	).Scan(&throughOrder); errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return s.loadTranscript(ctx, namespace, name, throughOrder, maxMessages)
}

func (s *Store) loadTranscript(
	ctx context.Context,
	namespace, name string,
	throughOrder int64,
	maxMessages int,
) ([]store.SessionMessage, error) {
	baseColumns := `message_id, sort_order, role, content, name, input, tool_calls, tool_call_id,
		 source_type, source_ref, metadata_json, created_at`
	where := `namespace = ? AND session_name = ?`
	args := []any{namespace, name}
	if throughOrder > 0 {
		where += ` AND sort_order <= ?`
		args = append(args, throughOrder)
	}
	var query string
	if maxMessages > 0 {
		query = `SELECT ` + baseColumns + ` FROM (
			SELECT id, ` + baseColumns + ` FROM session_messages WHERE ` + where + `
			ORDER BY sort_order DESC, id DESC LIMIT ?
		) ORDER BY sort_order, id`
		args = append(args, maxMessages)
	} else {
		query = `SELECT ` + baseColumns + ` FROM session_messages WHERE ` + where + ` ORDER BY sort_order, id`
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var messages []store.SessionMessage
	for rows.Next() {
		message, err := scanSessionMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func scanSessionMessage(row gatewayRowScanner) (store.SessionMessage, error) {
	var msg store.SessionMessage
	var nameStr, inputJSON, toolCallsJSON, toolCallID sql.NullString
	var metadataJSON string
	if err := row.Scan(
		&msg.ID, &msg.Order, &msg.Role, &msg.Content, &nameStr, &inputJSON, &toolCallsJSON, &toolCallID,
		&msg.SourceType, &msg.SourceRef, &metadataJSON, &msg.Timestamp,
	); err != nil {
		return msg, err
	}
	msg.Name = nameStr.String
	msg.ToolCallID = toolCallID.String
	if inputJSON.Valid && inputJSON.String != "" {
		if err := json.Unmarshal([]byte(inputJSON.String), &msg.Input); err != nil {
			return msg, fmt.Errorf("failed to unmarshal input: %w", err)
		}
	}
	if toolCallsJSON.Valid && toolCallsJSON.String != "" {
		if err := json.Unmarshal([]byte(toolCallsJSON.String), &msg.ToolCalls); err != nil {
			return msg, fmt.Errorf("failed to unmarshal tool_calls: %w", err)
		}
	}
	if metadataJSON != "" && metadataJSON != "{}" {
		if err := json.Unmarshal([]byte(metadataJSON), &msg.Metadata); err != nil {
			return msg, fmt.Errorf("failed to unmarshal message metadata: %w", err)
		}
	}
	return msg, nil
}

func newSessionMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate session message ID: %w", err)
	}
	return "local:" + hex.EncodeToString(value[:]), nil
}

func sessionMessageMatchesTx(
	ctx context.Context, tx *sql.Tx, namespace, name, messageID string, logicalOrder int64, compareOrder bool,
	msg store.SessionMessage, inputJSON, toolCallsJSON *string, metadataJSON string,
) (bool, error) {
	var storedOrder int64
	var storedTimestamp time.Time
	var storedRole, storedContent, storedSourceType, storedSourceRef, storedMetadata string
	var storedName, storedInput, storedToolCalls, storedToolCallID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT sort_order, role, content, name, input, tool_calls, tool_call_id,
		source_type, source_ref, metadata_json, created_at FROM session_messages
		WHERE namespace = ? AND session_name = ? AND message_id = ?`, namespace, name, messageID).Scan(
		&storedOrder, &storedRole, &storedContent, &storedName, &storedInput, &storedToolCalls, &storedToolCallID,
		&storedSourceType, &storedSourceRef, &storedMetadata, &storedTimestamp,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, store.ErrConflict
	}
	if err != nil {
		return false, err
	}
	return (!compareOrder || storedOrder == logicalOrder) && storedRole == msg.Role && storedContent == msg.Content &&
		nullableStringMatches(storedName, nilIfEmpty(msg.Name)) && nullableStringMatches(storedInput, inputJSON) &&
		nullableStringMatches(storedToolCalls, toolCallsJSON) && nullableStringMatches(storedToolCallID, nilIfEmpty(msg.ToolCallID)) &&
		storedSourceType == msg.SourceType && storedSourceRef == msg.SourceRef && storedMetadata == metadataJSON &&
		(msg.Timestamp.IsZero() || storedTimestamp.Equal(msg.Timestamp)), nil
}

func nullableStringMatches(stored sql.NullString, expected *string) bool {
	if expected == nil {
		return !stored.Valid
	}
	return stored.Valid && stored.String == *expected
}

func encodeSessionMessagePayload(msg store.SessionMessage) (*string, *string, string, error) {
	var inputJSON, toolCallsJSON *string
	if msg.Input != nil {
		data, err := json.Marshal(msg.Input)
		if err != nil {
			return nil, nil, "", fmt.Errorf("failed to marshal input: %w", err)
		}
		encoded := string(data)
		inputJSON = &encoded
	}
	if msg.ToolCalls != nil {
		data, err := json.Marshal(msg.ToolCalls)
		if err != nil {
			return nil, nil, "", fmt.Errorf("failed to marshal tool_calls: %w", err)
		}
		encoded := string(data)
		toolCallsJSON = &encoded
	}
	metadataJSON := "{}"
	if len(msg.Metadata) > 0 {
		data, err := json.Marshal(msg.Metadata)
		if err != nil {
			return nil, nil, "", fmt.Errorf("failed to marshal message metadata: %w", err)
		}
		metadataJSON = string(data)
	}
	return inputJSON, toolCallsJSON, metadataJSON, nil
}

func nextSessionMessageOrderTx(ctx context.Context, tx *sql.Tx, namespace, name string) (int64, error) {
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sort_order), 0) FROM session_messages
		WHERE namespace = ? AND session_name = ?`, namespace, name).Scan(&current); err != nil {
		return 0, err
	}
	return current + 2, nil
}

// UpdateTokenCounts atomically increments the token counters for a session.
func (s *Store) UpdateTokenCounts(ctx context.Context, namespace, name string, inputTokens, outputTokens int) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET input_tokens = input_tokens + ?, output_tokens = output_tokens + ?, updated_at = ? WHERE namespace = ? AND name = ?`,
		inputTokens, outputTokens, time.Now(), namespace, name)
	return err
}

// UpdateTokenCountsWithLock requires the exact active transient lock owner.
func (s *Store) UpdateTokenCountsWithLock(
	ctx context.Context,
	namespace, name, ownerName, ownerUID string,
	inputTokens, outputTokens int,
) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE sessions
		 SET input_tokens = input_tokens + ?, output_tokens = output_tokens + ?, updated_at = ?
		 WHERE namespace = ? AND name = ? AND active_task = ? AND active_task_uid = ?
		   AND (active_task_expires_at IS NULL OR active_task_expires_at > ?)
		   AND NOT EXISTS (
		     SELECT 1 FROM session_cleanup_intents
		     WHERE namespace = ? AND session_name = ?
		   )`,
		inputTokens, outputTokens, time.Now().UTC(), namespace, name, ownerName, ownerUID,
		time.Now().UTC(), namespace, name,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("%w: session %s/%s lock owner changed or expired", store.ErrConflict, namespace, name)
	}
	return nil
}

// nilIfEmpty returns nil if s is empty, otherwise returns a pointer to s.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
