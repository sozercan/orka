package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.SessionCleanupPersistenceStore = (*Store)(nil)

// GetSessionCleanupIntent returns the durable cross-store cleanup plan.
func (s *Store) GetSessionCleanupIntent(ctx context.Context, namespace, sessionName string) (*store.SessionCleanupIntent, error) {
	var encoded []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT plan FROM session_cleanup_intents WHERE namespace = ? AND session_name = ?`,
		namespace, sessionName,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var intent store.SessionCleanupIntent
	if err := json.Unmarshal(encoded, &intent); err != nil {
		return nil, err
	}
	return &intent, nil
}

// ListSessionCleanupIntents returns every pending durable cleanup plan in
// deterministic order for controller-startup recovery.
func (s *Store) ListSessionCleanupIntents(ctx context.Context) ([]store.SessionCleanupIntent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT plan FROM session_cleanup_intents ORDER BY namespace, session_name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	intents := make([]store.SessionCleanupIntent, 0)
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var intent store.SessionCleanupIntent
		if err := json.Unmarshal(encoded, &intent); err != nil {
			return nil, err
		}
		if err := normalizeSessionCleanupIntent(&intent); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

// GetSessionCleanupCompletion returns the durable deletion tombstone retained
// after the Session row and intent have been atomically removed.
func (s *Store) GetSessionCleanupCompletion(ctx context.Context, namespace, sessionName string) (*store.SessionCleanupCompletion, error) {
	completion := &store.SessionCleanupCompletion{}
	err := s.db.QueryRowContext(ctx,
		`SELECT namespace, session_name, session_uid, operation_id, operation_digest, completed_at
		 FROM session_cleanup_completions WHERE namespace = ? AND session_name = ?`,
		namespace, sessionName,
	).Scan(&completion.Namespace, &completion.SessionName, &completion.SessionUID, &completion.OperationID, &completion.OperationDigest, &completion.CompletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	completion.CompletedAt = completion.CompletedAt.UTC()
	return completion, nil
}

// HasSessionCleanupFenceForUID reports whether a Session UID belongs to a
// pending or completed deletion and may not acquire new Kubernetes authority.
func (s *Store) HasSessionCleanupFenceForUID(ctx context.Context, sessionUID string) (bool, error) {
	sessionUID = strings.TrimSpace(sessionUID)
	if sessionUID == "" {
		return false, nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_cleanup_completions WHERE session_uid = ?`, sessionUID,
	).Scan(&count); err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT plan FROM session_cleanup_intents`)
	if err != nil {
		return false, err
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return false, err
		}
		var intent store.SessionCleanupIntent
		if err := json.Unmarshal(encoded, &intent); err != nil {
			return false, err
		}
		if intent.SessionUID == sessionUID {
			return true, nil
		}
	}
	return false, rows.Err()
}

// GetSessionCleanupIdentity returns the Kubernetes Session UID already bound
// to a transcript row, including a bind-before-control crash residue.
func (s *Store) GetSessionCleanupIdentity(ctx context.Context, namespace, sessionName string) (string, error) {
	var sessionUID string
	err := s.db.QueryRowContext(ctx,
		`SELECT control_session_uid FROM sessions WHERE namespace = ? AND name = ?`, namespace, sessionName,
	).Scan(&sessionUID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return sessionUID, err
}

// BindSessionCleanupIdentity records the Kubernetes Session UID on the
// transcript row so deletion cannot confuse a pre-cutover replacement with the
// control record left by an older Session incarnation.
func (s *Store) BindSessionCleanupIdentity(ctx context.Context, namespace, sessionName, sessionUID string) error {
	trimmedNamespace := strings.TrimSpace(namespace)
	if namespace != trimmedNamespace {
		return store.ValidationErrorf("session namespace must not contain surrounding whitespace")
	}
	trimmedSessionName := strings.TrimSpace(sessionName)
	if sessionName != trimmedSessionName {
		return store.ValidationErrorf("session name must not contain surrounding whitespace")
	}
	namespace = trimmedNamespace
	sessionName = trimmedSessionName
	sessionUID = strings.TrimSpace(sessionUID)
	if err := store.ValidateControlIdentifier("session namespace", namespace); err != nil {
		return err
	}
	if err := store.ValidateControlIdentifier("session name", sessionName); err != nil {
		return err
	}
	if err := store.ValidateControlIdentifier("session UID", sessionUID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET control_session_uid = ?
		 WHERE namespace = ? AND name = ?
		   AND (control_session_uid = '' OR control_session_uid = ?)
		   AND NOT EXISTS (
		     SELECT 1 FROM session_cleanup_intents
		     WHERE namespace = ? AND session_name = ?
		   )
		   AND NOT EXISTS (
		     SELECT 1 FROM session_cleanup_completions
		     WHERE namespace = ? AND session_name = ?
		   )`,
		sessionUID, namespace, sessionName, sessionUID,
		namespace, sessionName, namespace, sessionName,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 1 {
		return nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sessions WHERE namespace = ? AND name = ?`, namespace, sessionName,
	).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return store.ErrNotFound
	}
	return store.ConflictErrorf("session %s/%s has a different cleanup identity or deletion fence", namespace, sessionName)
}

// HasSessionCleanupIntent reports whether new work must be refused for a
// Session whose deletion has crossed the durable intent boundary.
func (s *Store) HasSessionCleanupIntent(ctx context.Context, namespace, sessionName string) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_cleanup_intents WHERE namespace = ? AND session_name = ?`,
		strings.TrimSpace(namespace), strings.TrimSpace(sessionName),
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// PrepareSessionCleanup atomically re-proves SQLite quiescence and records the
// exact Kubernetes cleanup plan before any authoritative object is deleted.
func (s *Store) PrepareSessionCleanup(ctx context.Context, intent store.SessionCleanupIntent) (*store.SessionCleanupIntent, error) {
	if err := normalizeSessionCleanupIntent(&intent); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if existing, getErr := getSessionCleanupIntentTx(ctx, tx, intent.Namespace, intent.SessionName); getErr == nil {
		if existing.OperationID != intent.OperationID || existing.OperationDigest != intent.OperationDigest {
			return nil, store.ConflictErrorf("session cleanup intent for %s/%s belongs to a different operation", intent.Namespace, intent.SessionName)
		}
		return existing, nil
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}
	if err := validateSessionCleanupEligibilityTx(ctx, tx, intent); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_cleanup_intents(namespace, session_name, operation_id, operation_digest, plan, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		intent.Namespace, intent.SessionName, intent.OperationID, intent.OperationDigest, encoded, intent.PreparedAt,
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &intent, nil
}

// CompleteSessionCleanup removes SQLite-owned turn/outbox/transcript state only
// after the caller has reclaimed the exact Kubernetes plan in the intent.
//
//nolint:gocyclo // Receipt, transcript, projection, cursor, and intent deletion must remain one auditable transaction.
func (s *Store) CompleteSessionCleanup(ctx context.Context, request store.CompleteSessionCleanupRequest) error {
	namespace := strings.TrimSpace(request.Namespace)
	if request.Namespace != namespace {
		return store.ValidationErrorf("session namespace must not contain surrounding whitespace")
	}
	sessionName := strings.TrimSpace(request.SessionName)
	if request.SessionName != sessionName {
		return store.ValidationErrorf("session name must not contain surrounding whitespace")
	}
	request.Namespace = namespace
	request.SessionName = sessionName
	request.OperationID = strings.TrimSpace(request.OperationID)
	request.OperationDigest = strings.TrimSpace(request.OperationDigest)
	for field, value := range map[string]string{
		"session namespace": request.Namespace, "session name": request.SessionName,
		"session cleanup operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := store.ValidateCanonicalDigest("session cleanup operation digest", request.OperationDigest); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	intent, err := getSessionCleanupIntentTx(ctx, tx, request.Namespace, request.SessionName)
	if errors.Is(err, store.ErrNotFound) {
		completion, completionErr := getSessionCleanupCompletionTx(ctx, tx, request.Namespace, request.SessionName)
		if completionErr == nil && completion.OperationID == request.OperationID && completion.OperationDigest == request.OperationDigest {
			return nil
		}
		if completionErr == nil {
			return store.ConflictErrorf("session cleanup completion belongs to a different operation")
		}
		if !errors.Is(completionErr, store.ErrNotFound) {
			return completionErr
		}
		return store.ConflictErrorf("session %s/%s has no durable cleanup intent or completion receipt", request.Namespace, request.SessionName)
	}
	if err != nil {
		return err
	}
	if intent.OperationID != request.OperationID || intent.OperationDigest != request.OperationDigest {
		return store.ConflictErrorf("session cleanup completion does not match the durable intent")
	}
	var sessionCount int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sessions WHERE namespace = ? AND name = ?`, request.Namespace, request.SessionName,
	).Scan(&sessionCount); err != nil {
		return err
	}
	if sessionCount == 0 && intent.SessionUID == "" {
		return store.ErrNotFound
	}
	if err := validateSessionCleanupEligibilityTx(ctx, tx, *intent); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM outbox_projections
		 WHERE aggregate_kind = ?
		   AND aggregate_id IN (SELECT id FROM session_turns WHERE namespace = ? AND session_name = ?)`,
		sessionTurnAggregateKind, request.Namespace, request.SessionName,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM session_turns WHERE namespace = ? AND session_name = ?`, request.Namespace, request.SessionName,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM session_controls WHERE namespace = ? AND session_name = ?`, request.Namespace, request.SessionName,
	); err != nil {
		return err
	}
	completedAt := time.Now().UTC()
	receiptResult, err := tx.ExecContext(ctx,
		`INSERT INTO session_cleanup_completions(namespace, session_name, session_uid, operation_id, operation_digest, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(namespace, session_name) DO UPDATE SET
		   session_uid = excluded.session_uid,
		   operation_id = excluded.operation_id,
		   operation_digest = excluded.operation_digest,
		   completed_at = excluded.completed_at
		 WHERE session_cleanup_completions.session_uid = excluded.session_uid
		   AND session_cleanup_completions.operation_id = excluded.operation_id
		   AND session_cleanup_completions.operation_digest = excluded.operation_digest`,
		request.Namespace, request.SessionName, intent.SessionUID, request.OperationID, request.OperationDigest, completedAt,
	)
	if err != nil {
		return err
	}
	if rows, rowsErr := receiptResult.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if rows != 1 {
		return store.ConflictErrorf("session cleanup completion belongs to a different operation")
	}
	result, err := tx.ExecContext(ctx,
		`DELETE FROM sessions WHERE namespace = ? AND name = ? AND session_type <> ?`,
		request.Namespace, request.SessionName, store.SessionTypeGateway,
	)
	if err != nil {
		return err
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if deleted != sessionCount {
		return store.ConflictErrorf("session %s/%s changed before transcript cleanup", request.Namespace, request.SessionName)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE execution_events SET session_name = '', session_seq = 0 WHERE namespace = ? AND session_name = ?`,
		request.Namespace, request.SessionName,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM execution_event_session_sequences WHERE namespace = ? AND session_name = ?`,
		request.Namespace, request.SessionName,
	); err != nil {
		return err
	}
	intentResult, err := tx.ExecContext(ctx,
		`DELETE FROM session_cleanup_intents
		 WHERE namespace = ? AND session_name = ? AND operation_id = ? AND operation_digest = ?`,
		request.Namespace, request.SessionName, request.OperationID, request.OperationDigest,
	)
	if err != nil {
		return err
	}
	if rows, rowsErr := intentResult.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if rows != 1 {
		return store.ConflictErrorf("session cleanup intent changed before completion")
	}
	return tx.Commit()
}

//nolint:gocyclo // The durable cross-store plan validates each independent exact fence in one place.
func normalizeSessionCleanupIntent(intent *store.SessionCleanupIntent) error {
	namespace := strings.TrimSpace(intent.Namespace)
	if intent.Namespace != namespace {
		return store.ValidationErrorf("session namespace must not contain surrounding whitespace")
	}
	sessionName := strings.TrimSpace(intent.SessionName)
	if intent.SessionName != sessionName {
		return store.ValidationErrorf("session name must not contain surrounding whitespace")
	}
	intent.Namespace = namespace
	intent.SessionName = sessionName
	intent.SessionUID = strings.TrimSpace(intent.SessionUID)
	intent.ControlObjectUID = strings.TrimSpace(intent.ControlObjectUID)
	intent.ControlRequestDigest = strings.TrimSpace(intent.ControlRequestDigest)
	intent.LeaseName = strings.TrimSpace(intent.LeaseName)
	intent.LeaseObjectUID = strings.TrimSpace(intent.LeaseObjectUID)
	intent.OperationID = strings.TrimSpace(intent.OperationID)
	intent.OperationDigest = strings.TrimSpace(intent.OperationDigest)
	for field, value := range map[string]string{
		"session namespace": intent.Namespace, "session name": intent.SessionName,
		"session cleanup operation ID": intent.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := store.ValidateCanonicalDigest("session cleanup operation digest", intent.OperationDigest); err != nil {
		return err
	}
	if intent.PreparedAt.IsZero() {
		return store.ValidationErrorf("session cleanup preparation time is required")
	}
	intent.PreparedAt = intent.PreparedAt.UTC()
	if intent.SessionUID == "" {
		if intent.ControlObjectUID != "" || intent.ControlRequestDigest != "" || intent.ExpectedControlVersion != 0 ||
			intent.ExpectedLeaseGeneration != 0 || intent.LeaseName != "" || intent.LeaseObjectUID != "" ||
			len(intent.BranchClaims) != 0 || intent.ExpectedVerifiedBaseline != nil {
			return store.ValidationErrorf("transcript-only session cleanup must not carry Kubernetes control fences")
		}
		return nil
	}
	if err := store.ValidateControlIdentifier("session UID", intent.SessionUID); err != nil {
		return err
	}
	if intent.ControlRequestDigest == "" {
		if intent.ControlObjectUID != "" || intent.ExpectedControlVersion != 0 ||
			intent.ExpectedControlLastOperationID != "" || intent.ExpectedControlLastDigest != "" ||
			intent.ExpectedVerifiedBaseline != nil {
			return store.ValidationErrorf("UID-only session cleanup must not carry RuntimeSessionControl fences")
		}
	} else {
		if err := store.ValidateCanonicalDigest("session control request digest", intent.ControlRequestDigest); err != nil {
			return err
		}
		if intent.ExpectedControlVersion < 1 {
			return store.ValidationErrorf("session cleanup control version must be at least one")
		}
	}
	if intent.LeaseName == "" {
		if intent.LeaseObjectUID != "" || intent.ExpectedLeaseGeneration != 0 {
			return store.ValidationErrorf("session cleanup Lease fence is incomplete")
		}
	} else {
		if intent.ExpectedLeaseGeneration < 1 {
			return store.ValidationErrorf("session cleanup Lease generation must be at least one")
		}
		if err := store.ValidateControlIdentifier("session Lease name", intent.LeaseName); err != nil {
			return err
		}
	}
	for i := range intent.BranchClaims {
		claim := &intent.BranchClaims[i]
		claim.ID = strings.TrimSpace(claim.ID)
		claim.ExpectedRepositoryID = strings.TrimSpace(claim.ExpectedRepositoryID)
		claim.ExpectedRef = strings.TrimSpace(claim.ExpectedRef)
		claim.ExpectedOwnerUID = strings.TrimSpace(claim.ExpectedOwnerUID)
		claim.ExpectedRequestDigest = strings.TrimSpace(claim.ExpectedRequestDigest)
		claim.ExpectedBlockedReason = strings.TrimSpace(claim.ExpectedBlockedReason)
		claim.ExpectedPublicationID = strings.TrimSpace(claim.ExpectedPublicationID)
		if claim.ExpectedOwnerUID != intent.SessionUID {
			return store.ValidationErrorf("session cleanup branch claim owner does not match Session UID")
		}
		switch claim.ExpectedAvailability {
		case store.BranchClaimAvailable:
			if claim.ExpectedBlockedReason != "" || claim.ExpectedPublicationID != "" {
				return store.ValidationErrorf("available session cleanup branch claim carries blocked state")
			}
		default:
			return store.ValidationErrorf("session cleanup branch claim has unsupported availability %q", claim.ExpectedAvailability)
		}
		if claim.ExpectedVersion < 1 || claim.ExpectedGeneration < 1 {
			return store.ValidationErrorf("session cleanup branch claim version and generation must be at least one")
		}
		if err := store.ValidateControlIdentifier("branch claim ID", claim.ID); err != nil {
			return err
		}
		if err := store.ValidateControlIdentifier("publication repository ID", claim.ExpectedRepositoryID); err != nil {
			return err
		}
		if err := store.ValidateFullBranchRef(claim.ExpectedRef); err != nil {
			return err
		}
		if err := claim.ExpectedLastVerified.Validate("session cleanup branch baseline"); err != nil {
			return err
		}
		if err := store.ValidateCanonicalDigest("session cleanup branch request digest", claim.ExpectedRequestDigest); err != nil {
			return err
		}
	}
	return nil
}

func validateSessionCleanupEligibilityTx(ctx context.Context, tx *sql.Tx, intent store.SessionCleanupIntent) error {
	var sessionType, activeTask, activeTaskUID, controlSessionUID string
	var activeTaskExpiresAt sql.NullTime
	if err := tx.QueryRowContext(ctx,
		`SELECT session_type, active_task, active_task_uid, active_task_expires_at, control_session_uid
		 FROM sessions WHERE namespace = ? AND name = ?`,
		intent.Namespace, intent.SessionName,
	).Scan(&sessionType, &activeTask, &activeTaskUID, &activeTaskExpiresAt, &controlSessionUID); errors.Is(err, sql.ErrNoRows) {
		if intent.SessionUID != "" {
			return nil
		}
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	needsTurnIdentityProof := false
	if intent.SessionUID != "" {
		if controlSessionUID == "" {
			needsTurnIdentityProof = true
		} else if controlSessionUID != intent.SessionUID {
			return store.ConflictErrorf("session %s/%s transcript is not bound to Kubernetes Session UID %q", intent.Namespace, intent.SessionName, intent.SessionUID)
		}
	} else if controlSessionUID != "" {
		return store.ConflictErrorf("session %s/%s has Kubernetes cleanup identity %q but no control plan", intent.Namespace, intent.SessionName, controlSessionUID)
	}
	if sessionType == store.SessionTypeGateway {
		return store.ErrGatewayOwnedSession
	}
	if err := ensureSessionLockQuiescentTx(
		ctx, tx, intent.Namespace, intent.SessionName, activeTask, activeTaskUID, activeTaskExpiresAt,
	); err != nil {
		return err
	}
	var pendingGatewayEvents int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM gateway_events
		WHERE namespace = ? AND session_name = ? AND state IN (?, ?, ?, ?)`,
		intent.Namespace, intent.SessionName, store.GatewayEventAccepted, store.GatewayEventQueued,
		store.GatewayEventDispatching, store.GatewayEventTaskCreated,
	).Scan(&pendingGatewayEvents); err != nil {
		return err
	}
	if pendingGatewayEvents > 0 {
		return store.ErrConflict
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT session_uid, state, terminal_kind, terminal_content
		 FROM session_turns WHERE namespace = ? AND session_name = ?`,
		intent.Namespace, intent.SessionName,
	)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck
	turnCount := 0
	for rows.Next() {
		turnCount++
		var sessionUID string
		var state store.SessionTurnState
		var terminalKind store.SessionTurnTerminalKind
		var terminalContent string
		if err := rows.Scan(&sessionUID, &state, &terminalKind, &terminalContent); err != nil {
			return err
		}
		if intent.SessionUID == "" || sessionUID != intent.SessionUID || state != store.SessionTurnFinalized {
			return store.ErrConflict
		}
		if terminalKind == store.SessionTurnOutcomeMarker {
			var marker struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal([]byte(terminalContent), &marker); err != nil || marker.Kind == "OutcomeUnknown" {
				return store.ErrConflict
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if intent.SessionUID == "" && turnCount > 0 {
		return store.ErrConflict
	}
	if needsTurnIdentityProof && turnCount == 0 {
		return store.ConflictErrorf("legacy session %s/%s has no SessionTurn proof for Kubernetes Session UID %q", intent.Namespace, intent.SessionName, intent.SessionUID)
	}
	var unsettledProjections int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*)
		 FROM outbox_projections AS projections
		 JOIN session_turns AS turns ON projections.aggregate_kind = ? AND projections.aggregate_id = turns.id
		 WHERE turns.namespace = ? AND turns.session_name = ?
		   AND projections.state NOT IN (?, ?)`,
		sessionTurnAggregateKind, intent.Namespace, intent.SessionName,
		store.OutboxProjectionDelivered, store.OutboxProjectionDeadLetter,
	).Scan(&unsettledProjections); err != nil {
		return err
	}
	if unsettledProjections > 0 {
		return store.ErrConflict
	}
	return nil
}

func getSessionCleanupIntentTx(ctx context.Context, tx *sql.Tx, namespace, sessionName string) (*store.SessionCleanupIntent, error) {
	var encoded []byte
	err := tx.QueryRowContext(ctx,
		`SELECT plan FROM session_cleanup_intents WHERE namespace = ? AND session_name = ?`,
		namespace, sessionName,
	).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var intent store.SessionCleanupIntent
	if err := json.Unmarshal(encoded, &intent); err != nil {
		return nil, err
	}
	return &intent, nil
}

func getSessionCleanupCompletionTx(ctx context.Context, tx *sql.Tx, namespace, sessionName string) (*store.SessionCleanupCompletion, error) {
	completion := &store.SessionCleanupCompletion{}
	err := tx.QueryRowContext(ctx,
		`SELECT namespace, session_name, session_uid, operation_id, operation_digest, completed_at
		 FROM session_cleanup_completions WHERE namespace = ? AND session_name = ?`,
		namespace, sessionName,
	).Scan(&completion.Namespace, &completion.SessionName, &completion.SessionUID, &completion.OperationID, &completion.OperationDigest, &completion.CompletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	completion.CompletedAt = completion.CompletedAt.UTC()
	return completion, nil
}
