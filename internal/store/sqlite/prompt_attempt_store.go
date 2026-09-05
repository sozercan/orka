package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.PromptAttemptStore = (*Store)(nil)

// CreatePromptAttempt inserts a canonical prompt attempt. Repeating the exact
// immutable request, binding, and snapshot digests returns the committed row;
// reusing the identity for different immutable input conflicts.
func (s *Store) CreatePromptAttempt(ctx context.Context, attempt *store.PromptAttempt, fence store.ControllerEpochFence) (*store.PromptAttempt, error) {
	normalized, fence, err := store.NormalizePromptAttemptForCreate(attempt, fence)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return nil, err
	}
	if _, markerErr := getPromptAttemptReclamationMarkerSQLite(ctx, tx, normalized.Key.Namespace, normalized.Key.TaskUID); markerErr == nil {
		return nil, store.ConflictErrorf("Task %q has already prepared PromptAttempt reclamation", normalized.Key.TaskUID)
	} else if !errors.Is(markerErr, store.ErrNotFound) {
		return nil, markerErr
	}
	if existing, getErr := getPromptAttempt(ctx, tx, normalized.ID); getErr == nil {
		if samePromptAttemptCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("prompt attempt %q was reused with a different request digest or immutable identity", normalized.ID)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO prompt_attempts(
			id, namespace, task_uid, attempt, prompt_id, session_uid, session_lease_generation,
			runtime_instance_id, request_digest, binding_digest, snapshot_digest,
			execution_state, delivery_state, terminal_reason,
			outcome_marker, controller_epoch_name, controller_epoch, last_operation_id,
			last_operation_digest, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID, normalized.Key.Namespace, normalized.Key.TaskUID, normalized.Key.Attempt, normalized.Key.PromptID,
		normalized.SessionUID, normalized.SessionLeaseGeneration, normalized.RuntimeInstanceID, normalized.RequestDigest,
		normalized.BindingDigest, normalized.SnapshotDigest,
		string(normalized.ExecutionState), string(normalized.DeliveryState), normalized.TerminalReason, normalized.OutcomeMarker,
		normalized.ControllerEpochName, normalized.ControllerEpoch, normalized.LastOperationID,
		normalized.LastOperationDigest, normalized.Version, normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, err
		}
		existing, getErr := getPromptAttempt(ctx, tx, normalized.ID)
		if getErr != nil {
			return nil, store.ConflictErrorf("prompt attempt %q already exists with a conflicting identity", normalized.ID)
		}
		if samePromptAttemptCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("prompt attempt %q was reused with a different request digest or immutable identity", normalized.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &normalized, nil
}

// GetPromptAttempt returns one durable prompt attempt.
func (s *Store) GetPromptAttempt(ctx context.Context, id string) (*store.PromptAttempt, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("prompt attempt ID", id); err != nil {
		return nil, err
	}
	attempt, err := getPromptAttempt(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &attempt, nil
}

type promptAttemptReclamationMarker struct {
	Namespace                           string
	TaskName                            string
	TaskUID                             string
	Mode                                store.PromptAttemptReclamationMode
	ContinuitySession                   bool
	FinalContinuitySession              bool
	FinalPromptAttemptID                string
	TerminalProjectionID                string
	FinalSessionTurnID                  string
	TerminalProjectionAggregateKind     string
	TerminalProjectionAggregateID       string
	CandidateIDs                        []string
	RequestedExternalEffectAggregateIDs []string
	RelatedExternalEffectAggregateIDs   []string
	ControllerEpochName                 string
	ControllerEpoch                     int64
	CreatedAt                           time.Time
}

// PreparePromptAttemptReclamation proves and durably records the complete
// task-scoped retention barrier without deleting PromptAttempts.
func (s *Store) PreparePromptAttemptReclamation(ctx context.Context, request store.ReclaimPromptAttemptsRequest) error {
	_, err := s.runPromptAttemptReclamation(ctx, request, false)
	return err
}

// ReclaimPromptAttempts is the SQLite compatibility implementation of the
// task-scoped retention contract. Marker creation and deletion are committed in
// one transaction for direct callers; controller callers may prepare first so
// artifact retirement happens between durable authorization and deletion.
//
//nolint:gocyclo // Retention keeps every cross-aggregate safety barrier explicit and auditable in one transaction.
func (s *Store) ReclaimPromptAttempts(ctx context.Context, request store.ReclaimPromptAttemptsRequest) (int, error) {
	return s.runPromptAttemptReclamation(ctx, request, true)
}

func (s *Store) runPromptAttemptReclamation(ctx context.Context, request store.ReclaimPromptAttemptsRequest, deleteAttempts bool) (int, error) {
	normalized, err := normalizePromptAttemptReclamationRequestSQLite(request)
	if err != nil {
		return 0, err
	}
	fence, err := store.NormalizeEpochFence(normalized.Fence)
	if err != nil {
		return 0, err
	}
	normalized.Fence = fence

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, normalized.Fence); err != nil {
		return 0, err
	}
	marker, attempts, err := preparePromptAttemptReclamationSQLite(ctx, tx, normalized)
	if err != nil {
		return 0, err
	}
	if !deleteAttempts {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if len(attempts) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return 0, nil
	}
	authorized := make(map[string]struct{}, len(marker.CandidateIDs))
	for _, id := range marker.CandidateIDs {
		authorized[id] = struct{}{}
	}
	for _, attempt := range attempts {
		if _, ok := authorized[attempt.ID]; !ok {
			return 0, store.ConflictErrorf("PromptAttempt %q was created after Task reclamation was prepared", attempt.ID)
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM prompt_attempts WHERE namespace = ? AND task_uid = ?`, normalized.Namespace, normalized.TaskUID)
	if err != nil {
		return 0, fmt.Errorf("delete reclaimed prompt attempts: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count reclaimed prompt attempts: %w", err)
	}
	if deleted != int64(len(attempts)) {
		return 0, store.ConflictErrorf("prompt attempt reclamation deleted %d rows, expected %d", deleted, len(attempts))
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(deleted), nil
}

func normalizePromptAttemptReclamationRequestSQLite(request store.ReclaimPromptAttemptsRequest) (store.ReclaimPromptAttemptsRequest, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.TaskName = strings.TrimSpace(request.TaskName)
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	for field, value := range map[string]string{
		"prompt attempt namespace": request.Namespace,
		"prompt attempt Task name": request.TaskName,
		"prompt attempt Task UID":  request.TaskUID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return store.ReclaimPromptAttemptsRequest{}, err
		}
	}
	switch request.Mode {
	case store.PromptAttemptReclamationProjected, store.PromptAttemptReclamationUnbound, store.PromptAttemptReclamationNoAttempt:
	default:
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("unsupported prompt attempt reclamation mode %q", request.Mode)
	}
	if request.FinalContinuitySession && (!request.ContinuitySession || request.Mode != store.PromptAttemptReclamationProjected) {
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("final continuity SessionTurn binding requires projected continuity reclamation")
	}
	request.FinalPromptAttemptID = strings.TrimSpace(request.FinalPromptAttemptID)
	request.TerminalProjectionID = strings.TrimSpace(request.TerminalProjectionID)
	if request.Mode == store.PromptAttemptReclamationProjected {
		if err := store.ValidateControlIdentifier("final prompt attempt ID", request.FinalPromptAttemptID); err != nil {
			return store.ReclaimPromptAttemptsRequest{}, err
		}
		if request.TerminalProjectionID != "" {
			if err := store.ValidateControlIdentifier("terminal projection ID", request.TerminalProjectionID); err != nil {
				return store.ReclaimPromptAttemptsRequest{}, err
			}
		}
	} else if request.FinalPromptAttemptID != "" || request.TerminalProjectionID != "" {
		return store.ReclaimPromptAttemptsRequest{}, store.ValidationErrorf("only projected prompt attempt reclamation accepts final attempt and projection IDs")
	}
	request.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDsSQLite(request.RelatedExternalEffectAggregateIDs)
	for _, id := range request.RelatedExternalEffectAggregateIDs {
		if err := store.ValidateControlIdentifier("related external effect aggregate ID", id); err != nil {
			return store.ReclaimPromptAttemptsRequest{}, err
		}
	}
	return request, nil
}

func normalizePromptAttemptReclamationIDsSQLite(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func preparePromptAttemptReclamationSQLite(
	ctx context.Context,
	tx *sql.Tx,
	request store.ReclaimPromptAttemptsRequest,
) (promptAttemptReclamationMarker, []store.PromptAttempt, error) {
	attempts, err := listTaskPromptAttemptsSQLite(ctx, tx, request.Namespace, request.TaskUID)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	marker, err := getPromptAttemptReclamationMarkerSQLite(ctx, tx, request.Namespace, request.TaskUID)
	if err == nil {
		if err := validatePromptAttemptReclamationMarkerRequestSQLite(marker, request); err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
		if err := verifyPreparedPromptAttemptReclamationSQLite(ctx, tx, marker, attempts); err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
		return marker, attempts, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return promptAttemptReclamationMarker{}, nil, err
	}
	if request.Mode == store.PromptAttemptReclamationUnbound {
		attempts, err = settleUnboundPromptAttemptsForReclamationSQLite(ctx, tx, attempts, request.Fence)
		if err != nil {
			return promptAttemptReclamationMarker{}, nil, err
		}
	}
	marker, err = buildPromptAttemptReclamationMarkerSQLite(ctx, tx, request, attempts)
	if err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	if err := insertPromptAttemptReclamationMarkerSQLite(ctx, tx, marker); err != nil {
		return promptAttemptReclamationMarker{}, nil, err
	}
	return marker, attempts, nil
}

func listTaskPromptAttemptsSQLite(ctx context.Context, tx *sql.Tx, namespace, taskUID string) ([]store.PromptAttempt, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM prompt_attempts WHERE namespace = ? AND task_uid = ? ORDER BY attempt, id`, namespace, taskUID)
	if err != nil {
		return nil, fmt.Errorf("list prompt attempts for Task reclamation: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan prompt attempt for Task reclamation: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list prompt attempts for Task reclamation: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close prompt attempt reclamation rows: %w", err)
	}
	attempts := make([]store.PromptAttempt, 0, len(ids))
	for _, id := range ids {
		attempt, err := getPromptAttempt(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

func settleUnboundPromptAttemptsForReclamationSQLite(
	ctx context.Context,
	tx *sql.Tx,
	attempts []store.PromptAttempt,
	fence store.ControllerEpochFence,
) ([]store.PromptAttempt, error) {
	result := make([]store.PromptAttempt, 0, len(attempts))
	var newestID string
	if len(attempts) > 0 {
		newest, err := newestPromptAttemptForReclamationSQLite(append([]store.PromptAttempt(nil), attempts...))
		if err != nil {
			return nil, err
		}
		newestID = newest.ID
	}
	for _, attempt := range attempts {
		if attempt.ID != newestID {
			if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
				return nil, sqlitePromptAttemptReclaimNotReady("historical prompt attempt %q is not terminal", attempt.ID)
			}
			result = append(result, attempt)
			continue
		}
		if attempt.SessionUID != "" || attempt.SessionLeaseGeneration != 0 || attempt.RuntimeInstanceID != "" {
			return nil, sqlitePromptAttemptReclaimNotReady("unbound prompt attempt %q already has a runtime or Session binding", attempt.ID)
		}
		if store.IsTerminalPromptExecutionState(attempt.ExecutionState) {
			if !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
				return nil, sqlitePromptAttemptReclaimNotReady("unbound prompt attempt %q delivery is not terminal", attempt.ID)
			}
			result = append(result, attempt)
			continue
		}
		if attempt.ExecutionState != store.PromptExecutionQueued || attempt.DeliveryState != store.PromptDeliveryNotRequested {
			return nil, sqlitePromptAttemptReclaimNotReady("unbound prompt attempt %q is not safely cancellable from state %s", attempt.ID, attempt.ExecutionState)
		}
		operationID := store.CanonicalControlID("reclaim-unbound-cancel", attempt.ID)
		operationDigest := store.CanonicalBytesDigest([]byte("reclaim-unbound-cancel:" + attempt.ID))
		updatedAt := store.NormalizeControlTime(time.Time{})
		resultSQL, err := tx.ExecContext(ctx,
			`UPDATE prompt_attempts
			 SET execution_state = 'Cancelled', terminal_reason = ?, controller_epoch_name = ?, controller_epoch = ?,
			     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
			 WHERE id = ? AND version = ? AND execution_state = 'Queued'`,
			"Task deleted before durable execution status binding", fence.Name, fence.Epoch,
			operationID, operationDigest, updatedAt, attempt.ID, attempt.Version,
		)
		if err != nil {
			return nil, err
		}
		if err := rowsAffectedExactlyOne(resultSQL, "cancel unbound prompt attempt for reclamation"); err != nil {
			return nil, err
		}
		attempt, err = getPromptAttempt(ctx, tx, attempt.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, attempt)
	}
	return result, nil
}

func buildPromptAttemptReclamationMarkerSQLite(
	ctx context.Context,
	tx *sql.Tx,
	request store.ReclaimPromptAttemptsRequest,
	attempts []store.PromptAttempt,
) (promptAttemptReclamationMarker, error) {
	effectiveMode := request.Mode
	if effectiveMode == store.PromptAttemptReclamationUnbound && len(attempts) == 0 {
		effectiveMode = store.PromptAttemptReclamationNoAttempt
	}
	marker := promptAttemptReclamationMarker{
		Namespace: request.Namespace, TaskName: request.TaskName, TaskUID: request.TaskUID,
		Mode: effectiveMode, ContinuitySession: request.ContinuitySession,
		FinalContinuitySession:              request.FinalContinuitySession,
		RequestedExternalEffectAggregateIDs: append([]string(nil), request.RelatedExternalEffectAggregateIDs...),
		ControllerEpochName:                 request.Fence.Name, ControllerEpoch: request.Fence.Epoch, CreatedAt: store.NormalizeControlTime(time.Time{}),
	}
	if effectiveMode == store.PromptAttemptReclamationNoAttempt {
		if len(attempts) != 0 {
			return promptAttemptReclamationMarker{}, store.ConflictErrorf("Task %q declared no durable attempt but owns %d PromptAttempts", request.TaskUID, len(attempts))
		}
		marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDsSQLite(append([]string{request.TaskUID}, request.RelatedExternalEffectAggregateIDs...))
		if _, err := verifyPromptAttemptReferencesSQLite(ctx, tx, marker, nil); err != nil {
			return promptAttemptReclamationMarker{}, err
		}
		return marker, nil
	}
	if len(attempts) == 0 {
		return promptAttemptReclamationMarker{}, sqlitePromptAttemptReclaimNotReady("Task %q has no durable PromptAttempt and no reclamation marker", request.TaskUID)
	}
	for _, attempt := range attempts {
		if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			return promptAttemptReclamationMarker{}, sqlitePromptAttemptReclaimNotReady("prompt attempt %q is not terminal", attempt.ID)
		}
		marker.CandidateIDs = append(marker.CandidateIDs, attempt.ID)
		if attempt.SessionUID != "" {
			marker.RelatedExternalEffectAggregateIDs = append(marker.RelatedExternalEffectAggregateIDs, attempt.SessionUID)
		}
	}
	marker.CandidateIDs = normalizePromptAttemptReclamationIDsSQLite(marker.CandidateIDs)
	marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDsSQLite(append(marker.RelatedExternalEffectAggregateIDs, append([]string{request.TaskUID}, request.RelatedExternalEffectAggregateIDs...)...))
	finalAttempt, err := newestPromptAttemptForReclamationSQLite(attempts)
	if err != nil {
		return promptAttemptReclamationMarker{}, err
	}
	marker.FinalPromptAttemptID = finalAttempt.ID
	if request.Mode == store.PromptAttemptReclamationProjected && request.FinalPromptAttemptID != finalAttempt.ID {
		return promptAttemptReclamationMarker{}, store.ConflictErrorf("final prompt attempt %q is not the newest Task attempt %q", request.FinalPromptAttemptID, finalAttempt.ID)
	}
	turns, err := verifyPromptAttemptReferencesSQLite(ctx, tx, marker, attempts)
	if err != nil {
		return promptAttemptReclamationMarker{}, err
	}
	if request.Mode == store.PromptAttemptReclamationProjected {
		if request.TerminalProjectionID == "" {
			return promptAttemptReclamationMarker{}, store.ValidationErrorf("terminal projection ID is required before preparing projected reclamation")
		}
		marker.TerminalProjectionID = request.TerminalProjectionID
		if request.FinalContinuitySession {
			turn, ok := turns[finalAttempt.ID]
			if !ok {
				return promptAttemptReclamationMarker{}, sqlitePromptAttemptReclaimNotReady("final continuity prompt attempt %q has no finalized SessionTurn", finalAttempt.ID)
			}
			marker.FinalSessionTurnID = turn.ID
			marker.TerminalProjectionAggregateKind = "SessionTurn"
			marker.TerminalProjectionAggregateID = turn.ID
		} else {
			expectedID := store.CanonicalControlID("task-terminal-projection", request.Namespace, request.TaskUID, fmt.Sprint(finalAttempt.Key.Attempt))
			if request.TerminalProjectionID != expectedID {
				return promptAttemptReclamationMarker{}, store.ConflictErrorf("terminal projection %q does not match newest standalone prompt attempt %q", request.TerminalProjectionID, finalAttempt.ID)
			}
			marker.TerminalProjectionAggregateKind = "Task"
			marker.TerminalProjectionAggregateID = request.TaskUID
		}
		if err := verifyPromptAttemptTerminalProjectionMarkerSQLite(ctx, tx, marker); err != nil {
			return promptAttemptReclamationMarker{}, err
		}
	}
	return marker, nil
}

func newestPromptAttemptForReclamationSQLite(attempts []store.PromptAttempt) (store.PromptAttempt, error) {
	if len(attempts) == 0 {
		return store.PromptAttempt{}, store.ErrNotFound
	}
	copyAttempts := append([]store.PromptAttempt(nil), attempts...)
	sort.Slice(copyAttempts, func(i, j int) bool {
		if copyAttempts[i].Key.Attempt != copyAttempts[j].Key.Attempt {
			return copyAttempts[i].Key.Attempt > copyAttempts[j].Key.Attempt
		}
		return copyAttempts[i].ID < copyAttempts[j].ID
	})
	if len(copyAttempts) > 1 && copyAttempts[0].Key.Attempt == copyAttempts[1].Key.Attempt {
		return store.PromptAttempt{}, store.ConflictErrorf("Task %q has multiple PromptAttempts at newest attempt %d", copyAttempts[0].Key.TaskUID, copyAttempts[0].Key.Attempt)
	}
	return copyAttempts[0], nil
}

func verifyPreparedPromptAttemptReclamationSQLite(
	ctx context.Context,
	tx *sql.Tx,
	marker promptAttemptReclamationMarker,
	attempts []store.PromptAttempt,
) error {
	authorized := make(map[string]struct{}, len(marker.CandidateIDs))
	for _, id := range marker.CandidateIDs {
		authorized[id] = struct{}{}
	}
	for _, attempt := range attempts {
		if _, ok := authorized[attempt.ID]; !ok {
			return store.ConflictErrorf("PromptAttempt %q was created after Task reclamation was prepared", attempt.ID)
		}
		if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
			return sqlitePromptAttemptReclaimNotReady("prompt attempt %q is not terminal", attempt.ID)
		}
	}
	if marker.Mode == store.PromptAttemptReclamationNoAttempt && len(attempts) != 0 {
		return store.ConflictErrorf("no-attempt reclamation marker now has PromptAttempt candidates")
	}
	if _, err := verifyPromptAttemptReferencesSQLite(ctx, tx, marker, attempts); err != nil {
		return err
	}
	if marker.Mode == store.PromptAttemptReclamationProjected {
		return verifyPromptAttemptTerminalProjectionMarkerSQLite(ctx, tx, marker)
	}
	return nil
}

func verifyPromptAttemptReferencesSQLite(
	ctx context.Context,
	tx *sql.Tx,
	marker promptAttemptReclamationMarker,
	attempts []store.PromptAttempt,
) (map[string]store.SessionTurn, error) {
	protected := make(map[string]struct{}, len(marker.CandidateIDs))
	for _, id := range marker.CandidateIDs {
		protected[id] = struct{}{}
	}
	controlRows, err := tx.QueryContext(ctx,
		`SELECT session_name, availability, lease_task_uid, related_prompt_attempt_id
		 FROM session_controls WHERE namespace = ? AND (lease_task_uid = ? OR related_prompt_attempt_id <> '')`,
		marker.Namespace, marker.TaskUID,
	)
	if err != nil {
		return nil, fmt.Errorf("list Session references for prompt attempt reclamation: %w", err)
	}
	for controlRows.Next() {
		var sessionName, leaseTaskUID, relatedAttemptID string
		var availability store.SessionAvailability
		if err := controlRows.Scan(&sessionName, &availability, &leaseTaskUID, &relatedAttemptID); err != nil {
			_ = controlRows.Close()
			return nil, fmt.Errorf("scan Session reference for prompt attempt reclamation: %w", err)
		}
		if leaseTaskUID == marker.TaskUID {
			_ = controlRows.Close()
			return nil, sqlitePromptAttemptReclaimNotReady("Session %s/%s still has a mutation lease for Task %q", marker.Namespace, sessionName, marker.TaskUID)
		}
		if _, related := protected[relatedAttemptID]; related && availability == store.SessionReconciliationBlocked {
			_ = controlRows.Close()
			return nil, sqlitePromptAttemptReclaimNotReady("Session %s/%s still references prompt attempt %q", marker.Namespace, sessionName, relatedAttemptID)
		}
	}
	if err := controlRows.Err(); err != nil {
		_ = controlRows.Close()
		return nil, fmt.Errorf("list Session references for prompt attempt reclamation: %w", err)
	}
	if err := controlRows.Close(); err != nil {
		return nil, fmt.Errorf("close Session reference reclamation rows: %w", err)
	}

	turns := make(map[string]store.SessionTurn)
	if marker.ContinuitySession {
		for _, attempt := range attempts {
			if attempt.SessionUID == "" && attempt.SessionLeaseGeneration == 0 {
				continue
			}
			if attempt.SessionUID == "" || attempt.SessionLeaseGeneration < 1 {
				return nil, store.ConflictErrorf("prompt attempt %q has an incomplete SessionTurn binding", attempt.ID)
			}
			key := store.SessionTurnKey{
				SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
				TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
			}
			turnID, err := key.CanonicalID()
			if err != nil {
				return nil, err
			}
			turn, err := getSessionTurn(ctx, tx, turnID)
			if errors.Is(err, store.ErrNotFound) {
				return nil, sqlitePromptAttemptReclaimNotReady("SessionTurn %q for prompt attempt %q is not durable", turnID, attempt.ID)
			}
			if err != nil {
				return nil, fmt.Errorf("load SessionTurn for prompt attempt reclamation: %w", err)
			}
			if turn.Key != key || turn.PromptAttemptID != attempt.ID {
				return nil, store.ConflictErrorf("SessionTurn %q does not match prompt attempt %q", turnID, attempt.ID)
			}
			if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil || strings.TrimSpace(turn.ProjectionID) == "" {
				return nil, sqlitePromptAttemptReclaimNotReady("SessionTurn %q for prompt attempt %q is not finalized", turnID, attempt.ID)
			}
			turns[attempt.ID] = turn
		}
	}
	if err := verifyPromptAttemptPublicationsAndEffectsSQLite(ctx, tx, marker); err != nil {
		return nil, err
	}
	return turns, nil
}

func verifyPromptAttemptPublicationsAndEffectsSQLite(ctx context.Context, tx *sql.Tx, marker promptAttemptReclamationMarker) error {
	relatedEffects := make(map[string]struct{}, len(marker.RelatedExternalEffectAggregateIDs)+1)
	relatedEffects[marker.TaskUID] = struct{}{}
	for _, id := range marker.RelatedExternalEffectAggregateIDs {
		relatedEffects[id] = struct{}{}
	}
	publicationRows, err := tx.QueryContext(ctx, `SELECT id, state FROM publications WHERE namespace = ? AND task_uid = ?`, marker.Namespace, marker.TaskUID)
	if err != nil {
		return fmt.Errorf("list publications for prompt attempt reclamation: %w", err)
	}
	for publicationRows.Next() {
		var id string
		var state store.PublicationState
		if err := publicationRows.Scan(&id, &state); err != nil {
			_ = publicationRows.Close()
			return fmt.Errorf("scan publication for prompt attempt reclamation: %w", err)
		}
		if !store.IsTerminalPublicationState(state) {
			_ = publicationRows.Close()
			return sqlitePromptAttemptReclaimNotReady("publication %q is not terminal", id)
		}
		relatedEffects[id] = struct{}{}
	}
	if err := publicationRows.Err(); err != nil {
		_ = publicationRows.Close()
		return fmt.Errorf("list publications for prompt attempt reclamation: %w", err)
	}
	if err := publicationRows.Close(); err != nil {
		return fmt.Errorf("close publication reclamation rows: %w", err)
	}

	effectRows, err := tx.QueryContext(ctx, `SELECT id, aggregate_id, state FROM external_effects WHERE namespace = ?`, marker.Namespace)
	if err != nil {
		return fmt.Errorf("list external effects for prompt attempt reclamation: %w", err)
	}
	for effectRows.Next() {
		var id, aggregateID string
		var state store.ExternalEffectState
		if err := effectRows.Scan(&id, &aggregateID, &state); err != nil {
			_ = effectRows.Close()
			return fmt.Errorf("scan external effect for prompt attempt reclamation: %w", err)
		}
		if _, related := relatedEffects[aggregateID]; !related {
			continue
		}
		switch state {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			_ = effectRows.Close()
			return sqlitePromptAttemptReclaimNotReady("external effect %q is not terminal", id)
		}
	}
	if err := effectRows.Err(); err != nil {
		_ = effectRows.Close()
		return fmt.Errorf("list external effects for prompt attempt reclamation: %w", err)
	}
	if err := effectRows.Close(); err != nil {
		return fmt.Errorf("close external effect reclamation rows: %w", err)
	}
	return nil
}

func verifyPromptAttemptTerminalProjectionMarkerSQLite(ctx context.Context, tx *sql.Tx, marker promptAttemptReclamationMarker) error {
	projection, err := getOutboxProjection(ctx, tx, marker.TerminalProjectionID)
	if errors.Is(err, store.ErrNotFound) {
		return sqlitePromptAttemptReclaimNotReady("terminal projection %q is not durable", marker.TerminalProjectionID)
	}
	if err != nil {
		return fmt.Errorf("load terminal projection for prompt attempt reclamation: %w", err)
	}
	if projection.State != store.OutboxProjectionDelivered {
		return sqlitePromptAttemptReclaimNotReady("terminal projection %q is not delivered", projection.ID)
	}
	if projection.ProjectionKind != "TaskTerminalStatus" ||
		projection.AggregateKind != marker.TerminalProjectionAggregateKind || projection.AggregateID != marker.TerminalProjectionAggregateID {
		return store.ConflictErrorf("terminal projection %q does not match the prepared Task finalization aggregate", projection.ID)
	}
	if marker.FinalSessionTurnID != "" {
		turn, err := getSessionTurn(ctx, tx, marker.FinalSessionTurnID)
		if err != nil {
			return fmt.Errorf("load final SessionTurn for prompt attempt reclamation: %w", err)
		}
		if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil || turn.ProjectionID != projection.ID {
			return store.ConflictErrorf("terminal projection %q does not match final SessionTurn %q", projection.ID, marker.FinalSessionTurnID)
		}
	}
	return nil
}

func validatePromptAttemptReclamationMarkerRequestSQLite(marker promptAttemptReclamationMarker, request store.ReclaimPromptAttemptsRequest) error {
	modeMatches := marker.Mode == request.Mode ||
		request.Mode == store.PromptAttemptReclamationUnbound && marker.Mode == store.PromptAttemptReclamationNoAttempt
	if marker.Namespace != request.Namespace || marker.TaskName != request.TaskName || marker.TaskUID != request.TaskUID ||
		!modeMatches || marker.ContinuitySession != request.ContinuitySession ||
		!reflect.DeepEqual(marker.RequestedExternalEffectAggregateIDs, request.RelatedExternalEffectAggregateIDs) {
		return store.ConflictErrorf("prompt attempt reclamation marker does not match the current Task request")
	}
	if request.Mode == store.PromptAttemptReclamationProjected &&
		(marker.FinalPromptAttemptID != request.FinalPromptAttemptID ||
			request.TerminalProjectionID != "" && (marker.TerminalProjectionID != request.TerminalProjectionID || marker.FinalContinuitySession != request.FinalContinuitySession)) {
		return store.ConflictErrorf("prompt attempt reclamation marker does not match the current final attempt/projection")
	}
	if marker.Mode == store.PromptAttemptReclamationProjected && len(marker.CandidateIDs) == 0 {
		return store.ConflictErrorf("prompt attempt reclamation marker has no projected candidate set")
	}
	return nil
}

func insertPromptAttemptReclamationMarkerSQLite(ctx context.Context, tx *sql.Tx, marker promptAttemptReclamationMarker) error {
	candidateJSON, err := json.Marshal(marker.CandidateIDs)
	if err != nil {
		return err
	}
	requestedEffectsJSON, err := json.Marshal(marker.RequestedExternalEffectAggregateIDs)
	if err != nil {
		return err
	}
	relatedEffectsJSON, err := json.Marshal(marker.RelatedExternalEffectAggregateIDs)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO prompt_attempt_reclamations(
			namespace, task_name, task_uid, mode, continuity_session, final_continuity_session, final_prompt_attempt_id,
			terminal_projection_id, final_session_turn_id, terminal_projection_aggregate_kind,
			terminal_projection_aggregate_id, candidate_ids, requested_external_effect_aggregate_ids,
			related_external_effect_aggregate_ids, controller_epoch_name, controller_epoch, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		marker.Namespace, marker.TaskName, marker.TaskUID, string(marker.Mode), marker.ContinuitySession, marker.FinalContinuitySession,
		marker.FinalPromptAttemptID, marker.TerminalProjectionID, marker.FinalSessionTurnID,
		marker.TerminalProjectionAggregateKind, marker.TerminalProjectionAggregateID,
		candidateJSON, requestedEffectsJSON, relatedEffectsJSON, marker.ControllerEpochName, marker.ControllerEpoch, marker.CreatedAt,
	)
	if err != nil {
		if isSQLiteConstraintError(err) {
			return store.ConflictErrorf("prompt attempt reclamation marker for Task %q already exists", marker.TaskUID)
		}
		return fmt.Errorf("create prompt attempt reclamation marker: %w", err)
	}
	return nil
}

func getPromptAttemptReclamationMarkerSQLite(ctx context.Context, q controlQueryRower, namespace, taskUID string) (promptAttemptReclamationMarker, error) {
	var marker promptAttemptReclamationMarker
	var mode string
	var candidateJSON, requestedEffectsJSON, relatedEffectsJSON []byte
	err := q.QueryRowContext(ctx,
		`SELECT namespace, task_name, task_uid, mode, continuity_session, final_continuity_session, final_prompt_attempt_id,
		        terminal_projection_id, final_session_turn_id, terminal_projection_aggregate_kind,
		        terminal_projection_aggregate_id, candidate_ids, requested_external_effect_aggregate_ids,
		        related_external_effect_aggregate_ids, controller_epoch_name, controller_epoch, created_at
		 FROM prompt_attempt_reclamations WHERE namespace = ? AND task_uid = ?`, namespace, taskUID,
	).Scan(
		&marker.Namespace, &marker.TaskName, &marker.TaskUID, &mode, &marker.ContinuitySession, &marker.FinalContinuitySession,
		&marker.FinalPromptAttemptID, &marker.TerminalProjectionID, &marker.FinalSessionTurnID,
		&marker.TerminalProjectionAggregateKind, &marker.TerminalProjectionAggregateID,
		&candidateJSON, &requestedEffectsJSON, &relatedEffectsJSON,
		&marker.ControllerEpochName, &marker.ControllerEpoch, &marker.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return promptAttemptReclamationMarker{}, store.ErrNotFound
	}
	if err != nil {
		return promptAttemptReclamationMarker{}, fmt.Errorf("get prompt attempt reclamation marker: %w", err)
	}
	marker.Mode = store.PromptAttemptReclamationMode(mode)
	if err := json.Unmarshal(candidateJSON, &marker.CandidateIDs); err != nil {
		return promptAttemptReclamationMarker{}, fmt.Errorf("decode prompt attempt reclamation candidates: %w", err)
	}
	if err := json.Unmarshal(requestedEffectsJSON, &marker.RequestedExternalEffectAggregateIDs); err != nil {
		return promptAttemptReclamationMarker{}, fmt.Errorf("decode prompt attempt reclamation requested effects: %w", err)
	}
	if err := json.Unmarshal(relatedEffectsJSON, &marker.RelatedExternalEffectAggregateIDs); err != nil {
		return promptAttemptReclamationMarker{}, fmt.Errorf("decode prompt attempt reclamation related effects: %w", err)
	}
	marker.CandidateIDs = normalizePromptAttemptReclamationIDsSQLite(marker.CandidateIDs)
	marker.RequestedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDsSQLite(marker.RequestedExternalEffectAggregateIDs)
	marker.RelatedExternalEffectAggregateIDs = normalizePromptAttemptReclamationIDsSQLite(marker.RelatedExternalEffectAggregateIDs)
	return marker, nil
}

func sqlitePromptAttemptReclaimNotReady(format string, args ...any) error {
	return fmt.Errorf("%w: %s", store.ErrNotReady, fmt.Sprintf(format, args...))
}

// RecoverPromptAttemptPreSubmission returns a pre-acceptance attempt to
// Reserved without inventing a new attempt or replaying an accepted prompt.
func (s *Store) RecoverPromptAttemptPreSubmission(ctx context.Context, recovery store.PromptAttemptPreSubmissionRecovery) (*store.PromptAttempt, error) {
	recovery.ID = strings.TrimSpace(recovery.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", recovery.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(recovery.Fence)
	if err != nil {
		return nil, err
	}
	recovery.Fence = fence
	if recovery.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if recovery.ExpectedState == store.PromptExecutionSubmitting {
		if !recovery.ProvenNotAccepted {
			return nil, store.ValidationErrorf("Submitting attempts require proof that the prompt was not accepted")
		}
	} else if recovery.ExpectedState != store.PromptExecutionReserved &&
		recovery.ExpectedState != store.PromptExecutionSessionStarting && recovery.ExpectedState != store.PromptExecutionPlanned {
		return nil, store.ValidationErrorf("only Reserved, SessionStarting, Planned, or proven-unaccepted Submitting attempts may be recovered before acceptance")
	} else if recovery.ProvenNotAccepted {
		return nil, store.ValidationErrorf("provenNotAccepted is valid only for Submitting attempts")
	}
	if recovery.PreserveBindings && (recovery.ExpectedState != store.PromptExecutionSubmitting || !recovery.ProvenNotAccepted) {
		return nil, store.ValidationErrorf("preserveBindings requires a proven-unaccepted Submitting attempt")
	}
	recovery.OperationID = strings.TrimSpace(recovery.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt recovery operation ID", recovery.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt recovery operation digest", recovery.OperationDigest); err != nil {
		return nil, err
	}
	recovery.RecoveredAt = store.NormalizeControlTime(recovery.RecoveredAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, recovery.Fence); err != nil {
		return nil, err
	}
	attempt, err := getPromptAttempt(ctx, tx, recovery.ID)
	if err != nil {
		return nil, err
	}
	if attempt.LastOperationID == recovery.OperationID {
		if attempt.LastOperationDigest != recovery.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt recovery operation %q was reused with a different digest", recovery.OperationID)
		}
		if attempt.ExecutionState == store.PromptExecutionReserved {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt recovery operation %q was already applied with a different state", recovery.OperationID)
	}
	if attempt.Version != recovery.ExpectedVersion || attempt.ExecutionState != recovery.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q no longer matches pre-submission recovery version/state", attempt.ID)
	}
	query := `UPDATE prompt_attempts
		 SET execution_state = 'Reserved', runtime_instance_id = '', session_uid = '', session_lease_generation = 0,
		     controller_epoch_name = ?, controller_epoch = ?, last_operation_id = ?, last_operation_digest = ?,
		     version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND execution_state = ?`
	if recovery.PreserveBindings {
		query = `UPDATE prompt_attempts
			 SET execution_state = 'Reserved', controller_epoch_name = ?, controller_epoch = ?,
			     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
			 WHERE id = ? AND version = ? AND execution_state = ?`
	}
	result, err := tx.ExecContext(ctx, query,
		recovery.Fence.Name, recovery.Fence.Epoch, recovery.OperationID, recovery.OperationDigest,
		recovery.RecoveredAt, recovery.ID, recovery.ExpectedVersion, string(recovery.ExpectedState),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "prompt attempt pre-submission recovery"); err != nil {
		return nil, err
	}
	attempt.ExecutionState = store.PromptExecutionReserved
	if !recovery.PreserveBindings {
		attempt.RuntimeInstanceID = ""
		attempt.SessionUID = ""
		attempt.SessionLeaseGeneration = 0
	}
	attempt.ControllerEpochName = recovery.Fence.Name
	attempt.ControllerEpoch = recovery.Fence.Epoch
	attempt.LastOperationID = recovery.OperationID
	attempt.LastOperationDigest = recovery.OperationDigest
	attempt.Version++
	attempt.UpdatedAt = recovery.RecoveredAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &attempt, nil
}

// TransitionPromptAttemptExecution applies an exact version/state/epoch CAS.
//
//nolint:gocyclo // Execution-state, fencing, and immutable runtime/session bindings are one CAS boundary.
func (s *Store) TransitionPromptAttemptExecution(ctx context.Context, transition store.PromptAttemptExecutionTransition) (*store.PromptAttempt, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", transition.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(transition.Fence)
	if err != nil {
		return nil, err
	}
	transition.Fence = fence
	if transition.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if err := store.ValidatePromptExecutionTransition(transition.ExpectedState, transition.NewState); err != nil {
		return nil, err
	}
	transition.OperationID = strings.TrimSpace(transition.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt operation ID", transition.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt operation digest", transition.OperationDigest); err != nil {
		return nil, err
	}
	transition.RuntimeInstanceID = strings.TrimSpace(transition.RuntimeInstanceID)
	transition.SessionUID = strings.TrimSpace(transition.SessionUID)
	if transition.SessionLeaseGeneration < 0 {
		return nil, store.ValidationErrorf("session lease generation must not be negative")
	}
	if transition.SessionLeaseGeneration > 0 && transition.SessionUID == "" {
		return nil, store.ValidationErrorf("session UID is required when setting a session lease generation")
	}
	if transition.SessionUID != "" {
		if err := store.ValidateControlIdentifier("session UID", transition.SessionUID); err != nil {
			return nil, err
		}
	}
	if transition.RuntimeInstanceID != "" {
		if err := store.ValidateControlIdentifier("runtime instance ID", transition.RuntimeInstanceID); err != nil {
			return nil, err
		}
	}
	if err := store.ValidateControlReason("prompt attempt terminal reason", transition.TerminalReason); err != nil {
		return nil, err
	}
	if err := store.ValidateControlReason("prompt attempt outcome marker", transition.OutcomeMarker); err != nil {
		return nil, err
	}
	if transition.NewState == store.PromptExecutionOutcomeUnknown && strings.TrimSpace(transition.OutcomeMarker) == "" {
		return nil, store.ValidationErrorf("OutcomeUnknown requires an explicit outcome marker")
	}
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, transition.Fence); err != nil {
		return nil, err
	}
	attempt, err := getPromptAttempt(ctx, tx, transition.ID)
	if err != nil {
		return nil, err
	}
	if attempt.LastOperationID == transition.OperationID {
		if attempt.LastOperationDigest != transition.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt operation %q was reused with a different digest", transition.OperationID)
		}
		if attempt.ExecutionState == transition.NewState {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt operation %q was already applied to state %s", transition.OperationID, attempt.ExecutionState)
	}
	if attempt.Version != transition.ExpectedVersion || attempt.ExecutionState != transition.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q is version %d state %s, expected version %d state %s", attempt.ID, attempt.Version, attempt.ExecutionState, transition.ExpectedVersion, transition.ExpectedState)
	}
	if transition.RuntimeInstanceID != "" && attempt.RuntimeInstanceID != "" && attempt.RuntimeInstanceID != transition.RuntimeInstanceID {
		return nil, store.ConflictErrorf("prompt attempt %q runtime instance is immutable once set", attempt.ID)
	}
	if transition.SessionUID != "" && attempt.SessionUID != "" && attempt.SessionUID != transition.SessionUID {
		return nil, store.ConflictErrorf("prompt attempt %q session UID is immutable once set", attempt.ID)
	}
	if transition.SessionLeaseGeneration > 0 && attempt.SessionLeaseGeneration > 0 && attempt.SessionLeaseGeneration != transition.SessionLeaseGeneration {
		return nil, store.ConflictErrorf("prompt attempt %q session lease generation is immutable once set", attempt.ID)
	}

	runtimeInstanceID := attempt.RuntimeInstanceID
	if transition.RuntimeInstanceID != "" {
		runtimeInstanceID = transition.RuntimeInstanceID
	}
	sessionUID := attempt.SessionUID
	if transition.SessionUID != "" {
		sessionUID = transition.SessionUID
	}
	leaseGeneration := attempt.SessionLeaseGeneration
	if transition.SessionLeaseGeneration > 0 {
		leaseGeneration = transition.SessionLeaseGeneration
	}
	terminalReason := attempt.TerminalReason
	if transition.TerminalReason != "" || store.IsTerminalPromptExecutionState(transition.NewState) {
		terminalReason = transition.TerminalReason
	}
	outcomeMarker := attempt.OutcomeMarker
	if transition.OutcomeMarker != "" {
		outcomeMarker = transition.OutcomeMarker
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE prompt_attempts
		 SET execution_state = ?, runtime_instance_id = ?, session_uid = ?, session_lease_generation = ?,
		     terminal_reason = ?, outcome_marker = ?, controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND execution_state = ?`,
		string(transition.NewState), runtimeInstanceID, sessionUID, leaseGeneration, terminalReason, outcomeMarker,
		transition.Fence.Name, transition.Fence.Epoch, transition.OperationID, transition.OperationDigest,
		transition.UpdatedAt, transition.ID, transition.ExpectedVersion, string(transition.ExpectedState),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "prompt attempt execution"); err != nil {
		return nil, err
	}
	attempt.ExecutionState = transition.NewState
	attempt.RuntimeInstanceID = runtimeInstanceID
	attempt.SessionUID = sessionUID
	attempt.SessionLeaseGeneration = leaseGeneration
	attempt.TerminalReason = terminalReason
	attempt.OutcomeMarker = outcomeMarker
	attempt.ControllerEpochName = transition.Fence.Name
	attempt.ControllerEpoch = transition.Fence.Epoch
	attempt.LastOperationID = transition.OperationID
	attempt.LastOperationDigest = transition.OperationDigest
	attempt.Version++
	attempt.UpdatedAt = transition.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &attempt, nil
}

// TransitionPromptAttemptDelivery applies an exact delivery-state CAS.
func (s *Store) TransitionPromptAttemptDelivery(ctx context.Context, transition store.PromptAttemptDeliveryTransition) (*store.PromptAttempt, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", transition.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(transition.Fence)
	if err != nil {
		return nil, err
	}
	transition.Fence = fence
	if transition.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("prompt attempt expected version must be at least 1")
	}
	if err := store.ValidatePromptDeliveryTransition(transition.ExpectedState, transition.NewState); err != nil {
		return nil, err
	}
	transition.OperationID = strings.TrimSpace(transition.OperationID)
	if err := store.ValidateControlIdentifier("prompt attempt operation ID", transition.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt attempt operation digest", transition.OperationDigest); err != nil {
		return nil, err
	}
	if err := store.ValidateControlReason("prompt delivery terminal reason", transition.TerminalReason); err != nil {
		return nil, err
	}
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, transition.Fence); err != nil {
		return nil, err
	}
	attempt, err := getPromptAttempt(ctx, tx, transition.ID)
	if err != nil {
		return nil, err
	}
	if attempt.LastOperationID == transition.OperationID {
		if attempt.LastOperationDigest != transition.OperationDigest {
			return nil, store.ConflictErrorf("prompt attempt operation %q was reused with a different digest", transition.OperationID)
		}
		if attempt.DeliveryState == transition.NewState {
			return &attempt, nil
		}
		return nil, store.ConflictErrorf("prompt attempt operation %q was already applied to delivery state %s", transition.OperationID, attempt.DeliveryState)
	}
	if attempt.Version != transition.ExpectedVersion || attempt.DeliveryState != transition.ExpectedState {
		return nil, store.ConflictErrorf("prompt attempt %q is version %d delivery state %s, expected version %d state %s", attempt.ID, attempt.Version, attempt.DeliveryState, transition.ExpectedVersion, transition.ExpectedState)
	}

	terminalReason := attempt.TerminalReason
	if transition.TerminalReason != "" || store.IsTerminalPromptDeliveryState(transition.NewState) {
		terminalReason = transition.TerminalReason
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE prompt_attempts
		 SET delivery_state = ?, terminal_reason = ?, controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND delivery_state = ?`,
		string(transition.NewState), terminalReason, transition.Fence.Name, transition.Fence.Epoch,
		transition.OperationID, transition.OperationDigest, transition.UpdatedAt,
		transition.ID, transition.ExpectedVersion, string(transition.ExpectedState),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "prompt attempt delivery"); err != nil {
		return nil, err
	}
	attempt.DeliveryState = transition.NewState
	attempt.TerminalReason = terminalReason
	attempt.ControllerEpochName = transition.Fence.Name
	attempt.ControllerEpoch = transition.Fence.Epoch
	attempt.LastOperationID = transition.OperationID
	attempt.LastOperationDigest = transition.OperationDigest
	attempt.Version++
	attempt.UpdatedAt = transition.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &attempt, nil
}

func samePromptAttemptCreation(a, b store.PromptAttempt) bool {
	return a.ID == b.ID && a.Key == b.Key && a.RequestDigest == b.RequestDigest &&
		a.BindingDigest == b.BindingDigest && a.SnapshotDigest == b.SnapshotDigest
}

func getPromptAttempt(ctx context.Context, q controlQueryRower, id string) (store.PromptAttempt, error) {
	var attempt store.PromptAttempt
	err := q.QueryRowContext(ctx,
		`SELECT id, namespace, task_uid, attempt, prompt_id, session_uid, session_lease_generation,
		        runtime_instance_id, request_digest, binding_digest, snapshot_digest,
		        execution_state, delivery_state, terminal_reason,
		        outcome_marker, controller_epoch_name, controller_epoch, last_operation_id,
		        last_operation_digest, version, created_at, updated_at
		 FROM prompt_attempts WHERE id = ?`,
		id,
	).Scan(
		&attempt.ID, &attempt.Key.Namespace, &attempt.Key.TaskUID, &attempt.Key.Attempt, &attempt.Key.PromptID,
		&attempt.SessionUID, &attempt.SessionLeaseGeneration, &attempt.RuntimeInstanceID, &attempt.RequestDigest,
		&attempt.BindingDigest, &attempt.SnapshotDigest,
		&attempt.ExecutionState, &attempt.DeliveryState, &attempt.TerminalReason, &attempt.OutcomeMarker,
		&attempt.ControllerEpochName, &attempt.ControllerEpoch, &attempt.LastOperationID,
		&attempt.LastOperationDigest, &attempt.Version, &attempt.CreatedAt, &attempt.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.PromptAttempt{}, store.ErrNotFound
	}
	if err != nil {
		return store.PromptAttempt{}, fmt.Errorf("get prompt attempt: %w", err)
	}
	return attempt, nil
}
