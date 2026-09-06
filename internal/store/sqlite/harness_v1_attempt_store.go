/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

// CreateHarnessV1Attempt implements store.HarnessV1AttemptStore.
func (s *Store) CreateHarnessV1Attempt(
	ctx context.Context,
	attempt *store.HarnessV1Attempt,
	fence store.ControllerEpochFence,
) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	fence, err := store.NormalizeEpochFence(fence)
	if err != nil {
		return err
	}
	normalized := *attempt
	normalized.ControllerEpochName = fence.Name
	normalized.ControllerEpoch = fence.Epoch
	key := store.HarnessV1AttemptKey{
		Namespace: normalized.Namespace,
		TaskUID:   normalized.TaskUID,
		Attempt:   normalized.Attempt,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin harness v1 attempt create: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return err
	}

	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `INSERT INTO harness_v1_attempts
		(id, namespace, task_name, task_uid, attempt, binding_digest, snapshot_digest, request_digest,
		 turn_id, runtime_session_id, correlation_id, backend, backend_endpoint,
		 auth_secret_namespace, auth_secret_name, auth_secret_key, auth_secret_uid, auth_secret_resource_version,
		 state, last_event_seq, terminal_receipt_digest, terminal_reason, duplicate_safe, retry_class,
		 controller_epoch_name, controller_epoch, last_operation_id, last_operation_digest,
		 version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', '', ?, ?, ?, ?, '', '', 1, ?, ?)
		ON CONFLICT(namespace, task_uid, attempt) DO NOTHING`,
		key.CanonicalID(), normalized.Namespace, normalized.TaskName, normalized.TaskUID, normalized.Attempt,
		normalized.BindingDigest, normalized.SnapshotDigest, normalized.RequestDigest,
		normalized.TurnID, normalized.RuntimeSessionID, normalized.CorrelationID, normalized.Backend, normalized.BackendEndpoint,
		normalized.AuthSecretNamespace, normalized.AuthSecretName, normalized.AuthSecretKey, normalized.AuthSecretUID, normalized.AuthSecretResourceVersion,
		string(store.HarnessV1AttemptPrepared), normalized.DuplicateSafe, string(normalized.RetryClass),
		normalized.ControllerEpochName, normalized.ControllerEpoch, now, now)
	if err != nil {
		return fmt.Errorf("create harness v1 attempt: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create harness v1 attempt rows: %w", err)
	}
	switch affected {
	case 0:
		existing, getErr := scanHarnessV1Attempt(tx.QueryRowContext(ctx, harnessV1AttemptSelectSQL+`
			WHERE namespace = ? AND task_uid = ? AND attempt = ?`,
			key.Namespace, key.TaskUID, key.Attempt))
		if errors.Is(getErr, sql.ErrNoRows) {
			return fmt.Errorf("%w: harness v1 attempt %s conflicted without a readable row",
				store.ErrConflict, key.CanonicalID())
		}
		if getErr != nil {
			return fmt.Errorf("read conflicting harness v1 attempt: %w", getErr)
		}
		if !sameHarnessV1AttemptCreation(*existing, normalized) {
			return fmt.Errorf("%w: harness v1 attempt %s already exists with different immutable content",
				store.ErrDuplicateMismatch, key.CanonicalID())
		}
	case 1:
	default:
		return fmt.Errorf("%w: harness v1 attempt %s create affected %d rows",
			store.ErrConflict, key.CanonicalID(), affected)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit harness v1 attempt create: %w", err)
	}
	return nil
}

// GetHarnessV1Attempt implements store.HarnessV1AttemptStore.
func (s *Store) GetHarnessV1Attempt(ctx context.Context, key store.HarnessV1AttemptKey) (*store.HarnessV1Attempt, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	attempt, err := scanHarnessV1Attempt(s.db.QueryRowContext(ctx, harnessV1AttemptSelectSQL+`
		WHERE namespace = ? AND task_uid = ? AND attempt = ?`, key.Namespace, key.TaskUID, key.Attempt))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get harness v1 attempt: %w", err)
	}
	return attempt, nil
}

// ListHarnessV1AttemptsByTask implements store.HarnessV1AttemptStore.
func (s *Store) ListHarnessV1AttemptsByTask(ctx context.Context, namespace, taskUID string) ([]store.HarnessV1Attempt, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(taskUID) == "" {
		return nil, store.ValidationErrorf("harness v1 attempt namespace and task UID are required")
	}
	rows, err := s.db.QueryContext(ctx, harnessV1AttemptSelectSQL+`
		WHERE namespace = ? AND task_uid = ? ORDER BY attempt ASC`, namespace, taskUID)
	if err != nil {
		return nil, fmt.Errorf("list harness v1 attempts: %w", err)
	}
	return collectHarnessV1Attempts(rows)
}

// ReclaimHarnessV1Attempts implements store.HarnessV1AttemptStore. Attempt
// removal is fenced by the current controller epoch and re-proves every
// terminal and Session/outbox barrier in one SQLite transaction.
func (s *Store) ReclaimHarnessV1Attempts(
	ctx context.Context,
	request store.ReclaimHarnessV1AttemptsRequest,
) (int, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	request.BindingDigest = strings.TrimSpace(request.BindingDigest)
	if request.Namespace == "" || request.TaskUID == "" {
		return 0, store.ValidationErrorf("harness v1 reclamation namespace and task UID are required")
	}
	if err := store.ValidateCanonicalDigest("harness v1 reclamation binding digest", request.BindingDigest); err != nil {
		return 0, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin harness v1 attempt reclamation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, harnessV1AttemptSelectSQL+`
		WHERE namespace = ? AND task_uid = ? ORDER BY attempt ASC`, request.Namespace, request.TaskUID)
	if err != nil {
		return 0, fmt.Errorf("list harness v1 attempts for reclamation: %w", err)
	}
	attempts, err := collectHarnessV1Attempts(rows)
	if err != nil {
		return 0, err
	}
	for i := range attempts {
		attempt := &attempts[i]
		if attempt.BindingDigest != request.BindingDigest {
			return 0, fmt.Errorf("%w: harness v1 attempt binding changed before reclamation", store.ErrConflict)
		}
		if !store.IsTerminalHarnessV1AttemptState(attempt.State) {
			return 0, fmt.Errorf("%w: harness v1 attempt %s is not terminal", store.ErrNotReady,
				(store.HarnessV1AttemptKey{Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt}).CanonicalID())
		}
		if !request.SessionRequired || harnessV1AttemptHasNoSessionSideEffect(*attempt) {
			continue
		}
		ref := (store.HarnessV1AttemptKey{
			Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt,
		}).SessionReferenceID()
		var turnState, projectionID string
		err := tx.QueryRowContext(ctx, `SELECT state, projection_id FROM session_turns
			WHERE prompt_attempt_id = ?`, ref).Scan(&turnState, &projectionID)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: harness v1 attempt %s has no durable SessionTurn", store.ErrNotReady, ref)
		}
		if err != nil {
			return 0, fmt.Errorf("load harness v1 SessionTurn for reclamation: %w", err)
		}
		if turnState != string(store.SessionTurnFinalized) || strings.TrimSpace(projectionID) == "" {
			return 0, fmt.Errorf("%w: harness v1 SessionTurn %s is not finalized", store.ErrNotReady, ref)
		}
		var projectionState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM outbox_projections WHERE id = ?`, projectionID).Scan(&projectionState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, fmt.Errorf("%w: harness v1 SessionTurn %s has no terminal projection", store.ErrNotReady, ref)
			}
			return 0, fmt.Errorf("load harness v1 SessionTurn projection for reclamation: %w", err)
		}
		if projectionState != string(store.OutboxProjectionDelivered) {
			return 0, fmt.Errorf("%w: harness v1 SessionTurn projection %s is %s", store.ErrNotReady, projectionID, projectionState)
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM harness_v1_attempts WHERE namespace = ? AND task_uid = ?`, request.Namespace, request.TaskUID)
	if err != nil {
		return 0, fmt.Errorf("reclaim harness v1 attempts: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("reclaim harness v1 attempt rows: %w", err)
	}
	if deleted != int64(len(attempts)) {
		return 0, fmt.Errorf("%w: harness v1 reclamation deleted %d of %d attempts", store.ErrConflict, deleted, len(attempts))
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit harness v1 attempt reclamation: %w", err)
	}
	return int(deleted), nil
}

func harnessV1AttemptHasNoSessionSideEffect(attempt store.HarnessV1Attempt) bool {
	if attempt.TerminalReceiptDigest != "" {
		return false
	}
	if attempt.State == store.HarnessV1AttemptCancelled {
		return true
	}
	if attempt.State != store.HarnessV1AttemptRejected {
		return false
	}
	switch attempt.TerminalReason {
	case "BackendDisabled", "CredentialChanged", "InvalidBinding", "SessionConflict":
		return true
	default:
		return false
	}
}

// TransitionHarnessV1Attempt implements store.HarnessV1AttemptStore.
func (s *Store) TransitionHarnessV1Attempt(ctx context.Context, transition store.HarnessV1AttemptTransition) (*store.HarnessV1Attempt, error) {
	if err := transition.Key.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(transition.OperationID) == "" {
		return nil, store.ValidationErrorf("harness v1 attempt transition operation ID is required")
	}
	if err := store.ValidateCanonicalDigest("harness v1 attempt operation digest", transition.OperationDigest); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(transition.Fence)
	if err != nil {
		return nil, err
	}
	transition.Fence = fence

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin harness v1 attempt transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, transition.Fence); err != nil {
		return nil, err
	}

	current, err := scanHarnessV1Attempt(tx.QueryRowContext(ctx, harnessV1AttemptSelectSQL+`
		WHERE namespace = ? AND task_uid = ? AND attempt = ?`,
		transition.Key.Namespace, transition.Key.TaskUID, transition.Key.Attempt))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read harness v1 attempt: %w", err)
	}

	// Idempotent replay of an already-applied operation.
	if current.LastOperationID == transition.OperationID {
		if current.LastOperationDigest != transition.OperationDigest || current.State != transition.TargetState {
			return nil, fmt.Errorf("%w: harness v1 attempt %s operation %s was applied with different content",
				store.ErrConflict, transition.Key.CanonicalID(), transition.OperationID)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit harness v1 attempt replay: %w", err)
		}
		return current, nil
	}

	if current.Version != transition.ExpectedVersion {
		return nil, fmt.Errorf("%w: harness v1 attempt %s version %d does not match expected %d",
			store.ErrConflict, transition.Key.CanonicalID(), current.Version, transition.ExpectedVersion)
	}
	if current.State != transition.ExpectedState {
		return nil, fmt.Errorf("%w: harness v1 attempt %s state %s does not match expected %s",
			store.ErrConflict, transition.Key.CanonicalID(), current.State, transition.ExpectedState)
	}
	if err := store.ValidateHarnessV1AttemptTransition(current.State, transition.TargetState); err != nil {
		return nil, err
	}

	updated := *current
	updated.State = transition.TargetState
	updated.Version = current.Version + 1
	updated.LastOperationID = transition.OperationID
	updated.LastOperationDigest = transition.OperationDigest
	if transition.Fence.Name != "" {
		updated.ControllerEpochName = transition.Fence.Name
		updated.ControllerEpoch = transition.Fence.Epoch
	}
	applyHarnessV1AttemptUpdates(&updated, transition.Updates)
	if transition.TargetState == store.HarnessV1AttemptOutcomeUnknown && updated.TerminalReason == "" {
		return nil, store.ValidationErrorf("harness v1 attempt OutcomeUnknown requires a terminal reason")
	}
	updated.UpdatedAt = time.Now().UTC()

	var cancelRequestedAt any
	if updated.CancelRequestedAt != nil {
		cancelRequestedAt = updated.CancelRequestedAt.UTC()
	}
	result, err := tx.ExecContext(ctx, `UPDATE harness_v1_attempts SET
			state = ?, runtime_session_id = ?, correlation_id = ?, backend_endpoint = ?,
			last_event_seq = ?, cancel_requested_at = ?, terminal_receipt_digest = ?, terminal_reason = ?,
			controller_epoch_name = ?, controller_epoch = ?, last_operation_id = ?, last_operation_digest = ?,
			version = ?, updated_at = ?
		WHERE namespace = ? AND task_uid = ? AND attempt = ? AND version = ?`,
		string(updated.State), updated.RuntimeSessionID, updated.CorrelationID, updated.BackendEndpoint,
		updated.LastEventSeq, cancelRequestedAt, updated.TerminalReceiptDigest, updated.TerminalReason,
		updated.ControllerEpochName, updated.ControllerEpoch, updated.LastOperationID, updated.LastOperationDigest,
		updated.Version, updated.UpdatedAt,
		transition.Key.Namespace, transition.Key.TaskUID, transition.Key.Attempt, current.Version)
	if err != nil {
		return nil, fmt.Errorf("transition harness v1 attempt: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("transition harness v1 attempt rows: %w", err)
	}
	if affected != 1 {
		return nil, fmt.Errorf("%w: harness v1 attempt %s was concurrently modified",
			store.ErrConflict, transition.Key.CanonicalID())
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit harness v1 attempt transition: %w", err)
	}
	return &updated, nil
}

func applyHarnessV1AttemptUpdates(attempt *store.HarnessV1Attempt, updates store.HarnessV1AttemptUpdates) {
	if updates.RuntimeSessionID != nil {
		attempt.RuntimeSessionID = *updates.RuntimeSessionID
	}
	if updates.CorrelationID != nil {
		attempt.CorrelationID = *updates.CorrelationID
	}
	if updates.BackendEndpoint != nil {
		attempt.BackendEndpoint = *updates.BackendEndpoint
	}
	if updates.LastEventSeq != nil {
		attempt.LastEventSeq = *updates.LastEventSeq
	}
	if updates.CancelRequestedAt != nil {
		requestedAt := updates.CancelRequestedAt.UTC()
		attempt.CancelRequestedAt = &requestedAt
	}
	if updates.TerminalReceiptDigest != nil {
		attempt.TerminalReceiptDigest = *updates.TerminalReceiptDigest
	}
	if updates.TerminalReason != nil {
		attempt.TerminalReason = *updates.TerminalReason
	}
}

func sameHarnessV1AttemptCreation(a, b store.HarnessV1Attempt) bool {
	return a.Namespace == b.Namespace &&
		a.TaskName == b.TaskName &&
		a.TaskUID == b.TaskUID &&
		a.Attempt == b.Attempt &&
		a.BindingDigest == b.BindingDigest &&
		a.SnapshotDigest == b.SnapshotDigest &&
		a.RequestDigest == b.RequestDigest &&
		a.TurnID == b.TurnID &&
		a.Backend == b.Backend &&
		a.AuthSecretNamespace == b.AuthSecretNamespace &&
		a.AuthSecretName == b.AuthSecretName &&
		a.AuthSecretKey == b.AuthSecretKey &&
		a.AuthSecretUID == b.AuthSecretUID &&
		a.AuthSecretResourceVersion == b.AuthSecretResourceVersion &&
		a.DuplicateSafe == b.DuplicateSafe &&
		a.RetryClass == b.RetryClass
}

const harnessV1AttemptSelectSQL = `SELECT namespace, task_name, task_uid, attempt,
	binding_digest, snapshot_digest, request_digest, turn_id, runtime_session_id, correlation_id,
	backend, backend_endpoint, auth_secret_namespace, auth_secret_name, auth_secret_key, auth_secret_uid, auth_secret_resource_version,
	state, last_event_seq, cancel_requested_at, terminal_receipt_digest, terminal_reason,
	duplicate_safe, retry_class, controller_epoch_name, controller_epoch,
	last_operation_id, last_operation_digest, version, created_at, updated_at
	FROM harness_v1_attempts`

func scanHarnessV1Attempt(row sessionLineageScanner) (*store.HarnessV1Attempt, error) {
	var (
		attempt           store.HarnessV1Attempt
		state             string
		retryClass        string
		cancelRequestedAt sql.NullTime
	)
	if err := row.Scan(&attempt.Namespace, &attempt.TaskName, &attempt.TaskUID, &attempt.Attempt,
		&attempt.BindingDigest, &attempt.SnapshotDigest, &attempt.RequestDigest,
		&attempt.TurnID, &attempt.RuntimeSessionID, &attempt.CorrelationID,
		&attempt.Backend, &attempt.BackendEndpoint,
		&attempt.AuthSecretNamespace, &attempt.AuthSecretName, &attempt.AuthSecretKey, &attempt.AuthSecretUID, &attempt.AuthSecretResourceVersion,
		&state, &attempt.LastEventSeq, &cancelRequestedAt, &attempt.TerminalReceiptDigest, &attempt.TerminalReason,
		&attempt.DuplicateSafe, &retryClass, &attempt.ControllerEpochName, &attempt.ControllerEpoch,
		&attempt.LastOperationID, &attempt.LastOperationDigest,
		&attempt.Version, &attempt.CreatedAt, &attempt.UpdatedAt); err != nil {
		return nil, err
	}
	attempt.State = store.HarnessV1AttemptState(state)
	attempt.RetryClass = store.HarnessV1AttemptRetryClass(retryClass)
	if cancelRequestedAt.Valid {
		requestedAt := cancelRequestedAt.Time.UTC()
		attempt.CancelRequestedAt = &requestedAt
	}
	attempt.CreatedAt = attempt.CreatedAt.UTC()
	attempt.UpdatedAt = attempt.UpdatedAt.UTC()
	return &attempt, nil
}

func collectHarnessV1Attempts(rows *sql.Rows) ([]store.HarnessV1Attempt, error) {
	defer func() { _ = rows.Close() }()
	var attempts []store.HarnessV1Attempt
	for rows.Next() {
		attempt, err := scanHarnessV1Attempt(rows)
		if err != nil {
			return nil, fmt.Errorf("scan harness v1 attempt: %w", err)
		}
		attempts = append(attempts, *attempt)
	}
	return attempts, rows.Err()
}
