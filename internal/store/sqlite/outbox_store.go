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

var _ store.OutboxProjectionStore = (*Store)(nil)
var _ store.OutboxPersistenceStore = (*Store)(nil)

// EnqueueOutboxProjection inserts a restart-safe projection record. Reusing an
// ID with byte-identical payload and metadata is idempotent; any mismatch conflicts.
func (s *Store) EnqueueOutboxProjection(ctx context.Context, projection *store.OutboxProjection, fence store.ControllerEpochFence) (*store.OutboxProjection, error) {
	return s.enqueueOutboxProjection(ctx, projection, fence, true)
}

// EnqueueOutboxProjectionRecord persists an outbox record after a Kubernetes
// control store has validated the controller epoch fence.
func (s *Store) EnqueueOutboxProjectionRecord(ctx context.Context, projection *store.OutboxProjection, fence store.ControllerEpochFence) (*store.OutboxProjection, error) {
	return s.enqueueOutboxProjection(ctx, projection, fence, false)
}

func (s *Store) enqueueOutboxProjection(ctx context.Context, projection *store.OutboxProjection, fence store.ControllerEpochFence, requireSQLiteEpoch bool) (*store.OutboxProjection, error) {
	if projection == nil {
		return nil, store.ValidationErrorf("outbox projection is required")
	}
	scheduleExplicit := !projection.AvailableAt.IsZero()
	normalized, fence, err := normalizeOutboxProjectionForCreate(*projection, fence, time.Time{})
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if requireSQLiteEpoch {
		if err := requireControllerEpoch(ctx, tx, fence); err != nil {
			return nil, err
		}
	}
	created, err := enqueueOutboxProjectionTx(ctx, tx, normalized, scheduleExplicit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &created, nil
}

// GetOutboxProjection returns one outbox record.
func (s *Store) GetOutboxProjection(ctx context.Context, id string) (*store.OutboxProjection, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("outbox projection ID", id); err != nil {
		return nil, err
	}
	projection, err := getOutboxProjection(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &projection, nil
}

// ClaimOutboxProjections leases due pending records and expired delivering
// records in one transaction. Attempts and versions advance before records are returned.
func (s *Store) ClaimOutboxProjections(ctx context.Context, request store.ClaimOutboxProjectionsRequest) ([]store.OutboxProjection, error) {
	return s.claimOutboxProjections(ctx, request, true)
}

// ClaimOutboxProjectionRecords claims due records after a Kubernetes control
// store has validated the controller epoch fence.
func (s *Store) ClaimOutboxProjectionRecords(ctx context.Context, request store.ClaimOutboxProjectionsRequest) ([]store.OutboxProjection, error) {
	return s.claimOutboxProjections(ctx, request, false)
}

func (s *Store) claimOutboxProjections(ctx context.Context, request store.ClaimOutboxProjectionsRequest, requireSQLiteEpoch bool) ([]store.OutboxProjection, error) {
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	request.WorkerID = strings.TrimSpace(request.WorkerID)
	if err := store.ValidateControlIdentifier("outbox worker ID", request.WorkerID); err != nil {
		return nil, err
	}
	if request.LeaseDuration <= 0 || request.LeaseDuration > 24*time.Hour {
		return nil, store.ValidationErrorf("outbox lease duration must be greater than zero and no more than 24 hours")
	}
	request.Now = store.NormalizeControlTime(request.Now)
	limit := request.Limit
	if limit <= 0 {
		limit = defaultOutboxClaimLimit
	}
	if limit > maxOutboxClaimLimit {
		limit = maxOutboxClaimLimit
	}
	leaseExpiresAt := request.Now.Add(request.LeaseDuration)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if requireSQLiteEpoch {
		if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
			return nil, err
		}
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM outbox_projections
		 WHERE (state = 'Pending' AND available_at <= ?)
		    OR (state = 'Delivering' AND lease_expires_at <= ?)
		 ORDER BY available_at ASC, created_at ASC, id ASC
		 LIMIT ?`,
		request.Now, request.Now, limit,
	)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	claimed := make([]store.OutboxProjection, 0, len(ids))
	for _, id := range ids {
		projection, err := getOutboxProjection(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		result, err := tx.ExecContext(ctx,
			`UPDATE outbox_projections
			 SET state = 'Delivering', attempts = attempts + 1, lease_owner = ?, lease_expires_at = ?,
			     controller_epoch_name = ?, controller_epoch = ?, version = version + 1, updated_at = ?
			 WHERE id = ? AND version = ? AND (
			       (state = 'Pending' AND available_at <= ?)
			       OR (state = 'Delivering' AND lease_expires_at <= ?)
			 )`,
			request.WorkerID, leaseExpiresAt, request.Fence.Name, request.Fence.Epoch,
			request.Now, id, projection.Version, request.Now, request.Now,
		)
		if err != nil {
			return nil, err
		}
		if err := rowsAffectedExactlyOne(result, "outbox claim"); err != nil {
			return nil, err
		}
		projection.State = store.OutboxProjectionDelivering
		projection.Attempts++
		projection.LeaseOwner = request.WorkerID
		projection.LeaseExpiresAt = &leaseExpiresAt
		projection.ControllerEpochName = request.Fence.Name
		projection.ControllerEpoch = request.Fence.Epoch
		projection.Version++
		projection.UpdatedAt = request.Now
		claimed = append(claimed, projection)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// CompleteOutboxProjection completes, retries, or dead-letters the exact lease.
//
//nolint:gocyclo // Completion validates distinct retry, delivery, and dead-letter invariants.
func (s *Store) CompleteOutboxProjection(ctx context.Context, request store.CompleteOutboxProjectionRequest) (*store.OutboxProjection, error) {
	return s.completeOutboxProjection(ctx, request, true)
}

// CompleteOutboxProjectionRecord completes a claimed record after a
// Kubernetes control store has validated the controller epoch fence.
func (s *Store) CompleteOutboxProjectionRecord(ctx context.Context, request store.CompleteOutboxProjectionRequest) (*store.OutboxProjection, error) {
	return s.completeOutboxProjection(ctx, request, false)
}

//nolint:gocyclo // Completion validates distinct retry, delivery, and dead-letter invariants.
func (s *Store) completeOutboxProjection(ctx context.Context, request store.CompleteOutboxProjectionRequest, requireSQLiteEpoch bool) (*store.OutboxProjection, error) {
	request.ID = strings.TrimSpace(request.ID)
	request.LeaseOwner = strings.TrimSpace(request.LeaseOwner)
	if err := store.ValidateControlIdentifier("outbox projection ID", request.ID); err != nil {
		return nil, err
	}
	if err := store.ValidateControlIdentifier("outbox lease owner", request.LeaseOwner); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("outbox expected version must be at least 1")
	}
	request.OperationID = strings.TrimSpace(request.OperationID)
	if err := store.ValidateControlIdentifier("outbox completion operation ID", request.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("outbox completion operation digest", request.OperationDigest); err != nil {
		return nil, err
	}
	request.UpdatedAt = store.NormalizeControlTime(request.UpdatedAt)
	request.LastError = strings.TrimSpace(request.LastError)
	if err := store.ValidateControlReason("outbox last error", request.LastError); err != nil {
		return nil, err
	}
	switch request.NewState {
	case store.OutboxProjectionDelivered:
		if err := store.ValidateCanonicalDigest("outbox delivery digest", request.DeliveryDigest); err != nil {
			return nil, err
		}
		if request.LastError != "" {
			return nil, store.ValidationErrorf("delivered outbox projection must clear last error")
		}
	case store.OutboxProjectionPending:
		if request.LastError == "" {
			return nil, store.ValidationErrorf("retrying outbox projection requires an error")
		}
		if request.DeliveryDigest != "" {
			return nil, store.ValidationErrorf("retrying outbox projection must not set a delivery digest")
		}
		if request.AvailableAt.IsZero() {
			request.AvailableAt = request.UpdatedAt
		} else {
			request.AvailableAt = request.AvailableAt.UTC()
		}
	case store.OutboxProjectionDeadLetter:
		if request.LastError == "" {
			return nil, store.ValidationErrorf("dead-lettered outbox projection requires an error")
		}
		if request.DeliveryDigest != "" {
			return nil, store.ValidationErrorf("dead-lettered outbox projection must not set a delivery digest")
		}
	default:
		return nil, store.ValidationErrorf("outbox completion state must be Pending, Delivered, or DeadLetter")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if requireSQLiteEpoch {
		if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
			return nil, err
		}
	}
	projection, err := getOutboxProjection(ctx, tx, request.ID)
	if err != nil {
		return nil, err
	}
	if projection.LastOperationID == request.OperationID {
		if projection.LastOperationDigest != request.OperationDigest {
			return nil, store.ConflictErrorf("outbox completion operation %q was reused with a different digest", request.OperationID)
		}
		if outboxCompletionMatches(projection, request) {
			return &projection, nil
		}
		return nil, store.ConflictErrorf("outbox completion operation %q was already applied with different target values", request.OperationID)
	}
	if projection.Version != request.ExpectedVersion || projection.State != store.OutboxProjectionDelivering || projection.LeaseOwner != request.LeaseOwner {
		return nil, store.ConflictErrorf("outbox projection %q no longer matches expected version or lease owner", projection.ID)
	}

	availableAt := projection.AvailableAt
	var deliveredAt *time.Time
	if request.NewState == store.OutboxProjectionPending {
		availableAt = request.AvailableAt
	}
	if request.NewState == store.OutboxProjectionDelivered {
		value := request.UpdatedAt
		deliveredAt = &value
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE outbox_projections
		 SET state = ?, available_at = ?, lease_owner = '', lease_expires_at = NULL,
		     delivery_digest = ?, last_error = ?, last_operation_id = ?, last_operation_digest = ?,
		     controller_epoch_name = ?, controller_epoch = ?, version = version + 1, updated_at = ?, delivered_at = ?
		 WHERE id = ? AND version = ? AND state = 'Delivering' AND lease_owner = ?`,
		string(request.NewState), availableAt, request.DeliveryDigest, request.LastError,
		request.OperationID, request.OperationDigest, request.Fence.Name, request.Fence.Epoch,
		request.UpdatedAt, nullTimeValue(deliveredAt),
		request.ID, request.ExpectedVersion, request.LeaseOwner,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "outbox completion"); err != nil {
		return nil, err
	}
	projection.State = request.NewState
	projection.AvailableAt = availableAt
	projection.LeaseOwner = ""
	projection.LeaseExpiresAt = nil
	projection.DeliveryDigest = request.DeliveryDigest
	projection.LastError = request.LastError
	projection.LastOperationID = request.OperationID
	projection.LastOperationDigest = request.OperationDigest
	projection.ControllerEpochName = request.Fence.Name
	projection.ControllerEpoch = request.Fence.Epoch
	projection.Version++
	projection.UpdatedAt = request.UpdatedAt
	projection.DeliveredAt = deliveredAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &projection, nil
}

func normalizeOutboxProjectionForCreate(projection store.OutboxProjection, fence store.ControllerEpochFence, defaultTime time.Time) (store.OutboxProjection, store.ControllerEpochFence, error) {
	projection.ID = strings.TrimSpace(projection.ID)
	projection.AggregateKind = strings.TrimSpace(projection.AggregateKind)
	projection.AggregateID = strings.TrimSpace(projection.AggregateID)
	projection.ProjectionKind = strings.TrimSpace(projection.ProjectionKind)
	for field, value := range map[string]string{
		"outbox projection ID":   projection.ID,
		"outbox aggregate kind":  projection.AggregateKind,
		"outbox aggregate ID":    projection.AggregateID,
		"outbox projection kind": projection.ProjectionKind,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return store.OutboxProjection{}, store.ControllerEpochFence{}, err
		}
	}
	if err := store.ValidateControlPayload("outbox payload", projection.Payload); err != nil {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, err
	}
	if err := store.ValidateCanonicalDigest("outbox payload digest", projection.PayloadDigest); err != nil {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, err
	}
	if store.CanonicalBytesDigest(projection.Payload) != projection.PayloadDigest {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, store.ValidationErrorf("outbox payload digest does not match payload bytes")
	}
	fence, err := store.NormalizeEpochFence(fence)
	if err != nil {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, err
	}
	if projection.State == "" {
		projection.State = store.OutboxProjectionPending
	}
	if projection.State != store.OutboxProjectionPending {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, store.ValidationErrorf("new outbox projection must start in %s", store.OutboxProjectionPending)
	}
	if projection.Version != 0 && projection.Version != 1 {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, store.ValidationErrorf("new outbox projection version must be zero or one")
	}
	if projection.Attempts != 0 || projection.LeaseOwner != "" || projection.LeaseExpiresAt != nil ||
		projection.DeliveryDigest != "" || projection.DeliveredAt != nil || projection.LastOperationID != "" ||
		projection.LastOperationDigest != "" {
		return store.OutboxProjection{}, store.ControllerEpochFence{}, store.ValidationErrorf("new outbox projection must not include delivery state")
	}
	if defaultTime.IsZero() {
		defaultTime = store.NormalizeControlTime(projection.CreatedAt)
	} else {
		defaultTime = defaultTime.UTC()
	}
	projection.CreatedAt = defaultTime
	if projection.UpdatedAt.IsZero() {
		projection.UpdatedAt = defaultTime
	} else {
		projection.UpdatedAt = projection.UpdatedAt.UTC()
	}
	if projection.AvailableAt.IsZero() {
		projection.AvailableAt = defaultTime
	} else {
		projection.AvailableAt = projection.AvailableAt.UTC()
	}
	projection.InitialAvailableAt = projection.AvailableAt
	projection.ControllerEpochName = fence.Name
	projection.ControllerEpoch = fence.Epoch
	projection.Version = 1
	projection.LastError = ""
	return projection, fence, nil
}

func enqueueOutboxProjectionTx(ctx context.Context, tx *sql.Tx, projection store.OutboxProjection, compareSchedule bool) (store.OutboxProjection, error) {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_projections(
			id, aggregate_kind, aggregate_id, projection_kind, payload_digest, payload, state,
			attempts, initial_available_at, available_at, lease_owner, lease_expires_at, delivery_digest, last_error,
			last_operation_id, last_operation_digest, controller_epoch_name, controller_epoch,
			version, created_at, updated_at, delivered_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, '', NULL, '', '', '', '', ?, ?, 1, ?, ?, NULL)`,
		projection.ID, projection.AggregateKind, projection.AggregateID, projection.ProjectionKind,
		projection.PayloadDigest, []byte(projection.Payload), string(projection.State),
		projection.InitialAvailableAt, projection.AvailableAt, projection.ControllerEpochName,
		projection.ControllerEpoch, projection.CreatedAt, projection.UpdatedAt,
	)
	if err == nil {
		return projection, nil
	}
	if !isSQLiteConstraintError(err) {
		return store.OutboxProjection{}, err
	}
	existing, getErr := getOutboxProjection(ctx, tx, projection.ID)
	if getErr == nil && sameOutboxProjectionCreation(existing, projection, compareSchedule) {
		return existing, nil
	}
	return store.OutboxProjection{}, store.ConflictErrorf("outbox projection %q was reused with different payload or metadata", projection.ID)
}

func sameOutboxProjectionCreation(a, b store.OutboxProjection, compareSchedule bool) bool {
	if a.ID != b.ID || a.AggregateKind != b.AggregateKind || a.AggregateID != b.AggregateID ||
		a.ProjectionKind != b.ProjectionKind || a.PayloadDigest != b.PayloadDigest ||
		string(a.Payload) != string(b.Payload) {
		return false
	}
	return !compareSchedule || a.InitialAvailableAt.Equal(b.InitialAvailableAt)
}

func outboxCompletionMatches(projection store.OutboxProjection, request store.CompleteOutboxProjectionRequest) bool {
	if projection.State != request.NewState {
		return false
	}
	switch request.NewState {
	case store.OutboxProjectionDelivered:
		return projection.DeliveryDigest == request.DeliveryDigest && projection.LastError == ""
	case store.OutboxProjectionPending:
		return projection.DeliveryDigest == "" && projection.LastError == request.LastError &&
			projection.AvailableAt.Equal(request.AvailableAt)
	case store.OutboxProjectionDeadLetter:
		return projection.DeliveryDigest == "" && projection.LastError == request.LastError
	default:
		return false
	}
}

func getOutboxProjection(ctx context.Context, q controlQueryRower, id string) (store.OutboxProjection, error) {
	var projection store.OutboxProjection
	var payload []byte
	var leaseExpiry, deliveredAt sql.NullTime
	err := q.QueryRowContext(ctx,
		`SELECT id, aggregate_kind, aggregate_id, projection_kind, payload_digest, payload, state,
		        attempts, initial_available_at, available_at, lease_owner, lease_expires_at,
		        delivery_digest, last_error, last_operation_id, last_operation_digest,
		        controller_epoch_name, controller_epoch, version, created_at, updated_at, delivered_at
		 FROM outbox_projections WHERE id = ?`, id,
	).Scan(
		&projection.ID, &projection.AggregateKind, &projection.AggregateID, &projection.ProjectionKind,
		&projection.PayloadDigest, &payload, &projection.State, &projection.Attempts,
		&projection.InitialAvailableAt, &projection.AvailableAt, &projection.LeaseOwner, &leaseExpiry,
		&projection.DeliveryDigest, &projection.LastError, &projection.LastOperationID,
		&projection.LastOperationDigest, &projection.ControllerEpochName, &projection.ControllerEpoch, &projection.Version,
		&projection.CreatedAt, &projection.UpdatedAt, &deliveredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.OutboxProjection{}, store.ErrNotFound
	}
	if err != nil {
		return store.OutboxProjection{}, fmt.Errorf("get outbox projection: %w", err)
	}
	projection.Payload = append(projection.Payload, payload...)
	if leaseExpiry.Valid {
		value := leaseExpiry.Time
		projection.LeaseExpiresAt = &value
	}
	if deliveredAt.Valid {
		value := deliveredAt.Time
		projection.DeliveredAt = &value
	}
	return projection, nil
}

// CountUnsettledOutboxProjections reports Pending or Delivering projections
// without claiming them. It is used only by the planned-upgrade drain barrier.
func (s *Store) CountUnsettledOutboxProjections(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox_projections WHERE state IN ('Pending', 'Delivering')`,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unsettled outbox projections: %w", err)
	}
	return count, nil
}
