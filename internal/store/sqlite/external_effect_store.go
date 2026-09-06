package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.ExternalEffectStore = (*Store)(nil)
var _ store.ExternalEffectIdentityReader = (*Store)(nil)

// ReserveExternalEffect creates or returns a durable same-digest effect identity.
func (s *Store) ReserveExternalEffect(ctx context.Context, request store.ReserveExternalEffectRequest) (*store.ExternalEffect, error) {
	request.Identity.Kind = strings.TrimSpace(request.Identity.Kind)
	request.Identity.Namespace = strings.TrimSpace(request.Identity.Namespace)
	request.Identity.AggregateID = strings.TrimSpace(request.Identity.AggregateID)
	request.Identity.OperationID = strings.TrimSpace(request.Identity.OperationID)
	if err := request.Identity.Validate(); err != nil {
		return nil, err
	}
	id, err := request.Identity.CanonicalID()
	if err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("external effect request digest", request.RequestDigest); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	request.CreatedAt = store.NormalizeControlTime(request.CreatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	effect := store.ExternalEffect{
		ID:                  id,
		Identity:            request.Identity,
		RequestDigest:       request.RequestDigest,
		State:               store.ExternalEffectPending,
		ControllerEpochName: request.Fence.Name,
		ControllerEpoch:     request.Fence.Epoch,
		Version:             1,
		CreatedAt:           request.CreatedAt,
		UpdatedAt:           request.CreatedAt,
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO external_effects(
			id, kind, namespace, aggregate_id, operation_id, request_digest, state, response_digest,
			response, lease_owner, lease_expires_at, attempts, controller_epoch_name,
			controller_epoch, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, '', NULL, '', NULL, 0, ?, ?, 1, ?, ?)`,
		effect.ID, effect.Identity.Kind, effect.Identity.Namespace, effect.Identity.AggregateID,
		effect.Identity.OperationID, effect.RequestDigest, string(effect.State),
		effect.ControllerEpochName, effect.ControllerEpoch, effect.CreatedAt, effect.UpdatedAt,
	)
	if err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, err
		}
		existing, getErr := getExternalEffect(ctx, tx, effect.ID)
		if getErr == nil && existing.Identity == effect.Identity && existing.RequestDigest == effect.RequestDigest {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("external effect %q was reused with a different identity or request digest", effect.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &effect, nil
}

// GetExternalEffect returns one durable external-effect record.
func (s *Store) GetExternalEffect(ctx context.Context, id string) (*store.ExternalEffect, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("external effect ID", id); err != nil {
		return nil, err
	}
	effect, err := getExternalEffect(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &effect, nil
}

// GetExternalEffectByIdentity resolves one exact durable external-effect
// identity and rejects any impossible identity mismatch.
func (s *Store) GetExternalEffectByIdentity(ctx context.Context, identity store.ExternalEffectIdentity) (*store.ExternalEffect, error) {
	identity.Kind = strings.TrimSpace(identity.Kind)
	identity.Namespace = strings.TrimSpace(identity.Namespace)
	identity.AggregateID = strings.TrimSpace(identity.AggregateID)
	identity.OperationID = strings.TrimSpace(identity.OperationID)
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	id, err := identity.CanonicalID()
	if err != nil {
		return nil, err
	}
	effect, err := s.GetExternalEffect(ctx, id)
	if err != nil {
		return nil, err
	}
	if effect.Identity != identity {
		return nil, store.ConflictErrorf("external effect %q does not match its canonical identity", id)
	}
	return effect, nil
}

// TransitionExternalEffect applies an exact version/state/epoch CAS. Terminal
// same-response retries are idempotent; request-digest mismatch always conflicts.
//
//nolint:gocyclo // State, lease, digest, and terminal-response invariants are validated together.
func (s *Store) TransitionExternalEffect(ctx context.Context, transition store.ExternalEffectTransition) (*store.ExternalEffect, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("external effect ID", transition.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(transition.Fence)
	if err != nil {
		return nil, err
	}
	transition.Fence = fence
	if transition.ExpectedVersion < 1 {
		return nil, store.ValidationErrorf("external effect expected version must be at least 1")
	}
	if !store.IsKnownExternalEffectState(transition.ExpectedState) || !store.IsKnownExternalEffectState(transition.NewState) {
		return nil, store.ValidationErrorf("unsupported external effect transition %q -> %q", transition.ExpectedState, transition.NewState)
	}
	if !store.ValidExternalEffectTransition(transition.ExpectedState, transition.NewState) {
		return nil, store.ValidationErrorf("external effect transition %s -> %s is not allowed", transition.ExpectedState, transition.NewState)
	}
	if err := store.ValidateCanonicalDigest("external effect request digest", transition.RequestDigest); err != nil {
		return nil, err
	}
	transition.ExpectedLeaseOwner = strings.TrimSpace(transition.ExpectedLeaseOwner)
	transition.LeaseOwner = strings.TrimSpace(transition.LeaseOwner)
	transition.LeaseExpiresAt = store.NormalizeOptionalControlTime(transition.LeaseExpiresAt)
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)
	if transition.ExpectedState == store.ExternalEffectInFlight {
		if err := store.ValidateControlIdentifier("expected external effect lease owner", transition.ExpectedLeaseOwner); err != nil {
			return nil, err
		}
	} else if transition.ExpectedLeaseOwner != "" {
		return nil, store.ValidationErrorf("pending external effect transition must not set an expected lease owner")
	}
	if transition.NewState == store.ExternalEffectInFlight {
		if err := store.ValidateControlIdentifier("external effect lease owner", transition.LeaseOwner); err != nil {
			return nil, err
		}
		if transition.LeaseExpiresAt == nil || !transition.LeaseExpiresAt.After(transition.UpdatedAt) {
			return nil, store.ValidationErrorf("in-flight external effect requires a future lease expiry")
		}
		if len(transition.Response) > 0 || transition.ResponseDigest != "" {
			return nil, store.ValidationErrorf("in-flight external effect must not include a response")
		}
	} else {
		if transition.LeaseOwner != "" || transition.LeaseExpiresAt != nil {
			return nil, store.ValidationErrorf("non-in-flight external effect must clear lease fields")
		}
		if len(transition.Response) > 0 {
			if err := store.ValidateControlPayload("external effect response", transition.Response); err != nil {
				return nil, err
			}
			if err := store.ValidateCanonicalDigest("external effect response digest", transition.ResponseDigest); err != nil {
				return nil, err
			}
			if store.CanonicalBytesDigest(transition.Response) != transition.ResponseDigest {
				return nil, store.ValidationErrorf("external effect response digest does not match response bytes")
			}
		} else if transition.ResponseDigest != "" {
			return nil, store.ValidationErrorf("external effect response digest requires response bytes")
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, transition.Fence); err != nil {
		return nil, err
	}
	effect, err := getExternalEffect(ctx, tx, transition.ID)
	if err != nil {
		return nil, err
	}
	if effect.RequestDigest != transition.RequestDigest {
		return nil, store.ConflictErrorf("external effect %q request digest does not match reserved identity", effect.ID)
	}
	if effect.State == transition.NewState && effect.ResponseDigest == transition.ResponseDigest &&
		bytes.Equal(effect.Response, transition.Response) && effect.LeaseOwner == transition.LeaseOwner &&
		equalOptionalTime(effect.LeaseExpiresAt, transition.LeaseExpiresAt) {
		return &effect, nil
	}
	if effect.Version != transition.ExpectedVersion || effect.State != transition.ExpectedState || effect.LeaseOwner != transition.ExpectedLeaseOwner {
		return nil, store.ConflictErrorf("external effect %q no longer matches expected version, state, or lease owner", effect.ID)
	}
	if transition.ExpectedState == store.ExternalEffectInFlight && transition.NewState == store.ExternalEffectInFlight &&
		effect.LeaseOwner != transition.LeaseOwner && effect.LeaseExpiresAt != nil && effect.LeaseExpiresAt.After(transition.UpdatedAt) {
		return nil, store.ConflictErrorf("external effect %q is still leased by %q", effect.ID, effect.LeaseOwner)
	}

	attemptIncrement := int64(0)
	if transition.NewState == store.ExternalEffectInFlight {
		attemptIncrement = 1
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE external_effects
		 SET state = ?, response_digest = ?, response = ?, lease_owner = ?, lease_expires_at = ?,
		     attempts = attempts + ?, controller_epoch_name = ?, controller_epoch = ?,
		     version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND state = ? AND request_digest = ? AND lease_owner = ?`,
		string(transition.NewState), transition.ResponseDigest, []byte(transition.Response), transition.LeaseOwner,
		nullTimeValue(transition.LeaseExpiresAt), attemptIncrement, transition.Fence.Name,
		transition.Fence.Epoch, transition.UpdatedAt, transition.ID, transition.ExpectedVersion,
		string(transition.ExpectedState), transition.RequestDigest, transition.ExpectedLeaseOwner,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "external effect"); err != nil {
		return nil, err
	}
	effect.State = transition.NewState
	effect.ResponseDigest = transition.ResponseDigest
	effect.Response = append(effect.Response[:0], transition.Response...)
	effect.LeaseOwner = transition.LeaseOwner
	effect.LeaseExpiresAt = transition.LeaseExpiresAt
	effect.Attempts += attemptIncrement
	effect.ControllerEpochName = transition.Fence.Name
	effect.ControllerEpoch = transition.Fence.Epoch
	effect.Version++
	effect.UpdatedAt = transition.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &effect, nil
}

func equalOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func getExternalEffect(ctx context.Context, q controlQueryRower, id string) (store.ExternalEffect, error) {
	var effect store.ExternalEffect
	var response []byte
	var leaseExpiry sql.NullTime
	err := q.QueryRowContext(ctx,
		`SELECT id, kind, namespace, aggregate_id, operation_id, request_digest, state,
		        response_digest, response, lease_owner, lease_expires_at, attempts,
		        controller_epoch_name, controller_epoch, version, created_at, updated_at
		 FROM external_effects WHERE id = ?`, id,
	).Scan(
		&effect.ID, &effect.Identity.Kind, &effect.Identity.Namespace, &effect.Identity.AggregateID,
		&effect.Identity.OperationID, &effect.RequestDigest, &effect.State, &effect.ResponseDigest,
		&response, &effect.LeaseOwner, &leaseExpiry, &effect.Attempts, &effect.ControllerEpochName,
		&effect.ControllerEpoch, &effect.Version, &effect.CreatedAt, &effect.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ExternalEffect{}, store.ErrNotFound
	}
	if err != nil {
		return store.ExternalEffect{}, fmt.Errorf("get external effect: %w", err)
	}
	if len(response) > 0 {
		effect.Response = append(effect.Response, response...)
	}
	if leaseExpiry.Valid {
		value := leaseExpiry.Time
		effect.LeaseExpiresAt = &value
	}
	return effect, nil
}
