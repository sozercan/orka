package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.BranchClaimStore = (*Store)(nil)
var _ store.BranchClaimCreationStore = (*Store)(nil)

// CreateBranchClaim creates a canonical repository+full-ref ownership record.
// The same immutable identity and digest is idempotent; any mismatch conflicts.
func (s *Store) CreateBranchClaim(ctx context.Context, claim *store.BranchClaim, fence store.ControllerEpochFence) (*store.BranchClaim, error) {
	created, _, err := s.CreateBranchClaimWithResult(ctx, claim, fence)
	return created, err
}

// CreateBranchClaimWithResult creates or idempotently returns a claim and
// reports true only when this transaction inserted the row.
func (s *Store) CreateBranchClaimWithResult(ctx context.Context, claim *store.BranchClaim, fence store.ControllerEpochFence) (*store.BranchClaim, bool, error) {
	normalized, fence, err := store.NormalizeBranchClaimForCreate(claim, fence)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return nil, false, err
	}
	if existing, getErr := getBranchClaim(ctx, tx, normalized.ID); getErr == nil {
		if sameBranchClaimCreation(existing, normalized) {
			return &existing, false, nil
		}
		return nil, false, store.ConflictErrorf("branch claim %q was reused with a different owner or request digest", normalized.ID)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, false, getErr
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO branch_claims(
			id, repository_id, ref, owner_kind, owner_uid, generation, last_verified_absent,
			last_verified_sha, availability, blocked_reason, related_publication_id, request_digest,
			controller_epoch_name, controller_epoch, last_operation_id, last_operation_digest,
			version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID, normalized.RepositoryID, normalized.Ref, string(normalized.OwnerKind), normalized.OwnerUID,
		normalized.Generation, normalized.LastVerified.Absent, normalized.LastVerified.SHA, string(normalized.Availability),
		normalized.BlockedReason, normalized.RelatedPublicationID, normalized.RequestDigest,
		normalized.ControllerEpochName, normalized.ControllerEpoch, normalized.LastOperationID,
		normalized.LastOperationDigest, normalized.Version, normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, false, err
		}
		existing, getErr := getBranchClaim(ctx, tx, normalized.ID)
		if getErr == nil && sameBranchClaimCreation(existing, normalized) {
			return &existing, false, nil
		}
		return nil, false, store.ConflictErrorf("branch claim %q already exists with a different owner, baseline, or request digest", normalized.ID)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		existing, getErr := getBranchClaim(ctx, s.db, normalized.ID)
		if getErr == nil && sameBranchClaimCreation(existing, normalized) {
			return &existing, false, nil
		}
		return nil, false, err
	}
	return &normalized, true, nil
}

// GetBranchClaim returns one branch claim.
func (s *Store) GetBranchClaim(ctx context.Context, id string) (*store.BranchClaim, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("branch claim ID", id); err != nil {
		return nil, err
	}
	claim, err := getBranchClaim(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &claim, nil
}

// ReclaimBranchClaim deletes only the exact available Task-owned claim. A
// missing claim or one whose immutable owner/request identity was replaced is
// already reclaimed from the caller's perspective; every other mismatch is a
// conflict so stale recovery cannot delete a live claim.
func (s *Store) ReclaimBranchClaim(ctx context.Context, request store.ReclaimBranchClaimRequest) error {
	normalized, err := store.NormalizeBranchClaimReclamationRequest(request)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, normalized.Fence); err != nil {
		return err
	}
	claim, err := getBranchClaim(ctx, tx, normalized.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if store.BranchClaimReclamationIdentityReplaced(claim, normalized) {
		return nil
	}
	if !store.BranchClaimMatchesReclamation(claim, normalized) {
		return store.ConflictErrorf("branch claim %q no longer matches the exact Task-owner reclamation fence", claim.ID)
	}
	result, err := tx.ExecContext(ctx,
		`DELETE FROM branch_claims
		 WHERE id = ? AND repository_id = ? AND ref = ? AND owner_kind = ? AND owner_uid = ?
		   AND generation = ? AND last_verified_absent = ? AND last_verified_sha = ?
		   AND availability = ? AND blocked_reason = '' AND related_publication_id = ''
		   AND request_digest = ? AND version = ?`,
		normalized.ID, normalized.ExpectedRepositoryID, normalized.ExpectedRef,
		normalized.ExpectedOwnerKind, normalized.ExpectedOwnerUID, normalized.ExpectedGeneration,
		normalized.ExpectedLastVerified.Absent, normalized.ExpectedLastVerified.SHA,
		normalized.ExpectedAvailability, normalized.ExpectedRequestDigest, normalized.ExpectedVersion,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		latest, getErr := getBranchClaim(ctx, tx, normalized.ID)
		if errors.Is(getErr, store.ErrNotFound) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		if store.BranchClaimReclamationIdentityReplaced(latest, normalized) {
			return nil
		}
		return store.ConflictErrorf("branch claim %q changed during Task-owner reclamation", normalized.ID)
	}
	return tx.Commit()
}

// CompareAndSwapBranchClaim atomically updates the exact generation, baseline,
// and availability observed by the caller.
//
//nolint:gocyclo // The explicit baseline/generation/version CAS validation is intentionally centralized.
func (s *Store) CompareAndSwapBranchClaim(ctx context.Context, change store.BranchClaimCAS) (*store.BranchClaim, error) {
	change.ID = strings.TrimSpace(change.ID)
	if err := store.ValidateControlIdentifier("branch claim ID", change.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(change.Fence)
	if err != nil {
		return nil, err
	}
	change.Fence = fence
	if change.ExpectedVersion < 1 || change.ExpectedGeneration < 1 {
		return nil, store.ValidationErrorf("branch claim expected version and generation must be at least 1")
	}
	if change.NewGeneration != change.ExpectedGeneration && change.NewGeneration != change.ExpectedGeneration+1 {
		return nil, store.ValidationErrorf("branch claim generation may be preserved or incremented exactly by one")
	}
	if err := change.ExpectedLastVerified.Validate("expected branch baseline"); err != nil {
		return nil, err
	}
	if err := change.NewLastVerified.Validate("new branch baseline"); err != nil {
		return nil, err
	}
	if !store.IsKnownBranchClaimAvailability(change.ExpectedAvailability) || !store.IsKnownBranchClaimAvailability(change.NewAvailability) {
		return nil, store.ValidationErrorf("unsupported branch claim availability transition %q -> %q", change.ExpectedAvailability, change.NewAvailability)
	}
	change.BlockedReason = strings.TrimSpace(change.BlockedReason)
	change.RelatedPublicationID = strings.TrimSpace(change.RelatedPublicationID)
	if err := store.ValidateControlReason("branch claim blocked reason", change.BlockedReason); err != nil {
		return nil, err
	}
	if change.NewAvailability == store.BranchClaimAvailable && (change.BlockedReason != "" || change.RelatedPublicationID != "") {
		return nil, store.ValidationErrorf("available branch claim must clear blocked reason and related publication")
	}
	if change.NewAvailability == store.BranchClaimReconciliationBlocked && change.BlockedReason == "" {
		return nil, store.ValidationErrorf("reconciliation-blocked branch claim requires a reason")
	}
	if change.RelatedPublicationID != "" {
		if err := store.ValidateControlIdentifier("related publication ID", change.RelatedPublicationID); err != nil {
			return nil, err
		}
	}
	change.OperationID = strings.TrimSpace(change.OperationID)
	if err := store.ValidateControlIdentifier("branch claim operation ID", change.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("branch claim operation digest", change.OperationDigest); err != nil {
		return nil, err
	}
	change.UpdatedAt = store.NormalizeControlTime(change.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, change.Fence); err != nil {
		return nil, err
	}
	claim, err := getBranchClaim(ctx, tx, change.ID)
	if err != nil {
		return nil, err
	}
	if claim.LastOperationID == change.OperationID {
		if claim.LastOperationDigest != change.OperationDigest {
			return nil, store.ConflictErrorf("branch claim operation %q was reused with a different digest", change.OperationID)
		}
		if claim.Generation == change.NewGeneration && claim.LastVerified.Equal(change.NewLastVerified) &&
			claim.Availability == change.NewAvailability && claim.BlockedReason == change.BlockedReason &&
			claim.RelatedPublicationID == change.RelatedPublicationID {
			return &claim, nil
		}
		return nil, store.ConflictErrorf("branch claim operation %q was already applied with different target values", change.OperationID)
	}
	if claim.Version != change.ExpectedVersion || claim.Generation != change.ExpectedGeneration ||
		!claim.LastVerified.Equal(change.ExpectedLastVerified) || claim.Availability != change.ExpectedAvailability {
		return nil, store.ConflictErrorf("branch claim %q no longer matches expected version, generation, baseline, or availability", claim.ID)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE branch_claims
		 SET generation = ?, last_verified_absent = ?, last_verified_sha = ?, availability = ?,
		     blocked_reason = ?, related_publication_id = ?, controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND last_verified_absent = ?
		   AND last_verified_sha = ? AND availability = ?`,
		change.NewGeneration, change.NewLastVerified.Absent, change.NewLastVerified.SHA, string(change.NewAvailability),
		change.BlockedReason, change.RelatedPublicationID, change.Fence.Name, change.Fence.Epoch,
		change.OperationID, change.OperationDigest, change.UpdatedAt, change.ID, change.ExpectedVersion,
		change.ExpectedGeneration, change.ExpectedLastVerified.Absent, change.ExpectedLastVerified.SHA,
		string(change.ExpectedAvailability),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "branch claim"); err != nil {
		return nil, err
	}
	claim.Generation = change.NewGeneration
	claim.LastVerified = change.NewLastVerified
	claim.Availability = change.NewAvailability
	claim.BlockedReason = change.BlockedReason
	claim.RelatedPublicationID = change.RelatedPublicationID
	claim.ControllerEpochName = change.Fence.Name
	claim.ControllerEpoch = change.Fence.Epoch
	claim.LastOperationID = change.OperationID
	claim.LastOperationDigest = change.OperationDigest
	claim.Version++
	claim.UpdatedAt = change.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &claim, nil
}

func sameBranchClaimCreation(a, b store.BranchClaim) bool {
	return a.ID == b.ID && a.RepositoryID == b.RepositoryID && a.Ref == b.Ref &&
		a.OwnerKind == b.OwnerKind && a.OwnerUID == b.OwnerUID && a.RequestDigest == b.RequestDigest
}

func getBranchClaim(ctx context.Context, q controlQueryRower, id string) (store.BranchClaim, error) {
	var claim store.BranchClaim
	err := q.QueryRowContext(ctx,
		`SELECT id, repository_id, ref, owner_kind, owner_uid, generation, last_verified_absent,
		        last_verified_sha, availability, blocked_reason, related_publication_id, request_digest,
		        controller_epoch_name, controller_epoch, last_operation_id, last_operation_digest,
		        version, created_at, updated_at
		 FROM branch_claims WHERE id = ?`, id,
	).Scan(
		&claim.ID, &claim.RepositoryID, &claim.Ref, &claim.OwnerKind, &claim.OwnerUID, &claim.Generation,
		&claim.LastVerified.Absent, &claim.LastVerified.SHA, &claim.Availability, &claim.BlockedReason,
		&claim.RelatedPublicationID, &claim.RequestDigest, &claim.ControllerEpochName,
		&claim.ControllerEpoch, &claim.LastOperationID, &claim.LastOperationDigest,
		&claim.Version, &claim.CreatedAt, &claim.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.BranchClaim{}, store.ErrNotFound
	}
	if err != nil {
		return store.BranchClaim{}, fmt.Errorf("get branch claim: %w", err)
	}
	return claim, nil
}
