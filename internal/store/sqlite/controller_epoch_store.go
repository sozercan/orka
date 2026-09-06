package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orka-agents/orka/internal/store"
)

// GetControllerEpoch returns the current durable controller epoch.
func (s *Store) GetControllerEpoch(ctx context.Context, name string) (*store.ControllerEpoch, error) {
	name, err := store.NormalizeControllerEpochName(name)
	if err != nil {
		return nil, err
	}
	epoch, err := getControllerEpoch(ctx, s.db, name)
	if err != nil {
		return nil, err
	}
	return &epoch, nil
}

// CompareAndSwapControllerEpoch creates or advances the controller epoch with
// exact version/epoch CAS semantics. A same-holder, same-digest retry of an
// already committed target epoch is idempotent.
func (s *Store) CompareAndSwapControllerEpoch(ctx context.Context, change store.ControllerEpochCAS) (*store.ControllerEpoch, error) {
	change.Name = strings.TrimSpace(change.Name)
	name, err := store.NormalizeControllerEpochName(change.Name)
	if err != nil {
		return nil, err
	}
	change.Name = name
	change.HolderID = strings.TrimSpace(change.HolderID)
	if err := store.ValidateControlIdentifier("controller epoch holder ID", change.HolderID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("controller epoch request digest", change.RequestDigest); err != nil {
		return nil, err
	}
	if change.ExpectedVersion < 0 || change.ExpectedEpoch < 0 {
		return nil, store.ValidationErrorf("controller epoch expected version and epoch must not be negative")
	}
	if change.NewEpoch < 1 {
		return nil, store.ValidationErrorf("controller new epoch must be at least 1")
	}
	change.UpdatedAt = store.NormalizeControlTime(change.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	current, err := getControllerEpoch(ctx, tx, change.Name)
	if errors.Is(err, store.ErrNotFound) {
		if change.ExpectedVersion != 0 || change.ExpectedEpoch != 0 || change.NewEpoch != 1 {
			return nil, store.ConflictErrorf("controller epoch %q does not exist; creation requires expected version/epoch 0 and new epoch 1", change.Name)
		}
		created := store.ControllerEpoch{
			Name:          change.Name,
			Epoch:         change.NewEpoch,
			HolderID:      change.HolderID,
			RequestDigest: change.RequestDigest,
			Version:       1,
			AcquiredAt:    change.UpdatedAt,
			UpdatedAt:     change.UpdatedAt,
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO controller_epochs(name, epoch, holder_id, request_digest, version, acquired_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			created.Name, created.Epoch, created.HolderID, created.RequestDigest, created.Version, created.AcquiredAt, created.UpdatedAt,
		)
		if err != nil {
			if isSQLiteConstraintError(err) {
				return nil, store.ConflictErrorf("controller epoch %q was created concurrently", change.Name)
			}
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &created, nil
	}
	if err != nil {
		return nil, err
	}

	if current.Epoch == change.NewEpoch {
		if current.HolderID == change.HolderID && current.RequestDigest == change.RequestDigest {
			return &current, nil
		}
		return nil, store.ConflictErrorf("controller epoch %q target %d already exists with different holder or digest", change.Name, change.NewEpoch)
	}
	if current.Version != change.ExpectedVersion || current.Epoch != change.ExpectedEpoch {
		return nil, store.ConflictErrorf("controller epoch %q is version %d epoch %d, expected version %d epoch %d", change.Name, current.Version, current.Epoch, change.ExpectedVersion, change.ExpectedEpoch)
	}
	if change.NewEpoch != change.ExpectedEpoch+1 {
		return nil, store.ValidationErrorf("controller epoch must advance exactly by one: expected new epoch %d", change.ExpectedEpoch+1)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE controller_epochs
		 SET epoch = ?, holder_id = ?, request_digest = ?, version = version + 1, acquired_at = ?, updated_at = ?
		 WHERE name = ? AND version = ? AND epoch = ?`,
		change.NewEpoch, change.HolderID, change.RequestDigest, change.UpdatedAt, change.UpdatedAt,
		change.Name, change.ExpectedVersion, change.ExpectedEpoch,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "controller epoch"); err != nil {
		return nil, err
	}
	current.Epoch = change.NewEpoch
	current.HolderID = change.HolderID
	current.RequestDigest = change.RequestDigest
	current.Version++
	current.AcquiredAt = change.UpdatedAt
	current.UpdatedAt = change.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &current, nil
}

// SyncControllerEpochMirror records the exact Kubernetes-authoritative epoch
// used to fence SQLite payload mutations. It may move a stale mirror forward
// after a crash between authoritative acquisition and mirror persistence, but
// never rewinds or replaces conflicting evidence at the same epoch.
func (s *Store) SyncControllerEpochMirror(ctx context.Context, authoritative store.ControllerEpoch) error {
	name, err := store.NormalizeControllerEpochName(authoritative.Name)
	if err != nil {
		return err
	}
	authoritative.Name = name
	authoritative.HolderID = strings.TrimSpace(authoritative.HolderID)
	if err := store.ValidateControlIdentifier("controller epoch holder ID", authoritative.HolderID); err != nil {
		return err
	}
	if err := store.ValidateCanonicalDigest("controller epoch request digest", authoritative.RequestDigest); err != nil {
		return err
	}
	if authoritative.Epoch < 1 || authoritative.Version < 1 {
		return store.ValidationErrorf("mirrored controller epoch and version must be positive")
	}
	authoritative.AcquiredAt = store.NormalizeControlTime(authoritative.AcquiredAt)
	authoritative.UpdatedAt = store.NormalizeControlTime(authoritative.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin controller epoch mirror sync: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getControllerEpoch(ctx, tx, authoritative.Name)
	if errors.Is(err, store.ErrNotFound) {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO controller_epochs(name, epoch, holder_id, request_digest, version, acquired_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			authoritative.Name, authoritative.Epoch, authoritative.HolderID, authoritative.RequestDigest,
			authoritative.Version, authoritative.AcquiredAt, authoritative.UpdatedAt,
		)
		if err != nil {
			if isSQLiteConstraintError(err) {
				return store.ConflictErrorf("controller epoch mirror %q was created concurrently", authoritative.Name)
			}
			return fmt.Errorf("create controller epoch mirror: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit controller epoch mirror create: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read controller epoch mirror: %w", err)
	}
	if current.Epoch > authoritative.Epoch {
		return store.ConflictErrorf(
			"controller epoch mirror %q is ahead at %d, authoritative epoch is %d",
			authoritative.Name, current.Epoch, authoritative.Epoch,
		)
	}
	if current.Epoch == authoritative.Epoch {
		if current.HolderID != authoritative.HolderID || current.RequestDigest != authoritative.RequestDigest {
			return store.ConflictErrorf(
				"controller epoch mirror %q conflicts at epoch %d", authoritative.Name, authoritative.Epoch,
			)
		}
		return nil
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE controller_epochs
		 SET epoch = ?, holder_id = ?, request_digest = ?, version = ?, acquired_at = ?, updated_at = ?
		 WHERE name = ? AND epoch = ? AND holder_id = ?`,
		authoritative.Epoch, authoritative.HolderID, authoritative.RequestDigest, authoritative.Version,
		authoritative.AcquiredAt, authoritative.UpdatedAt,
		authoritative.Name, current.Epoch, current.HolderID,
	)
	if err != nil {
		return fmt.Errorf("update controller epoch mirror: %w", err)
	}
	if err := rowsAffectedExactlyOne(result, "controller epoch mirror"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit controller epoch mirror update: %w", err)
	}
	return nil
}

func getControllerEpoch(ctx context.Context, q controlQueryRower, name string) (store.ControllerEpoch, error) {
	var epoch store.ControllerEpoch
	err := q.QueryRowContext(ctx,
		`SELECT name, epoch, holder_id, request_digest, version, acquired_at, updated_at
		 FROM controller_epochs WHERE name = ?`,
		name,
	).Scan(&epoch.Name, &epoch.Epoch, &epoch.HolderID, &epoch.RequestDigest, &epoch.Version, &epoch.AcquiredAt, &epoch.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ControllerEpoch{}, store.ErrNotFound
	}
	if err != nil {
		return store.ControllerEpoch{}, fmt.Errorf("get controller epoch: %w", err)
	}
	return epoch, nil
}
