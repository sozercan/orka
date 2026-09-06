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

	"github.com/orka-agents/orka/internal/store"
)

// ProjectSessionLineage implements store.SessionLineageStore. Kubernetes has
// already established the authoritative lineage and mutation Lease before
// this idempotent payload projection runs. A conflicting row is an integrity
// failure to repair explicitly, never a SQLite decision about Session owner.
func (s *Store) ProjectSessionLineage(ctx context.Context, lineage store.SessionLineage) (*store.SessionLineage, error) {
	if err := lineage.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin session lineage claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, err := scanSessionLineage(tx.QueryRowContext(ctx, sessionLineageSelectSQL+`
		WHERE namespace = ? AND session_name = ?`, lineage.Namespace, lineage.SessionName))
	switch {
	case err == nil:
		if mismatch := sessionLineageProjectionMismatch(existing, lineage); mismatch != "" {
			return nil, fmt.Errorf("%w: session %s/%s lineage %s", store.ErrConflict,
				lineage.Namespace, lineage.SessionName, mismatch)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit session lineage projection verification: %w", err)
		}
		return existing, nil
	case errors.Is(err, sql.ErrNoRows):
	default:
		return nil, fmt.Errorf("read session lineage: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO session_lineages
		(namespace, session_name, namespace_uid, session_uid, contract_version,
		 lineage_generation, runtime_identity, config_digest, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		lineage.Namespace, lineage.SessionName, lineage.NamespaceUID, lineage.SessionUID,
		lineage.ContractVersion, lineage.LineageGeneration, lineage.RuntimeIdentity, lineage.ConfigDigest,
		lineage.Version, lineage.CreatedAt.UTC(), lineage.UpdatedAt.UTC()); err != nil {
		return nil, fmt.Errorf("project session lineage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit session lineage projection: %w", err)
	}
	result := lineage
	result.CreatedAt = result.CreatedAt.UTC()
	result.UpdatedAt = result.UpdatedAt.UTC()
	return &result, nil
}

// GetSessionLineage implements store.SessionLineageStore.
func (s *Store) GetSessionLineage(ctx context.Context, namespace, sessionName string) (*store.SessionLineage, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(sessionName) == "" {
		return nil, store.ValidationErrorf("session lineage namespace and session name are required")
	}
	lineage, err := scanSessionLineage(s.db.QueryRowContext(ctx, sessionLineageSelectSQL+`
		WHERE namespace = ? AND session_name = ?`, namespace, sessionName))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get session lineage: %w", err)
	}
	return lineage, nil
}

const sessionLineageSelectSQL = `SELECT namespace, session_name, namespace_uid, session_uid,
	contract_version, lineage_generation, runtime_identity, config_digest, version,
	created_at, updated_at
	FROM session_lineages`

type sessionLineageScanner interface {
	Scan(dest ...any) error
}

func scanSessionLineage(row sessionLineageScanner) (*store.SessionLineage, error) {
	var lineage store.SessionLineage
	if err := row.Scan(&lineage.Namespace, &lineage.SessionName, &lineage.NamespaceUID,
		&lineage.SessionUID, &lineage.ContractVersion, &lineage.LineageGeneration,
		&lineage.RuntimeIdentity, &lineage.ConfigDigest,
		&lineage.Version, &lineage.CreatedAt, &lineage.UpdatedAt); err != nil {
		return nil, err
	}
	lineage.CreatedAt = lineage.CreatedAt.UTC()
	lineage.UpdatedAt = lineage.UpdatedAt.UTC()
	return &lineage, nil
}

// sessionLineageProjectionMismatch reports any immutable difference between
// the Kubernetes-authoritative record and its SQLite payload projection.
func sessionLineageProjectionMismatch(existing *store.SessionLineage, lineage store.SessionLineage) string {
	switch {
	case existing.ContractVersion != lineage.ContractVersion:
		return fmt.Sprintf("is bound to contract %s, not %s", existing.ContractVersion, lineage.ContractVersion)
	case existing.SessionUID != lineage.SessionUID:
		return "belongs to a different Session UID; a recreated same-name Session never attaches to old runtime state"
	case existing.NamespaceUID != lineage.NamespaceUID:
		return "belongs to a different namespace UID; a recreated same-name namespace never attaches to old runtime state"
	case existing.RuntimeIdentity != lineage.RuntimeIdentity:
		return fmt.Sprintf("is bound to runtime identity %q, not %q", existing.RuntimeIdentity, lineage.RuntimeIdentity)
	case existing.ConfigDigest != lineage.ConfigDigest:
		return fmt.Sprintf("is bound to configuration digest %q, not %q", existing.ConfigDigest, lineage.ConfigDigest)
	case existing.LineageGeneration != lineage.LineageGeneration:
		return fmt.Sprintf("has lineage generation %d, not %d", existing.LineageGeneration, lineage.LineageGeneration)
	default:
		return ""
	}
}
