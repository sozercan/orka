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

	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/store"
)

var _ harness.RuntimeSessionStore = (*Store)(nil)

// CreateRuntimeSession persists a new backend-neutral RuntimeSession.
func (s *Store) CreateRuntimeSession(ctx context.Context, session *harness.RuntimeSession) error {
	normalized, err := normalizeRuntimeSessionForCreate(session)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO runtime_sessions (
			id, namespace, session_name, active_task, agent_name, provider, state, cleanup_policy,
			idle_timeout_ns, max_lifetime_ns, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(normalized.ID), normalized.Owner.Namespace, normalized.Owner.SessionName, normalized.Owner.ActiveTask,
		normalized.Owner.AgentName, string(normalized.Owner.Provider), string(normalized.State), string(normalized.CleanupPolicy),
		int64(normalized.IdleTimeout), int64(normalized.MaxLifetime), normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if isSQLiteConstraintError(err) {
			return fmt.Errorf("%w: runtime session %s/%s already exists", store.ErrConflict, normalized.Owner.Namespace, normalized.ID)
		}
		return err
	}
	*session = normalized
	return nil
}

// GetRuntimeSession loads a RuntimeSession by namespace and canonical ID.
func (s *Store) GetRuntimeSession(ctx context.Context, namespace string, id harness.RuntimeSessionID) (*harness.RuntimeSession, error) {
	session, err := getRuntimeSessionTx(ctx, s.db, namespace, id)
	if err != nil {
		return nil, err
	}
	return &session, nil
}

// TransitionRuntimeSession validates and persists an optimistic RuntimeSession state transition.
func (s *Store) TransitionRuntimeSession(
	ctx context.Context,
	transition harness.RuntimeSessionTransition,
) (*harness.RuntimeSession, error) {
	transition, err := normalizeRuntimeSessionTransition(transition)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	session, err := getRuntimeSessionTx(ctx, tx, transition.Namespace, transition.ID)
	if err != nil {
		return nil, err
	}
	if session.State != transition.From {
		return nil, fmt.Errorf(
			"%w: runtime session %s/%s is %s, expected %s",
			store.ErrConflict,
			transition.Namespace,
			transition.ID,
			session.State,
			transition.From,
		)
	}
	activeTask := session.Owner.ActiveTask
	if transition.ActiveTask != nil {
		activeTask = strings.TrimSpace(*transition.ActiveTask)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE runtime_sessions SET state = ?, active_task = ?, updated_at = ?
		 WHERE namespace = ? AND id = ? AND state = ?`,
		string(transition.To), activeTask, transition.UpdatedAt, transition.Namespace, string(transition.ID), string(transition.From),
	)
	if err != nil {
		return nil, err
	}
	if err := ensureRowsAffected(res); err != nil {
		return nil, err
	}
	session.State = transition.To
	session.Owner.ActiveTask = activeTask
	session.UpdatedAt = transition.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &session, nil
}

// DeleteRuntimeSession physically prunes a RuntimeSession after it reaches Deleted.
func (s *Store) DeleteRuntimeSession(ctx context.Context, namespace string, id harness.RuntimeSessionID) error {
	namespace, id, err := normalizeRuntimeSessionKey(namespace, id)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM runtime_sessions WHERE namespace = ? AND id = ? AND state = ?`,
		namespace,
		string(id),
		string(harness.RuntimeSessionStateDeleted),
	)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		return nil
	}
	session, err := getRuntimeSessionTx(ctx, s.db, namespace, id)
	if err != nil {
		return err
	}
	return store.ValidationErrorf("runtime session %s/%s must be %s before physical deletion (current state: %s)", namespace, id, harness.RuntimeSessionStateDeleted, session.State)
}

type runtimeSessionQueryable interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getRuntimeSessionTx(ctx context.Context, db runtimeSessionQueryable, namespace string, id harness.RuntimeSessionID) (harness.RuntimeSession, error) {
	namespace, id, err := normalizeRuntimeSessionKey(namespace, id)
	if err != nil {
		return harness.RuntimeSession{}, err
	}
	row := db.QueryRowContext(ctx, runtimeSessionSelectSQL()+` WHERE namespace = ? AND id = ?`, namespace, strings.TrimSpace(string(id)))
	session, err := scanRuntimeSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return harness.RuntimeSession{}, store.ErrNotFound
	}
	if err != nil {
		return harness.RuntimeSession{}, err
	}
	return session, nil
}

func runtimeSessionSelectSQL() string {
	return `SELECT id, namespace, session_name, active_task, agent_name, provider, state, cleanup_policy,
		idle_timeout_ns, max_lifetime_ns, created_at, updated_at FROM runtime_sessions`
}

type runtimeSessionScanner interface {
	Scan(dest ...any) error
}

func scanRuntimeSession(scanner runtimeSessionScanner) (harness.RuntimeSession, error) {
	var session harness.RuntimeSession
	var provider, state, cleanupPolicy string
	var idleTimeoutNS, maxLifetimeNS int64
	err := scanner.Scan(
		&session.ID,
		&session.Owner.Namespace,
		&session.Owner.SessionName,
		&session.Owner.ActiveTask,
		&session.Owner.AgentName,
		&provider,
		&state,
		&cleanupPolicy,
		&idleTimeoutNS,
		&maxLifetimeNS,
		&session.CreatedAt,
		&session.UpdatedAt,
	)
	if err != nil {
		return harness.RuntimeSession{}, err
	}
	session.Owner.Provider = harness.ProviderKind(provider)
	session.State = harness.RuntimeSessionState(state)
	session.CleanupPolicy = harness.RuntimeCleanupPolicy(cleanupPolicy)
	session.IdleTimeout = time.Duration(idleTimeoutNS)
	session.MaxLifetime = time.Duration(maxLifetimeNS)
	return session, nil
}

func normalizeRuntimeSessionForCreate(session *harness.RuntimeSession) (harness.RuntimeSession, error) {
	if session == nil {
		return harness.RuntimeSession{}, store.ValidationErrorf("runtime session is required")
	}
	normalized := *session
	normalized.ID = harness.RuntimeSessionID(strings.TrimSpace(string(normalized.ID)))
	normalized.Owner.Namespace = strings.TrimSpace(normalized.Owner.Namespace)
	normalized.Owner.SessionName = strings.TrimSpace(normalized.Owner.SessionName)
	normalized.Owner.ActiveTask = strings.TrimSpace(normalized.Owner.ActiveTask)
	normalized.Owner.AgentName = strings.TrimSpace(normalized.Owner.AgentName)
	normalized.Owner.Provider = harness.ProviderKind(strings.TrimSpace(string(normalized.Owner.Provider)))
	if normalized.State == "" {
		normalized.State = harness.RuntimeSessionStatePending
	}
	if normalized.CleanupPolicy == "" {
		normalized.CleanupPolicy = harness.RuntimeCleanupPolicyDelete
	}
	if err := normalized.Validate(); err != nil {
		return harness.RuntimeSession{}, store.ValidationErrorf("invalid runtime session: %v", err)
	}
	now := time.Now().UTC()
	if normalized.CreatedAt.IsZero() {
		normalized.CreatedAt = now
	} else {
		normalized.CreatedAt = normalized.CreatedAt.UTC()
	}
	if normalized.UpdatedAt.IsZero() {
		normalized.UpdatedAt = normalized.CreatedAt
	} else {
		normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	}
	return normalized, nil
}

func normalizeRuntimeSessionTransition(transition harness.RuntimeSessionTransition) (harness.RuntimeSessionTransition, error) {
	namespace, id, err := normalizeRuntimeSessionKey(transition.Namespace, transition.ID)
	if err != nil {
		return harness.RuntimeSessionTransition{}, err
	}
	transition.Namespace = namespace
	transition.ID = id
	if err := harness.ValidateRuntimeSessionTransition(transition.From, transition.To); err != nil {
		return harness.RuntimeSessionTransition{}, store.ValidationErrorf("invalid runtime session transition: %v", err)
	}
	if transition.UpdatedAt.IsZero() {
		transition.UpdatedAt = time.Now().UTC()
	} else {
		transition.UpdatedAt = transition.UpdatedAt.UTC()
	}
	return transition, nil
}

func normalizeRuntimeSessionKey(namespace string, id harness.RuntimeSessionID) (string, harness.RuntimeSessionID, error) {
	namespace = strings.TrimSpace(namespace)
	id = harness.RuntimeSessionID(strings.TrimSpace(string(id)))
	if namespace == "" {
		return "", "", store.ValidationErrorf("runtime session namespace is required")
	}
	if id == "" {
		return "", "", store.ValidationErrorf("runtime session id is required")
	}
	return namespace, id, nil
}
