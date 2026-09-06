/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package harness

import (
	"context"
	"time"
)

// RuntimeSessionTransition describes an optimistic state transition for a
// namespace-owned RuntimeSession.
type RuntimeSessionTransition struct {
	Namespace string
	ID        RuntimeSessionID
	From      RuntimeSessionState
	To        RuntimeSessionState
	// ActiveTask nil preserves the existing active task. A non-nil pointer is
	// trimmed and written; an empty string clears the active task.
	ActiveTask *string
	// UpdatedAt defaults to time.Now().UTC() when zero.
	UpdatedAt time.Time
}

// RuntimeSessionStore is the internal-store-first persistence seam for
// backend-neutral runtime sessions. Implementations should return store.ErrNotFound
// for missing sessions, store.ErrConflict for stale transitions, and
// store.ErrValidation for invalid records or requests.
type RuntimeSessionStore interface {
	CreateRuntimeSession(ctx context.Context, session *RuntimeSession) error
	GetRuntimeSession(ctx context.Context, namespace string, id RuntimeSessionID) (*RuntimeSession, error)
	TransitionRuntimeSession(ctx context.Context, transition RuntimeSessionTransition) (*RuntimeSession, error)
	DeleteRuntimeSession(ctx context.Context, namespace string, id RuntimeSessionID) error
}
