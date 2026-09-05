package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/store"
)

type pendingSessionCleanupStore struct {
	store.SessionStore
	cleanupErr error
}

func (s *pendingSessionCleanupStore) DeleteSession(ctx context.Context, namespace, name string) error {
	if s.cleanupErr != nil {
		return s.cleanupErr
	}
	return s.SessionStore.DeleteSession(ctx, namespace, name)
}

func TestHandlers_DeleteSession_UnsettledRuntimeCleanup(t *testing.T) {
	for _, managed := range []bool{false, true} {
		for _, cleanupErr := range []error{store.ErrNotReady, fmt.Errorf("write Task runtime cleanup is incomplete: %w", store.ErrNotReady)} {
			t.Run(fmt.Sprintf("manager=%t/error=%s", managed, cleanupErr), func(t *testing.T) {
				handlers, app, ss := setupTestHandlersWithSessionManager()
				ctx := context.Background()
				require.NoError(t, ss.CreateSession(ctx, &store.SessionRecord{
					Namespace: "default", Name: "cleanup-pending", SessionType: "task",
				}))
				pending := &pendingSessionCleanupStore{SessionStore: ss, cleanupErr: cleanupErr}
				handlers.sessionStore = pending
				if managed {
					handlers.sessionManager = controller.NewSessionManager(pending)
				}
				app.Delete("/sessions/:id", handlers.DeleteSession)

				resp, err := app.Test(httptest.NewRequest(http.MethodDelete, "/sessions/cleanup-pending", nil))
				require.NoError(t, err)
				defer func(body io.Closer) { require.NoError(t, body.Close()) }(resp.Body)
				require.Equal(t, http.StatusConflict, resp.StatusCode)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, "session has active or unsettled work", string(body))
				_, err = ss.GetSession(ctx, "default", "cleanup-pending")
				require.NoError(t, err)

				pending.cleanupErr = nil
				resp, err = app.Test(httptest.NewRequest(http.MethodDelete, "/sessions/cleanup-pending", nil))
				require.NoError(t, err)
				defer func(body io.Closer) { require.NoError(t, body.Close()) }(resp.Body)
				require.Equal(t, http.StatusNoContent, resp.StatusCode)
				_, err = ss.GetSession(ctx, "default", "cleanup-pending")
				require.ErrorIs(t, err, store.ErrNotFound)
			})
		}
	}
}
