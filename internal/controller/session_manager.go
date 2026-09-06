/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

// SessionManager manages sessions for conversation continuity.
// It delegates persistence to a store.SessionStore and orchestrates
// the acquire → job → append → release lifecycle.
type SessionManager struct {
	store             store.SessionStore
	gatewayEventStore store.GatewayEventStore
	cleanupStore      store.SessionCleanupStore
	epochs            *ControllerEpochManager
}

// NewSessionManager creates a new SessionManager backed by the given store.
func NewSessionManager(ss store.SessionStore) *SessionManager {
	return &SessionManager{store: ss}
}

// SetGatewayEventStore enables durable gateway transcript policy resolution.
func (m *SessionManager) SetGatewayEventStore(events store.GatewayEventStore) {
	if m != nil {
		m.gatewayEventStore = events
	}
}

// SetACPSessionCleanup enables the hard-cutover cross-store Session deletion
// path. It is configured only when ACP Kubernetes control storage is enabled.
func (m *SessionManager) SetACPSessionCleanup(cleanup store.SessionCleanupStore, epochs *ControllerEpochManager) {
	if m != nil {
		m.cleanupStore = cleanup
		m.epochs = epochs
	}
}

// IsLocked checks if a session is locked by another task.
func (m *SessionManager) IsLocked(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if event, ok, err := m.gatewayEventForTask(ctx, task); err != nil {
		return false, err
	} else if ok {
		if err := validateGatewayTaskSessionPolicy(task, event); err != nil {
			return false, err
		}
		return m.store.IsLocked(ctx, event.Namespace, event.SessionName, task.Name, string(task.UID))
	}
	if task.Spec.SessionRef == nil {
		return false, nil
	}

	locked, err := m.store.IsLocked(ctx, task.Namespace, task.Spec.SessionRef.Name, task.Name, string(task.UID))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Session doesn't exist yet, not locked
			return false, nil
		}
		return false, err
	}

	return locked, nil
}

// AcquireLock acquires the session lock for a task.
func (m *SessionManager) AcquireLock(ctx context.Context, task *corev1alpha1.Task) error {
	if event, ok, err := m.gatewayEventForTask(ctx, task); err != nil {
		return err
	} else if ok {
		if err := validateGatewayTaskSessionPolicy(task, event); err != nil {
			return err
		}
		return m.store.AcquireLock(ctx, event.Namespace, event.SessionName, task.Name, string(task.UID))
	}
	if task.Spec.SessionRef == nil {
		return nil
	}

	// Check if session exists
	_, err := m.store.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		// Session doesn't exist
		if task.Spec.SessionRef.Create {
			return m.createSession(ctx, task)
		}
		return fmt.Errorf("session %s not found and create=false: %w", task.Spec.SessionRef.Name, store.ErrNotFound)
	}

	return m.store.AcquireLock(ctx, task.Namespace, task.Spec.SessionRef.Name, task.Name, string(task.UID))
}

// ReleaseLock releases the session lock for a task.
func (m *SessionManager) ReleaseLock(ctx context.Context, task *corev1alpha1.Task) error {
	if event, ok, err := m.gatewayEventForTask(ctx, task); err != nil {
		return err
	} else if ok {
		return m.store.ReleaseLock(ctx, event.Namespace, event.SessionName, task.Name, string(task.UID))
	}
	if task.Spec.SessionRef == nil {
		return nil
	}

	if task.UID != "" {
		session, getErr := m.store.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name)
		if getErr == nil && session.ActiveTask == task.Name && session.ActiveTaskUID == "" {
			if acquireErr := m.store.AcquireLock(ctx, task.Namespace, task.Spec.SessionRef.Name, task.Name, string(task.UID)); acquireErr != nil {
				return acquireErr
			}
		} else if getErr != nil && !errors.Is(getErr, store.ErrNotFound) {
			return getErr
		}
	}
	err := m.store.ReleaseLock(ctx, task.Namespace, task.Spec.SessionRef.Name, task.Name, string(task.UID))
	if err != nil {
		// Ignore not-found errors (session may have been deleted)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (m *SessionManager) gatewayEventForTask(
	ctx context.Context, task *corev1alpha1.Task,
) (*store.GatewayEvent, bool, error) {
	if m == nil || m.gatewayEventStore == nil || task == nil || task.UID == "" {
		return nil, false, nil
	}
	event, err := m.gatewayEventStore.GetGatewayEventForTask(ctx, task.Namespace, task.Name, string(task.UID))
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return event, true, nil
}

func validateGatewayTaskSessionPolicy(task *corev1alpha1.Task, event *store.GatewayEvent) error {
	if task == nil || event == nil || task.Spec.SessionRef == nil {
		return store.ValidationErrorf("gateway Task session policy is missing")
	}
	ref := task.Spec.SessionRef
	if ref.Name != event.SessionName || ref.Create || ref.Append ||
		ref.MaxMessages != store.GatewayTranscriptMessageLimit ||
		ref.ThroughMessageID != store.GatewayUserMessageID(event.ID) || !ref.PromptIncluded {
		return store.ValidationErrorf("gateway Task session policy was modified")
	}
	return nil
}

// createSession creates a new session and acquires the lock in one step.
func (m *SessionManager) createSession(ctx context.Context, task *corev1alpha1.Task) error {
	now := time.Now()
	session := &store.SessionRecord{
		Namespace:     task.Namespace,
		Name:          task.Spec.SessionRef.Name,
		SessionType:   "task",
		ActiveTask:    task.Name,
		ActiveTaskUID: string(task.UID),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	return m.store.CreateSession(ctx, session)
}

// AppendMessages appends messages from a completed task to the session.
// The resultStore is used to fetch the task result for the assistant message.
func (m *SessionManager) AppendMessages(ctx context.Context, task *corev1alpha1.Task, resultStore store.ResultStore) error {
	if task.Spec.SessionRef == nil || !task.Spec.SessionRef.Append {
		return nil
	}

	if _, err := m.store.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}

	var prompt, response string

	if !task.Spec.SessionRef.PromptIncluded {
		if task.Spec.AI != nil && task.Spec.AI.Prompt != "" {
			prompt = task.Spec.AI.Prompt
		} else if task.Spec.Prompt != "" {
			prompt = task.Spec.Prompt
		}
	}

	// Try to get the response from the result store
	if resultStore != nil && task.Status.ResultRef != nil && task.Status.ResultRef.Available {
		data, err := resultStore.GetResult(ctx, task.Namespace, task.Name)
		if err == nil {
			response = string(data)
		}
	}

	now := time.Now()
	var messages []store.SessionMessage

	if prompt != "" {
		messages = append(messages, store.SessionMessage{
			Role:      "user",
			Content:   prompt,
			Timestamp: now,
		})
	}

	if response != "" {
		messages = append(messages, store.SessionMessage{
			Role:      "assistant",
			Content:   response,
			Timestamp: now,
		})
	}

	if len(messages) == 0 {
		return nil
	}

	return m.store.AppendMessages(ctx, task.Namespace, task.Spec.SessionRef.Name, messages)
}

// LoadTranscript loads the session transcript for a task.
func (m *SessionManager) LoadTranscript(ctx context.Context, task *corev1alpha1.Task) ([]store.SessionMessage, error) {
	if task == nil {
		return nil, nil
	}
	if m.gatewayEventStore != nil && task.UID != "" {
		event, err := m.gatewayEventStore.GetGatewayEventForTask(ctx, task.Namespace, task.Name, string(task.UID))
		if err == nil {
			return m.store.LoadTranscriptThrough(
				ctx,
				event.Namespace,
				event.SessionName,
				store.GatewayUserMessageID(event.ID),
				store.GatewayTranscriptMessageLimit,
			)
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}
	if task.Spec.SessionRef == nil {
		return nil, nil
	}

	maxMessages := 50 // Default
	if task.Spec.SessionRef.MaxMessages > 0 {
		maxMessages = int(task.Spec.SessionRef.MaxMessages)
	}

	var messages []store.SessionMessage
	var err error
	if task.Spec.SessionRef.ThroughMessageID != "" {
		messages, err = m.store.LoadTranscriptThrough(
			ctx, task.Namespace, task.Spec.SessionRef.Name, task.Spec.SessionRef.ThroughMessageID, maxMessages,
		)
	} else {
		messages, err = m.store.LoadTranscript(ctx, task.Namespace, task.Spec.SessionRef.Name, maxMessages)
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return messages, nil
}

// GetSession gets a session by name.
func (m *SessionManager) GetSession(ctx context.Context, namespace, name string) (*store.SessionRecord, error) {
	return m.store.GetSession(ctx, namespace, name)
}

// DeleteSession deletes a session. ACP-enabled deployments coordinate the
// Kubernetes-authoritative controls before removing the SQLite transcript.
func (m *SessionManager) DeleteSession(ctx context.Context, namespace, name string) error {
	if m.cleanupStore == nil {
		return m.store.DeleteSession(ctx, namespace, name)
	}
	if m.epochs == nil {
		return fmt.Errorf("ACP session cleanup requires a controller epoch manager")
	}
	fence, err := m.epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	operationID := store.CanonicalControlID("session-cleanup", namespace, name)
	operationDigest, err := acpDomainDigest("session-cleanup", map[string]string{
		"namespace": namespace, "sessionName": name, "operationID": operationID,
	})
	if err != nil {
		return err
	}
	return m.cleanupStore.ReclaimSession(ctx, store.ReclaimSessionRequest{
		Namespace: namespace, SessionName: name, Fence: fence,
		OperationID: operationID, OperationDigest: operationDigest, RequestedAt: time.Now().UTC(),
	})
}

// ListSessions lists all sessions in a namespace.
func (m *SessionManager) ListSessions(ctx context.Context, namespace string) ([]store.SessionMetadata, error) {
	return m.store.ListSessions(ctx, namespace)
}

// ListSessionsPage lists one name-ordered page of sessions in a namespace.
func (m *SessionManager) ListSessionsPage(ctx context.Context, namespace, afterName string, limit int, excludeType string) ([]store.SessionMetadata, bool, error) {
	return m.store.ListSessionsPage(ctx, namespace, afterName, limit, excludeType)
}
