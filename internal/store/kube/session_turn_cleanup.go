package kube

import (
	"context"
	"errors"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ store.SessionTurnCleanupReceiptStore = (*Store)(nil)

// GetSessionTurnCleanupReceipt reads archived finalization evidence only while
// the completed Session's authoritative Kubernetes resources remain absent.
func (s *Store) GetSessionTurnCleanupReceipt(ctx context.Context, namespace, sessionName, promptAttemptID string) (*store.SessionTurnCleanupReceipt, error) {
	if s.sessionCleanup == nil {
		return nil, ErrSessionCleanupStoreNotConfigured
	}
	if s.watchNamespace != "" && namespace != s.watchNamespace {
		return nil, store.ConflictErrorf("Session cleanup receipt is outside the configured namespace")
	}
	receipt, err := s.sessionCleanup.GetSessionTurnCleanupReceipt(ctx, namespace, sessionName, promptAttemptID)
	if err != nil {
		return nil, err
	}
	if receipt == nil || receipt.PromptAttemptID != promptAttemptID {
		return nil, store.ConflictErrorf("Session cleanup receipt does not match its prompt attempt")
	}
	if err := receipt.Validate(namespace, sessionName, receipt.TurnID); err != nil {
		return nil, err
	}
	if err := s.ensureCompletedSessionKubernetesStateAbsent(ctx, store.SessionCleanupCompletion{
		Namespace: namespace, SessionName: sessionName, SessionUID: receipt.Key.SessionUID,
	}); err != nil {
		return nil, err
	}
	return receipt, nil
}

func (s *Store) sessionTurnForPromptAttemptReclamation(
	ctx context.Context,
	marker promptAttemptReclamationMarker,
	turnID, promptAttemptID string,
) (*store.SessionTurn, *store.SessionTurnCleanupReceipt, error) {
	if s.sessionTurns == nil {
		return nil, nil, ErrSessionTurnStoreNotConfigured
	}
	turn, err := s.sessionTurns.GetSessionTurn(ctx, turnID)
	if !errors.Is(err, store.ErrNotFound) {
		return turn, nil, err
	}
	task := &corev1alpha1.Task{}
	if err := s.readClient().Get(ctx, client.ObjectKey{Namespace: marker.Namespace, Name: marker.TaskName}, task); err != nil {
		return nil, nil, mapKubernetesError("load Task for Session cleanup receipt", err)
	}
	if task.Spec.SessionRef == nil {
		return nil, nil, promptAttemptReclaimNotReady("missing SessionTurn %q has no Task Session binding", turnID)
	}
	receipt, err := s.GetSessionTurnCleanupReceipt(ctx, marker.Namespace, task.Spec.SessionRef.Name, promptAttemptID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, promptAttemptReclaimNotReady("SessionTurn %q has no durable finalization or cleanup receipt", turnID)
	}
	if err != nil {
		return nil, nil, err
	}
	if receipt.Key.TaskUID != marker.TaskUID || receipt.TurnID != turnID {
		return nil, nil, store.ConflictErrorf("Session cleanup receipt does not belong to the reclaiming Task")
	}
	if receipt.ProjectionState != store.OutboxProjectionDelivered {
		return nil, nil, promptAttemptReclaimNotReady("SessionTurn %q cleanup receipt projection was not delivered", turnID)
	}
	return receipt.SessionTurn(), receipt, nil
}

func (s *Store) verifySessionCleanupBeforePromptAttemptDeletion(
	ctx context.Context,
	marker promptAttemptReclamationMarker,
	candidates []*corev1alpha1.PromptAttempt,
) error {
	if !marker.ContinuitySession {
		return nil
	}
	// A newer attempt may settle before binding to its Session. Every older
	// bound attempt still owns frozen runtime cleanup authority, so none may
	// be removed until Session deletion has archived all of that evidence.
	for _, object := range candidates {
		attempt := promptAttemptFromObject(object)
		if attempt.SessionUID == "" && attempt.SessionLeaseGeneration == 0 {
			continue
		}
		key := store.SessionTurnKey{
			SessionUID: attempt.SessionUID, LeaseGeneration: attempt.SessionLeaseGeneration,
			TaskUID: attempt.Key.TaskUID, Attempt: attempt.Key.Attempt, PromptID: attempt.Key.PromptID,
		}
		turnID, err := key.CanonicalID()
		if err != nil {
			return err
		}
		_, receipt, err := s.sessionTurnForPromptAttemptReclamation(ctx, marker, turnID, attempt.ID)
		if err != nil {
			return err
		}
		if receipt == nil {
			return promptAttemptReclaimNotReady("Session cleanup has not archived prompt attempt %q", attempt.ID)
		}
	}
	return nil
}
