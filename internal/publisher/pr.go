package publisher

import (
	"context"
	"fmt"
	"strings"
)

type pullRequestRepositoryIdentity struct {
	Provider string `json:"provider"`
	ID       string `json:"id"`
}

// Key returns the exact immutable tuple identity used for PR reconciliation.
func (i PullRequestIntent) Key() (string, error) {
	if err := validatePullRequestIntent(i); err != nil {
		return "", err
	}
	return digestCanonical(struct {
		Domain                string                        `json:"domain"`
		BaseRepository        pullRequestRepositoryIdentity `json:"baseRepository"`
		BaseRef               string                        `json:"baseRef"`
		HeadRepository        pullRequestRepositoryIdentity `json:"headRepository"`
		HeadRef               string                        `json:"headRef"`
		PublicationGeneration int64                         `json:"publicationGeneration"`
		ExpectedHeadOID       string                        `json:"expectedHeadOid"`
		SessionUID            string                        `json:"sessionUid,omitempty"`
	}{
		Domain:         "orka.publisher.pr-intent.v1",
		BaseRepository: pullRequestRepositoryIdentity{Provider: i.BaseRepository.Provider, ID: i.BaseRepository.ID}, BaseRef: i.BaseRef,
		HeadRepository: pullRequestRepositoryIdentity{Provider: i.HeadRepository.Provider, ID: i.HeadRepository.ID}, HeadRef: i.HeadRef,
		PublicationGeneration: i.PublicationGeneration, ExpectedHeadOID: i.ExpectedHeadOID, SessionUID: i.SessionUID,
	})
}

// SessionKey identifies one Session's PR across verified publications. The
// per-publication Key and receipt still bind the current exact head commit.
func (i PullRequestIntent) SessionKey() (string, error) {
	if err := validatePullRequestIntent(i); err != nil {
		return "", err
	}
	if i.SessionUID == "" {
		return "", nil
	}
	return digestCanonical(struct {
		Domain         string                        `json:"domain"`
		BaseRepository pullRequestRepositoryIdentity `json:"baseRepository"`
		BaseRef        string                        `json:"baseRef"`
		HeadRepository pullRequestRepositoryIdentity `json:"headRepository"`
		HeadRef        string                        `json:"headRef"`
		SessionUID     string                        `json:"sessionUid"`
	}{
		Domain:         "orka.publisher.pr-session.v1",
		BaseRepository: pullRequestRepositoryIdentity{Provider: i.BaseRepository.Provider, ID: i.BaseRepository.ID}, BaseRef: i.BaseRef,
		HeadRepository: pullRequestRepositoryIdentity{Provider: i.HeadRepository.Provider, ID: i.HeadRepository.ID}, HeadRef: i.HeadRef,
		SessionUID: i.SessionUID,
	})
}

func validatePullRequestIntent(intent PullRequestIntent) error {
	if err := validateRepository(intent.BaseRepository); err != nil {
		return err
	}
	if err := validateRepository(intent.HeadRepository); err != nil {
		return err
	}
	if err := validateBranchRef(intent.BaseRef); err != nil {
		return err
	}
	if err := validateBranchRef(intent.HeadRef); err != nil {
		return err
	}
	if intent.PublicationGeneration < 1 {
		return invalid("PR publication generation", "must be at least 1")
	}
	if len(intent.SessionUID) > 512 || strings.TrimSpace(intent.SessionUID) != intent.SessionUID {
		return invalid("PR session UID", "must be a bounded immutable identifier")
	}
	return validateObjectID("PR expected head", intent.ExpectedHeadOID)
}

// ReconcilePullRequest validates the exact tuple before and after delegating to
// the SCM-specific reconciler. The caller must have durably persisted intent
// before invoking this method.
func ReconcilePullRequest(ctx context.Context, intent PullRequestIntent, reconciler PullRequestReconciler) (PullRequestReceipt, error) {
	if reconciler == nil {
		return PullRequestReceipt{}, invalid("PR reconciler", "must not be nil")
	}
	key, err := intent.Key()
	if err != nil {
		return PullRequestReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return PullRequestReceipt{}, err
	}
	receipt, err := reconciler.Reconcile(ctx, intent)
	if err != nil {
		return PullRequestReceipt{}, err
	}
	if receipt.IntentKey != key || receipt.ForgeID == "" || receipt.URL == "" || receipt.HeadOID != intent.ExpectedHeadOID {
		return PullRequestReceipt{}, operationError(ErrIdempotencyConflict, "validate PR receipt", "forge receipt does not match the exact persisted tuple", nil)
	}
	switch receipt.State {
	case PullRequestOpen, PullRequestClosed, PullRequestMerged:
		return receipt, nil
	default:
		return PullRequestReceipt{}, fmt.Errorf("validate PR receipt state %q: %w", receipt.State, ErrInvalidRequest)
	}
}
