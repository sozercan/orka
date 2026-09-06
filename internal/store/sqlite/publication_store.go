package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.PublicationStore = (*Store)(nil)

// CreatePublication persists immutable clean-room publication intent in Preparing.
func (s *Store) CreatePublication(ctx context.Context, publication *store.Publication, fence store.ControllerEpochFence) (*store.Publication, error) {
	normalized, fence, err := store.NormalizePublicationForCreate(publication, fence)
	if err != nil {
		return nil, err
	}
	prIntentJSON, err := marshalOptionalControlJSON(normalized.PRIntent)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return nil, err
	}
	if existing, getErr := getPublication(ctx, tx, normalized.ID); getErr == nil {
		if store.SamePublicationCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("publication %q was reused with different immutable intent or request digest", normalized.ID)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}
	claim, err := getBranchClaim(ctx, tx, normalized.BranchClaimID)
	if err != nil {
		return nil, fmt.Errorf("publication branch claim: %w", err)
	}
	if claim.RepositoryID != normalized.TargetRepositoryID || claim.Ref != normalized.TargetRef ||
		claim.Generation != normalized.BranchClaimGeneration || !claim.LastVerified.Equal(normalized.Baseline) ||
		claim.Availability != store.BranchClaimAvailable {
		return nil, store.ConflictErrorf("publication %q branch claim does not match exact target, generation, baseline, or availability", normalized.ID)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO publications(
			id, namespace, generation, task_uid, attempt, prompt_id, session_uid, branch_claim_id,
			branch_claim_generation, source_repository_id, source_ref, source_baseline_sha, target_repository_id, target_ref,
			baseline_absent, baseline_sha, artifact_id, artifact_digest, artifact_size_bytes, artifact_media_type,
			publication_credential_ref, commit_identity,
			commit_message, commit_timestamp, pr_intent, request_digest, state, prepared_receipt,
			publish_receipt, verification_receipt, pr_receipt, terminal_reason, controller_epoch_name,
			controller_epoch, last_operation_id, last_operation_digest, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID, normalized.Namespace, normalized.Generation, normalized.TaskUID, normalized.Attempt,
		normalized.PromptID, normalized.SessionUID, normalized.BranchClaimID, normalized.BranchClaimGeneration,
		normalized.SourceRepositoryID, normalized.SourceRef, normalized.SourceBaselineSHA, normalized.TargetRepositoryID, normalized.TargetRef,
		normalized.Baseline.Absent, normalized.Baseline.SHA, normalized.ArtifactID, normalized.ArtifactDigest,
		normalized.ArtifactSizeBytes, normalized.ArtifactMediaType,
		normalized.PublicationCredentialRef, normalized.CommitIdentity, normalized.CommitMessage,
		normalized.CommitTimestamp, prIntentJSON, normalized.RequestDigest, string(normalized.State), nil, nil, nil, nil,
		normalized.TerminalReason, normalized.ControllerEpochName, normalized.ControllerEpoch,
		normalized.LastOperationID, normalized.LastOperationDigest, normalized.Version,
		normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, err
		}
		existing, getErr := getPublication(ctx, tx, normalized.ID)
		if getErr == nil && store.SamePublicationCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("publication %q was reused with different immutable intent or request digest", normalized.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &normalized, nil
}

// GetPublication returns a publication and its exact receipts.
func (s *Store) GetPublication(ctx context.Context, id string) (*store.Publication, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("publication ID", id); err != nil {
		return nil, err
	}
	publication, err := getPublication(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &publication, nil
}

// SetPublicationPRIntent persists the exact PR tuple after independent remote
// verification has fixed the current head and before the first forge call.
func (s *Store) SetPublicationPRIntent(ctx context.Context, request store.SetPublicationPRIntentRequest) (*store.Publication, error) {
	request.ID = strings.TrimSpace(request.ID)
	if err := store.ValidateControlIdentifier("publication ID", request.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedVersion < 1 || request.ExpectedGeneration < 1 || request.ExpectedState != store.PublicationPrepared && request.ExpectedState != store.PublicationVerifying {
		return nil, store.ValidationErrorf("PR intent requires an exact Prepared or Verifying publication version/generation")
	}
	if err := store.ValidatePullRequestIntent(request.Intent, request.ExpectedGeneration); err != nil {
		return nil, err
	}
	request.OperationID = strings.TrimSpace(request.OperationID)
	if err := store.ValidateControlIdentifier("PR intent operation ID", request.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("PR intent operation digest", request.OperationDigest); err != nil {
		return nil, err
	}
	request.UpdatedAt = store.NormalizeControlTime(request.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	publication, err := getPublication(ctx, tx, request.ID)
	if err != nil {
		return nil, err
	}
	if publication.PreparedReceipt == nil {
		return nil, store.ValidationErrorf("PR intent requires a durable prepared receipt")
	}
	if request.ExpectedState == store.PublicationPrepared && publication.PreparedReceipt.CommitSHA != request.Intent.ExpectedHeadSHA {
		return nil, store.ValidationErrorf("Prepared PR intent expected head must equal the durable prepared commit")
	}
	if publication.LastOperationID == request.OperationID {
		if publication.LastOperationDigest == request.OperationDigest && publication.PRIntent != nil && reflect.DeepEqual(*publication.PRIntent, request.Intent) {
			return &publication, nil
		}
		return nil, store.ConflictErrorf("PR intent operation %q was reused with different content", request.OperationID)
	}
	if publication.Version != request.ExpectedVersion || publication.Generation != request.ExpectedGeneration || publication.State != request.ExpectedState || publication.PRIntent != nil {
		return nil, store.ConflictErrorf("publication %q no longer matches PR intent version/generation/state", publication.ID)
	}
	intentJSON, err := marshalOptionalControlJSON(&request.Intent)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE publications SET pr_intent = ?, controller_epoch_name = ?, controller_epoch = ?,
		 last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND state = ? AND pr_intent IS NULL`,
		intentJSON, request.Fence.Name, request.Fence.Epoch, request.OperationID, request.OperationDigest,
		request.UpdatedAt, request.ID, request.ExpectedVersion, request.ExpectedGeneration, string(request.ExpectedState),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "publication PR intent"); err != nil {
		return nil, err
	}
	intent := request.Intent
	publication.PRIntent = &intent
	publication.ControllerEpochName = request.Fence.Name
	publication.ControllerEpoch = request.Fence.Epoch
	publication.LastOperationID = request.OperationID
	publication.LastOperationDigest = request.OperationDigest
	publication.Version++
	publication.UpdatedAt = request.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &publication, nil
}

// SetPublicationPRReceipt commits the exact forge result before the
// Publication leaves Verifying.
func (s *Store) SetPublicationPRReceipt(ctx context.Context, request store.SetPublicationPRReceiptRequest) (*store.Publication, error) {
	request.ID = strings.TrimSpace(request.ID)
	if err := store.ValidateControlIdentifier("publication ID", request.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedVersion < 1 || request.ExpectedGeneration < 1 || request.ExpectedState != store.PublicationVerifying {
		return nil, store.ValidationErrorf("PR receipt requires an exact Verifying publication version/generation")
	}
	request.OperationID = strings.TrimSpace(request.OperationID)
	if err := store.ValidateControlIdentifier("PR receipt operation ID", request.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("PR receipt operation digest", request.OperationDigest); err != nil {
		return nil, err
	}
	request.UpdatedAt = store.NormalizeControlTime(request.UpdatedAt)
	if err := store.ValidatePullRequestReceipt(request.Receipt); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	publication, err := getPublication(ctx, tx, request.ID)
	if err != nil {
		return nil, err
	}
	if publication.PRIntent == nil || publication.PRIntent.ExpectedHeadSHA != request.Receipt.HeadSHA {
		return nil, store.ValidationErrorf("PR receipt does not match the persisted intent head")
	}
	if publication.LastOperationID == request.OperationID {
		if publication.LastOperationDigest == request.OperationDigest && publication.PullRequestReceipt != nil && reflect.DeepEqual(*publication.PullRequestReceipt, request.Receipt) {
			return &publication, nil
		}
		return nil, store.ConflictErrorf("PR receipt operation %q was reused with different content", request.OperationID)
	}
	if publication.Version != request.ExpectedVersion || publication.Generation != request.ExpectedGeneration || publication.State != request.ExpectedState || publication.PullRequestReceipt != nil {
		return nil, store.ConflictErrorf("publication %q no longer matches PR receipt version/generation/state", publication.ID)
	}
	receiptJSON, err := marshalOptionalControlJSON(&request.Receipt)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE publications SET pr_receipt = ?, controller_epoch_name = ?, controller_epoch = ?,
		 last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND state = 'Verifying' AND pr_receipt IS NULL`,
		receiptJSON, request.Fence.Name, request.Fence.Epoch, request.OperationID, request.OperationDigest,
		request.UpdatedAt, request.ID, request.ExpectedVersion, request.ExpectedGeneration,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "publication PR receipt"); err != nil {
		return nil, err
	}
	receipt := request.Receipt
	publication.PullRequestReceipt = &receipt
	publication.ControllerEpochName = request.Fence.Name
	publication.ControllerEpoch = request.Fence.Epoch
	publication.LastOperationID = request.OperationID
	publication.LastOperationDigest = request.OperationDigest
	publication.Version++
	publication.UpdatedAt = request.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &publication, nil
}

// TransitionPublication applies a fenced generation/version/state CAS and
// validates that the persisted receipt exactly matches immutable intent.
//
//nolint:gocyclo // Receipt-specific validation and the publication CAS intentionally share one boundary.
func (s *Store) TransitionPublication(ctx context.Context, transition store.PublicationTransition) (*store.Publication, error) {
	transition.ID = strings.TrimSpace(transition.ID)
	if err := store.ValidateControlIdentifier("publication ID", transition.ID); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(transition.Fence)
	if err != nil {
		return nil, err
	}
	transition.Fence = fence
	if transition.ExpectedVersion < 1 || transition.ExpectedGeneration < 1 {
		return nil, store.ValidationErrorf("publication expected version and generation must be at least 1")
	}
	if err := store.ValidatePublicationTransition(transition.ExpectedState, transition.NewState); err != nil {
		return nil, err
	}
	transition.OperationID = strings.TrimSpace(transition.OperationID)
	if err := store.ValidateControlIdentifier("publication operation ID", transition.OperationID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("publication operation digest", transition.OperationDigest); err != nil {
		return nil, err
	}
	transition.TerminalReason = strings.TrimSpace(transition.TerminalReason)
	if err := store.ValidateControlReason("publication terminal reason", transition.TerminalReason); err != nil {
		return nil, err
	}
	transition.UpdatedAt = store.NormalizeControlTime(transition.UpdatedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, transition.Fence); err != nil {
		return nil, err
	}
	publication, err := getPublication(ctx, tx, transition.ID)
	if err != nil {
		return nil, err
	}
	if err := store.ValidatePublicationTransitionReceipts(publication, transition); err != nil {
		return nil, err
	}
	if publication.LastOperationID == transition.OperationID {
		if publication.LastOperationDigest != transition.OperationDigest {
			return nil, store.ConflictErrorf("publication operation %q was reused with a different digest", transition.OperationID)
		}
		if publication.State == transition.NewState && store.PublicationTransitionReceiptsMatch(publication, transition) {
			return &publication, nil
		}
		return nil, store.ConflictErrorf("publication operation %q was already applied with different target state or receipt", transition.OperationID)
	}
	if publication.Version != transition.ExpectedVersion || publication.Generation != transition.ExpectedGeneration || publication.State != transition.ExpectedState {
		return nil, store.ConflictErrorf("publication %q is version %d generation %d state %s, expected version %d generation %d state %s", publication.ID, publication.Version, publication.Generation, publication.State, transition.ExpectedVersion, transition.ExpectedGeneration, transition.ExpectedState)
	}

	prepared := publication.PreparedReceipt
	if transition.PreparedReceipt != nil {
		copyValue := *transition.PreparedReceipt
		prepared = &copyValue
	}
	publish := publication.PublishReceipt
	if transition.PublishReceipt != nil {
		copyValue := *transition.PublishReceipt
		publish = &copyValue
	}
	verification := publication.VerificationReceipt
	if transition.VerificationReceipt != nil {
		copyValue := *transition.VerificationReceipt
		verification = &copyValue
	}
	preparedJSON, err := marshalOptionalControlJSON(prepared)
	if err != nil {
		return nil, err
	}
	publishJSON, err := marshalOptionalControlJSON(publish)
	if err != nil {
		return nil, err
	}
	verificationJSON, err := marshalOptionalControlJSON(verification)
	if err != nil {
		return nil, err
	}
	terminalReason := publication.TerminalReason
	if transition.TerminalReason != "" || store.IsTerminalPublicationState(transition.NewState) {
		terminalReason = transition.TerminalReason
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE publications
		 SET state = ?, prepared_receipt = ?, publish_receipt = ?, verification_receipt = ?,
		     terminal_reason = ?, controller_epoch_name = ?, controller_epoch = ?, last_operation_id = ?,
		     last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND state = ?`,
		string(transition.NewState), preparedJSON, publishJSON, verificationJSON, terminalReason,
		transition.Fence.Name, transition.Fence.Epoch, transition.OperationID, transition.OperationDigest,
		transition.UpdatedAt, transition.ID, transition.ExpectedVersion, transition.ExpectedGeneration,
		string(transition.ExpectedState),
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "publication"); err != nil {
		return nil, err
	}
	publication.State = transition.NewState
	publication.PreparedReceipt = prepared
	publication.PublishReceipt = publish
	publication.VerificationReceipt = verification
	publication.TerminalReason = terminalReason
	publication.ControllerEpochName = transition.Fence.Name
	publication.ControllerEpoch = transition.Fence.Epoch
	publication.LastOperationID = transition.OperationID
	publication.LastOperationDigest = transition.OperationDigest
	publication.Version++
	publication.UpdatedAt = transition.UpdatedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &publication, nil
}

func publicationReceipt(publication store.Publication) store.PublicationReceipt {
	return store.PublicationReceipt{
		PublicationID: publication.ID,
		Generation:    publication.Generation,
		State:         publication.State,
		Prepared:      publication.PreparedReceipt,
		Publish:       publication.PublishReceipt,
		Verification:  publication.VerificationReceipt,
		PullRequest:   publication.PullRequestReceipt,
	}
}

func getPublication(ctx context.Context, q controlQueryRower, id string) (store.Publication, error) {
	var publication store.Publication
	var prIntentJSON, preparedJSON, publishJSON, verificationJSON, pullRequestJSON []byte
	err := q.QueryRowContext(ctx,
		`SELECT id, namespace, generation, task_uid, attempt, prompt_id, session_uid, branch_claim_id,
		        branch_claim_generation, source_repository_id, source_ref, source_baseline_sha, target_repository_id, target_ref,
		        baseline_absent, baseline_sha, artifact_id, artifact_digest, artifact_size_bytes, artifact_media_type,
		        publication_credential_ref, commit_identity,
		        commit_message, commit_timestamp, pr_intent, request_digest, state, prepared_receipt,
		        publish_receipt, verification_receipt, pr_receipt, terminal_reason, controller_epoch_name,
		        controller_epoch, last_operation_id, last_operation_digest, version, created_at, updated_at
		 FROM publications WHERE id = ?`, id,
	).Scan(
		&publication.ID, &publication.Namespace, &publication.Generation, &publication.TaskUID,
		&publication.Attempt, &publication.PromptID, &publication.SessionUID, &publication.BranchClaimID,
		&publication.BranchClaimGeneration, &publication.SourceRepositoryID, &publication.SourceRef, &publication.SourceBaselineSHA,
		&publication.TargetRepositoryID, &publication.TargetRef, &publication.Baseline.Absent,
		&publication.Baseline.SHA, &publication.ArtifactID, &publication.ArtifactDigest,
		&publication.ArtifactSizeBytes, &publication.ArtifactMediaType, &publication.PublicationCredentialRef,
		&publication.CommitIdentity, &publication.CommitMessage, &publication.CommitTimestamp,
		&prIntentJSON, &publication.RequestDigest, &publication.State, &preparedJSON, &publishJSON,
		&verificationJSON, &pullRequestJSON, &publication.TerminalReason, &publication.ControllerEpochName,
		&publication.ControllerEpoch, &publication.LastOperationID, &publication.LastOperationDigest,
		&publication.Version, &publication.CreatedAt, &publication.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Publication{}, store.ErrNotFound
	}
	if err != nil {
		return store.Publication{}, fmt.Errorf("get publication: %w", err)
	}
	if controlJSONPresent(prIntentJSON) {
		publication.PRIntent = &store.PullRequestIntent{}
		if err := unmarshalOptionalControlJSON(prIntentJSON, publication.PRIntent); err != nil {
			return store.Publication{}, fmt.Errorf("decode publication PR intent: %w", err)
		}
	}
	if controlJSONPresent(preparedJSON) {
		publication.PreparedReceipt = &store.PreparedPublicationReceipt{}
		if err := unmarshalOptionalControlJSON(preparedJSON, publication.PreparedReceipt); err != nil {
			return store.Publication{}, fmt.Errorf("decode prepared receipt: %w", err)
		}
	}
	if controlJSONPresent(publishJSON) {
		publication.PublishReceipt = &store.PublishOperationReceipt{}
		if err := unmarshalOptionalControlJSON(publishJSON, publication.PublishReceipt); err != nil {
			return store.Publication{}, fmt.Errorf("decode publish receipt: %w", err)
		}
	}
	if controlJSONPresent(verificationJSON) {
		publication.VerificationReceipt = &store.PublicationVerificationReceipt{}
		if err := unmarshalOptionalControlJSON(verificationJSON, publication.VerificationReceipt); err != nil {
			return store.Publication{}, fmt.Errorf("decode verification receipt: %w", err)
		}
	}
	if controlJSONPresent(pullRequestJSON) {
		publication.PullRequestReceipt = &store.PullRequestOperationReceipt{}
		if err := unmarshalOptionalControlJSON(pullRequestJSON, publication.PullRequestReceipt); err != nil {
			return store.Publication{}, fmt.Errorf("decode pull request receipt: %w", err)
		}
	}
	return publication, nil
}
