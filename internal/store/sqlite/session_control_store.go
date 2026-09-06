package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

var _ store.SessionControlStore = (*Store)(nil)

const (
	sessionTurnAggregateKind     = "SessionTurn"
	sessionTurnRoleAssistant     = "assistant"
	sessionTurnRoleOutcomeMarker = "system"
	sessionControlFieldNamespace = "session namespace"
	sessionControlFieldName      = "session name"
	sessionControlFieldUID       = "session UID"
)

// CreateSessionControl attaches an immutable SessionUID and fenced control
// metadata to an existing transcript session.
func (s *Store) CreateSessionControl(ctx context.Context, control *store.SessionControl, fence store.ControllerEpochFence) (*store.SessionControl, error) {
	normalized, fence, err := store.NormalizeSessionControlForCreate(control, fence)
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
	if existing, getErr := getSessionControl(ctx, tx, normalized.Namespace, normalized.SessionName); getErr == nil {
		if sameSessionControlCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("session control %s/%s was reused with a different UID or request digest", normalized.Namespace, normalized.SessionName)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}
	var exists int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sessions WHERE namespace = ? AND name = ?`,
		normalized.Namespace, normalized.SessionName,
	).Scan(&exists); err != nil {
		return nil, err
	}
	if exists != 1 {
		return nil, store.ErrNotFound
	}
	verifiedRepositoryID, verifiedRef, verifiedSHA := baselineColumns(normalized.VerifiedBaseline)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO session_controls(
			namespace, session_name, session_uid, request_digest, availability, lease_generation, lease_task_uid,
			lease_attempt, lease_prompt_id, lease_request_digest, lease_acquired_at, lease_expires_at,
			blocked_reason, related_prompt_attempt_id, related_publication_id, verified_repository_id,
			verified_ref, verified_sha, controller_epoch_name, controller_epoch, last_operation_id,
			last_operation_digest, version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, '', 0, '', '', NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, '', '', 1, ?, ?)`,
		normalized.Namespace, normalized.SessionName, normalized.SessionUID, normalized.RequestDigest,
		string(normalized.Availability), normalized.LeaseGeneration, normalized.BlockedReason, normalized.RelatedPromptAttemptID,
		normalized.RelatedPublicationID, verifiedRepositoryID, verifiedRef, verifiedSHA,
		normalized.ControllerEpochName, normalized.ControllerEpoch, normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, err
		}
		existing, getErr := getSessionControl(ctx, tx, normalized.Namespace, normalized.SessionName)
		if getErr == nil && sameSessionControlCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("session control %s/%s already exists with a different immutable UID or request digest", normalized.Namespace, normalized.SessionName)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &normalized, nil
}

// GetSessionControl returns control state for an existing transcript session.
func (s *Store) GetSessionControl(ctx context.Context, namespace, sessionName string) (*store.SessionControl, error) {
	namespace = strings.TrimSpace(namespace)
	sessionName = strings.TrimSpace(sessionName)
	if err := store.ValidateControlIdentifier("session namespace", namespace); err != nil {
		return nil, err
	}
	if err := store.ValidateControlIdentifier("session name", sessionName); err != nil {
		return nil, err
	}
	control, err := getSessionControl(ctx, s.db, namespace, sessionName)
	if err != nil {
		return nil, err
	}
	return &control, nil
}

// AcquireSessionMutationLease acquires the next monotonic lease generation and
// mirrors ownership into the legacy session active_task field in the same transaction.
func (s *Store) AcquireSessionMutationLease(ctx context.Context, request store.AcquireSessionMutationLeaseRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	request.PromptID = strings.TrimSpace(request.PromptID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace, sessionControlFieldName: request.SessionName,
		sessionControlFieldUID: request.SessionUID, "lease task UID": request.TaskUID,
		"lease prompt ID": request.PromptID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedVersion < 1 || request.ExpectedLeaseGeneration < 0 || request.Attempt < 1 {
		return nil, store.ValidationErrorf("session expected version and attempt must be at least 1 and lease generation must not be negative")
	}
	if err := store.ValidateCanonicalDigest("session lease request digest", request.RequestDigest); err != nil {
		return nil, err
	}
	request.AcquiredAt = store.NormalizeControlTime(request.AcquiredAt)
	request.ExpiresAt = store.NormalizeOptionalControlTime(request.ExpiresAt)
	if request.ExpiresAt != nil && !request.ExpiresAt.After(request.AcquiredAt) {
		return nil, store.ValidationErrorf("session lease expiry must be after acquisition")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	control, err := getSessionControl(ctx, tx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	if control.SessionUID != request.SessionUID {
		return nil, store.ConflictErrorf("session %s/%s UID does not match immutable control record", request.Namespace, request.SessionName)
	}
	if control.Lease != nil && control.Lease.TaskUID == request.TaskUID && control.Lease.Attempt == request.Attempt &&
		control.Lease.PromptID == request.PromptID {
		if control.Lease.RequestDigest == request.RequestDigest {
			return &control, nil
		}
		return nil, store.ConflictErrorf("session lease identity was reused with a different request digest")
	}
	if control.Version != request.ExpectedVersion || control.LeaseGeneration != request.ExpectedLeaseGeneration {
		return nil, store.ConflictErrorf("session %s/%s is version %d lease generation %d, expected version %d generation %d", request.Namespace, request.SessionName, control.Version, control.LeaseGeneration, request.ExpectedVersion, request.ExpectedLeaseGeneration)
	}
	if control.Availability != store.SessionAvailable {
		return nil, store.ConflictErrorf("session %s/%s is reconciliation-blocked: %s", request.Namespace, request.SessionName, control.BlockedReason)
	}
	if control.Lease != nil {
		return nil, store.ConflictErrorf("session %s/%s is already leased by task %s", request.Namespace, request.SessionName, control.Lease.TaskUID)
	}

	legacyResult, err := tx.ExecContext(ctx,
		`UPDATE sessions SET active_task = ?, updated_at = ?
		 WHERE namespace = ? AND name = ? AND (active_task = '' OR active_task = ?)`,
		request.TaskUID, request.AcquiredAt, request.Namespace, request.SessionName, request.TaskUID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(legacyResult, "session active-task lease"); err != nil {
		return nil, store.ConflictErrorf("legacy session lock prevented mutation lease acquisition")
	}
	newGeneration := control.LeaseGeneration + 1
	result, err := tx.ExecContext(ctx,
		`UPDATE session_controls
		 SET lease_generation = ?, lease_task_uid = ?, lease_attempt = ?, lease_prompt_id = ?,
		     lease_request_digest = ?, lease_acquired_at = ?, lease_expires_at = ?,
		     controller_epoch_name = ?, controller_epoch = ?, version = version + 1, updated_at = ?
		 WHERE namespace = ? AND session_name = ? AND session_uid = ? AND version = ?
		   AND lease_generation = ? AND availability = 'Available' AND lease_task_uid = ''`,
		newGeneration, request.TaskUID, request.Attempt, request.PromptID, request.RequestDigest,
		request.AcquiredAt, nullTimeValue(request.ExpiresAt), request.Fence.Name, request.Fence.Epoch,
		request.AcquiredAt, request.Namespace, request.SessionName, request.SessionUID,
		request.ExpectedVersion, request.ExpectedLeaseGeneration,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "session mutation lease"); err != nil {
		return nil, err
	}
	control.LeaseGeneration = newGeneration
	control.Lease = &store.SessionMutationLease{
		Generation: newGeneration, TaskUID: request.TaskUID, Attempt: request.Attempt,
		PromptID: request.PromptID, RequestDigest: request.RequestDigest,
		AcquiredAt: request.AcquiredAt, ExpiresAt: request.ExpiresAt,
	}
	control.ControllerEpochName = request.Fence.Name
	control.ControllerEpoch = request.Fence.Epoch
	control.Version++
	control.UpdatedAt = request.AcquiredAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &control, nil
}

// CommitSessionRuntimeGeneration records the newest provider RuntimeSession
// generation proven live under the exact active Session lease.
func (s *Store) CommitSessionRuntimeGeneration(ctx context.Context, request store.CommitSessionRuntimeGenerationRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace,
		sessionControlFieldName:      request.SessionName,
		sessionControlFieldUID:       request.SessionUID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	if err := request.Key.Validate(); err != nil {
		return nil, err
	}
	if request.Key.SessionUID != request.SessionUID || request.ExpectedSessionVersion < 1 || request.Generation < 1 {
		return nil, store.ValidationErrorf("Session RuntimeSession generation commit fence is invalid")
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	committedAt := store.NormalizeControlTime(request.CommittedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return nil, err
	}
	control, err := getSessionControl(ctx, tx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	if control.SessionUID != request.SessionUID || control.Availability != store.SessionAvailable ||
		!sessionLeaseMatches(control.Lease, request.Key) {
		return nil, store.ConflictErrorf("session %s/%s no longer matches the active RuntimeSession generation commit fence", request.Namespace, request.SessionName)
	}
	if control.RuntimeSessionGeneration > request.Generation {
		return nil, store.ConflictErrorf("session %s/%s RuntimeSession generation is %d, not %d", request.Namespace, request.SessionName, control.RuntimeSessionGeneration, request.Generation)
	}
	if control.RuntimeSessionGeneration == request.Generation {
		return &control, nil
	}
	if control.Version != request.ExpectedSessionVersion {
		return nil, store.ConflictErrorf("session %s/%s is version %d, expected %d", request.Namespace, request.SessionName, control.Version, request.ExpectedSessionVersion)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE session_controls
		 SET runtime_session_generation = ?, controller_epoch_name = ?, controller_epoch = ?,
		     version = version + 1, updated_at = ?
		 WHERE namespace = ? AND session_name = ? AND session_uid = ? AND version = ?
		   AND runtime_session_generation = ? AND availability = 'Available'
		   AND lease_generation = ? AND lease_task_uid = ? AND lease_attempt = ? AND lease_prompt_id = ?`,
		request.Generation, fence.Name, fence.Epoch, committedAt,
		request.Namespace, request.SessionName, request.SessionUID, request.ExpectedSessionVersion,
		control.RuntimeSessionGeneration, request.Key.LeaseGeneration, request.Key.TaskUID,
		request.Key.Attempt, request.Key.PromptID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "Session RuntimeSession generation commit"); err != nil {
		return nil, err
	}
	control.RuntimeSessionGeneration = request.Generation
	control.ControllerEpochName = fence.Name
	control.ControllerEpoch = fence.Epoch
	control.Version++
	control.UpdatedAt = committedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &control, nil
}

// ReleaseSessionMutationLease atomically aborts an exact pre-prompt lease in
// the legacy SQLite-authoritative store.
func (s *Store) ReleaseSessionMutationLease(ctx context.Context, request store.ReleaseSessionMutationLeaseRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace, sessionControlFieldName: request.SessionName,
		sessionControlFieldUID: request.SessionUID, "session lease release operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	if err := request.Key.Validate(); err != nil {
		return nil, err
	}
	turnID, err := request.Key.CanonicalID()
	if err != nil {
		return nil, err
	}
	if request.Key.SessionUID != request.SessionUID || request.ExpectedSessionVersion < 1 || request.OperationID != "release-pre-prompt:"+turnID {
		return nil, store.ValidationErrorf("session lease release identity/version is invalid")
	}
	if err := store.ValidateCanonicalDigest("session lease request digest", request.LeaseRequestDigest); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("session lease release operation digest", request.OperationDigest); err != nil {
		return nil, err
	}
	expectedDigest, err := store.SessionLeaseReleaseOperationDigest(turnID, request.LeaseRequestDigest)
	if err != nil {
		return nil, err
	}
	if request.OperationDigest != expectedDigest {
		return nil, store.ValidationErrorf("session lease release operation digest does not match its Lease request digest")
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	request.ReleasedAt = store.NormalizeControlTime(request.ReleasedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	control, err := getSessionControl(ctx, tx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	if control.SessionUID != request.SessionUID {
		return nil, store.ConflictErrorf("session %s/%s UID does not match immutable control record", request.Namespace, request.SessionName)
	}
	if control.LastOperationID == request.OperationID {
		if control.LastOperationDigest != request.OperationDigest || control.Lease != nil {
			return nil, store.ConflictErrorf("session lease release operation %q was already applied with different target values", request.OperationID)
		}
		return &control, nil
	}
	if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatches(control.Lease, request.Key) || control.Lease.RequestDigest != request.LeaseRequestDigest {
		return nil, store.ConflictErrorf("session %s/%s no longer matches the pre-prompt lease release fence", request.Namespace, request.SessionName)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE sessions SET active_task = '', updated_at = ? WHERE namespace = ? AND name = ? AND active_task = ?`,
		request.ReleasedAt, request.Namespace, request.SessionName, request.Key.TaskUID,
	); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE session_controls
		 SET lease_task_uid = '', lease_attempt = 0, lease_prompt_id = '', lease_request_digest = '',
		     lease_acquired_at = NULL, lease_expires_at = NULL, controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE namespace = ? AND session_name = ? AND session_uid = ? AND version = ?
		   AND lease_generation = ? AND lease_task_uid = ? AND lease_attempt = ? AND lease_prompt_id = ? AND lease_request_digest = ?`,
		request.Fence.Name, request.Fence.Epoch, request.OperationID, request.OperationDigest, request.ReleasedAt,
		request.Namespace, request.SessionName, request.SessionUID, request.ExpectedSessionVersion,
		request.Key.LeaseGeneration, request.Key.TaskUID, request.Key.Attempt, request.Key.PromptID, request.LeaseRequestDigest,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(result, "pre-prompt session lease release"); err != nil {
		return nil, err
	}
	control.Lease = nil
	control.ControllerEpochName = request.Fence.Name
	control.ControllerEpoch = request.Fence.Epoch
	control.LastOperationID = request.OperationID
	control.LastOperationDigest = request.OperationDigest
	control.Version++
	control.UpdatedAt = request.ReleasedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &control, nil
}

// ReconcileSessionControl atomically establishes an independently observed
// branch baseline and returns a reconciliation-blocked Session and BranchClaim
// to Available using exact session/claim version and generation fences.
//
//nolint:gocyclo // Session and branch recovery are one explicit cross-aggregate CAS transaction.
func (s *Store) ReconcileSessionControl(ctx context.Context, request store.ReconcileSessionControlRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionUID = strings.TrimSpace(request.SessionUID)
	request.ExpectedRelatedPublicationID = strings.TrimSpace(request.ExpectedRelatedPublicationID)
	request.BranchClaimID = strings.TrimSpace(request.BranchClaimID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	for field, value := range map[string]string{
		sessionControlFieldNamespace: request.Namespace, sessionControlFieldName: request.SessionName,
		sessionControlFieldUID: request.SessionUID, "related publication ID": request.ExpectedRelatedPublicationID,
		"branch claim ID": request.BranchClaimID, "session reconciliation operation ID": request.OperationID,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedVersion < 1 || request.ExpectedLeaseGeneration < 1 ||
		request.ExpectedBranchClaimVersion < 1 || request.ExpectedBranchClaimGeneration < 1 {
		return nil, store.ValidationErrorf("session/branch reconciliation versions and generations must be at least 1")
	}
	if err := request.ExpectedBranchBaseline.Validate("expected reconciliation branch baseline"); err != nil {
		return nil, err
	}
	request.VerifiedBaseline.RepositoryID = strings.TrimSpace(request.VerifiedBaseline.RepositoryID)
	request.VerifiedBaseline.Ref = strings.TrimSpace(request.VerifiedBaseline.Ref)
	request.VerifiedBaseline.SHA = strings.TrimSpace(request.VerifiedBaseline.SHA)
	if err := store.ValidateVerifiedBaseline(request.VerifiedBaseline); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("session reconciliation operation digest", request.OperationDigest); err != nil {
		return nil, err
	}
	request.ReconciledAt = store.NormalizeControlTime(request.ReconciledAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	control, err := getSessionControl(ctx, tx, request.Namespace, request.SessionName)
	if err != nil {
		return nil, err
	}
	if control.LastOperationID == request.OperationID {
		if control.LastOperationDigest != request.OperationDigest {
			return nil, store.ConflictErrorf("session reconciliation operation %q was reused with a different digest", request.OperationID)
		}
		if control.Availability == store.SessionAvailable && control.Lease == nil &&
			control.VerifiedBaseline != nil && reflect.DeepEqual(*control.VerifiedBaseline, request.VerifiedBaseline) {
			return &control, nil
		}
		return nil, store.ConflictErrorf("session reconciliation operation %q was already applied with different target values", request.OperationID)
	}
	if control.SessionUID != request.SessionUID || control.Version != request.ExpectedVersion ||
		control.LeaseGeneration != request.ExpectedLeaseGeneration || control.Lease != nil ||
		control.Availability != store.SessionReconciliationBlocked ||
		control.RelatedPublicationID != request.ExpectedRelatedPublicationID {
		return nil, store.ConflictErrorf("session %s/%s no longer matches the blocked reconciliation fence", request.Namespace, request.SessionName)
	}
	claim, err := getBranchClaim(ctx, tx, request.BranchClaimID)
	if err != nil {
		return nil, err
	}
	if claim.Version != request.ExpectedBranchClaimVersion || claim.Generation != request.ExpectedBranchClaimGeneration ||
		!claim.LastVerified.Equal(request.ExpectedBranchBaseline) ||
		claim.Availability != store.BranchClaimReconciliationBlocked ||
		claim.RelatedPublicationID != request.ExpectedRelatedPublicationID ||
		claim.RepositoryID != request.VerifiedBaseline.RepositoryID || claim.Ref != request.VerifiedBaseline.Ref {
		return nil, store.ConflictErrorf("branch claim %q no longer matches the blocked reconciliation fence", claim.ID)
	}
	branchResult, err := tx.ExecContext(ctx,
		`UPDATE branch_claims
		 SET last_verified_absent = FALSE, last_verified_sha = ?, availability = 'Available',
		     blocked_reason = '', related_publication_id = '', controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND last_verified_absent = ?
		   AND last_verified_sha = ? AND availability = 'ReconciliationBlocked'
		   AND related_publication_id = ?`,
		request.VerifiedBaseline.SHA, request.Fence.Name, request.Fence.Epoch,
		request.OperationID, request.OperationDigest, request.ReconciledAt, claim.ID,
		request.ExpectedBranchClaimVersion, request.ExpectedBranchClaimGeneration,
		request.ExpectedBranchBaseline.Absent, request.ExpectedBranchBaseline.SHA,
		request.ExpectedRelatedPublicationID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(branchResult, "branch reconciliation"); err != nil {
		return nil, err
	}
	controlResult, err := tx.ExecContext(ctx,
		`UPDATE session_controls
		 SET availability = 'Available', blocked_reason = '', related_prompt_attempt_id = '',
		     related_publication_id = '', verified_repository_id = ?, verified_ref = ?, verified_sha = ?,
		     controller_epoch_name = ?, controller_epoch = ?, last_operation_id = ?,
		     last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE namespace = ? AND session_name = ? AND session_uid = ? AND version = ?
		   AND lease_generation = ? AND lease_task_uid = '' AND availability = 'ReconciliationBlocked'
		   AND related_publication_id = ?`,
		request.VerifiedBaseline.RepositoryID, request.VerifiedBaseline.Ref, request.VerifiedBaseline.SHA,
		request.Fence.Name, request.Fence.Epoch, request.OperationID, request.OperationDigest,
		request.ReconciledAt, request.Namespace, request.SessionName, request.SessionUID,
		request.ExpectedVersion, request.ExpectedLeaseGeneration, request.ExpectedRelatedPublicationID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(controlResult, "session reconciliation"); err != nil {
		return nil, err
	}
	control.Availability = store.SessionAvailable
	control.BlockedReason = ""
	control.RelatedPromptAttemptID = ""
	control.RelatedPublicationID = ""
	baseline := request.VerifiedBaseline
	control.VerifiedBaseline = &baseline
	control.ControllerEpochName = request.Fence.Name
	control.ControllerEpoch = request.Fence.Epoch
	control.LastOperationID = request.OperationID
	control.LastOperationDigest = request.OperationDigest
	control.Version++
	control.UpdatedAt = request.ReconciledAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &control, nil
}

// CreateSessionTurn opens a durable turn only under its exact active lease.
func (s *Store) CreateSessionTurn(ctx context.Context, request store.CreateSessionTurnRequest) (*store.SessionTurn, error) {
	normalized, fence, err := normalizeSessionTurnForCreate(request.Turn, request.Fence)
	if err != nil {
		return nil, err
	}
	if request.ExpectedSessionVersion < 1 {
		return nil, store.ValidationErrorf("expected session version must be at least 1")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, fence); err != nil {
		return nil, err
	}
	if existing, getErr := getSessionTurn(ctx, tx, normalized.ID); getErr == nil {
		if sameSessionTurnCreation(existing, normalized) {
			return &existing, nil
		}
		return nil, store.ConflictErrorf("session turn %q was reused with different prompt input or request digest", normalized.ID)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}
	control, err := getSessionControlByUID(ctx, tx, normalized.Key.SessionUID)
	if err != nil {
		return nil, err
	}
	if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatches(control.Lease, normalized.Key) {
		return nil, store.ConflictErrorf("session turn %q does not match the active session lease/version", normalized.ID)
	}
	attempt, err := getPromptAttempt(ctx, tx, normalized.PromptAttemptID)
	if err != nil {
		return nil, fmt.Errorf("session turn prompt attempt: %w", err)
	}
	if attempt.Key.TaskUID != normalized.Key.TaskUID || attempt.Key.Attempt != normalized.Key.Attempt ||
		attempt.Key.PromptID != normalized.Key.PromptID || attempt.SessionUID != normalized.Key.SessionUID ||
		attempt.SessionLeaseGeneration != normalized.Key.LeaseGeneration {
		return nil, store.ConflictErrorf("session turn prompt attempt does not match the fenced session turn identity")
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO session_turns(
			id, namespace, session_name, session_uid, lease_generation, task_uid, attempt, prompt_id, prompt_attempt_id,
			request_digest, user_prompt, state, terminal_kind, terminal_content, finalization_digest,
			publication_id, publication_receipt, controller_epoch_name, controller_epoch,
			version, created_at, finalized_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Open', '', '', '', '', NULL, ?, ?, 1, ?, NULL, ?)`,
		normalized.ID, control.Namespace, control.SessionName, normalized.Key.SessionUID, normalized.Key.LeaseGeneration, normalized.Key.TaskUID,
		normalized.Key.Attempt, normalized.Key.PromptID, normalized.PromptAttemptID, normalized.RequestDigest,
		normalized.UserPrompt, normalized.ControllerEpochName, normalized.ControllerEpoch,
		normalized.CreatedAt, normalized.UpdatedAt,
	)
	if err != nil {
		if isSQLiteConstraintError(err) {
			return nil, store.ConflictErrorf("session turn %q was created concurrently", normalized.ID)
		}
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &normalized, nil
}

// GetSessionTurn returns a durable session turn.
func (s *Store) GetSessionTurn(ctx context.Context, id string) (*store.SessionTurn, error) {
	id = strings.TrimSpace(id)
	if err := store.ValidateControlIdentifier("session turn ID", id); err != nil {
		return nil, err
	}
	turn, err := getSessionTurn(ctx, s.db, id)
	if err != nil {
		return nil, err
	}
	return &turn, nil
}

// FinalizeSessionTurn is the atomic cross-store completion barrier. It optionally
// appends the canonical user/terminal transcript entries, snapshots the publication
// receipt, advances or blocks branch/session baselines, releases only the matching
// lease, and enqueues the terminal projection before commit.
//
//nolint:gocyclo // This is the deliberate atomic cross-store completion transaction.
func (s *Store) FinalizeSessionTurn(ctx context.Context, request store.FinalizeSessionTurnRequest) (*store.SessionTurn, error) {
	if err := request.Key.Validate(); err != nil {
		return nil, err
	}
	turnID, err := request.Key.CanonicalID()
	if err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return nil, err
	}
	request.Fence = fence
	if request.ExpectedSessionVersion < 1 || request.ExpectedTurnVersion < 1 {
		return nil, store.ValidationErrorf("expected session and turn versions must be at least 1")
	}
	if err := store.ValidateCanonicalDigest("session turn finalization digest", request.FinalizationDigest); err != nil {
		return nil, err
	}
	if request.TerminalKind != store.SessionTurnAssistantResult && request.TerminalKind != store.SessionTurnOutcomeMarker {
		return nil, store.ValidationErrorf("unsupported session turn terminal kind %q", request.TerminalKind)
	}
	if request.TerminalKind == store.SessionTurnOutcomeMarker && strings.TrimSpace(request.TerminalContent) == "" {
		return nil, store.ValidationErrorf("outcome-marker finalization requires explicit marker content")
	}
	if request.SkipTranscriptAppend && request.SkipUserPromptAppend {
		return nil, store.ValidationErrorf("session turn cannot combine full transcript suppression with user-prompt-only suppression")
	}
	if err := store.ValidateControlText("session turn terminal content", request.TerminalContent); err != nil {
		return nil, err
	}
	request.PublicationID = strings.TrimSpace(request.PublicationID)
	if request.PublicationID != "" {
		if err := store.ValidateControlIdentifier("publication ID", request.PublicationID); err != nil {
			return nil, err
		}
	}
	request.BlockReason = strings.TrimSpace(request.BlockReason)
	if err := store.ValidateControlReason("session block reason", request.BlockReason); err != nil {
		return nil, err
	}
	if request.VerifiedBaseline != nil {
		copyBaseline := *request.VerifiedBaseline
		request.VerifiedBaseline = &copyBaseline
		if err := store.ValidateVerifiedBaseline(copyBaseline); err != nil {
			return nil, err
		}
	}
	request.FinalizedAt = store.NormalizeControlTime(request.FinalizedAt)
	projectionScheduleExplicit := !request.Projection.AvailableAt.IsZero()
	projection, _, err := normalizeOutboxProjectionForCreate(request.Projection, request.Fence, request.FinalizedAt)
	if err != nil {
		return nil, err
	}
	if projection.AggregateKind != sessionTurnAggregateKind || projection.AggregateID != turnID {
		return nil, store.ValidationErrorf("finalization projection must target SessionTurn aggregate %q", turnID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireControllerEpoch(ctx, tx, request.Fence); err != nil {
		return nil, err
	}
	turn, err := getSessionTurn(ctx, tx, turnID)
	if err != nil {
		return nil, err
	}
	if turn.State == store.SessionTurnFinalized {
		if turn.FinalizationDigest == request.FinalizationDigest && turn.TerminalKind == request.TerminalKind &&
			turn.TerminalContent == request.TerminalContent && turn.PublicationID == request.PublicationID {
			return &turn, nil
		}
		return nil, store.ConflictErrorf("session turn %q was finalized with different terminal data or digest", turn.ID)
	}
	if turn.Version != request.ExpectedTurnVersion {
		return nil, store.ConflictErrorf("session turn %q is version %d, expected %d", turn.ID, turn.Version, request.ExpectedTurnVersion)
	}
	control, err := getSessionControlByUID(ctx, tx, request.Key.SessionUID)
	if err != nil {
		return nil, err
	}
	if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatches(control.Lease, request.Key) {
		return nil, store.ConflictErrorf("session turn %q finalization is fenced by a different session version or lease", turn.ID)
	}
	attempt, err := getPromptAttempt(ctx, tx, turn.PromptAttemptID)
	if err != nil {
		return nil, fmt.Errorf("finalize prompt attempt: %w", err)
	}
	if err := validatePromptAttemptForFinalization(attempt, request); err != nil {
		return nil, err
	}

	var receipt *store.PublicationReceipt
	var publication *store.Publication
	verifiedBaseline := request.VerifiedBaseline
	if verifiedBaseline == nil && control.VerifiedBaseline != nil {
		copyBaseline := *control.VerifiedBaseline
		verifiedBaseline = &copyBaseline
	}
	blockReason := request.BlockReason
	if request.PublicationID != "" {
		value, getErr := getPublication(ctx, tx, request.PublicationID)
		if getErr != nil {
			return nil, fmt.Errorf("finalize publication: %w", getErr)
		}
		publication = &value
		if value.TaskUID != request.Key.TaskUID || value.Attempt != request.Key.Attempt || value.PromptID != request.Key.PromptID ||
			(value.SessionUID != "" && value.SessionUID != request.Key.SessionUID) || !store.IsTerminalPublicationState(value.State) {
			return nil, store.ConflictErrorf("publication %q does not match the terminal session turn identity/state", value.ID)
		}
		expectedDelivery, mapErr := promptDeliveryStateForPublication(value.State)
		if mapErr != nil {
			return nil, mapErr
		}
		if attempt.DeliveryState != expectedDelivery {
			return nil, store.ConflictErrorf("prompt attempt delivery state %s does not match publication state %s", attempt.DeliveryState, value.State)
		}
		receiptValue := publicationReceipt(value)
		receipt = &receiptValue
		derivedBaseline, unresolved, deriveErr := finalizationPublicationBaseline(value)
		if deriveErr != nil {
			return nil, deriveErr
		}
		if derivedBaseline != nil {
			if verifiedBaseline != nil && !reflect.DeepEqual(*verifiedBaseline, *derivedBaseline) {
				return nil, store.ConflictErrorf("requested verified baseline does not match independent publication receipt")
			}
			verifiedBaseline = derivedBaseline
		}
		if unresolved && blockReason == "" {
			return nil, store.ValidationErrorf("unresolved publication outcome requires a durable session block reason")
		}
	}
	if request.PublicationID == "" && verifiedBaseline != nil {
		return nil, store.ValidationErrorf("verified baseline advancement requires a publication receipt")
	}

	availability := store.SessionAvailable
	if blockReason != "" {
		availability = store.SessionReconciliationBlocked
	}
	if publication != nil {
		if err := finalizeBranchClaimTx(ctx, tx, *publication, verifiedBaseline, availability, blockReason, request.FinalizationDigest, request.Fence, request.FinalizedAt); err != nil {
			return nil, err
		}
	}
	receiptJSON, err := marshalOptionalControlJSON(receipt)
	if err != nil {
		return nil, err
	}
	messageCountDelta := 0
	if !request.SkipTranscriptAppend {
		terminalRole := sessionTurnRoleAssistant
		if request.TerminalKind == store.SessionTurnOutcomeMarker {
			terminalRole = sessionTurnRoleOutcomeMarker
		}
		messageCountDelta, err = appendSessionTurnTranscript(
			ctx, tx, control.Namespace, control.SessionName, turn.UserPrompt,
			terminalRole, request.TerminalContent, request.FinalizedAt, request.SkipUserPromptAppend,
		)
		if err != nil {
			return nil, err
		}
	}
	legacyResult, err := tx.ExecContext(ctx,
		`UPDATE sessions
		 SET message_count = message_count + ?, active_task = '', updated_at = ?
		 WHERE namespace = ? AND name = ? AND active_task = ?`,
		messageCountDelta, request.FinalizedAt, control.Namespace, control.SessionName, request.Key.TaskUID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(legacyResult, "session transcript finalization"); err != nil {
		return nil, err
	}

	verifiedRepositoryID, verifiedRef, verifiedSHA := baselineColumns(verifiedBaseline)
	relatedPromptAttemptID := ""
	relatedPublicationID := ""
	if availability == store.SessionReconciliationBlocked {
		relatedPromptAttemptID = turn.PromptAttemptID
		relatedPublicationID = request.PublicationID
	}
	controlResult, err := tx.ExecContext(ctx,
		`UPDATE session_controls
		 SET availability = ?, lease_task_uid = '', lease_attempt = 0, lease_prompt_id = '',
		     lease_request_digest = '', lease_acquired_at = NULL, lease_expires_at = NULL,
		     blocked_reason = ?, related_prompt_attempt_id = ?, related_publication_id = ?,
		     verified_repository_id = ?, verified_ref = ?, verified_sha = ?,
		     controller_epoch_name = ?, controller_epoch = ?, last_operation_id = ?,
		     last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE namespace = ? AND session_name = ? AND session_uid = ? AND version = ?
		   AND lease_generation = ? AND lease_task_uid = ? AND lease_attempt = ? AND lease_prompt_id = ?`,
		string(availability), blockReason, relatedPromptAttemptID, relatedPublicationID,
		verifiedRepositoryID, verifiedRef, verifiedSHA, request.Fence.Name, request.Fence.Epoch,
		"finalize:"+turn.ID, request.FinalizationDigest, request.FinalizedAt,
		control.Namespace, control.SessionName, control.SessionUID,
		request.ExpectedSessionVersion, request.Key.LeaseGeneration, request.Key.TaskUID,
		request.Key.Attempt, request.Key.PromptID,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(controlResult, "session finalization"); err != nil {
		return nil, err
	}
	turnResult, err := tx.ExecContext(ctx,
		`UPDATE session_turns
		 SET state = 'Finalized', terminal_kind = ?, terminal_content = ?, finalization_digest = ?,
		     publication_id = ?, publication_receipt = ?, projection_id = ?, projection_kind = ?,
		     projection_digest = ?, projection_available_at = ?, controller_epoch_name = ?, controller_epoch = ?,
		     version = version + 1, finalized_at = ?, updated_at = ?
		 WHERE id = ? AND version = ? AND state = 'Open'`,
		string(request.TerminalKind), request.TerminalContent, request.FinalizationDigest,
		request.PublicationID, receiptJSON, projection.ID, projection.ProjectionKind, projection.PayloadDigest,
		projection.InitialAvailableAt, request.Fence.Name, request.Fence.Epoch,
		request.FinalizedAt, request.FinalizedAt, turn.ID, request.ExpectedTurnVersion,
	)
	if err != nil {
		return nil, err
	}
	if err := rowsAffectedExactlyOne(turnResult, "session turn finalization"); err != nil {
		return nil, err
	}
	if _, err := enqueueOutboxProjectionTx(ctx, tx, projection, projectionScheduleExplicit); err != nil {
		return nil, err
	}

	turn.State = store.SessionTurnFinalized
	turn.TerminalKind = request.TerminalKind
	turn.TerminalContent = request.TerminalContent
	turn.FinalizationDigest = request.FinalizationDigest
	turn.PublicationID = request.PublicationID
	turn.PublicationReceipt = receipt
	turn.ProjectionID = projection.ID
	turn.ProjectionKind = projection.ProjectionKind
	turn.ProjectionDigest = projection.PayloadDigest
	turn.ProjectionAvailableAt = projection.InitialAvailableAt
	turn.ControllerEpochName = request.Fence.Name
	turn.ControllerEpoch = request.Fence.Epoch
	turn.Version++
	turn.FinalizedAt = &request.FinalizedAt
	turn.UpdatedAt = request.FinalizedAt
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &turn, nil
}

// ResumeSessionTurnFinalization is a compatibility no-op for the legacy
// SQLite-authoritative store because its transcript, control, lease, and outbox
// finalization commit in one transaction. It still validates the current epoch
// and exact persisted finalization identity so recovery cannot accept a
// different turn or digest.
func (s *Store) ResumeSessionTurnFinalization(ctx context.Context, request store.ResumeSessionTurnFinalizationRequest) (*store.SessionTurn, error) {
	if err := request.Key.Validate(); err != nil {
		return nil, err
	}
	turnID, err := request.Key.CanonicalID()
	if err != nil {
		return nil, err
	}
	request.PromptAttemptID = strings.TrimSpace(request.PromptAttemptID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", request.PromptAttemptID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("session turn finalization digest", request.FinalizationDigest); err != nil {
		return nil, err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
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
	turn, err := getSessionTurn(ctx, tx, turnID)
	if err != nil {
		return nil, err
	}
	if turn.State != store.SessionTurnFinalized || turn.Key != request.Key ||
		turn.PromptAttemptID != request.PromptAttemptID || turn.FinalizationDigest != request.FinalizationDigest {
		return nil, store.ConflictErrorf("session turn %q does not match the persisted finalization recovery identity", turnID)
	}
	return &turn, nil
}

func normalizeSessionTurnForCreate(turn store.SessionTurn, fence store.ControllerEpochFence) (store.SessionTurn, store.ControllerEpochFence, error) {
	turn.Key.SessionUID = strings.TrimSpace(turn.Key.SessionUID)
	turn.Key.TaskUID = strings.TrimSpace(turn.Key.TaskUID)
	turn.Key.PromptID = strings.TrimSpace(turn.Key.PromptID)
	if err := turn.Key.Validate(); err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	canonicalID, err := turn.Key.CanonicalID()
	if err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	turn.ID = strings.TrimSpace(turn.ID)
	if turn.ID == "" {
		turn.ID = canonicalID
	}
	if turn.ID != canonicalID {
		return store.SessionTurn{}, store.ControllerEpochFence{}, store.ValidationErrorf("session turn ID must equal canonical ID %q", canonicalID)
	}
	turn.PromptAttemptID = strings.TrimSpace(turn.PromptAttemptID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", turn.PromptAttemptID); err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	if err := store.ValidateCanonicalDigest("session turn request digest", turn.RequestDigest); err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	if strings.TrimSpace(turn.UserPrompt) == "" {
		return store.SessionTurn{}, store.ControllerEpochFence{}, store.ValidationErrorf("session turn user prompt is required")
	}
	if err := store.ValidateControlText("session turn user prompt", turn.UserPrompt); err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	if turn.State == "" {
		turn.State = store.SessionTurnOpen
	}
	if turn.State != store.SessionTurnOpen || turn.TerminalKind != "" || turn.TerminalContent != "" ||
		turn.FinalizationDigest != "" || turn.PublicationID != "" || turn.PublicationReceipt != nil || turn.FinalizedAt != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, store.ValidationErrorf("new session turn must be open and must not contain finalization data")
	}
	fence, err = store.NormalizeEpochFence(fence)
	if err != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, err
	}
	if turn.Version != 0 && turn.Version != 1 {
		return store.SessionTurn{}, store.ControllerEpochFence{}, store.ValidationErrorf("new session turn version must be zero or one")
	}
	now := store.NormalizeControlTime(turn.CreatedAt)
	turn.CreatedAt = now
	if turn.UpdatedAt.IsZero() {
		turn.UpdatedAt = now
	} else {
		turn.UpdatedAt = turn.UpdatedAt.UTC()
	}
	turn.ControllerEpochName = fence.Name
	turn.ControllerEpoch = fence.Epoch
	turn.Version = 1
	return turn, fence, nil
}

func baselineColumns(baseline *store.VerifiedBranchBaseline) (string, string, string) {
	if baseline == nil {
		return "", "", ""
	}
	return baseline.RepositoryID, baseline.Ref, baseline.SHA
}

func sameSessionControlCreation(a, b store.SessionControl) bool {
	return a.Namespace == b.Namespace && a.SessionName == b.SessionName && a.SessionUID == b.SessionUID &&
		a.RequestDigest == b.RequestDigest
}

func sameSessionTurnCreation(a, b store.SessionTurn) bool {
	return a.ID == b.ID && a.Key == b.Key && a.PromptAttemptID == b.PromptAttemptID &&
		a.RequestDigest == b.RequestDigest && a.UserPrompt == b.UserPrompt
}

func sessionLeaseMatches(lease *store.SessionMutationLease, key store.SessionTurnKey) bool {
	return lease != nil && lease.Generation == key.LeaseGeneration && lease.TaskUID == key.TaskUID &&
		lease.Attempt == key.Attempt && lease.PromptID == key.PromptID
}

func validatePromptAttemptForFinalization(attempt store.PromptAttempt, request store.FinalizeSessionTurnRequest) error {
	if attempt.Key.TaskUID != request.Key.TaskUID || attempt.Key.Attempt != request.Key.Attempt ||
		attempt.Key.PromptID != request.Key.PromptID || attempt.SessionUID != request.Key.SessionUID ||
		attempt.SessionLeaseGeneration != request.Key.LeaseGeneration {
		return store.ConflictErrorf("prompt attempt %q does not match the fenced session turn identity", attempt.ID)
	}
	if !store.IsTerminalPromptExecutionState(attempt.ExecutionState) {
		return store.ConflictErrorf("prompt attempt %q execution is not terminal: %s", attempt.ID, attempt.ExecutionState)
	}
	if !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return store.ConflictErrorf("prompt attempt %q delivery is not terminal: %s", attempt.ID, attempt.DeliveryState)
	}
	if request.TerminalKind == store.SessionTurnAssistantResult && attempt.ExecutionState != store.PromptExecutionSucceeded {
		return store.ValidationErrorf("assistant-result finalization requires succeeded prompt execution, got %s", attempt.ExecutionState)
	}
	if request.PublicationID == "" {
		switch attempt.DeliveryState {
		case store.PromptDeliveryNotRequested, store.PromptDeliveryReadValidated, store.PromptDeliveryNoChange,
			store.PromptDeliveryReadOnlyWorkspaceModified, store.PromptDeliveryCredentialBlocked, store.PromptDeliveryConflict:
			return nil
		default:
			return store.ConflictErrorf("prompt attempt delivery state %s requires a matching publication receipt", attempt.DeliveryState)
		}
	}
	return nil
}

func promptDeliveryStateForPublication(state store.PublicationState) (store.PromptDeliveryState, error) {
	switch state {
	case store.PublicationVerifiedExact:
		return store.PromptDeliveryVerifiedExact, nil
	case store.PublicationDeliveredSuperseded:
		return store.PromptDeliveryDeliveredSuperseded, nil
	case store.PublicationCancelledBeforePublish:
		return store.PromptDeliveryCancelledBeforePublish, nil
	case store.PublicationDeliveryConflict, store.PublicationPreparationFailed:
		return store.PromptDeliveryConflict, nil
	case store.PublicationCredentialBlocked:
		return store.PromptDeliveryCredentialBlocked, nil
	case store.PublicationOutcomeUnknown:
		return store.PromptDeliveryPublicationOutcomeUnknown, nil
	default:
		return "", store.ValidationErrorf("publication state %s cannot finalize a session turn", state)
	}
}

func finalizationPublicationBaseline(publication store.Publication) (*store.VerifiedBranchBaseline, bool, error) {
	switch publication.State {
	case store.PublicationVerifiedExact:
		if publication.PreparedReceipt == nil || publication.VerificationReceipt == nil {
			return nil, false, store.ValidationErrorf("verified publication lacks exact receipts")
		}
		return &store.VerifiedBranchBaseline{RepositoryID: publication.TargetRepositoryID, Ref: publication.TargetRef, SHA: publication.PreparedReceipt.CommitSHA}, false, nil
	case store.PublicationDeliveredSuperseded:
		if publication.VerificationReceipt == nil || publication.VerificationReceipt.ObservedRemote.Absent || publication.VerificationReceipt.ObservedRemote.SHA == "" {
			return nil, false, store.ValidationErrorf("superseded publication lacks observed descendant receipt")
		}
		return &store.VerifiedBranchBaseline{RepositoryID: publication.TargetRepositoryID, Ref: publication.TargetRef, SHA: publication.VerificationReceipt.ObservedRemote.SHA}, false, nil
	case store.PublicationOutcomeUnknown:
		return nil, true, nil
	case store.PublicationDeliveryConflict:
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

func finalizeBranchClaimTx(ctx context.Context, tx *sql.Tx, publication store.Publication, baseline *store.VerifiedBranchBaseline, availability store.SessionAvailability, blockReason, operationDigest string, fence store.ControllerEpochFence, updatedAt time.Time) error {
	claim, err := getBranchClaim(ctx, tx, publication.BranchClaimID)
	if err != nil {
		return err
	}
	if claim.Generation != publication.BranchClaimGeneration || claim.RepositoryID != publication.TargetRepositoryID ||
		claim.Ref != publication.TargetRef || !claim.LastVerified.Equal(publication.Baseline) ||
		claim.Availability != store.BranchClaimAvailable {
		return store.ConflictErrorf("branch claim %q no longer matches publication finalization baseline/generation", claim.ID)
	}
	newRemote := claim.LastVerified
	claimAvailability := store.BranchClaimAvailable
	relatedPublicationID := ""
	if baseline != nil {
		newRemote = store.RemoteRefState{SHA: baseline.SHA}
	}
	if availability == store.SessionReconciliationBlocked {
		claimAvailability = store.BranchClaimReconciliationBlocked
		relatedPublicationID = publication.ID
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE branch_claims
		 SET last_verified_absent = ?, last_verified_sha = ?, availability = ?, blocked_reason = ?,
		     related_publication_id = ?, controller_epoch_name = ?, controller_epoch = ?,
		     last_operation_id = ?, last_operation_digest = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND version = ? AND generation = ? AND last_verified_absent = ?
		   AND last_verified_sha = ? AND availability = 'Available'`,
		newRemote.Absent, newRemote.SHA, string(claimAvailability), blockReason, relatedPublicationID,
		fence.Name, fence.Epoch, "finalize:"+publication.ID, operationDigest, updatedAt,
		claim.ID, claim.Version, claim.Generation, claim.LastVerified.Absent, claim.LastVerified.SHA,
	)
	if err != nil {
		return err
	}
	return rowsAffectedExactlyOne(result, "branch claim finalization")
}

func getSessionControl(ctx context.Context, q controlQueryRower, namespace, sessionName string) (store.SessionControl, error) {
	return scanSessionControl(q.QueryRowContext(ctx, sessionControlSelectSQL()+` WHERE namespace = ? AND session_name = ?`, namespace, sessionName))
}

func getSessionControlByUID(ctx context.Context, q controlQueryRower, sessionUID string) (store.SessionControl, error) {
	return scanSessionControl(q.QueryRowContext(ctx, sessionControlSelectSQL()+` WHERE session_uid = ?`, sessionUID))
}

func sessionControlSelectSQL() string {
	return `SELECT namespace, session_name, session_uid, request_digest, availability, runtime_session_generation, lease_generation, lease_task_uid,
	        lease_attempt, lease_prompt_id, lease_request_digest, lease_acquired_at, lease_expires_at,
	        blocked_reason, related_prompt_attempt_id, related_publication_id, verified_repository_id,
	        verified_ref, verified_sha, controller_epoch_name, controller_epoch, last_operation_id,
	        last_operation_digest, version, created_at, updated_at
	 FROM session_controls`
}

type controlScanner interface {
	Scan(dest ...any) error
}

func scanSessionControl(scanner controlScanner) (store.SessionControl, error) {
	var control store.SessionControl
	var leaseTaskUID, leasePromptID, leaseRequestDigest string
	var leaseAttempt int64
	var leaseAcquired, leaseExpires sql.NullTime
	var verifiedRepositoryID, verifiedRef, verifiedSHA string
	err := scanner.Scan(
		&control.Namespace, &control.SessionName, &control.SessionUID, &control.RequestDigest, &control.Availability,
		&control.RuntimeSessionGeneration, &control.LeaseGeneration, &leaseTaskUID, &leaseAttempt, &leasePromptID, &leaseRequestDigest,
		&leaseAcquired, &leaseExpires, &control.BlockedReason, &control.RelatedPromptAttemptID,
		&control.RelatedPublicationID, &verifiedRepositoryID, &verifiedRef, &verifiedSHA,
		&control.ControllerEpochName, &control.ControllerEpoch, &control.LastOperationID,
		&control.LastOperationDigest, &control.Version,
		&control.CreatedAt, &control.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.SessionControl{}, store.ErrNotFound
	}
	if err != nil {
		return store.SessionControl{}, fmt.Errorf("get session control: %w", err)
	}
	if leaseTaskUID != "" {
		control.Lease = &store.SessionMutationLease{
			Generation: control.LeaseGeneration, TaskUID: leaseTaskUID, Attempt: leaseAttempt,
			PromptID: leasePromptID, RequestDigest: leaseRequestDigest, AcquiredAt: leaseAcquired.Time,
		}
		if leaseExpires.Valid {
			value := leaseExpires.Time
			control.Lease.ExpiresAt = &value
		}
	}
	if verifiedRepositoryID != "" {
		control.VerifiedBaseline = &store.VerifiedBranchBaseline{RepositoryID: verifiedRepositoryID, Ref: verifiedRef, SHA: verifiedSHA}
	}
	return control, nil
}

func getSessionTurn(ctx context.Context, q controlQueryRower, id string) (store.SessionTurn, error) {
	var turn store.SessionTurn
	var receiptJSON []byte
	var finalizedAt, projectionAvailableAt sql.NullTime
	err := q.QueryRowContext(ctx,
		`SELECT id, session_uid, lease_generation, task_uid, attempt, prompt_id, prompt_attempt_id,
		        request_digest, user_prompt, state, terminal_kind, terminal_content, finalization_digest,
		        publication_id, publication_receipt, projection_id, projection_kind, projection_digest,
		        projection_available_at, controller_epoch_name, controller_epoch,
		        version, created_at, finalized_at, updated_at
		 FROM session_turns WHERE id = ?`, id,
	).Scan(
		&turn.ID, &turn.Key.SessionUID, &turn.Key.LeaseGeneration, &turn.Key.TaskUID,
		&turn.Key.Attempt, &turn.Key.PromptID, &turn.PromptAttemptID, &turn.RequestDigest,
		&turn.UserPrompt, &turn.State, &turn.TerminalKind, &turn.TerminalContent,
		&turn.FinalizationDigest, &turn.PublicationID, &receiptJSON, &turn.ProjectionID,
		&turn.ProjectionKind, &turn.ProjectionDigest, &projectionAvailableAt, &turn.ControllerEpochName,
		&turn.ControllerEpoch, &turn.Version, &turn.CreatedAt, &finalizedAt, &turn.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return store.SessionTurn{}, store.ErrNotFound
	}
	if err != nil {
		return store.SessionTurn{}, fmt.Errorf("get session turn: %w", err)
	}
	if controlJSONPresent(receiptJSON) {
		turn.PublicationReceipt = &store.PublicationReceipt{}
		if err := unmarshalOptionalControlJSON(receiptJSON, turn.PublicationReceipt); err != nil {
			return store.SessionTurn{}, fmt.Errorf("decode session turn publication receipt: %w", err)
		}
	}
	if finalizedAt.Valid {
		value := finalizedAt.Time
		turn.FinalizedAt = &value
	}
	if projectionAvailableAt.Valid {
		turn.ProjectionAvailableAt = projectionAvailableAt.Time
	}
	return turn, nil
}
