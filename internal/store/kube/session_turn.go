package kube

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

// CreateSessionTurn validates the Kubernetes-authoritative SessionControl and
// PromptAttempt, then persists only the SQLite-owned open turn record.
func (s *Store) CreateSessionTurn(ctx context.Context, request store.CreateSessionTurnRequest) (*store.SessionTurn, error) {
	if s.sessionTurns == nil {
		return nil, ErrSessionTurnStoreNotConfigured
	}
	normalized, fence, err := normalizeSessionTurnForCreateKube(request.Turn, request.Fence)
	if err != nil {
		return nil, err
	}
	if request.ExpectedSessionVersion < 1 {
		return nil, store.ValidationErrorf("expected session version must be at least 1")
	}
	_, snapshot, err := s.requireControllerEpoch(ctx, fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	if existing, getErr := s.sessionTurns.GetSessionTurn(ctx, normalized.ID); getErr == nil {
		if sameSessionTurnCreationKube(*existing, normalized) {
			return existing, nil
		}
		return nil, store.ConflictErrorf("session turn %q was reused with different prompt input or request digest", normalized.ID)
	} else if !errors.Is(getErr, store.ErrNotFound) {
		return nil, getErr
	}

	sessionObject, err := s.findSessionControlByUID(ctx, normalized.Key.SessionUID)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(sessionObject)
	if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatchesKey(control.Lease, normalized.Key) {
		return nil, store.ConflictErrorf("session turn %q does not match the active Kubernetes Session lease/version", normalized.ID)
	}
	if err := s.verifyMirroredSessionLease(ctx, sessionObject, *control.Lease); err != nil {
		return nil, err
	}
	evidence, err := s.loadSessionTurnExecutionEvidence(ctx, control, normalized)
	if err != nil {
		return nil, err
	}
	if evidence.harnessV1 != nil && evidence.harnessV1.State != store.HarnessV1AttemptPrepared {
		return nil, store.ConflictErrorf(
			"new harness v1 SessionTurn must be opened while attempt %q is Prepared, got %s",
			normalized.PromptAttemptID, evidence.harnessV1.State,
		)
	}
	return s.sessionTurns.CreateSessionTurnRecord(ctx, store.CreateSessionTurnRecordRequest{
		Turn:        normalized,
		Namespace:   control.Namespace,
		SessionName: control.SessionName,
		Fence:       fence,
	})
}

// FinalizeSessionTurn implements the cross-store completion protocol:
//
//  1. validate all Kubernetes-authoritative fences and terminal receipts;
//  2. atomically commit SQLite turn/transcript/deferred-outbox data;
//  3. CAS-update Kubernetes BranchClaim and SessionControl, then release Lease;
//  4. activate the SQLite outbox projection.
//
// Every step is idempotent so restart after any committed boundary resumes the
// same finalization instead of replaying transcript or publication effects.
func (s *Store) FinalizeSessionTurn(ctx context.Context, request store.FinalizeSessionTurnRequest) (*store.SessionTurn, error) {
	if s.sessionTurns == nil {
		return nil, ErrSessionTurnStoreNotConfigured
	}
	if s.outbox == nil {
		return nil, ErrOutboxStoreNotConfigured
	}
	normalized, turnID, err := normalizeCrossStoreFinalizationRequest(request)
	if err != nil {
		return nil, err
	}
	fence, snapshot, err := s.requireControllerEpoch(ctx, normalized.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)
	normalized.Fence = fence

	turn, err := s.sessionTurns.GetSessionTurn(ctx, turnID)
	if err != nil {
		return nil, err
	}
	if turn.State == store.SessionTurnFinalized {
		if !sessionTurnRequestMatchesFinalized(*turn, normalized) {
			return nil, store.ConflictErrorf("session turn %q was finalized with different terminal data or digest", turn.ID)
		}
		return s.completePersistedSessionTurnFinalization(ctx, *turn, fence, snapshot, nil)
	}
	if turn.Version != normalized.ExpectedTurnVersion {
		return nil, store.ConflictErrorf("session turn %q is version %d, expected %d", turn.ID, turn.Version, normalized.ExpectedTurnVersion)
	}

	sessionObject, err := s.findSessionControlByUID(ctx, normalized.Key.SessionUID)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(sessionObject)
	if control.Version != normalized.ExpectedSessionVersion || !sessionLeaseMatchesKey(control.Lease, normalized.Key) {
		return nil, store.ConflictErrorf("session turn %q finalization is fenced by a different Kubernetes Session version or lease", turn.ID)
	}
	if err := s.verifyMirroredSessionLease(ctx, sessionObject, *control.Lease); err != nil {
		return nil, err
	}
	evidence, err := s.loadSessionTurnExecutionEvidence(ctx, control, *turn)
	if err != nil {
		return nil, err
	}
	if err := validateSessionTurnExecutionForFinalization(evidence, normalized); err != nil {
		return nil, err
	}
	plan, err := s.buildCrossStoreFinalizationPlan(ctx, control, *turn, evidence, normalized)
	if err != nil {
		return nil, err
	}

	turn, err = s.sessionTurns.CommitSessionTurnFinalization(ctx, store.CommitSessionTurnFinalizationRequest{
		Key:                  normalized.Key,
		Namespace:            control.Namespace,
		SessionName:          control.SessionName,
		Fence:                fence,
		ExpectedTurnVersion:  normalized.ExpectedTurnVersion,
		FinalizationDigest:   normalized.FinalizationDigest,
		TerminalKind:         normalized.TerminalKind,
		TerminalContent:      normalized.TerminalContent,
		SkipTranscriptAppend: normalized.SkipTranscriptAppend,
		SkipUserPromptAppend: normalized.SkipUserPromptAppend,
		PublicationID:        normalized.PublicationID,
		PublicationReceipt:   plan.receipt,
		Projection:           normalized.Projection,
		FinalizedAt:          normalized.FinalizedAt,
	})
	if err != nil {
		return nil, err
	}
	return s.completePersistedSessionTurnFinalization(ctx, *turn, fence, snapshot, &plan)
}

// ResumeSessionTurnFinalization completes only the Kubernetes and outbox tail
// of a finalization whose SQLite SessionTurn/transcript/outbox transaction is
// already durable. All timing, digest, terminal, publication, and projection
// inputs are reloaded from the persisted turn and outbox record.
func (s *Store) ResumeSessionTurnFinalization(ctx context.Context, request store.ResumeSessionTurnFinalizationRequest) (*store.SessionTurn, error) {
	if s.sessionTurns == nil {
		return nil, ErrSessionTurnStoreNotConfigured
	}
	if s.outbox == nil {
		return nil, ErrOutboxStoreNotConfigured
	}
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
	fence, snapshot, err := s.requireControllerEpoch(ctx, request.Fence)
	if err != nil {
		return nil, err
	}
	defer s.releaseControllerEpochMutation(snapshot)

	turn, err := s.sessionTurns.GetSessionTurn(ctx, turnID)
	if err != nil {
		return nil, err
	}
	if turn.State != store.SessionTurnFinalized || turn.Key != request.Key || turn.PromptAttemptID != request.PromptAttemptID || turn.FinalizationDigest != request.FinalizationDigest {
		return nil, store.ConflictErrorf("session turn %q does not match the persisted finalization recovery identity", turnID)
	}
	return s.completePersistedSessionTurnFinalization(ctx, *turn, fence, snapshot, nil)
}

func (s *Store) completePersistedSessionTurnFinalization(
	ctx context.Context,
	turn store.SessionTurn,
	fence store.ControllerEpochFence,
	snapshot epochSnapshot,
	precomputedPlan *crossStoreFinalizationPlan,
) (*store.SessionTurn, error) {
	if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil {
		return nil, store.ConflictErrorf("session turn %q has no durable finalized receipt to resume", turn.ID)
	}
	projection, err := s.outbox.GetOutboxProjection(ctx, turn.ProjectionID)
	if err != nil {
		return nil, fmt.Errorf("load finalized SessionTurn projection: %w", err)
	}
	if projection.AggregateKind != sessionTurnAggregateKind || projection.AggregateID != turn.ID ||
		projection.ProjectionKind != turn.ProjectionKind || projection.PayloadDigest != turn.ProjectionDigest ||
		!projection.InitialAvailableAt.Equal(turn.ProjectionAvailableAt) {
		return nil, store.ConflictErrorf("outbox projection %q does not match persisted session turn %q", projection.ID, turn.ID)
	}
	sessionObject, err := s.findSessionControlByUID(ctx, turn.Key.SessionUID)
	if err != nil {
		return nil, err
	}
	control := sessionControlFromObject(sessionObject)
	evidence, err := s.loadSessionTurnExecutionEvidence(ctx, control, turn)
	if err != nil {
		return nil, err
	}
	expectedLeaseRequestDigest, err := store.SessionMutationLeaseRequestDigest(
		turn.Key.SessionUID, turn.Key.LeaseGeneration, turn.Key.TaskUID, turn.Key.Attempt, turn.Key.PromptID, evidence.requestDigest,
	)
	if err != nil {
		return nil, fmt.Errorf("derive finalized Session Lease request digest: %w", err)
	}
	if control.LeaseGeneration > turn.Key.LeaseGeneration {
		if err := s.activatePersistedSessionTurnProjection(ctx, turn, projection, fence); err != nil {
			return nil, err
		}
		return &turn, nil
	}
	if !sessionControlAlreadyFinalized(control, turn.ID, turn.FinalizationDigest) {
		if !sessionLeaseMatchesKey(control.Lease, turn.Key) {
			return nil, store.ConflictErrorf("session turn %q recovery is fenced by a different Kubernetes Session lease", turn.ID)
		}
		if err := s.verifyMirroredSessionLease(ctx, sessionObject, *control.Lease); err != nil {
			return nil, err
		}
	}

	projection.AvailableAt = turn.ProjectionAvailableAt
	request := store.FinalizeSessionTurnRequest{
		Key:                    turn.Key,
		Fence:                  fence,
		ExpectedSessionVersion: control.Version,
		ExpectedTurnVersion:    turn.Version,
		FinalizationDigest:     turn.FinalizationDigest,
		TerminalKind:           turn.TerminalKind,
		TerminalContent:        turn.TerminalContent,
		PublicationID:          turn.PublicationID,
		Projection:             *projection,
		FinalizedAt:            turn.FinalizedAt.UTC(),
	}
	if err := validateSessionTurnExecutionForFinalization(evidence, request); err != nil {
		return nil, err
	}
	plan := crossStoreFinalizationPlan{}
	if precomputedPlan == nil {
		plan, err = s.buildCrossStoreFinalizationPlan(ctx, control, turn, evidence, request)
		if err != nil {
			return nil, err
		}
	} else {
		plan = *precomputedPlan
	}
	if !reflect.DeepEqual(turn.PublicationReceipt, plan.receipt) {
		return nil, store.ConflictErrorf("session turn %q has a different publication receipt snapshot", turn.ID)
	}

	if plan.publication != nil {
		if err := s.finalizeBranchClaimForSessionTurn(ctx, *plan.publication, plan.verifiedBaseline, plan.availability, plan.blockReason, request, fence, snapshot); err != nil {
			return nil, err
		}
	}
	updatedSession, err := s.finalizeSessionControlForTurn(ctx, sessionObject, control, turn, plan, request, fence, snapshot)
	if err != nil {
		return nil, err
	}
	if err := s.releaseFinalizedSessionLease(ctx, updatedSession, turn.Key, expectedLeaseRequestDigest); err != nil {
		return nil, err
	}
	if err := s.activatePersistedSessionTurnProjection(ctx, turn, projection, fence); err != nil {
		return nil, err
	}
	return &turn, nil
}

func (s *Store) activatePersistedSessionTurnProjection(
	ctx context.Context,
	turn store.SessionTurn,
	projection *store.OutboxProjection,
	fence store.ControllerEpochFence,
) error {
	if projection == nil || turn.FinalizedAt == nil {
		return store.ConflictErrorf("session turn %q lacks its persisted projection activation identity", turn.ID)
	}
	projection.AvailableAt = turn.ProjectionAvailableAt
	_, err := s.sessionTurns.ActivateSessionTurnProjection(ctx, store.ActivateSessionTurnProjectionRequest{
		TurnID:                 turn.ID,
		ProjectionID:           turn.ProjectionID,
		Fence:                  fence,
		FinalizationDigest:     turn.FinalizationDigest,
		ExpectedAggregateKind:  sessionTurnAggregateKind,
		ExpectedProjectionKind: turn.ProjectionKind,
		ExpectedPayloadDigest:  turn.ProjectionDigest,
		AvailableAt:            turn.ProjectionAvailableAt,
		UpdatedAt:              *turn.FinalizedAt,
	})
	return err
}

const sessionTurnAggregateKind = "SessionTurn"

type sessionTurnExecutionEvidence struct {
	prompt        *store.PromptAttempt
	harnessV1     *store.HarnessV1Attempt
	requestDigest string
}

func (s *Store) loadSessionTurnExecutionEvidence(
	ctx context.Context,
	control store.SessionControl,
	turn store.SessionTurn,
) (sessionTurnExecutionEvidence, error) {
	prompt, promptErr := s.GetPromptAttempt(ctx, turn.PromptAttemptID)
	if promptErr == nil {
		if err := validateSessionTurnPromptBinding(*prompt, turn.Key); err != nil {
			return sessionTurnExecutionEvidence{}, err
		}
		return sessionTurnExecutionEvidence{prompt: prompt, requestDigest: prompt.RequestDigest}, nil
	}
	if !errors.Is(promptErr, store.ErrNotFound) {
		return sessionTurnExecutionEvidence{}, fmt.Errorf("session turn prompt attempt: %w", promptErr)
	}
	if s.harnessV1Attempts == nil || turn.Key.Attempt > int64(^uint32(0)>>1) {
		return sessionTurnExecutionEvidence{}, fmt.Errorf("session turn execution attempt: %w", promptErr)
	}
	key := store.HarnessV1AttemptKey{
		Namespace: control.Namespace,
		TaskUID:   turn.Key.TaskUID,
		Attempt:   int32(turn.Key.Attempt),
	}
	attempt, err := s.harnessV1Attempts.GetHarnessV1Attempt(ctx, key)
	if err != nil {
		return sessionTurnExecutionEvidence{}, fmt.Errorf("session turn harness v1 attempt: %w", err)
	}
	if turn.PromptAttemptID != key.SessionReferenceID() || attempt.TurnID != turn.Key.PromptID ||
		attempt.TaskUID != turn.Key.TaskUID || int64(attempt.Attempt) != turn.Key.Attempt {
		return sessionTurnExecutionEvidence{}, store.ConflictErrorf(
			"session turn harness v1 attempt does not match the fenced SessionTurn identity",
		)
	}
	task := &corev1alpha1.Task{}
	if err := s.readClient().Get(ctx, types.NamespacedName{Namespace: control.Namespace, Name: attempt.TaskName}, task); err != nil {
		return sessionTurnExecutionEvidence{}, mapKubernetesError("read harness v1 SessionTurn Task", err)
	}
	binding := task.Status.AgentExecutionBinding
	if string(task.UID) != attempt.TaskUID || task.Spec.Type != corev1alpha1.TaskTypeAgent || binding == nil ||
		binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV1 ||
		binding.BindingDigest != attempt.BindingDigest || binding.Snapshot.Digest != attempt.SnapshotDigest {
		return sessionTurnExecutionEvidence{}, store.ConflictErrorf(
			"session turn harness v1 attempt does not match the immutable Kubernetes Task binding",
		)
	}
	if err := store.ValidateCanonicalDigest("harness v1 SessionTurn request digest", attempt.RequestDigest); err != nil {
		return sessionTurnExecutionEvidence{}, err
	}
	return sessionTurnExecutionEvidence{harnessV1: attempt, requestDigest: attempt.RequestDigest}, nil
}

func validateSessionTurnExecutionForFinalization(
	evidence sessionTurnExecutionEvidence,
	request store.FinalizeSessionTurnRequest,
) error {
	if evidence.prompt != nil {
		return validatePromptAttemptForCrossStoreFinalization(*evidence.prompt, request)
	}
	if evidence.harnessV1 == nil {
		return store.ConflictErrorf("session turn has no route-specific execution receipt")
	}
	attempt := evidence.harnessV1
	if !store.IsTerminalHarnessV1AttemptState(attempt.State) {
		key := store.HarnessV1AttemptKey{Namespace: attempt.Namespace, TaskUID: attempt.TaskUID, Attempt: attempt.Attempt}
		return store.ConflictErrorf("harness v1 attempt %q is not terminal: %s", key.CanonicalID(), attempt.State)
	}
	if request.PublicationID != "" || request.VerifiedBaseline != nil {
		return store.ValidationErrorf("harness v1 SessionTurn cannot carry v2 publication state")
	}
	if request.TerminalKind == store.SessionTurnAssistantResult && attempt.State != store.HarnessV1AttemptSucceeded {
		return store.ValidationErrorf("assistant-result finalization requires succeeded harness v1 execution, got %s", attempt.State)
	}
	if attempt.State == store.HarnessV1AttemptSucceeded && attempt.TerminalReceiptDigest == "" {
		return store.ConflictErrorf("succeeded harness v1 attempt has no authoritative terminal receipt")
	}
	return nil
}

type crossStoreFinalizationPlan struct {
	publication      *store.Publication
	receipt          *store.PublicationReceipt
	verifiedBaseline *store.VerifiedBranchBaseline
	availability     store.SessionAvailability
	blockReason      string
}

//nolint:gocyclo // Finalization keeps v1/v2 evidence and publication blocking decisions in one atomic plan.
func (s *Store) buildCrossStoreFinalizationPlan(
	ctx context.Context,
	control store.SessionControl,
	turn store.SessionTurn,
	evidence sessionTurnExecutionEvidence,
	request store.FinalizeSessionTurnRequest,
) (crossStoreFinalizationPlan, error) {
	plan := crossStoreFinalizationPlan{availability: store.SessionAvailable, blockReason: request.BlockReason}
	if control.VerifiedBaseline != nil {
		copyValue := *control.VerifiedBaseline
		plan.verifiedBaseline = &copyValue
	}
	if evidence.harnessV1 != nil {
		if request.PublicationID != "" || request.VerifiedBaseline != nil {
			return crossStoreFinalizationPlan{}, store.ValidationErrorf("harness v1 SessionTurn cannot finalize publication state")
		}
		if evidence.harnessV1.State == store.HarnessV1AttemptOutcomeUnknown {
			expected := strings.TrimSpace(evidence.harnessV1.TerminalReason)
			if expected == "" {
				expected = "harness v1 outcome cannot be proven"
			}
			if plan.blockReason == "" {
				plan.blockReason = expected
			} else if plan.blockReason != expected {
				return crossStoreFinalizationPlan{}, store.ConflictErrorf("session block reason does not match the harness v1 terminal receipt")
			}
			plan.availability = store.SessionReconciliationBlocked
		} else if plan.blockReason != "" {
			return crossStoreFinalizationPlan{}, store.ValidationErrorf("only OutcomeUnknown harness v1 execution may block a Session")
		}
		return plan, nil
	}
	if evidence.prompt == nil {
		return crossStoreFinalizationPlan{}, store.ConflictErrorf("session turn has no route-specific execution evidence")
	}
	attempt := *evidence.prompt
	if request.PublicationID == "" {
		if request.VerifiedBaseline != nil {
			return crossStoreFinalizationPlan{}, store.ValidationErrorf("verified baseline advancement requires a publication receipt")
		}
		if plan.blockReason != "" {
			return crossStoreFinalizationPlan{}, store.ValidationErrorf("session reconciliation blocking requires a publication receipt")
		}
		return plan, nil
	}

	publication, err := s.GetPublication(ctx, request.PublicationID)
	if err != nil {
		return crossStoreFinalizationPlan{}, fmt.Errorf("finalize publication: %w", err)
	}
	if publication.TaskUID != request.Key.TaskUID || publication.Attempt != request.Key.Attempt || publication.PromptID != request.Key.PromptID ||
		(publication.SessionUID != "" && publication.SessionUID != request.Key.SessionUID) || !store.IsTerminalPublicationState(publication.State) {
		return crossStoreFinalizationPlan{}, store.ConflictErrorf("publication %q does not match the terminal session turn identity/state", publication.ID)
	}
	expectedDelivery, err := promptDeliveryStateForPublicationKube(publication.State)
	if err != nil {
		return crossStoreFinalizationPlan{}, err
	}
	if attempt.DeliveryState != expectedDelivery {
		return crossStoreFinalizationPlan{}, store.ConflictErrorf("prompt attempt delivery state %s does not match publication state %s", attempt.DeliveryState, publication.State)
	}
	receipt := publicationReceiptKube(*publication)
	plan.publication = publication
	plan.receipt = &receipt
	derivedBaseline, unresolved, err := finalizationPublicationBaselineKube(*publication)
	if err != nil {
		return crossStoreFinalizationPlan{}, err
	}
	if derivedBaseline != nil {
		if request.VerifiedBaseline != nil && !reflect.DeepEqual(*request.VerifiedBaseline, *derivedBaseline) {
			return crossStoreFinalizationPlan{}, store.ConflictErrorf("requested verified baseline does not match independent publication receipt")
		}
		plan.verifiedBaseline = derivedBaseline
	}
	if unresolved {
		expectedBlockReason := sessionPublicationBlockReason(*publication)
		if plan.blockReason == "" {
			plan.blockReason = expectedBlockReason
		} else if plan.blockReason != expectedBlockReason {
			return crossStoreFinalizationPlan{}, store.ConflictErrorf("session block reason does not match the durable publication outcome")
		}
	}
	if plan.blockReason != "" {
		plan.availability = store.SessionReconciliationBlocked
	}
	_ = turn
	return plan, nil
}

func sessionPublicationBlockReason(publication store.Publication) string {
	reason := strings.TrimSpace(publication.TerminalReason)
	if reason == "" {
		reason = fmt.Sprintf("publication %s remains unresolved", publication.ID)
	}
	return reason
}

func (s *Store) finalizeBranchClaimForSessionTurn(ctx context.Context, publication store.Publication, baseline *store.VerifiedBranchBaseline, availability store.SessionAvailability, blockReason string, request store.FinalizeSessionTurnRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) error {
	object, err := s.getBranchClaimObject(ctx, publication.BranchClaimID)
	if err != nil {
		return err
	}
	claim := branchClaimFromObject(object)
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
	operationID := "finalize:" + publication.ID
	if claim.LastOperationID == operationID {
		if claim.LastOperationDigest == request.FinalizationDigest && claim.Generation == publication.BranchClaimGeneration && claim.LastVerified.Equal(newRemote) && claim.Availability == claimAvailability && claim.BlockedReason == blockReason && claim.RelatedPublicationID == relatedPublicationID {
			return nil
		}
		return store.ConflictErrorf("branch finalization operation %q was already applied with different target values", operationID)
	}
	if claim.Generation != publication.BranchClaimGeneration || claim.RepositoryID != publication.TargetRepositoryID || claim.Ref != publication.TargetRef || !claim.LastVerified.Equal(publication.Baseline) || claim.Availability != store.BranchClaimAvailable {
		return store.ConflictErrorf("branch claim %q no longer matches publication finalization baseline/generation", claim.ID)
	}
	updated := object.DeepCopy()
	remote := remoteRefToAPI(newRemote)
	updated.Status.LastVerified = &remote
	updated.Status.Availability = corev1alpha1.BranchClaimAvailability(claimAvailability)
	updated.Status.BlockedReason = blockReason
	updated.Status.RelatedPublicationID = relatedPublicationID
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, claim.Version+1, operationID, request.FinalizationDigest, claim.CreatedAt, request.FinalizedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return mapKubernetesError("finalize SessionTurn branch claim", err)
	}
	return nil
}

func (s *Store) finalizeSessionControlForTurn(ctx context.Context, original *corev1alpha1.RuntimeSessionControl, control store.SessionControl, turn store.SessionTurn, plan crossStoreFinalizationPlan, request store.FinalizeSessionTurnRequest, fence store.ControllerEpochFence, snapshot epochSnapshot) (*corev1alpha1.RuntimeSessionControl, error) {
	operationID := "finalize:" + turn.ID
	if control.LastOperationID == operationID {
		if sessionFinalizationTargetMatches(control, plan, request.FinalizationDigest, turn.PromptAttemptID, request.PublicationID) {
			return original, nil
		}
		return nil, store.ConflictErrorf("session finalization operation %q was already applied with different target values", operationID)
	}
	if control.Version != request.ExpectedSessionVersion || !sessionLeaseMatchesKey(control.Lease, request.Key) {
		return nil, store.ConflictErrorf("session turn %q finalization is fenced by a different Kubernetes Session version or lease", turn.ID)
	}
	updated := original.DeepCopy()
	updated.Status.Availability = corev1alpha1.RuntimeSessionControlAvailability(plan.availability)
	updated.Status.MutationLease = nil
	updated.Status.BlockedReason = plan.blockReason
	updated.Status.RelatedPromptAttemptID = ""
	updated.Status.RelatedPublicationID = ""
	if plan.availability == store.SessionReconciliationBlocked {
		updated.Status.RelatedPromptAttemptID = turn.PromptAttemptID
		updated.Status.RelatedPublicationID = request.PublicationID
	}
	updated.Status.VerifiedBaseline = verifiedBaselineToAPI(plan.verifiedBaseline)
	setMutationStatus(&updated.Status.ControlRecordMutationStatus, fence, snapshot, control.Version+1, operationID, request.FinalizationDigest, control.CreatedAt, request.FinalizedAt)
	if err := s.client.Status().Update(ctx, updated); err != nil {
		return nil, mapKubernetesError("finalize SessionTurn SessionControl", err)
	}
	return updated, nil
}

func sessionFinalizationTargetMatches(control store.SessionControl, plan crossStoreFinalizationPlan, finalizationDigest, promptAttemptID, publicationID string) bool {
	if control.LastOperationDigest != finalizationDigest || control.Lease != nil || control.Availability != plan.availability || control.BlockedReason != plan.blockReason || !reflect.DeepEqual(control.VerifiedBaseline, plan.verifiedBaseline) {
		return false
	}
	if plan.availability == store.SessionAvailable {
		return control.RelatedPromptAttemptID == "" && control.RelatedPublicationID == ""
	}
	return control.RelatedPromptAttemptID == promptAttemptID && control.RelatedPublicationID == publicationID
}

func (s *Store) releaseFinalizedSessionLease(ctx context.Context, object *corev1alpha1.RuntimeSessionControl, key store.SessionTurnKey, requestDigest string) error {
	if err := store.ValidateCanonicalDigest("finalized Session Lease request digest", requestDigest); err != nil {
		return err
	}
	lease, state, err := s.getSessionLease(ctx, object.Namespace, object.Spec.SessionName, object.Spec.SessionUID)
	if err != nil {
		return err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		if state.Mode == leaseModeEmpty && state.Generation == key.LeaseGeneration {
			return nil
		}
		return store.ConflictErrorf("finalized Session Lease has unexpected empty-state generation")
	}
	if state.Mode != leaseModeMutation || state.Generation != key.LeaseGeneration || state.TaskUID != key.TaskUID || state.Attempt != key.Attempt || state.PromptID != key.PromptID || state.RequestDigest != requestDigest {
		return store.ConflictErrorf("session mutation Lease no longer matches finalized turn")
	}
	updated := lease.DeepCopy()
	clearSessionLease(updated, state.Generation)
	if err := s.client.Update(ctx, updated); err != nil {
		return mapKubernetesError("release finalized Session mutation Lease", err)
	}
	return nil
}

func (s *Store) findSessionControlByUID(ctx context.Context, sessionUID string) (*corev1alpha1.RuntimeSessionControl, error) {
	list := &corev1alpha1.RuntimeSessionControlList{}
	if err := s.readClient().List(ctx, list, s.namespacedListOptions()...); err != nil {
		return nil, mapKubernetesError("list runtime session controls", err)
	}
	var match *corev1alpha1.RuntimeSessionControl
	for i := range list.Items {
		if list.Items[i].Spec.SessionUID != sessionUID {
			continue
		}
		if match != nil {
			return nil, store.ConflictErrorf("multiple RuntimeSessionControls exist for Session UID %q", sessionUID)
		}
		match = list.Items[i].DeepCopy()
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func normalizeSessionTurnForCreateKube(turn store.SessionTurn, fence store.ControllerEpochFence) (store.SessionTurn, store.ControllerEpochFence, error) {
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
	if turn.State != store.SessionTurnOpen || turn.TerminalKind != "" || turn.TerminalContent != "" || turn.FinalizationDigest != "" || turn.PublicationID != "" || turn.PublicationReceipt != nil || turn.FinalizedAt != nil {
		return store.SessionTurn{}, store.ControllerEpochFence{}, store.ValidationErrorf("new session turn must be open and must not contain finalization data")
	}
	normalizedFence, err := store.NormalizeEpochFence(fence)
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
	turn.ControllerEpochName = normalizedFence.Name
	turn.ControllerEpoch = normalizedFence.Epoch
	turn.Version = 1
	return turn, normalizedFence, nil
}

func normalizeCrossStoreFinalizationRequest(request store.FinalizeSessionTurnRequest) (store.FinalizeSessionTurnRequest, string, error) {
	if err := request.Key.Validate(); err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	turnID, err := request.Key.CanonicalID()
	if err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	fence, err := store.NormalizeEpochFence(request.Fence)
	if err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	request.Fence = fence
	if request.ExpectedSessionVersion < 1 || request.ExpectedTurnVersion < 1 {
		return store.FinalizeSessionTurnRequest{}, "", store.ValidationErrorf("expected session and turn versions must be at least 1")
	}
	if err := store.ValidateCanonicalDigest("session turn finalization digest", request.FinalizationDigest); err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	if request.TerminalKind != store.SessionTurnAssistantResult && request.TerminalKind != store.SessionTurnOutcomeMarker {
		return store.FinalizeSessionTurnRequest{}, "", store.ValidationErrorf("unsupported session turn terminal kind %q", request.TerminalKind)
	}
	if request.TerminalKind == store.SessionTurnOutcomeMarker && strings.TrimSpace(request.TerminalContent) == "" {
		return store.FinalizeSessionTurnRequest{}, "", store.ValidationErrorf("outcome-marker finalization requires explicit marker content")
	}
	if request.SkipTranscriptAppend && request.SkipUserPromptAppend {
		return store.FinalizeSessionTurnRequest{}, "", store.ValidationErrorf("session turn cannot combine full transcript suppression with user-prompt-only suppression")
	}
	if err := store.ValidateControlText("session turn terminal content", request.TerminalContent); err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	request.PublicationID = strings.TrimSpace(request.PublicationID)
	if request.PublicationID != "" {
		if err := store.ValidateControlIdentifier("publication ID", request.PublicationID); err != nil {
			return store.FinalizeSessionTurnRequest{}, "", err
		}
	}
	request.BlockReason = strings.TrimSpace(request.BlockReason)
	if err := store.ValidateControlReason("session block reason", request.BlockReason); err != nil {
		return store.FinalizeSessionTurnRequest{}, "", err
	}
	if request.VerifiedBaseline != nil {
		copyValue := *request.VerifiedBaseline
		request.VerifiedBaseline = &copyValue
		if err := validateVerifiedBaseline(copyValue); err != nil {
			return store.FinalizeSessionTurnRequest{}, "", err
		}
	}
	request.FinalizedAt = store.NormalizeControlTime(request.FinalizedAt)
	return request, turnID, nil
}

func validateSessionTurnPromptBinding(attempt store.PromptAttempt, key store.SessionTurnKey) error {
	if attempt.Key.TaskUID != key.TaskUID || attempt.Key.Attempt != key.Attempt || attempt.Key.PromptID != key.PromptID || attempt.SessionUID != key.SessionUID || attempt.SessionLeaseGeneration != key.LeaseGeneration {
		return store.ConflictErrorf("session turn prompt attempt does not match the fenced SessionTurn identity")
	}
	return nil
}

func validatePromptAttemptForCrossStoreFinalization(attempt store.PromptAttempt, request store.FinalizeSessionTurnRequest) error {
	if err := validateSessionTurnPromptBinding(attempt, request.Key); err != nil {
		return err
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

func promptDeliveryStateForPublicationKube(state store.PublicationState) (store.PromptDeliveryState, error) {
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

func finalizationPublicationBaselineKube(publication store.Publication) (*store.VerifiedBranchBaseline, bool, error) {
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
	case store.PublicationOutcomeUnknown, store.PublicationDeliveryConflict:
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

func publicationReceiptKube(publication store.Publication) store.PublicationReceipt {
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

func sessionTurnRequestMatchesFinalized(turn store.SessionTurn, request store.FinalizeSessionTurnRequest) bool {
	availableAt := request.Projection.AvailableAt
	if availableAt.IsZero() {
		availableAt = request.FinalizedAt
	}
	return turn.FinalizationDigest == request.FinalizationDigest && turn.TerminalKind == request.TerminalKind && turn.TerminalContent == request.TerminalContent && turn.PublicationID == request.PublicationID &&
		turn.ProjectionID == request.Projection.ID && turn.ProjectionKind == request.Projection.ProjectionKind && turn.ProjectionDigest == request.Projection.PayloadDigest && turn.ProjectionAvailableAt.Equal(availableAt)
}

func sessionControlAlreadyFinalized(control store.SessionControl, turnID, digest string) bool {
	return control.LastOperationID == "finalize:"+turnID && control.LastOperationDigest == digest && control.Lease == nil
}

func sessionLeaseMatchesKey(lease *store.SessionMutationLease, key store.SessionTurnKey) bool {
	return lease != nil && lease.Generation == key.LeaseGeneration && lease.TaskUID == key.TaskUID && lease.Attempt == key.Attempt && lease.PromptID == key.PromptID
}

func sameSessionTurnCreationKube(a, b store.SessionTurn) bool {
	return a.ID == b.ID && a.Key == b.Key && a.PromptAttemptID == b.PromptAttemptID && a.RequestDigest == b.RequestDigest && a.UserPrompt == b.UserPrompt
}
