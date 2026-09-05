package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/publisher"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

type persistedPublicationRecoveryContext struct {
	task                 *corev1alpha1.Task
	attemptID            string
	fence                store.ControllerEpochFence
	workspace            *corev1alpha1.WorkspaceConfig
	source               publisher.Repository
	pullRequestBase      publisher.Repository
	target               publisher.Repository
	sourceCredential     *publisherservice.CredentialReference
	targetReadCredential *publisherservice.CredentialReference
	writeCredential      *publisherservice.CredentialReference
	forgeCredential      *publisherservice.CredentialReference
	claim                *store.BranchClaim
	artifact             harnessv2.ArtifactReference
}

// reconcilePersistedPublication resumes only the immutable Publication already
// attached to a successfully settled prompt. It never allocates a new prompt or
// Publication identity.
func (d *ACPDispatcher) reconcilePersistedPublication(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (acpPublicationResult, error) {
	ctx, publicationTrace := startACPPublicationRecoverySpan(ctx, task, publicationIDForTask(task))
	result, err := d.reconcilePersistedPublicationOperation(ctx, task, attemptID, fence)
	publicationTrace.End(err)
	return result, err
}

func (d *ACPDispatcher) reconcilePersistedPublicationOperation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (acpPublicationResult, error) {
	publication, recovery, err := d.loadPersistedPublicationRecovery(ctx, task, attemptID, fence)
	if err != nil {
		return acpPublicationResult{}, err
	}
	for step := 0; step < 5 && !store.IsTerminalPublicationState(publication.State); step++ {
		publication, err = d.reconcilePersistedPublicationStep(ctx, publication, recovery)
		if err != nil {
			return acpPublicationResult{}, err
		}
	}
	return d.finishPersistedPublicationRecovery(ctx, publication, recovery)
}

func (d *ACPDispatcher) loadPersistedPublicationRecovery(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
) (*store.Publication, persistedPublicationRecoveryContext, error) {
	publication, err := d.Store.GetPublication(ctx, publicationIDForTask(task))
	if err != nil {
		return nil, persistedPublicationRecoveryContext{}, err
	}
	claim, err := d.Store.GetBranchClaim(ctx, publication.BranchClaimID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) || publication.SessionUID != "" || !store.IsTerminalPublicationState(publication.State) ||
			publication.State == store.PublicationOutcomeUnknown {
			return nil, persistedPublicationRecoveryContext{}, err
		}
		claim = nil
	}
	workspace := task.Spec.Workspace
	if workspace == nil || workspace.PublicationCredentialRef == nil {
		return nil, persistedPublicationRecoveryContext{}, fmt.Errorf("persisted publication workspace credentials are unavailable")
	}
	source, err := workspaceRepository(workspace)
	if err != nil {
		return nil, persistedPublicationRecoveryContext{}, err
	}
	target, err := workspacePublicationRepository(workspace)
	if err != nil {
		return nil, persistedPublicationRecoveryContext{}, err
	}
	pullRequestBase := source
	sourceCredential := publisherCredentialReference(workspace.ReadCredentialRef, publisherservice.CredentialRoleSourceRead)
	continuation := false
	if publication.SessionUID != "" {
		if task.Spec.SessionRef == nil || strings.TrimSpace(task.Spec.SessionRef.Name) == "" {
			return nil, persistedPublicationRecoveryContext{}, fmt.Errorf("persisted Session publication lacks Task session identity")
		}
		control, controlErr := d.Store.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
		if controlErr != nil {
			return nil, persistedPublicationRecoveryContext{}, controlErr
		}
		if control.SessionUID != publication.SessionUID {
			return nil, persistedPublicationRecoveryContext{}, fmt.Errorf("persisted Session publication UID drifted")
		}
		continuation = control.VerifiedBaseline != nil && publication.SourceRepositoryID == target.ID
	}
	if continuation {
		// Continuation preparation reads from the independently verified
		// publication target rather than the original upstream/PR base.
		source = target
		sourceCredential = publisherCredentialReference(workspace.PublicationReadCredentialRef, publisherservice.CredentialRoleTargetRead)
	}
	if source.ID != publication.SourceRepositoryID || target.ID != publication.TargetRepositoryID ||
		(claim != nil && claim.ID != publication.BranchClaimID) {
		return nil, persistedPublicationRecoveryContext{}, fmt.Errorf("persisted publication repository or BranchClaim identity drifted")
	}
	if publication.PRIntent != nil &&
		(publication.PRIntent.BaseRepositoryID != pullRequestBase.ID || publication.PRIntent.HeadRepositoryID != target.ID) {
		return nil, persistedPublicationRecoveryContext{}, fmt.Errorf("persisted pull request repository identity drifted")
	}
	return publication, persistedPublicationRecoveryContext{
		task: task, attemptID: attemptID, fence: fence, workspace: workspace,
		source: source, pullRequestBase: pullRequestBase, target: target,
		sourceCredential:     sourceCredential,
		targetReadCredential: publisherCredentialReference(workspace.PublicationReadCredentialRef, publisherservice.CredentialRoleTargetRead),
		writeCredential:      publisherCredentialReference(workspace.PublicationCredentialRef, publisherservice.CredentialRoleTargetWrite),
		forgeCredential:      publisherForgeCredentialReference(workspace.ForgeCredentialRef),
		claim:                claim,
		artifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(publication.ArtifactID), Digest: publication.ArtifactDigest,
			SizeBytes: publication.ArtifactSizeBytes, MediaType: publication.ArtifactMediaType,
		},
	}, nil
}

func (d *ACPDispatcher) reconcilePersistedPublicationStep(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, error) {
	switch publication.State {
	case store.PublicationPreparing:
		return d.recoverPublicationPreparing(ctx, publication, recovery)
	case store.PublicationPrepared:
		return d.recoverPublicationPrepared(ctx, publication, recovery)
	case store.PublicationPublishing:
		return d.recoverPublicationPublishing(ctx, publication, recovery)
	case store.PublicationVerifying:
		return d.recoverPublicationVerifying(ctx, publication, recovery)
	default:
		return nil, fmt.Errorf("publication recovery cannot resume state %s", publication.State)
	}
}

func (d *ACPDispatcher) recoverPublicationPreparing(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, error) {
	if err := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryValidating, store.PromptDeliveryPreparing, "recover-publication-preparing"); err != nil {
		return nil, err
	}
	operation := publicationOperationID("prepare", recovery.task)
	request := publisherservice.PublicationPrepareRequest{
		Metadata:            publisherservice.OperationMetadata{Namespace: recovery.task.Namespace, PublicationID: publication.ID, OperationID: operation},
		SourceCredentialRef: recovery.sourceCredential,
		DeltaArtifact:       recovery.artifact,
		Request: publisher.PrepareRequest{
			PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: operation,
			Source: recovery.source, SourceRef: publication.SourceRef, Target: recovery.target, TargetRef: publication.TargetRef,
			BranchClaimGeneration: publication.BranchClaimGeneration, BaselineOID: publication.SourceBaselineSHA,
			RemoteBefore: publisherRemoteRef(publication.Baseline), DeltaArtifactDigest: publication.ArtifactDigest,
			RelativeRoot:  strings.TrimSpace(recovery.workspace.SubPath),
			CommitMessage: publication.CommitMessage, CommitTimestamp: publication.CommitTimestamp,
		},
	}
	identity := store.ExternalEffectIdentity{
		Kind: "publisher.prepare", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: operation,
	}
	response, err := runACPExternalEffect(ctx, d, recovery.fence, identity, request, func(callCtx context.Context) (publisherservice.PublicationPrepareResponse, error) {
		return d.Publisher.PreparePublication(callCtx, request)
	})
	if err != nil {
		effectID, idErr := identity.CanonicalID()
		if idErr != nil {
			return nil, idErr
		}
		effect, getErr := d.Store.GetExternalEffect(ctx, effectID)
		if getErr != nil {
			return nil, getErr
		}
		if !retryableACPExternalEffectError(err) || effect.Attempts >= 3 {
			if settleErr := settleACPExternalEffectError(ctx, d, recovery.fence, identity, err); settleErr != nil {
				return nil, settleErr
			}
			publicationState := store.PublicationPreparationFailed
			deliveryState := store.PromptDeliveryConflict
			reason := "publication preparation failed during recovery"
			if retryableACPExternalEffectError(err) {
				publicationState = store.PublicationOutcomeUnknown
				deliveryState = store.PromptDeliveryPublicationOutcomeUnknown
				reason = "publication preparation outcome remained unknown after bounded recovery"
			}
			updated, transitionErr := d.transitionPublication(ctx, publication, recovery.fence, publicationState,
				publicationOperationID("prepare-terminal", recovery.task), mustACPDomainDigest("publication-prepare-terminal", publication.ID), nil, nil, nil, reason)
			if transitionErr != nil {
				return nil, transitionErr
			}
			if transitionErr := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryPreparing, deliveryState, "recover-publication-prepare-terminal"); transitionErr != nil {
				return nil, transitionErr
			}
			return updated, nil
		}
		return nil, err
	}
	prepared, err := preparedReceiptFromPublisher(response.Prepared)
	if err != nil {
		return nil, err
	}
	return d.transitionPublication(ctx, publication, recovery.fence, store.PublicationPrepared,
		operation, response.RequestDigest, prepared, nil, nil, "")
}

func (d *ACPDispatcher) recoverPublicationPrepared(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, error) {
	if err := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryPreparing, store.PromptDeliveryPrepared, "recover-publication-prepared"); err != nil {
		return nil, err
	}
	cancelled, err := d.taskCancellationRequested(ctx, recovery.task)
	if err != nil {
		return nil, err
	}
	if !cancelled {
		return d.transitionPublication(ctx, publication, recovery.fence, store.PublicationPublishing,
			publicationOperationID("publishing", recovery.task), mustACPDomainDigest("publication-publishing", publication.ID), nil, nil, nil, "")
	}
	publication, err = d.transitionPublication(ctx, publication, recovery.fence, store.PublicationCancelledBeforePublish,
		publicationOperationID("cancel-before-publish", recovery.task), mustACPDomainDigest("publication-cancel", publication.ID), nil, nil, nil, "")
	if err != nil {
		return nil, err
	}
	if err := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryPrepared, store.PromptDeliveryCancelledBeforePublish, "recover-cancel-before-publish"); err != nil {
		return nil, err
	}
	return publication, nil
}

func (d *ACPDispatcher) recoverPublicationPublishing(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, error) {
	if err := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryPrepared, store.PromptDeliveryPublishing, "recover-publication-publishing"); err != nil {
		return nil, err
	}
	if publication.PreparedReceipt == nil {
		return nil, fmt.Errorf("persisted publishing Publication lacks prepared receipt")
	}
	operation := publicationOperationID("publish", recovery.task)
	publishRequest := publisher.PublishRequest{
		PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: operation,
		Target: recovery.target, TargetRef: publication.TargetRef, Claim: publisherBranchClaim(*recovery.claim),
		RemoteBefore: publisherRemoteRef(publication.Baseline), ExpectedCommitOID: publication.PreparedReceipt.CommitSHA,
		BundleDigest: publication.PreparedReceipt.BundleDigest,
	}
	serviceRequest := publisherservice.PublicationPublishRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: recovery.task.Namespace, PublicationID: publication.ID, OperationID: operation},
		CredentialRef: recovery.writeCredential,
		Prepared:      publisherPreparedFromPublication(publication, recovery.source, recovery.target),
		Request:       publishRequest,
	}
	response, callErr := runACPExternalEffect(ctx, d, recovery.fence, store.ExternalEffectIdentity{
		Kind: "publisher.publish", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: operation,
	}, serviceRequest, func(callCtx context.Context) (publisherservice.PublicationPublishResponse, error) {
		return d.Publisher.Publish(callCtx, serviceRequest)
	})
	receipt := &store.PublishOperationReceipt{
		OperationID: operation, RequestDigest: mustACPDomainDigest("publisher-publish", publishRequest),
		TargetRepositoryID: recovery.target.ID, TargetRef: publication.TargetRef, RemoteBefore: publication.Baseline,
		ExpectedCommitSHA: publication.PreparedReceipt.CommitSHA, AcknowledgementUnknown: callErr != nil, PublishedAt: time.Now().UTC(),
	}
	if callErr == nil {
		receipt.OperationID, receipt.RequestDigest = response.Receipt.OperationID, response.Receipt.RequestDigest
		receipt.AcknowledgementUnknown = response.Receipt.Outcome == publisher.PublishOutcomeUnknown
	}
	return d.transitionPublication(ctx, publication, recovery.fence, store.PublicationVerifying,
		receipt.OperationID, receipt.RequestDigest, nil, receipt, nil, "")
}

func (d *ACPDispatcher) recoverPublicationVerifying(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, error) {
	if err := d.recoveryDeliveryTransition(ctx, recovery.attemptID, recovery.fence, store.PromptDeliveryPublishing, store.PromptDeliveryVerifying, "recover-publication-verifying"); err != nil {
		return nil, err
	}
	if publication.PreparedReceipt == nil {
		return nil, fmt.Errorf("persisted verifying Publication lacks prepared receipt")
	}
	operation := publicationOperationID("verify", recovery.task)
	verifyRequest := publisherservice.PublicationVerifyRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: recovery.task.Namespace, PublicationID: publication.ID, OperationID: operation},
		CredentialRef: recovery.targetReadCredential,
		Prepared:      publisherPreparedFromPublication(publication, recovery.source, recovery.target),
		Request: publisher.VerifyRequest{
			PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: operation,
			Target: recovery.target, TargetRef: publication.TargetRef, Claim: publisherBranchClaim(*recovery.claim),
			ExpectedCommitOID: publication.PreparedReceipt.CommitSHA, BundleDigest: publication.PreparedReceipt.BundleDigest,
		},
	}
	response, callErr := runACPExternalEffect(ctx, d, recovery.fence, store.ExternalEffectIdentity{
		Kind: "publisher.verify", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: operation,
	}, verifyRequest, func(callCtx context.Context) (publisherservice.PublicationVerifyResponse, error) {
		return d.Publisher.Verify(callCtx, verifyRequest)
	})
	verification := &store.PublicationVerificationReceipt{
		OperationID: operation, RequestDigest: mustACPDomainDigest("publisher-verify-unknown", publication.ID),
		Outcome: store.PublicationOutcomeUnknown, ExpectedCommitSHA: publication.PreparedReceipt.CommitSHA,
		ObservedRemote: store.RemoteRefState{}, VerifiedAt: time.Now().UTC(),
	}
	reason := "publication remote outcome could not be independently observed after restart"
	if callErr == nil {
		verification = storeVerificationReceipt(response.Receipt)
		if verification.Outcome == store.PublicationVerifiedExact || verification.Outcome == store.PublicationDeliveredSuperseded {
			reason = ""
		}
	} else if err := settleACPExternalEffect(ctx, d, recovery.fence, store.ExternalEffectIdentity{
		Kind: "publisher.verify", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: operation,
	}, store.ExternalEffectOutcomeUnknown, nil); err != nil {
		return nil, err
	}
	if publication.PublishReceipt != nil && publication.PublishReceipt.AcknowledgementUnknown {
		publishOperation := publicationOperationID("publish", recovery.task)
		publishIdentity := store.ExternalEffectIdentity{Kind: "publisher.publish", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: publishOperation}
		if verification.Outcome == store.PublicationVerifiedExact || verification.Outcome == store.PublicationDeliveredSuperseded {
			reconciled := publisherservice.PublicationPublishResponse{
				OperationID: publishOperation, RequestDigest: publication.PublishReceipt.RequestDigest,
				Receipt: publisher.PublishReceipt{
					PublicationID: publication.ID, PublicationGeneration: publication.Generation,
					OperationID: publishOperation, RequestDigest: publication.PublishReceipt.RequestDigest, Outcome: publisher.PublishAcknowledged,
					TargetRepositoryID: recovery.target.ID, TargetRef: publication.TargetRef,
					RemoteBefore: publisherRemoteRef(publication.Baseline), ExpectedCommitOID: publication.PreparedReceipt.CommitSHA,
					ObservedRemote: publisher.RemoteRef{OID: publication.PreparedReceipt.CommitSHA},
				},
			}
			if err := settleACPExternalEffect(ctx, d, recovery.fence, publishIdentity, store.ExternalEffectSucceeded, reconciled); err != nil {
				return nil, err
			}
		} else if err := settleACPExternalEffect(ctx, d, recovery.fence, publishIdentity, store.ExternalEffectOutcomeUnknown, nil); err != nil {
			return nil, err
		}
	}
	if reason == "" && recovery.workspace.CreatePR && publication.PRIntent == nil {
		baseRef, baseErr := canonicalWorkspaceBranchRef(recovery.workspace.PRBaseBranch)
		if baseErr != nil {
			return nil, baseErr
		}
		intent := store.PullRequestIntent{
			BaseRepositoryID: recovery.pullRequestBase.ID, BaseRef: baseRef,
			HeadRepositoryID: recovery.target.ID, HeadRef: publication.TargetRef,
			PublicationGeneration: publication.Generation, ExpectedHeadSHA: verification.ObservedRemote.SHA,
		}
		intentOperation := publicationOperationID("pr-intent", recovery.task)
		intentDigest, digestErr := acpDomainDigest("publication-pr-intent", map[string]any{"publicationID": publication.ID, "intent": intent})
		if digestErr != nil {
			return nil, digestErr
		}
		updatedPublication, setErr := d.Store.SetPublicationPRIntent(ctx, store.SetPublicationPRIntentRequest{
			ID: publication.ID, Fence: recovery.fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
			ExpectedState: store.PublicationVerifying, Intent: intent, OperationID: intentOperation,
			OperationDigest: intentDigest, UpdatedAt: time.Now().UTC(),
		})
		if setErr != nil {
			return nil, setErr
		}
		publication = updatedPublication
	}
	if reason == "" && publication.PRIntent != nil && publication.PullRequestReceipt == nil {
		var err error
		publication, verification, reason, err = d.recoverPublicationPullRequest(ctx, publication, verification, operation, recovery)
		if err != nil {
			return nil, err
		}
	}
	return d.transitionPublication(ctx, publication, recovery.fence, verification.Outcome,
		verification.OperationID, verification.RequestDigest, nil, nil, verification, reason)
}

func (d *ACPDispatcher) recoverPublicationPullRequest(
	ctx context.Context,
	publication *store.Publication,
	verification *store.PublicationVerificationReceipt,
	verificationOperation string,
	recovery persistedPublicationRecoveryContext,
) (*store.Publication, *store.PublicationVerificationReceipt, string, error) {
	prOperation := publicationOperationID("pr-reconcile", recovery.task)
	prRequest := publisherservice.PullRequestReconcileRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: recovery.task.Namespace, PublicationID: publication.ID, OperationID: prOperation},
		CredentialRef: recovery.forgeCredential,
		Intent: publisher.PullRequestIntent{
			BaseRepository: recovery.pullRequestBase, BaseRef: publication.PRIntent.BaseRef,
			HeadRepository: recovery.target, HeadRef: publication.PRIntent.HeadRef,
			PublicationGeneration: publication.Generation, ExpectedHeadOID: publication.PRIntent.ExpectedHeadSHA,
			SessionUID: publication.SessionUID,
		},
	}
	prResponse, err := runACPExternalEffect(ctx, d, recovery.fence, store.ExternalEffectIdentity{
		Kind: "publisher.pull-request", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: prOperation,
	}, prRequest, func(callCtx context.Context) (publisherservice.PullRequestReconcileResponse, error) {
		return d.Publisher.ReconcilePullRequest(callCtx, prRequest)
	})
	if err != nil {
		if settleErr := settleACPExternalEffect(ctx, d, recovery.fence, store.ExternalEffectIdentity{
			Kind: "publisher.pull-request", Namespace: recovery.task.Namespace, AggregateID: publication.ID, OperationID: prOperation,
		}, store.ExternalEffectOutcomeUnknown, nil); settleErr != nil {
			return nil, nil, "", settleErr
		}
		return publication, &store.PublicationVerificationReceipt{
			OperationID: verificationOperation, RequestDigest: mustACPDomainDigest("publisher-pr-unknown", publication.ID),
			Outcome: store.PublicationOutcomeUnknown, ExpectedCommitSHA: publication.PreparedReceipt.CommitSHA,
			ObservedRemote: store.RemoteRefState{}, VerifiedAt: time.Now().UTC(),
		}, "pull request reconciliation outcome could not be established after restart", nil
	}
	receipt := store.PullRequestOperationReceipt{
		OperationID: prResponse.OperationID, RequestDigest: prResponse.RequestDigest,
		IntentKey: prResponse.Receipt.IntentKey, ForgeID: prResponse.Receipt.ForgeID,
		URL: prResponse.Receipt.URL, State: string(prResponse.Receipt.State), HeadSHA: prResponse.Receipt.HeadOID,
		ReconciledAt: time.Now().UTC(),
	}
	receiptOp := publicationOperationID("pr-receipt", recovery.task)
	receiptDigest, err := acpDomainDigest("publication-pr-receipt", map[string]any{"publicationID": publication.ID, "receipt": receipt})
	if err != nil {
		return nil, nil, "", err
	}
	publication, err = d.Store.SetPublicationPRReceipt(ctx, store.SetPublicationPRReceiptRequest{
		ID: publication.ID, Fence: recovery.fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: store.PublicationVerifying, Receipt: receipt,
		OperationID: receiptOp, OperationDigest: receiptDigest, UpdatedAt: receipt.ReconciledAt,
	})
	return publication, verification, "", err
}

func (d *ACPDispatcher) finishPersistedPublicationRecovery(
	ctx context.Context,
	publication *store.Publication,
	recovery persistedPublicationRecoveryContext,
) (acpPublicationResult, error) {
	if !store.IsTerminalPublicationState(publication.State) {
		return acpPublicationResult{}, fmt.Errorf("publication reconciliation did not reach a terminal state")
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, recovery.attemptID)
	if err != nil {
		return acpPublicationResult{}, err
	}
	terminalDelivery := promptDeliveryForPublication(publication.State)
	if attempt.DeliveryState != terminalDelivery {
		if err := d.transitionDelivery(ctx, recovery.attemptID, recovery.fence, attempt.DeliveryState, terminalDelivery, "recover-publication-terminal", publication.TerminalReason); err != nil {
			return acpPublicationResult{}, err
		}
	}
	if err := d.reclaimStandaloneTaskBranchClaim(ctx, recovery.task, recovery.attemptID, recovery.fence, publication); err != nil {
		return acpPublicationResult{}, err
	}
	delta := harnessv2.WorkspaceDeltaDescriptor{
		State: harnessv2.WorkspaceDeltaPrepared, Intent: harnessv2.WorkspaceIntentWrite,
		VerifiedBaseline: harnessv2.WorkspaceBaseline{RepositoryIdentity: publication.SourceRepositoryID, Revision: publication.SourceBaselineSHA},
		RelativeRoot:     strings.TrimSpace(recovery.workspace.SubPath), Artifact: &recovery.artifact,
		PublicationSafe: true, NoFollowVerified: true,
	}
	status := publicationTaskDeliveryStatus(recovery.workspace, delta.VerifiedBaseline, delta, publication, strings.TrimPrefix(publication.TargetRef, "refs/heads/"))
	return acpPublicationResult{PublicationID: publication.ID, Status: status}, nil
}

func (d *ACPDispatcher) recoveryDeliveryTransition(ctx context.Context, attemptID string, fence store.ControllerEpochFence, from, to store.PromptDeliveryState, operation string) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if attempt.DeliveryState == to || store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return nil
	}
	if attempt.DeliveryState != from {
		return fmt.Errorf("publication recovery delivery state is %s, want %s", attempt.DeliveryState, from)
	}
	return d.transitionDelivery(ctx, attemptID, fence, from, to, operation, "")
}
