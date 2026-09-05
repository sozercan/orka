package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/publisher"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/taskterminal"
)

const (
	acpPublicationGeneration int64 = 1
	acpNoWorkspaceRevision         = taskterminal.NoWorkspaceRevision
)

type acpPublicationResult struct {
	PublicationID string
	Status        corev1alpha1.TaskDeliveryStatus
}

// publishWorkspaceDelta runs the clean-room prepare/publish/verify transaction
// for one immutable, already-durable workspace delta. The ACP child never
// controls the Git repository, branch, commit, credential, or request identity
// used by this path.
func (d *ACPDispatcher) publishWorkspaceDelta(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	baseline harnessv2.WorkspaceBaseline,
	delta harnessv2.WorkspaceDeltaDescriptor,
	session *acpTaskSession,
) (acpPublicationResult, error) {
	ctx, publicationTrace := startACPPublicationSpan(ctx, task, publicationIDForTask(task), false)
	result, err := d.publishWorkspaceDeltaOperation(ctx, task, attemptID, fence, baseline, delta, session)
	publicationTrace.End(err)
	return result, err
}

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func (d *ACPDispatcher) publishWorkspaceDeltaOperation(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	baseline harnessv2.WorkspaceBaseline,
	delta harnessv2.WorkspaceDeltaDescriptor,
	session *acpTaskSession,
) (acpPublicationResult, error) {
	// The delivery context bounds the sequential pre-publish publisher stages
	// (branch-claim refresh, preflight, prepare); the detached settlement
	// context created after the publishing CAS separately bounds publish,
	// verify, and PR reconciliation. Both windows budget
	// maxSequentialPublisherSettlementStages full publisher calls.
	deliveryCtx, cancelDelivery := context.WithTimeout(context.WithoutCancel(ctx), publicationSettlementWindow())
	defer cancelDelivery()
	ctx = deliveryCtx
	if d.Publisher == nil {
		return acpPublicationResult{}, fmt.Errorf("clean-room Workspace/Publisher is required")
	}
	workspace := task.Spec.Workspace
	if workspace == nil || workspace.Intent != corev1alpha1.WorkspaceIntentWrite {
		return acpPublicationResult{}, fmt.Errorf("non-empty workspace publication requires intent=write")
	}
	if delta.Artifact == nil || !delta.PublicationSafe || !delta.NoFollowVerified {
		return acpPublicationResult{}, fmt.Errorf("workspace delta is not publication-safe")
	}
	relativeRoot := strings.TrimSpace(workspace.SubPath)
	if delta.RelativeRoot != relativeRoot {
		return acpPublicationResult{}, fmt.Errorf("workspace delta relative root does not match the Task workspace subpath")
	}
	if workspace.PublicationCredentialRef == nil || strings.TrimSpace(workspace.PublicationCredentialRef.Name) == "" {
		return acpPublicationResult{}, &acpDeliveryError{
			state: corev1alpha1.TaskDeliveryStateCredentialBlocked, outcome: corev1alpha1.TaskDeliveryOutcomeCredentialBlocked,
			reason: "PublicationCredentialMissing", message: "write workspace requires a publication credential reference",
		}
	}
	if workspace.CreatePR && (workspace.ForgeCredentialRef == nil || strings.TrimSpace(workspace.ForgeCredentialRef.Name) == "") {
		return acpPublicationResult{}, &acpDeliveryError{
			state: corev1alpha1.TaskDeliveryStateCredentialBlocked, outcome: corev1alpha1.TaskDeliveryOutcomeCredentialBlocked,
			reason: "ForgeCredentialMissing", message: "pull request reconciliation requires a forge-only credential reference",
		}
	}

	source, err := workspaceRepository(workspace)
	if err != nil {
		return acpPublicationResult{}, err
	}
	pullRequestBase := source
	target, err := workspacePublicationRepository(workspace)
	if err != nil {
		return acpPublicationResult{}, err
	}
	if _, err := workspaceSourceRef(workspace); err != nil {
		return acpPublicationResult{}, err
	}
	// Publication preparation is pinned to the verified immutable revision. The
	// original selector may have been a bare branch or tag and is not durable
	// enough to reconstruct later publish/verify requests safely.
	sourceRef := baseline.Revision
	sourceCredentialRef := workspace.ReadCredentialRef
	sourceCredentialRole := publisherservice.CredentialRoleSourceRead
	if session != nil && session.VerifiedBaseline != nil {
		source = target
		sourceRef = session.VerifiedBaseline.SHA
		sourceCredentialRef = workspace.PublicationReadCredentialRef
		sourceCredentialRole = publisherservice.CredentialRoleTargetRead
	}
	branch := publicationBranch(task, session, workspace)
	targetRef := "refs/heads/" + branch
	ownerKind := store.BranchClaimOwnerTask
	ownerUID := string(task.UID)
	sessionUID := ""
	if session != nil {
		ownerKind = store.BranchClaimOwnerSession
		ownerUID = session.Binding.SessionUID
		sessionUID = session.Binding.SessionUID
	}
	claimIncarnation := publicationID(task)
	expectedRemote, err := expectedPublicationRemoteState(workspace, session, target, targetRef)
	if err != nil {
		return acpPublicationResult{}, err
	}
	currentClaimDigest, err := branchClaimRequestDigest(target.ID, targetRef, ownerKind, ownerUID, expectedRemote, claimIncarnation)
	if err != nil {
		return acpPublicationResult{}, err
	}

	claim, _, err := d.ensureBranchClaim(ctx, fence, target, targetRef, ownerKind, ownerUID, expectedRemote, claimIncarnation)
	if err != nil {
		return acpPublicationResult{}, err
	}
	if claim.Availability != store.BranchClaimAvailable {
		return acpPublicationResult{}, &acpDeliveryError{
			state: corev1alpha1.TaskDeliveryStateDeliveryConflict, outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
			reason: "BranchClaimBlocked", message: "publication branch is reconciliation-blocked",
		}
	}

	sourceCredential := publisherCredentialReference(sourceCredentialRef, sourceCredentialRole)
	targetReadCredential := publisherCredentialReference(workspace.PublicationReadCredentialRef, publisherservice.CredentialRoleTargetRead)
	writeCredential := publisherCredentialReference(workspace.PublicationCredentialRef, publisherservice.CredentialRoleTargetWrite)
	refreshedClaim, err := d.refreshBranchClaimBaseline(ctx, task, fence, target, claim, expectedRemote, targetReadCredential)
	if err != nil {
		return acpPublicationResult{}, d.withUnpublishedBranchClaimCleanup(ctx, fence, claim, currentClaimDigest, publicationID(task), err)
	}
	claim = refreshedClaim
	preflightOperation := publicationOperationID("preflight", task)
	preflightRequest := publisherservice.PublicationPreflightRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publicationID(task), OperationID: preflightOperation},
		CredentialRef: targetReadCredential,
		Request:       publisher.PreflightRequest{Target: target, Claim: publisherBranchClaim(*claim)},
	}
	preflightIdentity := store.ExternalEffectIdentity{
		Kind: "publisher.preflight", Namespace: task.Namespace, AggregateID: publicationID(task), OperationID: preflightOperation,
	}
	preflight, err := runACPExternalEffect(ctx, d, fence, preflightIdentity, preflightRequest, func(callCtx context.Context) (publisherservice.PublicationPreflightResponse, error) {
		return d.Publisher.PreflightPublication(callCtx, preflightRequest)
	})
	if err != nil {
		if settleErr := settleACPExternalEffectError(ctx, d, fence, preflightIdentity, err); settleErr != nil {
			return acpPublicationResult{}, d.withUnpublishedBranchClaimCleanup(
				ctx, fence, claim, currentClaimDigest, publicationID(task), fmt.Errorf("settle failed publication preflight effect: %w", settleErr),
			)
		}
		return acpPublicationResult{}, d.withUnpublishedBranchClaimCleanup(
			ctx, fence, claim, currentClaimDigest, publicationID(task), classifyPublisherDeliveryError("PublicationPreflightFailed", err),
		)
	}
	if !preflight.Result.Matches || !preflight.Result.Observed.Equal(publisherRemoteRef(claim.LastVerified)) {
		return acpPublicationResult{}, d.withUnpublishedBranchClaimCleanup(ctx, fence, claim, currentClaimDigest, publicationID(task), &acpDeliveryError{
			state: corev1alpha1.TaskDeliveryStateDeliveryConflict, outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
			reason: "BranchMoved", message: "publication branch no longer matches its verified BranchClaim baseline",
		})
	}

	publication, err := d.ensurePublication(ctx, task, attemptID, fence, sessionUID, source, sourceRef, target, targetRef, *claim, delta)
	if err != nil {
		durable, getErr := d.Store.GetPublication(ctx, publicationID(task))
		switch {
		case getErr == nil && publicationMatchesCreation(durable, task, attemptID, sessionUID, source, sourceRef, target, targetRef, claim, delta):
			publication = durable
		case errors.Is(getErr, store.ErrNotFound):
			return acpPublicationResult{}, d.withUnpublishedBranchClaimCleanup(
				ctx, fence, claim, currentClaimDigest, publicationID(task), err,
			)
		case getErr != nil:
			return acpPublicationResult{}, errors.Join(err, getErr)
		default:
			return acpPublicationResult{}, errors.Join(err, fmt.Errorf("persisted Publication identity drifted after create error"))
		}
	}
	if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryValidating, store.PromptDeliveryPreparing, "publication-preparing", ""); err != nil {
		return acpPublicationResult{}, err
	}
	prepareOperation := publicationOperationID("prepare", task)
	prepareRequest := publisherservice.PublicationPrepareRequest{
		Metadata:            publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publication.ID, OperationID: prepareOperation},
		SourceCredentialRef: sourceCredential,
		DeltaArtifact:       *delta.Artifact,
		Request: publisher.PrepareRequest{
			PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: prepareOperation,
			Source: source, SourceRef: sourceRef, Target: target, TargetRef: targetRef,
			BranchClaimGeneration: claim.Generation, BaselineOID: baseline.Revision,
			RemoteBefore: publisherRemoteRef(claim.LastVerified), DeltaArtifactDigest: delta.Artifact.Digest,
			RelativeRoot:  delta.RelativeRoot,
			CommitMessage: publication.CommitMessage, CommitTimestamp: publication.CommitTimestamp,
		},
	}
	prepareIdentity := store.ExternalEffectIdentity{
		Kind: "publisher.prepare", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: prepareOperation,
	}
	preparedResponse, err := runACPExternalEffectWithRetry(ctx, d, fence, prepareIdentity, prepareRequest, func(callCtx context.Context) (publisherservice.PublicationPrepareResponse, error) {
		return d.Publisher.PreparePublication(callCtx, prepareRequest)
	})
	if err != nil {
		if settleErr := settleACPExternalEffectError(ctx, d, fence, prepareIdentity, err); settleErr != nil {
			return acpPublicationResult{PublicationID: publication.ID}, fmt.Errorf("settle failed publication prepare effect: %w", settleErr)
		}
		if retryableACPExternalEffectError(err) {
			reason := "publication preparation outcome could not be established within the reconciliation bound"
			publicationID := publication.ID
			if transitionErr := d.transitionPublicationTerminal(ctx, publication, fence, store.PublicationOutcomeUnknown, reason); transitionErr != nil {
				return acpPublicationResult{PublicationID: publicationID}, transitionErr
			}
			publication, err = d.Store.GetPublication(ctx, publicationID)
			if err != nil {
				return acpPublicationResult{PublicationID: publicationID}, err
			}
			if transitionErr := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPreparing, store.PromptDeliveryPublicationOutcomeUnknown, "publication-prepare-unknown", reason); transitionErr != nil {
				return acpPublicationResult{PublicationID: publication.ID}, transitionErr
			}
			if reclaimErr := d.reclaimStandaloneTaskBranchClaim(ctx, task, attemptID, fence, publication); reclaimErr != nil {
				return acpPublicationResult{PublicationID: publication.ID}, reclaimErr
			}
			return acpPublicationResult{PublicationID: publication.ID}, &acpDeliveryError{
				state:   corev1alpha1.TaskDeliveryStatePublicationOutcomeUnknown,
				outcome: corev1alpha1.TaskDeliveryOutcomePublicationOutcomeUnknown,
				reason:  "PublicationPrepareOutcomeUnknown", message: reason,
			}
		}
		classified := classifyPublisherDeliveryError("PublicationPrepareFailed", err)
		terminalState := store.PublicationPreparationFailed
		deliveryState := store.PromptDeliveryConflict
		var deliveryErr *acpDeliveryError
		if errors.As(classified, &deliveryErr) && deliveryErr.state == corev1alpha1.TaskDeliveryStateCredentialBlocked {
			terminalState = store.PublicationCredentialBlocked
			deliveryState = store.PromptDeliveryCredentialBlocked
		}
		publicationID := publication.ID
		if transitionErr := d.transitionPublicationTerminal(ctx, publication, fence, terminalState, "publication preparation failed"); transitionErr != nil {
			return acpPublicationResult{PublicationID: publicationID}, transitionErr
		}
		publication, err = d.Store.GetPublication(ctx, publicationID)
		if err != nil {
			return acpPublicationResult{PublicationID: publicationID}, err
		}
		if transitionErr := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPreparing, deliveryState, "publication-prepare-failed", "publication preparation failed"); transitionErr != nil {
			return acpPublicationResult{PublicationID: publication.ID}, transitionErr
		}
		if reclaimErr := d.reclaimStandaloneTaskBranchClaim(ctx, task, attemptID, fence, publication); reclaimErr != nil {
			return acpPublicationResult{PublicationID: publication.ID}, reclaimErr
		}
		return acpPublicationResult{PublicationID: publication.ID}, classified
	}
	prepared, err := preparedReceiptFromPublisher(preparedResponse.Prepared)
	if err != nil {
		return acpPublicationResult{}, err
	}
	publication, err = d.transitionPublication(ctx, publication, fence, store.PublicationPrepared, prepareOperation, preparedResponse.RequestDigest, prepared, nil, nil, "")
	if err != nil {
		return acpPublicationResult{}, err
	}

	if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPreparing, store.PromptDeliveryPrepared, "publication-prepared", ""); err != nil {
		return acpPublicationResult{}, err
	}

	cancelCheckCtx, cancelCheck := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	cancelled, err := d.taskCancellationRequested(cancelCheckCtx, task)
	cancelCheck()
	if err != nil {
		return acpPublicationResult{}, err
	}
	if cancelled {
		won, cancelErr := d.cancelPreparedPublication(ctx, task)
		if cancelErr != nil {
			return acpPublicationResult{}, cancelErr
		}
		if !won {
			return acpPublicationResult{}, fmt.Errorf("publication cancellation lost before the publishing decision was observed")
		}
		publication, err = d.Store.GetPublication(ctx, publication.ID)
		if err != nil {
			return acpPublicationResult{}, err
		}
		if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPrepared, store.PromptDeliveryCancelledBeforePublish, "cancel-before-publish", ""); err != nil {
			return acpPublicationResult{}, err
		}
		if err := d.reclaimStandaloneTaskBranchClaim(ctx, task, attemptID, fence, publication); err != nil {
			return acpPublicationResult{}, err
		}
		return acpPublicationResult{PublicationID: publication.ID, Status: publicationTaskDeliveryStatus(workspace, baseline, delta, publication, branch)}, errACPPublicationCancelled
	}

	publication, err = d.transitionPublication(ctx, publication, fence, store.PublicationPublishing,
		publicationOperationID("publishing", task), mustACPDomainDigest("publication-publishing", publication.ID), nil, nil, nil, "")
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			latest, getErr := d.Store.GetPublication(ctx, publication.ID)
			if getErr == nil && latest.State == store.PublicationCancelledBeforePublish {
				if deliveryErr := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPrepared, store.PromptDeliveryCancelledBeforePublish, "cancel-before-publish", ""); deliveryErr != nil {
					return acpPublicationResult{}, deliveryErr
				}
				if reclaimErr := d.reclaimStandaloneTaskBranchClaim(ctx, task, attemptID, fence, latest); reclaimErr != nil {
					return acpPublicationResult{}, reclaimErr
				}
				return acpPublicationResult{PublicationID: latest.ID, Status: publicationTaskDeliveryStatus(workspace, baseline, delta, latest, branch)}, errACPPublicationCancelled
			}
		}
		return acpPublicationResult{}, err
	}
	if err := d.transitionDelivery(ctx, attemptID, fence, store.PromptDeliveryPrepared, store.PromptDeliveryPublishing, "publication-publishing", ""); err != nil {
		return acpPublicationResult{}, err
	}
	// Publishing won the durable CAS. Task cancellation may be recorded by the
	// caller, but it must not abort exact-commit publication or independent
	// remote verification.
	settlementCtx, cancelSettlement := context.WithTimeout(context.WithoutCancel(ctx), publicationSettlementWindow())
	defer cancelSettlement()

	publishOperation := publicationOperationID("publish", task)
	publishRequest := publisher.PublishRequest{
		PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: publishOperation,
		Target: target, TargetRef: targetRef, Claim: publisherBranchClaim(*claim), RemoteBefore: publisherRemoteRef(claim.LastVerified),
		ExpectedCommitOID: prepared.CommitSHA, BundleDigest: prepared.BundleDigest,
	}
	preparedTransport := publisherPreparedFromPublication(publication, source, target)
	publishServiceRequest := publisherservice.PublicationPublishRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publication.ID, OperationID: publishOperation},
		CredentialRef: writeCredential, Prepared: preparedTransport, Request: publishRequest,
	}
	publishResponse, publishErr := runACPExternalEffect(settlementCtx, d, fence, store.ExternalEffectIdentity{
		Kind: "publisher.publish", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: publishOperation,
	}, publishServiceRequest, func(callCtx context.Context) (publisherservice.PublicationPublishResponse, error) {
		return d.Publisher.Publish(callCtx, publishServiceRequest)
	})
	publishReceipt := &store.PublishOperationReceipt{
		OperationID: publishOperation, RequestDigest: mustACPDomainDigest("publisher-publish", publishRequest),
		TargetRepositoryID: target.ID, TargetRef: targetRef, RemoteBefore: claim.LastVerified,
		ExpectedCommitSHA: prepared.CommitSHA, AcknowledgementUnknown: publishErr != nil, PublishedAt: time.Now().UTC(),
	}
	if publishErr == nil {
		publishReceipt.OperationID = publishResponse.Receipt.OperationID
		publishReceipt.RequestDigest = publishResponse.Receipt.RequestDigest
		publishReceipt.AcknowledgementUnknown = publishResponse.Receipt.Outcome == publisher.PublishOutcomeUnknown
	}
	publication, err = d.transitionPublication(settlementCtx, publication, fence, store.PublicationVerifying,
		publishReceipt.OperationID, publishReceipt.RequestDigest, nil, publishReceipt, nil, "")
	if err != nil {
		return acpPublicationResult{}, err
	}
	if err := d.transitionDelivery(settlementCtx, attemptID, fence, store.PromptDeliveryPublishing, store.PromptDeliveryVerifying, "publication-verifying", ""); err != nil {
		return acpPublicationResult{}, err
	}

	verifyOperation := publicationOperationID("verify", task)
	verifyRequest := publisherservice.PublicationVerifyRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publication.ID, OperationID: verifyOperation},
		CredentialRef: targetReadCredential, Prepared: preparedTransport,
		Request: publisher.VerifyRequest{
			PublicationID: publication.ID, PublicationGeneration: publication.Generation, OperationID: verifyOperation,
			Target: target, TargetRef: targetRef, Claim: publisherBranchClaim(*claim), ExpectedCommitOID: prepared.CommitSHA,
			BundleDigest: prepared.BundleDigest,
		},
	}
	verifyResponse, verifyErr := runACPExternalEffect(settlementCtx, d, fence, store.ExternalEffectIdentity{
		Kind: "publisher.verify", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: verifyOperation,
	}, verifyRequest, func(callCtx context.Context) (publisherservice.PublicationVerifyResponse, error) {
		return d.Publisher.Verify(callCtx, verifyRequest)
	})
	verification := &store.PublicationVerificationReceipt{
		OperationID: verifyOperation, RequestDigest: mustACPDomainDigest("publisher-verify-unknown", publication.ID),
		Outcome: store.PublicationOutcomeUnknown, ExpectedCommitSHA: prepared.CommitSHA,
		ObservedRemote: store.RemoteRefState{}, VerifiedAt: time.Now().UTC(),
	}
	terminalReason := ""
	if verifyErr == nil {
		verification = storeVerificationReceipt(verifyResponse.Receipt)
		switch verification.Outcome {
		case store.PublicationDeliveryConflict:
			terminalReason = "publication remote state conflicts with the durable BranchClaim"
		case store.PublicationOutcomeUnknown:
			terminalReason = "publication remote outcome could not be independently observed"
		}
	} else {
		terminalReason = "publication remote outcome could not be independently observed"
		if err := settleACPExternalEffect(settlementCtx, d, fence, store.ExternalEffectIdentity{
			Kind: "publisher.verify", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: verifyOperation,
		}, store.ExternalEffectOutcomeUnknown, nil); err != nil {
			return acpPublicationResult{}, err
		}
	}
	if publishErr != nil {
		publishIdentity := store.ExternalEffectIdentity{Kind: "publisher.publish", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: publishOperation}
		if verification.Outcome == store.PublicationVerifiedExact || verification.Outcome == store.PublicationDeliveredSuperseded {
			reconciled := publisherservice.PublicationPublishResponse{
				OperationID: publishOperation, RequestDigest: publishReceipt.RequestDigest,
				Receipt: publisher.PublishReceipt{
					PublicationID: publication.ID, PublicationGeneration: publication.Generation,
					OperationID: publishOperation, RequestDigest: publishReceipt.RequestDigest, Outcome: publisher.PublishAcknowledged,
					TargetRepositoryID: target.ID, TargetRef: targetRef, RemoteBefore: publisherRemoteRef(claim.LastVerified),
					ExpectedCommitOID: prepared.CommitSHA, ObservedRemote: publisher.RemoteRef{OID: prepared.CommitSHA},
				},
			}
			if err := settleACPExternalEffect(settlementCtx, d, fence, publishIdentity, store.ExternalEffectSucceeded, reconciled); err != nil {
				return acpPublicationResult{}, err
			}
		} else if err := settleACPExternalEffect(settlementCtx, d, fence, publishIdentity, store.ExternalEffectOutcomeUnknown, nil); err != nil {
			return acpPublicationResult{}, err
		}
	}
	var prReceipt *corev1alpha1.TaskPullRequestReceipt
	if verifyErr == nil && workspace.CreatePR &&
		(verification.Outcome == store.PublicationVerifiedExact || verification.Outcome == store.PublicationDeliveredSuperseded) {
		if publication.PRIntent == nil {
			baseRef, baseErr := canonicalWorkspaceBranchRef(workspace.PRBaseBranch)
			if baseErr != nil {
				return acpPublicationResult{}, baseErr
			}
			intent := store.PullRequestIntent{
				BaseRepositoryID: pullRequestBase.ID, BaseRef: baseRef, HeadRepositoryID: target.ID, HeadRef: targetRef,
				PublicationGeneration: publication.Generation, ExpectedHeadSHA: verification.ObservedRemote.SHA,
			}
			intentOperation := publicationOperationID("pr-intent", task)
			intentDigest, digestErr := acpDomainDigest("publication-pr-intent", map[string]any{"publicationID": publication.ID, "intent": intent})
			if digestErr != nil {
				return acpPublicationResult{}, digestErr
			}
			publication, err = d.Store.SetPublicationPRIntent(settlementCtx, store.SetPublicationPRIntentRequest{
				ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
				ExpectedState: store.PublicationVerifying, Intent: intent, OperationID: intentOperation,
				OperationDigest: intentDigest, UpdatedAt: time.Now().UTC(),
			})
			if err != nil {
				return acpPublicationResult{}, err
			}
		}
		forgeIntent := publisher.PullRequestIntent{
			BaseRepository: pullRequestBase, BaseRef: publication.PRIntent.BaseRef,
			HeadRepository: target, HeadRef: publication.PRIntent.HeadRef,
			PublicationGeneration: publication.Generation, ExpectedHeadOID: publication.PRIntent.ExpectedHeadSHA,
			SessionUID: publication.SessionUID,
		}
		prOperation := publicationOperationID("pr-reconcile", task)
		prRequest := publisherservice.PullRequestReconcileRequest{
			Metadata:      publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publication.ID, OperationID: prOperation},
			CredentialRef: publisherForgeCredentialReference(workspace.ForgeCredentialRef),
			Intent:        forgeIntent,
		}
		prResponse, prErr := runACPExternalEffect(settlementCtx, d, fence, store.ExternalEffectIdentity{
			Kind: "publisher.pull-request", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: prOperation,
		}, prRequest, func(callCtx context.Context) (publisherservice.PullRequestReconcileResponse, error) {
			return d.Publisher.ReconcilePullRequest(callCtx, prRequest)
		})
		if prErr != nil {
			if err := settleACPExternalEffect(settlementCtx, d, fence, store.ExternalEffectIdentity{
				Kind: "publisher.pull-request", Namespace: task.Namespace, AggregateID: publication.ID, OperationID: prOperation,
			}, store.ExternalEffectOutcomeUnknown, nil); err != nil {
				return acpPublicationResult{}, err
			}
			verification = &store.PublicationVerificationReceipt{
				OperationID: verifyOperation, RequestDigest: mustACPDomainDigest("publisher-pr-unknown", publication.ID),
				Outcome: store.PublicationOutcomeUnknown, ExpectedCommitSHA: prepared.CommitSHA,
				ObservedRemote: store.RemoteRefState{}, VerifiedAt: time.Now().UTC(),
			}
			terminalReason = "pull request reconciliation outcome could not be established"
		} else {
			storedReceipt := store.PullRequestOperationReceipt{
				OperationID: prResponse.OperationID, RequestDigest: prResponse.RequestDigest,
				IntentKey: prResponse.Receipt.IntentKey, ForgeID: prResponse.Receipt.ForgeID,
				URL: prResponse.Receipt.URL, State: string(prResponse.Receipt.State), HeadSHA: prResponse.Receipt.HeadOID,
				ReconciledAt: time.Now().UTC(),
			}
			receiptOperation := publicationOperationID("pr-receipt", task)
			receiptDigest, digestErr := acpDomainDigest("publication-pr-receipt", map[string]any{"publicationID": publication.ID, "receipt": storedReceipt})
			if digestErr != nil {
				return acpPublicationResult{}, digestErr
			}
			publication, err = d.Store.SetPublicationPRReceipt(settlementCtx, store.SetPublicationPRReceiptRequest{
				ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
				ExpectedState: store.PublicationVerifying, Receipt: storedReceipt,
				OperationID: receiptOperation, OperationDigest: receiptDigest, UpdatedAt: storedReceipt.ReconciledAt,
			})
			if err != nil {
				return acpPublicationResult{}, err
			}
			prReceipt = taskPullRequestReceipt(prResponse.Receipt, workspace, branch)
		}
	}
	publication, err = d.transitionPublication(settlementCtx, publication, fence, verification.Outcome,
		verification.OperationID, verification.RequestDigest, nil, nil, verification, terminalReason)
	if err != nil {
		return acpPublicationResult{}, err
	}
	if err := d.transitionDelivery(settlementCtx, attemptID, fence, store.PromptDeliveryVerifying, promptDeliveryForPublication(publication.State),
		"publication-verified", terminalReason); err != nil {
		return acpPublicationResult{}, err
	}

	status := publicationTaskDeliveryStatus(workspace, baseline, delta, publication, branch)
	status.PRReceipt = prReceipt
	if cancelledAfterPublish, cancelErr := d.taskCancellationRequested(settlementCtx, task); cancelErr == nil && cancelledAfterPublish {
		status.Reason = corev1alpha1.TaskDeliveryReasonCancellationRequestedAfterPublish
		status.Message = "cancellation was requested after publication won the durable CAS; delivery reconciliation completed"
	}
	result := acpPublicationResult{PublicationID: publication.ID, Status: status}
	if err := d.reclaimStandaloneTaskBranchClaim(settlementCtx, task, attemptID, fence, publication); err != nil {
		return acpPublicationResult{}, err
	}
	switch publication.State {
	case store.PublicationVerifiedExact, store.PublicationDeliveredSuperseded:
		return result, nil
	case store.PublicationDeliveryConflict:
		return result, &acpDeliveryError{state: status.State, outcome: status.Outcome, reason: "DeliveryConflict", message: "publication remote verification found a conflicting branch state"}
	default:
		return result, &acpDeliveryError{state: status.State, outcome: status.Outcome, reason: "PublicationOutcomeUnknown", message: "publication outcome remains unknown after bounded verification"}
	}
}

func canonicalWorkspaceBranchRef(branch string) (string, error) {
	branch = strings.TrimSpace(branch)
	if strings.HasPrefix(branch, "refs/heads/") {
		if err := store.ValidateFullBranchRef(branch); err != nil {
			return "", err
		}
		return branch, nil
	}
	ref := "refs/heads/" + branch
	if err := store.ValidateFullBranchRef(ref); err != nil {
		return "", err
	}
	return ref, nil
}

func taskPullRequestReceipt(receipt publisher.PullRequestReceipt, workspace *corev1alpha1.WorkspaceConfig, headBranch string) *corev1alpha1.TaskPullRequestReceipt {
	result := &corev1alpha1.TaskPullRequestReceipt{
		ID: receipt.ForgeID, URL: receipt.URL, State: string(receipt.State),
		BaseBranch: strings.TrimPrefix(workspace.PRBaseBranch, "refs/heads/"), HeadBranch: headBranch, HeadSHA: receipt.HeadOID,
	}
	if number, ok := pullRequestNumberFromForgeID(receipt.ForgeID); ok {
		result.Number = number
	}
	return result
}

func pullRequestNumberFromForgeID(forgeID string) (int64, bool) {
	parts := strings.Split(strings.TrimSpace(forgeID), ":")
	if len(parts) < 3 || parts[0] != "github" {
		return 0, false
	}
	// Parse with bitSize 32 so the number always fits platform int; real forge
	// pull-request numbers never exceed that bound, and anything larger is an
	// invalid forge identity.
	number, err := strconv.ParseInt(parts[len(parts)-1], 10, 32)
	return number, err == nil && number > 0
}

var errACPPublicationCancelled = errors.New("publication cancelled before publish")

type acpDeliveryError struct {
	state   corev1alpha1.TaskDeliveryState
	outcome corev1alpha1.TaskDeliveryOutcome
	reason  corev1alpha1.TaskDeliveryReason
	message string
}

func (e *acpDeliveryError) Error() string { return e.message }

func (d *ACPDispatcher) terminalizeDeliveryError(ctx context.Context, attemptID string, fence store.ControllerEpochFence, status corev1alpha1.TaskDeliveryStatus) error {
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return nil
	}
	target := store.PromptDeliveryConflict
	switch status.State {
	case corev1alpha1.TaskDeliveryStateCredentialBlocked:
		target = store.PromptDeliveryCredentialBlocked
	case corev1alpha1.TaskDeliveryStatePublicationOutcomeUnknown:
		target = store.PromptDeliveryPublicationOutcomeUnknown
	}
	if err := store.ValidatePromptDeliveryTransition(attempt.DeliveryState, target); err != nil {
		return err
	}
	return d.transitionDelivery(ctx, attemptID, fence, attempt.DeliveryState, target, "publication-terminal-error", status.Message)
}

func classifyPublisherDeliveryError(reason string, err error) error {
	state := corev1alpha1.TaskDeliveryStateDeliveryConflict
	outcome := corev1alpha1.TaskDeliveryOutcomeDeliveryConflict
	var clientErr *publisherservice.ClientError
	if errors.As(err, &clientErr) && (clientErr.StatusCode == 401 || clientErr.StatusCode == 403 || strings.Contains(clientErr.Response.Code, "credential")) {
		state = corev1alpha1.TaskDeliveryStateCredentialBlocked
		outcome = corev1alpha1.TaskDeliveryOutcomeCredentialBlocked
	}
	return &acpDeliveryError{state: state, outcome: outcome, reason: corev1alpha1.TaskDeliveryReason(reason), message: err.Error()}
}

func (d *ACPDispatcher) refreshBranchClaimBaseline(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	target publisher.Repository,
	claim *store.BranchClaim,
	expected store.RemoteRefState,
	credential *publisherservice.CredentialReference,
) (*store.BranchClaim, error) {
	if claim.LastVerified.Equal(expected) {
		return claim, nil
	}
	probe := *claim
	probe.LastVerified = expected
	operationID := publicationOperationID("claim-refresh", task)
	request := publisherservice.PublicationPreflightRequest{
		Metadata:      publisherservice.OperationMetadata{Namespace: task.Namespace, PublicationID: publicationID(task), OperationID: operationID},
		CredentialRef: credential,
		Request:       publisher.PreflightRequest{Target: target, Claim: publisherBranchClaim(probe)},
	}
	identity := store.ExternalEffectIdentity{Kind: "publisher.claim-refresh", Namespace: task.Namespace, AggregateID: publicationID(task), OperationID: operationID}
	response, err := runACPExternalEffect(ctx, d, fence, identity, request, func(callCtx context.Context) (publisherservice.PublicationPreflightResponse, error) {
		return d.Publisher.PreflightPublication(callCtx, request)
	})
	if err != nil {
		if settleErr := settleACPExternalEffectError(ctx, d, fence, identity, err); settleErr != nil {
			return nil, fmt.Errorf("settle branch-claim refresh effect: %w", settleErr)
		}
		return nil, classifyPublisherDeliveryError("BranchClaimRefreshFailed", err)
	}
	if !response.Result.Matches || !response.Result.Observed.Equal(publisherRemoteRef(expected)) {
		return nil, &acpDeliveryError{
			state: corev1alpha1.TaskDeliveryStateDeliveryConflict, outcome: corev1alpha1.TaskDeliveryOutcomeDeliveryConflict,
			reason: "BranchMoved", message: "publication branch does not match the requested exact remote head",
		}
	}
	digest := mustACPDomainDigest("branch-claim-refresh", map[string]any{"claim": claim.ID, "from": claim.LastVerified, "to": expected, "task": task.UID})
	updated, err := d.Store.CompareAndSwapBranchClaim(ctx, store.BranchClaimCAS{
		ID: claim.ID, Fence: fence,
		ExpectedVersion: claim.Version, ExpectedGeneration: claim.Generation, NewGeneration: claim.Generation,
		ExpectedLastVerified: claim.LastVerified, NewLastVerified: expected,
		ExpectedAvailability: claim.Availability, NewAvailability: store.BranchClaimAvailable,
		OperationID: operationID, OperationDigest: digest, UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func expectedPublicationRemoteState(
	workspace *corev1alpha1.WorkspaceConfig,
	session *acpTaskSession,
	target publisher.Repository,
	targetRef string,
) (store.RemoteRefState, error) {
	if session != nil && session.VerifiedBaseline != nil {
		baseline := session.VerifiedBaseline
		if err := validateSessionExpectedRemoteSHA(workspace, baseline); err != nil {
			return store.RemoteRefState{}, err
		}
		if !sameWorkspaceRepositoryIdentity(baseline.RepositoryID, target.ID) || baseline.Ref != targetRef {
			return store.RemoteRefState{}, fmt.Errorf("verified session baseline does not match the publication target")
		}
		return store.RemoteRefState{SHA: baseline.SHA}, nil
	}
	if workspace != nil && strings.TrimSpace(workspace.ExpectedRemoteSHA) != "" {
		return store.RemoteRefState{SHA: strings.TrimSpace(workspace.ExpectedRemoteSHA)}, nil
	}
	return store.RemoteRefState{Absent: true}, nil
}

func (d *ACPDispatcher) ensureBranchClaim(
	ctx context.Context,
	fence store.ControllerEpochFence,
	target publisher.Repository,
	targetRef string,
	ownerKind store.BranchClaimOwnerKind,
	ownerUID string,
	expected store.RemoteRefState,
	incarnation string,
) (*store.BranchClaim, bool, error) {
	id, err := store.CanonicalBranchClaimID(target.ID, targetRef)
	if err != nil {
		return nil, false, err
	}
	digest, err := branchClaimRequestDigest(target.ID, targetRef, ownerKind, ownerUID, expected, incarnation)
	if err != nil {
		return nil, false, err
	}
	legacyDigest, err := legacyBranchClaimRequestDigest(target.ID, targetRef, ownerKind, ownerUID, expected)
	if err != nil {
		return nil, false, err
	}
	existing, err := d.Store.GetBranchClaim(ctx, id)
	if err == nil {
		if existing.RepositoryID != target.ID || existing.Ref != targetRef || existing.OwnerKind != ownerKind || existing.OwnerUID != ownerUID {
			return nil, false, fmt.Errorf("publication branch is already claimed by a different owner")
		}
		if ownerKind == store.BranchClaimOwnerTask && existing.RequestDigest != digest {
			if existing.RequestDigest != legacyDigest || !d.persistedPublicationOwnsLegacyClaim(ctx, incarnation, existing, expected) {
				return nil, false, fmt.Errorf("publication branch is already claimed by a different owner")
			}
		}
		return existing, false, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	request := &store.BranchClaim{
		ID: id, RepositoryID: target.ID, Ref: targetRef, OwnerKind: ownerKind, OwnerUID: ownerUID,
		Generation: 1, LastVerified: expected, Availability: store.BranchClaimAvailable,
		RequestDigest: digest, CreatedAt: time.Now().UTC(),
	}
	if creator, ok := d.Store.(store.BranchClaimCreationStore); ok {
		return creator.CreateBranchClaimWithResult(ctx, request, fence)
	}
	claim, err := d.Store.CreateBranchClaim(ctx, request, fence)
	return claim, false, err
}

func (d *ACPDispatcher) persistedPublicationOwnsLegacyClaim(
	ctx context.Context,
	publicationID string,
	claim *store.BranchClaim,
	expected store.RemoteRefState,
) bool {
	if claim == nil || strings.TrimSpace(publicationID) == "" {
		return false
	}
	publication, err := d.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return false
	}
	return publication.ID == publicationID && publication.TaskUID == claim.OwnerUID &&
		publication.BranchClaimID == claim.ID && publication.BranchClaimGeneration == claim.Generation &&
		publication.TargetRepositoryID == claim.RepositoryID && publication.TargetRef == claim.Ref &&
		publication.Baseline.Equal(expected)
}

func (d *ACPDispatcher) withUnpublishedBranchClaimCleanup(
	ctx context.Context,
	fence store.ControllerEpochFence,
	claim *store.BranchClaim,
	expectedRequestDigest string,
	publicationID string,
	cause error,
) error {
	if claim == nil || claim.RequestDigest != expectedRequestDigest {
		return cause
	}
	if _, err := d.Store.GetPublication(ctx, publicationID); err == nil {
		return cause
	} else if !errors.Is(err, store.ErrNotFound) {
		return errors.Join(cause, fmt.Errorf("check unpublished branch claim: %w", err))
	}
	cleanupErr := d.Store.ReclaimBranchClaim(ctx, store.ReclaimBranchClaimRequest{
		ID: claim.ID, Fence: fence, ExpectedVersion: claim.Version, ExpectedGeneration: claim.Generation,
		ExpectedRepositoryID: claim.RepositoryID, ExpectedRef: claim.Ref,
		ExpectedOwnerKind: claim.OwnerKind, ExpectedOwnerUID: claim.OwnerUID,
		ExpectedLastVerified: claim.LastVerified, ExpectedAvailability: claim.Availability,
		ExpectedRequestDigest: claim.RequestDigest,
	})
	if cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("reclaim unpublished branch claim: %w", cleanupErr))
	}
	return cause
}

func branchClaimRequestDigest(
	repositoryID, ref string,
	ownerKind store.BranchClaimOwnerKind,
	ownerUID string,
	expected store.RemoteRefState,
	incarnation string,
) (string, error) {
	payload := map[string]any{
		"repository": repositoryID,
		"ref":        ref,
		"ownerKind":  ownerKind,
		"ownerUID":   ownerUID,
		"expected":   expected,
	}
	if strings.TrimSpace(incarnation) == "" {
		if ownerKind == store.BranchClaimOwnerTask {
			return "", fmt.Errorf("task-owned branch claim requires an immutable incarnation")
		}
	} else {
		payload["incarnation"] = incarnation
	}
	return acpDomainDigest("branch-claim", payload)
}

func legacyBranchClaimRequestDigest(
	repositoryID, ref string,
	ownerKind store.BranchClaimOwnerKind,
	ownerUID string,
	expected store.RemoteRefState,
) (string, error) {
	return acpDomainDigest("branch-claim", map[string]any{
		"repository": repositoryID,
		"ref":        ref,
		"ownerKind":  ownerKind,
		"ownerUID":   ownerUID,
		"expected":   expected,
	})
}

func (d *ACPDispatcher) ensurePublication(ctx context.Context, task *corev1alpha1.Task, attemptID string, fence store.ControllerEpochFence, sessionUID string, source publisher.Repository, sourceRef string, target publisher.Repository, targetRef string, claim store.BranchClaim, delta harnessv2.WorkspaceDeltaDescriptor) (*store.Publication, error) {
	id := publicationID(task)
	commitAt := task.CreationTimestamp.Time.UTC().Truncate(time.Second)
	if commitAt.IsZero() {
		commitAt = time.Now().UTC().Truncate(time.Second)
	}
	message := "orka: apply agent task " + task.Name + "\n"
	requestDigest, err := acpDomainDigest("publication", map[string]any{
		"id": id, "attemptID": attemptID, "source": source, "sourceRef": sourceRef, "target": target,
		"targetRef": targetRef, "branchClaim": claim.ID, "branchClaimGeneration": claim.Generation,
		"baseline": claim.LastVerified, "artifactDigest": delta.Artifact.Digest, "relativeRoot": delta.RelativeRoot,
		"commitMessage": message, "commitTimestamp": commitAt,
	})
	if err != nil {
		return nil, err
	}
	return d.Store.CreatePublication(ctx, &store.Publication{
		ID: id, Namespace: task.Namespace, Generation: acpPublicationGeneration,
		TaskUID: string(task.UID), Attempt: int64(task.Status.Execution.Attempt), PromptID: task.Status.Execution.PromptID,
		SessionUID: sessionUID, BranchClaimID: claim.ID, BranchClaimGeneration: claim.Generation,
		SourceRepositoryID: source.ID, SourceRef: sourceRef, SourceBaselineSHA: delta.VerifiedBaseline.Revision,
		TargetRepositoryID: target.ID, TargetRef: targetRef,
		Baseline: claim.LastVerified, ArtifactID: string(delta.Artifact.ArtifactID), ArtifactDigest: delta.Artifact.Digest,
		ArtifactSizeBytes: delta.Artifact.SizeBytes, ArtifactMediaType: delta.Artifact.MediaType,
		PublicationCredentialRef: task.Spec.Workspace.PublicationCredentialRef.Name,
		CommitIdentity:           publisher.CommitAuthorName + " <" + publisher.CommitAuthorEmail + ">",
		CommitMessage:            message, CommitTimestamp: commitAt, RequestDigest: requestDigest, CreatedAt: time.Now().UTC(),
	}, fence)
}

//nolint:gocyclo // Ambiguous-create recovery intentionally compares the complete immutable Publication identity in one audit point.
func publicationMatchesCreation(
	publication *store.Publication,
	task *corev1alpha1.Task,
	attemptID string,
	sessionUID string,
	source publisher.Repository,
	sourceRef string,
	target publisher.Repository,
	targetRef string,
	claim *store.BranchClaim,
	delta harnessv2.WorkspaceDeltaDescriptor,
) bool {
	if publication == nil || task == nil || task.Status.Execution == nil || claim == nil || delta.Artifact == nil {
		return false
	}
	commitAt := publication.CommitTimestamp
	if !task.CreationTimestamp.IsZero() {
		expectedCommitAt := task.CreationTimestamp.Time.UTC().Truncate(time.Second)
		if !commitAt.Equal(expectedCommitAt) {
			return false
		}
	}
	message := "orka: apply agent task " + task.Name + "\n"
	expectedDigest, err := acpDomainDigest("publication", map[string]any{
		"id": publicationIDForTask(task), "attemptID": attemptID, "source": source, "sourceRef": sourceRef, "target": target,
		"targetRef": targetRef, "branchClaim": claim.ID, "branchClaimGeneration": claim.Generation,
		"baseline": claim.LastVerified, "artifactDigest": delta.Artifact.Digest, "relativeRoot": delta.RelativeRoot,
		"commitMessage": message, "commitTimestamp": commitAt,
	})
	if err != nil {
		return false
	}
	credentialName := ""
	if task.Spec.Workspace != nil && task.Spec.Workspace.PublicationCredentialRef != nil {
		credentialName = strings.TrimSpace(task.Spec.Workspace.PublicationCredentialRef.Name)
	}
	return publication.ID == publicationIDForTask(task) && publication.Namespace == task.Namespace &&
		publication.Generation == acpPublicationGeneration &&
		publication.TaskUID == string(task.UID) && publication.Attempt == int64(task.Status.Execution.Attempt) &&
		publication.PromptID == task.Status.Execution.PromptID && publication.SessionUID == sessionUID &&
		publication.BranchClaimID == claim.ID && publication.BranchClaimGeneration == claim.Generation &&
		publication.SourceRepositoryID == source.ID && publication.SourceRef == sourceRef &&
		publication.TargetRepositoryID == target.ID && publication.TargetRef == targetRef &&
		publication.SourceBaselineSHA == delta.VerifiedBaseline.Revision && publication.Baseline.Equal(claim.LastVerified) &&
		publication.ArtifactID == string(delta.Artifact.ArtifactID) &&
		publication.ArtifactDigest == delta.Artifact.Digest && publication.ArtifactSizeBytes == delta.Artifact.SizeBytes &&
		publication.ArtifactMediaType == delta.Artifact.MediaType && publication.PublicationCredentialRef == credentialName &&
		publication.CommitIdentity == publisher.CommitAuthorName+" <"+publisher.CommitAuthorEmail+">" &&
		publication.CommitMessage == message && publication.RequestDigest == expectedDigest
}

func (d *ACPDispatcher) transitionPublication(ctx context.Context, publication *store.Publication, fence store.ControllerEpochFence, state store.PublicationState, operationID, operationDigest string, prepared *store.PreparedPublicationReceipt, published *store.PublishOperationReceipt, verified *store.PublicationVerificationReceipt, reason string) (*store.Publication, error) {
	return d.Store.TransitionPublication(ctx, store.PublicationTransition{
		ID: publication.ID, Fence: fence, ExpectedVersion: publication.Version, ExpectedGeneration: publication.Generation,
		ExpectedState: publication.State, NewState: state, OperationID: operationID, OperationDigest: operationDigest,
		PreparedReceipt: prepared, PublishReceipt: published, VerificationReceipt: verified,
		TerminalReason: reason, UpdatedAt: time.Now().UTC(),
	})
}

func (d *ACPDispatcher) transitionPublicationTerminal(ctx context.Context, publication *store.Publication, fence store.ControllerEpochFence, state store.PublicationState, reason string) error {
	_, err := d.transitionPublication(ctx, publication, fence, state, publicationOperationID("terminal", nil), mustACPDomainDigest("publication-terminal", map[string]any{"id": publication.ID, "state": state}), nil, nil, nil, reason)
	return err
}

func (d *ACPDispatcher) taskCancellationRequested(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	latest := &corev1alpha1.Task{}
	if err := d.APIReader.Get(ctx, clientObjectKey(task), latest); err != nil {
		return false, err
	}
	return !latest.DeletionTimestamp.IsZero() || latest.Status.Phase == corev1alpha1.TaskPhaseCancelled, nil
}

//nolint:gocyclo // Reclamation deliberately verifies every durable publication, attempt, effect, owner, and baseline fence together.
func (d *ACPDispatcher) reclaimStandaloneTaskBranchClaim(
	ctx context.Context,
	task *corev1alpha1.Task,
	attemptID string,
	fence store.ControllerEpochFence,
	publication *store.Publication,
) error {
	if publication == nil {
		return fmt.Errorf("publication is required for branch-claim reclamation")
	}
	if publication.SessionUID != "" {
		return nil
	}
	if task == nil || task.Status.Execution == nil {
		return fmt.Errorf("task execution identity is required for branch-claim reclamation")
	}
	if !store.IsTerminalPublicationState(publication.State) {
		return fmt.Errorf("publication %s is not terminal", publication.ID)
	}
	if publication.State == store.PublicationOutcomeUnknown {
		// An ambiguous remote outcome must retain its claim as a durable
		// fail-closed ownership barrier until explicit reconciliation.
		return nil
	}
	if publication.ID != publicationIDForTask(task) || publication.Namespace != task.Namespace ||
		publication.TaskUID != string(task.UID) || publication.Attempt != int64(task.Status.Execution.Attempt) ||
		publication.PromptID != task.Status.Execution.PromptID {
		return fmt.Errorf("publication %s does not match the immutable Task execution identity", publication.ID)
	}
	attemptKey := store.PromptAttemptKey{
		Namespace: publication.Namespace,
		TaskUID:   publication.TaskUID,
		Attempt:   publication.Attempt,
		PromptID:  publication.PromptID,
	}
	expectedAttemptID, err := attemptKey.CanonicalID()
	if err != nil {
		return err
	}
	if attemptID != expectedAttemptID {
		return fmt.Errorf("prompt attempt identity does not match publication %s", publication.ID)
	}
	attempt, err := d.Store.GetPromptAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	expectedDelivery := promptDeliveryForPublication(publication.State)
	if attempt.Key != attemptKey || !store.IsTerminalPromptExecutionState(attempt.ExecutionState) ||
		attempt.DeliveryState != expectedDelivery || !store.IsTerminalPromptDeliveryState(attempt.DeliveryState) {
		return fmt.Errorf("publication %s delivery has not settled at its exact terminal fence", publication.ID)
	}
	if err := d.ensurePublicationExternalEffectsSettled(ctx, task, publication); err != nil {
		return err
	}

	expectedClaimDigest, err := branchClaimRequestDigest(
		publication.TargetRepositoryID, publication.TargetRef, store.BranchClaimOwnerTask,
		publication.TaskUID, publication.Baseline, publication.ID,
	)
	if err != nil {
		return err
	}
	legacyClaimDigest, err := legacyBranchClaimRequestDigest(
		publication.TargetRepositoryID, publication.TargetRef, store.BranchClaimOwnerTask,
		publication.TaskUID, publication.Baseline,
	)
	if err != nil {
		return err
	}
	claim, err := d.Store.GetBranchClaim(ctx, publication.BranchClaimID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if claim.OwnerKind != store.BranchClaimOwnerTask || claim.OwnerUID != publication.TaskUID ||
		(claim.RequestDigest != expectedClaimDigest && claim.RequestDigest != legacyClaimDigest) {
		// The immutable Task-owned identity was already deleted and replaced.
		// Never let recovery for the old terminal Task reclaim the new owner.
		return nil
	}
	if claim.ID != publication.BranchClaimID || claim.RepositoryID != publication.TargetRepositoryID ||
		claim.Ref != publication.TargetRef || claim.Generation != publication.BranchClaimGeneration ||
		claim.Availability != store.BranchClaimAvailable || claim.BlockedReason != "" || claim.RelatedPublicationID != "" {
		return fmt.Errorf("branch claim %s does not match the terminal publication fence", claim.ID)
	}

	expectedBaseline := publication.Baseline
	if publication.State == store.PublicationVerifiedExact || publication.State == store.PublicationDeliveredSuperseded {
		verification := publication.VerificationReceipt
		if verification == nil || verification.Outcome != publication.State ||
			verification.ObservedRemote.Absent || verification.ObservedRemote.SHA == "" {
			return fmt.Errorf("verified publication lacks an exact observed remote branch head")
		}
		expectedBaseline = verification.ObservedRemote
		if !claim.LastVerified.Equal(expectedBaseline) {
			if !claim.LastVerified.Equal(publication.Baseline) {
				return fmt.Errorf("branch claim %s baseline drifted before terminal reclamation", claim.ID)
			}
			digest := mustACPDomainDigest("branch-claim-advance", map[string]any{
				"claim": claim.ID, "publication": publication.ID, "sha": expectedBaseline.SHA,
			})
			claim, err = d.Store.CompareAndSwapBranchClaim(ctx, store.BranchClaimCAS{
				ID: claim.ID, Fence: fence, ExpectedVersion: claim.Version,
				ExpectedGeneration: claim.Generation, NewGeneration: claim.Generation,
				ExpectedLastVerified: claim.LastVerified, NewLastVerified: expectedBaseline,
				ExpectedAvailability: claim.Availability, NewAvailability: store.BranchClaimAvailable,
				OperationID: publicationOperationID("claim-advance", nil), OperationDigest: digest, UpdatedAt: time.Now().UTC(),
			})
			if err != nil {
				return err
			}
		}
	} else if !claim.LastVerified.Equal(expectedBaseline) {
		return fmt.Errorf("branch claim %s baseline drifted before terminal reclamation", claim.ID)
	}

	return d.Store.ReclaimBranchClaim(ctx, store.ReclaimBranchClaimRequest{
		ID: claim.ID, Fence: fence, ExpectedVersion: claim.Version, ExpectedGeneration: claim.Generation,
		ExpectedRepositoryID: claim.RepositoryID, ExpectedRef: claim.Ref,
		ExpectedOwnerKind: store.BranchClaimOwnerTask, ExpectedOwnerUID: publication.TaskUID,
		ExpectedLastVerified: expectedBaseline, ExpectedAvailability: store.BranchClaimAvailable,
		ExpectedRequestDigest: claim.RequestDigest,
	})
}

func (d *ACPDispatcher) ensurePublicationExternalEffectsSettled(
	ctx context.Context,
	task *corev1alpha1.Task,
	publication *store.Publication,
) error {
	effects := []struct {
		kind      string
		operation string
	}{
		{kind: "publisher.claim-refresh", operation: "claim-refresh"},
		{kind: "publisher.preflight", operation: "preflight"},
		{kind: "publisher.prepare", operation: "prepare"},
		{kind: "publisher.publish", operation: "publish"},
		{kind: "publisher.verify", operation: "verify"},
		{kind: "publisher.pull-request", operation: "pr-reconcile"},
	}
	for _, candidate := range effects {
		identity := store.ExternalEffectIdentity{
			Kind: candidate.kind, Namespace: publication.Namespace, AggregateID: publication.ID,
			OperationID: publicationOperationID(candidate.operation, task),
		}
		id, err := identity.CanonicalID()
		if err != nil {
			return err
		}
		effect, err := d.Store.GetExternalEffect(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		switch effect.State {
		case store.ExternalEffectSucceeded, store.ExternalEffectFailed, store.ExternalEffectOutcomeUnknown:
		default:
			return fmt.Errorf("publication %s external effect %s has not settled", publication.ID, effect.ID)
		}
	}
	return nil
}

func publicationTaskDeliveryStatus(workspace *corev1alpha1.WorkspaceConfig, baseline harnessv2.WorkspaceBaseline, delta harnessv2.WorkspaceDeltaDescriptor, publication *store.Publication, branch string) corev1alpha1.TaskDeliveryStatus {
	status := corev1alpha1.TaskDeliveryStatus{
		PublicationID: publication.ID, Branch: branch, StartingSHA: baseline.Revision,
		ArtifactDigest: delta.Artifact.Digest, LastTransitionTime: nowMeta(),
	}
	if workspace.SourceRepository != nil {
		copyValue := *workspace.SourceRepository
		status.SourceRepository = &copyValue
	} else {
		status.SourceRepository = &corev1alpha1.RepositoryIdentity{Provider: workspaceRepositoryProviderGitHub, ID: publication.SourceRepositoryID}
	}
	if workspace.PublicationRepository != nil {
		copyValue := *workspace.PublicationRepository
		status.PublicationRepository = &copyValue
	} else {
		status.PublicationRepository = &corev1alpha1.RepositoryIdentity{Provider: workspaceRepositoryProviderGitHub, ID: publication.TargetRepositoryID}
	}
	if publication.Baseline.Absent {
		empty := ""
		status.RemoteBeforeSHA = &empty
	} else {
		value := publication.Baseline.SHA
		status.RemoteBeforeSHA = &value
	}
	if publication.PreparedReceipt != nil {
		status.TreeSHA = publication.PreparedReceipt.TreeSHA
		status.ExpectedCommitSHA = publication.PreparedReceipt.CommitSHA
	}
	if publication.VerificationReceipt != nil && !publication.VerificationReceipt.ObservedRemote.Absent {
		status.VerifiedRemoteSHA = publication.VerificationReceipt.ObservedRemote.SHA
	}
	if publication.PullRequestReceipt != nil && publication.PRIntent != nil {
		status.PRReceipt = &corev1alpha1.TaskPullRequestReceipt{
			ID: publication.PullRequestReceipt.ForgeID, URL: publication.PullRequestReceipt.URL,
			State:      publication.PullRequestReceipt.State,
			BaseBranch: strings.TrimPrefix(publication.PRIntent.BaseRef, "refs/heads/"),
			HeadBranch: strings.TrimPrefix(publication.PRIntent.HeadRef, "refs/heads/"),
			HeadSHA:    publication.PullRequestReceipt.HeadSHA,
		}
		if number, ok := pullRequestNumberFromForgeID(publication.PullRequestReceipt.ForgeID); ok {
			status.PRReceipt.Number = number
		}
	}
	switch publication.State {
	case store.PublicationVerifiedExact:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStateVerifiedExact, corev1alpha1.TaskDeliveryOutcomeVerifiedExact
	case store.PublicationDeliveredSuperseded:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStateDeliveredSuperseded, corev1alpha1.TaskDeliveryOutcomeDeliveredSuperseded
		status.SupersedingRemoteSHA = status.VerifiedRemoteSHA
	case store.PublicationCancelledBeforePublish:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStateCancelledBeforePublish, corev1alpha1.TaskDeliveryOutcomeCancelledBeforePublish
	case store.PublicationDeliveryConflict, store.PublicationPreparationFailed:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStateDeliveryConflict, corev1alpha1.TaskDeliveryOutcomeDeliveryConflict
	case store.PublicationCredentialBlocked:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStateCredentialBlocked, corev1alpha1.TaskDeliveryOutcomeCredentialBlocked
	default:
		status.State, status.Outcome = corev1alpha1.TaskDeliveryStatePublicationOutcomeUnknown, corev1alpha1.TaskDeliveryOutcomePublicationOutcomeUnknown
	}
	status.Message = publication.TerminalReason
	return status
}

func storeVerificationReceipt(receipt publisher.VerificationReceipt) *store.PublicationVerificationReceipt {
	return &store.PublicationVerificationReceipt{
		OperationID: receipt.OperationID, RequestDigest: receipt.RequestDigest,
		Outcome: store.PublicationState(receipt.Outcome), ExpectedCommitSHA: receipt.ExpectedCommitOID,
		ObservedRemote:        store.RemoteRefState{Absent: receipt.ObservedRemote.Absent, SHA: receipt.ObservedRemote.OID},
		DescendantProofDigest: receipt.DescendantProofDigest, VerifiedAt: time.Now().UTC(),
	}
}

func promptDeliveryForPublication(state store.PublicationState) store.PromptDeliveryState {
	switch state {
	case store.PublicationVerifiedExact:
		return store.PromptDeliveryVerifiedExact
	case store.PublicationDeliveredSuperseded:
		return store.PromptDeliveryDeliveredSuperseded
	case store.PublicationCancelledBeforePublish:
		return store.PromptDeliveryCancelledBeforePublish
	case store.PublicationDeliveryConflict, store.PublicationPreparationFailed:
		return store.PromptDeliveryConflict
	case store.PublicationCredentialBlocked:
		return store.PromptDeliveryCredentialBlocked
	default:
		return store.PromptDeliveryPublicationOutcomeUnknown
	}
}

func publisherBranchClaim(claim store.BranchClaim) publisher.BranchClaim {
	return publisher.BranchClaim{
		RepositoryID: claim.RepositoryID, Ref: claim.Ref, OwnerKind: string(claim.OwnerKind), OwnerUID: claim.OwnerUID,
		Generation: claim.Generation, LastVerified: publisherRemoteRef(claim.LastVerified),
	}
}

func publisherRemoteRef(remote store.RemoteRefState) publisher.RemoteRef {
	return publisher.RemoteRef{Absent: remote.Absent, OID: remote.SHA}
}

func workspacePublicationRepository(workspace *corev1alpha1.WorkspaceConfig) (publisher.Repository, error) {
	rawURL := strings.TrimSpace(workspace.PublicationGitRepo)
	if rawURL == "" {
		rawURL = strings.TrimSpace(workspace.GitRepo)
	}
	parsed, derivedID, err := canonicalWorkspaceRepositoryURL(rawURL)
	if err != nil {
		return publisher.Repository{}, fmt.Errorf("publication repository: %w", err)
	}
	provider := workspaceRepositoryProviderGitHub
	identity := workspace.PublicationRepository
	if identity == nil && rawURL == strings.TrimSpace(workspace.GitRepo) {
		identity = workspace.SourceRepository
	}
	if identity != nil {
		provider = strings.ToLower(strings.TrimSpace(identity.Provider))
		id := strings.TrimSpace(identity.ID)
		if provider != workspaceRepositoryProviderGitHub || !sameWorkspaceRepositoryIdentity(id, derivedID) {
			return publisher.Repository{}, fmt.Errorf("publicationRepository must match the canonical credential-free URL identity %q", derivedID)
		}
	}
	return publisher.Repository{Provider: provider, ID: derivedID, URL: parsed.String()}, nil
}

func workspaceSourceRef(workspace *corev1alpha1.WorkspaceConfig) (string, error) {
	if raw := strings.TrimSpace(workspace.Ref); raw != "" {
		canonical, err := publisherservice.CanonicalWorkspaceSourceRef(raw)
		if err != nil {
			return "", fmt.Errorf("workspace source ref is invalid: %w", err)
		}
		return canonical, nil
	}
	branch := strings.TrimSpace(workspace.Branch)
	if branch == "" {
		branch = "main"
	}
	if strings.HasPrefix(branch, "refs/heads/") {
		return branch, nil
	}
	if strings.HasPrefix(branch, "refs/") || strings.Contains(branch, "..") || strings.ContainsAny(branch, " ~^:?*[\\") {
		return "", fmt.Errorf("workspace source branch is invalid")
	}
	return "refs/heads/" + branch, nil
}

func publicationBranch(task *corev1alpha1.Task, session *acpTaskSession, workspace *corev1alpha1.WorkspaceConfig) string {
	if branch := strings.TrimSpace(workspace.PushBranch); branch != "" {
		return strings.TrimPrefix(branch, "refs/heads/")
	}
	if session != nil {
		return "orka/session-" + session.Binding.SessionUID
	}
	return "orka/task-" + string(task.UID)
}

func preparedReceiptFromPublisher(value publisherservice.PreparedPublication) (*store.PreparedPublicationReceipt, error) {
	if err := value.BundleArtifact.Validate(); err != nil || value.BundleArtifact.MediaType != store.PreparedBundleMediaType ||
		value.BundleArtifact.Digest != value.BundleDigest || value.BundleArtifact.SizeBytes != value.BundleSize {
		return nil, fmt.Errorf("publisher prepared bundle artifact is invalid")
	}
	return &store.PreparedPublicationReceipt{
		OperationID: value.OperationID, RequestDigest: value.RequestDigest,
		TreeSHA: value.TreeOID, CommitSHA: value.CommitOID, ManifestDigest: value.ManifestDigest, RelativeRoot: value.RelativeRoot,
		BundleArtifactID: string(value.BundleArtifact.ArtifactID), BundleDigest: value.BundleDigest,
		BundleSizeBytes: value.BundleSize, BundleMediaType: value.BundleArtifact.MediaType, BundleRef: value.BundleRef,
		PreparedAt: value.CommitTimestamp,
	}, nil
}

func publisherPreparedFromPublication(publication *store.Publication, source, target publisher.Repository) publisherservice.PreparedPublication {
	if publication == nil || publication.PreparedReceipt == nil {
		return publisherservice.PreparedPublication{}
	}
	prepared := publication.PreparedReceipt
	return publisherservice.PreparedPublication{
		PublicationID: publication.ID, PublicationGeneration: publication.Generation,
		OperationID: prepared.OperationID, RequestDigest: prepared.RequestDigest,
		Source: source, SourceRef: publication.SourceRef, Target: target, TargetRef: publication.TargetRef,
		BranchClaimGeneration: publication.BranchClaimGeneration, BaselineOID: publication.SourceBaselineSHA,
		RemoteBefore: publisherRemoteRef(publication.Baseline), DeltaArtifactDigest: publication.ArtifactDigest,
		RelativeRoot: prepared.RelativeRoot, ManifestDigest: prepared.ManifestDigest, TreeOID: prepared.TreeSHA, CommitOID: prepared.CommitSHA,
		BundleDigest: prepared.BundleDigest, BundleSize: prepared.BundleSizeBytes, BundleRef: prepared.BundleRef,
		BundleArtifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(prepared.BundleArtifactID), Digest: prepared.BundleDigest,
			SizeBytes: prepared.BundleSizeBytes, MediaType: prepared.BundleMediaType,
		},
		CommitMessage: publication.CommitMessage, CommitTimestamp: publication.CommitTimestamp,
	}
}

func publicationID(task *corev1alpha1.Task) string {
	return publicationIDForTaskUID(task, task.UID)
}

func publicationIDForTaskUID(task *corev1alpha1.Task, taskUID types.UID) string {
	digest := mustACPDomainDigest("publication-id", map[string]any{"taskUID": string(taskUID), "attempt": task.Status.Execution.Attempt, "promptID": task.Status.Execution.PromptID})
	return "pub-" + strings.TrimPrefix(digest, "sha256:")[:48]
}

func publicationIDForTask(task *corev1alpha1.Task) string { return publicationID(task) }

// ACPPublicationIDForTask returns the immutable publication identity used by the credential/artifact brokers.
func ACPPublicationIDForTask(task *corev1alpha1.Task) string { return publicationID(task) }

func publicationOperationID(kind string, task *corev1alpha1.Task) string {
	return ACPPublicationOperationID(kind, task)
}

// ACPPublicationOperationID returns the deterministic external-effect operation identity for one Task publication step.
func ACPPublicationOperationID(kind string, task *corev1alpha1.Task) string {
	seed := kind
	if task != nil && task.Status.Execution != nil {
		seed += "-" + string(task.UID) + "-" + strconv.Itoa(int(task.Status.Execution.Attempt))
	}
	digest := mustACPDomainDigest("publication-operation", seed)
	return kind + "-" + strings.TrimPrefix(digest, "sha256:")[:32]
}

func mustACPDomainDigest(domain string, value any) string {
	digest, err := acpDomainDigest(domain, value)
	if err != nil {
		panic(err)
	}
	return digest
}

func clientObjectKey(task *corev1alpha1.Task) client.ObjectKey {
	return client.ObjectKey{Namespace: task.Namespace, Name: task.Name}
}

func taskDeliveryStatusForKubernetes(task *corev1alpha1.Task, status corev1alpha1.TaskDeliveryStatus) corev1alpha1.TaskDeliveryStatus {
	// Harness v2 uses "empty" as a protocol-only revision for Tasks without a
	// repository workspace. It is not a Git object ID and must not escape into
	// the schema-validated Task status. Preserve every other value so malformed
	// real-workspace revisions continue to fail closed at the API boundary.
	// A workspace may declare intent or options without a repository, matching
	// the emptyRuntimeWorkspace and prepareRuntimeWorkspace admission paths.
	if task != nil && (task.Spec.Workspace == nil || strings.TrimSpace(task.Spec.Workspace.GitRepo) == "") &&
		status.StartingSHA == acpNoWorkspaceRevision {
		status.StartingSHA = ""
	}
	return status
}

func (d *ACPDispatcher) patchDeliveryStatus(ctx context.Context, task *corev1alpha1.Task, status corev1alpha1.TaskDeliveryStatus) error {
	key := clientObjectKey(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		copyStatus := taskDeliveryStatusForKubernetes(latest, status)
		latest.Status.Delivery = &copyStatus
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

// terminalProjectionExecution builds the immutable terminal-projection
// execution payload from a deep copy of the Task's frozen execution identity,
// overlaying only the terminal classification. Prompt attempt reclamation
// validates the projection against the attempt's request digest and the
// Task's complete runtime identity — including the RuntimeSession supervisor
// boot ID and the profile/MCP/workspace digests — so a sparse payload makes
// the source PromptAttempt impossible to retire and the Task undeletable.
// The volatile transition time is cleared so retried settlements enqueue
// byte-identical payloads.
func terminalProjectionExecution(
	task *corev1alpha1.Task,
	state corev1alpha1.TaskExecutionState,
	outcome corev1alpha1.TaskExecutionOutcome,
	reason corev1alpha1.TaskExecutionReason,
	message string,
) corev1alpha1.TaskExecutionStatus {
	execution := corev1alpha1.TaskExecutionStatus{}
	if task != nil && task.Status.Execution != nil {
		execution = *task.Status.Execution.DeepCopy()
	}
	execution.State = state
	execution.Outcome = outcome
	execution.Reason = reason
	execution.Message = message
	execution.LastTransitionTime = nil
	return execution
}

func (d *ACPDispatcher) completeSuccessWithDelivery(ctx context.Context, task *corev1alpha1.Task, status corev1alpha1.TaskDeliveryStatus, message string) error {
	execution := terminalProjectionExecution(task,
		corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded, "", "")
	if err := d.enqueueStandaloneTaskProjection(ctx, task, taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: execution.Attempt,
		Phase: corev1alpha1.TaskPhaseSucceeded, Message: message, Execution: execution, Delivery: &status,
	}); err != nil {
		return err
	}
	status = taskDeliveryStatusForKubernetes(task, status)
	key := clientObjectKey(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = corev1alpha1.TaskPhaseSucceeded
		latest.Status.Message = message
		latest.Status.CompletionTime = &now
		if latest.Status.Execution == nil {
			latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{}
		}
		latest.Status.Execution.State = corev1alpha1.TaskExecutionStateSucceeded
		latest.Status.Execution.Outcome = corev1alpha1.TaskExecutionOutcomeSucceeded
		latest.Status.Execution.LastTransitionTime = &now
		status.LastTransitionTime = &now
		latest.Status.Delivery = &status
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) failTaskForDelivery(ctx context.Context, task *corev1alpha1.Task, status corev1alpha1.TaskDeliveryStatus, message string) error {
	// Execution succeeded and only delivery failed, so the projection carries
	// the frozen successful execution identity with the failed Phase.
	execution := terminalProjectionExecution(task,
		corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded, "", "")
	if err := d.enqueueStandaloneTaskProjection(ctx, task, taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: execution.Attempt,
		Phase: corev1alpha1.TaskPhaseFailed, Message: message, Execution: execution, Delivery: &status,
	}); err != nil {
		return err
	}
	status = taskDeliveryStatusForKubernetes(task, status)
	key := clientObjectKey(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = corev1alpha1.TaskPhaseFailed
		latest.Status.Message = message
		latest.Status.CompletionTime = &now
		if latest.Status.Execution == nil {
			latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{}
		}
		latest.Status.Execution.State = corev1alpha1.TaskExecutionStateSucceeded
		latest.Status.Execution.Outcome = corev1alpha1.TaskExecutionOutcomeSucceeded
		latest.Status.Execution.LastTransitionTime = &now
		status.LastTransitionTime = &now
		latest.Status.Delivery = &status
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) cancelTaskAfterExecution(ctx context.Context, task *corev1alpha1.Task, status corev1alpha1.TaskDeliveryStatus, message string) error {
	// Execution succeeded before the cancellation settled the publication, so
	// the projection carries the frozen successful execution identity with the
	// cancelled Phase.
	execution := terminalProjectionExecution(task,
		corev1alpha1.TaskExecutionStateSucceeded, corev1alpha1.TaskExecutionOutcomeSucceeded, "", "")
	if err := d.enqueueStandaloneTaskProjection(ctx, task, taskTerminalProjection{
		Namespace: task.Namespace, Task: task.Name, TaskUID: string(task.UID), Attempt: execution.Attempt,
		Phase: corev1alpha1.TaskPhaseCancelled, Message: message, Execution: execution, Delivery: &status,
	}); err != nil {
		return err
	}
	status = taskDeliveryStatusForKubernetes(task, status)
	key := clientObjectKey(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1alpha1.Task{}
		if err := d.Client.Get(ctx, key, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = corev1alpha1.TaskPhaseCancelled
		latest.Status.Message = message
		latest.Status.CompletionTime = &now
		if latest.Status.Execution == nil {
			latest.Status.Execution = &corev1alpha1.TaskExecutionStatus{}
		}
		latest.Status.Execution.State = corev1alpha1.TaskExecutionStateSucceeded
		latest.Status.Execution.Outcome = corev1alpha1.TaskExecutionOutcomeSucceeded
		latest.Status.Execution.LastTransitionTime = &now
		status.LastTransitionTime = &now
		latest.Status.Delivery = &status
		return d.Client.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (d *ACPDispatcher) cancelPreparedPublication(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if task == nil || task.Status.Execution == nil {
		return false, nil
	}
	publication, err := d.Store.GetPublication(ctx, publicationIDForTask(task))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if publication.State == store.PublicationCancelledBeforePublish {
		return true, nil
	}
	if publication.State != store.PublicationPrepared {
		return false, nil
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return false, err
	}
	_, err = d.transitionPublication(ctx, publication, fence, store.PublicationCancelledBeforePublish,
		publicationOperationID("cancel-before-publish", task), mustACPDomainDigest("publication-cancel", publication.ID), nil, nil, nil, "")
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			latest, getErr := d.Store.GetPublication(ctx, publication.ID)
			if getErr != nil {
				return false, getErr
			}
			return latest.State == store.PublicationCancelledBeforePublish, nil
		}
		return false, err
	}
	return true, nil
}

// maxSequentialPublisherSettlementStages is the largest number of sequential
// publisher-backed external-effect stages that run under one bounded context
// in publishWorkspaceDelta: the delivery context covers branch-claim refresh,
// preflight, and prepare; the detached settlement context covers publish,
// verify, and pull-request reconciliation. Ledger settle calls and durable
// store bookkeeping between stages are covered by the trailing margin.
const maxSequentialPublisherSettlementStages = 3

// publicationSettlementWindow bounds the multi-effect publication settlement
// and delivery flows. publishWorkspaceDelta runs its publisher-backed stages
// sequentially under a single context, so the window must budget every stage
// at its full bounded call duration rather than only one:
//
//	window = stages x (call timeout + settlement margin) + settlement margin
//
// With the default 4m publisher call bound that is 3 x 5m + 1m = 16m; with
// ORKA_PUBLISHER_PUBLISH_TIMEOUT=10m it is 3 x 11m + 1m = 34m. Budgeting only
// one call timeout would let a slow-but-acknowledged publish starve remote
// verification and PR reconciliation, misclassifying a delivered push as
// outcome-unknown.
func publicationSettlementWindow() time.Duration {
	perStage := externalEffectCallTimeout(store.ExternalEffectIdentity{Kind: "publisher.publish"}) + externalEffectLeaseSettlementMargin
	return maxSequentialPublisherSettlementStages*perStage + externalEffectLeaseSettlementMargin
}
