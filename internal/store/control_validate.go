package store

import (
	"reflect"
	"strings"
)

// IsKnownBranchClaimAvailability reports whether value is a supported branch claim availability.
func IsKnownBranchClaimAvailability(value BranchClaimAvailability) bool {
	return value == BranchClaimAvailable || value == BranchClaimReconciliationBlocked
}

// BranchClaimReclamationIdentityReplaced reports whether request replaces the identity recorded on claim.
func BranchClaimReclamationIdentityReplaced(claim BranchClaim, request ReclaimBranchClaimRequest) bool {
	return claim.OwnerKind != request.ExpectedOwnerKind || claim.OwnerUID != request.ExpectedOwnerUID || claim.RequestDigest != request.ExpectedRequestDigest
}

// BranchClaimMatchesReclamation reports whether claim already reflects request.
func BranchClaimMatchesReclamation(claim BranchClaim, request ReclaimBranchClaimRequest) bool {
	return claim.ID == request.ID && claim.RepositoryID == request.ExpectedRepositoryID && claim.Ref == request.ExpectedRef &&
		claim.OwnerKind == request.ExpectedOwnerKind && claim.OwnerUID == request.ExpectedOwnerUID &&
		claim.Generation == request.ExpectedGeneration && claim.LastVerified.Equal(request.ExpectedLastVerified) &&
		claim.Availability == request.ExpectedAvailability && claim.BlockedReason == "" && claim.RelatedPublicationID == "" &&
		claim.RequestDigest == request.ExpectedRequestDigest && claim.Version == request.ExpectedVersion
}

// IsKnownExternalEffectState reports whether state is a supported external effect state.
func IsKnownExternalEffectState(state ExternalEffectState) bool {
	switch state {
	case ExternalEffectPending, ExternalEffectInFlight, ExternalEffectSucceeded,
		ExternalEffectFailed, ExternalEffectOutcomeUnknown:
		return true
	default:
		return false
	}
}

// ValidExternalEffectTransition reports whether an external effect may move from one state to another.
func ValidExternalEffectTransition(from, to ExternalEffectState) bool {
	switch from {
	case ExternalEffectPending:
		return to == ExternalEffectInFlight || to == ExternalEffectSucceeded ||
			to == ExternalEffectFailed || to == ExternalEffectOutcomeUnknown
	case ExternalEffectInFlight:
		return to == ExternalEffectInFlight || to == ExternalEffectSucceeded ||
			to == ExternalEffectFailed || to == ExternalEffectOutcomeUnknown
	default:
		return false
	}
}

// ValidatePullRequestIntent validates a pull request intent against the publication generation.
func ValidatePullRequestIntent(intent PullRequestIntent, generation int64) error {
	for field, value := range map[string]string{
		"PR base repository ID": intent.BaseRepositoryID,
		"PR head repository ID": intent.HeadRepositoryID,
	} {
		if err := ValidateControlIdentifier(field, value); err != nil {
			return err
		}
	}
	if err := ValidateFullBranchRef(intent.BaseRef); err != nil {
		return err
	}
	if err := ValidateFullBranchRef(intent.HeadRef); err != nil {
		return err
	}
	if intent.PublicationGeneration != generation {
		return ValidationErrorf("PR intent publication generation %d does not match publication generation %d", intent.PublicationGeneration, generation)
	}
	return ValidateGitObjectID("PR expected head SHA", intent.ExpectedHeadSHA)
}

// ValidatePullRequestReceipt validates a pull request operation receipt.
func ValidatePullRequestReceipt(receipt PullRequestOperationReceipt) error {
	for field, value := range map[string]string{
		"PR receipt operation ID": receipt.OperationID, "PR receipt intent key": receipt.IntentKey,
		"PR receipt forge ID": receipt.ForgeID, "PR receipt URL": receipt.URL, "PR receipt state": receipt.State,
	} {
		if err := ValidateControlIdentifier(field, strings.TrimSpace(value)); err != nil {
			return err
		}
	}
	if err := ValidateCanonicalDigest("PR receipt request digest", receipt.RequestDigest); err != nil {
		return err
	}
	if err := ValidateGitObjectID("PR receipt head SHA", receipt.HeadSHA); err != nil {
		return err
	}
	if receipt.ReconciledAt.IsZero() {
		return ValidationErrorf("PR receipt reconciliation timestamp is required")
	}
	return nil
}

// ValidatePublicationTransitionReceipts validates the receipts carried by a publication transition.
//
//nolint:gocyclo // Each destination state has intentionally distinct exact-receipt invariants.
func ValidatePublicationTransitionReceipts(publication Publication, transition PublicationTransition) error {
	var receiptErr error
	switch transition.NewState {
	case PublicationPrepared:
		if transition.PreparedReceipt == nil || transition.PublishReceipt != nil || transition.VerificationReceipt != nil {
			return ValidationErrorf("Prepared transition requires only a prepared receipt")
		}
		receiptErr = ValidatePreparedReceipt(*transition.PreparedReceipt)
	case PublicationPublishing, PublicationCancelledBeforePublish:
		if transition.PreparedReceipt != nil || transition.PublishReceipt != nil || transition.VerificationReceipt != nil {
			return ValidationErrorf("%s transition must not replace receipts", transition.NewState)
		}
	case PublicationVerifying:
		if transition.PublishReceipt == nil || transition.PreparedReceipt != nil || transition.VerificationReceipt != nil {
			return ValidationErrorf("Verifying transition requires only a publish receipt")
		}
		receiptErr = ValidatePublishReceipt(publication, transition, *transition.PublishReceipt)
	case PublicationOutcomeUnknown:
		if (transition.ExpectedState == PublicationPreparing || transition.ExpectedState == PublicationPublishing) &&
			transition.VerificationReceipt == nil && transition.PreparedReceipt == nil && transition.PublishReceipt == nil {
			break
		}
		fallthrough
	case PublicationVerifiedExact, PublicationDeliveredSuperseded:
		if transition.VerificationReceipt == nil || transition.PreparedReceipt != nil || transition.PublishReceipt != nil {
			return ValidationErrorf("%s transition requires only a verification receipt", transition.NewState)
		}
		receiptErr = ValidateVerificationReceipt(publication, transition, *transition.VerificationReceipt)
	case PublicationDeliveryConflict:
		if transition.ExpectedState == PublicationVerifying {
			if transition.VerificationReceipt == nil || transition.PreparedReceipt != nil || transition.PublishReceipt != nil {
				return ValidationErrorf("verified DeliveryConflict transition requires only a verification receipt")
			}
			receiptErr = ValidateVerificationReceipt(publication, transition, *transition.VerificationReceipt)
		} else if transition.PreparedReceipt != nil || transition.PublishReceipt != nil || transition.VerificationReceipt != nil {
			return ValidationErrorf("preparation DeliveryConflict transition must not include receipts")
		}
	default:
		if transition.PreparedReceipt != nil || transition.PublishReceipt != nil || transition.VerificationReceipt != nil {
			return ValidationErrorf("%s transition must not include receipts", transition.NewState)
		}
	}
	if receiptErr != nil {
		return receiptErr
	}
	switch transition.NewState {
	case PublicationDeliveryConflict, PublicationCredentialBlocked,
		PublicationPreparationFailed, PublicationOutcomeUnknown:
		if strings.TrimSpace(transition.TerminalReason) == "" {
			return ValidationErrorf("terminal publication state %s requires a reason", transition.NewState)
		}
	}
	return nil
}

// ValidatePreparedReceipt validates a prepared publication receipt.
func ValidatePreparedReceipt(receipt PreparedPublicationReceipt) error {
	if err := ValidateControlIdentifier("prepared operation ID", receipt.OperationID); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("prepared request digest", receipt.RequestDigest); err != nil {
		return err
	}
	if err := ValidateGitObjectID("prepared tree SHA", receipt.TreeSHA); err != nil {
		return err
	}
	if err := ValidateGitObjectID("prepared commit SHA", receipt.CommitSHA); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("prepared manifest digest", receipt.ManifestDigest); err != nil {
		return err
	}
	if err := ValidateWorkspaceRelativeRoot(receipt.RelativeRoot); err != nil {
		return err
	}
	if err := ValidateControlIdentifier("prepared bundle artifact ID", receipt.BundleArtifactID); err != nil {
		return err
	}
	if err := ValidateCanonicalDigest("prepared bundle digest", receipt.BundleDigest); err != nil {
		return err
	}
	if receipt.BundleSizeBytes < 1 || receipt.BundleMediaType != PreparedBundleMediaType ||
		!strings.HasPrefix(receipt.BundleRef, "refs/orka/publications/") || len(strings.TrimPrefix(receipt.BundleRef, "refs/orka/publications/")) != 64 {
		return ValidationErrorf("prepared bundle artifact metadata is invalid")
	}
	if receipt.PreparedAt.IsZero() {
		return ValidationErrorf("prepared receipt timestamp is required")
	}
	return nil
}

// ValidatePublishReceipt validates a publish operation receipt against the publication and transition.
func ValidatePublishReceipt(publication Publication, transition PublicationTransition, receipt PublishOperationReceipt) error {
	if publication.PreparedReceipt == nil {
		return ValidationErrorf("publication must have a durable prepared receipt before publishing")
	}
	if receipt.OperationID != transition.OperationID || receipt.RequestDigest != transition.OperationDigest {
		return ValidationErrorf("publish receipt operation identity and digest must match transition")
	}
	if receipt.TargetRepositoryID != publication.TargetRepositoryID || receipt.TargetRef != publication.TargetRef || !receipt.RemoteBefore.Equal(publication.Baseline) || receipt.ExpectedCommitSHA != publication.PreparedReceipt.CommitSHA {
		return ValidationErrorf("publish receipt does not exactly match persisted publication target, baseline, or commit")
	}
	if err := receipt.RemoteBefore.Validate("publish remote-before"); err != nil {
		return err
	}
	if err := ValidateGitObjectID("publish expected commit SHA", receipt.ExpectedCommitSHA); err != nil {
		return err
	}
	if receipt.PublishedAt.IsZero() {
		return ValidationErrorf("publish receipt timestamp is required")
	}
	return nil
}

// ValidateVerificationReceipt validates a publication verification receipt against the publication and transition.
func ValidateVerificationReceipt(publication Publication, transition PublicationTransition, receipt PublicationVerificationReceipt) error {
	if publication.PreparedReceipt == nil || publication.PublishReceipt == nil {
		return ValidationErrorf("publication must have prepare and publish receipts before verification")
	}
	if receipt.OperationID != transition.OperationID || receipt.RequestDigest != transition.OperationDigest || receipt.Outcome != transition.NewState {
		return ValidationErrorf("verification receipt operation identity, digest, and outcome must match transition")
	}
	if receipt.ExpectedCommitSHA != publication.PreparedReceipt.CommitSHA {
		return ValidationErrorf("verification expected commit does not match prepared commit")
	}
	if receipt.VerifiedAt.IsZero() {
		return ValidationErrorf("verification receipt timestamp is required")
	}
	switch receipt.Outcome {
	case PublicationVerifiedExact:
		if err := receipt.ObservedRemote.Validate("verified remote"); err != nil {
			return err
		}
		if receipt.ObservedRemote.Absent || receipt.ObservedRemote.SHA != receipt.ExpectedCommitSHA {
			return ValidationErrorf("VerifiedExact requires observed remote to equal expected commit")
		}
		if receipt.DescendantProofDigest != "" {
			return ValidationErrorf("VerifiedExact must not include a descendant proof")
		}
	case PublicationDeliveredSuperseded:
		if err := receipt.ObservedRemote.Validate("superseding remote"); err != nil {
			return err
		}
		if receipt.ObservedRemote.Absent || receipt.ObservedRemote.SHA == receipt.ExpectedCommitSHA {
			return ValidationErrorf("DeliveredSuperseded requires a different observed descendant SHA")
		}
		if err := ValidateCanonicalDigest("descendant proof digest", receipt.DescendantProofDigest); err != nil {
			return err
		}
	case PublicationDeliveryConflict:
		if err := receipt.ObservedRemote.Validate("conflicting remote"); err != nil {
			return err
		}
	case PublicationOutcomeUnknown:
		if receipt.ObservedRemote.Absent || receipt.ObservedRemote.SHA != "" || receipt.DescendantProofDigest != "" {
			return ValidationErrorf("PublicationOutcomeUnknown must not invent a remote observation or descendant proof")
		}
	default:
		return ValidationErrorf("unsupported verification outcome %q", receipt.Outcome)
	}
	return nil
}

// ValidateVerifiedBaseline validates a verified branch baseline. Repository ID and ref are trimmed before validation; the SHA is validated as given.
func ValidateVerifiedBaseline(baseline VerifiedBranchBaseline) error {
	if err := ValidateControlIdentifier("verified repository ID", strings.TrimSpace(baseline.RepositoryID)); err != nil {
		return err
	}
	if err := ValidateFullBranchRef(strings.TrimSpace(baseline.Ref)); err != nil {
		return err
	}
	return ValidateGitObjectID("verified branch SHA", baseline.SHA)
}

// PublicationTransitionReceiptsMatch reports whether publication already carries the receipts in transition.
func PublicationTransitionReceiptsMatch(publication Publication, transition PublicationTransition) bool {
	if transition.PreparedReceipt != nil && !EqualPreparedReceipt(publication.PreparedReceipt, transition.PreparedReceipt) {
		return false
	}
	if transition.PublishReceipt != nil && !EqualPublishReceipt(publication.PublishReceipt, transition.PublishReceipt) {
		return false
	}
	if transition.VerificationReceipt != nil && !EqualVerificationReceipt(publication.VerificationReceipt, transition.VerificationReceipt) {
		return false
	}
	return true
}

// EqualPreparedReceipt reports whether two prepared receipts are equal.
func EqualPreparedReceipt(a, b *PreparedPublicationReceipt) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.OperationID == b.OperationID && a.RequestDigest == b.RequestDigest &&
		a.TreeSHA == b.TreeSHA && a.CommitSHA == b.CommitSHA && a.ManifestDigest == b.ManifestDigest && a.RelativeRoot == b.RelativeRoot &&
		a.BundleArtifactID == b.BundleArtifactID && a.BundleDigest == b.BundleDigest && a.BundleSizeBytes == b.BundleSizeBytes &&
		a.BundleMediaType == b.BundleMediaType && a.BundleRef == b.BundleRef && a.PreparedAt.Equal(b.PreparedAt)
}

// EqualPublishReceipt reports whether two publish receipts are equal.
func EqualPublishReceipt(a, b *PublishOperationReceipt) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.OperationID == b.OperationID && a.RequestDigest == b.RequestDigest &&
		a.TargetRepositoryID == b.TargetRepositoryID && a.TargetRef == b.TargetRef &&
		a.RemoteBefore.Equal(b.RemoteBefore) && a.ExpectedCommitSHA == b.ExpectedCommitSHA &&
		a.AcknowledgementUnknown == b.AcknowledgementUnknown && a.PublishedAt.Equal(b.PublishedAt)
}

// EqualVerificationReceipt reports whether two verification receipts are equal.
func EqualVerificationReceipt(a, b *PublicationVerificationReceipt) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.OperationID == b.OperationID && a.RequestDigest == b.RequestDigest && a.Outcome == b.Outcome &&
		a.ExpectedCommitSHA == b.ExpectedCommitSHA && a.ObservedRemote.Equal(b.ObservedRemote) &&
		a.DescendantProofDigest == b.DescendantProofDigest && a.VerifiedAt.Equal(b.VerifiedAt)
}

// SamePublicationCreation reports whether b is an idempotent replay of a.
func SamePublicationCreation(a, b Publication) bool {
	prIntentMatches := b.PRIntent == nil || reflect.DeepEqual(a.PRIntent, b.PRIntent)
	return a.ID == b.ID && a.Namespace == b.Namespace && a.Generation == b.Generation &&
		a.TaskUID == b.TaskUID && a.Attempt == b.Attempt && a.PromptID == b.PromptID &&
		a.SessionUID == b.SessionUID && a.BranchClaimID == b.BranchClaimID &&
		a.BranchClaimGeneration == b.BranchClaimGeneration && a.SourceRepositoryID == b.SourceRepositoryID &&
		a.SourceRef == b.SourceRef && a.SourceBaselineSHA == b.SourceBaselineSHA && a.TargetRepositoryID == b.TargetRepositoryID && a.TargetRef == b.TargetRef &&
		a.Baseline.Equal(b.Baseline) && a.ArtifactID == b.ArtifactID && a.ArtifactDigest == b.ArtifactDigest &&
		a.ArtifactSizeBytes == b.ArtifactSizeBytes && a.ArtifactMediaType == b.ArtifactMediaType &&
		a.PublicationCredentialRef == b.PublicationCredentialRef && a.CommitIdentity == b.CommitIdentity &&
		a.CommitMessage == b.CommitMessage && a.CommitTimestamp.Equal(b.CommitTimestamp) &&
		prIntentMatches && a.RequestDigest == b.RequestDigest
}
