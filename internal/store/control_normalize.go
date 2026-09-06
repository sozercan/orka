package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Helpers shared by every DurableControlStore backend. They normalize and
// validate store-level control records before a backend persists them, so the
// Kubernetes and SQLite implementations enforce the same contract.

// ConflictErrorf returns an error wrapping ErrConflict with a formatted detail.
func ConflictErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConflict, fmt.Sprintf(format, args...))
}

// CanonicalBytesDigest returns the canonical "sha256:<hex>" digest of value.
func CanonicalBytesDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// NormalizeControllerEpochName trims and validates a controller epoch name, defaulting to DefaultControllerEpochName.
func NormalizeControllerEpochName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = DefaultControllerEpochName
	}
	if err := ValidateControlIdentifier("controller epoch name", name); err != nil {
		return "", err
	}
	if name != DefaultControllerEpochName {
		return "", ValidationErrorf("first-release control store supports only controller epoch %q", DefaultControllerEpochName)
	}
	return name, nil
}

// NormalizeEpochFence trims and validates a controller epoch fence.
func NormalizeEpochFence(fence ControllerEpochFence) (ControllerEpochFence, error) {
	name, err := NormalizeControllerEpochName(fence.Name)
	if err != nil {
		return ControllerEpochFence{}, err
	}
	fence.Name = name
	fence.HolderID = strings.TrimSpace(fence.HolderID)
	if err := ValidateControlIdentifier("controller epoch holder ID", fence.HolderID); err != nil {
		return ControllerEpochFence{}, err
	}
	if fence.Epoch < 1 {
		return ControllerEpochFence{}, ValidationErrorf("controller epoch must be at least 1")
	}
	return fence, nil
}

// NormalizeControlTime returns value in UTC, substituting the current time when it is zero.
func NormalizeControlTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

// NormalizeOptionalControlTime returns a UTC copy of value, or nil.
func NormalizeOptionalControlTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

// NormalizeBranchClaimForCreate validates and normalizes a new branch claim and its fence.
func NormalizeBranchClaimForCreate(claim *BranchClaim, fence ControllerEpochFence) (BranchClaim, ControllerEpochFence, error) {
	if claim == nil {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("branch claim is required")
	}
	normalized := *claim
	normalized.RepositoryID = strings.TrimSpace(normalized.RepositoryID)
	normalized.Ref = strings.TrimSpace(normalized.Ref)
	normalized.OwnerUID = strings.TrimSpace(normalized.OwnerUID)
	if err := ValidateControlIdentifier("publication repository ID", normalized.RepositoryID); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	if err := ValidateFullBranchRef(normalized.Ref); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	canonicalID, err := CanonicalBranchClaimID(normalized.RepositoryID, normalized.Ref)
	if err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	normalized.ID = strings.TrimSpace(normalized.ID)
	if normalized.ID == "" {
		normalized.ID = canonicalID
	}
	if normalized.ID != canonicalID {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("branch claim ID must equal canonical ID %q", canonicalID)
	}
	if normalized.OwnerKind != BranchClaimOwnerTask && normalized.OwnerKind != BranchClaimOwnerSession {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("unsupported branch claim owner kind %q", normalized.OwnerKind)
	}
	if err := ValidateControlIdentifier("branch claim owner UID", normalized.OwnerUID); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	if normalized.Generation == 0 {
		normalized.Generation = 1
	}
	if normalized.Generation != 1 {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("new branch claim generation must be one")
	}
	if err := normalized.LastVerified.Validate("branch baseline"); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	if normalized.Availability == "" {
		normalized.Availability = BranchClaimAvailable
	}
	if !IsKnownBranchClaimAvailability(normalized.Availability) {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("unsupported branch claim availability %q", normalized.Availability)
	}
	normalized.BlockedReason = strings.TrimSpace(normalized.BlockedReason)
	normalized.RelatedPublicationID = strings.TrimSpace(normalized.RelatedPublicationID)
	if normalized.Availability == BranchClaimAvailable && (normalized.BlockedReason != "" || normalized.RelatedPublicationID != "") {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("available branch claim must not have block metadata")
	}
	if normalized.Availability == BranchClaimReconciliationBlocked && normalized.BlockedReason == "" {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("reconciliation-blocked branch claim requires a reason")
	}
	if err := ValidateControlReason("branch claim blocked reason", normalized.BlockedReason); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	if err := ValidateCanonicalDigest("branch claim request digest", normalized.RequestDigest); err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	fence, err = NormalizeEpochFence(fence)
	if err != nil {
		return BranchClaim{}, ControllerEpochFence{}, err
	}
	if normalized.Version != 0 && normalized.Version != 1 {
		return BranchClaim{}, ControllerEpochFence{}, ValidationErrorf("new branch claim version must be zero or one")
	}
	now := NormalizeControlTime(normalized.CreatedAt)
	normalized.CreatedAt = now
	if normalized.UpdatedAt.IsZero() {
		normalized.UpdatedAt = now
	} else {
		normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	}
	normalized.ControllerEpochName = fence.Name
	normalized.ControllerEpoch = fence.Epoch
	normalized.Version = 1
	normalized.LastOperationID = ""
	normalized.LastOperationDigest = ""
	return normalized, fence, nil
}

// NormalizeBranchClaimReclamationRequest validates and normalizes a branch claim reclamation request.
func NormalizeBranchClaimReclamationRequest(request ReclaimBranchClaimRequest) (ReclaimBranchClaimRequest, error) {
	request.ID = strings.TrimSpace(request.ID)
	request.ExpectedRepositoryID = strings.TrimSpace(request.ExpectedRepositoryID)
	request.ExpectedRef = strings.TrimSpace(request.ExpectedRef)
	request.ExpectedOwnerUID = strings.TrimSpace(request.ExpectedOwnerUID)
	request.ExpectedRequestDigest = strings.TrimSpace(request.ExpectedRequestDigest)
	if err := ValidateControlIdentifier("branch claim ID", request.ID); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	if err := ValidateControlIdentifier("publication repository ID", request.ExpectedRepositoryID); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	if err := ValidateFullBranchRef(request.ExpectedRef); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	canonicalID, err := CanonicalBranchClaimID(request.ExpectedRepositoryID, request.ExpectedRef)
	if err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	if request.ID != canonicalID {
		return ReclaimBranchClaimRequest{}, ValidationErrorf("branch claim ID must equal canonical ID %q", canonicalID)
	}
	if request.ExpectedOwnerKind != BranchClaimOwnerTask && request.ExpectedOwnerKind != BranchClaimOwnerSession {
		return ReclaimBranchClaimRequest{}, ValidationErrorf("branch claim owner kind is not reclaimable")
	}
	if err := ValidateControlIdentifier("branch claim owner UID", request.ExpectedOwnerUID); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	if request.ExpectedVersion < 1 || request.ExpectedGeneration < 1 {
		return ReclaimBranchClaimRequest{}, ValidationErrorf("branch claim expected version and generation must be at least 1")
	}
	if err := request.ExpectedLastVerified.Validate("expected branch baseline"); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	if request.ExpectedAvailability != BranchClaimAvailable {
		return ReclaimBranchClaimRequest{}, ValidationErrorf("only available branch claims may be reclaimed")
	}
	if err := ValidateCanonicalDigest("branch claim request digest", request.ExpectedRequestDigest); err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	fence, err := NormalizeEpochFence(request.Fence)
	if err != nil {
		return ReclaimBranchClaimRequest{}, err
	}
	request.Fence = fence
	return request, nil
}

// NormalizePromptAttemptForCreate validates and normalizes a new prompt attempt and its fence. It does not inspect CredentialBindings; backends that persist them call NormalizePromptCredentialBindings.
func NormalizePromptAttemptForCreate(attempt *PromptAttempt, fence ControllerEpochFence) (PromptAttempt, ControllerEpochFence, error) {
	if attempt == nil {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("prompt attempt is required")
	}
	normalized := *attempt
	normalized.Key.Namespace = strings.TrimSpace(normalized.Key.Namespace)
	normalized.Key.TaskUID = strings.TrimSpace(normalized.Key.TaskUID)
	normalized.Key.PromptID = strings.TrimSpace(normalized.Key.PromptID)
	if err := normalized.Key.Validate(); err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	canonicalID, err := normalized.Key.CanonicalID()
	if err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	normalized.ID = strings.TrimSpace(normalized.ID)
	if normalized.ID == "" {
		normalized.ID = canonicalID
	}
	if normalized.ID != canonicalID {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("prompt attempt ID must equal canonical ID %q", canonicalID)
	}
	if err := ValidateCanonicalDigest("prompt attempt request digest", normalized.RequestDigest); err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	if err := ValidateCanonicalDigest("prompt attempt binding digest", normalized.BindingDigest); err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	if err := ValidateCanonicalDigest("prompt attempt snapshot digest", normalized.SnapshotDigest); err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	if normalized.ExecutionState == "" {
		normalized.ExecutionState = PromptExecutionQueued
	}
	if !IsKnownPromptExecutionState(normalized.ExecutionState) {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("unsupported prompt execution state %q", normalized.ExecutionState)
	}
	if normalized.ExecutionState != PromptExecutionQueued {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("new prompt attempt must start in %s", PromptExecutionQueued)
	}
	if normalized.DeliveryState == "" {
		normalized.DeliveryState = PromptDeliveryNotRequested
	}
	if normalized.DeliveryState != PromptDeliveryNotRequested {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("new prompt attempt must start delivery in %s", PromptDeliveryNotRequested)
	}
	fence, err = NormalizeEpochFence(fence)
	if err != nil {
		return PromptAttempt{}, ControllerEpochFence{}, err
	}
	normalized.SessionUID = strings.TrimSpace(normalized.SessionUID)
	normalized.RuntimeInstanceID = strings.TrimSpace(normalized.RuntimeInstanceID)
	if normalized.SessionLeaseGeneration < 0 {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("session lease generation must not be negative")
	}
	if normalized.SessionLeaseGeneration > 0 && normalized.SessionUID == "" {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("session UID is required with a session lease generation")
	}
	if normalized.SessionUID != "" {
		if err := ValidateControlIdentifier("session UID", normalized.SessionUID); err != nil {
			return PromptAttempt{}, ControllerEpochFence{}, err
		}
	}
	if normalized.RuntimeInstanceID != "" {
		if err := ValidateControlIdentifier("runtime instance ID", normalized.RuntimeInstanceID); err != nil {
			return PromptAttempt{}, ControllerEpochFence{}, err
		}
	}
	if normalized.Version != 0 && normalized.Version != 1 {
		return PromptAttempt{}, ControllerEpochFence{}, ValidationErrorf("new prompt attempt version must be zero or one")
	}
	now := NormalizeControlTime(normalized.CreatedAt)
	normalized.CreatedAt = now
	if normalized.UpdatedAt.IsZero() {
		normalized.UpdatedAt = now
	} else {
		normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	}
	normalized.ControllerEpochName = fence.Name
	normalized.ControllerEpoch = fence.Epoch
	normalized.Version = 1
	normalized.LastOperationID = ""
	normalized.LastOperationDigest = ""
	return normalized, fence, nil
}

// NormalizePromptCredentialBindings sorts attempt.CredentialBindings by role
// and validates each binding against the attempt namespace, rejecting
// duplicate roles.
func NormalizePromptCredentialBindings(attempt *PromptAttempt) error {
	attempt.CredentialBindings = append([]PromptCredentialBinding(nil), attempt.CredentialBindings...)
	sort.Slice(attempt.CredentialBindings, func(i, j int) bool {
		return attempt.CredentialBindings[i].Role < attempt.CredentialBindings[j].Role
	})
	for i := range attempt.CredentialBindings {
		binding := attempt.CredentialBindings[i]
		if err := binding.Validate(); err != nil {
			return err
		}
		if binding.Namespace != attempt.Key.Namespace {
			return ValidationErrorf("prompt credential namespace must match the Task namespace")
		}
		if i > 0 && attempt.CredentialBindings[i-1].Role == binding.Role {
			return ValidationErrorf("prompt credential role %q is duplicated", binding.Role)
		}
	}
	return nil
}

// NormalizePullRequestIntent trims every string field of intent in place.
func NormalizePullRequestIntent(intent *PullRequestIntent) {
	intent.BaseRepositoryID = strings.TrimSpace(intent.BaseRepositoryID)
	intent.BaseRef = strings.TrimSpace(intent.BaseRef)
	intent.HeadRepositoryID = strings.TrimSpace(intent.HeadRepositoryID)
	intent.HeadRef = strings.TrimSpace(intent.HeadRef)
	intent.ExpectedHeadSHA = strings.TrimSpace(intent.ExpectedHeadSHA)
}

// NormalizePublicationForCreate validates and normalizes a new publication and its fence.
//
//nolint:gocyclo // Publication immutable-input validation is intentionally centralized.
func NormalizePublicationForCreate(publication *Publication, fence ControllerEpochFence) (Publication, ControllerEpochFence, error) {
	if publication == nil {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication is required")
	}
	normalized := *publication
	normalized.ID = strings.TrimSpace(normalized.ID)
	normalized.Namespace = strings.TrimSpace(normalized.Namespace)
	normalized.TaskUID = strings.TrimSpace(normalized.TaskUID)
	normalized.PromptID = strings.TrimSpace(normalized.PromptID)
	normalized.SessionUID = strings.TrimSpace(normalized.SessionUID)
	normalized.BranchClaimID = strings.TrimSpace(normalized.BranchClaimID)
	normalized.SourceRepositoryID = strings.TrimSpace(normalized.SourceRepositoryID)
	normalized.SourceRef = strings.TrimSpace(normalized.SourceRef)
	normalized.TargetRepositoryID = strings.TrimSpace(normalized.TargetRepositoryID)
	normalized.TargetRef = strings.TrimSpace(normalized.TargetRef)
	normalized.PublicationCredentialRef = strings.TrimSpace(normalized.PublicationCredentialRef)
	normalized.CommitIdentity = strings.TrimSpace(normalized.CommitIdentity)
	for field, value := range map[string]string{
		"publication ID":                   normalized.ID,
		"publication namespace":            normalized.Namespace,
		"publication task UID":             normalized.TaskUID,
		"publication prompt ID":            normalized.PromptID,
		"branch claim ID":                  normalized.BranchClaimID,
		"source repository ID":             normalized.SourceRepositoryID,
		"source ref":                       normalized.SourceRef,
		"target repository ID":             normalized.TargetRepositoryID,
		"publication credential reference": normalized.PublicationCredentialRef,
		"commit identity":                  normalized.CommitIdentity,
	} {
		if err := ValidateControlIdentifier(field, value); err != nil {
			return Publication{}, ControllerEpochFence{}, err
		}
	}
	if normalized.SessionUID != "" {
		if err := ValidateControlIdentifier("publication session UID", normalized.SessionUID); err != nil {
			return Publication{}, ControllerEpochFence{}, err
		}
	}
	if normalized.Generation < 1 || normalized.BranchClaimGeneration < 1 || normalized.Attempt < 1 {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication generation, branch claim generation, and attempt must be at least 1")
	}
	if err := ValidateFullBranchRef(normalized.TargetRef); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if err := normalized.Baseline.Validate("publication baseline"); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if err := ValidateCanonicalDigest("publication artifact digest", normalized.ArtifactDigest); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	normalized.SourceBaselineSHA = strings.TrimSpace(normalized.SourceBaselineSHA)
	if err := ValidateGitObjectID("publication source baseline SHA", normalized.SourceBaselineSHA); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	normalized.ArtifactID = strings.TrimSpace(normalized.ArtifactID)
	normalized.ArtifactMediaType = strings.TrimSpace(normalized.ArtifactMediaType)
	if err := ValidateControlIdentifier("publication artifact ID", normalized.ArtifactID); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if normalized.ArtifactSizeBytes < 1 {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication artifact size must be positive")
	}
	if normalized.ArtifactMediaType == "" {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication artifact media type is required")
	}
	if err := ValidateCanonicalDigest("publication request digest", normalized.RequestDigest); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if strings.TrimSpace(normalized.CommitMessage) == "" {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication commit message is required")
	}
	if err := ValidateControlReason("publication commit message", normalized.CommitMessage); err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if normalized.CommitTimestamp.IsZero() {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("publication commit timestamp is required")
	}
	normalized.CommitTimestamp = normalized.CommitTimestamp.UTC()
	if normalized.PRIntent != nil {
		copyIntent := *normalized.PRIntent
		NormalizePullRequestIntent(&copyIntent)
		normalized.PRIntent = &copyIntent
		if err := ValidatePullRequestIntent(copyIntent, normalized.Generation); err != nil {
			return Publication{}, ControllerEpochFence{}, err
		}
	}
	if normalized.State == "" {
		normalized.State = PublicationPreparing
	}
	if normalized.State != PublicationPreparing {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("new publication must start in %s", PublicationPreparing)
	}
	if normalized.PreparedReceipt != nil || normalized.PublishReceipt != nil || normalized.VerificationReceipt != nil || normalized.PullRequestReceipt != nil {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("new publication must not include operation receipts")
	}
	normalizedFence, err := NormalizeEpochFence(fence)
	if err != nil {
		return Publication{}, ControllerEpochFence{}, err
	}
	if normalized.Version != 0 && normalized.Version != 1 {
		return Publication{}, ControllerEpochFence{}, ValidationErrorf("new publication version must be zero or one")
	}
	now := NormalizeControlTime(normalized.CreatedAt)
	normalized.CreatedAt = now
	if normalized.UpdatedAt.IsZero() {
		normalized.UpdatedAt = now
	} else {
		normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	}
	normalized.ControllerEpochName = normalizedFence.Name
	normalized.ControllerEpoch = normalizedFence.Epoch
	normalized.Version = 1
	normalized.LastOperationID = ""
	normalized.LastOperationDigest = ""
	normalized.TerminalReason = ""
	return normalized, normalizedFence, nil
}

// NormalizeSessionControlForCreate validates and normalizes a new session control record and its fence.
func NormalizeSessionControlForCreate(control *SessionControl, fence ControllerEpochFence) (SessionControl, ControllerEpochFence, error) {
	if control == nil {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("session control is required")
	}
	normalized := *control
	normalized.Namespace = strings.TrimSpace(normalized.Namespace)
	normalized.SessionName = strings.TrimSpace(normalized.SessionName)
	normalized.SessionUID = strings.TrimSpace(normalized.SessionUID)
	for field, value := range map[string]string{
		"session namespace": normalized.Namespace, "session name": normalized.SessionName,
		"session UID": normalized.SessionUID,
	} {
		if err := ValidateControlIdentifier(field, value); err != nil {
			return SessionControl{}, ControllerEpochFence{}, err
		}
	}
	if err := ValidateCanonicalDigest("session control request digest", normalized.RequestDigest); err != nil {
		return SessionControl{}, ControllerEpochFence{}, err
	}
	if normalized.Availability == "" {
		normalized.Availability = SessionAvailable
	}
	if normalized.Availability != SessionAvailable && normalized.Availability != SessionReconciliationBlocked {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("unsupported session availability %q", normalized.Availability)
	}
	if normalized.Lease != nil {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("new session control must not contain an active lease")
	}
	if normalized.LeaseGeneration < 0 {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("session lease generation must not be negative")
	}
	normalized.BlockedReason = strings.TrimSpace(normalized.BlockedReason)
	if normalized.Availability == SessionAvailable && (normalized.BlockedReason != "" || normalized.RelatedPromptAttemptID != "" || normalized.RelatedPublicationID != "") {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("available session control must not contain reconciliation block metadata")
	}
	if normalized.Availability == SessionReconciliationBlocked && normalized.BlockedReason == "" {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("reconciliation-blocked session requires a reason")
	}
	if err := ValidateControlReason("session blocked reason", normalized.BlockedReason); err != nil {
		return SessionControl{}, ControllerEpochFence{}, err
	}
	if normalized.VerifiedBaseline != nil {
		copyBaseline := *normalized.VerifiedBaseline
		normalized.VerifiedBaseline = &copyBaseline
		if err := ValidateVerifiedBaseline(copyBaseline); err != nil {
			return SessionControl{}, ControllerEpochFence{}, err
		}
	}
	fence, err := NormalizeEpochFence(fence)
	if err != nil {
		return SessionControl{}, ControllerEpochFence{}, err
	}
	if normalized.Version != 0 && normalized.Version != 1 {
		return SessionControl{}, ControllerEpochFence{}, ValidationErrorf("new session control version must be zero or one")
	}
	now := NormalizeControlTime(normalized.CreatedAt)
	normalized.CreatedAt = now
	if normalized.UpdatedAt.IsZero() {
		normalized.UpdatedAt = now
	} else {
		normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	}
	normalized.ControllerEpochName = fence.Name
	normalized.ControllerEpoch = fence.Epoch
	normalized.Version = 1
	return normalized, fence, nil
}
