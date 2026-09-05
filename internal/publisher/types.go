package publisher

import (
	"context"
	"time"
)

const (
	DigestPrefix      = "sha256:"
	CommitAuthorName  = "Orka Publisher"
	CommitAuthorEmail = "publisher@orka.ai"
)

// Repository is an exact provider identity paired with one canonical clone
// URL. Identity equality is exact and does not fall back to URL comparison.
type Repository struct {
	Provider string `json:"provider"`
	ID       string `json:"id"`
	URL      string `json:"url"`
}

// RemoteRef is an exact remote observation. Exactly one of Absent or OID is
// set.
type RemoteRef struct {
	Absent bool   `json:"absent"`
	OID    string `json:"oid,omitempty"`
}

func (r RemoteRef) Equal(other RemoteRef) bool {
	return r.Absent == other.Absent && r.OID == other.OID
}

// BranchClaim is the publisher-facing immutable branch ownership fence.
type BranchClaim struct {
	RepositoryID string    `json:"repositoryId"`
	Ref          string    `json:"ref"`
	OwnerKind    string    `json:"ownerKind"`
	OwnerUID     string    `json:"ownerUid"`
	Generation   int64     `json:"generation"`
	LastVerified RemoteRef `json:"lastVerified"`
}

// PrepareRequest binds the immutable workspace artifact and deterministic
// commit to one Publication and target. Target may differ from Source for fork
// publication.
type PrepareRequest struct {
	PublicationID         string     `json:"publicationId"`
	PublicationGeneration int64      `json:"publicationGeneration"`
	OperationID           string     `json:"operationId"`
	Source                Repository `json:"source"`
	SourceRef             string     `json:"sourceRef"`
	Target                Repository `json:"target"`
	TargetRef             string     `json:"targetRef"`
	BranchClaimGeneration int64      `json:"branchClaimGeneration"`
	BaselineOID           string     `json:"baselineOid"`
	RemoteBefore          RemoteRef  `json:"remoteBefore"`
	DeltaArtifact         []byte     `json:"-"`
	DeltaArtifactDigest   string     `json:"deltaArtifactDigest"`
	RelativeRoot          string     `json:"relativeRoot,omitempty"`
	CommitMessage         string     `json:"commitMessage"`
	CommitTimestamp       time.Time  `json:"commitTimestamp"`
}

// ReclaimRequest binds cache reclamation to one exact Publication incarnation.
// A missing cache directory is already reclaimed and succeeds idempotently.
type ReclaimRequest struct {
	PublicationID         string `json:"publicationId"`
	PublicationGeneration int64  `json:"publicationGeneration"`
}

// ReclaimResult records whether the local publication cache existed when the
// reclaim operation ran. Reclaimed is true for both removed and already-absent
// state because both are terminally reclaimed.
type ReclaimResult struct {
	PublicationID         string `json:"publicationId"`
	PublicationGeneration int64  `json:"publicationGeneration"`
	Reclaimed             bool   `json:"reclaimed"`
}

// PreparedPublication is the durable exact output of Prepare.
type PreparedPublication struct {
	PublicationID         string     `json:"publicationId"`
	PublicationGeneration int64      `json:"publicationGeneration"`
	OperationID           string     `json:"operationId"`
	RequestDigest         string     `json:"requestDigest"`
	Source                Repository `json:"source"`
	SourceRef             string     `json:"sourceRef"`
	Target                Repository `json:"target"`
	TargetRef             string     `json:"targetRef"`
	BranchClaimGeneration int64      `json:"branchClaimGeneration"`
	BaselineOID           string     `json:"baselineOid"`
	RemoteBefore          RemoteRef  `json:"remoteBefore"`
	DeltaArtifactDigest   string     `json:"deltaArtifactDigest"`
	RelativeRoot          string     `json:"relativeRoot,omitempty"`
	ManifestDigest        string     `json:"manifestDigest"`
	TreeOID               string     `json:"treeOid"`
	CommitOID             string     `json:"commitOid"`
	BundleDigest          string     `json:"bundleDigest"`
	BundleSize            int64      `json:"bundleSize"`
	BundleRef             string     `json:"bundleRef"`
	BundlePath            string     `json:"bundlePath"`
	CommitMessage         string     `json:"commitMessage"`
	CommitTimestamp       time.Time  `json:"commitTimestamp"`
}

// PreflightRequest verifies that the target ref still exactly equals the
// persisted BranchClaim baseline before a mutation is eligible to start.
type PreflightRequest struct {
	Target Repository  `json:"target"`
	Claim  BranchClaim `json:"claim"`
}

// PreflightResult records the exact remote observation.
type PreflightResult struct {
	Expected RemoteRef `json:"expected"`
	Observed RemoteRef `json:"observed"`
	Matches  bool      `json:"matches"`
}

// PublishRequest binds a durable prepared artifact to one exact branch CAS.
type PublishRequest struct {
	PublicationID         string      `json:"publicationId"`
	PublicationGeneration int64       `json:"publicationGeneration"`
	OperationID           string      `json:"operationId"`
	Target                Repository  `json:"target"`
	TargetRef             string      `json:"targetRef"`
	Claim                 BranchClaim `json:"claim"`
	RemoteBefore          RemoteRef   `json:"remoteBefore"`
	ExpectedCommitOID     string      `json:"expectedCommitOid"`
	BundleDigest          string      `json:"bundleDigest"`
}

// PublishOutcome is the transport-level result. Remote delivery remains
// non-terminal until Verify independently observes it.
type PublishOutcome string

const (
	PublishAcknowledged   PublishOutcome = "Acknowledged"
	PublishAlreadyExact   PublishOutcome = "AlreadyExact"
	PublishCASRejected    PublishOutcome = "CASRejected"
	PublishOutcomeUnknown PublishOutcome = "PublicationOutcomeUnknown"
)

// PublishReceipt is durable and idempotent for one operation ID and digest.
type PublishReceipt struct {
	PublicationID         string         `json:"publicationId"`
	PublicationGeneration int64          `json:"publicationGeneration"`
	OperationID           string         `json:"operationId"`
	RequestDigest         string         `json:"requestDigest"`
	Outcome               PublishOutcome `json:"outcome"`
	TargetRepositoryID    string         `json:"targetRepositoryId"`
	TargetRef             string         `json:"targetRef"`
	RemoteBefore          RemoteRef      `json:"remoteBefore"`
	ExpectedCommitOID     string         `json:"expectedCommitOid"`
	ObservedRemote        RemoteRef      `json:"observedRemote"`
}

// VerificationOutcome is the independently observed delivery classification.
type VerificationOutcome string

const (
	VerifiedExact             VerificationOutcome = "VerifiedExact"
	DeliveredSuperseded       VerificationOutcome = "DeliveredSuperseded"
	DeliveryConflict          VerificationOutcome = "DeliveryConflict"
	PublicationOutcomeUnknown VerificationOutcome = "PublicationOutcomeUnknown"
)

// VerifyRequest performs a separate read-only observation of the target.
type VerifyRequest struct {
	PublicationID         string      `json:"publicationId"`
	PublicationGeneration int64       `json:"publicationGeneration"`
	OperationID           string      `json:"operationId"`
	Target                Repository  `json:"target"`
	TargetRef             string      `json:"targetRef"`
	Claim                 BranchClaim `json:"claim"`
	ExpectedCommitOID     string      `json:"expectedCommitOid"`
	BundleDigest          string      `json:"bundleDigest"`
}

// VerificationReceipt is an immutable observation receipt.
type VerificationReceipt struct {
	PublicationID         string              `json:"publicationId"`
	PublicationGeneration int64               `json:"publicationGeneration"`
	OperationID           string              `json:"operationId"`
	RequestDigest         string              `json:"requestDigest"`
	Outcome               VerificationOutcome `json:"outcome"`
	TargetRepositoryID    string              `json:"targetRepositoryId"`
	TargetRef             string              `json:"targetRef"`
	ExpectedCommitOID     string              `json:"expectedCommitOid"`
	ObservedRemote        RemoteRef           `json:"observedRemote"`
	DescendantProofDigest string              `json:"descendantProofDigest,omitempty"`
}

// PullRequestIntent is the complete immutable identity of one desired PR.
type PullRequestIntent struct {
	BaseRepository        Repository `json:"baseRepository"`
	BaseRef               string     `json:"baseRef"`
	HeadRepository        Repository `json:"headRepository"`
	HeadRef               string     `json:"headRef"`
	PublicationGeneration int64      `json:"publicationGeneration"`
	ExpectedHeadOID       string     `json:"expectedHeadOid"`
	// SessionUID is the immutable owner from the durable Publication. It lets
	// successive publications reuse their Session's PR without adopting a PR
	// created by another Session that happens to use the same branch.
	SessionUID string `json:"sessionUid,omitempty"`
}

// PullRequestState is the reconciled forge state.
type PullRequestState string

const (
	PullRequestOpen   PullRequestState = "Open"
	PullRequestClosed PullRequestState = "Closed"
	PullRequestMerged PullRequestState = "Merged"
)

// PullRequestReceipt must bind to the exact intent tuple; same-branch matches
// with a different tuple are never adopted.
type PullRequestReceipt struct {
	IntentKey string           `json:"intentKey"`
	ForgeID   string           `json:"forgeId"`
	URL       string           `json:"url"`
	State     PullRequestState `json:"state"`
	HeadOID   string           `json:"headOid"`
}

// PullRequestReconciler is implemented inside an SCM-capable clean-room
// boundary. It must look up and reconcile by PullRequestIntent.Key, must not
// adopt the first same-branch PR, and must not recreate a known closed or
// merged PR without an explicit higher-level policy decision.
type PullRequestReconciler interface {
	Reconcile(ctx context.Context, intent PullRequestIntent) (PullRequestReceipt, error)
}
