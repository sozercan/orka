package service

import (
	"context"
	"net/http"
	"time"

	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/publisher"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

const (
	ProtocolVersion = "orka.workspace-publisher.v1"

	HealthPath                   = "/v1/health"
	CapabilitiesPath             = "/v1/capabilities"
	WorkspaceResolvePath         = "/v1/workspaces/resolve"
	WorkspacePreparePath         = "/v1/workspaces/prepare"
	PublicationPreflightPath     = "/v1/publications/preflight"
	PublicationPreparePath       = "/v1/publications/prepare"
	PublicationPublishPath       = "/v1/publications/publish"
	PublicationVerifyPath        = "/v1/publications/verify"
	PublicationReclaimPath       = "/v1/publications/reclaim"
	PullRequestReconcilePath     = "/v1/pull-requests/reconcile"
	OperationCapabilityHeader    = "X-Orka-Publisher-Operation-Capability"
	OperationRequestDigestHeader = "X-Orka-Publisher-Request-Digest"
)

type Operation string

const (
	OperationWorkspaceResolve     Operation = "workspace.resolve"
	OperationWorkspacePrepare     Operation = "workspace.prepare"
	OperationPublicationPreflight Operation = "publication.preflight"
	OperationPublicationPrepare   Operation = "publication.prepare"
	OperationPublicationPublish   Operation = "publication.publish"
	OperationPublicationVerify    Operation = "publication.verify"
	OperationPublicationReclaim   Operation = "publication.reclaim"
	OperationPullRequestReconcile Operation = "pull-request.reconcile"
)

func (o Operation) Path() string {
	switch o {
	case OperationWorkspaceResolve:
		return WorkspaceResolvePath
	case OperationWorkspacePrepare:
		return WorkspacePreparePath
	case OperationPublicationPreflight:
		return PublicationPreflightPath
	case OperationPublicationPrepare:
		return PublicationPreparePath
	case OperationPublicationPublish:
		return PublicationPublishPath
	case OperationPublicationVerify:
		return PublicationVerifyPath
	case OperationPublicationReclaim:
		return PublicationReclaimPath
	case OperationPullRequestReconcile:
		return PullRequestReconcilePath
	default:
		return ""
	}
}

// OperationMetadata is the durable caller identity bound by the operation
// capability. Workspace preparation is Task-scoped; publication and PR
// operations are Publication-scoped.
type OperationMetadata struct {
	Namespace     string `json:"namespace"`
	OperationID   string `json:"operationId"`
	TaskID        string `json:"taskId,omitempty"`
	PublicationID string `json:"publicationId,omitempty"`
}

type CredentialKind string

type CredentialRole string

const (
	CredentialHTTPExtraHeader CredentialKind = "http-extra-header"
	CredentialForgeToken      CredentialKind = "forge-token"

	CredentialRoleSourceRead  CredentialRole = "SourceRead"
	CredentialRoleTargetRead  CredentialRole = "TargetRead"
	CredentialRoleTargetWrite CredentialRole = "TargetWrite"
	CredentialRoleForge       CredentialRole = "Forge"
)

// CredentialReference names a mounted file below the configured credential
// root. Raw credentials are never accepted in API requests. Role binds brokered
// requests to one frozen prompt-credential purpose.
type CredentialReference struct {
	Name string         `json:"name"`
	Kind CredentialKind `json:"kind"`
	Role CredentialRole `json:"role,omitempty"`
}

type WorkspaceLimits struct {
	MaxEntries       int   `json:"maxEntries,omitempty"`
	MaxFileBytes     int64 `json:"maxFileBytes,omitempty"`
	MaxExpandedBytes int64 `json:"maxExpandedBytes,omitempty"`
	MaxArtifactBytes int64 `json:"maxArtifactBytes,omitempty"`
	MaxPathBytes     int   `json:"maxPathBytes,omitempty"`
}

type WorkspaceResolveRequest struct {
	Metadata      OperationMetadata    `json:"metadata"`
	Source        publisher.Repository `json:"source"`
	SourceRef     string               `json:"sourceRef"`
	CredentialRef *CredentialReference `json:"credentialRef,omitempty"`
}

type WorkspaceResolveResponse struct {
	OperationID   string `json:"operationId"`
	RequestDigest string `json:"requestDigest"`
	RepositoryID  string `json:"repositoryId"`
	SourceRef     string `json:"sourceRef"`
	BaselineOID   string `json:"baselineOid"`
}

type WorkspacePrepareRequest struct {
	Metadata      OperationMetadata    `json:"metadata"`
	Source        publisher.Repository `json:"source"`
	SourceRef     string               `json:"sourceRef"`
	BaselineOID   string               `json:"baselineOid"`
	CredentialRef *CredentialReference `json:"credentialRef,omitempty"`
	Limits        WorkspaceLimits      `json:"limits,omitempty"`
}

type WorkspacePrepareResponse struct {
	OperationID    string                      `json:"operationId"`
	RequestDigest  string                      `json:"requestDigest"`
	RepositoryID   string                      `json:"repositoryId"`
	SourceRef      string                      `json:"sourceRef"`
	BaselineOID    string                      `json:"baselineOid"`
	TreeOID        string                      `json:"treeOid"`
	ManifestDigest string                      `json:"manifestDigest"`
	EntryCount     int                         `json:"entryCount"`
	ExpandedBytes  int64                       `json:"expandedBytes"`
	Artifact       harnessv2.ArtifactReference `json:"artifact"`
}

type PublicationPreflightRequest struct {
	Metadata      OperationMetadata          `json:"metadata"`
	CredentialRef *CredentialReference       `json:"credentialRef,omitempty"`
	Request       publisher.PreflightRequest `json:"request"`
}

type PublicationPreflightResponse struct {
	OperationID   string                    `json:"operationId"`
	RequestDigest string                    `json:"requestDigest"`
	Result        publisher.PreflightResult `json:"result"`
}

type PublicationPrepareRequest struct {
	Metadata            OperationMetadata           `json:"metadata"`
	SourceCredentialRef *CredentialReference        `json:"sourceCredentialRef,omitempty"`
	DeltaArtifact       harnessv2.ArtifactReference `json:"deltaArtifact"`
	Request             publisher.PrepareRequest    `json:"request"`
}

// PreparedPublication deliberately omits the publisher's local BundlePath.
type PreparedPublication struct {
	PublicationID         string                      `json:"publicationId"`
	PublicationGeneration int64                       `json:"publicationGeneration"`
	OperationID           string                      `json:"operationId"`
	RequestDigest         string                      `json:"requestDigest"`
	Source                publisher.Repository        `json:"source"`
	SourceRef             string                      `json:"sourceRef"`
	Target                publisher.Repository        `json:"target"`
	TargetRef             string                      `json:"targetRef"`
	BranchClaimGeneration int64                       `json:"branchClaimGeneration"`
	BaselineOID           string                      `json:"baselineOid"`
	RemoteBefore          publisher.RemoteRef         `json:"remoteBefore"`
	DeltaArtifactDigest   string                      `json:"deltaArtifactDigest"`
	RelativeRoot          string                      `json:"relativeRoot,omitempty"`
	ManifestDigest        string                      `json:"manifestDigest"`
	TreeOID               string                      `json:"treeOid"`
	CommitOID             string                      `json:"commitOid"`
	BundleDigest          string                      `json:"bundleDigest"`
	BundleSize            int64                       `json:"bundleSize"`
	BundleRef             string                      `json:"bundleRef"`
	BundleArtifact        harnessv2.ArtifactReference `json:"bundleArtifact"`
	CommitMessage         string                      `json:"commitMessage"`
	CommitTimestamp       time.Time                   `json:"commitTimestamp"`
}

type PublicationPrepareResponse struct {
	OperationID   string              `json:"operationId"`
	RequestDigest string              `json:"requestDigest"`
	Prepared      PreparedPublication `json:"prepared"`
}

type PublicationPublishRequest struct {
	Metadata      OperationMetadata        `json:"metadata"`
	CredentialRef *CredentialReference     `json:"credentialRef,omitempty"`
	Prepared      PreparedPublication      `json:"prepared"`
	Request       publisher.PublishRequest `json:"request"`
}

type PublicationPublishResponse struct {
	OperationID   string                   `json:"operationId"`
	RequestDigest string                   `json:"requestDigest"`
	Receipt       publisher.PublishReceipt `json:"receipt"`
}

type PublicationVerifyRequest struct {
	Metadata      OperationMetadata       `json:"metadata"`
	CredentialRef *CredentialReference    `json:"credentialRef,omitempty"`
	Prepared      PreparedPublication     `json:"prepared"`
	Request       publisher.VerifyRequest `json:"request"`
}

type PublicationVerifyResponse struct {
	OperationID   string                        `json:"operationId"`
	RequestDigest string                        `json:"requestDigest"`
	Receipt       publisher.VerificationReceipt `json:"receipt"`
}

type PublicationReclaimRequest struct {
	Metadata OperationMetadata        `json:"metadata"`
	Request  publisher.ReclaimRequest `json:"request"`
}

type PublicationReclaimResponse struct {
	OperationID   string                  `json:"operationId"`
	RequestDigest string                  `json:"requestDigest"`
	Result        publisher.ReclaimResult `json:"result"`
}

type PullRequestReconcileRequest struct {
	Metadata      OperationMetadata           `json:"metadata"`
	CredentialRef *CredentialReference        `json:"credentialRef,omitempty"`
	Intent        publisher.PullRequestIntent `json:"intent"`
}

type PullRequestReconcileResponse struct {
	OperationID   string                       `json:"operationId"`
	RequestDigest string                       `json:"requestDigest"`
	Receipt       publisher.PullRequestReceipt `json:"receipt"`
}

type ErrorResponse struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Retryable     bool   `json:"retryable"`
	OperationID   string `json:"operationId,omitempty"`
	RequestDigest string `json:"requestDigest,omitempty"`
}

type HealthResponse struct {
	Status string `json:"status"`
	Ready  bool   `json:"ready"`
}

type CapabilityLimits struct {
	MaxConcurrentOperations   int   `json:"maxConcurrentOperations"`
	MaxRequestBytes           int64 `json:"maxRequestBytes"`
	MaxResponseBytes          int64 `json:"maxResponseBytes"`
	MaxWorkspaceEntries       int   `json:"maxWorkspaceEntries"`
	MaxWorkspaceFileBytes     int64 `json:"maxWorkspaceFileBytes"`
	MaxWorkspaceBytes         int64 `json:"maxWorkspaceBytes"`
	MaxWorkspaceArtifactBytes int64 `json:"maxWorkspaceArtifactBytes"`
	MaxDeltaBytes             int64 `json:"maxDeltaBytes"`
	MaxBundleBytes            int64 `json:"maxBundleBytes"`
	MaxJournalBytes           int64 `json:"maxJournalBytes"`
}

type CapabilitiesResponse struct {
	Protocol                  string           `json:"protocol"`
	NetworkIdentity           string           `json:"networkIdentity"`
	Operations                []Operation      `json:"operations"`
	Authentication            []string         `json:"authentication"`
	CredentialKinds           []CredentialKind `json:"credentialKinds"`
	SCMSchemes                []string         `json:"scmSchemes"`
	GitVersion                string           `json:"gitVersion"`
	RedirectsAllowed          bool             `json:"redirectsAllowed"`
	ProviderOrMCPAccess       bool             `json:"providerOrMcpAccess"`
	PullRequestReconciliation bool             `json:"pullRequestReconciliation"`
	Limits                    CapabilityLimits `json:"limits"`
}

// PRReconcilerFactory lets an SCM-specific adapter consume a short-lived,
// operation-scoped credential file without expanding the public API to raw
// credentials. The file is deleted immediately after reconciliation.
type PRReconcilerFactory interface {
	New(ctx context.Context, credentialPath string) (publisher.PullRequestReconciler, error)
}

// Config contains only already-resolved secrets. The command package loads
// them from files and clears the corresponding environment variables.
type Config struct {
	ListenAddress             string
	ControllerBearerToken     []byte
	OperationCapabilitySecret []byte
	// ArtifactCapabilitySecret is a development/test fallback. Supported
	// deployments use ArtifactAuthorizationBrokerURL so the Publisher cannot
	// mint arbitrary artifact capabilities.
	ArtifactCapabilitySecret       []byte
	ArtifactAuthorizationBrokerURL string
	ArtifactAPIURL                 string
	ArtifactRoot                   string
	JournalRoot                    string
	TempRoot                       string
	CredentialRoot                 string
	CredentialBrokerURL            string
	DefaultGitCredential           *CredentialReference
	GitBinary                      string
	RequiredGitVersion             string
	AllowedSCMHosts                []string
	ProxyEnvironment               publisher.ProxyEnvironment
	AllowFileRepositories          bool
	MaxConcurrentOperations        int
	MaxRequestBytes                int64
	MaxResponseBytes               int64
	MaxJournalBytes                int64
	MaxDeltaBytes                  int64
	MaxBundleBytes                 int64
	MaxCommandOutput               int64
	PublishTimeout                 time.Duration
	VerifyAttempts                 int
	VerifyBackoff                  time.Duration
	WorkspaceLimits                WorkspaceLimits
	ArtifactTimeout                time.Duration
	CapabilityTTL                  time.Duration
	PRFactory                      PRReconcilerFactory
	HTTPClient                     *http.Client
	Now                            func() time.Time
}

func defaultWorkspaceLimits() WorkspaceLimits {
	limits := workspacedelta.DefaultLimits()
	return WorkspaceLimits{
		MaxEntries: limits.MaxEntries, MaxFileBytes: min(limits.MaxFileBytes, int64(64<<20)),
		MaxExpandedBytes: min(limits.MaxTotalBytes, int64(96<<20)),
		MaxArtifactBytes: min(limits.MaxArtifactBytes, artifactcap.DefaultWorkspaceArtifactMaxBytes),
		MaxPathBytes:     limits.MaxPathBytes,
	}
}
