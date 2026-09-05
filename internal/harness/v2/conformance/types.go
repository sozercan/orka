package conformance

import (
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

// WorkspaceGovernanceMode and WorkspaceGovernanceClaims share the portable
// v2 capability wire type; external registration adds policy around the same
// canonical field rather than defining a duplicate JSON member.
type WorkspaceGovernanceMode = harnessv2.WorkspaceGovernanceMode

const (
	WorkspaceGovernanceStrict  = harnessv2.WorkspaceGovernanceStrict
	WorkspaceGovernanceTrusted = harnessv2.WorkspaceGovernanceTrusted
)

type WorkspaceGovernanceClaims = harnessv2.WorkspaceGovernanceCapabilities

// CapabilitiesResponse is the external-runtime v2 capability envelope.
type CapabilitiesResponse struct {
	harnessv2.CapabilitiesResponse
}

func (r CapabilitiesResponse) Validate() error {
	return r.CapabilitiesResponse.Validate()
}

// Target is one exact external runtime registration to probe. Check never
// automatically retries mutations, reconnects prompt streams, or allocates a
// replacement prompt identity after ambiguous acceptance. A lifecycle probe
// deliberately resends exact operation identities to verify a claimed
// duplicate-safe replay matrix.
type Target struct {
	BaseURL                         string
	ControllerBearerToken           string
	OperationCapabilitySecret       []byte
	ControlTimeout                  time.Duration
	ExpectedRuntimeInstanceID       harnessv2.RuntimeInstanceID
	ExpectedControllerEpoch         uint64
	Profile                         harnessv2.RuntimeProfile
	ToolPolicy                      harnessv2.MCPToolPolicy
	ApprovalPolicy                  harnessv2.MCPApprovalPolicy
	Limits                          harnessv2.ProtocolLimits
	SupportsDrain                   bool
	SupportsPublicationFinalization bool
	WorkspaceGovernance             WorkspaceGovernanceClaims
	ProbeLifecycle                  bool
	// RequirePublicAddresses restricts every conformance dial to public
	// global unicast addresses. External (non-Service) registrations must set
	// it so a DNS name or endpoint cannot steer controller-originated probe
	// traffic at loopback, private, or link-local targets (including via DNS
	// rebinding between validation and dialing).
	RequirePublicAddresses bool
	// PinnedBackendAddresses, when set, forces every conformance dial to one of
	// the given verified backend ip:port targets instead of the endpoint's own
	// (Service ClusterIP) resolution. A same-namespace Service endpoint sets it
	// to the backend Pod addresses proven by the endpoint policy so a caller
	// that mutates the EndpointSlice between validation and dial cannot steer
	// bearer-authenticated probe traffic through the still-mutable Service. It
	// is mutually exclusive with RequirePublicAddresses (Service endpoints pin;
	// non-Service endpoints require public addresses).
	PinnedBackendAddresses []string
}

// Result contains only sanitized protocol observations. Authentication values,
// operation capabilities, prompt content, and provider output are never stored.
type Result struct {
	Passed                 bool
	Message                string
	ObservedCapabilities   *CapabilitiesResponse
	ObservedStatus         *harnessv2.StatusResponse
	LifecycleProbeExecuted bool
}
