package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	defaultControlTimeout = 60 * time.Second
	maxProbeResponseBytes = int64(harnessv2.MaxCanonicalJSONBytes)
)

// deniedSpecialUsePrefixes mirrors the SCM egress proxy's special-use deny
// list. Go's IsGlobalUnicast/IsPrivate predicates alone miss internally
// routable special-use ranges such as CGNAT 100.64/10, benchmarking
// 198.18/15, TEST-NETs, and 6to4/Teredo relays.
var deniedSpecialUsePrefixes = mustPrefixes(
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"224.0.0.0/4",
	"240.0.0.0/4",
	"::/128",
	"::1/128",
	"64:ff9b::/96",
	"64:ff9b:1::/48",
	"100::/64",
	"2001::/32",
	"2001:db8::/32",
	"2002::/16",
	"fc00::/7",
	"fe80::/10",
	"ff00::/8",
)

func mustPrefixes(values ...string) []netip.Prefix {
	result := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		result = append(result, netip.MustParsePrefix(value))
	}
	return result
}

// PublicAddressAllowed reports whether addr is a public global unicast
// address outside every special-use range.
func PublicAddressAllowed(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range deniedSpecialUsePrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// PublicAddressDialTransport returns a proxy-disabled transport whose every
// connection attempt is rejected unless the resolved address is a public
// global unicast address. The control hook runs per dial after DNS
// resolution, so it also defeats DNS rebinding between validation and dial.
// Non-Service external runtime traffic must use it so a hostname that
// resolves publicly at conformance cannot later rebind to an internal
// controller-reachable address.
func PublicAddressDialTransport() *http.Transport {
	transport := harnessv2.NewProxylessTransport()
	ApplyPublicAddressDialControl(transport)
	return transport
}

// ApplyPublicAddressDialControl installs the per-dial public-address control on
// an existing transport, preserving its TLS configuration and any other
// settings. Use it when a conformance dial must keep configured TLS roots (the
// harness v1 client) yet still reject a hostname that resolves or rebinds to a
// non-public, cross-namespace, or link-local address.
func ApplyPublicAddressDialControl(transport *http.Transport) {
	if transport == nil {
		return
	}
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			return requirePublicDialAddress(address)
		},
	}
	transport.DialContext = dialer.DialContext
}

// PinnedBackendDialTransport returns a proxy-disabled transport that dials only
// the given verified backend ip:port targets, ignoring the address the endpoint
// URL would otherwise resolve to (a Service ClusterIP). Pinning the
// authenticated connection to verified Pod backends closes the validate-then-dial
// TOCTOU that routing conformance probes through the still-mutable Service would
// leave open. It round-robins across the pins and fails closed when none remain.
func PinnedBackendDialTransport(addresses []string) *http.Transport {
	transport := harnessv2.NewProxylessTransport()
	ApplyPinnedBackendDial(transport, addresses)
	return transport
}

// ApplyPinnedBackendDial forces every dial on an existing transport to one of
// the given verified backend ip:port targets, preserving the transport's TLS
// configuration. Use it when a conformance dial must keep configured TLS roots
// (the harness v1 client) yet still pin to verified Service backends rather than
// the mutable Service ClusterIP.
func ApplyPinnedBackendDial(transport *http.Transport, addresses []string) {
	if transport == nil {
		return
	}
	pinned := append([]string(nil), addresses...)
	base := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var next atomic.Uint64
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		if len(pinned) == 0 {
			return nil, fmt.Errorf("no verified backend to dial")
		}
		target := pinned[int(next.Add(1)-1)%len(pinned)]
		return base.DialContext(ctx, network, target)
	}
}

// requirePublicDialAddress rejects dial targets that are not public global
// unicast addresses outside every special-use range.
func requirePublicDialAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("conformance dial address is invalid")
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("conformance dial address is invalid")
	}
	if !PublicAddressAllowed(parsed) {
		return fmt.Errorf("conformance dial target is not a public address")
	}
	return nil
}

// Check probes one external runtime. It is deliberately single-attempt: no
// prompt stream is reconnected or re-executed. Strict lifecycle checks use
// separate completed-workspace and cancellation sessions, consuming each
// original stream through terminal settlement.
func Check(ctx context.Context, target Target) Result {
	result := Result{}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateTarget(target); err != nil {
		result.Message = boundedMessage(err)
		return result
	}
	timeout := target.ControlTimeout
	if timeout <= 0 {
		timeout = defaultControlTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpClient := &http.Client{
		Timeout: timeout,
		// Conformance traffic targets a declared runtime endpoint; it must
		// never traverse an inherited environment proxy.
		Transport: harnessv2.NewProxylessTransport(),
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	// A Service endpoint pins to the verified backends; a non-Service endpoint
	// restricts every dial to a public address. The two are mutually exclusive,
	// and pinning takes precedence so verified backends are never re-resolved
	// through the mutable Service.
	if len(target.PinnedBackendAddresses) > 0 {
		httpClient.Transport = PinnedBackendDialTransport(target.PinnedBackendAddresses)
	} else if target.RequirePublicAddresses {
		httpClient.Transport = PublicAddressDialTransport()
	}
	expectedProfileDigest, err := harnessv2.CanonicalProfileDigest(target.Profile)
	if err != nil {
		result.Message = boundedMessage(fmt.Errorf("compute expected profile digest: %w", err))
		return result
	}
	client, err := harnessv2.NewClient(
		target.BaseURL,
		harnessv2.WithHTTPClient(httpClient),
		harnessv2.WithControlTimeout(timeout),
		harnessv2.WithControllerBearerToken(target.ControllerBearerToken),
		harnessv2.WithOperationCapabilitySecret(target.OperationCapabilitySecret),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: expectedProfileDigest, RuntimeInstanceID: target.ExpectedRuntimeInstanceID,
		}),
		harnessv2.WithProtocolLimits(target.Limits),
	)
	if err != nil {
		result.Message = boundedMessage(fmt.Errorf("construct v2 client: %w", err))
		return result
	}

	health, err := client.Health(probeCtx)
	if err != nil {
		result.Message = boundedMessage(fmt.Errorf("unauthenticated health probe: %w", err))
		return result
	}
	if health.Status != harnessv2.HealthStatusOK {
		result.Message = fmt.Sprintf("runtime health status is %q, want %q", health.Status, harnessv2.HealthStatusOK)
		return result
	}

	capabilities, err := getCapabilities(probeCtx, httpClient, target.BaseURL)
	if err != nil {
		result.Message = boundedMessage(fmt.Errorf("unauthenticated capabilities probe: %w", err))
		return result
	}
	result.ObservedCapabilities = capabilities
	if err := validateExactCapabilities(target, capabilities); err != nil {
		result.Message = boundedMessage(err)
		return result
	}

	if err := probeStatusAuthNegatives(probeCtx, httpClient, target); err != nil {
		result.Message = boundedMessage(err)
		return result
	}
	status, err := client.Status(probeCtx)
	if err != nil {
		result.Message = boundedMessage(fmt.Errorf("authenticated status probe: %w", err))
		return result
	}
	result.ObservedStatus = status
	if err := validateExactStatus(target, status); err != nil {
		result.Message = boundedMessage(err)
		return result
	}

	if err := probeMutationAuthNegatives(probeCtx, httpClient, client, target, status.Fence); err != nil {
		result.Message = boundedMessage(err)
		return result
	}

	if target.ProbeLifecycle {
		result.LifecycleProbeExecuted = true
		if err := probeLifecycle(probeCtx, httpClient, client, target, status); err != nil {
			result.Message = boundedMessage(err)
			return result
		}
	}

	result.Passed = true
	result.Message = "orka.harness.v2 conformance passed"
	return result
}

func validateTarget(target Target) error {
	if strings.TrimSpace(target.BaseURL) == "" {
		return fmt.Errorf("base URL is required")
	}
	if strings.TrimSpace(string(target.ExpectedRuntimeInstanceID)) == "" {
		return fmt.Errorf("expected runtime instance ID is required")
	}
	if target.ExpectedControllerEpoch == 0 {
		return fmt.Errorf("expected controller epoch is required")
	}
	if err := target.Profile.Validate(); err != nil {
		return fmt.Errorf("expected runtime profile: %w", err)
	}
	if len(target.Profile.AdapterDigests) != 1 {
		return fmt.Errorf("external AgentRuntime profile must pin exactly one adapter digest")
	}
	digest, err := harnessv2.CanonicalProfileDigest(target.Profile)
	if err != nil {
		return fmt.Errorf("expected runtime profile digest: %w", err)
	}
	if err := harnessv2.ValidateProfileDigest(digest); err != nil {
		return fmt.Errorf("expected runtime profile digest: %w", err)
	}
	if err := target.Limits.Validate(); err != nil {
		return fmt.Errorf("expected protocol limits: %w", err)
	}
	if err := target.WorkspaceGovernance.Validate(); err != nil {
		return fmt.Errorf("expected workspace governance: %w", err)
	}
	if len(target.ControllerBearerToken) < 32 {
		return fmt.Errorf("controller bearer token must be at least 32 bytes")
	}
	if len(target.OperationCapabilitySecret) < harnessv2.MinCapabilitySecretBytes {
		return fmt.Errorf("operation capability secret must be at least %d bytes", harnessv2.MinCapabilitySecretBytes)
	}
	if target.ControlTimeout < 0 {
		return fmt.Errorf("control timeout must not be negative")
	}
	return nil
}

func validateExactCapabilities(target Target, observed *CapabilitiesResponse) error {
	if observed == nil {
		return fmt.Errorf("runtime returned no capabilities")
	}
	expectedDigest, err := harnessv2.CanonicalProfileDigest(target.Profile)
	if err != nil {
		return fmt.Errorf("canonicalize expected runtime profile: %w", err)
	}
	base := observed.CapabilitiesResponse
	if base.RuntimeProfileDigest != expectedDigest {
		return fmt.Errorf("runtime profile digest %q does not match expected %q", base.RuntimeProfileDigest, expectedDigest)
	}
	if base.ProfileDigestSchemaVersion != harnessv2.ProfileDigestSchemaVersion {
		return fmt.Errorf("profile digest schema version %d does not match expected %d", base.ProfileDigestSchemaVersion, harnessv2.ProfileDigestSchemaVersion)
	}
	if base.ACPVersion != target.Profile.ACPProfile {
		return fmt.Errorf("ACP profile %q does not match expected %q", base.ACPVersion, target.Profile.ACPProfile)
	}
	if !maps.Equal(base.AdapterDigests, target.Profile.AdapterDigests) {
		return fmt.Errorf("adapter digest set does not exactly match the registered runtime profile")
	}
	if base.Limits != target.Limits {
		return fmt.Errorf("protocol limits do not exactly match the AgentRuntime registration")
	}
	if base.SupportsDrain != target.SupportsDrain {
		return fmt.Errorf("supportsDrain=%t does not match expected %t", base.SupportsDrain, target.SupportsDrain)
	}
	if base.SupportsPublicationFinalization != target.SupportsPublicationFinalization {
		return fmt.Errorf("supportsPublicationFinalization=%t does not match expected %t", base.SupportsPublicationFinalization, target.SupportsPublicationFinalization)
	}
	if base.SupportsAgentSessionConfiguration {
		return fmt.Errorf("supportsAgentSessionConfiguration must be false because Orka sends no AgentConfiguration to external runtimes")
	}
	if observed.WorkspaceGovernance != target.WorkspaceGovernance {
		return fmt.Errorf("workspace governance claims do not exactly match the AgentRuntime registration")
	}
	if !slices.Contains(base.Provider.ProviderKinds, target.Profile.ProviderKind) {
		return fmt.Errorf("provider kind %q is absent from capabilities", target.Profile.ProviderKind)
	}
	if !slices.Contains(base.Provider.Models, target.Profile.Model) {
		return fmt.Errorf("model %q is absent from capabilities", target.Profile.Model)
	}
	if !base.Provider.SupportsCancel {
		return fmt.Errorf("provider capabilities must support cancellation")
	}
	if target.WorkspaceGovernance.Strict() && !base.Provider.SupportsTools {
		return fmt.Errorf("strict-governed runtime must support prompt-scoped tools")
	}
	if target.WorkspaceGovernance.Strict() &&
		harnessv2.MCPPolicyRequiresPermissionCapability(target.ToolPolicy, target.ApprovalPolicy) &&
		!base.Provider.SupportsPermissions {
		return fmt.Errorf("strict-governed runtime policy requires permission support")
	}
	return nil
}

func validateExactStatus(target Target, status *harnessv2.StatusResponse) error {
	if status == nil {
		return fmt.Errorf("runtime returned no authenticated status")
	}
	expectedDigest, err := harnessv2.CanonicalProfileDigest(target.Profile)
	if err != nil {
		return fmt.Errorf("canonicalize expected runtime profile: %w", err)
	}
	if status.Fence.RuntimeInstanceID != target.ExpectedRuntimeInstanceID {
		return fmt.Errorf("authenticated status runtime instance ID %q does not match expected %q", status.Fence.RuntimeInstanceID, target.ExpectedRuntimeInstanceID)
	}
	if status.Fence.ControllerEpoch != target.ExpectedControllerEpoch {
		return fmt.Errorf("authenticated status controller epoch %d does not match expected %d", status.Fence.ControllerEpoch, target.ExpectedControllerEpoch)
	}
	if status.Fence.RuntimeProfileDigest != expectedDigest {
		return fmt.Errorf("authenticated status profile digest %q does not match expected %q", status.Fence.RuntimeProfileDigest, expectedDigest)
	}
	if status.Fence.ProfileDigestSchemaVersion != harnessv2.ProfileDigestSchemaVersion {
		return fmt.Errorf("authenticated status profile digest schema version %d does not match expected %d", status.Fence.ProfileDigestSchemaVersion, harnessv2.ProfileDigestSchemaVersion)
	}
	if status.Lifecycle != harnessv2.SupervisorLifecycleReady {
		return fmt.Errorf("supervisor lifecycle is %q, want %q", status.Lifecycle, harnessv2.SupervisorLifecycleReady)
	}
	if status.Drain.Requested || !status.Drain.AcceptingNewSessions {
		return fmt.Errorf("external runtime is not accepting new sessions")
	}
	if status.Pressure.ResidentSessions > target.Limits.MaxResidentSessions {
		return fmt.Errorf("resident session pressure %d exceeds limit %d", status.Pressure.ResidentSessions, target.Limits.MaxResidentSessions)
	}
	if status.Pressure.ActivePrompts > target.Limits.MaxConcurrentPrompts {
		return fmt.Errorf("active prompt pressure %d exceeds limit %d", status.Pressure.ActivePrompts, target.Limits.MaxConcurrentPrompts)
	}
	if status.Pressure.PendingPermissions > target.Limits.MaxPendingPermissions {
		return fmt.Errorf("pending permission pressure %d exceeds limit %d", status.Pressure.PendingPermissions, target.Limits.MaxPendingPermissions)
	}
	return nil
}

func getCapabilities(ctx context.Context, client *http.Client, baseURL string) (*CapabilitiesResponse, error) {
	endpoint, err := endpointURL(baseURL, harnessv2.CapabilitiesPath)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("capabilities endpoint returned HTTP %d", resp.StatusCode)
	}
	if err := requireJSON(resp.Header.Get("Content-Type")); err != nil {
		return nil, err
	}
	body, err := readBounded(resp.Body, resp.ContentLength, maxProbeResponseBytes)
	if err != nil {
		return nil, err
	}
	if _, err := harnessv2.CanonicalJSON(body); err != nil {
		return nil, fmt.Errorf("capabilities response is not valid bounded JSON: %w", err)
	}
	var response CapabilitiesResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("decode capabilities response: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, fmt.Errorf("validate capabilities response: %w", err)
	}
	return &response, nil
}

func probeStatusAuthNegatives(ctx context.Context, client *http.Client, target Target) error {
	if err := expectAuthRejected(ctx, client, target.BaseURL, http.MethodGet, harnessv2.StatusPath, "", "", nil); err != nil {
		return fmt.Errorf("unauthenticated status negative probe: %w", err)
	}
	wrongToken := strings.Repeat("w", 32)
	if wrongToken == target.ControllerBearerToken {
		wrongToken = strings.Repeat("z", 32)
	}
	if err := expectAuthRejected(ctx, client, target.BaseURL, http.MethodGet, harnessv2.StatusPath, wrongToken, "", nil); err != nil {
		return fmt.Errorf("wrong-token status negative probe: %w", err)
	}
	// The bearer alone must not read status: without a valid status
	// capability the runtime must reject the request.
	if err := expectAuthRejected(ctx, client, target.BaseURL, http.MethodGet, harnessv2.StatusPath, target.ControllerBearerToken, "", nil); err != nil {
		return fmt.Errorf("bearer-without-capability status negative probe: %w", err)
	}
	return nil
}

func probeMutationAuthNegatives(
	ctx context.Context,
	httpClient *http.Client,
	client *harnessv2.Client,
	target Target,
	poolFence harnessv2.Fence,
) error {
	probeID, err := newProbeID()
	if err != nil {
		return fmt.Errorf("allocate authentication probe identity: %w", err)
	}
	state := newLifecycleProbeState(httpClient, client, target, poolFence, "auth-"+probeID)
	// Vary only authentication so body validation cannot mask the capability
	// checks, regardless of the runtime's validation order.
	request, err := state.createSessionRequest("auth-negative-" + probeID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode authentication probe: %w", err)
	}
	path := harnessv2.RuntimeSessionsPath + "/" + string(state.sessionID)
	// A broken runtime may admit a negative probe. Clean up its unique session
	// on an unexpected response, including an ambiguous transport failure.
	state.sessionCreated = true
	defer state.cleanup(ctx)
	if err := expectAuthRejected(ctx, httpClient, target.BaseURL, http.MethodPut, path, "", "", body); err != nil {
		return fmt.Errorf("unauthenticated mutation negative probe: %w", err)
	}
	wrongToken := strings.Repeat("w", 32)
	if wrongToken == target.ControllerBearerToken {
		wrongToken = strings.Repeat("z", 32)
	}
	if err := expectAuthRejected(ctx, httpClient, target.BaseURL, http.MethodPut, path, wrongToken, "", body); err != nil {
		return fmt.Errorf("wrong-token mutation negative probe: %w", err)
	}
	if err := expectAuthRejected(ctx, httpClient, target.BaseURL, http.MethodPut, path, target.ControllerBearerToken, "", body); err != nil {
		return fmt.Errorf("missing operation-capability negative probe: %w", err)
	}
	if err := expectAuthRejected(ctx, httpClient, target.BaseURL, http.MethodPut, path, target.ControllerBearerToken, "invalid.capability", body); err != nil {
		return fmt.Errorf("invalid operation-capability negative probe: %w", err)
	}
	state.sessionCreated = false
	return nil
}

func expectAuthRejected(
	ctx context.Context,
	client *http.Client,
	baseURL, method, relative, bearer, capability string,
	body []byte,
) error {
	endpoint, err := endpointURL(baseURL, relative)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	if len(body) != 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if capability != "" {
		req.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		return fmt.Errorf("endpoint returned HTTP %d, want 401 or 403", resp.StatusCode)
	}
	return nil
}

func endpointURL(baseURL, relative string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return "", fmt.Errorf("base URL must be absolute HTTP(S)")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || strings.Contains(parsed.Path, "\\") {
		return "", fmt.Errorf("base URL contains unsupported components")
	}
	if !strings.HasPrefix(relative, "/v2/") || strings.ContainsAny(relative, "?#\\\r\n") || strings.Contains(relative, "//") {
		return "", fmt.Errorf("endpoint path is not a canonical /v2 path")
	}
	prefix := strings.TrimSuffix(parsed.Path, "/")
	if prefix == "/" {
		prefix = ""
	}
	parsed.Path = prefix + relative
	return parsed.String(), nil
}

func requireJSON(header string) error {
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return fmt.Errorf("parse response Content-Type: %w", err)
	}
	if !strings.EqualFold(mediaType, "application/json") {
		return fmt.Errorf("response Content-Type %q is unsupported", mediaType)
	}
	return nil
}

func readBounded(reader io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, fmt.Errorf("response Content-Length %d exceeds limit %d", contentLength, limit)
	}
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response exceeds limit %d", limit)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("response body is empty")
	}
	return body, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("JSON response contains a trailing value")
		}
		return fmt.Errorf("JSON response contains trailing data: %w", err)
	}
	return nil
}

func boundedMessage(err error) string {
	if err == nil {
		return ""
	}
	// Repair invalid sequences and truncate on a rune boundary so bounded
	// messages always remain valid UTF-8.
	message := strings.ToValidUTF8(strings.TrimSpace(err.Error()), "�")
	if len(message) <= 1024 {
		return message
	}
	end := 1024
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end]
}
