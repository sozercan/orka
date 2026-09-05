package v2

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const (
	MaxMCPTools          = 128
	MaxMCPArgumentsBytes = 256 << 10
	MaxMCPResultBytes    = 1 << 20

	MCPBrokerCallPath = "/internal/v2/acp/mcp/tools/call"
	// MCPBrokerPoolNamespaceHeader and MCPBrokerPoolUIDHeader carry the
	// non-secret pool identity so the broker can resolve and verify the
	// controller bearer before consuming the request body.
	MCPBrokerPoolNamespaceHeader = "X-Orka-ACP-Pool-Namespace"
	MCPBrokerPoolUIDHeader       = "X-Orka-ACP-Pool-UID"
)

type MCPToolSource string

const (
	MCPToolSourceBrokeredBuiltin MCPToolSource = "brokered_builtin"
	MCPToolSourceBrokeredCustom  MCPToolSource = "brokered_custom"
	MCPToolSourceProviderNative  MCPToolSource = "provider_native"
)

func (s MCPToolSource) Brokered() bool {
	return s == MCPToolSourceBrokeredBuiltin || s == MCPToolSourceBrokeredCustom
}

type MCPToolEffect string

const (
	MCPToolEffectReadOnly      MCPToolEffect = "read_only"
	MCPToolEffectConsequential MCPToolEffect = "consequential"
)

type MCPToolDescriptor struct {
	Name             string          `json:"name"`
	Description      string          `json:"description,omitempty"`
	InputSchema      json.RawMessage `json:"inputSchema,omitempty"`
	Source           MCPToolSource   `json:"source"`
	Effect           MCPToolEffect   `json:"effect"`
	DefinitionDigest string          `json:"definitionDigest,omitempty"`
}

func (d MCPToolDescriptor) Validate() error {
	if err := requireIdentifier("MCP tool name", d.Name); err != nil {
		return err
	}
	if err := validateBoundedString("MCP tool description", d.Description, d.Source.Brokered(), MaxProtocolStringBytes); err != nil {
		return err
	}
	switch d.Source {
	case MCPToolSourceBrokeredBuiltin, MCPToolSourceBrokeredCustom, MCPToolSourceProviderNative:
	default:
		return fmt.Errorf("unsupported MCP tool source %q", d.Source)
	}
	switch d.Effect {
	case MCPToolEffectReadOnly, MCPToolEffectConsequential:
	default:
		return fmt.Errorf("unsupported MCP tool effect %q", d.Effect)
	}
	if d.Source == MCPToolSourceBrokeredCustom {
		if err := validateSHA256Digest(d.DefinitionDigest); err != nil {
			return fmt.Errorf("custom MCP tool definition digest: %w", err)
		}
	} else if d.DefinitionDigest != "" {
		return fmt.Errorf("only custom MCP tools may carry a definition digest")
	}
	if d.Source.Brokered() {
		if len(d.InputSchema) == 0 {
			return fmt.Errorf("brokered MCP tool %q requires an input schema", d.Name)
		}
		if len(d.InputSchema) > MaxRawConfigBytes {
			return fmt.Errorf("MCP tool %q input schema exceeds %d bytes", d.Name, MaxRawConfigBytes)
		}
		canonical, err := CanonicalJSON(d.InputSchema)
		if err != nil {
			return fmt.Errorf("MCP tool %q input schema: %w", d.Name, err)
		}
		if len(canonical) == 0 || canonical[0] != '{' {
			return fmt.Errorf("MCP tool %q input schema must be a JSON object", d.Name)
		}
	} else if len(bytes.TrimSpace(d.InputSchema)) != 0 {
		return fmt.Errorf("provider-native tool %q must not carry an MCP input schema", d.Name)
	}
	return nil
}

type MCPToolPolicy struct {
	AllowedToolNames    []string            `json:"allowedToolNames"`
	DisallowedToolNames []string            `json:"disallowedToolNames"`
	AllowBash           bool                `json:"allowBash"`
	Tools               []MCPToolDescriptor `json:"tools"`
	DescriptorDigest    string              `json:"descriptorDigest"`
}

func (p MCPToolPolicy) Validate() error {
	if len(p.AllowedToolNames) > MaxMCPTools || len(p.DisallowedToolNames) > MaxMCPTools || len(p.Tools) > MaxMCPTools {
		return fmt.Errorf("MCP tool policy exceeds %d tools", MaxMCPTools)
	}
	if err := validateCanonicalToolNames("allowed MCP tool", p.AllowedToolNames); err != nil {
		return err
	}
	if err := validateCanonicalToolNames("disallowed MCP tool", p.DisallowedToolNames); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(p.Tools))
	last := ""
	for i := range p.Tools {
		tool := p.Tools[i]
		if err := tool.Validate(); err != nil {
			return fmt.Errorf("MCP tool descriptor %d: %w", i, err)
		}
		if _, ok := seen[tool.Name]; ok {
			return fmt.Errorf("duplicate MCP tool descriptor %q", tool.Name)
		}
		if last != "" && tool.Name <= last {
			return fmt.Errorf("MCP tool descriptors must be sorted by name")
		}
		if !p.Allows(tool.Name) {
			return fmt.Errorf("MCP tool descriptor %q is not allowed by the canonical policy", tool.Name)
		}
		seen[tool.Name] = struct{}{}
		last = tool.Name
	}
	for _, name := range p.AllowedToolNames {
		if !p.Allows(name) {
			continue
		}
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("allowed MCP tool %q has no canonical descriptor", name)
		}
	}
	encoded, err := CanonicalValue(struct {
		AllowedToolNames    []string            `json:"allowedToolNames"`
		DisallowedToolNames []string            `json:"disallowedToolNames,omitempty"`
		AllowBash           bool                `json:"allowBash"`
		Tools               []MCPToolDescriptor `json:"tools"`
	}{
		AllowedToolNames: p.AllowedToolNames, DisallowedToolNames: p.DisallowedToolNames,
		AllowBash: p.AllowBash, Tools: p.Tools,
	})
	if err != nil {
		return err
	}
	if len(encoded) > MaxRawConfigBytes {
		return fmt.Errorf("MCP tool policy exceeds %d bytes", MaxRawConfigBytes)
	}
	digest, err := CanonicalMCPToolDescriptorDigest(p.Tools)
	if err != nil {
		return err
	}
	if p.DescriptorDigest != digest {
		return fmt.Errorf("MCP tool descriptor digest does not match canonical descriptors")
	}
	return nil
}

func (p MCPToolPolicy) Allows(name string) bool {
	if strings.TrimSpace(name) == "" || !slices.Contains(p.AllowedToolNames, name) || slices.Contains(p.DisallowedToolNames, name) {
		return false
	}
	return p.AllowBash || !strings.EqualFold(name, "bash")
}

func (p MCPToolPolicy) Descriptor(name string) (MCPToolDescriptor, bool) {
	if !p.Allows(name) {
		return MCPToolDescriptor{}, false
	}
	index, ok := slices.BinarySearchFunc(p.Tools, name, func(tool MCPToolDescriptor, target string) int {
		return strings.Compare(tool.Name, target)
	})
	if !ok {
		return MCPToolDescriptor{}, false
	}
	return p.Tools[index], true
}

type MCPApprovalPolicy struct {
	RequiredTools []string `json:"requiredTools,omitempty"`
}

func (p MCPApprovalPolicy) Validate(toolPolicy MCPToolPolicy) error {
	if len(p.RequiredTools) > MaxMCPTools {
		return fmt.Errorf("MCP approval policy exceeds %d tools", MaxMCPTools)
	}
	if err := validateCanonicalToolNames("approval-required MCP tool", p.RequiredTools); err != nil {
		return err
	}
	for _, name := range p.RequiredTools {
		descriptor, ok := toolPolicy.Descriptor(name)
		if !ok || !descriptor.Source.Brokered() {
			return fmt.Errorf("approval-required MCP tool %q is not an allowed brokered tool", name)
		}
	}
	return nil
}

func (p MCPApprovalPolicy) Requires(toolName string) bool {
	return slices.Contains(p.RequiredTools, toolName)
}

type MCPPolicyConfiguration struct {
	ToolPolicyDigest       string            `json:"toolPolicyDigest"`
	ApprovalPolicyDigest   string            `json:"approvalPolicyDigest"`
	MCPConfigurationDigest string            `json:"mcpConfigurationDigest"`
	ToolPolicy             MCPToolPolicy     `json:"toolPolicy"`
	ApprovalPolicy         MCPApprovalPolicy `json:"approvalPolicy"`
}

// MCPPolicyRequiresPermissionCapability reports whether enforcing one policy
// requires the runtime to emit ACP permission requests. Brokered tools need
// that path only when approval is required; provider-native tools always need
// it so the supervisor can govern calls that bypass the MCP broker.
func MCPPolicyRequiresPermissionCapability(toolPolicy MCPToolPolicy, approvalPolicy MCPApprovalPolicy) bool {
	if len(approvalPolicy.RequiredTools) > 0 {
		return true
	}
	return slices.ContainsFunc(toolPolicy.Tools, func(tool MCPToolDescriptor) bool {
		return tool.Source == MCPToolSourceProviderNative
	})
}

func (c MCPPolicyConfiguration) validate() error {
	if err := validateSHA256Digest(c.ToolPolicyDigest); err != nil {
		return fmt.Errorf("tool policy digest: %w", err)
	}
	if err := validateSHA256Digest(c.ApprovalPolicyDigest); err != nil {
		return fmt.Errorf("approval policy digest: %w", err)
	}
	if err := validateSHA256Digest(c.MCPConfigurationDigest); err != nil {
		return fmt.Errorf("MCP configuration digest: %w", err)
	}
	if err := c.ToolPolicy.Validate(); err != nil {
		return fmt.Errorf("MCP tool policy: %w", err)
	}
	if err := c.ApprovalPolicy.Validate(c.ToolPolicy); err != nil {
		return fmt.Errorf("MCP approval policy: %w", err)
	}
	toolDigest, err := CanonicalRuntimeToolPolicyDigest(c.ToolPolicy.AllowedToolNames, c.ToolPolicy.DisallowedToolNames, c.ToolPolicy.AllowBash)
	if err != nil || toolDigest != c.ToolPolicyDigest {
		return fmt.Errorf("tool policy digest does not match embedded canonical policy")
	}
	approvalDigest, err := CanonicalMCPApprovalPolicyDigest(c.ApprovalPolicy)
	if err != nil || approvalDigest != c.ApprovalPolicyDigest {
		return fmt.Errorf("approval policy digest does not match embedded canonical policy")
	}
	mcpDigest, err := CanonicalMCPConfigurationDigest(c.ToolPolicy.AllowedToolNames)
	if err != nil || mcpDigest != c.MCPConfigurationDigest {
		return fmt.Errorf("MCP configuration digest does not match embedded canonical policy")
	}
	return nil
}

func (c MCPPolicyConfiguration) ValidateProfile(profile RuntimeProfile) error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.ToolPolicyDigest != profile.ToolPolicyDigest ||
		c.ApprovalPolicyDigest != profile.ApprovalPolicyDigest ||
		c.MCPConfigurationDigest != profile.MCPConfigurationDigest {
		return fmt.Errorf("MCP policy configuration does not match runtime profile")
	}
	return nil
}

func (c MCPPolicyConfiguration) Matches(other MCPPolicyConfiguration) bool {
	if c.ToolPolicyDigest != other.ToolPolicyDigest || c.ApprovalPolicyDigest != other.ApprovalPolicyDigest ||
		c.MCPConfigurationDigest != other.MCPConfigurationDigest ||
		c.ToolPolicy.DescriptorDigest != other.ToolPolicy.DescriptorDigest {
		return false
	}
	left, leftErr := CanonicalValue(c)
	right, rightErr := CanonicalValue(other)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

type MCPApprovalEvidence struct {
	PermissionRequestID PermissionRequestID `json:"permissionRequestID"`
	ToolCallID          string              `json:"toolCallID"`
	ToolName            string              `json:"toolName"`
	GrantedAt           time.Time           `json:"grantedAt"`
	ExpiresAt           time.Time           `json:"expiresAt"`
	Reusable            bool                `json:"reusable,omitempty"`
}

func (e MCPApprovalEvidence) ValidateFor(toolName string, now time.Time) error {
	if err := requireIdentifier("MCP approval permission request ID", string(e.PermissionRequestID)); err != nil {
		return err
	}
	if err := requireIdentifier("MCP approval tool call ID", e.ToolCallID); err != nil {
		return err
	}
	if e.ToolName != toolName {
		return fmt.Errorf("MCP approval evidence tool %q does not match call tool %q", e.ToolName, toolName)
	}
	if err := validateTimestamp("MCP approval grant timestamp", e.GrantedAt); err != nil {
		return err
	}
	if err := validateTimestamp("MCP approval expiry", e.ExpiresAt); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !e.ExpiresAt.After(now) || e.ExpiresAt.Before(e.GrantedAt) {
		return fmt.Errorf("MCP approval evidence is expired or invalid")
	}
	return nil
}

func CanonicalRuntimeToolPolicyDigest(allowed, disallowed []string, allowBash bool) (string, error) {
	return canonicalACPDomainDigest("tool-policy", map[string]any{
		"allowed": allowed, "disallowed": disallowed, "allowBash": allowBash,
	})
}

func CanonicalMCPApprovalPolicyDigest(policy MCPApprovalPolicy) (string, error) {
	return canonicalACPDomainDigest("approval-policy", map[string]any{"requiredTools": policy.RequiredTools})
}

func CanonicalMCPConfigurationDigest(allowed []string) (string, error) {
	return canonicalACPDomainDigest("mcp-configuration", map[string]any{"brokeredTools": allowed, "promptScoped": true})
}

func CanonicalMCPToolDescriptorDigest(tools []MCPToolDescriptor) (string, error) {
	return canonicalACPDomainDigest("mcp-tool-descriptors", tools)
}

func canonicalACPDomainDigest(domain string, value any) (string, error) {
	canonical, err := CanonicalValue(value)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = io.WriteString(hash, "orka.acp."+domain)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(canonical)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func validateCanonicalToolNames(label string, names []string) error {
	last := ""
	for _, name := range names {
		if err := requireIdentifier(label, name); err != nil {
			return err
		}
		if last != "" && name <= last {
			return fmt.Errorf("%s names must be unique and sorted", label)
		}
		last = name
	}
	return nil
}

type MCPToolCall struct {
	CallID    string               `json:"callID"`
	ToolName  string               `json:"toolName"`
	Arguments json.RawMessage      `json:"arguments"`
	Approval  *MCPApprovalEvidence `json:"approval,omitempty"`
}

func (c MCPToolCall) ValidateAt(auth PromptMCPAuthorization, now time.Time) (MCPToolDescriptor, error) {
	if err := requireIdentifier("MCP call ID", c.CallID); err != nil {
		return MCPToolDescriptor{}, err
	}
	if err := requireIdentifier("MCP call tool name", c.ToolName); err != nil {
		return MCPToolDescriptor{}, err
	}
	if len(c.Arguments) == 0 || len(c.Arguments) > MaxMCPArgumentsBytes {
		return MCPToolDescriptor{}, fmt.Errorf("MCP call arguments must be a JSON object no larger than %d bytes", MaxMCPArgumentsBytes)
	}
	canonical, err := CanonicalJSON(c.Arguments)
	if err != nil || len(canonical) == 0 || canonical[0] != '{' {
		return MCPToolDescriptor{}, fmt.Errorf("MCP call arguments must be a canonicalizable JSON object")
	}
	descriptor, ok := auth.ToolPolicy.Descriptor(c.ToolName)
	if !ok || !descriptor.Source.Brokered() {
		return MCPToolDescriptor{}, fmt.Errorf("MCP tool %q is not an allowed brokered tool", c.ToolName)
	}
	if auth.ApprovalPolicy.Requires(c.ToolName) {
		if c.Approval == nil {
			return MCPToolDescriptor{}, fmt.Errorf("MCP tool %q requires approval", c.ToolName)
		}
		if err := c.Approval.ValidateFor(c.ToolName, now); err != nil {
			return MCPToolDescriptor{}, err
		}
	} else if c.Approval != nil {
		return MCPToolDescriptor{}, fmt.Errorf("MCP tool %q must not carry unexpected approval evidence", c.ToolName)
	}
	return descriptor, nil
}

type MCPBrokerCallRequest struct {
	Protocol      string                 `json:"protocol"`
	Namespace     string                 `json:"namespace"`
	SessionState  RuntimeSessionState    `json:"sessionState"`
	Metadata      MutationMetadata       `json:"metadata"`
	Lease         PromptLease            `json:"lease"`
	Authorization PromptMCPAuthorization `json:"authorization"`
	Call          MCPToolCall            `json:"call"`
}

func (r MCPBrokerCallRequest) ValidateAt(now time.Time) (MCPToolDescriptor, error) {
	if err := validateProtocol(r.Protocol); err != nil {
		return MCPToolDescriptor{}, err
	}
	if err := requireIdentifier("MCP broker namespace", r.Namespace); err != nil {
		return MCPToolDescriptor{}, err
	}
	if r.SessionState != RuntimeSessionStatePromptRunning {
		return MCPToolDescriptor{}, fmt.Errorf("MCP broker calls require prompt_running session state")
	}
	if err := r.Metadata.validateAt(now, metadataRequirements{session: true, task: true, prompt: true}); err != nil {
		return MCPToolDescriptor{}, fmt.Errorf("metadata: %w", err)
	}
	if err := r.Lease.ValidateAt(now, 0, 0); err != nil {
		return MCPToolDescriptor{}, fmt.Errorf("prompt lease: %w", err)
	}
	if err := r.Authorization.ValidateForAt(r.Metadata, r.Lease, now); err != nil {
		return MCPToolDescriptor{}, err
	}
	if !r.Authorization.AuthorizedAt(r.SessionState, r.Lease, now) {
		return MCPToolDescriptor{}, fmt.Errorf("prompt-scoped MCP authorization is not active")
	}
	if r.Metadata.ExpiresAt.After(r.Authorization.ExpiresAt) || r.Metadata.ExpiresAt.After(r.Lease.ExpiresAt) {
		return MCPToolDescriptor{}, fmt.Errorf("MCP broker operation expiry must not outlive prompt authority")
	}
	descriptor, err := r.Call.ValidateAt(r.Authorization, now)
	if err != nil {
		return MCPToolDescriptor{}, err
	}
	if err := r.Metadata.ValidateDigest(r); err != nil {
		return MCPToolDescriptor{}, err
	}
	return descriptor, nil
}

type MCPBrokerCallResponse struct {
	Protocol string          `json:"protocol"`
	CallID   string          `json:"callID"`
	Result   json.RawMessage `json:"result"`
	IsError  bool            `json:"isError,omitempty"`
	Replayed bool            `json:"replayed,omitempty"`
}

func (r MCPBrokerCallResponse) Validate() error {
	if err := validateProtocol(r.Protocol); err != nil {
		return err
	}
	if err := requireIdentifier("MCP response call ID", r.CallID); err != nil {
		return err
	}
	if len(r.Result) == 0 || len(r.Result) > MaxMCPResultBytes {
		return fmt.Errorf("MCP response result must be valid JSON no larger than %d bytes", MaxMCPResultBytes)
	}
	_, err := CanonicalJSON(r.Result)
	return err
}
