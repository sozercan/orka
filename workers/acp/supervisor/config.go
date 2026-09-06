package supervisor

import (
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

const OperationCapabilityHeader = "X-Orka-Operation-Capability"

const sessionIdentityExhaustionReserve = 1

type ProviderProfile struct {
	Kind                  string
	Model                 string
	Command               string
	Args                  []string
	Environment           map[string]string
	ProjectSession        func(harnessv2.CreateRuntimeSessionRequest, acp.SessionPaths, ProviderProxyBinding) (ProviderSessionProjection, error)
	EnvironmentForSession func(harnessv2.CreateRuntimeSessionRequest, acp.SessionPaths, ProviderProxyBinding) (map[string]string, error)
	AuthMethodID          string
	AdapterName           string
	AdapterDigest         string
	PrepareSession        func(acp.SessionPaths) error
}

type ProviderSessionProjection struct {
	AdditionalArgs []string
	Environment    map[string]string
	NewSessionMeta acp.Meta
	// AgentDiagnosticFilter recognizes provider CLI diagnostics the adapter
	// wrote into the ACP agent message stream instead of its own log. Nil
	// forwards every chunk.
	AgentDiagnosticFilter *AgentDiagnosticFilter
}

// AgentDiagnosticFilter recognizes assistant text chunks that are provider
// CLI diagnostics rather than model output. Recognized chunks are withheld
// from the harness event stream and the terminal assistant text and logged by
// the supervisor instead. The recognizer sees only the chunk text; the
// supervisor anchors it on prompt state it can prove, so a model chunk that
// merely repeats a diagnostic sentence is forwarded: Startup diagnostics are
// withheld only when the session received them from the child
// (acp.PromptEvent.ReceivedAt, stamped before any buffering) before the
// provider proxy began relaying the prompt's first non-error inference
// response. Model output can only be derived from those bytes, while the CLI
// emits its startup diagnostics ahead of its first inference request.
//
// The recognizer returns a summary the supervisor may log. Chunk text is
// child-controlled and the child holds session credentials, so the summary
// must be built only from values the supervisor already knows (such as the
// session's own exclusion list), never from the chunk itself.
type AgentDiagnosticFilter struct {
	Startup func(text string) (summary string, ok bool)
}

type ArtifactUploader interface {
	UploadWorkspaceDelta(context.Context, harnessv2.CreateWorkspaceDeltaRequest, []byte, string) (harnessv2.ArtifactReference, error)
}

type WorkspaceMaterializer interface {
	Materialize(context.Context, harnessv2.CreateRuntimeSessionRequest, string) error
}

type WorkspaceMaterializerFunc func(context.Context, harnessv2.CreateRuntimeSessionRequest, string) error

func (f WorkspaceMaterializerFunc) Materialize(ctx context.Context, request harnessv2.CreateRuntimeSessionRequest, destination string) error {
	return f(ctx, request, destination)
}

type Config struct {
	ListenAddress string
	Fence         harnessv2.Fence
	Capabilities  harnessv2.CapabilitiesResponse
	Provider      ProviderProfile

	ControllerBearerToken string
	CapabilitySecret      []byte
	RequireCapabilities   bool

	SessionBaseDir string
	// DurableWorkspaceDir, when set, hosts each logical session's repository
	// workspace on the provider's durable data volume so a data-only cold
	// suspension preserves exactly that directory. The session root, home,
	// temporary files, XDG state, and every credential stay under the
	// ephemeral SessionBaseDir tree - EXCEPT the non-secret session identity
	// allocator state (high-water mark, range, lock), which moves to
	// <DurableWorkspaceDir>/.session-identity so a cold boot can never reuse
	// a pre-suspension child UID/GID; snapshots must preserve it.
	DurableWorkspaceDir   string
	UIDAllocator          *acp.UIDAllocator
	ProviderProxy         ProviderProxyConfig
	MCPBroker             MCPBroker
	WorkspaceMaterializer WorkspaceMaterializer
	ArtifactUploader      ArtifactUploader
	DeltaOptions          workspacedelta.Options
	// E2EPromptWriteFaultRecorder persists direct-pool fault consumption
	// outside the runtime Pod so replacement cannot re-arm the test fault.
	E2EPromptWriteFaultRecorder E2EPromptWriteFaultRecorder

	InitializeTimeout time.Duration
	PermissionTimeout time.Duration
	CancelGrace       time.Duration

	// E2EPromptWriteAmbiguityMarker enables a test-only transport fault for an
	// exact prompt marker. The supervisor aborts the first authenticated request
	// for each operation after fully decoding and validating it, but before
	// recording the operation. The one-shot record survives runtime and
	// supervisor recreation so live conformance exposes an accidental retry.
	E2EPromptWriteAmbiguityMarker string
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.ListenAddress) == "" {
		return fmt.Errorf("listen address is required")
	}
	if err := c.Fence.Validate(false); err != nil {
		return fmt.Errorf("supervisor fence: %w", err)
	}
	if c.Fence.RuntimeSessionUID != "" || c.Fence.RuntimeSessionGeneration != 0 {
		return fmt.Errorf("supervisor base fence must not contain runtime-session identity")
	}
	if err := c.Capabilities.Validate(); err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}
	if c.Capabilities.RuntimeProfileDigest != c.Fence.RuntimeProfileDigest {
		return fmt.Errorf("capabilities and fence runtime profile digests differ")
	}
	if c.Capabilities.ProfileDigestSchemaVersion != c.Fence.ProfileDigestSchemaVersion {
		return fmt.Errorf("capabilities and fence profile digest schema versions differ")
	}
	if strings.TrimSpace(c.Provider.Kind) == "" || strings.TrimSpace(c.Provider.Model) == "" {
		return fmt.Errorf("provider kind and model are required")
	}
	if len(c.Capabilities.Provider.ProviderKinds) != 1 || c.Capabilities.Provider.ProviderKinds[0] != c.Provider.Kind {
		return fmt.Errorf("provider capability kind does not match configured provider")
	}
	if c.Provider.Command == "" || !filepath.IsAbs(c.Provider.Command) {
		return fmt.Errorf("provider adapter command must be absolute")
	}
	if strings.TrimSpace(c.Provider.AdapterName) == "" {
		return fmt.Errorf("provider adapter name is required")
	}
	if got := c.Capabilities.AdapterDigests[c.Provider.AdapterName]; got == "" || got != c.Provider.AdapterDigest {
		return fmt.Errorf("provider adapter digest does not match advertised capability")
	}
	if len(c.ControllerBearerToken) < 32 {
		return fmt.Errorf("controller bearer token must be at least 32 bytes")
	}
	if c.RequireCapabilities && len(c.CapabilitySecret) < harnessv2.MinCapabilitySecretBytes {
		return fmt.Errorf("operation capability secret must be at least %d bytes", harnessv2.MinCapabilitySecretBytes)
	}
	if c.SessionBaseDir == "" || !filepath.IsAbs(c.SessionBaseDir) {
		return fmt.Errorf("session base directory must be absolute")
	}
	if c.DurableWorkspaceDir != "" && !filepath.IsAbs(c.DurableWorkspaceDir) {
		return fmt.Errorf("durable workspace directory must be absolute when set")
	}
	if c.DurableWorkspaceDir != "" {
		relative, err := filepath.Rel(filepath.Clean(c.DurableWorkspaceDir), filepath.Clean(c.SessionBaseDir))
		if err != nil {
			return fmt.Errorf("compare session and durable workspace directories: %w", err)
		}
		if relative == "." || (relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)) {
			return fmt.Errorf("session base directory must not equal or be beneath the durable workspace directory")
		}
	}
	if c.UIDAllocator == nil {
		return fmt.Errorf("UID allocator is required")
	}
	requiredIdentityCapacity := uint64(c.Capabilities.Limits.MaxResidentSessions) + sessionIdentityExhaustionReserve
	if uint64(c.UIDAllocator.Capacity()) < requiredIdentityCapacity {
		return fmt.Errorf("UID allocator capacity must provide at least %d resident identities plus the exhaustion reserve", c.Capabilities.Limits.MaxResidentSessions)
	}
	if c.WorkspaceMaterializer == nil {
		return fmt.Errorf("workspace materializer is required")
	}
	if _, _, err := c.ProviderProxy.normalized(); err != nil {
		return err
	}
	if c.MCPBroker == nil {
		return fmt.Errorf("controller MCP broker is required")
	}
	if c.InitializeTimeout < 0 || c.PermissionTimeout < 0 || c.CancelGrace < 0 {
		return fmt.Errorf("runtime timeouts must be non-negative")
	}
	if marker := c.E2EPromptWriteAmbiguityMarker; marker != "" {
		if strings.TrimSpace(marker) != marker || len(marker) > 128 ||
			!strings.HasPrefix(marker, "ORKA_E2E_") || !strings.HasSuffix(marker, "_OK") {
			return fmt.Errorf("E2E prompt write ambiguity marker must be an ORKA_E2E_*_OK token")
		}
		for _, value := range marker {
			if (value < 'A' || value > 'Z') && (value < '0' || value > '9') && value != '_' {
				return fmt.Errorf("E2E prompt write ambiguity marker must contain only uppercase ASCII letters, digits, and underscores")
			}
		}
		if c.DurableWorkspaceDir == "" && c.E2EPromptWriteFaultRecorder == nil {
			return fmt.Errorf("E2E prompt write fault recorder is required without a durable workspace")
		}
	}
	return nil
}

func (c Config) bearerMatches(value string) bool {
	value = strings.TrimSpace(value)
	token, ok := strings.CutPrefix(value, "Bearer ")
	if !ok || token == "" || strings.TrimSpace(token) != token || len(token) != len(c.ControllerBearerToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(c.ControllerBearerToken)) == 1
}

func EmptyWorkspaceMaterializer() WorkspaceMaterializer {
	return WorkspaceMaterializerFunc(func(_ context.Context, request harnessv2.CreateRuntimeSessionRequest, destination string) error {
		if request.Workspace.Baseline.Artifact != nil {
			return fmt.Errorf("workspace artifact materialization is not configured")
		}
		if err := os.MkdirAll(destination, 0o700); err != nil {
			return err
		}
		return nil
	})
}
