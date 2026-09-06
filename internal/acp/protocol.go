package acp

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// ProtocolVersion is the stable ACP wire protocol negotiated during initialize.
	ProtocolVersion = 1

	MethodInitialize        = "initialize"
	MethodAuthenticate      = "authenticate"
	MethodSessionNew        = "session/new"
	MethodSessionPrompt     = "session/prompt"
	MethodSessionCancel     = "session/cancel"
	MethodSessionUpdate     = "session/update"
	MethodRequestPermission = "session/request_permission"
	MethodCancelRequest     = "$/cancel_request"
)

type Meta map[string]any

const (
	protectedSessionMetaPrefix      = "orka."
	sessionMetaRuntimeSessionID     = protectedSessionMetaPrefix + "runtimeSessionID"
	sessionMetaGeneration           = protectedSessionMetaPrefix + "generation"
	sessionMetaRuntimeProfileDigest = protectedSessionMetaPrefix + "profileDigest"
)

// MergeNewSessionMeta combines provider-specific session metadata with
// Orka-owned runtime identity metadata. Provider metadata must never be able to
// replace or shadow the Orka fence, including through case variants.
func MergeNewSessionMeta(provider, protected Meta) (Meta, error) {
	merged := make(Meta, len(provider)+len(protected))
	for key, value := range provider {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("provider session metadata key is required")
		}
		if strings.HasPrefix(strings.ToLower(key), protectedSessionMetaPrefix) {
			return nil, fmt.Errorf("provider session metadata key %q uses protected Orka namespace", key)
		}
		merged[key] = value
	}
	for key, value := range protected {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("protected session metadata key is required")
		}
		if _, exists := merged[key]; exists {
			return nil, fmt.Errorf("provider session metadata key %q collides with protected metadata", key)
		}
		merged[key] = value
	}
	return merged, nil
}

type Implementation struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type FileSystemCapabilities struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

type ClientCapabilities struct {
	FS       FileSystemCapabilities `json:"fs"`
	Terminal bool                   `json:"terminal"`
	Session  map[string]any         `json:"session,omitempty"`
	Meta     Meta                   `json:"_meta,omitempty"`
}

type PromptCapabilities struct {
	Image           bool `json:"image"`
	Audio           bool `json:"audio"`
	EmbeddedContext bool `json:"embeddedContext"`
}

type AgentCapabilities struct {
	LoadSession         bool               `json:"loadSession"`
	PromptCapabilities  PromptCapabilities `json:"promptCapabilities"`
	MCPCapabilities     map[string]any     `json:"mcpCapabilities"`
	SessionCapabilities map[string]any     `json:"sessionCapabilities"`
	Auth                map[string]any     `json:"auth"`
	Meta                Meta               `json:"_meta,omitempty"`
}

type AuthMethod struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Meta        Meta   `json:"_meta,omitempty"`
}

type InitializeRequest struct {
	ProtocolVersion    int                `json:"protocolVersion"`
	ClientCapabilities ClientCapabilities `json:"clientCapabilities"`
	ClientInfo         *Implementation    `json:"clientInfo,omitempty"`
	Meta               Meta               `json:"_meta,omitempty"`
}

type AuthenticateRequest struct {
	MethodID string `json:"methodId"`
	Meta     Meta   `json:"_meta,omitempty"`
}

type AuthenticateResponse struct {
	Meta Meta `json:"_meta,omitempty"`
}

type InitializeResponse struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities AgentCapabilities `json:"agentCapabilities"`
	AgentInfo         *Implementation   `json:"agentInfo,omitempty"`
	AuthMethods       []AuthMethod      `json:"authMethods,omitempty"`
	Meta              Meta              `json:"_meta,omitempty"`
}

type EnvVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Meta  Meta   `json:"_meta,omitempty"`
}

type HTTPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Meta  Meta   `json:"_meta,omitempty"`
}

type MCPServer struct {
	Type    string        `json:"type,omitempty"`
	Name    string        `json:"name"`
	Command string        `json:"command,omitempty"`
	Args    []string      `json:"args,omitempty"`
	Env     []EnvVariable `json:"env,omitempty"`
	URL     string        `json:"url,omitempty"`
	Headers []HTTPHeader  `json:"headers,omitempty"`
	Meta    Meta          `json:"_meta,omitempty"`
}

type NewSessionRequest struct {
	CWD                   string      `json:"cwd"`
	AdditionalDirectories []string    `json:"additionalDirectories,omitempty"`
	MCPServers            []MCPServer `json:"mcpServers"`
	Meta                  Meta        `json:"_meta,omitempty"`
}

type NewSessionResponse struct {
	SessionID     string            `json:"sessionId"`
	Modes         json.RawMessage   `json:"modes,omitempty"`
	ConfigOptions []json.RawMessage `json:"configOptions,omitempty"`
	Meta          Meta              `json:"_meta,omitempty"`
}

type ContentBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Data     string          `json:"data,omitempty"`
	MIMEType string          `json:"mimeType,omitempty"`
	URI      string          `json:"uri,omitempty"`
	Name     string          `json:"name,omitempty"`
	Resource json.RawMessage `json:"resource,omitempty"`
	Meta     Meta            `json:"_meta,omitempty"`
}

func Text(text string) ContentBlock {
	return ContentBlock{Type: "text", Text: text}
}

type PromptRequest struct {
	SessionID string         `json:"sessionId"`
	Prompt    []ContentBlock `json:"prompt"`
	Meta      Meta           `json:"_meta,omitempty"`
}

type StopReason string

const (
	StopReasonEndTurn         StopReason = "end_turn"
	StopReasonMaxTokens       StopReason = "max_tokens"
	StopReasonMaxTurnRequests StopReason = "max_turn_requests"
	StopReasonRefusal         StopReason = "refusal"
	StopReasonCancelled       StopReason = "cancelled"
)

func (r StopReason) Validate() error {
	switch r {
	case StopReasonEndTurn, StopReasonMaxTokens, StopReasonMaxTurnRequests, StopReasonRefusal, StopReasonCancelled:
		return nil
	default:
		return fmt.Errorf("unsupported ACP stop reason %q", r)
	}
}

func (r StopReason) Successful() bool { return r == StopReasonEndTurn }

type PromptResponse struct {
	StopReason StopReason `json:"stopReason"`
	Meta       Meta       `json:"_meta,omitempty"`
}

type CancelNotification struct {
	SessionID string `json:"sessionId"`
	Meta      Meta   `json:"_meta,omitempty"`
}

type SessionNotification struct {
	SessionID string          `json:"sessionId"`
	Update    json.RawMessage `json:"update"`
	Meta      Meta            `json:"_meta,omitempty"`
}

type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Meta     Meta   `json:"_meta,omitempty"`
}

type RequestPermissionRequest struct {
	SessionID string             `json:"sessionId"`
	ToolCall  json.RawMessage    `json:"toolCall"`
	Options   []PermissionOption `json:"options"`
	Meta      Meta               `json:"_meta,omitempty"`
}

type RequestPermissionOutcome struct {
	Outcome  string `json:"outcome"`
	OptionID string `json:"optionId,omitempty"`
	Meta     Meta   `json:"_meta,omitempty"`
}

const (
	permissionOutcomeCancelled = "cancelled"
	permissionOutcomeSelected  = "selected"
)

func CancelledPermissionOutcome() RequestPermissionOutcome {
	return RequestPermissionOutcome{Outcome: permissionOutcomeCancelled}
}

func SelectedPermissionOutcome(optionID string) RequestPermissionOutcome {
	return RequestPermissionOutcome{Outcome: permissionOutcomeSelected, OptionID: optionID}
}

type RequestPermissionResponse struct {
	Outcome RequestPermissionOutcome `json:"outcome"`
	Meta    Meta                     `json:"_meta,omitempty"`
}

type CancelRequestNotification struct {
	RequestID json.RawMessage `json:"requestId"`
}
