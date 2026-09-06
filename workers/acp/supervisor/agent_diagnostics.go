package supervisor

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/acp"
)

// Built-in provider CLIs occasionally write operator-facing diagnostics into
// the ACP agent message stream instead of their own log. Left alone, those
// chunks are compacted together with the model's text and reach Task results,
// chat, and monitor reviews as if the model had said them. A provider
// projection declares a recognizer for the exact chunks its CLI emits
// (AgentDiagnosticFilter); the prompt stream withholds them before compaction,
// anchored on prompt state the supervisor can prove, and logs them so the
// CLI's report still reaches operators.

const (
	copilotDisabledToolsDiagnosticPrefix       = "Info: Disabled tools: "
	copilotUnknownExcludedToolDiagnosticPrefix = "Info: Unknown tool name in the tool excludedlist: "
)

// copilotStartupDiagnostic recognizes the tool-exclusion report GitHub Copilot
// CLI 1.0.77 emits as agent_message_chunk updates at the start of a prompt in
// --acp mode; --log-level none does not silence it. Each line arrives as its
// own chunk, ahead of the CLI's first inference request, so a chunk is
// recognized only when its entire text is one of:
//
//   - "Info: Disabled tools: a, b". The CLI lists every tool it removed from
//     the model's catalog, folding tools it disabled for its own reasons into
//     the same line, so the line is recognized when it is a comma-separated
//     list of tool identifiers that names at least one tool this session
//     excluded.
//   - "Info: Unknown tool name in the tool excludedlist: \"a\"" for exactly a
//     name this session passed in --excluded-tools that the CLI's catalog does
//     not contain.
//
// Anchoring on the session's own exclusion list means a chunk about some
// other tool is never recognized. The CLI forwards model deltas verbatim and
// without a messageId, so the supervisor additionally withholds startup
// diagnostics only when they were received before the provider proxy began
// relaying the prompt's first inference response.
//
// The returned summary names only tools from the session's exclusion list
// and counts the rest: the chunk is child-controlled and a name-shaped entry
// could carry a session credential, so no chunk text reaches the log.
func copilotStartupDiagnostic(excludedTools []string) func(string) (string, bool) {
	excluded := make(map[string]struct{}, len(excludedTools))
	for _, name := range excludedTools {
		excluded[name] = struct{}{}
	}
	return func(text string) (string, bool) {
		if list, ok := strings.CutPrefix(text, copilotDisabledToolsDiagnosticPrefix); ok {
			return copilotDisabledToolsSummary(list, excluded)
		}
		if quoted, ok := strings.CutPrefix(text, copilotUnknownExcludedToolDiagnosticPrefix); ok {
			name, err := strconv.Unquote(quoted)
			if err != nil {
				return "", false
			}
			if _, excludedName := excluded[name]; !excludedName {
				return "", false
			}
			return "unknown excluded tool " + strconv.Quote(name), true
		}
		return "", false
	}
}

func copilotDisabledToolsSummary(list string, excluded map[string]struct{}) (string, bool) {
	var known []string
	others := 0
	for entry := range strings.SplitSeq(list, ",") {
		name := strings.TrimSpace(entry)
		if !copilotToolIdentifier(name) {
			return "", false
		}
		if _, ok := excluded[name]; ok {
			known = append(known, name)
		} else {
			others++
		}
	}
	if len(known) == 0 {
		return "", false
	}
	summary := "disabled tools: " + strings.Join(known, ", ")
	if others > 0 {
		summary += " (+" + strconv.Itoa(others) + " not in the session exclusion list)"
	}
	return summary, true
}

// copilotToolIdentifier reports whether name is shaped like a Copilot tool
// identifier (built-in names such as list_bash, MCP names such as
// github-mcp-server-search_code).
func copilotToolIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.':
		default:
			return false
		}
	}
	return true
}

// withholdAgentDiagnostic reports whether event is an assistant text chunk the
// session's provider projection recognizes as a CLI startup diagnostic under
// the anchoring rule documented on AgentDiagnosticFilter. A withheld chunk is
// logged and never reaches compaction, the harness event stream, or the
// terminal assistant text. Only the recognizer's summary is logged: the chunk
// text is child-controlled and may carry session credentials.
func withholdAgentDiagnostic(state *sessionState, prompt *promptState, event acp.PromptEvent) bool {
	filter := state.agentDiagnosticFilter
	if filter == nil || filter.Startup == nil {
		return false
	}
	text, ok := assistantMessageText(event)
	if !ok || text == "" {
		return false
	}
	summary, recognized := filter.Startup(text)
	if !recognized {
		return false
	}
	// The receipt time is stamped when the session received the chunk from
	// the child, before any buffering, so a startup diagnostic queued behind
	// prompt acceptance or a slow consumer keeps the phase it was emitted in.
	promptID := prompt.request.Metadata.PromptID
	if state.providerProxy.modelOutputPossibleAt(string(promptID), promptEventReceivedAt(event)) {
		return false
	}
	slog.Info(
		"ACP provider CLI startup diagnostic withheld from the agent message stream",
		"runtimeSession", state.id, "promptID", promptID, "sequence", event.Sequence, "diagnostic", summary,
	)
	return true
}

// promptEventReceivedAt is the instant the session received event from the
// child; the enqueue timestamp stands in for events that carry no receipt
// time.
func promptEventReceivedAt(event acp.PromptEvent) time.Time {
	if !event.ReceivedAt.IsZero() {
		return event.ReceivedAt
	}
	return event.Timestamp
}
