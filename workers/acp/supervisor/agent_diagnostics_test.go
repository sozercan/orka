package supervisor

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/acp"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

// The fixture chunks below are the exact agent_message_chunk texts GitHub
// Copilot CLI 1.0.77 emitted in --acp mode under the supervisor's flags when
// --excluded-tools carried the pre-fix exclusion list (issue #460).
var copilotObservedStartupDiagnostics = []string{
	"Info: Disabled tools: list_agents, read_agent, skill, sql, task, write_agent",
	`Info: Unknown tool name in the tool excludedlist: "ask_user"`,
	`Info: Unknown tool name in the tool excludedlist: "code_review"`,
	`Info: Unknown tool name in the tool excludedlist: "github-mcp-server"`,
	`Info: Unknown tool name in the tool excludedlist: "report_intent"`,
}

// The CLI routes its inference retry notice through the same channel; it is
// deliberately not recognized here (tracked separately in #473).
const copilotObservedRetryNotice = "Info: Response was interrupted due to a server error. Retrying..."

func TestCopilotStartupDiagnosticRecognizesPinnedCLIDiagnostics(t *testing.T) {
	recognize := copilotStartupDiagnostic([]string{
		"ask_user", "code_review", "github-mcp-server", "list_agents", "read_agent", "report_intent",
		"skill", "sql", "task", "write_agent", "bash", "list_bash",
	})
	for _, text := range copilotObservedStartupDiagnostics {
		if _, ok := recognize(text); !ok {
			t.Fatalf("observed CLI diagnostic was not recognized: %q", text)
		}
	}
	withheld := map[string]string{
		// A restricted session's line names policy exclusions and tools the
		// CLI disabled on its own (web_search under a BYOK provider).
		"Info: Disabled tools: bash, list_bash, skill, web_search":     "disabled tools: bash, list_bash, skill (+1 not in the session exclusion list)",
		"Info: Disabled tools: github-mcp-server-search_code, sql":     "disabled tools: sql (+1 not in the session exclusion list)",
		`Info: Unknown tool name in the tool excludedlist: "ask_user"`: `unknown excluded tool "ask_user"`,
	}
	for text, wantSummary := range withheld {
		summary, ok := recognize(text)
		if !ok {
			t.Fatalf("diagnostic naming a session exclusion was not recognized: %q", text)
		}
		if summary != wantSummary {
			t.Fatalf("summary for %q = %q, want %q", text, summary, wantSummary)
		}
	}
	// The chunk is child-controlled: a name-shaped entry such as the session
	// proxy bearer must never reach the summary, only its count.
	const credential = "c2Vzc2lvbi1wcm94eS1iZWFyZXItdG9rZW4tdmFsdWU_-x"
	summary, ok := recognize("Info: Disabled tools: skill, " + credential)
	if !ok || strings.Contains(summary, credential) || summary != "disabled tools: skill (+1 not in the session exclusion list)" {
		t.Fatalf("credential-shaped entry leaked into the summary: ok=%v summary=%q", ok, summary)
	}
	forwarded := []string{
		"PONG",
		"",
		"Info:",
		"Info: Disabled tools: ",
		"Info: Disabled tools: web_search",
		"Info: Disabled tools: skill; PONG",
		"Info: Disabled tools: skill\nPONG",
		"Info: Disabled tools: skill, ",
		`Info: Unknown tool name in the tool excludedlist: "view"`,
		`Info: Unknown tool name in the tool excludedlist: skill`,
		`Info: Unknown tool name in the tool excludedlist: "skill" PONG`,
		copilotObservedRetryNotice,
		"Error: Could not connect to local model provider at http://127.0.0.1:1/v1.",
		"The disabled tools are: skill",
	}
	for _, text := range forwarded {
		if _, ok := recognize(text); ok {
			t.Fatalf("assistant text was recognized as a startup diagnostic: %q", text)
		}
	}
}

func TestWithholdAgentDiagnosticKeepsModelTextIntact(t *testing.T) {
	filter := &AgentDiagnosticFilter{Startup: copilotStartupDiagnostic(copilotAlwaysExcludedToolIDs)}
	compactor := newAssistantMessageCompactor()
	compactor.flushInterval = time.Hour
	t.Cleanup(compactor.close)
	now := time.Now().UTC()
	proxy := &providerProxySession{turnPromptID: "prompt-1"}
	state := &sessionState{id: "session-1", agentDiagnosticFilter: filter, providerProxy: proxy}
	prompt := testDiagnosticPromptState()

	var texts []string
	sequence := int64(0)
	push := func(text string, receivedAt time.Time) {
		sequence++
		event := testAssistantMessagePromptEvent(t, sequence, receivedAt, text)
		if withholdAgentDiagnostic(state, prompt, event) {
			return
		}
		for _, ready := range compactor.push(event, now) {
			ready, _ := assistantMessageText(ready)
			texts = append(texts, ready)
		}
	}
	// The CLI reports exclusions before its first inference request, then
	// the response streams a model answer that repeats the diagnostic
	// sentence verbatim as its very first delta and also echoes the CLI's
	// retry notice, which is not a recognized diagnostic.
	push(copilotObservedStartupDiagnostics[0], now)
	proxy.firstInferenceResponseStartedAt = now.Add(time.Millisecond)
	push(copilotObservedStartupDiagnostics[0], now.Add(2*time.Millisecond))
	push("PO", now.Add(3*time.Millisecond))
	push("NG", now.Add(4*time.Millisecond))
	push(copilotObservedRetryNotice, now.Add(5*time.Millisecond))
	for _, ready := range compactor.flushPending() {
		text, _ := assistantMessageText(ready)
		texts = append(texts, text)
	}
	want := copilotObservedStartupDiagnostics[0] + "PONG" + copilotObservedRetryNotice
	if got := strings.Join(texts, ""); got != want {
		t.Fatalf("assistant text after withholding = %q, want %q", got, want)
	}
}

func TestWithholdAgentDiagnosticAnchorsOnProviderProxyState(t *testing.T) {
	now := time.Now().UTC()
	filter := &AgentDiagnosticFilter{Startup: copilotStartupDiagnostic(copilotAlwaysExcludedToolIDs)}
	startup := testAssistantMessagePromptEvent(t, 1, now, copilotObservedStartupDiagnostics[0])
	retry := testAssistantMessagePromptEvent(t, 2, now, copilotObservedRetryNotice)
	toolCall := acp.PromptEvent{Type: acp.PromptEventUpdate, Sequence: 3, Timestamp: now, Update: &acp.SessionNotification{
		SessionID: "s",
		Update:    []byte(`{"sessionUpdate":"tool_call","toolCallId":"t","title":"` + copilotObservedStartupDiagnostics[0] + `"}`),
	}}

	// Sessions without a projection filter forward every chunk.
	if withholdAgentDiagnostic(&sessionState{}, testDiagnosticPromptState(), startup) {
		t.Fatal("session without a diagnostic filter withheld a chunk")
	}

	// A startup diagnostic is withheld when it was received before the
	// prompt's first non-error inference response began relaying, even if
	// it is consumed only after the proxy moved on; an identical chunk
	// received after that instant is model output. Non-text updates and
	// unrecognized CLI text are never withheld.
	state := &sessionState{id: "session-1", agentDiagnosticFilter: filter, providerProxy: &providerProxySession{turnPromptID: "prompt-1"}}
	if !withholdAgentDiagnostic(state, testDiagnosticPromptState(), startup) {
		t.Fatal("startup diagnostic was forwarded before any inference response")
	}
	if withholdAgentDiagnostic(state, testDiagnosticPromptState(), toolCall) {
		t.Fatal("tool_call update was withheld as a diagnostic")
	}
	if withholdAgentDiagnostic(state, testDiagnosticPromptState(), retry) {
		t.Fatal("inference retry notice was withheld; it is not a recognized diagnostic")
	}
	state.providerProxy = &providerProxySession{turnPromptID: "prompt-1", firstInferenceResponseStartedAt: now.Add(time.Millisecond)}
	if !withholdAgentDiagnostic(state, testDiagnosticPromptState(), startup) {
		t.Fatal("startup diagnostic received before the first inference response was forwarded once consumed late")
	}
	state.providerProxy = &providerProxySession{turnPromptID: "prompt-1", firstInferenceResponseStartedAt: now.Add(-time.Millisecond)}
	if withholdAgentDiagnostic(state, testDiagnosticPromptState(), startup) {
		t.Fatal("startup diagnostic text received after an inference response started was withheld")
	}
	// An event buffered before prompt acceptance is enqueued (and
	// timestamped) later than it was received; the receipt time decides.
	bufferedStartup := startup
	bufferedStartup.ReceivedAt = now.Add(-2 * time.Millisecond)
	if !withholdAgentDiagnostic(state, testDiagnosticPromptState(), bufferedStartup) {
		t.Fatal("startup diagnostic received before the first inference response was forwarded because it was enqueued late")
	}
	state.providerProxy = &providerProxySession{turnPromptID: "other-prompt", firstInferenceResponseStartedAt: now.Add(-time.Millisecond)}
	if !withholdAgentDiagnostic(state, testDiagnosticPromptState(), startup) {
		t.Fatal("another prompt's inference response unblocked a startup diagnostic")
	}
}

func testDiagnosticPromptState() *promptState {
	prompt := &promptState{}
	prompt.request.Metadata.PromptID = "prompt-1"
	return prompt
}

func TestCopilotProjectionDeclaresDiagnosticFilterForSessionExclusions(t *testing.T) {
	paths := acp.SessionPaths{Home: t.TempDir(), Config: t.TempDir()}
	proxy := ProviderProxyBinding{BaseURL: "http://127.0.0.1:1/v1", Credential: strings.Repeat("c", 32)}
	copilot, err := providerProfile(providerKindCopilot, "copilot-test", harnessv2.WorkspaceIntentRead)
	if err != nil {
		t.Fatal(err)
	}
	unrestricted, err := copilot.ProjectSession(testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "", "", nil, nil, true), paths, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if unrestricted.AgentDiagnosticFilter == nil || unrestricted.AgentDiagnosticFilter.Startup == nil {
		t.Fatal("unrestricted Copilot projection declared no diagnostic filter")
	}
	if _, ok := unrestricted.AgentDiagnosticFilter.Startup(copilotObservedStartupDiagnostics[0]); !ok {
		t.Fatalf("unrestricted projection did not recognize %q", copilotObservedStartupDiagnostics[0])
	}
	if _, ok := unrestricted.AgentDiagnosticFilter.Startup("Info: Disabled tools: bash, list_bash"); ok {
		t.Fatal("unrestricted projection recognized a diagnostic about tools it did not exclude")
	}

	restricted, err := copilot.ProjectSession(
		testProviderProjectionRequest(t, providerKindCopilot, "copilot-test", "", "", []string{providerToolRead, providerToolGrep}, nil, false), paths, proxy,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := restricted.AgentDiagnosticFilter.Startup("Info: Disabled tools: bash, list_bash, read_bash, stop_bash, write_bash, skill"); !ok {
		t.Fatal("restricted projection did not recognize the policy exclusion diagnostic")
	}
	if _, ok := restricted.AgentDiagnosticFilter.Startup("Info: Disabled tools: view, grep"); ok {
		t.Fatal("restricted projection recognized a diagnostic about authorized tools")
	}
}

// The pinned Copilot CLI registers exactly these built-in tool names from the
// permanent exclusion set; any other name makes it print an "Unknown tool
// name" diagnostic for every prompt. Re-verify against the CLI in --acp mode
// when the runtime image pin moves.
func TestCopilotAlwaysExcludedToolIDsMatchPinnedCLICatalog(t *testing.T) {
	want := "list_agents,read_agent,skill,sql,task,write_agent"
	if got := strings.Join(copilotAlwaysExcludedToolIDs, ","); got != want {
		t.Fatalf("copilotAlwaysExcludedToolIDs = %s, want %s (verified against Copilot CLI 1.0.77)", got, want)
	}
}

// The supervisor log carries only the recognizer's summary: chunk text is
// child-controlled and could smuggle a session credential.
func TestWithholdAgentDiagnosticLogsOnlyTheSummary(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	const credential = "c2Vzc2lvbi1wcm94eS1iZWFyZXItdG9rZW4tdmFsdWU_-x"
	state := &sessionState{
		id:                    "session-1",
		agentDiagnosticFilter: &AgentDiagnosticFilter{Startup: copilotStartupDiagnostic(copilotAlwaysExcludedToolIDs)},
		providerProxy:         &providerProxySession{turnPromptID: "prompt-1"},
	}
	event := testAssistantMessagePromptEvent(t, 1, time.Now().UTC(), "Info: Disabled tools: skill, "+credential)
	if !withholdAgentDiagnostic(state, testDiagnosticPromptState(), event) {
		t.Fatal("crafted disabled-tools line was not withheld")
	}
	if logged := logs.String(); strings.Contains(logged, credential) || !strings.Contains(logged, "disabled tools: skill (+1 not in the session exclusion list)") {
		t.Fatalf("supervisor log = %q", logged)
	}
}
