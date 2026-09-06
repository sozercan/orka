package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExecutionEventTypeConstantsCoverTaxonomy(t *testing.T) {
	want := []string{
		ExecutionEventTypeTaskCreated,
		ExecutionEventTypeTaskPhaseChanged,
		ExecutionEventTypeTaskJobCreated,
		ExecutionEventTypeTaskStarted,
		ExecutionEventTypeTaskSucceeded,
		ExecutionEventTypeTaskFailed,
		ExecutionEventTypeTaskCancelled,
		ExecutionEventTypeWorkerStarted,
		ExecutionEventTypeWorkerCompleted,
		ExecutionEventTypeWorkerFailed,
		ExecutionEventTypeModelRequestStarted,
		ExecutionEventTypeModelRequestCompleted,
		ExecutionEventTypeModelRequestFailed,
		ExecutionEventTypeModelUsageUpdated,
		ExecutionEventTypeModelContextUpdated,
		ExecutionEventTypeModelMessage,
		ExecutionEventTypeContextTruncated,
		ExecutionEventTypeToolCallStarted,
		ExecutionEventTypeToolCallCompleted,
		ExecutionEventTypeToolCallFailed,
		ExecutionEventTypeWorkspacePreparationStarted,
		ExecutionEventTypeWorkspacePreparationCompleted,
		ExecutionEventTypeWorkspacePreparationFailed,
		ExecutionEventTypeAgentRuntimeStarted,
		ExecutionEventTypeAgentRuntimeCommandStarted,
		ExecutionEventTypeAgentRuntimeCompleted,
		ExecutionEventTypeAgentRuntimeFailed,
		ExecutionEventTypeAgentRuntimeCancelled,
		ExecutionEventTypeResultSubmitted,
		ExecutionEventTypeGatewayDeliveryCompleted,
		ExecutionEventTypeArtifactUploadCompleted,
		ExecutionEventTypeArtifactUploadFailed,
		ExecutionEventTypeTaskForkRequested,
		ExecutionEventTypeTaskForkCreated,
		ExecutionEventTypeApprovalRequested,
		ExecutionEventTypeApprovalApproved,
		ExecutionEventTypeApprovalDeclined,
		ExecutionEventTypeApprovalExpired,
		ExecutionEventTypeApprovalCancelled,
		ExecutionEventTypePlanUpdated,
	}
	got := executionEventTypes
	if len(got) != len(want) {
		t.Fatalf("executionEventTypes length = %d, want %d", len(got), len(want))
	}
	seen := map[string]bool{}
	for _, typ := range got {
		if seen[typ] {
			t.Fatalf("executionEventTypes contains duplicate %q", typ)
		}
		seen[typ] = true
		if !IsValidExecutionEventType(typ) {
			t.Fatalf("IsValidExecutionEventType(%q) = false", typ)
		}
	}
	for _, typ := range want {
		if !seen[typ] {
			t.Fatalf("executionEventTypes missing %q", typ)
		}
	}
}

func TestExecutionEventTypeValidationRejectsEmptyAndUnknown(t *testing.T) {
	for _, value := range []string{"", " ", "TaskUnknown", "taskCreated"} {
		if IsValidExecutionEventType(value) {
			t.Fatalf("IsValidExecutionEventType(%q) = true, want false", value)
		}
		if got := NormalizeExecutionEventType(value); got != "" {
			t.Fatalf("NormalizeExecutionEventType(%q) = %q, want empty", value, got)
		}
	}
	if got := NormalizeExecutionEventType("  " + ExecutionEventTypeTaskCreated + " "); got != ExecutionEventTypeTaskCreated {
		t.Fatalf("NormalizeExecutionEventType() = %q", got)
	}
}

func TestTerminalTaskEventTaxonomy(t *testing.T) {
	want := []string{
		ExecutionEventTypeTaskSucceeded,
		ExecutionEventTypeTaskFailed,
		ExecutionEventTypeTaskCancelled,
	}
	got := TerminalTaskEventTypes()
	assertSameEventSet(t, got, want)
	for _, typ := range got {
		if !IsValidExecutionEventType(typ) {
			t.Fatalf("terminal task event %q is not a valid execution event type", typ)
		}
		if !IsTerminalTaskEventType("  " + typ + " ") {
			t.Fatalf("IsTerminalTaskEventType(%q) = false, want true", typ)
		}
	}
	for _, typ := range []string{ExecutionEventTypeTaskStarted, ExecutionEventTypeApprovalApproved, ExecutionEventTypeAgentRuntimeCompleted, ""} {
		if IsTerminalTaskEventType(typ) {
			t.Fatalf("IsTerminalTaskEventType(%q) = true, want false", typ)
		}
	}
	got[0] = ExecutionEventTypeTaskStarted
	if TerminalTaskEventTypes()[0] != ExecutionEventTypeTaskSucceeded {
		t.Fatalf("TerminalTaskEventTypes returned mutable package storage")
	}
}

func TestTerminalApprovalEventTaxonomy(t *testing.T) {
	want := []string{
		ExecutionEventTypeApprovalApproved,
		ExecutionEventTypeApprovalDeclined,
		ExecutionEventTypeApprovalExpired,
		ExecutionEventTypeApprovalCancelled,
	}
	got := terminalApprovalEventTypes
	assertSameEventSet(t, got, want)
	for _, typ := range got {
		if !IsValidExecutionEventType(typ) {
			t.Fatalf("terminal approval event %q is not a valid execution event type", typ)
		}
		if !IsTerminalApprovalEventType("  " + typ + " ") {
			t.Fatalf("IsTerminalApprovalEventType(%q) = false, want true", typ)
		}
	}
	for _, typ := range []string{ExecutionEventTypeApprovalRequested, ExecutionEventTypeTaskSucceeded, ExecutionEventTypeAgentRuntimeCompleted, ""} {
		if IsTerminalApprovalEventType(typ) {
			t.Fatalf("IsTerminalApprovalEventType(%q) = true, want false", typ)
		}
	}
}

func assertSameEventSet(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event set length = %d, want %d (got=%v want=%v)", len(got), len(want), got, want)
	}
	seen := map[string]bool{}
	for _, typ := range got {
		if seen[typ] {
			t.Fatalf("event set contains duplicate %q", typ)
		}
		seen[typ] = true
	}
	for _, typ := range want {
		if !seen[typ] {
			t.Fatalf("event set missing %q (got=%v)", typ, got)
		}
	}
}

func TestEventSeverityNormalization(t *testing.T) {
	tests := map[string]string{
		"":         ExecutionEventSeverityInfo,
		" ":        ExecutionEventSeverityInfo,
		"INFO":     ExecutionEventSeverityInfo,
		"warning":  ExecutionEventSeverityWarning,
		"Error":    ExecutionEventSeverityError,
		"debug":    ExecutionEventSeverityDebug,
		"critical": ExecutionEventSeverityInfo,
	}
	for input, want := range tests {
		if got := NormalizeExecutionEventSeverity(input); got != want {
			t.Fatalf("NormalizeExecutionEventSeverity(%q) = %q, want %q", input, got, want)
		}
	}
	for _, valid := range []string{ExecutionEventSeverityDebug, ExecutionEventSeverityInfo, ExecutionEventSeverityWarning, ExecutionEventSeverityError} {
		if !IsValidExecutionEventSeverity(valid) {
			t.Fatalf("IsValidExecutionEventSeverity(%q) = false", valid)
		}
	}
	if IsValidExecutionEventSeverity("critical") {
		t.Fatalf("IsValidExecutionEventSeverity(critical) = true, want false")
	}
}

func TestRedactExecutionEventTextCoversSecrets(t *testing.T) {
	bearerValue := fakeDashToken("bearer")
	txnValue := fakeDashToken("txn")
	cookieValue := fakeDashToken("cookie")
	openAIKey := fakeOpenAIKey()
	githubKey := fakeGitHubKey()
	anthropicKey := fakeAnthropicKey()
	jwt := fakeJWT()
	githubAuthValue := fakeDashToken("github-auth")
	apiKeyAuthValue := fakeDashToken("api-key-auth")
	input := strings.Join([]string{
		"Authorization: Bearer " + bearerValue,
		"Authorization: token " + githubAuthValue,
		"Authorization: ApiKey " + apiKeyAuthValue,
		"Transaction-Token: " + txnValue,
		"Co" + "okie: sessionid=" + cookieValue + "; theme=dark",
		`api` + `_k` + `ey="` + openAIKey + `"`,
		githubKey,
		anthropicKey,
		jwt,
	}, "\n")
	got := RedactExecutionEventText(input)
	for _, leaked := range []string{
		bearerValue,
		githubAuthValue,
		apiKeyAuthValue,
		txnValue,
		cookieValue,
		openAIKey,
		githubKey,
		anthropicKey,
		jwt[:8],
	} {
		if strings.Contains(got, leaked) {
			t.Fatalf("RedactExecutionEventText leaked %q in %q", leaked, got)
		}
	}
	if !strings.Contains(got, ExecutionEventRedactedValue) {
		t.Fatalf("RedactExecutionEventText() = %q, want redaction marker", got)
	}
}

func TestRedactExecutionEventTextStripsURLQueriesAndFragments(t *testing.T) {
	capabilityURL := "https://account.blob.core.windows.net/output.txt?sp=r&sig=usable-secret#download"
	schemeRelativeURL := "//account.blob.core.windows.net/output.txt?sp=r&sig=scheme-relative-secret#download"
	rootRelativeURL := "/api/artifacts/output?sig=root-relative-secret#download"
	pathRelativeURL := "artifacts/output?sig=path-relative-secret#download"
	singleRelativeURL := "output?sig=single-relative-secret#download"
	singleFragmentURL := "callback#single-fragment-secret"
	queryRelativeURL := "?sig=query-relative-secret#refresh"
	malformedURL := "https://example.test/%zz?sig=malformed-secret"
	userinfoURL := "//alice:scheme-relative-password@example.com/output.txt"
	got := RedactExecutionEventText("download " + capabilityURL + ", mirror " + schemeRelativeURL +
		"; root " + rootRelativeURL + "; path " + pathRelativeURL + "; file " + singleRelativeURL +
		"; callback " + singleFragmentURL + "; refresh " + queryRelativeURL +
		"; malformed " + malformedURL +
		"; authenticated mirror " + userinfoURL + "; then open https://example.com/docs and /docs/getting-started.")
	for _, leaked := range []string{
		"sig=", "usable-secret", "scheme-relative-secret", "root-relative-secret", "path-relative-secret",
		"single-relative-secret", "single-fragment-secret", "query-relative-secret",
		"malformed-secret", "%zz", "#download", "#refresh", "alice", "scheme-relative-password",
	} {
		if strings.Contains(got, leaked) {
			t.Fatalf("RedactExecutionEventText leaked %q in %q", leaked, got)
		}
	}
	if !strings.Contains(got, "https://account.blob.core.windows.net/output.txt,") ||
		!strings.Contains(got, "//account.blob.core.windows.net/output.txt;") ||
		!strings.Contains(got, "/api/artifacts/output;") ||
		!strings.Contains(got, "artifacts/output;") ||
		!strings.Contains(got, "output;") ||
		!strings.Contains(got, "callback;") ||
		!strings.Contains(got, "malformed "+ExecutionEventRedactedValue+";") ||
		!strings.Contains(got, "//example.com/output.txt;") ||
		!strings.Contains(got, "https://example.com/docs") ||
		!strings.Contains(got, "/docs/getting-started.") {
		t.Fatalf("RedactExecutionEventText() = %q", got)
	}
}

func TestRedactExecutionEventJSONPayload(t *testing.T) {
	bearerValue := fakeDashToken("bearer")
	cookieValue := fakeDashToken("cookie")
	openAIKey := fakeOpenAIKey()
	txnValue := fakeDashToken("txn")
	jwt := fakeJWT()
	contentBytes, err := json.Marshal(map[string]any{
		"headers": map[string]any{
			"Authorization": "Bearer " + bearerValue,
			"Cookie":        "sessionid=" + cookieValue,
		},
		"apiKey":  openAIKey,
		"message": "token " + jwt,
		"nested":  []any{map[string]any{"transaction_token": txnValue}},
		"benign":  "preserve me",
	})
	if err != nil {
		t.Fatalf("marshal test content: %v", err)
	}
	payload, err := SanitizeExecutionEventPayload("", json.RawMessage(contentBytes), "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	encoded := string(payload.Content)
	for _, leaked := range []string{bearerValue, cookieValue, openAIKey, jwt[:8], txnValue} {
		if strings.Contains(encoded, leaked) {
			t.Fatalf("SanitizeExecutionEventPayload leaked %q in %s", leaked, encoded)
		}
	}
	if !strings.Contains(encoded, "preserve me") {
		t.Fatalf("SanitizeExecutionEventPayload() = %s, want benign value preserved", encoded)
	}
	if !json.Valid(payload.Content) {
		t.Fatalf("sanitized content is not valid JSON: %s", payload.Content)
	}
}

func TestRedactExecutionEventJSONPreservesTokenUsageFields(t *testing.T) {
	content := json.RawMessage(`{"promptTokens":12,"completion_tokens":5,"cachedInputTokens":3,"totalTokenCount":17}`)
	payload, err := SanitizeExecutionEventPayload("", content, "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	var got map[string]int
	if err := json.Unmarshal(payload.Content, &got); err != nil {
		t.Fatalf("unmarshal sanitized content: %v", err)
	}
	if got["promptTokens"] != 12 || got["completion_tokens"] != 5 || got["cachedInputTokens"] != 3 || got["totalTokenCount"] != 17 {
		t.Fatalf("token usage fields = %#v, want numeric values preserved", got)
	}
}

func TestRedactExecutionEventJSONRedactsNonNumericTokenUsageFields(t *testing.T) {
	content := json.RawMessage(`{
		"inputTokens":"opaque-value",
		"outputTokens":true,
		"cachedInputTokens":{"value":3},
		"totalTokenCount":17
	}`)
	payload, err := SanitizeExecutionEventPayload("", content, "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload.Content, &got); err != nil {
		t.Fatalf("unmarshal sanitized content: %v", err)
	}
	for _, key := range []string{"inputTokens", "outputTokens", "cachedInputTokens"} {
		if got[key] != ExecutionEventRedactedValue {
			t.Fatalf("%s = %#v, want redacted", key, got[key])
		}
	}
	if got["totalTokenCount"] != float64(17) {
		t.Fatalf("numeric totalTokenCount = %#v, want 17", got["totalTokenCount"])
	}
}

func TestTruncateExecutionEventJSONPayloadBeforeDecode(t *testing.T) {
	content := json.RawMessage(`{"data":"` + strings.Repeat("x", MaxExecutionEventContentJSONBytes+100))
	payload, err := SanitizeExecutionEventPayload("", content, "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	if payload.Truncation == nil || !payload.Truncation.ContentJSONTruncated {
		t.Fatalf("Truncation metadata = %#v, want content JSON truncated", payload.Truncation)
	}
	if payload.Truncation.ContentJSONOriginalBytes != 0 {
		t.Fatalf("ContentJSONOriginalBytes = %d, want omitted for raw oversized input", payload.Truncation.ContentJSONOriginalBytes)
	}
	var contentBody map[string]any
	if err := json.Unmarshal(payload.Content, &contentBody); err != nil {
		t.Fatalf("unmarshal truncated content: %v", err)
	}
	if _, ok := contentBody["originalBytes"]; ok {
		t.Fatalf("truncated raw oversized content exposes originalBytes: %s", payload.Content)
	}
}

func TestTruncateExecutionEventJSONPayloadOmitsRawSecretPreview(t *testing.T) {
	apiValue := fakeOpenAIKey()
	content := json.RawMessage(`{"apiKey":"` + apiValue + `","data":"` +
		strings.Repeat("x", MaxExecutionEventContentJSONBytes+100))
	payload, err := SanitizeExecutionEventPayload("", content, "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	if strings.Contains(string(payload.Content), apiValue) {
		t.Fatalf("oversized content leaked key-based secret in preview: %s", payload.Content)
	}
}

func TestTruncateExecutionEventPayloadFields(t *testing.T) {
	payload, err := SanitizeExecutionEventPayload(
		strings.Repeat("s", MaxExecutionEventSummaryChars+10),
		nil,
		strings.Repeat("c", MaxExecutionEventContentTextChars+10),
	)
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	if payload.Truncation == nil || !payload.Truncation.SummaryTruncated || !payload.Truncation.ContentTextTruncated {
		t.Fatalf("Truncation metadata = %#v, want summary and contentText truncated", payload.Truncation)
	}
	if got := len([]rune(payload.Summary)); got > MaxExecutionEventSummaryChars {
		t.Fatalf("summary chars = %d, want <= %d", got, MaxExecutionEventSummaryChars)
	}
	if got := len([]rune(payload.ContentText)); got > MaxExecutionEventContentTextChars {
		t.Fatalf("contentText chars = %d, want <= %d", got, MaxExecutionEventContentTextChars)
	}
}

func TestTruncateExecutionEventJSONPayload(t *testing.T) {
	content := json.RawMessage(`{"data":"` + strings.Repeat("x", MaxExecutionEventContentJSONBytes+100) + `"}`)
	payload, err := SanitizeExecutionEventPayload("", content, "")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	if payload.Truncation == nil || !payload.Truncation.ContentJSONTruncated {
		t.Fatalf("Truncation metadata = %#v, want content JSON truncated", payload.Truncation)
	}
	var truncated map[string]any
	if err := json.Unmarshal(payload.Content, &truncated); err != nil {
		t.Fatalf("truncated content unmarshal: %v", err)
	}
	if truncated["truncated"] != true {
		t.Fatalf("truncated content = %#v, want truncated marker", truncated)
	}
}

func TestRedactExecutionEventPayloadPreservesShortBenignContent(t *testing.T) {
	content := json.RawMessage(`{"message":"hello","count":2}`)
	payload, err := SanitizeExecutionEventPayload("short summary", content, "short text")
	if err != nil {
		t.Fatalf("SanitizeExecutionEventPayload() error = %v", err)
	}
	if payload.Truncation != nil {
		t.Fatalf("Truncation = %#v, want nil", payload.Truncation)
	}
	if payload.Summary != "short summary" || payload.ContentText != "short text" {
		t.Fatalf("payload = %#v, want short fields preserved", payload)
	}
	if string(payload.Content) != `{"count":2,"message":"hello"}` && string(payload.Content) != `{"message":"hello","count":2}` {
		t.Fatalf("content = %s, want benign JSON preserved", payload.Content)
	}
}

func fakeDashToken(prefix string) string {
	return strings.Join([]string{prefix, "value", "for", "redaction"}, "-")
}

func fakeOpenAIKey() string {
	return strings.Join([]string{"sk", "test12345678901234567890"}, "-")
}

func fakeAnthropicKey() string {
	return strings.Join([]string{"sk", "ant", "api03", "1234567890abcdefghijklmnopqrstuvwxyz"}, "-")
}

func fakeGitHubKey() string {
	return "github" + "_pat_" + "1234567890abcdefghijklmnopqrstuvwxyz"
}

func fakeJWT() string {
	return strings.Join([]string{
		"ey" + "JhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9",
		"ey" + "JzdWIiOiJ0YXNrIiwiYXVkIjoib3JrYSJ9",
		"signaturevalue1234567890",
	}, ".")
}
