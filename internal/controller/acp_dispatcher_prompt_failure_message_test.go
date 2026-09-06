package controller

import (
	"errors"
	"strings"

	"testing"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/redact"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const (
	promptFailureTestGenericMessage = "prompt failed"
	promptFailureTestGenericCode    = "acp_prompt_failed"
	promptFailureTestUpstreamCode   = "provider_upstream_error"
)

func TestACPPromptFailureMessageProjectsRuntimeDetail(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		terminal harnessv2.Event
		want     string
	}{
		{name: "no failed payload", terminal: harnessv2.Event{Type: harnessv2.EventFailed}, want: promptFailureTestGenericMessage},
		{
			name:     "generic code only",
			terminal: harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{Code: promptFailureTestGenericCode, Message: "ACP prompt failed"}},
			want:     "prompt failed: ACP prompt failed",
		},
		{
			name:     "provider upstream error keeps code and detail",
			terminal: harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{Code: promptFailureTestUpstreamCode, Message: "provider upstream returned HTTP 402 for the final inference request: quota exceeded"}},
			want:     "prompt failed: provider_upstream_error: provider upstream returned HTTP 402 for the final inference request: quota exceeded",
		},
		{
			name:     "code without message",
			terminal: harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{Code: "turn_limit"}},
			want:     "prompt failed: turn_limit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := acpPromptFailureMessage(tc.terminal); got != tc.want {
				t.Fatalf("acpPromptFailureMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestACPPromptFailureMessageIsBounded(t *testing.T) {
	t.Parallel()
	terminal := harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{
		Code: promptFailureTestGenericCode, Message: strings.Repeat("x", 4*acpPromptFailureMessageLimit),
	}}
	if got := acpPromptFailureMessage(terminal); len(got) != acpPromptFailureMessageLimit {
		t.Fatalf("len(acpPromptFailureMessage()) = %d, want %d", len(got), acpPromptFailureMessageLimit)
	}
}

func TestACPWorkspaceValidationFailureMessageProjectsSupervisorReason(t *testing.T) {
	t.Parallel()
	const generic = "workspace validation failed before a trusted delta was established"
	if got := acpWorkspaceValidationFailureMessage(errors.New("dial tcp: connection refused")); got != generic {
		t.Fatalf("non-client error message = %q, want %q", got, generic)
	}
	clientErr := &harnessv2.ClientError{StatusCode: 409, Code: harnessv2.ErrorCodeSessionPoisoned, Message: "workspace validation failed: validate: reserved workspace path"}
	got := acpWorkspaceValidationFailureMessage(clientErr)
	if !strings.HasPrefix(got, generic+": ") || !strings.Contains(got, "reserved workspace path") {
		t.Fatalf("client error message = %q", got)
	}
	if got := acpWorkspaceValidationFailureMessage(&harnessv2.ClientError{StatusCode: 409, Message: "   "}); got != generic {
		t.Fatalf("blank client message = %q, want %q", got, generic)
	}
	long := &harnessv2.ClientError{StatusCode: 409, Code: harnessv2.ErrorCodeSessionPoisoned, Message: "workspace validation failed: " + strings.Repeat("x", 2*acpPromptFailureMessageLimit)}
	if got := acpWorkspaceValidationFailureMessage(long); len(got) > acpPromptFailureMessageLimit || !strings.HasPrefix(got, generic+": ") {
		t.Fatalf("len(long client message) = %d (limit %d), prefix ok=%v", len(got), acpPromptFailureMessageLimit, strings.HasPrefix(got, generic+": "))
	}
}

func TestACPPromptFailureMessageTruncatesOnRuneBoundary(t *testing.T) {
	t.Parallel()
	// Three-byte runes guarantee the byte limit falls inside a rune for at
	// least one of the two helpers' prefixes.
	multibyte := strings.Repeat("界", acpPromptFailureMessageLimit)
	got := acpPromptFailureMessage(harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{
		Code: promptFailureTestGenericCode, Message: multibyte,
	}})
	if !utf8.ValidString(got) || len(got) > acpPromptFailureMessageLimit || len(got) < acpPromptFailureMessageLimit-utf8.UTFMax {
		t.Fatalf("acpPromptFailureMessage() = %d bytes valid=%v", len(got), utf8.ValidString(got))
	}
	clientErr := &harnessv2.ClientError{StatusCode: 409, Code: harnessv2.ErrorCodeSessionPoisoned, Message: multibyte}
	got = acpWorkspaceValidationFailureMessage(clientErr)
	const generic = "workspace validation failed before a trusted delta was established: "
	// The complete projected message (prefix included) is bounded, on a rune
	// boundary, exactly like acpPromptFailureMessage.
	if !strings.HasPrefix(got, generic) || !utf8.ValidString(got) || len(got) > acpPromptFailureMessageLimit || len(got) < acpPromptFailureMessageLimit-utf8.UTFMax {
		t.Fatalf("acpWorkspaceValidationFailureMessage() = %q (%d bytes)", got, len(got))
	}
}

func TestACPStatusMessagesRedactCredentialShapedDetail(t *testing.T) {
	t.Parallel()
	const (
		leakedKey = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
		leakedJWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvcmthLXRlc3QifQ.c2lnbmF0dXJlLXRoYXQtbXVzdC1ub3QtbGVhaw"
	)
	detail := "upstream rejected api_key=" + leakedKey + " and Authorization: Bearer " + leakedJWT + " for the model"
	assertRedacted := func(label, got string) {
		t.Helper()
		if strings.Contains(got, leakedKey) || strings.Contains(got, leakedJWT) {
			t.Fatalf("%s leaked a credential: %q", label, got)
		}
		if !strings.Contains(got, "upstream rejected") || !strings.Contains(got, "[REDACTED]") {
			t.Fatalf("%s lost its surrounding prose: %q", label, got)
		}
	}
	assertRedacted("acpPromptFailureMessage", acpPromptFailureMessage(harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{
		Code: promptFailureTestUpstreamCode, Message: detail,
	}}))
	assertRedacted("acpWorkspaceValidationFailureMessage", acpWorkspaceValidationFailureMessage(&harnessv2.ClientError{
		StatusCode: 409, Code: harnessv2.ErrorCodeSessionPoisoned, Message: detail,
	}))
}

func TestACPPromptFailureMessageRedactsCredentialsSplitByControlRunes(t *testing.T) {
	t.Parallel()
	const key = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	// C0, DEL, and C1 controls inside the token must be dropped (not
	// replaced) so the token reassembles and is redacted as a whole.
	split := "upstream rejected api_key=" + key[:10] + "\x00" + key[10:20] + "\x7f" + key[20:30] + "\u0085" + key[30:] + " for the model"
	got := acpPromptFailureMessage(harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{Code: promptFailureTestUpstreamCode, Message: split}})
	for _, fragment := range []string{key[:10], key[10:20], key[20:30], key[30:]} {
		if strings.Contains(got, fragment) {
			t.Fatalf("acpPromptFailureMessage() leaked credential fragment %q: %q", fragment, got)
		}
	}
	if !strings.Contains(got, "upstream rejected") || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("acpPromptFailureMessage() = %q, want redacted prose", got)
	}
}

func TestStripACPControlRunesReassemblesLineWrappedCredential(t *testing.T) {
	t.Parallel()
	const key = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	split := "upstream rejected " + key[:11] + "\n" + key[11:] + " for the model"
	got := redact.SensitiveText(stripACPControlRunes(split))
	if strings.Contains(got, key[:11]) || strings.Contains(got, key[11:]) || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("sanitized detail = %q, want line-wrapped credential redacted", got)
	}
}

func TestACPPromptFailureMessageRedactsCredentialSplitAcrossCodeAndMessage(t *testing.T) {
	t.Parallel()
	// Neither field is credential-shaped on its own; the composed
	// "password: <value>" is, and must be redacted as one logical value.
	got := acpPromptFailureMessage(harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{
		Code: "password", Message: "correcthorsebatterystaple",
	}})
	if strings.Contains(got, "correcthorsebatterystaple") {
		t.Fatalf("acpPromptFailureMessage() persisted a credential split across code and message: %q", got)
	}
	if !strings.HasPrefix(got, "prompt failed: password: [REDACTED]") {
		t.Fatalf("acpPromptFailureMessage() = %q, want the composed value redacted", got)
	}
}

func TestACPPromptFailureMessageRedactsCredentialShapedCode(t *testing.T) {
	t.Parallel()
	terminal := harnessv2.Event{Type: harnessv2.EventFailed, Failed: &harnessv2.FailedEvent{
		Code: "api_key=sk-live-0123456789abcdefghij", Message: "upstream rejected the request",
	}}
	got := acpPromptFailureMessage(terminal)
	if strings.Contains(got, "sk-live-0123456789abcdefghij") {
		t.Fatalf("acpPromptFailureMessage() leaked a credential-shaped code: %q", got)
	}
	if !strings.Contains(got, "upstream rejected the request") {
		t.Fatalf("acpPromptFailureMessage() dropped the redacted detail: %q", got)
	}
}

func TestStripACPControlRunesDropsFormatRunesBeforeRedaction(t *testing.T) {
	t.Parallel()
	// A zero-width space (U+200B, category Cf) inside a credential must not
	// split the token past the redactor in persisted status or logs.
	const secret = "ak-live-0123456789abcdef"
	split := "api_key=" + secret[:10] + "\u200b" + secret[10:]
	got := redact.SensitiveText(stripACPControlRunes(split))
	if strings.Contains(got, secret) || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("sanitized detail = %q, want the reassembled credential redacted", got)
	}
}
