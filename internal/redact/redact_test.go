/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package redact

import (
	"strings"
	"testing"
)

func TestSensitiveTextPreservesRedactedMarkdown(t *testing.T) {
	value := strings.Repeat("a", 32)
	for input, want := range map[string]string{
		"Environment assignment `API_KEY=\"" + value + "\"`.": "Environment assignment `API_KEY=\"[REDACTED]\"`.",
		"Environment assignment `API_KEY='" + value + "'`.":   "Environment assignment `API_KEY='[REDACTED]'`.",
		`{"api_key":"` + value + `","line":12}`:               `{"api_key":"[REDACTED]","line":12}`,
	} {
		if got := SensitiveText(input); got != want || SensitiveText(got) != got {
			t.Fatal("redaction changed delimiters outside the credential or was not stable on a second pass")
		}
	}
	input := "API_KEY=" + redactedValue + strings.Repeat("a", 32)
	if got := SensitiveText(input); got != "API_KEY="+redactedValue {
		t.Fatal("a credential using the redaction marker as a prefix was not fully redacted")
	}
}

func TestSensitiveTextRedactsTxnTokenHeader(t *testing.T) {
	input := `curl -H "Txn-Token: opaque-secret-token" https://orka.example.test`
	got := SensitiveText(input)
	if strings.Contains(got, "opaque-secret-token") {
		t.Fatalf("SensitiveText leaked Txn-Token value: %q", got)
	}
	if !strings.Contains(got, "Txn-Token: "+redactedValue) {
		t.Fatalf("SensitiveText() = %q, want redacted Txn-Token header", got)
	}
}

func TestSensitiveTextRedactsJWT(t *testing.T) {
	input := "token eyJhbGciOiJSUzI1NiIsInR5cCI6InR4bnRva2VuK2p3dCJ9.eyJzdWIiOiJ3b3JrbG9hZCIsInR4biI6InR4bi0xMjMifQ.signaturevalue1234567890"
	got := SensitiveText(input)
	if strings.Contains(got, "eyJhbGci") {
		t.Fatalf("SensitiveText leaked JWT: %q", got)
	}
}

func TestSensitiveTextRedactsURLUserInfoWithoutPassword(t *testing.T) {
	input := `repo https://token@example.com/org/repo.git`
	got := SensitiveText(input)
	if strings.Contains(got, "token@example") {
		t.Fatalf("SensitiveText leaked URL userinfo: %q", got)
	}
	if !strings.Contains(got, "https://"+redactedValue+"@example.com") {
		t.Fatalf("SensitiveText() = %q, want redacted URL userinfo", got)
	}
}

func TestSensitiveTextRedactsSignedURLQueries(t *testing.T) {
	cases := map[string]string{
		"https://acct.blob.core.windows.net/c/b?sv=2024-05-04&se=2026-01-01&sig=abc%2Fdef123&sr=b":                     "https://acct.blob.core.windows.net/c/b?sv=2024-05-04&se=2026-01-01&sig=[REDACTED]&sr=b",
		"https://bucket.s3.amazonaws.com/k?X-Amz-Credential=AKIA%2F20260101&X-Amz-Signature=0123abcd&X-Amz-Expires=60": "https://bucket.s3.amazonaws.com/k?X-Amz-Credential=[REDACTED]", // the generic credential= redactor already consumes the remaining query,
		"see https://storage.googleapis.com/b/o?X-Goog-Signature=deadbeef for details":                                 "see https://storage.googleapis.com/b/o?X-Goog-Signature=[REDACTED] for details",
		"plain https://example.com/path?page=2&sort=asc stays":                                                         "plain https://example.com/path?page=2&sort=asc stays",
		"fetch https://h.example/o?sig=abc123#download, then https://h.example/p?signature=xyz; done":                  "fetch https://h.example/o?sig=[REDACTED]#download, then https://h.example/p?signature=[REDACTED]; done",
	}
	for input, want := range cases {
		if got := SensitiveText(input); got != want {
			t.Fatalf("SensitiveText(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSensitiveTextRedactsCompleteCommaBearingValue(t *testing.T) {
	got := SensitiveText("dial failed: password=short,correct-horse-battery-staple rejected")
	if strings.Contains(got, "correct-horse-battery-staple") || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("SensitiveText() = %q, want the complete comma-bearing value redacted", got)
	}
}

func TestSensitiveTextRedactsCookieHeaders(t *testing.T) {
	for _, input := range []string{
		"Cookie: sessionid=correct-horse-battery-staple; theme=dark",
		"Set-Cookie: sessionid=correct-horse-battery-staple; HttpOnly; Secure",
	} {
		got := SensitiveText(input)
		if strings.Contains(got, "correct-horse-battery-staple") || !strings.Contains(got, redactedValue) {
			t.Fatalf("SensitiveText(%q) = %q, want the cookie value redacted", input, got)
		}
	}
}
