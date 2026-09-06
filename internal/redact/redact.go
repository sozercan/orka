package redact

import (
	"regexp"
	"strings"
)

const redactedValue = "[REDACTED]"

var (
	authorizationHeaderRe = regexp.MustCompile(`(?i)\b(authorization\s*:\s*)(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	txnTokenHeaderRe      = regexp.MustCompile(`(?i)\b(txn-token\s*:\s*)[A-Za-z0-9._~+/=-]+`)
	cookieHeaderRe        = regexp.MustCompile(`(?i)\b((?:set-cookie|cookie)\s*:\s*)[^\r\n]+`)
	// Unquoted assignment values consume every non-space character: a
	// redactor must never leave a recoverable credential tail behind a
	// comma or semicolon, and over-redacting a following word in prose is
	// the safe direction.
	sensitiveAssignmentRe   = regexp.MustCompile(`(?i)(["']?)([A-Z0-9_.-]*(?:api[-_]?key|token|secret|password|passwd|pwd|credential|private[-_]?key|client[-_]?secret|access[-_]?token|refresh[-_]?token)[A-Z0-9_.-]*)(["']?)(\s*[:=]\s*)("[^"\r\n]*"|'[^'\r\n]*'|[^\s"']+)`)
	naturalLanguageSecretRe = regexp.MustCompile(`(?i)\b((?:api\s+key|token|secret|password|credential)\s+is\s+)([^\s]+)`) // e.g. "token is abc123"
	wellKnownTokenRe        = regexp.MustCompile(`\b(?:sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{30,}|xox[baprs]-[A-Za-z0-9-]{20,})\b`)
	jwtRe                   = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`)
	urlCredentialRe         = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+@`)
	// Signed/pre-signed URL query credentials whose parameter names do not
	// contain a generic secret keyword: Azure SAS (sig), AWS SigV4
	// (X-Amz-Signature, X-Amz-Security-Token), GCS (X-Goog-Signature), and
	// generic signature/sas parameters.
	signedURLQueryRe = regexp.MustCompile(`(?i)([?&](?:sig|signature|sas|x-amz-signature|x-amz-security-token|x-amz-credential|x-goog-signature|x-goog-credential)=)[^&#\s"'<>,;()\[\]]+`)
)

// SensitiveText replaces common credential and token shapes with a stable
// placeholder suitable for persistence in durable memory and proposals. It is
// intentionally conservative about preserving surrounding prose while removing
// values that look like credentials.
func SensitiveText(s string) string {
	if strings.TrimSpace(s) == "" {
		return s
	}
	s = authorizationHeaderRe.ReplaceAllString(s, `${1}`+redactedValue)
	s = txnTokenHeaderRe.ReplaceAllString(s, `${1}`+redactedValue)
	s = cookieHeaderRe.ReplaceAllString(s, `${1}`+redactedValue)
	s = sensitiveAssignmentRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := sensitiveAssignmentRe.FindStringSubmatch(match)
		replacement := redactedValue
		// Keep quoted values quoted so another pass cannot consume Markdown
		// delimiters or punctuation outside the original value.
		if quote := parts[5][:1]; quote == "\"" || quote == "'" {
			replacement = quote + replacement + quote
		}
		return parts[1] + parts[2] + parts[3] + parts[4] + replacement
	})
	s = naturalLanguageSecretRe.ReplaceAllString(s, `${1}`+redactedValue)
	s = urlCredentialRe.ReplaceAllString(s, `${1}`+redactedValue+`@`)
	s = signedURLQueryRe.ReplaceAllString(s, `${1}`+redactedValue)
	s = wellKnownTokenRe.ReplaceAllString(s, redactedValue)
	s = jwtRe.ReplaceAllString(s, redactedValue)
	return s
}
