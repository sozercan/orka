package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

const (
	DefaultPolicyConfigMapKey   = "policy"
	PolicyConfigMapAllowedLabel = "orka.ai/security-policy"
	MaxCustomPolicyBytes        = 32 * 1024
)

type PolicySource struct {
	Name   string `json:"name,omitempty"`
	Key    string `json:"key,omitempty"`
	Digest string `json:"digest,omitempty"`
}

type ScannerPolicy struct {
	CustomScanInstructions string
	FalsePositivePolicy    string
	CustomScanSource       PolicySource
	FalsePositiveSource    PolicySource
	Digest                 string
}

func PolicyRefKey(ref *corev1alpha1.PolicyConfigMapKeyRef) string {
	if ref == nil || strings.TrimSpace(ref.Key) == "" {
		return DefaultPolicyConfigMapKey
	}
	return strings.TrimSpace(ref.Key)
}

func ValidateCustomPolicyText(text string) error {
	if len([]byte(text)) > MaxCustomPolicyBytes {
		return fmt.Errorf("policy exceeds %d bytes", MaxCustomPolicyBytes)
	}
	if LooksLikeSecret(text) {
		return fmt.Errorf("policy appears to contain a secret or token")
	}
	return nil
}

var (
	policySensitivePrefixPattern = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9])(?:(?:github` + `_pat_|` + `g` + `hp_|xo` + `xb-|s` + `k-)[A-Za-z0-9_./+=:-]{8,}|(?:A` + `KIA|A` + `SIA)[A-Z0-9]{16})`)
	// The credential keyword may carry a conventional identifier prefix
	// ("OPENAI_API_KEY", "SLACK_BOT_TOKEN"): "\b" alone cannot see past the
	// "_" word character, so the prefix is matched explicitly. Quoted values
	// additionally admit spaces and any symbol ("correct horse battery
	// staple"); the unquoted alternative admits common password symbols
	// (@#$%^&*!?|,;\\`) so a dotenv or YAML literal like p@ssword-… is still
	// measured as one value. Structural characters stay excluded deliberately:
	// quotes delimit, and ()<>[]{} feed the placeholder and
	// call-syntax exemptions — swallowing them would break the code-plumbing
	// negatives (apiKey = strings.TrimSpace(cfg.APIKey)). Placeholder ($VAR, <example>, {{ .Token }}) and
	// code-reference exemptions run on the captured value either way.
	policySensitiveAssignmentPattern = regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9])(?:[A-Za-z0-9]+[_-]){0,3}(?:api[_-]?key|access[_-]?` + `token|refresh[_-]?` + `token|id[_-]?` + `token|auth[_-]?` + `token|to` + `ken|pass` + `word|clien` + `t[_-]?secret|secr` + `et|cred` + `entials?|priv` + `ate[_-]?key)\s*[:=]\s*(?:"([^"\r\n]{16,})"|'([^'\r\n]{16,})'|([A-Za-z0-9_./+=~:@#$%^&*!?|,;{}\\` + "`" + `-]{16,}))`)
	// YAML plain scalars may contain spaces without quoting. Scan the complete
	// line value so a short first word cannot hide a long credential value.
	// Requiring same-line whitespace after ':' excludes Go's ':=' assignments
	// (a value on the following line is reconstructed by
	// yamlMultilineScalarAssignmentsLookLikeSecret); the
	// token-part filter below keeps call expressions and other source syntax
	// out of this YAML-specific fallback.
	policySensitiveYAMLAssignmentPattern = regexp.MustCompile(`(?im)^[\t ]*(?:-\s*)?["']?(?:[A-Za-z0-9]+[_-]){0,3}(?:api[_-]?key|access[_-]?` + `token|refresh[_-]?` + `token|id[_-]?` + `token|auth[_-]?` + `token|to` + `ken|pass` + `word|clien` + `t[_-]?secret|secr` + `et|cred` + `entials?|priv` + `ate[_-]?key)["']?\s*:[ \t]+([^\r\n]+)$`)
	// YAML block scalars put their value on following indented lines. Match
	// the credential-bearing header here; yamlBlockScalarAssignmentsLookLikeSecret
	// reconstructs all content lines before evaluating the value.
	policySensitiveYAMLBlockHeaderPattern = regexp.MustCompile(`(?i)^[\t ]*(?:-\s*)?["']?(?:[A-Za-z0-9]+[_-]){0,3}(?:api[_-]?key|access[_-]?` + `token|refresh[_-]?` + `token|id[_-]?` + `token|auth[_-]?` + `token|to` + `ken|pass` + `word|clien` + `t[_-]?secret|secr` + `et|cred` + `entials?|priv` + `ate[_-]?key)["']?\s*:\s*[|>](?:[+-][1-9]?|[1-9][+-]?)?[ \t]*(?:#[^\r\n]*)?$`)
	// A credential key with no value on its line; the scalar (if any) sits
	// on the following indented lines.
	policySensitiveYAMLEmptyValuePattern = regexp.MustCompile(`(?i)^[\t ]*(?:-\s*)?["']?(?:[A-Za-z0-9]+[_-]){0,3}(?:api[_-]?key|access[_-]?` + `token|refresh[_-]?` + `token|id[_-]?` + `token|auth[_-]?` + `token|to` + `ken|pass` + `word|clien` + `t[_-]?secret|secr` + `et|cred` + `entials?|priv` + `ate[_-]?key)["']?\s*:\s*(?:#[^\r\n]*)?$`)
	policyJWTPattern                     = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_-])ey` + `J[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}([^A-Za-z0-9_-]|$)`)
	// Header-carried credentials are flagged only when a credential-shaped
	// value follows: "Authorization: Bearer $TOKEN" in documentation is not
	// a secret, "Authorization: Bearer eyJ…" or a 16+ character opaque token
	// is. The value alphabet is the RFC 6750 token68 grammar, which includes
	// "~".
	policyBearerHeaderPattern = regexp.MustCompile(`(?i)auth` + `orization\s*:\s*be` + `arer\s+([A-Za-z0-9_./+=~:-]{16,})`)
	// Basic credentials are base64(user:password); the same length floor and
	// placeholder exemptions apply.
	policyBasicHeaderPattern = regexp.MustCompile(`(?i)auth` + `orization\s*:\s*ba` + `sic\s+([A-Za-z0-9+/=_-]{16,})`)
	// Signed-URL query credentials (S3/GCS presigned URLs, SAS tokens) use
	// the same parameter set internal/redact scrubs; a published URL with a
	// live signature grants access just like a bearer token.
	policySignedURLPattern   = regexp.MustCompile(`(?i)[?&](?:sig|sign` + `ature|sas|x-amz-sign` + `ature|x-amz-sec` + `urity-token|x-amz-cred` + `ential|x-goog-sign` + `ature|x-goog-cred` + `ential)=([^&#\s"'<>,;()\[\]]{16,})`)
	policyTxnTokenPattern    = regexp.MustCompile(`(?i)\btxn?-to` + `ken\s*:\s*([A-Za-z0-9_./+=~:-]{16,})`)
	policyCookiePattern      = regexp.MustCompile(`(?i)\b(set-cookie|cookie)\s*:\s*([^\r\n]+)`)
	policyCookieValuePattern = regexp.MustCompile("^[A-Za-z0-9_./+=~:@#$%^&*!?|,'\\\\`-]{16,}$")
)

var (
	// These forms have syntax that unambiguously refers to a variable or
	// formatter rather than a literal value.
	directPlaceholderPattern = regexp.MustCompile(
		`^(?:` +
			`\$[A-Za-z_][A-Za-z0-9_]*` + // $VAR
			`|\$\{[A-Za-z_][A-Za-z0-9_]*\}` + // ${VAR}; fallbacks can contain literals and stay flagged
			`|%\([A-Za-z_][A-Za-z0-9_]*\)[A-Za-z]` + // %(name)s (Python)
			`)$`)
	windowsEnvironmentPlaceholderPattern = regexp.MustCompile(`^%[A-Z_][A-Z0-9_]*%$`)
	templateFieldPlaceholderPattern      = regexp.MustCompile(`^\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	// Brackets alone do not make a value a placeholder. Only established
	// marker words and credential-field examples receive that exemption.
	placeholderMarkerPattern = regexp.MustCompile(`(?i)^(?:redacted|masked|placeholder|example|sample|dummy|changeme|(?:your[-_ ]*)?(?:api[-_ ]?key|access[-_ ]?token|auth[-_ ]?token|id[-_ ]?token|refresh[-_ ]?token|transaction[-_ ]?token|token|password|passwd|secret|credential|private[-_ ]?key)(?:[-_ ]*(?:here|value|placeholder|redacted))?)$`)
)

// secretValuePlaceholder reports whether a credential-position value is an
// obvious placeholder rather than a literal secret. Only complete recognized
// forms are exempt; a value that merely begins with a placeholder character
// stays flagged.
func secretValuePlaceholder(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	if directPlaceholderPattern.MatchString(value) || windowsEnvironmentPlaceholderPattern.MatchString(value) {
		return true
	}
	if strings.HasPrefix(value, "{{") && strings.HasSuffix(value, "}}") {
		inner := strings.TrimSpace(value[2 : len(value)-2])
		return templateFieldPlaceholderPattern.MatchString(inner) || placeholderMarkerPattern.MatchString(inner)
	}
	if len(value) < 2 {
		return false
	}
	switch {
	case value[0] == '{' && value[len(value)-1] == '}',
		value[0] == '<' && value[len(value)-1] == '>',
		value[0] == '[' && value[len(value)-1] == ']',
		value[0] == '%' && value[len(value)-1] == '%':
		return placeholderMarkerPattern.MatchString(strings.TrimSpace(value[1 : len(value)-1]))
	default:
		return false
	}
}

// codeReferencePattern matches a qualified identifier such as
// strings.TrimSpace or cfg.Provider.APIKey: source code that reads a secret
// from configuration, not the secret itself.
var codeReferencePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$`)

// codeReferenceCredentialTail matches the final dotted segment of a code
// reference that names a credential field. Code that reads a secret refers to
// it by name (cfg.Provider.APIKey, settings.auth_token); a literal dotted
// secret ("correct.horse.battery.staple") does not end in the credential
// keyword it is assigned to, so only credential-named references are exempt.
var codeReferenceCredentialTail = regexp.MustCompile(`(?i)^(?:api[_-]?key|access[_-]?` + `token|refresh[_-]?` + `token|id[_-]?` + `token|auth[_-]?` + `token|to` + `ken|pass` + `word|passwd|clien` + `t[_-]?secret|secr` + `et|credentials?|priv` + `ate[_-]?key|key)$`)

// secretValueIsCode reports whether a credential-position value is source
// code rather than a literal: a call such as strings.TrimSpace(cfg.APIKey)
// (the value is immediately followed by "(") or a qualified identifier whose
// final segment names a credential field. Go/TS/Python that assigns apiKey
// from configuration would otherwise make any file that touches credential
// plumbing unpublishable, while arbitrary dotted literals stay flagged.
// callableReferencePattern matches an identifier that can legally precede a
// call and is either qualified (os.Getenv) or an unqualified credential
// reader (readPasswordFromKeychain). Requiring a complete call suffix and a
// reader-shaped unqualified name prevents arbitrary mixed-case or underscored
// credentials from buying the exemption by appending call punctuation.
var callableReferencePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
var unqualifiedCredentialReaderPattern = regexp.MustCompile(`(?i)^(?:fetch|get|load|lookup|read|resolve)[A-Za-z0-9_]*(?:api[_-]?key|token|password|passwd|secret|credential|private[_-]?key)[A-Za-z0-9_]*$`)

func callableReferenceShape(value string) bool {
	if !callableReferencePattern.MatchString(value) {
		return false
	}
	if strings.Contains(value, ".") {
		return true
	}
	return unqualifiedCredentialReaderPattern.MatchString(value)
}

func hasCompleteCallSuffix(text string, start int) bool {
	if start >= len(text) || text[start] != '(' {
		return false
	}
	depth := 0
	var quote byte
	escaped := false
	for i := start; i < len(text); i++ {
		ch := text[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				// The call must be the whole value: anything but trailing
				// whitespace, statement terminators, or a comment (for
				// example `+ "literal"`) reintroduces attacker-controlled text.
				return onlyStatementTrailer(text[i+1:])
			}
		case '\r', '\n':
			return false
		}
	}
	return false
}

// onlyStatementTrailer reports whether rest, up to the end of its line, is
// empty apart from whitespace, statement terminators, closing brackets, or a
// line comment.
func onlyStatementTrailer(rest string) bool {
	if i := strings.IndexAny(rest, "\r\n"); i >= 0 {
		// A following line that opens with an infix operator continues the
		// assignment (`call()\n  + "literal"`), so the call is not the whole
		// value.
		if nextLineContinuesExpression(rest[i:]) {
			return false
		}
		rest = rest[:i]
	}
	for {
		rest = strings.TrimLeft(rest, " \t;,)]}")
		switch {
		case rest == "":
			return true
		case strings.HasPrefix(rest, "//"), strings.HasPrefix(rest, "#"):
			// `//` is floor division in Python and `#` is not universally a
			// comment; accept them as comments only when no string literal
			// (the only way to smuggle a credential) follows.
			return !strings.ContainsAny(rest, "\"'`")
		case strings.HasPrefix(rest, "/*"):
			// A block comment is only a trailer when it closes on this line
			// and nothing but another trailer follows it. An unterminated
			// comment could hide a continuation on a later line, so it is
			// not a trailer (fail closed).
			end := strings.Index(rest[2:], "*/")
			if end < 0 {
				return false
			}
			rest = rest[2+end+2:]
		default:
			return false
		}
	}
}

// nextLineContinuesExpression reports whether the text after a line break
// starts (past whitespace, blank lines, and comment-only lines) with a token
// that continues the previous expression.
func nextLineContinuesExpression(text string) bool {
	for _, line := range splitLines(text)[1:] {
		trimmed := strings.TrimSpace(line)
		// Strip closed block comments that lead the line; an unterminated
		// one may hide a continuation on a later line, so fail closed.
		for strings.HasPrefix(trimmed, "/*") || strings.HasPrefix(trimmed, "*/") {
			if strings.HasPrefix(trimmed, "*/") {
				trimmed = strings.TrimSpace(trimmed[2:])
				continue
			}
			end := strings.Index(trimmed[2:], "*/")
			if end < 0 {
				return true
			}
			trimmed = strings.TrimSpace(trimmed[2+end+2:])
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "#") {
			continue
		}
		// Fail closed: only a line that starts like a new statement ends the
		// expression; any punctuation-led line (`+`, `.`, `[`, `(`, `?`, `*`
		// outside a block comment, …) is treated as a continuation.
		return !startsStatement(trimmed)
	}
	return false
}

// continuationWords are word-form binary operators that can only continue
// an expression. Control-flow keywords (`if`, `while`, `else`, …) are not
// listed: they start ordinary statements far more often than they act as
// postfix modifiers, and flagging them would reject common credential-loading
// code such as `password = read(ctx)\nif password == "" {`.
var continuationWords = map[string]bool{
	"and": true, "or": true, "not": true, "xor": true, "in": true, "is": true, "div": true, "mod": true,
	"as": true, "like": true, "between": true,
}

// startsStatement reports whether a trimmed code line begins the way a new
// statement does (identifier, keyword, closing brace, decorator, digit) rather
// than continuing the previous expression with punctuation.
func startsStatement(trimmed string) bool {
	if trimmed == "" {
		return false
	}
	// Word-form operators and postfix modifiers (Lua/Python/Ruby/SQL) continue
	// the previous expression even though they start with a letter.
	word := strings.ToLower(trimmed)
	if i := strings.IndexFunc(word, func(r rune) bool { return r < 'a' || r > 'z' }); i >= 0 {
		word = word[:i]
	}
	if continuationWords[word] {
		return false
	}
	r := rune(trimmed[0])
	switch {
	case r == '_', r == '$', r == '}', r == '@', r == ';':
		return true
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r >= 0x80:
		// Non-ASCII identifiers start statements; punctuation is ASCII.
		return true
	}
	return false
}

func secretValueIsCode(text string, value string, end int) bool {
	// Code-reference exemptions apply only to source-code assignments ('=').
	// In a credential-keyed YAML/config scalar (':'), a dotted identifier or
	// call-shaped value is still an attacker-controlled literal.
	if assignmentSeparatorBefore(text, end-len(value)) != '=' {
		return false
	}
	if callableReferenceShape(value) && hasCompleteCallSuffix(text, end) {
		return true
	}
	if !codeReferencePattern.MatchString(value) {
		return false
	}
	tail := value[strings.LastIndexByte(value, '.')+1:]
	return codeReferenceCredentialTail.MatchString(tail)
}

// assignmentSeparatorBefore finds the ':' or '=' that introduced the value
// starting at start, skipping spaces and an optional opening quote.
func assignmentSeparatorBefore(text string, start int) byte {
	for i := start - 1; i >= 0; i-- {
		switch text[i] {
		case ' ', '\t', '"', '\'':
			continue
		case ':', '=':
			return text[i]
		default:
			return 0
		}
	}
	return 0
}

func trimYAMLPlainScalarComment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value[0] == '#' {
		return ""
	}
	for i := 1; i < len(value); i++ {
		if value[i] == '#' && (value[i-1] == ' ' || value[i-1] == '\t') {
			value = value[:i]
			break
		}
	}
	return strings.TrimSpace(value)
}

func yamlPlainScalarAssignmentsLookLikeSecret(scan *secretScan) bool {
	for _, match := range scan.findAllSubmatch(policySensitiveYAMLAssignmentPattern, credentialKeywords, secretScanTokenRuns) {
		value := trimYAMLPlainScalarComment(match[1])
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = strings.TrimSpace(value[1 : len(value)-1])
		}
		if len(value) < 16 {
			continue
		}
		// A sufficiently long scalar under an explicit credential key is
		// sensitive regardless of punctuation or whitespace. The only safe
		// exception is a recognized placeholder form.
		if !secretValuePlaceholder(value) {
			return true
		}
	}
	return false
}

// yamlMultilineScalarAssignmentsLookLikeSecret reconstructs credential-keyed
// YAML scalars that span physical lines — a double- or single-quoted scalar
// whose closing quote sits on a later line (optionally with `\` escaped line
// breaks), a plain scalar folded across more-indented continuation lines, or
// a scalar that starts on the line after its key — and evaluates the joined
// value, which the single-line pattern cannot see.
func yamlMultilineScalarAssignmentsLookLikeSecret(scan *secretScan) bool {
	lines := scan.splitLines()
	for _, i := range scan.credentialKeyLines() {
		first, ok := yamlMultilineScalarStart(lines, i)
		if !ok {
			continue
		}
		candidate, quoted, closed, ok := yamlJoinScalarContinuation(lines, i, first)
		if !ok {
			continue
		}
		if !closed {
			// Fail closed: a quoted scalar still open at the cap cannot be
			// judged, so treat it as secret-like rather than unscanned.
			return true
		}
		// An unquoted multi-line reconstruction of an open source expression
		// (`password: normalize(\n  input,\n)`) is code, not a YAML scalar.
		// Quoted scalars are data whatever punctuation they carry.
		if !quoted && looksLikeOpenSourceExpression(candidate) {
			continue
		}
		if len(candidate) >= 16 && !secretValuePlaceholder(candidate) {
			return true
		}
	}
	return false
}

// looksLikeOpenSourceExpression reports whether an unquoted reconstructed
// value carries unbalanced brackets or a trailing operator, which a YAML
// plain scalar never does but a multi-line call or object literal does.
func looksLikeOpenSourceExpression(candidate string) bool {
	depth := 0
	for _, r := range candidate {
		switch r {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		}
	}
	if depth != 0 {
		return true
	}
	trimmed := strings.TrimSpace(candidate)
	return strings.HasSuffix(trimmed, ",") || strings.HasSuffix(trimmed, "+") || strings.HasSuffix(trimmed, "(")
}

// yamlMultilineScalarStart reports whether lines[i] is a credential-keyed
// line whose scalar continues on later lines, returning the first fragment
// (empty when the scalar starts on the next line). Block scalars are handled
// by yamlBlockScalarAssignmentsLookLikeSecret, quoted scalars closed on the
// same line by the single-line check, and nested collections are not scalars.
func yamlMultilineScalarStart(lines []string, i int) (string, bool) {
	line := lines[i]
	first := ""
	if match := policySensitiveYAMLAssignmentPattern.FindStringSubmatch(line); match != nil {
		first = trimYAMLPlainScalarComment(match[1])
	} else if !policySensitiveYAMLEmptyValuePattern.MatchString(line) {
		return "", false
	}
	if first != "" {
		if first[0] == '|' || first[0] == '>' {
			return "", false
		}
		if (first[0] == '"' || first[0] == '\'') && yamlClosingQuoteIndex(first[1:], first[0]) >= 0 {
			return "", false
		}
		return first, true
	}
	for _, next := range lines[i+1:] {
		trimmed := strings.TrimSpace(next)
		if trimmed == "" {
			continue
		}
		// A quoted next line is the scalar itself (a colon inside the quotes
		// is data); a bare `- item`, comment, or `key: value` is a collection.
		if trimmed[0] == '"' || trimmed[0] == '\'' {
			if end := yamlClosingQuoteIndex(trimmed[1:], trimmed[0]); end >= 0 && strings.HasPrefix(strings.TrimSpace(trimmed[end+2:]), ":") {
				return "", false // quoted mapping key
			}
			return "", true
		}
		if strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "#") || strings.Contains(trimmed, ": ") || strings.HasSuffix(trimmed, ":") {
			return "", false
		}
		return "", true
	}
	return "", false
}

// yamlJoinScalarContinuation joins the scalar that starts with first on
// lines[i] across its continuation lines. It returns the reconstructed value,
// whether the scalar was quoted, whether a quoted scalar was closed, and
// whether any continuation existed.
func yamlJoinScalarContinuation(lines []string, i int, first string) (candidate string, quoted, closed, ok bool) {
	const maxContinuationLines = 64
	var quote byte
	if first != "" && (first[0] == '"' || first[0] == '\'') {
		quote = first[0]
	}
	if first == "" {
		// The scalar starts on the next non-blank line; adopt its quoting.
		for _, next := range lines[i+1:] {
			trimmed := strings.TrimSpace(next)
			if trimmed == "" {
				continue
			}
			if trimmed[0] == '"' || trimmed[0] == '\'' {
				quote = trimmed[0]
			}
			break
		}
	}
	baseIndent := len(lines[i]) - len(strings.TrimLeft(lines[i], " \t"))
	var value strings.Builder
	// An escaped double-quoted line break joins fragments directly; every
	// other line break folds to a single space.
	escapedBreak := quote == '"' && strings.HasSuffix(first, "\\")
	value.WriteString(strings.TrimSuffix(first, "\\"))
	joined := 0
	closed = quote == 0
	for _, next := range lines[i+1:] {
		if joined >= maxContinuationLines {
			break
		}
		trimmed := strings.TrimSpace(next)
		if trimmed == "" {
			// Blank lines never end a scalar; only a dedent or the closing
			// quote does.
			continue
		}
		indent := len(next) - len(strings.TrimLeft(next, " \t"))
		if quote == 0 {
			if indent <= baseIndent || strings.Contains(trimmed, ": ") || strings.HasSuffix(trimmed, ":") {
				break
			}
			// Comment-only lines are not scalar content, and an inline
			// comment ends the plain scalar fragment.
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if trimmed = trimYAMLPlainScalarComment(trimmed); trimmed == "" {
				continue
			}
		}
		joined++
		if !escapedBreak && value.Len() > 0 {
			value.WriteByte(' ')
		}
		if quote != 0 && value.Len() == 0 && trimmed[0] == quote {
			trimmed = trimmed[1:] // opening quote of a next-line scalar
		}
		if quote != 0 {
			if end := yamlClosingQuoteIndex(trimmed, quote); end >= 0 {
				value.WriteString(trimmed[:end])
				closed = true
				break
			}
		}
		escapedBreak = quote == '"' && strings.HasSuffix(trimmed, "\\")
		value.WriteString(strings.TrimSuffix(trimmed, "\\"))
	}
	if joined == 0 {
		return "", quote != 0, closed, false
	}
	candidate = value.String()
	if quote != 0 {
		candidate = strings.TrimPrefix(candidate, string(quote))
	}
	return strings.TrimSpace(candidate), quote != 0, closed, true
}

// yamlClosingQuoteIndex returns the index of the first unescaped closing
// quote in line (a doubled single quote or a backslash-escaped double quote
// does not close the scalar), or -1 when the scalar stays open. Anything after
// the closing quote (an inline `# comment`) is ignored by the caller.
func yamlClosingQuoteIndex(line string, quote byte) int {
	for i := 0; i < len(line); i++ {
		switch {
		case quote == '"' && line[i] == '\\':
			i++
		case line[i] == quote:
			if quote == '\'' && i+1 < len(line) && line[i+1] == '\'' {
				i++
				continue
			}
			return i
		}
	}
	return -1
}

func yamlBlockScalarAssignmentsLookLikeSecret(scan *secretScan) bool {
	lines := scan.splitLines()
	for _, i := range scan.credentialKeyLines() {
		line := lines[i]
		if !policySensitiveYAMLBlockHeaderPattern.MatchString(line) {
			continue
		}
		baseIndent := len(line) - len(strings.TrimLeft(line, " \t"))
		var value strings.Builder
		for _, contentLine := range lines[i+1:] {
			if strings.TrimSpace(contentLine) == "" {
				continue
			}
			indent := len(contentLine) - len(strings.TrimLeft(contentLine, " \t"))
			if indent <= baseIndent {
				break
			}
			value.WriteString(strings.TrimLeft(contentLine, " \t"))
		}
		candidate := strings.TrimSpace(value.String())
		if len(candidate) < 16 || secretValuePlaceholder(candidate) {
			continue
		}
		return true
	}
	return false
}

func cookieHeadersLookLikeSecret(scan *secretScan) bool {
	for _, match := range scan.findAllSubmatch(policyCookiePattern, cookieKeywords, secretScanTokenRuns) {
		parts := strings.Split(match[2], ";")
		if strings.EqualFold(match[1], "set-cookie") && len(parts) > 1 {
			parts = parts[:1]
		}
		for _, part := range parts {
			_, value, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			value = strings.TrimSpace(value)
			if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
				value = value[1 : len(value)-1]
			}
			if !secretValuePlaceholder(value) && policyCookieValuePattern.MatchString(value) {
				return true
			}
		}
	}
	return false
}

// LooksLikeSecret reports whether text carries a credential-shaped value:
// known token prefixes, JWTs, PEM blocks, or an assignment/header whose value
// is long enough to be a real secret. Bare keywords such as
// "OPENAI_API_KEY=dummy" or "Authorization: Bearer $TOKEN" are not secrets
// and must not block documentation or code that merely mentions them.
//
// Each pattern is evaluated only around the keywords it requires (see
// secretScan); the verdict is the same as evaluating every pattern over the
// whole text.
func LooksLikeSecret(text string) bool {
	return looksLikeSecret(newSecretScan(stripUnsafeTextRunes(text)))
}

func looksLikeSecret(scan *secretScan) bool {
	if scan.matchAny(policySensitivePrefixPattern, sensitivePrefixKeywords, 0) {
		return true
	}
	if scan.sensitiveValueMatch(policySensitiveAssignmentPattern, credentialKeywords) {
		return true
	}
	if yamlPlainScalarAssignmentsLookLikeSecret(scan) {
		return true
	}
	if scan.matchAny(policyJWTPattern, jwtKeywords, 0) {
		return true
	}
	if scan.sensitiveValueMatch(policyBearerHeaderPattern, authorizationKeywords) ||
		scan.sensitiveValueMatch(policyBasicHeaderPattern, authorizationKeywords) ||
		scan.sensitiveValueMatch(policyTxnTokenPattern, txnTokenKeywords) {
		return true
	}
	if cookieHeadersLookLikeSecret(scan) {
		return true
	}
	if scan.sensitiveValueMatch(policySignedURLPattern, signedURLKeywords) {
		return true
	}
	if yamlBlockScalarAssignmentsLookLikeSecret(scan) {
		return true
	}
	if yamlMultilineScalarAssignmentsLookLikeSecret(scan) {
		return true
	}
	return scan.containsFolded(pemBlockNeedle)
}

func PolicyTextDigest(text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(text))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ScannerPolicyDigest(policy ScannerPolicy) string {
	parts := []string{ScannerPolicyVersion}
	if policy.CustomScanSource.Name != "" || policy.CustomScanInstructions != "" {
		parts = append(parts,
			"custom-scan", policy.CustomScanSource.Name, policy.CustomScanSource.Key,
			PolicyTextDigest(policy.CustomScanInstructions),
		)
	}
	if policy.FalsePositiveSource.Name != "" || policy.FalsePositivePolicy != "" {
		parts = append(parts,
			"false-positive", policy.FalsePositiveSource.Name, policy.FalsePositiveSource.Key,
			PolicyTextDigest(policy.FalsePositivePolicy),
		)
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (p ScannerPolicy) PromptPolicy() PromptPolicy {
	return PromptPolicy{
		CustomScanInstructions: p.CustomScanInstructions,
		FalsePositivePolicy:    p.FalsePositivePolicy,
		PolicyDigest:           p.Digest,
		CustomScanSource:       p.CustomScanSource.String(),
		FalsePositiveSource:    p.FalsePositiveSource.String(),
	}
}

func (s PolicySource) String() string {
	if s.Name == "" {
		return ""
	}
	key := s.Key
	if key == "" {
		key = DefaultPolicyConfigMapKey
	}
	if s.Digest == "" {
		return s.Name + "/" + key
	}
	return s.Name + "/" + key + " (" + s.Digest + ")"
}

func PolicyProvenanceEnv(policy ScannerPolicy) string {
	items := []string{}
	if value := policy.CustomScanSource.String(); value != "" {
		items = append(items, "customScan="+value)
	}
	if value := policy.FalsePositiveSource.String(); value != "" {
		items = append(items, "falsePositive="+value)
	}
	sort.Strings(items)
	return strings.Join(items, ";")
}

func LoadScannerPolicy(ctx context.Context, reader client.Reader, namespace string, spec corev1alpha1.RepositoryScanSpec) (ScannerPolicy, error) {
	policy := ScannerPolicy{}
	if reader == nil {
		policy.Digest = ScannerPolicyDigest(policy)
		return policy, nil
	}
	if spec.CustomScanInstructionsRef != nil {
		text, source, err := loadPolicyConfigMapKey(ctx, reader, namespace, spec.CustomScanInstructionsRef)
		if err != nil {
			return ScannerPolicy{}, fmt.Errorf("customScanInstructionsRef: %w", err)
		}
		policy.CustomScanInstructions = text
		policy.CustomScanSource = source
	}
	if spec.FalsePositivePolicyRef != nil {
		text, source, err := loadPolicyConfigMapKey(ctx, reader, namespace, spec.FalsePositivePolicyRef)
		if err != nil {
			return ScannerPolicy{}, fmt.Errorf("falsePositivePolicyRef: %w", err)
		}
		policy.FalsePositivePolicy = text
		policy.FalsePositiveSource = source
	}
	policy.Digest = ScannerPolicyDigest(policy)
	return policy, nil
}

func policyConfigMapAllowed(cm corev1.ConfigMap) bool {
	return strings.EqualFold(strings.TrimSpace(cm.Labels[PolicyConfigMapAllowedLabel]), "true") ||
		strings.EqualFold(strings.TrimSpace(cm.Annotations[PolicyConfigMapAllowedLabel]), "true")
}

func loadPolicyConfigMapKey(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref *corev1alpha1.PolicyConfigMapKeyRef,
) (string, PolicySource, error) {
	name := strings.TrimSpace(ref.Name)
	if name == "" {
		return "", PolicySource{}, fmt.Errorf("name is required")
	}
	key := PolicyRefKey(ref)
	var cm corev1.ConfigMap
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &cm); err != nil {
		return "", PolicySource{}, err
	}
	if !policyConfigMapAllowed(cm) {
		return "", PolicySource{}, fmt.Errorf("ConfigMap %q must be labeled or annotated %s=true to be used as scanner policy", name, PolicyConfigMapAllowedLabel)
	}
	value, ok := cm.Data[key]
	if !ok {
		return "", PolicySource{}, fmt.Errorf("key %q is missing in ConfigMap %q", key, name)
	}
	if err := ValidateCustomPolicyText(value); err != nil {
		return "", PolicySource{}, err
	}
	source := PolicySource{Name: name, Key: key, Digest: PolicyTextDigest(value)}
	return strings.TrimSpace(value), source, nil
}

func ScanRunIdempotencyKey(namespace, repositoryScan, mode, baseSHA, headSHA, subPath, policyDigest string) string {
	parts := []string{
		strings.TrimSpace(namespace),
		strings.TrimSpace(repositoryScan),
		strings.TrimSpace(mode),
		strings.TrimSpace(baseSHA),
		strings.TrimSpace(headSHA),
		strings.Trim(strings.TrimSpace(subPath), "/"),
		strings.TrimSpace(policyDigest),
		ScannerPolicyVersion,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "scanidem:" + hex.EncodeToString(sum[:])
}

// SecretLikeLineDigests returns one SHA-256 hex digest for every line of text
// that LooksLikeSecret flags on its own. Each digest covers the flagged
// line's code block together with the previous and the next code block and
// everything in between (blank and comment-only lines), so a caller that
// recognises a pre-existing secret-like line (a demo credential a
// repository already ships) also proves nothing was inserted, appended, or
// continued anywhere an expression could still reach it. Digests may repeat
// when identical windows occur more than once; callers compare them as
// multisets. No content is retained.
func SecretLikeLineDigests(text string) []string {
	lines := splitLines(text)
	var windows *lineWindows
	var digests []string
	for i, line := range lines {
		if strings.TrimSpace(line) == "" || !LooksLikeSecret(line) {
			continue
		}
		if windows == nil {
			windows = newLineWindows(lines)
		}
		digests = append(digests, windows.digest(i))
	}
	return digests
}

// StripLinesByDigest removes every secret-like line whose window digest (see
// SecretLikeLineDigests) is in known and returns the remaining text, so the
// caller can re-check that nothing secret-like remains once the recognised
// lines are taken out. Only secret-like lines can carry a known digest, so
// only they are hashed.
func StripLinesByDigest(text string, known map[string]struct{}) string {
	if len(known) == 0 {
		return text
	}
	lines := splitLines(text)
	var windows *lineWindows
	kept := make([]string, 0, len(lines))
	for i, line := range lines {
		if strings.TrimSpace(line) != "" && LooksLikeSecret(line) {
			if windows == nil {
				windows = newLineWindows(lines)
			}
			if _, ok := known[windows.digest(i)]; ok {
				continue
			}
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func splitLines(text string) []string {
	return strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
}

// lineWindows precomputes each line's code-block bounds so window digests
// (previous code block through next code block, gap lines included) cost
// O(window) and are cached per distinct window instead of being recomputed
// per flagged line.
type lineWindows struct {
	lines  []string
	before []int
	after  []int
	cache  map[[2]int]string
}

func newLineWindows(lines []string) *lineWindows {
	w := &lineWindows{lines: lines, before: make([]int, len(lines)), after: make([]int, len(lines)), cache: map[[2]int]string{}}
	// paragraphStart[i]/paragraphEnd[i] bound the code block containing line
	// i. Blank and comment-only lines are gap lines: they never end an
	// expression, so a block made only of them merges into the surrounding
	// gap and the window keeps extending to the next real code block.
	blank := func(i int) bool { return !isCodeLine(lines[i]) }
	start := 0
	for i := range lines {
		if i > 0 && blank(i-1) && !blank(i) {
			start = i
		}
		w.before[i] = start
	}
	end := len(lines) - 1
	for i := len(lines) - 1; i >= 0; i-- {
		if i+1 < len(lines) && blank(i+1) && !blank(i) {
			end = i
		}
		w.after[i] = end
	}
	// Extend to the neighbouring paragraphs: walk back over the gap to the
	// previous block's start, and forward over the gap to the next block's end.
	prevStart := make([]int, len(lines))
	nextEnd := make([]int, len(lines))
	for i := range lines {
		j := w.before[i] - 1
		for j >= 0 && blank(j) {
			j--
		}
		if j >= 0 {
			prevStart[i] = w.before[j]
		} else {
			prevStart[i] = w.before[i]
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		j := w.after[i] + 1
		for j < len(lines) && blank(j) {
			j++
		}
		if j < len(lines) {
			nextEnd[i] = w.after[j]
		} else {
			nextEnd[i] = w.after[i]
		}
	}
	w.before, w.after = prevStart, nextEnd
	return w
}

func isCodeLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	for _, prefix := range []string{"//", "#", "/*", "*"} {
		if strings.HasPrefix(trimmed, prefix) {
			return false
		}
	}
	return true
}

func (w *lineWindows) digest(i int) string {
	bounds := [2]int{w.before[i], w.after[i]}
	if cached, ok := w.cache[bounds]; ok {
		return cached
	}
	digest := sha256.Sum256([]byte(strings.Join(w.lines[bounds[0]:bounds[1]+1], "\n")))
	encoded := hex.EncodeToString(digest[:])
	w.cache[bounds] = encoded
	return encoded
}
