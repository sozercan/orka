package security

import (
	"regexp"
	"sort"
	"strings"
)

// Every credential shape LooksLikeSecret recognises contains one of a few
// literal keywords (a credential key such as "password", a header name such
// as "authorization", the "eyJ" JWT header, a vendor token prefix). A regexp
// search still visits every byte of the text for each pattern, which made the
// trusted workspace baseline cost roughly 30 s of CPU per session on a
// mid-size repository (issue #462). secretScan finds the keywords with plain
// substring search over an ASCII-folded copy of the text and confines each
// pattern to the lines around its keywords. The windows are line-aligned and
// wide enough to hold every match the pattern can produce, so the verdict is
// identical to evaluating the pattern over the whole text; the differential
// tests and fuzzer in secretscan_test.go pin that equivalence.
//
// Keywords are matched case-insensitively by the patterns, so the fold must
// agree with regexp case folding. ASCII folding does, except for the four
// non-ASCII runes that fold or lower-case into ASCII letters (İ, ı, ſ, and
// the Kelvin sign); a text containing any of them is evaluated over the whole
// text instead.
type secretScan struct {
	text  string
	lower string
	// full disables keyword windows: every pattern runs over the whole
	// text, exactly as before the keyword index existed.
	full bool

	lines          []string
	lineStarts     []int
	candidateLines []int
	candidatesDone bool
	// positions caches the keyword occurrences found for each keyword set.
	positions map[*keywordSet][]int
}

// keywordSet is a set of lower-case ASCII substrings, one of which every
// match of the patterns it gates must contain.
type keywordSet struct {
	needles []string
}

// foldUnsafeRunes are the non-ASCII runes whose simple case fold (regexp
// (?i)) or unicode.ToLower (strings.ToLower) reaches an ASCII letter.
// TestFoldUnsafeRunesAreComplete enumerates the Unicode tables to keep the
// list complete.
const foldUnsafeRunes = "İıſK"

const (
	// secretScanTokenRuns bounds how far past a keyword a pattern's match can
	// extend, counted in whitespace-separated runs of non-whitespace text: the
	// longest credential shapes are "<keyword> [quote] : value" and
	// "authorization : bearer value", at most three runs before the value
	// begins, and every value is confined to the line it starts on.
	secretScanTokenRuns = 4
	// secretScanMaxKeywordOccurrences bounds the cached start/end pairs for a
	// keyword set. Dense input falls back to exact whole-text evaluation rather
	// than building an index larger than the text it is meant to accelerate.
	secretScanMaxKeywordOccurrences = 4 << 10
)

var (
	// credentialKeywords are the suffixes of every alternative in the
	// credential-key alternation shared by the assignment and YAML patterns
	// (api/private key, *token, password, *secret, credential(s)).
	credentialKeywords = &keywordSet{needles: []string{
		"apikey", "api_key", "api-key", "privatekey", "private_key", "private-key",
		"token", "password", "secret", "credential",
	}}
	sensitivePrefixKeywords = &keywordSet{needles: []string{"github_pat_", "ghp_", "xoxb-", "sk-", "akia", "asia"}}
	jwtKeywords             = &keywordSet{needles: []string{"eyj"}}
	authorizationKeywords   = &keywordSet{needles: []string{"authorization"}}
	txnTokenKeywords        = &keywordSet{needles: []string{"tx-token", "txn-token"}}
	cookieKeywords          = &keywordSet{needles: []string{"cookie"}}
	// Each signed-URL parameter alternative ends in one of these once its
	// "=" is appended.
	signedURLKeywords = &keywordSet{needles: []string{"sig=", "signature=", "sas=", "token=", "credential="}}
	pemBlockNeedle    = "-----" + "begin "
)

type textSpan struct {
	start, end int
}

// newSecretScan indexes text, which must already be stripped by
// stripUnsafeTextRunes (the index relies on "\r", "\f", and "\v" being
// absent so that only spaces, tabs, and newlines separate tokens).
func newSecretScan(text string) *secretScan {
	scan := &secretScan{text: text}
	var b strings.Builder
	b.Grow(len(text))
	nonASCII := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c >= 'A' && c <= 'Z':
			c += 'a' - 'A'
		case c >= 0x80:
			nonASCII = true
		}
		b.WriteByte(c)
	}
	scan.lower = b.String()
	scan.full = nonASCII && strings.ContainsAny(text, foldUnsafeRunes)
	return scan
}

// keywordPositions returns every occurrence of the set's needles in the
// folded text as (start, end) pairs sorted by start, computed once per set.
func (s *secretScan) keywordPositions(set *keywordSet) []int {
	if positions, ok := s.positions[set]; ok {
		return positions
	}
	var positions []int
	for _, needle := range set.needles {
		for from := 0; from < len(s.lower); {
			i := strings.Index(s.lower[from:], needle)
			if i < 0 {
				break
			}
			pos := from + i
			if len(positions) == 2*secretScanMaxKeywordOccurrences {
				s.full = true
				s.positions = nil
				return nil
			}
			positions = append(positions, pos, pos+len(needle))
			from = pos + 1
		}
	}
	if len(set.needles) > 1 {
		sort.Sort(keywordPositionPairs(positions))
	}
	if s.positions == nil {
		s.positions = map[*keywordSet][]int{}
	}
	s.positions[set] = positions
	return positions
}

// keywordPositionPairs sorts a flat (start, end) pair list by start.
type keywordPositionPairs []int

func (p keywordPositionPairs) Len() int           { return len(p) / 2 }
func (p keywordPositionPairs) Less(i, j int) bool { return p[2*i] < p[2*j] }
func (p keywordPositionPairs) Swap(i, j int) {
	p[2*i], p[2*j] = p[2*j], p[2*i]
	p[2*i+1], p[2*j+1] = p[2*j+1], p[2*i+1]
}

// windows returns the merged, line-aligned spans of text in which a pattern
// that must contain one of the set's keywords can match. runs is how many
// whitespace-separated runs after a keyword the pattern may still span.
func (s *secretScan) windows(set *keywordSet, runs int) []textSpan {
	if s.full {
		return []textSpan{{start: 0, end: len(s.text)}}
	}
	positions := s.keywordPositions(set)
	if s.full {
		return []textSpan{{start: 0, end: len(s.text)}}
	}
	if len(positions) == 0 {
		return nil
	}
	// Keyword positions are sorted by start, and spanAround is monotone in
	// its start, so the spans arrive sorted and merge in one pass.
	merged := []textSpan{s.spanAround(positions[0], positions[1], runs)}
	for i := 2; i < len(positions); i += 2 {
		span := s.spanAround(positions[i], positions[i+1], runs)
		last := &merged[len(merged)-1]
		// Adjacent windows share only the newline between them; merging
		// them keeps the window a contiguous, line-aligned slice.
		if span.start <= last.end+1 {
			if span.end > last.end {
				last.end = span.end
			}
			continue
		}
		merged = append(merged, span)
	}
	return merged
}

// spanAround returns the line-aligned span from the start of the line
// holding the keyword at [start, end) to the end of the line reached after
// skipping runs whitespace-separated runs of non-whitespace text.
func (s *secretScan) spanAround(start, end, runs int) textSpan {
	lineStart := strings.LastIndexByte(s.text[:start], '\n') + 1
	pos := end
	for r := 0; r < runs && pos < len(s.text); r++ {
		for pos < len(s.text) && isScanSpace(s.text[pos]) {
			pos++
		}
		for pos < len(s.text) && !isScanSpace(s.text[pos]) {
			pos++
		}
	}
	lineEnd := len(s.text)
	if i := strings.IndexByte(s.text[pos:], '\n'); i >= 0 {
		lineEnd = pos + i
	}
	return textSpan{start: lineStart, end: lineEnd}
}

// isScanSpace mirrors the RE2 \s class ([\t\n\f\r ]).
func isScanSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r'
}

// matchAny reports whether pattern matches inside any keyword window.
func (s *secretScan) matchAny(pattern *regexp.Regexp, set *keywordSet, runs int) bool {
	for _, w := range s.windows(set, runs) {
		if pattern.MatchString(s.text[w.start:w.end]) {
			return true
		}
	}
	return false
}

// findAllSubmatch returns the submatches of pattern inside every keyword
// window, in text order.
func (s *secretScan) findAllSubmatch(pattern *regexp.Regexp, set *keywordSet, runs int) [][]string {
	windows := s.windows(set, runs)
	matches := make([][]string, 0, len(windows))
	for _, w := range windows {
		matches = append(matches, pattern.FindAllStringSubmatch(s.text[w.start:w.end], -1)...)
	}
	return matches
}

// sensitiveValueMatch reports whether pattern captures a value inside a
// keyword window that is neither a placeholder nor a code reference. The
// placeholder and code checks see the whole text, so an exemption that reads
// past the match (a call suffix continued on the next line) is unaffected
// by the window.
func (s *secretScan) sensitiveValueMatch(pattern *regexp.Regexp, set *keywordSet) bool {
	for _, w := range s.windows(set, secretScanTokenRuns) {
		for _, match := range pattern.FindAllStringSubmatchIndex(s.text[w.start:w.end], -1) {
			// Exactly one value alternative captures per match; find it.
			for group := 1; 2*group+1 < len(match); group++ {
				start, end := match[2*group], match[2*group+1]
				if start < 0 {
					continue
				}
				start, end = start+w.start, end+w.start
				value := s.text[start:end]
				if !secretValuePlaceholder(value) && !secretValueIsCode(s.text, value, end) {
					return true
				}
				break
			}
		}
	}
	return false
}

// containsFolded reports whether the case-folded text contains the ASCII
// needle, matching strings.Contains(strings.ToLower(text), needle).
func (s *secretScan) containsFolded(needle string) bool {
	if s.full {
		return strings.Contains(strings.ToLower(s.text), needle)
	}
	return strings.Contains(s.lower, needle)
}

// splitLines returns the text split into lines, computed once.
func (s *secretScan) splitLines() []string {
	if s.lines == nil {
		s.lines = strings.Split(s.text, "\n")
	}
	return s.lines
}

// credentialKeyLines returns, in order, the indices of the lines that can
// match a credential-keyed YAML pattern: the lines containing a credential
// keyword, or every line when keyword windows are disabled.
func (s *secretScan) credentialKeyLines() []int {
	if s.candidatesDone {
		return s.candidateLines
	}
	s.candidatesDone = true
	lines := s.splitLines()
	if s.full {
		s.candidateLines = make([]int, len(lines))
		for i := range lines {
			s.candidateLines[i] = i
		}
		return s.candidateLines
	}
	if s.lineStarts == nil {
		s.lineStarts = make([]int, 0, len(lines))
		offset := 0
		for _, line := range lines {
			s.lineStarts = append(s.lineStarts, offset)
			offset += len(line) + 1
		}
	}
	positions := s.keywordPositions(credentialKeywords)
	for i := 0; i < len(positions); i += 2 {
		line := sort.SearchInts(s.lineStarts, positions[i]+1) - 1
		if n := len(s.candidateLines); n == 0 || s.candidateLines[n-1] != line {
			s.candidateLines = append(s.candidateLines, line)
		}
	}
	return s.candidateLines
}
