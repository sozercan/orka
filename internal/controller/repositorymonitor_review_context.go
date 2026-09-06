package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/security"
)

// Review context bounds. The encoded orka.prReview.context.v1 payload is
// embedded in Task.spec.prompt, so the total budget must stay well below the
// etcd object size limit (~1.5 MiB) together with the rest of the prompt and
// the Task object; 700 KiB leaves that margin while matching the documented
// contract in website/docs/guides/repository-monitors.md. Patches are carried
// for at most MaxFiles files; identities (path, status, counts) are carried
// for at most MaxFileEntries files so the reviewer can still discover every
// changed file in the Git-metadata-free checkout. Identities take precedence
// over patches inside the byte budget.
const (
	repositoryMonitorReviewContextSchemaVersion    = "orka.prReview.context.v1"
	repositoryMonitorReviewContextMaxFiles         = 100
	repositoryMonitorReviewContextMaxFileEntries   = 2000
	repositoryMonitorReviewContextMaxBytes         = 700 << 10
	repositoryMonitorReviewContextMaxPatchBytes    = 64 << 10
	repositoryMonitorReviewContextMaxPathBytes     = 512
	repositoryMonitorReviewContextMaxStatusBytes   = 32
	repositoryMonitorReviewContextPatchTruncated   = "truncated"
	repositoryMonitorReviewContextStatusRemoved    = "removed"
	repositoryMonitorReviewContextStatusRenamed    = "renamed"
	repositoryMonitorReviewContextStatusModified   = "modified"
	repositoryMonitorReviewContextStatusChanged    = "changed"
	repositoryMonitorReviewContextStatusAdded      = "added"
	repositoryMonitorReviewContextStatusCopied     = "copied"
	repositoryMonitorReviewContextPatchUnavailable = "unavailable"
	repositoryMonitorReviewContextPatchCapped      = "capped"
	repositoryMonitorReviewContextBeginMarker      = "--- BEGIN orka.prReview.context.v1 ---"
	repositoryMonitorReviewContextEndMarker        = "--- END orka.prReview.context.v1 ---"

	repositoryMonitorReviewContextErrorGitHubStatus   = "github_api_status_"
	repositoryMonitorReviewContextErrorTimeout        = "timeout"
	repositoryMonitorReviewContextErrorNetwork        = "network_error"
	repositoryMonitorReviewContextErrorInvalidPayload = "invalid_response"
	repositoryMonitorReviewContextErrorRequestFailed  = "request_failed"

	// repositoryMonitorReviewContextArrayItemPrefix mirrors the indentation
	// json.MarshalIndent("", "  ") applies to elements of the top-level
	// "files" array so per-file size accounting matches the final encoding.
	repositoryMonitorReviewContextArrayItemPrefix = "    "
	repositoryMonitorReviewContextIndent          = "  "
)

type repositoryMonitorReviewContextFile struct {
	Path         string `json:"path"`
	PreviousPath string `json:"previousPath,omitempty"`
	Status       string `json:"status"`
	Additions    int    `json:"additions"`
	Deletions    int    `json:"deletions"`
	Patch        string `json:"patch,omitempty"`
	PatchOmitted string `json:"patchOmitted,omitempty"`
}

type repositoryMonitorReviewContextTruncation struct {
	Files bool `json:"files"`
	Bytes bool `json:"bytes"`
}

type repositoryMonitorReviewContext struct {
	SchemaVersion      string                                   `json:"schemaVersion"`
	Repo               string                                   `json:"repo"`
	PRNumber           int64                                    `json:"prNumber"`
	BaseSHA            string                                   `json:"baseSHA"`
	HeadSHA            string                                   `json:"headSHA"`
	ChangedFileCount   int                                      `json:"changedFileCount"`
	Files              []repositoryMonitorReviewContextFile     `json:"files"`
	Truncated          repositoryMonitorReviewContextTruncation `json:"truncated"`
	ContextUnavailable string                                   `json:"contextUnavailable,omitempty"`
}

// repositoryMonitorReviewContextDriftError reports that the pull request base,
// head, or head repository changed while the review context was assembled.
type repositoryMonitorReviewContextDriftError struct {
	Number int64
	Field  string
	Want   string
	Got    string
}

func (e *repositoryMonitorReviewContextDriftError) Error() string {
	return fmt.Sprintf("pull request #%d %s changed during review context assembly: want %q, got %q", e.Number, e.Field, e.Want, e.Got)
}

func newRepositoryMonitorReviewContext(owner, repository string, pr repositoryMonitorPullRequest) repositoryMonitorReviewContext {
	return repositoryMonitorReviewContext{
		SchemaVersion: repositoryMonitorReviewContextSchemaVersion,
		Repo:          owner + "/" + repository,
		PRNumber:      pr.Number,
		BaseSHA:       pr.BaseSHA,
		HeadSHA:       pr.HeadSHA,
		Files:         []repositoryMonitorReviewContextFile{},
	}
}

// buildRepositoryMonitorReviewContext fetches the pull request file list with
// patches, then refetches the pull request and fails closed when the base,
// head, or head repository drifted. The drift check runs even when the file
// listing failed so a moved head is never reviewed against stale identity.
// GitHub read failures do not fail the review: the returned context is
// marked unavailable with a sanitized error class only, so the reviewer
// inspects the checked-out tree instead.
func (r *RepositoryMonitorReconciler) buildRepositoryMonitorReviewContext(ctx context.Context, owner, repository, token string, pr repositoryMonitorPullRequest) (repositoryMonitorReviewContext, error) {
	logger := log.FromContext(ctx).WithName("repositorymonitor")
	// The file set is bound to the exact base/head SHAs through the compare
	// endpoint rather than the mutable pull request listing: a force-push
	// A->B->A between two PR reads would otherwise pass the drift check while
	// labelling B's patches as A.
	files, filesErr := r.listRepositoryMonitorCompareFiles(ctx, owner, repository, token, pr.BaseSHA, pr.HeadSHA)
	current, err := r.fetchRepositoryMonitorPullRequest(ctx, owner, repository, token, pr.Number)
	if err != nil {
		reviewContext := newRepositoryMonitorReviewContext(owner, repository, pr)
		reviewContext.ContextUnavailable = repositoryMonitorReviewContextErrorClass(err)
		logger.Info("pull request review context unavailable", "pr", pr.Number, "operation", "refetch_pull_request", "errorClass", reviewContext.ContextUnavailable)
		return reviewContext, nil
	}
	if driftErr := repositoryMonitorReviewContextDrift(pr, *current); driftErr != nil {
		return repositoryMonitorReviewContext{}, driftErr
	}
	if filesErr != nil {
		reviewContext := newRepositoryMonitorReviewContext(owner, repository, pr)
		reviewContext.ContextUnavailable = repositoryMonitorReviewContextErrorClass(filesErr)
		logger.Info("pull request review context unavailable", "pr", pr.Number, "operation", "list_files", "errorClass", reviewContext.ContextUnavailable)
		return reviewContext, nil
	}
	return repositoryMonitorReviewContextBindChangedFileCount(repositoryMonitorReviewContextFromFiles(owner, repository, pr, files), len(files), current.ChangedFiles), nil
}

// repositoryMonitorReviewContextBindChangedFileCount reconciles the compare
// listing against GitHub's authoritative changed-file total from the pull
// request read. The compare endpoint silently caps its file array, so a
// listing shorter than the total (or a cap-sized listing when the total is
// unknown) means files were never represented to the reviewer and the change
// set is incomplete.
func repositoryMonitorReviewContextBindChangedFileCount(reviewContext repositoryMonitorReviewContext, listed, total int) repositoryMonitorReviewContext {
	switch {
	case total > listed:
		reviewContext.ChangedFileCount = total
		reviewContext.Truncated.Files = true
	case total <= 0 && listed >= repositoryMonitorGitHubCompareMaxFiles:
		reviewContext.Truncated.Files = true
	}
	return reviewContext
}

func repositoryMonitorReviewContextDrift(expected, current repositoryMonitorPullRequest) error {
	checks := []struct {
		field     string
		want, got string
	}{
		{field: "head SHA", want: expected.HeadSHA, got: current.HeadSHA},
		{field: "base SHA", want: expected.BaseSHA, got: current.BaseSHA},
		{field: "head repository", want: expected.HeadRepo, got: current.HeadRepo},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.want) != strings.TrimSpace(check.got) {
			return &repositoryMonitorReviewContextDriftError{Number: expected.Number, Field: check.field, Want: check.want, Got: check.got}
		}
	}
	return nil
}

// repositoryMonitorReviewContextErrorClass reduces a GitHub read failure to a
// short class label. It never includes the error text, which may carry
// request URLs or response bodies.
func repositoryMonitorReviewContextErrorClass(err error) string {
	var apiErr *repositoryMonitorGitHubAPIError
	switch {
	case err == nil:
		return ""
	case errors.As(err, &apiErr):
		return fmt.Sprintf("%s%d", repositoryMonitorReviewContextErrorGitHubStatus, apiErr.StatusCode)
	case errors.Is(err, context.DeadlineExceeded):
		return repositoryMonitorReviewContextErrorTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return repositoryMonitorReviewContextErrorTimeout
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return repositoryMonitorReviewContextErrorNetwork
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return repositoryMonitorReviewContextErrorInvalidPayload
	}
	return repositoryMonitorReviewContextErrorRequestFailed
}

// repositoryMonitorReviewContextFromFiles renders the bounded context payload:
// identities for at most repositoryMonitorReviewContextMaxFileEntries files,
// patches (at most repositoryMonitorReviewContextMaxPatchBytes encoded each)
// for the first repositoryMonitorReviewContextMaxFiles of them, and at most
// repositoryMonitorReviewContextMaxBytes encoded payload overall. Identities
// are placed first: patches are only added while the budget allows, and
// identities are dropped only when the path list alone does not fit.
// truncated.files therefore means the change set is not fully represented,
// which the prompt turns into a mandatory non-passing verdict.
func repositoryMonitorReviewContextFromFiles(owner, repository string, pr repositoryMonitorPullRequest, files []repositoryMonitorPullRequestFileResponse) repositoryMonitorReviewContext {
	reviewContext := newRepositoryMonitorReviewContext(owner, repository, pr)
	reviewContext.ChangedFileCount = len(files)
	if len(files) > repositoryMonitorReviewContextMaxFileEntries {
		files = files[:repositoryMonitorReviewContextMaxFileEntries]
		reviewContext.Truncated.Files = true
	}

	entries := make([]repositoryMonitorReviewContextFile, 0, len(files))
	used := repositoryMonitorReviewContextEncodedSize(reviewContext)
	for i, file := range files {
		if repositoryMonitorReviewContextPathAltered(file) || repositoryMonitorReviewContextDeletedLinesAltered(file.Patch) {
			// A clipped, redacted, or control-stripped path or previous
			// path loses the file's exact identity, and the checkout
			// carries no Git metadata (and may not contain a deleted or
			// renamed-from path) to recover it, so the change set is no
			// longer completely represented. Likewise, a deleted patch
			// line that sanitization rewrote exists nowhere else: the
			// checkout holds only the new content.
			reviewContext.Truncated.Files = true
		}
		entry := repositoryMonitorReviewContextIdentityEntry(file, i >= repositoryMonitorReviewContextMaxFiles)
		entrySize := repositoryMonitorReviewContextEntrySize(entry, len(entries))
		if used+entrySize > repositoryMonitorReviewContextMaxBytes {
			reviewContext.Truncated.Files = true
			reviewContext.Truncated.Bytes = true
			break
		}
		entries = append(entries, entry)
		used += entrySize
	}
	for i := range min(len(entries), repositoryMonitorReviewContextMaxFiles) {
		withPatch := repositoryMonitorReviewContextFileEntry(files[i])
		if withPatch.Patch == "" {
			continue
		}
		delta := repositoryMonitorReviewContextEntrySize(withPatch, i) - repositoryMonitorReviewContextEntrySize(entries[i], i)
		if used+delta > repositoryMonitorReviewContextMaxBytes {
			reviewContext.Truncated.Bytes = true
			break
		}
		entries[i] = withPatch
		used += delta
	}
	reviewContext.Files = entries
	reviewContext = repositoryMonitorReviewContextTrimToBudget(reviewContext)
	return repositoryMonitorReviewContextMarkUnreviewableOmissions(reviewContext)
}

// repositoryMonitorReviewContextMarkUnreviewableOmissions marks the change
// set incomplete whenever an omitted patch (unavailable, truncated, or
// capped) hides what changed. The only exemption is a wholly added or copied
// file with no deletions: its complete new content is positively present in
// the head checkout, so nothing about the change is hidden. Everything else
// fails closed — a removed file, any patchless rename (a pure rename and a
// content-changing binary rename are reported identically, so the harmless
// case cannot be proven without the base blob), any deleted lines, and any
// modified/changed file (even addition-only: the checkout shows the final
// file but cannot identify which lines the change introduced).
func repositoryMonitorReviewContextMarkUnreviewableOmissions(reviewContext repositoryMonitorReviewContext) repositoryMonitorReviewContext {
	for _, file := range reviewContext.Files {
		if file.PatchOmitted == "" {
			// A complete patch must agree with GitHub's own line counts; a
			// short-served or otherwise inconsistent patch would hide part
			// of the change while claiming completeness.
			if file.Patch != "" && !patchMatchesLineCounts(file.Patch, file.Additions, file.Deletions) {
				reviewContext.Truncated.Files = true
				return reviewContext
			}
			continue
		}
		if (file.Status == repositoryMonitorReviewContextStatusAdded || file.Status == repositoryMonitorReviewContextStatusCopied) && file.Deletions == 0 {
			continue
		}
		reviewContext.Truncated.Files = true
		return reviewContext
	}
	return reviewContext
}

// repositoryMonitorReviewContextTrimToBudget verifies the final encoding,
// because the per-entry accounting is an estimate, and trims conservatively:
// the last remaining patch goes first, then the last identity entry.
func repositoryMonitorReviewContextTrimToBudget(reviewContext repositoryMonitorReviewContext) repositoryMonitorReviewContext {
	for repositoryMonitorReviewContextEncodedSize(reviewContext) > repositoryMonitorReviewContextMaxBytes && len(reviewContext.Files) > 0 {
		reviewContext.Truncated.Bytes = true
		if i := repositoryMonitorReviewContextLastPatchIndex(reviewContext.Files); i >= 0 {
			reviewContext.Files[i] = repositoryMonitorReviewContextDropPatch(reviewContext.Files[i])
			continue
		}
		reviewContext.Files = reviewContext.Files[:len(reviewContext.Files)-1]
		reviewContext.Truncated.Files = true
	}
	return reviewContext
}

func repositoryMonitorReviewContextLastPatchIndex(files []repositoryMonitorReviewContextFile) int {
	for i, file := range slices.Backward(files) {
		if file.Patch != "" {
			return i
		}
	}
	return -1
}

// repositoryMonitorReviewContextIdentityEntry renders the patch-free entry
// for a changed file. Files beyond the patch cap are marked capped; files
// inside it are marked truncated until a patch is attached, or unavailable
// when GitHub supplied none.
func repositoryMonitorReviewContextIdentityEntry(file repositoryMonitorPullRequestFileResponse, capped bool) repositoryMonitorReviewContextFile {
	if capped {
		// The patch is never rendered for a capped entry, so it is not
		// sanitized either: identity-only entries stay cheap.
		entry := repositoryMonitorReviewContextFileIdentity(file)
		entry.PatchOmitted = repositoryMonitorReviewContextPatchCapped
		return entry
	}
	return repositoryMonitorReviewContextDropPatch(repositoryMonitorReviewContextFileEntry(file))
}

func repositoryMonitorReviewContextFileIdentity(file repositoryMonitorPullRequestFileResponse) repositoryMonitorReviewContextFile {
	return repositoryMonitorReviewContextFile{
		Path:         repositoryMonitorReviewContextBoundedField(file.Filename, repositoryMonitorReviewContextMaxPathBytes),
		PreviousPath: repositoryMonitorReviewContextBoundedField(file.PreviousFilename, repositoryMonitorReviewContextMaxPathBytes),
		Status:       repositoryMonitorReviewContextBoundedField(file.Status, repositoryMonitorReviewContextMaxStatusBytes),
		Additions:    max(file.Additions, 0),
		Deletions:    max(file.Deletions, 0),
	}
}

func repositoryMonitorReviewContextFileEntry(file repositoryMonitorPullRequestFileResponse) repositoryMonitorReviewContextFile {
	entry := repositoryMonitorReviewContextFileIdentity(file)
	patch := repositoryMonitorReviewContextSanitize(file.Patch)
	if patch == "" {
		entry.PatchOmitted = repositoryMonitorReviewContextPatchUnavailable
		return entry
	}
	entry.Patch, entry.PatchOmitted = repositoryMonitorReviewContextTruncatePatch(patch, repositoryMonitorReviewContextMaxPatchBytes)
	return entry
}

func repositoryMonitorReviewContextDropPatch(entry repositoryMonitorReviewContextFile) repositoryMonitorReviewContextFile {
	if entry.Patch == "" {
		return entry
	}
	entry.Patch = ""
	entry.PatchOmitted = repositoryMonitorReviewContextPatchTruncated
	return entry
}

// repositoryMonitorReviewContextTruncatePatch keeps whole lines of patch so
// that the JSON-encoded string fits within maxEncodedBytes. It returns the
// omission marker when anything was cut.
func repositoryMonitorReviewContextTruncatePatch(patch string, maxEncodedBytes int) (string, string) {
	if repositoryMonitorReviewContextEncodedStringSize(patch) <= maxEncodedBytes {
		return patch, ""
	}
	candidate := patch
	if len(candidate) > maxEncodedBytes {
		candidate = candidate[:maxEncodedBytes]
	}
	for {
		cut := strings.LastIndexByte(candidate, '\n')
		if cut < 0 {
			return "", repositoryMonitorReviewContextPatchTruncated
		}
		candidate = candidate[:cut]
		if repositoryMonitorReviewContextEncodedStringSize(candidate) <= maxEncodedBytes {
			return candidate, repositoryMonitorReviewContextPatchTruncated
		}
	}
}

// repositoryMonitorReviewContextPathAltered reports whether the embedded
// path or previous path differs from GitHub's: bounding dropped bytes, or
// sanitization stripped controls or redacted a credential-shaped segment.
func repositoryMonitorReviewContextPathAltered(file repositoryMonitorPullRequestFileResponse) bool {
	for _, value := range []string{file.Filename, file.PreviousFilename} {
		if repositoryMonitorReviewContextBoundedField(value, repositoryMonitorReviewContextMaxPathBytes) != value {
			return true
		}
	}
	return false
}

func repositoryMonitorReviewContextBoundedField(value string, maxBytes int) string {
	shadow := strings.NewReplacer("\n", "", "\t", "").Replace(value)
	if sanitizedShadow := repositoryMonitorReviewContextSanitize(shadow); sanitizedShadow != shadow {
		value = sanitizedShadow
	} else {
		value = repositoryMonitorReviewContextSanitize(value)
	}
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\n", " "), "\t", " ")
	if len(value) <= maxBytes {
		return value
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

// repositoryMonitorReviewContextSanitize drops invalid UTF-8 and control
// characters other than newline and tab so GitHub-supplied text cannot carry
// terminal escapes or NUL bytes into the prompt, then redacts credential
// shapes: patches are untrusted pull request content that is persisted in
// Task.spec.prompt and readable through the Tasks API. Controls are stripped
// before redaction so a control byte inside a token cannot split it past the
// redactor and be reassembled into a credential by the strip.
// repositoryMonitorReviewContextDeletedLinesAltered reports whether
// sanitizing the patch would rewrite a deleted ("-") line — a redacted
// credential-shaped value, a stripped control rune, or invalid UTF-8. The
// reviewer cannot recover the original deleted content from the Git-free
// checkout, so the change set must be marked incomplete. The original text is
// never retained.
func repositoryMonitorReviewContextDeletedLinesAltered(patch string) bool {
	// GitHub file patches are hunk fragments without "--- a/…" file
	// headers, so every "-"-prefixed line is deleted content — including
	// one whose original text begins with "--".
	if !strings.HasPrefix(patch, "-") && !strings.Contains(patch, "\n-") {
		return false
	}
	// Sanitize the whole patch rather than each deleted line alone so a
	// credential spanning adjacent deleted lines is seen the way the
	// review context will render it.
	original := strings.Split(patch, "\n")
	sanitized := strings.Split(repositoryMonitorReviewContextSanitize(patch), "\n")
	if len(original) != len(sanitized) {
		return true
	}
	for i, line := range original {
		if strings.HasPrefix(line, "-") && line != sanitized[i] {
			return true
		}
	}
	return false
}

// patchMatchesLineCounts verifies a hunk fragment's added/deleted line
// totals against the counts GitHub reported for the file.
func patchMatchesLineCounts(patch string, additions, deletions int) bool {
	adds, dels := 0, 0
	inHunk := false
	for line := range strings.SplitSeq(patch, "\n") {
		if strings.HasPrefix(line, "@@") {
			inHunk = true
			continue
		}
		// Anything before the first hunk marker is header material
		// ("--- a/…", "+++ b/…"), not change content.
		if !inHunk || len(line) == 0 {
			continue
		}
		switch line[0] {
		case '+':
			adds++
		case '-':
			dels++
		}
	}
	return adds == additions && dels == deletions
}

// repositoryMonitorReviewContextSecretWindowLines bounds how many adjacent
// patch lines are reconstructed when looking for a credential that only
// becomes recognizable once its physical lines are joined (a YAML scalar
// folded or escaped across lines, a value continued on the next line).
const repositoryMonitorReviewContextSecretWindowLines = 8

func repositoryMonitorReviewContextSanitize(value string) string {
	stripped := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return -1
		}
		// Unicode format runes (zero-width spaces, joiners, directional
		// marks) are as invisible as C0/C1 controls and can split a
		// credential past the redactor; drop them before redaction too.
		if unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(value, ""))
	// Newlines and tabs survive the strip because they carry diff and code
	// structure, but both can split a credential past the redactor: a tab
	// within one line, a line break across two patch lines. Detection runs
	// on a shadow of each line with its diff marker and tabs removed; a
	// line the full secret policy recognizes is withheld outright, and so is
	// every line of a short window of adjacent lines that the policy only
	// recognizes once they are joined back together.
	lines := strings.Split(stripped, "\n")
	prefixes := make([]string, len(lines))
	shadows := make([]string, len(lines))
	withheld := make([]bool, len(lines))
	for i, line := range lines {
		if len(line) > 0 && (line[0] == '+' || line[0] == '-' || line[0] == ' ') {
			prefixes[i] = line[:1]
		}
		shadows[i] = strings.ReplaceAll(line[len(prefixes[i]):], "\t", "")
	}
	for i, line := range lines {
		shadow := shadows[i]
		if redacted := redact.SensitiveText(shadow); redacted != shadow {
			if strings.ContainsRune(line, '\t') {
				lines[i] = prefixes[i] + redacted
			} else {
				lines[i] = redact.SensitiveText(line)
			}
			continue
		}
		if security.LooksLikeSecret(shadow) {
			lines[i] = prefixes[i] + "[REDACTED]"
			withheld[i] = true
			continue
		}
		lines[i] = redact.SensitiveText(line)
	}
	for i := range lines {
		if withheld[i] || !repositoryMonitorReviewContextLineMayContinue(shadows[i]) {
			continue
		}
		var window strings.Builder
		window.WriteString(shadows[i])
		for j := i + 1; j < len(lines) && j < i+repositoryMonitorReviewContextSecretWindowLines; j++ {
			if withheld[j] {
				break
			}
			window.WriteString("\n" + shadows[j])
			if !security.LooksLikeSecret(window.String()) {
				continue
			}
			for k := i; k <= j; k++ {
				lines[k] = prefixes[k] + "[REDACTED]"
				withheld[k] = true
			}
			break
		}
	}
	// A final whole-text pass keeps any cross-line reassembly the redactor
	// itself performs.
	return redact.SensitiveText(strings.Join(lines, "\n"))
}

// repositoryMonitorReviewContextLineMayContinue reports whether a line could
// start an assignment whose value continues on the following lines: it
// carries a key separator and ends with an escaped line break, an open
// quote, a bare key, or a YAML block-scalar indicator. Only such lines seed
// the multi-line reconstruction, which keeps the policy cost proportional to
// the patch rather than to the patch times the window size.
func repositoryMonitorReviewContextLineMayContinue(shadow string) bool {
	trimmed := strings.TrimRight(shadow, " ")
	if trimmed == "" || !strings.ContainsAny(trimmed, ":=") {
		return false
	}
	if strings.HasSuffix(trimmed, "\\") || strings.HasSuffix(trimmed, ":") || strings.HasSuffix(trimmed, "=") {
		return true
	}
	for _, indicator := range []string{"|", ">", "|-", ">-", "|+", ">+"} {
		if strings.HasSuffix(trimmed, ": "+indicator) || strings.HasSuffix(trimmed, ":"+indicator) {
			return true
		}
	}
	return strings.Count(trimmed, "\"")%2 == 1 || strings.Count(trimmed, "'")%2 == 1
}

func repositoryMonitorReviewContextEncodedSize(reviewContext repositoryMonitorReviewContext) int {
	encoded, err := json.MarshalIndent(reviewContext, "", repositoryMonitorReviewContextIndent)
	if err != nil {
		return repositoryMonitorReviewContextMaxBytes + 1
	}
	return len(encoded)
}

func repositoryMonitorReviewContextEncodedStringSize(value string) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return len(value) + 2
	}
	return len(encoded)
}

// repositoryMonitorReviewContextEntrySize estimates the bytes one file entry
// adds to the indented payload, including the array separator and the
// per-element indentation applied by json.MarshalIndent.
func repositoryMonitorReviewContextEntrySize(entry repositoryMonitorReviewContextFile, existingEntries int) int {
	encoded, err := json.MarshalIndent(entry, repositoryMonitorReviewContextArrayItemPrefix, repositoryMonitorReviewContextIndent)
	if err != nil {
		return repositoryMonitorReviewContextMaxBytes + 1
	}
	// "[]" becomes "[\n    {...}\n  ]" for the first element; later elements add ",\n    ".
	overhead := len(",\n") + len(repositoryMonitorReviewContextArrayItemPrefix)
	if existingEntries == 0 {
		overhead = len("\n") + len(repositoryMonitorReviewContextArrayItemPrefix) + len("\n") + len(repositoryMonitorReviewContextIndent)
	}
	return len(encoded) + overhead
}

// renderRepositoryMonitorReviewContext wraps the indented JSON payload in
// line-anchored markers. JSON string values cannot contain a raw newline, so a
// marker preceded by a newline can only come from the renderer, never from a
// patch or path embedded in the payload.
func renderRepositoryMonitorReviewContext(reviewContext repositoryMonitorReviewContext) string {
	if reviewContext.Files == nil {
		reviewContext.Files = []repositoryMonitorReviewContextFile{}
	}
	encoded, err := json.MarshalIndent(reviewContext, "", repositoryMonitorReviewContextIndent)
	if err != nil {
		fallback := newRepositoryMonitorReviewContext("", "", repositoryMonitorPullRequest{})
		fallback.Repo = reviewContext.Repo
		fallback.PRNumber = reviewContext.PRNumber
		fallback.BaseSHA = reviewContext.BaseSHA
		fallback.HeadSHA = reviewContext.HeadSHA
		fallback.ContextUnavailable = repositoryMonitorReviewContextErrorInvalidPayload
		encoded, _ = json.MarshalIndent(fallback, "", repositoryMonitorReviewContextIndent)
	}
	return repositoryMonitorReviewContextBeginMarker + "\n" + string(encoded) + "\n" + repositoryMonitorReviewContextEndMarker
}

// repositoryMonitorReviewPromptContextBounds locates the rendered context
// block: the first line-anchored begin marker and the LAST line-anchored end
// marker after it. Untrusted payload text is JSON-encoded, so it can never
// place a marker at the start of a line; only the renderer's markers match.
func repositoryMonitorReviewPromptContextBounds(prompt string) (int, int, bool) {
	beginLine := "\n" + repositoryMonitorReviewContextBeginMarker + "\n"
	endLine := "\n" + repositoryMonitorReviewContextEndMarker
	start := strings.Index(prompt, beginLine)
	if start < 0 {
		return 0, 0, false
	}
	start++ // keep the newline that precedes the block
	end := strings.LastIndex(prompt, endLine)
	if end < start {
		return 0, 0, false
	}
	return start, end + len(endLine), true
}

// repositoryMonitorReviewPromptContext parses the embedded context block of a
// rendered review prompt.
func repositoryMonitorReviewPromptContext(prompt string) (repositoryMonitorReviewContext, bool) {
	start, end, found := repositoryMonitorReviewPromptContextBounds(prompt)
	if !found {
		return repositoryMonitorReviewContext{}, false
	}
	block := prompt[start:end]
	block = strings.TrimPrefix(block, repositoryMonitorReviewContextBeginMarker+"\n")
	block = strings.TrimSuffix(block, "\n"+repositoryMonitorReviewContextEndMarker)
	var parsed repositoryMonitorReviewContext
	if err := json.Unmarshal([]byte(block), &parsed); err != nil {
		return repositoryMonitorReviewContext{}, false
	}
	return parsed, true
}

// repositoryMonitorReviewContextIsUnavailableEnvelope reports whether the
// context is exactly the controller-shaped "GitHub unavailable" envelope for
// the expected pull request: no files, no truncation, and a well-formed error
// class. Such an envelope carries no untrusted content.
func repositoryMonitorReviewContextIsUnavailableEnvelope(candidate, expected repositoryMonitorReviewContext) bool {
	if candidate.SchemaVersion != expected.SchemaVersion || candidate.Repo != expected.Repo || candidate.PRNumber != expected.PRNumber ||
		candidate.BaseSHA != expected.BaseSHA || candidate.HeadSHA != expected.HeadSHA {
		return false
	}
	if candidate.ChangedFileCount != 0 || len(candidate.Files) != 0 || candidate.Truncated.Files || candidate.Truncated.Bytes {
		return false
	}
	return repositoryMonitorReviewContextErrorClassWellFormed(candidate.ContextUnavailable)
}

func repositoryMonitorReviewContextErrorClassWellFormed(class string) bool {
	switch class {
	case repositoryMonitorReviewContextErrorTimeout, repositoryMonitorReviewContextErrorNetwork, repositoryMonitorReviewContextErrorInvalidPayload, repositoryMonitorReviewContextErrorRequestFailed:
		return true
	}
	status, ok := strings.CutPrefix(class, repositoryMonitorReviewContextErrorGitHubStatus)
	if !ok || len(status) != 3 {
		return false
	}
	for _, r := range status {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// repositoryMonitorReviewVerdictGateReason enforces, controller-side, the
// rule the review prompt states: a "passed" verdict is only trustworthy when
// the reviewer saw the complete change set. A review Task whose embedded
// context is missing, unavailable, or file-truncated cannot have verified
// every changed file against a checkout without Git history, so its passed
// verdict must not become the automerge gate. The returned reason is empty
// when the verdict may stand.
func repositoryMonitorReviewVerdictGateReason(prompt, verdict string) string {
	if strings.TrimSpace(verdict) != repositoryMonitorReviewVerdictPassed {
		return ""
	}
	reviewContext, ok := repositoryMonitorReviewPromptContext(prompt)
	switch {
	case !ok:
		return "the review task carried no embedded diff context"
	case reviewContext.ContextUnavailable != "":
		return "the diff context was unavailable (" + reviewContext.ContextUnavailable + ")"
	case reviewContext.Truncated.Files:
		return "the change set was not completely represented in the diff context"
	default:
		return ""
	}
}
