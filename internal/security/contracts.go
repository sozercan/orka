package security

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/orka-agents/orka/internal/store"
)

const (
	ArtifactSlices          = "security-slices.json"
	ArtifactFindingsV2      = "security-findings.v2.json"
	ArtifactDroppedFindings = "security-dropped-findings.json"

	EnvRepositoryScanName   = "ORKA_SECURITY_REPOSITORY_SCAN"
	EnvReviewSliceJSON      = "ORKA_SECURITY_REVIEW_SLICE_JSON"
	EnvStage                = "ORKA_SECURITY_STAGE"
	EnvScanID               = "ORKA_SECURITY_SCAN_ID"
	EnvSliceID              = "ORKA_SECURITY_SLICE_ID"
	EnvFindingID            = "ORKA_SECURITY_FINDING_ID"
	EnvPatchBranch          = "ORKA_SECURITY_PATCH_BRANCH"
	EnvScanBaseCommit       = "ORKA_SECURITY_SCAN_BASE_COMMIT"
	EnvScanHeadCommit       = "ORKA_SECURITY_SCAN_HEAD_COMMIT"
	EnvScannerPolicyVersion = "ORKA_SECURITY_SCANNER_POLICY_VERSION"
	EnvPolicyDigest         = "ORKA_SECURITY_POLICY_DIGEST"
	EnvPolicyProvenance     = "ORKA_SECURITY_POLICY_PROVENANCE"

	ScannerPolicyVersion = "2026-06-orka-fp-policy-v1"

	SchemaVersionReviewSlices  = 1
	SchemaVersionReviewContext = 1
	SchemaVersionFindingsV2    = 2
	SchemaVersionPatchSummary  = 1
)

type ChangedLineRange = store.ChangedLineRange

// ReviewSlicesArtifact is the deterministic mapper output.
type ReviewSlicesArtifact struct {
	SchemaVersion        int                 `json:"schemaVersion"`
	BaseCommit           string              `json:"baseCommit,omitempty"`
	HeadCommit           string              `json:"headCommit,omitempty"`
	ChangedFilesComputed bool                `json:"changedFilesComputed,omitempty"`
	ChangedFiles         []string            `json:"changedFiles,omitempty"`
	ChangedLineRanges    []ChangedLineRange  `json:"changedLineRanges,omitempty"`
	DiffSummary          string              `json:"diffSummary,omitempty"`
	ChangedFilesError    string              `json:"changedFilesError,omitempty"`
	Slices               []store.ReviewSlice `json:"slices"`
}

type ReviewContextManifest struct {
	SchemaVersion     int                         `json:"schemaVersion"`
	SliceID           string                      `json:"sliceId"`
	ChangedFiles      []string                    `json:"changedFiles,omitempty"`
	ChangedLineRanges []ChangedLineRange          `json:"changedLineRanges,omitempty"`
	IncludedFiles     []ReviewContextIncludedFile `json:"includedFiles"`
	OmittedFiles      []ReviewContextOmittedFile  `json:"omittedFiles,omitempty"`
	PromptBytes       int                         `json:"promptBytes"`
	ApproximateTokens int                         `json:"approximateTokens"`
	Prompt            string                      `json:"prompt,omitempty"`
}

type ReviewContextIncludedFile struct {
	Path               string                   `json:"path"`
	Role               string                   `json:"role"`
	Bytes              int                      `json:"bytes"`
	IncludedBytes      int                      `json:"includedBytes"`
	IncludedLineRanges []ReviewContextLineRange `json:"includedLineRanges"`
	Excerpt            string                   `json:"excerpt,omitempty"`
	Truncated          bool                     `json:"truncated"`
	Readable           bool                     `json:"readable"`
	SkippedReason      *string                  `json:"skippedReason"`
}

type ReviewContextLineRange struct {
	StartLine int `json:"startLine"`
	EndLine   int `json:"endLine"`
}

type ReviewContextOmittedFile struct {
	Path   string `json:"path"`
	Role   string `json:"role"`
	Reason string `json:"reason"`
}

// FindingsV2Artifact captures evidence-backed slice review output.
type FindingsV2Artifact struct {
	SchemaVersion int                  `json:"schemaVersion"`
	Repository    FindingsV2Repository `json:"repository"`
	Scan          FindingsV2Scan       `json:"scan"`
	Findings      []FindingsV2Finding  `json:"findings"`
}

type FindingsV2Repository struct {
	RepoURL string `json:"repoURL"`
	Branch  string `json:"branch"`
	SubPath string `json:"subPath"`
	BaseSHA string `json:"baseSHA"`
	HeadSHA string `json:"headSHA"`
}

type FindingsV2Scan struct {
	Mode    string `json:"mode"`
	SliceID string `json:"sliceId"`
	Summary string `json:"summary"`
}

type FindingsV2Finding struct {
	Title                         string                  `json:"title"`
	Category                      string                  `json:"category"`
	Severity                      string                  `json:"severity"`
	Confidence                    string                  `json:"confidence"`
	Triage                        string                  `json:"triage"`
	Evidence                      []FindingsV2EvidenceRef `json:"evidence"`
	Summary                       string                  `json:"summary"`
	RootCause                     string                  `json:"rootCause"`
	Reproduction                  string                  `json:"reproduction"`
	Remediation                   string                  `json:"remediation"`
	SuggestedAction               string                  `json:"suggestedAction"`
	WhyTestsDoNotAlreadyCoverThis string                  `json:"whyTestsDoNotAlreadyCoverThis"`
	SuggestedRegressionTest       string                  `json:"suggestedRegressionTest"`
	MinimumFixScope               string                  `json:"minimumFixScope"`
}

type FindingsV2EvidenceRef struct {
	Path      string  `json:"path"`
	StartLine int     `json:"startLine"`
	EndLine   int     `json:"endLine"`
	Symbol    *string `json:"symbol"`
	Quote     *string `json:"quote"`
}

type DroppedFindingArtifact struct {
	SchemaVersion int                        `json:"schemaVersion"`
	Dropped       []DroppedFindingDiagnostic `json:"dropped"`
}

type DroppedFindingDiagnostic struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
	Sample any    `json:"sample,omitempty"`
	Layer  string `json:"layer"`
}

type PatchSummaryArtifact struct {
	SchemaVersion int            `json:"schemaVersion"`
	FindingID     string         `json:"findingId"`
	Summary       string         `json:"summary"`
	ChangedFiles  []string       `json:"changedFiles"`
	TestsRun      []PatchTestRun `json:"testsRun,omitempty"`
	Risk          string         `json:"risk"`
}

type PatchTestRun struct {
	Command  string `json:"command"`
	ExitCode int    `json:"exitCode"`
}

// ReviewContextArtifactName returns the context manifest artifact for a slice.
func ReviewContextArtifactName(sliceID string) string {
	return fmt.Sprintf("security-review-context-%s.json", sanitizeName(sliceID))
}

// ParseReviewSlicesArtifact parses and minimally validates mapper output.
func ParseReviewSlicesArtifact(data []byte) (*ReviewSlicesArtifact, error) {
	var artifact ReviewSlicesArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, err
	}
	if artifact.SchemaVersion != SchemaVersionReviewSlices {
		return nil, fmt.Errorf("unsupported security slices schemaVersion %d", artifact.SchemaVersion)
	}
	for _, file := range artifact.ChangedFiles {
		if !SafeRepoPath(file) {
			return nil, fmt.Errorf("changed file path %q is not repo-relative and safe", file)
		}
	}
	for _, lineRange := range artifact.ChangedLineRanges {
		if !validChangedLineRange(lineRange) {
			return nil, fmt.Errorf("changed line range for %q is invalid", lineRange.Path)
		}
	}
	for i := range artifact.Slices {
		if err := validateReviewSliceContract(artifact.Slices[i]); err != nil {
			return nil, fmt.Errorf("slice %d: %w", i, err)
		}
	}
	return &artifact, nil
}

// ParseReviewContextManifest parses and minimally validates a review context manifest.
func ParseReviewContextManifest(data []byte) (*ReviewContextManifest, error) {
	var manifest ReviewContextManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.SchemaVersion != SchemaVersionReviewContext {
		return nil, fmt.Errorf("unsupported review context schemaVersion %d", manifest.SchemaVersion)
	}
	if strings.TrimSpace(manifest.SliceID) == "" {
		return nil, fmt.Errorf("sliceId is required")
	}
	for _, file := range manifest.ChangedFiles {
		if !SafeRepoPath(file) {
			return nil, fmt.Errorf("changed file path %q is not repo-relative and safe", file)
		}
	}
	for _, lineRange := range manifest.ChangedLineRanges {
		if !validChangedLineRange(lineRange) {
			return nil, fmt.Errorf("changed line range for %q is invalid", lineRange.Path)
		}
	}
	for _, file := range manifest.IncludedFiles {
		if !SafeRepoPath(file.Path) {
			return nil, fmt.Errorf("included file path %q is not repo-relative and safe", file.Path)
		}
		for _, lineRange := range file.IncludedLineRanges {
			if lineRange.StartLine <= 0 || lineRange.EndLine < lineRange.StartLine {
				return nil, fmt.Errorf("included file %q has invalid line range", file.Path)
			}
		}
	}
	return &manifest, nil
}

// ParseFindingsV2Artifact parses and validates only the top-level contract.
func ParseFindingsV2Artifact(data []byte) (*FindingsV2Artifact, error) {
	var artifact FindingsV2Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, err
	}
	if artifact.SchemaVersion != SchemaVersionFindingsV2 {
		return nil, fmt.Errorf("unsupported findings schemaVersion %d", artifact.SchemaVersion)
	}
	if artifact.Findings == nil {
		return nil, fmt.Errorf("findings must be an array")
	}
	return &artifact, nil
}

func validateReviewSliceContract(slice store.ReviewSlice) error {
	if slice.SchemaVersion != 0 && slice.SchemaVersion != SchemaVersionReviewSlices {
		return fmt.Errorf("unsupported schemaVersion %d", slice.SchemaVersion)
	}
	if strings.TrimSpace(slice.ID) == "" {
		return fmt.Errorf("id is required")
	}
	if strings.TrimSpace(slice.RepositoryScan) == "" {
		return fmt.Errorf("repositoryScan is required")
	}
	if strings.TrimSpace(slice.Source) == "" {
		return fmt.Errorf("source is required")
	}
	if strings.TrimSpace(slice.Title) == "" {
		return fmt.Errorf("title is required")
	}
	for _, file := range append(append([]store.ReviewSliceFile{}, slice.OwnedFiles...), slice.ContextFiles...) {
		if !SafeRepoPath(file.Path) {
			return fmt.Errorf("path %q is not repo-relative and safe", file.Path)
		}
	}
	for _, test := range slice.Tests {
		if test.Path != "" && !SafeRepoPath(test.Path) {
			return fmt.Errorf("test path %q is not repo-relative and safe", test.Path)
		}
	}
	for _, file := range slice.ChangedFiles {
		if !SafeRepoPath(file) {
			return fmt.Errorf("changed file path %q is not repo-relative and safe", file)
		}
	}
	for _, lineRange := range slice.ChangedLineRanges {
		if !validChangedLineRange(lineRange) {
			return fmt.Errorf("changed line range for %q is invalid", lineRange.Path)
		}
	}
	return nil
}

func validChangedLineRange(lineRange ChangedLineRange) bool {
	return SafeRepoPath(lineRange.Path) && lineRange.StartLine > 0 && lineRange.EndLine >= lineRange.StartLine
}

// SafeRepoPath returns true when p is a clean relative repository path.
func SafeRepoPath(p string) bool {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	if p == "" || strings.HasPrefix(p, "/") {
		return false
	}
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
		return false
	}
	return cleaned == p
}

// FindingV2Fingerprint computes an Orka-owned stable fingerprint for a v2 finding.
func FindingV2Fingerprint(namespace, repositoryScan, repoURL, branch, subPath, sliceID string, finding FindingsV2Finding) string {
	refs := make([]findingV2FingerprintEvidenceRef, 0, len(finding.Evidence))
	seenRefs := map[string]struct{}{}
	for _, ref := range finding.Evidence {
		canonicalRef := findingV2FingerprintEvidenceRef{
			Path:      strings.TrimSpace(strings.ReplaceAll(ref.Path, "\\", "/")),
			StartLine: ref.StartLine,
			EndLine:   ref.EndLine,
			Symbol:    canonicalFingerprintSymbol(ref.Symbol),
		}
		key := canonicalEvidenceKey(canonicalRef)
		if _, ok := seenRefs[key]; ok {
			continue
		}
		seenRefs[key] = struct{}{}
		refs = append(refs, canonicalRef)
	}
	sort.Slice(refs, func(i, j int) bool {
		left := canonicalEvidenceKey(refs[i])
		right := canonicalEvidenceKey(refs[j])
		return left < right
	})

	payload := struct {
		Version        int                               `json:"version"`
		Namespace      string                            `json:"namespace"`
		RepositoryScan string                            `json:"repositoryScan"`
		RepoURL        string                            `json:"repoURL"`
		Branch         string                            `json:"branch"`
		SubPath        string                            `json:"subPath"`
		SliceID        string                            `json:"sliceID"`
		Category       string                            `json:"category"`
		Title          string                            `json:"title"`
		Evidence       []findingV2FingerprintEvidenceRef `json:"evidence"`
	}{
		Version:        2,
		Namespace:      strings.TrimSpace(namespace),
		RepositoryScan: strings.TrimSpace(repositoryScan),
		RepoURL:        strings.TrimSpace(repoURL),
		Branch:         strings.TrimSpace(branch),
		SubPath:        strings.Trim(strings.TrimSpace(subPath), "/"),
		SliceID:        strings.TrimSpace(sliceID),
		Category:       strings.ToLower(strings.TrimSpace(finding.Category)),
		Title:          normalizeFingerprintText(finding.Title),
		Evidence:       refs,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return "v2:" + hex.EncodeToString(sum[:])
}

// FindingV2TargetKey identifies the repository branch or ref and subpath that
// produced a finding without exposing those values through the store API.
func FindingV2TargetKey(repoURL, branch, subPath string) string {
	payload := struct {
		Version int    `json:"version"`
		RepoURL string `json:"repoURL"`
		Branch  string `json:"branch"`
		SubPath string `json:"subPath"`
	}{
		Version: 1,
		RepoURL: CanonicalRepositoryCloneURL(repoURL),
		Branch:  strings.TrimSpace(branch),
		SubPath: strings.Trim(strings.TrimSpace(subPath), "/"),
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return "v1:" + hex.EncodeToString(sum[:])
}

type findingV2FingerprintEvidenceRef struct {
	Path      string  `json:"path"`
	StartLine int     `json:"startLine"`
	EndLine   int     `json:"endLine"`
	Symbol    *string `json:"symbol,omitempty"`
}

func canonicalEvidenceKey(ref findingV2FingerprintEvidenceRef) string {
	symbol := ""
	if ref.Symbol != nil {
		symbol = strings.TrimSpace(*ref.Symbol)
	}
	return fmt.Sprintf("%s:%d:%d:%s", ref.Path, ref.StartLine, ref.EndLine, symbol)
}

func canonicalFingerprintSymbol(value *string) *string {
	if value == nil {
		return nil
	}
	symbol := strings.TrimSpace(*value)
	if symbol == "" {
		return nil
	}
	return &symbol
}

func normalizeFingerprintText(value string) string {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(value)))
	return strings.Join(fields, " ")
}
