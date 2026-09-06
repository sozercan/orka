package security

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/orka-agents/orka/internal/store"
)

const (
	AgentResultSchemaVersion = 1

	AgentResultKindThreatModel = "orka.security.threat-model.v1"
	AgentResultKindFindings    = "orka.security.findings.v1"
	AgentResultKindValidation  = "orka.security.validation.v1"
	AgentResultKindPatch       = "orka.security.patch.v1"

	maxThreatModelResultBytes = 1 << 20
	maxFindingsResultBytes    = 512 << 10
	maxValidationResultBytes  = 256 << 10
	maxPatchResultBytes       = 64 << 10
	// MaxPatchSummaryArtifactBytes bounds a pre-existing patch summary
	// artifact before it is decoded; it matches the terminal-result cap.
	MaxPatchSummaryArtifactBytes = maxPatchResultBytes
	maxPatchSummaryBytes         = 16 << 10
	maxPatchChangedFiles         = 64
	maxPatchTestsRun             = 64
	maxReviewContextBytes        = 256 << 10
	maxThreatModelBytes          = 768 << 10
	maxFindingTextBytes          = 64 << 10
	maxFindingSummaryBytes       = 16 << 10
	maxFindingEvidenceRefs       = 32
	maxFindingsPerSlice          = 3
	maxValidationListItems       = 64
	maxValidationItemBytes       = 8 << 10
	maxValidationEvidenceRefs    = 32
)

// AgentResultBinding is controller-owned identity that every SecurityScan
// agent result must echo exactly. It contains no credentials or mutable runtime
// configuration.
type AgentResultBinding struct {
	RepositoryScan string
	ScanID         string
	PolicyDigest   string
	ContextDigest  string
}

// ParseTrustedReviewContextManifest validates the mapper-owned context used as
// an evidence boundary for a review Task.
func ParseTrustedReviewContextManifest(data []byte) (*ReviewContextManifest, string, error) {
	var manifest ReviewContextManifest
	if err := decodeStrictResult(data, maxReviewContextBytes, &manifest); err != nil {
		return nil, "", err
	}
	if manifest.SchemaVersion != SchemaVersionReviewContext {
		return nil, "", fmt.Errorf("unsupported review context schemaVersion %d", manifest.SchemaVersion)
	}
	if strings.TrimSpace(manifest.SliceID) == "" {
		return nil, "", fmt.Errorf("review context sliceId is required")
	}
	if manifest.Prompt == "" {
		return nil, "", fmt.Errorf("review context prompt is required")
	}
	if len(manifest.Prompt) > defaultMaxReviewContextBytes || manifest.PromptBytes != len(manifest.Prompt) {
		return nil, "", fmt.Errorf("review context prompt does not match bounded promptBytes")
	}
	if manifest.ApproximateTokens != (manifest.PromptBytes+3)/4 {
		return nil, "", fmt.Errorf("review context approximateTokens does not match promptBytes")
	}
	if len(manifest.IncludedFiles) > defaultMaxReviewContextFiles {
		return nil, "", fmt.Errorf("review context includes more than %d files", defaultMaxReviewContextFiles)
	}
	if len(manifest.ChangedFiles) > maxReviewContextChangedFiles || len(manifest.ChangedLineRanges) > maxReviewContextChangedLineRanges {
		return nil, "", fmt.Errorf("review context changed-code metadata exceeds bounds")
	}
	if len(manifest.OmittedFiles) > 1024 {
		return nil, "", fmt.Errorf("review context omitted file metadata exceeds bounds")
	}
	parsed, err := ParseReviewContextManifest(data)
	if err != nil {
		return nil, "", err
	}
	digest, err := ReviewContextDigest(*parsed)
	if err != nil {
		return nil, "", err
	}
	return parsed, digest, nil
}

// ThreatModelResultEnvelope is the only accepted terminal result for the
// threat-model agent stage.
type ThreatModelResultEnvelope struct {
	SchemaVersion  int    `json:"schemaVersion"`
	Kind           string `json:"kind"`
	RepositoryScan string `json:"repositoryScan"`
	ScanID         string `json:"scanId"`
	PolicyDigest   string `json:"policyDigest"`
	ThreatModel    string `json:"threatModel"`
}

// FindingsResultEnvelope is the only accepted terminal result for one review
// slice. The context digest binds evidence to mapper-generated excerpts.
type FindingsResultEnvelope struct {
	SchemaVersion  int                `json:"schemaVersion"`
	Kind           string             `json:"kind"`
	RepositoryScan string             `json:"repositoryScan"`
	ScanID         string             `json:"scanId"`
	SliceID        string             `json:"sliceId"`
	PolicyDigest   string             `json:"policyDigest"`
	ContextDigest  string             `json:"contextDigest"`
	Findings       FindingsV2Artifact `json:"findings"`
}

// ValidationResultEnvelope is the only accepted terminal result for the
// per-finding validation stage.
type ValidationResultEnvelope struct {
	SchemaVersion  int                `json:"schemaVersion"`
	Kind           string             `json:"kind"`
	RepositoryScan string             `json:"repositoryScan"`
	ScanID         string             `json:"scanId"`
	FindingID      string             `json:"findingId"`
	PolicyDigest   string             `json:"policyDigest"`
	Validation     ValidationArtifact `json:"validation"`
}

type FindingsResultExpectation struct {
	Binding    AgentResultBinding
	SliceID    string
	Mode       string
	Repository FindingsV2Repository
}

type ValidationResultExpectation struct {
	Binding AgentResultBinding
	Finding *store.Finding
}

// ParseThreatModelResult strictly decodes and identity-binds a terminal ACP
// result. Markdown is kept inside JSON so unrelated assistant prose cannot be
// mistaken for scanner output.
func ParseThreatModelResult(data []byte, expected AgentResultBinding) (string, error) {
	var result ThreatModelResultEnvelope
	if err := decodeStrictResult(data, maxThreatModelResultBytes, &result); err != nil {
		return "", err
	}
	if result.SchemaVersion != AgentResultSchemaVersion {
		return "", fmt.Errorf("unsupported security result schemaVersion %d", result.SchemaVersion)
	}
	if result.Kind != AgentResultKindThreatModel {
		return "", fmt.Errorf("security result kind %q is not a threat model", result.Kind)
	}
	if err := validateAgentResultBinding(result.RepositoryScan, result.ScanID, result.PolicyDigest, "", expected); err != nil {
		return "", err
	}
	content := strings.TrimSpace(result.ThreatModel)
	if content == "" {
		return "", fmt.Errorf("threatModel is required")
	}
	if len(content) > maxThreatModelBytes {
		return "", fmt.Errorf("threatModel exceeds %d bytes", maxThreatModelBytes)
	}
	if !strings.HasPrefix(content, "#") {
		return "", fmt.Errorf("threatModel must be markdown beginning with a heading")
	}
	if securityResultLooksLikeToolTranscript(content) {
		return "", fmt.Errorf("threatModel looks like a tool transcript")
	}
	return content, nil
}

func securityResultLooksLikeToolTranscript(content string) bool {
	for _, marker := range []string{
		"<tool_call>",
		"</tool_call>",
		"<tool_name>",
		"</tool_name>",
		"<parameters>",
		"</parameters>",
		"<command>",
		"</command>",
	} {
		if strings.Contains(content, marker) {
			return true
		}
	}
	return false
}

// ParseFindingsResult strictly decodes a terminal review result and requires
// exact controller-owned repository, run, slice, policy, and context identity.
func ParseFindingsResult(data []byte, expected FindingsResultExpectation) (*FindingsV2Artifact, error) {
	var result FindingsResultEnvelope
	if err := decodeStrictResult(data, maxFindingsResultBytes, &result); err != nil {
		return nil, err
	}
	if result.SchemaVersion != AgentResultSchemaVersion {
		return nil, fmt.Errorf("unsupported security result schemaVersion %d", result.SchemaVersion)
	}
	if result.Kind != AgentResultKindFindings {
		return nil, fmt.Errorf("security result kind %q is not findings", result.Kind)
	}
	if err := validateAgentResultBinding(result.RepositoryScan, result.ScanID, result.PolicyDigest, result.ContextDigest, expected.Binding); err != nil {
		return nil, err
	}
	if strings.TrimSpace(result.SliceID) != strings.TrimSpace(expected.SliceID) {
		return nil, fmt.Errorf("security result sliceId does not match task slice")
	}
	if err := validateFindingsResultArtifact(result.Findings, expected); err != nil {
		return nil, err
	}
	return &result.Findings, nil
}

// ParseValidationResult strictly decodes a terminal validation result and
// restricts new evidence to the already accepted finding evidence boundary.
func ParseValidationResult(data []byte, expected ValidationResultExpectation) (*ValidationArtifact, error) {
	if expected.Finding == nil {
		return nil, fmt.Errorf("expected finding is required")
	}
	var result ValidationResultEnvelope
	if err := decodeStrictResult(data, maxValidationResultBytes, &result); err != nil {
		return nil, err
	}
	if result.SchemaVersion != AgentResultSchemaVersion {
		return nil, fmt.Errorf("unsupported security result schemaVersion %d", result.SchemaVersion)
	}
	if result.Kind != AgentResultKindValidation {
		return nil, fmt.Errorf("security result kind %q is not validation", result.Kind)
	}
	if err := validateAgentResultBinding(result.RepositoryScan, result.ScanID, result.PolicyDigest, "", expected.Binding); err != nil {
		return nil, err
	}
	findingID := strings.TrimSpace(expected.Finding.ID)
	if strings.TrimSpace(result.FindingID) != findingID || strings.TrimSpace(result.Validation.FindingID) != findingID {
		return nil, fmt.Errorf("security validation result findingId does not match task finding")
	}
	if err := validateValidationArtifact(result.Validation, expected.Finding); err != nil {
		return nil, err
	}
	return &result.Validation, nil
}

func decodeStrictResult(data []byte, maxBytes int, target any) error {
	if len(data) == 0 {
		return fmt.Errorf("security task result is empty")
	}
	if len(data) > maxBytes {
		return fmt.Errorf("security task result exceeds %d bytes", maxBytes)
	}
	if !utf8.Valid(data) {
		return fmt.Errorf("security task result is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode security task result: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errorsIsEOF(err) {
		if err == nil {
			return fmt.Errorf("security task result contains trailing JSON")
		}
		return fmt.Errorf("decode trailing security task result: %w", err)
	}
	return nil
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
}

func validateAgentResultBinding(repositoryScan, scanID, policyDigest, contextDigest string, expected AgentResultBinding) error {
	if strings.TrimSpace(repositoryScan) != strings.TrimSpace(expected.RepositoryScan) {
		return fmt.Errorf("security result repositoryScan does not match task owner")
	}
	if strings.TrimSpace(scanID) != strings.TrimSpace(expected.ScanID) {
		return fmt.Errorf("security result scanId does not match task run")
	}
	if strings.TrimSpace(policyDigest) != strings.TrimSpace(expected.PolicyDigest) {
		return fmt.Errorf("security result policyDigest does not match scan policy")
	}
	if strings.TrimSpace(contextDigest) != strings.TrimSpace(expected.ContextDigest) {
		return fmt.Errorf("security result contextDigest does not match mapper context")
	}
	return nil
}

func validateFindingsResultArtifact(artifact FindingsV2Artifact, expected FindingsResultExpectation) error {
	if artifact.SchemaVersion != SchemaVersionFindingsV2 {
		return fmt.Errorf("unsupported findings schemaVersion %d", artifact.SchemaVersion)
	}
	if artifact.Findings == nil {
		return fmt.Errorf("findings must be an array")
	}
	if len(artifact.Findings) > maxFindingsPerSlice {
		return fmt.Errorf("findings exceeds per-slice limit %d", maxFindingsPerSlice)
	}
	if artifact.Repository != expected.Repository {
		return fmt.Errorf("findings repository identity does not match scan run")
	}
	if strings.TrimSpace(artifact.Scan.Mode) != strings.TrimSpace(expected.Mode) {
		return fmt.Errorf("findings scan.mode does not match scan run")
	}
	if strings.TrimSpace(artifact.Scan.SliceID) != strings.TrimSpace(expected.SliceID) {
		return fmt.Errorf("findings scan.sliceId does not match task slice")
	}
	if err := boundedString("findings scan.summary", artifact.Scan.Summary, maxFindingSummaryBytes); err != nil {
		return err
	}
	for i := range artifact.Findings {
		if err := validateBoundedFinding(i, artifact.Findings[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateBoundedFinding(index int, finding FindingsV2Finding) error {
	fields := []struct {
		name  string
		value string
		limit int
	}{
		{"title", finding.Title, maxFindingSummaryBytes},
		{"category", finding.Category, maxFindingSummaryBytes},
		{"severity", finding.Severity, 32},
		{"confidence", finding.Confidence, 32},
		{"triage", finding.Triage, maxFindingSummaryBytes},
		{"summary", finding.Summary, maxFindingTextBytes},
		{"rootCause", finding.RootCause, maxFindingTextBytes},
		{"reproduction", finding.Reproduction, maxFindingTextBytes},
		{"remediation", finding.Remediation, maxFindingTextBytes},
		{"suggestedAction", finding.SuggestedAction, maxFindingTextBytes},
		{"whyTestsDoNotAlreadyCoverThis", finding.WhyTestsDoNotAlreadyCoverThis, maxFindingTextBytes},
		{"suggestedRegressionTest", finding.SuggestedRegressionTest, maxFindingTextBytes},
		{"minimumFixScope", finding.MinimumFixScope, maxFindingTextBytes},
	}
	for _, field := range fields {
		if err := boundedString(fmt.Sprintf("findings[%d].%s", index, field.name), field.value, field.limit); err != nil {
			return err
		}
	}
	if len(finding.Evidence) > maxFindingEvidenceRefs {
		return fmt.Errorf("findings[%d].evidence exceeds %d entries", index, maxFindingEvidenceRefs)
	}
	for evidenceIndex, ref := range finding.Evidence {
		if err := boundedString(fmt.Sprintf("findings[%d].evidence[%d].path", index, evidenceIndex), ref.Path, 4096); err != nil {
			return err
		}
		if ref.Symbol != nil {
			if err := boundedString(fmt.Sprintf("findings[%d].evidence[%d].symbol", index, evidenceIndex), *ref.Symbol, maxFindingSummaryBytes); err != nil {
				return err
			}
		}
		if ref.Quote != nil {
			if err := boundedString(fmt.Sprintf("findings[%d].evidence[%d].quote", index, evidenceIndex), *ref.Quote, maxFindingTextBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateValidationArtifact(artifact ValidationArtifact, finding *store.Finding) error {
	if artifact.Version != 1 {
		return fmt.Errorf("unsupported validation version %d", artifact.Version)
	}
	switch strings.TrimSpace(artifact.Status) {
	case "validated", "failed", "skipped":
	default:
		return fmt.Errorf("unsupported validation status %q", artifact.Status)
	}
	if strings.TrimSpace(artifact.Summary) == "" {
		return fmt.Errorf("validation summary is required")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"summary", artifact.Summary},
		{"reproduction", artifact.Reproduction},
		{"attack_path_analysis", artifact.AttackPathAnalysis},
		{"likelihood", artifact.Likelihood},
		{"impact", artifact.Impact},
	} {
		if err := boundedString("validation."+field.name, field.value, maxFindingTextBytes); err != nil {
			return err
		}
	}
	for name, values := range map[string][]string{
		"validation_steps": artifact.ValidationSteps,
		"assumptions":      artifact.Assumptions,
		"controls":         artifact.Controls,
		"blindspots":       artifact.Blindspots,
	} {
		if len(values) > maxValidationListItems {
			return fmt.Errorf("validation.%s exceeds %d entries", name, maxValidationListItems)
		}
		for i, value := range values {
			if err := boundedString(fmt.Sprintf("validation.%s[%d]", name, i), value, maxValidationItemBytes); err != nil {
				return err
			}
		}
	}
	if len(artifact.Evidence) > maxValidationEvidenceRefs {
		return fmt.Errorf("validation.evidence exceeds %d entries", maxValidationEvidenceRefs)
	}
	for i, ref := range artifact.Evidence {
		if err := validateValidationEvidenceRef(i, ref, finding.Evidence); err != nil {
			return err
		}
	}
	return nil
}

func validateValidationEvidenceRef(index int, ref store.FindingEvidenceRef, accepted []store.FindingEvidenceRef) error {
	if strings.TrimSpace(ref.TaskName) != "" || strings.TrimSpace(ref.Name) != "" {
		return fmt.Errorf("validation.evidence[%d] may not reference task artifacts", index)
	}
	switch strings.TrimSpace(ref.Kind) {
	case "file":
		if !SafeRepoPath(ref.Path) || ref.StartLine <= 0 || ref.EndLine < ref.StartLine {
			return fmt.Errorf("validation.evidence[%d] has invalid file range", index)
		}
		if !validationEvidenceWithinAcceptedFinding(ref, accepted) {
			return fmt.Errorf("validation.evidence[%d] is outside accepted finding evidence", index)
		}
	case "note":
		if strings.TrimSpace(ref.Label) == "" {
			return fmt.Errorf("validation.evidence[%d] note label is required", index)
		}
	default:
		return fmt.Errorf("validation.evidence[%d] kind %q is not allowed", index, ref.Kind)
	}
	for name, value := range map[string]string{
		"label":  ref.Label,
		"path":   ref.Path,
		"symbol": ref.Symbol,
		"quote":  ref.Quote,
	} {
		if err := boundedString(fmt.Sprintf("validation.evidence[%d].%s", index, name), value, maxFindingTextBytes); err != nil {
			return err
		}
	}
	return nil
}

func validationEvidenceWithinAcceptedFinding(ref store.FindingEvidenceRef, accepted []store.FindingEvidenceRef) bool {
	for _, candidate := range accepted {
		if candidate.Kind != "file" || candidate.Path != ref.Path {
			continue
		}
		if ref.StartLine >= candidate.StartLine && ref.EndLine <= candidate.EndLine {
			return true
		}
	}
	return false
}

func boundedString(name, value string, maxBytes int) error {
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", name, maxBytes)
	}
	if strings.ContainsRune(value, '\x00') || !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid text", name)
	}
	return nil
}

// ReviewContextDigest returns the canonical digest used to bind a review Task
// and its result to one mapper-generated context manifest.
func ReviewContextDigest(manifest ReviewContextManifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// PatchResultEnvelope is the harness-v2 terminal result of a security patch
// task. The agent applies the fix to the workspace and returns this envelope
// as its only output; it never writes artifact files. The authoritative diff
// is derived by the controller from the governed publication, so the envelope
// carries only the agent's account of what it changed.
type PatchResultEnvelope struct {
	SchemaVersion  int            `json:"schemaVersion"`
	Kind           string         `json:"kind"`
	RepositoryScan string         `json:"repositoryScan"`
	FindingID      string         `json:"findingId"`
	Summary        string         `json:"summary"`
	ChangedFiles   []string       `json:"changedFiles"`
	TestsRun       []PatchTestRun `json:"testsRun,omitempty"`
	Risk           string         `json:"risk"`
}

// PatchResultExpectation is the controller-owned identity a patch result must
// match exactly.
type PatchResultExpectation struct {
	RepositoryScan string
	FindingID      string
}

// ParsePatchResult strictly decodes a terminal patch result and requires the
// exact repository scan and finding identity. It returns the bounded patch
// summary the controller persists alongside the publication-derived diff.
func ParsePatchResult(data []byte, expected PatchResultExpectation) (*PatchSummaryArtifact, error) {
	var result PatchResultEnvelope
	if err := decodeStrictResult(data, maxPatchResultBytes, &result); err != nil {
		return nil, err
	}
	if result.SchemaVersion != AgentResultSchemaVersion {
		return nil, fmt.Errorf("unsupported security result schemaVersion %d", result.SchemaVersion)
	}
	if result.Kind != AgentResultKindPatch {
		return nil, fmt.Errorf("security result kind %q is not a patch", result.Kind)
	}
	if strings.TrimSpace(expected.RepositoryScan) == "" || result.RepositoryScan != expected.RepositoryScan {
		return nil, fmt.Errorf("patch result repositoryScan does not match the expected scan")
	}
	if strings.TrimSpace(expected.FindingID) == "" || result.FindingID != expected.FindingID {
		return nil, fmt.Errorf("patch result findingId does not match the expected finding")
	}
	return NormalizePatchSummaryArtifact(PatchSummaryArtifact{
		SchemaVersion: SchemaVersionPatchSummary,
		FindingID:     result.FindingID,
		Summary:       result.Summary,
		ChangedFiles:  result.ChangedFiles,
		TestsRun:      result.TestsRun,
		Risk:          result.Risk,
	})
}

// NormalizePatchSummaryArtifact applies the bounded, credential-rejecting
// validation every durable patch summary must pass, whether it arrived as a
// harness-v2 terminal result or as a pre-existing artifact written through the
// upload API. Summary text, changed-file paths, and test commands are
// agent-controlled and persist in artifacts, status, and PR bodies, so a
// credential-shaped value in any of them fails closed; the value itself is
// deliberately kept out of the error.
func NormalizePatchSummaryArtifact(artifact PatchSummaryArtifact) (*PatchSummaryArtifact, error) {
	if artifact.SchemaVersion != SchemaVersionPatchSummary {
		return nil, fmt.Errorf("unsupported patch summary schemaVersion %d", artifact.SchemaVersion)
	}
	if strings.TrimSpace(artifact.FindingID) == "" {
		return nil, fmt.Errorf("patch summary findingId is required")
	}
	summary := strings.TrimSpace(stripUnsafeTextRunes(artifact.Summary))
	if summary == "" {
		return nil, fmt.Errorf("patch summary is required")
	}
	if len(summary) > maxPatchSummaryBytes {
		return nil, fmt.Errorf("patch summary exceeds %d bytes", maxPatchSummaryBytes)
	}
	if securityResultLooksLikeToolTranscript(summary) {
		return nil, fmt.Errorf("patch summary looks like a tool transcript")
	}
	// Summary and test commands are agent-controlled and become a durable
	// artifact; a credential echoed there (for example while describing the
	// secret a remediation removed) must never persist. The value itself is
	// deliberately kept out of the error.
	if LooksLikeSecret(summary) {
		return nil, fmt.Errorf("patch summary contains a credential-shaped value")
	}
	if len(artifact.ChangedFiles) == 0 {
		return nil, fmt.Errorf("patch changedFiles is required")
	}
	if len(artifact.ChangedFiles) > maxPatchChangedFiles {
		return nil, fmt.Errorf("patch changedFiles exceeds %d entries", maxPatchChangedFiles)
	}
	changed := make([]string, 0, len(artifact.ChangedFiles))
	seen := make(map[string]struct{}, len(artifact.ChangedFiles))
	for _, file := range artifact.ChangedFiles {
		file = strings.TrimSpace(strings.ReplaceAll(stripUnsafeTextRunes(file), "\\", "/"))
		for strings.HasPrefix(file, "./") {
			file = strings.TrimPrefix(file, "./")
		}
		if file == "" || !SafeRepoPath(file) {
			return nil, fmt.Errorf("patch changedFiles contains an unsafe path")
		}
		// Paths are agent-controlled and persist in artifacts, status, and
		// PR bodies; a credential smuggled as a path segment must not.
		if LooksLikeSecret(file) {
			return nil, fmt.Errorf("patch changedFiles contains a credential-shaped path")
		}
		if _, duplicate := seen[file]; duplicate {
			continue
		}
		seen[file] = struct{}{}
		changed = append(changed, file)
	}
	if len(artifact.TestsRun) > maxPatchTestsRun {
		return nil, fmt.Errorf("patch testsRun exceeds %d entries", maxPatchTestsRun)
	}
	var testsRun []PatchTestRun
	if len(artifact.TestsRun) > 0 {
		testsRun = make([]PatchTestRun, len(artifact.TestsRun))
	}
	for i, run := range artifact.TestsRun {
		command := strings.TrimSpace(stripUnsafeTextRunes(run.Command))
		if command == "" || len(command) > maxValidationItemBytes {
			return nil, fmt.Errorf("patch testsRun contains an invalid command")
		}
		if LooksLikeSecret(command) {
			return nil, fmt.Errorf("patch testsRun contains a credential-shaped command")
		}
		testsRun[i] = PatchTestRun{Command: command, ExitCode: run.ExitCode}
	}
	risk := strings.ToLower(strings.TrimSpace(artifact.Risk))
	switch risk {
	case "low", "medium", "high":
	default:
		return nil, fmt.Errorf("patch risk must be low, medium, or high")
	}
	return &PatchSummaryArtifact{
		SchemaVersion: SchemaVersionPatchSummary,
		FindingID:     artifact.FindingID,
		Summary:       summary,
		ChangedFiles:  changed,
		TestsRun:      testsRun,
		Risk:          risk,
	}, nil
}
