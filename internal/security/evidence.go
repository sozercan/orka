package security

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orka-agents/orka/internal/store"
)

const (
	findingSeverityCritical = "critical"
	findingLevelHigh        = "high"
	findingLevelMedium      = "medium"
	findingLevelLow         = "low"
)

type FindingValidationOptions struct {
	Namespace            string
	RepositoryScan       string
	ScanRunID            string
	TaskName             string
	WorkspaceRoot        string
	TrustedRepository    FindingsV2Repository
	UseTrustedRepository bool
}

type FindingValidationResult struct {
	Accepted []*store.Finding
	Dropped  []DroppedFindingDiagnostic
}

// ValidateFindingsV2 validates and partitions a v2 findings artifact.
func ValidateFindingsV2(artifact FindingsV2Artifact, manifest ReviewContextManifest, opts FindingValidationOptions) FindingValidationResult {
	result := FindingValidationResult{}
	included := includedFileMap(manifest)
	for index, finding := range artifact.Findings {
		if reason := validateFindingRequiredFields(finding); reason != "" {
			result.Dropped = append(result.Dropped, droppedDiagnostic(index, reason, finding))
			continue
		}
		var reason string
		finding, reason = normalizeFindingEnums(finding)
		if reason != "" {
			result.Dropped = append(result.Dropped, droppedDiagnostic(index, reason, finding))
			continue
		}
		if reason := validateFindingEvidence(finding, included, opts.WorkspaceRoot); reason != "" {
			result.Dropped = append(result.Dropped, droppedDiagnostic(index, reason, finding))
			continue
		}
		result.Accepted = append(result.Accepted, ToFindingV2(
			opts.Namespace,
			opts.RepositoryScan,
			opts.ScanRunID,
			opts.TaskName,
			findingRepositoryMetadata(artifact.Repository, opts),
			artifact.Scan,
			finding,
		))
	}
	return result
}

func findingRepositoryMetadata(artifactRepo FindingsV2Repository, opts FindingValidationOptions) FindingsV2Repository {
	if opts.UseTrustedRepository {
		return opts.TrustedRepository
	}
	return artifactRepo
}

func includedFileMap(manifest ReviewContextManifest) map[string]ReviewContextIncludedFile {
	out := make(map[string]ReviewContextIncludedFile, len(manifest.IncludedFiles))
	for _, file := range manifest.IncludedFiles {
		out[file.Path] = file
	}
	return out
}

func validateFindingRequiredFields(finding FindingsV2Finding) string {
	required := []struct {
		name  string
		value string
	}{
		{name: "title", value: finding.Title},
		{name: "category", value: finding.Category},
		{name: "severity", value: finding.Severity},
		{name: "confidence", value: finding.Confidence},
		{name: "summary", value: finding.Summary},
		{name: "remediation", value: finding.Remediation},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return field.name + " is required"
		}
	}
	if len(finding.Evidence) == 0 {
		return "evidence is required"
	}
	return ""
}

func normalizeFindingEnums(finding FindingsV2Finding) (FindingsV2Finding, string) {
	severity, ok := canonicalFindingSeverity(finding.Severity)
	if !ok {
		return finding, fmt.Sprintf("unsupported severity %q", finding.Severity)
	}
	confidence, ok := canonicalFindingConfidence(finding.Confidence)
	if !ok {
		return finding, fmt.Sprintf("unsupported confidence %q", finding.Confidence)
	}
	finding.Severity = severity
	finding.Confidence = confidence
	return finding, ""
}

func canonicalFindingSeverity(value string) (string, bool) {
	switch normalized := strings.ToLower(strings.TrimSpace(value)); normalized {
	case findingSeverityCritical, findingLevelHigh, findingLevelMedium, findingLevelLow:
		return normalized, true
	default:
		return "", false
	}
}

func canonicalFindingConfidence(value string) (string, bool) {
	switch normalized := strings.ToLower(strings.TrimSpace(value)); normalized {
	case findingLevelHigh, findingLevelMedium, findingLevelLow:
		return normalized, true
	default:
		return "", false
	}
}

func validateFindingEvidence(finding FindingsV2Finding, included map[string]ReviewContextIncludedFile, workspaceRoot string) string {
	for _, ref := range finding.Evidence {
		if !SafeRepoPath(ref.Path) {
			return fmt.Sprintf("evidence path %q is not repo-relative and safe", ref.Path)
		}
		file, ok := included[ref.Path]
		if !ok {
			return "evidence file was not included in review context"
		}
		if ref.StartLine <= 0 || ref.EndLine <= 0 {
			return "evidence line ranges must include both startLine and endLine"
		}
		if ref.EndLine < ref.StartLine {
			return "evidence line range is inverted"
		}
		if !lineRangeIncluded(ref.StartLine, ref.EndLine, file.IncludedLineRanges) {
			return "evidence line range is outside included review context"
		}
		if ref.Quote != nil && strings.TrimSpace(*ref.Quote) != "" {
			if reason := validateEvidenceQuote(file, workspaceRoot, ref); reason != "" {
				return reason
			}
		}
	}
	return ""
}

func lineRangeIncluded(startLine, endLine int, ranges []ReviewContextLineRange) bool {
	for _, lineRange := range ranges {
		if startLine >= lineRange.StartLine && endLine <= lineRange.EndLine {
			return true
		}
	}
	return false
}

func validateEvidenceQuote(file ReviewContextIncludedFile, workspaceRoot string, ref FindingsV2EvidenceRef) string {
	if ref.Quote == nil || strings.TrimSpace(*ref.Quote) == "" {
		return ""
	}
	if strings.TrimSpace(file.Excerpt) != "" {
		if excerpt := excerptLinesForOriginalRange(file.Excerpt, file.IncludedLineRanges, ref.StartLine, ref.EndLine); excerpt != "" && strings.Contains(excerpt, *ref.Quote) {
			return ""
		}
		return "evidence quote does not match cited file range"
	}
	if strings.TrimSpace(workspaceRoot) == "" {
		return ""
	}
	fullPath := filepath.Join(workspaceRoot, filepath.FromSlash(ref.Path))
	cleanRoot, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return "workspace root cannot be resolved"
	}
	cleanPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "evidence file cannot be resolved"
	}
	if cleanPath != cleanRoot && !strings.HasPrefix(cleanPath, cleanRoot+string(filepath.Separator)) {
		return "evidence path escapes workspace"
	}
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return "evidence file is not readable"
	}
	return validateEvidenceQuoteInContent(string(data), ref)
}

func excerptLinesForOriginalRange(content string, ranges []ReviewContextLineRange, startLine, endLine int) string {
	excerptStartLine := 1
	for _, lineRange := range ranges {
		lineCount := lineRange.EndLine - lineRange.StartLine + 1
		if startLine >= lineRange.StartLine && endLine <= lineRange.EndLine {
			localStart := excerptStartLine + startLine - lineRange.StartLine
			localEnd := excerptStartLine + endLine - lineRange.StartLine
			return linesInRange(content, localStart, localEnd)
		}
		excerptStartLine += lineCount
	}
	return ""
}

func validateEvidenceQuoteInContent(content string, ref FindingsV2EvidenceRef) string {
	excerpt := linesInRange(content, ref.StartLine, ref.EndLine)
	if excerpt == "" {
		return "evidence line range is stale"
	}
	quote := *ref.Quote
	if strings.Contains(excerpt, quote) {
		return ""
	}
	return "evidence quote does not match cited file range"
}

func linesInRange(content string, startLine, endLine int) string {
	lines := strings.Split(content, "\n")
	if startLine > len(lines) {
		return ""
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	return strings.Join(lines[startLine-1:endLine], "\n")
}

func droppedDiagnostic(index int, reason string, finding FindingsV2Finding) DroppedFindingDiagnostic {
	return DroppedFindingDiagnostic{
		Index:  index,
		Reason: reason,
		Sample: droppedFindingSample(finding),
		Layer:  "validation",
	}
}

func droppedFindingSample(finding FindingsV2Finding) map[string]string {
	sample := map[string]string{}
	if strings.TrimSpace(finding.Title) != "" {
		sample["title"] = finding.Title
	}
	if strings.TrimSpace(finding.Category) != "" {
		sample["category"] = finding.Category
	}
	if strings.TrimSpace(finding.Severity) != "" {
		sample["severity"] = finding.Severity
	}
	return sample
}

// ToFindingV2 converts a validated v2 finding to durable store shape.
func ToFindingV2(
	namespace string,
	repositoryScan string,
	scanRunID string,
	taskName string,
	repo FindingsV2Repository,
	scan FindingsV2Scan,
	item FindingsV2Finding,
) *store.Finding {
	sliceID := strings.TrimSpace(scan.SliceID)
	fingerprint := FindingV2Fingerprint(namespace, repositoryScan, repo.RepoURL, repo.Branch, repo.SubPath, sliceID, item)
	evidence := make([]store.FindingEvidenceRef, 0, len(item.Evidence))
	for _, ref := range canonicalEvidenceRefs(item.Evidence) {
		evidence = append(evidence, store.FindingEvidenceRef{
			Kind:      "file",
			TaskName:  taskName,
			Path:      ref.Path,
			StartLine: ref.StartLine,
			EndLine:   ref.EndLine,
			Symbol:    derefString(ref.Symbol),
		})
	}

	filePath := ""
	line := 0
	if len(evidence) > 0 {
		filePath = evidence[0].Path
		line = evidence[0].StartLine
	}
	return &store.Finding{
		ID:                            FindingID(fingerprint),
		Namespace:                     namespace,
		RepositoryScan:                repositoryScan,
		ScanRunID:                     scanRunID,
		ScanTaskName:                  taskName,
		SliceID:                       sliceID,
		Fingerprint:                   fingerprint,
		TargetKey:                     FindingV2TargetKey(repo.RepoURL, repo.Branch, repo.SubPath),
		Title:                         item.Title,
		Category:                      item.Category,
		Summary:                       item.Summary,
		Severity:                      item.Severity,
		Confidence:                    item.Confidence,
		Triage:                        item.Triage,
		ValidationStatus:              "unvalidated",
		State:                         "open",
		FilePath:                      filePath,
		Line:                          line,
		CommitSHA:                     repo.HeadSHA,
		RootCause:                     item.RootCause,
		Reproduction:                  item.Reproduction,
		Remediation:                   item.Remediation,
		SuggestedAction:               item.SuggestedAction,
		WhyTestsDoNotAlreadyCoverThis: item.WhyTestsDoNotAlreadyCoverThis,
		SuggestedRegressionTest:       item.SuggestedRegressionTest,
		MinimumFixScope:               item.MinimumFixScope,
		Evidence:                      evidence,
	}
}

func canonicalEvidenceRefs(refs []FindingsV2EvidenceRef) []FindingsV2EvidenceRef {
	out := append([]FindingsV2EvidenceRef{}, refs...)
	sort.Slice(out, func(i, j int) bool {
		return canonicalEvidenceKey(findingV2FingerprintEvidenceRef{
			Path:      out[i].Path,
			StartLine: out[i].StartLine,
			EndLine:   out[i].EndLine,
			Symbol:    out[i].Symbol,
		}) < canonicalEvidenceKey(findingV2FingerprintEvidenceRef{
			Path:      out[j].Path,
			StartLine: out[j].StartLine,
			EndLine:   out[j].EndLine,
			Symbol:    out[j].Symbol,
		})
	})
	return out
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func DroppedFindingSampleJSON(diagnostic DroppedFindingDiagnostic) string {
	if diagnostic.Sample == nil {
		return ""
	}
	data, err := json.Marshal(diagnostic.Sample)
	if err != nil {
		return ""
	}
	return string(data)
}
