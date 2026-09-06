package security

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
)

const (
	ArtifactThreatModel    = "security-threat-model.md"
	ArtifactValidation     = "security-validation.json"
	ArtifactValidationText = "security-validation.txt"
	// ArtifactWorkspaceDir is the repo-root symlink the worker exposes for
	// writing security artifacts from inside the agent workspace.
	ArtifactWorkspaceDir = ".orka-artifacts"
	maxGeneratedTaskName = 63
)

const (
	StageThreatModel = "threat-model"
	StageMapper      = "mapper"
	StageReview      = "review"
	StageValidation  = "validation"
	StagePatch       = "patch"
)

// ValidationArtifact captures the per-finding validator/repro payload.
type ValidationArtifact struct {
	Version            int                            `json:"version"`
	FindingID          string                         `json:"finding_id"`
	Status             string                         `json:"status"`
	Summary            string                         `json:"summary"`
	ValidationSteps    []string                       `json:"validation_steps,omitempty"`
	Reproduction       string                         `json:"reproduction,omitempty"`
	AttackPathAnalysis string                         `json:"attack_path_analysis,omitempty"`
	Likelihood         string                         `json:"likelihood,omitempty"`
	Impact             string                         `json:"impact,omitempty"`
	Assumptions        []string                       `json:"assumptions,omitempty"`
	Controls           []string                       `json:"controls,omitempty"`
	Blindspots         []string                       `json:"blindspots,omitempty"`
	Evidence           ValidationArtifactEvidenceRefs `json:"evidence,omitempty"`
}

// ValidationArtifactEvidenceRefs accepts the structured validation evidence
// array and the existing shorthand forms used by validation agents.
type ValidationArtifactEvidenceRefs []store.FindingEvidenceRef

func (e *ValidationArtifactEvidenceRefs) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	switch {
	case trimmed == "", trimmed == "null":
		*e = nil
		return nil
	case strings.HasPrefix(trimmed, `"`):
		ref, ok, err := validationArtifactEvidenceRefFromJSON(data)
		if err != nil {
			return err
		}
		if !ok {
			*e = nil
			return nil
		}
		*e = ValidationArtifactEvidenceRefs{ref}
		return nil
	case strings.HasPrefix(trimmed, "{"):
		ref, _, err := validationArtifactEvidenceRefFromJSON(data)
		if err != nil {
			return err
		}
		*e = ValidationArtifactEvidenceRefs{ref}
		return nil
	default:
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		refs := make([]store.FindingEvidenceRef, 0, len(items))
		for _, item := range items {
			ref, ok, err := validationArtifactEvidenceRefFromJSON(item)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			refs = append(refs, ref)
		}
		*e = ValidationArtifactEvidenceRefs(refs)
		return nil
	}
}

func validationArtifactEvidenceRefFromJSON(data []byte) (store.FindingEvidenceRef, bool, error) {
	trimmed := strings.TrimSpace(string(data))
	switch {
	case trimmed == "", trimmed == "null":
		return store.FindingEvidenceRef{}, false, nil
	case strings.HasPrefix(trimmed, `"`):
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return store.FindingEvidenceRef{}, false, err
		}
		if strings.TrimSpace(text) == "" {
			return store.FindingEvidenceRef{}, false, nil
		}
		return store.FindingEvidenceRef{Kind: "note", Label: text}, true, nil
	default:
		var ref store.FindingEvidenceRef
		if err := json.Unmarshal(data, &ref); err != nil {
			return store.FindingEvidenceRef{}, false, err
		}
		return ref, true, nil
	}
}

// ParseRepositoryURL extracts owner and repo name from GitHub URLs.
func ParseRepositoryURL(repoURL string) (owner string, repository string) {
	if trimmed, ok := strings.CutPrefix(repoURL, "git@"); ok {
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) == 2 {
			repoPath := strings.TrimSuffix(parts[1], ".git")
			segments := strings.Split(strings.Trim(repoPath, "/"), "/")
			if len(segments) >= 2 {
				return segments[0], segments[1]
			}
		}
		return "", ""
	}

	u, err := url.Parse(repoURL)
	if err != nil {
		return "", ""
	}
	segments := strings.Split(strings.Trim(path.Clean(u.Path), "/"), "/")
	if len(segments) < 2 {
		return "", ""
	}
	return segments[0], strings.TrimSuffix(segments[1], ".git")
}

// ParseGitHubRepositoryURL validates a credential-free GitHub repository URL
// and returns its owner and repository name.
func ParseGitHubRepositoryURL(repoURL string) (owner string, repository string, err error) {
	value := strings.TrimSpace(repoURL)
	if value == "" {
		return "", "", fmt.Errorf("repository URL is required")
	}
	if after, ok := strings.CutPrefix(value, "git@"); ok {
		trimmed := after
		host, repoPath, ok := strings.Cut(trimmed, ":")
		if !ok || !strings.EqualFold(host, "github.com") {
			return "", "", fmt.Errorf("repository URL must be a GitHub repository URL")
		}
		return githubOwnerRepoFromPath(repoPath)
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return "", "", fmt.Errorf("repository URL must be a valid GitHub repository URL")
	}
	if parsed.User != nil {
		return "", "", fmt.Errorf("repository URL must not include credentials")
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "github.com") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("repository URL must be a GitHub repository URL")
	}
	return githubOwnerRepoFromPath(parsed.Path)
}

// CanonicalRepositoryCloneURL returns the canonical credential-free HTTPS
// clone URL for GitHub repository URLs accepted by ParseGitHubRepositoryURL,
// converting SSH roots (git@github.com:owner/repo[.git]) and normalizing HTTPS
// forms to https://github.com/owner/repo. This is the only repository form the
// ACP workspace preflight and the general worker's header-credential clone
// support. Non-GitHub URLs are returned trimmed and unchanged.
func CanonicalRepositoryCloneURL(repoURL string) string {
	trimmed := strings.TrimSpace(repoURL)
	owner, repository, err := ParseGitHubRepositoryURL(trimmed)
	if err != nil {
		return trimmed
	}
	return "https://github.com/" + owner + "/" + repository
}

// CanonicalWorkspaceRepositoryCloneURL canonicalizes a workspace repository
// URL to the only form the controller's workspace preflight accepts: a
// credential-free HTTPS URL without query or fragment. GitHub-style SSH roots
// (git@github.com:owner/repo[.git]) are first converted with
// CanonicalRepositoryCloneURL. The reject conditions mirror the RULE enforced
// by the controller's canonicalWorkspaceRepositoryURL — keep them in exact
// behavior parity (not stricter and not looser) so an accepted URL never
// fails the controller preflight after a Task is created. Empty input is
// allowed and returns an empty URL; error text describes only the failed
// condition so callers can prefix their own field name.
func CanonicalWorkspaceRepositoryCloneURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	canonical := CanonicalRepositoryCloneURL(trimmed)
	parsed, err := url.Parse(canonical)
	if err != nil || parsed.User != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", fmt.Errorf("must be a credential-free HTTPS URL without query or fragment")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return "", fmt.Errorf("must use the default HTTPS port")
	}
	if ip := net.ParseIP(strings.ToLower(parsed.Hostname())); ip != nil &&
		(ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()) {
		return "", fmt.Errorf("uses a forbidden IP literal")
	}
	if parsed.RawPath != "" && parsed.EscapedPath() != parsed.Path {
		return "", fmt.Errorf("has a non-canonical escaped path")
	}
	cleaned := strings.TrimSuffix(strings.TrimPrefix(path.Clean(parsed.Path), "/"), ".git")
	if cleaned == "" || cleaned == "." || parsed.Path == "/" || path.Clean(parsed.Path) != parsed.Path {
		return "", fmt.Errorf("path is invalid")
	}
	return canonical, nil
}

// WorkspaceRepositoryURLIdentity derives the canonical repository identity for
// a clone URL that already passed CanonicalWorkspaceRepositoryCloneURL,
// mirroring the controller's canonicalWorkspaceRepositoryURL derivation:
// lower-cased host plus the cleaned path with any .git suffix removed, with
// the path additionally lower-cased for github.com.
func WorkspaceRepositoryURLIdentity(canonicalURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(canonicalURL))
	if err != nil {
		return "", fmt.Errorf("repository URL is invalid")
	}
	host := strings.ToLower(parsed.Hostname())
	cleaned := strings.TrimSuffix(strings.TrimPrefix(path.Clean(parsed.Path), "/"), ".git")
	if host == "" || cleaned == "" || cleaned == "." {
		return "", fmt.Errorf("repository URL path is invalid")
	}
	if host == "github.com" {
		cleaned = strings.ToLower(cleaned)
	}
	return host + "/" + cleaned, nil
}

// SameWorkspaceRepositoryIdentity mirrors the controller's identity
// comparison: exact match, or case-insensitive match for github.com
// identities.
func SameWorkspaceRepositoryIdentity(first, second string) bool {
	first = strings.TrimSpace(first)
	second = strings.TrimSpace(second)
	if first == second {
		return true
	}
	return strings.HasPrefix(strings.ToLower(first), "github.com/") &&
		strings.HasPrefix(strings.ToLower(second), "github.com/") &&
		strings.EqualFold(first, second)
}

func githubOwnerRepoFromPath(repoPath string) (string, string, error) {
	repoPath = strings.Trim(repoPath, "/")
	segments := strings.Split(strings.TrimSuffix(repoPath, ".git"), "/")
	if len(segments) != 2 || !githubRepositoryPathSegmentIsSafe(segments[0]) || !githubRepositoryPathSegmentIsSafe(segments[1]) {
		return "", "", fmt.Errorf("repository URL must include GitHub owner and repository")
	}
	return segments[0], segments[1], nil
}

func githubRepositoryPathSegmentIsSafe(segment string) bool {
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for _, r := range segment {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	var out strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				out.WriteRune('-')
				lastDash = true
			}
		}
	}
	result := strings.Trim(out.String(), "-")
	if result == "" {
		return "security"
	}
	return result
}

// FindingID returns a stable finding identifier derived from its fingerprint.
func FindingID(fingerprint string) string {
	return "fnd_" + shortHash(fingerprint)
}

// ScanRunID returns a scan run ID derived from task identity.
func ScanRunID(taskName string) string {
	return "scan_" + shortHash(taskName)
}

// PatchProposalID returns a stable patch proposal ID for a task.
func PatchProposalID(taskName string) string {
	return "patch_" + shortHash(taskName)
}

// ScanStageTaskName returns a task name for a specific scan stage and optional scope.
func ScanStageTaskName(repositoryScanName, mode, stage, scope string) string {
	parts := []string{sanitizeName(repositoryScanName), sanitizeName(mode), sanitizeName(stage)}
	if strings.TrimSpace(scope) != "" {
		parts = append(parts, sanitizeName(scope))
	}
	parts = append(parts, fmt.Sprintf("%d", time.Now().Unix()))
	return boundedTaskName(parts...)
}

// ScanStageRetryTaskName returns a deterministic task name for one bounded
// retry of a specific stage within an existing scan run.
func ScanStageRetryTaskName(repositoryScanName, scanRunID, stage, scope string, attempt int) string {
	parts := []string{sanitizeName(repositoryScanName), sanitizeName(stage)}
	if strings.TrimSpace(scope) != "" {
		parts = append(parts, sanitizeName(scope))
	}
	parts = append(parts, sanitizeName(scanRunID), fmt.Sprintf("retry-%d", attempt))
	return boundedTaskName(parts...)
}

// AutoValidationTaskName binds automatic validation to one finding occurrence
// so a delayed retry cannot create another Task after a lost creation response.
func AutoValidationTaskName(repositoryScanName, findingID, scanRunID string) string {
	return boundedTaskName(
		sanitizeName(repositoryScanName),
		"auto-validation",
		sanitizeName(findingID),
		sanitizeName(scanRunID),
	)
}

// PatchTaskName returns a task name for a patch proposal bound to one finding
// occurrence.
func PatchTaskName(repositoryScanName, findingID, scanRunID string) string {
	return boundedTaskName(
		sanitizeName(repositoryScanName),
		"patch",
		sanitizeName(findingID),
		sanitizeName(scanRunID),
		fmt.Sprintf("%d", time.Now().Unix()),
	)
}

// PatchBranch returns the default branch name for a security patch proposal.
func PatchBranch(findingID, taskName string) string {
	return fmt.Sprintf("orka/security/%s-%s", sanitizeName(findingID), shortHash(taskName))
}

func boundedTaskName(parts ...string) string {
	base := strings.Join(parts, "-")
	if len(base) <= maxGeneratedTaskName {
		return base
	}

	visibleParts := parts
	if len(visibleParts) > 3 {
		visibleParts = visibleParts[:3]
	}
	visible := strings.Join(visibleParts, "-")
	hash := shortHash(base)
	maxVisible := max(maxGeneratedTaskName-len(hash)-1, 1)
	if len(visible) > maxVisible {
		visible = strings.Trim(visible[:maxVisible], "-")
		if visible == "" {
			visible = "security"
		}
	}

	return visible + "-" + hash
}

// EffectiveValidationMode returns the configured validation mode or the default.
func EffectiveValidationMode(scan *corev1alpha1.RepositoryScan) string {
	if scan.Spec.ValidationMode != "" {
		return scan.Spec.ValidationMode
	}
	return "light"
}

// EffectiveValidationMaxFindingsPerRun returns the automatic validation cap for light mode.
func EffectiveValidationMaxFindingsPerRun(scan *corev1alpha1.RepositoryScan) int32 {
	if scan.Spec.ValidationMaxFindingsPerRun != nil && *scan.Spec.ValidationMaxFindingsPerRun >= 0 {
		return *scan.Spec.ValidationMaxFindingsPerRun
	}
	return 2
}

// EffectiveValidationMinSeverity returns the minimum severity eligible for automatic validation.
func EffectiveValidationMinSeverity(scan *corev1alpha1.RepositoryScan) string {
	if scan.Spec.ValidationMinSeverity != "" {
		return strings.ToLower(strings.TrimSpace(scan.Spec.ValidationMinSeverity))
	}
	return findingLevelHigh
}

// EffectiveValidationMinConfidence returns the minimum confidence eligible for automatic validation.
func EffectiveValidationMinConfidence(scan *corev1alpha1.RepositoryScan) string {
	if scan.Spec.ValidationMinConfidence != "" {
		return strings.ToLower(strings.TrimSpace(scan.Spec.ValidationMinConfidence))
	}
	return findingLevelHigh
}

func SeverityMeetsMinimum(value, minimum string) bool {
	return severityRank(value) >= severityRank(minimum)
}

func ConfidenceMeetsMinimum(value, minimum string) bool {
	return confidenceRank(value) >= confidenceRank(minimum)
}

func severityRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "critical":
		return 4
	case findingLevelHigh:
		return 3
	case findingLevelMedium:
		return 2
	case findingLevelLow:
		return 1
	default:
		return 0
	}
}

func confidenceRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case findingLevelHigh:
		return 3
	case findingLevelMedium:
		return 2
	case findingLevelLow:
		return 1
	default:
		return 0
	}
}

// EffectiveHistoryDays returns the configured history window or a conservative default.
func EffectiveHistoryDays(scan *corev1alpha1.RepositoryScan) int32 {
	if scan.Spec.HistoryDays != nil && *scan.Spec.HistoryDays > 0 {
		return *scan.Spec.HistoryDays
	}
	return 30
}

// EffectiveMaxFindingsPerRun returns the configured cap or a conservative default.
func EffectiveMaxFindingsPerRun(scan *corev1alpha1.RepositoryScan) int32 {
	if scan.Spec.MaxFindingsPerRun != nil && *scan.Spec.MaxFindingsPerRun > 0 {
		return *scan.Spec.MaxFindingsPerRun
	}
	return 10
}

// EffectiveBranch returns the configured branch or the standard default.
func EffectiveBranch(scan *corev1alpha1.RepositoryScan) string {
	if scan.Spec.Branch != "" {
		return scan.Spec.Branch
	}
	return "main"
}

// EffectiveRef returns the configured checkout ref, if any.
func EffectiveRef(scan *corev1alpha1.RepositoryScan) string {
	return strings.TrimSpace(scan.Spec.Ref)
}

// EffectiveWorkspaceBranch returns the branch to pass to git clone for scan workspaces.
// Ref-only scans must not force the default branch before the worker can check out the ref.
func EffectiveWorkspaceBranch(scan *corev1alpha1.RepositoryScan) string {
	if scan.Spec.Branch != "" {
		return scan.Spec.Branch
	}
	if EffectiveRef(scan) != "" {
		return ""
	}
	return EffectiveBranch(scan)
}

// IsSuspended returns whether scheduled scans are paused.
func IsSuspended(scan *corev1alpha1.RepositoryScan) bool {
	return scan.Spec.Suspend != nil && *scan.Spec.Suspend
}

// ArtifactWorkspacePath returns the relative path from the agent working
// directory to the repo-root artifacts symlink.
func ArtifactWorkspacePath(subPath string) string {
	cleaned := strings.TrimSpace(strings.ReplaceAll(subPath, "\\", "/"))
	cleaned = strings.Trim(cleaned, "/")
	if cleaned == "" {
		return ArtifactWorkspaceDir
	}

	cleaned = path.Clean(cleaned)
	if cleaned == "." || cleaned == "/" || strings.HasPrefix(cleaned, "..") {
		return ArtifactWorkspaceDir
	}

	depth := 0
	for segment := range strings.SplitSeq(cleaned, "/") {
		if segment == "" || segment == "." {
			continue
		}
		depth++
	}
	if depth == 0 {
		return ArtifactWorkspaceDir
	}
	return strings.Repeat("../", depth) + ArtifactWorkspaceDir
}

// BuildThreatModelResultPrompt returns the harness-v2 prompt whose only output
// is a bounded, identity-bound terminal result.
func BuildThreatModelResultPrompt(scan *corev1alpha1.RepositoryScan, mode, baseCommit, headCommit, threatModel string, binding AgentResultBinding, policies ...PromptPolicy) string {
	var prompt strings.Builder
	hasExistingThreatModel := strings.TrimSpace(threatModel) != ""

	fmt.Fprintf(&prompt, "You are generating the canonical repository threat model for %s on branch %s.\n", scan.Spec.RepoURL, EffectiveBranch(scan))
	fmt.Fprintf(&prompt, "Scan mode: %s\n", mode)
	fmt.Fprintf(&prompt, "Validation mode: %s\n", EffectiveValidationMode(scan))
	fmt.Fprintf(&prompt, "History window: %d days\n", EffectiveHistoryDays(scan))
	if scan.Spec.SubPath != "" {
		fmt.Fprintf(&prompt, "Sub-path focus: %s\n", scan.Spec.SubPath)
	}
	if baseCommit != "" || headCommit != "" {
		fmt.Fprintf(&prompt, "Commit focus: base=%s head=%s\n", baseCommit, headCommit)
	}

	prompt.WriteString("\nYour only job in this stage is to understand the repository and produce a strong, reusable threat model.\n")
	prompt.WriteString("Do not create findings in this stage. Do not edit code, commit, or push.\n")
	prompt.WriteString("Ground the model in the actual repository structure, workflows, secrets, auth paths, network boundaries, privileged components, and attack surfaces you can support from the repo.\n")
	prompt.WriteString("\nThreat model requirements for security-threat-model.md:\n")
	prompt.WriteString("- Produce a substantial, engineering-grade markdown document that future finding agents can use as shared context.\n")
	prompt.WriteString("- Include these sections when applicable:\n")
	prompt.WriteString("  1. System Overview and deployment/runtime context\n")
	prompt.WriteString("  2. Key Assets, Trust Boundaries, and sensitive operations\n")
	prompt.WriteString("  3. Attacker-controlled inputs, operator-controlled inputs, and assumptions\n")
	prompt.WriteString("  4. Security-relevant data flows and entry points\n")
	prompt.WriteString("  5. Attack surface and existing mitigations by subsystem/component\n")
	prompt.WriteString("  6. Concrete attacker stories or abuse cases tied to this repository\n")
	prompt.WriteString("  7. Non-applicable or low-relevance vulnerability classes when helpful\n")
	prompt.WriteString("  8. Criticality calibration for what would count as critical, high, medium, and low impact here\n")
	if mode == "incremental" || mode == "manual" {
		prompt.WriteString("- Include a short section on security-relevant change analysis for the commits in scope and explain what changed versus what remains unchanged.\n")
	}
	prompt.WriteString("- Call out important uncertainties explicitly instead of inventing details.\n")
	appendCustomPolicyPrompt(&prompt, firstPromptPolicy(policies))
	if hasExistingThreatModel {
		prompt.WriteString("- Treat the existing threat model as baseline context to refine and extend. Do not replace it with a shorter version unless the repository is genuinely tiny.\n")
	}
	result := ThreatModelResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindThreatModel,
		RepositoryScan: binding.RepositoryScan,
		ScanID:         binding.ScanID,
		PolicyDigest:   binding.PolicyDigest,
		ThreatModel:    "# Threat Model\n\n...",
	}
	resultJSON, _ := json.Marshal(result)
	prompt.WriteString("\nTERMINAL RESULT CONTRACT:\n")
	prompt.WriteString("Do not write artifacts. Return exactly one JSON object and no markdown fence, commentary, or tool transcript.\n")
	prompt.WriteString("Use this exact envelope and identity values; replace only threatModel with the complete markdown document:\n")
	prompt.Write(resultJSON)
	prompt.WriteString("\n")
	if hasExistingThreatModel {
		prompt.WriteString("\nExisting threat model context:\n")
		prompt.WriteString(threatModel)
		prompt.WriteString("\n")
	}
	return prompt.String()
}

// BuildReviewResultPrompt returns the harness-v2 prompt with mapper-owned
// bounded context and an identity-bound findings result contract.
func BuildReviewResultPrompt(
	scan *corev1alpha1.RepositoryScan,
	mode, baseCommit, headCommit, threatModel string,
	slice store.ReviewSlice,
	binding AgentResultBinding,
	manifest ReviewContextManifest,
	repository FindingsV2Repository,
	policies ...PromptPolicy,
) string {
	var prompt strings.Builder
	sliceJSON, err := json.MarshalIndent(slice, "", "  ")
	if err != nil {
		sliceJSON = []byte("{}")
	}

	fmt.Fprintf(&prompt, "You are reviewing one deterministic security slice for %s on branch %s.\n", scan.Spec.RepoURL, EffectiveBranch(scan))
	fmt.Fprintf(&prompt, "Scan mode: %s\n", mode)
	fmt.Fprintf(&prompt, "Slice ID: %s\n", slice.ID)
	fmt.Fprintf(&prompt, "Slice title: %s\n", slice.Title)
	fmt.Fprintf(&prompt, "Slice kind: %s\n", slice.Kind)
	fmt.Fprintf(&prompt, "Max findings for this slice: %d\n", min(EffectiveMaxFindingsPerRun(scan), 3))
	if scan.Spec.SubPath != "" {
		fmt.Fprintf(&prompt, "Sub-path focus: %s\n", scan.Spec.SubPath)
	}
	if baseCommit != "" || headCommit != "" {
		fmt.Fprintf(&prompt, "Commit focus: base=%s head=%s\n", baseCommit, headCommit)
	}

	prompt.WriteString("\nYour job in this stage is to review only the bounded slice below and produce evidence-backed findings.\n")
	prompt.WriteString("Do not rewrite the threat model. Do not edit code, commit, push, or create pull requests.\n")
	prompt.WriteString("Inspect owned files first, then context files and tests. Avoid unrelated repository exploration unless absolutely necessary to understand a cited line.\n")
	prompt.WriteString("Prefer a small number of high-signal findings over broad speculation. If you cannot support a claim from the included slice files, omit it.\n")
	prompt.WriteString("Every finding must cite repo-relative file evidence with startLine and endLine from the files recorded in the review context manifest.\n")
	prompt.WriteString("Quote fields are optional; use them only when you can copy the cited file text exactly.\n")
	prompt.WriteString("\n")
	prompt.WriteString(ScannerFindingQualityPolicy())
	prompt.WriteString("\n")
	if len(slice.ChangedFiles) > 0 || len(slice.ChangedLineRanges) > 0 {
		prompt.WriteString("\n")
		prompt.WriteString(incrementalChangedRiskPolicy())
		prompt.WriteString("\n")
	}
	appendCustomPolicyPrompt(&prompt, firstPromptPolicy(policies))

	result := FindingsResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindFindings,
		RepositoryScan: binding.RepositoryScan,
		ScanID:         binding.ScanID,
		SliceID:        slice.ID,
		PolicyDigest:   binding.PolicyDigest,
		ContextDigest:  binding.ContextDigest,
		Findings: FindingsV2Artifact{
			SchemaVersion: SchemaVersionFindingsV2,
			Repository:    repository,
			Scan:          FindingsV2Scan{Mode: mode, SliceID: slice.ID, Summary: "..."},
			Findings:      []FindingsV2Finding{},
		},
	}
	resultJSON, _ := json.Marshal(result)
	prompt.WriteString("\nTERMINAL RESULT CONTRACT:\n")
	prompt.WriteString("Do not write artifacts or edit the workspace. Return exactly one JSON object and no markdown fence, commentary, or tool transcript.\n")
	prompt.WriteString("Use this exact envelope, repository identity, and binding values. Populate findings.findings; keep it an empty array when no supported finding exists:\n")
	prompt.Write(resultJSON)
	prompt.WriteString("\n")
	prompt.WriteString("\nTRUSTED MAPPER-OWNED REVIEW CONTEXT:\n")
	prompt.WriteString("The context below is the complete evidence boundary. Cite only its included paths and line ranges.\n")
	prompt.WriteString(manifest.Prompt)
	if !strings.HasSuffix(manifest.Prompt, "\n") {
		prompt.WriteString("\n")
	}
	prompt.WriteString("Each finding object must use these keys: title, category, severity, confidence, triage, evidence, summary, rootCause, reproduction, remediation, suggestedAction, whyTestsDoNotAlreadyCoverThis, suggestedRegressionTest, minimumFixScope.\n")
	prompt.WriteString("Use severity exactly one of: critical, high, medium, low. Use confidence exactly one of: high, medium, low.\n")
	prompt.WriteString("Set scan.sliceId exactly to the slice ID above. Even when this slice has zero findings, write valid JSON with an empty findings array.\n")

	prompt.WriteString("\nReview slice metadata:\n")
	prompt.Write(sliceJSON)
	prompt.WriteString("\n")
	if strings.TrimSpace(threatModel) != "" {
		prompt.WriteString("\nShared threat model context:\n")
		prompt.WriteString(threatModel)
		prompt.WriteString("\n")
	}
	return prompt.String()
}

// BuildValidationResultPrompt returns the harness-v2 validation prompt whose
// terminal result is bound to the finding and scanner policy.
func BuildValidationResultPrompt(scan *corev1alpha1.RepositoryScan, finding *store.Finding, binding AgentResultBinding, policies ...PromptPolicy) string {
	var prompt strings.Builder

	fmt.Fprintf(&prompt, "You are validating and, when safe, attempting to reproduce a single security finding for %s on branch %s.\n", scan.Spec.RepoURL, EffectiveBranch(scan))
	fmt.Fprintf(&prompt, "Finding ID: %s\n", finding.ID)
	fmt.Fprintf(&prompt, "Title: %s\n", finding.Title)
	fmt.Fprintf(&prompt, "Severity: %s\n", finding.Severity)
	fmt.Fprintf(&prompt, "Confidence: %s\n", finding.Confidence)
	if finding.FilePath != "" {
		fmt.Fprintf(&prompt, "Primary location: %s:%d\n", finding.FilePath, finding.Line)
	}
	if finding.CommitSHA != "" {
		fmt.Fprintf(&prompt, "Commit: %s\n", finding.CommitSHA)
	}
	if finding.RootCause != "" {
		fmt.Fprintf(&prompt, "Root cause hypothesis: %s\n", finding.RootCause)
	}
	if finding.Remediation != "" {
		fmt.Fprintf(&prompt, "Suggested remediation: %s\n", finding.Remediation)
	}
	prompt.WriteString("\nRequirements:\n")
	prompt.WriteString("1. Validate only this finding. Do not look for unrelated vulnerabilities.\n")
	prompt.WriteString("2. Prefer safe, focused reproduction steps. Do not perform destructive actions.\n")
	prompt.WriteString("3. Tighten or lower confidence when the code or environment does not support the original claim.\n")
	prompt.WriteString("4. Capture a concrete attack-path analysis for how the issue could be exploited, what assumptions it depends on, and which controls already limit it.\n")
	prompt.WriteString("5. Fail the finding when it is theoretical, stale, docs-only, test-only, client-only, or lacks an attacker-controlled path to a sensitive sink.\n")
	prompt.WriteString("6. Do not edit code, commit, or push during validation.\n")
	prompt.WriteString("\n")
	prompt.WriteString(ScannerValidationQualityPolicy())
	prompt.WriteString("\n")
	appendCustomPolicyPrompt(&prompt, firstPromptPolicy(policies))

	result := ValidationResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindValidation,
		RepositoryScan: binding.RepositoryScan,
		ScanID:         binding.ScanID,
		FindingID:      finding.ID,
		PolicyDigest:   binding.PolicyDigest,
		Validation: ValidationArtifact{
			Version:   1,
			FindingID: finding.ID,
			Status:    "validated",
			Summary:   "...",
		},
	}
	resultJSON, _ := json.Marshal(result)
	prompt.WriteString("\nTERMINAL RESULT CONTRACT:\n")
	prompt.WriteString("Do not write artifacts or edit the workspace. Return exactly one JSON object and no markdown fence, commentary, or tool transcript.\n")
	prompt.WriteString("Use this exact envelope and binding values. Complete validation and use only file ranges already present in the accepted finding evidence boundary:\n")
	prompt.Write(resultJSON)
	prompt.WriteString("\n")
	evidenceJSON, _ := json.Marshal(finding.Evidence)
	prompt.WriteString("Accepted finding evidence boundary:\n")
	prompt.Write(evidenceJSON)
	prompt.WriteString("\n")
	prompt.WriteString("Use status=validated when the code path and validation strongly support the issue.\n")
	prompt.WriteString("Use status=failed when the original claim does not hold after review or reproduction attempts.\n")
	prompt.WriteString("Use status=skipped when the environment or safety constraints prevent meaningful validation.\n")
	prompt.WriteString("If you create additional evidence or transcript artifacts, reference them in the evidence array.\n")
	return prompt.String()
}

// BuildPatchPrompt returns the harness-v2 prompt for patch proposal tasks:
// the agent edits the workspace and returns an identity-bound patch result
// envelope; Orka publishes the delta and derives the reviewable diff from the
// published commit.
func BuildPatchPrompt(scan *corev1alpha1.RepositoryScan, finding *store.Finding, patchBranch string) string {
	var prompt strings.Builder
	fmt.Fprintf(&prompt, "Generate a minimal security patch for repository %s on branch %s.\n", scan.Spec.RepoURL, EffectiveBranch(scan))
	if strings.TrimSpace(patchBranch) != "" {
		fmt.Fprintf(&prompt, "Orka will push the final diff to patch branch %s after the task finishes.\n", patchBranch)
	}
	fmt.Fprintf(&prompt, "Finding ID: %s\nTitle: %s\nSeverity: %s\nConfidence: %s\n", finding.ID, finding.Title, finding.Severity, finding.Confidence)
	if finding.FilePath != "" {
		fmt.Fprintf(&prompt, "Primary file: %s:%d\n", finding.FilePath, finding.Line)
	}
	if finding.RootCause != "" {
		fmt.Fprintf(&prompt, "Root cause: %s\n", finding.RootCause)
	}
	if finding.Remediation != "" {
		fmt.Fprintf(&prompt, "Remediation guidance: %s\n", finding.Remediation)
	}
	prompt.WriteString("\nRequirements:\n")
	prompt.WriteString("1. Fix only this finding.\n")
	prompt.WriteString("2. Apply the fix directly to the checked-out workspace files. Do not stop at a diff artifact or a written description.\n")
	prompt.WriteString("3. Keep the code diff as small and reviewable as possible.\n")
	prompt.WriteString("4. Preserve existing behavior unless the vulnerability requires a behavior change.\n")
	prompt.WriteString("5. Run focused tests when available.\n")
	prompt.WriteString("6. The changedFiles you report in the terminal result below must exactly match the workspace files you actually edited.\n")
	prompt.WriteString("7. Do not commit, push, or open a pull request directly. Leave the final file changes in the workspace so Orka can create the commit and push it to the patch branch automatically.\n")
	prompt.WriteString("8. Change only source files that belong to the fix. Do not create diff, summary, or metadata files anywhere in the workspace (no .orka-artifacts directory): every workspace change becomes part of the published commit, and unexpected files fail the proposal.\n")
	result := PatchResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindPatch,
		RepositoryScan: scan.Name,
		FindingID:      finding.ID,
		Summary:        "...",
		ChangedFiles:   []string{"path/to/changed-file"},
		TestsRun:       []PatchTestRun{{Command: "go test ./...", ExitCode: 0}},
		Risk:           "low|medium|high",
	}
	resultJSON, _ := json.Marshal(result)
	prompt.WriteString("\nTERMINAL RESULT CONTRACT:\n")
	prompt.WriteString("Do not write artifacts. Return exactly one JSON object and no markdown fence, commentary, or tool transcript.\n")
	prompt.WriteString("Use this exact envelope and identity values; replace only summary, changedFiles, testsRun, and risk:\n")
	prompt.Write(resultJSON)
	prompt.WriteString("\n")
	prompt.WriteString("changedFiles must list every file you changed, as repository-root-relative paths, and must exactly match the files in the published commit; Orka derives the reviewable diff from that commit, and a mismatch fails the proposal.\n")
	return prompt.String()
}

const (
	remediationPullRequestTitlePrefix = "fix(security): "
	maxRemediationTitleBytes          = 120
	maxRemediationBodySectionBytes    = 4 << 10
)

// stripUnsafeTextRunes removes invalid UTF-8 and control or format runes that
// could hide content when agent-produced text is validated or persisted.
func stripUnsafeTextRunes(value string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(value, ""))
}

// neutralizePublishedText prepares agent-produced text for publication in
// GitHub Markdown (remediation PR titles and bodies): invalid UTF-8 and
// control/format runes are stripped so nothing invisible survives, credential
// shapes are redacted, and active constructs are defanged — @mentions and
// HTML comment markers gain a zero-width break, and lines that would read as
// slash-commands are prefixed with one — so a repository cannot prompt-inject
// pings or bot commands through a scanner result.
func neutralizePublishedText(value string) string {
	stripped := stripUnsafeTextRunes(value)
	stripped = redact.SensitiveText(stripped)
	stripped = strings.ReplaceAll(stripped, "<!--", "<\u200b!--")
	stripped = strings.ReplaceAll(stripped, "-->", "--\u200b>")
	var b strings.Builder
	b.Grow(len(stripped))
	for i := 0; i < len(stripped); i++ {
		ch := stripped[i]
		b.WriteByte(ch)
		if ch == '@' && i+1 < len(stripped) {
			next := stripped[i+1]
			if (next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') {
				b.WriteRune('\u200b')
			}
		}
	}
	lines := strings.Split(b.String(), "\n")
	for i, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		if strings.HasPrefix(trimmed, "/") {
			indent := line[:len(line)-len(trimmed)]
			lines[i] = indent + "\u200b" + trimmed
		}
	}
	return strings.Join(lines, "\n")
}

// truncateOnRuneBoundary bounds text to at most limit bytes without splitting
// a multibyte character; a byte-index cut would leave invalid UTF-8 that
// json.Marshal replaces with U+FFFD.
func truncateOnRuneBoundary(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return strings.TrimSpace(text[:cut])
}

// RemediationPullRequestTitle renders the reviewer-facing title of a
// remediation pull request from the finding.
func RemediationPullRequestTitle(finding *store.Finding) string {
	title := ""
	if finding != nil {
		title = strings.Join(strings.Fields(neutralizePublishedText(finding.Title)), " ")
	}
	if title == "" {
		title = "security remediation"
	}
	title = truncateOnRuneBoundary(title, maxRemediationTitleBytes)
	return remediationPullRequestTitlePrefix + title
}

// RemediationPullRequestBody renders the reviewer-facing body of a remediation
// pull request: the finding's summary, root cause, and remediation guidance,
// plus the patch agent's own account of the change. The caller appends the
// publisher's intent marker so the clean-room publisher still recognizes the
// pull request as its own.
func RemediationPullRequestBody(finding *store.Finding, summary *PatchSummaryArtifact) string {
	var body strings.Builder
	section := func(heading, text string) {
		text = strings.TrimSpace(neutralizePublishedText(text))
		if text == "" {
			return
		}
		if len(text) > maxRemediationBodySectionBytes {
			text = truncateOnRuneBoundary(text, maxRemediationBodySectionBytes) + "…"
		}
		body.WriteString(heading)
		body.WriteString("\n")
		body.WriteString(text)
		body.WriteString("\n\n")
	}
	if finding != nil {
		fmt.Fprintf(&body, "Security remediation for finding `%s`", finding.ID)
		if finding.Severity != "" {
			fmt.Fprintf(&body, " (%s severity", finding.Severity)
			if finding.Confidence != "" {
				fmt.Fprintf(&body, ", %s confidence", finding.Confidence)
			}
			body.WriteString(")")
		}
		body.WriteString(".\n\n")
		if finding.FilePath != "" {
			location := finding.FilePath
			if finding.Line > 0 {
				location = fmt.Sprintf("%s:%d", finding.FilePath, finding.Line)
			}
			section("**Location:**", "`"+location+"`")
		}
		section("**Summary:**", finding.Summary)
		section("**Root cause:**", finding.RootCause)
		section("**Remediation guidance:**", finding.Remediation)
	}
	if summary != nil {
		section("**Patch:**", summary.Summary)
		if len(summary.ChangedFiles) > 0 {
			var files strings.Builder
			for _, file := range summary.ChangedFiles {
				files.WriteString("- `")
				files.WriteString(file)
				files.WriteString("`\n")
			}
			section("**Changed files:**", files.String())
		}
		if len(summary.TestsRun) > 0 {
			var tests strings.Builder
			for _, test := range summary.TestsRun {
				fmt.Fprintf(&tests, "- `%s` (exit %d)\n", test.Command, test.ExitCode)
			}
			section("**Tests run:**", tests.String())
		}
		if summary.Risk != "" {
			section("**Risk:**", summary.Risk)
		}
	}
	body.WriteString("Generated by Orka repository security scanning; the branch was published by the clean-room workspace publisher.")
	return body.String()
}
