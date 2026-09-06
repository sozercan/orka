package security

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

func TestArtifactWorkspacePath(t *testing.T) {
	tests := []struct {
		name    string
		subPath string
		want    string
	}{
		{name: "root", subPath: "", want: ArtifactWorkspaceDir},
		{name: "single level", subPath: "services", want: "../" + ArtifactWorkspaceDir},
		{name: "nested", subPath: "services/api", want: "../../" + ArtifactWorkspaceDir},
		{name: "normalizes slashes", subPath: "/services/api/", want: "../../" + ArtifactWorkspaceDir},
		{name: "ignores traversal", subPath: "../services", want: ArtifactWorkspaceDir},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ArtifactWorkspacePath(tt.subPath); got != tt.want {
				t.Fatalf("ArtifactWorkspacePath(%q) = %q, want %q", tt.subPath, got, tt.want)
			}
		})
	}
}

func TestParseGitHubRepositoryURL(t *testing.T) {
	tests := []struct {
		name      string
		repoURL   string
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		{
			name:      "HTTPS URL",
			repoURL:   "https://github.com/example/project",
			wantOwner: "example",
			wantRepo:  "project",
		},
		{
			name:      "HTTPS URL with git suffix and trailing slash",
			repoURL:   "https://github.com/example/project.git/",
			wantOwner: "example",
			wantRepo:  "project",
		},
		{
			name:      "SSH URL",
			repoURL:   "git@github.com:example/project.git",
			wantOwner: "example",
			wantRepo:  "project",
		},
		{
			name:    "rejects credentials",
			repoURL: "https://token@github.com/example/project",
			wantErr: true,
		},
		{
			name:    "rejects SSH URL query",
			repoURL: "git@github.com:example/project?token=secret",
			wantErr: true,
		},
		{
			name:    "rejects SSH URL credential-like repo",
			repoURL: "git@github.com:example/project@secret",
			wantErr: true,
		},
		{
			name:    "rejects non GitHub host",
			repoURL: "https://example.com/example/project",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, err := ParseGitHubRepositoryURL(tt.repoURL)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseGitHubRepositoryURL(%q) succeeded, want error", tt.repoURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseGitHubRepositoryURL(%q) error = %v", tt.repoURL, err)
			}
			if owner != tt.wantOwner || repo != tt.wantRepo {
				t.Fatalf("ParseGitHubRepositoryURL(%q) = %q/%q, want %q/%q", tt.repoURL, owner, repo, tt.wantOwner, tt.wantRepo)
			}
		})
	}
}

func TestCanonicalRepositoryCloneURL(t *testing.T) {
	tests := []struct {
		name    string
		repoURL string
		want    string
	}{
		{
			name:    "SSH GitHub root converted to HTTPS",
			repoURL: "git@github.com:example/project.git",
			want:    "https://github.com/example/project",
		},
		{
			name:    "SSH GitHub root without git suffix",
			repoURL: "git@github.com:example/project",
			want:    "https://github.com/example/project",
		},
		{
			name:    "HTTPS GitHub URL normalized",
			repoURL: " https://github.com/example/project.git ",
			want:    "https://github.com/example/project",
		},
		{
			name:    "canonical HTTPS GitHub URL unchanged",
			repoURL: "https://github.com/example/project",
			want:    "https://github.com/example/project",
		},
		{
			name:    "non-GitHub HTTPS URL passes through",
			repoURL: "https://git.example.com/example/project.git",
			want:    "https://git.example.com/example/project.git",
		},
		{
			name:    "credentialed URL passes through for downstream rejection",
			repoURL: "https://token@github.com/example/project",
			want:    "https://token@github.com/example/project",
		},
		{
			name:    "empty URL stays empty",
			repoURL: "  ",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanonicalRepositoryCloneURL(tt.repoURL); got != tt.want {
				t.Fatalf("CanonicalRepositoryCloneURL(%q) = %q, want %q", tt.repoURL, got, tt.want)
			}
		})
	}
}

func TestCanonicalWorkspaceRepositoryCloneURL(t *testing.T) {
	tests := []struct {
		name    string
		repoURL string
		want    string
		wantErr string
	}{
		{name: "empty URL is allowed", repoURL: "  ", want: ""},
		{name: "GitHub SSH root canonicalized", repoURL: "git@github.com:example/project.git", want: "https://github.com/example/project"},
		{name: "non-GitHub HTTPS URL accepted", repoURL: "https://git.example.com/example/project.git", want: "https://git.example.com/example/project.git"},
		{name: "plain HTTP rejected", repoURL: "http://github.com/example/project", wantErr: "credential-free HTTPS URL"},
		{name: "credentialed URL rejected", repoURL: "https://user:token@git.example.com/example/project", wantErr: "credential-free HTTPS URL"},
		{name: "non-GitHub SSH rejected", repoURL: "git@git.example.com:example/project.git", wantErr: "credential-free HTTPS URL"},
		{name: "query rejected", repoURL: "https://git.example.com/example/project?x=1", wantErr: "credential-free HTTPS URL"},
		{name: "non-default port rejected", repoURL: "https://git.example.com:8443/example/project", wantErr: "default HTTPS port"},
		{name: "escaped path separator rejected", repoURL: "https://git.example.com/example%2Fproject", wantErr: "non-canonical escaped path"},
		{name: "trailing slash rejected", repoURL: "https://git.example.com/example/project/", wantErr: "path is invalid"},
		{name: "empty path rejected", repoURL: "https://git.example.com/", wantErr: "path is invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicalWorkspaceRepositoryCloneURL(tt.repoURL)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("CanonicalWorkspaceRepositoryCloneURL(%q) error = %v, want %q", tt.repoURL, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalWorkspaceRepositoryCloneURL(%q) error = %v", tt.repoURL, err)
			}
			if got != tt.want {
				t.Fatalf("CanonicalWorkspaceRepositoryCloneURL(%q) = %q, want %q", tt.repoURL, got, tt.want)
			}
		})
	}
}

func TestEffectiveWorkspaceBranch(t *testing.T) {
	tests := []struct {
		name string
		spec corev1alpha1.RepositoryScanSpec
		want string
	}{
		{
			name: "explicit branch wins",
			spec: corev1alpha1.RepositoryScanSpec{Branch: "release", Ref: "v1.2.3"},
			want: "release",
		},
		{
			name: "ref only omits implicit branch",
			spec: corev1alpha1.RepositoryScanSpec{Ref: "v1.2.3"},
			want: "",
		},
		{
			name: "default branch without ref",
			spec: corev1alpha1.RepositoryScanSpec{},
			want: "main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := &corev1alpha1.RepositoryScan{Spec: tt.spec}
			if got := EffectiveWorkspaceBranch(scan); got != tt.want {
				t.Fatalf("EffectiveWorkspaceBranch() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildThreatModelResultPromptRequiresThreatModelOnly(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/project",
			Branch:  "main",
		},
	}

	got := BuildThreatModelResultPrompt(scan, "manual", "", "", "# Existing", testResultBinding())
	if !strings.Contains(got, "Your only job in this stage is to understand the repository and produce a strong, reusable threat model.") {
		t.Fatalf("BuildThreatModelResultPrompt() missing stage instruction:\n%s", got)
	}
	if !strings.Contains(got, "TERMINAL RESULT CONTRACT") {
		t.Fatalf("BuildThreatModelResultPrompt() missing terminal result contract:\n%s", got)
	}
	if !strings.Contains(got, "Existing threat model context") {
		t.Fatalf("BuildThreatModelResultPrompt() missing existing threat model context:\n%s", got)
	}
	if strings.Contains(got, "security-findings") {
		t.Fatalf("BuildThreatModelResultPrompt() unexpectedly references findings artifact:\n%s", got)
	}
}

func testResultBinding() AgentResultBinding {
	return AgentResultBinding{
		RepositoryScan: "project",
		ScanID:         "scan_123",
		PolicyDigest:   "sha256:policy",
		ContextDigest:  "sha256:context",
	}
}

func testReviewContextManifest(sliceID string) ReviewContextManifest {
	prompt := "bounded context\n"
	return ReviewContextManifest{
		SchemaVersion:     SchemaVersionReviewContext,
		SliceID:           sliceID,
		Prompt:            prompt,
		PromptBytes:       len(prompt),
		ApproximateTokens: 4,
	}
}

func TestBuildValidationResultPromptIncludesAttackPathAnalysis(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/project",
			Branch:  "main",
		},
	}

	finding := &store.Finding{
		ID:         "fnd_123",
		Title:      "Command injection",
		Severity:   "high",
		Confidence: "high",
		FilePath:   "cmd/run.go",
		Line:       42,
	}

	got := BuildValidationResultPrompt(scan, finding, testResultBinding())
	if !strings.Contains(got, "TERMINAL RESULT CONTRACT") {
		t.Fatalf("BuildValidationResultPrompt() missing terminal result contract:\n%s", got)
	}
	if !strings.Contains(got, "attack-path analysis") {
		t.Fatalf("BuildValidationResultPrompt() missing attack path requirement:\n%s", got)
	}
}

func TestBuildReviewPromptIncludesFindingQualityPolicy(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	slice := store.ReviewSlice{ID: "slice_api", Title: "API", Kind: "package", RepositoryScan: "repo", Source: "mapper"}
	got := BuildReviewResultPrompt(scan, "initial", "", "", "", slice, testResultBinding(), testReviewContextManifest(slice.ID), FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"})
	for _, want := range []string{"FINDING QUALITY POLICY", "attacker-controlled source", "trust boundary", "docs-only", "dependency version", "React/TSX XSS", "shell-script command injection"} {
		if !strings.Contains(got, want) {
			t.Fatalf("BuildReviewResultPrompt() missing %q:\n%s", want, got)
		}
	}
}

func TestBuildReviewPromptIncludesOrkaSpecificThreatCategories(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	slice := store.ReviewSlice{
		ID: "slice_api", Title: "API", Kind: "package", RepositoryScan: "repo", Source: "mapper",
		ChangedFiles: []string{"internal/api/security.go"},
	}
	got := BuildReviewResultPrompt(scan, "manual", "base", "head", "", slice, testResultBinding(), testReviewContextManifest(slice.ID), FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"})
	for _, want := range []string{"Kubernetes RBAC", "task and pod execution isolation", "workspace write boundaries", "artifact and result ingestion", "Git credentials", "context-token and TxToken", "AI-agent prompt", "tenant and namespace isolation", "raw token or transcript persistence", "INCREMENTAL/MANUAL CHANGE-FOCUS POLICY"} {
		if !strings.Contains(got, want) {
			t.Fatalf("BuildReviewResultPrompt() missing %q:\n%s", want, got)
		}
	}
}

func TestBuildReviewResultPromptPreservesFindingsV2Contract(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	slice := store.ReviewSlice{ID: "slice_api", Title: "API", Kind: "package", RepositoryScan: "repo", Source: "mapper"}
	got := BuildReviewResultPrompt(scan, "initial", "", "", "", slice, testResultBinding(), testReviewContextManifest(slice.ID), FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"})
	if !strings.Contains(got, `"findings":[]`) || !strings.Contains(got, "empty array when no supported finding exists") {
		t.Fatalf("BuildReviewResultPrompt() missing v2 findings contract:\n%s", got)
	}
}

func TestBuildValidationResultPromptIncludesFindingQualityPolicy(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	finding := &store.Finding{ID: "fnd_1", Title: "Finding", Severity: "high", Confidence: "high"}
	got := BuildValidationResultPrompt(scan, finding, testResultBinding())
	for _, want := range []string{"FINDING QUALITY POLICY", "theoretical, stale, docs-only, test-only, client-only", "status=failed"} {
		if !strings.Contains(got, want) {
			t.Fatalf("BuildValidationResultPrompt() missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "write security-findings.v2.json") {
		t.Fatalf("BuildValidationResultPrompt() contains review-stage findings artifact instruction:\n%s", got)
	}
}

func TestBuildReviewPromptIncludesCustomPolicyButPreservesDefaultPolicy(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	slice := store.ReviewSlice{ID: "slice_api", Title: "API", Kind: "package", RepositoryScan: "repo", Source: "mapper"}
	policy := PromptPolicy{
		CustomScanInstructions: "Treat webhook signature bypasses as critical for this repository.",
		FalsePositivePolicy:    "Suppress findings about intentionally public demo endpoints.",
		PolicyDigest:           "sha256:test",
		CustomScanSource:       "scan-policy/policy (sha256:scan)",
		FalsePositiveSource:    "fp-policy/policy (sha256:fp)",
	}
	got := BuildReviewResultPrompt(scan, "initial", "", "", "", slice, testResultBinding(), testReviewContextManifest(slice.ID), FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"}, policy)
	for _, want := range []string{"CONFIGMAP-BACKED SCANNER POLICY", "webhook signature bypasses", "public demo endpoints", "Default Orka security policy", "prompt/tool-injection handling remain mandatory"} {
		if !strings.Contains(got, want) {
			t.Fatalf("BuildReviewResultPrompt() missing %q:\n%s", want, got)
		}
	}
}

func TestBuildThreatModelResultPromptIncludesCustomPolicy(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{Spec: corev1alpha1.RepositoryScanSpec{RepoURL: "https://github.com/example/project", Branch: "main"}}
	got := BuildThreatModelResultPrompt(scan, "initial", "", "", "", testResultBinding(), PromptPolicy{CustomScanInstructions: "Focus on operator RBAC drift."})
	if !strings.Contains(got, "Focus on operator RBAC drift") || !strings.Contains(got, "Default Orka security policy") {
		t.Fatalf("BuildThreatModelResultPrompt() missing custom policy:\n%s", got)
	}
}

func TestValidateCustomPolicyTextRejectsOversizedAndSecret(t *testing.T) {
	if err := ValidateCustomPolicyText(strings.Repeat("a", MaxCustomPolicyBytes+1)); err == nil {
		t.Fatal("ValidateCustomPolicyText() accepted oversized policy")
	}
	if err := ValidateCustomPolicyText("token " + "g" + "hp_" + strings.Repeat("x", 32)); err == nil {
		t.Fatal("ValidateCustomPolicyText() accepted secret-like policy")
	}
	assignment := "to" + "ken" + "=" + strings.Repeat("x", 32)
	if err := ValidateCustomPolicyText("Never send " + assignment + " to scanners."); err == nil {
		t.Fatal("ValidateCustomPolicyText() accepted generic token assignment")
	}
	jwt := "ey" + "JhbGciOiJIUzI1NiJ9." + strings.Repeat("a", 16) + "." + strings.Repeat("b", 16)
	if err := ValidateCustomPolicyText("Do not include " + jwt + " in policies."); err == nil {
		t.Fatal("ValidateCustomPolicyText() accepted JWT-like policy")
	}
	if err := ValidateCustomPolicyText("Use risk-sk-score as a false-positive category name."); err != nil {
		t.Fatalf("ValidateCustomPolicyText() rejected benign sk substring: %v", err)
	}
	if err := ValidateCustomPolicyText("Prefer token-based reasoning about auth boundaries, without embedding credentials."); err != nil {
		t.Fatalf("ValidateCustomPolicyText() rejected benign token wording: %v", err)
	}
}

func TestValidationArtifactEvidenceRefsUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []store.FindingEvidenceRef
	}{
		{
			name: "string shorthand",
			raw:  `"validation transcript"`,
			want: []store.FindingEvidenceRef{{Kind: "note", Label: "validation transcript"}},
		},
		{
			name: "object shorthand",
			raw:  `{"kind":"artifact","name":"security-validation.txt","label":"trace"}`,
			want: []store.FindingEvidenceRef{{Kind: "artifact", Name: "security-validation.txt", Label: "trace"}},
		},
		{
			name: "mixed array",
			raw:  `["validation transcript",{"kind":"artifact","name":"security-validation.txt"},null,"  "]`,
			want: []store.FindingEvidenceRef{
				{Kind: "note", Label: "validation transcript"},
				{Kind: "artifact", Name: "security-validation.txt"},
			},
		},
		{
			name: "null",
			raw:  `null`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ValidationArtifactEvidenceRefs
			if err := json.Unmarshal([]byte(tt.raw), &got); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len(got) = %d, want %d: %#v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got[%d] = %#v, want %#v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestBuildPatchPromptRequiresWorkspaceEditAndManagedPush(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{
		Spec: corev1alpha1.RepositoryScanSpec{
			RepoURL: "https://github.com/example/project",
			Branch:  "main",
		},
	}

	finding := &store.Finding{
		ID:         "fnd_123",
		Title:      "Command injection",
		Severity:   "critical",
		Confidence: "high",
	}

	got := BuildPatchPrompt(scan, finding, "orka/security/fnd-123")
	if !strings.Contains(got, "patch branch orka/security/fnd-123") {
		t.Fatalf("BuildPatchPrompt() missing patch branch guidance:\n%s", got)
	}
	if !strings.Contains(got, "Apply the fix directly to the checked-out workspace files.") {
		t.Fatalf("BuildPatchPrompt() missing workspace-edit directive:\n%s", got)
	}
	if !strings.Contains(got, "Do not commit, push, or open a pull request directly.") {
		t.Fatalf("BuildPatchPrompt() missing no-manual-push instruction:\n%s", got)
	}
	if !strings.Contains(got, "Orka can create the commit and push it to the patch branch automatically.") {
		t.Fatalf("BuildPatchPrompt() missing Orka-managed push instruction:\n%s", got)
	}
	if strings.Contains(got, "REQUIRED_SECURITY_ARTIFACTS") || strings.Contains(got, ".orka-artifacts/") {
		t.Fatalf("BuildPatchPrompt() still asks for workspace artifact files, which poison the harness-v2 delta:\n%s", got)
	}
	if !strings.Contains(got, "no .orka-artifacts directory") {
		t.Fatalf("BuildPatchPrompt() missing the no-artifact-files requirement:\n%s", got)
	}
	if !strings.Contains(got, "TERMINAL RESULT CONTRACT:") ||
		!strings.Contains(got, `"kind":"orka.security.patch.v1","repositoryScan":"","findingId":"fnd_123"`) {
		t.Fatalf("BuildPatchPrompt() missing the identity-bound patch result envelope:\n%s", got)
	}
	if !strings.Contains(got, "must exactly match the files in the published commit") {
		t.Fatalf("BuildPatchPrompt() missing changedFiles verification guidance:\n%s", got)
	}
}

func TestGeneratedSecurityTaskNamesStayLabelSafe(t *testing.T) {
	scanName := "demo-security-repository-security1-1776034262"

	names := []string{
		ScanStageTaskName(scanName, "initial", "threat-model", ""),
		ScanStageTaskName(scanName, "initial", "discovery", "ci-cd-supply-chain"),
		ScanStageTaskName(scanName, "initial", "discovery", "ci-cd-supply-chain-4"),
		ScanStageRetryTaskName(scanName, "scan_1234567890abcdef", StageReview, "ci-cd-supply-chain", 1),
		AutoValidationTaskName(scanName, "fnd_1234567890abcdef", "scan_1234567890abcdef"),
		PatchTaskName(scanName, "fnd_1234567890abcdef", "scan_1234567890abcdef"),
	}

	for _, name := range names {
		if len(name) > 63 {
			t.Fatalf("generated task name %q has length %d, want <= 63", name, len(name))
		}
		if strings.Contains(name, "--") {
			t.Fatalf("generated task name %q should not contain duplicate separators", name)
		}
	}
}

func TestPatchTaskNameSeparatesFindingOccurrences(t *testing.T) {
	first := PatchTaskName("demo-security-repository", "fnd_1234567890abcdef", "scan_first")
	second := PatchTaskName("demo-security-repository", "fnd_1234567890abcdef", "scan_second")

	if first == second {
		t.Fatalf("PatchTaskName() reused %q across finding occurrences", first)
	}
	if PatchProposalID(first) == PatchProposalID(second) {
		t.Fatal("PatchProposalID() reused an ID across finding occurrences")
	}
}

func TestScanStageRetryTaskNameIsDeterministicAndAttemptBound(t *testing.T) {
	first := ScanStageRetryTaskName("demo-security-repository", "scan_1234567890abcdef", StageReview, "slice_api", 1)
	repeated := ScanStageRetryTaskName("demo-security-repository", "scan_1234567890abcdef", StageReview, "slice_api", 1)
	secondAttempt := ScanStageRetryTaskName("demo-security-repository", "scan_1234567890abcdef", StageReview, "slice_api", 2)

	if first != repeated {
		t.Fatalf("ScanStageRetryTaskName() = %q then %q, want deterministic", first, repeated)
	}
	if first == secondAttempt {
		t.Fatalf("ScanStageRetryTaskName() = %q for attempts 1 and 2, want attempt-bound names", first)
	}
	if len(first) > maxGeneratedTaskName || len(secondAttempt) > maxGeneratedTaskName {
		t.Fatalf("retry task names lengths = %d/%d, want <= %d", len(first), len(secondAttempt), maxGeneratedTaskName)
	}
}

func TestPatchBranchUsesUniqueTaskHash(t *testing.T) {
	branchA := PatchBranch("fnd_1234567890abcdef", "demo-security-repository-patch-a")
	branchB := PatchBranch("fnd_1234567890abcdef", "demo-security-repository-patch-b")

	if !strings.HasPrefix(branchA, "orka/security/fnd-1234567890abcdef-") {
		t.Fatalf("PatchBranch() = %q, want finding prefix preserved", branchA)
	}
	if branchA == branchB {
		t.Fatalf("PatchBranch() should vary by task name, got identical branches %q", branchA)
	}
}

func TestLoadScannerPolicyRequiresPolicyConfigMapOptInLabel(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: "default"}, Data: map[string]string{"policy": "custom policy"}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	_, err := LoadScannerPolicy(context.Background(), reader, "default", corev1alpha1.RepositoryScanSpec{CustomScanInstructionsRef: &corev1alpha1.PolicyConfigMapKeyRef{Name: "policy"}})
	if err == nil || !strings.Contains(err.Error(), PolicyConfigMapAllowedLabel) {
		t.Fatalf("LoadScannerPolicy() error = %v, want opt-in label error", err)
	}
}

// looksLikeSecretNegatives are placeholders, bare keywords, and code that
// reads credentials from configuration; none is a secret.
var looksLikeSecretNegatives = []string{
	"env OPENAI_API_KEY=dummy ANTHROPIC_API_KEY=dummy vekil",
	"curl -H 'Authorization: Bearer $VEKIL_TOKEN' http://host.docker.internal:1337/v1/models",
	"Authorization: Bearer <your-token>",
	"Authorization: Basic $BASE64_CREDS",
	"Authorization: Bearer {{ .Token }}",
	"password=changeme",
	"api_key=${OPENAI_API_KEY}",
	"The token is validated by the proxy; set TOKEN=xxxx in .env",
	"Txn-Token: <transaction token>",
	"runtime.apiKey = strings.TrimSpace(cfg.APIKey)",
	"apiKey = strings.TrimSpace(os.Getenv(apiKeyEnv))",
	"apiKey = strings.TrimSpace(os.Getenv(apiKeyEnv)) /* trailing note */",
	"apiKey = strings.TrimSpace(os.Getenv(apiKeyEnv))\nreturn apiKey",
	"password = readPasswordFromKeychain(ctx)\nif password == \"\" {\n\treturn nil\n}",
	"apiKey = strings.TrimSpace(os.Getenv(apiKeyEnv)) // read from the environment",

	"password = readPasswordFromKeychain(ctx)",
	"password = read_password(ctx)",
	"apiKey = cfg.Provider.APIKey",
	"https://example.com/download?signature=$SIGNED_URL_TOKEN",
	"token = os.Getenv(tokenEnv)",
	"Cookie: theme=dark",
	"Cookie: session=$TOKEN",
	"Set-Cookie: theme=dark; Path=/; HttpOnly",
	"password: $PASSWORD # injected at runtime",
	"password: |-\n  ${PASSWORD}",
	"password: \"${PASS\n  WORD}\"",
	"password: short\nother: value",
	"password:\n  user: alice\n  host: db",
	"password:\n  \"quoted key\": alice",
	"password: normalize(\n  input,\n)",
	"credentials:\n  - name: alpha\n  - name: beta",
	"password: short\n  # explanatory note for operators that is fairly long\nother: value",
	"password: short # a trailing note that is fairly long\n  # more notes\nother: value",
	"SECRET=dummy",
	"credential: placeholder",
}

// looksLikeSecretPositives are credential-shaped values in every form the
// heuristic recognises.
var looksLikeSecretPositives = []string{
	"Authorization: Bearer " + strings.Repeat("q", 24) + "-opaque",
	"api_key=" + strings.Repeat("0123456789abcdef", 2),
	"OPENAI_API_KEY=" + "s" + "k-" + strings.Repeat("a", 24),
	"Txn-Token: " + strings.Repeat("t", 32),
	"-----" + "BEGIN RSA PRIVATE KEY-----",
	"g" + "hp_" + strings.Repeat("x", 36),
	"api_key = " + strings.Repeat("abcd", 5) + "-secret.v2",
	"OPENAI_API_KEY=${OPENAI_API_KEY:-" + strings.Repeat("horse", 5) + "}",
	`password=${UNSET:-correct-horse-battery-staple}`,
	`api_key = strings.TrimSpace(cfg.APIKey) + "` + strings.Repeat("stapl", 5) + `"`,
	`api_key = strings.TrimSpace(cfg.APIKey) /* note */ + "` + strings.Repeat("stapl", 5) + `"`,
	`apiKey = readApiKeyFromEnvironment() /*`,
	"apiKey = readApiKeyFromEnvironment()\n  + \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment()\n\n  // note\n  + \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment()\n/* note */ + \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment()\n/* open\n*/ + \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment()\n  [\"concat\"](\"" + strings.Repeat("stapl", 5) + "\")",
	"apiKey = readApiKeyFromEnvironment()\n  (\"" + strings.Repeat("stapl", 5) + "\")",
	"apiKey = readApiKeyFromEnvironment()\n  or \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment()\n  * 0 || \"" + strings.Repeat("stapl", 5) + "\"",
	"apiKey = readApiKeyFromEnvironment() // 1 or \"" + strings.Repeat("stapl", 5) + "\"",
	"password: \"correct-\\\n  horse-battery-staple\"",
	"password: \"correct-\n  horse-battery-staple\"",
	"password: correct-\n  horse-battery-staple",
	"password: \"abcdefgh\n  ijklmnop\" # rotated",
	"password: abcdefgh\n\n  ijklmnop",
	"password: abc\n  def\n  ghi\n  jkl\n  mno",
	"password:\n  " + strings.Repeat("live", 6) + "",
	"password:\n  \"prefix: correct-horse-battery-staple\"",
	"password:\n  \"correct-horse-\n    battery-staple+\"",
	"password: \"" + strings.Repeat("\\\n", 70) + strings.Repeat("live", 6) + "\"",
	"password: \"abcdefghijklmnopqrstuvwxyz\n" + strings.Repeat("  x\n", 70) + "  end\"",
	"password: " + strings.Repeat("p", 20),
	"OPENAI_API_KEY=" + strings.Repeat("a1b2c3d4", 3),
	"SLACK_BOT_TOKEN: " + strings.Repeat("z9y8", 6),
	`password="correct horse battery staple"`,
	"client_secret='pass phrase with spaces!'",
	"password=correct.horse.battery.staple",
	"Authorization: Bearer ~" + strings.Repeat("a", 20),
	"Authorization: Basic dXNlcjpwYXNzd29yZA==",
	"PASSWORD=p@ssword-correct-horse",
	`password="$tr0ng-passw0rd-2024!extra"`,
	`api_key="<не-placeholder>` + "0123456789abcdef" + `"`,
	`password="${UNSET:-correct-horse-battery-staple}"`,
	"https://bucket.s3.amazonaws.com/artifact?X-Amz-Credential=AXXX%2F20260831&X-" + "Amz-Signature=" + strings.Repeat("f0e1d2c3", 8),
	"curl 'https://acct.blob.core.windows.net/c/b?sig=" + strings.Repeat("Zx", 12) + "'",
	"api_key=" + strings.Repeat("0123456789abcdef", 2) + "(",
	"api_key=abcdefghijklmnopqrst(",
	"password=CorrectHorseBattery(ctx)",
	"password=correct_horse_battery(ctx)",
	"PASSWORD=p@ssword&correct-horse-battery-staple",
	"PASSWORD=short|correct-horse-battery-staple",
	"PASSWORD=short,correct-horse-battery-staple",
	"PASSWORD=short;correct-horse-battery-staple",
	`PASSWORD=short\correct-horse-battery-staple`,
	"password: >-\n  correct-horse-battery-staple",
	`"password": >-
  correct-horse-battery-staple`,
	`'client_secret': |
  correct-horse-battery-staple`,
	`"password": correct horse battery staple`,
	"password: |\n  correct-horse-\n  battery-staple",
	"password: >-\n  correct horse battery staple",
	"password: |-\n  correct(horse)battery-staple",
	"api_key: |\n    " + strings.Repeat("0a1b2c3d", 3),
	"password: |2-\n  correct-horse-battery-staple",
	"SECRET=correct-horse-battery-staple",
	"credential: correct-horse-battery-staple-value",
	// Dotted values in credential-keyed config scalars are attacker-
	// controllable literal shapes; only '=' code assignments are exempt.
	"password: config.production.password",
	"token: config.Providers.Default.AccessToken",
	"password: readPasswordFromKeychain(ctx)",
	"PASSWORD: short`correct-horse-battery-staple",
	"PASSWORD: short correct-horse-battery-staple",
	"PASSWORD: short correct-horse-battery-staple # rotated credential",
	`password: correct(horse)battery-staple`,
	`password="{correct-horse-battery-staple}"`,
	`password="[correct-horse-battery-staple]"`,
	`password="<correct-horse-battery-staple>"`,
	`password="%correct-horse-battery-staple%"`,
	"PASSWORD=short\u200bcorrect-horse-battery-staple",
	"Cookie: sessionid=correct-horse-battery-staple",
	"Cookie: theme=dark; sessionid=correct-horse-battery-staple",
	"Set-Cookie: sessionid=correct-horse-battery-staple; HttpOnly",
}

func TestLooksLikeSecretIgnoresPlaceholdersAndBareKeywords(t *testing.T) {
	t.Parallel()
	for _, text := range looksLikeSecretNegatives {
		if LooksLikeSecret(text) {
			t.Fatalf("LooksLikeSecret(%q) = true, want false for a placeholder or bare keyword", text)
		}
	}
	for _, text := range looksLikeSecretPositives {
		if !LooksLikeSecret(text) {
			t.Fatalf("LooksLikeSecret(%q) = false, want true for a credential-shaped value", text)
		}
	}
}

func TestSecretValuePlaceholderRequiresRecognizedForms(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"$TOKEN",
		"${TOKEN}",
		"{{ .Token }}",
		"{placeholder}",
		"<your-token>",
		"[REDACTED]",
		"%PASSWORD%",
		"%(password)s",
	} {
		if !secretValuePlaceholder(value) {
			t.Fatalf("secretValuePlaceholder(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"{correct-horse-battery-staple}",
		"[correct-horse-battery-staple]",
		"<correct-horse-battery-staple>",
		"%correct-horse-battery-staple%",
	} {
		if secretValuePlaceholder(value) {
			t.Fatalf("secretValuePlaceholder(%q) = true, want false for a wrapped literal", value)
		}
	}
}

func TestRemediationPullRequestBodyNeutralizesActiveText(t *testing.T) {
	t.Parallel()
	finding := &store.Finding{
		ID:      "fnd_inject",
		Title:   "Fix @maintainer <!-- hidden --> issue",
		Summary: "Details:\n/landpr\n@oncall please merge\napi_key=" + strings.Repeat("k9j8", 6),
	}
	body := RemediationPullRequestBody(finding, nil)
	for _, forbidden := range []string{"@maintainer", "@oncall", "<!--", "\n/landpr", strings.Repeat("k9j8", 6)} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("PR body carries active or credential text %q:\n%s", forbidden, body)
		}
	}
	title := RemediationPullRequestTitle(finding)
	if strings.Contains(title, "@maintainer") || strings.Contains(title, "<!--") {
		t.Fatalf("PR title carries active text: %q", title)
	}
}

func TestRemediationPullRequestBodyWithholdsLineWrappedCredential(t *testing.T) {
	t.Parallel()
	const key = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
	finding := &store.Finding{ID: "fnd_wrap", Title: "wrap", Summary: "found " + key[:11] + "\n" + key[11:] + " in config"}
	body := RemediationPullRequestBody(finding, nil)
	if strings.Contains(body, key[:11]) || strings.Contains(body, key[11:]) {
		t.Fatalf("PR body carries wrapped credential fragments:\n%s", body)
	}
	if !strings.Contains(body, "content withheld") {
		t.Fatalf("PR body did not withhold the wrapped-credential section:\n%s", body)
	}
}

func TestSecretLikeLineDigestsSharesWindowsAcrossCommentOnlyLines(t *testing.T) {
	var b strings.Builder
	for range 2000 {
		b.WriteString("# api_key=" + strings.Repeat("k9", 12) + "\n")
	}
	digests := SecretLikeLineDigests(b.String())
	if len(digests) != 2000 {
		t.Fatalf("digests = %d, want 2000", len(digests))
	}
	for _, d := range digests[1:] {
		if d != digests[0] {
			t.Fatal("comment-only flagged lines must share one window digest")
		}
	}
}
