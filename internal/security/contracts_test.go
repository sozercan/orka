package security

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/orka/internal/store"
)

func TestParseContractsAcceptValidExamples(t *testing.T) {
	slicesData := []byte(`{"schemaVersion":1,"slices":[{"id":"slice_demo","repositoryScan":"repo","source":"deterministic-go-package","title":"Go package internal/security","summary":"Security parsing.","kind":"package","ownedFiles":[{"path":"internal/security/security.go","reason":"source"}],"confidence":"high","status":"pending"}]}`)
	if _, err := ParseReviewSlicesArtifact(slicesData); err != nil {
		t.Fatalf("ParseReviewSlicesArtifact() error = %v", err)
	}

	contextData := []byte(`{"schemaVersion":1,"sliceId":"slice_demo","includedFiles":[{"path":"internal/security/security.go","role":"owned","bytes":10,"includedBytes":10,"includedLineRanges":[{"startLine":1,"endLine":2}],"truncated":false,"readable":true,"skippedReason":null}],"promptBytes":100,"approximateTokens":25}`)
	if _, err := ParseReviewContextManifest(contextData); err != nil {
		t.Fatalf("ParseReviewContextManifest() error = %v", err)
	}

	findingsData := []byte(`{"schemaVersion":2,"repository":{"repoURL":"https://github.com/example/app","branch":"main","subPath":"","baseSHA":"","headSHA":""},"scan":{"mode":"initial","sliceId":"slice_demo","summary":"Reviewed."},"findings":[]}`)
	if _, err := ParseFindingsV2Artifact(findingsData); err != nil {
		t.Fatalf("ParseFindingsV2Artifact() error = %v", err)
	}
}

func TestParseContractsRejectMalformedExamples(t *testing.T) {
	if _, err := ParseReviewSlicesArtifact([]byte(`{"schemaVersion":1,"slices":[{"id":"slice_bad","repositoryScan":"repo","source":"mapper","title":"bad","ownedFiles":[{"path":"../secret"}]}]}`)); err == nil {
		t.Fatal("ParseReviewSlicesArtifact() error = nil, want unsafe path rejected")
	}
	if _, err := ParseReviewContextManifest([]byte(`{"schemaVersion":1,"sliceId":"slice_bad","includedFiles":[{"path":"app.go","role":"owned","includedLineRanges":[{"startLine":5,"endLine":3}]}]}`)); err == nil {
		t.Fatal("ParseReviewContextManifest() error = nil, want invalid range rejected")
	}
	if _, err := ParseFindingsV2Artifact([]byte(`{"schemaVersion":2}`)); err == nil {
		t.Fatal("ParseFindingsV2Artifact() error = nil, want missing findings array rejected")
	}
}

func TestValidateFindingsV2PartitionsAcceptedAndDropped(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "internal/archive/extract.go", "package archive\n\nfunc Extract(name string) string {\n\treturn name\n}\n")
	quote := "return name"
	artifact := FindingsV2Artifact{
		SchemaVersion: SchemaVersionFindingsV2,
		Repository: FindingsV2Repository{
			RepoURL: "https://github.com/example/app",
			Branch:  "main",
			HeadSHA: "abc123",
		},
		Scan: FindingsV2Scan{Mode: "initial", SliceID: "slice_archive"},
		Findings: []FindingsV2Finding{
			{
				Title:       "Archive path escape",
				Category:    "path-traversal",
				Severity:    "high",
				Confidence:  "high",
				Triage:      "confirmed-risk",
				Summary:     "Archive path is trusted.",
				Remediation: "Check resolved paths.",
				Evidence: []FindingsV2EvidenceRef{{
					Path:      "internal/archive/extract.go",
					StartLine: 4,
					EndLine:   4,
					Quote:     &quote,
				}},
			},
			{
				Title:       "Traversal",
				Category:    "path-traversal",
				Severity:    "high",
				Confidence:  "high",
				Summary:     "bad path",
				Remediation: "fix",
				Evidence: []FindingsV2EvidenceRef{{
					Path:      "../secret.txt",
					StartLine: 1,
					EndLine:   1,
				}},
			},
		},
	}
	manifest := ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_archive",
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "internal/archive/extract.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 1, EndLine: 5}},
			Readable:           true,
		}},
	}

	got := ValidateFindingsV2(artifact, manifest, FindingValidationOptions{
		Namespace:      "default",
		RepositoryScan: "repo",
		ScanRunID:      "scan1",
		TaskName:       "task1",
		WorkspaceRoot:  root,
	})
	if len(got.Accepted) != 1 || len(got.Dropped) != 1 {
		t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 1/1", len(got.Accepted), len(got.Dropped))
	}
	if got.Accepted[0].Category != "path-traversal" || got.Accepted[0].SliceID != "slice_archive" {
		t.Fatalf("accepted finding = %#v, want v2 metadata", got.Accepted[0])
	}
	if got.Accepted[0].ValidationStatus != "unvalidated" {
		t.Fatalf("accepted validation status = %q, want unvalidated", got.Accepted[0].ValidationStatus)
	}
	if got.Dropped[0].Reason == "" {
		t.Fatalf("dropped diagnostic = %#v, want reason", got.Dropped[0])
	}
}

func TestValidateFindingsV2AcceptsButDoesNotPersistQuoteWithoutWorkspaceRoot(t *testing.T) {
	quote := "func main"
	finding := validFinding()
	finding.Evidence[0].Quote = &quote

	got := ValidateFindingsV2(FindingsV2Artifact{
		SchemaVersion: SchemaVersionFindingsV2,
		Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
		Scan:          FindingsV2Scan{Mode: "initial", SliceID: "slice_app"},
		Findings:      []FindingsV2Finding{finding},
	}, ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_app",
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "app.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 1, EndLine: 2}},
			Readable:           true,
		}},
	}, FindingValidationOptions{
		Namespace:      "default",
		RepositoryScan: "repo",
		ScanRunID:      "scan1",
		TaskName:       "task1",
	})
	if len(got.Accepted) != 1 || len(got.Dropped) != 0 {
		t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 1/0", len(got.Accepted), len(got.Dropped))
	}
	if got.Accepted[0].Evidence[0].Quote != "" {
		t.Fatalf("accepted quote = %q, want quote omitted from durable evidence", got.Accepted[0].Evidence[0].Quote)
	}
}

func TestValidateFindingsV2AcceptsQuoteFromNonPrefixExcerpt(t *testing.T) {
	quote := "dangerous changed line"
	finding := validFinding()
	finding.Evidence[0].StartLine = 20
	finding.Evidence[0].EndLine = 20
	finding.Evidence[0].Quote = &quote

	got := ValidateFindingsV2(FindingsV2Artifact{
		SchemaVersion: SchemaVersionFindingsV2,
		Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
		Scan:          FindingsV2Scan{Mode: "manual", SliceID: "slice_app"},
		Findings:      []FindingsV2Finding{finding},
	}, ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_app",
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "app.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 16, EndLine: 24}},
			Excerpt:            strings.Join([]string{"line16", "line17", "line18", "line19", "dangerous changed line", "line21", "line22", "line23", "line24"}, "\n") + "\n",
			Readable:           true,
		}},
	}, FindingValidationOptions{
		Namespace:      "default",
		RepositoryScan: "repo",
		ScanRunID:      "scan1",
		TaskName:       "task1",
	})
	if len(got.Accepted) != 1 || len(got.Dropped) != 0 {
		t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 1/0; dropped=%#v", len(got.Accepted), len(got.Dropped), got.Dropped)
	}
}

func TestValidateFindingsV2RejectsQuoteFromWrongLineInNonPrefixExcerpt(t *testing.T) {
	quote := "dangerous changed line"
	finding := validFinding()
	finding.Evidence[0].StartLine = 24
	finding.Evidence[0].EndLine = 24
	finding.Evidence[0].Quote = &quote

	got := ValidateFindingsV2(FindingsV2Artifact{
		SchemaVersion: SchemaVersionFindingsV2,
		Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
		Scan:          FindingsV2Scan{Mode: "manual", SliceID: "slice_app"},
		Findings:      []FindingsV2Finding{finding},
	}, ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_app",
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "app.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 16, EndLine: 24}},
			Excerpt:            strings.Join([]string{"line16", "line17", "line18", "line19", "dangerous changed line", "line21", "line22", "line23", "line24"}, "\n") + "\n",
			Readable:           true,
		}},
	}, FindingValidationOptions{
		Namespace:      "default",
		RepositoryScan: "repo",
		ScanRunID:      "scan1",
		TaskName:       "task1",
	})
	if len(got.Accepted) != 0 || len(got.Dropped) != 1 || !strings.Contains(got.Dropped[0].Reason, "quote does not match") {
		t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%#v, want quote mismatch drop", len(got.Accepted), got.Dropped)
	}
}

func TestValidateFindingsV2NormalizesSeverityAndConfidence(t *testing.T) {
	finding := validFinding()
	finding.Severity = " High "
	finding.Confidence = "MEDIUM"

	got := ValidateFindingsV2(FindingsV2Artifact{
		SchemaVersion: SchemaVersionFindingsV2,
		Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
		Scan:          FindingsV2Scan{Mode: "initial", SliceID: "slice_app"},
		Findings:      []FindingsV2Finding{finding},
	}, basicReviewContextManifest(), FindingValidationOptions{
		Namespace:      "default",
		RepositoryScan: "repo",
		ScanRunID:      "scan1",
		TaskName:       "task1",
	})

	if len(got.Accepted) != 1 || len(got.Dropped) != 0 {
		t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 1/0", len(got.Accepted), len(got.Dropped))
	}
	if got.Accepted[0].Severity != "high" {
		t.Fatalf("accepted severity = %q, want high", got.Accepted[0].Severity)
	}
	if got.Accepted[0].Confidence != "medium" {
		t.Fatalf("accepted confidence = %q, want medium", got.Accepted[0].Confidence)
	}
}

func TestValidateFindingsV2DropsUnsupportedSeverityAndConfidence(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*FindingsV2Finding)
		wantDrop string
	}{
		{
			name: "unsupported severity",
			mutate: func(f *FindingsV2Finding) {
				f.Severity = "critical severity"
			},
			wantDrop: "unsupported severity",
		},
		{
			name: "unsupported confidence",
			mutate: func(f *FindingsV2Finding) {
				f.Confidence = "very high"
			},
			wantDrop: "unsupported confidence",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finding := validFinding()
			tt.mutate(&finding)

			got := ValidateFindingsV2(FindingsV2Artifact{
				SchemaVersion: SchemaVersionFindingsV2,
				Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
				Scan:          FindingsV2Scan{Mode: "initial", SliceID: "slice_app"},
				Findings:      []FindingsV2Finding{finding},
			}, basicReviewContextManifest(), FindingValidationOptions{
				Namespace:      "default",
				RepositoryScan: "repo",
				ScanRunID:      "scan1",
				TaskName:       "task1",
			})

			if len(got.Accepted) != 0 || len(got.Dropped) != 1 {
				t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 0/1", len(got.Accepted), len(got.Dropped))
			}
			if !strings.Contains(got.Dropped[0].Reason, tt.wantDrop) {
				t.Fatalf("drop reason = %q, want contains %q", got.Dropped[0].Reason, tt.wantDrop)
			}
		})
	}
}

func TestValidateFindingRequiredFieldsUsesDeterministicOrder(t *testing.T) {
	finding := validFinding()
	finding.Title = ""
	finding.Category = ""
	finding.Severity = ""

	if got := validateFindingRequiredFields(finding); got != "title is required" {
		t.Fatalf("validateFindingRequiredFields() = %q, want title is required", got)
	}
}

func TestValidateFindingsV2RejectsMissingEvidenceStaleRangesAndQuoteMismatch(t *testing.T) {
	badQuote := "not in file"
	compactedQuote := "package main func main() {}"
	base := validFinding()
	tests := []struct {
		name     string
		mutate   func(*FindingsV2Finding)
		wantDrop string
	}{
		{
			name: "missing evidence",
			mutate: func(f *FindingsV2Finding) {
				f.Evidence = nil
			},
			wantDrop: "evidence is required",
		},
		{
			name: "stale range",
			mutate: func(f *FindingsV2Finding) {
				f.Evidence[0].StartLine = 20
				f.Evidence[0].EndLine = 21
			},
			wantDrop: "outside included review context",
		},
		{
			name: "quote mismatch",
			mutate: func(f *FindingsV2Finding) {
				f.Evidence[0].Quote = &badQuote
			},
			wantDrop: "quote does not match",
		},
		{
			name: "quote requires exact substring",
			mutate: func(f *FindingsV2Finding) {
				f.Evidence[0].StartLine = 1
				f.Evidence[0].EndLine = 2
				f.Evidence[0].Quote = &compactedQuote
			},
			wantDrop: "quote does not match",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finding := base
			finding.Evidence = append([]FindingsV2EvidenceRef{}, base.Evidence...)
			tt.mutate(&finding)
			got := ValidateFindingsV2(FindingsV2Artifact{
				SchemaVersion: SchemaVersionFindingsV2,
				Repository:    FindingsV2Repository{RepoURL: "https://github.com/example/app", Branch: "main"},
				Scan:          FindingsV2Scan{Mode: "initial", SliceID: "slice_app"},
				Findings:      []FindingsV2Finding{finding},
			}, ReviewContextManifest{
				SchemaVersion: SchemaVersionReviewContext,
				SliceID:       "slice_app",
				IncludedFiles: []ReviewContextIncludedFile{{
					Path:               "app.go",
					IncludedLineRanges: []ReviewContextLineRange{{StartLine: 1, EndLine: 2}},
					Excerpt:            "package main\nfunc main() {}\n",
					Readable:           true,
				}},
			}, FindingValidationOptions{
				Namespace:      "default",
				RepositoryScan: "repo",
				ScanRunID:      "scan1",
				TaskName:       "task1",
			})
			if len(got.Accepted) != 0 || len(got.Dropped) != 1 {
				t.Fatalf("ValidateFindingsV2() accepted=%d dropped=%d, want 0/1", len(got.Accepted), len(got.Dropped))
			}
			if !strings.Contains(got.Dropped[0].Reason, tt.wantDrop) {
				t.Fatalf("drop reason = %q, want contains %q", got.Dropped[0].Reason, tt.wantDrop)
			}
		})
	}
}

func basicReviewContextManifest() ReviewContextManifest {
	return ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_app",
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "app.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 1, EndLine: 2}},
			Readable:           true,
		}},
	}
}

func TestFindingV2FingerprintIgnoresEvidenceOrder(t *testing.T) {
	first := validFinding()
	first.Evidence = append(first.Evidence, FindingsV2EvidenceRef{Path: "other.go", StartLine: 2, EndLine: 2})
	second := first
	second.Evidence = []FindingsV2EvidenceRef{first.Evidence[1], first.Evidence[0]}

	left := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", first)
	right := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", second)
	if left != right {
		t.Fatalf("fingerprints differ for reordered evidence: %q vs %q", left, right)
	}
}

func TestFindingV2FingerprintIgnoresEvidenceQuote(t *testing.T) {
	quoteA := "token := \"secret\""
	quoteB := "token := os.Getenv(\"TOKEN\")"
	first := validFinding()
	first.Evidence[0].Quote = &quoteA
	second := validFinding()
	second.Evidence[0].Quote = &quoteB

	left := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", first)
	right := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", second)
	if left != right {
		t.Fatalf("fingerprints differ for quote-only change: %q vs %q", left, right)
	}
}

func TestFindingV2FingerprintCanonicalizesEquivalentEvidence(t *testing.T) {
	emptySymbol := ""
	first := validFinding()
	second := validFinding()
	second.Evidence = []FindingsV2EvidenceRef{
		{Path: " app.go ", StartLine: 2, EndLine: 2, Symbol: &emptySymbol},
		{Path: "app.go", StartLine: 2, EndLine: 2},
	}

	left := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", first)
	right := FindingV2Fingerprint("ns", "repo", "https://github.com/example/app", "main", "", "slice", second)
	if left != right {
		t.Fatalf("fingerprints differ for equivalent evidence refs: %q vs %q", left, right)
	}
}

func TestFindingV2TargetKeyCanonicalizesInputAndSeparatesBranches(t *testing.T) {
	canonical := FindingV2TargetKey("https://github.com/example/app", "main", "services/api")
	equivalent := FindingV2TargetKey(" https://github.com/example/app ", " main ", "/services/api/")
	sshEquivalent := FindingV2TargetKey("git@github.com:example/app.git", "main", "services/api")
	dotGitEquivalent := FindingV2TargetKey("https://github.com/example/app.git", "main", "services/api")
	otherBranch := FindingV2TargetKey("https://github.com/example/app", "release", "services/api")

	if canonical != equivalent {
		t.Fatalf("equivalent target keys differ: %q vs %q", canonical, equivalent)
	}
	if canonical != sshEquivalent || canonical != dotGitEquivalent {
		t.Fatalf("canonical repository target keys differ: HTTPS %q, SSH %q, HTTPS .git %q", canonical, sshEquivalent, dotGitEquivalent)
	}
	if canonical == otherBranch {
		t.Fatalf("target key %q does not distinguish branches", canonical)
	}
}

func TestBuildReviewContextBoundsPromptAndManifest(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app.go", strings.Repeat("line\n", 20))
	writeFile(t, root, "db.go", strings.Repeat("db\n", 20))
	slice := store.ReviewSlice{
		ID:             "slice_app",
		RepositoryScan: "repo",
		Title:          "App",
		Kind:           "package",
		OwnedFiles:     []store.ReviewSliceFile{{Path: "app.go"}, {Path: "db.go"}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{MaxFiles: 2, MaxBytes: 360})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if len(manifest.IncludedFiles) != 1 || !manifest.IncludedFiles[0].Truncated {
		t.Fatalf("manifest = %#v, want one truncated included file", manifest)
	}
	if len(manifest.OmittedFiles) != 1 || manifest.OmittedFiles[0].Path != "db.go" || manifest.OmittedFiles[0].Reason != "maxBytes" {
		t.Fatalf("omitted files = %#v, want db.go omitted by maxBytes", manifest.OmittedFiles)
	}
	if manifest.PromptBytes != len(prompt) || len(prompt) > 360 {
		t.Fatalf("prompt bytes = manifest:%d actual:%d, want actual under max", manifest.PromptBytes, len(prompt))
	}
	if !strings.Contains(prompt, "Valid evidence paths") || !strings.Contains(prompt, "app.go") {
		t.Fatalf("prompt = %q, want evidence path list and file excerpt", prompt)
	}
	included := manifest.IncludedFiles[0]
	if included.Excerpt == "" || !strings.Contains(included.Excerpt, "line") {
		t.Fatalf("manifest excerpt = %q, want trusted file excerpt", included.Excerpt)
	}
	if gotLines := strings.Count(included.Excerpt, "line"); gotLines != included.IncludedLineRanges[0].EndLine {
		t.Fatalf("manifest excerpt line count = %d, want advertised end line %d", gotLines, included.IncludedLineRanges[0].EndLine)
	}
	if strings.Contains(prompt, "db.go") {
		t.Fatalf("prompt = %q, want omitted file absent from valid evidence paths", prompt)
	}
}

func TestBuildReviewContextIncludesChangedLineRanges(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app.go", "package main\n\nfunc main() {}\n")
	slice := store.ReviewSlice{
		ID:             "slice_app",
		RepositoryScan: "repo",
		Title:          "App",
		Kind:           "package",
		OwnedFiles:     []store.ReviewSliceFile{{Path: "app.go"}},
		ChangedFiles:   []string{"app.go"},
		ChangedLineRanges: []store.ChangedLineRange{{
			Path:      "app.go",
			StartLine: 3,
			EndLine:   3,
		}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if !strings.Contains(prompt, "Changed-code focus") || !strings.Contains(prompt, "app.go:3-3") || !strings.Contains(prompt, "newly introduced") {
		t.Fatalf("prompt missing changed-line focus:\n%s", prompt)
	}
	if len(manifest.ChangedLineRanges) != 1 || manifest.ChangedLineRanges[0].Path != "app.go" {
		t.Fatalf("manifest changed ranges = %#v, want app.go range", manifest.ChangedLineRanges)
	}
}

func TestReviewContextManifestRoundTripsChangedRanges(t *testing.T) {
	manifest := ReviewContextManifest{
		SchemaVersion: SchemaVersionReviewContext,
		SliceID:       "slice_app",
		ChangedFiles:  []string{"app.go"},
		ChangedLineRanges: []ChangedLineRange{{
			Path:      "app.go",
			StartLine: 2,
			EndLine:   4,
		}},
		IncludedFiles: []ReviewContextIncludedFile{{
			Path:               "app.go",
			IncludedLineRanges: []ReviewContextLineRange{{StartLine: 1, EndLine: 10}},
			Readable:           true,
		}},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	got, err := ParseReviewContextManifest(data)
	if err != nil {
		t.Fatalf("ParseReviewContextManifest() error = %v", err)
	}
	if len(got.ChangedLineRanges) != 1 || got.ChangedLineRanges[0].StartLine != 2 {
		t.Fatalf("changed ranges = %#v, want round trip", got.ChangedLineRanges)
	}
}

func validFinding() FindingsV2Finding {
	return FindingsV2Finding{
		Title:       "Finding",
		Category:    "test",
		Severity:    "high",
		Confidence:  "high",
		Summary:     "summary",
		Remediation: "remediation",
		Evidence: []FindingsV2EvidenceRef{{
			Path:      "app.go",
			StartLine: 2,
			EndLine:   2,
		}},
	}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func TestDroppedFindingSampleJSON(t *testing.T) {
	raw := DroppedFindingSampleJSON(DroppedFindingDiagnostic{Sample: map[string]string{"title": "bad"}})
	var got map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("DroppedFindingSampleJSON() returned invalid JSON: %v", err)
	}
	if got["title"] != "bad" {
		t.Fatalf("sample = %#v, want title", got)
	}
}

func TestBuildReviewContextUsesChangedLineRangesForLateFileExcerpt(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app.go", strings.Repeat("line\n", 40))
	slice := store.ReviewSlice{
		ID:                "slice_app",
		RepositoryScan:    "repo",
		Title:             "App",
		Kind:              "package",
		OwnedFiles:        []store.ReviewSliceFile{{Path: "app.go"}},
		ChangedFiles:      []string{"app.go"},
		ChangedLineRanges: []store.ChangedLineRange{{Path: "app.go", StartLine: 20, EndLine: 30}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{MaxFiles: 1, MaxBytes: 5000})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if len(manifest.IncludedFiles) != 1 || len(manifest.IncludedFiles[0].IncludedLineRanges) != 1 {
		t.Fatalf("included ranges = %#v", manifest.IncludedFiles)
	}
	included := manifest.IncludedFiles[0].IncludedLineRanges[0]
	if included.StartLine > 20 || included.EndLine < 30 {
		t.Fatalf("included range = %#v, want it to cover changed lines 20-30", included)
	}
	if included.StartLine != 20 || included.EndLine != 34 {
		t.Fatalf("included range = %#v, want bounded context around changed lines", included)
	}
	if len(manifest.ChangedLineRanges) != 1 || manifest.ChangedLineRanges[0].StartLine != 20 || manifest.ChangedLineRanges[0].EndLine != 30 {
		t.Fatalf("changed ranges = %#v, want 20-30 preserved", manifest.ChangedLineRanges)
	}
	if !strings.Contains(prompt, "    20  line") || !strings.Contains(prompt, "app.go:20-30") {
		t.Fatalf("prompt missing late changed lines/range:\n%s", prompt)
	}
}

func TestBuildReviewContextIncludesLongChangedLineWithinBudget(t *testing.T) {
	root := t.TempDir()
	longLine := strings.Repeat("a", 5000)
	writeFile(t, root, "app.go", "before\n"+longLine+"\nafter\n")
	slice := store.ReviewSlice{
		ID:                "slice_app",
		RepositoryScan:    "repo",
		Title:             "App",
		Kind:              "package",
		OwnedFiles:        []store.ReviewSliceFile{{Path: "app.go"}},
		ChangedFiles:      []string{"app.go"},
		ChangedLineRanges: []store.ChangedLineRange{{Path: "app.go", StartLine: 2, EndLine: 2}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{MaxFiles: 1, MaxBytes: 7000})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if len(manifest.IncludedFiles) != 1 || len(manifest.IncludedFiles[0].IncludedLineRanges) != 1 {
		t.Fatalf("included files = %#v", manifest.IncludedFiles)
	}
	if !strings.Contains(prompt, strings.Repeat("a", 4500)) {
		t.Fatalf("prompt did not include long changed line within budget")
	}
	if len(manifest.ChangedLineRanges) != 1 || manifest.ChangedLineRanges[0].StartLine != 2 {
		t.Fatalf("changed ranges = %#v, want line 2 preserved", manifest.ChangedLineRanges)
	}
}

func TestBuildReviewContextPrioritizesChangedLineOverLongPrecontext(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app.go", strings.Repeat("x", 5000)+"\nchanged\nafter\n")
	slice := store.ReviewSlice{
		ID:                "slice_app",
		RepositoryScan:    "repo",
		Title:             "App",
		Kind:              "package",
		OwnedFiles:        []store.ReviewSliceFile{{Path: "app.go"}},
		ChangedFiles:      []string{"app.go"},
		ChangedLineRanges: []store.ChangedLineRange{{Path: "app.go", StartLine: 2, EndLine: 2}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{MaxFiles: 1, MaxBytes: 1200})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if !strings.Contains(prompt, "     2  changed") {
		t.Fatalf("prompt omitted changed line after long precontext:\n%s", prompt)
	}
	if len(manifest.ChangedLineRanges) != 1 || manifest.ChangedLineRanges[0].StartLine != 2 {
		t.Fatalf("changed ranges = %#v, want changed line preserved", manifest.ChangedLineRanges)
	}
}

func TestReadBoundedLogicalLineStopsSkippedLineAtBudget(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader(strings.Repeat("x", 32*1024)+"\nchanged\n"), 1024)
	_, truncated, readAny, consumedBytes, err := readBoundedLogicalLine(reader, false, 2048)
	if err != nil {
		t.Fatalf("readBoundedLogicalLine() error = %v", err)
	}
	if !readAny || !truncated {
		t.Fatalf("readAny=%v truncated=%v, want skipped line truncated at budget", readAny, truncated)
	}
	if consumedBytes > 4096 {
		t.Fatalf("consumedBytes = %d, want bounded close to skip budget", consumedBytes)
	}
}

func TestBuildReviewContextPreservesCapturedRangeWhenLaterRangeExceedsSeekLimit(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "app.go", "first\n"+strings.Repeat("x", maxReviewContextChangedSeekBytes+4096)+"\nsecond\n")
	slice := store.ReviewSlice{
		ID:             "slice_app",
		RepositoryScan: "repo",
		Title:          "App",
		Kind:           "package",
		OwnedFiles:     []store.ReviewSliceFile{{Path: "app.go"}},
		ChangedFiles:   []string{"app.go"},
		ChangedLineRanges: []store.ChangedLineRange{
			{Path: "app.go", StartLine: 1, EndLine: 1},
			{Path: "app.go", StartLine: 3, EndLine: 3},
		},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{MaxFiles: 1, MaxBytes: 2000})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if !strings.Contains(prompt, "     1  first") {
		t.Fatalf("prompt omitted captured first range:\n%s", prompt)
	}
	if len(manifest.IncludedFiles) != 1 || !manifest.IncludedFiles[0].Truncated {
		t.Fatalf("included files = %#v, want one truncated partial excerpt", manifest.IncludedFiles)
	}
	if len(manifest.ChangedLineRanges) != 1 || manifest.ChangedLineRanges[0].StartLine != 1 {
		t.Fatalf("changed ranges = %#v, want reachable first range preserved", manifest.ChangedLineRanges)
	}
}
