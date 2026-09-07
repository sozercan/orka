package security

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

func TestParseThreatModelResultRequiresExactEnvelopeAndBinding(t *testing.T) {
	expected := AgentResultBinding{
		RepositoryScan: "vekil",
		ScanID:         "scan_123",
		PolicyDigest:   "sha256:policy",
	}
	result := ThreatModelResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindThreatModel,
		RepositoryScan: expected.RepositoryScan,
		ScanID:         expected.ScanID,
		PolicyDigest:   expected.PolicyDigest,
		ThreatModel:    "# Threat Model\n\nTrusted boundaries.",
	}
	data := mustMarshalSecurityResult(t, result)
	got, err := ParseThreatModelResult(data, expected)
	if err != nil {
		t.Fatalf("ParseThreatModelResult() error = %v", err)
	}
	if got != result.ThreatModel {
		t.Fatalf("ParseThreatModelResult() = %q, want %q", got, result.ThreatModel)
	}

	result.ScanID = "scan_stale"
	if _, err := ParseThreatModelResult(mustMarshalSecurityResult(t, result), expected); err == nil || !strings.Contains(err.Error(), "scanId") {
		t.Fatalf("ParseThreatModelResult(stale) error = %v, want scan binding rejection", err)
	}
	if _, err := ParseThreatModelResult(append(data, []byte(` {}`)...), expected); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("ParseThreatModelResult(trailing) error = %v, want trailing JSON rejection", err)
	}
}

func TestParseThreatModelResultRedactsCredentials(t *testing.T) {
	expected := AgentResultBinding{RepositoryScan: "repo", ScanID: "scan_123", PolicyDigest: "sha256:policy"}
	// Synthetic values are assembled here so the fixtures cannot be mistaken
	// for credentials copied from a repository.
	token := "ghp_" + strings.Repeat("a", 36)
	value := strings.Repeat("b", 32)
	const prefix = "# Threat model\n\nCredential handling in `config/auth.go:12`.\n\n"
	tests := []struct {
		name, content, want string
	}{
		{name: "token", content: "Found `" + token + "`.", want: "Found `[REDACTED]`."},
		{name: "assignment", content: "```\nAPI_KEY=\"" + value + "\"\n```", want: "```\nAPI_KEY=\"[REDACTED]\"\n```"},
		{name: "inline assignment", content: "Environment assignment `API_KEY=\"" + value + "\"`.", want: "Environment assignment `API_KEY=\"[REDACTED]\"`."},
		{name: "bearer header", content: "Authorization: Bearer " + value, want: "Authorization: [REDACTED]"},
		{name: "transaction header", content: "Txn-Token: " + value, want: "Txn-Token: [REDACTED]"},
		{name: "URL credentials", content: "https://user:" + value + "@example.test/repo", want: "https://[REDACTED]@example.test/repo"},
		{name: "signed URL", content: "https://example.test/file?sig=" + value + "&page=1", want: "https://example.test/file?sig=[REDACTED]&page=1"},
		{name: "prose", content: "The token is " + value, want: "The token is [REDACTED]"},
		{name: "NUL", content: token[:2] + "\x00" + token[2:], want: "[REDACTED]"},
		{name: "escape", content: token[:2] + "\x1b" + token[2:], want: "[REDACTED]"},
		{name: "delete", content: token[:2] + "\x7f" + token[2:], want: "[REDACTED]"},
		{name: "C1 control", content: token[:2] + "\u0085" + token[2:], want: "[REDACTED]"},
		{name: "zero-width space", content: token[:2] + "\u200b" + token[2:], want: "[REDACTED]"},
		{name: "zero-width joiner", content: token[:2] + "\u200d" + token[2:], want: "[REDACTED]"},
		{name: "directional override", content: token[:2] + "\u202e" + token[2:], want: "[REDACTED]"},
		{
			name:    "ordinary markdown",
			content: "Credentials come from Kubernetes Secrets.\n\n## 認証\n\n```go\n\tloadCredentials()\n```\n\n- Rotate keys regularly.",
			want:    "Credentials come from Kubernetes Secrets.\n\n## 認証\n\n```go\n\tloadCredentials()\n```\n\n- Rotate keys regularly.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ThreatModelResultEnvelope{
				SchemaVersion: AgentResultSchemaVersion, Kind: AgentResultKindThreatModel,
				RepositoryScan: expected.RepositoryScan, ScanID: expected.ScanID, PolicyDigest: expected.PolicyDigest,
				ThreatModel: prefix + tt.content,
			}
			got, err := ParseThreatModelResult(mustMarshalSecurityResult(t, result), expected)
			if err != nil {
				t.Fatalf("ParseThreatModelResult() error = %v", err)
			}
			if got != prefix+tt.want {
				t.Fatal("threat model did not redact the credential while preserving surrounding markdown")
			}
			result.ThreatModel = got
			again, err := ParseThreatModelResult(mustMarshalSecurityResult(t, result), expected)
			if err != nil || again != got {
				t.Fatalf("sanitized threat model is not stable on reprocessing: %v", err)
			}
		})
	}
}

func TestParseThreatModelResultValidatesSanitizedContent(t *testing.T) {
	tests := []struct {
		name, content, wantError string
	}{
		{name: "empty", content: " \n\t", wantError: "threatModel is required"},
		{name: "only invisible runes", content: "\x00\u200b", wantError: "threatModel is required"},
		{name: "missing heading", content: "Trusted boundaries.", wantError: "beginning with a heading"},
		{name: "transcript", content: "# Model\n<tool_call>read</tool_call>", wantError: "tool transcript"},
		{name: "hidden transcript", content: "# Model\n<tool_\u200bcall>read</tool_\u200bcall>", wantError: "tool transcript"},
		{name: "oversized before stripping", content: "# " + strings.Repeat("a", maxThreatModelBytes-2) + "\x00", wantError: "threatModel exceeds"},
		{name: "oversized after redaction", content: "# Model\n" + strings.Repeat("pwd=x\n", maxThreatModelBytes/10), wantError: "threatModel exceeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ThreatModelResultEnvelope{
				SchemaVersion: AgentResultSchemaVersion, Kind: AgentResultKindThreatModel,
				ThreatModel: tt.content,
			}
			got, err := ParseThreatModelResult(mustMarshalSecurityResult(t, result), AgentResultBinding{})
			if err == nil || !strings.Contains(err.Error(), tt.wantError) || got != "" {
				t.Fatalf("ParseThreatModelResult() error = %v, want %q and no content", err, tt.wantError)
			}
		})
	}
}

func TestParseFindingsResultRequiresExactRepositoryAndContext(t *testing.T) {
	repository := FindingsV2Repository{
		RepoURL: "https://github.com/sozercan/vekil",
		Branch:  "main",
		HeadSHA: "0123456789abcdef",
	}
	expected := FindingsResultExpectation{
		Binding: AgentResultBinding{
			RepositoryScan: "vekil",
			ScanID:         "scan_123",
			PolicyDigest:   "sha256:policy",
			ContextDigest:  "sha256:context",
		},
		SliceID:    "slice_api",
		Mode:       "initial",
		Repository: repository,
	}
	result := FindingsResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindFindings,
		RepositoryScan: expected.Binding.RepositoryScan,
		ScanID:         expected.Binding.ScanID,
		SliceID:        expected.SliceID,
		PolicyDigest:   expected.Binding.PolicyDigest,
		ContextDigest:  expected.Binding.ContextDigest,
		Findings: FindingsV2Artifact{
			SchemaVersion: SchemaVersionFindingsV2,
			Repository:    repository,
			Scan:          FindingsV2Scan{Mode: expected.Mode, SliceID: expected.SliceID, Summary: "reviewed"},
			Findings: []FindingsV2Finding{{
				Title:       "Authorization bypass",
				Category:    "authz",
				Severity:    "high",
				Confidence:  "high",
				Summary:     "A trusted boundary is bypassed.",
				Remediation: "Enforce authorization.",
				Evidence: []FindingsV2EvidenceRef{{
					Path:      "internal/api/server.go",
					StartLine: 10,
					EndLine:   12,
				}},
			}},
		},
	}
	if _, err := ParseFindingsResult(mustMarshalSecurityResult(t, result), expected); err != nil {
		t.Fatalf("ParseFindingsResult() error = %v", err)
	}

	result.ContextDigest = "sha256:stale"
	if _, err := ParseFindingsResult(mustMarshalSecurityResult(t, result), expected); err == nil || !strings.Contains(err.Error(), "contextDigest") {
		t.Fatalf("ParseFindingsResult(stale context) error = %v, want context rejection", err)
	}
	result.ContextDigest = expected.Binding.ContextDigest
	result.Findings.Repository.HeadSHA = "different"
	if _, err := ParseFindingsResult(mustMarshalSecurityResult(t, result), expected); err == nil || !strings.Contains(err.Error(), "repository identity") {
		t.Fatalf("ParseFindingsResult(repository mismatch) error = %v, want repository rejection", err)
	}
}

func TestParseValidationResultRestrictsEvidenceToAcceptedFinding(t *testing.T) {
	finding := &store.Finding{
		ID: "fnd_123",
		Evidence: []store.FindingEvidenceRef{{
			Kind:      "file",
			Path:      "internal/api/server.go",
			StartLine: 10,
			EndLine:   20,
		}},
	}
	expected := ValidationResultExpectation{
		Binding: AgentResultBinding{RepositoryScan: "vekil", ScanID: "scan_123", PolicyDigest: "sha256:policy"},
		Finding: finding,
	}
	result := ValidationResultEnvelope{
		SchemaVersion:  AgentResultSchemaVersion,
		Kind:           AgentResultKindValidation,
		RepositoryScan: expected.Binding.RepositoryScan,
		ScanID:         expected.Binding.ScanID,
		FindingID:      finding.ID,
		PolicyDigest:   expected.Binding.PolicyDigest,
		Validation: ValidationArtifact{
			Version:   1,
			FindingID: finding.ID,
			Status:    "validated",
			Summary:   "Confirmed.",
			Evidence: ValidationArtifactEvidenceRefs{{
				Kind:      "file",
				Path:      "internal/api/server.go",
				StartLine: 12,
				EndLine:   15,
			}},
		},
	}
	if _, err := ParseValidationResult(mustMarshalSecurityResult(t, result), expected); err != nil {
		t.Fatalf("ParseValidationResult() error = %v", err)
	}

	result.Validation.Evidence[0].StartLine = 1
	if _, err := ParseValidationResult(mustMarshalSecurityResult(t, result), expected); err == nil || !strings.Contains(err.Error(), "outside accepted finding evidence") {
		t.Fatalf("ParseValidationResult(out of range) error = %v, want evidence rejection", err)
	}
	result.Validation.Evidence[0] = store.FindingEvidenceRef{Kind: "artifact", Name: "transcript.txt"}
	if _, err := ParseValidationResult(mustMarshalSecurityResult(t, result), expected); err == nil || !strings.Contains(err.Error(), "task artifacts") {
		t.Fatalf("ParseValidationResult(artifact) error = %v, want artifact rejection", err)
	}
}

func TestParseTrustedReviewContextManifestBindsExactPrompt(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal", "api"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "internal", "api", "server.go"), []byte("package api\n\nfunc Serve() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	slice := store.ReviewSlice{
		ID:         "slice_api",
		Title:      "API",
		Kind:       "package",
		OwnedFiles: []store.ReviewSliceFile{{Path: "internal/api/server.go"}},
	}
	prompt, manifest, err := BuildReviewContext(root, slice, ReviewContextOptions{})
	if err != nil {
		t.Fatalf("BuildReviewContext() error = %v", err)
	}
	if manifest.Prompt != prompt {
		t.Fatal("manifest prompt does not match generated prompt")
	}
	data := mustMarshalSecurityResult(t, manifest)
	parsed, digest, err := ParseTrustedReviewContextManifest(data)
	if err != nil {
		t.Fatalf("ParseTrustedReviewContextManifest() error = %v", err)
	}
	if parsed.Prompt != prompt || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("trusted context prompt/digest = %q/%q", parsed.Prompt, digest)
	}

	manifest.PromptBytes++
	if _, _, err := ParseTrustedReviewContextManifest(mustMarshalSecurityResult(t, manifest)); err == nil || !strings.Contains(err.Error(), "promptBytes") {
		t.Fatalf("ParseTrustedReviewContextManifest(tampered) error = %v, want prompt binding rejection", err)
	}
}

func TestHarnessV2SecurityPromptsRequireTerminalJSONWithoutArtifacts(t *testing.T) {
	scan := &corev1alpha1.RepositoryScan{}
	scan.Name = "vekil"
	scan.Spec.RepoURL = "https://github.com/sozercan/vekil"
	scan.Spec.Branch = "main"
	binding := AgentResultBinding{RepositoryScan: scan.Name, ScanID: "scan_123", PolicyDigest: "sha256:policy", ContextDigest: "sha256:context"}
	manifest := ReviewContextManifest{SchemaVersion: SchemaVersionReviewContext, SliceID: "slice_api", Prompt: "bounded context\n", PromptBytes: len("bounded context\n"), ApproximateTokens: 4}
	slice := store.ReviewSlice{ID: "slice_api", Title: "API", Kind: "package"}
	finding := &store.Finding{ID: "fnd_123", Title: "Finding", Severity: "high", Confidence: "high"}

	prompts := []string{
		BuildThreatModelResultPrompt(scan, "initial", "", "", "", binding),
		BuildReviewResultPrompt(scan, "initial", "", "", "", slice, binding, manifest, FindingsV2Repository{RepoURL: scan.Spec.RepoURL, Branch: "main"}),
		BuildValidationResultPrompt(scan, finding, binding),
	}
	for i, prompt := range prompts {
		if !strings.Contains(prompt, "Return exactly one JSON object") {
			t.Fatalf("prompt[%d] missing terminal JSON contract:\n%s", i, prompt)
		}
		if strings.Contains(prompt, "REQUIRED_SECURITY_ARTIFACTS") || strings.Contains(prompt, "Write these artifacts") {
			t.Fatalf("prompt[%d] retained artifact contract:\n%s", i, prompt)
		}
	}
}

func mustMarshalSecurityResult(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}

const testPatchFindingID = "fnd_1"

func TestParsePatchResultAcceptsIdentityBoundEnvelope(t *testing.T) {
	t.Parallel()
	data := []byte(`{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"Escaped the redirect parameter.","changedFiles":["./routes/index.js","routes/index.js","views/login.hbs"],"testsRun":[{"command":"npm test","exitCode":0}],"risk":"LOW"}`)
	got, err := ParsePatchResult(data, PatchResultExpectation{RepositoryScan: "kaset", FindingID: testPatchFindingID})
	if err != nil {
		t.Fatalf("ParsePatchResult() error = %v", err)
	}
	if got.SchemaVersion != SchemaVersionPatchSummary || got.FindingID != testPatchFindingID || got.Risk != "low" || len(got.TestsRun) != 1 {
		t.Fatalf("summary = %#v", got)
	}
	if len(got.ChangedFiles) != 2 || got.ChangedFiles[0] != "routes/index.js" || got.ChangedFiles[1] != "views/login.hbs" {
		t.Fatalf("changedFiles = %#v, want deduplicated normalized paths", got.ChangedFiles)
	}
}

func TestParsePatchResultNormalizesInvisibleRunes(t *testing.T) {
	t.Parallel()
	data := []byte(`{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"Escaped\u200b redirect parameter.","changedFiles":["routes/\u200bindex.js"],"testsRun":[{"command":"npm\u200b test","exitCode":0}],"risk":"low"}`)
	got, err := ParsePatchResult(data, PatchResultExpectation{RepositoryScan: "kaset", FindingID: testPatchFindingID})
	if err != nil {
		t.Fatalf("ParsePatchResult() error = %v", err)
	}
	if got.Summary != "Escaped redirect parameter." || got.ChangedFiles[0] != "routes/index.js" || got.TestsRun[0].Command != "npm test" {
		t.Fatalf("normalized summary = %#v", got)
	}
}

func TestParsePatchResultRejectsInvalidEnvelopes(t *testing.T) {
	t.Parallel()
	expected := PatchResultExpectation{RepositoryScan: "kaset", FindingID: testPatchFindingID}
	cases := map[string]string{
		"wrong kind":                          `{"schemaVersion":1,"kind":"orka.security.findings.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"risk":"low"}`,
		"wrong finding":                       `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_2","summary":"s","changedFiles":["a.go"],"risk":"low"}`,
		"wrong scan":                          `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"other","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"risk":"low"}`,
		"unknown field":                       `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"risk":"low","diff":"x"}`,
		"no changed files":                    `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":[],"risk":"low"}`,
		"unsafe path":                         `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["../etc/passwd"],"risk":"low"}`,
		"bad risk":                            `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"risk":"critical"}`,
		"tool transcript":                     `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"<tool_call>rm</tool_call>","changedFiles":["a.go"],"risk":"low"}`,
		"markdown fence":                      "```json\n{\"schemaVersion\":1}\n```",
		"empty":                               ``,
		"credential summary":                  `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"Removed api_key=0123456789abcdef0123 from config","changedFiles":["a.go"],"risk":"low"}`,
		"credential summary with format rune": `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"Removed password=short\u200bcorrect-horse-battery-staple","changedFiles":["a.go"],"risk":"low"}`,
		"credential test command":             `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"testsRun":[{"command":"AUTH_TOKEN=0123456789abcdef0123 npm test","exitCode":0}],"risk":"low"}`,
		"credential test command with format rune": `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["a.go"],"testsRun":[{"command":"PASSWORD=short\u200bcorrect-horse-battery-staple npm test","exitCode":0}],"risk":"low"}`,
		"credential-shaped path":                   `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["cfg/api_key=0123456789abcdef0123.txt"],"risk":"low"}`,
		"credential-shaped path with format rune":  `{"schemaVersion":1,"kind":"orka.security.patch.v1","repositoryScan":"kaset","findingId":"fnd_1","summary":"s","changedFiles":["cfg/password=short\u200bcorrect-horse-battery-staple.txt"],"risk":"low"}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParsePatchResult([]byte(payload), expected); err == nil {
				t.Fatalf("ParsePatchResult() accepted %s", name)
			}
		})
	}
}

func TestParsePatchResultRejectsGenericCredentialAssignments(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"AWS_SECRET_ACCESS_KEY", "DATABASE_SECRET", "SERVICE_CREDENTIALS"} {
		for _, field := range []string{"summary", "test command"} {
			t.Run(name+"/"+field, func(t *testing.T) {
				payload := map[string]any{
					"schemaVersion": 1, "kind": "orka.security.patch.v1", "repositoryScan": "kaset",
					"findingId": testPatchFindingID, "summary": "Removed the credential.", "changedFiles": []string{"config.yml"}, "risk": "low",
				}
				assignment := name + "=" + strings.Repeat("0a1b2c3d", 5)
				if field == "summary" {
					payload["summary"] = "Removed " + assignment + " from config."
				} else {
					payload["testsRun"] = []map[string]any{{"command": assignment + " npm test", "exitCode": 0}}
				}
				_, err := ParsePatchResult(mustMarshalSecurityResult(t, payload), PatchResultExpectation{RepositoryScan: "kaset", FindingID: testPatchFindingID})
				if err == nil {
					t.Fatal("patch result accepted a credential assignment")
				}
			})
		}
	}
}
