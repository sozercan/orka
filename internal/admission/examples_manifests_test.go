package admission

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apimachineryyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	sigsyaml "sigs.k8s.io/yaml"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	fakeworkspacev1alpha1 "github.com/orka-agents/orka/api/fake.workspace/v1alpha1"
	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/executionmode"

	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestShippedManifestsDecodeStrictly guards the YAML that users copy first.
// Every Orka-group document under examples/ and config/samples/ must
// strict-decode into its typed API object (catching unknown or misspelled
// fields that the API server would reject), and every built-in runtime Agent
// must pass the same admission contract the live webhook enforces, including
// the harness-v2 "spec.model.name is required" rule.
func TestShippedManifestsDecodeStrictly(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, workspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, acpworkspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, fakeworkspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, gatewayv1alpha1.AddToScheme(scheme))

	admissionScheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(admissionScheme))
	require.NoError(t, corev1alpha1.AddToScheme(admissionScheme))

	roots := []string{
		filepath.Join("..", "..", "examples"),
		filepath.Join("..", "..", "config", "samples"),
	}
	checkedDocuments := 0
	checkedAgents := 0
	for _, root := range roots {
		require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
				return nil
			}
			documents, err := splitYAMLDocuments(path)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			for index, document := range documents {
				object, checked, err := strictDecodeOrkaDocument(scheme, document)
				if err != nil {
					return fmt.Errorf("%s document %d: %w", path, index+1, err)
				}
				if !checked {
					continue
				}
				checkedDocuments++
				if agent, ok := object.(*corev1alpha1.Agent); ok {
					checkedAgents++
					if err := validateExampleAgentContract(t, admissionScheme, agent); err != nil {
						return fmt.Errorf("%s document %d: %w", path, index+1, err)
					}
				}
			}
			return nil
		}))
	}
	// Guard the guard: if the walk finds nothing, the roots moved and the
	// test is validating air.
	require.Greater(t, checkedDocuments, 10, "expected Orka manifests under examples/ and config/samples/")
	require.Greater(t, checkedAgents, 3, "expected Agent manifests under examples/ and config/samples/")
}

func splitYAMLDocuments(path string) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return splitYAMLDocumentsFrom(file)
}

// splitYAMLDocumentsFrom splits a multi-document YAML stream on "---",
// dropping empty documents.
func splitYAMLDocumentsFrom(source io.Reader) ([][]byte, error) {
	reader := apimachineryyaml.NewYAMLReader(bufio.NewReader(source))
	var documents [][]byte
	for {
		document, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return documents, nil
		}
		if err != nil {
			return nil, err
		}
		if len(strings.TrimSpace(string(document))) == 0 {
			continue
		}
		documents = append(documents, document)
	}
}

// strictDecodeOrkaDocument decodes one YAML document into its registered Orka
// typed object with unknown fields rejected. Known Kubernetes and Kustomize
// resources are skipped. Unknown API identities are errors so misspelled Orka
// groups cannot bypass validation; other third-party kinds need explicit support.
func strictDecodeOrkaDocument(scheme *runtime.Scheme, document []byte) (runtime.Object, bool, error) {
	var typeMeta metav1.TypeMeta
	if err := sigsyaml.Unmarshal(document, &typeMeta); err != nil {
		return nil, false, fmt.Errorf("read apiVersion/kind: %w", err)
	}
	if typeMeta.APIVersion == "" || typeMeta.Kind == "" {
		return nil, false, nil
	}
	groupVersion, err := schema.ParseGroupVersion(typeMeta.APIVersion)
	if err != nil {
		return nil, false, fmt.Errorf("parse apiVersion %q: %w", typeMeta.APIVersion, err)
	}
	gvk := groupVersion.WithKind(typeMeta.Kind)
	if clientgoscheme.Scheme.Recognizes(gvk) ||
		(typeMeta.APIVersion == "kustomize.config.k8s.io/v1beta1" && typeMeta.Kind == "Kustomization") {
		return nil, false, nil
	}
	object, err := scheme.New(gvk)
	if err != nil {
		return nil, false, fmt.Errorf("unsupported manifest %s: %w", gvk, err)
	}
	if err := sigsyaml.UnmarshalStrict(document, object); err != nil {
		return nil, false, fmt.Errorf("strict decode %s: %w", typeMeta.Kind, err)
	}
	return object, true, nil
}

func TestStrictDecodeOrkaDocument_APIIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	for _, tc := range []struct {
		name       string
		apiVersion string
		kind       string
		checked    bool
		wantErr    bool
	}{
		{"Orka Task", "core.orka.ai/v1alpha1", "Task", true, false},
		{"misspelled Orka group", "core.orka.io/v1alpha1", "Task", false, true},
		{"Task in core v1", "v1", "Task", false, true},
		{"Kubernetes Namespace", "v1", "Namespace", false, false},
		{"Kubernetes Deployment", "apps/v1", "Deployment", false, false},
		{"Kustomization", "kustomize.config.k8s.io/v1beta1", "Kustomization", false, false},
		{"misspelled Kustomization kind", "kustomize.config.k8s.io/v1beta1", "Kustomizatoin", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document := fmt.Sprintf("apiVersion: %s\nkind: %s\n", tc.apiVersion, tc.kind)
			object, checked, err := strictDecodeOrkaDocument(scheme, []byte(document))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.checked, checked)
			if checked {
				require.IsType(t, &corev1alpha1.Task{}, object)
			} else {
				require.Nil(t, object)
			}
		})
	}
}

// validateExampleAgentContract runs a shipped Agent through the real
// AgentContractValidator under a harness-v2 namespace, mirroring the mutating
// contract defaulter that runs first in a live cluster. This is what catches
// an example Agent that would deploy but fail at dispatch, such as a built-in
// runtime Agent without spec.model.name.
func validateExampleAgentContract(t *testing.T, scheme *runtime.Scheme, agent *corev1alpha1.Agent) error {
	t.Helper()
	object := agent.DeepCopy()
	if object.Namespace == "" {
		object.Namespace = "default"
	}
	if err := executionmode.DefaultBuiltInAgentContract(object, executionmode.HarnessV2); err != nil {
		return fmt.Errorf("default built-in contract: %w", err)
	}
	namespace := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name:   object.Namespace,
			Labels: map[string]string{executionmode.NamespaceLabel: string(executionmode.HarnessV2)},
		},
	}
	validator := &AgentContractValidator{
		decoder: ctrladmission.NewDecoder(scheme),
		reader:  fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(namespace).Build(),
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshal Agent: %w", err)
	}
	response := validator.Handle(context.Background(), ctrladmission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: object.Namespace,
		Object:    runtime.RawExtension{Raw: raw},
	}})
	if !response.Allowed {
		return fmt.Errorf("Agent %q would be denied by admission: %s", object.Name, response.Result.Message)
	}
	return nil
}

// TestDocumentedManifestsDecodeStrictly guards the YAML people copy out of the
// root README, documentation site, and example READMEs. Every YAML fence or
// shell heredoc that carries an Orka apiVersion and kind must strict-decode
// into its typed API object, so a renamed or misspelled field cannot tell
// users to write something the API server rejects.
//
// Fragments without either type metadata field and known non-Orka resources
// are skipped. Unknown apiVersion/kind pairs and partial type metadata are
// rejected. kubectl manifest heredocs must parse and include both fields.
// A deliberately invalid block (showing a rejected manifest, for example)
// opts out with an HTML comment on the line before its opening fence:
//
//	<!-- orka:skip-strict-decode reason -->
func TestDocumentedManifestsDecodeStrictly(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, workspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, acpworkspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, fakeworkspacev1alpha1.AddToScheme(scheme))
	require.NoError(t, gatewayv1alpha1.AddToScheme(scheme))

	checkedDocuments := 0
	checkMarkdown := func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		blocks, err := extractYAMLCodeBlocks(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, block := range blocks {
			documents, err := splitYAMLDocumentsFrom(strings.NewReader(block.body))
			if err != nil {
				if block.strict {
					return fmt.Errorf("%s:%d: YAML does not parse: %w", path, block.line, err)
				}
				// Other shell heredocs may carry arbitrary text.
				continue
			}
			if block.wholeManifest && len(documents) == 0 {
				return fmt.Errorf("%s:%d: kubectl manifest is empty", path, block.line)
			}
			for index, document := range documents {
				isManifest, err := looksLikeManifest(document)
				if err != nil {
					if block.strict {
						return fmt.Errorf("%s:%d document %d: invalid YAML block: %w", path, block.line, index+1, err)
					}
					continue
				}
				if !isManifest {
					if block.wholeManifest {
						return fmt.Errorf("%s:%d document %d: kubectl manifest must include apiVersion and kind", path, block.line, index+1)
					}
					// YAML fences and other heredocs may illustrate a fragment.
					continue
				}
				object, checked, err := strictDecodeOrkaDocument(scheme, document)
				if err != nil {
					return fmt.Errorf("%s:%d document %d: %w", path, block.line, index+1, err)
				}
				if !checked || object == nil {
					continue
				}
				checkedDocuments++
			}
		}
		return nil
	}
	for _, root := range []string{
		filepath.Join("..", "..", "README.md"),
		filepath.Join("..", "..", "website", "docs"),
		filepath.Join("..", "..", "examples"),
	} {
		require.NoError(t, filepath.WalkDir(root, checkMarkdown))
	}
	// Guard the guard: if the walk finds nothing, the docs moved and the test
	// is validating air.
	require.Greater(t, checkedDocuments, 40, "expected Orka manifests in documented code blocks")
}

// yamlCodeBlock is one extracted YAML block, with the 1-based line number of
// its first content line so a failure points at the right place in the source file.
type yamlCodeBlock struct {
	line int
	body string
	// YAML fences and kubectl manifest heredocs must parse as YAML. Other
	// heredocs may carry scripts, patches, or arbitrary text.
	strict bool
	// kubectl apply/create requires complete objects, not YAML fragments.
	wholeManifest bool
}

const docsStrictDecodeSkipMarker = "<!-- orka:skip-strict-decode"

// extractYAMLCodeBlocks returns YAML fences and shell heredocs from a markdown
// file, accepting backtick or tilde fences and respecting the opt-out marker.
func extractYAMLCodeBlocks(path string) ([]yamlCodeBlock, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	var blocks []yamlCodeBlock
	for i := 0; i < len(lines); i++ {
		fence, language, ok := parseOpeningFence(lines[i])
		if !ok {
			continue
		}
		start := i
		var body []string
		i++
		for ; i < len(lines); i++ {
			closing, info, ok := parseOpeningFence(lines[i])
			if ok && info == "" && closing[0] == fence[0] && len(closing) >= len(fence) {
				break
			}
			body = append(body, lines[i])
		}
		if start > 0 && strings.Contains(lines[start-1], docsStrictDecodeSkipMarker) {
			continue
		}
		switch language {
		case "yaml", "yml":
			blocks = append(blocks, yamlCodeBlock{line: start + 2, body: strings.Join(body, "\n"), strict: true})
		case "bash", "sh", "shell", "console":
			// The install and first-task instructions pipe manifests through
			// `kubectl apply -f - <<'EOF'`. Those are the manifests users copy
			// before any other, so they get checked too.
			heredocs, err := extractShellHeredocs(start+2, body)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, heredocs...)
		}
	}
	return blocks, nil
}

var heredocOpener = regexp.MustCompile(`<<(-?)\s*['"]?([A-Za-z_][A-Za-z0-9_]*)['"]?`)

// Match the kubectl stdin-manifest commands used by the install examples.
var kubectlManifestHeredoc = regexp.MustCompile(`^\s*(?:\$\s+)?kubectl\b.*\b(?:apply|create)\b.*(?:-f\s+|--filename(?:=|\s+))-(?:\s|$)`)

// extractShellHeredocs returns the body of every heredoc in a shell code
// block. baseLine is the 1-based line number of the shell block's first content
// line. Non-manifest heredocs are filtered out later by looksLikeManifest.
func extractShellHeredocs(baseLine int, lines []string) ([]yamlCodeBlock, error) {
	var blocks []yamlCodeBlock
	for i := 0; i < len(lines); i++ {
		match := heredocOpener.FindStringSubmatch(lines[i])
		if match == nil {
			continue
		}
		stripTabs := match[1] == "-"
		delimiter := match[2]
		wholeManifest := kubectlManifestHeredoc.MatchString(lines[i])
		start := i
		var body []string
		i++
		for ; i < len(lines); i++ {
			line := lines[i]
			if stripTabs {
				line = strings.TrimLeft(line, "\t")
			}
			if line == delimiter {
				break
			}
			body = append(body, line)
		}
		if i == len(lines) {
			return nil, fmt.Errorf("line %d: unterminated heredoc %q", baseLine+start, delimiter)
		}
		blocks = append(blocks, yamlCodeBlock{
			line: baseLine + start + 1, body: strings.Join(body, "\n"),
			strict: wholeManifest, wholeManifest: wholeManifest,
		})
	}
	return blocks, nil
}

// parseOpeningFence reports the fence run and language of a code fence line.
// Docusaurus allows metadata after the language ("```yaml title=x"); only the
// first word is the language.
func parseOpeningFence(line string) (string, string, bool) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < 3 || (trimmed[0] != '`' && trimmed[0] != '~') {
		return "", "", false
	}
	run := 0
	for run < len(trimmed) && trimmed[run] == trimmed[0] {
		run++
	}
	if run < 3 {
		return "", "", false
	}
	info := strings.TrimSpace(trimmed[run:])
	language, _, _ := strings.Cut(info, " ")
	return trimmed[:run], strings.ToLower(language), true
}

func TestExtractYAMLCodeBlocks(t *testing.T) {
	for _, fence := range []string{"```", "~~~"} {
		for _, command := range []string{"kubectl apply -f -", "kubectl -n orka-system create --filename=-"} {
			t.Run(fence+"/"+command, func(t *testing.T) {
				const manifest = "apiVersion: core.orka.ai/v1alpha1\nkind: Task"
				content := strings.Join([]string{
					"# Example", "", fence + "yaml title=task", manifest, fence + fence[:1], "",
					fence + "bash", command + " <<'EOF'", manifest, "EOF",
					"cat <<'TEXT'", "[arbitrary text", "TEXT", fence,
				}, "\n")
				path := filepath.Join(t.TempDir(), "example.md")
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

				blocks, err := extractYAMLCodeBlocks(path)
				require.NoError(t, err)
				require.Equal(t, []yamlCodeBlock{
					{line: 4, body: manifest, strict: true},
					{line: 10, body: manifest, strict: true, wholeManifest: true},
					{line: 14, body: "[arbitrary text"},
				}, blocks)
			})
		}
	}
}

func TestExtractYAMLCodeBlocks_HeredocTerminators(t *testing.T) {
	const manifest = "apiVersion: core.orka.ai/v1alpha1\nkind: Task"
	for _, tc := range []struct {
		name       string
		opener     string
		body       string
		terminator string
		wantErr    bool
	}{
		{"plain", "<<EOF", manifest, "EOF", false},
		{"missing terminator", "<<EOF", manifest, "", true},
		{"space-indented terminator", "<<EOF", manifest, " EOF", true},
		{"tab-indented terminator", "<<EOF", manifest, "\tEOF", true},
		{"trailing whitespace", "<<EOF", manifest, "EOF ", true},
		{"strip leading tabs", "<<-'EOF'", "\tapiVersion: core.orka.ai/v1alpha1\n\t\tkind: Task", "\t\tEOF", false},
		{"spaces are not stripped", "<<-EOF", manifest, "\t EOF", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "example.md")
			content := strings.Join([]string{
				"```bash", "kubectl apply -f - " + tc.opener, tc.body, tc.terminator, "```",
			}, "\n")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

			blocks, err := extractYAMLCodeBlocks(path)
			if tc.wantErr {
				require.EqualError(t, err, "line 2: unterminated heredoc \"EOF\"")
				return
			}
			require.NoError(t, err)
			require.Equal(t, []yamlCodeBlock{{line: 3, body: manifest, strict: true, wholeManifest: true}}, blocks)
		})
	}
}

// looksLikeManifest identifies Kubernetes objects and rejects malformed YAML
// or partial type metadata. Documents without either type metadata field are
// fragments, as are lists and scalars illustrating a single field.
func looksLikeManifest(document []byte) (bool, error) {
	var probe any
	if err := sigsyaml.Unmarshal(document, &probe); err != nil {
		return false, err
	}
	mapping, isMapping := probe.(map[string]any)
	if !isMapping {
		return false, nil
	}
	_, hasAPIVersion := mapping["apiVersion"]
	_, hasKind := mapping["kind"]
	if !hasAPIVersion && !hasKind {
		return false, nil
	}

	var typeMeta metav1.TypeMeta
	if err := sigsyaml.Unmarshal(document, &typeMeta); err != nil {
		return false, err
	}
	if typeMeta.APIVersion == "" || typeMeta.Kind == "" {
		return false, errors.New("manifest must include non-empty apiVersion and kind")
	}
	return true, nil
}

func TestLooksLikeManifest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		document string
		manifest bool
		wantErr  bool
	}{
		{"manifest", "apiVersion: core.orka.ai/v1alpha1\nkind: Task", true, false},
		{"fragment", "spec:\n  agentRef: coder", false, false},
		{"list", "- kind: Task", false, false},
		{"scalar", "Task", false, false},
		{"missing apiVersion", "kind: Task", false, true},
		{"missing kind", "apiVersion: core.orka.ai/v1alpha1", false, true},
		{"misspelled apiVersion", "apiVerison: core.orka.ai/v1alpha1\nkind: Task", false, true},
		{"misspelled kind", "apiVersion: core.orka.ai/v1alpha1\nknd: Task", false, true},
		{"empty metadata", "apiVersion: ''\nkind: ''", false, true},
		{"invalid YAML", "kind: [Task", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest, err := looksLikeManifest([]byte(tc.document))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.manifest, manifest)
		})
	}
}
