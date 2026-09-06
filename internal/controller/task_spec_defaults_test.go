package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// TestTaskSpecWithServerDefaultsMatchesCRD walks every default declared in
// the generated Task CRD (top-level and nested) and checks the Go mirror
// produces exactly that value, so the two cannot drift silently.
func TestTaskSpecWithServerDefaultsMatchesCRD(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "core.orka.ai_tasks.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	if len(versions) == 0 {
		t.Fatal("task CRD has no versions")
	}
	schema := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
	specSchema := schema["properties"].(map[string]any)["spec"].(map[string]any)

	defaults := map[string]any{}
	var walk func(node map[string]any, path string)
	walk = func(node map[string]any, path string) {
		if value, ok := node["default"]; ok {
			defaults[path] = value
		}
		if properties, ok := node["properties"].(map[string]any); ok {
			for name, child := range properties {
				if childMap, ok := child.(map[string]any); ok {
					walk(childMap, path+"."+name)
				}
			}
		}
		if items, ok := node["items"].(map[string]any); ok {
			walk(items, path+"[]")
		}
	}
	walk(specSchema, "")

	// Every enclosing object present so nested defaults apply.
	spec := corev1alpha1.TaskSpec{
		Type:        corev1alpha1.TaskTypeAgent,
		RetryPolicy: &corev1alpha1.RetryPolicy{},
		SessionRef:  &corev1alpha1.SessionReference{Name: "s"},
		Workspace: &corev1alpha1.WorkspaceConfig{
			ReadCredentialRef:            &corev1alpha1.WorkspaceCredentialReference{Name: "r"},
			PublicationReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "pr"},
			PublicationCredentialRef:     &corev1alpha1.WorkspaceCredentialReference{Name: "p"},
			ForgeCredentialRef:           &corev1alpha1.WorkspaceCredentialReference{Name: "f"},
		},
		Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{}},
	}
	encoded, err := json.Marshal(taskSpecWithServerDefaults(spec))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	// Fields serialized without omitempty are always sent by Go clients, so
	// the API server never applies their default; nothing to mirror.
	alwaysSerialized := map[string]bool{".sessionRef.append": true}
	for path, want := range defaults {
		if alwaysSerialized[path] {
			continue
		}
		if strings.Contains(path, "[]") {
			// List item defaults (spec.env[]) apply per element; the spec above has no elements.
			continue
		}
		value, present := lookupJSONPath(got, strings.Split(strings.TrimPrefix(path, "."), "."))
		if !present {
			if isZeroDefault(want) {
				// Zero-value defaults are omitted by omitempty and need no mirror.
				continue
			}
			t.Fatalf("CRD default %s = %#v is not applied by taskSpecWithServerDefaults", path, want)
		}
		if fmt.Sprint(value) != fmt.Sprint(want) {
			t.Fatalf("CRD default %s = %#v, helper applies %#v", path, want, value)
		}
	}
	if len(defaults) < 10 {
		t.Fatalf("expected the CRD walk to find the known defaults, found %d", len(defaults))
	}
}

func lookupJSONPath(node any, path []string) (any, bool) {
	current := node
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[key]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func isZeroDefault(value any) bool {
	switch v := value.(type) {
	case bool:
		return !v
	case float64:
		return v == 0
	case int:
		return v == 0
	case string:
		return v == ""
	}
	return false
}

func TestTaskSpecWithServerDefaultsPreservesExplicitValues(t *testing.T) {
	priority := int32(700)
	timeout := metav1.Duration{Duration: 2}
	spec := corev1alpha1.TaskSpec{
		Type:              corev1alpha1.TaskTypeAgent,
		Priority:          &priority,
		Timeout:           &timeout,
		ConcurrencyPolicy: corev1alpha1.AllowConcurrent,
		RetryPolicy:       &corev1alpha1.RetryPolicy{MaxRetries: 2},
		SessionRef:        &corev1alpha1.SessionReference{Name: "s"},
	}
	got := taskSpecWithServerDefaults(spec)
	if *got.Priority != 700 || got.ConcurrencyPolicy != corev1alpha1.AllowConcurrent || got.Timeout.Duration != 2 {
		t.Fatalf("explicit values were overwritten: %#v", got)
	}
	if got.RetryPolicy.BackoffMultiplier != taskSpecDefaultRetryBackoffMultiplier || got.RetryPolicy.MaxRetries != 2 {
		t.Fatalf("retry policy defaults = %#v", got.RetryPolicy)
	}
	if got.SessionRef.MaxMessages != taskSpecDefaultSessionMaxMessages {
		t.Fatalf("session defaults = %#v", got.SessionRef)
	}
	if spec.RetryPolicy.BackoffMultiplier != 0 {
		t.Fatal("input spec was mutated")
	}
}

// TestMatchingReviewRetryTaskToleratesServerDefaults reproduces the live
// wedge: the retry Task the controller created on the previous reconcile
// comes back from the API server with CRD defaults stamped on, and must
// still be recognised as the expected retry identity.
func TestMatchingReviewRetryTaskToleratesServerDefaults(t *testing.T) {
	priority := int32(700)
	timeout := metav1.Duration{Duration: 2}
	build := func() *corev1alpha1.Task {
		task := &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scan-review-retry", Namespace: "orka-system",
				Labels:      map[string]string{"orka.ai/security-slice-id": "slice_1"},
				Annotations: map[string]string{"orka.ai/security-review-attempt": "1"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "core.orka.ai/v1alpha1", Kind: "RepositoryScan", Name: "scan", UID: "uid-1", Controller: new(true),
				}},
			},
			Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: "reviewer"},
				Prompt: "review", Timeout: &timeout, Priority: &priority,
				Workspace: &corev1alpha1.WorkspaceConfig{GitRepo: "https://github.com/o/r", Branch: "main"},
			},
		}
		return task
	}
	desired := build()
	existing := build()
	existing.Spec = taskSpecWithServerDefaults(existing.Spec)
	if !apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) == false {
		t.Fatal("test setup: server defaults should make the raw specs differ")
	}
	if !matchingReviewRetryTask(existing, desired) {
		t.Fatal("retry task with server defaults applied must match the desired identity")
	}
	// The prompt is deterministic within a run and stays part of the identity.
	reprompted := build()
	reprompted.Spec = taskSpecWithServerDefaults(reprompted.Spec)
	reprompted.Spec.Prompt = "review something else"
	if matchingReviewRetryTask(reprompted, desired) {
		t.Fatal("a retry task with a different prompt must not match")
	}
	if got := reviewRetryTaskMismatch(reprompted, desired); !strings.Contains(got, "prompt differs") {
		t.Fatalf("mismatch diagnostic = %q, want a prompt difference", got)
	}
	tampered := build()
	tampered.Spec = taskSpecWithServerDefaults(tampered.Spec)
	tampered.Spec.AgentRef = &corev1alpha1.AgentReference{Name: "someone-else"}
	if matchingReviewRetryTask(tampered, desired) {
		t.Fatal("a retry task bound to a different agent must not match")
	}
	// The conflict diagnostic is persisted into the scan run and the
	// RepositoryScan condition, so it names differing field paths only and
	// never echoes spec values from either Task.
	leaky := build()
	leaky.Spec = taskSpecWithServerDefaults(leaky.Spec)
	leaky.Spec.Env = []corev1.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghp_secretvalue"}}
	leaky.Spec.Workspace.GitRepo = "https://user:hunter2@github.com/o/r"
	got := reviewRetryTaskMismatch(leaky, desired)
	for _, secret := range []string{"ghp_secretvalue", "hunter2", "github.com"} {
		if strings.Contains(got, secret) {
			t.Fatalf("mismatch diagnostic %q leaks spec value %q", got, secret)
		}
	}
	for _, path := range []string{"env", "workspace.gitRepo"} {
		if !strings.Contains(got, path) {
			t.Fatalf("mismatch diagnostic %q should name field path %q", got, path)
		}
	}
	rewired := build()
	rewired.Spec = taskSpecWithServerDefaults(rewired.Spec)
	rewired.Spec.Workspace.GitRepo = "https://github.com/o/other"
	if matchingReviewRetryTask(rewired, desired) {
		t.Fatal("a retry task with a different workspace must not match")
	}
	attempt := build()
	attempt.Spec = taskSpecWithServerDefaults(attempt.Spec)
	attempt.Annotations["orka.ai/security-review-attempt"] = "0"
	if matchingReviewRetryTask(attempt, desired) {
		t.Fatal("a task with a different attempt annotation must not match")
	}
}
