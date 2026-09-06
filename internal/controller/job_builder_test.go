/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	testControllerURL                     = "http://orka-controller.orka-system.svc:8080"
	testGitCredentials                    = "git-credentials"
	testOpenAIAPIKey                      = "OPENAI_API_KEY"
	testBusyboxImage                      = "busybox:latest"
	testAgentSecretName                   = "agent-secret"
	testProviderSecretName                = "provider-secret"
	testProviderBaseURL                   = "https://api.example.test/v1"
	testAIWorkerServiceAccountName        = "release-ai-worker"
	testVendorWorkerServiceAccountName    = "release-vendor-worker"
	testContainerWorkerServiceAccountName = "release-container-worker"
	testFalseValue                        = "false"
)

const (
	testTask               = "test-task"
	defaultNS              = "default"
	envAIProviderKey       = "ORKA_AI_PROVIDER"
	testCodexSandboxMode   = "danger-full-access"
	testTransactionID      = "txn-123"
	testNodeLabelKey       = "sandbox-runtime"
	testNodeValueKata      = "kata"
	testNodeValueGVisor    = "gvisor"
	testRuntimeClassKata   = "kata-qemu"
	testRuntimeClassGVisor = "gvisor"
	testTTSAudience        = "tts-audience"
	testTaskConfigVolume   = "task-secrets"
)

func TestEnvFlagEnabledAliases(t *testing.T) {
	const name = "ORKA_TEST_FLAG"

	for _, value := range []string{"1", "true", "TRUE", "t", "yes", "y", "on"} {
		t.Run("true_"+value, func(t *testing.T) {
			t.Setenv(name, value)
			if !envFlagEnabled(name) {
				t.Fatalf("envFlagEnabled(%q) = false, want true", value)
			}
		})
	}

	for _, value := range []string{"", "0", "false", "FALSE", "f", "no", "n", "off", "invalid"} {
		t.Run("false_"+value, func(t *testing.T) {
			t.Setenv(name, value)
			if envFlagEnabled(name) {
				t.Fatalf("envFlagEnabled(%q) = true, want false", value)
			}
		})
	}
}

func TestNewJobBuilderSnapshotsDirectRuntimeSecretPolicy(t *testing.T) {
	t.Setenv(directProviderSecretsEnvVar, "true")
	t.Setenv(directSecretMountsEnvVar, "true")
	t.Setenv(directGitCredentialsEnvVar, "true")

	builder := setupJobBuilder()
	t.Setenv(directProviderSecretsEnvVar, "false")
	t.Setenv(directSecretMountsEnvVar, "false")
	t.Setenv(directGitCredentialsEnvVar, "false")

	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type:  corev1alpha1.TaskTypeContainer,
		Image: testBusyboxImage,
	}}
	if !builder.directProviderSecretsAllowed(task) {
		t.Fatal("provider secret policy should be captured when JobBuilder is constructed")
	}
	if !builder.directSecretMountsAllowed(task) {
		t.Fatal("secret mount policy should be captured when JobBuilder is constructed")
	}
	if !builder.directGitCredentialsAllowed(task) {
		t.Fatal("git credential policy should be captured when JobBuilder is constructed")
	}
}

func setupJobBuilder() *JobBuilder {
	scheme := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	b := NewJobBuilder(fakeClient)
	b.ControllerURL = testControllerURL
	b.ControllerMode = executionmode.HarnessV2
	return b
}

// buildEnvVars builds the environment variables for the container
func (b *JobBuilder) buildEnvVars(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) []corev1.EnvVar {
	return b.buildEnvVarsWithOptions(ctx, task, agent, provider, JobBuildOptions{})
}

func assertServiceAccountName(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("ServiceAccountName = %s, want %s", got, want)
	}
}

func assertAutomountServiceAccountToken(t *testing.T, got *bool, want bool) {
	t.Helper()
	if got == nil {
		t.Fatalf("AutomountServiceAccountToken is nil, want %v", want)
	}
	if *got != want {
		t.Errorf("AutomountServiceAccountToken = %v, want %v", *got, want)
	}
}

func TestNewJobBuilder(t *testing.T) {
	builder := setupJobBuilder()
	if builder == nil {
		t.Fatal("NewJobBuilder returned nil")
	}
	if builder.AIWorkerImage != DefaultAIWorkerImage {
		t.Errorf("AIWorkerImage = %s, want %s", builder.AIWorkerImage, DefaultAIWorkerImage)
	}
	if builder.GeneralWorkerImage != DefaultGeneralWorkerImage {
		t.Errorf("GeneralWorkerImage = %s, want %s", builder.GeneralWorkerImage, DefaultGeneralWorkerImage)
	}
	if builder.AIWorkerServiceAccountName != AIWorkerServiceAccount {
		t.Errorf("AIWorkerServiceAccountName = %s, want %s", builder.AIWorkerServiceAccountName, AIWorkerServiceAccount)
	}
	if builder.ContainerWorkerServiceAccountName != ContainerWorkerServiceAccount {
		t.Errorf("ContainerWorkerServiceAccountName = %s, want %s", builder.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount)
	}
}

func TestJobBuilder_Build_UsesConfiguredWorkerServiceAccountNames(t *testing.T) {
	builder := setupJobBuilder()
	builder.AIWorkerServiceAccountName = testAIWorkerServiceAccountName
	builder.ContainerWorkerServiceAccountName = testContainerWorkerServiceAccountName

	tests := []struct {
		name string
		task *corev1alpha1.Task
		want string
	}{
		{
			name: "ai",
			task: &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "ai-task", Namespace: defaultNS},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAI,
					AI: &corev1alpha1.AISpec{
						Provider: "anthropic",
						Model:    "test-model",
						Prompt:   "test prompt",
					},
				},
			},
			want: testAIWorkerServiceAccountName,
		},
		{
			name: "container",
			task: &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "container-task", Namespace: defaultNS},
				Spec: corev1alpha1.TaskSpec{
					Type:  corev1alpha1.TaskTypeContainer,
					Image: testBusyboxImage,
				},
			},
			want: testContainerWorkerServiceAccountName,
		},
		{
			name: "runtime auth only",
			task: &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "runtime-auth-only-task",
					Namespace: defaultNS,
					Annotations: map[string]string{
						labels.AnnotationAgentRuntimeAuthOnly: scheduledRunLabelValue,
					},
				},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAI,
					AI: &corev1alpha1.AISpec{
						Provider: "anthropic",
						Model:    "test-model",
						Prompt:   "test prompt",
					},
				},
			},
			want: testContainerWorkerServiceAccountName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := builder.Build(context.Background(), tt.task, nil, nil)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			assertServiceAccountName(t, job.Spec.Template.Spec.ServiceAccountName, tt.want)
		})
	}
}

func TestJobBuilder_WorkerServiceAccountForTask_EmptyNamesUseDefaults(t *testing.T) {
	builder := setupJobBuilder()
	builder.AIWorkerServiceAccountName = ""
	builder.ContainerWorkerServiceAccountName = ""

	tests := []struct {
		name     string
		taskType corev1alpha1.TaskType
		want     string
	}{
		{name: "ai", taskType: corev1alpha1.TaskTypeAI, want: AIWorkerServiceAccount},
		{name: "container", taskType: corev1alpha1.TaskTypeContainer, want: ContainerWorkerServiceAccount},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: tt.taskType}}
			if got := builder.workerServiceAccountForTask(task); got != tt.want {
				t.Fatalf("workerServiceAccountForTask() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestJobBuilder_Build_ContainerTask(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   testBusyboxImage,
			Command: []string{"echo"},
			Args:    []string{"hello"},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if job == nil {
		t.Fatal("Build() returned nil job")
	}

	assertServiceAccountName(t, job.Spec.Template.Spec.ServiceAccountName, ContainerWorkerServiceAccount)
	assertAutomountServiceAccountToken(t, job.Spec.Template.Spec.AutomountServiceAccountToken, false)

	// Verify container settings
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != testBusyboxImage {
		t.Errorf("Image = %s, want %s", container.Image, testBusyboxImage)
	}
	if len(container.Command) != 1 || container.Command[0] != "echo" {
		t.Errorf("Command = %v, want [echo]", container.Command)
	}
	if len(container.Args) != 1 || container.Args[0] != "hello" {
		t.Errorf("Args = %v, want [hello]", container.Args)
	}
}

func TestJobBuilder_Build_GeneralContainerWorkerAutomountsTokenForCallback(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "general-container-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Command: []string{"echo"},
			Args:    []string{"hello"},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	assertServiceAccountName(t, job.Spec.Template.Spec.ServiceAccountName, ContainerWorkerServiceAccount)
	assertAutomountServiceAccountToken(t, job.Spec.Template.Spec.AutomountServiceAccountToken, true)
}

func TestJobBuilder_Build_PropagatesTransactionMetadata(t *testing.T) {
	builder := setupJobBuilder()
	builder.EnforceTransactionCredentialAuth = true
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "busybox:latest",
			Env: []corev1.EnvVar{
				{Name: workerenv.TransactionScope, Value: "spoofed"},
				{Name: workerenv.TransactionScopes, Value: "spoofed"},
				{Name: workerenv.TransactionCredentialAuthorizationEnforced, Value: testFalseValue},
			},
			Transaction: &corev1alpha1.TaskTransaction{
				Profile:                "transaction-token",
				ID:                     testTransactionID,
				Issuer:                 "https://issuer.example.test",
				Subject:                "spiffe://example.test/ns/default/sa/client",
				RequestingWorkload:     "spiffe://example.test/ns/default/sa/client",
				Scope:                  "read write",
				Scopes:                 []string{"read", "write"},
				ContextDigest:          "sha256:context",
				RequesterContextDigest: "sha256:requester",
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	for name, meta := range map[string]metav1.ObjectMeta{
		"job":          job.ObjectMeta,
		"pod template": job.Spec.Template.ObjectMeta,
	} {
		if meta.Labels[labels.LabelTransactionID] != labels.SelectorValue(testTransactionID) {
			t.Fatalf("%s transaction label = %q, want txn-123", name, meta.Labels[labels.LabelTransactionID])
		}
		if meta.Labels[labels.LabelAuthProfile] != "transaction-token" {
			t.Fatalf("%s auth profile label = %q, want transaction-token", name, meta.Labels[labels.LabelAuthProfile])
		}
		if meta.Annotations[labels.AnnotationTransactionID] != testTransactionID {
			t.Fatalf("%s transaction annotation = %q, want txn-123", name, meta.Annotations[labels.AnnotationTransactionID])
		}
		if meta.Annotations[labels.AnnotationTransactionContextDigest] != "sha256:context" {
			t.Fatalf("%s context digest annotation = %q", name, meta.Annotations[labels.AnnotationTransactionContextDigest])
		}
	}

	envVars := job.Spec.Template.Spec.Containers[0].Env
	wantEnv := map[string]string{
		workerenv.TransactionID:                              testTransactionID,
		workerenv.TransactionProfile:                         "transaction-token",
		workerenv.TransactionIssuer:                          "https://issuer.example.test",
		workerenv.TransactionSubject:                         "spiffe://example.test/ns/default/sa/client",
		workerenv.TransactionRequestingWorkload:              "spiffe://example.test/ns/default/sa/client",
		workerenv.TransactionScope:                           "read write",
		workerenv.TransactionScopes:                          "read,write",
		workerenv.TransactionContextDigest:                   "sha256:context",
		workerenv.TransactionRequesterContextDigest:          "sha256:requester",
		workerenv.TransactionCredentialAuthorizationEnforced: booleanTrueValue,
	}
	for name, want := range wantEnv {
		env, ok := findEnvVar(envVars, name)
		if !ok {
			t.Fatalf("missing env var %s", name)
		}
		if env.Value != want {
			t.Fatalf("%s = %q, want %q", name, env.Value, want)
		}
		if count := countEnvVars(envVars, name); count != 1 {
			t.Fatalf("%s appeared %d times, want exactly once", name, count)
		}
	}
}

func TestJobBuilder_Build_MountsTransactionTokenSecret(t *testing.T) {
	builder := setupJobBuilder()
	builder.ContextTokenTTSEndpoint = "https://tts.example.test"
	builder.ContextTokenTTSTokenSource = contexttoken.TTSTokenSourceIncoming
	builder.ContextTokenTTSAudience = testTTSAudience
	builder.ContextTokenTTSTimeout = "7s"
	builder.ContextTokenSubjectTokenType = "urn:ietf:params:oauth:token-type:txn_token"
	builder.ContextTokenChildScope = "orka:agents:run"
	builder.ContextTokenOutboundScope = "orka:tools:use"
	builder.ContextTokenChildTokenTTL = "3m"
	builder.ContextTokenToolTokenTTL = "30s"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			Annotations: map[string]string{
				labels.AnnotationTransactionTokenSecret: "child-tx-token",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "busybox:latest",
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	var foundVolume bool
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "transaction-token" && volume.Secret != nil && volume.Secret.SecretName == "child-tx-token" {
			if volume.Secret.DefaultMode == nil {
				t.Fatal("transaction-token secret volume DefaultMode is nil, want 0400")
			}
			if *volume.Secret.DefaultMode != int32(0400) {
				t.Fatalf("transaction-token secret volume DefaultMode = %#o, want 0400", *volume.Secret.DefaultMode)
			}
			foundVolume = true
		}
	}
	if !foundVolume {
		t.Fatalf("expected transaction-token secret volume, got %#v", job.Spec.Template.Spec.Volumes)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if _, ok := findEnvVar(container.Env, workerenv.TransactionTokenFile); !ok {
		t.Fatalf("missing %s env var", workerenv.TransactionTokenFile)
	}
	if _, ok := findEnvVar(container.Env, workerenv.ContextTokenSubjectTokenFile); !ok {
		t.Fatalf("missing %s env var", workerenv.ContextTokenSubjectTokenFile)
	}
	for name, want := range map[string]string{
		workerenv.ContextTokenTTSEndpoint:      "https://tts.example.test",
		workerenv.ContextTokenTTSTokenSource:   contexttoken.TTSTokenSourceIncoming,
		workerenv.ContextTokenTTSAudience:      testTTSAudience,
		workerenv.ContextTokenTTSTimeout:       "7s",
		workerenv.ContextTokenSubjectTokenType: "urn:ietf:params:oauth:token-type:txn_token",
		workerenv.ContextTokenChildScope:       "orka:agents:run",
		workerenv.ContextTokenOutboundScope:    "orka:tools:use",
		workerenv.ContextTokenChildTokenTTL:    "3m",
		workerenv.ContextTokenToolTokenTTL:     "30s",
	} {
		got, ok := findEnvVar(container.Env, name)
		if !ok || got.Value != want {
			t.Fatalf("env %s = %#v, want %q", name, got, want)
		}
	}
}

func TestJobBuilder_AddTransactionTokenSecretExposesToAllContainers(t *testing.T) {
	builder := setupJobBuilder()
	builder.ContextTokenTTSTokenSource = contexttoken.TTSTokenSourceIncoming
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				labels.AnnotationTransactionTokenSecret: "child-tx-token",
			},
		},
	}
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "worker"},
						{Name: "sidecar"},
					},
				},
			},
		},
	}

	builder.addTransactionTokenSecret(job, task)

	for _, container := range job.Spec.Template.Spec.Containers {
		if !hasVolumeMount(container.VolumeMounts, "transaction-token") {
			t.Fatalf("%s container missing transaction-token mount: %#v", container.Name, container.VolumeMounts)
		}
		for _, name := range []string{workerenv.TransactionTokenFile, workerenv.ContextTokenSubjectTokenFile} {
			got, ok := findEnvVar(container.Env, name)
			if !ok {
				t.Fatalf("%s container missing %s env var", container.Name, name)
			}
			if got.Value != "/var/run/orka/transaction-token/token" {
				t.Fatalf("%s container %s = %q, want token file path", container.Name, name, got.Value)
			}
		}
	}
}

func TestJobBuilder_Build_InjectsContextTokenTTSConfigWithoutTransactionTokenSecret(t *testing.T) {
	builder := setupJobBuilder()
	builder.ContextTokenTTSEndpoint = "https://tts.example.test"
	builder.ContextTokenTTSTokenSource = contexttoken.TTSTokenSourceServiceAccount
	builder.ContextTokenTTSAudience = testTTSAudience
	builder.ContextTokenTTSTimeout = "7s"
	builder.ContextTokenSubjectTokenType = "urn:ietf:params:oauth:token-type:txn_token"
	builder.ContextTokenChildScope = "orka:agents:run"
	builder.ContextTokenOutboundScope = "orka:tools:use"
	builder.ContextTokenChildTokenTTL = "3m"
	builder.ContextTokenToolTokenTTL = "30s"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "busybox:latest",
			Transaction: &corev1alpha1.TaskTransaction{
				ID:     "txn-123",
				Scope:  "orka:tools:use",
				Scopes: []string{"orka:tools:use"},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	for name, want := range map[string]string{
		workerenv.ContextTokenTTSEndpoint:      "https://tts.example.test",
		workerenv.ContextTokenTTSTokenSource:   contexttoken.TTSTokenSourceServiceAccount,
		workerenv.ContextTokenTTSAudience:      testTTSAudience,
		workerenv.ContextTokenTTSTimeout:       "7s",
		workerenv.ContextTokenSubjectTokenType: "urn:ietf:params:oauth:token-type:txn_token",
		workerenv.ContextTokenChildScope:       "orka:agents:run",
		workerenv.ContextTokenOutboundScope:    "orka:tools:use",
		workerenv.ContextTokenChildTokenTTL:    "3m",
		workerenv.ContextTokenToolTokenTTL:     "30s",
	} {
		got, ok := findEnvVar(container.Env, name)
		if !ok || got.Value != want {
			t.Fatalf("env %s = %#v, want %q", name, got, want)
		}
	}
	if hasVolume(job.Spec.Template.Spec.Volumes, "transaction-token") {
		t.Fatalf("unexpected transaction-token volume without transaction token secret annotation: %#v", job.Spec.Template.Spec.Volumes)
	}
	for _, name := range []string{workerenv.TransactionTokenFile, workerenv.ContextTokenSubjectTokenFile} {
		if got, ok := findEnvVar(container.Env, name); ok {
			t.Fatalf("unexpected env %s without transaction token secret annotation: %#v", name, got)
		}
	}
}

func TestJobBuilder_Build_AITask(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider: "anthropic",
				Model:    "claude-3-5-sonnet",
				Prompt:   "Hello",
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	assertServiceAccountName(t, job.Spec.Template.Spec.ServiceAccountName, AIWorkerServiceAccount)
	assertAutomountServiceAccountToken(t, job.Spec.Template.Spec.AutomountServiceAccountToken, true)

	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != DefaultAIWorkerImage {
		t.Errorf("Image = %s, want %s", container.Image, DefaultAIWorkerImage)
	}
}

func TestJobBuilder_Build_WithTimeout(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Timeout: &metav1.Duration{Duration: 5 * time.Minute},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Error("ActiveDeadlineSeconds should be set")
	}
	if *job.Spec.ActiveDeadlineSeconds != 300 {
		t.Errorf("ActiveDeadlineSeconds = %d, want 300", *job.Spec.ActiveDeadlineSeconds)
	}
}

func TestJobBuilder_Build_WithSession(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "test-session", PromptIncluded: true, ThroughMessageID: "message-1",
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	assertAutomountServiceAccountToken(t, job.Spec.Template.Spec.AutomountServiceAccountToken, true)

	// Verify session-data emptyDir volume
	hasSessionVolume := false
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.Name == "session-data" {
			hasSessionVolume = true
			if vol.EmptyDir == nil {
				t.Error("session-data volume should be emptyDir")
			}
			break
		}
	}
	if !hasSessionVolume {
		t.Error("Job should have session-data volume")
	}

	// Verify init container exists
	if len(job.Spec.Template.Spec.InitContainers) == 0 {
		t.Fatal("Job should have init container for session fetch")
	}
	initContainer := job.Spec.Template.Spec.InitContainers[0]
	if initContainer.Name != "fetch-session" {
		t.Errorf("Init container name = %s, want fetch-session", initContainer.Name)
	}
	if !hasVolume(job.Spec.Template.Spec.Volumes, "session-token") ||
		!hasVolumeMount(initContainer.VolumeMounts, "session-token") {
		t.Fatal("fetch-session init container must receive its projected session token")
	}
	if hasVolumeMount(job.Spec.Template.Spec.Containers[0].VolumeMounts, "session-token") {
		t.Fatal("main worker container must not mount the dedicated session token")
	}
	transcriptURL, ok := findEnvVar(initContainer.Env, sessionTranscriptURLEnv)
	if !ok || !strings.Contains(transcriptURL.Value, "taskName="+testTask) {
		t.Fatalf("%s env = %#v, want task-aware transcript URL", sessionTranscriptURLEnv, transcriptURL)
	}
	if required, ok := findEnvVar(initContainer.Env, sessionTranscriptRequiredEnv); !ok || required.Value != scheduledRunLabelValue {
		t.Fatalf("%s env = %#v, want true", sessionTranscriptRequiredEnv, required)
	}
	if attempts, ok := findEnvVar(initContainer.Env, sessionTranscriptMaxAttemptsEnv); !ok || attempts.Value != "300" {
		t.Fatalf("%s env = %#v, want 300", sessionTranscriptMaxAttemptsEnv, attempts)
	}
	for _, want := range []string{
		"transcript.jsonl.tmp", `mv "$TMP" "$FINAL"`, "attempt=$((attempt + 1))",
		`SA_JWT=$(cat "$TOKEN_FILE")`,
		`"$ORKA_SESSION_TRANSCRIPT_URL"`, `"$ORKA_SESSION_TRANSCRIPT_MAX_ATTEMPTS"`,
		`if [ "$ORKA_SESSION_TRANSCRIPT_REQUIRED" = "true" ]`, "exit 1",
	} {
		if !strings.Contains(initContainer.Command[2], want) {
			t.Fatalf("fetch-session command = %q, want %q", initContainer.Command[2], want)
		}
	}
	if env, ok := findEnvVar(job.Spec.Template.Spec.Containers[0].Env, workerenv.SessionPromptIncluded); !ok || env.Value != scheduledRunLabelValue {
		t.Fatalf("%s env = %#v, want true", workerenv.SessionPromptIncluded, env)
	}
}

func TestSessionTranscriptFetchCommandAllowsEmptyFallbackOnlyWhenPromptIsNotIncluded(t *testing.T) {
	command := sessionTranscriptFetchCommand()
	shortTimeout := &metav1.Duration{Duration: 2 * time.Second}
	longTimeout := &metav1.Duration{Duration: 2 * time.Minute}
	if sessionTranscriptMaxAttempts(false, nil) != "5" || sessionTranscriptMaxAttempts(true, nil) != "300" ||
		sessionTranscriptMaxAttempts(false, shortTimeout) != "1" ||
		sessionTranscriptMaxAttempts(true, shortTimeout) != "1" ||
		sessionTranscriptMaxAttempts(true, longTimeout) != "119" ||
		!strings.Contains(command, `: > "$TMP"`) || !strings.Contains(command, `mv "$TMP" "$FINAL"`) {
		t.Fatalf("sessionTranscriptFetchCommand() = %q", command)
	}
}

func TestJobBuilder_Build_CustomContainerWithSessionKeepsTokenOutOfMainContainer(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-session-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   testBusyboxImage,
			Command: []string{"echo"},
			Args:    []string{"hello"},
			SessionRef: &corev1alpha1.SessionReference{
				Name: "test-session",
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	assertServiceAccountName(t, job.Spec.Template.Spec.ServiceAccountName, ContainerWorkerServiceAccount)
	assertAutomountServiceAccountToken(t, job.Spec.Template.Spec.AutomountServiceAccountToken, false)
	if !hasVolume(job.Spec.Template.Spec.Volumes, "session-token") {
		t.Fatal("session-token projected volume should be present for the fetch init container")
	}
	if hasVolumeMount(job.Spec.Template.Spec.Containers[0].VolumeMounts, "session-token") {
		t.Fatal("main custom container should not mount the session token")
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("init container count = %d, want 1", len(job.Spec.Template.Spec.InitContainers))
	}
	if !hasVolumeMount(job.Spec.Template.Spec.InitContainers[0].VolumeMounts, "session-token") {
		t.Fatal("fetch-session init container should mount the session token")
	}
	if required, ok := findEnvVar(job.Spec.Template.Spec.InitContainers[0].Env, sessionTranscriptRequiredEnv); !ok || required.Value != "false" {
		t.Fatalf("%s env = %#v, want false", sessionTranscriptRequiredEnv, required)
	}
	if attempts, ok := findEnvVar(job.Spec.Template.Spec.InitContainers[0].Env, sessionTranscriptMaxAttemptsEnv); !ok || attempts.Value != "5" {
		t.Fatalf("%s env = %#v, want 5", sessionTranscriptMaxAttemptsEnv, attempts)
	}
}

func TestJobBuilder_SessionURLIsNotInterpolatedIntoShell(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "x$(touch /tmp/session-injection)",
			},
		},
	}
	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	initContainer := job.Spec.Template.Spec.InitContainers[0]
	if strings.Contains(initContainer.Command[2], "session-injection") {
		t.Fatalf("session value was interpolated into shell command: %q", initContainer.Command[2])
	}
	if transcriptURL, ok := findEnvVar(initContainer.Env, sessionTranscriptURLEnv); !ok || !strings.Contains(transcriptURL.Value, "session-injection") {
		t.Fatalf("%s env = %#v", sessionTranscriptURLEnv, transcriptURL)
	}
}

func TestJobBuilder_Build_AppliesAgentExecutionDefaults(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Execution: &corev1alpha1.ExecutionSpec{
				RuntimeClassName: testRuntimeClassKata,
				NodeSelector: map[string]string{
					testNodeLabelKey: testNodeValueKata,
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      testNodeLabelKey,
						Operator: corev1.TolerationOpEqual,
						Value:    testNodeValueKata,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      testNodeLabelKey,
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{testNodeValueKata},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	podSpec := job.Spec.Template.Spec
	if podSpec.RuntimeClassName == nil || *podSpec.RuntimeClassName != testRuntimeClassKata {
		t.Fatalf("RuntimeClassName = %v, want %s", podSpec.RuntimeClassName, testRuntimeClassKata)
	}
	if got := podSpec.NodeSelector[testNodeLabelKey]; got != testNodeValueKata {
		t.Errorf("NodeSelector[%s] = %q, want %q", testNodeLabelKey, got, testNodeValueKata)
	}
	if len(podSpec.Tolerations) != 1 {
		t.Fatalf("Tolerations len = %d, want 1", len(podSpec.Tolerations))
	}
	if podSpec.Tolerations[0].Value != testNodeValueKata {
		t.Errorf("Tolerations[0].Value = %q, want %q", podSpec.Tolerations[0].Value, testNodeValueKata)
	}
	if podSpec.Affinity == nil || podSpec.Affinity.NodeAffinity == nil {
		t.Fatal("Affinity.NodeAffinity should not be nil")
	}

	podSpec.NodeSelector[testNodeLabelKey] = "modified"
	if agent.Spec.Execution.NodeSelector[testNodeLabelKey] != testNodeValueKata {
		t.Error("Build should copy agent execution nodeSelector instead of aliasing it")
	}
}

func TestJobBuilder_Build_TaskExecutionOverridesAgentExecution(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
			Execution: &corev1alpha1.ExecutionSpec{
				RuntimeClassName: testRuntimeClassGVisor,
				NodeSelector: map[string]string{
					testNodeLabelKey: testNodeValueGVisor,
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      testNodeLabelKey,
						Operator: corev1.TolerationOpEqual,
						Value:    testNodeValueGVisor,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Affinity: &corev1.Affinity{
					PodAntiAffinity: &corev1.PodAntiAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
							{
								Weight: 1,
								PodAffinityTerm: corev1.PodAffinityTerm{
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					},
				},
			},
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Execution: &corev1alpha1.ExecutionSpec{
				RuntimeClassName: testRuntimeClassKata,
				NodeSelector: map[string]string{
					testNodeLabelKey: "kata",
					"dedicated":      "true",
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      testNodeLabelKey,
						Operator: corev1.TolerationOpEqual,
						Value:    testNodeValueKata,
						Effect:   corev1.TaintEffectNoSchedule,
					},
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{},
				},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	podSpec := job.Spec.Template.Spec
	if podSpec.RuntimeClassName == nil || *podSpec.RuntimeClassName != testRuntimeClassGVisor {
		t.Fatalf("RuntimeClassName = %v, want %s", podSpec.RuntimeClassName, testRuntimeClassGVisor)
	}
	if len(podSpec.NodeSelector) != 1 {
		t.Fatalf("NodeSelector len = %d, want 1", len(podSpec.NodeSelector))
	}
	if got := podSpec.NodeSelector[testNodeLabelKey]; got != testNodeValueGVisor {
		t.Errorf("NodeSelector[%s] = %q, want %q", testNodeLabelKey, got, testNodeValueGVisor)
	}
	if len(podSpec.Tolerations) != 1 {
		t.Fatalf("Tolerations len = %d, want 1", len(podSpec.Tolerations))
	}
	if podSpec.Tolerations[0].Value != testNodeValueGVisor {
		t.Errorf("Tolerations[0].Value = %q, want %q", podSpec.Tolerations[0].Value, testNodeValueGVisor)
	}
	if podSpec.Affinity == nil || podSpec.Affinity.PodAntiAffinity == nil {
		t.Fatal("Affinity.PodAntiAffinity should not be nil")
	}
	if podSpec.Affinity.NodeAffinity != nil {
		t.Error("Task affinity should replace agent affinity instead of merging with it")
	}
}

func TestResolveExecution_IgnoresWorkspace(t *testing.T) {
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					Enabled:     true,
					TemplateRef: &corev1alpha1.WorkspaceTemplateReference{Name: "default"},
				},
			},
		},
	}

	if execution := resolveExecution(task, nil); execution != nil {
		t.Fatalf("resolveExecution() = %#v, want nil when only workspace is set", execution)
	}

	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Execution: &corev1alpha1.ExecutionSpec{
				RuntimeClassName: testRuntimeClassKata,
			},
		},
	}

	execution := resolveExecution(task, agent)
	if execution == nil {
		t.Fatal("resolveExecution() returned nil, want agent execution")
	}
	if execution.RuntimeClassName != testRuntimeClassKata {
		t.Fatalf("RuntimeClassName = %q, want %q", execution.RuntimeClassName, testRuntimeClassKata)
	}
	if execution.Workspace != nil {
		t.Fatalf("Workspace = %#v, want nil", execution.Workspace)
	}
}

func TestJobBuilder_buildPodSecurityContext(t *testing.T) {
	builder := setupJobBuilder()
	psc := builder.buildPodSecurityContext()

	if psc == nil {
		t.Fatal("buildPodSecurityContext returned nil")
	}
	if psc.RunAsNonRoot == nil || !*psc.RunAsNonRoot {
		t.Error("RunAsNonRoot should be true")
	}
	if psc.RunAsUser == nil || *psc.RunAsUser != 1000 {
		t.Errorf("RunAsUser = %v, want 1000", psc.RunAsUser)
	}
}

func TestJobBuilder_buildContainerSecurityContext(t *testing.T) {
	builder := setupJobBuilder()
	csc := builder.buildContainerSecurityContext()

	if csc == nil {
		t.Fatal("buildContainerSecurityContext returned nil")
	}
	if csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation {
		t.Error("AllowPrivilegeEscalation should be false")
	}
	if csc.ReadOnlyRootFilesystem == nil || !*csc.ReadOnlyRootFilesystem {
		t.Error("ReadOnlyRootFilesystem should be true")
	}
	if csc.Capabilities == nil || len(csc.Capabilities.Drop) == 0 {
		t.Error("Should drop all capabilities")
	}
}

func TestJobBuilder_buildResources_TaskResources(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
			},
		},
	}

	resources := builder.buildResources(task, nil)
	cpuReq := resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "200m" {
		t.Errorf("CPU request = %s, want 200m", cpuReq.String())
	}
}

func TestJobBuilder_buildResources_AgentResources(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{}, // No resources
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("300m"),
				},
			},
		},
	}

	resources := builder.buildResources(task, agent)
	cpuReq := resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "300m" {
		t.Errorf("CPU request = %s, want 300m", cpuReq.String())
	}
}

func TestJobBuilder_buildResources_Defaults(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{}, // No resources
	}

	resources := builder.buildResources(task, nil)
	cpuReq := resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "100m" {
		t.Errorf("CPU request = %s, want 100m (default)", cpuReq.String())
	}
	memReq := resources.Requests[corev1.ResourceMemory]
	if memReq.String() != "512Mi" {
		t.Errorf("memory request = %s, want 512Mi (default)", memReq.String())
	}
	cpuLim := resources.Limits[corev1.ResourceCPU]
	if cpuLim.String() != "1" {
		t.Errorf("CPU limit = %s, want 1 (default)", cpuLim.String())
	}
	memLim := resources.Limits[corev1.ResourceMemory]
	if memLim.String() != "2Gi" {
		t.Errorf("memory limit = %s, want 2Gi (default; smaller values OOMKilled real Go/Node test suites)", memLim.String())
	}
}

func TestJobBuilder_buildResources_DefaultsAreIndependent(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{}}

	first := builder.buildResources(task, nil)
	second := builder.buildResources(task, nil)

	first.Requests[corev1.ResourceCPU] = resource.MustParse("900m")
	delete(first.Limits, corev1.ResourceMemory)

	cpuReq := second.Requests[corev1.ResourceCPU]
	if got := cpuReq.String(); got != "100m" {
		t.Fatalf("second default CPU request = %s after mutating first result, want 100m", got)
	}
	if _, ok := second.Limits[corev1.ResourceMemory]; !ok {
		t.Fatal("second default memory limit disappeared after mutating first result")
	}
}

func TestJobBuilder_buildEnvVars(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
			Env: []corev1.EnvVar{
				{Name: "CUSTOM_VAR", Value: "custom-value"},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)

	// Check required env vars
	hasTaskName := false
	hasTaskNamespace := false
	hasResultEndpoint := false
	hasControllerURL := false
	hasCustomVar := false

	for _, env := range envVars {
		switch env.Name {
		case TaskNameEnvVar:
			hasTaskName = true
			if env.Value != testTask {
				t.Errorf("ORKA_TASK_NAME = %s, want test-task", env.Value)
			}
		case TaskNamespaceEnvVar:
			hasTaskNamespace = true
			if env.Value != defaultNS {
				t.Errorf("ORKA_TASK_NAMESPACE = %s, want default", env.Value)
			}
		case ResultEndpointEnvVar:
			hasResultEndpoint = true
		case ControllerURLEnvVar:
			hasControllerURL = true
		case "CUSTOM_VAR":
			hasCustomVar = true
			if env.Value != "custom-value" {
				t.Errorf("CUSTOM_VAR = %s, want custom-value", env.Value)
			}
		}
	}

	if !hasTaskName {
		t.Error("Missing ORKA_TASK_NAME")
	}
	if !hasTaskNamespace {
		t.Error("Missing ORKA_TASK_NAMESPACE")
	}
	if !hasResultEndpoint {
		t.Error("Missing ORKA_RESULT_ENDPOINT")
	}
	if !hasControllerURL {
		t.Error("Missing ORKA_CONTROLLER_URL")
	}
	if !hasCustomVar {
		t.Error("Missing CUSTOM_VAR")
	}
}

func TestJobBuilder_buildEnvVars_AITask(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider:     "anthropic",
				Model:        "claude-3-5-sonnet",
				Prompt:       "Hello",
				SystemPrompt: "You are helpful",
				Tools:        []string{"web_search"},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)

	// Check AI-specific env vars
	hasProvider := false
	hasModel := false
	hasPrompt := false

	for _, env := range envVars {
		switch env.Name {
		case envAIProviderKey:
			hasProvider = true
			if env.Value != "anthropic" {
				t.Errorf("ORKA_AI_PROVIDER = %s, want anthropic", env.Value)
			}
		case "ORKA_AI_MODEL":
			hasModel = true
			if env.Value != "claude-3-5-sonnet" {
				t.Errorf("ORKA_AI_MODEL = %s, want claude-3-5-sonnet", env.Value)
			}
		case "ORKA_AI_PROMPT":
			hasPrompt = true
			if env.Value != "Hello" {
				t.Errorf("ORKA_AI_PROMPT = %s, want Hello", env.Value)
			}
		}
	}

	if !hasProvider {
		t.Error("Missing ORKA_AI_PROVIDER")
	}
	if !hasModel {
		t.Error("Missing ORKA_AI_MODEL")
	}
	if !hasPrompt {
		t.Error("Missing ORKA_AI_PROMPT")
	}
}

func TestJobBuilder_buildEnvVars_WithAgent(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Hello from task",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
			SystemPrompt: &corev1alpha1.PromptSource{
				Inline: "You are an assistant",
			},
			Tools: []corev1alpha1.ToolReference{
				{Name: "code_exec"},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)

	// Agent values should be used when task doesn't specify them
	hasProvider := false
	hasModel := false

	for _, env := range envVars {
		switch env.Name {
		case envAIProviderKey:
			hasProvider = true
			if env.Value != "openai" {
				t.Errorf("ORKA_AI_PROVIDER = %s, want openai", env.Value)
			}
		case "ORKA_AI_MODEL":
			hasModel = true
			if env.Value != "gpt-4" {
				t.Errorf("ORKA_AI_MODEL = %s, want gpt-4", env.Value)
			}
		}
	}

	if !hasProvider {
		t.Error("Missing ORKA_AI_PROVIDER")
	}
	if !hasModel {
		t.Error("Missing ORKA_AI_MODEL")
	}
}

func TestJobBuilder_buildEnvVars_WithProvider(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Hello",
		},
	}
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			DefaultModel: "claude-3-opus",
			BaseURL:      "https://custom.api.com",
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, provider)

	hasBaseURL := false
	for _, env := range envVars {
		if env.Name == "ORKA_AI_BASE_URL" {
			hasBaseURL = true
			if env.Value != "https://custom.api.com" {
				t.Errorf("ORKA_AI_BASE_URL = %s, want https://custom.api.com", env.Value)
			}
		}
	}

	if !hasBaseURL {
		t.Error("Missing ORKA_AI_BASE_URL")
	}
}

func TestJobBuilder_buildEnvVars_ProviderCRDTypeOverridesModelProvider(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Hello",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Name:     "gpt-4",
			},
		},
	}
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type:    corev1alpha1.ProviderTypeAzureOpenAI,
			BaseURL: "https://my-azure.openai.azure.com",
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, provider)

	for _, env := range envVars {
		if env.Name == envAIProviderKey {
			if env.Value != string(corev1alpha1.ProviderTypeAzureOpenAI) {
				t.Errorf("ORKA_AI_PROVIDER = %s, want %s (Provider CRD type should override model.provider)",
					env.Value, corev1alpha1.ProviderTypeAzureOpenAI)
			}
			return
		}
	}
	t.Error("Missing ORKA_AI_PROVIDER")
}

func TestJobBuilder_buildContainer_ContainerWithoutImage(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "", // No image specified
		},
	}

	container := builder.buildContainerWithOptions(context.Background(), task, nil, nil, JobBuildOptions{})
	if container.Image != DefaultGeneralWorkerImage {
		t.Errorf("Image = %s, want %s", container.Image, DefaultGeneralWorkerImage)
	}
}

func TestConstants(t *testing.T) {
	if DefaultAIWorkerImage != "ghcr.io/orka-agents/orka/ai-worker:latest" {
		t.Errorf("DefaultAIWorkerImage = %s", DefaultAIWorkerImage)
	}
	if DefaultGeneralWorkerImage != "ghcr.io/orka-agents/orka/general-worker:latest" {
		t.Errorf("DefaultGeneralWorkerImage = %s", DefaultGeneralWorkerImage)
	}
	if ResultEndpointEnvVar != "ORKA_RESULT_ENDPOINT" {
		t.Errorf("ResultEndpointEnvVar = %s", ResultEndpointEnvVar)
	}
	if ControllerURLEnvVar != "ORKA_CONTROLLER_URL" {
		t.Errorf("ControllerURLEnvVar = %s", ControllerURLEnvVar)
	}
	if TaskNameEnvVar != "ORKA_TASK_NAME" {
		t.Errorf("TaskNameEnvVar = %s", TaskNameEnvVar)
	}
	if TaskNamespaceEnvVar != "ORKA_TASK_NAMESPACE" {
		t.Errorf("TaskNamespaceEnvVar = %s", TaskNamespaceEnvVar)
	}
}

func TestJobBuilder_buildEnvVars_WithCoordination(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Coordinate work",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:               true,
				MaxDepth:              3,
				MaxConcurrentChildren: 5,
				AllowedAgents: []corev1alpha1.AllowedAgent{
					{Name: "backend-dev"},
					{Name: "frontend-dev"},
				},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)

	tests := []struct {
		name  string
		value string
	}{
		{"ORKA_COORDINATION_ENABLED", "true"},
		{"ORKA_COORDINATION_MAX_DEPTH", "3"},
		{"ORKA_COORDINATION_MAX_CHILDREN", "5"},
		{"ORKA_COORDINATION_ALLOWED_AGENTS", "backend-dev,frontend-dev"},
		{"ORKA_COORDINATION_DEPTH", "0"},
		{workerenv.ControllerMode, string(executionmode.HarnessV2)},
	}
	for _, tt := range tests {
		env, found := findEnvVar(envVars, tt.name)
		if !found {
			t.Errorf("Missing %s", tt.name)
		} else if env.Value != tt.value {
			t.Errorf("%s = %s, want %s", tt.name, env.Value, tt.value)
		}
	}

	toolsEnv, found := findEnvVar(envVars, "ORKA_AI_TOOLS")
	if !found {
		t.Fatal("Missing ORKA_AI_TOOLS")
	}
	for _, tool := range []string{"delegate_task", "wait_for_tasks", "create_container_task", "list_pull_requests", "check_pr_review_marker", "check_pull_request_ci"} {
		if !strings.Contains(toolsEnv.Value, tool) {
			t.Errorf("ORKA_AI_TOOLS = %s, want to contain %s", toolsEnv.Value, tool)
		}
	}
}

func TestJobBuilder_buildEnvVars_WithCoordination_IncludesMemoryTools(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Coordinate work and remember important findings",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled: true,
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)

	toolsEnv, found := findEnvVar(envVars, "ORKA_AI_TOOLS")
	if !found {
		t.Fatal("Missing ORKA_AI_TOOLS")
	}

	tools := map[string]bool{}
	for tool := range strings.SplitSeq(toolsEnv.Value, ",") {
		tools[strings.TrimSpace(tool)] = true
	}

	for _, want := range []string{"recall_memory", "remember", "propose_memory", "search_transcript"} {
		if !tools[want] {
			t.Errorf("ORKA_AI_TOOLS = %s, want to contain %s", toolsEnv.Value, want)
		}
	}
}

func TestJobBuilder_buildEnvVars_WithCoordination_ChildTask(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-task",
			Namespace: defaultNS,
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "2",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Sub-task work",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:               true,
				MaxDepth:              3,
				MaxConcurrentChildren: 5,
				AllowedAgents: []corev1alpha1.AllowedAgent{
					{Name: "backend-dev"},
				},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)

	env, found := findEnvVar(envVars, "ORKA_COORDINATION_DEPTH")
	if !found {
		t.Fatal("Missing ORKA_COORDINATION_DEPTH")
	}
	if env.Value != "2" {
		t.Errorf("ORKA_COORDINATION_DEPTH = %s, want 2", env.Value)
	}
}

func TestJobBuilder_buildEnvVars_NoCoordination(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Simple task",
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "anthropic",
				Name:     "claude-3-5-sonnet",
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)

	if env, found := findEnvVar(envVars, workerenv.CoordinationEnabled); !found || env.Value != "" {
		t.Fatalf("%s = %#v, found=%t; want explicit empty controller-owned value", workerenv.CoordinationEnabled, env, found)
	}
	coordinationVars := []string{
		"ORKA_COORDINATION_MAX_DEPTH",
		"ORKA_COORDINATION_MAX_CHILDREN",
		"ORKA_COORDINATION_ALLOWED_AGENTS",
		"ORKA_COORDINATION_DEPTH",
	}
	for _, name := range coordinationVars {
		if _, found := findEnvVar(envVars, name); found {
			t.Errorf("Unexpected env var %s present without coordination", name)
		}
	}
}

// --- Agent task path tests ---

// helper to find env var by name
func findEnvVar(envVars []corev1.EnvVar, name string) (corev1.EnvVar, bool) {
	for _, e := range envVars {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func countEnvVars(envVars []corev1.EnvVar, name string) int {
	count := 0
	for _, e := range envVars {
		if e.Name == name {
			count++
		}
	}
	return count
}

// helper to check volume exists by name
func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, v := range volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

func requireWorkspaceCredentialProjection(
	t *testing.T,
	volumes []corev1.Volume,
	volumeName string,
	secretName string,
	secretKey string,
) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != volumeName {
			continue
		}
		if volume.Secret == nil {
			t.Fatalf("volume %q has no Secret source", volumeName)
		}
		if volume.Secret.SecretName != secretName {
			t.Fatalf("volume %q SecretName = %q, want %q", volumeName, volume.Secret.SecretName, secretName)
		}
		if len(volume.Secret.Items) != 1 {
			t.Fatalf("volume %q items = %#v, want one key projection", volumeName, volume.Secret.Items)
		}
		item := volume.Secret.Items[0]
		if item.Key != secretKey || item.Path != defaultACPWorkspaceCredentialKey {
			t.Fatalf("volume %q projection = %#v, want key %q at %q", volumeName, item, secretKey, defaultACPWorkspaceCredentialKey)
		}
		return
	}
	t.Fatalf("missing workspace credential volume %q", volumeName)
}

// helper to find a volume mount by name
func findVolumeMount(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, m := range mounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

// helper to check volume mount exists by name
func hasVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	_, ok := findVolumeMount(mounts, name)
	return ok
}

// helper to check EnvFrom includes the standard agent secret.
func hasAgentEnvFromSecret(envFrom []corev1.EnvFromSource) bool {
	for _, ef := range envFrom {
		if ef.SecretRef != nil && ef.SecretRef.Name == testAgentSecretName {
			return true
		}
	}
	return false
}

func TestJobBuilder_Build_ContainerTask_GitSecretVolume_DirectMountOptIn(t *testing.T) {
	t.Setenv(directGitCredentialsEnvVar, "true")

	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: testBusyboxImage,
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:           "https://github.com/example/repo",
				ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "my-git-creds"},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	mount, ok := findVolumeMount(job.Spec.Template.Spec.Containers[0].VolumeMounts, "git-read-credentials")
	if !ok {
		t.Fatal("Missing git-credentials volume mount")
	}
	if mount.MountPath != "/secrets/git" {
		t.Errorf("git-credentials mountPath = %s, want /secrets/git", mount.MountPath)
	}
	if !mount.ReadOnly {
		t.Error("git-credentials mount should be read-only")
	}
	requireWorkspaceCredentialProjection(t, job.Spec.Template.Spec.Volumes, "git-read-credentials", "my-git-creds", defaultACPWorkspaceCredentialKey)
}

func TestJobBuilder_Build_UntrustedContainerTask_DirectSecretsDisabledByDefault(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeContainer,
			Image:     testBusyboxImage,
			Command:   []string{"echo"},
			Args:      []string{"hello"},
			SecretRef: &corev1alpha1.SecretReference{Name: "task-secret"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-sec", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			SecretRef: &corev1.LocalObjectReference{Name: testAgentSecretName},
		},
	}
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: testProviderSecretName,
				Key:  defaultSecretKey,
			},
			BaseURL: testProviderBaseURL,
		},
	}

	job, err := builder.Build(context.Background(), task, agent, provider)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	if _, ok := findEnvVar(container.Env, testOpenAIAPIKey); ok {
		t.Fatal("OPENAI_API_KEY should not be present on untrusted container task by default")
	}
	if _, ok := findEnvVar(container.Env, "OPENAI_BASE_URL"); ok {
		t.Fatal("OPENAI_BASE_URL should not be present on untrusted container task by default")
	}
	if hasVolume(job.Spec.Template.Spec.Volumes, testTaskConfigVolume) {
		t.Fatal("task-secrets volume should not be present on untrusted container task by default")
	}
	if hasVolumeMount(container.VolumeMounts, testTaskConfigVolume) {
		t.Fatal("task-secrets volume mount should not be present on untrusted container task by default")
	}
	if hasVolume(job.Spec.Template.Spec.Volumes, "agent-secrets") {
		t.Fatal("agent-secrets volume should not be present on untrusted container task by default")
	}
	if hasVolumeMount(container.VolumeMounts, "agent-secrets") {
		t.Fatal("agent-secrets volume mount should not be present on untrusted container task by default")
	}
	if hasAgentEnvFromSecret(container.EnvFrom) {
		t.Fatal("agent secret EnvFrom should not be present on untrusted container task by default")
	}
}

func TestJobBuilder_Build_ContainerTask_Workspace(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "golang:1.26",
			Command: []string{"sh", "-lc"},
			Args:    []string{"go test ./..."},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:           "https://github.com/example/repo.git",
				Branch:            "feature",
				Ref:               "abc123",
				SubPath:           "src",
				ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "git-credentials"},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if !hasVolume(job.Spec.Template.Spec.Volumes, "workspace") {
		t.Fatal("missing workspace volume")
	}
	if !hasVolume(job.Spec.Template.Spec.Volumes, "home") {
		t.Fatal("missing home volume")
	}
	if !hasVolume(job.Spec.Template.Spec.Volumes, "git-read-credentials") {
		t.Fatal("missing read git credential volume")
	}
	if hasVolume(job.Spec.Template.Spec.Volumes, "git-publication-credentials") {
		t.Fatal("read-only custom-image workspace should not include publication credentials")
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("init container count = %d, want 1", len(job.Spec.Template.Spec.InitContainers))
	}
	init := job.Spec.Template.Spec.InitContainers[0]
	if init.Name != "prepare-workspace" {
		t.Errorf("init name = %q", init.Name)
	}
	if init.Image != builder.GeneralWorkerImage {
		t.Errorf("init image = %q, want %q", init.Image, builder.GeneralWorkerImage)
	}
	if !hasVolumeMount(init.VolumeMounts, "git-read-credentials") {
		t.Fatal("prepare-workspace init container missing git-credentials mount")
	}
	if _, ok := findEnvVar(init.Env, "ORKA_GIT_REPO"); !ok {
		t.Fatal("init missing ORKA_GIT_REPO")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if hasVolumeMount(container.VolumeMounts, "git-read-credentials") || hasVolumeMount(container.VolumeMounts, "git-publication-credentials") {
		t.Fatal("main container should not mount git credentials by default")
	}
	if container.WorkingDir != "/workspace/src" {
		t.Errorf("workingDir = %q, want /workspace/src", container.WorkingDir)
	}
	if _, ok := findEnvVar(container.Env, "ORKA_GIT_REF"); !ok {
		t.Fatal("container missing ORKA_GIT_REF")
	}
	if _, ok := findEnvVar(container.Env, "ORKA_PUSH_BRANCH"); ok {
		t.Fatal("read-only custom-image workspace unexpectedly included ORKA_PUSH_BRANCH")
	}
}

func TestJobBuilder_Build_ContainerTask_CustomImagePushBranchFailsClosed(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "custom-container-publish", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: testBusyboxImage,
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:                  "https://github.com/example/source.git",
				PushBranch:               "orka/publish",
				ReadCredentialRef:        &corev1alpha1.WorkspaceCredentialReference{Name: "source-read"},
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publication-write"},
			},
		},
	}

	const want = "custom-image container Tasks do not support workspace.pushBranch publication"
	if _, err := builder.Build(context.Background(), task, nil, nil); err == nil || err.Error() != want {
		t.Fatalf("Build() error = %v, want %q", err, want)
	}
}

func TestJobBuilder_Build_ContainerTask_ManagedPushBranchAccepted(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "managed-container-publish", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:  corev1alpha1.TaskTypeContainer,
			Image: "",
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:                  "https://github.com/example/source.git",
				PushBranch:               "orka/publish",
				ReadCredentialRef:        &corev1alpha1.WorkspaceCredentialReference{Name: "source-read"},
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "publication-write"},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v, want managed container publication accepted", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != builder.GeneralWorkerImage {
		t.Fatalf("container image = %q, want managed worker %q", container.Image, builder.GeneralWorkerImage)
	}
	if pushBranch, ok := findEnvVar(container.Env, "ORKA_PUSH_BRANCH"); !ok || pushBranch.Value != "orka/publish" {
		t.Fatalf("ORKA_PUSH_BRANCH = %#v, present=%v, want orka/publish", pushBranch, ok)
	}
}

func TestJobBuilder_Build_ContainerTask_PushUsesSeparatedCredentials(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "container-publish", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Command: []string{"sh", "-lc"},
			Args:    []string{"echo change >> file.txt"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:                  "https://github.com/example/source.git",
				PublicationGitRepo:       "https://github.com/example/target.git",
				PushBranch:               "orka/publish",
				ReadCredentialRef:        &corev1alpha1.WorkspaceCredentialReference{Name: "source-read", Key: "source-token"},
				PublicationCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "target-write", Key: "target-token"},
			},
		},
	}

	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("init container count = %d, want 1", len(job.Spec.Template.Spec.InitContainers))
	}
	init := job.Spec.Template.Spec.InitContainers[0]
	if !hasVolumeMount(init.VolumeMounts, "git-read-credentials") || hasVolumeMount(init.VolumeMounts, "git-publication-credentials") {
		t.Fatalf("workspace init mounts = %#v, want source read credential only", init.VolumeMounts)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if !hasVolumeMount(container.VolumeMounts, "git-publication-credentials") || hasVolumeMount(container.VolumeMounts, "git-read-credentials") {
		t.Fatalf("worker mounts = %#v, want target publication credential only", container.VolumeMounts)
	}
	requireWorkspaceCredentialProjection(t, job.Spec.Template.Spec.Volumes, "git-read-credentials", "source-read", "source-token")
	requireWorkspaceCredentialProjection(t, job.Spec.Template.Spec.Volumes, "git-publication-credentials", "target-write", "target-token")
	if forkRepo, ok := findEnvVar(container.Env, workerenv.ForkRepo); !ok || forkRepo.Value != "https://github.com/example/target.git" {
		t.Fatalf("%s = %#v, want publication repository", workerenv.ForkRepo, forkRepo)
	}
}

func TestJobBuilder_Build_ContainerTask_PushRequiresPublicationCredential(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "container-publish", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:           "https://github.com/example/source.git",
				PushBranch:        "orka/publish",
				ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: "source-read"},
			},
		},
	}

	if _, err := builder.Build(context.Background(), task, nil, nil); err == nil || !strings.Contains(err.Error(), "publicationCredentialRef") {
		t.Fatalf("Build() error = %v, want missing publication credential", err)
	}
}

func TestJobBuilder_Build_ContainerTask_RejectsUnsupportedPublicationGuarantees(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		mutate func(*corev1alpha1.WorkspaceConfig)
	}{
		{
			name:  "expected remote SHA",
			field: "workspace.expectedRemoteSHA",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.ExpectedRemoteSHA = strings.Repeat("a", 40)
			},
		},
		{
			name:  "create PR",
			field: "workspace.createPR",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.CreatePR = true
			},
		},
		{
			name:  "max changed files",
			field: "workspace.maxChangedFiles",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				limit := int32(1)
				workspace.MaxChangedFiles = &limit
			},
		},
		{
			name:  "allowed paths",
			field: "workspace.allowedPaths",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.AllowedPaths = []string{"src/**"}
			},
		},
		{
			name:  "repository control paths",
			field: "workspace.denyRepositoryControlPaths",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.DenyRepositoryControlPaths = true
			},
		},
		{
			name:  "binary files",
			field: "workspace.rejectBinaryFiles",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.RejectBinaryFiles = true
			},
		},
		{
			name:  "secret-like content",
			field: "workspace.rejectSecretLikeContent",
			mutate: func(workspace *corev1alpha1.WorkspaceConfig) {
				workspace.RejectSecretLikeContent = true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workspace := &corev1alpha1.WorkspaceConfig{GitRepo: "https://github.com/example/source.git"}
			tt.mutate(workspace)
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "container-unsupported-publication", Namespace: defaultNS},
				Spec: corev1alpha1.TaskSpec{
					Type:      corev1alpha1.TaskTypeContainer,
					Image:     testBusyboxImage,
					Workspace: workspace,
				},
			}

			_, err := setupJobBuilder().Build(context.Background(), task, nil, nil)
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("Build() error = %v, want unsupported %s error", err, tt.field)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// addSecretVolumes
// ---------------------------------------------------------------------------

func TestAddSecretVolumes_ProviderOpenAI(t *testing.T) {
	jb := setupJobBuilder()
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "openai-secret",
				Key:  "my-key",
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, nil, provider)
	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == testOpenAIAPIKey && env.ValueFrom != nil &&
			env.ValueFrom.SecretKeyRef.Key == "my-key" {
			found = true
		}
	}
	if !found {
		t.Error("expected OPENAI_API_KEY env var from secret")
	}
}

func TestAddSecretVolumes_ProviderAnthropic(t *testing.T) {
	jb := setupJobBuilder()
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeAnthropic,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "anthropic-secret",
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, nil, provider)
	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "ANTHROPIC_API_KEY" && env.ValueFrom != nil &&
			env.ValueFrom.SecretKeyRef.Key == defaultSecretKey {
			found = true
		}
	}
	if !found {
		t.Error("expected ANTHROPIC_API_KEY env var with default key")
	}
}

func TestAddSecretVolumes_TaskSecret(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAI,
			SecretRef: &corev1alpha1.SecretReference{Name: "task-secret"},
		},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, nil, nil)
	found := false
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == testTaskConfigVolume && v.Secret != nil && v.Secret.SecretName == "task-secret" {
			found = true
		}
	}
	if !found {
		t.Error("expected task-secrets volume")
	}
}

func TestAddSecretVolumes_AgentSecret(t *testing.T) {
	jb := setupJobBuilder()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-sec", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			SecretRef: &corev1.LocalObjectReference{Name: testAgentSecretName},
			Model:     &corev1alpha1.ModelConfig{Provider: "openai"},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, agent, nil)
	foundVol := false
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "agent-secrets" && v.Secret != nil && v.Secret.SecretName == testAgentSecretName {
			foundVol = true
		}
	}
	if !foundVol {
		t.Error("expected agent-secrets volume")
	}
	foundEnvFrom := false
	for _, ef := range job.Spec.Template.Spec.Containers[0].EnvFrom {
		if ef.SecretRef != nil && ef.SecretRef.Name == testAgentSecretName {
			foundEnvFrom = true
		}
	}
	if !foundEnvFrom {
		t.Error("expected agent-secret envFrom")
	}
}

func TestAddSecretVolumes_AIAgentEnvFromReservesTelemetryEnv(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			SecretRef: &corev1.LocalObjectReference{Name: testAgentSecretName},
		},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, agent, nil)

	for _, name := range []string{
		workerenv.EnableTelemetry,
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
		"OTEL_RESOURCE_ATTRIBUTES",
	} {
		got, ok := findEnvVar(job.Spec.Template.Spec.Containers[0].Env, name)
		if !ok || got.Value != "" || got.ValueFrom != nil {
			t.Fatalf("reserved env %s = %#v, found=%v", name, got, ok)
		}
	}
}

func TestAddSecretVolumes_FallbackProvider(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)
	fbProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "fb-prov", Namespace: defaultNS},
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "fb-secret",
				Key:  defaultSecretKey,
			},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fbProvider).Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "fb-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Fallbacks: []corev1alpha1.ModelFallback{
					{ProviderRef: "fb-prov", Model: "gpt-3.5"},
				},
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, agent, nil)
	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "ORKA_AI_FALLBACK_0_API_KEY" && env.ValueFrom != nil {
			found = true
		}
	}
	if !found {
		t.Error("expected fallback API key env var")
	}
}

func TestJobBuilder_Build_UntrustedFallbackProvidersDoNotReadProviderSecretsByDefault(t *testing.T) {
	t.Setenv(directProviderSecretsEnvVar, "false")

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)
	var providerGets int
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1alpha1.Provider); ok {
					providerGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "fb-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Fallbacks: []corev1alpha1.ModelFallback{
					{ProviderRef: "fb-prov", Model: "gpt-3.5"},
				},
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   testBusyboxImage,
			Command: []string{"echo"},
			Args:    []string{"hello"},
		},
	}

	job, err := jb.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if providerGets != 0 {
		t.Fatalf("Provider Get count = %d, want 0", providerGets)
	}
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if strings.HasPrefix(env.Name, "ORKA_AI_FALLBACK_") && strings.HasSuffix(env.Name, "_API_KEY") {
			t.Fatalf("unexpected fallback API key env var %s", env.Name)
		}
	}
}

func TestAddSecretVolumes_ProviderAzureOpenAI(t *testing.T) {
	jb := setupJobBuilder()
	provider := &corev1alpha1.Provider{
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeAzureOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "azure-secret",
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	job, _ := jb.Build(context.Background(), task, nil, nil)
	jb.addSecretVolumes(context.Background(), job, task, nil, provider)
	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == testOpenAIAPIKey {
			found = true
		}
	}
	if !found {
		t.Error("expected OPENAI_API_KEY for Azure OpenAI provider")
	}
}

// ---------------------------------------------------------------------------
// addAIEnvVars — fallback providers
// ---------------------------------------------------------------------------

func TestAddAIEnvVars_FallbackProviders(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)
	fbProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "fb-prov2", Namespace: defaultNS},
		Spec: corev1alpha1.ProviderSpec{
			Type:    corev1alpha1.ProviderTypeOpenAI,
			BaseURL: "https://custom.api.com",
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "fb-secret2",
			},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fbProvider).Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "fb-agent2", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{
				Provider: "openai",
				Fallbacks: []corev1alpha1.ModelFallback{
					{ProviderRef: "fb-prov2", Model: "gpt-3.5"},
				},
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI:   &corev1alpha1.AISpec{Prompt: "test"},
		},
	}
	envVars := jb.addAIEnvVars(context.Background(), nil, task, agent, nil)
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	if envMap["ORKA_AI_FALLBACK_COUNT"] != "1" {
		t.Errorf("expected fallback count 1, got %s", envMap["ORKA_AI_FALLBACK_COUNT"])
	}
	if envMap["ORKA_AI_FALLBACK_0_PROVIDER"] != string(corev1alpha1.ProviderTypeOpenAI) {
		t.Errorf("expected fallback provider openai, got %s", envMap["ORKA_AI_FALLBACK_0_PROVIDER"])
	}
	if envMap["ORKA_AI_FALLBACK_0_BASE_URL"] != "https://custom.api.com" {
		t.Errorf("expected fallback base URL, got %s", envMap["ORKA_AI_FALLBACK_0_BASE_URL"])
	}
}

// ---------------------------------------------------------------------------
// addWorkspaceEnvVars
// ---------------------------------------------------------------------------

func TestAddWorkspaceEnvVars_AllFields(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:            "https://github.com/org/repo",
				Branch:             "main",
				Ref:                "abc123",
				SubPath:            "src/",
				PublicationGitRepo: "https://github.com/fork/repo",
				PRBaseBranch:       "develop",
				PushBranch:         "feature-branch",
			},
		},
	}
	envVars := jb.addWorkspaceEnvVars(nil, task)
	expectedVars := map[string]string{
		"ORKA_GIT_REPO":            "https://github.com/org/repo",
		"ORKA_GIT_BRANCH":          "main",
		"ORKA_GIT_REF":             "abc123",
		"ORKA_WORKSPACE_SUBPATH":   "src/",
		"ORKA_FORK_REPO":           "https://github.com/fork/repo",
		"ORKA_PR_BASE_BRANCH":      "develop",
		"ORKA_PUSH_BRANCH":         "feature-branch",
		"ORKA_REQUIRE_PUSH_BRANCH": "true",
	}
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	for k, v := range expectedVars {
		if envMap[k] != v {
			t.Errorf("expected %s=%s, got %s", k, v, envMap[k])
		}
	}
}

// ---------------------------------------------------------------------------
// addAIEnvVars — fallback providers and child task coordination
// ---------------------------------------------------------------------------

func TestAddAIEnvVars_ChildTaskMessaging(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			Labels:    map[string]string{labels.LabelParentTask: "parent"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI:   &corev1alpha1.AISpec{Prompt: "test"},
		},
	}
	envVars := jb.addAIEnvVars(context.Background(), nil, task, nil, nil)
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	// Child task should get messaging tools auto-injected
	tools := envMap["ORKA_AI_TOOLS"]
	if !strings.Contains(tools, "send_message") || !strings.Contains(tools, "check_messages") {
		t.Errorf("expected messaging tools for child task, got %s", tools)
	}
	// Also ORKA_COORDINATION_ENABLED should be set
	if envMap["ORKA_COORDINATION_ENABLED"] != scheduledRunLabelValue {
		t.Error("expected ORKA_COORDINATION_ENABLED=true for child task without coordination agent")
	}
}

func TestAddAIEnvVars_ChildTaskMessagingDisabled(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testTask,
			Namespace:   defaultNS,
			Labels:      map[string]string{labels.LabelParentTask: "parent"},
			Annotations: map[string]string{labels.AnnotationDisableCoordinationToolInject: scheduledRunLabelValue},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI:   &corev1alpha1.AISpec{Prompt: "test"},
		},
	}
	envVars := jb.addAIEnvVars(context.Background(), nil, task, nil, nil)
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	if tools := envMap[workerenv.AITools]; tools != "" {
		t.Fatalf("%s = %q, want no auto-injected child messaging tools", workerenv.AITools, tools)
	}
	// Disabling coordination tool injection should not change the existing child-task
	// coordination mode marker; it only prevents implicit messaging tools from being
	// exposed to the LLM.
	if enabled := envMap[workerenv.CoordinationEnabled]; enabled != scheduledRunLabelValue {
		t.Fatalf("%s = %q, want %q", workerenv.CoordinationEnabled, enabled, scheduledRunLabelValue)
	}
}

func TestAddAIEnvVars_CoordinationEnabled(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI:   &corev1alpha1.AISpec{Prompt: "test"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "coord-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled: true,
			},
		},
	}
	envVars := jb.addAIEnvVars(context.Background(), nil, task, agent, nil)
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	tools := envMap["ORKA_AI_TOOLS"]
	for _, tool := range []string{"delegate_task", "list_pull_requests", "check_pr_review_marker"} {
		if !strings.Contains(tools, tool) {
			t.Errorf("expected coordination tool %s, got %s", tool, tools)
		}
	}
}

func TestAddAIEnvVars_CoordinationEnabledWithExplicitToolsOnly(t *testing.T) {
	jb := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testTask,
			Namespace:   defaultNS,
			Labels:      map[string]string{labels.LabelParentTask: labels.SelectorValue("scheduled-parent")},
			Annotations: map[string]string{labels.AnnotationDisableCoordinationToolInject: scheduledRunLabelValue},
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Prompt: "test",
				Tools:  []string{"list_pull_requests", "check_pr_review_marker"},
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "coord-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled: true,
			},
		},
	}
	envVars := jb.addAIEnvVars(context.Background(), nil, task, agent, nil)
	envMap := make(map[string]string)
	for _, e := range envVars {
		envMap[e.Name] = e.Value
	}
	if envMap[workerenv.CoordinationEnabled] != scheduledRunLabelValue {
		t.Error("expected ORKA_COORDINATION_ENABLED=true")
	}
	tools := envMap[workerenv.AITools]
	for _, tool := range []string{"list_pull_requests", "check_pr_review_marker"} {
		if !strings.Contains(tools, tool) {
			t.Errorf("expected explicit tool %s, got %s", tool, tools)
		}
	}
	for _, tool := range []string{"delegate_task", "send_message", "check_messages", "merge_pull_request", "auto_merge_pull_request"} {
		if strings.Contains(tools, tool) {
			t.Errorf("unexpected auto-injected coordination tool %s in %s", tool, tools)
		}
	}
}

func TestJobBuilderBuildAddsSkillVolumeAndConfigMap(t *testing.T) {
	const skillVolumeName = "skills"

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)

	skill := &corev1alpha1.Skill{
		ObjectMeta: metav1.ObjectMeta{Name: "research-skill", Namespace: defaultNS},
		Spec: corev1alpha1.SkillSpec{
			Description: "Research guidance",
			Content: corev1alpha1.SkillContent{
				Inline: "Use reliable sources.",
				Files: map[string]string{
					"templates/checklist.md": "- [ ] verify source",
				},
			},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(skill).Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-task", Namespace: defaultNS, UID: "uid-1234-5678"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider: "openai",
				Model:    "gpt-4o-mini",
				Prompt:   "hello",
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Skills: []corev1alpha1.SkillReference{{Name: "research-skill"}},
		},
	}

	job, err := jb.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if !hasVolume(job.Spec.Template.Spec.Volumes, skillVolumeName) {
		t.Fatal("expected skills volume to be mounted")
	}
	if !hasVolumeMount(job.Spec.Template.Spec.Containers[0].VolumeMounts, skillVolumeName) {
		t.Fatal("expected skills volume mount")
	}

	var skillsVolume *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == skillVolumeName {
			skillsVolume = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if skillsVolume == nil || skillsVolume.ConfigMap == nil {
		t.Fatal("skills volume should reference a ConfigMap")
	}

	cm := &corev1.ConfigMap{}
	if err := fc.Get(context.Background(), types.NamespacedName{
		Name:      skillsVolume.ConfigMap.Name,
		Namespace: defaultNS,
	}, cm); err != nil {
		t.Fatalf("expected skill ConfigMap to exist: %v", err)
	}
	if got := strings.TrimSpace(cm.Data["system-prompt"]); got != "Use reliable sources." {
		t.Fatalf("system-prompt = %q, want %q", got, "Use reliable sources.")
	}
	if len(skillsVolume.ConfigMap.Items) == 0 {
		t.Fatal("expected skills ConfigMap volume to include key-to-path mappings")
	}
}

func TestJobBuilderBuildFailsWhenSkillMissing(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "missing-skill-task",
			Namespace: defaultNS,
			UID:       types.UID("12345678-1234-1234-1234-123456789012"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider: "openai",
				Model:    "gpt-4o-mini",
				Prompt:   "hello",
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-skill-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Skills: []corev1alpha1.SkillReference{{Name: "does-not-exist"}},
		},
	}

	_, err := builder.Build(context.Background(), task, agent, nil)
	if err == nil {
		t.Fatal("expected Build() to fail when referenced skill does not exist")
	}
	if !strings.Contains(err.Error(), "failed to get Skill") {
		t.Fatalf("error = %v, expected missing skill message", err)
	}
}

func TestJobBuilderBuildDeduplicatesSkills(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)

	skill := &corev1alpha1.Skill{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-skill", Namespace: defaultNS},
		Spec: corev1alpha1.SkillSpec{
			Description: "Shared skill",
			Content: corev1alpha1.SkillContent{
				Inline: "Shared skill content.",
			},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(skill).Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	// Same skill referenced by both agent and task
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "dedup-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Skills: []corev1alpha1.SkillReference{{Name: "shared-skill"}},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "dedup-task", Namespace: defaultNS, UID: "uid-dedup"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider: "openai",
				Model:    "gpt-4o-mini",
				Prompt:   "hello",
				Skills:   []corev1alpha1.SkillReference{{Name: "shared-skill"}},
			},
		},
	}

	job, err := jb.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	// Find the skills ConfigMap and verify content appears only once
	var skillsVolume *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "skills" {
			skillsVolume = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if skillsVolume == nil || skillsVolume.ConfigMap == nil {
		t.Fatal("skills volume should reference a ConfigMap")
	}

	cm := &corev1.ConfigMap{}
	if err := fc.Get(context.Background(), types.NamespacedName{
		Name:      skillsVolume.ConfigMap.Name,
		Namespace: defaultNS,
	}, cm); err != nil {
		t.Fatalf("expected skill ConfigMap to exist: %v", err)
	}

	// Should have exactly 1 inline entry (skill-0-inline), not 2
	inlineCount := 0
	for key := range cm.Data {
		if strings.HasPrefix(key, "skill-") && strings.HasSuffix(key, "-inline") {
			inlineCount++
		}
	}
	if inlineCount != 1 {
		t.Fatalf("expected 1 inline skill entry (deduplicated), got %d", inlineCount)
	}
}

func TestJobBuilderBuildLoadsConfigMapSkills(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)

	skillCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "skill-cm", Namespace: defaultNS},
		Data:       map[string]string{"skill.txt": "You are a careful reviewer."},
	}

	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(skillCM).Build()
	jb := NewJobBuilder(fc)
	jb.ControllerURL = testControllerURL

	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "configmap-skill-task", Namespace: defaultNS, UID: "uid-configmap-skill"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Provider: "openai",
				Model:    "gpt-4o-mini",
				Prompt:   "hello",
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "configmap-skill-agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Skills: []corev1alpha1.SkillReference{{
				ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "skill-cm", Key: "skill.txt"},
			}},
		},
	}

	job, err := jb.Build(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	var skillsVolume *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "skills" {
			skillsVolume = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if skillsVolume == nil || skillsVolume.ConfigMap == nil {
		t.Fatal("skills volume should reference a ConfigMap")
	}

	cm := &corev1.ConfigMap{}
	if err := fc.Get(context.Background(), types.NamespacedName{
		Name:      skillsVolume.ConfigMap.Name,
		Namespace: defaultNS,
	}, cm); err != nil {
		t.Fatalf("expected generated skill ConfigMap to exist: %v", err)
	}
	if got := strings.TrimSpace(cm.Data["system-prompt"]); got != "You are a careful reviewer." {
		t.Fatalf("system-prompt = %q, want %q", got, "You are a careful reviewer.")
	}
}

func TestJobBuilder_buildEnvVars_WithApprovalRequiredTools(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "Coordinate incident"},
	}
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{
		Model: &corev1alpha1.ModelConfig{Provider: "anthropic", Name: "claude"},
		Coordination: &corev1alpha1.CoordinationConfig{
			Enabled:               true,
			Autonomous:            true,
			ApprovalRequiredTools: []string{"dispatch_work_order", "escalate_incident"},
		},
	}}
	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)
	env, found := findEnvVar(envVars, workerenv.ApprovalRequiredTools)
	if !found {
		t.Fatalf("missing %s", workerenv.ApprovalRequiredTools)
	}
	if env.Value != "dispatch_work_order,escalate_incident" {
		t.Fatalf("%s = %q", workerenv.ApprovalRequiredTools, env.Value)
	}
}

func TestJobBuilder_buildEnvVars_WithResolvedApprovalsOption(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "Coordinate incident"},
	}
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Provider: "anthropic", Name: "claude"}}}
	envVars := builder.buildEnvVarsWithOptions(context.Background(), task, agent, nil, JobBuildOptions{ResolvedApprovalsJSON: `[{"id":"k","status":"approved"}]`})
	env, found := findEnvVar(envVars, workerenv.ResolvedApprovals)
	if !found {
		t.Fatalf("missing %s", workerenv.ResolvedApprovals)
	}
	if env.Value != `[{"id":"k","status":"approved"}]` {
		t.Fatalf("%s = %q", workerenv.ResolvedApprovals, env.Value)
	}
}

func TestJobBuilder_buildEnvVars_KeepsEmptyResolvedApprovalsEnvOverride(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "real-uid"},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Coordinate incident",
		},
	}
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{
		Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
		Coordination: &corev1alpha1.CoordinationConfig{
			Enabled:               true,
			Autonomous:            true,
			ApprovalRequiredTools: []string{"dispatch_work_order"},
		},
	}}
	envVars := builder.buildEnvVarsWithOptions(context.Background(), task, agent, nil, JobBuildOptions{})
	env, ok := findEnvVar(envVars, workerenv.ResolvedApprovals)
	if !ok {
		t.Fatalf("missing %s", workerenv.ResolvedApprovals)
	}
	if env.Value != "" {
		t.Fatalf("%s = %q, want explicit empty value", workerenv.ResolvedApprovals, env.Value)
	}
}

func TestJobBuilder_buildEnvVars_AutonomousCoordinationIncludesRequestApprovalTool(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "Coordinate incident"},
	}
	agent := &corev1alpha1.Agent{Spec: corev1alpha1.AgentSpec{
		Model:        &corev1alpha1.ModelConfig{Provider: "anthropic", Name: "claude"},
		Coordination: &corev1alpha1.CoordinationConfig{Enabled: true, Autonomous: true},
	}}
	envVars := builder.buildEnvVars(context.Background(), task, agent, nil)
	toolsEnv, found := findEnvVar(envVars, workerenv.AITools)
	if !found {
		t.Fatal("missing ORKA_AI_TOOLS")
	}
	if !strings.Contains(toolsEnv.Value, "request_approval") {
		t.Fatalf("ORKA_AI_TOOLS = %s, want request_approval", toolsEnv.Value)
	}
}

func TestJobBuilder_buildEnvVars_TaskEnvCannotSpoofApprovalState(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS, UID: "real-uid"},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "Coordinate incident",
			Env: []corev1.EnvVar{
				{Name: workerenv.TaskUID, Value: "spoofed-uid"},
				{Name: workerenv.AgentName, Value: "spoofed-agent"},
				{Name: workerenv.AITools, Value: "request_approval"},
				{Name: workerenv.CoordinationEnabled, Value: scheduledRunLabelValue},
				{Name: workerenv.AutonomousMode, Value: scheduledRunLabelValue},
				{Name: workerenv.ResolvedApprovals, Value: `[{"id":"spoofed"}]`},
				{Name: workerenv.ApprovalRequiredTools, Value: "spoofed_tool"},
			},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "real-agent", Namespace: defaultNS},
		Spec:       corev1alpha1.AgentSpec{Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"}},
	}
	envVars := builder.buildEnvVarsWithOptions(context.Background(), task, agent, nil, JobBuildOptions{})
	if env, ok := findEnvVar(envVars, workerenv.TaskUID); !ok || env.Value != "real-uid" {
		t.Fatalf("%s = %#v, found=%t; want real-uid", workerenv.TaskUID, env, ok)
	}
	if env, ok := findEnvVar(envVars, workerenv.AgentName); !ok || env.Value != "real-agent" {
		t.Fatalf("%s = %#v, found=%t; want real-agent", workerenv.AgentName, env, ok)
	}
	for _, name := range []string{
		workerenv.AITools,
		workerenv.CoordinationEnabled,
		workerenv.AutonomousMode,
		workerenv.ResolvedApprovals,
		workerenv.ApprovalRequiredTools,
	} {
		if env, ok := findEnvVar(envVars, name); !ok || env.Value != "" {
			t.Fatalf("%s = %#v, found=%t; want explicit empty controller-owned value", name, env, ok)
		}
	}
}

func TestSafeWorkerOTLPEnvValueStripsUserinfoFromEndpoints(t *testing.T) {
	got := safeWorkerOTLPEnvValue("OTEL_EXPORTER_OTLP_ENDPOINT", "https://user:pass@collector:4318")
	if got != "https://collector:4318" {
		t.Fatalf("sanitized endpoint = %q, want %q", got, "https://collector:4318")
	}
	got = safeWorkerOTLPEnvValue("OTEL_EXPORTER_OTLP_ENDPOINT", "user:pass@collector:4317")
	if got != "collector:4317" {
		t.Fatalf("scheme-less sanitized endpoint = %q, want %q", got, "collector:4317")
	}
	got = safeWorkerOTLPEnvValue("OTEL_EXPORTER_OTLP_ENDPOINT", " https://user:pass@collector:4318 ")
	if got != "https://collector:4318" {
		t.Fatalf("trimmed sanitized endpoint = %q, want %q", got, "https://collector:4318")
	}
	got = safeWorkerOTLPEnvValue("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example/v1/traces?token=secret#frag")
	if got != "https://collector.example/v1/traces" {
		t.Fatalf("query sanitized endpoint = %q, want %q", got, "https://collector.example/v1/traces")
	}

	unchanged := safeWorkerOTLPEnvValue("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret")
	if unchanged != "authorization=secret" {
		t.Fatalf("non-endpoint value = %q, want unchanged", unchanged)
	}
}

func TestJobBuilder_buildEnvVars_TelemetryRequiresWorkerReachableEndpoint(t *testing.T) {
	tests := []struct {
		name            string
		endpoint        string
		tracesEndpoint  string
		metricsEndpoint string
	}{
		{name: "empty endpoint", endpoint: ""},
		{name: "localhost endpoint", endpoint: "http://localhost:4317"},
		{name: "IPv4 loopback endpoint", endpoint: "127.0.0.1:4317"},
		{name: "IPv6 loopback endpoint", endpoint: "http://[0:0:0:0:0:0:0:1]:4317"},
		{name: "zoned IPv6 loopback endpoint", endpoint: "http://[::1%25lo]:4317"},
		{name: "IPv6 unspecified endpoint", endpoint: "http://[::]:4317"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tt.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tt.tracesEndpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", tt.metricsEndpoint)
			builder := setupJobBuilder()
			builder.EnableTelemetry = true
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "p"},
			}

			envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
			if _, ok := findEnvVar(envVars, workerenv.EnableTelemetry); ok {
				t.Fatalf("%s should not be set without a worker-reachable OTLP endpoint", workerenv.EnableTelemetry)
			}
			if _, ok := findEnvVar(envVars, "OTEL_EXPORTER_OTLP_ENDPOINT"); ok {
				t.Fatal("loopback or empty OTLP endpoint should not be copied into task workloads")
			}
		})
	}
}

func TestJobBuilder_buildEnvVars_TelemetryDropsUnreachableSignalOverrides(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "1s")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	builder := setupJobBuilder()
	builder.EnableTelemetry = true
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "p"},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
	if got, ok := findEnvVar(envVars, "OTEL_EXPORTER_OTLP_ENDPOINT"); !ok || got.Value != "otel-collector:4317" {
		t.Fatalf("generic endpoint = %#v, found=%v", got, ok)
	}
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
	} {
		if got, ok := findEnvVar(envVars, name); ok {
			t.Fatalf("%s should not be copied with unreachable traces endpoint, got %#v", name, got)
		}
	}
	if got, ok := findEnvVar(envVars, "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"); !ok || got.Value != "grpc" {
		t.Fatalf("metrics protocol = %#v, found=%v", got, ok)
	}
}

func TestJobBuilder_buildEnvVars_TelemetryAllowsSignalSpecificEndpoints(t *testing.T) {
	tests := []struct {
		name      string
		traces    string
		metrics   string
		wantNames map[string]string
	}{
		{name: "traces only", traces: "otel-traces:4317", wantNames: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "otel-traces:4317"}},
		{name: "metrics only", metrics: "otel-metrics:4317", wantNames: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "otel-metrics:4317"}},
		{name: "both signals", traces: "otel-traces:4317", metrics: "otel-metrics:4317", wantNames: map[string]string{
			"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT":  "otel-traces:4317",
			"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "otel-metrics:4317",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tt.traces)
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", tt.metrics)
			builder := setupJobBuilder()
			builder.EnableTelemetry = true
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "p"},
			}

			envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
			if got, ok := findEnvVar(envVars, workerenv.EnableTelemetry); !ok || got.Value != scheduledRunLabelValue {
				t.Fatalf("%s = %#v, found=%v", workerenv.EnableTelemetry, got, ok)
			}
			for name, want := range tt.wantNames {
				if got, ok := findEnvVar(envVars, name); !ok || got.Value != want {
					t.Fatalf("%s = %#v, found=%v, want %q", name, got, ok, want)
				}
			}
		})
	}
}

func TestJobBuilder_buildEnvVars_IgnoresTaskSuppliedAITelemetryEnv(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAI,
			Prompt: "p",
			Env: []corev1.EnvVar{
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "https://example.invalid:4317"},
				{Name: workerenv.EnableTelemetry, Value: scheduledRunLabelValue},
				{Name: workerenv.TraceParent, Value: "00-" + strings.Repeat("1", 32) + "-" + strings.Repeat("2", 16) + "-01"},
				{Name: "CUSTOM_ENV", Value: "kept"},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
	for _, name := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", workerenv.EnableTelemetry, workerenv.TraceParent} {
		if _, ok := findEnvVar(envVars, name); ok {
			t.Fatalf("task-supplied %s should be ignored for AI workers", name)
		}
	}
	if got, ok := findEnvVar(envVars, "CUSTOM_ENV"); !ok || got.Value != "kept" {
		t.Fatalf("CUSTOM_ENV = %#v, found=%v", got, ok)
	}
}

func TestJobBuilder_buildEnvVars_PreservesContainerTelemetryEnv(t *testing.T) {
	builder := setupJobBuilder()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testTask, Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox",
			Command: []string{"true"},
			Env: []corev1.EnvVar{
				{Name: workerenv.EnableTelemetry, Value: scheduledRunLabelValue},
				{Name: "OTEL_RESOURCE_ATTRIBUTES", Value: "service.name=user-container"},
			},
		},
	}

	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
	if got, ok := findEnvVar(envVars, workerenv.EnableTelemetry); !ok || got.Value != scheduledRunLabelValue {
		t.Fatalf("%s = %#v, found=%v", workerenv.EnableTelemetry, got, ok)
	}
	if got, ok := findEnvVar(envVars, "OTEL_RESOURCE_ATTRIBUTES"); !ok || got.Value != "service.name=user-container" {
		t.Fatalf("OTEL_RESOURCE_ATTRIBUTES = %#v, found=%v", got, ok)
	}
}

func TestJobBuilder_buildEnvVars_Telemetry(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "3s")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "gzip")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=controller")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "authorization=secret")
	builder := setupJobBuilder()
	builder.EnableTelemetry = true
	traceparent := "00-" + strings.Repeat("1", 32) + "-" + strings.Repeat("2", 16) + "-01"
	tracestate := "vendor=value"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTask,
			Namespace: defaultNS,
			Annotations: map[string]string{
				labels.AnnotationTraceParent:  traceparent,
				labels.AnnotationTraceState:   tracestate,
				labels.AnnotationTraceBaggage: "tenant=acme",
			},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "p"},
	}
	envVars := builder.buildEnvVars(context.Background(), task, nil, nil)
	if got, ok := findEnvVar(envVars, workerenv.EnableTelemetry); !ok || got.Value != scheduledRunLabelValue {
		t.Fatalf("%s = %#v, found=%v", workerenv.EnableTelemetry, got, ok)
	}
	for name, want := range map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT":           "otel-collector:4317",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE":    "true",
		"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT":    "3s",
		"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION": "gzip",
	} {
		if got, ok := findEnvVar(envVars, name); !ok || got.Value != want {
			t.Fatalf("%s = %#v, found=%v, want %q", name, got, ok, want)
		}
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_RESOURCE_ATTRIBUTES"} {
		if _, ok := findEnvVar(envVars, name); ok {
			t.Fatalf("%s must not be copied into task workloads", name)
		}
	}
	if got, ok := findEnvVar(envVars, workerenv.TraceParent); !ok || got.Value != traceparent {
		t.Fatalf("%s = %#v, found=%v", workerenv.TraceParent, got, ok)
	}
	if got, ok := findEnvVar(envVars, workerenv.TraceState); !ok || got.Value != tracestate {
		t.Fatalf("%s = %#v, found=%v", workerenv.TraceState, got, ok)
	}
	if got, ok := findEnvVar(envVars, workerenv.TraceBaggage); ok {
		t.Fatalf("%s must not be copied into task workloads, got %#v", workerenv.TraceBaggage, got)
	}

	containerTask := task.DeepCopy()
	containerTask.Spec.Type = corev1alpha1.TaskTypeContainer
	envVars = builder.buildEnvVars(context.Background(), containerTask, nil, nil)
	if _, ok := findEnvVar(envVars, workerenv.EnableTelemetry); ok {
		t.Fatal("generic container tasks must not receive telemetry enablement")
	}
}

func TestReadOnlyAgentRuntimeGuardsAllowOpencode(t *testing.T) {
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeOpencode},
		},
	}

	if got := readOnlyAgentRuntimeType(agent); got != corev1alpha1.AgentRuntimeOpencode {
		t.Fatalf("readOnlyAgentRuntimeType() = %q, want opencode", got)
	}
	keys, err := readOnlyAgentRuntimeSecretKeys(agent)
	if err != nil || len(keys) != 0 {
		t.Fatalf("readOnlyAgentRuntimeSecretKeys() = %#v, %v, want no runtime secret keys", keys, err)
	}
}

var benchmarkResourceRequirementsSink corev1.ResourceRequirements

func BenchmarkJobBuilderBuildResourcesDefaults(b *testing.B) {
	builder := &JobBuilder{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{}}

	b.ReportAllocs()

	for b.Loop() {
		benchmarkResourceRequirementsSink = builder.buildResources(task, nil)
	}
}

func TestAddTransactionEnvVarsIncludesCredentialAuthority(t *testing.T) {
	tx := &corev1alpha1.TaskTransaction{Context: map[string]string{"secret": "resource-credential"}}
	env := addTransactionEnvVars(nil, tx, []string{"tenant:outbound-credentials:read"})
	got, ok := findEnvVar(env, workerenv.TransactionCredentialSecret)
	if !ok || got.Value != "resource-credential" {
		t.Fatalf("credential constraint env = %#v", env)
	}
	got, ok = findEnvVar(env, workerenv.TransactionCredentialReadScopes)
	if !ok || got.Value != "tenant:outbound-credentials:read" {
		t.Fatalf("credential scopes env = %#v", env)
	}
}

func TestSetTransactionCredentialAuthorizationEnvDisablesOutsideEnforceMode(t *testing.T) {
	env := []corev1.EnvVar{{
		Name:  workerenv.TransactionCredentialAuthorizationEnforced,
		Value: booleanTrueValue,
	}}
	env = setTransactionCredentialAuthorizationEnv(
		env,
		&corev1alpha1.TaskTransaction{ID: "txn-1"},
		false,
	)
	got, ok := findEnvVar(env, workerenv.TransactionCredentialAuthorizationEnforced)
	if !ok || got.Value != testFalseValue {
		t.Fatalf("credential authorization enforcement env = %#v, want false", env)
	}
}

func TestReadOnlyAgentAllowedToolsStayWorkspaceScoped(t *testing.T) {
	tools := readOnlyAgentAllowedTools()
	joined := strings.Join(tools, ",")
	if strings.Contains(joined, "Read(**)") || strings.Contains(joined, "Glob(**)") || strings.Contains(joined, "Grep(**)") || strings.Contains(joined, "LS(**)") {
		t.Fatalf("read-only tools escaped workspace scope: %#v", tools)
	}
	if !strings.Contains(joined, "Read(/workspace/**)") {
		t.Fatalf("read-only tools = %#v, missing workspace read scope", tools)
	}
	if strings.Contains(joined, "/tmp/orka-harness-workspace-") {
		t.Fatalf("read-only tools expose other harness workspaces: %#v", tools)
	}
}

func TestValidateReadOnlyBuiltInAgentRuntimeAllowsCodexReadOnlyAgentMode(t *testing.T) {
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		labels.AnnotationAgentReadOnly: scheduledRunLabelValue,
	}}}
	// Codex read-only tasks run in the native read-only agent mode with a
	// kernel-enforced read-only sandbox, so validation accepts them.
	if err := validateReadOnlyBuiltInAgentRuntime(task, corev1alpha1.AgentRuntimeCodex); err != nil {
		t.Fatalf("validateReadOnlyBuiltInAgentRuntime() error = %v, want codex accepted", err)
	}
	if err := validateReadOnlyBuiltInAgentRuntime(task, corev1alpha1.AgentRuntimeCopilot); err == nil ||
		!strings.Contains(err.Error(), "copilot runtime credentials") {
		t.Fatalf("validateReadOnlyBuiltInAgentRuntime(copilot) error = %v, want fail-closed rejection", err)
	}
}

func TestValidateContainerDeliveredPromptSize(t *testing.T) {
	ctx := context.Background()
	oversized := strings.Repeat("x", maxContainerDeliveredPromptBytes+1)
	builder := func(objects ...client.Object) *JobBuilder {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		return &JobBuilder{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	}
	small := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "hello"}}
	if err := builder().validateContainerDeliveredPromptSize(ctx, small, nil); err != nil {
		t.Fatalf("small prompt rejected: %v", err)
	}
	big := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, AI: &corev1alpha1.AISpec{Prompt: oversized}}}
	err := builder().validateContainerDeliveredPromptSize(ctx, big, nil)
	if err == nil || !strings.Contains(err.Error(), "MAX_ARG_STRLEN") {
		t.Fatalf("oversized prompt error = %v, want actionable env-limit message", err)
	}
	// spec.ai.prompt wins over spec.prompt (resolveAIConfig precedence): a
	// short spec.prompt must not mask an oversized exported spec.ai.prompt.
	masked := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "short", AI: &corev1alpha1.AISpec{Prompt: oversized}}}
	if err := builder().validateContainerDeliveredPromptSize(ctx, masked, nil); err == nil {
		t.Fatal("oversized spec.ai.prompt was masked by a short spec.prompt")
	}
	bigSystem := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, AI: &corev1alpha1.AISpec{Prompt: "ok", SystemPrompt: oversized}}}
	if err := builder().validateContainerDeliveredPromptSize(ctx, bigSystem, nil); err == nil {
		t.Fatal("oversized system prompt was accepted")
	}
	// A ConfigMap-backed Agent system prompt is resolved before export and
	// must be measured after resolution.
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sys", Namespace: "default"}, Data: map[string]string{"prompt": oversized}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{SystemPrompt: &corev1alpha1.PromptSource{
			ConfigMapRef: &corev1alpha1.ConfigMapKeySelector{Name: "sys", Key: "prompt"},
		}},
	}
	fromConfigMap := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}, Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI, Prompt: "ok"}}
	if err := builder(cm).validateContainerDeliveredPromptSize(ctx, fromConfigMap, agent); err == nil {
		t.Fatal("oversized ConfigMap-backed system prompt was accepted")
	}
	// Container Tasks never export these fields; unused optional prompts
	// must not fail an otherwise runnable container.
	containerTask := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer, Image: "alpine", Prompt: oversized}}
	if err := builder().validateContainerDeliveredPromptSize(ctx, containerTask, nil); err != nil {
		t.Fatalf("container task rejected on unused prompt field: %v", err)
	}
}
