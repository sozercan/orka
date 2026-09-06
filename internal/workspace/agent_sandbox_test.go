/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package workspace

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandbox "sigs.k8s.io/agent-sandbox/clients/go/sandbox"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	fakeextensions "sigs.k8s.io/agent-sandbox/clients/k8s/extensions/clientset/versioned/fake"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

const (
	fakeCodingAgentTemplate = "coding-agent"
	fakeTestNamespace       = "default"
)

func TestAgentSandboxExecutorReattachesClaimNameAcrossExecutorInstances(t *testing.T) {
	store := newFakeAgentSandboxStore()
	req := ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	}

	firstExecutor := NewAgentSandboxExecutor()
	firstExecutor.newClient = store.newClient
	created, err := firstExecutor.Claim(context.Background(), req)
	if err != nil {
		t.Fatalf("first Claim() error = %v", err)
	}
	if !created.Created || created.Reused {
		t.Fatalf("first Created/Reused = %v/%v, want true/false", created.Created, created.Reused)
	}

	secondExecutor := NewAgentSandboxExecutor()
	secondExecutor.newClient = store.newClient
	reuseReq := req
	reuseReq.ClaimName = created.Ref.ClaimName
	reuseReq.ReuseKey = "session-1"
	reused, err := secondExecutor.Claim(context.Background(), reuseReq)
	if err != nil {
		t.Fatalf("second Claim() error = %v", err)
	}
	if reused.Created || !reused.Reused {
		t.Fatalf("second Created/Reused = %v/%v, want false/true", reused.Created, reused.Reused)
	}
	if reused.Phase != PhaseReady {
		t.Fatalf("second phase = %s, want %s", reused.Phase, PhaseReady)
	}
	if reused.Ref.Namespace != created.Ref.Namespace || reused.Ref.ClaimName != created.Ref.ClaimName || reused.Ref.SandboxName != created.Ref.SandboxName {
		t.Fatalf("reattached ref = %#v, want same backend identity as %#v", reused.Ref, created.Ref)
	}

	desc, err := secondExecutor.Describe(context.Background(), DescribeRequest{Ref: reused.Ref})
	if err != nil {
		t.Fatalf("Describe() after reattach error = %v", err)
	}
	if desc.Phase != PhaseReady || desc.ReuseKey != "session-1" {
		t.Fatalf("description after reattach = %#v, want ready with reuse key", desc)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.createCalls) != 1 {
		t.Fatalf("CreateSandbox calls = %d, want 1", len(store.createCalls))
	}
	if len(store.getCalls) != 1 {
		t.Fatalf("GetSandbox calls = %d, want 1", len(store.getCalls))
	}
	if call := store.getCalls[0]; call.claimName != created.Ref.ClaimName || call.namespace != fakeTestNamespace {
		t.Fatalf("GetSandbox call = %#v, want claim %q namespace %s", call, created.Ref.ClaimName, fakeTestNamespace)
	}
	if len(store.clientOptions) != 2 {
		t.Fatalf("client creations = %d, want 2", len(store.clientOptions))
	}
	if got := store.clientOptions[1].WarmPoolName; got != fakeCodingAgentTemplate {
		t.Fatalf("reattach client WarmPoolName = %q, want coding-agent", got)
	}
}

func TestAgentSandboxExecutorReattachCallsGetSandboxWithRequestedClaim(t *testing.T) {
	store := newFakeAgentSandboxStore()
	store.seed(fakeTestNamespace, "retained-claim", "retained-sandbox")

	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claimed, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		ClaimName: "retained-claim",
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if claimed.Created || !claimed.Reused {
		t.Fatalf("Created/Reused = %v/%v, want false/true", claimed.Created, claimed.Reused)
	}
	if claimed.Ref.ClaimName != "retained-claim" || claimed.Ref.SandboxName != "retained-sandbox" {
		t.Fatalf("Claim() ref = %#v, want retained claim identity", claimed.Ref)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.getCalls) != 1 {
		t.Fatalf("GetSandbox calls = %d, want 1", len(store.getCalls))
	}
	if call := store.getCalls[0]; call.claimName != "retained-claim" || call.namespace != fakeTestNamespace {
		t.Fatalf("GetSandbox call = %#v, want retained-claim/%s", call, fakeTestNamespace)
	}
	if len(store.createCalls) != 0 {
		t.Fatalf("CreateSandbox calls = %d, want 0 for explicit reattach", len(store.createCalls))
	}
}

func TestAgentSandboxExecutorCreatesNamedClaimWhenReattachMisses(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient

	claimed, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace:       fakeTestNamespace,
		ClaimName:       "orka-session-fixed",
		CreateIfMissing: true,
		Template:        TemplateRef{Name: fakeCodingAgentTemplate},
		ReuseKey:        "session-1",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if !claimed.Created || claimed.Reused {
		t.Fatalf("Created/Reused = %v/%v, want true/false", claimed.Created, claimed.Reused)
	}
	if claimed.Ref.ClaimName != "orka-session-fixed" {
		t.Fatalf("claim name = %q, want deterministic name", claimed.Ref.ClaimName)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.getCalls) != 1 {
		t.Fatalf("GetSandbox calls = %d, want 1 before named create", len(store.getCalls))
	}
	if len(store.createCalls) != 1 {
		t.Fatalf("CreateSandbox calls = %d, want 1", len(store.createCalls))
	}
	if call := store.createCalls[0]; call.claimName != "orka-session-fixed" || call.template != fakeCodingAgentTemplate || call.namespace != fakeTestNamespace {
		t.Fatalf("CreateSandboxWithName call = %#v, want named session claim", call)
	}
}

func TestAgentSandboxSDKClientRollsBackCreatedNamedClaimWhenAttachFails(t *testing.T) {
	extensionsClient := fakeextensions.NewSimpleClientset() //nolint:staticcheck // generated fake clientset still uses deprecated testing package helpers
	k8sHelper := &sandbox.K8sHelper{
		ExtensionsClient: extensionsClient.ExtensionsV1beta1(),
		Log:              logr.Discard(),
	}
	sdkClient, err := sandbox.NewClient(context.Background(), sandbox.Options{
		WarmPoolName:        fakeCodingAgentTemplate,
		Namespace:           fakeTestNamespace,
		APIURL:              "http://localhost:65535",
		SandboxReadyTimeout: 5 * time.Millisecond,
		RequestTimeout:      5 * time.Millisecond,
		PerAttemptTimeout:   5 * time.Millisecond,
		Quiet:               true,
		K8sHelper:           k8sHelper,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client := &agentSandboxSDKClient{client: sdkClient, k8s: k8sHelper, readyTimeout: 5 * time.Millisecond}

	_, err = client.CreateSandboxWithName(context.Background(), "rollback-claim", fakeCodingAgentTemplate, fakeTestNamespace, "none")
	if err == nil {
		t.Fatal("CreateSandboxWithName() error = nil, want attach failure")
	}
	_, getErr := extensionsClient.ExtensionsV1beta1().SandboxClaims(fakeTestNamespace).Get(
		context.Background(),
		"rollback-claim",
		metav1.GetOptions{},
	)
	if !k8serrors.IsNotFound(getErr) {
		t.Fatalf("rollback claim Get() error = %v, want not found", getErr)
	}
}

func TestAgentSandboxSDKClientCreateSandboxWithNameSetsWarmPoolRef(t *testing.T) {
	extensionsClient := fakeextensions.NewSimpleClientset() //nolint:staticcheck // generated fake clientset still uses deprecated testing package helpers
	var gotWarmPoolRef string
	extensionsClient.PrependReactor("create", "sandboxclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			t.Fatalf("create reactor action = %T, want CreateAction", action)
		}
		claim, ok := createAction.GetObject().(*sandboxextv1beta1.SandboxClaim)
		if !ok {
			t.Fatalf("created object = %T, want SandboxClaim", createAction.GetObject())
		}
		gotWarmPoolRef = claim.Spec.WarmPoolRef.Name
		return false, nil, nil
	})
	k8sHelper := &sandbox.K8sHelper{
		ExtensionsClient: extensionsClient.ExtensionsV1beta1(),
		Log:              logr.Discard(),
	}
	sdkClient, err := sandbox.NewClient(context.Background(), sandbox.Options{
		WarmPoolName:        fakeCodingAgentTemplate,
		Namespace:           fakeTestNamespace,
		APIURL:              "http://localhost:65535",
		SandboxReadyTimeout: 5 * time.Millisecond,
		RequestTimeout:      5 * time.Millisecond,
		PerAttemptTimeout:   5 * time.Millisecond,
		Quiet:               true,
		K8sHelper:           k8sHelper,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client := &agentSandboxSDKClient{client: sdkClient, k8s: k8sHelper, readyTimeout: 5 * time.Millisecond}

	_, err = client.CreateSandboxWithName(
		context.Background(),
		"warm-pool-claim",
		fakeCodingAgentTemplate,
		fakeTestNamespace,
		"none",
	)
	if err == nil {
		t.Fatal("CreateSandboxWithName() error = nil, want attach failure")
	}
	if gotWarmPoolRef != fakeCodingAgentTemplate {
		t.Fatalf("created claim warm pool ref = %q, want %q", gotWarmPoolRef, fakeCodingAgentTemplate)
	}
}

func TestAgentSandboxSDKClientWaitsForCreatedSandboxReady(t *testing.T) {
	extensionsClient := fakeextensions.NewSimpleClientset( //nolint:staticcheck // generated fake clientset still uses deprecated testing package helpers
		&sandboxextv1beta1.SandboxClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "ready-claim", Namespace: fakeTestNamespace},
			Status: sandboxextv1beta1.SandboxClaimStatus{
				SandboxStatus: sandboxextv1beta1.SandboxStatus{Name: "warm-sandbox"},
			},
		})
	agentsClient := fakeagents.NewSimpleClientset() //nolint:staticcheck // generated fake clientset still uses deprecated testing package helpers
	getCount := 0
	agentsClient.PrependReactor("get", "sandboxes", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		getCount++
		status := metav1.ConditionFalse
		if getCount >= 2 {
			status = metav1.ConditionTrue
		}
		return true, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: "warm-sandbox", Namespace: fakeTestNamespace},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: status,
				}},
			},
		}, nil
	})
	client := &agentSandboxSDKClient{
		k8s: &sandbox.K8sHelper{
			AgentsClient:     agentsClient.AgentsV1beta1(),
			ExtensionsClient: extensionsClient.ExtensionsV1beta1(),
			Log:              logr.Discard(),
		},
		readyTimeout: time.Second,
	}

	if err := client.waitCreatedSandboxReady(context.Background(), "ready-claim", fakeTestNamespace); err != nil {
		t.Fatalf("waitCreatedSandboxReady() error = %v", err)
	}
	if getCount < 2 {
		t.Fatalf("sandbox readiness checks = %d, want at least 2", getCount)
	}
}

func TestAgentSandboxExecutorLocalClaimNameReuseSkipsGetSandbox(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	req := ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	}

	created, err := executor.Claim(context.Background(), req)
	if err != nil {
		t.Fatalf("first Claim() error = %v", err)
	}
	store.resetCalls()

	reuseReq := req
	reuseReq.ClaimName = created.Ref.ClaimName
	reused, err := executor.Claim(context.Background(), reuseReq)
	if err != nil {
		t.Fatalf("second Claim() error = %v", err)
	}
	if reused.Created || !reused.Reused {
		t.Fatalf("second Created/Reused = %v/%v, want false/true", reused.Created, reused.Reused)
	}
	if reused.Ref != created.Ref {
		t.Fatalf("reused ref = %#v, want %#v", reused.Ref, created.Ref)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.getCalls) != 0 {
		t.Fatalf("GetSandbox calls = %d, want 0 for local reuse", len(store.getCalls))
	}
	if len(store.createCalls) != 0 {
		t.Fatalf("CreateSandbox calls = %d, want 0 for local reuse", len(store.createCalls))
	}
	if len(store.clientOptions) != 0 {
		t.Fatalf("new client calls = %d, want 0 for local reuse", len(store.clientOptions))
	}
}

func TestAgentSandboxExecutorClaimPropagatesTimeoutToSandboxOptions(t *testing.T) {
	tests := []struct {
		name              string
		timeout           time.Duration
		maxRequestTimeout time.Duration
		want              time.Duration
	}{
		{
			name:    "claim timeout only",
			timeout: 17 * time.Second,
			want:    17 * time.Second,
		},
		{
			name:              "max request timeout extends transport",
			timeout:           3 * time.Second,
			maxRequestTimeout: 120 * time.Second,
			want:              120 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeAgentSandboxStore()
			executor := NewAgentSandboxExecutor()
			executor.newClient = store.newClient

			_, err := executor.Claim(context.Background(), ClaimRequest{
				Namespace:         fakeTestNamespace,
				Template:          TemplateRef{Name: fakeCodingAgentTemplate},
				Timeout:           tt.timeout,
				MaxRequestTimeout: tt.maxRequestTimeout,
			})
			if err != nil {
				t.Fatalf("Claim() error = %v", err)
			}

			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.clientOptions) != 1 {
				t.Fatalf("client creations = %d, want 1", len(store.clientOptions))
			}
			opts := store.clientOptions[0]
			if opts.RequestTimeout != tt.want {
				t.Errorf("RequestTimeout = %v, want %v", opts.RequestTimeout, tt.want)
			}
			if opts.PerAttemptTimeout != tt.want {
				t.Errorf("PerAttemptTimeout = %v, want %v", opts.PerAttemptTimeout, tt.want)
			}
		})
	}
}

func TestAgentSandboxExecutorClaimPassesWarmPoolPolicy(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient

	_, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace:      fakeTestNamespace,
		Template:       TemplateRef{Name: fakeCodingAgentTemplate},
		WarmPoolPolicy: "none",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.createCalls) != 1 {
		t.Fatalf("CreateSandbox calls = %d, want 1", len(store.createCalls))
	}
	if got := store.createCalls[0].warmPoolPolicy; got != "none" {
		t.Fatalf("warm pool policy = %q, want %q", got, "none")
	}
}

func TestAgentSandboxExecutorClaimUsesClaimNamespaceAndTemplateName(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor(WithAgentSandboxAPIURL("http://router.example"))
	executor.newClient = store.newClient

	_, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: "task-ns",
		Template:  TemplateRef{Namespace: "template-ns", Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.createCalls) != 1 {
		t.Fatalf("CreateSandbox calls = %d, want 1", len(store.createCalls))
	}
	if call := store.createCalls[0]; call.template != fakeCodingAgentTemplate || call.namespace != "task-ns" {
		t.Fatalf("CreateSandbox call = %#v, want template coding-agent in claim namespace task-ns", call)
	}
	if len(store.clientOptions) != 1 {
		t.Fatalf("client creations = %d, want 1", len(store.clientOptions))
	}
	if got := store.clientOptions[0].APIURL; got != "http://router.example" {
		t.Fatalf("client APIURL = %q, want router URL", got)
	}
}

func TestAgentSandboxCommandStringRendersDeterministicallyAndSafely(t *testing.T) {
	env := map[string]string{
		"VALUE": "quote ' and spaces",
		"A":     "first",
	}
	_, envFilePath, envContent, err := agentSandboxEnvFile(time.Unix(0, 123), env)
	if err != nil {
		t.Fatalf("agentSandboxEnvFile() error = %v", err)
	}
	if string(envContent) != "export A='first'\nexport VALUE='quote '\"'\"' and spaces'\n" {
		t.Fatalf("env file content = %q", string(envContent))
	}

	command, err := agentSandboxCommandString(ExecRequest{
		Command: []string{"sh", "-c", "printf '%s' \"$VALUE\""},
		Env:     env,
		WorkDir: "/workspace/dir with ' quote",
	}, envFilePath)
	if err != nil {
		t.Fatalf("agentSandboxCommandString() error = %v", err)
	}

	script := strings.Join([]string{
		agentSandboxEnvFilePrelude(envFilePath),
		"cd",
		shellQuote("/workspace/dir with ' quote"),
		"&&",
		shellQuote("sh"),
		shellQuote("-c"),
		shellQuote("printf '%s' \"$VALUE\""),
	}, " ")
	want := "sh -c " + shellQuote(script)
	if command != want {
		t.Fatalf("command = %q, want %q", command, want)
	}
	if strings.Contains(command, "A=first") || strings.Contains(command, "VALUE=quote") {
		t.Fatalf("command leaked env assignments: %q", command)
	}
}

func TestAgentSandboxCommandStringRejectsUnsafeInputs(t *testing.T) {
	tests := []struct {
		name string
		req  ExecRequest
	}{
		{name: "workdir nul", req: ExecRequest{WorkDir: "bad\x00dir", Command: []string{"echo"}}},
		{name: "empty env name", req: ExecRequest{Command: []string{"echo"}, Env: map[string]string{"": "value"}}},
		{name: "env contains equals", req: ExecRequest{Command: []string{"echo"}, Env: map[string]string{"A=B": "value"}}},
		{name: "env invalid shell name", req: ExecRequest{Command: []string{"echo"}, Env: map[string]string{"1BAD": "value"}}},
		{name: "env value nul", req: ExecRequest{Command: []string{"echo"}, Env: map[string]string{"A": "bad\x00value"}}},
		{name: "arg nul", req: ExecRequest{Command: []string{"bad\x00arg"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.req.Env) > 0 {
				if _, _, _, err := agentSandboxEnvFile(time.Unix(0, 1), tt.req.Env); err == nil {
					t.Fatal("agentSandboxEnvFile() error = nil, want error")
				}
				return
			}
			if _, err := agentSandboxCommandString(tt.req, ""); err == nil {
				t.Fatal("agentSandboxCommandString() error = nil, want error")
			}
		})
	}
}

func TestAgentSandboxExecutorExecRendersCommandAndMapsCommandFailure(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claim, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	handle := store.mustHandle(t, claim.Ref.Namespace, claim.Ref.ClaimName)
	handle.nextRun = &sandbox.ExecutionResult{Stdout: "abcdef", Stderr: "ghijkl", ExitCode: 2}

	result, err := executor.Exec(context.Background(), ExecRequest{
		Ref:            claim.Ref,
		Command:        []string{"echo", "hello world"},
		Env:            map[string]string{"B": "two", "A": "one"},
		WorkDir:        "/workspace/repo",
		MaxOutputBytes: 3,
	})
	if !IsKind(err, ErrorKindCommandFailed) {
		t.Fatalf("Exec() error = %v, want kind %s", err, ErrorKindCommandFailed)
	}
	if result == nil || result.ExitCode != 2 {
		t.Fatalf("Exec() result = %#v, want exit code 2", result)
	}
	if result.Stdout != "abc" || result.Stderr != "ghi" || !result.StdoutTruncated || !result.StderrTruncated {
		t.Fatalf("Exec() truncated output = %#v", result)
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()
	if len(handle.writes) != 1 {
		t.Fatalf("env file writes = %d, want 1", len(handle.writes))
	}
	var envFileName string
	var envFileContent []byte
	for name, content := range handle.writes {
		envFileName = name
		envFileContent = content
	}
	if !strings.HasPrefix(envFileName, agentSandboxEnvFilePrefix) {
		t.Fatalf("env file name = %q, want prefix %q", envFileName, agentSandboxEnvFilePrefix)
	}
	if string(envFileContent) != "export A='one'\nexport B='two'\n" {
		t.Fatalf("env file content = %q", string(envFileContent))
	}
	if len(handle.runCommands) != 1 {
		t.Fatalf("Run calls = %d, want 1", len(handle.runCommands))
	}
	envFilePath := agentSandboxExecRoot + envFileName
	script := strings.Join([]string{
		agentSandboxEnvFilePrelude(envFilePath),
		"cd", shellQuote("/workspace/repo"), "&&",
		shellQuote("echo"), shellQuote("hello world"),
	}, " ")
	wantCommand := "sh -c " + shellQuote(script)
	if handle.runCommands[0] != wantCommand {
		t.Fatalf("Run command = %q, want %q", handle.runCommands[0], wantCommand)
	}
	if strings.Contains(handle.runCommands[0], "A=one") || strings.Contains(handle.runCommands[0], "B=two") {
		t.Fatalf("Run command leaked env assignments: %q", handle.runCommands[0])
	}
}

func TestAgentSandboxExecutorExecRemovesEnvFileWhenRunFails(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claim, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	handle := store.mustHandle(t, claim.Ref.Namespace, claim.Ref.ClaimName)
	handle.runErr = fmt.Errorf("transport failed before shell started")

	_, err = executor.Exec(context.Background(), ExecRequest{
		Ref:     claim.Ref,
		Command: []string{"echo", "hello"},
		Env:     map[string]string{"GIT_TOKEN": "secret-token"},
	})
	if err == nil {
		t.Fatal("Exec() error = nil, want run failure")
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()
	if len(handle.writes) != 1 {
		t.Fatalf("env file writes = %d, want 1", len(handle.writes))
	}
	var envFileName string
	for name := range handle.writes {
		envFileName = name
	}
	if len(handle.runCommands) != 2 {
		t.Fatalf("Run commands = %#v, want command plus env cleanup", handle.runCommands)
	}
	wantCleanup := "rm -f " + shellQuote(agentSandboxExecRoot+envFileName)
	if handle.runCommands[1] != wantCleanup {
		t.Fatalf("cleanup command = %q, want %q", handle.runCommands[1], wantCleanup)
	}
}

func TestAgentSandboxExecutorExecRejectsStdin(t *testing.T) {
	executor := NewAgentSandboxExecutor()
	_, err := executor.Exec(context.Background(), ExecRequest{Command: []string{"cat"}, Stdin: []byte("input")})
	if !IsKind(err, ErrorKindInvalidArgument) {
		t.Fatalf("Exec() error = %v, want kind %s", err, ErrorKindInvalidArgument)
	}
}

func TestAgentSandboxExecutorUploadWritesPlainFilesAndRejectsNestedPaths(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claim, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	handle := store.mustHandle(t, claim.Ref.Namespace, claim.Ref.ClaimName)

	uploaded, err := executor.Upload(context.Background(), UploadRequest{
		Ref:       claim.Ref,
		Artifacts: []UploadArtifact{{Path: "out.txt", Data: []byte("hello")}},
	})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if len(uploaded.Artifacts) != 1 || uploaded.Artifacts[0].Path != "out.txt" {
		t.Fatalf("Upload() artifacts = %#v", uploaded.Artifacts)
	}

	handle.mu.Lock()
	got := string(handle.writes["out.txt"])
	handle.mu.Unlock()
	if got != "hello" {
		t.Fatalf("written out.txt = %q, want hello", got)
	}

	_, err = executor.Upload(context.Background(), UploadRequest{
		Ref:       claim.Ref,
		Artifacts: []UploadArtifact{{Path: "logs/out.txt", Data: []byte("nested")}},
	})
	if !IsKind(err, ErrorKindInvalidArgument) {
		t.Fatalf("Upload(nested) error = %v, want kind %s", err, ErrorKindInvalidArgument)
	}
}

func TestAgentSandboxExecutorDownloadRecursivelyListsAndReadsFiles(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claim, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	handle := store.mustHandle(t, claim.Ref.Namespace, claim.Ref.ClaimName)
	handle.lists = map[string][]sandbox.FileEntry{
		".": {
			{Name: "root.txt", Type: sandbox.FileTypeFile, Size: 4, ModTime: time.Unix(1700000000, 0)},
			{Name: "dir", Type: sandbox.FileTypeDirectory},
		},
		"dir": {{Name: "child.txt", Type: sandbox.FileTypeFile, Size: 5, ModTime: time.Unix(1700000001, 0)}},
	}
	handle.reads = map[string][]byte{
		"root.txt":      []byte("root"),
		"dir/child.txt": []byte("child"),
	}

	down, err := executor.Download(context.Background(), DownloadRequest{Ref: claim.Ref})
	if err != nil {
		t.Fatalf("Download() error = %v", err)
	}
	if len(down.Artifacts) != 2 {
		t.Fatalf("Download() artifacts = %d, want 2: %#v", len(down.Artifacts), down.Artifacts)
	}
	if down.Artifacts[0].Path != "dir/child.txt" || string(down.Artifacts[0].Data) != "child" {
		t.Fatalf("first artifact = %#v", down.Artifacts[0])
	}
	if down.Artifacts[1].Path != "root.txt" || string(down.Artifacts[1].Data) != "root" {
		t.Fatalf("second artifact = %#v", down.Artifacts[1])
	}
}

func TestAgentSandboxExecutorReleaseRetainsAndDeleteUsesClient(t *testing.T) {
	store := newFakeAgentSandboxStore()
	executor := NewAgentSandboxExecutor()
	executor.newClient = store.newClient
	claim, err := executor.Claim(context.Background(), ClaimRequest{
		Namespace: fakeTestNamespace,
		Template:  TemplateRef{Name: fakeCodingAgentTemplate},
		ReuseKey:  "session-1",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	handle := store.mustHandle(t, claim.Ref.Namespace, claim.Ref.ClaimName)

	released, err := executor.Release(context.Background(), ReleaseRequest{
		Ref:    claim.Ref,
		Retain: true,
		Reason: "debug",
	})
	if err != nil {
		t.Fatalf("Release(retain) error = %v", err)
	}
	if !released.Retained || released.Phase != PhaseRetained || !strings.Contains(released.Message, "debug") {
		t.Fatalf("Release(retain) = %#v", released)
	}
	handle.mu.Lock()
	disconnected := handle.disconnected
	closed := handle.closed
	handle.mu.Unlock()
	if !disconnected || closed {
		t.Fatalf("disconnect/close = %v/%v, want true/false", disconnected, closed)
	}

	deleted, err := executor.Delete(context.Background(), DeleteRequest{Ref: claim.Ref, Reason: "done"})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted.Deleted || deleted.Phase != PhaseDeleted {
		t.Fatalf("Delete() = %#v", deleted)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deleteCalls) != 1 {
		t.Fatalf("DeleteSandbox calls = %d, want 1", len(store.deleteCalls))
	}
	if call := store.deleteCalls[0]; call.claimName != claim.Ref.ClaimName || call.namespace != fakeTestNamespace {
		t.Fatalf("DeleteSandbox call = %#v, want claimed workspace/%s", call, fakeTestNamespace)
	}
}

type fakeAgentSandboxStore struct {
	mu sync.Mutex

	nextClaim int
	handles   map[string]*fakeAgentSandboxHandle

	clientOptions []sandbox.Options
	createCalls   []fakeAgentSandboxCreateCall
	getCalls      []fakeAgentSandboxGetCall
	deleteCalls   []fakeAgentSandboxDeleteCall
}

type fakeAgentSandboxCreateCall struct {
	claimName      string
	template       string
	namespace      string
	warmPoolPolicy string
}

type fakeAgentSandboxGetCall struct {
	claimName string
	namespace string
}

type fakeAgentSandboxDeleteCall struct {
	claimName string
	namespace string
}

func newFakeAgentSandboxStore() *fakeAgentSandboxStore {
	return &fakeAgentSandboxStore{
		handles: make(map[string]*fakeAgentSandboxHandle),
	}
}

func (s *fakeAgentSandboxStore) newClient(_ context.Context, opts sandbox.Options) (agentSandboxClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientOptions = append(s.clientOptions, opts)
	return &fakeAgentSandboxClient{store: s}, nil
}

func (s *fakeAgentSandboxStore) seed(namespace, claimName, sandboxName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handles[fakeAgentSandboxKey(namespace, claimName)] = &fakeAgentSandboxHandle{
		claimName:   claimName,
		sandboxName: sandboxName,
		podName:     sandboxName + "-pod",
		ready:       true,
		annotations: map[string]string{"seeded": "true"},
	}
}

func (s *fakeAgentSandboxStore) createHandleLocked(namespace, claimName, sandboxName, template string) *fakeAgentSandboxHandle {
	handle := &fakeAgentSandboxHandle{
		claimName:   claimName,
		sandboxName: sandboxName,
		podName:     sandboxName + "-pod",
		ready:       true,
		annotations: map[string]string{"template": template},
	}
	s.handles[fakeAgentSandboxKey(namespace, claimName)] = handle
	return handle
}

func (s *fakeAgentSandboxStore) mustHandle(t *testing.T, namespace, claimName string) *fakeAgentSandboxHandle {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	handle := s.handles[fakeAgentSandboxKey(namespace, claimName)]
	if handle == nil {
		t.Fatalf("missing fake handle for %s/%s", namespace, claimName)
	}
	return handle
}

func (s *fakeAgentSandboxStore) resetCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientOptions = nil
	s.createCalls = nil
	s.getCalls = nil
	s.deleteCalls = nil
}

type fakeAgentSandboxClient struct {
	store *fakeAgentSandboxStore
}

func (c *fakeAgentSandboxClient) CreateSandbox(_ context.Context, template, namespace, warmPoolPolicy string) (agentSandboxHandle, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.nextClaim++
	claimName := fmt.Sprintf("claim-%d", c.store.nextClaim)
	sandboxName := fmt.Sprintf("sandbox-%d", c.store.nextClaim)
	c.store.createCalls = append(c.store.createCalls, fakeAgentSandboxCreateCall{template: template, namespace: namespace, warmPoolPolicy: warmPoolPolicy})
	return c.store.createHandleLocked(namespace, claimName, sandboxName, template), nil
}

func (c *fakeAgentSandboxClient) CreateSandboxWithName(_ context.Context, claimName, template, namespace, warmPoolPolicy string) (agentSandboxHandle, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.createCalls = append(c.store.createCalls, fakeAgentSandboxCreateCall{claimName: claimName, template: template, namespace: namespace, warmPoolPolicy: warmPoolPolicy})
	return c.store.createHandleLocked(namespace, claimName, claimName+"-sandbox", template), nil
}

func (c *fakeAgentSandboxClient) GetSandbox(_ context.Context, claimName, namespace string) (agentSandboxHandle, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.getCalls = append(c.store.getCalls, fakeAgentSandboxGetCall{claimName: claimName, namespace: namespace})
	handle := c.store.handles[fakeAgentSandboxKey(namespace, claimName)]
	if handle == nil {
		return nil, NewError("claim", ErrorKindNotFound, "fake claim not found", false, nil)
	}
	handle.ready = true
	return handle, nil
}

func (c *fakeAgentSandboxClient) DeleteSandbox(_ context.Context, claimName, namespace string) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.deleteCalls = append(c.store.deleteCalls, fakeAgentSandboxDeleteCall{claimName: claimName, namespace: namespace})
	delete(c.store.handles, fakeAgentSandboxKey(namespace, claimName))
	return nil
}

func fakeAgentSandboxKey(namespace, claimName string) string {
	return namespace + "/" + claimName
}

type fakeAgentSandboxHandle struct {
	mu sync.Mutex

	claimName   string
	sandboxName string
	podName     string
	ready       bool
	annotations map[string]string

	runCommands   []string
	writes        map[string][]byte
	reads         map[string][]byte
	lists         map[string][]sandbox.FileEntry
	nextRun       *sandbox.ExecutionResult
	runErr        error
	writeErr      error
	readErr       error
	listErr       error
	closeErr      error
	disconnectErr error
	closed        bool
	disconnected  bool
}

func (h *fakeAgentSandboxHandle) Open(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ready = true
	return nil
}

func (h *fakeAgentSandboxHandle) Close(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closeErr != nil {
		return h.closeErr
	}
	h.ready = false
	h.closed = true
	return nil
}

func (h *fakeAgentSandboxHandle) Disconnect(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.disconnectErr != nil {
		return h.disconnectErr
	}
	h.ready = false
	h.disconnected = true
	return nil
}

func (h *fakeAgentSandboxHandle) IsReady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

func (h *fakeAgentSandboxHandle) Run(_ context.Context, command string, _ ...sandbox.CallOption) (*sandbox.ExecutionResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.runCommands = append(h.runCommands, command)
	if h.runErr != nil {
		return nil, h.runErr
	}
	if h.nextRun != nil {
		result := *h.nextRun
		return &result, nil
	}
	return &sandbox.ExecutionResult{ExitCode: 0}, nil
}

func (h *fakeAgentSandboxHandle) Write(_ context.Context, path string, content []byte, _ ...sandbox.CallOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writeErr != nil {
		return h.writeErr
	}
	if h.writes == nil {
		h.writes = make(map[string][]byte)
	}
	h.writes[path] = append([]byte(nil), content...)
	return nil
}

func (h *fakeAgentSandboxHandle) Read(_ context.Context, path string, _ ...sandbox.CallOption) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.readErr != nil {
		return nil, h.readErr
	}
	if data, ok := h.reads[path]; ok {
		return append([]byte(nil), data...), nil
	}
	if data, ok := h.writes[path]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, nil
}

func (h *fakeAgentSandboxHandle) List(_ context.Context, path string, _ ...sandbox.CallOption) ([]sandbox.FileEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.listErr != nil {
		return nil, h.listErr
	}
	entries := h.lists[path]
	return append([]sandbox.FileEntry(nil), entries...), nil
}

func (h *fakeAgentSandboxHandle) Exists(context.Context, string, ...sandbox.CallOption) (bool, error) {
	return false, nil
}

func (h *fakeAgentSandboxHandle) ClaimName() string {
	return h.claimName
}

func (h *fakeAgentSandboxHandle) SandboxName() string {
	return h.sandboxName
}

func (h *fakeAgentSandboxHandle) PodName() string {
	return h.podName
}

func (h *fakeAgentSandboxHandle) PodIP() string {
	return ""
}

func (h *fakeAgentSandboxHandle) Annotations() map[string]string {
	return copyStringMap(h.annotations)
}
