/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/store/sqlite"
)

const (
	acpWorkspaceTestSessionName                   = "review-loop"
	acpWorkspaceTestMissingSessionName            = "missing-session"
	acpWorkspaceTestOtherNamespace                = "other-namespace"
	acpWorkspaceTestRuntimePoolName               = "acp-ws-codex-0123456789abcdef"
	acpWorkspaceTestTemplateName                  = "task-template"
	acpWorkspaceTestTemplateRefRequiredError      = "requires templateRef.name"
	acpWorkspaceTestTemplateRefForbiddenError     = "templateRef must be omitted"
	acpWorkspaceTestCleanupDeleteError            = "always deleted after authenticated drain"
	acpWorkspaceTestSessionReferenceRequiredError = "requires spec.sessionRef.name"
)

type failingAgentExecutionSnapshotPersistStore struct {
	store.AgentExecutionSnapshotStore
}

type failNthExecutionWorkspaceListReader struct {
	client.Reader
	failAt int
	calls  int
}

func (r *failNthExecutionWorkspaceListReader) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	if _, ok := list.(*workspacev1alpha1.ExecutionWorkspaceList); ok {
		r.calls++
		if r.calls == r.failAt {
			return errors.New("simulated post-Session quota list outage")
		}
	}
	return r.Reader.List(ctx, list, options...)
}

func (s failingAgentExecutionSnapshotPersistStore) PersistAgentExecutionSnapshot(
	context.Context,
	store.AgentExecutionSnapshot,
) error {
	return errors.New("snapshot persistence unavailable")
}

func workspaceBindingTestTask(mutate func(*corev1alpha1.ExecutionWorkspaceSpec)) *corev1alpha1.Task {
	task := bindingTestTask()
	workspace := &corev1alpha1.ExecutionWorkspaceSpec{
		Enabled:  true,
		Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
	}
	if mutate != nil {
		mutate(workspace)
	}
	task.Spec.Execution = &corev1alpha1.ExecutionSpec{Workspace: workspace}
	return task
}

func TestResolveACPWorkspaceBinding(t *testing.T) {
	tests := []struct {
		name                      string
		task                      *corev1alpha1.Task
		wantErr                   string
		wantNil                   bool
		wantSession               string
		sessionUID                string
		enforceNamespaceIsolation bool
	}{
		{name: "nil task", task: nil, wantNil: true},
		{name: "no workspace", task: bindingTestTask(), wantNil: true},
		{
			name:    "disabled workspace",
			task:    workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) { ws.Enabled = false }),
			wantNil: true,
		},
		{
			name:        "defaults resolve to a per-task binding",
			task:        workspaceBindingTestTask(nil),
			wantSession: "task:11111111-1111-1111-1111-111111111111",
		},
		{
			name: "session reuse binds to the continued session",
			task: func() *corev1alpha1.Task {
				task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
					ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
				})
				task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
				return task
			}(),
			sessionUID:  "session-uid-review-loop",
			wantSession: "session:session-uid-review-loop",
		},
		{
			name: "session reuse rejects non-default workspace slot",
			task: func() *corev1alpha1.Task {
				task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
					ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
					ws.WorkspaceSlot = "secondary"
				})
				task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
				return task
			}(),
			sessionUID: "session-uid-review-loop",
			wantErr:    "supports only workspaceSlot",
		},
		{
			name:    "substrate without templateRef fails closed",
			task:    workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) { ws.Provider = corev1alpha1.WorkspaceProviderSubstrate }),
			wantErr: acpWorkspaceTestTemplateRefRequiredError,
		},
		{
			name: "substrate with an infrastructure template resolves",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
				ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: substrateTestTemplateNamespace}
			}),
			wantSession: "task:11111111-1111-1111-1111-111111111111",
		},
		{
			name: "substrate rejects invalid template namespace",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
				ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: "Bad_NS"}
			}),
			wantErr: "templateRef.namespace",
		},
		{
			name: "substrate rejects invalid template name",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
				ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: "bad/name", Namespace: substrateTestTemplateNamespace}
			}),
			wantErr: "templateRef.name",
		},
		{
			name: "substrate cross-namespace template fails under namespace isolation",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
				ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: substrateTestBaseTemplateName, Namespace: substrateTestTemplateNamespace}
			}),
			enforceNamespaceIsolation: true,
			wantErr:                   "cross-namespace execution workspace templateRef is not allowed",
		},
		{
			name:    "unknown provider fails closed",
			task:    workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) { ws.Provider = corev1alpha1.WorkspaceProvider("other") }),
			wantErr: "does not support ACP RuntimeSessions",
		},
		{
			name: "templateRef fails closed",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: "operator-template"}
			}),
			wantErr: acpWorkspaceTestTemplateRefForbiddenError,
		},
		{
			name: "retain cleanup fails closed",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicyRetain
			}),
			wantErr: acpWorkspaceTestCleanupDeleteError,
		},
		{
			name:    "onDetach fails closed",
			task:    workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) { ws.OnDetach = corev1alpha1.WorkspaceOnDetachSuspend }),
			wantErr: "onDetach is not supported",
		},
		{
			name:    "boot fails closed",
			task:    workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) { ws.Boot = true }),
			wantErr: "not supported for ACP RuntimeSessions",
		},
		{
			name: "session reuse without sessionRef fails closed",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
			}),
			wantErr: acpWorkspaceTestSessionReferenceRequiredError,
		},
		{
			name: "task-scoped workspace with sessionRef fails closed",
			task: func() *corev1alpha1.Task {
				task := workspaceBindingTestTask(nil)
				task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
				return task
			}(),
			wantErr: "reusePolicy none cannot be used with spec.sessionRef",
		},
		{
			name: "classRef fails closed without a resolved class",
			task: workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
				ws.Enabled = false
				ws.ClassRef = &corev1alpha1.WorkspaceClassReference{Name: "class"}
			}),
			wantErr: "requires a resolved workspace class",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding, err := resolveACPWorkspaceBinding(tt.task, corev1alpha1.WorkspaceProviderAgentSandbox, tt.enforceNamespaceIsolation, tt.sessionUID)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveACPWorkspaceBinding() error = %v", err)
			}
			if tt.wantNil {
				if binding != nil {
					t.Fatalf("binding = %#v, want nil", binding)
				}
				return
			}
			if binding == nil {
				t.Fatal("binding = nil, want resolved binding")
			}
			if binding.SessionKey != tt.wantSession {
				t.Fatalf("session key = %q, want %q", binding.SessionKey, tt.wantSession)
			}
			if binding.SessionUID != tt.sessionUID {
				t.Fatalf("session UID = %q, want %q", binding.SessionUID, tt.sessionUID)
			}
			if binding.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete || binding.WorkspaceSlot != defaultWorkspaceSlotName {
				t.Fatalf("binding defaults = %q/%q, want delete/default", binding.CleanupPolicy, binding.WorkspaceSlot)
			}
			if binding.Provider == corev1alpha1.WorkspaceProviderSubstrate &&
				(binding.TemplateNamespace == "" || binding.TemplateName == "") {
				t.Fatalf("substrate binding template = %q/%q, want frozen infrastructure template reference", binding.TemplateNamespace, binding.TemplateName)
			}
			digest, err := acpWorkspaceBindingDigest(binding)
			if err != nil || digest != binding.BindingDigest {
				t.Fatalf("binding digest = %q (err=%v), want canonical %q", binding.BindingDigest, err, digest)
			}
			if err := validateACPWorkspaceBindingValues(binding); err != nil {
				t.Fatalf("resolved binding failed re-verification: %v", err)
			}
		})
	}
}

func TestApplyACPWorkspaceBindingToPlanChangesPoolIdentity(t *testing.T) {
	ctx := context.Background()
	task := bindingTestTask()
	agent := bindingTestAgent()
	reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	plainPlan, err := PlanACPRuntimeWithConfiguration(task, agent, reconciler.ACPRuntimeImages, configuration)
	if err != nil {
		t.Fatal(err)
	}

	binding, err := resolveACPWorkspaceBinding(workspaceBindingTestTask(nil), corev1alpha1.WorkspaceProviderAgentSandbox, false, "")
	if err != nil || binding == nil {
		t.Fatalf("resolveACPWorkspaceBinding() = %#v, %v", binding, err)
	}
	workspacePlan, err := applyACPWorkspaceBindingToPlan(plainPlan, binding)
	if err != nil {
		t.Fatal(err)
	}
	if workspacePlan.PoolName == plainPlan.PoolName {
		t.Fatal("workspace-backed plan reused the plain RuntimePool identity")
	}
	if !strings.HasPrefix(workspacePlan.PoolName, "acp-ws-codex-") {
		t.Fatalf("workspace pool name = %q, want acp-ws-codex- prefix", workspacePlan.PoolName)
	}
	if workspacePlan.Workspace == nil || workspacePlan.Workspace.BindingDigest != binding.BindingDigest {
		t.Fatalf("workspace plan binding = %#v, want %q", workspacePlan.Workspace, binding.BindingDigest)
	}

	again, err := applyACPWorkspaceBindingToPlan(plainPlan, binding)
	if err != nil || again.PoolName != workspacePlan.PoolName {
		t.Fatalf("workspace pool identity is not deterministic: %q vs %q (err=%v)", again.PoolName, workspacePlan.PoolName, err)
	}

	unchanged, err := applyACPWorkspaceBindingToPlan(plainPlan, nil)
	if err != nil || unchanged.PoolName != plainPlan.PoolName || unchanged.Workspace != nil {
		t.Fatalf("nil binding changed the plan: %#v (err=%v)", unchanged, err)
	}
}

func TestSessionWorkspacePoolIdentityRejectsRuntimeProfileRotation(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
	reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, task, bindingTestAgent())
	if err != nil {
		t.Fatal(err)
	}
	plainPlan, err := PlanACPRuntimeWithConfiguration(task, bindingTestAgent(), reconciler.ACPRuntimeImages, configuration)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := resolveACPWorkspaceBinding(
		task, corev1alpha1.WorkspaceProviderAgentSandbox, false, "session-uid-review-loop",
	)
	if err != nil {
		t.Fatal(err)
	}
	firstPlan, err := applyACPWorkspaceBindingToPlan(plainPlan, binding)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPlainPlan := plainPlan
	rotatedPlainPlan.Profile.ProviderKind = "rotated-provider"
	rotatedPlainPlan.Profile.Model = "rotated-model"
	rotatedPlainPlan.Digest, err = harnessv2.CanonicalProfileDigest(rotatedPlainPlan.Profile)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPlan, err := applyACPWorkspaceBindingToPlan(rotatedPlainPlan, binding)
	if err != nil {
		t.Fatal(err)
	}
	if rotatedPlan.PoolName != firstPlan.PoolName {
		t.Fatalf("session workspace pool rotated with the runtime profile: first=%q rotated=%q", firstPlan.PoolName, rotatedPlan.PoolName)
	}
	if _, _, err := reconciler.ensureACPRuntimePool(ctx, task.Namespace, firstPlan, "", "", ""); err != nil {
		t.Fatalf("create session workspace RuntimePool: %v", err)
	}
	if _, _, err := reconciler.ensureACPRuntimePool(ctx, task.Namespace, rotatedPlan, "", "", ""); !errors.Is(err, store.ErrValidation) ||
		!strings.Contains(err.Error(), "cannot rotate the runtime image or profile") {
		t.Fatalf("rotated profile error = %v, want permanent session-workspace rejection", err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := reconciler.List(ctx, &pools, client.InNamespace(task.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].Name != firstPlan.PoolName {
		t.Fatalf("runtime pools after rejected rotation = %#v, want only %q", pools.Items, firstPlan.PoolName)
	}
}

func TestSessionWorkspacePoolIdentityRejectsWorkspaceSelectionRotation(t *testing.T) {
	ctx := context.Background()
	firstTask := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	firstTask.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
	reconciler, _ := newBindingTestReconciler(t, firstTask, bindingTestNamespace())
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, firstTask, bindingTestAgent())
	if err != nil {
		t.Fatal(err)
	}
	plainPlan, err := PlanACPRuntimeWithConfiguration(firstTask, bindingTestAgent(), reconciler.ACPRuntimeImages, configuration)
	if err != nil {
		t.Fatal(err)
	}
	firstBinding, err := resolveACPWorkspaceBinding(
		firstTask, corev1alpha1.WorkspaceProviderAgentSandbox, false, "session-uid-review-loop",
	)
	if err != nil {
		t.Fatal(err)
	}
	firstPlan, err := applyACPWorkspaceBindingToPlan(plainPlan, firstBinding)
	if err != nil {
		t.Fatal(err)
	}

	rotatedTask := firstTask.DeepCopy()
	rotatedTask.Spec.Execution.Workspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
	rotatedTask.Spec.Execution.Workspace.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{
		Namespace: substrateTestTemplateNamespace, Name: substrateTestBaseTemplateName,
	}
	rotatedBinding, err := resolveACPWorkspaceBinding(
		rotatedTask, corev1alpha1.WorkspaceProviderAgentSandbox, false, "session-uid-review-loop",
	)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPlan, err := applyACPWorkspaceBindingToPlan(plainPlan, rotatedBinding)
	if err != nil {
		t.Fatal(err)
	}
	if rotatedBinding.BindingDigest == firstBinding.BindingDigest {
		t.Fatal("workspace selection rotation did not change the frozen binding")
	}
	if rotatedPlan.PoolName != firstPlan.PoolName {
		t.Fatalf("session workspace pool rotated with workspace selection: first=%q rotated=%q", firstPlan.PoolName, rotatedPlan.PoolName)
	}
	if _, _, err := reconciler.ensureACPRuntimePool(ctx, firstTask.Namespace, firstPlan, "", "", ""); err != nil {
		t.Fatalf("create session workspace RuntimePool: %v", err)
	}
	if _, _, err := reconciler.ensureACPRuntimePool(ctx, firstTask.Namespace, rotatedPlan, "", "", ""); !errors.Is(err, store.ErrValidation) ||
		!strings.Contains(err.Error(), "cannot change the workspace provider") {
		t.Fatalf("rotated workspace selection error = %v, want permanent session-workspace rejection", err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := reconciler.List(ctx, &pools, client.InNamespace(firstTask.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].Name != firstPlan.PoolName {
		t.Fatalf("runtime pools after rejected workspace rotation = %#v, want only %q", pools.Items, firstPlan.PoolName)
	}
}

func TestAgentExecutionBindingFreezesWorkspaceBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task := workspaceBindingTestTask(nil)
	agent := bindingTestAgent()
	reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
	reconciler.ExecutionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProviderAgentSandbox

	live := task.DeepCopy()
	if _, err, handled := reconciler.ensureAgentExecutionBinding(ctx, live, agent); err != nil || handled {
		t.Fatalf("ensure binding = handled=%v err=%v", handled, err)
	}
	bound := &corev1alpha1.Task{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, bound); err != nil {
		t.Fatal(err)
	}
	verified, err := reconciler.loadVerifiedBoundExecution(ctx, bound, bound.Status.AgentExecutionBinding)
	if err != nil {
		t.Fatalf("loadVerifiedBoundExecution() error = %v", err)
	}
	if verified.plan.Workspace == nil {
		t.Fatal("verified plan is missing the frozen workspace binding")
	}
	if !strings.HasPrefix(verified.plan.PoolName, "acp-ws-codex-") {
		t.Fatalf("verified pool name = %q, want workspace-backed pool identity", verified.plan.PoolName)
	}
	if verified.body.ExecutionWorkspace == nil ||
		verified.body.ExecutionWorkspace.BindingDigest != verified.plan.Workspace.BindingDigest {
		t.Fatalf("snapshot body workspace = %#v, want frozen binding digest", verified.body.ExecutionWorkspace)
	}
	if verified.plan.Workspace.SessionKey != "task:"+string(task.UID) {
		t.Fatalf("frozen session key = %q, want task-scoped key", verified.plan.Workspace.SessionKey)
	}
}

func TestAgentExecutionBindingFreezesImmutableWorkspaceSessionUID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName, Create: true, Append: true}
	agent := bindingTestAgent()
	reconciler, durableStore := newBindingTestReconciler(t, task, bindingTestNamespace())
	reconciler.ExecutionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProviderAgentSandbox
	reconciler.DurableControlStore = durableStore
	reconciler.SessionManager = NewSessionManager(durableStore)
	epochs := NewControllerEpochManager(durableStore, "workspace-session-binding-test")
	reconciler.ControllerEpochManager = epochs
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	defer func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("stop epoch manager: %v", err)
		}
	}()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err, handled := reconciler.ensureAgentExecutionBinding(ctx, task.DeepCopy(), agent); err != nil || handled {
		t.Fatalf("ensure session-reused binding = handled=%v err=%v", handled, err)
	}
	control, err := durableStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name)
	if err != nil {
		t.Fatalf("load established SessionControl: %v", err)
	}
	bound := &corev1alpha1.Task{}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), bound); err != nil {
		t.Fatal(err)
	}
	verified, err := reconciler.loadVerifiedBoundExecution(ctx, bound, bound.Status.AgentExecutionBinding)
	if err != nil {
		t.Fatal(err)
	}
	if verified.plan.Workspace == nil || verified.body.ExecutionWorkspace == nil {
		t.Fatal("session-reused execution snapshot is missing its workspace binding")
	}
	if verified.plan.Workspace.SessionUID != control.SessionUID ||
		verified.body.ExecutionWorkspace.SessionUID != control.SessionUID ||
		verified.plan.Workspace.SessionKey != "session:"+control.SessionUID {
		t.Fatalf("frozen workspace identity = plan %#v snapshot %#v, want Session UID %q", verified.plan.Workspace, verified.body.ExecutionWorkspace, control.SessionUID)
	}
}

func TestResolveAgentExecutionCandidateKeepsPostSessionQuotaFailureTransient(t *testing.T) {
	ctx := context.Background()
	limit := int32(1)
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendSubstrate, func(f *acpClassFixture) {
		f.provider.Status.SupportedFeatures = append(
			f.provider.Status.SupportedFeatures,
			workspacev1alpha1.WorkspaceFeatureSuspend,
		)
		f.class.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		f.class.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend,
			workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		f.profile.Spec.Substrate.Suspend = &acpworkspacev1alpha1.SubstrateSuspendPolicy{
			Mode: acpworkspacev1alpha1.SubstrateSuspendModeDataOnly,
		}
		f.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	})
	task := bindingTestTask()
	task.Spec.Execution = &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
		ClassRef:    &corev1alpha1.WorkspaceClassReference{Name: acpTestClassName},
		ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
	}}
	task.Spec.SessionRef = &corev1alpha1.SessionReference{
		Name: acpWorkspaceTestSessionName, Create: true, Append: true,
	}

	objects := append(fixture.objects(), task, bindingTestNamespace())
	classReconciler := acpClassTestReconciler(t, objects...)
	reconciler, durableStore := newBindingTestReconciler(t)
	reconciler.Client = classReconciler.Client
	reconciler.Scheme = classReconciler.Scheme
	reconciler.WorkspaceProviderAPIEnabled = true
	reconciler.DurableControlStore = durableStore
	reconciler.SessionManager = NewSessionManager(durableStore)
	reconciler.ControllerEpochManager = NewControllerEpochManager(durableStore, "post-session-quota-test")
	reader := &failNthExecutionWorkspaceListReader{Reader: classReconciler.Client, failAt: 2}
	reconciler.APIReader = reader

	_, err := reconciler.resolveAgentExecutionCandidate(ctx, task, bindingTestAgent())
	if err == nil || !errors.Is(err, errACPWorkspacePlanningTransient) {
		t.Fatalf("post-Session quota outage error = %v, want transient planning marker", err)
	}
	if isPermanentACPAgentConfigurationError(err) {
		t.Fatalf("post-Session quota outage was classified permanent: %v", err)
	}
	if reader.calls != 2 {
		t.Fatalf("execution workspace list calls = %d, want the second quota read to fail", reader.calls)
	}
}

func TestResolveAgentExecutionCandidateDoesNotCreateWorkspaceSessionBeforeValidation(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "invalid-candidate", Create: true, Append: true}
	reconciler, durableStore := newBindingTestReconciler(t, task)
	reconciler.ExecutionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProviderAgentSandbox
	reconciler.DurableControlStore = durableStore
	reconciler.SessionManager = NewSessionManager(durableStore)
	reconciler.ControllerEpochManager = NewControllerEpochManager(durableStore, "workspace-session-pure-candidate-test")

	if _, err := reconciler.resolveAgentExecutionCandidate(ctx, task, bindingTestAgent()); err == nil ||
		!strings.Contains(err.Error(), "resolve task namespace identity") {
		t.Fatalf("candidate error = %v, want namespace validation failure", err)
	}
	if _, err := durableStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed candidate created SessionControl: %v", err)
	}
	if _, err := durableStore.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed candidate created transcript Session: %v", err)
	}
}

func TestResolveAgentExecutionCandidateClassifiesMissingWorkspaceSessionAsPermanent(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestMissingSessionName, Create: false, Append: true}
	reconciler, durableStore := newBindingTestReconciler(t, task, bindingTestNamespace())
	reconciler.ExecutionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProviderAgentSandbox
	reconciler.DurableControlStore = durableStore
	reconciler.SessionManager = NewSessionManager(durableStore)
	reconciler.ControllerEpochManager = NewControllerEpochManager(durableStore, "workspace-session-missing-test")

	_, err := reconciler.resolveAgentExecutionCandidate(ctx, task, bindingTestAgent())
	if err == nil || !errors.Is(err, store.ErrNotFound) || !isPermanentACPAgentConfigurationError(err) {
		t.Fatalf("missing Session candidate error = %v, permanent=%t, want permanent ErrNotFound", err, isPermanentACPAgentConfigurationError(err))
	}
}

func TestPermanentACPWorkspaceSessionPlanningError(t *testing.T) {
	for _, err := range []error{store.ErrNotFound, store.ErrConflict, store.ErrValidation} {
		if !permanentACPWorkspaceSessionPlanningError(err) {
			t.Fatalf("planning error %v was not classified as permanent", err)
		}
	}
	if permanentACPWorkspaceSessionPlanningError(errors.New("store temporarily unavailable")) {
		t.Fatal("transient store error was classified as permanent")
	}
}

func TestAgentExecutionBindingDoesNotCreateWorkspaceSessionBeforeSnapshotPersistence(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "snapshot-failure", Create: true, Append: true}
	reconciler, durableStore := newBindingTestReconciler(t, task, bindingTestNamespace())
	reconciler.ExecutionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProviderAgentSandbox
	reconciler.DurableControlStore = durableStore
	reconciler.SessionManager = NewSessionManager(durableStore)
	reconciler.ControllerEpochManager = NewControllerEpochManager(durableStore, "workspace-session-snapshot-test")
	reconciler.AgentExecutionSnapshots = failingAgentExecutionSnapshotPersistStore{
		AgentExecutionSnapshotStore: reconciler.AgentExecutionSnapshots,
	}

	if _, err, handled := reconciler.ensureAgentExecutionBinding(ctx, task.DeepCopy(), bindingTestAgent()); err != nil || !handled {
		t.Fatalf("snapshot failure binding result = handled=%v err=%v, want handled retry", handled, err)
	}
	if _, err := durableStore.GetSessionControl(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("snapshot persistence failure created SessionControl: %v", err)
	}
	if _, err := durableStore.GetSession(ctx, task.Namespace, task.Spec.SessionRef.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("snapshot persistence failure created transcript Session: %v", err)
	}
}

func TestSessionWorkspacePoolIdentityRotatesWithSessionUID(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpWorkspaceTestSessionName}
	agent := bindingTestAgent()
	reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	plainPlan, err := PlanACPRuntimeWithConfiguration(task, agent, reconciler.ACPRuntimeImages, configuration)
	if err != nil {
		t.Fatal(err)
	}
	first, err := resolveACPWorkspaceBinding(task, corev1alpha1.WorkspaceProviderAgentSandbox, false, "session-incarnation-one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveACPWorkspaceBinding(task, corev1alpha1.WorkspaceProviderAgentSandbox, false, "session-incarnation-two")
	if err != nil {
		t.Fatal(err)
	}
	firstPlan, err := applyACPWorkspaceBindingToPlan(plainPlan, first)
	if err != nil {
		t.Fatal(err)
	}
	secondPlan, err := applyACPWorkspaceBindingToPlan(plainPlan, second)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan.PoolName == secondPlan.PoolName || first.BindingDigest == second.BindingDigest {
		t.Fatalf("recreated Session reused workspace identity: first=%s/%s second=%s/%s", firstPlan.PoolName, first.BindingDigest, secondPlan.PoolName, second.BindingDigest)
	}
}

func TestSessionWorkspaceLineageConfigPinsRuntimeImageAndBinding(t *testing.T) {
	profileDigest := harnessv2.ProfileDigest(testControlDigestForDispatcher("session-workspace-lineage-profile"))
	plan := ACPRuntimePlan{
		Image:  "registry.example/orka-acp@sha256:" + strings.Repeat("a", 64),
		Digest: profileDigest,
		Workspace: &ACPRuntimeWorkspaceBinding{
			ReusePolicy:   corev1alpha1.WorkspaceReusePolicySession,
			BindingDigest: testControlDigestForDispatcher("session-workspace-lineage-binding"),
		},
	}
	baseDigest, err := acpSessionLineageConfigDigest(plan)
	if err != nil {
		t.Fatal(err)
	}

	changedImage := plan
	changedImage.Image = "registry.example/orka-acp@sha256:" + strings.Repeat("b", 64)
	changedImageDigest, err := acpSessionLineageConfigDigest(changedImage)
	if err != nil {
		t.Fatal(err)
	}
	changedBinding := plan
	changedBinding.Workspace = new(ACPRuntimeWorkspaceBinding)
	*changedBinding.Workspace = *plan.Workspace
	changedBinding.Workspace.BindingDigest = testControlDigestForDispatcher("session-workspace-lineage-binding-next")
	changedBindingDigest, err := acpSessionLineageConfigDigest(changedBinding)
	if err != nil {
		t.Fatal(err)
	}
	if baseDigest == changedImageDigest || baseDigest == changedBindingDigest {
		t.Fatalf("workspace lineage digest did not rotate: base=%s image=%s binding=%s", baseDigest, changedImageDigest, changedBindingDigest)
	}

	plainPlan := plan
	plainPlan.Workspace = nil
	plainDigest, err := acpSessionLineageConfigDigest(plainPlan)
	if err != nil || plainDigest != string(profileDigest) {
		t.Fatalf("plain lineage digest = %q, %v, want profile digest %q", plainDigest, err, profileDigest)
	}
}

func TestVerifiedSnapshotWorkspaceBindingRejectsTamperedIdentity(t *testing.T) {
	binding := &corev1alpha1.AgentExecutionBinding{
		Task: corev1alpha1.AgentExecutionBindingTaskRef{UID: types.UID("11111111-1111-1111-1111-111111111111")},
	}
	frozen, err := resolveACPWorkspaceBinding(workspaceBindingTestTask(nil), corev1alpha1.WorkspaceProviderAgentSandbox, false, "")
	if err != nil || frozen == nil {
		t.Fatalf("resolveACPWorkspaceBinding() = %#v, %v", frozen, err)
	}
	valid := agentExecutionSnapshotWorkspaceBinding{
		Provider:      string(frozen.Provider),
		ReusePolicy:   string(frozen.ReusePolicy),
		CleanupPolicy: string(frozen.CleanupPolicy),
		WorkspaceSlot: frozen.WorkspaceSlot,
		SessionUID:    frozen.SessionUID,
		SessionKey:    frozen.SessionKey,
		BindingDigest: frozen.BindingDigest,
	}

	if _, err := verifiedSnapshotWorkspaceBinding(binding, agentExecutionSnapshotBody{ExecutionWorkspace: &valid}); err != nil {
		t.Fatalf("valid frozen binding rejected: %v", err)
	}

	// A stale digest fails first; recompute a valid digest over the tampered
	// session key so the immutable Task-identity check is what rejects it.
	tamperedSession := valid
	tamperedSession.SessionKey = "task:another-task-uid"
	recomputed, err := acpWorkspaceBindingDigest(&ACPRuntimeWorkspaceBinding{
		Provider:      corev1alpha1.WorkspaceProvider(tamperedSession.Provider),
		ReusePolicy:   corev1alpha1.WorkspaceReusePolicy(tamperedSession.ReusePolicy),
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicy(tamperedSession.CleanupPolicy),
		WorkspaceSlot: tamperedSession.WorkspaceSlot,
		SessionUID:    tamperedSession.SessionUID,
		SessionKey:    tamperedSession.SessionKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	tamperedSession.BindingDigest = recomputed
	if _, err := verifiedSnapshotWorkspaceBinding(binding, agentExecutionSnapshotBody{ExecutionWorkspace: &tamperedSession}); err == nil ||
		!strings.Contains(err.Error(), "session key") {
		t.Fatalf("tampered session key error = %v, want session key mismatch", err)
	}

	tamperedDigest := valid
	tamperedDigest.CleanupPolicy = string(corev1alpha1.WorkspaceCleanupPolicyRetain)
	if _, err := verifiedSnapshotWorkspaceBinding(binding, agentExecutionSnapshotBody{ExecutionWorkspace: &tamperedDigest}); err == nil {
		t.Fatal("tampered cleanup policy passed verification")
	}
}

func TestVerifiedSnapshotWorkspaceBindingAcceptsLegacyClassOnDetachDigest(t *testing.T) {
	task := acpClassTestTask()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	reconciler := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := reconciler.resolveACPWorkspaceClass(context.Background(), task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	frozen, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve class binding: %v", err)
	}
	legacyDigest, err := legacyACPWorkspaceBindingDigest(frozen)
	if err != nil {
		t.Fatalf("legacy binding digest: %v", err)
	}
	if legacyDigest == frozen.BindingDigest {
		t.Fatal("legacy classOnDetach digest unexpectedly matches the current digest")
	}
	legacy := *frozen
	legacy.BindingDigest = legacyDigest
	if err := validateACPWorkspaceBindingValues(&legacy); err == nil {
		t.Fatal("new binding validation unexpectedly accepted the legacy digest")
	}

	snapshot := agentExecutionSnapshotWorkspaceBinding{
		Provider:          string(frozen.Provider),
		ReusePolicy:       string(frozen.ReusePolicy),
		CleanupPolicy:     string(frozen.CleanupPolicy),
		WorkspaceSlot:     frozen.WorkspaceSlot,
		SessionUID:        frozen.SessionUID,
		SessionKey:        frozen.SessionKey,
		TemplateNamespace: frozen.TemplateNamespace,
		TemplateName:      frozen.TemplateName,
		Class:             snapshotWorkspaceClassFromBinding(frozen.Class),
		BindingDigest:     legacyDigest,
	}
	binding := &corev1alpha1.AgentExecutionBinding{
		Task: corev1alpha1.AgentExecutionBindingTaskRef{UID: task.UID},
	}
	restored, err := verifiedSnapshotWorkspaceBinding(
		binding,
		agentExecutionSnapshotBody{ExecutionWorkspace: &snapshot},
	)
	if err != nil {
		t.Fatalf("pre-upgrade class snapshot rejected: %v", err)
	}
	if restored.BindingDigest != legacyDigest {
		t.Fatalf("restored digest = %q, want legacy digest %q", restored.BindingDigest, legacyDigest)
	}
}

func TestEnsureACPRuntimePoolCreatesWorkspaceBackedPool(t *testing.T) {
	ctx := context.Background()
	task := workspaceBindingTestTask(nil)
	agent := bindingTestAgent()
	reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, task, agent)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanACPRuntimeWithConfiguration(task, agent, reconciler.ACPRuntimeImages, configuration)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := resolveACPWorkspaceBinding(task, corev1alpha1.WorkspaceProviderAgentSandbox, false, "")
	if err != nil {
		t.Fatal(err)
	}
	plan, err = applyACPWorkspaceBindingToPlan(plan, binding)
	if err != nil {
		t.Fatal(err)
	}

	pool, preexisting, err := reconciler.ensureACPRuntimePool(ctx, task.Namespace, plan, "", "", "")
	if err != nil {
		t.Fatalf("ensureACPRuntimePool() error = %v", err)
	}
	if preexisting {
		t.Fatal("new workspace RuntimePool reported as preexisting")
	}
	if pool.Spec.ExecutionWorkspace == nil ||
		pool.Spec.ExecutionWorkspace.Provider != corev1alpha1.WorkspaceProviderAgentSandbox ||
		pool.Spec.ExecutionWorkspace.BindingDigest != binding.BindingDigest {
		t.Fatalf("pool executionWorkspace = %#v, want frozen binding", pool.Spec.ExecutionWorkspace)
	}
	if pool.Spec.Capacity == nil || pool.Spec.Capacity.MaxResidentSessions != 1 || pool.Spec.Capacity.MaxRunningPrompts != 1 {
		t.Fatalf("pool capacity = %#v, want single-session 1/1", pool.Spec.Capacity)
	}
	if pool.Labels[acpRuntimeWorkspaceProviderLabel] != string(corev1alpha1.WorkspaceProviderAgentSandbox) {
		t.Fatalf("pool labels = %#v, want workspace provider label", pool.Labels)
	}
	reattached, preexisting, err := reconciler.ensureACPRuntimePool(ctx, task.Namespace, plan, "", "", "")
	if err != nil {
		t.Fatalf("reattach workspace RuntimePool: %v", err)
	}
	if !preexisting || reattached.UID != pool.UID {
		t.Fatalf("reattached pool = %s/%t, want existing UID %s", reattached.UID, preexisting, pool.UID)
	}

	// A frozen plain plan must never bind to a workspace-backed pool.
	plainPlan := plan
	plainPlan.Workspace = nil
	if _, _, err := reconciler.ensureACPRuntimePool(ctx, task.Namespace, plainPlan, "", "", ""); err == nil ||
		!strings.Contains(err.Error(), "execution workspace binding does not match") {
		t.Fatalf("mismatched pool binding error = %v, want exact-binding rejection", err)
	}
}

func TestEnsureACPRuntimePoolValidatesCreateRaceWinner(t *testing.T) {
	newFixture := func(t *testing.T) (*TaskReconciler, *corev1alpha1.Task, ACPRuntimePlan) {
		t.Helper()
		ctx := context.Background()
		task := workspaceBindingTestTask(nil)
		reconciler, _ := newBindingTestReconciler(t, task, bindingTestNamespace())
		configuration, err := resolveACPAgentSessionConfiguration(ctx, reconciler.Client, task, bindingTestAgent())
		if err != nil {
			t.Fatal(err)
		}
		plan, err := PlanACPRuntimeWithConfiguration(task, bindingTestAgent(), reconciler.ACPRuntimeImages, configuration)
		if err != nil {
			t.Fatal(err)
		}
		binding, err := resolveACPWorkspaceBinding(task, corev1alpha1.WorkspaceProviderAgentSandbox, false, "")
		if err != nil {
			t.Fatal(err)
		}
		plan, err = applyACPWorkspaceBindingToPlan(plan, binding)
		if err != nil {
			t.Fatal(err)
		}
		return reconciler, task, plan
	}

	t.Run("rejects mismatched winner", func(t *testing.T) {
		reconciler, task, plan := newFixture(t)
		withWatch, ok := reconciler.Client.(client.WithWatch)
		if !ok {
			t.Fatal("binding test client does not support watch")
		}
		reconciler.Client = interceptor.NewClient(withWatch, interceptor.Funcs{
			Create: func(ctx context.Context, _ client.WithWatch, object client.Object, options ...client.CreateOption) error {
				pool, ok := object.(*corev1alpha1.RuntimePool)
				if !ok {
					return withWatch.Create(ctx, object, options...)
				}
				winner := pool.DeepCopy()
				winner.Spec.ExecutionWorkspace = nil
				if err := withWatch.Create(ctx, winner, options...); err != nil {
					return err
				}
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "runtimepools"}, pool.Name,
				)
			},
		})

		if _, _, err := reconciler.ensureACPRuntimePool(context.Background(), task.Namespace, plan, "", "", ""); err == nil ||
			!strings.Contains(err.Error(), "execution workspace binding does not match") {
			t.Fatalf("create-race winner error = %v, want exact workspace-binding rejection", err)
		}
	})

	t.Run("activates matching winner", func(t *testing.T) {
		reconciler, task, plan := newFixture(t)
		withWatch, ok := reconciler.Client.(client.WithWatch)
		if !ok {
			t.Fatal("binding test client does not support watch")
		}
		reconciler.Client = interceptor.NewClient(withWatch, interceptor.Funcs{
			Create: func(ctx context.Context, _ client.WithWatch, object client.Object, options ...client.CreateOption) error {
				pool, ok := object.(*corev1alpha1.RuntimePool)
				if !ok {
					return withWatch.Create(ctx, object, options...)
				}
				winner := pool.DeepCopy()
				winner.Spec.DesiredReplicas = 0
				if err := withWatch.Create(ctx, winner, options...); err != nil {
					return err
				}
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "runtimepools"}, pool.Name,
				)
			},
		})

		pool, preexisting, err := reconciler.ensureACPRuntimePool(context.Background(), task.Namespace, plan, "", "", "")
		if err != nil {
			t.Fatalf("activate matching create-race winner: %v", err)
		}
		if !preexisting || pool.Spec.DesiredReplicas != 1 {
			t.Fatalf("create-race winner = preexisting:%t desired:%d, want true/1", preexisting, pool.Spec.DesiredReplicas)
		}
	})
}

func TestACPRuntimePoolWorkspaceMatchesPlanRequiresExactProviderFields(t *testing.T) {
	digest := "sha256:" + strings.Repeat("9", 64)
	plan := ACPRuntimePlan{Workspace: &ACPRuntimeWorkspaceBinding{
		Provider: corev1alpha1.WorkspaceProviderSubstrate, BindingDigest: digest,
		TemplateNamespace: substrateTestTemplateNamespace, TemplateName: substrateTestBaseTemplateName,
	}}
	pool := &corev1alpha1.RuntimePool{Spec: corev1alpha1.RuntimePoolSpec{
		ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
			Provider: corev1alpha1.WorkspaceProviderSubstrate, BindingDigest: digest,
			Substrate: &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
				BaseTemplateNamespace: substrateTestTemplateNamespace, BaseTemplateName: substrateTestBaseTemplateName,
			},
		},
	}}
	if !acpRuntimePoolWorkspaceMatchesPlan(pool, plan) {
		t.Fatal("exact Substrate workspace binding did not match")
	}

	pool.Spec.ExecutionWorkspace.Substrate.BaseTemplateName = "other-infrastructure"
	if acpRuntimePoolWorkspaceMatchesPlan(pool, plan) {
		t.Fatal("Substrate workspace binding ignored the infrastructure template name")
	}
	pool.Spec.ExecutionWorkspace.Substrate.BaseTemplateName = plan.Workspace.TemplateName
	pool.Spec.ExecutionWorkspace.Substrate.BaseTemplateNamespace = acpWorkspaceTestOtherNamespace
	if acpRuntimePoolWorkspaceMatchesPlan(pool, plan) {
		t.Fatal("Substrate workspace binding ignored the infrastructure template namespace")
	}
}

// The queue path must use the linked workspace's creation-time runtime
// namespace after mutable controller configuration drifts. Validating the
// current flag before loading the workspace would reject this continuation
// before the frozen namespace can govern pool creation or reuse.
func TestQueueACPRuntimeTaskUsesFrozenWorkspaceRuntimeNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := suspendableSubstrateFixture(t)
	task := suspendableSessionTask()
	reconciler := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	controlStore := sqlite.NewStore(db, "test")
	reconciler.DurableControlStore = controlStore
	reconciler.ControllerEpochManager = NewControllerEpochManager(controlStore, "controller-test")
	reconciler.ACPWorkspaceDispatchEnabled = true
	reconciler.SubstrateEnabled = true
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- reconciler.ControllerEpochManager.Start(epochCtx) }()
	t.Cleanup(func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("controller epoch manager shutdown: %v", err)
		}
	})
	if _, err := reconciler.ControllerEpochManager.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	bound := bindSuspendableSessionTaskForSettlement(t, reconciler, task)
	if _, err := reconciler.queueACPRuntimeTask(ctx, bound, bindingTestAgent()); err != nil {
		t.Fatalf("materialize workspace through queue: %v", err)
	}
	current := &corev1alpha1.Task{}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	workspaceName := current.Labels[acpExecutionWorkspaceLinkLabel]
	if workspaceName == "" {
		t.Fatal("queue did not link the class workspace")
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatal(err)
	}
	if got := workspace.Annotations[acpWorkspaceRuntimeNamespaceAnnotation]; got != acpTestRuntimeNamespace {
		t.Fatalf("frozen workspace runtime namespace = %q, want %q", got, acpTestRuntimeNamespace)
	}
	admitTestACPWorkspace(t, reconciler, workspace)
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.queueACPRuntimeTask(ctx, current, bindingTestAgent()); err != nil {
		t.Fatalf("attach workspace through queue: %v", err)
	}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatal(err)
	}
	acknowledgeTestACPWorkspaceAttachment(t, reconciler, workspace)

	// The current flag now aliases the Substrate template namespace and is
	// invalid for new workspaces. This existing workspace remains valid
	// because its provider-child namespace was frozen before the drift.
	reconciler.ACPRuntimeNamespace = acpTestSubstrateNamespace
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.queueACPRuntimeTask(ctx, current, bindingTestAgent()); err != nil {
		t.Fatalf("queue with frozen workspace namespace: %v", err)
	}
	var pools corev1alpha1.RuntimePoolList
	if err := reconciler.List(ctx, &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].Spec.RuntimeNamespace != acpTestRuntimeNamespace {
		t.Fatalf("workspace pools = %#v, want one pool in frozen runtime namespace %q", pools.Items, acpTestRuntimeNamespace)
	}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Execution == nil || current.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued {
		t.Fatalf("queue status = %#v, want Queued", current.Status.Execution)
	}
	if _, err := reconciler.queueACPRuntimeTask(ctx, current, bindingTestAgent()); err != nil {
		t.Fatalf("reuse pool with frozen workspace namespace: %v", err)
	}
}

// The post-materialization handshake repeats every admission and attachment
// gate through the uncached reader. A lifecycle transition between workspace
// readiness and pool creation must abort the pool before prompt dispatch.
func TestVerifyACPWorkspaceReadyForPool(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	const taskUID = "attached-task-uid"
	readyObjects := func() (*workspacev1alpha1.ExecutionWorkspace, *corev1alpha1.RuntimePool) {
		now := time.Now().UTC()
		workspace := &workspacev1alpha1.ExecutionWorkspace{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: corev1.NamespaceDefault, Name: "acp-ws-alive", UID: types.UID("alive-ws-uid"),
				Generation: 1, CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)),
				Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: "acp-ws-session-handshake"},
			},
			Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
				DesiredState:    workspacev1alpha1.ExecutionWorkspaceDesiredReady,
				AttachmentEpoch: 3,
				Attachment: &workspacev1alpha1.ExecutionWorkspaceAttachment{
					TaskRef:   workspacev1alpha1.ObjectIdentityReference{UID: types.UID(taskUID)},
					Epoch:     3,
					ExpiresAt: metav1.NewTime(now.Add(time.Hour)),
				},
				Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
					MaxLifetime: &metav1.Duration{Duration: 2 * time.Hour},
				},
			},
			Status: workspacev1alpha1.ExecutionWorkspaceStatus{
				State: workspacev1alpha1.ExecutionWorkspaceStateAttached, AttachedEpoch: 3,
			},
		}
		markWorkspaceAdmittedForPolicyReview(workspace, workspace.Generation)
		workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
		workspace.Status.AttachedEpoch = 3
		workspace.Status.Conditions = append(workspace.Status.Conditions, metav1.Condition{
			Type: string(workspacev1alpha1.ConditionWorkspaceAttached), Status: metav1.ConditionTrue,
			Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: workspace.Generation,
		})
		pool := &corev1alpha1.RuntimePool{ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault, Name: "acp-ws-session-handshake", UID: types.UID("handshake-pool-uid"),
			Labels:      map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name},
			Annotations: map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)},
		}}
		return workspace, pool
	}
	workspace, pool := readyObjects()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workspace, pool).Build()
	r := &TaskReconciler{Client: c, APIReader: c, Scheme: scheme}
	ctx := context.Background()

	if err := r.verifyACPWorkspaceReadyForPool(ctx, pool, workspace.Name, string(workspace.UID), taskUID); err != nil {
		t.Fatalf("ready workspace must pass the handshake: %v", err)
	}

	// Finalization between readiness and pool creation is one form of the
	// same withdrawn-handshake race.
	if err := c.Delete(ctx, workspace); err != nil {
		t.Fatalf("delete workspace: %v", err)
	}
	if err := r.verifyACPWorkspaceReadyForPool(ctx, pool, workspace.Name, string(workspace.UID), taskUID); err == nil {
		t.Fatal("a finalized workspace must abort the handshake")
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &corev1alpha1.RuntimePool{}); err == nil {
		t.Fatal("the aborted pool must be deleted so no prompt can dispatch through the orphan")
	}

	t.Run("retries pool deletion after a resource version conflict", func(t *testing.T) {
		workspace, pool := readyObjects()
		workspace.Spec.CoreAdmission.AdmittedGeneration = 0
		withWatch := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workspace, pool).Build()
		deleteCalls := 0
		kubeClient := interceptor.NewClient(withWatch, interceptor.Funcs{
			Delete: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.DeleteOption) error {
				if _, isPool := object.(*corev1alpha1.RuntimePool); !isPool {
					return delegate.Delete(ctx, object, options...)
				}
				deleteCalls++
				if deleteCalls == 1 {
					current := &corev1alpha1.RuntimePool{}
					key := client.ObjectKeyFromObject(object)
					if err := delegate.Get(ctx, key, current); err != nil {
						return err
					}
					current.Status.ObservedGeneration++
					if err := delegate.Update(ctx, current); err != nil {
						return err
					}
					return apierrors.NewConflict(
						schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "runtimepools"},
						object.GetName(), errors.New("simulated RuntimePool controller update"),
					)
				}
				return delegate.Delete(ctx, object, options...)
			},
		})
		reconciler := &TaskReconciler{Client: kubeClient, APIReader: kubeClient, Scheme: scheme}
		if err := reconciler.verifyACPWorkspaceReadyForPool(
			context.Background(), pool, workspace.Name, string(workspace.UID), taskUID,
		); err == nil || !strings.Contains(err.Error(), "the pool was aborted") {
			t.Fatalf("handshake error = %v, want proven pool abortion", err)
		}
		if deleteCalls != 2 {
			t.Fatalf("RuntimePool delete calls = %d, want 2", deleteCalls)
		}
		if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
			t.Fatalf("RuntimePool survived retried deletion: %v", err)
		}
	})

	tests := []struct {
		name   string
		mutate func(*workspacev1alpha1.ExecutionWorkspace)
	}{
		{
			name: "quarantined intent",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
			},
		},
		{
			name: "terminal failure",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
			},
		},
		{
			name: "core admission withdrawn",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Spec.CoreAdmission.AdmittedGeneration = 0
			},
		},
		{
			name: "attachment revocation started",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = "3 2026-08-23T00:00:00Z"
			},
		},
		{
			name: "attachment epoch not enforced",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Status.AttachedEpoch = 0
			},
		},
		{
			name: "attached to another Task",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Spec.Attachment.TaskRef.UID = types.UID("other-task-uid")
			},
		},
		{
			name: "maximum lifetime elapsed",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.CreationTimestamp = metav1.NewTime(time.Now().Add(-3 * time.Hour))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workspace, pool := readyObjects()
			tt.mutate(workspace)
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workspace, pool).Build()
			reconciler := &TaskReconciler{Client: kubeClient, APIReader: kubeClient, Scheme: scheme}
			if err := reconciler.verifyACPWorkspaceReadyForPool(
				context.Background(), pool, workspace.Name, string(workspace.UID), taskUID,
			); err == nil {
				t.Fatal("withdrawn workspace readiness must abort the handshake")
			}
			if err := kubeClient.Get(context.Background(), types.NamespacedName{
				Namespace: pool.Namespace, Name: pool.Name,
			}, &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
				t.Fatalf("aborted RuntimePool must be deleted, got %v", err)
			}
		})
	}
}

func TestReapIdlePoolsDeletesStoppedWorkspacePools(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	stopped := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultNS, Name: acpWorkspaceTestRuntimePoolName, UID: types.UID("ws-pool-uid"), Generation: 1,
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: now.Add(-3 * time.Hour).Format(time.RFC3339Nano)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 0,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
				BindingDigest: "sha256:" + strings.Repeat("9", 64),
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped, ObservedGeneration: 1},
	}
	plain := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault, Name: "acp-codex-0123456789abcdef", UID: types.UID("plain-pool-uid"),
			Annotations: map[string]string{acpRuntimeLastDemandAnnotation: now.Add(-3 * time.Hour).Format(time.RFC3339Nano)},
		},
		Spec:   corev1alpha1.RuntimePoolSpec{DesiredReplicas: 0},
		Status: corev1alpha1.RuntimePoolStatus{Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(stopped, plain).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "ws-pool-gc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "ws-pool-gc-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Client: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute}
	if err := dispatcher.reapIdlePools(ctx, nil); err != nil {
		t.Fatal(err)
	}

	got := &corev1alpha1.RuntimePool{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: corev1.NamespaceDefault, Name: stopped.Name}, got); err == nil {
		t.Fatal("stopped idle workspace pool was not garbage collected")
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: corev1.NamespaceDefault, Name: plain.Name}, got); err != nil {
		t.Fatalf("plain stopped pool must be retained for reuse: %v", err)
	}

	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestReapIdlePoolsUsesFrozenWorkspaceIdleTimeoutForScaleDown(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		globalTTL     time.Duration
		idleTimeout   time.Duration
		lastDemandAgo time.Duration
		wantReplicas  int32
	}{
		{
			name:          "longer class timeout retains physical workspace",
			globalTTL:     time.Minute,
			idleTimeout:   4 * time.Hour,
			lastDemandAgo: 3 * time.Hour,
			wantReplicas:  1,
		},
		{
			name:          "shorter class timeout retires physical workspace",
			globalTTL:     4 * time.Hour,
			idleTimeout:   time.Minute,
			lastDemandAgo: 3 * time.Minute,
			wantReplicas:  0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			workspace := &workspacev1alpha1.ExecutionWorkspace{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: corev1.NamespaceDefault,
					Name:      "acp-ws-scale-timeout",
					UID:       types.UID("scale-timeout-workspace-uid"),
					Annotations: map[string]string{
						acpExecutionWorkspacePoolAnnotation: acpWorkspaceTestRuntimePoolName,
					},
				},
				Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
					Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
						IdleTimeout: &metav1.Duration{Duration: testCase.idleTimeout},
					},
				},
			}
			pool := &corev1alpha1.RuntimePool{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  corev1.NamespaceDefault,
					Name:       acpWorkspaceTestRuntimePoolName,
					UID:        types.UID("scale-timeout-pool-uid"),
					Generation: 1,
					Labels: map[string]string{
						acpExecutionWorkspaceLinkLabel: workspace.Name,
					},
					Annotations: map[string]string{
						acpRuntimeLastDemandAnnotation:     now.Add(-testCase.lastDemandAgo).Format(time.RFC3339Nano),
						acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
					},
				},
				Spec: corev1alpha1.RuntimePoolSpec{
					DesiredReplicas: 1,
					ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
						Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
						BindingDigest: "sha256:" + strings.Repeat("5", 64),
					},
				},
			}
			kubeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&corev1alpha1.RuntimePool{}).
				WithObjects(workspace, pool).
				Build()
			db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "workspace-idle-timeout.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close() //nolint:errcheck
			epochs := NewControllerEpochManager(sqlite.NewStore(db, "test"), "workspace-idle-timeout-controller")
			epochCtx, cancelEpoch := context.WithCancel(context.Background())
			epochDone := make(chan error, 1)
			go func() { epochDone <- epochs.Start(epochCtx) }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := epochs.CurrentFence(ctx); err != nil {
				t.Fatal(err)
			}

			dispatcher := &ACPDispatcher{
				Client: kubeClient, APIReader: kubeClient, Epochs: epochs, IdlePoolTTL: testCase.globalTTL,
			}
			if err := dispatcher.reapIdlePools(ctx, nil); err != nil {
				t.Fatal(err)
			}
			current := &corev1alpha1.RuntimePool{}
			if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
				t.Fatal(err)
			}
			if current.Spec.DesiredReplicas != testCase.wantReplicas {
				t.Fatalf("DesiredReplicas = %d, want %d", current.Spec.DesiredReplicas, testCase.wantReplicas)
			}

			cancelEpoch()
			if err := <-epochDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The idle reaper must never destroy quarantine evidence: a workspace the
// detach-timeout settlement deliberately preserved stays even when its
// linked pool idles at Stopped past the TTL.
func TestReapIdlePoolsPreservesQuarantinedWorkspaces(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault, Name: "acp-ws-quarantined", UID: types.UID("quarantined-ws-uid"),
			Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: acpWorkspaceTestRuntimePoolName},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined,
		},
	}
	stopped := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault, Name: acpWorkspaceTestRuntimePoolName, UID: types.UID("ws-pool-uid"), Generation: 1,
			Labels: map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name},
			Annotations: map[string]string{
				acpRuntimeLastDemandAnnotation:     now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 0,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
				BindingDigest: "sha256:" + strings.Repeat("8", 64),
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped, ObservedGeneration: 1},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(stopped, workspace).Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, IdlePoolTTL: time.Minute}
	ctx := context.Background()
	if err := dispatcher.reapStoppedWorkspacePool(ctx, stopped, 0, time.Now().UTC()); err != nil {
		t.Fatalf("reap stopped workspace pool: %v", err)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: corev1.NamespaceDefault, Name: workspace.Name}, &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("the quarantined workspace must survive the idle reaper: %v", err)
	}
}

func TestReapStoppedWorkspacePoolHonorsFrozenIdleTimeout(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault,
			Name:      "acp-ws-idle-timeout",
			UID:       types.UID("idle-timeout-workspace-uid"),
			Annotations: map[string]string{
				acpExecutionWorkspacePoolAnnotation: acpWorkspaceTestRuntimePoolName,
			},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
			Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
				IdleTimeout: &metav1.Duration{Duration: 4 * time.Hour},
			},
		},
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  corev1.NamespaceDefault,
			Name:       acpWorkspaceTestRuntimePoolName,
			UID:        types.UID("idle-timeout-pool-uid"),
			Generation: 1,
			Labels: map[string]string{
				acpExecutionWorkspaceLinkLabel: workspace.Name,
			},
			Annotations: map[string]string{
				acpRuntimeLastDemandAnnotation:     now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 0,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
				BindingDigest: "sha256:" + strings.Repeat("6", 64),
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle:          corev1alpha1.RuntimePoolLifecycleStopped,
			ObservedGeneration: 1,
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(workspace, pool).
		Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient, IdlePoolTTL: time.Minute}
	ctx := context.Background()

	if err := dispatcher.reapStoppedWorkspacePool(ctx, pool, 0, now); err != nil {
		t.Fatalf("reap before frozen idle timeout: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(workspace), &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("workspace was deleted before frozen idle timeout: %v", err)
	}

	if err := dispatcher.reapStoppedWorkspacePool(ctx, pool, 0, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("reap after frozen idle timeout: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(workspace), &workspacev1alpha1.ExecutionWorkspace{}); !apierrors.IsNotFound(err) {
		t.Fatalf("workspace survived past frozen idle timeout: %v", err)
	}
}

func TestReapStoppedWorkspacePoolUsesAPIReaderForWorkspaceAbsence(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: corev1.NamespaceDefault,
			Name:      "acp-ws-api-reader",
			UID:       types.UID("api-reader-workspace-uid"),
			Finalizers: []string{
				"workspace.orka.ai/test-finalizer",
			},
			Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: acpWorkspaceTestRuntimePoolName},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady},
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  corev1.NamespaceDefault,
			Name:       acpWorkspaceTestRuntimePoolName,
			UID:        types.UID("api-reader-pool-uid"),
			Generation: 1,
			Labels:     map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name},
			Annotations: map[string]string{
				acpRuntimeLastDemandAnnotation:     now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			DesiredReplicas: 0,
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
				BindingDigest: "sha256:" + strings.Repeat("7", 64),
			},
		},
		Status: corev1alpha1.RuntimePoolStatus{
			Lifecycle:          corev1alpha1.RuntimePoolLifecycleStopped,
			ObservedGeneration: 1,
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}).
		WithObjects(workspace, pool).
		Build()
	cachedClient := &missingExecutionWorkspaceGetClient{Client: baseClient, key: client.ObjectKeyFromObject(workspace)}
	dispatcher := &ACPDispatcher{Client: cachedClient, APIReader: baseClient, IdlePoolTTL: time.Minute}
	ctx := context.Background()
	if err := dispatcher.reapStoppedWorkspacePool(ctx, pool, 0, now); err != nil {
		t.Fatalf("reap stopped workspace pool: %v", err)
	}

	currentWorkspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), currentWorkspace); err != nil {
		t.Fatalf("workspace finalization was bypassed: %v", err)
	}
	if currentWorkspace.DeletionTimestamp.IsZero() {
		t.Fatal("workspace was not sent through controller-first finalization")
	}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); err != nil {
		t.Fatalf("RuntimePool was deleted before workspace finalization: %v", err)
	}
}

type missingExecutionWorkspaceGetClient struct {
	client.Client
	key client.ObjectKey
}

func (c *missingExecutionWorkspaceGetClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*workspacev1alpha1.ExecutionWorkspace); ok && key == c.key {
		return apierrors.NewNotFound(schema.GroupResource{
			Group: workspacev1alpha1.GroupVersion.Group, Resource: "executionworkspaces",
		}, key.Name)
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func TestProjectACPExecutionWorkspaceStatusTransitions(t *testing.T) {
	scheme := bindingTestScheme(t)
	task := workspaceBindingTestTask(nil)
	task.Labels = map[string]string{acpRuntimeTaskPoolLabel: acpWorkspaceTestRuntimePoolName}
	task.Status = corev1alpha1.TaskStatus{
		Phase: corev1alpha1.TaskPhaseRunning,
		Execution: &corev1alpha1.TaskExecutionStatus{
			State:           corev1alpha1.TaskExecutionStateRunning,
			RuntimePoolName: acpWorkspaceTestRuntimePoolName,
		},
		ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
			Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
			Phase:    corev1alpha1.ExecutionWorkspacePhasePending,
			Reason:   corev1alpha1.ExecutionWorkspaceReasonPending,
			Reused:   true,
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task).Build()
	reconciler := &TaskReconciler{Client: kubeClient, Scheme: scheme}
	ctx := context.Background()

	if err := reconciler.projectACPExecutionWorkspaceStatus(ctx, task); err != nil {
		t.Fatalf("project running: %v", err)
	}
	if task.Status.ExecutionWorkspace.Phase != corev1alpha1.ExecutionWorkspacePhaseReady ||
		task.Status.ExecutionWorkspace.Reason != corev1alpha1.ExecutionWorkspaceReasonReady ||
		!task.Status.ExecutionWorkspace.Reused {
		t.Fatalf("running projection = %q/%q, want Ready/WorkspaceReady", task.Status.ExecutionWorkspace.Phase, task.Status.ExecutionWorkspace.Reason)
	}

	task.Status.Execution.State = corev1alpha1.TaskExecutionStateSucceeded
	task.Status.Delivery = &corev1alpha1.TaskDeliveryStatus{State: corev1alpha1.TaskDeliveryStateValidating}
	if err := reconciler.projectACPExecutionWorkspaceStatus(ctx, task); err != nil {
		t.Fatalf("project successful execution with nonterminal delivery: %v", err)
	}
	if task.Status.ExecutionWorkspace.Phase != corev1alpha1.ExecutionWorkspacePhaseReady {
		t.Fatalf("nonterminal delivery projection = %q, want Ready", task.Status.ExecutionWorkspace.Phase)
	}

	task.Status.Delivery.State = corev1alpha1.TaskDeliveryStateVerifiedExact
	if err := reconciler.projectACPExecutionWorkspaceStatus(ctx, task); err != nil {
		t.Fatalf("project terminal delivery: %v", err)
	}
	if task.Status.ExecutionWorkspace.Phase != corev1alpha1.ExecutionWorkspacePhaseReleased ||
		task.Status.ExecutionWorkspace.Reason != corev1alpha1.ExecutionWorkspaceReasonReleased {
		t.Fatalf("terminal projection = %q/%q, want Released/WorkspaceReleased", task.Status.ExecutionWorkspace.Phase, task.Status.ExecutionWorkspace.Reason)
	}

	// A Failed projection is never overridden.
	task.Status.ExecutionWorkspace.Phase = corev1alpha1.ExecutionWorkspacePhaseFailed
	task.Status.ExecutionWorkspace.Reason = corev1alpha1.ExecutionWorkspaceReasonValidationFailed
	if err := reconciler.projectACPExecutionWorkspaceStatus(ctx, task); err != nil {
		t.Fatalf("project failed: %v", err)
	}
	if task.Status.ExecutionWorkspace.Phase != corev1alpha1.ExecutionWorkspacePhaseFailed {
		t.Fatal("Failed workspace projection was overridden")
	}
}

// A resumed lineage asserts a committed durable checkpoint only when a
// RuntimeSession actually committed one before the suspension: a first Task
// cancelled before session creation validly suspends a volume that never held
// a checkpoint, and its continuation must materialize fresh instead of
// failing closed forever over data that never existed.
func TestTaskExpectsDurableResumeRequiresCommittedSession(t *testing.T) {
	scheme := bindingTestScheme(t)
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register workspace scheme: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "acp-ws-lineage", UID: types.UID("ws-lineage-uid"),
			Annotations: map[string]string{acpWorkspaceResumedLineageAnnotation: booleanTrueValue},
		},
	}
	task := workspaceBindingTestTask(nil)
	task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name}
	task.Annotations = map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task, workspace).Build()
	dispatcher := &ACPDispatcher{Client: kubeClient, APIReader: kubeClient}
	ctx := context.Background()

	expects, _, err := dispatcher.taskExpectsDurableResume(ctx, task)
	if err != nil {
		t.Fatalf("resume expectation: %v", err)
	}
	if expects {
		t.Fatal("a lineage with no committed durable session must not assert a checkpoint")
	}

	// Session creation records the synchronous durable commit with its
	// generation; the floor only ever advances.
	if err := dispatcher.markLinkedWorkspaceDurableSessionCommitted(ctx, task, 3); err != nil {
		t.Fatalf("record durable session commit: %v", err)
	}
	expects, floor, err := dispatcher.taskExpectsDurableResume(ctx, task)
	if err != nil {
		t.Fatalf("resume expectation after commit: %v", err)
	}
	if !expects || floor != 3 {
		t.Fatalf("expects=%v floor=%d, want an asserted checkpoint at generation 3", expects, floor)
	}

	// The stamp is monotonic and pinned to the workspace incarnation: an
	// older generation never regresses the floor.
	if err := dispatcher.markLinkedWorkspaceDurableSessionCommitted(ctx, task, 2); err != nil {
		t.Fatalf("repeat commit record: %v", err)
	}
	if _, floor, _ = dispatcher.taskExpectsDurableResume(ctx, task); floor != 3 {
		t.Fatalf("floor = %d after an older stamp, want the monotonic 3", floor)
	}
	task.Annotations[acpExecutionWorkspaceUIDAnnotation] = "different-incarnation"
	expects, _, err = dispatcher.taskExpectsDurableResume(ctx, task)
	if err != nil || expects {
		t.Fatalf("a different incarnation must not inherit the lineage assertion (expects=%v err=%v)", expects, err)
	}

	// Corrupt or zero generation records must fail dispatch instead of
	// disabling the stale-snapshot floor.
	task.Annotations[acpExecutionWorkspaceUIDAnnotation] = string(workspace.UID)
	for _, recorded := range []string{"not-a-generation", "0"} {
		if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, workspace); err != nil {
			t.Fatalf("read workspace before corrupting generation: %v", err)
		}
		base := workspace.DeepCopy()
		workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = recorded
		if err := kubeClient.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
			t.Fatalf("write invalid durable generation %q: %v", recorded, err)
		}
		if _, _, err := dispatcher.taskExpectsDurableResume(ctx, task); err == nil ||
			!strings.Contains(err.Error(), "invalid durable checkpoint generation") {
			t.Fatalf("invalid generation %q error = %v", recorded, err)
		}
	}
}

// The attachment-epoch projection must claim only an epoch the adapter is
// actually enforcing: the requested spec epoch and the enforced status epoch
// deliberately diverge while attachment is pending and after max-lifetime
// enforcement clears the enforced epoch.
func TestProjectACPClassAttachmentIdentityRequiresAcknowledgedEpoch(t *testing.T) {
	scheme := bindingTestScheme(t)
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register workspace scheme: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: acpTestNamespace, Name: "acp-ws-epoch", UID: types.UID("ws-epoch-uid")},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Attachment: &workspacev1alpha1.ExecutionWorkspaceAttachment{
				TaskRef: workspacev1alpha1.ObjectIdentityReference{Name: "epoch-task", UID: types.UID("epoch-task-uid")},
				Epoch:   3,
			},
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{
			State: workspacev1alpha1.ExecutionWorkspaceStateAttached,
		},
	}
	task := workspaceBindingTestTask(nil)
	task.UID = types.UID("epoch-task-uid")
	task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name}
	task.Annotations = map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task, workspace).Build()
	reconciler := &TaskReconciler{Client: kubeClient, Scheme: scheme}
	ctx := context.Background()

	// Pending attachment: the adapter has not acknowledged the epoch yet.
	next := &corev1alpha1.ExecutionWorkspaceStatus{}
	if err := reconciler.projectACPClassAttachmentIdentity(ctx, task, next); err != nil {
		t.Fatalf("project pending attachment identity: %v", err)
	}
	if next.AttachedEpoch != 0 {
		t.Fatalf("unacknowledged attachment projected epoch %d; the Task may only claim an enforced epoch", next.AttachedEpoch)
	}

	// The adapter acknowledged exactly this epoch.
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	current.Status.AttachedEpoch = 3
	if err := kubeClient.Update(ctx, current); err != nil {
		t.Fatalf("acknowledge epoch: %v", err)
	}
	next = &corev1alpha1.ExecutionWorkspaceStatus{}
	if err := reconciler.projectACPClassAttachmentIdentity(ctx, task, next); err != nil {
		t.Fatalf("project acknowledged attachment identity: %v", err)
	}
	if next.AttachedEpoch != 3 {
		t.Fatalf("acknowledged attachment projected epoch %d, want 3", next.AttachedEpoch)
	}
}

// Settlement completion refreshes the Released projection: the terminal
// transition runs before revocation, so without the refresh the status would
// permanently claim state Attached and the pre-revocation epoch.
func TestRefreshACPReleasedWorkspaceProjectionClearsAttachment(t *testing.T) {
	scheme := bindingTestScheme(t)
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register workspace scheme: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: acpTestNamespace, Name: "acp-ws-refresh", UID: types.UID("ws-refresh-uid")},
		Status:     workspacev1alpha1.ExecutionWorkspaceStatus{State: workspacev1alpha1.ExecutionWorkspaceStateSuspended},
	}
	task := workspaceBindingTestTask(nil)
	task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name}
	task.Annotations = map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)}
	task.Status = corev1alpha1.TaskStatus{
		ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
			Phase:         corev1alpha1.ExecutionWorkspacePhaseReleased,
			State:         string(workspacev1alpha1.ExecutionWorkspaceStateAttached),
			AttachedEpoch: 3,
		},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).WithObjects(task, workspace).Build()
	reconciler := &TaskReconciler{Client: kubeClient, Scheme: scheme}
	ctx := context.Background()

	if err := reconciler.refreshACPReleasedWorkspaceProjection(ctx, task); err != nil {
		t.Fatalf("refresh released projection: %v", err)
	}
	if task.Status.ExecutionWorkspace.AttachedEpoch != 0 ||
		task.Status.ExecutionWorkspace.State != string(workspacev1alpha1.ExecutionWorkspaceStateSuspended) {
		t.Fatalf("refreshed projection = %+v, want the revoked epoch cleared and the live workspace state", task.Status.ExecutionWorkspace)
	}

	// A workspace held only by its cleanup finalizer still serves cached
	// pre-delete status; the refresh must not freeze that stale state into
	// the released Task.
	deleting := workspace.DeepCopy()
	deleting.Name = "acp-ws-refresh-deleting"
	deleting.UID = types.UID("ws-refresh-deleting-uid")
	deleting.ResourceVersion = ""
	deleting.Finalizers = []string{"workspace.orka.ai/cleanup"}
	if err := kubeClient.Create(ctx, deleting); err != nil {
		t.Fatalf("create deleting workspace: %v", err)
	}
	if err := kubeClient.Delete(ctx, deleting); err != nil {
		t.Fatalf("start deleting workspace: %v", err)
	}
	task.Labels[acpExecutionWorkspaceLinkLabel] = deleting.Name
	task.Annotations[acpExecutionWorkspaceUIDAnnotation] = string(deleting.UID)
	task.Status.ExecutionWorkspace.State = string(workspacev1alpha1.ExecutionWorkspaceStateAttached)
	task.Status.ExecutionWorkspace.AttachedEpoch = 4
	if err := reconciler.refreshACPReleasedWorkspaceProjection(ctx, task); err != nil {
		t.Fatalf("refresh over a deleting workspace: %v", err)
	}
	if task.Status.ExecutionWorkspace.AttachedEpoch != 0 || task.Status.ExecutionWorkspace.State != "" {
		t.Fatalf("deleting-workspace projection = %+v, want no copied state", task.Status.ExecutionWorkspace)
	}

	// A deleted workspace leaves no state claim even while the informer cache
	// still serves the pre-delete Ready object.
	staleWorkspace := workspace.DeepCopy()
	staleWorkspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateReady
	task.Labels[acpExecutionWorkspaceLinkLabel] = workspace.Name
	task.Annotations[acpExecutionWorkspaceUIDAnnotation] = string(workspace.UID)
	task.Status.ExecutionWorkspace.State = string(workspacev1alpha1.ExecutionWorkspaceStateAttached)
	task.Status.ExecutionWorkspace.AttachedEpoch = 5
	if err := kubeClient.Delete(ctx, workspace); err != nil {
		t.Fatalf("delete workspace: %v", err)
	}
	staleCache := interceptor.NewClient(kubeClient, interceptor.Funcs{
		Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if current, ok := object.(*workspacev1alpha1.ExecutionWorkspace); ok && key == client.ObjectKeyFromObject(staleWorkspace) {
				staleWorkspace.DeepCopyInto(current)
				return nil
			}
			return delegate.Get(ctx, key, object, options...)
		},
	})
	reconciler.Client = staleCache
	reconciler.APIReader = kubeClient
	if err := reconciler.refreshACPReleasedWorkspaceProjection(ctx, task); err != nil {
		t.Fatalf("refresh after workspace deletion: %v", err)
	}
	if task.Status.ExecutionWorkspace.AttachedEpoch != 0 || task.Status.ExecutionWorkspace.State != "" {
		t.Fatalf("post-deletion projection = %+v, want cleared attachment identity", task.Status.ExecutionWorkspace)
	}
}

// A failed suspension preserves no checkpoint: the idle reaper must be able
// to reclaim the stopped pool while a genuinely suspended (or suspending)
// workspace stays deliberately retained.
func TestReapIdlePoolsReclaimsFailedSuspensions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	makePool := func(name, workspaceName string) *corev1alpha1.RuntimePool {
		return &corev1alpha1.RuntimePool{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: defaultNS, Name: name, UID: types.UID(name + "-uid"), Generation: 1,
				Annotations: map[string]string{
					acpRuntimeLastDemandAnnotation:     now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
					acpExecutionWorkspaceUIDAnnotation: workspaceName + "-uid",
				},
				Labels: map[string]string{acpExecutionWorkspaceLinkLabel: workspaceName},
			},
			Spec: corev1alpha1.RuntimePoolSpec{
				DesiredReplicas: 0,
				ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
					Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
					BindingDigest: "sha256:" + strings.Repeat("9", 64),
				},
			},
			Status: corev1alpha1.RuntimePoolStatus{Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped, ObservedGeneration: 1},
		}
	}
	makeWorkspace := func(name, poolName string, state workspacev1alpha1.ExecutionWorkspaceState) *workspacev1alpha1.ExecutionWorkspace {
		return &workspacev1alpha1.ExecutionWorkspace{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: defaultNS, Name: name, UID: types.UID(name + "-uid"),
				Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: poolName},
			},
			Spec:   workspacev1alpha1.ExecutionWorkspaceSpec{DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredSuspended},
			Status: workspacev1alpha1.ExecutionWorkspaceStatus{State: state},
		}
	}
	suspendedPool := makePool("acp-ws-suspended-fedcba9876543210", "acp-ws-suspended")
	failedPool := makePool("acp-ws-failed-fedcba9876543210", "acp-ws-failed")
	suspended := makeWorkspace("acp-ws-suspended", suspendedPool.Name, workspacev1alpha1.ExecutionWorkspaceStateSuspended)
	failed := makeWorkspace("acp-ws-failed", failedPool.Name, workspacev1alpha1.ExecutionWorkspaceStateFailed)
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}, &workspacev1alpha1.ExecutionWorkspace{}).
		WithObjects(suspendedPool, failedPool, suspended, failed).Build()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "ws-failed-gc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "test")
	epochs := NewControllerEpochManager(controlStore, "ws-failed-gc-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		t.Fatal(err)
	}

	dispatcher := &ACPDispatcher{Client: kubeClient, Epochs: epochs, IdlePoolTTL: time.Minute}
	if err := dispatcher.reapIdlePools(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: defaultNS, Name: suspended.Name},
		&workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("a genuinely suspended workspace must be retained for cold resume: %v", err)
	}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: defaultNS, Name: failed.Name},
		&workspacev1alpha1.ExecutionWorkspace{}); err == nil {
		t.Fatal("a failed suspension preserves no checkpoint and must be reclaimed")
	}

	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

// A transient workspace read failure during the class-identity projection
// must fail the phase transition instead of persisting a projection stripped
// of ClassRef/WorkspaceRef/State/AttachedEpoch: the advanced phase would
// never retry the dropped identity.
func TestProjectACPExecutionWorkspaceStatusRetriesOnIdentityReadFailure(t *testing.T) {
	scheme := bindingTestScheme(t)
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register workspace scheme: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: acpTestNamespace, Name: "acp-ws-read-fail", UID: types.UID("ws-read-fail-uid")},
	}
	task := workspaceBindingTestTask(nil)
	task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name}
	task.Status = corev1alpha1.TaskStatus{
		Phase: corev1alpha1.TaskPhaseRunning,
		Execution: &corev1alpha1.TaskExecutionStatus{
			State:           corev1alpha1.TaskExecutionStateRunning,
			RuntimePoolName: acpWorkspaceTestRuntimePoolName,
		},
		ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
			Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
			Phase:    corev1alpha1.ExecutionWorkspacePhasePending,
			Reason:   corev1alpha1.ExecutionWorkspaceReasonPending,
		},
	}
	readErr := errors.New("injected workspace read failure")
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithObjects(task, workspace).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, isWorkspace := obj.(*workspacev1alpha1.ExecutionWorkspace); isWorkspace {
					return readErr
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
	reconciler := &TaskReconciler{Client: kubeClient, Scheme: scheme}
	ctx := context.Background()

	if err := reconciler.projectACPExecutionWorkspaceStatus(ctx, task); !errors.Is(err, readErr) {
		t.Fatalf("projection error = %v, want the surfaced read failure", err)
	}
	current := &corev1alpha1.Task{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if current.Status.ExecutionWorkspace.Phase != corev1alpha1.ExecutionWorkspacePhasePending {
		t.Fatalf("phase advanced to %q over a failed identity read; the transition must retry instead", current.Status.ExecutionWorkspace.Phase)
	}
}

// resolveACPWorkspaceBinding is the legacy (class-less) test entry point for
// resolveACPWorkspaceBindingWithClass.
//
//nolint:unparam // Keeps the production signature so call sites read as the real resolver.
func resolveACPWorkspaceBinding(
	task *corev1alpha1.Task,
	defaultProvider corev1alpha1.WorkspaceProvider,
	enforceNamespaceIsolation bool,
	sessionUID string,
) (*ACPRuntimeWorkspaceBinding, error) {
	return resolveACPWorkspaceBindingWithClass(task, defaultProvider, enforceNamespaceIsolation, sessionUID, nil)
}
