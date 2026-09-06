/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
)

const (
	acpTestProviderName = "acp-provider"
	acpTestConfigName   = "acp-config"
	acpTestClassName    = "acp-class"
	acpTestNamespace    = "default"
	acpTestSessionName  = "session-a"
	acpTestInfraName    = "infra"
	acpTestAttachedTask = "attached-task"

	acpTestSubstrateNamespace = "substrate-system"
	acpTestDurableCapacity    = "1Gi"
	acpTestSessionPoolName    = "acp-ws-session-0123456789abcdef"
	acpTestStorageProvisioner = "test.orka.ai/provisioner"
	acpTestInfraTemplateName  = "infra-template"
)

const acpTestSandboxPoolName = "acp-ws-agent-sandbox-0123456789abcdef"

func testACPWorkspaceScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testWorkspaceScheme(t)
	if err := acpworkspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add acp.workspace scheme: %v", err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add storage scheme: %v", err)
	}
	return scheme
}

// acpTestDefaultStorageClass is the cluster default StorageClass with Delete
// reclaim semantics that durable-volume profiles resolve against.
func acpTestDefaultStorageClass() *storagev1.StorageClass {
	reclaim := corev1.PersistentVolumeReclaimDelete
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "acp-test-default",
			Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": booleanTrueValue},
		},
		Provisioner:   acpTestStorageProvisioner,
		ReclaimPolicy: &reclaim,
	}
}

func TestValidateDurableStorageClassReclaimMatchesKubernetesDefaultNameTieBreak(t *testing.T) {
	t.Parallel()
	reclaim := corev1.PersistentVolumeReclaimDelete
	created := metav1.NewTime(time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC))
	defaultClass := func(name string) *storagev1.StorageClass {
		return &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: created,
				Annotations:       map[string]string{"storageclass.kubernetes.io/is-default-class": booleanTrueValue},
			},
			Provisioner:   acpTestStorageProvisioner,
			ReclaimPolicy: &reclaim,
		}
	}
	reader := fake.NewClientBuilder().WithScheme(testACPWorkspaceScheme(t)).WithObjects(
		defaultClass("zeta-default"),
		defaultClass("alpha-default"),
	).Build()

	class, err := validateDurableStorageClassReclaim(context.Background(), reader, "", "profile")
	if err != nil {
		t.Fatalf("resolve default StorageClass: %v", err)
	}
	if class.Name != "alpha-default" {
		t.Fatalf("default StorageClass = %q, want Kubernetes tie-break winner %q", class.Name, "alpha-default")
	}
}

type acpClassFixture struct {
	class    *workspacev1alpha1.ExecutionWorkspaceClass
	provider *workspacev1alpha1.ExecutionWorkspaceProvider
	config   *acpworkspacev1alpha1.RuntimeProviderConfig
	profile  *acpworkspacev1alpha1.RuntimeWorkspaceProfile
}

func (f *acpClassFixture) objects() []client.Object {
	return []client.Object{f.class, f.provider, f.config, f.profile, acpTestDefaultStorageClass()}
}

// pinProfileHash recomputes and pins the class profile hash exactly the way
// the class controller would, using the unstructured shape of the profile.
func (f *acpClassFixture) pinProfileHash(t *testing.T) {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(f.profile)
	if err != nil {
		t.Fatalf("convert profile: %v", err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(acpworkspacev1alpha1.GroupVersion.WithKind(acpWorkspaceProviderProfileKind))
	hash, err := acpWorkspaceClassProfileHash(f.class, f.provider, u)
	if err != nil {
		t.Fatalf("hash class profile: %v", err)
	}
	f.class.Status.ProfileHash = hash
}

func newACPClassFixture(t *testing.T, backend acpworkspacev1alpha1.RuntimeProviderBackend, mutate ...func(*acpClassFixture)) *acpClassFixture {
	t.Helper()
	fixture := &acpClassFixture{
		provider: &workspacev1alpha1.ExecutionWorkspaceProvider{
			ObjectMeta: metav1.ObjectMeta{Name: acpTestProviderName, UID: types.UID("acp-provider-uid"), Generation: 1},
			Spec: workspacev1alpha1.ExecutionWorkspaceProviderSpec{
				ControllerName: acpWorkspaceProviderControllerName,
				ParametersRef: workspacev1alpha1.TypedObjectReference{
					Group: acpworkspacev1alpha1.GroupVersion.Group, Kind: acpWorkspaceProviderConfigKind, Name: acpTestConfigName,
				},
				LifecycleState:    workspacev1alpha1.ExecutionWorkspaceProviderActive,
				RequiredContracts: []string{workspacev1alpha1.ContractVersionV1},
			},
			Status: workspacev1alpha1.ExecutionWorkspaceProviderStatus{
				ObservedGeneration:  1,
				PinnedParametersUID: "acp-config-uid",
				SupportedFeatures: []workspacev1alpha1.ExecutionWorkspaceFeature{
					workspacev1alpha1.WorkspaceFeatureExec,
					workspacev1alpha1.WorkspaceFeatureFiles,
					workspacev1alpha1.WorkspaceFeatureReset,
					workspacev1alpha1.WorkspaceFeatureTLS,
				},
				Conditions: []metav1.Condition{{
					Type: string(workspacev1alpha1.ConditionProviderReady), Status: metav1.ConditionTrue,
					Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: 1,
				}},
			},
		},
		config: &acpworkspacev1alpha1.RuntimeProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: acpTestConfigName, UID: types.UID("acp-config-uid"), Generation: 1},
			Spec:       acpworkspacev1alpha1.RuntimeProviderConfigSpec{Backend: backend},
		},
		profile: &acpworkspacev1alpha1.RuntimeWorkspaceProfile{
			ObjectMeta: metav1.ObjectMeta{Namespace: acpTestNamespace, Name: "acp-profile", UID: types.UID("acp-profile-uid"), Generation: 1},
		},
	}
	if backend == acpworkspacev1alpha1.RuntimeProviderBackendSubstrate {
		fixture.profile.Spec.Substrate = &acpworkspacev1alpha1.SubstrateProfileSpec{
			TemplateRef: acpworkspacev1alpha1.SubstrateTemplateReference{Name: acpTestInfraTemplateName, Namespace: acpTestSubstrateNamespace},
		}
	} else {
		fixture.provider.Status.SupportedFeatures = append(
			fixture.provider.Status.SupportedFeatures,
			workspacev1alpha1.WorkspaceFeatureSuspend,
		)
	}
	fixture.class = &workspacev1alpha1.ExecutionWorkspaceClass{
		ObjectMeta: metav1.ObjectMeta{Namespace: acpTestNamespace, Name: acpTestClassName, UID: types.UID("acp-class-uid"), Generation: 1},
		Spec: workspacev1alpha1.ExecutionWorkspaceClassSpec{
			ProviderRef: &workspacev1alpha1.ClusterObjectReference{Name: acpTestProviderName},
			ParametersRef: &workspacev1alpha1.TypedObjectReference{
				Group: acpworkspacev1alpha1.GroupVersion.Group, Kind: acpWorkspaceProviderProfileKind, Name: "acp-profile",
			},
			Mode:               workspacev1alpha1.ExecutionWorkspaceModeInteractive,
			AllowedReuseScopes: []workspacev1alpha1.WorkspaceReuseScope{workspacev1alpha1.WorkspaceReuseScopeNone, workspacev1alpha1.WorkspaceReuseScopeSession},
			Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
				DefaultOnDetach: workspacev1alpha1.WorkspaceOnDetachDelete,
				AllowedOnDetach: []workspacev1alpha1.WorkspaceOnDetach{workspacev1alpha1.WorkspaceOnDetachDelete},
				DetachTimeout:   metav1.Duration{Duration: 2 * time.Minute},
				MaxLifetime:     &metav1.Duration{Duration: 8 * time.Hour},
				DeletionPolicy: workspacev1alpha1.ExecutionWorkspaceDeletionPolicy{
					ProviderResources: workspacev1alpha1.WorkspaceDeletionActionDelete,
					PersistentVolumes: workspacev1alpha1.WorkspaceDeletionActionDelete,
					Checkpoints:       workspacev1alpha1.WorkspaceDeletionActionDelete,
				},
			},
		},
		Status: workspacev1alpha1.ExecutionWorkspaceClassStatus{
			ObservedGeneration: 1,
			ProviderRef:        &workspacev1alpha1.ClusterObjectReference{Name: acpTestProviderName},
			Conditions: []metav1.Condition{{
				Type: string(workspacev1alpha1.ConditionClassReady), Status: metav1.ConditionTrue,
				Reason: string(workspacev1alpha1.ReasonReady), ObservedGeneration: 1,
			}},
		},
	}
	for _, m := range mutate {
		m(fixture)
	}
	fixture.pinProfileHash(t)
	return fixture
}

func acpClassTestTask(mutate ...func(*corev1alpha1.Task)) *corev1alpha1.Task {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "class-task", UID: types.UID("class-task-uid"), Generation: 1,
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
				ClassRef: &corev1alpha1.WorkspaceClassReference{Name: acpTestClassName},
			}},
		},
	}
	for _, m := range mutate {
		m(task)
	}
	return task
}

// admitTestACPWorkspace stands in for the workspace core controller and the
// ACP adapter: it persists the fake API-server identity, the core admission
// marker, and an adapter-observed Ready status so attachment can proceed.
// attachTestACPWorkspace drives ensureACPClassWorkspace through the full
// attach handshake: the attach pass (never ready), core re-admission of the
// bumped generation, the adapter's enforced-epoch acknowledgement, and the
// final ready pass.
func attachTestACPWorkspace(
	t *testing.T,
	r *TaskReconciler,
	task *corev1alpha1.Task,
	plan ACPRuntimePlan,
	workspaceName string,
) (string, bool) {
	t.Helper()
	ctx := context.Background()
	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil {
		t.Fatalf("attach ensure: %v", err)
	}
	if ready {
		return name, ready
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace for attach handshake: %v", err)
	}
	if workspace.Spec.Attachment == nil {
		return name, ready
	}
	acknowledgeTestACPWorkspaceAttachment(t, r, workspace)
	name, ready, err = r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil {
		t.Fatalf("post-handshake ensure: %v", err)
	}
	return name, ready
}

func acknowledgeTestACPWorkspaceAttachment(
	t *testing.T,
	r *TaskReconciler,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) {
	t.Helper()
	if workspace.Spec.Attachment == nil {
		t.Fatal("workspace attachment is required")
	}
	admitTestACPWorkspace(t, r, workspace)
	base := workspace.DeepCopy()
	workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateAttached
	workspace.Status.AttachedEpoch = workspace.Spec.Attachment.Epoch
	workspace.Status.Conditions = append(workspace.Status.Conditions, metav1.Condition{
		Type:               string(workspacev1alpha1.ConditionWorkspaceAttached),
		Status:             metav1.ConditionTrue,
		Reason:             string(workspacev1alpha1.ReasonReady),
		ObservedGeneration: workspace.Generation,
	})
	if err := r.Status().Patch(context.Background(), workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("acknowledge enforced epoch: %v", err)
	}
}

func admitTestACPWorkspace(t *testing.T, r *TaskReconciler, workspace *workspacev1alpha1.ExecutionWorkspace) {
	t.Helper()
	ctx := context.Background()
	if workspace.UID == "" {
		// The fake client does not assign object UIDs on Create.
		workspace.UID = types.UID(workspace.Name + "-uid")
	}
	if workspace.CreationTimestamp.IsZero() {
		// The fake client does not stamp creation time; maxLifetime clamping
		// would otherwise treat the workspace as already expired.
		workspace.CreationTimestamp = metav1.Now()
	}
	markWorkspaceAdmittedForPolicyReview(workspace, workspace.Generation)
	if err := r.Update(ctx, workspace); err != nil {
		t.Fatalf("admit workspace: %v", err)
	}
	// The spec update zeroes the in-memory status under the fake status
	// subresource; re-apply the adapter-observed pieces before persisting.
	markWorkspaceAdmittedForPolicyReview(workspace, workspace.Generation)
	workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateReady
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("mark workspace status: %v", err)
	}
}

func acpClassTestReconciler(t *testing.T, objects ...client.Object) *TaskReconciler {
	t.Helper()
	scheme := testACPWorkspaceScheme(t)
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1alpha1.Task{}, acpTaskSessionNameField, acpTaskSessionNameTestIndex).
		WithStatusSubresource(
			&workspacev1alpha1.ExecutionWorkspace{},
			&workspacev1alpha1.ExecutionWorkspaceClass{},
			&workspacev1alpha1.ExecutionWorkspaceProvider{},
			&corev1alpha1.Task{},
			&corev1alpha1.RuntimePool{},
		)
	if len(objects) > 0 {
		builder = builder.WithObjects(objects...)
	}
	c := builder.Build()
	return &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		WorkspaceProviderAPIEnabled:  true,
		WorkspaceSettlementProtected: true,
	}
}

func TestResolveACPWorkspaceClassMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		backend     acpworkspacev1alpha1.RuntimeProviderBackend
		mutate      func(*acpClassFixture)
		mutateAfter func(*acpClassFixture)
		task        *corev1alpha1.Task
		wantErr     string
		check       func(*testing.T, *acpResolvedWorkspaceClass)
	}{
		{
			name:    "agent-sandbox class resolves",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			check: func(t *testing.T, resolved *acpResolvedWorkspaceClass) {
				if resolved.Backend != corev1alpha1.WorkspaceProviderAgentSandbox {
					t.Fatalf("backend = %s", resolved.Backend)
				}
				if resolved.SubstrateTemplateName != "" || resolved.SubstrateTemplateNamespace != "" {
					t.Fatalf("agent-sandbox class resolved a substrate template")
				}
				if resolved.Binding.UID != "acp-class-uid" || resolved.Binding.ProviderUID != "acp-provider-uid" {
					t.Fatalf("binding identity = %+v", resolved.Binding)
				}
				if resolved.Binding.MaxLifetime != "8h0m0s" || resolved.Binding.DetachTimeout != "2m0s" {
					t.Fatalf("binding lifecycle = %+v", resolved.Binding)
				}
			},
		},
		{
			name:    "substrate class resolves infrastructure template",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendSubstrate,
			check: func(t *testing.T, resolved *acpResolvedWorkspaceClass) {
				if resolved.Backend != corev1alpha1.WorkspaceProviderSubstrate {
					t.Fatalf("backend = %s", resolved.Backend)
				}
				if resolved.SubstrateTemplateNamespace != acpTestSubstrateNamespace || resolved.SubstrateTemplateName != acpTestInfraTemplateName {
					t.Fatalf("substrate template = %s/%s", resolved.SubstrateTemplateNamespace, resolved.SubstrateTemplateName)
				}
			},
		},
		{
			name:    "substrate template namespace defaults to the class namespace",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendSubstrate,
			mutate: func(f *acpClassFixture) {
				f.profile.Spec.Substrate.TemplateRef.Namespace = ""
			},
			check: func(t *testing.T, resolved *acpResolvedWorkspaceClass) {
				if resolved.SubstrateTemplateNamespace != acpTestNamespace {
					t.Fatalf("substrate template namespace = %s", resolved.SubstrateTemplateNamespace)
				}
			},
		},
		{
			name:    "class not ready at current generation",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutateAfter: func(f *acpClassFixture) {
				f.class.Status.Conditions[0].ObservedGeneration = 0
			},
			wantErr: "not ready at its current generation",
		},
		{
			name:    "pinned profile hash drift fails closed",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutateAfter: func(f *acpClassFixture) {
				f.class.Status.ProfileHash = "sha256:" + strings.Repeat("0", 64)
			},
			wantErr: "drifted from its pinned hash",
		},
		{
			name:    "foreign provider controllerName",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.provider.Spec.ControllerName = "someone.else/adapter"
			},
			wantErr: "is not the ACP RuntimePool adapter",
		},
		{
			name:    "draining provider rejects new workspaces",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.provider.Spec.LifecycleState = workspacev1alpha1.ExecutionWorkspaceProviderDraining
			},
			wantErr: "rejects new ACP workspaces",
		},
		{
			name:    "provider not ready",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutateAfter: func(f *acpClassFixture) {
				f.provider.Status.Conditions[0].Status = metav1.ConditionFalse
			},
			wantErr: "is not ready",
		},
		{
			name:    "class parameters kind is not an ACP profile",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.class.Spec.ParametersRef.Kind = "SomethingElse"
			},
			wantErr: "is not an ACP RuntimeWorkspaceProfile",
		},
		{
			name:    "provider parameters kind is not an ACP config",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.provider.Spec.ParametersRef.Kind = "SomethingElse"
			},
			wantErr: "is not an ACP RuntimeProviderConfig",
		},
		{
			name:    "service mode classes are rejected",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.class.Spec.Mode = workspacev1alpha1.ExecutionWorkspaceModeService
			},
			wantErr: "only Interactive classes",
		},
		{
			name:    "retaining deletion policy is rejected",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.class.Spec.Lifecycle.DeletionPolicy.PersistentVolumes = workspacev1alpha1.WorkspaceDeletionActionRetain
			},
			wantErr: "retained workspace data is not yet supported",
		},
		{
			name:    "substrate profile without a template",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendSubstrate,
			mutate: func(f *acpClassFixture) {
				f.profile.Spec.Substrate = nil
			},
			wantErr: "must name the operator-owned Substrate infrastructure ActorTemplate",
		},
		{
			name:    "agent-sandbox profile with substrate inputs",
			backend: acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox,
			mutate: func(f *acpClassFixture) {
				f.profile.Spec.Substrate = &acpworkspacev1alpha1.SubstrateProfileSpec{
					TemplateRef: acpworkspacev1alpha1.SubstrateTemplateReference{Name: acpTestInfraName},
				}
			},
			wantErr: "backend is agent-sandbox",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mutations := []func(*acpClassFixture){}
			if tt.mutate != nil {
				mutations = append(mutations, tt.mutate)
			}
			fixture := newACPClassFixture(t, tt.backend, mutations...)
			if tt.mutateAfter != nil {
				tt.mutateAfter(fixture)
			}
			task := tt.task
			if task == nil {
				task = acpClassTestTask()
			}
			r := acpClassTestReconciler(t, fixture.objects()...)
			resolved, err := r.resolveACPWorkspaceClass(context.Background(), task)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveACPWorkspaceClass() error = %v", err)
			}
			if resolved == nil {
				t.Fatalf("resolved class is nil")
			}
			if tt.check != nil {
				tt.check(t, resolved)
			}
		})
	}
}

func TestResolveACPWorkspaceClassRequiresProviderAPI(t *testing.T) {
	t.Parallel()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	r := acpClassTestReconciler(t, fixture.objects()...)
	r.WorkspaceProviderAPIEnabled = false
	_, err := r.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
	if err == nil || !strings.Contains(err.Error(), "requires the workspace provider API") {
		t.Fatalf("error = %v", err)
	}
}

func TestResolveACPWorkspaceClassRejectsWithdrawnProviderFeature(t *testing.T) {
	t.Parallel()
	fixture := suspendableSubstrateFixture(t)
	fixture.provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeatureExec,
		workspacev1alpha1.WorkspaceFeatureFiles,
		workspacev1alpha1.WorkspaceFeatureReset,
		workspacev1alpha1.WorkspaceFeatureTLS,
	}
	r := acpClassTestReconciler(t, fixture.objects()...)
	_, err := r.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
	if err == nil || !strings.Contains(err.Error(), "no longer supports every explicit or implied class feature") {
		t.Fatalf("error = %v, want live provider feature withdrawal rejected", err)
	}
}

func TestResolveACPWorkspaceClassAllowsDeleteContinuationAfterSuspendWithdrawal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const sessionUID = "existing-session-uid"
	fixture := suspendableSubstrateFixture(t)
	fixture.provider.Status.SupportedFeatures = []workspacev1alpha1.ExecutionWorkspaceFeature{
		workspacev1alpha1.WorkspaceFeatureExec,
		workspacev1alpha1.WorkspaceFeatureFiles,
		workspacev1alpha1.WorkspaceFeatureReset,
		workspacev1alpha1.WorkspaceFeatureTLS,
	}
	fixture.class.Status.ObservedGeneration = fixture.class.Generation
	apimeta.SetStatusCondition(&fixture.class.Status.Conditions, metav1.Condition{
		Type:               string(workspacev1alpha1.ConditionClassReady),
		Status:             metav1.ConditionFalse,
		ObservedGeneration: fixture.class.Generation,
		Reason:             reasonRequiredFeatures,
		Message:            messageProviderFeaturesMissing,
	})
	task := suspendableSessionTask()
	task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachDelete
	newTaskReconciler := acpClassTestReconciler(t, fixture.objects()...)
	if _, err := newTaskReconciler.resolveACPWorkspaceClassWithSessionUID(ctx, task, sessionUID); err == nil ||
		!strings.Contains(err.Error(), "not ready at its current generation") {
		t.Fatalf("brand-new Delete task after Suspend withdrawal error = %v, want current class readiness rejection", err)
	}

	probe := &ACPRuntimeWorkspaceBinding{
		ReusePolicy:   corev1alpha1.WorkspaceReusePolicySession,
		WorkspaceSlot: defaultWorkspaceSlotName,
		SessionUID:    sessionUID,
		Class:         &ACPWorkspaceClassBinding{UID: string(fixture.class.UID)},
	}
	workspaceName := acpClassWorkspaceName(task, probe)
	poolName := "existing-delete-continuation-pool"
	workspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: task.Namespace,
			Name:      workspaceName,
			UID:       types.UID("existing-delete-continuation-workspace-uid"),
			Labels: map[string]string{
				workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue,
			},
			Annotations: map[string]string{
				acpExecutionWorkspacePoolAnnotation: poolName,
				acpWorkspaceBackendAnnotation:       string(corev1alpha1.WorkspaceProviderSubstrate),
			},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Mode: workspacev1alpha1.ExecutionWorkspaceModeInteractive,
			ClassBinding: workspacev1alpha1.ImmutableObjectBinding{
				Name: fixture.class.Name, UID: fixture.class.UID, Generation: fixture.class.Generation,
				ProfileHash: fixture.class.Status.ProfileHash,
			},
			ProviderBinding: workspacev1alpha1.ImmutableObjectBinding{
				Name: fixture.provider.Name, UID: fixture.provider.UID, Generation: fixture.provider.Generation,
			},
			SessionRef: &workspacev1alpha1.ObjectIdentityReference{
				Name: acpTestSessionName, UID: types.UID(sessionUID),
			},
			Slot:         defaultWorkspaceSlotName,
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{State: workspacev1alpha1.ExecutionWorkspaceStateReady},
	}
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  task.Namespace,
			Name:       poolName,
			UID:        types.UID("existing-delete-continuation-pool-uid"),
			Generation: 1,
			Labels: map[string]string{
				acpExecutionWorkspaceLinkLabel:   workspace.Name,
				acpRuntimeWorkspaceProviderLabel: string(corev1alpha1.WorkspaceProviderSubstrate),
			},
			Annotations: map[string]string{
				acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
			},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
				Provider:      corev1alpha1.WorkspaceProviderSubstrate,
				BindingDigest: "sha256:" + strings.Repeat("a", 64),
				Substrate: &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
					BaseTemplateNamespace: acpTestSubstrateNamespace,
					BaseTemplateName:      acpTestInfraTemplateName,
				},
			},
		},
	}
	objects := append(fixture.objects(), workspace, pool)
	notServing := acpClassTestReconciler(t, objects...)
	if _, err := notServing.resolveACPWorkspaceClassWithSessionUID(ctx, task, sessionUID); err == nil ||
		!strings.Contains(err.Error(), "not ready at its current generation") {
		t.Fatalf("non-serving Delete-bound continuation after Suspend withdrawal error = %v, want current class readiness rejection", err)
	}
	pool.Status = corev1alpha1.RuntimePoolStatus{
		ObservedGeneration: pool.Generation,
		Lifecycle:          corev1alpha1.RuntimePoolLifecycleServing,
		AdmissionState:     corev1alpha1.RuntimePoolAdmissionAccepting,
	}
	objects = append(fixture.objects(), workspace, pool)
	r := acpClassTestReconciler(t, objects...)
	resolved, err := r.resolveACPWorkspaceClassWithSessionUID(ctx, task, sessionUID)
	if err != nil {
		t.Fatalf("resolve Delete-bound continuation after Suspend withdrawal: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, sessionUID, resolved)
	if err != nil {
		t.Fatalf("freeze Delete-bound continuation: %v", err)
	}
	if binding.Class == nil || binding.Class.EffectiveOnDetach != string(workspacev1alpha1.WorkspaceOnDetachDelete) {
		t.Fatalf("continuation class binding = %+v, want frozen Delete action", binding.Class)
	}

	suspendedWorkspace := workspace.DeepCopy()
	suspendedWorkspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	suspendedWorkspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
	suspendedReconciler := acpClassTestReconciler(t, append(fixture.objects(), suspendedWorkspace, pool.DeepCopy())...)
	if _, err := suspendedReconciler.resolveACPWorkspaceClassWithSessionUID(ctx, task, sessionUID); err == nil ||
		!strings.Contains(err.Error(), "not ready at its current generation") {
		t.Fatalf("Suspended Delete-bound continuation after Suspend withdrawal error = %v, want current class readiness rejection", err)
	}
}

func TestResolveAgentExecutionCandidatePreservesTransientStorageClassReadErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		named bool
	}{
		{name: "default StorageClass list"},
		{name: "named StorageClass get", named: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := suspendableSandboxFixture(t)
			if tt.named {
				fixture.profile.Spec.AgentSandbox.Suspend.Volume.StorageClassName = acpTestDefaultStorageClass().Name
				fixture.pinProfileHash(t)
			}
			task := acpClassTestTask()
			objects := append(fixture.objects(), bindingTestNamespace())
			r := acpClassTestReconciler(t, objects...)
			bindingReconciler, _ := newBindingTestReconciler(t)
			r.AgentExecutionSnapshots = bindingReconciler.AgentExecutionSnapshots
			r.ACPRuntimeEnabled = bindingReconciler.ACPRuntimeEnabled
			r.ACPRuntimeNamespace = bindingReconciler.ACPRuntimeNamespace
			r.ACPRuntimeImages = bindingReconciler.ACPRuntimeImages

			withWatch, ok := r.Client.(client.WithWatch)
			if !ok {
				t.Fatal("fake client does not support watch interception")
			}
			transient := errors.New("temporary StorageClass API outage")
			functions := interceptor.Funcs{}
			if tt.named {
				functions.Get = func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, isStorageClass := obj.(*storagev1.StorageClass); isStorageClass {
						return transient
					}
					return c.Get(ctx, key, obj, opts...)
				}
			} else {
				functions.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, isStorageClasses := list.(*storagev1.StorageClassList); isStorageClasses {
						return transient
					}
					return c.List(ctx, list, opts...)
				}
			}
			r.APIReader = interceptor.NewClient(withWatch, functions)

			_, err := r.resolveAgentExecutionCandidate(context.Background(), task, bindingTestAgent())
			if !errors.Is(err, transient) || isPermanentACPAgentConfigurationError(err) {
				t.Fatalf("candidate error = %v, permanent=%t, want retryable StorageClass read failure", err, isPermanentACPAgentConfigurationError(err))
			}
		})
	}
}

func TestResolveACPClassWorkspaceBindingPolicy(t *testing.T) {
	t.Parallel()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	r := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := r.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}

	t.Run("binding freezes class identity into the digest", func(t *testing.T) {
		task := acpClassTestTask()
		binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
		if err != nil {
			t.Fatalf("resolve binding: %v", err)
		}
		if binding.Class == nil || binding.Class.EffectiveOnDetach != string(workspacev1alpha1.WorkspaceOnDetachDelete) {
			t.Fatalf("class binding = %+v", binding.Class)
		}
		if binding.Provider != corev1alpha1.WorkspaceProviderAgentSandbox ||
			binding.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete {
			t.Fatalf("binding = %+v", binding)
		}
		if err := validateACPWorkspaceBindingValues(binding); err != nil {
			t.Fatalf("frozen binding validation: %v", err)
		}
		legacyTask := acpClassTestTask(func(task *corev1alpha1.Task) {
			task.Spec.Execution.Workspace = &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}
		})
		legacy, err := resolveACPWorkspaceBinding(legacyTask, corev1alpha1.WorkspaceProviderAgentSandbox, false, "")
		if err != nil {
			t.Fatalf("resolve legacy binding: %v", err)
		}
		if legacy.BindingDigest == binding.BindingDigest {
			t.Fatalf("class-backed binding digest must differ from the legacy digest")
		}
	})

	t.Run("requested detach action outside the class allowlist fails", func(t *testing.T) {
		task := acpClassTestTask(func(task *corev1alpha1.Task) {
			task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachSuspend
		})
		_, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
		if err == nil || !strings.Contains(err.Error(), "is not allowed by class") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("suspend default fails closed until cold resume exists", func(t *testing.T) {
		suspendFixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
			f.class.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
			f.class.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
				workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
			}
		})
		suspendReconciler := acpClassTestReconciler(t, suspendFixture.objects()...)
		suspendResolved, err := suspendReconciler.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
		if err != nil {
			t.Fatalf("resolve class: %v", err)
		}
		if _, err := resolveACPWorkspaceBindingWithClass(acpClassTestTask(), "", false, "", suspendResolved); err == nil ||
			!strings.Contains(err.Error(), "permits DataOnly suspension") {
			t.Fatalf("error = %v", err)
		}
		// The Task may still pick the executable Delete action explicitly.
		deleteTask := acpClassTestTask(func(task *corev1alpha1.Task) {
			task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachDelete
		})
		if _, err := resolveACPWorkspaceBindingWithClass(deleteTask, "", false, "", suspendResolved); err != nil {
			t.Fatalf("explicit Delete action: %v", err)
		}
	})

	t.Run("reuse scope outside the class allowlist fails", func(t *testing.T) {
		noneOnly := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
			f.class.Spec.AllowedReuseScopes = []workspacev1alpha1.WorkspaceReuseScope{workspacev1alpha1.WorkspaceReuseScopeNone}
		})
		noneReconciler := acpClassTestReconciler(t, noneOnly.objects()...)
		noneResolved, err := noneReconciler.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
		if err != nil {
			t.Fatalf("resolve class: %v", err)
		}
		task := acpClassTestTask(func(task *corev1alpha1.Task) {
			task.Spec.Execution.Workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
			task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpTestSessionName, Create: true}
		})
		if _, err := resolveACPWorkspaceBindingWithClass(task, "", false, "session-uid-1", noneResolved); err == nil ||
			!strings.Contains(err.Error(), "not allowed by class") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestRejectUnsupportedACPWorkspacePlanClassGates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name       string
		configure  func(*TaskReconciler)
		wantReject string
	}{
		{
			name:       "workspace provider API disabled",
			configure:  func(r *TaskReconciler) { r.WorkspaceProviderAPIEnabled = false },
			wantReject: "requires the workspace provider API",
		},
		{
			name:       "agent-sandbox backend disabled",
			configure:  func(r *TaskReconciler) { r.ACPWorkspaceDispatchEnabled = true },
			wantReject: "agent-sandbox is disabled",
		},
		{
			name:       "workspace dispatch disabled",
			configure:  func(r *TaskReconciler) { r.AgentSandboxEnabled = true },
			wantReject: "dispatch is disabled",
		},
		{
			name: "class-backed dispatch admitted",
			configure: func(r *TaskReconciler) {
				r.AgentSandboxEnabled = true
				r.ACPWorkspaceDispatchEnabled = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
			r := acpClassTestReconciler(t, fixture.objects()...)
			tt.configure(r)
			plan, rejected := r.rejectUnsupportedACPWorkspacePlan(ctx, acpClassTestTask())
			if tt.wantReject == "" {
				if rejected {
					t.Fatalf("plan rejected: %+v", plan)
				}
				return
			}
			if !rejected || !strings.Contains(plan.rejectionReason, tt.wantReject) {
				t.Fatalf("plan = %+v, want rejection containing %q", plan, tt.wantReject)
			}
			if plan.workspaceStatusError == nil {
				t.Fatalf("class-shaped rejection must project a workspace validation failure")
			}
		})
	}
}

func TestACPWorkspaceClassBindingSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendSubstrate)
	r := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := r.resolveACPWorkspaceClass(context.Background(), acpClassTestTask())
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(acpClassTestTask(), "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	frozen := snapshotWorkspaceClassFromBinding(binding.Class)
	rebuilt := workspaceClassBindingFromSnapshot(frozen)
	if !reflect.DeepEqual(rebuilt, binding.Class) {
		t.Fatalf("snapshot round trip changed the class binding:\n%+v\n%+v", rebuilt, binding.Class)
	}
	restored := *binding
	restored.Class = rebuilt
	if err := validateACPWorkspaceBindingValues(&restored); err != nil {
		t.Fatalf("restored binding validation: %v", err)
	}

	tampered := *binding
	tamperedClass := *binding.Class
	tamperedClass.ProfileHash = "sha256:" + strings.Repeat("1", 64)
	tampered.Class = &tamperedClass
	if err := validateACPWorkspaceBindingValues(&tampered); err == nil {
		t.Fatalf("tampered class profile hash must fail canonical validation")
	}

	suspended := *binding
	suspendedClass := *binding.Class
	suspendedClass.EffectiveOnDetach = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	suspended.Class = &suspendedClass
	if err := validateACPWorkspaceBindingValues(&suspended); err == nil ||
		!strings.Contains(err.Error(), "DataOnly suspension policy") {
		t.Fatalf("a tampered Suspend action without its policy must stay rejected, got %v", err)
	}
}

func TestEnsureACPClassWorkspaceLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}

	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if ready || name != "" {
		t.Fatalf("workspace must not be ready before core admission")
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("created workspace: %v", err)
	}
	if workspace.Annotations[acpExecutionWorkspacePoolAnnotation] != plan.PoolName {
		t.Fatalf("workspace pool annotation = %q", workspace.Annotations[acpExecutionWorkspacePoolAnnotation])
	}
	owned := false
	for _, owner := range workspace.OwnerReferences {
		if owner.UID == task.UID {
			owned = true
			if owner.Controller != nil && *owner.Controller {
				t.Fatalf("per-Task workspace must not be controller-owned; the ACP projection owns Task status")
			}
		}
	}
	if !owned {
		t.Fatalf("per-Task workspace must carry a Task owner reference")
	}
	if workspace.Spec.ClassBinding.ProfileHash != binding.Class.ProfileHash ||
		workspace.Spec.Lifecycle.DefaultOnDetach != workspacev1alpha1.WorkspaceOnDetachDelete {
		t.Fatalf("workspace spec = %+v", workspace.Spec)
	}

	// Admit the workspace and let the adapter-observed status catch up; the
	// second ensure attaches and links the Task.
	admitTestACPWorkspace(t, r, workspace)
	name, ready = attachTestACPWorkspace(t, r, task, plan, workspaceName)
	if !ready || name != workspaceName {
		t.Fatalf("ensure = (%q, %v), want attached workspace", name, ready)
	}
	linked := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, linked); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if linked.Labels[acpExecutionWorkspaceLinkLabel] != workspaceName {
		t.Fatalf("task link label = %q", linked.Labels[acpExecutionWorkspaceLinkLabel])
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read attached workspace: %v", err)
	}
	if workspace.Spec.Attachment == nil || workspace.Spec.Attachment.TaskRef.UID != task.UID || workspace.Spec.Attachment.Epoch != 1 {
		t.Fatalf("attachment = %+v", workspace.Spec.Attachment)
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspace.Spec.Attachment.TokenSecretRef.Name}, secret); err != nil {
		t.Fatalf("attachment secret: %v", err)
	}

	// Idempotent re-ensure keeps the same attachment.
	if _, ready, err = r.ensureACPClassWorkspace(ctx, task, plan); err != nil || !ready {
		t.Fatalf("re-ensure = (%v, %v)", ready, err)
	}

}

// A workspace the adapter marked terminally Failed (for example the frozen
// maximum lifetime elapsed and its RuntimePool was torn down) must fail the
// waiting Task instead of requeueing forever.
// A live provider usage-policy edit must fail class resolution closed
// immediately: the class's cached Ready condition lags policy changes and the
// profile hash excludes usagePolicy, so the resolver re-checks the selector.
func TestResolveACPWorkspaceClassEnforcesLiveNamespacePolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	fixture.provider.Spec.UsagePolicy = &workspacev1alpha1.ExecutionWorkspaceProviderUsagePolicy{
		AllowedNamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"workspace-tier": "allowed"},
		},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: acpTestNamespace}}
	r := acpClassTestReconciler(t, append(fixture.objects(), namespace)...)
	_, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask())
	if err == nil || !strings.Contains(err.Error(), "usage policy does not allow namespace") {
		t.Fatalf("a disallowed namespace must fail live class resolution, got %v", err)
	}

	current := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: acpTestNamespace}, current); err != nil {
		t.Fatalf("read namespace: %v", err)
	}
	current.Labels = map[string]string{"workspace-tier": "allowed"}
	if err := r.Update(ctx, current); err != nil {
		t.Fatalf("label namespace: %v", err)
	}
	if _, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask()); err != nil {
		t.Fatalf("a matching namespace must resolve, got %v", err)
	}
}

// Once the execution binding is frozen, planning must not reapply
// new-allocation readiness: core deliberately admits Draining providers for
// already-admitted workspaces, and re-resolving the class here would fail the
// bound Task and destroy its valid workspace.
func TestRejectUnsupportedACPWorkspacePlanTrustsFrozenBinding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	fixture.provider.Spec.LifecycleState = workspacev1alpha1.ExecutionWorkspaceProviderDraining
	r := acpClassTestReconciler(t, fixture.objects()...)
	r.AgentSandboxEnabled = true
	r.ACPWorkspaceDispatchEnabled = true

	unbound := acpClassTestTask()
	if _, rejected := r.rejectUnsupportedACPWorkspacePlan(ctx, unbound); !rejected {
		t.Fatal("a NEW allocation against a Draining provider must be rejected")
	}

	bound := acpClassTestTask(func(task *corev1alpha1.Task) {
		task.Status.AgentExecutionBinding = &corev1alpha1.AgentExecutionBinding{}
	})
	if plan, rejected := r.rejectUnsupportedACPWorkspacePlan(ctx, bound); rejected {
		t.Fatalf("a frozen binding must not re-run new-allocation readiness, got rejection %+v", plan)
	}

	// The configuration gates for bound Tasks are enforced against the
	// VERIFIED frozen plan at the queue chokepoint - never against the
	// public status projection, which can still be nil after a restart
	// between the binding patch and the first queue operation.
	frozen := &ACPRuntimeWorkspaceBinding{Provider: corev1alpha1.WorkspaceProviderAgentSandbox}
	if reason := r.frozenWorkspaceDispatchDisabledReason(frozen); reason != "" {
		t.Fatalf("enabled flags must admit the frozen plan, got %q", reason)
	}
	r.AgentSandboxEnabled = false
	if reason := r.frozenWorkspaceDispatchDisabledReason(frozen); reason == "" {
		t.Fatal("the provider dispatch gate must still apply to the frozen plan")
	}
	r.AgentSandboxEnabled = true
	r.ACPWorkspaceDispatchEnabled = false
	if reason := r.frozenWorkspaceDispatchDisabledReason(frozen); reason == "" {
		t.Fatal("the workspace dispatch gate must still apply to the frozen plan")
	}
	r.ACPWorkspaceDispatchEnabled = true
	r.WorkspaceProviderAPIEnabled = false
	classBacked := &ACPRuntimeWorkspaceBinding{
		Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
		Class:    &ACPWorkspaceClassBinding{Name: "class"},
	}
	if reason := r.frozenWorkspaceDispatchDisabledReason(classBacked); reason == "" {
		t.Fatal("a class-backed frozen plan must require the workspace provider API")
	}
}

// Deleting or replacing the frozen class, provider, or RuntimeWorkspaceProfile
// after the snapshot froze is irreversible for this incarnation: the
// dependency-loss and profile-mismatch denials must fail the Task instead of
// requeueing forever.
func TestEnsureACPClassWorkspaceDependencyLossIsTerminal(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		"ClassNotFound", "ClassProfileMismatch", "ClassPolicyMismatch", reasonProviderNotFound,
		"ParametersDeleting", "ParametersNotFound",
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
			task := acpClassTestTask()
			r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
			resolved, err := r.resolveACPWorkspaceClass(ctx, task)
			if err != nil {
				t.Fatalf("resolve class: %v", err)
			}
			binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
			if err != nil {
				t.Fatalf("resolve binding: %v", err)
			}
			plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
			if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
				t.Fatalf("first ensure: %v", err)
			}
			workspaceName := acpClassWorkspaceName(task, binding)
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
				t.Fatalf("created workspace: %v", err)
			}
			workspace.Status.Conditions = []metav1.Condition{{
				Type:               string(workspacev1alpha1.ConditionWorkspaceAdmitted),
				Status:             metav1.ConditionFalse,
				Reason:             reason,
				Message:            "frozen dependency was deleted",
				ObservedGeneration: workspace.Generation,
				LastTransitionTime: metav1.Now(),
			}}
			if err := r.Status().Update(ctx, workspace); err != nil {
				t.Fatalf("record dependency-loss denial: %v", err)
			}
			_, _, err = r.ensureACPClassWorkspace(ctx, task, plan)
			if err == nil || !errors.Is(err, errACPWorkspaceBindingConflict) {
				t.Fatalf("dependency-loss denial %s must be terminal, got %v", reason, err)
			}
		})
	}
}

// The settlement link must be durable BEFORE the session workspace is
// created: a crash between Create and the post-create link patch would
// otherwise orphan the deterministic session workspace forever. The link
// label lands even when the Create itself fails.
func TestEnsureACPClassWorkspacePersistsLinkBeforeCreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask(func(task *corev1alpha1.Task) {
		task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpTestSessionName}
		task.Spec.Execution.Workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
	})
	scheme := testACPWorkspaceScheme(t)
	createErr := errors.New("injected create failure")
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(
		&workspacev1alpha1.ExecutionWorkspace{}, &corev1alpha1.Task{},
	).WithObjects(append(fixture.objects(), task)...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isWorkspace := obj.(*workspacev1alpha1.ExecutionWorkspace); isWorkspace {
				return createErr
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		WorkspaceProviderAPIEnabled:  true,
		WorkspaceSettlementProtected: true,
	}
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "session-uid-1", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); !errors.Is(err, createErr) {
		t.Fatalf("ensure error = %v, want the injected create failure", err)
	}
	current := &corev1alpha1.Task{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	name := acpClassWorkspaceName(task, binding)
	if current.Labels[acpExecutionWorkspaceLinkLabel] != name {
		t.Fatalf("link label = %q, want the settlement link %q persisted BEFORE creation", current.Labels[acpExecutionWorkspaceLinkLabel], name)
	}
}

func TestEnsureACPClassWorkspaceFailedStateIsTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("created workspace: %v", err)
	}
	workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("mark workspace failed: %v", err)
	}
	_, _, err = r.ensureACPClassWorkspace(ctx, task, plan)
	if err == nil || !errors.Is(err, errACPWorkspaceTerminalFailure) {
		t.Fatalf("a terminally failed workspace must surface errACPWorkspaceTerminalFailure, got %v", err)
	}
}

// A continuation must never attach while a revocation stamp stands — even for
// a Suspend detach action. The suspend settlement retires the stamp in the
// same optimistic patch that lands DesiredState=Suspended; attaching earlier
// would reuse the workspace warm and let the old settlement observe a foreign
// attachment as completion, silently skipping the requested checkpoint.
func TestEnsureACPClassWorkspaceBlocksContinuationDuringPendingSuspend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	workspace.Spec.AttachmentEpoch = 1
	admitTestACPWorkspace(t, r, workspace)
	// The predecessor's Suspend settlement is mid-flight: the attachment was
	// revoked and the revocation stamp stands, but the suspension patch has
	// not landed DesiredState=Suspended yet.
	base := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf(
		"%d %s", workspace.Spec.AttachmentEpoch, time.Now().UTC().Format(time.RFC3339Nano),
	)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("stamp pending suspend settlement: %v", err)
	}

	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil || ready || name != "" {
		t.Fatalf("ensure during pending Suspend settlement = (%q, %v, %v), want blocked", name, ready, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("re-read workspace: %v", err)
	}
	if workspace.Spec.Attachment != nil {
		t.Fatalf("attachment = %+v, want none while the revocation stamp stands", workspace.Spec.Attachment)
	}
}

// TestEnsureACPClassWorkspaceSessionContention proves attachment exclusivity
// on a genuinely shared workspace: two session-reused Tasks with the same
// immutable Session UID derive the same workspace name, so the competitor
// contends for the exact workspace the holder attached.
func TestEnsureACPClassWorkspaceSessionContention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	sessionScoped := func(name, uid string) *corev1alpha1.Task {
		return acpClassTestTask(func(task *corev1alpha1.Task) {
			task.Name = name
			task.UID = types.UID(uid)
			task.Spec.Execution.Workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
			task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpTestSessionName, Create: true}
		})
	}
	holder := sessionScoped("session-holder", "session-holder-uid")
	competitor := sessionScoped("session-competitor", "session-competitor-uid")
	r := acpClassTestReconciler(t, append(fixture.objects(), holder, competitor)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, holder)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	holderBinding, err := resolveACPWorkspaceBindingWithClass(holder, "", false, "session-uid-1", resolved)
	if err != nil {
		t.Fatalf("resolve holder binding: %v", err)
	}
	competitorBinding, err := resolveACPWorkspaceBindingWithClass(competitor, "", false, "session-uid-1", resolved)
	if err != nil {
		t.Fatalf("resolve competitor binding: %v", err)
	}
	workspaceName := acpClassWorkspaceName(holder, holderBinding)
	if got := acpClassWorkspaceName(competitor, competitorBinding); got != workspaceName {
		t.Fatalf("session-reused Tasks must derive one workspace name, got %q and %q", workspaceName, got)
	}

	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: holderBinding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, holder, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: holder.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, holder, plan, workspace.Name); !ready {
		t.Fatalf("holder attach = (%v)", ready)
	}

	// The competitor resolves the same workspace but must not take the held
	// attachment: it either waits (not ready) or fails closed, never attaches.
	competitorPlan := ACPRuntimePlan{PoolName: plan.PoolName, Workspace: competitorBinding}
	if _, ready, err := r.ensureACPClassWorkspace(ctx, competitor, competitorPlan); err == nil && ready {
		t.Fatalf("competitor must not attach a held workspace")
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: holder.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("re-read workspace: %v", err)
	}
	if workspace.Spec.Attachment == nil || workspace.Spec.Attachment.TaskRef.UID != holder.UID {
		t.Fatalf("attachment = %+v, want held by %s", workspace.Spec.Attachment, holder.UID)
	}
}

func TestEnsureACPClassWorkspaceRejectsSuspendedAttachedWorkspace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatal("workspace attachment did not become ready")
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read attached workspace: %v", err)
	}
	base := workspace.DeepCopy()
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("request suspension: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("re-read suspended workspace: %v", err)
	}
	markWorkspaceAdmittedForPolicyReview(workspace, workspace.Generation)
	workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspending
	workspace.Status.AttachedEpoch = workspace.Spec.Attachment.Epoch
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("publish suspending status: %v", err)
	}

	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err == nil || !errors.Is(err, errACPWorkspaceBindingConflict) ||
		!strings.Contains(err.Error(), string(workspacev1alpha1.ExecutionWorkspaceDesiredSuspended)) {
		t.Fatalf("ensure suspended attached workspace = (%q, %v, %v), want desired-state rejection", name, ready, err)
	}
}

// A revised class or provider snapshot keeps the same session workspace name
// while the class UID and Session UID remain stable. The successor waits for
// the attached predecessor's Delete settlement, but the same stale workspace
// is rejected once no predecessor owns it.
func TestEnsureACPClassWorkspaceQueuesRevisedSessionBehindPredecessor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ACPWorkspaceClassBinding)
	}{
		{
			name: "class generation",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.Generation++
				class.ProfileHash = "sha256:" + strings.Repeat("9", 64)
			},
		},
		{
			name: "provider generation",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.ProviderGeneration++
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
			sessionTask := func(name, uid string) *corev1alpha1.Task {
				return acpClassTestTask(func(task *corev1alpha1.Task) {
					task.Name = name
					task.UID = types.UID(uid)
					task.Spec.Execution.Workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
					task.Spec.SessionRef = &corev1alpha1.SessionReference{Name: acpTestSessionName, Create: true}
				})
			}
			holder := sessionTask("revision-holder", "revision-holder-uid")
			successor := sessionTask("revision-successor", "revision-successor-uid")
			r := acpClassTestReconciler(t, append(fixture.objects(), holder, successor)...)
			resolved, err := r.resolveACPWorkspaceClass(ctx, holder)
			if err != nil {
				t.Fatalf("resolve class: %v", err)
			}
			holderBinding, err := resolveACPWorkspaceBindingWithClass(holder, "", false, "session-revision-uid", resolved)
			if err != nil {
				t.Fatalf("resolve holder binding: %v", err)
			}
			successorBinding, err := resolveACPWorkspaceBindingWithClass(successor, "", false, "session-revision-uid", resolved)
			if err != nil {
				t.Fatalf("resolve successor binding: %v", err)
			}
			holderPlan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: holderBinding}
			if _, _, err := r.ensureACPClassWorkspace(ctx, holder, holderPlan); err != nil {
				t.Fatalf("materialize holder workspace: %v", err)
			}
			workspaceName := acpClassWorkspaceName(holder, holderBinding)
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: holder.Namespace, Name: workspaceName}, workspace); err != nil {
				t.Fatalf("read workspace: %v", err)
			}
			admitTestACPWorkspace(t, r, workspace)
			if _, ready := attachTestACPWorkspace(t, r, holder, holderPlan, workspace.Name); !ready {
				t.Fatal("holder attachment did not become ready")
			}

			revisedBinding := *successorBinding
			revisedClass := *successorBinding.Class
			test.mutate(&revisedClass)
			revisedBinding.Class = &revisedClass
			revisedPlan := ACPRuntimePlan{PoolName: holderPlan.PoolName, Workspace: &revisedBinding}
			name, ready, err := r.ensureACPClassWorkspace(ctx, successor, revisedPlan)
			if err != nil || ready || name != "" {
				t.Fatalf("ensure revised successor behind predecessor = (%q, %v, %v), want queued", name, ready, err)
			}
			queued := &corev1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(successor), queued); err != nil {
				t.Fatalf("read successor: %v", err)
			}
			if queued.Labels[acpExecutionWorkspaceLinkLabel] != "" {
				t.Fatalf("queued successor linked to predecessor workspace: %#v", queued.Labels)
			}

			if err := r.Get(ctx, types.NamespacedName{Namespace: holder.Namespace, Name: workspaceName}, workspace); err != nil {
				t.Fatalf("reload workspace: %v", err)
			}
			workspace.Spec.Attachment = nil
			workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf(
				"%d %s", workspace.Spec.AttachmentEpoch, time.Now().UTC().Format(time.RFC3339Nano),
			)
			if err := r.Update(ctx, workspace); err != nil {
				t.Fatalf("record predecessor revocation: %v", err)
			}
			name, ready, err = r.ensureACPClassWorkspace(ctx, successor, revisedPlan)
			if err != nil || ready || name != "" {
				t.Fatalf("ensure revised successor during predecessor revocation = (%q, %v, %v), want queued", name, ready, err)
			}

			delete(workspace.Annotations, acpWorkspaceRevocationStartedAnnotation)
			if err := r.Update(ctx, workspace); err != nil {
				t.Fatalf("clear predecessor revocation marker: %v", err)
			}
			if _, _, err := r.ensureACPClassWorkspace(ctx, successor, revisedPlan); err == nil ||
				!errors.Is(err, errACPWorkspaceBindingConflict) {
				t.Fatalf("unowned stale workspace error = %v, want binding conflict", err)
			}
		})
	}
}

func TestEnsureACPClassWorkspaceRejectsForeignAdoption(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	name := acpClassWorkspaceName(task, binding)
	plan := ACPRuntimePlan{PoolName: "acp-ws-foreign-check", Workspace: binding}
	foreign := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: task.Namespace, Name: name, UID: types.UID("foreign-uid"),
			Labels: map[string]string{
				workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue,
			},
			Annotations: map[string]string{
				acpExecutionWorkspacePoolAnnotation:     plan.PoolName,
				acpWorkspaceProviderConfigUIDAnnotation: binding.Class.ProviderConfigUID,
				acpWorkspaceBackendAnnotation:           string(binding.Provider),
			},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Mode: workspacev1alpha1.ExecutionWorkspaceModeInteractive,
			ClassBinding: workspacev1alpha1.ImmutableObjectBinding{
				Name: binding.Class.Name, UID: types.UID("other-class-uid"), Generation: 1, ProfileHash: binding.Class.ProfileHash,
			},
			ProviderBinding: workspacev1alpha1.ImmutableObjectBinding{
				Name: binding.Class.ProviderName, UID: types.UID(binding.Class.ProviderUID), Generation: 1,
			},
			Slot: binding.WorkspaceSlot, DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
			Lifecycle: fixture.class.Spec.Lifecycle,
		},
	}
	if err := r.Create(ctx, foreign); err != nil {
		t.Fatalf("create foreign workspace: %v", err)
	}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err == nil ||
		!strings.Contains(err.Error(), "class binding does not match") {
		t.Fatalf("error = %v", err)
	}
}

func TestEnsureACPClassWorkspaceBackfillsAndValidatesSuspendedCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	limit := int32(1)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	fixture.pinProfileHash(t)
	task := suspendableSessionTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, suspendTestSessionUID, resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSessionPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize workspace: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: acpClassWorkspaceName(task, binding)}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	delete(workspace.Annotations, acpWorkspaceMaxSuspendedAnnotation)
	if err := r.Update(ctx, workspace); err != nil {
		t.Fatalf("remove legacy cap annotation: %v", err)
	}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("adopt legacy workspace: %v", err)
	}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("read migrated workspace: %v", err)
	}
	if got := workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation]; got != "1" {
		t.Fatalf("backfilled suspended-workspace cap = %q, want 1", got)
	}

	workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "2"
	if err := r.Update(ctx, workspace); err != nil {
		t.Fatalf("write mismatched cap annotation: %v", err)
	}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err == nil ||
		!errors.Is(err, errACPWorkspaceBindingConflict) ||
		!strings.Contains(err.Error(), "suspended-workspace cap") {
		t.Fatalf("mismatched suspended-workspace cap error = %v, want binding conflict", err)
	}
}

// TestEnsureACPClassWorkspaceRejectsProviderIdentityDrift proves adoption is
// fail-closed against provider identity or materialization drift: a workspace
// whose recorded provider identity or controller-owned RuntimePool markers no
// longer match the frozen binding is rejected instead of reused.
func TestEnsureACPClassWorkspaceRejectsProviderIdentityDrift(t *testing.T) {
	t.Parallel()
	const (
		materializationMarkersError = "materialization markers do not match"
		frozenProviderError         = "provider config, backend, or suspend mode does not match"
	)
	tests := []struct {
		name    string
		mutate  func(*workspacev1alpha1.ExecutionWorkspace)
		wantErr string
	}{
		{
			name: "provider generation drift",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Spec.ProviderBinding.Generation = 7
			},
			wantErr: "provider binding does not match",
		},
		{
			name: "provider config UID drift",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Annotations[acpWorkspaceProviderConfigUIDAnnotation] = "recreated-config-uid"
			},
			wantErr: frozenProviderError,
		},
		{
			name: "backend drift",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Annotations[acpWorkspaceBackendAnnotation] = string(acpworkspacev1alpha1.RuntimeProviderBackendSubstrate)
			},
			wantErr: frozenProviderError,
		},
		{
			name: "suspend mode drift",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Annotations[acpWorkspaceSuspendModeAnnotation] = string(acpworkspacev1alpha1.SubstrateSuspendModeDataOnly)
			},
			wantErr: frozenProviderError,
		},
		{
			name: "controller label missing",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				delete(workspace.Labels, workspacev1alpha1.ProviderControllerLabel)
			},
			wantErr: materializationMarkersError,
		},
		{
			name: "runtime pool annotation missing",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				delete(workspace.Annotations, acpExecutionWorkspacePoolAnnotation)
			},
			wantErr: materializationMarkersError,
		},
		{
			name: "runtime pool annotation drift",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace) {
				workspace.Annotations[acpExecutionWorkspacePoolAnnotation] = "acp-ws-other"
			},
			wantErr: materializationMarkersError,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
			task := acpClassTestTask()
			r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
			resolved, err := r.resolveACPWorkspaceClass(ctx, task)
			if err != nil {
				t.Fatalf("resolve class: %v", err)
			}
			binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
			if err != nil {
				t.Fatalf("resolve binding: %v", err)
			}
			plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
			if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
				t.Fatalf("materialize: %v", err)
			}
			workspace := &workspacev1alpha1.ExecutionWorkspace{}
			workspaceName := acpClassWorkspaceName(task, binding)
			if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
				t.Fatalf("read workspace: %v", err)
			}
			tc.mutate(workspace)
			if err := r.Update(ctx, workspace); err != nil {
				t.Fatalf("drift workspace: %v", err)
			}
			if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err == nil ||
				!strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// TestSettleACPClassWorkspaceSkipsForeignLinkTarget proves settlement
// revalidates the mutable link label: a workspace the Task neither owns nor
// shares a Session with is skipped, never revoked or deleted.
func TestSettleACPClassWorkspaceSkipsForeignLinkTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	foreign := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "acp-ws-foreign", UID: types.UID("foreign-ws-uid"),
			Labels: map[string]string{workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Mode:         workspacev1alpha1.ExecutionWorkspaceModeInteractive,
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
			Lifecycle:    fixture.class.Spec.Lifecycle,
		},
	}
	task := acpClassTestTask(func(task *corev1alpha1.Task) {
		task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: foreign.Name}
	})
	r := acpClassTestReconciler(t, append(fixture.objects(), task, foreign)...)

	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || !done {
		t.Fatalf("settle = (%v, %v), want (true, nil)", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: acpTestNamespace, Name: foreign.Name}, foreign); err != nil {
		t.Fatalf("foreign workspace must survive settlement: %v", err)
	}
	if foreign.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("foreign workspace desired state = %q", foreign.Spec.DesiredState)
	}

	// A link naming a workspace without the ACP controller label is equally
	// skipped even when the Task owns it.
	unlabeled := foreign.DeepCopy()
	unlabeled.ObjectMeta = metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: "acp-ws-unlabeled", UID: types.UID("unlabeled-ws-uid"),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: corev1alpha1.GroupVersion.String(), Kind: taskResourceKind, Name: task.Name, UID: task.UID,
		}},
	}
	unlabeled.ResourceVersion = ""
	if err := r.Create(ctx, unlabeled); err != nil {
		t.Fatalf("create unlabeled workspace: %v", err)
	}
	task.Labels[acpExecutionWorkspaceLinkLabel] = unlabeled.Name
	if done, err := r.settleACPClassWorkspace(ctx, task); err != nil || !done {
		t.Fatalf("settle unlabeled = (%v, %v), want (true, nil)", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: acpTestNamespace, Name: unlabeled.Name}, unlabeled); err != nil {
		t.Fatalf("unlabeled workspace must survive settlement: %v", err)
	}
}

// TestSettleACPClassWorkspaceQuarantinesPastDetachTimeout proves the frozen
// detachTimeout bounds settlement: when the adapter never releases the revoked
// epoch, the workspace is quarantined fail-closed and the Task releases.
// TestSettleACPClassWorkspaceSkipsRecreatedIncarnation proves an old Task
// never settles a same-name workspace from a different incarnation (for
// example a Session recreated under the same name).
func TestSettleACPClassWorkspaceSkipsRecreatedIncarnation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	name := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatalf("attach = (%v)", ready)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if task.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
		t.Fatalf("link must pin the workspace incarnation UID, got %q", task.Annotations[acpExecutionWorkspaceUIDAnnotation])
	}

	// Replace the workspace with a same-name different-UID incarnation.
	if err := r.Delete(ctx, workspace); err != nil {
		t.Fatalf("delete original workspace: %v", err)
	}
	replacement := workspace.DeepCopy()
	replacement.ObjectMeta = metav1.ObjectMeta{
		Namespace: workspace.Namespace, Name: workspace.Name, UID: types.UID("recreated-incarnation-uid"),
		Labels:      workspace.Labels,
		Annotations: map[string]string{acpExecutionWorkspacePoolAnnotation: plan.PoolName},
	}
	replacement.Spec.Attachment = nil
	replacement.Spec.AttachmentEpoch = 0
	replacement.ResourceVersion = ""
	if err := r.Create(ctx, replacement); err != nil {
		t.Fatalf("create replacement incarnation: %v", err)
	}
	if done, err := r.settleACPClassWorkspace(ctx, task); err != nil || !done {
		t.Fatalf("settle over a recreated incarnation = (%v, %v), want a clean skip", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, replacement); err != nil {
		t.Fatalf("the recreated incarnation must survive an old Task's settlement: %v", err)
	}
}

// Settlement can race deterministic-name replacement after its uncached read.
// The stamp patch and BeginRevocation must both fence the original UID so an
// old Task cannot mark or detach the replacement workspace.
func TestACPWorkspaceRevocationFencesReplacementIncarnation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := testBoundWorkspace(t, acpTestNamespace, "revocation-race", "class", "provider")
	workspace.Spec.AttachmentEpoch = 4
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{
		TaskRef:        workspacev1alpha1.ObjectIdentityReference{Name: "original-task", UID: types.UID("original-task-uid")},
		Epoch:          4,
		TokenSecretRef: workspacev1alpha1.SecretReference{Name: attachmentSecretName(workspace.Name, 4)},
	}
	scheme := testACPWorkspaceScheme(t)
	replaced := false
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&workspacev1alpha1.ExecutionWorkspace{}).
		WithObjects(workspace).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				candidate, isWorkspace := obj.(*workspacev1alpha1.ExecutionWorkspace)
				if isWorkspace && candidate.Annotations[acpWorkspaceRevocationStartedAnnotation] != "" && !replaced {
					current := &workspacev1alpha1.ExecutionWorkspace{}
					key := client.ObjectKeyFromObject(workspace)
					if err := cl.Get(ctx, key, current); err != nil {
						return err
					}
					if err := cl.Delete(ctx, current); err != nil {
						return err
					}
					replacement := current.DeepCopy()
					replacement.ObjectMeta = metav1.ObjectMeta{
						Namespace: current.Namespace,
						Name:      current.Name,
						UID:       types.UID("replacement-workspace-uid"),
						Labels:    current.Labels,
					}
					replacement.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{
						TaskRef:        workspacev1alpha1.ObjectIdentityReference{Name: "replacement-task", UID: types.UID("replacement-task-uid")},
						Epoch:          4,
						TokenSecretRef: workspacev1alpha1.SecretReference{Name: attachmentSecretName(workspace.Name, 4)},
					}
					replacement.Status = workspacev1alpha1.ExecutionWorkspaceStatus{}
					if err := cl.Create(ctx, replacement); err != nil {
						return err
					}
					replaced = true
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	r := &TaskReconciler{Client: c, APIReader: c, Scheme: scheme}
	original := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), original); err != nil {
		t.Fatalf("read original workspace: %v", err)
	}

	if err := r.markACPWorkspaceRevocationStarted(ctx, original, 4); err == nil {
		t.Fatal("revocation stamp patch succeeded across workspace replacement")
	}
	replacement := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), replacement); err != nil {
		t.Fatalf("read replacement workspace: %v", err)
	}
	if replacement.UID != "replacement-workspace-uid" ||
		replacement.Annotations[acpWorkspaceRevocationStartedAnnotation] != "" ||
		replacement.Spec.Attachment == nil || replacement.Spec.Attachment.TaskRef.UID != "replacement-task-uid" {
		t.Fatalf("replacement workspace was changed by stale stamp: %#v", replacement)
	}

	manager := WorkspaceAttachmentManager{Client: c}
	if err := manager.BeginRevocation(ctx, original, 4); err == nil || !strings.Contains(err.Error(), "was replaced") {
		t.Fatalf("BeginRevocation across replacement error = %v, want incarnation rejection", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), replacement); err != nil {
		t.Fatalf("re-read replacement workspace: %v", err)
	}
	if replacement.Spec.Attachment == nil || replacement.Spec.Attachment.TaskRef.UID != "replacement-task-uid" {
		t.Fatalf("replacement attachment was revoked by stale Task: %#v", replacement.Spec.Attachment)
	}
}

func TestSettleACPClassWorkspaceQuarantinesPastDetachTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatalf("attach = (%v)", ready)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read attached workspace: %v", err)
	}
	workspace.Status.AttachedEpoch = workspace.Spec.Attachment.Epoch
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("enforce epoch: %v", err)
	}

	// First settle revokes and stamps the revocation start; the adapter still
	// enforces the epoch and the deadline has not passed, so it waits.
	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || done {
		t.Fatalf("settle while enforced = (%v, %v), want (false, nil)", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read revoked workspace: %v", err)
	}
	if workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] == "" {
		t.Fatalf("revocation start must be stamped")
	}

	// Backdate the stamp past the frozen detachTimeout: settlement quarantines
	// the workspace and reports done so the Task finalizer releases.
	base := workspace.DeepCopy()
	expired := time.Now().UTC().Add(-workspace.Spec.Lifecycle.DetachTimeout.Duration - time.Minute)
	workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf("%d %s",
		workspace.Spec.AttachmentEpoch, expired.Format(time.RFC3339Nano))
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("backdate revocation: %v", err)
	}
	// A stamp bound to a previous epoch never serves as this revocation's
	// deadline: it is replaced with a fresh stamp instead of quarantining.
	staleBase := workspace.DeepCopy()
	workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf("%d %s",
		workspace.Spec.AttachmentEpoch-1, expired.Format(time.RFC3339Nano))
	if err := r.Patch(ctx, workspace, client.MergeFrom(staleBase)); err != nil {
		t.Fatalf("stale-epoch stamp: %v", err)
	}
	if done, err := r.settleACPClassWorkspace(ctx, task); err != nil || done {
		t.Fatalf("stale-epoch settle = (%v, %v), want a fresh deadline instead of quarantine", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("re-read workspace: %v", err)
	}
	if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined {
		t.Fatal("a stale-epoch stamp must never quarantine the live workspace")
	}
	base = workspace.DeepCopy()
	workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf("%d %s",
		workspace.Spec.AttachmentEpoch, expired.Format(time.RFC3339Nano))
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("re-backdate revocation: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, task)
	if err != nil || !done {
		t.Fatalf("settle past deadline = (%v, %v), want (true, nil)", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("quarantined workspace must survive: %v", err)
	}
	if workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined {
		t.Fatalf("desired state = %q, want Quarantined", workspace.Spec.DesiredState)
	}
}

func TestQuarantineACPWorkspacePastDetachTimeoutRefusesForeignCredentials(t *testing.T) {
	t.Parallel()
	const (
		secretKind = "Secret"
		leaseKind  = "Lease"
	)
	tests := []struct {
		name   string
		object func(*workspacev1alpha1.ExecutionWorkspace, metav1.OwnerReference) client.Object
		empty  func() client.Object
	}{
		{
			name: secretKind,
			object: func(workspace *workspacev1alpha1.ExecutionWorkspace, owner metav1.OwnerReference) client.Object {
				return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      attachmentSecretName(workspace.Name, workspace.Spec.AttachmentEpoch),
					OwnerReferences: []metav1.OwnerReference{
						owner,
					},
				}}
			},
			empty: func() client.Object { return &corev1.Secret{} },
		},
		{
			name: leaseKind,
			object: func(workspace *workspacev1alpha1.ExecutionWorkspace, owner metav1.OwnerReference) client.Object {
				return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      attachmentLeaseName(workspace.Name),
					OwnerReferences: []metav1.OwnerReference{
						owner,
					},
				}}
			},
			empty: func() client.Object { return &coordinationv1.Lease{} },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := testBoundWorkspace(t, acpTestNamespace, "foreign-quarantine", "class", "provider")
			workspace.Spec.AttachmentEpoch = 3
			workspace.Spec.Lifecycle.DetachTimeout = metav1.Duration{Duration: time.Minute}
			if workspace.Annotations == nil {
				workspace.Annotations = map[string]string{}
			}
			expired := time.Now().UTC().Add(-2 * time.Minute)
			workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf(
				"%d %s", workspace.Spec.AttachmentEpoch, expired.Format(time.RFC3339Nano),
			)
			foreignWorkspace := workspace.DeepCopy()
			foreignWorkspace.Name = "foreign-owner"
			foreignWorkspace.UID = types.UID("foreign-owner-uid")
			foreignOwner := *metav1.NewControllerRef(
				foreignWorkspace,
				workspacev1alpha1.GroupVersion.WithKind("ExecutionWorkspace"),
			)
			foreign := test.object(workspace, foreignOwner)
			r := acpClassTestReconciler(t, workspace, foreign)
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read workspace: %v", err)
			}

			done, err := r.quarantineACPWorkspacePastDetachTimeout(ctx, current)
			if err == nil || !strings.Contains(err.Error(), "is not controlled by workspace") || done {
				t.Fatalf("quarantine with foreign %s = (%v, %v), want ownership rejection", test.name, done, err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(foreign), test.empty()); err != nil {
				t.Fatalf("foreign %s must be preserved: %v", test.name, err)
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("reload workspace: %v", err)
			}
			if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined {
				t.Fatalf("desired state = %q, want fail-closed Quarantined", current.Spec.DesiredState)
			}
		})
	}
}

// TestResolveACPClassRejectsDeletingProviderConfig proves the resolver fails
// closed on a RuntimeProviderConfig the operator has already withdrawn.
func TestResolveACPClassRejectsDeletingProviderConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
		now := metav1.Now()
		f.config.DeletionTimestamp = &now
		f.config.Finalizers = []string{"acp.workspace.orka.ai/e2e-hold"}
	})
	r := acpClassTestReconciler(t, fixture.objects()...)
	if _, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask()); err == nil ||
		!strings.Contains(err.Error(), "is being deleted") {
		t.Fatalf("error = %v, want the deleting provider config rejected", err)
	}
}

func TestSettleACPClassWorkspaceRevokesAndDeletes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatalf("attach = (%v)", ready)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); err != nil {
		t.Fatalf("reload task: %v", err)
	}

	// Simulate the adapter enforcing the epoch: settlement must not finalize
	// or delete while the data plane still reports the attachment.
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read attached workspace: %v", err)
	}
	workspace.Status.AttachedEpoch = workspace.Spec.Attachment.Epoch
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("enforce epoch: %v", err)
	}
	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil {
		t.Fatalf("settle while enforced: %v", err)
	}
	if done {
		t.Fatalf("settlement must wait for the adapter to release the epoch")
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("workspace must still exist while the epoch is enforced: %v", err)
	}
	if workspace.Spec.Attachment != nil {
		t.Fatalf("revocation must clear attachment intent")
	}

	// Adapter releases the epoch; settlement finalizes and deletes.
	workspace.Status.AttachedEpoch = 0
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("release epoch: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, task)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if !done {
		t.Fatalf("settlement must complete after the epoch is released")
	}
	err = r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("workspace must be deleted, got %v", err)
	}

	// Settlement is idempotent after deletion.
	if done, err := r.settleACPClassWorkspace(ctx, task); err != nil || !done {
		t.Fatalf("repeat settle = (%v, %v)", done, err)
	}
}

// A frozen snapshot carrying a Retain deletion action must fail closed on an
// older controller whose only implemented settlement action destroys the
// workspace: rollback must never begin destructive cleanup under a retention
// contract this version cannot honor.
func TestValidateACPWorkspaceClassBindingRejectsRetainActions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	r := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask())
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(acpClassTestTask(), "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	tampered := *binding.Class
	tampered.DeletionPolicy.PersistentVolumes = string(workspacev1alpha1.WorkspaceDeletionActionRetain)
	if err := validateACPWorkspaceClassBindingValues(&tampered); err == nil ||
		!strings.Contains(err.Error(), "only Delete is supported") {
		t.Fatalf("error = %v, want a fail-closed Retain rejection", err)
	}
}

func TestValidateACPWorkspaceClassBindingRejectsInvalidLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	r := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask())
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(acpClassTestTask(), "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ACPWorkspaceClassBinding)
		want   string
	}{
		{
			name: "default action outside allowlist",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.DefaultOnDetach = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
			},
			want: "default detach action \"Suspend\" is not allowed",
		},
		{
			name: "effective action outside allowlist",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.DefaultOnDetach = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
				class.AllowedOnDetach = []string{string(workspacev1alpha1.WorkspaceOnDetachSuspend)}
			},
			want: "effective detach action \"Delete\" is not allowed",
		},
		{
			name: "zero detach timeout",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.DetachTimeout = "0s"
			},
			want: "detach timeout must be positive",
		},
		{
			name: "negative idle timeout",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.IdleTimeout = "-1s"
			},
			want: "idle timeout must be positive",
		},
		{
			name: "zero maximum lifetime",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.MaxLifetime = "0s"
			},
			want: "maximum lifetime must be positive",
		},
		{
			name: "maximum lifetime below idle timeout",
			mutate: func(class *ACPWorkspaceClassBinding) {
				class.IdleTimeout = "2h"
				class.MaxLifetime = "1h"
			},
			want: "maximum lifetime must be greater than or equal to idle timeout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tampered := *binding.Class
			tampered.AllowedOnDetach = append([]string(nil), binding.Class.AllowedOnDetach...)
			tt.mutate(&tampered)
			if err := validateACPWorkspaceClassBindingValues(&tampered); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

// A same-name replacement of the immutable RuntimeProviderConfig is fenced by
// the adapter-pinned config identity: class resolution must fail closed
// instead of dispatching new Tasks onto the replacement backend.
func TestResolveACPWorkspaceClassRejectsReplacedProviderConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
		f.provider.Status.PinnedParametersUID = "the-original-config-uid"
	})
	r := acpClassTestReconciler(t, fixture.objects()...)
	if _, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask()); err == nil ||
		!strings.Contains(err.Error(), "was replaced") {
		t.Fatalf("error = %v, want a fail-closed replaced-config rejection", err)
	}
}

// A durable-volume profile bound to a retaining StorageClass violates the
// all-Delete lifecycle: finalization would report the volume deleted while
// Kubernetes leaves the PV and repository data behind.
func TestResolveACPWorkspaceClassRejectsRetainingStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	retain := corev1.PersistentVolumeReclaimRetain
	retaining := &storagev1.StorageClass{
		ObjectMeta:    metav1.ObjectMeta{Name: "retaining-class"},
		Provisioner:   acpTestStorageProvisioner,
		ReclaimPolicy: &retain,
	}
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
		f.profile.Spec.AgentSandbox = &acpworkspacev1alpha1.AgentSandboxProfileSpec{
			Suspend: &acpworkspacev1alpha1.AgentSandboxSuspendPolicy{
				Mode: acpworkspacev1alpha1.SubstrateSuspendModeDataOnly,
				Volume: acpworkspacev1alpha1.AgentSandboxDurableVolume{
					Capacity: acpTestDurableCapacity, StorageClassName: "retaining-class",
				},
			},
		}
	})
	r := acpClassTestReconciler(t, append(fixture.objects(), retaining)...)
	if _, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask()); err == nil ||
		!strings.Contains(err.Error(), "only Delete reclaim is admitted") {
		t.Fatalf("error = %v, want a retaining-class rejection", err)
	}
}

// The metadata annotation is only a mirror. Class resolution must wait for
// the adapter to establish the controller-owned status pin before it can
// authorize a new workspace against this provider.
func TestResolveACPWorkspaceClassRequiresProtectedProviderConfigPin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox, func(f *acpClassFixture) {
		f.provider.Status.PinnedParametersUID = ""
		f.provider.Annotations = map[string]string{
			acpWorkspaceProviderConfigUIDAnnotation: string(f.config.UID),
		}
	})
	r := acpClassTestReconciler(t, fixture.objects()...)
	if _, err := r.resolveACPWorkspaceClass(ctx, acpClassTestTask()); err == nil ||
		!strings.Contains(err.Error(), "no protected RuntimeProviderConfig UID pin") {
		t.Fatalf("error = %v, want the missing protected-pin rejection", err)
	}
}

// A terminally Failed workspace still held by its predecessor keeps the
// continuation queued: the predecessor's settlement removes this incarnation
// and the deterministic name is recreated fresh, so a permanent
// WorkspaceFailed rejection would be wrong.
func TestEnsureACPClassWorkspaceQueuesBehindFailedAttachedPredecessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	// The predecessor still holds the attachment when the workspace fails.
	workspace.Spec.AttachmentEpoch = 4
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{
		TaskRef:        workspacev1alpha1.ObjectIdentityReference{Name: "predecessor", UID: types.UID("predecessor-uid")},
		Epoch:          4,
		TokenSHA256:    "sha256:" + strings.Repeat("f", 64),
		TokenSecretRef: workspacev1alpha1.SecretReference{Name: "predecessor-secret"},
		ExpiresAt:      metav1.Now(),
	}
	if err := r.Update(ctx, workspace); err != nil {
		t.Fatalf("attach predecessor: %v", err)
	}
	workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	if err := r.Status().Update(ctx, workspace); err != nil {
		t.Fatalf("fail workspace: %v", err)
	}
	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil || ready || name != "" {
		t.Fatalf("ensure behind a failed attached predecessor = (%q, %v, %v), want queued", name, ready, err)
	}
}

// The attached-ready fast path rechecks the frozen maxLifetime: a deadline
// that elapsed between Attach and the adapter's Failed publication must not
// admit RuntimePool demand from the stale Ready observation.
func TestEnsureACPClassWorkspaceRefusesReadyPastMaxLifetime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatalf("attach = (%v)", ready)
	}
	// The lifetime elapses after attachment; the adapter has not yet failed
	// the workspace.
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}, workspace); err != nil {
		t.Fatalf("reload workspace: %v", err)
	}
	base := workspace.DeepCopy()
	workspace.Spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: time.Millisecond}
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("bound lifetime: %v", err)
	}
	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil || ready || name != "" {
		t.Fatalf("ensure past maxLifetime = (%q, %v, %v), want not-ready", name, ready, err)
	}
}

// An attachment can expire while a Task still waits for its RuntimePool. The
// readiness path must rotate the epoch and bearer before recreating pool
// demand, then delete the superseded bearer only after the new epoch is
// enforced.
func TestEnsureACPClassWorkspaceRotatesExpiredAttachmentBeforeReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatal("initial attachment did not become ready")
	}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("reload attached workspace: %v", err)
	}
	firstEpoch := workspace.Spec.Attachment.Epoch
	firstSecret := workspace.Spec.Attachment.TokenSecretRef.Name
	firstDigest := workspace.Spec.Attachment.TokenSHA256
	base := workspace.DeepCopy()
	workspace.Spec.Attachment.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Minute))
	// Simulate stale metadata from a predecessor. Credential rotation must bind
	// this Task's frozen action atomically with the replacement attachment.
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("expire attachment: %v", err)
	}
	acknowledgeTestACPWorkspaceAttachment(t, r, workspace)

	name, ready, err := r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil || ready || name != "" {
		t.Fatalf("ensure with expired attachment = (%q, %v, %v), want rotation pending", name, ready, err)
	}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("reload rotated workspace: %v", err)
	}
	rotated := workspace.Spec.Attachment
	if rotated == nil || rotated.Epoch != firstEpoch+1 || rotated.TaskRef.UID != task.UID {
		t.Fatalf("rotated attachment = %#v, want Task %s at epoch %d", rotated, task.UID, firstEpoch+1)
	}
	if rotated.TokenSecretRef.Name == firstSecret || rotated.TokenSHA256 == firstDigest || !rotated.ExpiresAt.After(time.Now()) {
		t.Fatalf("rotation did not replace and renew the expired credential: %#v", rotated)
	}
	if got := workspace.Annotations[acpWorkspaceDetachActionAnnotation]; got != binding.Class.EffectiveOnDetach {
		t.Fatalf("rotated detach action = %q, want %q", got, binding.Class.EffectiveOnDetach)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: firstSecret}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired attachment Secret still exists after rotation: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: rotated.TokenSecretRef.Name}, &corev1.Secret{}); err != nil {
		t.Fatalf("rotated attachment Secret: %v", err)
	}

	acknowledgeTestACPWorkspaceAttachment(t, r, workspace)
	name, ready, err = r.ensureACPClassWorkspace(ctx, task, plan)
	if err != nil || !ready || name != workspaceName {
		t.Fatalf("ensure after rotation handshake = (%q, %v, %v), want ready", name, ready, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: firstSecret}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("superseded attachment Secret still exists: %v", err)
	}
}

// Running ACP Tasks bypass the queue path, so their attachment expiry must be
// revisited by handleRunning. Rotation keeps the same Task attached and the
// public projection advances only after the adapter acknowledges the new epoch.
func TestHandleRunningRotatesExpiredACPClassWorkspaceAttachment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newACPClassFixture(t, acpworkspacev1alpha1.RuntimeProviderBackendAgentSandbox)
	task := acpClassTestTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSandboxPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	workspaceName := acpClassWorkspaceName(task, binding)
	workspaceKey := types.NamespacedName{Namespace: task.Namespace, Name: workspaceName}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, workspaceKey, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatal("initial attachment did not become ready")
	}
	if err := r.Get(ctx, workspaceKey, workspace); err != nil {
		t.Fatalf("reload attached workspace: %v", err)
	}
	firstEpoch := workspace.Spec.Attachment.Epoch
	firstSecret := workspace.Spec.Attachment.TokenSecretRef.Name

	running := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), running); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	running.Status.Phase = corev1alpha1.TaskPhaseRunning
	running.Status.Execution = &corev1alpha1.TaskExecutionStatus{
		State: corev1alpha1.TaskExecutionStateRunning, RuntimePoolName: plan.PoolName,
	}
	running.Status.ExecutionWorkspace = &corev1alpha1.ExecutionWorkspaceStatus{
		Provider:      binding.Provider,
		Phase:         corev1alpha1.ExecutionWorkspacePhaseReady,
		Reason:        corev1alpha1.ExecutionWorkspaceReasonReady,
		ClassRef:      &corev1alpha1.WorkspaceClassReference{Name: workspace.Spec.ClassBinding.Name},
		WorkspaceRef:  &corev1alpha1.WorkspaceObjectReference{Name: workspace.Name, UID: string(workspace.UID)},
		State:         string(workspacev1alpha1.ExecutionWorkspaceStateAttached),
		AttachedEpoch: firstEpoch,
	}
	if err := r.Status().Update(ctx, running); err != nil {
		t.Fatalf("mark task running: %v", err)
	}
	base := workspace.DeepCopy()
	workspace.Spec.Attachment.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Minute))
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("expire attachment: %v", err)
	}
	acknowledgeTestACPWorkspaceAttachment(t, r, workspace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), running); err != nil {
		t.Fatalf("reload running task: %v", err)
	}
	result, err := r.handleRunning(ctx, running)
	if err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("handleRunning = (%v, %v), want one-second requeue after rotation", result, err)
	}
	if err := r.Get(ctx, workspaceKey, workspace); err != nil {
		t.Fatalf("reload rotated workspace: %v", err)
	}
	if workspace.Spec.Attachment == nil || workspace.Spec.Attachment.Epoch != firstEpoch+1 ||
		workspace.Spec.Attachment.TaskRef.UID != task.UID || !workspace.Spec.Attachment.ExpiresAt.After(time.Now()) {
		t.Fatalf("running attachment was not rotated: %#v", workspace.Spec.Attachment)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: firstSecret}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired running attachment Secret still exists: %v", err)
	}

	rotatedEpoch := workspace.Spec.Attachment.Epoch
	if _, err := r.handleRunning(ctx, running); err != nil {
		t.Fatalf("repeat handleRunning before acknowledgement: %v", err)
	}
	if err := r.Get(ctx, workspaceKey, workspace); err != nil {
		t.Fatalf("reload pending rotation: %v", err)
	}
	if workspace.Spec.Attachment.Epoch != rotatedEpoch {
		t.Fatalf("pending rotation advanced twice: got epoch %d, want %d", workspace.Spec.Attachment.Epoch, rotatedEpoch)
	}

	acknowledgeTestACPWorkspaceAttachment(t, r, workspace)
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), running); err != nil {
		t.Fatalf("reload task before projection refresh: %v", err)
	}
	if err := r.projectACPExecutionWorkspaceStatus(ctx, running); err != nil {
		t.Fatalf("refresh rotated attachment projection: %v", err)
	}
	if running.Status.ExecutionWorkspace.AttachedEpoch != rotatedEpoch {
		t.Fatalf("projected attachment epoch = %d, want acknowledged epoch %d", running.Status.ExecutionWorkspace.AttachedEpoch, rotatedEpoch)
	}
}
