/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sevents "k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
)

const (
	advancedFenceValue    = "advanced"
	emptyCaseName         = "empty"
	invalidTimestamp      = "not-a-timestamp"
	malformedCaseName     = "malformed"
	replacementSessionUID = "replacement-session-uid"
	whitespaceCaseName    = "whitespace"
	whitespaceOnlyValue   = " \t "
	wrongClassUIDValue    = "wrong-class-uid"
)

func retentionTestWorkspace(t *testing.T, name string, mutate ...func(*workspacev1alpha1.ExecutionWorkspace)) *workspacev1alpha1.ExecutionWorkspace {
	t.Helper()
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = name
	workspace.UID = types.UID(name + "-uid")
	workspace.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	// Retention acts only once the core cleanup finalizer is installed.
	workspace.Finalizers = append(workspace.Finalizers, executionWorkspaceFinalizer)
	workspace.Spec.Lifecycle.IdleTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	workspace.Spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: 24 * time.Hour}
	for _, m := range mutate {
		m(workspace)
	}
	return workspace
}

func reconcileRetention(t *testing.T, c client.Client, workspace *workspacev1alpha1.ExecutionWorkspace) ctrl.Result {
	t.Helper()
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name},
	})
	if err != nil {
		t.Fatalf("retention reconcile: %v", err)
	}
	return result
}

func retentionFenceTimestampClient(base client.WithWatch, now func() time.Time) client.WithWatch {
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, object client.Object, options ...client.CreateOption) error {
			lease, isLease := object.(*coordinationv1.Lease)
			if isLease && strings.HasPrefix(lease.Name, labels.ACPWorkspaceRetentionFenceLeaseNamePrefix) &&
				lease.CreationTimestamp.IsZero() {
				lease.CreationTimestamp = metav1.NewTime(now().UTC())
			}
			return c.Create(ctx, object, options...)
		},
	})
}

func TestACPWorkspaceRetentionExpiresSuspendedWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-a", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("suspended workspace past its idle retention must be deleting (finalizer-held), got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionFenceOrdersRacingContinuations(t *testing.T) {
	t.Parallel()
	fenceTime := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name          string
		createdAt     time.Time
		controllerNow time.Time
		wantDeleting  bool
		wantFenceGone bool
	}{
		{name: "pre-fence", createdAt: fenceTime.Add(-time.Second), wantFenceGone: true},
		{name: "post-fence", createdAt: fenceTime.Add(time.Second), wantDeleting: true},
		{
			name:          "controller clock trails API server",
			createdAt:     fenceTime.Add(-time.Second),
			controllerNow: fenceTime.Add(-time.Minute),
			wantFenceGone: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			sessionUID := "retention-race-session-uid-" + test.name
			workspace := retentionTestWorkspace(t, "acp-ws-retention-race-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
					Name: acpTestSessionName, UID: types.UID(sessionUID),
				}
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = fenceTime.Add(-time.Hour).Format(time.RFC3339Nano)
				delete(w.Annotations, acpWorkspaceResumeRequestedAnnotation)
			})
			control := &corev1alpha1.RuntimeSessionControl{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
					UID:       types.UID("retention-race-control-uid-" + test.name),
				},
				Spec: corev1alpha1.RuntimeSessionControlSpec{
					SessionName: acpTestSessionName,
					SessionUID:  sessionUID,
				},
			}
			continuation := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         workspace.Namespace,
					Name:              "retention-race-continuation-" + test.name,
					UID:               types.UID("retention-race-continuation-uid-" + test.name),
					CreationTimestamp: metav1.NewTime(test.createdAt),
					Labels: map[string]string{
						acpExecutionWorkspaceLinkLabel: workspace.Name,
					},
					Annotations: map[string]string{
						acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
					},
				},
				Spec: corev1alpha1.TaskSpec{
					Type:       corev1alpha1.TaskTypeAgent,
					SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
				},
			}
			baseClient := acpAdapterTestClient(t, workspace, control)
			timestamped := retentionFenceTimestampClient(baseClient, func() time.Time { return fenceTime })
			continuationCreated := false
			intercepted := interceptor.NewClient(timestamped, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, object client.Object, options ...client.CreateOption) error {
					lease, isLease := object.(*coordinationv1.Lease)
					if !isLease || !strings.HasPrefix(lease.Name, labels.ACPWorkspaceRetentionFenceLeaseNamePrefix) {
						return c.Create(ctx, object, options...)
					}
					if err := c.Create(ctx, object, options...); err != nil {
						return err
					}
					continuationCreated = true
					return c.Create(ctx, continuation)
				},
			})
			controllerNow := test.controllerNow
			if controllerNow.IsZero() {
				controllerNow = fenceTime
			}
			reconciler := &ACPWorkspaceRetentionReconciler{
				Client: intercepted, APIReader: intercepted, Now: func() time.Time { return controllerNow },
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("retention reconcile: %v", err)
			}
			if !continuationCreated {
				t.Fatal("test did not create the continuation after the initial demand scan")
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read workspace after retention race: %v", err)
			}
			if got := !current.DeletionTimestamp.IsZero(); got != test.wantDeleting {
				t.Fatalf("workspace deleting = %v, want %v", got, test.wantDeleting)
			}
			fence := &coordinationv1.Lease{}
			err := baseClient.Get(ctx, types.NamespacedName{
				Namespace: workspace.Namespace,
				Name:      acpWorkspaceRetentionFenceLeaseName(workspace),
			}, fence)
			if got := apierrors.IsNotFound(err); got != test.wantFenceGone {
				t.Fatalf("retention fence absent = %v, want %v, err=%v", got, test.wantFenceGone, err)
			}
		})
	}
}

func TestACPWorkspaceRetentionFenceBlocksOldIncarnationUntilReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fenceTime := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	binding := &ACPRuntimeWorkspaceBinding{
		ReusePolicy:   corev1alpha1.WorkspaceReusePolicySession,
		SessionUID:    "retention-fence-session-uid",
		WorkspaceSlot: defaultWorkspaceSlotName,
		Class:         &ACPWorkspaceClassBinding{UID: "retention-fence-class-uid"},
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Namespace:         acpTestNamespace,
		Name:              "pre-expiry-task",
		UID:               types.UID("pre-expiry-task-uid"),
		CreationTimestamp: metav1.NewTime(fenceTime.Add(-time.Second)),
	}}
	workspaceName := acpClassWorkspaceName(task, binding)
	fencedUID := types.UID("expired-workspace-uid")
	fence := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace:         task.Namespace,
		Name:              labels.ACPWorkspaceRetentionFenceLeaseNamePrefix + "0123456789abcdef01234567",
		CreationTimestamp: metav1.NewTime(fenceTime),
		Labels: map[string]string{
			acpWorkspaceRetentionFenceIdentityLabel: labels.SelectorValue(workspaceName),
		},
		Annotations: map[string]string{
			acpWorkspaceRetentionFenceNameAnnotation:       workspaceName,
			acpWorkspaceRetentionFenceUIDAnnotation:        string(fencedUID),
			acpWorkspaceRetentionFenceSessionUIDAnnotation: binding.SessionUID,
			acpWorkspaceRetentionFenceClassUIDAnnotation:   binding.Class.UID,
		},
	}}
	kubeClient := acpAdapterTestClient(t, fence)
	reconciler := &TaskReconciler{Client: kubeClient, APIReader: kubeClient}

	stillPresent := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: task.Namespace, Name: workspaceName, UID: fencedUID},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			DesiredState: workspacev1alpha1.ExecutionWorkspaceDesiredSuspended,
		},
	}
	blocked, err := reconciler.taskBlockedByACPWorkspaceRetentionFence(ctx, task, binding, workspaceName, stillPresent)
	if err != nil || !blocked {
		t.Fatalf("Task against the fenced workspace = (blocked=%v, err=%v), want blocked", blocked, err)
	}

	deleting := stillPresent.DeepCopy()
	deleting.DeletionTimestamp = &metav1.Time{Time: fenceTime.Add(time.Second)}
	blocked, err = reconciler.taskBlockedByACPWorkspaceRetentionFence(ctx, task, binding, workspaceName, deleting)
	if err != nil || !blocked {
		t.Fatalf("Task against the deleting fenced workspace = (blocked=%v, err=%v), want blocked", blocked, err)
	}

	replacement := stillPresent.DeepCopy()
	replacement.UID = types.UID("replacement-workspace-uid")
	blocked, err = reconciler.taskBlockedByACPWorkspaceRetentionFence(ctx, task, binding, workspaceName, replacement)
	if err != nil || blocked {
		t.Fatalf("Task against a replacement workspace = (blocked=%v, err=%v), want admitted", blocked, err)
	}

	postExpiry := task.DeepCopy()
	postExpiry.Name = "post-expiry-task"
	postExpiry.UID = types.UID("post-expiry-task-uid")
	postExpiry.CreationTimestamp = metav1.NewTime(fenceTime.Add(time.Second))
	blocked, err = reconciler.taskBlockedByACPWorkspaceRetentionFence(ctx, postExpiry, binding, workspaceName, stillPresent)
	if err != nil || !blocked {
		t.Fatalf("post-fence Task against the old UID = (blocked=%v, err=%v), want blocked", blocked, err)
	}
	blocked, err = reconciler.taskBlockedByACPWorkspaceRetentionFence(ctx, postExpiry, binding, workspaceName, nil)
	if err != nil || blocked {
		t.Fatalf("post-fence Task after old UID removal = (blocked=%v, err=%v), want admitted", blocked, err)
	}
}

func TestACPWorkspaceRetentionFenceReusesLogicalWorkspaceLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	firstActivation := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	now := firstActivation
	sessionUID := "retention-reuse-session-uid"
	workspace := retentionTestWorkspace(t, "acp-ws-retention-reuse", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID(sessionUID),
		}
	})
	control := &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workspace.Namespace,
			Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
			UID:       types.UID("retention-reuse-control-uid"),
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName: acpTestSessionName,
			SessionUID:  sessionUID,
		},
	}
	baseClient := acpAdapterTestClient(t, control)
	c := retentionFenceTimestampClient(baseClient, func() time.Time { return now })
	reconciler := &ACPWorkspaceRetentionReconciler{
		Client: c, APIReader: c, Now: func() time.Time { return now },
	}
	first, err := reconciler.ensureACPWorkspaceRetentionFence(ctx, workspace)
	if err != nil {
		t.Fatalf("create first fence: %v", err)
	}
	if activatedAt, err := acpWorkspaceRetentionFenceActivation(first); err != nil || !activatedAt.Equal(firstActivation) {
		t.Fatalf("first fence activation = (%v, %v), want %v", activatedAt, err, firstActivation)
	}
	replacement := workspace.DeepCopy()
	replacement.UID = types.UID("acp-ws-retention-reuse-replacement-uid")
	secondActivation := firstActivation.Add(time.Minute)
	now = secondActivation
	second, err := reconciler.ensureACPWorkspaceRetentionFence(ctx, replacement)
	if err != nil {
		t.Fatalf("reuse fence for replacement: %v", err)
	}
	if first.Name != second.Name {
		t.Fatalf("replacement fence name = %q, want logical fence %q", second.Name, first.Name)
	}
	if second.Annotations[acpWorkspaceRetentionFenceUIDAnnotation] != string(replacement.UID) {
		t.Fatalf("fenced UID = %q, want %q", second.Annotations[acpWorkspaceRetentionFenceUIDAnnotation], replacement.UID)
	}
	if activatedAt, err := acpWorkspaceRetentionFenceActivation(second); err != nil || !activatedAt.Equal(secondActivation) {
		t.Fatalf("replacement fence activation = (%v, %v), want %v", activatedAt, err, secondActivation)
	}
	list := &coordinationv1.LeaseList{}
	if err := c.List(ctx, list, client.InNamespace(workspace.Namespace)); err != nil {
		t.Fatalf("list retention fences: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("retention fence count = %d, want one reused Lease", len(list.Items))
	}
}

func TestACPWorkspaceRetentionFenceReplacesInvalidReservedLease(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*coordinationv1.Lease)
	}{
		{
			name: "malformed identity",
			mutate: func(lease *coordinationv1.Lease) {
				delete(lease.Annotations, acpWorkspaceRetentionFenceNameAnnotation)
			},
		},
		{
			name: "wrong owner",
			mutate: func(lease *coordinationv1.Lease) {
				lease.OwnerReferences[0].UID = types.UID("wrong-control-uid")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, 8, 28, 7, 0, 0, 0, time.UTC)
			suffix := strings.ReplaceAll(test.name, " ", "-")
			sessionUID := "invalid-fence-session-uid-" + suffix
			workspace := retentionTestWorkspace(t, "acp-ws-invalid-fence-"+suffix, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
					Name: acpTestSessionName, UID: types.UID(sessionUID),
				}
				w.Spec.Lifecycle.MaxLifetime = nil
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
			})
			control := &corev1alpha1.RuntimeSessionControl{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
					UID:       types.UID("invalid-fence-control-uid-" + suffix),
				},
				Spec: corev1alpha1.RuntimeSessionControlSpec{
					SessionName: acpTestSessionName,
					SessionUID:  sessionUID,
				},
			}
			invalid := newACPWorkspaceRetentionFence(workspace, control)
			invalid.UID = types.UID("invalid-fence-uid-" + suffix)
			invalid.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))
			test.mutate(invalid)

			baseClient := acpAdapterTestClient(t, workspace, control, invalid)
			c := retentionFenceTimestampClient(baseClient, func() time.Time { return now })
			reconciler := &ACPWorkspaceRetentionReconciler{
				Client: c, APIReader: c, Now: func() time.Time { return now },
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("reconcile invalid retention fence: %v", err)
			}

			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil || current.DeletionTimestamp.IsZero() {
				t.Fatalf("idle-expired workspace must enter cleanup after fence recovery, got err=%v deleting=%v", err, current.DeletionTimestamp)
			}
			replacement := &coordinationv1.Lease{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(invalid), replacement); err != nil {
				t.Fatalf("read replacement retention fence: %v", err)
			}
			if err := validateACPWorkspaceRetentionFence(replacement, current, control); err != nil {
				t.Fatalf("replacement retention fence is invalid: %v", err)
			}
			if activatedAt, err := acpWorkspaceRetentionFenceActivation(replacement); err != nil || !activatedAt.Equal(now) {
				t.Fatalf("replacement fence activation = (%v, %v), want %v", activatedAt, err, now)
			}
		})
	}
}

func TestACPWorkspaceRetentionReclaimsMalformedFenceWithoutIdleTimeout(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		age          time.Duration
		maxLifetime  time.Duration
		wantDeleting bool
	}{
		{name: "before max lifetime", age: time.Hour, maxLifetime: 2 * time.Hour},
		{name: "after max lifetime", age: 2 * time.Hour, maxLifetime: time.Hour, wantDeleting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC)
			suffix := strings.ReplaceAll(test.name, " ", "-")
			sessionUID := "max-lifetime-fence-session-uid-" + suffix
			workspace := retentionTestWorkspace(t, "acp-ws-max-lifetime-fence-"+suffix, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.CreationTimestamp = metav1.NewTime(now.Add(-test.age))
				w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
					Name: acpTestSessionName, UID: types.UID(sessionUID),
				}
				w.Spec.Lifecycle.IdleTimeout = nil
				w.Spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: test.maxLifetime}
			})
			control := &corev1alpha1.RuntimeSessionControl{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
					UID:       types.UID("max-lifetime-fence-control-uid-" + suffix),
				},
				Spec: corev1alpha1.RuntimeSessionControlSpec{
					SessionName: acpTestSessionName,
					SessionUID:  sessionUID,
				},
			}
			fence := newACPWorkspaceRetentionFence(workspace, control)
			fence.UID = types.UID("malformed-max-lifetime-fence-uid-" + suffix)
			fence.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))
			fence.Annotations[acpWorkspaceRetentionFenceUIDAnnotation] = ""

			c := acpAdapterTestClient(t, workspace, control, fence)
			binding := &ACPRuntimeWorkspaceBinding{
				ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
				SessionUID:  sessionUID,
				Class:       &ACPWorkspaceClassBinding{UID: string(workspace.Spec.ClassBinding.UID)},
			}
			task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace}}
			taskReconciler := &TaskReconciler{Client: c, APIReader: c}
			if blocked, err := taskReconciler.taskBlockedByACPWorkspaceRetentionFence(
				ctx, task, binding, workspace.Name, workspace,
			); blocked || !errors.Is(err, errACPWorkspaceBindingConflict) {
				t.Fatalf("Task before malformed fence recovery = (blocked=%v, err=%v), want fail-closed conflict", blocked, err)
			}

			reconciler := &ACPWorkspaceRetentionReconciler{
				Client: c, APIReader: c, Now: func() time.Time { return now },
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("reconcile maxLifetime-only malformed fence: %v", err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(fence), &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
				t.Fatalf("malformed retention fence survived recovery: %v", err)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read workspace after malformed fence recovery: %v", err)
			}
			if got := !current.DeletionTimestamp.IsZero(); got != test.wantDeleting {
				t.Fatalf("workspace deleting = %v, want %v", got, test.wantDeleting)
			}
			if blocked, err := taskReconciler.taskBlockedByACPWorkspaceRetentionFence(
				ctx, task, binding, workspace.Name, current,
			); err != nil || blocked {
				t.Fatalf("Task after malformed fence recovery = (blocked=%v, err=%v), want admitted", blocked, err)
			}
		})
	}
}

func TestACPWorkspaceRetentionReclaimsInvalidlyOwnedFenceWithoutIdleTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC)
	sessionUID := "invalid-owner-max-lifetime-session-uid"
	workspace := retentionTestWorkspace(t, "acp-ws-invalid-owner-max-lifetime", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID(sessionUID),
		}
		w.Spec.Lifecycle.IdleTimeout = nil
		w.Spec.Lifecycle.MaxLifetime = &metav1.Duration{Duration: 2 * time.Hour}
	})
	control := &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workspace.Namespace,
			Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
			UID:       types.UID("valid-max-lifetime-control-uid"),
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName: acpTestSessionName,
			SessionUID:  sessionUID,
		},
	}
	fence := newACPWorkspaceRetentionFence(workspace, control)
	fence.UID = types.UID("invalid-owner-max-lifetime-fence-uid")
	fence.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))
	fence.OwnerReferences[0].UID = types.UID("wrong-control-owner-uid")

	c := acpAdapterTestClient(t, workspace, control, fence)
	binding := &ACPRuntimeWorkspaceBinding{
		ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
		SessionUID:  sessionUID,
		Class:       &ACPWorkspaceClassBinding{UID: string(workspace.Spec.ClassBinding.UID)},
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: workspace.Namespace}}
	taskReconciler := &TaskReconciler{Client: c, APIReader: c}
	if blocked, err := taskReconciler.taskBlockedByACPWorkspaceRetentionFence(
		ctx, task, binding, workspace.Name, workspace,
	); err != nil || !blocked {
		t.Fatalf("Task before invalid-owner fence recovery = (blocked=%v, err=%v), want blocked", blocked, err)
	}

	reconciler := &ACPWorkspaceRetentionReconciler{
		Client: c, APIReader: c, Now: func() time.Time { return now },
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatalf("reconcile invalid-owner maxLifetime-only fence: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(fence), &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("invalidly owned retention fence survived recovery: %v", err)
	}
	if blocked, err := taskReconciler.taskBlockedByACPWorkspaceRetentionFence(
		ctx, task, binding, workspace.Name, workspace,
	); err != nil || blocked {
		t.Fatalf("Task after invalid-owner fence recovery = (blocked=%v, err=%v), want admitted", blocked, err)
	}
}

func TestACPWorkspaceRetentionFenceSkipsReplacementSessionControl(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-replaced-session", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("expired-session-uid"),
		}
	})
	control := &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workspace.Namespace,
			Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
			UID:       types.UID("replacement-control-uid"),
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName: acpTestSessionName,
			SessionUID:  replacementSessionUID,
		},
	}
	c := acpAdapterTestClient(t, control)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, APIReader: c}
	fence, err := reconciler.ensureACPWorkspaceRetentionFence(ctx, workspace)
	if err != nil || fence != nil {
		t.Fatalf("replacement Session fence = (%v, %v), want no surviving fence", fence, err)
	}
}

func TestACPWorkspaceRetentionRecoveredFenceHonorsActivation(t *testing.T) {
	t.Parallel()
	fenceTime := time.Now().UTC()
	for _, test := range []struct {
		name          string
		suffix        string
		createdAt     time.Time
		wantDeleting  bool
		wantFenceGone bool
	}{
		{name: "pre-fence demand", suffix: "pre-fence", createdAt: fenceTime.Add(-time.Second), wantFenceGone: true},
		{name: "post-fence demand", suffix: "post-fence", createdAt: fenceTime.Add(time.Second), wantDeleting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			sessionUID := "recovered-demand-session-uid-" + test.suffix
			workspace := retentionTestWorkspace(t, "acp-ws-retention-recovered-demand-"+test.suffix, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
					Name: acpTestSessionName, UID: types.UID(sessionUID),
				}
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = fenceTime.Add(-time.Hour).Format(time.RFC3339Nano)
				delete(w.Annotations, acpWorkspaceResumeRequestedAnnotation)
			})
			continuation := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         workspace.Namespace,
					Name:              "recovered-demand-task-" + test.suffix,
					UID:               types.UID("recovered-demand-task-uid-" + test.suffix),
					CreationTimestamp: metav1.NewTime(test.createdAt),
					Labels: map[string]string{
						acpExecutionWorkspaceLinkLabel: workspace.Name,
					},
					Annotations: map[string]string{
						acpExecutionWorkspaceUIDAnnotation: string(workspace.UID),
					},
				},
				Spec: corev1alpha1.TaskSpec{
					Type:       corev1alpha1.TaskTypeAgent,
					SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
				},
			}
			control := &corev1alpha1.RuntimeSessionControl{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: workspace.Namespace,
					Name:      storekube.RuntimeSessionControlObjectName(acpTestSessionName),
					UID:       types.UID("recovered-demand-control-uid-" + test.suffix),
				},
				Spec: corev1alpha1.RuntimeSessionControlSpec{
					SessionName: acpTestSessionName,
					SessionUID:  sessionUID,
				},
			}
			fence := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
				Namespace:         workspace.Namespace,
				Name:              acpWorkspaceRetentionFenceLeaseName(workspace),
				CreationTimestamp: metav1.NewTime(fenceTime),
				Labels: map[string]string{
					acpWorkspaceRetentionFenceIdentityLabel: labels.SelectorValue(workspace.Name),
				},
				Annotations: map[string]string{
					acpWorkspaceRetentionFenceNameAnnotation:       workspace.Name,
					acpWorkspaceRetentionFenceUIDAnnotation:        string(workspace.UID),
					acpWorkspaceRetentionFenceSessionUIDAnnotation: string(workspace.Spec.SessionRef.UID),
					acpWorkspaceRetentionFenceClassUIDAnnotation:   string(workspace.Spec.ClassBinding.UID),
					acpWorkspaceRetentionFenceActivatedAnnotation:  fenceTime.Format(time.RFC3339Nano),
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RuntimeSessionControl",
					Name: control.Name, UID: control.UID,
				}},
			}}
			c := acpAdapterTestClient(t, workspace, continuation, control, fence)
			reconciler := &ACPWorkspaceRetentionReconciler{
				Client: c, APIReader: c, Now: func() time.Time { return fenceTime },
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("retention reconcile: %v", err)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read recovered workspace: %v", err)
			}
			if got := !current.DeletionTimestamp.IsZero(); got != test.wantDeleting {
				t.Fatalf("workspace deleting = %v, want %v", got, test.wantDeleting)
			}
			err := c.Get(ctx, client.ObjectKeyFromObject(fence), &coordinationv1.Lease{})
			if got := apierrors.IsNotFound(err); got != test.wantFenceGone {
				t.Fatalf("retention fence absent = %v, want %v, err=%v", got, test.wantFenceGone, err)
			}
		})
	}
}

// A malformed controller-written last-detached-at stamp means the
// admission-protected metadata was corrupted: idle retention must fail closed
// on the bounded maxLifetime path instead of treating the workspace as
// instantly idle-expired via the creation-time fallback.
func TestACPWorkspaceRetentionFailsClosedOnMalformedIdleStamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-badstamp", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = invalidTimestamp
	})
	c := acpAdapterTestClient(t, workspace)
	result := reconcileRetention(t, c, workspace)
	if result.RequeueAfter <= 0 {
		t.Fatalf("a malformed idle stamp must hold on a bounded requeue, got %+v", result)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("workspace must survive a malformed idle stamp: %v", err)
	}
	if !current.DeletionTimestamp.IsZero() {
		t.Fatal("a malformed idle stamp must never expire the workspace through the creation-time fallback")
	}
}

func TestACPWorkspaceRetentionFailsClosedOnEmptyIdleStamp(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		stamp string
	}{
		{name: emptyCaseName, stamp: ""},
		{name: whitespaceCaseName, stamp: whitespaceOnlyValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := retentionTestWorkspace(t, "acp-ws-retention-empty-stamp-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = test.stamp
			})
			c := acpAdapterTestClient(t, workspace)
			result := reconcileRetention(t, c, workspace)
			if result.RequeueAfter <= 0 {
				t.Fatalf("an empty idle stamp must hold on a bounded requeue, got %+v", result)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
				t.Fatalf("workspace must survive an empty idle stamp: %v", err)
			}
			if !current.DeletionTimestamp.IsZero() {
				t.Fatal("an empty idle stamp must never expire the workspace through the creation-time fallback")
			}
		})
	}
}

func TestACPWorkspaceRetentionBoundsCorruptIdleMetadataWithoutMaxLifetime(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*workspacev1alpha1.ExecutionWorkspace, time.Time)
	}{
		{
			name: "last detached stamp",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace, _ time.Time) {
				workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = invalidTimestamp
			},
		},
		{
			name: "revocation stamp",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace, now time.Time) {
				workspace.Spec.AttachmentEpoch = 3
				workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = "not-an-epoch-and-time"
				workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
			},
		},
		{
			name: "malformed pending demand stamp",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace, now time.Time) {
				workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				workspace.Annotations[acpWorkspaceResumeRequestedAnnotation] = invalidTimestamp
				workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
			},
		},
		{
			name: "empty pending demand stamp",
			mutate: func(workspace *workspacev1alpha1.ExecutionWorkspace, now time.Time) {
				workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
				workspace.Annotations[acpWorkspaceResumeRequestedAnnotation] = whitespaceOnlyValue
				workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, 8, 28, 3, 0, 0, 0, time.UTC)
			workspace := retentionTestWorkspace(t, "acp-ws-retention-corrupt-"+strings.ReplaceAll(test.name, " ", "-"), func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.Lifecycle.MaxLifetime = nil
				test.mutate(w, now)
			})
			c := acpAdapterTestClient(t, workspace)
			reconciler := &ACPWorkspaceRetentionReconciler{Client: c, Now: func() time.Time { return now }}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}

			result, err := reconciler.Reconcile(ctx, request)
			if err != nil {
				t.Fatalf("stamp fallback retention deadline: %v", err)
			}
			if result.RequeueAfter != time.Second {
				t.Fatalf("fallback migration requeue = %s, want 1s", result.RequeueAfter)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read bounded workspace: %v", err)
			}
			deadline := now.Add(acpWorkspaceLegacyRetentionGrace)
			if got := current.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; got != deadline.Format(time.RFC3339Nano) {
				t.Fatalf("fallback retention deadline = %q, want %q", got, deadline.Format(time.RFC3339Nano))
			}

			now = deadline
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatalf("expire corrupt workspace: %v", err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil || current.DeletionTimestamp.IsZero() {
				t.Fatalf("corrupt workspace must enter cleanup at its fallback deadline, got err=%v deleting=%v", err, current.DeletionTimestamp)
			}
		})
	}
}

func TestACPWorkspaceRetentionKeepsFreshWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-b", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	result := reconcileRetention(t, c, workspace)
	if result.RequeueAfter <= 0 {
		t.Fatalf("a fresh workspace must requeue for its future deadline, got %+v", result)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("fresh workspace must survive: %v", err)
	}
}

func TestACPWorkspaceRetentionMigratesLegacyDetachStamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-legacy-detach", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Spec.AttachmentEpoch = 3
		delete(w.Annotations, acpWorkspaceLastDetachedAnnotation)
	})
	c := acpAdapterTestClient(t, workspace)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}
	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("migrate legacy detach stamp: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("migration requeue = %s, want 1s", result.RequeueAfter)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("legacy suspended workspace must survive migration: %v", err)
	}
	if got := current.Annotations[acpWorkspaceLastDetachedAnnotation]; got != now.Format(time.RFC3339Nano) {
		t.Fatalf("migrated last-detached-at = %q, want %q", got, now.Format(time.RFC3339Nano))
	}
	if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("migration changed workspace lifecycle: deleting=%v desired=%s",
			current.DeletionTimestamp, current.Spec.DesiredState)
	}
	result, err = reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("reconcile migrated workspace: %v", err)
	}
	if result.RequeueAfter <= time.Minute {
		t.Fatalf("migrated workspace did not receive a fresh idle interval: %+v", result)
	}
}

func TestACPWorkspaceRetentionBoundsLegacyUnboundedSuspension(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-legacy-unbounded", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Spec.Lifecycle.IdleTimeout = nil
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{workspacev1alpha1.WorkspaceOnDetachSuspend}
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	})
	c := acpAdapterTestClient(t, workspace)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}

	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("stamp legacy retention deadline: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("migration requeue = %s, want 1s", result.RequeueAfter)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("legacy workspace must survive deadline migration: %v", err)
	}
	wantDeadline := now.Add(acpWorkspaceLegacyRetentionGrace)
	if got := current.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; got != wantDeadline.Format(time.RFC3339Nano) {
		t.Fatalf("legacy retention deadline = %q, want %q", got, wantDeadline.Format(time.RFC3339Nano))
	}

	now = wantDeadline.Add(-time.Minute)
	result, err = reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("reconcile before legacy retention deadline: %v", err)
	}
	if result.RequeueAfter != time.Minute+time.Second {
		t.Fatalf("pre-expiry requeue = %s, want %s", result.RequeueAfter, time.Minute+time.Second)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("legacy workspace must survive until its migration deadline: %v", err)
	}
	if !current.DeletionTimestamp.IsZero() {
		t.Fatalf("legacy workspace deleted before migration deadline: %v", current.DeletionTimestamp)
	}

	now = wantDeadline
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatalf("expire legacy workspace: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil || current.DeletionTimestamp.IsZero() {
		t.Fatalf("legacy workspace must enter finalizer-held deletion at its migration deadline, got err=%v deleting=%v", err, current.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionDoesNotBoundDeleteOnlyDurableProfile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-delete-only-durable", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.CreationTimestamp = metav1.NewTime(now.Add(-7 * 24 * time.Hour))
		w.Spec.Lifecycle.IdleTimeout = nil
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Status.AttachedEpoch = 1
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	})
	c := acpAdapterTestClient(t, workspace)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, Now: func() time.Time { return now }}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	if err != nil {
		t.Fatalf("reconcile Delete-only durable-profile workspace: %v", err)
	}
	if result.RequeueAfter != acpWorkspaceRetentionRequeue {
		t.Fatalf("requeue = %s, want %s", result.RequeueAfter, acpWorkspaceRetentionRequeue)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read Delete-only durable-profile workspace: %v", err)
	}
	if deadline, present := current.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; present {
		t.Fatalf("Delete-only workspace received fallback deadline %q", deadline)
	}
	if !current.DeletionTimestamp.IsZero() {
		t.Fatalf("Delete-only workspace entered deletion: %v", current.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionBoundsLegacyMalformedDurableMarkers(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: emptyCaseName, value: ""},
		{name: whitespaceCaseName, value: whitespaceOnlyValue},
		{name: testFalseValue, value: testFalseValue},
		{name: malformedCaseName, value: "not-a-boolean"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
			workspace := retentionTestWorkspace(t, "acp-ws-retention-legacy-marker-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.Lifecycle.IdleTimeout = nil
				w.Spec.Lifecycle.MaxLifetime = nil
				w.Annotations[acpWorkspaceDurableAnnotation] = test.value
			})
			c := acpAdapterTestClient(t, workspace)
			reconciler := &ACPWorkspaceRetentionReconciler{Client: c, Now: func() time.Time { return now }}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("stamp legacy retention deadline: %v", err)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("read legacy workspace: %v", err)
			}
			want := now.Add(acpWorkspaceLegacyRetentionGrace).Format(time.RFC3339Nano)
			if got := current.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; got != want {
				t.Fatalf("legacy retention deadline = %q, want %q", got, want)
			}
		})
	}
}

func TestACPWorkspaceRetentionEnforcesMaxLifetimeEvenWhileAttached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-c", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.CreationTimestamp = metav1.NewTime(time.Now().Add(-25 * time.Hour))
		w.Spec.AttachmentEpoch = 1
		w.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{
			TaskRef:        workspacev1alpha1.ObjectIdentityReference{Name: acpTestAttachedTask, UID: types.UID("task-uid")},
			Epoch:          1,
			TokenSHA256:    "sha256:" + strings.Repeat("d", 64),
			TokenSecretRef: workspacev1alpha1.SecretReference{Name: "attach"},
			ExpiresAt:      metav1.Now(),
		}
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("maxLifetime must force terminal cleanup even while attached, got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionEnforcesMaxLifetimeBeforeQuotaLeaseRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-expired-invalid-lease", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.CreationTimestamp = metav1.NewTime(now.Add(-25 * time.Hour))
		w.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
	})
	c := acpAdapterTestClient(t, workspace)
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	lease, err := newACPSuspendQuotaLease(workspace, acpSuspendQuotaClaims{})
	if err != nil {
		t.Fatalf("build quota Lease: %v", err)
	}
	lease.Annotations[acpSuspendQuotaLeaseClassUIDAnnotation] = wrongClassUIDValue
	if err := c.Create(ctx, lease); err != nil {
		t.Fatalf("create malformed quota Lease: %v", err)
	}
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, APIReader: c, Now: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
		t.Fatalf("expire workspace before malformed Lease recovery: %v", err)
	}
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("expired workspace must be deleting despite malformed quota Lease, got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionEnforcesIdleExpiryBeforeQuotaLeaseRecovery(t *testing.T) {
	t.Parallel()
	for _, desiredState := range []workspacev1alpha1.ExecutionWorkspaceDesiredState{
		workspacev1alpha1.ExecutionWorkspaceDesiredSuspended,
		workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined,
	} {
		t.Run(string(desiredState), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC)
			workspace := retentionTestWorkspace(t, "acp-ws-retention-idle-invalid-lease-"+strings.ToLower(string(desiredState)), func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
				w.Spec.DesiredState = desiredState
				w.Spec.Lifecycle.MaxLifetime = nil
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
				w.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
			})
			c := acpAdapterTestClient(t, workspace)
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), workspace); err != nil {
				t.Fatalf("read workspace: %v", err)
			}
			lease, err := newACPSuspendQuotaLease(workspace, acpSuspendQuotaClaims{})
			if err != nil {
				t.Fatalf("build quota Lease: %v", err)
			}
			lease.Annotations[acpSuspendQuotaLeaseClassUIDAnnotation] = wrongClassUIDValue
			if err := c.Create(ctx, lease); err != nil {
				t.Fatalf("create malformed quota Lease: %v", err)
			}
			reconciler := &ACPWorkspaceRetentionReconciler{Client: c, APIReader: c, Now: func() time.Time { return now }}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}); err != nil {
				t.Fatalf("expire idle workspace before malformed Lease recovery: %v", err)
			}
			deleting := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
				t.Fatalf("idle-expired workspace must be deleting despite malformed quota Lease, got err=%v deleting=%v", err, deleting.DeletionTimestamp)
			}
		})
	}
}

func TestACPWorkspaceRetentionSuspendsIdleReadyWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-d", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("idle suspendable workspace must survive as suspended: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state = %s, want Suspended", current.Spec.DesiredState)
	}
}

func TestACPWorkspaceRetentionHonorsDefaultSuspendWithoutActionStamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-default-suspend", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("default-Suspend workspace without an action stamp must survive: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state = %s, want the frozen default Suspend action honored", current.Spec.DesiredState)
	}
}

func TestACPWorkspaceRetentionFailsClosedOnMalformedDurableCapability(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", whitespaceOnlyValue, testFalseValue} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := retentionTestWorkspace(t, "acp-ws-retention-invalid-durable", func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
				w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
					workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
				}
				delete(w.Annotations, acpWorkspaceDetachActionAnnotation)
				w.Annotations[acpWorkspaceDurableAnnotation] = value
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			})
			c := acpAdapterTestClient(t, workspace)
			result := reconcileRetention(t, c, workspace)
			if result.RequeueAfter <= 0 {
				t.Fatalf("an invalid durable marker must hold on a bounded requeue, got %+v", result)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("workspace must survive an invalid durable marker: %v", err)
			}
			if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
				t.Fatalf("invalid durable marker changed the workspace: deleting=%v desired=%s",
					current.DeletionTimestamp, current.Spec.DesiredState)
			}
		})
	}
}

func TestACPWorkspaceRetentionUsesClassDefaultAfterTaskOverride(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-class-default", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachDelete
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("idle retention must apply the class Delete default after a Task Suspend override, got err=%v deleting=%v",
			err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionDeletesIdleFailedReadyWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-failed-ready", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceResumedLineageAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("idle terminal failure must enter deletion without another suspension interval, got err=%v deleting=%v",
			err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionFailsClosedOnEmptyDetachAction(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		action string
	}{
		{name: emptyCaseName, action: ""},
		{name: whitespaceCaseName, action: whitespaceOnlyValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := retentionTestWorkspace(t, "acp-ws-retention-empty-action-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Annotations[acpWorkspaceDetachActionAnnotation] = test.action
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			})
			c := acpAdapterTestClient(t, workspace)
			result := reconcileRetention(t, c, workspace)
			if result.RequeueAfter <= 0 {
				t.Fatalf("an invalid frozen detach action must hold on a bounded requeue, got %+v", result)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("workspace must survive an invalid frozen detach action: %v", err)
			}
			if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
				t.Fatalf("invalid frozen detach action changed the workspace: deleting=%v desired=%s",
					current.DeletionTimestamp, current.Spec.DesiredState)
			}
		})
	}
}

func TestACPWorkspaceRetentionDeletesIdleReadyDeleteClasses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-e", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("idle Delete-class workspace must be deleting (finalizer-held), got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionIgnoresForeignWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-f", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Labels = nil
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("a foreign workspace must never be retention-managed: %v", err)
	}
}

func TestACPWorkspaceRetentionWaitsForEnforcedEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Revocation cleared the attachment but the adapter still enforces the
	// epoch, and no detach instant is recorded: idle evaluation must wait
	// instead of falling back to the creation timestamp and expiring a
	// workspace whose Task simply ran longer than idleTimeout.
	workspace := retentionTestWorkspace(t, "acp-ws-retention-g", func(w *workspacev1alpha1.ExecutionWorkspace) {
		// The high-water mark alone never defers idle handling; the
		// adapter-enforced epoch does.
		w.Spec.AttachmentEpoch = 3
		w.Status.AttachedEpoch = 3
	})
	c := acpAdapterTestClient(t, workspace)
	result := reconcileRetention(t, c, workspace)
	if result.RequeueAfter <= 0 {
		t.Fatalf("an enforced epoch must requeue idle evaluation, got %+v", result)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("workspace must survive while the epoch settles: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("desired state = %s, want it untouched while the epoch settles", current.Spec.DesiredState)
	}
}

func TestACPWorkspaceRetentionStartsIdleClockAfterEnforcedEpochClears(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	revocationStartedAt := time.Now().Add(-time.Hour).UTC()
	detachedAt := revocationStartedAt.Format(time.RFC3339Nano)
	now := time.Now().UTC()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-released-epoch", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.AttachmentEpoch = 3
		w.Status.AttachedEpoch = 0
		w.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf("3 %s", detachedAt)
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = detachedAt
	})
	c := acpAdapterTestClient(t, workspace)
	reconciler := &ACPWorkspaceRetentionReconciler{
		Client: c,
		Now:    func() time.Time { return now },
	}
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(workspace),
	})
	if err != nil {
		t.Fatalf("retention reconcile: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("released epoch requeue = %s, want 1s", result.RequeueAfter)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("workspace must survive epoch-release handoff: %v", err)
	}
	if !current.DeletionTimestamp.IsZero() {
		t.Fatal("workspace must not expire from the provisional revocation-start clock")
	}
	if got, want := current.Annotations[acpWorkspaceLastDetachedAnnotation], now.Format(time.RFC3339Nano); got != want {
		t.Fatalf("last-detached-at = %q, want epoch release instant %q", got, want)
	}
	if got := current.Annotations[acpWorkspaceRevocationStartedAnnotation]; got != fmt.Sprintf("3 %s", detachedAt) {
		t.Fatalf("revocation marker = %q, want it retained for Task settlement", got)
	}
}

func TestACPWorkspaceRetentionFailsClosedOnMalformedRevocationStamp(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		stamp string
	}{
		{name: emptyCaseName, stamp: ""},
		{name: "malformed", stamp: "not-an-epoch-and-time"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			detachedAt := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			workspace := retentionTestWorkspace(t, "acp-ws-retention-bad-revocation-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.AttachmentEpoch = 3
				w.Annotations[acpWorkspaceRevocationStartedAnnotation] = test.stamp
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = detachedAt
			})
			c := acpAdapterTestClient(t, workspace)
			result := reconcileRetention(t, c, workspace)
			if result.RequeueAfter <= 0 {
				t.Fatalf("malformed revocation stamp must hold on a bounded requeue, got %+v", result)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("workspace must survive malformed revocation metadata: %v", err)
			}
			if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
				t.Fatalf("malformed revocation metadata changed workspace: deleting=%v desired=%s",
					current.DeletionTimestamp, current.Spec.DesiredState)
			}
			if got := current.Annotations[acpWorkspaceLastDetachedAnnotation]; got != detachedAt {
				t.Fatalf("last-detached-at = %q, want protected stamp %q unchanged", got, detachedAt)
			}
		})
	}
}

func TestACPWorkspaceRetentionIdleSuspensionHonorsQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	suspendable := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		return retentionTestWorkspace(t, name, func(w *workspacev1alpha1.ExecutionWorkspace) {
			w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
			w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
				workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
			}
			w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
			w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
			w.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
			w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		})
	}
	idle := suspendable("acp-ws-retention-h")
	occupant := suspendable("acp-ws-retention-i")
	occupant.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	occupant.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	c := acpAdapterTestClient(t, idle, occupant)
	result := reconcileRetention(t, c, idle)
	if result.RequeueAfter != acpWorkspaceRetentionRequeue {
		t.Fatalf("idle suspension over an exhausted cap requeue = %s, want %s",
			result.RequeueAfter, acpWorkspaceRetentionRequeue)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: idle.Namespace, Name: idle.Name}, current); err != nil {
		t.Fatalf("idle workspace must survive quota exhaustion: %v", err)
	}
	if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("quota exhaustion changed the frozen Suspend policy: deleting=%v desired=%s",
			current.DeletionTimestamp, current.Spec.DesiredState)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: occupant.Namespace, Name: occupant.Name}, &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("the quota occupant must survive: %v", err)
	}
	occupant.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	if err := c.Update(ctx, occupant); err != nil {
		t.Fatalf("free the quota slot: %v", err)
	}
	reconcileRetention(t, c, idle)
	if err := c.Get(ctx, types.NamespacedName{Namespace: idle.Namespace, Name: idle.Name}, current); err != nil {
		t.Fatalf("read workspace after quota frees: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state after quota frees = %s, want Suspended", current.Spec.DesiredState)
	}
}

func TestACPWorkspaceRetentionBoundsLegacyQuotaWaitWithoutMaxLifetime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 28, 4, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-legacy-quota-wait", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend,
			workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		w.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "0"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: c, APIReader: c, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(workspace)}

	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("stamp quota fallback deadline: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("quota fallback migration requeue = %s, want 1s", result.RequeueAfter)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read quota-blocked workspace: %v", err)
	}
	deadline := now.Add(acpWorkspaceLegacyRetentionGrace)
	if got := current.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; got != deadline.Format(time.RFC3339Nano) {
		t.Fatalf("quota fallback deadline = %q, want %q", got, deadline.Format(time.RFC3339Nano))
	}

	now = deadline.Add(-time.Minute)
	result, err = reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatalf("reconcile quota wait before fallback deadline: %v", err)
	}
	if result.RequeueAfter != time.Minute+time.Second {
		t.Fatalf("pre-deadline quota wait requeue = %s, want %s", result.RequeueAfter, time.Minute+time.Second)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil || !current.DeletionTimestamp.IsZero() {
		t.Fatalf("quota-blocked workspace must survive until the fallback deadline, got err=%v deleting=%v", err, current.DeletionTimestamp)
	}

	now = deadline
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatalf("expire quota-blocked workspace: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil || current.DeletionTimestamp.IsZero() {
		t.Fatalf("quota-blocked workspace must enter cleanup at the fallback deadline, got err=%v deleting=%v", err, current.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionSuspendStampsFreshDetachInstant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-j", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read suspended workspace: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state = %s, want Suspended", current.Spec.DesiredState)
	}
	stamp, err := time.Parse(time.RFC3339Nano, current.Annotations[acpWorkspaceLastDetachedAnnotation])
	if err != nil {
		t.Fatalf("parse refreshed detach instant: %v", err)
	}
	if time.Since(stamp) > time.Minute {
		t.Fatalf("idle suspension must stamp a fresh detach instant so the suspended state earns a full retention interval, got %s", stamp)
	}
	// The freshly suspended workspace is not immediately expired by the next
	// retention pass.
	if result := reconcileRetention(t, c, current); result.RequeueAfter <= 0 {
		t.Fatalf("freshly suspended workspace must requeue, got %+v", result)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("freshly suspended workspace must survive the next pass: %v", err)
	}
}

func TestACPWorkspaceRetentionPreservesDetachInstantForDeadColdResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	detachedAt := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	workspace := retentionTestWorkspace(t, "acp-ws-retention-dead-resume-clock", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		w.Annotations[acpWorkspaceResumedLineageAnnotation] = booleanTrueValue
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano) +
			" vanished-continuation vanished-continuation-uid"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = detachedAt
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read re-suspended workspace: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state = %s, want Suspended", current.Spec.DesiredState)
	}
	if got := current.Annotations[acpWorkspaceLastDetachedAnnotation]; got != detachedAt {
		t.Fatalf("dead cold resume changed last-detached-at to %q, want %q", got, detachedAt)
	}
}

func TestACPSuspendQuotaLockRetiresIdleEntry(t *testing.T) {
	t.Parallel()
	namespace := "quota-lock-retire"
	classUID := types.UID("quota-lock-retire-uid")
	key := namespace + "/" + string(classUID)

	unlock := lockACPSuspendQuota(namespace, classUID)
	acpSuspendQuotaLocksMu.Lock()
	entry, presentWhileHeld := acpSuspendQuotaLocks[key]
	referencesWhileHeld := 0
	if entry != nil {
		referencesWhileHeld = entry.references
	}
	acpSuspendQuotaLocksMu.Unlock()
	unlock()

	if !presentWhileHeld || referencesWhileHeld != 1 {
		t.Fatalf("held lock entry = (present=%v references=%d), want one live reference", presentWhileHeld, referencesWhileHeld)
	}
	acpSuspendQuotaLocksMu.Lock()
	_, presentAfterRelease := acpSuspendQuotaLocks[key]
	acpSuspendQuotaLocksMu.Unlock()
	if presentAfterRelease {
		t.Fatal("idle quota lock entry survived its final release")
	}
}

func TestACPSuspendQuotaLeaseSerializesClaimsAcrossLeaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("cross-leader-quota-class-uid")
	shape := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		return retentionTestWorkspace(t, name, func(workspace *workspacev1alpha1.ExecutionWorkspace) {
			workspace.Spec.ClassBinding.UID = classUID
			workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
		})
	}
	first := shape("acp-ws-quota-first")
	second := shape("acp-ws-quota-second")
	c := acpAdapterTestClient(t, first, second)
	if err := c.Get(ctx, client.ObjectKeyFromObject(first), first); err != nil {
		t.Fatalf("read first claimant: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), second); err != nil {
		t.Fatalf("read second claimant: %v", err)
	}

	if err := claimACPSuspendQuotaSlot(ctx, c, c, first, 1); err != nil {
		t.Fatalf("record first pending claim: %v", err)
	}
	if err := suspendACPWorkspaceWithinQuota(ctx, c, c, second, time.Now(), false, "", 0); !errors.Is(err, errACPSuspendQuotaExhausted) {
		t.Fatalf("second claim while the first is pending = %v, want quota exhaustion", err)
	}
	currentSecond := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), currentSecond); err != nil {
		t.Fatalf("read blocked claimant: %v", err)
	}
	if currentSecond.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("blocked claimant desired state = %s, want Ready", currentSecond.Spec.DesiredState)
	}

	if err := suspendACPWorkspaceWithinQuota(ctx, c, c, first, time.Now(), false, "", 0); err != nil {
		t.Fatalf("recover first claim: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), currentSecond); err != nil {
		t.Fatalf("refresh second claimant: %v", err)
	}
	if err := suspendACPWorkspaceWithinQuota(ctx, c, c, currentSecond, time.Now(), false, "", 0); !errors.Is(err, errACPSuspendQuotaExhausted) {
		t.Fatalf("second claim after the first committed = %v, want quota exhaustion", err)
	}
	lease := &coordinationv1.Lease{}
	err := c.Get(ctx, types.NamespacedName{
		Namespace: first.Namespace,
		Name:      acpSuspendQuotaLeaseName(classUID),
	}, lease)
	if err != nil {
		t.Fatalf("read persistent quota Lease: %v", err)
	}
	claims, err := readACPSuspendQuotaClaims(lease, first.Spec.ClassBinding.Name, classUID)
	if err != nil {
		t.Fatalf("read persistent quota claims: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("persistent claims = %#v, want settled occupancy counted only from live workspaces", claims)
	}
}

func TestACPSuspendQuotaLeaseReplacesMalformedReservedLease(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*coordinationv1.Lease)
	}{
		{
			name: "invalid class identity",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Annotations[acpSuspendQuotaLeaseClassUIDAnnotation] = wrongClassUIDValue
			},
		},
		{
			name: "invalid ownership",
			mutate: func(lease *coordinationv1.Lease) {
				lease.OwnerReferences[0].UID = types.UID("wrong-class-owner-uid")
			},
		},
		{
			name: "invalid claims JSON",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Annotations[acpSuspendQuotaLeaseClaimsAnnotation] = "{"
			},
		},
		{
			name: "invalid claim entry",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Annotations[acpSuspendQuotaLeaseClaimsAnnotation] = `{"workspace-uid":{"workspaceName":"","resourceVersion":"1"}}`
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			suffix := strings.ReplaceAll(test.name, " ", "-")
			classUID := types.UID("malformed-quota-class-uid-" + suffix)
			workspace := retentionTestWorkspace(t, "acp-ws-malformed-quota-"+suffix, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.ClassBinding.UID = classUID
				w.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
			})
			baseClient := acpAdapterTestClient(t, workspace)
			if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), workspace); err != nil {
				t.Fatalf("read quota claimant: %v", err)
			}
			lease, err := newACPSuspendQuotaLease(workspace, acpSuspendQuotaClaims{})
			if err != nil {
				t.Fatalf("build malformed quota Lease: %v", err)
			}
			lease.UID = types.UID("malformed-quota-lease-uid-" + suffix)
			test.mutate(lease)
			if err := baseClient.Create(ctx, lease); err != nil {
				t.Fatalf("create malformed quota Lease: %v", err)
			}
			if lease.ResourceVersion == "" {
				t.Fatal("test quota Lease has no resourceVersion fence")
			}

			deleteUsedExactPreconditions := false
			intercepted := interceptor.NewClient(baseClient, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, object client.Object, options ...client.DeleteOption) error {
					quotaLease, ok := object.(*coordinationv1.Lease)
					if ok && quotaLease.Name == lease.Name {
						deleteOptions := (&client.DeleteOptions{}).ApplyOptions(options)
						deleteUsedExactPreconditions = deleteOptions.Preconditions != nil &&
							deleteOptions.Preconditions.UID != nil && *deleteOptions.Preconditions.UID == lease.UID &&
							deleteOptions.Preconditions.ResourceVersion != nil &&
							*deleteOptions.Preconditions.ResourceVersion == lease.ResourceVersion
					}
					return c.Delete(ctx, object, options...)
				},
			})
			if err := claimACPSuspendQuotaSlot(ctx, intercepted, intercepted, workspace, 1); err != nil {
				t.Fatalf("replace malformed quota Lease and claim slot: %v", err)
			}
			if !deleteUsedExactPreconditions {
				t.Fatal("malformed quota Lease delete did not carry exact UID and resourceVersion preconditions")
			}
			replacement := &coordinationv1.Lease{}
			if err := baseClient.Get(ctx, types.NamespacedName{
				Namespace: workspace.Namespace,
				Name:      acpSuspendQuotaLeaseName(classUID),
			}, replacement); err != nil {
				t.Fatalf("read replacement quota Lease: %v", err)
			}
			claims, err := readACPSuspendQuotaClaims(replacement, workspace.Spec.ClassBinding.Name, classUID)
			if err != nil {
				t.Fatalf("replacement quota Lease is invalid: %v", err)
			}
			claim, ok := claims[string(workspace.UID)]
			if !ok || claim.WorkspaceName != workspace.Name || claim.ResourceVersion != workspace.ResourceVersion {
				t.Fatalf("replacement quota claim = %#v, want current workspace fence", claims)
			}
		})
	}
}

func TestACPSuspendQuotaLeaseStoresOnlyOnePendingClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("single-pending-quota-class-uid")
	shape := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		return retentionTestWorkspace(t, name, func(workspace *workspacev1alpha1.ExecutionWorkspace) {
			workspace.Spec.ClassBinding.UID = classUID
			workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "2"
		})
	}
	first := shape("acp-ws-pending-first")
	second := shape("acp-ws-pending-second")
	c := acpAdapterTestClient(t, first, second)
	if err := c.Get(ctx, client.ObjectKeyFromObject(first), first); err != nil {
		t.Fatalf("read first claimant: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), second); err != nil {
		t.Fatalf("read second claimant: %v", err)
	}
	if err := claimACPSuspendQuotaSlot(ctx, c, c, first, 2); err != nil {
		t.Fatalf("record first pending claim: %v", err)
	}
	if err := claimACPSuspendQuotaSlot(ctx, c, c, second, 2); !errors.Is(err, errACPSuspendQuotaBusy) {
		t.Fatalf("second pending claim with quota headroom = %v, want serialized retry", err)
	}
	lease := &coordinationv1.Lease{}
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: first.Namespace,
		Name:      acpSuspendQuotaLeaseName(classUID),
	}, lease); err != nil {
		t.Fatalf("read quota Lease: %v", err)
	}
	claims, err := readACPSuspendQuotaClaims(lease, first.Spec.ClassBinding.Name, classUID)
	if err != nil {
		t.Fatalf("read quota claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("pending claims = %#v, want one constant-size claim", claims)
	}
	if _, ok := claims[string(first.UID)]; !ok {
		t.Fatalf("pending claims = %#v, want only the first transaction", claims)
	}
}

func TestACPSuspendQuotaLeasePrunesFencedPendingClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("stale-quota-class-uid")
	shape := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		return retentionTestWorkspace(t, name, func(workspace *workspacev1alpha1.ExecutionWorkspace) {
			workspace.Spec.ClassBinding.UID = classUID
			workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
		})
	}
	stale := shape("acp-ws-quota-stale")
	replacement := shape("acp-ws-quota-replacement")
	c := acpAdapterTestClient(t, stale, replacement)
	if err := c.Get(ctx, client.ObjectKeyFromObject(stale), stale); err != nil {
		t.Fatalf("read stale claimant: %v", err)
	}
	if err := claimACPSuspendQuotaSlot(ctx, c, c, stale, 1); err != nil {
		t.Fatalf("record stale pending claim: %v", err)
	}
	base := stale.DeepCopy()
	stale.Annotations["test.orka.ai/fence"] = advancedFenceValue
	if err := c.Patch(ctx, stale, client.MergeFrom(base)); err != nil {
		t.Fatalf("advance stale claimant resourceVersion: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replacement), replacement); err != nil {
		t.Fatalf("read replacement claimant: %v", err)
	}
	if err := suspendACPWorkspaceWithinQuota(ctx, c, c, replacement, time.Now(), false, "", 0); err != nil {
		t.Fatalf("claim after the old workspace fence advanced: %v", err)
	}
	lease := &coordinationv1.Lease{}
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: replacement.Namespace,
		Name:      acpSuspendQuotaLeaseName(classUID),
	}, lease); err != nil {
		t.Fatalf("read quota Lease: %v", err)
	}
	claims, err := readACPSuspendQuotaClaims(lease, replacement.Spec.ClassBinding.Name, classUID)
	if err != nil {
		t.Fatalf("read quota claims: %v", err)
	}
	if _, ok := claims[string(stale.UID)]; ok {
		t.Fatalf("stale pending claim survived its workspace fence: %#v", claims)
	}
	if _, ok := claims[string(replacement.UID)]; !ok {
		t.Fatalf("replacement claim missing after stale-claim recovery: %#v", claims)
	}
}

func TestReleaseObsoleteACPSuspendQuotaLeaseUsesAuthoritativeWorkspace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("authoritative-release-class-uid")
	workspace := retentionTestWorkspace(t, "acp-ws-authoritative-release", func(workspace *workspacev1alpha1.ExecutionWorkspace) {
		workspace.Spec.ClassBinding.UID = classUID
		workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
	})
	c := acpAdapterTestClient(t, workspace)
	stale := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), stale); err != nil {
		t.Fatalf("read cached workspace snapshot: %v", err)
	}
	advanced := stale.DeepCopy()
	base := advanced.DeepCopy()
	advanced.Annotations["test.orka.ai/quota-fence"] = advancedFenceValue
	if err := c.Patch(ctx, advanced, client.MergeFrom(base)); err != nil {
		t.Fatalf("advance authoritative workspace resourceVersion: %v", err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read authoritative workspace: %v", err)
	}
	if current.ResourceVersion == stale.ResourceVersion {
		t.Fatal("test setup did not produce a stale workspace resourceVersion")
	}
	if err := claimACPSuspendQuotaSlot(ctx, c, c, current, 1); err != nil {
		t.Fatalf("record in-flight quota claim: %v", err)
	}
	if err := releaseObsoleteACPSuspendQuotaLease(ctx, c, c, stale); err != nil {
		t.Fatalf("release quota claim from stale cache snapshot: %v", err)
	}
	lease := &coordinationv1.Lease{}
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      acpSuspendQuotaLeaseName(classUID),
	}, lease); err != nil {
		t.Fatalf("read quota Lease: %v", err)
	}
	claims, err := readACPSuspendQuotaClaims(lease, workspace.Spec.ClassBinding.Name, classUID)
	if err != nil {
		t.Fatalf("read quota claims: %v", err)
	}
	claim, ok := claims[string(workspace.UID)]
	if !ok || claim.ResourceVersion != current.ResourceVersion {
		t.Fatalf("authoritative in-flight claim was released from stale cache state: %#v", claims)
	}
}

func TestSettleACPClassWorkspacePreservesDetachClockBeforeAttachment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-pre-attachment-suspend"
	workspace.UID = types.UID("acp-ws-pre-attachment-suspend-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID(suspendTestSessionUID),
	}
	workspace.Spec.Lifecycle.AllowedOnDetach = append(
		workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	detachedAt := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
	workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = detachedAt
	task := retentionSettlementTask(
		"pre-attachment-suspend-task",
		"pre-attachment-suspend-task-uid",
		workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if !taskNeverHeldACPWorkspaceAttachment(task) {
		t.Fatal("fixture unexpectedly records a workspace attachment")
	}

	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || !done {
		t.Fatalf("pre-attachment settlement = (%v, %v), want a completed suspension", done, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read suspended workspace: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state = %s, want Suspended", current.Spec.DesiredState)
	}
	if got := current.Annotations[acpWorkspaceLastDetachedAnnotation]; got != detachedAt {
		t.Fatalf("pre-attachment settlement changed last-detached-at to %q, want %q", got, detachedAt)
	}
}

func TestACPWorkspaceRetentionDeletesUncommittedDefaultSuspendWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// A Task materialized the workspace and was cancelled before attachment:
	// without the synchronous durable-session commit stamp, retention must
	// delete the empty incarnation instead of making it resumable.
	workspace := retentionTestWorkspace(t, "acp-ws-retention-k", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
			workspacev1alpha1.WorkspaceOnDetachSuspend, workspacev1alpha1.WorkspaceOnDetachDelete,
		}
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil || current.DeletionTimestamp.IsZero() {
		t.Fatalf("uncommitted suspendable workspace must be deleting, got err=%v deleting=%v", err, current.DeletionTimestamp)
	}
}

func TestExpireWorkspaceSkipsEventWhenDeleteAlreadyCompleted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-retention-delete-race")
	baseClient := acpAdapterTestClient(t, workspace)
	deleteCalled := false
	intercepted := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
			deleteCalled = true
			return apierrors.NewNotFound(
				schema.GroupResource{Group: workspacev1alpha1.GroupVersion.Group, Resource: "executionworkspaces"},
				workspace.Name,
			)
		},
	})
	recorder := k8sevents.NewFakeRecorder(1)
	reconciler := &ACPWorkspaceRetentionReconciler{Client: intercepted, Recorder: recorder}
	if err := reconciler.expireWorkspace(ctx, workspace, "IdleExpired", "expired", false); err != nil {
		t.Fatalf("expire workspace after concurrent deletion: %v", err)
	}
	if !deleteCalled {
		t.Fatal("retention did not attempt deletion")
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("concurrent deletion emitted a retention Event: %q", event)
	default:
	}
}

// A live cold-resume requester is active demand even while the observed state
// remains Suspended; idle retention must wait for that exact Task.
func TestACPWorkspaceRetentionWaitsForColdResumeInFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	requester := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: "cold-resume-requester", UID: types.UID("cold-resume-requester-uid"),
	}}
	workspace := retentionTestWorkspace(t, "acp-ws-retention-l", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
		w.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().UTC().Format(time.RFC3339Nano) +
			" cold-resume-requester cold-resume-requester-uid"
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
	})
	c := acpAdapterTestClient(t, workspace, requester)
	result := reconcileRetention(t, c, workspace)
	if result.RequeueAfter <= 0 {
		t.Fatalf("resume in flight must requeue, got %+v", result)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("workspace must survive a resume in flight: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("desired state = %s, want the resume request untouched", current.Spec.DesiredState)
	}
}

// A dead cold-resume requester must not turn stale observed suspension into
// permanent demand. With no maxLifetime, idleTimeout still reclaims the
// workspace from either provider transition state.
func TestACPWorkspaceRetentionExpiresDeadColdResumeState(t *testing.T) {
	t.Parallel()
	for _, state := range []workspacev1alpha1.ExecutionWorkspaceState{
		workspacev1alpha1.ExecutionWorkspaceStateSuspended,
		workspacev1alpha1.ExecutionWorkspaceStateSuspending,
	} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := retentionTestWorkspace(t, "acp-ws-dead-resume-"+strings.ToLower(string(state)), func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Spec.Lifecycle.MaxLifetime = nil
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
				w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().UTC().Format(time.RFC3339Nano) +
					" vanished-resume-requester vanished-resume-requester-uid"
				w.Status.State = state
			})
			c := acpAdapterTestClient(t, workspace)
			reconcileRetention(t, c, workspace)
			deleting := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
				t.Fatalf("dead resume in %s must idle out, got err=%v deleting=%v", state, err, deleting.DeletionTimestamp)
			}
		})
	}
}

// A terminally failed suspension preserves no data and must not hold a quota
// slot. Quarantine expires at the earliest configured idle or lifetime bound.
func TestACPWorkspaceRetentionFailedAndQuarantinedHandling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	failed := retentionTestWorkspace(t, "acp-ws-retention-m", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	c := acpAdapterTestClient(t, failed)
	count, err := countSuspendedClassWorkspaces(ctx, c, failed.Namespace, failed.Spec.ClassBinding.UID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want a failed suspension excluded from the quota", count)
	}

	quarantined := retentionTestWorkspace(t, "acp-ws-retention-n", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		w.CreationTimestamp = metav1.NewTime(time.Now().Add(-25 * time.Hour))
	})
	qc := acpAdapterTestClient(t, quarantined)
	reconcileRetention(t, qc, quarantined)
	deletingQuarantined := &workspacev1alpha1.ExecutionWorkspace{}
	if getErr := qc.Get(ctx, types.NamespacedName{Namespace: quarantined.Namespace, Name: quarantined.Name}, deletingQuarantined); getErr != nil || deletingQuarantined.DeletionTimestamp.IsZero() {
		t.Fatalf("quarantined workspace past maxLifetime must be deleting, got err=%v deleting=%v", getErr, deletingQuarantined.DeletionTimestamp)
	}

	// A recent quarantine survives until its idle deadline.
	fresh := retentionTestWorkspace(t, "acp-ws-retention-o", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	})
	fc := acpAdapterTestClient(t, fresh)
	reconcileRetention(t, fc, fresh)
	if err := fc.Get(ctx, types.NamespacedName{Namespace: fresh.Namespace, Name: fresh.Name}, &workspacev1alpha1.ExecutionWorkspace{}); err != nil {
		t.Fatalf("quarantined workspace inside idleTimeout must survive: %v", err)
	}

	idleOnly := retentionTestWorkspace(t, "acp-ws-retention-idle-only-quarantine", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	ic := acpAdapterTestClient(t, idleOnly)
	reconcileRetention(t, ic, idleOnly)
	deletingIdleOnly := &workspacev1alpha1.ExecutionWorkspace{}
	if err := ic.Get(ctx, client.ObjectKeyFromObject(idleOnly), deletingIdleOnly); err != nil || deletingIdleOnly.DeletionTimestamp.IsZero() {
		t.Fatalf("idle-only quarantine must enter terminal cleanup, got err=%v deleting=%v", err, deletingIdleOnly.DeletionTimestamp)
	}
}

// quotaSessionControlStore serves exactly the GetSessionControl lookup the
// suspend-quota exclusion performs; sessions maps name -> immutable UID.
type quotaSessionControlStore struct {
	store.DurableControlStore
	namespace string
	sessions  map[string]string
}

func (s *quotaSessionControlStore) GetSessionControl(_ context.Context, namespace, name string) (*store.SessionControl, error) {
	uid, ok := s.sessions[name]
	if !ok || namespace != s.namespace {
		return nil, store.ErrNotFound
	}
	return &store.SessionControl{
		Namespace: namespace, SessionName: name, SessionUID: uid,
		Availability: store.SessionAvailable,
	}, nil
}

type quotaSessionTranscriptStore struct {
	store.SessionStore
	namespace string
	sessions  map[string]string
}

func (s *quotaSessionTranscriptStore) GetSession(_ context.Context, namespace, name string) (*store.SessionRecord, error) {
	if _, ok := s.sessions[name]; !ok || namespace != s.namespace {
		return nil, store.ErrNotFound
	}
	return &store.SessionRecord{Namespace: namespace, Name: name, SessionType: defaultACPSessionType}, nil
}

// attachQuotaSessionStores wires minimal durable-session fakes so class
// resolution can resolve immutable Session UIDs for quota exclusions.
func attachQuotaSessionStores(r *TaskReconciler, sessions map[string]string) {
	r.DurableControlStore = &quotaSessionControlStore{namespace: acpTestNamespace, sessions: sessions}
	r.SessionManager = &SessionManager{store: &quotaSessionTranscriptStore{namespace: acpTestNamespace, sessions: sessions}}
	r.ControllerEpochManager = &ControllerEpochManager{}
}

func TestResolveACPWorkspaceClassRejectsSuspendableClassWithoutExpiry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		withCap bool
	}{
		{name: "no bounds"},
		{name: "quota only", withCap: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fixture := suspendableSubstrateFixture(t)
			fixture.class.Spec.Lifecycle.IdleTimeout = nil
			fixture.class.Spec.Lifecycle.MaxLifetime = nil
			fixture.profile.Spec.Retention = nil
			if test.withCap {
				limit := int32(1)
				fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
			}
			fixture.pinProfileHash(t)
			task := suspendableSessionTask()
			r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
			if _, err := r.resolveACPWorkspaceClass(ctx, task); err == nil ||
				!strings.Contains(err.Error(), "requires an expiry bound") {
				t.Fatalf("suspend-capable class without expiry error = %v, want an expiry-bound rejection", err)
			}
		})
	}
}

func TestResolveACPWorkspaceClassRejectsSuspendQuotaWithoutMaxLifetime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	fixture.class.Spec.Lifecycle.IdleTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	fixture.class.Spec.Lifecycle.MaxLifetime = nil
	limit := int32(1)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	fixture.pinProfileHash(t)
	task := suspendableSessionTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	if _, err := r.resolveACPWorkspaceClass(ctx, task); err == nil ||
		!strings.Contains(err.Error(), "requires maxLifetime") {
		t.Fatalf("quota-capped class without maxLifetime error = %v, want maxLifetime rejection", err)
	}
}

func TestACPWorkspaceSuspendQuotaMessageOmitsForbiddenDeleteOverride(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	fixture.class.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
	fixture.class.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	}
	limit := int32(0)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	fixture.pinProfileHash(t)
	task := suspendableSessionTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), task)...)
	_, err := r.resolveACPWorkspaceClass(ctx, task)
	if err == nil || !strings.Contains(err.Error(), "retention cap") {
		t.Fatalf("quota error = %v, want exhausted retention cap", err)
	}
	if strings.Contains(err.Error(), "onDetach Delete") {
		t.Fatalf("quota error suggests a class-forbidden Delete override: %v", err)
	}
}

func TestValidateACPWorkspaceClassBindingAllowsLegacyUnboundedRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	task := suspendableSessionTask()
	r := acpClassTestReconciler(t, fixture.objects()...)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve bounded class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, suspendTestSessionUID, resolved)
	if err != nil {
		t.Fatalf("resolve bounded workspace binding: %v", err)
	}
	legacy := *binding.Class
	legacy.IdleTimeout = ""
	legacy.MaxLifetime = ""
	legacy.MaxSuspendedWorkspaces = nil
	if err := validateACPWorkspaceClassBindingValues(&legacy); err != nil {
		t.Fatalf("legacy unbounded frozen binding must remain executable after upgrade: %v", err)
	}
}

func TestACPWorkspaceSuspendQuotaAdmitsOwnSessionContinuation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{
		MaxSuspendedWorkspaces: func() *int32 { limit := int32(1); return &limit }(),
	}
	fixture.pinProfileHash(t)
	task := suspendableSessionTask()
	// The last quota slot is held by this very session's suspended workspace:
	// the continuation that would resume it must still be admitted, or it
	// could never reach ensureACPClassWorkspace to free the slot.
	own := acpAdapterWorkspace(t, "")
	own.Name = "acp-ws-own-session-suspended"
	own.UID = types.UID("own-session-suspended-uid")
	own.Spec.ClassBinding = workspacev1alpha1.ImmutableObjectBinding{
		Name: fixture.class.Name, UID: fixture.class.UID, Generation: 1, ProfileHash: fixture.class.Status.ProfileHash,
	}
	own.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{Name: acpTestSessionName, UID: types.UID("session-uid-1")}
	own.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	r := acpClassTestReconciler(t, append(fixture.objects(), task, own)...)
	attachQuotaSessionStores(r, map[string]string{acpTestSessionName: "session-uid-1"})
	if _, err := r.resolveACPWorkspaceClass(ctx, task); err != nil {
		t.Fatalf("a continuation of the suspended session must be admitted, got %v", err)
	}

	// A Task from another session still sees the exhausted cap.
	foreignSession := acpClassTestTask(func(other *corev1alpha1.Task) {
		other.Name = "other-session-task"
		other.UID = types.UID("other-session-task-uid")
		other.Spec.Execution.Workspace.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
		other.Spec.SessionRef = &corev1alpha1.SessionReference{Name: "session-b", Create: true}
	})
	if err := r.Create(ctx, foreignSession); err != nil {
		t.Fatalf("create foreign-session task: %v", err)
	}
	if _, err := r.resolveACPWorkspaceClass(ctx, foreignSession); err == nil ||
		!strings.Contains(err.Error(), "retention cap") {
		t.Fatalf("foreign-session admission error = %v, want retention cap exhaustion", err)
	}

	// A Session recreated under the same NAME resolves a different immutable
	// UID: the old incarnation's suspended workspace still consumes the cap,
	// so exclusion never matches on the reusable name alone.
	attachQuotaSessionStores(r, map[string]string{acpTestSessionName: "session-uid-2"})
	if _, err := r.resolveACPWorkspaceClass(ctx, task); err == nil ||
		!strings.Contains(err.Error(), "retention cap") {
		t.Fatalf("recreated-session admission error = %v, want the old-UID workspace still counted", err)
	}
}

func TestACPWorkspaceSuspendQuotaAdmitsQuotaBlockedReadyContinuation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	limit := int32(1)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{MaxSuspendedWorkspaces: &limit}
	fixture.pinProfileHash(t)
	holder := suspendableSessionTask()
	r := acpClassTestReconciler(t, append(fixture.objects(), holder)...)
	attachQuotaSessionStores(r, map[string]string{acpTestSessionName: suspendTestSessionUID})
	resolved, err := r.resolveACPWorkspaceClass(ctx, holder)
	if err != nil {
		t.Fatalf("resolve holder class: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(holder, "", false, suspendTestSessionUID, resolved)
	if err != nil {
		t.Fatalf("resolve holder binding: %v", err)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSessionPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, holder, plan); err != nil {
		t.Fatalf("materialize holder workspace: %v", err)
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	key := types.NamespacedName{Namespace: holder.Namespace, Name: acpClassWorkspaceName(holder, binding)}
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("read holder workspace: %v", err)
	}
	admitTestACPWorkspace(t, r, workspace)
	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("reload admitted workspace: %v", err)
	}
	base := workspace.DeepCopy()
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	delete(workspace.Annotations, acpWorkspaceResumeRequestedAnnotation)
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("mark quota-blocked workspace committed: %v", err)
	}

	occupant := acpAdapterWorkspace(t, "")
	occupant.Name = "acp-ws-quota-ready-occupant"
	occupant.UID = types.UID("acp-ws-quota-ready-occupant-uid")
	occupant.Spec.ClassBinding = workspace.Spec.ClassBinding
	occupant.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	if err := r.Create(ctx, occupant); err != nil {
		t.Fatalf("fill suspension quota: %v", err)
	}

	continuation := holder.DeepCopy()
	continuation.ObjectMeta = metav1.ObjectMeta{
		Namespace: holder.Namespace,
		Name:      "quota-ready-continuation",
		UID:       types.UID("quota-ready-continuation-uid"),
	}
	if _, err := r.resolveACPWorkspaceClass(ctx, continuation); err != nil {
		t.Fatalf("quota-blocked Ready workspace continuation must be admitted: %v", err)
	}

	if err := r.Get(ctx, key, workspace); err != nil {
		t.Fatalf("reload quota-blocked workspace: %v", err)
	}
	base = workspace.DeepCopy()
	delete(workspace.Annotations, acpWorkspaceDurableSessionCommittedAnnotation)
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("remove durable commit marker: %v", err)
	}
	if _, err := r.resolveACPWorkspaceClass(ctx, continuation); err == nil ||
		!strings.Contains(err.Error(), "retention cap") {
		t.Fatalf("uncommitted Ready workspace admission error = %v, want retention cap exhaustion", err)
	}
}

//nolint:gocyclo // This regression test covers admission, contention, and eventual settlement in one flow.
func TestSettleACPClassWorkspaceEnforcesSuspendQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	fixture.profile.Spec.Retention = &acpworkspacev1alpha1.RetentionPolicy{
		MaxSuspendedWorkspaces: func() *int32 { limit := int32(1); return &limit }(),
	}
	fixture.pinProfileHash(t)
	task := suspendableSessionTask()
	// An existing suspended workspace of the same class consumes the cap.
	other := acpAdapterWorkspace(t, "")
	other.Name = "acp-ws-existing-suspended"
	other.UID = types.UID("existing-suspended-uid")
	other.Spec.ClassBinding = workspacev1alpha1.ImmutableObjectBinding{
		Name: fixture.class.Name, UID: fixture.class.UID, Generation: 1, ProfileHash: fixture.class.Status.ProfileHash,
	}
	other.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	r := acpClassTestReconciler(t, append(fixture.objects(), task, other)...)

	// Admission rejects a prospective Suspend beyond the cap.
	if _, err := r.resolveACPWorkspaceClass(ctx, task); err == nil ||
		!strings.Contains(err.Error(), "retention cap") {
		t.Fatalf("admission error = %v, want retention cap exhaustion", err)
	}

	// With headroom the Task admits; settlement then re-checks the live count
	// and keeps the frozen Suspend action pending when the cap is gone.
	if err := r.Delete(ctx, other); err != nil {
		t.Fatalf("free the cap: %v", err)
	}
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	resolved, err := r.resolveACPWorkspaceClass(ctx, task)
	if err != nil {
		t.Fatalf("resolve class with headroom: %v", err)
	}
	binding, err := resolveACPWorkspaceBindingWithClass(task, "", false, "session-uid-1", resolved)
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	if binding.Class.MaxSuspendedWorkspaces == nil || *binding.Class.MaxSuspendedWorkspaces != 1 {
		t.Fatalf("frozen retention cap = %v", binding.Class.MaxSuspendedWorkspaces)
	}
	plan := ACPRuntimePlan{PoolName: acpTestSessionPoolName, Workspace: binding}
	if _, _, err := r.ensureACPClassWorkspace(ctx, task, plan); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	name := acpClassWorkspaceName(task, binding)
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	if workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] != "1" {
		t.Fatalf("frozen cap annotation = %q", workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation])
	}
	if workspace.Annotations[acpWorkspaceDetachActionAnnotation] != string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		t.Fatalf("creation must freeze the effective detach action, got %q",
			workspace.Annotations[acpWorkspaceDetachActionAnnotation])
	}
	admitTestACPWorkspace(t, r, workspace)
	if _, ready := attachTestACPWorkspace(t, r, task, plan, workspace.Name); !ready {
		t.Fatalf("attach = (%v)", ready)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("read attached workspace: %v", err)
	}
	base := workspace.DeepCopy()
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	if err := r.Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("record durable session commit: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); err != nil {
		t.Fatalf("reload task: %v", err)
	}

	// Another suspended workspace appears before settlement; the cap is gone.
	competitor := acpAdapterWorkspace(t, "")
	competitor.Name = "acp-ws-competitor-suspended"
	competitor.UID = types.UID("competitor-suspended-uid")
	competitor.Spec.ClassBinding = workspace.Spec.ClassBinding
	competitor.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	if err := r.Create(ctx, competitor); err != nil {
		t.Fatalf("create competitor: %v", err)
	}
	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || done {
		t.Fatalf("settle while attached = (%v, %v), want revocation retry", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("read revoked workspace: %v", err)
	}
	base = workspace.DeepCopy()
	workspace.Status.AttachedEpoch = 0
	if err := r.Status().Patch(ctx, workspace, client.MergeFrom(base)); err != nil {
		t.Fatalf("release enforced epoch: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, task)
	if err != nil || done {
		t.Fatalf("quota-exhausted settle = (%v, %v), want pending Suspend", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("workspace must survive quota exhaustion: %v", err)
	}
	if !workspace.DeletionTimestamp.IsZero() || workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("quota exhaustion changed workspace: deleting=%v desired=%s",
			workspace.DeletionTimestamp, workspace.Spec.DesiredState)
	}
	if err := r.Delete(ctx, competitor); err != nil {
		t.Fatalf("free settlement quota: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, task)
	if err != nil || !done {
		t.Fatalf("settle after quota frees = (%v, %v), want completion", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		t.Fatalf("read suspended workspace: %v", err)
	}
	if workspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("desired state after quota frees = %s, want Suspended", workspace.Spec.DesiredState)
	}
}

// Pending demand whose requesting Task disappeared (or settled terminally)
// must not hold the workspace forever; live demand still defers idle expiry.
func TestACPWorkspaceRetentionRetiresDeadPendingDemand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Dead requester: the named Task does not exist, so the creation-stamped
	// demand no longer blocks idle handling and the Delete-class workspace
	// enters terminal cleanup.
	dead := retentionTestWorkspace(t, "acp-ws-demand-dead", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) + " vanished-task"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, dead)
	reconcileRetention(t, c, dead)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: dead.Namespace, Name: dead.Name}, deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("dead-demand workspace must idle out, got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}

	// Live requester: demand defers idle expiry.
	requester := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: "live-requester", UID: types.UID("live-requester-uid"),
	}}
	live := retentionTestWorkspace(t, "acp-ws-demand-live", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) + " live-requester live-requester-uid"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c = acpAdapterTestClient(t, live, requester)
	reconcileRetention(t, c, live)
	kept := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: live.Namespace, Name: live.Name}, kept); err != nil || !kept.DeletionTimestamp.IsZero() {
		t.Fatalf("live demand must defer idle expiry, got err=%v deleting=%v", err, kept.DeletionTimestamp)
	}

	// A replacement Task recycled under the requester's namespace/name is
	// not the requester: the UID mismatch retires the stale demand.
	replacement := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: "recycled-requester", UID: types.UID("replacement-uid"),
	}}
	stale := retentionTestWorkspace(t, "acp-ws-demand-recycled", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) + " recycled-requester original-uid"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c = acpAdapterTestClient(t, stale, replacement)
	reconcileRetention(t, c, stale)
	recycled := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: stale.Namespace, Name: stale.Name}, recycled); err != nil || recycled.DeletionTimestamp.IsZero() {
		t.Fatalf("UID-mismatched demand must idle out, got err=%v deleting=%v", err, recycled.DeletionTimestamp)
	}

	// The intermediate legacy format carried only a mutable Task name. A live
	// replacement under that name is not demand unless the exact Session scan
	// independently proves it can continue this workspace incarnation.
	legacyReplacement := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: "legacy-recycled-requester", UID: types.UID("legacy-replacement-uid"),
	}}
	legacy := retentionTestWorkspace(t, "acp-ws-demand-legacy-recycled", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("legacy-session-uid"),
		}
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) + " legacy-recycled-requester"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c = acpAdapterTestClient(t, legacy, legacyReplacement)
	reconcileRetention(t, c, legacy)
	legacyRecycled := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(legacy), legacyRecycled); err != nil || legacyRecycled.DeletionTimestamp.IsZero() {
		t.Fatalf("legacy name-only demand with no exact Session continuation must idle out, got err=%v deleting=%v", err, legacyRecycled.DeletionTimestamp)
	}
}

// A timestamp-only demand stamp from an older controller has no requester
// identity. For an idle-only class, the live Session scan must retire it when
// no continuation remains instead of relying on an absent maxLifetime.
func TestACPWorkspaceRetentionRetiresLegacyPendingDemandWithoutMaxLifetime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-demand-legacy", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("legacy-demand-session-uid"),
		}
		w.Spec.Lifecycle.MaxLifetime = nil
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace)
	reconcileRetention(t, c, workspace)
	deleting := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), deleting); err != nil || deleting.DeletionTimestamp.IsZero() {
		t.Fatalf("legacy demand without a live continuation must idle out, got err=%v deleting=%v", err, deleting.DeletionTimestamp)
	}
}

func TestACPWorkspaceRetentionFailsClosedOnEmptyPendingDemand(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		stamp string
	}{
		{name: emptyCaseName, stamp: ""},
		{name: whitespaceCaseName, stamp: whitespaceOnlyValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			workspace := retentionTestWorkspace(t, "acp-ws-demand-empty-"+test.name, func(w *workspacev1alpha1.ExecutionWorkspace) {
				w.Annotations[acpWorkspaceResumeRequestedAnnotation] = test.stamp
				w.Annotations[acpWorkspaceLastDetachedAnnotation] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
			})
			c := acpAdapterTestClient(t, workspace)
			result := reconcileRetention(t, c, workspace)
			if result.RequeueAfter <= 0 {
				t.Fatalf("an empty pending-demand stamp must hold on a bounded requeue, got %+v", result)
			}
			current := &workspacev1alpha1.ExecutionWorkspace{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
				t.Fatalf("workspace must survive an empty pending-demand stamp: %v", err)
			}
			if !current.DeletionTimestamp.IsZero() {
				t.Fatal("an empty pending-demand stamp must not trigger idle deletion")
			}
		})
	}
}

// The single UID-bound demand stamp records only the LAST writer: when the
// recorded requester settles terminally while another live continuation on
// the same Session still waits, retention must keep the workspace instead of
// expiring it out from under the surviving waiter.
func TestACPWorkspaceRetentionHonorsSurvivingSessionContinuations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	terminalRequester := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-writer", UID: types.UID("lc-writer-uid"),
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
		},
	}
	terminalRequester.Status.Phase = corev1alpha1.TaskPhaseFailed
	waiter := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-waiter", UID: types.UID("lc-waiter-uid"),
			// The waiter was reconciled at least once and carries the
			// controller-written link to this exact workspace incarnation.
			Labels:      map[string]string{acpExecutionWorkspaceLinkLabel: "acp-ws-demand-survivor"},
			Annotations: map[string]string{acpExecutionWorkspaceUIDAnnotation: "acp-ws-demand-survivor-uid"},
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
		},
	}
	workspace := retentionTestWorkspace(t, "acp-ws-demand-survivor", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
		w.Annotations[acpWorkspaceResumeRequestedAnnotation] =
			time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) + " lc-writer lc-writer-uid"
		w.Annotations[acpWorkspaceLastDetachedAnnotation] =
			time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	})
	c := acpAdapterTestClient(t, workspace, terminalRequester, waiter)
	reconcileRetention(t, c, workspace)
	kept := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, kept); err != nil ||
		!kept.DeletionTimestamp.IsZero() {
		t.Fatalf("a live Session continuation must keep the workspace, got err=%v deleting=%v", err, kept.DeletionTimestamp)
	}

	// Once the surviving waiter settles terminally too, no demand remains and
	// idle expiry proceeds.
	currentWaiter := &corev1alpha1.Task{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: waiter.Namespace, Name: waiter.Name}, currentWaiter); err != nil {
		t.Fatalf("read waiter: %v", err)
	}
	currentWaiter.Status.Phase = corev1alpha1.TaskPhaseCancelled
	if err := c.Update(ctx, currentWaiter); err != nil {
		t.Fatalf("settle waiter: %v", err)
	}
	reconcileRetention(t, c, workspace)
	expired := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, expired); err != nil ||
		expired.DeletionTimestamp.IsZero() {
		t.Fatalf("with no live continuation the workspace must idle out, got err=%v deleting=%v", err, expired.DeletionTimestamp)
	}
}

// A terminally failed durable suspension keeps its quota slot. Present but
// invalid protected markers also fail closed as potentially durable, while an
// absent marker or an explicit durable-data-absent proof frees the slot.
func TestCountSuspendedClassWorkspacesChargesDurableFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("count-class-uid")
	shape := func(name string, mutate func(*workspacev1alpha1.ExecutionWorkspace)) *workspacev1alpha1.ExecutionWorkspace {
		w := acpAdapterWorkspace(t, "")
		w.Name = name
		w.UID = types.UID(name + "-uid")
		w.Spec.ClassBinding.UID = classUID
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		mutate(w)
		return w
	}
	durableFailed := shape("acp-ws-durable-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	emptyMarkerFailed := shape("acp-ws-empty-marker-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceDurableAnnotation] = ""
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	whitespaceMarkerFailed := shape("acp-ws-whitespace-marker-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceDurableAnnotation] = whitespaceOnlyValue
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	invalidMarkerFailed := shape("acp-ws-invalid-marker-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceDurableAnnotation] = "not-a-boolean"
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	provenEmptyFailed := shape("acp-ws-proven-empty-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Annotations[acpWorkspaceDurableAnnotation] = whitespaceOnlyValue
		w.Annotations[acpWorkspaceDurableDataAbsentAnnotation] = booleanTrueValue
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	plainFailed := shape("acp-ws-plain-failed", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	})
	suspended := shape("acp-ws-clean-suspended", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
	})
	c := acpAdapterTestClient(t, durableFailed, emptyMarkerFailed, whitespaceMarkerFailed, invalidMarkerFailed,
		provenEmptyFailed, plainFailed, suspended)
	count, err := countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Fatalf("count = %d, want durable and malformed failures plus the suspended workspace charged", count)
	}
}

// A cold resume lifts DesiredState to Ready before the preserved runtime is
// serving. Its prior Suspended/Suspending state must keep consuming capacity
// until the adapter projects Ready or Attached, or proves the data absent.
func TestCountSuspendedClassWorkspacesChargesColdResumesInFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("cold-resume-class-uid")
	shape := func(name string, state workspacev1alpha1.ExecutionWorkspaceState, dataAbsent bool) *workspacev1alpha1.ExecutionWorkspace {
		w := acpAdapterWorkspace(t, "")
		w.Name = name
		w.UID = types.UID(name + "-uid")
		w.Spec.ClassBinding.UID = classUID
		w.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
		w.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		if dataAbsent {
			w.Annotations[acpWorkspaceDurableDataAbsentAnnotation] = booleanTrueValue
		}
		w.Status.State = state
		return w
	}
	objects := []client.Object{
		shape("acp-ws-resume-suspended", workspacev1alpha1.ExecutionWorkspaceStateSuspended, false),
		shape("acp-ws-resume-suspending", workspacev1alpha1.ExecutionWorkspaceStateSuspending, false),
		shape("acp-ws-resume-ready", workspacev1alpha1.ExecutionWorkspaceStateReady, false),
		shape("acp-ws-resume-attached", workspacev1alpha1.ExecutionWorkspaceStateAttached, false),
		shape("acp-ws-resume-proven-empty", workspacev1alpha1.ExecutionWorkspaceStateSuspended, true),
	}
	c := acpAdapterTestClient(t, objects...)
	count, err := countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want only the two in-flight cold resumes charged", count)
	}
}

func TestCountSuspendedClassWorkspacesChargesDurableMaintenanceUntilTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("maintenance-class-uid")
	shape := func(name string, desired workspacev1alpha1.ExecutionWorkspaceDesiredState, state workspacev1alpha1.ExecutionWorkspaceState, disposition bool) *workspacev1alpha1.ExecutionWorkspace {
		workspace := acpAdapterWorkspace(t, "")
		workspace.Name = name
		workspace.UID = types.UID(name + "-uid")
		workspace.Spec.ClassBinding.UID = classUID
		workspace.Spec.DesiredState = desired
		workspace.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
		workspace.Status.State = state
		if disposition {
			workspace.Status.Disposition = &workspacev1alpha1.ExecutionWorkspaceDisposition{
				Compute:           workspacev1alpha1.DispositionDeleted,
				AccessCredentials: workspacev1alpha1.DispositionRevoked,
				EphemeralSecrets:  workspacev1alpha1.DispositionDeleted,
				WorkspaceData:     workspacev1alpha1.DispositionDeleted,
				PersistentVolumes: workspacev1alpha1.DispositionDeleted,
				Checkpoints:       workspacev1alpha1.DispositionDeleted,
				ProviderResources: workspacev1alpha1.DispositionDeleted,
			}
		}
		return workspace
	}
	invalidDisposition := shape("acp-ws-delete-invalid-disposition", workspacev1alpha1.ExecutionWorkspaceDesiredDeleted, workspacev1alpha1.ExecutionWorkspaceStateDeleted, true)
	invalidDisposition.Status.Disposition.PersistentVolumes = workspacev1alpha1.DispositionRetained
	legacyDeleting := shape("acp-ws-legacy-deleting", workspacev1alpha1.ExecutionWorkspaceDesiredReady, workspacev1alpha1.ExecutionWorkspaceStateReady, false)
	delete(legacyDeleting.Annotations, acpWorkspaceDurableAnnotation)
	legacyDeleting.Spec.Lifecycle.DefaultOnDetach = workspacev1alpha1.WorkspaceOnDetachSuspend
	legacyDeleting.Spec.Lifecycle.AllowedOnDetach = []workspacev1alpha1.WorkspaceOnDetach{
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	}
	legacyDeleting.Finalizers = []string{executionWorkspaceFinalizer}
	legacyDeleting.DeletionTimestamp = &metav1.Time{Time: time.Now().UTC()}
	objects := []client.Object{
		shape("acp-ws-quarantine-draining", workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined, workspacev1alpha1.ExecutionWorkspaceStateDeleting, false),
		shape("acp-ws-quarantine-terminal", workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined, workspacev1alpha1.ExecutionWorkspaceStateQuarantined, false),
		shape("acp-ws-delete-draining", workspacev1alpha1.ExecutionWorkspaceDesiredDeleted, workspacev1alpha1.ExecutionWorkspaceStateDeleting, false),
		shape("acp-ws-delete-unproven", workspacev1alpha1.ExecutionWorkspaceDesiredDeleted, workspacev1alpha1.ExecutionWorkspaceStateDeleted, false),
		invalidDisposition,
		shape("acp-ws-delete-terminal", workspacev1alpha1.ExecutionWorkspaceDesiredDeleted, workspacev1alpha1.ExecutionWorkspaceStateDeleted, true),
		legacyDeleting,
	}
	c := acpAdapterTestClient(t, objects...)
	count, err := countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Fatalf("count = %d, want quarantine/delete maintenance and legacy deletion charged until terminal proof", count)
	}
}

func TestCountSuspendedClassWorkspacesIgnoresForeignWorkspaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("owned-count-class-uid")
	shape := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		workspace := acpAdapterWorkspace(t, "")
		workspace.Name = name
		workspace.UID = types.UID(name + "-uid")
		workspace.Spec.ClassBinding.UID = classUID
		workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
		workspace.Status.State = workspacev1alpha1.ExecutionWorkspaceStateSuspended
		return workspace
	}
	owned := shape("acp-ws-owned-quota")
	foreign := shape("foreign-ws-copied-class")
	delete(foreign.Labels, workspacev1alpha1.ProviderControllerLabel)
	c := acpAdapterTestClient(t, owned, foreign)
	count, err := countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want only the ACP-owned workspace charged", count)
	}
}

func TestACPWorkspaceSuspendedCapFailsClosedOnEmptyAnnotation(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", whitespaceOnlyValue} {
		workspace := acpAdapterWorkspace(t, "")
		workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = value
		cap := acpWorkspaceSuspendedCapFromAnnotation(workspace)
		if cap == nil || *cap != 0 {
			t.Fatalf("cap for present value %q = %v, want exhausted zero cap", value, cap)
		}
	}

	workspace := acpAdapterWorkspace(t, "")
	if cap := acpWorkspaceSuspendedCapFromAnnotation(workspace); cap != nil {
		t.Fatalf("cap without an annotation = %d, want unbounded nil", *cap)
	}
}

// Demand binds to the workspace incarnation: a waiter already linked to a
// DIFFERENT workspace (for example a recreated Session's fresh incarnation)
// never counts as demand for this one.
func TestLiveSessionContinuationRequiresExactIncarnation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-incarnation", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-old"),
		}
	})
	foreignLinked := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-foreign", UID: types.UID("lc-foreign-uid"),
			Labels:      map[string]string{acpExecutionWorkspaceLinkLabel: "acp-ws-other"},
			Annotations: map[string]string{acpExecutionWorkspaceUIDAnnotation: "other-uid"},
		},
		Spec: corev1alpha1.TaskSpec{SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName}},
	}
	c := acpAdapterTestClient(t, workspace, foreignLinked)
	live, err := liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil {
		t.Fatalf("continuation check: %v", err)
	}
	if live {
		t.Fatal("a waiter linked to a different workspace incarnation must not count as demand")
	}

	exact := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-exact", UID: types.UID("lc-exact-uid"),
			Labels:      map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name},
			Annotations: map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)},
		},
		Spec: corev1alpha1.TaskSpec{SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName}},
	}
	if err := c.Create(ctx, exact); err != nil {
		t.Fatalf("create exact waiter: %v", err)
	}
	live, err = liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil || !live {
		t.Fatalf("exact-incarnation waiter must count as demand, got (%v, %v)", live, err)
	}
}

type failingListReader struct {
	client.Reader
}

func (f *failingListReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return fmt.Errorf("simulated apiserver outage")
}

type quotaLeaderHandoffReader struct {
	client.Reader
	writer   client.Client
	firstKey types.NamespacedName
	leaseKey types.NamespacedName
	advanced bool
}

func (r *quotaLeaderHandoffReader) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	if err := r.Reader.List(ctx, list, options...); err != nil {
		return err
	}
	if r.advanced {
		return nil
	}
	if _, ok := list.(*workspacev1alpha1.ExecutionWorkspaceList); !ok {
		return nil
	}
	r.advanced = true
	first := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.writer.Get(ctx, r.firstKey, first); err != nil {
		return err
	}
	firstBase := first.DeepCopy()
	first.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	if err := r.writer.Patch(ctx, first, client.MergeFrom(firstBase)); err != nil {
		return err
	}
	lease := &coordinationv1.Lease{}
	if err := r.writer.Get(ctx, r.leaseKey, lease); err != nil {
		return err
	}
	leaseBase := lease.DeepCopy()
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations["test.orka.ai/leader-handoff"] = advancedFenceValue
	return r.writer.Patch(ctx, lease, client.MergeFrom(leaseBase))
}

func TestACPSuspendQuotaLeaseFencesWorkspaceSnapshotAcrossLeaderHandoff(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("handoff-quota-class-uid")
	shape := func(name string) *workspacev1alpha1.ExecutionWorkspace {
		return retentionTestWorkspace(t, name, func(workspace *workspacev1alpha1.ExecutionWorkspace) {
			workspace.Spec.ClassBinding.UID = classUID
			workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "1"
		})
	}
	first := shape("acp-ws-handoff-first")
	second := shape("acp-ws-handoff-second")
	c := acpAdapterTestClient(t, first, second)
	if err := c.Get(ctx, client.ObjectKeyFromObject(first), first); err != nil {
		t.Fatalf("read first workspace: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), second); err != nil {
		t.Fatalf("read second workspace: %v", err)
	}
	lease, err := newACPSuspendQuotaLease(second, acpSuspendQuotaClaims{})
	if err != nil {
		t.Fatalf("build quota Lease: %v", err)
	}
	if err := c.Create(ctx, lease); err != nil {
		t.Fatalf("create quota Lease: %v", err)
	}
	reader := &quotaLeaderHandoffReader{
		Reader:   c,
		writer:   c,
		firstKey: client.ObjectKeyFromObject(first),
		leaseKey: client.ObjectKeyFromObject(lease),
	}
	if err := claimACPSuspendQuotaSlot(ctx, c, reader, second, 1); !errors.Is(err, errACPSuspendQuotaBusy) {
		t.Fatalf("handoff claim = %v, want retry after the Lease fence advances", err)
	}
	currentSecond := &workspacev1alpha1.ExecutionWorkspace{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(second), currentSecond); err != nil {
		t.Fatalf("read second workspace: %v", err)
	}
	if currentSecond.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("stale occupancy snapshot suspended the second workspace: %s", currentSecond.Spec.DesiredState)
	}
}

type conflictSuccessorLinkClient struct {
	client.Client
	target     types.NamespacedName
	conflicted bool
}

func (c *conflictSuccessorLinkClient) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.PatchOption,
) error {
	if task, ok := object.(*corev1alpha1.Task); ok && !c.conflicted &&
		client.ObjectKeyFromObject(task) == c.target {
		c.conflicted = true
		return apierrors.NewConflict(
			schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "tasks"},
			task.Name,
			errors.New("simulated successor link conflict"),
		)
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

func retentionSettlementTask(
	name string,
	uid string,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	action workspacev1alpha1.WorkspaceOnDetach,
) *corev1alpha1.Task {
	task := retentionContinuationTask(name, uid, action)
	task.Labels = map[string]string{acpExecutionWorkspaceLinkLabel: workspace.Name}
	task.Annotations = map[string]string{acpExecutionWorkspaceUIDAnnotation: string(workspace.UID)}
	return task
}

func retentionContinuationTask(
	name string,
	uid string,
	action workspacev1alpha1.WorkspaceOnDetach,
) *corev1alpha1.Task {
	task := suspendableSessionTask()
	task.Name = name
	task.UID = types.UID(uid)
	task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachPolicy(action)
	return task
}

// A transient quota-read failure must requeue the Task, never permanently
// reject it: only actual cap exhaustion and real validation failures are
// terminal.
func TestSuspendQuotaReadFailureIsTransient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	limit := int32(1)
	task := acpClassTestTask()
	task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachPolicy(workspacev1alpha1.WorkspaceOnDetachSuspend)
	r := acpClassTestReconciler(t, task)
	resolved := &acpResolvedWorkspaceClass{
		Binding:         ACPWorkspaceClassBinding{MaxSuspendedWorkspaces: &limit},
		DefaultOnDetach: workspacev1alpha1.WorkspaceOnDetachSuspend,
	}
	class := &workspacev1alpha1.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{Name: acpTestClassName, UID: "class-uid"}}
	err := r.enforceACPWorkspaceSuspendQuota(ctx, &failingListReader{}, task, class, resolved)
	if err == nil || !errors.Is(err, errACPWorkspacePlanningTransient) {
		t.Fatalf("quota-read outage must classify transient, got %v", err)
	}

	// The plan consumer requeues a transient plan instead of failing the Task.
	result, planErr := r.rejectPlannedAgentExecution(ctx, task,
		agentExecutionPlan{path: agentExecutionPathRejected, transientError: err})
	if planErr == nil || result.RequeueAfter != 0 {
		t.Fatalf("transient plan must return the error for backoff requeue, got (%v, %v)", result, planErr)
	}
	current := &corev1alpha1.Task{}
	if getErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); getErr != nil {
		t.Fatalf("reload task: %v", getErr)
	}
	if current.Status.Phase == corev1alpha1.TaskPhaseFailed {
		t.Fatal("a transient planning failure must never permanently fail the Task")
	}
}

// A requester that terminated before it ever executed against the workspace
// must not destroy the retained repository while another live continuation is
// queued on the same incarnation: settlement completes without the
// destructive action and the successor's own frozen action governs.
func TestSettleACPClassWorkspaceDefersDeleteToQueuedContinuation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-deferred-delete"
	workspace.UID = types.UID("deferred-delete-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	// The waiter's Suspend override must be class-allowed, or it would be
	// rejected as a non-successor by the AllowedOnDetach validation.
	workspace.Spec.Lifecycle.AllowedOnDetach = append(workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachDelete)
	dead := retentionSettlementTask(
		"lc-dead-requester", "lc-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	waiter := retentionSettlementTask(
		"lc-live-waiter", "lc-waiter-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
	dead = bindSuspendableSessionTaskForSettlement(t, r, dead)
	waiter = bindSuspendableSessionTaskForSettlement(t, r, waiter)

	done, err := r.settleACPClassWorkspace(ctx, dead)
	if err != nil || !done {
		t.Fatalf("settle with a queued continuation = (%v, %v), want done without destruction", done, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("the workspace must survive for the queued continuation: %v", err)
	}
	// The deferral transfers the successor's policy: the dead requester's
	// Delete must not survive to destroy the retained workspace if the
	// successor also terminates before attaching. The waiter's explicit
	// Suspend override governs from here.
	if got := current.Annotations[acpWorkspaceDetachActionAnnotation]; got != string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		t.Fatalf("deferred detach action = %q, want the successor's Suspend override", got)
	}
	// Restore Delete for the final-phase assertion below (the successor
	// policy path is exercised above; the remaining check proves the
	// destructive settle still runs once no continuation is left).
	base := current.DeepCopy()
	current.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachDelete)
	if err := r.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		t.Fatalf("restore Delete action: %v", err)
	}

	// With the last continuation gone, the stored Delete executes normally.
	if err := r.Delete(ctx, waiter); err != nil {
		t.Fatalf("remove waiter: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, dead)
	if err != nil || !done {
		t.Fatalf("settle without continuations = (%v, %v), want destructive completion", done, err)
	}
	err = r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("with no live continuation the frozen Delete must execute, got %v", err)
	}
}

// Settlement receipts are monotonic by attachment epoch: a Task whose receipt
// was displaced by a later Task's settlement must complete as done (and mark
// itself durably) instead of re-applying its detach action to newer session
// state; a foreign attachment likewise makes the done decision durable.
//
//nolint:gocyclo // The receipt-ordering cases form one settlement regression matrix.
func TestSettleACPClassWorkspaceHonorsDisplacedReceipts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	shape := func(name string) (*workspacev1alpha1.ExecutionWorkspace, *corev1alpha1.Task) {
		workspace := acpAdapterWorkspace(t, "")
		workspace.Name = name
		workspace.UID = types.UID(name + "-uid")
		workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
		workspace.Spec.Lifecycle.AllowedOnDetach = append(
			workspace.Spec.Lifecycle.AllowedOnDetach,
			workspacev1alpha1.WorkspaceOnDetachSuspend,
		)
		workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
		workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
		task := retentionSettlementTask(name+"-task", name+"-task-uid", workspace, workspacev1alpha1.WorkspaceOnDetachSuspend)
		task.Annotations[acpTaskAttachmentEpochAnnotation] = "3"
		return workspace, task
	}

	// Displaced receipt from a later epoch: settle completes without acting.
	workspace, task := shape("acp-ws-displaced")
	workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] = formatACPWorkspaceSettlementReceipt("successor-uid", 5)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || !done {
		t.Fatalf("displaced-receipt settle = (%v, %v), want done", done, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	if current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatal("a displaced settlement must never re-apply Suspend to the successor's state")
	}
	settledTask := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, settledTask); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if settledTask.Annotations[acpTaskWorkspaceSettledAnnotation] == "" {
		t.Fatal("the displaced settlement must mark the Task durably settled")
	}

	// An EARLIER-epoch receipt does not supersede: this Task's own action is
	// still owed and settlement proceeds (revocation is not needed here: the
	// workspace is unattached, so the Suspend patch lands).
	workspace, task = shape("acp-ws-owed")
	workspace.Spec.AttachmentEpoch = 5
	workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] = formatACPWorkspaceSettlementReceipt("predecessor-uid", 1)
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if _, err = r.settleACPClassWorkspace(ctx, task); err != nil {
		t.Fatalf("owed settle: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read owed workspace: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredSuspended {
		t.Fatalf("an earlier-epoch receipt must not supersede this Task's owed Suspend, state=%s", current.Spec.DesiredState)
	}
	receiptUID, receiptEpoch, receiptOK := parseACPWorkspaceSettlementReceipt(
		current.Annotations[acpWorkspaceLastSettledTaskAnnotation])
	if !receiptOK || receiptUID != string(task.UID) || receiptEpoch != 3 {
		t.Fatalf("owed settlement receipt = (%q, %d, %v), want Task %q at its recorded epoch 3", receiptUID, receiptEpoch, receiptOK, task.UID)
	}

	// Retention can suspend between revocation and Task settlement. The
	// fallback receipt must still use this Task's epoch, not a later
	// continuation's workspace high-water epoch.
	workspace, task = shape("acp-ws-already-suspended")
	workspace.Spec.AttachmentEpoch = 5
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if done, settleErr := r.settleACPClassWorkspace(ctx, task); settleErr != nil || !done {
		t.Fatalf("already-suspended settle = (%v, %v), want done", done, settleErr)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read already-suspended workspace: %v", err)
	}
	receiptUID, receiptEpoch, receiptOK = parseACPWorkspaceSettlementReceipt(
		current.Annotations[acpWorkspaceLastSettledTaskAnnotation])
	if !receiptOK || receiptUID != string(task.UID) || receiptEpoch != 3 {
		t.Fatalf("already-suspended receipt = (%q, %d, %v), want Task %q at its recorded epoch 3", receiptUID, receiptEpoch, receiptOK, task.UID)
	}

	// A queued continuation can terminate before it ever attaches, leaving a
	// recorded epoch of zero. If retention already suspended the workspace,
	// settling that continuation must not replace a later attached Task's
	// receipt with the epoch-zero receipt.
	workspace, task = shape("acp-ws-unattached-already-suspended")
	task.Annotations[acpTaskAttachmentEpochAnnotation] = "0"
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] =
		formatACPWorkspaceSettlementReceipt("attached-successor-uid", 5)
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if done, settleErr := r.settleACPClassWorkspace(ctx, task); settleErr != nil || !done {
		t.Fatalf("unattached already-suspended settle = (%v, %v), want done", done, settleErr)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read unattached already-suspended workspace: %v", err)
	}
	receiptUID, receiptEpoch, receiptOK = parseACPWorkspaceSettlementReceipt(
		current.Annotations[acpWorkspaceLastSettledTaskAnnotation])
	if !receiptOK || receiptUID != "attached-successor-uid" || receiptEpoch != 5 {
		t.Fatalf("unattached settlement receipt = (%q, %d, %v), want preserved successor receipt at epoch 5", receiptUID, receiptEpoch, receiptOK)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, settledTask); err != nil {
		t.Fatalf("read unattached settled task: %v", err)
	}
	if settledTask.Annotations[acpTaskWorkspaceSettledAnnotation] == "" {
		t.Fatal("the unattached settlement must mark the Task durably settled")
	}

	// The same epoch-zero settlement must not suspend a workspace that a
	// later attached Task has already settled and a continuation resumed.
	workspace, task = shape("acp-ws-unattached-ready")
	task.Annotations[acpTaskAttachmentEpochAnnotation] = "0"
	workspace.Spec.AttachmentEpoch = 5
	workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] =
		formatACPWorkspaceSettlementReceipt("attached-successor-uid", 5)
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if done, settleErr := r.settleACPClassWorkspace(ctx, task); settleErr != nil || !done {
		t.Fatalf("unattached Ready settle = (%v, %v), want done", done, settleErr)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read unattached Ready workspace: %v", err)
	}
	if current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("unattached stale settlement changed newer workspace state to %s", current.Spec.DesiredState)
	}
	receiptUID, receiptEpoch, receiptOK = parseACPWorkspaceSettlementReceipt(
		current.Annotations[acpWorkspaceLastSettledTaskAnnotation])
	if !receiptOK || receiptUID != "attached-successor-uid" || receiptEpoch != 5 {
		t.Fatalf("unattached Ready receipt = (%q, %d, %v), want preserved successor receipt at epoch 5", receiptUID, receiptEpoch, receiptOK)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, settledTask); err != nil {
		t.Fatalf("read unattached Ready settled task: %v", err)
	}
	if settledTask.Annotations[acpTaskWorkspaceSettledAnnotation] == "" {
		t.Fatal("the displaced unattached settlement must mark the Task durably settled")
	}

	// A UID-bound resume demand belongs to the current epoch-zero requester,
	// not the predecessor named by the positive receipt. Its frozen Delete
	// action must still run if it terminates before Attach.
	workspace, task = shape("acp-ws-current-resume-requester")
	task.Spec.Execution.Workspace.OnDetach = corev1alpha1.WorkspaceOnDetachPolicy(workspacev1alpha1.WorkspaceOnDetachDelete)
	task.Annotations[acpTaskAttachmentEpochAnnotation] = "0"
	workspace.Spec.AttachmentEpoch = 5
	workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] =
		formatACPWorkspaceSettlementReceipt("attached-predecessor-uid", 5)
	workspace.Annotations[acpWorkspaceResumeRequestedAnnotation] =
		time.Now().UTC().Format(time.RFC3339Nano) + " " + task.Name + " " + string(task.UID)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachDelete)
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if done, settleErr := r.settleACPClassWorkspace(ctx, task); settleErr != nil || !done {
		t.Fatalf("current resume requester settle = (%v, %v), want completed Delete", done, settleErr)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); !apierrors.IsNotFound(err) {
		t.Fatalf("current resume requester skipped its Delete action: %v", err)
	}

	// A foreign attachment makes the done decision durable on the Task.
	workspace, task = shape("acp-ws-foreign-attached")
	workspace.Spec.AttachmentEpoch = 7
	workspace.Spec.Attachment = &workspacev1alpha1.ExecutionWorkspaceAttachment{
		TaskRef:        workspacev1alpha1.ObjectIdentityReference{Name: "successor", UID: types.UID("successor-uid")},
		Epoch:          7,
		TokenSHA256:    "sha256:" + strings.Repeat("d", 64),
		TokenSecretRef: workspacev1alpha1.SecretReference{Name: "successor-secret"},
		ExpiresAt:      metav1.Now(),
	}
	r = acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	if done, err = r.settleACPClassWorkspace(ctx, task); err != nil || !done {
		t.Fatalf("foreign-attachment settle = (%v, %v), want done", done, err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, settledTask); err != nil {
		t.Fatalf("read foreign-attached task: %v", err)
	}
	if settledTask.Annotations[acpTaskWorkspaceSettledAnnotation] == "" {
		t.Fatal("a foreign attachment must mark the displaced Task durably settled")
	}
}

// A terminally failed suspension that PROVED no durable data exists (the
// adapter's proven-empty marker) frees its quota slot even on a durable
// class; resume-loss failures that preserve a claim stay charged.
func TestCountSuspendedClassWorkspacesFreesProvenEmptyFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	classUID := types.UID("count-empty-class-uid")
	provenEmpty := acpAdapterWorkspace(t, "")
	provenEmpty.Name = "acp-ws-proven-empty"
	provenEmpty.UID = types.UID("acp-ws-proven-empty-uid")
	provenEmpty.Spec.ClassBinding.UID = classUID
	provenEmpty.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	provenEmpty.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	provenEmpty.Annotations[acpWorkspaceDurableDataAbsentAnnotation] = booleanTrueValue
	provenEmpty.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	c := acpAdapterTestClient(t, provenEmpty)
	count, err := countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want a proven-empty failure to free its quota slot", count)
	}

	failedResume := acpAdapterWorkspace(t, "")
	failedResume.Name = "acp-ws-failed-resume"
	failedResume.UID = types.UID("acp-ws-failed-resume-uid")
	failedResume.Spec.ClassBinding.UID = classUID
	failedResume.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	failedResume.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	failedResume.Status.State = workspacev1alpha1.ExecutionWorkspaceStateFailed
	if err := c.Create(ctx, failedResume); err != nil {
		t.Fatalf("create failed resume: %v", err)
	}
	count, err = countSuspendedClassWorkspaces(ctx, c, acpTestNamespace, classUID, nil)
	if err != nil {
		t.Fatalf("count failed resume: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want a failed durable resume charged against the quota", count)
	}
}

// A live Task that merely shares the Session name without requesting a
// session-reused execution workspace can never attach the workspace and must
// not count as demand.
func TestLiveSessionContinuationIgnoresNonWorkspaceTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-nonws-demand", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
	})
	transcriptOnly := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-transcript-only", UID: types.UID("lc-transcript-only-uid"),
		},
		Spec: corev1alpha1.TaskSpec{SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName}},
	}
	c := acpAdapterTestClient(t, workspace, transcriptOnly)
	live, err := liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil {
		t.Fatalf("continuation check: %v", err)
	}
	if live {
		t.Fatal("a transcript-only session Task must never count as workspace demand")
	}
}

// An unlinked continuation counts as demand only when its classRef resolves
// to THIS workspace's class: a different class (or the legacy enabled path)
// deliberately produces a separate workspace incarnation and must not defer
// settlement or retention of this one.
func TestLiveSessionContinuationRequiresMatchingClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workspace := retentionTestWorkspace(t, "acp-ws-class-demand", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
	})
	otherClass := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-other-class", UID: types.UID("lc-other-class-uid"),
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
			Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
				ClassRef:    &corev1alpha1.WorkspaceClassReference{Name: "some-other-class"},
				ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
			}},
		},
	}
	// The bound class exists at its frozen immutable identity: unlinked
	// demand additionally verifies the live class UID against the binding.
	boundClass := &workspacev1alpha1.ExecutionWorkspaceClass{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: workspace.Spec.ClassBinding.Name,
			UID: workspace.Spec.ClassBinding.UID,
		},
	}
	c := acpAdapterTestClient(t, workspace, otherClass, boundClass)
	live, err := liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil {
		t.Fatalf("continuation check: %v", err)
	}
	if live {
		t.Fatal("a different-class waiter must never count as demand for this workspace")
	}

	matching := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "lc-matching-class", UID: types.UID("lc-matching-class-uid"),
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
			Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
				ClassRef:    &corev1alpha1.WorkspaceClassReference{Name: workspace.Spec.ClassBinding.Name},
				ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
			}},
		},
	}
	if err := c.Create(ctx, matching); err != nil {
		t.Fatalf("create matching waiter: %v", err)
	}
	live, err = liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil || !live {
		t.Fatalf("a matching-class unlinked waiter must count as demand, got (%v, %v)", live, err)
	}

	// The class is deleted and recreated under the same name: the waiter's
	// classRef now resolves the REPLACEMENT class and a different workspace
	// incarnation, so it must not suppress this workspace's retention.
	if err := c.Delete(ctx, boundClass); err != nil {
		t.Fatalf("delete bound class: %v", err)
	}
	recreated := &workspacev1alpha1.ExecutionWorkspaceClass{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: workspace.Spec.ClassBinding.Name,
			UID: types.UID("recreated-class-uid"),
		},
	}
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatalf("recreate class: %v", err)
	}
	live, err = liveACPSessionContinuationExists(ctx, c, workspace)
	if err != nil || live {
		t.Fatalf("a waiter resolving a recreated class must not count as demand, got (%v, %v)", live, err)
	}
}

func TestPendingWorkspaceDemandRequiresMatchingSessionUIDForUnlinkedTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fenceTime := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	workspace := retentionTestWorkspace(t, "acp-ws-session-uid-demand", func(w *workspacev1alpha1.ExecutionWorkspace) {
		w.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("original-session-uid"),
		}
	})
	waiter := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: acpTestNamespace, Name: "session-uid-waiter", UID: types.UID("session-uid-waiter-uid"),
			CreationTimestamp: metav1.NewTime(fenceTime.Add(time.Second)),
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: acpTestSessionName},
			Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
				ClassRef:    &corev1alpha1.WorkspaceClassReference{Name: workspace.Spec.ClassBinding.Name},
				ReusePolicy: corev1alpha1.WorkspaceReusePolicySession,
			}},
		},
	}
	boundClass := &workspacev1alpha1.ExecutionWorkspaceClass{ObjectMeta: metav1.ObjectMeta{
		Namespace: acpTestNamespace, Name: workspace.Spec.ClassBinding.Name, UID: workspace.Spec.ClassBinding.UID,
	}}
	c := acpAdapterTestClient(t, workspace, waiter, boundClass)
	sessions := map[string]string{acpTestSessionName: replacementSessionUID}
	reconciler := &ACPWorkspaceRetentionReconciler{
		Client: c, APIReader: c,
		DurableControlStore: &quotaSessionControlStore{namespace: acpTestNamespace, sessions: sessions},
	}
	if outstanding, err := reconciler.pendingWorkspaceDemandOutstanding(ctx, workspace, nil); err != nil || outstanding {
		t.Fatalf("replacement Session demand = (%v, %v), want false", outstanding, err)
	}
	sessions[acpTestSessionName] = string(workspace.Spec.SessionRef.UID)
	if outstanding, err := reconciler.pendingWorkspaceDemandOutstanding(ctx, workspace, &fenceTime); err != nil || outstanding {
		t.Fatalf("post-fence unlinked Session demand = (%v, %v), want false", outstanding, err)
	}
	if outstanding, err := reconciler.pendingWorkspaceDemandOutstanding(ctx, workspace, nil); err != nil || !outstanding {
		t.Fatalf("matching Session demand = (%v, %v), want true", outstanding, err)
	}
}

func TestDeferACPSettlementWaitsForSuccessorBinding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-unbound-successor"
	workspace.UID = types.UID("acp-ws-unbound-successor-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID(suspendTestSessionUID),
	}
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachDelete)
	dead := retentionSettlementTask(
		"unbound-successor-dead", "unbound-successor-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	waiter := retentionContinuationTask(
		"unbound-successor-waiter", "unbound-successor-waiter-uid",
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)

	deferred, retry, err := r.deferACPSettlementToSuccessor(ctx, workspace, dead)
	if err != nil || deferred || !retry {
		t.Fatalf("unbound successor deferral = (deferred=%v retry=%v err=%v), want a retry without transfer", deferred, retry, err)
	}
	currentWaiter := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(waiter), currentWaiter); err != nil {
		t.Fatalf("read unbound successor: %v", err)
	}
	if currentWaiter.Labels[acpExecutionWorkspaceLinkLabel] != "" ||
		currentWaiter.Annotations[acpExecutionWorkspaceUIDAnnotation] != "" ||
		controllerutil.ContainsFinalizer(currentWaiter, labels.TaskFinalizer) {
		t.Fatalf("unbound successor acquired settlement ownership: labels=%#v annotations=%#v finalizers=%#v",
			currentWaiter.Labels, currentWaiter.Annotations, currentWaiter.Finalizers)
	}
	if err := r.Delete(ctx, currentWaiter); err != nil {
		t.Fatalf("delete unbound successor: %v", err)
	}
	dead = bindSuspendableSessionTaskForSettlement(t, r, dead)
	done, err := r.settleACPClassWorkspace(ctx, dead)
	if err != nil || !done {
		t.Fatalf("settle after unbound successor vanished = (%v, %v), want predecessor cleanup", done, err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), &workspacev1alpha1.ExecutionWorkspace{}); !apierrors.IsNotFound(err) {
		t.Fatalf("predecessor must delete the workspace after the unbound successor vanishes, got %v", err)
	}
}

func TestDeferACPSettlementClearsPredecessorRevocationStamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-revocation-handoff"
	workspace.UID = types.UID("acp-ws-revocation-handoff-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	workspace.Spec.AttachmentEpoch = 3
	workspace.Annotations[acpWorkspaceRevocationStartedAnnotation] = fmt.Sprintf(
		"3 %s", time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	)
	dead := retentionSettlementTask(
		"revocation-handoff-dead", "revocation-handoff-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	waiter := retentionContinuationTask(
		"revocation-handoff-waiter", "revocation-handoff-waiter-uid",
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
	bindSuspendableSessionTaskForSettlement(t, r, waiter)

	deferred, retry, err := r.deferACPSettlementToSuccessor(ctx, workspace, dead)
	if err != nil || !deferred || retry {
		t.Fatalf("revocation handoff = (deferred=%v retry=%v err=%v), want a completed transfer", deferred, retry, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read handed-off workspace: %v", err)
	}
	if stamp := current.Annotations[acpWorkspaceRevocationStartedAnnotation]; stamp != "" {
		t.Fatalf("predecessor revocation stamp survived handoff: %q", stamp)
	}
	linked := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(waiter), linked); err != nil {
		t.Fatalf("read linked successor: %v", err)
	}
	if linked.Labels[acpExecutionWorkspaceLinkLabel] != workspace.Name ||
		linked.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
		t.Fatalf("successor link = %q/%q, want %q/%q",
			linked.Labels[acpExecutionWorkspaceLinkLabel], linked.Annotations[acpExecutionWorkspaceUIDAnnotation],
			workspace.Name, workspace.UID)
	}
}

// A queued waiter whose explicit onDetach override is outside the class's
// AllowedOnDetach can never attach this workspace: it is NOT a successor, and
// settlement must keep cleanup ownership instead of transferring a policy the
// class forbids.
func TestDeferACPSettlementRejectsPolicyForbiddenSuccessorOverride(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	shape := func(name string, onDetach corev1alpha1.WorkspaceOnDetachPolicy) (bool, *workspacev1alpha1.ExecutionWorkspace, *TaskReconciler) {
		fixture := suspendableSubstrateFixture(t)
		workspace := acpAdapterWorkspace(t, "")
		workspace.Name = name
		workspace.UID = types.UID(name + "-uid")
		workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
		dead := retentionSettlementTask(name+"-dead", name+"-dead-uid", workspace, workspacev1alpha1.WorkspaceOnDetachDelete)
		waiter := retentionSettlementTask(name+"-waiter", name+"-waiter-uid", workspace, workspacev1alpha1.WorkspaceOnDetach(onDetach))
		r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
		bindSuspendableSessionTaskForSettlement(t, r, waiter)
		deferred, retry, err := r.deferACPSettlementToSuccessor(ctx, workspace, dead)
		if err != nil || retry {
			t.Fatalf("defer(%s) = (deferred=%v retry=%v err=%v), want no retry and no error", name, deferred, retry, err)
		}
		return deferred, workspace, r
	}

	// AllowedOnDetach is [Delete]: a Suspend override is class-forbidden.
	if deferred, _, _ := shape("acp-ws-forbidden-override", corev1alpha1.WorkspaceOnDetachPolicy(workspacev1alpha1.WorkspaceOnDetachSuspend)); deferred {
		t.Fatal("a class-forbidden successor override must not defer settlement")
	}

	// A valid continuation queued BEHIND an ineligible one must still defer:
	// settlement scans past the forbidden override instead of concluding no
	// successor exists.
	t.Run("scans past an ineligible candidate", func(t *testing.T) {
		t.Parallel()
		fixture := suspendableSubstrateFixture(t)
		workspace := acpAdapterWorkspace(t, "")
		workspace.Name = "acp-ws-scan-candidates"
		workspace.UID = types.UID("acp-ws-scan-candidates-uid")
		workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
			Name: acpTestSessionName, UID: types.UID("session-uid-1"),
		}
		dead := retentionSettlementTask("scan-dead", "scan-dead-uid", workspace, workspacev1alpha1.WorkspaceOnDetachDelete)
		forbidden := retentionSettlementTask("scan-forbidden", "scan-forbidden-uid", workspace, workspacev1alpha1.WorkspaceOnDetachSuspend)
		valid := retentionSettlementTask("scan-valid", "scan-valid-uid", workspace, workspacev1alpha1.WorkspaceOnDetachDelete)
		r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, forbidden, valid)...)
		bindSuspendableSessionTaskForSettlement(t, r, valid)
		deferred, retry, err := r.deferACPSettlementToSuccessor(context.Background(), workspace, dead)
		if err != nil || retry {
			t.Fatalf("defer = (deferred=%v retry=%v err=%v)", deferred, retry, err)
		}
		if !deferred {
			t.Fatal("a valid continuation behind an ineligible candidate must still defer settlement")
		}
	})
	deferred, workspace, r := shape("acp-ws-allowed-override", corev1alpha1.WorkspaceOnDetachPolicy(workspacev1alpha1.WorkspaceOnDetachDelete))
	if !deferred {
		t.Fatal("a class-allowed successor override must defer settlement")
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("read deferred workspace: %v", err)
	}
	if current.Annotations[acpWorkspaceDetachActionAnnotation] != string(workspacev1alpha1.WorkspaceOnDetachDelete) {
		t.Fatalf("transferred detach action = %q, want the successor's allowed Delete", current.Annotations[acpWorkspaceDetachActionAnnotation])
	}
	// Ownership is transferred durably: the selected successor carries the
	// cleanup finalizer and exact workspace link before the predecessor's
	// settlement completes.
	linkedSuccessor := &corev1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: acpTestNamespace, Name: "acp-ws-allowed-override-waiter"}, linkedSuccessor); err != nil {
		t.Fatalf("read successor: %v", err)
	}
	if linkedSuccessor.Labels[acpExecutionWorkspaceLinkLabel] != workspace.Name ||
		linkedSuccessor.Annotations[acpExecutionWorkspaceUIDAnnotation] != string(workspace.UID) {
		t.Fatalf("successor link = %q/%q, want the deferred workspace's exact link",
			linkedSuccessor.Labels[acpExecutionWorkspaceLinkLabel], linkedSuccessor.Annotations[acpExecutionWorkspaceUIDAnnotation])
	}
	if !controllerutil.ContainsFinalizer(linkedSuccessor, labels.TaskFinalizer) {
		t.Fatal("successor cleanup finalizer must be installed before settlement ownership transfers")
	}
}

// A successor-link conflict can leave the candidate's action on the
// workspace. If that successor then disappears, the predecessor must restore
// its own effective action before settling instead of preserving stale data.
func TestSettleACPClassWorkspaceRestoresPolicyAfterSuccessorLinkConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-link-conflict"
	workspace.UID = types.UID("acp-ws-link-conflict-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	workspace.Spec.Lifecycle.AllowedOnDetach = append(workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachDelete)
	dead := retentionSettlementTask(
		"link-conflict-dead", "link-conflict-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	waiter := retentionContinuationTask(
		"link-conflict-waiter", "link-conflict-waiter-uid",
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
	dead = bindSuspendableSessionTaskForSettlement(t, r, dead)
	bindSuspendableSessionTaskForSettlement(t, r, waiter)
	baseClient := r.Client
	r.Client = &conflictSuccessorLinkClient{
		Client: baseClient,
		target: types.NamespacedName{Namespace: waiter.Namespace, Name: waiter.Name},
	}
	r.APIReader = baseClient

	deferred, retry, err := r.deferACPSettlementToSuccessor(ctx, workspace, dead)
	if err != nil || deferred || !retry {
		t.Fatalf("defer with link conflict = (deferred=%v retry=%v err=%v), want a retry", deferred, retry, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read workspace after link conflict: %v", err)
	}
	if current.Annotations[acpWorkspaceDetachActionAnnotation] != string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		t.Fatalf("transferred action = %q, want the failed transfer residue", current.Annotations[acpWorkspaceDetachActionAnnotation])
	}
	if err := baseClient.Delete(ctx, waiter); err != nil {
		t.Fatalf("delete vanished successor: %v", err)
	}
	done, err := r.settleACPClassWorkspace(ctx, dead)
	if err != nil || !done {
		t.Fatalf("settle after successor vanished = (%v, %v), want predecessor completion", done, err)
	}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), current); !apierrors.IsNotFound(err) {
		t.Fatalf("the predecessor Delete policy must be restored, got %v", err)
	}
}

// A destructive successor policy must not land before its ownership link.
// Otherwise an executed predecessor whose Suspend is quota-blocked can retry
// after the link conflict and delete the retained workspace under the stale
// successor action.
func TestSettleACPClassWorkspaceKeepsSuspendUntilDeleteSuccessorLinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-delete-link-conflict"
	workspace.UID = types.UID("acp-ws-delete-link-conflict-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	workspace.Spec.Lifecycle.AllowedOnDetach = append(workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "0"
	workspace.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	dead := retentionSettlementTask(
		"delete-link-conflict-dead", "delete-link-conflict-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	dead.Status.Execution = &corev1alpha1.TaskExecutionStatus{RuntimePoolName: "acp-ws-runtime-pool"}
	waiter := retentionContinuationTask(
		"delete-link-conflict-waiter", "delete-link-conflict-waiter-uid",
		workspacev1alpha1.WorkspaceOnDetachDelete,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
	dead = bindSuspendableSessionTaskForSettlement(t, r, dead)
	bindSuspendableSessionTaskForSettlement(t, r, waiter)
	baseClient := r.Client
	r.Client = &conflictSuccessorLinkClient{
		Client: baseClient,
		target: types.NamespacedName{Namespace: waiter.Namespace, Name: waiter.Name},
	}
	r.APIReader = baseClient

	done, err := r.settleACPClassWorkspace(ctx, dead)
	if err != nil || done {
		t.Fatalf("quota settle with Delete successor link conflict = (%v, %v), want retry", done, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("read workspace after Delete successor link conflict: %v", err)
	}
	if got := current.Annotations[acpWorkspaceDetachActionAnnotation]; got != string(workspacev1alpha1.WorkspaceOnDetachSuspend) {
		t.Fatalf("detach action after failed Delete transfer = %q, want predecessor Suspend", got)
	}
	if err := baseClient.Delete(ctx, waiter); err != nil {
		t.Fatalf("delete vanished successor: %v", err)
	}
	done, err = r.settleACPClassWorkspace(ctx, dead)
	if err != nil || done {
		t.Fatalf("quota settle after Delete successor vanished = (%v, %v), want pending Suspend", done, err)
	}
	if err := baseClient.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("the predecessor Suspend policy must preserve the workspace: %v", err)
	}
}

// Quota exhaustion reports why settlement is pending without deleting the
// workspace or completing the Task's frozen Suspend action.
func TestSettleACPClassWorkspaceReportsPendingQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-quota-event"
	workspace.UID = types.UID("acp-ws-quota-event-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	workspace.Spec.Lifecycle.AllowedOnDetach = append(workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "0"
	workspace.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	task := retentionSettlementTask(
		"quota-event-task", "quota-event-task-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, task)...)
	task = bindSuspendableSessionTaskForSettlement(t, r, task)
	recorder := record.NewFakeRecorder(2)
	r.Recorder = recorder

	done, err := r.settleACPClassWorkspace(ctx, task)
	if err != nil || done {
		t.Fatalf("quota-exhausted settlement = (%v, %v), want a pending retry", done, err)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "SuspendQuotaExhausted") {
			t.Fatalf("quota Event = %q, want SuspendQuotaExhausted", event)
		}
	default:
		t.Fatal("quota exhaustion did not emit its Event")
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(workspace), current); err != nil {
		t.Fatalf("workspace must survive quota exhaustion: %v", err)
	}
	if !current.DeletionTimestamp.IsZero() || current.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredReady {
		t.Fatalf("quota exhaustion changed workspace: deleting=%v desired=%s",
			current.DeletionTimestamp, current.Spec.DesiredState)
	}
}

// A live queued continuation can take a still-Ready workspace directly when
// an executed predecessor's frozen Suspend action cannot claim a quota slot.
func TestSettleACPClassWorkspaceQuotaWaitDefersToSuccessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := suspendableSubstrateFixture(t)
	workspace := acpAdapterWorkspace(t, "")
	workspace.Name = "acp-ws-quota-deferred"
	workspace.UID = types.UID("acp-ws-quota-deferred-uid")
	workspace.Spec.SessionRef = &workspacev1alpha1.ObjectIdentityReference{
		Name: acpTestSessionName, UID: types.UID("session-uid-1"),
	}
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredReady
	workspace.Spec.Lifecycle.AllowedOnDetach = append(workspace.Spec.Lifecycle.AllowedOnDetach,
		workspacev1alpha1.WorkspaceOnDetachSuspend)
	workspace.Annotations[acpWorkspaceDetachActionAnnotation] = string(workspacev1alpha1.WorkspaceOnDetachSuspend)
	// A frozen cap of zero makes any suspension quota-exhausted.
	workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation] = "0"
	workspace.Annotations[acpWorkspaceDurableAnnotation] = booleanTrueValue
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = "1"
	dead := retentionSettlementTask(
		"lc-quota-dead", "lc-quota-dead-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	dead.Status.Execution = &corev1alpha1.TaskExecutionStatus{RuntimePoolName: "acp-ws-runtime-pool"}
	waiter := retentionSettlementTask(
		"lc-quota-waiter", "lc-quota-waiter-uid", workspace,
		workspacev1alpha1.WorkspaceOnDetachSuspend,
	)
	r := acpClassTestReconciler(t, append(fixture.objects(), workspace, dead, waiter)...)
	dead = bindSuspendableSessionTaskForSettlement(t, r, dead)
	bindSuspendableSessionTaskForSettlement(t, r, waiter)
	if taskNeverHeldACPWorkspaceAttachment(dead) {
		t.Fatal("fixture must record the predecessor's completed workspace execution")
	}
	done, err := r.settleACPClassWorkspace(ctx, dead)
	if err != nil || !done {
		t.Fatalf("quota-exhausted settle with a queued continuation = (%v, %v), want deferred completion", done, err)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: workspace.Name}, current); err != nil {
		t.Fatalf("the workspace must survive the quota wait for the queued continuation: %v", err)
	}
}

// liveACPSessionContinuationExists reports whether any live, non-terminal
// Task in the workspace's namespace targets this exact workspace incarnation
// through its Session. The reader uses the Task CRD's server-side selectable
// field; list errors fail closed as outstanding demand.
func liveACPSessionContinuationExists(
	ctx context.Context,
	reader client.Reader,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (bool, error) {
	successors, err := liveACPSessionContinuations(ctx, reader, workspace, "")
	if err != nil {
		return true, err
	}
	return len(successors) > 0, nil
}
