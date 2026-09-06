/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
)

func TestSubstrateMCPPoolActorLeaseToolHolder(t *testing.T) {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "actor-00000",
		},
	}
	tool := &corev1alpha1.Tool{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "tool", UID: types.UID("tool-uid")}}

	setSubstrateMCPPoolActorLeaseHolder(lease, tool, "actor-00000")

	if lease.Labels[labels.LabelPurpose] != substratePoolActorLeasePurpose {
		t.Fatalf("purpose label = %q, want %q", lease.Labels[labels.LabelPurpose], substratePoolActorLeasePurpose)
	}
	if lease.Labels[substratePoolActorLeaseHolderUIDLabel] != "tool-uid" {
		t.Fatalf("holder uid label = %q, want tool-uid", lease.Labels[substratePoolActorLeaseHolderUIDLabel])
	}
	if !substratePoolActorLeaseHeldByTool(lease, tool) {
		t.Fatalf("lease should be held by tool")
	}
}

func TestSubstratePoolActorLeaseIdentityHelpers(t *testing.T) {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "fallback-actor"}}
	if got := substratePoolActorLeaseActorID(lease); got != "fallback-actor" {
		t.Fatalf("actor id fallback = %q, want fallback-actor", got)
	}

	lease.Labels = map[string]string{substratePoolActorLeaseActorIDLabel: " labeled-actor "}
	if got := substratePoolActorLeaseActorID(lease); got != "labeled-actor" {
		t.Fatalf("actor id label = %q, want labeled-actor", got)
	}

	prefix, ordinal, ok := substratePoolActorPrefixAndOrdinal("pool-prefix-00042")
	if !ok || prefix != "pool-prefix" || ordinal != 42 {
		t.Fatalf("prefix/ordinal = %q, %d, %t; want pool-prefix, 42, true", prefix, ordinal, ok)
	}
	if _, _, ok := substratePoolActorPrefixAndOrdinal("pool-prefix-42"); ok {
		t.Fatalf("expected non-five-digit ordinal to be rejected")
	}
}

func TestSubstratePoolActorLeaseHasActiveHolderRequiresLegacyTaskCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	legacyLease := func() *coordinationv1.Lease {
		return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "actor-00000",
			Annotations: map[string]string{
				legacySubstratePoolActorLeaseTaskNSAnno:   "ns",
				legacySubstratePoolActorLeaseTaskNameAnno: "legacy-task",
				legacySubstratePoolActorLeaseTaskUIDAnno:  "task-uid",
			},
		}}
	}
	taskWithCleanup := func(uid string, phase corev1alpha1.TaskPhase, cleanup *corev1alpha1.ExecutionWorkspaceStatus) *corev1alpha1.Task {
		return &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "legacy-task", UID: types.UID(uid)},
			Status:     corev1alpha1.TaskStatus{Phase: phase, ExecutionWorkspace: cleanup},
		}
	}
	cleanupSucceeded := &corev1alpha1.ExecutionWorkspaceStatus{
		Provider:      corev1alpha1.WorkspaceProviderSubstrate,
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
		Phase:         corev1alpha1.ExecutionWorkspacePhaseDeleted,
		Reason:        corev1alpha1.ExecutionWorkspaceReasonDeleted,
	}
	cleanupFailed := &corev1alpha1.ExecutionWorkspaceStatus{
		Provider:      corev1alpha1.WorkspaceProviderSubstrate,
		CleanupPolicy: corev1alpha1.WorkspaceCleanupPolicyDelete,
		Phase:         corev1alpha1.ExecutionWorkspacePhaseFailed,
		Reason:        corev1alpha1.ExecutionWorkspaceReasonCleanupFailed,
	}
	deletingTask := taskWithCleanup("task-uid", corev1alpha1.TaskPhaseSucceeded, cleanupSucceeded)
	now := metav1.Now()
	deletingTask.DeletionTimestamp = &now
	deletingTask.Finalizers = []string{"test/cleanup"}
	cases := []struct {
		name    string
		objects []client.Object
		busy    bool
	}{
		{name: "task gone", busy: true},
		{name: "task replaced", objects: []client.Object{taskWithCleanup("other-uid", corev1alpha1.TaskPhaseSucceeded, cleanupSucceeded)}, busy: true},
		{name: "task deleting", objects: []client.Object{deletingTask}, busy: true},
		{name: "task succeeded without cleanup", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseSucceeded, nil)}, busy: true},
		{name: "task failed without cleanup", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseFailed, nil)}, busy: true},
		{name: "task cancelled without cleanup", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseCancelled, nil)}, busy: true},
		{name: "task cleanup failed", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseFailed, cleanupFailed)}, busy: true},
		{name: "task running", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseRunning, nil)}, busy: true},
		{name: "task finalizing", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseFinalizing, cleanupSucceeded)}, busy: true},
		{name: "succeeded task cleaned up", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseSucceeded, cleanupSucceeded)}, busy: false},
		{name: "failed task cleaned up", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseFailed, cleanupSucceeded)}, busy: false},
		{name: "cancelled task cleaned up", objects: []client.Object{taskWithCleanup("task-uid", corev1alpha1.TaskPhaseCancelled, cleanupSucceeded)}, busy: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			busy, err := substratePoolActorLeaseHasActiveHolder(context.Background(), reader, legacyLease())
			if err != nil {
				t.Fatalf("substratePoolActorLeaseHasActiveHolder() error = %v", err)
			}
			if busy != tc.busy {
				t.Fatalf("busy = %v, want %v", busy, tc.busy)
			}
		})
	}
}
