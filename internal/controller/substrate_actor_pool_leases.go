/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/workspace/statusrules"
)

const (
	substratePoolActorLeasePurpose        = "substrate-actor-pool-lease"
	substrateMCPToolActorLeasePurpose     = "substrate-mcp-tool-actor-lease"
	substratePoolActorLeaseActorIDLabel   = "orka.ai/substrate-pool-actor-id"
	substratePoolActorLeaseHolderUIDLabel = "orka.ai/substrate-pool-holder-uid"
	substratePoolActorLeaseToolNSAnno     = "orka.ai/substrate-pool-tool-namespace"
	substratePoolActorLeaseToolNameAnno   = "orka.ai/substrate-pool-tool-name"
	substratePoolActorLeaseToolUIDAnno    = "orka.ai/substrate-pool-tool-uid"

	// Legacy Task-held lease annotations written by controllers that still ran
	// the removed per-Task workspace path. No current code writes them; they are
	// recognized only so pre-upgrade leases can be reclaimed after the original
	// Task is terminal and workspace cleanup is confirmed.
	legacySubstratePoolActorLeaseTaskNSAnno   = "orka.ai/substrate-pool-task-namespace"
	legacySubstratePoolActorLeaseTaskNameAnno = "orka.ai/substrate-pool-task-name"
	legacySubstratePoolActorLeaseTaskUIDAnno  = "orka.ai/substrate-pool-task-uid"
)

func deterministicSubstratePoolActorPrefix(namespace, name string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(namespace) + "\x00" + strings.TrimSpace(name)))
	return fmt.Sprintf("orka-p-%s", hex.EncodeToString(sum[:])[:24])
}

func newSubstrateMCPPoolActorLease(
	tool *corev1alpha1.Tool,
	namespace string,
	name string,
	actorID string,
) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	setSubstrateMCPPoolActorLeaseHolder(lease, tool, actorID)
	return lease
}

func setSubstrateMCPPoolActorLeaseHolder(lease *coordinationv1.Lease, tool *corev1alpha1.Tool, actorID string) {
	setSubstrateMCPToolLeaseHolder(lease, tool, actorID, substratePoolActorLeasePurpose)
}

func substrateMCPToolActorLeaseName(actorID string) string {
	return strings.TrimSpace(actorID)
}

func newSubstrateMCPToolActorLease(
	tool *corev1alpha1.Tool,
	namespace string,
	actorID string,
) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      substrateMCPToolActorLeaseName(actorID),
		},
	}
	setSubstrateMCPToolActorLeaseHolder(lease, tool, actorID)
	return lease
}

func setSubstrateMCPToolActorLeaseHolder(lease *coordinationv1.Lease, tool *corev1alpha1.Tool, actorID string) {
	setSubstrateMCPToolLeaseHolder(lease, tool, actorID, substrateMCPToolActorLeasePurpose)
}

func setSubstrateMCPToolLeaseHolder(lease *coordinationv1.Lease, tool *corev1alpha1.Tool, actorID string, purpose string) {
	if lease.Labels == nil {
		lease.Labels = map[string]string{}
	}
	lease.Labels[labels.LabelManaged] = managedLabelValue
	lease.Labels[labels.LabelPurpose] = purpose
	lease.Labels[substratePoolActorLeaseActorIDLabel] = labels.SelectorValue(actorID)
	lease.Labels[substratePoolActorLeaseHolderUIDLabel] = labels.SelectorValue(string(tool.UID))
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[substratePoolActorLeaseToolNSAnno] = tool.Namespace
	lease.Annotations[substratePoolActorLeaseToolNameAnno] = tool.Name
	lease.Annotations[substratePoolActorLeaseToolUIDAnno] = string(tool.UID)
	now := metav1.NewMicroTime(time.Now())
	holder := fmt.Sprintf("tool/%s/%s/%s", tool.Namespace, tool.Name, tool.UID)
	lease.Spec.HolderIdentity = &holder
	lease.Spec.AcquireTime = &now
	lease.Spec.RenewTime = &now
}

func substratePoolActorLeaseHeldByTool(lease *coordinationv1.Lease, tool *corev1alpha1.Tool) bool {
	if lease == nil || tool == nil || lease.Annotations == nil {
		return false
	}
	if lease.Annotations[substratePoolActorLeaseToolNSAnno] != tool.Namespace ||
		lease.Annotations[substratePoolActorLeaseToolNameAnno] != tool.Name {
		return false
	}
	leaseUID := lease.Annotations[substratePoolActorLeaseToolUIDAnno]
	return leaseUID == "" || string(tool.UID) == "" || leaseUID == string(tool.UID)
}

func substrateMCPToolActorLeaseHeldByTool(lease *coordinationv1.Lease, tool *corev1alpha1.Tool) bool {
	if lease == nil || tool == nil || lease.Annotations == nil {
		return false
	}
	if lease.Annotations[substratePoolActorLeaseToolNSAnno] != tool.Namespace ||
		lease.Annotations[substratePoolActorLeaseToolNameAnno] != tool.Name {
		return false
	}
	leaseUID := lease.Annotations[substratePoolActorLeaseToolUIDAnno]
	return leaseUID == "" || string(tool.UID) == "" || leaseUID == string(tool.UID)
}

func substratePoolActorLeaseActorID(lease *coordinationv1.Lease) string {
	if lease == nil {
		return ""
	}
	if lease.Labels != nil {
		if actorID := strings.TrimSpace(lease.Labels[substratePoolActorLeaseActorIDLabel]); actorID != "" {
			return actorID
		}
	}
	return strings.TrimSpace(lease.Name)
}

func substrateMCPToolActorLeaseActorID(lease *coordinationv1.Lease) string {
	if lease == nil {
		return ""
	}
	if lease.Labels != nil {
		if actorID := strings.TrimSpace(lease.Labels[substratePoolActorLeaseActorIDLabel]); actorID != "" {
			return actorID
		}
	}
	return strings.TrimSpace(lease.Name)
}

func substratePoolActorLeaseName(actorID string) string {
	return strings.TrimSpace(actorID)
}

func substratePoolActorLeaseHasActiveHolder(ctx context.Context, reader client.Reader, lease *coordinationv1.Lease) (bool, error) {
	if lease == nil || lease.Annotations == nil {
		return true, nil
	}
	toolNamespace := strings.TrimSpace(lease.Annotations[substratePoolActorLeaseToolNSAnno])
	toolName := strings.TrimSpace(lease.Annotations[substratePoolActorLeaseToolNameAnno])
	if toolNamespace != "" && toolName != "" {
		return substratePoolActorLeaseHasActiveToolHolder(ctx, reader, toolNamespace, toolName, lease.Annotations[substratePoolActorLeaseToolUIDAnno])
	}
	taskNamespace := strings.TrimSpace(lease.Annotations[legacySubstratePoolActorLeaseTaskNSAnno])
	taskName := strings.TrimSpace(lease.Annotations[legacySubstratePoolActorLeaseTaskNameAnno])
	taskUID := strings.TrimSpace(lease.Annotations[legacySubstratePoolActorLeaseTaskUIDAnno])
	if taskNamespace != "" && taskName != "" && taskUID != "" {
		return legacySubstratePoolActorLeaseHasActiveTaskHolder(ctx, reader, taskNamespace, taskName, taskUID)
	}
	// Leases without a recognized holder fail closed as busy.
	return true, nil
}

// legacySubstratePoolActorLeaseHasActiveTaskHolder keeps pre-upgrade leases busy
// until their original Task is terminal and confirms workspace cleanup. A missing
// or replaced Task cannot prove the actor is safe to reuse.
func legacySubstratePoolActorLeaseHasActiveTaskHolder(
	ctx context.Context,
	reader client.Reader,
	taskNamespace string,
	taskName string,
	taskUID string,
) (bool, error) {
	task := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: taskNamespace, Name: taskName}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return true, err
	}
	if string(task.UID) != taskUID || !task.DeletionTimestamp.IsZero() {
		return true, nil
	}
	switch task.Status.Phase {
	case corev1alpha1.TaskPhaseSucceeded, corev1alpha1.TaskPhaseFailed, corev1alpha1.TaskPhaseCancelled:
		return !statusrules.CleanupSucceeded(task.Status.ExecutionWorkspace), nil
	default:
		return true, nil
	}
}

func substratePoolActorLeaseHasActiveToolHolder(
	ctx context.Context,
	reader client.Reader,
	toolNamespace string,
	toolName string,
	toolUID string,
) (bool, error) {
	tool := &corev1alpha1.Tool{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: toolNamespace, Name: toolName}, tool); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return true, err
	}
	toolUID = strings.TrimSpace(toolUID)
	if toolUID != "" && string(tool.UID) != toolUID {
		return true, nil
	}
	return true, nil
}

func deleteCurrentObjectPreconditions(obj client.Object) []client.DeleteOption {
	if obj == nil {
		return nil
	}
	preconditions := &metav1.Preconditions{}
	if obj.GetUID() != "" {
		uid := obj.GetUID()
		preconditions.UID = &uid
	}
	if obj.GetResourceVersion() != "" {
		resourceVersion := obj.GetResourceVersion()
		preconditions.ResourceVersion = &resourceVersion
	}
	if preconditions.UID == nil && preconditions.ResourceVersion == nil {
		return nil
	}
	return []client.DeleteOption{&client.DeleteOptions{Preconditions: preconditions}}
}

func substrateLeaseStillMatchesAfterDeleteConflict(
	ctx context.Context,
	reader client.Reader,
	lease *coordinationv1.Lease,
	matches func(*coordinationv1.Lease) bool,
) (bool, error) {
	if lease == nil || matches == nil {
		return false, nil
	}
	latest := &coordinationv1.Lease{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: lease.Namespace, Name: lease.Name}, latest); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return matches(latest), nil
}

func (r *SubstrateActorPoolReconciler) activeSubstratePoolActorLeaseCount(
	ctx context.Context,
	namespace string,
	prefix string,
	target int,
) (int, error) {
	var leases coordinationv1.LeaseList
	if err := r.List(ctx, &leases, client.InNamespace(namespace), client.MatchingLabels{
		labels.LabelPurpose: substratePoolActorLeasePurpose,
	}); err != nil {
		return 0, err
	}

	active := 0
	for i := range leases.Items {
		lease := &leases.Items[i]
		actorID := substratePoolActorLeaseActorID(lease)
		ordinal, ok := substratePoolActorOrdinalFromID(actorID, prefix)
		if !ok || ordinal < target {
			continue
		}
		busy, err := substratePoolActorLeaseHasActiveHolder(ctx, r.Client, lease)
		if err != nil {
			return active, err
		}
		if busy {
			active++
		}
	}
	return active, nil
}

func substratePoolActorOrdinalFromID(actorID string, prefix string) (int, bool) {
	actorID = strings.TrimSpace(actorID)
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	suffix, ok := strings.CutPrefix(actorID, prefix+"-")
	if !ok || len(suffix) != 5 {
		return 0, false
	}
	ordinal := 0
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		ordinal = ordinal*10 + int(ch-'0')
	}
	return ordinal, true
}

func substratePoolActorPrefixAndOrdinal(actorID string) (string, int, bool) {
	actorID = strings.TrimSpace(actorID)
	separator := strings.LastIndex(actorID, "-")
	if separator <= 0 || separator == len(actorID)-1 || len(actorID)-separator-1 != 5 {
		return "", 0, false
	}
	prefix := strings.TrimSpace(actorID[:separator])
	if prefix == "" {
		return "", 0, false
	}
	ordinal := 0
	for _, ch := range actorID[separator+1:] {
		if ch < '0' || ch > '9' {
			return "", 0, false
		}
		ordinal = ordinal*10 + int(ch-'0')
	}
	return prefix, ordinal, true
}
