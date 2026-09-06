/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/pkg/workspaceprovider"
)

const (
	// acpWorkspaceLastDetachedAnnotation records when the last Task attachment
	// was revoked. The class idleTimeout counts from this instant (or from
	// creation for a workspace that never attached).
	acpWorkspaceLastDetachedAnnotation = "acp.workspace.orka.ai/last-detached-at"
	// acpWorkspaceMaxSuspendedAnnotation freezes the class retention cap on
	// the materialized workspace so settlement enforces it without reloading
	// the execution snapshot.
	acpWorkspaceMaxSuspendedAnnotation = "acp.workspace.orka.ai/max-suspended"
	// acpWorkspaceLegacyRetentionDeadlineAnnotation stores a fixed fallback
	// cleanup deadline when the normal idle clock cannot safely bound a
	// workspace. The historical key is retained for compatibility with
	// workspaces migrated by earlier builds of the retention controller.
	acpWorkspaceLegacyRetentionDeadlineAnnotation = "acp.workspace.orka.ai/legacy-retention-deadline"
	// acpWorkspaceRetentionRequeue bounds how often retention re-evaluates a
	// workspace with no imminent deadline.
	acpWorkspaceRetentionRequeue     = 5 * time.Minute
	acpWorkspaceLegacyRetentionGrace = 24 * time.Hour
	// One class-owned Lease stores at most one pending suspended-capacity
	// claim. Its resourceVersion serializes transactions across leader handoff;
	// settled occupancy is counted from live workspaces, so the annotation is
	// constant-size regardless of the configured cap.
	acpSuspendQuotaLeaseClassUIDAnnotation         = "acp.workspace.orka.ai/suspend-quota-class-uid"
	acpSuspendQuotaLeaseClaimsAnnotation           = "acp.workspace.orka.ai/suspend-quota-claims"
	acpWorkspaceRetentionFenceIdentityLabel        = "acp.workspace.orka.ai/retention-fence-for"
	acpWorkspaceRetentionFenceNameAnnotation       = "acp.workspace.orka.ai/retention-fence-workspace"
	acpWorkspaceRetentionFenceUIDAnnotation        = "acp.workspace.orka.ai/retention-fence-workspace-uid"
	acpWorkspaceRetentionFenceSessionUIDAnnotation = "acp.workspace.orka.ai/retention-fence-session-uid"
	acpWorkspaceRetentionFenceClassUIDAnnotation   = "acp.workspace.orka.ai/retention-fence-class-uid"
	acpWorkspaceRetentionFenceActivatedAnnotation  = "acp.workspace.orka.ai/retention-fence-activated-at"
	acpTaskSessionNameField                        = "spec.sessionRef.name"
	maxACPSuspendQuotaPendingClaims                = 1
)

var errACPWorkspaceRevocationStampInvalid = errors.New("workspace revocation stamp is invalid")

// ACPWorkspaceRetentionReconciler enforces the frozen class lifetime policy on
// class-backed ACP execution workspaces: idleTimeout bounds how long an
// unattached workspace may stay Ready or Suspended, and maxLifetime is the
// hard upper bound that forces terminal cleanup regardless of state. It acts
// in Orka core's role (it writes desired state and object deletion only);
// the workspace adapter keeps executing the transitions.
type ACPWorkspaceRetentionReconciler struct {
	client.Client
	// APIReader bypasses the informer cache for suspended-capacity counts so
	// a quota claim never trusts a stale list. Falls back to Client when nil.
	APIReader client.Reader
	// DurableControlStore resolves immutable Session identities for unlinked
	// continuation Tasks. Without it, retention keeps demand fail-closed.
	DurableControlStore store.DurableControlStore
	Recorder            events.EventRecorder
	Now                 func() time.Time
}

func (r *ACPWorkspaceRetentionReconciler) quotaReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ACPWorkspaceRetentionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

//nolint:gocyclo // The retention decision table stays auditable in one place.
func (r *ACPWorkspaceRetentionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := r.Get(ctx, req.NamespacedName, workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if workspace.Labels[workspacev1alpha1.ProviderControllerLabel] != acpWorkspaceControllerLabelValue {
		return ctrl.Result{}, nil
	}
	if !workspace.DeletionTimestamp.IsZero() ||
		workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted {
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(workspace, executionWorkspaceFinalizer) {
		// The cleanup finalizer is the guarantee that an expiry delete runs
		// the linked RuntimePool teardown and records the terminal
		// disposition; deleting before the core controller installs it would
		// remove the object immediately and orphan the pool. Wait for it.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	now := r.now()
	fallbackDeadline, deadlineStamped, err := r.ensureRetentionFallbackDeadline(ctx, workspace, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deadlineStamped {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	// Inspect the deterministic retention fence before either lifetime path can
	// return. A malformed reserved Lease must not survive a maxLifetime-only
	// workspace and keep future incarnations fail-closed forever.
	retentionFence, demandCutoff, err := r.currentACPWorkspaceRetentionFence(ctx, workspace)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The lifetime deadline never bypasses idle evaluation: a class whose
	// maxLifetime is nearer than the poll interval must still apply an
	// already-elapsed idleTimeout now, so the earliest applicable deadline
	// only clamps the requeue below.
	lifetimeRequeue := acpWorkspaceRetentionRequeue
	if lifetime := workspace.Spec.Lifecycle.MaxLifetime; lifetime != nil && lifetime.Duration > 0 {
		deadline := workspace.CreationTimestamp.Add(lifetime.Duration)
		if !now.Before(deadline) {
			return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "MaxLifetimeExpired",
				"class maxLifetime elapsed; the workspace is forced into terminal cleanup", false)
		}
		if requeue := deadline.Sub(now) + time.Second; requeue < lifetimeRequeue {
			lifetimeRequeue = requeue
		}
	}
	if !fallbackDeadline.IsZero() {
		if !now.Before(fallbackDeadline) {
			return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "FallbackRetentionExpired",
				"the persisted fallback retention deadline elapsed; forcing terminal cleanup", false)
		}
		if requeue := fallbackDeadline.Sub(now) + time.Second; requeue < lifetimeRequeue {
			lifetimeRequeue = requeue
		}
	}
	releaseQuota := func(result ctrl.Result) (ctrl.Result, error) {
		if err := releaseObsoleteACPSuspendQuotaLease(ctx, r.Client, r.quotaReader(), workspace); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}
	if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined {
		return r.reconcileQuarantinedWorkspaceRetention(ctx, workspace, now, lifetimeRequeue)
	}

	if workspace.Spec.Attachment != nil || workspace.Status.AttachedEpoch > 0 {
		// A live attachment, or a revoked one whose epoch the adapter still
		// enforces, defers idle evaluation; the detach instant stamped at
		// revocation start opens a fresh idle window. spec.attachmentEpoch is
		// deliberately NOT consulted: it is the monotonic high-water mark and
		// stays positive forever after the first attachment.
		return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
	}
	if refreshed, err := refreshACPWorkspaceDetachInstantAfterEpochRelease(ctx, r.Client, workspace, now); err != nil {
		if errors.Is(err, errACPWorkspaceRevocationStampInvalid) {
			r.recordRetention(workspace, "RetentionRevocationStampInvalid",
				"the revocation-started-at annotation is invalid; idle retention is held and only maxLifetime applies")
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		return ctrl.Result{}, err
	} else if refreshed {
		// Re-read after the optimistic patch before applying idle policy. This
		// also gives Task settlement a chance to execute the frozen detach
		// action against the completed revocation.
		return releaseQuota(ctrl.Result{RequeueAfter: time.Second})
	}
	idle := workspace.Spec.Lifecycle.IdleTimeout
	if idle == nil || idle.Duration <= 0 {
		// With idle retention disabled, demand cannot change any decision
		// here (every branch below the demand lookup returns the same
		// lifetime requeue), so the uncached requester lookup and the
		// namespace-wide continuation scan are skipped entirely instead of
		// running O(workspaces x Tasks) every five-minute requeue.
		return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
	}
	if _, present := workspace.Annotations[acpWorkspaceLastDetachedAnnotation]; !present && workspace.Spec.AttachmentEpoch > 0 {
		// Older controllers could settle a suspension without recording the
		// detach instant. Start one full idle interval at the first post-upgrade
		// observation instead of expiring that retained workspace from its
		// creation time. A present but malformed stamp remains fail-closed below.
		base := workspace.DeepCopy()
		if workspace.Annotations == nil {
			workspace.Annotations = map[string]string{}
		}
		workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = now.Format(time.RFC3339Nano)
		if err := r.Patch(ctx, workspace, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		return releaseQuota(ctrl.Result{RequeueAfter: time.Second})
	}
	demandOutstanding, err := r.pendingWorkspaceDemandOutstanding(ctx, workspace, demandCutoff)
	if err != nil {
		return ctrl.Result{}, err
	}
	if demandOutstanding && retentionFence != nil {
		// A controller restart can occur after the idle-expiry fence is
		// installed but before its post-fence demand scan cancels it. Retire
		// that stale barrier before returning for live demand so the requester
		// can resume the preserved incarnation.
		if err := r.deleteACPWorkspaceRetentionFence(ctx, retentionFence); err != nil {
			return ctrl.Result{}, err
		}
	}
	if workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady && demandOutstanding {
		// A live continuation requested cold resume and has not attached yet;
		// the workspace is actively demanded even after the boot completes.
		// Observed Suspended or Suspending alone is not demand: if the requester
		// dies, idle retention resumes from the actual detach timestamp.
		return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
	}
	idleStart := workspace.CreationTimestamp.Time
	if value, present := workspace.Annotations[acpWorkspaceLastDetachedAnnotation]; present {
		raw := strings.TrimSpace(value)
		parsed, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr != nil {
			// The stamp is controller-written, so a malformed value means the
			// admission-protected metadata was corrupted. Falling back to the
			// creation time would treat a long-lived workspace as instantly
			// idle-expired and destroy or suspend it; fail closed on the
			// bounded maxLifetime path instead.
			r.recordRetention(workspace, "RetentionIdleStampInvalid",
				"the last-detached-at annotation is not RFC3339Nano; idle retention is held and only maxLifetime applies")
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		idleStart = parsed
	}
	deadline := idleStart.Add(idle.Duration)
	if now.Before(deadline) {
		requeue := min(deadline.Sub(now)+time.Second, lifetimeRequeue)
		return releaseQuota(ctrl.Result{RequeueAfter: requeue})
	}

	switch workspace.Spec.DesiredState {
	case workspacev1alpha1.ExecutionWorkspaceDesiredSuspended:
		if demandOutstanding {
			// A continuation already registered UID-bound demand (it can
			// stamp it while the suspension still settles); the retained
			// checkpoint is about to be resumed, not expired. maxLifetime
			// remains the hard bound if that requester dies.
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		// A suspended workspace past its idle timeout has exhausted its
		// retention: only terminal deletion is admitted until richer retention
		// dispositions exist.
		return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "IdleRetentionExpired",
			"class idleTimeout elapsed for the suspended workspace; retention is exhausted", true)
	case workspacev1alpha1.ExecutionWorkspaceDesiredReady:
		// Task detach overrides govern settlement only. Once an unattached
		// Ready workspace exhausts idleTimeout, the frozen class default is
		// the retention policy regardless of the last Task's choice.
		if recordedAction, present := workspace.Annotations[acpWorkspaceDetachActionAnnotation]; present &&
			recordedAction != string(workspacev1alpha1.WorkspaceOnDetachSuspend) &&
			recordedAction != string(workspacev1alpha1.WorkspaceOnDetachDelete) {
			// The controller-written settlement action is corrupt or from a
			// newer binary. Hold cleanup even though idle retention uses the
			// class default: another controller path may still consume it.
			r.recordRetention(workspace, "UnknownIdleAction",
				"class idleTimeout elapsed, but the recorded Task detach action is not executable by this controller; failing closed")
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		idleAction := string(workspace.Spec.Lifecycle.DefaultOnDetach)
		if idleAction != string(workspacev1alpha1.WorkspaceOnDetachSuspend) &&
			idleAction != string(workspacev1alpha1.WorkspaceOnDetachDelete) {
			r.recordRetention(workspace, "UnknownIdleAction",
				"class idleTimeout elapsed, but the frozen class default is not executable by this controller; failing closed")
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		if workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateFailed {
			// The adapter treats Failed as terminal and will not retry a cold
			// resume or suspension. Starting another suspension would only
			// refresh last-detached-at and extend retained data past idleTimeout.
			return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "FailedWorkspaceIdleExpired",
				"class idleTimeout elapsed for the terminally failed workspace; applying the Delete disposition", true)
		}
		if durableMarker, present := workspace.Annotations[acpWorkspaceDurableAnnotation]; idleAction == string(workspacev1alpha1.WorkspaceOnDetachSuspend) && present && durableMarker != booleanTrueValue {
			// A present durable marker is controller-authenticated. Invalid
			// content is corruption, not evidence that Delete is safe.
			r.recordRetention(workspace, "InvalidDurableCapability",
				"class idleTimeout elapsed, but the durable-workspace marker is invalid; failing closed")
			return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
		}
		if idleAction == string(workspacev1alpha1.WorkspaceOnDetachSuspend) &&
			runtimePoolWorkspaceSuspendableAnnotationPresent(workspace) &&
			strings.TrimSpace(workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation]) == "" {
			// A materialized RuntimePool is not proof that this incarnation
			// contains a durable session. Match Task settlement and delete an
			// empty workspace rather than suspending it for a later continuation.
			return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "UncommittedWorkspaceIdleExpired",
				"class idleTimeout elapsed before a durable RuntimeSession checkpoint was committed; deleting the empty workspace incarnation", true)
		}
		if idleAction == string(workspacev1alpha1.WorkspaceOnDetachSuspend) &&
			runtimePoolWorkspaceSuspendableAnnotationPresent(workspace) {
			_, resumeUnfulfilled := workspace.Annotations[acpWorkspaceResumeRequestedAnnotation]
			preserveLastDetached := resumeUnfulfilled &&
				workspace.Annotations[acpWorkspaceResumedLineageAnnotation] == booleanTrueValue
			err := suspendACPWorkspaceWithinQuota(
				ctx, r.Client, r.quotaReader(), workspace, now, preserveLastDetached, "", 0,
			)
			switch {
			case errors.Is(err, errACPSuspendQuotaExhausted):
				// Quota exhaustion does not authorize replacing the class
				// default Suspend action with Delete. Keep the workspace Ready
				// and retry; maxLifetime remains the independent hard bound.
				r.recordRetention(workspace, "SuspendQuotaExhausted",
					"class idleTimeout elapsed and the retention cap is exhausted; the class Suspend action remains pending")
				return ctrl.Result{RequeueAfter: lifetimeRequeue}, nil
			case errors.Is(err, errACPSuspendQuotaBusy), apierrors.IsConflict(err), apierrors.IsNotFound(err):
				return ctrl.Result{RequeueAfter: time.Second}, nil
			case err != nil:
				return ctrl.Result{}, err
			}
			r.recordRetention(workspace, "IdleSuspended", "class idleTimeout elapsed; applying the class default Suspend action")
			metrics.RecordACPWorkspaceRetentionAction("suspend", "idle_timeout")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "IdleExpired",
			"class idleTimeout elapsed for the unattached workspace; applying the class Delete disposition", true)
	default:
		return releaseQuota(ctrl.Result{RequeueAfter: lifetimeRequeue})
	}
}

func (r *ACPWorkspaceRetentionReconciler) reconcileQuarantinedWorkspaceRetention(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	now time.Time,
	lifetimeRequeue time.Duration,
) (ctrl.Result, error) {
	// Quarantine is terminal and never reusable, but it can still carry the
	// deterministic Session workspace name. Apply idleTimeout from the
	// revocation/detach instant so an idle-only class eventually enters the
	// finalizer-backed cleanup path instead of retaining the record forever.
	idle := workspace.Spec.Lifecycle.IdleTimeout
	if idle == nil || idle.Duration <= 0 {
		if err := releaseObsoleteACPSuspendQuotaLease(ctx, r.Client, r.quotaReader(), workspace); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: lifetimeRequeue}, nil
	}
	idleStart := workspace.CreationTimestamp.Time
	if value, present := workspace.Annotations[acpWorkspaceLastDetachedAnnotation]; present {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
		if err != nil {
			r.recordRetention(workspace, "RetentionIdleStampInvalid",
				"the last-detached-at annotation is not RFC3339Nano; quarantined cleanup is held and only maxLifetime applies")
			if releaseErr := releaseObsoleteACPSuspendQuotaLease(ctx, r.Client, r.quotaReader(), workspace); releaseErr != nil {
				return ctrl.Result{}, releaseErr
			}
			return ctrl.Result{RequeueAfter: lifetimeRequeue}, nil
		}
		idleStart = parsed
	}
	deadline := idleStart.Add(idle.Duration)
	if now.Before(deadline) {
		if err := releaseObsoleteACPSuspendQuotaLease(ctx, r.Client, r.quotaReader(), workspace); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: min(deadline.Sub(now)+time.Second, lifetimeRequeue)}, nil
	}
	return ctrl.Result{}, r.expireWorkspace(ctx, workspace, "QuarantineIdleExpired",
		"class idleTimeout elapsed for the terminal quarantined workspace; forcing terminal cleanup", false)
}

// ensureRetentionFallbackDeadline gives workspaces whose normal idle clock is
// unavailable one bounded migration window from their first observation. This
// covers legacy unbounded retention, corrupted controller-owned idle metadata,
// and old quota-capped bindings that lack the maxLifetime now required for
// bounded suspension deferral.
func (r *ACPWorkspaceRetentionReconciler) ensureRetentionFallbackDeadline(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	now time.Time,
) (time.Time, bool, error) {
	if workspace.Spec.Lifecycle.MaxLifetime != nil && workspace.Spec.Lifecycle.MaxLifetime.Duration > 0 {
		return time.Time{}, false, nil
	}
	reason, required := acpWorkspaceRetentionFallbackReason(workspace)
	if !required {
		return time.Time{}, false, nil
	}
	if raw, present := workspace.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation]; present {
		if deadline, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
			return deadline, false, nil
		}
	}

	deadline := now.Add(acpWorkspaceLegacyRetentionGrace).UTC()
	base := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[acpWorkspaceLegacyRetentionDeadlineAnnotation] = deadline.Format(time.RFC3339Nano)
	if err := r.Patch(ctx, workspace, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return time.Time{}, true, nil
		}
		return time.Time{}, false, err
	}
	r.recordRetention(workspace, "FallbackRetentionBounded",
		reason+"; terminal cleanup is scheduled after a 24-hour migration grace period")
	return deadline, true, nil
}

func acpWorkspaceRetentionFallbackReason(workspace *workspacev1alpha1.ExecutionWorkspace) (string, bool) {
	idle := workspace.Spec.Lifecycle.IdleTimeout
	if raw, present := workspace.Annotations[acpWorkspaceRevocationStartedAnnotation]; present {
		if _, _, ok := parseACPWorkspaceRevocationStamp(raw); !ok {
			return "the protected revocation stamp is invalid", true
		}
	}
	if idle != nil && idle.Duration > 0 {
		if raw, present := workspace.Annotations[acpWorkspaceResumeRequestedAnnotation]; present {
			if _, valid := parseACPWorkspaceResumeRequestStamp(raw); !valid {
				return "the protected pending-demand stamp is invalid", true
			}
		}
		if raw, present := workspace.Annotations[acpWorkspaceLastDetachedAnnotation]; present {
			if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err != nil {
				return "the protected idle stamp is invalid", true
			}
		}
		if workspace.Spec.Lifecycle.DefaultOnDetach == workspacev1alpha1.WorkspaceOnDetachSuspend {
			if _, capped := workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation]; capped {
				return "a legacy quota-capped Suspend binding has no maxLifetime", true
			}
		}
		return "", false
	}
	if durable, present := workspace.Annotations[acpWorkspaceDurableAnnotation]; present && durable != booleanTrueValue {
		return "the protected durable-workspace marker is invalid", true
	}
	if slices.Contains(workspace.Spec.Lifecycle.AllowedOnDetach, workspacev1alpha1.WorkspaceOnDetachSuspend) &&
		runtimePoolWorkspaceMayContainDurableData(workspace) {
		return "a legacy suspend-capable workspace has no frozen expiry", true
	}
	return "", false
}

// refreshACPWorkspaceDetachInstantAfterEpochRelease replaces the provisional
// revocation-start timestamp once the adapter no longer enforces the epoch.
// Comparing the two stamps makes the update one-shot while retaining the
// revocation marker until Task settlement applies the frozen detach action.
func refreshACPWorkspaceDetachInstantAfterEpochRelease(
	ctx context.Context,
	writer client.Client,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	now time.Time,
) (bool, error) {
	rawStamp, present := workspace.Annotations[acpWorkspaceRevocationStartedAnnotation]
	if !present {
		return false, nil
	}
	stampedEpoch, revocationStartedAt, ok := parseACPWorkspaceRevocationStamp(
		rawStamp,
	)
	if !ok {
		return false, errACPWorkspaceRevocationStampInvalid
	}
	if stampedEpoch != workspace.Spec.AttachmentEpoch {
		return false, nil
	}
	detachedAt, err := time.Parse(time.RFC3339Nano, workspace.Annotations[acpWorkspaceLastDetachedAnnotation])
	if err != nil || !detachedAt.Equal(revocationStartedAt) {
		return false, nil
	}
	refreshedAt := now.UTC()
	if !refreshedAt.After(revocationStartedAt) {
		refreshedAt = revocationStartedAt.Add(time.Nanosecond)
	}
	base := workspace.DeepCopy()
	workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = refreshedAt.Format(time.RFC3339Nano)
	if err := writer.Patch(ctx, workspace, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return true, nil
		}
		return true, err
	}
	return true, nil
}

// runtimePoolWorkspaceSuspendableAnnotationPresent reports whether the
// materialized workspace carries frozen evidence that its class profile
// permitted DataOnly suspension. The detach action remains accepted for
// workspaces created before the durable marker was introduced.
func runtimePoolWorkspaceSuspendableAnnotationPresent(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	durable, present := workspace.Annotations[acpWorkspaceDurableAnnotation]
	if present {
		return durable == booleanTrueValue
	}
	return workspace.Spec.Lifecycle.DefaultOnDetach == workspacev1alpha1.WorkspaceOnDetachSuspend ||
		workspace.Annotations[acpWorkspaceDetachActionAnnotation] == string(workspacev1alpha1.WorkspaceOnDetachSuspend)
}

// runtimePoolWorkspaceMayContainDurableData is the fail-closed retention and
// quota predicate. A present malformed controller-owned durable marker is
// possible preservation evidence until the adapter proves data absence or
// terminal cleanup.
func runtimePoolWorkspaceMayContainDurableData(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	if workspace == nil || workspace.Annotations[acpWorkspaceDurableDataAbsentAnnotation] == booleanTrueValue {
		return false
	}
	if _, present := workspace.Annotations[acpWorkspaceDurableAnnotation]; present {
		return true
	}
	return runtimePoolWorkspaceSuspendableAnnotationPresent(workspace)
}

func (r *ACPWorkspaceRetentionReconciler) expireWorkspace(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	reason, message string,
	fenced bool,
) error {
	var retentionFence *coordinationv1.Lease
	if fenced && workspace.Spec.SessionRef != nil {
		var err error
		retentionFence, err = r.ensureACPWorkspaceRetentionFence(ctx, workspace)
		if err != nil {
			return err
		}
		if retentionFence != nil {
			// Fence creation is the idle-expiry linearization point. Re-scan
			// through the uncached reader after it exists so every continuation
			// admitted before the fence wins and cancels deletion. A Task born
			// later is ordered after expiry and may start a fresh incarnation.
			activatedAt, activationErr := acpWorkspaceRetentionFenceActivation(retentionFence)
			if activationErr != nil {
				return activationErr
			}
			demandOutstanding, demandErr := r.pendingWorkspaceDemandOutstanding(ctx, workspace, &activatedAt)
			if demandErr != nil {
				return demandErr
			}
			if demandOutstanding {
				return r.deleteACPWorkspaceRetentionFence(ctx, retentionFence)
			}
		}
	}
	// Idle-triggered deletions are fenced with UID+resourceVersion so a
	// concurrent attachment or resume settles as a retried conflict instead
	// of destroying a workspace that became actively demanded; the
	// maxLifetime hard bound stays intentionally unconditional.
	preconditions := []client.DeleteOption{client.Preconditions{UID: &workspace.UID}}
	if fenced {
		preconditions = deleteCurrentObjectPreconditions(workspace)
	}
	if err := r.Delete(ctx, workspace, preconditions...); err != nil {
		if apierrors.IsNotFound(err) {
			// Another owner already completed deletion. Retention applied no
			// action, so do not emit an Event or increment the action metric.
			return nil
		}
		if fenced && apierrors.IsConflict(err) {
			// The fenced deletion lost to a concurrent update (an attachment
			// or resume made the workspace actively demanded); nothing was
			// applied, so no Event or action metric is recorded. Cancel the
			// expiry fence too so the winning Task remains admissible.
			if retentionFence != nil {
				return r.deleteACPWorkspaceRetentionFence(ctx, retentionFence)
			}
			return nil
		}
		return err
	}
	r.recordRetention(workspace, reason, message)
	metrics.RecordACPWorkspaceRetentionAction("delete", strings.ToLower(reason))
	return nil
}

func (r *ACPWorkspaceRetentionReconciler) ensureACPWorkspaceRetentionFence(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (*coordinationv1.Lease, error) {
	control, err := r.acpWorkspaceRetentionControl(ctx, workspace)
	if err != nil || control == nil {
		return nil, err
	}

	key := types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      acpWorkspaceRetentionFenceLeaseName(workspace),
	}
	lease := &coordinationv1.Lease{}
	err = r.quotaReader().Get(ctx, key, lease)
	if err == nil {
		return r.activateACPWorkspaceRetentionFence(ctx, lease, workspace, control)
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read workspace retention fence Lease: %w", err)
	}

	lease = newACPWorkspaceRetentionFence(workspace, control)
	if err := r.Create(ctx, lease); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create workspace retention fence Lease: %w", err)
		}
		if getErr := r.quotaReader().Get(ctx, key, lease); getErr != nil {
			return nil, fmt.Errorf("read concurrently created workspace retention fence Lease: %w", getErr)
		}
		return r.activateACPWorkspaceRetentionFence(ctx, lease, workspace, control)
	}
	if _, err := r.ensureACPWorkspaceRetentionFenceActivation(ctx, lease); err != nil {
		return nil, err
	}
	return lease, nil
}

func (r *ACPWorkspaceRetentionReconciler) acpWorkspaceRetentionControl(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (*corev1alpha1.RuntimeSessionControl, error) {
	if workspace == nil || workspace.Spec.SessionRef == nil {
		return nil, nil
	}
	sessionName := strings.TrimSpace(workspace.Spec.SessionRef.Name)
	sessionUID := strings.TrimSpace(string(workspace.Spec.SessionRef.UID))
	classUID := strings.TrimSpace(string(workspace.Spec.ClassBinding.UID))
	if sessionName == "" || sessionUID == "" || classUID == "" || workspace.UID == "" {
		return nil, fmt.Errorf("session workspace retention fence requires complete workspace, class, and Session identities")
	}

	control := &corev1alpha1.RuntimeSessionControl{}
	controlKey := types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      storekube.RuntimeSessionControlObjectName(sessionName),
	}
	if err := r.quotaReader().Get(ctx, controlKey, control); err != nil {
		if apierrors.IsNotFound(err) {
			// A deleted Session cannot admit another continuation for this
			// immutable UID, so no surviving admission fence is needed.
			return nil, nil
		}
		return nil, fmt.Errorf("read RuntimeSessionControl for workspace retention fence: %w", err)
	}
	if control.UID == "" || control.Spec.SessionName != sessionName {
		return nil, fmt.Errorf("RuntimeSessionControl identity does not match the retained workspace Session")
	}
	if control.Spec.SessionUID != sessionUID {
		// A same-name replacement Session cannot admit a continuation for the
		// workspace's immutable Session UID, so it leaves no surviving Task
		// admission path to fence.
		return nil, nil
	}
	return control, nil
}

func newACPWorkspaceRetentionFence(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	control *corev1alpha1.RuntimeSessionControl,
) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: workspace.Namespace,
		Name:      acpWorkspaceRetentionFenceLeaseName(workspace),
		Labels: map[string]string{
			acpWorkspaceRetentionFenceIdentityLabel: labels.SelectorValue(workspace.Name),
		},
		Annotations: map[string]string{
			acpWorkspaceRetentionFenceNameAnnotation:       workspace.Name,
			acpWorkspaceRetentionFenceUIDAnnotation:        string(workspace.UID),
			acpWorkspaceRetentionFenceSessionUIDAnnotation: string(workspace.Spec.SessionRef.UID),
			acpWorkspaceRetentionFenceClassUIDAnnotation:   string(workspace.Spec.ClassBinding.UID),
		},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: corev1alpha1.GroupVersion.String(),
			Kind:       "RuntimeSessionControl",
			Name:       control.Name,
			UID:        control.UID,
		}},
	}}
}

func (r *ACPWorkspaceRetentionReconciler) activateACPWorkspaceRetentionFence(
	ctx context.Context,
	lease *coordinationv1.Lease,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	control *corev1alpha1.RuntimeSessionControl,
) (*coordinationv1.Lease, error) {
	if err := validateACPWorkspaceRetentionFence(lease, workspace, control); err != nil {
		return r.replaceACPWorkspaceRetentionFence(ctx, lease, workspace, control)
	}
	if lease.Annotations[acpWorkspaceRetentionFenceUIDAnnotation] == string(workspace.UID) {
		if _, err := r.ensureACPWorkspaceRetentionFenceActivation(ctx, lease); err != nil {
			return nil, err
		}
		return lease, nil
	}
	// Keep the deterministic logical-workspace name, but recreate the Lease for
	// each workspace incarnation. Its API-server creationTimestamp is the
	// linearization point shared with Task creationTimestamp; patching the old
	// Lease would retain an earlier cutoff and could miss new demand.
	return r.replaceACPWorkspaceRetentionFence(ctx, lease, workspace, control)
}

func (r *ACPWorkspaceRetentionReconciler) replaceACPWorkspaceRetentionFence(
	ctx context.Context,
	lease *coordinationv1.Lease,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	control *corev1alpha1.RuntimeSessionControl,
) (*coordinationv1.Lease, error) {
	if err := r.deleteACPWorkspaceRetentionFence(ctx, lease); err != nil {
		return nil, err
	}
	replacement := newACPWorkspaceRetentionFence(workspace, control)
	if err := r.Create(ctx, replacement); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("recreate workspace retention fence Lease: %w", err)
		}
		if getErr := r.quotaReader().Get(ctx, client.ObjectKeyFromObject(replacement), replacement); getErr != nil {
			return nil, fmt.Errorf("read concurrently recreated workspace retention fence Lease: %w", getErr)
		}
		return r.activateACPWorkspaceRetentionFence(ctx, replacement, workspace, control)
	}
	if _, err := r.ensureACPWorkspaceRetentionFenceActivation(ctx, replacement); err != nil {
		return nil, err
	}
	return replacement, nil
}

func (r *ACPWorkspaceRetentionReconciler) ensureACPWorkspaceRetentionFenceActivation(
	ctx context.Context,
	lease *coordinationv1.Lease,
) (time.Time, error) {
	if lease == nil || lease.CreationTimestamp.IsZero() {
		return time.Time{}, fmt.Errorf("workspace retention fence has no API-server creation timestamp")
	}
	activatedAt := lease.CreationTimestamp.UTC()
	canonical := activatedAt.Format(time.RFC3339Nano)
	if raw := strings.TrimSpace(lease.Annotations[acpWorkspaceRetentionFenceActivatedAnnotation]); raw == canonical {
		return activatedAt, nil
	}
	// The API server owns creationTimestamp, so it is safe to canonicalize old,
	// missing, or controller-clock activation values to that shared ordering
	// point. Tasks and the fence are then compared on one clock.
	base := lease.DeepCopy()
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[acpWorkspaceRetentionFenceActivatedAnnotation] = canonical
	if err := r.Patch(ctx, lease, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return time.Time{}, fmt.Errorf("canonicalize workspace retention fence activation: %w", err)
	}
	return activatedAt, nil
}

func acpWorkspaceRetentionFenceActivation(lease *coordinationv1.Lease) (time.Time, error) {
	if lease == nil || lease.CreationTimestamp.IsZero() {
		return time.Time{}, fmt.Errorf("workspace retention fence has no API-server creation timestamp")
	}
	return lease.CreationTimestamp.UTC(), nil
}

func validateACPWorkspaceRetentionFence(
	lease *coordinationv1.Lease,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	control *corev1alpha1.RuntimeSessionControl,
) error {
	if lease == nil || workspace == nil || workspace.Spec.SessionRef == nil || control == nil {
		return fmt.Errorf("workspace retention fence validation requires complete objects")
	}
	if !strings.HasPrefix(lease.Name, labels.ACPWorkspaceRetentionFenceLeaseNamePrefix) ||
		lease.Labels[acpWorkspaceRetentionFenceIdentityLabel] != labels.SelectorValue(workspace.Name) ||
		lease.Annotations[acpWorkspaceRetentionFenceNameAnnotation] != workspace.Name ||
		strings.TrimSpace(lease.Annotations[acpWorkspaceRetentionFenceUIDAnnotation]) == "" ||
		lease.Annotations[acpWorkspaceRetentionFenceSessionUIDAnnotation] != string(workspace.Spec.SessionRef.UID) ||
		lease.Annotations[acpWorkspaceRetentionFenceClassUIDAnnotation] != string(workspace.Spec.ClassBinding.UID) {
		return fmt.Errorf("workspace retention fence Lease does not match the logical Session workspace")
	}
	for _, owner := range lease.OwnerReferences {
		if owner.APIVersion == corev1alpha1.GroupVersion.String() && owner.Kind == "RuntimeSessionControl" &&
			owner.Name == control.Name && owner.UID == control.UID {
			return nil
		}
	}
	return fmt.Errorf("workspace retention fence Lease is not owned by the exact RuntimeSessionControl")
}

func (r *ACPWorkspaceRetentionReconciler) deleteACPWorkspaceRetentionFence(
	ctx context.Context,
	lease *coordinationv1.Lease,
) error {
	if lease == nil {
		return nil
	}
	if err := r.Delete(ctx, lease, deleteCurrentObjectPreconditions(lease)...); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete workspace retention fence Lease: %w", err)
	}
	return nil
}

func (r *ACPWorkspaceRetentionReconciler) currentACPWorkspaceRetentionFence(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (*coordinationv1.Lease, *time.Time, error) {
	if workspace == nil || workspace.Spec.SessionRef == nil || workspace.UID == "" {
		return nil, nil, nil
	}
	lease := &coordinationv1.Lease{}
	err := r.quotaReader().Get(ctx, types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      acpWorkspaceRetentionFenceLeaseName(workspace),
	}, lease)
	if apierrors.IsNotFound(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read current workspace retention fence Lease: %w", err)
	}
	control, err := r.acpWorkspaceRetentionControl(ctx, workspace)
	if err != nil {
		return nil, nil, err
	}
	if control == nil || validateACPWorkspaceRetentionFence(lease, workspace, control) != nil {
		// This deterministic name is reserved for the logical workspace. Remove
		// malformed or incorrectly owned pre-upgrade objects with exact object
		// preconditions so a concurrent valid replacement wins and the next
		// reconcile re-reads it.
		if err := r.deleteACPWorkspaceRetentionFence(ctx, lease); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	fencedWorkspaceUID := strings.TrimSpace(lease.Annotations[acpWorkspaceRetentionFenceUIDAnnotation])
	if fencedWorkspaceUID != string(workspace.UID) {
		return nil, nil, nil
	}
	activatedAt, err := r.ensureACPWorkspaceRetentionFenceActivation(ctx, lease)
	if err != nil {
		return nil, nil, err
	}
	return lease, &activatedAt, nil
}

func acpWorkspaceRetentionFenceLeaseName(workspace *workspacev1alpha1.ExecutionWorkspace) string {
	identity := workspace.Namespace + "\x00" + workspace.Name
	sum := sha256.Sum256([]byte(identity))
	return labels.ACPWorkspaceRetentionFenceLeaseNamePrefix + hex.EncodeToString(sum[:12])
}

func (r *TaskReconciler) taskBlockedByACPWorkspaceRetentionFence(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *ACPRuntimeWorkspaceBinding,
	workspaceName string,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) (bool, error) {
	if task == nil || binding == nil || binding.Class == nil ||
		binding.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
		return false, nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	fences := &coordinationv1.LeaseList{}
	if err := reader.List(ctx, fences,
		client.InNamespace(task.Namespace),
		client.MatchingLabels{acpWorkspaceRetentionFenceIdentityLabel: labels.SelectorValue(workspaceName)},
	); err != nil {
		return false, fmt.Errorf("list workspace retention fences: %w", err)
	}
	for i := range fences.Items {
		fence := &fences.Items[i]
		if !strings.HasPrefix(fence.Name, labels.ACPWorkspaceRetentionFenceLeaseNamePrefix) {
			continue
		}
		fencedWorkspaceUID := strings.TrimSpace(fence.Annotations[acpWorkspaceRetentionFenceUIDAnnotation])
		if fence.Annotations[acpWorkspaceRetentionFenceNameAnnotation] != workspaceName ||
			fence.Annotations[acpWorkspaceRetentionFenceSessionUIDAnnotation] != binding.SessionUID ||
			fence.Annotations[acpWorkspaceRetentionFenceClassUIDAnnotation] != binding.Class.UID ||
			fencedWorkspaceUID == "" {
			return false, fmt.Errorf("%w: workspace retention fence metadata is invalid", errACPWorkspaceBindingConflict)
		}
		if workspace != nil && string(workspace.UID) == fencedWorkspaceUID {
			// The retention reconciler creates the barrier before its final
			// uncached demand scan. Tasks visible to that scan cancel expiry;
			// later Tasks wait without mutating the old incarnation until cleanup
			// exposes an absent or replacement UID.
			return true, nil
		}
	}
	return false, nil
}

func (r *ACPWorkspaceRetentionReconciler) recordRetention(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	reason, message string,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(workspace, nil, corev1.EventTypeNormal, reason, "Retention", "%s", message)
}

type acpSuspendQuotaLockEntry struct {
	mutex      sync.Mutex
	references int
}

// acpSuspendQuotaLocks avoids redundant Lease contention inside one process.
// The Kubernetes Lease below fences claims across leader handoff. Entries
// remain only while a holder or waiter references them, so deleted and
// recreated classes do not accumulate process-local locks.
var (
	acpSuspendQuotaLocksMu sync.Mutex
	acpSuspendQuotaLocks   = map[string]*acpSuspendQuotaLockEntry{}
)

// errACPSuspendQuotaExhausted reports a claim rejected by the frozen class
// retention cap; callers reject admission or keep the frozen Suspend action
// pending until capacity opens.
var errACPSuspendQuotaExhausted = errors.New("class suspended-workspace retention cap is exhausted")

// errACPSuspendQuotaBusy asks the caller to retry after another workspace's
// Kubernetes-backed count-and-patch transaction finishes or is recovered.
var errACPSuspendQuotaBusy = errors.New("class suspended-workspace quota claim is in progress")

func lockACPSuspendQuota(namespace string, classUID types.UID) func() {
	key := namespace + "/" + string(classUID)
	acpSuspendQuotaLocksMu.Lock()
	entry := acpSuspendQuotaLocks[key]
	if entry == nil {
		entry = &acpSuspendQuotaLockEntry{}
		acpSuspendQuotaLocks[key] = entry
	}
	entry.references++
	acpSuspendQuotaLocksMu.Unlock()

	entry.mutex.Lock()
	return func() {
		entry.mutex.Unlock()
		acpSuspendQuotaLocksMu.Lock()
		entry.references--
		if entry.references == 0 {
			delete(acpSuspendQuotaLocks, key)
		}
		acpSuspendQuotaLocksMu.Unlock()
	}
}

func acpSuspendQuotaLeaseName(classUID types.UID) string {
	sum := sha256.Sum256([]byte(classUID))
	return labels.ACPSuspendQuotaLeaseNamePrefix + hex.EncodeToString(sum[:12])
}

type acpSuspendQuotaClaim struct {
	WorkspaceName   string `json:"workspaceName"`
	ResourceVersion string `json:"resourceVersion"`
}

type acpSuspendQuotaClaims map[string]acpSuspendQuotaClaim

func newACPSuspendQuotaLease(
	workspace *workspacev1alpha1.ExecutionWorkspace,
	claims acpSuspendQuotaClaims,
) (*coordinationv1.Lease, error) {
	if workspace == nil || workspace.UID == "" || workspace.Spec.ClassBinding.Name == "" ||
		workspace.Spec.ClassBinding.UID == "" || workspace.ResourceVersion == "" {
		return nil, errors.New("workspace, class, and resourceVersion identities are required for a suspension quota claim")
	}
	controller := true
	holder := string(workspace.Spec.ClassBinding.UID)
	now := metav1.NewMicroTime(time.Now().UTC())
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workspace.Namespace,
			Name:      acpSuspendQuotaLeaseName(workspace.Spec.ClassBinding.UID),
			Annotations: map[string]string{
				acpSuspendQuotaLeaseClassUIDAnnotation: string(workspace.Spec.ClassBinding.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: workspacev1alpha1.GroupVersion.String(),
				Kind:       "ExecutionWorkspaceClass",
				Name:       workspace.Spec.ClassBinding.Name,
				UID:        workspace.Spec.ClassBinding.UID,
				Controller: &controller,
			}},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder,
			AcquireTime:    &now,
			RenewTime:      &now,
		},
	}
	if err := setACPSuspendQuotaClaims(lease, claims); err != nil {
		return nil, err
	}
	return lease, nil
}

func readACPSuspendQuotaClaims(
	lease *coordinationv1.Lease,
	className string,
	classUID types.UID,
) (acpSuspendQuotaClaims, error) {
	if lease == nil || lease.Annotations == nil ||
		lease.Annotations[acpSuspendQuotaLeaseClassUIDAnnotation] != string(classUID) {
		return nil, errors.New("suspension quota Lease has invalid class identity")
	}
	owner := metav1.GetControllerOf(lease)
	if owner == nil || owner.APIVersion != workspacev1alpha1.GroupVersion.String() ||
		owner.Kind != "ExecutionWorkspaceClass" || owner.Name != className || owner.UID != classUID {
		return nil, errors.New("suspension quota Lease has invalid class ownership")
	}
	claims := acpSuspendQuotaClaims{}
	if err := json.Unmarshal([]byte(lease.Annotations[acpSuspendQuotaLeaseClaimsAnnotation]), &claims); err != nil {
		return nil, fmt.Errorf("decode suspension quota claims: %w", err)
	}
	if len(claims) > maxACPSuspendQuotaPendingClaims {
		return nil, errors.New("suspension quota Lease carries more than one pending workspace claim")
	}
	for uid, claim := range claims {
		if strings.TrimSpace(uid) == "" || strings.TrimSpace(claim.WorkspaceName) == "" ||
			strings.TrimSpace(claim.ResourceVersion) == "" {
			return nil, errors.New("suspension quota Lease has an invalid workspace claim")
		}
	}
	return claims, nil
}

func setACPSuspendQuotaClaims(lease *coordinationv1.Lease, claims acpSuspendQuotaClaims) error {
	if len(claims) > maxACPSuspendQuotaPendingClaims {
		return errors.New("suspension quota Lease cannot store more than one pending workspace claim")
	}
	encoded, err := json.Marshal(claims)
	if err != nil {
		return fmt.Errorf("encode suspension quota claims: %w", err)
	}
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[acpSuspendQuotaLeaseClaimsAnnotation] = string(encoded)
	return nil
}

func listACPSuspendQuotaWorkspaces(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	classUID types.UID,
) (map[string]*workspacev1alpha1.ExecutionWorkspace, error) {
	list := &workspacev1alpha1.ExecutionWorkspaceList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list execution workspaces for retention accounting: %w", err)
	}
	workspaces := make(map[string]*workspacev1alpha1.ExecutionWorkspace, len(list.Items))
	for i := range list.Items {
		workspace := &list.Items[i]
		if workspace.Labels[workspacev1alpha1.ProviderControllerLabel] != acpWorkspaceControllerLabelValue ||
			workspace.Spec.ClassBinding.UID != classUID || workspace.UID == "" {
			continue
		}
		workspaces[string(workspace.UID)] = workspace.DeepCopy()
	}
	return workspaces, nil
}

func normalizeACPSuspendQuotaClaims(
	claims acpSuspendQuotaClaims,
	workspaces map[string]*workspacev1alpha1.ExecutionWorkspace,
) (acpSuspendQuotaClaims, map[string]struct{}, bool) {
	normalized := make(acpSuspendQuotaClaims, len(claims))
	occupied := make(map[string]struct{}, len(workspaces))
	for uid, workspace := range workspaces {
		if workspaceConsumesSuspendedQuota(workspace) {
			occupied[uid] = struct{}{}
		}
	}
	changed := false
	for uid, claim := range claims {
		workspace := workspaces[uid]
		keep := workspace != nil && claim.WorkspaceName == workspace.Name &&
			!workspaceConsumesSuspendedQuota(workspace) && workspace.DeletionTimestamp.IsZero() &&
			workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady &&
			claim.ResourceVersion == workspace.ResourceVersion
		if !keep {
			changed = true
			continue
		}
		normalized[uid] = claim
		occupied[uid] = struct{}{}
	}
	return normalized, occupied, changed
}

func patchACPSuspendQuotaClaims(
	ctx context.Context,
	writer client.Client,
	lease *coordinationv1.Lease,
	claims acpSuspendQuotaClaims,
) error {
	base := lease.DeepCopy()
	if err := setACPSuspendQuotaClaims(lease, claims); err != nil {
		return err
	}
	now := metav1.NewMicroTime(time.Now().UTC())
	lease.Spec.RenewTime = &now
	if err := writer.Patch(ctx, lease, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return errACPSuspendQuotaBusy
		}
		return fmt.Errorf("patch suspension quota Lease: %w", err)
	}
	return nil
}

func createACPSuspendQuotaClaimLease(
	ctx context.Context,
	writer client.Client,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	limit int32,
	workspaces map[string]*workspacev1alpha1.ExecutionWorkspace,
) error {
	occupied := 0
	for uid, candidate := range workspaces {
		if uid != string(workspace.UID) && workspaceConsumesSuspendedQuota(candidate) {
			occupied++
		}
	}
	if occupied >= int(limit) {
		return errACPSuspendQuotaExhausted
	}
	claims := acpSuspendQuotaClaims{
		string(workspace.UID): {
			WorkspaceName:   workspace.Name,
			ResourceVersion: workspace.ResourceVersion,
		},
	}
	desired, err := newACPSuspendQuotaLease(workspace, claims)
	if err != nil {
		return err
	}
	if err := writer.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errACPSuspendQuotaBusy
		}
		return fmt.Errorf("create suspension quota Lease: %w", err)
	}
	return nil
}

func claimACPSuspendQuotaSlot(
	ctx context.Context,
	writer client.Client,
	reader client.Reader,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	limit int32,
) error {
	if limit <= 0 {
		return errACPSuspendQuotaExhausted
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      acpSuspendQuotaLeaseName(workspace.Spec.ClassBinding.UID),
	}
	err := reader.Get(ctx, key, lease)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("read suspension quota Lease: %w", err)
	}
	// Read the durable transaction fence before taking the authoritative
	// workspace snapshot. Any claim or release that races the list advances
	// the Lease resourceVersion, so the final optimistic write retries instead
	// of committing against stale occupancy from another leader.
	workspaces, listErr := listACPSuspendQuotaWorkspaces(
		ctx, reader, workspace.Namespace, workspace.Spec.ClassBinding.UID,
	)
	if listErr != nil {
		return listErr
	}
	if apierrors.IsNotFound(err) {
		return createACPSuspendQuotaClaimLease(ctx, writer, workspace, limit, workspaces)
	}
	claims, err := readACPSuspendQuotaClaims(
		lease, workspace.Spec.ClassBinding.Name, workspace.Spec.ClassBinding.UID,
	)
	if err != nil {
		// The name is reserved for this class transaction fence. Replace a
		// malformed pre-upgrade or tenant-created object only if its exact UID
		// and resourceVersion still match the authoritative read. A concurrent
		// valid replacement wins and forces this claimant to retry.
		if deleteErr := writer.Delete(ctx, lease, deleteCurrentObjectPreconditions(lease)...); deleteErr != nil {
			if apierrors.IsConflict(deleteErr) || apierrors.IsNotFound(deleteErr) {
				return errACPSuspendQuotaBusy
			}
			return fmt.Errorf("delete malformed suspension quota Lease: %w", deleteErr)
		}
		return createACPSuspendQuotaClaimLease(ctx, writer, workspace, limit, workspaces)
	}
	claims, occupied, changed := normalizeACPSuspendQuotaClaims(claims, workspaces)
	uid := string(workspace.UID)
	delete(occupied, uid)
	desiredClaim := acpSuspendQuotaClaim{
		WorkspaceName:   workspace.Name,
		ResourceVersion: workspace.ResourceVersion,
	}
	currentClaim, claimed := claims[uid]
	if !claimed && len(occupied) >= int(limit) {
		if changed {
			if err := patchACPSuspendQuotaClaims(ctx, writer, lease, claims); err != nil {
				return err
			}
		}
		return errACPSuspendQuotaExhausted
	}
	if !claimed && len(claims) > 0 {
		if changed {
			if err := patchACPSuspendQuotaClaims(ctx, writer, lease, claims); err != nil {
				return err
			}
		}
		return errACPSuspendQuotaBusy
	}
	if !claimed || currentClaim != desiredClaim {
		claims[uid] = desiredClaim
		changed = true
	}
	if !changed {
		return nil
	}
	return patchACPSuspendQuotaClaims(ctx, writer, lease, claims)
}

func releaseObsoleteACPSuspendQuotaLease(
	ctx context.Context,
	writer client.Client,
	reader client.Reader,
	workspace *workspacev1alpha1.ExecutionWorkspace,
) error {
	if workspace == nil || acpWorkspaceSuspendedCapFromAnnotation(workspace) == nil ||
		workspace.Spec.ClassBinding.Name == "" || workspace.Spec.ClassBinding.UID == "" || workspace.UID == "" {
		return nil
	}
	unlock := lockACPSuspendQuota(workspace.Namespace, workspace.Spec.ClassBinding.UID)
	defer unlock()

	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{
		Namespace: workspace.Namespace,
		Name:      acpSuspendQuotaLeaseName(workspace.Spec.ClassBinding.UID),
	}
	if err := reader.Get(ctx, key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read suspension quota Lease for recovery: %w", err)
	}
	claims, err := readACPSuspendQuotaClaims(
		lease, workspace.Spec.ClassBinding.Name, workspace.Spec.ClassBinding.UID,
	)
	if err != nil {
		return err
	}
	uid := string(workspace.UID)
	claim, claimed := claims[uid]
	if !claimed {
		return nil
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	workspaceKey := client.ObjectKeyFromObject(workspace)
	if err := reader.Get(ctx, workspaceKey, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read execution workspace for suspension quota recovery: %w", err)
		}
		current = nil
	}
	if current != nil && current.UID == workspace.UID &&
		!workspaceConsumesSuspendedQuota(current) && current.DeletionTimestamp.IsZero() &&
		current.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady &&
		claim.WorkspaceName == current.Name && claim.ResourceVersion == current.ResourceVersion {
		// The workspace write recorded by this pending claim can still succeed.
		// Keep the slot reserved so a replacement leader can finish it.
		return nil
	}
	delete(claims, uid)
	return patchACPSuspendQuotaClaims(ctx, writer, lease, claims)
}

// suspendACPWorkspaceWithinQuota atomically claims one suspension slot under
// the workspace's frozen retention cap and patches it to Suspended, stamping
// the detach instant so the suspended state earns a full retention interval.
// It returns errACPSuspendQuotaExhausted when the cap is already consumed and
// passes patch conflicts through for the caller's requeue policy.
func suspendACPWorkspaceWithinQuota(
	ctx context.Context,
	writer client.Client,
	reader client.Reader,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	now time.Time,
	preserveLastDetached bool,
	settledTaskUID string,
	settledTaskEpoch int64,
) error {
	if settledTaskUID != "" &&
		!acpWorkspaceResumeDemandBelongsToTask(
			workspace.Annotations[acpWorkspaceResumeRequestedAnnotation], settledTaskUID,
		) && acpWorkspaceSettlementReceiptCoversTask(
		workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation], settledTaskUID, settledTaskEpoch,
	) {
		// This settlement already landed or a later attachment displaced it.
		// Do not reserve quota or suspend newer Ready state.
		return nil
	}
	unlock := lockACPSuspendQuota(workspace.Namespace, workspace.Spec.ClassBinding.UID)
	defer unlock()
	limit := acpWorkspaceSuspendedCapFromAnnotation(workspace)
	if limit != nil {
		if err := claimACPSuspendQuotaSlot(ctx, writer, reader, workspace, *limit); err != nil {
			return err
		}
	}
	base := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	if !preserveLastDetached {
		workspace.Annotations[acpWorkspaceLastDetachedAnnotation] = now.UTC().Format(time.RFC3339Nano)
	}
	delete(workspace.Annotations, acpWorkspaceRevocationStartedAnnotation)
	if settledTaskUID != "" {
		// The settlement receipt lands in the SAME patch as the suspension:
		// if the controller dies before the separate Task-side marker patch,
		// a restarted reconcile of that Task finds its receipt here and
		// completes the marker instead of re-applying Suspend to newer
		// session state.
		workspace.Annotations[acpWorkspaceLastSettledTaskAnnotation] =
			formatACPWorkspaceSettlementReceipt(settledTaskUID, settledTaskEpoch)
	}
	// Suspension settles any pending provisioning or resume demand; a later
	// continuation stamps fresh demand when it flips the workspace back.
	delete(workspace.Annotations, acpWorkspaceResumeRequestedAnnotation)
	workspace.Spec.DesiredState = workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	return writer.Patch(ctx, workspace, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

func workspaceConsumesSuspendedQuota(workspace *workspacev1alpha1.ExecutionWorkspace) bool {
	if workspace == nil {
		return false
	}
	durableDataAbsent := workspace.Annotations[acpWorkspaceDurableDataAbsentAnnotation] == booleanTrueValue
	mayContainDurableData := runtimePoolWorkspaceMayContainDurableData(workspace)
	if workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateFailed &&
		!mayContainDurableData {
		return false
	}
	suspendedCharge := workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredSuspended
	coldResumeCharge := workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredReady &&
		(workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspended ||
			workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateSuspending) &&
		!durableDataAbsent
	failedDurableCharge := workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateFailed &&
		mayContainDurableData
	deletedCleanupProven := workspace.Status.State == workspacev1alpha1.ExecutionWorkspaceStateDeleted &&
		workspaceprovider.ValidateInteractiveDeletedDisposition(
			workspace.Status.Disposition, workspace.Spec.Lifecycle.DeletionPolicy,
		) == nil
	maintenanceCharge := mayContainDurableData &&
		((workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined &&
			workspace.Status.State != workspacev1alpha1.ExecutionWorkspaceStateQuarantined) ||
			(workspace.Spec.DesiredState == workspacev1alpha1.ExecutionWorkspaceDesiredDeleted &&
				!deletedCleanupProven))
	deletingCharge := !workspace.DeletionTimestamp.IsZero() &&
		mayContainDurableData &&
		!deletedCleanupProven
	return suspendedCharge || coldResumeCharge || failedDurableCharge || maintenanceCharge || deletingCharge
}

// countSuspendedClassWorkspaces counts suspended workspaces bound to the exact
// class UID in one namespace, skipping candidates the exclude predicate
// accepts.
func countSuspendedClassWorkspaces(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	classUID types.UID,
	exclude func(*workspacev1alpha1.ExecutionWorkspace) bool,
) (int, error) {
	list := &workspacev1alpha1.ExecutionWorkspaceList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return 0, fmt.Errorf("list execution workspaces for retention accounting: %w", err)
	}
	count := 0
	for i := range list.Items {
		workspace := &list.Items[i]
		if workspace.Labels[workspacev1alpha1.ProviderControllerLabel] != acpWorkspaceControllerLabelValue ||
			workspace.Spec.ClassBinding.UID != classUID || (exclude != nil && exclude(workspace)) {
			continue
		}
		if workspaceConsumesSuspendedQuota(workspace) {
			count++
		}
	}
	return count, nil
}

// pendingWorkspaceDemandOutstanding reports whether the demand record on the
// workspace still has a live requester. Current records bind the requesting
// Task's immutable UID. Legacy records without that identity fall through to
// the exact Session continuation scan below.
func (r *ACPWorkspaceRetentionReconciler) pendingWorkspaceDemandOutstanding(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	createdNotAfter *time.Time,
) (bool, error) {
	if outstanding, err := r.recordedWorkspaceDemandLive(ctx, workspace, createdNotAfter); err != nil || outstanding {
		return outstanding, err
	}
	// The single UID-bound stamp records only the LAST writer: when several
	// Tasks queue for the same suspended Session workspace, a later requester
	// overwrites an earlier one, and the recorded requester terminating must
	// not surrender the workspace while another live continuation still
	// waits. Any live, non-terminal Task on the workspace's Session keeps
	// demand outstanding; maxLifetime remains the hard bound.
	return r.liveSessionContinuationExists(ctx, workspace, createdNotAfter)
}

// recordedWorkspaceDemandLive reports whether the current UID-bound demand
// stamp names a live, non-terminal requester.
func (r *ACPWorkspaceRetentionReconciler) recordedWorkspaceDemandLive(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	createdNotAfter *time.Time,
) (bool, error) {
	value, present := workspace.Annotations[acpWorkspaceResumeRequestedAnnotation]
	if !present {
		return false, nil
	}
	fields, valid := parseACPWorkspaceResumeRequestStamp(value)
	if !valid {
		// A malformed controller-owned stamp is not safe evidence that demand
		// ended. Fail closed while the persisted fallback deadline bounds the
		// corruption when the class has no maxLifetime.
		return true, nil
	}
	if len(fields) < 3 {
		// Legacy controllers recorded either only the timestamp or the timestamp
		// plus a mutable Task name. Neither format proves requester identity, so
		// resolve demand through the exact Session continuation scan below.
		return false, nil
	}
	task := &corev1alpha1.Task{}
	err := r.quotaReader().Get(ctx, types.NamespacedName{Namespace: workspace.Namespace, Name: fields[1]}, task)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if len(fields) >= 3 && string(task.UID) != fields[2] {
		// A replacement Task under the recycled namespace/name is not the
		// requester; its unrelated lifetime must not keep stale demand alive.
		return false, nil
	}
	if taskCreatedAfter(task, createdNotAfter) {
		return false, nil
	}
	if !task.DeletionTimestamp.IsZero() ||
		task.Status.Phase == corev1alpha1.TaskPhaseSucceeded ||
		task.Status.Phase == corev1alpha1.TaskPhaseFailed ||
		task.Status.Phase == corev1alpha1.TaskPhaseCancelled {
		// The requester can never attach; its settlement (or deletion
		// settlement) owns any remaining cleanup.
		return false, nil
	}
	return true, nil
}

func parseACPWorkspaceResumeRequestStamp(value string) ([]string, bool) {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return nil, false
	}
	if _, err := time.Parse(time.RFC3339Nano, fields[0]); err != nil {
		return nil, false
	}
	return fields, true
}

// liveSessionContinuationExists reports whether any live, non-terminal Task
// targets the workspace's Session. The Task CRD exposes sessionRef.name as a
// selectable field, so the uncached read remains bounded to that Session.
func (r *ACPWorkspaceRetentionReconciler) liveSessionContinuationExists(
	ctx context.Context,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	createdNotAfter *time.Time,
) (bool, error) {
	candidates, err := liveACPSessionContinuations(ctx, r.quotaReader(), workspace, "")
	if err != nil {
		return true, err
	}
	for _, task := range candidates {
		if taskCreatedAfter(task, createdNotAfter) {
			continue
		}
		linked := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel]) == workspace.Name &&
			strings.TrimSpace(task.Annotations[acpExecutionWorkspaceUIDAnnotation]) == string(workspace.UID)
		if linked {
			return true, nil
		}
		if r.DurableControlStore == nil {
			// Without the immutable Session lookup, retain the existing
			// fail-closed behavior for an otherwise eligible waiter.
			return true, nil
		}
		control, controlErr := r.DurableControlStore.GetSessionControl(
			ctx, task.Namespace, strings.TrimSpace(task.Spec.SessionRef.Name),
		)
		switch {
		case errors.Is(controlErr, store.ErrNotFound):
			continue
		case controlErr != nil:
			return true, controlErr
		case control == nil:
			return true, fmt.Errorf("session control lookup returned no identity for %s/%s", task.Namespace, task.Spec.SessionRef.Name)
		case strings.TrimSpace(control.SessionUID) == string(workspace.Spec.SessionRef.UID):
			return true, nil
		}
	}
	return false, nil
}

func taskCreatedAfter(task *corev1alpha1.Task, cutoff *time.Time) bool {
	return task != nil && cutoff != nil && !task.CreationTimestamp.IsZero() && task.CreationTimestamp.After(*cutoff)
}

// liveACPSessionContinuations returns every live, non-terminal continuation
// Task targeting this exact workspace incarnation, in list order. It fails
// closed (error, treated as demand outstanding by callers) on list errors.
// Returning ALL candidates lets settlement scan past one ineligible waiter
// (an out-of-policy override, a recreated Session) instead of concluding no
// successor exists while a later valid continuation is queued.
func liveACPSessionContinuations(
	ctx context.Context,
	reader client.Reader,
	workspace *workspacev1alpha1.ExecutionWorkspace,
	excludeTaskUID types.UID,
) ([]*corev1alpha1.Task, error) {
	if workspace.Spec.SessionRef == nil || strings.TrimSpace(workspace.Spec.SessionRef.Name) == "" {
		return nil, nil
	}
	sessionName := strings.TrimSpace(workspace.Spec.SessionRef.Name)
	tasks := &corev1alpha1.TaskList{}
	if err := reader.List(ctx, tasks,
		client.InNamespace(workspace.Namespace),
		client.MatchingFields{acpTaskSessionNameField: sessionName},
	); err != nil {
		return nil, err
	}
	// The class-UID verification for unlinked candidates is resolved once: a
	// class deleted and recreated under the same name carries a different
	// immutable UID, and its Tasks resolve a NEW workspace incarnation, so
	// they are never demand for this one.
	classIdentityChecked := false
	classIdentityMatches := false
	var candidates []*corev1alpha1.Task
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if excludeTaskUID != "" && task.UID == excludeTaskUID {
			continue
		}
		if task.Spec.SessionRef == nil || strings.TrimSpace(task.Spec.SessionRef.Name) != sessionName {
			continue
		}
		// Demand binds to this workspace INCARNATION, not the Session name: a
		// Session deleted and recreated under the same namespace/name
		// resolves a different immutable Session UID, and its Tasks can never
		// attach here. A waiter that has been reconciled carries the
		// controller-written workspace link (name plus incarnation UID);
		// verification rejects cross-incarnation links before they are ever
		// stamped, so a Task linked elsewhere is never demand for this
		// workspace. A not-yet-linked waiter counts until its first
		// reconcile either links it here or fails it terminally.
		linkName := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel])
		linkUID := strings.TrimSpace(task.Annotations[acpExecutionWorkspaceUIDAnnotation])
		exactIncarnation := linkName == workspace.Name && linkUID == string(workspace.UID)
		if !exactIncarnation && linkName != "" {
			continue
		}
		if !exactIncarnation {
			// An unlinked Task counts only when it actually requests a
			// session-reused execution workspace: a plain transcript-backed
			// Task sharing the Session name can never attach here, and
			// counting it would suppress idle retention (and hold quota) for
			// its entire lifetime.
			if task.Spec.Execution == nil || task.Spec.Execution.Workspace == nil ||
				task.Spec.Execution.Workspace.ClassRef == nil ||
				task.Spec.Execution.Workspace.ClassRef.Name != workspace.Spec.ClassBinding.Name ||
				task.Spec.Execution.Workspace.ReusePolicy != corev1alpha1.WorkspaceReusePolicySession {
				// Different classes deliberately produce separate workspace
				// incarnations, and the legacy enabled-workspace path never
				// binds a class workspace: a Task that cannot resolve to
				// THIS workspace must not defer its settlement or hold its
				// retention.
				continue
			}
			if !classIdentityChecked {
				classIdentityChecked = true
				class := &workspacev1alpha1.ExecutionWorkspaceClass{}
				err := reader.Get(ctx, types.NamespacedName{
					Namespace: workspace.Namespace, Name: workspace.Spec.ClassBinding.Name,
				}, class)
				switch {
				case apierrors.IsNotFound(err):
					// The frozen class is gone: name-matched waiters resolve
					// nothing (or a future recreation's NEW incarnation).
				case err != nil:
					return nil, err
				default:
					classIdentityMatches = class.UID == workspace.Spec.ClassBinding.UID
				}
			}
			if !classIdentityMatches {
				// The class was deleted and recreated under the same name:
				// this waiter's classRef resolves the REPLACEMENT class and a
				// different workspace incarnation, so it must not suppress
				// this workspace's settlement or idle retention.
				continue
			}
		}
		if !task.DeletionTimestamp.IsZero() ||
			task.Status.Phase == corev1alpha1.TaskPhaseSucceeded ||
			task.Status.Phase == corev1alpha1.TaskPhaseFailed ||
			task.Status.Phase == corev1alpha1.TaskPhaseCancelled {
			continue
		}
		candidates = append(candidates, task)
	}
	return candidates, nil
}

// acpWorkspaceSuspendedCapFromAnnotation parses the frozen retention cap
// recorded on the materialized workspace, or nil when unbounded.
func acpWorkspaceSuspendedCapFromAnnotation(workspace *workspacev1alpha1.ExecutionWorkspace) *int32 {
	value, present := workspace.Annotations[acpWorkspaceMaxSuspendedAnnotation]
	if !present {
		// Absent means the class froze no cap: retention is unbounded by
		// design.
		return nil
	}
	raw := strings.TrimSpace(value)
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed < 0 {
		// A present-but-invalid frozen value fails closed as an exhausted cap
		// (zero) instead of silently disabling the class's hard quota.
		zero := int32(0)
		return &zero
	}
	limit := int32(parsed)
	return &limit
}

// SetupWithManager registers retention enforcement for ACP class workspaces.
func (r *ACPWorkspaceRetentionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ours := predicate.NewPredicateFuncs(func(object client.Object) bool {
		return object.GetLabels()[workspacev1alpha1.ProviderControllerLabel] == acpWorkspaceControllerLabelValue
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&workspacev1alpha1.ExecutionWorkspace{}).
		WithEventFilter(ours).
		Named("acp-workspace-retention").
		Complete(r)
}
