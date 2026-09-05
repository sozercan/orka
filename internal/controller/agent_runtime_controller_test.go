package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/harness/harnesstest"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	v2conformance "github.com/orka-agents/orka/internal/harness/v2/conformance"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const agentRuntimeV1TestBearer = "0123456789abcdef0123456789abcdef"

func TestAgentRuntimeReconcilerMarksStrictV2RuntimeReady(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready {
		t.Fatalf("Ready = false, message=%q", updated.Status.Message)
	}
	if !slices.Contains(updated.Finalizers, agentRuntimeFinalizer) {
		t.Fatalf("Ready AgentRuntime finalizers = %v, want %q", updated.Finalizers, agentRuntimeFinalizer)
	}
	if updated.Status.ObservedCapabilities == nil {
		t.Fatal("ObservedCapabilities is nil")
	}
	observed := updated.Status.ObservedCapabilities
	if observed.ProtocolVersion != harnessv2.ProtocolVersion || observed.Transport != "http+ndjson" {
		t.Fatalf("observed protocol = %#v", observed)
	}
	if observed.RuntimeInstanceID != string(config.RuntimeInstanceID) || observed.SupervisorBootID != string(config.SupervisorBootID) {
		t.Fatalf("observed exact instance = %#v", observed)
	}
	if observed.RuntimeProfileDigest != runtimeObject.Spec.Capabilities.Profile.Digest || observed.Limits == nil ||
		observed.Limits.MaxConcurrentPrompts != int32(limits.MaxConcurrentPrompts) {
		t.Fatalf("observed profile/limits = %#v", observed)
	}
	if observed.WorkspaceGovernance == nil || !observed.WorkspaceGovernance.Strict() {
		t.Fatalf("observed strict governance = %#v", observed.WorkspaceGovernance)
	}
	if updated.Status.ObservedControllerAuthRefResourceVersion == "" || updated.Status.ObservedOperationCapabilityRefResourceVersion == "" {
		t.Fatalf("observed auth versions = %#v", updated.Status)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, agentRuntimeReadyCondition)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != agentRuntimeReasonReady {
		t.Fatalf("Ready condition = %#v", condition)
	}
	counts := server.Counts()
	// Workspace validation and running cancellation use separate sessions.
	if counts.PromptStarts != 2 || counts.PromptCancels != 2 || counts.SessionDeletes != 2 || counts.WorkspaceDeltas != 1 {
		t.Fatalf("hostile conformance counts = %#v", counts)
	}
}

func TestAgentRuntimeReconcilerRejectsDuplicatePoolIdentityWithoutDisruptingOwner(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	owner, ownerSecret := testAgentRuntimeAndSecret(t, server.URL(), config)
	contender, contenderSecret := duplicateAgentRuntimeTestRegistration(owner, ownerSecret, "runtime-duplicate")
	reconciler := newAgentRuntimeUnitReconciler(t, owner, ownerSecret, contender, contenderSecret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(owner)); err != nil {
		t.Fatalf("reconcile owner: %v", err)
	}
	readyOwner := getAgentRuntime(t, reconciler, owner)
	if !readyOwner.Status.Ready || readyOwner.Status.ObservedCapabilities == nil {
		t.Fatalf("owner status = %#v", readyOwner.Status)
	}

	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(contender)); err != nil {
		t.Fatalf("reconcile contender: %v", err)
	}
	rejectedContender := getAgentRuntime(t, reconciler, contender)
	assertAgentRuntimePoolIdentityRejected(t, rejectedContender, owner.Name)
	assertAgentRuntimeNoCleanupFinalizer(t, rejectedContender)
	assertAgentRuntimeMCPPreAuth(t, reconciler, string(config.RuntimePoolUID), config.ControllerBearerToken)

	// Simulate a duplicate status published by an older controller, then prove
	// that reconciliation clears it instead of retaining its complete identity.
	legacyDuplicate := getAgentRuntime(t, reconciler, contender)
	legacyDuplicate.Status = readyOwner.DeepCopy().Status
	if err := reconciler.Status().Update(t.Context(), &legacyDuplicate); err != nil {
		t.Fatalf("seed legacy duplicate status: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(contender)); err != nil {
		t.Fatalf("reconcile legacy duplicate: %v", err)
	}
	rejectedContender = getAgentRuntime(t, reconciler, contender)
	assertAgentRuntimePoolIdentityRejected(t, rejectedContender, owner.Name)
	assertAgentRuntimeNoCleanupFinalizer(t, rejectedContender)
	assertAgentRuntimeMCPPreAuth(t, reconciler, string(config.RuntimePoolUID), config.ControllerBearerToken)

	// NotReady does not release a retained identity. Existing sessions continue
	// to pre-authenticate against it, and a contender must remain blocked.
	notReadyOwner := getAgentRuntime(t, reconciler, owner)
	notReadyOwner.Status.Ready = false
	if err := reconciler.Status().Update(t.Context(), &notReadyOwner); err != nil {
		t.Fatalf("mark owner NotReady: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(contender)); err != nil {
		t.Fatalf("reconcile contender against NotReady owner: %v", err)
	}
	rejectedContender = getAgentRuntime(t, reconciler, contender)
	assertAgentRuntimePoolIdentityRejected(t, rejectedContender, owner.Name)
	assertAgentRuntimeNoCleanupFinalizer(t, rejectedContender)
	assertAgentRuntimeMCPPreAuth(t, reconciler, string(config.RuntimePoolUID), config.ControllerBearerToken)

	if err := reconciler.Delete(t.Context(), &notReadyOwner); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(contender)); err != nil {
		t.Fatalf("reconcile contender while owner is deleting: %v", err)
	}
	rejectedContender = getAgentRuntime(t, reconciler, contender)
	assertAgentRuntimePoolIdentityRejected(t, rejectedContender, owner.Name)
	assertAgentRuntimeNoCleanupFinalizer(t, rejectedContender)
	deletingOwner := getAgentRuntime(t, reconciler, owner)
	if deletingOwner.DeletionTimestamp.IsZero() || !slices.Contains(deletingOwner.Finalizers, agentRuntimeFinalizer) {
		t.Fatalf("deleting owner metadata = %#v", deletingOwner.ObjectMeta)
	}

	deletingOwner.Finalizers = slices.DeleteFunc(deletingOwner.Finalizers, func(value string) bool {
		return value == agentRuntimeFinalizer || value == agentRuntimeSecretGCFinalizer
	})
	if err := reconciler.Update(t.Context(), &deletingOwner); err != nil {
		t.Fatalf("complete owner cleanup: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(contender)); err != nil {
		t.Fatalf("reconcile contender after owner cleanup: %v", err)
	}
	newOwner := getAgentRuntime(t, reconciler, contender)
	if !newOwner.Status.Ready || newOwner.Status.ObservedCapabilities == nil ||
		newOwner.Status.ObservedCapabilities.RuntimePoolUID != string(config.RuntimePoolUID) {
		t.Fatalf("contender did not acquire released pool identity: %#v", newOwner.Status)
	}
}

//nolint:gocyclo // The drain, Task proof, delayed projection, and deletion checks form one lifecycle.
func TestAgentRuntimeReconcilerDeletionDrainsBeforeRemovingFinalizer(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	fence := harnessv2.Fence{
		RuntimeInstanceID:          config.RuntimeInstanceID,
		SupervisorBootID:           config.SupervisorBootID,
		ControllerEpoch:            1,
		RuntimePoolUID:             config.RuntimePoolUID,
		RuntimePoolGeneration:      7,
		RuntimeProfileDigest:       profileDigest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	server := newAgentRuntimeDeletionTestServer(config.ControllerBearerToken, config.OperationCapabilitySecret, fence)
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL, config)
	runtimeObject.UID = types.UID("runtime-uid")
	runtimeObject.Generation = 2
	secret.UID = types.UID("runtime-auth-uid")
	secret.ResourceVersion = "1"
	matchingTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "matching-task")
	matchingTask.Status.Execution.RuntimeSessionSupervisorBootID = string(config.SupervisorBootID)
	queuedTask := testAgentRuntimeCleanupTask(t, runtimeObject, "", "queued-task")
	queuedTask.Status.Execution.RuntimeSessionUID = ""
	queuedTask.Status.Execution.RuntimeSessionGeneration = 0
	staleAuthorityTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "stale-authority-task")
	staleAuthorityTask.Status.Execution.RuntimeSessionSupervisorBootID = "other-supervisor-boot"
	cleanedBootTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "cleaned-boot-task")
	cleanedBootTask.Status.Execution.RuntimeSessionSupervisorBootID = "previous-supervisor-boot"
	cleanedBootDigest, err := taskScopedRuntimeSessionCleanupDigest(
		cleanedBootTask.UID, cleanedBootTask.Status.Execution.Attempt,
		cleanedBootTask.Status.Execution.RuntimeInstanceID, cleanedBootTask.Status.Execution.RuntimeSessionUID,
		cleanedBootTask.Status.Execution.RuntimeSessionGeneration,
	)
	if err != nil {
		t.Fatalf("build prior-boot AgentRuntime Task cleanup receipt: %v", err)
	}
	cleanedBootTask.Status.Execution.RuntimeSessionCleanupDigest = cleanedBootDigest
	unrelatedTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "unrelated-task")
	unrelatedTask.Status.AgentExecutionBinding.RuntimeRef.UID = "other-runtime-uid"
	unrelatedTask.Status.Execution.AgentRuntimeUID = "other-runtime-uid"
	historicalTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "historical-task")
	historicalTask.Status.AgentExecutionBinding.RuntimeRef.Generation = 1
	historicalTask.Status.AgentExecutionBinding.BindingDigest, err = canonicalAgentExecutionBindingDigest(
		*historicalTask.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatalf("canonicalize historical AgentRuntime Task binding: %v", err)
	}
	historicalCleanupDigest, err := taskScopedRuntimeSessionCleanupDigest(
		historicalTask.UID, historicalTask.Status.Execution.Attempt,
		historicalTask.Status.Execution.RuntimeInstanceID, historicalTask.Status.Execution.RuntimeSessionUID,
		historicalTask.Status.Execution.RuntimeSessionGeneration,
	)
	if err != nil {
		t.Fatalf("build historical AgentRuntime Task cleanup receipt: %v", err)
	}
	historicalTask.Status.Execution.RuntimeSessionCleanupDigest = historicalCleanupDigest
	reconciler := newAgentRuntimeUnitReconciler(
		t, runtimeObject, secret, matchingTask, queuedTask, staleAuthorityTask, cleanedBootTask, unrelatedTask, historicalTask,
	)
	allowAgentRuntimeLoopback(t)
	seedDeletingAgentRuntime(t, reconciler, runtimeObject, secret, fence)

	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err == nil ||
		!strings.Contains(err.Error(), "request authenticated AgentRuntime deletion drain") {
		t.Fatalf("first deletion reconcile error = %v, want injected drain failure", err)
	}
	assertAgentRuntimeCleanupFinalizer(t, getAgentRuntime(t, reconciler, runtimeObject))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
	if err != nil {
		t.Fatalf("retry deletion drain: %v", err)
	}
	if result.RequeueAfter != agentRuntimeDeleteRequeue {
		t.Fatalf("drain retry result = %#v", result)
	}
	operationIDs := server.DrainOperationIDs()
	wantOperationID := harnessv2.OperationID("agent-runtime-delete-drain-g7")
	if len(operationIDs) != 2 || operationIDs[0] != wantOperationID || operationIDs[1] != wantOperationID {
		t.Fatalf("drain operation IDs = %v, want two %q attempts", operationIDs, wantOperationID)
	}
	assertAgentRuntimeCleanupFinalizer(t, getAgentRuntime(t, reconciler, runtimeObject))

	result, err = reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
	if err != nil {
		t.Fatalf("observe resident session after drain: %v", err)
	}
	if result.RequeueAfter != agentRuntimeDeleteRequeue {
		t.Fatalf("resident-session result = %#v", result)
	}
	assertAgentRuntimeCleanupFinalizer(t, getAgentRuntime(t, reconciler, runtimeObject))

	result, err = reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
	if err != nil {
		t.Fatalf("record quiescent drain cleanup: %v", err)
	}
	if result.RequeueAfter != agentRuntimeDeleteRequeue {
		t.Fatalf("uncertified Task result = %#v, want deletion requeue", result)
	}
	completedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(matchingTask), completedTask); err != nil {
		t.Fatalf("get Task after AgentRuntime drain: %v", err)
	}
	if !taskHasAgentRuntimeDrainCleanupProofForUID(completedTask, completedTask.UID) {
		t.Fatalf("matching Task cleanup receipt = %q, want drained completion proof", completedTask.Status.Execution.RuntimeSessionCleanupDigest)
	}
	queued := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(queuedTask), queued); err != nil {
		t.Fatalf("get queued Task after AgentRuntime drain: %v", err)
	}
	if !taskHasAgentRuntimeDrainCleanupProofForUID(queued, queued.UID) {
		t.Fatalf("queued Task drain proof = %q, want durable proof", queued.Status.Execution.RuntimeSessionCleanupDigest)
	}
	stale := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(staleAuthorityTask), stale); err != nil {
		t.Fatalf("get stale-authority Task after AgentRuntime drain: %v", err)
	}
	if stale.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf("stale-authority Task cleanup receipt = %q, want fail-closed empty receipt", stale.Status.Execution.RuntimeSessionCleanupDigest)
	}
	cleanedBoot := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(cleanedBootTask), cleanedBoot); err != nil {
		t.Fatalf("get prior-boot Task after AgentRuntime drain: %v", err)
	}
	if cleanedBoot.Status.Execution.RuntimeSessionCleanupDigest != cleanedBootDigest ||
		!taskScopedRuntimeSessionCleanupCompleteForUID(cleanedBoot, cleanedBoot.UID) {
		t.Fatalf("prior-boot Task cleanup receipt = %q, want preserved exact receipt", cleanedBoot.Status.Execution.RuntimeSessionCleanupDigest)
	}
	historical := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(historicalTask), historical); err != nil {
		t.Fatalf("get historical Task after AgentRuntime drain: %v", err)
	}
	if historical.Status.Execution.RuntimeSessionCleanupDigest != historicalCleanupDigest ||
		!taskScopedRuntimeSessionCleanupCompleteForUID(historical, historical.UID) {
		t.Fatalf("historical Task cleanup receipt = %q, want preserved exact receipt", historical.Status.Execution.RuntimeSessionCleanupDigest)
	}
	deleting := getAgentRuntime(t, reconciler, runtimeObject)
	assertAgentRuntimeCleanupFinalizer(t, deleting)

	completedTask.Status.Execution.RuntimeSessionGeneration++
	if err := reconciler.Status().Update(t.Context(), completedTask); err != nil {
		t.Fatalf("project rotated matching Task RuntimeSession generation: %v", err)
	}
	stale.Status.Execution.RuntimeSessionSupervisorBootID = string(config.SupervisorBootID)
	if err := reconciler.Status().Update(t.Context(), stale); err != nil {
		t.Fatalf("restore stale-authority Task cleanup authority: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("complete quiescent deletion after Task cleanup: %v", err)
	}
	var deleted corev1alpha1.AgentRuntime
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(runtimeObject), &deleted); !apierrors.IsNotFound(err) {
		t.Fatalf("AgentRuntime after quiescent deletion Get() error = %v, object=%#v", err, deleted.ObjectMeta)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(queuedTask), queued); err != nil {
		t.Fatalf("reload queued Task after AgentRuntime deletion: %v", err)
	}
	queued.Status.Execution.RuntimeInstanceID = string(config.RuntimeInstanceID)
	queued.Status.Execution.RuntimeSessionUID = "queued-task-session"
	queued.Status.Execution.RuntimeSessionGeneration = 1
	if err := reconciler.Status().Update(t.Context(), queued); err != nil {
		t.Fatalf("project delayed queued Task RuntimeSession identity: %v", err)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(queuedTask), queued); err != nil {
		t.Fatalf("reload delayed queued Task identity: %v", err)
	}
	dispatcher := &ACPDispatcher{Client: reconciler.Client, APIReader: reconciler.APIReader}
	ready, err := dispatcher.cleanupRecoveredTaskScopedRuntimeSession(t.Context(), queued)
	if err != nil || !ready {
		t.Fatalf("recover delayed Task from AgentRuntime drain proof = ready %t, error %v", ready, err)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(queuedTask), queued); err != nil {
		t.Fatalf("reload recovered delayed Task: %v", err)
	}
	if !taskScopedRuntimeSessionCleanupComplete(queued) {
		t.Fatalf("delayed Task cleanup receipt = %q, want drain completion proof", queued.Status.Execution.RuntimeSessionCleanupDigest)
	}
	if !taskHasAgentRuntimeDrainCleanupProofForUID(queued, queued.UID) {
		t.Fatalf("delayed Task cleanup receipt = %q, want preserved drain proof", queued.Status.Execution.RuntimeSessionCleanupDigest)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(matchingTask), completedTask); err != nil {
		t.Fatalf("reload rotated matching Task after AgentRuntime deletion: %v", err)
	}
	ready, err = dispatcher.cleanupRecoveredTaskScopedRuntimeSession(t.Context(), completedTask)
	if err != nil || !ready {
		t.Fatalf("recover rotated Task from AgentRuntime drain proof = ready %t, error %v", ready, err)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(matchingTask), completedTask); err != nil {
		t.Fatalf("reload recovered rotated Task: %v", err)
	}
	if !taskScopedRuntimeSessionCleanupComplete(completedTask) {
		t.Fatalf("rotated Task cleanup receipt = %q, want drain completion proof", completedTask.Status.Execution.RuntimeSessionCleanupDigest)
	}
	if !taskHasAgentRuntimeDrainCleanupProofForUID(completedTask, completedTask.UID) {
		t.Fatalf("rotated Task cleanup receipt = %q, want preserved drain proof", completedTask.Status.Execution.RuntimeSessionCleanupDigest)
	}
	completedTask.Status.Execution.RuntimeSessionGeneration++
	if err := reconciler.Status().Update(t.Context(), completedTask); err != nil {
		t.Fatalf("project post-recovery Task RuntimeSession generation: %v", err)
	}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(matchingTask), completedTask); err != nil {
		t.Fatalf("reload post-recovery rotated Task: %v", err)
	}
	ready, err = dispatcher.cleanupRecoveredTaskScopedRuntimeSession(t.Context(), completedTask)
	if err != nil || !ready {
		t.Fatalf("recover post-proof Task generation = ready %t, error %v", ready, err)
	}
	if !taskHasAgentRuntimeDrainCleanupProofForUID(completedTask, completedTask.UID) {
		t.Fatalf("post-recovery Task cleanup receipt = %q, want preserved drain proof", completedTask.Status.Execution.RuntimeSessionCleanupDigest)
	}
	unrelated := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(unrelatedTask), unrelated); err != nil {
		t.Fatalf("get unrelated Task after AgentRuntime deletion: %v", err)
	}
	if unrelated.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf("unrelated Task cleanup receipt = %q, want empty", unrelated.Status.Execution.RuntimeSessionCleanupDigest)
	}
	if protocolErrors := server.ProtocolErrors(); len(protocolErrors) != 0 {
		t.Fatalf("deletion protocol errors = %v", protocolErrors)
	}
}

func TestAgentRuntimeReconcilerRecoversUncommittedCleanupFinalizer(t *testing.T) {
	for _, test := range []struct {
		name      string
		boundTask bool
	}{
		{name: "without bound Tasks"},
		{name: "retains finalizer for bound Task", boundTask: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
			config := conformancetest.Config{
				ControllerBearerToken:     strings.Repeat("t", 32),
				OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
				RuntimeInstanceID:         "external-runtime-instance-1",
				SupervisorBootID:          "boot-1",
				RuntimePoolUID:            "external-pool-1",
				Profile:                   profile,
				Limits:                    limits,
				SupportsDrain:             true,
				WorkspaceGovernance:       claims,
			}
			server, err := conformancetest.NewServer(config)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()

			runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
			runtimeObject.UID = types.UID("runtime-uid")
			objects := []client.Object{runtimeObject, secret}
			if test.boundTask {
				objects = append(objects, testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "bound-task"))
			}
			reconciler := newAgentRuntimeUnitReconciler(t, objects...)
			reconciler.Client = &agentRuntimeFailCleanupSecretCreateClient{Client: reconciler.Client}
			allowAgentRuntimeLoopback(t)

			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err == nil ||
				!strings.Contains(err.Error(), "injected cleanup Secret create failure") {
				t.Fatalf("initial reconcile error = %v, want injected cleanup Secret failure", err)
			}
			incomplete := getAgentRuntime(t, reconciler, runtimeObject)
			if !slices.Contains(incomplete.Finalizers, agentRuntimeFinalizer) ||
				!slices.Contains(incomplete.Finalizers, agentRuntimeSecretGCFinalizer) {
				t.Fatalf("AgentRuntime finalizers after failed Secret create = %v", incomplete.Finalizers)
			}
			if incomplete.Status.Ready || incomplete.Status.ObservedCapabilities != nil {
				t.Fatalf("AgentRuntime status after failed Secret create = %#v", incomplete.Status)
			}
			cleanupSecretName, err := agentRuntimeCleanupSecretName(&incomplete)
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.APIReader.Get(
				t.Context(), types.NamespacedName{Namespace: incomplete.Namespace, Name: cleanupSecretName}, &corev1.Secret{},
			); !apierrors.IsNotFound(err) {
				t.Fatalf("cleanup Secret after failed create Get() error = %v", err)
			}

			if err := reconciler.Delete(t.Context(), &incomplete); err != nil {
				t.Fatalf("delete AgentRuntime after failed Secret create: %v", err)
			}
			result, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
			if test.boundTask {
				if err == nil || !strings.Contains(err.Error(), "cleanup finalizer retained for bound Task") {
					t.Fatalf("bound Task deletion reconcile error = %v", err)
				}
				assertAgentRuntimeCleanupFinalizer(t, getAgentRuntime(t, reconciler, runtimeObject))
				return
			}
			if err != nil {
				t.Fatalf("recover uncommitted cleanup finalizer: %v", err)
			}
			if result.RequeueAfter != agentRuntimeDeleteRequeue {
				t.Fatalf("uncommitted cleanup recovery result = %#v", result)
			}
			recovering := getAgentRuntime(t, reconciler, runtimeObject)
			if slices.Contains(recovering.Finalizers, agentRuntimeFinalizer) ||
				!slices.Contains(recovering.Finalizers, agentRuntimeSecretGCFinalizer) {
				t.Fatalf("AgentRuntime finalizers after uncommitted cleanup recovery = %v", recovering.Finalizers)
			}
			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
				t.Fatalf("complete uncommitted cleanup recovery: %v", err)
			}
			if err := reconciler.Get(
				t.Context(), client.ObjectKeyFromObject(runtimeObject), &corev1alpha1.AgentRuntime{},
			); !apierrors.IsNotFound(err) {
				t.Fatalf("AgentRuntime after uncommitted cleanup recovery Get() error = %v", err)
			}
		})
	}
}

func TestAgentRuntimeReconcilerDeletionAdvancesControllerEpochFence(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	frozenFence := harnessv2.Fence{
		RuntimeInstanceID:          config.RuntimeInstanceID,
		SupervisorBootID:           config.SupervisorBootID,
		ControllerEpoch:            1,
		RuntimePoolUID:             config.RuntimePoolUID,
		RuntimePoolGeneration:      7,
		RuntimeProfileDigest:       profileDigest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	oldServer := newAgentRuntimeDeletionTestServer(
		config.ControllerBearerToken, config.OperationCapabilitySecret, frozenFence,
	)
	defer oldServer.Close()
	liveFence := frozenFence
	liveFence.ControllerEpoch = 2
	liveFence.SupervisorBootID = "boot-2"
	server := newAgentRuntimeDeletionTestServer(
		config.ControllerBearerToken, config.OperationCapabilitySecret, liveFence,
	)
	defer server.Close()

	const serviceEndpoint = "http://runtime.default.svc.cluster.local:8080"
	oldAddress := oldServer.Listener.Addr().(*net.TCPAddr)
	newAddress := server.Listener.Addr().(*net.TCPAddr)
	oldPort := int32(oldAddress.Port)
	newPort := int32(newAddress.Port)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "runtime"},
			Ports:    []corev1.ServicePort{{Name: "acp", Port: 8080}},
		},
	}
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-old", UID: types.UID("runtime-old-uid"),
			Labels: map[string]string{"app": "runtime"},
		},
		Status: corev1.PodStatus{
			PodIP: oldAddress.IP.String(), PodIPs: []corev1.PodIP{{IP: oldAddress.IP.String()}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-new", UID: types.UID("runtime-new-uid"),
			Labels: map[string]string{"app": "runtime"},
		},
		Status: corev1.PodStatus{
			PodIP: newAddress.IP.String(), PodIPs: []corev1.PodIP{{IP: newAddress.IP.String()}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	oldReady, oldTerminating, newReady := false, true, true
	servicePortName := "acp"
	oldSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-old",
			Labels: map[string]string{discoveryv1.LabelServiceName: service.Name},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &servicePortName, Port: &oldPort}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{oldAddress.IP.String()},
			Conditions: discoveryv1.EndpointConditions{Ready: &oldReady, Terminating: &oldTerminating},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: oldPod.Name},
		}},
	}
	newSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-new",
			Labels: map[string]string{discoveryv1.LabelServiceName: service.Name},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &servicePortName, Port: &newPort}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{newAddress.IP.String()}, Conditions: discoveryv1.EndpointConditions{Ready: &newReady},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: newPod.Name},
		}},
	}

	runtimeObject, secret := testAgentRuntimeAndSecret(t, serviceEndpoint, config)
	runtimeObject.UID = types.UID("runtime-uid")
	runtimeObject.Generation = 2
	secret.UID = types.UID("runtime-auth-uid")
	secret.ResourceVersion = "1"
	boundTask := testAgentRuntimeCleanupTask(t, runtimeObject, config.RuntimeInstanceID, "bound-task")
	boundTask.Status.Execution.RuntimeSessionSupervisorBootID = string(config.SupervisorBootID)
	reconciler := newAgentRuntimeUnitReconciler(
		t, runtimeObject, secret, boundTask, service, oldPod, newPod, oldSlice, newSlice,
	)
	allowAgentRuntimeLoopback(t)
	seedDeletingAgentRuntime(t, reconciler, runtimeObject, secret, frozenFence)

	setAgentRuntimeTestControllerEpoch(reconciler.ControllerEpochManager, 2)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err == nil ||
		!strings.Contains(err.Error(), "requires one remaining Service backend") {
		t.Fatalf("two-backend rotated-epoch deletion reconcile error = %v, want rollout fence", err)
	}
	if got := len(oldServer.DrainFences()) + len(server.DrainFences()); got != 0 {
		t.Fatalf("drain attempts while old Service backend remained = %d, want 0", got)
	}
	if err := reconciler.Delete(t.Context(), oldSlice); err != nil {
		t.Fatalf("remove old Service backend EndpointSlice: %v", err)
	}

	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err == nil ||
		!strings.Contains(err.Error(), "request authenticated AgentRuntime deletion drain") {
		t.Fatalf("first rotated-epoch deletion reconcile error = %v, want injected drain failure", err)
	}
	for step := range 3 {
		result, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
		if err != nil {
			t.Fatalf("rotated-epoch deletion reconcile %d: %v", step+2, err)
		}
		if result.RequeueAfter != agentRuntimeDeleteRequeue {
			t.Fatalf("rotated-epoch deletion result %d = %#v", step+2, result)
		}
	}
	deleting := getAgentRuntime(t, reconciler, runtimeObject)
	assertAgentRuntimeCleanupFinalizer(t, deleting)
	uncertifiedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(boundTask), uncertifiedTask); err != nil {
		t.Fatalf("get uncertified bound Task after rotated-epoch drain: %v", err)
	}
	if uncertifiedTask.Status.Execution.RuntimeSessionCleanupDigest != "" {
		t.Fatalf(
			"bound Task cleanup receipt = %q, want no proof from a different supervisor boot",
			uncertifiedTask.Status.Execution.RuntimeSessionCleanupDigest,
		)
	}
	cleanupDigest, err := agentRuntimeDrainCleanupProofDigest(
		uncertifiedTask.UID, uncertifiedTask.Status.AgentExecutionBinding,
	)
	if err != nil {
		t.Fatalf("build prior-boot Task cleanup receipt: %v", err)
	}
	uncertifiedTask.Status.Execution.RuntimeSessionCleanupDigest = cleanupDigest
	if err := reconciler.Status().Update(t.Context(), uncertifiedTask); err != nil {
		t.Fatalf("record prior-boot Task cleanup receipt: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("complete rotated-epoch deletion with prior cleanup receipt: %v", err)
	}
	if err := reconciler.Get(
		t.Context(), client.ObjectKeyFromObject(runtimeObject), &corev1alpha1.AgentRuntime{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("AgentRuntime after rotated-epoch deletion Get() error = %v", err)
	}
	completedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(boundTask), completedTask); err != nil {
		t.Fatalf("get bound Task after rotated-epoch deletion: %v", err)
	}
	if completedTask.Status.Execution.RuntimeSessionCleanupDigest != cleanupDigest ||
		!taskHasAgentRuntimeDrainCleanupProofForUID(completedTask, completedTask.UID) {
		t.Fatalf("bound Task cleanup receipt = %q, want preserved old-boot proof", completedTask.Status.Execution.RuntimeSessionCleanupDigest)
	}
	for _, drainFence := range server.DrainFences() {
		if drainFence.ControllerEpoch != liveFence.ControllerEpoch || drainFence.SupervisorBootID != liveFence.SupervisorBootID {
			t.Fatalf("drain fence = %#v, want live fence %#v", drainFence, liveFence)
		}
	}
	if len(server.DrainFences()) != 2 {
		t.Fatalf("drain fences = %v, want two attempts", server.DrainFences())
	}
	if len(oldServer.DrainFences()) != 0 {
		t.Fatalf("old-backend drain fences = %v, want none", oldServer.DrainFences())
	}
	if protocolErrors := oldServer.ProtocolErrors(); len(protocolErrors) != 0 {
		t.Fatalf("old-backend protocol errors = %v", protocolErrors)
	}
	if protocolErrors := server.ProtocolErrors(); len(protocolErrors) != 0 {
		t.Fatalf("rotated-epoch deletion protocol errors = %v", protocolErrors)
	}
}

func TestValidateAgentRuntimeDeletionStatusControllerEpochRotation(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatal(err)
	}
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	baseFrozen, _ := testAgentRuntimeAndSecret(t, "https://runtime.example", config)
	baseFrozen.Status.ObservedCapabilities = &corev1alpha1.AgentRuntimeObservedCapabilities{
		RuntimeInstanceID:          string(config.RuntimeInstanceID),
		SupervisorBootID:           string(config.SupervisorBootID),
		ControllerEpoch:            1,
		RuntimePoolUID:             string(config.RuntimePoolUID),
		RuntimePoolGeneration:      7,
		RuntimeProfileDigest:       string(profileDigest),
		ProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
	}
	baseStatus := &harnessv2.StatusResponse{Fence: harnessv2.Fence{
		RuntimeInstanceID:          config.RuntimeInstanceID,
		SupervisorBootID:           config.SupervisorBootID,
		ControllerEpoch:            1,
		RuntimePoolUID:             config.RuntimePoolUID,
		RuntimePoolGeneration:      7,
		RuntimeProfileDigest:       profileDigest,
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}}

	for _, test := range []struct {
		name            string
		controllerEpoch int64
		mutate          func(*corev1alpha1.AgentRuntime, *harnessv2.StatusResponse)
		wantError       bool
	}{
		{name: "same epoch exact boot", controllerEpoch: 1},
		{name: "advanced epoch rotated boot", controllerEpoch: 2, mutate: func(_ *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			status.Fence.ControllerEpoch = 2
			status.Fence.SupervisorBootID = "boot-2"
		}},
		{name: "advanced epoch reused boot", controllerEpoch: 2, mutate: func(_ *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			status.Fence.ControllerEpoch = 2
		}, wantError: true},
		{name: "stale live epoch", controllerEpoch: 2, mutate: func(_ *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			status.Fence.SupervisorBootID = "boot-2"
		}, wantError: true},
		{name: "same epoch boot drift", controllerEpoch: 1, mutate: func(_ *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			status.Fence.SupervisorBootID = "boot-2"
		}, wantError: true},
		{name: "future frozen epoch", controllerEpoch: 2, mutate: func(frozen *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			frozen.Status.ObservedCapabilities.ControllerEpoch = 3
			frozen.Status.ObservedCapabilities.SupervisorBootID = "boot-3"
			status.Fence.ControllerEpoch = 2
			status.Fence.SupervisorBootID = "boot-2"
		}, wantError: true},
		{name: "immutable fence drift", controllerEpoch: 2, mutate: func(_ *corev1alpha1.AgentRuntime, status *harnessv2.StatusResponse) {
			status.Fence.ControllerEpoch = 2
			status.Fence.SupervisorBootID = "boot-2"
			status.Fence.RuntimePoolGeneration++
		}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			frozen := baseFrozen.DeepCopy()
			status := *baseStatus
			if test.mutate != nil {
				test.mutate(frozen, &status)
			}
			err := validateAgentRuntimeDeletionStatus(frozen, test.controllerEpoch, &status)
			if (err != nil) != test.wantError {
				t.Fatalf("validateAgentRuntimeDeletionStatus() error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func testAgentRuntimeCleanupTask(
	t *testing.T,
	runtimeObject *corev1alpha1.AgentRuntime,
	runtimeInstanceID harnessv2.RuntimeInstanceID,
	name string,
) *corev1alpha1.Task {
	t.Helper()
	taskUID := types.UID(name + "-uid")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: runtimeObject.Namespace, Name: name, UID: taskUID},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
		Status: corev1alpha1.TaskStatus{
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				SchemaVersion:   1,
				ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
				Backend:         corev1alpha1.AgentExecutionBackendExternalEndpoint,
				Task:            corev1alpha1.AgentExecutionBindingTaskRef{UID: taskUID},
				RuntimeRef: &corev1alpha1.AgentExecutionRuntimeRef{
					Name: runtimeObject.Name, UID: runtimeObject.UID, Generation: runtimeObject.Generation,
				},
				RuntimeProfileDigest:              runtimeObject.Spec.Capabilities.Profile.Digest,
				RuntimeProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
			},
			Execution: &corev1alpha1.TaskExecutionStatus{
				Attempt: 1, AgentRuntimeName: runtimeObject.Name, AgentRuntimeUID: string(runtimeObject.UID),
				RuntimeInstanceID: string(runtimeInstanceID), RuntimeSessionUID: name + "-session",
				RuntimeSessionGeneration: 1,
			},
		},
	}
	digest, err := canonicalAgentExecutionBindingDigest(*task.Status.AgentExecutionBinding)
	if err != nil {
		t.Fatalf("canonicalize AgentRuntime cleanup Task binding: %v", err)
	}
	task.Status.AgentExecutionBinding.BindingDigest = digest
	return task
}

func TestAgentRuntimeReconcilerDeletionUsesLastGoodAuthorityAfterFailedReprobe(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("initial conformance: %v", err)
	}
	ready := getAgentRuntime(t, reconciler, runtimeObject)
	if !ready.Status.Ready || !agentRuntimeObservedStatusIdentityComplete(ready.Status.ObservedCapabilities) {
		t.Fatalf("initial conformance status = %#v", ready.Status)
	}
	cleanupSecretName, err := agentRuntimeCleanupSecretName(&ready)
	if err != nil {
		t.Fatal(err)
	}
	cleanupSecret := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), types.NamespacedName{Namespace: ready.Namespace, Name: cleanupSecretName}, cleanupSecret); err != nil {
		t.Fatalf("get persisted cleanup authority: %v", err)
	}
	frozen, _, err := decodeAgentRuntimeDeletionSnapshot(&ready, cleanupSecret)
	if err != nil {
		t.Fatalf("decode persisted cleanup authority: %v", err)
	}
	if frozen.Spec.Deployment.Endpoint != server.URL() {
		t.Fatalf("frozen cleanup endpoint = %q, want %q", frozen.Spec.Deployment.Endpoint, server.URL())
	}

	const unavailableEndpoint = "http://127.0.0.1:1"
	ready.Spec.Deployment.Endpoint = unavailableEndpoint
	ready.Generation++
	if err := reconciler.Update(t.Context(), &ready); err != nil {
		t.Fatalf("update AgentRuntime to unavailable endpoint: %v", err)
	}
	currentSecret := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(secret), currentSecret); err != nil {
		t.Fatalf("get AgentRuntime auth Secret: %v", err)
	}
	currentSecret.Annotations[agentRuntimeAuthEndpointAnnotation] = unavailableEndpoint
	currentSecret.Data["controller-token"] = []byte(strings.Repeat("x", 32))
	currentSecret.Data["capability-secret"] = []byte(strings.Repeat("y", 32))
	if err := reconciler.Update(t.Context(), currentSecret); err != nil {
		t.Fatalf("rotate AgentRuntime auth Secret: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("failed reprobe reconcile: %v", err)
	}
	unavailable := getAgentRuntime(t, reconciler, runtimeObject)
	if unavailable.Status.Ready || unavailable.Status.ObservedCapabilities != nil {
		t.Fatalf("failed reprobe status = %#v, want NotReady without current authority", unavailable.Status)
	}
	assertAgentRuntimeCleanupFinalizer(t, unavailable)
	if err := reconciler.Delete(t.Context(), &unavailable); err != nil {
		t.Fatalf("delete AgentRuntime after failed reprobe: %v", err)
	}
	cleanupSecret = &corev1.Secret{}
	if err := reconciler.Get(t.Context(), types.NamespacedName{Namespace: ready.Namespace, Name: cleanupSecretName}, cleanupSecret); err != nil {
		t.Fatalf("get cleanup authority before simulated garbage collection: %v", err)
	}
	if err := reconciler.Delete(t.Context(), cleanupSecret); err != nil {
		t.Fatalf("simulate cleanup Secret garbage collection: %v", err)
	}

	result, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject))
	if err != nil {
		t.Fatalf("drain with frozen cleanup authority: %v", err)
	}
	if result.RequeueAfter != agentRuntimeDeleteRequeue {
		t.Fatalf("frozen cleanup drain result = %#v", result)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("complete drain with frozen cleanup authority: %v", err)
	}
	deleting := getAgentRuntime(t, reconciler, runtimeObject)
	if slices.Contains(deleting.Finalizers, agentRuntimeFinalizer) ||
		!slices.Contains(deleting.Finalizers, agentRuntimeSecretGCFinalizer) {
		t.Fatalf("AgentRuntime finalizers after drain = %v, want only cleanup Secret GC authority", deleting.Finalizers)
	}
	retainedCleanupSecret := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), types.NamespacedName{Namespace: ready.Namespace, Name: cleanupSecretName}, retainedCleanupSecret); err != nil {
		t.Fatalf("get cleanup authority retained through owner deletion: %v", err)
	}
	if retainedCleanupSecret.DeletionTimestamp.IsZero() ||
		!slices.Contains(retainedCleanupSecret.Finalizers, agentRuntimeSecretFinalizer) {
		t.Fatalf("cleanup Secret metadata during garbage collection = %#v", retainedCleanupSecret.ObjectMeta)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("release cleanup Secret after authenticated drain: %v", err)
	}
	if err := reconciler.Get(t.Context(), types.NamespacedName{Namespace: ready.Namespace, Name: cleanupSecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup Secret after finalization Get() error = %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("finish deletion after cleanup Secret removal: %v", err)
	}
	var deleted corev1alpha1.AgentRuntime
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(runtimeObject), &deleted); !apierrors.IsNotFound(err) {
		t.Fatalf("AgentRuntime after frozen-authority deletion Get() error = %v, object=%#v", err, deleted.ObjectMeta)
	}
}

func TestAgentRuntimeReconcilerDeletionWithoutDrainSupportFailsClosed(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             false,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("register runtime without drain support: %v", err)
	}
	ready := getAgentRuntime(t, reconciler, runtimeObject)
	assertAgentRuntimeCleanupFinalizer(t, ready)
	if err := reconciler.Delete(t.Context(), &ready); err != nil {
		t.Fatalf("delete runtime without drain support: %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err == nil ||
		!strings.Contains(err.Error(), "requires supportsDrain=true") {
		t.Fatalf("deletion reconcile error = %v, want fail-closed drain requirement", err)
	}
	deleting := getAgentRuntime(t, reconciler, runtimeObject)
	if deleting.DeletionTimestamp.IsZero() {
		t.Fatal("runtime without drain support was not retained in deleting state")
	}
	assertAgentRuntimeCleanupFinalizer(t, deleting)
}

func TestAgentRuntimeReconcilerClearsLegacyDuplicatePoolIdentityWhenProbeFails(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}

	owner, ownerSecret := testAgentRuntimeAndSecret(t, server.URL(), config)
	duplicate, duplicateSecret := duplicateAgentRuntimeTestRegistration(owner, ownerSecret, "runtime-duplicate")
	reconciler := newAgentRuntimeUnitReconciler(t, owner, ownerSecret, duplicate, duplicateSecret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(owner)); err != nil {
		t.Fatalf("reconcile owner: %v", err)
	}
	readyOwner := getAgentRuntime(t, reconciler, owner)
	legacyDuplicate := getAgentRuntime(t, reconciler, duplicate)
	legacyDuplicate.Status = readyOwner.DeepCopy().Status
	if err := reconciler.Status().Update(t.Context(), &legacyDuplicate); err != nil {
		t.Fatalf("seed legacy duplicate status: %v", err)
	}

	server.Close()
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(duplicate)); err != nil {
		t.Fatalf("reconcile unavailable legacy duplicate: %v", err)
	}
	rejectedDuplicate := getAgentRuntime(t, reconciler, duplicate)
	assertAgentRuntimePoolIdentityRejected(t, rejectedDuplicate, owner.Name)
	assertAgentRuntimeNoCleanupFinalizer(t, rejectedDuplicate)
	assertAgentRuntimeMCPPreAuth(t, reconciler, string(config.RuntimePoolUID), config.ControllerBearerToken)
}

func TestAgentRuntimeReconcilerRejectsManagedRuntimePoolIdentityAfterProbeFailure(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "managed-pool-uid",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	readyRuntime := getAgentRuntime(t, reconciler, runtimeObject)
	if !readyRuntime.Status.Ready || readyRuntime.Status.ObservedCapabilities == nil {
		t.Fatalf("initial runtime status = %#v", readyRuntime.Status)
	}

	managedPool := &corev1alpha1.RuntimePool{ObjectMeta: metav1.ObjectMeta{
		Namespace: runtimeObject.Namespace, Name: "managed-pool", UID: types.UID(config.RuntimePoolUID),
	}}
	if err := reconciler.Create(t.Context(), managedPool); err != nil {
		t.Fatalf("create managed RuntimePool: %v", err)
	}
	server.Close()
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("reconcile failed probe against managed RuntimePool: %v", err)
	}
	rejectedRuntime := getAgentRuntime(t, reconciler, runtimeObject)
	assertAgentRuntimePoolIdentityRejected(t, rejectedRuntime, managedPool.Name)
	assertAgentRuntimeCleanupFinalizer(t, rejectedRuntime)
}

func TestAgentRuntimePoolIdentityAllowsDistinctUIDs(t *testing.T) {
	existing := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime-a"},
		Status: corev1alpha1.AgentRuntimeStatus{
			ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{RuntimePoolUID: "pool-a"},
		},
	}
	contender := &corev1alpha1.AgentRuntime{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime-b"}}
	reconciler := newAgentRuntimeUnitReconciler(t, existing, contender)
	owner, err := reconciler.conflictingAgentRuntimePoolIdentityOwner(
		t.Context(), contender, &corev1alpha1.AgentRuntimeObservedCapabilities{RuntimePoolUID: "pool-b"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if owner != nil {
		t.Fatalf("distinct pool identity reported owner %s/%s", owner.Namespace, owner.Name)
	}
}

func TestAgentRuntimeReconcilerRetainsAuthenticatedObservationAcrossTransientProbeFailure(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	ready := getAgentRuntime(t, reconciler, runtimeObject)
	if !ready.Status.Ready || !agentRuntimeObservedStatusIdentityComplete(ready.Status.ObservedCapabilities) {
		t.Fatalf("initial conformance status = %#v", ready.Status)
	}
	previous := ready.Status.ObservedCapabilities.DeepCopy()
	controllerAuthVersion := ready.Status.ObservedControllerAuthRefResourceVersion
	capabilityAuthVersion := ready.Status.ObservedOperationCapabilityRefResourceVersion

	server.Close()
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	unavailable := getAgentRuntime(t, reconciler, runtimeObject)
	if unavailable.Status.Ready {
		t.Fatalf("transiently unavailable runtime remained ready: %#v", unavailable.Status)
	}
	if !reflect.DeepEqual(unavailable.Status.ObservedCapabilities, previous) {
		t.Fatalf("transient failure observation = %#v, want retained %#v", unavailable.Status.ObservedCapabilities, previous)
	}
	if unavailable.Status.ObservedControllerAuthRefResourceVersion != controllerAuthVersion ||
		unavailable.Status.ObservedOperationCapabilityRefResourceVersion != capabilityAuthVersion {
		t.Fatalf("transient failure auth versions = %q/%q, want %q/%q",
			unavailable.Status.ObservedControllerAuthRefResourceVersion,
			unavailable.Status.ObservedOperationCapabilityRefResourceVersion,
			controllerAuthVersion, capabilityAuthVersion)
	}

	verifier := &TaskReconciler{
		Client: reconciler.Client, APIReader: reconciler.APIReader,
		ControllerEpochManager: reconciler.ControllerEpochManager,
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: runtimeObject.Namespace}}
	if _, _, err := verifier.resolveExternalAgentRuntimeSnapshot(t.Context(), task, &unavailable); err == nil ||
		!strings.Contains(err.Error(), "has not passed current-generation v2 conformance") {
		t.Fatalf("new admission after transient failure error = %v, want readiness rejection", err)
	}
}

func TestAgentRuntimeReconcilerRecoversAfterRuntimeRotatesToCurrentControllerEpoch(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	if current := getAgentRuntime(t, reconciler, runtimeObject); !current.Status.Ready {
		t.Fatalf("initial Ready = false, message=%q", current.Status.Message)
	}

	setAgentRuntimeTestControllerEpoch(reconciler.ControllerEpochManager, 2)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	stale := getAgentRuntime(t, reconciler, runtimeObject)
	if stale.Status.Ready || !strings.Contains(stale.Status.Message, "controller epoch 1 does not match expected 2") {
		t.Fatalf("stale runtime status = %#v", stale.Status)
	}

	fence := server.Fence()
	fence.ControllerEpoch = 2
	fence.SupervisorBootID = "boot-2"
	if err := server.SetFence(fence); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	recovered := getAgentRuntime(t, reconciler, runtimeObject)
	if !recovered.Status.Ready || recovered.Status.ObservedCapabilities == nil ||
		recovered.Status.ObservedCapabilities.ControllerEpoch != 2 ||
		recovered.Status.ObservedCapabilities.SupervisorBootID != "boot-2" {
		t.Fatalf("rotated runtime status = %#v", recovered.Status)
	}
	cleanupSecretName, err := agentRuntimeCleanupSecretName(&recovered)
	if err != nil {
		t.Fatal(err)
	}
	cleanupSecret := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), types.NamespacedName{Namespace: recovered.Namespace, Name: cleanupSecretName}, cleanupSecret); err != nil {
		t.Fatalf("get refreshed cleanup authority: %v", err)
	}
	frozen, _, err := decodeAgentRuntimeDeletionSnapshot(&recovered, cleanupSecret)
	if err != nil {
		t.Fatalf("decode refreshed cleanup authority: %v", err)
	}
	if frozen.Status.ObservedCapabilities.ControllerEpoch != 2 ||
		frozen.Status.ObservedCapabilities.SupervisorBootID != "boot-2" {
		t.Fatalf("refreshed cleanup authority = %#v", frozen.Status.ObservedCapabilities)
	}
}

func TestAgentRuntimeReconcilerMarksV2RuntimeUnreadyWithoutEpochManager(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	reconciler.ControllerEpochManager = nil
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || updated.Status.Message != "current controller epoch manager is unavailable" {
		t.Fatalf("runtime status = %#v, want fail-closed missing epoch manager", updated.Status)
	}
}

func TestAgentRuntimeReconcilerUsesBrokerRegistryForV2Conformance(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{"delegate_task", "run_validation", "wait_for_tasks"}
	var err error
	profile.ToolPolicyDigest, err = harnessv2.CanonicalRuntimeToolPolicyDigest(
		policy.AllowedTools, policy.DisallowedTools, policy.AllowBash,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile.MCPConfigurationDigest, err = harnessv2.CanonicalMCPConfigurationDigest(policy.AllowedTools)
	if err != nil {
		t.Fatal(err)
	}
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	runtimeObject.Spec.Capabilities.MCPPolicy = &policy
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	registry := tools.NewRegistry()
	if err := tools.RegisterBrokeredCoordinationTools(registry, reconciler.Client); err != nil {
		t.Fatal(err)
	}
	reconciler.MCPRegistry = registry
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready {
		t.Fatalf("Ready = false, message=%q", updated.Status.Message)
	}
}

func TestAgentRuntimeReconcilerPreservesRegisteredSelectionFromCapabilitySupersets(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken:     strings.Repeat("t", 32),
		OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID:         "external-runtime-instance-1",
		SupervisorBootID:          "boot-1",
		RuntimePoolUID:            "external-pool-1",
		Profile:                   profile,
		ProviderKinds:             []string{"another-provider", profile.ProviderKind},
		Models:                    []string{"another-model", profile.Model},
		Limits:                    limits,
		SupportsDrain:             true,
		WorkspaceGovernance:       claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready || updated.Status.ObservedCapabilities == nil {
		t.Fatalf("superset runtime readiness = %#v", updated.Status)
	}
	if updated.Status.ObservedCapabilities.ProviderKind != profile.ProviderKind ||
		updated.Status.ObservedCapabilities.Model != profile.Model {
		t.Fatalf("observed provider/model = %#v, want registered %q/%q", updated.Status.ObservedCapabilities, profile.ProviderKind, profile.Model)
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: runtimeObject.Namespace}}
	taskReconciler := &TaskReconciler{
		Client: reconciler.Client, APIReader: reconciler.APIReader,
		ControllerEpochManager: reconciler.ControllerEpochManager,
	}
	if _, _, err := taskReconciler.resolveExternalAgentRuntimeSnapshot(t.Context(), task, &updated); err != nil {
		t.Fatalf("binding rejected conformed provider/model capability supersets: %v", err)
	}
}

func TestAgentRuntimeReconcilerDoesNotRepeatHostileCycleWhenIdentityIsUnchanged(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "external-runtime-instance-1", SupervisorBootID: "boot-1", RuntimePoolUID: "external-pool-1",
		Profile: profile, Limits: limits, SupportsDrain: true, WorkspaceGovernance: claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	for range 2 {
		if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
			t.Fatal(err)
		}
	}
	counts := server.Counts()
	if counts.PromptStarts != 2 || counts.SessionCreates != 2 {
		t.Fatalf("deep hostile cycle repeated on unchanged ready runtime: %#v", counts)
	}
}

func TestAgentRuntimeReconcilerRechecksHostileCycleAfterMCPToolDescriptorChange(t *testing.T) {
	const toolName = "external_lookup"
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	policy := testAgentRuntimeMCPPolicy()
	policy.AllowedTools = []string{toolName}
	var err error
	profile.ToolPolicyDigest, err = harnessv2.CanonicalRuntimeToolPolicyDigest(
		policy.AllowedTools, policy.DisallowedTools, policy.AllowBash,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile.MCPConfigurationDigest, err = harnessv2.CanonicalMCPConfigurationDigest(policy.AllowedTools)
	if err != nil {
		t.Fatal(err)
	}
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "external-runtime-instance-1", SupervisorBootID: "boot-1", RuntimePoolUID: "external-pool-1",
		Profile: profile, Limits: limits, SupportsDrain: true, WorkspaceGovernance: claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	runtimeObject.Spec.Capabilities.MCPPolicy = &policy
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Namespace: runtimeObject.Namespace, Name: toolName, UID: types.UID("external-lookup-uid"), Generation: 1},
		Spec: corev1alpha1.ToolSpec{
			Description: "look up a value", BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
			HTTP: &corev1alpha1.HTTPExecution{URL: "https://tool.example.invalid", Method: "POST"},
		},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret, tool)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	first := getAgentRuntime(t, reconciler, runtimeObject)
	if !first.Status.Ready || first.Status.ObservedCapabilities == nil ||
		first.Status.ObservedCapabilities.MCPToolDescriptorDigest == "" {
		t.Fatalf("initial descriptor conformance status = %#v", first.Status)
	}
	firstDigest := first.Status.ObservedCapabilities.MCPToolDescriptorDigest

	currentTool := &corev1alpha1.Tool{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(tool), currentTool); err != nil {
		t.Fatal(err)
	}
	currentTool.Spec.Description = "look up a changed value"
	currentTool.Generation++
	if err := reconciler.Update(t.Context(), currentTool); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	second := getAgentRuntime(t, reconciler, runtimeObject)
	if !second.Status.Ready || second.Status.ObservedCapabilities == nil ||
		second.Status.ObservedCapabilities.MCPToolDescriptorDigest == firstDigest {
		t.Fatalf("changed descriptor conformance status = %#v", second.Status)
	}
	counts := server.Counts()
	if counts.SessionCreates != 4 || counts.PromptStarts != 4 {
		t.Fatalf("descriptor change did not rerun full lifecycle: %#v", counts)
	}

	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	counts = server.Counts()
	if counts.SessionCreates != 4 || counts.PromptStarts != 4 {
		t.Fatalf("unchanged descriptor repeated full lifecycle: %#v", counts)
	}
}

func TestAgentRuntimeReconcilerRechecksHostileCycleAfterAuthenticatedIdentityChange(t *testing.T) {
	tests := []struct {
		name                   string
		mutate                 func(*harnessv2.Fence)
		advanceControllerEpoch bool
	}{
		{name: "supervisor boot", mutate: func(fence *harnessv2.Fence) { fence.SupervisorBootID = "boot-2" }},
		{name: "controller epoch", mutate: func(fence *harnessv2.Fence) { fence.ControllerEpoch++ }, advanceControllerEpoch: true},
		{name: "runtime pool UID", mutate: func(fence *harnessv2.Fence) { fence.RuntimePoolUID = "external-pool-2" }},
		{name: "runtime pool generation", mutate: func(fence *harnessv2.Fence) { fence.RuntimePoolGeneration++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
			config := conformancetest.Config{
				ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
				RuntimeInstanceID: "external-runtime-instance-1", SupervisorBootID: "boot-1", RuntimePoolUID: "external-pool-1",
				Profile: profile, Limits: limits, SupportsDrain: true, WorkspaceGovernance: claims,
			}
			server, err := conformancetest.NewServer(config)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
			reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
			allowAgentRuntimeLoopback(t)
			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
				t.Fatal(err)
			}

			fence := server.Fence()
			test.mutate(&fence)
			if test.advanceControllerEpoch {
				setAgentRuntimeTestControllerEpoch(reconciler.ControllerEpochManager, int64(fence.ControllerEpoch))
			}
			if err := server.SetFence(fence); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
				t.Fatal(err)
			}
			updated := getAgentRuntime(t, reconciler, runtimeObject)
			if !updated.Status.Ready {
				t.Fatalf("Ready = false after identity change, message=%q", updated.Status.Message)
			}
			counts := server.Counts()
			if counts.SessionCreates != 4 || counts.PromptStarts != 4 {
				t.Fatalf("authenticated identity change did not rerun full lifecycle: %#v", counts)
			}
		})
	}
}

func TestAgentRuntimeAuthenticatedIdentityChanged(t *testing.T) {
	fence := harnessv2.Fence{
		RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot-1", ControllerEpoch: 1,
		RuntimePoolUID: "pool-1", RuntimePoolGeneration: 1, RuntimeProfileDigest: harnessv2.ProfileDigest(testControllerDigest("profile")),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	previous := &corev1alpha1.AgentRuntimeObservedCapabilities{
		RuntimeInstanceID: string(fence.RuntimeInstanceID), SupervisorBootID: string(fence.SupervisorBootID),
		ControllerEpoch: int64(fence.ControllerEpoch), RuntimePoolUID: string(fence.RuntimePoolUID),
		RuntimePoolGeneration: int64(fence.RuntimePoolGeneration), RuntimeProfileDigest: string(fence.RuntimeProfileDigest),
		ProfileDigestSchemaVersion: int32(fence.ProfileDigestSchemaVersion),
	}
	if agentRuntimeAuthenticatedIdentityChanged(previous, &harnessv2.StatusResponse{Fence: fence}) {
		t.Fatal("unchanged authenticated identity was reported changed")
	}
	if !agentRuntimeAuthenticatedIdentityChanged(nil, &harnessv2.StatusResponse{Fence: fence}) {
		t.Fatal("missing previous identity must require lifecycle conformance")
	}
	if !agentRuntimeAuthenticatedIdentityChanged(previous, nil) {
		t.Fatal("missing current identity must require lifecycle conformance")
	}

	tests := []struct {
		name   string
		mutate func(*harnessv2.Fence)
	}{
		{name: "runtime instance", mutate: func(f *harnessv2.Fence) { f.RuntimeInstanceID = "runtime-2" }},
		{name: "supervisor boot", mutate: func(f *harnessv2.Fence) { f.SupervisorBootID = "boot-2" }},
		{name: "controller epoch", mutate: func(f *harnessv2.Fence) { f.ControllerEpoch++ }},
		{name: "runtime pool UID", mutate: func(f *harnessv2.Fence) { f.RuntimePoolUID = "pool-2" }},
		{name: "runtime pool generation", mutate: func(f *harnessv2.Fence) { f.RuntimePoolGeneration++ }},
		{name: "runtime profile", mutate: func(f *harnessv2.Fence) {
			f.RuntimeProfileDigest = harnessv2.ProfileDigest(testControllerDigest("profile-2"))
		}},
		{name: "profile schema", mutate: func(f *harnessv2.Fence) { f.ProfileDigestSchemaVersion++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := fence
			test.mutate(&changed)
			if !agentRuntimeAuthenticatedIdentityChanged(previous, &harnessv2.StatusResponse{Fence: changed}) {
				t.Fatalf("%s change did not require lifecycle conformance", test.name)
			}
		})
	}
}

func TestRetainedAgentRuntimeObservationRequiresStableAuthority(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	previous := &corev1alpha1.AgentRuntimeObservedCapabilities{
		ProtocolVersion:            harnessv2.ProtocolVersion,
		RuntimeInstanceID:          "runtime-1",
		SupervisorBootID:           "boot-1",
		ControllerEpoch:            1,
		RuntimePoolUID:             "pool-1",
		RuntimePoolGeneration:      1,
		RuntimeProfileDigest:       testControllerDigest("profile"),
		ProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
		MCPToolDescriptorDigest:    testControllerDigest("mcp-descriptors"),
	}
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Generation: 7},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
		},
		Status: corev1alpha1.AgentRuntimeStatus{
			ObservedGeneration:                            7,
			ObservedControllerAuthRefResourceVersion:      "controller-rv-1",
			ObservedOperationCapabilityRefResourceVersion: "capability-rv-1",
			ObservedCapabilities:                          previous,
		},
	}
	partial := previous.DeepCopy()
	partial.RuntimeInstanceID = ""

	retained := retainedAgentRuntimeObservation(
		runtimeObject, false, partial, "controller-rv-1", "capability-rv-1",
	)
	if retained == previous || !reflect.DeepEqual(retained, previous) {
		t.Fatalf("partial authenticated observation = %#v, want a copy of %#v", retained, previous)
	}

	unprovenSameIdentity := previous.DeepCopy()
	unprovenSameIdentity.MCPToolDescriptorDigest = testControllerDigest("changed-mcp-descriptors")
	if got := retainedAgentRuntimeObservation(
		runtimeObject, false, unprovenSameIdentity, "controller-rv-1", "capability-rv-1",
	); got == previous || !reflect.DeepEqual(got, previous) {
		t.Fatalf("failed same-identity observation = %#v, want a copy of last proven %#v", got, previous)
	}

	replacement := previous.DeepCopy()
	replacement.RuntimeInstanceID = "runtime-2"
	if got := retainedAgentRuntimeObservation(
		runtimeObject, false, replacement, "controller-rv-1", "capability-rv-1",
	); got != replacement {
		t.Fatalf("complete replacement observation = %#v, want newly authenticated identity %#v", got, replacement)
	}

	tests := []struct {
		name                       string
		mutateRuntime              func(*corev1alpha1.AgentRuntime)
		controllerAuthVersion      string
		operationCapabilityVersion string
	}{
		{
			name:                  "AgentRuntime generation changed",
			mutateRuntime:         func(runtime *corev1alpha1.AgentRuntime) { runtime.Generation++ },
			controllerAuthVersion: "controller-rv-1", operationCapabilityVersion: "capability-rv-1",
		},
		{
			name: "controller auth Secret changed", controllerAuthVersion: "controller-rv-2",
			operationCapabilityVersion: "capability-rv-1",
		},
		{
			name: "operation capability Secret changed", controllerAuthVersion: "controller-rv-1",
			operationCapabilityVersion: "capability-rv-2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := runtimeObject.DeepCopy()
			if test.mutateRuntime != nil {
				test.mutateRuntime(current)
			}
			got := retainedAgentRuntimeObservation(
				current, false, partial, test.controllerAuthVersion, test.operationCapabilityVersion,
			)
			if got != partial {
				t.Fatalf("unsafe authority change retained prior observation: %#v", got)
			}
		})
	}
}

func TestAgentRuntimeReconcilerRechecksHostileCycleAfterAuthRotation(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "external-runtime-instance-1", SupervisorBootID: "boot-1", RuntimePoolUID: "external-pool-1",
		Profile: profile, Limits: limits, SupportsDrain: true, WorkspaceGovernance: claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(secret), stored); err != nil {
		t.Fatal(err)
	}
	stored.Data["capability-secret"] = []byte(strings.Repeat("q", 32))
	if err := reconciler.Update(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	// The fake runtime still has the old key, so rotation must force a deep probe
	// and fail closed rather than leaving the prior Ready status in place.
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	// The rotated key now fails closed at the earliest capability-guarded
	// surface: the status probe itself rejects the stale capability secret.
	if updated.Status.Ready ||
		(!strings.Contains(updated.Status.Message, "operation capability") && !strings.Contains(updated.Status.Message, "status authorization failed")) {
		t.Fatalf("rotated mismatched capability key did not fail closed: %#v", updated.Status)
	}
}

func TestAgentRuntimeReconcilerMarksHarnessV1RuntimeReady(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{
		RuntimeName: "external-v1", AuthToken: agentRuntimeV1TestBearer,
	})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready {
		t.Fatalf("harness v1 runtime Ready = false, message=%q", updated.Status.Message)
	}
	if updated.Status.ObservedCapabilities == nil ||
		updated.Status.ObservedCapabilities.ProtocolVersion != harness.ProtocolVersion ||
		updated.Status.ObservedCapabilities.Transport != harness.HTTPTransport ||
		updated.Status.ObservedCapabilities.RuntimeName != "external-v1" {
		t.Fatalf("observed harness v1 capabilities = %#v", updated.Status.ObservedCapabilities)
	}
	if updated.Status.ObservedAuthRefResourceVersion == "" ||
		updated.Status.ObservedControllerAuthRefResourceVersion != "" ||
		updated.Status.ObservedOperationCapabilityRefResourceVersion != "" {
		t.Fatalf("contract-specific observed auth versions = %#v", updated.Status)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, agentRuntimeReadyCondition)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != agentRuntimeReasonReady {
		t.Fatalf("Ready condition = %#v", condition)
	}
	encodedStatus := updated.Status.Message + updated.Status.ObservedCapabilities.RuntimeName +
		updated.Status.ObservedCapabilities.RuntimeVersion
	if strings.Contains(encodedStatus, agentRuntimeV1TestBearer) {
		t.Fatal("harness v1 status leaked the bearer token")
	}
}

func TestAgentRuntimeReconcilerHarnessV1RejectsWeakBearerToken(t *testing.T) {
	tests := []struct {
		name        string
		token       string
		wantMessage string
	}{
		{name: "short", token: strings.Repeat("t", agentRuntimeMinBearerBytes-1), wantMessage: "at least 32 bytes"},
		{name: "space", token: strings.Repeat("t", 16) + " " + strings.Repeat("t", 16), wantMessage: "invalid HTTP header bytes"},
		{name: "control", token: strings.Repeat("t", 16) + "\n" + strings.Repeat("t", 16), wantMessage: "invalid HTTP header bytes"},
		{name: "non-ASCII", token: strings.Repeat("t", 31) + "é", wantMessage: "invalid HTTP header bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{AuthToken: agentRuntimeV1TestBearer})
			defer server.Close()
			runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
			secret.Data["token"] = []byte(test.token)
			reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
			allowAgentRuntimeLoopback(t)
			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
				t.Fatal(err)
			}
			updated := getAgentRuntime(t, reconciler, runtimeObject)
			if updated.Status.Ready || !strings.Contains(updated.Status.Message, test.wantMessage) {
				t.Fatalf("weak harness v1 bearer status = %#v, want message containing %q", updated.Status, test.wantMessage)
			}
			if strings.Contains(updated.Status.Message, test.token) {
				t.Fatal("harness v1 status leaked the rejected bearer token")
			}
		})
	}
}

func TestValidateHarnessV1AgentRuntimeEndpointSpecRejectsUserinfo(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		forbidden []string
	}{
		{
			name: "username", endpoint: "https://" + "operator" + "@runtime.example.invalid",
			forbidden: []string{"operator@"},
		},
		{
			name: "username and password", endpoint: "https://" + "operator" + ":" + "passphrase" + "@runtime.example.invalid",
			forbidden: []string{"operator", "passphrase"},
		},
		{
			name: "percent-encoded userinfo", endpoint: "https://" + "%6fperator" + ":" + "p%40ss" + "@runtime.example.invalid",
			forbidden: []string{"%6fperator", "p%40ss", "operator", "p@ss"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHarnessV1AgentRuntimeEndpointSpec(tt.endpoint)
			if err == nil || !strings.Contains(err.Error(), "must not include userinfo") {
				t.Fatalf("validateHarnessV1AgentRuntimeEndpointSpec() error = %v, want userinfo rejection", err)
			}
			for _, forbidden := range tt.forbidden {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("endpoint validation error disclosed URL userinfo: %q", err)
				}
			}
		})
	}
}

func TestValidateHarnessV1AgentRuntimeExecutableCapabilitiesRequiresControllerCompatibleRuntime(t *testing.T) {
	base := harness.CapabilitiesResponse{
		RuntimeName:             "external-v1",
		ToolExecutionModes:      []harness.ToolExecutionMode{harness.ToolExecutionModeObserved},
		SupportsCancel:          true,
		SupportsRuntimeSessions: true,
		MaxOutputBytes:          harness.MaxFetchTurnOutputBytes,
	}
	if err := validateHarnessV1AgentRuntimeExecutableCapabilities(&base); err != nil {
		t.Fatalf("controller-compatible capabilities rejected: %v", err)
	}

	brokeredWithoutCancel := base
	brokeredWithoutCancel.ToolExecutionModes = []harness.ToolExecutionMode{harness.ToolExecutionModeBrokered}
	brokeredWithoutCancel.BrokeredToolClasses = []harness.BrokeredToolClass{harness.BrokeredToolClassRead}
	brokeredWithoutCancel.SupportsContinuation = true
	brokeredWithoutCancel.SupportsCancel = false
	if err := validateHarnessV1AgentRuntimeExecutableCapabilities(&brokeredWithoutCancel); err == nil ||
		!strings.Contains(err.Error(), "supportsCancel") {
		t.Fatalf("brokered runtime without cancellation error = %v, want supportsCancel", err)
	}

	oversizedOutput := base
	oversizedOutput.MaxOutputBytes = harness.MaxFetchTurnOutputBytes + 1
	if err := validateHarnessV1AgentRuntimeExecutableCapabilities(&oversizedOutput); err == nil ||
		!strings.Contains(err.Error(), "fetch limit") {
		t.Fatalf("oversized output capability error = %v, want fetch limit", err)
	}
}

func TestAgentRuntimeReconcilerHarnessV1RequiresDeclaredCapabilitySubset(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{AuthToken: agentRuntimeV1TestBearer})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	required := true
	runtimeObject.Spec.Capabilities.SupportsArtifacts = &required
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "supportsArtifacts") {
		t.Fatalf("missing declared harness v1 capability status = %#v", updated.Status)
	}
}

func TestAgentRuntimeReconcilerHarnessV1AuthRotationForcesConformance(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{AuthToken: agentRuntimeV1TestBearer})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	ready := getAgentRuntime(t, reconciler, runtimeObject)
	if !ready.Status.Ready {
		t.Fatalf("initial harness v1 readiness = %#v", ready.Status)
	}
	stored := &corev1.Secret{}
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(secret), stored); err != nil {
		t.Fatal(err)
	}
	stored.Data["token"] = []byte("rotated-token-not-accepted-by-runtime")
	if err := reconciler.Update(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready ||
		(!strings.Contains(updated.Status.Message, "unauthorized") && !strings.Contains(updated.Status.Message, "401")) {
		t.Fatalf("rotated harness v1 auth did not fail closed = %#v", updated.Status)
	}
	if updated.Status.ObservedAuthRefResourceVersion == ready.Status.ObservedAuthRefResourceVersion {
		t.Fatalf("observed harness v1 auth resourceVersion did not advance: before=%q after=%q",
			ready.Status.ObservedAuthRefResourceVersion, updated.Status.ObservedAuthRefResourceVersion)
	}
}

func TestAgentRuntimeReconcilerHarnessV1ShallowProbeRejectsRuntimeBearerDrift(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{
		RuntimeName: "external-v1", AuthToken: strings.Repeat("r", agentRuntimeMinBearerBytes),
	})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	runtimeObject.Status = corev1alpha1.AgentRuntimeStatus{
		Ready:                          true,
		ObservedGeneration:             runtimeObject.Generation,
		ObservedAuthRefResourceVersion: secret.ResourceVersion,
		ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
			ProtocolVersion: harness.ProtocolVersion,
			Transport:       harness.HTTPTransport,
			RuntimeName:     "external-v1",
		},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "configured bearer was rejected") {
		t.Fatalf("runtime-side bearer drift did not fail the shallow probe: %#v", updated.Status)
	}
	if strings.Contains(updated.Status.Message, agentRuntimeV1TestBearer) {
		t.Fatal("shallow probe status leaked the configured bearer")
	}
}

func TestAgentRuntimeReconcilerHarnessV1UsesConfiguredTLSClient(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{
		AuthToken: agentRuntimeV1TestBearer,
		TLS:       true,
	})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	reconciler.HarnessV1HTTPClient = server.Client()
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready {
		t.Fatalf("private-CA harness v1 readiness = %#v", updated.Status)
	}
}

func TestAgentRuntimeReconcilerHarnessV1RejectsSecretIdentityChangeDuringConformance(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*corev1.Secret)
		wantMessage string
	}{
		{
			name:        "UID replacement",
			mutate:      func(secret *corev1.Secret) { secret.UID = types.UID("replacement-secret-uid") },
			wantMessage: "replaced during conformance",
		},
		{
			name:        "resourceVersion rotation",
			mutate:      func(secret *corev1.Secret) { secret.ResourceVersion = "rotated-resource-version" },
			wantMessage: "changed during conformance",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{AuthToken: agentRuntimeV1TestBearer})
			defer server.Close()
			runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
			reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
			reconciler.APIReader = &agentRuntimeMutateSecondSecretReadClient{
				Client: reconciler.Client, SecretKey: client.ObjectKeyFromObject(secret), Mutate: test.mutate,
			}
			allowAgentRuntimeLoopback(t)
			if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
				t.Fatal(err)
			}
			updated := getAgentRuntime(t, reconciler, runtimeObject)
			if updated.Status.Ready || !strings.Contains(updated.Status.Message, test.wantMessage) {
				t.Fatalf("Secret identity race status = %#v", updated.Status)
			}
		})
	}
}

func TestAgentRuntimeReconcilerHarnessV1RequiresSecretUID(t *testing.T) {
	server := harnesstest.NewFakeHarnessServer(harnesstest.FakeHarnessConfig{AuthToken: agentRuntimeV1TestBearer})
	defer server.Close()
	runtimeObject, secret := testHarnessV1AgentRuntimeAndSecret(server.URL())
	secret.UID = ""
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "UID is required") {
		t.Fatalf("missing bearer Secret UID status = %#v", updated.Status)
	}
}

func TestAgentRuntimeReconcilerUnclassifiedContractNotReady(t *testing.T) {
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "unclassified", Generation: 1},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: "https://runtime.example.com"},
		},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "unclassified") {
		t.Fatalf("unclassified runtime status = %#v", updated.Status)
	}
}

func TestValidateAgentRuntimeSpecRequiresExactCanonicalProfileDigest(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "https://runtime.example.com", config)
	runtimeObject.Spec.Capabilities.Profile.Digest = testControllerDigest("wrong")
	if err := validateAgentRuntimeSpec(runtimeObject); err == nil || !strings.Contains(err.Error(), "canonical digest") {
		t.Fatalf("validateAgentRuntimeSpec() = %v, want canonical digest mismatch", err)
	}
}

func TestValidateAgentRuntimeSpecRejectsTrustedRuntimeStrictClaims(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	claims.Mode = v2conformance.WorkspaceGovernanceTrusted
	claims.Trusted = true
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "https://runtime.example.com", config)
	if err := validateAgentRuntimeSpec(runtimeObject); err == nil || !strings.Contains(err.Error(), "must not claim strict") {
		t.Fatalf("validateAgentRuntimeSpec() = %v, want trusted strict-claim rejection", err)
	}
}

func TestAgentRuntimeTrustedNonGovernedRegistrationIsExplicitAndNotStrictEligible(t *testing.T) {
	profile, _, limits := testAgentRuntimeProfileClaimsAndLimits()
	claims := v2conformance.WorkspaceGovernanceClaims{Mode: v2conformance.WorkspaceGovernanceTrusted, Trusted: true}
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "trusted-runtime-1", SupervisorBootID: "boot-1", RuntimePoolUID: "external-pool-1",
		Profile: profile, Limits: limits, WorkspaceGovernance: claims,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	if runtimeObject.Spec.Capabilities.SupportsStrictWorkspaceIntent(corev1alpha1.WorkspaceIntentRead) ||
		runtimeObject.Spec.Capabilities.SupportsStrictWorkspaceIntent(corev1alpha1.WorkspaceIntentWrite) {
		t.Fatal("trusted non-governed runtime was eligible for strict workspace intent")
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if !updated.Status.Ready || updated.Status.ObservedCapabilities == nil ||
		updated.Status.ObservedCapabilities.WorkspaceGovernance == nil ||
		!updated.Status.ObservedCapabilities.WorkspaceGovernance.Trusted {
		t.Fatalf("trusted runtime registration = %#v", updated.Status)
	}
	if server.Counts().WorkspaceDeltas != 0 {
		t.Fatal("trusted non-governed conformance unexpectedly relied on strict workspace delta production")
	}
}

func TestAgentRuntimeReconcilerFailsClosedWhenStatusIsUnauthenticated(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot-1", RuntimePoolUID: "pool-1",
		Profile: profile, Limits: limits, WorkspaceGovernance: claims, AllowUnauthenticatedStatus: true,
	}
	server, err := conformancetest.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	runtimeObject, secret := testAgentRuntimeAndSecret(t, server.URL(), config)
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	allowAgentRuntimeLoopback(t)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "status negative probe") {
		t.Fatalf("unauthenticated status exposure = %#v", updated.Status)
	}
}

func TestAgentRuntimeReconcilerRejectsMissingCapabilitySecretKey(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{
		ControllerBearerToken: strings.Repeat("t", 32), OperationCapabilitySecret: []byte(strings.Repeat("s", 32)),
		RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims,
	}
	runtimeObject, secret := testAgentRuntimeAndSecret(t, "https://runtime.example.com", config)
	delete(secret.Data, "capability-secret")
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, secret)
	if _, err := reconciler.Reconcile(t.Context(), reconcileRequestFor(runtimeObject)); err != nil {
		t.Fatal(err)
	}
	updated := getAgentRuntime(t, reconciler, runtimeObject)
	if updated.Status.Ready || !strings.Contains(updated.Status.Message, "at least 32 bytes") {
		t.Fatalf("missing capability key status = %#v", updated.Status)
	}
}

func expectAgentRuntimeEndpointPolicyError(t *testing.T, r *AgentRuntimeReconciler, runtimeObject *corev1alpha1.AgentRuntime, endpoint, wantSubstr string) {
	t.Helper()
	runtimeObject.Spec.Deployment.Endpoint = endpoint
	err := r.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject)
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("endpoint %q error = %v, want %q", endpoint, err, wantSubstr)
	}
}

func TestAgentRuntimeEndpointPolicy(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "http://runtime.default.svc.cluster.local:8080", config)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "runtime"},
			Ports:    []corev1.ServicePort{{Port: 8080}},
		},
	}
	backendPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime-pod", Labels: map[string]string{"app": "runtime"}},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.9", PodIPs: []corev1.PodIP{{IP: "10.0.0.9"}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	validSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-valid",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: new(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.9"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-pod"},
		}},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, service, backendPod, validSlice)
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err != nil {
		t.Fatalf("same-namespace Service endpoint: %v", err)
	}
	// Dispatch pins to the verified backend Pod IP:port, not the Service.
	pins, err := reconciler.AgentRuntimeServiceBackendPins(t.Context(), runtimeObject)
	if err != nil {
		t.Fatalf("backend pins error: %v", err)
	}
	if len(pins) != 1 || pins[0] != "10.0.0.9:8080" {
		t.Fatalf("backend pins = %v, want [10.0.0.9:8080]", pins)
	}
	if err := reconciler.Delete(t.Context(), validSlice); err != nil {
		t.Fatal(err)
	}
	expectAgentRuntimeEndpointPolicyError(t, reconciler, runtimeObject, "http://runtime.other.svc.cluster.local:8080", "must match")
	expectAgentRuntimeEndpointPolicyError(t, reconciler, runtimeObject, "http://runtime.example.com", "https")
	for _, endpoint := range []string{
		"https://100.64.0.1", "https://198.18.0.5", "https://192.0.2.9", "https://[2002::1]",
	} {
		expectAgentRuntimeEndpointPolicyError(t, reconciler, runtimeObject, endpoint, "non-public IP")
	}
	runtimeObject.Spec.Deployment.Endpoint = "http://runtime.default.svc.cluster.local:8080"
	// A forged address that is not one of the backing Pod's IPs is rejected
	// even though the slice claims a same-namespace Pod TargetRef.
	forgedAddressSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-forged",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"169.254.169.254"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-pod"},
		}},
	}
	if err := reconciler.Create(t.Context(), forgedAddressSlice); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil || !strings.Contains(err.Error(), "not an IP of backend Pod") {
		t.Fatalf("forged EndpointSlice address error = %v", err)
	}
	if err := reconciler.Delete(t.Context(), forgedAddressSlice); err != nil {
		t.Fatal(err)
	}
	// A slice whose TargetRef Pod does not match the Service selector is
	// rejected even if the address matches that Pod's real IP.
	strayPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "stray-pod", Labels: map[string]string{"app": "other"}},
		Status:     corev1.PodStatus{PodIP: "10.0.0.20", PodIPs: []corev1.PodIP{{IP: "10.0.0.20"}}},
	}
	if err := reconciler.Create(t.Context(), strayPod); err != nil {
		t.Fatal(err)
	}
	straySlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-stray",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.20"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "stray-pod"},
		}},
	}
	if err := reconciler.Create(t.Context(), straySlice); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil || !strings.Contains(err.Error(), "selector does not select") {
		t.Fatalf("stray-pod EndpointSlice error = %v", err)
	}
	if err := reconciler.Delete(t.Context(), straySlice); err != nil {
		t.Fatal(err)
	}
	// A cross-namespace TargetRef is rejected outright.
	crossNamespaceSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-mirrored",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.9"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "other-namespace", Name: "victim"},
		}},
	}
	if err := reconciler.Create(t.Context(), crossNamespaceSlice); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil || !strings.Contains(err.Error(), "same-namespace Pod") {
		t.Fatalf("cross-namespace backend error = %v", err)
	}
	if err := reconciler.Delete(t.Context(), crossNamespaceSlice); err != nil {
		t.Fatal(err)
	}
	service.Spec.Selector = nil
	if err := reconciler.Update(t.Context(), service); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil || !strings.Contains(err.Error(), "no selector") {
		t.Fatalf("selectorless Service endpoint error = %v", err)
	}
	service.Spec.Selector = map[string]string{"app": "runtime"}
	service.Spec.Type = corev1.ServiceTypeExternalName
	service.Spec.ExternalName = "internal.other-namespace.svc.cluster.local"
	if err := reconciler.Update(t.Context(), service); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil || !strings.Contains(err.Error(), "ExternalName") {
		t.Fatalf("ExternalName Service endpoint error = %v", err)
	}
}

func TestAgentRuntimeServiceBackendPinsFailClosedWithoutEndpoints(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "http://runtime.default.svc.cluster.local:8080", config)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "runtime"},
			Ports:    []corev1.ServicePort{{Port: 8080}},
		},
	}
	// A selector-backed Service with no EndpointSlices (a rollout gap) must fail
	// closed instead of yielding zero pins that degrade to an unpinned Service
	// ClusterIP dial exempt from the public-address control.
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, service)
	if _, err := reconciler.AgentRuntimeServiceBackendPins(t.Context(), runtimeObject); err == nil ||
		!strings.Contains(err.Error(), "no verified backend endpoint for port 8080") {
		t.Fatalf("empty-backend pins error = %v", err)
	}
	if err := reconciler.validateAgentRuntimeEndpointPolicy(t.Context(), runtimeObject); err == nil ||
		!strings.Contains(err.Error(), "no verified backend endpoint for port 8080") {
		t.Fatalf("empty-backend policy error = %v", err)
	}
}

func TestAgentRuntimeServiceBackendPinsSelectsMatchingPort(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "http://runtime.default.svc.cluster.local:8080", config)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "runtime"},
			Ports: []corev1.ServicePort{
				{Name: "acp", Port: 8080},
				{Name: "metrics", Port: 9090},
			},
		},
	}
	backendPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime-pod", Labels: map[string]string{"app": "runtime"}},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.9", PodIPs: []corev1.PodIP{{IP: "10.0.0.9"}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-multiport",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports: []discoveryv1.EndpointPort{
			{Name: new("acp"), Port: new(int32(8443))},
			{Name: new("metrics"), Port: new(int32(9090))},
		},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.9"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-pod"},
		}},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, service, backendPod, slice)
	pins, err := reconciler.AgentRuntimeServiceBackendPins(t.Context(), runtimeObject)
	if err != nil {
		t.Fatalf("multi-port pins error: %v", err)
	}
	// The endpoint URL targets ServicePort 8080 ("acp"), which maps to the
	// EndpointSlice "acp" target 8443; the metrics listener (9090) must never be
	// pinned or receive controller bearer/capability traffic.
	if len(pins) != 1 || pins[0] != "10.0.0.9:8443" {
		t.Fatalf("multi-port pins = %v, want [10.0.0.9:8443]", pins)
	}
}

func TestAgentRuntimeServiceBackendPinsExcludesUnreadyEndpoints(t *testing.T) {
	profile, claims, limits := testAgentRuntimeProfileClaimsAndLimits()
	config := conformancetest.Config{RuntimeInstanceID: "runtime-1", Profile: profile, Limits: limits, WorkspaceGovernance: claims}
	runtimeObject, _ := testAgentRuntimeAndSecret(t, "http://runtime.default.svc.cluster.local:8080", config)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "runtime"},
			Ports:    []corev1.ServicePort{{Port: 8080}},
		},
	}
	pod := func(name, ip string, ready corev1.ConditionStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, Labels: map[string]string{"app": "runtime"}},
			Status: corev1.PodStatus{
				PodIP: ip, PodIPs: []corev1.PodIP{{IP: ip}},
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}},
			},
		}
	}
	readyPod := pod("runtime-ready", "10.0.0.9", corev1.ConditionTrue)
	unreadyPod := pod("runtime-unready", "10.0.0.10", corev1.ConditionFalse)
	terminatingPod := pod("runtime-terminating", "10.0.0.11", corev1.ConditionTrue)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-mixed",
			Labels: map[string]string{discoveryv1.LabelServiceName: "runtime"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Port: new(int32(8080))}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.9"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-ready"}},
			{Addresses: []string{"10.0.0.10"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true)}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-unready"}},
			{Addresses: []string{"10.0.0.11"}, Conditions: discoveryv1.EndpointConditions{Ready: new(true), Terminating: new(true)}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "runtime-terminating"}},
		},
	}
	reconciler := newAgentRuntimeUnitReconciler(t, runtimeObject, service, readyPod, unreadyPod, terminatingPod, slice)
	pins, err := reconciler.AgentRuntimeServiceBackendPins(t.Context(), runtimeObject)
	if err != nil {
		t.Fatalf("mixed-readiness pins error: %v", err)
	}
	// Only the ready, non-terminating backend is pinned; the Pod-unready backend
	// is excluded even though its EndpointSlice condition says Ready, and the
	// terminating endpoint is also excluded from the round-robin pin set.
	if len(pins) != 1 || pins[0] != "10.0.0.9:8080" {
		t.Fatalf("mixed-readiness pins = %v, want [10.0.0.9:8080]", pins)
	}
}

func TestValidateHarnessV1AgentRuntimeEndpointSpecRequiresTLS(t *testing.T) {
	for _, endpoint := range []string{
		"http://runtime.default.svc:8080",
		"http://runtime.default.svc.cluster.local:8080",
		"http://runtime.example.invalid",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if err := validateHarnessV1AgentRuntimeEndpointSpec(endpoint); err == nil || !strings.Contains(err.Error(), "must use https") {
				t.Fatalf("validateHarnessV1AgentRuntimeEndpointSpec(%q) error = %v, want TLS rejection", endpoint, err)
			}
		})
	}
	if err := validateHarnessV1AgentRuntimeEndpointSpec("https://runtime.default.svc:8443"); err != nil {
		t.Fatalf("TLS Service endpoint rejected: %v", err)
	}
}

func TestAgentRuntimeProfilePreservesModelLimits(t *testing.T) {
	base, _, _ := testAgentRuntimeProfileClaimsAndLimits()
	base.ProviderKind = runtimePoolProviderOpencode
	base.Model = "openai/gpt-test"
	base.AdapterDigests = map[string]string{"opencode": testControllerDigest("opencode-adapter")}
	base.ProxyCredentialScope = "model:openai/gpt-test"
	base.ModelLimits = &harnessv2.ModelTokenLimits{Context: 32768, Output: 4096}
	spec := corev1alpha1.AgentRuntimeProfileSpec{
		DigestSchemaVersion:      int32(harnessv2.ProfileDigestSchemaVersion),
		ACPProfile:               base.ACPProfile,
		AdapterName:              "opencode",
		AdapterDigest:            base.AdapterDigests["opencode"],
		ProviderKind:             base.ProviderKind,
		Model:                    base.Model,
		ModelLimits:              &corev1alpha1.ModelTokenLimits{Context: 32768, Output: 4096},
		AgentConfigurationDigest: base.AgentConfigurationDigest,
		ToolPolicyDigest:         base.ToolPolicyDigest,
		ApprovalPolicyDigest:     base.ApprovalPolicyDigest,
		MCPConfigurationDigest:   base.MCPConfigurationDigest,
		WorkspaceIntent:          corev1alpha1.WorkspaceIntent(base.WorkspaceIntent),
		ProxyCredentialRole:      base.ProxyCredentialRole,
		ProxyCredentialScope:     base.ProxyCredentialScope,
		ResourceClass:            base.ResourceClass,
	}
	profile, err := agentRuntimeProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ModelLimits == nil || profile.ModelLimits.Context != 32768 || profile.ModelLimits.Output != 4096 {
		t.Fatalf("model limits = %#v", profile.ModelLimits)
	}
}

func testAgentRuntimeAndSecret(t *testing.T, endpoint string, config conformancetest.Config) (*corev1alpha1.AgentRuntime, *corev1.Secret) {
	t.Helper()
	profileDigest, err := harnessv2.CanonicalProfileDigest(config.Profile)
	if err != nil {
		t.Fatal(err)
	}
	adapterName, adapterDigest := "", ""
	for adapterName, adapterDigest = range config.Profile.AdapterDigests {
		break
	}
	var apiModelLimits *corev1alpha1.ModelTokenLimits
	if config.Profile.ModelLimits != nil {
		apiModelLimits = &corev1alpha1.ModelTokenLimits{
			Context: config.Profile.ModelLimits.Context,
			Output:  config.Profile.ModelLimits.Output,
		}
	}
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime", UID: types.UID("runtime-uid"), Generation: 1,
		},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			Deployment:      corev1alpha1.AgentRuntimeDeploymentSpec{Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: endpoint},
			ClientAuth: corev1alpha1.AgentRuntimeClientAuth{
				ControllerBearerTokenSecretRef: &corev1alpha1.AgentRuntimeSecretKeyReference{Name: "runtime-auth", Key: "controller-token"},
				OperationCapabilitySecretRef:   &corev1alpha1.AgentRuntimeSecretKeyReference{Name: "runtime-auth", Key: "capability-secret"},
			},
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				RuntimeInstanceID: string(config.RuntimeInstanceID),
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					Digest: string(profileDigest), DigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
					ACPProfile: config.Profile.ACPProfile, AdapterName: adapterName, AdapterDigest: adapterDigest,
					ProviderKind: config.Profile.ProviderKind, Model: config.Profile.Model, ModelLimits: apiModelLimits,
					AgentConfigurationDigest: config.Profile.AgentConfigurationDigest,
					ToolPolicyDigest:         config.Profile.ToolPolicyDigest, ApprovalPolicyDigest: config.Profile.ApprovalPolicyDigest,
					MCPConfigurationDigest: config.Profile.MCPConfigurationDigest,
					WorkspaceIntent:        corev1alpha1.WorkspaceIntent(config.Profile.WorkspaceIntent),
					ProxyCredentialRole:    config.Profile.ProxyCredentialRole, ProxyCredentialScope: config.Profile.ProxyCredentialScope,
					ResourceClass: config.Profile.ResourceClass,
				},
				MCPPolicy: func() *corev1alpha1.AgentRuntimeMCPPolicySpec {
					policy := testAgentRuntimeMCPPolicy()
					return &policy
				}(),
				Limits: &corev1alpha1.AgentRuntimeProtocolLimits{
					MaxResidentSessions: int32(config.Limits.MaxResidentSessions), MaxConcurrentPrompts: int32(config.Limits.MaxConcurrentPrompts),
					MaxRequestBytes: int32(config.Limits.MaxRequestBytes), MaxEventLineBytes: int32(config.Limits.MaxEventLineBytes),
					MaxTerminalResultBytes: int32(config.Limits.MaxTerminalResultBytes), MaxBufferedEvents: int32(config.Limits.MaxBufferedEvents),
					MaxUpdateEventsPerSecond: int32(config.Limits.MaxUpdateEventsPerSecond), MinPromptLeaseMillis: config.Limits.MinPromptLeaseMillis,
					MaxPromptLeaseMillis: config.Limits.MaxPromptLeaseMillis, MaxPendingPermissions: int32(config.Limits.MaxPendingPermissions),
					MaxWorkspaceDeltaBytes: config.Limits.MaxWorkspaceDeltaBytes,
				},
				SupportsDrain:                   config.SupportsDrain,
				SupportsPublicationFinalization: config.SupportsPublicationFinalization,
				WorkspaceGovernance: &corev1alpha1.AgentRuntimeWorkspaceGovernanceCapabilities{
					Mode: corev1alpha1.AgentRuntimeWorkspaceGovernanceMode(config.WorkspaceGovernance.Mode), Trusted: config.WorkspaceGovernance.Trusted,
					OrkaOwnedWorkspaceDeltas:        config.WorkspaceGovernance.OrkaOwnedWorkspaceDeltas,
					PromptScopedBrokerAuthorization: config.WorkspaceGovernance.PromptScopedBrokerAuthorization,
					NoDirectSCMPublication:          config.WorkspaceGovernance.NoDirectSCMPublication,
					OrkaOwnedCleanRoomPublication:   config.WorkspaceGovernance.OrkaOwnedCleanRoomPublication,
					ExactInstanceFencing:            config.WorkspaceGovernance.ExactInstanceFencing,
					DuplicateSafeMutations:          config.WorkspaceGovernance.DuplicateSafeMutations,
					CancellationSettlement:          config.WorkspaceGovernance.CancellationSettlement,
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-auth", UID: types.UID("runtime-auth-uid"), ResourceVersion: "1",
			Labels:      map[string]string{agentRuntimeAuthUseLabel: "true", agentRuntimeAuthRefNameLabel: runtimeObject.Name},
			Annotations: map[string]string{agentRuntimeAuthEndpointAnnotation: endpoint},
		},
		Data: map[string][]byte{
			"controller-token":  []byte(config.ControllerBearerToken),
			"capability-secret": append([]byte(nil), config.OperationCapabilitySecret...),
		},
	}
	return runtimeObject, secret
}

func duplicateAgentRuntimeTestRegistration(
	runtimeObject *corev1alpha1.AgentRuntime,
	secret *corev1.Secret,
	name string,
) (*corev1alpha1.AgentRuntime, *corev1.Secret) {
	duplicate := runtimeObject.DeepCopy()
	duplicate.Name = name
	duplicate.ResourceVersion = ""
	duplicate.UID = types.UID("zz-" + name + "-uid")
	duplicate.Status = corev1alpha1.AgentRuntimeStatus{}
	duplicateSecret := secret.DeepCopy()
	duplicateSecret.Name = name + "-auth"
	duplicateSecret.ResourceVersion = "1"
	duplicateSecret.UID = types.UID(name + "-auth-uid")
	duplicateSecret.Labels[agentRuntimeAuthRefNameLabel] = name
	duplicate.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name = duplicateSecret.Name
	duplicate.Spec.ClientAuth.OperationCapabilitySecretRef.Name = duplicateSecret.Name
	return duplicate, duplicateSecret
}

type agentRuntimeDeletionTestServer struct {
	*httptest.Server
	bearer           string
	capability       []byte
	fence            harnessv2.Fence
	mu               sync.Mutex
	drainRequested   bool
	drainRequestedAt time.Time
	postDrainReads   int
	drainOperations  []harnessv2.OperationID
	drainFences      []harnessv2.Fence
	protocolErrors   []string
}

func newAgentRuntimeDeletionTestServer(
	bearer string,
	capability []byte,
	fence harnessv2.Fence,
) *agentRuntimeDeletionTestServer {
	server := &agentRuntimeDeletionTestServer{
		bearer: bearer, capability: append([]byte(nil), capability...), fence: fence,
	}
	server.Server = httptest.NewServer(http.HandlerFunc(server.handle))
	return server
}

func (s *agentRuntimeDeletionTestServer) handle(w http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodGet && request.URL.Path == harnessv2.StatusPath:
		s.handleStatus(w, request)
	case request.Method == http.MethodPut && request.URL.Path == harnessv2.DrainPath:
		s.handleDrain(w, request)
	default:
		s.protocolErrorf("unexpected request %s %s", request.Method, request.URL.Path)
		writeAgentRuntimeDeletionTestError(w, http.StatusNotFound, harnessv2.ErrorCodeInvalidRequest, "unexpected request", false)
	}
}

func (s *agentRuntimeDeletionTestServer) handleStatus(w http.ResponseWriter, request *http.Request) {
	if !s.authorizeBearer(w, request) {
		return
	}
	s.mu.Lock()
	fence := s.fence
	s.mu.Unlock()
	binding := harnessv2.StatusCapabilityBinding{
		RuntimeProfileDigest: fence.RuntimeProfileDigest,
		RuntimeInstanceID:    fence.RuntimeInstanceID,
	}
	if _, err := harnessv2.VerifyStatusCapability(
		s.capability, request.Header.Get(harnessv2.OperationCapabilityHeader), binding, time.Now().UTC(),
	); err != nil {
		s.protocolErrorf("verify status capability: %v", err)
		writeAgentRuntimeDeletionTestError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "invalid status capability", false)
		return
	}

	s.mu.Lock()
	drainRequested := s.drainRequested
	requestedAt := s.drainRequestedAt
	postDrainRead := 0
	if drainRequested {
		s.postDrainReads++
		postDrainRead = s.postDrainReads
	}
	s.mu.Unlock()

	now := time.Now().UTC()
	status := harnessv2.StatusResponse{
		Protocol:  harnessv2.ProtocolVersion,
		Fence:     fence,
		Lifecycle: harnessv2.SupervisorLifecycleReady,
		Drain:     harnessv2.DrainStatus{AcceptingNewSessions: true},
		Timestamp: now,
	}
	if drainRequested {
		status.Lifecycle = harnessv2.SupervisorLifecycleDraining
		status.Drain = harnessv2.DrainStatus{
			AcceptingNewSessions: false,
			Requested:            true,
			RequestedAt:          requestedAt,
			Reason:               "agent_runtime_deletion",
		}
	}
	if !drainRequested || postDrainRead == 1 {
		status.Sessions = []harnessv2.RuntimeSessionStatus{{
			RuntimeSessionID:  "runtime-session-1",
			RuntimeSessionUID: "session-uid-1",
			Generation:        1,
			State:             harnessv2.RuntimeSessionStateIdle,
			LastTransitionAt:  now.Add(-time.Second),
		}}
		status.Pressure.ResidentSessions = 1
	}
	writeAgentRuntimeDeletionTestJSON(w, http.StatusOK, status)
}

func (s *agentRuntimeDeletionTestServer) handleDrain(w http.ResponseWriter, request *http.Request) {
	if !s.authorizeBearer(w, request) {
		return
	}
	s.mu.Lock()
	fence := s.fence
	s.mu.Unlock()
	var drain harnessv2.DrainRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&drain); err != nil {
		s.protocolErrorf("decode drain request: %v", err)
		writeAgentRuntimeDeletionTestError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "invalid drain request", false)
		return
	}
	now := time.Now().UTC()
	if err := drain.ValidateAt(now); err != nil {
		s.protocolErrorf("validate drain request: %v", err)
		writeAgentRuntimeDeletionTestError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "invalid drain request", false)
		return
	}
	if err := harnessv2.VerifyOperationCapability(
		s.capability, request.Header.Get(harnessv2.OperationCapabilityHeader), drain.Metadata, false, now,
	); err != nil {
		s.protocolErrorf("verify drain capability: %v", err)
		writeAgentRuntimeDeletionTestError(w, http.StatusForbidden, harnessv2.ErrorCodeForbidden, "invalid drain capability", false)
		return
	}
	if mismatch := harnessv2.CompareFence(fence, drain.Metadata.Fence, false); mismatch != harnessv2.FenceMatch {
		s.protocolErrorf("drain fence mismatch: %s", mismatch)
		writeAgentRuntimeDeletionTestError(w, http.StatusGone, harnessv2.ErrorCodeStaleFence, "stale drain fence", false)
		return
	}
	if drain.Reason != "agent_runtime_deletion" {
		s.protocolErrorf("drain reason = %q", drain.Reason)
		writeAgentRuntimeDeletionTestError(w, http.StatusBadRequest, harnessv2.ErrorCodeInvalidRequest, "invalid drain reason", false)
		return
	}

	s.mu.Lock()
	s.drainOperations = append(s.drainOperations, drain.Metadata.OperationID)
	s.drainFences = append(s.drainFences, drain.Metadata.Fence)
	attempt := len(s.drainOperations)
	if attempt == 1 {
		s.mu.Unlock()
		writeAgentRuntimeDeletionTestError(w, http.StatusInternalServerError, harnessv2.ErrorCodeSessionPoisoned, "injected drain failure", true)
		return
	}
	s.drainRequested = true
	s.drainRequestedAt = now
	s.mu.Unlock()
	writeAgentRuntimeDeletionTestJSON(w, http.StatusOK, harnessv2.DrainResponse{
		Protocol:       harnessv2.ProtocolVersion,
		Classification: harnessv2.Classification{Class: harnessv2.RequestClassificationFresh},
		Drain: harnessv2.DrainStatus{
			AcceptingNewSessions: false,
			Requested:            true,
			RequestedAt:          now,
			Reason:               drain.Reason,
		},
	})
}

func (s *agentRuntimeDeletionTestServer) authorizeBearer(w http.ResponseWriter, request *http.Request) bool {
	if request.Header.Get("Authorization") == "Bearer "+s.bearer {
		return true
	}
	s.protocolErrorf("invalid bearer authorization")
	writeAgentRuntimeDeletionTestError(w, http.StatusUnauthorized, harnessv2.ErrorCodeUnauthenticated, "authentication required", false)
	return false
}

func (s *agentRuntimeDeletionTestServer) DrainOperationIDs() []harnessv2.OperationID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.drainOperations)
}

func (s *agentRuntimeDeletionTestServer) SetFence(fence harnessv2.Fence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fence = fence
}

func (s *agentRuntimeDeletionTestServer) DrainFences() []harnessv2.Fence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.drainFences)
}

func (s *agentRuntimeDeletionTestServer) ProtocolErrors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.protocolErrors)
}

func (s *agentRuntimeDeletionTestServer) protocolErrorf(format string, values ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocolErrors = append(s.protocolErrors, fmt.Sprintf(format, values...))
}

func writeAgentRuntimeDeletionTestError(
	w http.ResponseWriter,
	status int,
	code harnessv2.ErrorCode,
	message string,
	retryable bool,
) {
	writeAgentRuntimeDeletionTestJSON(w, status, harnessv2.ErrorResponse{
		Protocol: harnessv2.ProtocolVersion, Code: code, Message: message, Retryable: retryable,
	})
}

func writeAgentRuntimeDeletionTestJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func seedDeletingAgentRuntime(
	t *testing.T,
	reconciler *AgentRuntimeReconciler,
	runtimeObject *corev1alpha1.AgentRuntime,
	secretObject *corev1.Secret,
	fence harnessv2.Fence,
) {
	t.Helper()
	current := getAgentRuntime(t, reconciler, runtimeObject)
	current.Finalizers = append(current.Finalizers, agentRuntimeFinalizer)
	if err := reconciler.Update(t.Context(), &current); err != nil {
		t.Fatalf("install test cleanup finalizer: %v", err)
	}
	var currentSecret corev1.Secret
	if err := reconciler.Get(t.Context(), client.ObjectKeyFromObject(secretObject), &currentSecret); err != nil {
		t.Fatalf("get test auth Secret: %v", err)
	}
	current = getAgentRuntime(t, reconciler, runtimeObject)
	now := metav1.Now()
	current.Status = corev1alpha1.AgentRuntimeStatus{
		Ready:              true,
		ObservedGeneration: current.Generation,
		ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
			ProtocolVersion:            harnessv2.ProtocolVersion,
			RuntimeInstanceID:          string(fence.RuntimeInstanceID),
			SupervisorBootID:           string(fence.SupervisorBootID),
			ControllerEpoch:            int64(fence.ControllerEpoch),
			RuntimePoolUID:             string(fence.RuntimePoolUID),
			RuntimePoolGeneration:      int64(fence.RuntimePoolGeneration),
			RuntimeProfileDigest:       string(fence.RuntimeProfileDigest),
			ProfileDigestSchemaVersion: int32(fence.ProfileDigestSchemaVersion),
		},
		ObservedControllerAuthRefResourceVersion:      currentSecret.ResourceVersion,
		ObservedOperationCapabilityRefResourceVersion: currentSecret.ResourceVersion,
		ObservedAuthRefResourceVersion:                currentSecret.ResourceVersion,
		LastValidated:                                 &now,
	}
	if err := reconciler.Status().Update(t.Context(), &current); err != nil {
		t.Fatalf("seed test AgentRuntime status: %v", err)
	}
	current = getAgentRuntime(t, reconciler, runtimeObject)
	if err := reconciler.Delete(t.Context(), &current); err != nil {
		t.Fatalf("mark test AgentRuntime deleting: %v", err)
	}
}

func assertAgentRuntimeCleanupFinalizer(t *testing.T, runtimeObject corev1alpha1.AgentRuntime) {
	t.Helper()
	if !slices.Contains(runtimeObject.Finalizers, agentRuntimeFinalizer) {
		t.Fatalf("AgentRuntime finalizers = %v, want %q", runtimeObject.Finalizers, agentRuntimeFinalizer)
	}
}

func assertAgentRuntimeNoCleanupFinalizer(t *testing.T, runtimeObject corev1alpha1.AgentRuntime) {
	t.Helper()
	if slices.Contains(runtimeObject.Finalizers, agentRuntimeFinalizer) {
		t.Fatalf("AgentRuntime unexpectedly retained cleanup finalizer: %v", runtimeObject.Finalizers)
	}
}

func assertAgentRuntimePoolIdentityRejected(t *testing.T, runtimeObject corev1alpha1.AgentRuntime, ownerName string) {
	t.Helper()
	if runtimeObject.Status.Ready || runtimeObject.Status.ObservedCapabilities != nil {
		t.Fatalf("duplicate pool identity status = %#v", runtimeObject.Status)
	}
	if runtimeObject.Status.ObservedControllerAuthRefResourceVersion != "" ||
		runtimeObject.Status.ObservedOperationCapabilityRefResourceVersion != "" ||
		runtimeObject.Status.ObservedAuthRefResourceVersion != "" {
		t.Fatalf("duplicate pool identity retained auth versions: %#v", runtimeObject.Status)
	}
	if !strings.Contains(runtimeObject.Status.Message, ownerName) {
		t.Fatalf("duplicate pool identity message = %q, want owner %q", runtimeObject.Status.Message, ownerName)
	}
}

func assertAgentRuntimeMCPPreAuth(
	t *testing.T,
	reconciler *AgentRuntimeReconciler,
	poolUID string,
	bearer string,
) {
	t.Helper()
	resolver := KubernetesACPMCPBrokerCredentialResolver{
		Reader: reconciler.APIReader,
		Epochs: reconciler.ControllerEpochManager,
	}
	if err := resolver.PreAuthenticateACPMCPBroker(
		t.Context(), "default", poolUID, "Bearer "+bearer,
	); err != nil {
		t.Fatalf("incumbent MCP pre-authentication: %v", err)
	}
}

func testHarnessV1AgentRuntimeAndSecret(endpoint string) (*corev1alpha1.AgentRuntime, *corev1.Secret) {
	supportsCancel := true
	supportsRuntimeSessions := true
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "runtime-v1", Generation: 1},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
			Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{
				Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: endpoint,
			},
			ClientAuth: corev1alpha1.AgentRuntimeClientAuth{
				BearerAuthRef: &corev1alpha1.AgentRuntimeBearerAuthReference{Name: "runtime-v1-auth", Key: "token"},
			},
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				ToolExecutionModes: []corev1alpha1.AgentRuntimeToolExecutionMode{
					corev1alpha1.AgentRuntimeToolExecutionModeObserved,
				},
				SupportsCancel:          &supportsCancel,
				SupportsRuntimeSessions: &supportsRuntimeSessions,
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "runtime-v1-auth", UID: types.UID("runtime-v1-auth-uid"), ResourceVersion: "1",
			Labels: map[string]string{
				agentRuntimeAuthUseLabel: scheduledRunLabelValue, agentRuntimeAuthRefNameLabel: runtimeObject.Name,
			},
			Annotations: map[string]string{agentRuntimeAuthEndpointAnnotation: endpoint},
		},
		Data: map[string][]byte{"token": []byte(agentRuntimeV1TestBearer)},
	}
	return runtimeObject, secret
}

func testAgentRuntimeProfileClaimsAndLimits() (harnessv2.RuntimeProfile, v2conformance.WorkspaceGovernanceClaims, harnessv2.ProtocolLimits) {
	policy := testAgentRuntimeMCPPolicy()
	toolPolicyDigest, _ := harnessv2.CanonicalRuntimeToolPolicyDigest(policy.AllowedTools, policy.DisallowedTools, policy.AllowBash)
	approvalPolicyDigest, _ := harnessv2.CanonicalMCPApprovalPolicyDigest(agentRuntimeMCPApprovalPolicy(&policy))
	mcpConfigurationDigest, _ := harnessv2.CanonicalMCPConfigurationDigest(policy.AllowedTools)
	profile := harnessv2.RuntimeProfile{
		ACPProfile:     harnessv2.ACPProfileV1,
		AdapterDigests: map[string]string{"codex": testControllerDigest("adapter")},
		ProviderKind:   "codex", Model: acpTestModel,
		AgentConfigurationDigest: testControllerDigest("agent"), ToolPolicyDigest: toolPolicyDigest,
		ApprovalPolicyDigest: approvalPolicyDigest, MCPConfigurationDigest: mcpConfigurationDigest,
		WorkspaceIntent:     harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole: "provider-proxy", ProxyCredentialScope: "session-and-prompt", ResourceClass: "standard",
	}
	claims := v2conformance.WorkspaceGovernanceClaims{
		Mode:                     v2conformance.WorkspaceGovernanceStrict,
		OrkaOwnedWorkspaceDeltas: true, PromptScopedBrokerAuthorization: true,
		NoDirectSCMPublication: true, OrkaOwnedCleanRoomPublication: true,
		ExactInstanceFencing: true, DuplicateSafeMutations: true, CancellationSettlement: true,
	}
	return profile, claims, harnessv2.DefaultProtocolLimits()
}

func testAgentRuntimeMCPPolicy() corev1alpha1.AgentRuntimeMCPPolicySpec {
	return corev1alpha1.AgentRuntimeMCPPolicySpec{
		AllowedTools:          []string{},
		DisallowedTools:       []string{},
		ApprovalRequiredTools: []string{},
	}
}

func testControllerDigest(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func reconcileRequestFor(object client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(object)}
}

func newAgentRuntimeUnitReconciler(t *testing.T, objects ...client.Object) *AgentRuntimeReconciler {
	t.Helper()
	v2AuthSecrets := map[client.ObjectKey]struct{}{}
	for _, object := range objects {
		runtimeObject, ok := object.(*corev1alpha1.AgentRuntime)
		if !ok || runtimeObject.RegisteredContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 {
			continue
		}
		if runtimeObject.UID == "" {
			runtimeObject.UID = types.UID("test-" + runtimeObject.Namespace + "-" + runtimeObject.Name + "-uid")
		}
		for _, ref := range []*corev1alpha1.AgentRuntimeSecretKeyReference{
			runtimeObject.Spec.ClientAuth.ControllerBearerTokenSecretRef,
			runtimeObject.Spec.ClientAuth.OperationCapabilitySecretRef,
		} {
			if ref != nil {
				v2AuthSecrets[client.ObjectKey{Namespace: runtimeObject.Namespace, Name: ref.Name}] = struct{}{}
			}
		}
	}
	for _, object := range objects {
		secret, ok := object.(*corev1.Secret)
		if !ok || secret.UID != "" {
			continue
		}
		if _, referenced := v2AuthSecrets[client.ObjectKeyFromObject(secret)]; referenced {
			secret.UID = types.UID("test-" + secret.Namespace + "-" + secret.Name + "-uid")
		}
	}
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.AgentRuntime{}, &corev1alpha1.Task{}).
		WithObjects(objects...).Build()
	return &AgentRuntimeReconciler{
		Client: fakeClient, APIReader: fakeClient, Scheme: scheme,
		ControllerEpochManager: readyAgentRuntimeTestEpochManager(1),
	}
}

func readyAgentRuntimeTestEpochManager(epoch int64) *ControllerEpochManager {
	manager := NewControllerEpochManager(nil, "agent-runtime-test-controller")
	manager.current = &store.ControllerEpoch{
		Name: store.DefaultControllerEpochName, Epoch: epoch, HolderID: manager.HolderID,
	}
	close(manager.ready)
	return manager
}

func setAgentRuntimeTestControllerEpoch(manager *ControllerEpochManager, epoch int64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.current = &store.ControllerEpoch{
		Name: store.DefaultControllerEpochName, Epoch: epoch, HolderID: manager.HolderID,
	}
}

func getAgentRuntime(t *testing.T, reconciler *AgentRuntimeReconciler, object *corev1alpha1.AgentRuntime) corev1alpha1.AgentRuntime {
	t.Helper()
	var updated corev1alpha1.AgentRuntime
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(object), &updated); err != nil {
		t.Fatal(err)
	}
	return updated
}

func allowAgentRuntimeLoopback(t *testing.T) {
	t.Helper()
	agentRuntimeAllowInsecureLoopbackForTests = true
	t.Cleanup(func() { agentRuntimeAllowInsecureLoopbackForTests = false })
}

type agentRuntimeMutateSecondSecretReadClient struct {
	client.Client
	SecretKey client.ObjectKey
	Mutate    func(*corev1.Secret)
	reads     int
}

type agentRuntimeFailCleanupSecretCreateClient struct {
	client.Client
	failed bool
}

func (c *agentRuntimeFailCleanupSecretCreateClient) Create(
	ctx context.Context,
	object client.Object,
	options ...client.CreateOption,
) error {
	secret, ok := object.(*corev1.Secret)
	if ok && secret.Type == agentRuntimeCleanupSecretType && !c.failed {
		c.failed = true
		return fmt.Errorf("injected cleanup Secret create failure")
	}
	return c.Client.Create(ctx, object, options...)
}

func (c *agentRuntimeMutateSecondSecretReadClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if err := c.Client.Get(ctx, key, object, options...); err != nil {
		return err
	}
	secret, ok := object.(*corev1.Secret)
	if !ok || key != c.SecretKey {
		return nil
	}
	c.reads++
	if c.reads == 2 && c.Mutate != nil {
		c.Mutate(secret)
	}
	return nil
}
