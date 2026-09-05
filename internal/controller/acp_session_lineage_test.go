/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	kubestore "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type acpLineageTestFixture struct {
	continuity  *ACPSessionContinuity
	controls    *kubestore.Store
	persistence *sqlite.Store
	fence       store.ControllerEpochFence
}

func newACPLineageTestFixture(t *testing.T, wrapProjection func(store.SessionLineageStore) store.SessionLineageStore) *acpLineageTestFixture {
	t.Helper()
	db, err := sqlite.NewDB(filepath.Join(t.TempDir(), "lineage.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	persistence := sqlite.NewStore(db, "lineage-test")

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	rawClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.ControllerEpoch{}, &corev1alpha1.RuntimeSessionControl{}).
		Build()
	rawClient = withControllerEpochLeaseUIDs(t, rawClient)
	controls, err := kubestore.NewComposite(rawClient, "orka-system", persistence)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := controls.CompareAndSwapControllerEpoch(context.Background(), store.ControllerEpochCAS{
		ExpectedVersion: 0, ExpectedEpoch: 0, NewEpoch: 1,
		HolderID: "lineage-controller", RequestDigest: acpSessionTestDigest("lineage-epoch"),
		UpdatedAt: time.Date(2026, 8, 5, 9, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := store.SessionLineageStore(persistence)
	if wrapProjection != nil {
		projection = wrapProjection(projection)
	}
	continuity, err := NewACPSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: controls, Transcripts: persistence, Publications: controls, BranchClaims: controls,
		GatewayEvents: persistence, Lineages: projection,
		NewSessionUID: func() (string, error) { return "acp-lineage-session-uid", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &acpLineageTestFixture{
		continuity: continuity, controls: controls, persistence: persistence,
		fence: store.ControllerEpochFence{Name: epoch.Name, Epoch: epoch.Epoch, HolderID: epoch.HolderID},
	}
}

func (f *acpLineageTestFixture) ensureSession(t *testing.T, name string) *store.SessionControl {
	t.Helper()
	control, err := f.continuity.EnsureSession(context.Background(), ACPEnsureSessionRequest{
		Namespace: "ns", SessionName: name, SessionType: "task", Fence: f.fence,
		CreatedAt: time.Date(2026, 8, 5, 9, 5, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return control
}

func acpLineageLeaseRequest(
	control *store.SessionControl,
	fence store.ControllerEpochFence,
	taskUID string,
) ACPAcquireSessionLeaseRequest {
	return ACPAcquireSessionLeaseRequest{
		Session: *control, Fence: fence, TaskName: taskUID, TaskUID: taskUID, Attempt: 1, PromptID: "prompt-" + taskUID,
		PromptRequestDigest: acpSessionTestDigest("request-" + taskUID),
		AcquiredAt:          time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
		NamespaceUID:        "ns-uid-1",
		RuntimeIdentity:     "codex",
		ConfigDigest:        acpSessionTestDigest("profile-1"),
	}
}

func TestAcquireMutationLeaseEstablishesKubernetesAuthoritativeLineage(t *testing.T) {
	ctx := context.Background()
	fixture := newACPLineageTestFixture(t, nil)
	control := fixture.ensureSession(t, "lineage-session")

	lease, err := fixture.continuity.AcquireMutationLease(ctx, acpLineageLeaseRequest(control, fixture.fence, "task-1"))
	if err != nil {
		t.Fatalf("acquire with lineage: %v", err)
	}
	if lease.Session.Lineage == nil || lease.Session.Lineage.ContractVersion != string(corev1alpha1.AgentRuntimeContractHarnessV2) ||
		lease.Session.Lineage.RuntimeIdentity != "codex" || lease.Session.Lineage.NamespaceUID != "ns-uid-1" ||
		lease.Session.Lineage.LineageGeneration != 1 || lease.Session.Lineage.ConfigDigest != acpSessionTestDigest("profile-1") {
		t.Fatalf("authoritative lineage = %+v", lease.Session.Lineage)
	}
	authoritative, err := fixture.controls.GetSessionControl(ctx, "ns", "lineage-session")
	if err != nil {
		t.Fatal(err)
	}
	if authoritative.Lineage == nil || authoritative.Lease == nil || authoritative.Version != 2 {
		t.Fatalf("lineage and Lease were not committed together: %+v", authoritative)
	}
	projected, err := fixture.persistence.GetSessionLineage(ctx, "ns", "lineage-session")
	if err != nil {
		t.Fatal(err)
	}
	if projected.SessionUID != authoritative.SessionUID || projected.ConfigDigest != authoritative.Lineage.ConfigDigest {
		t.Fatalf("SQLite projection = %+v, authority = %+v", projected, authoritative.Lineage)
	}
}

func TestAcquireMutationLeaseRejectsNonemptyUnclassifiedSession(t *testing.T) {
	ctx := context.Background()
	fixture := newACPLineageTestFixture(t, nil)
	control := fixture.ensureSession(t, "unclassified-session")
	if err := fixture.persistence.AppendMessages(ctx, "ns", "unclassified-session", []store.SessionMessage{{Role: "user", Content: "legacy transcript"}}); err != nil {
		t.Fatal(err)
	}

	_, err := fixture.continuity.AcquireMutationLease(ctx, acpLineageLeaseRequest(control, fixture.fence, "task-1"))
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("nonempty unclassified acquisition error = %v, want conflict", err)
	}
	after, getErr := fixture.controls.GetSessionControl(ctx, "ns", "unclassified-session")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if after.Lineage != nil || after.Lease != nil || after.LeaseGeneration != 0 {
		t.Fatalf("unclassified Session mutated authority: %+v", after)
	}
	if _, getErr := fixture.persistence.GetSessionLineage(ctx, "ns", "unclassified-session"); !errors.Is(getErr, store.ErrNotFound) {
		t.Fatalf("unclassified Session gained SQLite lineage: %v", getErr)
	}
}

func TestAcquireMutationLeaseRequiresLinkedGatewayTask(t *testing.T) {
	ctx := context.Background()
	fixture := newACPLineageTestFixture(t, nil)
	now := time.Date(2026, 8, 5, 9, 30, 0, 0, time.UTC)
	event := store.GatewayEvent{
		ID: "gateway-lineage-event", Namespace: "ns", NamespaceUID: "ns-uid-1",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "gateway",
		ExternalEventID: "external-gateway-lineage-event", ProtocolVersion: "orka.gateway.v1", EventType: "text",
		AccountID: "account", ContextID: "context", SenderID: "sender", Text: "gateway prompt",
		SessionName: "gateway-lineage-session", TaskName: "gateway-task",
		NextAttemptAt: now, ReceivedAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := fixture.persistence.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil || !created {
		t.Fatalf("admit Gateway event = (created=%v, err=%v)", created, err)
	}
	control, err := fixture.continuity.EnsureSession(ctx, ACPEnsureSessionRequest{
		Namespace: event.Namespace, SessionName: event.SessionName, SessionType: store.SessionTypeGateway,
		RequireExistingTranscript: true, Fence: fixture.fence, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := acpLineageLeaseRequest(control, fixture.fence, "gateway-task-uid")
	request.TaskName = event.TaskName
	if _, err := fixture.continuity.AcquireMutationLease(ctx, request); !errors.Is(err, store.ErrNotReady) {
		t.Fatalf("unlinked Gateway Task acquisition error = %v, want ErrNotReady", err)
	}
	after, err := fixture.controls.GetSessionControl(ctx, event.Namespace, event.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if after.Lineage != nil || after.Lease != nil || after.LeaseGeneration != 0 {
		t.Fatalf("unlinked Gateway Task mutated Session authority: %+v", after)
	}
	if _, err := fixture.persistence.ClaimNextGatewayEvent(ctx, event.Namespace, "gateway-owner", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := fixture.persistence.MarkGatewayEventTaskCreated(
		ctx, event.Namespace, event.ID, event.TaskName, request.TaskUID, "gateway-owner", now,
	); err != nil {
		t.Fatal(err)
	}
	request.Session = *after
	request.TaskName = "unrelated-task"
	request.TaskUID = "unrelated-task-uid"
	if _, err := fixture.continuity.AcquireMutationLease(ctx, request); !errors.Is(err, store.ErrNotReady) {
		t.Fatalf("unrelated Gateway Task acquisition error = %v, want ErrNotReady", err)
	}
	request.TaskName = event.TaskName
	request.TaskUID = "gateway-task-uid"
	lease, err := fixture.continuity.AcquireMutationLease(ctx, request)
	if err != nil {
		t.Fatalf("linked Gateway Task acquisition error = %v", err)
	}
	if lease.Session.Lineage == nil || lease.Session.Lease == nil || lease.Session.Lease.TaskUID != request.TaskUID {
		t.Fatalf("linked Gateway Task lease = %+v", lease.Session)
	}
}

func TestAcquireMutationLeaseRejectsLineageIdentityAndConfigMismatch(t *testing.T) {
	ctx := context.Background()
	fixture := newACPLineageTestFixture(t, nil)
	control := fixture.ensureSession(t, "mismatch-session")
	lease, err := fixture.continuity.AcquireMutationLease(ctx, acpLineageLeaseRequest(control, fixture.fence, "task-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.continuity.ReleaseMutationLease(ctx, ACPReleaseSessionLeaseRequest{Lease: *lease, Fence: fixture.fence}); err != nil {
		t.Fatal(err)
	}
	baseline, err := fixture.controls.GetSessionControl(ctx, "ns", "mismatch-session")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*ACPAcquireSessionLeaseRequest)
	}{
		{name: "runtime", mutate: func(request *ACPAcquireSessionLeaseRequest) { request.RuntimeIdentity = "opencode" }},
		{name: "configuration", mutate: func(request *ACPAcquireSessionLeaseRequest) { request.ConfigDigest = acpSessionTestDigest("profile-2") }},
		{name: "namespace UID", mutate: func(request *ACPAcquireSessionLeaseRequest) { request.NamespaceUID = "ns-uid-recreated" }},
		{name: "Session UID", mutate: func(request *ACPAcquireSessionLeaseRequest) { request.Session.SessionUID = "session-uid-recreated" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := acpLineageLeaseRequest(baseline, fixture.fence, "task-2")
			test.mutate(&request)
			if _, err := fixture.continuity.AcquireMutationLease(ctx, request); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("mismatch error = %v, want conflict", err)
			}
			after, err := fixture.controls.GetSessionControl(ctx, "ns", "mismatch-session")
			if err != nil {
				t.Fatal(err)
			}
			if after.Lease != nil || after.LeaseGeneration != baseline.LeaseGeneration || after.Lineage == nil || after.Lineage.RuntimeIdentity != "codex" {
				t.Fatalf("mismatch changed Session authority: %+v", after)
			}
		})
	}
}

type failOnceLineageProjection struct {
	store.SessionLineageStore
	fail bool
}

func (s *failOnceLineageProjection) ProjectSessionLineage(ctx context.Context, lineage store.SessionLineage) (*store.SessionLineage, error) {
	if s.fail {
		s.fail = false
		return nil, errors.New("simulated SQLite projection failure")
	}
	return s.SessionLineageStore.ProjectSessionLineage(ctx, lineage)
}

func TestAcquireMutationLeaseRecoversProjectionAfterKubernetesCommit(t *testing.T) {
	ctx := context.Background()
	var projection *failOnceLineageProjection
	fixture := newACPLineageTestFixture(t, func(delegate store.SessionLineageStore) store.SessionLineageStore {
		projection = &failOnceLineageProjection{SessionLineageStore: delegate, fail: true}
		return projection
	})
	control := fixture.ensureSession(t, "projection-recovery")
	request := acpLineageLeaseRequest(control, fixture.fence, "task-1")

	if _, err := fixture.continuity.AcquireMutationLease(ctx, request); err == nil {
		t.Fatal("expected simulated projection failure")
	}
	authoritative, err := fixture.controls.GetSessionControl(ctx, "ns", "projection-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if authoritative.Lineage == nil || authoritative.Lease == nil {
		t.Fatalf("Kubernetes must retain lineage and exact Lease after projection failure: %+v", authoritative)
	}
	if _, err := fixture.persistence.GetSessionLineage(ctx, "ns", "projection-recovery"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed projection unexpectedly became visible: %v", err)
	}

	request.Session = *authoritative
	recovered, err := fixture.continuity.AcquireMutationLease(ctx, request)
	if err != nil {
		t.Fatalf("retry exact authoritative Lease: %v", err)
	}
	if recovered.Key.LeaseGeneration != authoritative.LeaseGeneration {
		t.Fatalf("retry advanced Lease generation: got %d want %d", recovered.Key.LeaseGeneration, authoritative.LeaseGeneration)
	}
	if _, err := fixture.persistence.GetSessionLineage(ctx, "ns", "projection-recovery"); err != nil {
		t.Fatalf("projection was not recovered: %v", err)
	}
}
