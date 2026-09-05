package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

const artifactAuthorizationReplacementSecretUID = "replacement-secret-uid"

func TestACPArtifactAuthorizationBrokerIssuesExactUploadCapability(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	poolUID := types.UID("pool-uid")
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pool", UID: poolUID, Generation: 1},
		Status:     corev1alpha1.RuntimePoolStatus{ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{PodNamespace: "orka-runtimes", RuntimeInstanceID: "runtime-1", ControllerEpoch: 1}},
	}
	controllerToken := strings.Repeat("t", 32)
	operationSecret := []byte(strings.Repeat("s", 32))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1-randomized", Labels: map[string]string{runtimePoolAuthLabelAPI: runtimePoolAuthLabelValueAPI, runtimePoolUIDLabelAPI: string(poolUID), runtimePoolCredentialEpochLabelAPI: "1"}}, Data: map[string][]byte{runtimePoolControllerTokenKeyAPI: []byte(controllerToken), runtimePoolCapabilitySecretKeyAPI: operationSecret}}
	taskUID := types.UID("task-uid")
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: taskUID}, Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{State: corev1alpha1.TaskExecutionStateSettling, PromptID: "prompt-1", RuntimeSessionUID: "session-1", RuntimeInstanceID: "runtime-1"}}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, secret, task).Build()
	artifactSecret := []byte(strings.Repeat("a", 32))
	secretFile := filepath.Join(t.TempDir(), "artifact-secret")
	if err := os.WriteFile(secretFile, artifactSecret, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envACPArtifactSecretFile, secretFile)

	artifactData := []byte("delta")
	digest := artifactcap.DigestBytes(artifactData)
	artifactID, _ := artifactcap.ArtifactIDForDigest(digest)
	metadata := harnessv2.MutationMetadata{
		Fence:   harnessv2.Fence{RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot", ControllerEpoch: 1, RuntimePoolUID: harnessv2.RuntimePoolUID(poolUID), RuntimePoolGeneration: 1, RuntimeSessionUID: "session-1", RuntimeSessionGeneration: 1, RuntimeProfileDigest: harnessv2.ProfileDigest("sha256:" + strings.Repeat("b", 64)), ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion},
		TaskUID: harnessv2.TaskUID(taskUID), TaskAttempt: 1, PromptID: "prompt-1", OperationID: "authorize-delta-1", RequestDigestSchemaVersion: 1, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	request := acpArtifactAuthorizationRequest{Namespace: "default", Metadata: metadata, Artifact: harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(artifactID), Digest: digest, SizeBytes: int64(len(artifactData)), MediaType: artifactcap.MediaTypeWorkspaceDelta}}
	requestDigest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata.RequestDigest = requestDigest
	capability, err := harnessv2.SignOperationCapability(operationSecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(request)
	app := fiber.New()
	reservations := &recordingCapabilityReservations{}
	server := &Server{app: app, client: kubeClient, config: ServerConfig{ArtifactReservations: reservations}}
	server.installACPArtifactAuthorizationBroker()
	httpRequest := httptest.NewRequest(http.MethodPost, acpArtifactAuthorizationPath, bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+controllerToken)
	httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	httpRequest.Header.Set(harnessv2.MCPBrokerPoolNamespaceHeader, "default")
	httpRequest.Header.Set(harnessv2.MCPBrokerPoolUIDHeader, string(poolUID))
	reservationStart := time.Now().UTC()
	response, err := app.Test(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	var issued acpArtifactAuthorizationResponse
	if err := json.NewDecoder(response.Body).Decode(&issued); err != nil {
		t.Fatal(err)
	}
	presented := artifactcap.PresentedRequest{Method: http.MethodPut, Path: mustArtifactPath(t, digest), ObjectDigest: digest, ContentLength: int64(len(artifactData)), MediaType: artifactcap.MediaTypeWorkspaceDelta, RequestDigest: issued.RequestDigest}
	if _, err := artifactcap.Verify(artifactSecret, issued.Capability, presented, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if len(reservations.requests) != 1 || reservations.requests[0] != (artifactcap.OperationRequest{
		Operation: artifactcap.OperationUpload, ObjectDigest: digest,
		Identity:      artifactcap.Identity{Namespace: "default", TaskID: string(taskUID)},
		ContentLength: int64(len(artifactData)), MediaType: artifactcap.MediaTypeWorkspaceDelta,
		OperationID: "runtime-delta-upload-authorize-delta-1",
	}) {
		t.Fatalf("capability reservations = %#v, want exact upload binding", reservations.requests)
	}
	minimumExpiry := reservationStart.Add(2*time.Minute + artifactcap.MaxClockSkew)
	if reservations.expiresAt[0].Before(minimumExpiry) {
		t.Fatalf("capability reservation expiry = %s, want at least %s", reservations.expiresAt[0], minimumExpiry)
	}
}

func TestACPArtifactAuthorizationBrokerExternalRuntimeUsesFrozenAuthority(t *testing.T) {
	const maxWorkspaceDeltaBytes = int64(16)
	tests := []struct {
		name                    string
		rotateSecrets           bool
		draining                bool
		frozenMaxWorkspaceBytes int64
		artifactSizeBytes       int64
		wantStatus              int
	}{
		{
			name: "artifact at frozen byte limit", frozenMaxWorkspaceBytes: maxWorkspaceDeltaBytes,
			artifactSizeBytes: maxWorkspaceDeltaBytes, wantStatus: http.StatusOK,
		},
		{
			name: "draining runtime with exact frozen authority", draining: true,
			frozenMaxWorkspaceBytes: maxWorkspaceDeltaBytes, artifactSizeBytes: maxWorkspaceDeltaBytes,
			wantStatus: http.StatusOK,
		},
		{
			name: "artifact exceeds frozen byte limit", frozenMaxWorkspaceBytes: maxWorkspaceDeltaBytes,
			artifactSizeBytes: maxWorkspaceDeltaBytes + 1, wantStatus: http.StatusForbidden,
		},
		{
			name: "invalid zero frozen byte limit", frozenMaxWorkspaceBytes: 0,
			artifactSizeBytes: 1, wantStatus: http.StatusForbidden,
		},
		{
			name: "post-binding Secret rotation", rotateSecrets: true,
			frozenMaxWorkspaceBytes: maxWorkspaceDeltaBytes, artifactSizeBytes: maxWorkspaceDeltaBytes,
			wantStatus: http.StatusForbidden,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExternalArtifactAuthorizationFixture(
				t, test.rotateSecrets, test.draining, test.frozenMaxWorkspaceBytes, test.artifactSizeBytes,
			)
			response, err := fixture.server.app.Test(fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			wantReservations := 0
			if test.wantStatus == http.StatusOK {
				wantReservations = 1
			}
			if len(fixture.reservations.requests) != wantReservations {
				t.Fatalf("capability reservations = %d, want %d", len(fixture.reservations.requests), wantReservations)
			}
		})
	}
}

type externalArtifactAuthorizationFixture struct {
	server       *Server
	request      *http.Request
	reservations *recordingCapabilityReservations
}

func newExternalArtifactAuthorizationFixture(
	t *testing.T,
	rotateSecrets bool,
	draining bool,
	frozenMaxWorkspaceBytes int64,
	artifactSizeBytes int64,
) externalArtifactAuthorizationFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	const (
		namespace       = "default"
		runtimeName     = "external-v2"
		runtimeUID      = "external-runtime-uid"
		externalPoolUID = "external-pool-uid"
		controllerName  = "external-controller-auth"
		controllerKey   = "token"
		capabilityName  = "external-capability-auth"
		capabilityKey   = "secret"
	)
	profileDigest := "sha256:" + strings.Repeat("b", 64)
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	governance := &corev1alpha1.AgentRuntimeWorkspaceGovernanceCapabilities{
		Mode:                     corev1alpha1.AgentRuntimeWorkspaceGovernanceStrict,
		OrkaOwnedWorkspaceDeltas: true, PromptScopedBrokerAuthorization: true,
		NoDirectSCMPublication: true, OrkaOwnedCleanRoomPublication: true,
		ExactInstanceFencing: true, DuplicateSafeMutations: true, CancellationSettlement: true,
	}
	runtimeObject := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: runtimeName, UID: runtimeUID, Generation: 1},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Deployment: corev1alpha1.AgentRuntimeDeploymentSpec{
				Mode: corev1alpha1.AgentRuntimeDeploymentModeExternalEndpoint, Endpoint: "https://runtime.example",
			},
			ClientAuth: corev1alpha1.AgentRuntimeClientAuth{
				ControllerBearerTokenSecretRef: &corev1alpha1.AgentRuntimeSecretKeyReference{Name: controllerName, Key: controllerKey},
				OperationCapabilitySecretRef:   &corev1alpha1.AgentRuntimeSecretKeyReference{Name: capabilityName, Key: capabilityKey},
			},
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				RuntimeInstanceID: "runtime-1",
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					Digest: profileDigest, DigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
					WorkspaceIntent: corev1alpha1.WorkspaceIntentWrite,
				},
				WorkspaceGovernance: governance, SupportsPublicationFinalization: true,
			},
		},
		Status: corev1alpha1.AgentRuntimeStatus{
			Ready: true, ObservedGeneration: 1,
			ObservedControllerAuthRefResourceVersion:      "1",
			ObservedOperationCapabilityRefResourceVersion: "1",
			ObservedCapabilities: &corev1alpha1.AgentRuntimeObservedCapabilities{
				ProtocolVersion: harnessv2.ProtocolVersion, RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot-1",
				ControllerEpoch: 1, RuntimePoolUID: externalPoolUID, RuntimePoolGeneration: 1,
				RuntimeProfileDigest: profileDigest, ProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
				SupportsPublicationFinalization: true,
			},
		},
	}
	controllerToken := []byte(strings.Repeat("t", 32))
	operationSecret := []byte(strings.Repeat("s", 32))
	controllerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: controllerName, UID: "controller-secret-uid", ResourceVersion: "1"},
		Data:       map[string][]byte{controllerKey: controllerToken},
	}
	capabilitySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: capabilityName, UID: "capability-secret-uid", ResourceVersion: "1"},
		Data:       map[string][]byte{capabilityKey: operationSecret},
	}

	taskUID := types.UID("external-task-uid")
	frozenLimits := harnessv2.DefaultProtocolLimits()
	frozenLimits.MaxWorkspaceDeltaBytes = frozenMaxWorkspaceBytes
	frozen := acpArtifactExecutionSnapshot{
		SchemaVersion:   store.AgentExecutionSnapshotSchemaVersion,
		ContractVersion: string(corev1alpha1.AgentRuntimeContractHarnessV2),
		Backend:         string(corev1alpha1.AgentExecutionBackendExternalEndpoint), ProfileDigest: profileDigest,
		ExternalRuntime: &acpArtifactExternalRuntimeExecutionSnapshot{
			Namespace: namespace, Endpoint: runtimeObject.Spec.Deployment.Endpoint, RuntimeInstanceID: "runtime-1",
			Limits: frozenLimits, SupportsPublicationFinalization: true,
			ControllerAuth: acpArtifactExecutionSnapshotSecretRef{
				Role: "controller-auth", Namespace: namespace, Name: controllerName,
				UID: string(controllerSecret.UID), ResourceVersion: controllerSecret.ResourceVersion, Keys: []string{controllerKey},
			},
			OperationCapability: acpArtifactExecutionSnapshotSecretRef{
				Role: "operation-capability", Namespace: namespace, Name: capabilityName,
				UID: string(capabilitySecret.UID), ResourceVersion: capabilitySecret.ResourceVersion, Keys: []string{capabilityKey},
			},
		},
	}
	snapshotBody, err := harnessv2.CanonicalValue(frozen)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDigest := store.CanonicalAgentExecutionSnapshotDigest(snapshotBody)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "external-task", UID: taskUID, Generation: 1},
		Status: corev1alpha1.TaskStatus{
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				SchemaVersion: 1, ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
				Backend: corev1alpha1.AgentExecutionBackendExternalEndpoint,
				Task:    corev1alpha1.AgentExecutionBindingTaskRef{UID: taskUID, BoundSpecGeneration: 1},
				Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
					ID:     (store.AgentExecutionSnapshotKey{TaskUID: string(taskUID), Digest: snapshotDigest}).ID(),
					Digest: snapshotDigest, SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
				},
				RuntimeRef:           &corev1alpha1.AgentExecutionRuntimeRef{Name: runtimeName, UID: runtimeUID, Generation: 1},
				RuntimeProfileDigest: profileDigest, RuntimeProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
			},
			Execution: &corev1alpha1.TaskExecutionStatus{
				State: corev1alpha1.TaskExecutionStateRunning, Attempt: 1, PromptID: "prompt-1",
				AgentRuntimeName: runtimeName, AgentRuntimeUID: runtimeUID, RuntimeInstanceID: "runtime-1",
				RuntimeSessionUID: "session-1", RuntimeSessionGeneration: 1, ControllerEpoch: 1,
			},
		},
	}
	snapshotStore := &fixedAgentExecutionSnapshotStore{snapshot: &store.AgentExecutionSnapshot{
		TaskUID: string(taskUID), Digest: snapshotDigest, SchemaVersion: store.AgentExecutionSnapshotSchemaVersion, Body: snapshotBody,
	}}
	if rotateSecrets {
		controllerToken = []byte(strings.Repeat("r", 32))
		operationSecret = []byte(strings.Repeat("n", 32))
		controllerSecret.UID = "rotated-controller-secret-uid"
		controllerSecret.ResourceVersion = "2"
		controllerSecret.Data[controllerKey] = controllerToken
		capabilitySecret.UID = "rotated-capability-secret-uid"
		capabilitySecret.ResourceVersion = "2"
		capabilitySecret.Data[capabilityKey] = operationSecret
		runtimeObject.Status.ObservedControllerAuthRefResourceVersion = "2"
		runtimeObject.Status.ObservedOperationCapabilityRefResourceVersion = "2"
	}
	if draining {
		runtimeObject.Status.Ready = false
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject, controllerSecret, capabilitySecret, task).Build()
	reservations := &recordingCapabilityReservations{}
	app := fiber.New()
	server := &Server{app: app, client: kubeClient, config: ServerConfig{
		APIReader: kubeClient, ArtifactReservations: reservations, AgentExecutionSnapshots: snapshotStore,
		ControllerEpochs: publisherEpochSourceForTest(),
	}}
	server.installACPArtifactAuthorizationBroker()

	artifactSecret := []byte(strings.Repeat("a", 32))
	writeAPISecretFile(t, envACPArtifactSecretFile, "external-artifact", artifactSecret)
	artifactData := bytes.Repeat([]byte("d"), int(artifactSizeBytes))
	digest := artifactcap.DigestBytes(artifactData)
	artifactID, err := artifactcap.ArtifactIDForDigest(digest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := harnessv2.MutationMetadata{
		Fence: harnessv2.Fence{
			RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot-1", ControllerEpoch: 1,
			RuntimePoolUID: externalPoolUID, RuntimePoolGeneration: 1,
			RuntimeSessionUID: "session-1", RuntimeSessionGeneration: 1,
			RuntimeProfileDigest: harnessv2.ProfileDigest(profileDigest), ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		TaskUID: harnessv2.TaskUID(taskUID), TaskAttempt: 1, PromptID: "prompt-1", OperationID: "authorize-external-delta-1",
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	requestBody := acpArtifactAuthorizationRequest{
		Namespace: namespace, Metadata: metadata,
		Artifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(artifactID), Digest: digest,
			SizeBytes: int64(len(artifactData)), MediaType: artifactcap.MediaTypeWorkspaceDelta,
		},
	}
	requestDigest, err := harnessv2.CanonicalRequestDigest(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	requestBody.Metadata.RequestDigest = requestDigest
	capability, err := harnessv2.SignOperationCapability(operationSecret, harnessv2.ClaimsForMutation(requestBody.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, acpArtifactAuthorizationPath, bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+string(controllerToken))
	httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	httpRequest.Header.Set(harnessv2.MCPBrokerPoolNamespaceHeader, namespace)
	httpRequest.Header.Set(harnessv2.MCPBrokerPoolUIDHeader, externalPoolUID)
	return externalArtifactAuthorizationFixture{server: server, request: httpRequest, reservations: reservations}
}

func TestACPArtifactAuthorizationBrokerAuthenticatesBeforeBody(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	poolUID := types.UID("pool-uid")
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pool", UID: poolUID, Generation: 1},
		Status:     corev1alpha1.RuntimePoolStatus{ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{PodNamespace: "orka-runtimes", RuntimeInstanceID: "runtime-1", ControllerEpoch: 1}},
	}
	controllerToken := strings.Repeat("t", 32)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orka-runtimes", Name: "pool-auth-e1", Labels: map[string]string{runtimePoolAuthLabelAPI: runtimePoolAuthLabelValueAPI, runtimePoolUIDLabelAPI: string(poolUID)}},
		Data:       map[string][]byte{runtimePoolControllerTokenKeyAPI: []byte(controllerToken), runtimePoolCapabilitySecretKeyAPI: []byte(strings.Repeat("s", 32))},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, secret).Build()
	app := fiber.New()
	server := &Server{app: app, client: kubeClient, config: ServerConfig{ArtifactReservations: &recordingCapabilityReservations{}}}
	server.installACPArtifactAuthorizationBroker()

	cases := []struct {
		name           string
		bearer         string
		setPoolHeaders bool
		body           string
	}{
		{name: "missing pool identity headers", bearer: controllerToken, setPoolHeaders: false, body: "{}"},
		// An invalid-JSON body would yield 400 if the handler parsed it first;
		// rejecting the wrong bearer with 403 proves pre-auth precedes the body.
		{name: "wrong bearer before body parse", bearer: strings.Repeat("x", 32), setPoolHeaders: true, body: "not-json{"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpRequest := httptest.NewRequest(http.MethodPost, acpArtifactAuthorizationPath, strings.NewReader(tc.body))
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Authorization", "Bearer "+tc.bearer)
			if tc.setPoolHeaders {
				httpRequest.Header.Set(harnessv2.MCPBrokerPoolNamespaceHeader, "default")
				httpRequest.Header.Set(harnessv2.MCPBrokerPoolUIDHeader, string(poolUID))
			}
			response, err := app.Test(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != fiber.StatusForbidden {
				t.Fatalf("status = %d, want %d", response.StatusCode, fiber.StatusForbidden)
			}
		})
	}
}

type recordingCapabilityReservations struct {
	requests  []artifactcap.OperationRequest
	expiresAt []time.Time
	err       error
}

func (r *recordingCapabilityReservations) Reserve(_ context.Context, request artifactcap.OperationRequest, expiresAt time.Time) error {
	if r.err != nil {
		return r.err
	}
	r.requests = append(r.requests, request)
	r.expiresAt = append(r.expiresAt, expiresAt)
	return nil
}

func TestResolveArtifactRuntimePoolByIdentityUsesPrivateWorkspaceBinding(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	poolUID := types.UID("pool-uid")
	secretName := "pool-auth-e1-" + strings.Repeat("a", 24)
	boundUID := types.UID("bound-secret-uid")
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pool", UID: poolUID,
			Annotations: map[string]string{runtimePoolPrivateAuthBindingAPI + "1": secretName + "/" + string(boundUID)},
		},
		Spec: corev1alpha1.RuntimePoolSpec{ExecutionWorkspace: &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
			Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
		}},
		Status: corev1alpha1.RuntimePoolStatus{ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
			PodNamespace: "orka-runtimes", ControllerEpoch: 1,
		}},
	}
	immutable := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "orka-runtimes", Name: secretName, UID: boundUID,
			Labels: map[string]string{
				runtimePoolAuthLabelAPI:            runtimePoolAuthLabelValueAPI,
				runtimePoolUIDLabelAPI:             string(poolUID),
				runtimePoolCredentialEpochLabelAPI: "1",
			},
		},
		Immutable: &immutable,
		Data: map[string][]byte{
			runtimePoolControllerTokenKeyAPI:  []byte(strings.Repeat("t", 32)),
			runtimePoolCapabilitySecretKeyAPI: []byte(strings.Repeat("c", 32)),
		},
	}

	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, secret).Build()
	server := &Server{client: reader}
	_, selected, err := server.resolveArtifactRuntimePoolByIdentity(context.Background(), pool.Namespace, string(pool.UID))
	if err != nil || selected == nil || selected.UID != boundUID {
		t.Fatalf("bound artifact auth Secret = %#v, error=%v; want UID %q", selected, err, boundUID)
	}

	replacement := secret.DeepCopy()
	replacement.UID = artifactAuthorizationReplacementSecretUID
	replacementReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool.DeepCopy(), replacement).Build()
	replacementServer := &Server{client: replacementReader}
	if _, _, err := replacementServer.resolveArtifactRuntimePoolByIdentity(context.Background(), pool.Namespace, string(pool.UID)); err == nil ||
		!strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("recreated artifact auth Secret error = %v, want immutable UID rejection", err)
	}
}

func TestACPArtifactAuthorizationBrokerRejectsStaleCachedRevocationState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	poolUID := types.UID("pool-uid")
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pool", UID: poolUID, Generation: 1},
		Status: corev1alpha1.RuntimePoolStatus{ActiveInstance: &corev1alpha1.RuntimePoolActiveInstanceStatus{
			PodNamespace: "orka-runtimes", RuntimeInstanceID: "runtime-1", ControllerEpoch: 1,
		}},
	}
	controllerToken := strings.Repeat("t", 32)
	operationSecret := []byte(strings.Repeat("s", 32))
	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "orka-runtimes", Name: "pool-auth-e1", UID: "old-secret-uid",
			Labels: map[string]string{runtimePoolAuthLabelAPI: runtimePoolAuthLabelValueAPI, runtimePoolUIDLabelAPI: string(poolUID)},
		},
		Data: map[string][]byte{
			runtimePoolControllerTokenKeyAPI:  []byte(controllerToken),
			runtimePoolCapabilitySecretKeyAPI: operationSecret,
		},
	}
	taskUID := types.UID("task-uid")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: taskUID},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateSettling, PromptID: "prompt-1",
			RuntimeSessionUID: "session-1", RuntimeInstanceID: "runtime-1",
		}},
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, authSecret, task).Build()

	artifactSecret := []byte(strings.Repeat("a", 32))
	writeAPISecretFile(t, envACPArtifactSecretFile, "artifact", artifactSecret)
	artifactData := []byte("delta")
	digest := artifactcap.DigestBytes(artifactData)
	artifactID, err := artifactcap.ArtifactIDForDigest(digest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := harnessv2.MutationMetadata{
		Fence: harnessv2.Fence{
			RuntimeInstanceID: "runtime-1", SupervisorBootID: "boot", ControllerEpoch: 1,
			RuntimePoolUID: harnessv2.RuntimePoolUID(poolUID), RuntimePoolGeneration: 1,
			RuntimeSessionUID: "session-1", RuntimeSessionGeneration: 1,
			RuntimeProfileDigest: harnessv2.ProfileDigest("sha256:" + strings.Repeat("b", 64)), ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		TaskUID: harnessv2.TaskUID(taskUID), TaskAttempt: 1, PromptID: "prompt-1", OperationID: "authorize-delta-1",
		RequestDigestSchemaVersion: 1, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	request := acpArtifactAuthorizationRequest{
		Namespace: "default", Metadata: metadata,
		Artifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(artifactID), Digest: digest,
			SizeBytes: int64(len(artifactData)), MediaType: artifactcap.MediaTypeWorkspaceDelta,
		},
	}
	requestDigest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata.RequestDigest = requestDigest
	capability, err := harnessv2.SignOperationCapability(operationSecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		currentObjects func() []client.Object
	}{
		{
			name: "runtime pool replacement",
			currentObjects: func() []client.Object {
				replacementPool := pool.DeepCopy()
				replacementPool.UID = "replacement-pool-uid"
				replacementSecret := authSecret.DeepCopy()
				replacementSecret.UID = artifactAuthorizationReplacementSecretUID
				replacementSecret.Labels[runtimePoolUIDLabelAPI] = string(replacementPool.UID)
				return []client.Object{replacementPool, replacementSecret, task.DeepCopy()}
			},
		},
		{
			name: "runtime pool auth Secret replacement",
			currentObjects: func() []client.Object {
				replacementSecret := authSecret.DeepCopy()
				replacementSecret.UID = artifactAuthorizationReplacementSecretUID
				replacementSecret.Data = map[string][]byte{
					runtimePoolControllerTokenKeyAPI:  []byte(strings.Repeat("r", 32)),
					runtimePoolCapabilitySecretKeyAPI: []byte(strings.Repeat("n", 32)),
				}
				return []client.Object{pool.DeepCopy(), replacementSecret, task.DeepCopy()}
			},
		},
		{
			name: "Task cancellation",
			currentObjects: func() []client.Object {
				cancelledTask := task.DeepCopy()
				cancelledTask.Status.Execution.State = corev1alpha1.TaskExecutionStateCancelled
				return []client.Object{pool.DeepCopy(), authSecret.DeepCopy(), cancelledTask}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.currentObjects()...).Build()
			app := fiber.New()
			server := &Server{app: app, client: cachedClient, config: ServerConfig{APIReader: apiReader}}
			server.installACPArtifactAuthorizationBroker()
			httpRequest := httptest.NewRequest(http.MethodPost, acpArtifactAuthorizationPath, bytes.NewReader(body))
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Authorization", "Bearer "+controllerToken)
			httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
			response, err := app.Test(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusForbidden {
				t.Fatalf("status=%d, want %d", response.StatusCode, http.StatusForbidden)
			}
		})
	}
}

func mustArtifactPath(t *testing.T, digest string) string {
	t.Helper()
	value, err := artifactcap.ObjectPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestPublisherArtifactAuthorizationBrokerBindsTaskAndPublicationState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("publisher-task-uid")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "publisher-task", UID: taskUID},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStatePlanned, PromptID: "prompt-publisher",
		}},
	}
	delta := []byte("publisher delta")
	deltaDigest := artifactcap.DigestBytes(delta)
	deltaID, err := artifactcap.ArtifactIDForDigest(deltaDigest)
	if err != nil {
		t.Fatal(err)
	}
	bundle := []byte("prepared git bundle")
	bundleDigest := artifactcap.DigestBytes(bundle)
	bundleID, err := artifactcap.ArtifactIDForDigest(bundleDigest)
	if err != nil {
		t.Fatal(err)
	}
	publication := &corev1alpha1.Publication{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "publication-control"},
		Spec: corev1alpha1.PublicationSpec{
			ID: "publication-id", Generation: 1, TaskUID: string(taskUID), Attempt: 1, PromptID: "prompt-publisher",
			BranchClaimID: "branch-claim", BranchClaimGeneration: 1, SourceRepositoryID: "github.com/o/r",
			SourceRef: "refs/heads/main", SourceBaselineSHA: strings.Repeat("1", 40), TargetRepositoryID: "github.com/o/r",
			TargetRef: "refs/heads/change", ArtifactID: deltaID, ArtifactDigest: deltaDigest,
			ArtifactSizeBytes: int64(len(delta)), ArtifactMediaType: artifactcap.MediaTypeWorkspaceDelta,
			PublicationCredentialRef: "credential", CommitIdentity: "task", CommitMessage: "change",
			CommitTimestamp: metav1.NewTime(time.Now().UTC()), RequestDigest: "sha256:" + strings.Repeat("2", 64),
		},
		Status: corev1alpha1.PublicationStatus{State: corev1alpha1.PublicationControlState(store.PublicationPreparing)},
	}
	readyPublication := publication.DeepCopy()
	readyPublication.Name = "publication-ready"
	readyPublication.Spec.ID = "publication-ready-id"
	readyPublication.Status = corev1alpha1.PublicationStatus{
		State: corev1alpha1.PublicationControlState(store.PublicationVerifying),
		PreparedReceipt: &corev1alpha1.PreparedPublicationControlReceipt{
			OperationID: "prepare-ready", RequestDigest: "sha256:" + strings.Repeat("3", 64),
			TreeSHA: strings.Repeat("4", 40), CommitSHA: strings.Repeat("5", 40), ManifestDigest: "sha256:" + strings.Repeat("6", 64),
			BundleArtifactID: bundleID, BundleDigest: bundleDigest, BundleSizeBytes: int64(len(bundle)),
			BundleMediaType: artifactcap.MediaTypeGitBundle, BundleRef: "refs/orka/publications/" + strings.Repeat("7", 64),
			PreparedAt: metav1.NewTime(time.Now().UTC()),
		},
	}
	effects := []*corev1alpha1.ExternalEffect{
		publisherEffectForTest("workspace-effect", "workspace.prepare", string(taskUID), "workspace-prepare-prompt-publisher"),
		publisherEffectForTest("prepare-effect", "publisher.prepare", publication.Spec.ID, "prepare-operation"),
		publisherEffectForTest("verify-effect", "publisher.verify", readyPublication.Spec.ID, "verify-operation"),
	}
	objects := []client.Object{task, publication, readyPublication}
	for _, effect := range effects {
		objects = append(objects, effect)
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	artifactSecret := []byte(strings.Repeat("a", 32))
	publisherToken := strings.Repeat("p", 32)
	writeAPISecretFile(t, envACPArtifactSecretFile, "artifact", artifactSecret)
	writeAPISecretFile(t, envWorkspacePublisherControllerTokenFile, "publisher", []byte(publisherToken))

	workspace := []byte("workspace tar")
	workspaceDigest := artifactcap.DigestBytes(workspace)
	workspaceID, err := artifactcap.ArtifactIDForDigest(workspaceDigest)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		request     publisherservice.ArtifactAuthorizationRequest
		apiReader   client.Reader
		epochSource ControllerEpochFenceSource
		wantStatus  int
	}{
		{
			name: "workspace upload",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationWorkspacePrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-prompt-publisher"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(workspaceID), Digest: workspaceDigest, SizeBytes: int64(len(workspace)), MediaType: artifactcap.MediaTypeWorkspaceTar},
				Attempt:           1,
			},
		},
		{
			name: "publication delta download",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationPublicationPrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", PublicationID: publication.Spec.ID, OperationID: "prepare-operation"},
				ArtifactOperation: artifactcap.OperationDownload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(deltaID), Digest: deltaDigest, SizeBytes: int64(len(delta)), MediaType: artifactcap.MediaTypeWorkspaceDelta},
				Attempt:           1,
			},
		},
		{
			name: "prepared bundle upload",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationPublicationPrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", PublicationID: publication.Spec.ID, OperationID: "prepare-operation"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(bundleID), Digest: bundleDigest, SizeBytes: int64(len(bundle)), MediaType: artifactcap.MediaTypeGitBundle},
				Attempt:           1,
			},
		},
		{
			name: "prepared bundle verification download",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationPublicationVerify,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", PublicationID: readyPublication.Spec.ID, OperationID: "verify-operation"},
				ArtifactOperation: artifactcap.OperationDownload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(bundleID), Digest: bundleDigest, SizeBytes: int64(len(bundle)), MediaType: artifactcap.MediaTypeGitBundle},
				Attempt:           1,
			},
		},
		{
			name: "transient controller epoch read failure",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationWorkspacePrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-prompt-publisher"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(workspaceID), Digest: workspaceDigest, SizeBytes: int64(len(workspace)), MediaType: artifactcap.MediaTypeWorkspaceTar},
				Attempt:           1,
			},
			epochSource: fixedControllerEpochFenceSource{err: context.DeadlineExceeded},
			wantStatus:  http.StatusServiceUnavailable,
		},
		{
			name: "transient task state read failure",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationWorkspacePrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-prompt-publisher"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(workspaceID), Digest: workspaceDigest, SizeBytes: int64(len(workspace)), MediaType: artifactcap.MediaTypeWorkspaceTar},
				Attempt:           1,
			},
			apiReader:  failingPublisherAuthorizationReader{err: context.DeadlineExceeded},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "missing task remains denied",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationWorkspacePrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-prompt-publisher"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(workspaceID), Digest: workspaceDigest, SizeBytes: int64(len(workspace)), MediaType: artifactcap.MediaTypeWorkspaceTar},
				Attempt:           1,
			},
			apiReader:  fake.NewClientBuilder().WithScheme(scheme).Build(),
			wantStatus: http.StatusForbidden,
		},
		{
			name: "transient publication state read failure",
			request: publisherservice.ArtifactAuthorizationRequest{
				ParentOperation:   publisherservice.OperationPublicationPrepare,
				Metadata:          publisherservice.OperationMetadata{Namespace: "default", PublicationID: publication.Spec.ID, OperationID: "prepare-operation"},
				ArtifactOperation: artifactcap.OperationUpload,
				Artifact:          harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(bundleID), Digest: bundleDigest, SizeBytes: int64(len(bundle)), MediaType: artifactcap.MediaTypeGitBundle},
				Attempt:           1,
			},
			apiReader:  failingPublisherAuthorizationReader{err: context.DeadlineExceeded},
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			epochSource := test.epochSource
			if epochSource == nil {
				epochSource = publisherEpochSourceForTest()
			}
			wantStatus := test.wantStatus
			if wantStatus == 0 {
				wantStatus = http.StatusOK
			}
			app := fiber.New()
			server := &Server{
				app: app, client: kubeClient,
				config: ServerConfig{
					ArtifactReservations: &recordingCapabilityReservations{},
					APIReader:            test.apiReader,
					ControllerEpochs:     epochSource,
					ExternalEffects:      publisherEffectReaderForTest(effects...),
				},
			}
			server.installACPArtifactAuthorizationBroker()
			body, err := json.Marshal(test.request)
			if err != nil {
				t.Fatal(err)
			}
			httpRequest := httptest.NewRequest(http.MethodPost, publisherservice.ArtifactAuthorizationBrokerPath, bytes.NewReader(body))
			httpRequest.Header.Set("Content-Type", "application/json")
			httpRequest.Header.Set("Authorization", "Bearer "+publisherToken)
			response, err := app.Test(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != wantStatus {
				t.Fatalf("status=%d, want %d", response.StatusCode, wantStatus)
			}
			if wantStatus != http.StatusOK {
				return
			}
			var issued publisherservice.ArtifactAuthorizationResponse
			if err := json.NewDecoder(response.Body).Decode(&issued); err != nil {
				t.Fatal(err)
			}
			binding, err := publisherservice.ArtifactBinding(test.request)
			if err != nil {
				t.Fatal(err)
			}
			path, err := artifactcap.ObjectPath(binding.ObjectDigest)
			if err != nil {
				t.Fatal(err)
			}
			presented := artifactcap.PresentedRequest{
				Method: binding.Method(), Path: path, ObjectDigest: binding.ObjectDigest,
				ContentLength: binding.ContentLength, MediaType: binding.MediaType, RequestDigest: issued.RequestDigest,
			}
			if _, err := artifactcap.Verify(artifactSecret, issued.Capability, presented, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublisherArtifactAuthorizationBrokerToleratesTaskProjectionSkew(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("fresh-publisher-task-uid")
	skewedTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "publisher-task", UID: taskUID},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateSubmitting, PromptID: "fresh-prompt",
		}},
	}
	cachedTask := skewedTask.DeepCopy()
	cachedTask.Status.Execution.State = corev1alpha1.TaskExecutionStatePlanned

	workspace := []byte("fresh workspace tar")
	workspaceDigest := artifactcap.DigestBytes(workspace)
	workspaceID, err := artifactcap.ArtifactIDForDigest(workspaceDigest)
	if err != nil {
		t.Fatal(err)
	}
	request := publisherservice.ArtifactAuthorizationRequest{
		ParentOperation: publisherservice.OperationWorkspacePrepare,
		Metadata: publisherservice.OperationMetadata{
			Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-fresh-prompt",
		},
		ArtifactOperation: artifactcap.OperationUpload,
		Artifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(workspaceID), Digest: workspaceDigest,
			SizeBytes: int64(len(workspace)), MediaType: artifactcap.MediaTypeWorkspaceTar,
		},
		Attempt: 1,
	}
	effect := publisherEffectForTest(
		"fresh-workspace-effect", "workspace.prepare", string(taskUID), request.Metadata.OperationID,
	)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cachedTask, effect.DeepCopy()).Build()
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(skewedTask, effect).Build()

	artifactSecret := []byte(strings.Repeat("a", 32))
	publisherToken := strings.Repeat("p", 32)
	writeAPISecretFile(t, envACPArtifactSecretFile, "artifact", artifactSecret)
	writeAPISecretFile(t, envWorkspacePublisherControllerTokenFile, "publisher", []byte(publisherToken))

	app := fiber.New()
	server := &Server{
		app: app, client: cachedClient,
		config: ServerConfig{
			APIReader: apiReader, ArtifactReservations: &recordingCapabilityReservations{},
			ControllerEpochs: publisherEpochSourceForTest(), ExternalEffects: publisherEffectReaderForTest(effect),
		},
	}
	server.installACPArtifactAuthorizationBroker()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, publisherservice.ArtifactAuthorizationBrokerPath, bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+publisherToken)
	response, err := app.Test(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestTaskStateAllowsWorkspacePreparation(t *testing.T) {
	for _, state := range []corev1alpha1.TaskExecutionState{
		corev1alpha1.TaskExecutionStateQueued,
		corev1alpha1.TaskExecutionStateReserved,
		corev1alpha1.TaskExecutionStateSessionStarting,
		corev1alpha1.TaskExecutionStatePlanned,
	} {
		if !taskStateAllowsWorkspacePreparation(state) {
			t.Fatalf("Task state %s rejected, want workspace preparation authorization", state)
		}
	}
	if taskStateAllowsWorkspacePreparation(corev1alpha1.TaskExecutionStateSubmitting) {
		t.Fatal("Submitting unexpectedly authorized for workspace credential preparation")
	}
}

func TestAuthorizePublisherWorkspaceUploadRejectsPostSubmissionStates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	taskUID := types.UID("publisher-task-post-submission-uid")
	request := publisherservice.ArtifactAuthorizationRequest{
		ParentOperation: publisherservice.OperationWorkspacePrepare,
		Metadata: publisherservice.OperationMetadata{
			Namespace: "default", TaskID: string(taskUID), OperationID: "workspace-prepare-prompt",
		},
		ArtifactOperation: artifactcap.OperationUpload,
	}
	states := []corev1alpha1.TaskExecutionState{
		corev1alpha1.TaskExecutionStateSubmittedUnknown,
		corev1alpha1.TaskExecutionStateAccepted,
		corev1alpha1.TaskExecutionStateRunning,
		corev1alpha1.TaskExecutionStateSettling,
		corev1alpha1.TaskExecutionStateSucceeded,
		corev1alpha1.TaskExecutionStateFailed,
		corev1alpha1.TaskExecutionStateCancelled,
		corev1alpha1.TaskExecutionStateOutcomeUnknown,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "publisher-task", UID: taskUID},
				Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
					State: state, PromptID: "prompt",
				}},
			}
			apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).Build()
			server := &Server{client: apiReader, config: ServerConfig{APIReader: apiReader}}
			if err := server.authorizePublisherArtifactRequest(context.Background(), request); err == nil {
				t.Fatalf("workspace upload authorized in Task state %s", state)
			}
		})
	}
}

func TestPublisherArtifactAuthorizationBrokerUsesFreshPublicationState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	delta := []byte("fresh publication delta")
	deltaDigest := artifactcap.DigestBytes(delta)
	deltaID, err := artifactcap.ArtifactIDForDigest(deltaDigest)
	if err != nil {
		t.Fatal(err)
	}
	freshPublication := &corev1alpha1.Publication{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fresh-publication"},
		Spec: corev1alpha1.PublicationSpec{
			ID: "fresh-publication-id", ArtifactID: deltaID, ArtifactDigest: deltaDigest,
			ArtifactSizeBytes: int64(len(delta)), ArtifactMediaType: artifactcap.MediaTypeWorkspaceDelta,
		},
		Status: corev1alpha1.PublicationStatus{State: corev1alpha1.PublicationControlState(store.PublicationPreparing)},
	}
	stalePublication := freshPublication.DeepCopy()
	stalePublication.Status.State = corev1alpha1.PublicationControlState(store.PublicationPublishing)
	request := publisherservice.ArtifactAuthorizationRequest{
		ParentOperation: publisherservice.OperationPublicationPrepare,
		Metadata: publisherservice.OperationMetadata{
			Namespace: "default", PublicationID: freshPublication.Spec.ID, OperationID: "prepare-fresh-publication",
		},
		ArtifactOperation: artifactcap.OperationDownload,
		Artifact: harnessv2.ArtifactReference{
			ArtifactID: harnessv2.ArtifactID(deltaID), Digest: deltaDigest,
			SizeBytes: int64(len(delta)), MediaType: artifactcap.MediaTypeWorkspaceDelta,
		},
		Attempt: 1,
	}
	effect := publisherEffectForTest(
		"fresh-publication-effect", "publisher.prepare", freshPublication.Spec.ID, request.Metadata.OperationID,
	)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stalePublication).Build()
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(freshPublication, effect).Build()

	artifactSecret := []byte(strings.Repeat("a", 32))
	publisherToken := strings.Repeat("p", 32)
	writeAPISecretFile(t, envACPArtifactSecretFile, "artifact", artifactSecret)
	writeAPISecretFile(t, envWorkspacePublisherControllerTokenFile, "publisher", []byte(publisherToken))

	app := fiber.New()
	server := &Server{
		app: app, client: cachedClient,
		config: ServerConfig{
			APIReader: apiReader, ArtifactReservations: &recordingCapabilityReservations{},
			ControllerEpochs: publisherEpochSourceForTest(), ExternalEffects: publisherEffectReaderForTest(effect),
		},
	}
	server.installACPArtifactAuthorizationBroker()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, publisherservice.ArtifactAuthorizationBrokerPath, bytes.NewReader(body))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+publisherToken)
	response, err := app.Test(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestAuthorizePublisherParentEffectUsesExactIdentityReader(t *testing.T) {
	metadata := publisherservice.OperationMetadata{
		Namespace: "default", TaskID: "task-uid", OperationID: "workspace-prepare-prompt",
	}
	inFlightEffect := publisherEffectForTest(
		"workspace-effect", "workspace.prepare", metadata.TaskID, metadata.OperationID,
	)
	settledEffect := inFlightEffect.DeepCopy()
	settledEffect.Status.State = corev1alpha1.ExternalEffectControlState(store.ExternalEffectSucceeded)

	tests := []struct {
		name    string
		effect  *corev1alpha1.ExternalEffect
		wantErr bool
	}{
		{
			name:   "exact reader observes in-flight effect",
			effect: inFlightEffect.DeepCopy(),
		},
		{
			name:    "exact reader rejects settled effect",
			effect:  settledEffect,
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{config: ServerConfig{
				ControllerEpochs: publisherEpochSourceForTest(), ExternalEffects: publisherEffectReaderForTest(test.effect),
			}}

			err := server.authorizePublisherParentEffect(
				context.Background(), publisherservice.OperationWorkspacePrepare, metadata,
			)
			if test.wantErr && err == nil {
				t.Fatal("expected authorization to fail")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("expected authorization to succeed: %v", err)
			}
		})
	}
}

func TestAuthorizePublisherParentEffectRequiresLiveLease(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	metadata := publisherservice.OperationMetadata{
		Namespace: "default", TaskID: "task-uid", OperationID: "workspace-prepare-prompt",
	}
	base := func() *corev1alpha1.ExternalEffect {
		return publisherEffectForTest("workspace-effect", "workspace.prepare", metadata.TaskID, metadata.OperationID)
	}
	tests := []struct {
		name            string
		mutate          func(*corev1alpha1.ExternalEffect)
		epochSource     ControllerEpochFenceSource
		wantErr         bool
		wantUnavailable bool
	}{
		{name: "live lease authorizes", mutate: func(*corev1alpha1.ExternalEffect) {}, epochSource: publisherEpochSourceForTest()},
		{
			name: "expired lease is rejected",
			mutate: func(e *corev1alpha1.ExternalEffect) {
				e.Status.LeaseExpiresAt = &metav1.Time{Time: time.Now().UTC().Add(-time.Minute)}
			},
			epochSource: publisherEpochSourceForTest(),
			wantErr:     true,
		},
		{
			name: "missing lease expiry is rejected", mutate: func(e *corev1alpha1.ExternalEffect) { e.Status.LeaseExpiresAt = nil },
			epochSource: publisherEpochSourceForTest(), wantErr: true,
		},
		{
			name: "missing lease owner is rejected", mutate: func(e *corev1alpha1.ExternalEffect) { e.Status.LeaseOwner = "" },
			epochSource: publisherEpochSourceForTest(), wantErr: true,
		},
		{
			name: "missing controller epoch is rejected", mutate: func(e *corev1alpha1.ExternalEffect) { e.Status.ControllerEpoch = 0 },
			epochSource: publisherEpochSourceForTest(), wantErr: true,
		},
		{
			name: "stale controller epoch is rejected", mutate: func(*corev1alpha1.ExternalEffect) {},
			epochSource: fixedControllerEpochFenceSource{fence: store.ControllerEpochFence{
				Name: store.DefaultControllerEpochName, Epoch: 2, HolderID: "controller-2",
			}},
			wantErr: true,
		},
		{
			name:        "missing controller epoch name is rejected",
			mutate:      func(e *corev1alpha1.ExternalEffect) { e.Status.ControllerEpochName = "" },
			epochSource: publisherEpochSourceForTest(), wantErr: true,
		},
		{
			name: "unavailable controller authority is rejected", mutate: func(*corev1alpha1.ExternalEffect) {},
			epochSource: fixedControllerEpochFenceSource{err: context.Canceled}, wantErr: true, wantUnavailable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effect := base()
			test.mutate(effect)
			server := &Server{
				config: ServerConfig{
					ControllerEpochs: test.epochSource, ExternalEffects: publisherEffectReaderForTest(effect),
				},
			}
			err := server.authorizePublisherParentEffect(
				context.Background(), publisherservice.OperationWorkspacePrepare, metadata,
			)
			if test.wantErr && err == nil {
				t.Fatal("expected authorization to fail for a non-live lease")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("expected authorization to succeed: %v", err)
			}
			if got := errors.Is(err, errPublisherArtifactAuthorizationUnavailable); got != test.wantUnavailable {
				t.Fatalf("unavailable classification = %t, want %t (error %v)", got, test.wantUnavailable, err)
			}
		})
	}
}

func TestControllerEpochStoreFenceSourceReadsDurableAuthority(t *testing.T) {
	epochStore := &fixedControllerEpochStore{fence: store.ControllerEpochFence{
		Name: store.DefaultControllerEpochName, Epoch: 7, HolderID: "controller-7",
	}}
	source := NewControllerEpochStoreFenceSource(epochStore)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fence, err := source.CurrentFence(ctx)
	if err != nil {
		t.Fatalf("CurrentFence() error = %v", err)
	}
	if epochStore.requestedName != store.DefaultControllerEpochName {
		t.Fatalf("requested epoch name = %q", epochStore.requestedName)
	}
	if fence.Name != store.DefaultControllerEpochName || fence.Epoch != 7 || fence.HolderID != "controller-7" {
		t.Fatalf("CurrentFence() = %#v", fence)
	}
}

func publisherEffectForTest(name, kind, aggregateID, operationID string) *corev1alpha1.ExternalEffect {
	return &corev1alpha1.ExternalEffect{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec: corev1alpha1.ExternalEffectSpec{
			ID: name, Kind: kind, IdentityNamespace: "default", AggregateID: aggregateID,
			OperationID: operationID, RequestDigest: "sha256:" + strings.Repeat("9", 64),
		},
		Status: corev1alpha1.ExternalEffectStatus{
			State:                       corev1alpha1.ExternalEffectControlState(store.ExternalEffectInFlight),
			LeaseOwner:                  "controller-epoch-1",
			LeaseExpiresAt:              &metav1.Time{Time: time.Now().UTC().Add(2 * time.Minute)},
			ControlRecordMutationStatus: corev1alpha1.ControlRecordMutationStatus{ControllerEpochName: store.DefaultControllerEpochName, ControllerEpoch: 1},
		},
	}
}

type externalEffectIdentityReaderFunc func(context.Context, store.ExternalEffectIdentity) (*store.ExternalEffect, error)

func (f externalEffectIdentityReaderFunc) GetExternalEffectByIdentity(
	ctx context.Context,
	identity store.ExternalEffectIdentity,
) (*store.ExternalEffect, error) {
	return f(ctx, identity)
}

func publisherEffectReaderForTest(effects ...*corev1alpha1.ExternalEffect) store.ExternalEffectIdentityReader {
	return externalEffectIdentityReaderFunc(func(_ context.Context, identity store.ExternalEffectIdentity) (*store.ExternalEffect, error) {
		for _, effect := range effects {
			if effect == nil || effect.Spec.Kind != identity.Kind || effect.Spec.IdentityNamespace != identity.Namespace ||
				effect.Spec.AggregateID != identity.AggregateID || effect.Spec.OperationID != identity.OperationID {
				continue
			}
			var leaseExpiresAt *time.Time
			if effect.Status.LeaseExpiresAt != nil {
				value := effect.Status.LeaseExpiresAt.UTC()
				leaseExpiresAt = &value
			}
			return &store.ExternalEffect{
				ID: effect.Spec.ID,
				Identity: store.ExternalEffectIdentity{
					Kind: effect.Spec.Kind, Namespace: effect.Spec.IdentityNamespace,
					AggregateID: effect.Spec.AggregateID, OperationID: effect.Spec.OperationID,
				},
				State: store.ExternalEffectState(effect.Status.State), LeaseOwner: effect.Status.LeaseOwner,
				LeaseExpiresAt: leaseExpiresAt, ControllerEpochName: effect.Status.ControllerEpochName,
				ControllerEpoch: effect.Status.ControllerEpoch,
			}, nil
		}
		return nil, store.ErrNotFound
	})
}

type fixedControllerEpochFenceSource struct {
	fence store.ControllerEpochFence
	err   error
}

type fixedAgentExecutionSnapshotStore struct {
	snapshot *store.AgentExecutionSnapshot
	err      error
}

func (s *fixedAgentExecutionSnapshotStore) PersistAgentExecutionSnapshot(context.Context, store.AgentExecutionSnapshot) error {
	return errors.New("unexpected snapshot persistence")
}

func (s *fixedAgentExecutionSnapshotStore) GetAgentExecutionSnapshot(
	_ context.Context,
	key store.AgentExecutionSnapshotKey,
) (*store.AgentExecutionSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.snapshot == nil || s.snapshot.TaskUID != key.TaskUID || s.snapshot.Digest != key.Digest {
		return nil, store.ErrNotFound
	}
	copy := *s.snapshot
	copy.Body = append([]byte(nil), s.snapshot.Body...)
	return &copy, nil
}

func (s *fixedAgentExecutionSnapshotStore) ListAgentExecutionSnapshotKeys(
	context.Context,
	string,
) ([]store.AgentExecutionSnapshotKey, error) {
	return nil, errors.New("unexpected snapshot listing")
}

func (s *fixedAgentExecutionSnapshotStore) DeleteAgentExecutionSnapshots(context.Context, string) error {
	return errors.New("unexpected snapshot deletion")
}

type failingPublisherAuthorizationReader struct {
	err error
}

func (r failingPublisherAuthorizationReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return r.err
}

func (r failingPublisherAuthorizationReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return r.err
}

type fixedControllerEpochStore struct {
	fence         store.ControllerEpochFence
	err           error
	requestedName string
}

func (s *fixedControllerEpochStore) GetControllerEpochFence(_ context.Context, name string) (store.ControllerEpochFence, error) {
	s.requestedName = name
	return s.fence, s.err
}

func (s fixedControllerEpochFenceSource) CurrentFence(context.Context) (store.ControllerEpochFence, error) {
	return s.fence, s.err
}

func publisherEpochSourceForTest() ControllerEpochFenceSource {
	return fixedControllerEpochFenceSource{fence: store.ControllerEpochFence{
		Name: store.DefaultControllerEpochName, Epoch: 1, HolderID: "controller-1",
	}}
}

func writeAPISecretFile(t *testing.T, env, name string, value []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, value, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(env, path)
}
