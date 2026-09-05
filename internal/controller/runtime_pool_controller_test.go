/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	orkametrics "github.com/orka-agents/orka/internal/metrics"
	storekube "github.com/orka-agents/orka/internal/store/kube"
)

var (
	runtimePoolTestNow               = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	runtimePoolTestProviderToken     = []byte("0123456789abcdef0123456789abcdef")
	runtimePoolTestProviderTokenNext = []byte("fedcba9876543210fedcba9876543210")
)

const (
	runtimePoolTestNextModel     = "gpt-next"
	runtimePoolTestTamperedImage = "attacker.invalid/runtime:latest"
)

type fakeRuntimePoolSupervisorClient struct {
	probe       RuntimePoolProbeResult
	probeErr    error
	probeCalls  int
	afterProbe  func()
	drainCalls  int
	drainReason string
	drainErr    error
}

type runtimePoolPodDeleteRecordingClient struct {
	client.Client
	podDeleteHadPreconditions bool
	podDeleteUID              types.UID
	podDeleteResourceVersion  string
}

type runtimePoolNamespaceReadClient struct {
	client.Client
	namespaceReads       int
	rejectNamespaceReads bool
}

func TestRuntimePoolWorkspaceDeletionDrainCompleteRequiresCurrentGeneration(t *testing.T) {
	pool := &corev1alpha1.RuntimePool{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Status: corev1alpha1.RuntimePoolStatus{
			ObservedGeneration: 1,
			DesiredReplicas:    0,
			CurrentReplicas:    0,
			Lifecycle:          corev1alpha1.RuntimePoolLifecycleStopped,
		},
	}
	if runtimePoolWorkspaceDeletionDrainComplete(pool) {
		t.Fatal("stale Stopped status completed workspace deletion drain")
	}

	pool.Status.ObservedGeneration = pool.Generation
	if !runtimePoolWorkspaceDeletionDrainComplete(pool) {
		t.Fatal("current Stopped status did not complete workspace deletion drain")
	}
}

func (c *runtimePoolNamespaceReadClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, ok := object.(*corev1.Namespace); ok {
		c.namespaceReads++
		if c.rejectNamespaceReads {
			return errors.New("cached Namespace reads are forbidden")
		}
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (c *runtimePoolPodDeleteRecordingClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if _, ok := object.(*corev1.Pod); ok {
		deleteOptions := (&client.DeleteOptions{}).ApplyOptions(options)
		if deleteOptions.Preconditions != nil {
			c.podDeleteHadPreconditions = true
			if deleteOptions.Preconditions.UID != nil {
				c.podDeleteUID = *deleteOptions.Preconditions.UID
			}
			if deleteOptions.Preconditions.ResourceVersion != nil {
				c.podDeleteResourceVersion = *deleteOptions.Preconditions.ResourceVersion
			}
		}
	}
	return c.Client.Delete(ctx, object, options...)
}

func (f *fakeRuntimePoolSupervisorClient) Probe(_ context.Context, _, _ string, _ []byte) (RuntimePoolProbeResult, error) {
	f.probeCalls++
	if f.afterProbe != nil {
		f.afterProbe()
	}
	return f.probe, f.probeErr
}

func (f *fakeRuntimePoolSupervisorClient) RequestDrain(
	_ context.Context,
	_, _ string,
	_ []byte,
	_ harnessv2.StatusResponse,
	reason string,
) error {
	f.drainCalls++
	f.drainReason = reason
	return f.drainErr
}

func TestRuntimePoolReconcilerScalesZeroToOneWithHardenedResources(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	r.EnablePDB = true
	r.ControllerAPIPort = 18080

	runtimePoolReconcile(t, r, pool)

	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: base}, &deployment); err != nil {
		t.Fatalf("Get Deployment: %v", err)
	}
	assertRuntimePoolDeploymentHardening(t, r, pool, &deployment)
	assertRuntimePoolDiscoveryAndNetwork(t, r, pool, base)

	gotPool := runtimePoolTestGetPool(t, r, pool)
	if gotPool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStarting || gotPool.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status = %s/%s, want Starting/Closed", gotPool.Status.Lifecycle, gotPool.Status.AdmissionState)
	}
}

func TestRuntimePoolNamespaceRejectsArbitraryOverride(t *testing.T) {
	reconciler := &RuntimePoolReconciler{RuntimeNamespace: acpTestRuntimeNamespace}
	pool := &corev1alpha1.RuntimePool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "pool"}}

	for _, allowed := range []string{"", acpTestRuntimeNamespace, "team-a"} {
		pool.Spec.RuntimeNamespace = allowed
		if _, err := reconciler.runtimePoolNamespace(pool); err != nil {
			t.Fatalf("runtimeNamespace %q rejected: %v", allowed, err)
		}
	}
	pool.Spec.RuntimeNamespace = "victim-namespace"
	if _, err := reconciler.runtimePoolNamespace(pool); err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatalf("arbitrary runtimeNamespace error = %v, want not-permitted rejection", err)
	}

	// With no configured runtime namespace, only the pool's own namespace is allowed.
	unconfigured := &RuntimePoolReconciler{}
	pool.Spec.RuntimeNamespace = acpTestRuntimeNamespace
	if _, err := unconfigured.runtimePoolNamespace(pool); err == nil {
		t.Fatal("unconfigured controller accepted a non-pool runtime namespace")
	}
}

func TestRuntimePoolReconcilerReadsRuntimeNamespaceUncached(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	runtimeNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: acpTestRuntimeNamespace}}
	cachedClient := &runtimePoolNamespaceReadClient{
		Client:               fake.NewClientBuilder().WithScheme(scheme).Build(),
		rejectNamespaceReads: true,
	}
	apiReader := &runtimePoolNamespaceReadClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeNamespace).Build(),
	}
	reconciler := &RuntimePoolReconciler{Client: cachedClient, APIReader: apiReader}

	if err := reconciler.ensureRuntimePoolNamespace(context.Background(), runtimePoolConfig{namespace: runtimeNamespace.Name}); err != nil {
		t.Fatalf("ensureRuntimePoolNamespace() error = %v", err)
	}
	if cachedClient.namespaceReads != 0 {
		t.Fatalf("cached Namespace reads = %d, want 0", cachedClient.namespaceReads)
	}
	if apiReader.namespaceReads != 1 {
		t.Fatalf("uncached Namespace reads = %d, want 1", apiReader.namespaceReads)
	}
}

func assertRuntimePoolDeploymentHardening(t *testing.T, r *RuntimePoolReconciler, pool *corev1alpha1.RuntimePool, deployment *appsv1.Deployment) {
	t.Helper()
	assertRuntimePoolDeploymentShape(t, pool, deployment)

	podSpec := deployment.Spec.Template.Spec
	assertRuntimePoolPodHardening(t, podSpec)

	container := podSpec.Containers[0]
	assertRuntimePoolContainerHardening(t, container)
	assertRuntimePoolBoundedVolumes(t, podSpec.Volumes)

	if deployment.Spec.Template.Annotations[runtimePoolPIDsAnnotation] != "4096" {
		t.Fatalf("PID limit annotation = %q, want 4096", deployment.Spec.Template.Annotations[runtimePoolPIDsAnnotation])
	}
	assertRuntimePoolEnvironment(t, r, pool, container.Env)
}

func assertRuntimePoolDeploymentShape(t *testing.T, pool *corev1alpha1.RuntimePool, deployment *appsv1.Deployment) {
	t.Helper()
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 1 {
		t.Fatalf("Deployment replicas = %d, want 1", got)
	}
	if deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType || deployment.Spec.Strategy.RollingUpdate != nil {
		t.Fatalf("Deployment strategy = %#v, want Recreate without rolling update", deployment.Spec.Strategy)
	}
	if !metav1.IsControlledBy(deployment, pool) {
		t.Fatal("same-namespace Deployment is not controller-owned by RuntimePool")
	}
}

func assertRuntimePoolPodHardening(t *testing.T, podSpec corev1.PodSpec) {
	t.Helper()
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.FSGroup != nil {
		t.Fatalf("runtime Pod fsGroup exposes root-owned credential volumes to child GIDs: %d", *podSpec.SecurityContext.FSGroup)
	}
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Fatal("runtime Pod must disable service-account token automount")
	}
	if podSpec.EnableServiceLinks == nil || *podSpec.EnableServiceLinks {
		t.Fatal("runtime Pod must disable service links")
	}
	if len(podSpec.Containers) != 1 {
		t.Fatalf("container count = %d, want 1", len(podSpec.Containers))
	}
}

func assertRuntimePoolContainerHardening(t *testing.T, container corev1.Container) {
	t.Helper()
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("runtime supervisor root filesystem is not read-only")
	}
	if container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("runtime supervisor allows privilege escalation")
	}
	if container.SecurityContext.Capabilities == nil || strings.Join(capabilityStrings(container.SecurityContext.Capabilities.Drop), ",") != "ALL" {
		t.Fatalf("dropped capabilities = %#v, want ALL", container.SecurityContext.Capabilities)
	}
	if got := strings.Join(capabilityStrings(container.SecurityContext.Capabilities.Add), ","); got != "CHOWN,KILL,SETGID,SETUID" {
		t.Fatalf("added capabilities = %q, want documented supervisor set", got)
	}
}

func assertRuntimePoolBoundedVolumes(t *testing.T, volumes []corev1.Volume) {
	t.Helper()
	for _, volumeName := range []string{runtimePoolSessionsVolume, runtimePoolTempVolume, runtimePoolHomeVolume} {
		volume := runtimePoolTestVolume(volumes, volumeName)
		if volume == nil || volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.IsZero() {
			t.Fatalf("volume %q is not a bounded emptyDir", volumeName)
		}
	}
}

func assertRuntimePoolEnvironment(t *testing.T, r *RuntimePoolReconciler, pool *corev1alpha1.RuntimePool, envVars []corev1.EnvVar) {
	t.Helper()
	environment := make(map[string]string, len(envVars))
	for _, env := range envVars {
		if strings.Contains(env.Value, "token-") || strings.Contains(env.Value, "secret-") {
			t.Fatalf("secret value was embedded in environment variable %s", env.Name)
		}
		environment[env.Name] = env.Value
	}
	modelContextLimit := ""
	modelOutputLimit := ""
	if pool.Spec.Runtime.Profile.ModelLimits != nil {
		modelContextLimit = strconv.FormatInt(pool.Spec.Runtime.Profile.ModelLimits.Context, 10)
		modelOutputLimit = strconv.FormatInt(pool.Spec.Runtime.Profile.ModelLimits.Output, 10)
	}
	for name, want := range map[string]string{
		"ORKA_ACP_PROVIDER":                     pool.Spec.Runtime.Profile.ProviderKind,
		"ORKA_ACP_MODEL":                        pool.Spec.Runtime.Profile.Model,
		"ORKA_ACP_MODEL_CONTEXT_LIMIT":          modelContextLimit,
		"ORKA_ACP_MODEL_OUTPUT_LIMIT":           modelOutputLimit,
		"ORKA_ACP_WORKSPACE_INTENT":             string(pool.Spec.Runtime.Profile.WorkspaceIntent),
		"ORKA_ACP_AGENT_CONFIGURATION_DIGEST":   pool.Spec.Runtime.Profile.AgentConfigurationDigest,
		"ORKA_ACP_TOOL_POLICY_DIGEST":           pool.Spec.Runtime.Profile.ToolPolicyDigest,
		"ORKA_ACP_APPROVAL_POLICY_DIGEST":       pool.Spec.Runtime.Profile.ApprovalPolicyDigest,
		"ORKA_ACP_MCP_CONFIGURATION_DIGEST":     pool.Spec.Runtime.Profile.MCPConfigurationDigest,
		"ORKA_ACP_PROXY_CREDENTIAL_ROLE":        pool.Spec.Runtime.Profile.ProxyCredentialRole,
		"ORKA_ACP_PROXY_CREDENTIAL_SCOPE":       pool.Spec.Runtime.Profile.ProxyCredentialScope,
		"ORKA_ACP_PROVIDER_PROXY_BASE_URL":      r.ProviderProxy.BaseURL,
		"ORKA_ACP_PROVIDER_TOKEN_FILE":          runtimePoolProviderTokenPath,
		"ORKA_ACP_WORKSPACE_MAX_ARTIFACT_BYTES": strconv.FormatInt(r.WorkspaceArtifactMaxBytes, 10),
	} {
		if environment[name] != want {
			t.Fatalf("environment %s = %q, want %q", name, environment[name], want)
		}
	}
}

//nolint:gocyclo // The explicit state-machine branches are easier to audit together.
func assertRuntimePoolDiscoveryAndNetwork(t *testing.T, r *RuntimePoolReconciler, pool *corev1alpha1.RuntimePool, base string) {
	t.Helper()
	var service corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: base}, &service); err != nil {
		t.Fatalf("Get Service: %v", err)
	}
	if service.Spec.ClusterIP != corev1.ClusterIPNone || !reflectStringMapEqual(service.Spec.Selector, map[string]string{runtimePoolKeyLabel: runtimePoolKey(pool.Namespace, pool.Name)}) {
		t.Fatalf("Service is not headless exact-pool discovery: %#v", service.Spec)
	}
	var authSecrets corev1.SecretList
	if err := r.List(context.Background(), &authSecrets, client.InNamespace(pool.Namespace), client.MatchingLabels{
		runtimePoolUIDLabel: string(pool.UID), runtimePoolAuthLabel: "true",
	}); err != nil {
		t.Fatalf("List pool auth Secrets: %v", err)
	}
	if len(authSecrets.Items) != 1 {
		t.Fatalf("pool auth Secret count = %d, want 1", len(authSecrets.Items))
	}
	authData := authSecrets.Items[0].Data
	if len(authData[runtimePoolControllerTokenKey]) == 0 || len(authData[runtimePoolCapabilitySecretKey]) == 0 {
		t.Fatalf("pool auth Secret keys = %v, want %q and %q", secretDataKeys(authData), runtimePoolControllerTokenKey, runtimePoolCapabilitySecretKey)
	}
	if _, legacy := authData["token"]; legacy {
		t.Fatal("pool auth Secret contains legacy token key")
	}
	if _, legacy := authData["secret"]; legacy {
		t.Fatal("pool auth Secret contains legacy secret key")
	}
	var poolSecrets corev1.SecretList
	if err := r.List(context.Background(), &poolSecrets, client.InNamespace(pool.Namespace), client.MatchingLabels{
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		t.Fatalf("List pool Secrets: %v", err)
	}
	providerSecretCount := 0
	for i := range poolSecrets.Items {
		providerToken, exists := poolSecrets.Items[i].Data[runtimePoolProviderTokenKey]
		if !exists {
			continue
		}
		providerSecretCount++
		if !bytes.Equal(providerToken, runtimePoolTestProviderToken) {
			t.Fatal("RuntimePool provider Secret did not copy the configured authenticated proxy token exactly")
		}
	}
	if providerSecretCount != 1 {
		t.Fatalf("provider Secret count = %d, want 1", providerSecretCount)
	}

	var policies networkingv1.NetworkPolicyList
	if err := r.List(context.Background(), &policies, client.InNamespace(pool.Namespace), client.MatchingLabels{runtimePoolKeyLabel: runtimePoolKey(pool.Namespace, pool.Name)}); err != nil {
		t.Fatalf("List NetworkPolicies: %v", err)
	}
	if len(policies.Items) != 5 {
		t.Fatalf("NetworkPolicy count = %d, want 5", len(policies.Items))
	}
	for i := range policies.Items {
		if strings.Contains(policies.Items[i].Name, "scm") {
			t.Fatalf("runtime pool unexpectedly has SCM egress policy %q", policies.Items[i].Name)
		}
	}
	var providerPolicy networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{
		Namespace: pool.Namespace, Name: runtimePoolChildName(base, "provider-proxy-egress"),
	}, &providerPolicy); err != nil {
		t.Fatalf("Get provider proxy egress policy: %v", err)
	}
	if len(providerPolicy.Spec.Egress) != 1 || len(providerPolicy.Spec.Egress[0].To) != 1 || len(providerPolicy.Spec.Egress[0].Ports) != 1 {
		t.Fatalf("provider proxy egress policy = %#v", providerPolicy.Spec.Egress)
	}
	peer := providerPolicy.Spec.Egress[0].To[0]
	if peer.NamespaceSelector == nil || peer.NamespaceSelector.MatchLabels[corev1.LabelMetadataName] != r.ProviderProxy.Namespace {
		t.Fatalf("provider proxy namespace selector = %#v", peer.NamespaceSelector)
	}
	if peer.PodSelector == nil || !reflectStringMapEqual(peer.PodSelector.MatchLabels, r.ProviderProxy.PodLabels) {
		t.Fatalf("provider proxy Pod selector = %#v, want %#v", peer.PodSelector, r.ProviderProxy.PodLabels)
	}
	if port := providerPolicy.Spec.Egress[0].Ports[0].Port; port == nil || port.IntVal != 8080 {
		t.Fatalf("provider proxy egress port = %#v, want 8080", port)
	}
	var controlPolicy networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{
		Namespace: pool.Namespace, Name: runtimePoolChildName(base, "control-in"),
	}, &controlPolicy); err != nil {
		t.Fatalf("Get control ingress policy: %v", err)
	}
	if len(controlPolicy.Spec.Ingress) != 1 || len(controlPolicy.Spec.Ingress[0].From) != 1 {
		t.Fatalf("control ingress policy = %#v", controlPolicy.Spec.Ingress)
	}
	controlPeer := controlPolicy.Spec.Ingress[0].From[0]
	if controlPeer.PodSelector == nil || controlPeer.PodSelector.MatchLabels[runtimePoolNetworkRoleLabel] != "controller" {
		t.Fatalf("control ingress Pod selector = %#v, want stable controller network role", controlPeer.PodSelector)
	}
	var controllerPolicy networkingv1.NetworkPolicy
	if err := r.Get(context.Background(), types.NamespacedName{
		Namespace: pool.Namespace, Name: runtimePoolChildName(base, "controller-egress"),
	}, &controllerPolicy); err != nil {
		t.Fatalf("Get controller egress policy: %v", err)
	}
	if len(controllerPolicy.Spec.Egress) != 1 || len(controllerPolicy.Spec.Egress[0].Ports) != 1 {
		t.Fatalf("controller egress policy = %#v", controllerPolicy.Spec.Egress)
	}
	if len(controllerPolicy.Spec.Egress[0].To) != 1 || controllerPolicy.Spec.Egress[0].To[0].PodSelector == nil ||
		controllerPolicy.Spec.Egress[0].To[0].PodSelector.MatchLabels[runtimePoolNetworkRoleLabel] != "controller" {
		t.Fatalf("controller egress Pod selector = %#v, want stable controller network role", controllerPolicy.Spec.Egress[0].To)
	}
	if port := controllerPolicy.Spec.Egress[0].Ports[0].Port; port == nil || port.IntVal != r.ControllerAPIPort {
		t.Fatalf("controller egress port = %#v, want %d", port, r.ControllerAPIPort)
	}
	var pdb policyv1.PodDisruptionBudget
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolChildName(base, "pdb")}, &pdb); err != nil {
		t.Fatalf("Get PDB: %v", err)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 0 {
		t.Fatalf("PDB maxUnavailable = %#v, want 0", pdb.Spec.MaxUnavailable)
	}
}

func TestRuntimePoolReconcilerRotatesProviderTokenOnlyAfterAuthenticatedDrain(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	tokenPath := runtimePoolTestTokenFile(t, runtimePoolTestProviderToken)
	r.ProviderProxy.BearerToken = nil
	r.ProviderProxy.BearerTokenFile = tokenPath

	oldDeployment, oldPod := runtimePoolTestStartServing(t, r, pool, supervisor, "old-pod", "old-pod-uid", "10.0.0.61", "old-boot")
	oldGeneration := runtimePoolProviderTokenGeneration(runtimePoolTestProviderToken)
	oldRevision := oldDeployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]
	if active := runtimePoolTestGetPool(t, r, pool).Status.ActiveInstance; active == nil || active.ProviderTokenGeneration != oldGeneration {
		t.Fatalf("old active provider generation = %#v, want %q", active, oldGeneration)
	}

	runtimePoolTestWriteTokenFile(t, tokenPath, runtimePoolTestProviderTokenNext)
	runtimePoolReconcile(t, r, pool)
	newGeneration := runtimePoolProviderTokenGeneration(runtimePoolTestProviderTokenNext)
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	if got := deployment.Spec.Template.Annotations[runtimePoolProviderTokenGenerationAnnotation]; got != oldGeneration {
		t.Fatalf("live Deployment provider generation changed before drain = %q, want %q", got, oldGeneration)
	}
	if got := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]; got != oldRevision {
		t.Fatalf("live Deployment revision changed before drain = %q, want %q", got, oldRevision)
	}
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 1 {
		t.Fatalf("live Deployment replicas during drain = %d, want 1", got)
	}
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls after token rotation = %d, want 1", supervisor.drainCalls)
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining {
		t.Fatalf("rotation status = %s/%s, want Draining/Draining", status.Lifecycle, status.AdmissionState)
	}
	if status.ActiveInstance == nil || status.ActiveInstance.ProviderTokenGeneration != oldGeneration {
		t.Fatalf("active provider generation during drain = %#v, want old generation %q", status.ActiveInstance, oldGeneration)
	}
	assertRuntimePoolProviderSecret(t, r, pool, newGeneration, runtimePoolTestProviderTokenNext)

	supervisor.probe = runtimePoolValidProbe(pool, &oldPod, "old-boot", true)
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("lifecycle after authenticated quiescence = %s, want Quiescent", got)
	}
	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 0 {
		t.Fatalf("replicas after persisted quiescence = %d, want 0", got)
	}
	if got := deployment.Spec.Template.Annotations[runtimePoolProviderTokenGenerationAnnotation]; got != oldGeneration {
		t.Fatalf("template changed before old Pod termination = %q, want %q", got, oldGeneration)
	}
	if err := r.Delete(context.Background(), &oldPod); err != nil {
		t.Fatalf("delete old Pod: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	if got := deployment.Spec.Template.Annotations[runtimePoolProviderTokenGenerationAnnotation]; got != newGeneration {
		t.Fatalf("replacement Deployment provider generation = %q, want %q", got, newGeneration)
	}
	if got := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]; got == oldRevision {
		t.Fatalf("replacement Deployment retained old revision %q", got)
	}
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 1 {
		t.Fatalf("replacement Deployment replicas = %d, want 1", got)
	}

	newPod := runtimePoolReadyPodForDeployment(pool, deployment, "new-pod", "new-pod-uid", "10.0.0.62")
	runtimePoolTestCreatePod(t, r, &newPod)
	supervisor.probe = runtimePoolValidProbe(pool, &newPod, "new-boot", false)
	runtimePoolReconcile(t, r, pool)
	status = runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("converged status = %s/%s, want Serving/Accepting", status.Lifecycle, status.AdmissionState)
	}
	if status.ActiveInstance == nil || status.ActiveInstance.PodUID != string(newPod.UID) || status.ActiveInstance.ProviderTokenGeneration != newGeneration {
		t.Fatalf("converged active instance = %#v, want new Pod and generation %q", status.ActiveInstance, newGeneration)
	}
}

func TestRuntimePoolReconcilerPrunesStaleEpochSecretsAfterAuthenticatedRollout(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	oldDeployment, oldPod := runtimePoolTestStartServing(t, r, pool, supervisor, "epoch-old-pod", "epoch-old-uid", "10.0.0.63", "epoch-old-boot")
	oldAuthName := runtimePoolTestVolume(oldDeployment.Spec.Template.Spec.Volumes, "pool-auth").Secret.SecretName
	oldProviderName := runtimePoolTestVolume(oldDeployment.Spec.Template.Spec.Volumes, runtimePoolProviderCapabilityVolume).Secret.SecretName
	oldAuth := &corev1.Secret{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: oldAuthName}, oldAuth); err != nil {
		t.Fatalf("get old epoch auth Secret: %v", err)
	}
	legacyAuth := oldAuth.DeepCopy()
	legacyAuth.ObjectMeta = metav1.ObjectMeta{
		Name:      runtimePoolAuthSuffixPattern.ReplaceAllString(oldAuthName, "auth-e6"),
		Namespace: pool.Namespace,
		Labels:    cloneStringMap(oldAuth.Labels),
	}
	delete(legacyAuth.Data, runtimePoolBootstrapNonceKey)
	if err := r.Create(context.Background(), legacyAuth); err != nil {
		t.Fatalf("create stale legacy two-key auth Secret: %v", err)
	}
	unrelated := oldAuth.DeepCopy()
	unrelated.ObjectMeta = metav1.ObjectMeta{Name: "user-managed-auth", Namespace: pool.Namespace, Labels: cloneStringMap(oldAuth.Labels)}
	if err := r.Create(context.Background(), unrelated); err != nil {
		t.Fatalf("create unrelated labeled Secret: %v", err)
	}
	replicas := int32(1)
	oldReplicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "epoch-old-rs", Namespace: pool.Namespace, Generation: 1, Labels: cloneStringMap(oldDeployment.Spec.Template.Labels),
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas, Selector: oldDeployment.Spec.Selector.DeepCopy(), Template: *oldDeployment.Spec.Template.DeepCopy(),
		},
		Status: appsv1.ReplicaSetStatus{ObservedGeneration: 1, Replicas: 1, FullyLabeledReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	for i := range oldReplicaSet.Spec.Template.Spec.Volumes {
		volume := &oldReplicaSet.Spec.Template.Spec.Volumes[i]
		switch volume.Name {
		case "pool-auth":
			volume.Secret = nil
			volume.Projected = &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{
				Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: oldAuthName}},
			}}}
		case runtimePoolProviderCapabilityVolume:
			volume.Secret = nil
		}
	}
	oldReplicaSet.Spec.Template.Spec.Containers[0].EnvFrom = append(
		oldReplicaSet.Spec.Template.Spec.Containers[0].EnvFrom,
		corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: oldProviderName}}},
	)
	oldReplicaSet.Spec.Template.Spec.Volumes = append(oldReplicaSet.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "csi-provider", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
			Driver: "test.csi.orka.ai", NodePublishSecretRef: &corev1.LocalObjectReference{Name: oldProviderName},
		}},
	})
	if err := r.Create(context.Background(), oldReplicaSet); err != nil {
		t.Fatalf("create old epoch ReplicaSet: %v", err)
	}

	r.ControllerEpoch = 8
	runtimePoolReconcile(t, r, pool)
	runtimePoolTestAssertSecretsExist(t, r, pool.Namespace, "before authenticated drain", oldAuthName, oldProviderName, unrelated.Name)
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls after controller epoch rotation = %d, want 1", supervisor.drainCalls)
	}

	supervisor.probe = runtimePoolValidProbe(pool, &oldPod, "epoch-old-boot", true)
	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 0 {
		t.Fatalf("replicas after epoch-rotation drain = %d, want 0", got)
	}
	runtimePoolTestAssertSecretsExist(t, r, pool.Namespace, "while the old Pod was live", oldAuthName, oldProviderName, unrelated.Name)
	if err := r.Delete(context.Background(), &oldPod); err != nil {
		t.Fatalf("delete old epoch Pod: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	environment := runtimePoolLiteralEnvironment(deployment.Spec.Template.Spec.Containers[0].Env)
	if got := environment["ORKA_ACP_CONTROLLER_EPOCH"]; got != "8" {
		t.Fatalf("replacement Deployment controller epoch = %q, want 8", got)
	}

	// The old ReplicaSet remains able to create Pods after the old Pod has gone,
	// so its projected/envFrom Secret references must retain the old credentials.
	runtimePoolReconcile(t, r, pool)
	runtimePoolTestAssertSecretsExist(t, r, pool.Namespace, "while an old ReplicaSet could create Pods", oldAuthName, oldProviderName)
	currentReplicaSet := &appsv1.ReplicaSet{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: oldReplicaSet.Name}, currentReplicaSet); err != nil {
		t.Fatalf("get old epoch ReplicaSet for scale-down: %v", err)
	}
	currentReplicaSet.Generation = 2
	currentReplicaSet.Spec.Replicas = new(int32(0))
	currentReplicaSet.Status = appsv1.ReplicaSetStatus{ObservedGeneration: 1}
	if err := r.Update(context.Background(), currentReplicaSet); err != nil {
		t.Fatalf("scale old epoch ReplicaSet to zero: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	runtimePoolTestAssertSecretsExist(t, r, pool.Namespace, "before ReplicaSet scale-down was observed", oldAuthName, oldProviderName)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: oldReplicaSet.Name}, currentReplicaSet); err != nil {
		t.Fatalf("get old epoch ReplicaSet after unobserved scale-down: %v", err)
	}
	if got := ptr.Deref(currentReplicaSet.Spec.Replicas, -1); got != 0 || currentReplicaSet.Status.ObservedGeneration >= currentReplicaSet.Generation {
		t.Fatalf("ReplicaSet scale-down state = replicas %d observed/generation %d/%d, want zero and unobserved", got, currentReplicaSet.Status.ObservedGeneration, currentReplicaSet.Generation)
	}
	if err := r.Delete(context.Background(), currentReplicaSet); err != nil {
		t.Fatalf("delete fully stopped old epoch ReplicaSet: %v", err)
	}
	currentDeployment := runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	currentDeployment.Generation = 2
	if err := r.Update(context.Background(), currentDeployment); err != nil {
		t.Fatalf("advance Deployment generation before observation: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: oldDeployment.Name}, currentDeployment); err != nil {
		t.Fatalf("get unobserved Deployment generation: %v", err)
	}
	currentDeployment.Status.ObservedGeneration = currentDeployment.Generation - 1
	if err := r.Status().Update(context.Background(), currentDeployment); err != nil {
		t.Fatalf("set unobserved Deployment status: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	runtimePoolTestAssertSecretsExist(t, r, pool.Namespace, "before Deployment generation was observed", oldAuthName, oldProviderName)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: oldDeployment.Name}, currentDeployment); err != nil {
		t.Fatalf("get Deployment for observation: %v", err)
	}
	currentDeployment.Status.ObservedGeneration = currentDeployment.Generation
	if err := r.Status().Update(context.Background(), currentDeployment); err != nil {
		t.Fatalf("observe current Deployment generation: %v", err)
	}
	// A follow-up reconcile observes no live workload reference and may safely
	// remove the old immutable credentials.
	runtimePoolReconcile(t, r, pool)
	var secrets corev1.SecretList
	if err := r.List(context.Background(), &secrets, client.InNamespace(pool.Namespace), client.MatchingLabels{
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		t.Fatalf("list RuntimePool Secrets after epoch rotation: %v", err)
	}
	if len(secrets.Items) != 3 {
		names := make([]string, 0, len(secrets.Items))
		for i := range secrets.Items {
			names = append(names, secrets.Items[i].Name)
		}
		sort.Strings(names)
		t.Fatalf("RuntimePool Secret count after epoch rotation = %d, want 2 current Secrets plus unrelated Secret: %v", len(secrets.Items), names)
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if secret.Name == unrelated.Name {
			continue
		}
		if secret.Name == legacyAuth.Name {
			t.Fatalf("stale legacy two-key RuntimePool auth Secret survived epoch rotation: %s", secret.Name)
		}
		if !strings.Contains(secret.Name, "-e8") {
			t.Fatalf("stale RuntimePool Secret survived epoch rotation: %s", secret.Name)
		}
	}
}

func TestRuntimePoolReconcilerDrainsGenericProfileRolloutBeforeTemplateChange(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	oldDeployment, oldPod := runtimePoolTestStartServing(t, r, pool, supervisor, "profile-old-pod", "profile-old-uid", "10.0.0.71", "profile-old-boot")
	oldDigest := pool.Spec.Runtime.Profile.Digest
	oldRevision := oldDeployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]

	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.Runtime.Profile.Model = runtimePoolTestNextModel
	current.Spec.Runtime.Profile.ProxyCredentialScope = "model:" + runtimePoolTestNextModel
	current.Generation++
	runtimePoolTestRefreshProfileDigest(t, &current)
	newDigest := current.Spec.Runtime.Profile.Digest
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("update RuntimePool profile: %v", err)
	}

	supervisor.probe = runtimePoolValidProbe(pool, &oldPod, "profile-old-boot", false)
	runtimePoolReconcile(t, r, pool)
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	oldEnvironment := runtimePoolLiteralEnvironment(deployment.Spec.Template.Spec.Containers[0].Env)
	if oldEnvironment["ORKA_ACP_MODEL"] != acpTestModel || oldEnvironment["ORKA_ACP_RUNTIME_PROFILE_DIGEST"] != oldDigest {
		t.Fatalf("live template changed before profile drain: %#v", oldEnvironment)
	}
	if deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation] != oldRevision {
		t.Fatal("live profile template revision changed before drain")
	}
	if supervisor.drainCalls != 1 {
		t.Fatalf("profile rollout drain calls = %d, want 1", supervisor.drainCalls)
	}

	supervisor.probe = runtimePoolValidProbe(pool, &oldPod, "profile-old-boot", true)
	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)
	if err := r.Delete(context.Background(), &oldPod); err != nil {
		t.Fatalf("delete old profile Pod: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, oldDeployment.Name)
	newEnvironment := runtimePoolLiteralEnvironment(deployment.Spec.Template.Spec.Containers[0].Env)
	if newEnvironment["ORKA_ACP_MODEL"] != runtimePoolTestNextModel || newEnvironment["ORKA_ACP_RUNTIME_PROFILE_DIGEST"] != newDigest {
		t.Fatalf("replacement profile environment = %#v", newEnvironment)
	}
	if newEnvironment["ORKA_ACP_RUNTIME_POOL_GENERATION"] != "2" {
		t.Fatalf("replacement RuntimePool generation = %q, want 2", newEnvironment["ORKA_ACP_RUNTIME_POOL_GENERATION"])
	}
	if deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation] == oldRevision {
		t.Fatal("profile rollout did not produce a new Pod template revision")
	}
}

func TestRuntimePoolReconcilerRolloutAllowsLegacySupervisorWithoutIdentityCapacity(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	_, oldPod := runtimePoolTestStartServing(t, r, pool, supervisor, "legacy-old-pod", "legacy-old-uid", "10.0.0.72", "legacy-old-boot")

	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.Runtime.Profile.Model = runtimePoolTestNextModel
	current.Spec.Runtime.Profile.ProxyCredentialScope = "model:" + runtimePoolTestNextModel
	current.Generation++
	runtimePoolTestRefreshProfileDigest(t, &current)
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("update RuntimePool profile: %v", err)
	}

	legacyProbe := runtimePoolValidProbe(pool, &oldPod, "legacy-old-boot", false)
	legacyProbe.Status.SessionIdentityCapacity = nil
	supervisor.probe = legacyProbe
	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if supervisor.drainCalls != 1 {
		t.Fatalf("legacy rollout drain calls = %d, want 1", supervisor.drainCalls)
	}
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining {
		t.Fatalf("legacy rollout status = %s/%s, want Draining/Draining", status.Lifecycle, status.AdmissionState)
	}

	legacyDrainingProbe := runtimePoolValidProbe(pool, &oldPod, "legacy-old-boot", true)
	legacyDrainingProbe.Status.SessionIdentityCapacity = nil
	supervisor.probe = legacyDrainingProbe
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("legacy rollout lifecycle after authenticated quiescence = %s, want Quiescent", got)
	}
}

func TestRuntimePoolReconcilerRolloutWaitsForReservationsButNotQueuedDemand(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	deployment, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "demand-pod", "demand-pod-uid", "10.0.0.81", "demand-boot")

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	runtimePoolReconcile(t, r, pool)
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "demand-boot", true)
	current := runtimePoolTestGetPool(t, r, pool)
	current.Status.Capacity.QueuedTasks = 3
	current.Status.Capacity.ReservedPrompts = 1
	current.Status.Capacity.Reservations = []corev1alpha1.RuntimePoolCapacityReservationStatus{{
		PoolUID: string(pool.UID), TaskUID: "queued-task", Attempt: 1, ControllerEpoch: 7,
		RuntimeInstanceID: "demand-pod-uid.demand-boot", PromptSlots: 1,
		ReservedAt: metav1.NewTime(runtimePoolTestNow), ExpiresAt: metav1.NewTime(runtimePoolTestNow.Add(time.Minute)),
	}}
	if err := r.Status().Update(context.Background(), &current); err != nil {
		t.Fatalf("add rollout reservation: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleDraining {
		t.Fatalf("lifecycle with reservation = %s, want Draining", got)
	}
	if got := ptr.Deref(runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name).Spec.Replicas, -1); got != 1 {
		t.Fatalf("replicas with reservation = %d, want 1", got)
	}

	current = runtimePoolTestGetPool(t, r, pool)
	current.Status.Capacity.ReservedPrompts = 0
	current.Status.Capacity.Reservations = nil
	if err := r.Status().Update(context.Background(), &current); err != nil {
		t.Fatalf("release rollout reservation: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent || current.Status.Capacity.QueuedTasks != 3 {
		t.Fatalf("queued-demand quiescence = %s queued=%d, want Quiescent with queued demand preserved", current.Status.Lifecycle, current.Status.Capacity.QueuedTasks)
	}
	runtimePoolReconcile(t, r, pool)
	if got := ptr.Deref(runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name).Spec.Replicas, -1); got != 0 {
		t.Fatalf("replicas after queued-demand drain = %d, want 0", got)
	}
}

func TestRuntimePoolReconcilerUnreadyRolloutClearsObservedReadiness(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	_, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "unready-rollout-pod", "unready-rollout-pod-uid", "10.0.0.82", "unready-rollout-boot")

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	runtimePoolTestSetPodReady(t, r, &pod, false)
	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.ActiveInstance == nil || status.ActiveInstance.PodUID != string(pod.UID) {
		t.Fatalf("active instance = %#v, want the previous runtime fence preserved", status.ActiveInstance)
	}
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != runtimePoolSchedulingReasonPodNotReady {
		t.Fatalf("scheduling condition = %#v, want Unknown/PodNotReady", condition)
	}
	if got := runtimePoolReadyReplicas(status); got != 0 {
		t.Fatalf("ready replicas = %d, want 0 while no rollout Pod is Ready", got)
	}
}

func TestRuntimePoolReconcilerUnreadyRolloutPreservesSchedulingFailure(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	_, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "unschedulable-rollout-pod", "unschedulable-rollout-pod-uid", "10.0.0.83", "unschedulable-rollout-boot")

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	runtimePoolTestSetPodReady(t, r, &pod, false)
	currentPod := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), currentPod); err != nil {
		t.Fatalf("get runtime Pod: %v", err)
	}
	scheduled := findRuntimePoolPodCondition(currentPod.Status.Conditions, corev1.PodScheduled)
	if scheduled == nil {
		t.Fatal("runtime Pod has no Scheduled condition")
	}
	scheduled.Status = corev1.ConditionFalse
	scheduled.Reason = corev1.PodReasonUnschedulable
	scheduled.Message = "insufficient cpu"
	if err := r.Status().Update(context.Background(), currentPod); err != nil {
		t.Fatalf("update runtime Pod scheduling: %v", err)
	}
	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != corev1.PodReasonUnschedulable || condition.Message != "insufficient cpu" {
		t.Fatalf("scheduling condition = %#v, want False/Unschedulable with the Pod message", condition)
	}
	if got := runtimePoolReadyReplicas(status); got != 0 {
		t.Fatalf("ready replicas = %d, want 0 while the rollout Pod is unschedulable", got)
	}
}

func TestRuntimePoolReconcilerStoppedRolloutClearsObservedReadiness(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	deployment, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "stopping-rollout-pod", "stopping-rollout-pod-uid", "10.0.0.84", "stopping-rollout-boot")

	currentPod := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), currentPod); err != nil {
		t.Fatalf("get runtime Pod: %v", err)
	}
	currentPod.Finalizers = append(currentPod.Finalizers, "test.orka.ai/hold-termination")
	if err := r.Update(context.Background(), currentPod); err != nil {
		t.Fatalf("add runtime Pod test finalizer: %v", err)
	}

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	runtimePoolReconcile(t, r, pool)
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "stopping-rollout-boot", true)
	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)
	if got := ptr.Deref(runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name).Spec.Replicas, -1); got != 0 {
		t.Fatalf("replicas after authenticated drain = %d, want 0", got)
	}

	if err := r.Delete(context.Background(), currentPod); err != nil {
		t.Fatalf("delete old runtime Pod: %v", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), currentPod); err != nil {
		t.Fatalf("get terminating runtime Pod: %v", err)
	}
	if currentPod.DeletionTimestamp.IsZero() {
		t.Fatal("runtime Pod is not terminating")
	}

	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.ActiveInstance != nil {
		t.Fatalf("active instance = %#v, want nil while the old runtime Pod terminates", status.ActiveInstance)
	}
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != runtimePoolSchedulingReasonPodNotReady {
		t.Fatalf("scheduling condition = %#v, want Unknown/PodNotReady", condition)
	}
	if got := runtimePoolReadyReplicas(status); got != 0 {
		t.Fatalf("ready replicas = %d, want 0 while the old rollout Pod terminates", got)
	}
}

func TestRuntimePoolReconcilerRolloutFailureAndTimeoutPreserveOldPod(t *testing.T) {
	t.Run("drain request failure retries without template replacement", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		supervisor := &fakeRuntimePoolSupervisorClient{}
		r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
		deployment, _ := runtimePoolTestStartServing(t, r, pool, supervisor, "failure-pod", "failure-pod-uid", "10.0.0.91", "failure-boot")
		oldRevision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]

		r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
		supervisor.drainErr = errors.New("temporary drain failure")
		runtimePoolReconcile(t, r, pool)
		status := runtimePoolTestGetPool(t, r, pool).Status
		if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
			t.Fatalf("failure status = %s/%s, want Degraded/Closed", status.Lifecycle, status.AdmissionState)
		}
		deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
		if ptr.Deref(deployment.Spec.Replicas, -1) != 1 || deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation] != oldRevision {
			t.Fatal("drain failure replaced or stopped the live old template")
		}

		supervisor.drainErr = nil
		runtimePoolReconcile(t, r, pool)
		status = runtimePoolTestGetPool(t, r, pool).Status
		if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining || supervisor.drainCalls != 2 {
			t.Fatalf("retry status/calls = %s/%d, want Draining/2", status.Lifecycle, supervisor.drainCalls)
		}
	})

	t.Run("drain timeout fails closed and later converges", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		supervisor := &fakeRuntimePoolSupervisorClient{}
		now := runtimePoolTestNow
		r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
		r.Now = func() time.Time { return now }
		deployment, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "timeout-pod", "timeout-pod-uid", "10.0.0.92", "timeout-boot")
		oldRevision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]

		r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
		runtimePoolReconcile(t, r, pool)
		supervisor.probe = runtimePoolValidProbe(pool, &pod, "timeout-boot", true)
		supervisor.probe.Status.Pressure.LiveDescendants = 1
		now = now.Add(time.Duration(pool.Spec.ColdStartTimeoutSeconds+1) * time.Second)
		runtimePoolReconcile(t, r, pool)
		status := runtimePoolTestGetPool(t, r, pool).Status
		condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
		if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || condition == nil || condition.Reason != runtimePoolRolloutReasonTimedOut {
			t.Fatalf("timeout status/condition = %s/%#v, want Degraded/RolloutTimedOut", status.Lifecycle, condition)
		}
		deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
		if ptr.Deref(deployment.Spec.Replicas, -1) != 1 || deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation] != oldRevision {
			t.Fatal("rollout timeout replaced or stopped the live old template")
		}

		runtimePoolReconcile(t, r, pool)
		status = runtimePoolTestGetPool(t, r, pool).Status
		condition = meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
		if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || condition == nil || condition.Reason != runtimePoolRolloutReasonTimedOut {
			t.Fatalf("timeout retry status/condition = %s/%#v, want Degraded/RolloutTimedOut", status.Lifecycle, condition)
		}

		supervisor.probe.Status.Pressure.LiveDescendants = 0
		runtimePoolReconcile(t, r, pool)
		if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
			t.Fatalf("lifecycle after late quiescence = %s, want Quiescent", got)
		}
	})
}

func TestRuntimePoolReconcilerRolloutFailsClosedWhenActivePodDisappears(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	deployment, oldPod := runtimePoolTestStartServing(t, r, pool, supervisor, "lost-rollout-pod", "lost-rollout-pod-uid", "10.0.0.93", "lost-rollout-boot")
	oldRevision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	if err := r.Delete(context.Background(), &oldPod); err != nil {
		t.Fatalf("delete old runtime Pod: %v", err)
	}
	replacement := runtimePoolReadyPodForDeployment(pool, deployment, "unadmitted-replacement-pod", "unadmitted-replacement-pod-uid", "10.0.0.94")
	runtimePoolTestCreatePod(t, r, &replacement)

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		got.Status.ActiveInstance == nil || got.Status.ActiveInstance.PodUID != string(oldPod.UID) ||
		condition == nil || condition.Reason != corev1alpha1.RuntimePoolReasonRolloutFailed {
		t.Fatalf("active-Pod loss status = %s/%s active=%#v condition=%#v, want fail-closed with the old fence preserved", got.Status.Lifecycle, got.Status.AdmissionState, got.Status.ActiveInstance, condition)
	}
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
	if replicas := ptr.Deref(deployment.Spec.Replicas, -1); replicas != 1 {
		t.Fatalf("replicas after active-Pod loss = %d, want 1", replicas)
	}
	if revision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]; revision != oldRevision {
		t.Fatalf("active-Pod loss installed replacement template %q without termination proof; want %q", revision, oldRevision)
	}
}

func TestRuntimePoolPodTemplateRevisionChangesForEveryRuntimeIdentityInput(t *testing.T) {
	pool := runtimePoolTestObject(1)
	r := runtimePoolTestReconciler(t, runtimePoolTestScheme(t), nil, pool)
	cfg, err := r.runtimePoolConfig(pool)
	if err != nil {
		t.Fatalf("runtimePoolConfig: %v", err)
	}
	selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
	base := r.runtimePoolPodTemplate(pool, cfg, selector, "auth-e7", "provider-old")
	baseRevision := base.Annotations[runtimePoolTemplateRevisionAnnotation]

	tests := map[string]func(*corev1alpha1.RuntimePool, *runtimePoolConfig, *string, *string){
		"image": func(pool *corev1alpha1.RuntimePool, _ *runtimePoolConfig, _, _ *string) {
			pool.Spec.Runtime.Image = "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)
		},
		"profile": func(pool *corev1alpha1.RuntimePool, cfg *runtimePoolConfig, _, _ *string) {
			pool.Spec.Runtime.Profile.Digest = "sha256:" + strings.Repeat("8", 64)
			cfg.profile.Model = "changed-model"
		},
		"auth epoch": func(_ *corev1alpha1.RuntimePool, _ *runtimePoolConfig, auth, _ *string) { *auth = "auth-e8" },
		"provider token": func(_ *corev1alpha1.RuntimePool, cfg *runtimePoolConfig, _, provider *string) {
			cfg.providerProxy.tokenGeneration = runtimePoolProviderTokenGeneration(runtimePoolTestProviderTokenNext)
			*provider = "provider-next"
		},
		"other template field": func(_ *corev1alpha1.RuntimePool, cfg *runtimePoolConfig, _, _ *string) {
			cfg.providerProxy.baseURL = "http://provider-auth-proxy.orka-system.svc:9090"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changedPool := pool.DeepCopy()
			changedConfig := cfg
			authName, providerName := "auth-e7", "provider-old"
			mutate(changedPool, &changedConfig, &authName, &providerName)
			changed := r.runtimePoolPodTemplate(changedPool, changedConfig, selector, authName, providerName)
			if changed.Annotations[runtimePoolTemplateRevisionAnnotation] == baseRevision {
				t.Fatalf("template revision did not change for %s", name)
			}
		})
	}
}

func TestRuntimePoolPodTemplateProjectsE2EPromptWriteAmbiguityMarker(t *testing.T) {
	pool := runtimePoolTestObject(1)
	r := runtimePoolTestReconciler(t, runtimePoolTestScheme(t), nil, pool)
	r.E2EPromptWriteAmbiguityMarker = "ORKA_E2E_WS_LC_AMBIGUOUS_OK"
	cfg, err := r.runtimePoolConfig(pool)
	if err != nil {
		t.Fatalf("runtimePoolConfig: %v", err)
	}
	selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
	template := r.runtimePoolPodTemplate(pool, cfg, selector, "auth", "provider")
	assertRuntimePoolEnvironment(t, r, pool, template.Spec.Containers[0].Env)

	found := false
	for _, item := range template.Spec.Containers[0].Env {
		if item.Name == runtimePoolE2EPromptWriteAmbiguity && item.Value == r.E2EPromptWriteAmbiguityMarker {
			found = true
		}
	}
	if !found {
		t.Fatal("runtime template omitted the configured E2E prompt write ambiguity marker")
	}
}

func TestRuntimePoolReconcilerScaleDownRequiresDrainAndQuiescentStatus(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod", "pod-uid-1", "10.0.0.21")
	supervisor := &fakeRuntimePoolSupervisorClient{probe: runtimePoolValidProbe(pool, &pod, "boot-1", false)}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	var current corev1alpha1.RuntimePool
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.DesiredReplicas = 0
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("set desired replicas to zero: %v", err)
	}
	currentPod := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), currentPod); err != nil {
		t.Fatalf("get runtime Pod: %v", err)
	}
	currentPod.Finalizers = append(currentPod.Finalizers, "test.orka.ai/hold-termination")
	if err := r.Update(context.Background(), currentPod); err != nil {
		t.Fatalf("add runtime Pod test finalizer: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 1 {
		t.Fatalf("replicas after initial drain request = %d, want 1", got)
	}
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls = %d, want 1", supervisor.drainCalls)
	}
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining {
		t.Fatalf("lifecycle after drain request = %s, want Draining", current.Status.Lifecycle)
	}

	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-1", true)
	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 1 {
		t.Fatalf("replicas at first quiescent observation = %d, want 1", got)
	}
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("lifecycle = %s, want Quiescent barrier", current.Status.Lifecycle)
	}

	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	if got := ptr.Deref(deployment.Spec.Replicas, -1); got != 0 {
		t.Fatalf("replicas after persisted quiescent barrier = %d, want 0", got)
	}
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || current.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status after scale down = %s/%s, want Stopping/Closed", current.Status.Lifecycle, current.Status.AdmissionState)
	}
	if err := r.Delete(context.Background(), currentPod); err != nil {
		t.Fatalf("delete runtime Pod: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Status.ActiveInstance != nil {
		t.Fatalf("active instance = %#v, want nil while the scaled-down Pod terminates", current.Status.ActiveInstance)
	}
	condition := meta.FindStatusCondition(current.Status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != runtimePoolSchedulingReasonPodNotReady {
		t.Fatalf("scheduling condition = %#v, want Unknown/PodNotReady", condition)
	}
	if got := runtimePoolReadyReplicas(current.Status); got != 0 {
		t.Fatalf("ready replicas = %d, want 0 while the scaled-down Pod terminates", got)
	}
}

func TestRuntimePoolCompletedScaleToZero(t *testing.T) {
	tests := []struct {
		name     string
		previous corev1alpha1.RuntimePoolStatus
		current  corev1alpha1.RuntimePoolStatus
		want     bool
	}{
		{
			name: "completed",
			previous: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 1, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopping,
			},
			current: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 0, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped,
			},
			want: true,
		},
		{
			name: "initially stopped",
			current: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 0, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped,
			},
		},
		{
			name: "already stopped",
			previous: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 0, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped,
			},
			current: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 0, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopped,
			},
		},
		{
			name: "not stopped",
			previous: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 1, CurrentReplicas: 1, Lifecycle: corev1alpha1.RuntimePoolLifecycleServing,
			},
			current: corev1alpha1.RuntimePoolStatus{
				DesiredReplicas: 0, CurrentReplicas: 0, Lifecycle: corev1alpha1.RuntimePoolLifecycleStopping,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := runtimePoolCompletedScaleToZero(test.previous, test.current); got != test.want {
				t.Fatalf("runtimePoolCompletedScaleToZero() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRuntimePoolReadyReplicasRequiresReadyCondition(t *testing.T) {
	active := &corev1alpha1.RuntimePoolActiveInstanceStatus{PodName: "runtime-pod"}
	tests := []struct {
		name      string
		status    corev1alpha1.RuntimePoolStatus
		wantReady int32
	}{
		{
			name: "selected ready pod",
			status: corev1alpha1.RuntimePoolStatus{
				ActiveInstance: active,
				Conditions: []metav1.Condition{{
					Type: corev1alpha1.RuntimePoolConditionSchedulingReady, Status: metav1.ConditionTrue,
				}},
			},
			wantReady: 1,
		},
		{
			name: "retained fence for not ready pod",
			status: corev1alpha1.RuntimePoolStatus{
				ActiveInstance: active,
				Conditions: []metav1.Condition{{
					Type: corev1alpha1.RuntimePoolConditionSchedulingReady, Status: metav1.ConditionUnknown, Reason: runtimePoolSchedulingReasonPodNotReady,
				}},
			},
		},
		{name: "no selected instance"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := runtimePoolReadyReplicas(test.status); got != test.wantReady {
				t.Fatalf("runtimePoolReadyReplicas() = %d, want %d", got, test.wantReady)
			}
		})
	}
}

func TestFinishRuntimePoolStatusCountsScaleToZeroOnceForStaleReconcile(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(0)
	pool.Name = "scale-zero-idempotent"
	pool.UID = types.UID("scale-zero-idempotent-uid")
	pool.Status = corev1alpha1.RuntimePoolStatus{
		DesiredReplicas: 0,
		CurrentReplicas: 1,
		Lifecycle:       corev1alpha1.RuntimePoolLifecycleStopping,
	}
	r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

	current := runtimePoolTestGetPool(t, r, pool)
	stale := current.DeepCopy()
	stopped := current.Status
	stopped.CurrentReplicas = 0
	stopped.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
	stopped.ActiveInstance = nil

	counter := orkametrics.ACPRuntimePoolScaleToZeroTotal.WithLabelValues(pool.Namespace, pool.Name)
	var beforeMetric dto.Metric
	if err := counter.Write(&beforeMetric); err != nil {
		t.Fatalf("read initial scale-to-zero counter: %v", err)
	}
	before := beforeMetric.GetCounter().GetValue()
	if _, err := r.finishRuntimePoolStatus(context.Background(), &current, stopped, runtimePoolRequeue); err != nil {
		t.Fatalf("first stopped status patch: %v", err)
	}
	// The stale writer loses the optimistic lock; that is a normal race, so
	// it requeues quietly instead of surfacing a reconcile error, and it
	// must not count the scale-to-zero a second time.
	result, err := r.finishRuntimePoolStatus(context.Background(), stale, stopped, runtimePoolRequeue)
	if err != nil || result.RequeueAfter != runtimePoolConflictRequeue {
		t.Fatalf("stale stopped status patch = (%+v, %v), want quiet requeue", result, err)
	}
	var afterMetric dto.Metric
	if err := counter.Write(&afterMetric); err != nil {
		t.Fatalf("read final scale-to-zero counter: %v", err)
	}
	if got := afterMetric.GetCounter().GetValue() - before; got != 1 {
		t.Fatalf("scale-to-zero counter delta = %v, want 1", got)
	}
}

func TestRuntimePoolReconcilerClosesAdmissionForTwoReadyPods(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod1 := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod-1", "pod-uid-1", "10.0.0.31")
	pod2 := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod-2", "pod-uid-2", "10.0.0.32")
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod1, &pod2)

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleAmbiguous || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAmbiguous {
		t.Fatalf("status = %s/%s, want Ambiguous/Ambiguous", got.Status.Lifecycle, got.Status.AdmissionState)
	}
	if got.Status.ActiveInstance != nil {
		t.Fatalf("active instance = %#v, want nil", got.Status.ActiveInstance)
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("probe calls = %d, want none for ambiguous Pods", supervisor.probeCalls)
	}
	condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionAdmissionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != corev1alpha1.RuntimePoolReasonRuntimeAmbiguous {
		t.Fatalf("AdmissionReady condition = %#v", condition)
	}
}

func TestRuntimePoolReconcilerKeepsSupersededPlainPoolAvailableForBoundDemand(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		"profileDigest": pool.Spec.Runtime.Profile.Digest,
		"runtimeImage":  pool.Spec.Runtime.Image,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Name = acpRuntimePoolName(pool.Spec.Runtime.Profile.ProviderKind, harnessv2.ProfileDigest(identity))
	pool.UID = types.UID(pool.Namespace + "-retired-pool-uid")
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	deployment, _ := runtimePoolTestStartServing(t, r, pool, supervisor, "retained-pod", "retained-pod-uid", "10.0.0.95", "retained-boot")
	r.AllowedImages.Codex = "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Spec.DesiredReplicas != 1 || got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing ||
		got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("superseded pool = replicas %d status %s/%s, want 1 and Serving/Accepting", got.Spec.DesiredReplicas, got.Status.Lifecycle, got.Status.AdmissionState)
	}
	if got.Spec.Runtime.Image != pool.Spec.Runtime.Image {
		t.Fatalf("superseded pool image = %q, want immutable original %q", got.Spec.Runtime.Image, pool.Spec.Runtime.Image)
	}
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
	if replicas := ptr.Deref(deployment.Spec.Replicas, -1); replicas != 1 {
		t.Fatalf("superseded deployment replicas = %d, want 1 while bound demand may remain", replicas)
	}
	if meta.FindStatusCondition(got.Status.Conditions, acpRuntimePoolImageProvenanceCondition) != nil {
		t.Fatal("test requires Deployment-backed historical image provenance")
	}
	newGeneration := got.DeepCopy()
	newGeneration.Generation++
	historicalConfig, err := r.runtimePoolConfigForDrain(newGeneration)
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := r.historicalRuntimePoolImageAuthorized(context.Background(), newGeneration, historicalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("Deployment provenance did not survive a controller-owned generation change")
	}
}

func TestRuntimePoolReconcilerReportsSupersededScaleToZeroPoolStopped(t *testing.T) {
	for _, tc := range []struct {
		name            string
		configuredImage string
	}{
		{name: "rotated image", configuredImage: "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)},
		{name: "provider image removed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtimePoolTestScheme(t)
			pool := runtimePoolTestObject(0)
			identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
				"profileDigest": pool.Spec.Runtime.Profile.Digest,
				"runtimeImage":  pool.Spec.Runtime.Image,
			})
			if err != nil {
				t.Fatal(err)
			}
			pool.Name = acpRuntimePoolName(pool.Spec.Runtime.Profile.ProviderKind, harnessv2.ProfileDigest(identity))
			pool.UID = types.UID(pool.Namespace + "-superseded-stopped-pool-uid")
			meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
				Type:               acpRuntimePoolImageProvenanceCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: pool.Generation,
				Reason:             acpRuntimePoolImageProvenanceReason,
				Message:            "RuntimePool image and profile match a verified immutable Task execution plan",
			})
			pool.Generation++
			r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)
			r.AllowedImages.Codex = tc.configuredImage

			runtimePoolReconcile(t, r, pool)
			got := runtimePoolTestGetPool(t, r, pool)
			condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
			if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped ||
				got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
				condition == nil || condition.Status != metav1.ConditionTrue {
				t.Fatalf("historical scale-to-zero status = %s/%s condition=%#v, want Stopped/Closed with rollout ready", got.Status.Lifecycle, got.Status.AdmissionState, condition)
			}
		})
	}
}

func TestHistoricalRuntimePoolImageRecoveryBackfillsWorkspaceProvenanceFromTaskBinding(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolTestObject(1)
	pool.Name = "acp-ws-codex-" + strings.Repeat("a", 16)
	pool.UID = types.UID(pool.Namespace + "-historical-workspace-pool-uid")
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
		BindingDigest: "sha256:" + strings.Repeat("7", 64),
	}
	pool.Spec.Capacity = &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: 1, MaxRunningPrompts: 1}
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	r.AllowedImages.Codex = "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)

	if !acpRuntimePoolImageRequiresHistoricalRecovery(pool, r.AllowedImages) {
		t.Fatal("workspace RuntimePool was excluded from historical image recovery")
	}
	if acpRuntimePoolImageSuperseded(pool, r.AllowedImages) {
		t.Fatal("workspace RuntimePool was exposed to the plain-pool superseded reaper")
	}
	historicalConfig, err := r.runtimePoolConfigForDrain(pool)
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := r.historicalRuntimePoolImageAuthorized(context.Background(), pool, historicalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if authorized {
		t.Fatal("unproven workspace RuntimePool was authorized for a historical image")
	}

	forged := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  pool.Namespace,
			Name:       "forged-workspace-provenance",
			UID:        types.UID("forged-workspace-provenance-uid"),
			Generation: 1,
		},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			RuntimePoolName: pool.Name,
			RuntimePoolUID:  string(pool.UID),
		}},
	}
	if err := r.Create(context.Background(), forged); err != nil {
		t.Fatal(err)
	}
	authorized, err = r.historicalRuntimePoolImageAuthorized(context.Background(), pool, historicalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if authorized {
		t.Fatal("caller-written Task execution status authorized a historical workspace image")
	}

	snapshotDigest := "sha256:" + strings.Repeat("8", 64)
	evidence := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  pool.Namespace,
			Name:       "verified-workspace-provenance",
			UID:        types.UID("verified-workspace-provenance-uid"),
			Generation: 1,
		},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{
			RuntimePoolName: pool.Name,
			RuntimePoolUID:  string(pool.UID),
		}},
	}
	evidence.Status.AgentExecutionBinding = &corev1alpha1.AgentExecutionBinding{
		SchemaVersion:   1,
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		Backend:         corev1alpha1.AgentExecutionBackendRuntimePool,
		Task: corev1alpha1.AgentExecutionBindingTaskRef{
			NamespaceUID:        types.UID("workspace-provenance-namespace-uid"),
			UID:                 evidence.UID,
			BoundSpecGeneration: evidence.Generation,
		},
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
			ID:            string(evidence.UID) + "/" + snapshotDigest,
			Digest:        snapshotDigest,
			SchemaVersion: 1,
		},
		RuntimeType:                       corev1alpha1.AgentRuntimeCodex,
		RuntimeProfileDigest:              pool.Spec.Runtime.Profile.Digest,
		RuntimeProfileDigestSchemaVersion: 1,
		BoundAt:                           metav1.NewTime(runtimePoolTestNow),
	}
	evidence.Status.AgentExecutionBinding.BindingDigest, err = canonicalAgentExecutionBindingDigest(*evidence.Status.AgentExecutionBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), evidence); err != nil {
		t.Fatal(err)
	}
	authorized, err = r.historicalRuntimePoolImageAuthorized(context.Background(), pool, historicalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("exact Task execution binding did not authorize the historical workspace image")
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, acpRuntimePoolImageProvenanceCondition)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != acpRuntimePoolImageProvenanceReason {
		t.Fatalf("backfilled image provenance = %#v, want True/%s", condition, acpRuntimePoolImageProvenanceReason)
	}
	pool.Generation++
	authorized, err = r.historicalRuntimePoolImageAuthorized(context.Background(), pool, historicalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("proven workspace RuntimePool was not authorized after an image rotation")
	}
}

func TestHistoricalRuntimePoolImageRecoveryBackfillsWorkspaceProvenanceFromSessionLineage(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	if err := workspacev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	const (
		workspaceName = "retained-session-workspace"
		sessionName   = "retained-session"
		sessionUID    = "retained-session-uid"
	)
	poolIdentity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		acpWorkspaceSessionUIDMapKey: sessionUID,
		acpWorkspaceSlotMapKey:       defaultWorkspaceSlotName,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := runtimePoolTestObject(0)
	pool.Name = acpWorkspaceRuntimePoolName("session", harnessv2.ProfileDigest(poolIdentity))
	pool.UID = types.UID("retained-session-pool-uid")
	pool.Labels[acpExecutionWorkspaceLinkLabel] = workspaceName
	pool.Annotations = map[string]string{}
	pool.Annotations[acpExecutionWorkspaceUIDAnnotation] = "retained-session-workspace-uid"
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
		BindingDigest: "sha256:" + strings.Repeat("7", 64),
	}
	pool.Spec.Capacity = &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: 1, MaxRunningPrompts: 1}

	classBinding := workspacev1alpha1.ImmutableObjectBinding{
		Name: "workspace-class", UID: types.UID("workspace-class-uid"), Generation: 1,
		ProfileHash: "sha256:" + strings.Repeat("4", 64),
	}
	providerBinding := workspacev1alpha1.ImmutableObjectBinding{
		Name: "workspace-provider", UID: types.UID("workspace-provider-uid"), Generation: 1,
	}
	linkedWorkspace := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      workspaceName,
			UID:       types.UID(pool.Annotations[acpExecutionWorkspaceUIDAnnotation]),
			Labels: map[string]string{
				workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue,
			},
			Annotations: map[string]string{
				acpExecutionWorkspacePoolAnnotation: pool.Name,
				acpWorkspaceBackendAnnotation:       string(pool.Spec.ExecutionWorkspace.Provider),
			},
			Generation: 2,
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			Mode:            workspacev1alpha1.ExecutionWorkspaceModeInteractive,
			ClassBinding:    classBinding,
			ProviderBinding: providerBinding,
			CoreAdmission: &workspacev1alpha1.ExecutionWorkspaceCoreAdmission{
				ClassBinding: classBinding, ProviderBinding: providerBinding, AdmittedGeneration: 2,
			},
			SessionRef: &workspacev1alpha1.ObjectIdentityReference{
				Name: sessionName,
				UID:  types.UID(sessionUID),
			},
			Slot: defaultWorkspaceSlotName,
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{
			ObservedGeneration: 2,
			Conditions: []metav1.Condition{{
				Type:               string(workspacev1alpha1.ConditionWorkspaceAdmitted),
				Status:             metav1.ConditionTrue,
				Reason:             string(workspacev1alpha1.ReasonReady),
				ObservedGeneration: 2,
			}},
		},
	}
	lineageDigest, err := acpSessionLineageConfigurationDigest(
		pool.Spec.Runtime.Profile.Digest,
		pool.Spec.Runtime.Image,
		pool.Spec.ExecutionWorkspace.BindingDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	sessionControl := &corev1alpha1.RuntimeSessionControl{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      storekube.RuntimeSessionControlObjectName(sessionName),
			UID:       types.UID("retained-session-control-uid"),
		},
		Spec: corev1alpha1.RuntimeSessionControlSpec{
			SessionName:   sessionName,
			SessionUID:    sessionUID,
			RequestDigest: "sha256:" + strings.Repeat("5", 64),
			Owner:         corev1alpha1.ControlRecordOwner{Kind: "Session", UID: sessionUID},
		},
		Status: corev1alpha1.RuntimeSessionControlStatus{
			ControlRecordMutationStatus: corev1alpha1.ControlRecordMutationStatus{Version: 1},
			Lineage: &corev1alpha1.RuntimeSessionLineageStatus{
				NamespaceUID:    types.UID("retained-session-namespace-uid"),
				SessionUID:      sessionUID,
				ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
				Generation:      1,
				RuntimeIdentity: runtimePoolProviderCodex,
				ConfigDigest:    lineageDigest,
				EstablishedAt:   metav1.NewTime(runtimePoolTestNow),
			},
		},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: pool.Namespace,
		UID:  sessionControl.Status.Lineage.NamespaceUID,
	}}

	tests := []struct {
		name   string
		mutate func(*corev1alpha1.RuntimePool, *workspacev1alpha1.ExecutionWorkspace, *corev1alpha1.RuntimeSessionControl)
		want   bool
	}{
		{
			name: "workspace incarnation mismatch",
			mutate: func(candidate *corev1alpha1.RuntimePool, _ *workspacev1alpha1.ExecutionWorkspace, _ *corev1alpha1.RuntimeSessionControl) {
				candidate.Annotations[acpExecutionWorkspaceUIDAnnotation] = "recreated-workspace-uid"
			},
		},
		{
			name: "workspace lacks core admission evidence",
			mutate: func(_ *corev1alpha1.RuntimePool, candidate *workspacev1alpha1.ExecutionWorkspace, _ *corev1alpha1.RuntimeSessionControl) {
				candidate.Spec.CoreAdmission = nil
			},
		},
		{
			name: "reciprocal link has nondeterministic pool identity",
			mutate: func(candidate *corev1alpha1.RuntimePool, workspace *workspacev1alpha1.ExecutionWorkspace, _ *corev1alpha1.RuntimeSessionControl) {
				candidate.Name = "acp-ws-session-0000000000000000"
				workspace.Annotations[acpExecutionWorkspacePoolAnnotation] = candidate.Name
			},
		},
		{
			name: "session lineage does not match runtime configuration",
			mutate: func(_ *corev1alpha1.RuntimePool, _ *workspacev1alpha1.ExecutionWorkspace, candidate *corev1alpha1.RuntimeSessionControl) {
				candidate.Status.Lineage.ConfigDigest = "sha256:" + strings.Repeat("6", 64)
			},
		},
		{name: "exact retained session lineage", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidatePool := pool.DeepCopy()
			candidateWorkspace := linkedWorkspace.DeepCopy()
			candidateControl := sessionControl.DeepCopy()
			if tc.mutate != nil {
				tc.mutate(candidatePool, candidateWorkspace, candidateControl)
			}
			r := runtimePoolTestReconciler(t, scheme, nil, candidatePool, candidateWorkspace, candidateControl, namespace.DeepCopy())
			r.AllowedImages.Codex = "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)
			historicalConfig, err := r.runtimePoolConfigForDrain(candidatePool)
			if err != nil {
				t.Fatal(err)
			}
			authorized, err := r.historicalRuntimePoolImageAuthorized(context.Background(), candidatePool, historicalConfig)
			if err != nil {
				t.Fatal(err)
			}
			if authorized != tc.want {
				t.Fatalf("historical image authorized = %t, want %t", authorized, tc.want)
			}
			condition := meta.FindStatusCondition(candidatePool.Status.Conditions, acpRuntimePoolImageProvenanceCondition)
			if (condition != nil) != tc.want {
				t.Fatalf("backfilled provenance condition = %#v, want present %t", condition, tc.want)
			}
		})
	}
}

func TestRuntimePoolReconcilerReportsBadImageAndScheduling(t *testing.T) {
	t.Run("unmanaged pool is rejected", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		delete(pool.Labels, acpRuntimePoolLabel)
		r := runtimePoolTestReconciler(t, scheme, nil, pool)

		runtimePoolReconcile(t, r, pool)
		got := runtimePoolTestGetPool(t, r, pool)
		if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || !strings.Contains(got.Status.Message, "controller-owned") {
			t.Fatalf("unmanaged status = %#v", got.Status)
		}
	})

	t.Run("deterministic superseded pool without provenance is rejected", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		pool.Spec.Runtime.Image = "docker.io/example/unapproved@sha256:" + strings.Repeat("9", 64)
		identity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
			"profileDigest": pool.Spec.Runtime.Profile.Digest,
			"runtimeImage":  pool.Spec.Runtime.Image,
		})
		if err != nil {
			t.Fatal(err)
		}
		pool.Name = acpRuntimePoolName(pool.Spec.Runtime.Profile.ProviderKind, harnessv2.ProfileDigest(identity))
		pool.UID = types.UID(pool.Namespace + "-unproven-superseded-pool-uid")
		r := runtimePoolTestReconciler(t, scheme, nil, pool)

		runtimePoolReconcile(t, r, pool)
		got := runtimePoolTestGetPool(t, r, pool)
		if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || !strings.Contains(got.Status.Message, "controller-approved") {
			t.Fatalf("unapproved-image status = %#v", got.Status)
		}
		var deployment appsv1.Deployment
		err = r.Get(context.Background(), types.NamespacedName{
			Namespace: pool.Namespace,
			Name:      runtimePoolResourceName(pool.Namespace, pool.Name),
		}, &deployment)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("Deployment exists for unproven superseded image, err=%v", err)
		}
	})

	t.Run("mutable image is rejected", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		pool.Spec.Runtime.Image = "docker.io/sozercan/orka-acp:latest"
		r := runtimePoolTestReconciler(t, scheme, nil, pool)

		runtimePoolReconcile(t, r, pool)
		got := runtimePoolTestGetPool(t, r, pool)
		condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
		if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || condition == nil || condition.Status != metav1.ConditionFalse {
			t.Fatalf("bad-image status = %#v", got.Status)
		}
		var deployment appsv1.Deployment
		err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolResourceName(pool.Namespace, pool.Name)}, &deployment)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("Deployment exists for mutable image, err=%v", err)
		}
	})

	t.Run("unschedulable pod closes admission", func(t *testing.T) {
		scheme := runtimePoolTestScheme(t)
		pool := runtimePoolTestObject(1)
		pod := runtimePoolPendingPod(pool, pool.Namespace, "codex-pod", "pod-uid-1")
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "insufficient cpu",
		}}
		r := runtimePoolTestReconciler(t, scheme, nil, pool, &pod)

		runtimePoolReconcile(t, r, pool)
		got := runtimePoolTestGetPool(t, r, pool)
		condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
		if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "Unschedulable" {
			t.Fatalf("scheduling status = %#v", got.Status)
		}
	})
}

func TestRuntimePoolReconcilerCleanupOnlySkipsActivePool(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	r.CleanupOnly = true

	result := runtimePoolReconcile(t, r, pool)
	if result != (ctrl.Result{}) {
		t.Fatalf("cleanup-only active pool result = %#v, want zero result", result)
	}
	got := runtimePoolTestGetPool(t, r, pool)
	if controllerutil.ContainsFinalizer(&got, runtimePoolFinalizer) {
		t.Fatal("cleanup-only reconciliation added a RuntimePool finalizer")
	}
	var deployment appsv1.Deployment
	err := r.Get(context.Background(), types.NamespacedName{
		Namespace: pool.Namespace,
		Name:      runtimePoolResourceName(pool.Namespace, pool.Name),
	}, &deployment)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup-only reconciliation created Deployment, err=%v", err)
	}
}

func TestRuntimePoolReconcilerCleanupOnlyFinalizerCleansCrossNamespaceChildren(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(0)
	pool.Spec.RuntimeNamespace = acpTestRuntimeNamespace
	pool.Finalizers = []string{runtimePoolFinalizer}
	deletedAt := metav1.NewTime(runtimePoolTestNow)
	pool.DeletionTimestamp = &deletedAt
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	labels := map[string]string{
		runtimePoolKeyLabel:       runtimePoolKey(pool.Namespace, pool.Name),
		runtimePoolNamespaceLabel: pool.Namespace,
		runtimePoolNameLabel:      pool.Name,
	}
	objects := []client.Object{
		pool,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: base, Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: base, Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "managed-secret", Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runtime-pod", Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "runtime-rs", Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(base, "pdb"), Namespace: pool.Spec.RuntimeNamespace, Labels: labels}},
	}
	for _, suffix := range []string{
		"deny-all", "control-in", "dns-egress", "provider-proxy-egress", "controller-egress",
		"control-plane-egress", "artifact-api-egress",
	} {
		objects = append(objects, &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
			Name: runtimePoolChildName(base, suffix), Namespace: pool.Spec.RuntimeNamespace, Labels: labels,
		}})
	}
	r := runtimePoolTestReconciler(t, scheme, nil, objects...)
	r.CleanupOnly = true

	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)

	for _, object := range objects[1:] {
		key := client.ObjectKeyFromObject(object)
		check := object.DeepCopyObject().(client.Object)
		if err := r.Get(context.Background(), key, check); !apierrors.IsNotFound(err) {
			t.Fatalf("child %T %s still exists, err=%v", object, key, err)
		}
	}
	var got corev1alpha1.RuntimePool
	err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &got)
	if err == nil && controllerutil.ContainsFinalizer(&got, runtimePoolFinalizer) {
		t.Fatal("RuntimePool cleanup finalizer was not removed")
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("Get deleting RuntimePool: %v", err)
	}
}

func TestRuntimePoolReconcilerDeletionRemovesMetricsBeforeCleanupCompletes(t *testing.T) {
	orkametrics.ACPRuntimePoolDesiredReplicas.Reset()
	orkametrics.ACPRuntimePoolReadyReplicas.Reset()
	orkametrics.ACPRuntimePoolSessionsActive.Reset()
	orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
	orkametrics.ACPRuntimePoolQueuedTasks.Reset()
	orkametrics.ACPRuntimePoolAdmissionState.Reset()
	orkametrics.ACPRuntimePoolScaleToZeroTotal.Reset()
	t.Cleanup(func() {
		orkametrics.ACPRuntimePoolDesiredReplicas.Reset()
		orkametrics.ACPRuntimePoolReadyReplicas.Reset()
		orkametrics.ACPRuntimePoolSessionsActive.Reset()
		orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
		orkametrics.ACPRuntimePoolQueuedTasks.Reset()
		orkametrics.ACPRuntimePoolAdmissionState.Reset()
		orkametrics.ACPRuntimePoolScaleToZeroTotal.Reset()
	})

	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pool.Finalizers = []string{runtimePoolFinalizer}
	deletedAt := metav1.NewTime(runtimePoolTestNow)
	pool.DeletionTimestamp = &deletedAt
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: base, Namespace: pool.Namespace, Finalizers: []string{"test.orka.ai/hold-cleanup"},
	}}
	r := runtimePoolTestReconciler(t, scheme, nil, pool, deployment)
	orkametrics.RecordACPRuntimePoolStatus(pool.Namespace, pool.Name, 1, 1, 2, 1, 3, string(corev1alpha1.RuntimePoolAdmissionAccepting))
	orkametrics.RecordACPRuntimePoolScaleToZero(pool.Namespace, pool.Name)
	if got := runtimePoolMetricSeriesCount(orkametrics.ACPRuntimePoolDesiredReplicas); got != 1 {
		t.Fatalf("desired replica series before deletion = %d, want 1", got)
	}

	runtimePoolReconcile(t, r, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	if !controllerutil.ContainsFinalizer(&current, runtimePoolFinalizer) {
		t.Fatal("RuntimePool finalizer was removed before child cleanup completed")
	}
	for name, collector := range map[string]prometheus.Collector{
		"desired replicas":  orkametrics.ACPRuntimePoolDesiredReplicas,
		"ready replicas":    orkametrics.ACPRuntimePoolReadyReplicas,
		"active sessions":   orkametrics.ACPRuntimePoolSessionsActive,
		"in-flight prompts": orkametrics.ACPRuntimePoolPromptsInFlight,
		"queued tasks":      orkametrics.ACPRuntimePoolQueuedTasks,
		"admission state":   orkametrics.ACPRuntimePoolAdmissionState,
		"scale-to-zero":     orkametrics.ACPRuntimePoolScaleToZeroTotal,
	} {
		if got := runtimePoolMetricSeriesCount(collector); got != 0 {
			t.Fatalf("%s series during deletion = %d, want 0", name, got)
		}
	}
}

func TestACPRuntimePoolMetricsAreExposedAndDeletedFromRegistry(t *testing.T) {
	orkametrics.ACPRuntimePoolDesiredReplicas.Reset()
	orkametrics.ACPRuntimePoolReadyReplicas.Reset()
	orkametrics.ACPRuntimePoolSessionsActive.Reset()
	orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
	orkametrics.ACPRuntimePoolQueuedTasks.Reset()
	orkametrics.ACPRuntimePoolAdmissionState.Reset()
	orkametrics.ACPRuntimePoolScaleToZeroTotal.Reset()
	t.Cleanup(func() {
		orkametrics.ACPRuntimePoolDesiredReplicas.Reset()
		orkametrics.ACPRuntimePoolReadyReplicas.Reset()
		orkametrics.ACPRuntimePoolSessionsActive.Reset()
		orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
		orkametrics.ACPRuntimePoolQueuedTasks.Reset()
		orkametrics.ACPRuntimePoolAdmissionState.Reset()
		orkametrics.ACPRuntimePoolScaleToZeroTotal.Reset()
	})

	const namespace = "metrics-test"
	const poolName = "codex-pool"
	orkametrics.RecordACPRuntimePoolStatus(namespace, poolName, 2, 1, 3, 4, 5, "Accepting")
	orkametrics.RecordACPRuntimePoolScaleToZero(namespace, poolName)

	body := scrapeControllerMetrics(t)
	wantLines := []string{
		`orka_acp_runtime_pool_desired_replicas{namespace="metrics-test",runtime_pool="codex-pool"} 2`,
		`orka_acp_runtime_pool_ready_replicas{namespace="metrics-test",runtime_pool="codex-pool"} 1`,
		`orka_acp_runtime_pool_sessions_active{namespace="metrics-test",runtime_pool="codex-pool"} 3`,
		`orka_acp_runtime_pool_prompts_in_flight{namespace="metrics-test",runtime_pool="codex-pool"} 4`,
		`orka_acp_runtime_pool_queued_tasks{namespace="metrics-test",runtime_pool="codex-pool"} 5`,
		`orka_acp_runtime_pool_admission_state{namespace="metrics-test",runtime_pool="codex-pool",state="accepting"} 1`,
		`orka_acp_runtime_pool_admission_state{namespace="metrics-test",runtime_pool="codex-pool",state="ambiguous"} 0`,
		`orka_acp_runtime_pool_admission_state{namespace="metrics-test",runtime_pool="codex-pool",state="closed"} 0`,
		`orka_acp_runtime_pool_admission_state{namespace="metrics-test",runtime_pool="codex-pool",state="draining"} 0`,
		`orka_acp_runtime_pool_admission_state{namespace="metrics-test",runtime_pool="codex-pool",state="unknown"} 0`,
		`orka_acp_runtime_pool_scale_to_zero_total{namespace="metrics-test",runtime_pool="codex-pool"} 1`,
	}
	for _, want := range wantLines {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics scrape missing %q", want)
		}
	}

	orkametrics.DeleteACPRuntimePool(namespace, poolName)
	body = scrapeControllerMetrics(t)
	for _, metricName := range []string{
		"orka_acp_runtime_pool_desired_replicas",
		"orka_acp_runtime_pool_ready_replicas",
		"orka_acp_runtime_pool_sessions_active",
		"orka_acp_runtime_pool_prompts_in_flight",
		"orka_acp_runtime_pool_queued_tasks",
		"orka_acp_runtime_pool_admission_state",
		"orka_acp_runtime_pool_scale_to_zero_total",
	} {
		if strings.Contains(body, metricName+`{namespace="`+namespace+`",runtime_pool="`+poolName+`"`) {
			t.Fatalf("metrics scrape retained deleted %s series", metricName)
		}
	}
}

func scrapeControllerMetrics(t *testing.T) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics scrape status = %d, want %d", recorder.Code, http.StatusOK)
	}
	return recorder.Body.String()
}

func runtimePoolMetricSeriesCount(collector prometheus.Collector) int {
	metrics := make(chan prometheus.Metric, 16)
	collector.Collect(metrics)
	close(metrics)
	return len(metrics)
}

func TestRuntimePoolReconcilerPublishesExactActivePodStatus(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod", "pod-uid-exact", "10.0.0.42")
	supervisor := &fakeRuntimePoolSupervisorClient{probe: runtimePoolValidProbe(pool, &pod, "boot-exact", false)}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	active := got.Status.ActiveInstance
	if active == nil {
		t.Fatal("active instance is nil")
	}
	if active.PodNamespace != pod.Namespace || active.PodName != pod.Name || active.PodAddress != pod.Status.PodIP || active.PodUID != string(pod.UID) {
		t.Fatalf("active Pod identity = %#v, want exact Pod %#v", active, pod.ObjectMeta)
	}
	if active.BootID != "boot-exact" || active.RuntimeInstanceID != "pod-uid-exact.boot-exact" {
		t.Fatalf("active runtime fence = boot %q instance %q", active.BootID, active.RuntimeInstanceID)
	}
	if active.ControllerEpoch != 7 || active.ProtocolVersion != corev1alpha1.RuntimePoolProtocolHarnessV2 || active.ProfileDigest != pool.Spec.Runtime.Profile.Digest {
		t.Fatalf("active protocol/profile fence = %#v", active)
	}
	if active.LastObservedTime == nil || !active.LastObservedTime.Time.Equal(runtimePoolTestNow) {
		t.Fatalf("last observed = %#v, want %s", active.LastObservedTime, runtimePoolTestNow)
	}
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("status = %s/%s, want Serving/Accepting", got.Status.Lifecycle, got.Status.AdmissionState)
	}
	for _, conditionType := range []string{
		corev1alpha1.RuntimePoolConditionAdmissionReady,
		corev1alpha1.RuntimePoolConditionPodSecurityReady,
		corev1alpha1.RuntimePoolConditionQuotaReady,
		corev1alpha1.RuntimePoolConditionSchedulingReady,
		corev1alpha1.RuntimePoolConditionRolloutReady,
	} {
		condition := meta.FindStatusCondition(got.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			t.Fatalf("condition %s = %#v, want True", conditionType, condition)
		}
	}
}

func TestRuntimePoolReconcilerUnhealthySupervisorClearsReadinessBeforeRecycle(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "unhealthy-pod", "unhealthy-pod-uid", "10.0.0.48")
	probe := runtimePoolValidProbe(pool, &pod, "unhealthy-boot", false)
	probe.Status.Lifecycle = harnessv2.SupervisorLifecycleUnhealthy
	supervisor := &fakeRuntimePoolSupervisorClient{probe: probe}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("unhealthy status = %s/%s, want Degraded/Closed", status.Lifecycle, status.AdmissionState)
	}
	if status.ActiveInstance != nil {
		t.Fatalf("active instance = %#v, want nil after unhealthy Pod deletion", status.ActiveInstance)
	}
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != runtimePoolSchedulingReasonPodNotReady {
		t.Fatalf("scheduling condition = %#v, want Unknown/PodNotReady", condition)
	}
	if got := runtimePoolReadyReplicas(status); got != 0 {
		t.Fatalf("ready replicas = %d, want 0 after unhealthy Pod deletion", got)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unhealthy runtime Pod still exists, err=%v", err)
	}
}

func TestRuntimePoolReconcilerClearsProbePressureAfterActivePodDisappears(t *testing.T) {
	orkametrics.ACPRuntimePoolSessionsActive.Reset()
	orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
	t.Cleanup(func() {
		orkametrics.ACPRuntimePoolSessionsActive.Reset()
		orkametrics.ACPRuntimePoolPromptsInFlight.Reset()
	})

	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "lost-pressure-pod", "lost-pressure-pod-uid", "10.0.0.49")
	supervisor := &fakeRuntimePoolSupervisorClient{probe: runtimePoolValidProbe(pool, &pod, "lost-pressure-boot", false)}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	current.Status.Capacity.ResidentSessions = 3
	current.Status.Capacity.RunningPrompts = 2
	current.Status.Capacity.PendingPermissions = 1
	current.Status.Capacity.LiveDescendants = 4
	current.Status.Capacity.QueuedTasks = 5
	current.Status.Capacity.ReservedSessions = 1
	current.Status.Capacity.FinalizingSessions = 2
	if err := r.Status().Update(context.Background(), &current); err != nil {
		t.Fatalf("persist stale pressure fixture: %v", err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete active runtime Pod: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.ActiveInstance != nil {
		t.Fatalf("active instance = %#v, want nil after active Pod disappearance", status.ActiveInstance)
	}
	if status.Capacity.ResidentSessions != 0 || status.Capacity.RunningPrompts != 0 ||
		status.Capacity.PendingPermissions != 0 || status.Capacity.LiveDescendants != 0 {
		t.Fatalf("unfenced probe pressure = %#v, want all supervisor counters cleared", status.Capacity)
	}
	if status.Capacity.QueuedTasks != 5 || status.Capacity.ReservedSessions != 1 || status.Capacity.FinalizingSessions != 2 {
		t.Fatalf("controller-owned capacity = %#v, want queued/reserved/finalizing counters preserved", status.Capacity)
	}
	for name, gauge := range map[string]*prometheus.GaugeVec{
		"active sessions":   orkametrics.ACPRuntimePoolSessionsActive,
		"in-flight prompts": orkametrics.ACPRuntimePoolPromptsInFlight,
	} {
		var metric dto.Metric
		if err := gauge.WithLabelValues(pool.Namespace, pool.Name).Write(&metric); err != nil {
			t.Fatalf("read %s gauge: %v", name, err)
		}
		if got := metric.GetGauge().GetValue(); got != 0 {
			t.Fatalf("%s gauge = %v, want 0 after active Pod disappearance", name, got)
		}
	}
}

func TestRuntimePoolReconcilerPreservesActiveFenceAcrossReadinessGap(t *testing.T) {
	const oldRuntimeInstanceID = "restart-gap-pod-uid.boot-before-gap"

	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "restart-gap-pod", "restart-gap-pod-uid", "10.0.0.45")
	supervisor := &fakeRuntimePoolSupervisorClient{probe: runtimePoolValidProbe(pool, &pod, "boot-before-gap", false)}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	runtimePoolTestSetPodReady(t, r, &pod, false)
	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		got.Status.ActiveInstance == nil || got.Status.ActiveInstance.RuntimeInstanceID != oldRuntimeInstanceID {
		t.Fatalf("readiness-gap status = %s/%s active=%#v, want Degraded/Closed with the old fence preserved", got.Status.Lifecycle, got.Status.AdmissionState, got.Status.ActiveInstance)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); err != nil {
		t.Fatalf("runtime Pod disappeared during the readiness gap: %v", err)
	}
}

func TestRuntimePoolReconcilerRecyclesPodAfterInPlaceSupervisorRestart(t *testing.T) {
	const oldRuntimeInstanceID = "restart-pod-uid.boot-before-restart"

	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "restart-pod", "restart-pod-uid", "10.0.0.47")
	supervisor := &fakeRuntimePoolSupervisorClient{probe: runtimePoolValidProbe(pool, &pod, "boot-before-restart", false)}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)
	deleteRecorder := &runtimePoolPodDeleteRecordingClient{Client: r.Client}
	r.Client = deleteRecorder

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Status.ActiveInstance == nil || got.Status.ActiveInstance.RuntimeInstanceID != oldRuntimeInstanceID {
		t.Fatalf("initial active instance = %#v", got.Status.ActiveInstance)
	}

	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-after-restart", false)
	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status after restart detection = %s/%s, want Degraded/Closed", got.Status.Lifecycle, got.Status.AdmissionState)
	}
	if got.Status.ActiveInstance == nil || got.Status.ActiveInstance.RuntimeInstanceID != oldRuntimeInstanceID {
		t.Fatalf("restart detection replaced the persisted active fence: %#v", got.Status.ActiveInstance)
	}
	condition := meta.FindStatusCondition(got.Status.Conditions, corev1alpha1.RuntimePoolConditionAdmissionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != runtimePoolSupervisorRestartReasonDetected {
		t.Fatalf("restart admission condition = %#v", condition)
	}
	if deleteRecorder.podDeleteHadPreconditions {
		t.Fatal("runtime Pod was deleted before the admission closure persisted")
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); err != nil {
		t.Fatalf("runtime Pod disappeared before the replacement barrier persisted: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed || got.Status.ActiveInstance != nil {
		t.Fatalf("status after restart Pod recycle = %s/%s active=%#v, want Stopping/Closed with no active instance", got.Status.Lifecycle, got.Status.AdmissionState, got.Status.ActiveInstance)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("in-place restarted runtime Pod still exists, err=%v", err)
	}
	if !deleteRecorder.podDeleteHadPreconditions || deleteRecorder.podDeleteUID != pod.UID || deleteRecorder.podDeleteResourceVersion == "" {
		t.Fatalf(
			"restart Pod delete preconditions = present:%t uid:%q resourceVersion:%q, want exact UID %q and non-empty resourceVersion",
			deleteRecorder.podDeleteHadPreconditions,
			deleteRecorder.podDeleteUID,
			deleteRecorder.podDeleteResourceVersion,
			pod.UID,
		)
	}
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	if replicas := ptr.Deref(deployment.Spec.Replicas, 0); replicas != 1 {
		t.Fatalf("deployment replicas after restart Pod recycle = %d, want 1", replicas)
	}
}

func TestRuntimePoolReconcilerRecyclesInPlaceRestartDuringRollout(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	deployment, pod := runtimePoolTestStartServing(t, r, pool, supervisor, "rollout-restart-pod", "rollout-restart-uid", "10.0.0.46", "rollout-boot-before")
	oldRevision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]
	deleteRecorder := &runtimePoolPodDeleteRecordingClient{Client: r.Client}
	r.Client = deleteRecorder

	r.ProviderProxy.BearerToken = bytes.Clone(runtimePoolTestProviderTokenNext)
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "rollout-boot-after", false)
	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		got.Status.ActiveInstance == nil || got.Status.ActiveInstance.RuntimeInstanceID != "rollout-restart-uid.rollout-boot-before" {
		t.Fatalf("rollout restart barrier = %s/%s active=%#v", got.Status.Lifecycle, got.Status.AdmissionState, got.Status.ActiveInstance)
	}
	if deleteRecorder.podDeleteHadPreconditions {
		t.Fatal("rollout runtime Pod was deleted before the restart barrier persisted")
	}

	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || got.Status.ActiveInstance != nil {
		t.Fatalf("rollout restart recycle status = %s active=%#v", got.Status.Lifecycle, got.Status.ActiveInstance)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("rollout in-place restarted Pod still exists, err=%v", err)
	}
	if !deleteRecorder.podDeleteHadPreconditions || deleteRecorder.podDeleteUID != pod.UID || deleteRecorder.podDeleteResourceVersion == "" {
		t.Fatalf("rollout restart delete preconditions = present:%t uid:%q resourceVersion:%q", deleteRecorder.podDeleteHadPreconditions, deleteRecorder.podDeleteUID, deleteRecorder.podDeleteResourceVersion)
	}
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
	if replicas := ptr.Deref(deployment.Spec.Replicas, -1); replicas != 0 {
		t.Fatalf("rollout deployment replicas after restart recycle = %d, want 0", replicas)
	}

	runtimePoolReconcile(t, r, pool)
	deployment = runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name)
	if replicas := ptr.Deref(deployment.Spec.Replicas, -1); replicas != 1 {
		t.Fatalf("rollout deployment replicas after staging replacement = %d, want 1", replicas)
	}
	if revision := deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation]; revision == oldRevision {
		t.Fatalf("rollout replacement retained old template revision %q", revision)
	}
}

func TestRuntimePoolReconcilerMigratesSteadyStateWithoutIdentityCapacity(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "legacy-serving-pod", "legacy-serving-uid", "10.0.0.44")
	probe := runtimePoolValidProbe(pool, &pod, "legacy-serving-boot", false)
	probe.Status.SessionIdentityCapacity = nil
	supervisor := &fakeRuntimePoolSupervisorClient{probe: probe}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining {
		t.Fatalf("steady-state legacy status = %s/%s, want Draining/Draining", got.Status.Lifecycle, got.Status.AdmissionState)
	}
	if got.Status.ActiveInstance == nil {
		t.Fatal("legacy supervisor migration lost exact active instance")
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 1 || supervisor.drainReason != harnessv2.DrainReasonSessionIdentityCapacity {
		t.Fatalf("legacy migration drain = calls:%d reason:%q", supervisor.drainCalls, supervisor.drainReason)
	}
}

func TestRuntimePoolReconcilerRotatesBeforeSessionIdentityExhaustion(t *testing.T) {
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolTestObject(1)
	pod := runtimePoolReadyPod(pool, pool.Namespace, "codex-pod", "pod-uid-identity", "10.0.0.43")
	probe := runtimePoolValidProbe(pool, &pod, "boot-identity", false)
	probe.Status.SessionIdentityCapacity.Remaining = probe.Status.SessionIdentityCapacity.ExhaustionReserve
	probe.Status.Drain.AcceptingNewSessions = false
	supervisor := &fakeRuntimePoolSupervisorClient{probe: probe}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool, &pod)
	deleteRecorder := &runtimePoolPodDeleteRecordingClient{Client: r.Client}
	r.Client = deleteRecorder

	runtimePoolReconcile(t, r, pool)
	got := runtimePoolTestGetPool(t, r, pool)
	if supervisor.drainCalls != 0 {
		t.Fatalf("identity drain was requested before admission closure persisted: %d calls", supervisor.drainCalls)
	}
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining {
		t.Fatalf("status after identity admission closure = %s/%s, want Draining/Draining", got.Status.Lifecycle, got.Status.AdmissionState)
	}

	got.Status.Capacity.ReservedSessions = 1
	got.Status.Capacity.Reservations = []corev1alpha1.RuntimePoolCapacityReservationStatus{{
		PoolUID: string(pool.UID), TaskUID: "reservation-race", Attempt: 1, ControllerEpoch: 7,
		RuntimeInstanceID: "pod-uid-identity.boot-identity", ResidentSlots: 1,
		ReservedAt: metav1.NewTime(runtimePoolTestNow), ExpiresAt: metav1.NewTime(runtimePoolTestNow.Add(time.Minute)),
	}}
	if err := r.Status().Update(context.Background(), &got); err != nil {
		t.Fatalf("add raced identity reservation: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 0 {
		t.Fatalf("identity drain was requested while a raced reservation remained: %d calls", supervisor.drainCalls)
	}

	got = runtimePoolTestGetPool(t, r, pool)
	got.Status.Capacity.ReservedSessions = 0
	got.Status.Capacity.Reservations = nil
	if err := r.Status().Update(context.Background(), &got); err != nil {
		t.Fatalf("release raced identity reservation: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 1 || supervisor.drainReason != harnessv2.DrainReasonSessionIdentityCapacity {
		t.Fatalf("identity drain = calls:%d reason:%q, want one %q request", supervisor.drainCalls, supervisor.drainReason, harnessv2.DrainReasonSessionIdentityCapacity)
	}

	supervisor.probe.Status.Lifecycle = harnessv2.SupervisorLifecycleDraining
	supervisor.probe.Status.Drain = harnessv2.DrainStatus{
		AcceptingNewSessions: false,
		Requested:            true,
		RequestedAt:          runtimePoolTestNow.Add(-time.Minute),
		Reason:               harnessv2.DrainReasonSessionIdentityCapacity,
	}
	supervisor.probe.Status.Sessions = []harnessv2.RuntimeSessionStatus{{
		RuntimeSessionID:    "runtime-session-1",
		RuntimeSessionUID:   "session-uid-1",
		Generation:          1,
		State:               harnessv2.RuntimeSessionStateIdle,
		LastTransitionAt:    runtimePoolTestNow.Add(-time.Minute),
		LiveDescendantCount: 0,
	}}
	supervisor.probe.Status.Pressure.ResidentSessions = 1
	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining {
		t.Fatalf("lifecycle with resident session = %s, want Draining", got.Status.Lifecycle)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); err != nil {
		t.Fatalf("runtime Pod was deleted before quiescence: %v", err)
	}

	supervisor.probe.Status.Sessions = nil
	supervisor.probe.Status.Pressure.ResidentSessions = 0
	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("lifecycle at first quiescent observation = %s, want Quiescent", got.Status.Lifecycle)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); err != nil {
		t.Fatalf("runtime Pod was deleted before quiescence persisted: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	got = runtimePoolTestGetPool(t, r, pool)
	if got.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || got.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status after identity-limited Pod deletion = %s/%s, want Stopping/Closed", got.Status.Lifecycle, got.Status.AdmissionState)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("identity-limited runtime Pod still exists, err=%v", err)
	}
	if !deleteRecorder.podDeleteHadPreconditions || deleteRecorder.podDeleteUID != pod.UID || deleteRecorder.podDeleteResourceVersion == "" {
		t.Fatalf(
			"identity-limited Pod delete preconditions = present:%t uid:%q resourceVersion:%q, want exact UID %q and non-empty resourceVersion",
			deleteRecorder.podDeleteHadPreconditions,
			deleteRecorder.podDeleteUID,
			deleteRecorder.podDeleteResourceVersion,
			pod.UID,
		)
	}
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	if got := ptr.Deref(deployment.Spec.Replicas, 0); got != 1 {
		t.Fatalf("deployment replicas after Pod rotation = %d, want 1 for replacement", got)
	}
}

func runtimePoolTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1alpha1.AddToScheme,
		corev1.AddToScheme,
		appsv1.AddToScheme,
		networkingv1.AddToScheme,
		policyv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("AddToScheme: %v", err)
		}
	}
	return scheme
}

func runtimePoolTestObject(replicas int32) *corev1alpha1.RuntimePool {
	const namespace = "tenant-a"
	artifactDigest := "sha256:" + strings.Repeat("b", 64)
	harnessProfile := harnessv2.RuntimeProfile{
		ACPProfile:               harnessv2.ACPProfileV1,
		AdapterDigests:           map[string]string{"codex-acp": artifactDigest},
		ProviderKind:             runtimePoolProviderCodex,
		Model:                    acpTestModel,
		AgentConfigurationDigest: "sha256:" + strings.Repeat("c", 64),
		ToolPolicyDigest:         "sha256:" + strings.Repeat("d", 64),
		ApprovalPolicyDigest:     "sha256:" + strings.Repeat("e", 64),
		MCPConfigurationDigest:   "sha256:" + strings.Repeat("f", 64),
		WorkspaceIntent:          harnessv2.WorkspaceIntentRead,
		ProxyCredentialRole:      "provider-inference",
		ProxyCredentialScope:     "model:gpt-test",
		ResourceClass:            "standard",
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(harnessProfile)
	if err != nil {
		panic(err)
	}
	return &corev1alpha1.RuntimePool{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1alpha1.GroupVersion.String(), Kind: "RuntimePool"},
		ObjectMeta: metav1.ObjectMeta{
			Name: runtimePoolProviderCodex, Namespace: namespace, UID: types.UID(namespace + "-" + runtimePoolProviderCodex + "-uid"), Generation: 1,
			Labels: map[string]string{acpRuntimePoolLabel: "true"},
		},
		Spec: corev1alpha1.RuntimePoolSpec{
			TrustDomain: corev1alpha1.RuntimePoolTrustDomain{Namespace: namespace, Identity: namespace + "/default"},
			Runtime: corev1alpha1.RuntimePoolRuntimeSpec{
				Image: "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("a", 64),
				Profile: corev1alpha1.RuntimePoolProfileSpec{
					ProtocolVersion:          corev1alpha1.RuntimePoolProtocolHarnessV2,
					Digest:                   string(profileDigest),
					DigestSchemaVersion:      strconv.FormatUint(uint64(harnessv2.ProfileDigestSchemaVersion), 10),
					ACPProfile:               harnessProfile.ACPProfile,
					AdapterDigests:           harnessProfile.AdapterDigests,
					ProviderKind:             harnessProfile.ProviderKind,
					Model:                    harnessProfile.Model,
					AgentConfigurationDigest: harnessProfile.AgentConfigurationDigest,
					ToolPolicyDigest:         harnessProfile.ToolPolicyDigest,
					ApprovalPolicyDigest:     harnessProfile.ApprovalPolicyDigest,
					MCPConfigurationDigest:   harnessProfile.MCPConfigurationDigest,
					WorkspaceIntent:          corev1alpha1.WorkspaceIntent(harnessProfile.WorkspaceIntent),
					ProxyCredentialRole:      harnessProfile.ProxyCredentialRole,
					ProxyCredentialScope:     harnessProfile.ProxyCredentialScope,
					ResourceClass:            harnessProfile.ResourceClass,
				},
			},
			DesiredReplicas: replicas,
			Capacity: &corev1alpha1.RuntimePoolCapacitySpec{
				MaxResidentSessions: 10,
				MaxRunningPrompts:   4,
			},
			ColdStartTimeoutSeconds: 120,
		},
	}
}

func runtimePoolTestReconciler(
	t *testing.T,
	scheme *runtime.Scheme,
	supervisor RuntimePoolSupervisorClient,
	objects ...client.Object,
) *RuntimePoolReconciler {
	t.Helper()
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.RuntimePool{}, &corev1alpha1.Task{}, &appsv1.Deployment{}, &corev1.Pod{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, delegate client.WithWatch, object client.Object, opts ...client.CreateOption) error {
				if secret, ok := object.(*corev1.Secret); ok && secret.UID == "" {
					secret.UID = types.UID("test-uid-" + secret.Name)
				}
				return delegate.Create(ctx, object, opts...)
			},
		}).
		WithObjects(objects...).
		Build()
	r := &RuntimePoolReconciler{
		Client: cl, Scheme: scheme, ControllerEpoch: 7, SupervisorClient: supervisor,
		ControllerAPIURL: "http://orka-api.default.svc:8080", ControllerAPIPort: 8080,
		WorkspaceArtifactMaxBytes: 100 << 20,
		ProviderProxy: RuntimePoolProviderProxyConfig{
			BaseURL: "http://provider-auth-proxy.orka-system.svc:8080", Namespace: "orka-system",
			PodLabels:   map[string]string{"app.kubernetes.io/name": "orka", "app.kubernetes.io/component": "provider-auth-proxy"},
			BearerToken: bytes.Clone(runtimePoolTestProviderToken),
		},
		AgentSandboxEnabled: true,
		AllowedImages:       ACPRuntimeImages{Codex: "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("a", 64)},
		Rand:                &runtimePoolTestEntropyReader{}, Now: func() time.Time { return runtimePoolTestNow },
		WorkspaceCredentialSeeder: func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
			return false, nil
		},
	}
	return r
}

type runtimePoolTestEntropyReader struct {
	next byte
}

func (r *runtimePoolTestEntropyReader) Read(buffer []byte) (int, error) {
	r.next++
	for i := range buffer {
		buffer[i] = r.next
	}
	return len(buffer), nil
}

func runtimePoolTestStartServing(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
	supervisor *fakeRuntimePoolSupervisorClient,
	podName, podUID, podIP, bootID string,
) (*appsv1.Deployment, corev1.Pod) {
	t.Helper()
	runtimePoolReconcile(t, r, pool)
	deployment := runtimePoolTestDeployment(t, r, pool.Namespace, runtimePoolResourceName(pool.Namespace, pool.Name))
	pod := runtimePoolReadyPodForDeployment(pool, deployment, podName, podUID, podIP)
	runtimePoolTestCreatePod(t, r, &pod)
	supervisor.probe = runtimePoolValidProbe(pool, &pod, bootID, false)
	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("initial serving status = %s/%s, want Serving/Accepting", status.Lifecycle, status.AdmissionState)
	}
	return runtimePoolTestDeployment(t, r, pool.Namespace, deployment.Name), pod
}

func runtimePoolReadyPodForDeployment(
	pool *corev1alpha1.RuntimePool,
	deployment *appsv1.Deployment,
	name, uid, ip string,
) corev1.Pod {
	pod := runtimePoolReadyPod(pool, deployment.Namespace, name, uid, ip)
	pod.Labels = cloneStringMap(deployment.Spec.Template.Labels)
	pod.Annotations = cloneStringMap(deployment.Spec.Template.Annotations)
	return pod
}

func runtimePoolTestSetPodReady(t *testing.T, r *RuntimePoolReconciler, pod *corev1.Pod, ready bool) {
	t.Helper()
	current := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
		t.Fatalf("get runtime Pod: %v", err)
	}
	want := corev1.ConditionFalse
	if ready {
		want = corev1.ConditionTrue
	}
	for i := range current.Status.Conditions {
		if current.Status.Conditions[i].Type == corev1.PodReady {
			current.Status.Conditions[i].Status = want
			if err := r.Status().Update(context.Background(), current); err != nil {
				t.Fatalf("update runtime Pod readiness: %v", err)
			}
			return
		}
	}
	t.Fatal("runtime Pod has no Ready condition")
}

func runtimePoolTestCreatePod(t *testing.T, r *RuntimePoolReconciler, pod *corev1.Pod) {
	t.Helper()
	status := pod.Status.DeepCopy()
	pod.Status = corev1.PodStatus{}
	if err := r.Create(context.Background(), pod); err != nil {
		t.Fatalf("create runtime Pod: %v", err)
	}
	pod.Status = *status
	if err := r.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("update runtime Pod status: %v", err)
	}
}

func runtimePoolTestTokenFile(t *testing.T, token []byte) string {
	t.Helper()
	path := t.TempDir() + "/provider-token"
	runtimePoolTestWriteTokenFile(t, path, token)
	return path
}

func runtimePoolTestWriteTokenFile(t *testing.T, path string, token []byte) {
	t.Helper()
	if err := os.WriteFile(path, append(bytes.Clone(token), '\n'), 0o600); err != nil {
		t.Fatalf("write provider token file: %v", err)
	}
}

func runtimePoolTestRefreshProfileDigest(t *testing.T, pool *corev1alpha1.RuntimePool) {
	t.Helper()
	profile, err := runtimePoolHarnessProfile(pool.Spec.Runtime.Profile)
	if err != nil {
		t.Fatalf("build runtime profile: %v", err)
	}
	digest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		t.Fatalf("digest runtime profile: %v", err)
	}
	pool.Spec.Runtime.Profile.Digest = string(digest)
}

func runtimePoolTestAssertSecretsExist(t *testing.T, r *RuntimePoolReconciler, namespace, stage string, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &corev1.Secret{}); err != nil {
			t.Fatalf("Secret %q was deleted %s: %v", name, stage, err)
		}
	}
}

func assertRuntimePoolProviderSecret(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
	generation string,
	token []byte,
) {
	t.Helper()
	var secrets corev1.SecretList
	if err := r.List(context.Background(), &secrets, client.InNamespace(pool.Namespace), client.MatchingLabels{
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		t.Fatalf("list RuntimePool Secrets: %v", err)
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if !strings.Contains(secret.Name, "-g"+generation) {
			continue
		}
		if secret.Immutable == nil || !*secret.Immutable || !bytes.Equal(secret.Data[runtimePoolProviderTokenKey], token) {
			t.Fatalf("provider Secret %q is not immutable or does not contain the intended token", secret.Name)
		}
		return
	}
	t.Fatalf("provider Secret for generation %q not found", generation)
}

func runtimePoolReconcile(t *testing.T, r *RuntimePoolReconciler, pool *corev1alpha1.RuntimePool) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func runtimePoolTestFinalize(
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
) (ctrl.Result, bool, error) {
	current := &corev1alpha1.RuntimePool{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pool), current); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, nil
		}
		return ctrl.Result{}, false, err
	}
	result, err := r.finalizeRuntimePool(context.Background(), current)
	return result, false, err
}

func runtimePoolReadyPod(pool *corev1alpha1.RuntimePool, namespace, name, uid, ip string) corev1.Pod {
	pod := runtimePoolPendingPod(pool, namespace, name, uid)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = ip
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	return pod
}

func runtimePoolPendingPod(pool *corev1alpha1.RuntimePool, namespace, name, uid string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace, UID: types.UID(uid),
			Annotations: map[string]string{
				runtimePoolProfileAnnotation:                 pool.Spec.Runtime.Profile.Digest,
				runtimePoolProviderTokenGenerationAnnotation: runtimePoolProviderTokenGeneration(runtimePoolTestProviderToken),
			},
			Labels: map[string]string{
				runtimePoolKeyLabel:       runtimePoolKey(pool.Namespace, pool.Name),
				runtimePoolNameLabel:      pool.Name,
				runtimePoolNamespaceLabel: pool.Namespace,
				runtimePoolUIDLabel:       string(pool.UID),
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func runtimePoolValidProbe(pool *corev1alpha1.RuntimePool, pod *corev1.Pod, bootID string, draining bool) RuntimePoolProbeResult {
	limits := harnessv2.DefaultProtocolLimits()
	status := harnessv2.StatusResponse{
		Protocol: harnessv2.ProtocolVersion,
		Fence: harnessv2.Fence{
			RuntimeInstanceID:          harnessv2.RuntimeInstanceID(runtimePoolRuntimeInstanceID(pod.UID, harnessv2.SupervisorBootID(bootID))),
			SupervisorBootID:           harnessv2.SupervisorBootID(bootID),
			ControllerEpoch:            7,
			RuntimePoolUID:             harnessv2.RuntimePoolUID(pool.UID),
			RuntimePoolGeneration:      uint64(pool.Generation),
			RuntimeProfileDigest:       harnessv2.ProfileDigest(pool.Spec.Runtime.Profile.Digest),
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
		},
		Lifecycle: harnessv2.SupervisorLifecycleReady,
		Drain:     harnessv2.DrainStatus{AcceptingNewSessions: true},
		SessionIdentityCapacity: &harnessv2.SessionIdentityCapacity{
			Total: 10_000, Remaining: 9_999, ExhaustionReserve: 1,
		},
		Timestamp: runtimePoolTestNow,
	}
	if draining {
		status.Lifecycle = harnessv2.SupervisorLifecycleDraining
		status.Drain = harnessv2.DrainStatus{
			AcceptingNewSessions: false,
			Requested:            true,
			RequestedAt:          runtimePoolTestNow.Add(-time.Minute),
			Reason:               "runtime_pool_scale_to_zero",
		}
	}
	return RuntimePoolProbeResult{
		Capabilities: harnessv2.CapabilitiesResponse{
			Protocol: harnessv2.ProtocolVersion, Transport: "http+ndjson", ACPVersion: harnessv2.ACPProfileV1,
			RuntimeProfileDigest:       harnessv2.ProfileDigest(pool.Spec.Runtime.Profile.Digest),
			ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
			AdapterDigests:             map[string]string{"codex-acp": "sha256:" + strings.Repeat("b", 64)},
			Limits:                     limits,
			Provider: harnessv2.ProviderCapabilities{
				ProviderKinds: []string{runtimePoolProviderCodex}, Models: []string{acpTestModel}, SupportsPermissions: true, SupportsCancel: true, SupportsTools: true,
			},
			WorkspaceGovernance: harnessv2.StrictWorkspaceGovernanceCapabilities(),
			SupportsDrain:       true,
		},
		Status: status,
	}
}

func runtimePoolTestDeployment(t *testing.T, r *RuntimePoolReconciler, namespace, name string) *appsv1.Deployment {
	t.Helper()
	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &deployment); err != nil {
		t.Fatalf("Get Deployment: %v", err)
	}
	return &deployment
}

func runtimePoolTestGetPool(t *testing.T, r *RuntimePoolReconciler, pool *corev1alpha1.RuntimePool) corev1alpha1.RuntimePool {
	t.Helper()
	var got corev1alpha1.RuntimePool
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &got); err != nil {
		t.Fatalf("Get RuntimePool: %v", err)
	}
	return got
}

func runtimePoolTestVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func capabilityStrings(values []corev1.Capability) []string {
	result := make([]string, len(values))
	for i := range values {
		result[i] = string(values[i])
	}
	return result
}

func secretDataKeys(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	return keys
}

func reflectStringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
