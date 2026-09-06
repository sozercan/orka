/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	sandboxcontrollers "sigs.k8s.io/agent-sandbox/controllers"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxextcontrollers "sigs.k8s.io/agent-sandbox/extensions/controllers"
)

const (
	sandboxClaimProvisioningFailedReason = "ProvisioningFailed"
	sandboxClaimProviderExhaustedMessage = "provider capacity exhausted"
)

type failPrivateAuthFinalBindingPatchClient struct {
	client.Client
	failed             bool
	deletedPrivateAuth int
}

type commitPrivateAuthBindingThenErrorClient struct {
	client.Client
	failed             bool
	deletedPrivateAuth int
}

func (c *commitPrivateAuthBindingThenErrorClient) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.PatchOption,
) error {
	ambiguousBindingPatch := false
	if pool, ok := object.(*corev1alpha1.RuntimePool); ok && !c.failed {
		for key, value := range pool.Annotations {
			if strings.HasPrefix(key, runtimePoolPrivateAuthBindingPrefix) && strings.TrimSpace(value) != "" {
				ambiguousBindingPatch = true
				break
			}
		}
	}
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	if ambiguousBindingPatch {
		c.failed = true
		return errors.New("ambiguous private auth binding patch result")
	}
	return nil
}

func (c *commitPrivateAuthBindingThenErrorClient) Delete(
	ctx context.Context,
	object client.Object,
	options ...client.DeleteOption,
) error {
	if secret, ok := object.(*corev1.Secret); ok && secret.Labels[runtimePoolAuthLabel] == booleanTrueValue {
		c.deletedPrivateAuth++
	}
	return c.Client.Delete(ctx, object, options...)
}

func (c *failPrivateAuthFinalBindingPatchClient) Patch(
	ctx context.Context,
	object client.Object,
	patch client.Patch,
	options ...client.PatchOption,
) error {
	if pool, ok := object.(*corev1alpha1.RuntimePool); ok && !c.failed {
		for key, value := range pool.Annotations {
			value = strings.TrimSpace(value)
			if strings.HasPrefix(key, runtimePoolPrivateAuthBindingPrefix) && value != "" {
				c.failed = true
				return errors.New("transient private auth binding patch failure")
			}
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

func (c *failPrivateAuthFinalBindingPatchClient) Delete(
	ctx context.Context,
	object client.Object,
	options ...client.DeleteOption,
) error {
	if secret, ok := object.(*corev1.Secret); ok && secret.Labels[runtimePoolAuthLabel] == booleanTrueValue {
		c.deletedPrivateAuth++
	}
	return c.Client.Delete(ctx, object, options...)
}

func runtimePoolWorkspaceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtimePoolTestScheme(t)
	if err := sandboxv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme sandbox core: %v", err)
	}
	if err := sandboxextv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme sandbox extensions: %v", err)
	}
	return scheme
}

func runtimePoolWorkspaceTestObject() *corev1alpha1.RuntimePool {
	pool := runtimePoolTestObject(1)
	pool.Name = acpWorkspaceTestRuntimePoolName
	pool.Spec.ExecutionWorkspace = &corev1alpha1.RuntimePoolExecutionWorkspaceSpec{
		Provider:      corev1alpha1.WorkspaceProviderAgentSandbox,
		BindingDigest: "sha256:" + strings.Repeat("9", 64),
	}
	pool.Spec.Capacity = &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: 1, MaxRunningPrompts: 1}
	return pool
}

func runtimePoolTestPrivateAuthSecret(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
) corev1.Secret {
	t.Helper()
	var secrets corev1.SecretList
	if err := r.List(context.Background(), &secrets, client.InNamespace(pool.Namespace), client.MatchingLabels{
		runtimePoolAuthLabel: booleanTrueValue,
		runtimePoolUIDLabel:  string(pool.UID),
	}); err != nil {
		t.Fatalf("list private RuntimePool auth Secrets: %v", err)
	}
	if len(secrets.Items) != 1 {
		t.Fatalf("private RuntimePool auth Secret count = %d, want 1", len(secrets.Items))
	}
	return secrets.Items[0]
}

func runtimePoolWorkspaceTestChildren(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
) (*sandboxextv1beta1.SandboxTemplate, *sandboxextv1beta1.SandboxWarmPool, *sandboxextv1beta1.SandboxClaim) {
	t.Helper()
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	template := &sandboxextv1beta1.SandboxTemplate{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolSandboxTemplateName(base)}, template); err != nil {
		if apierrors.IsNotFound(err) {
			template = nil
		} else {
			t.Fatalf("Get SandboxTemplate: %v", err)
		}
	}
	warmPool := &sandboxextv1beta1.SandboxWarmPool{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolSandboxWarmPoolName(base)}, warmPool); err != nil {
		if apierrors.IsNotFound(err) {
			warmPool = nil
		} else {
			t.Fatalf("Get SandboxWarmPool: %v", err)
		}
	}
	claim := &sandboxextv1beta1.SandboxClaim{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolSandboxClaimName(base)}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			claim = nil
		} else {
			t.Fatalf("Get SandboxClaim: %v", err)
		}
	}
	return template, warmPool, claim
}

func runtimePoolWorkspaceReadyPod(
	pool *corev1alpha1.RuntimePool,
	template *sandboxextv1beta1.SandboxTemplate,
	name, uid, ip string,
) corev1.Pod {
	pod := runtimePoolReadyPod(pool, pool.Namespace, name, uid, ip)
	pod.Labels = cloneStringMap(template.Spec.PodTemplate.ObjectMeta.Labels)
	pod.Annotations = cloneStringMap(template.Spec.PodTemplate.ObjectMeta.Annotations)
	pod.Spec = *template.Spec.PodTemplate.Spec.DeepCopy()
	if pod.Spec.ServiceAccountName == "" {
		pod.Spec.ServiceAccountName = runtimePoolDefaultServiceAccountName
	}
	// Kubernetes mirrors serviceAccountName into the deprecated serviceAccount
	// alias when it converts a stored Pod back to v1.
	pod.Spec.DeprecatedServiceAccount = pod.Spec.ServiceAccountName
	return pod
}

func runtimePoolWorkspaceTestMaterialization(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
	template *sandboxextv1beta1.SandboxTemplate,
	ip string,
) corev1.Pod {
	t.Helper()
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	claim := &sandboxextv1beta1.SandboxClaim{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: runtimePoolSandboxClaimName(base)}, claim); err != nil {
		t.Fatalf("Get SandboxClaim for materialization: %v", err)
	}
	if claim.UID == "" {
		claim.UID = types.UID("sandbox-claim-uid")
		if err := r.Update(context.Background(), claim); err != nil {
			t.Fatalf("assign test SandboxClaim UID: %v", err)
		}
	}
	sandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claim.Name,
			Namespace: claim.Namespace,
			Labels: map[string]string{
				sandboxextv1beta1.SandboxIDLabel:           string(claim.UID),
				sandboxv1beta1.SandboxTemplateRefHashLabel: sandboxextcontrollers.SandboxTemplateRefHash(template.Name),
			},
			Annotations: map[string]string{
				sandboxv1beta1.SandboxTemplateRefAnnotation: template.Name,
			},
		},
		Spec: sandboxv1beta1.SandboxSpec{SandboxBlueprint: *template.Spec.SandboxBlueprint.DeepCopy()},
	}
	// The upstream claim controller merges the claim's volumeClaimTemplates
	// into the materialized Sandbox; mirror that so attestation is proven
	// against real provider behavior for suspend-capable pools.
	for i := range claim.Spec.VolumeClaimTemplates {
		sandbox.Spec.VolumeClaimTemplates = append(
			sandbox.Spec.VolumeClaimTemplates, *claim.Spec.VolumeClaimTemplates[i].DeepCopy(),
		)
	}
	sandboxextcontrollers.ApplySandboxSecureDefaults(template, &sandbox.Spec.PodTemplate.Spec)
	sandbox.Spec.PodTemplate.ObjectMeta.Labels = mergeStringMap(
		sandbox.Spec.PodTemplate.ObjectMeta.Labels,
		map[string]string{
			sandboxextv1beta1.SandboxIDLabel:           string(claim.UID),
			sandboxv1beta1.SandboxTemplateRefHashLabel: sandboxextcontrollers.SandboxTemplateRefHash(template.Name),
		},
	)
	if err := controllerutil.SetControllerReference(claim, sandbox, r.Scheme); err != nil {
		t.Fatalf("Set Sandbox owner reference: %v", err)
	}
	if err := r.Create(context.Background(), sandbox); err != nil {
		t.Fatalf("Create Sandbox materialization: %v", err)
	}

	pod := runtimePoolWorkspaceReadyPod(pool, template, "sandbox-pod", "sandbox-pod-uid", ip)
	pod.Labels = cloneStringMap(sandbox.Spec.PodTemplate.ObjectMeta.Labels)
	pod.Labels[sandboxcontrollers.SandboxNameHashLabel] = sandboxcontrollers.NameHash(sandbox.Name)
	if err := controllerutil.SetControllerReference(sandbox, &pod, r.Scheme); err != nil {
		t.Fatalf("Set runtime Pod Sandbox owner reference: %v", err)
	}
	runtimePoolTestCreatePod(t, r, &pod)
	return pod
}

func assertWorkspaceRuntimePoolBootstrapEnvironment(
	t *testing.T,
	r *RuntimePoolReconciler,
	pool *corev1alpha1.RuntimePool,
	env []corev1.EnvVar,
) {
	t.Helper()
	byName := make(map[string]corev1.EnvVar, len(env))
	for i := range env {
		byName[env[i].Name] = env[i]
	}
	for _, name := range []string{
		runtimePoolControllerTokenFileEnv, runtimePoolCapabilitySecretFileEnv, runtimePoolProviderTokenFileEnv,
		"ORKA_ACP_CONTROLLER_TOKEN_BOOTSTRAP", "ORKA_ACP_CAPABILITY_SECRET_BOOTSTRAP", "ORKA_ACP_PROVIDER_TOKEN_BOOTSTRAP",
	} {
		if _, found := byName[name]; found {
			t.Fatalf("provider-visible workspace template exposes credential environment %s", name)
		}
	}
	if strings.TrimSpace(byName["ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"].Value) == "" ||
		strings.TrimSpace(byName[harnessv2.CredentialBootstrapPublicKeyEnv].Value) == "" {
		t.Fatal("provider-visible workspace template is missing signed credential-bootstrap material")
	}
	authSecret := runtimePoolTestPrivateAuthSecret(t, r, pool)
	if bytes.Equal(authSecret.Data[runtimePoolCapabilitySecretKey], authSecret.Data[runtimePoolBootstrapSigningSeedKey]) {
		t.Fatal("bootstrap signing seed is not separated from the delivered capability secret")
	}
	wantPublicKey, err := harnessv2.CredentialBootstrapPublicKey(authSecret.Data[runtimePoolBootstrapSigningSeedKey])
	if err != nil {
		t.Fatalf("derive expected workspace bootstrap public key: %v", err)
	}
	publicKey := byName[harnessv2.CredentialBootstrapPublicKeyEnv].Value
	if publicKey != wantPublicKey {
		t.Fatal("workspace template bootstrap public key is not bound to the controller-only signing seed")
	}
	capabilitySignature, err := harnessv2.SignCredentialBootstrap(
		authSecret.Data[runtimePoolCapabilitySecretKey],
		byName["ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"].Value,
		[]byte("credential-bootstrap-payload"),
	)
	if err != nil {
		t.Fatalf("sign bootstrap proof with delivered capability secret: %v", err)
	}
	if err := harnessv2.VerifyCredentialBootstrap(
		publicKey,
		byName["ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"].Value,
		[]byte("credential-bootstrap-payload"),
		capabilitySignature,
	); err == nil {
		t.Fatal("delivered capability secret authorized a credential-bootstrap payload")
	}
	baseEnvironment := append([]corev1.EnvVar(nil), env...)
	baseEnvironment = append(baseEnvironment, corev1.EnvVar{Name: "ORKA_ACP_PROVIDER_TOKEN_FILE", Value: runtimePoolProviderTokenPath})
	assertRuntimePoolEnvironment(t, r, pool, baseEnvironment)
}

func TestHistoricalRuntimePoolImageRecoveryRejectsOwnedSandboxTemplateWithoutControllerProvenance(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	cfg, err := r.runtimePoolConfigForDrain(pool)
	if err != nil {
		t.Fatal(err)
	}
	selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
	deployed := r.runtimePoolPodTemplate(pool, cfg, selector, "legacy-auth", "legacy-provider")
	deployed = runtimePoolWorkspaceBootstrapTemplate(deployed, "legacy-nonce", "legacy-public-key")
	if err := r.createRuntimePoolSandboxTemplate(context.Background(), pool, cfg, deployed); err != nil {
		t.Fatal(err)
	}

	authorized, err := r.historicalRuntimePoolImageAuthorized(context.Background(), pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if authorized {
		t.Fatal("caller-constructible SandboxTemplate authorized the historical workspace image")
	}
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               acpRuntimePoolImageProvenanceCondition,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pool.Generation,
		Reason:             acpRuntimePoolImageProvenanceReason,
	})
	authorized, err = r.historicalRuntimePoolImageAuthorized(context.Background(), pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("controller-written provenance did not authorize the historical workspace image")
	}
}

func TestWorkspaceRuntimePoolMaterializesProviderWorkload(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)

	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	template, warmPool, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || warmPool == nil || claim == nil {
		t.Fatalf("workspace children template/warmPool/claim = %v/%v/%v, want all present", template != nil, warmPool != nil, claim != nil)
	}
	if template.Spec.NetworkPolicyManagement != sandboxextv1beta1.NetworkPolicyManagementUnmanaged {
		t.Fatalf("sandbox template network policy management = %q, want Unmanaged (the pool NetworkPolicies own the boundary)", template.Spec.NetworkPolicyManagement)
	}
	if template.Spec.EnvVarsInjectionPolicy != sandboxextv1beta1.EnvVarsInjectionPolicyDisallowed ||
		template.Spec.VolumeClaimTemplatesPolicy != sandboxextv1beta1.VolumeClaimTemplatesPolicyDisallowed {
		t.Fatalf("sandbox template injection policies = %q/%q, want Disallowed/Disallowed", template.Spec.EnvVarsInjectionPolicy, template.Spec.VolumeClaimTemplatesPolicy)
	}
	podSpec := template.Spec.PodTemplate.Spec
	assertRuntimePoolPodHardening(t, podSpec)
	assertRuntimePoolContainerHardening(t, podSpec.Containers[0])
	assertRuntimePoolBoundedVolumes(t, podSpec.Volumes)
	assertWorkspaceRuntimePoolBootstrapEnvironment(t, r, pool, podSpec.Containers[0].Env)
	for _, name := range []string{runtimePoolAuthVolume, runtimePoolProviderCapabilityVolume} {
		if runtimePoolTestVolume(podSpec.Volumes, name) != nil {
			t.Fatalf("provider-visible workspace template exposes credential volume %q", name)
		}
		for i := range podSpec.Containers[0].VolumeMounts {
			if podSpec.Containers[0].VolumeMounts[i].Name == name {
				t.Fatalf("provider-visible workspace template exposes credential mount %q", name)
			}
		}
	}
	if template.Spec.PodTemplate.ObjectMeta.Labels[runtimePoolKeyLabel] != runtimePoolKey(pool.Namespace, pool.Name) {
		t.Fatal("sandbox template Pod labels do not carry the pool selector label")
	}
	if strings.TrimSpace(template.Spec.PodTemplate.ObjectMeta.Annotations[runtimePoolTemplateRevisionAnnotation]) == "" {
		t.Fatal("sandbox template Pod annotations do not carry the template revision")
	}
	if template.Spec.PodTemplate.ObjectMeta.Annotations[runtimePoolProfileAnnotation] != pool.Spec.Runtime.Profile.Digest {
		t.Fatal("sandbox template Pod annotations do not carry the immutable profile digest")
	}

	if got := ptr.Deref(warmPool.Spec.Replicas, -1); got != 0 {
		t.Fatalf("sandbox warm pool replicas = %d, want 0 (claims always cold-start from the exact template)", got)
	}
	if warmPool.Spec.TemplateRef.Name != runtimePoolSandboxTemplateName(base) {
		t.Fatalf("sandbox warm pool templateRef = %q, want %q", warmPool.Spec.TemplateRef.Name, runtimePoolSandboxTemplateName(base))
	}
	if claim.Spec.WarmPoolRef.Name != runtimePoolSandboxWarmPoolName(base) {
		t.Fatalf("sandbox claim warmPoolRef = %q, want %q", claim.Spec.WarmPoolRef.Name, runtimePoolSandboxWarmPoolName(base))
	}
	if len(claim.Spec.Env) != 0 || len(claim.Spec.VolumeClaimTemplates) != 0 {
		t.Fatal("sandbox claim must not inject env or volumes; credentials never cross the provider API")
	}
	serializedTemplate, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("Marshal SandboxTemplate: %v", err)
	}
	var secrets corev1.SecretList
	if err := r.List(context.Background(), &secrets, client.InNamespace(pool.Namespace), client.MatchingLabels{runtimePoolUIDLabel: string(pool.UID)}); err != nil {
		t.Fatalf("List workspace RuntimePool Secrets: %v", err)
	}
	privateName := regexp.MustCompile(`-[0-9a-f]{24}$`)
	credentialSecrets := 0
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if secret.Labels[runtimePoolAuthLabel] != booleanTrueValue && secret.Labels[runtimePoolProviderCredentialLabel] != booleanTrueValue {
			continue
		}
		credentialSecrets++
		if !privateName.MatchString(secret.Name) {
			t.Fatalf("workspace credential Secret %q does not have an unpredictable private suffix", secret.Name)
		}
		if bytes.Contains(serializedTemplate, []byte(secret.Name)) {
			t.Fatalf("provider-visible SandboxTemplate exposes private credential Secret %q", secret.Name)
		}
	}
	if credentialSecrets != 2 {
		t.Fatalf("workspace credential Secret count = %d, want auth and provider Secrets", credentialSecrets)
	}

	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: base}, &deployment); !apierrors.IsNotFound(err) {
		t.Fatalf("workspace-backed pool created a Deployment (err=%v); the provider owns the workload", err)
	}

	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStarting || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status = %s/%s, want Starting/Closed", status.Lifecycle, status.AdmissionState)
	}
}

func TestWorkspaceRuntimePoolIgnoresForeignReadyPodWithSamePoolKey(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil {
		t.Fatal("workspace template was not materialized")
	}
	foreign := runtimePoolWorkspaceReadyPod(pool, template, "foreign-pod", "foreign-pod-uid", "10.0.0.90")
	foreign.Labels[runtimePoolUIDLabel] = "previous-pool-uid"
	runtimePoolTestCreatePod(t, r, &foreign)

	legitimate := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.91")
	supervisor.probe = runtimePoolValidProbe(pool, &legitimate, "workspace-boot", false)
	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting ||
		status.ActiveInstance == nil || status.ActiveInstance.PodUID != string(legitimate.UID) {
		t.Fatalf("workspace status with foreign Pod = %s/%s active=%#v, want legitimate provider Pod serving", status.Lifecycle, status.AdmissionState, status.ActiveInstance)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&foreign), &corev1.Pod{}); err != nil {
		t.Fatalf("foreign Pod should be ignored rather than managed: %v", err)
	}
}

func TestWorkspaceRuntimePoolRequeuesAfterCreatingClaimBeforeUsingReadyPod(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace template and claim were not materialized")
	}
	stale := runtimePoolWorkspaceReadyPod(pool, template, "stale-pod", "stale-pod-uid", "10.0.0.92")
	runtimePoolTestCreatePod(t, r, &stale)
	if err := r.Delete(context.Background(), claim); err != nil {
		t.Fatalf("delete old SandboxClaim: %v", err)
	}

	result := runtimePoolReconcile(t, r, pool)
	if result.RequeueAfter == 0 {
		t.Fatal("new SandboxClaim creation did not requeue for a fresh claim and Pod read")
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("stale pre-claim Pod triggered %d authenticated probes", supervisor.probeCalls)
	}
	if _, _, replacement := runtimePoolWorkspaceTestChildren(t, r, pool); replacement == nil {
		t.Fatal("replacement SandboxClaim was not created")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStarting ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		status.ActiveInstance != nil {
		t.Fatalf("replacement claim status = %s/%s active=%#v, want Starting/Closed with no active instance", status.Lifecycle, status.AdmissionState, status.ActiveInstance)
	}
}

func TestWorkspaceRuntimePoolRejectsUnboundPrivateAuthSecret(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	immutable := true
	forged := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "attacker-selected-auth",
			Namespace: pool.Namespace,
			Labels: map[string]string{
				runtimePoolAuthLabel:            booleanTrueValue,
				runtimePoolUIDLabel:             string(pool.UID),
				runtimePoolCredentialEpochLabel: "7",
			},
		},
		Immutable: &immutable,
		Data: map[string][]byte{
			runtimePoolControllerTokenKey:      bytes.Repeat([]byte("a"), 32),
			runtimePoolCapabilitySecretKey:     bytes.Repeat([]byte("b"), 32),
			runtimePoolBootstrapNonceKey:       bytes.Repeat([]byte("c"), 32),
			runtimePoolBootstrapSigningSeedKey: bytes.Repeat([]byte("d"), 32),
		},
	}
	r := runtimePoolTestReconciler(t, scheme, nil, pool, forged)

	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "refusing to adopt an unbound private RuntimePool auth Secret") {
		t.Fatalf("status = %s/%s %q, want Degraded/Closed unbound private auth Secret rejection", status.Lifecycle, status.AdmissionState, status.Message)
	}
	current := &corev1.Secret{}
	if getErr := r.Get(context.Background(), client.ObjectKeyFromObject(forged), current); getErr != nil {
		t.Fatalf("Get forged Secret: %v", getErr)
	}
	if metav1.IsControlledBy(current, pool) {
		t.Fatal("controller adopted the forged private auth Secret")
	}
}

func TestWorkspaceRuntimePoolDiscardsUnboundPrivateAuthSecretAfterBindingPatchFailure(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	cfg, err := r.runtimePoolConfig(&current)
	if err != nil {
		t.Fatalf("runtimePoolConfig() error = %v", err)
	}
	failingClient := &failPrivateAuthFinalBindingPatchClient{Client: r.Client}
	r.Client = failingClient

	if _, err := r.ensurePrivateWorkspaceRuntimePoolAuthSecret(context.Background(), &current, cfg, "7"); err == nil ||
		!strings.Contains(err.Error(), "transient private auth binding patch failure") {
		t.Fatalf("initial private auth Secret ensure error = %v, want transient binding failure", err)
	}
	if !failingClient.failed {
		t.Fatal("final private auth Secret binding patch was not intercepted")
	}

	bindingKey := runtimePoolPrivateAuthSecretBindingAnnotation(cfg.controllerEpoch)
	persisted := runtimePoolTestGetPool(t, r, pool)
	if binding := strings.TrimSpace(persisted.Annotations[bindingKey]); binding != "" {
		t.Fatalf("private auth Secret name was published before its UID was bound: %q", binding)
	}
	var secrets corev1.SecretList
	if err := r.List(context.Background(), &secrets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolAuthLabel: booleanTrueValue, runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		t.Fatalf("list unbound private auth Secrets: %v", err)
	}
	matches := runtimePoolAuthSecretsForEpoch(secrets.Items, cfg.controllerEpoch)
	if len(matches) != 1 {
		t.Fatalf("unbound private auth Secrets = %#v, want exactly one crash orphan", matches)
	}

	recovered, err := r.ensurePrivateWorkspaceRuntimePoolAuthSecret(context.Background(), &persisted, cfg, "7")
	if err != nil {
		t.Fatalf("replace unbound private auth Secret: %v", err)
	}
	if failingClient.deletedPrivateAuth != 1 {
		t.Fatalf("discarded private auth Secrets = %d, want the crash orphan deleted once", failingClient.deletedPrivateAuth)
	}
	if recovered.UID == "" {
		t.Fatalf("replacement private auth Secret = %s/%s, want immutable UID", recovered.Name, recovered.UID)
	}
	boundPool := runtimePoolTestGetPool(t, r, pool)
	boundName, boundUID, err := parseRuntimePoolPrivateSecretBinding(boundPool.Annotations[bindingKey])
	if err != nil || boundName != recovered.Name || boundUID != recovered.UID {
		t.Fatalf("final private auth binding = %q, parsed %s/%s, error=%v", boundPool.Annotations[bindingKey], boundName, boundUID, err)
	}
	secrets = corev1.SecretList{}
	if err := r.List(context.Background(), &secrets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolAuthLabel: booleanTrueValue, runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		t.Fatalf("list recovered private auth Secrets: %v", err)
	}
	if matches := runtimePoolAuthSecretsForEpoch(secrets.Items, cfg.controllerEpoch); len(matches) != 1 {
		t.Fatalf("private auth Secret count after recovery = %d, want one", len(matches))
	}
}

func TestWorkspaceRuntimePoolPreservesAuthoritativelyBoundAuthSecretAfterAmbiguousPatch(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	stale := current.DeepCopy()
	cfg, err := r.runtimePoolConfig(&current)
	if err != nil {
		t.Fatalf("runtimePoolConfig() error = %v", err)
	}
	authoritativeClient := r.Client
	ambiguousClient := &commitPrivateAuthBindingThenErrorClient{Client: authoritativeClient}
	r.Client = ambiguousClient
	r.APIReader = authoritativeClient

	if _, err := r.ensurePrivateWorkspaceRuntimePoolAuthSecret(context.Background(), &current, cfg, "7"); err == nil ||
		!strings.Contains(err.Error(), "ambiguous private auth binding patch result") {
		t.Fatalf("initial private auth Secret ensure error = %v, want ambiguous committed patch result", err)
	}
	if !ambiguousClient.failed {
		t.Fatal("private auth binding patch did not commit before the simulated transport error")
	}

	boundPool := runtimePoolTestGetPool(t, r, pool)
	bindingKey := runtimePoolPrivateAuthSecretBindingAnnotation(cfg.controllerEpoch)
	boundName, boundUID, err := parseRuntimePoolPrivateSecretBinding(boundPool.Annotations[bindingKey])
	if err != nil {
		t.Fatalf("authoritative private auth binding = %q, error=%v", boundPool.Annotations[bindingKey], err)
	}
	recovered, err := r.ensurePrivateWorkspaceRuntimePoolAuthSecret(context.Background(), stale, cfg, "7")
	if err != nil {
		t.Fatalf("recover authoritatively bound private auth Secret from stale pool: %v", err)
	}
	if recovered.Name != boundName || recovered.UID != boundUID {
		t.Fatalf("recovered private auth Secret = %s/%s, want bound %s/%s", recovered.Name, recovered.UID, boundName, boundUID)
	}
	if ambiguousClient.deletedPrivateAuth != 0 {
		t.Fatalf("deleted authoritatively bound private auth Secrets = %d, want 0", ambiguousClient.deletedPrivateAuth)
	}
}

func TestWorkspaceRuntimePoolRejectsRecreatedPrivateAuthSecret(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, nil, pool)
	runtimePoolReconcile(t, r, pool)

	currentPool := runtimePoolTestGetPool(t, r, pool)
	binding := currentPool.Annotations[runtimePoolPrivateAuthSecretBindingAnnotation(7)]
	name, uid, err := parseRuntimePoolPrivateSecretBinding(binding)
	if err != nil {
		t.Fatalf("parse private auth binding: %v", err)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: pool.Namespace, Name: name}
	if err := r.Get(context.Background(), key, secret); err != nil {
		t.Fatalf("Get bound private auth Secret: %v", err)
	}
	if secret.UID != uid {
		t.Fatalf("bound Secret UID = %q, want %q", secret.UID, uid)
	}
	replacement := secret.DeepCopy()
	if err := r.Delete(context.Background(), secret); err != nil {
		t.Fatalf("Delete bound private auth Secret: %v", err)
	}
	replacement.ResourceVersion = ""
	replacement.UID = "attacker-replacement-uid"
	if err := r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("Create replacement private auth Secret: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "auth Secret UID changed") {
		t.Fatalf("status = %s/%s %q, want Degraded/Closed bound private auth Secret UID rejection", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolRejectsUnownedProviderChildren(t *testing.T) {
	tests := []struct {
		name   string
		object func() client.Object
	}{
		{name: "template", object: func() client.Object { return &sandboxextv1beta1.SandboxTemplate{} }},
		{name: "warm pool", object: func() client.Object { return &sandboxextv1beta1.SandboxWarmPool{} }},
		{name: "claim", object: func() client.Object { return &sandboxextv1beta1.SandboxClaim{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtimePoolWorkspaceTestScheme(t)
			pool := runtimePoolWorkspaceTestObject()
			base := runtimePoolResourceName(pool.Namespace, pool.Name)
			object := tt.object()
			object.SetNamespace(pool.Namespace)
			switch object.(type) {
			case *sandboxextv1beta1.SandboxTemplate:
				object.SetName(runtimePoolSandboxTemplateName(base))
			case *sandboxextv1beta1.SandboxWarmPool:
				object.SetName(runtimePoolSandboxWarmPoolName(base))
			case *sandboxextv1beta1.SandboxClaim:
				object.SetName(runtimePoolSandboxClaimName(base))
			}
			r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool, object)

			runtimePoolReconcile(t, r, pool)

			status := runtimePoolTestGetPool(t, r, pool).Status
			if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
				status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
				!strings.Contains(status.Message, "ownership identity") {
				t.Fatalf("unowned %s status = %s/%s %q, want Degraded/Closed ownership failure", tt.name, status.Lifecycle, status.AdmissionState, status.Message)
			}
		})
	}
}

func TestWorkspaceRuntimePoolScaleDownIgnoresSandboxTemplateOwnershipDrift(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.70")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-template-drift", false)
	runtimePoolReconcile(t, r, pool)

	delete(template.Labels, runtimePoolUIDLabel)
	if err := r.Update(context.Background(), template); err != nil {
		t.Fatalf("drift SandboxTemplate ownership: %v", err)
	}
	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 0
	current.Generation++
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("scale ownership-drifted workspace to zero: %v", err)
	}

	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls after SandboxTemplate ownership drift = %d, status %s/%s %q, want 1", supervisor.drainCalls, status.Lifecycle, status.AdmissionState, status.Message)
	}
	if _, _, claim = runtimePoolWorkspaceTestChildren(t, r, pool); claim == nil {
		t.Fatal("claim was deleted before authenticated drain quiescence")
	}
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining ||
		strings.Contains(status.Message, "ownership identity") {
		t.Fatalf("ownership-drifted scale-down status = %s/%s %q, want authenticated Draining", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolRecyclesClaimBeforeRepairingTamperedTemplate(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	template.Spec.PodTemplate.Spec.Containers[0].Image = runtimePoolTestTamperedImage
	if err := r.Update(context.Background(), template); err != nil {
		t.Fatalf("tamper SandboxTemplate: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	if supervisor.probeCalls != 0 {
		t.Fatalf("tampered template triggered %d authenticated probes before claim recycling", supervisor.probeCalls)
	}
	if _, _, claim = runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("tampered SandboxTemplate claim was not recycled")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("tampered template status = %s/%s, want Stopping/Closed", status.Lifecycle, status.AdmissionState)
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim = runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("controller did not repair the template and reacquire a claim")
	}
	if got := template.Spec.PodTemplate.Spec.Containers[0].Image; got == runtimePoolTestTamperedImage {
		t.Fatal("controller retained the tampered runtime image")
	}
}

func TestWorkspaceRuntimePoolColdStartTimeout(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	pool.Spec.ColdStartTimeoutSeconds = 5
	now := runtimePoolTestNow
	r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)
	r.Now = func() time.Time { return now }

	runtimePoolReconcile(t, r, pool)
	now = now.Add(6 * time.Second)
	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		condition == nil || condition.Reason != runtimePoolRolloutReasonTimedOut {
		t.Fatalf("cold-start status/condition = %s/%#v, want Degraded/RolloutTimedOut", status.Lifecycle, condition)
	}
}

func TestWorkspaceRuntimePoolPreservesExplicitClaimFailure(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

	runtimePoolReconcile(t, r, pool)
	_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if claim == nil {
		t.Fatal("workspace claim was not materialized")
	}
	claim.Status.Conditions = []metav1.Condition{{
		Type:    string(sandboxv1beta1.SandboxConditionReady),
		Status:  metav1.ConditionFalse,
		Reason:  sandboxClaimProvisioningFailedReason,
		Message: sandboxClaimProviderExhaustedMessage,
	}}
	if err := r.Update(context.Background(), claim); err != nil {
		t.Fatalf("update failed SandboxClaim: %v", err)
	}

	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != corev1alpha1.RuntimePoolReasonRolloutFailed ||
		!strings.Contains(status.Message, sandboxClaimProviderExhaustedMessage) {
		t.Fatalf("claim failure status/condition = %s/%s %q/%#v, want Degraded/Closed explicit RolloutFailed", status.Lifecycle, status.AdmissionState, status.Message, condition)
	}
}

func TestWorkspaceRuntimePoolScaleToZeroCleansUpTerminallyFailedClaim(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	r := runtimePoolTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

	runtimePoolReconcile(t, r, pool)
	_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if claim == nil {
		t.Fatal("workspace claim was not materialized")
	}
	claim.Status.Conditions = []metav1.Condition{{
		Type:    string(sandboxv1beta1.SandboxConditionReady),
		Status:  metav1.ConditionFalse,
		Reason:  sandboxClaimProvisioningFailedReason,
		Message: sandboxClaimProviderExhaustedMessage,
	}}
	if err := r.Update(context.Background(), claim); err != nil {
		t.Fatalf("update failed SandboxClaim: %v", err)
	}
	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 0
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("scale failed workspace pool to zero: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	if _, _, claim = runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("terminally failed SandboxClaim blocked scale-to-zero cleanup")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle == corev1alpha1.RuntimePoolLifecycleDegraded && strings.Contains(status.Message, sandboxClaimProviderExhaustedMessage) {
		t.Fatalf("scale-to-zero remained stuck on terminal claim failure: %s/%q", status.Lifecycle, status.Message)
	}
}

func TestWorkspaceRuntimePoolFinalizationPreservesIsolationUntilClaimGone(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	pool.Finalizers = []string{runtimePoolFinalizer}
	deletedAt := metav1.NewTime(runtimePoolTestNow)
	pool.DeletionTimestamp = &deletedAt
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	labels := map[string]string{
		runtimePoolManagedByLabel:   runtimePoolManagedByLabelValue,
		runtimePoolApplicationLabel: runtimePoolApplicationLabelValue,
		runtimePoolKeyLabel:         runtimePoolKey(pool.Namespace, pool.Name),
		runtimePoolNameLabel:        pool.Name,
		runtimePoolNamespaceLabel:   pool.Namespace,
		runtimePoolUIDLabel:         string(pool.UID),
	}
	claim := &sandboxextv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{
		Name:       runtimePoolSandboxClaimName(base),
		Namespace:  pool.Namespace,
		Labels:     cloneStringMap(labels),
		Finalizers: []string{"test.orka.ai/hold-provider-cleanup"},
	}}
	if err := controllerutil.SetControllerReference(pool, claim, scheme); err != nil {
		t.Fatalf("set claim owner reference: %v", err)
	}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
		Name: runtimePoolChildName(base, "deny-all"), Namespace: pool.Namespace, Labels: labels,
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "pool-auth", Namespace: pool.Namespace, Labels: labels,
	}}
	r := runtimePoolTestReconciler(t, scheme, nil, pool, claim, policy, secret)

	result, gone, err := runtimePoolTestFinalize(r, pool)
	if err != nil {
		t.Fatalf("finalize workspace RuntimePool: %v", err)
	}
	if gone {
		t.Fatal("RuntimePool disappeared before provider claim cleanup completed")
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("finalization result = %#v, want requeue while provider claim remains", result)
	}
	currentClaim := &sandboxextv1beta1.SandboxClaim{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(claim), currentClaim); err != nil {
		t.Fatalf("get terminating SandboxClaim: %v", err)
	}
	if currentClaim.DeletionTimestamp.IsZero() {
		t.Fatal("provider SandboxClaim deletion was not initiated")
	}
	for _, object := range []client.Object{policy, secret} {
		check := object.DeepCopyObject().(client.Object)
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(object), check); err != nil {
			t.Fatalf("isolation child %T was removed before provider teardown completed: %v", object, err)
		}
	}
	currentPool := runtimePoolTestGetPool(t, r, pool)
	if !controllerutil.ContainsFinalizer(&currentPool, runtimePoolFinalizer) {
		t.Fatal("RuntimePool finalizer was removed before provider teardown completed")
	}
}

func TestWorkspaceRuntimePoolServesThroughSandboxPod(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.71")
	_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod.Annotations[sandboxv1beta1.SandboxPropagatedLabelsAnnotation] = "provider-bookkeeping"
	pod.Annotations[sandboxv1beta1.SandboxPropagatedAnnotationsAnnotation] = "provider-bookkeeping"
	if err := r.Update(context.Background(), &pod); err != nil {
		t.Fatalf("add provider-managed Pod annotations: %v", err)
	}
	if claim == nil {
		t.Fatal("materialized SandboxClaim is missing")
	}
	if pod.Labels[sandboxextv1beta1.SandboxIDLabel] != string(claim.UID) {
		t.Fatalf("materialized Pod claim UID label = %q, want %q", pod.Labels[sandboxextv1beta1.SandboxIDLabel], claim.UID)
	}
	if pod.Spec.DeprecatedServiceAccount != runtimePoolDefaultServiceAccountName {
		t.Fatalf("materialized Pod serviceAccount alias = %q, want Kubernetes default %q", pod.Spec.DeprecatedServiceAccount, runtimePoolDefaultServiceAccountName)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionAccepting {
		t.Fatalf("status = %s/%s, want Serving/Accepting", status.Lifecycle, status.AdmissionState)
	}
	active := status.ActiveInstance
	if active == nil || active.PodName != "sandbox-pod" || active.PodUID != "sandbox-pod-uid" ||
		active.PodAddress != "10.0.0.71" || active.BootID != "sandbox-boot" ||
		active.RuntimeInstanceID != runtimePoolRuntimeInstanceID(pod.UID, "sandbox-boot") {
		t.Fatalf("ActiveInstance = %#v, want the exact sandbox Pod identity", active)
	}
	if status.Capacity.MaxResidentSessions != 1 || status.Capacity.MaxRunningPrompts != 1 {
		t.Fatalf("capacity = %d/%d, want 1/1 single-session workspace pool", status.Capacity.MaxResidentSessions, status.Capacity.MaxRunningPrompts)
	}
	if seedCalls != 1 {
		t.Fatalf("credential bootstrap calls = %d, want 1 after exact Sandbox attestation", seedCalls)
	}
}

func TestWorkspaceRuntimePoolRejectsForgedSandboxClaimIdentityBeforeCredentialSeed(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.78")
	sandbox := &sandboxv1beta1.Sandbox{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, sandbox); err != nil {
		t.Fatalf("get Sandbox materialization: %v", err)
	}
	sandbox.Labels[sandboxextv1beta1.SandboxIDLabel] = "foreign-claim-uid"
	if err := r.Update(context.Background(), sandbox); err != nil {
		t.Fatalf("forge Sandbox claim identity: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 0 || supervisor.probeCalls != 0 {
		t.Fatalf("forged Sandbox received credential activity: seeds=%d probes=%d", seedCalls, supervisor.probeCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("forged SandboxClaim identity was not recycled")
	}
}

func TestWorkspaceRuntimePoolRejectsRacedSandboxMaterializationBeforeCredentialSeed(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.74")
	sandbox := &sandboxv1beta1.Sandbox{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, sandbox); err != nil {
		t.Fatalf("Get Sandbox materialization: %v", err)
	}
	sandbox.Spec.PodTemplate.Spec.Containers[0].Image = runtimePoolTestTamperedImage
	if err := r.Update(context.Background(), sandbox); err != nil {
		t.Fatalf("tamper materialized Sandbox: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 0 {
		t.Fatalf("raced Sandbox received %d credential bootstrap requests before attestation", seedCalls)
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("raced Sandbox received %d authenticated probes before attestation", supervisor.probeCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("raced SandboxClaim was not recycled")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "materialization") {
		t.Fatalf("raced materialization status = %s/%s %q, want Degraded/Closed attestation failure", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolRejectsForgedPodBeforeCredentialSeed(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.75")
	pod.Spec.Containers[0].Image = runtimePoolTestTamperedImage
	if err := r.Update(context.Background(), &pod); err != nil {
		t.Fatalf("tamper materialized Pod: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 0 {
		t.Fatalf("forged Pod received %d credential bootstrap requests before attestation", seedCalls)
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("forged Pod received %d authenticated probes before attestation", supervisor.probeCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("forged Pod SandboxClaim was not recycled")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "PodSpec") {
		t.Fatalf("forged Pod status = %s/%s %q, want Degraded/Closed PodSpec attestation failure", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolRejectsForgedPodLabelsBeforeCredentialSeed(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.76")
	pod.Labels["attacker.example/allow-egress"] = booleanTrueValue
	if err := r.Update(context.Background(), &pod); err != nil {
		t.Fatalf("tamper materialized Pod labels: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 0 {
		t.Fatalf("forged Pod labels received %d credential bootstrap requests before attestation", seedCalls)
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("forged Pod labels received %d authenticated probes before attestation", supervisor.probeCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("forged Pod-label SandboxClaim was not recycled")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "Pod labels") {
		t.Fatalf("forged Pod-label status = %s/%s %q, want Degraded/Closed label attestation failure", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolRejectsForgedPodAnnotationsBeforeCredentialSeed(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.79")
	pod.Annotations["k8s.v1.cni.cncf.io/networks"] = "attacker-network"
	if err := r.Update(context.Background(), &pod); err != nil {
		t.Fatalf("tamper materialized Pod annotations: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "sandbox-boot", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 0 {
		t.Fatalf("forged Pod annotations received %d credential bootstrap requests before attestation", seedCalls)
	}
	if supervisor.probeCalls != 0 {
		t.Fatalf("forged Pod annotations received %d authenticated probes before attestation", supervisor.probeCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("forged Pod-annotation SandboxClaim was not recycled")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!strings.Contains(status.Message, "Pod annotations") {
		t.Fatalf("forged Pod-annotation status = %s/%s %q, want Degraded/Closed annotation attestation failure", status.Lifecycle, status.AdmissionState, status.Message)
	}
}

func TestWorkspaceRuntimePoolSupervisorRestartRecyclesClaim(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.72")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-1", false)
	runtimePoolReconcile(t, r, pool)

	// The supervisor restarted in place: same Pod, different boot.
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-2", false)
	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("admission after in-place restart = %s, want Closed", status.AdmissionState)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim == nil {
		t.Fatal("claim deleted before the admission closure barrier persisted")
	}

	runtimePoolReconcile(t, r, pool)
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("claim was not deleted to recycle the restarted supervisor instance")
	}
	status = runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || status.ActiveInstance != nil {
		t.Fatalf("status = %s (active=%v), want Stopping with no active instance", status.Lifecycle, status.ActiveInstance)
	}
}

func TestWorkspaceRuntimePoolDisabledAdmissionPreservesActiveInstance(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.84")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-disabled", false)
	runtimePoolReconcile(t, r, pool)
	serving := runtimePoolTestGetPool(t, r, pool)
	if serving.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing || serving.Status.ActiveInstance == nil {
		t.Fatalf("precondition status = %s active=%v, want Serving active instance", serving.Status.Lifecycle, serving.Status.ActiveInstance)
	}
	wantRuntimeInstanceID := serving.Status.ActiveInstance.RuntimeInstanceID

	r.AgentSandboxEnabled = false
	runtimePoolReconcile(t, r, pool)
	disabled := runtimePoolTestGetPool(t, r, pool)
	if disabled.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleServing ||
		disabled.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		disabled.Status.ActiveInstance == nil ||
		disabled.Status.ActiveInstance.RuntimeInstanceID != wantRuntimeInstanceID {
		t.Fatalf("disabled status = %s/%s active=%#v, want serving instance preserved with admission closed", disabled.Status.Lifecycle, disabled.Status.AdmissionState, disabled.Status.ActiveInstance)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim == nil {
		t.Fatal("disabling admission deleted the active SandboxClaim")
	}
}

func TestWorkspaceRuntimePoolRolloutRecyclesUnadmittedReadyClaimWithoutProbe(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.85")
	r.ControllerEpoch++

	runtimePoolReconcile(t, r, pool)

	if supervisor.probeCalls != 0 {
		t.Fatalf("unadmitted Ready workspace received %d authenticated rollout probes", supervisor.probeCalls)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("unadmitted Ready workspace claim survived rollout replacement")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || status.ActiveInstance != nil {
		t.Fatalf("rollout status = %s active=%v, want Stopping with no active instance", status.Lifecycle, status.ActiveInstance)
	}
}

func TestWorkspaceRuntimePoolRecyclesClaimWhenPhysicalPodChanges(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)
	seedCalls := 0
	r.WorkspaceCredentialSeeder = func(context.Context, string, string, []byte, harnessv2.CredentialBootstrapRequest) (bool, error) {
		seedCalls++
		return false, nil
	}

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace template and claim were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.82")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-original", false)
	runtimePoolReconcile(t, r, pool)
	if seedCalls != 1 {
		t.Fatalf("initial credential seed calls = %d, want 1", seedCalls)
	}

	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete replaced workspace Pod: %v", err)
	}
	replacement := pod.DeepCopy()
	replacement.UID = types.UID("replacement-sandbox-pod-uid")
	replacement.ResourceVersion = ""
	replacement.Status.PodIP = "10.0.0.83"
	runtimePoolTestCreatePod(t, r, replacement)
	supervisor.probe = runtimePoolValidProbe(pool, replacement, "boot-replacement", false)

	runtimePoolReconcile(t, r, pool)

	if seedCalls != 1 {
		t.Fatalf("replacement physical Pod received %d total credential seeds, want the original seed only", seedCalls)
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("replacement physical Pod did not trigger SandboxClaim recycling")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded ||
		status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		status.ActiveInstance != nil ||
		!strings.Contains(status.Message, "physical instance changed") {
		t.Fatalf("replacement physical Pod status = %s/%s active=%v message=%q, want Degraded/Closed with no active instance", status.Lifecycle, status.AdmissionState, status.ActiveInstance, status.Message)
	}
}

func TestWorkspaceRuntimePoolRotatesConsumedBootstrapBeforeReplacementClaim(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace template and claim were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.80")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-bootstrap-rotation", false)
	runtimePoolReconcile(t, r, pool)

	oldAuth := runtimePoolTestPrivateAuthSecret(t, r, pool)
	oldBinding, err := runtimePoolBootstrapInstanceBindingFromAnnotation(new(runtimePoolTestGetPool(t, r, pool)))
	if err != nil || oldBinding == nil {
		t.Fatalf("bootstrap instance binding = %#v, error=%v, want the served Pod binding", oldBinding, err)
	}
	if oldBinding.AuthSecretUID != oldAuth.UID || oldBinding.WorkloadUID != pod.UID {
		t.Fatalf("bootstrap instance binding = %#v, want auth Secret %q and Pod %q", oldBinding, oldAuth.UID, pod.UID)
	}

	if err := r.Delete(context.Background(), claim); err != nil {
		t.Fatalf("delete disappeared SandboxClaim: %v", err)
	}
	sandbox := &sandboxv1beta1.Sandbox{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(claim), sandbox); err == nil {
		if err := r.Delete(context.Background(), sandbox); err != nil {
			t.Fatalf("delete disappeared Sandbox: %v", err)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("get disappeared Sandbox: %v", err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete disappeared sandbox Pod: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	if strings.TrimSpace(current.Annotations[runtimePoolPrivateAuthSecretBindingAnnotation(7)]) != "" {
		t.Fatal("consumed auth Secret remained published after physical instance disappearance")
	}
	if strings.TrimSpace(current.Annotations[runtimePoolBootstrapInstanceBindingAnnotation]) == "" {
		t.Fatal("old physical-instance binding cleared before fresh credentials were published")
	}
	if _, _, replacement := runtimePoolWorkspaceTestChildren(t, r, pool); replacement != nil {
		t.Fatal("replacement claim was acquired before consumed bootstrap material rotated")
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&oldAuth), &corev1.Secret{}); err != nil {
		t.Fatalf("consumed auth Secret disappeared before create-before-publish rotation: %v", err)
	}

	r.Rand = &runtimePoolTestEntropyReader{next: 100}
	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	newAuth := runtimePoolTestPrivateAuthSecret(t, r, pool)
	if newAuth.UID == oldAuth.UID || newAuth.Name == oldAuth.Name {
		t.Fatalf("rotated auth Secret retained consumed identity %s/%s", newAuth.Name, newAuth.UID)
	}
	if bytes.Equal(newAuth.Data[runtimePoolBootstrapNonceKey], oldAuth.Data[runtimePoolBootstrapNonceKey]) ||
		bytes.Equal(newAuth.Data[runtimePoolBootstrapSigningSeedKey], oldAuth.Data[runtimePoolBootstrapSigningSeedKey]) {
		t.Fatal("rotated auth Secret retained consumed bootstrap material")
	}
	if strings.TrimSpace(current.Annotations[runtimePoolPrivateAuthSecretBindingAnnotation(7)]) == "" {
		t.Fatal("fresh auth Secret was not published before clearing the old instance binding")
	}
	if strings.TrimSpace(current.Annotations[runtimePoolBootstrapInstanceBindingAnnotation]) != "" {
		t.Fatal("old physical-instance binding survived fresh credential publication")
	}
	if _, _, replacement := runtimePoolWorkspaceTestChildren(t, r, pool); replacement != nil {
		t.Fatal("replacement claim was acquired in the credential-rotation barrier pass")
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(&oldAuth), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("consumed auth Secret survived rotation: %v", err)
	}

	for range 6 {
		runtimePoolReconcile(t, r, pool)
		if _, _, replacement := runtimePoolWorkspaceTestChildren(t, r, pool); replacement != nil {
			return
		}
	}
	t.Fatal("replacement claim was not acquired after fresh bootstrap material was published")
}

func TestWorkspaceRuntimePoolRecoversAfterBoundAuthSecretDisappears(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if template == nil || claim == nil {
		t.Fatal("workspace template and claim were not materialized")
	}
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.81")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-missing-auth", false)
	runtimePoolReconcile(t, r, pool)

	oldAuth := runtimePoolTestPrivateAuthSecret(t, r, pool)
	current := runtimePoolTestGetPool(t, r, pool)
	bindingKey := runtimePoolPrivateAuthSecretBindingAnnotation(7)
	oldBinding := current.Annotations[bindingKey]
	if oldBinding == "" || strings.TrimSpace(current.Annotations[runtimePoolBootstrapInstanceBindingAnnotation]) == "" {
		t.Fatal("serving workspace did not record credential and physical-instance bindings")
	}
	if err := r.Delete(context.Background(), &oldAuth); err != nil {
		t.Fatalf("delete bound auth Secret: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Annotations[bindingKey] != oldBinding {
		t.Fatal("missing auth Secret binding was cleared before deleting the provider claim")
	}
	if _, _, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool); currentClaim != nil {
		t.Fatal("provider claim survived missing-auth recovery")
	}

	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	if current.Annotations[bindingKey] != oldBinding {
		t.Fatal("missing auth Secret binding was cleared while the provider Pod remained")
	}
	sandbox := &sandboxv1beta1.Sandbox{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(claim), sandbox); err == nil {
		if err := r.Delete(context.Background(), sandbox); err != nil {
			t.Fatalf("delete disappeared Sandbox: %v", err)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("get disappeared Sandbox: %v", err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete disappeared sandbox Pod: %v", err)
	}

	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	if strings.TrimSpace(current.Annotations[bindingKey]) != "" {
		t.Fatal("missing auth Secret remained published after provider workload absence was proven")
	}
	if strings.TrimSpace(current.Annotations[runtimePoolBootstrapInstanceBindingAnnotation]) == "" {
		t.Fatal("old physical-instance binding cleared before fresh credentials were published")
	}

	r.Rand = &runtimePoolTestEntropyReader{next: 100}
	runtimePoolReconcile(t, r, pool)
	current = runtimePoolTestGetPool(t, r, pool)
	newAuth := runtimePoolTestPrivateAuthSecret(t, r, pool)
	if newAuth.UID == oldAuth.UID || newAuth.Name == oldAuth.Name {
		t.Fatalf("replacement auth Secret retained missing identity %s/%s", newAuth.Name, newAuth.UID)
	}
	if strings.TrimSpace(current.Annotations[bindingKey]) == "" {
		t.Fatal("fresh auth Secret was not published")
	}
	if strings.TrimSpace(current.Annotations[runtimePoolBootstrapInstanceBindingAnnotation]) != "" {
		t.Fatal("old physical-instance binding survived fresh credential publication")
	}
	if _, _, replacement := runtimePoolWorkspaceTestChildren(t, r, pool); replacement != nil {
		t.Fatal("replacement claim was acquired in the credential-rotation barrier pass")
	}
}

func TestWorkspaceRuntimePoolScaleToZeroRecyclesUnadmittedReadyClaimWithoutProbe(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.86")
	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 0
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("scale unadmitted workspace pool to zero: %v", err)
	}

	runtimePoolReconcile(t, r, pool)

	if supervisor.probeCalls != 0 {
		t.Fatalf("unadmitted Ready workspace received %d authenticated scale-down probes", supervisor.probeCalls)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("unadmitted Ready workspace claim survived scale-down")
	}
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopping || status.ActiveInstance != nil {
		t.Fatalf("scale-down status = %s active=%v, want Stopping with no active instance", status.Lifecycle, status.ActiveInstance)
	}
}

func TestWorkspaceRuntimePoolScaleToZeroDrainsThenDeletesClaim(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.73")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-1", false)
	runtimePoolReconcile(t, r, pool)

	// Scale to zero. The spec change bumps the pool generation, so the flow
	// mirrors the Deployment path: authenticated rollout drain of the exact
	// old instance validated against the deployed template identity.
	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 0
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("scale pool to zero: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls = %d, want 1", supervisor.drainCalls)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim == nil {
		t.Fatal("claim deleted before authenticated drain quiescence")
	}

	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-1", true)
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("lifecycle after quiescent probe = %s, want Quiescent", got)
	}
	runtimePoolReconcile(t, r, pool)
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("claim was not deleted after the persisted quiescence barrier")
	}

	// The provider cascades the sandbox Pod; simulate its termination.
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete sandbox Pod: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	runtimePoolReconcile(t, r, pool)
	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped {
		t.Fatalf("lifecycle = %s, want Stopped", status.Lifecycle)
	}
	if status.ActiveInstance != nil {
		t.Fatal("stopped workspace pool retained an active instance")
	}
}

func TestWorkspaceRuntimePoolDemandReturnCompletesDrainBeforeReplacement(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.74")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-reactivate", false)
	runtimePoolReconcile(t, r, pool)

	current := runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 0
	current.Generation++
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("scale pool to zero: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 1 {
		t.Fatalf("drain calls = %d, want 1", supervisor.drainCalls)
	}

	current = runtimePoolTestGetPool(t, r, pool)
	current.Spec.DesiredReplicas = 1
	current.Generation++
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("restore workspace demand: %v", err)
	}
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-reactivate", true)
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("lifecycle after demand returned to a drained workspace = %s, want Quiescent rollout barrier", got)
	}
	runtimePoolReconcile(t, r, pool)
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("old drained claim survived renewed demand")
	}

	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete old sandbox Pod: %v", err)
	}
	for range 5 {
		runtimePoolReconcile(t, r, pool)
		_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
		if claim != nil {
			return
		}
	}
	t.Fatal("renewed demand did not create a replacement SandboxClaim")
}

func TestWorkspaceRuntimePoolFinalizerDeletesProviderChildren(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	if template, warmPool, claim := runtimePoolWorkspaceTestChildren(t, r, pool); template == nil || warmPool == nil || claim == nil {
		t.Fatal("workspace children were not materialized")
	}
	current := runtimePoolTestGetPool(t, r, pool)
	if err := r.Delete(context.Background(), &current); err != nil {
		t.Fatalf("delete pool: %v", err)
	}
	// Finalization is idempotent and requeues until every child is gone.
	for range 5 {
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}})
		if err != nil {
			t.Fatalf("finalize reconcile: %v", err)
		}
		if result.RequeueAfter == 0 {
			break
		}
	}
	if template, warmPool, claim := runtimePoolWorkspaceTestChildren(t, r, pool); template != nil || warmPool != nil || claim != nil {
		t.Fatalf("workspace children survived finalization: template=%v warmPool=%v claim=%v", template != nil, warmPool != nil, claim != nil)
	}
	var got corev1alpha1.RuntimePool
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("pool still present after finalization: %v", err)
	}
}

func TestWorkspaceRuntimePoolFinalizerWaitsForRecordedSandboxDeletion(t *testing.T) {
	const checkpointedSandboxName = "checkpointed-sandbox"

	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolSandboxSuspendTestObject()
	r := runtimePoolSandboxSuspendTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

	runtimePoolReconcile(t, r, pool)
	_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if claim == nil {
		t.Fatal("workspace claim was not materialized")
	}
	checkpointed := &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{
		Name:      checkpointedSandboxName,
		Namespace: claim.Namespace,
		UID:       types.UID(checkpointedSandboxName + "-uid"),
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(claim, sandboxextv1beta1.GroupVersion.WithKind("SandboxClaim")),
		},
	}}
	if err := r.Create(context.Background(), checkpointed); err != nil {
		t.Fatalf("create checkpointed Sandbox: %v", err)
	}
	record, err := json.Marshal(sandboxSuspendRecord{
		Name: checkpointed.Name, UID: checkpointed.UID,
		PVCUID: types.UID("checkpointed-pvc-uid"), PVName: "checkpointed-pv", PVUID: types.UID("checkpointed-pv-uid"),
	})
	if err != nil {
		t.Fatalf("encode checkpoint record: %v", err)
	}
	current := runtimePoolTestGetPool(t, r, pool)
	current.Annotations[sandboxSuspendedAnnotation] = string(record)
	if err := r.Update(context.Background(), &current); err != nil {
		t.Fatalf("record checkpointed Sandbox: %v", err)
	}
	current = runtimePoolTestGetPool(t, r, pool)
	if err := r.Delete(context.Background(), &current); err != nil {
		t.Fatalf("delete pool: %v", err)
	}

	for range 4 {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
			t.Fatalf("finalize while Sandbox remains: %v", err)
		}
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); err != nil {
		t.Fatalf("RuntimePool finalized before the recorded Sandbox disappeared: %v", err)
	}
	if err := r.Delete(context.Background(), checkpointed); err != nil {
		t.Fatalf("delete checkpointed Sandbox: %v", err)
	}
	for range 4 {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
			t.Fatalf("finish finalization: %v", err)
		}
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
		t.Fatalf("RuntimePool survived recorded Sandbox deletion: %v", err)
	}
}

func TestWorkspaceRuntimePoolFinalizerRecoversLegacyLineageSandboxIdentity(t *testing.T) {
	for _, test := range []struct {
		name         string
		claimPresent bool
	}{
		{name: "owned claim remains", claimPresent: true},
		{name: "claim already deleted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtimePoolWorkspaceTestScheme(t)
			pool := runtimePoolSandboxSuspendTestObject()
			r := runtimePoolSandboxSuspendTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

			runtimePoolReconcile(t, r, pool)
			_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
			if claim == nil {
				t.Fatal("workspace claim was not materialized")
			}
			if claim.UID == "" {
				claim.UID = types.UID("legacy-lineage-claim-uid")
				if err := r.Update(ctx, claim); err != nil {
					t.Fatalf("assign claim UID: %v", err)
				}
			}
			cfg, err := r.runtimePoolConfigForDeletion(pool)
			if err != nil {
				t.Fatalf("resolve deletion config: %v", err)
			}
			podLabels := cloneStringMap(cfg.labels)
			podLabels[sandboxextv1beta1.SandboxIDLabel] = string(claim.UID)
			sandbox := &sandboxv1beta1.Sandbox{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: claim.Namespace,
					Name:      "legacy-lineage-sandbox",
					UID:       types.UID("legacy-lineage-sandbox-uid"),
					Labels: map[string]string{
						sandboxextv1beta1.SandboxIDLabel: string(claim.UID),
					},
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(claim, sandboxextv1beta1.GroupVersion.WithKind("SandboxClaim")),
					},
				},
			}
			sandbox.Spec.PodTemplate.ObjectMeta.Labels = podLabels
			if err := r.Create(ctx, sandbox); err != nil {
				t.Fatalf("create legacy-lineage Sandbox: %v", err)
			}
			legacy, err := json.Marshal(sandboxDurableLineageRecord{
				PVCUID: types.UID("legacy-pvc-uid"),
				PVName: "legacy-pv",
				PVUID:  types.UID("legacy-pv-uid"),
			})
			if err != nil {
				t.Fatalf("encode legacy lineage: %v", err)
			}
			current := runtimePoolTestGetPool(t, r, pool)
			current.Annotations[runtimePoolDurableLineageAnnotation] = string(legacy)
			if err := r.Update(ctx, &current); err != nil {
				t.Fatalf("record legacy lineage: %v", err)
			}
			if !test.claimPresent {
				if err := r.Delete(ctx, claim); err != nil {
					t.Fatalf("delete claim before finalization: %v", err)
				}
			}

			current = runtimePoolTestGetPool(t, r, pool)
			remaining, err := r.deleteRuntimePoolWorkspaceChildren(ctx, &current, cfg)
			if err != nil {
				t.Fatalf("recover legacy finalization identity: %v", err)
			}
			if !remaining {
				t.Fatal("identity recovery must requeue before provider child deletion")
			}
			recordedPool := runtimePoolTestGetPool(t, r, pool)
			recorded := sandboxRecordedDurableLineage(&recordedPool)
			if recorded == nil || recorded.Name != sandbox.Name || recorded.UID != sandbox.UID {
				t.Fatalf("recovered lineage = %+v, want Sandbox %s/%s", recorded, sandbox.Name, sandbox.UID)
			}
			template, warmPool, currentClaim := runtimePoolWorkspaceTestChildren(t, r, pool)
			if template == nil || warmPool == nil || (test.claimPresent && currentClaim == nil) {
				t.Fatalf("provider children changed before identity persistence: template=%v warmPool=%v claim=%v",
					template != nil, warmPool != nil, currentClaim != nil)
			}

			if err := r.Delete(ctx, &recordedPool); err != nil {
				t.Fatalf("delete pool: %v", err)
			}
			for range 4 {
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
					t.Fatalf("finalize while recovered Sandbox remains: %v", err)
				}
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); err != nil {
				t.Fatalf("RuntimePool finalized before the recovered Sandbox disappeared: %v", err)
			}
			if err := r.Delete(ctx, sandbox); err != nil {
				t.Fatalf("delete recovered Sandbox: %v", err)
			}
			for range 8 {
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
					t.Fatalf("finish finalization: %v", err)
				}
				if err := r.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); apierrors.IsNotFound(err) {
					break
				}
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
				t.Fatalf("RuntimePool survived recovered Sandbox deletion: %v", err)
			}
		})
	}
}

func TestWorkspaceRuntimePoolFinalizerProvesLegacyLineageSandboxAbsent(t *testing.T) {
	ctx := context.Background()
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolSandboxSuspendTestObject()
	r := runtimePoolSandboxSuspendTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

	runtimePoolReconcile(t, r, pool)
	_, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool)
	if claim == nil {
		t.Fatal("workspace claim was not materialized")
	}
	legacy, err := json.Marshal(sandboxDurableLineageRecord{
		PVCUID: types.UID("legacy-pvc-uid"),
		PVName: "legacy-pv",
		PVUID:  types.UID("legacy-pv-uid"),
	})
	if err != nil {
		t.Fatalf("encode legacy lineage: %v", err)
	}
	current := runtimePoolTestGetPool(t, r, pool)
	current.Annotations[runtimePoolDurableLineageAnnotation] = string(legacy)
	if err := r.Update(ctx, &current); err != nil {
		t.Fatalf("record legacy lineage: %v", err)
	}
	if err := r.Delete(ctx, claim); err != nil {
		t.Fatalf("delete absent-lineage claim: %v", err)
	}
	current = runtimePoolTestGetPool(t, r, pool)
	if err := r.Delete(ctx, &current); err != nil {
		t.Fatalf("delete pool: %v", err)
	}
	for range 8 {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)}); err != nil {
			t.Fatalf("finalize after proving Sandbox absence: %v", err)
		}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
		t.Fatalf("RuntimePool remained after claim and Sandbox absence was proven: %v", err)
	}
}

func TestWorkspaceRuntimePoolFinalizerBypassesMalformedDurableMetadata(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
	}{
		{name: "checkpoint", annotation: sandboxSuspendedAnnotation},
		{name: "lineage", annotation: runtimePoolDurableLineageAnnotation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtimePoolWorkspaceTestScheme(t)
			pool := runtimePoolSandboxSuspendTestObject()
			r := runtimePoolSandboxSuspendTestReconciler(t, scheme, &fakeRuntimePoolSupervisorClient{}, pool)

			runtimePoolReconcile(t, r, pool)
			if template, warmPool, claim := runtimePoolWorkspaceTestChildren(t, r, pool); template == nil || warmPool == nil || claim == nil {
				t.Fatal("workspace children were not materialized")
			}
			current := runtimePoolTestGetPool(t, r, pool)
			current.Annotations[tt.annotation] = malformedSandboxMetadata
			if err := r.Update(context.Background(), &current); err != nil {
				t.Fatalf("record malformed %s metadata: %v", tt.name, err)
			}
			current = runtimePoolTestGetPool(t, r, pool)
			if err := r.Delete(context.Background(), &current); err != nil {
				t.Fatalf("delete pool: %v", err)
			}

			for range 8 {
				result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
				if err != nil {
					t.Fatalf("finalize reconcile: %v", err)
				}
				if result.RequeueAfter == 0 {
					break
				}
			}
			if template, warmPool, claim := runtimePoolWorkspaceTestChildren(t, r, pool); template != nil || warmPool != nil || claim != nil {
				t.Fatalf("workspace children survived finalization: template=%v warmPool=%v claim=%v", template != nil, warmPool != nil, claim != nil)
			}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
				t.Fatalf("pool still present after finalization: %v", err)
			}
		})
	}
}

func TestWorkspaceRuntimePoolFinalizerDrainsLiveInstanceBeforeClaimDeletion(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)
	template, _, _ := runtimePoolWorkspaceTestChildren(t, r, pool)
	pod := runtimePoolWorkspaceTestMaterialization(t, r, pool, template, "10.0.0.75")
	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-finalizer", false)
	runtimePoolReconcile(t, r, pool)
	r.AllowedImages.Codex = "docker.io/sozercan/orka-acp@sha256:" + strings.Repeat("9", 64)

	current := runtimePoolTestGetPool(t, r, pool)
	if err := r.Delete(context.Background(), &current); err != nil {
		t.Fatalf("delete live workspace RuntimePool: %v", err)
	}
	runtimePoolReconcile(t, r, pool)
	if supervisor.drainCalls != 1 {
		t.Fatalf("finalizer drain calls = %d, want 1", supervisor.drainCalls)
	}
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim == nil {
		t.Fatal("live claim was deleted before authenticated finalizer drain quiescence")
	}

	supervisor.probe = runtimePoolValidProbe(pool, &pod, "boot-finalizer", true)
	runtimePoolReconcile(t, r, pool)
	if got := runtimePoolTestGetPool(t, r, pool).Status.Lifecycle; got != corev1alpha1.RuntimePoolLifecycleQuiescent {
		t.Fatalf("finalizer lifecycle after quiescent probe = %s, want Quiescent", got)
	}
	runtimePoolReconcile(t, r, pool)
	if _, _, claim := runtimePoolWorkspaceTestChildren(t, r, pool); claim != nil {
		t.Fatal("quiescent claim survived finalizer scale-down")
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatalf("delete cascaded workspace Pod fixture: %v", err)
	}
	for range 8 {
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		if err != nil {
			t.Fatalf("finish drained workspace finalization: %v", err)
		}
		if result.RequeueAfter == 0 {
			break
		}
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pool), &corev1alpha1.RuntimePool{}); !apierrors.IsNotFound(err) {
		t.Fatalf("drained workspace RuntimePool survived finalization: %v", err)
	}
}

func TestWorkspaceRuntimePoolFinalizerRefusesForeignProviderChildren(t *testing.T) {
	scheme := runtimePoolWorkspaceTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	pool.Finalizers = []string{runtimePoolFinalizer}
	deletedAt := metav1.NewTime(runtimePoolTestNow)
	pool.DeletionTimestamp = &deletedAt
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	foreign := &sandboxextv1beta1.SandboxClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      runtimePoolSandboxClaimName(base),
		Namespace: pool.Namespace,
		Labels: map[string]string{
			runtimePoolManagedByLabel: "another-controller",
			runtimePoolUIDLabel:       "foreign-pool-uid",
		},
	}}
	r := runtimePoolTestReconciler(t, scheme, nil, pool, foreign)

	_, _, err := runtimePoolTestFinalize(r, pool)
	if err == nil || !strings.Contains(err.Error(), "refusing to delete foreign") {
		t.Fatalf("finalization error = %v, want foreign-resource ownership rejection", err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(foreign), &sandboxextv1beta1.SandboxClaim{}); err != nil {
		t.Fatalf("foreign SandboxClaim was deleted: %v", err)
	}
	current := runtimePoolTestGetPool(t, r, pool)
	if !controllerutil.ContainsFinalizer(&current, runtimePoolFinalizer) {
		t.Fatal("RuntimePool finalizer was removed after foreign-resource rejection")
	}
}

func TestValidateRuntimePoolExecutionWorkspace(t *testing.T) {
	pool := runtimePoolWorkspaceTestObject()
	if err := validateRuntimePoolExecutionWorkspace(pool); err != nil {
		t.Fatalf("valid workspace pool rejected: %v", err)
	}

	plain := runtimePoolTestObject(1)
	if err := validateRuntimePoolExecutionWorkspace(plain); err != nil {
		t.Fatalf("plain pool rejected: %v", err)
	}

	substrateMissingTemplate := runtimePoolWorkspaceTestObject()
	substrateMissingTemplate.Spec.ExecutionWorkspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
	if err := validateRuntimePoolExecutionWorkspace(substrateMissingTemplate); err == nil || !strings.Contains(err.Error(), "infrastructure ActorTemplate") {
		t.Fatalf("substrate provider error = %v, want missing infrastructure template", err)
	}
	substratePool := runtimePoolWorkspaceTestObject()
	substratePool.Spec.ExecutionWorkspace.Provider = corev1alpha1.WorkspaceProviderSubstrate
	substratePool.Spec.ExecutionWorkspace.Substrate = &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
		BaseTemplateNamespace: substrateTestTemplateNamespace, BaseTemplateName: substrateTestBaseTemplateName,
	}
	if err := validateRuntimePoolExecutionWorkspace(substratePool); err != nil {
		t.Fatalf("valid substrate pool rejected: %v", err)
	}
	if err := validateRuntimePoolExecutionWorkspaceNamespace(substratePool, acpTestRuntimeNamespace); err != nil {
		t.Fatalf("separate substrate template namespace rejected: %v", err)
	}
	if err := validateRuntimePoolExecutionWorkspaceNamespace(substratePool, substrateTestTemplateNamespace); err == nil ||
		!strings.Contains(err.Error(), "must differ") {
		t.Fatalf("shared template/runtime namespace error = %v, want Secret-boundary rejection", err)
	}
	sandboxWithSubstrate := runtimePoolWorkspaceTestObject()
	sandboxWithSubstrate.Spec.ExecutionWorkspace.Substrate = &corev1alpha1.RuntimePoolSubstrateWorkspaceSpec{
		BaseTemplateNamespace: substrateTestTemplateNamespace, BaseTemplateName: substrateTestBaseTemplateName,
	}
	if err := validateRuntimePoolExecutionWorkspace(sandboxWithSubstrate); err == nil || !strings.Contains(err.Error(), "only valid for provider substrate") {
		t.Fatalf("sandbox-with-substrate error = %v, want provider mismatch", err)
	}
	substrateWithAgentSandbox := substratePool.DeepCopy()
	substrateWithAgentSandbox.Spec.ExecutionWorkspace.AgentSandbox = &corev1alpha1.RuntimePoolAgentSandboxWorkspaceSpec{}
	if err := validateRuntimePoolExecutionWorkspace(substrateWithAgentSandbox); err == nil || !strings.Contains(err.Error(), "only valid for provider agent-sandbox") {
		t.Fatalf("substrate-with-agent-sandbox error = %v, want provider mismatch", err)
	}
	sandboxMissingSuspendVolume := runtimePoolSandboxSuspendTestObject()
	sandboxMissingSuspendVolume.Spec.ExecutionWorkspace.AgentSandbox.SuspendVolume = nil
	if err := validateRuntimePoolExecutionWorkspace(sandboxMissingSuspendVolume); err == nil || !strings.Contains(err.Error(), "suspendMode and suspendVolume") {
		t.Fatalf("sandbox-without-suspend-volume error = %v, want paired-field rejection", err)
	}
	sandboxMissingSuspendMode := runtimePoolSandboxSuspendTestObject()
	sandboxMissingSuspendMode.Spec.ExecutionWorkspace.AgentSandbox.SuspendMode = ""
	if err := validateRuntimePoolExecutionWorkspace(sandboxMissingSuspendMode); err == nil || !strings.Contains(err.Error(), "suspendMode and suspendVolume") {
		t.Fatalf("sandbox-without-suspend-mode error = %v, want paired-field rejection", err)
	}
	for _, tt := range []struct {
		name    string
		mutate  func(*corev1alpha1.RuntimePoolSandboxDurableVolumeSpec)
		wantErr string
	}{
		{
			name: "zero durable capacity",
			mutate: func(volume *corev1alpha1.RuntimePoolSandboxDurableVolumeSpec) {
				volume.Capacity = "0"
			},
			wantErr: "must be positive",
		},
		{
			name: "read-only durable access mode",
			mutate: func(volume *corev1alpha1.RuntimePoolSandboxDurableVolumeSpec) {
				volume.AccessModes = []string{string(corev1.ReadOnlyMany)}
			},
			wantErr: "not a writable mode",
		},
		{
			name: "invalid durable storage class name",
			mutate: func(volume *corev1alpha1.RuntimePoolSandboxDurableVolumeSpec) {
				volume.StorageClassName = "INVALID_CLASS"
			},
			wantErr: "not a valid storage class name",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			restored := runtimePoolSandboxSuspendTestObject()
			tt.mutate(restored.Spec.ExecutionWorkspace.AgentSandbox.SuspendVolume)
			if err := validateRuntimePoolExecutionWorkspace(restored); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("restored RuntimePool error = %v, want %q", err, tt.wantErr)
			}
		})
	}

	badDigest := runtimePoolWorkspaceTestObject()
	badDigest.Spec.ExecutionWorkspace.BindingDigest = "not-a-digest"
	if err := validateRuntimePoolExecutionWorkspace(badDigest); err == nil || !strings.Contains(err.Error(), "bindingDigest") {
		t.Fatalf("binding digest error = %v, want digest rejection", err)
	}

	badCapacity := runtimePoolWorkspaceTestObject()
	badCapacity.Spec.Capacity = &corev1alpha1.RuntimePoolCapacitySpec{MaxResidentSessions: 10, MaxRunningPrompts: 4}
	if err := validateRuntimePoolExecutionWorkspace(badCapacity); err == nil || !strings.Contains(err.Error(), "exactly one resident RuntimeSession") {
		t.Fatalf("capacity error = %v, want single-session requirement", err)
	}
}

func TestWorkspaceRuntimePoolMissingProviderCRDsFailsClosed(t *testing.T) {
	// The scheme intentionally omits the sandbox extension types: without the
	// externally operated provider installation, the pool degrades and closes
	// admission instead of falling back to a Deployment.
	scheme := runtimePoolTestScheme(t)
	pool := runtimePoolWorkspaceTestObject()
	supervisor := &fakeRuntimePoolSupervisorClient{}
	r := runtimePoolTestReconciler(t, scheme, supervisor, pool)

	runtimePoolReconcile(t, r, pool)

	status := runtimePoolTestGetPool(t, r, pool).Status
	if status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDegraded || status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed {
		t.Fatalf("status = %s/%s, want Degraded/Closed", status.Lifecycle, status.AdmissionState)
	}
	if !strings.Contains(status.Message, "agent-sandbox provider CRDs are not installed") {
		t.Fatalf("message = %q, want missing provider CRD failure", status.Message)
	}
	var deployment appsv1.Deployment
	base := runtimePoolResourceName(pool.Namespace, pool.Name)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: pool.Namespace, Name: base}, &deployment); !apierrors.IsNotFound(err) {
		t.Fatalf("degraded workspace pool created a Deployment (err=%v); there is no cross-backend fallback", err)
	}
}
