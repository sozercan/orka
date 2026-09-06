/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/events"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	orkametrics "github.com/orka-agents/orka/internal/metrics"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/workspace"
)

var (
	errRuntimePoolBootstrapInstanceConflict = errors.New("RuntimePool bootstrap credentials are bound to another physical workspace instance")
	errWorkspaceRuntimePoolAuthBindingLost  = errors.New("bound private RuntimePool auth Secret no longer exists")
)

const (
	runtimePoolFinalizer          = "orka.ai/runtime-pool-cleanup"
	runtimePoolRequeue            = 10 * time.Second
	runtimePoolProbeTimeout       = 10 * time.Second
	runtimePoolPort         int32 = 8080

	runtimePoolManagedByLabel                     = "app.kubernetes.io/managed-by"
	runtimePoolManagedByLabelValue                = "orka"
	runtimePoolApplicationLabel                   = "app.kubernetes.io/name"
	runtimePoolApplicationLabelValue              = "orka-acp-runtime"
	runtimePoolKeyLabel                           = "orka.ai/runtime-pool-key"
	runtimePoolNameLabel                          = "orka.ai/runtime-pool-name"
	runtimePoolNamespaceLabel                     = "orka.ai/runtime-pool-namespace"
	runtimePoolUIDLabel                           = "orka.ai/runtime-pool-uid"
	runtimePoolNetworkRoleLabel                   = "orka.ai/network-role"
	runtimePoolAuthLabel                          = "orka.ai/runtime-pool-auth"
	runtimePoolCredentialEpochLabel               = "orka.ai/runtime-pool-controller-epoch"
	runtimePoolProviderCredentialLabel            = "orka.ai/runtime-pool-provider-credential"
	runtimePoolProviderGenerationLabel            = "orka.ai/runtime-pool-provider-generation"
	runtimePoolProfileAnnotation                  = "orka.ai/runtime-profile-digest"
	runtimePoolProviderTokenGenerationAnnotation  = "orka.ai/provider-token-generation"
	runtimePoolTemplateRevisionAnnotation         = "orka.ai/runtime-template-revision"
	runtimePoolPIDsAnnotation                     = "orka.ai/runtime-pids-limit"
	runtimePoolPrivateAuthBindingPrefix           = "orka.ai/private-auth-secret-e"
	runtimePoolBootstrapInstanceBindingAnnotation = "orka.ai/bootstrap-instance-binding"

	runtimePoolControllerTokenKey      = "controller-token"
	runtimePoolCapabilitySecretKey     = "capability-secret"
	runtimePoolBootstrapSigningSeedKey = "bootstrap-signing-seed"
	// runtimePoolBootstrapNonceKey holds the public per-instance credential
	// bootstrap nonce used by provider-hosted supervisors that boot
	// credential-free. It binds a signed request to the exact workload and
	// grants nothing by itself.
	runtimePoolBootstrapNonceKey = "bootstrap-nonce"
	runtimePoolProviderTokenKey  = "token"

	runtimePoolControllerTokenPath     = "/var/run/secrets/orka/auth/controller-token"
	runtimePoolCapabilitySecretPath    = "/var/run/secrets/orka/auth/capability-secret"
	runtimePoolProviderTokenPath       = "/var/run/secrets/orka/provider/token"
	runtimePoolOperationHeader         = "X-Orka-Operation-Capability"
	runtimePoolControllerTokenFileEnv  = "ORKA_ACP_CONTROLLER_TOKEN_FILE"
	runtimePoolCapabilitySecretFileEnv = "ORKA_ACP_CAPABILITY_SECRET_FILE"
	runtimePoolProviderTokenFileEnv    = "ORKA_ACP_PROVIDER_TOKEN_FILE"
	runtimePoolE2EPromptWriteAmbiguity = "ORKA_ACP_E2E_PROMPT_WRITE_AMBIGUITY_MARKER"

	runtimePoolAuthVolume               = "pool-auth"
	runtimePoolProviderCapabilityVolume = "provider-capability"
	runtimePoolSessionsVolume           = "sessions"
	runtimePoolTempVolume               = "tmp"
	runtimePoolHomeVolume               = "home"

	runtimePoolProviderCodex              = "codex"
	runtimePoolProviderClaude             = "claude"
	runtimePoolProviderCopilot            = "copilot"
	runtimePoolProviderOpencode           = "opencode"
	runtimePoolResourceClassStandard      = "standard"
	runtimePoolDefaultControllerNamespace = "orka-system"
	runtimePoolDefaultServiceAccountName  = "default"

	runtimePoolRolloutReasonDraining       = "RolloutDraining"
	runtimePoolRolloutReasonQuiescent      = "RolloutQuiescent"
	runtimePoolRolloutReasonStopping       = "RolloutStopping"
	runtimePoolRolloutReasonStarting       = "RolloutStarting"
	runtimePoolRolloutReasonTimedOut       = "RolloutTimedOut"
	runtimePoolSchedulingReasonPodNotReady = "PodNotReady"

	runtimePoolIdentityCapacityReasonDraining  = "IdentityCapacityDraining"
	runtimePoolIdentityCapacityReasonQuiescent = "IdentityCapacityQuiescent"
	runtimePoolIdentityCapacityReasonStopping  = "IdentityCapacityStopping"

	runtimePoolSupervisorRestartReasonDetected = "SupervisorRestartDetected"
	runtimePoolSupervisorRestartReasonStopping = "SupervisorRestartStopping"

	// Shared drain/rollout status messages, identical across the Deployment,
	// Agent Sandbox, and Substrate workload backends.
	runtimePoolMessageStopped               = "runtime pool is stopped"
	runtimePoolMessageDrainUnauthenticated  = "cannot authenticate the previous active runtime instance to prove drain"
	runtimePoolMessageDrainRequested        = "drain requested; waiting for authenticated quiescence"
	runtimePoolMessageDrainSettling         = "waiting for sessions, prompts, permissions, reservations, or finalization work to settle"
	runtimePoolMessageDrainQuiescent        = "authenticated supervisor and controller state are quiescent"
	runtimePoolMessageRolloutDrainRequested = "authenticated rollout drain requested; waiting for a subsequent quiescent observation"
	runtimePoolMessageRolloutSettling       = "waiting for controller reservations and supervisor sessions, prompts, permissions, descendants, or finalization to settle"
	runtimePoolMessageRolloutQuiescent      = "authenticated old runtime and controller reservations are quiescent"
)

var (
	digestPinnedImagePattern         = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)
	runtimePoolAuthSuffixPattern     = regexp.MustCompile(`auth-e[1-9][0-9]*(?:-[0-9a-f]{24})?$`)
	runtimePoolProviderSuffixPattern = regexp.MustCompile(`provider-e[1-9][0-9]*-g[0-9a-f]{16}(?:-[0-9a-f]{24})?$`)
)

// RuntimePoolProbeResult is the authenticated, exact-instance supervisor view
// used to select and fence one active runtime Pod.
type RuntimePoolProbeResult struct {
	Capabilities harnessv2.CapabilitiesResponse
	Status       harnessv2.StatusResponse
}

// RuntimePoolSupervisorClient is the v2 control surface required by the
// Kubernetes reconciler. It intentionally excludes task and session dispatch.
type RuntimePoolSupervisorClient interface {
	Probe(ctx context.Context, endpoint, bearerToken string, capabilitySecret []byte) (RuntimePoolProbeResult, error)
	RequestDrain(ctx context.Context, endpoint, bearerToken string, capabilitySecret []byte, status harnessv2.StatusResponse, reason string) error
}

// RuntimePoolProviderProxyConfig binds managed RuntimePools to the authenticated
// cluster-local provider proxy. BearerToken is copied into an epoch-scoped
// RuntimePool Secret and is never placed in Pod environment variables.
type RuntimePoolProviderProxyConfig struct {
	BaseURL         string
	Namespace       string
	PodLabels       map[string]string
	BearerToken     []byte
	BearerTokenFile string
}

func (c RuntimePoolProviderProxyConfig) String() string {
	return fmt.Sprintf("{BaseURL:%q Namespace:%q PodLabels:%v BearerToken:[redacted]}", c.BaseURL, c.Namespace, c.PodLabels)
}

func (c RuntimePoolProviderProxyConfig) GoString() string { return c.String() }

type runtimePoolProviderProxyConfig struct {
	baseURL         string
	namespace       string
	podLabels       map[string]string
	port            int32
	token           []byte
	tokenGeneration string
}

// Validate verifies that the provider boundary is complete and cluster-local.
func (c RuntimePoolProviderProxyConfig) Validate() error {
	_, err := c.normalized()
	return err
}

func (c RuntimePoolProviderProxyConfig) normalized() (runtimePoolProviderProxyConfig, error) {
	baseURL := strings.TrimSpace(c.BaseURL)
	if baseURL == "" {
		return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy base URL is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != urlSchemeHTTP && parsed.Scheme != urlSchemeHTTPS) {
		return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy base URL is invalid")
	}
	namespace := strings.TrimSpace(c.Namespace)
	if errs := validation.IsDNS1123Label(namespace); len(errs) != 0 {
		return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy namespace is required and must be a DNS label")
	}
	hostname := strings.ToLower(parsed.Hostname())
	serviceSuffix := "." + strings.ToLower(namespace) + ".svc"
	if !strings.HasSuffix(hostname, serviceSuffix) && !strings.HasSuffix(hostname, serviceSuffix+".cluster.local") {
		return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy base URL must address a Service in its configured namespace")
	}
	port := int32(80)
	if parsed.Scheme == "https" {
		port = 443
	}
	if rawPort := parsed.Port(); rawPort != "" {
		parsedPort, parseErr := strconv.ParseUint(rawPort, 10, 16)
		if parseErr != nil || parsedPort == 0 {
			return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy base URL port is invalid")
		}
		port = int32(parsedPort)
	}
	if len(c.PodLabels) == 0 {
		return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy pod labels are required")
	}
	podLabels := cloneStringMap(c.PodLabels)
	for key, value := range podLabels {
		if errs := validation.IsQualifiedName(key); len(errs) != 0 {
			return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy pod label key is invalid")
		}
		if errs := validation.IsValidLabelValue(value); len(errs) != 0 {
			return runtimePoolProviderProxyConfig{}, fmt.Errorf("authenticated provider proxy pod label value is invalid")
		}
	}
	token := bytes.Clone(c.BearerToken)
	if strings.TrimSpace(c.BearerTokenFile) != "" {
		if len(token) != 0 {
			return runtimePoolProviderProxyConfig{}, fmt.Errorf("provider proxy bearer token and token file are mutually exclusive")
		}
		var readErr error
		token, readErr = readRuntimePoolProviderTokenFile(c.BearerTokenFile)
		if readErr != nil {
			return runtimePoolProviderProxyConfig{}, readErr
		}
	}
	if err := validateRuntimePoolProviderToken(token); err != nil {
		return runtimePoolProviderProxyConfig{}, err
	}
	return runtimePoolProviderProxyConfig{
		baseURL: parsed.String(), namespace: namespace, podLabels: podLabels, port: port, token: token,
		tokenGeneration: runtimePoolProviderTokenGeneration(token),
	}, nil
}

func readRuntimePoolProviderTokenFile(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("provider proxy bearer token file must be an absolute clean path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve provider proxy bearer token file: %w", err)
	}
	root, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("resolve provider proxy bearer token directory: %w", err)
	}
	if rel, relErr := filepath.Rel(root, resolved); relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("provider proxy bearer token file escapes its mounted directory")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 32 || info.Size() > 4096 {
		return nil, fmt.Errorf("provider proxy bearer token file is unavailable")
	}
	value, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read provider proxy bearer token file: %w", err)
	}
	return bytes.TrimSpace(value), nil
}

func runtimePoolProviderTokenGeneration(token []byte) string {
	sum := sha256.Sum256(token)
	return hex.EncodeToString(sum[:])[:16]
}

func validateRuntimePoolProviderToken(token []byte) error {
	if len(token) < 32 || len(token) > 4096 {
		return fmt.Errorf("authenticated provider proxy bearer token is required")
	}
	for _, value := range token {
		if value <= ' ' || value == 0x7f {
			return fmt.Errorf("authenticated provider proxy bearer token is invalid")
		}
	}
	return nil
}

// RuntimePoolReconciler reconciles controller-owned built-in ACP runtime pools.
type RuntimePoolReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *k8sruntime.Scheme

	// RuntimeNamespace is used when spec.runtimeNamespace is empty.
	RuntimeNamespace string
	// ControllerNamespace is the namespace allowed to reach the runtime control port.
	ControllerNamespace string
	// ControllerAPIURL is the exact internal artifact/broker API base URL.
	ControllerAPIURL string
	// ControllerAPIPort is the controller Pod target port allowed by the runtime egress policy.
	ControllerAPIPort int32
	// WorkspaceArtifactMaxBytes is the Publisher-advertised maximum inbound
	// workspace artifact size propagated to every built-in runtime.
	WorkspaceArtifactMaxBytes int64
	// ProviderProxy is the authenticated, NetworkPolicy-confined provider boundary.
	ProviderProxy RuntimePoolProviderProxyConfig
	// ControllerEpoch is a test/static override. Production uses Epochs.
	ControllerEpoch int64
	// Epochs supplies the current durable leader epoch.
	Epochs *ControllerEpochManager
	// EnablePDB protects a serving singleton from voluntary disruption.
	EnablePDB bool
	// AllowedImages is the controller-configured immutable image allowlist for
	// built-in pools. RuntimePool is controller-owned; its public spec cannot be
	// used to select an arbitrary privileged supervisor image.
	AllowedImages ACPRuntimeImages
	// CleanupOnly keeps deletion finalization active while ACP runtime admission
	// is disabled without adding finalizers or creating runtime resources.
	CleanupOnly bool
	// E2EPromptWriteAmbiguityMarker is a disabled-by-default live-conformance
	// fault marker projected into built-in runtime supervisors.
	E2EPromptWriteAmbiguityMarker string

	// AgentSandboxEnabled admits Agent Sandbox-backed workspace pools.
	AgentSandboxEnabled bool
	// SubstrateEnabled admits Substrate-backed workspace pools.
	SubstrateEnabled bool
	// SubstrateConfig carries the externally operated Substrate control-plane
	// and router configuration for Substrate-backed workspace pools.
	SubstrateConfig SubstrateConfig
	// SubstrateActorControlFactory builds the narrow, suspension-free actor
	// control client. Tests inject fakes; production defaults to the gRPC client.
	SubstrateActorControlFactory func(SubstrateConfig) (workspace.SubstrateRuntimeActorControl, error)
	// SubstrateCredentialSeeder overrides the fresh-boot credential PUT for
	// tests. Production sends fresh boots through the router; data-resumed actors
	// require the provider control's operation-fenced bootstrap contract.
	SubstrateCredentialSeeder func(ctx context.Context, routeHost, nonce string, capabilitySecret []byte, request harnessv2.CredentialBootstrapRequest) error
	// WorkspaceCredentialSeeder overrides the Agent Sandbox credential
	// bootstrap PUT for tests. Production seeds the exact attested Pod endpoint
	// directly after provider materialization is verified.
	WorkspaceCredentialSeeder func(ctx context.Context, endpoint, nonce string, capabilitySecret []byte, request harnessv2.CredentialBootstrapRequest) (alreadyComplete bool, err error)

	SupervisorClient RuntimePoolSupervisorClient
	HTTPClient       *http.Client
	Rand             io.Reader
	Now              func() time.Time

	substrateSupervisorOnce  sync.Once
	substrateSupervisorHTTP  *http.Client
	substrateSupervisorSetup error
}

// +kubebuilder:rbac:groups=core.orka.ai,resources=runtimepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.orka.ai,resources=runtimepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.orka.ai,resources=runtimepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;services;secrets;namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile converges the singleton Deployment and publishes exact-Pod pool status.
func (r *RuntimePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	pool := &corev1alpha1.RuntimePool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			orkametrics.DeleteACPRuntimePool(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !pool.DeletionTimestamp.IsZero() {
		orkametrics.DeleteACPRuntimePool(pool.Namespace, pool.Name)
		if pool.Spec.ExecutionWorkspace != nil && !runtimePoolWorkspaceDeletionDrainComplete(pool) {
			return r.reconcileDeletingWorkspaceRuntimePool(ctx, pool)
		}
		return r.finalizeRuntimePool(ctx, pool)
	}
	if r.CleanupOnly {
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(pool, runtimePoolFinalizer) {
		base := pool.DeepCopy()
		controllerutil.AddFinalizer(pool, runtimePoolFinalizer)
		if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
			return ctrl.Result{}, err
		}
	}

	cfg, err := r.runtimePoolConfig(pool)
	if err != nil && acpRuntimePoolImageRequiresHistoricalRecovery(pool, r.AllowedImages) {
		if historicalConfig, historicalErr := r.runtimePoolConfigForDrain(pool); historicalErr == nil {
			authorized, authorizationErr := r.historicalRuntimePoolImageAuthorized(ctx, pool, historicalConfig)
			if authorizationErr != nil {
				return ctrl.Result{}, authorizationErr
			}
			if authorized {
				cfg = historicalConfig
				err = nil
			}
		}
	}
	if err != nil {
		if pool.Spec.ExecutionWorkspace != nil {
			preserveFence, preserveErr := r.workspacePoolFailureRequiresDurableStatePreservation(ctx, pool)
			if preserveErr != nil {
				return ctrl.Result{}, errors.Join(err, fmt.Errorf("check linked workspace suspension intent: %w", preserveErr))
			}
			if preserveFence {
				return r.finishWorkspacePoolFailureWithPreservedDurableState(
					ctx, pool, "runtime configuration failed", err,
				)
			}
		}
		status := r.baseRuntimePoolStatus(pool, 0)
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage(err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	logger.Info("Reconciling RuntimePool", "runtimePool", pool.Name, "runtimeNamespace", cfg.namespace, "desiredReplicas", pool.Spec.DesiredReplicas)

	if runtimePoolIsSubstrateBacked(pool) {
		return r.reconcileSubstrateBackedRuntimePool(ctx, pool, cfg)
	}

	if err := r.ensureRuntimePoolNamespace(ctx, cfg); err != nil {
		return r.finishWorkspacePoolPrerequisiteFailure(ctx, pool, cfg, "runtime namespace prerequisite failed", err)
	}
	authSecret, providerSecret, err := r.ensureRuntimePoolSecrets(ctx, pool, cfg)
	if err != nil {
		if errors.Is(err, errWorkspaceRuntimePoolAuthBindingLost) {
			return r.reconcileWorkspaceRuntimePoolMissingAuthSecret(ctx, pool, cfg)
		}
		return r.finishWorkspacePoolPrerequisiteFailure(ctx, pool, cfg, "runtime credential prerequisite failed", err)
	}
	if err := r.ensureRuntimePoolAncillaryResources(ctx, pool, cfg); err != nil {
		return r.finishWorkspacePoolPrerequisiteFailure(ctx, pool, cfg, "runtime ancillary-resource prerequisite failed", err)
	}
	if pool.Spec.ExecutionWorkspace != nil {
		return r.reconcileWorkspaceBackedRuntimePool(ctx, pool, cfg, authSecret, providerSecret)
	}

	selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
	desiredTemplate := r.runtimePoolPodTemplate(pool, cfg, selector, authSecret.Name, providerSecret.Name)
	existingDeployment := &appsv1.Deployment{}
	existingErr := r.Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: cfg.baseName}, existingDeployment)
	if existingErr != nil && !apierrors.IsNotFound(existingErr) {
		return ctrl.Result{}, existingErr
	}
	if apierrors.IsNotFound(existingErr) {
		existingDeployment = nil
	}

	pods, err := r.listRuntimePoolPods(ctx, cfg)
	if err != nil {
		return ctrl.Result{}, err
	}
	if existingDeployment != nil && runtimePoolDeploymentNeedsRollout(existingDeployment, desiredTemplate) {
		return r.reconcileRuntimePoolRollout(ctx, pool, cfg, existingDeployment, pods, desiredTemplate)
	}

	targetReplicas := pool.Spec.DesiredReplicas
	if targetReplicas == 0 && existingDeployment != nil && ptr.Deref(existingDeployment.Spec.Replicas, 0) > 0 {
		// A live in-memory pool stays at one until authenticated drain and a
		// separately observed Quiescent status form the scale-down barrier.
		targetReplicas = 1
	}
	deployment, err := r.ensureRuntimePoolDeployment(ctx, pool, cfg, desiredTemplate, targetReplicas, existingDeployment == nil)
	if err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}
	if err := r.pruneStaleRuntimePoolSecrets(ctx, pool, cfg, deployment, authSecret.Name, providerSecret.Name); err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}
	status := r.baseRuntimePoolStatus(pool, countRuntimePoolPods(pods))
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionPodSecurityReady, metav1.ConditionTrue, "PodSecurityConfigured", "runtime Pod security controls are configured")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionQuotaReady, metav1.ConditionTrue, "ResourcesAdmitted", "runtime resources were admitted")
	r.applyDeploymentFailureConditions(pool, deployment, &status)

	readyPods := readyRuntimePoolPods(pods)
	if len(readyPods) > 1 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleAmbiguous
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionAmbiguous
		status.ActiveInstance = nil
		status.Message = fmt.Sprintf("found %d Ready runtime Pods; exact-instance admission is closed", len(readyPods))
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRuntimeAmbiguous, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRuntimeAmbiguous, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if pool.Spec.DesiredReplicas == 0 {
		return r.reconcileRuntimePoolScaleDown(ctx, pool, cfg, deployment, pods, readyPods, authSecret, status)
	}
	return r.reconcileRuntimePoolServing(ctx, pool, cfg, pods, readyPods, authSecret, status)
}

func runtimePoolWorkspaceDeletionDrainComplete(pool *corev1alpha1.RuntimePool) bool {
	return pool != nil && pool.Status.ObservedGeneration == pool.Generation &&
		pool.Status.DesiredReplicas == 0 && pool.Status.CurrentReplicas == 0 &&
		pool.Status.ActiveInstance == nil && pool.Status.Lifecycle == corev1alpha1.RuntimePoolLifecycleStopped
}

// reconcileDeletingWorkspaceRuntimePool routes deletion through the same
// authenticated scale-to-zero state machine as idle shutdown. The local spec
// override is never persisted; it only closes admission and proves quiescence
// before finalization removes the provider workload and isolation boundary.
func (r *RuntimePoolReconciler) reconcileDeletingWorkspaceRuntimePool(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) (ctrl.Result, error) {
	draining := pool.DeepCopy()
	draining.Spec.DesiredReplicas = 0
	cfg, err := r.runtimePoolConfigForDrain(draining)
	if err != nil {
		return ctrl.Result{}, err
	}
	if runtimePoolIsSubstrateBacked(draining) {
		return r.reconcileSubstrateBackedRuntimePool(ctx, draining, cfg)
	}
	if err := r.ensureRuntimePoolNamespace(ctx, cfg); err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, draining, cfg, err)
	}
	authSecret, providerSecret, err := r.ensureRuntimePoolSecrets(ctx, draining, cfg)
	if err != nil {
		if errors.Is(err, errWorkspaceRuntimePoolAuthBindingLost) {
			return r.reconcileWorkspaceRuntimePoolMissingAuthSecret(ctx, draining, cfg)
		}
		return r.finishRuntimePoolResourceFailure(ctx, draining, cfg, err)
	}
	return r.reconcileWorkspaceBackedRuntimePool(ctx, draining, cfg, authSecret, providerSecret)
}

type runtimePoolConfig struct {
	namespace           string
	baseName            string
	labels              map[string]string
	controllerEpoch     int64
	maxResidentSessions int32
	maxRunningPrompts   int32
	protocol            corev1alpha1.RuntimePoolProtocolVersion
	profile             harnessv2.RuntimeProfile
	providerProxy       runtimePoolProviderProxyConfig
}

type runtimePoolBootstrapInstanceBinding struct {
	AuthSecretUID types.UID `json:"authSecretUID"`
	WorkloadUID   types.UID `json:"workloadUID"`
}

func (r *RuntimePoolReconciler) runtimePoolConfig(pool *corev1alpha1.RuntimePool) (runtimePoolConfig, error) {
	return r.runtimePoolConfigWithImageAdmission(pool, true)
}

// runtimePoolConfigForDrain reconstructs the exact pool configuration without
// applying the current-image allowlist. Callers outside deletion must first
// prove that the historical image was authorized for this exact pool object.
func (r *RuntimePoolReconciler) runtimePoolConfigForDrain(pool *corev1alpha1.RuntimePool) (runtimePoolConfig, error) {
	return r.runtimePoolConfigWithImageAdmission(pool, false)
}

func (r *RuntimePoolReconciler) historicalRuntimePoolImageAuthorized(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (bool, error) {
	condition := meta.FindStatusCondition(pool.Status.Conditions, acpRuntimePoolImageProvenanceCondition)
	if condition != nil && condition.Status == metav1.ConditionTrue &&
		condition.ObservedGeneration > 0 && condition.ObservedGeneration <= pool.Generation &&
		condition.Reason == acpRuntimePoolImageProvenanceReason {
		// RuntimePool UID plus the CRD's immutable runtime image/profile,
		// trust-domain, runtime-namespace, and workspace binding keep this proof
		// attached to one exact workload identity. Controller-owned replica and
		// capacity changes may advance generation without invalidating it.
		return true, nil
	}
	if pool.Spec.ExecutionWorkspace != nil {
		return r.backfillHistoricalWorkspaceRuntimePoolImageProvenance(ctx, pool)
	}

	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: cfg.baseName}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for key, value := range map[string]string{
		runtimePoolManagedByLabel: runtimePoolManagedByLabelValue,
		runtimePoolUIDLabel:       string(pool.UID),
		runtimePoolNameLabel:      pool.Name,
		runtimePoolNamespaceLabel: pool.Namespace,
	} {
		if deployment.Labels[key] != value || deployment.Spec.Template.Labels[key] != value {
			return false, nil
		}
	}
	return historicalRuntimePoolTemplateMatches(pool, cfg, deployment.Spec.Template), nil
}

func (r *RuntimePoolReconciler) backfillHistoricalWorkspaceRuntimePoolImageProvenance(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	tasks := &corev1alpha1.TaskList{}
	if err := reader.List(ctx, tasks, client.InNamespace(pool.Namespace)); err != nil {
		return false, fmt.Errorf("list Tasks for historical workspace RuntimePool image provenance: %w", err)
	}
	message := ""
	for i := range tasks.Items {
		if !historicalWorkspaceRuntimePoolTaskBindingMatches(&tasks.Items[i], pool) {
			continue
		}
		message = "RuntimePool image and profile match an exact controller-written Task execution binding"
		break
	}
	if message == "" {
		matches, err := r.historicalWorkspaceRuntimePoolSessionLineageMatches(ctx, reader, pool)
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
		}
		message = "RuntimePool image and profile match an admitted workspace and immutable Session lineage"
	}
	r.setRuntimePoolCondition(
		pool,
		&pool.Status,
		acpRuntimePoolImageProvenanceCondition,
		metav1.ConditionTrue,
		acpRuntimePoolImageProvenanceReason,
		message,
	)
	return true, nil
}

func historicalWorkspaceRuntimePoolTaskBindingMatches(
	task *corev1alpha1.Task,
	pool *corev1alpha1.RuntimePool,
) bool {
	if task == nil || pool == nil || task.Namespace != pool.Namespace || task.UID == "" || pool.UID == "" {
		return false
	}
	execution := task.Status.Execution
	binding := task.Status.AgentExecutionBinding
	if execution == nil || binding == nil ||
		strings.TrimSpace(execution.RuntimePoolName) != pool.Name ||
		strings.TrimSpace(execution.RuntimePoolUID) != string(pool.UID) ||
		binding.SchemaVersion != 1 ||
		binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		binding.Backend != corev1alpha1.AgentExecutionBackendRuntimePool ||
		binding.Task.UID != task.UID || binding.Task.BoundSpecGeneration != task.Generation ||
		binding.RuntimeType != corev1alpha1.AgentRuntimeType(pool.Spec.Runtime.Profile.ProviderKind) ||
		binding.RuntimeProfileDigest != pool.Spec.Runtime.Profile.Digest ||
		binding.RuntimeProfileDigestSchemaVersion != 1 {
		return false
	}
	canonicalDigest, err := canonicalAgentExecutionBindingDigest(*binding)
	return err == nil && canonicalDigest == binding.BindingDigest
}

func (r *RuntimePoolReconciler) historicalWorkspaceRuntimePoolSessionLineageMatches(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
) (bool, error) {
	// Session workspaces may outlive every Task that used them. The core
	// admission marker and condition prove the linked workspace was admitted by
	// Orka, while the immutable Session UID, deterministic pool name, and
	// append-once lineage digest bind that workspace to this historical runtime
	// image, profile, and workspace configuration.
	sessionName, sessionUID, matched, err := historicalWorkspaceRuntimePoolSessionIdentity(ctx, reader, pool)
	if err != nil || !matched {
		return false, err
	}
	return historicalWorkspaceRuntimePoolSessionControlMatches(ctx, reader, pool, sessionName, sessionUID)
}

func historicalWorkspaceRuntimePoolSessionIdentity(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
) (string, string, bool, error) {
	workspaceName := strings.TrimSpace(pool.Labels[acpExecutionWorkspaceLinkLabel])
	workspaceUID := strings.TrimSpace(pool.Annotations[acpExecutionWorkspaceUIDAnnotation])
	if workspaceName == "" || workspaceUID == "" || pool.Spec.ExecutionWorkspace == nil {
		return "", "", false, nil
	}
	linkedWorkspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: workspaceName}, linkedWorkspace); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("read linked ExecutionWorkspace for historical RuntimePool image provenance: %w", err)
	}
	if linkedWorkspace.UID == "" || string(linkedWorkspace.UID) != workspaceUID ||
		linkedWorkspace.Labels[workspacev1alpha1.ProviderControllerLabel] != acpWorkspaceControllerLabelValue ||
		linkedWorkspace.Annotations[acpExecutionWorkspacePoolAnnotation] != pool.Name ||
		linkedWorkspace.Annotations[acpWorkspaceBackendAnnotation] != string(pool.Spec.ExecutionWorkspace.Provider) ||
		linkedWorkspace.Spec.Mode != workspacev1alpha1.ExecutionWorkspaceModeInteractive ||
		linkedWorkspace.Spec.SessionRef == nil || !workspaceHasCoreAdmissionEvidence(linkedWorkspace) {
		return "", "", false, nil
	}
	sessionName := strings.TrimSpace(linkedWorkspace.Spec.SessionRef.Name)
	sessionUID := strings.TrimSpace(string(linkedWorkspace.Spec.SessionRef.UID))
	workspaceSlot := strings.TrimSpace(linkedWorkspace.Spec.Slot)
	if workspaceSlot == "" {
		workspaceSlot = defaultWorkspaceSlotName
	}
	if sessionName == "" || sessionUID == "" {
		return "", "", false, nil
	}
	poolIdentity, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		acpWorkspaceSessionUIDMapKey: sessionUID,
		acpWorkspaceSlotMapKey:       workspaceSlot,
	})
	if err != nil {
		return "", "", false, err
	}
	matched := pool.Name == acpWorkspaceRuntimePoolName("session", harnessv2.ProfileDigest(poolIdentity))
	return sessionName, sessionUID, matched, nil
}

func historicalWorkspaceRuntimePoolSessionControlMatches(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
	sessionName string,
	sessionUID string,
) (bool, error) {
	sessionControl := &corev1alpha1.RuntimeSessionControl{}
	controlKey := types.NamespacedName{
		Namespace: pool.Namespace,
		Name:      storekube.RuntimeSessionControlObjectName(sessionName),
	}
	if err := reader.Get(ctx, controlKey, sessionControl); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read RuntimeSessionControl for historical RuntimePool image provenance: %w", err)
	}
	lineage := sessionControl.Status.Lineage
	if sessionControl.Spec.SessionName != sessionName || sessionControl.Spec.SessionUID != sessionUID ||
		sessionControl.Spec.Owner.Kind != "Session" || sessionControl.Spec.Owner.UID != sessionUID ||
		sessionControl.Status.Version < 1 || lineage == nil || lineage.SessionUID != sessionUID || lineage.Generation < 1 ||
		lineage.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		lineage.RuntimeIdentity != pool.Spec.Runtime.Profile.ProviderKind || lineage.EstablishedAt.IsZero() {
		return false, nil
	}
	if ref := strings.TrimSpace(sessionControl.Spec.RuntimePoolRef); ref != "" && ref != pool.Name {
		return false, nil
	}
	if uid := strings.TrimSpace(sessionControl.Spec.RuntimePoolUID); uid != "" && uid != string(pool.UID) {
		return false, nil
	}
	if digest := strings.TrimSpace(sessionControl.Spec.RuntimeProfileDigest); digest != "" && digest != pool.Spec.Runtime.Profile.Digest {
		return false, nil
	}

	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: pool.Namespace}, namespace); err != nil {
		return false, fmt.Errorf("read namespace identity for historical RuntimePool image provenance: %w", err)
	}
	if namespace.UID == "" || lineage.NamespaceUID != namespace.UID {
		return false, nil
	}
	expectedLineage, err := acpSessionLineageConfigurationDigest(
		pool.Spec.Runtime.Profile.Digest,
		pool.Spec.Runtime.Image,
		pool.Spec.ExecutionWorkspace.BindingDigest,
	)
	if err != nil {
		return false, err
	}
	return lineage.ConfigDigest == expectedLineage, nil
}

func historicalRuntimePoolTemplateMatches(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	template corev1.PodTemplateSpec,
) bool {
	if pool == nil || len(template.Spec.Containers) != 1 ||
		template.Spec.Containers[0].Image != pool.Spec.Runtime.Image {
		return false
	}
	deployedPool, deployedConfig, err := runtimePoolValidationTargetFromTemplate(pool, template)
	if err != nil {
		return false
	}
	return deployedPool.Generation <= pool.Generation &&
		deployedPool.Spec.Runtime.Profile.Digest == pool.Spec.Runtime.Profile.Digest &&
		deployedConfig.protocol == cfg.protocol &&
		reflect.DeepEqual(deployedConfig.profile, cfg.profile)
}

func (r *RuntimePoolReconciler) runtimePoolConfigWithImageAdmission(
	pool *corev1alpha1.RuntimePool,
	enforceApprovedImage bool,
) (runtimePoolConfig, error) {
	if err := validateRuntimePoolObject(pool); err != nil {
		return runtimePoolConfig{}, err
	}
	if enforceApprovedImage {
		if err := r.validateRuntimePoolImage(pool); err != nil {
			return runtimePoolConfig{}, err
		}
	} else if err := validateRuntimePoolImageReference(pool); err != nil {
		return runtimePoolConfig{}, err
	}
	profile, protocol, err := validateRuntimePoolProfile(pool)
	if err != nil {
		return runtimePoolConfig{}, err
	}
	if err := validateRuntimePoolExecutionWorkspace(pool); err != nil {
		return runtimePoolConfig{}, err
	}
	namespace, err := r.runtimePoolNamespace(pool)
	if err != nil {
		return runtimePoolConfig{}, err
	}
	if err := validateRuntimePoolExecutionWorkspaceNamespace(pool, namespace); err != nil {
		return runtimePoolConfig{}, err
	}
	epoch := r.effectiveControllerEpoch(pool)
	maxSessions, maxPrompts, err := runtimePoolCapacity(pool)
	if err != nil {
		return runtimePoolConfig{}, err
	}
	providerProxy, err := r.ProviderProxy.normalized()
	if err != nil {
		return runtimePoolConfig{}, err
	}
	if err := validateRuntimePoolControllerAPIURL(r.ControllerAPIURL); err != nil {
		return runtimePoolConfig{}, err
	}
	if r.WorkspaceArtifactMaxBytes <= 0 {
		return runtimePoolConfig{}, fmt.Errorf("publisher workspace artifact limit must be positive")
	}

	baseName := runtimePoolResourceName(pool.Namespace, pool.Name)
	labels := map[string]string{
		runtimePoolManagedByLabel:   runtimePoolManagedByLabelValue,
		runtimePoolApplicationLabel: runtimePoolApplicationLabelValue,
		runtimePoolKeyLabel:         runtimePoolKey(pool.Namespace, pool.Name),
		runtimePoolNameLabel:        pool.Name,
		runtimePoolNamespaceLabel:   pool.Namespace,
		runtimePoolUIDLabel:         string(pool.UID),
		runtimePoolNetworkRoleLabel: "provider-client",
	}
	return runtimePoolConfig{
		namespace: namespace, baseName: baseName, labels: labels, controllerEpoch: epoch,
		maxResidentSessions: maxSessions, maxRunningPrompts: maxPrompts, protocol: protocol, profile: profile,
		providerProxy: providerProxy,
	}, nil
}

func validateRuntimePoolObject(pool *corev1alpha1.RuntimePool) error {
	if pool == nil {
		return fmt.Errorf("RuntimePool is required")
	}
	if pool.UID == "" {
		return fmt.Errorf("RuntimePool UID is required")
	}
	if pool.Generation <= 0 {
		return fmt.Errorf("RuntimePool generation must be positive")
	}
	if pool.Labels[acpRuntimePoolLabel] != scheduledRunLabelValue {
		return fmt.Errorf("RuntimePool is not controller-owned")
	}
	if pool.Spec.DesiredReplicas < 0 || pool.Spec.DesiredReplicas > 1 {
		return fmt.Errorf("spec.desiredReplicas must be zero or one")
	}
	return nil
}

func (r *RuntimePoolReconciler) validateRuntimePoolImage(pool *corev1alpha1.RuntimePool) error {
	if err := validateRuntimePoolImageReference(pool); err != nil {
		return err
	}
	image := strings.TrimSpace(pool.Spec.Runtime.Image)
	allowedImage, err := configuredACPRuntimeImage(pool.Spec.Runtime.Profile.ProviderKind, r.AllowedImages)
	if err != nil {
		return err
	}
	if allowedImage == "" || image != allowedImage {
		return fmt.Errorf("spec.runtime.image is not the controller-approved image for provider %q", pool.Spec.Runtime.Profile.ProviderKind)
	}
	return nil
}

func validateRuntimePoolImageReference(pool *corev1alpha1.RuntimePool) error {
	image := strings.TrimSpace(pool.Spec.Runtime.Image)
	if !digestPinnedImagePattern.MatchString(image) {
		return fmt.Errorf("spec.runtime.image must be pinned by sha256 digest")
	}
	return nil
}

func validateRuntimePoolProfile(pool *corev1alpha1.RuntimePool) (harnessv2.RuntimeProfile, corev1alpha1.RuntimePoolProtocolVersion, error) {
	if !validSHA256Digest(pool.Spec.Runtime.Profile.Digest) {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.digest must be a sha256 digest")
	}
	if strings.TrimSpace(pool.Spec.Runtime.Profile.DigestSchemaVersion) == "" {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.digestSchemaVersion is required")
	}
	if !runtimePoolDigestSchemaMatches(pool.Spec.Runtime.Profile.DigestSchemaVersion, harnessv2.ProfileDigestSchemaVersion) {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.digestSchemaVersion is unsupported")
	}

	profile, err := runtimePoolHarnessProfile(pool.Spec.Runtime.Profile)
	if err != nil {
		return harnessv2.RuntimeProfile{}, "", err
	}
	if profile.ResourceClass != runtimePoolResourceClassStandard {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.resourceClass %q is not supported", profile.ResourceClass)
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("canonicalize runtime profile: %w", err)
	}
	if string(profileDigest) != pool.Spec.Runtime.Profile.Digest {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.digest does not match the explicit immutable profile")
	}

	protocol := pool.Spec.Runtime.Profile.ProtocolVersion
	if protocol == "" {
		protocol = corev1alpha1.RuntimePoolProtocolHarnessV2
	}
	if protocol != corev1alpha1.RuntimePoolProtocolHarnessV2 {
		return harnessv2.RuntimeProfile{}, "", fmt.Errorf("spec.runtime.profile.protocolVersion must be %q", corev1alpha1.RuntimePoolProtocolHarnessV2)
	}
	return profile, protocol, nil
}

func (r *RuntimePoolReconciler) runtimePoolNamespace(pool *corev1alpha1.RuntimePool) (string, error) {
	requested := strings.TrimSpace(pool.Spec.RuntimeNamespace)
	configured := strings.TrimSpace(r.RuntimeNamespace)
	// The controller creates the pool's Deployment, Secrets, Service,
	// NetworkPolicy, and PDB in the physical runtime namespace on the caller's
	// behalf. A caller with only namespaced RuntimePool-create rights must not
	// be able to steer those controller-owned resources into an arbitrary
	// namespace, so an explicit spec.runtimeNamespace is accepted only when it
	// matches the controller-configured runtime namespace or the pool's own
	// namespace; anything else is rejected rather than reconciled.
	allowed := map[string]struct{}{pool.Namespace: {}}
	if configured != "" {
		allowed[configured] = struct{}{}
	}
	namespace := requested
	if namespace == "" {
		namespace = configured
	}
	if namespace == "" {
		namespace = pool.Namespace
	}
	if _, ok := allowed[namespace]; !ok {
		return "", fmt.Errorf("spec.runtimeNamespace %q is not permitted; use the pool namespace or the controller-configured runtime namespace", requested)
	}
	if errs := validation.IsDNS1123Label(namespace); len(errs) != 0 {
		return "", fmt.Errorf("runtime namespace is invalid")
	}
	return namespace, nil
}

func runtimePoolCapacity(pool *corev1alpha1.RuntimePool) (int32, int32, error) {
	maxSessions := corev1alpha1.DefaultRuntimePoolMaxResidentSessions
	maxPrompts := corev1alpha1.DefaultRuntimePoolMaxRunningPrompts
	if pool.Spec.Capacity != nil {
		if pool.Spec.Capacity.MaxResidentSessions > 0 {
			maxSessions = pool.Spec.Capacity.MaxResidentSessions
		}
		if pool.Spec.Capacity.MaxRunningPrompts > 0 {
			maxPrompts = pool.Spec.Capacity.MaxRunningPrompts
		}
	}
	if maxPrompts > maxSessions {
		return 0, 0, fmt.Errorf("max running prompts cannot exceed max resident sessions")
	}
	return maxSessions, maxPrompts, nil
}

func validateRuntimePoolControllerAPIURL(rawURL string) error {
	controllerAPI, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || controllerAPI.Host == "" || controllerAPI.User != nil || controllerAPI.RawQuery != "" || controllerAPI.Fragment != "" ||
		(controllerAPI.Scheme != urlSchemeHTTP && controllerAPI.Scheme != urlSchemeHTTPS) {
		return fmt.Errorf("controller artifact API URL is invalid")
	}
	return nil
}

func (r *RuntimePoolReconciler) reconcileRuntimePoolServing(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pods []corev1.Pod,
	readyPods []corev1.Pod,
	authSecret *corev1.Secret,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	return r.reconcileRuntimePoolServingWithPostProbeFence(
		ctx, pool, cfg, pods, readyPods, authSecret, status, nil,
	)
}

type runtimePoolPostProbeFence func(
	context.Context,
	*corev1alpha1.RuntimePoolActiveInstanceStatus,
) (ctrl.Result, bool, error)

func (r *RuntimePoolReconciler) reconcileRuntimePoolServingWithPostProbeFence(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pods []corev1.Pod,
	readyPods []corev1.Pod,
	authSecret *corev1.Secret,
	status corev1alpha1.RuntimePoolStatus,
	postProbeFence runtimePoolPostProbeFence,
) (ctrl.Result, error) {
	if len(readyPods) == 0 {
		if runtimePoolActiveInstancePodPresent(status.ActiveInstance, pods) {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "previously active runtime Pod is not Ready; preserving its exact fence while admission is closed"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, runtimePoolSchedulingReasonPodNotReady, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "waiting for one Ready runtime Pod"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		if reason, message, ok := runtimePoolPodFailure(pods); ok {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.Message = message
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, reason, message)
		}
		if reason, message, ok := runtimePoolSchedulingFailure(pods); ok {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.Message = message
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionFalse, reason, message)
		} else {
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, "SchedulingPending", "waiting for a scheduled runtime Pod")
		}
		if status.Lifecycle == corev1alpha1.RuntimePoolLifecycleStarting && pool.Spec.ExecutionWorkspace != nil {
			r.applyProviderRuntimePoolColdStartStatus(pool, &status, status.Message)
		} else if condition := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady); condition == nil || condition.Reason == "Reconciling" {
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, "RolloutPending", "waiting for runtime rollout")
		}
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	probe, err := r.supervisorClientForPool(pool).Probe(ctx, runtimePoolInstanceEndpoint(pool, &readyPods[0]), string(authSecret.Data[runtimePoolControllerTokenKey]), authSecret.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		if !runtimePoolActiveInstanceMatchesPod(status.ActiveInstance, &readyPods[0]) {
			status.ActiveInstance = nil
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = sanitizeRuntimePoolMessage("authenticated runtime status probe failed: " + err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	active, err := validateRuntimePoolProbeForRollout(pool, cfg, &readyPods[0], probe, r.now())
	if err != nil {
		if !runtimePoolActiveInstanceMatchesPod(status.ActiveInstance, &readyPods[0]) {
			status.ActiveInstance = nil
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = sanitizeRuntimePoolMessage(err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if postProbeFence != nil {
		result, handled, fenceErr := postProbeFence(ctx, active)
		if fenceErr != nil || handled {
			return result, fenceErr
		}
	}
	if runtimePoolSupervisorRestartedInPlace(pool.Status.ActiveInstance, active) {
		return r.reconcileRuntimePoolInPlaceSupervisorRestart(ctx, pool, nil, &readyPods[0], active, status)
	}
	status.ActiveInstance = active
	applyRuntimePoolProbeCapacity(&status, cfg, probe)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected runtime Pod is scheduled")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "ExactInstanceReady", "selected runtime Pod and supervisor profile are ready")
	if probe.Status.SessionIdentityCapacity == nil {
		probe.Status.SessionIdentityCapacity = &harnessv2.SessionIdentityCapacity{Total: 2, Remaining: 1, ExhaustionReserve: 1}
		return r.reconcileRuntimePoolIdentityCapacityRotation(ctx, pool, &readyPods[0], authSecret, active, probe, status)
	}
	if probe.Status.SessionIdentityCapacity.RotationRequired() {
		return r.reconcileRuntimePoolIdentityCapacityRotation(ctx, pool, &readyPods[0], authSecret, active, probe, status)
	}

	switch {
	case probe.Status.Lifecycle == harnessv2.SupervisorLifecycleUnhealthy:
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "runtime supervisor reported unhealthy; recycling exact instance"
		if err := r.recycleRuntimePoolInstance(ctx, pool, &readyPods[0]); err != nil {
			status.Message = sanitizeRuntimePoolMessage("runtime supervisor reported unhealthy; exact instance recycling failed: " + err.Error())
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.ActiveInstance = nil
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, runtimePoolSchedulingReasonPodNotReady, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
	case probe.Status.Drain.Requested || probe.Status.Lifecycle == harnessv2.SupervisorLifecycleDraining || probe.Status.Lifecycle == harnessv2.SupervisorLifecycleTerminating:
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = "runtime supervisor is draining"
	case probe.Status.Lifecycle == harnessv2.SupervisorLifecycleBooting:
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "runtime supervisor is booting"
	case runtimePoolAtCapacity(status.Capacity):
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleServing
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "runtime pool is at configured capacity"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAtCapacity, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	default:
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleServing
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionAccepting
		status.Message = "one exact runtime instance is ready"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionTrue, "Serving", status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
}

func (r *RuntimePoolReconciler) reconcileRuntimePoolInPlaceSupervisorRestart(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	rolloutDeployment *appsv1.Deployment,
	pod *corev1.Pod,
	observed *corev1alpha1.RuntimePoolActiveInstanceStatus,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	status.ActiveInstance = pool.Status.ActiveInstance
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "runtime supervisor restarted within the selected Pod; admission is closed before exact Pod replacement"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected runtime Pod remains scheduled")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolSupervisorRestartReasonDetected, status.Message)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, runtimePoolSupervisorRestartReasonDetected, status.Message)
	if !runtimePoolSupervisorRestartAdmissionClosurePersisted(pool, observed) {
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if rolloutDeployment != nil {
		if err := r.stopRuntimePoolDeployment(ctx, rolloutDeployment); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.recycleRuntimePoolInstance(ctx, pool, pod); err != nil {
		return ctrl.Result{}, err
	}
	status.ActiveInstance = nil
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.Message = "runtime Pod with an in-place supervisor restart is stopping for controller-owned emptyDir replacement"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolSupervisorRestartReasonStopping, status.Message)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolSupervisorRestartReasonStopping, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func runtimePoolActiveInstanceMatchesPod(active *corev1alpha1.RuntimePoolActiveInstanceStatus, pod *corev1.Pod) bool {
	return active != nil && pod != nil && strings.TrimSpace(active.PodUID) != "" && active.PodUID == string(pod.UID)
}

func runtimePoolActiveInstancePodPresent(active *corev1alpha1.RuntimePoolActiveInstanceStatus, pods []corev1.Pod) bool {
	for i := range pods {
		if runtimePoolActiveInstanceMatchesPod(active, &pods[i]) {
			return true
		}
	}
	return false
}

func runtimePoolSupervisorRestartedInPlace(previous, observed *corev1alpha1.RuntimePoolActiveInstanceStatus) bool {
	if previous == nil || observed == nil || strings.TrimSpace(previous.PodUID) == "" || previous.PodUID != observed.PodUID {
		return false
	}
	return previous.BootID != observed.BootID || previous.RuntimeInstanceID != observed.RuntimeInstanceID
}

func runtimePoolSupervisorRestartAdmissionClosurePersisted(
	pool *corev1alpha1.RuntimePool,
	observed *corev1alpha1.RuntimePoolActiveInstanceStatus,
) bool {
	if pool == nil || pool.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionClosed ||
		!runtimePoolSupervisorRestartedInPlace(pool.Status.ActiveInstance, observed) {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionAdmissionReady)
	return condition != nil && condition.ObservedGeneration == pool.Generation &&
		condition.Status == metav1.ConditionFalse && condition.Reason == runtimePoolSupervisorRestartReasonDetected
}

func (r *RuntimePoolReconciler) reconcileRuntimePoolIdentityCapacityRotation(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	pod *corev1.Pod,
	authSecret *corev1.Secret,
	active *corev1alpha1.RuntimePoolActiveInstanceStatus,
	probe RuntimePoolProbeResult,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	identityCapacity := probe.Status.SessionIdentityCapacity
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
	status.Message = fmt.Sprintf(
		"session identity capacity reached its replacement watermark (%d remaining, %d reserved)",
		identityCapacity.Remaining,
		identityCapacity.ExhaustionReserve,
	)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
	if !probe.Status.Drain.Requested && !runtimePoolIdentityCapacityAdmissionClosurePersisted(pool, active) {
		status.Message = "session identity admission is closed; persisting exact-instance admission barrier before drain"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolRolloutControllerWorkIsQuiescent(pool.Status.Capacity) {
		status.Message = "session identity admission is closed; waiting for controller reservations or finalization work to settle before drain"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if !probe.Status.Drain.Requested {
		if err := r.supervisorClientForPool(pool).RequestDrain(
			ctx,
			runtimePoolInstanceEndpoint(pool, pod),
			string(authSecret.Data[runtimePoolControllerTokenKey]),
			authSecret.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			harnessv2.DrainReasonSessionIdentityCapacity,
		); err != nil {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = sanitizeRuntimePoolMessage("authenticated identity-capacity drain request failed: " + err.Error())
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Message = "session identity admission is closed; authenticated drain requested"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolRolloutProbeIsQuiescent(pool.Status.Capacity, probe.Status) {
		status.Message = "waiting for identity-capacity drain barriers to become quiescent"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if !runtimePoolIdentityCapacityQuiescencePersisted(pool, active) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleQuiescent
		status.Message = "authenticated identity-capacity drain is quiescent; persisting exact-instance replacement barrier"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonQuiescent, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	// Data-only Substrate pools store the allocator high-water with the
	// DurableDir data. Cold-booting the same actor leaves its identity capacity
	// exhausted, while resetting that state would reuse a prior child UID/GID.
	// recycleSubstrateActor therefore records checkpoint loss before teardown;
	// safe rollover requires a separate durable-ownership migration contract.
	if err := r.recycleRuntimePoolInstance(ctx, pool, pod); err != nil {
		return ctrl.Result{}, err
	}
	status.ActiveInstance = nil
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "quiescent identity-limited runtime Pod is stopping for controller-owned replacement"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, runtimePoolIdentityCapacityReasonStopping, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func (r *RuntimePoolReconciler) reconcileRuntimePoolRollout(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	deployment *appsv1.Deployment,
	pods []corev1.Pod,
	desiredTemplate corev1.PodTemplateSpec,
) (ctrl.Result, error) {
	status := r.baseRuntimePoolStatus(pool, countRuntimePoolPods(pods))
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
	status.Message = "runtime template changed; admission is closed before Recreate rollout"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionPodSecurityReady, metav1.ConditionTrue, "PodSecurityConfigured", "runtime Pod security controls are configured")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionQuotaReady, metav1.ConditionTrue, "ResourcesAdmitted", "runtime resources were admitted")
	r.applyDeploymentFailureConditions(pool, deployment, &status)

	readyPods := readyRuntimePoolPods(pods)
	if len(readyPods) > 1 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleAmbiguous
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionAmbiguous
		status.ActiveInstance = nil
		status.Message = fmt.Sprintf("found %d Ready runtime Pods while preparing Recreate rollout", len(readyPods))
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRuntimeAmbiguous, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if ptr.Deref(deployment.Spec.Replicas, 0) == 0 {
		return r.reconcileStoppedRuntimePoolRollout(ctx, pool, cfg, pods, desiredTemplate, status)
	}
	if len(readyPods) == 0 {
		return r.reconcileUnreadyRuntimePoolRollout(ctx, pool, deployment, pods, status)
	}
	return r.reconcileReadyRuntimePoolRollout(ctx, pool, cfg, deployment, &readyPods[0], desiredTemplate, status)
}

func (r *RuntimePoolReconciler) reconcileStoppedRuntimePoolRollout(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pods []corev1.Pod,
	desiredTemplate corev1.PodTemplateSpec,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	if len(pods) > 0 {
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.Message = "waiting for the drained old runtime Pod to terminate before applying the new Recreate template"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, runtimePoolSchedulingReasonPodNotReady, status.Message)
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if _, err := r.ensureRuntimePoolDeployment(ctx, pool, cfg, desiredTemplate, pool.Spec.DesiredReplicas, true); err != nil {
		return r.finishRuntimePoolResourceFailure(ctx, pool, cfg, err)
	}
	status.ActiveInstance = nil
	if pool.Spec.DesiredReplicas == 0 {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
		status.Message = "new runtime template is staged at zero replicas"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "RolloutConverged", status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
	status.Message = "old runtime terminated; starting the new immutable runtime template"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStarting, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func (r *RuntimePoolReconciler) reconcileUnreadyRuntimePoolRollout(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	deployment *appsv1.Deployment,
	pods []corev1.Pod,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	if reason, message, ok := runtimePoolSchedulingFailure(pods); ok {
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionFalse, reason, message)
	} else {
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, runtimePoolSchedulingReasonPodNotReady, "no Ready runtime Pod is available during Recreate rollout")
	}
	if pool.Status.ActiveInstance != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = "cannot authenticate the previous active runtime instance before Recreate rollout"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if !runtimePoolRolloutControllerWorkIsQuiescent(status.Capacity) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.Message = "waiting for controller reservations or finalization work before stopping an unadmitted runtime Pod"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	if err := r.stopRuntimePoolDeployment(ctx, deployment); err != nil {
		return ctrl.Result{}, err
	}
	status.ActiveInstance = nil
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "stopping an unadmitted old runtime Pod before applying the new Recreate template"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func (r *RuntimePoolReconciler) reconcileReadyRuntimePoolRollout(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	deployment *appsv1.Deployment,
	pod *corev1.Pod,
	desiredTemplate corev1.PodTemplateSpec,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	validationPool, validationConfig, err := runtimePoolDeploymentValidationTarget(pool, deployment)
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	authSecret, err := r.runtimePoolDeploymentAuthSecret(ctx, deployment)
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	probe, err := r.supervisorClientForPool(pool).Probe(ctx, runtimePoolInstanceEndpoint(pool, pod), string(authSecret.Data[runtimePoolControllerTokenKey]), authSecret.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated rollout status probe failed: %w", err))
	}
	active, err := validateRuntimePoolProbeForRollout(validationPool, validationConfig, pod, probe, r.now())
	if err != nil {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, err)
	}
	if runtimePoolSupervisorRestartedInPlace(pool.Status.ActiveInstance, active) {
		return r.reconcileRuntimePoolInPlaceSupervisorRestart(ctx, pool, deployment, pod, active, status)
	}
	if !runtimePoolRolloutActiveInstanceMatches(pool.Status.ActiveInstance, active) {
		return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated runtime identity changed before rollout drain"))
	}
	status.ActiveInstance = active
	applyRuntimePoolProbeCapacity(&status, cfg, probe)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected old runtime Pod remains scheduled during rollout drain")

	if !probe.Status.Drain.Requested {
		reason := "runtime_pool_rollout_" + runtimePoolShortRevision(desiredTemplate.Annotations[runtimePoolTemplateRevisionAnnotation])
		if err := r.supervisorClientForPool(pool).RequestDrain(
			ctx,
			runtimePoolInstanceEndpoint(pool, pod),
			string(authSecret.Data[runtimePoolControllerTokenKey]),
			authSecret.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			reason,
		); err != nil {
			return r.finishRuntimePoolRolloutFailure(ctx, pool, status, fmt.Errorf("authenticated rollout drain request failed: %w", err))
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.Message = runtimePoolMessageRolloutDrainRequested
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolRolloutProbeIsQuiescent(status.Capacity, probe.Status) {
		if r.runtimePoolRolloutTimedOut(pool) {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "timed out waiting for authenticated rollout drain barriers; preserving the old runtime Pod"
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, runtimePoolRolloutReasonTimedOut, status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.Message = runtimePoolMessageRolloutSettling
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonDraining, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if !runtimePoolRolloutQuiescencePersisted(pool) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleQuiescent
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageRolloutQuiescent
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonQuiescent, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if err := r.stopRuntimePoolDeployment(ctx, deployment); err != nil {
		return ctrl.Result{}, err
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "quiescent old runtime Deployment is stopping before the Recreate template changes"
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStopping, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func (r *RuntimePoolReconciler) finishRuntimePoolRolloutFailure(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	status corev1alpha1.RuntimePoolStatus,
	err error,
) (ctrl.Result, error) {
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = sanitizeRuntimePoolMessage(err.Error())
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
}

func runtimePoolRolloutControllerWorkIsQuiescent(capacity corev1alpha1.RuntimePoolCapacityStatus) bool {
	return capacity.ReservedSessions == 0 && capacity.ReservedPrompts == 0 &&
		len(capacity.Reservations) == 0 && capacity.FinalizingSessions == 0
}

func runtimePoolRolloutProbeIsQuiescent(controllerCapacity corev1alpha1.RuntimePoolCapacityStatus, status harnessv2.StatusResponse) bool {
	return runtimePoolRolloutControllerWorkIsQuiescent(controllerCapacity) && upgradeDrainSupervisorIsQuiescent(status)
}

func runtimePoolRolloutActiveInstanceMatches(previous, observed *corev1alpha1.RuntimePoolActiveInstanceStatus) bool {
	if previous == nil {
		return true
	}
	return observed != nil && previous.PodNamespace == observed.PodNamespace && previous.PodName == observed.PodName &&
		previous.PodUID == observed.PodUID && previous.BootID == observed.BootID &&
		previous.RuntimeInstanceID == observed.RuntimeInstanceID && previous.ControllerEpoch == observed.ControllerEpoch &&
		previous.ProfileDigest == observed.ProfileDigest &&
		(previous.ProviderTokenGeneration == "" || previous.ProviderTokenGeneration == observed.ProviderTokenGeneration)
}

func runtimePoolRolloutQuiescencePersisted(pool *corev1alpha1.RuntimePool) bool {
	if pool == nil || pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	return condition != nil && condition.ObservedGeneration == pool.Generation && condition.Reason == runtimePoolRolloutReasonQuiescent
}

func runtimePoolIdentityCapacityAdmissionClosurePersisted(
	pool *corev1alpha1.RuntimePool,
	observed *corev1alpha1.RuntimePoolActiveInstanceStatus,
) bool {
	if pool == nil || pool.Status.ActiveInstance == nil ||
		pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleDraining ||
		pool.Status.AdmissionState != corev1alpha1.RuntimePoolAdmissionDraining {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionAdmissionReady)
	return condition != nil && condition.ObservedGeneration == pool.Generation &&
		condition.Reason == runtimePoolIdentityCapacityReasonDraining &&
		runtimePoolRolloutActiveInstanceMatches(pool.Status.ActiveInstance, observed)
}

func runtimePoolIdentityCapacityQuiescencePersisted(
	pool *corev1alpha1.RuntimePool,
	observed *corev1alpha1.RuntimePoolActiveInstanceStatus,
) bool {
	if pool == nil || pool.Status.ActiveInstance == nil || pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionAdmissionReady)
	return condition != nil && condition.ObservedGeneration == pool.Generation &&
		condition.Reason == runtimePoolIdentityCapacityReasonQuiescent &&
		runtimePoolRolloutActiveInstanceMatches(pool.Status.ActiveInstance, observed)
}

func (r *RuntimePoolReconciler) runtimePoolRolloutTimedOut(pool *corev1alpha1.RuntimePool) bool {
	if pool == nil {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	if condition == nil || condition.ObservedGeneration != pool.Generation {
		return false
	}
	if condition.Reason == runtimePoolRolloutReasonTimedOut {
		return true
	}
	if condition.Reason != runtimePoolRolloutReasonDraining {
		return false
	}
	timeout := time.Duration(pool.Spec.ColdStartTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	return !condition.LastTransitionTime.IsZero() && !r.now().Before(condition.LastTransitionTime.Add(timeout))
}

func (r *RuntimePoolReconciler) providerRuntimePoolColdStartTimedOut(pool *corev1alpha1.RuntimePool) bool {
	if pool == nil || pool.Spec.DesiredReplicas == 0 || pool.Spec.ExecutionWorkspace == nil {
		return false
	}
	condition := meta.FindStatusCondition(pool.Status.Conditions, corev1alpha1.RuntimePoolConditionRolloutReady)
	if condition == nil || condition.ObservedGeneration != pool.Generation {
		return false
	}
	if condition.Reason == runtimePoolRolloutReasonTimedOut {
		return true
	}
	if condition.Reason != runtimePoolRolloutReasonStarting {
		return false
	}
	timeout := time.Duration(pool.Spec.ColdStartTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(corev1alpha1.DefaultRuntimePoolColdStartTimeoutSeconds) * time.Second
	}
	return !condition.LastTransitionTime.IsZero() && !r.now().Before(condition.LastTransitionTime.Add(timeout))
}

func (r *RuntimePoolReconciler) applyProviderRuntimePoolColdStartStatus(
	pool *corev1alpha1.RuntimePool,
	status *corev1alpha1.RuntimePoolStatus,
	message string,
) {
	status.ActiveInstance = nil
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	if r.providerRuntimePoolColdStartTimedOut(pool) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.Message = "workspace provider runtime did not become ready before the configured cold-start deadline"
		r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
		r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, runtimePoolRolloutReasonTimedOut, status.Message)
		return
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStarting
	status.Message = message
	r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
	r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionUnknown, runtimePoolRolloutReasonStarting, status.Message)
}

func runtimePoolShortRevision(revision string) string {
	revision = strings.TrimPrefix(strings.TrimSpace(revision), "sha256:")
	if len(revision) > 16 {
		return revision[:16]
	}
	if revision == "" {
		return "unknown"
	}
	return revision
}

func (r *RuntimePoolReconciler) reconcileRuntimePoolScaleDown(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	deployment *appsv1.Deployment,
	pods []corev1.Pod,
	readyPods []corev1.Pod,
	authSecret *corev1.Secret,
	status corev1alpha1.RuntimePoolStatus,
) (ctrl.Result, error) {
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, "runtime pool is scaling down")

	if ptr.Deref(deployment.Spec.Replicas, 0) == 0 {
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		if len(pods) == 0 {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopped
			status.ActiveInstance = nil
			status.Message = runtimePoolMessageStopped
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, "ScaledToZero", status.Message)
			r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "ScaledToZero", status.Message)
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.ActiveInstance = nil
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
		status.Message = "waiting for the quiescent runtime Pod to terminate"
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionUnknown, runtimePoolSchedulingReasonPodNotReady, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if len(readyPods) == 0 {
		status.ActiveInstance = nil
		if pool.Status.ActiveInstance == nil && runtimePoolControllerWorkIsQuiescent(pool.Status.Capacity) {
			if err := r.stopRuntimePoolDeployment(ctx, deployment); err != nil {
				return ctrl.Result{}, err
			}
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = "stopping a runtime Pod that never became active"
			return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.Message = runtimePoolMessageDrainUnauthenticated
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	probe, err := r.supervisorClientForPool(pool).Probe(ctx, runtimePoolInstanceEndpoint(pool, &readyPods[0]), string(authSecret.Data[runtimePoolControllerTokenKey]), authSecret.Data[runtimePoolCapabilitySecretKey])
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage("authenticated drain status probe failed: " + err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	active, err := validateRuntimePoolProbe(pool, cfg, &readyPods[0], probe, r.now())
	if err != nil {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
		status.ActiveInstance = nil
		status.Message = sanitizeRuntimePoolMessage(err.Error())
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, status.Message)
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}
	status.ActiveInstance = active
	applyRuntimePoolProbeCapacity(&status, cfg, probe)
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionSchedulingReady, metav1.ConditionTrue, "PodScheduled", "selected runtime Pod is scheduled")
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionTrue, "ExactInstanceReady", "selected runtime Pod and supervisor profile are ready")

	if !probe.Status.Drain.Requested {
		if err := r.supervisorClientForPool(pool).RequestDrain(
			ctx,
			runtimePoolInstanceEndpoint(pool, &readyPods[0]),
			string(authSecret.Data[runtimePoolControllerTokenKey]),
			authSecret.Data[runtimePoolCapabilitySecretKey],
			probe.Status,
			"runtime_pool_scale_to_zero",
		); err != nil {
			status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
			status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
			status.Message = sanitizeRuntimePoolMessage("authenticated drain request failed: " + err.Error())
			return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
		}
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainRequested
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if !runtimePoolProbeIsQuiescent(pool.Status.Capacity, probe.Status) {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDraining
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainSettling
		return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
	}

	if pool.Status.Lifecycle != corev1alpha1.RuntimePoolLifecycleQuiescent {
		status.Lifecycle = corev1alpha1.RuntimePoolLifecycleQuiescent
		status.AdmissionState = corev1alpha1.RuntimePoolAdmissionDraining
		status.Message = runtimePoolMessageDrainQuiescent
		return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
	}

	if err := r.stopRuntimePoolDeployment(ctx, deployment); err != nil {
		return ctrl.Result{}, err
	}
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleStopping
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.Message = "quiescent runtime Deployment is stopping"
	return r.finishRuntimePoolStatus(ctx, pool, status, time.Second)
}

func (r *RuntimePoolReconciler) ensureRuntimePoolNamespace(ctx context.Context, cfg runtimePoolConfig) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	namespace := &corev1.Namespace{}
	err := reader.Get(ctx, types.NamespacedName{Name: cfg.namespace}, namespace)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: cfg.namespace,
		Labels: map[string]string{
			"app.kubernetes.io/name":             "orka",
			"app.kubernetes.io/component":        "acp-runtime",
			"app.kubernetes.io/managed-by":       "orka",
			"orka.ai/runtime-namespace":          "true",
			"pod-security.kubernetes.io/enforce": "baseline",
			"pod-security.kubernetes.io/warn":    "restricted",
			"pod-security.kubernetes.io/audit":   "restricted",
		},
	}}
	if err := r.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create RuntimePool namespace: %w", err)
	}
	return nil
}

func (r *RuntimePoolReconciler) ensureRuntimePoolSecrets(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (*corev1.Secret, *corev1.Secret, error) {
	if pool.Spec.ExecutionWorkspace != nil {
		return r.ensurePrivateWorkspaceRuntimePoolSecrets(ctx, pool, cfg)
	}

	epoch := strconv.FormatInt(cfg.controllerEpoch, 10)
	authName := runtimePoolChildName(cfg.baseName, "auth-e"+strconv.FormatInt(cfg.controllerEpoch, 10))
	auth, err := r.ensureRuntimePoolSecret(ctx, pool, cfg, authName, map[string]int{
		runtimePoolControllerTokenKey:  32,
		runtimePoolCapabilitySecretKey: 32,
		runtimePoolBootstrapNonceKey:   32,
	}, map[string]string{
		runtimePoolAuthLabel:            "true",
		runtimePoolCredentialEpochLabel: epoch,
	})
	if err != nil {
		return nil, nil, err
	}
	providerName := runtimePoolChildName(cfg.baseName, "provider-e"+strconv.FormatInt(cfg.controllerEpoch, 10)+"-g"+cfg.providerProxy.tokenGeneration)
	provider, err := r.ensureRuntimePoolProviderSecret(ctx, pool, cfg, providerName, map[string]string{
		runtimePoolProviderCredentialLabel: "true",
		runtimePoolCredentialEpochLabel:    epoch,
		runtimePoolProviderGenerationLabel: cfg.providerProxy.tokenGeneration,
	})
	if err != nil {
		return nil, nil, err
	}
	return auth, provider, nil
}

// ensurePrivateWorkspaceRuntimePoolSecrets gives provider-workspace bootstrap
// credentials unpredictable names that never enter provider-visible templates.
// The controller publishes the auth Secret's exact name and immutable UID only
// after creation, then seeds credentials only after exact workload checks.
func (r *RuntimePoolReconciler) ensurePrivateWorkspaceRuntimePoolSecrets(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) (*corev1.Secret, *corev1.Secret, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	epoch := strconv.FormatInt(cfg.controllerEpoch, 10)

	auth, err := r.ensurePrivateWorkspaceRuntimePoolAuthSecret(ctx, pool, cfg, epoch)
	if err != nil {
		return nil, nil, err
	}

	var providerSecrets corev1.SecretList
	if err := reader.List(ctx, &providerSecrets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolManagedByLabel: runtimePoolManagedByLabelValue,
		runtimePoolUIDLabel:       string(pool.UID),
	}); err != nil {
		return nil, nil, err
	}
	providerMatches := runtimePoolProviderSecretsForGeneration(
		providerSecrets.Items, cfg.controllerEpoch, cfg.providerProxy.tokenGeneration,
	)
	if len(providerMatches) > 1 {
		return nil, nil, fmt.Errorf("workspace RuntimePool requires exactly one private provider Secret for controller epoch %d and token generation %s", cfg.controllerEpoch, cfg.providerProxy.tokenGeneration)
	}
	providerName := ""
	if len(providerMatches) == 1 {
		providerName = providerMatches[0].Name
	} else {
		suffix, err := r.randomHex(12)
		if err != nil {
			return nil, nil, fmt.Errorf("generate private RuntimePool provider Secret name: %w", err)
		}
		providerName = runtimePoolChildName(cfg.baseName, "provider-e"+epoch+"-g"+cfg.providerProxy.tokenGeneration+"-"+suffix)
	}
	provider, err := r.ensureRuntimePoolProviderSecret(ctx, pool, cfg, providerName, map[string]string{
		runtimePoolProviderCredentialLabel: "true",
		runtimePoolCredentialEpochLabel:    epoch,
		runtimePoolProviderGenerationLabel: cfg.providerProxy.tokenGeneration,
	})
	if err != nil {
		return nil, nil, err
	}
	return auth, provider, nil
}

func (r *RuntimePoolReconciler) ensurePrivateWorkspaceRuntimePoolAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	epoch string,
) (*corev1.Secret, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	bindingKey := runtimePoolPrivateAuthSecretBindingAnnotation(cfg.controllerEpoch)
	authKeys := map[string]int{
		runtimePoolControllerTokenKey:      32,
		runtimePoolCapabilitySecretKey:     32,
		runtimePoolBootstrapNonceKey:       32,
		runtimePoolBootstrapSigningSeedKey: 32,
	}
	authLabels := map[string]string{
		runtimePoolAuthLabel:            "true",
		runtimePoolCredentialEpochLabel: epoch,
	}
	binding := strings.TrimSpace(pool.Annotations[bindingKey])
	if binding != "" {
		return r.boundPrivateWorkspaceRuntimePoolAuthSecret(ctx, pool, cfg, cfg.controllerEpoch)
	}

	var candidates corev1.SecretList
	if err := reader.List(ctx, &candidates, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolAuthLabel: "true",
		runtimePoolUIDLabel:  string(pool.UID),
	}); err != nil {
		return nil, err
	}
	authoritativePool := &corev1alpha1.RuntimePool{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(pool), authoritativePool); err != nil {
		return nil, fmt.Errorf("refresh RuntimePool before private auth Secret cleanup: %w", err)
	}
	if authoritativePool.UID != pool.UID {
		return nil, fmt.Errorf("RuntimePool UID changed before private auth Secret cleanup")
	}
	if authoritativeBinding := strings.TrimSpace(authoritativePool.Annotations[bindingKey]); authoritativeBinding != "" {
		return r.boundPrivateWorkspaceRuntimePoolAuthSecret(ctx, authoritativePool, cfg, cfg.controllerEpoch)
	}
	matches := runtimePoolAuthSecretsForEpoch(candidates.Items, cfg.controllerEpoch)
	for i := range matches {
		if !runtimePoolPrivateAuthSecretMatchesPool(&matches[i], pool, cfg) {
			return nil, fmt.Errorf("refusing to adopt an unbound private RuntimePool auth Secret for controller epoch %d", cfg.controllerEpoch)
		}
	}
	for i := range matches {
		if err := r.deleteRuntimePoolManagedSecret(ctx, &matches[i]); err != nil {
			return nil, fmt.Errorf("discard unbound private RuntimePool auth Secret: %w", err)
		}
	}

	suffix, err := r.randomHex(12)
	if err != nil {
		return nil, fmt.Errorf("generate private RuntimePool auth Secret name: %w", err)
	}
	name := runtimePoolChildName(cfg.baseName, "auth-e"+epoch+"-"+suffix)
	secret, err := r.createRuntimePoolSecret(ctx, pool, cfg, name, authKeys, authLabels)
	if err != nil {
		return nil, err
	}
	if err := r.bindPrivateRuntimePoolAuthSecret(ctx, pool, bindingKey, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

func (r *RuntimePoolReconciler) boundPrivateWorkspaceRuntimePoolAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	epoch int64,
) (*corev1.Secret, error) {
	bindingKey := runtimePoolPrivateAuthSecretBindingAnnotation(epoch)
	binding := strings.TrimSpace(pool.Annotations[bindingKey])
	if binding == "" {
		return nil, fmt.Errorf("private RuntimePool auth Secret binding for controller epoch %d is missing", epoch)
	}
	name, uid, err := parseRuntimePoolPrivateSecretBinding(binding)
	if err != nil {
		return nil, err
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w for controller epoch %d", errWorkspaceRuntimePoolAuthBindingLost, epoch)
		}
		return nil, fmt.Errorf("read bound private RuntimePool auth Secret: %w", err)
	}
	if secret.UID != uid {
		return nil, fmt.Errorf("bound private RuntimePool auth Secret UID changed")
	}
	deployedConfig := cfg
	deployedConfig.controllerEpoch = epoch
	if !runtimePoolPrivateAuthSecretMatchesPool(secret, pool, deployedConfig) {
		return nil, fmt.Errorf("bound private RuntimePool auth Secret does not carry the exact immutable RuntimePool ownership identity")
	}
	return secret, nil
}

func runtimePoolPrivateAuthSecretBindingAnnotation(epoch int64) string {
	return runtimePoolPrivateAuthBindingPrefix + strconv.FormatInt(epoch, 10)
}

func parseRuntimePoolPrivateSecretBinding(binding string) (string, types.UID, error) {
	name, rawUID, ok := strings.Cut(strings.TrimSpace(binding), "/")
	if !ok || len(validation.IsDNS1123Subdomain(name)) != 0 || strings.TrimSpace(rawUID) == "" {
		return "", "", fmt.Errorf("private RuntimePool auth Secret binding is invalid")
	}
	return name, types.UID(rawUID), nil
}

func runtimePoolPrivateAuthSecretOwnedByPool(
	secret *corev1.Secret,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) bool {
	if !runtimePoolManagedCredentialSecret(secret, cfg) {
		return false
	}
	return secret.Namespace != pool.Namespace || metav1.IsControlledBy(secret, pool)
}

func runtimePoolPrivateAuthSecretMatchesPool(
	secret *corev1.Secret,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) bool {
	return runtimePoolPrivateAuthSecretOwnedByPool(secret, pool, cfg) &&
		len(runtimePoolAuthSecretsForEpoch([]corev1.Secret{*secret}, cfg.controllerEpoch)) == 1
}

func (r *RuntimePoolReconciler) bindPrivateRuntimePoolAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	bindingKey string,
	secret *corev1.Secret,
) error {
	if secret == nil || secret.UID == "" {
		return fmt.Errorf("created private RuntimePool auth Secret has no immutable UID")
	}
	binding := secret.Name + "/" + string(secret.UID)
	current := strings.TrimSpace(pool.Annotations[bindingKey])
	if current != "" && current != binding {
		return fmt.Errorf("private RuntimePool auth Secret binding changed")
	}
	if current == binding {
		return nil
	}
	base := pool.DeepCopy()
	if pool.Annotations == nil {
		pool.Annotations = map[string]string{}
	}
	pool.Annotations[bindingKey] = binding
	if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("record private RuntimePool auth Secret binding: %w", err)
	}
	return nil
}

func runtimePoolBootstrapInstanceBindingFromAnnotation(
	pool *corev1alpha1.RuntimePool,
) (*runtimePoolBootstrapInstanceBinding, error) {
	if pool == nil {
		return nil, fmt.Errorf("RuntimePool is required for bootstrap instance binding")
	}
	raw := strings.TrimSpace(pool.Annotations[runtimePoolBootstrapInstanceBindingAnnotation])
	if raw == "" {
		return nil, nil
	}
	var binding runtimePoolBootstrapInstanceBinding
	if err := json.Unmarshal([]byte(raw), &binding); err != nil || binding.AuthSecretUID == "" || binding.WorkloadUID == "" {
		return nil, fmt.Errorf("RuntimePool bootstrap instance binding is invalid")
	}
	return &binding, nil
}

func (r *RuntimePoolReconciler) bindWorkspaceRuntimePoolBootstrapInstance(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	authSecret *corev1.Secret,
	workloadUID types.UID,
) error {
	if authSecret == nil || authSecret.UID == "" || workloadUID == "" {
		return fmt.Errorf("RuntimePool bootstrap auth Secret and workload UIDs are required")
	}
	desired := runtimePoolBootstrapInstanceBinding{AuthSecretUID: authSecret.UID, WorkloadUID: workloadUID}
	existing, err := runtimePoolBootstrapInstanceBindingFromAnnotation(pool)
	if err != nil {
		return err
	}
	if existing != nil {
		if *existing == desired {
			return nil
		}
		return errRuntimePoolBootstrapInstanceConflict
	}
	value, err := json.Marshal(desired)
	if err != nil {
		return fmt.Errorf("encode RuntimePool bootstrap instance binding: %w", err)
	}
	return r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolBootstrapInstanceBindingAnnotation, string(value))
}

// rotateConsumedWorkspaceRuntimePoolAuthSecret advances private auth Secret
// rotation after a previously bound physical instance is gone. The first pass
// unpublishes the consumed Secret binding; the normal create-before-publish
// path then deletes that unbound Secret and creates fresh credentials. A final
// pass clears the old instance binding before a replacement workload exists.
func (r *RuntimePoolReconciler) rotateConsumedWorkspaceRuntimePoolAuthSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	authSecret *corev1.Secret,
) (bool, error) {
	binding, err := runtimePoolBootstrapInstanceBindingFromAnnotation(pool)
	if err != nil || binding == nil {
		return false, err
	}
	if authSecret == nil || authSecret.UID == "" {
		return false, fmt.Errorf("bound private RuntimePool auth Secret has no immutable UID")
	}
	if authSecret.UID == binding.AuthSecretUID {
		return true, r.patchRuntimePoolAnnotation(
			ctx, pool, runtimePoolPrivateAuthSecretBindingAnnotation(cfg.controllerEpoch), "",
		)
	}
	return true, r.patchRuntimePoolAnnotation(ctx, pool, runtimePoolBootstrapInstanceBindingAnnotation, "")
}

func (r *RuntimePoolReconciler) patchRuntimePoolAnnotation(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	key, value string,
) error {
	current := pool.Annotations[key]
	if current == value || (value == "" && current == "") {
		return nil
	}
	base := pool.DeepCopy()
	if pool.Annotations == nil {
		pool.Annotations = map[string]string{}
	}
	if value == "" {
		delete(pool.Annotations, key)
	} else {
		pool.Annotations[key] = value
	}
	if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("record RuntimePool annotation: %w", err)
	}
	return nil
}

func (r *RuntimePoolReconciler) pruneStaleRuntimePoolSecrets(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	deployment *appsv1.Deployment,
	currentNames ...string,
) error {
	if deployment == nil {
		return nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	liveDeployment := &appsv1.Deployment{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: deployment.Namespace, Name: deployment.Name}, liveDeployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read RuntimePool Deployment for stale credential cleanup: %w", err)
	}
	if liveDeployment.Generation > 0 && liveDeployment.Status.ObservedGeneration < liveDeployment.Generation {
		return nil
	}

	keep := make(map[string]struct{}, len(currentNames)+2)
	for _, name := range currentNames {
		addRuntimeSecretName(keep, name)
	}
	addRuntimePoolSecretReferences(keep, liveDeployment.Spec.Template.Spec)

	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		return fmt.Errorf("list RuntimePool Pods for stale credential cleanup: %w", err)
	}
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded || pods.Items[i].Status.Phase == corev1.PodFailed {
			continue
		}
		addRuntimePoolSecretReferences(keep, pods.Items[i].Spec)
	}

	var replicaSets appsv1.ReplicaSetList
	if err := reader.List(ctx, &replicaSets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel: string(pool.UID),
	}); err != nil {
		return fmt.Errorf("list RuntimePool ReplicaSets for stale credential cleanup: %w", err)
	}
	for i := range replicaSets.Items {
		replicaSet := &replicaSets.Items[i]
		if !runtimePoolReplicaSetCanCreatePods(replicaSet) {
			continue
		}
		addRuntimePoolSecretReferences(keep, replicaSet.Spec.Template.Spec)
	}

	var secrets corev1.SecretList
	if err := reader.List(ctx, &secrets, client.InNamespace(cfg.namespace), client.MatchingLabels{
		runtimePoolManagedByLabel: runtimePoolManagedByLabelValue,
		runtimePoolKeyLabel:       cfg.labels[runtimePoolKeyLabel],
		runtimePoolUIDLabel:       string(pool.UID),
	}); err != nil {
		return fmt.Errorf("list managed RuntimePool Secrets for stale credential cleanup: %w", err)
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if _, current := keep[secret.Name]; current || !runtimePoolManagedCredentialSecret(secret, cfg) {
			continue
		}
		if err := r.deleteRuntimePoolManagedSecret(ctx, secret); err != nil {
			return err
		}
	}
	return nil
}

func runtimePoolReplicaSetCanCreatePods(replicaSet *appsv1.ReplicaSet) bool {
	if replicaSet == nil {
		return false
	}
	return replicaSet.Status.ObservedGeneration < replicaSet.Generation ||
		ptr.Deref(replicaSet.Spec.Replicas, 0) > 0 || replicaSet.Status.Replicas > 0 ||
		replicaSet.Status.FullyLabeledReplicas > 0 || replicaSet.Status.ReadyReplicas > 0 ||
		replicaSet.Status.AvailableReplicas > 0 || ptr.Deref(replicaSet.Status.TerminatingReplicas, 0) > 0
}

func (r *RuntimePoolReconciler) deleteRuntimePoolManagedSecret(ctx context.Context, secret *corev1.Secret) error {
	deleteOptions := make([]client.DeleteOption, 0, 1)
	preconditions := client.Preconditions{}
	if secret.UID != "" {
		uid := secret.UID
		preconditions.UID = &uid
	}
	if secret.ResourceVersion != "" {
		resourceVersion := secret.ResourceVersion
		preconditions.ResourceVersion = &resourceVersion
	}
	if preconditions.UID != nil || preconditions.ResourceVersion != nil {
		deleteOptions = append(deleteOptions, preconditions)
	}
	if err := r.Delete(ctx, secret, deleteOptions...); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete stale managed RuntimePool Secret %q: %w", secret.Name, err)
	}
	return nil
}

func addRuntimePoolSecretReferences(keep map[string]struct{}, podSpec corev1.PodSpec) {
	for i := range podSpec.ImagePullSecrets {
		addRuntimeSecretName(keep, podSpec.ImagePullSecrets[i].Name)
	}
	for i := range podSpec.Volumes {
		addRuntimeVolumeSecretReferences(keep, &podSpec.Volumes[i])
	}
	for i := range podSpec.InitContainers {
		addRuntimeContainerSecretReferences(keep, podSpec.InitContainers[i].Env, podSpec.InitContainers[i].EnvFrom)
	}
	for i := range podSpec.Containers {
		addRuntimeContainerSecretReferences(keep, podSpec.Containers[i].Env, podSpec.Containers[i].EnvFrom)
	}
	for i := range podSpec.EphemeralContainers {
		addRuntimeContainerSecretReferences(keep, podSpec.EphemeralContainers[i].Env, podSpec.EphemeralContainers[i].EnvFrom)
	}
}

func addRuntimeVolumeSecretReferences(keep map[string]struct{}, volume *corev1.Volume) {
	if volume == nil {
		return
	}
	if volume.Secret != nil {
		addRuntimeSecretName(keep, volume.Secret.SecretName)
	}
	if volume.Projected != nil {
		for i := range volume.Projected.Sources {
			if volume.Projected.Sources[i].Secret != nil {
				addRuntimeSecretName(keep, volume.Projected.Sources[i].Secret.Name)
			}
		}
	}
	if volume.ISCSI != nil && volume.ISCSI.SecretRef != nil {
		addRuntimeSecretName(keep, volume.ISCSI.SecretRef.Name)
	}
	if volume.RBD != nil && volume.RBD.SecretRef != nil {
		addRuntimeSecretName(keep, volume.RBD.SecretRef.Name)
	}
	if volume.FlexVolume != nil && volume.FlexVolume.SecretRef != nil {
		addRuntimeSecretName(keep, volume.FlexVolume.SecretRef.Name)
	}
	if volume.Cinder != nil && volume.Cinder.SecretRef != nil {
		addRuntimeSecretName(keep, volume.Cinder.SecretRef.Name)
	}
	if volume.CephFS != nil && volume.CephFS.SecretRef != nil {
		addRuntimeSecretName(keep, volume.CephFS.SecretRef.Name)
	}
	if volume.AzureFile != nil {
		addRuntimeSecretName(keep, volume.AzureFile.SecretName)
	}
	if volume.ScaleIO != nil && volume.ScaleIO.SecretRef != nil {
		addRuntimeSecretName(keep, volume.ScaleIO.SecretRef.Name)
	}
	if volume.StorageOS != nil && volume.StorageOS.SecretRef != nil {
		addRuntimeSecretName(keep, volume.StorageOS.SecretRef.Name)
	}
	if volume.CSI != nil && volume.CSI.NodePublishSecretRef != nil {
		addRuntimeSecretName(keep, volume.CSI.NodePublishSecretRef.Name)
	}
}

func addRuntimeSecretName(keep map[string]struct{}, name string) {
	if name = strings.TrimSpace(name); name != "" {
		keep[name] = struct{}{}
	}
}

func addRuntimeContainerSecretReferences(keep map[string]struct{}, env []corev1.EnvVar, envFrom []corev1.EnvFromSource) {
	for i := range env {
		if env[i].ValueFrom != nil && env[i].ValueFrom.SecretKeyRef != nil {
			addRuntimeSecretName(keep, env[i].ValueFrom.SecretKeyRef.Name)
		}
	}
	for i := range envFrom {
		if envFrom[i].SecretRef != nil {
			addRuntimeSecretName(keep, envFrom[i].SecretRef.Name)
		}
	}
}

func runtimePoolManagedCredentialSecret(secret *corev1.Secret, cfg runtimePoolConfig) bool {
	// RuntimePools may manage a dedicated runtime namespace, where Kubernetes
	// forbids cross-namespace ownerReferences. Exact immutable ownership labels,
	// generated-name reconstruction, and data shape form the ownership boundary.
	if secret == nil || secret.Immutable == nil || !*secret.Immutable {
		return false
	}
	for key, value := range cfg.labels {
		if secret.Labels[key] != value {
			return false
		}
	}
	if suffix := runtimePoolAuthSuffixPattern.FindString(secret.Name); suffix != "" &&
		runtimePoolChildName(cfg.baseName, suffix) == secret.Name {
		if secret.Labels[runtimePoolAuthLabel] != scheduledRunLabelValue ||
			len(secret.Data[runtimePoolControllerTokenKey]) == 0 ||
			len(secret.Data[runtimePoolCapabilitySecretKey]) == 0 {
			return false
		}
		// Pre-workspace RuntimePool auth Secrets had exactly the two control
		// credentials. Keep recognizing that historical shape so epoch rotation
		// can prune it after no live workload references it.
		return len(secret.Data) == 2 ||
			(len(secret.Data) == 3 && len(secret.Data[runtimePoolBootstrapNonceKey]) > 0) ||
			(len(secret.Data) == 4 && len(secret.Data[runtimePoolBootstrapNonceKey]) > 0 &&
				len(secret.Data[runtimePoolBootstrapSigningSeedKey]) >= harnessv2.MinCapabilitySecretBytes)
	}
	if suffix := runtimePoolProviderSuffixPattern.FindString(secret.Name); suffix != "" &&
		runtimePoolChildName(cfg.baseName, suffix) == secret.Name {
		return len(secret.Data) == 1 && len(secret.Data[runtimePoolProviderTokenKey]) > 0
	}
	return false
}

func runtimePoolProviderSecretsForGeneration(
	secrets []corev1.Secret,
	epoch int64,
	generation string,
) []corev1.Secret {
	epochValue := strconv.FormatInt(epoch, 10)
	legacySuffix := "provider-e" + epochValue + "-g" + generation
	randomSuffixPrefix := legacySuffix + "-"
	matched := make([]corev1.Secret, 0, 1)
	for i := range secrets {
		secretEpoch := strings.TrimSpace(secrets[i].Labels[runtimePoolCredentialEpochLabel])
		secretGeneration := strings.TrimSpace(secrets[i].Labels[runtimePoolProviderGenerationLabel])
		labeledMatch := secretEpoch == epochValue && secretGeneration == generation
		legacyMatch := secretEpoch == "" && secretGeneration == "" &&
			(strings.HasSuffix(secrets[i].Name, legacySuffix) ||
				strings.HasPrefix(runtimePoolProviderSuffixPattern.FindString(secrets[i].Name), randomSuffixPrefix))
		if labeledMatch || legacyMatch {
			matched = append(matched, *secrets[i].DeepCopy())
		}
	}
	return matched
}

func (r *RuntimePoolReconciler) ensureRuntimePoolProviderSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	name string,
	extraLabels map[string]string,
) (*corev1.Secret, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secret := &corev1.Secret{}
	// Uncached read: see ensureRuntimePoolSecret.
	err := reader.Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: name}, secret)
	if apierrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.namespace, Labels: mergeStringMap(cloneStringMap(cfg.labels), extraLabels)},
			Type:       corev1.SecretTypeOpaque,
			Immutable:  new(true),
			Data:       map[string][]byte{runtimePoolProviderTokenKey: bytes.Clone(cfg.providerProxy.token)},
		}
		if err := r.setRuntimePoolControllerReference(pool, secret); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, secret); err != nil {
			return nil, fmt.Errorf("create managed RuntimePool provider Secret: %w", err)
		}
		return secret, nil
	}
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(secret.Data[runtimePoolProviderTokenKey], cfg.providerProxy.token) {
		return nil, fmt.Errorf("managed RuntimePool provider Secret does not match the configured authenticated proxy token")
	}
	base := secret.DeepCopy()
	secret.Labels = mergeStringMap(secret.Labels, cfg.labels)
	secret.Labels = mergeStringMap(secret.Labels, extraLabels)
	if err := r.setRuntimePoolControllerReference(pool, secret); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(base.ObjectMeta, secret.ObjectMeta) {
		if err := r.Patch(ctx, secret, client.MergeFrom(base)); err != nil {
			return nil, err
		}
	}
	return secret, nil
}

func (r *RuntimePoolReconciler) ensureRuntimePoolSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	name string,
	keys map[string]int,
	extraLabels map[string]string,
) (*corev1.Secret, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secret := &corev1.Secret{}
	// Uncached read: pool Secrets always live in the runtime namespace, but the
	// namespace-scoped manager cache is configured independently, so a direct
	// read keeps Secret handling correct regardless of cache scope drift.
	err := reader.Get(ctx, types.NamespacedName{Namespace: cfg.namespace, Name: name}, secret)
	if apierrors.IsNotFound(err) {
		return r.createRuntimePoolSecret(ctx, pool, cfg, name, keys, extraLabels)
	}
	if err != nil {
		return nil, err
	}
	for key := range keys {
		value := secret.Data[key]
		if strings.TrimSpace(string(value)) == "" {
			return nil, fmt.Errorf("managed RuntimePool Secret %q is missing required key %q", secret.Name, key)
		}
	}
	base := secret.DeepCopy()
	secret.Labels = mergeStringMap(secret.Labels, cfg.labels)
	secret.Labels = mergeStringMap(secret.Labels, extraLabels)
	if err := r.setRuntimePoolControllerReference(pool, secret); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(base.ObjectMeta, secret.ObjectMeta) {
		if err := r.Patch(ctx, secret, client.MergeFrom(base)); err != nil {
			return nil, err
		}
	}
	return secret, nil
}

func (r *RuntimePoolReconciler) createRuntimePoolSecret(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	name string,
	keys map[string]int,
	extraLabels map[string]string,
) (*corev1.Secret, error) {
	data := make(map[string][]byte, len(keys))
	for key, size := range keys {
		value, err := r.randomSecret(size)
		if err != nil {
			return nil, fmt.Errorf("generate managed RuntimePool secret: %w", err)
		}
		data[key] = []byte(value)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.namespace, Labels: mergeStringMap(cloneStringMap(cfg.labels), extraLabels)},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  new(true),
		Data:       data,
	}
	if err := r.setRuntimePoolControllerReference(pool, secret); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, secret); err != nil {
		return nil, fmt.Errorf("create managed RuntimePool Secret: %w", err)
	}
	return secret, nil
}

func (r *RuntimePoolReconciler) ensureRuntimePoolAncillaryResources(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
) error {
	if err := r.ensureRuntimePoolService(ctx, pool, cfg); err != nil {
		return err
	}
	if err := r.ensureRuntimePoolNetworkPolicies(ctx, pool, cfg); err != nil {
		return err
	}
	return r.ensureRuntimePoolPDB(ctx, pool, cfg)
}

func (r *RuntimePoolReconciler) ensureRuntimePoolDeployment(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	desiredTemplate corev1.PodTemplateSpec,
	replicas int32,
	allowTemplateUpdate bool,
) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: cfg.baseName, Namespace: cfg.namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		creating := deployment.ResourceVersion == ""
		deployment.Labels = mergeStringMap(deployment.Labels, cfg.labels)
		if err := r.setRuntimePoolControllerReference(pool, deployment); err != nil {
			return err
		}
		selector := map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
		if !creating && !allowTemplateUpdate && runtimePoolDeploymentNeedsRollout(deployment, desiredTemplate) {
			return fmt.Errorf("RuntimePool Deployment template requires an authenticated drain before replacement")
		}
		deployment.Spec.Replicas = new(replicas)
		deployment.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
		deployment.Spec.MinReadySeconds = 2
		deployment.Spec.ProgressDeadlineSeconds = new(max(int32(30), pool.Spec.ColdStartTimeoutSeconds))
		deployment.Spec.RevisionHistoryLimit = new(int32(1))
		if creating {
			deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
		}
		if creating || allowTemplateUpdate {
			deployment.Spec.Template = *desiredTemplate.DeepCopy()
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile RuntimePool Deployment: %w", err)
	}
	return deployment, nil
}

func runtimePoolDeploymentNeedsRollout(deployment *appsv1.Deployment, desiredTemplate corev1.PodTemplateSpec) bool {
	if deployment == nil {
		return false
	}
	desiredRevision := strings.TrimSpace(desiredTemplate.Annotations[runtimePoolTemplateRevisionAnnotation])
	deployedRevision := strings.TrimSpace(deployment.Spec.Template.Annotations[runtimePoolTemplateRevisionAnnotation])
	return desiredRevision == "" || deployedRevision != desiredRevision
}

func (r *RuntimePoolReconciler) runtimePoolPodTemplate(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	selector map[string]string,
	authSecretName, providerSecretName string,
) corev1.PodTemplateSpec {
	labels := mergeStringMap(cloneStringMap(cfg.labels), selector)
	annotations := map[string]string{
		runtimePoolProfileAnnotation:                 pool.Spec.Runtime.Profile.Digest,
		runtimePoolProviderTokenGenerationAnnotation: cfg.providerProxy.tokenGeneration,
		// Kubernetes has no portable PodSpec PID limit. This bounded value is
		// consumed by supported runtime/admission integrations when available.
		runtimePoolPIDsAnnotation: "4096",
	}
	zero := int64(0)
	mode := int32(0o400)
	terminationGrace := int64(120)
	adapterDigestsJSON, _ := json.Marshal(cfg.profile.AdapterDigests)
	modelContextLimit := ""
	modelOutputLimit := ""
	if cfg.profile.ModelLimits != nil {
		modelContextLimit = strconv.FormatInt(cfg.profile.ModelLimits.Context, 10)
		modelOutputLimit = strconv.FormatInt(cfg.profile.ModelLimits.Output, 10)
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			NodeSelector:                  map[string]string{"kubernetes.io/os": "linux"},
			AutomountServiceAccountToken:  new(false),
			EnableServiceLinks:            new(false),
			TerminationGracePeriodSeconds: &terminationGrace,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:      &zero,
				RunAsGroup:     &zero,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            "runtime",
				Image:           pool.Spec.Runtime.Image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "control", ContainerPort: runtimePoolPort, Protocol: corev1.ProtocolTCP}},
				Env: []corev1.EnvVar{
					{Name: "ORKA_ACP_LISTEN_ADDRESS", Value: ":8080"},
					{Name: "ORKA_ACP_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
					{Name: "ORKA_ACP_POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "ORKA_ACP_POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					{Name: "ORKA_ACP_CONTROLLER_EPOCH", Value: strconv.FormatInt(cfg.controllerEpoch, 10)},
					{Name: "ORKA_ACP_RUNTIME_POOL_UID", Value: string(pool.UID)},
					{Name: "ORKA_ACP_RUNTIME_POOL_GENERATION", Value: strconv.FormatInt(pool.Generation, 10)},
					{Name: "ORKA_ACP_RUNTIME_PROFILE_DIGEST", Value: pool.Spec.Runtime.Profile.Digest},
					{Name: "ORKA_ACP_PROFILE_DIGEST_SCHEMA_VERSION", Value: pool.Spec.Runtime.Profile.DigestSchemaVersion},
					{Name: "ORKA_ACP_ACP_PROFILE", Value: cfg.profile.ACPProfile},
					{Name: "ORKA_ACP_ADAPTER_DIGESTS_JSON", Value: string(adapterDigestsJSON)},
					{Name: "ORKA_ACP_PROVIDER", Value: cfg.profile.ProviderKind},
					{Name: "ORKA_ACP_PROVIDER_PROXY_BASE_URL", Value: cfg.providerProxy.baseURL},
					{Name: "ORKA_ACP_MODEL", Value: cfg.profile.Model},
					{Name: "ORKA_ACP_MODEL_CONTEXT_LIMIT", Value: modelContextLimit},
					{Name: "ORKA_ACP_MODEL_OUTPUT_LIMIT", Value: modelOutputLimit},
					{Name: "ORKA_ACP_WORKSPACE_INTENT", Value: string(cfg.profile.WorkspaceIntent)},
					{Name: "ORKA_ACP_AGENT_CONFIGURATION_DIGEST", Value: cfg.profile.AgentConfigurationDigest},
					{Name: "ORKA_ACP_TOOL_POLICY_DIGEST", Value: cfg.profile.ToolPolicyDigest},
					{Name: "ORKA_ACP_APPROVAL_POLICY_DIGEST", Value: cfg.profile.ApprovalPolicyDigest},
					{Name: "ORKA_ACP_MCP_CONFIGURATION_DIGEST", Value: cfg.profile.MCPConfigurationDigest},
					{Name: "ORKA_ACP_PROXY_CREDENTIAL_ROLE", Value: cfg.profile.ProxyCredentialRole},
					{Name: "ORKA_ACP_PROXY_CREDENTIAL_SCOPE", Value: cfg.profile.ProxyCredentialScope},
					{Name: "ORKA_ACP_RESOURCE_CLASS", Value: cfg.profile.ResourceClass},
					{Name: runtimePoolControllerTokenFileEnv, Value: runtimePoolControllerTokenPath},
					{Name: runtimePoolCapabilitySecretFileEnv, Value: runtimePoolCapabilitySecretPath},
					{Name: runtimePoolProviderTokenFileEnv, Value: runtimePoolProviderTokenPath},
					{Name: "ORKA_ACP_PROVIDER_TOKEN_GENERATION", Value: cfg.providerProxy.tokenGeneration},
					{Name: "ORKA_ACP_ARTIFACT_API_URL", Value: strings.TrimRight(r.ControllerAPIURL, "/")},
					{Name: "ORKA_ACP_WORKSPACE_MAX_ARTIFACT_BYTES", Value: strconv.FormatInt(r.WorkspaceArtifactMaxBytes, 10)},
					{Name: "ORKA_ACP_MCP_BROKER_URL", Value: strings.TrimRight(r.ControllerAPIURL, "/")},
					{Name: "ORKA_ACP_TRUST_NAMESPACE", Value: pool.Spec.TrustDomain.Namespace},
					{Name: "ORKA_ACP_SESSION_BASE_DIR", Value: "/sessions"},
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:                &zero,
					RunAsGroup:               &zero,
					RunAsNonRoot:             new(false),
					AllowPrivilegeEscalation: new(false),
					ReadOnlyRootFilesystem:   new(true),
					Privileged:               new(false),
					Capabilities: &corev1.Capabilities{
						Drop: []corev1.Capability{"ALL"},
						Add:  []corev1.Capability{"CHOWN", "KILL", "SETGID", "SETUID"},
					},
					SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				Resources: runtimePoolResourceRequirements(pool.Spec.Runtime.Profile.ResourceClass),
				VolumeMounts: []corev1.VolumeMount{
					{Name: runtimePoolAuthVolume, MountPath: "/var/run/secrets/orka/auth", ReadOnly: true},
					{Name: runtimePoolProviderCapabilityVolume, MountPath: "/var/run/secrets/orka/provider", ReadOnly: true},
					{Name: runtimePoolSessionsVolume, MountPath: "/sessions"},
					{Name: runtimePoolTempVolume, MountPath: "/tmp"},
					{Name: runtimePoolHomeVolume, MountPath: "/home/worker"},
				},
				StartupProbe:   runtimePoolHTTPProbe(30, 2, 1),
				ReadinessProbe: runtimePoolHTTPProbe(3, 5, 2),
				LivenessProbe:  runtimePoolHTTPProbe(3, 10, 3),
				Lifecycle:      &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 5}}},
			}},
			Volumes: []corev1.Volume{
				{Name: runtimePoolAuthVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: authSecretName, DefaultMode: &mode, Items: []corev1.KeyToPath{
					{Key: runtimePoolControllerTokenKey, Path: runtimePoolControllerTokenKey, Mode: &mode},
					{Key: runtimePoolCapabilitySecretKey, Path: runtimePoolCapabilitySecretKey, Mode: &mode},
				}}}},
				{Name: runtimePoolProviderCapabilityVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: providerSecretName, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: runtimePoolProviderTokenKey, Path: runtimePoolProviderTokenKey, Mode: &mode}}}}},
				{Name: runtimePoolSessionsVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("4Gi"))}}},
				{Name: runtimePoolTempVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("512Mi"))}}},
				{Name: runtimePoolHomeVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: new(resource.MustParse("256Mi"))}}},
			},
		},
	}
	if marker := strings.TrimSpace(r.E2EPromptWriteAmbiguityMarker); marker != "" {
		template.Spec.Containers[0].Env = append(template.Spec.Containers[0].Env, corev1.EnvVar{
			Name: runtimePoolE2EPromptWriteAmbiguity, Value: marker,
		})
	}
	template.Annotations[runtimePoolTemplateRevisionAnnotation] = runtimePoolPodTemplateRevision(template)
	return template
}

func runtimePoolJSONRevision(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal RuntimePool template revision: %w", err)
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func runtimePoolPodTemplateRevision(template corev1.PodTemplateSpec) string {
	copy := *template.DeepCopy()
	delete(copy.Annotations, runtimePoolTemplateRevisionAnnotation)
	payload, err := json.Marshal(copy)
	if err != nil {
		panic(fmt.Sprintf("marshal RuntimePool Pod template revision: %v", err))
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func runtimePoolResourceRequirements(resourceClass string) corev1.ResourceRequirements {
	if resourceClass != runtimePoolResourceClassStandard {
		return corev1.ResourceRequirements{}
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("250m"),
			corev1.ResourceMemory:           resource.MustParse("512Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("4Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
		},
	}
}

func runtimePoolHTTPProbe(failureThreshold, periodSeconds, timeoutSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: harnessv2.HealthPath, Port: intstr.FromString("control"), Scheme: corev1.URISchemeHTTP}},
		InitialDelaySeconds: 0,
		TimeoutSeconds:      timeoutSeconds,
		PeriodSeconds:       periodSeconds,
		FailureThreshold:    failureThreshold,
		SuccessThreshold:    1,
	}
}

func (r *RuntimePoolReconciler) ensureRuntimePoolService(ctx context.Context, pool *corev1alpha1.RuntimePool, cfg runtimePoolConfig) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cfg.baseName, Namespace: cfg.namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = mergeStringMap(service.Labels, cfg.labels)
		if err := r.setRuntimePoolControllerReference(pool, service); err != nil {
			return err
		}
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.ClusterIPs = []string{corev1.ClusterIPNone}
		service.Spec.PublishNotReadyAddresses = false
		service.Spec.Selector = map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}
		service.Spec.Ports = []corev1.ServicePort{{Name: "control", Port: runtimePoolPort, TargetPort: intstr.FromString("control"), Protocol: corev1.ProtocolTCP}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcile RuntimePool headless Service: %w", err)
	}
	return nil
}

func controllerNamespaceForRuntimePool(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return runtimePoolDefaultControllerNamespace
	}
	return namespace
}

func (r *RuntimePoolReconciler) ensureRuntimePoolNetworkPolicies(ctx context.Context, pool *corev1alpha1.RuntimePool, cfg runtimePoolConfig) error {
	selector := metav1.LabelSelector{MatchLabels: map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}}
	controllerNamespace := controllerNamespaceForRuntimePool(r.ControllerNamespace)
	policies := make([]networkingv1.NetworkPolicy, 0, 6)
	policies = append(policies, []networkingv1.NetworkPolicy{
		{
			ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "deny-all"), Namespace: cfg.namespace},
			Spec:       networkingv1.NetworkPolicySpec{PodSelector: selector, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "control-in"), Namespace: cfg.namespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: selector,
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
				Ingress: []networkingv1.NetworkPolicyIngressRule{{
					From: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: controllerNamespace}},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{runtimePoolNetworkRoleLabel: "controller"}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(runtimePoolPort))}},
				}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "dns-egress"), Namespace: cfg.namespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: selector,
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress: []networkingv1.NetworkPolicyEgressRule{{
					To: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: metav1.NamespaceSystem}},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"k8s-app": "kube-dns"}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: new(corev1.ProtocolUDP), Port: new(intstr.FromInt32(53))},
						{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(53))},
					},
				}},
			},
		},
	}...)
	policies = append(policies,
		networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "provider-proxy-egress"), Namespace: cfg.namespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: selector, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress: []networkingv1.NetworkPolicyEgressRule{{
					To: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: cfg.providerProxy.namespace}},
						PodSelector:       &metav1.LabelSelector{MatchLabels: cloneStringMap(cfg.providerProxy.podLabels)},
					}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(cfg.providerProxy.port))}},
				}},
			},
		},
		networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "controller-egress"), Namespace: cfg.namespace},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: selector, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress: []networkingv1.NetworkPolicyEgressRule{{
					To: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: controllerNamespace}},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{runtimePoolNetworkRoleLabel: "controller"}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(r.ControllerAPIPort))}},
				}},
			},
		},
	)
	for i := range policies {
		policy := &networkingv1.NetworkPolicy{ObjectMeta: policies[i].ObjectMeta}
		desired := policies[i].Spec
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, policy, func() error {
			policy.Labels = mergeStringMap(policy.Labels, cfg.labels)
			if err := r.setRuntimePoolControllerReference(pool, policy); err != nil {
				return err
			}
			policy.Spec = desired
			return nil
		})
		if err != nil {
			return fmt.Errorf("reconcile RuntimePool NetworkPolicy %q: %w", policy.Name, err)
		}
	}
	return nil
}

func (r *RuntimePoolReconciler) ensureRuntimePoolPDB(ctx context.Context, pool *corev1alpha1.RuntimePool, cfg runtimePoolConfig) error {
	name := runtimePoolChildName(cfg.baseName, "pdb")
	pdb := &policyv1.PodDisruptionBudget{}
	key := types.NamespacedName{Namespace: cfg.namespace, Name: name}
	if !r.EnablePDB {
		if err := r.Get(ctx, key, pdb); err == nil {
			return client.IgnoreNotFound(r.Delete(ctx, pdb))
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	pdb.ObjectMeta = metav1.ObjectMeta{Name: name, Namespace: cfg.namespace}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = mergeStringMap(pdb.Labels, cfg.labels)
		if err := r.setRuntimePoolControllerReference(pool, pdb); err != nil {
			return err
		}
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: new(intstr.FromInt32(0)),
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}},
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcile RuntimePool PodDisruptionBudget: %w", err)
	}
	return nil
}

func (r *RuntimePoolReconciler) stopRuntimePoolDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	if deployment == nil {
		return fmt.Errorf("RuntimePool Deployment is required")
	}
	if ptr.Deref(deployment.Spec.Replicas, 0) == 0 {
		return nil
	}
	base := deployment.DeepCopy()
	deployment.Spec.Replicas = new(int32(0))
	return r.Patch(ctx, deployment, client.MergeFrom(base))
}

func (r *RuntimePoolReconciler) listRuntimePoolPods(ctx context.Context, cfg runtimePoolConfig) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(cfg.namespace), client.MatchingLabels{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func readyRuntimePoolPods(pods []corev1.Pod) []corev1.Pod {
	ready := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning || strings.TrimSpace(pod.Status.PodIP) == "" {
			continue
		}
		condition := findRuntimePoolPodCondition(pod.Status.Conditions, corev1.PodReady)
		if condition != nil && condition.Status == corev1.ConditionTrue {
			ready = append(ready, *pod.DeepCopy())
		}
	}
	return ready
}

func findRuntimePoolPodCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func countRuntimePoolPods(pods []corev1.Pod) int32 {
	var count int32
	for i := range pods {
		if pods[i].Status.Phase != corev1.PodSucceeded && pods[i].Status.Phase != corev1.PodFailed {
			count++
		}
	}
	return count
}

func runtimePoolSchedulingFailure(pods []corev1.Pod) (string, string, bool) {
	for i := range pods {
		condition := findRuntimePoolPodCondition(pods[i].Status.Conditions, corev1.PodScheduled)
		if condition != nil && condition.Status == corev1.ConditionFalse {
			message := sanitizeRuntimePoolMessage(condition.Message)
			if message == "" {
				message = "runtime Pod is unschedulable"
			}
			reason := strings.TrimSpace(condition.Reason)
			if reason == "" {
				reason = corev1alpha1.RuntimePoolReasonSchedulingFailed
			}
			return reason, message, true
		}
	}
	return "", "", false
}

func runtimePoolPodFailure(pods []corev1.Pod) (string, string, bool) {
	for i := range pods {
		statuses := append([]corev1.ContainerStatus(nil), pods[i].Status.InitContainerStatuses...)
		statuses = append(statuses, pods[i].Status.ContainerStatuses...)
		for j := range statuses {
			waiting := statuses[j].State.Waiting
			if waiting == nil {
				continue
			}
			switch waiting.Reason {
			case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError", "RunContainerError", "CrashLoopBackOff":
				message := sanitizeRuntimePoolMessage(waiting.Message)
				if message == "" {
					message = "runtime container failed: " + waiting.Reason
				}
				return corev1alpha1.RuntimePoolReasonRolloutFailed, message, true
			}
		}
	}
	return "", "", false
}

func (r *RuntimePoolReconciler) applyDeploymentFailureConditions(pool *corev1alpha1.RuntimePool, deployment *appsv1.Deployment, status *corev1alpha1.RuntimePoolStatus) {
	if deployment == nil || status == nil {
		return
	}
	for i := range deployment.Status.Conditions {
		condition := deployment.Status.Conditions[i]
		if condition.Type != appsv1.DeploymentReplicaFailure || condition.Status != corev1.ConditionTrue {
			continue
		}
		message := sanitizeRuntimePoolMessage(condition.Message)
		lower := strings.ToLower(message)
		switch {
		case strings.Contains(lower, "podsecurity"), strings.Contains(lower, "pod security"), strings.Contains(lower, "restricted:"):
			r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionPodSecurityReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonPodSecurityRejected, message)
		case strings.Contains(lower, "quota"), strings.Contains(lower, "resourcequota"):
			r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionQuotaReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonQuotaRejected, message)
		default:
			r.setRuntimePoolCondition(pool, status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonRolloutFailed, message)
		}
	}
}

func runtimePoolDeploymentValidationTarget(
	pool *corev1alpha1.RuntimePool,
	deployment *appsv1.Deployment,
) (*corev1alpha1.RuntimePool, runtimePoolConfig, error) {
	if pool == nil || deployment == nil || len(deployment.Spec.Template.Spec.Containers) != 1 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool template is invalid")
	}
	return runtimePoolValidationTargetFromTemplate(pool, deployment.Spec.Template)
}

// runtimePoolValidationTargetFromTemplate reconstructs the deployed pool
// identity from a rendered Pod template regardless of the workload backend
// (Deployment or provider workspace) that materialized it.
func runtimePoolValidationTargetFromTemplate(
	pool *corev1alpha1.RuntimePool,
	template corev1.PodTemplateSpec,
) (*corev1alpha1.RuntimePool, runtimePoolConfig, error) {
	if pool == nil || len(template.Spec.Containers) != 1 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool template is invalid")
	}
	environment := runtimePoolLiteralEnvironment(template.Spec.Containers[0].Env)
	poolGeneration, err := strconv.ParseInt(environment["ORKA_ACP_RUNTIME_POOL_GENERATION"], 10, 64)
	if err != nil || poolGeneration <= 0 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool generation is invalid")
	}
	controllerEpoch, err := strconv.ParseInt(environment["ORKA_ACP_CONTROLLER_EPOCH"], 10, 64)
	if err != nil || controllerEpoch <= 0 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool controller epoch is invalid")
	}
	providerGeneration := strings.TrimSpace(environment["ORKA_ACP_PROVIDER_TOKEN_GENERATION"])
	if !validRuntimePoolProviderTokenGeneration(providerGeneration) ||
		template.Annotations[runtimePoolProviderTokenGenerationAnnotation] != providerGeneration {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool provider token generation is invalid")
	}
	profileDigest := strings.TrimSpace(environment["ORKA_ACP_RUNTIME_PROFILE_DIGEST"])
	if !validSHA256Digest(profileDigest) || template.Annotations[runtimePoolProfileAnnotation] != profileDigest {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool profile digest is invalid")
	}
	adapterDigests := map[string]string{}
	if err := json.Unmarshal([]byte(environment["ORKA_ACP_ADAPTER_DIGESTS_JSON"]), &adapterDigests); err != nil || len(adapterDigests) == 0 {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool adapter digests are invalid")
	}
	modelLimits, err := runtimePoolModelLimitsFromEnvironment(environment)
	if err != nil {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool model limits are invalid: %w", err)
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile:               environment["ORKA_ACP_ACP_PROFILE"],
		AdapterDigests:           adapterDigests,
		ProviderKind:             environment["ORKA_ACP_PROVIDER"],
		Model:                    environment["ORKA_ACP_MODEL"],
		ModelLimits:              modelLimits,
		AgentConfigurationDigest: environment["ORKA_ACP_AGENT_CONFIGURATION_DIGEST"],
		ToolPolicyDigest:         environment["ORKA_ACP_TOOL_POLICY_DIGEST"],
		ApprovalPolicyDigest:     environment["ORKA_ACP_APPROVAL_POLICY_DIGEST"],
		MCPConfigurationDigest:   environment["ORKA_ACP_MCP_CONFIGURATION_DIGEST"],
		WorkspaceIntent:          harnessv2.WorkspaceIntent(environment["ORKA_ACP_WORKSPACE_INTENT"]),
		ProxyCredentialRole:      environment["ORKA_ACP_PROXY_CREDENTIAL_ROLE"],
		ProxyCredentialScope:     environment["ORKA_ACP_PROXY_CREDENTIAL_SCOPE"],
		ResourceClass:            environment["ORKA_ACP_RESOURCE_CLASS"],
	}
	if err := profile.Validate(); err != nil {
		return nil, runtimePoolConfig{}, fmt.Errorf("deployed RuntimePool profile is invalid: %w", err)
	}
	deployedPool := pool.DeepCopy()
	deployedPool.Generation = poolGeneration
	deployedPool.Spec.Runtime.Profile.Digest = profileDigest
	deployedPool.Spec.Runtime.Profile.DigestSchemaVersion = environment["ORKA_ACP_PROFILE_DIGEST_SCHEMA_VERSION"]
	return deployedPool, runtimePoolConfig{
		controllerEpoch: controllerEpoch,
		protocol:        corev1alpha1.RuntimePoolProtocolHarnessV2,
		profile:         profile,
		providerProxy: runtimePoolProviderProxyConfig{
			tokenGeneration: providerGeneration,
		},
	}, nil
}

func runtimePoolModelLimitsFromEnvironment(environment map[string]string) (*harnessv2.ModelTokenLimits, error) {
	contextValue := strings.TrimSpace(environment["ORKA_ACP_MODEL_CONTEXT_LIMIT"])
	outputValue := strings.TrimSpace(environment["ORKA_ACP_MODEL_OUTPUT_LIMIT"])
	if contextValue == "" && outputValue == "" {
		return nil, nil
	}
	if contextValue == "" || outputValue == "" {
		return nil, fmt.Errorf("context and output limits must be set together")
	}
	contextLimit, err := strconv.ParseInt(contextValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("context limit must be an integer")
	}
	outputLimit, err := strconv.ParseInt(outputValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("output limit must be an integer")
	}
	limits := &harnessv2.ModelTokenLimits{Context: contextLimit, Output: outputLimit}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return limits, nil
}

func runtimePoolLiteralEnvironment(values []corev1.EnvVar) map[string]string {
	result := make(map[string]string, len(values))
	for i := range values {
		if values[i].ValueFrom == nil {
			result[values[i].Name] = strings.TrimSpace(values[i].Value)
		}
	}
	return result
}

func validRuntimePoolProviderTokenGeneration(generation string) bool {
	if len(generation) != 16 {
		return false
	}
	_, err := hex.DecodeString(generation)
	return err == nil && generation == strings.ToLower(generation)
}

func (r *RuntimePoolReconciler) runtimePoolDeploymentAuthSecret(
	ctx context.Context,
	deployment *appsv1.Deployment,
) (*corev1.Secret, error) {
	if deployment == nil {
		return nil, fmt.Errorf("RuntimePool Deployment is required")
	}
	secretName := ""
	for i := range deployment.Spec.Template.Spec.Volumes {
		volume := deployment.Spec.Template.Spec.Volumes[i]
		if volume.Name == runtimePoolAuthVolume && volume.Secret != nil {
			secretName = strings.TrimSpace(volume.Secret.SecretName)
			break
		}
	}
	if secretName == "" {
		return nil, fmt.Errorf("deployed RuntimePool auth Secret reference is missing")
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: deployment.Namespace, Name: secretName}, secret); err != nil {
		return nil, fmt.Errorf("get deployed RuntimePool auth Secret: %w", err)
	}
	if strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKey])) == "" ||
		len(secret.Data[runtimePoolCapabilitySecretKey]) == 0 {
		return nil, fmt.Errorf("deployed RuntimePool auth Secret is incomplete")
	}
	return secret, nil
}

func validateRuntimePoolProbe(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pod *corev1.Pod,
	probe RuntimePoolProbeResult,
	now time.Time,
) (*corev1alpha1.RuntimePoolActiveInstanceStatus, error) {
	return validateRuntimePoolProbeWithIdentityCapacityRequirement(pool, cfg, pod, probe, now, true)
}

func validateRuntimePoolProbeForRollout(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pod *corev1.Pod,
	probe RuntimePoolProbeResult,
	now time.Time,
) (*corev1alpha1.RuntimePoolActiveInstanceStatus, error) {
	return validateRuntimePoolProbeWithIdentityCapacityRequirement(pool, cfg, pod, probe, now, false)
}

func validateRuntimePoolProbeWithIdentityCapacityRequirement(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pod *corev1.Pod,
	probe RuntimePoolProbeResult,
	now time.Time,
	requireSessionIdentityCapacity bool,
) (*corev1alpha1.RuntimePoolActiveInstanceStatus, error) {
	if err := probe.Capabilities.Validate(); err != nil {
		return nil, fmt.Errorf("runtime capabilities are invalid: %w", err)
	}
	if err := probe.Status.Validate(); err != nil {
		return nil, fmt.Errorf("runtime status is invalid: %w", err)
	}
	if probe.Capabilities.Protocol != harnessv2.ProtocolVersion || probe.Status.Protocol != harnessv2.ProtocolVersion {
		return nil, fmt.Errorf("runtime protocol does not match %q", harnessv2.ProtocolVersion)
	}
	if probe.Capabilities.ACPVersion != cfg.profile.ACPProfile {
		return nil, fmt.Errorf("runtime ACP profile does not match the immutable RuntimePool profile")
	}
	if !reflect.DeepEqual(probe.Capabilities.AdapterDigests, cfg.profile.AdapterDigests) {
		return nil, fmt.Errorf("runtime adapter digests do not match the immutable RuntimePool profile")
	}
	if len(probe.Capabilities.Provider.ProviderKinds) != 1 || probe.Capabilities.Provider.ProviderKinds[0] != cfg.profile.ProviderKind {
		return nil, fmt.Errorf("runtime provider kind does not match the immutable RuntimePool profile")
	}
	if len(probe.Capabilities.Provider.Models) != 1 || probe.Capabilities.Provider.Models[0] != cfg.profile.Model {
		return nil, fmt.Errorf("runtime model does not match the immutable RuntimePool profile")
	}
	if !probe.Capabilities.Provider.SupportsCancel || !probe.Capabilities.Provider.SupportsTools {
		return nil, fmt.Errorf("runtime supervisor is missing required cancel or tool capabilities")
	}
	if !probe.Capabilities.WorkspaceGovernance.Strict() {
		return nil, fmt.Errorf("runtime supervisor does not advertise strict workspace governance")
	}
	if string(probe.Capabilities.RuntimeProfileDigest) != pool.Spec.Runtime.Profile.Digest || string(probe.Status.Fence.RuntimeProfileDigest) != pool.Spec.Runtime.Profile.Digest {
		return nil, fmt.Errorf("runtime profile digest does not match the immutable RuntimePool profile")
	}
	if !runtimePoolDigestSchemaMatches(pool.Spec.Runtime.Profile.DigestSchemaVersion, probe.Capabilities.ProfileDigestSchemaVersion) ||
		!runtimePoolDigestSchemaMatches(pool.Spec.Runtime.Profile.DigestSchemaVersion, probe.Status.Fence.ProfileDigestSchemaVersion) {
		return nil, fmt.Errorf("runtime profile digest schema does not match the immutable RuntimePool profile")
	}
	if string(probe.Status.Fence.RuntimePoolUID) != string(pool.UID) {
		return nil, fmt.Errorf("runtime status is fenced to another RuntimePool UID")
	}
	if probe.Status.Fence.RuntimePoolGeneration != uint64(pool.Generation) {
		return nil, fmt.Errorf("runtime status is fenced to another RuntimePool generation")
	}
	if probe.Status.Fence.ControllerEpoch != uint64(cfg.controllerEpoch) {
		return nil, fmt.Errorf("runtime status is fenced to another controller epoch")
	}
	if !probe.Capabilities.SupportsDrain {
		return nil, fmt.Errorf("runtime supervisor does not advertise drain support")
	}
	providerGeneration, err := validateRuntimePoolPodRevision(pool, cfg, pod)
	if err != nil {
		return nil, err
	}
	expectedInstanceID := runtimePoolRuntimeInstanceID(pod.UID, probe.Status.Fence.SupervisorBootID)
	if string(probe.Status.Fence.RuntimeInstanceID) != expectedInstanceID {
		return nil, fmt.Errorf("runtime instance ID does not match the selected Pod UID and supervisor boot ID")
	}
	if probe.Capabilities.Limits.MaxResidentSessions == 0 || probe.Capabilities.Limits.MaxConcurrentPrompts == 0 {
		return nil, fmt.Errorf("runtime supervisor advertised zero capacity")
	}
	if requireSessionIdentityCapacity && probe.Status.SessionIdentityCapacity == nil {
		return nil, fmt.Errorf("runtime supervisor did not report session identity capacity")
	}
	if probe.Status.SessionIdentityCapacity != nil {
		usableSessionIdentities := probe.Status.SessionIdentityCapacity.Total - probe.Status.SessionIdentityCapacity.ExhaustionReserve
		if usableSessionIdentities < uint64(probe.Capabilities.Limits.MaxResidentSessions) {
			return nil, fmt.Errorf("runtime supervisor session identity capacity is smaller than resident-session capacity")
		}
	}
	observed := metav1.NewTime(now.UTC())
	return &corev1alpha1.RuntimePoolActiveInstanceStatus{
		PodNamespace:               pod.Namespace,
		PodName:                    pod.Name,
		PodAddress:                 pod.Status.PodIP,
		PodUID:                     string(pod.UID),
		BootID:                     string(probe.Status.Fence.SupervisorBootID),
		RuntimeInstanceID:          expectedInstanceID,
		ControllerEpoch:            cfg.controllerEpoch,
		ProtocolVersion:            cfg.protocol,
		ProfileDigest:              pool.Spec.Runtime.Profile.Digest,
		ProfileDigestSchemaVersion: pool.Spec.Runtime.Profile.DigestSchemaVersion,
		ProviderTokenGeneration:    providerGeneration,
		LastObservedTime:           &observed,
	}, nil
}

func validateRuntimePoolPodRevision(
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	pod *corev1.Pod,
) (string, error) {
	providerGeneration := strings.TrimSpace(cfg.providerProxy.tokenGeneration)
	if providerGeneration == "" && pool.Status.ActiveInstance != nil {
		providerGeneration = strings.TrimSpace(pool.Status.ActiveInstance.ProviderTokenGeneration)
	}
	podProviderGeneration := strings.TrimSpace(pod.Annotations[runtimePoolProviderTokenGenerationAnnotation])
	if providerGeneration == "" {
		providerGeneration = podProviderGeneration
	}
	if !validRuntimePoolProviderTokenGeneration(providerGeneration) || podProviderGeneration != providerGeneration {
		return "", fmt.Errorf("runtime Pod provider token generation does not match the intended immutable template")
	}
	if strings.TrimSpace(pod.Annotations[runtimePoolProfileAnnotation]) != pool.Spec.Runtime.Profile.Digest {
		return "", fmt.Errorf("runtime Pod profile annotation does not match the intended immutable profile")
	}
	return providerGeneration, nil
}

func applyRuntimePoolProbeCapacity(status *corev1alpha1.RuntimePoolStatus, cfg runtimePoolConfig, probe RuntimePoolProbeResult) {
	if status == nil {
		return
	}
	status.Capacity.MaxResidentSessions = min(cfg.maxResidentSessions, int32(probe.Capabilities.Limits.MaxResidentSessions))
	status.Capacity.MaxRunningPrompts = min(cfg.maxRunningPrompts, int32(probe.Capabilities.Limits.MaxConcurrentPrompts))
	status.Capacity.ResidentSessions = int32(probe.Status.Pressure.ResidentSessions)
	status.Capacity.RunningPrompts = int32(probe.Status.Pressure.ActivePrompts)
	status.Capacity.PendingPermissions = int32(probe.Status.Pressure.PendingPermissions)
	status.Capacity.LiveDescendants = int32(probe.Status.Pressure.LiveDescendants)
	var finalizing int32
	for i := range probe.Status.Sessions {
		if probe.Status.Sessions[i].ReservedForFinalization {
			finalizing++
		}
	}
	status.Capacity.FinalizingSessions = max(status.Capacity.FinalizingSessions, finalizing)
}

func runtimePoolAtCapacity(capacity corev1alpha1.RuntimePoolCapacityStatus) bool {
	return capacity.ResidentSessions+capacity.ReservedSessions >= capacity.MaxResidentSessions ||
		capacity.RunningPrompts >= capacity.MaxRunningPrompts
}

func runtimePoolControllerWorkIsQuiescent(capacity corev1alpha1.RuntimePoolCapacityStatus) bool {
	return capacity.QueuedTasks == 0 && capacity.ReservedSessions == 0 && capacity.FinalizingSessions == 0
}

func runtimePoolProbeIsQuiescent(controllerCapacity corev1alpha1.RuntimePoolCapacityStatus, status harnessv2.StatusResponse) bool {
	return status.Drain.Requested && !status.Drain.AcceptingNewSessions &&
		runtimePoolControllerWorkIsQuiescent(controllerCapacity) &&
		status.Pressure.ResidentSessions == 0 && status.Pressure.ActivePrompts == 0 &&
		status.Pressure.QueuedAdmissions == 0 && status.Pressure.PendingPermissions == 0 &&
		status.Pressure.LiveDescendants == 0 && len(status.Sessions) == 0 &&
		len(status.ActivePrompts) == 0 && len(status.PendingPermissions) == 0
}

func (r *RuntimePoolReconciler) baseRuntimePoolStatus(pool *corev1alpha1.RuntimePool, currentReplicas int32) corev1alpha1.RuntimePoolStatus {
	status := pool.DeepCopy().Status
	status.ObservedGeneration = pool.Generation
	status.ControllerEpoch = r.effectiveControllerEpoch(pool)
	status.DesiredReplicas = pool.Spec.DesiredReplicas
	status.CurrentReplicas = currentReplicas
	status.Capacity.MaxResidentSessions = corev1alpha1.DefaultRuntimePoolMaxResidentSessions
	status.Capacity.MaxRunningPrompts = corev1alpha1.DefaultRuntimePoolMaxRunningPrompts
	if pool.Spec.Capacity != nil {
		if pool.Spec.Capacity.MaxResidentSessions > 0 {
			status.Capacity.MaxResidentSessions = pool.Spec.Capacity.MaxResidentSessions
		}
		if pool.Spec.Capacity.MaxRunningPrompts > 0 {
			status.Capacity.MaxRunningPrompts = pool.Spec.Capacity.MaxRunningPrompts
		}
	}
	for _, conditionType := range []string{
		corev1alpha1.RuntimePoolConditionAdmissionReady,
		corev1alpha1.RuntimePoolConditionPodSecurityReady,
		corev1alpha1.RuntimePoolConditionQuotaReady,
		corev1alpha1.RuntimePoolConditionSchedulingReady,
		corev1alpha1.RuntimePoolConditionRolloutReady,
	} {
		if meta.FindStatusCondition(status.Conditions, conditionType) == nil {
			r.setRuntimePoolCondition(pool, &status, conditionType, metav1.ConditionUnknown, "Reconciling", "RuntimePool reconciliation has not completed this check")
		}
	}
	return status
}

func (r *RuntimePoolReconciler) finishRuntimePoolResourceFailure(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	cfg runtimePoolConfig,
	err error,
) (ctrl.Result, error) {
	status := r.baseRuntimePoolStatus(pool, 0)
	status.ControllerEpoch = cfg.controllerEpoch
	status.Lifecycle = corev1alpha1.RuntimePoolLifecycleDegraded
	status.AdmissionState = corev1alpha1.RuntimePoolAdmissionClosed
	status.ActiveInstance = nil
	status.Message = sanitizeRuntimePoolMessage(err.Error())
	reason := corev1alpha1.RuntimePoolReasonRolloutFailed
	lower := strings.ToLower(status.Message)
	switch {
	case strings.Contains(lower, "podsecurity"), strings.Contains(lower, "pod security"), strings.Contains(lower, "restricted:"):
		reason = corev1alpha1.RuntimePoolReasonPodSecurityRejected
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionPodSecurityReady, metav1.ConditionFalse, reason, status.Message)
	case strings.Contains(lower, "quota"), strings.Contains(lower, "resourcequota"):
		reason = corev1alpha1.RuntimePoolReasonQuotaRejected
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionQuotaReady, metav1.ConditionFalse, reason, status.Message)
	default:
		r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionRolloutReady, metav1.ConditionFalse, reason, status.Message)
	}
	r.setRuntimePoolCondition(pool, &status, corev1alpha1.RuntimePoolConditionAdmissionReady, metav1.ConditionFalse, corev1alpha1.RuntimePoolReasonAdmissionClosed, status.Message)
	return r.finishRuntimePoolStatus(ctx, pool, status, runtimePoolRequeue)
}

// runtimePoolConflictRequeue is how soon a reconcile that lost the status
// optimistic lock re-reads the pool.
const runtimePoolConflictRequeue = time.Second

func (r *RuntimePoolReconciler) finishRuntimePoolStatus(
	ctx context.Context,
	pool *corev1alpha1.RuntimePool,
	status corev1alpha1.RuntimePoolStatus,
	requeueAfter time.Duration,
) (ctrl.Result, error) {
	clearRuntimePoolUnfencedProbePressure(&status)
	status.Message = sanitizeRuntimePoolMessage(status.Message)
	if reflect.DeepEqual(pool.Status, status) {
		recordRuntimePoolMetrics(pool, status)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	scaledToZero := runtimePoolCompletedScaleToZero(pool.Status, status)
	base := pool.DeepCopy()
	pool.Status = status
	if err := r.Status().Patch(ctx, pool, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) {
			// Another writer (the drain, session, or supervisor probe path)
			// advanced the pool first; recompute from the fresh object instead
			// of surfacing the optimistic-lock miss as a reconcile error.
			return ctrl.Result{RequeueAfter: runtimePoolConflictRequeue}, nil
		}
		return ctrl.Result{}, err
	}
	recordRuntimePoolMetrics(pool, status)
	if scaledToZero {
		orkametrics.RecordACPRuntimePoolScaleToZero(pool.Namespace, pool.Name)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// Supervisor pressure belongs to one exact authenticated runtime instance.
// Once that fence is absent, retaining the last probe would publish stale
// sessions and prompts for a Pod that was lost, rejected, or recycled.
func clearRuntimePoolUnfencedProbePressure(status *corev1alpha1.RuntimePoolStatus) {
	if status == nil || status.ActiveInstance != nil {
		return
	}
	status.Capacity.ResidentSessions = 0
	status.Capacity.RunningPrompts = 0
	status.Capacity.PendingPermissions = 0
	status.Capacity.LiveDescendants = 0
}

func recordRuntimePoolMetrics(pool *corev1alpha1.RuntimePool, status corev1alpha1.RuntimePoolStatus) {
	if pool == nil {
		return
	}
	orkametrics.RecordACPRuntimePoolStatus(
		pool.Namespace,
		pool.Name,
		status.DesiredReplicas,
		runtimePoolReadyReplicas(status),
		status.Capacity.ResidentSessions,
		status.Capacity.RunningPrompts,
		status.Capacity.QueuedTasks,
		string(status.AdmissionState),
	)
}

func runtimePoolReadyReplicas(status corev1alpha1.RuntimePoolStatus) int32 {
	if status.ActiveInstance == nil {
		return 0
	}
	ready := meta.FindStatusCondition(status.Conditions, corev1alpha1.RuntimePoolConditionSchedulingReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		return 0
	}
	return 1
}

func runtimePoolCompletedScaleToZero(previous, current corev1alpha1.RuntimePoolStatus) bool {
	if current.DesiredReplicas != 0 || current.CurrentReplicas != 0 ||
		current.Lifecycle != corev1alpha1.RuntimePoolLifecycleStopped {
		return false
	}
	return previous.DesiredReplicas > 0 || previous.CurrentReplicas > 0 || previous.ActiveInstance != nil ||
		previous.Lifecycle == corev1alpha1.RuntimePoolLifecycleStarting ||
		previous.Lifecycle == corev1alpha1.RuntimePoolLifecycleServing ||
		previous.Lifecycle == corev1alpha1.RuntimePoolLifecycleDraining ||
		previous.Lifecycle == corev1alpha1.RuntimePoolLifecycleQuiescent ||
		previous.Lifecycle == corev1alpha1.RuntimePoolLifecycleStopping
}

func (r *RuntimePoolReconciler) setRuntimePoolCondition(
	pool *corev1alpha1.RuntimePool,
	status *corev1alpha1.RuntimePoolStatus,
	conditionType string,
	conditionStatus metav1.ConditionStatus,
	reason, message string,
) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		ObservedGeneration: pool.Generation,
		LastTransitionTime: metav1.NewTime(r.now()),
		Reason:             sanitizeRuntimePoolReason(reason),
		Message:            sanitizeRuntimePoolMessage(message),
	})
}

func (r *RuntimePoolReconciler) finalizeRuntimePool(ctx context.Context, pool *corev1alpha1.RuntimePool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pool, runtimePoolFinalizer) {
		return ctrl.Result{}, nil
	}
	cfg, err := r.runtimePoolConfigForDeletion(pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pool.Spec.ExecutionWorkspace != nil {
		var workspaceRemaining bool
		var workspaceErr error
		if runtimePoolIsSubstrateBacked(pool) {
			workspaceRemaining, workspaceErr = r.deleteSubstrateRuntimePoolChildren(ctx, pool, cfg)
		} else {
			workspaceRemaining, workspaceErr = r.deleteRuntimePoolWorkspaceChildren(ctx, pool, cfg)
		}
		if workspaceErr != nil {
			return ctrl.Result{}, workspaceErr
		}
		if workspaceRemaining {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	remaining, err := r.deleteRuntimePoolChildren(ctx, cfg)
	if err != nil {
		return ctrl.Result{}, err
	}
	if remaining {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	base := pool.DeepCopy()
	controllerutil.RemoveFinalizer(pool, runtimePoolFinalizer)
	if err := r.Patch(ctx, pool, client.MergeFrom(base)); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	orkametrics.DeleteACPRuntimePool(pool.Namespace, pool.Name)
	return ctrl.Result{}, nil
}

func (r *RuntimePoolReconciler) runtimePoolConfigForDeletion(pool *corev1alpha1.RuntimePool) (runtimePoolConfig, error) {
	namespace := strings.TrimSpace(pool.Spec.RuntimeNamespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.RuntimeNamespace)
	}
	if namespace == "" {
		namespace = pool.Namespace
	}
	if errs := validation.IsDNS1123Label(namespace); len(errs) != 0 {
		return runtimePoolConfig{}, fmt.Errorf("runtime namespace is invalid")
	}
	return runtimePoolConfig{
		namespace: namespace,
		baseName:  runtimePoolResourceName(pool.Namespace, pool.Name),
		labels: map[string]string{
			runtimePoolManagedByLabel:   runtimePoolManagedByLabelValue,
			runtimePoolApplicationLabel: runtimePoolApplicationLabelValue,
			runtimePoolKeyLabel:         runtimePoolKey(pool.Namespace, pool.Name),
			runtimePoolNamespaceLabel:   pool.Namespace,
			runtimePoolNameLabel:        pool.Name,
			runtimePoolUIDLabel:         string(pool.UID),
		},
	}, nil
}

func (r *RuntimePoolReconciler) deleteRuntimePoolChildren(ctx context.Context, cfg runtimePoolConfig) (bool, error) {
	remaining := false
	deleteObject := func(obj client.Object) error {
		key := types.NamespacedName{Namespace: cfg.namespace, Name: obj.GetName()}
		if err := r.Get(ctx, key, obj); err != nil {
			return client.IgnoreNotFound(err)
		}
		if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		check := obj.DeepCopyObject().(client.Object)
		if err := r.Get(ctx, key, check); err == nil {
			remaining = true
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	objects := make([]client.Object, 0, 9)
	objects = append(objects,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: cfg.baseName}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cfg.baseName}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, "pdb")}},
	)
	for _, suffix := range []string{
		"deny-all", "control-in", "dns-egress", "provider-proxy-egress", "controller-egress",
		// Retain cleanup for policy names used by pre-cutover builds.
		"control-plane-egress", "artifact-api-egress",
	} {
		objects = append(objects, &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: runtimePoolChildName(cfg.baseName, suffix)}})
	}
	for _, obj := range objects {
		obj.SetNamespace(cfg.namespace)
		if err := deleteObject(obj); err != nil {
			return false, err
		}
	}
	for _, list := range []client.ObjectList{&corev1.PodList{}, &corev1.SecretList{}, &appsv1.ReplicaSetList{}} {
		if err := r.List(ctx, list, client.InNamespace(cfg.namespace), client.MatchingLabels{runtimePoolKeyLabel: cfg.labels[runtimePoolKeyLabel]}); err != nil {
			return false, err
		}
		for _, obj := range objectsFromList(list) {
			if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
		}
		if len(objectsFromList(list)) > 0 {
			remaining = true
		}
	}
	return remaining, nil
}

func objectsFromList(list client.ObjectList) []client.Object {
	switch typed := list.(type) {
	case *corev1.PodList:
		result := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			result = append(result, &typed.Items[i])
		}
		return result
	case *corev1.SecretList:
		result := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			result = append(result, &typed.Items[i])
		}
		return result
	case *appsv1.ReplicaSetList:
		result := make([]client.Object, 0, len(typed.Items))
		for i := range typed.Items {
			result = append(result, &typed.Items[i])
		}
		return result
	default:
		return nil
	}
}

func (r *RuntimePoolReconciler) setRuntimePoolControllerReference(pool *corev1alpha1.RuntimePool, object client.Object) error {
	if object.GetNamespace() != pool.Namespace {
		// Namespaced ownerReferences cannot cross namespaces. The finalizer and
		// immutable ownership labels provide explicit cross-namespace cleanup.
		return nil
	}
	return controllerutil.SetControllerReference(pool, object, r.Scheme)
}

func (r *RuntimePoolReconciler) effectiveControllerEpoch(pool *corev1alpha1.RuntimePool) int64 {
	if r.Epochs != nil {
		if current, ok := r.Epochs.Current(); ok && current.Epoch > 0 {
			return current.Epoch
		}
	}
	if r.ControllerEpoch > 0 {
		return r.ControllerEpoch
	}
	if pool.Status.ControllerEpoch > 0 {
		return pool.Status.ControllerEpoch
	}
	return 1
}

func (r *RuntimePoolReconciler) supervisorClient() RuntimePoolSupervisorClient {
	if r.SupervisorClient != nil {
		return r.SupervisorClient
	}
	httpClient := r.HTTPClient
	if httpClient == nil {
		// Supervisor probes and drains target exact Pod endpoints with
		// authenticated headers; environment proxies must never carry them.
		httpClient = &http.Client{Timeout: runtimePoolProbeTimeout, Transport: harnessv2.NewProxylessTransport()}
	}
	isolatedClient := *httpClient
	isolatedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &runtimePoolHTTPSupervisorClient{client: &isolatedClient, now: r.now}
}

// supervisorClientForPool selects the transport for the pool's workload
// backend: Substrate-backed pools reach the exact actor through the provider
// router with the logical route host preserved; every other pool dials the
// exact Pod address directly.
func (r *RuntimePoolReconciler) supervisorClientForPool(pool *corev1alpha1.RuntimePool) RuntimePoolSupervisorClient {
	if r.SupervisorClient != nil {
		return r.SupervisorClient
	}
	if pool != nil && pool.Spec.ExecutionWorkspace != nil &&
		pool.Spec.ExecutionWorkspace.Provider == corev1alpha1.WorkspaceProviderSubstrate {
		httpClient, err := r.substrateSupervisorHTTPClient()
		if err != nil {
			return &runtimePoolFailingSupervisorClient{err: err}
		}
		return &runtimePoolHTTPSupervisorClient{client: httpClient, now: r.now}
	}
	return r.supervisorClient()
}

func (r *RuntimePoolReconciler) substrateSupervisorHTTPClient() (*http.Client, error) {
	r.substrateSupervisorOnce.Do(func() {
		transport, err := substrateRouteHTTPTransport(r.SubstrateConfig.RouterURL, r.SubstrateConfig.ActorDNSSuffix)
		if err != nil {
			r.substrateSupervisorSetup = err
			return
		}
		client := &http.Client{Timeout: runtimePoolProbeTimeout, Transport: transport}
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
		r.substrateSupervisorHTTP = client
	})
	return r.substrateSupervisorHTTP, r.substrateSupervisorSetup
}

// runtimePoolFailingSupervisorClient surfaces a supervisor-transport
// configuration failure as an authenticated-probe failure so the pool degrades
// with a sanitized message instead of dialing an unintended endpoint.
type runtimePoolFailingSupervisorClient struct{ err error }

func (c *runtimePoolFailingSupervisorClient) Probe(context.Context, string, string, []byte) (RuntimePoolProbeResult, error) {
	return RuntimePoolProbeResult{}, c.err
}

func (c *runtimePoolFailingSupervisorClient) RequestDrain(
	context.Context, string, string, []byte, harnessv2.StatusResponse, string,
) error {
	return c.err
}

// runtimePoolInstanceEndpoint resolves the authenticated control endpoint for
// the exact selected instance. Substrate-backed pools use the actor route host
// (dialed through the provider router by the pool's substrate transport);
// every other pool dials the exact Pod address.
func runtimePoolInstanceEndpoint(pool *corev1alpha1.RuntimePool, pod *corev1.Pod) string {
	if pool != nil && pool.Spec.ExecutionWorkspace != nil &&
		pool.Spec.ExecutionWorkspace.Provider == corev1alpha1.WorkspaceProviderSubstrate {
		return urlSchemeHTTP + "://" + strings.TrimSpace(pod.Status.PodIP)
	}
	return runtimePoolPodEndpoint(pod)
}

// substrateRouteHTTPTransport dials the Substrate router for every host under
// the actor DNS suffix while preserving the logical route host as the HTTP
// Host header, exactly like the verified MCP actor routing. Hosts outside the
// suffix are refused: this transport exists only for actor-routed requests.
type substrateRouteRoundTripper struct {
	scheme    string
	basePath  string
	transport *http.Transport
}

func (t *substrateRouteRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, fmt.Errorf("substrate route request URL is required")
	}
	clone := request.Clone(request.Context())
	urlCopy := *request.URL
	urlCopy.Scheme = t.scheme
	if t.basePath != "" {
		urlCopy.Path = strings.TrimRight(t.basePath, "/") + "/" + strings.TrimLeft(urlCopy.Path, "/")
		// Path now contains the decoded combination. Clear RawPath so net/http
		// derives a matching escaped form instead of reusing the actor-relative
		// path from the original request.
		urlCopy.RawPath = ""
	}
	clone.URL = &urlCopy
	return t.transport.RoundTrip(clone)
}

func substrateRouteHTTPTransport(routerURL, actorDNSSuffix string) (http.RoundTripper, error) {
	parsed, err := url.Parse(strings.TrimSpace(routerURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != urlSchemeHTTP && parsed.Scheme != urlSchemeHTTPS) {
		return nil, fmt.Errorf("substrate router URL is invalid")
	}
	routerAddress := parsed.Host
	if parsed.Port() == "" {
		port := "80"
		if parsed.Scheme == urlSchemeHTTPS {
			port = "443"
		}
		routerAddress = net.JoinHostPort(parsed.Hostname(), port)
	}
	normalizedSuffix := strings.ToLower(strings.Trim(strings.TrimSpace(actorDNSSuffix), "."))
	if normalizedSuffix == "" {
		return nil, fmt.Errorf("substrate actor DNS suffix is required")
	}
	if problems := validation.IsDNS1123Subdomain(normalizedSuffix); len(problems) > 0 {
		return nil, fmt.Errorf("substrate actor DNS suffix is invalid: %s", strings.Join(problems, "; "))
	}
	suffix := "." + normalizedSuffix
	transport := harnessv2.NewProxylessTransport()
	if parsed.Scheme == urlSchemeHTTPS {
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: parsed.Hostname(),
		}
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			host = address
		}
		if !strings.HasSuffix(strings.ToLower(host), suffix) {
			return nil, fmt.Errorf("substrate route transport refuses non-actor host")
		}
		return dialer.DialContext(ctx, network, routerAddress)
	}
	return &substrateRouteRoundTripper{
		scheme: parsed.Scheme, basePath: strings.TrimRight(parsed.Path, "/"), transport: transport,
	}, nil
}

func (r *RuntimePoolReconciler) randomSecret(size int) (string, error) {
	reader := r.Rand
	if reader == nil {
		reader = rand.Reader
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (r *RuntimePoolReconciler) randomHex(size int) (string, error) {
	reader := r.Rand
	if reader == nil {
		reader = rand.Reader
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func (r *RuntimePoolReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func runtimePoolResourceName(namespace, name string) string {
	hash := sha256.Sum256([]byte(namespace + "\x00" + name))
	suffix := hex.EncodeToString(hash[:5])
	prefix := strings.Trim(strings.ToLower(name), "-")
	if prefix == "" {
		prefix = "runtime"
	}
	maxPrefix := 63 - len(suffix) - 1
	if len(prefix) > maxPrefix {
		prefix = strings.TrimRight(prefix[:maxPrefix], "-")
	}
	return prefix + "-" + suffix
}

func runtimePoolChildName(base, suffix string) string {
	suffix = strings.Trim(suffix, "-")
	maxBase := 63 - len(suffix) - 1
	if maxBase < 1 {
		hash := sha256.Sum256([]byte(suffix))
		return "runtime-" + hex.EncodeToString(hash[:8])
	}
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + "-" + suffix
}

func runtimePoolKey(namespace, name string) string {
	hash := sha256.Sum256([]byte(namespace + "\x00" + name))
	return hex.EncodeToString(hash[:16])
}

func runtimePoolRuntimeInstanceID(podUID types.UID, bootID harnessv2.SupervisorBootID) string {
	return string(podUID) + "." + string(bootID)
}

func runtimePoolPodEndpoint(pod *corev1.Pod) string {
	host := net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(runtimePoolPort)))
	return (&url.URL{Scheme: "http", Host: host}).String()
}

func runtimePoolDigestSchemaMatches(spec string, observed uint32) bool {
	spec = strings.TrimSpace(strings.ToLower(spec))
	return spec == strconv.FormatUint(uint64(observed), 10) || spec == "v"+strconv.FormatUint(uint64(observed), 10)
}

func runtimePoolHarnessProfile(spec corev1alpha1.RuntimePoolProfileSpec) (harnessv2.RuntimeProfile, error) {
	var modelLimits *harnessv2.ModelTokenLimits
	if spec.ModelLimits != nil {
		modelLimits = &harnessv2.ModelTokenLimits{
			Context: spec.ModelLimits.Context,
			Output:  spec.ModelLimits.Output,
		}
	}
	profile := harnessv2.RuntimeProfile{
		ACPProfile:               strings.TrimSpace(spec.ACPProfile),
		AdapterDigests:           cloneStringMap(spec.AdapterDigests),
		ProviderKind:             strings.TrimSpace(spec.ProviderKind),
		Model:                    strings.TrimSpace(spec.Model),
		ModelLimits:              modelLimits,
		AgentConfigurationDigest: strings.TrimSpace(spec.AgentConfigurationDigest),
		ToolPolicyDigest:         strings.TrimSpace(spec.ToolPolicyDigest),
		ApprovalPolicyDigest:     strings.TrimSpace(spec.ApprovalPolicyDigest),
		MCPConfigurationDigest:   strings.TrimSpace(spec.MCPConfigurationDigest),
		WorkspaceIntent:          harnessv2.WorkspaceIntent(spec.WorkspaceIntent),
		ProxyCredentialRole:      strings.TrimSpace(spec.ProxyCredentialRole),
		ProxyCredentialScope:     strings.TrimSpace(spec.ProxyCredentialScope),
		ResourceClass:            strings.TrimSpace(spec.ResourceClass),
	}
	if err := profile.Validate(); err != nil {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("spec.runtime.profile is invalid: %w", err)
	}
	if profile.ProviderKind == runtimePoolProviderOpencode && profile.ModelLimits == nil {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("spec.runtime.profile.modelLimits is required for built-in OpenCode")
	}
	switch profile.ProviderKind {
	case runtimePoolProviderCodex, runtimePoolProviderClaude, runtimePoolProviderCopilot, runtimePoolProviderOpencode:
	default:
		return harnessv2.RuntimeProfile{}, fmt.Errorf("spec.runtime.profile.providerKind %q is not a supported built-in provider", profile.ProviderKind)
	}
	if profile.ResourceClass != runtimePoolResourceClassStandard {
		return harnessv2.RuntimeProfile{}, fmt.Errorf("spec.runtime.profile.resourceClass %q is not supported", profile.ResourceClass)
	}
	return profile, nil
}

func validSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func sanitizeRuntimePoolMessage(message string) string {
	message = events.RedactExecutionEventText(strings.TrimSpace(message))
	return truncateUTF8(strings.ToValidUTF8(message, "�"), 1024)
}

func sanitizeRuntimePoolReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "Reconciled"
	}
	var b strings.Builder
	for i, r := range reason {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (i > 0 && r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "Reconciled"
	}
	result := b.String()
	if len(result) > 63 {
		result = result[:63]
	}
	return result
}

func cloneStringMap(source map[string]string) map[string]string {
	return maps.Clone(source)
}

func mergeStringMap(destination, source map[string]string) map[string]string {
	if destination == nil {
		destination = make(map[string]string, len(source))
	}
	maps.Copy(destination, source)
	return destination
}

type runtimePoolHTTPSupervisorClient struct {
	client *http.Client
	now    func() time.Time
}

func (c *runtimePoolHTTPSupervisorClient) Probe(ctx context.Context, endpoint, bearerToken string, capabilitySecret []byte) (RuntimePoolProbeResult, error) {
	var result RuntimePoolProbeResult
	if err := c.getJSON(ctx, endpoint+harnessv2.CapabilitiesPath, "", "", &result.Capabilities); err != nil {
		return result, fmt.Errorf("get capabilities: %w", err)
	}
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now().UTC()
	}
	// Status requires proof of the pool's capability secret in addition to
	// the bearer, bound to the pool's profile digest and carrying a single-use
	// nonce; the boot ID and instance ID remain the values status discovers.
	nonce, err := harnessv2.NewCapabilityNonce()
	if err != nil {
		return result, fmt.Errorf("generate status nonce: %w", err)
	}
	binding := harnessv2.StatusCapabilityBinding{RuntimeProfileDigest: result.Capabilities.RuntimeProfileDigest}
	capability, err := harnessv2.SignStatusCapability(capabilitySecret, harnessv2.NewStatusCapabilityClaims(binding, nonce, now.Add(harnessv2.DefaultStatusCapabilityTTL)))
	if err != nil {
		return result, fmt.Errorf("sign status capability: %w", err)
	}
	if err := c.getJSON(ctx, endpoint+harnessv2.StatusPath, bearerToken, capability, &result.Status); err != nil {
		return result, fmt.Errorf("get status: %w", err)
	}
	return result, nil
}

func (c *runtimePoolHTTPSupervisorClient) RequestDrain(
	ctx context.Context,
	endpoint, bearerToken string,
	capabilitySecret []byte,
	status harnessv2.StatusResponse,
	reason string,
) error {
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now().UTC()
	}
	request := harnessv2.DrainRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: harnessv2.MutationMetadata{
			Fence:                      status.Fence,
			OperationID:                harnessv2.OperationID("runtime-pool-drain-" + strconv.FormatUint(status.Fence.RuntimePoolGeneration, 10)),
			RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
			ExpiresAt:                  now.Add(time.Minute),
		},
		Reason: reason,
	}
	digest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		return err
	}
	request.Metadata.RequestDigest = digest
	token, err := harnessv2.SignOperationCapability(capabilitySecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		return err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+harnessv2.DrainPath, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set(runtimePoolOperationHeader, token)
	response, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("drain endpoint returned HTTP %d", response.StatusCode)
	}
	var drain harnessv2.DrainResponse
	if err := decodeBoundedJSON(response.Body, &drain); err != nil {
		return err
	}
	return drain.Validate()
}

func (c *runtimePoolHTTPSupervisorClient) getJSON(ctx context.Context, endpoint, bearerToken, capability string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	if capability != "" {
		req.Header.Set(runtimePoolOperationHeader, capability)
	}
	response, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("endpoint returned HTTP %d", response.StatusCode)
	}
	return decodeBoundedJSON(response.Body, target)
}

func decodeBoundedJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("response contains trailing JSON data")
	}
	return nil
}

func runtimePoolRequestForChild(_ context.Context, object client.Object) []reconcile.Request {
	labels := object.GetLabels()
	namespace := strings.TrimSpace(labels[runtimePoolNamespaceLabel])
	name := strings.TrimSpace(labels[runtimePoolNameLabel])
	if namespace == "" || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
}

// SetupWithManager sets up watches for RuntimePools and their cross-namespace children.
func (r *RuntimePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.RuntimePool{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Watches(&networkingv1.NetworkPolicy{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Watches(&policyv1.PodDisruptionBudget{}, handler.EnqueueRequestsFromMapFunc(runtimePoolRequestForChild)).
		Named("runtimepool").
		Complete(r)
}
