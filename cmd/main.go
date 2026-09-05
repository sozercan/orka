/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"

	acpworkspacev1alpha1 "github.com/orka-agents/orka/api/acp.workspace/v1alpha1"
	fakeworkspacev1alpha1 "github.com/orka-agents/orka/api/fake.workspace/v1alpha1"
	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	orkaadmission "github.com/orka-agents/orka/internal/admission"
	"github.com/orka-agents/orka/internal/api"
	"github.com/orka-agents/orka/internal/artifactcap"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/controller"
	"github.com/orka-agents/orka/internal/executionmode"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	_ "github.com/orka-agents/orka/internal/llm/anthropic"
	_ "github.com/orka-agents/orka/internal/llm/openai"
	_ "github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/outboundaccess"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"

	"github.com/orka-agents/orka/internal/store/sqlite"
	"github.com/orka-agents/orka/internal/tokenexchange"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/worker"
	"github.com/orka-agents/orka/internal/workerenv"
	// +kubebuilder:scaffold:imports
)

const (
	taskResourceKind            = "Task"
	serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	controllerLeaderElectionID  = "03b49a10.orka.ai"
)

var (
	scheme                       = runtime.NewScheme()
	setupLog                     = ctrl.Log.WithName("setup")
	controllerProcessIncarnation = uuid.NewString()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))

	utilruntime.Must(corev1alpha1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1alpha1.AddToScheme(scheme))
	utilruntime.Must(sandboxv1beta1.AddToScheme(scheme))
	utilruntime.Must(sandboxextv1beta1.AddToScheme(scheme))
	utilruntime.Must(workspacev1alpha1.AddToScheme(scheme))
	utilruntime.Must(acpworkspacev1alpha1.AddToScheme(scheme))
	utilruntime.Must(fakeworkspacev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func splitCommaList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func validateWorkspaceProviderSecurityConfig(apiEnabled, classUseAdmissionEnabled, provenanceAdmissionEnabled bool) error {
	if apiEnabled && !classUseAdmissionEnabled {
		return fmt.Errorf("workspace provider API requires workspace class use admission")
	}
	if apiEnabled && !provenanceAdmissionEnabled {
		// Settlement authorizes controller-privileged revocation and deletion
		// through the reserved acp.workspace.orka.ai/ Task metadata; without
		// the provenance webhook those keys are forgeable by any direct
		// Kubernetes Task writer, so class-backed workspaces must never be
		// served without it.
		return fmt.Errorf("workspace provider API requires Task provenance admission (--task-provenance-admission-enabled) to protect the reserved workspace settlement metadata")
	}
	return nil
}

func validateStaticTrustedServiceReferences(
	watchNamespace string,
	trust outboundaccess.TrustConfig,
) error {
	watchNamespace = strings.TrimSpace(watchNamespace)
	for _, ref := range append(trust.Gateways.References(), trust.TokenEndpoints.References()...) {
		if ref.Namespace != watchNamespace {
			return fmt.Errorf(
				"trusted Service %s/%s:%d must be in controller watch namespace %q",
				ref.Namespace,
				ref.Name,
				ref.Port,
				watchNamespace,
			)
		}
	}
	return nil
}

func workspaceCleanupAPIsInstalled(mapper meta.RESTMapper) (bool, error) {
	for _, gvk := range []schema.GroupVersionKind{
		workspacev1alpha1.GroupVersion.WithKind("ExecutionWorkspaceProvider"),
		workspacev1alpha1.GroupVersion.WithKind("ExecutionWorkspacePool"),
		workspacev1alpha1.GroupVersion.WithKind("ExecutionWorkspaceClass"),
		workspacev1alpha1.GroupVersion.WithKind("ExecutionWorkspace"),
	} {
		if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			if meta.IsNoMatchError(err) {
				return false, nil
			}
			return false, fmt.Errorf("discover %s: %w", gvk.String(), err)
		}
	}
	return true, nil
}

func managerWebhookAdmissionEnabled(taskProvenanceEnabled, workspaceClassUseEnabled bool) bool {
	return taskProvenanceEnabled || workspaceClassUseEnabled
}

func validateDisabledSubstrateRecoveryConfig(
	ctx context.Context,
	reader crclient.Reader,
	watchNamespace string,
	cfg controller.SubstrateConfig,
	configErr error,
) error {
	if reader == nil {
		return fmt.Errorf("kubernetes reader is required to discover existing substrate RuntimePools")
	}

	pools := &corev1alpha1.RuntimePoolList{}
	if err := reader.List(ctx, pools, crclient.InNamespace(strings.TrimSpace(watchNamespace))); err != nil {
		return fmt.Errorf("list RuntimePools for disabled substrate recovery: %w", err)
	}
	for i := range pools.Items {
		workspace := pools.Items[i].Spec.ExecutionWorkspace
		if workspace == nil || workspace.Provider != corev1alpha1.WorkspaceProviderSubstrate {
			continue
		}
		pool := &pools.Items[i]
		if configErr != nil {
			return fmt.Errorf(
				"parse substrate recovery configuration for existing RuntimePool %s/%s: %w",
				pool.Namespace,
				pool.Name,
				configErr,
			)
		}
		if err := cfg.ValidateACPRuntimePool(); err != nil {
			return fmt.Errorf(
				"existing substrate RuntimePool %s/%s requires valid recovery configuration: %w",
				pool.Namespace,
				pool.Name,
				err,
			)
		}
		return nil
	}
	return nil
}

// nolint:gocyclo
func main() {
	acpUpgradeDrainOptions := controller.DefaultACPUpgradeDrainOptions()
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var taskProvenanceAdmissionEnabled bool
	var workspaceClassUseAdmissionEnabled bool
	var taskProvenanceAdmissionTrustedUsers string
	var taskProvenanceAdmissionTrustedServiceAccounts string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var apiPort int
	var watchNamespace string
	var generalWorkerImage string
	var aiWorkerServiceAccountName string
	var vendorWorkerServiceAccountName string
	var containerWorkerServiceAccountName string
	var aiWorkerClusterRoleName string
	var vendorWorkerClusterRoleName string
	var containerWorkerClusterRoleName string
	var workerRoleBindingNamePrefix string
	var chatEnabled bool
	var chatProvider string
	var chatModel string
	var chatMaxIterations int
	var chatMaxDuration time.Duration
	var chatToolTimeout time.Duration
	var chatMaxConcurrent int
	var chatMaxTasksPerTurn int
	var chatMaxSessionSize int
	var chatMaxPrematureEndRetries int
	var gatewayEnabled bool
	var gatewayPendingPerSession int
	var gatewayMaxRecordsPerGateway int
	var gatewayMaxRejectedRecordsPerGateway int
	var gatewayEventExpiry time.Duration
	var gatewayTerminalRetention time.Duration
	var gatewayDeliveryTimeout time.Duration
	var gatewayDeliveryMaxAttempts int
	var gatewayClaimLease time.Duration
	var gatewayPollInterval time.Duration
	var gatewayBatchSize int
	var aiWorkerImage string
	var storeBackend string
	var storePath string
	var agentExecutionSnapshotKeyFile string
	var agentExecutionSnapshotRetention time.Duration
	var agentExecutionSnapshotRetentionInterval time.Duration
	var controllerURL string
	var enforceNamespaceIsolation bool
	var maxTasksPerNamespace int
	var controllerModeValue string
	var executionModeControllerUsernames string
	var harnessV1Endpoint string
	var harnessV1CAFile string
	var harnessV1AuthSecretNamespace string
	var harnessV1AuthSecretName string
	var harnessV1AuthSecretKey string
	var harnessV1DispatchInterval time.Duration
	var harnessV1DispatchWorkers int
	var acpIdlePoolTTL time.Duration
	var acpCodexRuntimeImage string
	var acpClaudeRuntimeImage string
	var acpCopilotRuntimeImage string
	var acpOpencodeRuntimeImage string
	var acpRuntimeNamespace string
	var acpProviderProxyNamespace string
	var acpProviderProxyBaseURL string
	var acpProviderProxyPodLabels string
	var acpProviderProxyTokenFile string
	var acpE2EPromptWriteAmbiguityMarker string
	var agentSandboxEnabled bool
	var acpWorkspaceDispatchEnabled bool
	var agentSandboxCleanupPolicy string
	var oidcIssuer string
	var oidcAudience string
	var oidcJWKSURL string
	var oidcAllowedSubjects string
	var oidcNamespace string
	var contextTokenProfile string
	var contextTokenIssuer string
	var contextTokenAudience string
	var contextTokenJWKSURL string
	var contextTokenHeaders string
	var contextTokenAuthzMode string
	var contextTokenTaskCreateScopes string
	var contextTokenTaskReadScopes string
	var contextTokenTaskListScopes string
	var contextTokenTaskDeleteScopes string
	var contextTokenTaskUpdateScopes string
	var contextTokenToolReadScopes string
	var contextTokenToolUseScopes string
	var contextTokenProviderUseScopes string
	var contextTokenSecretReadScopes string
	var contextTokenSecretCredentialReadScopes string
	var contextTokenConfigMapReadScopes string
	var contextTokenAgentReadScopes string
	var contextTokenAgentWriteScopes string
	var contextTokenMemoryReadScopes string
	var contextTokenMemoryWriteScopes string
	var contextTokenSessionReadScopes string
	var contextTokenSessionWriteScopes string
	var contextTokenSecurityReadScopes string
	var contextTokenSecurityWriteScopes string
	var contextTokenMonitorReadScopes string
	var contextTokenMonitorWriteScopes string
	var contextTokenMonitorOperateScopes string
	var contextTokenSkillReadScopes string
	var contextTokenSkillWriteScopes string
	var contextTokenGatewayReadScopes string
	var contextTokenGatewayOperateScopes string
	var contextTokenTTSEndpoint string
	var contextTokenTTSAudience string
	var contextTokenTTSTimeout string
	var contextTokenTTSTokenSource string
	var contextTokenSubjectTokenType string
	var contextTokenChildScope string
	var contextTokenOutboundScope string
	var contextTokenChildTokenTTL string
	var contextTokenToolTokenTTL string
	var outboundAccessTrustedGatewayServices string
	var outboundAccessTrustedTokenEndpointServices string
	var enableTracing bool
	var workspaceProviderAPIEnabled bool
	var fakeWorkspaceProviderEnabled bool
	var tlsOpts []func(*tls.Config)

	executionWorkspaceDefaultProvider := controller.ExecutionWorkspaceDefaultProviderFromEnv(os.Getenv)
	executionWorkspaceDefaultProviderFlag := string(executionWorkspaceDefaultProvider)
	agentSandboxEnabled = strings.EqualFold(os.Getenv("ORKA_AGENT_SANDBOX_ENABLED"), "true")
	acpWorkspaceDispatchEnabled = strings.EqualFold(os.Getenv("ORKA_ACP_WORKSPACE_DISPATCH_ENABLED"), "true")
	agentSandboxConfig, agentSandboxConfigErr := controller.AgentSandboxConfigFromEnv(os.Getenv)
	agentSandboxCleanupPolicy = string(agentSandboxConfig.CleanupPolicy)
	substrateEnabled := strings.EqualFold(os.Getenv("ORKA_SUBSTRATE_ENABLED"), "true")
	substrateConfig, substrateConfigErr := controller.SubstrateConfigFromEnv(os.Getenv)
	substrateCleanupPolicy := string(substrateConfig.CleanupPolicy)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&controllerModeValue, "controller-mode", os.Getenv("ORKA_CONTROLLER_MODE"),
		"Required controller mode: harness-v1 or harness-v2. An installation never serves both modes.")
	flag.StringVar(&executionModeControllerUsernames, "execution-mode-controller-usernames",
		os.Getenv("ORKA_EXECUTION_MODE_CONTROLLER_USERNAMES"),
		"Comma-separated exact Kubernetes usernames authorized for controller-owned admission writes.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.BoolVar(&taskProvenanceAdmissionEnabled, "task-provenance-admission-enabled",
		envBool("ORKA_TASK_PROVENANCE_ADMISSION_ENABLED"),
		"Enable validating admission that rejects untrusted direct Task writes to Orka-managed "+
			"provenance fields.")
	flag.StringVar(&taskProvenanceAdmissionTrustedUsers, "task-provenance-admission-trusted-users",
		os.Getenv("ORKA_TASK_PROVENANCE_ADMISSION_TRUSTED_USERS"),
		"Comma-separated Kubernetes usernames trusted to set Orka-managed Task provenance fields. "+
			"Defaults to the controller ServiceAccount usernames in the controller namespace.")
	flag.StringVar(&taskProvenanceAdmissionTrustedServiceAccounts,
		"task-provenance-admission-trusted-service-accounts",
		os.Getenv("ORKA_TASK_PROVENANCE_ADMISSION_TRUSTED_SERVICE_ACCOUNTS"),
		"Comma-separated ServiceAccount names trusted in the target Task namespace to set "+
			"Orka-managed Task provenance fields. Defaults to the configured AI and vendor worker ServiceAccounts.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&apiPort, "api-port", 8080, "The port the REST API server binds to.")
	flag.StringVar(&watchNamespace, "watch-namespace", "", "Required single namespace to watch for resources.")
	flag.BoolVar(&workspaceProviderAPIEnabled, "enable-workspace-provider-api",
		envBool("ORKA_ENABLE_WORKSPACE_PROVIDER_API"),
		"Enable workspace.orka.ai provider/class/pool/workspace coordination controllers.")
	flag.BoolVar(&workspaceClassUseAdmissionEnabled, "workspace-class-use-admission-enabled",
		envBool("ORKA_WORKSPACE_CLASS_USE_ADMISSION_ENABLED"),
		"Enable fail-closed Task and Tool admission checks for ExecutionWorkspaceClass use.")
	flag.BoolVar(&fakeWorkspaceProviderEnabled, "enable-fake-workspace-provider",
		envBool("ORKA_ENABLE_FAKE_WORKSPACE_PROVIDER"),
		"Enable the development-only fake.workspace.orka.ai/v1 adapter; requires --enable-workspace-provider-api.")
	flag.StringVar(&aiWorkerImage, "ai-worker-image",
		controller.DefaultAIWorkerImage, "Container image for AI worker.")
	flag.StringVar(&generalWorkerImage, "general-worker-image",
		controller.DefaultGeneralWorkerImage, "Container image for general worker.")
	flag.StringVar(&aiWorkerServiceAccountName, "ai-worker-service-account-name",
		controller.AIWorkerServiceAccount, "ServiceAccount name for AI worker tasks.")
	flag.StringVar(&vendorWorkerServiceAccountName, "vendor-worker-service-account-name",
		controller.VendorWorkerServiceAccount, "ServiceAccount name for vendor worker tasks.")
	flag.StringVar(&containerWorkerServiceAccountName, "container-worker-service-account-name",
		controller.ContainerWorkerServiceAccount, "ServiceAccount name for container worker tasks.")
	flag.StringVar(&aiWorkerClusterRoleName, "ai-worker-cluster-role-name",
		controller.DefaultAIWorkerClusterRoleName, "ClusterRole name for AI worker tasks.")
	flag.StringVar(&vendorWorkerClusterRoleName, "vendor-worker-cluster-role-name",
		controller.DefaultVendorWorkerClusterRoleName, "ClusterRole name for vendor worker tasks.")
	flag.StringVar(&containerWorkerClusterRoleName, "container-worker-cluster-role-name",
		controller.DefaultContainerWorkerClusterRoleName, "ClusterRole name for container worker tasks.")
	flag.StringVar(&workerRoleBindingNamePrefix, "worker-role-binding-prefix",
		os.Getenv("ORKA_WORKER_ROLE_BINDING_PREFIX"),
		"Prefix for per-namespace worker RoleBinding names. Empty uses the 'orka' prefix.")
	flag.BoolVar(&chatEnabled, "chat-enabled", true, "Enable the chat endpoint.")
	flag.StringVar(&chatProvider, "chat-provider", "", "Default Provider CRD name for chat.")
	flag.StringVar(&chatModel, "chat-model", "", "Default model for chat.")
	flag.IntVar(&chatMaxIterations, "chat-max-iterations", 50, "Max tool execution loops per chat request.")
	flag.DurationVar(&chatMaxDuration, "chat-max-duration", 30*time.Minute, "Max wall-clock time per chat request.")
	flag.DurationVar(&chatToolTimeout, "chat-tool-timeout", 60*time.Second, "Max time for a single tool execution.")
	flag.IntVar(&chatMaxConcurrent, "chat-max-concurrent", 10, "Max concurrent chat sessions.")
	flag.IntVar(&chatMaxTasksPerTurn, "chat-max-tasks-per-turn", 5, "Max tasks created per chat turn.")
	flag.IntVar(&chatMaxSessionSize, "chat-max-session-size", 500*1024,
		"Soft limit for session ConfigMap size before truncation (bytes).")
	flag.IntVar(&chatMaxPrematureEndRetries, "chat-max-premature-end-retries", 3,
		"How many times to re-prompt the coordinator with 'continue with tool_use' before accepting a "+
			"no-tool-use response as the final turn. The model must emit the GOAL_STATE sentinel on its true "+
			"final turn — see coordinatorSystemPrompt.")
	flag.BoolVar(&gatewayEnabled, "gateway-enabled", true, "Enable generic gateway reconciliation and ingress.")
	flag.IntVar(&gatewayPendingPerSession, "gateway-pending-per-session", 100,
		"Maximum pending gateway events per Session.")
	flag.IntVar(&gatewayMaxRecordsPerGateway, "gateway-max-records-per-gateway", 1000,
		"Maximum retained gateway event records per Gateway before ingress is throttled.")
	flag.IntVar(&gatewayMaxRejectedRecordsPerGateway, "gateway-max-rejected-records-per-gateway", 250,
		"Maximum retained rejected event audit records per Gateway.")
	flag.DurationVar(&gatewayEventExpiry, "gateway-event-expiry", 24*time.Hour,
		"Maximum age for queued events and delivery retries.")
	flag.DurationVar(&gatewayTerminalRetention, "gateway-terminal-retention", 30*24*time.Hour,
		"Retention for terminal gateway events and deliveries.")
	flag.DurationVar(&gatewayDeliveryTimeout, "gateway-delivery-timeout", 15*time.Second,
		"Timeout for one synchronous adapter delivery call.")
	flag.IntVar(&gatewayDeliveryMaxAttempts, "gateway-delivery-max-attempts", 10,
		"Maximum adapter delivery attempts before dead-lettering.")
	flag.DurationVar(&gatewayClaimLease, "gateway-claim-lease", time.Minute,
		"Lease duration for gateway event and delivery claims.")
	flag.DurationVar(&gatewayPollInterval, "gateway-poll-interval", 500*time.Millisecond,
		"Gateway dispatcher and delivery poll interval.")
	flag.IntVar(&gatewayBatchSize, "gateway-batch-size", 25,
		"Maximum gateway events and deliveries processed per iteration.")
	flag.StringVar(&storeBackend, "store-backend", "sqlite", "Storage backend (sqlite)")
	flag.StringVar(&storePath, "store-path", "/data/orka.db", "Path to SQLite database file")
	flag.StringVar(&agentExecutionSnapshotKeyFile, "agent-execution-snapshot-key-file", "",
		"Path to the 32-byte (raw or base64) AES-256 key encrypting immutable agent execution snapshots. "+
			"When set, executable agent Tasks freeze a write-once binding and encrypted snapshot before dispatch.")
	flag.DurationVar(&agentExecutionSnapshotRetention, "agent-execution-snapshot-retention",
		envDurationDefault("ORKA_AGENT_EXECUTION_SNAPSHOT_RETENTION", controller.DefaultAgentExecutionSnapshotRetention),
		"Minimum audit/backup retention period for encrypted execution snapshots after all references disappear.")
	flag.DurationVar(&agentExecutionSnapshotRetentionInterval, "agent-execution-snapshot-retention-interval",
		envDurationDefault("ORKA_AGENT_EXECUTION_SNAPSHOT_RETENTION_INTERVAL", controller.DefaultAgentExecutionSnapshotRetentionInterval),
		"Interval between reference-aware encrypted execution snapshot retention scans.")
	flag.StringVar(&controllerURL, "controller-url", "",
		"Base URL for the controller API, used by workers. E.g. http://orka-controller.orka-system.svc:8080")
	flag.BoolVar(&enforceNamespaceIsolation, "enforce-namespace-isolation", false,
		"When true, restrict users to their ServiceAccount's namespace for all operations.")
	flag.IntVar(&maxTasksPerNamespace, "max-tasks-per-namespace", 0,
		"Maximum active tasks per namespace (0 = unlimited).")
	flag.StringVar(&harnessV1Endpoint, "harness-v1-endpoint", os.Getenv("ORKA_HARNESS_V1_ENDPOINT"),
		"Base URL of the built-in harness v1 wrapper Service.")
	flag.StringVar(&harnessV1CAFile, "harness-v1-ca-file", os.Getenv("ORKA_HARNESS_V1_CA_FILE"),
		"CA bundle used to authenticate built-in and registered harness v1 Services.")
	flag.StringVar(&harnessV1AuthSecretNamespace, "harness-v1-auth-secret-namespace",
		os.Getenv("ORKA_HARNESS_V1_AUTH_SECRET_NAMESPACE"),
		"Namespace of the dedicated harness v1 wrapper bearer-token Secret.")
	flag.StringVar(&harnessV1AuthSecretName, "harness-v1-auth-secret-name",
		os.Getenv("ORKA_HARNESS_V1_AUTH_SECRET_NAME"),
		"Name of the dedicated harness v1 wrapper bearer-token Secret.")
	flag.StringVar(&harnessV1AuthSecretKey, "harness-v1-auth-secret-key",
		envStringDefault("ORKA_HARNESS_V1_AUTH_SECRET_KEY", "token"),
		"Key in the dedicated harness v1 wrapper bearer-token Secret.")
	flag.DurationVar(&harnessV1DispatchInterval, "harness-v1-dispatch-interval",
		envDurationDefault("ORKA_HARNESS_V1_DISPATCH_INTERVAL", controller.DefaultHarnessV1DispatchInterval),
		"Interval between durable harness v1 attempt recovery scans.")
	flag.IntVar(&harnessV1DispatchWorkers, "harness-v1-dispatch-workers",
		controller.DefaultHarnessV1DispatchWorkers,
		"Maximum concurrent harness v1 attempt workers.")
	flag.DurationVar(&acpIdlePoolTTL, "acp-idle-pool-ttl", envDurationDefault("ORKA_ACP_IDLE_POOL_TTL", controller.DefaultACPIdlePoolTTL),
		"Scale an idle ACP RuntimePool to zero after this duration.")
	flag.StringVar(&acpCodexRuntimeImage, "acp-codex-runtime-image", os.Getenv("ORKA_ACP_CODEX_RUNTIME_IMAGE"),
		"Digest-pinned Codex ACP runtime image.")
	flag.StringVar(&acpClaudeRuntimeImage, "acp-claude-runtime-image", os.Getenv("ORKA_ACP_CLAUDE_RUNTIME_IMAGE"),
		"Digest-pinned Claude ACP runtime image.")
	flag.StringVar(&acpCopilotRuntimeImage, "acp-copilot-runtime-image", os.Getenv("ORKA_ACP_COPILOT_RUNTIME_IMAGE"),
		"Digest-pinned Copilot ACP runtime image.")
	flag.StringVar(&acpOpencodeRuntimeImage, "acp-opencode-runtime-image", os.Getenv("ORKA_ACP_OPENCODE_RUNTIME_IMAGE"),
		"Digest-pinned OpenCode ACP runtime image.")
	flag.StringVar(&acpRuntimeNamespace, "acp-runtime-namespace", envStringDefault("ORKA_ACP_RUNTIME_NAMESPACE", "orka-runtimes"),
		"Physical namespace for managed ACP runtime Pods.")
	flag.StringVar(&acpProviderProxyNamespace, "acp-provider-proxy-namespace", envStringDefault("ORKA_ACP_PROVIDER_PROXY_NAMESPACE", "vekil-system"),
		"Namespace containing the approved credential-injecting provider proxy.")
	flag.StringVar(&acpProviderProxyBaseURL, "acp-provider-proxy-base-url", os.Getenv("ORKA_ACP_PROVIDER_PROXY_BASE_URL"),
		"Cluster-local base URL of the authenticated provider proxy boundary.")
	flag.StringVar(&acpProviderProxyPodLabels, "acp-provider-proxy-pod-labels", envStringDefault("ORKA_ACP_PROVIDER_PROXY_POD_LABELS", "orka.ai/network-role=provider-auth-proxy"),
		"Comma-separated exact Pod labels selected by RuntimePool provider-proxy egress policy.")
	flag.StringVar(&acpProviderProxyTokenFile, "acp-provider-proxy-token-file", os.Getenv("ORKA_ACP_PROVIDER_PROXY_TOKEN_FILE"),
		"Mounted file containing the authenticated provider proxy bearer token.")
	flag.StringVar(&acpE2EPromptWriteAmbiguityMarker, "acp-e2e-prompt-write-ambiguity-marker", os.Getenv("ORKA_ACP_E2E_PROMPT_WRITE_AMBIGUITY_MARKER"),
		"Test-only exact prompt marker that aborts a fully validated ACP prompt request before acceptance is recorded.")
	flag.StringVar(&executionWorkspaceDefaultProviderFlag, "execution-workspace-default-provider",
		executionWorkspaceDefaultProviderFlag,
		"Default execution workspace provider when Task execution.workspace.provider is omitted (agent-sandbox, substrate).")
	flag.BoolVar(&agentSandboxEnabled, "agent-sandbox-enabled", agentSandboxEnabled,
		"Enable experimental agent sandbox workspace execution for agent Tasks.")
	flag.BoolVar(&acpWorkspaceDispatchEnabled, "acp-workspace-dispatch-enabled", acpWorkspaceDispatchEnabled,
		"Admit workspace-provider-backed ACP RuntimeSession dispatch (requires the matching --agent-sandbox-enabled or --substrate-enabled provider flag); when false, Task.spec.execution.workspace agent Tasks fail closed.")
	flag.StringVar(&agentSandboxConfig.RouterURL, "agent-sandbox-router-url", agentSandboxConfig.RouterURL,
		"Agent sandbox router base URL used by worker Jobs for workspace claims.")
	flag.StringVar(&agentSandboxConfig.DefaultTemplate, "agent-sandbox-default-template",
		agentSandboxConfig.DefaultTemplate,
		"Default agent-sandbox SandboxWarmPool name used when a Task omits execution.workspace.templateRef.name.")
	flag.StringVar(&agentSandboxConfig.WarmPoolPolicy, "agent-sandbox-warm-pool-policy",
		agentSandboxConfig.WarmPoolPolicy,
		"Agent sandbox warm pool policy (disabled, template).")
	flag.StringVar(&agentSandboxConfig.NamespaceStrategy, "agent-sandbox-namespace-strategy",
		agentSandboxConfig.NamespaceStrategy,
		"Agent sandbox namespace strategy (task, controller).")
	flag.DurationVar(&agentSandboxConfig.ClaimTimeout, "agent-sandbox-claim-timeout",
		agentSandboxConfig.ClaimTimeout,
		"Timeout for agent sandbox workspace claim and readiness operations.")
	flag.DurationVar(&agentSandboxConfig.CommandTimeout, "agent-sandbox-command-timeout",
		agentSandboxConfig.CommandTimeout,
		"Timeout for agent runtime execution inside the sandbox.")
	flag.StringVar(&agentSandboxCleanupPolicy, "agent-sandbox-cleanup-policy", agentSandboxCleanupPolicy,
		"Default agent sandbox workspace cleanup policy (delete, retain).")
	flag.BoolVar(&substrateEnabled, "substrate-enabled", substrateEnabled,
		"Enable experimental Substrate execution workspace provider for agent Tasks.")
	flag.StringVar(&substrateConfig.APIEndpoint, "substrate-api-endpoint", substrateConfig.APIEndpoint,
		"Substrate control API endpoint used by worker Jobs.")
	flag.StringVar(&substrateConfig.APICAFile, "substrate-api-ca-file", substrateConfig.APICAFile,
		"CA bundle file for the Substrate control API.")
	flag.BoolVar(&substrateConfig.APIInsecureSkipVerify, "substrate-api-insecure-skip-verify",
		substrateConfig.APIInsecureSkipVerify,
		"Skip Substrate control API certificate verification. Only for local smoke tests.")
	flag.StringVar(&substrateConfig.RouterURL, "substrate-router-url", substrateConfig.RouterURL,
		"Substrate router base URL used by worker Jobs for actor daemon calls.")
	flag.StringVar(&substrateConfig.ActorDNSSuffix, "substrate-actor-dns-suffix", substrateConfig.ActorDNSSuffix,
		"DNS suffix used to route HTTP requests to active Substrate actors.")
	flag.StringVar(&substrateConfig.DefaultTemplate, "substrate-default-template", substrateConfig.DefaultTemplate,
		"Default Substrate ActorTemplate name used when a Task omits execution.workspace.templateRef.name.")
	flag.StringVar(&substrateConfig.DefaultTemplateNS, "substrate-default-template-namespace",
		substrateConfig.DefaultTemplateNS,
		"Default Substrate ActorTemplate namespace used when a Task omits execution.workspace.templateRef.namespace.")
	flag.StringVar(&substrateConfig.BootstrapSecretName, "substrate-bootstrap-token-secret-name",
		substrateConfig.BootstrapSecretName,
		"Kubernetes Secret name containing the Substrate workspace daemon bootstrap token in each Task namespace.")
	flag.StringVar(&substrateConfig.BootstrapSecretKey, "substrate-bootstrap-token-secret-key",
		substrateConfig.BootstrapSecretKey,
		"Kubernetes Secret key containing the Substrate workspace daemon bootstrap token.")
	flag.StringVar(&substrateConfig.SessionIdentitySecretName, "substrate-session-identity-token-secret-name",
		substrateConfig.SessionIdentitySecretName,
		"Kubernetes Secret name containing the bearer token for Substrate SessionIdentity.")
	flag.StringVar(&substrateConfig.SessionIdentitySecretKey, "substrate-session-identity-token-secret-key",
		substrateConfig.SessionIdentitySecretKey,
		"Kubernetes Secret key containing the bearer token for Substrate SessionIdentity.")
	flag.BoolVar(&substrateConfig.SessionIdentityRequired, "substrate-session-identity-required",
		substrateConfig.SessionIdentityRequired,
		"Fail Substrate workspace handoff when SessionIdentity cannot mint a per-actor JWT.")
	flag.BoolVar(&substrateConfig.SessionIdentityMintCert, "substrate-session-identity-mint-cert",
		substrateConfig.SessionIdentityMintCert,
		"Unsupported alpha option for Substrate SessionIdentity certificate minting; currently rejected when enabled.")
	flag.StringVar(&substrateConfig.SessionIdentityAudience, "substrate-session-identity-audience",
		substrateConfig.SessionIdentityAudience,
		"Comma-separated audiences requested from Substrate SessionIdentity minted JWTs.")
	flag.StringVar(&substrateConfig.SessionIdentityAppID, "substrate-session-identity-app-id",
		substrateConfig.SessionIdentityAppID,
		"Application ID requested from Substrate SessionIdentity minted JWTs.")
	flag.StringVar(&substrateConfig.SessionIdentityUserID, "substrate-session-identity-user-id",
		substrateConfig.SessionIdentityUserID,
		"User ID requested from Substrate SessionIdentity minted JWTs.")
	flag.DurationVar(&substrateConfig.ClaimTimeout, "substrate-claim-timeout", substrateConfig.ClaimTimeout,
		"Timeout for Substrate actor claim, readiness, release, retain, and delete operations.")
	flag.DurationVar(&substrateConfig.CommandTimeout, "substrate-command-timeout", substrateConfig.CommandTimeout,
		"Timeout for agent runtime execution inside the Substrate actor.")
	flag.StringVar(&substrateCleanupPolicy, "substrate-cleanup-policy", substrateCleanupPolicy,
		"Default Substrate workspace cleanup policy (delete, retain).")
	flag.StringVar(&oidcIssuer, "oidc-issuer", os.Getenv("ORKA_OIDC_ISSUER"),
		"OIDC issuer URL for authenticating external API requests. Requires --oidc-audience when set.")
	flag.StringVar(&oidcAudience, "oidc-audience", os.Getenv("ORKA_OIDC_AUDIENCE"),
		"OIDC audience expected in external API bearer tokens. Requires --oidc-issuer when set.")
	flag.StringVar(&oidcJWKSURL, "oidc-jwks-url", os.Getenv("ORKA_OIDC_JWKS_URL"),
		"Optional OIDC JWKS URL. When empty, it is discovered from the issuer metadata.")
	flag.StringVar(&oidcAllowedSubjects, "oidc-allowed-subjects", os.Getenv("ORKA_OIDC_ALLOWED_SUBJECTS"),
		"Comma-separated OIDC subject allowlist patterns. Required when OIDC is enabled; supports shell-style wildcards.")
	flag.StringVar(&oidcNamespace, "oidc-namespace", os.Getenv("ORKA_OIDC_NAMESPACE"),
		"Namespace assigned to authorized OIDC callers for namespace isolation. Defaults to default.")
	flag.StringVar(&contextTokenProfile, "context-token-profile", os.Getenv("ORKA_CONTEXT_TOKEN_PROFILE"),
		"Context-token profile for external API requests (supported: transaction-token).")
	flag.StringVar(&contextTokenIssuer, "context-token-issuer", os.Getenv("ORKA_CONTEXT_TOKEN_ISSUER"),
		"Context-token issuer URL. Requires --context-token-profile and --context-token-audience when set.")
	flag.StringVar(&contextTokenAudience, "context-token-audience", os.Getenv("ORKA_CONTEXT_TOKEN_AUDIENCE"),
		"Context-token audience expected in external API tokens. "+
			"Requires --context-token-profile and --context-token-issuer when set.")
	flag.StringVar(&contextTokenJWKSURL, "context-token-jwks-url", os.Getenv("ORKA_CONTEXT_TOKEN_JWKS_URL"),
		"Optional context-token JWKS URL. For transaction-token, defaults to <issuer>/.well-known/jwks.json.")
	flag.StringVar(&contextTokenHeaders, "context-token-headers", os.Getenv("ORKA_CONTEXT_TOKEN_HEADERS"),
		"Comma-separated context-token headers. Use Header for raw tokens or Header:Scheme for scheme-prefixed "+
			"tokens (default for transaction-token: Txn-Token; bearer opt-in: Txn-Token,Authorization:Bearer).")
	flag.StringVar(&contextTokenAuthzMode, "context-token-authz-mode", os.Getenv("ORKA_CONTEXT_TOKEN_AUTHZ_MODE"),
		"Context-token authorization mode: off, audit, or enforce. Empty defaults to off.")
	flag.StringVar(&contextTokenTaskCreateScopes, "context-token-task-create-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TASK_CREATE_SCOPES"),
		"Comma-separated context-token scopes that authorize Task creation. Defaults to orka:tasks:create.")
	flag.StringVar(&contextTokenTaskReadScopes, "context-token-task-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TASK_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize Task reads and related data. Defaults to orka:tasks:get.")
	flag.StringVar(&contextTokenTaskListScopes, "context-token-task-list-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TASK_LIST_SCOPES"),
		"Comma-separated context-token scopes that authorize Task listing. Defaults to orka:tasks:list.")
	flag.StringVar(&contextTokenTaskDeleteScopes, "context-token-task-delete-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TASK_DELETE_SCOPES"),
		"Comma-separated context-token scopes that authorize Task deletion. Defaults to orka:tasks:delete.")
	flag.StringVar(&contextTokenTaskUpdateScopes, "context-token-task-update-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TASK_UPDATE_SCOPES"),
		"Comma-separated context-token scopes that authorize Task-adjacent mutations. Defaults to orka:tasks:update.")
	flag.StringVar(&contextTokenToolReadScopes, "context-token-tool-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TOOL_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize Tool reads. Defaults to orka:tools:read.")
	flag.StringVar(&contextTokenToolUseScopes, "context-token-tool-use-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_TOOL_USE_SCOPES"),
		"Comma-separated context-token scopes that authorize Orka-managed tool execution. Defaults to orka:tools:use.")
	flag.StringVar(&contextTokenProviderUseScopes, "context-token-provider-use-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_PROVIDER_USE_SCOPES"),
		"Comma-separated context-token scopes that authorize model provider use and listing. Defaults to orka:providers:use.")
	flag.StringVar(&contextTokenSecretReadScopes, "context-token-secret-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SECRET_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize Secret metadata reads. Defaults to orka:secrets:read.")
	flag.StringVar(&contextTokenSecretCredentialReadScopes, "context-token-secret-credential-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SECRET_CREDENTIAL_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize using Secret data or ServiceAccount tokens "+
			"as outbound credentials. Defaults to orka:secrets:credentials:read.")
	flag.StringVar(&contextTokenConfigMapReadScopes, "context-token-configmap-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_CONFIGMAP_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize ConfigMap reads used as operation inputs. "+
			"Defaults to orka:configmaps:read.")
	flag.StringVar(&contextTokenAgentReadScopes, "context-token-agent-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_AGENT_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize Agent reads. Defaults to orka:agents:read.")
	flag.StringVar(&contextTokenAgentWriteScopes, "context-token-agent-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_AGENT_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize Agent writes. Defaults to orka:agents:write.")
	flag.StringVar(&contextTokenMemoryReadScopes, "context-token-memory-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_MEMORY_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize memory reads. Defaults to orka:memory:read.")
	flag.StringVar(&contextTokenMemoryWriteScopes, "context-token-memory-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_MEMORY_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize memory writes. Defaults to orka:memory:write.")
	flag.StringVar(&contextTokenSessionReadScopes, "context-token-session-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SESSION_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize session reads. Defaults to orka:sessions:read.")
	flag.StringVar(&contextTokenSessionWriteScopes, "context-token-session-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SESSION_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize session writes. Defaults to orka:sessions:write.")
	flag.StringVar(&contextTokenSecurityReadScopes, "context-token-security-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SECURITY_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize security scan reads. Defaults to orka:security:read.")
	flag.StringVar(&contextTokenSecurityWriteScopes, "context-token-security-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SECURITY_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize security scan writes. Defaults to orka:security:write.")
	flag.StringVar(&contextTokenMonitorReadScopes, "context-token-monitor-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_MONITOR_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize repository monitor reads. Defaults to orka:monitors:read.")
	flag.StringVar(&contextTokenMonitorWriteScopes, "context-token-monitor-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_MONITOR_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize repository monitor writes. Defaults to orka:monitors:write.")
	flag.StringVar(&contextTokenMonitorOperateScopes, "context-token-monitor-operate-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_MONITOR_OPERATE_SCOPES"),
		"Comma-separated context-token scopes that authorize repository monitor operations. "+
			"Defaults to orka:monitors:operate.")
	flag.StringVar(&contextTokenSkillReadScopes, "context-token-skill-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SKILL_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize Skill reads. Defaults to orka:skills:read.")
	flag.StringVar(&contextTokenSkillWriteScopes, "context-token-skill-write-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_SKILL_WRITE_SCOPES"),
		"Comma-separated context-token scopes that authorize Skill writes. Defaults to orka:skills:write.")
	flag.StringVar(&contextTokenGatewayReadScopes, "context-token-gateway-read-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_GATEWAY_READ_SCOPES"),
		"Comma-separated context-token scopes that authorize gateway reads. Defaults to orka:gateways:read.")
	flag.StringVar(&contextTokenGatewayOperateScopes, "context-token-gateway-operate-scopes",
		os.Getenv("ORKA_CONTEXT_TOKEN_GATEWAY_OPERATE_SCOPES"),
		"Comma-separated context-token scopes that authorize delivery retries. Defaults to orka:gateways:operate.")
	flag.StringVar(&contextTokenTTSEndpoint, "context-token-tts-endpoint", os.Getenv("ORKA_CONTEXT_TOKEN_TTS_ENDPOINT"),
		"Exact transaction-token TTS OAuth endpoint for optional token exchange/replacement.")
	flag.StringVar(&contextTokenTTSAudience, "context-token-tts-audience", os.Getenv("ORKA_CONTEXT_TOKEN_TTS_AUDIENCE"),
		"Audience to request from transaction-token TTS exchanges.")
	flag.StringVar(&contextTokenTTSTimeout, "context-token-tts-timeout", os.Getenv("ORKA_CONTEXT_TOKEN_TTS_TIMEOUT"),
		"Timeout for transaction-token TTS exchanges. Defaults to 5s when TTS is enabled.")
	flag.StringVar(&contextTokenTTSTokenSource, "context-token-tts-token-source",
		os.Getenv("ORKA_CONTEXT_TOKEN_TTS_TOKEN_SOURCE"),
		"Subject token source for transaction-token TTS exchanges: serviceAccount, incoming, or none.")
	flag.StringVar(&contextTokenSubjectTokenType, "context-token-subject-token-type",
		os.Getenv("ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_TYPE"),
		"Subject token type for worker-side transaction-token TTS exchanges. Defaults from token source when empty.")
	flag.StringVar(&contextTokenChildScope, "context-token-child-scope", os.Getenv("ORKA_CONTEXT_TOKEN_CHILD_SCOPE"),
		"Scope workers request for child delegated TxTokens when TTS is configured.")
	flag.StringVar(&contextTokenOutboundScope, "context-token-outbound-scope",
		os.Getenv("ORKA_CONTEXT_TOKEN_OUTBOUND_SCOPE"),
		"Scope workers request for outbound HTTP Tool TxTokens when TTS is configured.")
	flag.StringVar(&contextTokenChildTokenTTL, "context-token-child-token-ttl",
		os.Getenv("ORKA_CONTEXT_TOKEN_CHILD_TOKEN_TTL"),
		"Requested TTL for child delegation TxTokens. Defaults to 5m when TTS is enabled.")
	flag.StringVar(&contextTokenToolTokenTTL, "context-token-tool-token-ttl",
		os.Getenv("ORKA_CONTEXT_TOKEN_TOOL_TOKEN_TTL"),
		"Requested TTL for outbound tool TxTokens. Defaults to 2m when TTS is enabled.")
	flag.StringVar(&outboundAccessTrustedGatewayServices, "outbound-access-trusted-gateway-services",
		os.Getenv("ORKA_OUTBOUND_ACCESS_TRUSTED_GATEWAY_SERVICES"),
		"Comma-separated exact namespace/name:port Service references allowed for cross-namespace outbound gateways.")
	flag.StringVar(&outboundAccessTrustedTokenEndpointServices, "outbound-access-trusted-token-endpoint-services",
		os.Getenv("ORKA_OUTBOUND_ACCESS_TRUSTED_TOKEN_ENDPOINT_SERVICES"),
		"Comma-separated exact namespace/name:port Service references allowed for cross-namespace token endpoints.")
	flag.BoolVar(&enableTracing, "enable-telemetry", false,
		"Enable OpenTelemetry tracing and metrics. Configure endpoint via OTEL_EXPORTER_OTLP_ENDPOINT env var.")
	flag.BoolVar(&enableTracing, "enable-tracing", false,
		"Alias for --enable-telemetry; enables OpenTelemetry traces and metrics.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	acpUpgradeDrainOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	if handled, err := controller.RunACPUpgradeDrainTriggerMode(context.Background(), acpUpgradeDrainOptions); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, "ACP planned-upgrade drain trigger failed")
			os.Exit(1)
		}
		return
	}
	mode, err := executionmode.Parse(controllerModeValue)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	watchNamespace = strings.TrimSpace(watchNamespace)
	if watchNamespace == "" {
		fmt.Fprintln(os.Stderr, "--watch-namespace is required; controller modes cannot use a cluster-wide watch")
		os.Exit(1)
	}
	acpUpgradeDrainOptions.WatchNamespace = watchNamespace
	if !enforceNamespaceIsolation {
		fmt.Fprintln(os.Stderr, "--enforce-namespace-isolation=true is required for a static controller installation")
		os.Exit(1)
	}
	harnessV1Enabled := mode == executionmode.HarnessV1
	acpRuntimeEnabled := mode == executionmode.HarnessV2
	if harnessV1Enabled {
		// Cluster-scoped gateway/workspace infrastructure is owned only by the
		// harness-v2 installation. A v1 installation must remain namespaced.
		gatewayEnabled = false
		workspaceProviderAPIEnabled = false
		workspaceClassUseAdmissionEnabled = false
		fakeWorkspaceProviderEnabled = false
	}
	if !enableLeaderElection {
		fmt.Fprintln(os.Stderr, "--leader-elect=true is required for an isolated controller installation")
		os.Exit(1)
	}
	if len(splitCommaList(executionModeControllerUsernames)) == 0 {
		fmt.Fprintln(os.Stderr, "--execution-mode-controller-usernames must contain at least one exact username")
		os.Exit(1)
	}
	if err := validateAgentExecutionSnapshotOptions(
		mode,
		agentExecutionSnapshotKeyFile,
		agentExecutionSnapshotRetention,
		agentExecutionSnapshotRetentionInterval,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if harnessV1Enabled {
		missing := make([]string, 0, 5)
		for name, value := range map[string]string{
			"--harness-v1-endpoint":              harnessV1Endpoint,
			"--harness-v1-ca-file":               harnessV1CAFile,
			"--harness-v1-auth-secret-namespace": harnessV1AuthSecretNamespace,
			"--harness-v1-auth-secret-name":      harnessV1AuthSecretName,
			"--harness-v1-auth-secret-key":       harnessV1AuthSecretKey,
		} {
			if strings.TrimSpace(value) == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) != 0 {
			slices.Sort(missing)
			fmt.Fprintf(os.Stderr, "harness v1 requires %s\n", strings.Join(missing, ", "))
			os.Exit(1)
		}
		if err := validateHarnessV1TLSEndpoint(harnessV1Endpoint); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validateHarnessV1DispatchOptions(harnessV1DispatchInterval, harnessV1DispatchWorkers); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	// Empty worker ServiceAccount flags retain the package defaults for callers that
	// explicitly clear a flag, matching the zero-value fallback in the controller.
	if aiWorkerServiceAccountName == "" {
		aiWorkerServiceAccountName = controller.AIWorkerServiceAccount
	}
	if vendorWorkerServiceAccountName == "" {
		vendorWorkerServiceAccountName = controller.VendorWorkerServiceAccount
	}
	if containerWorkerServiceAccountName == "" {
		containerWorkerServiceAccountName = controller.ContainerWorkerServiceAccount
	}
	// Preserve explicit admission overrides, but keep its empty default aligned
	// with the configured trusted worker ServiceAccounts. Container workers remain
	// excluded, matching the existing admission default.
	if len(workerenv.SplitCSV(taskProvenanceAdmissionTrustedServiceAccounts)) == 0 {
		taskProvenanceAdmissionTrustedServiceAccounts = strings.Join([]string{
			aiWorkerServiceAccountName,
			vendorWorkerServiceAccountName,
		}, ",")
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog.Info("configured isolated controller mode", "mode", mode, "namespace", watchNamespace)

	executionWorkspaceDefaultProvider = corev1alpha1.WorkspaceProvider(executionWorkspaceDefaultProviderFlag)
	if !controller.WorkspaceProviderSupported(executionWorkspaceDefaultProvider) {
		setupLog.Error(fmt.Errorf("unsupported execution workspace default provider %q", executionWorkspaceDefaultProvider),
			"invalid execution workspace configuration")
		os.Exit(1)
	}
	agentSandboxConfig.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicy(agentSandboxCleanupPolicy)
	agentSandboxConfig = agentSandboxConfig.WithDefaults()
	if agentSandboxEnabled {
		if agentSandboxConfigErr != nil {
			setupLog.Error(agentSandboxConfigErr, "invalid agent sandbox configuration from environment")
			os.Exit(1)
		}
		if err := agentSandboxConfig.Validate(); err != nil {
			setupLog.Error(err, "invalid agent sandbox configuration")
			os.Exit(1)
		}
	}
	substrateConfig.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicy(substrateCleanupPolicy)
	substrateConfig = substrateConfig.WithDefaults()
	if substrateEnabled {
		if substrateConfigErr != nil {
			setupLog.Error(substrateConfigErr, "invalid substrate configuration from environment")
			os.Exit(1)
		}
		if err := validateEnabledSubstrateConfig(substrateConfig, workspaceProviderAPIEnabled); err != nil {
			setupLog.Error(err, "invalid substrate configuration")
			os.Exit(1)
		}
	}

	contextTokenConfig, err := api.NewContextTokenConfig(
		contextTokenProfile,
		contextTokenIssuer,
		contextTokenAudience,
		contextTokenJWKSURL,
		contextTokenHeaders,
	)
	if err != nil {
		setupLog.Error(err, "invalid context token configuration")
		os.Exit(1)
	}
	contextTokenAuthzConfig, err := api.NewContextTokenAuthorizationConfig(api.ContextTokenAuthorizationConfigOptions{
		Mode:                       contextTokenAuthzMode,
		TaskCreateScopes:           contextTokenTaskCreateScopes,
		TaskReadScopes:             contextTokenTaskReadScopes,
		TaskListScopes:             contextTokenTaskListScopes,
		TaskDeleteScopes:           contextTokenTaskDeleteScopes,
		TaskUpdateScopes:           contextTokenTaskUpdateScopes,
		ToolReadScopes:             contextTokenToolReadScopes,
		ToolUseScopes:              contextTokenToolUseScopes,
		ProviderUseScopes:          contextTokenProviderUseScopes,
		SecretReadScopes:           contextTokenSecretReadScopes,
		SecretCredentialReadScopes: contextTokenSecretCredentialReadScopes,
		ConfigMapReadScopes:        contextTokenConfigMapReadScopes,
		AgentReadScopes:            contextTokenAgentReadScopes,
		AgentWriteScopes:           contextTokenAgentWriteScopes,
		MemoryReadScopes:           contextTokenMemoryReadScopes,
		MemoryWriteScopes:          contextTokenMemoryWriteScopes,
		SessionReadScopes:          contextTokenSessionReadScopes,
		SessionWriteScopes:         contextTokenSessionWriteScopes,
		SecurityReadScopes:         contextTokenSecurityReadScopes,
		SecurityWriteScopes:        contextTokenSecurityWriteScopes,
		MonitorReadScopes:          contextTokenMonitorReadScopes,
		MonitorWriteScopes:         contextTokenMonitorWriteScopes,
		MonitorOperateScopes:       contextTokenMonitorOperateScopes,
		SkillReadScopes:            contextTokenSkillReadScopes,
		SkillWriteScopes:           contextTokenSkillWriteScopes,
		GatewayReadScopes:          contextTokenGatewayReadScopes,
		GatewayOperateScopes:       contextTokenGatewayOperateScopes,
	})
	if err != nil {
		setupLog.Error(err, "invalid context token authorization configuration")
		os.Exit(1)
	}
	contextTokenTTSConfig, err := api.NewContextTokenTTSConfig(
		contextTokenTTSEndpoint,
		contextTokenTTSAudience,
		contextTokenTTSTimeout,
		contextTokenTTSTokenSource,
		contextTokenChildTokenTTL,
		contextTokenToolTokenTTL,
	)
	if err != nil {
		setupLog.Error(err, "invalid context token TTS configuration")
		os.Exit(1)
	}
	trustedGateways, err := outboundaccess.ParseTrustedServiceReferences(outboundAccessTrustedGatewayServices)
	if err != nil {
		setupLog.Error(err, "invalid trusted outbound gateway Service references")
		os.Exit(1)
	}
	trustedTokenEndpoints, err := outboundaccess.ParseTrustedServiceReferences(outboundAccessTrustedTokenEndpointServices)
	if err != nil {
		setupLog.Error(err, "invalid trusted token endpoint Service references")
		os.Exit(1)
	}
	outboundAccessTrust := outboundaccess.TrustConfig{Gateways: trustedGateways, TokenEndpoints: trustedTokenEndpoints}
	if err := validateStaticTrustedServiceReferences(watchNamespace, outboundAccessTrust); err != nil {
		setupLog.Error(err, "invalid static-controller outbound trust configuration")
		os.Exit(1)
	}

	if err := validateWorkspaceProviderSecurityConfig(
		workspaceProviderAPIEnabled,
		workspaceClassUseAdmissionEnabled,
		taskProvenanceAdmissionEnabled,
	); err != nil {
		setupLog.Error(err, "invalid workspace provider security configuration")
		os.Exit(1)
	}
	managerAdmissionEnabled := managerWebhookAdmissionEnabled(
		taskProvenanceAdmissionEnabled,
		workspaceClassUseAdmissionEnabled,
	)

	// Initialize OpenTelemetry tracing (noop when disabled)
	tracingShutdown, err := tracing.Init("orka-controller", enableTracing)
	if err != nil {
		setupLog.Error(err, "failed to initialize tracing")
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracingShutdown(shutdownCtx); err != nil {
			setupLog.Error(err, "failed to shutdown tracing")
		}
	}()

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	processCtx, stopProcess := context.WithCancel(ctrl.SetupSignalHandler())
	defer stopProcess()
	restConfig := ctrl.GetConfigOrDie()
	mgrOptions := ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsServerOptions,
		WebhookServer:                 webhookServer,
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              controllerLeaderElectionID,
		LeaderElectionNamespace:       watchNamespace,
		LeaderElectionReleaseOnCancel: true,
	}

	// Tenant resources are always namespace-scoped. Only harness v2 may also
	// cache RuntimePool child kinds from its separately owned runtime namespace.
	runtimeCacheNamespace := ""
	if acpRuntimeEnabled {
		runtimeCacheNamespace = acpRuntimeNamespace
	}
	mgrOptions.Cache = managerCacheOptions(
		watchNamespace,
		runtimeCacheNamespace,
	)

	mgr, err := ctrl.NewManager(restConfig, mgrOptions)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "unable to create Kubernetes clientset")
		os.Exit(1)
	}
	modeNamespace, err := kubeClient.CoreV1().Namespaces().Get(
		context.Background(), watchNamespace, metav1.GetOptions{},
	)
	if err != nil {
		setupLog.Error(err, "unable to read controller-mode namespace", "namespace", watchNamespace)
		os.Exit(1)
	}
	if err := executionmode.ValidateNamespace(modeNamespace, mode); err != nil {
		setupLog.Error(err, "controller-mode namespace claim failed")
		os.Exit(1)
	}
	if acpRuntimeEnabled && !substrateEnabled {
		checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := validateDisabledSubstrateRecoveryConfig(
			checkCtx,
			mgr.GetAPIReader(),
			watchNamespace,
			substrateConfig,
			substrateConfigErr,
		)
		cancel()
		if err != nil {
			setupLog.Error(err, "disabled substrate recovery configuration is unusable")
			os.Exit(1)
		}
	}
	controllerHolderID := currentControllerHolderID()
	if gatewayEnabled {
		checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := gatewayruntime.WaitForGatewayPrerequisites(checkCtx, mgr.GetAPIReader(), 500*time.Millisecond)
		cancel()
		if err != nil {
			if gatewayruntime.GatewayPrerequisiteErrorIsTransient(err) {
				setupLog.Error(err, "gateway prerequisite verification failed")
				os.Exit(1)
			}
			setupLog.Error(err, "gateway prerequisites are unavailable; disabling generic gateway")
			gatewayEnabled = false
		}
	}

	if workspaceClassUseAdmissionEnabled {
		orkaadmission.RegisterWorkspaceClassUseWebhooks(
			mgr.GetWebhookServer(),
			mgr.GetScheme(),
			controller.WorkspaceClassAuthorizer{Client: mgr.GetClient()},
		)
		setupLog.Info("registered Task and Tool workspace class use admission")
	}

	if taskProvenanceAdmissionEnabled {
		admissionConfig := orkaadmission.NewTaskProvenanceConfig(
			true,
			executionModeControllerUsernames,
			taskProvenanceAdmissionTrustedUsers,
			taskProvenanceAdmissionTrustedServiceAccounts,
			currentPodNamespace(),
		)
		orkaadmission.RegisterTaskProvenanceWebhook(mgr.GetWebhookServer(), mgr.GetScheme(), admissionConfig)
		setupLog.Info("enabled Task provenance validating admission",
			"trustedUsers", strings.Join(admissionConfig.TrustedUsernames, ","),
			"trustedServiceAccounts", strings.Join(admissionConfig.TrustedServiceAccountNames, ","),
		)
	}
	if managerAdmissionEnabled {
		orkaadmission.RegisterExecutionModeWebhooks(
			mgr.GetWebhookServer(),
			mgr.GetScheme(),
			mgr.GetAPIReader(),
			orkaadmission.ExecutionModeConfig{
				ControllerUsernames: splitCommaList(executionModeControllerUsernames),
			},
		)
		setupLog.Info("registered immutable namespace mode and execution-authority admission")
	}

	// The clientset is reused for pod log and broker operations.
	outboundAccessResolver := &outboundaccess.KubernetesResolver{
		Reader:     mgr.GetAPIReader(),
		KubeClient: kubeClient,
		Trust:      outboundAccessTrust,
		Exchanger:  tokenexchange.NewClient(tokenexchange.ClientOptions{}),
	}
	var brokeredTransactionExchange *worker.TransactionExchangeConfig
	var brokeredTTSExchanger contexttoken.Exchanger
	if contextTokenTTSConfig.Enabled() {
		sharedTTSClient, clientErr := contexttoken.NewTTSClient(contextTokenTTSConfig)
		if clientErr != nil {
			setupLog.Error(clientErr, "unable to create brokered transaction-token exchanger")
			os.Exit(1)
		}
		brokeredTTSExchanger = sharedTTSClient
		brokeredTransactionExchange = &worker.TransactionExchangeConfig{
			TTS:              contextTokenTTSConfig,
			Exchanger:        sharedTTSClient,
			SubjectTokenType: contextTokenSubjectTokenType,
			OutboundScope:    contextTokenOutboundScope,
		}
	}
	var acpMCPRegistry *tools.Registry
	if acpRuntimeEnabled {
		acpMCPRegistry = tools.NewRegistry()
		if err := tools.RegisterBrokeredWebTools(acpMCPRegistry); err != nil {
			setupLog.Error(err, "unable to register ACP MCP broker web tools")
			os.Exit(1)
		}
		if err := tools.RegisterBrokeredCoordinationTools(acpMCPRegistry, mgr.GetClient()); err != nil {
			setupLog.Error(err, "unable to register ACP MCP broker coordination tools")
			os.Exit(1)
		}
		if err := tools.RegisterBrokeredDelegateTaskTool(
			acpMCPRegistry,
			mgr.GetClient(),
			tools.BrokeredDelegateTaskTransactionExchangeConfig{
				TTS:                 contextTokenTTSConfig,
				Exchanger:           brokeredTTSExchanger,
				SubjectTokenType:    contextTokenSubjectTokenType,
				ChildScope:          contextTokenChildScope,
				ResolveSubjectToken: newBrokeredDelegateTaskSubjectTokenResolver(mgr.GetAPIReader(), workerenv.ServiceAccountTokenFile),
			},
		); err != nil {
			setupLog.Error(err, "unable to register configured ACP delegate_task broker")
			os.Exit(1)
		}
	}

	// Create SQLite store
	if storeBackend != "sqlite" {
		setupLog.Error(fmt.Errorf("unsupported store backend: %s", storeBackend), "unknown store backend")
		os.Exit(1)
	}

	sqliteStore, err := sqlite.OpenLockedStore(storePath)
	if err != nil {
		setupLog.Error(err, "unable to acquire the exclusive SQLite store and run migrations", "path", storePath)
		os.Exit(1)
	}
	if err := mgr.Add(sqliteStore); err != nil {
		setupLog.Error(err, "unable to add SQLite store as runnable")
		os.Exit(1)
	}
	snapshotCipher, cipherErr := loadAgentExecutionSnapshotCipher(agentExecutionSnapshotKeyFile)
	if cipherErr != nil {
		setupLog.Error(cipherErr, "unable to load agent execution snapshot key; snapshot encryption fails closed",
			"path", agentExecutionSnapshotKeyFile)
		os.Exit(1)
	}
	if cipherErr := sqliteStore.SetAgentExecutionSnapshotCipher(snapshotCipher); cipherErr != nil {
		setupLog.Error(cipherErr, "unable to activate agent execution snapshot key; snapshot encryption fails closed",
			"path", agentExecutionSnapshotKeyFile)
		os.Exit(1)
	}
	agentExecutionSnapshotStore := sqliteStore
	setupLog.Info("agent execution binding stage enabled: executable agent Tasks freeze an immutable encrypted snapshot and write-once binding before dispatch")
	snapshotRetentionManager := &controller.AgentExecutionSnapshotRetentionManager{
		APIReader: mgr.GetAPIReader(),
		Store:     sqliteStore,
		Namespace: watchNamespace,
		Retention: agentExecutionSnapshotRetention,
		Interval:  agentExecutionSnapshotRetentionInterval,
	}
	if err := mgr.Add(snapshotRetentionManager); err != nil {
		setupLog.Error(err, "unable to add agent execution snapshot retention manager")
		os.Exit(1)
	}
	controlNamespace, err := acpControlNamespace(acpRuntimeEnabled || harnessV1Enabled, currentPodNamespace())
	if err != nil {
		setupLog.Error(err, "unable to configure Kubernetes ACP control store")
		os.Exit(1)
	}

	// Create helper components. Kubernetes ACP admission is feature-gated, but
	// the control store, epoch manager, and cleanup recovery remain available in
	// a controller Pod after admission is disabled so pre-existing durable ACP
	// Sessions can still be reclaimed safely.
	acpAdmissionGate := controller.NewACPAdmissionGate()
	sessionManager := controller.NewSessionManager(sqliteStore)
	var taskCleanupControlStore store.DurableControlStore
	var durableControlStore store.DurableControlStore
	var controllerEpochManager *controller.ControllerEpochManager
	var acpSessionContinuity *controller.ACPSessionContinuity
	var kubeControlStore *storekube.Store
	if controlNamespace != "" {
		// Session deletion must retain runtime cleanup even when admission is
		// disabled. This dispatcher only performs authenticated recovery; it
		// does not run the admission loop.
		sessionCleanupDispatcher := &controller.ACPDispatcher{
			Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), ResultStore: sqliteStore,
			Snapshots:               agentExecutionSnapshotStore,
			SubstrateRouterURL:      substrateConfig.RouterURL,
			SubstrateActorDNSSuffix: substrateConfig.ActorDNSSuffix,
		}
		controlStoreOptions := []storekube.Option{
			storekube.WithAPIReader(mgr.GetAPIReader()),
			storekube.WithWatchNamespace(watchNamespace),
			storekube.WithSessionRuntimeCleanup(sessionCleanupDispatcher.CleanupSessionRuntime),
		}
		if harnessV1Enabled {
			controlStoreOptions = append(controlStoreOptions, storekube.WithoutClusterScopedBranchClaims())
		}
		kubeControlStore, err = storekube.NewComposite(
			mgr.GetClient(), controlNamespace, sqliteStore, controlStoreOptions...,
		)
		if err != nil {
			setupLog.Error(err, "unable to configure Kubernetes ACP control store")
			os.Exit(1)
		}
		controllerEpochManager = controller.NewControllerEpochManager(kubeControlStore, controllerHolderID).
			WithMirror(sqliteStore)
		sessionCleanupDispatcher.Store = kubeControlStore
		sessionCleanupDispatcher.Epochs = controllerEpochManager
		sessionManager.SetACPSessionCleanup(kubeControlStore, controllerEpochManager)
		if err := mgr.Add(controllerEpochManager); err != nil {
			setupLog.Error(err, "unable to add controller epoch manager")
			os.Exit(1)
		}
		sessionCleanupRecovery := controller.NewSessionCleanupRecoveryManager(kubeControlStore, controllerEpochManager)
		if err := mgr.Add(sessionCleanupRecovery); err != nil {
			setupLog.Error(err, "unable to add Session cleanup recovery manager")
			os.Exit(1)
		}
	}
	controlStoreWiring, err := newACPControlStoreWiring(acpRuntimeEnabled, kubeControlStore)
	if err != nil {
		setupLog.Error(err, "unable to configure ACP control-store wiring")
		os.Exit(1)
	}
	taskCleanupControlStore = controlStoreWiring.taskCleanup
	durableControlStore = controlStoreWiring.runtime
	if acpRuntimeEnabled || harnessV1Enabled {
		if kubeControlStore == nil {
			setupLog.Error(errors.New("kubernetes session control store is unavailable"),
				"unable to create shared agent Session continuity manager")
			os.Exit(1)
		}
		if harnessV1Enabled {
			acpSessionContinuity, err = controller.NewHarnessV1SessionContinuity(controller.HarnessV1SessionContinuityConfig{
				SessionControls: kubeControlStore, Transcripts: sqliteStore, GatewayEvents: sqliteStore, Lineages: sqliteStore,
			})
		} else {
			acpSessionContinuity, err = controller.NewACPSessionContinuity(controller.ACPSessionContinuityConfig{
				SessionControls: kubeControlStore, Transcripts: sqliteStore, Publications: kubeControlStore, BranchClaims: kubeControlStore,
				GatewayEvents: sqliteStore, Lineages: sqliteStore,
			})
		}
		if err != nil {
			setupLog.Error(err, "unable to create shared agent Session continuity manager")
			os.Exit(1)
		}
	}

	var artifactRetentionWiring acpArtifactRetentionWiring
	var publisherClient *publisherservice.Client
	var artifactCapabilitySecret []byte
	publisherWorkspaceArtifactMaxBytes := artifactcap.DefaultWorkspaceArtifactMaxBytes
	if acpRuntimeEnabled {
		artifactRoot := strings.TrimSpace(os.Getenv("ORKA_ACP_ARTIFACT_ROOT"))
		if artifactRoot == "" {
			artifactRoot = artifactcap.DefaultRoot
		}
		artifactRetentionWiring, err = newACPArtifactRetentionWiring(true, artifactRoot)
		if err != nil {
			setupLog.Error(err, "unable to configure ACP artifact retention")
			os.Exit(1)
		}
		if err := mgr.Add(artifactRetentionWiring.collector); err != nil {
			setupLog.Error(err, "unable to add ACP artifact retention")
			os.Exit(1)
		}
		publisherClient, artifactCapabilitySecret, publisherWorkspaceArtifactMaxBytes, err = workspacePublisherClientFromEnv()
		if err != nil {
			setupLog.Error(err, "unable to configure Workspace/Publisher client")
			os.Exit(1)
		}
	}
	sessionManager.SetGatewayEventStore(sqliteStore)
	maxTasksPerNamespaceValue := int32(maxTasksPerNamespace) //nolint:gosec // flag default is non-negative
	gatewayConfig := gatewayruntime.Config{
		Enabled: gatewayEnabled, Namespace: watchNamespace, PendingPerSession: gatewayPendingPerSession,
		MaxTasksPerNamespace:         maxTasksPerNamespaceValue,
		MaxRecordsPerGateway:         gatewayMaxRecordsPerGateway,
		MaxRejectedRecordsPerGateway: gatewayMaxRejectedRecordsPerGateway,
		EventExpiry:                  gatewayEventExpiry,
		TerminalRetention:            gatewayTerminalRetention, DeliveryTimeout: gatewayDeliveryTimeout,
		DeliveryMaxAttempts: gatewayDeliveryMaxAttempts, ClaimLease: gatewayClaimLease,
		PollInterval: gatewayPollInterval, BatchSize: gatewayBatchSize,
	}
	gatewayService := gatewayruntime.NewService(mgr.GetClient(), sqliteStore, sqliteStore, sqliteStore, gatewayConfig)
	gatewayService.APIReader = mgr.GetAPIReader()
	if gatewayEnabled {
		if err := mgr.Add(gatewayService); err != nil {
			setupLog.Error(err, "unable to add gateway service")
			os.Exit(1)
		}

	}
	webhookNotifier := controller.NewWebhookNotifier()
	webhookNotifier.SetKubeClient(mgr.GetClient())
	jobBuilder := controller.NewJobBuilder(mgr.GetClient())
	jobBuilder.ControllerMode = mode
	jobBuilder.AIWorkerImage = aiWorkerImage
	jobBuilder.GeneralWorkerImage = generalWorkerImage
	jobBuilder.AIWorkerServiceAccountName = aiWorkerServiceAccountName
	jobBuilder.ContainerWorkerServiceAccountName = containerWorkerServiceAccountName
	if contextTokenTTSConfig.Enabled() {
		jobBuilder.ContextTokenTTSEndpoint = contextTokenTTSConfig.Endpoint
		jobBuilder.ContextTokenTTSAudience = contextTokenTTSConfig.Audience
		jobBuilder.ContextTokenTTSTokenSource = contextTokenTTSConfig.TokenSource
		if contextTokenTTSConfig.Timeout > 0 {
			jobBuilder.ContextTokenTTSTimeout = contextTokenTTSConfig.Timeout.String()
		}
		if contextTokenTTSConfig.ChildTokenTTL > 0 {
			jobBuilder.ContextTokenChildTokenTTL = contextTokenTTSConfig.ChildTokenTTL.String()
		}
		if contextTokenTTSConfig.ToolTokenTTL > 0 {
			jobBuilder.ContextTokenToolTokenTTL = contextTokenTTSConfig.ToolTokenTTL.String()
		}
		jobBuilder.ContextTokenSubjectTokenType = contextTokenSubjectTokenType
		jobBuilder.ContextTokenChildScope = contextTokenChildScope
		jobBuilder.ContextTokenOutboundScope = contextTokenOutboundScope
	}
	setupLog.Info("worker images configured",
		"ai", aiWorkerImage,
		"general", generalWorkerImage,
	)
	jobBuilder.ControllerURL = controllerURL
	jobBuilder.EnableTelemetry = enableTracing
	jobBuilder.EnforceTransactionCredentialAuth =
		contextTokenAuthzConfig.Mode == api.ContextTokenAuthorizationModeEnforce
	jobBuilder.TransactionCredentialReadScopes = append(
		[]string(nil),
		contextTokenAuthzConfig.SecretCredentialReadScopes()...,
	)
	jobBuilder.OutboundAccessTrustedGatewayServices = outboundAccessTrustedGatewayServices
	jobBuilder.OutboundAccessTrustedTokenEndpointServices = outboundAccessTrustedTokenEndpointServices
	// Auto-discover controller URL from in-cluster service if not explicitly set
	if jobBuilder.ControllerURL == "" {
		ns := os.Getenv(workerenv.PodNamespace)
		if ns == "" {
			if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
				ns = strings.TrimSpace(string(data))
			}
		}
		if ns != "" {
			jobBuilder.ControllerURL = fmt.Sprintf("http://orka.%s.svc:%d", ns, apiPort)
			setupLog.Info("auto-discovered controller URL", "url", jobBuilder.ControllerURL)
		}
	}
	if agentSandboxConfig.NamespaceStrategy == controller.AgentSandboxNamespaceStrategyController &&
		agentSandboxConfig.ControllerNamespace == "" {
		agentSandboxConfig.ControllerNamespace = currentPodNamespace()
	}

	if acpRuntimeEnabled {
		runtimePoolReconciler := &controller.RuntimePoolReconciler{
			Client:           mgr.GetClient(),
			APIReader:        mgr.GetAPIReader(),
			Scheme:           mgr.GetScheme(),
			RuntimeNamespace: acpRuntimeNamespace,
		}
		providerProxyLabels, err := parseExactLabels(acpProviderProxyPodLabels)
		if err != nil {
			setupLog.Error(err, "unable to configure authenticated ACP provider proxy labels")
			os.Exit(1)
		}
		runtimePoolReconciler.ControllerNamespace = controlNamespace
		runtimePoolReconciler.ControllerAPIURL = jobBuilder.ControllerURL
		runtimePoolReconciler.ControllerAPIPort = int32(apiPort)
		runtimePoolReconciler.WorkspaceArtifactMaxBytes = publisherWorkspaceArtifactMaxBytes
		runtimePoolReconciler.ProviderProxy = controller.RuntimePoolProviderProxyConfig{
			BaseURL:         acpProviderProxyBaseURL,
			Namespace:       acpProviderProxyNamespace,
			PodLabels:       providerProxyLabels,
			BearerTokenFile: acpProviderProxyTokenFile,
		}
		runtimePoolReconciler.Epochs = controllerEpochManager
		runtimePoolReconciler.EnablePDB = true
		runtimePoolReconciler.E2EPromptWriteAmbiguityMarker = acpE2EPromptWriteAmbiguityMarker
		runtimePoolReconciler.AgentSandboxEnabled = agentSandboxEnabled
		runtimePoolReconciler.SubstrateEnabled = substrateEnabled
		// Keep the provider connection and trust configuration available after
		// admission is disabled so existing Substrate-backed pools can still
		// destroy actors and release their finalizers.
		runtimePoolReconciler.SubstrateConfig = substrateConfig
		runtimePoolReconciler.AllowedImages = controller.ACPRuntimeImages{
			Codex: acpCodexRuntimeImage, Claude: acpClaudeRuntimeImage, Copilot: acpCopilotRuntimeImage,
			Opencode: acpOpencodeRuntimeImage,
		}
		if err := runtimePoolReconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "RuntimePool")
			os.Exit(1)
		}
	}

	// Setup Task controller with helper components.
	taskReconciler := &controller.TaskReconciler{
		Client:                       mgr.GetClient(),
		APIReader:                    mgr.GetAPIReader(),
		Scheme:                       mgr.GetScheme(),
		JobBuilder:                   jobBuilder,
		SessionManager:               sessionManager,
		WebhookNotifier:              webhookNotifier,
		KubeClient:                   kubeClient,
		ResultStore:                  sqliteStore,
		PlanStore:                    sqliteStore,
		MessageStore:                 sqliteStore,
		ArtifactStore:                sqliteStore,
		ExecutionEventStore:          sqliteStore,
		DurableControlStore:          taskCleanupControlStore,
		AgentExecutionSnapshots:      agentExecutionSnapshotStore,
		RepositoryValidationBindings: sqliteStore,
		MCPRegistry:                  acpMCPRegistry,
		HarnessV1Enabled:             harnessV1Enabled,
		HarnessV1Endpoint:            harnessV1Endpoint,
		HarnessV1AuthSecretNamespace: harnessV1AuthSecretNamespace,
		HarnessV1AuthSecretName:      harnessV1AuthSecretName,
		HarnessV1AuthSecretKey:       harnessV1AuthSecretKey,
		HarnessV1Attempts:            sqliteStore,
		ACPArtifactRetirer:           artifactRetentionWiring.taskCleanup,
		ACPPublicationReclaimer:      publisherClient,
		ControllerEpochManager:       controllerEpochManager,
		ACPAdmissionGate:             acpAdmissionGate,
		ACPRuntimeEnabled:            acpRuntimeEnabled,
		ACPRuntimeImages: controller.ACPRuntimeImages{
			Codex: acpCodexRuntimeImage, Claude: acpClaudeRuntimeImage, Copilot: acpCopilotRuntimeImage,
			Opencode: acpOpencodeRuntimeImage,
		},
		ACPRuntimeNamespace:         acpRuntimeNamespace,
		OutboundAccessResolver:      outboundAccessResolver,
		BrokeredTransactionExchange: brokeredTransactionExchange,

		EnforceNamespaceIsolation:         enforceNamespaceIsolation,
		MaxTasksPerNamespace:              maxTasksPerNamespaceValue,
		ExecutionWorkspaceDefaultProvider: executionWorkspaceDefaultProvider,
		WorkspaceProviderAPIEnabled:       workspaceProviderAPIEnabled,
		WorkspaceSettlementProtected:      taskProvenanceAdmissionEnabled,
		ACPWorkspaceDispatchEnabled:       acpWorkspaceDispatchEnabled,
		AgentSandboxEnabled:               agentSandboxEnabled,
		AgentSandboxConfig:                agentSandboxConfig,
		SubstrateEnabled:                  substrateEnabled,
		SubstrateConfig:                   substrateConfig,
		AIWorkerServiceAccountName:        aiWorkerServiceAccountName,
		VendorWorkerServiceAccountName:    vendorWorkerServiceAccountName,
		ContainerWorkerServiceAccountName: containerWorkerServiceAccountName,
		AIWorkerClusterRoleName:           aiWorkerClusterRoleName,
		VendorWorkerClusterRoleName:       vendorWorkerClusterRoleName,
		ContainerWorkerClusterRoleName:    containerWorkerClusterRoleName,
		WorkerRoleBindingNamePrefix:       workerRoleBindingNamePrefix,
		EnforceTransactionCredentialAuth:  contextTokenAuthzConfig.Mode == api.ContextTokenAuthorizationModeEnforce,
		TransactionCredentialReadScopes: append(
			[]string(nil),
			contextTokenAuthzConfig.SecretCredentialReadScopes()...,
		),
		OutboundAccessTrust: outboundAccessTrust,
	}
	if err := taskReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Task")
		os.Exit(1)
	}
	var harnessV1HTTPClient *http.Client
	if harnessV1Enabled {
		if controllerEpochManager == nil || agentExecutionSnapshotStore == nil {
			setupLog.Error(errors.New("harness v1 requires controller epoch and encrypted snapshot stores"),
				"unable to add harness v1 dispatcher")
			os.Exit(1)
		}
		var clientErr error
		harnessV1HTTPClient, clientErr = newHarnessV1TLSHTTPClient(harnessV1CAFile)
		if clientErr != nil {
			setupLog.Error(clientErr, "unable to configure harness v1 TLS client")
			os.Exit(1)
		}
		harnessV1Dispatcher := &controller.HarnessV1Dispatcher{
			Client:          mgr.GetClient(),
			APIReader:       mgr.GetAPIReader(),
			Attempts:        sqliteStore,
			Snapshots:       agentExecutionSnapshotStore,
			ResultStore:     sqliteStore,
			EventStore:      sqliteStore,
			ExternalEffects: kubeControlStore,
			BrokeredToolExecutor: &controller.KubernetesHarnessV1BrokeredToolExecutor{
				Reader:              mgr.GetAPIReader(),
				KubeClient:          kubeClient,
				OutboundAccess:      outboundAccessResolver,
				TransactionExchange: brokeredTransactionExchange,
				EnforceTransactionCredentialAuth: contextTokenAuthzConfig.Mode ==
					api.ContextTokenAuthorizationModeEnforce,
				TransactionCredentialReadScopes: append(
					[]string(nil),
					contextTokenAuthzConfig.SecretCredentialReadScopes()...,
				),
			},
			Sessions:      acpSessionContinuity,
			Epochs:        controllerEpochManager,
			Interval:      harnessV1DispatchInterval,
			MaxConcurrent: harnessV1DispatchWorkers,
			HTTPClient:    harnessV1HTTPClient,
		}
		taskReconciler.HarnessV1SettlementAcknowledger = harnessV1Dispatcher
		if err := mgr.Add(harnessV1Dispatcher); err != nil {
			setupLog.Error(err, "unable to add harness v1 dispatcher")
			os.Exit(1)
		}
	}
	if harnessV1Enabled || acpRuntimeEnabled {
		// Session settlement in both harness planes commits terminal Task status
		// through the shared Kubernetes outbox.
		agentOutboxProjector := &controller.ACPOutboxProjector{
			Client: mgr.GetClient(), Store: kubeControlStore, Epochs: controllerEpochManager, WorkerID: controllerHolderID + "-outbox",
		}
		if err := mgr.Add(agentOutboxProjector); err != nil {
			setupLog.Error(err, "unable to add agent outbox projector")
			os.Exit(1)
		}
	}
	if acpRuntimeEnabled {
		acpDispatcher := &controller.ACPDispatcher{
			Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Store: durableControlStore, ResultStore: sqliteStore,
			EventStore: sqliteStore, PlanStore: sqliteStore,
			Snapshots: agentExecutionSnapshotStore,
			Epochs:    controllerEpochManager, Sessions: acpSessionContinuity,
			Publisher: publisherClient, ArtifactCapabilitySecret: artifactCapabilitySecret,
			ArtifactReservations: artifactRetentionWiring.collector,
			AdmissionGate:        acpAdmissionGate,
			IdlePoolTTL:          acpIdlePoolTTL,
			MCPRegistry:          acpMCPRegistry,
			ACPRuntimeImages: controller.ACPRuntimeImages{
				Codex: acpCodexRuntimeImage, Claude: acpClaudeRuntimeImage, Copilot: acpCopilotRuntimeImage,
				Opencode: acpOpencodeRuntimeImage,
			},
			// Keep routing available after new Substrate admission is disabled:
			// existing Tasks and RuntimeSessions still need authenticated recovery,
			// cancellation, finalization, drain, and cleanup against their actors.
			SubstrateRouterURL:      substrateConfig.RouterURL,
			SubstrateActorDNSSuffix: substrateConfig.ActorDNSSuffix,
		}
		if err := mgr.Add(acpDispatcher); err != nil {
			setupLog.Error(err, "unable to add ACP dispatcher")
			os.Exit(1)
		}
		if strings.TrimSpace(acpUpgradeDrainOptions.MarkerNamespace) == "" {
			acpUpgradeDrainOptions.MarkerNamespace = controlNamespace
		}
		upgradeDrain := controller.NewACPUpgradeDrainCoordinator(
			mgr.GetClient(), mgr.GetAPIReader(), controllerEpochManager, durableControlStore,
			&controller.KubernetesACPUpgradeDrainBarrierObserver{Reader: mgr.GetAPIReader(), Outbox: sqliteStore},
			acpAdmissionGate, acpUpgradeDrainOptions,
		)
		upgradeDrain.SubstrateConfig = substrateConfig
		if err := mgr.Add(upgradeDrain); err != nil {
			setupLog.Error(err, "unable to add ACP planned-upgrade drain coordinator")
			os.Exit(1)
		}
		if err := mgr.AddReadyzCheck("acp-upgrade-drain", upgradeDrain.ReadyzChecker()); err != nil {
			setupLog.Error(err, "unable to add ACP planned-upgrade readiness check")
			os.Exit(1)
		}
	}

	if err := (&controller.OutboundAccessPolicyReconciler{
		Client:                     mgr.GetClient(),
		APIReader:                  mgr.GetAPIReader(),
		Scheme:                     mgr.GetScheme(),
		Trust:                      outboundAccessTrust,
		AIWorkerServiceAccountName: aiWorkerServiceAccountName,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "OutboundAccessPolicy")
		os.Exit(1)
	}

	if err := (&controller.ToolReconciler{
		Client:                      mgr.GetClient(),
		Scheme:                      mgr.GetScheme(),
		SubstrateEnabled:            substrateEnabled,
		SubstrateConfig:             substrateConfig,
		EnforceNamespaceIsolation:   enforceNamespaceIsolation,
		WorkspaceProviderAPIEnabled: workspaceProviderAPIEnabled,
		OutboundAccessTrust:         outboundAccessTrust,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Tool")
		os.Exit(1)
	}

	if err := (&controller.SubstrateActorPoolReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		SubstrateEnabled: substrateEnabled,
		SubstrateConfig:  substrateConfig,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SubstrateActorPool")
		os.Exit(1)
	}

	if fakeWorkspaceProviderEnabled && !workspaceProviderAPIEnabled {
		setupLog.Error(
			fmt.Errorf("fake workspace provider requires workspace provider API"),
			"invalid workspace provider feature gates",
		)
		os.Exit(1)
	}
	registerWorkspaceCoreControllers := acpRuntimeEnabled && workspaceProviderAPIEnabled
	if acpRuntimeEnabled && !workspaceProviderAPIEnabled {
		workspaceAPIsInstalled, err := workspaceCleanupAPIsInstalled(mgr.GetRESTMapper())
		if err != nil {
			setupLog.Error(err, "unable to discover workspace cleanup APIs")
			os.Exit(1)
		}
		registerWorkspaceCoreControllers = workspaceAPIsInstalled
		if !workspaceAPIsInstalled {
			setupLog.Info("workspace CRDs are not installed; skipping cleanup-only workspace controllers")
		}
		if workspaceAPIsInstalled && !taskProvenanceAdmissionEnabled {
			// Class-backed settlement performs controller-privileged deletion
			// from the reserved Task metadata; without the provenance webhook
			// those keys are forgeable. Cleanup-only installations (the stock
			// installer with CRDs bundled) keep starting, but the Task
			// reconciler disables the privileged settlement actions below and
			// existing workspaces are cleaned through explicit workspace
			// deletion instead.
			setupLog.Info("Task provenance admission is disabled; class-backed Task settlement runs non-destructively and workspaces require explicit deletion")
		}
	}
	if registerWorkspaceCoreControllers {
		if err := (&controller.ExecutionWorkspaceProviderReconciler{
			Client:      mgr.GetClient(),
			APIReader:   mgr.GetAPIReader(),
			RESTMapper:  mgr.GetRESTMapper(),
			CleanupOnly: !workspaceProviderAPIEnabled,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ExecutionWorkspaceProviderCore")
			os.Exit(1)
		}
		if err := (&controller.ExecutionWorkspaceReconciler{
			Client:                  mgr.GetClient(),
			APIReader:               mgr.GetAPIReader(),
			RESTMapper:              mgr.GetRESTMapper(),
			AdmissionLeaseNamespace: currentPodNamespace(),
			CleanupOnly:             !workspaceProviderAPIEnabled,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ExecutionWorkspaceCore")
			os.Exit(1)
		}
		if err := (&controller.ExecutionWorkspaceClassReconciler{
			Client:      mgr.GetClient(),
			APIReader:   mgr.GetAPIReader(),
			RESTMapper:  mgr.GetRESTMapper(),
			CleanupOnly: !workspaceProviderAPIEnabled,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ExecutionWorkspaceClassCore")
			os.Exit(1)
		}
	}
	if registerWorkspaceCoreControllers && acpRuntimeEnabled {
		// The in-tree ACP RuntimePool workspace adapter serves class-backed
		// execution workspaces. It registers even when dispatch or provider
		// flags are off so existing workspaces keep converging toward cleanup;
		// provider advertisement itself fails closed on the flags.
		if err := (&controller.ACPWorkspaceProviderAdapterReconciler{
			Client:                      mgr.GetClient(),
			AgentSandboxEnabled:         agentSandboxEnabled,
			SubstrateEnabled:            substrateEnabled,
			ACPWorkspaceDispatchEnabled: acpWorkspaceDispatchEnabled,
			WorkspaceProviderAPIEnabled: workspaceProviderAPIEnabled,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ACPWorkspaceProviderAdapter")
			os.Exit(1)
		}
		if err := (&controller.ACPExecutionWorkspaceAdapterReconciler{
			Client:           mgr.GetClient(),
			APIReader:        mgr.GetAPIReader(),
			RuntimeNamespace: acpRuntimeNamespace,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ACPExecutionWorkspaceAdapter")
			os.Exit(1)
		}
		if err := (&controller.ACPWorkspaceRetentionReconciler{
			Client:              mgr.GetClient(),
			APIReader:           mgr.GetAPIReader(),
			DurableControlStore: durableControlStore,
			Recorder:            mgr.GetEventRecorder("acp-workspace-retention"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ACPWorkspaceRetention")
			os.Exit(1)
		}
	}
	if workspaceProviderAPIEnabled {
		if fakeWorkspaceProviderEnabled {
			if err := (&controller.FakeExecutionWorkspaceProviderReconciler{
				Client: mgr.GetClient(),
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "FakeExecutionWorkspaceProvider")
				os.Exit(1)
			}
			if err := (&controller.FakeExecutionWorkspacePoolReconciler{
				Client: mgr.GetClient(),
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "FakeExecutionWorkspacePool")
				os.Exit(1)
			}
			if err := (&controller.FakeExecutionWorkspaceReconciler{
				Client:     mgr.GetClient(),
				APIReader:  mgr.GetAPIReader(),
				RESTMapper: mgr.GetRESTMapper(),
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to create controller", "controller", "FakeExecutionWorkspace")
				os.Exit(1)
			}
		}
	}

	if err := (&controller.AgentReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Agent")
		os.Exit(1)
	}

	if err := (&controller.AgentRuntimeReconciler{
		Client:                 mgr.GetClient(),
		APIReader:              mgr.GetAPIReader(),
		Scheme:                 mgr.GetScheme(),
		HarnessV1HTTPClient:    harnessV1HTTPClient,
		MCPRegistry:            acpMCPRegistry,
		ControllerEpochManager: controllerEpochManager,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AgentRuntime")
		os.Exit(1)
	}

	if gatewayEnabled {
		if err := (&controller.GatewayClassReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "GatewayClass")
			os.Exit(1)
		}
		if err := (&controller.GatewayReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "Gateway")
			os.Exit(1)
		}
		if err := (&controller.GatewayBindingReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "GatewayBinding")
			os.Exit(1)
		}
	}

	if err := (&controller.ProviderReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Provider")
		os.Exit(1)
	}

	if err := (&controller.SkillReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Skill")
		os.Exit(1)
	}

	if err := (&controller.RepositoryScanReconciler{
		Client:        mgr.GetClient(),
		APIReader:     mgr.GetAPIReader(),
		Scheme:        mgr.GetScheme(),
		SecurityStore: sqliteStore,
		ArtifactStore: sqliteStore,
		ResultStore:   sqliteStore,
		// Governed publications are recorded by the ACP dispatcher in the
		// durable control store; verifying patch proposals must read the same
		// store, not the SQLite payload store.
		PublicationStore: durableControlStore,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RepositoryScan")
		os.Exit(1)
	}

	if err := (&controller.RepositoryMonitorReconciler{
		Client:                    mgr.GetClient(),
		Scheme:                    mgr.GetScheme(),
		Store:                     sqliteStore,
		ResultStore:               sqliteStore,
		ArtifactStore:             sqliteStore,
		EnforceNamespaceIsolation: enforceNamespaceIsolation,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RepositoryMonitor")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
	if managerAdmissionEnabled {
		if err := mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()); err != nil {
			setupLog.Error(err, "unable to set up webhook ready check")
			os.Exit(1)
		}
	}
	// Register coordination tools the Anthropic/OpenAI proxy advertises but that
	// RegisterChatToolsDefault does not provide. Without these the proxy lists the
	// tool in coordinatorProxyTools but ToLLMTools silently drops it, leaving the
	// coordinator to call a tool name that comes back as "not available in this
	// request" and aborting the chat-to-PR workflow after all the real work is done.
	tools.RegisterProxyPRTools(mgr.GetClient())

	// Start REST API server
	var publisherControllerEpochs api.ControllerEpochFenceSource
	if kubeControlStore != nil {
		publisherControllerEpochs = api.NewControllerEpochStoreFenceSource(kubeControlStore)
	}
	apiServer := api.NewServer(mgr.GetClient(), sessionManager, api.ServerConfig{
		Port:                      apiPort,
		WatchNamespace:            watchNamespace,
		ExecutionMode:             mode,
		EnforceNamespaceIsolation: enforceNamespaceIsolation,
		OIDC: api.OIDCConfig{
			Issuer:          oidcIssuer,
			Audience:        oidcAudience,
			JWKSURL:         oidcJWKSURL,
			AllowedSubjects: splitCommaList(oidcAllowedSubjects),
			Namespace:       oidcNamespace,
		},
		ContextTokens:             contextTokenConfig,
		ContextTokenAuthorization: contextTokenAuthzConfig,
		ResultStore:               sqliteStore,
		SessionStore:              sqliteStore,
		PlanStore:                 sqliteStore,
		MessageStore:              sqliteStore,
		ArtifactStore:             sqliteStore,
		ArtifactReservations:      artifactRetentionWiring.runtimeReservations,
		AgentExecutionSnapshots:   agentExecutionSnapshotStore,
		ExternalEffects:           kubeControlStore,
		MemoryStore:               sqliteStore,
		MemoryProposalStore:       sqliteStore,
		SecurityStore:             sqliteStore,
		RepositoryMonitorStore:    sqliteStore,
		ExecutionEventStore:       sqliteStore,
		GatewayEventStore:         sqliteStore,
		GatewayDeliveryStore:      sqliteStore,
		GatewayService:            gatewayService,
		HealthChecker:             sqliteStore,
		Clientset:                 kubeClient,
		APIReader:                 mgr.GetAPIReader(),
		ControllerEpochs:          publisherControllerEpochs,
		E2EPromptFaultEnabled:     strings.TrimSpace(acpE2EPromptWriteAmbiguityMarker) != "",
		Chat: api.ChatConfig{
			Enabled:                chatEnabled,
			Provider:               chatProvider,
			Model:                  chatModel,
			MaxIterations:          chatMaxIterations,
			MaxDuration:            chatMaxDuration,
			ToolTimeout:            chatToolTimeout,
			MaxConcurrent:          chatMaxConcurrent,
			MaxTasksPerTurn:        chatMaxTasksPerTurn,
			MaxSessionSize:         chatMaxSessionSize,
			MaxPrematureEndRetries: chatMaxPrematureEndRetries,
			RuntimeAvailability: api.ACPRuntimeAvailability{
				Codex:    acpRuntimeEnabled && controller.ACPRuntimeImageAvailable(acpCodexRuntimeImage),
				Claude:   acpRuntimeEnabled && controller.ACPRuntimeImageAvailable(acpClaudeRuntimeImage),
				Copilot:  acpRuntimeEnabled && controller.ACPRuntimeImageAvailable(acpCopilotRuntimeImage),
				OpenCode: acpRuntimeEnabled && controller.ACPRuntimeImageAvailable(acpOpencodeRuntimeImage),
			},
		},
	})
	if acpRuntimeEnabled {
		mcpBroker, err := controller.NewProductionACPMCPBroker(controller.ACPMCPBrokerDependencies{
			Reader: mgr.GetAPIReader(), Epochs: controllerEpochManager, ControlStore: durableControlStore,
			AgentExecutionSnapshots: agentExecutionSnapshotStore,
			KubeClient:              kubeClient, Registry: acpMCPRegistry,
			OutboundAccess: outboundAccessResolver, TransactionExchange: brokeredTransactionExchange,
			EnforceTransactionCredentialAuth: contextTokenAuthzConfig.Mode == api.ContextTokenAuthorizationModeEnforce,
			TransactionCredentialReadScopes:  contextTokenAuthzConfig.SecretCredentialReadScopes(),
			ContextFactory: func(ctx context.Context, request harnessv2.MCPBrokerCallRequest) (*tools.ToolContext, error) {
				task, ok := controller.ACPMCPAuthenticatedTaskFromContext(ctx)
				if !ok || task.Namespace != request.Namespace || task.UID != string(request.Metadata.TaskUID) {
					return nil, fmt.Errorf("authenticated ACP MCP task context is unavailable")
				}
				return &tools.ToolContext{
					Client: mgr.GetClient(), PolicyReader: mgr.GetAPIReader(), KubeClient: kubeClient, Namespace: request.Namespace,
					SessionID: string(request.Authorization.RuntimeSessionUID), TaskID: task.Name,
					TaskUID: task.UID, ParentTaskID: task.ParentTaskID, AgentName: task.AgentName,
					OperationID: string(request.Metadata.OperationID), ExternalEffects: durableControlStore,
					Tenant: request.Namespace, WatchNamespace: watchNamespace,
					EnforceNamespaceIsolation: enforceNamespaceIsolation, Brokered: true,
					TaskProvenanceProtected:      taskProvenanceAdmissionEnabled,
					RepositoryValidationBindings: sqliteStore,
					ResultStore:                  sqliteStore, MessageStore: sqliteStore, SessionDeleter: sessionManager,
					MemoryReader: sqliteStore, MemoryProposalWriter: sqliteStore, TranscriptSearcher: sqliteStore,
				}, nil
			},
		})
		if err != nil {
			setupLog.Error(err, "unable to construct ACP MCP broker")
			os.Exit(1)
		}
		if err := apiServer.RegisterACPMCPBroker(mcpBroker); err != nil {
			setupLog.Error(err, "unable to register ACP MCP broker")
			os.Exit(1)
		}
	}

	// Add API server as a runnable
	if err := mgr.Add(apiServer); err != nil {
		setupLog.Error(err, "unable to add API server")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(processCtx); err != nil {
		stopProcess()
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func validateEnabledSubstrateConfig(cfg controller.SubstrateConfig, legacyWorkspaceProviderAPIEnabled bool) error {
	if legacyWorkspaceProviderAPIEnabled {
		return cfg.Validate()
	}
	return cfg.ValidateACPRuntimePool()
}

func newBrokeredDelegateTaskSubjectTokenResolver(
	reader crclient.Reader,
	serviceAccountTokenFile string,
) tools.DelegateTaskSubjectTokenResolver {
	return func(ctx context.Context, parentTask *corev1alpha1.Task, tokenSource string) (string, error) {
		switch tokenSource {
		case contexttoken.TTSTokenSourceServiceAccount:
			return workerenv.ReadTokenFile(serviceAccountTokenFile, "controller service account token")
		case contexttoken.TTSTokenSourceIncoming:
			if reader == nil {
				return "", fmt.Errorf("kubernetes reader is required for incoming brokered transaction tokens")
			}
			if parentTask == nil || parentTask.UID == "" {
				return "", fmt.Errorf("authenticated parent Task identity is required for incoming brokered transaction tokens")
			}
			secretName := strings.TrimSpace(parentTask.Annotations[labels.AnnotationTransactionTokenSecret])
			if secretName == "" {
				return "", fmt.Errorf("authenticated parent Task does not reference an incoming transaction-token Secret")
			}
			secret := &corev1.Secret{}
			if err := reader.Get(ctx, crclient.ObjectKey{Name: secretName, Namespace: parentTask.Namespace}, secret); err != nil {
				return "", fmt.Errorf("read authenticated parent transaction-token Secret: %w", err)
			}
			if !secretOwnedByTask(secret, parentTask) {
				return "", fmt.Errorf("authenticated parent transaction-token Secret is not owned by the parent Task")
			}
			token := strings.TrimSpace(string(secret.Data["token"]))
			if token == "" {
				return "", fmt.Errorf("authenticated parent transaction-token Secret token is missing or empty")
			}
			return token, nil
		case contexttoken.TTSTokenSourceNone:
			return "", fmt.Errorf("context token TTS token source %q does not provide a subject token", tokenSource)
		default:
			return "", fmt.Errorf("unsupported context token TTS token source %q", tokenSource)
		}
	}
}

func secretOwnedByTask(secret *corev1.Secret, task *corev1alpha1.Task) bool {
	if secret == nil || task == nil || task.UID == "" || secret.Namespace != task.Namespace {
		return false
	}
	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == corev1alpha1.GroupVersion.String() && owner.Kind == taskResourceKind &&
			owner.Name == task.Name && owner.UID == task.UID {
			return true
		}
	}
	return false
}

func workspacePublisherClientFromEnv() (*publisherservice.Client, []byte, int64, error) {
	artifactSecretPath := strings.TrimSpace(os.Getenv("ORKA_ACP_ARTIFACT_CAPABILITY_SECRET_FILE"))
	var artifactSecret []byte
	if artifactSecretPath != "" {
		value, err := os.ReadFile(artifactSecretPath)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("read ACP artifact capability secret: %w", err)
		}
		artifactSecret = []byte(strings.TrimSpace(string(value)))
	}
	baseURL := strings.TrimSpace(os.Getenv("ORKA_WORKSPACE_PUBLISHER_URL"))
	if baseURL == "" {
		return nil, artifactSecret, artifactcap.DefaultWorkspaceArtifactMaxBytes, nil
	}
	bearerPath := strings.TrimSpace(os.Getenv("ORKA_WORKSPACE_PUBLISHER_CONTROLLER_TOKEN_FILE"))
	capabilityPath := strings.TrimSpace(os.Getenv("ORKA_WORKSPACE_PUBLISHER_CAPABILITY_SECRET_FILE"))
	if bearerPath == "" || capabilityPath == "" {
		return nil, nil, 0, fmt.Errorf("Workspace/Publisher auth file paths are required")
	}
	bearer, err := os.ReadFile(bearerPath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read Workspace/Publisher controller token: %w", err)
	}
	capability, err := os.ReadFile(capabilityPath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read Workspace/Publisher capability secret: %w", err)
	}
	client, err := publisherservice.NewClient(publisherservice.ClientConfig{
		BaseURL: baseURL, BearerToken: []byte(strings.TrimSpace(string(bearer))),
		CapabilitySecret: []byte(strings.TrimSpace(string(capability))),
	})
	if err != nil {
		return nil, nil, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capabilities, err := client.Capabilities(ctx)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read Workspace/Publisher capabilities: %w", err)
	}
	if capabilities.Protocol != publisherservice.ProtocolVersion {
		return nil, nil, 0, fmt.Errorf("Workspace/Publisher protocol %q is incompatible", capabilities.Protocol)
	}
	maxArtifactBytes := capabilities.Limits.MaxWorkspaceArtifactBytes
	if maxArtifactBytes <= 0 || maxArtifactBytes == math.MaxInt64 {
		return nil, nil, 0, fmt.Errorf("Workspace/Publisher max workspace artifact bytes must be positive and less than %d", int64(math.MaxInt64))
	}
	return client, artifactSecret, maxArtifactBytes, nil
}

func envStringDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envDurationDefault(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseExactLabels(raw string) (map[string]string, error) {
	result := map[string]string{}
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, value, ok := strings.Cut(entry, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("provider proxy Pod label %q must be key=value", entry)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("provider proxy Pod label %q is duplicated", key)
		}
		result[key] = value
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one provider proxy Pod label is required")
	}
	return result, nil
}

func managerCacheOptions(watchNamespace, acpRuntimeNamespace string) cache.Options {
	watchNamespace = strings.TrimSpace(watchNamespace)
	if watchNamespace == "" {
		return cache.Options{}
	}

	options := cache.Options{
		DefaultNamespaces: map[string]cache.Config{watchNamespace: {}},
	}
	runtimeNamespace := strings.TrimSpace(acpRuntimeNamespace)
	if runtimeNamespace == "" {
		return options
	}

	runtimeChildNamespaces := map[string]cache.Config{
		watchNamespace:   {},
		runtimeNamespace: {},
	}
	options.ByObject = make(map[crclient.Object]cache.ByObject)
	options.ByObject[&appsv1.Deployment{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&appsv1.ReplicaSet{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&corev1.Pod{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&corev1.Service{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&corev1.Secret{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&networkingv1.NetworkPolicy{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	options.ByObject[&policyv1.PodDisruptionBudget{}] = cache.ByObject{Namespaces: runtimeChildNamespaces}
	return options
}

func envBool(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid boolean %s=%q: %v\n", name, value, err)
		os.Exit(1)
	}
	return parsed
}

func acpControlNamespace(runtimeEnabled bool, controllerNamespace string) (string, error) {
	controllerNamespace = strings.TrimSpace(controllerNamespace)
	if controllerNamespace == "" {
		if !runtimeEnabled {
			return "", nil
		}
		return "", fmt.Errorf("controller namespace is unavailable")
	}
	return controllerNamespace, nil
}

type acpArtifactRetentionWiring struct {
	collector           *artifactcap.Collector
	taskCleanup         artifactcap.IdentityRetirer
	runtimeReservations artifactcap.CapabilityReservationRecorder
}

func newACPArtifactRetentionWiring(runtimeEnabled bool, root string) (acpArtifactRetentionWiring, error) {
	if !runtimeEnabled {
		return acpArtifactRetentionWiring{}, nil
	}
	collector, err := artifactcap.NewCollector(artifactcap.CollectorConfig{Root: root})
	if err != nil {
		return acpArtifactRetentionWiring{}, err
	}
	wiring := acpArtifactRetentionWiring{
		collector:           collector,
		taskCleanup:         collector,
		runtimeReservations: collector,
	}
	return wiring, nil
}

type acpControlStoreWiring struct {
	taskCleanup store.DurableControlStore
	runtime     store.DurableControlStore
}

func newACPControlStoreWiring(runtimeEnabled bool, kubeControlStore *storekube.Store) (acpControlStoreWiring, error) {
	var wiring acpControlStoreWiring
	if !runtimeEnabled {
		return wiring, nil
	}
	if kubeControlStore == nil {
		return acpControlStoreWiring{}, fmt.Errorf("kubernetes ACP control store is unavailable")
	}
	wiring.taskCleanup = kubeControlStore
	wiring.runtime = kubeControlStore
	return wiring, nil
}

func currentControllerHolderID() string {
	if holder := strings.TrimSpace(os.Getenv("ORKA_CONTROLLER_HOLDER_ID")); holder != "" {
		return holder
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	return controllerHolderIDForIncarnation(hostname, controllerProcessIncarnation)
}

func controllerHolderIDForIncarnation(hostname, incarnation string) string {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		hostname = "controller"
	}
	return hostname + "-" + strings.TrimSpace(incarnation)
}

func currentPodNamespace() string {
	if namespace := strings.TrimSpace(os.Getenv(workerenv.PodNamespace)); namespace != "" {
		return namespace
	}
	data, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// loadAgentExecutionSnapshotCipher reads the AES-256 snapshot key from a file
// holding either exactly 32 raw bytes or whitespace-padded base64 text.
func loadAgentExecutionSnapshotCipher(path string) (*sqlite.AgentExecutionSnapshotCipher, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- operator-supplied key path.
	if err != nil {
		return nil, err
	}
	key := raw
	if len(key) != sqlite.AgentExecutionSnapshotKeyBytes {
		decoded, decodeErr := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(raw)))
		if decodeErr != nil || len(decoded) != sqlite.AgentExecutionSnapshotKeyBytes {
			return nil, fmt.Errorf("snapshot key must be %d raw bytes or their base64 encoding", sqlite.AgentExecutionSnapshotKeyBytes)
		}
		key = decoded
	}
	return sqlite.NewAgentExecutionSnapshotCipher(key)
}

func validateAgentExecutionSnapshotOptions(
	mode executionmode.Mode,
	keyFile string,
	retention time.Duration,
	interval time.Duration,
) error {
	if strings.TrimSpace(keyFile) == "" {
		return fmt.Errorf("%s requires --agent-execution-snapshot-key-file", mode)
	}
	if retention <= 0 || interval <= 0 {
		return errors.New("agent execution snapshot retention and retention interval must be positive")
	}
	return nil
}

func validateHarnessV1DispatchOptions(interval time.Duration, workers int) error {
	if interval <= 0 {
		return errors.New("harness v1 dispatch interval must be positive")
	}
	return controller.ValidateHarnessV1DispatchWorkers(workers)
}
