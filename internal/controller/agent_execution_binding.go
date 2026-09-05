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
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

// The binding stage implements the coexistence plan's write-once execution
// binding: every executable agent Task freezes its protocol, backend, and an
// immutable content-addressed execution snapshot before any executor-specific
// side effect. The snapshot store and revisioned backend control are mandatory:
// a controller that cannot prove either one never queues executor demand.

const agentExecutionBindingConflictReason = "BindingConflict"

var errExternalAgentRuntimeMCPToolDescriptorsNotConformed = errors.New("external AgentRuntime MCP tool descriptors do not match current conformance")

type frozenExternalRuntimeBindingDriftError struct{ err error }

func (e *frozenExternalRuntimeBindingDriftError) Error() string { return e.err.Error() }
func (e *frozenExternalRuntimeBindingDriftError) Unwrap() error { return e.err }

func frozenExternalRuntimeBindingDrift(err error) error {
	if err == nil {
		return nil
	}
	return &frozenExternalRuntimeBindingDriftError{err: err}
}

func isFrozenExternalRuntimeBindingDrift(err error) bool {
	var drift *frozenExternalRuntimeBindingDriftError
	return errors.As(err, &drift)
}

// agentExecutionCandidate is the pure resolution product: the prospective
// binding plus the plaintext snapshot body it references. Resolution performs
// reads only; no durable writes or runtime side effects.
type agentExecutionCandidate struct {
	binding             corev1alpha1.AgentExecutionBinding
	snapshotBody        []byte
	workspaceSessionUID string
}

// agentExecutionSnapshotBody is the canonical non-secret executable input
// record frozen into the immutable snapshot. Credentials remain references
// only; raw credential values and TxTokens never enter this structure.
type agentExecutionSnapshotBody struct {
	SchemaVersion    int32                                  `json:"schemaVersion"`
	ContractVersion  string                                 `json:"contractVersion"`
	Backend          string                                 `json:"backend"`
	RuntimeType      string                                 `json:"runtimeType"`
	Agent            agentExecutionSnapshotAgent            `json:"agent"`
	Configuration    agentExecutionSnapshotConfig           `json:"configuration"`
	RuntimeImage     string                                 `json:"runtimeImage"`
	RuntimeProfile   harnessv2.RuntimeProfile               `json:"runtimeProfile"`
	ProfileDigest    string                                 `json:"profileDigest"`
	PoolName         string                                 `json:"poolName"`
	MCPConfiguration *harnessv2.MCPPolicyConfiguration      `json:"mcpConfiguration,omitempty"`
	Prompt           string                                 `json:"prompt"`
	Timeout          string                                 `json:"timeout,omitempty"`
	RetryPolicy      *corev1alpha1.RetryPolicy              `json:"retryPolicy,omitempty"`
	SessionRef       *corev1alpha1.SessionReference         `json:"sessionRef,omitempty"`
	Workspace        *corev1alpha1.WorkspaceConfig          `json:"workspace,omitempty"`
	RuntimeOverride  *corev1alpha1.AgentRuntimeSpec         `json:"runtimeOverride,omitempty"`
	DefaultTools     *agentExecutionSnapshotToolPolicy      `json:"defaultTools,omitempty"`
	HarnessV1        *agentExecutionSnapshotHarnessV1       `json:"harnessV1,omitempty"`
	ExternalRuntime  *agentExecutionSnapshotExternalRuntime `json:"externalRuntime,omitempty"`
	// ExecutionWorkspace freezes the resolved execution-workspace binding for
	// workspace-provider-backed RuntimePools. It is absent for plain pools.
	ExecutionWorkspace *agentExecutionSnapshotWorkspaceBinding `json:"executionWorkspace,omitempty"`
}

// agentExecutionSnapshotExternalRuntime freezes the non-secret registration
// and authentication authority used by one external v2 binding. The runtime's
// live status fence is re-read immediately before dispatch because controller
// epochs and supervisor boots may change without changing the registered
// endpoint or profile.
type agentExecutionSnapshotExternalRuntime struct {
	Namespace                       string                                    `json:"namespace"`
	Endpoint                        string                                    `json:"endpoint"`
	RuntimeInstanceID               string                                    `json:"runtimeInstanceID"`
	Limits                          harnessv2.ProtocolLimits                  `json:"limits"`
	WorkspaceGovernance             harnessv2.WorkspaceGovernanceCapabilities `json:"workspaceGovernance"`
	SupportsDrain                   bool                                      `json:"supportsDrain"`
	SupportsPublicationFinalization bool                                      `json:"supportsPublicationFinalization"`
	ControllerAuth                  agentExecutionSnapshotSecretRef           `json:"controllerAuth"`
	OperationCapability             agentExecutionSnapshotSecretRef           `json:"operationCapability"`
}

// agentExecutionSnapshotWorkspaceBinding freezes the canonical, provider-neutral
// execution-workspace binding. It never carries provider-native identifiers,
// physical workspace names, or secrets.
type agentExecutionSnapshotWorkspaceBinding struct {
	Provider          string                                `json:"provider"`
	ReusePolicy       string                                `json:"reusePolicy"`
	CleanupPolicy     string                                `json:"cleanupPolicy"`
	WorkspaceSlot     string                                `json:"workspaceSlot"`
	SessionUID        string                                `json:"sessionUID,omitempty"`
	SessionKey        string                                `json:"sessionKey"`
	TemplateNamespace string                                `json:"templateNamespace,omitempty"`
	TemplateName      string                                `json:"templateName,omitempty"`
	Class             *agentExecutionSnapshotWorkspaceClass `json:"class,omitempty"`
	BindingDigest     string                                `json:"bindingDigest"`
}

// agentExecutionSnapshotWorkspaceClass freezes the controller-first class
// binding for class-selected execution workspaces. It carries only Orka-owned
// identity and policy: no provider-native identifiers and no secrets.
type agentExecutionSnapshotWorkspaceClass struct {
	Name               string                               `json:"name"`
	UID                string                               `json:"uid"`
	Generation         int64                                `json:"generation"`
	ProfileHash        string                               `json:"profileHash"`
	ProviderName       string                               `json:"providerName"`
	ProviderUID        string                               `json:"providerUID"`
	ProviderGeneration int64                                `json:"providerGeneration"`
	ProviderConfigUID  string                               `json:"providerConfigUID,omitempty"`
	EffectiveOnDetach  string                               `json:"effectiveOnDetach"`
	SuspendMode        string                               `json:"suspendMode,omitempty"`
	SandboxVolume      *agentExecutionSnapshotSandboxVolume `json:"sandboxVolume,omitempty"`
	MaxSuspended       *int32                               `json:"maxSuspended,omitempty"`
	DefaultOnDetach    string                               `json:"defaultOnDetach"`
	AllowedOnDetach    []string                             `json:"allowedOnDetach"`
	DetachTimeout      string                               `json:"detachTimeout"`
	IdleTimeout        string                               `json:"idleTimeout,omitempty"`
	MaxLifetime        string                               `json:"maxLifetime,omitempty"`
	DeletionPolicy     struct {
		ProviderResources string `json:"providerResources"`
		PersistentVolumes string `json:"persistentVolumes"`
		Checkpoints       string `json:"checkpoints"`
	} `json:"deletionPolicy"`
}

// agentExecutionSnapshotSandboxVolume freezes the durable workspace PVC shape
// for suspend-capable agent-sandbox class bindings.
type agentExecutionSnapshotSandboxVolume struct {
	StorageClassName string   `json:"storageClassName,omitempty"`
	StorageClassUID  string   `json:"storageClassUID,omitempty"`
	AccessModes      []string `json:"accessModes"`
	Capacity         string   `json:"capacity"`
}

type agentExecutionSnapshotAgent struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

type agentExecutionSnapshotConfig struct {
	AgentUID        string `json:"agentUID"`
	AgentGeneration int64  `json:"agentGeneration"`
	ProviderKind    string `json:"providerKind"`
	Model           string `json:"model"`
	MaxTurns        int32  `json:"maxTurns"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
	SystemPrompt    string `json:"systemPrompt,omitempty"`
}

// agentExecutionSnapshotToolPolicy preserves the omitted-versus-explicit-empty
// allowlist distinction from the Agent defaults.
type agentExecutionSnapshotToolPolicy struct {
	AllowedToolsOmitted bool     `json:"allowedToolsOmitted"`
	AllowedTools        []string `json:"allowedTools"`
	AllowBash           *bool    `json:"allowBash,omitempty"`
}

// agentExecutionSnapshotHarnessV1 freezes the non-secret wrapper or external
// endpoint identity used by the v1 dispatcher. Secret contents never enter the
// snapshot; the exact Secret UID/resourceVersion is verified again before the
// first executor side effect.
type agentExecutionSnapshotHarnessV1 struct {
	Endpoint                  string                                           `json:"endpoint"`
	Backend                   string                                           `json:"backend"`
	RuntimeName               string                                           `json:"runtimeName"`
	TaskSpecDigest            string                                           `json:"taskSpecDigest,omitempty"`
	ToolExecutionMode         string                                           `json:"toolExecutionMode,omitempty"`
	BrokeredToolClasses       []corev1alpha1.AgentRuntimeBrokeredToolClass     `json:"brokeredToolClasses,omitempty"`
	BrokeredTools             []agentExecutionSnapshotHarnessV1BrokeredTool    `json:"brokeredTools,omitempty"`
	RuntimeAuthOnly           bool                                             `json:"runtimeAuthOnly,omitempty"`
	AuthSecretNamespace       string                                           `json:"authSecretNamespace"`
	AuthSecretName            string                                           `json:"authSecretName"`
	AuthSecretKey             string                                           `json:"authSecretKey"`
	AuthSecretUID             string                                           `json:"authSecretUID"`
	AuthSecretResourceVersion string                                           `json:"authSecretResourceVersion"`
	DuplicateSafe             bool                                             `json:"duplicateSafe"`
	SessionName               string                                           `json:"sessionName"`
	SessionBootstrap          *agentExecutionSnapshotHarnessV1SessionBootstrap `json:"sessionBootstrap,omitempty"`
	CredentialRefs            []agentExecutionSnapshotSecretRef                `json:"credentialRefs,omitempty"`
}

// agentExecutionSnapshotHarnessV1SessionBootstrap freezes the canonical
// transcript suffix used to give a fresh v1 CLI process conversation context.
// The rendered JSONL is kept inside the encrypted, content-addressed snapshot
// so dispatch and recovery never re-read mutable transcript state.
type agentExecutionSnapshotHarnessV1SessionBootstrap struct {
	SchemaVersion   int    `json:"schemaVersion"`
	SessionUID      string `json:"sessionUID"`
	ControlVersion  int64  `json:"controlVersion"`
	LeaseGeneration int64  `json:"leaseGeneration"`
	Artifact        string `json:"artifact"`
	Digest          string `json:"digest"`
	MessageCount    uint32 `json:"messageCount"`
	TotalMessages   int    `json:"totalMessages"`
	Truncated       bool   `json:"truncated,omitempty"`
}

type agentExecutionSnapshotSecretRef struct {
	Role            string   `json:"role"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	UID             string   `json:"uid"`
	ResourceVersion string   `json:"resourceVersion"`
	Keys            []string `json:"keys"`
}

type verifiedAgentExecution struct {
	binding          *corev1alpha1.AgentExecutionBinding
	snapshot         *store.AgentExecutionSnapshot
	promptAttempt    *store.PromptAttempt
	body             agentExecutionSnapshotBody
	plan             ACPRuntimePlan
	externalRuntime  *corev1alpha1.AgentRuntime
	frozenTask       *corev1alpha1.Task
	configuration    harnessv2.AgentSessionConfiguration
	mcpConfiguration harnessv2.MCPPolicyConfiguration
}

// resolveAgentExecutionCandidate performs pure candidate resolution for an
// explicitly v2-classified built-in agent Task: it resolves the frozen session
// configuration and runtime plan, assembles the snapshot body, and computes
// the canonical binding. It performs no durable writes.
func (r *TaskReconciler) resolveAgentExecutionCandidate(
	ctx context.Context,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
) (*agentExecutionCandidate, error) {
	return r.resolveAgentExecutionCandidateWithWorkspaceSessionUID(ctx, task, agent, "")
}

func (r *TaskReconciler) resolveAgentExecutionCandidateWithWorkspaceSessionUID(
	ctx context.Context,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
	workspaceSessionUID string,
) (*agentExecutionCandidate, error) {
	if r.AgentExecutionSnapshots == nil {
		return nil, errors.New("encrypted agent execution snapshot store is required; execution admission fails closed")
	}
	if task == nil || task.UID == "" || task.Generation < 1 {
		return nil, errors.New("task UID and positive spec generation are required for execution binding")
	}
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.RuntimeRef != nil &&
		strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		if strings.TrimSpace(workspaceSessionUID) != "" || taskRequestsExecutionWorkspace(task) {
			return nil, permanentACPAgentConfiguration(errors.New("external v2 AgentRuntime bindings do not support Task.spec.execution.workspace"))
		}
		return r.resolveExternalAgentExecutionCandidate(ctx, task, agent)
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	configuration, err := resolveACPAgentSessionConfiguration(ctx, reader, task, agent)
	if err != nil {
		return nil, err
	}
	plan, err := PlanACPRuntimeWithConfiguration(task, agent, r.ACPRuntimeImages, configuration)
	if err != nil {
		return nil, permanentACPAgentConfiguration(err)
	}
	workspaceSessionUID = strings.TrimSpace(workspaceSessionUID)
	if workspaceSessionUID == "" && taskRequestsWorkspaceClass(task) &&
		task.Spec.Execution.Workspace.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		// A class-backed continuation must resolve its immutable Session UID
		// before the class resolver considers live storage. The linked
		// RuntimePool carries the frozen durable-volume identity, so requiring
		// the original StorageClass or a current cluster default first would
		// reject a valid continuation after that class was retired.
		plannedUID, sessionErr := r.planACPWorkspaceSessionUID(ctx, task)
		if sessionErr != nil {
			wrapped := fmt.Errorf("plan immutable execution-workspace Session identity: %w", sessionErr)
			if permanentACPWorkspaceSessionPlanningError(sessionErr) {
				return nil, permanentACPAgentConfiguration(wrapped)
			}
			return nil, wrapped
		}
		workspaceSessionUID = plannedUID
	}
	var resolvedClass *acpResolvedWorkspaceClass
	if workspaceSessionUID == "" {
		resolvedClass, err = r.resolveACPWorkspaceClass(ctx, task)
	} else {
		resolvedClass, err = r.resolveACPWorkspaceClassWithSessionUID(ctx, task, workspaceSessionUID)
	}
	if err != nil {
		return nil, classifyACPWorkspaceClassResolutionError(err)
	}
	workspaceBinding, err := validateACPWorkspaceBindingRequestWithClass(task, r.ExecutionWorkspaceDefaultProvider, r.EnforceNamespaceIsolation, resolvedClass)
	if err != nil {
		return nil, permanentACPAgentConfiguration(err)
	}
	if workspaceBinding != nil && workspaceBinding.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		if workspaceSessionUID == "" {
			plannedUID, sessionErr := r.planACPWorkspaceSessionUID(ctx, task)
			if sessionErr != nil {
				wrapped := fmt.Errorf("plan immutable execution-workspace Session identity: %w", sessionErr)
				if permanentACPWorkspaceSessionPlanningError(sessionErr) {
					return nil, permanentACPAgentConfiguration(wrapped)
				}
				return nil, wrapped
			}
			workspaceSessionUID = plannedUID
		}
		if taskRequestsWorkspaceClass(task) {
			resolvedClass, err = r.resolveACPWorkspaceClassWithSessionUID(ctx, task, workspaceSessionUID)
			if err != nil {
				return nil, classifyACPWorkspaceClassResolutionError(err)
			}
		}
		workspaceBinding, err = resolveACPWorkspaceBindingWithClass(
			task, r.ExecutionWorkspaceDefaultProvider, r.EnforceNamespaceIsolation, workspaceSessionUID, resolvedClass,
		)
		if err != nil {
			return nil, permanentACPAgentConfiguration(err)
		}
	}
	plan, err = applyACPWorkspaceBindingToPlan(plan, workspaceBinding)
	if err != nil {
		return nil, err
	}
	var mcpConfiguration harnessv2.MCPPolicyConfiguration
	if r.MCPRegistry != nil {
		mcpConfiguration, err = buildRuntimeSessionMCPConfigurationWithRegistry(
			ctx, reader, task, agent, plan.Profile, r.MCPRegistry,
		)
	} else {
		mcpConfiguration, err = buildRuntimeSessionMCPConfiguration(
			ctx, reader, task, agent, plan.Profile,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("resolve frozen ACP MCP configuration: %w", err)
	}

	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: task.Namespace}, namespace); err != nil {
		return nil, fmt.Errorf("resolve task namespace identity: %w", err)
	}

	body := agentExecutionSnapshotBody{
		SchemaVersion:   store.AgentExecutionSnapshotSchemaVersion,
		ContractVersion: string(corev1alpha1.AgentRuntimeContractHarnessV2),
		Backend:         string(corev1alpha1.AgentExecutionBackendRuntimePool),
		RuntimeType:     string(agent.Spec.Runtime.Type),
		Agent: agentExecutionSnapshotAgent{
			Namespace:  agent.Namespace,
			Name:       agent.Name,
			UID:        string(agent.UID),
			Generation: agent.Generation,
		},
		Configuration: agentExecutionSnapshotConfig{
			AgentUID:        configuration.AgentUID,
			AgentGeneration: configuration.AgentGeneration,
			ProviderKind:    configuration.ProviderKind,
			Model:           configuration.Model,
			MaxTurns:        configuration.MaxTurns,
			ReasoningEffort: configuration.ReasoningEffort,
			SystemPrompt:    configuration.SystemPrompt,
		},
		RuntimeImage:     plan.Image,
		RuntimeProfile:   plan.Profile,
		ProfileDigest:    string(plan.Digest),
		PoolName:         plan.PoolName,
		MCPConfiguration: &mcpConfiguration,
		Prompt:           task.Spec.Prompt,
		RetryPolicy:      task.Spec.RetryPolicy.DeepCopy(),
		SessionRef:       task.Spec.SessionRef.DeepCopy(),
		Workspace:        task.Spec.Workspace.DeepCopy(),
		RuntimeOverride:  task.Spec.AgentRuntime.DeepCopy(),
	}
	if task.Spec.Timeout != nil {
		body.Timeout = task.Spec.Timeout.Duration.String()
	}
	if workspaceBinding != nil {
		body.ExecutionWorkspace = &agentExecutionSnapshotWorkspaceBinding{
			Provider:          string(workspaceBinding.Provider),
			ReusePolicy:       string(workspaceBinding.ReusePolicy),
			CleanupPolicy:     string(workspaceBinding.CleanupPolicy),
			WorkspaceSlot:     workspaceBinding.WorkspaceSlot,
			SessionUID:        workspaceBinding.SessionUID,
			SessionKey:        workspaceBinding.SessionKey,
			TemplateNamespace: workspaceBinding.TemplateNamespace,
			TemplateName:      workspaceBinding.TemplateName,
			Class:             snapshotWorkspaceClassFromBinding(workspaceBinding.Class),
			BindingDigest:     workspaceBinding.BindingDigest,
		}
	}
	if agent.Spec.Runtime.DefaultAllowedTools != nil || agent.Spec.Runtime.DefaultAllowBash != nil {
		body.DefaultTools = &agentExecutionSnapshotToolPolicy{
			AllowedToolsOmitted: agent.Spec.Runtime.DefaultAllowedTools == nil,
			AllowedTools:        append([]string(nil), agent.Spec.Runtime.DefaultAllowedTools...),
			AllowBash:           agent.Spec.Runtime.DefaultAllowBash,
		}
	}

	encoded, err := canonicalAgentExecutionSnapshotBody(body)
	if err != nil {
		return nil, err
	}
	snapshotDigest := store.CanonicalAgentExecutionSnapshotDigest(encoded)

	binding := corev1alpha1.AgentExecutionBinding{
		SchemaVersion:   1,
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		Backend:         corev1alpha1.AgentExecutionBackendRuntimePool,
		Task: corev1alpha1.AgentExecutionBindingTaskRef{
			NamespaceUID:        namespace.UID,
			UID:                 task.UID,
			BoundSpecGeneration: task.Generation,
		},
		Agent: &corev1alpha1.AgentExecutionAgentRef{
			Namespace:  agent.Namespace,
			Name:       agent.Name,
			UID:        agent.UID,
			Generation: agent.Generation,
		},
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
			ID:            string(task.UID) + "/" + snapshotDigest,
			Digest:        snapshotDigest,
			SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		},
		RuntimeType:                       agent.Spec.Runtime.Type,
		RuntimeProfileDigest:              string(plan.Digest),
		RuntimeProfileDigestSchemaVersion: 1,
	}

	digest, err := canonicalAgentExecutionBindingDigest(binding)
	if err != nil {
		return nil, err
	}
	binding.BindingDigest = digest
	binding.BoundAt = metav1.Now()

	return &agentExecutionCandidate{
		binding: binding, snapshotBody: encoded, workspaceSessionUID: workspaceSessionUID,
	}, nil
}

func (r *TaskReconciler) resolveExternalAgentExecutionCandidate(
	ctx context.Context,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
) (*agentExecutionCandidate, error) {
	if task == nil || task.UID == "" || task.Generation < 1 || agent == nil || agent.UID == "" || agent.Generation < 1 {
		return nil, errors.New("task and Agent immutable identities are required for an external v2 binding")
	}
	if agent.Spec.Runtime == nil || agent.Spec.Runtime.RuntimeRef == nil || strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) == "" {
		return nil, permanentACPAgentConfiguration(errors.New("external v2 binding requires Agent.spec.runtime.runtimeRef"))
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if reader == nil {
		return nil, errors.New("API reader is required for external v2 binding")
	}

	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: task.Namespace}, namespace); err != nil {
		return nil, fmt.Errorf("resolve Task namespace identity: %w", err)
	}
	runtime := &corev1alpha1.AgentRuntime{}
	runtimeName := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: runtimeName}, runtime); err != nil {
		return nil, fmt.Errorf("resolve external AgentRuntime %q: %w", runtimeName, err)
	}
	if runtime.UID == "" || runtime.Generation < 1 {
		return nil, errors.New("external AgentRuntime immutable identity is incomplete")
	}
	profile, external, err := r.resolveExternalAgentRuntimeSnapshot(ctx, task, runtime)
	if err != nil {
		return nil, err
	}
	registry := r.MCPRegistry
	if registry == nil {
		registry = tools.DefaultRegistry
	}
	mcpConfiguration, err := buildExternalRuntimeSessionMCPConfigurationWithRegistry(
		ctx, reader, task, agent, runtime, profile, registry,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve frozen external v2 MCP configuration: %w", err)
	}
	if runtime.Status.ObservedCapabilities.MCPToolDescriptorDigest != mcpConfiguration.ToolPolicy.DescriptorDigest {
		return nil, errors.New("external AgentRuntime MCP tool descriptors have not passed current conformance")
	}

	body := agentExecutionSnapshotBody{
		SchemaVersion:   store.AgentExecutionSnapshotSchemaVersion,
		ContractVersion: string(corev1alpha1.AgentRuntimeContractHarnessV2),
		Backend:         string(corev1alpha1.AgentExecutionBackendExternalEndpoint),
		Agent: agentExecutionSnapshotAgent{
			Namespace: agent.Namespace, Name: agent.Name, UID: string(agent.UID), Generation: agent.Generation,
		},
		RuntimeProfile:   profile,
		ProfileDigest:    runtime.Spec.Capabilities.Profile.Digest,
		MCPConfiguration: &mcpConfiguration,
		Prompt:           task.Spec.Prompt,
		RetryPolicy:      task.Spec.RetryPolicy.DeepCopy(),
		SessionRef:       task.Spec.SessionRef.DeepCopy(),
		Workspace:        task.Spec.Workspace.DeepCopy(),
		RuntimeOverride:  task.Spec.AgentRuntime.DeepCopy(),
		ExternalRuntime:  external,
	}
	if task.Spec.Timeout != nil {
		body.Timeout = task.Spec.Timeout.Duration.String()
	}
	if agent.Spec.Runtime.DefaultAllowedTools != nil || agent.Spec.Runtime.DefaultAllowBash != nil {
		body.DefaultTools = &agentExecutionSnapshotToolPolicy{
			AllowedToolsOmitted: agent.Spec.Runtime.DefaultAllowedTools == nil,
			AllowedTools:        append([]string(nil), agent.Spec.Runtime.DefaultAllowedTools...),
			AllowBash:           agent.Spec.Runtime.DefaultAllowBash,
		}
	}
	encoded, err := canonicalAgentExecutionSnapshotBody(body)
	if err != nil {
		return nil, err
	}
	snapshotDigest := store.CanonicalAgentExecutionSnapshotDigest(encoded)
	binding := corev1alpha1.AgentExecutionBinding{
		SchemaVersion:   1,
		ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
		Backend:         corev1alpha1.AgentExecutionBackendExternalEndpoint,
		Task: corev1alpha1.AgentExecutionBindingTaskRef{
			NamespaceUID: namespace.UID, UID: task.UID, BoundSpecGeneration: task.Generation,
		},
		Agent: &corev1alpha1.AgentExecutionAgentRef{
			Namespace: agent.Namespace, Name: agent.Name, UID: agent.UID, Generation: agent.Generation,
		},
		Snapshot: corev1alpha1.AgentExecutionSnapshotRef{
			ID: string(task.UID) + "/" + snapshotDigest, Digest: snapshotDigest,
			SchemaVersion: store.AgentExecutionSnapshotSchemaVersion,
		},
		RuntimeRef: &corev1alpha1.AgentExecutionRuntimeRef{
			Name: runtime.Name, UID: runtime.UID, Generation: runtime.Generation,
		},
		RuntimeProfileDigest:              runtime.Spec.Capabilities.Profile.Digest,
		RuntimeProfileDigestSchemaVersion: int32(harnessv2.ProfileDigestSchemaVersion),
	}
	binding.BindingDigest, err = canonicalAgentExecutionBindingDigest(binding)
	if err != nil {
		return nil, err
	}
	binding.BoundAt = metav1.Now()
	return &agentExecutionCandidate{binding: binding, snapshotBody: encoded}, nil
}

//nolint:gocyclo // External runtime admission keeps every registered, observed, and secret-authority check in one fail-closed boundary.
func (r *TaskReconciler) resolveExternalAgentRuntimeSnapshot(
	ctx context.Context,
	task *corev1alpha1.Task,
	runtime *corev1alpha1.AgentRuntime,
) (harnessv2.RuntimeProfile, *agentExecutionSnapshotExternalRuntime, error) {
	return r.resolveExternalAgentRuntimeSnapshotWithReadyRequirement(ctx, task, runtime, true)
}

//nolint:gocyclo // This keeps the existing fail-closed registration, observation, epoch, and Secret checks in one boundary while varying only admission readiness.
func (r *TaskReconciler) resolveExternalAgentRuntimeSnapshotWithReadyRequirement(
	ctx context.Context,
	task *corev1alpha1.Task,
	runtime *corev1alpha1.AgentRuntime,
	requireReady bool,
) (harnessv2.RuntimeProfile, *agentExecutionSnapshotExternalRuntime, error) {
	if runtime == nil {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime is required")
	}
	if err := validateAgentRuntimeSpec(runtime); err != nil {
		return harnessv2.RuntimeProfile{}, nil, permanentACPAgentConfiguration(err)
	}
	if err := externalAgentRuntimeConformanceError(task, runtime, requireReady); err != nil {
		return harnessv2.RuntimeProfile{}, nil, err
	}
	capabilities := runtime.Spec.Capabilities
	observed := runtime.Status.ObservedCapabilities
	if capabilities == nil || capabilities.Profile == nil || capabilities.Limits == nil ||
		capabilities.WorkspaceGovernance == nil || observed == nil || observed.Limits == nil || observed.WorkspaceGovernance == nil {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime current-generation conformance record is incomplete")
	}
	currentFence, err := r.ControllerEpochManager.CurrentFence(ctx)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, fmt.Errorf("resolve current controller epoch for external AgentRuntime binding: %w", err)
	}
	if observed.ControllerEpoch != currentFence.Epoch {
		return harnessv2.RuntimeProfile{}, nil, fmt.Errorf(
			"external AgentRuntime is fenced to controller epoch %d, current epoch is %d",
			observed.ControllerEpoch, currentFence.Epoch,
		)
	}
	profile, err := agentRuntimeProfile(*capabilities.Profile)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, permanentACPAgentConfiguration(err)
	}
	limits, err := agentRuntimeProtocolLimits(*capabilities.Limits)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, permanentACPAgentConfiguration(err)
	}
	observedLimits, err := agentRuntimeProtocolLimits(*observed.Limits)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime observed protocol limits are invalid")
	}
	governance, err := agentRuntimeWorkspaceGovernance(*capabilities.WorkspaceGovernance)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, permanentACPAgentConfiguration(err)
	}
	observedGovernance, err := agentRuntimeWorkspaceGovernance(*observed.WorkspaceGovernance)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime observed workspace governance is invalid")
	}
	if observed.ProtocolVersion != harnessv2.ProtocolVersion || observed.Transport != "http+ndjson" ||
		observed.ACPVersion != harnessv2.ACPProfileV1 || observed.RuntimeInstanceID != capabilities.RuntimeInstanceID ||
		observed.RuntimeProfileDigest != capabilities.Profile.Digest ||
		observed.ProfileDigestSchemaVersion != capabilities.Profile.DigestSchemaVersion ||
		observed.AdapterName != capabilities.Profile.AdapterName || observed.AdapterDigest != capabilities.Profile.AdapterDigest ||
		observed.ProviderKind != capabilities.Profile.ProviderKind || observed.Model != capabilities.Profile.Model ||
		observedLimits != limits || observedGovernance != governance ||
		observed.SupportsDrain != capabilities.SupportsDrain ||
		observed.SupportsPublicationFinalization != capabilities.SupportsPublicationFinalization {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime observed capabilities do not exactly match its registered v2 claims")
	}
	if !governance.Strict() {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime does not provide strict workspace governance")
	}
	if err := capabilities.ValidateStrictWorkspaceIntent(corev1alpha1.WorkspaceIntent(profile.WorkspaceIntent)); err != nil {
		return harnessv2.RuntimeProfile{}, nil, permanentACPAgentConfiguration(err)
	}
	reconciler := &AgentRuntimeReconciler{Client: r.Client, APIReader: r.APIReader}
	auth, err := reconciler.agentRuntimeAuthMaterial(ctx, runtime)
	if err != nil {
		return harnessv2.RuntimeProfile{}, nil, err
	}
	if runtime.Status.ObservedControllerAuthRefResourceVersion != auth.controllerResourceVersion ||
		runtime.Status.ObservedOperationCapabilityRefResourceVersion != auth.capabilityResourceVersion {
		return harnessv2.RuntimeProfile{}, nil, errors.New("external AgentRuntime authentication material changed after conformance")
	}
	controllerRef := runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	capabilityRef := runtime.Spec.ClientAuth.OperationCapabilitySecretRef
	external := &agentExecutionSnapshotExternalRuntime{
		Namespace:                       runtime.Namespace,
		Endpoint:                        strings.TrimSpace(runtime.Spec.Deployment.Endpoint),
		RuntimeInstanceID:               capabilities.RuntimeInstanceID,
		Limits:                          limits,
		WorkspaceGovernance:             governance,
		SupportsDrain:                   capabilities.SupportsDrain,
		SupportsPublicationFinalization: capabilities.SupportsPublicationFinalization,
		ControllerAuth: agentExecutionSnapshotSecretRef{
			Role: "controller-auth", Namespace: runtime.Namespace, Name: controllerRef.Name,
			UID: string(auth.controllerSecretUID), ResourceVersion: auth.controllerResourceVersion, Keys: []string{controllerRef.Key},
		},
		OperationCapability: agentExecutionSnapshotSecretRef{
			Role: "operation-capability", Namespace: runtime.Namespace, Name: capabilityRef.Name,
			UID: string(auth.capabilitySecretUID), ResourceVersion: auth.capabilityResourceVersion, Keys: []string{capabilityRef.Key},
		},
	}
	return profile, external, nil
}

func permanentACPWorkspaceSessionPlanningError(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrValidation)
}

func classifyACPWorkspaceClassResolutionError(err error) error {
	if isRetryableACPWorkspaceClassResolutionError(err) || errors.Is(err, errACPWorkspacePlanningTransient) {
		return err
	}
	return permanentACPAgentConfiguration(err)
}

// canonicalAgentExecutionBindingDigest computes the canonical binding digest
// over the binding with its digest and timestamp cleared, so re-resolution of
// identical inputs is digest-stable across reconciles.
func canonicalAgentExecutionBindingDigest(binding corev1alpha1.AgentExecutionBinding) (string, error) {
	normalized := *binding.DeepCopy()
	normalized.BindingDigest = ""
	normalized.BoundAt = metav1.Time{}
	return acpDomainDigest("agent-execution-binding", normalized)
}

func canonicalAgentExecutionSnapshotBody(body agentExecutionSnapshotBody) ([]byte, error) {
	encoded, err := harnessv2.CanonicalValue(body)
	if err != nil {
		return nil, fmt.Errorf("canonicalize snapshot body: %w", err)
	}
	return encoded, nil
}

// persistAgentExecutionSnapshot idempotently stores the immutable snapshot.
func (r *TaskReconciler) persistAgentExecutionSnapshot(
	ctx context.Context,
	task *corev1alpha1.Task,
	candidate *agentExecutionCandidate,
) error {
	return r.AgentExecutionSnapshots.PersistAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshot{
		TaskUID:       string(task.UID),
		Digest:        candidate.binding.Snapshot.Digest,
		SchemaVersion: candidate.binding.Snapshot.SchemaVersion,
		Body:          candidate.snapshotBody,
	})
}

// errAgentExecutionBindingConflict marks a permanent conflict: an existing
// binding never gets overwritten and the Task never dispatches under a
// mismatched candidate.
type errAgentExecutionBindingConflict struct {
	existingDigest  string
	candidateDigest string
}

func (e *errAgentExecutionBindingConflict) Error() string {
	return fmt.Sprintf("task already carries immutable execution binding %s; refusing to replace it with %s",
		e.existingDigest, e.candidateDigest)
}

// persistAgentExecutionBinding implements the uncached compare-if-absent
// write-once binding CAS. It never overwrites an existing binding.
func (r *TaskReconciler) persistAgentExecutionBinding(
	ctx context.Context,
	task *corev1alpha1.Task,
	candidate *agentExecutionCandidate,
) (*corev1alpha1.AgentExecutionBinding, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	current := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		return nil, fmt.Errorf("uncached task read before binding: %w", err)
	}
	if current.UID != task.UID {
		return nil, fmt.Errorf("task UID changed from %s to %s; never dispatching", task.UID, current.UID)
	}
	// A matching persisted binding is authoritative even if this reconcile is
	// recovering an ambiguous status-patch response.
	if existing := current.Status.AgentExecutionBinding; existing != nil {
		if existing.BindingDigest == candidate.binding.BindingDigest {
			return existing, nil
		}
		return nil, &errAgentExecutionBindingConflict{
			existingDigest:  existing.BindingDigest,
			candidateDigest: candidate.binding.BindingDigest,
		}
	}
	if current.Generation != candidate.binding.Task.BoundSpecGeneration {
		return nil, fmt.Errorf("task spec generation changed from bound candidate %d to %d; refusing stale binding",
			candidate.binding.Task.BoundSpecGeneration, current.Generation)
	}
	if !current.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("task is deleting; a deleting task may be classified only for cleanup and never dispatches")
	}
	if !controllerutil.ContainsFinalizer(current, labels.TaskFinalizer) {
		return nil, errors.New("task cleanup finalizer is missing; refusing to persist an executable binding")
	}
	base := current.DeepCopy()
	current.Status.AgentExecutionBinding = candidate.binding.DeepCopy()
	if err := r.Status().Patch(ctx, current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		// A transport error can arrive after the API server committed the status
		// patch. Re-read before classifying the failure so an exact binding is
		// recognized instead of treated as a conflicting retry.
		latest := &corev1alpha1.Task{}
		if readErr := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, latest); readErr == nil &&
			latest.UID == task.UID {
			if existing := latest.Status.AgentExecutionBinding; existing != nil {
				if existing.BindingDigest == candidate.binding.BindingDigest {
					return existing, nil
				}
				return nil, &errAgentExecutionBindingConflict{
					existingDigest:  existing.BindingDigest,
					candidateDigest: candidate.binding.BindingDigest,
				}
			}
		}
		return nil, fmt.Errorf("write-once binding patch: %w", err)
	}
	return current.Status.AgentExecutionBinding, nil
}

func decodeAgentExecutionSnapshot(body []byte) (agentExecutionSnapshotBody, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var decoded agentExecutionSnapshotBody
	if err := decoder.Decode(&decoded); err != nil {
		return agentExecutionSnapshotBody{}, fmt.Errorf("decode agent execution snapshot: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return agentExecutionSnapshotBody{}, errors.New("decode agent execution snapshot: multiple JSON values")
		}
		return agentExecutionSnapshotBody{}, fmt.Errorf("decode agent execution snapshot trailer: %w", err)
	}
	return decoded, nil
}

//nolint:gocyclo // Snapshot validation intentionally checks every immutable v2 execution field together.
func validateAgentExecutionSnapshot(
	binding *corev1alpha1.AgentExecutionBinding,
	snapshot *store.AgentExecutionSnapshot,
	body agentExecutionSnapshotBody,
) (ACPRuntimePlan, harnessv2.AgentSessionConfiguration, harnessv2.MCPPolicyConfiguration, error) {
	if binding == nil || snapshot == nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("binding and execution snapshot are required")
	}
	key := store.AgentExecutionSnapshotKey{TaskUID: string(binding.Task.UID), Digest: binding.Snapshot.Digest}
	if binding.Snapshot.ID != key.ID() || snapshot.TaskUID != key.TaskUID || snapshot.Digest != key.Digest {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot identity does not exactly match the binding")
	}
	if binding.Snapshot.SchemaVersion != store.AgentExecutionSnapshotSchemaVersion ||
		snapshot.SchemaVersion != binding.Snapshot.SchemaVersion || body.SchemaVersion != binding.Snapshot.SchemaVersion {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("unsupported or mismatched execution snapshot schema version")
	}
	if binding.SchemaVersion != 1 || binding.RuntimeProfileDigestSchemaVersion != 1 {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("unsupported execution binding or RuntimeProfile digest schema version")
	}
	if computed := store.CanonicalAgentExecutionSnapshotDigest(snapshot.Body); computed != binding.Snapshot.Digest {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("execution snapshot body digest %s does not match binding digest %s", computed, binding.Snapshot.Digest)
	}
	canonicalBody, err := canonicalAgentExecutionSnapshotBody(body)
	if err != nil || !bytes.Equal(canonicalBody, snapshot.Body) {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot body is not canonical")
	}
	if body.ContractVersion != string(binding.ContractVersion) || body.Backend != string(binding.Backend) ||
		body.RuntimeType != string(binding.RuntimeType) || binding.Agent == nil ||
		body.Agent.Namespace != binding.Agent.Namespace || body.Agent.Name != binding.Agent.Name ||
		body.Agent.UID != string(binding.Agent.UID) || body.Agent.Generation != binding.Agent.Generation {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot route or Agent identity does not exactly match the binding")
	}
	if body.Timeout != "" {
		if _, err := time.ParseDuration(body.Timeout); err != nil {
			return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen Task timeout: %w", err)
		}
	}
	if err := body.RuntimeProfile.Validate(); err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen RuntimeProfile: %w", err)
	}
	if body.MCPConfiguration == nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot is missing the frozen MCP policy configuration")
	}
	mcpConfiguration := *body.MCPConfiguration
	if err := mcpConfiguration.ValidateProfile(body.RuntimeProfile); err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen MCP policy configuration: %w", err)
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(body.RuntimeProfile)
	if err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("digest frozen RuntimeProfile: %w", err)
	}
	if string(profileDigest) != body.ProfileDigest || body.ProfileDigest != binding.RuntimeProfileDigest {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("frozen RuntimeProfile digest does not exactly match the binding")
	}
	if binding.Backend == corev1alpha1.AgentExecutionBackendExternalEndpoint {
		if err := validateExternalAgentExecutionSnapshot(binding, body); err != nil {
			return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, err
		}
		return ACPRuntimePlan{Profile: body.RuntimeProfile, Digest: profileDigest},
			harnessv2.AgentSessionConfiguration{}, mcpConfiguration, nil
	}
	if binding.Backend != corev1alpha1.AgentExecutionBackendRuntimePool || binding.RuntimeRef != nil || body.ExternalRuntime != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot has an unsupported or inconsistent ACP backend")
	}
	configuration := harnessv2.AgentSessionConfiguration{
		AgentUID: body.Configuration.AgentUID, AgentGeneration: body.Configuration.AgentGeneration,
		ProviderKind: body.Configuration.ProviderKind, Model: body.Configuration.Model,
		MaxTurns: body.Configuration.MaxTurns, ReasoningEffort: body.Configuration.ReasoningEffort,
		SystemPrompt: body.Configuration.SystemPrompt,
	}
	if configuration.AgentUID != body.Agent.UID || configuration.AgentGeneration != body.Agent.Generation {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot configuration Agent identity is inconsistent")
	}
	if body.RuntimeType != body.Configuration.ProviderKind || body.RuntimeType != body.RuntimeProfile.ProviderKind {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("execution snapshot runtime type is inconsistent with the frozen configuration/profile")
	}
	if err := configuration.ValidateProfile(body.RuntimeProfile); err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen Agent configuration: %w", err)
	}
	if !ACPRuntimeImageAvailable(body.RuntimeImage) {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("frozen runtime image is not digest pinned")
	}
	poolIdentityDigest, err := acpDomainDigest("runtime-pool-identity", map[string]string{
		acpRuntimePoolIdentityProfileDigestKey: body.ProfileDigest, acpRuntimePoolIdentityRuntimeImageKey: body.RuntimeImage,
	})
	if err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, err
	}
	wantPoolName := acpRuntimePoolName(body.RuntimeType, harnessv2.ProfileDigest(poolIdentityDigest))
	workspaceBinding, err := verifiedSnapshotWorkspaceBinding(binding, body)
	if err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, err
	}
	if workspaceBinding != nil {
		workspacePlan, workspaceErr := applyACPWorkspaceBindingToPlan(ACPRuntimePlan{
			PoolName: wantPoolName, Image: body.RuntimeImage, Profile: body.RuntimeProfile, Digest: profileDigest,
		}, workspaceBinding)
		if workspaceErr != nil {
			return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, workspaceErr
		}
		wantPoolName = workspacePlan.PoolName
	}
	if strings.TrimSpace(body.PoolName) == "" || body.PoolName != wantPoolName {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, errors.New("frozen RuntimePool identity is inconsistent")
	}
	return ACPRuntimePlan{
		PoolName: body.PoolName, Image: body.RuntimeImage, Profile: body.RuntimeProfile, Digest: profileDigest,
		Workspace: workspaceBinding,
	}, configuration, mcpConfiguration, nil
}

func validateExternalAgentExecutionSnapshot(
	binding *corev1alpha1.AgentExecutionBinding,
	body agentExecutionSnapshotBody,
) error {
	external := body.ExternalRuntime
	if binding == nil || binding.RuntimeRef == nil || external == nil {
		return errors.New("external v2 execution snapshot is missing its frozen AgentRuntime authority")
	}
	if binding.RuntimeType != "" || body.RuntimeType != "" || body.RuntimeImage != "" || body.PoolName != "" ||
		body.ExecutionWorkspace != nil || body.HarnessV1 != nil || body.Configuration != (agentExecutionSnapshotConfig{}) {
		return errors.New("external v2 execution snapshot carries RuntimePool or adapter-owned configuration")
	}
	if strings.TrimSpace(binding.RuntimeRef.Name) == "" || binding.RuntimeRef.UID == "" || binding.RuntimeRef.Generation < 1 {
		return errors.New("external v2 execution binding has an incomplete AgentRuntime identity")
	}
	if strings.TrimSpace(external.Namespace) == "" || strings.TrimSpace(external.Endpoint) == "" ||
		strings.TrimSpace(external.RuntimeInstanceID) == "" {
		return errors.New("external v2 execution snapshot has an incomplete endpoint or runtime identity")
	}
	if err := validateAgentRuntimeEndpointSpec(external.Endpoint); err != nil {
		return fmt.Errorf("validate frozen external AgentRuntime endpoint: %w", err)
	}
	if err := external.Limits.Validate(); err != nil {
		return fmt.Errorf("validate frozen external AgentRuntime limits: %w", err)
	}
	if !external.WorkspaceGovernance.Strict() {
		return errors.New("external v2 execution snapshot does not provide strict workspace governance")
	}
	if body.RuntimeProfile.WorkspaceIntent == harnessv2.WorkspaceIntentWrite && !external.SupportsPublicationFinalization {
		return errors.New("external v2 execution snapshot does not support controller-owned publication finalization")
	}
	if err := validateExternalAgentRuntimeSnapshotSecret(external.Namespace, "controller-auth", external.ControllerAuth); err != nil {
		return err
	}
	return validateExternalAgentRuntimeSnapshotSecret(external.Namespace, "operation-capability", external.OperationCapability)
}

func validateExternalAgentRuntimeSnapshotSecret(namespace, role string, ref agentExecutionSnapshotSecretRef) error {
	if ref.Role != role || ref.Namespace != namespace || strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.UID) == "" ||
		strings.TrimSpace(ref.ResourceVersion) == "" || len(ref.Keys) != 1 || strings.TrimSpace(ref.Keys[0]) == "" {
		return fmt.Errorf("frozen external AgentRuntime %s Secret identity is incomplete", role)
	}
	return nil
}

// snapshotWorkspaceClassFromBinding freezes the class binding into its
// snapshot encoding.
func snapshotWorkspaceClassFromBinding(class *ACPWorkspaceClassBinding) *agentExecutionSnapshotWorkspaceClass {
	if class == nil {
		return nil
	}
	frozen := &agentExecutionSnapshotWorkspaceClass{
		Name:               class.Name,
		UID:                class.UID,
		Generation:         class.Generation,
		ProfileHash:        class.ProfileHash,
		ProviderName:       class.ProviderName,
		ProviderUID:        class.ProviderUID,
		ProviderGeneration: class.ProviderGeneration,
		ProviderConfigUID:  class.ProviderConfigUID,
		EffectiveOnDetach:  class.EffectiveOnDetach,
		SuspendMode:        class.SuspendMode,
		DefaultOnDetach:    class.DefaultOnDetach,
		AllowedOnDetach:    append([]string(nil), class.AllowedOnDetach...),
		DetachTimeout:      class.DetachTimeout,
		IdleTimeout:        class.IdleTimeout,
		MaxLifetime:        class.MaxLifetime,
	}
	if class.SandboxVolume != nil {
		frozen.SandboxVolume = &agentExecutionSnapshotSandboxVolume{
			StorageClassName: class.SandboxVolume.StorageClassName,
			StorageClassUID:  class.SandboxVolume.StorageClassUID,
			AccessModes:      append([]string(nil), class.SandboxVolume.AccessModes...),
			Capacity:         class.SandboxVolume.Capacity,
		}
	}
	if class.MaxSuspendedWorkspaces != nil {
		limit := *class.MaxSuspendedWorkspaces
		frozen.MaxSuspended = &limit
	}
	frozen.DeletionPolicy.ProviderResources = class.DeletionPolicy.ProviderResources
	frozen.DeletionPolicy.PersistentVolumes = class.DeletionPolicy.PersistentVolumes
	frozen.DeletionPolicy.Checkpoints = class.DeletionPolicy.Checkpoints
	return frozen
}

// workspaceClassBindingFromSnapshot rebuilds the frozen class binding from its
// snapshot encoding.
func workspaceClassBindingFromSnapshot(class *agentExecutionSnapshotWorkspaceClass) *ACPWorkspaceClassBinding {
	if class == nil {
		return nil
	}
	return &ACPWorkspaceClassBinding{
		Name:               class.Name,
		UID:                class.UID,
		Generation:         class.Generation,
		ProfileHash:        class.ProfileHash,
		ProviderName:       class.ProviderName,
		ProviderUID:        class.ProviderUID,
		ProviderGeneration: class.ProviderGeneration,
		ProviderConfigUID:  class.ProviderConfigUID,
		EffectiveOnDetach:  class.EffectiveOnDetach,
		SuspendMode:        class.SuspendMode,
		DefaultOnDetach:    class.DefaultOnDetach,
		AllowedOnDetach:    append([]string(nil), class.AllowedOnDetach...),
		DetachTimeout:      class.DetachTimeout,
		IdleTimeout:        class.IdleTimeout,
		MaxLifetime:        class.MaxLifetime,
		SandboxVolume:      sandboxVolumeFromSnapshot(class.SandboxVolume),
		MaxSuspendedWorkspaces: func() *int32 {
			if class.MaxSuspended == nil {
				return nil
			}
			limit := *class.MaxSuspended
			return &limit
		}(),
		DeletionPolicy: ACPWorkspaceClassDeletionPolicy{
			ProviderResources: class.DeletionPolicy.ProviderResources,
			PersistentVolumes: class.DeletionPolicy.PersistentVolumes,
			Checkpoints:       class.DeletionPolicy.Checkpoints,
		},
	}
}

func sandboxVolumeFromSnapshot(volume *agentExecutionSnapshotSandboxVolume) *ACPSandboxDurableVolume {
	if volume == nil {
		return nil
	}
	return &ACPSandboxDurableVolume{
		StorageClassName: volume.StorageClassName,
		StorageClassUID:  volume.StorageClassUID,
		AccessModes:      append([]string(nil), volume.AccessModes...),
		Capacity:         volume.Capacity,
	}
}

// verifiedSnapshotWorkspaceBinding re-verifies the frozen execution-workspace
// binding against its canonical digest and the immutable Task/session identity.
func verifiedSnapshotWorkspaceBinding(
	binding *corev1alpha1.AgentExecutionBinding,
	body agentExecutionSnapshotBody,
) (*ACPRuntimeWorkspaceBinding, error) {
	if body.ExecutionWorkspace == nil {
		return nil, nil
	}
	frozen := &ACPRuntimeWorkspaceBinding{
		Provider:          corev1alpha1.WorkspaceProvider(body.ExecutionWorkspace.Provider),
		ReusePolicy:       corev1alpha1.WorkspaceReusePolicy(body.ExecutionWorkspace.ReusePolicy),
		CleanupPolicy:     corev1alpha1.WorkspaceCleanupPolicy(body.ExecutionWorkspace.CleanupPolicy),
		WorkspaceSlot:     body.ExecutionWorkspace.WorkspaceSlot,
		SessionUID:        body.ExecutionWorkspace.SessionUID,
		SessionKey:        body.ExecutionWorkspace.SessionKey,
		TemplateNamespace: body.ExecutionWorkspace.TemplateNamespace,
		TemplateName:      body.ExecutionWorkspace.TemplateName,
		Class:             workspaceClassBindingFromSnapshot(body.ExecutionWorkspace.Class),
		BindingDigest:     body.ExecutionWorkspace.BindingDigest,
	}
	if err := validateSnapshotACPWorkspaceBindingValues(frozen); err != nil {
		return nil, err
	}
	wantSessionKey := "task:" + string(binding.Task.UID)
	if frozen.ReusePolicy == corev1alpha1.WorkspaceReusePolicySession {
		if body.SessionRef == nil || strings.TrimSpace(body.SessionRef.Name) == "" {
			return nil, errors.New("frozen execution workspace session reuse is missing the frozen sessionRef")
		}
		wantSessionKey = "session:" + frozen.SessionUID
	}
	if frozen.SessionKey != wantSessionKey {
		return nil, errors.New("frozen execution workspace session key does not match the immutable Task and session identity")
	}
	return frozen, nil
}

// loadVerifiedACPWorkspaceBindingForSettlement reads the encrypted execution
// snapshot without applying dispatch-only deletion or generation gates. The
// immutable binding remains the settlement authority after a Task terminates or
// begins deleting.
func (r *TaskReconciler) loadVerifiedACPWorkspaceBindingForSettlement(
	ctx context.Context,
	task *corev1alpha1.Task,
) (*ACPRuntimeWorkspaceBinding, error) {
	if task == nil {
		return nil, nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	current := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("uncached Task read before execution workspace settlement: %w", err)
	}
	if current.UID != task.UID {
		return nil, nil
	}
	binding := current.Status.AgentExecutionBinding
	if binding == nil || binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		binding.Backend != corev1alpha1.AgentExecutionBackendRuntimePool {
		return nil, nil
	}
	if binding.Task.UID != current.UID {
		return nil, errors.New("execution workspace settlement binding does not belong to the current Task UID")
	}
	canonicalDigest, err := canonicalAgentExecutionBindingDigest(*binding)
	if err != nil || canonicalDigest != binding.BindingDigest {
		return nil, errors.New("execution workspace settlement binding failed canonical integrity verification")
	}
	if r.AgentExecutionSnapshots == nil {
		return nil, errors.New("encrypted agent execution snapshot store is required for execution workspace settlement")
	}
	snapshot, err := r.AgentExecutionSnapshots.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{
		TaskUID: string(binding.Task.UID), Digest: binding.Snapshot.Digest,
	})
	if err != nil {
		return nil, fmt.Errorf("load immutable execution snapshot for workspace settlement: %w", err)
	}
	body, err := decodeAgentExecutionSnapshot(snapshot.Body)
	if err != nil {
		return nil, err
	}
	plan, _, _, err := validateAgentExecutionSnapshot(binding, snapshot, body)
	if err != nil {
		return nil, err
	}
	return plan.Workspace, nil
}

func frozenTaskFromAgentExecutionSnapshot(task *corev1alpha1.Task, binding *corev1alpha1.AgentExecutionBinding, body agentExecutionSnapshotBody) *corev1alpha1.Task {
	frozen := task.DeepCopy()
	frozen.Generation = binding.Task.BoundSpecGeneration
	frozen.Spec.Prompt = body.Prompt
	frozen.Spec.Timeout = nil
	if body.Timeout != "" {
		if duration, err := time.ParseDuration(body.Timeout); err == nil {
			frozen.Spec.Timeout = &metav1.Duration{Duration: duration}
		}
	}
	frozen.Spec.SessionRef = body.SessionRef.DeepCopy()
	frozen.Spec.Workspace = body.Workspace.DeepCopy()
	frozen.Spec.AgentRuntime = body.RuntimeOverride.DeepCopy()
	frozen.Spec.RetryPolicy = body.RetryPolicy.DeepCopy()
	return frozen
}

// loadVerifiedBoundExecution re-reads the Task, backend control, and encrypted
// snapshot uncached immediately before executor demand is persisted.
func (r *TaskReconciler) loadVerifiedBoundExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
) (*verifiedAgentExecution, error) {
	return r.loadVerifiedBoundExecutionWithReadyRequirement(ctx, task, binding, true)
}

// loadVerifiedBoundExecutionForActiveSession preserves the immutable binding
// checks for already-admitted work without requiring the runtime to keep
// accepting new sessions while it drains.
func (r *TaskReconciler) loadVerifiedBoundExecutionForActiveSession(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
) (*verifiedAgentExecution, error) {
	return r.loadVerifiedBoundExecutionWithReadyRequirement(ctx, task, binding, false)
}

//nolint:gocyclo // This keeps the existing immutable binding and snapshot verification in one boundary while varying only external admission readiness.
func (r *TaskReconciler) loadVerifiedBoundExecutionWithReadyRequirement(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
	requireExternalRuntimeReady bool,
) (*verifiedAgentExecution, error) {
	if r.AgentExecutionSnapshots == nil {
		return nil, errors.New("encrypted agent execution snapshot store is required; execution fails closed")
	}
	if task == nil || binding == nil {
		return nil, errors.New("task and execution binding are required")
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	current := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
		return nil, fmt.Errorf("uncached task read before executor side effect: %w", err)
	}
	if current.UID != task.UID {
		return nil, fmt.Errorf("task UID changed after binding; never dispatching")
	}
	if !current.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("task began deleting after binding; dispatch is cancelled")
	}
	if !requireExternalRuntimeReady && (current.Status.Execution == nil ||
		(current.Status.Execution.State != corev1alpha1.TaskExecutionStateSubmitting &&
			current.Status.Execution.State != corev1alpha1.TaskExecutionStateAccepted &&
			current.Status.Execution.State != corev1alpha1.TaskExecutionStateRunning)) {
		return nil, errors.New("active external RuntimeSession execution is unavailable")
	}
	persisted := current.Status.AgentExecutionBinding
	if persisted == nil || persisted.BindingDigest != binding.BindingDigest {
		return nil, fmt.Errorf("persisted binding does not match the verified candidate; refusing to dispatch")
	}
	canonicalDigest, err := canonicalAgentExecutionBindingDigest(*persisted)
	if err != nil || canonicalDigest != persisted.BindingDigest {
		return nil, fmt.Errorf("persisted binding failed canonical integrity verification")
	}
	if persisted.Task.UID != current.UID || persisted.Task.BoundSpecGeneration != current.Generation {
		return nil, fmt.Errorf("task UID/spec generation no longer exactly matches the immutable binding")
	}
	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: current.Namespace}, namespace); err != nil {
		return nil, fmt.Errorf("uncached namespace identity read before executor side effect: %w", err)
	}
	if namespace.UID == "" || namespace.UID != persisted.Task.NamespaceUID {
		return nil, errors.New("task namespace identity no longer exactly matches the immutable binding")
	}
	if persisted.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		(persisted.Backend != corev1alpha1.AgentExecutionBackendRuntimePool &&
			persisted.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint) {
		return nil, fmt.Errorf("binding route %s/%s is not dispatchable by the ACP executor", persisted.ContractVersion, persisted.Backend)
	}
	snapshot, err := r.AgentExecutionSnapshots.GetAgentExecutionSnapshot(ctx, store.AgentExecutionSnapshotKey{
		TaskUID: string(persisted.Task.UID), Digest: persisted.Snapshot.Digest,
	})
	if err != nil {
		return nil, fmt.Errorf("load immutable execution snapshot: %w", err)
	}
	body, err := decodeAgentExecutionSnapshot(snapshot.Body)
	if err != nil {
		return nil, err
	}
	plan, configuration, mcpConfiguration, err := validateAgentExecutionSnapshot(persisted, snapshot, body)
	if err != nil {
		return nil, err
	}
	var externalRuntime *corev1alpha1.AgentRuntime
	if persisted.Backend == corev1alpha1.AgentExecutionBackendExternalEndpoint {
		if persisted.RuntimeRef == nil || body.ExternalRuntime == nil || body.ExternalRuntime.Namespace != current.Namespace {
			return nil, errors.New("external v2 binding does not match the Task namespace")
		}
		runtime := &corev1alpha1.AgentRuntime{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: current.Namespace, Name: persisted.RuntimeRef.Name}, runtime); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, frozenExternalRuntimeBindingDrift(fmt.Errorf("load frozen external AgentRuntime: %w", err))
			}
			return nil, fmt.Errorf("load frozen external AgentRuntime: %w", err)
		}
		if runtime.Name != persisted.RuntimeRef.Name || runtime.UID != persisted.RuntimeRef.UID ||
			runtime.Generation != persisted.RuntimeRef.Generation {
			return nil, frozenExternalRuntimeBindingDrift(errors.New("external AgentRuntime identity or generation changed after binding"))
		}
		observed := runtime.Status.ObservedCapabilities
		if observed == nil || observed.MCPToolDescriptorDigest != mcpConfiguration.ToolPolicy.DescriptorDigest {
			return nil, frozenExternalRuntimeBindingDrift(errExternalAgentRuntimeMCPToolDescriptorsNotConformed)
		}
		reconciler := &AgentRuntimeReconciler{Client: r.Client, APIReader: r.APIReader}
		currentAuth, err := reconciler.agentRuntimeAuthMaterial(ctx, runtime)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, frozenExternalRuntimeBindingDrift(errors.New("external AgentRuntime authentication authority changed after binding"))
			}
			return nil, fmt.Errorf("revalidate frozen external AgentRuntime authentication authority: %w", err)
		}
		if string(currentAuth.controllerSecretUID) != body.ExternalRuntime.ControllerAuth.UID ||
			currentAuth.controllerResourceVersion != body.ExternalRuntime.ControllerAuth.ResourceVersion ||
			string(currentAuth.capabilitySecretUID) != body.ExternalRuntime.OperationCapability.UID ||
			currentAuth.capabilityResourceVersion != body.ExternalRuntime.OperationCapability.ResourceVersion {
			return nil, frozenExternalRuntimeBindingDrift(errors.New("external AgentRuntime authentication authority changed after binding"))
		}
		currentProfile, currentExternal, err := r.resolveExternalAgentRuntimeSnapshotWithReadyRequirement(
			ctx, current, runtime, requireExternalRuntimeReady,
		)
		if err != nil {
			return nil, fmt.Errorf("revalidate frozen external AgentRuntime: %w", err)
		}
		currentProfileDigest, err := harnessv2.CanonicalProfileDigest(currentProfile)
		if err != nil || currentProfileDigest != plan.Digest {
			return nil, frozenExternalRuntimeBindingDrift(errors.New("external AgentRuntime profile changed after binding"))
		}
		frozenAuthority, err := harnessv2.CanonicalValue(body.ExternalRuntime)
		if err != nil {
			return nil, fmt.Errorf("canonicalize frozen external AgentRuntime authority: %w", err)
		}
		currentAuthority, err := harnessv2.CanonicalValue(currentExternal)
		if err != nil || !bytes.Equal(frozenAuthority, currentAuthority) {
			return nil, frozenExternalRuntimeBindingDrift(errors.New("external AgentRuntime endpoint, capabilities, or authentication authority changed after binding"))
		}
		externalRuntime = runtime.DeepCopy()
	}
	return &verifiedAgentExecution{
		binding: persisted.DeepCopy(), snapshot: snapshot, body: body, plan: plan,
		externalRuntime: externalRuntime,
		frozenTask:      frozenTaskFromAgentExecutionSnapshot(current, persisted, body), configuration: configuration,
		mcpConfiguration: mcpConfiguration,
	}, nil
}

// verifyBoundExecution preserves the narrow verification API used by binding
// reconciliation while sharing the exact queue-time verification path.
func (r *TaskReconciler) verifyBoundExecution(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
) error {
	_, err := r.loadVerifiedBoundExecution(ctx, task, binding)
	return err
}

func (r *TaskReconciler) handleAgentExecutionBindingVerificationFailure(
	ctx context.Context,
	task *corev1alpha1.Task,
	err error,
	logMessage string,
) (ctrl.Result, error) {
	if task.Status.Execution == nil && isFrozenExternalRuntimeBindingDrift(err) {
		return r.failACPPlanningTask(
			ctx, task, corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"), err.Error(),
		)
	}
	logf.FromContext(ctx).Error(err, logMessage)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// ensureAgentExecutionBinding runs the binding stage for the ACP execution
// path. It returns handled=true when the caller must return the result
// immediately (failure or requeue) and handled=false when dispatch may
// proceed under a verified binding.
func (r *TaskReconciler) ensureAgentExecutionBinding(
	ctx context.Context,
	task *corev1alpha1.Task,
	agent *corev1alpha1.Agent,
) (ctrl.Result, error, bool) {
	log := logf.FromContext(ctx)
	if task == nil {
		return ctrl.Result{}, errors.New("task is required for execution binding"), true
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if task.Status.AgentExecutionBinding == nil {
		current := &corev1alpha1.Task{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, current); err != nil {
			log.Error(err, "uncached Task read before execution binding")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil, true
		}
		if current.UID != task.UID {
			log.Error(errors.New("task UID changed"), "execution binding withheld")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
		}
		if current.Status.AgentExecutionBinding != nil {
			task = current
		}
	}
	if existing := task.Status.AgentExecutionBinding; existing != nil {
		if err := r.verifyBoundExecution(ctx, task, existing); err != nil {
			result, handleErr := r.handleAgentExecutionBindingVerificationFailure(
				ctx, task, err, "bound execution verification failed",
			)
			return result, handleErr, true
		}
		return ctrl.Result{}, nil, false
	}

	candidate, err := r.resolveAgentExecutionCandidate(ctx, task, agent)
	if err != nil {
		if isPermanentACPAgentConfigurationError(err) {
			result, failErr := r.failACPPlanningTask(
				ctx, task, corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"), err.Error(),
			)
			return result, failErr, true
		}
		log.Error(err, "agent execution candidate resolution failed")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
	}
	if err := r.persistAgentExecutionSnapshot(ctx, task, candidate); err != nil {
		log.Error(err, "immutable execution snapshot persistence failed")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
	}
	if candidate.workspaceSessionUID != "" {
		sessionUID, sessionErr := r.ensureACPWorkspaceSessionUID(ctx, task, candidate.workspaceSessionUID)
		if sessionErr != nil {
			log.Error(sessionErr, "execution-workspace Session identity establishment failed")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
		}
		if sessionUID != candidate.workspaceSessionUID {
			candidate, err = r.resolveAgentExecutionCandidateWithWorkspaceSessionUID(ctx, task, agent, sessionUID)
			if err != nil {
				if isPermanentACPAgentConfigurationError(err) {
					result, failErr := r.failACPPlanningTask(
						ctx, task, corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile"), err.Error(),
					)
					return result, failErr, true
				}
				log.Error(err, "agent execution candidate rebuild after concurrent Session creation failed")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
			}
			if err := r.persistAgentExecutionSnapshot(ctx, task, candidate); err != nil {
				log.Error(err, "immutable execution snapshot persistence failed after concurrent Session creation")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
			}
		}
	}
	binding, err := r.persistAgentExecutionBinding(ctx, task, candidate)
	if err != nil {
		conflict := &errAgentExecutionBindingConflict{}
		if errors.As(err, &conflict) {
			if r.Recorder != nil {
				r.Recorder.Eventf(task, corev1.EventTypeWarning, agentExecutionBindingConflictReason, "%s", err.Error())
			}
			result, failErr := r.failTask(ctx, task, err.Error())
			return result, failErr, true
		}
		log.Error(err, "write-once binding persistence failed")
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil, true
	}
	task.Status.AgentExecutionBinding = binding
	if err := r.verifyBoundExecution(ctx, task, binding); err != nil {
		result, handleErr := r.handleAgentExecutionBindingVerificationFailure(
			ctx, task, err, "bound execution verification failed after binding",
		)
		return result, handleErr, true
	}
	return ctrl.Result{}, nil, false
}
