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
)

// The binding stage implements the coexistence plan's write-once execution
// binding: every executable agent Task freezes its protocol, backend, and an
// immutable content-addressed execution snapshot before any executor-specific
// side effect. The snapshot store and revisioned backend control are mandatory:
// a controller that cannot prove either one never queues executor demand.

const agentExecutionBindingConflictReason = "BindingConflict"

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
	SchemaVersion    int32                             `json:"schemaVersion"`
	ContractVersion  string                            `json:"contractVersion"`
	Backend          string                            `json:"backend"`
	RuntimeType      string                            `json:"runtimeType"`
	Agent            agentExecutionSnapshotAgent       `json:"agent"`
	Configuration    agentExecutionSnapshotConfig      `json:"configuration"`
	RuntimeImage     string                            `json:"runtimeImage"`
	RuntimeProfile   harnessv2.RuntimeProfile          `json:"runtimeProfile"`
	ProfileDigest    string                            `json:"profileDigest"`
	PoolName         string                            `json:"poolName"`
	MCPConfiguration *harnessv2.MCPPolicyConfiguration `json:"mcpConfiguration,omitempty"`
	Prompt           string                            `json:"prompt"`
	Timeout          string                            `json:"timeout,omitempty"`
	RetryPolicy      *corev1alpha1.RetryPolicy         `json:"retryPolicy,omitempty"`
	SessionRef       *corev1alpha1.SessionReference    `json:"sessionRef,omitempty"`
	Workspace        *corev1alpha1.WorkspaceConfig     `json:"workspace,omitempty"`
	RuntimeOverride  *corev1alpha1.AgentRuntimeSpec    `json:"runtimeOverride,omitempty"`
	DefaultTools     *agentExecutionSnapshotToolPolicy `json:"defaultTools,omitempty"`
	HarnessV1        *agentExecutionSnapshotHarnessV1  `json:"harnessV1,omitempty"`
	// ExecutionWorkspace freezes the resolved execution-workspace binding for
	// workspace-provider-backed RuntimePools. It is absent for plain pools.
	ExecutionWorkspace *agentExecutionSnapshotWorkspaceBinding `json:"executionWorkspace,omitempty"`
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
	if body.Timeout != "" {
		if _, err := time.ParseDuration(body.Timeout); err != nil {
			return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen Task timeout: %w", err)
		}
	}
	if err := body.RuntimeProfile.Validate(); err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen RuntimeProfile: %w", err)
	}
	if err := configuration.ValidateProfile(body.RuntimeProfile); err != nil {
		return ACPRuntimePlan{}, harnessv2.AgentSessionConfiguration{}, harnessv2.MCPPolicyConfiguration{}, fmt.Errorf("validate frozen Agent configuration: %w", err)
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
	if persisted.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 || persisted.Backend != corev1alpha1.AgentExecutionBackendRuntimePool {
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
	return &verifiedAgentExecution{
		binding: persisted.DeepCopy(), snapshot: snapshot, body: body, plan: plan,
		frozenTask: frozenTaskFromAgentExecutionSnapshot(current, persisted, body), configuration: configuration,
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
			log.Error(err, "bound execution verification failed")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
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
		log.Error(err, "bound execution verification failed after binding")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil, true
	}
	return ctrl.Result{}, nil, false
}
