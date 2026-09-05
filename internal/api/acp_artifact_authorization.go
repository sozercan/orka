package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

const acpArtifactAuthorizationPath = "/internal/v2/acp/artifact-authorizations"

var (
	errPublisherArtifactAuthorizationUnavailable = errors.New("publisher artifact authorization authority is unavailable")
	errPublisherAuthorizationObjectNotFound      = errors.New("publisher authorization object not found")
)

type acpArtifactAuthorizationRequest struct {
	Namespace string                      `json:"namespace"`
	Metadata  harnessv2.MutationMetadata  `json:"metadata"`
	Artifact  harnessv2.ArtifactReference `json:"artifact"`
}

type acpArtifactAuthorizationResponse struct {
	Capability    string `json:"capability"`
	RequestDigest string `json:"requestDigest"`
}

type acpArtifactRuntimeProvider struct {
	pool                      *corev1alpha1.RuntimePool
	external                  *corev1alpha1.AgentRuntime
	externalAuthority         *acpExternalArtifactRuntimeAuthority
	controllerBearer          string
	operationCapabilitySecret []byte
}

type acpExternalArtifactRuntimeAuthority struct {
	runtime                   *corev1alpha1.AgentRuntime
	controllerSecret          *corev1.Secret
	capabilitySecret          *corev1.Secret
	controllerBearer          string
	operationCapabilitySecret []byte
	controllerFence           store.ControllerEpochFence
}

type acpArtifactExecutionSnapshot struct {
	SchemaVersion   int32                                        `json:"schemaVersion"`
	ContractVersion string                                       `json:"contractVersion"`
	Backend         string                                       `json:"backend"`
	ProfileDigest   string                                       `json:"profileDigest"`
	ExternalRuntime *acpArtifactExternalRuntimeExecutionSnapshot `json:"externalRuntime,omitempty"`
}

type acpArtifactExternalRuntimeExecutionSnapshot struct {
	Namespace                       string                                `json:"namespace"`
	Endpoint                        string                                `json:"endpoint"`
	RuntimeInstanceID               string                                `json:"runtimeInstanceID"`
	Limits                          harnessv2.ProtocolLimits              `json:"limits"`
	SupportsPublicationFinalization bool                                  `json:"supportsPublicationFinalization"`
	ControllerAuth                  acpArtifactExecutionSnapshotSecretRef `json:"controllerAuth"`
	OperationCapability             acpArtifactExecutionSnapshotSecretRef `json:"operationCapability"`
}

type acpArtifactExecutionSnapshotSecretRef struct {
	Role            string   `json:"role"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	UID             string   `json:"uid"`
	ResourceVersion string   `json:"resourceVersion"`
	Keys            []string `json:"keys"`
}

func (s *Server) installACPArtifactAuthorizationBroker() {
	s.app.Post(acpArtifactAuthorizationPath, s.issueACPArtifactAuthorization)
	s.app.Post(publisherservice.ArtifactAuthorizationBrokerPath, s.issuePublisherArtifactAuthorization)
	s.app.Post(publisherservice.CredentialBrokerPath, s.issuePublisherCredential)
}

func (s *Server) issueACPArtifactAuthorization(c fiber.Ctx) error {
	// Authenticate on the provider bearer resolved from the non-secret pool
	// namespace/UID headers before consuming the body: runtimes can reach
	// this endpoint, so an unauthenticated peer must be rejected without being
	// allowed to stream — or drip-feed a declared-length — request body and
	// occupy controller handlers.
	poolNamespace := strings.TrimSpace(string(c.Request().Header.Peek(harnessv2.MCPBrokerPoolNamespaceHeader)))
	poolUID := strings.TrimSpace(string(c.Request().Header.Peek(harnessv2.MCPBrokerPoolUIDHeader)))
	if poolNamespace == "" || poolUID == "" {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	provider, err := s.resolveArtifactRuntimeProviderByIdentity(c.Context(), poolNamespace, poolUID)
	if err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	bearer := strings.TrimSpace(strings.TrimPrefix(string(c.Request().Header.Peek("Authorization")), "Bearer "))
	if !constantAPIStringEqual(bearer, provider.controllerBearer) {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	var request acpArtifactAuthorizationRequest
	if err := json.Unmarshal(c.Body(), &request); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid_request"})
	}
	// The body's pool identity must match the pre-authenticated headers so a
	// valid bearer for one pool cannot authorize an artifact for another.
	if request.Namespace != poolNamespace || string(request.Metadata.Fence.RuntimePoolUID) != poolUID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	now := time.Now().UTC()
	if request.Namespace == "" || request.Metadata.PromptID == "" || request.Metadata.TaskUID == "" ||
		request.Artifact.MediaType != artifactcap.MediaTypeWorkspaceDelta || request.Artifact.Validate() != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid_request"})
	}
	if got, err := harnessv2.CanonicalRequestDigest(request); err != nil || got != request.Metadata.RequestDigest {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid_request"})
	}
	capabilityHeader := string(c.Request().Header.Peek(harnessv2.OperationCapabilityHeader))
	if provider.pool != nil {
		// Verify with a fresh timestamp: provider resolution above performs
		// Kubernetes I/O, and a capability that expired while it ran must not be
		// accepted against the stale pre-resolution clock.
		if err := harnessv2.VerifyOperationCapability(provider.operationCapabilitySecret, capabilityHeader, request.Metadata, true, time.Now().UTC()); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
		}
		if provider.pool.Status.ActiveInstance == nil || provider.pool.Status.ActiveInstance.RuntimeInstanceID != string(request.Metadata.Fence.RuntimeInstanceID) {
			return c.Status(fiber.StatusGone).JSON(fiber.Map{"error": "stale_runtime"})
		}
		task, taskErr := findTaskByUIDWithReader(c.Context(), s.authorizationReader(), request.Namespace, string(request.Metadata.TaskUID))
		if taskErr != nil || task.Status.Execution == nil || task.Status.Execution.PromptID != string(request.Metadata.PromptID) ||
			task.Status.Execution.RuntimeSessionUID != string(request.Metadata.Fence.RuntimeSessionUID) ||
			task.Status.Execution.RuntimeInstanceID != string(request.Metadata.Fence.RuntimeInstanceID) ||
			(task.Status.Execution.State != corev1alpha1.TaskExecutionStateRunning && task.Status.Execution.State != corev1alpha1.TaskExecutionStateSettling) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
		}
	} else if err := s.authorizeExternalACPArtifactRequest(c.Context(), provider, request, bearer, capabilityHeader); err != nil {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	artifactSecret, err := readACPArtifactCapabilitySecret()
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	binding := artifactcap.OperationRequest{
		Operation: artifactcap.OperationUpload, ObjectDigest: request.Artifact.Digest,
		Identity:      artifactcap.Identity{Namespace: request.Namespace, TaskID: string(request.Metadata.TaskUID)},
		ContentLength: request.Artifact.SizeBytes, MediaType: request.Artifact.MediaType,
		OperationID: "runtime-delta-upload-" + string(request.Metadata.OperationID),
	}
	const capabilityTTL = 2 * time.Minute
	authorization, err := artifactcap.Issue(artifactSecret, binding, now, capabilityTTL)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	if s.config.ArtifactReservations == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	if err := s.config.ArtifactReservations.Reserve(c.Context(), binding, now.Add(capabilityTTL+artifactcap.MaxClockSkew)); err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	return c.JSON(acpArtifactAuthorizationResponse{Capability: authorization.Capability, RequestDigest: authorization.RequestDigest})
}

const (
	runtimePoolControllerTokenKeyAPI   = "controller-token"
	runtimePoolCapabilitySecretKeyAPI  = "capability-secret"
	runtimePoolAuthLabelAPI            = "orka.ai/runtime-pool-auth"
	runtimePoolAuthLabelValueAPI       = "true"
	runtimePoolUIDLabelAPI             = "orka.ai/runtime-pool-uid"
	runtimePoolCredentialEpochLabelAPI = "orka.ai/runtime-pool-controller-epoch"
	runtimePoolPrivateAuthBindingAPI   = "orka.ai/private-auth-secret-e"
)

func (s *Server) authorizationReader() client.Reader {
	if s.config.APIReader != nil {
		return s.config.APIReader
	}
	return s.client
}

func (s *Server) resolveArtifactRuntimeProviderByIdentity(
	ctx context.Context,
	namespace string,
	poolUID string,
) (*acpArtifactRuntimeProvider, error) {
	reader := s.authorizationReader()
	if reader == nil {
		return nil, fmt.Errorf("artifact runtime provider reader is unavailable")
	}
	pool, err := findArtifactRuntimePoolByIdentity(ctx, reader, namespace, poolUID)
	if err != nil {
		return nil, err
	}
	external, err := findArtifactExternalRuntimeByIdentity(ctx, reader, namespace, poolUID)
	if err != nil {
		return nil, err
	}
	if (pool == nil) == (external == nil) {
		return nil, fmt.Errorf("artifact runtime provider identity is missing or ambiguous")
	}
	if pool != nil {
		secret, err := resolveArtifactRuntimePoolAuthSecret(ctx, reader, pool)
		if err != nil {
			return nil, err
		}
		return &acpArtifactRuntimeProvider{
			pool:                      pool,
			controllerBearer:          strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKeyAPI])),
			operationCapabilitySecret: append([]byte(nil), secret.Data[runtimePoolCapabilitySecretKeyAPI]...),
		}, nil
	}
	authority, err := s.resolveExternalArtifactRuntimeAuthority(ctx, external, poolUID)
	if err != nil {
		return nil, err
	}
	return &acpArtifactRuntimeProvider{
		external:                  external,
		externalAuthority:         authority,
		controllerBearer:          authority.controllerBearer,
		operationCapabilitySecret: append([]byte(nil), authority.operationCapabilitySecret...),
	}, nil
}

func findArtifactRuntimePoolByIdentity(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	poolUID string,
) (*corev1alpha1.RuntimePool, error) {
	var pools corev1alpha1.RuntimePoolList
	if err := reader.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var pool *corev1alpha1.RuntimePool
	for i := range pools.Items {
		candidate := &pools.Items[i]
		if string(candidate.UID) != poolUID {
			continue
		}
		if pool != nil {
			return nil, fmt.Errorf("runtime pool UID is ambiguous")
		}
		pool = candidate.DeepCopy()
	}
	return pool, nil
}

func findArtifactExternalRuntimeByIdentity(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	poolUID string,
) (*corev1alpha1.AgentRuntime, error) {
	var runtimes corev1alpha1.AgentRuntimeList
	if err := reader.List(ctx, &runtimes, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var runtime *corev1alpha1.AgentRuntime
	for i := range runtimes.Items {
		candidate := &runtimes.Items[i]
		observed := candidate.Status.ObservedCapabilities
		if observed == nil || observed.RuntimePoolUID != poolUID {
			continue
		}
		if runtime != nil {
			return nil, fmt.Errorf("external AgentRuntime pool UID is ambiguous")
		}
		runtime = candidate.DeepCopy()
	}
	return runtime, nil
}

func (s *Server) resolveExternalArtifactRuntimeAuthority(
	ctx context.Context,
	runtime *corev1alpha1.AgentRuntime,
	poolUID string,
) (*acpExternalArtifactRuntimeAuthority, error) {
	if err := validateExternalArtifactRuntimeAuthority(runtime); err != nil {
		return nil, err
	}
	if s.config.ControllerEpochs == nil {
		return nil, fmt.Errorf("controller epoch authority is unavailable")
	}
	controllerFence, err := s.config.ControllerEpochs.CurrentFence(ctx)
	if err != nil {
		return nil, fmt.Errorf("read current controller epoch: %w", err)
	}
	if controllerFence.Epoch < 1 {
		return nil, fmt.Errorf("current controller epoch is invalid")
	}
	if err := validateExternalArtifactRuntimeObservedIdentity(runtime, controllerFence, poolUID); err != nil {
		return nil, err
	}
	controllerRef, capabilityRef, err := externalArtifactRuntimeSecretReferences(runtime)
	if err != nil {
		return nil, err
	}
	reader := s.authorizationReader()
	controllerSecret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: runtime.Namespace, Name: controllerRef.Name}, controllerSecret); err != nil {
		return nil, fmt.Errorf("read external AgentRuntime controller bearer Secret: %w", err)
	}
	capabilitySecret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: runtime.Namespace, Name: capabilityRef.Name}, capabilitySecret); err != nil {
		return nil, fmt.Errorf("read external AgentRuntime operation capability Secret: %w", err)
	}
	controllerToken, capabilityKey, err := validateExternalArtifactRuntimeSecretMaterial(
		runtime, controllerSecret, capabilitySecret, controllerRef, capabilityRef,
	)
	if err != nil {
		return nil, err
	}
	return &acpExternalArtifactRuntimeAuthority{
		runtime: runtime.DeepCopy(), controllerSecret: controllerSecret.DeepCopy(), capabilitySecret: capabilitySecret.DeepCopy(),
		controllerBearer: string(controllerToken), operationCapabilitySecret: append([]byte(nil), capabilityKey...),
		controllerFence: controllerFence,
	}, nil
}

func validateExternalArtifactRuntimeAuthority(runtime *corev1alpha1.AgentRuntime) error {
	if runtime == nil || runtime.UID == "" || runtime.Generation < 1 ||
		runtime.RegisteredContractVersion() != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		runtime.Status.ObservedGeneration != runtime.Generation ||
		runtime.Spec.Capabilities == nil || runtime.Spec.Capabilities.Profile == nil ||
		runtime.Spec.Capabilities.WorkspaceGovernance == nil ||
		!runtime.Spec.Capabilities.WorkspaceGovernance.Strict() ||
		runtime.Spec.Capabilities.Profile.WorkspaceIntent != corev1alpha1.WorkspaceIntentWrite ||
		!runtime.Spec.Capabilities.SupportsPublicationFinalization ||
		runtime.Status.ObservedCapabilities == nil {
		return fmt.Errorf("external AgentRuntime authority is incomplete for artifact authorization")
	}
	return nil
}

func validateExternalArtifactRuntimeObservedIdentity(
	runtime *corev1alpha1.AgentRuntime,
	controllerFence store.ControllerEpochFence,
	poolUID string,
) error {
	registered := runtime.Spec.Capabilities
	observed := runtime.Status.ObservedCapabilities
	if observed.ProtocolVersion != harnessv2.ProtocolVersion ||
		observed.ControllerEpoch != controllerFence.Epoch || observed.RuntimePoolUID != poolUID ||
		observed.RuntimePoolGeneration < 1 || strings.TrimSpace(observed.SupervisorBootID) == "" ||
		observed.RuntimeInstanceID != registered.RuntimeInstanceID ||
		observed.RuntimeProfileDigest != registered.Profile.Digest ||
		observed.ProfileDigestSchemaVersion != registered.Profile.DigestSchemaVersion ||
		registered.Profile.DigestSchemaVersion != int32(harnessv2.ProfileDigestSchemaVersion) ||
		observed.SupportsPublicationFinalization != registered.SupportsPublicationFinalization {
		return fmt.Errorf("external AgentRuntime artifact authorization identity is stale")
	}
	return nil
}

func externalArtifactRuntimeSecretReferences(
	runtime *corev1alpha1.AgentRuntime,
) (*corev1alpha1.AgentRuntimeSecretKeyReference, *corev1alpha1.AgentRuntimeSecretKeyReference, error) {
	controllerRef := runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	capabilityRef := runtime.Spec.ClientAuth.OperationCapabilitySecretRef
	if controllerRef == nil || capabilityRef == nil || strings.TrimSpace(controllerRef.Name) == "" ||
		strings.TrimSpace(controllerRef.Key) == "" || strings.TrimSpace(capabilityRef.Name) == "" ||
		strings.TrimSpace(capabilityRef.Key) == "" || *controllerRef == *capabilityRef {
		return nil, nil, fmt.Errorf("external AgentRuntime artifact authorization references are invalid")
	}
	return controllerRef, capabilityRef, nil
}

func validateExternalArtifactRuntimeSecretMaterial(
	runtime *corev1alpha1.AgentRuntime,
	controllerSecret *corev1.Secret,
	capabilitySecret *corev1.Secret,
	controllerRef *corev1alpha1.AgentRuntimeSecretKeyReference,
	capabilityRef *corev1alpha1.AgentRuntimeSecretKeyReference,
) ([]byte, []byte, error) {
	controllerToken := controllerSecret.Data[controllerRef.Key]
	capabilityKey := capabilitySecret.Data[capabilityRef.Key]
	if controllerSecret.UID == "" || capabilitySecret.UID == "" ||
		strings.TrimSpace(controllerSecret.ResourceVersion) == "" || strings.TrimSpace(capabilitySecret.ResourceVersion) == "" ||
		controllerSecret.ResourceVersion != runtime.Status.ObservedControllerAuthRefResourceVersion ||
		capabilitySecret.ResourceVersion != runtime.Status.ObservedOperationCapabilityRefResourceVersion ||
		len(controllerToken) < 32 || len(capabilityKey) < harnessv2.MinCapabilitySecretBytes ||
		!bytes.Equal(controllerToken, bytes.TrimSpace(controllerToken)) ||
		!bytes.Equal(capabilityKey, bytes.TrimSpace(capabilityKey)) || bytes.Equal(controllerToken, capabilityKey) {
		return nil, nil, fmt.Errorf("external AgentRuntime artifact authorization material is invalid or stale")
	}
	return controllerToken, capabilityKey, nil
}

func (s *Server) authorizeExternalACPArtifactRequest(
	ctx context.Context,
	provider *acpArtifactRuntimeProvider,
	request acpArtifactAuthorizationRequest,
	presentedBearer string,
	capabilityHeader string,
) error {
	if err := validateExternalArtifactRuntimeProvider(provider); err != nil {
		return err
	}
	task, execution, binding, err := s.loadExternalArtifactTaskAuthority(ctx, request)
	if err != nil {
		return err
	}

	// Re-resolve after consuming the body. This closes races where the
	// registration, its conformed Secret versions, or a colliding provider
	// appears after preauthentication.
	currentProvider, err := s.resolveArtifactRuntimeProviderByIdentity(
		ctx, request.Namespace, string(request.Metadata.Fence.RuntimePoolUID),
	)
	if err != nil || currentProvider.external == nil || currentProvider.externalAuthority == nil || currentProvider.pool != nil {
		return fmt.Errorf("external artifact runtime authority changed after preauthentication")
	}
	current := currentProvider.external
	authority := currentProvider.externalAuthority
	if err := validateExternalArtifactRuntimeContinuity(provider, current, execution, presentedBearer, authority); err != nil {
		return err
	}
	if err := validateExternalArtifactTaskBinding(task, binding, current); err != nil {
		return err
	}
	if err := validateExternalArtifactRuntimeFence(execution, binding, authority, request.Metadata.Fence); err != nil {
		return err
	}
	if err := s.validateFrozenExternalArtifactRuntimeAuthority(
		ctx, task, binding, authority, request.Artifact.SizeBytes,
	); err != nil {
		return err
	}
	// Verify last, against the freshly resolved frozen capability Secret. Any
	// expiry or Secret rotation during the preceding reads fails closed.
	if err := harnessv2.VerifyOperationCapability(
		authority.operationCapabilitySecret, capabilityHeader, request.Metadata, true, time.Now().UTC(),
	); err != nil {
		return fmt.Errorf("external artifact operation capability is invalid")
	}
	return nil
}

func validateExternalArtifactRuntimeProvider(provider *acpArtifactRuntimeProvider) error {
	if provider == nil || provider.external == nil || provider.externalAuthority == nil || provider.pool != nil {
		return fmt.Errorf("external artifact runtime provider is incomplete")
	}
	return nil
}

func (s *Server) loadExternalArtifactTaskAuthority(
	ctx context.Context,
	request acpArtifactAuthorizationRequest,
) (*corev1alpha1.Task, *corev1alpha1.TaskExecutionStatus, *corev1alpha1.AgentExecutionBinding, error) {
	task, err := findTaskByUIDWithReader(ctx, s.authorizationReader(), request.Namespace, string(request.Metadata.TaskUID))
	if err != nil || task.Status.Execution == nil || task.Status.AgentExecutionBinding == nil {
		return nil, nil, nil, fmt.Errorf("external artifact Task authority is unavailable")
	}
	execution := task.Status.Execution
	if execution.State != corev1alpha1.TaskExecutionStateRunning && execution.State != corev1alpha1.TaskExecutionStateSettling {
		return nil, nil, nil, fmt.Errorf("external artifact Task is not active")
	}
	if err := validateExternalArtifactTaskIdentity(execution, request); err != nil {
		return nil, nil, nil, err
	}
	return task, execution, task.Status.AgentExecutionBinding, nil
}

func validateExternalArtifactTaskIdentity(
	execution *corev1alpha1.TaskExecutionStatus,
	request acpArtifactAuthorizationRequest,
) error {
	if execution.Attempt < 1 || uint32(execution.Attempt) != request.Metadata.TaskAttempt ||
		execution.PromptID != string(request.Metadata.PromptID) ||
		execution.RuntimeSessionUID != string(request.Metadata.Fence.RuntimeSessionUID) ||
		execution.RuntimeSessionGeneration < 1 ||
		uint64(execution.RuntimeSessionGeneration) != request.Metadata.Fence.RuntimeSessionGeneration ||
		execution.RuntimeInstanceID != string(request.Metadata.Fence.RuntimeInstanceID) ||
		execution.ControllerEpoch < 1 || uint64(execution.ControllerEpoch) != request.Metadata.Fence.ControllerEpoch ||
		execution.RuntimePoolName != "" || execution.RuntimePoolUID != "" ||
		execution.AgentRuntimeName == "" || execution.AgentRuntimeUID == "" {
		return fmt.Errorf("external artifact Task identity does not match the request")
	}
	return nil
}

func validateExternalArtifactRuntimeContinuity(
	provider *acpArtifactRuntimeProvider,
	current *corev1alpha1.AgentRuntime,
	execution *corev1alpha1.TaskExecutionStatus,
	presentedBearer string,
	authority *acpExternalArtifactRuntimeAuthority,
) error {
	if current.Name != provider.external.Name || current.UID != provider.external.UID || current.Generation != provider.external.Generation ||
		execution.AgentRuntimeName != current.Name || execution.AgentRuntimeUID != string(current.UID) ||
		!constantAPIStringEqual(presentedBearer, authority.controllerBearer) {
		return fmt.Errorf("external artifact runtime identity changed after preauthentication")
	}
	return nil
}

func validateExternalArtifactTaskBinding(
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
	runtime *corev1alpha1.AgentRuntime,
) error {
	if binding.SchemaVersion != 1 || binding.ContractVersion != corev1alpha1.AgentRuntimeContractHarnessV2 ||
		binding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint || binding.RuntimeType != "" ||
		binding.RuntimeRef == nil || binding.RuntimeRef.Name != runtime.Name || binding.RuntimeRef.UID != runtime.UID ||
		binding.RuntimeRef.Generation != runtime.Generation || binding.Task.UID != task.UID ||
		binding.Task.BoundSpecGeneration != task.Generation || binding.RuntimeProfileDigest == "" ||
		binding.RuntimeProfileDigest != runtime.Spec.Capabilities.Profile.Digest ||
		binding.RuntimeProfileDigestSchemaVersion != int32(harnessv2.ProfileDigestSchemaVersion) {
		return fmt.Errorf("external artifact runtime does not match the immutable Task binding")
	}
	return nil
}

func validateExternalArtifactRuntimeFence(
	execution *corev1alpha1.TaskExecutionStatus,
	binding *corev1alpha1.AgentExecutionBinding,
	authority *acpExternalArtifactRuntimeAuthority,
	requestFence harnessv2.Fence,
) error {
	observed := authority.runtime.Status.ObservedCapabilities
	expectedFence := harnessv2.Fence{
		RuntimeInstanceID:          harnessv2.RuntimeInstanceID(observed.RuntimeInstanceID),
		SupervisorBootID:           harnessv2.SupervisorBootID(observed.SupervisorBootID),
		ControllerEpoch:            uint64(authority.controllerFence.Epoch),
		RuntimePoolUID:             harnessv2.RuntimePoolUID(observed.RuntimePoolUID),
		RuntimePoolGeneration:      uint64(observed.RuntimePoolGeneration),
		RuntimeSessionUID:          harnessv2.RuntimeSessionUID(execution.RuntimeSessionUID),
		RuntimeSessionGeneration:   uint64(execution.RuntimeSessionGeneration),
		RuntimeProfileDigest:       harnessv2.ProfileDigest(binding.RuntimeProfileDigest),
		ProfileDigestSchemaVersion: harnessv2.ProfileDigestSchemaVersion,
	}
	if err := expectedFence.Validate(true); err != nil ||
		harnessv2.CompareFence(expectedFence, requestFence, true) != harnessv2.FenceMatch {
		return fmt.Errorf("external artifact runtime fence is stale")
	}
	if execution.RuntimeSessionSupervisorBootID != "" && execution.RuntimeSessionSupervisorBootID != observed.SupervisorBootID {
		return fmt.Errorf("external artifact runtime supervisor boot does not match the Task")
	}
	if execution.RuntimeSessionProfileDigest != "" && execution.RuntimeSessionProfileDigest != binding.RuntimeProfileDigest {
		return fmt.Errorf("external artifact runtime profile does not match the Task")
	}
	return nil
}

func (s *Server) validateFrozenExternalArtifactRuntimeAuthority(
	ctx context.Context,
	task *corev1alpha1.Task,
	binding *corev1alpha1.AgentExecutionBinding,
	authority *acpExternalArtifactRuntimeAuthority,
	artifactSizeBytes int64,
) error {
	if s.config.AgentExecutionSnapshots == nil || task == nil || binding == nil || authority == nil || authority.runtime == nil {
		return fmt.Errorf("encrypted execution snapshot authority is unavailable")
	}
	key := store.AgentExecutionSnapshotKey{TaskUID: string(task.UID), Digest: binding.Snapshot.Digest}
	if err := key.Validate(); err != nil || binding.Snapshot.ID != key.ID() ||
		binding.Snapshot.SchemaVersion != store.AgentExecutionSnapshotSchemaVersion {
		return fmt.Errorf("external artifact execution snapshot reference is invalid")
	}
	snapshot, err := s.config.AgentExecutionSnapshots.GetAgentExecutionSnapshot(ctx, key)
	if err != nil || snapshot == nil || snapshot.TaskUID != key.TaskUID || snapshot.Digest != key.Digest ||
		snapshot.SchemaVersion != binding.Snapshot.SchemaVersion ||
		store.CanonicalAgentExecutionSnapshotDigest(snapshot.Body) != binding.Snapshot.Digest {
		return fmt.Errorf("external artifact execution snapshot is unavailable or corrupt")
	}
	var frozen acpArtifactExecutionSnapshot
	if err := json.Unmarshal(snapshot.Body, &frozen); err != nil || frozen.ExternalRuntime == nil {
		return fmt.Errorf("external artifact execution snapshot is invalid")
	}
	runtime := authority.runtime
	external := frozen.ExternalRuntime
	if frozen.SchemaVersion != store.AgentExecutionSnapshotSchemaVersion ||
		frozen.ContractVersion != string(corev1alpha1.AgentRuntimeContractHarnessV2) ||
		frozen.Backend != string(corev1alpha1.AgentExecutionBackendExternalEndpoint) ||
		frozen.ProfileDigest != binding.RuntimeProfileDigest ||
		external.Namespace != runtime.Namespace || external.Endpoint != strings.TrimSpace(runtime.Spec.Deployment.Endpoint) ||
		external.RuntimeInstanceID != runtime.Spec.Capabilities.RuntimeInstanceID ||
		!external.SupportsPublicationFinalization {
		return fmt.Errorf("external artifact runtime no longer matches its frozen execution snapshot")
	}
	if err := external.Limits.Validate(); err != nil {
		return fmt.Errorf("frozen external AgentRuntime protocol limits are invalid")
	}
	if artifactSizeBytes < 0 || artifactSizeBytes > external.Limits.MaxWorkspaceDeltaBytes {
		return fmt.Errorf("external artifact exceeds the frozen workspace delta byte limit")
	}
	if !externalArtifactSnapshotSecretsMatch(external, runtime, authority) {
		return fmt.Errorf("external artifact runtime Secret authority changed after Task binding")
	}
	return nil
}

func externalArtifactSnapshotSecretsMatch(
	external *acpArtifactExternalRuntimeExecutionSnapshot,
	runtime *corev1alpha1.AgentRuntime,
	authority *acpExternalArtifactRuntimeAuthority,
) bool {
	if external == nil || runtime == nil || authority == nil {
		return false
	}
	controllerRef := runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef
	capabilityRef := runtime.Spec.ClientAuth.OperationCapabilitySecretRef
	return controllerRef != nil && capabilityRef != nil &&
		externalArtifactSnapshotSecretMatches(
			external.ControllerAuth, "controller-auth", runtime.Namespace, *controllerRef, authority.controllerSecret,
		) &&
		externalArtifactSnapshotSecretMatches(
			external.OperationCapability, "operation-capability", runtime.Namespace, *capabilityRef, authority.capabilitySecret,
		)
}

func externalArtifactSnapshotSecretMatches(
	frozen acpArtifactExecutionSnapshotSecretRef,
	role string,
	namespace string,
	reference corev1alpha1.AgentRuntimeSecretKeyReference,
	secret *corev1.Secret,
) bool {
	return secret != nil && frozen.Role == role && frozen.Namespace == namespace && frozen.Name == reference.Name &&
		frozen.UID == string(secret.UID) && frozen.ResourceVersion == secret.ResourceVersion &&
		len(frozen.Keys) == 1 && frozen.Keys[0] == reference.Key
}

// resolveArtifactRuntimePoolByIdentity resolves the RuntimePool and its exact
// active-instance controller-auth Secret from a non-secret pool namespace and
// UID. It is called with header-supplied identity so the caller's bearer can be
// authenticated before the request body is read.
func (s *Server) resolveArtifactRuntimePoolByIdentity(ctx context.Context, poolNamespace, poolUID string) (*corev1alpha1.RuntimePool, *corev1.Secret, error) {
	reader := s.authorizationReader()
	pool, err := findArtifactRuntimePoolByIdentity(ctx, reader, poolNamespace, poolUID)
	if err != nil {
		return nil, nil, err
	}
	if pool == nil || pool.Status.ActiveInstance == nil {
		return nil, nil, fmt.Errorf("runtime pool not found")
	}
	secret, err := resolveArtifactRuntimePoolAuthSecret(ctx, reader, pool)
	if err != nil {
		return nil, nil, err
	}
	return pool, secret, nil
}

func resolveArtifactRuntimePoolAuthSecret(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
) (*corev1.Secret, error) {
	if pool == nil || pool.Status.ActiveInstance == nil {
		return nil, fmt.Errorf("runtime pool active instance is unavailable")
	}
	if pool.Spec.ExecutionWorkspace != nil {
		secret, err := resolveBoundArtifactRuntimePoolAuthSecret(ctx, reader, pool)
		if err != nil {
			return nil, err
		}
		return secret, nil
	}
	var secrets corev1.SecretList
	if err := reader.List(ctx, &secrets, client.InNamespace(pool.Status.ActiveInstance.PodNamespace), client.MatchingLabels{
		runtimePoolAuthLabelAPI: runtimePoolAuthLabelValueAPI, runtimePoolUIDLabelAPI: string(pool.UID),
	}); err != nil {
		return nil, err
	}
	// During graceful epoch replacement both the draining instance's Secret
	// and the next epoch's Secret exist; select the one mounted by the
	// pool's exact active instance instead of requiring one Secret globally.
	epoch := strconv.FormatInt(pool.Status.ActiveInstance.ControllerEpoch, 10)
	legacySuffix := "auth-e" + epoch
	var matched []*corev1.Secret
	for i := range secrets.Items {
		secretEpoch := strings.TrimSpace(secrets.Items[i].Labels[runtimePoolCredentialEpochLabelAPI])
		if secretEpoch == epoch || (secretEpoch == "" && strings.HasSuffix(secrets.Items[i].Name, legacySuffix)) {
			matched = append(matched, &secrets.Items[i])
		}
	}
	if len(matched) != 1 {
		return nil, fmt.Errorf("runtime pool auth secret is ambiguous for controller epoch %d", pool.Status.ActiveInstance.ControllerEpoch)
	}
	return matched[0].DeepCopy(), nil
}

func resolveBoundArtifactRuntimePoolAuthSecret(
	ctx context.Context,
	reader client.Reader,
	pool *corev1alpha1.RuntimePool,
) (*corev1.Secret, error) {
	epoch := strconv.FormatInt(pool.Status.ActiveInstance.ControllerEpoch, 10)
	binding := strings.TrimSpace(pool.Annotations[runtimePoolPrivateAuthBindingAPI+epoch])
	name, rawUID, ok := strings.Cut(binding, "/")
	if !ok || len(validation.IsDNS1123Subdomain(name)) != 0 || strings.TrimSpace(rawUID) == "" {
		return nil, fmt.Errorf("private runtime pool auth Secret binding is invalid")
	}
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: pool.Status.ActiveInstance.PodNamespace, Name: name}, secret); err != nil {
		return nil, fmt.Errorf("read bound private runtime pool auth Secret: %w", err)
	}
	if string(secret.UID) != rawUID {
		return nil, fmt.Errorf("bound private runtime pool auth Secret UID changed")
	}
	if secret.Immutable == nil || !*secret.Immutable ||
		secret.Labels[runtimePoolAuthLabelAPI] != runtimePoolAuthLabelValueAPI ||
		secret.Labels[runtimePoolUIDLabelAPI] != string(pool.UID) ||
		secret.Labels[runtimePoolCredentialEpochLabelAPI] != epoch ||
		strings.TrimSpace(string(secret.Data[runtimePoolControllerTokenKeyAPI])) == "" ||
		len(secret.Data[runtimePoolCapabilitySecretKeyAPI]) == 0 {
		return nil, fmt.Errorf("bound private runtime pool auth Secret does not carry the exact immutable pool identity")
	}
	return secret.DeepCopy(), nil
}

func (s *Server) findTaskByUID(ctx context.Context, namespace, uid string) (*corev1alpha1.Task, error) {
	return findTaskByUIDWithReader(ctx, s.authorizationReader(), namespace, uid)
}

func findTaskByUIDWithReader(ctx context.Context, reader client.Reader, namespace, uid string) (*corev1alpha1.Task, error) {
	var tasks corev1alpha1.TaskList
	if err := reader.List(ctx, &tasks, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range tasks.Items {
		if string(tasks.Items[i].UID) == uid {
			return tasks.Items[i].DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("%w: Task", errPublisherAuthorizationObjectNotFound)
}

func readACPArtifactCapabilitySecret() ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(envACPArtifactSecretFile))
	if path == "" {
		return nil, fmt.Errorf("artifact capability secret is not configured")
	}
	return readACPArtifactCapabilitySecretFile(path)
}

func readACPArtifactCapabilitySecretFile(path string) ([]byte, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("artifact capability secret is unavailable")
	}
	value = []byte(strings.TrimSpace(string(value)))
	if len(value) < artifactcap.MinSecretBytes {
		return nil, fmt.Errorf("artifact capability secret is unavailable")
	}
	return value, nil
}

func constantAPIStringEqual(left, right string) bool {
	if len(left) != len(right) || left == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

const envWorkspacePublisherControllerTokenFile = "ORKA_WORKSPACE_PUBLISHER_CONTROLLER_TOKEN_FILE"

func (s *Server) issuePublisherArtifactAuthorization(c fiber.Ctx) error {
	// Authenticate from headers before consuming the body: runtime Pods can
	// reach this endpoint, so unauthenticated peers must be rejected without
	// being allowed to stream request bodies.
	expectedBearer, err := readSecretAtEnvPath(envWorkspacePublisherControllerTokenFile, 16)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_authorization_unavailable"})
	}
	bearer := strings.TrimSpace(strings.TrimPrefix(string(c.Request().Header.Peek("Authorization")), "Bearer "))
	if !constantAPIStringEqual(bearer, string(expectedBearer)) {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	var request publisherservice.ArtifactAuthorizationRequest
	decoder := json.NewDecoder(strings.NewReader(string(c.Body())))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) == nil || request.Validate() != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid_request"})
	}
	if err := s.authorizePublisherArtifactRequest(c.Context(), request); err != nil {
		if errors.Is(err, errPublisherArtifactAuthorizationUnavailable) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_authorization_unavailable"})
		}
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	if err := s.authorizePublisherParentEffect(c.Context(), request.ParentOperation, request.Metadata); err != nil {
		if errors.Is(err, errPublisherArtifactAuthorizationUnavailable) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_authorization_unavailable"})
		}
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "authorization_failed"})
	}
	artifactSecret, err := readACPArtifactCapabilitySecret()
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	binding, err := publisherservice.ArtifactBinding(request)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid_request"})
	}
	now := time.Now().UTC()
	const capabilityTTL = 2 * time.Minute
	authorization, err := artifactcap.Issue(artifactSecret, binding, now, capabilityTTL)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	if s.config.ArtifactReservations == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	if err := s.config.ArtifactReservations.Reserve(c.Context(), binding, now.Add(capabilityTTL+artifactcap.MaxClockSkew)); err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "artifact_transport_unavailable"})
	}
	return c.JSON(publisherservice.ArtifactAuthorizationResponse{
		Capability: authorization.Capability, RequestDigest: authorization.RequestDigest,
	})
}

func (s *Server) authorizePublisherArtifactRequest(ctx context.Context, request publisherservice.ArtifactAuthorizationRequest) error {
	switch request.ParentOperation {
	case publisherservice.OperationWorkspacePrepare:
		return s.authorizePublisherWorkspaceUpload(ctx, request)
	case publisherservice.OperationPublicationPrepare:
		if request.ArtifactOperation == artifactcap.OperationUpload {
			return s.authorizePublisherBundleUpload(ctx, request)
		}
		return s.authorizePublisherDeltaDownload(ctx, request)
	case publisherservice.OperationPublicationPublish, publisherservice.OperationPublicationVerify:
		return s.authorizePublisherBundleDownload(ctx, request)
	default:
		return fmt.Errorf("unsupported publisher artifact operation")
	}
}

func (s *Server) authorizePublisherWorkspaceUpload(ctx context.Context, request publisherservice.ArtifactAuthorizationRequest) error {
	log := logf.FromContext(ctx)
	if request.ArtifactOperation != artifactcap.OperationUpload || request.Metadata.TaskID == "" || request.Metadata.PublicationID != "" {
		log.Info("publisher artifact authorization denied", "reason", "workspace_identity_invalid", "parentOperation", request.ParentOperation, "namespace", request.Metadata.Namespace, "operationID", request.Metadata.OperationID)
		return fmt.Errorf("workspace artifact identity is invalid")
	}
	task, err := s.findTaskByUID(ctx, request.Metadata.Namespace, request.Metadata.TaskID)
	if err != nil && !errors.Is(err, errPublisherAuthorizationObjectNotFound) {
		log.Info("publisher artifact authorization denied", "reason", "workspace_task_read_failed", "parentOperation", request.ParentOperation, "namespace", request.Metadata.Namespace, "operationID", request.Metadata.OperationID, "error", err)
		return fmt.Errorf("%w: workspace Task could not be read", errPublisherArtifactAuthorizationUnavailable)
	}
	if err != nil || task.Status.Execution == nil {
		log.Info("publisher artifact authorization denied", "reason", "workspace_task_unavailable", "parentOperation", request.ParentOperation, "namespace", request.Metadata.Namespace, "operationID", request.Metadata.OperationID)
		return fmt.Errorf("workspace Task is unavailable")
	}
	execution := task.Status.Execution
	// Task status and the external-effect lease are committed through separate
	// Kubernetes objects. The fresh Task projection can therefore advance once
	// to Submitting while the exact workspace effect still reads InFlight. Keep
	// that observed handoff tolerant, but reject every later and terminal state.
	// authorizePublisherParentEffect separately enforces the live current-epoch
	// lease for this exact workspace preparation operation.
	if (!taskStateAllowsWorkspacePreparation(execution.State) && execution.State != corev1alpha1.TaskExecutionStateSubmitting) ||
		execution.PromptID == "" || request.Metadata.OperationID != "workspace-prepare-"+execution.PromptID {
		log.Info("publisher artifact authorization denied", "reason", "workspace_task_state_mismatch", "parentOperation", request.ParentOperation, "namespace", request.Metadata.Namespace, "operationID", request.Metadata.OperationID, "executionState", execution.State, "promptIDMatches", request.Metadata.OperationID == "workspace-prepare-"+execution.PromptID)
		return fmt.Errorf("workspace Task is not in the exact preparation handoff")
	}
	return nil
}

func taskStateAllowsWorkspacePreparation(state corev1alpha1.TaskExecutionState) bool {
	switch state {
	case corev1alpha1.TaskExecutionStateQueued,
		corev1alpha1.TaskExecutionStateReserved,
		corev1alpha1.TaskExecutionStateSessionStarting,
		corev1alpha1.TaskExecutionStatePlanned:
		return true
	default:
		return false
	}
}

func (s *Server) authorizePublisherDeltaDownload(ctx context.Context, request publisherservice.ArtifactAuthorizationRequest) error {
	if request.ArtifactOperation != artifactcap.OperationDownload || request.Metadata.PublicationID == "" || request.Metadata.TaskID != "" {
		return fmt.Errorf("publication artifact identity is invalid")
	}
	publication, err := s.findPublicationByID(ctx, request.Metadata.Namespace, request.Metadata.PublicationID)
	if err != nil && !errors.Is(err, errPublisherAuthorizationObjectNotFound) {
		return fmt.Errorf("%w: Publication could not be read", errPublisherArtifactAuthorizationUnavailable)
	}
	if err != nil || string(publication.Status.State) != string(store.PublicationPreparing) {
		return fmt.Errorf("publication is not preparing")
	}
	if publication.Spec.ArtifactID != string(request.Artifact.ArtifactID) ||
		publication.Spec.ArtifactDigest != request.Artifact.Digest ||
		publication.Spec.ArtifactSizeBytes != request.Artifact.SizeBytes ||
		publication.Spec.ArtifactMediaType != request.Artifact.MediaType {
		return fmt.Errorf("publication artifact identity drifted")
	}
	return nil
}

func (s *Server) authorizePublisherBundleUpload(ctx context.Context, request publisherservice.ArtifactAuthorizationRequest) error {
	if request.ArtifactOperation != artifactcap.OperationUpload || request.Artifact.MediaType != artifactcap.MediaTypeGitBundle ||
		request.Metadata.PublicationID == "" || request.Metadata.TaskID != "" {
		return fmt.Errorf("prepared bundle upload identity is invalid")
	}
	publication, err := s.findPublicationByID(ctx, request.Metadata.Namespace, request.Metadata.PublicationID)
	if err != nil && !errors.Is(err, errPublisherAuthorizationObjectNotFound) {
		return fmt.Errorf("%w: Publication could not be read", errPublisherArtifactAuthorizationUnavailable)
	}
	if err != nil || string(publication.Status.State) != string(store.PublicationPreparing) || publication.Status.PreparedReceipt != nil {
		return fmt.Errorf("publication is not accepting a prepared bundle")
	}
	return nil
}

func (s *Server) authorizePublisherBundleDownload(ctx context.Context, request publisherservice.ArtifactAuthorizationRequest) error {
	if request.ArtifactOperation != artifactcap.OperationDownload || request.Artifact.MediaType != artifactcap.MediaTypeGitBundle ||
		request.Metadata.PublicationID == "" || request.Metadata.TaskID != "" {
		return fmt.Errorf("prepared bundle download identity is invalid")
	}
	publication, err := s.findPublicationByID(ctx, request.Metadata.Namespace, request.Metadata.PublicationID)
	if err != nil && !errors.Is(err, errPublisherAuthorizationObjectNotFound) {
		return fmt.Errorf("%w: Publication could not be read", errPublisherArtifactAuthorizationUnavailable)
	}
	if err != nil || publication.Status.PreparedReceipt == nil {
		return fmt.Errorf("publication prepared receipt is unavailable")
	}
	expectedState := store.PublicationPublishing
	if request.ParentOperation == publisherservice.OperationPublicationVerify {
		expectedState = store.PublicationVerifying
	}
	receipt := publication.Status.PreparedReceipt
	if string(publication.Status.State) != string(expectedState) || receipt.BundleArtifactID != string(request.Artifact.ArtifactID) ||
		receipt.BundleDigest != request.Artifact.Digest || receipt.BundleSizeBytes != request.Artifact.SizeBytes ||
		receipt.BundleMediaType != request.Artifact.MediaType {
		return fmt.Errorf("publication prepared bundle identity drifted")
	}
	return nil
}

func (s *Server) findPublicationByID(ctx context.Context, namespace, id string) (*corev1alpha1.Publication, error) {
	var publications corev1alpha1.PublicationList
	if err := s.authorizationReader().List(ctx, &publications, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range publications.Items {
		if publications.Items[i].Spec.ID == id {
			return publications.Items[i].DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("%w: Publication", errPublisherAuthorizationObjectNotFound)
}

func readSecretAtEnvPath(name string, minimum int) ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(name))
	if path == "" {
		return nil, fmt.Errorf("%s is not configured", name)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	value = []byte(strings.TrimSpace(string(value)))
	if len(value) < minimum {
		return nil, fmt.Errorf("%s is unavailable", name)
	}
	return value, nil
}

func (s *Server) authorizePublisherParentEffect(
	ctx context.Context,
	operation publisherservice.Operation,
	metadata publisherservice.OperationMetadata,
) error {
	log := logf.FromContext(ctx)
	if s.config.ControllerEpochs == nil || s.config.ExternalEffects == nil {
		log.Info("publisher artifact authorization denied", "reason", "parent_epoch_authority_unavailable", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID)
		return errPublisherArtifactAuthorizationUnavailable
	}
	currentFence, err := s.config.ControllerEpochs.CurrentFence(ctx)
	if err != nil || strings.TrimSpace(currentFence.Name) == "" || currentFence.Epoch <= 0 || strings.TrimSpace(currentFence.HolderID) == "" {
		log.Info("publisher artifact authorization denied", "reason", "parent_epoch_unavailable", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID, "error", err)
		return fmt.Errorf("%w: controller epoch could not be read", errPublisherArtifactAuthorizationUnavailable)
	}
	kind := ""
	aggregateID := metadata.PublicationID
	switch operation {
	case publisherservice.OperationWorkspaceResolve:
		kind, aggregateID = "workspace.resolve", metadata.TaskID
	case publisherservice.OperationWorkspacePrepare:
		kind, aggregateID = "workspace.prepare", metadata.TaskID
	case publisherservice.OperationPublicationPreflight:
		kind = "publisher.preflight"
	case publisherservice.OperationPublicationPrepare:
		kind = "publisher.prepare"
	case publisherservice.OperationPublicationPublish:
		kind = "publisher.publish"
	case publisherservice.OperationPublicationVerify:
		kind = "publisher.verify"
	case publisherservice.OperationPullRequestReconcile:
		kind = "publisher.pull-request"
	default:
		return fmt.Errorf("unsupported Publisher parent operation")
	}
	if aggregateID == "" || metadata.OperationID == "" {
		log.Info("publisher artifact authorization denied", "reason", "parent_identity_incomplete", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID)
		return fmt.Errorf("publisher parent effect identity is incomplete")
	}
	identity := store.ExternalEffectIdentity{
		Kind: kind, Namespace: metadata.Namespace, AggregateID: aggregateID, OperationID: metadata.OperationID,
	}
	effect, err := s.config.ExternalEffects.GetExternalEffectByIdentity(ctx, identity)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.Info("publisher artifact authorization denied", "reason", "parent_effect_not_in_flight", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID)
			return fmt.Errorf("publisher parent effect is not exactly in flight")
		}
		log.Info("publisher artifact authorization denied", "reason", "parent_effect_read_failed", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID, "error", err)
		return fmt.Errorf("%w: parent effect could not be read: %v", errPublisherArtifactAuthorizationUnavailable, err)
	}
	now := time.Now().UTC()
	if effect.Identity != identity || effect.State != store.ExternalEffectInFlight ||
		!externalEffectLeaseActive(effect, currentFence, now) {
		log.Info("publisher artifact authorization denied", "reason", "parent_effect_not_exactly_in_flight", "parentOperation", operation, "namespace", metadata.Namespace, "operationID", metadata.OperationID, "controllerEpoch", currentFence.Epoch)
		return fmt.Errorf("publisher parent effect is not exactly in flight")
	}
	return nil
}

// externalEffectLeaseActive reports whether an in-flight external effect still
// holds a live lease under the controller's current durable fence. The
// publisher broker paths authenticate on a shared bearer with no per-request
// epoch capability, so a lease from a superseded controller epoch must stop
// authorizing broker access immediately even when its wall-clock expiry has not
// elapsed yet.
func externalEffectLeaseActive(effect *store.ExternalEffect, fence store.ControllerEpochFence, now time.Time) bool {
	if effect == nil || strings.TrimSpace(effect.LeaseOwner) == "" || effect.ControllerEpoch <= 0 {
		return false
	}
	return effect.ControllerEpochName == fence.Name && effect.ControllerEpoch == fence.Epoch &&
		effect.LeaseExpiresAt != nil && effect.LeaseExpiresAt.After(now)
}
