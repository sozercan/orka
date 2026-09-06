package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// SubstrateSnapshotContentScope is the immutable content classification
// returned with a provider ActorSnapshot.
type SubstrateSnapshotContentScope string

const (
	SubstrateSnapshotContentScopeData SubstrateSnapshotContentScope = "Data"
	SubstrateSnapshotContentScopeFull SubstrateSnapshotContentScope = "Full"
)

// SubstrateDataSnapshotFence carries the provider identities and versions that
// an atomic data-only resume must compare in the same operation that resumes
// the actor. Provider-native names stay inside the controller/control process.
// Only a canonical digest is persisted.
type SubstrateDataSnapshotFence struct {
	ActorID            string
	ActorUID           string
	ActorVersion       int64
	SnapshotAtespace   string
	SnapshotName       string
	SnapshotUID        string
	SnapshotVersion    int64
	SourceActorUID     string
	SourceActorVersion int64
	ContentScope       SubstrateSnapshotContentScope
}

// SubstrateActorTemplateFence identifies the exact controller-derived
// ActorTemplate that an atomic data-only resume is allowed to use.
type SubstrateActorTemplateFence struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	Revision        string
}

// SubstrateDataCheckpointFence binds a data-only checkpoint mutation to the
// exact actor lifetime and controller-derived ActorTemplate revision whose
// snapshot policy was verified before the call.
type SubstrateDataCheckpointFence struct {
	OperationID  string
	ActorID      string
	ActorUID     string
	ActorVersion int64
	Template     SubstrateActorTemplateFence
}

// SubstrateDataResumeFence binds a resume mutation to both the immutable data
// snapshot and the exact ActorTemplate revision used for the cold boot.
type SubstrateDataResumeFence struct {
	OperationID string
	Snapshot    SubstrateDataSnapshotFence
	Template    SubstrateActorTemplateFence
}

// SubstrateDataCheckpointOperationProof is the provider-persisted result of
// one atomically fenced data-only checkpoint request. ActorVersion is the
// exact source version accepted by the mutation. GetActor must keep returning
// the proof after the call so the controller can recover an accepted request
// without creating another snapshot.
type SubstrateDataCheckpointOperationProof struct {
	OperationID  string
	ActorID      string
	ActorUID     string
	ActorVersion int64
}

// SubstrateDataResumeOperationProof is the provider-persisted result of one
// atomically fenced data-only resume. The controller supplies OperationID and
// the provider records it with the exact Actor UID/version accepted by the
// mutation. GetActor must keep returning this proof after the call so a
// controller restart can distinguish that operation from an out-of-band
// resume of the same deterministic Actor ID.
type SubstrateDataResumeOperationProof struct {
	OperationID  string
	ActorID      string
	ActorUID     string
	ActorVersion int64
}

// SubstrateWorkerPodFence identifies the exact provider worker Pod that may
// receive resumed-runtime credentials. Providers must compare every field
// atomically with delivery and must not log or persist it outside safe status.
type SubstrateWorkerPodFence struct {
	Namespace string
	Name      string
	UID       string
}

// SubstrateDataResumeCredentialFence binds credential delivery to both the
// accepted data-resume operation and its exact current worker Pod.
type SubstrateDataResumeCredentialFence struct {
	ResumeOperation SubstrateDataResumeOperationProof
	WorkerPod       SubstrateWorkerPodFence
}

// SubstrateCredentialBootstrapEnvelope carries the exact controller-signed
// credential request a provider must deliver to a resumed actor. Body contains
// credentials and must never be persisted or logged.
type SubstrateCredentialBootstrapEnvelope struct {
	Nonce     string
	Signature string
	Body      []byte
}

// SubstrateCredentialBootstrapResult reports the supervisor bootstrap outcome.
// A payload conflict means the exact workload was already seeded differently;
// AlreadyComplete means its one-time bootstrap endpoint has already closed.
// FenceConflict means the provider rejected the actor/operation comparison and
// did not deliver any part of the credential body.
type SubstrateCredentialBootstrapResult struct {
	AlreadyComplete bool
	PayloadConflict bool
	FenceConflict   bool
}

// SubstrateRuntimeActor is the sanitized Actor view used by workspace-backed
// ACP RuntimePools. It carries no snapshot URIs or provider credentials.
type SubstrateRuntimeActor struct {
	ActorID      string
	ActorUID     string
	ActorVersion int64
	// LatestDataOperationID identifies the provider's latest data-affecting
	// checkpoint or resume mutation for this Actor lifetime. Status-only Actor
	// updates do not change it. Every data-affecting mutation, including one
	// outside Orka, must replace it so stale operation proofs fail closed.
	LatestDataOperationID string
	TemplateNamespace     string
	TemplateName          string
	Status                string
	PodNamespace          string
	PodName               string
	PodIP                 string
	// SnapshotObserved reports that the provider recorded a completed or
	// in-progress snapshot for this actor. Pools without an operator-permitted
	// data-only suspension policy never request snapshots, so an observed
	// snapshot there is proof of a provider-initiated suspension and forces a
	// fail-closed recycle. Data-only pools expect snapshot records after a
	// requested suspension, but resume also requires an immutable Data-scope
	// proof and an atomic provider-side comparison.
	SnapshotObserved bool
	// DataSnapshot is the immutable provider proof for the actor's latest
	// durable snapshot. It remains nil when the configured protocol cannot
	// retrieve ActorSnapshot identity, versions, and content scope.
	DataSnapshot *SubstrateDataSnapshotFence
	// DataResumeOperation is the durable provider result for the last accepted
	// atomically fenced data-only resume. It contains no credentials or snapshot
	// location. Providers that cannot persist and return it must not advertise
	// data-snapshot resume fencing support.
	DataResumeOperation *SubstrateDataResumeOperationProof
	// DataCheckpointOperation is the durable provider result for the last
	// accepted atomically fenced data-only checkpoint request. Providers that
	// cannot persist and idempotently return it must not advertise data
	// checkpoint fencing support.
	DataCheckpointOperation *SubstrateDataCheckpointOperationProof
}

// VerifiedDataSnapshotFence validates and canonicalizes the immutable proof
// needed for a data-only resume. The returned digest is safe to persist; the
// provider-native names and UIDs remain inside the controller process.
func (a *SubstrateRuntimeActor) VerifiedDataSnapshotFence(actorID string) (SubstrateDataSnapshotFence, string, error) {
	if a == nil || strings.TrimSpace(a.ActorID) != strings.TrimSpace(actorID) {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider snapshot proof does not identify the exact actor")
	}
	if a.DataSnapshot == nil {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider did not return an immutable ActorSnapshot proof")
	}
	fence := *a.DataSnapshot
	fence.ActorID = strings.TrimSpace(fence.ActorID)
	fence.ActorUID = strings.TrimSpace(fence.ActorUID)
	fence.SnapshotAtespace = strings.TrimSpace(fence.SnapshotAtespace)
	fence.SnapshotName = strings.TrimSpace(fence.SnapshotName)
	fence.SnapshotUID = strings.TrimSpace(fence.SnapshotUID)
	fence.SourceActorUID = strings.TrimSpace(fence.SourceActorUID)
	if fence.ActorID != strings.TrimSpace(actorID) || fence.ActorUID == "" || fence.ActorUID != strings.TrimSpace(a.ActorUID) ||
		fence.ActorVersion <= 0 || a.ActorVersion <= 0 || fence.ActorVersion > a.ActorVersion {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider snapshot proof is missing a valid actor UID/version lineage")
	}
	if fence.SnapshotAtespace == "" || fence.SnapshotName == "" || fence.SnapshotUID == "" || fence.SnapshotVersion <= 0 {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider snapshot proof is missing immutable snapshot identity")
	}
	if fence.SourceActorUID == "" || fence.SourceActorUID != fence.ActorUID ||
		fence.SourceActorVersion <= 0 || fence.SourceActorVersion > a.ActorVersion {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider snapshot proof does not bind the snapshot to this actor generation")
	}
	if fence.ContentScope != SubstrateSnapshotContentScopeData {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("provider ActorSnapshot content scope is not Data")
	}
	payload, err := json.Marshal(struct {
		SchemaVersion      string                        `json:"schemaVersion"`
		ActorID            string                        `json:"actorID"`
		ActorUID           string                        `json:"actorUID"`
		SnapshotAtespace   string                        `json:"snapshotAtespace"`
		SnapshotName       string                        `json:"snapshotName"`
		SnapshotUID        string                        `json:"snapshotUID"`
		SnapshotVersion    int64                         `json:"snapshotVersion"`
		SourceActorUID     string                        `json:"sourceActorUID"`
		SourceActorVersion int64                         `json:"sourceActorVersion"`
		ContentScope       SubstrateSnapshotContentScope `json:"contentScope"`
	}{
		SchemaVersion:      "orka.substrate-data-snapshot-fence.v2",
		ActorID:            fence.ActorID,
		ActorUID:           fence.ActorUID,
		SnapshotAtespace:   fence.SnapshotAtespace,
		SnapshotName:       fence.SnapshotName,
		SnapshotUID:        fence.SnapshotUID,
		SnapshotVersion:    fence.SnapshotVersion,
		SourceActorUID:     fence.SourceActorUID,
		SourceActorVersion: fence.SourceActorVersion,
		ContentScope:       fence.ContentScope,
	})
	if err != nil {
		return SubstrateDataSnapshotFence{}, "", fmt.Errorf("encode provider snapshot proof: %w", err)
	}
	sum := sha256.Sum256(payload)
	// ActorVersion is a mutable resume fence, not part of immutable snapshot
	// identity. Status-only Actor updates may advance it after checkpointing
	// without changing snapshot identity or data-operation lineage.
	fence.ActorVersion = a.ActorVersion
	return fence, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ImmutableIdentityDigest returns a safe digest of only the immutable
// ActorSnapshot identity. It deliberately excludes the mutable ActorVersion so
// callers can prove that an accepted suspension produced a new snapshot.
func (f SubstrateDataSnapshotFence) ImmutableIdentityDigest() (string, error) {
	namespace := strings.TrimSpace(f.SnapshotAtespace)
	name := strings.TrimSpace(f.SnapshotName)
	uid := strings.TrimSpace(f.SnapshotUID)
	if namespace == "" || name == "" || uid == "" || f.SnapshotVersion <= 0 {
		return "", fmt.Errorf("provider snapshot proof is missing immutable snapshot identity")
	}
	payload, err := json.Marshal(struct {
		SchemaVersion   string `json:"schemaVersion"`
		Namespace       string `json:"namespace"`
		Name            string `json:"name"`
		UID             string `json:"uid"`
		SnapshotVersion int64  `json:"snapshotVersion"`
	}{
		SchemaVersion:   "orka.substrate-data-snapshot-identity.v1",
		Namespace:       namespace,
		Name:            name,
		UID:             uid,
		SnapshotVersion: f.SnapshotVersion,
	})
	if err != nil {
		return "", fmt.Errorf("encode provider snapshot identity: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// VerifiedDataCheckpointOperation validates the durable provider result for
// one controller-issued checkpoint operation and returns a safe digest for
// RuntimePool metadata. The source Actor version remains fixed in the proof;
// status-only Actor updates may advance the current Actor version.
func (a *SubstrateRuntimeActor) VerifiedDataCheckpointOperation(
	actorID, operationID string,
	sourceActorVersion int64,
) (SubstrateDataCheckpointOperationProof, string, error) {
	actorID = strings.TrimSpace(actorID)
	operationID = strings.TrimSpace(operationID)
	if a == nil || actorID == "" || strings.TrimSpace(a.ActorID) != actorID || operationID == "" {
		return SubstrateDataCheckpointOperationProof{}, "", fmt.Errorf("provider checkpoint proof does not identify the requested actor and operation")
	}
	if a.DataCheckpointOperation == nil {
		return SubstrateDataCheckpointOperationProof{}, "", fmt.Errorf("provider did not return a durable data-checkpoint operation proof")
	}
	proof := *a.DataCheckpointOperation
	proof.OperationID = strings.TrimSpace(proof.OperationID)
	proof.ActorID = strings.TrimSpace(proof.ActorID)
	proof.ActorUID = strings.TrimSpace(proof.ActorUID)
	actorUID := strings.TrimSpace(a.ActorUID)
	if proof.OperationID != operationID || strings.TrimSpace(a.LatestDataOperationID) != operationID ||
		proof.ActorID != actorID || proof.ActorUID == "" || proof.ActorUID != actorUID ||
		sourceActorVersion <= 0 || proof.ActorVersion != sourceActorVersion || a.ActorVersion < sourceActorVersion {
		return SubstrateDataCheckpointOperationProof{}, "", fmt.Errorf("provider data-checkpoint operation proof is not the latest operation for the exact actor lifetime and requested source Actor version")
	}
	payload, err := json.Marshal(struct {
		SchemaVersion string `json:"schemaVersion"`
		OperationID   string `json:"operationID"`
		ActorID       string `json:"actorID"`
		ActorUID      string `json:"actorUID"`
		ActorVersion  int64  `json:"actorVersion"`
	}{
		SchemaVersion: "orka.substrate-data-checkpoint-operation.v1",
		OperationID:   proof.OperationID,
		ActorID:       proof.ActorID,
		ActorUID:      proof.ActorUID,
		ActorVersion:  proof.ActorVersion,
	})
	if err != nil {
		return SubstrateDataCheckpointOperationProof{}, "", fmt.Errorf("encode provider data-checkpoint operation proof: %w", err)
	}
	sum := sha256.Sum256(payload)
	return proof, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// VerifiedDataResumeOperation validates the durable provider result for one
// controller-issued resume operation and returns a safe digest for RuntimePool
// metadata. The accepted Actor version remains fixed in the proof while the
// current Actor version may advance as status changes.
func (a *SubstrateRuntimeActor) VerifiedDataResumeOperation(
	actorID, operationID string,
) (SubstrateDataResumeOperationProof, string, error) {
	actorID = strings.TrimSpace(actorID)
	operationID = strings.TrimSpace(operationID)
	if a == nil || actorID == "" || strings.TrimSpace(a.ActorID) != actorID || operationID == "" {
		return SubstrateDataResumeOperationProof{}, "", fmt.Errorf("provider resume proof does not identify the requested actor and operation")
	}
	if a.DataResumeOperation == nil {
		return SubstrateDataResumeOperationProof{}, "", fmt.Errorf("provider did not return a durable data-resume operation proof")
	}
	proof := *a.DataResumeOperation
	proof.OperationID = strings.TrimSpace(proof.OperationID)
	proof.ActorID = strings.TrimSpace(proof.ActorID)
	proof.ActorUID = strings.TrimSpace(proof.ActorUID)
	actorUID := strings.TrimSpace(a.ActorUID)
	if proof.OperationID != operationID || strings.TrimSpace(a.LatestDataOperationID) != operationID ||
		proof.ActorID != actorID || proof.ActorUID == "" ||
		proof.ActorUID != actorUID || proof.ActorVersion <= 0 || a.ActorVersion < proof.ActorVersion {
		return SubstrateDataResumeOperationProof{}, "", fmt.Errorf("provider data-resume operation proof is not the latest operation for the exact actor lifetime")
	}
	payload, err := json.Marshal(struct {
		SchemaVersion string `json:"schemaVersion"`
		OperationID   string `json:"operationID"`
		ActorID       string `json:"actorID"`
		ActorUID      string `json:"actorUID"`
		ActorVersion  int64  `json:"actorVersion"`
	}{
		SchemaVersion: "orka.substrate-data-resume-operation.v1",
		OperationID:   proof.OperationID,
		ActorID:       proof.ActorID,
		ActorUID:      proof.ActorUID,
		ActorVersion:  proof.ActorVersion,
	})
	if err != nil {
		return SubstrateDataResumeOperationProof{}, "", fmt.Errorf("encode provider data-resume operation proof: %w", err)
	}
	sum := sha256.Sum256(payload)
	return proof, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// Running reports the exact provider running state; anything else refuses
// exact-instance admission.
func (a *SubstrateRuntimeActor) Running() bool {
	return a != nil && a.Status == substrateStatusRunning && strings.TrimSpace(a.PodIP) != ""
}

// SuspendedOrSuspending reports a provider-side suspension, which checkpoints
// supervisor memory and is prohibited for ACP RuntimePool actors.
func (a *SubstrateRuntimeActor) SuspendedOrSuspending() bool {
	return a != nil && (a.Status == substrateStatusSuspended || a.Status == substrateStatusSuspending)
}

// Suspended reports the settled provider state that permits DeleteActor.
func (a *SubstrateRuntimeActor) Suspended() bool {
	return a != nil && a.Status == substrateStatusSuspended
}

// Suspending reports an in-flight provider suspension transition.
func (a *SubstrateRuntimeActor) Suspending() bool {
	return a != nil && a.Status == substrateStatusSuspending
}

// RunningStatus reports provider-side liveness (STATUS_RUNNING) regardless of
// route readiness: a just-resumed actor can be Running before its Pod IP is
// populated, which is a transitional state, never a crash.
func (a *SubstrateRuntimeActor) RunningStatus() bool {
	return a != nil && a.Status == substrateStatusRunning
}

// Resuming reports an in-flight provider cold resume: ResumeActor accepted
// the request but the workload has not reached Running yet. The checkpoint is
// being consumed, not crashed.
func (a *SubstrateRuntimeActor) Resuming() bool {
	return a != nil && a.Status == substrateStatusResuming
}

// Crashed reports a provider state whose worker workload is no longer
// assigned. A crashed actor may be deleted only after Orka has durably proven
// the exact prior workload absent.
func (a *SubstrateRuntimeActor) Crashed() bool {
	return a != nil && a.Status == substrateStatusCrashed
}

// SubstrateRuntimeActorControl is the narrow Substrate control surface needed
// to host one ACP RuntimePool instance in an Actor. Suspending a live
// workload is prohibited: gVisor suspension checkpoints supervisor process
// memory — including live pool and provider-proxy credentials — into provider
// snapshot storage, which the ACP execution-workspace contract forbids.
// Because the provider deletes only suspended actors, teardown first destroys
// the workload's memory (deleting its single-workload worker Pod), then calls
// SettleActor purely to transition the memoryless actor into the deletable
// suspended state — with nothing left to checkpoint — and then DeleteActor.
type SubstrateRuntimeActorControl interface {
	// GetActor returns nil with no error when the actor does not exist.
	GetActor(ctx context.Context, actorID string) (*SubstrateRuntimeActor, error)
	CreateActor(ctx context.Context, actorID, templateNamespace, templateName string) (*SubstrateRuntimeActor, error)
	// ResumeActor with boot=true starts the workload from scratch. ACP hosting
	// always boots fresh so a supervisor lifetime is exactly one boot.
	ResumeActor(ctx context.Context, actorID string, boot bool) (*SubstrateRuntimeActor, error)
	// SettleActor transitions the actor toward the provider's deletable
	// suspended state. It must only be called after the actor's workload
	// memory has been destroyed and its absence confirmed — settling a live
	// supervisor would checkpoint credentials and is prohibited.
	SettleActor(ctx context.Context, actorID string) (*SubstrateRuntimeActor, error)
	// DeleteActor returns nil when the actor is already absent. The provider
	// accepts deletion of suspended (settled) or crashed actors.
	DeleteActor(ctx context.Context, actorID string) error
	Close() error
}

// SubstrateRuntimeActorDataCheckpointControl is the extra provider contract
// needed to create a data-only checkpoint. SuspendActorForDataCheckpoint must
// compare every expected actor and ActorTemplate fence field atomically with
// the suspension mutation, persist the controller-issued OperationID with the
// exact source Actor UID/version, expose that proof through GetActor, and make
// repeated calls with the same OperationID return the original accepted result
// without creating another snapshot. A client that can only preflight mutable
// resources must not implement this interface.
type SubstrateRuntimeActorDataCheckpointControl interface {
	DataSnapshotCheckpointFencingSupported() bool
	SuspendActorForDataCheckpoint(
		ctx context.Context,
		actorID string,
		expected SubstrateDataCheckpointFence,
	) (*SubstrateRuntimeActor, error)
}

// SubstrateRuntimeActorDataResumeControl is the extra provider contract needed
// to restore a data-only checkpoint. ResumeActorFromDataCheckpoint must compare
// every expected actor, snapshot, and ActorTemplate fence field atomically with
// the resume mutation, persist the controller-issued OperationID with the exact
// accepted Actor UID/version, and expose that proof through GetActor. Repeating
// the same OperationID must return the original accepted result without replaying
// the snapshot. BootstrapActorCredentialsForDataResume must then atomically
// compare that proof, LatestDataOperationID, and the exact worker Pod fence
// with expected before forwarding the signed envelope to the same current
// actor workload. It must repeat those comparisons on every call, including
// when reporting AlreadyComplete, so the controller can fence admission after
// its authenticated runtime probe. If any fence changes, it must fail before
// delivering credentials. A client that can only preflight mutable resources
// or send through an unfenced logical route must not implement this interface.
type SubstrateRuntimeActorDataResumeControl interface {
	DataSnapshotResumeFencingSupported() bool
	DataResumeCredentialBootstrapFencingSupported() bool
	ResumeActorFromDataCheckpoint(
		ctx context.Context,
		actorID string,
		expected SubstrateDataResumeFence,
	) (*SubstrateRuntimeActor, error)
	BootstrapActorCredentialsForDataResume(
		ctx context.Context,
		actorID string,
		expected SubstrateDataResumeCredentialFence,
		envelope SubstrateCredentialBootstrapEnvelope,
	) (SubstrateCredentialBootstrapResult, error)
}

// SubstrateRuntimeActorCreateRecoveryControl is the provider attestation
// required before retrying a CreateActor call whose outcome was ambiguous.
// A GetActor miss alone is insufficient because the original call may still
// materialize the deterministic actor later.
type SubstrateRuntimeActorCreateRecoveryControl interface {
	ActorCreateRecoveryAttestationSupported() bool
	ConfirmActorCreationSettled(ctx context.Context, actorID string) (bool, error)
}

type substrateRuntimeActorControl struct {
	control substrateControlClient
}

// NewSubstrateRuntimeActorControl builds the control-only Substrate client for
// ACP RuntimePool hosting.
func NewSubstrateRuntimeActorControl(cfg SubstrateConfig) (SubstrateRuntimeActorControl, error) {
	if cfg.ControlClient == nil {
		client, err := newGRPCSubstrateControlClient(cfg)
		if err != nil {
			return nil, err
		}
		cfg.ControlClient = client
	}
	return &substrateRuntimeActorControl{control: cfg.ControlClient}, nil
}

func (c *substrateRuntimeActorControl) GetActor(ctx context.Context, actorID string) (*SubstrateRuntimeActor, error) {
	actor, err := c.control.GetActor(ctx, actorID)
	if err != nil {
		if IsKind(err, ErrorKindNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return substrateRuntimeActorView(actor), nil
}

func (c *substrateRuntimeActorControl) CreateActor(
	ctx context.Context,
	actorID, templateNamespace, templateName string,
) (*SubstrateRuntimeActor, error) {
	actor, err := c.control.CreateActor(ctx, actorID, templateNamespace, templateName)
	if err != nil {
		if IsKind(err, ErrorKindAlreadyExists) {
			return c.GetActor(ctx, actorID)
		}
		return nil, err
	}
	return substrateRuntimeActorView(actor), nil
}

func (c *substrateRuntimeActorControl) ResumeActor(ctx context.Context, actorID string, boot bool) (*SubstrateRuntimeActor, error) {
	actor, err := c.control.ResumeActor(ctx, actorID, boot)
	if err != nil {
		return nil, err
	}
	return substrateRuntimeActorView(actor), nil
}

func (c *substrateRuntimeActorControl) SettleActor(ctx context.Context, actorID string) (*SubstrateRuntimeActor, error) {
	actor, err := c.control.SuspendActor(ctx, actorID)
	if err != nil {
		return nil, err
	}
	return substrateRuntimeActorView(actor), nil
}

func (c *substrateRuntimeActorControl) DeleteActor(ctx context.Context, actorID string) error {
	if err := c.control.DeleteActor(ctx, actorID); err != nil && !IsKind(err, ErrorKindNotFound) {
		return err
	}
	return nil
}

func (c *substrateRuntimeActorControl) ActorCreateRecoveryAttestationSupported() bool {
	return false
}

func (c *substrateRuntimeActorControl) ConfirmActorCreationSettled(context.Context, string) (bool, error) {
	return false, fmt.Errorf("substrate provider does not expose operation-level actor creation settlement")
}

func (c *substrateRuntimeActorControl) Close() error {
	if closer, ok := c.control.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func substrateRuntimeActorView(actor *substrateActor) *SubstrateRuntimeActor {
	if actor == nil {
		return nil
	}
	return &SubstrateRuntimeActor{
		ActorID:           strings.TrimSpace(actor.ActorID),
		TemplateNamespace: strings.TrimSpace(actor.TemplateNamespace),
		TemplateName:      strings.TrimSpace(actor.TemplateName),
		Status:            strings.TrimSpace(actor.Status),
		PodNamespace:      strings.TrimSpace(actor.PodNamespace),
		PodName:           strings.TrimSpace(actor.PodName),
		PodIP:             strings.TrimSpace(actor.PodIP),
		SnapshotObserved:  strings.TrimSpace(actor.LastSnapshot) != "" || strings.TrimSpace(actor.InProgressSnapshot) != "",
	}
}
