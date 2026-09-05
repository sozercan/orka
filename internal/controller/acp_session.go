package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
)

const (
	defaultACPSessionType = "task"

	acpSessionTurnAggregateKind    = "SessionTurn"
	acpOutcomeUnknownKind          = "OutcomeUnknown"
	acpPrePromptLeaseReleasePrefix = "release-pre-prompt:"
)

// ACPSessionContinuityConfig supplies the durable stores used by the ACP
// session-continuity boundary. In production these interfaces are normally
// implemented by the same SQLite-backed durable control store.
type ACPSessionContinuityConfig struct {
	SessionControls store.SessionControlStore
	Transcripts     store.SessionStore
	GatewayEvents   store.GatewayEventStore
	Publications    store.PublicationStore
	BranchClaims    store.BranchClaimStore
	BootstrapLimits ACPBootstrapLimits
	NewSessionUID   func() (string, error)

	// Lineages receives an idempotent SQLite payload projection only after the
	// Kubernetes SessionControl store has established or verified lineage in
	// the same status CAS that records the mutation Lease.
	Lineages store.SessionLineageStore
}

// HarnessV1SessionContinuityConfig supplies only the namespaced continuity
// stores used by the publication-free harness v1 contract.
type HarnessV1SessionContinuityConfig struct {
	SessionControls store.SessionControlStore
	Transcripts     store.SessionStore
	GatewayEvents   store.GatewayEventStore
	BootstrapLimits ACPBootstrapLimits
	NewSessionUID   func() (string, error)
	Lineages        store.SessionLineageStore
}

// ACPSessionContinuity is the controller-side integration boundary for durable
// ACP Session identity, mutation fencing, canonical transcript bootstrap, and
// atomic SessionTurn completion. It deliberately does not submit prompts.
type ACPSessionContinuity struct {
	controls        store.SessionControlStore
	transcripts     store.SessionStore
	gatewayEvents   store.GatewayEventStore
	publications    store.PublicationStore
	branchClaims    store.BranchClaimStore
	bootstrapLimits ACPBootstrapLimits
	newSessionUID   func() (string, error)
	lineages        store.SessionLineageStore
}

// RecordsLineage reports whether Session lineage recording is configured.
func (c *ACPSessionContinuity) RecordsLineage() bool {
	return c != nil && c.lineages != nil
}

// NewACPSessionContinuity creates a continuity component. All stores are
// required because publication ambiguity and blocked-session recovery must be
// classified from durable receipts rather than caller assertions.
func NewACPSessionContinuity(config ACPSessionContinuityConfig) (*ACPSessionContinuity, error) {
	if config.SessionControls == nil || config.Transcripts == nil || config.Publications == nil || config.BranchClaims == nil {
		return nil, fmt.Errorf("ACP session continuity requires session-control, transcript, publication, and branch-claim stores")
	}
	return newSessionContinuity(config)
}

// NewHarnessV1SessionContinuity creates the publication-free v1 continuity
// boundary. A nonempty publication ID fails closed because no PublicationStore
// or cluster-scoped BranchClaimStore is attached.
func NewHarnessV1SessionContinuity(config HarnessV1SessionContinuityConfig) (*ACPSessionContinuity, error) {
	if config.SessionControls == nil || config.Transcripts == nil {
		return nil, fmt.Errorf("harness v1 session continuity requires session-control and transcript stores")
	}
	return newSessionContinuity(ACPSessionContinuityConfig{
		SessionControls: config.SessionControls,
		Transcripts:     config.Transcripts,
		GatewayEvents:   config.GatewayEvents,
		BootstrapLimits: config.BootstrapLimits,
		NewSessionUID:   config.NewSessionUID,
		Lineages:        config.Lineages,
	})
}

func newSessionContinuity(config ACPSessionContinuityConfig) (*ACPSessionContinuity, error) {
	limits, err := config.BootstrapLimits.withDefaults()
	if err != nil {
		return nil, err
	}
	newSessionUID := config.NewSessionUID
	if newSessionUID == nil {
		newSessionUID = newACPSessionUID
	}
	return &ACPSessionContinuity{
		controls:        config.SessionControls,
		transcripts:     config.Transcripts,
		gatewayEvents:   config.GatewayEvents,
		publications:    config.Publications,
		branchClaims:    config.BranchClaims,
		lineages:        config.Lineages,
		bootstrapLimits: limits,
		newSessionUID:   newSessionUID,
	}, nil
}

// ACPEnsureSessionRequest identifies one canonical transcript Session. An
// optional ExpectedSessionUID turns loading into an exact immutable-identity
// assertion; when omitted, a UID is generated only if the control record does
// not already exist.
type ACPEnsureSessionRequest struct {
	Namespace                 string
	SessionName               string
	SessionType               string
	ExpectedSessionUID        string
	RequireExistingTranscript bool
	Fence                     store.ControllerEpochFence
	CreatedAt                 time.Time
}

// EnsureSession creates or loads the transcript Session and its immutable
// SessionControl UID record. Concurrent creators converge on the first durable
// UID; an explicitly expected different UID is rejected.
func (c *ACPSessionContinuity) EnsureSession(ctx context.Context, request ACPEnsureSessionRequest) (*store.SessionControl, error) {
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.SessionName = strings.TrimSpace(request.SessionName)
	request.SessionType = strings.TrimSpace(request.SessionType)
	request.ExpectedSessionUID = strings.TrimSpace(request.ExpectedSessionUID)
	if request.SessionType == "" {
		request.SessionType = defaultACPSessionType
	}
	for field, value := range map[string]string{
		"session namespace": request.Namespace,
		"session name":      request.SessionName,
		"session type":      request.SessionType,
	} {
		if err := store.ValidateControlIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	if request.SessionType != defaultACPSessionType && request.SessionType != "chat" && request.SessionType != store.SessionTypeGateway {
		return nil, store.ValidationErrorf("unsupported transcript session type %q", request.SessionType)
	}
	if request.ExpectedSessionUID != "" {
		if err := store.ValidateControlIdentifier("expected session UID", request.ExpectedSessionUID); err != nil {
			return nil, err
		}
	}
	if control, err := c.controls.GetSessionControl(ctx, request.Namespace, request.SessionName); err == nil {
		if err := c.validateExistingSession(ctx, control, request); err != nil {
			return nil, err
		}
		return control, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("load ACP session control: %w", err)
	}

	now := request.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if request.RequireExistingTranscript {
		existing, transcriptErr := c.transcripts.GetSession(ctx, request.Namespace, request.SessionName)
		if transcriptErr != nil {
			if errors.Is(transcriptErr, store.ErrNotFound) {
				return nil, fmt.Errorf("session %s/%s does not exist and create=false: %w", request.Namespace, request.SessionName, store.ErrNotFound)
			}
			return nil, fmt.Errorf("load required transcript session: %w", transcriptErr)
		}
		if err := validateTranscriptSession(existing, request.SessionType); err != nil {
			return nil, err
		}
	} else if err := c.ensureTranscriptSession(ctx, request, now); err != nil {
		return nil, err
	}

	sessionUID := request.ExpectedSessionUID
	if sessionUID == "" {
		generated, err := c.newSessionUID()
		if err != nil {
			return nil, fmt.Errorf("generate ACP session UID: %w", err)
		}
		sessionUID = strings.TrimSpace(generated)
		if err := store.ValidateControlIdentifier("generated session UID", sessionUID); err != nil {
			return nil, err
		}
	}
	requestDigest, err := acpDomainDigest("session-control", map[string]any{
		"namespace": request.Namespace, "sessionName": request.SessionName, "sessionType": request.SessionType,
	})
	if err != nil {
		return nil, fmt.Errorf("digest ACP session control: %w", err)
	}
	created, err := c.controls.CreateSessionControl(ctx, &store.SessionControl{
		Namespace:     request.Namespace,
		SessionName:   request.SessionName,
		SessionUID:    sessionUID,
		RequestDigest: requestDigest,
		Availability:  store.SessionAvailable,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, request.Fence)
	if err == nil {
		return created, nil
	}
	// A racing creator can win with a different generated UID. Load the winner
	// and accept it only when the caller did not assert a specific UID.
	winner, getErr := c.controls.GetSessionControl(ctx, request.Namespace, request.SessionName)
	if getErr != nil {
		return nil, fmt.Errorf("create ACP session control: %w", err)
	}
	if request.ExpectedSessionUID != "" && winner.SessionUID != request.ExpectedSessionUID {
		return nil, fmt.Errorf("%w: session %s/%s has immutable UID %q, expected %q", store.ErrConflict,
			request.Namespace, request.SessionName, winner.SessionUID, request.ExpectedSessionUID)
	}
	if validateErr := c.validateExistingSession(ctx, winner, request); validateErr != nil {
		return nil, validateErr
	}
	return winner, nil
}

func (c *ACPSessionContinuity) ensureTranscriptSession(ctx context.Context, request ACPEnsureSessionRequest, now time.Time) error {
	existing, err := c.transcripts.GetSession(ctx, request.Namespace, request.SessionName)
	if err == nil {
		return validateTranscriptSession(existing, request.SessionType)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("load transcript session: %w", err)
	}
	createErr := c.transcripts.CreateSession(ctx, &store.SessionRecord{
		Namespace: request.Namespace, Name: request.SessionName, SessionType: request.SessionType,
		CreatedAt: now, UpdatedAt: now,
	})
	if createErr == nil {
		return nil
	}
	// SessionStore predates typed create conflicts. Read after any create error
	// so concurrent creators can converge without depending on driver errors.
	existing, getErr := c.transcripts.GetSession(ctx, request.Namespace, request.SessionName)
	if getErr != nil {
		return fmt.Errorf("create transcript session: %w", createErr)
	}
	return validateTranscriptSession(existing, request.SessionType)
}

func (c *ACPSessionContinuity) validateExistingSession(ctx context.Context, control *store.SessionControl, request ACPEnsureSessionRequest) error {
	if control == nil {
		return fmt.Errorf("ACP session control is nil")
	}
	if request.ExpectedSessionUID != "" && control.SessionUID != request.ExpectedSessionUID {
		return fmt.Errorf("%w: session %s/%s has immutable UID %q, expected %q", store.ErrConflict,
			request.Namespace, request.SessionName, control.SessionUID, request.ExpectedSessionUID)
	}
	session, err := c.transcripts.GetSession(ctx, request.Namespace, request.SessionName)
	if err != nil {
		return fmt.Errorf("load transcript for ACP session control: %w", err)
	}
	return validateTranscriptSession(session, request.SessionType)
}

func validateTranscriptSession(session *store.SessionRecord, expectedType string) error {
	if session == nil {
		return fmt.Errorf("transcript session is nil")
	}
	if session.SessionType != expectedType {
		return fmt.Errorf("%w: transcript session %s/%s has type %q, expected %q", store.ErrConflict,
			session.Namespace, session.Name, session.SessionType, expectedType)
	}
	return nil
}

// ACPAcquireSessionLeaseRequest acquires the next generation from an exact
// SessionControl snapshot. PromptRequestDigest binds the lease to the immutable
// prompt attempt input without allowing a different prompt to reuse it.
type ACPAcquireSessionLeaseRequest struct {
	Session             store.SessionControl
	Fence               store.ControllerEpochFence
	TaskName            string
	TaskUID             string
	Attempt             int64
	PromptID            string
	PromptRequestDigest string
	AcquiredAt          time.Time
	ExpiresAt           *time.Time

	// NamespaceUID, RuntimeIdentity, and ConfigDigest establish or verify the
	// Kubernetes-authoritative Session protocol/runtime lineage atomically with
	// the lease status CAS when lineage projection is configured.
	NamespaceUID      string
	ContractVersion   corev1alpha1.AgentRuntimeContractVersion
	LineageGeneration int64
	RuntimeIdentity   string
	ConfigDigest      string
}

// ACPSessionLease is the exact mutation fence that must be used when opening
// and finalizing the SessionTurn.
type ACPSessionLease struct {
	Session store.SessionControl
	Key     store.SessionTurnKey
}

type ACPReleaseSessionLeaseRequest struct {
	Lease      ACPSessionLease
	Fence      store.ControllerEpochFence
	ReleasedAt time.Time
}

// AcquireMutationLease acquires one monotonic, non-reusable Session lease.
func (c *ACPSessionContinuity) AcquireMutationLease(ctx context.Context, request ACPAcquireSessionLeaseRequest) (*ACPSessionLease, error) {
	request.TaskName = strings.TrimSpace(request.TaskName)
	request.TaskUID = strings.TrimSpace(request.TaskUID)
	request.PromptID = strings.TrimSpace(request.PromptID)
	if request.Session.Availability != store.SessionAvailable {
		return nil, fmt.Errorf("%w: session %s/%s is reconciliation-blocked", store.ErrConflict,
			request.Session.Namespace, request.Session.SessionName)
	}
	if err := store.ValidateCanonicalDigest("prompt request digest", request.PromptRequestDigest); err != nil {
		return nil, err
	}
	leaseGeneration := request.Session.LeaseGeneration + 1
	if request.Session.Lease != nil {
		leaseGeneration = request.Session.Lease.Generation
	}
	leaseDigest, err := acpSessionMutationLeaseDigest(
		request.Session.SessionUID, leaseGeneration,
		request.TaskUID, request.Attempt, request.PromptID, request.PromptRequestDigest,
	)
	if err != nil {
		return nil, fmt.Errorf("digest ACP session mutation lease: %w", err)
	}
	if existing := request.Session.Lease; existing != nil &&
		(existing.TaskUID != request.TaskUID || existing.Attempt != request.Attempt ||
			existing.PromptID != request.PromptID || existing.RequestDigest != leaseDigest) {
		return nil, fmt.Errorf("%w: session %s/%s is already leased by a different prompt operation",
			store.ErrConflict, request.Session.Namespace, request.Session.SessionName)
	}
	acquiredAt := request.AcquiredAt.UTC()
	if acquiredAt.IsZero() {
		acquiredAt = time.Now().UTC()
	}
	lineageClaim, err := c.prepareSessionLineageClaim(ctx, request)
	if err != nil {
		return nil, err
	}
	control, err := c.controls.AcquireSessionMutationLease(ctx, store.AcquireSessionMutationLeaseRequest{
		Namespace: request.Session.Namespace, SessionName: request.Session.SessionName, SessionUID: request.Session.SessionUID,
		Fence: request.Fence, ExpectedVersion: request.Session.Version, ExpectedLeaseGeneration: request.Session.LeaseGeneration,
		TaskUID: request.TaskUID, Attempt: request.Attempt, PromptID: request.PromptID,
		RequestDigest: leaseDigest, AcquiredAt: acquiredAt, ExpiresAt: request.ExpiresAt,
		Lineage: lineageClaim,
	})
	if err != nil {
		return nil, fmt.Errorf("acquire ACP session mutation lease: %w", err)
	}
	key := store.SessionTurnKey{
		SessionUID: control.SessionUID, LeaseGeneration: control.LeaseGeneration,
		TaskUID: request.TaskUID, Attempt: request.Attempt, PromptID: request.PromptID,
	}
	if err := validateACPSessionLease(control, key); err != nil {
		return nil, err
	}
	lease := &ACPSessionLease{Session: *control, Key: key}
	if c.lineages != nil {
		if control.Lineage == nil {
			return nil, fmt.Errorf("kubernetes session lease committed without authoritative lineage")
		}
		if _, err := c.lineages.ProjectSessionLineage(ctx, *control.Lineage); err != nil {
			// Retain the exact Kubernetes lease on projection failure. Releasing it
			// would allow another owner to proceed before the payload projection is
			// repaired; an idempotent retry completes the projection first.
			return nil, fmt.Errorf("project Kubernetes-authoritative Session lineage: %w", err)
		}
	}
	return lease, nil
}

// CommitRuntimeSessionGeneration records the newest provider RuntimeSession
// generation proven live while this exact Session mutation lease is active.
func (c *ACPSessionContinuity) CommitRuntimeSessionGeneration(
	ctx context.Context,
	lease ACPSessionLease,
	fence store.ControllerEpochFence,
	generation uint64,
	committedAt time.Time,
) (*ACPSessionLease, error) {
	if err := validateACPSessionLease(&lease.Session, lease.Key); err != nil {
		return nil, err
	}
	if generation == 0 || generation > maxControllerRuntimeSessionGeneration {
		return nil, store.ValidationErrorf("ACP RuntimeSession generation is outside durable Session status capacity")
	}
	control, err := c.controls.CommitSessionRuntimeGeneration(ctx, store.CommitSessionRuntimeGenerationRequest{
		Namespace: lease.Session.Namespace, SessionName: lease.Session.SessionName, SessionUID: lease.Session.SessionUID,
		Key: lease.Key, Fence: fence, ExpectedSessionVersion: lease.Session.Version,
		Generation: int64(generation), CommittedAt: committedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("commit ACP RuntimeSession generation: %w", err)
	}
	if control.RuntimeSessionGeneration != int64(generation) {
		return nil, fmt.Errorf("%w: committed ACP RuntimeSession generation changed", store.ErrConflict)
	}
	if err := validateACPSessionLease(control, lease.Key); err != nil {
		return nil, err
	}
	lease.Session = *control
	return &lease, nil
}

// prepareSessionLineageClaim determines only whether an absent authoritative
// lineage may be established. Kubernetes remains the decision point: a stale
// caller that observes a nonempty transcript can still verify a lineage that a
// concurrent first user already committed, but it cannot establish one.
func (c *ACPSessionContinuity) prepareSessionLineageClaim(ctx context.Context, request ACPAcquireSessionLeaseRequest) (*store.ClaimSessionLineageRequest, error) {
	if c.lineages == nil {
		return nil, nil
	}
	contractVersion := request.ContractVersion
	if contractVersion == "" {
		contractVersion = corev1alpha1.AgentRuntimeContractHarnessV2
	}
	lineageGeneration := request.LineageGeneration
	if lineageGeneration == 0 {
		lineageGeneration = 1
	}
	claim := &store.ClaimSessionLineageRequest{
		Namespace:         request.Session.Namespace,
		SessionName:       request.Session.SessionName,
		NamespaceUID:      request.NamespaceUID,
		SessionUID:        request.Session.SessionUID,
		ContractVersion:   string(contractVersion),
		LineageGeneration: lineageGeneration,
		RuntimeIdentity:   request.RuntimeIdentity,
		ConfigDigest:      request.ConfigDigest,
	}
	if request.Session.Lineage != nil {
		claim.LineageGeneration = request.Session.Lineage.LineageGeneration
	} else {
		record, err := c.transcripts.GetSession(ctx, request.Session.Namespace, request.Session.SessionName)
		if err != nil {
			return nil, fmt.Errorf("read session transcript for lineage classification: %w", err)
		}
		if record.SessionType == store.SessionTypeGateway {
			if err := c.verifyGatewayLineageOwner(ctx, request); err != nil {
				return nil, err
			}
			claim.EstablishIfAbsent = true
		} else {
			// Non-Gateway lineage can be established only before the transcript
			// contains messages. Existing unclassified transcripts fail closed.
			claim.EstablishIfAbsent = record.MessageCount == 0
		}
	}
	if err := claim.Validate(); err != nil {
		return nil, err
	}
	return claim, nil
}

func (c *ACPSessionContinuity) verifyGatewayLineageOwner(ctx context.Context, request ACPAcquireSessionLeaseRequest) error {
	if c.gatewayEvents == nil {
		return fmt.Errorf("%w: Gateway Task ownership store is unavailable", store.ErrNotReady)
	}
	event, err := c.gatewayEvents.GetGatewayEventForTask(
		ctx, request.Session.Namespace, request.TaskName, request.TaskUID,
	)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrValidation) {
		return fmt.Errorf("%w: Gateway Task ownership linkage is pending", store.ErrNotReady)
	}
	if err != nil {
		return fmt.Errorf("verify Gateway Task ownership linkage: %w", err)
	}
	if event.Namespace != request.Session.Namespace || event.SessionName != request.Session.SessionName ||
		event.NamespaceUID != request.NamespaceUID {
		return fmt.Errorf("%w: linked Gateway event does not match Session lineage identity", store.ErrConflict)
	}
	return nil
}

func (c *ACPSessionContinuity) ReleaseMutationLease(ctx context.Context, request ACPReleaseSessionLeaseRequest) (*store.SessionControl, error) {
	if err := validateACPSessionLease(&request.Lease.Session, request.Lease.Key); err != nil {
		return nil, err
	}
	turnID, err := request.Lease.Key.CanonicalID()
	if err != nil {
		return nil, err
	}
	leaseDigest := request.Lease.Session.Lease.RequestDigest
	operationID := acpPrePromptLeaseReleasePrefix + turnID
	operationDigest, err := store.SessionLeaseReleaseOperationDigest(turnID, leaseDigest)
	if err != nil {
		return nil, err
	}
	releasedAt := request.ReleasedAt.UTC()
	if releasedAt.IsZero() {
		releasedAt = time.Now().UTC()
	}
	return c.controls.ReleaseSessionMutationLease(ctx, store.ReleaseSessionMutationLeaseRequest{
		Namespace: request.Lease.Session.Namespace, SessionName: request.Lease.Session.SessionName,
		SessionUID: request.Lease.Session.SessionUID, Key: request.Lease.Key, Fence: request.Fence,
		ExpectedSessionVersion: request.Lease.Session.Version, LeaseRequestDigest: leaseDigest,
		OperationID: operationID, OperationDigest: operationDigest, ReleasedAt: releasedAt,
	})
}

func acpSessionMutationLeaseDigest(sessionUID string, leaseGeneration int64, taskUID string, attempt int64, promptID, promptRequestDigest string) (string, error) {
	return store.SessionMutationLeaseRequestDigest(
		sessionUID, leaseGeneration, taskUID, attempt, promptID, promptRequestDigest,
	)
}

// ACPOpenSessionTurnRequest opens the durable turn after the PromptAttempt has
// been bound to the returned lease, but before any prompt request is written.
type ACPOpenSessionTurnRequest struct {
	Lease                ACPSessionLease
	Fence                store.ControllerEpochFence
	PromptAttemptID      string
	PromptRequestDigest  string
	UserPrompt           string
	SkipTranscriptAppend bool
	SkipUserPromptAppend bool
	OpenedAt             time.Time
}

// ACPSessionTurn is the exact session and turn snapshot used for terminal
// finalization. A newer lease or Session version cannot finalize this turn.
type ACPSessionTurn struct {
	Lease                ACPSessionLease
	Turn                 store.SessionTurn
	SkipTranscriptAppend bool
	SkipUserPromptAppend bool
}

// OpenTurn creates the SessionTurn durability barrier that prompt execution
// must not cross before this method succeeds.
func (c *ACPSessionContinuity) OpenTurn(ctx context.Context, request ACPOpenSessionTurnRequest) (*ACPSessionTurn, error) {
	request.PromptAttemptID = strings.TrimSpace(request.PromptAttemptID)
	if err := store.ValidateControlIdentifier("prompt attempt ID", request.PromptAttemptID); err != nil {
		return nil, err
	}
	if err := store.ValidateCanonicalDigest("prompt request digest", request.PromptRequestDigest); err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.UserPrompt) == "" {
		return nil, store.ValidationErrorf("session turn user prompt is required")
	}
	if err := store.ValidateControlText("session turn user prompt", request.UserPrompt); err != nil {
		return nil, err
	}
	if request.SkipTranscriptAppend && request.SkipUserPromptAppend {
		return nil, store.ValidationErrorf("session turn cannot combine full transcript suppression with user-prompt-only suppression")
	}
	if err := validateACPSessionLease(&request.Lease.Session, request.Lease.Key); err != nil {
		return nil, err
	}
	turnID, err := request.Lease.Key.CanonicalID()
	if err != nil {
		return nil, err
	}
	turnDigest, err := acpSessionTurnDigest(
		turnID, request.PromptAttemptID, request.PromptRequestDigest, request.UserPrompt,
		request.SkipTranscriptAppend, request.SkipUserPromptAppend,
	)
	if err != nil {
		return nil, fmt.Errorf("digest ACP session turn: %w", err)
	}
	openedAt := request.OpenedAt.UTC()
	if openedAt.IsZero() {
		openedAt = time.Now().UTC()
	}
	turn, err := c.controls.CreateSessionTurn(ctx, store.CreateSessionTurnRequest{
		Turn: store.SessionTurn{
			ID: turnID, Key: request.Lease.Key, PromptAttemptID: request.PromptAttemptID,
			RequestDigest: turnDigest, UserPrompt: request.UserPrompt, CreatedAt: openedAt, UpdatedAt: openedAt,
		},
		Fence: request.Fence, ExpectedSessionVersion: request.Lease.Session.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("open ACP session turn before prompt execution: %w", err)
	}
	return &ACPSessionTurn{
		Lease: request.Lease, Turn: *turn,
		SkipTranscriptAppend: request.SkipTranscriptAppend,
		SkipUserPromptAppend: request.SkipUserPromptAppend,
	}, nil
}

func acpSessionTurnDigest(
	turnID, promptAttemptID, promptRequestDigest, userPrompt string,
	skipTranscriptAppend, skipUserPromptAppend bool,
) (string, error) {
	identity := map[string]any{
		"turnID": turnID, "promptAttemptID": promptAttemptID,
		"promptRequestDigest": promptRequestDigest, "userPrompt": userPrompt,
	}
	if skipTranscriptAppend {
		identity["skipTranscriptAppend"] = true
	}
	if skipUserPromptAppend {
		identity["skipUserPromptAppend"] = true
	}
	return acpDomainDigest("session-turn", identity)
}

// ACPFinalizationProjection is the transactional-outbox projection committed
// with the SessionTurn. The helper owns the aggregate identity and payload
// digest so callers cannot accidentally project a different turn.
type ACPFinalizationProjection struct {
	ID             string
	ProjectionKind string
	Payload        json.RawMessage
	AvailableAt    time.Time
}

// ACPFinalizeAssistantRequest finalizes a known prompt result. If publication
// is terminally ambiguous, the assistant result remains canonical but the
// Session and BranchClaim stay reconciliation-blocked.
type ACPFinalizeAssistantRequest struct {
	SessionTurn     ACPSessionTurn
	Fence           store.ControllerEpochFence
	AssistantResult string
	PublicationID   string
	Projection      ACPFinalizationProjection
	FinalizedAt     time.Time
}

// ACPFinalizeOutcomeUnknownRequest records an explicit system outcome marker.
// There is intentionally no assistant-result field, preventing invented output
// when prompt acceptance or settlement cannot be established.
type ACPFinalizeOutcomeUnknownRequest struct {
	SessionTurn   ACPSessionTurn
	Fence         store.ControllerEpochFence
	Reason        string
	PublicationID string
	Projection    ACPFinalizationProjection
	FinalizedAt   time.Time
}

// ACPFinalizeOutcomeMarkerRequest records a proven non-success terminal marker
// such as Cancelled or Failed without misclassifying it as OutcomeUnknown.
type ACPFinalizeOutcomeMarkerRequest struct {
	SessionTurn ACPSessionTurn
	Fence       store.ControllerEpochFence
	Kind        string
	Reason      string
	Projection  ACPFinalizationProjection
	FinalizedAt time.Time
}

// ACPSessionFinalization is the committed turn plus its latest Session state.
type ACPSessionFinalization struct {
	Turn    store.SessionTurn
	Session store.SessionControl
}

// ACPResumeSessionTurnFinalizationRequest identifies an already-finalized
// durable SessionTurn whose Kubernetes and outbox activation tail may have
// been interrupted by controller restart.
type ACPResumeSessionTurnFinalizationRequest struct {
	SessionTurn ACPSessionTurn
	Fence       store.ControllerEpochFence
}

// ResumeSessionTurnFinalization reloads every finalization input from the
// persisted SessionTurn/outbox records and idempotently completes the remaining
// BranchClaim, SessionControl, coordination Lease, and outbox activation steps.
func (c *ACPSessionContinuity) ResumeSessionTurnFinalization(ctx context.Context, request ACPResumeSessionTurnFinalizationRequest) (*ACPSessionFinalization, error) {
	turn := request.SessionTurn.Turn
	if turn.State != store.SessionTurnFinalized || turn.FinalizedAt == nil {
		return nil, fmt.Errorf("%w: ACP session turn is not durably finalized", store.ErrConflict)
	}
	if turn.Key != request.SessionTurn.Lease.Key || request.SessionTurn.Lease.Session.SessionUID != turn.Key.SessionUID {
		return nil, fmt.Errorf("%w: ACP session turn recovery identity does not match the captured Session", store.ErrConflict)
	}
	resumed, err := c.controls.ResumeSessionTurnFinalization(ctx, store.ResumeSessionTurnFinalizationRequest{
		Key: turn.Key, PromptAttemptID: turn.PromptAttemptID,
		FinalizationDigest: turn.FinalizationDigest, Fence: request.Fence,
	})
	if err != nil {
		return nil, fmt.Errorf("resume ACP session turn finalization: %w", err)
	}
	control, err := c.controls.GetSessionControl(ctx, request.SessionTurn.Lease.Session.Namespace, request.SessionTurn.Lease.Session.SessionName)
	if err != nil {
		return nil, fmt.Errorf("reload resumed ACP session: %w", err)
	}
	if control.SessionUID != turn.Key.SessionUID {
		return nil, fmt.Errorf("%w: resumed ACP session UID does not match the finalized turn", store.ErrConflict)
	}
	return &ACPSessionFinalization{Turn: *resumed, Session: *control}, nil
}

// FinalizeAssistantResult atomically appends the user prompt and known
// assistant result, snapshots any durable publication receipt, releases only
// the matching lease, and derives baseline advancement inside the store.
func (c *ACPSessionContinuity) FinalizeAssistantResult(ctx context.Context, request ACPFinalizeAssistantRequest) (*ACPSessionFinalization, error) {
	if err := store.ValidateControlText("assistant result", request.AssistantResult); err != nil {
		return nil, err
	}
	return c.finalizeTurn(ctx, request.SessionTurn, request.Fence, store.SessionTurnAssistantResult,
		request.AssistantResult, request.PublicationID, request.Projection, request.FinalizedAt)
}

// FinalizeOutcomeUnknown atomically appends the user prompt and an explicit
// canonical outcome marker. It never accepts or synthesizes assistant content.
func (c *ACPSessionContinuity) FinalizeOutcomeUnknown(ctx context.Context, request ACPFinalizeOutcomeUnknownRequest) (*ACPSessionFinalization, error) {
	reason := strings.TrimSpace(request.Reason)
	if reason == "" {
		return nil, store.ValidationErrorf("OutcomeUnknown reason is required")
	}
	if err := store.ValidateControlReason("OutcomeUnknown reason", reason); err != nil {
		return nil, err
	}
	marker, err := canonicalACPOutcomeUnknownMarker(reason)
	if err != nil {
		return nil, err
	}
	return c.finalizeTurn(ctx, request.SessionTurn, request.Fence, store.SessionTurnOutcomeMarker,
		marker, request.PublicationID, request.Projection, request.FinalizedAt)
}

func (c *ACPSessionContinuity) FinalizeOutcomeMarker(ctx context.Context, request ACPFinalizeOutcomeMarkerRequest) (*ACPSessionFinalization, error) {
	kind := strings.TrimSpace(request.Kind)
	reason := strings.TrimSpace(request.Reason)
	if kind == "" || reason == "" || kind == acpOutcomeUnknownKind {
		return nil, store.ValidationErrorf("terminal outcome marker kind and reason are required")
	}
	if err := store.ValidateControlReason("terminal outcome marker reason", reason); err != nil {
		return nil, err
	}
	markerBytes, err := json.Marshal(map[string]any{
		"kind": kind, "reason": reason, "assistantResultRecorded": false,
	})
	if err != nil {
		return nil, err
	}
	return c.finalizeTurn(ctx, request.SessionTurn, request.Fence, store.SessionTurnOutcomeMarker,
		string(markerBytes), "", request.Projection, request.FinalizedAt)
}

func (c *ACPSessionContinuity) finalizeTurn(
	ctx context.Context,
	sessionTurn ACPSessionTurn,
	fence store.ControllerEpochFence,
	terminalKind store.SessionTurnTerminalKind,
	terminalContent string,
	publicationID string,
	projectionInput ACPFinalizationProjection,
	finalizedAt time.Time,
) (*ACPSessionFinalization, error) {
	if err := validateACPSessionLease(&sessionTurn.Lease.Session, sessionTurn.Lease.Key); err != nil {
		return nil, err
	}
	if sessionTurn.Turn.Key != sessionTurn.Lease.Key || sessionTurn.Turn.State != store.SessionTurnOpen {
		return nil, fmt.Errorf("%w: ACP session turn is not the open turn for the captured lease", store.ErrConflict)
	}
	publicationID = strings.TrimSpace(publicationID)
	blockReason, err := c.publicationBlockReason(ctx, publicationID)
	if err != nil {
		return nil, err
	}
	projection, err := buildACPSessionTurnProjection(sessionTurn.Turn.ID, projectionInput)
	if err != nil {
		return nil, err
	}
	finalizationIdentity := map[string]any{
		"turnID": sessionTurn.Turn.ID, "terminalKind": terminalKind, "terminalContent": terminalContent,
		"publicationID": publicationID, "projectionID": projection.ID, "projectionPayloadDigest": projection.PayloadDigest,
	}
	if blockReason != "" {
		finalizationIdentity["blockReason"] = blockReason
	}
	if sessionTurn.SkipTranscriptAppend {
		finalizationIdentity["skipTranscriptAppend"] = true
	}
	if sessionTurn.SkipUserPromptAppend {
		finalizationIdentity["skipUserPromptAppend"] = true
	}
	finalizationDigest, err := acpDomainDigest("session-turn-finalization", finalizationIdentity)
	if err != nil {
		return nil, fmt.Errorf("digest ACP session turn finalization: %w", err)
	}
	when := finalizedAt.UTC()
	if when.IsZero() {
		when = time.Now().UTC()
	}
	finalized, err := c.controls.FinalizeSessionTurn(ctx, store.FinalizeSessionTurnRequest{
		Key: sessionTurn.Turn.Key, Fence: fence,
		ExpectedSessionVersion: sessionTurn.Lease.Session.Version, ExpectedTurnVersion: sessionTurn.Turn.Version,
		FinalizationDigest: finalizationDigest, TerminalKind: terminalKind, TerminalContent: terminalContent,
		SkipTranscriptAppend: sessionTurn.SkipTranscriptAppend,
		SkipUserPromptAppend: sessionTurn.SkipUserPromptAppend,
		PublicationID:        publicationID,
		// VerifiedBaseline is deliberately omitted. The durable store may advance
		// it only by deriving it from the independently verified Publication receipt.
		BlockReason: blockReason, Projection: projection, FinalizedAt: when,
	})
	if err != nil {
		return nil, fmt.Errorf("finalize ACP session turn: %w", err)
	}
	control, err := c.controls.GetSessionControl(ctx, sessionTurn.Lease.Session.Namespace, sessionTurn.Lease.Session.SessionName)
	if err != nil {
		return nil, fmt.Errorf("reload finalized ACP session: %w", err)
	}
	return &ACPSessionFinalization{Turn: *finalized, Session: *control}, nil
}

func (c *ACPSessionContinuity) publicationBlockReason(ctx context.Context, publicationID string) (string, error) {
	if publicationID == "" {
		return "", nil
	}
	if err := store.ValidateControlIdentifier("publication ID", publicationID); err != nil {
		return "", err
	}
	if c.publications == nil {
		return "", fmt.Errorf("%w: harness v1 Session finalization cannot reference a publication", store.ErrConflict)
	}
	publication, err := c.publications.GetPublication(ctx, publicationID)
	if err != nil {
		return "", fmt.Errorf("load ACP session publication: %w", err)
	}
	if !store.IsTerminalPublicationState(publication.State) {
		return "", fmt.Errorf("%w: publication %q is not terminal", store.ErrConflict, publication.ID)
	}
	switch publication.State {
	case store.PublicationVerifiedExact:
		if publication.PreparedReceipt == nil || publication.VerificationReceipt == nil ||
			publication.VerificationReceipt.Outcome != store.PublicationVerifiedExact ||
			publication.VerificationReceipt.ObservedRemote.SHA != publication.PreparedReceipt.CommitSHA {
			return "", store.ValidationErrorf("verified publication %q lacks an independent exact receipt", publication.ID)
		}
	case store.PublicationDeliveredSuperseded:
		if publication.VerificationReceipt == nil || publication.VerificationReceipt.Outcome != store.PublicationDeliveredSuperseded ||
			publication.VerificationReceipt.ObservedRemote.Absent || publication.VerificationReceipt.ObservedRemote.SHA == "" ||
			publication.VerificationReceipt.DescendantProofDigest == "" {
			return "", store.ValidationErrorf("superseded publication %q lacks an independent descendant receipt", publication.ID)
		}
	case store.PublicationOutcomeUnknown, store.PublicationDeliveryConflict:
		reason := strings.TrimSpace(publication.TerminalReason)
		if reason == "" {
			reason = fmt.Sprintf("publication %s remains unresolved", publication.ID)
		}
		return reason, nil
	}
	return "", nil
}

func buildACPSessionTurnProjection(turnID string, input ACPFinalizationProjection) (store.OutboxProjection, error) {
	input.ID = strings.TrimSpace(input.ID)
	input.ProjectionKind = strings.TrimSpace(input.ProjectionKind)
	if input.ProjectionKind == "" {
		return store.OutboxProjection{}, store.ValidationErrorf("session finalization projection kind is required")
	}
	if !json.Valid(input.Payload) {
		return store.OutboxProjection{}, store.ValidationErrorf("session finalization projection payload must be valid JSON")
	}
	if input.ID == "" {
		input.ID = store.CanonicalControlID("outbox", turnID, input.ProjectionKind)
	}
	return store.OutboxProjection{
		ID: input.ID, AggregateKind: acpSessionTurnAggregateKind, AggregateID: turnID,
		ProjectionKind: input.ProjectionKind, Payload: append(json.RawMessage(nil), input.Payload...),
		PayloadDigest: canonicalACPPayloadDigest(input.Payload), AvailableAt: input.AvailableAt,
	}, nil
}

func canonicalACPPayloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type acpOutcomeUnknownMarker struct {
	Kind                    string `json:"kind"`
	Reason                  string `json:"reason"`
	AssistantResultRecorded bool   `json:"assistantResultRecorded"`
}

func canonicalACPOutcomeUnknownMarker(reason string) (string, error) {
	canonical, err := json.Marshal(acpOutcomeUnknownMarker{
		Kind: acpOutcomeUnknownKind, Reason: reason, AssistantResultRecorded: false,
	})
	if err != nil {
		return "", fmt.Errorf("encode OutcomeUnknown marker: %w", err)
	}
	return string(canonical), nil
}

func validateACPSessionLease(control *store.SessionControl, key store.SessionTurnKey) error {
	if control == nil || control.Lease == nil {
		return fmt.Errorf("%w: ACP session mutation lease is not active", store.ErrConflict)
	}
	if err := key.Validate(); err != nil {
		return err
	}
	lease := control.Lease
	if control.SessionUID != key.SessionUID || control.LeaseGeneration != key.LeaseGeneration ||
		lease.Generation != key.LeaseGeneration || lease.TaskUID != key.TaskUID ||
		lease.Attempt != key.Attempt || lease.PromptID != key.PromptID {
		return fmt.Errorf("%w: ACP session mutation lease does not match the turn fence", store.ErrConflict)
	}
	return nil
}

func newACPSessionUID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "acp-session-" + hex.EncodeToString(entropy[:]), nil
}
