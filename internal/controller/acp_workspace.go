package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/publisher"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

const defaultACPSourceBranch = "main"

const workspaceRepositoryProviderGitHub = "github"

var errWorkspaceRepositoryHTTPSPort = errors.New("repository HTTPS URL must use port 443")

type preparedACPRuntimeWorkspace struct {
	baseline       harnessv2.WorkspaceBaseline
	spec           harnessv2.WorkspaceSpec
	authorization  *harnessv2.ArtifactAuthorization
	bindingDigest  string
	createIssuedAt time.Time
	// priorRepositoryIdentity records the canonical repository identity the
	// session ran on BEFORE a verified publication transition moved its
	// continuation to a new repository. Under an expected durable resume it
	// authorizes the supervisor to wipe a checkpoint bound to exactly this
	// identity and re-materialize from the new baseline.
	priorRepositoryIdentity string
}

// taskExpectsDurableResume reports whether the Task's linked execution
// workspace carries a resumed lineage: every session on it must find a
// committed durable checkpoint, and the runtime fails creation when the
// preserved data is missing instead of silently materializing fresh. The
// returned floor is the newest committed checkpoint generation the
// controller ever recorded; a same-identity checkpoint older than it is a
// stale provider restore.
func (d *ACPDispatcher) taskExpectsDurableResume(ctx context.Context, task *corev1alpha1.Task) (bool, uint64, error) {
	name := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel])
	uid := strings.TrimSpace(task.Annotations[acpExecutionWorkspaceUIDAnnotation])
	if name == "" || uid == "" {
		return false, 0, nil
	}
	reader := client.Reader(d.Client)
	if d.APIReader != nil {
		reader = d.APIReader
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		// Fail closed: silently omitting the resume expectation on a read
		// failure would let a lost checkpoint be silently replaced by a
		// fresh materialization. The dispatch aborts and retries instead.
		return false, 0, fmt.Errorf("resolve the linked workspace's resume lineage: %w", err)
	}
	// The lineage asserts a committed checkpoint only when a RuntimeSession
	// was actually created (and therefore committed its durable marker) on
	// this workspace before the suspension: a first Task cancelled before
	// session creation validly suspends a volume that never held a
	// checkpoint, and its continuation must materialize fresh instead of
	// failing closed forever over data that never existed.
	if string(workspace.UID) != uid ||
		workspace.Annotations[acpWorkspaceResumedLineageAnnotation] != booleanTrueValue {
		return false, 0, nil
	}
	floor, committed, err := workspaceDurableSessionGeneration(workspace)
	if err != nil {
		return false, 0, err
	}
	if !committed {
		return false, 0, nil
	}
	return true, floor, nil
}

// taskRuntimeSessionGenerationFloor returns the newest RuntimeSession
// generation committed on the linked workspace. Session planning uses this
// durable high-water mark when its controller-local binding cache cannot prove
// that reusing a generation is safe.
func (d *ACPDispatcher) taskRuntimeSessionGenerationFloor(ctx context.Context, task *corev1alpha1.Task) (uint64, error) {
	name := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel])
	uid := strings.TrimSpace(task.Annotations[acpExecutionWorkspaceUIDAnnotation])
	if name == "" || uid == "" {
		return 0, nil
	}
	reader := client.Reader(d.Client)
	if d.APIReader != nil {
		reader = d.APIReader
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		return 0, fmt.Errorf("resolve the linked workspace's RuntimeSession generation floor: %w", err)
	}
	if string(workspace.UID) != uid {
		return 0, nil
	}
	floor, _, err := workspaceDurableSessionGeneration(workspace)
	return floor, err
}

func workspaceDurableSessionGeneration(workspace *workspacev1alpha1.ExecutionWorkspace) (uint64, bool, error) {
	recorded := strings.TrimSpace(workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation])
	if recorded == "" {
		return 0, false, nil
	}
	if recorded == booleanTrueValue {
		// A record stamped before generations were tracked asserts the
		// checkpoint's existence without a generation floor.
		return 0, true, nil
	}
	floor, parseErr := strconv.ParseUint(recorded, 10, 64)
	if parseErr != nil || floor == 0 {
		// A corrupt controller-owned record must not disable the stale-snapshot
		// fence. Keep the raw annotation out of the error because metadata can be
		// modified outside this controller.
		return 0, false, fmt.Errorf("linked workspace %s has an invalid durable checkpoint generation record", workspace.Name)
	}
	return floor, true, nil
}

// markLinkedWorkspaceDurableSessionCommitted durably records on the linked
// execution workspace that a RuntimeSession creation completed - and with it
// the supervisor's durable checkpoint commit - so a later resumed lineage can
// assert the committed checkpoint's existence.
func (d *ACPDispatcher) markLinkedWorkspaceDurableSessionCommitted(ctx context.Context, task *corev1alpha1.Task, generation uint64) error {
	name := strings.TrimSpace(task.Labels[acpExecutionWorkspaceLinkLabel])
	uid := strings.TrimSpace(task.Annotations[acpExecutionWorkspaceUIDAnnotation])
	if name == "" || uid == "" {
		return nil
	}
	workspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, workspace); err != nil {
		return client.IgnoreNotFound(err)
	}
	if string(workspace.UID) != uid {
		return nil
	}
	// The record carries the NEWEST committed checkpoint generation and only
	// advances: a stale provider restore then presents an older recorded
	// generation than this floor and the resume assertion rejects it.
	recorded := strings.TrimSpace(workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation])
	if current, parseErr := strconv.ParseUint(recorded, 10, 64); parseErr == nil && current >= generation {
		return nil
	}
	base := workspace.DeepCopy()
	if workspace.Annotations == nil {
		workspace.Annotations = map[string]string{}
	}
	workspace.Annotations[acpWorkspaceDurableSessionCommittedAnnotation] = strconv.FormatUint(generation, 10)
	if err := d.Client.Patch(ctx, workspace, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

//nolint:gocyclo // Workspace continuation, clean-room preparation, and exact authorization checks are audited together.
func (d *ACPDispatcher) prepareRuntimeWorkspace(
	ctx context.Context,
	task *corev1alpha1.Task,
	fence store.ControllerEpochFence,
	session *acpTaskSession,
	plannedAt time.Time,
	runtimeSessionReused bool,
) (preparedACPRuntimeWorkspace, error) {
	if plannedAt.IsZero() {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("RuntimeSession creation timestamp is required for workspace authorization")
	}
	plannedAt = plannedAt.UTC()
	workspace := task.Spec.Workspace
	priorRepositoryIdentity := ""
	if session != nil && session.VerifiedBaseline != nil {
		continuationWorkspace, err := runtimeWorkspaceForSessionContinuation(workspace, session.VerifiedBaseline)
		if err != nil {
			return preparedACPRuntimeWorkspace{}, err
		}
		if workspace != nil && continuationWorkspace != nil &&
			!sameCanonicalWorkspaceRepository(workspace.GitRepo, continuationWorkspace.GitRepo) {
			// The verified publication transition moved the session to a new
			// repository (for example the fork a PR publishes to); the prior
			// canonical identity authorizes the durable-resume checkpoint
			// transition below.
			if prior, priorErr := workspaceRepository(workspace); priorErr == nil {
				priorRepositoryIdentity = prior.ID
			}
		}
		workspace = continuationWorkspace
	}
	if workspace == nil || strings.TrimSpace(workspace.GitRepo) == "" {
		// The protocol baseline must be identical for every turn of one
		// logical Session: the supervisor compares the delta request's
		// VerifiedBaseline against the baseline the session was created with,
		// so a task-scoped identity would fail every repo-less continuation
		// with a digest conflict.
		scope := string(task.UID)
		if session != nil && strings.TrimSpace(session.Binding.SessionUID) != "" {
			scope = session.Binding.SessionUID
		}
		baseline, spec, err := emptyRuntimeWorkspace(task, scope)
		if err != nil {
			return preparedACPRuntimeWorkspace{}, err
		}
		// Keep the stable Session-scoped baseline in the binding digest. Older
		// controllers erased the task-scoped identity here; making the Session
		// identity explicit forces those live sessions through one generation
		// rotation instead of reusing a supervisor with a different baseline.
		bindingDigest, err := acpRuntimeWorkspaceBindingDigest("", spec)
		if err != nil {
			return preparedACPRuntimeWorkspace{}, err
		}
		return preparedACPRuntimeWorkspace{
			baseline: baseline, spec: spec, bindingDigest: bindingDigest, createIssuedAt: plannedAt,
		}, nil
	}
	if d.Publisher == nil || len(d.ArtifactCapabilitySecret) < artifactcap.MinSecretBytes {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("clean-room Workspace/Publisher and artifact authorization are required")
	}
	repository, err := workspaceRepository(workspace)
	if err != nil {
		return preparedACPRuntimeWorkspace{}, err
	}
	if session != nil && session.VerifiedBaseline != nil && !sameWorkspaceRepositoryIdentity(repository.ID, session.VerifiedBaseline.RepositoryID) {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("session workspace repository does not match the verified publication baseline")
	}
	sourceRef, err := runtimeWorkspaceSourceRef(workspace)
	if err != nil {
		return preparedACPRuntimeWorkspace{}, err
	}
	credentialRole := publisherservice.CredentialRoleSourceRead
	if session != nil && session.VerifiedBaseline != nil {
		credentialRole = publisherservice.CredentialRoleTargetRead
	}
	credential := publisherCredentialReference(workspace.ReadCredentialRef, credentialRole)
	metadata := publisherservice.OperationMetadata{
		Namespace: task.Namespace, TaskID: string(task.UID), OperationID: "workspace-resolve-" + task.Status.Execution.PromptID,
	}
	resolveRequest := publisherservice.WorkspaceResolveRequest{
		Metadata: metadata, Source: repository, SourceRef: sourceRef, CredentialRef: credential,
	}
	resolveIdentity := store.ExternalEffectIdentity{
		Kind: "workspace.resolve", Namespace: task.Namespace, AggregateID: string(task.UID), OperationID: metadata.OperationID,
	}
	resolved, err := runACPExternalEffectWithRetry(ctx, d, fence, resolveIdentity, resolveRequest, func(callCtx context.Context) (publisherservice.WorkspaceResolveResponse, error) {
		return d.Publisher.ResolveWorkspace(callCtx, resolveRequest)
	})
	if err != nil {
		if settleErr := settleACPExternalEffectError(ctx, d, fence, resolveIdentity, err); settleErr != nil {
			return preparedACPRuntimeWorkspace{}, fmt.Errorf("settle failed workspace resolve effect: %w", settleErr)
		}
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("resolve source workspace: %w", err)
	}
	if session != nil && session.VerifiedBaseline != nil {
		if resolved.BaselineOID != session.VerifiedBaseline.SHA {
			return preparedACPRuntimeWorkspace{}, fmt.Errorf("session workspace baseline moved from %s to %s", session.VerifiedBaseline.SHA, resolved.BaselineOID)
		}
	}
	metadata.OperationID = "workspace-prepare-" + task.Status.Execution.PromptID
	prepareRequest := publisherservice.WorkspacePrepareRequest{
		Metadata: metadata, Source: repository, SourceRef: resolved.SourceRef, BaselineOID: resolved.BaselineOID,
		CredentialRef: credential,
	}
	prepareIdentity := store.ExternalEffectIdentity{
		Kind: "workspace.prepare", Namespace: task.Namespace, AggregateID: string(task.UID), OperationID: metadata.OperationID,
	}
	prepared, err := runACPExternalEffectWithRetry(ctx, d, fence, prepareIdentity, prepareRequest, func(callCtx context.Context) (publisherservice.WorkspacePrepareResponse, error) {
		return d.Publisher.PrepareWorkspace(callCtx, prepareRequest)
	})
	if err != nil {
		if settleErr := settleACPExternalEffectError(ctx, d, fence, prepareIdentity, err); settleErr != nil {
			return preparedACPRuntimeWorkspace{}, fmt.Errorf("settle failed workspace prepare effect: %w", settleErr)
		}
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("prepare source workspace: %w", err)
	}
	prepareCommittedAt, err := externalEffectSucceededAt(ctx, d.Store, prepareIdentity)
	if err != nil {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("load committed workspace prepare effect: %w", err)
	}
	createIssuedAt := plannedAt
	if prepareCommittedAt.After(createIssuedAt) {
		createIssuedAt = prepareCommittedAt
	}
	baseline := harnessv2.WorkspaceBaseline{
		RepositoryIdentity: repository.ID, Revision: resolved.BaselineOID,
		TreeDigest: prepared.ManifestDigest, Artifact: &prepared.Artifact,
	}
	spec := harnessv2.WorkspaceSpec{
		Intent: harnessv2.WorkspaceIntent(effectiveACPWorkspaceIntent(task)), Baseline: baseline,
		RelativeRoot: strings.TrimSpace(workspace.SubPath),
	}
	bindingDigest, err := acpRuntimeWorkspaceBindingDigest(resolved.SourceRef, spec)
	if err != nil {
		return preparedACPRuntimeWorkspace{}, err
	}
	result := preparedACPRuntimeWorkspace{
		baseline: baseline, spec: spec, bindingDigest: bindingDigest,
		createIssuedAt: createIssuedAt, priorRepositoryIdentity: priorRepositoryIdentity,
	}
	if runtimeSessionReused || session != nil && session.Reused {
		return result, nil
	}

	// The operation ID must be unique per session creation attempt: a
	// RuntimeSession recreated after a completed or ambiguously-failed first
	// download (capacity loss, post-download creation failure) needs a fresh
	// ledger operation, or the replacement session hits ErrReplay and can
	// never materialize its workspace. Bind the ID to the execution attempt
	// and session generation, which advance across recreations.
	binding := artifactcap.OperationRequest{
		Operation: artifactcap.OperationDownload, ObjectDigest: prepared.Artifact.Digest,
		Identity:      artifactcap.Identity{Namespace: task.Namespace, TaskID: string(task.UID)},
		ContentLength: prepared.Artifact.SizeBytes, MediaType: prepared.Artifact.MediaType,
		OperationID: fmt.Sprintf("runtime-workspace-download-%s-a%d-g%d",
			task.Status.Execution.PromptID, task.Status.Execution.Attempt, task.Status.Execution.RuntimeSessionGeneration),
	}
	authorizedAt := result.createIssuedAt
	const capabilityTTL = artifactcap.MaxCapabilityTTL
	authorization, err := artifactcap.Issue(d.ArtifactCapabilitySecret, binding, authorizedAt, capabilityTTL)
	if err != nil {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("authorize runtime workspace download: %w", err)
	}
	if d.ArtifactReservations == nil {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("artifact capability reservation recorder is required")
	}
	if err := d.ArtifactReservations.Reserve(ctx, binding, authorizedAt.Add(capabilityTTL+artifactcap.MaxClockSkew)); err != nil {
		return preparedACPRuntimeWorkspace{}, fmt.Errorf("reserve runtime workspace download: %w", err)
	}
	result.authorization = &harnessv2.ArtifactAuthorization{
		Capability: authorization.Capability, RequestDigest: authorization.RequestDigest,
	}
	return result, nil
}

func externalEffectSucceededAt(
	ctx context.Context,
	effects store.ExternalEffectStore,
	identity store.ExternalEffectIdentity,
) (time.Time, error) {
	if effects == nil {
		return time.Time{}, fmt.Errorf("external-effect store is required")
	}
	var (
		effect *store.ExternalEffect
		err    error
	)
	if reader, ok := effects.(store.ExternalEffectIdentityReader); ok {
		effect, err = reader.GetExternalEffectByIdentity(ctx, identity)
	} else {
		id, idErr := identity.CanonicalID()
		if idErr != nil {
			return time.Time{}, idErr
		}
		effect, err = effects.GetExternalEffect(ctx, id)
	}
	if err != nil {
		return time.Time{}, err
	}
	if effect.State != store.ExternalEffectSucceeded || effect.UpdatedAt.IsZero() {
		return time.Time{}, fmt.Errorf("external effect %s lacks a durable success timestamp", effect.ID)
	}
	return effect.UpdatedAt.UTC(), nil
}

func acpRuntimeWorkspaceBindingDigest(sourceRef string, workspace harnessv2.WorkspaceSpec) (string, error) {
	return acpDomainDigest("runtime-session-workspace-binding", map[string]any{
		"repositoryIdentity": strings.TrimSpace(workspace.Baseline.RepositoryIdentity),
		"sourceRef":          strings.TrimSpace(sourceRef),
		"revision":           strings.TrimSpace(workspace.Baseline.Revision),
		"treeDigest":         strings.TrimSpace(workspace.Baseline.TreeDigest),
		"intent":             workspace.Intent,
		"relativeRoot":       strings.TrimSpace(workspace.RelativeRoot),
	})
}

func runtimeWorkspaceSourceRef(workspace *corev1alpha1.WorkspaceConfig) (string, error) {
	if workspace == nil {
		return "", fmt.Errorf("workspace is required")
	}
	if ref := strings.TrimSpace(workspace.Ref); ref != "" {
		canonical, err := publisherservice.CanonicalWorkspaceSourceRef(ref)
		if err != nil {
			return "", fmt.Errorf("workspace source ref is invalid: %w", err)
		}
		return canonical, nil
	}
	branch := strings.TrimSpace(workspace.Branch)
	if branch == "" {
		// Empty is an explicit request for the Publisher to resolve and freeze
		// the repository's advertised HEAD/default branch before execution.
		return "", nil
	}
	if !strings.HasPrefix(branch, "refs/") {
		branch = "refs/heads/" + branch
	}
	canonical, err := publisherservice.CanonicalWorkspaceSourceRef(branch)
	if err != nil {
		return "", fmt.Errorf("workspace source branch is invalid: %w", err)
	}
	return canonical, nil
}

func runtimeWorkspaceForSessionContinuation(workspace *corev1alpha1.WorkspaceConfig, baseline *store.VerifiedBranchBaseline) (*corev1alpha1.WorkspaceConfig, error) {
	if workspace == nil {
		return nil, fmt.Errorf("session continuation has a verified branch baseline but Task.spec.workspace is missing")
	}
	if baseline == nil {
		return workspace, nil
	}
	if err := validateSessionExpectedRemoteSHA(workspace, baseline); err != nil {
		return nil, err
	}
	copyWorkspace := *workspace
	publicationGitRepo := strings.TrimSpace(copyWorkspace.PublicationGitRepo)
	if publicationGitRepo != "" {
		sourceGitRepo := copyWorkspace.GitRepo
		copyWorkspace.GitRepo = copyWorkspace.PublicationGitRepo
		if copyWorkspace.PublicationRepository == nil && !sameCanonicalWorkspaceRepository(sourceGitRepo, publicationGitRepo) {
			copyWorkspace.SourceRepository = nil
		}
	}
	if copyWorkspace.PublicationRepository != nil {
		copyIdentity := *copyWorkspace.PublicationRepository
		copyWorkspace.SourceRepository = &copyIdentity
	}
	copyWorkspace.ReadCredentialRef = copyWorkspace.PublicationReadCredentialRef
	copyWorkspace.Branch = ""
	copyWorkspace.Ref = baseline.Ref
	return &copyWorkspace, nil
}

func sameWorkspaceRepositoryIdentity(first, second string) bool {
	first = strings.TrimSpace(first)
	second = strings.TrimSpace(second)
	if first == second {
		return true
	}
	return strings.HasPrefix(strings.ToLower(first), "github.com/") &&
		strings.HasPrefix(strings.ToLower(second), "github.com/") &&
		strings.EqualFold(first, second)
}

func sameCanonicalWorkspaceRepository(first, second string) bool {
	_, firstID, firstErr := canonicalWorkspaceRepositoryURL(strings.TrimSpace(first))
	_, secondID, secondErr := canonicalWorkspaceRepositoryURL(strings.TrimSpace(second))
	if firstErr == nil && secondErr == nil {
		return firstID == secondID
	}
	return strings.TrimSpace(first) == strings.TrimSpace(second)
}

func validateSessionExpectedRemoteSHA(workspace *corev1alpha1.WorkspaceConfig, baseline *store.VerifiedBranchBaseline) error {
	if baseline == nil {
		return nil
	}
	if workspace == nil {
		return fmt.Errorf("session continuation has a verified branch baseline but Task.spec.workspace is missing")
	}
	if err := store.ValidateGitObjectID("verified session baseline", baseline.SHA); err != nil {
		return err
	}
	if expected := strings.TrimSpace(workspace.ExpectedRemoteSHA); expected != "" && expected != baseline.SHA {
		return fmt.Errorf("workspace expectedRemoteSHA conflicts with the verified session baseline")
	}
	return nil
}

func workspaceRepository(workspace *corev1alpha1.WorkspaceConfig) (publisher.Repository, error) {
	rawURL := strings.TrimSpace(workspace.GitRepo)
	parsed, derivedID, err := canonicalWorkspaceRepositoryURL(rawURL)
	if err != nil {
		return publisher.Repository{}, fmt.Errorf("workspace gitRepo: %w", err)
	}
	provider := workspaceRepositoryProviderGitHub
	if workspace.SourceRepository != nil {
		provider = strings.ToLower(strings.TrimSpace(workspace.SourceRepository.Provider))
		id := strings.TrimSpace(workspace.SourceRepository.ID)
		if provider != workspaceRepositoryProviderGitHub || !sameWorkspaceRepositoryIdentity(id, derivedID) {
			return publisher.Repository{}, fmt.Errorf("workspace sourceRepository must match the canonical credential-free URL identity %q", derivedID)
		}
	}
	return publisher.Repository{Provider: provider, ID: derivedID, URL: parsed.String()}, nil
}

func canonicalWorkspaceRepositoryURL(rawURL string) (*url.URL, string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User != nil || parsed.Scheme != urlSchemeHTTPS || parsed.Host == "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, "", fmt.Errorf("repository must be a credential-free HTTPS URL without query or fragment")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return nil, "", errWorkspaceRepositoryHTTPSPort
	}
	// Mirror the Publisher's validateRepository IP-literal rule so a Task the
	// Publisher would unconditionally reject fails preflight instead of
	// settling as WorkspaceUnsupported after creation.
	if ip := net.ParseIP(strings.ToLower(parsed.Hostname())); ip != nil &&
		(ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()) {
		return nil, "", fmt.Errorf("repository URL uses a forbidden IP literal")
	}
	if parsed.RawPath != "" && parsed.EscapedPath() != parsed.Path {
		return nil, "", fmt.Errorf("repository URL escaped path is non-canonical")
	}
	cleaned := strings.TrimSuffix(strings.TrimPrefix(path.Clean(parsed.Path), "/"), ".git")
	if cleaned == "" || cleaned == "." || parsed.Path == "/" || path.Clean(parsed.Path) != parsed.Path {
		return nil, "", fmt.Errorf("repository URL path is invalid")
	}
	parsed.Scheme = urlSchemeHTTPS
	parsed.Host = strings.ToLower(parsed.Host)
	identityPath := cleaned
	if parsed.Hostname() == "github.com" {
		identityPath = strings.ToLower(identityPath)
	}
	derivedID := parsed.Hostname() + "/" + identityPath
	return parsed, derivedID, nil
}

func publisherCredentialReference(reference *corev1alpha1.WorkspaceCredentialReference, roles ...publisherservice.CredentialRole) *publisherservice.CredentialReference {
	if reference == nil || strings.TrimSpace(reference.Name) == "" {
		return nil
	}
	var role publisherservice.CredentialRole
	if len(roles) > 0 {
		role = roles[0]
	}
	return &publisherservice.CredentialReference{Name: strings.TrimSpace(reference.Name), Kind: publisherservice.CredentialHTTPExtraHeader, Role: role}
}

func publisherForgeCredentialReference(reference *corev1alpha1.WorkspaceCredentialReference) *publisherservice.CredentialReference {
	if reference == nil || strings.TrimSpace(reference.Name) == "" {
		return nil
	}
	return &publisherservice.CredentialReference{
		Name: strings.TrimSpace(reference.Name), Kind: publisherservice.CredentialForgeToken, Role: publisherservice.CredentialRoleForge,
	}
}
