package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

const conformanceMaxWorkspaceEntries uint32 = 4096

type lifecycleProbeState struct {
	client               *harnessv2.Client
	httpClient           *http.Client
	target               Target
	sessionID            harnessv2.RuntimeSessionID
	sessionFence         harnessv2.Fence
	workspace            harnessv2.WorkspaceSpec
	taskUID              harnessv2.TaskUID
	promptID             harnessv2.PromptID
	lease                harnessv2.PromptLease
	sessionCreated       bool
	promptStarted        bool
	cancelAttempted      bool
	deleteAttempted      bool
	publicationFinalized bool
}

func probeLifecycle(
	ctx context.Context,
	httpClient *http.Client,
	client *harnessv2.Client,
	target Target,
	status *harnessv2.StatusResponse,
) error {
	if status == nil {
		return fmt.Errorf("hostile lifecycle probe requires authenticated status")
	}
	probeID, err := newProbeID()
	if err != nil {
		return fmt.Errorf("allocate conformance identity: %w", err)
	}
	// Cancellation retires a session. Prove workspace validation and
	// publication on a separate, successfully completed prompt first.
	if target.WorkspaceGovernance.Strict() {
		workspaceProbeID := "workspace-" + probeID
		workspaceState := newLifecycleProbeState(httpClient, client, target, status.Fence, workspaceProbeID)
		if err := workspaceState.probeWorkspaceLifecycle(ctx, status, workspaceProbeID); err != nil {
			return err
		}
	}
	state := newLifecycleProbeState(httpClient, client, target, status.Fence, probeID)
	defer state.cleanup(ctx)
	if err := probeStaleInstanceFence(ctx, client, target, status.Fence, state.workspace, state.taskUID, probeID); err != nil {
		return err
	}
	if err := state.probeSessionCreation(ctx, status, probeID); err != nil {
		return err
	}
	if target.WorkspaceGovernance.Strict() {
		if err := state.probePromptScopedAuthorizationRejection(ctx, probeID); err != nil {
			return err
		}
	}
	// Check the generation while the session is still idle. Cancellation may
	// retire it before the controller can issue an explicit deletion.
	deleteRequest, err := state.deleteSessionRequest("delete-session-" + probeID)
	if err != nil {
		return err
	}
	if err := state.probeStaleSessionDeletion(ctx, deleteRequest); err != nil {
		return err
	}
	promptRequest, settlement, err := state.probePromptCancellationLifecycle(ctx, probeID)
	if err != nil {
		return err
	}
	completed := settlement.TerminalEvent == harnessv2.EventCompleted
	// A prompt that finishes before cancellation still owes workspace
	// validation and publication finalization before its session can retire.
	if completed && target.WorkspaceGovernance.Strict() {
		if err := state.probeCompletedPromptWorkspace(ctx, probeID, promptRequest.Metadata, settlement); err != nil {
			return err
		}
	}
	return state.probeSessionDeletion(ctx, probeID, !completed)
}

func newLifecycleProbeState(
	httpClient *http.Client,
	client *harnessv2.Client,
	target Target,
	fence harnessv2.Fence,
	probeID string,
) *lifecycleProbeState {
	sessionID := harnessv2.RuntimeSessionID("conformance-session-" + probeID)
	sessionUID := harnessv2.RuntimeSessionUID("conformance-session-uid-" + probeID)
	taskUID := harnessv2.TaskUID("conformance-task-" + probeID)
	promptID := harnessv2.PromptID("conformance-prompt-" + probeID)

	fence.RuntimeSessionUID = sessionUID
	fence.RuntimeSessionGeneration = 1
	workspace := harnessv2.WorkspaceSpec{
		Intent: target.Profile.WorkspaceIntent,
		Baseline: harnessv2.WorkspaceBaseline{
			RepositoryIdentity: "orka-conformance.invalid/repository",
			Revision:           "conformance-baseline",
			TreeDigest:         digestString("conformance-baseline-" + probeID),
		},
	}
	return &lifecycleProbeState{
		client: client, httpClient: httpClient, target: target,
		sessionID: sessionID, sessionFence: fence, workspace: workspace,
		taskUID: taskUID, promptID: promptID,
	}
}

func (s *lifecycleProbeState) probeWorkspaceLifecycle(ctx context.Context, status *harnessv2.StatusResponse, probeID string) error {
	defer s.cleanup(ctx)
	if err := s.probeSessionCreation(ctx, status, probeID); err != nil {
		return err
	}
	promptRequest, settlement, err := s.probeCompletedPromptLifecycle(ctx, probeID)
	if err != nil {
		return err
	}
	if err := s.probeCompletedPromptWorkspace(ctx, probeID, promptRequest.Metadata, settlement); err != nil {
		return err
	}
	return s.probeSessionDeletion(ctx, probeID, false)
}

func (s *lifecycleProbeState) probeCompletedPromptWorkspace(
	ctx context.Context,
	probeID string,
	metadata harnessv2.MutationMetadata,
	settlement harnessv2.PromptSettlement,
) error {
	delta, err := s.probeWorkspaceDelta(ctx, probeID, metadata, settlement)
	if err != nil {
		return err
	}
	if s.target.SupportsPublicationFinalization && delta.Delta.State == harnessv2.WorkspaceDeltaPrepared {
		if err := s.probePublicationFinalization(ctx, probeID, metadata, delta.Delta); err != nil {
			return err
		}
	}
	return nil
}

func (s *lifecycleProbeState) probeSessionCreation(
	ctx context.Context,
	status *harnessv2.StatusResponse,
	probeID string,
) error {
	request, err := s.createSessionRequest("create-session-" + probeID)
	if err != nil {
		return err
	}
	created, err := s.client.CreateRuntimeSession(ctx, request)
	if err != nil {
		return fmt.Errorf("create conformance runtime session: %w", err)
	}
	s.sessionCreated = true
	if created.Classification.Class != harnessv2.RequestClassificationFresh {
		return fmt.Errorf("new conformance session classified as %q, want fresh", created.Classification.Class)
	}
	if created.Session.RuntimeInstanceID != status.Fence.RuntimeInstanceID ||
		created.Session.SupervisorBootID != status.Fence.SupervisorBootID {
		return fmt.Errorf("created session is not bound to the authenticated runtime instance and boot")
	}
	if s.target.WorkspaceGovernance.DuplicateSafeMutations {
		return s.probeCreateSessionReplay(ctx, request, created)
	}
	return nil
}

func (s *lifecycleProbeState) probeCompletedPromptLifecycle(
	ctx context.Context,
	probeID string,
) (harnessv2.StartPromptRequest, harnessv2.PromptSettlement, error) {
	request, err := s.startPromptRequest("start-prompt-" + probeID)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	request.Input.Content[0].Text = "Orka harness conformance: respond with ok and perform no external side effects."
	request.Input.Metadata["orka.conformance"] = "complete-for-workspace"
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	s.promptStarted = true
	stream, err := s.client.StartPrompt(ctx, s.sessionID, request)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("open workspace probe prompt stream: %w", err)
	}
	defer stream.Close() //nolint:errcheck
	result := consumePromptStream(stream)
	if result.err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("consume workspace probe prompt stream: %w", result.err)
	}
	if result.terminal == nil || result.terminal.Type != harnessv2.EventCompleted {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("workspace probe prompt did not complete successfully")
	}
	streamDone := make(chan promptStreamResult, 1)
	streamDone <- result
	// Replaying cancellation and settlement is safe while the completed
	// session awaits validation. A canceled session may already be retired.
	return s.probeCancellationSettlement(ctx, probeID, request, streamDone, true)
}

func (s *lifecycleProbeState) probePromptCancellationLifecycle(
	ctx context.Context,
	probeID string,
) (harnessv2.StartPromptRequest, harnessv2.PromptSettlement, error) {
	request, err := s.startPromptRequest("start-prompt-" + probeID)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	s.promptStarted = true
	stream, err := s.client.StartPrompt(ctx, s.sessionID, request)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("open single non-reconnectable prompt stream: %w", err)
	}
	defer stream.Close() //nolint:errcheck

	accepted, err := stream.Decode()
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("read prompt acceptance event: %w", err)
	}
	if accepted.Type != harnessv2.EventAccepted {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("first prompt event is %q, want %q", accepted.Type, harnessv2.EventAccepted)
	}

	streamDone := make(chan promptStreamResult, 1)
	go func() {
		streamDone <- consumePromptStream(stream)
	}()
	var replay promptReplayObservation
	if s.target.WorkspaceGovernance.DuplicateSafeMutations {
		if accepted.Accepted == nil {
			return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("accepted prompt event omitted acceptance metadata")
		}
		replay, err = s.probeAcceptedPromptReplay(ctx, request, accepted.Accepted.AcceptedAt)
		if err != nil {
			return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
		}
	}
	request, settlement, err := s.probeCancellationSettlement(ctx, probeID, request, streamDone, false)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	if err := replay.validateSettlement(settlement); err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	return request, settlement, nil
}

func (s *lifecycleProbeState) probeCancellationSettlement(
	ctx context.Context,
	probeID string,
	request harnessv2.StartPromptRequest,
	streamDone <-chan promptStreamResult,
	verifyReplay bool,
) (harnessv2.StartPromptRequest, harnessv2.PromptSettlement, error) {
	cancelRequest, err := s.cancelPromptRequest("cancel-prompt-"+probeID, request.Metadata)
	if err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	s.cancelAttempted = true
	cancelResponse, cancelErr := s.client.CancelPrompt(ctx, s.sessionID, cancelRequest)

	var streamResult promptStreamResult
	select {
	case streamResult = <-streamDone:
	case <-ctx.Done():
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("wait for original prompt settlement: %w", ctx.Err())
	}
	if streamResult.err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("consume original prompt stream without reconnect: %w", streamResult.err)
	}
	if cancelErr != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("cancel accepted prompt without replay: %w", cancelErr)
	}
	if cancelResponse == nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, fmt.Errorf("cancel endpoint returned no settlement")
	}
	if err := validatePromptCancellationSettlement(streamResult.terminal, cancelResponse.Settlement); err != nil {
		return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
	}
	if verifyReplay && s.target.WorkspaceGovernance.DuplicateSafeMutations {
		if err := s.probeCancellationReplay(ctx, cancelRequest, cancelResponse); err != nil {
			return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
		}
		if err := s.probeSettledPromptReplay(ctx, request, cancelResponse.Settlement); err != nil {
			return harnessv2.StartPromptRequest{}, harnessv2.PromptSettlement{}, err
		}
	}
	return request, cancelResponse.Settlement, nil
}

func validatePromptCancellationSettlement(terminal *harnessv2.Event, settlement harnessv2.PromptSettlement) error {
	if terminal == nil {
		return fmt.Errorf("original prompt stream returned no terminal event")
	}
	if settlement.TerminalEvent != terminal.Type {
		return fmt.Errorf("cancel settlement terminal event %q does not match original stream terminal event %q", settlement.TerminalEvent, terminal.Type)
	}
	if expectedOutcome := terminalOutcome(terminal); settlement.Outcome != expectedOutcome {
		return fmt.Errorf("cancel settlement outcome %q does not match original stream outcome %q", settlement.Outcome, expectedOutcome)
	}
	return nil
}

func (s *lifecycleProbeState) probeSessionDeletion(ctx context.Context, probeID string, allowAutomaticCleanup bool) error {
	request, err := s.deleteSessionRequest("delete-session-" + probeID)
	if err != nil {
		return err
	}
	if !allowAutomaticCleanup {
		if err := s.probeStaleSessionDeletion(ctx, request); err != nil {
			return err
		}
	}
	var deleted *harnessv2.DeleteRuntimeSessionResponse
	for {
		deleted, err = s.client.DeleteRuntimeSession(ctx, s.sessionID, request)
		if err == nil {
			break
		}
		var clientErr *harnessv2.ClientError
		if !allowAutomaticCleanup || !errors.As(err, &clientErr) {
			return fmt.Errorf("delete conformance runtime session: %w", err)
		}
		if clientErr.StatusCode == http.StatusNotFound {
			// A canceled session may already have been retired. Verify absence
			// on the same runtime boot, rather than accepting a bare 404.
			status, statusErr := s.client.Status(ctx)
			if statusErr != nil {
				return fmt.Errorf("verify canceled runtime session retirement: %w", statusErr)
			}
			if mismatch := harnessv2.CompareFence(s.sessionFence, status.Fence, false); mismatch != harnessv2.FenceMatch {
				return fmt.Errorf("runtime fence changed during cancellation cleanup: %s", mismatch)
			}
			for _, session := range status.Sessions {
				if session.RuntimeSessionID == s.sessionID || session.RuntimeSessionUID == s.sessionFence.RuntimeSessionUID {
					return fmt.Errorf("canceled runtime session remains resident after deletion returned not found")
				}
			}
			s.deleteAttempted = true
			return nil
		}
		if clientErr.StatusCode != http.StatusConflict || clientErr.Code != harnessv2.ErrorCodeAlreadyAccepted || !clientErr.Retryable {
			return fmt.Errorf("delete conformance runtime session: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for canceled runtime session cleanup: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	s.deleteAttempted = true
	if deleted.Classification.Class != harnessv2.RequestClassificationFresh {
		return fmt.Errorf("new conformance session deletion classified as %q, want fresh", deleted.Classification.Class)
	}
	if s.target.WorkspaceGovernance.DuplicateSafeMutations {
		return s.probeDeletionReplay(ctx, request, deleted)
	}
	return nil
}

func probeStaleInstanceFence(
	ctx context.Context,
	client *harnessv2.Client,
	target Target,
	poolFence harnessv2.Fence,
	workspace harnessv2.WorkspaceSpec,
	taskUID harnessv2.TaskUID,
	probeID string,
) error {
	staleFence := poolFence
	staleFence.RuntimeInstanceID = harnessv2.RuntimeInstanceID(string(poolFence.RuntimeInstanceID) + "-stale")
	staleFence.RuntimeSessionUID = harnessv2.RuntimeSessionUID("conformance-stale-session-uid-" + probeID)
	staleFence.RuntimeSessionGeneration = 1
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol:         harnessv2.ProtocolVersion,
		Metadata:         newMetadata(staleFence, taskUID, 1, "", harnessv2.OperationID("stale-fence-"+probeID), time.Now().UTC().Add(30*time.Second)),
		RuntimeSessionID: harnessv2.RuntimeSessionID("conformance-stale-session-" + probeID),
		Profile:          target.Profile,
		MCPConfiguration: targetMCPConfiguration(target),
		Workspace:        workspace,
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return fmt.Errorf("build stale-fence request: %w", err)
	}
	_, err := client.CreateRuntimeSession(ctx, request)
	if err == nil {
		return fmt.Errorf("runtime accepted session creation for a stale runtime instance fence")
	}
	var clientErr *harnessv2.ClientError
	if !errors.As(err, &clientErr) || clientErr.StatusCode != http.StatusGone || clientErr.Classification == nil ||
		clientErr.Classification.Class != harnessv2.RequestClassificationStaleFence ||
		clientErr.Classification.FenceMismatch != harnessv2.FenceMismatchRuntimeInstance {
		return fmt.Errorf("stale runtime instance fence returned %v, want HTTP 410 stale_fence/runtime_instance", err)
	}
	return nil
}

func (s *lifecycleProbeState) createSessionRequest(operation string) (harnessv2.CreateRuntimeSessionRequest, error) {
	request := harnessv2.CreateRuntimeSessionRequest{
		Protocol:         harnessv2.ProtocolVersion,
		Metadata:         newMetadata(s.sessionFence, s.taskUID, 1, "", harnessv2.OperationID(operation), time.Now().UTC().Add(30*time.Second)),
		RuntimeSessionID: s.sessionID,
		Profile:          s.target.Profile,
		MCPConfiguration: targetMCPConfiguration(s.target),
		Workspace:        s.workspace,
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return harnessv2.CreateRuntimeSessionRequest{}, fmt.Errorf("build create-session request: %w", err)
	}
	return request, nil
}

func (s *lifecycleProbeState) startPromptRequest(operation string) (harnessv2.StartPromptRequest, error) {
	now := time.Now().UTC()
	leaseDuration := time.Duration(s.target.Limits.MaxPromptLeaseMillis) * time.Millisecond
	if preferred := 30 * time.Second; leaseDuration > preferred {
		leaseDuration = preferred
	}
	minimum := time.Duration(s.target.Limits.MinPromptLeaseMillis) * time.Millisecond
	if leaseDuration < minimum {
		leaseDuration = minimum
	}
	if leaseDuration <= 0 {
		return harnessv2.StartPromptRequest{}, fmt.Errorf("runtime prompt lease limits do not permit a positive conformance lease")
	}
	s.lease = harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(leaseDuration)}
	metadata := newMetadata(s.sessionFence, s.taskUID, 1, s.promptID, harnessv2.OperationID(operation), s.lease.ExpiresAt)
	authorization, err := s.promptAuthorization(metadata, s.lease)
	if err != nil {
		return harnessv2.StartPromptRequest{}, err
	}
	request := harnessv2.StartPromptRequest{
		Protocol:         harnessv2.ProtocolVersion,
		Metadata:         metadata,
		Lease:            s.lease,
		MCPAuthorization: authorization,
		Input: harnessv2.PromptInput{
			Content: []harnessv2.ContentBlock{{
				Type: harnessv2.ContentBlockText,
				Text: "Orka harness conformance: perform no external side effects and await cancellation.",
			}},
			Metadata: map[string]string{"orka.conformance": "cancel-after-accept"},
		},
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return harnessv2.StartPromptRequest{}, fmt.Errorf("build start-prompt request: %w", err)
	}
	return request, nil
}

func targetMCPConfiguration(target Target) harnessv2.MCPPolicyConfiguration {
	toolPolicy := target.ToolPolicy
	if toolPolicy.Tools == nil {
		toolPolicy.Tools = []harnessv2.MCPToolDescriptor{}
	}
	if toolPolicy.AllowedToolNames == nil {
		toolPolicy.AllowedToolNames = []string{}
	}
	if toolPolicy.DescriptorDigest == "" {
		toolPolicy.DescriptorDigest, _ = harnessv2.CanonicalMCPToolDescriptorDigest(toolPolicy.Tools)
	}
	return harnessv2.MCPPolicyConfiguration{
		ToolPolicyDigest:       target.Profile.ToolPolicyDigest,
		ApprovalPolicyDigest:   target.Profile.ApprovalPolicyDigest,
		MCPConfigurationDigest: target.Profile.MCPConfigurationDigest,
		ToolPolicy:             toolPolicy, ApprovalPolicy: target.ApprovalPolicy,
	}
}

func (s *lifecycleProbeState) promptAuthorization(
	metadata harnessv2.MutationMetadata,
	lease harnessv2.PromptLease,
) (harnessv2.PromptMCPAuthorization, error) {
	configuration := targetMCPConfiguration(s.target)
	authorization := harnessv2.PromptMCPAuthorization{
		RuntimeSessionUID: metadata.Fence.RuntimeSessionUID,
		SessionGeneration: metadata.Fence.RuntimeSessionGeneration,
		TaskUID:           metadata.TaskUID, TaskAttempt: metadata.TaskAttempt, PromptID: metadata.PromptID,
		LeaseGeneration: lease.Generation, ToolPolicyDigest: s.target.Profile.ToolPolicyDigest,
		ApprovalPolicyDigest:   s.target.Profile.ApprovalPolicyDigest,
		MCPConfigurationDigest: s.target.Profile.MCPConfigurationDigest,
		ToolPolicy:             configuration.ToolPolicy, ApprovalPolicy: configuration.ApprovalPolicy, ExpiresAt: lease.ExpiresAt,
	}
	if err := authorization.ValidateForAt(metadata, lease, time.Now().UTC()); err != nil {
		return harnessv2.PromptMCPAuthorization{}, fmt.Errorf("canonical conformance MCP policy: %w", err)
	}
	return authorization, nil
}

func (s *lifecycleProbeState) cancelPromptRequest(operation string, promptMetadata harnessv2.MutationMetadata) (harnessv2.CancelPromptRequest, error) {
	now := time.Now().UTC()
	remaining := s.lease.ExpiresAt.Sub(now)
	if remaining <= time.Millisecond {
		return harnessv2.CancelPromptRequest{}, fmt.Errorf("prompt lease expired before cancellation could be issued")
	}
	settlementWindow := 10 * time.Second
	if remaining <= settlementWindow {
		settlementWindow = remaining / 2
	}
	if settlementWindow <= 0 {
		return harnessv2.CancelPromptRequest{}, fmt.Errorf("prompt lease leaves no cancellation settlement window")
	}
	metadata := newMetadata(s.sessionFence, promptMetadata.TaskUID, promptMetadata.TaskAttempt, promptMetadata.PromptID,
		harnessv2.OperationID(operation), s.lease.ExpiresAt)
	request := harnessv2.CancelPromptRequest{
		Protocol:           harnessv2.ProtocolVersion,
		Metadata:           metadata,
		Reason:             harnessv2.CancelReasonUserRequested,
		SettlementDeadline: now.Add(settlementWindow),
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return harnessv2.CancelPromptRequest{}, fmt.Errorf("build cancel-prompt request: %w", err)
	}
	return request, nil
}

func (s *lifecycleProbeState) deleteSessionRequest(operation string) (harnessv2.DeleteRuntimeSessionRequest, error) {
	request := harnessv2.DeleteRuntimeSessionRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: newMetadata(s.sessionFence, "", 0, "", harnessv2.OperationID(operation), time.Now().UTC().Add(30*time.Second)),
		Reason:   "conformance probe complete",
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return harnessv2.DeleteRuntimeSessionRequest{}, fmt.Errorf("build delete-session request: %w", err)
	}
	return request, nil
}

func (s *lifecycleProbeState) probePromptScopedAuthorizationRejection(ctx context.Context, probeID string) error {
	now := time.Now().UTC()
	leaseDuration := min(time.Duration(s.target.Limits.MaxPromptLeaseMillis)*time.Millisecond, 10*time.Second)
	minimum := time.Duration(s.target.Limits.MinPromptLeaseMillis) * time.Millisecond
	if leaseDuration < minimum {
		leaseDuration = minimum
	}
	lease := harnessv2.PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(leaseDuration)}
	promptID := harnessv2.PromptID("conformance-invalid-auth-" + probeID)
	metadata := newMetadata(s.sessionFence, s.taskUID, 1, promptID, harnessv2.OperationID("invalid-mcp-auth-"+probeID), lease.ExpiresAt)
	authorization, err := s.promptAuthorization(metadata, lease)
	if err != nil {
		return err
	}
	authorization.PromptID = harnessv2.PromptID("wrong-prompt-" + probeID)
	request := harnessv2.StartPromptRequest{
		Protocol:         harnessv2.ProtocolVersion,
		Metadata:         metadata,
		Lease:            lease,
		MCPAuthorization: authorization,
		Input:            harnessv2.PromptInput{Content: []harnessv2.ContentBlock{{Type: harnessv2.ContentBlockText, Text: "reject this mismatched authorization"}}},
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return fmt.Errorf("build invalid prompt-scoped authorization probe: %w", err)
	}
	capability, err := harnessv2.SignOperationCapability(s.target.OperationCapabilitySecret, harnessv2.ClaimsForMutation(request.Metadata))
	if err != nil {
		return fmt.Errorf("sign invalid prompt-scoped authorization probe: %w", err)
	}
	path, err := harnessv2.PromptPath(s.sessionID, promptID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	endpoint, err := endpointURL(s.target.BaseURL, path)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Accept", harnessv2.NDJSONMediaType+", application/json")
	httpRequest.Header.Set("Accept-Encoding", "identity")
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+s.target.ControllerBearerToken)
	httpRequest.Header.Set(harnessv2.OperationCapabilityHeader, capability)
	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("send invalid prompt-scoped authorization probe: %w", err)
	}
	defer response.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusUnprocessableEntity {
		return fmt.Errorf("mismatched prompt-scoped authorization returned HTTP %d, want 400 or 422", response.StatusCode)
	}
	return nil
}

func (s *lifecycleProbeState) probeWorkspaceDelta(
	ctx context.Context,
	probeID string,
	promptMetadata harnessv2.MutationMetadata,
	settlement harnessv2.PromptSettlement,
) (*harnessv2.CreateWorkspaceDeltaResponse, error) {
	settlementDigest, err := harnessv2.CanonicalPromptSettlementDigest(settlement)
	if err != nil {
		return nil, fmt.Errorf("canonicalize prompt settlement: %w", err)
	}
	maxBytes := s.target.Limits.MaxWorkspaceDeltaBytes
	request := harnessv2.CreateWorkspaceDeltaRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: newMetadata(s.sessionFence, promptMetadata.TaskUID, promptMetadata.TaskAttempt, promptMetadata.PromptID,
			harnessv2.OperationID("workspace-delta-"+probeID), time.Now().UTC().Add(30*time.Second)),
		DeltaID:                harnessv2.WorkspaceDeltaID("conformance-delta-" + probeID),
		Intent:                 s.target.Profile.WorkspaceIntent,
		VerifiedBaseline:       s.workspace.Baseline,
		PromptSettlementDigest: settlementDigest,
		Limits: harnessv2.WorkspaceDeltaLimits{
			MaxBytes:   maxBytes,
			MaxEntries: conformanceMaxWorkspaceEntries,
		},
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return nil, fmt.Errorf("build workspace-delta request: %w", err)
	}
	response, err := s.client.CreateWorkspaceDelta(ctx, s.sessionID, request)
	if err != nil {
		return nil, fmt.Errorf("create Orka-owned workspace delta: %w", err)
	}
	if response.Classification.Class != harnessv2.RequestClassificationFresh {
		return nil, fmt.Errorf("new workspace delta classified as %q, want fresh", response.Classification.Class)
	}
	if !response.Delta.NoFollowVerified {
		return nil, fmt.Errorf("workspace delta did not prove no-follow traversal")
	}
	if response.Delta.State != harnessv2.WorkspaceDeltaReadOnlyModified && !response.Delta.PublicationSafe {
		return nil, fmt.Errorf("workspace delta is not publication safe")
	}
	if s.target.WorkspaceGovernance.DuplicateSafeMutations {
		if err := s.probeWorkspaceDeltaReplay(ctx, request, response); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (s *lifecycleProbeState) probePublicationFinalization(
	ctx context.Context, probeID string, promptMetadata harnessv2.MutationMetadata, delta harnessv2.WorkspaceDeltaDescriptor,
) error {
	request := harnessv2.FinalizeRuntimeSessionPublicationRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: newMetadata(s.sessionFence, promptMetadata.TaskUID, promptMetadata.TaskAttempt, promptMetadata.PromptID,
			harnessv2.OperationID("publication-finalization-"+probeID), time.Now().UTC().Add(30*time.Second)),
		WorkspaceDeltaID: delta.DeltaID, PublicationID: "conformance-publication-" + probeID,
		PublicationGeneration: 1, PublicationVersion: 1, TerminalState: harnessv2.PublicationTerminalVerifiedExact,
		TerminalReceiptDigest: digestString("conformance-publication-receipt-" + probeID),
	}
	if err := setRequestDigest(&request, &request.Metadata); err != nil {
		return fmt.Errorf("build publication-finalization request: %w", err)
	}
	response, err := s.client.FinalizeRuntimeSessionPublication(ctx, s.sessionID, request)
	if err != nil {
		return fmt.Errorf("finalize conformance RuntimeSession publication: %w", err)
	}
	if response.Classification.Class != harnessv2.RequestClassificationFresh || response.Session.State != harnessv2.RuntimeSessionStateFinalizing {
		return fmt.Errorf("new publication finalization returned %#v", response)
	}
	s.publicationFinalized = true
	if s.target.WorkspaceGovernance.DuplicateSafeMutations {
		if err := s.probePublicationFinalizationReplay(ctx, request, response); err != nil {
			return err
		}
	}
	return s.probePublicationFinalizationRecovery(ctx, request, response)
}

func (s *lifecycleProbeState) cleanup(ctx context.Context) {
	if !s.sessionCreated {
		return
	}
	// Cancel and delete each get a full control-timeout budget rather than
	// sharing one fixed 5s window, so a slow cancellation cannot starve the
	// deletion and leak a resident probe session until capacity is exhausted.
	budget := s.target.ControlTimeout
	if budget <= 0 {
		budget = defaultControlTimeout
	}
	if s.promptStarted && !s.cancelAttempted {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
		request, err := s.cancelPromptRequest("cleanup-cancel-"+string(s.promptID), newMetadata(s.sessionFence, s.taskUID, 1, s.promptID, "placeholder", time.Now().UTC().Add(budget)))
		if err == nil {
			s.cancelAttempted = true
			_, _ = s.client.CancelPrompt(cancelCtx, s.sessionID, request)
		}
		cancel()
	}
	if !s.deleteAttempted {
		// Verify deletion succeeded and retry once on a transient failure, so a
		// runtime that needs more than one attempt to release the session does
		// not leave it resident.
		for attempt := range 2 {
			deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
			request, err := s.deleteSessionRequest(fmt.Sprintf("cleanup-delete-%s-%d", s.sessionID, attempt))
			if err != nil {
				cancel()
				return
			}
			_, deleteErr := s.client.DeleteRuntimeSession(deleteCtx, s.sessionID, request)
			cancel()
			if deleteErr == nil {
				s.deleteAttempted = true
				return
			}
		}
	}
}

type promptStreamResult struct {
	terminal *harnessv2.Event
	err      error
}

func consumePromptStream(stream *harnessv2.PromptStream) promptStreamResult {
	var terminal *harnessv2.Event
	for {
		event, err := stream.Decode()
		if errors.Is(err, io.EOF) {
			return promptStreamResult{terminal: terminal}
		}
		if err != nil {
			return promptStreamResult{terminal: terminal, err: err}
		}
		if event.Type.IsTerminal() {
			copy := event
			terminal = &copy
		}
	}
}

func newMetadata(
	fence harnessv2.Fence,
	taskUID harnessv2.TaskUID,
	taskAttempt uint32,
	promptID harnessv2.PromptID,
	operationID harnessv2.OperationID,
	expiresAt time.Time,
) harnessv2.MutationMetadata {
	return harnessv2.MutationMetadata{
		Fence:                      fence,
		TaskUID:                    taskUID,
		TaskAttempt:                taskAttempt,
		PromptID:                   promptID,
		OperationID:                operationID,
		RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
		RequestDigest:              harnessv2.RequestDigest(digestString("placeholder")),
		ExpiresAt:                  expiresAt,
	}
}

func setRequestDigest(request any, metadata *harnessv2.MutationMetadata) error {
	if metadata == nil {
		return fmt.Errorf("request metadata is required")
	}
	metadata.RequestDigest = harnessv2.RequestDigest(digestString("placeholder"))
	digest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		return err
	}
	metadata.RequestDigest = digest
	return nil
}

func newProbeID() (string, error) {
	var randomBytes [12]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(randomBytes[:]), nil
}

func digestString(value string) string {
	return digestBytes([]byte(value))
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func terminalOutcome(event *harnessv2.Event) harnessv2.PromptOutcome {
	if event == nil {
		return ""
	}
	switch event.Type {
	case harnessv2.EventCompleted:
		return harnessv2.PromptOutcomeSucceeded
	case harnessv2.EventCancelled:
		return harnessv2.PromptOutcomeCancelled
	case harnessv2.EventFailed:
		return harnessv2.PromptOutcomeFailed
	case harnessv2.EventOutcomeUnknown:
		return harnessv2.PromptOutcomeUnknown
	default:
		return ""
	}
}
