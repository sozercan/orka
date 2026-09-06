/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package workspace

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandbox "sigs.k8s.io/agent-sandbox/clients/go/sandbox"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
)

const (
	agentSandboxRouterURLEnv             = "ORKA_AGENT_SANDBOX_ROUTER_URL"
	agentSandboxDefaultFileMode          = 0o644
	agentSandboxDefaultRootList          = "."
	agentSandboxEnvFilePrefix            = "orka-env-"
	agentSandboxExecRoot                 = "/app/"
	agentSandboxReadyMaxPollInterval     = time.Second
	agentSandboxReadyInitialPollInterval = 10 * time.Millisecond
	agentSandboxSDKReadyTimeout          = 180 * time.Second
	agentSandboxWorkspaceIDPrefix        = "agent-sandbox:"
	agentSandboxWorkspaceReadyMessage    = "workspace ready"
)

// AgentSandboxOption configures an AgentSandboxExecutor.
type AgentSandboxOption func(*AgentSandboxExecutor)

// WithAgentSandboxAPIURL overrides the sandbox-router URL used by the
// agent-sandbox SDK. When unset, NewAgentSandboxExecutor reads
// ORKA_AGENT_SANDBOX_ROUTER_URL lazily for each new claim.
func WithAgentSandboxAPIURL(apiURL string) AgentSandboxOption {
	return func(e *AgentSandboxExecutor) {
		e.apiURL = strings.TrimSpace(apiURL)
	}
}

type agentSandboxHandle interface {
	sandbox.Handle
	sandbox.Info
}

type agentSandboxClient interface {
	CreateSandbox(ctx context.Context, template, namespace, warmPoolPolicy string) (agentSandboxHandle, error)
	CreateSandboxWithName(ctx context.Context, claimName, template, namespace, warmPoolPolicy string) (agentSandboxHandle, error)
	GetSandbox(ctx context.Context, claimName, namespace string) (agentSandboxHandle, error)
	DeleteSandbox(ctx context.Context, claimName, namespace string) error
}

type agentSandboxClientFactory func(context.Context, sandbox.Options) (agentSandboxClient, error)

type agentSandboxSDKClient struct {
	client       *sandbox.Client
	k8s          *sandbox.K8sHelper
	readyTimeout time.Duration
}

func (c *agentSandboxSDKClient) CreateSandbox(ctx context.Context, template, namespace, _ string) (agentSandboxHandle, error) {
	claim, err := c.createSandboxClaim(ctx, "", template, namespace)
	if err != nil {
		return nil, err
	}
	return c.openCreatedSandbox(ctx, claim.Name, namespace)
}

func (c *agentSandboxSDKClient) CreateSandboxWithName(ctx context.Context, claimName, template, namespace, warmPoolPolicy string) (agentSandboxHandle, error) {
	if strings.TrimSpace(claimName) == "" {
		return c.CreateSandbox(ctx, template, namespace, warmPoolPolicy)
	}
	created := false
	if _, err := c.createSandboxClaim(ctx, claimName, template, namespace); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return nil, err
		}
	} else {
		created = true
	}
	if created {
		return c.openCreatedSandbox(ctx, claimName, namespace)
	}
	return c.client.GetSandbox(ctx, claimName, namespace)
}

func (c *agentSandboxSDKClient) openCreatedSandbox(ctx context.Context, claimName, namespace string) (agentSandboxHandle, error) {
	if err := c.waitCreatedSandboxReady(ctx, claimName, namespace); err != nil {
		agentSandboxDeleteCreatedClaim(c.k8s, claimName, namespace)
		return nil, err
	}
	handle, err := c.client.GetSandbox(ctx, claimName, namespace)
	if err != nil {
		agentSandboxDeleteCreatedClaim(c.k8s, claimName, namespace)
	}
	return handle, err
}

func (c *agentSandboxSDKClient) waitCreatedSandboxReady(ctx context.Context, claimName, namespace string) error {
	ctx, cancel := agentSandboxReadyContext(ctx, c.readyTimeout)
	defer cancel()

	sandboxName, err := c.waitClaimSandboxName(ctx, claimName, namespace)
	if err != nil {
		return err
	}
	return c.waitSandboxReady(ctx, sandboxName, namespace)
}

func (c *agentSandboxSDKClient) waitClaimSandboxName(ctx context.Context, claimName, namespace string) (string, error) {
	if c.k8s == nil || c.k8s.ExtensionsClient == nil {
		return "", fmt.Errorf("agent sandbox extensions client is not configured")
	}
	backoff := agentSandboxReadyInitialPollInterval
	var lastErr error
	for {
		claim, err := c.k8s.ExtensionsClient.SandboxClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err == nil {
			lastErr = nil
			if name := strings.TrimSpace(claim.Status.SandboxStatus.Name); name != "" {
				return name, nil
			}
		} else if k8serrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: claim %s deleted before sandbox name was resolved", sandbox.ErrSandboxDeleted, claimName)
		} else {
			lastErr = err
		}
		if err := sleepContext(ctx, backoff); err != nil {
			if lastErr != nil {
				return "", fmt.Errorf("%w: sandbox name not resolved for claim %s: %w", sandbox.ErrTimeout, claimName, lastErr)
			}
			return "", fmt.Errorf("%w: sandbox name not resolved for claim %s: %w", sandbox.ErrTimeout, claimName, err)
		}
		backoff = agentSandboxNextReadyBackoff(backoff)
	}
}

func (c *agentSandboxSDKClient) waitSandboxReady(ctx context.Context, sandboxName, namespace string) error {
	if c.k8s == nil || c.k8s.AgentsClient == nil {
		return fmt.Errorf("agent sandbox agents client is not configured")
	}
	backoff := agentSandboxReadyInitialPollInterval
	var lastErr error
	for {
		sb, err := c.k8s.AgentsClient.Sandboxes(namespace).Get(ctx, sandboxName, metav1.GetOptions{})
		if err == nil {
			lastErr = nil
			if agentSandboxReady(sb.Status.Conditions) {
				return nil
			}
		} else if k8serrors.IsNotFound(err) {
			return fmt.Errorf("%w: sandbox %s deleted before becoming ready", sandbox.ErrSandboxDeleted, sandboxName)
		} else {
			lastErr = err
		}
		if err := sleepContext(ctx, backoff); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%w: sandbox %s did not become ready: %w", sandbox.ErrTimeout, sandboxName, lastErr)
			}
			return fmt.Errorf("%w: sandbox %s did not become ready: %w", sandbox.ErrTimeout, sandboxName, err)
		}
		backoff = agentSandboxNextReadyBackoff(backoff)
	}
}

func agentSandboxNextReadyBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next <= 0 {
		return agentSandboxReadyInitialPollInterval
	}
	return min(next, agentSandboxReadyMaxPollInterval)
}

func agentSandboxReady(conditions []metav1.Condition) bool {
	for _, cond := range conditions {
		if cond.Type == string(sandboxv1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func agentSandboxReadyContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = agentSandboxSDKReadyTimeout
	}
	return contextWithTimeout(ctx, timeout)
}

func (c *agentSandboxSDKClient) createSandboxClaim(ctx context.Context, claimName, warmPoolName, namespace string) (*sandboxextv1beta1.SandboxClaim, error) {
	claim := &sandboxextv1beta1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: namespace,
		},
		Spec: sandboxextv1beta1.SandboxClaimSpec{
			WarmPoolRef: sandboxextv1beta1.SandboxWarmPoolRef{Name: warmPoolName},
		},
	}
	if strings.TrimSpace(claimName) == "" {
		claim.GenerateName = "sandbox-claim-"
	}
	created, err := c.k8s.ExtensionsClient.SandboxClaims(namespace).Create(ctx, claim, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: warmpool=%s namespace=%s: %w", sandbox.ErrClaimFailed, warmPoolName, namespace, err)
	}
	return created, nil
}

func agentSandboxDeleteCreatedClaim(k8s *sandbox.K8sHelper, claimName, namespace string) {
	if k8s == nil || strings.TrimSpace(claimName) == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := k8s.ExtensionsClient.SandboxClaims(namespace).Delete(cleanupCtx, claimName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		k8s.Log.Error(err, "failed to roll back sandbox claim", "claim", claimName, "namespace", namespace)
	}
}

func (c *agentSandboxSDKClient) GetSandbox(ctx context.Context, claimName, namespace string) (agentSandboxHandle, error) {
	return c.client.GetSandbox(ctx, claimName, namespace)
}

func (c *agentSandboxSDKClient) DeleteSandbox(ctx context.Context, claimName, namespace string) error {
	return c.client.DeleteSandbox(ctx, claimName, namespace)
}

// AgentSandboxExecutor adapts sigs.k8s.io/agent-sandbox to WorkspaceExecutor.
//
// The SDK owns the Kubernetes SandboxClaim lifecycle and generates claim names.
// Orka keeps a small in-process metadata cache so callers can describe, reuse,
// and clean up workspaces through the backend-neutral WorkspaceExecutor
// contract.
type AgentSandboxExecutor struct {
	mu sync.Mutex

	apiURL    string
	newClient agentSandboxClientFactory
	now       func() time.Time

	workspaces map[string]*agentSandboxWorkspace
	handles    map[string]agentSandboxHandle
	clients    map[string]agentSandboxClient
	reuseIndex map[string]string
}

type agentSandboxWorkspace struct {
	ref         WorkspaceRef
	template    TemplateRef
	reuseKey    string
	phase       Phase
	retained    bool
	message     string
	createdAt   time.Time
	readyAt     time.Time
	releasedAt  time.Time
	deletedAt   time.Time
	labels      map[string]string
	annotations map[string]string
	artifacts   map[string]DownloadedArtifact
}

// NewAgentSandboxExecutor returns a WorkspaceExecutor backed by the
// agent-sandbox Go SDK.
func NewAgentSandboxExecutor(opts ...AgentSandboxOption) *AgentSandboxExecutor {
	e := &AgentSandboxExecutor{
		newClient: func(ctx context.Context, opts sandbox.Options) (agentSandboxClient, error) {
			k8s := opts.K8sHelper
			if k8s == nil {
				var err error
				k8s, err = sandbox.NewK8sHelper(opts.RestConfig, opts.Logger)
				if err != nil {
					return nil, err
				}
				opts.K8sHelper = k8s
			}
			client, err := sandbox.NewClient(ctx, opts)
			if err != nil {
				return nil, err
			}
			return &agentSandboxSDKClient{client: client, k8s: k8s, readyTimeout: opts.SandboxReadyTimeout}, nil
		},
		now:        time.Now,
		workspaces: make(map[string]*agentSandboxWorkspace),
		handles:    make(map[string]agentSandboxHandle),
		clients:    make(map[string]agentSandboxClient),
		reuseIndex: make(map[string]string),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

var _ WorkspaceExecutor = (*AgentSandboxExecutor)(nil)

// Claim creates a new agent-sandbox claim or reuses a locally-known reusable
// workspace by reuse key or claim name.
func (e *AgentSandboxExecutor) Claim(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("claim", err)
	}
	if strings.TrimSpace(req.Namespace) == "" {
		return nil, NewError("claim", ErrorKindInvalidArgument, "namespace is required", false, nil)
	}
	if strings.TrimSpace(req.Template.Name) == "" {
		return nil, NewError("claim", ErrorKindInvalidArgument, "template name is required", false, nil)
	}

	if req.ReuseKey != "" || req.ClaimName != "" {
		result, err := e.tryReuseClaim(ctx, req)
		if result != nil {
			return result, nil
		}
		if err != nil && (!req.CreateIfMissing || req.ClaimName == "" || !IsKind(err, ErrorKindNotFound)) {
			return nil, err
		}
	}

	client, err := e.newClient(ctx, e.agentSandboxOptions(req))
	if err != nil {
		return nil, agentSandboxError("claim", err)
	}
	handle, err := e.createSandbox(ctx, client, req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, contextError("claim", ctxErr)
		}
		return nil, agentSandboxError("claim", err)
	}

	ref, err := agentSandboxRef(req.Namespace, handle)
	if err != nil {
		_ = handle.Close(context.Background())
		return nil, NewError("claim", ErrorKindUnknown, "agent sandbox opened without a claim name", false, err)
	}
	now := e.now()
	ws := &agentSandboxWorkspace{
		ref:         ref,
		template:    req.Template,
		reuseKey:    req.ReuseKey,
		phase:       PhaseReady,
		message:     agentSandboxWorkspaceReadyMessage,
		createdAt:   now,
		readyAt:     now,
		labels:      copyStringMap(req.Labels),
		annotations: mergeAgentSandboxAnnotations(req.Annotations, handle.Annotations()),
		artifacts:   make(map[string]DownloadedArtifact),
	}

	key := workspaceKey(ref)
	e.mu.Lock()
	if existing := e.workspaces[key]; existing != nil && existing.phase != PhaseDeleted {
		e.mu.Unlock()
		_ = handle.Close(context.Background())
		return nil, NewError("claim", ErrorKindAlreadyExists, "workspace already exists in local cache", false, nil)
	}
	e.workspaces[key] = ws
	e.handles[key] = handle
	e.clients[key] = client
	if req.ReuseKey != "" {
		e.reuseIndex[reuseIndexKey(req.Namespace, req.Template, req.ReuseKey)] = key
	}
	e.mu.Unlock()

	return &ClaimResult{
		Ref:       ref,
		Template:  req.Template,
		ReuseKey:  req.ReuseKey,
		Created:   true,
		Phase:     PhaseReady,
		Message:   ws.message,
		ClaimedAt: now,
	}, nil
}

func (e *AgentSandboxExecutor) createSandbox(ctx context.Context, client agentSandboxClient, req ClaimRequest) (agentSandboxHandle, error) {
	if req.CreateIfMissing && strings.TrimSpace(req.ClaimName) != "" {
		return client.CreateSandboxWithName(ctx, req.ClaimName, req.Template.Name, req.Namespace, req.WarmPoolPolicy)
	}
	return client.CreateSandbox(ctx, req.Template.Name, req.Namespace, req.WarmPoolPolicy)
}

// WaitReady reconnects when necessary and returns once the workspace can accept
// SDK operations.
func (e *AgentSandboxExecutor) WaitReady(ctx context.Context, req WaitReadyRequest) (*ReadyResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("wait ready", err)
	}

	ws, handle, err := e.findWorkspaceAndHandle("wait ready", req.Ref)
	if err != nil {
		return nil, err
	}
	if err := e.ensureWorkspaceReady(ctx, "wait ready", ws, handle); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	ws, err = e.findWorkspaceLocked("wait ready", req.Ref)
	if err != nil {
		return nil, err
	}
	return &ReadyResult{Ref: ws.ref, Phase: ws.phase, Message: ws.message, ReadyAt: ws.readyAt}, nil
}

// Exec runs a command in a ready agent sandbox.
func (e *AgentSandboxExecutor) Exec(ctx context.Context, req ExecRequest) (*ExecResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("exec", err)
	}
	if len(req.Command) == 0 || strings.TrimSpace(req.Command[0]) == "" {
		return nil, NewError("exec", ErrorKindInvalidArgument, "command is required", false, nil)
	}
	if len(req.Stdin) > 0 {
		return nil, NewError("exec", ErrorKindInvalidArgument, "stdin is not supported by agent-sandbox command execution", false, nil)
	}

	startedAt := e.now()
	envFileName, envFilePath, envFileContent, err := agentSandboxEnvFile(startedAt, req.Env)
	if err != nil {
		return nil, NewError("exec", ErrorKindInvalidArgument, err.Error(), false, err)
	}
	command, err := agentSandboxCommandString(req, envFilePath)
	if err != nil {
		return nil, NewError("exec", ErrorKindInvalidArgument, err.Error(), false, err)
	}

	ws, handle, err := e.findReadyWorkspaceAndHandle("exec", req.Ref)
	if err != nil {
		return nil, err
	}
	if envFileName != "" {
		if err := handle.Write(ctx, envFileName, envFileContent); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("exec", ctxErr)
			}
			return nil, agentSandboxError("exec", err)
		}
	}

	// Claim normally sizes the cached SDK client for MaxRequestTimeout, which
	// covers long Exec calls. Keep this per-call timeout as a safety net for
	// handles created without that larger transport budget.
	runOpts := []sandbox.CallOption{}
	if req.Timeout > 0 {
		runOpts = append(runOpts, sandbox.WithTimeout(req.Timeout))
	}
	result, err := handle.Run(ctx, command, runOpts...)
	if err != nil {
		e.cleanupAgentSandboxEnvFile(handle, envFileName)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, contextError("exec", ctxErr)
		}
		return nil, agentSandboxError("exec", err)
	}
	finishedAt := e.now()

	execResult := &ExecResult{
		Ref:        ws.ref,
		Command:    append([]string(nil), req.Command...),
		Stdout:     result.Stdout,
		Stderr:     result.Stderr,
		ExitCode:   result.ExitCode,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
	if req.MaxOutputBytes > 0 {
		execResult.Stdout, execResult.StdoutTruncated = truncateBytes(execResult.Stdout, req.MaxOutputBytes)
		execResult.Stderr, execResult.StderrTruncated = truncateBytes(execResult.Stderr, req.MaxOutputBytes)
	}
	if execResult.ExitCode != 0 {
		return execResult, NewError("exec", ErrorKindCommandFailed, fmt.Sprintf("command exited with code %d", execResult.ExitCode), false, nil)
	}
	return execResult, nil
}

// Upload writes files into the sandbox root. The SDK Write API accepts a single
// filename, so nested paths are rejected here instead of silently flattening.
func (e *AgentSandboxExecutor) Upload(ctx context.Context, req UploadRequest) (*UploadResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("upload", err)
	}
	if len(req.Artifacts) == 0 {
		return nil, NewError("upload", ErrorKindInvalidArgument, "at least one artifact is required", false, nil)
	}

	ws, handle, err := e.findUsableWorkspaceAndHandle("upload", req.Ref)
	if err != nil {
		return nil, err
	}

	uploaded := make([]Artifact, 0, len(req.Artifacts))
	for _, artifact := range req.Artifacts {
		artifactPath, err := cleanArtifactPath(artifact.Path)
		if err != nil {
			return nil, NewError("upload", ErrorKindInvalidArgument, err.Error(), false, err)
		}
		if strings.Contains(artifactPath, "/") {
			return nil, NewError("upload", ErrorKindInvalidArgument, "agent-sandbox upload only supports plain filenames", false, nil)
		}
		data := append([]byte(nil), artifact.Data...)
		if err := handle.Write(ctx, artifactPath, data); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("upload", ctxErr)
			}
			return nil, agentSandboxError("upload", err)
		}

		mode := artifact.Mode
		if mode == 0 {
			mode = agentSandboxDefaultFileMode
		}
		modTime := artifact.ModTime
		if modTime.IsZero() {
			modTime = e.now()
		}
		meta := Artifact{
			Path:    artifactPath,
			Size:    int64(len(data)),
			Digest:  digest(data),
			Mode:    mode,
			ModTime: modTime,
		}
		e.mu.Lock()
		if ws.artifacts == nil {
			ws.artifacts = make(map[string]DownloadedArtifact)
		}
		ws.artifacts[artifactPath] = DownloadedArtifact{Artifact: meta, Data: data}
		e.mu.Unlock()
		uploaded = append(uploaded, meta)
	}
	return &UploadResult{Ref: ws.ref, Artifacts: uploaded}, nil
}

// Download reads files from the sandbox. Empty Paths recursively downloads all
// files visible from the sandbox root.
func (e *AgentSandboxExecutor) Download(ctx context.Context, req DownloadRequest) (*DownloadResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("download", err)
	}

	ws, handle, err := e.findUsableWorkspaceAndHandle("download", req.Ref)
	if err != nil {
		return nil, err
	}

	paths := append([]string(nil), req.Paths...)
	listed := make(map[string]Artifact)
	if len(paths) == 0 {
		paths, listed, err = e.listAgentSandboxFiles(ctx, handle, agentSandboxDefaultRootList)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("download", ctxErr)
			}
			return nil, agentSandboxError("download", err)
		}
	}

	downloaded := make([]DownloadedArtifact, 0, len(paths))
	for _, requestedPath := range paths {
		artifactPath, err := cleanArtifactPath(requestedPath)
		if err != nil {
			return nil, NewError("download", ErrorKindInvalidArgument, err.Error(), false, err)
		}
		data, err := handle.Read(ctx, artifactPath)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("download", ctxErr)
			}
			return nil, agentSandboxError("download", err)
		}
		meta := e.downloadedArtifactMetadata(ws, artifactPath, data, listed[artifactPath])
		item := DownloadedArtifact{Artifact: meta, Data: data}
		downloaded = append(downloaded, item)

		e.mu.Lock()
		if ws.artifacts == nil {
			ws.artifacts = make(map[string]DownloadedArtifact)
		}
		cached := item
		cached.Data = append([]byte(nil), data...)
		ws.artifacts[artifactPath] = cached
		e.mu.Unlock()
	}
	return &DownloadResult{Ref: ws.ref, Artifacts: downloaded}, nil
}

// Release disconnects from the sandbox without deleting the SandboxClaim.
func (e *AgentSandboxExecutor) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("release", err)
	}

	ws, handle, err := e.findWorkspaceAndHandle("release", req.Ref)
	if err != nil {
		return nil, err
	}
	if err := handle.Disconnect(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, contextError("release", ctxErr)
		}
		return nil, agentSandboxError("release", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if ws.phase == PhaseDeleted {
		return nil, NewError("release", ErrorKindNotFound, "workspace is deleted", false, nil)
	}
	ws.releasedAt = e.now()
	if req.Retain {
		ws.phase = PhaseRetained
		ws.retained = true
		ws.message = releaseMessage(req.Reason, "workspace retained")
		return &ReleaseResult{Ref: ws.ref, Retained: true, Phase: ws.phase, Message: ws.message}, nil
	}

	ws.phase = PhaseReleased
	ws.retained = false
	ws.message = releaseMessage(req.Reason, "workspace released")
	e.removeReuseIndexLocked(ws)
	return &ReleaseResult{Ref: ws.ref, Released: true, Phase: ws.phase, Message: ws.message}, nil
}

// Delete closes and deletes the backing SandboxClaim. Repeated deletion of a
// locally-deleted workspace is idempotent.
func (e *AgentSandboxExecutor) Delete(ctx context.Context, req DeleteRequest) (*DeleteResult, error) {
	ctx, cancel := contextWithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, contextError("delete", err)
	}

	e.mu.Lock()
	ws, err := e.findWorkspaceLocked("delete", req.Ref)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if ws.phase == PhaseDeleted {
		result := &DeleteResult{Ref: ws.ref, Deleted: false, Phase: ws.phase, Message: ws.message}
		e.mu.Unlock()
		return result, nil
	}
	key := workspaceKey(ws.ref)
	handle := e.handles[key]
	client := e.clients[key]
	e.mu.Unlock()

	if client != nil {
		if err := client.DeleteSandbox(ctx, ws.ref.ClaimName, ws.ref.Namespace); err != nil && !agentSandboxIsNotFound(err) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("delete", ctxErr)
			}
			return nil, agentSandboxError("delete", err)
		}
	} else {
		if handle == nil {
			return nil, NewError("delete", ErrorKindFailedPrecondition, "workspace handle is unavailable", false, nil)
		}
		if err := handle.Close(ctx); err != nil && !agentSandboxIsNotFound(err) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, contextError("delete", ctxErr)
			}
			return nil, agentSandboxError("delete", err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	ws.phase = PhaseDeleted
	ws.retained = false
	ws.deletedAt = e.now()
	ws.message = releaseMessage(req.Reason, "workspace deleted")
	e.removeReuseIndexLocked(ws)
	delete(e.handles, key)
	delete(e.clients, key)
	return &DeleteResult{Ref: ws.ref, Deleted: true, Phase: ws.phase, Message: ws.message}, nil
}

// Describe returns the executor's local snapshot for a workspace.
func (e *AgentSandboxExecutor) Describe(ctx context.Context, req DescribeRequest) (*Description, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, contextError("describe", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, err := e.findWorkspaceLocked("describe", req.Ref)
	if err != nil {
		return nil, err
	}
	return e.descriptionLocked(ws), nil
}

func (e *AgentSandboxExecutor) apiURLForClaim() string {
	if e.apiURL != "" {
		return e.apiURL
	}
	return strings.TrimSpace(os.Getenv(agentSandboxRouterURLEnv))
}

func (e *AgentSandboxExecutor) agentSandboxOptions(req ClaimRequest) sandbox.Options {
	opts := sandbox.Options{
		WarmPoolName: req.Template.Name,
		Namespace:    req.Namespace,
		APIURL:       e.apiURLForClaim(),
		Quiet:        true,
	}
	// The HTTP client built from these Options is reused for the lifetime
	// of the cached handle, including subsequent Exec calls. Use the
	// longer of Timeout and MaxRequestTimeout so a long-running command
	// is not killed by a transport-level ResponseHeaderTimeout sized for
	// the short claim-readiness window.
	transportTimeout := max(req.Timeout, req.MaxRequestTimeout)
	if transportTimeout > 0 {
		opts.RequestTimeout = transportTimeout
		opts.PerAttemptTimeout = transportTimeout
	}
	return opts
}

func (e *AgentSandboxExecutor) tryReuseClaim(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	if result, err := e.tryReuseLocalClaim(ctx, req); result != nil || err != nil {
		return result, err
	}
	if req.ClaimName == "" {
		return nil, nil
	}
	return e.reattachClaim(ctx, req)
}

func (e *AgentSandboxExecutor) tryReuseLocalClaim(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	e.mu.Lock()
	var ws *agentSandboxWorkspace
	var handle agentSandboxHandle
	if req.ReuseKey != "" {
		if existingKey := e.reuseIndex[reuseIndexKey(req.Namespace, req.Template, req.ReuseKey)]; existingKey != "" {
			ws = e.reusableWorkspaceLocked(existingKey)
			handle = e.handles[existingKey]
		}
	}
	if ws == nil && req.ClaimName != "" {
		key := workspaceKey(WorkspaceRef{Namespace: req.Namespace, ClaimName: req.ClaimName})
		ws = e.reusableWorkspaceLocked(key)
		handle = e.handles[key]
	}
	e.mu.Unlock()
	if ws == nil {
		return nil, nil
	}
	if handle == nil {
		return nil, NewError("claim", ErrorKindFailedPrecondition, "workspace handle is unavailable", false, nil)
	}
	if err := e.ensureWorkspaceReady(ctx, "claim", ws, handle); err != nil {
		return nil, err
	}

	now := e.now()
	e.mu.Lock()
	defer e.mu.Unlock()
	ws.phase = PhaseReady
	ws.retained = false
	ws.message = "workspace reused"
	if ws.readyAt.IsZero() {
		ws.readyAt = now
	}
	e.refreshRefLocked(ws, handle)
	return &ClaimResult{
		Ref:       ws.ref,
		Template:  ws.template,
		ReuseKey:  ws.reuseKey,
		Reused:    true,
		Phase:     ws.phase,
		Message:   ws.message,
		ClaimedAt: now,
	}, nil
}

func (e *AgentSandboxExecutor) reattachClaim(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	client, err := e.newClient(ctx, e.agentSandboxOptions(req))
	if err != nil {
		return nil, agentSandboxError("claim", err)
	}
	handle, err := client.GetSandbox(ctx, req.ClaimName, req.Namespace)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, contextError("claim", ctxErr)
		}
		return nil, agentSandboxError("claim", err)
	}

	ref, err := agentSandboxRef(req.Namespace, handle)
	if err != nil {
		_ = handle.Disconnect(context.Background())
		return nil, NewError("claim", ErrorKindUnknown, "agent sandbox reattached without a claim name", false, err)
	}
	now := e.now()
	ws := &agentSandboxWorkspace{
		ref:         ref,
		template:    req.Template,
		reuseKey:    req.ReuseKey,
		phase:       PhaseReady,
		message:     "workspace reused",
		createdAt:   now,
		readyAt:     now,
		labels:      copyStringMap(req.Labels),
		annotations: mergeAgentSandboxAnnotations(req.Annotations, handle.Annotations()),
		artifacts:   make(map[string]DownloadedArtifact),
	}

	key := workspaceKey(ref)
	e.mu.Lock()
	if existing := e.workspaces[key]; existing != nil && existing.phase != PhaseDeleted {
		e.mu.Unlock()
		_ = handle.Disconnect(context.Background())
		return nil, NewError("claim", ErrorKindAlreadyExists, "workspace already exists in local cache", false, nil)
	}
	e.workspaces[key] = ws
	e.handles[key] = handle
	e.clients[key] = client
	if req.ReuseKey != "" {
		e.reuseIndex[reuseIndexKey(req.Namespace, req.Template, req.ReuseKey)] = key
	}
	e.mu.Unlock()

	return &ClaimResult{
		Ref:       ref,
		Template:  req.Template,
		ReuseKey:  req.ReuseKey,
		Reused:    true,
		Phase:     PhaseReady,
		Message:   ws.message,
		ClaimedAt: now,
	}, nil
}

func (e *AgentSandboxExecutor) ensureWorkspaceReady(ctx context.Context, op string, ws *agentSandboxWorkspace, handle agentSandboxHandle) error {
	e.mu.Lock()
	switch ws.phase {
	case PhaseDeleted:
		e.mu.Unlock()
		return NewError(op, ErrorKindNotFound, "workspace is deleted", false, nil)
	case PhaseFailed:
		message := ws.message
		e.mu.Unlock()
		return NewError(op, ErrorKindFailedPrecondition, message, false, nil)
	case PhaseReleased:
		e.mu.Unlock()
		return NewError(op, ErrorKindFailedPrecondition, fmt.Sprintf("workspace phase is %s", PhaseReleased), false, nil)
	}
	e.mu.Unlock()

	if !handle.IsReady() {
		if err := handle.Open(ctx); err != nil && !errors.Is(err, sandbox.ErrAlreadyOpen) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return contextError(op, ctxErr)
			}
			e.markWorkspaceOpenFailure(ws, err)
			return agentSandboxError(op, err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if ws.phase == PhaseDeleted {
		return NewError(op, ErrorKindNotFound, "workspace is deleted", false, nil)
	}
	if ws.phase == PhaseFailed {
		return NewError(op, ErrorKindFailedPrecondition, ws.message, false, nil)
	}
	ws.phase = PhaseReady
	ws.retained = false
	ws.message = agentSandboxWorkspaceReadyMessage
	if ws.readyAt.IsZero() {
		ws.readyAt = e.now()
	}
	e.refreshRefLocked(ws, handle)
	return nil
}

func (e *AgentSandboxExecutor) markWorkspaceOpenFailure(ws *agentSandboxWorkspace, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if agentSandboxIsNotFound(err) {
		ws.phase = PhaseDeleted
		ws.deletedAt = e.now()
		ws.message = "workspace deleted"
		e.removeReuseIndexLocked(ws)
		key := workspaceKey(ws.ref)
		delete(e.handles, key)
		delete(e.clients, key)
		return
	}
	ws.phase = PhaseFailed
	ws.message = err.Error()
	e.removeReuseIndexLocked(ws)
}

func (e *AgentSandboxExecutor) refreshRefLocked(ws *agentSandboxWorkspace, handle agentSandboxHandle) {
	if claimName := handle.ClaimName(); claimName != "" {
		ws.ref.ClaimName = claimName
	}
	if sandboxName := handle.SandboxName(); sandboxName != "" {
		ws.ref.SandboxName = sandboxName
	}
	if ws.ref.ID == "" {
		ws.ref.ID = agentSandboxWorkspaceIDPrefix + workspaceKey(ws.ref)
	}
	ws.annotations = mergeAgentSandboxAnnotations(ws.annotations, handle.Annotations())
}

func (e *AgentSandboxExecutor) findWorkspaceAndHandle(op string, ref WorkspaceRef) (*agentSandboxWorkspace, agentSandboxHandle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, err := e.findWorkspaceLocked(op, ref)
	if err != nil {
		return nil, nil, err
	}
	if ws.phase == PhaseDeleted {
		return nil, nil, NewError(op, ErrorKindNotFound, "workspace is deleted", false, nil)
	}
	handle := e.handles[workspaceKey(ws.ref)]
	if handle == nil {
		return nil, nil, NewError(op, ErrorKindFailedPrecondition, "workspace handle is unavailable", false, nil)
	}
	return ws, handle, nil
}

func (e *AgentSandboxExecutor) findReadyWorkspaceAndHandle(op string, ref WorkspaceRef) (*agentSandboxWorkspace, agentSandboxHandle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, err := e.findWorkspaceLocked(op, ref)
	if err != nil {
		return nil, nil, err
	}
	if ws.phase == PhaseDeleted {
		return nil, nil, NewError(op, ErrorKindNotFound, "workspace is deleted", false, nil)
	}
	if ws.phase != PhaseReady {
		return nil, nil, NewError(op, ErrorKindFailedPrecondition, fmt.Sprintf("workspace phase is %s", ws.phase), false, nil)
	}
	handle := e.handles[workspaceKey(ws.ref)]
	if handle == nil {
		return nil, nil, NewError(op, ErrorKindFailedPrecondition, "workspace handle is unavailable", false, nil)
	}
	return ws, handle, nil
}

func (e *AgentSandboxExecutor) findUsableWorkspaceAndHandle(op string, ref WorkspaceRef) (*agentSandboxWorkspace, agentSandboxHandle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, err := e.findWorkspaceLocked(op, ref)
	if err != nil {
		return nil, nil, err
	}
	switch ws.phase {
	case PhaseDeleted:
		return nil, nil, NewError(op, ErrorKindNotFound, "workspace is deleted", false, nil)
	case PhaseFailed:
		return nil, nil, NewError(op, ErrorKindFailedPrecondition, "workspace has failed", false, nil)
	}
	handle := e.handles[workspaceKey(ws.ref)]
	if handle == nil {
		return nil, nil, NewError(op, ErrorKindFailedPrecondition, "workspace handle is unavailable", false, nil)
	}
	return ws, handle, nil
}

func (e *AgentSandboxExecutor) findWorkspaceLocked(op string, ref WorkspaceRef) (*agentSandboxWorkspace, error) {
	if ref.IsZero() {
		return nil, NewError(op, ErrorKindInvalidArgument, "workspace reference is required", false, nil)
	}
	if key := workspaceKey(ref); key != "" {
		if ws := e.workspaces[key]; ws != nil {
			return ws, nil
		}
	}
	if ref.ID != "" || ref.SandboxName != "" {
		for _, ws := range e.workspaces {
			if ref.ID != "" && ws.ref.ID == ref.ID {
				return ws, nil
			}
			if ref.SandboxName != "" && ws.ref.SandboxName == ref.SandboxName && (ref.Namespace == "" || ref.Namespace == ws.ref.Namespace) {
				return ws, nil
			}
		}
	}
	return nil, NewError(op, ErrorKindNotFound, "workspace not found", false, nil)
}

func (e *AgentSandboxExecutor) reusableWorkspaceLocked(key string) *agentSandboxWorkspace {
	ws := e.workspaces[key]
	if ws == nil {
		return nil
	}
	switch ws.phase {
	case PhaseDeleted, PhaseFailed, PhaseReleased:
		return nil
	default:
		return ws
	}
}

func (e *AgentSandboxExecutor) removeReuseIndexLocked(ws *agentSandboxWorkspace) {
	if ws.reuseKey == "" {
		return
	}
	key := reuseIndexKey(ws.ref.Namespace, ws.template, ws.reuseKey)
	if e.reuseIndex[key] == workspaceKey(ws.ref) {
		delete(e.reuseIndex, key)
	}
}

func (e *AgentSandboxExecutor) descriptionLocked(ws *agentSandboxWorkspace) *Description {
	artifacts := make([]Artifact, 0, len(ws.artifacts))
	for _, artifact := range ws.artifacts {
		artifacts = append(artifacts, artifact.Artifact)
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	return &Description{
		Ref:         ws.ref,
		Template:    ws.template,
		ReuseKey:    ws.reuseKey,
		Phase:       ws.phase,
		Retained:    ws.retained,
		Message:     ws.message,
		CreatedAt:   ws.createdAt,
		ReadyAt:     ws.readyAt,
		ReleasedAt:  ws.releasedAt,
		DeletedAt:   ws.deletedAt,
		Labels:      copyStringMap(ws.labels),
		Annotations: copyStringMap(ws.annotations),
		Artifacts:   artifacts,
	}
}

func (e *AgentSandboxExecutor) downloadedArtifactMetadata(ws *agentSandboxWorkspace, artifactPath string, data []byte, listed Artifact) Artifact {
	meta := Artifact{
		Path:    artifactPath,
		Size:    int64(len(data)),
		Digest:  digest(data),
		Mode:    agentSandboxDefaultFileMode,
		ModTime: e.now(),
	}
	if listed.Path != "" {
		meta.Mode = listed.Mode
		if !listed.ModTime.IsZero() {
			meta.ModTime = listed.ModTime
		}
	}
	e.mu.Lock()
	if cached, ok := ws.artifacts[artifactPath]; ok {
		if cached.Mode != 0 {
			meta.Mode = cached.Mode
		}
		if !cached.ModTime.IsZero() && listed.Path == "" {
			meta.ModTime = cached.ModTime
		}
	}
	e.mu.Unlock()
	return meta
}

func (e *AgentSandboxExecutor) listAgentSandboxFiles(ctx context.Context, handle agentSandboxHandle, dir string) ([]string, map[string]Artifact, error) {
	entries, err := handle.List(ctx, agentSandboxListPath(dir))
	if err != nil {
		return nil, nil, err
	}

	var files []string
	metadata := make(map[string]Artifact)
	for _, entry := range entries {
		entryPath := agentSandboxJoinListPath(dir, entry.Name)
		if entryPath == "" || entryPath == "." {
			continue
		}
		switch entry.Type {
		case sandbox.FileTypeFile:
			artifact, err := agentSandboxListedArtifact(entryPath, entry)
			if err != nil {
				return nil, nil, err
			}
			files = append(files, artifact.Path)
			metadata[artifact.Path] = artifact
		case sandbox.FileTypeDirectory:
			childFiles, childMetadata, err := e.listAgentSandboxFiles(ctx, handle, entryPath)
			if err != nil {
				return nil, nil, err
			}
			files = append(files, childFiles...)
			maps.Copy(metadata, childMetadata)
		}
	}
	sort.Strings(files)
	return files, metadata, nil
}

func agentSandboxRef(namespace string, handle agentSandboxHandle) (WorkspaceRef, error) {
	claimName := handle.ClaimName()
	if strings.TrimSpace(claimName) == "" {
		return WorkspaceRef{}, fmt.Errorf("empty claim name")
	}
	ref := WorkspaceRef{
		Namespace:   namespace,
		ClaimName:   claimName,
		SandboxName: handle.SandboxName(),
	}
	ref.ID = agentSandboxWorkspaceIDPrefix + workspaceKey(ref)
	return ref, nil
}

func mergeAgentSandboxAnnotations(local, backend map[string]string) map[string]string {
	out := copyStringMap(local)
	if len(backend) == 0 {
		return out
	}
	if out == nil {
		out = make(map[string]string, len(backend))
	}
	for key, value := range backend {
		if _, exists := out[key]; !exists {
			out[key] = value
		}
	}
	return out
}

func (e *AgentSandboxExecutor) cleanupAgentSandboxEnvFile(handle agentSandboxHandle, envFileName string) {
	if handle == nil || strings.TrimSpace(envFileName) == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = handle.Run(cleanupCtx, "rm -f "+shellQuote(agentSandboxExecRoot+envFileName))
}

func agentSandboxCommandString(req ExecRequest, envFilePath string) (string, error) {
	parts := make([]string, 0, len(req.Command)+6)
	if strings.ContainsRune(req.WorkDir, '\x00') {
		return "", fmt.Errorf("workdir contains NUL byte")
	}
	if strings.ContainsRune(envFilePath, '\x00') {
		return "", fmt.Errorf("env file path contains NUL byte")
	}
	if len(req.Env) > 0 && strings.TrimSpace(envFilePath) == "" {
		return "", fmt.Errorf("env file path is required when environment variables are set")
	}

	if strings.TrimSpace(envFilePath) != "" {
		parts = append(parts, agentSandboxEnvFilePrelude(envFilePath))
	}
	if strings.TrimSpace(req.WorkDir) != "" {
		parts = append(parts, "cd", shellQuote(req.WorkDir), "&&")
	}

	for _, arg := range req.Command {
		if strings.ContainsRune(arg, '\x00') {
			return "", fmt.Errorf("command argument contains NUL byte")
		}
		parts = append(parts, shellQuote(arg))
	}
	script := strings.Join(parts, " ")
	return "sh -c " + shellQuote(script), nil
}

func agentSandboxEnvFile(startedAt time.Time, env map[string]string) (string, string, []byte, error) {
	if len(env) == 0 {
		return "", "", nil, nil
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var content strings.Builder
	for _, key := range keys {
		value := env[key]
		if key == "" {
			return "", "", nil, fmt.Errorf("environment variable name is required")
		}
		if strings.Contains(key, "=") {
			return "", "", nil, fmt.Errorf("environment variable %q contains '='", key)
		}
		if !agentSandboxIsShellEnvName(key) {
			return "", "", nil, fmt.Errorf("environment variable %q is not a shell-compatible name", key)
		}
		if strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return "", "", nil, fmt.Errorf("environment variable %q contains NUL byte", key)
		}
		content.WriteString("export ")
		content.WriteString(key)
		content.WriteString("=")
		content.WriteString(shellQuote(value))
		content.WriteByte('\n')
	}

	stamp := startedAt.UnixNano()
	if stamp < 0 {
		stamp = -stamp
	}
	name := fmt.Sprintf("%s%d", agentSandboxEnvFilePrefix, stamp)
	return name, agentSandboxExecRoot + name, []byte(content.String()), nil
}

func agentSandboxIsShellEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' || ('A' <= r && r <= 'Z') || ('a' <= r && r <= 'z') || (i > 0 && '0' <= r && r <= '9') {
			continue
		}
		return false
	}
	return true
}

func agentSandboxEnvFilePrelude(envFilePath string) string {
	return "env_file=" + shellQuote(envFilePath) +
		`; set -a; . "$env_file"; env_status=$?; set +a; rm -f "$env_file"; ` +
		`if [ "$env_status" -ne 0 ]; then exit "$env_status"; fi;`
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func agentSandboxListPath(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" || dir == "." {
		return agentSandboxDefaultRootList
	}
	return strings.TrimPrefix(path.Clean("/"+dir), "/")
}

func agentSandboxJoinListPath(dir, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	cleanName := strings.TrimPrefix(path.Clean("/"+name), "/")
	if cleanName == "." {
		return ""
	}
	cleanDir := agentSandboxListPath(dir)
	if cleanDir == agentSandboxDefaultRootList {
		return cleanName
	}
	if cleanName == cleanDir || strings.HasPrefix(cleanName, cleanDir+"/") {
		return cleanName
	}
	return path.Join(cleanDir, cleanName)
}

func agentSandboxListedArtifact(artifactPath string, entry sandbox.FileEntry) (Artifact, error) {
	cleanPath, err := cleanArtifactPath(artifactPath)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{
		Path:    cleanPath,
		Size:    entry.Size,
		Mode:    agentSandboxDefaultFileMode,
		ModTime: entry.ModTime,
	}, nil
}

func agentSandboxError(op string, err error) error {
	if err == nil {
		return nil
	}
	if workspaceErr, ok := errors.AsType[*Error](err); ok {
		return workspaceErr
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return NewError(op, ErrorKindTimeout, "operation timed out", true, err)
	}
	if errors.Is(err, context.Canceled) {
		return NewError(op, ErrorKindCanceled, "operation canceled", true, err)
	}

	if httpErr, ok := errors.AsType[*sandbox.HTTPError](err); ok {
		return agentSandboxHTTPError(op, httpErr, err)
	}

	switch {
	case errors.Is(err, sandbox.ErrTimeout):
		return NewError(op, ErrorKindTimeout, "agent sandbox operation timed out", true, err)
	case errors.Is(err, sandbox.ErrNotReady):
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox is not ready", true, err)
	case errors.Is(err, sandbox.ErrAlreadyOpen):
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox is already open", false, err)
	case errors.Is(err, sandbox.ErrOrphanedClaim):
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox claim is orphaned", false, err)
	case errors.Is(err, sandbox.ErrSandboxDeleted):
		return NewError(op, ErrorKindNotFound, "agent sandbox was deleted", false, err)
	case errors.Is(err, sandbox.ErrGatewayDeleted):
		return NewError(op, ErrorKindNotFound, "agent sandbox gateway was deleted", true, err)
	case errors.Is(err, sandbox.ErrPortForwardDied):
		return NewError(op, ErrorKindUnknown, "agent sandbox port-forward connection lost", true, err)
	case errors.Is(err, sandbox.ErrRetriesExhausted):
		return NewError(op, ErrorKindUnknown, "agent sandbox retries exhausted", true, err)
	case errors.Is(err, sandbox.ErrResponseTooLarge):
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox response is too large", false, err)
	case errors.Is(err, sandbox.ErrClaimFailed):
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox claim failed", false, err)
	}

	if k8serrors.IsNotFound(err) {
		return NewError(op, ErrorKindNotFound, "Kubernetes resource not found", false, err)
	}
	if k8serrors.IsAlreadyExists(err) {
		return NewError(op, ErrorKindAlreadyExists, "Kubernetes resource already exists", false, err)
	}
	if k8serrors.IsTimeout(err) || k8serrors.IsServerTimeout(err) {
		return NewError(op, ErrorKindTimeout, "Kubernetes operation timed out", true, err)
	}
	if k8serrors.IsTooManyRequests(err) || k8serrors.IsServiceUnavailable(err) {
		return NewError(op, ErrorKindUnknown, "Kubernetes API is temporarily unavailable", true, err)
	}
	if k8serrors.IsInvalid(err) || k8serrors.IsBadRequest(err) {
		return NewError(op, ErrorKindInvalidArgument, "Kubernetes rejected the request", false, err)
	}
	if k8serrors.IsForbidden(err) || k8serrors.IsUnauthorized(err) {
		return NewError(op, ErrorKindFailedPrecondition, "Kubernetes access denied", false, err)
	}
	if agentSandboxIsValidationError(err) {
		return NewError(op, ErrorKindInvalidArgument, "invalid agent sandbox options", false, err)
	}
	return NewError(op, ErrorKindUnknown, "agent sandbox operation failed", false, err)
}

func agentSandboxHTTPError(op string, httpErr *sandbox.HTTPError, err error) error {
	switch httpErr.StatusCode {
	case http.StatusBadRequest:
		return NewError(op, ErrorKindInvalidArgument, "agent sandbox rejected the request", false, err)
	case http.StatusUnauthorized, http.StatusForbidden:
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox access denied", false, err)
	case http.StatusNotFound:
		return NewError(op, ErrorKindNotFound, "agent sandbox resource not found", false, err)
	case http.StatusConflict:
		return NewError(op, ErrorKindFailedPrecondition, "agent sandbox resource conflict", false, err)
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return NewError(op, ErrorKindTimeout, "agent sandbox request timed out", true, err)
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusInternalServerError:
		return NewError(op, ErrorKindUnknown, "agent sandbox service is temporarily unavailable", true, err)
	default:
		retryable := httpErr.StatusCode >= 500
		return NewError(op, ErrorKindUnknown, fmt.Sprintf("agent sandbox returned HTTP %d", httpErr.StatusCode), retryable, err)
	}
}

func agentSandboxIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sandbox.ErrSandboxDeleted) || k8serrors.IsNotFound(err) {
		return true
	}
	var httpErr *sandbox.HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound
}

func agentSandboxIsValidationError(err error) bool {
	message := err.Error()
	return strings.HasPrefix(message, "sandbox: ") &&
		(strings.Contains(message, " is required") ||
			strings.Contains(message, " is not a valid ") ||
			strings.Contains(message, " must "))
}
