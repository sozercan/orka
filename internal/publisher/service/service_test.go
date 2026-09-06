package service

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/publisher"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

const (
	testNamespace      = "default"
	testMainRef        = "refs/heads/" + githubTestBaseBranch
	testPublicationRef = "refs/heads/orka/test-publication"
)

type serviceFixture struct {
	server          *Server
	httpServer      *httptest.Server
	client          *Client
	artifact        *artifactFixture
	bearer          []byte
	operationSecret []byte
	artifactSecret  []byte
	config          Config
}

type artifactFixture struct {
	service *artifactcap.Service
	server  *httptest.Server
	secret  []byte
	root    string
	calls   atomic.Int64
}

type artifactAuthorizerFunc func(context.Context, ArtifactAuthorizationRequest) (artifactcap.Authorization, error)

func (f artifactAuthorizerFunc) Authorize(ctx context.Context, request ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
	return f(ctx, request)
}

func TestWorkspaceResolveAndPrepareAcceptExactCommit(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	resolve, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-resolve-exact", TaskID: "task-exact"},
		Source:   repository.source, SourceRef: repository.baselineOID,
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace exact commit: %v", err)
	}
	if resolve.SourceRef != repository.baselineOID || resolve.BaselineOID != repository.baselineOID {
		t.Fatalf("exact resolve = %#v, want baseline %s", resolve, repository.baselineOID)
	}
	prepared, err := fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-prepare-exact", TaskID: "task-exact"},
		Source:   repository.source, SourceRef: repository.baselineOID, BaselineOID: repository.baselineOID,
	})
	if err != nil {
		t.Fatalf("PrepareWorkspace exact commit: %v", err)
	}
	if prepared.SourceRef != repository.baselineOID || prepared.BaselineOID != repository.baselineOID || prepared.Artifact.Digest == "" {
		t.Fatalf("exact prepared workspace = %#v", prepared)
	}
	_, err = fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-prepare-exact-mismatch", TaskID: "task-exact"},
		Source:   repository.source, SourceRef: repository.baselineOID, BaselineOID: strings.Repeat("a", 40),
	})
	assertClientCode(t, err, "invalid_request")
}

func TestWorkspacePrepareRetriesRetryableArtifactAuthorizationFailure(t *testing.T) {
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	local := &localArtifactAuthorizer{secret: fixture.artifactSecret, ttl: time.Minute, now: time.Now}
	var authorizationCalls atomic.Int64
	fixture.server.artifacts.authorizer = artifactAuthorizerFunc(func(ctx context.Context, request ArtifactAuthorizationRequest) (artifactcap.Authorization, error) {
		if authorizationCalls.Add(1) == 1 {
			return artifactcap.Authorization{}, apiError(
				ErrArtifactTransport, "artifact_authorization_transport_failed",
				"artifact authorization broker transport failed", http.StatusServiceUnavailable, true, context.DeadlineExceeded,
			)
		}
		return local.Authorize(ctx, request)
	})
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{
			Namespace: testNamespace, OperationID: "workspace-prepare-retryable-authorization", TaskID: "task-retryable-authorization",
		},
		Source: repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}

	_, err := fixture.client.PrepareWorkspace(context.Background(), request)
	var first *ClientError
	if !errors.As(err, &first) || first.StatusCode != http.StatusServiceUnavailable || !first.Response.Retryable ||
		first.Response.Code != "artifact_authorization_failed" {
		t.Fatalf("first PrepareWorkspace error = %#v, want retryable artifact authorization failure", err)
	}
	prepared, err := fixture.client.PrepareWorkspace(context.Background(), request)
	if err != nil {
		t.Fatalf("retry PrepareWorkspace: %v", err)
	}
	if prepared.Artifact.Digest == "" || authorizationCalls.Load() != 2 {
		t.Fatalf("retry result = %#v, authorization calls = %d", prepared, authorizationCalls.Load())
	}
}

func TestWorkspaceResolveFreezesAdvertisedDefaultBranch(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	resolved, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-resolve-default", TaskID: "task-default"},
		Source:   repository.source,
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace default branch: %v", err)
	}
	if resolved.SourceRef != "refs/heads/main" || resolved.BaselineOID != repository.baselineOID {
		t.Fatalf("default resolve = %#v, want refs/heads/main at %s", resolved, repository.baselineOID)
	}
}

func TestWorkspaceResolveRejectsUnbornDefaultBranchAsAbsent(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	_, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-resolve-unborn", TaskID: "task-unborn"},
		Source:   repository.target,
	})
	assertClientCode(t, err, "source_ref_absent")
}

func TestWorkspaceResolveAndPrepareAcceptBranches(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)

	var effectManifestDigest, effectArtifactDigest string
	for index, test := range []struct {
		name      string
		sourceRef string
	}{
		{name: "short", sourceRef: githubTestBaseBranch},
		{name: "fully-qualified", sourceRef: testMainRef},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolve, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-resolve-branch-%d", index),
					TaskID: fmt.Sprintf("task-resolve-branch-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef,
			})
			if err != nil {
				t.Fatalf("ResolveWorkspace branch %q: %v", test.sourceRef, err)
			}
			if resolve.SourceRef != testMainRef || resolve.BaselineOID != repository.baselineOID {
				t.Fatalf("branch resolve = %#v, want ref %q at %s", resolve, testMainRef, repository.baselineOID)
			}

			prepared, err := fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-prepare-branch-%d", index),
					TaskID: fmt.Sprintf("task-prepare-branch-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef, BaselineOID: resolve.BaselineOID,
			})
			if err != nil {
				t.Fatalf("PrepareWorkspace branch %q: %v", test.sourceRef, err)
			}
			if prepared.SourceRef != testMainRef || prepared.BaselineOID != repository.baselineOID || prepared.Artifact.Digest == "" {
				t.Fatalf("branch prepared workspace = %#v", prepared)
			}
			if index == 0 {
				effectManifestDigest = prepared.ManifestDigest
				effectArtifactDigest = prepared.Artifact.Digest
				return
			}
			if prepared.ManifestDigest != effectManifestDigest || prepared.Artifact.Digest != effectArtifactDigest {
				t.Fatalf("equivalent branch inputs produced different effects: manifest %q vs %q, artifact %q vs %q",
					prepared.ManifestDigest, effectManifestDigest, prepared.Artifact.Digest, effectArtifactDigest)
			}
		})
	}
}

func TestWorkspaceResolveAndPrepareAcceptTags(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	const shortTag = "v1.2.3"
	canonicalTag := "refs/tags/" + shortTag
	runGitEnv(t, repository.seed, fixedGitEnv(), "tag", "-a", shortTag, "-m", "release")
	runGit(t, repository.seed, "push", repository.source.URL, canonicalTag)

	var effectManifestDigest, effectArtifactDigest string
	for index, test := range []struct {
		name      string
		sourceRef string
	}{
		{name: "short", sourceRef: shortTag},
		{name: "fully-qualified", sourceRef: canonicalTag},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolve, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-resolve-tag-%d", index),
					TaskID: fmt.Sprintf("task-resolve-tag-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef,
			})
			if err != nil {
				t.Fatalf("ResolveWorkspace tag %q: %v", test.sourceRef, err)
			}
			if resolve.SourceRef != canonicalTag || resolve.BaselineOID != repository.baselineOID {
				t.Fatalf("tag resolve = %#v, want ref %q at %s", resolve, canonicalTag, repository.baselineOID)
			}

			prepared, err := fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-prepare-tag-%d", index),
					TaskID: fmt.Sprintf("task-prepare-tag-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef, BaselineOID: resolve.BaselineOID,
			})
			if err != nil {
				t.Fatalf("PrepareWorkspace tag %q: %v", test.sourceRef, err)
			}
			if prepared.SourceRef != canonicalTag || prepared.BaselineOID != repository.baselineOID || prepared.Artifact.Digest == "" {
				t.Fatalf("tag prepared workspace = %#v", prepared)
			}
			if index == 0 {
				effectManifestDigest = prepared.ManifestDigest
				effectArtifactDigest = prepared.Artifact.Digest
				return
			}
			if prepared.ManifestDigest != effectManifestDigest || prepared.Artifact.Digest != effectArtifactDigest {
				t.Fatalf("equivalent tag inputs produced different effects: manifest %q vs %q, artifact %q vs %q",
					prepared.ManifestDigest, effectManifestDigest, prepared.Artifact.Digest, effectArtifactDigest)
			}
		})
	}
}

func TestWorkspaceResolveAndPrepareRejectAmbiguousBareRef(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	runGitEnv(t, repository.seed, fixedGitEnv(), "tag", "-a", githubTestBaseBranch, "-m", "ambiguous")
	runGit(t, repository.seed, "push", repository.source.URL, "refs/tags/"+githubTestBaseBranch)

	for index, sourceRef := range []string{testMainRef, "refs/tags/main"} {
		resolved, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
			Metadata: OperationMetadata{
				Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-resolve-explicit-%d", index),
				TaskID: fmt.Sprintf("task-resolve-explicit-%d", index),
			},
			Source: repository.source, SourceRef: sourceRef,
		})
		if err != nil {
			t.Fatalf("ResolveWorkspace explicit ref %q: %v", sourceRef, err)
		}
		if resolved.SourceRef != sourceRef || resolved.BaselineOID != repository.baselineOID {
			t.Fatalf("explicit ref resolve = %#v, want %q at %s", resolved, sourceRef, repository.baselineOID)
		}
	}

	_, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-resolve-ambiguous", TaskID: "task-resolve-ambiguous"},
		Source:   repository.source, SourceRef: githubTestBaseBranch,
	})
	assertClientCode(t, err, "source_ref_ambiguous")

	_, err = fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-prepare-ambiguous", TaskID: "task-prepare-ambiguous"},
		Source:   repository.source, SourceRef: githubTestBaseBranch, BaselineOID: repository.baselineOID,
	})
	assertClientCode(t, err, "source_ref_ambiguous")
}

func TestWorkspaceResolveAndPrepareRejectInvalidSourceRefs(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	for index, test := range []struct {
		name      string
		sourceRef string
	}{
		{name: "empty-canonical-tag", sourceRef: "refs/tags/"},
		{name: "canonical-forbidden-sequence", sourceRef: "refs/tags/release..candidate"},
		{name: "canonical-forbidden-component", sourceRef: "refs/tags/.hidden"},
		{name: "short-forbidden-sequence", sourceRef: "release@{candidate"},
		{name: "short-leading-dash", sourceRef: "-release"},
		{name: "unsupported-ref-namespace", sourceRef: "refs/remotes/origin/main"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := fixture.client.ResolveWorkspace(context.Background(), WorkspaceResolveRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-resolve-invalid-tag-%d", index),
					TaskID: fmt.Sprintf("task-resolve-invalid-tag-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef,
			})
			assertClientCode(t, err, "invalid_request")

			_, err = fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
				Metadata: OperationMetadata{
					Namespace: testNamespace, OperationID: fmt.Sprintf("workspace-prepare-invalid-tag-%d", index),
					TaskID: fmt.Sprintf("task-prepare-invalid-tag-%d", index),
				},
				Source: repository.source, SourceRef: test.sourceRef, BaselineOID: repository.baselineOID,
			})
			assertClientCode(t, err, "invalid_request")
		})
	}
}

func TestWorkspacePrepareUploadsDeterministicSanitizedArtifact(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-prepare-1", TaskID: "task-uid-1"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}
	response, err := fixture.client.PrepareWorkspace(context.Background(), request)
	if err != nil {
		t.Fatalf("PrepareWorkspace: %v", err)
	}
	if response.RepositoryID != repository.source.ID || response.BaselineOID != repository.baselineOID ||
		response.Artifact.MediaType != artifactcap.MediaTypeWorkspaceTar || response.EntryCount < 3 {
		t.Fatalf("unexpected response: %#v", response)
	}
	calls := fixture.artifact.calls.Load()
	repeated, err := fixture.client.PrepareWorkspace(context.Background(), request)
	if err != nil {
		t.Fatalf("repeat PrepareWorkspace: %v", err)
	}
	if repeated != response {
		t.Fatalf("replayed response changed:\nfirst=%#v\nsecond=%#v", response, repeated)
	}
	if fixture.artifact.calls.Load() != calls {
		t.Fatal("journal replay repeated the artifact upload")
	}
	archive := fixture.artifact.download(t, artifactcap.Identity{Namespace: testNamespace, TaskID: "task-uid-1"}, response.Artifact)
	entries := readWorkspaceTar(t, archive)
	if got := entries["keep.txt"]; got != "old\n" {
		t.Fatalf("keep.txt = %q", got)
	}
	if got := entries["bin/run"]; got != "#!/bin/sh\nexit 0\n" {
		t.Fatalf("bin/run = %q", got)
	}
	secondRequest := request
	secondRequest.Metadata.OperationID = "workspace-prepare-2"
	second, err := fixture.client.PrepareWorkspace(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("second PrepareWorkspace: %v", err)
	}
	if second.Artifact.Digest != response.Artifact.Digest || second.ManifestDigest != response.ManifestDigest {
		t.Fatalf("workspace preparation is not deterministic: %#v vs %#v", response, second)
	}
}

func TestWorkspacePreparePreservesSafeSymlink(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, true)
	fixture := newServiceFixture(t)
	response, err := fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-safe-symlink", TaskID: "task-safe-symlink"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	})
	if err != nil {
		t.Fatalf("PrepareWorkspace safe symlink: %v", err)
	}
	archive := fixture.artifact.download(t, artifactcap.Identity{Namespace: testNamespace, TaskID: "task-safe-symlink"}, response.Artifact)
	entries := readWorkspaceTar(t, archive)
	if got := entries["latest"]; got != "symlink:keep.txt" {
		t.Fatalf("materialized symlink metadata = %q, want %q", got, "symlink:keep.txt")
	}
}

func TestWorkspacePrepareRejectsUnsafeSymlinkAndSourceMovement(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	if err := os.Symlink("../../outside", filepath.Join(repository.seed, "escape")); err != nil {
		t.Fatal(err)
	}
	runGitEnv(t, repository.seed, fixedGitEnv(), "add", "--all")
	runGitEnv(t, repository.seed, fixedGitEnv(), "commit", "-m", "unsafe symlink")
	repository.baselineOID = strings.TrimSpace(runGit(t, repository.seed, "rev-parse", "HEAD"))
	runGit(t, repository.seed, "push", repository.source.URL, "HEAD:refs/heads/main")
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-symlink", TaskID: "task-symlink"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}
	_, err := fixture.client.PrepareWorkspace(context.Background(), request)
	assertClientCode(t, err, "invalid_request")

	moved := newRepositoryFixture(t, false)
	commitFile(t, moved.seed, "moved.txt", "moved\n")
	runGit(t, moved.seed, "push", moved.source.URL, "HEAD:refs/heads/main")
	request.Metadata = OperationMetadata{Namespace: testNamespace, OperationID: "workspace-moved", TaskID: "task-moved"}
	request.Source = moved.source
	request.BaselineOID = moved.baselineOID
	_, err = fixture.client.PrepareWorkspace(context.Background(), request)
	assertClientCode(t, err, "source_moved")
}

func TestOperationAuthorizationBodyBindingAndNoSecretEcho(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-auth", TaskID: "task-auth"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalRequestBody(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := RequestDigest(http.MethodPost, WorkspacePreparePath, canonical)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := SignCapability(fixture.operationSecret, NewClaims(OperationWorkspacePrepare, request.Metadata, digest, time.Now().UTC(), time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(canonical, []byte(testMainRef), []byte("refs/heads/evil"), 1)
	httpRequest, err := http.NewRequest(http.MethodPost, fixture.httpServer.URL+WorkspacePreparePath, bytes.NewReader(mutated))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+string(fixture.bearer))
	httpRequest.Header.Set(OperationCapabilityHeader, capability)
	httpRequest.Header.Set(OperationRequestDigestHeader, digest)
	response, err := fixture.httpServer.Client().Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, body=%s", response.StatusCode, data)
	}
	for _, secret := range [][]byte{fixture.bearer, fixture.operationSecret, fixture.artifactSecret, []byte(capability)} {
		if bytes.Contains(data, secret) {
			t.Fatalf("response echoed secret material: %q", data)
		}
	}

	duplicate := strings.Replace(string(canonical), `"sourceRef":`, `"sourceRef":testMainRef,"sourceRef":`, 1)
	httpRequest, _ = http.NewRequest(http.MethodPost, fixture.httpServer.URL+WorkspacePreparePath, strings.NewReader(duplicate))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+string(fixture.bearer))
	response, err = fixture.httpServer.Client().Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate-key status = %d", response.StatusCode)
	}
}

func TestOperationJournalRejectsConflictingReuse(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-conflict", TaskID: "task-conflict"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}
	if _, err := fixture.client.PrepareWorkspace(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.Limits.MaxEntries = 50
	_, err := fixture.client.PrepareWorkspace(context.Background(), request)
	assertClientCode(t, err, "operation_conflict")
}

func TestPublicationPreparePublishVerifyLocalBareRepository(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	fixture := newServiceFixture(t)
	delta := buildDelta(t, repository.seed)
	publicationID := "publication-uid-1"
	deltaReference := fixture.artifact.upload(t, artifactcap.Identity{Namespace: testNamespace, PublicationID: publicationID}, "seed-delta", artifactcap.MediaTypeWorkspaceDelta, delta.Artifact)

	prepareMetadata := OperationMetadata{Namespace: testNamespace, OperationID: "publication-prepare-1", PublicationID: publicationID}
	prepareRequest := publisher.PrepareRequest{
		PublicationID: publicationID, PublicationGeneration: 1, OperationID: prepareMetadata.OperationID,
		Source: repository.source, SourceRef: githubTestBaseBranch, Target: repository.target, TargetRef: testPublicationRef,
		BranchClaimGeneration: 1, BaselineOID: repository.baselineOID, RemoteBefore: publisher.RemoteRef{Absent: true},
		DeltaArtifactDigest: delta.ArtifactDigest, CommitMessage: "Orka publication publication-uid-1\n",
		CommitTimestamp: time.Date(2026, time.July, 24, 12, 34, 56, 0, time.UTC),
	}
	preparedResponse, err := fixture.client.PreparePublication(context.Background(), PublicationPrepareRequest{
		Metadata: prepareMetadata, DeltaArtifact: deltaReference, Request: prepareRequest,
	})
	if err != nil {
		t.Fatalf("PreparePublication: %v", err)
	}
	prepared := preparedResponse.Prepared
	if prepared.SourceRef != testMainRef || prepared.CommitOID == "" || prepared.BundleDigest == "" {
		t.Fatalf("incomplete or non-canonical prepared publication: %#v", prepared)
	}
	encoded, _ := json.Marshal(preparedResponse)
	if bytes.Contains(encoded, []byte("bundlePath")) || bytes.Contains(encoded, []byte(fixture.config.ArtifactRoot)) {
		t.Fatalf("response exposed local durable path: %s", encoded)
	}
	// Simulate complete loss of the Publisher-local prepared cache. Publish and
	// Verify must recover exclusively from the controller artifact API receipt.
	if err := os.RemoveAll(filepath.Join(fixture.config.ArtifactRoot, publicationID)); err != nil {
		t.Fatal(err)
	}
	claim := publisher.BranchClaim{
		RepositoryID: repository.target.ID, Ref: testPublicationRef, OwnerKind: "Task", OwnerUID: "task-uid-publish",
		Generation: 1, LastVerified: publisher.RemoteRef{Absent: true},
	}
	preflight, err := fixture.client.PreflightPublication(context.Background(), PublicationPreflightRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "publication-preflight-1", PublicationID: publicationID},
		Request:  publisher.PreflightRequest{Target: repository.target, Claim: claim},
	})
	if err != nil || !preflight.Result.Matches || !preflight.Result.Observed.Absent {
		t.Fatalf("PreflightPublication = %#v, %v", preflight, err)
	}
	publishRequest := publisher.PublishRequest{
		PublicationID: publicationID, PublicationGeneration: 1, OperationID: "publication-publish-1",
		Target: repository.target, TargetRef: testPublicationRef, Claim: claim, RemoteBefore: publisher.RemoteRef{Absent: true},
		ExpectedCommitOID: prepared.CommitOID, BundleDigest: prepared.BundleDigest,
	}
	published, err := fixture.client.Publish(context.Background(), PublicationPublishRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: publishRequest.OperationID, PublicationID: publicationID},
		Prepared: prepared, Request: publishRequest,
	})
	if err != nil || published.Receipt.Outcome != publisher.PublishAcknowledged {
		t.Fatalf("Publish = %#v, %v", published, err)
	}
	verifyRequest := publisher.VerifyRequest{
		PublicationID: publicationID, PublicationGeneration: 1, OperationID: "publication-verify-1",
		Target: repository.target, TargetRef: testPublicationRef, Claim: claim,
		ExpectedCommitOID: prepared.CommitOID, BundleDigest: prepared.BundleDigest,
	}
	verified, err := fixture.client.Verify(context.Background(), PublicationVerifyRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: verifyRequest.OperationID, PublicationID: publicationID},
		Prepared: prepared, Request: verifyRequest,
	})
	if err != nil || verified.Receipt.Outcome != publisher.VerifiedExact {
		t.Fatalf("Verify = %#v, %v", verified, err)
	}
	observed := strings.TrimSpace(runGit(t, "", "ls-remote", "--refs", repository.target.URL, testPublicationRef))
	if !strings.HasPrefix(observed, prepared.CommitOID+"\t") {
		t.Fatalf("target ref = %q, want %s", observed, prepared.CommitOID)
	}
}

func TestExactPullRequestIntentAndCredentialFileBoundary(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	credentialRoot := t.TempDir()
	if err := os.Chmod(credentialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	credentialPath := filepath.Join(credentialRoot, "forge-token")
	if err := os.WriteFile(credentialPath, []byte("Authorization: Bearer forge-secret-marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	var ephemeralCredentialPath string
	factory := PRReconcilerFactoryFunc(func(_ context.Context, path string) (publisher.PullRequestReconciler, error) {
		ephemeralCredentialPath = path
		if path == "" || path == credentialPath {
			t.Fatalf("factory received non-ephemeral credential path %q", path)
		}
		value, err := os.ReadFile(path)
		if err != nil || string(value) != "forge-secret-marker" {
			t.Fatalf("ephemeral credential = %q, %v", value, err)
		}
		return pullRequestReconcilerFunc(func(_ context.Context, intent publisher.PullRequestIntent) (publisher.PullRequestReceipt, error) {
			key, err := intent.Key()
			if err != nil {
				return publisher.PullRequestReceipt{}, err
			}
			return publisher.PullRequestReceipt{IntentKey: key, ForgeID: "pr-42", URL: "https://forge.example/pr/42", State: publisher.PullRequestOpen, HeadOID: intent.ExpectedHeadOID}, nil
		}), nil
	})
	fixture := newServiceFixtureWithOptions(t, factory, func(config *Config) { config.CredentialRoot = credentialRoot })
	intent := publisher.PullRequestIntent{
		BaseRepository: repository.source, BaseRef: testMainRef, HeadRepository: repository.target,
		HeadRef: testPublicationRef, PublicationGeneration: 1, ExpectedHeadOID: repository.baselineOID,
	}
	response, err := fixture.client.ReconcilePullRequest(context.Background(), PullRequestReconcileRequest{
		Metadata:      OperationMetadata{Namespace: testNamespace, OperationID: "pr-reconcile-1", PublicationID: "publication-pr-1"},
		CredentialRef: &CredentialReference{Name: "forge-token", Kind: CredentialForgeToken}, Intent: intent,
	})
	if err != nil || response.Receipt.ForgeID != "pr-42" {
		t.Fatalf("ReconcilePullRequest = %#v, %v", response, err)
	}
	if ephemeralCredentialPath == "" {
		t.Fatal("factory did not receive an ephemeral credential")
	}
	if _, err := os.Stat(ephemeralCredentialPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ephemeral credential still exists: %v", err)
	}
}

func TestCredentialTraversalAndSymlinkAreRejected(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	credentialRoot := t.TempDir()
	if err := os.Chmod(credentialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(outside, []byte("Authorization: Bearer secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(credentialRoot, "linked")); err != nil {
		t.Fatal(err)
	}
	fixture := newServiceFixtureWithOptions(t, nil, func(config *Config) { config.CredentialRoot = credentialRoot })
	httpsSource := repository.source
	httpsSource.URL = "https://example.invalid/repository.git"
	base := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "credential-linked", TaskID: "task-credential"},
		Source:   httpsSource, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
		CredentialRef: &CredentialReference{Name: "linked", Kind: CredentialHTTPExtraHeader},
	}
	_, err := fixture.client.PrepareWorkspace(context.Background(), base)
	assertClientCode(t, err, "credential_unavailable")
	base.Metadata.OperationID = "credential-traversal"
	base.CredentialRef.Name = "../token"
	_, err = fixture.client.PrepareWorkspace(context.Background(), base)
	assertClientCode(t, err, "invalid_credential_ref")
}

func TestCapabilitiesDeclareSeparateNoProviderIdentity(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	capabilities, err := fixture.client.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.NetworkIdentity != "workspace-publisher" || capabilities.ProviderOrMCPAccess || capabilities.RedirectsAllowed ||
		capabilities.Protocol != ProtocolVersion || capabilities.GitVersion == "" || capabilities.PullRequestReconciliation ||
		slices.Contains(capabilities.Operations, OperationPullRequestReconcile) || slices.Contains(capabilities.CredentialKinds, CredentialForgeToken) {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestCapabilitiesAdvertiseConfiguredPullRequestReconciliation(t *testing.T) {
	t.Parallel()
	factory := PRReconcilerFactoryFunc(func(context.Context, string) (publisher.PullRequestReconciler, error) {
		return nil, errors.New("not invoked by capability discovery")
	})
	fixture := newServiceFixtureWithOptions(t, factory, nil)
	capabilities, err := fixture.client.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.PullRequestReconciliation || !slices.Contains(capabilities.Operations, OperationPullRequestReconcile) ||
		!slices.Contains(capabilities.CredentialKinds, CredentialForgeToken) {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	return newServiceFixtureWithOptions(t, nil, nil)
}

func newServiceFixtureWithOptions(t *testing.T, factory PRReconcilerFactory, edit func(*Config)) *serviceFixture {
	t.Helper()
	artifactSecret := []byte(strings.Repeat("a", artifactcap.MinSecretBytes))
	artifact := newArtifactFixture(t, artifactSecret)
	bearer := []byte("controller-bearer-token-for-tests")
	operationSecret := []byte(strings.Repeat("o", MinSecretBytes))
	gitBinary, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitBinary, err = filepath.Abs(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		ControllerBearerToken: bearer, OperationCapabilitySecret: operationSecret,
		ArtifactCapabilitySecret: artifactSecret, ArtifactAPIURL: artifact.server.URL,
		ArtifactRoot: filepath.Join(t.TempDir(), "publication-artifacts"), JournalRoot: filepath.Join(t.TempDir(), "journal"),
		TempRoot: filepath.Join(t.TempDir(), "tmp"), GitBinary: gitBinary, AllowFileRepositories: true,
		MaxConcurrentOperations: 4, MaxRequestBytes: 2 << 20, MaxResponseBytes: 2 << 20,
		MaxJournalBytes: 512 << 20, MaxDeltaBytes: 32 << 20, MaxBundleBytes: 64 << 20,
		MaxCommandOutput: 4 << 20, PublishTimeout: 10 * time.Second, VerifyAttempts: 1,
		VerifyBackoff: time.Millisecond, ArtifactTimeout: 10 * time.Second, CapabilityTTL: time.Minute,
		WorkspaceLimits: WorkspaceLimits{MaxEntries: 1000, MaxFileBytes: 4 << 20, MaxExpandedBytes: 16 << 20, MaxArtifactBytes: 32 << 20, MaxPathBytes: 4096},
		PRFactory:       factory, HTTPClient: artifact.server.Client(),
	}
	if edit != nil {
		edit(&config)
	}
	server, err := New(config)
	if err != nil {
		t.Fatalf("New service: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	client, err := NewClient(ClientConfig{
		BaseURL: httpServer.URL, HTTPClient: httpServer.Client(), BearerToken: bearer,
		CapabilitySecret: operationSecret, CapabilityTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &serviceFixture{
		server: server, httpServer: httpServer, client: client, artifact: artifact,
		bearer: bearer, operationSecret: operationSecret, artifactSecret: artifactSecret, config: config,
	}
}

func newArtifactFixture(t *testing.T, secret []byte) *artifactFixture {
	t.Helper()
	root := t.TempDir()
	service, err := artifactcap.NewService(artifactcap.ServiceConfig{Root: root, Secret: secret, MaxObjectBytes: 128 << 20})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &artifactFixture{service: service, secret: secret, root: root}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fixture.calls.Add(1)
		presented, token, err := presentedArtifactRequest(request)
		if err != nil {
			http.Error(writer, "invalid", http.StatusBadRequest)
			return
		}
		switch request.Method {
		case http.MethodPut:
			artifact, err := service.Upload(request.Context(), token, presented, request.Body)
			if err != nil {
				http.Error(writer, "rejected", artifactErrorStatus(err))
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(artifact)
		case http.MethodGet:
			download, err := service.OpenDownload(request.Context(), token, presented)
			if err != nil {
				http.Error(writer, "rejected", artifactErrorStatus(err))
				return
			}
			writer.Header().Set("Content-Type", download.Artifact.MediaType)
			writer.Header().Set("Content-Length", strconv.FormatInt(download.Artifact.SizeBytes, 10))
			writer.Header().Set(artifactcap.ObjectDigestHeader, download.Artifact.Digest)
			writer.WriteHeader(http.StatusOK)
			_, _ = io.Copy(writer, download)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func presentedArtifactRequest(request *http.Request) (artifactcap.PresentedRequest, string, error) {
	digestHex := filepath.Base(request.URL.Path)
	if len(digestHex) != 64 {
		return artifactcap.PresentedRequest{}, "", fmt.Errorf("digest")
	}
	length, err := strconv.ParseInt(request.Header.Get(artifactcap.ContentLengthHeader), 10, 64)
	if err != nil {
		return artifactcap.PresentedRequest{}, "", err
	}
	presented := artifactcap.PresentedRequest{
		Method: request.Method, Path: request.URL.Path, ObjectDigest: "sha256:" + digestHex,
		ContentLength: length, MediaType: request.Header.Get(artifactcap.MediaTypeHeader),
		RequestDigest: request.Header.Get(artifactcap.RequestDigestHeader),
	}
	if err := presented.Validate(); err != nil {
		return artifactcap.PresentedRequest{}, "", err
	}
	return presented, request.Header.Get(artifactcap.CapabilityHeader), nil
}

func artifactErrorStatus(err error) int {
	switch {
	case errors.Is(err, artifactcap.ErrReplay), errors.Is(err, artifactcap.ErrOperationConflict):
		return http.StatusConflict
	case errors.Is(err, artifactcap.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, artifactcap.ErrUnauthorized), errors.Is(err, artifactcap.ErrExpired):
		return http.StatusForbidden
	default:
		return http.StatusBadRequest
	}
}

func (f *artifactFixture) upload(t *testing.T, identity artifactcap.Identity, operationID, mediaType string, data []byte) harnessv2.ArtifactReference {
	t.Helper()
	digest := artifactcap.DigestBytes(data)
	artifactID, err := artifactcap.ArtifactIDForDigest(digest)
	if err != nil {
		t.Fatal(err)
	}
	binding := artifactcap.OperationRequest{Operation: artifactcap.OperationUpload, ObjectDigest: digest, Identity: identity, ContentLength: int64(len(data)), MediaType: mediaType, OperationID: operationID}
	authorization, err := artifactcap.Issue(f.secret, binding, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := artifactcap.ObjectPath(digest)
	request, _ := http.NewRequest(http.MethodPut, f.server.URL+endpoint, bytes.NewReader(data))
	request.ContentLength = int64(len(data))
	request.Header.Set("Content-Type", mediaType)
	request.Header.Set(artifactcap.CapabilityHeader, authorization.Capability)
	request.Header.Set(artifactcap.RequestDigestHeader, authorization.RequestDigest)
	request.Header.Set(artifactcap.ContentLengthHeader, strconv.Itoa(len(data)))
	request.Header.Set(artifactcap.MediaTypeHeader, mediaType)
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("artifact upload status = %d", response.StatusCode)
	}
	return harnessv2.ArtifactReference{ArtifactID: harnessv2.ArtifactID(artifactID), Digest: digest, SizeBytes: int64(len(data)), MediaType: mediaType}
}

func (f *artifactFixture) download(t *testing.T, identity artifactcap.Identity, reference harnessv2.ArtifactReference) []byte {
	t.Helper()
	binding := artifactcap.OperationRequest{Operation: artifactcap.OperationDownload, ObjectDigest: reference.Digest, Identity: identity, ContentLength: reference.SizeBytes, MediaType: reference.MediaType, OperationID: "download-" + hashID(string(reference.ArtifactID), identity.TaskID, identity.PublicationID)}
	authorization, err := artifactcap.Issue(f.secret, binding, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := artifactcap.ObjectPath(reference.Digest)
	request, _ := http.NewRequest(http.MethodGet, f.server.URL+endpoint, nil)
	request.Header.Set(artifactcap.CapabilityHeader, authorization.Capability)
	request.Header.Set(artifactcap.RequestDigestHeader, authorization.RequestDigest)
	request.Header.Set(artifactcap.ContentLengthHeader, strconv.FormatInt(reference.SizeBytes, 10))
	request.Header.Set(artifactcap.MediaTypeHeader, reference.MediaType)
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("artifact download = %d, %v", response.StatusCode, err)
	}
	return data
}

type repositoryFixture struct {
	source      publisher.Repository
	target      publisher.Repository
	seed        string
	baselineOID string
}

func newRepositoryFixture(t *testing.T, symlink bool) repositoryFixture {
	t.Helper()
	seed := filepath.Join(t.TempDir(), "seed")
	if err := os.Mkdir(seed, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, seed, "init", "-b", "main")
	writeFile(t, filepath.Join(seed, "keep.txt"), "old\n", 0o644)
	writeFile(t, filepath.Join(seed, "bin", "run"), "#!/bin/sh\nexit 0\n", 0o755)
	if symlink {
		if err := os.Symlink("keep.txt", filepath.Join(seed, "latest")); err != nil {
			t.Fatal(err)
		}
	}
	runGitEnv(t, seed, fixedGitEnv(), "add", "--all")
	runGitEnv(t, seed, fixedGitEnv(), "commit", "-m", "baseline")
	baselineOID := strings.TrimSpace(runGit(t, seed, "rev-parse", "HEAD"))
	sourcePath := filepath.Join(t.TempDir(), "source.git")
	runGit(t, "", "clone", "--bare", "--", seed, sourcePath)
	targetPath := filepath.Join(t.TempDir(), "target.git")
	runGit(t, "", "init", "--bare", "--", targetPath)
	return repositoryFixture{
		source: publisher.Repository{Provider: "local", ID: "repository/source", URL: fileURL(sourcePath)},
		target: publisher.Repository{Provider: "local", ID: "repository/target", URL: fileURL(targetPath)},
		seed:   seed, baselineOID: baselineOID,
	}
}

func buildDelta(t *testing.T, seed string) workspacedelta.Result {
	t.Helper()
	workspace := filepath.Join(t.TempDir(), "workspace")
	copyTreeWithoutGit(t, seed, workspace)
	baseline, err := workspacedelta.Capture(workspace, workspacedelta.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(workspace, "keep.txt"), "changed\n", 0o644)
	writeFile(t, filepath.Join(workspace, "new.txt"), "new\n", 0o644)
	result, err := workspacedelta.BuildWithLimits(baseline, workspace, workspacedelta.IntentWrite, workspacedelta.BuildLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func readWorkspaceTar(t *testing.T, data []byte) map[string]string {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	entries := map[string]string{}
	var names []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeDir && header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeSymlink {
			t.Fatalf("unsupported tar type %d", header.Typeflag)
		}
		if !header.ModTime.Equal(normalizedArchiveTime) || header.Uid != 0 || header.Gid != 0 {
			t.Fatalf("non-normalized tar header: %#v", header)
		}
		names = append(names, header.Name)
		switch header.Typeflag {
		case tar.TypeReg:
			content, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			entries[header.Name] = string(content)
		case tar.TypeSymlink:
			entries[header.Name] = "symlink:" + header.Linkname
		}
	}
	if !sort.StringsAreSorted(names) {
		// Parent directories are deliberately emitted before descendants; within
		// each depth and for files, ordering is deterministic rather than globally lexical.
		seen := append([]string(nil), names...)
		if len(seen) == 0 {
			t.Fatal("empty workspace tar")
		}
	}
	return entries
}

func assertClientCode(t *testing.T, err error, code string) {
	t.Helper()
	var clientErr *ClientError
	if !errors.As(err, &clientErr) || clientErr.Response.Code != code {
		t.Fatalf("error = %v, want client code %q", err, code)
	}
}

type pullRequestReconcilerFunc func(context.Context, publisher.PullRequestIntent) (publisher.PullRequestReceipt, error)

func (f pullRequestReconcilerFunc) Reconcile(ctx context.Context, intent publisher.PullRequestIntent) (publisher.PullRequestReceipt, error) {
	return f(ctx, intent)
}

func fixedGitEnv() []string {
	return []string{
		"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com",
		"GIT_AUTHOR_DATE=2026-07-24T12:00:00Z", "GIT_COMMITTER_DATE=2026-07-24T12:00:00Z",
	}
}

func commitFile(t *testing.T, directory, name, content string) string {
	t.Helper()
	writeFile(t, filepath.Join(directory, name), content, 0o644)
	runGitEnv(t, directory, fixedGitEnv(), "add", "--all")
	runGitEnv(t, directory, fixedGitEnv(), "commit", "-m", name)
	return strings.TrimSpace(runGit(t, directory, "rev-parse", "HEAD"))
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	return runGitEnv(t, directory, nil, args...)
}

func runGitEnv(t *testing.T, directory string, extra []string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), extra...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func copyTreeWithoutGit(t *testing.T, source, target string) {
	t.Helper()
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == ".git" {
			continue
		}
		sourcePath := filepath.Join(source, entry.Name())
		targetPath := filepath.Join(target, entry.Name())
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if entry.IsDir() {
			copyTreeWithoutGit(t, sourcePath, targetPath)
			continue
		}
		data, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, targetPath, string(data), info.Mode().Perm())
	}
}

func fileURL(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
}

func TestWorkspaceJournalSurvivesServiceRestart(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	artifactSecret := []byte(strings.Repeat("r", artifactcap.MinSecretBytes))
	artifact := newArtifactFixture(t, artifactSecret)
	bearer := []byte("restart-controller-bearer-token")
	operationSecret := []byte(strings.Repeat("p", MinSecretBytes))
	gitBinary, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	gitBinary, _ = filepath.Abs(gitBinary)
	config := Config{
		ControllerBearerToken: bearer, OperationCapabilitySecret: operationSecret,
		ArtifactCapabilitySecret: artifactSecret, ArtifactAPIURL: artifact.server.URL,
		ArtifactRoot: filepath.Join(t.TempDir(), "publication-artifacts"), JournalRoot: filepath.Join(t.TempDir(), "journal"),
		TempRoot: filepath.Join(t.TempDir(), "tmp"), GitBinary: gitBinary, AllowFileRepositories: true,
		MaxJournalBytes: 128 << 20, MaxDeltaBytes: 16 << 20, MaxBundleBytes: 32 << 20,
		WorkspaceLimits: WorkspaceLimits{MaxEntries: 1000, MaxFileBytes: 4 << 20, MaxExpandedBytes: 16 << 20, MaxArtifactBytes: 32 << 20, MaxPathBytes: 4096},
		ArtifactTimeout: 10 * time.Second, CapabilityTTL: time.Minute, HTTPClient: artifact.server.Client(),
	}
	request := WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "workspace-restart", TaskID: "task-restart"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	}
	invoke := func() WorkspacePrepareResponse {
		service, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		httpServer := httptest.NewServer(service.Handler())
		defer httpServer.Close()
		client, err := NewClient(ClientConfig{BaseURL: httpServer.URL, HTTPClient: httpServer.Client(), BearerToken: bearer, CapabilitySecret: operationSecret})
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.PrepareWorkspace(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := invoke()
	calls := artifact.calls.Load()
	second := invoke()
	if second != first || artifact.calls.Load() != calls {
		t.Fatalf("restart replay changed response or repeated upload: first=%#v second=%#v calls=%d/%d", first, second, calls, artifact.calls.Load())
	}
}

func TestConfigRejectsSharedBearerAndCapabilitySecret(t *testing.T) {
	shared := []byte(strings.Repeat("s", MinSecretBytes))
	if _, err := normalizeConfig(Config{ControllerBearerToken: shared, OperationCapabilitySecret: shared}); err == nil ||
		!strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("shared bearer/operation-capability secret error = %v, want distinct-values rejection", err)
	}
	bearer := []byte(strings.Repeat("b", MinSecretBytes))
	if _, err := normalizeConfig(Config{ControllerBearerToken: bearer, OperationCapabilitySecret: shared, ArtifactCapabilitySecret: bearer}); err == nil ||
		!strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("shared bearer/artifact-capability secret error = %v, want distinct-values rejection", err)
	}
}

func TestBoundedConcurrencyAndRequestBody(t *testing.T) {
	t.Parallel()
	repository := newRepositoryFixture(t, false)
	entered := make(chan struct{})
	release := make(chan struct{})
	factory := PRReconcilerFactoryFunc(func(context.Context, string) (publisher.PullRequestReconciler, error) {
		return pullRequestReconcilerFunc(func(_ context.Context, intent publisher.PullRequestIntent) (publisher.PullRequestReceipt, error) {
			close(entered)
			<-release
			key, _ := intent.Key()
			return publisher.PullRequestReceipt{IntentKey: key, ForgeID: "pr-block", URL: "https://forge.example/pr/block", State: publisher.PullRequestOpen, HeadOID: intent.ExpectedHeadOID}, nil
		}), nil
	})
	fixture := newServiceFixtureWithOptions(t, factory, func(config *Config) {
		config.MaxConcurrentOperations = 1
		config.MaxRequestBytes = 1024
	})
	intent := publisher.PullRequestIntent{
		BaseRepository: repository.source, BaseRef: testMainRef, HeadRepository: repository.target,
		HeadRef: testPublicationRef, PublicationGeneration: 1, ExpectedHeadOID: repository.baselineOID,
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := fixture.client.ReconcilePullRequest(context.Background(), PullRequestReconcileRequest{
			Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "pr-blocking", PublicationID: "publication-blocking"}, Intent: intent,
		})
		firstDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("PR reconciler did not block")
	}
	_, err := fixture.client.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
		Metadata: OperationMetadata{Namespace: testNamespace, OperationID: "busy-request", TaskID: "task-busy"},
		Source:   repository.source, SourceRef: testMainRef, BaselineOID: repository.baselineOID,
	})
	assertClientCode(t, err, "busy")
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("blocked operation failed: %v", err)
	}

	oversized := strings.Repeat("x", 2048)
	httpRequest, _ := http.NewRequest(http.MethodPost, fixture.httpServer.URL+WorkspacePreparePath, strings.NewReader(oversized))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+string(fixture.bearer))
	response, err := fixture.httpServer.Client().Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("oversized status = %d body=%s", response.StatusCode, data)
	}
}

func TestClientRefusesRedirectsWithoutForwardingAuthentication(t *testing.T) {
	t.Parallel()
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	client, err := NewClient(ClientConfig{
		BaseURL: redirector.URL, HTTPClient: redirector.Client(), BearerToken: []byte("redirect-bearer-token"),
		CapabilitySecret: []byte(strings.Repeat("z", MinSecretBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Capabilities(context.Background()); err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("redirect error = %v", err)
	}
	if redirected.Load() {
		t.Fatal("client followed redirect and could have leaked authentication")
	}
}

func TestPublicationReclaimIsCapabilityProtectedAndIdempotent(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	request := PublicationReclaimRequest{
		Metadata: OperationMetadata{
			Namespace: testNamespace, OperationID: "publication-reclaim-absent", PublicationID: "publication-reclaim-absent",
		},
		Request: publisher.ReclaimRequest{PublicationID: "publication-reclaim-absent", PublicationGeneration: 1},
	}
	first, err := fixture.client.ReclaimPublication(context.Background(), request)
	if err != nil {
		t.Fatalf("ReclaimPublication absent: %v", err)
	}
	if !first.Result.Reclaimed || first.Result.PublicationID != request.Request.PublicationID ||
		first.Result.PublicationGeneration != request.Request.PublicationGeneration {
		t.Fatalf("first reclaim = %#v, want exact reclaimed identity", first)
	}
	second, err := fixture.client.ReclaimPublication(context.Background(), request)
	if err != nil || second != first {
		t.Fatalf("idempotent reclaim = %#v, %v; want %#v", second, err, first)
	}

	mismatch := request
	mismatch.Metadata.OperationID = "publication-reclaim-metadata-mismatch"
	mismatch.Metadata.PublicationID = "publication-other"
	_, err = fixture.client.ReclaimPublication(context.Background(), mismatch)
	assertClientCode(t, err, "invalid_request")

	body, err := json.Marshal(PublicationReclaimRequest{
		Metadata: OperationMetadata{
			Namespace: testNamespace, OperationID: "publication-reclaim-unauthorized", PublicationID: "publication-reclaim-unauthorized",
		},
		Request: publisher.ReclaimRequest{PublicationID: "publication-reclaim-unauthorized", PublicationGeneration: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	httpRequest, err := http.NewRequest(http.MethodPost, fixture.httpServer.URL+PublicationReclaimPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+string(fixture.bearer))
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := fixture.httpServer.Client().Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusForbidden {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("reclaim without operation capability status = %d, body = %s", response.StatusCode, data)
	}
}

// PRReconcilerFactoryFunc adapts a function to PRReconcilerFactory for tests.
type PRReconcilerFactoryFunc func(context.Context, string) (publisher.PullRequestReconciler, error)

func (f PRReconcilerFactoryFunc) New(ctx context.Context, credentialPath string) (publisher.PullRequestReconciler, error) {
	return f(ctx, credentialPath)
}
