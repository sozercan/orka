package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/artifactcap"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	canonicalWorkspaceRepositoryTestURL = "https://github.com/orka-agents/orka.git"
	canonicalWorkspaceRepositoryTestID  = "github.com/orka-agents/orka"
)

func TestPrepareRuntimeWorkspaceRetriesTransientPublisherFailuresUnderOneLease(t *testing.T) {
	const (
		namespace = "tenant-a"
		taskUID   = "task-workspace-retry"
		promptID  = "prompt-workspace-retry"
	)
	baselineOID := strings.Repeat("a", 40)
	manifestDigest := "sha256:" + strings.Repeat("b", 64)
	artifactDigest := "sha256:" + strings.Repeat("c", 64)
	artifactID, err := artifactcap.ArtifactIDForDigest(artifactDigest)
	if err != nil {
		t.Fatal(err)
	}

	resolveRequests := make([]publisherservice.WorkspaceResolveRequest, 0, 2)
	prepareRequests := make([]publisherservice.WorkspacePrepareRequest, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case publisherservice.WorkspaceResolvePath:
			var request publisherservice.WorkspaceResolveRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode resolve request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			resolveRequests = append(resolveRequests, request)
			if len(resolveRequests) == 1 {
				writeDispatcherJSONStatus(w, http.StatusBadGateway, publisherservice.ErrorResponse{
					Code: "scm_failure", Message: "transient Git failure", Retryable: true,
				})
				return
			}
			writeDispatcherJSON(w, publisherservice.WorkspaceResolveResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("resolve-retry"),
				RepositoryID: request.Source.ID, SourceRef: "refs/heads/main", BaselineOID: baselineOID,
			})
		case publisherservice.WorkspacePreparePath:
			var request publisherservice.WorkspacePrepareRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode prepare request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			prepareRequests = append(prepareRequests, request)
			if len(prepareRequests) == 1 {
				writeDispatcherJSONStatus(w, http.StatusBadGateway, publisherservice.ErrorResponse{
					Code: "scm_failure", Message: "transient Git failure", Retryable: true,
				})
				return
			}
			writeDispatcherJSON(w, publisherservice.WorkspacePrepareResponse{
				OperationID: request.Metadata.OperationID, RequestDigest: testControlDigestForDispatcher("prepare-retry"),
				RepositoryID: request.Source.ID, SourceRef: request.SourceRef, BaselineOID: request.BaselineOID,
				TreeOID: strings.Repeat("d", 40), ManifestDigest: manifestDigest, EntryCount: 1, ExpandedBytes: 64,
				Artifact: harnessv2.ArtifactReference{
					ArtifactID: harnessv2.ArtifactID(artifactID), Digest: artifactDigest,
					SizeBytes: 64, MediaType: artifactcap.MediaTypeWorkspaceTar,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	publisherClient, err := publisherservice.NewClient(publisherservice.ClientConfig{
		BaseURL: server.URL, HTTPClient: server.Client(),
		BearerToken: []byte(strings.Repeat("e", 32)), CapabilitySecret: []byte(strings.Repeat("f", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	controlStore, fence, closeStore := newACPSessionTestStore(t, filepath.Join(t.TempDir(), "workspace-retry.db"))
	defer closeStore()
	dispatcher := &ACPDispatcher{
		Store: controlStore, Publisher: publisherClient,
		ArtifactCapabilitySecret: []byte(strings.Repeat("1", artifactcap.MinSecretBytes)),
		ArtifactReservations:     acceptingArtifactReservations{},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "workspace-retry", UID: types.UID(taskUID)},
		Spec: corev1alpha1.TaskSpec{Workspace: &corev1alpha1.WorkspaceConfig{
			Intent: corev1alpha1.WorkspaceIntentRead, GitRepo: canonicalWorkspaceRepositoryTestURL,
			SourceRepository: &corev1alpha1.RepositoryIdentity{
				Provider: workspaceRepositoryProviderGitHub, ID: canonicalWorkspaceRepositoryTestID,
			},
			Branch: "main",
		}},
		Status: corev1alpha1.TaskStatus{Execution: &corev1alpha1.TaskExecutionStatus{PromptID: promptID}},
	}

	plannedAt := time.Date(2020, time.September, 2, 7, 0, 0, 0, time.UTC)
	prepared, err := dispatcher.prepareRuntimeWorkspace(context.Background(), task, fence, &acpTaskSession{}, plannedAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.baseline.Revision != baselineOID || prepared.baseline.TreeDigest != manifestDigest || prepared.spec.Baseline.Artifact == nil {
		t.Fatalf("prepared workspace = %#v", prepared)
	}
	for label, check := range map[string]struct {
		requestsEqual bool
		attempts      int
		operationIDs  []string
		kind          string
		operationID   string
	}{
		"resolve": {
			len(resolveRequests) == 2 && reflect.DeepEqual(resolveRequests[0], resolveRequests[1]),
			len(resolveRequests), workspaceResolveOperationIDs(resolveRequests),
			"workspace.resolve", "workspace-resolve-" + promptID,
		},
		"prepare": {
			len(prepareRequests) == 2 && reflect.DeepEqual(prepareRequests[0], prepareRequests[1]),
			len(prepareRequests), workspacePrepareOperationIDs(prepareRequests),
			"workspace.prepare", "workspace-prepare-" + promptID,
		},
	} {
		if check.attempts != 2 || !check.requestsEqual {
			t.Fatalf("%s requests changed across %d attempts", label, check.attempts)
		}
		if check.operationIDs[0] != check.operationID || check.operationIDs[1] != check.operationID {
			t.Fatalf("%s operation IDs = %v, want two stable %q attempts", label, check.operationIDs, check.operationID)
		}
		identity := store.ExternalEffectIdentity{
			Kind: check.kind, Namespace: namespace, AggregateID: taskUID, OperationID: check.operationID,
		}
		effectID, err := identity.CanonicalID()
		if err != nil {
			t.Fatal(err)
		}
		effect, err := controlStore.GetExternalEffect(context.Background(), effectID)
		if err != nil {
			t.Fatal(err)
		}
		if effect.State != store.ExternalEffectSucceeded || effect.Attempts != 1 {
			t.Fatalf("%s external effect = state %s attempts %d, want Succeeded with one lease", label, effect.State, effect.Attempts)
		}
		if check.kind == "workspace.prepare" && !prepared.createIssuedAt.Equal(effect.UpdatedAt) {
			t.Fatalf("RuntimeSession create issued at = %s, want durable prepare completion %s", prepared.createIssuedAt, effect.UpdatedAt)
		}
	}
	replayed, err := dispatcher.prepareRuntimeWorkspace(context.Background(), task, fence, &acpTaskSession{}, plannedAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.authorization == nil || !reflect.DeepEqual(prepared.authorization, replayed.authorization) {
		t.Fatalf("workspace authorization changed across one RuntimeSession creation replay")
	}
	if !prepared.createIssuedAt.Equal(replayed.createIssuedAt) {
		t.Fatalf("RuntimeSession create timestamp changed across replay: %s != %s", prepared.createIssuedAt, replayed.createIssuedAt)
	}
}

func workspaceResolveOperationIDs(requests []publisherservice.WorkspaceResolveRequest) []string {
	operationIDs := make([]string, 0, len(requests))
	for _, request := range requests {
		operationIDs = append(operationIDs, request.Metadata.OperationID)
	}
	return operationIDs
}

func workspacePrepareOperationIDs(requests []publisherservice.WorkspacePrepareRequest) []string {
	operationIDs := make([]string, 0, len(requests))
	for _, request := range requests {
		operationIDs = append(operationIDs, request.Metadata.OperationID)
	}
	return operationIDs
}

func TestRuntimeWorkspaceForSessionContinuationUsesPublicationTargetReadCredential(t *testing.T) {
	sourceRead := &corev1alpha1.WorkspaceCredentialReference{Name: "source-read"}
	targetRead := &corev1alpha1.WorkspaceCredentialReference{Name: "target-read"}
	workspace := &corev1alpha1.WorkspaceConfig{
		GitRepo:                      "https://github.com/orka-agents/source.git",
		SourceRepository:             &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/source"},
		Branch:                       "main",
		ReadCredentialRef:            sourceRead,
		PublicationGitRepo:           "https://github.com/orka-agents/target.git",
		PublicationRepository:        &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/target"},
		PublicationReadCredentialRef: targetRead,
	}
	baseline := &store.VerifiedBranchBaseline{
		RepositoryID: "github.com/orka-agents/target",
		Ref:          "refs/heads/orka/session-session-uid",
		SHA:          strings.Repeat("a", 40),
	}

	got, err := runtimeWorkspaceForSessionContinuation(workspace, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if got == workspace {
		t.Fatal("session continuation mutated the original workspace instead of using a copy")
	}
	if got.GitRepo != workspace.PublicationGitRepo || got.SourceRepository == nil || got.SourceRepository.ID != workspace.PublicationRepository.ID {
		t.Fatalf("continuation repository = %#v, want publication target", got)
	}
	if got.ReadCredentialRef == nil || got.ReadCredentialRef.Name != targetRead.Name {
		t.Fatalf("continuation read credential = %#v, want target read credential", got.ReadCredentialRef)
	}
	if got.Ref != baseline.Ref || got.Branch != "" {
		t.Fatalf("continuation source = ref %q branch %q, want verified ref %q", got.Ref, got.Branch, baseline.Ref)
	}
	if workspace.ReadCredentialRef == nil || workspace.ReadCredentialRef.Name != sourceRead.Name || workspace.GitRepo == workspace.PublicationGitRepo {
		t.Fatalf("source workspace was mutated: %#v", workspace)
	}

	conflicting := *workspace
	conflicting.ExpectedRemoteSHA = strings.Repeat("b", 40)
	if _, err := runtimeWorkspaceForSessionContinuation(&conflicting, baseline); err == nil {
		t.Fatal("conflicting expectedRemoteSHA unexpectedly accepted before prompt execution")
	}
}

func TestRuntimeWorkspaceForSessionContinuationClearsSourceIdentityForCrossRepositoryPublication(t *testing.T) {
	sourceIdentity := &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/source"}
	workspace := &corev1alpha1.WorkspaceConfig{
		GitRepo:            "https://github.com/orka-agents/source.git",
		SourceRepository:   sourceIdentity,
		PublicationGitRepo: "https://github.com/orka-agents/target.git",
	}
	baseline := &store.VerifiedBranchBaseline{
		RepositoryID: "github.com/orka-agents/target",
		Ref:          "refs/heads/orka/session-session-uid",
		SHA:          strings.Repeat("a", 40),
	}

	got, err := runtimeWorkspaceForSessionContinuation(workspace, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if got.GitRepo != workspace.PublicationGitRepo {
		t.Fatalf("continuation gitRepo = %q, want %q", got.GitRepo, workspace.PublicationGitRepo)
	}
	if got.SourceRepository != nil {
		t.Fatalf("continuation sourceRepository = %#v, want derived publication identity", got.SourceRepository)
	}
	if workspace.SourceRepository != sourceIdentity {
		t.Fatalf("source workspace identity was mutated: %#v", workspace.SourceRepository)
	}
}

func TestRuntimeWorkspaceForSessionContinuationPreservesSourceIdentityForSameRepositoryPublication(t *testing.T) {
	sourceIdentity := &corev1alpha1.RepositoryIdentity{Provider: "github", ID: "github.com/orka-agents/source"}
	workspace := &corev1alpha1.WorkspaceConfig{
		GitRepo:            "https://GitHub.COM/orka-agents/source.git",
		SourceRepository:   sourceIdentity,
		PublicationGitRepo: "https://github.com/orka-agents/source",
	}
	baseline := &store.VerifiedBranchBaseline{
		RepositoryID: sourceIdentity.ID,
		Ref:          "refs/heads/orka/session-session-uid",
		SHA:          strings.Repeat("a", 40),
	}

	got, err := runtimeWorkspaceForSessionContinuation(workspace, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if got.GitRepo != workspace.PublicationGitRepo {
		t.Fatalf("continuation gitRepo = %q, want %q", got.GitRepo, workspace.PublicationGitRepo)
	}
	if got.SourceRepository == nil || *got.SourceRepository != *sourceIdentity {
		t.Fatalf("continuation sourceRepository = %#v, want %#v", got.SourceRepository, sourceIdentity)
	}
}

func TestRuntimeWorkspaceSourceRefDoesNotAssumeMain(t *testing.T) {
	tests := []struct {
		name      string
		workspace *corev1alpha1.WorkspaceConfig
		want      string
		wantErr   string
	}{
		{name: "explicit branch", workspace: &corev1alpha1.WorkspaceConfig{Branch: "trunk"}, want: "refs/heads/trunk"},
		{name: "full branch ref", workspace: &corev1alpha1.WorkspaceConfig{Branch: "refs/heads/release/v2"}, want: "refs/heads/release/v2"},
		{name: "exact ref", workspace: &corev1alpha1.WorkspaceConfig{Ref: "v2.0.0"}, want: "v2.0.0"},
		{name: "default resolved by publisher", workspace: &corev1alpha1.WorkspaceConfig{}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := runtimeWorkspaceSourceRef(test.workspace)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("runtimeWorkspaceSourceRef() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("runtimeWorkspaceSourceRef() = %q, %v, want %q", got, err, test.want)
			}
		})
	}
}

func TestCanonicalWorkspaceRepositoryURLAllowsOnlyDefaultHTTPSPort(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantURL string
		wantID  string
		wantErr string
	}{
		{
			name:    "implicit default port retains canonical host behavior",
			rawURL:  "https://GitHub.COM/orka-agents/orka.git",
			wantURL: canonicalWorkspaceRepositoryTestURL,
			wantID:  canonicalWorkspaceRepositoryTestID,
		},
		{
			name:    "explicit default port is accepted",
			rawURL:  "https://GitHub.COM:443/orka-agents/orka.git",
			wantURL: "https://github.com:443/orka-agents/orka.git",
			wantID:  canonicalWorkspaceRepositoryTestID,
		},
		{
			name:    "explicit non-default port is rejected",
			rawURL:  "https://github.com:8443/orka-agents/orka.git",
			wantErr: errWorkspaceRepositoryHTTPSPort.Error(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, id, err := canonicalWorkspaceRepositoryURL(test.rawURL)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("canonicalWorkspaceRepositoryURL() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parsed.String() != test.wantURL || id != test.wantID {
				t.Fatalf("canonicalWorkspaceRepositoryURL() = (%q, %q), want (%q, %q)", parsed.String(), id, test.wantURL, test.wantID)
			}
		})
	}
}
