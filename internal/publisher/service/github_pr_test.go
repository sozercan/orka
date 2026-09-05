package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/publisher"
)

const (
	githubTestAuthorization = "test-operation-token-not-a-secret"
	githubTestHost          = "github.example"
	githubTestSHA           = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	githubTestBaseBranch    = "main"
)

type githubPRTestScenario struct {
	mu sync.Mutex

	base githubRepository
	head githubRepository

	listStatus   int
	list         []githubPullRequest
	listLink     string
	headSHA      string
	createStatus int
	create       githubPullRequest

	requests []string
	created  []githubCreatePullRequest
}

func newGitHubPRTestScenario(t *testing.T) *githubPRTestScenario {
	t.Helper()
	return &githubPRTestScenario{
		base:         githubTestRepository(101, "acme/base"),
		head:         githubTestRepository(202, "bot/fork"),
		listStatus:   http.StatusOK,
		headSHA:      githubTestSHA,
		createStatus: http.StatusCreated,
	}
}

func (s *githubPRTestScenario) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer "+githubTestAuthorization {
			t.Errorf("Authorization = %q", got)
		}
		if got := request.Header.Get("X-GitHub-Api-Version"); got != githubAPIVersion {
			t.Errorf("X-GitHub-Api-Version = %q", got)
		}
		if got := request.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Errorf("Accept = %q", got)
		}

		s.mu.Lock()
		s.requests = append(s.requests, request.Method+" "+request.URL.RequestURI())
		s.mu.Unlock()

		switch request.Method + " " + request.URL.Path {
		case http.MethodGet + " /repos/acme/base":
			writeGitHubTestJSON(t, writer, http.StatusOK, s.base)
		case http.MethodGet + " /repos/bot/fork":
			writeGitHubTestJSON(t, writer, http.StatusOK, s.head)
		case http.MethodGet + " /repos/acme/base/pulls":
			if request.URL.Query().Get("state") != "all" || request.URL.Query().Get("head") != "bot:orka-change" ||
				request.URL.Query().Get("base") != githubTestBaseBranch || request.URL.Query().Get("per_page") != "100" {
				t.Errorf("pull request query = %q", request.URL.RawQuery)
			}
			s.mu.Lock()
			status, link := s.listStatus, s.listLink
			value := append([]githubPullRequest(nil), s.list...)
			s.mu.Unlock()
			if link != "" {
				writer.Header().Set("Link", link)
			}
			writeGitHubTestJSON(t, writer, status, value)
		case http.MethodGet + " /repos/bot/fork/git/ref/heads/orka-change":
			s.mu.Lock()
			sha := s.headSHA
			s.mu.Unlock()
			value := githubGitRef{Ref: "refs/heads/orka-change"}
			value.Object.Type = "commit"
			value.Object.SHA = sha
			writeGitHubTestJSON(t, writer, http.StatusOK, value)
		case http.MethodPost + " /repos/acme/base/pulls":
			var value githubCreatePullRequest
			if err := json.NewDecoder(request.Body).Decode(&value); err != nil {
				t.Errorf("decode create request: %v", err)
			}
			s.mu.Lock()
			s.created = append(s.created, value)
			status, response := s.createStatus, s.create
			s.mu.Unlock()
			writeGitHubTestJSON(t, writer, status, response)
		default:
			t.Errorf("unexpected GitHub request %s %s", request.Method, request.URL.RequestURI())
			writeGitHubTestJSON(t, writer, http.StatusNotFound, map[string]string{"message": "not found"})
		}
	})
}

func (s *githubPRTestScenario) requestSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func (s *githubPRTestScenario) createSnapshot() []githubCreatePullRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]githubCreatePullRequest(nil), s.created...)
}

func TestGitHubPRReconcilerCreatesExactPullRequest(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	key, err := intent.Key()
	if err != nil {
		t.Fatal(err)
	}
	scenario.create = githubTestPullRequest(scenario.base, scenario.head, 17, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(key))
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatalf("ReconcilePullRequest: %v", err)
	}
	if receipt.IntentKey != key || receipt.ForgeID != "github:101:17" || receipt.URL != "https://github.example/acme/base/pull/17" ||
		receipt.State != publisher.PullRequestOpen || receipt.HeadOID != githubTestSHA {
		t.Fatalf("receipt = %#v", receipt)
	}
	created := scenario.createSnapshot()
	if len(created) != 1 {
		t.Fatalf("create requests = %#v", created)
	}
	request := created[0]
	if request.Title != "Orka publication generation 7" || request.Head != "bot:orka-change" || request.HeadRepository != "fork" ||
		request.Base != "main" || request.Draft || request.MaintainerCanModify || request.Body != githubPullRequestBody(7, key, "") {
		t.Fatalf("create request = %#v", request)
	}
	if strings.Contains(request.Body, githubTestAuthorization) {
		t.Fatal("create body contains the operation credential")
	}
	wantRequests := []string{
		"GET /repos/acme/base", "GET /repos/bot/fork",
		"GET /repos/acme/base/pulls?base=main&head=bot%3Aorka-change&per_page=100&state=all",
		"GET /repos/bot/fork/git/ref/heads/orka-change", "POST /repos/acme/base/pulls",
		"GET /repos/bot/fork/git/ref/heads/orka-change",
	}
	if got := scenario.requestSnapshot(); !slices.Equal(got, wantRequests) {
		t.Fatalf("requests = %#v, want %#v", got, wantRequests)
	}
}

func TestGitHubPRReconcilerNormalizesDefaultPortAndRepositoryCase(t *testing.T) {
	t.Parallel()
	intent := githubTestIntent()
	intent.BaseRepository.URL = "https://github.example:443/ACME/BASE.git"
	intent.HeadRepository.URL = "https://github.example:443/BOT/FORK.git"
	intentKey, err := intent.Key()
	if err != nil {
		t.Fatal(err)
	}
	base := githubTestRepository(101, "acme/base")
	head := githubTestRepository(202, "bot/fork")
	created := githubTestPullRequest(base, head, 17, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(intentKey))
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method + " " + strings.ToLower(request.URL.Path) {
		case http.MethodGet + " /repos/acme/base":
			writeGitHubTestJSON(t, writer, http.StatusOK, base)
		case http.MethodGet + " /repos/bot/fork":
			writeGitHubTestJSON(t, writer, http.StatusOK, head)
		case http.MethodGet + " /repos/acme/base/pulls":
			writeGitHubTestJSON(t, writer, http.StatusOK, []githubPullRequest(nil))
		case http.MethodGet + " /repos/bot/fork/git/ref/heads/orka-change":
			writeGitHubTestJSON(t, writer, http.StatusOK, githubGitRef{
				Ref: "refs/heads/orka-change",
				Object: struct {
					Type string `json:"type"`
					SHA  string `json:"sha"`
				}{Type: "commit", SHA: githubTestSHA},
			})
		case http.MethodPost + " /repos/acme/base/pulls":
			writeGitHubTestJSON(t, writer, http.StatusCreated, created)
		default:
			t.Errorf("unexpected GitHub request %s %s", request.Method, request.URL.RequestURI())
			writeGitHubTestJSON(t, writer, http.StatusNotFound, map[string]string{"message": "not found"})
		}
	})
	reconciler := newGitHubTestReconciler(t, handler)

	receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatalf("ReconcilePullRequest: %v", err)
	}
	if strings.Contains(receipt.URL, ":443") || !strings.EqualFold(receipt.URL, "https://github.example/acme/base/pull/17") {
		t.Fatalf("receipt URL = %q, want normalized canonical repository URL", receipt.URL)
	}
}

func TestGitHubPRReconcilerAcceptsCanonicalURLRepositoryIdentity(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	intent.BaseRepository.ID = githubTestHost + "/" + scenario.base.FullName
	intent.HeadRepository.ID = githubTestHost + "/" + scenario.head.FullName
	key, err := intent.Key()
	if err != nil {
		t.Fatal(err)
	}
	scenario.list = []githubPullRequest{
		githubTestPullRequest(scenario.base, scenario.head, 23, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(key)),
	}
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatalf("ReconcilePullRequest: %v", err)
	}
	if receipt.ForgeID != "github:101:23" || receipt.State != publisher.PullRequestOpen {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestGitHubPRReconcilerReturnsExactExistingOpenPullRequest(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	key, _ := intent.Key()
	scenario.list = []githubPullRequest{
		githubTestPullRequest(scenario.base, scenario.head, 42, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(key)),
	}
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatalf("ReconcilePullRequest: %v", err)
	}
	if receipt.ForgeID != "github:101:42" || receipt.State != publisher.PullRequestOpen {
		t.Fatalf("receipt = %#v", receipt)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
	for _, request := range scenario.requestSnapshot() {
		if strings.Contains(request, "/git/ref/") {
			t.Fatalf("existing pull request unexpectedly re-read the mutable head ref: %s", request)
		}
	}
}

func TestGitHubPRReconcilerRejectsAmbiguousExactPullRequests(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	key, _ := intent.Key()
	scenario.list = []githubPullRequest{
		githubTestPullRequest(scenario.base, scenario.head, 1, publisher.PullRequestClosed, githubTestSHA, githubIntentMarker(key)),
		githubTestPullRequest(scenario.base, scenario.head, 2, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(key)),
	}
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	_, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want idempotency conflict", err)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
}

func TestGitHubPRReconcilerRejectsWrongHeadSHA(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	key, _ := intent.Key()
	scenario.list = []githubPullRequest{
		githubTestPullRequest(scenario.base, scenario.head, 9, publisher.PullRequestOpen, strings.Repeat("b", 40), githubIntentMarker(key)),
	}
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	_, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) || !strings.Contains(err.Error(), "head SHA") {
		t.Fatalf("error = %v, want head SHA conflict", err)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
}

func TestGitHubPRReconcilerRejectsMovedHeadBeforeCreate(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	scenario.headSHA = strings.Repeat("b", 40)
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	_, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) || !strings.Contains(err.Error(), "head ref") {
		t.Fatalf("error = %v, want moved head conflict", err)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
}

func TestGitHubPRReconcilerRejectsRepositoryIdentityMismatch(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	scenario.head.ID++
	scenario.head.FullName = "someone-else/fork"
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	_, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) || !strings.Contains(err.Error(), "repository identity") {
		t.Fatalf("error = %v, want repository identity conflict", err)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
}

func TestGitHubPRReconcilerReturnsKnownClosedAndMergedPullRequests(t *testing.T) {
	t.Parallel()
	for _, state := range []publisher.PullRequestState{publisher.PullRequestClosed, publisher.PullRequestMerged} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			scenario := newGitHubPRTestScenario(t)
			intent := githubTestIntent()
			key, _ := intent.Key()
			scenario.list = []githubPullRequest{
				githubTestPullRequest(scenario.base, scenario.head, 33, state, githubTestSHA, githubIntentMarker(key)),
			}
			reconciler := newGitHubTestReconciler(t, scenario.handler(t))

			receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
			if err != nil {
				t.Fatalf("ReconcilePullRequest: %v", err)
			}
			if receipt.State != state {
				t.Fatalf("receipt = %#v, want state %s", receipt, state)
			}
			if created := scenario.createSnapshot(); len(created) != 0 {
				t.Fatalf("known %s pull request was recreated: %#v", state, created)
			}
		})
	}
}

func TestGitHubPRReconcilerNeverAdoptsUnmarkedSameBranchPullRequest(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	scenario.list = []githubPullRequest{
		githubTestPullRequest(scenario.base, scenario.head, 55, publisher.PullRequestOpen, githubTestSHA, "manual pull request"),
	}
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))

	_, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) || !strings.Contains(err.Error(), "unrelated open") {
		t.Fatalf("error = %v, want unrelated pull request conflict", err)
	}
	if created := scenario.createSnapshot(); len(created) != 0 {
		t.Fatalf("unexpected create requests = %#v", created)
	}
}

func TestGitHubPRReconcilerRecoversAmbiguousCreateByExactTuple(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	key, _ := intent.Key()
	exact := githubTestPullRequest(scenario.base, scenario.head, 77, publisher.PullRequestOpen, githubTestSHA, githubIntentMarker(key))
	var listCalls atomic.Int32
	baseHandler := scenario.handler(t)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/repos/acme/base/pulls" {
			if listCalls.Add(1) > 1 {
				scenario.mu.Lock()
				scenario.list = []githubPullRequest{exact}
				scenario.mu.Unlock()
			}
		}
		if request.Method == http.MethodPost && request.URL.Path == "/repos/acme/base/pulls" {
			scenario.mu.Lock()
			scenario.createStatus = http.StatusInternalServerError
			scenario.mu.Unlock()
		}
		baseHandler.ServeHTTP(writer, request)
	})
	reconciler := newGitHubTestReconciler(t, handler)

	receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatalf("ReconcilePullRequest: %v", err)
	}
	if receipt.ForgeID != "github:101:77" || listCalls.Load() != 2 {
		t.Fatalf("receipt = %#v listCalls=%d", receipt, listCalls.Load())
	}
}

func TestGitHubPRReconcilerSanitizesAPIErrors(t *testing.T) {
	t.Parallel()
	upstreamMarker := "upstream-body-secret-marker"
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(writer, `{"message":%q,"token":%q}`, upstreamMarker, githubTestAuthorization)
	}))
	defer server.Close()
	reconciler := newGitHubTestReconcilerForServer(t, server, defaultGitHubMaxResponseBytes, 0)

	_, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler)
	if err == nil {
		t.Fatal("expected GitHub API error")
	}
	var operationErr *operationError
	if !errors.As(err, &operationErr) || operationErr.code != "github_api_error" || !operationErr.retryable {
		t.Fatalf("error = %#v", err)
	}
	if strings.Contains(err.Error(), upstreamMarker) || strings.Contains(err.Error(), githubTestAuthorization) {
		t.Fatalf("error leaked upstream response or token: %v", err)
	}
}

func TestGitHubPRReconcilerDeniesRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Bool
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
	defer target.Close()
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+request.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	reconciler := newGitHubTestReconcilerForServer(t, redirector, defaultGitHubMaxResponseBytes, 0)

	_, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler)
	if err == nil {
		t.Fatal("expected redirect denial")
	}
	if redirected.Load() {
		t.Fatal("GitHub client followed a redirect with the operation credential")
	}
	if strings.Contains(err.Error(), githubTestAuthorization) {
		t.Fatalf("redirect error leaked token: %v", err)
	}
}

func TestGitHubPRReconcilerBoundsResponsesAndTimeouts(t *testing.T) {
	t.Parallel()
	t.Run("response limit", func(t *testing.T) {
		t.Parallel()
		server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"padding":"` + strings.Repeat("x", 1024) + `"}`))
		}))
		defer server.Close()
		reconciler := newGitHubTestReconcilerForServer(t, server, 128, 0)
		if _, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler); err == nil {
			t.Fatal("expected response limit error")
		}
	})
	t.Run("request timeout", func(t *testing.T) {
		t.Parallel()
		started := make(chan struct{})
		server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(started)
			<-request.Context().Done()
		}))
		defer server.Close()
		reconciler := newGitHubTestReconcilerForServer(t, server, defaultGitHubMaxResponseBytes, 50*time.Millisecond)
		begin := time.Now()
		if _, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler); err == nil {
			t.Fatal("expected timeout error")
		}
		if time.Since(begin) > time.Second {
			t.Fatal("GitHub request exceeded the strict timeout")
		}
		select {
		case <-started:
		default:
			t.Fatal("GitHub request did not reach the test server")
		}
	})
}

func TestGitHubPRReconcilerRejectsCandidatePagination(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	scenario.listLink = `<https://api.github.example/repos/acme/base/pulls?page=2>; rel="next"`
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))
	_, err := publisher.ReconcilePullRequest(context.Background(), githubTestIntent(), reconciler)
	if !errors.Is(err, publisher.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want bounded candidate conflict", err)
	}
}

func newGitHubTestReconciler(t *testing.T, handler http.Handler) publisher.PullRequestReconciler {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return newGitHubTestReconcilerForServer(t, server, defaultGitHubMaxResponseBytes, 0)
}

func newGitHubTestReconcilerForServer(
	t *testing.T,
	server *httptest.Server,
	maxResponseBytes int64,
	timeout time.Duration,
) publisher.PullRequestReconciler {
	t.Helper()
	operationFile := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(operationFile, []byte(githubTestAuthorization), 0o600); err != nil {
		t.Fatal(err)
	}
	factory, err := NewGitHubPRReconcilerFactory(GitHubPRReconcilerFactoryConfig{
		APIBaseURL: server.URL, HTTPClient: server.Client(), RequestTimeout: timeout, MaxResponseBytes: maxResponseBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := factory.New(context.Background(), operationFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(operationFile); err != nil {
		t.Fatal(err)
	}
	return reconciler
}

func githubTestIntent() publisher.PullRequestIntent {
	return publisher.PullRequestIntent{
		BaseRepository:        publisher.Repository{Provider: githubProvider, ID: "repository-node-101", URL: "https://github.example/acme/base.git"},
		BaseRef:               "refs/heads/main",
		HeadRepository:        publisher.Repository{Provider: githubProvider, ID: "repository-node-202", URL: "https://github.example/bot/fork.git"},
		HeadRef:               "refs/heads/orka-change",
		PublicationGeneration: 7,
		ExpectedHeadOID:       githubTestSHA,
	}
}

func githubTestRepository(id int64, fullName string) githubRepository {
	return githubRepository{
		ID: id, NodeID: fmt.Sprintf("repository-node-%d", id), FullName: fullName,
		HTMLURL:  "https://" + githubTestHost + "/" + fullName,
		CloneURL: "https://" + githubTestHost + "/" + fullName + ".git",
	}
}

func githubTestPullRequest(
	base, head githubRepository,
	number int64,
	state publisher.PullRequestState,
	headSHA, body string,
) githubPullRequest {
	candidate := githubPullRequest{
		Number: number, HTMLURL: base.HTMLURL + "/pull/" + fmt.Sprint(number), Body: &body,
		Head: githubPullRequestRef{Ref: "orka-change", SHA: headSHA, Repo: &head},
		Base: githubPullRequestRef{Ref: githubTestBaseBranch, Repo: &base},
	}
	switch state {
	case publisher.PullRequestOpen:
		candidate.State = githubPullRequestStateOpen
	case publisher.PullRequestClosed:
		candidate.State = githubPullRequestStateClosed
	case publisher.PullRequestMerged:
		candidate.State = githubPullRequestStateClosed
		mergedAt := "2026-07-25T12:00:00Z"
		candidate.MergedAt = &mergedAt
	default:
		panic("unsupported test pull request state")
	}
	return candidate
}

func writeGitHubTestJSON(t *testing.T, writer http.ResponseWriter, status int, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode GitHub response: %v", err)
	}
}

func TestLoadConfigFromEnvExplicitlyEnablesGitHubPRReconciliation(t *testing.T) {
	clearPublisherEnvForTest(t)
	controller := writePublisherSecretForTest(t, "controller", strings.Repeat("c", 32))
	operation := writePublisherSecretForTest(t, "operation", strings.Repeat("o", MinSecretBytes))
	t.Setenv(EnvControllerTokenFile, controller)
	t.Setenv(EnvOperationCapabilitySecretFile, operation)
	t.Setenv(EnvArtifactAuthorizationBrokerURL, "https://orka-api.example")
	t.Setenv(EnvCredentialBrokerURL, "https://orka-api.example")
	t.Setenv(EnvArtifactAPIURL, "https://orka-api.example")
	setProductionPublisherProxyForTest(t)
	t.Setenv(EnvGitHubPREnabled, "true")
	t.Setenv(EnvGitHubAPIBaseURL, "https://api.github.example/api/v3")
	t.Setenv(EnvGitHubRequestTimeout, "7s")
	t.Setenv(EnvGitHubMaxResponseBytes, "12345")

	config, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	factory, ok := config.PRFactory.(*GitHubPRReconcilerFactory)
	if !ok || factory == nil {
		t.Fatalf("PRFactory = %T, want GitHub factory", config.PRFactory)
	}
	if factory.apiBaseURL.String() != "https://api.github.example/api/v3" || factory.requestTimeout != 7*time.Second ||
		factory.maxResponseBytes != 12345 {
		t.Fatalf("factory = %#v", factory)
	}
}

func TestLoadConfigFromEnvRejectsImplicitGitHubPRConfiguration(t *testing.T) {
	clearPublisherEnvForTest(t)
	controller := writePublisherSecretForTest(t, "controller", strings.Repeat("c", 32))
	operation := writePublisherSecretForTest(t, "operation", strings.Repeat("o", MinSecretBytes))
	t.Setenv(EnvControllerTokenFile, controller)
	t.Setenv(EnvOperationCapabilitySecretFile, operation)
	t.Setenv(EnvArtifactAuthorizationBrokerURL, "https://orka-api.example")
	t.Setenv(EnvCredentialBrokerURL, "https://orka-api.example")
	t.Setenv(EnvArtifactAPIURL, "https://orka-api.example")
	setProductionPublisherProxyForTest(t)
	t.Setenv(EnvGitHubAPIBaseURL, "https://api.github.example")

	if _, err := LoadConfigFromEnv(); err == nil || !strings.Contains(err.Error(), EnvGitHubPREnabled+"=true") {
		t.Fatalf("LoadConfigFromEnv error = %v", err)
	}
}

func clearPublisherEnvForTest(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		EnvListenAddress, EnvControllerTokenFile, EnvOperationCapabilitySecretFile, EnvArtifactCapabilitySecretFile, EnvArtifactAuthorizationBrokerURL,
		EnvArtifactAPIURL, EnvArtifactRoot, EnvJournalRoot, EnvTempRoot, EnvCredentialRoot, EnvCredentialBrokerURL, EnvDefaultGitCredentialName,
		EnvGitBinary, EnvRequiredGitVersion, EnvAllowedSCMHosts, EnvAllowFileRepositories, EnvMaxConcurrentOperations,
		EnvMaxRequestBytes, EnvMaxResponseBytes, EnvMaxJournalBytes, EnvMaxDeltaBytes, EnvMaxBundleBytes, EnvMaxCommandOutput,
		EnvPublishTimeout, EnvArtifactTimeout, EnvCapabilityTTL, EnvWorkspaceMaxEntries, EnvWorkspaceMaxFileBytes,
		EnvWorkspaceMaxBytes, EnvWorkspaceMaxArtifactBytes, EnvWorkspaceMaxPathBytes, EnvGitHubPREnabled,
		EnvGitHubAPIBaseURL, EnvGitHubRequestTimeout, EnvGitHubMaxResponseBytes, EnvSCMEgressProxyRequired,
		EnvAllowDevelopmentFallbacks, "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy",
	} {
		t.Setenv(name, "")
	}
}

func setProductionPublisherProxyForTest(t *testing.T) {
	t.Helper()
	t.Setenv(EnvSCMEgressProxyRequired, "true")
	t.Setenv(
		"HTTPS_PROXY",
		"http://orka-publisher:"+strings.Repeat("p", 64)+"@proxy.orka-system.svc:8080",
	)
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,::1,.svc,.cluster.local")
}

func writePublisherSecretForTest(t *testing.T, name, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
