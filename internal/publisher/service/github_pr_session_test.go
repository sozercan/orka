package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orka-agents/orka/internal/publisher"
)

func TestGitHubPRReconcilerContinuesSessionAcrossPublicationsAndRetries(t *testing.T) {
	t.Parallel()
	scenario := newGitHubPRTestScenario(t)
	intent := githubTestIntent()
	intent.SessionUID = "publication-session-owner"
	key, err := intent.Key()
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := intent.SessionKey()
	if err != nil {
		t.Fatal(err)
	}
	body := githubPullRequestBody(intent.PublicationGeneration, key, sessionKey)
	scenario.create = githubTestPullRequest(scenario.base, scenario.head, 42, publisher.PullRequestOpen, intent.ExpectedHeadOID, body)
	reconciler := newGitHubTestReconciler(t, scenario.handler(t))
	first, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
	if err != nil {
		t.Fatal(err)
	}
	created := scenario.createSnapshot()
	if len(created) != 1 || created[0].Body != body || strings.Contains(body, intent.SessionUID) {
		t.Fatalf("initial PR did not bind its opaque Session marker: %#v", created)
	}
	for _, nextSHA := range []string{strings.Repeat("b", 40), strings.Repeat("c", 40)} {
		intent.ExpectedHeadOID = nextSHA
		intent.PublicationGeneration++
		currentKey, keyErr := intent.Key()
		if keyErr != nil || currentKey == key {
			t.Fatalf("publication key did not change with its exact head: %q, %v", currentKey, keyErr)
		}
		// GitHub moves an open PR's head when its branch is published. Its
		// original body remains untouched, including across client retries.
		scenario.mu.Lock()
		scenario.list = []githubPullRequest{
			githubTestPullRequest(scenario.base, scenario.head, 42, publisher.PullRequestOpen, nextSHA, body),
		}
		scenario.headSHA = nextSHA
		scenario.mu.Unlock()
		for range 2 {
			receipt, reconcileErr := publisher.ReconcilePullRequest(context.Background(), intent, reconciler)
			if reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			if receipt.ForgeID != first.ForgeID || receipt.IntentKey != currentKey || receipt.HeadOID != nextSHA {
				t.Fatalf("continuation/retry did not reuse the owned PR with an exact new receipt: %#v", receipt)
			}
		}
	}
	if len(scenario.createSnapshot()) != 1 {
		t.Fatal("Session continuation or retry created another PR")
	}
	for _, request := range scenario.requestSnapshot() {
		if strings.HasPrefix(request, "PATCH ") || strings.HasPrefix(request, "PUT ") {
			t.Fatalf("continuation overwrote the existing PR: %s", request)
		}
	}
}

func TestGitHubPRReconcilerRejectsConflictingSessionOwnership(t *testing.T) {
	t.Parallel()
	intent := githubTestIntent()
	intent.SessionUID = "publication-session-owner"
	key, err := intent.Key()
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := intent.SessionKey()
	if err != nil {
		t.Fatal(err)
	}
	marker := githubSessionMarkerPrefix + sessionKey + githubIntentMarkerSuffix
	body := githubPullRequestBody(intent.PublicationGeneration, key, sessionKey)
	other := intent
	other.SessionUID = "different-session-owner"
	otherKey, err := other.SessionKey()
	if err != nil {
		t.Fatal(err)
	}
	otherMarker := githubSessionMarkerPrefix + otherKey + githubIntentMarkerSuffix
	cases := []struct {
		name   string
		body   string
		mutate func(*githubPullRequest)
		twice  bool
	}{
		{name: "exact head marker without Session ownership", body: githubIntentMarker(key)},
		{name: "another Session owns same branch", body: githubIntentMarker(key) + "\n\n" + otherMarker},
		{name: "duplicate Session marker", body: body + "\n\n" + marker},
		{name: "two Session owners", body: otherMarker + "\n\n" + body},
		{name: "inline Session marker", body: "quoted ownership " + marker},
		{name: "wrong current head", body: body, mutate: func(pr *githubPullRequest) { pr.Head.SHA = strings.Repeat("b", 40) }},
		{name: "wrong base ref", body: body, mutate: func(pr *githubPullRequest) { pr.Base.Ref = "different-base" }},
		{name: "wrong repository", body: body, mutate: func(pr *githubPullRequest) {
			copyRepo := *pr.Head.Repo
			copyRepo.ID++
			pr.Head.Repo = &copyRepo
		}},
		{name: "duplicate owned PRs", body: body, twice: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scenario := newGitHubPRTestScenario(t)
			candidate := githubTestPullRequest(scenario.base, scenario.head, 42, publisher.PullRequestOpen, intent.ExpectedHeadOID, tc.body)
			if tc.mutate != nil {
				tc.mutate(&candidate)
			}
			scenario.list = []githubPullRequest{candidate}
			if tc.twice {
				candidate.Number++
				scenario.list = append(scenario.list, candidate)
			}
			reconciler := newGitHubTestReconciler(t, scenario.handler(t))
			if _, err := publisher.ReconcilePullRequest(context.Background(), intent, reconciler); !errors.Is(err, publisher.ErrIdempotencyConflict) {
				t.Fatalf("error = %v, want ownership conflict", err)
			}
			if len(scenario.createSnapshot()) != 0 {
				t.Fatal("conflicting Session ownership created another PR")
			}
		})
	}
}

func TestGitHubPRReconcilerDoesNotRecreateClosedSessionPullRequests(t *testing.T) {
	t.Parallel()
	for _, state := range []publisher.PullRequestState{publisher.PullRequestClosed, publisher.PullRequestMerged} {
		t.Run(string(state), func(t *testing.T) {
			scenario := newGitHubPRTestScenario(t)
			intent := githubTestIntent()
			intent.SessionUID = "publication-session-owner"
			key, err := intent.Key()
			if err != nil {
				t.Fatal(err)
			}
			sessionKey, err := intent.SessionKey()
			if err != nil {
				t.Fatal(err)
			}
			scenario.list = []githubPullRequest{githubTestPullRequest(scenario.base, scenario.head, 42, state,
				intent.ExpectedHeadOID, githubPullRequestBody(intent.PublicationGeneration, key, sessionKey))}
			receipt, err := publisher.ReconcilePullRequest(context.Background(), intent, newGitHubTestReconciler(t, scenario.handler(t)))
			if err != nil || receipt.State != state || len(scenario.createSnapshot()) != 0 {
				t.Fatalf("known %s PR was not preserved: %#v, %v", state, receipt, err)
			}
		})
	}
}
