package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/publisher"
)

const (
	defaultGitHubAPIBaseURL          = "https://api.github.com"
	defaultGitHubRequestTimeout      = 15 * time.Second
	defaultGitHubMaxResponseBytes    = int64(4 << 20)
	maxGitHubRequestTimeout          = 2 * time.Minute
	maxGitHubResponseBytes           = int64(16 << 20)
	maxGitHubAuthBytes               = int64(32 << 10)
	maxGitHubRequestBytes            = int64(32 << 10)
	maxGitHubReceiptURLBytes         = 2048
	maxGitHubPullRequestCandidates   = 100
	githubAPIVersion                 = "2026-03-10"
	githubProvider                   = "github"
	githubPullRequestStateOpen       = "open"
	githubPullRequestStateClosed     = "closed"
	githubIntentMarkerPrefix         = "<!-- orka.publisher.pr-intent.v1 key="
	githubSessionMarkerPrefix        = "<!-- orka.publisher.pr-session.v1 key="
	githubIntentMarkerSuffix         = " -->"
	githubPullRequestTitlePrefix     = "Orka publication generation "
	githubPullRequestBodyDescription = "Created by the Orka clean-room workspace publisher."
)

var githubRepositoryComponentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$`)

// GitHubPRReconcilerFactoryConfig contains only non-secret GitHub REST client
// configuration. Each reconciler receives its token from the operation-scoped
// credential file passed to PRReconcilerFactory.New.
type GitHubPRReconcilerFactoryConfig struct {
	APIBaseURL       string
	HTTPClient       *http.Client
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

// GitHubPRReconcilerFactory creates one short-lived reconciler per operation.
// It never accepts a token in configuration or environment variables.
type GitHubPRReconcilerFactory struct {
	apiBaseURL       *url.URL
	httpClient       *http.Client
	requestTimeout   time.Duration
	maxResponseBytes int64
}

// NewGitHubPRReconcilerFactory creates a strict GitHub REST factory. Redirects
// are denied, every request has a hard deadline, and every response is bounded.
func NewGitHubPRReconcilerFactory(config GitHubPRReconcilerFactoryConfig) (*GitHubPRReconcilerFactory, error) {
	apiBaseURL, err := normalizeGitHubAPIBaseURL(config.APIBaseURL)
	if err != nil {
		return nil, err
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultGitHubRequestTimeout
	}
	if requestTimeout <= 0 || requestTimeout > maxGitHubRequestTimeout {
		return nil, fmt.Errorf("GitHub request timeout must be between 1ns and %s", maxGitHubRequestTimeout)
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultGitHubMaxResponseBytes
	}
	if maxResponseBytes < 1 || maxResponseBytes > maxGitHubResponseBytes {
		return nil, fmt.Errorf("GitHub response limit must be between 1 and %d bytes", maxGitHubResponseBytes)
	}
	client := strictGitHubHTTPClient(config.HTTPClient, requestTimeout)
	return &GitHubPRReconcilerFactory{
		apiBaseURL: apiBaseURL, httpClient: client, requestTimeout: requestTimeout, maxResponseBytes: maxResponseBytes,
	}, nil
}

func (f *GitHubPRReconcilerFactory) New(ctx context.Context, credentialPath string) (publisher.PullRequestReconciler, error) {
	if f == nil || f.apiBaseURL == nil || f.httpClient == nil {
		return nil, apiError(ErrNotConfigured, "github_pr_not_configured", "GitHub pull request reconciliation is not configured", http.StatusServiceUnavailable, false, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authorization, err := readGitHubOperationAuthorization(credentialPath)
	if err != nil {
		return nil, err
	}
	return &githubPRReconciler{
		apiBaseURL: f.apiBaseURL, httpClient: f.httpClient, requestTimeout: f.requestTimeout,
		maxResponseBytes: f.maxResponseBytes, authorization: authorization,
	}, nil
}

func normalizeGitHubAPIBaseURL(raw string) (*url.URL, error) {
	if raw == "" {
		raw = defaultGitHubAPIBaseURL
	}
	if len(raw) > maxGitHubReceiptURLBytes || strings.TrimSpace(raw) != raw {
		return nil, fmt.Errorf("GitHub API base URL is empty, non-canonical, or too long")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != schemeHTTPS || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Opaque != "" || parsed.RawPath != "" {
		return nil, fmt.Errorf("GitHub API base URL must be a credential-free HTTPS URL")
	}
	if parsed.Host != strings.ToLower(parsed.Host) {
		return nil, fmt.Errorf("GitHub API base URL host must be lower-case")
	}
	if parsed.Path != "" && (path.Clean(parsed.Path) != parsed.Path || strings.HasSuffix(parsed.Path, "/")) {
		return nil, fmt.Errorf("GitHub API base URL path must be clean and have no trailing slash")
	}
	return parsed, nil
}

func strictGitHubHTTPClient(configured *http.Client, timeout time.Duration) *http.Client {
	client := &http.Client{}
	if configured != nil {
		*client = *configured
	}
	client.Timeout = timeout
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errGitHubRedirect }
	if client.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = timeout
		transport.TLSHandshakeTimeout = min(10*time.Second, timeout)
		transport.MaxResponseHeaderBytes = 64 << 10
		client.Transport = transport
	} else if transport, ok := client.Transport.(*http.Transport); ok {
		clone := transport.Clone()
		clone.ResponseHeaderTimeout = timeout
		clone.TLSHandshakeTimeout = min(10*time.Second, timeout)
		clone.MaxResponseHeaderBytes = 64 << 10
		client.Transport = clone
	}
	return client
}

var errGitHubRedirect = errors.New("GitHub REST redirects are forbidden")

func readGitHubOperationAuthorization(credentialPath string) (string, error) {
	if credentialPath == "" {
		return "", apiError(ErrCredential, "forge_credential_required", "GitHub pull request reconciliation requires an operation-scoped forge credential", http.StatusBadRequest, false, nil)
	}
	if !pathIsAbsoluteClean(credentialPath) {
		return "", apiError(ErrCredential, "forge_credential_unavailable", "operation-scoped forge credential is unavailable", http.StatusServiceUnavailable, false, nil)
	}
	info, err := os.Lstat(credentialPath)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 1 ||
		info.Size() > maxGitHubAuthBytes || info.Mode().Perm()&0o077 != 0 {
		return "", apiError(ErrCredential, "forge_credential_unavailable", "operation-scoped forge credential is unavailable", http.StatusServiceUnavailable, false, nil)
	}
	value, err := os.ReadFile(credentialPath)
	if err != nil {
		return "", apiError(ErrCredential, "forge_credential_unavailable", "operation-scoped forge credential is unavailable", http.StatusServiceUnavailable, true, nil)
	}
	if len(value) < 8 || int64(len(value)) > maxGitHubAuthBytes || bytes.ContainsAny(value, "\r\n\x00") ||
		string(value) != strings.TrimSpace(string(value)) || !isVisibleASCII(value) {
		return "", apiError(ErrCredential, "forge_credential_invalid", "operation-scoped forge credential is invalid", http.StatusBadRequest, false, nil)
	}
	return string(value), nil
}

func isVisibleASCII(value []byte) bool {
	for _, current := range value {
		if current < 0x21 || current > 0x7e {
			return false
		}
	}
	return true
}

func pathIsAbsoluteClean(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value
}

type githubPRReconciler struct {
	apiBaseURL       *url.URL
	httpClient       *http.Client
	requestTimeout   time.Duration
	maxResponseBytes int64
	authorization    string
}

type githubRepositoryIntent struct {
	owner        string
	name         string
	fullName     string
	branch       string
	repositoryID string
	htmlURL      string
	cloneURL     string
}

type githubRepository struct {
	ID       int64  `json:"id"`
	NodeID   string `json:"node_id"`
	FullName string `json:"full_name"`
	HTMLURL  string `json:"html_url"`
	CloneURL string `json:"clone_url"`
}

type githubResolvedRepository struct {
	intent githubRepositoryIntent
	api    githubRepository
}

type githubPullRequest struct {
	Number   int64                `json:"number"`
	HTMLURL  string               `json:"html_url"`
	State    string               `json:"state"`
	MergedAt *string              `json:"merged_at"`
	Body     *string              `json:"body"`
	Head     githubPullRequestRef `json:"head"`
	Base     githubPullRequestRef `json:"base"`
}

type githubPullRequestRef struct {
	Ref  string            `json:"ref"`
	SHA  string            `json:"sha"`
	Repo *githubRepository `json:"repo"`
}

type githubGitRef struct {
	Ref    string `json:"ref"`
	Object struct {
		Type string `json:"type"`
		SHA  string `json:"sha"`
	} `json:"object"`
}

type githubCreatePullRequest struct {
	Title               string `json:"title"`
	Body                string `json:"body"`
	Head                string `json:"head"`
	HeadRepository      string `json:"head_repo,omitempty"`
	Base                string `json:"base"`
	Draft               bool   `json:"draft"`
	MaintainerCanModify bool   `json:"maintainer_can_modify"`
}

func (r *githubPRReconciler) Reconcile(ctx context.Context, intent publisher.PullRequestIntent) (publisher.PullRequestReceipt, error) {
	intentKey, err := intent.Key()
	if err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	baseIntent, err := parseGitHubRepositoryIntent(intent.BaseRepository, intent.BaseRef)
	if err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	headIntent, err := parseGitHubRepositoryIntent(intent.HeadRepository, intent.HeadRef)
	if err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	if baseIntent.repositoryID == headIntent.repositoryID && baseIntent.branch == headIntent.branch {
		return publisher.PullRequestReceipt{}, &publisher.Error{
			Kind: publisher.ErrInvalidRequest, Operation: "validate GitHub pull request", Detail: "base and head cannot be the same repository and ref",
		}
	}
	base, err := r.resolveRepository(ctx, baseIntent)
	if err != nil {
		return publisher.PullRequestReceipt{}, translateGitHubAPIError(err)
	}
	head := base
	if base.intent.repositoryID != headIntent.repositoryID {
		head, err = r.resolveRepository(ctx, headIntent)
		if err != nil {
			return publisher.PullRequestReceipt{}, translateGitHubAPIError(err)
		}
	} else {
		if err := verifyGitHubRepository(headIntent, &base.api); err != nil {
			return publisher.PullRequestReceipt{}, err
		}
		head.intent = headIntent
	}
	receipt, found, err := r.lookup(ctx, intent, intentKey, base, head)
	if err != nil {
		return publisher.PullRequestReceipt{}, translateGitHubAPIError(err)
	}
	if found {
		return receipt, nil
	}
	if err := r.verifyHead(ctx, head, intent.ExpectedHeadOID); err != nil {
		return publisher.PullRequestReceipt{}, translateGitHubAPIError(err)
	}
	created, err := r.create(ctx, intent, intentKey, base, head)
	if err != nil {
		if githubCreateMayHaveSucceeded(err) {
			if recovered, ok, lookupErr := r.lookup(ctx, intent, intentKey, base, head); lookupErr == nil && ok {
				return recovered, nil
			} else if lookupErr != nil && !isGitHubAPIError(lookupErr) {
				return publisher.PullRequestReceipt{}, lookupErr
			}
		}
		return publisher.PullRequestReceipt{}, translateGitHubAPIError(err)
	}
	receipt, err = r.receiptForCandidate(intent, intentKey, base, head, created, true)
	if err == nil {
		if verifyErr := r.verifyHead(ctx, head, intent.ExpectedHeadOID); verifyErr != nil {
			return publisher.PullRequestReceipt{}, translateGitHubAPIError(verifyErr)
		}
		return receipt, nil
	}
	if recovered, ok, lookupErr := r.lookup(ctx, intent, intentKey, base, head); lookupErr == nil && ok {
		return recovered, nil
	}
	return publisher.PullRequestReceipt{}, err
}

func parseGitHubRepositoryIntent(repository publisher.Repository, ref string) (githubRepositoryIntent, error) {
	if repository.Provider != githubProvider {
		return githubRepositoryIntent{}, githubInvalidRepository("repository provider is not github")
	}
	parsed, err := url.Parse(repository.URL)
	if err != nil || parsed.Scheme != schemeHTTPS || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Opaque != "" || parsed.RawPath != "" {
		return githubRepositoryIntent{}, githubInvalidRepository("repository URL is not a credential-free canonical HTTPS URL")
	}
	trimmed := strings.TrimPrefix(parsed.Path, "/")
	trimmed = strings.TrimSuffix(trimmed, ".git")
	components := strings.Split(trimmed, "/")
	if len(components) != 2 || !githubRepositoryComponentPattern.MatchString(components[0]) || !githubRepositoryComponentPattern.MatchString(components[1]) {
		return githubRepositoryIntent{}, githubInvalidRepository("repository URL must contain exactly one owner and repository name")
	}
	fullName := components[0] + "/" + components[1]
	host := parsed.Host
	if parsed.Port() == "443" {
		host = parsed.Hostname()
		if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
	}
	htmlURL := (&url.URL{Scheme: schemeHTTPS, Host: host, Path: "/" + fullName}).String()
	cloneURL := htmlURL + ".git"
	canonicalInputURL := (&url.URL{Scheme: schemeHTTPS, Host: host, Path: parsed.Path}).String()
	if canonicalInputURL != htmlURL && canonicalInputURL != cloneURL {
		return githubRepositoryIntent{}, githubInvalidRepository("repository URL is not canonical for its identity")
	}
	branch := strings.TrimPrefix(ref, "refs/heads/")
	if branch == ref || branch == "" {
		return githubRepositoryIntent{}, &publisher.Error{Kind: publisher.ErrInvalidRef, Operation: "validate GitHub branch", Detail: "branch must be a canonical full heads ref"}
	}
	return githubRepositoryIntent{
		owner: components[0], name: components[1], fullName: fullName, branch: branch,
		repositoryID: repository.ID, htmlURL: htmlURL, cloneURL: cloneURL,
	}, nil
}

func githubInvalidRepository(detail string) error {
	return &publisher.Error{Kind: publisher.ErrInvalidRepository, Operation: "validate GitHub repository", Detail: detail}
}

func githubConflict(detail string) error {
	return &publisher.Error{Kind: publisher.ErrIdempotencyConflict, Operation: "reconcile GitHub pull request", Detail: detail}
}

func (r *githubPRReconciler) resolveRepository(ctx context.Context, intent githubRepositoryIntent) (githubResolvedRepository, error) {
	var response githubRepository
	if err := r.doJSON(ctx, http.MethodGet, r.endpoint("/repos/"+intent.owner+"/"+intent.name, nil), nil, &response, http.StatusOK); err != nil {
		return githubResolvedRepository{}, err
	}
	if err := verifyGitHubRepository(intent, &response); err != nil {
		return githubResolvedRepository{}, err
	}
	return githubResolvedRepository{intent: intent, api: response}, nil
}

func verifyGitHubRepository(expected githubRepositoryIntent, actual *githubRepository) error {
	if actual == nil || actual.ID < 1 || !strings.EqualFold(actual.FullName, expected.fullName) ||
		!strings.EqualFold(actual.HTMLURL, expected.htmlURL) || !strings.EqualFold(actual.CloneURL, expected.cloneURL) ||
		len(actual.NodeID) < 1 || len(actual.NodeID) > 512 {
		return githubConflict("GitHub repository identity does not match the persisted canonical repository")
	}
	parsedURL, err := url.Parse(expected.htmlURL)
	if err != nil {
		return githubConflict("GitHub repository canonical URL could not be interpreted")
	}
	canonicalURLID := strings.ToLower(parsedURL.Hostname()) + "/" + expected.fullName
	if expected.repositoryID != actual.NodeID && expected.repositoryID != strconv.FormatInt(actual.ID, 10) && expected.repositoryID != canonicalURLID {
		return githubConflict("GitHub repository ID does not match the persisted provider-stable or canonical URL identity")
	}
	return nil
}

func (r *githubPRReconciler) lookup(
	ctx context.Context,
	intent publisher.PullRequestIntent,
	intentKey string,
	base, head githubResolvedRepository,
) (publisher.PullRequestReceipt, bool, error) {
	query := url.Values{
		"state":    {"all"},
		"head":     {head.intent.owner + ":" + head.intent.branch},
		"base":     {base.intent.branch},
		"per_page": {strconv.Itoa(maxGitHubPullRequestCandidates)},
	}
	var candidates []githubPullRequest
	headers, err := r.doJSONWithHeaders(
		ctx, http.MethodGet, r.endpoint("/repos/"+base.intent.owner+"/"+base.intent.name+"/pulls", query), nil, &candidates, http.StatusOK,
	)
	if err != nil {
		return publisher.PullRequestReceipt{}, false, err
	}
	if hasGitHubNextPage(headers.Get("Link")) {
		return publisher.PullRequestReceipt{}, false, githubConflict("GitHub pull request candidate history exceeds the reconciliation bound")
	}
	marker, err := githubReconciliationMarker(intent, intentKey)
	if err != nil {
		return publisher.PullRequestReceipt{}, false, err
	}
	exact := make([]githubPullRequest, 0, 1)
	unrelatedOpen := false
	for i := range candidates {
		candidate := candidates[i]
		claimed := candidate.Body != nil && strings.Contains(*candidate.Body, marker)
		marked := candidate.Body != nil && githubBodyHasReconciliationMarker(*candidate.Body, marker)
		if claimed && !marked {
			return publisher.PullRequestReceipt{}, false, githubConflict("a pull request contains a malformed or duplicate exact intent marker")
		}
		matches := githubCandidateMatchesRepositoriesAndRefs(candidate, base, head)
		if marked {
			if !matches {
				return publisher.PullRequestReceipt{}, false, githubConflict("a pull request carrying the exact intent marker has mismatched repositories or refs")
			}
			if candidate.Head.SHA != intent.ExpectedHeadOID {
				return publisher.PullRequestReceipt{}, false, githubConflict("the exact pull request intent has a different head SHA")
			}
			exact = append(exact, candidate)
			continue
		}
		if matches {
			state, stateErr := githubPullRequestState(candidate)
			if stateErr != nil {
				return publisher.PullRequestReceipt{}, false, stateErr
			}
			if state == publisher.PullRequestOpen {
				unrelatedOpen = true
			}
		}
	}
	if len(exact) > 1 {
		return publisher.PullRequestReceipt{}, false, githubConflict("multiple pull requests claim the exact immutable intent")
	}
	if len(exact) == 1 {
		receipt, receiptErr := r.receiptForCandidate(intent, intentKey, base, head, exact[0], false)
		return receipt, receiptErr == nil, receiptErr
	}
	if unrelatedOpen {
		return publisher.PullRequestReceipt{}, false, githubConflict("an unrelated open pull request already uses the exact head and base repositories and refs")
	}
	return publisher.PullRequestReceipt{}, false, nil
}

func githubCandidateMatchesRepositoriesAndRefs(candidate githubPullRequest, base, head githubResolvedRepository) bool {
	return candidate.Base.Ref == base.intent.branch && candidate.Head.Ref == head.intent.branch &&
		githubRepositoryMatches(candidate.Base.Repo, base) && githubRepositoryMatches(candidate.Head.Repo, head)
}

func githubRepositoryMatches(candidate *githubRepository, expected githubResolvedRepository) bool {
	return candidate != nil && candidate.ID == expected.api.ID && candidate.NodeID == expected.api.NodeID &&
		strings.EqualFold(candidate.FullName, expected.api.FullName) && strings.EqualFold(candidate.HTMLURL, expected.api.HTMLURL) &&
		strings.EqualFold(candidate.CloneURL, expected.api.CloneURL)
}

func githubPullRequestState(candidate githubPullRequest) (publisher.PullRequestState, error) {
	switch candidate.State {
	case githubPullRequestStateOpen:
		if candidate.MergedAt != nil {
			return "", githubConflict("GitHub reported an open pull request as merged")
		}
		return publisher.PullRequestOpen, nil
	case githubPullRequestStateClosed:
		if candidate.MergedAt != nil {
			return publisher.PullRequestMerged, nil
		}
		return publisher.PullRequestClosed, nil
	default:
		return "", githubConflict("GitHub returned an unsupported pull request state")
	}
}

func (r *githubPRReconciler) receiptForCandidate(
	intent publisher.PullRequestIntent,
	intentKey string,
	base, head githubResolvedRepository,
	candidate githubPullRequest,
	requireOpen bool,
) (publisher.PullRequestReceipt, error) {
	marker, err := githubReconciliationMarker(intent, intentKey)
	if err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	if candidate.Body == nil || !githubBodyHasReconciliationMarker(*candidate.Body, marker) ||
		!githubCandidateMatchesRepositoriesAndRefs(candidate, base, head) || candidate.Head.SHA != intent.ExpectedHeadOID {
		return publisher.PullRequestReceipt{}, githubConflict("GitHub pull request does not match the exact immutable intent")
	}
	state, err := githubPullRequestState(candidate)
	if err != nil {
		return publisher.PullRequestReceipt{}, err
	}
	if requireOpen && state != publisher.PullRequestOpen {
		return publisher.PullRequestReceipt{}, githubConflict("newly created GitHub pull request is not open")
	}
	if candidate.Number < 1 {
		return publisher.PullRequestReceipt{}, githubConflict("GitHub pull request number is invalid")
	}
	expectedURL := base.intent.htmlURL + "/pull/" + strconv.FormatInt(candidate.Number, 10)
	if !strings.EqualFold(candidate.HTMLURL, expectedURL) || len(expectedURL) > maxGitHubReceiptURLBytes {
		return publisher.PullRequestReceipt{}, githubConflict("GitHub pull request URL is not canonical")
	}
	forgeID := "github:" + strconv.FormatInt(base.api.ID, 10) + ":" + strconv.FormatInt(candidate.Number, 10)
	if len(forgeID) > 128 {
		return publisher.PullRequestReceipt{}, githubConflict("GitHub pull request identity exceeds the receipt bound")
	}
	return publisher.PullRequestReceipt{
		IntentKey: intentKey, ForgeID: forgeID, URL: expectedURL, State: state, HeadOID: intent.ExpectedHeadOID,
	}, nil
}

func (r *githubPRReconciler) verifyHead(ctx context.Context, head githubResolvedRepository, expectedOID string) error {
	var response githubGitRef
	endpoint := "/repos/" + head.intent.owner + "/" + head.intent.name + "/git/ref/heads/" + head.intent.branch
	if err := r.doJSON(ctx, http.MethodGet, r.endpoint(endpoint, nil), nil, &response, http.StatusOK); err != nil {
		return err
	}
	if response.Ref != "refs/heads/"+head.intent.branch || response.Object.Type != "commit" || response.Object.SHA != expectedOID {
		return githubConflict("GitHub head ref does not equal the persisted expected head SHA")
	}
	return nil
}

func (r *githubPRReconciler) create(
	ctx context.Context,
	intent publisher.PullRequestIntent,
	intentKey string,
	base, head githubResolvedRepository,
) (githubPullRequest, error) {
	sessionKey, err := intent.SessionKey()
	if err != nil {
		return githubPullRequest{}, err
	}
	request := githubCreatePullRequest{
		Title: githubPullRequestTitlePrefix + strconv.FormatInt(intent.PublicationGeneration, 10),
		Body:  githubPullRequestBody(intent.PublicationGeneration, intentKey, sessionKey),
		Head:  head.intent.owner + ":" + head.intent.branch,
		Base:  base.intent.branch, Draft: false, MaintainerCanModify: false,
	}
	if base.api.ID != head.api.ID {
		request.HeadRepository = head.intent.name
	}
	var response githubPullRequest
	err = r.doJSON(
		ctx, http.MethodPost, r.endpoint("/repos/"+base.intent.owner+"/"+base.intent.name+"/pulls", nil), request, &response, http.StatusCreated,
	)
	return response, err
}

func githubIntentMarker(intentKey string) string {
	return githubIntentMarkerPrefix + intentKey + githubIntentMarkerSuffix
}

func githubReconciliationMarker(intent publisher.PullRequestIntent, intentKey string) (string, error) {
	if intent.SessionUID == "" {
		return githubIntentMarker(intentKey), nil
	}
	key, err := intent.SessionKey()
	if err != nil {
		return "", err
	}
	return githubSessionMarkerPrefix + key + githubIntentMarkerSuffix, nil
}

func githubBodyHasReconciliationMarker(body, marker string) bool {
	if strings.Count(body, marker) != 1 || (body != marker && !strings.HasSuffix(body, "\n\n"+marker)) {
		return false
	}
	// A body claiming multiple Sessions is ambiguous even if one exact marker
	// is the final line. A per-head marker alone never authorizes continuation.
	return !strings.HasPrefix(marker, githubSessionMarkerPrefix) || strings.Count(body, githubSessionMarkerPrefix) == 1
}

func githubPullRequestBody(generation int64, intentKey, sessionKey string) string {
	body := githubPullRequestBodyDescription + "\n\nPublication generation: " + strconv.FormatInt(generation, 10) + "\n\n" + githubIntentMarker(intentKey)
	if sessionKey != "" {
		body += "\n\n" + githubSessionMarkerPrefix + sessionKey + githubIntentMarkerSuffix
	}
	return body
}

type githubAPIRequestError struct {
	retryable        bool
	mayHaveSucceeded bool
}

func (e *githubAPIRequestError) Error() string { return "GitHub REST API request failed" }

func isGitHubAPIError(err error) bool {
	var target *githubAPIRequestError
	return errors.As(err, &target)
}

func githubCreateMayHaveSucceeded(err error) bool {
	var target *githubAPIRequestError
	return errors.As(err, &target) && target.mayHaveSucceeded
}

func translateGitHubAPIError(err error) error {
	if err == nil || !isGitHubAPIError(err) {
		return err
	}
	var apiErr *githubAPIRequestError
	_ = errors.As(err, &apiErr)
	return apiError(
		ErrSCMTransport, "github_api_error", "GitHub REST API request failed", http.StatusBadGateway, apiErr.retryable, nil,
	)
}

func (r *githubPRReconciler) endpoint(suffix string, query url.Values) string {
	result := *r.apiBaseURL
	result.Path = strings.TrimSuffix(result.Path, "/") + suffix
	result.RawQuery = query.Encode()
	return result.String()
}

func (r *githubPRReconciler) doJSON(
	ctx context.Context,
	method, endpoint string,
	input, output any,
	expectedStatus int,
) error {
	_, err := r.doJSONWithHeaders(ctx, method, endpoint, input, output, expectedStatus)
	return err
}

func (r *githubPRReconciler) doJSONWithHeaders(
	ctx context.Context,
	method, endpoint string,
	input, output any,
	expectedStatus int,
) (http.Header, error) {
	requestCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil || int64(len(encoded)) > maxGitHubRequestBytes {
			return nil, &githubAPIRequestError{}
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, body)
	if err != nil {
		return nil, &githubAPIRequestError{}
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+r.authorization)
	request.Header.Set("User-Agent", "orka-workspace-publisher")
	request.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := r.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &githubAPIRequestError{retryable: !errors.Is(err, errGitHubRedirect), mayHaveSucceeded: method == http.MethodPost && !errors.Is(err, errGitHubRedirect)}
	}
	defer response.Body.Close() //nolint:errcheck
	limited := io.LimitReader(response.Body, r.maxResponseBytes+1)
	data, readErr := io.ReadAll(limited)
	if readErr != nil || int64(len(data)) > r.maxResponseBytes {
		return nil, &githubAPIRequestError{retryable: readErr != nil || method == http.MethodPost, mayHaveSucceeded: method == http.MethodPost}
	}
	if response.StatusCode != expectedStatus {
		mayHaveSucceeded := method == http.MethodPost && (response.StatusCode == http.StatusConflict || response.StatusCode == http.StatusUnprocessableEntity ||
			response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError)
		rateLimited := response.StatusCode == http.StatusTooManyRequests || (response.StatusCode == http.StatusForbidden &&
			(response.Header.Get("Retry-After") != "" || response.Header.Get("X-RateLimit-Remaining") == "0"))
		return nil, &githubAPIRequestError{
			retryable:        rateLimited || response.StatusCode >= http.StatusInternalServerError || mayHaveSucceeded,
			mayHaveSucceeded: mayHaveSucceeded,
		}
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || (mediaType != "application/json" && mediaType != "application/vnd.github+json") {
		return nil, &githubAPIRequestError{retryable: method == http.MethodPost, mayHaveSucceeded: method == http.MethodPost}
	}
	if len(data) == 0 {
		return nil, &githubAPIRequestError{retryable: method == http.MethodPost, mayHaveSucceeded: method == http.MethodPost}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(output); err != nil {
		return nil, &githubAPIRequestError{retryable: method == http.MethodPost, mayHaveSucceeded: method == http.MethodPost}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, &githubAPIRequestError{retryable: method == http.MethodPost, mayHaveSucceeded: method == http.MethodPost}
	}
	return response.Header.Clone(), nil
}

func hasGitHubNextPage(link string) bool {
	for part := range strings.SplitSeq(link, ",") {
		if strings.Contains(part, `rel="next"`) {
			return true
		}
	}
	return false
}
