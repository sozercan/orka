/*
Copyright (c) 2026.
MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/security"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	// repositoryScanPublishedCommitMaxFiles bounds the files a remediation
	// commit may touch; GitHub's commit endpoint returns at most 300 files.
	repositoryScanPublishedCommitMaxFiles = 300
	// repositoryScanPublishedCommitPageSize is GitHub's documented per_page
	// maximum for the commit files listing.
	repositoryScanPublishedCommitPageSize    = 100
	repositoryScanForgeCredentialKey         = defaultACPWorkspaceCredentialKey
	repositoryScanNonCanonicalTargetReason   = "repository scan publication target is not a canonical GitHub repository"
	repositoryScanArtifactStoreNotConfigured = "artifact store is not configured"
)

// verifyPatchTaskEvidence produces the patch diff and summary artifacts for a
// succeeded patch task. Two contracts are accepted:
//
//   - Artifact contract: both artifacts already exist in the artifact store
//     (written by a harness that uploads workspace artifacts) and verify
//     against each other.
//   - Harness-v2 result contract: the task's terminal result is an
//     identity-bound orka.security.patch.v1 envelope. The reviewable diff is
//     never taken from agent output; it is derived from the exact commit the
//     governed publication proved on the remote, fetched with the forge
//     credential, and the envelope's changedFiles must match it exactly. Both
//     artifacts are then persisted under the standard names so the API and
//     dashboard keep one contract.
func (r *RepositoryScanReconciler) verifyPatchTaskEvidence(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	task *corev1alpha1.Task,
	findingID string,
	publication securityPatchPublicationReceipt,
) (patchVerificationResult, string, error) {
	if r.ArtifactStore == nil {
		return patchVerificationResult{}, repositoryScanArtifactStoreNotConfigured, nil
	}
	diffName, summaryName := patchArtifactNames(findingID)
	present, err := r.patchArtifactsPresent(ctx, task, diffName, summaryName)
	if err != nil {
		return patchVerificationResult{}, "", err
	}
	if present {
		verified, reason, err := r.verifyPatchTaskArtifacts(ctx, scan, task, findingID)
		if err != nil || reason != "" {
			return verified, reason, err
		}
		// Pre-existing artifacts are internally consistent but unproven:
		// the namespace-scoped internal upload API (or a stale earlier
		// attempt) could have seeded a mutually consistent diff/summary
		// pair for unrelated content. Bind them to the exact published
		// commit before accepting them as review evidence.
		reason, err = r.verifyArtifactDiffMatchesPublishedCommit(ctx, scan, task, diffName, publication)
		if err != nil || reason != "" {
			return patchVerificationResult{}, reason, err
		}
		// Worker-written artifacts are raw. Persist the validated summary
		// and the credential-redacted diff so the durable evidence carries
		// the same guarantees as the result-contract branch below: a
		// remediation that removed a checked-in credential must not keep it
		// in the referenced artifacts. The sanitized diff still binds to the
		// published commit on later reconciles (see the dual comparison in
		// verifyArtifactDiffMatchesPublishedCommit).
		if err := r.persistSanitizedPatchArtifacts(ctx, task, diffName, summaryName, verified.summary); err != nil {
			return patchVerificationResult{}, "", err
		}
		return patchVerificationResult{diffArtifact: diffName, summaryArtifact: summaryName}, "", nil
	}

	result, validationProblem, err := r.loadAgentTaskResult(ctx, task)
	if err != nil {
		return patchVerificationResult{}, "", err
	}
	if validationProblem != "" {
		return patchVerificationResult{}, "patch artifacts are missing and the " + validationProblem, nil
	}
	summary, err := security.ParsePatchResult(result, security.PatchResultExpectation{RepositoryScan: scan.Name, FindingID: findingID})
	if err != nil {
		return patchVerificationResult{}, "patch terminal result is missing or invalid: " + err.Error(), nil
	}
	if publication.publication == nil || strings.TrimSpace(publication.publication.ExpectedCommitSHA) == "" {
		return patchVerificationResult{}, "verified patch publication commit is unavailable", nil
	}
	token, reason, err := r.repositoryScanForgeToken(ctx, scan)
	if err != nil || reason != "" {
		return patchVerificationResult{}, reason, err
	}
	targetRepo := security.CanonicalRepositoryCloneURL(scan.Spec.ForkRepo)
	if targetRepo == "" {
		targetRepo = security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL)
	}
	owner, repository, err := security.ParseGitHubRepositoryURL(targetRepo)
	if err != nil {
		return patchVerificationResult{}, repositoryScanNonCanonicalTargetReason, nil
	}
	files, reason, err := r.fetchRepositoryScanPublishedCommit(ctx, owner, repository, publication.publication.ExpectedCommitSHA, token)
	if err != nil || reason != "" {
		return patchVerificationResult{}, reason, err
	}
	diff, commitPaths, reason := repositoryScanDiffFromPublishedCommit(files)
	if reason != "" {
		return patchVerificationResult{}, reason, nil
	}
	if _, err := repositoryMonitorPathsFromPatch(diff); err != nil {
		return patchVerificationResult{}, "published commit diff is not a canonical git diff: " + err.Error(), nil
	}
	if !sameStringSet(rootRelativePatchSummaryFiles(summary.ChangedFiles, scan), commitPaths) {
		return patchVerificationResult{}, "patch result changedFiles do not match the published commit", nil
	}
	summaryData, err := json.Marshal(summary)
	if err != nil {
		return patchVerificationResult{}, "", err
	}
	// The published diff can legitimately carry a removed credential on a
	// deleted line; redact credential shapes (and strip controls) so the
	// durable artifact never preserves the secret the remediation removed.
	// Path and changed-file checks above ran on the unmodified diff.
	diff = repositoryMonitorReviewContextSanitize(diff)
	if err := r.ArtifactStore.SaveArtifact(ctx, task.Namespace, task.Name, diffName, "text/x-diff", []byte(diff)); err != nil {
		return patchVerificationResult{}, "", err
	}
	if err := r.ArtifactStore.SaveArtifact(ctx, task.Namespace, task.Name, summaryName, "application/json", summaryData); err != nil {
		return patchVerificationResult{}, "", err
	}
	return patchVerificationResult{diffArtifact: diffName, summaryArtifact: summaryName}, "", nil
}

// persistSanitizedPatchArtifacts rewrites pre-existing patch artifacts in
// their validated form: the normalised summary and the credential-redacted
// diff. Path and content binding already ran on the unmodified diff.
func (r *RepositoryScanReconciler) persistSanitizedPatchArtifacts(
	ctx context.Context,
	task *corev1alpha1.Task,
	diffName, summaryName string,
	summary *security.PatchSummaryArtifact,
) error {
	if summary == nil {
		return fmt.Errorf("validated patch summary is missing for %s", summaryName)
	}
	summaryData, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	if err := r.ArtifactStore.SaveArtifact(ctx, task.Namespace, task.Name, summaryName, "application/json", summaryData); err != nil {
		return err
	}
	diffData, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, diffName)
	if err != nil {
		return err
	}
	if sanitized := repositoryMonitorReviewContextSanitize(string(diffData)); sanitized != string(diffData) {
		if err := r.ArtifactStore.SaveArtifact(ctx, task.Namespace, task.Name, diffName, "text/x-diff", []byte(sanitized)); err != nil {
			return err
		}
	}
	return nil
}

// repositoryScanHTTPClient returns the reconciler's client or a bounded
// default. http.DefaultClient has no timeout, and a stalled GitHub response
// would block the scan reconcile worker indefinitely.
func (r *RepositoryScanReconciler) repositoryScanHTTPClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// verifyArtifactDiffMatchesPublishedCommit fails closed unless the stored
// diff artifact touches exactly the file set of the publication's verified
// commit, fetched fresh with the forge credential.
func (r *RepositoryScanReconciler) verifyArtifactDiffMatchesPublishedCommit(
	ctx context.Context,
	scan *corev1alpha1.RepositoryScan,
	task *corev1alpha1.Task,
	diffName string,
	publication securityPatchPublicationReceipt,
) (string, error) {
	if publication.publication == nil || strings.TrimSpace(publication.publication.ExpectedCommitSHA) == "" {
		return "verified patch publication commit is unavailable", nil
	}
	token, reason, err := r.repositoryScanForgeToken(ctx, scan)
	if err != nil || reason != "" {
		return reason, err
	}
	targetRepo := security.CanonicalRepositoryCloneURL(scan.Spec.ForkRepo)
	if targetRepo == "" {
		targetRepo = security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL)
	}
	owner, repository, err := security.ParseGitHubRepositoryURL(targetRepo)
	if err != nil {
		return repositoryScanNonCanonicalTargetReason, nil
	}
	files, reason, err := r.fetchRepositoryScanPublishedCommit(ctx, owner, repository, publication.publication.ExpectedCommitSHA, token)
	if err != nil || reason != "" {
		return reason, err
	}
	commitDiff, commitPaths, reason := repositoryScanDiffFromPublishedCommit(files)
	if reason != "" {
		return reason, nil
	}
	diffData, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, diffName)
	if err != nil {
		return "", err
	}
	artifactPaths, err := repositoryMonitorPathsFromPatch(string(diffData))
	if err != nil {
		return "patch diff artifact is not a canonical git diff: " + err.Error(), nil
	}
	if !sameStringSet(artifactPaths, commitPaths) {
		return "patch diff artifact does not match the published commit", nil
	}
	// Filenames alone are spoofable: an unrelated diff touching the same
	// paths would pass the set check. Bind the content too — each file's
	// complete hunk body (path headers, positions, context, and every changed
	// line, however it is prefixed) must be identical to the published
	// commit's. Index headers, which legitimately vary between diff generators
	// without changing the represented patch, are excluded.
	// Mode, rename, copy, similarity, and binary metadata is rejected because
	// the commit-files response does not provide enough data to verify it.
	// Stored artifacts come in two provenances: the result-contract path
	// persists the commit diff credential-redacted, while legacy
	// harness-written artifacts are raw — so the stored diff must match the
	// fresh commit diff in either its raw or its sanitized form (both are
	// exact commit-derived representations; the sanitizer is deterministic).
	// Without the sanitized comparison, the second reconcile of an already
	// verified proposal would fail solely because [REDACTED] differs from
	// the credential the remediation removed.
	if !samePatchHunks(string(diffData), commitDiff) &&
		!samePatchHunks(string(diffData), repositoryMonitorReviewContextSanitize(commitDiff)) {
		return "patch diff artifact content does not match the published commit", nil
	}
	return "", nil
}

// patchHunksByFile binds each file's canonical old/new path headers to its
// verbatim hunk body, from the first "@@" line to the next file header.
var recognizedDiffMetadataPattern = regexp.MustCompile(`^(?:index [0-9a-f]+\.\.[0-9a-f]+(?: [0-7]{6})?|--- .+|\+\+\+ .+)$`)

func patchHunksByFile(diff string) (map[string]string, bool) {
	const nullPath = "/dev/null"

	hunks := map[string]string{}
	seenPaths := map[string]struct{}{}
	var body []string
	current := ""
	oldHeader := ""
	newHeader := ""
	inHunk := false
	duplicate := false
	invalidBlock := false
	flush := func() {
		if current != "" {
			wantOld := "a/" + current
			wantNew := "b/" + current
			headersValid := (oldHeader == wantOld && newHeader == wantNew) ||
				(oldHeader == nullPath && newHeader == wantNew) ||
				(oldHeader == wantOld && newHeader == nullPath)
			if !headersValid || len(body) == 0 {
				invalidBlock = true
			} else {
				hunks[current] = oldHeader + "\n" + newHeader + "\n" + strings.TrimRight(strings.Join(body, "\n"), "\n")
			}
		}
		body = nil
		oldHeader = ""
		newHeader = ""
		inHunk = false
	}
	unknownPrefix := false
	for line := range strings.SplitSeq(diff, "\n") {
		if strings.HasPrefix(line, "diff --git ") {
			flush()
			current = ""
			fields := strings.Fields(line)
			if len(fields) == 4 && strings.HasPrefix(fields[2], "a/") && strings.HasPrefix(fields[3], "b/") {
				candidate := strings.TrimPrefix(fields[3], "b/")
				if candidate != "" && fields[2] == "a/"+candidate {
					current = candidate
				} else {
					invalidBlock = true
				}
			} else {
				invalidBlock = true
			}
			// Any repeated file block — hunkless or not — invalidates the
			// diff: a second header for the same path could hide arbitrary
			// content the comparison would never see.
			if current != "" {
				if _, exists := seenPaths[current]; exists {
					duplicate = true
				}
				seenPaths[current] = struct{}{}
			}
			continue
		}
		if inHunk {
			body = append(body, line)
			continue
		}
		if strings.HasPrefix(line, "@@") {
			inHunk = true
			body = append(body, line)
			continue
		}
		if strings.HasPrefix(line, "--- ") {
			if oldHeader != "" || newHeader != "" {
				invalidBlock = true
			}
			oldHeader = strings.TrimPrefix(line, "--- ")
			continue
		}
		if strings.HasPrefix(line, "+++ ") {
			if oldHeader == "" || newHeader != "" {
				invalidBlock = true
			}
			newHeader = strings.TrimPrefix(line, "+++ ")
			continue
		}
		// Outside hunks, only recognized diff metadata may appear: a
		// fabricated reviewer-facing line smuggled before the first hunk
		// would otherwise render in the artifact yet escape comparison.
		if !recognizedDiffMetadataLine(line) {
			unknownPrefix = true
		}
	}
	flush()
	return hunks, !duplicate && !unknownPrefix && !invalidBlock
}

// recognizedDiffMetadataLine reports whether a pre-hunk line is standard git
// diff metadata.
func recognizedDiffMetadataLine(line string) bool {
	if strings.TrimSpace(line) == "" {
		return true
	}
	return recognizedDiffMetadataPattern.MatchString(line)
}

func samePatchHunks(a, b string) bool {
	hunksA, okA := patchHunksByFile(a)
	hunksB, okB := patchHunksByFile(b)
	if !okA || !okB || len(hunksA) != len(hunksB) {
		return false
	}
	for path, hunk := range hunksA {
		if other, ok := hunksB[path]; !ok || hunk != other {
			return false
		}
	}
	return true
}

func (r *RepositoryScanReconciler) patchArtifactsPresent(ctx context.Context, task *corev1alpha1.Task, names ...string) (bool, error) {
	for _, name := range names {
		if _, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, name); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

// repositoryScanForgeToken reads the scan's forge credential. The token never
// leaves the controller: it authenticates the read of the published commit and
// is not persisted or logged.
func (r *RepositoryScanReconciler) repositoryScanForgeToken(ctx context.Context, scan *corev1alpha1.RepositoryScan) (string, string, error) {
	if scan.Spec.ForgeCredentialRef == nil || strings.TrimSpace(scan.Spec.ForgeCredentialRef.Name) == "" {
		return "", "spec.forgeCredentialRef is required to verify the published patch", nil
	}
	var reader = r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if reader == nil {
		return "", "forge credential client is not configured", nil
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: scan.Namespace, Name: strings.TrimSpace(scan.Spec.ForgeCredentialRef.Name)}
	if err := reader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "forge credential secret was not found", nil
		}
		return "", "", err
	}
	for _, name := range []string{repositoryScanForgeCredentialKey, "password", workerenv.GitHubToken} {
		if value := strings.TrimSpace(string(secret.Data[name])); value != "" {
			return value, "", nil
		}
	}
	return "", "forge credential secret carries no token", nil
}

type repositoryScanCommitFileResponse struct {
	Filename         string `json:"filename"`
	PreviousFilename string `json:"previous_filename"`
	Status           string `json:"status"`
	Patch            string `json:"patch"`
	Additions        int    `json:"additions"`
	Deletions        int    `json:"deletions"`
}

type repositoryScanCommitResponse struct {
	SHA   string                             `json:"sha"`
	Files []repositoryScanCommitFileResponse `json:"files"`
}

// errRepositoryScanPublishedCommitTransient marks a GitHub read that failed
// for a reason expected to clear on its own (transport error, throttling, or
// a server-side error). It is returned as a reconcile error so the patch
// verification is retried rather than settled as a failed proposal.
var errRepositoryScanPublishedCommitTransient = errors.New("published patch commit could not be read from GitHub; retrying")

// repositoryScanGitHubStatusTransient reports the response statuses that are
// retried: request timeout, rate limiting, and every server-side error.
// Other non-2xx statuses (for example 401, 403, 404, 422) are terminal for
// the proposal.
func repositoryScanGitHubStatusTransient(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

func (r *RepositoryScanReconciler) fetchRepositoryScanPublishedCommit(ctx context.Context, owner, repository, sha, token string) ([]repositoryScanCommitFileResponse, string, error) {
	if store.ValidateGitObjectID("published patch commit", sha) != nil {
		return nil, "verified patch publication commit is invalid", nil
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	// The commit endpoint paginates its files array; per_page's documented
	// maximum is 100, so pages are followed explicitly up to the supported
	// file-count bound and anything beyond fails closed.
	var files []repositoryScanCommitFileResponse
	maxPages := repositoryScanPublishedCommitMaxFiles / repositoryScanPublishedCommitPageSize
	for page := 1; ; page++ {
		if page > maxPages {
			return nil, "published patch commit exceeds the supported file count", nil
		}
		endpoint := fmt.Sprintf("%s/repos/%s/%s/commits/%s?per_page=%d&page=%d",
			baseURL, url.PathEscape(owner), url.PathEscape(repository), url.PathEscape(sha), repositoryScanPublishedCommitPageSize, page)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, "", err
		}
		repositoryMonitorSetGitHubHeaders(req, token)
		resp, err := r.repositoryScanHTTPClient().Do(req)
		if err != nil {
			// A transport failure is transient: surface it as a reconcile
			// error so controller-runtime retries the verification instead
			// of persisting the proposal as failed. The underlying error may
			// carry the request URL, so it is logged rather than returned.
			log.FromContext(ctx).Error(err, "published patch commit request failed", "owner", owner, "repository", repository, "sha", sha)
			return nil, "", errRepositoryScanPublishedCommitTransient
		}
		body, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
		_ = resp.Body.Close()
		if err != nil {
			return nil, "published patch commit response exceeded the read limit", nil
		}
		if repositoryScanGitHubStatusTransient(resp.StatusCode) {
			log.FromContext(ctx).Info("published patch commit request returned a transient status", "status", resp.StatusCode, "owner", owner, "repository", repository, "sha", sha)
			return nil, "", fmt.Errorf("%w (HTTP %d)", errRepositoryScanPublishedCommitTransient, resp.StatusCode)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Sprintf("published patch commit could not be read from GitHub (HTTP %d)", resp.StatusCode), nil
		}
		var commit repositoryScanCommitResponse
		if err := json.Unmarshal(body, &commit); err != nil {
			return nil, "published patch commit response is not valid JSON", nil
		}
		// Every page must identify the same verified commit.
		if !strings.EqualFold(strings.TrimSpace(commit.SHA), sha) {
			return nil, "published patch commit response does not match the verified commit", nil
		}
		files = append(files, commit.Files...)
		if len(files) > repositoryScanPublishedCommitMaxFiles {
			return nil, "published patch commit exceeds the supported file count", nil
		}
		if !strings.Contains(resp.Header.Get("Link"), `rel="next"`) {
			break
		}
	}
	if len(files) == 0 {
		return nil, "published patch commit contains no files", nil
	}
	return files, "", nil
}

// repositoryScanDiffFromPublishedCommit renders the published commit's file
// patches as one canonical git diff. Renames, copies, and files without a
// textual patch (binary or oversized) cannot be reviewed as a patch and fail
// closed.
func repositoryScanDiffFromPublishedCommit(files []repositoryScanCommitFileResponse) (string, []string, string) {
	var diff strings.Builder
	paths := make([]string, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		path := strings.TrimSpace(file.Filename)
		// Trimming must not rewrite identity: a legal filename with leading
		// or trailing whitespace would otherwise verify as a different path.
		if path != file.Filename {
			return "", nil, "published patch commit contains a whitespace-altered file path"
		}
		if path == "" || !security.SafeRepoPath(path) || strings.ContainsAny(path, " \"\\\t\r\n") {
			return "", nil, "published patch commit contains an unsafe file path"
		}
		if _, duplicate := seen[path]; duplicate {
			return "", nil, "published patch commit repeats a file path: " + path
		}
		seen[path] = struct{}{}
		if strings.TrimSpace(file.PreviousFilename) != "" && strings.TrimSpace(file.PreviousFilename) != path {
			return "", nil, "published patch commit renames or copies a file, which security patches do not support"
		}
		if strings.TrimSpace(file.Patch) == "" {
			return "", nil, "published patch commit contains a file without a text patch: " + path
		}
		// GitHub can serve a nonempty but truncated patch for a large
		// change; a fragment whose line totals disagree with the reported
		// counts would hide part of the actual commit from the evidence.
		if !patchMatchesLineCounts(file.Patch, file.Additions, file.Deletions) {
			return "", nil, "published patch commit contains an inconsistent file patch: " + path
		}
		fmt.Fprintf(&diff, "diff --git a/%s b/%s\n", path, path)
		// Mode lines are intentionally omitted: the commit file listing does
		// not carry file modes, and fabricating "100644" would misrepresent
		// an executable-bit change to reviewers.
		switch strings.ToLower(strings.TrimSpace(file.Status)) {
		case "added":
			fmt.Fprintf(&diff, "--- /dev/null\n+++ b/%s\n", path)
		case "removed":
			fmt.Fprintf(&diff, "--- a/%s\n+++ /dev/null\n", path)
		case "modified", "changed", "":
			fmt.Fprintf(&diff, "--- a/%s\n+++ b/%s\n", path, path)
		default:
			return "", nil, "published patch commit contains an unsupported change: " + strings.TrimSpace(file.Status)
		}
		diff.WriteString(strings.TrimRight(file.Patch, "\n"))
		diff.WriteString("\n")
		paths = append(paths, path)
	}
	return diff.String(), paths, ""
}

const (
	// repositoryScanGenericPublicationTitlePrefix is the publisher's default
	// pull request title; a remediation PR still carrying it has not been
	// decorated with the finding yet.
	repositoryScanGenericPublicationTitlePrefix = "Orka publication generation "
	repositoryScanIntentMarkerPrefix            = "<!-- orka.publisher.pr-intent.v1 key="
	repositoryScanPullRequestBodyField          = "body"
	repositoryScanPullRequestTitleField         = "title"
)

// decorateSecurityPatchPullRequest gives the publisher-created remediation
// pull request a reviewer-facing title and body derived from the finding and
// the verified patch summary. It is best-effort and idempotent: only a PR
// that still carries the publisher's generic title is updated, the
// publisher's intent marker is preserved as the final body line so the PR
// remains recognizable to the clean-room publisher, and any failure is
// logged without affecting the proposal.
func (r *RepositoryScanReconciler) decorateSecurityPatchPullRequest(ctx context.Context, scan *corev1alpha1.RepositoryScan, task *corev1alpha1.Task, findingID string, prNumber int, summaryArtifact string) {
	logger := log.FromContext(ctx).WithValues("namespace", task.Namespace, "task", task.Name, "finding", findingID, "pullRequest", prNumber)
	if prNumber <= 0 || r.SecurityStore == nil || r.ArtifactStore == nil {
		return
	}
	finding, err := r.SecurityStore.GetFinding(ctx, scan.Namespace, findingID)
	if err != nil {
		logger.Info("skipping remediation pull request decoration", "reason", "finding unavailable")
		return
	}
	var summary *security.PatchSummaryArtifact
	if strings.TrimSpace(summaryArtifact) != "" {
		if data, _, err := r.ArtifactStore.GetArtifact(ctx, task.Namespace, task.Name, summaryArtifact); err == nil {
			var parsed security.PatchSummaryArtifact
			if json.Unmarshal(data, &parsed) == nil {
				summary = &parsed
			}
		}
	}
	token, reason, err := r.repositoryScanForgeToken(ctx, scan)
	if err != nil || reason != "" {
		logger.Info("skipping remediation pull request decoration", "reason", "forge credential unavailable")
		return
	}
	targetRepo := security.CanonicalRepositoryCloneURL(scan.Spec.RepoURL)
	owner, repository, err := security.ParseGitHubRepositoryURL(targetRepo)
	if err != nil {
		return
	}
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), prNumber)
	client := r.repositoryScanHTTPClient()
	current, ok := r.readRemediationPullRequest(ctx, client, endpoint, token, logger)
	if !ok || !strings.HasPrefix(strings.TrimSpace(current.Title), repositoryScanGenericPublicationTitlePrefix) {
		return
	}
	marker := ""
	for line := range strings.SplitSeq(current.Body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), repositoryScanIntentMarkerPrefix) {
			marker = strings.TrimSpace(line)
		}
	}
	if marker == "" {
		logger.Info("skipping remediation pull request decoration", "reason", "publisher intent marker missing")
		return
	}
	title := security.RemediationPullRequestTitle(finding)
	body := security.RemediationPullRequestBody(finding, summary) + "\n\n" + marker
	if security.LooksLikeSecret(title) || security.LooksLikeSecret(body) {
		logger.Info("skipping remediation pull request decoration", "reason", "rendered text looks like a secret")
		return
	}
	payload, err := json.Marshal(map[string]string{repositoryScanPullRequestTitleField: title, repositoryScanPullRequestBodyField: body})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Info("skipping remediation pull request decoration", "reason", "GitHub request failed")
		return
	}
	defer resp.Body.Close() //nolint:errcheck
	if _, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit); err != nil {
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Info("remediation pull request decoration rejected by GitHub", "status", resp.StatusCode)
		return
	}
	logger.Info("remediation pull request decorated with the finding")
}

type repositoryScanPullRequestResponse struct {
	Title          string     `json:"title"`
	Body           string     `json:"body"`
	Merged         bool       `json:"merged"`
	MergedAt       *time.Time `json:"merged_at"`
	MergeCommitSHA string     `json:"merge_commit_sha"`
	Base           struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

type repositoryScanCompareResponse struct {
	Status string `json:"status"`
}

func repositoryScanMergedResolutionRetryable(failure *repositoryMonitorGitHubAPIError) bool {
	if failure == nil {
		return false
	}
	if failure.StatusCode == http.StatusUnauthorized || failure.StatusCode == http.StatusForbidden {
		return true
	}
	return repositoryMonitorFailedCommandRunRetryable("[" + repositoryMonitorRunFailureState(failure) + "]")
}

func (r *RepositoryScanReconciler) repositoryScanPullRequestMerged(ctx context.Context, owner, repository, token string, prNumber int, targetBranch, scanHeadCommit string) (bool, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", baseURL, url.PathEscape(owner), url.PathEscape(repository), prNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := r.repositoryScanHTTPClient().Do(req)
	if err != nil {
		return false, fmt.Errorf("read remediation pull request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		failure := &repositoryMonitorGitHubAPIError{Operation: "read remediation pull request", StatusCode: resp.StatusCode, Body: string(body)}
		if repositoryScanMergedResolutionRetryable(failure) {
			return false, failure
		}
		return false, nil
	}
	var current repositoryScanPullRequestResponse
	if err := json.Unmarshal(body, &current); err != nil {
		return false, fmt.Errorf("decode remediation pull request: %w", err)
	}
	targetBranch = strings.TrimPrefix(strings.TrimSpace(targetBranch), "refs/heads/")
	baseBranch := strings.TrimPrefix(strings.TrimSpace(current.Base.Ref), "refs/heads/")
	mergeCommit := strings.TrimSpace(current.MergeCommitSHA)
	scanHeadCommit = strings.TrimSpace(scanHeadCommit)
	if targetBranch == "" || strings.HasPrefix(targetBranch, "ref:") || baseBranch != targetBranch || (!current.Merged && current.MergedAt == nil) ||
		store.ValidateGitObjectID("remediation merge commit", mergeCommit) != nil || store.ValidateGitObjectID("scan head commit", scanHeadCommit) != nil {
		return false, nil
	}
	return r.repositoryScanHeadContainsCommit(ctx, owner, repository, token, mergeCommit, scanHeadCommit)
}

func (r *RepositoryScanReconciler) repositoryScanHeadContainsCommit(ctx context.Context, owner, repository, token, commit, scanHeadCommit string) (bool, error) {
	baseURL := strings.TrimRight(r.GitHubAPIBaseURL, "/")
	if baseURL == "" {
		baseURL = repositoryMonitorDefaultGitHubAPIBaseURL
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/compare/%s...%s", baseURL, url.PathEscape(owner), url.PathEscape(repository), url.PathEscape(commit), url.PathEscape(scanHeadCommit))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := r.repositoryScanHTTPClient().Do(req)
	if err != nil {
		return false, fmt.Errorf("compare remediation merge commit to scan head: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil {
		return false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		failure := &repositoryMonitorGitHubAPIError{Operation: "compare remediation merge commit to scan head", StatusCode: resp.StatusCode, Body: string(body)}
		if repositoryScanMergedResolutionRetryable(failure) {
			return false, failure
		}
		return false, nil
	}
	var comparison repositoryScanCompareResponse
	if err := json.Unmarshal(body, &comparison); err != nil {
		return false, fmt.Errorf("decode remediation merge comparison: %w", err)
	}
	switch strings.TrimSpace(comparison.Status) {
	case "ahead", "identical":
		return true, nil
	default:
		return false, nil
	}
}

func (r *RepositoryScanReconciler) readRemediationPullRequest(ctx context.Context, client *http.Client, endpoint, token string, logger logr.Logger) (repositoryScanPullRequestResponse, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return repositoryScanPullRequestResponse{}, false
	}
	repositoryMonitorSetGitHubHeaders(req, token)
	resp, err := client.Do(req)
	if err != nil {
		logger.Info("skipping remediation pull request decoration", "reason", "GitHub request failed")
		return repositoryScanPullRequestResponse{}, false
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := readRepositoryMonitorGitHubResponse(resp.Body, repositoryMonitorGitHubResponseLimit)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Info("skipping remediation pull request decoration", "reason", "pull request could not be read", "status", resp.StatusCode)
		return repositoryScanPullRequestResponse{}, false
	}
	var current repositoryScanPullRequestResponse
	if err := json.Unmarshal(body, &current); err != nil {
		return repositoryScanPullRequestResponse{}, false
	}
	return current, true
}
