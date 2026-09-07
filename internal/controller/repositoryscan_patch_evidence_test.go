/*
Copyright (c) 2026.
MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestSecurityPatchDecorationPreservesEditedPublisherBody(t *testing.T) {
	for _, edit := range []string{"append", "prepend", "replace description"} {
		t.Run(edit, func(t *testing.T) {
			var seenToken string
			server, pr := newPatchCommitServerWithPullRequest(t, nil, &seenToken)
			fixture := patchFixtureWithForgeSecret(t, "edited-body", server, true)
			switch edit {
			case "append":
				pr.body += "\n\nDeployment checklist: pending"
			case "prepend":
				pr.body = "Deployment checklist: pending\n\n" + pr.body
			case "replace description":
				pr.body = strings.Replace(pr.body, "Created by the Orka clean-room workspace publisher.", "Reviewed by the owner.", 1)
			}
			original := pr.body
			fixture.reconciler.decorateSecurityPatchPullRequest(context.Background(), fixture.scan, patchTaskForFixture(fixture, true), fixture.finding.ID, 42, "")
			if pr.patched != nil || pr.body != original {
				t.Fatal("decoration overwrote an edited publisher body")
			}
		})
	}
}

func TestIngestPatchTaskBindsPublishedFileModes(t *testing.T) {
	modeDiff := strings.Replace(testPatchFullDiff, "\n--- ", "\nold mode 100644\nnew mode 100755\n--- ", 1)
	modified := repositoryScanCommitFileResponse{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"}
	added := repositoryScanCommitFileResponse{Filename: "app.py", Status: "added", Additions: 1, Patch: "@@ -0,0 +1 @@\n+safe()"}
	for _, tc := range []struct {
		name     string
		file     repositoryScanCommitFileResponse
		diff     string
		artifact string
		wantPass bool
	}{
		{name: "result preserves executable change", file: modified, diff: modeDiff, wantPass: true},
		{name: "artifact matches executable change", file: modified, diff: modeDiff, artifact: modeDiff, wantPass: true},
		{name: "artifact omits executable change", file: modified, diff: modeDiff, artifact: testPatchFullDiff},
		{name: "artifact misstates executable change", file: modified, diff: modeDiff, artifact: strings.Replace(modeDiff, "new mode 100755", "new mode 100644", 1)},
		{name: "new executable file", file: added, diff: "diff --git a/app.py b/app.py\nnew file mode 100755\n--- /dev/null\n+++ b/app.py\n@@ -0,0 +1 @@\n+safe()\n", wantPass: true},
		{name: "missing full diff", file: modified},
		{name: "full diff has different content", file: modified, diff: strings.Replace(modeDiff, "+safe()", "+other()", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var seenToken string
			server, response := newPatchCommitServerWithPullRequest(t, []repositoryScanCommitFileResponse{tc.file}, &seenToken)
			response.commitDiff = tc.diff
			fixture := patchFixtureWithForgeSecret(t, "file-modes", server, true)
			if err := fixture.store.SaveResult(ctx, fixture.proposal.Namespace, fixture.proposal.TaskName, repositoryScanPatchResultEnvelope(fixture, []string{"app.py"})); err != nil {
				t.Fatal(err)
			}
			if tc.artifact != "" {
				savePatchArtifacts(t, fixture, tc.artifact, []string{"app.py"})
			}
			task := patchTaskForFixture(fixture, true)
			if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
				t.Fatal(err)
			}
			if !tc.wantPass {
				assertPatchIngestState(t, fixture, scanRunPhaseFailed, findingStateOpen)
				return
			}
			assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
			diffName, _ := patchArtifactNames(fixture.finding.ID)
			data, _, err := fixture.store.GetArtifact(ctx, task.Namespace, task.Name, diffName)
			if err != nil || string(data) != tc.diff {
				t.Fatal("verified artifact did not preserve the published file-mode transition")
			}
			if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
				t.Fatal(err)
			}
			assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
		})
	}
}

func TestIngestPatchTaskRetriesTransientPublishedDiffFailure(t *testing.T) {
	ctx := context.Background()
	var seenToken string
	server, response := newPatchCommitServerWithPullRequest(t, []repositoryScanCommitFileResponse{
		{Filename: "app.py", Status: "modified", Additions: 1, Deletions: 1, Patch: "@@ -1 +1 @@\n-unsafe()\n+safe()"},
	}, &seenToken)
	response.diffStatus = http.StatusServiceUnavailable
	fixture := patchFixtureWithForgeSecret(t, "diff-outage", server, true)
	savePatchArtifacts(t, fixture, testPatchFullDiff, []string{"app.py"})
	task := patchTaskForFixture(fixture, true)
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); !errors.Is(err, errRepositoryScanPublishedCommitTransient) {
		t.Fatalf("ingestPatchTask() error = %v, want retryable diff failure", err)
	}
	assertPatchIngestState(t, fixture, scanRunPhasePending, findingStatePatchPending)
	response.diffStatus = http.StatusOK
	if err := fixture.reconciler.ingestPatchTask(ctx, fixture.scan, task); err != nil {
		t.Fatal(err)
	}
	assertPatchIngestState(t, fixture, patchProposalStatusPROpened, findingStatePROpen)
}
