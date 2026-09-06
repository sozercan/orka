/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

const (
	testScanRunID2 = "scan-2"
	testStateOpen  = "open"
)

func testPatchProposalPublication(suffix string) (*store.PatchProposal, *store.PatchProposal) {
	proposal := &store.PatchProposal{
		ID:             "patch-" + suffix,
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		FindingID:      "finding-" + suffix,
		TaskName:       "task-" + suffix,
		Branch:         "orka/security/" + suffix,
		Status:         "pending",
	}
	bound := *proposal
	prNumber := 42
	headSHA := strings.Repeat("b", 40)
	bound.DiffArtifact = "security-patch-" + suffix + ".diff"
	bound.SummaryArtifact = "security-patch-" + suffix + ".json"
	bound.Status = securityPatchProposalStatusPROpened
	bound.PRNumber = &prNumber
	bound.PRURL = "https://github.com/example/source/pull/42"
	bound.PublicationEvidence = &store.PatchPublicationEvidence{
		PublicationID:      "pub-" + suffix,
		ArtifactDigest:     "sha256:" + strings.Repeat("a", 64),
		SourceRepositoryID: "github.com/example/source",
		SourceRef:          strings.Repeat("1", 40),
		SourceBaselineSHA:  strings.Repeat("1", 40),
		TargetRepositoryID: "github.com/example/target",
		TargetRef:          "refs/heads/" + bound.Branch,
		ExpectedCommitSHA:  headSHA,
		VerifiedRemoteSHA:  headSHA,
		PRIntent: store.PullRequestIntent{
			BaseRepositoryID:      "github.com/example/source",
			BaseRef:               "refs/heads/main",
			HeadRepositoryID:      "github.com/example/target",
			HeadRef:               "refs/heads/" + bound.Branch,
			PublicationGeneration: 1,
			ExpectedHeadSHA:       headSHA,
		},
		PRReceipt: store.PatchPullRequestEvidence{
			IntentKey: "sha256:" + strings.Repeat("c", 64),
			ForgeID:   "github:123:42",
			Number:    prNumber,
			URL:       bound.PRURL,
			State:     "Open",
			HeadSHA:   headSHA,
		},
	}
	return proposal, &bound
}

func clonePatchProposal(proposal *store.PatchProposal) *store.PatchProposal {
	if proposal == nil {
		return nil
	}
	clone := *proposal
	if proposal.PRNumber != nil {
		value := *proposal.PRNumber
		clone.PRNumber = &value
	}
	if proposal.PublicationEvidence != nil {
		evidence := *proposal.PublicationEvidence
		clone.PublicationEvidence = &evidence
	}
	return &clone
}

func onlyPatchProposal(t *testing.T, s *Store, namespace, findingID string) store.PatchProposal {
	t.Helper()
	proposals, err := s.ListPatchProposals(context.Background(), namespace, findingID)
	if err != nil {
		t.Fatalf("ListPatchProposals() error = %v", err)
	}
	if len(proposals) != 1 {
		t.Fatalf("len(proposals) = %d, want 1", len(proposals))
	}
	return proposals[0]
}

func TestPatchProposalPublicationEvidenceBindIsImmutableAndReplaySafe(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	initial, bound := testPatchProposalPublication("immutable")
	if err := s.CreatePatchProposal(ctx, initial); err != nil {
		t.Fatalf("CreatePatchProposal() error = %v", err)
	}
	if err := s.BindPatchProposalPublicationEvidence(ctx, bound); err != nil {
		t.Fatalf("BindPatchProposalPublicationEvidence() error = %v", err)
	}

	stored := onlyPatchProposal(t, s, initial.Namespace, initial.FindingID)
	if !reflect.DeepEqual(stored.PublicationEvidence, bound.PublicationEvidence) {
		t.Fatalf("publication evidence = %#v, want %#v", stored.PublicationEvidence, bound.PublicationEvidence)
	}
	firstUpdatedAt := stored.UpdatedAt

	replay := clonePatchProposal(bound)
	replay.CreatedAt = time.Time{}
	replay.UpdatedAt = time.Time{}
	if err := s.BindPatchProposalPublicationEvidence(ctx, replay); err != nil {
		t.Fatalf("identical BindPatchProposalPublicationEvidence() replay error = %v", err)
	}
	if !replay.UpdatedAt.Equal(firstUpdatedAt) {
		t.Fatalf("identical replay updatedAt = %v, want unchanged %v", replay.UpdatedAt, firstUpdatedAt)
	}

	identicalUpdate := clonePatchProposal(&stored)
	identicalUpdate.PublicationEvidence = nil
	if err := s.UpdatePatchProposal(ctx, identicalUpdate); err != nil {
		t.Fatalf("identical UpdatePatchProposal() error = %v", err)
	}
	if !identicalUpdate.UpdatedAt.Equal(firstUpdatedAt) {
		t.Fatalf("identical generic update updatedAt = %v, want unchanged %v", identicalUpdate.UpdatedAt, firstUpdatedAt)
	}

	mutations := []struct {
		name   string
		mutate func(*store.PatchProposal)
	}{
		{name: "task name", mutate: func(p *store.PatchProposal) { p.TaskName = "other-task" }},
		{name: "branch", mutate: func(p *store.PatchProposal) { p.Branch = "orka/security/other" }},
		{name: "diff artifact", mutate: func(p *store.PatchProposal) { p.DiffArtifact = "other.diff" }},
		{name: "summary artifact", mutate: func(p *store.PatchProposal) { p.SummaryArtifact = "other.json" }},
		{name: "status", mutate: func(p *store.PatchProposal) { p.Status = publishPhaseFailed }},
		{name: "PR number", mutate: func(p *store.PatchProposal) { value := 43; p.PRNumber = &value }},
		{name: "PR URL", mutate: func(p *store.PatchProposal) { p.PRURL = "https://github.com/example/source/pull/43" }},
	}
	for _, tt := range mutations {
		t.Run(tt.name, func(t *testing.T) {
			candidate := clonePatchProposal(&stored)
			candidate.PublicationEvidence = nil
			tt.mutate(candidate)
			if err := s.UpdatePatchProposal(ctx, candidate); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("UpdatePatchProposal() error = %v, want conflict", err)
			}
			after := onlyPatchProposal(t, s, initial.Namespace, initial.FindingID)
			if !after.UpdatedAt.Equal(firstUpdatedAt) || !reflect.DeepEqual(after, stored) {
				t.Fatalf("bound proposal changed after rejected %s mutation: got %#v, want %#v", tt.name, after, stored)
			}
		})
	}

	conflictingBind := clonePatchProposal(bound)
	conflictingBind.PublicationEvidence.ArtifactDigest = "sha256:" + strings.Repeat("d", 64)
	if err := s.BindPatchProposalPublicationEvidence(ctx, conflictingBind); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("conflicting BindPatchProposalPublicationEvidence() error = %v, want conflict", err)
	}
	afterConflict := onlyPatchProposal(t, s, initial.Namespace, initial.FindingID)
	if !reflect.DeepEqual(afterConflict, stored) {
		t.Fatalf("bound proposal changed after conflicting bind: got %#v, want %#v", afterConflict, stored)
	}
}

func TestCreateScanRunAtomicallyRejectsConcurrentActiveIdempotency(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	for index, id := range []string{"scan-concurrent-a", "scan-concurrent-b"} {
		idempotencyKey := "scanidem:concurrent-a"
		if index == 1 {
			idempotencyKey = "scanidem:concurrent-b"
		}
		go func() {
			<-start
			results <- s.CreateScanRun(ctx, &store.ScanRun{
				ID:             id,
				Namespace:      "ns1",
				RepositoryScan: "repo1",
				TaskName:       id + "-task",
				Mode:           "manual",
				Phase:          "pending",
				IdempotencyKey: idempotencyKey,
				StartedAt:      time.Now(),
			})
		}()
	}
	close(start)

	successes := 0
	conflicts := 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, store.ErrConflict):
			conflicts++
		default:
			t.Fatalf("CreateScanRun() error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("CreateScanRun() successes/conflicts = %d/%d, want 1/1", successes, conflicts)
	}

	runs, _, err := s.ListScanRuns(ctx, "ns1", "repo1", 10, "")
	if err != nil {
		t.Fatalf("ListScanRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].IdempotencyKey == "" || runs[0].Phase != "pending" {
		t.Fatalf("runs = %#v, want one pending claimed run", runs)
	}

	runs[0].Phase = "failed"
	completedAt := time.Now()
	runs[0].CompletedAt = &completedAt
	if err := s.UpdateScanRun(ctx, &runs[0]); err != nil {
		t.Fatalf("UpdateScanRun() error = %v", err)
	}
	if err := s.CreateScanRun(ctx, &store.ScanRun{
		ID:             "scan-concurrent-retry",
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		TaskName:       "scan-concurrent-retry-task",
		Mode:           "manual",
		Phase:          "pending",
		IdempotencyKey: "scanidem:concurrent-retry",
		StartedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("CreateScanRun() after terminal run error = %v", err)
	}
}

func TestCreateScanRunAtomicallyRejectsConcurrentActiveAcrossConnections(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "security-scan-admission.db")
	dbA, err := NewDB(databasePath)
	if err != nil {
		t.Fatalf("NewDB(first) error = %v", err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	dbB, err := NewDB(databasePath)
	if err != nil {
		t.Fatalf("NewDB(second) error = %v", err)
	}
	t.Cleanup(func() { _ = dbB.Close() })
	stores := []*Store{NewStore(dbA, databasePath), NewStore(dbB, databasePath)}

	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, len(stores))
	for index := range stores {
		go func() {
			<-start
			results <- stores[index].CreateScanRun(ctx, &store.ScanRun{
				ID:             "scan-cross-connection-" + string(rune('a'+index)),
				Namespace:      "ns1",
				RepositoryScan: "repo1",
				TaskName:       "scan-cross-connection-task-" + string(rune('a'+index)),
				Mode:           "manual",
				Phase:          "pending",
				IdempotencyKey: "scanidem:cross-connection-" + string(rune('a'+index)),
				StartedAt:      time.Now(),
			})
		}()
	}
	close(start)

	successes := 0
	conflicts := 0
	for range stores {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, store.ErrConflict):
			conflicts++
		default:
			t.Fatalf("CreateScanRun() cross-connection error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("CreateScanRun() cross-connection successes/conflicts = %d/%d, want 1/1", successes, conflicts)
	}

	runs, _, err := stores[0].ListScanRuns(ctx, "ns1", "repo1", 10, "")
	if err != nil {
		t.Fatalf("ListScanRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].Phase != "pending" {
		t.Fatalf("runs = %#v, want one pending cross-connection claim", runs)
	}
}

func TestSaveThreatModelReplacesCurrentModel(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	first := &store.ThreatModel{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		Content:        "first threat model",
		Source:         "generated",
	}
	if err := s.SaveThreatModel(ctx, first); err != nil {
		t.Fatalf("SaveThreatModel(first): %v", err)
	}

	second := &store.ThreatModel{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		Content:        "updated threat model",
		Source:         "edited",
	}
	if err := s.SaveThreatModel(ctx, second); err != nil {
		t.Fatalf("SaveThreatModel(second): %v", err)
	}

	got, err := s.GetLatestThreatModel(ctx, "ns1", "repo1")
	if err != nil {
		t.Fatalf("GetLatestThreatModel: %v", err)
	}
	if got.Content != "updated threat model" {
		t.Fatalf("Content = %q, want %q", got.Content, "updated threat model")
	}
	if got.Source != "edited" {
		t.Fatalf("Source = %q, want %q", got.Source, "edited")
	}
	if got.Version != 2 {
		t.Fatalf("Version = %d, want 2", got.Version)
	}

	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM security_threat_models WHERE namespace = ? AND repository_scan = ?`,
		"ns1", "repo1",
	).Scan(&count); err != nil {
		t.Fatalf("count threat models: %v", err)
	}
	if count != 1 {
		t.Fatalf("threat model row count = %d, want 1", count)
	}
}

func TestSaveThreatModelRedactsContent(t *testing.T) {
	for _, source := range []string{"generated", "edited"} {
		t.Run(source, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			token := "ghp_" + strings.Repeat("a", 36)
			model := &store.ThreatModel{
				Namespace: "ns1", RepositoryScan: "repo1", Source: source,
				Content: "Notes for `config/auth.go:12`.\n\n\t" + token[:2] + "\u200b" + token[2:] + "\n\nRotate keys.",
			}
			const want = "Notes for `config/auth.go:12`.\n\n\t[REDACTED]\n\nRotate keys."
			if err := s.SaveThreatModel(ctx, model); err != nil {
				t.Fatalf("SaveThreatModel() error = %v", err)
			}
			if model.Content != want {
				t.Fatal("SaveThreatModel() left unsanitized content in the returned model")
			}
			var persisted string
			if err := s.db.QueryRowContext(ctx,
				`SELECT content FROM security_threat_models WHERE namespace = ? AND repository_scan = ?`,
				model.Namespace, model.RepositoryScan,
			).Scan(&persisted); err != nil {
				t.Fatalf("read stored threat model: %v", err)
			}
			if persisted != want {
				t.Fatal("SaveThreatModel() persisted unsanitized content")
			}
			got, err := s.GetLatestThreatModel(ctx, model.Namespace, model.RepositoryScan)
			if err != nil {
				t.Fatalf("GetLatestThreatModel() error = %v", err)
			}
			if got.Content != want || got.Source != source || got.Version != 1 {
				t.Fatal("GetLatestThreatModel() did not preserve the sanitized content and metadata")
			}
		})
	}
}

func TestSaveThreatModelCollapsesExistingVersions(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	createdAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	for version, content := range map[int]string{
		1: "older model",
		2: "newer model",
	} {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO security_threat_models
			 (namespace, repository_scan, version, content, source, generated_by_scan, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			"ns1", "repo1", version, content, "generated", "", createdAt, createdAt,
		); err != nil {
			t.Fatalf("seed threat model version %d: %v", version, err)
		}
	}

	current := &store.ThreatModel{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		Content:        "singleton threat model",
		Source:         "edited",
	}
	if err := s.SaveThreatModel(ctx, current); err != nil {
		t.Fatalf("SaveThreatModel(current): %v", err)
	}

	got, err := s.GetLatestThreatModel(ctx, "ns1", "repo1")
	if err != nil {
		t.Fatalf("GetLatestThreatModel: %v", err)
	}
	if got.Content != "singleton threat model" {
		t.Fatalf("Content = %q, want %q", got.Content, "singleton threat model")
	}
	if got.Version != 3 {
		t.Fatalf("Version = %d, want 3", got.Version)
	}

	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM security_threat_models WHERE namespace = ? AND repository_scan = ?`,
		"ns1", "repo1",
	).Scan(&count); err != nil {
		t.Fatalf("count threat models: %v", err)
	}
	if count != 1 {
		t.Fatalf("threat model row count = %d, want 1", count)
	}
}

func TestUpsertFindingPreservesMostAdvancedStateAndPRMetadata(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	prNumber := 123
	initial := &store.Finding{
		ID:               "fnd-123",
		Namespace:        "ns1",
		RepositoryScan:   "repo1",
		ScanRunID:        "scan-1",
		Fingerprint:      "repo1:file.go:unauthenticated-preview",
		Title:            "Preview disclosure",
		Summary:          "initial summary",
		Severity:         "medium",
		Confidence:       "high",
		ValidationStatus: "validated",
		State:            "pr_open",
		PatchProposalID:  "patch-123",
		PRNumber:         &prNumber,
		PRURL:            "https://github.com/example/repo/pull/123",
	}
	if err := s.UpsertFinding(ctx, initial); err != nil {
		t.Fatalf("UpsertFinding(initial): %v", err)
	}

	laterStage := &store.Finding{
		ID:               "fnd-123",
		Namespace:        "ns1",
		RepositoryScan:   "repo1",
		ScanRunID:        testScanRunID2,
		Fingerprint:      initial.Fingerprint,
		Title:            initial.Title,
		Summary:          "later summary",
		Severity:         initial.Severity,
		Confidence:       initial.Confidence,
		ValidationStatus: "pending",
		State:            "patch_ready",
	}
	if err := s.UpsertFinding(ctx, laterStage); err != nil {
		t.Fatalf("UpsertFinding(laterStage): %v", err)
	}

	got, err := s.GetFinding(ctx, "ns1", "fnd-123")
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got.State != "pr_open" {
		t.Fatalf("State = %q, want %q", got.State, "pr_open")
	}
	if got.ValidationStatus != "validated" {
		t.Fatalf("ValidationStatus = %q, want %q", got.ValidationStatus, "validated")
	}
	if got.PatchProposalID != "patch-123" {
		t.Fatalf("PatchProposalID = %q, want %q", got.PatchProposalID, "patch-123")
	}
	if got.PRNumber == nil || *got.PRNumber != prNumber {
		t.Fatalf("PRNumber = %#v, want %d", got.PRNumber, prNumber)
	}
	if got.PRURL != "https://github.com/example/repo/pull/123" {
		t.Fatalf("PRURL = %q, want preserved PR URL", got.PRURL)
	}
	if got.Summary != "later summary" {
		t.Fatalf("Summary = %q, want later summary to keep newer descriptive fields", got.Summary)
	}
}

func TestUpsertFindingAllowsPendingValidationToBecomeTerminal(t *testing.T) {
	for _, tc := range []struct {
		status         string
		validationJSON string
	}{
		{
			status:         "failed",
			validationJSON: `{"status":"failed","summary":"validation failed"}`,
		},
		{
			status:         "skipped",
			validationJSON: `{"status":"skipped","summary":"validation skipped"}`,
		},
	} {
		t.Run(tc.status, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()

			initial := &store.Finding{
				ID:               "fnd-" + tc.status,
				Namespace:        "ns1",
				RepositoryScan:   "repo1",
				ScanRunID:        "scan-1",
				Fingerprint:      "repo1:file.go:" + tc.status,
				Title:            "Finding",
				Summary:          "pending validation",
				Severity:         "high",
				Confidence:       "medium",
				ValidationStatus: "pending",
				State:            testStateOpen,
				ValidationJSON:   `{"status":"pending"}`,
			}
			if err := s.UpsertFinding(ctx, initial); err != nil {
				t.Fatalf("UpsertFinding(initial): %v", err)
			}

			terminal := *initial
			terminal.ScanRunID = testScanRunID2
			terminal.Summary = "terminal validation"
			terminal.ValidationStatus = tc.status
			terminal.ValidationJSON = tc.validationJSON
			if err := s.UpsertFinding(ctx, &terminal); err != nil {
				t.Fatalf("UpsertFinding(terminal): %v", err)
			}

			got, err := s.GetFinding(ctx, "ns1", initial.ID)
			if err != nil {
				t.Fatalf("GetFinding: %v", err)
			}
			if got.ValidationStatus != tc.status {
				t.Fatalf("ValidationStatus = %q, want %q", got.ValidationStatus, tc.status)
			}
			if got.ValidationJSON != tc.validationJSON {
				t.Fatalf("ValidationJSON = %q, want %q", got.ValidationJSON, tc.validationJSON)
			}
		})
	}
}

func TestUpsertFindingKeepsValidationJSONWhenValidatedStatusIsPreserved(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	initial := &store.Finding{
		ID:               "fnd-validated",
		Namespace:        "ns1",
		RepositoryScan:   "repo1",
		ScanRunID:        "scan-1",
		Fingerprint:      "repo1:file.go:validated",
		Title:            "Finding",
		Summary:          "validated",
		Severity:         "high",
		Confidence:       "medium",
		ValidationStatus: "validated",
		State:            testStateOpen,
		ValidationJSON:   `{"status":"validated","summary":"confirmed"}`,
	}
	if err := s.UpsertFinding(ctx, initial); err != nil {
		t.Fatalf("UpsertFinding(initial): %v", err)
	}

	lowerStatus := *initial
	lowerStatus.ScanRunID = testScanRunID2
	lowerStatus.ValidationStatus = "failed"
	lowerStatus.ValidationJSON = `{"status":"failed","summary":"later failure"}`
	if err := s.UpsertFinding(ctx, &lowerStatus); err != nil {
		t.Fatalf("UpsertFinding(lowerStatus): %v", err)
	}

	got, err := s.GetFinding(ctx, "ns1", initial.ID)
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got.ValidationStatus != "validated" {
		t.Fatalf("ValidationStatus = %q, want validated", got.ValidationStatus)
	}
	if got.ValidationJSON != initial.ValidationJSON {
		t.Fatalf("ValidationJSON = %q, want %q", got.ValidationJSON, initial.ValidationJSON)
	}
}

func TestUpsertFindingAllowsPatchPendingToReturnOpen(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	initial := &store.Finding{
		ID:               "fnd-patch-pending",
		Namespace:        "ns1",
		RepositoryScan:   "repo1",
		ScanRunID:        "scan-1",
		Fingerprint:      "repo1:file.go:patch-pending",
		Title:            "Finding",
		Summary:          "patch pending",
		Severity:         "high",
		Confidence:       "medium",
		ValidationStatus: "validated",
		State:            "patch_pending",
		PatchProposalID:  "patch-123",
	}
	if err := s.UpsertFinding(ctx, initial); err != nil {
		t.Fatalf("UpsertFinding(initial): %v", err)
	}

	open := *initial
	open.ScanRunID = testScanRunID2
	open.State = testStateOpen
	if err := s.UpsertFinding(ctx, &open); err != nil {
		t.Fatalf("UpsertFinding(open): %v", err)
	}

	got, err := s.GetFinding(ctx, "ns1", initial.ID)
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got.State != testStateOpen {
		t.Fatalf("State = %q, want open", got.State)
	}
}

func TestUpsertFindingPreservesFinalStatesOverOpen(t *testing.T) {
	for _, finalState := range []string{"fixed", "resolved", "dismissed", "suppressed", "false_positive"} {
		t.Run(finalState, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()

			initial := &store.Finding{
				ID:               "fnd-" + finalState,
				Namespace:        "ns1",
				RepositoryScan:   "repo1",
				ScanRunID:        "scan-1",
				Fingerprint:      "repo1:file.go:" + finalState,
				Title:            "Finding",
				Summary:          "final state",
				Severity:         "high",
				Confidence:       "medium",
				ValidationStatus: "validated",
				State:            finalState,
			}
			if err := s.UpsertFinding(ctx, initial); err != nil {
				t.Fatalf("UpsertFinding(initial): %v", err)
			}

			reopened := *initial
			reopened.ScanRunID = testScanRunID2
			reopened.State = testStateOpen
			if err := s.UpsertFinding(ctx, &reopened); err != nil {
				t.Fatalf("UpsertFinding(reopened): %v", err)
			}

			got, err := s.GetFinding(ctx, "ns1", initial.ID)
			if err != nil {
				t.Fatalf("GetFinding: %v", err)
			}
			if got.State != finalState {
				t.Fatalf("State = %q, want %q", got.State, finalState)
			}
		})
	}
}

func TestReviewSliceStoreRoundTripFilteringAndNamespaceIsolation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	slice := &store.ReviewSlice{
		ID:              "slice_repo1_api",
		Namespace:       "ns1",
		RepositoryScan:  "repo1",
		Source:          "deterministic-go-package",
		Title:           "Go package internal/api",
		Summary:         "API handlers",
		Kind:            "package",
		Confidence:      "high",
		Status:          "pending",
		LastScanRunID:   "scan-current",
		Entrypoints:     []store.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "handler"}},
		OwnedFiles:      []store.ReviewSliceFile{{Path: "internal/api/security.go", Reason: "source"}},
		ContextFiles:    []store.ReviewSliceFile{{Path: "internal/api/security_test.go", Reason: "tests"}},
		Tests:           []store.ReviewSliceTest{{Path: "internal/api/security_test.go", Command: "go test ./internal/api"}},
		Tags:            []string{"language:go"},
		TrustBoundaries: []string{"network"},
	}
	if err := s.UpsertReviewSlice(ctx, slice); err != nil {
		t.Fatalf("UpsertReviewSlice() error = %v", err)
	}

	got, err := s.GetReviewSlice(ctx, "ns1", "repo1", "slice_repo1_api")
	if err != nil {
		t.Fatalf("GetReviewSlice() error = %v", err)
	}
	if got.Title != slice.Title || len(got.OwnedFiles) != 1 || got.OwnedFiles[0].Path != "internal/api/security.go" {
		t.Fatalf("GetReviewSlice() = %#v, want JSON fields round-tripped", got)
	}

	if err := s.UpdateReviewSliceStatus(ctx, "ns1", "repo1", "slice_repo1_api", "scan-stale", "reviewed"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateReviewSliceStatus(stale run) error = %v, want not found", err)
	}
	if err := s.UpdateReviewSliceStatus(ctx, "ns1", "repo1", "slice_repo1_api", "scan-current", "reviewed"); err != nil {
		t.Fatalf("UpdateReviewSliceStatus() error = %v", err)
	}
	reviewed, _, err := s.ListReviewSlices(ctx, store.ReviewSliceFilter{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		Status:         "reviewed",
		LastScanRunID:  "scan-current",
	})
	if err != nil {
		t.Fatalf("ListReviewSlices(reviewed) error = %v", err)
	}
	if len(reviewed) != 1 || reviewed[0].LastReviewedAt == nil {
		t.Fatalf("reviewed slices = %#v, want reviewed slice with timestamp", reviewed)
	}
	staleRun, _, err := s.ListReviewSlices(ctx, store.ReviewSliceFilter{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		Status:         "reviewed",
		LastScanRunID:  "scan-stale",
	})
	if err != nil {
		t.Fatalf("ListReviewSlices(stale run) error = %v", err)
	}
	if len(staleRun) != 0 {
		t.Fatalf("ListReviewSlices(stale run) = %#v, want run isolation", staleRun)
	}

	otherNamespace, _, err := s.ListReviewSlices(ctx, store.ReviewSliceFilter{
		Namespace:      "ns2",
		RepositoryScan: "repo1",
	})
	if err != nil {
		t.Fatalf("ListReviewSlices(ns2) error = %v", err)
	}
	if len(otherNamespace) != 0 {
		t.Fatalf("ListReviewSlices(ns2) = %#v, want namespace isolation", otherNamespace)
	}
}

func TestDroppedFindingStoreRoundTripFiltering(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	first := &store.DroppedFinding{
		ID:             "drop1",
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		ScanRunID:      "scan1",
		TaskName:       "task1",
		SliceID:        "slice1",
		Reason:         "evidence file was not included in review context",
		SampleJSON:     `{"title":"bad"}`,
	}
	if err := s.CreateDroppedFinding(ctx, first); err != nil {
		t.Fatalf("CreateDroppedFinding(first) error = %v", err)
	}
	if err := s.CreateDroppedFinding(ctx, &store.DroppedFinding{
		ID:             "drop2",
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		ScanRunID:      "scan2",
		TaskName:       "task2",
		Reason:         "missing evidence",
	}); err != nil {
		t.Fatalf("CreateDroppedFinding(second) error = %v", err)
	}

	got, _, err := s.ListDroppedFindings(ctx, store.DroppedFindingFilter{
		Namespace:      "ns1",
		RepositoryScan: "repo1",
		ScanRunID:      "scan1",
	})
	if err != nil {
		t.Fatalf("ListDroppedFindings() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "drop1" || got[0].SampleJSON != first.SampleJSON {
		t.Fatalf("ListDroppedFindings() = %#v, want scan1 diagnostic", got)
	}
}

func TestFailedValidationExcludesFindingFromRecommendedFilter(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	items := []*store.Finding{
		{ID: "f_failed", Namespace: "ns1", RepositoryScan: "repo1", ScanRunID: "scan1", Fingerprint: "fp-failed", Title: "failed", Summary: "failed", Severity: "critical", Confidence: "high", ValidationStatus: "failed", State: "open"},
		{ID: "f_open", Namespace: "ns1", RepositoryScan: "repo1", ScanRunID: "scan1", Fingerprint: "fp-open", Title: "open", Summary: "open", Severity: "high", Confidence: "high", ValidationStatus: "unvalidated", State: "open"},
	}
	for _, item := range items {
		if err := s.UpsertFinding(ctx, item); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", item.ID, err)
		}
	}
	got, _, err := s.ListFindings(ctx, store.FindingFilter{Namespace: "ns1", RepositoryScan: "repo1", Recommended: true})
	if err != nil {
		t.Fatalf("ListFindings(recommended) error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "f_open" {
		t.Fatalf("recommended findings = %#v, want only unvalidated open finding", got)
	}
}

func TestValidatedFindingsRankHigher(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	items := []*store.Finding{
		{ID: "f_unvalidated", Namespace: "ns1", RepositoryScan: "repo1", ScanRunID: "scan1", Fingerprint: "fp-unvalidated", Title: "unvalidated", Summary: "unvalidated", Severity: "high", Confidence: "high", ValidationStatus: "unvalidated", State: "open"},
		{ID: "f_validated", Namespace: "ns1", RepositoryScan: "repo1", ScanRunID: "scan1", Fingerprint: "fp-validated", Title: "validated", Summary: "validated", Severity: "high", Confidence: "medium", ValidationStatus: "validated", State: "open"},
	}
	for _, item := range items {
		if err := s.UpsertFinding(ctx, item); err != nil {
			t.Fatalf("UpsertFinding(%s) error = %v", item.ID, err)
		}
	}
	got, _, err := s.ListFindings(ctx, store.FindingFilter{Namespace: "ns1", RepositoryScan: "repo1", Recommended: true})
	if err != nil {
		t.Fatalf("ListFindings(recommended) error = %v", err)
	}
	if len(got) != 2 || got[0].ID != "f_validated" {
		t.Fatalf("recommended findings order = %#v, want validated first", got)
	}
}
