package sqlite

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orka-agents/orka/internal/store"
)

func TestScanTaskIngestionRollsBackAndRetries(t *testing.T) {
	for _, failure := range []struct{ name, trigger string }{
		{"finding", "BEFORE INSERT ON security_findings WHEN NEW.id = 'finding-2'"},
		{"counters", "BEFORE UPDATE ON security_scan_runs"},
		{"receipt", "BEFORE INSERT ON security_scan_task_ingestions"},
	} {
		t.Run(failure.name, func(t *testing.T) {
			ctx := context.Background()
			s := setupTestStore(t)
			identity := store.ScanTaskIdentity{Namespace: "ns", RepositoryScan: "repo", ScanRunID: "run", TaskName: "review", TaskUID: "task-uid", Stage: "review", SliceID: "slice"}
			run := &store.ScanRun{ID: identity.ScanRunID, Namespace: identity.Namespace, RepositoryScan: identity.RepositoryScan, Phase: "running"}
			require.NoError(t, s.CreateScanRun(ctx, run))
			slice := &store.ReviewSlice{Namespace: "ns", RepositoryScan: "repo", ID: "slice", LastScanRunID: "run", Status: "pending"}
			require.NoError(t, s.UpsertReviewSlice(ctx, slice))
			_, err := s.db.Exec("CREATE TRIGGER fail_ingestion " + failure.trigger + " BEGIN SELECT RAISE(ABORT, 'injected ingestion failure'); END")
			require.NoError(t, err)
			apply := func(tx store.SecurityStore, current *store.ScanRun) error {
				if err := tx.SaveThreatModel(ctx, &store.ThreatModel{Namespace: "ns", RepositoryScan: "repo", Content: "# Model", GeneratedByScan: "run"}); err != nil {
					return err
				}
				if err := tx.UpdateReviewSliceStatus(ctx, "ns", "repo", "slice", "run", "reviewed"); err != nil {
					return err
				}
				if err := tx.CreateDroppedFinding(ctx, &store.DroppedFinding{ID: "drop", Namespace: "ns", RepositoryScan: "repo", ScanRunID: "run", TaskName: "review", Reason: "missing evidence"}); err != nil {
					return err
				}
				for i := 1; i <= 2; i++ {
					id := fmt.Sprintf("finding-%d", i)
					if err := tx.UpsertObservedFinding(ctx, &store.Finding{ID: id, Namespace: "ns", RepositoryScan: "repo", ScanRunID: "run", Fingerprint: id}); err != nil {
						return err
					}
				}
				if err := tx.MarkFindingDuplicate(ctx, "ns", "finding-2", "finding-1"); err != nil {
					return err
				}
				current.ReviewedSliceCount++
				current.AcceptedFindings += 2
				current.DroppedFindings++
				return nil
			}
			applied, err := s.ApplyScanTaskIngestion(ctx, &store.ScanTaskIngestion{ScanTaskIdentity: identity}, apply)
			require.ErrorContains(t, err, "injected ingestion failure")
			require.False(t, applied)
			gotRun, err := s.GetScanRun(ctx, "ns", "run")
			require.NoError(t, err)
			require.Equal(t, run, gotRun)
			gotSlice, err := s.GetReviewSlice(ctx, "ns", "repo", "slice")
			require.NoError(t, err)
			require.Equal(t, slice, gotSlice)
			findings, _, err := s.ListFindings(ctx, store.FindingFilter{Namespace: "ns", RepositoryScan: "repo", IncludeDuplicates: true})
			require.NoError(t, err)
			require.Empty(t, findings)
			dropped, _, err := s.ListDroppedFindings(ctx, store.DroppedFindingFilter{Namespace: "ns", ScanRunID: "run"})
			require.NoError(t, err)
			require.Empty(t, dropped)
			_, err = s.GetLatestThreatModel(ctx, "ns", "repo")
			require.ErrorIs(t, err, store.ErrNotFound)
			_, err = s.GetScanTaskIngestion(ctx, identity)
			require.ErrorIs(t, err, store.ErrNotFound)

			_, err = s.db.Exec("DROP TRIGGER fail_ingestion")
			require.NoError(t, err)
			ingestion := &store.ScanTaskIngestion{ScanTaskIdentity: identity, FindingIDs: []string{"finding-1"}, DroppedFindingsJSON: `{"schemaVersion":1,"dropped":[]}`}
			applied, err = s.ApplyScanTaskIngestion(ctx, ingestion, apply)
			require.NoError(t, err)
			require.True(t, applied)
			gotRun, err = s.GetScanRun(ctx, "ns", "run")
			require.NoError(t, err)
			require.Equal(t, 1, gotRun.ReviewedSliceCount)
			require.Equal(t, 2, gotRun.AcceptedFindings)
			require.Equal(t, 1, gotRun.DroppedFindings)
			gotReceipt, err := s.GetScanTaskIngestion(ctx, identity)
			require.NoError(t, err)
			require.Equal(t, ingestion, gotReceipt)
			model, err := s.GetLatestThreatModel(ctx, "ns", "repo")
			require.NoError(t, err)
			require.EqualValues(t, 1, model.Version)
			alias, err := s.GetFinding(ctx, "ns", "finding-2")
			require.NoError(t, err)
			require.Equal(t, "finding-1", alias.DuplicateOf)

			applied, err = s.ApplyScanTaskIngestion(ctx, ingestion, func(store.SecurityStore, *store.ScanRun) error {
				t.Fatal("replay invoked result ingestion")
				return nil
			})
			require.NoError(t, err)
			require.False(t, applied)
			require.NoError(t, s.CompleteScanTaskIngestion(ctx, identity))
			gotReceipt, err = s.GetScanTaskIngestion(ctx, identity)
			require.NoError(t, err)
			require.True(t, gotReceipt.Completed)
			require.Equal(t, ingestion.IngestedAt, gotReceipt.IngestedAt)
		})
	}
}

func TestScanTaskIngestionFencesRunAndTaskIdentity(t *testing.T) {
	ctx := context.Background()
	s := setupTestStore(t)
	identity := store.ScanTaskIdentity{Namespace: "ns", RepositoryScan: "repo", ScanRunID: "run", TaskName: "mapper", TaskUID: "uid-1", Stage: "mapper"}
	run := &store.ScanRun{ID: "run", Namespace: "ns", RepositoryScan: "repo", Phase: "running"}
	require.NoError(t, s.CreateScanRun(ctx, run))
	apply := func(_ store.SecurityStore, run *store.ScanRun) error { run.SliceCount++; return nil }
	applied, err := s.ApplyScanTaskIngestion(ctx, &store.ScanTaskIngestion{ScanTaskIdentity: identity}, apply)
	require.NoError(t, err)
	require.True(t, applied)
	changed := identity
	changed.Stage = "review"
	_, err = s.GetScanTaskIngestion(ctx, changed)
	assertIngestionConflict := func(task store.ScanTaskIdentity) {
		t.Helper()
		applied, err := s.ApplyScanTaskIngestion(ctx, &store.ScanTaskIngestion{ScanTaskIdentity: task}, apply)
		require.ErrorIs(t, err, store.ErrConflict)
		require.False(t, applied)
	}
	require.ErrorIs(t, err, store.ErrConflict)
	assertIngestionConflict(changed)
	changed = identity
	changed.TaskUID = "uid-2"
	_, err = s.GetScanTaskIngestion(ctx, changed)
	require.ErrorIs(t, err, store.ErrNotFound, "Task name reuse must not reuse the old receipt")
	changed.RepositoryScan = "another-repo"
	assertIngestionConflict(changed)
	for _, phase := range []string{"succeeded", "failed"} {
		run.Phase = phase
		require.NoError(t, s.UpdateScanRun(ctx, run))
		identity.TaskUID = "unseen-task"
		applied, err := s.ApplyScanTaskIngestion(ctx, &store.ScanTaskIngestion{ScanTaskIdentity: identity}, func(store.SecurityStore, *store.ScanRun) error {
			t.Fatal("terminal run invoked result ingestion")
			return nil
		})
		require.NoError(t, err)
		require.False(t, applied)
	}
}

func TestScanTaskIngestionConcurrentReplay(t *testing.T) {
	ctx := context.Background()
	s := setupTestStore(t)
	identity := store.ScanTaskIdentity{Namespace: "ns", RepositoryScan: "repo", ScanRunID: "run", TaskName: "review", TaskUID: "uid", Stage: "review"}
	require.NoError(t, s.CreateScanRun(ctx, &store.ScanRun{ID: "run", Namespace: "ns", RepositoryScan: "repo", Phase: "running"}))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, err := s.ApplyScanTaskIngestion(ctx, &store.ScanTaskIngestion{ScanTaskIdentity: identity}, func(_ store.SecurityStore, run *store.ScanRun) error {
				run.ReviewedSliceCount++
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	run, err := s.GetScanRun(ctx, "ns", "run")
	require.NoError(t, err)
	require.Equal(t, 1, run.ReviewedSliceCount)
}
