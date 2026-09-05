package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orka-agents/orka/internal/store"
)

func TestScanRunIdentityIsImmutable(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	run := &store.ScanRun{
		ID: "scan_bound", Namespace: "ns", RepositoryScan: "repo", Phase: "pending",
		RepositoryScanUID: "repo-uid", RepositoryScanGeneration: 3,
	}
	require.NoError(t, s.CreateScanRun(ctx, run))
	before, err := s.GetScanRun(ctx, run.Namespace, run.ID)
	require.NoError(t, err)
	require.Equal(t, run.RepositoryScanUID, before.RepositoryScanUID)
	require.Equal(t, run.RepositoryScanGeneration, before.RepositoryScanGeneration)
	for _, change := range []struct {
		name   string
		mutate func(*store.ScanRun)
	}{
		{"UID", func(r *store.ScanRun) { r.RepositoryScanUID = "recreated-uid" }},
		{"generation", func(r *store.ScanRun) { r.RepositoryScanGeneration++ }},
		{"name", func(r *store.ScanRun) { r.RepositoryScan = "other-repo" }},
		{"clear binding", func(r *store.ScanRun) { r.RepositoryScanUID = ""; r.RepositoryScanGeneration = 0 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			candidate := *before
			change.mutate(&candidate)
			candidate.Summary = "must not be written"
			require.ErrorIs(t, s.UpdateScanRun(ctx, &candidate), store.ErrConflict)
			after, err := s.GetScanRun(ctx, run.Namespace, run.ID)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
	run.Phase = "running"
	run.StartedAt = time.Now().Add(time.Hour)
	require.NoError(t, s.UpdateScanRun(ctx, run))
	runs, _, err := s.ListScanRuns(ctx, run.Namespace, run.RepositoryScan, 10, "")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, before.StartedAt, runs[0].StartedAt)
	require.Equal(t, before.RepositoryScanUID, runs[0].RepositoryScanUID)
	require.Equal(t, before.RepositoryScanGeneration, runs[0].RepositoryScanGeneration)
}

func TestScanRunLateUpdateCannotReactivateAfterNewerRunFinishes(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	old := &store.ScanRun{ID: "scan_old", Namespace: "ns", RepositoryScan: "repo", Phase: "running"}
	require.NoError(t, s.CreateScanRun(ctx, old))
	late := *old
	old.Phase = "failed"
	require.NoError(t, s.UpdateScanRun(ctx, old))
	newer := &store.ScanRun{ID: "scan_new", Namespace: "ns", RepositoryScan: "repo", Phase: "pending"}
	require.NoError(t, s.CreateScanRun(ctx, newer))
	newer.Phase = "succeeded"
	require.NoError(t, s.UpdateScanRun(ctx, newer))
	require.ErrorIs(t, s.UpdateScanRun(ctx, &late), store.ErrConflict)
	after, err := s.GetScanRun(ctx, old.Namespace, old.ID)
	require.NoError(t, err)
	require.Equal(t, "failed", after.Phase)
}

func TestScanRunLegacyMigrationDoesNotInventIdentity(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	legacy := &store.ScanRun{ID: "scan_legacy", Namespace: "ns", RepositoryScan: "repo", Phase: "succeeded"}
	require.NoError(t, s.CreateScanRun(ctx, legacy))
	_, err := s.db.Exec(`ALTER TABLE security_scan_runs DROP COLUMN repository_scan_uid`)
	require.NoError(t, err)
	_, err = s.db.Exec(`ALTER TABLE security_scan_runs DROP COLUMN repository_scan_generation`)
	require.NoError(t, err)
	require.NoError(t, migrate(s.db))
	after, err := s.GetScanRun(ctx, legacy.Namespace, legacy.ID)
	require.NoError(t, err)
	require.Empty(t, after.RepositoryScanUID)
	require.Zero(t, after.RepositoryScanGeneration)
	require.Equal(t, "succeeded", after.Phase)
	after.RepositoryScanUID, after.RepositoryScanGeneration = "current-uid", 1
	require.ErrorIs(t, s.UpdateScanRun(ctx, after), store.ErrConflict)
}

func TestListScanRunsUsesAdmissionOrderDespiteClockSkew(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	for index, start := range []time.Time{time.Now().Add(time.Hour), time.Now().Add(-time.Hour)} {
		require.NoError(t, s.CreateScanRun(ctx, &store.ScanRun{
			ID: []string{"scan_old", "scan_new"}[index], Namespace: "ns", RepositoryScan: "repo", Phase: "succeeded", StartedAt: start,
		}))
	}
	runs, _, err := s.ListScanRuns(ctx, "ns", "repo", 1, "")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "scan_new", runs[0].ID)
}
