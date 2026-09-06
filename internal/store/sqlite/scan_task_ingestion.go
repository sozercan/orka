package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

type securityDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) securityDB() securityDB {
	if s.securityTx != nil {
		return s.securityTx
	}
	return s.db
}

func (s *Store) withSecurityTransaction(ctx context.Context, apply func(*Store) error) error {
	if s.securityTx != nil {
		return apply(s)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := apply(&Store{db: s.db, securityTx: tx}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) GetScanTaskIngestion(ctx context.Context, task store.ScanTaskIdentity) (*store.ScanTaskIngestion, error) {
	ingestion := &store.ScanTaskIngestion{ScanTaskIdentity: task}
	var findingIDs string
	err := s.securityDB().QueryRowContext(ctx, `SELECT repository_scan, stage, slice_id,
		finding_ids_json, dropped_findings_json, completed, ingested_at
		FROM security_scan_task_ingestions
		WHERE namespace = ? AND scan_run_id = ? AND task_name = ? AND task_uid = ?`,
		task.Namespace, task.ScanRunID, task.TaskName, task.TaskUID,
	).Scan(&ingestion.RepositoryScan, &ingestion.Stage, &ingestion.SliceID,
		&findingIDs, &ingestion.DroppedFindingsJSON, &ingestion.Completed, &ingestion.IngestedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if ingestion.ScanTaskIdentity != task {
		return nil, fmt.Errorf("%w: scan Task ingestion identity changed", store.ErrConflict)
	}
	if err := unmarshalSecurityJSON(findingIDs, &ingestion.FindingIDs); err != nil {
		return nil, err
	}
	return ingestion, nil
}

func (s *Store) ApplyScanTaskIngestion(ctx context.Context, ingestion *store.ScanTaskIngestion, apply func(store.SecurityStore, *store.ScanRun) error) (bool, error) {
	if ingestion == nil || ingestion.Namespace == "" || ingestion.RepositoryScan == "" ||
		ingestion.ScanRunID == "" || ingestion.TaskName == "" || ingestion.Stage == "" || apply == nil {
		return false, store.ValidationErrorf("scan Task ingestion requires a run, Task identity, and apply callback")
	}
	applied := false
	err := s.withSecurityTransaction(ctx, func(tx *Store) error {
		if _, err := tx.GetScanTaskIngestion(ctx, ingestion.ScanTaskIdentity); err == nil {
			return nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		run, err := tx.GetScanRun(ctx, ingestion.Namespace, ingestion.ScanRunID)
		if err != nil {
			return err
		}
		if run.RepositoryScan != ingestion.RepositoryScan {
			return fmt.Errorf("%w: scan Task belongs to a different RepositoryScan", store.ErrConflict)
		}
		if run.Phase == "succeeded" || run.Phase == "failed" {
			return nil
		}
		if err := apply(tx, run); err != nil {
			return err
		}
		if err := tx.UpdateScanRun(ctx, run); err != nil {
			return err
		}
		findingIDs, err := marshalSecurityJSON(ingestion.FindingIDs)
		if err != nil {
			return err
		}
		ingestion.IngestedAt = time.Now().UTC()
		if _, err := tx.securityDB().ExecContext(ctx, `INSERT INTO security_scan_task_ingestions
			(namespace, repository_scan, scan_run_id, task_name, task_uid, stage, slice_id,
			 finding_ids_json, dropped_findings_json, completed, ingested_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ingestion.Namespace, ingestion.RepositoryScan, ingestion.ScanRunID, ingestion.TaskName,
			ingestion.TaskUID, ingestion.Stage, ingestion.SliceID, findingIDs,
			ingestion.DroppedFindingsJSON, ingestion.Completed, ingestion.IngestedAt); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied && err == nil, err
}

func (s *Store) CompleteScanTaskIngestion(ctx context.Context, task store.ScanTaskIdentity) error {
	result, err := s.securityDB().ExecContext(ctx, `UPDATE security_scan_task_ingestions SET completed = TRUE
		WHERE namespace = ? AND scan_run_id = ? AND task_name = ? AND task_uid = ? AND repository_scan = ? AND stage = ? AND slice_id = ?`,
		task.Namespace, task.ScanRunID, task.TaskName, task.TaskUID, task.RepositoryScan, task.Stage, task.SliceID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return store.ErrNotFound
	}
	return nil
}
