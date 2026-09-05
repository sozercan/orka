package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	_ "modernc.org/sqlite" // SQLite driver registration
)

var (
	dbSizeBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "orka_store_db_size_bytes",
			Help: "Size of the SQLite database file in bytes",
		},
	)
)

func init() {
	metrics.Registry.MustRegister(dbSizeBytes)
}

// NewDB opens a SQLite database with recommended pragmas for WAL mode,
// busy timeout, synchronous mode, and foreign keys.
func NewDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// Single writer — SQLite only supports one concurrent writer.
	// WAL mode allows concurrent reads alongside the single writer.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // keep connection alive

	// Set pragmas per-connection. These are not persistent in SQLite and
	// must be set on each connection. With MaxOpenConns(1), we have exactly one.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close() //nolint:errcheck
			return nil, fmt.Errorf("failed to set pragma %q: %w", p, err)
		}
	}

	if err := migrate(db); err != nil {
		db.Close() //nolint:errcheck
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return db, nil
}

//nolint:gocyclo // schema migrations intentionally keep ordered, fail-fast upgrade steps in one transaction boundary
func migrate(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS results (
			namespace  TEXT NOT NULL,
			task_name  TEXT NOT NULL,
			data       BLOB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, task_name)
		)`,
		`CREATE TABLE IF NOT EXISTS prompt_result_receipts (
			attempt_id       TEXT NOT NULL PRIMARY KEY,
			namespace        TEXT NOT NULL,
			task_name        TEXT NOT NULL,
			operation_id     TEXT NOT NULL,
			operation_digest TEXT NOT NULL,
			data             BLOB NOT NULL,
			created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_prompt_result_receipts_task
			ON prompt_result_receipts(namespace, task_name)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			namespace     TEXT NOT NULL,
			name          TEXT NOT NULL,
			session_type  TEXT NOT NULL DEFAULT 'task',
			owner_type    TEXT NOT NULL DEFAULT '',
			owner_ref     TEXT NOT NULL DEFAULT '',
			active_task            TEXT NOT NULL DEFAULT '',
			active_task_uid        TEXT NOT NULL DEFAULT '',
			active_task_expires_at TIMESTAMP,
			control_session_uid    TEXT NOT NULL DEFAULT '',
			message_count   INTEGER NOT NULL DEFAULT 0,
			input_tokens  INTEGER NOT NULL DEFAULT 0,
			output_tokens INTEGER NOT NULL DEFAULT 0,
			cancelled     BOOLEAN NOT NULL DEFAULT FALSE,
			created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, name)
		)`,
		`CREATE TABLE IF NOT EXISTS session_cleanup_intents (
			namespace        TEXT NOT NULL,
			session_name     TEXT NOT NULL,
			operation_id     TEXT NOT NULL,
			operation_digest TEXT NOT NULL,
			plan             BLOB NOT NULL,
			created_at       TIMESTAMP NOT NULL,
			PRIMARY KEY(namespace, session_name)
		)`,
		`CREATE TABLE IF NOT EXISTS session_cleanup_completions (
			namespace        TEXT NOT NULL,
			session_name     TEXT NOT NULL,
			session_uid      TEXT NOT NULL DEFAULT '',
			operation_id     TEXT NOT NULL,
			operation_digest TEXT NOT NULL,
			completed_at     TIMESTAMP NOT NULL,
			PRIMARY KEY(namespace, session_name)
		)`,
		`CREATE TABLE IF NOT EXISTS session_turn_cleanup_receipts (
			turn_id           TEXT PRIMARY KEY,
			prompt_attempt_id TEXT NOT NULL UNIQUE,
			namespace         TEXT NOT NULL,
			session_name      TEXT NOT NULL,
			session_uid       TEXT NOT NULL,
			receipt_digest    TEXT NOT NULL,
			receipt           BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS session_messages (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace    TEXT NOT NULL,
			session_name TEXT NOT NULL,
			message_id   TEXT NOT NULL DEFAULT '',
			sort_order   INTEGER NOT NULL DEFAULT 0,
			role         TEXT NOT NULL,
			content      TEXT NOT NULL DEFAULT '',
			name         TEXT,
			input        TEXT,
			tool_calls   TEXT,
			tool_call_id TEXT,
			source_type  TEXT NOT NULL DEFAULT '',
			source_ref   TEXT NOT NULL DEFAULT '',
			metadata_json TEXT NOT NULL DEFAULT '{}',
			created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (namespace, session_name) REFERENCES sessions(namespace, name) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS runtime_sessions (
			id              TEXT NOT NULL,
			namespace       TEXT NOT NULL,
			session_name    TEXT NOT NULL,
			active_task     TEXT NOT NULL DEFAULT '',
			agent_name      TEXT NOT NULL DEFAULT '',
			provider        TEXT NOT NULL,
			state           TEXT NOT NULL,
			cleanup_policy  TEXT NOT NULL,
			idle_timeout_ns INTEGER NOT NULL DEFAULT 0,
			max_lifetime_ns INTEGER NOT NULL DEFAULT 0,
			created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_runtime_sessions_namespace_updated
			ON runtime_sessions(namespace, updated_at DESC, id ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_runtime_sessions_owner
			ON runtime_sessions(namespace, session_name, provider, state, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_runtime_sessions_active_task
			ON runtime_sessions(namespace, active_task, updated_at DESC)
			WHERE active_task <> ''`,
		`CREATE INDEX IF NOT EXISTS idx_runtime_sessions_cleanup
			ON runtime_sessions(namespace, state, cleanup_policy, updated_at ASC)`,
		`CREATE TABLE IF NOT EXISTS plan_states (
			namespace     TEXT NOT NULL,
			task_name     TEXT NOT NULL,
			iteration     INTEGER NOT NULL DEFAULT 0,
			summary       TEXT NOT NULL DEFAULT '',
			progress_pct  INTEGER NOT NULL DEFAULT 0,
			goal_complete BOOLEAN NOT NULL DEFAULT FALSE,
			plan_document TEXT NOT NULL DEFAULT '',
			created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, task_name)
		)`,
		`CREATE TABLE IF NOT EXISTS execution_events (
			id              TEXT PRIMARY KEY,
			namespace       TEXT NOT NULL,
			stream_type     TEXT NOT NULL,
			stream_id       TEXT NOT NULL,
			seq             INTEGER NOT NULL,
			session_seq     INTEGER NOT NULL DEFAULT 0,
			dedupe_key      TEXT NOT NULL DEFAULT '',
			type            TEXT NOT NULL,
			severity        TEXT NOT NULL DEFAULT 'info',
			task_name       TEXT NOT NULL DEFAULT '',
			session_name    TEXT NOT NULL DEFAULT '',
			agent_name      TEXT NOT NULL DEFAULT '',
			tool_name       TEXT NOT NULL DEFAULT '',
			tool_call_id    TEXT NOT NULL DEFAULT '',
			summary         TEXT NOT NULL DEFAULT '',
			content_json    TEXT,
			content_text    TEXT NOT NULL DEFAULT '',
			truncation_json TEXT,
			created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(namespace, stream_type, stream_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_session_messages_order ON session_messages(namespace, session_name, id)`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_namespace ON sessions(namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_results_namespace ON results(namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_plan_states_namespace ON plan_states(namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_execution_events_stream_seq
			ON execution_events(namespace, stream_type, stream_id, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_execution_events_type
			ON execution_events(namespace, stream_type, stream_id, type, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_execution_events_task
			ON execution_events(namespace, task_name, seq)`,
		`CREATE INDEX IF NOT EXISTS idx_execution_events_session
			ON execution_events(namespace, session_name, seq)`,
		`CREATE TABLE IF NOT EXISTS execution_event_session_sequences (
			namespace    TEXT NOT NULL,
			session_name TEXT NOT NULL,
			latest_seq   INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (namespace, session_name)
		)`,
		`CREATE TABLE IF NOT EXISTS memories (
			id                 TEXT PRIMARY KEY,
			namespace          TEXT NOT NULL,
			session_name       TEXT NOT NULL DEFAULT '',
			agent_name         TEXT NOT NULL DEFAULT '',
			task_name          TEXT NOT NULL DEFAULT '',
			parent_task        TEXT NOT NULL DEFAULT '',
			source             TEXT NOT NULL DEFAULT '',
			source_proposal_id TEXT NOT NULL DEFAULT '',
			content            TEXT NOT NULL,
			tags_json        TEXT NOT NULL DEFAULT '[]',
			disabled         BOOLEAN NOT NULL DEFAULT FALSE,
			deleted          BOOLEAN NOT NULL DEFAULT FALSE,
			created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_recalled_at TIMESTAMP,
			recalled_count   INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_memories_recall
			ON memories(namespace, deleted, disabled, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_memories_task ON memories(namespace, task_name)`,
		`CREATE INDEX IF NOT EXISTS idx_memories_agent ON memories(namespace, agent_name)`,
		`CREATE TABLE IF NOT EXISTS memory_proposals (
			id          TEXT PRIMARY KEY,
			namespace   TEXT NOT NULL,
			task_name   TEXT NOT NULL DEFAULT '',
			agent_name  TEXT NOT NULL DEFAULT '',
			type        TEXT NOT NULL,
			skill_name  TEXT NOT NULL DEFAULT '',
			title       TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			content     TEXT NOT NULL DEFAULT '',
			patch       TEXT NOT NULL DEFAULT '',
			status            TEXT NOT NULL DEFAULT 'pending',
			reviewer          TEXT NOT NULL DEFAULT '',
			review_note       TEXT NOT NULL DEFAULT '',
			applied_memory_id TEXT NOT NULL DEFAULT '',
			applied_by        TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			reviewed_at       TIMESTAMP,
			applied_at        TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_memory_proposals_status
			ON memory_proposals(namespace, status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_memory_proposals_task
			ON memory_proposals(namespace, task_name, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace   TEXT NOT NULL,
			from_task   TEXT NOT NULL,
			to_task     TEXT NOT NULL,
			parent_task TEXT NOT NULL,
			content     TEXT NOT NULL,
			read        BOOLEAN NOT NULL DEFAULT FALSE,
			created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(namespace, to_task, read)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(namespace, parent_task)`,
		`CREATE TABLE IF NOT EXISTS artifacts (
			namespace    TEXT NOT NULL,
			task_name    TEXT NOT NULL,
			filename     TEXT NOT NULL,
			content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
			size         INTEGER NOT NULL,
			data         BLOB NOT NULL,
			created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, task_name, filename)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(namespace, task_name)`,
		`CREATE TABLE IF NOT EXISTS security_scan_runs (
			id              TEXT PRIMARY KEY,
			namespace       TEXT NOT NULL,
			repository_scan TEXT NOT NULL,
			task_name       TEXT NOT NULL,
			mode            TEXT NOT NULL,
			phase           TEXT NOT NULL,
			base_commit     TEXT NOT NULL DEFAULT '',
			head_commit     TEXT NOT NULL DEFAULT '',
			commit_count    INTEGER NOT NULL DEFAULT 0,
			slice_count     INTEGER NOT NULL DEFAULT 0,
			reviewed_slice_count INTEGER NOT NULL DEFAULT 0,
			skipped_slice_count INTEGER NOT NULL DEFAULT 0,
			accepted_findings INTEGER NOT NULL DEFAULT 0,
			dropped_findings INTEGER NOT NULL DEFAULT 0,
			summary         TEXT NOT NULL DEFAULT '',
			error_message   TEXT NOT NULL DEFAULT '',
			started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at    TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_scan_runs_repo
			ON security_scan_runs(namespace, repository_scan, started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS security_threat_models (
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			version           INTEGER NOT NULL,
			content           TEXT NOT NULL,
			source            TEXT NOT NULL,
			generated_by_scan TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, repository_scan, version)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_threat_models_latest
			ON security_threat_models(namespace, repository_scan, version DESC)`,
		`CREATE TABLE IF NOT EXISTS security_findings (
			id                TEXT PRIMARY KEY,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			scan_run_id       TEXT NOT NULL,
			slice_id          TEXT NOT NULL DEFAULT '',
			fingerprint       TEXT NOT NULL,
			title             TEXT NOT NULL,
			category          TEXT NOT NULL DEFAULT '',
			summary           TEXT NOT NULL,
			severity          TEXT NOT NULL,
			confidence        TEXT NOT NULL,
			triage            TEXT NOT NULL DEFAULT '',
			validation_status TEXT NOT NULL,
			state             TEXT NOT NULL,
			file_path         TEXT NOT NULL DEFAULT '',
			line              INTEGER NOT NULL DEFAULT 0,
			commit_sha        TEXT NOT NULL DEFAULT '',
			root_cause        TEXT NOT NULL DEFAULT '',
			reproduction      TEXT NOT NULL DEFAULT '',
			remediation       TEXT NOT NULL DEFAULT '',
			suggested_action  TEXT NOT NULL DEFAULT '',
			why_tests_do_not_cover TEXT NOT NULL DEFAULT '',
			suggested_regression_test TEXT NOT NULL DEFAULT '',
			minimum_fix_scope TEXT NOT NULL DEFAULT '',
			evidence_json     TEXT NOT NULL DEFAULT '',
			validation_json   TEXT NOT NULL DEFAULT '',
			patch_proposal_id TEXT NOT NULL DEFAULT '',
			pr_number         INTEGER,
			pr_url            TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(namespace, repository_scan, fingerprint)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_findings_repo
			ON security_findings(namespace, repository_scan, severity, validation_status, state, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS security_review_slices (
			id                TEXT NOT NULL,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			source            TEXT NOT NULL,
			title             TEXT NOT NULL,
			summary           TEXT NOT NULL DEFAULT '',
			kind              TEXT NOT NULL DEFAULT 'unknown',
			confidence        TEXT NOT NULL DEFAULT 'medium',
			status            TEXT NOT NULL DEFAULT 'pending',
			entrypoints_json  TEXT NOT NULL DEFAULT '[]',
			owned_files_json  TEXT NOT NULL DEFAULT '[]',
			context_files_json TEXT NOT NULL DEFAULT '[]',
			tests_json        TEXT NOT NULL DEFAULT '[]',
			tags_json         TEXT NOT NULL DEFAULT '[]',
			trust_boundaries_json TEXT NOT NULL DEFAULT '[]',
			changed_files_json TEXT NOT NULL DEFAULT '[]',
			changed_line_ranges_json TEXT NOT NULL DEFAULT '[]',
			review_context_json TEXT NOT NULL DEFAULT '',
			review_context_hash TEXT NOT NULL DEFAULT '',
			last_scan_run_id  TEXT NOT NULL DEFAULT '',
			last_reviewed_at  TIMESTAMP,
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, repository_scan, id)
		)`,
		`CREATE TABLE IF NOT EXISTS security_dropped_findings (
			id                TEXT PRIMARY KEY,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			scan_run_id       TEXT NOT NULL,
			task_name         TEXT NOT NULL,
			slice_id          TEXT NOT NULL DEFAULT '',
			reason            TEXT NOT NULL,
			sample_json       TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_dropped_findings_run
			ON security_dropped_findings(namespace, repository_scan, scan_run_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS security_patch_proposals (
			id                TEXT PRIMARY KEY,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			finding_id        TEXT NOT NULL,
			task_name         TEXT NOT NULL,
			branch            TEXT NOT NULL,
			diff_artifact     TEXT NOT NULL DEFAULT '',
			summary_artifact  TEXT NOT NULL DEFAULT '',
			status            TEXT NOT NULL,
			reason            TEXT NOT NULL DEFAULT '',
			pr_number         INTEGER,
			pr_url            TEXT NOT NULL DEFAULT '',
			publication_evidence_json TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_patch_proposals_finding
			ON security_patch_proposals(namespace, finding_id, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS repository_monitors (
			namespace  TEXT NOT NULL,
			name       TEXT NOT NULL,
			uid        TEXT NOT NULL DEFAULT '',
			repo_url   TEXT NOT NULL,
			owner      TEXT NOT NULL DEFAULT '',
			repository TEXT NOT NULL DEFAULT '',
			branch     TEXT NOT NULL DEFAULT '',
			generation INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (namespace, name)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_repository_monitors_repo
			ON repository_monitors(namespace, owner, repository)`,
		`CREATE TABLE IF NOT EXISTS monitor_runs (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			trigger            TEXT NOT NULL,
			target_kind        TEXT NOT NULL DEFAULT '',
			target_number      INTEGER NOT NULL DEFAULT 0,
			target_sha         TEXT NOT NULL DEFAULT '',
			command_event_id   TEXT NOT NULL DEFAULT '',
			phase              TEXT NOT NULL,
			started_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at       TIMESTAMP,
			selected_count     INTEGER NOT NULL DEFAULT 0,
			created_task_count INTEGER NOT NULL DEFAULT 0,
			skipped_count      INTEGER NOT NULL DEFAULT 0,
			error              TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_runs_monitor
			ON monitor_runs(monitor_namespace, monitor_name, started_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_runs_target
			ON monitor_runs(monitor_namespace, monitor_name, phase, trigger, target_kind, target_number, target_sha)`,
		`DROP INDEX IF EXISTS idx_monitor_runs_active`,
		`DROP INDEX IF EXISTS idx_monitor_runs_queued`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_runs_running
			ON monitor_runs(monitor_namespace, monitor_name)
			WHERE phase = 'running'`,
		`CREATE TABLE IF NOT EXISTS monitor_items (
			monitor_namespace     TEXT NOT NULL,
			monitor_name          TEXT NOT NULL,
			kind                  TEXT NOT NULL,
			item_key              TEXT NOT NULL,
			number                INTEGER NOT NULL DEFAULT 0,
			sha                   TEXT NOT NULL DEFAULT '',
			title                 TEXT NOT NULL DEFAULT '',
			body                  TEXT NOT NULL DEFAULT '',
			html_url              TEXT NOT NULL DEFAULT '',
			author                TEXT NOT NULL DEFAULT '',
			state                 TEXT NOT NULL DEFAULT '',
			labels_json           TEXT NOT NULL DEFAULT '[]',
			snapshot_digest       TEXT NOT NULL DEFAULT '',
			github_updated_at     TIMESTAMP NOT NULL DEFAULT '0001-01-01T00:00:00Z',
			workflow_phase        TEXT NOT NULL DEFAULT '',
			linked_pr_number      INTEGER NOT NULL DEFAULT 0,
			last_command_id       TEXT NOT NULL DEFAULT '',
			last_command_intent   TEXT NOT NULL DEFAULT '',
			last_action_id        TEXT NOT NULL DEFAULT '',
			last_action_kind      TEXT NOT NULL DEFAULT '',
			last_action_task_name TEXT NOT NULL DEFAULT '',
			base_branch           TEXT NOT NULL DEFAULT '',
			head_branch           TEXT NOT NULL DEFAULT '',
			head_sha              TEXT NOT NULL DEFAULT '',
			base_sha              TEXT NOT NULL DEFAULT '',
			draft                 BOOLEAN NOT NULL DEFAULT FALSE,
			mergeable_state       TEXT NOT NULL DEFAULT '',
			ci_state              TEXT NOT NULL DEFAULT '',
			skip_reason           TEXT NOT NULL DEFAULT '',
			last_review_id        TEXT NOT NULL DEFAULT '',
			last_reviewed_head_sha TEXT NOT NULL DEFAULT '',
			last_verdict          TEXT NOT NULL DEFAULT '',
			repair_state          TEXT NOT NULL DEFAULT '',
			automerge_state       TEXT NOT NULL DEFAULT '',
			status_comment_id     TEXT NOT NULL DEFAULT '',
			status_comment_url    TEXT NOT NULL DEFAULT '',
			last_publish_id       TEXT NOT NULL DEFAULT '',
			last_publish_phase    TEXT NOT NULL DEFAULT '',
			last_publish_reason   TEXT NOT NULL DEFAULT '',
			last_publish_url      TEXT NOT NULL DEFAULT '',
			updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_seen_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (monitor_namespace, monitor_name, kind, item_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_items_queue
			ON monitor_items(monitor_namespace, monitor_name, kind, state, last_verdict, repair_state, automerge_state, updated_at DESC)`,

		`CREATE TABLE IF NOT EXISTS work_actions (
			id                     TEXT PRIMARY KEY,
			monitor_namespace      TEXT NOT NULL,
			monitor_name           TEXT NOT NULL,
			run_id                 TEXT NOT NULL DEFAULT '',
			command_event_id       TEXT NOT NULL DEFAULT '',
			monitor_generation     INTEGER NOT NULL DEFAULT 0,
			target_kind            TEXT NOT NULL DEFAULT '',
			target_number          INTEGER NOT NULL DEFAULT 0,
			target_sha             TEXT NOT NULL DEFAULT '',
			target_snapshot_digest TEXT NOT NULL DEFAULT '',
			intent                 TEXT NOT NULL DEFAULT '',
			desired_action         TEXT NOT NULL DEFAULT '',
			depends_on_action_id   TEXT NOT NULL DEFAULT '',
			dedupe_key             TEXT NOT NULL DEFAULT '',
			idempotency_key        TEXT NOT NULL DEFAULT '',
			status                 TEXT NOT NULL DEFAULT '',
			phase                  TEXT NOT NULL DEFAULT '',
			attempt                INTEGER NOT NULL DEFAULT 0,
			lease_owner            TEXT NOT NULL DEFAULT '',
			lease_expires_at       TIMESTAMP,
			task_name              TEXT NOT NULL DEFAULT '',
			blocked_reason         TEXT NOT NULL DEFAULT '',
			error                  TEXT NOT NULL DEFAULT '',
			artifact_ids           TEXT NOT NULL DEFAULT '',
			payload_digest         TEXT NOT NULL DEFAULT '',
			metadata_json          TEXT NOT NULL DEFAULT '{}',
			created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at           TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_work_actions_monitor
			ON work_actions(monitor_namespace, monitor_name, target_kind, target_number, desired_action, status, updated_at DESC)`,
		`DROP INDEX IF EXISTS idx_work_actions_dedupe`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_work_actions_dedupe
			ON work_actions(monitor_namespace, monitor_name, dedupe_key)
			WHERE dedupe_key <> '' AND status IN ('queued', 'leased', 'running')`,
		`CREATE TABLE IF NOT EXISTS action_records (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			kind               TEXT NOT NULL,
			number             INTEGER NOT NULL DEFAULT 0,
			action_kind        TEXT NOT NULL,
			snapshot_digest    TEXT NOT NULL DEFAULT '',
			head_sha           TEXT NOT NULL DEFAULT '',
			task_name          TEXT NOT NULL DEFAULT '',
			command_event_id   TEXT NOT NULL DEFAULT '',
			work_action_id     TEXT NOT NULL DEFAULT '',
			monitor_generation INTEGER NOT NULL DEFAULT 0,
			verdict            TEXT NOT NULL DEFAULT '',
			confidence         TEXT NOT NULL DEFAULT '',
			summary            TEXT NOT NULL DEFAULT '',
			payload_json       TEXT NOT NULL DEFAULT '{}',
			payload_digest     TEXT NOT NULL DEFAULT '',
			created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_action_records_monitor
			ON action_records(monitor_namespace, monitor_name, kind, number, action_kind, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS review_records (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			kind               TEXT NOT NULL,
			number             INTEGER NOT NULL DEFAULT 0,
			head_sha           TEXT NOT NULL DEFAULT '',
			task_name          TEXT NOT NULL DEFAULT '',
			task_namespace     TEXT NOT NULL DEFAULT '',
			verdict            TEXT NOT NULL DEFAULT '',
			confidence         TEXT NOT NULL DEFAULT '',
			repairable         BOOLEAN NOT NULL DEFAULT FALSE,
			security_status    TEXT NOT NULL DEFAULT '',
			findings_json      TEXT NOT NULL DEFAULT '[]',
			summary            TEXT NOT NULL DEFAULT '',
			suggested_comment  TEXT NOT NULL DEFAULT '',
			validation_task          TEXT NOT NULL DEFAULT '',
			validation_image         TEXT NOT NULL DEFAULT '',
			validation_command_digest TEXT NOT NULL DEFAULT '',
			validation_status        TEXT NOT NULL DEFAULT '',
			validation_evidence      TEXT NOT NULL DEFAULT '',
			rendered_comment   TEXT NOT NULL DEFAULT '',
			marker             TEXT NOT NULL DEFAULT '',
			github_review_id   TEXT NOT NULL DEFAULT '',
			github_comment_id  TEXT NOT NULL DEFAULT '',
			github_comment_url TEXT NOT NULL DEFAULT '',
			created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_review_records_item
			ON review_records(monitor_namespace, monitor_name, kind, number, head_sha, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS review_publish_records (
			id                   TEXT PRIMARY KEY,
			monitor_namespace    TEXT NOT NULL,
			monitor_name         TEXT NOT NULL,
			item_kind            TEXT NOT NULL DEFAULT '',
			item_number          INTEGER NOT NULL DEFAULT 0,
			head_sha             TEXT NOT NULL DEFAULT '',
			run_id               TEXT NOT NULL DEFAULT '',
			review_task_name     TEXT NOT NULL DEFAULT '',
			review_record_id     TEXT NOT NULL DEFAULT '',
			phase                TEXT NOT NULL DEFAULT '',
			event                TEXT NOT NULL DEFAULT '',
			github_review_id     TEXT NOT NULL DEFAULT '',
			github_review_url    TEXT NOT NULL DEFAULT '',
			body_digest          TEXT NOT NULL DEFAULT '',
			inline_comment_count INTEGER NOT NULL DEFAULT 0,
			skip_reason          TEXT NOT NULL DEFAULT '',
			error                TEXT NOT NULL DEFAULT '',
			created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_review_publish_records_item
			ON review_publish_records(monitor_namespace, monitor_name, item_kind, item_number, head_sha, phase, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_review_publish_records_review
			ON review_publish_records(monitor_namespace, review_record_id, updated_at DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_review_publish_records_succeeded_head
			ON review_publish_records(monitor_namespace, monitor_name, item_kind, item_number, head_sha)
			WHERE phase = 'succeeded'`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_review_publish_records_active_head
			ON review_publish_records(monitor_namespace, monitor_name, item_kind, item_number, head_sha)
			WHERE phase IN ('started', 'succeeded')`,
		`CREATE TABLE IF NOT EXISTS command_events (
			id                    TEXT PRIMARY KEY,
			monitor_namespace     TEXT NOT NULL,
			monitor_name          TEXT NOT NULL,
			repo                  TEXT NOT NULL DEFAULT '',
			kind                  TEXT NOT NULL DEFAULT '',
			number                INTEGER NOT NULL DEFAULT 0,
			source                TEXT NOT NULL DEFAULT '',
			delivery_id           TEXT NOT NULL DEFAULT '',
			label                 TEXT NOT NULL DEFAULT '',
			monitor_generation    INTEGER NOT NULL DEFAULT 0,
			dedupe_key            TEXT NOT NULL DEFAULT '',
			idempotency_key       TEXT NOT NULL DEFAULT '',
			comment_id            TEXT NOT NULL DEFAULT '',
			comment_url           TEXT NOT NULL DEFAULT '',
			author                TEXT NOT NULL DEFAULT '',
			author_association    TEXT NOT NULL DEFAULT '',
			permission            TEXT NOT NULL DEFAULT '',
			command               TEXT NOT NULL DEFAULT '',
			intent                TEXT NOT NULL DEFAULT '',
			head_sha              TEXT NOT NULL DEFAULT '',
			issue_snapshot_digest TEXT NOT NULL DEFAULT '',
			status                TEXT NOT NULL DEFAULT '',
			status_comment_id     TEXT NOT NULL DEFAULT '',
			created_repair_job_id TEXT NOT NULL DEFAULT '',
			created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			processed_at          TIMESTAMP,
			error                 TEXT NOT NULL DEFAULT '',
			UNIQUE(monitor_namespace, monitor_name, comment_id, command, head_sha)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_command_events_monitor
			ON command_events(monitor_namespace, monitor_name, created_at DESC)`,

		`CREATE TABLE IF NOT EXISTS implementation_jobs (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			repo               TEXT NOT NULL DEFAULT '',
			issue_number       INTEGER NOT NULL DEFAULT 0,
			plan_id            TEXT NOT NULL DEFAULT '',
			snapshot_digest    TEXT NOT NULL DEFAULT '',
			phase              TEXT NOT NULL DEFAULT '',
			attempt            INTEGER NOT NULL DEFAULT 0,
			branch             TEXT NOT NULL DEFAULT '',
			patch_artifact_id  TEXT NOT NULL DEFAULT '',
			pr_number          INTEGER NOT NULL DEFAULT 0,
			validation_state   TEXT NOT NULL DEFAULT '',
			task_name          TEXT NOT NULL DEFAULT '',
			mutation_task_name TEXT NOT NULL DEFAULT '',
			command_event_id   TEXT NOT NULL DEFAULT '',
			work_action_id     TEXT NOT NULL DEFAULT '',
			monitor_generation INTEGER NOT NULL DEFAULT 0,
			error              TEXT NOT NULL DEFAULT '',
			created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at       TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_implementation_jobs_monitor
			ON implementation_jobs(monitor_namespace, monitor_name, issue_number, phase, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS github_mutation_records (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			run_id             TEXT NOT NULL DEFAULT '',
			command_event_id   TEXT NOT NULL DEFAULT '',
			work_action_id     TEXT NOT NULL DEFAULT '',
			monitor_generation INTEGER NOT NULL DEFAULT 0,
			operation          TEXT NOT NULL,
			target_kind        TEXT NOT NULL DEFAULT '',
			target_number      INTEGER NOT NULL DEFAULT 0,
			target_sha         TEXT NOT NULL DEFAULT '',
			actor              TEXT NOT NULL DEFAULT '',
			reason             TEXT NOT NULL DEFAULT '',
			request_digest     TEXT NOT NULL DEFAULT '',
			github_url         TEXT NOT NULL DEFAULT '',
			github_request_id  TEXT NOT NULL DEFAULT '',
			external_id        TEXT NOT NULL DEFAULT '',
			status             TEXT NOT NULL DEFAULT '',
			error              TEXT NOT NULL DEFAULT '',
			created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_github_mutation_records_monitor
			ON github_mutation_records(monitor_namespace, monitor_name, target_kind, target_number, operation, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS repair_jobs (
			id                  TEXT PRIMARY KEY,
			monitor_namespace   TEXT NOT NULL,
			monitor_name        TEXT NOT NULL,
			repo                TEXT NOT NULL DEFAULT '',
			pr_number           INTEGER NOT NULL DEFAULT 0,
			intent              TEXT NOT NULL DEFAULT '',
			source              TEXT NOT NULL DEFAULT '',
			head_sha            TEXT NOT NULL DEFAULT '',
			base_sha            TEXT NOT NULL DEFAULT '',
			phase               TEXT NOT NULL DEFAULT '',
			repair_count_pr     INTEGER NOT NULL DEFAULT 0,
			repair_count_head   INTEGER NOT NULL DEFAULT 0,
			validation_attempts INTEGER NOT NULL DEFAULT 0,
			review_fix_attempts INTEGER NOT NULL DEFAULT 0,
			task_name           TEXT NOT NULL DEFAULT '',
			branch              TEXT NOT NULL DEFAULT '',
			pushed_sha          TEXT NOT NULL DEFAULT '',
			last_error          TEXT NOT NULL DEFAULT '',
			created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at        TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_repair_jobs_monitor
			ON repair_jobs(monitor_namespace, monitor_name, pr_number, phase, updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS monitor_events (
			id                 TEXT PRIMARY KEY,
			monitor_namespace  TEXT NOT NULL,
			monitor_name       TEXT NOT NULL,
			run_id             TEXT NOT NULL DEFAULT '',
			item_kind          TEXT NOT NULL DEFAULT '',
			item_number        INTEGER NOT NULL DEFAULT 0,
			item_sha           TEXT NOT NULL DEFAULT '',
			event_type         TEXT NOT NULL,
			actor              TEXT NOT NULL DEFAULT '',
			summary            TEXT NOT NULL DEFAULT '',
			metadata_json      TEXT NOT NULL DEFAULT '{}',
			created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_events_monitor
			ON monitor_events(monitor_namespace, monitor_name, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS gateway_events (
			id                    TEXT NOT NULL,
			namespace             TEXT NOT NULL,
			namespace_uid         TEXT NOT NULL DEFAULT '',
			gateway_uid           TEXT NOT NULL,
			gateway_generation    INTEGER NOT NULL DEFAULT 0,
			gateway_name          TEXT NOT NULL,
			binding_name          TEXT NOT NULL DEFAULT '',
			binding_uid           TEXT NOT NULL DEFAULT '',
			binding_generation    INTEGER NOT NULL DEFAULT 0,
			agent_name            TEXT NOT NULL DEFAULT '',
			agent_uid             TEXT NOT NULL DEFAULT '',
			external_event_id     TEXT NOT NULL,
			protocol_version      TEXT NOT NULL,
			event_type            TEXT NOT NULL,
			state                 TEXT NOT NULL,
			state_message         TEXT NOT NULL DEFAULT '',
			account_id            TEXT NOT NULL,
			context_id            TEXT NOT NULL,
			thread_id             TEXT NOT NULL DEFAULT '',
			sender_id             TEXT NOT NULL,
			sender_display_name   TEXT NOT NULL DEFAULT '',
			text                  TEXT NOT NULL DEFAULT '',
			reply_target          TEXT NOT NULL DEFAULT '',
			metadata_json         TEXT NOT NULL DEFAULT '{}',
			session_name          TEXT NOT NULL DEFAULT '',
			 task_name             TEXT NOT NULL DEFAULT '',
			 task_uid              TEXT NOT NULL DEFAULT '',
			 task_runtime_allowed_tools_json TEXT,
			 delivery_id           TEXT NOT NULL DEFAULT '',
			 provider_message_id    TEXT NOT NULL DEFAULT '',
			 trace_parent          TEXT NOT NULL DEFAULT '',
			 trace_state           TEXT NOT NULL DEFAULT '',
			 transcript_order      INTEGER NOT NULL DEFAULT 0,
			attempt_count         INTEGER NOT NULL DEFAULT 0,
			claim_owner           TEXT NOT NULL DEFAULT '',
			claim_until           TIMESTAMP,
			next_attempt_at       TIMESTAMP NOT NULL,
			occurred_at           TIMESTAMP,
			received_at           TIMESTAMP NOT NULL,
			expires_at            TIMESTAMP NOT NULL,
			created_at            TIMESTAMP NOT NULL,
			updated_at            TIMESTAMP NOT NULL,
			completed_at          TIMESTAMP,
			PRIMARY KEY (namespace, id),
			UNIQUE(namespace, gateway_uid, external_event_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_dispatch
			ON gateway_events(state, next_attempt_at, received_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_session
			ON gateway_events(namespace, session_name, state, received_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_task_owner
			ON gateway_events(namespace, session_name, task_name, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_task_identity
			ON gateway_events(namespace, task_name, task_uid, state, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_gateway
			ON gateway_events(namespace, gateway_name, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS gateway_event_tombstones (
			namespace          TEXT NOT NULL,
			gateway_uid        TEXT NOT NULL,
			external_event_id  TEXT NOT NULL,
			event_id           TEXT NOT NULL,
			task_name          TEXT NOT NULL DEFAULT '',
			task_uid           TEXT NOT NULL DEFAULT '',
			envelope_digest    TEXT NOT NULL,
			session_name       TEXT NOT NULL DEFAULT '',
			transcript_order   INTEGER NOT NULL DEFAULT 0,
			expires_at         TIMESTAMP NOT NULL,
			created_at         TIMESTAMP NOT NULL,
			PRIMARY KEY (namespace, gateway_uid, external_event_id),
			UNIQUE(namespace, event_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_event_tombstones_expiry
			ON gateway_event_tombstones(namespace, expires_at)`,
		`CREATE TABLE IF NOT EXISTS gateway_deliveries (
			id                    TEXT NOT NULL,
			idempotency_id        TEXT NOT NULL,
			namespace             TEXT NOT NULL,
			namespace_uid         TEXT NOT NULL DEFAULT '',
			gateway_uid           TEXT NOT NULL,
			gateway_generation    INTEGER NOT NULL DEFAULT 0,
			gateway_name          TEXT NOT NULL,
			binding_name          TEXT NOT NULL DEFAULT '',
			event_id              TEXT NOT NULL,
			task_name             TEXT NOT NULL DEFAULT '',
			session_name          TEXT NOT NULL DEFAULT '',
			kind                  TEXT NOT NULL,
			state                 TEXT NOT NULL,
			account_id            TEXT NOT NULL,
			context_id            TEXT NOT NULL,
			thread_id             TEXT NOT NULL DEFAULT '',
			reply_target          TEXT NOT NULL,
			text                  TEXT NOT NULL,
			metadata_json         TEXT NOT NULL DEFAULT '{}',
			attempt_count         INTEGER NOT NULL DEFAULT 0,
			max_attempts          INTEGER NOT NULL DEFAULT 10,
			manual_retry_count    INTEGER NOT NULL DEFAULT 0,
			next_attempt_at       TIMESTAMP NOT NULL,
			expires_at            TIMESTAMP NOT NULL,
			 provider_message_id   TEXT NOT NULL DEFAULT '',
			 trace_parent          TEXT NOT NULL DEFAULT '',
			 trace_state           TEXT NOT NULL DEFAULT '',
			 last_error            TEXT NOT NULL DEFAULT '',
			claim_owner           TEXT NOT NULL DEFAULT '',
			claim_until           TIMESTAMP,
			created_at            TIMESTAMP NOT NULL,
			updated_at            TIMESTAMP NOT NULL,
			delivered_at          TIMESTAMP,
			PRIMARY KEY (namespace, id),
			UNIQUE(namespace, idempotency_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_deliveries_send
			ON gateway_deliveries(state, next_attempt_at, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_deliveries_event
			ON gateway_deliveries(namespace, event_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_deliveries_gateway
			ON gateway_deliveries(namespace, gateway_name, created_at DESC)`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	if err := ensureSQLiteColumns(db, "sessions", []sqliteColumnMigration{
		{Name: "owner_type", Definition: "owner_type TEXT NOT NULL DEFAULT ''"},
		{Name: "owner_ref", Definition: "owner_ref TEXT NOT NULL DEFAULT ''"},
		{Name: "active_task_uid", Definition: "active_task_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "active_task_expires_at", Definition: "active_task_expires_at TIMESTAMP"},
		{Name: "control_session_uid", Definition: "control_session_uid TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "session_cleanup_completions", []sqliteColumnMigration{
		{Name: "session_uid", Definition: "session_uid TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_session_cleanup_completions_uid
		ON session_cleanup_completions(session_uid) WHERE session_uid <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE sessions SET active_task_uid = COALESCE((
		SELECT event.task_uid FROM gateway_events event
		WHERE event.namespace = sessions.namespace AND event.session_name = sessions.name
		  AND event.task_name = sessions.active_task AND event.state = 'TaskCreated' AND event.task_uid <> ''
		ORDER BY event.created_at DESC, event.id DESC LIMIT 1
	), '') WHERE active_task <> '' AND active_task_uid = ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := ensureSQLiteColumns(db, "gateway_events", []sqliteColumnMigration{
		{Name: "namespace_uid", Definition: "namespace_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "gateway_generation", Definition: "gateway_generation INTEGER NOT NULL DEFAULT 0"},
	}); err != nil {
		return err
	}
	if err := ensureGatewayEventTombstoneSchema(db); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "gateway_deliveries", []sqliteColumnMigration{
		{Name: "namespace_uid", Definition: "namespace_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "gateway_generation", Definition: "gateway_generation INTEGER NOT NULL DEFAULT 0"},
	}); err != nil {
		return err
	}

	if err := ensureSQLiteColumns(db, "session_messages", []sqliteColumnMigration{
		{Name: "message_id", Definition: "message_id TEXT NOT NULL DEFAULT ''"},
		{Name: "sort_order", Definition: "sort_order INTEGER NOT NULL DEFAULT 0"},
		{Name: "source_type", Definition: "source_type TEXT NOT NULL DEFAULT ''"},
		{Name: "source_ref", Definition: "source_ref TEXT NOT NULL DEFAULT ''"},
		{Name: "metadata_json", Definition: "metadata_json TEXT NOT NULL DEFAULT '{}'"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE session_messages SET message_id = 'legacy:' || id WHERE message_id = ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`UPDATE session_messages SET sort_order = id * 2 WHERE sort_order = 0`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_session_messages_message_id
		ON session_messages(namespace, session_name, message_id) WHERE message_id <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_session_messages_namespace_message_id
		ON session_messages(namespace, message_id) WHERE message_id <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_session_messages_sort_order
		ON session_messages(namespace, session_name, sort_order) WHERE sort_order > 0`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if err := ensureSQLiteColumns(db, "gateway_events", []sqliteColumnMigration{
		{Name: "transcript_order", Definition: "transcript_order INTEGER NOT NULL DEFAULT 0"},
		{Name: "binding_uid", Definition: "binding_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "binding_generation", Definition: "binding_generation INTEGER NOT NULL DEFAULT 0"},
		{Name: "agent_name", Definition: "agent_name TEXT NOT NULL DEFAULT ''"},
		{Name: "agent_uid", Definition: "agent_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "task_uid", Definition: "task_uid TEXT NOT NULL DEFAULT ''"},
		{Name: "task_runtime_allowed_tools_json", Definition: "task_runtime_allowed_tools_json TEXT"},
		{Name: "delivery_id", Definition: "delivery_id TEXT NOT NULL DEFAULT ''"},
		{Name: "provider_message_id", Definition: "provider_message_id TEXT NOT NULL DEFAULT ''"},
		{Name: "trace_parent", Definition: "trace_parent TEXT NOT NULL DEFAULT ''"},
		{Name: "trace_state", Definition: "trace_state TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "gateway_deliveries", []sqliteColumnMigration{
		{Name: "trace_parent", Definition: "trace_parent TEXT NOT NULL DEFAULT ''"},
		{Name: "trace_state", Definition: "trace_state TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}

	if err := ensureSQLiteColumns(db, "execution_events", []sqliteColumnMigration{
		{Name: "session_seq", Definition: "session_seq INTEGER NOT NULL DEFAULT 0"},
		{Name: "dedupe_key", Definition: "dedupe_key TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_events_stream_dedupe_key
		ON execution_events(namespace, stream_type, stream_id, dedupe_key) WHERE dedupe_key <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_execution_events_session_seq
		ON execution_events(namespace, session_name, session_seq)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS execution_event_session_sequences (
		namespace    TEXT NOT NULL,
		session_name TEXT NOT NULL,
		latest_seq   INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (namespace, session_name)
	)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := backfillExecutionEventSessionCursors(db); err != nil {
		return err
	}

	if err := ensureSQLiteColumns(db, "memories", []sqliteColumnMigration{
		{Name: "source_proposal_id", Definition: "source_proposal_id TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "memory_proposals", []sqliteColumnMigration{
		{Name: "applied_memory_id", Definition: "applied_memory_id TEXT NOT NULL DEFAULT ''"},
		{Name: "applied_by", Definition: "applied_by TEXT NOT NULL DEFAULT ''"},
		{Name: "applied_at", Definition: "applied_at TIMESTAMP"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "monitor_runs", []sqliteColumnMigration{
		{Name: "command_event_id", Definition: "command_event_id TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "monitor_items", []sqliteColumnMigration{
		{Name: "body", Definition: "body TEXT NOT NULL DEFAULT ''"},
		{Name: "html_url", Definition: "html_url TEXT NOT NULL DEFAULT ''"},
		{Name: "skip_reason", Definition: "skip_reason TEXT NOT NULL DEFAULT ''"},
		{Name: "last_publish_id", Definition: "last_publish_id TEXT NOT NULL DEFAULT ''"},
		{Name: "last_publish_phase", Definition: "last_publish_phase TEXT NOT NULL DEFAULT ''"},
		{Name: "last_publish_reason", Definition: "last_publish_reason TEXT NOT NULL DEFAULT ''"},
		{Name: "last_publish_url", Definition: "last_publish_url TEXT NOT NULL DEFAULT ''"},
		{Name: "snapshot_digest", Definition: "snapshot_digest TEXT NOT NULL DEFAULT ''"},
		{Name: "github_updated_at", Definition: "github_updated_at TIMESTAMP NOT NULL DEFAULT '0001-01-01T00:00:00Z'"},
		{Name: "workflow_phase", Definition: "workflow_phase TEXT NOT NULL DEFAULT ''"},
		{Name: "linked_pr_number", Definition: "linked_pr_number INTEGER NOT NULL DEFAULT 0"},
		{Name: "last_command_id", Definition: "last_command_id TEXT NOT NULL DEFAULT ''"},
		{Name: "last_command_intent", Definition: "last_command_intent TEXT NOT NULL DEFAULT ''"},
		{Name: "last_action_id", Definition: "last_action_id TEXT NOT NULL DEFAULT ''"},
		{Name: "last_action_kind", Definition: "last_action_kind TEXT NOT NULL DEFAULT ''"},
		{Name: "last_action_task_name", Definition: "last_action_task_name TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE monitor_items SET github_updated_at = updated_at WHERE github_updated_at IS NULL OR github_updated_at = '0001-01-01T00:00:00Z'`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := ensureSQLiteColumns(db, "review_records", []sqliteColumnMigration{
		{Name: "validation_task", Definition: "validation_task TEXT NOT NULL DEFAULT ''"},
		{Name: "validation_image", Definition: "validation_image TEXT NOT NULL DEFAULT ''"},
		{Name: "validation_command_digest", Definition: "validation_command_digest TEXT NOT NULL DEFAULT ''"},
		{Name: "validation_status", Definition: "validation_status TEXT NOT NULL DEFAULT ''"},
		{Name: "validation_evidence", Definition: "validation_evidence TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "command_events", []sqliteColumnMigration{
		{Name: "source", Definition: "source TEXT NOT NULL DEFAULT ''"},
		{Name: "delivery_id", Definition: "delivery_id TEXT NOT NULL DEFAULT ''"},
		{Name: "label", Definition: "label TEXT NOT NULL DEFAULT ''"},
		{Name: "monitor_generation", Definition: "monitor_generation INTEGER NOT NULL DEFAULT 0"},
		{Name: "dedupe_key", Definition: "dedupe_key TEXT NOT NULL DEFAULT ''"},
		{Name: "idempotency_key", Definition: "idempotency_key TEXT NOT NULL DEFAULT ''"},
		{Name: "issue_snapshot_digest", Definition: "issue_snapshot_digest TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_command_events_dedupe
		ON command_events(monitor_namespace, monitor_name, dedupe_key)
		WHERE dedupe_key <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := ensureSQLiteColumns(db, "security_scan_runs", []sqliteColumnMigration{
		{Name: "slice_count", Definition: "slice_count INTEGER NOT NULL DEFAULT 0"},
		{Name: "reviewed_slice_count", Definition: "reviewed_slice_count INTEGER NOT NULL DEFAULT 0"},
		{Name: "skipped_slice_count", Definition: "skipped_slice_count INTEGER NOT NULL DEFAULT 0"},
		{Name: "accepted_findings", Definition: "accepted_findings INTEGER NOT NULL DEFAULT 0"},
		{Name: "dropped_findings", Definition: "dropped_findings INTEGER NOT NULL DEFAULT 0"},
		{Name: "scanner_policy_version", Definition: "scanner_policy_version TEXT NOT NULL DEFAULT ''"},
		{Name: "policy_digest", Definition: "policy_digest TEXT NOT NULL DEFAULT ''"},
		{Name: "idempotency_key", Definition: "idempotency_key TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "security_findings", []sqliteColumnMigration{
		{Name: "slice_id", Definition: "slice_id TEXT NOT NULL DEFAULT ''"},
		{Name: "category", Definition: "category TEXT NOT NULL DEFAULT ''"},
		{Name: "triage", Definition: "triage TEXT NOT NULL DEFAULT ''"},
		{Name: "reproduction", Definition: "reproduction TEXT NOT NULL DEFAULT ''"},
		{Name: "why_tests_do_not_cover", Definition: "why_tests_do_not_cover TEXT NOT NULL DEFAULT ''"},
		{Name: "suggested_regression_test", Definition: "suggested_regression_test TEXT NOT NULL DEFAULT ''"},
		{Name: "minimum_fix_scope", Definition: "minimum_fix_scope TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "security_review_slices", []sqliteColumnMigration{
		{Name: "changed_files_json", Definition: "changed_files_json TEXT NOT NULL DEFAULT '[]'"},
		{Name: "changed_line_ranges_json", Definition: "changed_line_ranges_json TEXT NOT NULL DEFAULT '[]'"},
		{Name: "review_context_json", Definition: "review_context_json TEXT NOT NULL DEFAULT ''"},
		{Name: "review_context_hash", Definition: "review_context_hash TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "security_dropped_findings", []sqliteColumnMigration{
		{Name: "layer", Definition: "layer TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSQLiteColumns(db, "security_patch_proposals", []sqliteColumnMigration{
		{Name: "publication_evidence_json", Definition: "publication_evidence_json TEXT NOT NULL DEFAULT ''"},
		{Name: "reason", Definition: "reason TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if err := ensureSecurityReviewSlicesScopedPrimaryKey(db); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_security_findings_slice
		ON security_findings(namespace, repository_scan, slice_id, category, updated_at DESC)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_security_review_slices_repo
		ON security_review_slices(namespace, repository_scan, status, updated_at DESC)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_security_dropped_findings_layer
		ON security_dropped_findings(namespace, repository_scan, layer, created_at DESC)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_source_proposal
		ON memories(namespace, source_proposal_id)
		WHERE source_proposal_id <> ''`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := migrateControlStore(db); err != nil {
		return err
	}
	if err := migrateAgentExecution(db); err != nil {
		return err
	}

	return nil
}

func backfillExecutionEventSessionCursors(db *sql.DB) error {
	_, err := db.Exec(`INSERT INTO execution_event_session_sequences(namespace, session_name, latest_seq)
		SELECT namespace, session_name, MAX(CASE WHEN session_seq > 0 THEN session_seq ELSE rowid END)
		FROM execution_events
		WHERE session_name <> ''
		GROUP BY namespace, session_name
		ON CONFLICT(namespace, session_name) DO UPDATE SET latest_seq =
			CASE
				WHEN execution_event_session_sequences.latest_seq > excluded.latest_seq
				THEN execution_event_session_sequences.latest_seq
				ELSE excluded.latest_seq
			END`)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

func ensureGatewayEventTombstoneSchema(db *sql.DB) error {
	if err := ensureSQLiteColumns(db, "gateway_event_tombstones", []sqliteColumnMigration{
		{Name: "task_name", Definition: "task_name TEXT NOT NULL DEFAULT ''"},
		{Name: "task_uid", Definition: "task_uid TEXT NOT NULL DEFAULT ''"},
	}); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_gateway_event_tombstones_task
		ON gateway_event_tombstones(namespace, task_name, task_uid)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

type sqliteColumnMigration struct {
	Name       string
	Definition string
}

func ensureSQLiteColumns(db *sql.DB, table string, columns []sqliteColumnMigration) error {
	existing := map[string]struct{}{}
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, pk int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	for _, column := range columns {
		if _, ok := existing[column.Name]; ok {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", table, column.Definition)); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}
	return nil
}

func ensureSecurityReviewSlicesScopedPrimaryKey(db *sql.DB) error {
	pkColumns, err := sqlitePrimaryKeyColumns(db, "security_review_slices")
	if err != nil {
		return err
	}
	if len(pkColumns) == 3 &&
		pkColumns[0] == "namespace" &&
		pkColumns[1] == "repository_scan" &&
		pkColumns[2] == "id" {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DROP TABLE IF EXISTS security_review_slices_migration`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := tx.Exec(`CREATE TABLE security_review_slices_migration (
		id                TEXT NOT NULL,
		namespace         TEXT NOT NULL,
		repository_scan   TEXT NOT NULL,
		source            TEXT NOT NULL,
		title             TEXT NOT NULL,
		summary           TEXT NOT NULL DEFAULT '',
		kind              TEXT NOT NULL DEFAULT 'unknown',
		confidence        TEXT NOT NULL DEFAULT 'medium',
		status            TEXT NOT NULL DEFAULT 'pending',
		entrypoints_json  TEXT NOT NULL DEFAULT '[]',
		owned_files_json  TEXT NOT NULL DEFAULT '[]',
		context_files_json TEXT NOT NULL DEFAULT '[]',
		tests_json        TEXT NOT NULL DEFAULT '[]',
		tags_json         TEXT NOT NULL DEFAULT '[]',
		trust_boundaries_json TEXT NOT NULL DEFAULT '[]',
		changed_files_json TEXT NOT NULL DEFAULT '[]',
		changed_line_ranges_json TEXT NOT NULL DEFAULT '[]',
		review_context_json TEXT NOT NULL DEFAULT '',
		review_context_hash TEXT NOT NULL DEFAULT '',
		last_scan_run_id  TEXT NOT NULL DEFAULT '',
		last_reviewed_at  TIMESTAMP,
		created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (namespace, repository_scan, id)
	)`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO security_review_slices_migration
		(id, namespace, repository_scan, source, title, summary, kind, confidence, status,
		 entrypoints_json, owned_files_json, context_files_json, tests_json, tags_json,
		 trust_boundaries_json, changed_files_json, changed_line_ranges_json, review_context_json,
		 review_context_hash, last_scan_run_id, last_reviewed_at, created_at, updated_at)
		SELECT id, namespace, repository_scan, source, title, summary, kind, confidence, status,
		 entrypoints_json, owned_files_json, context_files_json, tests_json, tags_json,
		 trust_boundaries_json, changed_files_json, changed_line_ranges_json, review_context_json,
		 review_context_hash, last_scan_run_id, last_reviewed_at, created_at, updated_at
		FROM security_review_slices`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := tx.Exec(`DROP TABLE security_review_slices`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE security_review_slices_migration RENAME TO security_review_slices`); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

func sqlitePrimaryKeyColumns(db *sql.DB, table string) ([]string, error) {
	type pkColumn struct {
		name string
		seq  int
	}
	var columns []pkColumn
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, pk int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("migration failed: %w", err)
		}
		if pk > 0 {
			columns = append(columns, pkColumn{name: name, seq: pk})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}
	for i := range columns {
		for j := i + 1; j < len(columns); j++ {
			if columns[j].seq < columns[i].seq {
				columns[i], columns[j] = columns[j], columns[i]
			}
		}
	}
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.name)
	}
	return names, nil
}

// Store implements both store.ResultStore and store.SessionStore.
type Store struct {
	db               *sql.DB
	dbPath           string
	processLock      io.Closer
	executionEventMu sync.Mutex

	// snapshotCipher encrypts immutable agent execution snapshot bodies at
	// rest. Snapshot persistence fails closed while it is nil.
	snapshotCipher *AgentExecutionSnapshotCipher

	// applyMemoryProposalAfterAcceptedRead is a test hook used to coordinate
	// multi-connection proposal-apply races after an accepted proposal is read.
	applyMemoryProposalAfterAcceptedRead func()

	// archiveMemoryProposalAfterActiveRead is a test hook used to coordinate
	// multi-connection proposal-archive races after an active proposal is read.
	archiveMemoryProposalAfterActiveRead func()
}

// NewStore creates a new Store backed by the given SQLite database.
// The dbPath is the filesystem path to the database file (used for metrics and logging).
func NewStore(db *sql.DB, dbPath string) *Store {
	return &Store{db: db, dbPath: dbPath}
}

// OpenLockedStore acquires the process-lifetime filesystem lock adjacent to
// path before SQLite is opened or migrated. Production controller wiring must
// use this constructor so overlapping Pods or releases cannot migrate or write
// the same database even if Kubernetes ownership fencing is misconfigured.
func OpenLockedStore(path string) (*Store, error) {
	lock, err := lockDatabaseFile(path)
	if err != nil {
		return nil, err
	}
	db, err := NewDB(path)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &Store{db: db, dbPath: path, processLock: lock}, nil
}

// Start runs background maintenance and blocks until ctx is cancelled,
// then closes the database without issuing any final writes.
// It satisfies the controller-runtime manager.Runnable interface.
func (s *Store) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("sqlite-store")

	logger.Info("SQLite store is configured — ensure a PersistentVolume is mounted at the store path for data durability", "path", s.dbPath)

	// Update DB size metric periodically
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	// Record initial size
	s.updateDBSizeMetric()

	for {
		select {
		case <-ctx.Done():
			return s.close()
		case <-ticker.C:
			s.updateDBSizeMetric()
		}
	}
}

// NeedLeaderElection keeps SQLite-backed background mutation behind the
// controller-runtime leader gate. The filesystem lock remains held for the
// whole process lifetime, including standby/startup, and is the final defense
// against overlapping writers.
func (s *Store) NeedLeaderElection() bool { return true }

func (s *Store) close() error {
	dbErr := s.db.Close()
	var lockErr error
	if s.processLock != nil {
		lockErr = s.processLock.Close()
		s.processLock = nil
	}
	if dbErr != nil {
		return dbErr
	}
	return lockErr
}

// updateDBSizeMetric reads the database file size and updates the gauge.
func (s *Store) updateDBSizeMetric() {
	if s.dbPath == "" || s.dbPath == ":memory:" {
		return
	}
	info, err := os.Stat(s.dbPath)
	if err != nil {
		return
	}
	dbSizeBytes.Set(float64(info.Size()))
}

// HealthCheck verifies the database is reachable by executing a simple query.
func (s *Store) HealthCheck(ctx context.Context) error {
	var n int
	return s.db.QueryRowContext(ctx, "SELECT 1").Scan(&n)
}
