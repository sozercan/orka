package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
	moderncsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	defaultMemoryLimit          = 50
	maxMemoryLimit              = 200
	defaultProposalLimit        = 100
	maxProposalLimit            = 500
	defaultTranscriptLimit      = 10
	maxTranscriptLimit          = 50
	defaultTranscriptSnippetLen = 360
	maxTranscriptSnippetLen     = 1000

	memorySourceProposal = "memory_proposal"
	proposalTypeMemory   = "memory"

	proposalStatusPending  = "pending"
	proposalStatusAccepted = "accepted"
	proposalStatusRejected = "rejected"
	proposalStatusArchived = "archived"
	proposalStatusApplied  = "applied"
)

type rowScanner interface {
	Scan(dest ...any) error
}

// CreateMemory inserts a durable memory record.
func (s *Store) CreateMemory(ctx context.Context, memory *store.Memory) error {
	if memory == nil {
		return store.ValidationErrorf("memory is required")
	}
	if strings.TrimSpace(memory.Namespace) == "" {
		return store.ValidationErrorf("namespace is required")
	}
	memory.Content = redact.SensitiveText(memory.Content)
	if strings.TrimSpace(memory.Content) == "" {
		return store.ValidationErrorf("content is required")
	}
	if memory.ID == "" {
		memory.ID = "mem-" + uuid.NewString()
	}
	if memory.Source == "" {
		memory.Source = "manual"
	}
	now := time.Now()
	if memory.CreatedAt.IsZero() {
		memory.CreatedAt = now
	}
	if memory.UpdatedAt.IsZero() {
		memory.UpdatedAt = now
	}

	tagsJSON, err := marshalTags(memory.Tags)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO memories
		 (id, namespace, session_name, agent_name, task_name, parent_task, source, source_proposal_id, content, tags_json,
		  disabled, deleted, created_at, updated_at, last_recalled_at, recalled_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		memory.ID, memory.Namespace, memory.SessionName, memory.AgentName, memory.TaskName, memory.ParentTask,
		memory.Source, memory.SourceProposalID, memory.Content, tagsJSON, memory.Disabled, memory.Deleted, memory.CreatedAt,
		memory.UpdatedAt, memory.LastRecalledAt, memory.RecalledCount,
	)
	return err
}

// GetMemory fetches a memory by ID within a namespace.
func (s *Store) GetMemory(ctx context.Context, namespace, id string) (*store.Memory, error) {
	memory, err := scanMemory(s.db.QueryRowContext(ctx, selectMemorySQL()+` WHERE namespace = ? AND id = ?`, namespace, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return memory, nil
}

// ListMemories lists memories matching the filter ordered for compact recall/governance.
func (s *Store) ListMemories(ctx context.Context, filter store.MemoryFilter) ([]store.Memory, error) {
	if strings.TrimSpace(filter.Namespace) == "" {
		return nil, store.ValidationErrorf("namespace is required")
	}

	query := selectMemorySQL() + ` WHERE namespace = ?`
	args := []any{filter.Namespace}
	query = appendMemoryFilters(query, &args, filter)
	query += ` ORDER BY updated_at DESC, id DESC LIMIT ?`
	args = append(args, boundedLimit(filter.Limit, defaultMemoryLimit, maxMemoryLimit))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var memories []store.Memory
	for rows.Next() {
		memory, err := scanMemory(rows)
		if err != nil {
			return nil, err
		}
		memories = append(memories, *memory)
	}
	return memories, rows.Err()
}

// UpdateMemory updates mutable memory fields.
func (s *Store) UpdateMemory(ctx context.Context, memory *store.Memory) error {
	if memory == nil {
		return store.ValidationErrorf("memory is required")
	}
	if memory.Namespace == "" || memory.ID == "" {
		return store.ValidationErrorf("namespace and id are required")
	}
	memory.Content = redact.SensitiveText(memory.Content)
	if strings.TrimSpace(memory.Content) == "" {
		return store.ValidationErrorf("content is required")
	}
	tagsJSON, err := marshalTags(memory.Tags)
	if err != nil {
		return err
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE memories
		 SET session_name = ?, agent_name = ?, task_name = ?, parent_task = ?, source = ?, source_proposal_id = ?, content = ?, tags_json = ?,
		     disabled = ?, deleted = ?, updated_at = CURRENT_TIMESTAMP
		 WHERE namespace = ? AND id = ?`,
		memory.SessionName, memory.AgentName, memory.TaskName, memory.ParentTask, memory.Source, memory.SourceProposalID,
		memory.Content, tagsJSON, memory.Disabled, memory.Deleted, memory.Namespace, memory.ID,
	)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res)
}

// DeleteMemory soft-deletes a memory by ID within a namespace.
func (s *Store) DeleteMemory(ctx context.Context, namespace, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE memories SET deleted = TRUE, disabled = TRUE, updated_at = CURRENT_TIMESTAMP WHERE namespace = ? AND id = ?`,
		namespace, id,
	)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res)
}

// SetMemoryDisabled toggles memory recall without deleting the provenance record.
func (s *Store) SetMemoryDisabled(ctx context.Context, namespace, id string, disabled bool) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE memories SET disabled = ?, updated_at = CURRENT_TIMESTAMP WHERE namespace = ? AND id = ? AND deleted = FALSE`,
		disabled, namespace, id,
	)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res)
}

// SearchTranscript searches transcript content and returns compact snippets.
func (s *Store) SearchTranscript(ctx context.Context, filter store.TranscriptSearchFilter) ([]store.TranscriptSearchResult, error) {
	if strings.TrimSpace(filter.Namespace) == "" {
		return nil, store.ValidationErrorf("namespace is required")
	}

	var query strings.Builder
	query.WriteString(`SELECT message.id, message.message_id, message.session_name, message.role,
		COALESCE(message.name, ''), message.content, message.created_at
		FROM session_messages AS message
		JOIN sessions AS session ON session.namespace = message.namespace AND session.name = message.session_name
		WHERE message.namespace = ? AND session.session_type <> ? AND message.content <> ''`)
	args := []any{filter.Namespace, store.SessionTypeGateway}

	searchTerm := strings.TrimSpace(filter.Query)
	searchTerms := transcriptSearchTerms(searchTerm)
	for _, term := range searchTerms {
		query.WriteString(` AND lower(message.content) LIKE ?`)
		args = append(args, "%"+strings.ToLower(term)+"%")
	}
	if filter.SessionName != "" {
		query.WriteString(` AND message.session_name = ?`)
		args = append(args, filter.SessionName)
	}
	if filter.ExcludeSessionName != "" {
		query.WriteString(` AND message.session_name <> ?`)
		args = append(args, filter.ExcludeSessionName)
	}
	roles := compactStrings(filter.Roles)
	if len(roles) > 0 {
		placeholders := make([]string, 0, len(roles))
		for _, role := range roles {
			placeholders = append(placeholders, "?")
			args = append(args, role)
		}
		query.WriteString(` AND message.role IN (` + strings.Join(placeholders, ",") + `)`)
	}

	query.WriteString(` ORDER BY message.created_at DESC, message.id DESC LIMIT ?`)
	args = append(args, boundedLimit(filter.Limit, defaultTranscriptLimit, maxTranscriptLimit))

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	snippetLen := boundedLimit(filter.MaxSnippetLength, defaultTranscriptSnippetLen, maxTranscriptSnippetLen)
	snippetTerm := searchTerm
	if len(searchTerms) > 0 {
		snippetTerm = searchTerms[0]
	}
	var results []store.TranscriptSearchResult
	for rows.Next() {
		var result store.TranscriptSearchResult
		var content string
		if err := rows.Scan(&result.MessageID, &result.StableMessageID, &result.SessionName, &result.Role, &result.Name, &content, &result.CreatedAt); err != nil {
			return nil, err
		}
		result.Snippet = buildSnippet(content, snippetTerm, snippetLen)
		results = append(results, result)
	}
	return results, rows.Err()
}

// CreateMemoryProposal inserts a governance proposal.
func (s *Store) CreateMemoryProposal(ctx context.Context, proposal *store.MemoryProposal) error {
	if proposal == nil {
		return store.ValidationErrorf("proposal is required")
	}
	if strings.TrimSpace(proposal.Namespace) == "" {
		return store.ValidationErrorf("namespace is required")
	}
	proposal.Type = strings.ToLower(strings.TrimSpace(proposal.Type))
	if proposal.Type == "" {
		proposal.Type = "skill"
	}
	proposal.Title = redact.SensitiveText(proposal.Title)
	proposal.Description = redact.SensitiveText(proposal.Description)
	proposal.Content = redact.SensitiveText(proposal.Content)
	proposal.Patch = redact.SensitiveText(proposal.Patch)
	proposal.Reviewer = redact.SensitiveText(proposal.Reviewer)
	proposal.ReviewNote = redact.SensitiveText(proposal.ReviewNote)
	proposal.AppliedBy = redact.SensitiveText(proposal.AppliedBy)
	if strings.TrimSpace(proposal.Title) == "" {
		return store.ValidationErrorf("title is required")
	}
	if proposal.ID == "" {
		proposal.ID = "mprop-" + uuid.NewString()
	}
	proposal.Status = normalizeProposalStatus(proposal.Status)
	if proposal.Status == "" {
		proposal.Status = proposalStatusPending
	}
	if !isKnownProposalStatus(proposal.Status) {
		return store.ValidationErrorf("invalid proposal status %q", proposal.Status)
	}
	if proposal.Status == proposalStatusApplied && proposal.AppliedMemoryID == "" {
		return store.ValidationErrorf("invalid applied proposal without applied memory id")
	}
	now := time.Now()
	if proposal.CreatedAt.IsZero() {
		proposal.CreatedAt = now
	}
	if proposal.UpdatedAt.IsZero() {
		proposal.UpdatedAt = now
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO memory_proposals
		 (id, namespace, task_name, agent_name, type, skill_name, title, description, content, patch,
		  status, reviewer, review_note, applied_memory_id, applied_by, created_at, updated_at, reviewed_at, applied_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		proposal.ID, proposal.Namespace, proposal.TaskName, proposal.AgentName, proposal.Type, proposal.SkillName,
		proposal.Title, proposal.Description, proposal.Content, proposal.Patch, proposal.Status, proposal.Reviewer,
		proposal.ReviewNote, proposal.AppliedMemoryID, proposal.AppliedBy, proposal.CreatedAt, proposal.UpdatedAt,
		proposal.ReviewedAt, proposal.AppliedAt,
	)
	return err
}

// GetMemoryProposal fetches a proposal by ID within a namespace.
func (s *Store) GetMemoryProposal(ctx context.Context, namespace, id string) (*store.MemoryProposal, error) {
	proposal, err := scanMemoryProposal(s.db.QueryRowContext(ctx, selectMemoryProposalSQL()+` WHERE namespace = ? AND id = ?`, namespace, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return proposal, nil
}

// ListMemoryProposals lists proposals for governance review.
func (s *Store) ListMemoryProposals(ctx context.Context, filter store.MemoryProposalFilter) ([]store.MemoryProposal, error) {
	if strings.TrimSpace(filter.Namespace) == "" {
		return nil, store.ValidationErrorf("namespace is required")
	}

	query := selectMemoryProposalSQL() + ` WHERE namespace = ?`
	args := []any{filter.Namespace}
	if filter.TaskName != "" {
		query += ` AND task_name = ?`
		args = append(args, filter.TaskName)
	}
	if filter.AgentName != "" {
		query += ` AND agent_name = ?`
		args = append(args, filter.AgentName)
	}
	if filter.Type != "" {
		query += ` AND type = ?`
		args = append(args, filter.Type)
	}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		like := "%" + strings.ToLower(q) + "%"
		query += ` AND (lower(title) LIKE ? OR lower(description) LIKE ? OR lower(content) LIKE ? OR lower(skill_name) LIKE ?)`
		args = append(args, like, like, like, like)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, boundedLimit(filter.Limit, defaultProposalLimit, maxProposalLimit))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var proposals []store.MemoryProposal
	for rows.Next() {
		proposal, err := scanMemoryProposal(rows)
		if err != nil {
			return nil, err
		}
		proposals = append(proposals, *proposal)
	}
	return proposals, rows.Err()
}

// ReviewMemoryProposal records a reviewer decision. It never applies the proposal automatically.
func (s *Store) ReviewMemoryProposal(ctx context.Context, review store.MemoryProposalReview) error {
	review.Namespace = strings.TrimSpace(review.Namespace)
	review.ID = strings.TrimSpace(review.ID)
	if review.Namespace == "" || review.ID == "" {
		return store.ValidationErrorf("namespace and id are required")
	}
	review.Status = normalizeProposalStatus(review.Status)
	if !isReviewDecisionStatus(review.Status) {
		return store.ValidationErrorf("proposal review status must be accepted or rejected")
	}

	proposal, err := s.GetMemoryProposal(ctx, review.Namespace, review.ID)
	if err != nil {
		return err
	}
	if normalizeProposalStatus(proposal.Status) != proposalStatusPending {
		return store.ValidationErrorf("proposal status %q cannot be reviewed", proposal.Status)
	}
	if proposal.AppliedMemoryID != "" {
		return store.ValidationErrorf("applied proposal cannot be reviewed")
	}

	review.Reviewer = redact.SensitiveText(review.Reviewer)
	review.ReviewNote = redact.SensitiveText(review.ReviewNote)
	res, err := s.db.ExecContext(ctx,
		`UPDATE memory_proposals
		 SET status = ?, reviewer = ?, review_note = ?, reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		 WHERE namespace = ? AND id = ? AND status = ?`,
		review.Status, review.Reviewer, review.ReviewNote, review.Namespace, review.ID, proposalStatusPending,
	)
	if err != nil {
		return err
	}
	return ensureRowsAffected(res)
}

// ArchiveMemoryProposal archives a proposal without applying it.
func (s *Store) ArchiveMemoryProposal(ctx context.Context, namespace, id string) error {
	namespace = strings.TrimSpace(namespace)
	id = strings.TrimSpace(id)
	if namespace == "" || id == "" {
		return store.ValidationErrorf("namespace and id are required")
	}
	proposal, err := s.GetMemoryProposal(ctx, namespace, id)
	if err != nil {
		return err
	}
	if normalizeProposalStatus(proposal.Status) == proposalStatusApplied || proposal.AppliedMemoryID != "" {
		return store.ValidationErrorf("applied proposal cannot be archived")
	}
	if normalizeProposalStatus(proposal.Status) == proposalStatusArchived {
		return nil
	}
	if s.archiveMemoryProposalAfterActiveRead != nil {
		s.archiveMemoryProposalAfterActiveRead()
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE memory_proposals
		 SET status = ?, updated_at = CURRENT_TIMESTAMP
		 WHERE namespace = ? AND id = ? AND status != ? AND applied_memory_id = ''`,
		proposalStatusArchived, namespace, id, proposalStatusApplied,
	)
	if err != nil {
		return err
	}
	if err := ensureRowsAffected(res); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: proposal changed before archive", store.ErrConflict)
		}
		return err
	}
	return nil
}

// ApplyMemoryProposal applies an accepted memory proposal into durable memories.
func (s *Store) ApplyMemoryProposal(ctx context.Context, apply store.MemoryProposalApply) (*store.Memory, error) {
	apply.Namespace = strings.TrimSpace(apply.Namespace)
	apply.ID = strings.TrimSpace(apply.ID)
	if apply.Namespace == "" || apply.ID == "" {
		return nil, store.ValidationErrorf("namespace and id are required")
	}
	apply.AppliedBy = redact.SensitiveText(strings.TrimSpace(apply.AppliedBy))

	const maxAttempts = 5
	retryBackoffs := [...]time.Duration{
		50 * time.Millisecond,
		150 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}
	var lastErr error
	for attempt := range maxAttempts {
		memory, err := s.applyMemoryProposalOnce(ctx, apply)
		if err == nil {
			return memory, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !isSQLiteRetryableError(err) {
			return nil, err
		}
		lastErr = err
		if attempt == maxAttempts-1 {
			break
		}
		backoff := retryBackoffs[attempt]
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func (s *Store) applyMemoryProposalOnce(ctx context.Context, apply store.MemoryProposalApply) (*store.Memory, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	proposal, err := scanMemoryProposal(tx.QueryRowContext(ctx, selectMemoryProposalSQL()+` WHERE namespace = ? AND id = ?`, apply.Namespace, apply.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	status := normalizeProposalStatus(proposal.Status)
	if status == proposalStatusApplied || proposal.AppliedMemoryID != "" {
		if proposal.AppliedMemoryID == "" {
			return nil, fmt.Errorf("applied proposal is missing applied memory id")
		}
		if err := authorizeExistingProposalMemory(ctx, apply, proposal.Namespace, proposal.AppliedMemoryID); err != nil {
			return nil, err
		}
		memory, err := scanMemory(tx.QueryRowContext(ctx, selectMemorySQL()+` WHERE namespace = ? AND id = ?`, proposal.Namespace, proposal.AppliedMemoryID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("applied proposal references missing memory")
		}
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		committed = true
		return memory, nil
	}

	if strings.ToLower(strings.TrimSpace(proposal.Type)) != proposalTypeMemory {
		return nil, store.ValidationErrorf("proposal type %q cannot be applied as memory", proposal.Type)
	}
	if status != proposalStatusAccepted {
		return nil, store.ValidationErrorf("proposal status %q cannot be applied", proposal.Status)
	}
	if hook := s.applyMemoryProposalAfterAcceptedRead; hook != nil {
		hook()
	}

	existing, err := scanMemory(tx.QueryRowContext(ctx, selectMemorySQL()+` WHERE namespace = ? AND source_proposal_id = ?`, apply.Namespace, apply.ID))
	if err == nil {
		if err := authorizeExistingProposalMemory(ctx, apply, existing.Namespace, existing.ID); err != nil {
			return nil, err
		}
		now := time.Now()
		res, err := tx.ExecContext(ctx,
			`UPDATE memory_proposals
			 SET status = ?, applied_memory_id = ?, applied_by = ?, applied_at = COALESCE(applied_at, ?), updated_at = ?
			 WHERE namespace = ? AND id = ? AND status = ? AND (applied_memory_id = '' OR applied_memory_id = ?)`,
			proposalStatusApplied, existing.ID, apply.AppliedBy, now, now, apply.Namespace, apply.ID, proposalStatusAccepted, existing.ID,
		)
		if err != nil {
			return nil, err
		}
		if err := ensureRowsAffectedConflict(res); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		committed = true
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	now := time.Now()
	memory := &store.Memory{
		ID:               "mem-" + uuid.NewString(),
		Namespace:        proposal.Namespace,
		AgentName:        proposal.AgentName,
		TaskName:         proposal.TaskName,
		Source:           memorySourceProposal,
		SourceProposalID: proposal.ID,
		Content:          redact.SensitiveText(proposal.Content),
		Tags:             tagsFromProposalDescription(proposal.Description),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if strings.TrimSpace(memory.Content) == "" {
		return nil, store.ValidationErrorf("content is required")
	}
	tagsJSON, err := marshalTags(memory.Tags)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memories
		 (id, namespace, session_name, agent_name, task_name, parent_task, source, source_proposal_id, content, tags_json,
		  disabled, deleted, created_at, updated_at, last_recalled_at, recalled_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		memory.ID, memory.Namespace, memory.SessionName, memory.AgentName, memory.TaskName, memory.ParentTask,
		memory.Source, memory.SourceProposalID, memory.Content, tagsJSON, memory.Disabled, memory.Deleted, memory.CreatedAt,
		memory.UpdatedAt, memory.LastRecalledAt, memory.RecalledCount,
	); err != nil {
		if !isSQLiteConstraintError(err) {
			return nil, err
		}
		existing, lookupErr := scanMemory(tx.QueryRowContext(ctx, selectMemorySQL()+` WHERE namespace = ? AND source_proposal_id = ?`, apply.Namespace, apply.ID))
		if lookupErr != nil {
			return nil, err
		}
		if err := authorizeExistingProposalMemory(ctx, apply, existing.Namespace, existing.ID); err != nil {
			return nil, err
		}
		if err := markMemoryProposalApplied(ctx, tx, apply, existing.ID, true); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		committed = true
		return existing, nil
	}
	if err := markMemoryProposalApplied(ctx, tx, apply, memory.ID, false); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return memory, nil
}

func authorizeExistingProposalMemory(ctx context.Context, apply store.MemoryProposalApply, namespace, id string) error {
	if apply.AuthorizeExistingMemory == nil {
		return nil
	}
	return apply.AuthorizeExistingMemory(ctx, namespace, id)
}

func markMemoryProposalApplied(ctx context.Context, tx *sql.Tx, apply store.MemoryProposalApply, memoryID string, allowExistingAppliedID bool) error {
	now := time.Now()
	query := `UPDATE memory_proposals
		 SET status = ?, applied_memory_id = ?, applied_by = ?, applied_at = ?, updated_at = ?
		 WHERE namespace = ? AND id = ? AND status = ? AND applied_memory_id = ''`
	args := []any{proposalStatusApplied, memoryID, apply.AppliedBy, now, now, apply.Namespace, apply.ID, proposalStatusAccepted}
	if allowExistingAppliedID {
		query = `UPDATE memory_proposals
		 SET status = ?, applied_memory_id = ?, applied_by = ?, applied_at = COALESCE(applied_at, ?), updated_at = ?
		 WHERE namespace = ? AND id = ? AND status = ? AND (applied_memory_id = '' OR applied_memory_id = ?)`
		args = append(args, memoryID)
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return ensureRowsAffectedConflict(res)
}

func ensureRowsAffectedConflict(res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: memory proposal changed during apply", store.ErrConflict)
	}
	return nil
}

func isSQLiteRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if code, ok := sqliteErrorCode(err); ok {
		switch primarySQLiteCode(code) {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
			return true
		}
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "sqlite_busy") ||
		strings.Contains(msg, "sqlite_locked") ||
		strings.Contains(msg, "busy snapshot")
}

func isSQLiteConstraintError(err error) bool {
	if err == nil {
		return false
	}
	if code, ok := sqliteErrorCode(err); ok {
		return primarySQLiteCode(code) == sqlite3.SQLITE_CONSTRAINT
	}
	return strings.Contains(strings.ToLower(err.Error()), "constraint failed")
}

func sqliteErrorCode(err error) (int, bool) {
	if sqliteErr, ok := errors.AsType[*moderncsqlite.Error](err); ok {
		return sqliteErr.Code(), true
	}
	return 0, false
}

func primarySQLiteCode(code int) int {
	return code & 0xff
}

func selectMemorySQL() string {
	return `SELECT id, namespace, session_name, agent_name, task_name, parent_task, source, source_proposal_id, content, tags_json,
		disabled, deleted, created_at, updated_at, last_recalled_at, recalled_count FROM memories`
}

func appendMemoryFilters(query string, args *[]any, filter store.MemoryFilter) string {
	if !filter.IncludeDisabled {
		query += ` AND disabled = FALSE`
	}
	if !filter.IncludeDeleted {
		query += ` AND deleted = FALSE`
	}
	if filter.SessionName != "" {
		query += ` AND session_name = ?`
		*args = append(*args, filter.SessionName)
	}
	if filter.AgentName != "" {
		query += ` AND agent_name = ?`
		*args = append(*args, filter.AgentName)
	}
	if filter.TaskName != "" {
		query += ` AND task_name = ?`
		*args = append(*args, filter.TaskName)
	}
	if filter.ParentTask != "" {
		query += ` AND parent_task = ?`
		*args = append(*args, filter.ParentTask)
	}
	if filter.Source != "" {
		query += ` AND source = ?`
		*args = append(*args, filter.Source)
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		like := "%" + strings.ToLower(q) + "%"
		query += ` AND (lower(content) LIKE ? OR lower(tags_json) LIKE ?)`
		*args = append(*args, like, like)
	}
	for _, tag := range compactStrings(filter.Tags) {
		query += ` AND lower(tags_json) LIKE ?`
		*args = append(*args, "%\""+strings.ToLower(tag)+"\"%")
	}
	ids := compactStrings(filter.IDs)
	if len(ids) > 0 {
		placeholders := make([]string, 0, len(ids))
		for _, id := range ids {
			placeholders = append(placeholders, "?")
			*args = append(*args, id)
		}
		query += ` AND id IN (` + strings.Join(placeholders, ",") + `)`
	}
	return query
}

func scanMemory(scanner rowScanner) (*store.Memory, error) {
	var memory store.Memory
	var tagsJSON string
	var lastRecalled sql.NullTime
	err := scanner.Scan(
		&memory.ID, &memory.Namespace, &memory.SessionName, &memory.AgentName, &memory.TaskName, &memory.ParentTask,
		&memory.Source, &memory.SourceProposalID, &memory.Content, &tagsJSON, &memory.Disabled, &memory.Deleted,
		&memory.CreatedAt, &memory.UpdatedAt, &lastRecalled, &memory.RecalledCount,
	)
	if err != nil {
		return nil, err
	}
	if lastRecalled.Valid {
		memory.LastRecalledAt = &lastRecalled.Time
	}
	if tagsJSON != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &memory.Tags); err != nil {
			return nil, fmt.Errorf("failed to unmarshal memory tags: %w", err)
		}
	}
	return &memory, nil
}

func selectMemoryProposalSQL() string {
	return `SELECT id, namespace, task_name, agent_name, type, skill_name, title, description, content, patch,
		status, reviewer, review_note, applied_memory_id, applied_by, created_at, updated_at, reviewed_at, applied_at
		FROM memory_proposals`
}

func scanMemoryProposal(scanner rowScanner) (*store.MemoryProposal, error) {
	var proposal store.MemoryProposal
	var reviewedAt, appliedAt sql.NullTime
	err := scanner.Scan(
		&proposal.ID, &proposal.Namespace, &proposal.TaskName, &proposal.AgentName, &proposal.Type, &proposal.SkillName,
		&proposal.Title, &proposal.Description, &proposal.Content, &proposal.Patch, &proposal.Status, &proposal.Reviewer,
		&proposal.ReviewNote, &proposal.AppliedMemoryID, &proposal.AppliedBy, &proposal.CreatedAt, &proposal.UpdatedAt,
		&reviewedAt, &appliedAt,
	)
	if err != nil {
		return nil, err
	}
	if reviewedAt.Valid {
		proposal.ReviewedAt = &reviewedAt.Time
	}
	if appliedAt.Valid {
		proposal.AppliedAt = &appliedAt.Time
	}
	return &proposal, nil
}

func normalizeProposalStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func isKnownProposalStatus(status string) bool {
	switch normalizeProposalStatus(status) {
	case proposalStatusPending, proposalStatusAccepted, proposalStatusRejected, proposalStatusArchived, proposalStatusApplied:
		return true
	default:
		return false
	}
}

func isReviewDecisionStatus(status string) bool {
	switch normalizeProposalStatus(status) {
	case proposalStatusAccepted, proposalStatusRejected:
		return true
	default:
		return false
	}
}

func tagsFromProposalDescription(description string) []string {
	for line := range strings.SplitSeq(description, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "tags") {
			continue
		}

		var tags []string
		for tag := range strings.SplitSeq(value, ",") {
			tags = append(tags, strings.TrimSpace(tag))
		}
		return normalizeTags(tags)
	}
	return nil
}

func marshalTags(tags []string) (string, error) {
	tags = normalizeTags(tags)
	b, err := json.Marshal(tags)
	if err != nil {
		return "", fmt.Errorf("failed to marshal memory tags: %w", err)
	}
	return string(b), nil
}

func normalizeTags(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func transcriptSearchTerms(query string) []string {
	var terms []string
	var b strings.Builder
	inQuote := rune(0)

	flush := func() {
		term := strings.TrimSpace(b.String())
		if term != "" {
			terms = append(terms, term)
		}
		b.Reset()
	}

	for _, r := range query {
		if inQuote != 0 {
			if r == inQuote {
				flush()
				inQuote = 0
				continue
			}
			b.WriteRune(r)
			continue
		}

		switch {
		case r == '\'' || r == '"':
			flush()
			inQuote = r
		case unicode.IsSpace(r):
			flush()
		default:
			b.WriteRune(r)
		}
	}
	flush()
	return compactStrings(terms)
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func boundedLimit(limit, def, max int) int {
	if limit <= 0 {
		return def
	}
	if limit > max {
		return max
	}
	return limit
}

func ensureRowsAffected(res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrNotFound
	}
	return nil
}

func buildSnippet(content, term string, maxLen int) string {
	content = collapseWhitespace(content)
	if maxLen <= 0 || len([]rune(content)) <= maxLen {
		return content
	}

	idx := -1
	lowerContent := strings.ToLower(content)
	term = strings.ToLower(strings.TrimSpace(term))
	if term != "" {
		idx = strings.Index(lowerContent, term)
		if idx < 0 {
			for token := range strings.FieldsSeq(term) {
				if len(token) < 3 {
					continue
				}
				idx = strings.Index(lowerContent, token)
				if idx >= 0 {
					break
				}
			}
		}
	}

	runes := []rune(content)
	if idx < 0 {
		return string(runes[:maxLen]) + "…"
	}

	center := len([]rune(content[:idx]))
	start := max(center-maxLen/3, 0)
	end := start + maxLen
	if end > len(runes) {
		end = len(runes)
		start = max(end-maxLen, 0)
	}
	snippet := string(runes[start:end])
	if start > 0 {
		snippet = "…" + snippet
	}
	if end < len(runes) {
		snippet += "…"
	}
	return snippet
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
