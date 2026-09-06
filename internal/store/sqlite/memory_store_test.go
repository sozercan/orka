package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
	sqlite3 "modernc.org/sqlite/lib"
)

const memoryTestTaskA = "task-a"

func TestMemoryStore(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	memory := &store.Memory{
		Namespace:   "ns-mem",
		SessionName: "session-a",
		AgentName:   "agent-a",
		TaskName:    memoryTestTaskA,
		ParentTask:  "parent-a",
		Source:      "remember_tool",
		Content:     "Prefer Postgres migrations for durable storage work.",
		Tags:        []string{"storage", "durability", "storage"},
	}
	if err := s.CreateMemory(ctx, memory); err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	if memory.ID == "" {
		t.Fatalf("CreateMemory did not assign ID")
	}

	got, err := s.GetMemory(ctx, "ns-mem", memory.ID)
	if err != nil {
		t.Fatalf("GetMemory: %v", err)
	}
	if got.Content != memory.Content || got.Namespace != "ns-mem" || got.Source != "remember_tool" {
		t.Fatalf("unexpected memory: %+v", got)
	}
	if len(got.Tags) != 2 {
		t.Fatalf("expected compacted tags, got %+v", got.Tags)
	}

	listed, err := s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-mem", Query: "postgres", Tags: []string{"storage"}})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != memory.ID {
		t.Fatalf("ListMemories = %+v, want created memory", listed)
	}

	if err := s.SetMemoryDisabled(ctx, "ns-mem", memory.ID, true); err != nil {
		t.Fatalf("SetMemoryDisabled: %v", err)
	}
	listed, err = s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-mem", Query: "postgres"})
	if err != nil {
		t.Fatalf("ListMemories disabled hidden: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("disabled memory should be hidden, got %+v", listed)
	}
	listed, err = s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-mem", Query: "postgres", IncludeDisabled: true})
	if err != nil {
		t.Fatalf("ListMemories include disabled: %v", err)
	}
	if len(listed) != 1 || !listed[0].Disabled {
		t.Fatalf("expected disabled memory when included, got %+v", listed)
	}

	if err := s.DeleteMemory(ctx, "ns-mem", memory.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	listed, err = s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-mem", IncludeDisabled: true})
	if err != nil {
		t.Fatalf("ListMemories after delete: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("deleted memory should be hidden, got %+v", listed)
	}
	listed, err = s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-mem", IncludeDisabled: true, IncludeDeleted: true})
	if err != nil {
		t.Fatalf("ListMemories include deleted: %v", err)
	}
	if len(listed) != 1 || !listed[0].Deleted {
		t.Fatalf("expected soft-deleted memory when included, got %+v", listed)
	}

	if err := s.DeleteMemory(ctx, "ns-mem", "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteMemory missing error = %v, want ErrNotFound", err)
	}
}

func TestMemoryProposalStore(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	proposal := &store.MemoryProposal{
		Namespace:   "ns-prop",
		TaskName:    memoryTestTaskA,
		AgentName:   "agent-a",
		Type:        "skill",
		SkillName:   "sqlite-memory",
		Title:       "Add SQLite memory helper",
		Description: "Capture a reusable SQLite migration pattern.",
		Content:     "When adding store tables, keep migrations idempotent and covered by tests.",
		Patch:       "diff --git a/skills/sqlite.md b/skills/sqlite.md",
	}
	if err := s.CreateMemoryProposal(ctx, proposal); err != nil {
		t.Fatalf("CreateMemoryProposal: %v", err)
	}
	if proposal.ID == "" {
		t.Fatalf("CreateMemoryProposal did not assign ID")
	}

	listed, err := s.ListMemoryProposals(ctx, store.MemoryProposalFilter{Namespace: "ns-prop", Status: "pending", Query: "sqlite"})
	if err != nil {
		t.Fatalf("ListMemoryProposals: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != proposal.ID || listed[0].Status != proposalStatusPending {
		t.Fatalf("unexpected proposals: %+v", listed)
	}

	if err := s.ReviewMemoryProposal(ctx, store.MemoryProposalReview{
		Namespace:  "ns-prop",
		ID:         proposal.ID,
		Status:     "accepted",
		Reviewer:   "maintainer",
		ReviewNote: "Looks useful.",
	}); err != nil {
		t.Fatalf("ReviewMemoryProposal: %v", err)
	}
	got, err := s.GetMemoryProposal(ctx, "ns-prop", proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal: %v", err)
	}
	if got.Status != "accepted" || got.Reviewer != "maintainer" || got.ReviewedAt == nil {
		t.Fatalf("review not persisted: %+v", got)
	}

	if err := s.ArchiveMemoryProposal(ctx, "ns-prop", proposal.ID); err != nil {
		t.Fatalf("ArchiveMemoryProposal: %v", err)
	}
	got, err = s.GetMemoryProposal(ctx, "ns-prop", proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal archived: %v", err)
	}
	if got.Status != "archived" {
		t.Fatalf("archive status = %q, want archived", got.Status)
	}

	if _, err := s.GetMemoryProposal(ctx, "ns-prop", "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetMemoryProposal missing error = %v, want ErrNotFound", err)
	}
}

func setupDiskStorePair(t *testing.T) (*Store, *Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	openStore := func(label string) *Store {
		t.Helper()
		db, err := NewDB(dbPath)
		if err != nil {
			t.Fatalf("NewDB %s: %v", label, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return NewStore(db, dbPath)
	}
	return openStore("first"), openStore("second")
}

func TestApplyMemoryProposal(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	proposal := &store.MemoryProposal{
		Namespace:   "ns-apply",
		TaskName:    memoryTestTaskA,
		AgentName:   "agent-a",
		Type:        "memory",
		Title:       "Prefer explicit migrations",
		Description: "Store migration guidance.\n\nTags: Storage, sqlite, storage",
		Content:     "Keep SQLite memory migrations idempotent and covered by tests.",
	}
	if err := s.CreateMemoryProposal(ctx, proposal); err != nil {
		t.Fatalf("CreateMemoryProposal: %v", err)
	}
	if err := s.ReviewMemoryProposal(ctx, store.MemoryProposalReview{
		Namespace: "ns-apply",
		ID:        proposal.ID,
		Status:    "accepted",
		Reviewer:  "maintainer",
	}); err != nil {
		t.Fatalf("ReviewMemoryProposal: %v", err)
	}

	memory, err := s.ApplyMemoryProposal(ctx, store.MemoryProposalApply{
		Namespace: "ns-apply",
		ID:        proposal.ID,
		AppliedBy: "coordinator",
	})
	if err != nil {
		t.Fatalf("ApplyMemoryProposal: %v", err)
	}
	if memory.ID == "" || memory.Source != "memory_proposal" || memory.SourceProposalID != proposal.ID {
		t.Fatalf("unexpected applied memory provenance: %+v", memory)
	}
	if memory.Content != proposal.Content || memory.Namespace != "ns-apply" || memory.TaskName != memoryTestTaskA || memory.AgentName != "agent-a" {
		t.Fatalf("unexpected applied memory: %+v", memory)
	}
	if got, want := strings.Join(memory.Tags, ","), "storage,sqlite"; got != want {
		t.Fatalf("tags = %q, want %q", got, want)
	}

	updated, err := s.GetMemoryProposal(ctx, "ns-apply", proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal: %v", err)
	}
	if updated.Status != proposalStatusApplied || updated.AppliedMemoryID != memory.ID || updated.AppliedBy != "coordinator" || updated.AppliedAt == nil {
		t.Fatalf("proposal apply metadata not persisted: %+v", updated)
	}

	again, err := s.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: "ns-apply", ID: proposal.ID, AppliedBy: "other"})
	if err != nil {
		t.Fatalf("ApplyMemoryProposal second call: %v", err)
	}
	if again.ID != memory.ID {
		t.Fatalf("second apply memory id = %q, want %q", again.ID, memory.ID)
	}
	listed, err := s.ListMemories(ctx, store.MemoryFilter{Namespace: "ns-apply", Source: "memory_proposal"})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != memory.ID {
		t.Fatalf("expected exactly one applied memory, got %+v", listed)
	}
	if err := s.ArchiveMemoryProposal(ctx, "ns-apply", proposal.ID); err == nil {
		t.Fatalf("ArchiveMemoryProposal applied proposal succeeded, want error")
	}
}

func TestTagsFromProposalDescriptionUsesFirstTagsLine(t *testing.T) {
	got := tagsFromProposalDescription("Intro\nTags: Alpha, beta, alpha\nMore\nTags: ignored")
	if strings.Join(got, ",") != "alpha,beta" {
		t.Fatalf("tags = %q, want alpha,beta", strings.Join(got, ","))
	}
}

func TestIsSQLiteRetryableErrorUsesStructuredSQLiteCodesFirst(t *testing.T) {
	err := sqliteConstraintError(t)
	code, ok := sqliteErrorCode(err)
	if !ok {
		t.Fatalf("constraint error did not expose a structured sqlite code: %v", err)
	}
	if primarySQLiteCode(code) != sqlite3.SQLITE_CONSTRAINT {
		t.Fatalf("sqlite code = %d, want SQLITE_CONSTRAINT", primarySQLiteCode(code))
	}

	wrapped := fmt.Errorf("database is locked: %w", err)
	if isSQLiteRetryableError(wrapped) {
		t.Fatalf("structured non-retryable sqlite errors should not use substring fallback")
	}
	if !isSQLiteRetryableError(errors.New("database is locked")) {
		t.Fatalf("unstructured locked errors should use substring fallback")
	}
}

func sqliteConstraintError(t *testing.T) error {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("CREATE TABLE retry_code_test (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO retry_code_test (id) VALUES (1)"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	_, err = db.Exec("INSERT INTO retry_code_test (id) VALUES (1)")
	if err == nil {
		t.Fatalf("duplicate primary key insert succeeded, want constraint error")
	}
	return err
}

func TestApplyMemoryProposalConcurrentIdempotent(t *testing.T) {
	s1, s2 := setupDiskStorePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const ns = "ns-apply-concurrent"
	proposal := &store.MemoryProposal{
		Namespace: ns,
		TaskName:  "task-concurrent",
		Type:      "memory",
		Title:     "Concurrent proposal",
		Content:   "Only one durable memory should be created for concurrent applies.",
	}
	if err := s1.CreateMemoryProposal(ctx, proposal); err != nil {
		t.Fatalf("CreateMemoryProposal: %v", err)
	}
	if err := s1.ReviewMemoryProposal(ctx, store.MemoryProposalReview{Namespace: ns, ID: proposal.ID, Status: "accepted"}); err != nil {
		t.Fatalf("ReviewMemoryProposal: %v", err)
	}

	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)
	installHook := func(s *Store) {
		var once sync.Once
		s.applyMemoryProposalAfterAcceptedRead = func() {
			once.Do(func() {
				select {
				case ready <- struct{}{}:
				case <-ctx.Done():
					return
				}
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}
	installHook(s1)
	installHook(s2)

	type applyResult struct {
		name   string
		memory *store.Memory
		err    error
	}
	results := make(chan applyResult, 2)
	var wg sync.WaitGroup
	for name, s := range map[string]*Store{"first": s1, "second": s2} {
		wg.Go(func() {
			memory, err := s.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: ns, ID: proposal.ID, AppliedBy: name})
			results <- applyResult{name: name, memory: memory, err: err}
		})
	}
	for i := range 2 {
		select {
		case <-ready:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for apply %d to read accepted proposal", i+1)
		}
	}
	releaseAll()
	wg.Wait()
	close(results)

	var memoryID string
	for result := range results {
		if result.err != nil {
			t.Fatalf("%s ApplyMemoryProposal: %v", result.name, result.err)
		}
		if result.memory == nil || result.memory.ID == "" {
			t.Fatalf("%s returned empty memory: %+v", result.name, result.memory)
		}
		if memoryID == "" {
			memoryID = result.memory.ID
		} else if result.memory.ID != memoryID {
			t.Fatalf("concurrent applies returned different memories: %q and %q", memoryID, result.memory.ID)
		}
	}

	listed, err := s1.ListMemories(ctx, store.MemoryFilter{Namespace: ns, Source: "memory_proposal"})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != memoryID || listed[0].SourceProposalID != proposal.ID {
		t.Fatalf("expected exactly one applied memory %q for proposal %q, got %+v", memoryID, proposal.ID, listed)
	}
	updated, err := s2.GetMemoryProposal(ctx, ns, proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal: %v", err)
	}
	if updated.Status != proposalStatusApplied || updated.AppliedMemoryID != memoryID {
		t.Fatalf("proposal apply metadata = %+v, want status applied and memory %q", updated, memoryID)
	}
}

func TestApplyMemoryProposalDoesNotOverwriteConcurrentStatusChange(t *testing.T) {
	s1, s2 := setupDiskStorePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const ns = "ns-apply-stale-status"
	proposal := &store.MemoryProposal{
		Namespace: ns,
		TaskName:  "task-stale",
		Type:      "memory",
		Title:     "Stale apply proposal",
		Content:   "A stale apply must not overwrite an archived proposal status.",
	}
	if err := s1.CreateMemoryProposal(ctx, proposal); err != nil {
		t.Fatalf("CreateMemoryProposal: %v", err)
	}
	if err := s1.ReviewMemoryProposal(ctx, store.MemoryProposalReview{Namespace: ns, ID: proposal.ID, Status: "accepted"}); err != nil {
		t.Fatalf("ReviewMemoryProposal: %v", err)
	}

	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseApply := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseApply)
	var readyOnce sync.Once
	s1.applyMemoryProposalAfterAcceptedRead = func() {
		readyOnce.Do(func() {
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
	}

	type applyResult struct {
		memory *store.Memory
		err    error
	}
	resultCh := make(chan applyResult, 1)
	go func() {
		memory, err := s1.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: ns, ID: proposal.ID, AppliedBy: "stale"})
		resultCh <- applyResult{memory: memory, err: err}
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for apply to read accepted proposal")
	}

	if err := s2.ArchiveMemoryProposal(ctx, ns, proposal.ID); err != nil {
		t.Fatalf("ArchiveMemoryProposal: %v", err)
	}
	releaseApply()

	var result applyResult
	select {
	case result = <-resultCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for stale apply result")
	}
	if result.err == nil {
		t.Fatalf("stale ApplyMemoryProposal returned memory %+v, want error", result.memory)
	}
	lowerErr := strings.ToLower(result.err.Error())
	if strings.Contains(lowerErr, "database is locked") || strings.Contains(lowerErr, "sqlite_busy") || strings.Contains(lowerErr, "sqlite_locked") {
		t.Fatalf("stale ApplyMemoryProposal returned raw SQLite concurrency error: %v", result.err)
	}
	if !errors.Is(result.err, store.ErrConflict) && !strings.Contains(result.err.Error(), "cannot be applied") {
		t.Fatalf("stale ApplyMemoryProposal error = %v, want conflict or cannot be applied", result.err)
	}

	updated, err := s1.GetMemoryProposal(ctx, ns, proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal: %v", err)
	}
	if updated.Status != "archived" || updated.AppliedMemoryID != "" {
		t.Fatalf("proposal after stale apply = %+v, want archived with no applied memory", updated)
	}
	listed, err := s2.ListMemories(ctx, store.MemoryFilter{Namespace: ns, Source: "memory_proposal"})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("stale apply created memories: %+v", listed)
	}
}

func TestArchiveMemoryProposalDoesNotOverwriteConcurrentApply(t *testing.T) {
	s1, s2 := setupDiskStorePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const ns = "ns-archive-stale-apply"
	proposal := &store.MemoryProposal{
		Namespace: ns,
		TaskName:  "task-archive-stale",
		Type:      "memory",
		Title:     "Stale archive proposal",
		Content:   "A stale archive must not overwrite an applied proposal status.",
	}
	if err := s1.CreateMemoryProposal(ctx, proposal); err != nil {
		t.Fatalf("CreateMemoryProposal: %v", err)
	}
	if err := s1.ReviewMemoryProposal(ctx, store.MemoryProposalReview{Namespace: ns, ID: proposal.ID, Status: "accepted"}); err != nil {
		t.Fatalf("ReviewMemoryProposal: %v", err)
	}

	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseArchive := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseArchive)
	var readyOnce sync.Once
	s1.archiveMemoryProposalAfterActiveRead = func() {
		readyOnce.Do(func() {
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
	}

	archiveCh := make(chan error, 1)
	go func() {
		archiveCh <- s1.ArchiveMemoryProposal(ctx, ns, proposal.ID)
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for archive to read accepted proposal")
	}

	memory, err := s2.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: ns, ID: proposal.ID, AppliedBy: "winner"})
	if err != nil {
		t.Fatalf("ApplyMemoryProposal: %v", err)
	}
	if memory == nil || memory.ID == "" {
		t.Fatalf("ApplyMemoryProposal returned empty memory: %+v", memory)
	}
	releaseArchive()

	var archiveErr error
	select {
	case archiveErr = <-archiveCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for stale archive result")
	}
	if archiveErr == nil {
		t.Fatalf("stale ArchiveMemoryProposal succeeded, want error")
	}
	lowerErr := strings.ToLower(archiveErr.Error())
	if strings.Contains(lowerErr, "database is locked") || strings.Contains(lowerErr, "sqlite_busy") || strings.Contains(lowerErr, "sqlite_locked") {
		t.Fatalf("stale ArchiveMemoryProposal returned raw SQLite concurrency error: %v", archiveErr)
	}
	if !errors.Is(archiveErr, store.ErrConflict) && !strings.Contains(lowerErr, "changed") {
		t.Fatalf("stale ArchiveMemoryProposal error = %v, want conflict", archiveErr)
	}

	updated, err := s1.GetMemoryProposal(ctx, ns, proposal.ID)
	if err != nil {
		t.Fatalf("GetMemoryProposal: %v", err)
	}
	if updated.Status != proposalStatusApplied || updated.AppliedMemoryID != memory.ID {
		t.Fatalf("proposal after stale archive = %+v, want applied with memory %q", updated, memory.ID)
	}
	listed, err := s2.ListMemories(ctx, store.MemoryFilter{Namespace: ns, Source: "memory_proposal"})
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != memory.ID || listed[0].SourceProposalID != proposal.ID {
		t.Fatalf("expected exactly one applied memory %q for proposal %q, got %+v", memory.ID, proposal.ID, listed)
	}
}

func TestApplyMemoryProposalValidation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	pending := &store.MemoryProposal{
		Namespace: "ns-apply-validation",
		Type:      "memory",
		Title:     "Pending memory",
		Content:   "Only accepted proposals should apply.",
	}
	if err := s.CreateMemoryProposal(ctx, pending); err != nil {
		t.Fatalf("CreateMemoryProposal pending: %v", err)
	}
	if _, err := s.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: pending.Namespace, ID: pending.ID}); err == nil || !strings.Contains(err.Error(), "cannot be applied") {
		t.Fatalf("ApplyMemoryProposal pending error = %v, want cannot be applied", err)
	}

	skill := &store.MemoryProposal{
		Namespace: "ns-apply-validation",
		Type:      "skill",
		Title:     "Skill proposal",
		Content:   "Skill content should not become durable memory.",
	}
	if err := s.CreateMemoryProposal(ctx, skill); err != nil {
		t.Fatalf("CreateMemoryProposal skill: %v", err)
	}
	if err := s.ReviewMemoryProposal(ctx, store.MemoryProposalReview{Namespace: skill.Namespace, ID: skill.ID, Status: "accepted"}); err != nil {
		t.Fatalf("ReviewMemoryProposal skill: %v", err)
	}
	if _, err := s.ApplyMemoryProposal(ctx, store.MemoryProposalApply{Namespace: skill.Namespace, ID: skill.ID}); err == nil || !strings.Contains(err.Error(), "cannot be applied as memory") {
		t.Fatalf("ApplyMemoryProposal skill error = %v, want cannot be applied as memory", err)
	}
}

func TestTranscriptSearch(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	for _, name := range []string{"prior", "current"} {
		if err := s.CreateSession(ctx, &store.SessionRecord{
			Namespace:   "ns-transcript",
			Name:        name,
			SessionType: "task",
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			t.Fatalf("CreateSession %s: %v", name, err)
		}
	}
	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns-transcript", Name: "gateway-private", SessionType: store.SessionTypeGateway,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession gateway-private: %v", err)
	}

	priorLong := strings.Repeat("prefix ", 80) + "needle migration details live here" + strings.Repeat(" suffix", 80)
	if err := s.AppendMessages(ctx, "ns-transcript", "prior", []store.SessionMessage{
		{Role: "user", Content: "unrelated setup", Timestamp: now},
		{Role: "assistant", Content: priorLong, Timestamp: now.Add(time.Second)},
	}); err != nil {
		t.Fatalf("AppendMessages prior: %v", err)
	}
	if err := s.AppendMessages(ctx, "ns-transcript", "current", []store.SessionMessage{
		{Role: "assistant", Content: "needle from the current active session should be excluded", Timestamp: now.Add(2 * time.Second)},
	}); err != nil {
		t.Fatalf("AppendMessages current: %v", err)
	}
	if err := s.AppendMessages(ctx, "ns-transcript", "gateway-private", []store.SessionMessage{
		{Role: "user", Content: "needle from another gateway conversation must never be searchable", Timestamp: now.Add(3 * time.Second)},
	}); err != nil {
		t.Fatalf("AppendMessages gateway-private: %v", err)
	}

	results, err := s.SearchTranscript(ctx, store.TranscriptSearchFilter{
		Namespace:          "ns-transcript",
		Query:              "needle",
		ExcludeSessionName: "current",
		Limit:              5,
		MaxSnippetLength:   90,
	})
	if err != nil {
		t.Fatalf("SearchTranscript: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1: %+v", len(results), results)
	}
	if results[0].SessionName != "prior" || results[0].Role != "assistant" {
		t.Fatalf("unexpected result: %+v", results[0])
	}
	if !strings.Contains(results[0].Snippet, "needle") {
		t.Fatalf("snippet missing search term: %q", results[0].Snippet)
	}
	if len([]rune(results[0].Snippet)) > 92 { // allow ellipsis on both sides
		t.Fatalf("snippet too long: %d %q", len([]rune(results[0].Snippet)), results[0].Snippet)
	}
}
