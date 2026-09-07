package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

// setupDiskStore creates a Store backed by a real on-disk SQLite file.
func setupDiskStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB(%s) failed: %v", dbPath, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, dbPath)
}

func TestIntegration_DiskPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	ctx := context.Background()

	// Open, write, close
	db1, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	s1 := NewStore(db1, dbPath)

	if err := s1.SaveResult(ctx, "ns", "task1", []byte("persisted-data")); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	if err := s1.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "sess1", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := s1.AppendMessages(ctx, "ns", "sess1", []store.SessionMessage{
		{Role: "user", Content: "persisted-msg", Timestamp: now},
	}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	_ = db1.Close()

	// Reopen and verify
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB reopen: %v", err)
	}
	defer db2.Close() //nolint:errcheck
	s2 := NewStore(db2, dbPath)

	got, err := s2.GetResult(ctx, "ns", "task1")
	if err != nil {
		t.Fatalf("GetResult after reopen: %v", err)
	}
	if string(got) != "persisted-data" {
		t.Errorf("result = %q, want %q", got, "persisted-data")
	}

	msgs, err := s2.LoadTranscript(ctx, "ns", "sess1", 0)
	if err != nil {
		t.Fatalf("LoadTranscript after reopen: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Content != "persisted-msg" {
		t.Errorf("transcript = %+v, want 1 message with 'persisted-msg'", msgs)
	}
}

func TestIntegration_LargeResult(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()

	// 5MB result — well beyond the old 1MB ConfigMap limit
	data := make([]byte, 5*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := s.SaveResult(ctx, "ns", "big-task", data); err != nil {
		t.Fatalf("SaveResult 5MB: %v", err)
	}

	got, err := s.GetResult(ctx, "ns", "big-task")
	if err != nil {
		t.Fatalf("GetResult 5MB: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("5MB result data mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestIntegration_LargeSessionTranscript(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "big-session", SessionType: "chat", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Append 500 messages in batches
	const totalMessages = 500
	const batchSize = 50
	for batch := range totalMessages / batchSize {
		msgs := make([]store.SessionMessage, batchSize)
		for i := range msgs {
			idx := batch*batchSize + i
			role := "user"
			if idx%2 == 1 {
				role = roleAssistant
			}
			msgs[i] = store.SessionMessage{
				Role:      role,
				Content:   fmt.Sprintf("Message %d with some content to make it realistic", idx),
				Timestamp: now.Add(time.Duration(idx) * time.Second),
			}
		}
		if err := s.AppendMessages(ctx, "ns", "big-session", msgs); err != nil {
			t.Fatalf("AppendMessages batch %d: %v", batch, err)
		}
	}

	// Verify count
	sess, err := s.GetSession(ctx, "ns", "big-session")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.MessageCount != totalMessages {
		t.Errorf("MessageCount = %d, want %d", sess.MessageCount, totalMessages)
	}

	// Verify all messages load
	msgs, err := s.LoadTranscript(ctx, "ns", "big-session", 0)
	if err != nil {
		t.Fatalf("LoadTranscript: %v", err)
	}
	if len(msgs) != totalMessages {
		t.Errorf("got %d messages, want %d", len(msgs), totalMessages)
	}

	// Verify ordering
	for i := 1; i < len(msgs); i++ {
		if !msgs[i].Timestamp.After(msgs[i-1].Timestamp) && msgs[i].Timestamp != msgs[i-1].Timestamp {
			t.Errorf("messages not ordered at index %d", i)
			break
		}
	}

	// Test pagination with limit
	page, err := s.LoadTranscript(ctx, "ns", "big-session", 10)
	if err != nil {
		t.Fatalf("LoadTranscript with limit: %v", err)
	}
	if len(page) != 10 {
		t.Errorf("got %d messages with limit 10, want 10", len(page))
	}
	if page[0].Content != "Message 490 with some content to make it realistic" {
		t.Errorf("first limited message = %q", page[0].Content)
	}
	if page[len(page)-1].Content != "Message 499 with some content to make it realistic" {
		t.Errorf("last limited message = %q", page[len(page)-1].Content)
	}
}

func TestIntegration_ConcurrentReadWrite(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	// Create a session for concurrent message appends
	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "concurrent-sess", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	const writers = 10
	const readsPerWriter = 5
	var wg sync.WaitGroup

	// Concurrent writers for results
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range readsPerWriter {
				key := fmt.Sprintf("task-%d-%d", i, j)
				if err := s.SaveResult(ctx, "ns", key, fmt.Appendf(nil, "data-%d-%d", i, j)); err != nil {
					t.Errorf("concurrent SaveResult %s: %v", key, err)
				}
			}
		}(i)
	}

	// Concurrent writers for session messages
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := []store.SessionMessage{
				{Role: "user", Content: fmt.Sprintf("from writer %d", i), Timestamp: now},
			}
			if err := s.AppendMessages(ctx, "ns", "concurrent-sess", msg); err != nil {
				t.Errorf("concurrent AppendMessages writer %d: %v", i, err)
			}
		}(i)
	}

	// Concurrent readers
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Read results (may or may not exist yet)
			s.GetResult(ctx, "ns", fmt.Sprintf("task-%d-0", i)) //nolint:errcheck
			// Read sessions
			s.ListSessions(ctx, "ns") //nolint:errcheck
			// Read transcript
			s.LoadTranscript(ctx, "ns", "concurrent-sess", 0) //nolint:errcheck
		}(i)
	}

	wg.Wait()

	// Verify all results were written
	for i := range writers {
		for j := range readsPerWriter {
			key := fmt.Sprintf("task-%d-%d", i, j)
			got, err := s.GetResult(ctx, "ns", key)
			if err != nil {
				t.Errorf("GetResult %s: %v", key, err)
				continue
			}
			expected := fmt.Sprintf("data-%d-%d", i, j)
			if string(got) != expected {
				t.Errorf("%s = %q, want %q", key, got, expected)
			}
		}
	}

	// Verify all messages were appended
	sess, err := s.GetSession(ctx, "ns", "concurrent-sess")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.MessageCount != writers {
		t.Errorf("MessageCount = %d, want %d", sess.MessageCount, writers)
	}
}

func TestIntegration_ConcurrentLocking(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "lock-race", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Race N goroutines to acquire the lock — exactly one should win
	const racers = 20
	var wg sync.WaitGroup
	winners := make(chan int, racers)

	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.AcquireLock(ctx, "ns", "lock-race", fmt.Sprintf("task-%d", i), "")
			if err == nil {
				winners <- i
			}
		}(i)
	}

	wg.Wait()
	close(winners)

	winnerList := make([]int, 0, 1)
	for w := range winners {
		winnerList = append(winnerList, w)
	}

	if len(winnerList) != 1 {
		t.Errorf("expected exactly 1 lock winner, got %d: %v", len(winnerList), winnerList)
	}
}

func TestIntegration_FullTaskLifecycle(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	ns := "production"
	sessionName := "my-session"
	taskName := "my-task"

	// 1. Create session
	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: ns, Name: sessionName, SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// 2. Acquire lock
	if err := s.AcquireLock(ctx, ns, sessionName, taskName, ""); err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	// 3. Verify locked for other tasks
	locked, err := s.IsLocked(ctx, ns, sessionName, "other-task", "")
	if err != nil {
		t.Fatalf("IsLocked: %v", err)
	}
	if !locked {
		t.Error("expected session to be locked for other-task")
	}

	// 4. Append user message
	if err := s.AppendMessages(ctx, ns, sessionName, []store.SessionMessage{
		{Role: "user", Content: "Run my container task", Timestamp: now},
	}); err != nil {
		t.Fatalf("AppendMessages user: %v", err)
	}

	// 5. Save result
	result := []byte(`{"status": "success", "output": "hello world"}`)
	if err := s.SaveResult(ctx, ns, taskName, result); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	// 6. Append assistant response
	if err := s.AppendMessages(ctx, ns, sessionName, []store.SessionMessage{
		{Role: roleAssistant, Content: "Task completed successfully", Timestamp: now.Add(time.Second)},
	}); err != nil {
		t.Fatalf("AppendMessages assistant: %v", err)
	}

	// 7. Release lock
	if err := s.ReleaseLock(ctx, ns, sessionName, taskName, ""); err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}

	// 8. Verify final state
	sess, err := s.GetSession(ctx, ns, sessionName)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.MessageCount != 2 {
		t.Errorf("MessageCount = %d, want 2", sess.MessageCount)
	}
	if sess.ActiveTask != "" {
		t.Errorf("ActiveTask = %q, want empty (unlocked)", sess.ActiveTask)
	}
	if len(sess.Messages) != 2 {
		t.Fatalf("Messages = %d, want 2", len(sess.Messages))
	}
	if sess.Messages[0].Role != "user" || sess.Messages[1].Role != roleAssistant {
		t.Errorf("unexpected message roles: %s, %s", sess.Messages[0].Role, sess.Messages[1].Role)
	}

	// 9. Verify result
	gotResult, err := s.GetResult(ctx, ns, taskName)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !bytes.Equal(gotResult, result) {
		t.Errorf("result = %q, want %q", gotResult, result)
	}

	// 10. Cleanup: delete task result and session
	if err := s.DeleteResult(ctx, ns, taskName); err != nil {
		t.Fatalf("DeleteResult: %v", err)
	}
	if err := s.DeleteSession(ctx, ns, sessionName); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	// Verify cleanup
	_, err = s.GetResult(ctx, ns, taskName)
	if err != store.ErrNotFound {
		t.Errorf("result after delete: got %v, want ErrNotFound", err)
	}
	_, err = s.GetSession(ctx, ns, sessionName)
	if err != store.ErrNotFound {
		t.Errorf("session after delete: got %v, want ErrNotFound", err)
	}
	msgs, err := s.LoadTranscript(ctx, ns, sessionName, 0)
	if err != nil {
		t.Fatalf("LoadTranscript after delete: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("messages after cascade delete: got %d, want 0", len(msgs))
	}
}

func TestIntegration_MultiNamespaceIsolation(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	namespaces := []string{"team-a", "team-b", "team-c"}

	for _, ns := range namespaces {
		// Each namespace gets its own result and session
		if err := s.SaveResult(ctx, ns, "shared-task", []byte("data-"+ns)); err != nil {
			t.Fatalf("SaveResult %s: %v", ns, err)
		}
		if err := s.CreateSession(ctx, &store.SessionRecord{
			Namespace: ns, Name: "shared-session", SessionType: "task", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateSession %s: %v", ns, err)
		}
		if err := s.AppendMessages(ctx, ns, "shared-session", []store.SessionMessage{
			{Role: "user", Content: "msg-from-" + ns, Timestamp: now},
		}); err != nil {
			t.Fatalf("AppendMessages %s: %v", ns, err)
		}
	}

	// Verify isolation: each namespace sees only its own data
	for _, ns := range namespaces {
		got, err := s.GetResult(ctx, ns, "shared-task")
		if err != nil {
			t.Fatalf("GetResult %s: %v", ns, err)
		}
		if string(got) != "data-"+ns {
			t.Errorf("ns %s result = %q, want %q", ns, got, "data-"+ns)
		}

		sessions, err := s.ListSessions(ctx, ns)
		if err != nil {
			t.Fatalf("ListSessions %s: %v", ns, err)
		}
		if len(sessions) != 1 {
			t.Errorf("ns %s sessions = %d, want 1", ns, len(sessions))
		}

		msgs, err := s.LoadTranscript(ctx, ns, "shared-session", 0)
		if err != nil {
			t.Fatalf("LoadTranscript %s: %v", ns, err)
		}
		if len(msgs) != 1 || msgs[0].Content != "msg-from-"+ns {
			t.Errorf("ns %s messages = %+v", ns, msgs)
		}
	}

	// Delete one namespace's data — others unaffected
	if err := s.DeleteSession(ctx, "team-a", "shared-session"); err != nil {
		t.Fatalf("DeleteSession team-a: %v", err)
	}
	if err := s.DeleteResult(ctx, "team-a", "shared-task"); err != nil {
		t.Fatalf("DeleteResult team-a: %v", err)
	}

	// team-b and team-c still have their data
	for _, ns := range []string{"team-b", "team-c"} {
		if _, err := s.GetResult(ctx, ns, "shared-task"); err != nil {
			t.Errorf("GetResult %s after team-a delete: %v", ns, err)
		}
		if _, err := s.GetSession(ctx, ns, "shared-session"); err != nil {
			t.Errorf("GetSession %s after team-a delete: %v", ns, err)
		}
	}
}

func TestIntegration_BinaryData(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()

	// Test various binary patterns
	tests := []struct {
		name string
		data []byte
	}{
		{"null bytes", []byte{0, 0, 0, 0, 0}},
		{"all byte values", func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}()},
		{"random binary", func() []byte {
			b := make([]byte, 4096)
			r := rand.New(rand.NewSource(42))
			r.Read(b)
			return b
		}()},
		{"empty", []byte{}},
		{"unicode", []byte("こんにちは世界 🌍 مرحبا")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.SaveResult(ctx, "ns", "binary-"+tt.name, tt.data); err != nil {
				t.Fatalf("SaveResult: %v", err)
			}
			got, err := s.GetResult(ctx, "ns", "binary-"+tt.name)
			if err != nil {
				t.Fatalf("GetResult: %v", err)
			}
			if !bytes.Equal(got, tt.data) {
				t.Errorf("data mismatch: got %d bytes, want %d bytes", len(got), len(tt.data))
			}
		})
	}
}

func TestIntegration_SessionTokenTracking(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace:    "ns",
		Name:         "token-session",
		SessionType:  "chat",
		InputTokens:  100,
		OutputTokens: 200,
		CreatedAt:    now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sess, err := s.GetSession(ctx, "ns", "token-session")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.InputTokens != 100 || sess.OutputTokens != 200 {
		t.Errorf("tokens = (%d, %d), want (100, 200)", sess.InputTokens, sess.OutputTokens)
	}

	// Verify listing includes token info
	sessions, err := s.ListSessions(ctx, "ns")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("got %d sessions, want 1", len(sessions))
	}
	if sessions[0].InputTokens != 100 || sessions[0].OutputTokens != 200 {
		t.Errorf("listed tokens = (%d, %d), want (100, 200)", sessions[0].InputTokens, sessions[0].OutputTokens)
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "shutdown.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	s := NewStore(db, dbPath)
	ctx := context.Background()

	// Write data before shutdown
	if err := s.SaveResult(ctx, "ns", "pre-shutdown", []byte("important")); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	// Simulate graceful shutdown via context cancellation
	shutdownCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- s.Start(shutdownCtx)
	}()

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Reopen and verify data survived
	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB reopen: %v", err)
	}
	defer db2.Close() //nolint:errcheck
	s2 := NewStore(db2, dbPath)

	got, err := s2.GetResult(ctx, "ns", "pre-shutdown")
	if err != nil {
		t.Fatalf("GetResult after shutdown: %v", err)
	}
	if string(got) != "important" {
		t.Errorf("got %q, want %q", got, "important")
	}
}

func TestIntegration_ShutdownDoesNotWritePlannerStatistics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "shutdown-no-write.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE shutdown_optimizer_probe (
			id INTEGER PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE INDEX shutdown_optimizer_probe_value
			ON shutdown_optimizer_probe(value);
		WITH RECURSIVE sequence(value) AS (
			SELECT 1
			UNION ALL
			SELECT value + 1 FROM sequence WHERE value < 10000
		)
		INSERT INTO shutdown_optimizer_probe(value)
			SELECT printf('value-%05d', value) FROM sequence;
	`); err != nil {
		_ = db.Close()
		t.Fatalf("seed optimizer probe: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM shutdown_optimizer_probe WHERE value = ?`, "value-05000").Scan(&count); err != nil {
		_ = db.Close()
		t.Fatalf("query optimizer probe: %v", err)
	}
	if count != 1 {
		_ = db.Close()
		t.Fatalf("optimizer probe count = %d, want 1", count)
	}

	var statisticsTables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'`).Scan(&statisticsTables); err != nil {
		_ = db.Close()
		t.Fatalf("query pre-shutdown statistics tables: %v", err)
	}
	if statisticsTables != 0 {
		_ = db.Close()
		t.Fatalf("pre-shutdown sqlite_stat1 tables = %d, want 0", statisticsTables)
	}

	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewStore(db, dbPath).Start(shutdownCtx); err != nil {
		t.Fatalf("Start after cancellation: %v", err)
	}

	reopened, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open after shutdown: %v", err)
	}
	defer reopened.Close() //nolint:errcheck
	if err := reopened.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'`).Scan(&statisticsTables); err != nil {
		t.Fatalf("query post-shutdown statistics tables: %v", err)
	}
	if statisticsTables != 0 {
		t.Fatalf("post-shutdown sqlite_stat1 tables = %d, want 0", statisticsTables)
	}
}

func TestIntegration_HealthCheckOnDisk(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()

	if err := s.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
}

func TestIntegration_DBSizeMetric(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "metrics.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer db.Close() //nolint:errcheck
	s := NewStore(db, dbPath)
	ctx := context.Background()

	// Write some data to ensure the file has measurable size
	for i := range 100 {
		if err := s.SaveResult(ctx, "ns", fmt.Sprintf("task-%d", i), []byte(strings.Repeat("x", 1024))); err != nil {
			t.Fatalf("SaveResult: %v", err)
		}
	}

	// Trigger metric update
	s.updateDBSizeMetric()

	// Verify the file exists and has non-zero size
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() == 0 {
		t.Error("DB file size is 0 after writing 100 results")
	}
}

func TestIntegration_DuplicateSessionCreate(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	session := &store.SessionRecord{
		Namespace: "ns", Name: "dup-session", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateSession(ctx, session); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Second create should fail (primary key conflict)
	err := s.CreateSession(ctx, session)
	if err == nil {
		t.Error("expected error on duplicate session create, got nil")
	}
}

func TestIntegration_AppendMessagesEmptyList(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "empty-append", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Appending empty list should succeed and not change count
	if err := s.AppendMessages(ctx, "ns", "empty-append", []store.SessionMessage{}); err != nil {
		t.Fatalf("AppendMessages empty: %v", err)
	}

	sess, err := s.GetSession(ctx, "ns", "empty-append")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", sess.MessageCount)
	}
}

func TestIntegration_ReleaseLockWrongTask(t *testing.T) {
	s := setupDiskStore(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "ns", Name: "wrong-release", SessionType: "task", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if err := s.AcquireLock(ctx, "ns", "wrong-release", "task-a", ""); err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}

	// Release with wrong task name — should be a no-op (lock stays)
	if err := s.ReleaseLock(ctx, "ns", "wrong-release", "task-b", ""); err != nil {
		t.Fatalf("ReleaseLock wrong task: %v", err)
	}

	// Lock should still be held by task-a
	locked, err := s.IsLocked(ctx, "ns", "wrong-release", "task-b", "")
	if err != nil {
		t.Fatalf("IsLocked: %v", err)
	}
	if !locked {
		t.Error("expected lock to still be held after wrong-task release")
	}
}

func TestIntegration_MigrateIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "migrate.db")

	// Run NewDB twice — migrations should be idempotent (IF NOT EXISTS)
	db1, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB first: %v", err)
	}
	_ = db1.Close()

	db2, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB second (idempotent migration): %v", err)
	}
	defer db2.Close() //nolint:errcheck

	// Verify DB is functional
	s := NewStore(db2, dbPath)
	if err := s.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck after double-migrate: %v", err)
	}
}

func TestIntegration_MigrateSecurityScanLegacySchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "security-legacy.db")
	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := legacyDB.Exec(`
		CREATE TABLE security_findings (
			id                TEXT PRIMARY KEY,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			scan_run_id       TEXT NOT NULL,
			fingerprint       TEXT NOT NULL,
			title             TEXT NOT NULL,
			summary           TEXT NOT NULL,
			severity          TEXT NOT NULL,
			confidence        TEXT NOT NULL,
			validation_status TEXT NOT NULL,
			state             TEXT NOT NULL,
			file_path         TEXT NOT NULL DEFAULT '',
			line              INTEGER NOT NULL DEFAULT 0,
			commit_sha        TEXT NOT NULL DEFAULT '',
			root_cause        TEXT NOT NULL DEFAULT '',
			remediation       TEXT NOT NULL DEFAULT '',
			suggested_action  TEXT NOT NULL DEFAULT '',
			evidence_json     TEXT NOT NULL DEFAULT '',
			validation_json   TEXT NOT NULL DEFAULT '',
			patch_proposal_id TEXT NOT NULL DEFAULT '',
			pr_number         INTEGER,
			pr_url            TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(namespace, repository_scan, fingerprint)
		);
		CREATE TABLE security_review_slices (
			id                TEXT PRIMARY KEY,
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
			last_scan_run_id  TEXT NOT NULL DEFAULT '',
			last_reviewed_at  TIMESTAMP,
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(namespace, repository_scan, id)
		);
		INSERT INTO security_review_slices (id, namespace, repository_scan, source, title)
		VALUES ('slice_api', 'ns1', 'repo1', 'legacy', 'Legacy API slice');
		INSERT INTO security_findings (
			id, namespace, repository_scan, scan_run_id, fingerprint, title, summary,
			severity, confidence, validation_status, state, created_at, updated_at
		) VALUES (
			'fnd_legacy_dismissed', 'ns1', 'repo1', 'scan_legacy', 'legacy-fingerprint',
			'Legacy dismissed finding', 'Legacy summary', 'high', 'high', 'validated', 'dismissed',
			'2026-08-01T00:00:00Z', '2026-08-03T00:00:00Z'
		);
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB() migrated legacy security schema error = %v", err)
	}
	defer db.Close() //nolint:errcheck
	secStore := NewStore(db, dbPath)

	if _, err := secStore.GetReviewSlice(context.Background(), "ns1", "repo1", "slice_api"); err != nil {
		t.Fatalf("GetReviewSlice() after migration error = %v", err)
	}
	for _, column := range []string{"slice_id", "target_key", "category", "triage", "reproduction", "minimum_fix_scope", "duplicate_of", "decision_at"} {
		if !sqliteTableHasColumn(t, db, "security_findings", column) {
			t.Fatalf("security_findings missing migrated column %q", column)
		}
	}
	migratedFinding, err := secStore.GetFinding(context.Background(), "ns1", "fnd_legacy_dismissed")
	if err != nil {
		t.Fatalf("GetFinding() migrated legacy decision error = %v", err)
	}
	if !migratedFinding.DecisionAt.IsZero() {
		t.Fatalf("migrated decisionAt = %v, want unknown legacy decision time", migratedFinding.DecisionAt)
	}

	ctx := context.Background()
	if err := secStore.UpsertReviewSlice(ctx, &store.ReviewSlice{
		ID:             "slice_api",
		Namespace:      "ns2",
		RepositoryScan: "repo1",
		Source:         "legacy-migration-test",
		Title:          "Same ID in different namespace",
	}); err != nil {
		t.Fatalf("UpsertReviewSlice() same id in different namespace error = %v", err)
	}
	if _, err := secStore.GetReviewSlice(ctx, "ns2", "repo1", "slice_api"); err != nil {
		t.Fatalf("GetReviewSlice(ns2) error = %v", err)
	}
}

func TestIntegration_MigrateAndReopenPatchProposalPublicationEvidence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "patch-publication-legacy.db")
	initial, bound := testPatchProposalPublication("legacy")

	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := legacyDB.Exec(`
		CREATE TABLE security_patch_proposals (
			id                TEXT PRIMARY KEY,
			namespace         TEXT NOT NULL,
			repository_scan   TEXT NOT NULL,
			finding_id        TEXT NOT NULL,
			task_name         TEXT NOT NULL,
			branch            TEXT NOT NULL,
			diff_artifact     TEXT NOT NULL DEFAULT '',
			summary_artifact  TEXT NOT NULL DEFAULT '',
			status            TEXT NOT NULL,
			pr_number         INTEGER,
			pr_url            TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX idx_security_patch_proposals_finding
			ON security_patch_proposals(namespace, finding_id, created_at DESC);
		INSERT INTO security_patch_proposals (
			id, namespace, repository_scan, finding_id, task_name, branch, status
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, initial.ID, initial.Namespace, initial.RepositoryScan, initial.FindingID, initial.TaskName, initial.Branch, initial.Status); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy patch proposal schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB() migrated patch proposal schema error = %v", err)
	}
	if !sqliteTableHasColumn(t, db, "security_patch_proposals", "publication_evidence_json") {
		_ = db.Close()
		t.Fatal("security_patch_proposals missing migrated publication_evidence_json column")
	}
	s := NewStore(db, dbPath)
	if err := s.BindPatchProposalPublicationEvidence(context.Background(), bound); err != nil {
		_ = db.Close()
		t.Fatalf("BindPatchProposalPublicationEvidence() after migration error = %v", err)
	}
	boundUpdatedAt := bound.UpdatedAt
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	reopenedDB, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB() reopen error = %v", err)
	}
	defer reopenedDB.Close() //nolint:errcheck
	reopenedStore := NewStore(reopenedDB, dbPath)
	stored := onlyPatchProposal(t, reopenedStore, initial.Namespace, initial.FindingID)
	if !reflect.DeepEqual(stored.PublicationEvidence, bound.PublicationEvidence) {
		t.Fatalf("reopened publication evidence = %#v, want %#v", stored.PublicationEvidence, bound.PublicationEvidence)
	}
	if !stored.UpdatedAt.Equal(boundUpdatedAt) {
		t.Fatalf("reopened updatedAt = %v, want %v", stored.UpdatedAt, boundUpdatedAt)
	}

	replay := clonePatchProposal(bound)
	replay.UpdatedAt = time.Time{}
	if err := reopenedStore.BindPatchProposalPublicationEvidence(context.Background(), replay); err != nil {
		t.Fatalf("identical replay after reopen error = %v", err)
	}
	if !replay.UpdatedAt.Equal(boundUpdatedAt) {
		t.Fatalf("replay after reopen updatedAt = %v, want unchanged %v", replay.UpdatedAt, boundUpdatedAt)
	}
}

func sqliteTableHasColumn(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s) error = %v", table, err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, pk int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table_info(%s) error = %v", table, err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info(%s) rows error = %v", table, err)
	}
	return false
}

func TestIntegration_WALModeEnabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("journal_mode = %q, want %q", journalMode, "wal")
	}
}

func TestIntegration_ForeignKeysEnabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fk.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	var fk int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, want 1", fk)
	}
}
