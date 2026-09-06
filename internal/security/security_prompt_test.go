package security

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

// TestReviewSlicePromptMetadataIgnoresStoreBookkeeping proves the prompt
// projection is stable across the mutable fields the scan run updates, so
// the controller can rebuild the exact prompt of a Task it created earlier.
func TestReviewSlicePromptMetadataIgnoresStoreBookkeeping(t *testing.T) {
	now := time.Now()
	slice := store.ReviewSlice{SchemaVersion: 1, ID: "slice_1", RepositoryScan: "scan", Source: "mapper", Title: "t", Kind: "source", Confidence: "high",
		OwnedFiles: []store.ReviewSliceFile{{Path: "a.js"}}, Status: "pending", CreatedAt: now, UpdatedAt: now}
	before, _ := json.Marshal(newReviewSlicePromptMetadata(slice))
	slice.Status = "reviewed"
	slice.LastScanRunID = "scan_2"
	slice.UpdatedAt = now.Add(time.Hour)
	slice.LastReviewedAt = &slice.UpdatedAt
	slice.ReviewContextJSON = "{}"
	slice.ReviewContextHash = "sha256:abc"
	after, _ := json.Marshal(newReviewSlicePromptMetadata(slice))
	if string(before) != string(after) {
		t.Fatalf("projection changed with store bookkeeping:\n%s\n%s", before, after)
	}
	for _, forbidden := range []string{"status", "updatedAt", "createdAt", "lastScanRunID", "lastReviewedAt", "reviewContextJSON", "reviewContextHash"} {
		if strings.Contains(string(after), "\""+forbidden+"\"") {
			t.Fatalf("projection must not include %s", forbidden)
		}
	}
	if !strings.Contains(string(after), "\"ownedFiles\"") {
		t.Fatal("projection must keep the slice scope")
	}
}
