package supervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/workspacedelta"
)

func TestBuildWorkspaceDeltaPropagatesRequestMaxBytes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("before\n"), 0o644); err != nil {
		t.Fatalf("write baseline file: %v", err)
	}
	baseline, err := workspacedelta.Capture(root, workspacedelta.Options{})
	if err != nil {
		t.Fatalf("capture baseline: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte(strings.Repeat("x", 2048)), 0o644); err != nil {
		t.Fatalf("write oversized workspace change: %v", err)
	}

	_, err = buildWorkspaceDeltaContext(
		context.Background(),
		baseline,
		root,
		workspacedelta.IntentWrite,
		harnessv2.WorkspaceDeltaLimits{MaxBytes: 1024},
	)
	if !errors.Is(err, workspacedelta.ErrLimitExceeded) {
		t.Fatalf("buildWorkspaceDelta error = %v, want ErrLimitExceeded", err)
	}
	var pathErr *workspacedelta.PathError
	if !errors.As(err, &pathErr) || pathErr.Op != "retain changed content" {
		t.Fatalf("buildWorkspaceDelta error = %v, want request limit before content retention", err)
	}

	result, err := buildWorkspaceDeltaContext(
		context.Background(),
		baseline,
		root,
		workspacedelta.IntentWrite,
		harnessv2.WorkspaceDeltaLimits{MaxBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("buildWorkspaceDelta within request limit: %v", err)
	}
	if result.Classification != workspacedelta.ClassificationWriteDelta || len(result.Artifact) == 0 {
		t.Fatalf("buildWorkspaceDelta result = classification %q, artifact bytes %d", result.Classification, len(result.Artifact))
	}
}
