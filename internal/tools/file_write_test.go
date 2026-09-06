/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestFileWriteTool(root string) *FileWriteTool {
	return &FileWriteTool{
		workDir:      root,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{root},
	}
}

func createRelativeSymlink(t *testing.T, target, link string) {
	t.Helper()
	relativeTarget, err := filepath.Rel(filepath.Dir(link), target)
	if err != nil {
		t.Fatalf("failed to make symlink target relative: %v", err)
	}
	if err := os.Symlink(relativeTarget, link); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}
}

func TestFileWriteTool_Name(t *testing.T) {
	tool := NewFileWriteTool()
	if got := tool.Name(); got != fileWriteToolName {
		t.Errorf("Name() = %v, want %v", got, fileWriteToolName)
	}
}

func TestFileWriteTool_Description(t *testing.T) {
	tool := NewFileWriteTool()
	if desc := tool.Description(); desc == "" {
		t.Error("Description() returned empty string")
	}
}

func TestFileWriteTool_Parameters(t *testing.T) {
	tool := NewFileWriteTool()
	params := tool.Parameters()
	if params == nil {
		t.Fatal("Parameters() returned nil")
	}
	var schema map[string]any
	if err := json.Unmarshal(params, &schema); err != nil {
		t.Errorf("Parameters() returned invalid JSON: %v", err)
	}
	if schema[jsonSchemaTypeField] != typeObject {
		t.Error("Parameters schema should have type: object")
	}
}

func TestFileWriteTool_Execute_Write(t *testing.T) {
	tmpDir := t.TempDir()
	tool := &FileWriteTool{
		workDir:      tmpDir,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{tmpDir},
	}

	args := json.RawMessage(`{"path": "test.txt", "content": "hello world"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var writeResult FileWriteResult
	if err := json.Unmarshal([]byte(result), &writeResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !writeResult.Created {
		t.Error("expected created = true for new file")
	}
	if writeResult.Mode != "write" {
		t.Errorf("mode = %q, want %q", writeResult.Mode, "write")
	}

	// Verify file content
	content, err := os.ReadFile(filepath.Join(tmpDir, "test.txt"))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("file content = %q, want %q", string(content), "hello world")
	}
}

func TestFileWriteTool_Execute_Append(t *testing.T) {
	tmpDir := t.TempDir()
	tool := &FileWriteTool{
		workDir:      tmpDir,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{tmpDir},
	}

	// Write initial content
	filePath := filepath.Join(tmpDir, "append.txt")
	if err := os.WriteFile(filePath, []byte("line1\n"), 0644); err != nil {
		t.Fatalf("failed to write initial file: %v", err)
	}

	args := json.RawMessage(`{"path": "append.txt", "content": "line2\n", "mode": "append"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var writeResult FileWriteResult
	if err := json.Unmarshal([]byte(result), &writeResult); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if writeResult.Created {
		t.Error("expected created = false for existing file")
	}
	if writeResult.Mode != "append" {
		t.Errorf("mode = %q, want %q", writeResult.Mode, "append")
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if string(content) != "line1\nline2\n" {
		t.Errorf("file content = %q, want %q", string(content), "line1\nline2\n")
	}
}

func TestFileWriteTool_Execute_CreateDirs(t *testing.T) {
	tmpDir := t.TempDir()
	tool := newTestFileWriteTool(tmpDir)

	args := json.RawMessage(`{"path": "deep/nested/dir/file.txt", "content": "nested"}`)
	_, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "deep", "nested", "dir", "file.txt"))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	if string(content) != "nested" {
		t.Errorf("file content = %q, want %q", string(content), "nested")
	}
}

func TestFileWriteTool_Execute_CreateDirsFalse(t *testing.T) {
	tmpDir := t.TempDir()
	tool := newTestFileWriteTool(tmpDir)

	_, err := tool.Execute(context.Background(), json.RawMessage(
		`{"path":"missing/parent/file.txt","content":"blocked","create_dirs":false}`,
	))
	if err == nil {
		t.Fatal("Execute() expected error when create_dirs=false and parent is missing")
	}
	if _, statErr := os.Stat(filepath.Join(tmpDir, "missing")); !os.IsNotExist(statErr) {
		t.Errorf("missing parent was created with create_dirs=false; stat error = %v", statErr)
	}

	existingDir := filepath.Join(tmpDir, "existing")
	if err := os.Mkdir(existingDir, 0755); err != nil {
		t.Fatalf("failed to create existing parent: %v", err)
	}
	_, err = tool.Execute(context.Background(), json.RawMessage(
		`{"path":"existing/file.txt","content":"allowed","create_dirs":false}`,
	))
	if err != nil {
		t.Fatalf("Execute() with an existing parent error = %v", err)
	}
	content, err := os.ReadFile(filepath.Join(existingDir, "file.txt"))
	if err != nil {
		t.Fatalf("failed to read file under existing parent: %v", err)
	}
	if got, want := string(content), "allowed"; got != want {
		t.Errorf("file content = %q, want %q", got, want)
	}
}

func TestFileWriteTool_AllowedRootPrecedence(t *testing.T) {
	parentDir := t.TempDir()
	nestedDir := filepath.Join(parentDir, "nested")
	if err := os.Mkdir(nestedDir, 0755); err != nil {
		t.Fatalf("failed to create nested root: %v", err)
	}
	target := filepath.Join(nestedDir, "file.txt")

	tests := []struct {
		name         string
		allowedPaths []string
		wantRoot     string
		wantRelative string
	}{
		{
			name:         "broad root first",
			allowedPaths: []string{parentDir, nestedDir},
			wantRoot:     parentDir,
			wantRelative: filepath.Join("nested", "file.txt"),
		},
		{
			name:         "nested root first",
			allowedPaths: []string{nestedDir, parentDir},
			wantRoot:     nestedDir,
			wantRelative: "file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tool := &FileWriteTool{allowedPaths: tt.allowedPaths}
			gotRoot, gotRelative, ok := tool.allowedRoot(target)
			if !ok {
				t.Fatal("allowedRoot() did not find a containing root")
			}
			if gotRoot != tt.wantRoot {
				t.Errorf("allowedRoot() root = %q, want %q", gotRoot, tt.wantRoot)
			}
			if gotRelative != tt.wantRelative {
				t.Errorf("allowedRoot() relative path = %q, want %q", gotRelative, tt.wantRelative)
			}
		})
	}
}

func TestFileWriteTool_Execute_FirstAllowedRootOpenFailureDoesNotFallback(t *testing.T) {
	parentDir := t.TempDir()
	missingRoot := filepath.Join(parentDir, "missing-root")
	tool := &FileWriteTool{
		workDir:      missingRoot,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{missingRoot, parentDir},
	}

	_, err := tool.Execute(context.Background(), json.RawMessage(
		`{"path":"file.txt","content":"blocked"}`,
	))
	if err == nil {
		t.Fatal("Execute() expected an error when the selected allowed root cannot be opened")
	}
	if !strings.Contains(err.Error(), "failed to open allowed directory") {
		t.Fatalf("Execute() error = %v, want allowed-root open failure", err)
	}
	if _, statErr := os.Stat(missingRoot); !os.IsNotExist(statErr) {
		t.Errorf("Execute() fell back to a broader root; stat error = %v", statErr)
	}
}

func TestFileWriteTool_Execute_RootedOpenFailure(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(tmpDir, "target"), 0755); err != nil {
		t.Fatalf("failed to create target directory: %v", err)
	}
	tool := newTestFileWriteTool(tmpDir)

	_, err := tool.Execute(context.Background(), json.RawMessage(
		`{"path":"target","content":"blocked","create_dirs":false}`,
	))
	if err == nil {
		t.Fatal("Execute() expected an error when the target cannot be opened for writing")
	}
	if !strings.Contains(err.Error(), "failed to open file") {
		t.Fatalf("Execute() error = %v, want rooted file-open failure", err)
	}
}

func TestFileWriteTool_Execute_ComponentBoundPaths(t *testing.T) {
	t.Run("sibling prefix denied", func(t *testing.T) {
		parentDir := t.TempDir()
		allowedDir := filepath.Join(parentDir, "allowed")
		siblingDir := filepath.Join(parentDir, "allowed-sibling")
		if err := os.Mkdir(allowedDir, 0755); err != nil {
			t.Fatalf("failed to create allowed directory: %v", err)
		}
		if err := os.Mkdir(siblingDir, 0755); err != nil {
			t.Fatalf("failed to create sibling directory: %v", err)
		}
		tool := newTestFileWriteTool(allowedDir)
		escapedPath := filepath.Join(siblingDir, "escaped.txt")
		args, err := json.Marshal(FileWriteArgs{Path: escapedPath, Content: "escaped"})
		if err != nil {
			t.Fatalf("failed to marshal arguments: %v", err)
		}

		if _, err := tool.Execute(context.Background(), args); err == nil {
			t.Fatal("Execute() expected an error for a lexical sibling-prefix path")
		}
		if _, err := os.Stat(escapedPath); !os.IsNotExist(err) {
			t.Errorf("sibling-prefix escape created a file; stat error = %v", err)
		}
	})

	t.Run("parent traversal denied", func(t *testing.T) {
		allowedDir := t.TempDir()
		tool := newTestFileWriteTool(allowedDir)
		escapedPath := filepath.Join(filepath.Dir(allowedDir), "escaped.txt")

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"../escaped.txt","content":"escaped"}`,
		)); err == nil {
			t.Fatal("Execute() expected an error for parent traversal")
		}
		if _, err := os.Stat(escapedPath); !os.IsNotExist(err) {
			t.Errorf("parent traversal created a file; stat error = %v", err)
		}
	})

	t.Run("in-root parent component allowed", func(t *testing.T) {
		allowedDir := t.TempDir()
		tool := newTestFileWriteTool(allowedDir)

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"nested/../inside.txt","content":"inside"}`,
		)); err != nil {
			t.Fatalf("Execute() rejected an in-root parent component: %v", err)
		}
		content, err := os.ReadFile(filepath.Join(allowedDir, "inside.txt"))
		if err != nil {
			t.Fatalf("failed to read in-root file: %v", err)
		}
		if got, want := string(content), "inside"; got != want {
			t.Errorf("file content = %q, want %q", got, want)
		}
	})

	t.Run("double dot filename allowed", func(t *testing.T) {
		allowedDir := t.TempDir()
		tool := newTestFileWriteTool(allowedDir)

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"version..txt","content":"allowed"}`,
		)); err != nil {
			t.Fatalf("Execute() rejected a filename containing two dots: %v", err)
		}
	})
}

func TestFileWriteTool_Execute_RootedSymlinks(t *testing.T) {
	t.Run("in-root symlink allowed", func(t *testing.T) {
		allowedDir := t.TempDir()
		realDir := filepath.Join(allowedDir, "real")
		if err := os.Mkdir(realDir, 0755); err != nil {
			t.Fatalf("failed to create real directory: %v", err)
		}
		createRelativeSymlink(t, realDir, filepath.Join(allowedDir, "link"))
		tool := newTestFileWriteTool(allowedDir)

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"link/file.txt","content":"first"}`,
		)); err != nil {
			t.Fatalf("write through in-root symlink error = %v", err)
		}
		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"link/file.txt","content":" second","mode":"append"}`,
		)); err != nil {
			t.Fatalf("append through in-root symlink error = %v", err)
		}
		content, err := os.ReadFile(filepath.Join(realDir, "file.txt"))
		if err != nil {
			t.Fatalf("failed to read file through real path: %v", err)
		}
		if got, want := string(content), "first second"; got != want {
			t.Errorf("file content = %q, want %q", got, want)
		}
	})

	t.Run("parent symlink escape denied", func(t *testing.T) {
		allowedDir := t.TempDir()
		outsideDir := t.TempDir()
		createRelativeSymlink(t, outsideDir, filepath.Join(allowedDir, "outside"))
		tool := newTestFileWriteTool(allowedDir)
		escapedPath := filepath.Join(outsideDir, "escaped.txt")

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"outside/escaped.txt","content":"escaped","create_dirs":false}`,
		)); err == nil {
			t.Fatal("Execute() expected an error for a parent symlink escaping the root")
		}
		if _, err := os.Stat(escapedPath); !os.IsNotExist(err) {
			t.Errorf("parent symlink escape created a file; stat error = %v", err)
		}
	})

	for _, mode := range []string{modeWrite, modeAppend} {
		t.Run("final symlink escape "+mode, func(t *testing.T) {
			allowedDir := t.TempDir()
			outsideDir := t.TempDir()
			outsidePath := filepath.Join(outsideDir, "outside.txt")
			if err := os.WriteFile(outsidePath, []byte("original"), 0644); err != nil {
				t.Fatalf("failed to create outside file: %v", err)
			}
			createRelativeSymlink(t, outsidePath, filepath.Join(allowedDir, "link.txt"))
			tool := newTestFileWriteTool(allowedDir)
			args, err := json.Marshal(FileWriteArgs{
				Path:    "link.txt",
				Content: "changed",
				Mode:    mode,
			})
			if err != nil {
				t.Fatalf("failed to marshal arguments: %v", err)
			}

			if _, err := tool.Execute(context.Background(), args); err == nil {
				t.Fatalf("Execute() expected an error for final symlink escape in %s mode", mode)
			}
			content, err := os.ReadFile(outsidePath)
			if err != nil {
				t.Fatalf("failed to read outside file: %v", err)
			}
			if got, want := string(content), "original"; got != want {
				t.Errorf("outside file content = %q, want %q", got, want)
			}
		})
	}

	t.Run("dangling symlink escape denied", func(t *testing.T) {
		allowedDir := t.TempDir()
		outsideDir := t.TempDir()
		outsidePath := filepath.Join(outsideDir, "missing.txt")
		createRelativeSymlink(t, outsidePath, filepath.Join(allowedDir, "dangling.txt"))
		tool := newTestFileWriteTool(allowedDir)

		if _, err := tool.Execute(context.Background(), json.RawMessage(
			`{"path":"dangling.txt","content":"escaped"}`,
		)); err == nil {
			t.Fatal("Execute() expected an error for a dangling symlink escaping the root")
		}
		if _, err := os.Stat(outsidePath); !os.IsNotExist(err) {
			t.Errorf("dangling symlink escape created a file; stat error = %v", err)
		}
	})
}

func TestFileWriteTool_Execute_PathTraversal(t *testing.T) {
	tmpDir := t.TempDir()
	tool := &FileWriteTool{
		workDir:      tmpDir,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{tmpDir},
	}

	args := json.RawMessage(`{"path": "../../../etc/passwd", "content": "hack"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for path traversal")
	}
}

func TestFileWriteTool_Execute_PathRestriction(t *testing.T) {
	tool := &FileWriteTool{
		workDir:      defaultWorkspacePath,
		maxFileSize:  1024 * 1024,
		allowedPaths: []string{defaultWorkspacePath, tempDirPath},
	}

	args := json.RawMessage(`{"path": "/etc/passwd", "content": "hack"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for restricted path")
	}
	if err != nil && !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected access denied error, got: %v", err)
	}
}

func TestFileWriteTool_Execute_MaxFileSize(t *testing.T) {
	tmpDir := t.TempDir()
	tool := &FileWriteTool{
		workDir:      tmpDir,
		maxFileSize:  100,
		allowedPaths: []string{tmpDir},
	}

	bigContent := strings.Repeat("x", 200)
	args := json.RawMessage(`{"path": "big.txt", "content": "` + bigContent + `"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for oversized content")
	}
}

func TestFileWriteTool_Execute_EmptyPath(t *testing.T) {
	tool := NewFileWriteTool()
	args := json.RawMessage(`{"path": "", "content": "test"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for empty path")
	}
}

func TestFileWriteTool_Execute_InvalidMode(t *testing.T) {
	tool := NewFileWriteTool()
	args := json.RawMessage(`{"path": "test.txt", "content": "test", "mode": "invalid"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for invalid mode")
	}
}

func TestFileWriteTool_Execute_InvalidJSON(t *testing.T) {
	tool := NewFileWriteTool()
	args := json.RawMessage(invalidJSONText)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Error("Execute() expected error for invalid JSON")
	}
}
