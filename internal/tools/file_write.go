/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	modeWrite  = "write"
	modeAppend = "append"
)

// FileWriteTool implements file writing functionality
type FileWriteTool struct {
	workDir      string
	maxFileSize  int64
	allowedPaths []string
}

// FileWriteArgs are the arguments for the file write tool
type FileWriteArgs struct {
	Path       string `json:"path"`
	Content    string `json:"content"`
	Mode       string `json:"mode,omitempty"`
	CreateDirs bool   `json:"create_dirs,omitempty"`
}

// FileWriteResult represents the file write result
type FileWriteResult struct {
	Path    string `json:"path"`
	Size    int    `json:"size"`
	Mode    string `json:"mode"`
	Created bool   `json:"created"`
}

// NewFileWriteTool creates a new file write tool
func NewFileWriteTool() *FileWriteTool {
	workDir := os.Getenv(workerenv.WorkDir)
	if workDir == "" {
		workDir = defaultWorkspacePath
	}

	return &FileWriteTool{
		workDir:     workDir,
		maxFileSize: 1024 * 1024, // 1MB max
		allowedPaths: []string{
			defaultWorkspacePath,
			tempDirPath,
		},
	}
}

// Name returns the tool name
func (t *FileWriteTool) Name() string {
	return fileWriteToolName
}

// Description returns the tool description
func (t *FileWriteTool) Description() string {
	return "Write content to a file in the workspace. Use this to create or modify files."
}

// Parameters returns the JSON Schema for parameters
func (t *FileWriteTool) Parameters() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {
				"type": "string",
				"description": "Path to the file to write (relative to workspace or absolute)"
			},
			"content": {
				"type": "string",
				"description": "Content to write to the file"
			},
			"mode": {
				"type": "string",
				"description": "Write mode: 'write' (overwrite) or 'append' (default: write)",
				"enum": ["write", "append"],
				"default": "write"
			},
			"create_dirs": {
				"type": "boolean",
				"description": "Create parent directories if they don't exist (default: true)",
				"default": true
			}
		},
		"required": ["path", "content"]
	}`)
}

// Execute writes the file
func (t *FileWriteTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var writeArgs FileWriteArgs
	if err := json.Unmarshal(args, &writeArgs); err != nil {
		return "", fmt.Errorf("invalid arguments: %w", err)
	}

	if writeArgs.Path == "" {
		return "", fmt.Errorf("path is required")
	}

	if writeArgs.Mode == "" {
		writeArgs.Mode = modeWrite
	}
	if writeArgs.Mode != modeWrite && writeArgs.Mode != modeAppend {
		return "", fmt.Errorf("mode must be 'write' or 'append'")
	}

	// Default create_dirs to true — check if explicitly set to false via raw args
	createDirs := true
	var rawMap map[string]any
	if err := json.Unmarshal(args, &rawMap); err == nil {
		if v, ok := rawMap["create_dirs"]; ok {
			if b, ok := v.(bool); ok {
				createDirs = b
			}
		}
	}

	// Check content size
	if int64(len(writeArgs.Content)) > t.maxFileSize {
		return "", fmt.Errorf("content size %d exceeds maximum %d bytes", len(writeArgs.Content), t.maxFileSize)
	}

	// Resolve the path and select the configured allowed directory that will
	// anchor every filesystem operation. The lexical check selects a root;
	// os.Root enforces containment while following path components, including
	// when symlinks change concurrently.
	filePath := writeArgs.Path
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(t.workDir, filePath)
	}
	filePath = filepath.Clean(filePath)

	rootPath, relativePath, ok := t.allowedRoot(filePath)
	if !ok {
		return "", fmt.Errorf("access denied: path outside allowed directories")
	}

	// Allowed paths are trusted, provisioned roots. OpenRoot pins the selected
	// directory for this operation so descendant lookups cannot escape it.
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return "", fmt.Errorf("failed to open allowed directory: %w", err)
	}
	defer root.Close() //nolint:errcheck

	// Check whether the target already exists through the same anchored root
	// used for the write.
	_, statErr := root.Stat(relativePath)
	created := os.IsNotExist(statErr)

	// Create parent directories if needed
	if createDirs {
		dir := filepath.Dir(relativePath)
		if err := root.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create directories: %w", err)
		}
	}

	flags := os.O_CREATE | os.O_WRONLY
	if writeArgs.Mode == modeAppend {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	f, err := root.OpenFile(relativePath, flags, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	if _, err := f.WriteString(writeArgs.Content); err != nil {
		f.Close() //nolint:errcheck
		return "", fmt.Errorf("failed to write file: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	result := FileWriteResult{
		Path:    writeArgs.Path,
		Size:    len(writeArgs.Content),
		Mode:    writeArgs.Mode,
		Created: created,
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}

	return string(output), nil
}

// allowedRoot returns the first configured allowed directory containing path
// and path relative to that directory. filepath.Rel plus filepath.IsLocal
// enforce component boundaries, unlike a raw string-prefix check which also
// matches lexical siblings. Configured order is significant: the first match
// defines the os.Root confinement boundary and later roots are not fallbacks.
func (t *FileWriteTool) allowedRoot(path string) (rootPath, relativePath string, ok bool) {
	for _, allowedPath := range t.allowedPaths {
		rootPath = filepath.Clean(allowedPath)
		relativePath, err := filepath.Rel(rootPath, path)
		if err != nil || !filepath.IsLocal(relativePath) {
			continue
		}
		return rootPath, relativePath, true
	}
	return "", "", false
}

// Ensure FileWriteTool implements Tool
var _ Tool = (*FileWriteTool)(nil)
