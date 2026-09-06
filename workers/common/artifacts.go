/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package common

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/workerenv"
)

// artifactMaxRetries and the shared maxBackoff cap size artifact uploads to
// the same ~4 minute window as result submission (2s, 4s, 8s, 16s, 32s, then
// 60s steps), so a worker that reaches its uploads during a routine
// single-replica controller restart still persists its output.
const artifactMaxRetries = 9

const (
	artifactsDirEnv           = "ORKA_ARTIFACTS_DIR"
	defaultArtifactsDir       = "/tmp/artifacts"
	workspaceArtifactsDirName = ".orka-artifacts"
	maxTotalSize              = 50 << 20 // 50 MB
	maxFileSize               = 10 << 20 // 10 MB
	artifactPath              = "internal/v1/artifacts"
)

func artifactsDir() string {
	if dir := strings.TrimSpace(os.Getenv(artifactsDirEnv)); dir != "" {
		return filepath.Clean(dir)
	}
	return defaultArtifactsDir
}

// EnsureWorkspaceArtifactsLink exposes /tmp/artifacts inside the repo root so
// runtime agents can write artifacts using a workspace-relative path.
func EnsureWorkspaceArtifactsLink(workspaceDir string) error {
	if workspaceDir == "" {
		return nil
	}
	artifactRoot := artifactsDir()
	if err := os.MkdirAll(artifactRoot, 0o755); err != nil {
		return fmt.Errorf("failed to create artifacts directory: %w", err)
	}
	workspaceRoot, err := os.OpenRoot(workspaceDir)
	if err != nil {
		return fmt.Errorf("failed to open workspace directory: %w", err)
	}
	defer workspaceRoot.Close() //nolint:errcheck

	linkPath := filepath.Join(workspaceDir, workspaceArtifactsDirName)
	info, err := workspaceRoot.Lstat(workspaceArtifactsDirName)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			target, readErr := workspaceRoot.Readlink(workspaceArtifactsDirName)
			if readErr == nil {
				resolved := target
				if !filepath.IsAbs(resolved) {
					resolved = filepath.Join(filepath.Dir(linkPath), resolved)
				}
				if filepath.Clean(resolved) == filepath.Clean(artifactRoot) {
					return nil
				}
			}
		}
		return fmt.Errorf("workspace artifact path %s already exists", linkPath)
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("failed to inspect workspace artifact path: %w", err)
	}

	if err := workspaceRoot.Symlink(artifactRoot, workspaceArtifactsDirName); err != nil {
		return fmt.Errorf("failed to create workspace artifact symlink: %w", err)
	}
	return nil
}

// MissingArtifacts returns required artifact filenames that do not exist yet
// or are present but empty.
func MissingArtifacts(filenames []string) ([]string, error) {
	missing := make([]string, 0, len(filenames))
	localNames := make([]string, 0, len(filenames))
	for _, filename := range filenames {
		localName, err := artifactFilename(filename)
		if err != nil {
			return nil, err
		}
		localNames = append(localNames, localName)
	}
	artifactRoot := artifactsDir()
	root, err := os.OpenRoot(artifactRoot)
	if os.IsNotExist(err) {
		return append(missing, filenames...), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open artifacts directory: %w", err)
	}
	defer root.Close() //nolint:errcheck

	for i, filename := range filenames {
		localName := localNames[i]
		info, err := root.Stat(localName)
		switch {
		case os.IsNotExist(err):
			missing = append(missing, filename)
		case err != nil:
			return nil, fmt.Errorf("failed to stat artifact %s: %w", filename, err)
		case info.IsDir() || info.Size() == 0:
			missing = append(missing, filename)
		}
	}
	return missing, nil
}

// WriteArtifactFile writes an artifact file into the shared upload directory.
func WriteArtifactFile(filename string, data []byte) error {
	rawFilename := filename
	filename, err := artifactFilename(filename)
	if err != nil {
		return fmt.Errorf("invalid artifact filename %q", rawFilename)
	}
	artifactRoot := artifactsDir()
	if err := os.MkdirAll(artifactRoot, 0o755); err != nil {
		return fmt.Errorf("failed to create artifacts directory: %w", err)
	}
	root, err := os.OpenRoot(artifactRoot)
	if err != nil {
		return fmt.Errorf("failed to open artifacts directory: %w", err)
	}
	defer root.Close() //nolint:errcheck
	if err := root.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("failed to write artifact %s: %w", filename, err)
	}
	return nil
}

func artifactFilename(filename string) (string, error) {
	if filename == "" || filename == "." || filename == ".." ||
		strings.ContainsAny(filename, "/\\") || !fs.ValidPath(filename) {
		return "", fmt.Errorf("invalid artifact filename")
	}
	localName, err := filepath.Localize(filename)
	if err != nil {
		return "", err
	}
	if !filepath.IsLocal(localName) || localName != filepath.Base(localName) {
		return "", fmt.Errorf("invalid artifact filename")
	}
	return localName, nil
}

// UploadArtifacts scans /tmp/artifacts and uploads each file to the controller.
// It is called after SubmitResult to persist any files the agent wrote.
// Returns nil if the artifacts directory does not exist or is empty.
func UploadArtifacts() error {
	artifactRoot := artifactsDir()
	info, err := os.Lstat(artifactRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to inspect artifacts directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("artifacts directory must not be a symlink")
	}
	if !info.IsDir() {
		return fmt.Errorf("artifacts path is not a directory")
	}

	dirFile, err := openNoFollow(artifactRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open artifacts directory: %w", err)
	}
	defer dirFile.Close() //nolint:errcheck
	entries, err := dirFile.ReadDir(-1)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read artifacts directory: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	var totalSize int64

	baseEndpoint, err := artifactEndpointBase()
	if err != nil {
		return fmt.Errorf("failed to construct artifact endpoint: %w", err)
	}

	saToken := workerServiceAccountToken()

	type pendingArtifact struct {
		filename    string
		data        []byte
		contentType string
	}

	pending := make([]pendingArtifact, 0, len(entries))
	var uploadErrors []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		filename := filepath.Base(e.Name())
		// Reject filenames with path traversal or special characters
		if filename == "." || filename == ".." || strings.ContainsAny(filename, "/\\") {
			fmt.Fprintf(os.Stderr, "artifact: skipping invalid filename %q\n", filename)
			continue
		}
		if e.Type()&os.ModeSymlink != 0 {
			fmt.Fprintf(os.Stderr, "artifact: skipping symlink %s\n", filename)
			continue
		}
		info, err := e.Info()
		if err != nil {
			fmt.Fprintf(os.Stderr, "artifact: failed to inspect %s: %v\n", filename, err)
			uploadErrors = append(uploadErrors, fmt.Sprintf("%s: %v", filename, err))
			continue
		}
		if !info.Mode().IsRegular() {
			fmt.Fprintf(os.Stderr, "artifact: skipping non-regular file %s\n", filename)
			continue
		}
		// Reject symlinks and open relative to the already-open artifact directory
		// so the artifact root path is not re-resolved after the no-follow check.
		file, err := openAtNoFollow(dirFile, filename)
		if err != nil {
			if isNoFollowSkippable(err) {
				fmt.Fprintf(os.Stderr, "artifact: skipping unsafe or missing file %s: %v\n", filename, err)
				continue
			}
			fmt.Fprintf(os.Stderr, "artifact: failed to open %s: %v\n", filename, err)
			uploadErrors = append(uploadErrors, fmt.Sprintf("%s: %v", filename, err))
			continue
		}
		fi, err := file.Stat()
		if err != nil {
			_ = file.Close()
			fmt.Fprintf(os.Stderr, "artifact: failed to stat %s: %v\n", filename, err)
			uploadErrors = append(uploadErrors, fmt.Sprintf("%s: %v", filename, err))
			continue
		}
		if fi.IsDir() {
			_ = file.Close()
			continue
		}
		if fi.Size() > maxFileSize {
			_ = file.Close()
			fmt.Fprintf(os.Stderr, "artifact: skipping %s (%d bytes exceeds %d byte limit)\n", filename, fi.Size(), maxFileSize)
			continue
		}
		data, err := io.ReadAll(io.LimitReader(file, maxFileSize+1))
		_ = file.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "artifact: failed to read %s: %v\n", filename, err)
			uploadErrors = append(uploadErrors, fmt.Sprintf("%s: %v", filename, err))
			continue
		}

		if len(data) > maxFileSize {
			fmt.Fprintf(os.Stderr, "artifact: skipping %s (%d bytes exceeds %d byte limit)\n", filename, len(data), maxFileSize)
			continue
		}
		totalSize += int64(len(data))
		if totalSize > maxTotalSize {
			return fmt.Errorf("total artifact size %d bytes exceeds limit of %d", totalSize, maxTotalSize)
		}
		pending = append(pending, pendingArtifact{
			filename:    filename,
			data:        data,
			contentType: detectContentType(filename, data),
		})
	}

	for _, artifact := range pending {
		endpoint := fmt.Sprintf("%s/%s", baseEndpoint, url.PathEscape(artifact.filename))
		if err := doPostWithContentType(endpoint, artifact.data, saToken, artifact.contentType); err != nil {
			fmt.Fprintf(os.Stderr, "artifact: failed to upload %s: %v\n", artifact.filename, err)
			uploadErrors = append(uploadErrors, fmt.Sprintf("%s: %v", artifact.filename, err))
		} else {
			fmt.Printf("artifact: uploaded %s (%d bytes, %s)\n", artifact.filename, len(artifact.data), artifact.contentType)
		}
	}

	if len(uploadErrors) > 0 {
		return fmt.Errorf("some artifacts failed to upload: %s", strings.Join(uploadErrors, "; "))
	}
	return nil
}

func artifactEndpointBase() (string, error) {
	controllerURL := os.Getenv(workerenv.ControllerURL)
	if controllerURL == "" {
		return "", fmt.Errorf("%s must be set", workerenv.ControllerURL)
	}

	namespace := os.Getenv(workerenv.TaskNamespace)
	if namespace == "" {
		data, err := os.ReadFile(saNamespacePath)
		if err != nil {
			return "", fmt.Errorf("%s not set and cannot read namespace from SA: %w", workerenv.TaskNamespace, err)
		}
		namespace = strings.TrimSpace(string(data))
	}

	taskName := os.Getenv(workerenv.TaskName)
	if taskName == "" {
		return "", fmt.Errorf("%s must be set", workerenv.TaskName)
	}

	controllerURL = strings.TrimRight(controllerURL, "/")
	return fmt.Sprintf("%s/%s/%s/%s", controllerURL, artifactPath, namespace, taskName), nil
}

func detectContentType(filename string, data []byte) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".pdf":
		return "application/pdf"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".zip":
		return "application/zip"
	case ".json":
		return "application/json"
	case ".csv":
		return "text/csv"
	case ".html":
		return "text/html"
	case ".md":
		return "text/markdown"
	case ".txt":
		return "text/plain"
	}

	// Check for .tar.gz
	if strings.HasSuffix(strings.ToLower(filename), ".tar.gz") {
		return "application/gzip"
	}

	return http.DetectContentType(data)
}

func doPostWithContentType(endpoint string, data []byte, saToken, contentType string) error {
	var lastErr error
	for attempt := range artifactMaxRetries {
		if attempt > 0 {
			backoff := min(time.Duration(1<<uint(attempt))*time.Second, maxBackoff)
			time.Sleep(backoff)
		}

		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", contentType)
		if saToken != "" {
			req.Header.Set("Authorization", "Bearer "+saToken)
		}

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %w", err)
			fmt.Fprintf(os.Stderr, "artifact upload attempt %d/%d failed: %v\n", attempt+1, artifactMaxRetries, lastErr)
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close() //nolint:errcheck
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = &httpStatusError{Status: resp.StatusCode}
		if permanentSubmissionError(lastErr) {
			// The same classification as result submission: an oversized
			// artifact, rejected credentials, or disabled storage does not
			// change on retry.
			return fmt.Errorf("artifact upload rejected permanently: %w", lastErr)
		}
		fmt.Fprintf(os.Stderr, "artifact upload attempt %d/%d failed: %v\n", attempt+1, artifactMaxRetries, lastErr)
	}

	return fmt.Errorf("all %d artifact upload attempts failed: %w", artifactMaxRetries, lastErr)
}
