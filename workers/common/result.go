/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package common

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	// resultMaxRetries and maxBackoff size the result submission window
	// (~4 minutes of backoff) so a finished worker's result survives a routine
	// single-replica controller restart (Recreate strategy: image pull, leader
	// election, and PVC reattach commonly take 1-3 minutes of API downtime).
	// Artifact uploads keep their own, shorter budget (artifactMaxRetries).
	resultMaxRetries = 9
	maxBackoff       = 60 * time.Second
	saTokenPath      = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saNamespacePath  = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// MaxStructuredSummaryChars bounds agent-written summaries stored in structured
	// results. Diffs remain intact for workspace handoff, but oversized summaries
	// can otherwise blow up coordinator context windows and provider request limits.
	MaxStructuredSummaryChars = 32 * 1024
)

const (
	// resultStdoutMarkerFile mirrors the stdout result marker to a file so a
	// supervising process can recover it when stdout is truncated.
	resultStdoutMarkerFile  = "/app/orka-result-marker"
	resultStdoutTokenPrefix = "ORKA_RESULT_TOKEN:"
)

var resultStdoutMarkerPath = resultStdoutMarkerFile

// retrySleep is stubbed by tests so retry-exhaustion cases do not spend the
// full multi-minute backoff window.
var retrySleep = time.Sleep

// SubmitResult sends the task result to the controller via HTTP POST.
// It reads ORKA_RESULT_ENDPOINT or constructs the URL from ORKA_CONTROLLER_URL.
// Retries with exponential backoff capped at maxBackoff (2s, 4s, 8s, 16s,
// 32s, then 60s steps — ~4 minutes in total) so a controller restart does not
// discard a completed worker's result.
func SubmitResult(result []byte) error {
	if len(bytes.TrimSpace(result)) == 0 {
		return fmt.Errorf("result must not be blank")
	}

	if workerenv.IsTrue(os.Getenv(workerenv.ResultStdout)) {
		marker := workerenv.ResultStdoutPrefix + base64.StdEncoding.EncodeToString(result)
		fileData := marker + "\n"
		if token := strings.TrimSpace(os.Getenv(workerenv.ResultStdoutToken)); token != "" {
			fileData = resultStdoutTokenPrefix + token + "\n" + fileData
		}
		if err := os.WriteFile(resultStdoutMarkerPath, []byte(fileData), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to write stdout result marker file: %v\n", err)
		}
		fmt.Println(marker)
		return nil
	}

	endpoint, err := resultEndpoint()
	if err != nil {
		return err
	}

	saToken := workerServiceAccountToken()

	var lastErr error
	for attempt := range resultMaxRetries {
		if attempt > 0 {
			backoff := min(time.Duration(1<<uint(attempt))*time.Second, maxBackoff)
			retrySleep(backoff)
		}

		lastErr = doPost(endpoint, result, saToken)
		if lastErr == nil {
			return nil
		}
		if permanentSubmissionError(lastErr) {
			// A definitive client-side rejection (oversized result, invalid
			// worker authorization, result storage disabled) does not change
			// by resending the identical request; fail now so the Job and Task
			// settle instead of idling through the multi-minute window.
			return fmt.Errorf("result submission rejected permanently: %w", lastErr)
		}
		fmt.Fprintf(os.Stderr, "result submission attempt %d/%d failed: %v\n", attempt+1, resultMaxRetries, lastErr)
	}

	return fmt.Errorf("all %d result submission attempts failed: %w", resultMaxRetries, lastErr)
}

// httpStatusError carries a non-2xx controller response so callers can tell
// permanent rejections from transient failures.
type httpStatusError struct {
	Status int
	Body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body)
}

// permanentSubmissionError reports whether err is a controller response that
// will not change on retry: every 4xx except timeouts (408) and throttling
// (429), plus 501 (result storage disabled). Transport errors, 5xx, and
// throttling remain retryable.
func permanentSubmissionError(err error) bool {
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.Status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	case http.StatusNotImplemented:
		return true
	}
	return statusErr.Status >= 400 && statusErr.Status < 500
}

func resultEndpoint() (string, error) {
	// Prefer explicit endpoint
	if ep := os.Getenv(workerenv.ResultEndpoint); ep != "" {
		return ep, nil
	}

	// Construct from controller URL + task identity
	controllerURL := os.Getenv(workerenv.ControllerURL)
	if controllerURL == "" {
		return "", fmt.Errorf("%s or %s must be set", workerenv.ResultEndpoint, workerenv.ControllerURL)
	}

	namespace := os.Getenv(workerenv.TaskNamespace)
	if namespace == "" {
		// Fall back to downward API namespace file
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
	return fmt.Sprintf("%s/internal/v1/results/%s/%s", controllerURL, namespace, taskName), nil
}

func workerServiceAccountToken() string {
	if path := strings.TrimSpace(os.Getenv(workerenv.ServiceAccountTokenPath)); path != "" {
		if token, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(token))
		}
	}

	if token, err := os.ReadFile(saTokenPath); err == nil {
		return strings.TrimSpace(string(token))
	}

	return strings.TrimSpace(os.Getenv(workerenv.ServiceAccountToken))
}

func doPost(endpoint string, data []byte, saToken string) error {
	return doPostOnceWithContentType(endpoint, data, saToken, "application/octet-stream", 30*time.Second)
}

func doPostOnceWithContentType(endpoint string, data []byte, saToken, contentType string, timeout time.Duration) error {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	if saToken != "" {
		req.Header.Set("Authorization", "Bearer "+saToken)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return &httpStatusError{Status: resp.StatusCode, Body: string(body)}
}

// StructuredResult is an optional structured envelope for task results.
// Workers can use this to include diffs, verdicts, and metadata alongside
// the human-readable summary. Plain-text results remain backward compatible.
type StructuredResult struct {
	Version    int      `json:"version"`
	Summary    string   `json:"summary"`
	BaseSHA    string   `json:"baseSHA,omitempty"`
	HeadSHA    string   `json:"headSHA,omitempty"`
	Diff       string   `json:"diff,omitempty"`
	Verdict    string   `json:"verdict,omitempty"`
	Feedback   string   `json:"feedback,omitempty"`
	Files      []string `json:"files,omitempty"`
	PushBranch string   `json:"pushBranch,omitempty"`
	PushError  string   `json:"pushError,omitempty"`
	// Data carries generic machine-readable task output. Keep large payloads in
	// artifacts and put references here; parent/coordinator summaries may bound it.
	Data      map[string]any `json:"data,omitempty"`
	Artifacts []ArtifactRef  `json:"artifacts,omitempty"`
}

// ArtifactRef is a safe structured reference to a task artifact. The artifact
// bytes remain in Orka artifact storage; this envelope carries only metadata
// that coordinators and remote runtimes can use to fetch or reason about it.
type ArtifactRef struct {
	Filename    string `json:"filename"`
	ContentType string `json:"contentType,omitempty"`
	Size        int64  `json:"size,omitempty"`
	Description string `json:"description,omitempty"`
}

// FormatStructuredResult serializes a StructuredResult to JSON bytes.
func FormatStructuredResult(r *StructuredResult) ([]byte, error) {
	if r.Version == 0 {
		r.Version = 1
	}
	return json.Marshal(r)
}

// ParseStructuredResult attempts to parse a result string as a StructuredResult.
// If the input is not valid JSON or doesn't have the expected structure,
// it returns a StructuredResult with the raw input as Summary (backward compatible).
func ParseStructuredResult(raw string) *StructuredResult {
	var sr StructuredResult
	if err := json.Unmarshal([]byte(raw), &sr); err != nil || sr.Version == 0 {
		return &StructuredResult{
			Version: 1,
			Summary: raw,
		}
	}
	return &sr
}

// TruncateStructuredSummary bounds human-readable result summaries while making
// truncation explicit to downstream coordinators.
func TruncateStructuredSummary(summary string) string {
	if len(summary) <= MaxStructuredSummaryChars {
		return summary
	}
	return summary[:MaxStructuredSummaryChars] + fmt.Sprintf(
		"\n[summary truncated, full summary: %d chars]",
		len(summary),
	)
}
