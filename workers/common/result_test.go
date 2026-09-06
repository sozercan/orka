/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package common

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/workerenv"
)

func TestSubmitResult_Success(t *testing.T) {
	var received []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("expected Content-Type application/octet-stream, got %s", r.Header.Get("Content-Type"))
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		received = buf[:n]
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)

	err := SubmitResult([]byte("hello result"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(received) != "hello result" {
		t.Errorf("received = %q, want %q", string(received), "hello result")
	}
}

func TestSubmitResult_RejectsBlank(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)

	for _, result := range [][]byte{nil, {}, []byte(" \n\t")} {
		if err := SubmitResult(result); err == nil || !strings.Contains(err.Error(), "must not be blank") {
			t.Fatalf("SubmitResult(%q) error = %v, want blank result error", result, err)
		}
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("submission attempts = %d, want 0", got)
	}
}

func TestSubmitResult_ResultStdoutWritesMarkerFile(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "orka-result-marker")
	originalMarkerPath := resultStdoutMarkerPath
	resultStdoutMarkerPath = markerPath
	t.Cleanup(func() {
		resultStdoutMarkerPath = originalMarkerPath
	})
	t.Setenv(workerenv.ResultStdout, "true")

	result := []byte(`{"kind":"typed-review","payload":"large-review"}`)
	wantMarker := workerenv.ResultStdoutPrefix + base64.StdEncoding.EncodeToString(result)

	var submitErr error
	stdout := captureStdout(t, func() {
		submitErr = SubmitResult(result)
	})
	if submitErr != nil {
		t.Fatalf("SubmitResult() error = %v", submitErr)
	}
	if !strings.Contains(stdout, wantMarker+"\n") {
		t.Fatalf("stdout = %q, want marker %q", stdout, wantMarker)
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read marker file: %v", err)
	}
	if string(data) != wantMarker+"\n" {
		t.Fatalf("marker file = %q, want %q", string(data), wantMarker+"\n")
	}
}

func TestSubmitResult_RetryOnFailure(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("temporary error")) //nolint:errcheck
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)

	err := SubmitResult([]byte("retry result"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestSubmitResult_AllRetriesFail(t *testing.T) {
	var slept []time.Duration
	retrySleep = func(d time.Duration) { slept = append(slept, d) }
	t.Cleanup(func() { retrySleep = time.Sleep })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("always fails")) //nolint:errcheck
	}))
	defer srv.Close()

	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)
	defer func() {
		// The backoff schedule must outlast a routine controller restart:
		// exponential up to the 60s cap, ~4 minutes in total.
		var total time.Duration
		for _, d := range slept {
			if d > maxBackoff {
				t.Fatalf("backoff %v exceeds cap %v", d, maxBackoff)
			}
			total += d
		}
		if total < 3*time.Minute {
			t.Fatalf("total backoff %v is shorter than a controller restart window", total)
		}
	}()

	err := SubmitResult([]byte("failing result"))
	if err == nil {
		t.Fatal("expected error after all retries exhausted")
	}
}

func TestSubmitResult_ConstructEndpointFromControllerURL(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	t.Setenv("ORKA_RESULT_ENDPOINT", "")
	t.Setenv("ORKA_CONTROLLER_URL", srv.URL)
	t.Setenv("ORKA_TASK_NAMESPACE", "test-ns")
	t.Setenv("ORKA_TASK_NAME", "my-task")

	err := SubmitResult([]byte("constructed url"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPath != "/internal/v1/results/test-ns/my-task" {
		t.Errorf("gotPath = %q, want /internal/v1/results/test-ns/my-task", gotPath)
	}
}

func TestSubmitResult_MissingEnvVars(t *testing.T) {
	t.Setenv("ORKA_RESULT_ENDPOINT", "")
	t.Setenv("ORKA_CONTROLLER_URL", "")

	err := SubmitResult([]byte("should fail"))
	if err == nil {
		t.Fatal("expected error when no endpoint or controller URL is set")
	}
}

func TestSubmitResult_BearerToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)

	// When no SA token file exists, no auth header is sent
	err := SubmitResult([]byte("no token"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Without the SA token file mounted, Authorization should be empty
	if gotAuth != "" {
		t.Logf("Authorization header present (SA token file may exist): %s", gotAuth)
	}
}

func TestSubmitResult_BearerTokenFromConfiguredPath(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("path-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)
	t.Setenv(workerenv.ServiceAccountTokenPath, tokenPath)
	t.Setenv(workerenv.ServiceAccountToken, "fallback-token")

	if err := SubmitResult([]byte("with token path")); err != nil {
		t.Fatalf("SubmitResult() error = %v", err)
	}
	if gotAuth != "Bearer path-token" {
		t.Fatalf("Authorization = %q, want Bearer path-token", gotAuth)
	}
}

func TestFormatStructuredResult(t *testing.T) {
	sr := &StructuredResult{
		Summary: "Added auth middleware",
		BaseSHA: "abc123",
		Diff:    "diff --git a/auth.go b/auth.go\n+// auth",
		Files:   []string{"auth.go"},
		Verdict: "APPROVED",
		Data:    map[string]any{"risk": "low", "count": float64(2)},
		Artifacts: []ArtifactRef{{
			Filename:    "evidence.json",
			ContentType: "application/json",
			Size:        128,
		}},
	}
	data, err := FormatStructuredResult(sr)
	if err != nil {
		t.Fatalf("FormatStructuredResult: %v", err)
	}
	// Should set version to 1
	var parsed StructuredResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Version != 1 {
		t.Errorf("expected version 1, got %d", parsed.Version)
	}
	if parsed.Summary != "Added auth middleware" {
		t.Errorf("expected summary %q, got %q", "Added auth middleware", parsed.Summary)
	}
	if parsed.Diff != sr.Diff {
		t.Errorf("diff mismatch")
	}
	if parsed.Data["risk"] != "low" || parsed.Data["count"] != float64(2) {
		t.Errorf("data mismatch: %#v", parsed.Data)
	}
	if len(parsed.Artifacts) != 1 || parsed.Artifacts[0].Filename != "evidence.json" {
		t.Errorf("artifacts mismatch: %#v", parsed.Artifacts)
	}
}

func TestFormatStructuredResult_PreservesVersion(t *testing.T) {
	sr := &StructuredResult{Version: 2, Summary: "test"}
	data, err := FormatStructuredResult(sr)
	if err != nil {
		t.Fatalf("FormatStructuredResult: %v", err)
	}
	var parsed StructuredResult
	_ = json.Unmarshal(data, &parsed)
	if parsed.Version != 2 {
		t.Errorf("expected version 2, got %d", parsed.Version)
	}
}

func TestParseStructuredResult_Valid(t *testing.T) {
	input := strings.Join([]string{
		`{"version":1,"summary":"done","baseSHA":"abc","diff":"patch",`,
		`"verdict":"APPROVED","files":["a.go"],"data":{"answer":42}}`,
	}, "")
	sr := ParseStructuredResult(input)
	if sr.Version != 1 {
		t.Errorf("expected version 1, got %d", sr.Version)
	}
	if sr.Summary != "done" {
		t.Errorf("expected summary %q, got %q", "done", sr.Summary)
	}
	if sr.Diff != "patch" {
		t.Errorf("expected diff %q, got %q", "patch", sr.Diff)
	}
	if sr.Verdict != "APPROVED" {
		t.Errorf("expected verdict APPROVED, got %q", sr.Verdict)
	}
	if sr.Data["answer"] != float64(42) {
		t.Errorf("expected data answer=42, got %#v", sr.Data)
	}
}

func TestParseStructuredResult_PlainText(t *testing.T) {
	sr := ParseStructuredResult("just some text output")
	if sr.Version != 1 {
		t.Errorf("expected version 1, got %d", sr.Version)
	}
	if sr.Summary != "just some text output" {
		t.Errorf("expected summary to be raw text, got %q", sr.Summary)
	}
	if sr.Diff != "" {
		t.Errorf("expected empty diff for plain text")
	}
}

func TestParseStructuredResult_InvalidJSON(t *testing.T) {
	sr := ParseStructuredResult("{bad json")
	if sr.Summary != "{bad json" {
		t.Errorf("expected raw text as summary")
	}
}

func TestParseStructuredResult_MissingVersion(t *testing.T) {
	// JSON without version field should be treated as plain text
	sr := ParseStructuredResult(`{"summary":"test"}`)
	if sr.Summary != `{"summary":"test"}` {
		t.Errorf("expected raw JSON as summary when version=0, got %q", sr.Summary)
	}
}

func TestSubmitResult_PermanentRejectionDoesNotRetry(t *testing.T) {
	var slept []time.Duration
	retrySleep = func(d time.Duration) { slept = append(slept, d) }
	t.Cleanup(func() { retrySleep = time.Sleep })

	permanent := []int{
		http.StatusRequestEntityTooLarge, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotImplemented,
	}
	for _, status := range permanent {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.WriteHeader(status)
			w.Write([]byte("rejected")) //nolint:errcheck
		}))
		t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)
		slept = nil
		err := SubmitResult([]byte("rejected result"))
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), "rejected permanently") {
			t.Fatalf("status %d: error = %v, want a permanent rejection", status, err)
		}
		if got := attempts.Load(); got != 1 {
			t.Fatalf("status %d: attempts = %d, want 1", status, got)
		}
		if len(slept) != 0 {
			t.Fatalf("status %d: slept %v before giving up", status, slept)
		}
	}
}

func TestSubmitResult_ThrottlingIsRetried(t *testing.T) {
	retrySleep = func(time.Duration) {}
	t.Cleanup(func() { retrySleep = time.Sleep })
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	t.Setenv("ORKA_RESULT_ENDPOINT", srv.URL)
	if err := SubmitResult([]byte("throttled result")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}
