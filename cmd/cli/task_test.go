/* Copyright (c) 2026. MIT License - see LICENSE file for details. */

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/cli/client"
)

func TestFormatAge(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
		wantExact string // exact match, or "" if we just want a suffix check
		wantSfx   string // suffix to check
	}{
		{"empty", "", "<unknown>", ""},
		{"invalid", "not-a-date", "not-a-date", ""},
		{"seconds_ago", time.Now().Add(-30 * time.Second).Format(time.RFC3339), "", "s"},
		{"minutes_ago", time.Now().Add(-5 * time.Minute).Format(time.RFC3339), "", "m"},
		{"hours_ago", time.Now().Add(-3 * time.Hour).Format(time.RFC3339), "", "h"},
		{"days_ago", time.Now().Add(-48 * time.Hour).Format(time.RFC3339), "", "d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatAge(tt.timestamp)
			if tt.wantExact != "" {
				if got != tt.wantExact {
					t.Errorf("formatAge(%q) = %q, want %q", tt.timestamp, got, tt.wantExact)
				}
			} else if tt.wantSfx != "" {
				if len(got) == 0 {
					t.Errorf("formatAge(%q) returned empty string", tt.timestamp)
				} else if got[len(got)-1:] != tt.wantSfx {
					t.Errorf("formatAge(%q) = %q, want suffix %q", tt.timestamp, got, tt.wantSfx)
				}
			}
		})
	}
}

func TestNewTaskCmd(t *testing.T) {
	cmd := newTaskCmd()

	if cmd.Use != "task" {
		t.Errorf("Use = %q, want %q", cmd.Use, "task")
	}

	// Verify subcommands exist
	subNames := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subNames[sub.Use] = true
	}
	for _, want := range []string{"create <prompt>", "list", "get <name>", "status <name>", "logs <name>", "delete <name>"} {
		if !subNames[want] {
			t.Errorf("missing subcommand %q", want)
		}
	}
}

func TestNewTaskCreateCmdFlags(t *testing.T) {
	cmd := newTaskCreateCmd()

	// Verify flags
	for _, flagName := range []string{
		"type", "agent", "provider", "timeout", "workspace-intent", "git-repo",
		"read-credential", "read-credential-key", "publication-git-repo",
		"publication-read-credential", "publication-read-credential-key",
		"publication-credential", "publication-credential-key", "forge-credential", "forge-credential-key",
		"push-branch", "create-pr",
	} {
		if cmd.Flags().Lookup(flagName) == nil {
			t.Errorf("missing flag %q", flagName)
		}
	}

	// Verify default values
	typeVal, _ := cmd.Flags().GetString("type")
	if typeVal != "ai" {
		t.Errorf("default type = %q, want %q", typeVal, "ai")
	}
	providerVal, _ := cmd.Flags().GetString("provider")
	if providerVal != "" {
		t.Errorf("default provider = %q, want it unset so the sole ready Provider is used", providerVal)
	}
}

func TestNewTaskListCmdAliases(t *testing.T) {
	cmd := newTaskListCmd()

	found := slices.Contains(cmd.Aliases, "ls")
	if !found {
		t.Error("expected 'ls' alias on list command")
	}

	// Verify flags
	if cmd.Flags().Lookup("status") == nil {
		t.Error("missing flag 'status'")
	}
	if cmd.Flags().Lookup("limit") == nil {
		t.Error("missing flag 'limit'")
	}
}

func TestNewTaskGetCmdArgs(t *testing.T) {
	cmd := newTaskGetCmd()
	cmd.SetArgs([]string{})
	// Without args, should error (requires exactly 1)
	err := cmd.Execute()
	if err == nil {
		t.Error("expected error when no args provided")
	}
}

func TestWaitForTaskPhaseContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForTaskPhase(
		ctx,
		"my-task",
		nil,
		time.Millisecond,
		func(context.Context) (string, error) {
			return "Running", nil
		},
		io.Discard,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForTaskPhase error = %v, want context.Canceled", err)
	}
}

func TestNewTaskDeleteCmdAliases(t *testing.T) {
	cmd := newTaskDeleteCmd()
	found := slices.Contains(cmd.Aliases, "rm")
	if !found {
		t.Error("expected 'rm' alias on delete command")
	}
}

func TestNewTaskLogsCmdFlags(t *testing.T) {
	cmd := newTaskLogsCmd()

	flag := cmd.Flags().Lookup("follow")
	if flag == nil {
		t.Fatal("missing flag 'follow'")
	}
	if flag.Shorthand != "f" {
		t.Errorf("follow shorthand = %q, want %q", flag.Shorthand, "f")
	}
}

// ---------------------------------------------------------------------------
// Task command execution with mock servers
// ---------------------------------------------------------------------------

func taskAPIServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == tasksAPIPath:
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"metadata": map[string]any{"name": "task-abc123", "namespace": "default"},
			})
		case r.Method == http.MethodGet && r.URL.Path == cliProvidersAPIPath:
			// One ready Provider: ai tasks created without --provider use it.
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				testProviderItemsKey: []map[string]any{{
					"metadata":            map[string]any{"name": testProviderSecondary, "namespace": "default"},
					testProviderStatusKey: map[string]any{testProviderReadyKey: true},
				}},
			})
		case r.Method == http.MethodGet && r.URL.Path == tasksAPIPath:
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "t1", "namespace": "default",
							"creationTimestamp": time.Now().Add(-5 * time.Minute).Format(time.RFC3339),
							"labels":            map[string]any{"orka.ai/parent-task": "parent-task"},
						},
						"spec": map[string]any{
							"type":        "ai",
							"transaction": map[string]any{"id": "txn-123"},
						},
						"status": map[string]any{"phase": "Succeeded"},
					},
					{
						"metadata": map[string]any{
							"name": "t2", "namespace": "default",
							"creationTimestamp": time.Now().Add(-2 * time.Hour).Format(time.RFC3339),
						},
						"spec":   map[string]any{"type": "container"},
						"status": map[string]any{"phase": "Running"},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/my-task":
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"metadata": map[string]any{"name": "my-task"},
				"spec": map[string]any{
					"transaction": map[string]any{
						"id":      "txn-123",
						"profile": "transaction-token",
					},
				},
				"status": map[string]any{"phase": "Succeeded"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/my-task/logs":
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"logs": "line1\nline2\n",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/msg-task/logs":
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"message": "Task is still pending",
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/tasks/my-task":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "not found: %s %s", r.Method, r.URL.Path) //nolint:errcheck
		}
	}))
}

func TestNewTaskCreateCmd_Execute(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "do something"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskCreateCmd_WithAgent(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--agent", "my-agent", "--type", "agent", "do stuff"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestTaskCreateMaterializesExternalRuntimeAllowedTools(t *testing.T) {
	tests := []struct {
		name            string
		contractVersion corev1alpha1.AgentRuntimeContractVersion
		allowedTools    []string
		explicitType    bool
	}{
		{
			name:            "harness v2 registered allowlist",
			contractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
			allowedTools:    []string{"read_file", "search_code"},
		},
		{
			name:            "harness v2 registered deny-all",
			contractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
			allowedTools:    []string{},
			explicitType:    true,
		},
		{
			name:            "harness v1 required deny-all",
			contractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
			allowedTools:    []string{},
			explicitType:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)

			var created struct {
				AgentRuntime *struct {
					AllowedTools *[]string `json:"allowedTools"`
				} `json:"agentRuntime"`
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/external-agent":
					fmt.Fprint(w, `{"metadata":{"name":"external-agent"},"spec":{"runtime":{"runtimeRef":{"name":"external-runtime"}}}}`) //nolint:errcheck
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtimes/external-runtime":
					runtimeSpec := map[string]any{"contractVersion": tt.contractVersion}
					if tt.contractVersion == corev1alpha1.AgentRuntimeContractHarnessV2 {
						runtimeSpec["capabilities"] = map[string]any{
							"mcpPolicy": map[string]any{"allowedTools": tt.allowedTools},
							"profile":   map[string]any{"workspaceIntent": "read"},
						}
					}
					json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
						"metadata": map[string]any{"name": "external-runtime"},
						"spec":     runtimeSpec,
					})
				case r.Method == http.MethodPost && r.URL.Path == tasksAPIPath:
					if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
						t.Errorf("decode create request: %v", err)
					}
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, `{"metadata":{"name":"task-external"}}`) //nolint:errcheck
				default:
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprintf(w, "not found: %s %s", r.Method, r.URL.Path) //nolint:errcheck
				}
			}))
			defer srv.Close()

			args := []string{"task", "create", "--server", srv.URL, "--agent", "external-agent"}
			if tt.explicitType {
				args = append(args, "--type", "agent")
			}
			args = append(args, "do stuff")
			root := newRootCmd()
			root.SetArgs(args)
			if err := root.Execute(); err != nil {
				t.Fatalf("Execute() error: %v", err)
			}
			if created.AgentRuntime == nil || created.AgentRuntime.AllowedTools == nil {
				t.Fatalf("agentRuntime = %#v, want explicit allowedTools", created.AgentRuntime)
			}
			if !slices.Equal(*created.AgentRuntime.AllowedTools, tt.allowedTools) {
				t.Fatalf("allowedTools = %#v, want %#v", *created.AgentRuntime.AllowedTools, tt.allowedTools)
			}
		})
	}
}

func TestTaskCreateRejectsExternalRuntimeWorkspaceIntentMismatch(t *testing.T) {
	tests := []struct {
		name          string
		taskIntent    corev1alpha1.WorkspaceIntent
		profileIntent corev1alpha1.WorkspaceIntent
	}{
		{name: "read Task with write profile", taskIntent: corev1alpha1.WorkspaceIntentRead, profileIntent: corev1alpha1.WorkspaceIntentWrite},
		{name: "write Task with read profile", taskIntent: corev1alpha1.WorkspaceIntentWrite, profileIntent: corev1alpha1.WorkspaceIntentRead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			postCount := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/external-agent":
					fmt.Fprint(w, `{"metadata":{"name":"external-agent"},"spec":{"runtime":{"runtimeRef":{"name":"external-runtime"}}}}`) //nolint:errcheck
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtimes/external-runtime":
					json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
						"metadata": map[string]any{"name": "external-runtime"},
						"spec": map[string]any{
							"contractVersion": "orka.harness.v2",
							"capabilities": map[string]any{
								"mcpPolicy": map[string]any{"allowedTools": []string{}},
								"profile":   map[string]any{"workspaceIntent": tt.profileIntent},
							},
						},
					})
				case r.Method == http.MethodPost && r.URL.Path == tasksAPIPath:
					postCount++
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, `{"metadata":{"name":"unexpected-task"}}`) //nolint:errcheck
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()

			args := []string{"task", "create", "--server", srv.URL, "--agent", "external-agent", "--type", "agent"}
			if tt.taskIntent == corev1alpha1.WorkspaceIntentWrite {
				args = append(args,
					"--workspace-intent", "write",
					"--git-repo", "https://github.com/source/repo",
					"--publication-credential", "repo-write",
				)
			}
			args = append(args, "do stuff")
			root := newRootCmd()
			root.SetArgs(args)
			err := root.Execute()
			want := fmt.Sprintf("profile workspace intent %q does not match Task intent %q", tt.profileIntent, tt.taskIntent)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Execute() error = %v, want %q", err, want)
			}
			if postCount != 0 {
				t.Fatalf("Task POST count = %d, want 0", postCount)
			}
		})
	}
}

func TestNewTaskListCmd_Execute(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskListCmd_WithStatus(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL, "--status", "Running"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskListCmd_WithStatusScansPaginatedResults(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != tasksAPIPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		requests = append(requests, r.URL.RawQuery)
		if got := r.URL.Query().Get("limit"); got != "500" {
			t.Errorf("limit query = %q, want 500 for filtered pagination", got)
		}

		switch r.URL.Query().Get("continue") {
		case "":
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              "first-page-task",
							"namespace":         "default",
							"creationTimestamp": time.Now().Format(time.RFC3339),
						},
						"spec":   map[string]any{"type": "ai"},
						"status": map[string]any{"phase": "Succeeded"},
					},
				},
				"metadata": map[string]any{"continue": "next"},
			})
		case "next":
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              "matching-task",
							"namespace":         "default",
							"creationTimestamp": time.Now().Format(time.RFC3339),
						},
						"spec":   map[string]any{"type": "ai"},
						"status": map[string]any{"phase": "Running"},
					},
				},
			})
		default:
			t.Errorf("unexpected continue token %q", r.URL.Query().Get("continue"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL, "--status", "Running"})

	stdout, err := captureOutput(t, root.Execute)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if !strings.Contains(stdout, "matching-task") {
		t.Fatalf("stdout = %q, want paginated matching task", stdout)
	}
}

func TestNewTaskListCmd_WithStatusFallsBackWhenCacheRejectsPagination(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != tasksAPIPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		requests = append(requests, r.URL.RawQuery)
		if r.URL.Query().Get("limit") != "" && r.URL.Query().Get("limit") != "0" {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"error": map[string]any{"message": "failed to list tasks: continue list option is not supported by the cache"},
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"items": []map[string]any{
				{
					"metadata": map[string]any{
						"name":              "matching-task",
						"namespace":         "default",
						"creationTimestamp": time.Now().Format(time.RFC3339),
					},
					"spec":   map[string]any{"type": "ai"},
					"status": map[string]any{"phase": "Running"},
				},
			},
		})
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL, "--status", "Running"})

	stdout, err := captureOutput(t, root.Execute)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want initial paginated request plus fallback", len(requests))
	}
	if !strings.Contains(requests[0], "limit=500") {
		t.Fatalf("first query = %q, want paginated request", requests[0])
	}
	if !strings.Contains(requests[1], "limit=0") || strings.Contains(requests[1], "continue=") {
		t.Fatalf("fallback query = %q, want explicit unpaginated request", requests[1])
	}
	if !strings.Contains(stdout, "matching-task") {
		t.Fatalf("stdout = %q, want fallback matching task", stdout)
	}
}

func TestNewTaskListCmd_WithTransaction(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL, "--transaction", "txn-123"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskListCmd_Empty(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"items": []any{}}) //nolint:errcheck
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskGetCmd_Execute(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "get", "--server", srv.URL, "my-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskGetCmd_ShowTransaction(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "get", "--server", srv.URL, "--show-transaction", "my-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskGetCmd_NotFound(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "get", "--server", srv.URL, "nonexistent"})

	if err := root.Execute(); err == nil {
		t.Error("expected error for nonexistent task")
	}
}

func TestNewTaskLogsCmd_Execute(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "logs", "--server", srv.URL, "my-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskLogsCmd_MessageOnly(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "logs", "--server", srv.URL, "msg-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskDeleteCmd_Execute(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "delete", "--server", srv.URL, "my-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskDeleteCmd_NotFound(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "delete", "--server", srv.URL, "nonexistent"})

	if err := root.Execute(); err == nil {
		t.Error("expected error for nonexistent task")
	}
}

func TestNewTaskLogsCmd_NoArgs(t *testing.T) {
	cmd := newTaskLogsCmd()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when no args")
	}
}

func TestNewTaskDeleteCmd_NoArgs(t *testing.T) {
	cmd := newTaskDeleteCmd()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when no args")
	}
}

func TestNewTaskLogsCmd_Follow(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/tasks/my-task/logs" && r.URL.Query().Get("follow") == "true" {
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, ok := w.(http.Flusher)
			fmt.Fprint(w, "event: log\ndata: {\"line\":\"log line 1\"}\n\n") //nolint:errcheck
			if ok {
				flusher.Flush()
			}
			// Close immediately to end stream
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "logs", "--server", srv.URL, "--follow", "my-task"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskLogsCmd_NotFound(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "not found") //nolint:errcheck
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "logs", "--server", srv.URL, "nonexistent"})

	if err := root.Execute(); err == nil {
		t.Error("expected error for nonexistent task logs")
	}
}

func TestNewTaskCreateCmd_NoArgs(t *testing.T) {
	cmd := newTaskCreateCmd()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when no args")
	}
}

func TestNewTaskListCmd_WithStatusNoMatch(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "list", "--server", srv.URL, "--status", "Pending"})

	// Should display "No tasks found." since no Pending tasks
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestNewTaskCreateCmd_ContainerType(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	srv := taskAPIServer()
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--type", "container", "run my container"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
}

func TestTaskCreateManifestInjectsNamespace(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	manifest := filepath.Join(tmp, "task.yaml")
	manifestData := []byte("metadata:\n  name: manifest-task\nspec:\n  type: container\n  image: alpine\n")
	if err := os.WriteFile(manifest, manifestData, 0o600); err != nil {
		t.Fatal(err)
	}

	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != tasksAPIPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"metadata": map[string]any{"name": "manifest-task", "namespace": "team-a"},
		})
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--namespace", "team-a", "-f", manifest})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	metadata, ok := captured["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata missing in request: %#v", captured)
	}
	if got := metadata["namespace"]; got != "team-a" {
		t.Fatalf("metadata.namespace = %#v, want team-a", got)
	}
}

func TestTaskCreateRequiresPromptForDefaultAI(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", "http://127.0.0.1:1"})
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "prompt is required") {
		t.Fatalf("Execute() error = %v, want prompt required", err)
	}
}

const (
	testProviderItemsKey  = "items"
	testProviderReadyKey  = "ready"
	testProviderStatusKey = "status"
	testProviderPrimary   = "anthropic-prod"
	testProviderSecondary = "openai-prod"
)

func TestNewTaskCreateCmdInfersTypeAndProvider(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == tasksAPIPath:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			bodies = append(bodies, body)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "task-inferred"}}) //nolint:errcheck
		case r.Method == http.MethodGet && r.URL.Path == cliProvidersAPIPath:
			json.NewEncoder(w).Encode(map[string]any{testProviderItemsKey: []map[string]any{ //nolint:errcheck
				{"metadata": map[string]any{"name": testProviderPrimary}, testProviderStatusKey: map[string]any{testProviderReadyKey: true}},
				{"metadata": map[string]any{"name": testProviderSecondary}, testProviderStatusKey: map[string]any{testProviderReadyKey: false}},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// --image without --type is a container task and must not reference a Provider.
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--image", "busybox", "--command", "sh", "--arg", "-c", "--arg", "echo hi", "run the container"})
	if err := root.Execute(); err != nil {
		t.Fatalf("container Execute() error: %v", err)
	}
	// An ai task without --provider resolves the sole ready Provider.
	root = newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "summarize"})
	if err := root.Execute(); err != nil {
		t.Fatalf("ai Execute() error: %v", err)
	}
	if len(bodies) != 2 {
		t.Fatalf("created %d tasks, want 2", len(bodies))
	}
	if bodies[0]["type"] != "container" || bodies[0]["ai"] != nil {
		t.Fatalf("container request = %#v, want type container without ai", bodies[0])
	}
	ai, _ := bodies[1]["ai"].(map[string]any)
	ref, _ := ai["providerRef"].(map[string]any)
	if bodies[1]["type"] != "ai" || ref["name"] != testProviderPrimary {
		t.Fatalf("ai request = %#v, want the sole ready Provider anthropic", bodies[1])
	}
}

func TestNewTaskCreateCmdExplainsAmbiguousProviders(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == cliProvidersAPIPath {
			json.NewEncoder(w).Encode(map[string]any{testProviderItemsKey: []map[string]any{ //nolint:errcheck
				{"metadata": map[string]any{"name": testProviderPrimary}, testProviderStatusKey: map[string]any{testProviderReadyKey: true}},
				{"metadata": map[string]any{"name": testProviderSecondary}, testProviderStatusKey: map[string]any{testProviderReadyKey: true}},
			}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "summarize"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), testProviderPrimary+", "+testProviderSecondary) || !strings.Contains(err.Error(), "--provider") {
		t.Fatalf("Execute() error = %v, want the available Providers and a --provider hint", err)
	}
}

// TestResolveDefaultProviderPrefersReadyDefault mirrors the server resolver:
// a ready Provider named "default" wins even when another ready Provider
// exists; otherwise a sole ready Provider is used.
func TestResolveDefaultProviderPrefersReadyDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"name":"openai","ready":true},{"name":"default","ready":true}]}`) //nolint:errcheck
	}))
	defer srv.Close()
	c := client.NewWithNamespace(srv.URL, "", "default")
	name, err := resolveDefaultProviderName(context.Background(), c)
	if err != nil || name != "default" {
		t.Fatalf("resolveDefaultProviderName() = %q, %v; want the ready Provider named default", name, err)
	}
}

// TestTaskCreateAgentTypeInference verifies --agent resolves the referenced
// Agent before choosing the task type: a native AI Agent keeps the ai default
// (the documented self-bootstrapping flow), a runtime Agent infers agent, and
// an unreadable Agent conservatively falls back to ai.
// TestTaskCreateRejectsAmbiguousImageAndAgent: --image and --agent imply
// different task types; without an explicit --type the CLI must fail fast
// instead of silently choosing container and carrying the agentRef along.
func TestTaskCreateRejectsAmbiguousImageAndAgent(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks" {
			t.Error("task was created from an ambiguous flag combination")
		}
		fmt.Fprint(w, `{}`) //nolint:errcheck
	}))
	defer srv.Close()
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--image", "alpine", "--agent", "coordinator", "do stuff"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("Execute() error = %v, want ambiguous-flags rejection", err)
	}
}

// TestTaskCreateAgentTypeSurfacesForbiddenLookup: a token without agents
// read permission must not silently submit the wrong task type.
func TestTaskCreateAgentTypeSurfacesForbiddenLookup(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/guarded" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":"forbidden"}`) //nolint:errcheck
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks" {
			t.Error("task was created despite an undetermined type")
		}
		fmt.Fprint(w, `{}`) //nolint:errcheck
	}))
	defer srv.Close()
	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--agent", "guarded", "do stuff"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "pass --type") {
		t.Fatalf("Execute() error = %v, want explicit --type guidance", err)
	}
}

func TestTaskCreateExplicitAgentTypeSurfacesForbiddenPolicyLookup(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	postCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/guarded":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":"forbidden"}`) //nolint:errcheck
		case r.Method == http.MethodPost && r.URL.Path == tasksAPIPath:
			postCount++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"metadata":{"name":"unexpected-task"}}`) //nolint:errcheck
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	root := newRootCmd()
	root.SetArgs([]string{"task", "create", "--server", srv.URL, "--agent", "guarded", "--type", "agent", "do stuff"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "resolve AgentRuntime policy") || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("Execute() error = %v, want forbidden AgentRuntime policy lookup", err)
	}
	if postCount != 0 {
		t.Fatalf("Task POST count = %d, want 0", postCount)
	}
}

func TestTaskCreateAgentTypeRejectsMalformedLookup(t *testing.T) {
	for name, responseBody := range map[string]string{
		"invalid json": `{"spec":`,
		"null object":  `null`,
		"missing name": `{ "spec": {} }`,
	} {
		t.Run(name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/broken" {
					fmt.Fprint(w, responseBody) //nolint:errcheck
					return
				}
				if r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks" {
					t.Error("task was created despite an invalid Agent response")
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer srv.Close()

			root := newRootCmd()
			root.SetArgs([]string{"task", "create", "--server", srv.URL, "--agent", "broken", "do stuff"})
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), "pass --type") {
				t.Fatalf("Execute() error = %v, want explicit --type guidance", err)
			}
		})
	}
}

func TestTaskCreateAgentTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		agent    string
		body     string
		status   int
		wantType string
	}{
		{name: "native ai agent", agent: "coordinator", body: `{"metadata":{"name":"coordinator"},"spec":{"providerRef":{"name":"p"}}}`, status: http.StatusOK, wantType: "ai"},
		{name: "runtime agent", agent: "codex-agent", body: `{"metadata":{"name":"codex-agent"},"spec":{"runtime":{"type":"codex"}}}`, status: http.StatusOK, wantType: "agent"},
		{name: "missing agent keeps ai default", agent: "missing", body: `{"error":"not found"}`, status: http.StatusNotFound, wantType: "ai"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			var created struct {
				Type string `json:"type"`
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agents/"+tc.agent:
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body) //nolint:errcheck
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/providers":
					fmt.Fprint(w, `{"items":[{"name":"p","ready":true}]}`) //nolint:errcheck
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks":
					if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
						t.Errorf("decode create request: %v", err)
					}
					fmt.Fprint(w, `{"metadata":{"name":"task-x"}}`) //nolint:errcheck
				default:
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, `{}`) //nolint:errcheck
				}
			}))
			defer srv.Close()
			root := newRootCmd()
			root.SetArgs([]string{"task", "create", "--server", srv.URL, "--agent", tc.agent, "do stuff"})
			if err := root.Execute(); err != nil {
				t.Fatalf("Execute() error: %v", err)
			}
			if created.Type != tc.wantType {
				t.Fatalf("created task type = %q, want %q", created.Type, tc.wantType)
			}
		})
	}
}
