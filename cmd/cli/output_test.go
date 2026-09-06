package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestPrintGenericTableRendersTypedSingleObject(t *testing.T) {
	cmd := &cobra.Command{}
	var out strings.Builder
	cmd.SetOut(&out)
	type detail struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	}
	var value detail
	value.Metadata.Name, value.Metadata.Namespace, value.Status.Phase = "cli-hello", "cli-ns", "Completed"
	if err := printGenericTable(cmd, &value); err != nil {
		t.Fatalf("printGenericTable() error = %v", err)
	}
	if strings.Contains(out.String(), "No resources found") || !strings.Contains(out.String(), "cli-hello") || !strings.Contains(out.String(), "Completed") {
		t.Fatalf("table output = %q", out.String())
	}
}

const (
	tableTestItemsKey = "items"
	tableTestStateKey = "state"
)

func TestPrintGenericTableRendersStatusReadyBool(t *testing.T) {
	for _, tc := range []struct {
		ready bool
		want  string
	}{{true, "Ready"}, {false, "NotReady"}} {
		cmd := &cobra.Command{}
		var out strings.Builder
		cmd.SetOut(&out)
		value := map[string]any{tableTestItemsKey: []any{map[string]any{
			"metadata": map[string]any{"name": "openai-vekil", "namespace": "orka-system"},
			"status":   map[string]any{"ready": tc.ready},
		}}}
		if err := printGenericTable(cmd, value); err != nil {
			t.Fatalf("printGenericTable() error = %v", err)
		}
		if !strings.Contains(out.String(), tc.want) {
			t.Fatalf("table output %q lacks %q", out.String(), tc.want)
		}
	}
}

func TestPrintGenericTableLabelsForgeRecordsByNumber(t *testing.T) {
	cmd := &cobra.Command{}
	var out strings.Builder
	cmd.SetOut(&out)
	const namespace = "monitor-ns"
	value := map[string]any{tableTestItemsKey: []any{map[string]any{
		"monitorNamespace": namespace,
		"number":           float64(358),
		"title":            strings.Repeat("long title ", 12),
		tableTestStateKey:  "open",
		"workflowPhase":    "approval_required",
		"updatedAt":        "2026-08-30T17:00:00Z",
	}}}
	if err := printGenericTable(cmd, value); err != nil {
		t.Fatalf("printGenericTable() error = %v", err)
	}
	got := out.String()
	for _, want := range []string{"#358 long title", "…", namespace, "open/approval_required"} {
		if !strings.Contains(got, want) {
			t.Fatalf("table output %q lacks %q", got, want)
		}
	}
	if strings.Contains(got, "-\t") || strings.Contains(got, strings.Repeat("long title ", 12)) {
		t.Fatalf("table output %q rendered a dash name or an untruncated title", got)
	}
}

func TestPrintGenericTableStripsTerminalControlsFromForgeTitles(t *testing.T) {
	cmd := &cobra.Command{}
	var out strings.Builder
	cmd.SetOut(&out)
	value := map[string]any{tableTestItemsKey: []any{map[string]any{
		"number":          float64(7),
		"title":           "safe\x1b]8;;https://example.invalid\a link\x1b]8;;\a\u202Ehidden",
		tableTestStateKey: "open",
	}}}
	if err := printGenericTable(cmd, value); err != nil {
		t.Fatalf("printGenericTable() error = %v", err)
	}
	got := out.String()
	if strings.ContainsAny(got, "\x1b\a") || strings.ContainsRune(got, '\u202e') {
		t.Fatalf("table output retains terminal control text: %q", got)
	}
	if !strings.Contains(got, "#7 safe]8;;https://example.invalid link]8;;hidden") {
		t.Fatalf("table output lost visible title text: %q", got)
	}
}

func TestPrintGenericTableOmitsEmptyColumns(t *testing.T) {
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.Flags().StringP("output", "o", "table", "")
	items := []any{
		map[string]any{"id": "gpt-5.6-sol"},
		map[string]any{"id": "claude-opus-5"},
	}
	if err := printGenericTable(cmd, map[string]any{"items": items}); err != nil {
		t.Fatalf("printGenericTable: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected header and two rows, got %q", out.String())
	}
	if strings.TrimSpace(lines[0]) != "NAME" {
		t.Fatalf("expected only the NAME column, got header %q", lines[0])
	}
	for _, line := range lines[1:] {
		if strings.Contains(line, "\t") || strings.TrimSpace(line) == "-" {
			t.Fatalf("expected single-column rows without placeholders, got %q", line)
		}
	}

	out.Reset()
	items = []any{
		map[string]any{"name": "a", "namespace": "ns", "status": map[string]any{"ready": true}},
		map[string]any{"name": "b"},
	}
	if err := printGenericTable(cmd, map[string]any{"items": items}); err != nil {
		t.Fatalf("printGenericTable: %v", err)
	}
	lines = strings.Split(strings.TrimSpace(out.String()), "\n")
	if !strings.HasPrefix(lines[0], "NAME") || !strings.Contains(lines[0], "NAMESPACE") || !strings.Contains(lines[0], "STATUS") || strings.Contains(lines[0], "AGE") {
		t.Fatalf("expected NAME/NAMESPACE/STATUS without AGE, got header %q", lines[0])
	}
	if !strings.Contains(lines[2], "-") {
		t.Fatalf("expected dash placeholders for the row missing values, got %q", lines[2])
	}
}

func TestGenericRowStatusReadsFlatReadiness(t *testing.T) {
	if got := genericRowStatus(map[string]any{"name": "p", "ready": true}); got != "Ready" {
		t.Fatalf("flat ready = %q, want Ready", got)
	}
	if got := genericRowStatus(map[string]any{"name": "p", "ready": false}); got != "NotReady" {
		t.Fatalf("flat not ready = %q, want NotReady", got)
	}
}
