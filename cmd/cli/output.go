package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"
	sigsyaml "sigs.k8s.io/yaml"
)

const (
	outputTable  = "table"
	outputJSON   = "json"
	outputYAML   = "yaml"
	cliQueryTrue = "true"
)

func addOutputFlag(cmd *cobra.Command, defaultValue string) {
	cmd.Flags().StringP("output", "o", defaultValue, "Output format: table, json, yaml")
}

func outputFormat(cmd *cobra.Command) (string, error) {
	format, _ := cmd.Flags().GetString("output")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = outputTable
	}
	switch format {
	case outputTable, outputJSON, outputYAML:
		return format, nil
	default:
		return "", fmt.Errorf("unsupported output format %q (must be table, json, or yaml)", format)
	}
}

func printStructured(cmd *cobra.Command, value any) error {
	format, err := outputFormat(cmd)
	if err != nil {
		return err
	}
	switch format {
	case outputJSON:
		out, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return fmt.Errorf("formatting json output: %w", err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(out)) //nolint:errcheck
	case outputYAML:
		out, err := sigsyaml.Marshal(value)
		if err != nil {
			return fmt.Errorf("formatting yaml output: %w", err)
		}
		fmt.Fprint(cmd.OutOrStdout(), string(out)) //nolint:errcheck
	default:
		return printGenericTable(cmd, value)
	}
	return nil
}

func printGenericTable(cmd *cobra.Command, value any) error {
	// Typed client responses (a single resource struct) are rendered through
	// their JSON shape so one object prints as a one-row table instead of
	// "No resources found."
	if _, isMap := value.(map[string]any); !isMap {
		if _, isSlice := value.([]any); !isSlice {
			if encoded, err := json.Marshal(value); err == nil {
				var generic any
				if json.Unmarshal(encoded, &generic) == nil {
					value = generic
				}
			}
		}
	}
	items := listItems(value)
	if len(items) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No resources found.") //nolint:errcheck
		return nil
	}
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{
			genericRowName(item),
			genericRowNamespace(item),
			genericRowStatus(item),
			genericRowTimestamp(item),
		})
	}
	headers := []string{"NAME", "NAMESPACE", "STATUS", "AGE"}
	columns := genericTableColumns(headers, rows)
	for _, row := range rows {
		row[3] = formatAge(row[3])
	}
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(pickColumns(headers, columns), "\t")) //nolint:errcheck
	for _, row := range rows {
		cells := pickColumns(row, columns)
		for i := range cells {
			cells[i] = dash(cells[i])
		}
		fmt.Fprintln(w, strings.Join(cells, "\t")) //nolint:errcheck
	}
	return w.Flush()
}

// genericTableColumns keeps the NAME column and every other column that at
// least one row fills in, so resources without a namespace, status, or
// timestamp (built-in tools, model IDs, secret metadata) do not print
// dash-only columns.
func genericTableColumns(headers []string, rows [][]string) []int {
	columns := []int{0}
	for col := 1; col < len(headers); col++ {
		for _, row := range rows {
			if col < len(row) && strings.TrimSpace(row[col]) != "" {
				columns = append(columns, col)
				break
			}
		}
	}
	return columns
}

func pickColumns(row []string, columns []int) []string {
	picked := make([]string, 0, len(columns))
	for _, col := range columns {
		if col < len(row) {
			picked = append(picked, row[col])
		} else {
			picked = append(picked, "")
		}
	}
	return picked
}

const genericRowTitleLimit = 60

// sanitizeTerminalText strips control and format runes from forge-supplied
// text before it reaches the terminal: an untrusted issue or PR title must
// not carry ANSI/OSC escapes into an operator's display.
func sanitizeTerminalText(value string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, value)
}

// genericRowName labels a row by its resource name, falling back to the
// "#number title" form used by forge-backed records (monitor items,
// commands, work actions) that carry no name of their own.
func genericRowName(item map[string]any) string {
	name := firstString(item, "name", "id")
	if name == "" {
		name = nestedString(item, "metadata", "name")
	}
	if name == "" {
		name = firstString(item, "monitorName")
	}
	if number := firstString(item, "number"); number != "" {
		label := "#" + number
		if title := strings.TrimSpace(sanitizeTerminalText(firstString(item, "title"))); title != "" {
			if len(title) > genericRowTitleLimit {
				// Cut on a rune boundary so a multibyte character crossing
				// the limit cannot become invalid UTF-8 in the table.
				cut := genericRowTitleLimit
				for cut > 0 && !utf8.RuneStart(title[cut]) {
					cut--
				}
				title = strings.TrimSpace(title[:cut]) + "…"
			}
			label += " " + title
		}
		if name == "" {
			return label
		}
		return name + " " + label
	}
	return name
}

func genericRowNamespace(item map[string]any) string {
	namespace := firstString(item, "namespace", "monitorNamespace")
	if namespace == "" {
		namespace = nestedString(item, "metadata", "namespace")
	}
	return namespace
}

// genericRowStatus prefers the resource phase and appends the workflow
// phase when a record tracks both (for example an "open" item that is
// "approval_required").
func genericRowStatus(item map[string]any) string {
	status := firstString(item, "phase", "status", "state")
	if status == "" {
		status = nestedString(item, "status", "phase")
	}
	if status == "" {
		// The restricted flat projection served to context-token callers
		// carries readiness at the top level.
		if ready, ok := item["ready"].(bool); ok {
			if ready {
				status = "Ready"
			} else {
				status = "NotReady"
			}
		}
	}
	if status == "" {
		// Resources such as Providers expose readiness as status.ready
		// instead of a phase.
		if statusMap, ok := item["status"].(map[string]any); ok {
			if ready, ok := statusMap["ready"].(bool); ok {
				if ready {
					status = "Ready"
				} else {
					status = "NotReady"
				}
			}
		}
	}
	if workflow := firstString(item, "workflowPhase"); workflow != "" && workflow != status {
		if status == "" {
			return workflow
		}
		return status + "/" + workflow
	}
	return status
}

func genericRowTimestamp(item map[string]any) string {
	age := firstString(item, "createdAt")
	if age == "" {
		age = nestedString(item, "metadata", "creationTimestamp")
	}
	if age == "" {
		age = firstString(item, "updatedAt", "lastSeenAt")
	}
	return age
}

func readFileOrStdin(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path is required")
	}
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func manifestJSON(path string) ([]byte, error) {
	data, err := readFileOrStdin(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}
	if json.Valid(trimmed) {
		return trimmed, nil
	}
	jsonBody, err := sigsyaml.YAMLToJSON(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	return jsonBody, nil
}

func manifestMap(path string) (map[string]any, []byte, error) {
	body, err := manifestJSON(path)
	if err != nil {
		return nil, nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, nil, fmt.Errorf("parsing manifest: %w", err)
	}
	return m, body, nil
}

func manifestWithNamespaceJSON(cmd *cobra.Command, path, namespace string) ([]byte, error) {
	m, _, err := manifestMap(path)
	if err != nil {
		return nil, err
	}
	metadata, _ := m["metadata"].(map[string]any)
	metadataNS := ""
	if metadata != nil {
		metadataNS = strings.TrimSpace(anyString(metadata["namespace"]))
	}
	topLevelNS := strings.TrimSpace(anyString(m["namespace"]))
	if metadataNS != "" && topLevelNS != "" && metadataNS != topLevelNS {
		return nil, fmt.Errorf("manifest metadata.namespace %q does not match top-level namespace %q", metadataNS, topLevelNS)
	}
	manifestNS := strings.TrimSpace(manifestNamespace(m))
	flagNS, _ := cmd.Flags().GetString("namespace")
	if strings.TrimSpace(flagNS) != "" && manifestNS != "" && manifestNS != flagNS {
		return nil, fmt.Errorf("manifest namespace %q does not match --namespace %q", manifestNS, flagNS)
	}
	ensureManifestNamespace(m, namespace)
	body, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshaling manifest: %w", err)
	}
	return body, nil
}

func ensureManifestNamespace(m map[string]any, namespace string) {
	if strings.TrimSpace(namespace) == "" || m == nil {
		return
	}
	topLevelNS := strings.TrimSpace(anyString(m["namespace"]))
	metadata, _ := m["metadata"].(map[string]any)
	if metadata != nil {
		if strings.TrimSpace(anyString(metadata["namespace"])) == "" {
			if topLevelNS != "" {
				metadata["namespace"] = topLevelNS
			} else {
				metadata["namespace"] = namespace
			}
		}
		return
	}
	if topLevelNS == "" {
		m["namespace"] = namespace
	}
}

func manifestNamespace(m map[string]any) string {
	metadata, _ := m["metadata"].(map[string]any)
	if metadata != nil {
		if ns := strings.TrimSpace(anyString(metadata["namespace"])); ns != "" {
			return ns
		}
	}
	return anyString(m["namespace"])
}

func namespaceQueryForManifest(
	cmd *cobra.Command,
	clientNamespace string,
	manifest map[string]any,
) (map[string]string, error) {
	manifestNS := strings.TrimSpace(manifestNamespace(manifest))
	if manifestNS == "" {
		return nil, nil
	}
	flagNS, _ := cmd.Flags().GetString("namespace")
	if strings.TrimSpace(flagNS) != "" && flagNS != manifestNS {
		return nil, fmt.Errorf("manifest namespace %q does not match --namespace %q", manifestNS, flagNS)
	}
	if strings.TrimSpace(clientNamespace) != "" && strings.TrimSpace(flagNS) != "" && clientNamespace != manifestNS {
		return nil, fmt.Errorf("manifest namespace %q does not match selected namespace %q", manifestNS, clientNamespace)
	}
	return map[string]string{"namespace": manifestNS}, nil
}

func listItems(value any) []map[string]any {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]any); ok {
		if raw, ok := m["items"]; ok {
			return anySliceToMaps(raw)
		}
		if raw, ok := m["data"]; ok {
			return anySliceToMaps(raw)
		}
		return []map[string]any{m}
	}
	return anySliceToMaps(value)
}

func anySliceToMaps(raw any) []map[string]any {
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	items := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			items = append(items, m)
		}
	}
	return items
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if s := anyString(m[key]); s != "" {
			return s
		}
	}
	return ""
}

func nestedString(m map[string]any, keys ...string) string {
	cur := m
	for i, key := range keys {
		if i == len(keys)-1 {
			return anyString(cur[key])
		}
		next, ok := cur[key].(map[string]any)
		if !ok {
			return ""
		}
		cur = next
	}
	return ""
}

func anyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	case bool:
		return fmt.Sprintf("%t", x)
	default:
		return ""
	}
}

func dash(s string) string {
	if strings.TrimSpace(s) == "" || s == "<unknown>" {
		return "-"
	}
	return s
}

func metadataName(value any) string {
	m, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if s := firstString(m, "name", "id"); s != "" {
		return s
	}
	return nestedString(m, "metadata", "name")
}

func mergeQuery(base map[string]string, pairs ...string) map[string]string {
	q := map[string]string{}
	for k, v := range base {
		if v != "" {
			q[k] = v
		}
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			q[pairs[i]] = pairs[i+1]
		}
	}
	return q
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
