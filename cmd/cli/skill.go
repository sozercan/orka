/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"unicode/utf8"

	sigsyaml "sigs.k8s.io/yaml"

	"github.com/spf13/cobra"

	"github.com/orka-agents/orka/internal/cli/client"
)

func newSkillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Manage skills",
	}
	cmd.AddCommand(newSkillListCmd())
	cmd.AddCommand(newSkillGetCmd())
	cmd.AddCommand(newSkillContentCmd())
	cmd.AddCommand(newSkillCreateCmd())
	cmd.AddCommand(newSkillImportCmd())
	cmd.AddCommand(newSkillUpdateCmd())
	cmd.AddCommand(newSkillDeleteCmd())
	cmd.AddCommand(newSkillValidateCmd())
	cmd.AddCommand(newSkillInitCmd())
	return cmd
}

func newSkillListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List skills",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := newClientFromCmd(cmd)
			skills, err := c.ListSkills(context.Background(), client.ListOptions{
				Namespace: c.Namespace,
			})
			if err != nil {
				return err
			}

			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			if format != outputTable {
				return printStructured(cmd, skills)
			}

			if len(skills) == 0 {
				fmt.Println("No skills found.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tDISPLAY NAME\tVERSION\tPHASE\tTAGS") //nolint:errcheck
			for _, s := range skills {
				displayName := s.DisplayName
				if displayName == "" {
					displayName = "-"
				}
				version := s.Version
				if version == "" {
					version = "-"
				}
				phase := s.Phase
				if phase == "" {
					phase = "-"
				}
				tags := "-"
				if len(s.Tags) > 0 {
					tags = strings.Join(s.Tags, ", ")
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", s.Name, displayName, version, phase, tags) //nolint:errcheck
			}
			w.Flush() //nolint:errcheck
			return nil
		},
	}
	addOutputFlag(cmd, outputTable)
	return cmd
}

func newSkillGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get skill details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			skill, err := c.GetSkill(context.Background(), args[0], client.GetOptions{
				Namespace: c.Namespace,
			})
			if err != nil {
				return err
			}

			return printStructured(cmd, skill)
		},
	}
	addOutputFlag(cmd, outputJSON)
	return cmd
}

func newSkillContentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "content <name>",
		Short: "Print raw skill content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			body, _, err := c.GetRaw(context.Background(), "/api/v1/skills/"+url.PathEscape(args[0])+"/content", nil)
			if err != nil {
				return err
			}
			_, _ = cmd.OutOrStdout().Write(body)
			if len(body) == 0 || body[len(body)-1] != '\n' {
				fmt.Fprintln(cmd.OutOrStdout()) //nolint:errcheck
			}
			return nil
		},
	}
}

func newSkillDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a skill",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			if err := c.DeleteSkill(context.Background(), args[0], client.GetOptions{
				Namespace: c.Namespace,
			}); err != nil {
				return err
			}
			fmt.Printf("Skill deleted: %s\n", args[0])
			return nil
		},
	}
}

func newSkillCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create -f <file>",
		Short: "Create a skill from a YAML manifest",
		RunE: func(cmd *cobra.Command, _ []string) error {
			file, _ := cmd.Flags().GetString("file")
			if file == "" {
				return fmt.Errorf("--file (-f) is required")
			}

			c := newClientFromCmd(cmd)
			jsonBody, err := manifestWithNamespaceJSON(cmd, file, c.Namespace)
			if err != nil {
				return err
			}
			skill, err := c.CreateSkill(context.Background(), jsonBody)
			if err != nil {
				return err
			}

			name := ""
			if m, ok := (*skill)["metadata"].(map[string]any); ok {
				name, _ = m["name"].(string)
			}
			fmt.Printf("Skill created: %s\n", name)
			return nil
		},
	}
	cmd.Flags().StringP("file", "f", "", "Path to skill YAML manifest")
	return cmd
}

func newSkillImportCmd() *cobra.Command {
	var name, description string
	cmd := &cobra.Command{
		Use:   "import <path/to/SKILL.md>",
		Short: "Create a skill from a local SKILL.md file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			data, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("reading file: %w", err)
			}

			if name == "" {
				name = deriveSkillImportName(filePath, data)
				if name == "" {
					return fmt.Errorf("could not derive a skill name from %s; pass --name", filePath)
				}
			}
			if description == "" {
				description = deriveSkillImportDescription(data)
			}
			if description == "" {
				description = fmt.Sprintf("Imported from %s", filePath)
			}

			c := newClientFromCmd(cmd)
			body := map[string]any{
				"name":      name,
				"namespace": c.Namespace,
				"spec": map[string]any{
					"description": description,
					"content": map[string]any{
						"inline": string(data),
					},
				},
			}

			bodyJSON, err := json.Marshal(body)
			if err != nil {
				return fmt.Errorf("marshaling request: %w", err)
			}

			skill, err := c.CreateSkill(context.Background(), bodyJSON)
			if err != nil {
				return err
			}

			createdName := name
			if m, ok := (*skill)["metadata"].(map[string]any); ok {
				if n, ok := m["name"].(string); ok {
					createdName = n
				}
			}
			fmt.Printf("Skill imported: %s (from %s)\n", createdName, filePath)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Override skill name (default: the SKILL.md H1 heading, then the parent directory of a SKILL.md, then the filename)")
	cmd.Flags().StringVar(&description, "description", "", "Override skill description (default: the SKILL.md \"## Description\" section)")
	return cmd
}

// deriveSkillImportName picks a Kubernetes-safe skill name for an imported
// SKILL.md: the H1 heading when present, otherwise the parent directory when
// the file itself is the conventional SKILL.md, otherwise the file name.
func deriveSkillImportName(filePath string, data []byte) string {
	frontmatter, body := splitSkillFrontmatter(data)
	if name := sanitizeSkillName(frontmatter["name"]); name != "" {
		return name
	}
	if heading := skillMarkdownHeading(body); heading != "" {
		if name := sanitizeSkillName(heading); name != "" {
			return name
		}
	}
	cleaned := filepath.Clean(filePath)
	base := filepath.Base(cleaned)
	stem := strings.TrimSuffix(strings.TrimSuffix(base, ".md"), ".MD")
	if strings.EqualFold(stem, "skill") {
		if parent := filepath.Base(filepath.Dir(cleaned)); parent != "." && parent != string(filepath.Separator) && parent != "" {
			if name := sanitizeSkillName(parent); name != "" {
				return name
			}
		}
	}
	return sanitizeSkillName(stem)
}

// deriveSkillImportDescription returns the first paragraph under a
// "## Description" heading, or the first non-heading paragraph when the file
// has no description section.
func deriveSkillImportDescription(data []byte) string {
	frontmatter, body := splitSkillFrontmatter(data)
	if description := truncateSkillDescription(frontmatter["description"]); description != "" {
		return description
	}
	lines := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")
	inDescription := false
	firstParagraph := ""
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "#") {
			title := strings.ToLower(strings.TrimSpace(strings.TrimLeft(line, "#")))
			inDescription = title == "description"
			continue
		}
		if line == "" {
			continue
		}
		if inDescription {
			return truncateSkillDescription(line)
		}
		if firstParagraph == "" {
			firstParagraph = line
		}
	}
	return truncateSkillDescription(firstParagraph)
}

func skillMarkdownHeading(data []byte) string {
	for raw := range strings.SplitSeq(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if heading, ok := strings.CutPrefix(line, "# "); ok {
			return strings.TrimSpace(heading)
		}
		return ""
	}
	return ""
}

// sanitizeSkillName lowercases and collapses a free-form label into a
// DNS-1123 label (a-z, 0-9, and interior dashes).
func sanitizeSkillName(value string) string {
	var b strings.Builder
	lastDash := true
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	name := strings.Trim(b.String(), "-")
	if len(name) > 63 {
		name = strings.Trim(name[:63], "-")
	}
	return name
}

func truncateSkillDescription(value string) string {
	const maxLen = 256
	value = strings.TrimSpace(value)
	if len(value) <= maxLen {
		return value
	}
	// Cut on a rune boundary so a multibyte character crossing the limit
	// cannot become invalid UTF-8 in the stored description.
	cut := maxLen
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return strings.TrimSpace(value[:cut])
}

// splitSkillFrontmatter separates a leading YAML frontmatter block
// (---\nkey: value\n---) from the Markdown body. Only simple scalar
// `key: value` lines are read; quoted values are unquoted.
func splitSkillFrontmatter(data []byte) (map[string]string, []byte) {
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	fields := map[string]string{}
	if !strings.HasPrefix(text, "---\n") {
		return fields, data
	}
	rest := text[len("---\n"):]
	before, after, ok := strings.Cut(rest, "\n---")
	if !ok {
		return fields, data
	}
	// Parse the block as YAML so folded (`>-`) and literal (`|`) block
	// scalars, multi-line flow scalars, and quoting are all honoured;
	// only top-level scalar values are kept. A block that is not valid
	// YAML falls back to the simple `key: value` line reader.
	var parsed map[string]any
	if err := sigsyaml.Unmarshal([]byte(before), &parsed); err == nil {
		for key, value := range parsed {
			if scalar, ok := skillFrontmatterScalar(value); ok && strings.TrimSpace(key) != "" && scalar != "" {
				fields[strings.TrimSpace(key)] = scalar
			}
		}
	} else {
		for line := range strings.SplitSeq(before, "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
				value = value[1 : len(value)-1]
			}
			if key != "" && value != "" {
				fields[key] = value
			}
		}
	}
	body := after
	body = strings.TrimPrefix(body, "\n")
	return fields, []byte(body)
}

// skillFrontmatterScalar renders a parsed frontmatter value as trimmed text
// when it is a scalar; lists and maps are not usable as name/description.
func skillFrontmatterScalar(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v), true
	case bool, int, int64, float64:
		return strings.TrimSpace(fmt.Sprint(v)), true
	default:
		return "", false
	}
}

func newSkillUpdateCmd() *cobra.Command {
	return newCRUDUpdateCmd(crudResourceSpec{
		BasePath: "/api/v1/skills",
		Name:     "skill",
	})
}

func newSkillValidateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "validate [-f manifest.yaml] [SKILL.md]",
		Short: "Validate a local skill manifest or SKILL.md file",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if file != "" {
				m, _, err := manifestMap(file)
				if err != nil {
					return err
				}
				if metadataName(m) == "" {
					return fmt.Errorf("skill manifest must include metadata.name or name")
				}
				spec, _ := m["spec"].(map[string]any)
				if spec == nil {
					return fmt.Errorf("skill manifest must include spec")
				}
				if firstString(spec, "description") == "" {
					return fmt.Errorf("skill manifest spec.description is required")
				}
				content, _ := spec["content"].(map[string]any)
				if content == nil || strings.TrimSpace(anyString(content["inline"])) == "" {
					return fmt.Errorf("skill manifest spec.content.inline is required")
				}
				fmt.Fprintln(cmd.OutOrStdout(), "Skill manifest is valid.") //nolint:errcheck
				return nil
			}
			path := "SKILL.md"
			if len(args) == 1 {
				path = args[0]
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("reading skill file: %w", err)
			}
			if strings.TrimSpace(string(data)) == "" {
				return fmt.Errorf("skill file is empty")
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Skill file is valid.") //nolint:errcheck
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "Path to skill YAML/JSON manifest")
	return cmd
}

func newSkillInitCmd() *cobra.Command {
	var name, description string
	var force bool
	cmd := &cobra.Command{
		Use:   "init [dir]",
		Short: "Initialize a local SKILL.md template",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			if name == "" {
				name = "new-skill"
				if dir != "." {
					if derived := sanitizeSkillName(filepath.Base(filepath.Clean(dir))); derived != "" {
						name = derived
					}
				}
			}
			if description == "" {
				description = "Describe when and how to use this skill."
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("creating directory: %w", err)
			}
			path := dir + string(os.PathSeparator) + "SKILL.md"
			content := fmt.Sprintf(
				"# %s\n\n## Description\n\n%s\n\n## Instructions\n\n- Add step-by-step guidance here.\n",
				name,
				description,
			)
			flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
			if force {
				flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
			}
			f, err := os.OpenFile(path, flags, 0o644)
			if err != nil {
				if os.IsExist(err) {
					return fmt.Errorf("%s already exists (use --force to overwrite)", path)
				}
				return fmt.Errorf("opening %s: %w", path, err)
			}
			defer f.Close() //nolint:errcheck
			if _, err := f.WriteString(content); err != nil {
				return fmt.Errorf("writing %s: %w", path, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Skill template created: %s\n", path) //nolint:errcheck
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Skill name for the template")
	cmd.Flags().StringVar(&description, "description", "", "Skill description for the template")
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite an existing SKILL.md")
	return cmd
}
