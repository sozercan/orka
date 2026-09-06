package main

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDeriveSkillImportName(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		data     string
		want     string
	}{
		{"h1 heading wins", "docs/SKILL.md", "# Release Notes Helper\n\n## Description\n\nx\n", "release-notes-helper"},
		{"parent dir for conventional SKILL.md", "skills/pr-triage/SKILL.md", "## Description\n\nno heading\n", "pr-triage"},
		{"lowercase skill.md also uses parent", "skills/Pr_Triage/skill.md", "text\n", "pr-triage"},
		{"bare SKILL.md in cwd falls back to filename", "SKILL.md", "text\n", "skill"},
		{"other filename", "notes/My Skill.md", "text\n", "my-skill"},
		{"heading with symbols", "SKILL.md", "#   Deploy: v2 (beta)!\n", "deploy-v2-beta"},
		{"empty heading falls through", "skills/alpha/SKILL.md", "# \n", "alpha"},
		{"frontmatter name wins", "skills/beta/SKILL.md", "---\nname: Ship It\ndescription: d\n---\n# Other\n", "ship-it"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deriveSkillImportName(tt.filePath, []byte(tt.data)); got != tt.want {
				t.Fatalf("deriveSkillImportName(%q) = %q, want %q", tt.filePath, got, tt.want)
			}
		})
	}
}

func TestDeriveSkillImportDescription(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{"description section", "# n\n\n## Description\n\nUse this for triage.\nMore.\n\n## Instructions\n\n- step\n", "Use this for triage."},
		{"case-insensitive heading", "# n\n\n## DESCRIPTION\n\n  Trimmed text  \n", "Trimmed text"},
		{"no description section uses first paragraph", "# n\n\nFirst paragraph here.\n\n## Instructions\n\n- step\n", "First paragraph here."},
		{"only headings", "# n\n\n## Description\n\n## Instructions\n", ""},
		{"crlf", "# n\r\n\r\n## Description\r\n\r\nWindows line.\r\n", "Windows line."},
		{"frontmatter description", "---\nname: x\ndescription: \"Frontmatter wins.\"\n---\n# n\n\nBody prose.\n", "Frontmatter wins."},
		{"frontmatter without description skips the block", "---\nname: x\n---\n# n\n\nBody prose.\n", "Body prose."},
		{"frontmatter folded block scalar", "---\nname: x\ndescription: >-\n  Folded line one\n  and line two.\n---\n# n\n\nBody prose.\n", "Folded line one and line two."},
		{"frontmatter literal block scalar", "---\nname: x\ndescription: |\n  Literal line one\n  line two\n---\n# n\n\nBody prose.\n", "Literal line one\nline two"},
		{"frontmatter list description is ignored", "---\nname: x\ndescription:\n  - not\n  - scalar\n---\n# n\n\nBody prose.\n", "Body prose."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deriveSkillImportDescription([]byte(tt.data)); got != tt.want {
				t.Fatalf("deriveSkillImportDescription() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSanitizeSkillNameBounds(t *testing.T) {
	long := ""
	for range 80 {
		long += "a"
	}
	if got := sanitizeSkillName(long); len(got) != 63 {
		t.Fatalf("expected 63-char name, got %d", len(got))
	}
	if got := sanitizeSkillName("---"); got != "" {
		t.Fatalf("expected empty name for dashes only, got %q", got)
	}
}

func TestTruncateSkillDescriptionKeepsValidUTF8(t *testing.T) {
	value := strings.Repeat("a", 255) + "é" + strings.Repeat("b", 10)
	got := truncateSkillDescription(value)
	if !utf8.ValidString(got) {
		t.Fatalf("truncated description is not valid UTF-8: %q", got)
	}
	if len(got) > 256 || strings.ContainsRune(got, 'b') {
		t.Fatalf("truncation kept too much: len %d", len(got))
	}
}
