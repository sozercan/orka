/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"encoding/json"
	"slices"
	"testing"
	"unicode/utf8"
)

func TestRegisterBrokeredWebToolsIsIdempotentAndBounded(t *testing.T) {
	registry := NewRegistry()

	if err := RegisterBrokeredWebTools(registry); err != nil {
		t.Fatalf("RegisterBrokeredWebTools() error = %v", err)
	}
	first := registry.Names()
	if err := RegisterBrokeredWebTools(registry); err != nil {
		t.Fatalf("second RegisterBrokeredWebTools() error = %v", err)
	}
	if got := registry.Names(); !slices.Equal(got, first) {
		t.Fatalf("second registration changed names: first=%v second=%v", first, got)
	}

	want := []string{webFetchToolName, webSearchToolName}
	slices.Sort(want)
	if !slices.Equal(first, want) {
		t.Fatalf("registered broker web tools = %v, want %v", first, want)
	}
	if tool, ok := registry.Get(webSearchToolName); !ok {
		t.Fatalf("broker web registry is missing %q", webSearchToolName)
	} else if search, ok := tool.(*WebSearchTool); !ok {
		t.Fatalf("registered %q implementation = %T, want *WebSearchTool", webSearchToolName, tool)
	} else {
		var schema struct {
			Properties map[string]struct {
				Maximum   int `json:"maximum"`
				MaxLength int `json:"maxLength"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(search.Parameters(), &schema); err != nil {
			t.Fatalf("decode brokered web_search schema: %v", err)
		}
		if got := schema.Properties["query"].MaxLength; got != brokeredWebSearchMaxQueryChars {
			t.Fatalf("brokered web_search query maxLength = %d, want %d", got, brokeredWebSearchMaxQueryChars)
		}
		if got := schema.Properties["limit"].Maximum; got != brokeredWebSearchMaxResults {
			t.Fatalf("brokered web_search limit maximum = %d, want %d", got, brokeredWebSearchMaxResults)
		}
	}
	if tool, ok := registry.Get(webFetchToolName); !ok {
		t.Fatalf("broker web registry is missing %q", webFetchToolName)
	} else if fetch, ok := tool.(*WebFetchTool); !ok {
		t.Fatalf("registered %q implementation = %T, want *WebFetchTool", webFetchToolName, tool)
	} else {
		var schema struct {
			Properties map[string]struct {
				Maximum   int `json:"maximum"`
				MaxLength int `json:"maxLength"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(fetch.Parameters(), &schema); err != nil {
			t.Fatalf("decode brokered web_fetch schema: %v", err)
		}
		if got := schema.Properties["max_chars"].Maximum; got != brokeredWebFetchMaxChars {
			t.Fatalf("brokered web_fetch max_chars maximum = %d, want %d", got, brokeredWebFetchMaxChars)
		}
		if got := schema.Properties["url"].MaxLength; got != brokeredWebFetchMaxURLBytes/utf8.UTFMax {
			t.Fatalf("brokered web_fetch URL maxLength = %d, want %d", got, brokeredWebFetchMaxURLBytes/utf8.UTFMax)
		}
	}
}

func TestRegisterBrokeredWebToolsRequiresRegistry(t *testing.T) {
	if err := RegisterBrokeredWebTools(nil); err == nil {
		t.Fatal("RegisterBrokeredWebTools(nil) expected error")
	}
}
