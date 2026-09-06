/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package genai

import "testing"

func TestProviderNamesIncludesSpecEnum(t *testing.T) {
	if len(ProviderNames) != 16 {
		t.Fatalf("len(ProviderNames) = %d, want 16", len(ProviderNames))
	}
	seen := map[string]bool{}
	for _, provider := range ProviderNames {
		if provider == "" {
			t.Fatal("empty provider enum value")
		}
		if seen[provider] {
			t.Fatalf("duplicate provider enum %q", provider)
		}
		seen[provider] = true
	}
	for _, provider := range []string{ProviderAnthropic, ProviderOpenAI, ProviderAzureOpenAI, ProviderAWSBedrock, ProviderMoonshotAI} {
		if !seen[provider] {
			t.Fatalf("missing provider enum %q", provider)
		}
	}
}

func TestNormalizeProviderName(t *testing.T) {
	tests := map[string]string{
		"openai":          ProviderOpenAI,
		"azure-openai":    ProviderAzureOpenAI,
		"azure.ai.openai": ProviderAzureOpenAI,
		"anthropic":       ProviderAnthropic,
		"bedrock":         ProviderAWSBedrock,
		"unknown-gateway": "unknown-gateway",
	}
	for input, want := range tests {
		if got := NormalizeProviderName(input); got != want {
			t.Fatalf("NormalizeProviderName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestHistogramBuckets(t *testing.T) {
	if len(OperationDurationBuckets) != 14 {
		t.Fatalf("operation duration bucket count = %d, want 14", len(OperationDurationBuckets))
	}
	if len(TokenUsageBuckets) == 0 || TokenUsageBuckets[0] != 1 {
		t.Fatalf("unexpected token usage buckets: %#v", TokenUsageBuckets)
	}
}
