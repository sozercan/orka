package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orka-agents/orka/internal/acp"
)

// runtimeDockerfileCase describes one built-in ACP runtime image. requiredPins
// reference internal/acp pin constants directly so a pin bump that forgets the
// Dockerfile fails this suite; extraForbidden holds forbidden substrings that
// cannot be enforced uniformly (each exception is explained inline).
type runtimeDockerfileCase struct {
	provider       string
	dockerfile     string
	requiredPins   []string
	extraForbidden []string
}

// universalForbiddenSubstrings is the union of the previously per-image
// forbidden lists that holds for every runtime Dockerfile. Secret-shaped
// terms are concatenated so scanners do not flag the test itself.
var universalForbiddenSubstrings = []string{
	"apt-get ", "apk add", "git clone", "npm install", "RUN npx ", "RUN curl ", "RUN wget ", "ADD http",
	"GITHUB_" + "TOKEN", "GH_" + "TOKEN", "SSH_AUTH_SOCK",
	"OPENAI_API_" + "KEY=", "OPENCODE_SERVER_" + "PASS" + "WORD=", "ARG API_" + "KEY", "ARG TO" + "KEN",
	":latest",
}

// Bare "curl ", "wget ", and "npx " cannot be universal: the node-based images
// legitimately mention those names while deleting the binaries
// (rm -f /usr/local/bin/npx ... /usr/bin/curl /usr/bin/wget). Invocation forms
// (RUN curl/RUN wget/RUN npx) stay universally forbidden above.
// "unknown-linux-musl" cannot be universal: the Codex CLI npm package vendors
// its binaries under *-unknown-linux-musl target triples.
var (
	forbiddenFetchTools = []string{"curl ", "wget "}
	forbiddenMuslTriple = "unknown-linux-musl"
	forbiddenBareNpx    = "npx "
)

func runtimeDockerfileCases() []runtimeDockerfileCase {
	return []runtimeDockerfileCase{
		{
			provider:   "codex",
			dockerfile: "codex/Dockerfile",
			requiredPins: []string{
				acp.CodexACPVersion,
				acp.CodexACPSourceCommit,
				acp.CodexACPTarSHA256,
				acp.CodexACPOrkaPatchSHA256,
				acp.CodexACPOrkaDistSHA256,
				acp.CodexCLIVersion,
				acp.CodexCLISourceCommit,
				acp.CodexCLILinuxX64SHA256,
				acp.CodexCLILinuxARM64SHA256,
			},
			// Codex vendors musl-triple CLI binaries, so the musl term is
			// excluded; its node base image removal lines mention npx.
			extraForbidden: forbiddenFetchTools,
		},
		{
			provider:   "claude",
			dockerfile: "claude/Dockerfile",
			requiredPins: []string{
				acp.ClaudeACPVersion,
				acp.ClaudeACPSourceCommit,
				acp.ClaudeACPTarSHA256,
				acp.ClaudeAgentSDKVersion,
				acp.ClaudeCodeVersion,
				acp.ClaudeSDKLinuxX64SHA256,
				acp.ClaudeSDKLinuxARM64SHA256,
			},
			extraForbidden: append([]string{forbiddenMuslTriple}, forbiddenFetchTools...),
		},
		{
			provider:   "copilot",
			dockerfile: "copilot/Dockerfile",
			requiredPins: []string{
				acp.CopilotCLIVersion,
				acp.CopilotCLISourceCommit,
				acp.CopilotCLILinuxX64SHA256,
				acp.CopilotCLILinuxARM64SHA256,
			},
			// Copilot's hardening line removes /usr/bin/curl and /usr/bin/wget,
			// so the bare fetch-tool terms are excluded for this image only.
			extraForbidden: []string{forbiddenMuslTriple},
		},
		{
			provider:   "opencode",
			dockerfile: "opencode/Dockerfile",
			requiredPins: []string{
				acp.OpenCodeVersion,
				acp.OpenCodeSourceCommit,
				acp.OpenCodeSourceTarSHA256,
				acp.OpenCodeLinuxX64BaselineTarSHA256,
				acp.OpenCodeLinuxARM64TarSHA256,
				acp.OpenCodeLinuxX64BinarySHA256,
				acp.OpenCodeLinuxARM64BinarySHA256,
				acp.OpenCodeRipgrepVersion,
				acp.OpenCodeRipgrepDebianVersion,
				acp.OpenCodeRipgrepLinuxX64DebSHA256,
				acp.OpenCodeRipgrepLinuxARM64DebSHA256,
				acp.OpenCodeRipgrepLinuxX64BinarySHA256,
				acp.OpenCodeRipgrepLinuxARM64BinarySHA256,
				acp.OpenCodeRootInstructionSHA256,
				acp.OpenCodeImageNoticeSHA256,
			},
			// The debian-based OpenCode image ships no node tooling at all, so
			// even the bare npx term holds there.
			extraForbidden: append([]string{forbiddenMuslTriple, forbiddenBareNpx}, forbiddenFetchTools...),
		},
	}
}

func TestRuntimeDockerfilesArePinnedAndHardened(t *testing.T) {
	t.Parallel()
	for _, tc := range runtimeDockerfileCases() {
		t.Run(tc.provider, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(tc.dockerfile)
			if err != nil {
				t.Fatal(err)
			}
			contents := string(data)

			for line := range strings.SplitSeq(contents, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "FROM ") && !strings.Contains(trimmed, "@sha256:") {
					t.Errorf("un-pinned base image: %s", line)
				}
			}

			forbidden := append(append([]string(nil), universalForbiddenSubstrings...), tc.extraForbidden...)
			for _, value := range forbidden {
				if strings.Contains(contents, value) {
					t.Errorf("Dockerfile contains forbidden mutable or secret-bearing surface %q", value)
				}
			}

			for _, pin := range tc.requiredPins {
				if pin == "" {
					t.Fatal("empty required pin constant")
				}
				if !strings.Contains(contents, pin) {
					t.Errorf("Dockerfile is missing pinned artifact reference %q from internal/acp/pins.go", pin)
				}
			}

			for _, required := range []string{
				"ORKA_ACP_PROVIDER=" + tc.provider,
				"ENTRYPOINT [\"/usr/local/bin/orka-acp-runtime\"]",
			} {
				if !strings.Contains(contents, required) {
					t.Errorf("Dockerfile is missing %q", required)
				}
			}
		})
	}
}

func TestAgentKitDockerfileRequiresFrozenRuntimeImage(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("agentkit", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, required := range []string{
		"ARG AGENTKIT_RUNTIME_IMAGE",
		"ARG AGENTKIT_ADAPTER_DIGEST",
		"FROM --platform=$TARGETPLATFORM ${AGENTKIT_RUNTIME_IMAGE}",
		"case \"$AGENTKIT_RUNTIME_IMAGE\" in *@sha256:*",
		"case \"$AGENTKIT_ADAPTER_DIGEST\" in sha256:*",
		"test \"$AGENTKIT_ADAPTER_DIGEST\" = \"sha256:$digest\"",
		"test -x /opt/agentkit/bin/agentkit-serve",
		"test -s /agent/agent.yaml",
		"chown -R 0:0 /opt/agentkit",
		"chmod -R a+rX,go-w /opt/agentkit",
		"chown 0:0 /agent /agent/agent.yaml",
		"chmod 0555 /agent",
		"chmod 0444 /agent/agent.yaml",
		"ORKA_ACP_PROVIDER=agentkit",
		"ORKA_ACP_AGENTKIT_ADAPTER_DIGEST=${AGENTKIT_ADAPTER_DIGEST}",
		"io.orka.acp.adapter.name=\"agentkit-serve-acp\"",
		"CMD []",
		"ENTRYPOINT [\"/usr/local/bin/orka-acp-runtime\"]",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("AgentKit Dockerfile is missing %q", required)
		}
	}
	for _, forbidden := range universalForbiddenSubstrings {
		if strings.Contains(contents, forbidden) {
			t.Errorf("AgentKit Dockerfile contains forbidden mutable or secret-bearing surface %q", forbidden)
		}
	}
	lastFrom := strings.LastIndex(contents, "FROM ")
	finalBase := "FROM --platform=$TARGETPLATFORM ${AGENTKIT_RUNTIME_IMAGE}\n"
	if lastFrom < 0 || !strings.HasPrefix(contents[lastFrom:], finalBase) {
		t.Fatal("AgentKit runtime image is not the final base stage")
	}
}

func TestCopilotPinIsNewerThanCredentiallessBYOKACPFixBoundary(t *testing.T) {
	t.Parallel()
	got := parseVersionCore(t, acp.CopilotCLIVersion)
	fixBoundary := [3]int{1, 0, 76}
	if compareVersionCore(got, fixBoundary) <= 0 {
		t.Fatalf(
			"Copilot CLI version %q must be newer than credentialless BYOK ACP fix boundary 1.0.76-0",
			acp.CopilotCLIVersion,
		)
	}
}

func parseVersionCore(t *testing.T, version string) [3]int {
	t.Helper()
	var parsed [3]int
	core, _, _ := strings.Cut(version, "-")
	if count, err := fmt.Sscanf(core, "%d.%d.%d", &parsed[0], &parsed[1], &parsed[2]); err != nil || count != len(parsed) {
		t.Fatalf("parse Copilot CLI version %q: parsed %d components: %v", version, count, err)
	}
	return parsed
}

func compareVersionCore(left, right [3]int) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func TestDockerContextIncludesCopilotLicenseInputs(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, pattern := range []string{"!LICENSE", "!NOTICE.md"} {
		if !strings.Contains(contents, pattern) {
			t.Errorf(".dockerignore is missing %q", pattern)
		}
	}
}

func TestOpenCodeRootInstructionIsPinnedAndFailClosed(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("opencode", "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != acp.OpenCodeRootInstructionSHA256 {
		t.Fatalf("AGENTS.md SHA-256 = %s, want %s", got, acp.OpenCodeRootInstructionSHA256)
	}
	content := string(data)
	for _, required := range []string{
		"workspace, provider proxy, MCP broker, and permission policy are authoritative",
		"Do not inspect or modify other session trees",
		"Never bypass an OpenCode allow/deny decision",
		"Do not change OpenCode configuration",
		"Never print, copy, persist, or expose credentials",
	} {
		if !strings.Contains(content, required) {
			t.Errorf("AGENTS.md is missing guardrail %q", required)
		}
	}
}

func TestOpenCodeImageNoticeIsPinnedAndIncludesOpenCodeLicense(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("opencode", "NOTICE.md"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != acp.OpenCodeImageNoticeSHA256 {
		t.Fatalf("NOTICE.md SHA-256 = %s, want %s", got, acp.OpenCodeImageNoticeSHA256)
	}
	root, err := os.ReadFile(filepath.Join("..", "..", "..", "NOTICE.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(root) != string(data) {
		t.Fatal("image NOTICE.md does not match the repository third-party notice")
	}
	content := string(data)
	for _, required := range []string{
		"## OpenCode",
		"OpenCode " + acp.OpenCodeVersion + " native binary",
		"Copyright (c) 2025 opencode",
	} {
		if !strings.Contains(content, required) {
			t.Errorf("NOTICE.md is missing %q", required)
		}
	}
}
