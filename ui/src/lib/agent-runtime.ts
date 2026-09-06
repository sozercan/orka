import type { BuiltInAgentRuntimeType } from "@/schemas/agent";

export type { BuiltInAgentRuntimeType } from "@/schemas/agent";

const BUILT_IN_AGENT_RUNTIME_LABELS: Record<
  BuiltInAgentRuntimeType,
  { option: string; compact: string }
> = {
  claude: { option: "Claude ACP", compact: "claude ACP" },
  codex: { option: "OpenAI Codex ACP", compact: "codex ACP" },
  copilot: { option: "GitHub Copilot ACP", compact: "copilot ACP" },
  opencode: { option: "OpenCode ACP", compact: "OpenCode ACP" },
};

export const OPENCODE_REVIEWED_RUNTIME_DEFAULTS = {
  defaultAllowedTools: ["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
  defaultAllowBash: true,
} as const;

export function builtInAgentRuntimeLabel(type: BuiltInAgentRuntimeType) {
  return BUILT_IN_AGENT_RUNTIME_LABELS[type].compact;
}

export function isOpenCodeModelID(value: string) {
  const normalized = value.trim();
  const slash = normalized.indexOf("/");
  return (
    slash > 0 &&
    slash < normalized.length - 1 &&
    !normalized.includes("{") &&
    !normalized.includes("}")
  );
}
