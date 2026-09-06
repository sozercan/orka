import { describe, expect, it } from "vitest";
import {
  OPENCODE_REVIEWED_RUNTIME_DEFAULTS,
  builtInAgentRuntimeLabel,
  isOpenCodeModelID,
} from "./agent-runtime";

describe("built-in agent runtime display", () => {
  it("preserves existing compact labels and brands OpenCode", () => {
    expect(builtInAgentRuntimeLabel("claude")).toBe("claude ACP");
    expect(builtInAgentRuntimeLabel("codex")).toBe("codex ACP");
    expect(builtInAgentRuntimeLabel("copilot")).toBe("copilot ACP");
    expect(builtInAgentRuntimeLabel("opencode")).toBe("OpenCode ACP");
  });

  it("defines the reviewed OpenCode native-tool policy", () => {
    expect(OPENCODE_REVIEWED_RUNTIME_DEFAULTS).toEqual({
      defaultAllowedTools: ["Read", "Write", "Edit", "Bash", "Glob", "Grep"],
      defaultAllowBash: true,
    });
  });

  it("validates literal OpenCode provider/model IDs", () => {
    expect(isOpenCodeModelID("openai/gpt-5.4")).toBe(true);
    expect(isOpenCodeModelID("gpt-5.4")).toBe(false);
    expect(isOpenCodeModelID("{env:TOKEN}/gpt")).toBe(false);
    expect(isOpenCodeModelID("openai/")).toBe(false);
  });
});
