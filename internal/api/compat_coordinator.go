/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"github.com/gofiber/fiber/v3"

	"github.com/orka-agents/orka/internal/llm"
)

type compatCoordinatorSetup struct {
	Namespace           string
	ToolUseAction       string
	AuthorizationConfig ContextTokenAuthorizationConfig
}

// prepareCompatCoordinatorTools mutates req for Orka coordinator mode and
// reports whether coordinator tools are enabled for this request. Protocol
// adapters keep their own error mapping; this module owns the shared mutation
// order so OpenAI and Anthropic do not drift.
func prepareCompatCoordinatorTools(
	c fiber.Ctx,
	req *llm.CompletionRequest,
	setup compatCoordinatorSetup,
) (bool, error) {
	if c.Get("X-Orka-Tools") == "disabled" {
		return false, nil
	}
	req.Tools = nil
	injectOrkaTools(req)
	req.Tools = filterCompletionToolsForContextToken(c, setup.AuthorizationConfig, req.Tools)
	if err := authorizeContextTokenToolUse(c, setup.AuthorizationConfig, setup.ToolUseAction, completionToolNames(req.Tools)); err != nil {
		return false, err
	}
	req.SystemPrompt = coordinatorSystemPrompt(setup.Namespace) + "\n\n" + req.SystemPrompt
	req.Messages = stripClientToolMessages(req.Messages)
	return true, nil
}
