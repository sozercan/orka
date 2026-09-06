/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"

	"github.com/orka-agents/orka/internal/store"
)

func (h *Handlers) ensureMemoryStore() error {
	if h.memoryStore == nil {
		return fiber.NewError(fiber.StatusNotImplemented, "memory store not configured")
	}
	return nil
}

func (h *Handlers) ensureMemoryProposalStore() error {
	if h.memoryProposalStore == nil {
		return fiber.NewError(fiber.StatusNotImplemented, "memory proposal store not configured")
	}
	return nil
}

// ListMemories lists namespace-scoped memories.
func (h *Handlers) ListMemories(c fiber.Ctx) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listMemories", h.contextTokenAuthorization.MemoryReadScopes); err != nil {
		return err
	}
	filter, err := parseMemoryFilter(c, namespace)
	if err != nil {
		return err
	}

	memories, err := h.memoryStore.ListMemories(c.Context(), filter)
	if err != nil {
		return memoryStoreError("list memories", "memory", err)
	}
	return c.JSON(ListResponse{Items: memories, Metadata: ListMeta{}})
}

// CreateMemory creates a namespace-scoped memory.
func (h *Handlers) CreateMemory(c fiber.Ctx) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	var memory store.Memory
	if err := c.Bind().JSON(&memory); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	namespace, err := h.resolveNamespace(c, memory.Namespace)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "createMemory", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	memory.Namespace = namespace
	if strings.TrimSpace(memory.Content) == "" {
		return fiber.NewError(fiber.StatusBadRequest, "content is required")
	}

	if err := h.memoryStore.CreateMemory(c.Context(), &memory); err != nil {
		return memoryStoreError("create memory", "memory", err)
	}
	return c.Status(fiber.StatusCreated).JSON(memory)
}

// GetMemory gets a memory by ID.
func (h *Handlers) GetMemory(c fiber.Ctx) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getMemory", h.contextTokenAuthorization.MemoryReadScopes); err != nil {
		return err
	}
	memory, err := h.memoryStore.GetMemory(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return memoryStoreError("get memory", "memory", err)
	}
	return c.JSON(memory)
}

// UpdateMemory updates mutable fields on a memory.
func (h *Handlers) UpdateMemory(c fiber.Ctx) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	var req store.Memory
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	explicitNamespace := c.Query("namespace", "")
	if explicitNamespace == "" {
		explicitNamespace = req.Namespace
	}
	namespace, err := h.resolveNamespace(c, explicitNamespace)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "updateMemory", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}

	memory, err := h.memoryStore.GetMemory(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return memoryStoreError("get memory", "memory", err)
	}
	applyMemoryUpdate(memory, req)
	memory.Namespace = namespace
	memory.ID = c.Params("id")
	if strings.TrimSpace(memory.Content) == "" {
		return fiber.NewError(fiber.StatusBadRequest, "content is required")
	}

	if err := h.memoryStore.UpdateMemory(c.Context(), memory); err != nil {
		return memoryStoreError("update memory", "memory", err)
	}
	updated, err := h.memoryStore.GetMemory(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return memoryStoreError("get memory", "memory", err)
	}
	return c.JSON(updated)
}

// DeleteMemory soft-deletes a memory.
func (h *Handlers) DeleteMemory(c fiber.Ctx) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "deleteMemory", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	if err := h.memoryStore.DeleteMemory(c.Context(), namespace, c.Params("id")); err != nil {
		return memoryStoreError("delete memory", "memory", err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// DisableMemory disables a memory for recall without deleting it.
func (h *Handlers) DisableMemory(c fiber.Ctx) error {
	return h.setMemoryDisabled(c, true)
}

// EnableMemory enables a previously disabled memory.
func (h *Handlers) EnableMemory(c fiber.Ctx) error {
	return h.setMemoryDisabled(c, false)
}

func (h *Handlers) setMemoryDisabled(c fiber.Ctx, disabled bool) error {
	if err := h.ensureMemoryStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "setMemoryDisabled", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	if err := h.memoryStore.SetMemoryDisabled(c.Context(), namespace, c.Params("id"), disabled); err != nil {
		return memoryStoreError("update memory", "memory", err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// ListMemoryProposals lists memory governance proposals.
func (h *Handlers) ListMemoryProposals(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listMemoryProposals", h.contextTokenAuthorization.MemoryReadScopes); err != nil {
		return err
	}
	filter, err := parseMemoryProposalFilter(c, namespace)
	if err != nil {
		return err
	}
	proposals, err := h.memoryProposalStore.ListMemoryProposals(c.Context(), filter)
	if err != nil {
		return memoryStoreError("list memory proposals", "memory proposal", err)
	}
	return c.JSON(ListResponse{Items: proposals, Metadata: ListMeta{}})
}

// CreateMemoryProposal creates a governance proposal. It does not apply proposals automatically.
func (h *Handlers) CreateMemoryProposal(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	var proposal store.MemoryProposal
	if err := c.Bind().JSON(&proposal); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	namespace, err := h.resolveNamespace(c, proposal.Namespace)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "createMemoryProposal", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	proposal.Namespace = namespace
	if strings.TrimSpace(proposal.Title) == "" {
		return fiber.NewError(fiber.StatusBadRequest, "title is required")
	}

	if err := h.memoryProposalStore.CreateMemoryProposal(c.Context(), &proposal); err != nil {
		return memoryStoreError("create memory proposal", "memory proposal", err)
	}
	return c.Status(fiber.StatusCreated).JSON(proposal)
}

// GetMemoryProposal gets a proposal by ID.
func (h *Handlers) GetMemoryProposal(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getMemoryProposal", h.contextTokenAuthorization.MemoryReadScopes); err != nil {
		return err
	}
	proposal, err := h.memoryProposalStore.GetMemoryProposal(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return memoryStoreError("get memory proposal", "memory proposal", err)
	}
	return c.JSON(proposal)
}

// ReviewMemoryProposal records a review decision without applying the proposal automatically.
func (h *Handlers) ReviewMemoryProposal(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	review, err := bindMemoryProposalReview(c, c.Query("namespace", ""), c.Params("id"))
	if err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, review.Namespace)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "reviewMemoryProposal", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	review.Namespace = namespace
	if review.Reviewer == "" {
		if ui := GetUserInfo(c); ui != nil {
			review.Reviewer = ui.Username
		}
	}
	if err := h.memoryProposalStore.ReviewMemoryProposal(c.Context(), review); err != nil {
		return memoryStoreError("review memory proposal", "memory proposal", err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// ArchiveMemoryProposal archives a proposal without applying it.
func (h *Handlers) ArchiveMemoryProposal(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "archiveMemoryProposal", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	if err := h.memoryProposalStore.ArchiveMemoryProposal(c.Context(), namespace, c.Params("id")); err != nil {
		return memoryStoreError("archive memory proposal", "memory proposal", err)
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// ApplyMemoryProposal applies an accepted memory proposal into durable memory.
func (h *Handlers) ApplyMemoryProposal(c fiber.Ctx) error {
	if err := h.ensureMemoryProposalStore(); err != nil {
		return err
	}
	apply, err := bindMemoryProposalApply(c, c.Query("namespace", ""), c.Params("id"))
	if err != nil {
		return err
	}
	namespace, err := h.resolveNamespace(c, apply.Namespace)
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "applyMemoryProposal", h.contextTokenAuthorization.MemoryWriteScopes); err != nil {
		return err
	}
	apply.Namespace = namespace
	if ui := GetUserInfo(c); ui != nil && ui.AuthType == AuthTypeTokenReview {
		// The store may resolve either an applied ID or a source-proposal link.
		// Authorize its final choice within the transaction, including retries.
		apply.AuthorizeExistingMemory = func(ctx context.Context, namespace, id string) error {
			return authorizeKubernetesResourceAction(ctx, h.clientset, ui,
				namespace, "get", "core.orka.ai", "memories", id)
		}
	}
	if apply.AppliedBy == "" {
		if ui := GetUserInfo(c); ui != nil {
			apply.AppliedBy = ui.Username
		}
	}
	memory, err := h.memoryProposalStore.ApplyMemoryProposal(c.Context(), apply)
	if err != nil {
		if forbidden, ok := errors.AsType[*fiber.Error](err); ok && forbidden.Code == fiber.StatusForbidden {
			return forbidden
		}
		return memoryStoreError("apply memory proposal", "memory proposal", err)
	}
	return c.JSON(memory)
}

func parseMemoryFilter(c fiber.Ctx, namespace string) (store.MemoryFilter, error) {
	limit, err := parseOptionalLimit(c.Query("limit", ""))
	if err != nil {
		return store.MemoryFilter{}, err
	}
	query := c.Query("query", "")
	if query == "" {
		query = c.Query("q", "")
	}
	return store.MemoryFilter{
		Namespace:       namespace,
		Query:           query,
		SessionName:     c.Query("sessionName", ""),
		AgentName:       c.Query("agentName", ""),
		TaskName:        c.Query("taskName", ""),
		ParentTask:      c.Query("parentTask", ""),
		Source:          c.Query("source", ""),
		Tags:            splitCSV(c.Query("tags", "")),
		IDs:             splitCSV(c.Query("ids", "")),
		IncludeDisabled: c.Query("includeDisabled", "") == queryTrue,
		IncludeDeleted:  c.Query("includeDeleted", "") == queryTrue,
		Limit:           limit,
	}, nil
}

func parseMemoryProposalFilter(c fiber.Ctx, namespace string) (store.MemoryProposalFilter, error) {
	limit, err := parseOptionalLimit(c.Query("limit", ""))
	if err != nil {
		return store.MemoryProposalFilter{}, err
	}
	query := c.Query("query", "")
	if query == "" {
		query = c.Query("q", "")
	}
	return store.MemoryProposalFilter{
		Namespace: namespace,
		TaskName:  c.Query("taskName", ""),
		AgentName: c.Query("agentName", ""),
		Type:      c.Query("type", ""),
		Status:    c.Query("status", ""),
		Query:     query,
		Limit:     limit,
	}, nil
}

func parseOptionalLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		return 0, fiber.NewError(fiber.StatusBadRequest, "invalid limit")
	}
	return limit, nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func applyMemoryUpdate(memory *store.Memory, req store.Memory) {
	if req.SessionName != "" {
		memory.SessionName = req.SessionName
	}
	if req.AgentName != "" {
		memory.AgentName = req.AgentName
	}
	if req.TaskName != "" {
		memory.TaskName = req.TaskName
	}
	if req.ParentTask != "" {
		memory.ParentTask = req.ParentTask
	}
	if req.Source != "" {
		memory.Source = req.Source
	}
	if req.Content != "" {
		memory.Content = req.Content
	}
	if req.Tags != nil {
		memory.Tags = req.Tags
	}
	if req.Disabled {
		memory.Disabled = true
	}
	if req.Deleted {
		memory.Deleted = true
	}
}

func bindMemoryProposalReview(c fiber.Ctx, fallbackNamespace, id string) (store.MemoryProposalReview, error) {
	var req struct {
		Namespace  string `json:"namespace"`
		Status     string `json:"status"`
		Reviewer   string `json:"reviewer"`
		ReviewNote string `json:"reviewNote"`
	}
	if err := c.Bind().JSON(&req); err != nil {
		return store.MemoryProposalReview{}, fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	if req.Namespace == "" {
		req.Namespace = fallbackNamespace
	}
	if strings.TrimSpace(req.Status) == "" {
		return store.MemoryProposalReview{}, fiber.NewError(fiber.StatusBadRequest, "status is required")
	}
	return store.MemoryProposalReview{
		Namespace:  req.Namespace,
		ID:         id,
		Status:     req.Status,
		Reviewer:   req.Reviewer,
		ReviewNote: req.ReviewNote,
	}, nil
}

func bindMemoryProposalApply(c fiber.Ctx, fallbackNamespace, id string) (store.MemoryProposalApply, error) {
	var req struct {
		Namespace string `json:"namespace"`
		AppliedBy string `json:"appliedBy"`
	}
	if strings.TrimSpace(string(c.Body())) != "" {
		if err := c.Bind().JSON(&req); err != nil {
			return store.MemoryProposalApply{}, fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
	}
	if req.Namespace == "" {
		req.Namespace = fallbackNamespace
	}
	return store.MemoryProposalApply{
		Namespace: req.Namespace,
		ID:        id,
		AppliedBy: req.AppliedBy,
	}, nil
}

func memoryStoreError(action, resource string, err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return fiber.NewError(fiber.StatusNotFound, fmt.Sprintf("%s not found", resource))
	}
	if errors.Is(err, store.ErrConflict) {
		return fiber.NewError(fiber.StatusConflict, err.Error())
	}
	if isStoreValidationError(err) {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to %s: %v", action, err))
}

func isStoreValidationError(err error) bool {
	return errors.Is(err, store.ErrValidation)
}
