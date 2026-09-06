/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/store"
)

// taskAccess owns the safe Task read path for public API handlers: name-scoped
// context-token authorization, Kubernetes object loading, not-found mapping, and
// loaded-object authorization for effective Task context.
type taskAccess struct {
	h *Handlers
}

func (h *Handlers) taskAccess() taskAccess {
	return taskAccess{h: h}
}

func (a taskAccess) load(c fiber.Ctx, namespace, taskName string) (*corev1alpha1.Task, error) {
	task := &corev1alpha1.Task{}
	if err := a.h.client.Get(c.Context(), types.NamespacedName{Name: taskName, Namespace: namespace}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fiber.NewError(fiber.StatusNotFound, "task not found")
		}
		return nil, fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to get task: %v", err))
	}
	return task, nil
}

func (a taskAccess) authorizeReadable(c fiber.Ctx, action, namespace, taskName string) error {
	return a.h.authorizeContextTokenTaskRead(c, action, namespace, taskName)
}

func (a taskAccess) loadReadable(c fiber.Ctx, action, namespace, taskName string) (*corev1alpha1.Task, error) {
	if err := a.authorizeReadable(c, action, namespace, taskName); err != nil {
		return nil, err
	}
	return a.loadAuthorized(c, action, namespace, taskName)
}

func (a taskAccess) loadAuthorized(c fiber.Ctx, action, namespace, taskName string) (*corev1alpha1.Task, error) {
	task, err := a.load(c, namespace, taskName)
	if err != nil {
		return nil, err
	}
	if err := a.h.authorizeContextTokenLoadedTask(c, action, task); err != nil {
		return nil, err
	}
	if err := a.authorizeGatewayTask(c, action, task, false); err != nil {
		return nil, err
	}
	return task, nil
}

func (a taskAccess) ensureReadable(c fiber.Ctx, action, namespace, taskName string) error {
	_, err := a.loadReadable(c, action, namespace, taskName)
	return err
}

// loadReadableForContextToken preserves handlers whose non-context-token callers
// historically did not need the parent Task object to exist before continuing.
// The caller should already have run authorizeReadable for the URL task name.
func (a taskAccess) loadReadableForContextToken(c fiber.Ctx, action, namespace, taskName string) (*corev1alpha1.Task, error) {
	ui := GetUserInfo(c)
	contextTokenCaller := a.h.contextTokenAuthorization.Enabled() && ui != nil &&
		ui.AuthType == AuthTypeContextToken && ui.ContextToken != nil
	tokenReviewCaller := ui != nil && ui.AuthType == AuthTypeTokenReview
	if !contextTokenCaller && !tokenReviewCaller {
		return nil, nil
	}
	task, err := a.load(c, namespace, taskName)
	if err != nil {
		return nil, err
	}
	if contextTokenCaller {
		if err := a.h.authorizeContextTokenLoadedTask(c, action, task); err != nil {
			return nil, err
		}
	}
	if err := a.authorizeGatewayTask(c, action, task, false); err != nil {
		return nil, err
	}
	return task, nil
}

func (a taskAccess) authorizeGatewayTaskOperate(c fiber.Ctx, action string, task *corev1alpha1.Task) error {
	return a.authorizeGatewayTask(c, action, task, true)
}

type gatewayTaskAuthorizationKey struct {
	GatewayNamespace string
	NamespaceUID     string
	GatewayName      string
	GatewayUID       string
}

func (a taskAccess) gatewayTaskReadableCached(
	c fiber.Ctx, action string, task *corev1alpha1.Task, cache map[gatewayTaskAuthorizationKey]bool,
) (bool, error) {
	identity, gatewayOwned, err := a.gatewayTaskIdentity(c, task)
	if err != nil {
		return false, err
	}
	if !gatewayOwned {
		return true, nil
	}
	key := gatewayTaskAuthorizationKey{
		GatewayNamespace: identity.GatewayNamespace, NamespaceUID: identity.NamespaceUID,
		GatewayName: identity.GatewayName, GatewayUID: identity.GatewayUID,
	}
	if cache != nil {
		if allowed, ok := cache[key]; ok {
			return allowed, nil
		}
	}
	err = a.authorizeGatewayTaskIdentity(c, action, task, identity, false)
	if err == nil {
		if cache != nil {
			cache[key] = true
		}
		return true, nil
	}
	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) && (fiberErr.Code == fiber.StatusForbidden || fiberErr.Code == fiber.StatusNotFound) {
		if cache != nil {
			cache[key] = false
		}
		return false, nil
	}
	return false, err
}

func (a taskAccess) authorizeGatewayTask(c fiber.Ctx, action string, task *corev1alpha1.Task, operate bool) error {
	identity, gatewayOwned, err := a.gatewayTaskIdentity(c, task)
	if err != nil {
		return err
	}
	if !gatewayOwned {
		return nil
	}
	return a.authorizeGatewayTaskIdentity(c, action, task, identity, operate)
}

func (a taskAccess) authorizeGatewayTaskIdentity(
	c fiber.Ctx, action string, task *corev1alpha1.Task, identity gatewayruntime.TaskOwnerIdentity, operate bool,
) error {
	if err := a.verifyCurrentGatewayTaskIdentity(c, task, identity); err != nil {
		return err
	}
	if operate {
		if err := a.h.authorizeContextTokenAction(c, action+"GatewayOperate", a.h.contextTokenAuthorization.GatewayOperateScopes); err != nil {
			return err
		}
	}
	if err := a.h.authorizeContextTokenAction(c, action+"GatewayRead", a.h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := a.h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", identity.GatewayNamespace, identity.GatewayName); err != nil {
		return err
	}
	if operate {
		if err := a.h.authorizeGatewayKubernetes(c, gatewayVerbUpdate, "gateways", identity.GatewayNamespace, identity.GatewayName); err != nil {
			return err
		}
	}
	return nil
}

func (a taskAccess) verifyCurrentGatewayTaskIdentity(
	c fiber.Ctx, task *corev1alpha1.Task, identity gatewayruntime.TaskOwnerIdentity,
) error {
	if task == nil || strings.TrimSpace(identity.GatewayNamespace) == "" || strings.TrimSpace(identity.NamespaceUID) == "" ||
		strings.TrimSpace(identity.GatewayName) == "" || strings.TrimSpace(identity.GatewayUID) == "" {
		return fiber.NewError(fiber.StatusNotFound, "task not found")
	}
	reader := a.h.apiReader
	if reader == nil {
		reader = a.h.client
	}
	if reader == nil {
		return fiber.NewError(fiber.StatusInternalServerError, "gateway identity reader is unavailable")
	}
	namespace := &corev1.Namespace{}
	if err := reader.Get(c.Context(), types.NamespacedName{Name: identity.GatewayNamespace}, namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusNotFound, "task not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to verify gateway namespace identity")
	}
	if string(namespace.UID) != identity.NamespaceUID {
		return fiber.NewError(fiber.StatusNotFound, "task not found")
	}
	gateway := &gatewayv1alpha1.Gateway{}
	if err := reader.Get(c.Context(), types.NamespacedName{Namespace: identity.GatewayNamespace, Name: identity.GatewayName}, gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusNotFound, "task not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to verify Gateway identity")
	}
	if string(gateway.UID) != identity.GatewayUID {
		return fiber.NewError(fiber.StatusNotFound, "task not found")
	}
	return nil
}

func (a taskAccess) gatewayTaskIdentity(c fiber.Ctx, task *corev1alpha1.Task) (gatewayruntime.TaskOwnerIdentity, bool, error) {
	return gatewayTaskIdentity(c.Context(), a.h.gatewayEventStore, task)
}

func gatewayTaskIdentity(ctx context.Context, events store.GatewayEventStore, task *corev1alpha1.Task) (gatewayruntime.TaskOwnerIdentity, bool, error) {
	if task == nil {
		return gatewayruntime.TaskOwnerIdentity{}, false, nil
	}
	annotations := task.GetAnnotations()
	labels := task.GetLabels()
	eventID := strings.TrimSpace(annotations[gatewayruntime.TaskGatewayEventAnnotation])
	if eventID == "" {
		eventID = strings.TrimSpace(labels[gatewayruntime.TaskGatewayEventLabel])
	}
	identity, gatewayOwned := gatewayruntime.TaskOwner(task)

	if events != nil {
		var durable *gatewayruntime.TaskOwnerIdentity
		if task.UID != "" {
			event, err := events.GetGatewayEventForTask(ctx, task.Namespace, task.Name, string(task.UID))
			if err == nil {
				durable = &gatewayruntime.TaskOwnerIdentity{
					GatewayNamespace: event.Namespace, NamespaceUID: event.NamespaceUID,
					GatewayName: event.GatewayName, GatewayUID: event.GatewayUID,
				}
			} else if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrValidation) {
				return gatewayruntime.TaskOwnerIdentity{}, gatewayOwned, fiber.NewError(fiber.StatusInternalServerError, "failed to verify gateway task ownership")
			}
		}
		if durable == nil && eventID != "" {
			event, err := events.GetGatewayEvent(ctx, task.Namespace, eventID)
			if err == nil && event.TaskName == task.Name && (event.TaskUID == "" || event.TaskUID == string(task.UID)) {
				durable = &gatewayruntime.TaskOwnerIdentity{
					GatewayNamespace: event.Namespace, NamespaceUID: event.NamespaceUID,
					GatewayName: event.GatewayName, GatewayUID: event.GatewayUID,
				}
			} else if err != nil && !errors.Is(err, store.ErrNotFound) {
				return gatewayruntime.TaskOwnerIdentity{}, true, fiber.NewError(fiber.StatusInternalServerError, "failed to verify gateway task ownership")
			}
		}
		if durable != nil {
			if gatewayOwned && gatewayTaskOwnerIdentityComplete(identity) && !gatewayTaskOwnerIdentityEqual(identity, *durable) {
				return gatewayruntime.TaskOwnerIdentity{}, true, fiber.NewError(fiber.StatusNotFound, "task not found")
			}
			return *durable, true, nil
		}
	}
	return identity, gatewayOwned, nil
}

func gatewayTaskOwnerIdentityComplete(identity gatewayruntime.TaskOwnerIdentity) bool {
	return strings.TrimSpace(identity.GatewayNamespace) != "" && strings.TrimSpace(identity.NamespaceUID) != "" &&
		strings.TrimSpace(identity.GatewayName) != "" && strings.TrimSpace(identity.GatewayUID) != ""
}

func gatewayTaskOwnerIdentityEqual(left, right gatewayruntime.TaskOwnerIdentity) bool {
	return left.GatewayNamespace == right.GatewayNamespace && left.NamespaceUID == right.NamespaceUID &&
		left.GatewayName == right.GatewayName && left.GatewayUID == right.GatewayUID
}
