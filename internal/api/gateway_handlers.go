/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/gateway/protocol"
	"github.com/orka-agents/orka/internal/store"
)

const (
	gatewayVerbGet    = "get"
	gatewayVerbList   = "list"
	gatewayVerbUpdate = "update"
)

// HandleGatewayEvent accepts one adapter-authenticated normalized event.
func (h *Handlers) HandleGatewayEvent(c fiber.Ctx) error {
	if h.gatewayService == nil {
		c.RequestCtx().SetConnectionClose()
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway ingress is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Params("namespace"))
	if err != nil {
		c.RequestCtx().SetConnectionClose()
		return err
	}
	limiterKey := c.IP()
	// This limiter intentionally runs before body parsing, authentication, and duplicate lookup.
	// Letting retries bypass it would give unauthenticated sources a datastore-backed oracle and
	// reopen the pre-auth resource-exhaustion path; a throttled duplicate may safely retry later.
	if h.gatewayIngressLimiter != nil && !h.gatewayIngressLimiter.Allow(limiterKey, time.Now()) {
		c.RequestCtx().SetConnectionClose()
		return fiber.NewError(fiber.StatusTooManyRequests, "gateway ingress rate limit exceeded")
	}
	body, err := readGatewayIngressBody(c)
	if err != nil {
		return err
	}
	response, err := h.gatewayService.AdmitEvent(
		c.Context(), namespace, c.Params("name"), c.Get("Authorization"), body,
	)
	if err != nil {
		if httpErr, ok := errors.AsType[*gatewayruntime.HTTPError](err); ok {
			return fiber.NewError(httpErr.Code, httpErr.Message)
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to accept gateway event")
	}
	return c.Status(fiber.StatusAccepted).JSON(response)
}

func readGatewayIngressBody(c fiber.Ctx) ([]byte, error) {
	if encoding := strings.TrimSpace(c.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		c.RequestCtx().SetConnectionClose()
		return nil, fiber.NewError(fiber.StatusUnsupportedMediaType, "compressed gateway ingress is not supported")
	}
	if length := c.Request().Header.ContentLength(); length > protocol.MaxHTTPBodyBytes {
		c.RequestCtx().SetConnectionClose()
		return nil, fiber.NewError(fiber.StatusRequestEntityTooLarge, "gateway event body exceeds 256 KiB")
	}
	stream := c.Request().BodyStream()
	if stream == nil {
		body := c.Body()
		if len(body) > protocol.MaxHTTPBodyBytes {
			c.RequestCtx().SetConnectionClose()
			return nil, fiber.NewError(fiber.StatusRequestEntityTooLarge, "gateway event body exceeds 256 KiB")
		}
		return body, nil
	}
	if closer, ok := stream.(io.Closer); ok {
		defer closer.Close() //nolint:errcheck
	}
	body, err := io.ReadAll(io.LimitReader(stream, protocol.MaxHTTPBodyBytes+1))
	if err != nil {
		c.RequestCtx().SetConnectionClose()
		return nil, fiber.NewError(fiber.StatusBadRequest, "failed to read gateway event body")
	}
	if len(body) > protocol.MaxHTTPBodyBytes {
		c.RequestCtx().SetConnectionClose()
		return nil, fiber.NewError(fiber.StatusRequestEntityTooLarge, "gateway event body exceeds 256 KiB")
	}
	return body, nil
}

// ListGatewayClasses lists cluster-scoped adapter profiles.
func (h *Handlers) ListGatewayClasses(c fiber.Ctx) error {
	if err := h.authorizeContextTokenAction(c, "listGatewayClasses", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbList, "gatewayclasses", "", ""); err != nil {
		return err
	}
	pagination, err := ParsePagination(c.Query("limit", ""), c.Query("continue", ""))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	list := &gatewayv1alpha1.GatewayClassList{}
	if err := h.listGatewayPage(c.Context(), list, client.ListOptions{Limit: pagination.Limit, Continue: pagination.Continue}, "gateway classes"); err != nil {
		return err
	}
	return c.JSON(ListResponse{Items: list.Items, Metadata: ListMeta{Continue: list.Continue, RemainingItemCount: list.RemainingItemCount}})
}

// GetGatewayClass gets one cluster-scoped adapter profile.
func (h *Handlers) GetGatewayClass(c fiber.Ctx) error {
	if err := h.authorizeContextTokenAction(c, "getGatewayClass", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gatewayclasses", "", c.Params("name")); err != nil {
		return err
	}
	object := &gatewayv1alpha1.GatewayClass{}
	if err := h.client.Get(c.Context(), client.ObjectKey{Name: c.Params("name")}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusNotFound, "gateway class not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get gateway class")
	}
	return c.JSON(object)
}

// ListGateways lists namespaced adapter instances.
func (h *Handlers) ListGateways(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listGateways", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbList, "gateways", namespace, ""); err != nil {
		return err
	}
	pagination, err := ParsePagination(c.Query("limit", ""), c.Query("continue", ""))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	list := &gatewayv1alpha1.GatewayList{}
	if err := h.listGatewayPage(c.Context(), list, client.ListOptions{Namespace: namespace, Limit: pagination.Limit, Continue: pagination.Continue}, "gateways"); err != nil {
		return err
	}
	return c.JSON(ListResponse{Items: list.Items, Metadata: ListMeta{Continue: list.Continue, RemainingItemCount: list.RemainingItemCount}})
}

// GetGateway gets one namespaced adapter instance.
func (h *Handlers) GetGateway(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getGateway", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, c.Params("name")); err != nil {
		return err
	}
	object := &gatewayv1alpha1.Gateway{}
	if err := h.client.Get(c.Context(), client.ObjectKey{Namespace: namespace, Name: c.Params("name")}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusNotFound, "gateway not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get gateway")
	}
	return c.JSON(object)
}

// ListGatewayBindings lists namespaced semantic bindings.
func (h *Handlers) ListGatewayBindings(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listGatewayBindings", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbList, "gatewaybindings", namespace, ""); err != nil {
		return err
	}
	pagination, err := ParsePagination(c.Query("limit", ""), c.Query("continue", ""))
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	list := &gatewayv1alpha1.GatewayBindingList{}
	if err := h.listGatewayPage(c.Context(), list, client.ListOptions{Namespace: namespace, Limit: pagination.Limit, Continue: pagination.Continue}, "gateway bindings"); err != nil {
		return err
	}
	return c.JSON(ListResponse{Items: list.Items, Metadata: ListMeta{Continue: list.Continue, RemainingItemCount: list.RemainingItemCount}})
}

// GetGatewayBinding gets one namespaced semantic binding.
func (h *Handlers) GetGatewayBinding(c fiber.Ctx) error {
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getGatewayBinding", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gatewaybindings", namespace, c.Params("name")); err != nil {
		return err
	}
	object := &gatewayv1alpha1.GatewayBinding{}
	if err := h.client.Get(c.Context(), client.ObjectKey{Namespace: namespace, Name: c.Params("name")}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return fiber.NewError(fiber.StatusNotFound, "gateway binding not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get gateway binding")
	}
	return c.JSON(object)
}

// ListGatewayEvents lists normalized durable ingress records.
func (h *Handlers) ListGatewayEvents(c fiber.Ctx) error {
	if h.gatewayEventStore == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway event store is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listGatewayEvents", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	gatewayName := strings.TrimSpace(c.Query("gateway", ""))
	var namespaceUID, gatewayUID string
	var gatewayUIDs []string
	if gatewayName != "" {
		identity, identityErr := h.currentGatewayIdentity(c, namespace, gatewayName)
		if identityErr != nil {
			return identityErr
		}
		if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, gatewayName); err != nil {
			return err
		}
		namespaceUID, gatewayUID = identity.NamespaceUID, identity.GatewayUID
	} else {
		if err := h.authorizeGatewayKubernetes(c, gatewayVerbList, "gateways", namespace, ""); err != nil {
			return err
		}
		namespaceUID, err = h.currentNamespaceUID(c, namespace)
		if err != nil {
			return err
		}
		gatewayUIDs, err = h.currentGatewayUIDs(c, namespace)
		if err != nil {
			return err
		}
	}
	pagination, cursor, err := parseGatewayPagination(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	if gatewayName == "" && len(gatewayUIDs) == 0 {
		return c.JSON(ListResponse{Items: []store.GatewayEvent{}, Metadata: ListMeta{}})
	}
	pageLimit := gatewayPageLimit(pagination.Limit)
	events, err := h.gatewayEventStore.ListGatewayEvents(c.Context(), store.GatewayEventFilter{
		Namespace: namespace, NamespaceUID: namespaceUID, GatewayUID: gatewayUID, GatewayUIDs: gatewayUIDs,
		GatewayName: gatewayName, BindingName: c.Query("binding", ""),
		SessionName: c.Query("session", ""), TaskName: c.Query("task", ""),
		States:          parseGatewayEventStates(c.Query("state", "")),
		BeforeCreatedAt: cursor.CreatedAt, BeforeID: cursor.ID, Limit: gatewayStoreListLimit(pageLimit),
	})
	if err != nil {
		return gatewayStoreHTTPError(err, "list gateway events")
	}
	events, next := paginateGatewayEvents(events, pageLimit)
	return c.JSON(ListResponse{Items: events, Metadata: ListMeta{Continue: next}})
}

// GetGatewayEvent gets one normalized ingress record.
func (h *Handlers) GetGatewayEvent(c fiber.Ctx) error {
	if h.gatewayEventStore == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway event store is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getGatewayEvent", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	event, err := h.gatewayEventStore.GetGatewayEvent(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return gatewayStoreHTTPError(err, "get gateway event")
	}
	identity, err := h.currentGatewayIdentity(c, namespace, event.GatewayName)
	if err != nil {
		return err
	}
	if !identity.matches(event.NamespaceUID, event.GatewayUID) {
		return fiber.NewError(fiber.StatusNotFound, "gateway event not found")
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, event.GatewayName); err != nil {
		return err
	}
	return c.JSON(event)
}

// ListGatewayDeliveries lists durable outbox records.
func (h *Handlers) ListGatewayDeliveries(c fiber.Ctx) error {
	if h.gatewayDeliveryStore == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway delivery store is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "listGatewayDeliveries", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	gatewayName := strings.TrimSpace(c.Query("gateway", ""))
	var namespaceUID, gatewayUID string
	var gatewayUIDs []string
	if gatewayName != "" {
		identity, identityErr := h.currentGatewayIdentity(c, namespace, gatewayName)
		if identityErr != nil {
			return identityErr
		}
		if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, gatewayName); err != nil {
			return err
		}
		namespaceUID, gatewayUID = identity.NamespaceUID, identity.GatewayUID
	} else {
		if err := h.authorizeGatewayKubernetes(c, gatewayVerbList, "gateways", namespace, ""); err != nil {
			return err
		}
		namespaceUID, err = h.currentNamespaceUID(c, namespace)
		if err != nil {
			return err
		}
		gatewayUIDs, err = h.currentGatewayUIDs(c, namespace)
		if err != nil {
			return err
		}
	}
	pagination, cursor, err := parseGatewayPagination(c)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	if gatewayName == "" && len(gatewayUIDs) == 0 {
		return c.JSON(ListResponse{Items: []store.GatewayDelivery{}, Metadata: ListMeta{}})
	}
	pageLimit := gatewayPageLimit(pagination.Limit)
	deliveries, err := h.gatewayDeliveryStore.ListGatewayDeliveries(c.Context(), store.GatewayDeliveryFilter{
		Namespace: namespace, NamespaceUID: namespaceUID, GatewayUID: gatewayUID, GatewayUIDs: gatewayUIDs,
		GatewayName: gatewayName, BindingName: c.Query("binding", ""),
		EventID: c.Query("event", ""), SessionName: c.Query("session", ""), TaskName: c.Query("task", ""),
		States:          parseGatewayDeliveryStates(c.Query("state", "")),
		BeforeCreatedAt: cursor.CreatedAt, BeforeID: cursor.ID, Limit: gatewayStoreListLimit(pageLimit),
	})
	if err != nil {
		return gatewayStoreHTTPError(err, "list gateway deliveries")
	}
	deliveries, next := paginateGatewayDeliveries(deliveries, pageLimit)
	return c.JSON(ListResponse{Items: deliveries, Metadata: ListMeta{Continue: next}})
}

// GetGatewayDelivery gets one durable outbox record.
func (h *Handlers) GetGatewayDelivery(c fiber.Ctx) error {
	if h.gatewayDeliveryStore == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway delivery store is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "getGatewayDelivery", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	delivery, err := h.gatewayDeliveryStore.GetGatewayDelivery(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return gatewayStoreHTTPError(err, "get gateway delivery")
	}
	identity, err := h.currentGatewayIdentity(c, namespace, delivery.GatewayName)
	if err != nil {
		return err
	}
	if !identity.matches(delivery.NamespaceUID, delivery.GatewayUID) {
		return fiber.NewError(fiber.StatusNotFound, "gateway delivery not found")
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, delivery.GatewayName); err != nil {
		return err
	}
	return c.JSON(delivery)
}

// RetryGatewayDelivery manually requeues one dead-lettered delivery.
func (h *Handlers) RetryGatewayDelivery(c fiber.Ctx) error {
	if h.gatewayService == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway delivery service is unavailable")
	}
	if h.gatewayDeliveryStore == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway delivery store is unavailable")
	}
	namespace, err := h.resolveNamespace(c, c.Query("namespace", ""))
	if err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "retryGatewayDelivery", h.contextTokenAuthorization.GatewayOperateScopes); err != nil {
		return err
	}
	if err := h.authorizeContextTokenAction(c, "retryGatewayDeliveryRead", h.contextTokenAuthorization.GatewayReadScopes); err != nil {
		return err
	}
	current, err := h.gatewayDeliveryStore.GetGatewayDelivery(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return gatewayStoreHTTPError(err, "get gateway delivery")
	}
	identity, err := h.currentGatewayIdentity(c, namespace, current.GatewayName)
	if err != nil {
		return err
	}
	if !identity.matches(current.NamespaceUID, current.GatewayUID) {
		return fiber.NewError(fiber.StatusNotFound, "gateway delivery not found")
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbGet, "gateways", namespace, current.GatewayName); err != nil {
		return err
	}
	if err := h.authorizeGatewayKubernetes(c, gatewayVerbUpdate, "gateways", namespace, current.GatewayName); err != nil {
		return err
	}
	delivery, err := h.gatewayService.RetryDelivery(c.Context(), namespace, c.Params("id"))
	if err != nil {
		return gatewayStoreHTTPError(err, "retry gateway delivery")
	}
	return c.Status(http.StatusAccepted).JSON(delivery)
}

type gatewayCurrentIdentity struct {
	NamespaceUID string
	GatewayUID   string
}

func (i gatewayCurrentIdentity) matches(namespaceUID, gatewayUID string) bool {
	return i.NamespaceUID != "" && i.GatewayUID != "" && i.NamespaceUID == namespaceUID && i.GatewayUID == gatewayUID
}

// listGatewayPage serves one page of gateway resources. Gateway reads prefer
// the uncached API reader, which pages natively. When only the cached client
// is configured, a limited request is served as one unlimited cached list
// (the cache truncates at Limit and stamps an unusable continue sentinel) and
// continue tokens are rejected, so a cache-backed fallback never presents a
// truncated collection as complete behind an empty cursor.
func (h *Handlers) listGatewayPage(ctx context.Context, list client.ObjectList, opts client.ListOptions, what string) error {
	switch {
	case h.apiReader != nil:
		if err := h.apiReader.List(ctx, list, &opts); err != nil {
			// An expired or malformed continue token is the caller's
			// cursor problem (410/400), not a server failure.
			return listPageError(what, err)
		}
	case h.client == nil:
		return fiber.NewError(fiber.StatusServiceUnavailable, "gateway API reader is unavailable")
	case opts.Continue != "":
		return fiber.NewError(fiber.StatusBadRequest, "continue is not supported: list pagination requires an uncached API reader")
	default:
		opts.Limit = 0
		if err := h.client.List(ctx, list, &opts); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, "failed to list "+what)
		}
		list.SetRemainingItemCount(nil)
	}
	// Defense in depth: the cache sentinel must never reach a client.
	list.SetContinue(NormalizeListContinue(list.GetContinue()))
	return nil
}

func (h *Handlers) gatewayIdentityReader() client.Reader {
	if h.apiReader != nil {
		return h.apiReader
	}
	return h.client
}

func (h *Handlers) currentNamespaceUID(c fiber.Ctx, namespace string) (string, error) {
	reader := h.gatewayIdentityReader()
	if reader == nil {
		return "", fiber.NewError(fiber.StatusServiceUnavailable, "gateway identity lookup is unavailable")
	}
	object := &corev1.Namespace{}
	if err := reader.Get(c.Context(), client.ObjectKey{Name: namespace}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fiber.NewError(fiber.StatusNotFound, "namespace not found")
		}
		return "", fiber.NewError(fiber.StatusInternalServerError, "failed to resolve namespace identity")
	}
	return string(object.UID), nil
}

func (h *Handlers) currentGatewayIdentity(c fiber.Ctx, namespace, name string) (gatewayCurrentIdentity, error) {
	namespaceUID, err := h.currentNamespaceUID(c, namespace)
	if err != nil {
		return gatewayCurrentIdentity{}, err
	}
	reader := h.gatewayIdentityReader()
	object := &gatewayv1alpha1.Gateway{}
	if err := reader.Get(c.Context(), client.ObjectKey{Namespace: namespace, Name: name}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return gatewayCurrentIdentity{}, fiber.NewError(fiber.StatusNotFound, "gateway not found")
		}
		return gatewayCurrentIdentity{}, fiber.NewError(fiber.StatusInternalServerError, "failed to resolve gateway identity")
	}
	return gatewayCurrentIdentity{NamespaceUID: namespaceUID, GatewayUID: string(object.UID)}, nil
}

func (h *Handlers) currentGatewayUIDs(c fiber.Ctx, namespace string) ([]string, error) {
	reader := h.gatewayIdentityReader()
	if reader == nil {
		return nil, fiber.NewError(fiber.StatusServiceUnavailable, "gateway identity lookup is unavailable")
	}
	list := &gatewayv1alpha1.GatewayList{}
	if err := reader.List(c.Context(), list, client.InNamespace(namespace)); err != nil {
		return nil, fiber.NewError(fiber.StatusInternalServerError, "failed to list current gateway identities")
	}
	result := make([]string, 0, len(list.Items))
	for i := range list.Items {
		if uid := string(list.Items[i].UID); uid != "" {
			result = append(result, uid)
		}
	}
	return result, nil
}

type gatewayListCursor struct {
	CreatedAt *time.Time `json:"createdAt,omitempty"`
	ID        string     `json:"id,omitempty"`
}

func parseGatewayPagination(c fiber.Ctx) (*Pagination, gatewayListCursor, error) {
	rawCursor := strings.TrimSpace(c.Query("continue", ""))
	if rawCursor == "" {
		rawCursor = strings.TrimSpace(c.Query("cursor", ""))
	}
	rawLimit := strings.TrimSpace(c.Query("limit", ""))
	if rawLimit == "0" {
		rawLimit = fmt.Sprint(MaxLimit)
	}
	pagination, err := ParsePagination(rawLimit, rawCursor)
	if err != nil {
		return nil, gatewayListCursor{}, err
	}
	cursor, err := decodeGatewayListCursor(rawCursor)
	if err != nil {
		return nil, gatewayListCursor{}, err
	}
	return pagination, cursor, nil
}

func decodeGatewayListCursor(raw string) (gatewayListCursor, error) {
	if raw == "" {
		return gatewayListCursor{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return gatewayListCursor{}, fmt.Errorf("invalid continue token")
	}
	var cursor gatewayListCursor
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.CreatedAt == nil || cursor.ID == "" {
		return gatewayListCursor{}, fmt.Errorf("invalid continue token")
	}
	return cursor, nil
}

func encodeGatewayListCursor(createdAt time.Time, id string) string {
	data, _ := json.Marshal(gatewayListCursor{CreatedAt: &createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(data)
}

func gatewayPageLimit(limit int64) int {
	if limit < 1 || limit > int64(MaxLimit) {
		return MaxLimit
	}
	return int(limit)
}

func gatewayStoreListLimit(pageLimit int) int {
	if pageLimit <= 0 || pageLimit > MaxLimit {
		pageLimit = MaxLimit
	}
	return pageLimit + 1
}

func paginateGatewayEvents(events []store.GatewayEvent, limit int) ([]store.GatewayEvent, string) {
	if limit <= 0 || limit > MaxLimit {
		limit = MaxLimit
	}
	if len(events) <= limit {
		return events, ""
	}
	page := events[:limit]
	last := page[len(page)-1]
	return page, encodeGatewayListCursor(last.CreatedAt, last.ID)
}

func paginateGatewayDeliveries(deliveries []store.GatewayDelivery, limit int) ([]store.GatewayDelivery, string) {
	if limit <= 0 || limit > MaxLimit {
		limit = MaxLimit
	}
	if len(deliveries) <= limit {
		return deliveries, ""
	}
	page := deliveries[:limit]
	last := page[len(page)-1]
	return page, encodeGatewayListCursor(last.CreatedAt, last.ID)
}

func (h *Handlers) authorizeGatewayKubernetes(
	c fiber.Ctx, verb, resource, namespace, name string,
) error {
	return authorizeKubernetesResourceAction(
		c.Context(), h.clientset, GetUserInfo(c), namespace, verb, gatewayv1alpha1.GroupVersion.Group, resource, name,
	)
}

func parseGatewayEventStates(raw string) []store.GatewayEventState {
	parts := splitGatewayFilter(raw)
	states := make([]store.GatewayEventState, 0, len(parts))
	for _, part := range parts {
		states = append(states, store.GatewayEventState(part))
	}
	return states
}

func parseGatewayDeliveryStates(raw string) []store.GatewayDeliveryState {
	parts := splitGatewayFilter(raw)
	states := make([]store.GatewayDeliveryState, 0, len(parts))
	for _, part := range parts {
		states = append(states, store.GatewayDeliveryState(part))
	}
	return states
}

func splitGatewayFilter(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var result []string
	for part := range strings.SplitSeq(raw, ",") {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func gatewayStoreHTTPError(err error, operation string) error {
	if httpErr, ok := errors.AsType[*gatewayruntime.HTTPError](err); ok {
		return fiber.NewError(httpErr.Code, httpErr.Message)
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return fiber.NewError(fiber.StatusNotFound, strings.TrimPrefix(operation, "get ")+" not found")
	case errors.Is(err, store.ErrConflict):
		return fiber.NewError(fiber.StatusConflict, fmt.Sprintf("cannot %s in the current state", operation))
	case errors.Is(err, store.ErrValidation):
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	default:
		return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("failed to %s", operation))
	}
}
