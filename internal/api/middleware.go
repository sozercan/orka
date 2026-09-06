/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/requestid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/tracing"
)

// NewLoggingMiddleware creates a logging middleware
func NewLoggingMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()

		// Get request ID from middleware
		reqID := requestid.FromContext(c)

		// Process request
		err := c.Next()

		// Log the request
		duration := time.Since(start)
		status := effectiveStatusCode(c, err)

		log.Info("request completed",
			"requestId", reqID,
			"method", c.Method(),
			"path", c.Path(),
			"status", status,
			"duration", duration.String(),
			"ip", c.IP(),
		)

		return err
	}
}

// NewMetricsMiddleware creates a metrics middleware
func NewMetricsMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()

		// Process request
		err := c.Next()

		// Record metrics
		duration := time.Since(start)
		status := effectiveStatusCode(c, err)
		method := c.Method()
		path := c.Route().Path

		// Record in Prometheus metrics
		metrics.RecordAPIRequest(path, method, status, duration.Seconds())

		return err
	}
}

// NewTracingMiddleware creates an OpenTelemetry tracing middleware.
func NewTracingMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		tracer := tracing.Tracer("orka.api")
		ctx := otel.GetTextMapPropagator().Extract(c.Context(), fiberHeaderCarrier{c: c})
		method := c.Method()
		ctx, span := tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", method),
				attribute.String("http.url", c.BaseURL()+c.Path()),
			),
		)
		defer span.End()

		// Make the extracted server span context visible to downstream handlers.
		c.SetContext(ctx)

		if reqID := requestid.FromContext(c); reqID != "" {
			span.SetAttributes(attribute.String("http.request_id", reqID))
		}

		err := c.Next()

		if route := c.Route().Path; route != "" {
			span.SetName(fmt.Sprintf("%s %s", method, route))
			span.SetAttributes(attribute.String("http.route", route))
		}

		status := effectiveStatusCode(c, err)
		span.SetAttributes(attribute.Int("http.status_code", status))
		if status >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
		}

		return err
	}
}

// effectiveStatusCode resolves the status a client will actually receive.
// Handler errors are mapped by the app error handler only after middleware
// unwinds, so the response still carries the default 200 here. A 404 on a
// non-API path is served as the SPA index page with 200 by that same error
// handler, so it is reported as a success rather than a failure.
func effectiveStatusCode(c fiber.Ctx, err error) int {
	status := c.Response().StatusCode()
	if err != nil && status < fiber.StatusBadRequest {
		if fiberErr, ok := errors.AsType[*fiber.Error](err); ok {
			status = fiberErr.Code
		} else {
			status = fiber.StatusInternalServerError
		}
	}
	if status == fiber.StatusNotFound && spaFallbackEligible(c.Path()) {
		if _, ok := spaIndexHTML(); ok {
			status = fiber.StatusOK
		}
	}
	return status
}

type fiberHeaderCarrier struct {
	c fiber.Ctx
}

func (c fiberHeaderCarrier) Get(key string) string {
	if c.c == nil {
		return ""
	}
	return c.c.Get(key)
}

func (c fiberHeaderCarrier) Set(string, string) {}

func (c fiberHeaderCarrier) Keys() []string {
	if c.c == nil {
		return nil
	}
	headers := c.c.GetReqHeaders()
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	return keys
}
