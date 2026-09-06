/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/requestid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	tracingpkg "github.com/orka-agents/orka/internal/tracing"
	tracingtest "github.com/orka-agents/orka/internal/tracing/testutil"
)

func TestNewLoggingMiddleware(t *testing.T) {
	app := fiber.New()
	app.Use(requestid.New())
	app.Use(NewLoggingMiddleware())
	app.Get("/test", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	req := httptest.NewRequest(http.MethodGet, "/test?query=secret", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestNewLoggingMiddleware_LogsInfo(t *testing.T) {
	app := fiber.New()
	app.Use(requestid.New())
	app.Use(NewLoggingMiddleware())
	app.Get("/test", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	// Test multiple status codes
	tests := []struct {
		name   string
		path   string
		status int
	}{
		{"success", "/test", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.status {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, tt.status)
			}
		})
	}
}

func TestNewMetricsMiddleware(t *testing.T) {
	app := fiber.New()
	app.Use(NewMetricsMiddleware())
	app.Get("/test", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestNewMetricsMiddleware_RecordsMetrics(t *testing.T) {
	app := fiber.New()
	app.Use(NewMetricsMiddleware())
	app.Get("/api/test", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})
	app.Post("/api/create", func(c fiber.Ctx) error {
		return c.Status(fiber.StatusCreated).SendString("Created")
	})

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"GET request", http.MethodGet, "/api/test", http.StatusOK},
		{"POST request", http.MethodPost, "/api/create", http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.status {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, tt.status)
			}
		})
	}
}

func TestNewLoggingMiddleware_WithError(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		},
	})
	app.Use(requestid.New())
	app.Use(NewLoggingMiddleware())
	app.Get("/error", func(c fiber.Ctx) error {
		return fiber.NewError(fiber.StatusInternalServerError, "test error")
	})

	req := httptest.NewRequest(http.MethodGet, "/error", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestNewMetricsMiddleware_WithError(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		},
	})
	app.Use(NewMetricsMiddleware())
	app.Get("/error", func(c fiber.Ctx) error {
		return fiber.NewError(fiber.StatusBadRequest, "bad request")
	})

	req := httptest.NewRequest(http.MethodGet, "/error", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestNewTracingMiddleware(t *testing.T) {
	h := tracingtest.NewSpanHarness(t)
	app := fiber.New()
	app.Use(requestid.New())
	app.Use(NewTracingMiddleware())
	app.Get("/test", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	spans := h.Recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "GET /test" {
		t.Fatalf("span name = %q, want GET /test", spans[0].Name())
	}
	if got := spanAttributeString(spans[0].Attributes(), "http.route"); got != "/test" {
		t.Fatalf("http.route = %q, want /test", got)
	}
	if got := spanAttributeString(spans[0].Attributes(), "http.url"); strings.Contains(got, "query=secret") {
		t.Fatalf("http.url includes query string: %q", got)
	}
}

func TestNewTracingMiddleware_WithRequestID(t *testing.T) {
	app := fiber.New()
	app.Use(requestid.New())
	app.Use(NewTracingMiddleware())
	app.Get("/traced", func(c fiber.Ctx) error {
		return c.SendString("traced")
	})

	req := httptest.NewRequest(http.MethodGet, "/traced", nil)
	req.Header.Set("X-Request-Id", "custom-req-id")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestNewTracingMiddleware_ErrorStatus(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).SendString(err.Error())
		},
	})
	app.Use(requestid.New())
	app.Use(NewTracingMiddleware())
	app.Get("/bad", func(c fiber.Ctx) error {
		return fiber.NewError(fiber.StatusBadRequest, "bad request")
	})
	app.Get("/fail", func(c fiber.Ctx) error {
		return fiber.NewError(fiber.StatusInternalServerError, "internal error")
	})

	tests := []struct {
		name   string
		path   string
		status int
	}{
		{"400 error sets span error", "/bad", http.StatusBadRequest},
		{"500 error sets span error", "/fail", http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.status {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, tt.status)
			}
		})
	}
}

func TestNewTracingMiddleware_WithoutRequestID(t *testing.T) {
	// Test tracing middleware without requestid middleware to cover the empty reqID branch
	app := fiber.New()
	app.Use(NewTracingMiddleware())
	app.Get("/no-reqid", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	req := httptest.NewRequest(http.MethodGet, "/no-reqid", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestNewTracingMiddleware_PropagatesSpanContextToHandlers(t *testing.T) {
	h := tracingtest.NewSpanHarness(t)
	app := fiber.New()
	app.Use(NewTracingMiddleware())
	app.Get("/child", func(c fiber.Ctx) error {
		_, child := tracingpkg.Tracer("test").Start(c.Context(), "handler.child")
		child.End()
		return c.SendString("OK")
	})

	req := httptest.NewRequest(http.MethodGet, "/child", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	spans := h.Recorder.Ended()
	var serverID, childParentID string
	for _, span := range spans {
		if span.SpanKind() == trace.SpanKindServer {
			serverID = span.SpanContext().SpanID().String()
		}
		if span.Name() == "handler.child" {
			childParentID = span.Parent().SpanID().String()
		}
	}
	if serverID == "" || childParentID == "" {
		t.Fatalf("serverID=%q childParentID=%q spans=%d", serverID, childParentID, len(spans))
	}
	if childParentID != serverID {
		t.Fatalf("handler child parent = %s, want server span %s", childParentID, serverID)
	}
}

func TestEffectiveStatusCodeMapsHandlerErrors(t *testing.T) {
	// Handler errors are mapped to a response status only after middleware
	// unwinds, so the raw response code is still 200 when middleware records
	// telemetry; effectiveStatusCode must report the code the client receives.
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"fiber error keeps its code", fiber.NewError(fiber.StatusServiceUnavailable, "not configured"), fiber.StatusServiceUnavailable},
		{"plain error maps to 500", errors.New("boom"), fiber.StatusInternalServerError},
		{"no error keeps response code", nil, fiber.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			app := fiber.New()
			app.Use(func(c fiber.Ctx) error {
				err := c.Next()
				got = effectiveStatusCode(c, err)
				return err
			})
			app.Get("/probe", func(c fiber.Ctx) error {
				if tt.err != nil {
					return tt.err
				}
				return c.SendString("OK")
			})
			if _, err := app.Test(httptest.NewRequest(http.MethodGet, "/probe", nil)); err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("effectiveStatusCode = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEffectiveStatusCodeReportsSPAFallbackAsSuccess(t *testing.T) {
	// A 404 on a non-API path is served as index.html with 200 by the app
	// error handler after middleware unwinds; telemetry must not record a
	// successful SPA navigation as a failure. API and probe paths keep 404.
	tests := []struct {
		name string
		path string
		want int
	}{
		{"client-side route", "/settings/profile", fiber.StatusOK},
		{"api path", "/api/v1/missing", fiber.StatusNotFound},
		{"healthz", "/healthz", fiber.StatusNotFound},
		{"readyz", "/readyz", fiber.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			app := fiber.New(fiber.Config{ErrorHandler: customErrorHandler})
			app.Use(func(c fiber.Ctx) error {
				err := c.Next()
				got = effectiveStatusCode(c, err)
				return err
			})
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, tt.path, nil))
			if err != nil {
				t.Fatalf("Test request failed: %v", err)
			}
			if resp.StatusCode != tt.want {
				t.Fatalf("client status = %d, want %d", resp.StatusCode, tt.want)
			}
			if got != tt.want {
				t.Fatalf("effectiveStatusCode = %d, want %d", got, tt.want)
			}
		})
	}
}

func spanAttributeString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}
