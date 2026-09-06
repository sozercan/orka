/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"errors"
	"testing"

	"github.com/gofiber/fiber/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestParsePagination(t *testing.T) {
	tests := []struct {
		name          string
		limitStr      string
		continueToken string
		wantLimit     int64
		wantContinue  string
		wantErr       bool
	}{
		{
			name:          "default values",
			limitStr:      "",
			continueToken: "",
			wantLimit:     DefaultLimit,
			wantContinue:  "",
			wantErr:       false,
		},
		{
			name:          "valid limit",
			limitStr:      "50",
			continueToken: "",
			wantLimit:     50,
			wantContinue:  "",
			wantErr:       false,
		},
		{
			name:          "valid limit with continue token",
			limitStr:      "25",
			continueToken: "abc123",
			wantLimit:     25,
			wantContinue:  "abc123",
			wantErr:       false,
		},
		{
			name:          "limit exceeds max",
			limitStr:      "1000",
			continueToken: "",
			wantLimit:     MaxLimit,
			wantContinue:  "",
			wantErr:       false,
		},
		{
			name:          "limit equals max",
			limitStr:      "500",
			continueToken: "",
			wantLimit:     MaxLimit,
			wantContinue:  "",
			wantErr:       false,
		},
		{
			name:          "minimum valid limit",
			limitStr:      "1",
			continueToken: "",
			wantLimit:     1,
			wantContinue:  "",
			wantErr:       false,
		},
		{
			name:          "invalid limit - not a number",
			limitStr:      "abc",
			continueToken: "",
			wantErr:       true,
		},
		{
			name:          "invalid limit - negative",
			limitStr:      "-1",
			continueToken: "",
			wantErr:       true,
		},
		{
			name:          "invalid limit - zero",
			limitStr:      "0",
			continueToken: "",
			wantErr:       true,
		},
		{
			name:          "invalid limit - float",
			limitStr:      "10.5",
			continueToken: "",
			wantErr:       true,
		},
		{
			name:          "empty limit with continue token",
			limitStr:      "",
			continueToken: "token123",
			wantLimit:     DefaultLimit,
			wantContinue:  "token123",
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParsePagination(tt.limitStr, tt.continueToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePagination() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if p.Limit != tt.wantLimit {
				t.Errorf("ParsePagination() Limit = %v, want %v", p.Limit, tt.wantLimit)
			}
			if p.Continue != tt.wantContinue {
				t.Errorf("ParsePagination() Continue = %v, want %v", p.Continue, tt.wantContinue)
			}
		})
	}
}

func TestParsePagination_Constants(t *testing.T) {
	// Verify constants are set correctly
	if DefaultLimit != 100 {
		t.Errorf("DefaultLimit = %d, want 100", DefaultLimit)
	}
	if MaxLimit != 500 {
		t.Errorf("MaxLimit = %d, want 500", MaxLimit)
	}
}

func TestParsePaginationDropsCacheContinueSentinel(t *testing.T) {
	p, err := ParsePagination("50", cacheContinueUnsupported)
	if err != nil {
		t.Fatalf("ParsePagination() error = %v", err)
	}
	if p.Continue != "" {
		t.Fatalf("ParsePagination() Continue = %q, want empty", p.Continue)
	}
	if got := NormalizeListContinue(cacheContinueUnsupported); got != "" {
		t.Fatalf("NormalizeListContinue(sentinel) = %q, want empty", got)
	}
	if got := NormalizeListContinue("eyJ2IjoibWV0YSJ9"); got != "eyJ2IjoibWV0YSJ9" {
		t.Fatalf("NormalizeListContinue(real token) = %q, want unchanged", got)
	}
}

func TestListPageErrorPreservesExpiredContinue(t *testing.T) {
	expired := apierrors.NewResourceExpired("too old resource version")
	err := listPageError("tasks", expired)
	var fiberErr *fiber.Error
	if !errors.As(err, &fiberErr) || fiberErr.Code != fiber.StatusGone {
		t.Fatalf("expired continue error = %v, want HTTP 410", err)
	}
	bad := listPageError("tasks", apierrors.NewBadRequest("invalid continue token"))
	if !errors.As(bad, &fiberErr) || fiberErr.Code != fiber.StatusBadRequest {
		t.Fatalf("malformed continue error = %v, want HTTP 400", bad)
	}
	other := listPageError("tasks", errors.New("boom"))
	if !errors.As(other, &fiberErr) || fiberErr.Code != fiber.StatusInternalServerError {
		t.Fatalf("generic list error = %v, want HTTP 500", other)
	}
}
