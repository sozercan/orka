package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

func TestListSessionsPageCursorsInSQL(t *testing.T) {
	s := newCoexistenceTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, name := range []string{"a-chat", "b-gateway", "c-chat", "d-chat", "e-chat"} {
		kind := "chat"
		if name == "b-gateway" {
			kind = store.SessionTypeGateway
		}
		if err := s.CreateSession(ctx, &store.SessionRecord{Namespace: "ns", Name: name, SessionType: kind, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateSession(ctx, &store.SessionRecord{Namespace: "other", Name: "aa-chat", SessionType: "chat", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	page, more, err := s.ListSessionsPage(ctx, "ns", "", 2, store.SessionTypeGateway)
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(page) != 2 || page[0].Name != "a-chat" || page[1].Name != "c-chat" {
		t.Fatalf("first page = %+v more=%v", page, more)
	}
	page, more, err = s.ListSessionsPage(ctx, "ns", page[1].Name, 2, store.SessionTypeGateway)
	if err != nil {
		t.Fatal(err)
	}
	if more || len(page) != 2 || page[0].Name != "d-chat" || page[1].Name != "e-chat" {
		t.Fatalf("second page = %+v more=%v", page, more)
	}
	if page, _, err := s.ListSessionsPage(ctx, "ns", "", 10, ""); err != nil || len(page) != 5 {
		t.Fatalf("unfiltered page = %+v err=%v", page, err)
	}
}
