package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/store"
)

const (
	testGatewayBindingUID = "binding-uid"
	testChangedEnvelope   = "changed envelope"
	testDifferentIdentity = "-other"
)

func testGatewayEvent(now time.Time, suffix string) store.GatewayEvent {
	return store.GatewayEvent{
		ID:                "gev-" + suffix,
		Namespace:         "default",
		NamespaceUID:      "namespace-uid",
		GatewayUID:        "gateway-uid",
		GatewayGeneration: 1,
		GatewayName:       "chat",
		BindingName:       "room",
		ExternalEventID:   "external-" + suffix,
		ProtocolVersion:   "orka.gateway.v1",
		EventType:         "text",
		AccountID:         "acct",
		ContextID:         "context",
		SenderID:          "sender",
		Text:              "hello " + suffix,
		ReplyTarget:       "context",
		SessionName:       "gateway-session",
		TaskName:          "gateway-task-" + suffix,
		ReceivedAt:        now,
		NextAttemptAt:     now,
		ExpiresAt:         now.Add(24 * time.Hour),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

func attachDeliveredExpiryDelivery(
	t *testing.T, s *Store, ctx context.Context, event store.GatewayEvent, completedAt time.Time,
) {
	t.Helper()
	deliveredAt := completedAt
	deliveryID := "gdl-" + event.ID
	if _, _, err := s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID, Reason: "expired", CompletedAt: completedAt,
		Delivery: store.GatewayDelivery{
			ID: deliveryID, IdempotencyID: deliveryID, Namespace: event.Namespace,
			NamespaceUID: event.NamespaceUID, GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
			GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
			SessionName: event.SessionName, Kind: "error", State: store.GatewayDeliveryDelivered,
			AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
			Text: "expired", MaxAttempts: 10, NextAttemptAt: completedAt, ExpiresAt: completedAt.Add(time.Hour),
			ProviderMessageID: "provider-" + event.ID, CreatedAt: completedAt, UpdatedAt: completedAt, DeliveredAt: &deliveredAt,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayEventAdmissionDeduplicatesTranscript(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "one")
	event.ThreadID = "thread-1"
	event.SenderDisplayName = "Sender One"
	event.Metadata = map[string]string{"fixture": "value"}

	got, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	})
	if err != nil || !created || got.State != store.GatewayEventQueued {
		t.Fatalf("AdmitGatewayEvent() = (%+v, %v, %v)", got, created, err)
	}
	duplicate, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	})
	if err != nil || created || duplicate.ID != event.ID {
		t.Fatalf("duplicate AdmitGatewayEvent() = (%+v, %v, %v)", duplicate, created, err)
	}
	session, err := s.GetSession(ctx, event.Namespace, event.SessionName)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if len(session.Messages) != 1 || session.Messages[0].ID != "gateway:"+event.ID+":user" {
		t.Fatalf("session messages = %#v, want one stable gateway message", session.Messages)
	}
	metadata := session.Messages[0].Metadata
	for key, want := range map[string]string{
		"fixture": "value", "gateway": event.GatewayName, "binding": event.BindingName,
		"externalEventId": event.ExternalEventID, "accountId": event.AccountID, "contextId": event.ContextID,
		"threadId": event.ThreadID, "senderId": event.SenderID, "senderDisplayName": event.SenderDisplayName,
		gatewayEnvelopeDigestMetadataKey: store.GatewayEventEnvelopeDigest(&event),
	} {
		if got := metadata[key]; got != want {
			t.Errorf("session message metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestGatewayBackgroundClaimsAndMaintenanceRespectNamespace(t *testing.T) {
	const (
		teamB    = "team-b"
		sessionB = "session-b"
	)
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	eventA := testGatewayEvent(now, "scope-a")
	eventA.Namespace = "team-a"
	eventA.SessionName = "session-a"
	eventB := testGatewayEvent(now.Add(time.Second), "scope-b")
	eventB.Namespace = teamB
	eventB.SessionName = sessionB
	for _, event := range []store.GatewayEvent{eventA, eventB} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	claimedEvent, err := s.ClaimNextGatewayEvent(ctx, teamB, "worker", now.Add(2*time.Second), time.Minute)
	if err != nil || claimedEvent.Namespace != teamB || claimedEvent.ID != eventB.ID {
		t.Fatalf("scoped event claim = (%+v, %v)", claimedEvent, err)
	}

	makeDelivery := func(namespace, id string, expiresAt time.Time) *store.GatewayDelivery {
		return &store.GatewayDelivery{
			ID: id, IdempotencyID: id, Namespace: namespace, NamespaceUID: namespace + "-uid", GatewayUID: "uid", GatewayGeneration: 1, GatewayName: "chat",
			EventID: "event-" + id, Kind: "final", AccountID: "acct", ContextID: "room",
			ReplyTarget: "room", Text: id, MaxAttempts: 10, NextAttemptAt: now,
			ExpiresAt: expiresAt, CreatedAt: now, UpdatedAt: now,
		}
	}
	deliveryA := makeDelivery("team-a", "delivery-a", now.Add(time.Hour))
	deliveryB := makeDelivery(teamB, "delivery-b", now.Add(time.Hour))
	for _, delivery := range []*store.GatewayDelivery{deliveryA, deliveryB} {
		if _, _, err := s.CreateGatewayDelivery(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}
	claimedDelivery, err := s.ClaimNextGatewayDelivery(ctx, teamB, "worker", now, time.Minute)
	if err != nil || claimedDelivery.Namespace != teamB || claimedDelivery.ID != deliveryB.ID {
		t.Fatalf("scoped delivery claim = (%+v, %v)", claimedDelivery, err)
	}

	expiredA := makeDelivery("team-a", "expired-a", now.Add(-time.Second))
	expiredB := makeDelivery(teamB, "expired-b", now.Add(-time.Second))
	for _, delivery := range []*store.GatewayDelivery{expiredA, expiredB} {
		if _, _, err := s.CreateGatewayDelivery(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}
	expiredEventA := testGatewayEvent(now.Add(-time.Hour), "expired-scope-a")
	expiredEventA.Namespace = "team-a"
	expiredEventA.SessionName = "expired-session-a"
	expiredEventA.ExpiresAt = now.Add(-time.Second)
	expiredEventB := testGatewayEvent(now.Add(-time.Hour), "expired-scope-b")
	expiredEventB.Namespace = teamB
	expiredEventB.SessionName = "expired-session-b"
	expiredEventB.ExpiresAt = now.Add(-time.Second)
	for _, event := range []store.GatewayEvent{expiredEventA, expiredEventB} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := s.MaintainGatewayRecords(ctx, "team-a", now, now.Add(-time.Hour))
	if err != nil || result.ExpiredEvents != 0 || result.ExpiredDeliveries != 1 {
		t.Fatalf("scoped maintenance = (%+v, %v)", result, err)
	}
	storedEventA, err := s.GetGatewayEvent(ctx, "team-a", expiredEventA.ID)
	if err != nil || storedEventA.State != store.GatewayEventQueued {
		t.Fatalf("team-a active event was mutated by maintenance = (%+v, %v)", storedEventA, err)
	}
	storedEventB, err := s.GetGatewayEvent(ctx, teamB, expiredEventB.ID)
	if err != nil || storedEventB.State != store.GatewayEventQueued {
		t.Fatalf("team-b event was mutated by team-a maintenance = (%+v, %v)", storedEventB, err)
	}
	storedA, err := s.GetGatewayDelivery(ctx, "team-a", expiredA.ID)
	if err != nil || storedA.State != store.GatewayDeliveryExpired {
		t.Fatalf("team-a expired delivery = (%+v, %v)", storedA, err)
	}
	storedB, err := s.GetGatewayDelivery(ctx, teamB, expiredB.ID)
	if err != nil || storedB.State != store.GatewayDeliveryPending {
		t.Fatalf("team-b delivery was mutated by team-a maintenance = (%+v, %v)", storedB, err)
	}
}

func TestGatewayEventQueueLimitDoesNotAppendMessage(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "first")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: first, AppendUserMessage: true, PendingLimit: 1}); err != nil {
		t.Fatal(err)
	}
	second := testGatewayEvent(now.Add(time.Second), "second")
	got, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: second, AppendUserMessage: true, PendingLimit: 1})
	if err != nil || !created || got.State != store.GatewayEventDeadLettered {
		t.Fatalf("second admission = (%+v, %v, %v)", got, created, err)
	}
	session, err := s.GetSession(ctx, first.Namespace, first.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Messages) != 1 {
		t.Fatalf("message count = %d, want 1", len(session.Messages))
	}
}

func TestGatewayQueueStatsParsesSQLiteAggregateTimestamps(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "queue-stats")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	delivery := &store.GatewayDelivery{
		ID: "gdl-queue-stats", IdempotencyID: "gdl-queue-stats", Namespace: event.Namespace,
		NamespaceUID: event.NamespaceUID, GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
		GatewayName: event.GatewayName, EventID: event.ID, Kind: "final", AccountID: event.AccountID,
		ContextID: event.ContextID, ReplyTarget: event.ReplyTarget, Text: "done", MaxAttempts: 10,
		NextAttemptAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := s.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GetGatewayQueueStats(ctx, event.Namespace)
	if err != nil {
		t.Fatalf("GetGatewayQueueStats() error = %v", err)
	}
	if stats.PendingEvents != 1 || stats.PendingDeliveries != 1 {
		t.Fatalf("queue stats = %+v", stats)
	}
	if stats.OldestEventReceived == nil || !stats.OldestEventReceived.Equal(event.ReceivedAt) {
		t.Fatalf("oldest event = %v, want %v", stats.OldestEventReceived, event.ReceivedAt)
	}
	if stats.OldestDeliveryDue == nil || !stats.OldestDeliveryDue.Equal(delivery.NextAttemptAt) {
		t.Fatalf("oldest delivery = %v, want %v", stats.OldestDeliveryDue, delivery.NextAttemptAt)
	}
}

func TestGatewayDispatchProjectionAndDeliveryLifecycle(t *testing.T) {
	const providerMessageID = "provider-message"

	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "lifecycle")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimNextGatewayEvent(ctx, "", "worker-a", now, time.Minute)
	if err != nil || claimed.ID != event.ID || claimed.State != store.GatewayEventDispatching {
		t.Fatalf("ClaimNextGatewayEvent() = (%+v, %v)", claimed, err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "worker-b", now, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second claim error = %v, want ErrNotFound", err)
	}
	if err := s.MarkGatewayEventTaskCreated(ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "worker-a", now); err != nil {
		t.Fatal(err)
	}

	delivery := store.GatewayDelivery{
		ID: "gdl-lifecycle", IdempotencyID: "gdl-lifecycle", Namespace: event.Namespace,
		NamespaceUID: event.NamespaceUID,
		GatewayUID:   event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
		GatewayName: event.GatewayName, BindingName: event.BindingName,
		EventID: event.ID, TaskName: event.TaskName, SessionName: event.SessionName,
		Kind: "final", AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
		Text: "done", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: event.ExpiresAt,
		CreatedAt: now, UpdatedAt: now,
	}
	projected, created, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
		EventID: event.ID,
		Message: store.SessionMessage{
			ID: "gateway:" + event.ID + ":assistant", Role: "assistant", Content: "done",
			SourceType: "gateway-task", SourceRef: event.TaskName, Timestamp: now,
		},
		Delivery: delivery, CompletedAt: now,
	})
	if err != nil || !created || projected.ID != delivery.ID {
		t.Fatalf("ProjectGatewayTerminal() = (%+v, %v, %v)", projected, created, err)
	}
	if _, created, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
		EventID:  event.ID,
		Message:  store.SessionMessage{ID: "gateway:" + event.ID + ":assistant", Role: "assistant", Content: "done"},
		Delivery: delivery, CompletedAt: now,
	}); err != nil || created {
		t.Fatalf("duplicate projection = (created=%v, err=%v)", created, err)
	}

	session, err := s.GetSession(ctx, event.Namespace, event.SessionName)
	if err != nil {
		t.Fatal(err)
	}
	if session.ActiveTask != "" || len(session.Messages) != 2 {
		t.Fatalf("session after projection = %+v", session)
	}

	claimedDelivery, err := s.ClaimNextGatewayDelivery(ctx, "", "delivery-a", now, time.Minute)
	if err != nil || claimedDelivery.ID != delivery.ID || claimedDelivery.AttemptCount != 1 {
		t.Fatalf("ClaimNextGatewayDelivery() = (%+v, %v)", claimedDelivery, err)
	}
	if err := s.ScheduleGatewayDeliveryRetry(ctx, delivery.Namespace, delivery.ID, "delivery-a", "temporary", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayDelivery(ctx, "", "delivery-b", now, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("early retry claim error = %v", err)
	}
	claimedDelivery, err = s.ClaimNextGatewayDelivery(ctx, "", "delivery-b", now.Add(time.Second), time.Minute)
	if err != nil || claimedDelivery.AttemptCount != 2 {
		t.Fatalf("retry claim = (%+v, %v)", claimedDelivery, err)
	}
	if err := s.MarkGatewayDeliveryDelivered(ctx, delivery.Namespace, delivery.ID, "delivery-b", providerMessageID, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetGatewayDelivery(ctx, delivery.Namespace, delivery.ID)
	if err != nil || stored.State != store.GatewayDeliveryDelivered || stored.ProviderMessageID != providerMessageID {
		t.Fatalf("stored delivery = (%+v, %v)", stored, err)
	}
}

func TestFreezeGatewayEventTaskRuntimeAllowedToolsIsClaimFencedAndFirstWriteWins(t *testing.T) {
	for _, test := range []struct {
		name         string
		allowedTools []string
	}{
		{name: "registered tools", allowedTools: []string{"read_evidence"}},
		{name: "explicit deny all", allowedTools: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			event := testGatewayEvent(now, "freeze-policy-"+strings.ReplaceAll(test.name, " ", "-"))
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: event, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.FreezeGatewayEventTaskRuntimeAllowedTools(
				ctx, event.Namespace, event.ID, "owner-a", test.allowedTools, now,
			); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("unclaimed freeze error = %v, want ErrConflict", err)
			}
			if _, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "owner-a", now, time.Minute); err != nil {
				t.Fatal(err)
			}
			frozen, err := s.FreezeGatewayEventTaskRuntimeAllowedTools(
				ctx, event.Namespace, event.ID, "owner-a", test.allowedTools, now,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !frozen.TaskPolicyFrozen || frozen.TaskAllowedTools == nil ||
				!slices.Equal(frozen.TaskAllowedTools, test.allowedTools) {
				t.Fatalf("frozen allowedTools = %#v, frozen=%v", frozen.TaskAllowedTools, frozen.TaskPolicyFrozen)
			}
			if _, err := s.FreezeGatewayEventTaskRuntimeAllowedTools(
				ctx, event.Namespace, event.ID, "owner-a", test.allowedTools, now.Add(time.Second),
			); err != nil {
				t.Fatalf("idempotent freeze error = %v", err)
			}
			if _, err := s.FreezeGatewayEventTaskRuntimeAllowedTools(
				ctx, event.Namespace, event.ID, "owner-a", []string{"different_tool"}, now.Add(2*time.Second),
			); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("overwrite error = %v, want ErrConflict", err)
			}
			if _, err := s.FreezeGatewayEventTaskRuntimeAllowedTools(
				ctx, event.Namespace, event.ID, "owner-b", test.allowedTools, now.Add(2*time.Second),
			); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("stale-owner freeze error = %v, want ErrConflict", err)
			}
			stored, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !stored.TaskPolicyFrozen || stored.TaskAllowedTools == nil ||
				!slices.Equal(stored.TaskAllowedTools, test.allowedTools) {
				t.Fatalf("stored allowedTools = %#v, frozen=%v", stored.TaskAllowedTools, stored.TaskPolicyFrozen)
			}
		})
	}
}

func TestGatewayMaintenanceExpiresAndCleans(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now.Add(-25*time.Hour), "expired")
	event.ExpiresAt = now.Add(-time.Hour)
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "event expired", now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MaintainGatewayRecords(ctx, "", now, now.Add(-30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || got.State != store.GatewayEventExpired {
		t.Fatalf("expired event = (%+v, %v)", got, err)
	}
}

func TestExpiryProjectionRepairsLegacyExpiredEventWithoutDelivery(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "repair-expired")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	const reason = "The message expired before execution could start."
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", reason, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	delivery := store.GatewayDelivery{
		ID: "gdl-repair-expired", IdempotencyID: "gdl-repair-expired",
		Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
		GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
		GatewayName: event.GatewayName, BindingName: event.BindingName,
		EventID: event.ID, TaskName: event.TaskName, SessionName: event.SessionName,
		Kind: "error", State: store.GatewayDeliveryPending,
		AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
		Text: reason, MaxAttempts: 10, NextAttemptAt: now.Add(time.Second),
		ExpiresAt: now.Add(time.Hour), CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	stored, created, err := s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID, Reason: reason, Delivery: delivery, CompletedAt: now.Add(time.Second),
	})
	if err != nil || !created || stored.ID != delivery.ID {
		t.Fatalf("ExpireGatewayEventWithDelivery() = (%+v, %v, %v)", stored, created, err)
	}
	storedEvent, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || storedEvent.State != store.GatewayEventExpired || storedEvent.DeliveryID != delivery.ID {
		t.Fatalf("repaired event = (%+v, %v)", storedEvent, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE gateway_events SET delivery_id = '' WHERE namespace = ? AND id = ?`, event.Namespace, event.ID); err != nil {
		t.Fatal(err)
	}
	stored, created, err = s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID, Reason: reason, Delivery: delivery, CompletedAt: now.Add(2 * time.Second),
	})
	if err != nil || created || stored.ID != delivery.ID {
		t.Fatalf("existing delivery repair = (%+v, %v, %v)", stored, created, err)
	}
	storedEvent, err = s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || storedEvent.DeliveryID != delivery.ID {
		t.Fatalf("relinked event = (%+v, %v)", storedEvent, err)
	}
}

func TestMaintenancePreservesExpiredEventUntilDeliveryRepair(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	completedAt := now.Add(-31 * 24 * time.Hour)
	event := testGatewayEvent(completedAt.Add(-time.Hour), "preserve-unrepaired")
	event.ExpiresAt = completedAt
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", completedAt); err != nil {
		t.Fatal(err)
	}
	result, err := s.MaintainGatewayRecords(ctx, event.Namespace, now, now.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedEvents != 0 || result.UpsertedTombstones != 0 {
		t.Fatalf("maintenance removed unrepaired event: %+v", result)
	}
	stored, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || stored.State != store.GatewayEventExpired || stored.DeliveryID != "" {
		t.Fatalf("preserved event = (%+v, %v)", stored, err)
	}
}

func TestUnrepairableExpiredEventCanBeDeadLettered(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "unrepairable-expired")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkExpiredGatewayEventDeadLettered(ctx, event.Namespace, event.ID, "identity unavailable", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || stored.State != store.GatewayEventDeadLettered || stored.DeliveryID != "" {
		t.Fatalf("dead-lettered legacy event = (%+v, %v)", stored, err)
	}
}

func TestGatewayMaintenanceCompactsSessionHistoryIntoBoundedTombstone(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	completedAt := now.Add(-31 * 24 * time.Hour)
	event := testGatewayEvent(completedAt.Add(-time.Hour), "compacted")
	event.ExpiresAt = completedAt
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", completedAt); err != nil {
		t.Fatal(err)
	}
	deliveredAt := completedAt
	if _, _, err := s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: event.ID, Reason: "expired", CompletedAt: completedAt,
		Delivery: store.GatewayDelivery{
			ID: "gdl-compacted", IdempotencyID: "gdl-compacted", Namespace: event.Namespace,
			NamespaceUID: event.NamespaceUID, GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
			GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
			SessionName: event.SessionName, Kind: "error", State: store.GatewayDeliveryDelivered,
			AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
			Text: "expired", MaxAttempts: 10, NextAttemptAt: completedAt, ExpiresAt: completedAt.Add(time.Hour),
			ProviderMessageID: "provider-compacted", CreatedAt: completedAt, UpdatedAt: completedAt, DeliveredAt: &deliveredAt,
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := s.MaintainGatewayRecords(ctx, "", now, now.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedEvents != 1 || result.UpsertedTombstones != 1 ||
		result.DeletedSessionMessages != 2 || result.DeletedSessions != 1 {
		t.Fatalf("maintenance result = %+v", result)
	}
	if _, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetGatewayEvent() error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetSession(ctx, event.Namespace, event.SessionName); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetSession() error = %v, want ErrNotFound", err)
	}

	sameEnvelope := event
	sameEnvelope.ID += testDifferentIdentity
	sameEnvelope.TaskName += testDifferentIdentity
	duplicate, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: sameEnvelope, AppendUserMessage: true, PendingLimit: 100,
	})
	if err != nil || created || duplicate.ID != event.ID || duplicate.State != store.GatewayEventCompleted {
		t.Fatalf("tombstoned duplicate = (%+v, %v, %v)", duplicate, created, err)
	}
	changed := sameEnvelope
	changed.ID += testDifferentIdentity
	changed.TaskName += testDifferentIdentity
	changed.Text = testChangedEnvelope
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: changed, AppendUserMessage: true, PendingLimit: 100,
	}); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("changed tombstoned duplicate error = %v, want ErrDuplicateMismatch", err)
	}

	future := now.Add(31 * 24 * time.Hour)
	result, err = s.MaintainGatewayRecords(ctx, "", future, future.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedTombstones != 1 {
		t.Fatalf("expired tombstone maintenance result = %+v", result)
	}
	replayed := event
	replayed.ReceivedAt = future
	replayed.NextAttemptAt = future
	replayed.ExpiresAt = future.Add(24 * time.Hour)
	replayed.CreatedAt = future
	replayed.UpdatedAt = future
	if admitted, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: replayed, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil || !created || admitted.State != store.GatewayEventQueued {
		t.Fatalf("event after tombstone expiry = (%+v, %v, %v)", admitted, created, err)
	}
}

func TestGatewayClaimsRecoverAfterLeaseExpiry(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "lease")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "worker-a", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := s.ClaimNextGatewayEvent(ctx, "", "worker-b", now.Add(2*time.Minute), time.Minute)
	if err != nil || reclaimed.ClaimOwner != "worker-b" || reclaimed.AttemptCount != 2 {
		t.Fatalf("reclaimed event = (%+v, %v)", reclaimed, err)
	}
}

func TestGatewayBurstRemainsBounded(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for i := range 1000 {
		event := testGatewayEvent(now.Add(time.Duration(i)*time.Microsecond), fmt.Sprintf("burst-%04d", i))
		event.SessionName = fmt.Sprintf("session-%03d", i%100)
		if _, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1000,
		}); err != nil || !created {
			t.Fatalf("burst admission %d = (created=%v, err=%v)", i, created, err)
		}
	}

	// Provider retries may arrive out of order after a response loss. Replaying the
	// full burst in reverse must hit durable deduplication without growing either
	// the event ledger or canonical Session transcript.
	for i := 999; i >= 0; i-- {
		event := testGatewayEvent(now.Add(time.Duration(i)*time.Microsecond), fmt.Sprintf("burst-%04d", i))
		event.SessionName = fmt.Sprintf("session-%03d", i%100)
		got, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1000,
		})
		if err != nil || created || got.ID != event.ID {
			t.Fatalf("reverse duplicate %d = (%+v, created=%v, err=%v)", i, got, created, err)
		}
	}

	events, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1000 {
		t.Fatalf("events = %d, want 1000", len(events))
	}
	for i := range 100 {
		session, err := s.GetSession(ctx, "default", fmt.Sprintf("session-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if session.MessageCount != 10 || len(session.Messages) != 10 {
			t.Fatalf("session %d messages = (%d, %d), want (10, 10)", i, session.MessageCount, len(session.Messages))
		}
	}

	// Session reservation bounds immediately claimable work to one event per
	// active Session even though all 1,000 durable events are due.
	claimedSessions := make(map[string]struct{}, 100)
	for i := range 100 {
		claimed, err := s.ClaimNextGatewayEvent(ctx, "default", fmt.Sprintf("worker-%03d", i), now.Add(time.Second), time.Minute)
		if err != nil {
			t.Fatalf("claim %d error = %v", i, err)
		}
		if _, exists := claimedSessions[claimed.SessionName]; exists {
			t.Fatalf("Session %q was claimed more than once", claimed.SessionName)
		}
		claimedSessions[claimed.SessionName] = struct{}{}
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "default", "worker-overflow", now.Add(time.Second), time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("claim beyond active Session count error = %v, want ErrNotFound", err)
	}
	afterClaims, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	stateCounts := make(map[store.GatewayEventState]int)
	for _, event := range afterClaims {
		stateCounts[event.State]++
	}
	if stateCounts[store.GatewayEventDispatching] != 100 || stateCounts[store.GatewayEventQueued] != 900 {
		t.Fatalf("event state counts = %#v, want 100 Dispatching and 900 Queued", stateCounts)
	}
}

//nolint:gocyclo // disaster-recovery behavior is clearest as one end-to-end snapshot/restore scenario
func TestGatewayBackupRestoreResumesQueuedWorkWithoutReplayingTerminalDelivery(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "gateway-source.db")
	backupPath := filepath.Join(tempDir, "gateway-backup.db")
	restoredPath := filepath.Join(tempDir, "gateway-restored.db")
	db, err := NewDB(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(db, sourcePath)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	newDelivery := func(event store.GatewayEvent, id, text string) store.GatewayDelivery {
		return store.GatewayDelivery{
			ID: id, IdempotencyID: id, Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
			GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration, GatewayName: event.GatewayName,
			BindingName: event.BindingName, EventID: event.ID, TaskName: event.TaskName, SessionName: event.SessionName,
			Kind: "final", AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
			Text: text, MaxAttempts: 10, NextAttemptAt: event.ReceivedAt, ExpiresAt: event.ExpiresAt,
			CreatedAt: event.ReceivedAt, UpdatedAt: event.ReceivedAt,
		}
	}
	project := func(event store.GatewayEvent, taskUID string, delivery store.GatewayDelivery) {
		t.Helper()
		claimed, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatch-"+event.ID, event.ReceivedAt, time.Minute)
		if err != nil || claimed.ID != event.ID {
			t.Fatalf("claim %s = (%+v, %v)", event.ID, claimed, err)
		}
		if err := s.MarkGatewayEventTaskCreated(ctx, event.Namespace, event.ID, event.TaskName, taskUID, "dispatch-"+event.ID, event.ReceivedAt); err != nil {
			t.Fatal(err)
		}
		if _, created, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
			EventID: event.ID,
			Message: store.SessionMessage{
				ID: "gateway:" + event.ID + ":assistant", Role: "assistant", Content: delivery.Text,
				SourceType: "gateway-task", SourceRef: event.TaskName, Timestamp: event.ReceivedAt,
			},
			Delivery: delivery, CompletedAt: event.ReceivedAt,
		}); err != nil || !created {
			t.Fatalf("project %s = (created=%v, err=%v)", event.ID, created, err)
		}
	}

	deliveredEvent := testGatewayEvent(now, "restore-delivered")
	deliveredEvent.SessionName = "restore-delivered-session"
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: deliveredEvent, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	deliveredDelivery := newDelivery(deliveredEvent, "gdl-restore-delivered", "delivered response")
	project(deliveredEvent, "task-uid-delivered", deliveredDelivery)
	if _, err := s.ClaimNextGatewayDelivery(ctx, deliveredEvent.Namespace, "delivery-worker", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGatewayDeliveryDelivered(ctx, deliveredEvent.Namespace, deliveredDelivery.ID, "delivery-worker", "provider-message", now); err != nil {
		t.Fatal(err)
	}

	pendingEvent := testGatewayEvent(now.Add(time.Second), "restore-pending")
	pendingEvent.SessionName = "restore-pending-session"
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: pendingEvent, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	pendingDelivery := newDelivery(pendingEvent, "gdl-restore-pending", "pending response")
	project(pendingEvent, "task-uid-pending", pendingDelivery)

	queuedEvent := testGatewayEvent(now.Add(2*time.Second), "restore-queued")
	queuedEvent.SessionName = "restore-queued-session"
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: queuedEvent, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}

	queuedBefore, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{
		Namespace: "default", States: []store.GatewayEventState{store.GatewayEventQueued}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	pendingBefore, err := s.ListGatewayDeliveries(ctx, store.GatewayDeliveryFilter{
		Namespace: "default", States: []store.GatewayDeliveryState{store.GatewayDeliveryPending}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(queuedBefore) != 1 || len(pendingBefore) != 1 {
		t.Fatalf("pre-backup queues = (%d events, %d deliveries), want (1, 1)", len(queuedBefore), len(pendingBefore))
	}

	// A quiesced backup may copy only the main database after checkpointing WAL.
	// Copy through a separate backup path so the restore exercises a new file.
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backupPath, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err = os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(restoredPath, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}

	restoredDB, err := NewDB(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restoredDB.Close() })
	restored := NewStore(restoredDB, restoredPath)

	restoredDeliveredEvent, err := restored.GetGatewayEvent(ctx, deliveredEvent.Namespace, deliveredEvent.ID)
	if err != nil || restoredDeliveredEvent.State != store.GatewayEventCompleted ||
		restoredDeliveredEvent.TaskName != deliveredEvent.TaskName || restoredDeliveredEvent.TaskUID != "task-uid-delivered" {
		t.Fatalf("restored delivered event = (%+v, %v)", restoredDeliveredEvent, err)
	}
	restoredPendingEvent, err := restored.GetGatewayEvent(ctx, pendingEvent.Namespace, pendingEvent.ID)
	if err != nil || restoredPendingEvent.State != store.GatewayEventCompleted || restoredPendingEvent.TaskUID != "task-uid-pending" {
		t.Fatalf("restored pending event = (%+v, %v)", restoredPendingEvent, err)
	}
	restoredQueuedEvent, err := restored.GetGatewayEvent(ctx, queuedEvent.Namespace, queuedEvent.ID)
	if err != nil || restoredQueuedEvent.State != store.GatewayEventQueued || restoredQueuedEvent.TaskName != queuedEvent.TaskName {
		t.Fatalf("restored queued event = (%+v, %v)", restoredQueuedEvent, err)
	}
	for sessionName, wantMessages := range map[string]int{
		deliveredEvent.SessionName: 2,
		pendingEvent.SessionName:   2,
		queuedEvent.SessionName:    1,
	} {
		session, err := restored.GetSession(ctx, "default", sessionName)
		if err != nil || len(session.Messages) != wantMessages {
			t.Fatalf("restored Session %q = (%+v, %v), want %d messages", sessionName, session, err, wantMessages)
		}
	}

	restoredDelivered, err := restored.GetGatewayDelivery(ctx, "default", deliveredDelivery.ID)
	if err != nil || restoredDelivered.State != store.GatewayDeliveryDelivered ||
		restoredDelivered.ProviderMessageID != "provider-message" || restoredDelivered.TaskName != deliveredEvent.TaskName {
		t.Fatalf("restored delivered delivery = (%+v, %v)", restoredDelivered, err)
	}
	restoredPending, err := restored.GetGatewayDelivery(ctx, "default", pendingDelivery.ID)
	if err != nil || restoredPending.State != store.GatewayDeliveryPending || restoredPending.TaskName != pendingEvent.TaskName {
		t.Fatalf("restored pending delivery = (%+v, %v)", restoredPending, err)
	}

	// Re-projecting a terminal event after restore must preserve the delivered
	// outbox row rather than creating a fresh provider side effect.
	gotDelivery, created, err := restored.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
		EventID: deliveredEvent.ID,
		Message: store.SessionMessage{
			ID: "gateway:" + deliveredEvent.ID + ":assistant", Role: "assistant", Content: deliveredDelivery.Text,
			SourceType: "gateway-task", SourceRef: deliveredEvent.TaskName, Timestamp: now,
		},
		Delivery: deliveredDelivery, CompletedAt: now,
	})
	if err != nil || created || gotDelivery.ID != deliveredDelivery.ID || gotDelivery.State != store.GatewayDeliveryDelivered {
		t.Fatalf("duplicate restored projection = (%+v, created=%v, err=%v)", gotDelivery, created, err)
	}

	claimedEvent, err := restored.ClaimNextGatewayEvent(ctx, "default", "restored-dispatch", now.Add(3*time.Second), time.Minute)
	if err != nil || claimedEvent.ID != queuedEvent.ID || claimedEvent.TaskName != queuedEvent.TaskName {
		t.Fatalf("restored event claim = (%+v, %v)", claimedEvent, err)
	}
	claimedDelivery, err := restored.ClaimNextGatewayDelivery(ctx, "default", "restored-delivery", now.Add(3*time.Second), time.Minute)
	if err != nil || claimedDelivery.ID != pendingDelivery.ID || claimedDelivery.AttemptCount != 1 {
		t.Fatalf("restored delivery claim = (%+v, %v)", claimedDelivery, err)
	}
	if _, err := restored.ClaimNextGatewayDelivery(ctx, "default", "unexpected-replay", now.Add(3*time.Second), time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("terminal delivery replay claim error = %v, want ErrNotFound", err)
	}
	restoredDelivered, err = restored.GetGatewayDelivery(ctx, "default", deliveredDelivery.ID)
	if err != nil || restoredDelivered.State != store.GatewayDeliveryDelivered || restoredDelivered.ProviderMessageID != "provider-message" {
		t.Fatalf("delivered side effect changed after restore = (%+v, %v)", restoredDelivered, err)
	}
}

func TestGatewayMigrationBackfillsActiveTaskUID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active-task-uid.db")
	db, err := NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(db, path)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "active-task-uid")
	event.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatcher", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGatewayEventTaskCreated(
		ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "dispatcher", now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE sessions SET active_task_uid = ''
		WHERE namespace = ? AND name = ?`, event.Namespace, event.SessionName); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s = NewStore(db, path)
	session, err := s.GetSession(ctx, event.Namespace, event.SessionName)
	if err != nil || session.ActiveTask != claimed.TaskName || session.ActiveTaskUID != "task-uid" {
		t.Fatalf("migrated active Task identity = (%+v, %v)", session, err)
	}
}

func TestGatewayMigrationBackfillsLegacySessionMessageIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE sessions (
		namespace TEXT NOT NULL, name TEXT NOT NULL, session_type TEXT NOT NULL DEFAULT 'task',
		active_task TEXT NOT NULL DEFAULT '', message_count INTEGER NOT NULL DEFAULT 0,
		input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
		cancelled BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMP, updated_at TIMESTAMP,
		PRIMARY KEY(namespace, name));
		CREATE TABLE session_messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL, session_name TEXT NOT NULL,
		role TEXT NOT NULL, content TEXT NOT NULL DEFAULT '', name TEXT, input TEXT, tool_calls TEXT,
		tool_call_id TEXT, created_at TIMESTAMP);`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := db.Exec(`INSERT INTO sessions(namespace, name, message_count, created_at, updated_at) VALUES('default','legacy',1,?,?);
		INSERT INTO session_messages(namespace, session_name, role, content, created_at) VALUES('default','legacy','user','old message',?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	messages, err := NewStore(db, path).LoadTranscript(context.Background(), "default", "legacy", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ID != "legacy:1" || messages[0].Content != "old message" {
		t.Fatalf("migrated messages = %#v", messages)
	}
}

func TestGatewayStaleClaimOwnerCannotDowngradeWork(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "owner")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "owner-a", now, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "owner-b", now.Add(2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryGatewayEvent(ctx, event.Namespace, event.ID, "owner-a", "stale", now); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale RetryGatewayEvent() error = %v, want ErrConflict", err)
	}
	if err := s.MarkGatewayEventTaskCreated(ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "owner-b", now); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
	if err != nil || got.State != store.GatewayEventTaskCreated {
		t.Fatalf("event after stale owner = (%+v, %v)", got, err)
	}
}

func TestGatewayTranscriptUsesLogicalTurnOrderAndCutoffs(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "turn-1")
	second := testGatewayEvent(now.Add(time.Second), "turn-2")
	second.SessionName = first.SessionName
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: first, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: second, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	firstStored, err := s.GetGatewayEvent(ctx, first.Namespace, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondStored, err := s.GetGatewayEvent(ctx, second.Namespace, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstCutoff, err := s.LoadTranscriptThrough(ctx, first.Namespace, first.SessionName, "gateway:"+first.ID+":user", 50)
	if err != nil || len(firstCutoff) != 1 || firstCutoff[0].Content != first.Text {
		t.Fatalf("first cutoff = (%#v, %v)", firstCutoff, err)
	}
	if err := s.MarkGatewayEventTaskCreated(ctx, first.Namespace, first.ID, first.TaskName, "task-uid", "worker", now); err == nil {
		t.Fatal("MarkGatewayEventTaskCreated unexpectedly succeeded before claim")
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "worker", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGatewayEventTaskCreated(ctx, first.Namespace, first.ID, first.TaskName, "task-uid", "worker", now); err != nil {
		t.Fatal(err)
	}
	delivery := store.GatewayDelivery{
		ID: "gdl-turn-1", IdempotencyID: "gdl-turn-1", Namespace: first.Namespace,
		NamespaceUID: first.NamespaceUID,
		GatewayUID:   first.GatewayUID, GatewayGeneration: first.GatewayGeneration,
		GatewayName: first.GatewayName, BindingName: first.BindingName,
		EventID: first.ID, TaskName: first.TaskName, SessionName: first.SessionName,
		Kind: "final", AccountID: first.AccountID, ContextID: first.ContextID, ReplyTarget: first.ReplyTarget,
		Text: "assistant one", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: first.ExpiresAt,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
		EventID:  first.ID,
		Message:  store.SessionMessage{ID: "gateway:" + first.ID + ":assistant", Role: "assistant", Content: "assistant one"},
		Delivery: delivery, CompletedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	full, err := s.LoadTranscript(ctx, first.Namespace, first.SessionName, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != 3 || full[0].Content != first.Text || full[1].Content != "assistant one" || full[2].Content != second.Text {
		t.Fatalf("logical transcript = %#v", full)
	}
	secondCutoff, err := s.LoadTranscriptThrough(ctx, second.Namespace, second.SessionName, "gateway:"+second.ID+":user", 50)
	if err != nil || len(secondCutoff) != 3 || secondStored.TranscriptOrder <= firstStored.TranscriptOrder {
		t.Fatalf("second cutoff = (%#v, %v), orders %d/%d", secondCutoff, err, firstStored.TranscriptOrder, secondStored.TranscriptOrder)
	}
}

func TestGatewayRetryBackoffPreservesSessionFIFO(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "fifo-first")
	second := testGatewayEvent(now.Add(time.Second), "fifo-second")
	second.SessionName = first.SessionName
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: first, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: second, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayEvent(ctx, "", "worker", now, time.Minute)
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first claim = (%+v, %v)", claimed, err)
	}
	retryAt := now.Add(time.Hour)
	if err := s.RetryGatewayEvent(ctx, first.Namespace, first.ID, "worker", "temporary", retryAt); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "worker", now.Add(time.Minute), time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("later event overtook retrying head: %v", err)
	}
	claimed, err = s.ClaimNextGatewayEvent(ctx, "", "worker", retryAt, time.Minute)
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("post-backoff claim = (%+v, %v), want first event", claimed, err)
	}
}

func TestGatewayProjectionRotationDoesNotStarveLaterCandidates(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "projection-first")
	first.SessionName = "projection-session-first"
	second := testGatewayEvent(now.Add(time.Second), "projection-second")
	second.SessionName = "projection-session-second"
	for _, event := range []store.GatewayEvent{first, second} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
			t.Fatal(err)
		}
		claimed, err := s.ClaimNextGatewayEvent(ctx, "", "worker", event.ReceivedAt, time.Minute)
		if err != nil || claimed.ID != event.ID {
			t.Fatalf("claim %s = (%+v, %v)", event.ID, claimed, err)
		}
		if err := s.MarkGatewayEventTaskCreated(ctx, event.Namespace, event.ID, event.TaskName, "task-uid-"+event.ID, "worker", event.ReceivedAt); err != nil {
			t.Fatal(err)
		}
	}
	due := now.Add(time.Minute)
	candidates, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{
		States:    []store.GatewayEventState{store.GatewayEventTaskCreated},
		DueBefore: &due, OrderByNextAttempt: true, Limit: 1,
	})
	if err != nil || len(candidates) != 1 || candidates[0].ID != first.ID {
		t.Fatalf("first projection candidate = (%+v, %v)", candidates, err)
	}
	if err := s.DeferGatewayEventProjection(ctx, first.Namespace, first.ID, due.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	candidates, err = s.ListGatewayEvents(ctx, store.GatewayEventFilter{
		States:    []store.GatewayEventState{store.GatewayEventTaskCreated},
		DueBefore: &due, OrderByNextAttempt: true, Limit: 1,
	})
	if err != nil || len(candidates) != 1 || candidates[0].ID != second.ID {
		t.Fatalf("rotated projection candidate = (%+v, %v)", candidates, err)
	}
}

func TestGatewayDeliveryClaimUsesTranscriptOrderWithinSession(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "delivery-order-first")
	second := testGatewayEvent(now.Add(time.Second), "delivery-order-second")
	second.SessionName = first.SessionName
	for _, event := range []store.GatewayEvent{first, second} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	makeDelivery := func(event store.GatewayEvent, id string, createdAt time.Time) *store.GatewayDelivery {
		return &store.GatewayDelivery{
			ID: id, IdempotencyID: id, Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
			GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
			GatewayName: event.GatewayName, BindingName: event.BindingName, EventID: event.ID,
			SessionName: event.SessionName, Kind: "error", State: store.GatewayDeliveryPending,
			AccountID: event.AccountID, ContextID: event.ContextID, ReplyTarget: event.ReplyTarget,
			Text: "expired", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}
	}
	firstDelivery := makeDelivery(first, "gdl-delivery-order-first", now.Add(2*time.Second))
	secondDelivery := makeDelivery(second, "gdl-delivery-order-second", now.Add(time.Second))
	for _, delivery := range []*store.GatewayDelivery{secondDelivery, firstDelivery} {
		if _, _, err := s.CreateGatewayDelivery(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := s.ClaimNextGatewayDelivery(ctx, "default", "worker", now.Add(3*time.Second), time.Minute)
	if err != nil || claimed.ID != firstDelivery.ID {
		t.Fatalf("first delivery claim = (%+v, %v), want %s", claimed, err, firstDelivery.ID)
	}
}

func TestGatewayDeliveryClaimWaitsForEarlierExpiredDeliveryRepair(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "missing-delivery-first")
	second := testGatewayEvent(now.Add(time.Second), "missing-delivery-second")
	second.SessionName = first.SessionName
	for _, event := range []store.GatewayEvent{first, second} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ExpireGatewayEvent(ctx, first.Namespace, first.ID, "", "legacy expiry", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	secondDelivery := &store.GatewayDelivery{
		ID: "gdl-missing-delivery-second", IdempotencyID: "gdl-missing-delivery-second",
		Namespace: second.Namespace, NamespaceUID: second.NamespaceUID,
		GatewayUID: second.GatewayUID, GatewayGeneration: second.GatewayGeneration,
		GatewayName: second.GatewayName, BindingName: second.BindingName, EventID: second.ID,
		SessionName: second.SessionName, Kind: "final", State: store.GatewayDeliveryPending,
		AccountID: second.AccountID, ContextID: second.ContextID, ReplyTarget: second.ReplyTarget,
		Text: "later", MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now.Add(3 * time.Second), UpdatedAt: now.Add(3 * time.Second),
	}
	if _, _, err := s.CreateGatewayDelivery(ctx, secondDelivery); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayDelivery(ctx, "default", "worker", now.Add(4*time.Second), time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("later delivery bypassed missing predecessor: %v", err)
	}
	firstDelivery := *secondDelivery
	firstDelivery.ID = "gdl-missing-delivery-first"
	firstDelivery.IdempotencyID = firstDelivery.ID
	firstDelivery.EventID = first.ID
	firstDelivery.Text = "earlier expiry"
	firstDelivery.CreatedAt = now.Add(2 * time.Second)
	firstDelivery.UpdatedAt = firstDelivery.CreatedAt
	if _, _, err := s.ExpireGatewayEventWithDelivery(ctx, store.GatewayExpiryProjection{
		EventID: first.ID, Reason: "legacy expiry", Delivery: firstDelivery, CompletedAt: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayDelivery(ctx, "default", "worker", now.Add(4*time.Second), time.Minute)
	if err != nil || claimed.ID != firstDelivery.ID {
		t.Fatalf("first claim after repair = (%+v, %v)", claimed, err)
	}
}

func TestGatewayExpiryListingProcessesOldestDeadlineFirst(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	oldest := testGatewayEvent(now, "expiry-oldest")
	oldest.SessionName = "expiry-session-oldest"
	oldest.ExpiresAt = now.Add(time.Minute)
	middle := testGatewayEvent(now.Add(time.Second), "expiry-middle")
	middle.SessionName = "expiry-session-middle"
	middle.ExpiresAt = now.Add(2 * time.Minute)
	newest := testGatewayEvent(now.Add(2*time.Second), "expiry-newest")
	newest.SessionName = "expiry-session-newest"
	newest.ExpiresAt = now.Add(3 * time.Minute)
	for _, event := range []store.GatewayEvent{newest, oldest, middle} {
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := now.Add(4 * time.Minute)
	listed, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{
		States: []store.GatewayEventState{store.GatewayEventQueued}, ExpiresBefore: &cutoff,
		OrderByExpiry: true, Limit: 2,
	})
	if err != nil || len(listed) != 2 || listed[0].ID != oldest.ID || listed[1].ID != middle.ID {
		t.Fatalf("expiry order = (%#v, %v)", listed, err)
	}
}

func TestGatewayDispatchUsesCommittedTranscriptOrder(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	committedFirst := testGatewayEvent(now.Add(time.Second), "committed-first")
	committedSecond := testGatewayEvent(now, "committed-second")
	committedSecond.SessionName = committedFirst.SessionName
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: committedFirst, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: committedSecond, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayEvent(ctx, "", "worker", now.Add(2*time.Second), time.Minute)
	if err != nil || claimed.ID != committedFirst.ID {
		t.Fatalf("claim = (%+v, %v), want lower committed transcript order", claimed, err)
	}
}

func TestGatewayLedgerCursorTraversesOlderEvents(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for i := range 3 {
		event := testGatewayEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("cursor-%d", i))
		event.SessionName = fmt.Sprintf("cursor-session-%d", i)
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
			t.Fatal(err)
		}
	}
	firstPage, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default", Limit: 2})
	if err != nil || len(firstPage) != 2 {
		t.Fatalf("first page = (%+v, %v)", firstPage, err)
	}
	last := firstPage[len(firstPage)-1]
	secondPage, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{
		Namespace: "default", BeforeCreatedAt: &last.CreatedAt, BeforeID: last.ID, Limit: 2,
	})
	if err != nil || len(secondPage) != 1 {
		t.Fatalf("second page = (%+v, %v)", secondPage, err)
	}
	firstPageIDs := make(map[string]struct{}, len(firstPage))
	for _, event := range firstPage {
		firstPageIDs[event.ID] = struct{}{}
	}
	for _, event := range secondPage {
		if _, overlaps := firstPageIDs[event.ID]; overlaps {
			t.Errorf("second page event %q overlaps first page", event.ID)
		}
		if event.CreatedAt.After(last.CreatedAt) ||
			(event.CreatedAt.Equal(last.CreatedAt) && event.ID >= last.ID) {
			t.Errorf("second page event = (%s, %s), want older than cursor (%s, %s)",
				event.CreatedAt, event.ID, last.CreatedAt, last.ID)
		}
	}
}

func TestGatewayCanonicalMessageActsAsDedupeTombstone(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "retained-dedupe")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM gateway_events WHERE namespace = ? AND id = ?`, event.Namespace, event.ID); err != nil {
		t.Fatal(err)
	}
	retained, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100})
	if err != nil || created || retained.State != store.GatewayEventCompleted || retained.TranscriptOrder == 0 {
		t.Fatalf("retained admission = (%+v, created=%v, err=%v)", retained, created, err)
	}
	session, err := s.GetSession(ctx, event.Namespace, event.SessionName)
	if err != nil || len(session.Messages) != 1 {
		t.Fatalf("retained Session = (%+v, %v)", session, err)
	}
	changed := event
	changed.Text = "changed payload"
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: changed, AppendUserMessage: true, PendingLimit: 100,
	}); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("changed retained admission error = %v, want ErrDuplicateMismatch", err)
	}
}

func TestGatewayDeliveryRetryPreservesSessionOrder(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	makeDelivery := func(id string, created time.Time) *store.GatewayDelivery {
		return &store.GatewayDelivery{
			ID: id, IdempotencyID: id, Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "uid", GatewayGeneration: 1, GatewayName: "chat",
			EventID: "event-" + id, SessionName: "ordered-session", Kind: "final",
			AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: id,
			MaxAttempts: 10, NextAttemptAt: created, ExpiresAt: now.Add(time.Hour),
			CreatedAt: created, UpdatedAt: created,
		}
	}
	first := makeDelivery("delivery-first", now)
	second := makeDelivery("delivery-second", now.Add(time.Second))
	if _, _, err := s.CreateGatewayDelivery(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateGatewayDelivery(ctx, second); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayDelivery(ctx, "", "worker", now.Add(2*time.Second), time.Minute)
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first claim = (%+v, %v)", claimed, err)
	}
	retryAt := now.Add(10 * time.Minute)
	if err := s.ScheduleGatewayDeliveryRetry(ctx, first.Namespace, first.ID, "worker", "temporary", retryAt); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayDelivery(ctx, "", "worker", now.Add(3*time.Second), time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("later delivery overtook retrying predecessor: %v", err)
	}
	claimed, err = s.ClaimNextGatewayDelivery(ctx, "", "worker", retryAt, time.Minute)
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("retry claim = (%+v, %v)", claimed, err)
	}
	if err := s.MarkGatewayDeliveryDelivered(ctx, first.Namespace, first.ID, "worker", "provider-first", retryAt); err != nil {
		t.Fatal(err)
	}
	claimed, err = s.ClaimNextGatewayDelivery(ctx, "", "worker", retryAt, time.Minute)
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("second claim after predecessor terminal = (%+v, %v)", claimed, err)
	}
}

func TestGatewaySessionDeletionIsPermanentlyProtected(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "session-delete")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSession(ctx, event.Namespace, event.SessionName); !errors.Is(err, store.ErrConflict) || !errors.Is(err, store.ErrGatewayOwnedSession) {
		t.Fatalf("DeleteSession() error = %v, want ErrConflict and ErrGatewayOwnedSession", err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSession(ctx, event.Namespace, event.SessionName); !errors.Is(err, store.ErrGatewayOwnedSession) {
		t.Fatalf("DeleteSession() after terminal event error = %v, want ErrGatewayOwnedSession", err)
	}
	if _, err := s.GetSession(ctx, event.Namespace, event.SessionName); err != nil {
		t.Fatalf("GetSession() after rejected delete error = %v", err)
	}
}

func TestGatewayActiveDeliveryClaimSurvivesExpiryMaintenance(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	delivery := &store.GatewayDelivery{
		ID: "active-expiry", IdempotencyID: "active-expiry", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event", SessionName: "session",
		Kind: "final", AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: "reply",
		MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: now.Add(time.Second), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := s.CreateGatewayDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayDelivery(ctx, "", "worker", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MaintainGatewayRecords(ctx, "", now.Add(2*time.Second), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetGatewayDelivery(ctx, "default", delivery.ID)
	if err != nil || stored.State != store.GatewayDeliverySending {
		t.Fatalf("active delivery after maintenance = (%+v, %v)", stored, err)
	}
	if err := s.MarkGatewayDeliveryDelivered(ctx, "default", delivery.ID, "worker", "provider-id", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayEventClaimRenewalFailsAfterLeaseLoss(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "renew-claim")
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "owner-a", now, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayEvent(ctx, "", "owner-b", now.Add(2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewGatewayEventClaim(ctx, event.Namespace, event.ID, "owner-a", now.Add(2*time.Second), time.Minute); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale renewal error = %v, want ErrConflict", err)
	}
}

func TestGatewayAdmissionRejectsSessionOwnedByAnotherSource(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.CreateSession(ctx, &store.SessionRecord{
		Namespace: "default", Name: "shared-explicit", SessionType: "chat", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	event := testGatewayEvent(now, "session-owner")
	event.SessionName = "shared-explicit"
	event.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("AdmitGatewayEvent() error = %v, want ErrConflict", err)
	}
	session, err := s.GetSession(ctx, "default", "shared-explicit")
	if err != nil || len(session.Messages) != 0 {
		t.Fatalf("foreign Session was modified: (%+v, %v)", session, err)
	}
}

func TestGatewayAbandonedDeliveryCannotExceedMaxAttempts(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := &store.GatewayDelivery{
		ID: "max-attempt-first", IdempotencyID: "max-attempt-first", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "uid", GatewayGeneration: 1, GatewayName: "chat", EventID: "event-first", SessionName: "attempt-session",
		Kind: "final", AccountID: "acct", ContextID: "room", ReplyTarget: "room", Text: "first",
		MaxAttempts: 1, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	second := *first
	second.ID = "max-attempt-second"
	second.IdempotencyID = second.ID
	second.EventID = "event-second"
	second.Text = "second"
	second.CreatedAt = now.Add(time.Second)
	if _, _, err := s.CreateGatewayDelivery(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateGatewayDelivery(ctx, &second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNextGatewayDelivery(ctx, "", "crashed", now, time.Second); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextGatewayDelivery(ctx, "", "recovery", now.Add(2*time.Second), time.Minute)
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("recovery claim = (%+v, %v), want second delivery", claimed, err)
	}
	stored, err := s.GetGatewayDelivery(ctx, "default", first.ID)
	if err != nil || stored.State != store.GatewayDeliveryDeadLettered || stored.AttemptCount != 1 {
		t.Fatalf("exhausted delivery = (%+v, %v)", stored, err)
	}
}

func TestGatewayRecordLimitBoundsDurableGrowth(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	first := testGatewayEvent(now, "capacity-first")
	second := testGatewayEvent(now.Add(time.Second), "capacity-second")
	second.SessionName = "other-session"
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: first, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: second, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1,
	}); !errors.Is(err, store.ErrCapacity) {
		t.Fatalf("second admission error = %v, want ErrCapacity", err)
	}
	events, err := s.ListGatewayEvents(ctx, store.GatewayEventFilter{Namespace: "default"})
	if err != nil || len(events) != 1 {
		t.Fatalf("stored events = (%#v, %v)", events, err)
	}
}

func TestGatewayOwnedSessionRejectsGenericMutation(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	event := testGatewayEvent(now, "owned-session")
	event.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event, AppendUserMessage: true, PendingLimit: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendMessages(ctx, event.Namespace, event.SessionName, []store.SessionMessage{{Role: "user", Content: "foreign"}}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("AppendMessages() error = %v, want ErrConflict", err)
	}
	if err := s.AcquireLock(ctx, event.Namespace, event.SessionName, "foreign-task", "foreign-uid"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("AcquireLock(foreign) error = %v, want ErrValidation", err)
	}
	claimed, err := s.ClaimNextGatewayEvent(ctx, "", "dispatcher", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AcquireLock(ctx, event.Namespace, event.SessionName, claimed.TaskName, "task-uid"); !errors.Is(err, store.ErrNotReady) {
		t.Fatalf("AcquireLock(pre-link gateway task) error = %v, want ErrNotReady", err)
	}
	if err := s.MarkGatewayEventTaskCreated(
		ctx, event.Namespace, event.ID, claimed.TaskName, "task-uid", "dispatcher", now,
	); err != nil {
		t.Fatal(err)
	}
	if err := s.AcquireLock(ctx, event.Namespace, event.SessionName, claimed.TaskName, "task-uid"); err != nil {
		t.Fatalf("AcquireLock(linked gateway task) error = %v", err)
	}
}

func TestRejectedAuditQuotaDoesNotConsumeOperationalCapacity(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	rejected := testGatewayEvent(now, "quota-rejected")
	rejected.State = store.GatewayEventRejected
	rejected.SessionName = ""
	rejected.TaskName = ""
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: rejected, GatewayRecordLimit: 1, RejectedRecordLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	accepted := testGatewayEvent(now.Add(time.Second), "quota-accepted")
	accepted.SessionName = "accepted-session"
	accepted.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: accepted, AppendUserMessage: true, PendingLimit: 100,
		GatewayRecordLimit: 1, RejectedRecordLimit: 1,
	}); err != nil {
		t.Fatalf("accepted event was blocked by rejected audit quota: %v", err)
	}
}

func TestGatewayTombstoneDuplicateBypassesCapacityLimit(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	completedAt := now.Add(-31 * 24 * time.Hour)
	original := testGatewayEvent(completedAt.Add(-time.Hour), "tombstone-capacity")
	original.BindingUID = testGatewayBindingUID
	original.ExpiresAt = completedAt
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: original, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, original.Namespace, original.ID, "", "expired", completedAt); err != nil {
		t.Fatal(err)
	}
	attachDeliveredExpiryDelivery(t, s, ctx, original, completedAt)
	result, err := s.MaintainGatewayRecords(ctx, original.Namespace, now, now.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedEvents != 1 || result.UpsertedTombstones != 1 ||
		result.DeletedSessionMessages != 2 || result.DeletedSessions != 1 {
		t.Fatalf("maintenance result = %+v", result)
	}
	if _, err := s.GetGatewayEvent(ctx, original.Namespace, original.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("compacted event lookup error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetSession(ctx, original.Namespace, original.SessionName); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("compacted Session lookup error = %v, want ErrNotFound", err)
	}
	filler := testGatewayEvent(now.Add(time.Second), "tombstone-filler")
	filler.SessionName = "filler-session"
	filler.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: filler, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	replay := original
	replay.ReceivedAt = now.Add(2 * time.Second)
	replay.NextAttemptAt = replay.ReceivedAt
	replay.ExpiresAt = replay.ReceivedAt.Add(24 * time.Hour)
	replay.CreatedAt = replay.ReceivedAt
	replay.UpdatedAt = replay.ReceivedAt
	retained, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: replay, AppendUserMessage: true, PendingLimit: 100, GatewayRecordLimit: 1,
	})
	if err != nil || created || retained.State != store.GatewayEventCompleted {
		t.Fatalf("retained duplicate at capacity = (%+v, created=%v, err=%v)", retained, created, err)
	}
}

func TestProjectGatewayTerminalRejectsMismatchedDeliveryIdentity(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*store.GatewayDelivery)
	}{
		{name: "event ID", mutate: func(delivery *store.GatewayDelivery) { delivery.EventID += testDifferentIdentity }},
		{name: "namespace UID", mutate: func(delivery *store.GatewayDelivery) { delivery.NamespaceUID += testDifferentIdentity }},
		{name: "gateway UID", mutate: func(delivery *store.GatewayDelivery) { delivery.GatewayUID += testDifferentIdentity }},
		{name: "gateway generation", mutate: func(delivery *store.GatewayDelivery) { delivery.GatewayGeneration++ }},
		{name: "gateway name", mutate: func(delivery *store.GatewayDelivery) { delivery.GatewayName += testDifferentIdentity }},
		{name: "binding name", mutate: func(delivery *store.GatewayDelivery) { delivery.BindingName += testDifferentIdentity }},
		{name: "task name", mutate: func(delivery *store.GatewayDelivery) { delivery.TaskName += testDifferentIdentity }},
		{name: "session name", mutate: func(delivery *store.GatewayDelivery) { delivery.SessionName += testDifferentIdentity }},
		{name: "account ID", mutate: func(delivery *store.GatewayDelivery) { delivery.AccountID += testDifferentIdentity }},
		{name: "context ID", mutate: func(delivery *store.GatewayDelivery) { delivery.ContextID += testDifferentIdentity }},
		{name: "thread ID", mutate: func(delivery *store.GatewayDelivery) { delivery.ThreadID += testDifferentIdentity }},
		{name: "reply target", mutate: func(delivery *store.GatewayDelivery) { delivery.ReplyTarget += testDifferentIdentity }},
	}

	for i, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			event := testGatewayEvent(now, fmt.Sprintf("projection-identity-%d", i))
			event.BindingUID = testGatewayBindingUID
			event.ThreadID = "thread"
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: event, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatcher", now, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkGatewayEventTaskCreated(
				ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "dispatcher", now,
			); err != nil {
				t.Fatal(err)
			}

			delivery := store.GatewayDelivery{
				ID: "gdl-" + event.ID, IdempotencyID: "gdl-" + event.ID,
				Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
				GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
				GatewayName: event.GatewayName, BindingName: event.BindingName,
				EventID: event.ID, TaskName: event.TaskName, SessionName: event.SessionName,
				Kind: "final", AccountID: event.AccountID, ContextID: event.ContextID,
				ThreadID: event.ThreadID, ReplyTarget: event.ReplyTarget, Text: "done",
				MaxAttempts: 10, NextAttemptAt: now, ExpiresAt: event.ExpiresAt,
				CreatedAt: now, UpdatedAt: now,
			}
			tc.mutate(&delivery)
			if _, _, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
				EventID: event.ID,
				Message: store.SessionMessage{
					ID: "gateway:" + event.ID + ":assistant", Role: "assistant", Content: "done",
					SourceType: "gateway-task", SourceRef: event.TaskName,
				},
				Delivery: delivery, CompletedAt: now,
			}); err == nil {
				t.Fatal("ProjectGatewayTerminal() succeeded with mismatched immutable delivery identity")
			}

			stored, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != store.GatewayEventTaskCreated || stored.DeliveryID != "" {
				t.Fatalf("event after rejected projection = %+v", stored)
			}
			if _, err := s.GetGatewayDelivery(ctx, event.Namespace, delivery.ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetGatewayDelivery() error = %v, want ErrNotFound", err)
			}
			session, err := s.GetSession(ctx, event.Namespace, event.SessionName)
			if err != nil {
				t.Fatal(err)
			}
			if session.ActiveTask != event.TaskName || len(session.Messages) != 1 {
				t.Fatalf("session after rejected projection = %+v", session)
			}
		})
	}
}

func TestGatewayMaintenanceLeavesActiveEventsForServiceProjection(t *testing.T) {
	states := []struct {
		name        string
		taskCreated bool
		setup       func(*testing.T, *Store, context.Context, store.GatewayEvent, time.Time)
	}{
		{name: "queued"},
		{
			name: "dispatching",
			setup: func(t *testing.T, s *Store, ctx context.Context, event store.GatewayEvent, claimAt time.Time) {
				t.Helper()
				if _, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatcher", claimAt, time.Minute); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:        "task-created",
			taskCreated: true,
			setup: func(t *testing.T, s *Store, ctx context.Context, event store.GatewayEvent, claimAt time.Time) {
				t.Helper()
				if _, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatcher", claimAt, time.Minute); err != nil {
					t.Fatal(err)
				}
				if err := s.MarkGatewayEventTaskCreated(
					ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "dispatcher", claimAt,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for i, tc := range states {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			claimAt := now.Add(-2 * time.Hour)
			stale := testGatewayEvent(claimAt, fmt.Sprintf("maintenance-stale-%d", i))
			stale.BindingUID = testGatewayBindingUID
			stale.ExpiresAt = now.Add(-time.Hour)
			later := testGatewayEvent(now, fmt.Sprintf("maintenance-later-%d", i))
			later.BindingUID = stale.BindingUID
			later.SessionName = stale.SessionName
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: stale, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: later, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			if tc.setup != nil {
				tc.setup(t, s, ctx, stale, claimAt)
			}

			result, err := s.MaintainGatewayRecords(ctx, stale.Namespace, now, now.Add(-30*24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			wantState := store.GatewayEventQueued
			wantActiveTask := ""
			if tc.name == "dispatching" {
				wantState = store.GatewayEventDispatching
				wantActiveTask = stale.TaskName
			}
			if tc.taskCreated {
				wantState = store.GatewayEventTaskCreated
				wantActiveTask = stale.TaskName
			}
			if result.ExpiredEvents != 0 {
				t.Fatalf("ExpiredEvents = %d, want 0", result.ExpiredEvents)
			}
			stored, err := s.GetGatewayEvent(ctx, stale.Namespace, stale.ID)
			if err != nil || stored.State != wantState {
				t.Fatalf("stale event = (%+v, %v), want state %s", stored, err, wantState)
			}
			session, err := s.GetSession(ctx, stale.Namespace, stale.SessionName)
			if err != nil {
				t.Fatal(err)
			}
			if session.ActiveTask != wantActiveTask {
				t.Fatalf("active task after maintenance = %q, want %q", session.ActiveTask, wantActiveTask)
			}
			_ = later
		})
	}
}

func TestGatewayTranscriptReservesTerminalPositionsAcrossMultipleAdmissions(t *testing.T) {
	terminalCases := []struct {
		name       string
		terminalID func(store.GatewayEvent) string
		content    string
		project    func(*testing.T, *Store, context.Context, store.GatewayEvent, time.Time)
	}{
		{
			name:       "assistant",
			terminalID: func(event store.GatewayEvent) string { return "gateway:" + event.ID + ":assistant" },
			content:    "assistant one",
			project: func(t *testing.T, s *Store, ctx context.Context, event store.GatewayEvent, now time.Time) {
				t.Helper()
				if _, err := s.ClaimNextGatewayEvent(ctx, event.Namespace, "dispatcher", now, time.Minute); err != nil {
					t.Fatal(err)
				}
				if err := s.MarkGatewayEventTaskCreated(
					ctx, event.Namespace, event.ID, event.TaskName, "task-uid", "dispatcher", now,
				); err != nil {
					t.Fatal(err)
				}
				delivery := store.GatewayDelivery{
					ID: "gdl-" + event.ID, IdempotencyID: "gdl-" + event.ID,
					Namespace: event.Namespace, NamespaceUID: event.NamespaceUID,
					GatewayUID: event.GatewayUID, GatewayGeneration: event.GatewayGeneration,
					GatewayName: event.GatewayName, BindingName: event.BindingName,
					EventID: event.ID, TaskName: event.TaskName, SessionName: event.SessionName,
					Kind: "final", AccountID: event.AccountID, ContextID: event.ContextID,
					ReplyTarget: event.ReplyTarget, Text: "assistant one", MaxAttempts: 10,
					NextAttemptAt: now, ExpiresAt: event.ExpiresAt, CreatedAt: now, UpdatedAt: now,
				}
				if _, _, err := s.ProjectGatewayTerminal(ctx, store.GatewayTerminalProjection{
					EventID: event.ID,
					Message: store.SessionMessage{
						ID: "gateway:" + event.ID + ":assistant", Role: "assistant", Content: "assistant one",
						SourceType: "gateway-task", SourceRef: event.TaskName,
					},
					Delivery: delivery, CompletedAt: now,
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:       "error",
			terminalID: func(event store.GatewayEvent) string { return "gateway:" + event.ID + ":error" },
			content:    "event expired",
			project: func(t *testing.T, s *Store, ctx context.Context, event store.GatewayEvent, now time.Time) {
				t.Helper()
				if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "event expired", now); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for i, tc := range terminalCases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			first := testGatewayEvent(now, fmt.Sprintf("terminal-slot-first-%d", i))
			first.BindingUID = testGatewayBindingUID
			second := testGatewayEvent(now.Add(time.Second), fmt.Sprintf("terminal-slot-second-%d", i))
			second.BindingUID = first.BindingUID
			second.SessionName = first.SessionName
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: first, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: second, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			firstStored, err := s.GetGatewayEvent(ctx, first.Namespace, first.ID)
			if err != nil {
				t.Fatal(err)
			}
			secondStored, err := s.GetGatewayEvent(ctx, second.Namespace, second.ID)
			if err != nil {
				t.Fatal(err)
			}
			if firstStored.TranscriptOrder+1 >= secondStored.TranscriptOrder {
				t.Fatalf("terminal slot %d collides with second user order %d", firstStored.TranscriptOrder+1, secondStored.TranscriptOrder)
			}

			tc.project(t, s, ctx, first, now.Add(2*time.Second))
			messages, err := s.LoadTranscript(ctx, first.Namespace, first.SessionName, 0)
			if err != nil {
				t.Fatal(err)
			}
			wantIDs := []string{store.GatewayUserMessageID(first.ID), tc.terminalID(first), store.GatewayUserMessageID(second.ID)}
			wantContent := []string{first.Text, tc.content, second.Text}
			if len(messages) != len(wantIDs) {
				t.Fatalf("messages = %#v", messages)
			}
			for i := range wantIDs {
				if messages[i].ID != wantIDs[i] || messages[i].Content != wantContent[i] {
					t.Errorf("message[%d] = %+v, want ID %q content %q", i, messages[i], wantIDs[i], wantContent[i])
				}
			}
			if messages[0].Order+1 != messages[1].Order || messages[1].Order+1 != messages[2].Order {
				t.Fatalf("message orders = %d, %d, %d", messages[0].Order, messages[1].Order, messages[2].Order)
			}
		})
	}
}

func TestGatewayLiveDedupRejectsChangedEnvelope(t *testing.T) {
	t.Run("same event ID", func(t *testing.T) {
		s := setupTestStore(t)
		ctx := context.Background()
		now := time.Now().UTC().Truncate(time.Second)
		event := testGatewayEvent(now, "dedupe-same-id")
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
		changed := event
		changed.Text = testChangedEnvelope
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: changed, AppendUserMessage: true, PendingLimit: 100,
		}); !errors.Is(err, store.ErrDuplicateMismatch) {
			t.Fatalf("changed duplicate error = %v, want ErrDuplicateMismatch", err)
		}
	})

	t.Run("same external event ID", func(t *testing.T) {
		s := setupTestStore(t)
		ctx := context.Background()
		now := time.Now().UTC().Truncate(time.Second)
		event := testGatewayEvent(now, "dedupe-same-external")
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: event, AppendUserMessage: true, PendingLimit: 100,
		}); err != nil {
			t.Fatal(err)
		}
		changed := event
		changed.ID += testDifferentIdentity
		changed.Text = testChangedEnvelope
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
			Event: changed, AppendUserMessage: true, PendingLimit: 100,
		}); !errors.Is(err, store.ErrDuplicateMismatch) {
			t.Fatalf("changed duplicate error = %v, want ErrDuplicateMismatch", err)
		}
	})

	t.Run("concurrent admission race", func(t *testing.T) {
		s := setupTestStore(t)
		ctx := context.Background()
		now := time.Now().UTC().Truncate(time.Second)
		first := testGatewayEvent(now, "dedupe-race")
		second := first
		second.ID += testDifferentIdentity
		second.TaskName += testDifferentIdentity
		second.Text = testChangedEnvelope

		type outcome struct {
			created bool
			err     error
		}
		start := make(chan struct{})
		outcomes := make(chan outcome, 2)
		var wg sync.WaitGroup
		for _, event := range []store.GatewayEvent{first, second} {
			wg.Go(func() {
				<-start
				_, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
					Event: event, AppendUserMessage: true, PendingLimit: 100,
				})
				outcomes <- outcome{created: created, err: err}
			})
		}
		close(start)
		wg.Wait()
		close(outcomes)

		var created, mismatched int
		for result := range outcomes {
			switch {
			case result.err == nil && result.created:
				created++
			case errors.Is(result.err, store.ErrDuplicateMismatch):
				mismatched++
			default:
				t.Errorf("unexpected concurrent admission outcome = %+v", result)
			}
		}
		if created != 1 || mismatched != 1 {
			t.Fatalf("concurrent outcomes: created=%d mismatch=%d", created, mismatched)
		}
	})
}

func TestGatewayDedupUsesNormalizedMetadata(t *testing.T) {
	cases := []struct {
		name      string
		original  map[string]string
		duplicate map[string]string
	}{
		{name: "nil and empty", original: nil, duplicate: map[string]string{}},
		{name: "equivalent normalized values", original: map[string]string{"key": "value"}, duplicate: map[string]string{"key": "  value  "}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			event := testGatewayEvent(now, fmt.Sprintf("dedupe-metadata-%d", i))
			event.Metadata = tc.original
			if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: event, AppendUserMessage: true, PendingLimit: 100,
			}); err != nil {
				t.Fatal(err)
			}
			duplicate := event
			duplicate.Metadata = tc.duplicate
			got, created, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
				Event: duplicate, AppendUserMessage: true, PendingLimit: 100,
			})
			if err != nil || created || got.ID != event.ID {
				t.Fatalf("normalized duplicate = (%+v, created=%v, err=%v)", got, created, err)
			}
		})
	}
}

func TestGatewayDeliveryRejectsIdempotencyReuseForDifferentDelivery(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	original := &store.GatewayDelivery{
		ID: "gdl-idempotency-original", IdempotencyID: "stable-idempotency-key",
		Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1,
		GatewayName: "chat", BindingName: "room", EventID: "event-original", TaskName: "task-original",
		SessionName: "session-original", Kind: "final", AccountID: "acct", ContextID: "context",
		ReplyTarget: "context", Text: "original", MaxAttempts: 10, NextAttemptAt: now,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := s.CreateGatewayDelivery(ctx, original); err != nil || !created {
		t.Fatalf("original delivery = (created=%v, err=%v)", created, err)
	}
	different := *original
	different.ID = "gdl-idempotency-different"
	different.EventID = "event-different"
	different.TaskName = "task-different"
	different.SessionName = "session-different"
	different.Text = "different"
	if _, _, err := s.CreateGatewayDelivery(ctx, &different); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("reused idempotency key error = %v, want ErrDuplicateMismatch", err)
	}
	stored, err := s.GetGatewayDelivery(ctx, original.Namespace, original.ID)
	if err != nil || stored.EventID != original.EventID || stored.Text != original.Text {
		t.Fatalf("stored original delivery = (%+v, %v)", stored, err)
	}
	if _, err := s.GetGatewayDelivery(ctx, different.Namespace, different.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("different delivery lookup error = %v, want ErrNotFound", err)
	}
}

func TestMarkGatewayEventTaskCreatedRejectsInvalidIdentityWithoutStrandingSession(t *testing.T) {
	cases := []struct {
		name     string
		taskName func(store.GatewayEvent) string
		taskUID  string
	}{
		{name: "empty task name", taskName: func(store.GatewayEvent) string { return "" }, taskUID: "task-uid"},
		{name: "empty task UID", taskName: func(event store.GatewayEvent) string { return event.TaskName }},
		{name: "task name mismatch", taskName: func(event store.GatewayEvent) string { return event.TaskName + testDifferentIdentity }, taskUID: "task-uid"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Second)
			first := testGatewayEvent(now, fmt.Sprintf("task-identity-first-%d", i))
			first.BindingUID = testGatewayBindingUID
			later := testGatewayEvent(now.Add(time.Second), fmt.Sprintf("task-identity-later-%d", i))
			later.BindingUID = first.BindingUID
			later.SessionName = first.SessionName
			for _, event := range []store.GatewayEvent{first, later} {
				if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
					Event: event, AppendUserMessage: true, PendingLimit: 100,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ClaimNextGatewayEvent(ctx, first.Namespace, "dispatcher", now, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkGatewayEventTaskCreated(
				ctx, first.Namespace, first.ID, tc.taskName(first), tc.taskUID, "dispatcher", now,
			); err == nil {
				t.Error("MarkGatewayEventTaskCreated() succeeded with invalid task identity")
			}
			stored, err := s.GetGatewayEvent(ctx, first.Namespace, first.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != store.GatewayEventDispatching || stored.TaskName != first.TaskName || stored.TaskUID != "" {
				t.Fatalf("event after rejected task identity = %+v", stored)
			}
			session, err := s.GetSession(ctx, first.Namespace, first.SessionName)
			if err != nil {
				t.Fatal(err)
			}
			if session.ActiveTask != first.TaskName {
				t.Fatalf("active task after rejected task identity = %q, want %q", session.ActiveTask, first.TaskName)
			}
			if err := s.RetryGatewayEvent(ctx, first.Namespace, first.ID, "dispatcher", "retry", now); err != nil {
				t.Fatalf("RetryGatewayEvent() after rejected task identity = %v", err)
			}
			if err := s.ExpireGatewayEvent(ctx, first.Namespace, first.ID, "", "expired", now); err != nil {
				t.Fatalf("ExpireGatewayEvent() after retry = %v", err)
			}
			claimed, err := s.ClaimNextGatewayEvent(ctx, first.Namespace, "next-dispatcher", now.Add(time.Second), time.Minute)
			if err != nil || claimed.ID != later.ID {
				t.Fatalf("next claim = (%+v, %v), want %s", claimed, err, later.ID)
			}
		})
	}
}

func TestGatewayWritesRejectInvalidStates(t *testing.T) {
	t.Run("event", func(t *testing.T) {
		s := setupTestStore(t)
		ctx := context.Background()
		now := time.Now().UTC().Truncate(time.Second)
		event := testGatewayEvent(now, "invalid-state")
		event.State = store.GatewayEventState("Unknown")
		event.SessionName = ""
		event.TaskName = ""
		if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{Event: event}); !errors.Is(err, store.ErrValidation) {
			t.Fatalf("invalid event state error = %v, want ErrValidation", err)
		}
		if _, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("invalid event lookup error = %v, want ErrNotFound", err)
		}
	})

	t.Run("delivery", func(t *testing.T) {
		s := setupTestStore(t)
		ctx := context.Background()
		now := time.Now().UTC().Truncate(time.Second)
		delivery := &store.GatewayDelivery{
			ID: "gdl-invalid-state", IdempotencyID: "gdl-invalid-state",
			Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1,
			GatewayName: "chat", EventID: "event", Kind: "final", State: store.GatewayDeliveryState("Unknown"),
			AccountID: "acct", ContextID: "context", ReplyTarget: "context", Text: "done",
			NextAttemptAt: now, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
		}
		if _, _, err := s.CreateGatewayDelivery(ctx, delivery); !errors.Is(err, store.ErrValidation) {
			t.Fatalf("invalid delivery state error = %v, want ErrValidation", err)
		}
		if _, err := s.GetGatewayDelivery(ctx, delivery.Namespace, delivery.ID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("invalid delivery lookup error = %v, want ErrNotFound", err)
		}
	})
}

func TestGatewayRetentionDeletesOnlyExactCanonicalMessages(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	terminalAt := now.Add(-2 * time.Hour)
	event := testGatewayEvent(terminalAt.Add(-time.Minute), "retention-exact")
	event.ID = "gev-retention_%"
	event.ExternalEventID = "external-retention-exact"
	event.TaskName = "gateway-task-retention-exact"
	event.BindingUID = testGatewayBindingUID
	if _, _, err := s.AdmitGatewayEvent(ctx, store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireGatewayEvent(ctx, event.Namespace, event.ID, "", "expired", terminalAt); err != nil {
		t.Fatal(err)
	}
	attachDeliveredExpiryDelivery(t, s, ctx, event, terminalAt)

	prefixNeighbor := "gateway:" + event.ID + ":error-neighbor"
	likeNeighbor := "gateway:gev-retention-XX:user"
	sourceNeighbor := "message-with-neighbor-source"
	exactSource := "message-with-exact-source"
	for i, message := range []struct {
		id         string
		sourceType string
		sourceRef  string
	}{
		{id: prefixNeighbor, sourceType: "manual", sourceRef: event.ID + "-neighbor"},
		{id: likeNeighbor, sourceType: "manual", sourceRef: event.ID + "-neighbor"},
		{id: sourceNeighbor, sourceType: "gateway-event", sourceRef: event.ID + "-neighbor"},
		{id: exactSource, sourceType: "gateway-event", sourceRef: event.ID},
	} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO session_messages
			(namespace, session_name, message_id, sort_order, role, content, source_type, source_ref, metadata_json, created_at)
			VALUES (?, ?, ?, ?, 'assistant', ?, ?, ?, '{}', ?)`,
			event.Namespace, event.SessionName, message.id, 10+i, message.id,
			message.sourceType, message.sourceRef, terminalAt,
		); err != nil {
			t.Fatal(err)
		}
	}

	result, err := s.MaintainGatewayRecords(ctx, event.Namespace, now, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSessionMessages != 3 {
		t.Fatalf("DeletedSessionMessages = %d, want 3 exact canonical/source matches", result.DeletedSessionMessages)
	}
	if _, err := s.GetGatewayEvent(ctx, event.Namespace, event.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retained terminal event lookup error = %v, want ErrNotFound", err)
	}
	messages, err := s.LoadTranscript(ctx, event.Namespace, event.SessionName, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{prefixNeighbor: true, likeNeighbor: true, sourceNeighbor: true}
	if len(messages) != len(want) {
		t.Fatalf("retained messages = %#v", messages)
	}
	for _, message := range messages {
		if !want[message.ID] {
			t.Errorf("unexpected retained message %q", message.ID)
		}
		delete(want, message.ID)
	}
	if len(want) != 0 {
		t.Fatalf("missing retained messages = %#v", want)
	}
}
