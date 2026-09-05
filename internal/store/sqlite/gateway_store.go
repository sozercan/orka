/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	executionevents "github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/redact"
	"github.com/orka-agents/orka/internal/store"
)

const gatewaySessionOwnerType = store.SessionTypeGateway
const gatewayEnvelopeDigestMetadataKey = "gatewayEnvelopeDigest"

const (
	gatewayDeliveryKindFinal = "final"
	gatewayDeliveryKindError = "error"
)

const gatewayEventColumns = `id, namespace, namespace_uid, gateway_uid, gateway_generation, gateway_name, binding_name, binding_uid, binding_generation, agent_name, agent_uid, external_event_id,
	protocol_version, event_type, state, state_message, account_id, context_id, thread_id, sender_id,
	sender_display_name, text, reply_target, metadata_json, session_name, task_name, task_uid, task_runtime_allowed_tools_json, delivery_id, provider_message_id, trace_parent, trace_state, transcript_order, attempt_count,
	claim_owner, claim_until, next_attempt_at, occurred_at, received_at, expires_at, created_at, updated_at, completed_at`

const gatewayListOrder = ` ORDER BY created_at DESC, id DESC`

const gatewayDeliveryColumns = `id, idempotency_id, namespace, namespace_uid, gateway_uid, gateway_generation, gateway_name, binding_name,
	event_id, task_name, session_name, kind, state, account_id, context_id, thread_id, reply_target,
	text, metadata_json, attempt_count, max_attempts, manual_retry_count, next_attempt_at, expires_at,
	provider_message_id, trace_parent, trace_state, last_error, claim_owner, claim_until, created_at, updated_at, delivered_at`

var (
	_ store.GatewayEventStore    = (*Store)(nil)
	_ store.GatewayDeliveryStore = (*Store)(nil)
)

// AdmitGatewayEvent atomically deduplicates a normalized event and, for accepted events,
// creates/upserts the Session and appends one stable user message.
func (s *Store) AdmitGatewayEvent(ctx context.Context, admission store.GatewayEventAdmission) (*store.GatewayEvent, bool, error) {
	event, appendUserMessage, metadataJSON, now, err := prepareGatewayEventAdmission(admission)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback() //nolint:errcheck

	if existing, err := getGatewayEventQuery(ctx, tx, event.Namespace, event.ID); err == nil {
		if !gatewayEventsHaveSameEnvelope(existing, &event) {
			return nil, false, store.ErrDuplicateMismatch
		}
		return existing, false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	if tombstone, err := getGatewayEventTombstoneQuery(
		ctx, tx, event.Namespace, event.GatewayUID, event.ExternalEventID, now,
	); err == nil {
		if tombstone.EnvelopeDigest != store.GatewayEventEnvelopeDigest(&event) {
			return nil, false, store.ErrDuplicateMismatch
		}
		event.ID = tombstone.EventID
		event.State = store.GatewayEventCompleted
		event.StateMessage = "event is retained by a deduplication tombstone"
		event.SessionName = tombstone.SessionName
		event.TranscriptOrder = tombstone.TranscriptOrder
		return &event, false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	retainedSession, retainedOrder, retained, err := retainedGatewayMessage(ctx, tx, &event)
	if err != nil {
		return nil, false, err
	}
	if retained {
		event.State = store.GatewayEventCompleted
		event.StateMessage = "event is retained in canonical session history"
		event.SessionName = retainedSession
		event.TranscriptOrder = retainedOrder
		return &event, false, nil
	}
	limit := admission.GatewayRecordLimit
	statePredicate := `state <> ?`
	stateArg := store.GatewayEventRejected
	if !admission.AppendUserMessage {
		limit = admission.RejectedRecordLimit
		statePredicate = `state = ?`
	}
	if limit > 0 {
		var stored int
		query := `SELECT COUNT(*) FROM gateway_events WHERE namespace = ? AND gateway_uid = ? AND ` + statePredicate
		if err := tx.QueryRowContext(ctx, query, event.Namespace, event.GatewayUID, stateArg).Scan(&stored); err != nil {
			return nil, false, err
		}
		if stored >= limit {
			return nil, false, store.ErrCapacity
		}
	}
	appendUserMessage, err = applyGatewayPendingLimit(
		ctx, tx, &event, appendUserMessage, admission.PendingLimit,
	)
	if err != nil {
		return nil, false, err
	}
	if appendUserMessage {
		event.TranscriptOrder, err = prepareGatewaySessionOrderTx(ctx, tx, &event, now)
		if err != nil {
			return nil, false, err
		}
	}
	created, existing, err := insertGatewayEventTx(ctx, tx, &event, metadataJSON)
	if err != nil {
		return nil, false, err
	}
	if !created {
		return existing, false, nil
	}
	if appendUserMessage {
		if err := appendGatewayUserMessageTx(ctx, tx, &event, now); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return &event, true, nil
}

func prepareGatewayEventAdmission(admission store.GatewayEventAdmission) (store.GatewayEvent, bool, string, time.Time, error) {
	event := admission.Event
	if err := validateGatewayEvent(&event); err != nil {
		return store.GatewayEvent{}, false, "", time.Time{}, err
	}
	now := event.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	event.CreatedAt = now
	if event.UpdatedAt.IsZero() {
		event.UpdatedAt = now
	}
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = now
	}
	if event.NextAttemptAt.IsZero() {
		event.NextAttemptAt = now
	}
	if event.ExpiresAt.IsZero() {
		return store.GatewayEvent{}, false, "", time.Time{}, store.ValidationErrorf("gateway event expiresAt is required")
	}
	appendUserMessage := admission.AppendUserMessage
	if appendUserMessage {
		event.State = store.GatewayEventQueued
		if event.SessionName == "" || event.TaskName == "" {
			return store.GatewayEvent{}, false, "", time.Time{}, store.ValidationErrorf(
				"accepted gateway event requires sessionName and taskName",
			)
		}
	} else if event.State == "" {
		event.State = store.GatewayEventRejected
	}
	if !store.IsValidGatewayEventState(event.State) {
		return store.GatewayEvent{}, false, "", time.Time{}, store.ValidationErrorf("unsupported gateway event state %q", event.State)
	}
	event.StateMessage = sanitizeGatewayStoreText(event.StateMessage, 1024)
	event.SenderDisplayName = sanitizeGatewayStoreText(event.SenderDisplayName, 256)
	event.Metadata = normalizeGatewayStoreMetadata(event.Metadata)
	metadataJSON, err := marshalStringMap(event.Metadata)
	if err != nil {
		return store.GatewayEvent{}, false, "", time.Time{}, err
	}
	return event, appendUserMessage, metadataJSON, now, nil
}

func normalizeGatewayStoreMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		normalized[key] = sanitizeGatewayStoreText(value, 256)
	}
	return normalized
}

func applyGatewayPendingLimit(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
	appendUserMessage bool,
	pendingLimit int,
) (bool, error) {
	if !appendUserMessage || pendingLimit <= 0 {
		return appendUserMessage, nil
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM gateway_events
		WHERE namespace = ? AND session_name = ? AND state IN (?, ?, ?, ?)`,
		event.Namespace, event.SessionName,
		store.GatewayEventAccepted, store.GatewayEventQueued,
		store.GatewayEventDispatching, store.GatewayEventTaskCreated,
	).Scan(&pending); err != nil {
		return false, err
	}
	if pending < pendingLimit {
		return true, nil
	}
	event.State = store.GatewayEventDeadLettered
	event.StateMessage = "session queue limit exceeded"
	event.TaskName = ""
	return false, nil
}

func insertGatewayEventTx(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
	metadataJSON string,
) (bool, *store.GatewayEvent, error) {
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO gateway_events (
		id, namespace, namespace_uid, gateway_uid, gateway_generation, gateway_name, binding_name, binding_uid, binding_generation, agent_name, agent_uid,
		external_event_id, protocol_version,
		event_type, state, state_message, account_id, context_id, thread_id, sender_id,
		sender_display_name, text, reply_target, metadata_json, session_name, task_name, task_uid, delivery_id, provider_message_id, trace_parent, trace_state, transcript_order, attempt_count,
		claim_owner, claim_until, next_attempt_at, occurred_at, received_at, expires_at, created_at, updated_at, completed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.Namespace, event.NamespaceUID, event.GatewayUID, event.GatewayGeneration, event.GatewayName, event.BindingName, event.BindingUID,
		event.BindingGeneration, event.AgentName, event.AgentUID, event.ExternalEventID, event.ProtocolVersion, event.EventType, event.State, event.StateMessage, event.AccountID, event.ContextID,
		event.ThreadID, event.SenderID, event.SenderDisplayName, event.Text, event.ReplyTarget, metadataJSON,
		event.SessionName, event.TaskName, event.TaskUID, event.DeliveryID, event.ProviderMessageID, event.TraceParent, event.TraceState,
		event.TranscriptOrder, event.AttemptCount, event.ClaimOwner,
		nullableTime(event.ClaimUntil),
		event.NextAttemptAt, nullableTime(event.OccurredAt), event.ReceivedAt, event.ExpiresAt, event.CreatedAt,
		event.UpdatedAt, nullableTime(event.CompletedAt),
	)
	if err != nil {
		return false, nil, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, nil, err
	}
	if rows > 0 {
		return true, nil, nil
	}
	existing, err := getGatewayEventByExternalIDQuery(
		ctx, tx, event.Namespace, event.GatewayUID, event.ExternalEventID,
	)
	if err == nil && !gatewayEventsHaveSameEnvelope(existing, event) {
		return false, nil, store.ErrDuplicateMismatch
	}
	return false, existing, err
}

func gatewayEventsHaveSameEnvelope(left, right *store.GatewayEvent) bool {
	return store.GatewayEventEnvelopeDigest(left) == store.GatewayEventEnvelopeDigest(right)
}

func retainedGatewayMessage(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
) (string, int64, bool, error) {
	var sessionName string
	var order int64
	var metadataJSON string
	err := tx.QueryRowContext(ctx, `SELECT session_name, sort_order, metadata_json FROM session_messages
		WHERE namespace = ? AND message_id = ? LIMIT 1`,
		event.Namespace, gatewayUserMessageID(event.ID),
	).Scan(&sessionName, &order, &metadataJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	metadata := map[string]string{}
	if err := unmarshalStringMap(metadataJSON, &metadata); err != nil {
		return "", 0, false, err
	}
	if metadata[gatewayEnvelopeDigestMetadataKey] == "" ||
		metadata[gatewayEnvelopeDigestMetadataKey] != store.GatewayEventEnvelopeDigest(event) {
		return "", 0, false, store.ErrDuplicateMismatch
	}
	return sessionName, order, true, nil
}

func prepareGatewaySessionOrderTx(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
	now time.Time,
) (int64, error) {
	ownerRef := event.GatewayUID + "/" + event.BindingUID
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions
		(namespace, name, session_type, owner_type, owner_ref, active_task, active_task_uid, message_count, input_tokens, output_tokens, cancelled, created_at, updated_at)
		VALUES (?, ?, 'gateway', 'gateway', ?, '', '', 0, 0, 0, FALSE, ?, ?)
		ON CONFLICT(namespace, name) DO NOTHING`, event.Namespace, event.SessionName, ownerRef, now, now); err != nil {
		return 0, err
	}
	var sessionType, ownerType, existingOwnerRef string
	if err := tx.QueryRowContext(ctx, `SELECT session_type, owner_type, owner_ref FROM sessions
		WHERE namespace = ? AND name = ?`, event.Namespace, event.SessionName).Scan(
		&sessionType, &ownerType, &existingOwnerRef,
	); err != nil {
		return 0, err
	}
	if sessionType != gatewaySessionOwnerType || ownerType != gatewaySessionOwnerType || existingOwnerRef != ownerRef {
		return 0, store.ErrConflict
	}
	return nextSessionMessageOrderTx(ctx, tx, event.Namespace, event.SessionName)
}

func appendGatewayUserMessageTx(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
	now time.Time,
) error {
	messageMetadata := cloneStringMap(event.Metadata)
	messageMetadata["gateway"] = event.GatewayName
	messageMetadata["binding"] = event.BindingName
	messageMetadata["externalEventId"] = event.ExternalEventID
	messageMetadata["accountId"] = event.AccountID
	messageMetadata["contextId"] = event.ContextID
	messageMetadata["senderId"] = event.SenderID
	messageMetadata[gatewayEnvelopeDigestMetadataKey] = store.GatewayEventEnvelopeDigest(event)
	if event.ThreadID != "" {
		messageMetadata["threadId"] = event.ThreadID
	}
	if event.SenderDisplayName != "" {
		messageMetadata["senderDisplayName"] = event.SenderDisplayName
	}
	messageMetadataJSON, err := marshalStringMap(messageMetadata)
	if err != nil {
		return err
	}
	messageResult, err := tx.ExecContext(ctx, `INSERT INTO session_messages
		(namespace, session_name, message_id, sort_order, role, content, source_type, source_ref, metadata_json, created_at)
		VALUES (?, ?, ?, ?, 'user', ?, 'gateway-event', ?, ?, ?)
		ON CONFLICT(namespace, session_name, message_id) WHERE message_id <> '' DO NOTHING`,
		event.Namespace, event.SessionName, gatewayUserMessageID(event.ID), event.TranscriptOrder, event.Text,
		event.ID, messageMetadataJSON, event.ReceivedAt,
	)
	if err != nil {
		return err
	}
	inserted, err := messageResult.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		return store.ErrConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE sessions SET message_count = message_count + 1, updated_at = ?
		WHERE namespace = ? AND name = ?`, now, event.Namespace, event.SessionName)
	return err
}

// GetGatewayEvent returns one event by stable Orka event ID.
func (s *Store) GetGatewayEvent(ctx context.Context, namespace, id string) (*store.GatewayEvent, error) {
	return getGatewayEventQuery(ctx, s.db, namespace, id)
}

// GetGatewayEventDuplicate resolves a durable duplicate before mutable readiness checks.
func (s *Store) GetGatewayEventDuplicate(
	ctx context.Context, candidate *store.GatewayEvent, now time.Time,
) (*store.GatewayEvent, error) {
	if candidate == nil {
		return nil, store.ValidationErrorf("gateway event is required")
	}
	normalized := *candidate
	normalized.SenderDisplayName = sanitizeGatewayStoreText(normalized.SenderDisplayName, 256)
	normalized.Metadata = normalizeGatewayStoreMetadata(normalized.Metadata)
	candidate = &normalized
	if err := validateGatewayEvent(candidate); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	if existing, err := getGatewayEventByExternalIDQuery(
		ctx, tx, candidate.Namespace, candidate.GatewayUID, candidate.ExternalEventID,
	); err == nil {
		if store.GatewayEventEnvelopeDigest(existing) != store.GatewayEventEnvelopeDigest(candidate) {
			return nil, store.ErrDuplicateMismatch
		}
		return existing, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if tombstone, err := getGatewayEventTombstoneQuery(
		ctx, tx, candidate.Namespace, candidate.GatewayUID, candidate.ExternalEventID, now,
	); err == nil {
		if tombstone.EnvelopeDigest != store.GatewayEventEnvelopeDigest(candidate) {
			return nil, store.ErrDuplicateMismatch
		}
		copy := *candidate
		copy.ID = tombstone.EventID
		copy.State = store.GatewayEventCompleted
		copy.StateMessage = "event is retained by a deduplication tombstone"
		copy.SessionName = tombstone.SessionName
		copy.TranscriptOrder = tombstone.TranscriptOrder
		return &copy, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	retainedSession, retainedOrder, retained, err := retainedGatewayMessage(ctx, tx, candidate)
	if err != nil {
		return nil, err
	}
	if retained {
		copy := *candidate
		copy.State = store.GatewayEventCompleted
		copy.StateMessage = "event is retained in canonical session history"
		copy.SessionName = retainedSession
		copy.TranscriptOrder = retainedOrder
		return &copy, nil
	}
	return nil, store.ErrNotFound
}

// GetGatewayEventForTask resolves durable gateway ownership for one linked Task identity.
func (s *Store) GetGatewayEventForTask(ctx context.Context, namespace, taskName, taskUID string) (*store.GatewayEvent, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(taskName) == "" || strings.TrimSpace(taskUID) == "" {
		return nil, store.ValidationErrorf("namespace, taskName, and taskUID are required")
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+gatewayEventColumns+` FROM gateway_events
		WHERE namespace = ? AND task_name = ? AND task_uid = ?
		ORDER BY created_at DESC, id DESC LIMIT 1`,
		namespace, taskName, taskUID,
	)
	event, err := scanGatewayEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return event, err
}

// HasGatewayTaskTombstone reports whether retention compacted the exact linked Task identity.
func (s *Store) HasGatewayTaskTombstone(ctx context.Context, namespace, taskName, taskUID string) (bool, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(taskName) == "" || strings.TrimSpace(taskUID) == "" {
		return false, store.ValidationErrorf("namespace, taskName, and taskUID are required")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM gateway_event_tombstones
		WHERE namespace = ? AND task_name = ? AND task_uid = ?`, namespace, taskName, taskUID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// ListGatewayEvents lists newest-first normalized ingress records.
func (s *Store) ListGatewayEvents(ctx context.Context, filter store.GatewayEventFilter) ([]store.GatewayEvent, error) {
	query := `SELECT ` + gatewayEventColumns + ` FROM gateway_events WHERE 1 = 1`
	args := []any{}
	if strings.TrimSpace(filter.Namespace) != "" {
		query += ` AND namespace = ?`
		args = append(args, filter.Namespace)
	}
	query, args = appendGatewayStringFilter(query, args, "namespace_uid", filter.NamespaceUID)
	query, args = appendGatewayStringFilter(query, args, "gateway_uid", filter.GatewayUID)
	query, args = appendGatewayStringSetFilter(query, args, "gateway_uid", filter.GatewayUIDs)
	query, args = appendGatewayStringFilter(query, args, "id", filter.ID)
	query, args = appendGatewayStringFilter(query, args, "gateway_name", filter.GatewayName)
	query, args = appendGatewayStringFilter(query, args, "binding_name", filter.BindingName)
	query, args = appendGatewayStringFilter(query, args, "session_name", filter.SessionName)
	query, args = appendGatewayStringFilter(query, args, "task_name", filter.TaskName)
	if filter.BeforeCreatedAt != nil && filter.BeforeID != "" {
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, filter.BeforeCreatedAt.UTC(), filter.BeforeCreatedAt.UTC(), filter.BeforeID)
	}
	if filter.DueBefore != nil {
		query += ` AND next_attempt_at <= ?`
		args = append(args, filter.DueBefore.UTC())
	}
	if filter.ExpiresBefore != nil {
		query += ` AND expires_at <= ?`
		args = append(args, filter.ExpiresBefore.UTC())
	}
	if len(filter.States) > 0 {
		placeholders := make([]string, 0, len(filter.States))
		for _, state := range filter.States {
			if !store.IsValidGatewayEventState(state) {
				return nil, store.ValidationErrorf("unsupported gateway event state %q", state)
			}
			placeholders = append(placeholders, "?")
			args = append(args, state)
		}
		query += ` AND state IN (` + strings.Join(placeholders, ",") + `)`
	}
	if filter.SessionHeadOnly {
		query += ` AND (gateway_events.session_name = '' OR NOT EXISTS (
			SELECT 1 FROM gateway_events earlier
			WHERE earlier.namespace = gateway_events.namespace
			  AND earlier.session_name = gateway_events.session_name
			  AND earlier.transcript_order > 0
			  AND earlier.transcript_order < gateway_events.transcript_order
			  AND (earlier.state IN (?, ?, ?, ?)
			       OR (earlier.state = ? AND earlier.delivery_id = ''))
		))`
		args = append(args, store.GatewayEventAccepted, store.GatewayEventQueued,
			store.GatewayEventDispatching, store.GatewayEventTaskCreated, store.GatewayEventExpired)
	}
	if filter.MissingDelivery {
		query += ` AND delivery_id = ''`
	}
	if filter.OrderByExpiry {
		query += ` ORDER BY expires_at ASC, received_at ASC, id ASC`
	} else if filter.OrderByNextAttempt {
		query += ` ORDER BY next_attempt_at ASC, received_at ASC, id ASC`
	} else {
		query += gatewayListOrder
	}
	query += ` LIMIT ?`
	args = append(args, boundedGatewayLimit(filter.Limit))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var gatewayEvents []store.GatewayEvent
	for rows.Next() {
		event, err := scanGatewayEvent(rows)
		if err != nil {
			return nil, err
		}
		gatewayEvents = append(gatewayEvents, *event)
	}
	return gatewayEvents, rows.Err()
}

// ClaimNextGatewayEvent leases the oldest runnable event and reserves its Session.
func (s *Store) ClaimNextGatewayEvent(ctx context.Context, namespace, owner string, now time.Time, lease time.Duration) (*store.GatewayEvent, error) {
	if strings.TrimSpace(owner) == "" || lease <= 0 {
		return nil, store.ValidationErrorf("claim owner and positive lease are required")
	}
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	row := tx.QueryRowContext(ctx, `SELECT `+prefixedColumns("e", gatewayEventColumns)+`
		FROM gateway_events e
		JOIN sessions s ON s.namespace = e.namespace AND s.name = e.session_name
		WHERE (? = '' OR e.namespace = ?) AND e.next_attempt_at <= ? AND e.session_name <> ''
		  AND ((e.state = ? AND e.expires_at > ?) OR
		       (e.state = ? AND (e.claim_until IS NULL OR e.claim_until <= ?)))
		  AND (s.active_task = '' OR (s.active_task = e.task_name AND s.active_task_uid = e.task_uid))
		  AND NOT EXISTS (
			SELECT 1 FROM gateway_events earlier
			WHERE earlier.namespace = e.namespace AND earlier.session_name = e.session_name
			  AND earlier.transcript_order > 0 AND earlier.transcript_order < e.transcript_order
			  AND earlier.state IN (?, ?, ?, ?)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM gateway_events active
			WHERE active.namespace = e.namespace AND active.session_name = e.session_name
			  AND active.id <> e.id AND active.state = ?
		  )
		ORDER BY e.received_at ASC, e.id ASC LIMIT 1`,
		namespace, namespace, now, store.GatewayEventQueued, now, store.GatewayEventDispatching, now,
		store.GatewayEventAccepted, store.GatewayEventQueued, store.GatewayEventDispatching, store.GatewayEventTaskCreated,
		store.GatewayEventTaskCreated,
	)
	event, err := scanGatewayEvent(row)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, store.ErrNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	claimUntil := now.Add(lease)
	result, err := tx.ExecContext(ctx, `UPDATE gateway_events SET state = ?, claim_owner = ?, claim_until = ?,
		attempt_count = attempt_count + 1, updated_at = ?
		WHERE namespace = ? AND id = ? AND (state = ? OR (state = ? AND (claim_until IS NULL OR claim_until <= ?)))`,
		store.GatewayEventDispatching, owner, claimUntil, now, event.Namespace, event.ID,
		store.GatewayEventQueued, store.GatewayEventDispatching, now,
	)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated == 0 {
		return nil, store.ErrNotFound
	}
	lockResult, err := tx.ExecContext(ctx, `UPDATE sessions SET active_task = ?, active_task_uid = ?, updated_at = ?
		WHERE namespace = ? AND name = ? AND (active_task = '' OR
		  (active_task = ? AND active_task_uid = ?))`,
		event.TaskName, event.TaskUID, now, event.Namespace, event.SessionName, event.TaskName, event.TaskUID,
	)
	if err != nil {
		return nil, err
	}
	locked, err := lockResult.RowsAffected()
	if err != nil {
		return nil, err
	}
	if locked == 0 {
		return nil, store.ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	event.State = store.GatewayEventDispatching
	event.ClaimOwner = owner
	event.ClaimUntil = &claimUntil
	event.AttemptCount++
	event.UpdatedAt = now
	return event, nil
}

// RenewGatewayEventClaim proves ownership immediately before a consequential Task create.
func (s *Store) RenewGatewayEventClaim(
	ctx context.Context,
	namespace, id, owner string,
	now time.Time,
	lease time.Duration,
) (*store.GatewayEvent, error) {
	if strings.TrimSpace(owner) == "" || lease <= 0 {
		return nil, store.ValidationErrorf("claim owner and positive lease are required")
	}
	now = now.UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_events SET claim_until = ?, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?
		  AND claim_until > ? AND expires_at > ?`,
		now.Add(lease), now, namespace, id, store.GatewayEventDispatching, owner, now, now)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated == 0 {
		return nil, store.ErrConflict
	}
	return s.GetGatewayEvent(ctx, namespace, id)
}

// FreezeGatewayEventTaskRuntimeAllowedTools records the external runtime policy
// that will be written to the deterministic Task. The first value wins so
// crash recovery never rebuilds the Task from mutable live policy.
func (s *Store) FreezeGatewayEventTaskRuntimeAllowedTools(
	ctx context.Context,
	namespace, id, owner string,
	allowedTools []string,
	now time.Time,
) (*store.GatewayEvent, error) {
	if strings.TrimSpace(owner) == "" || allowedTools == nil {
		return nil, store.ValidationErrorf("claim owner and explicit allowedTools are required")
	}
	encoded, err := json.Marshal(allowedTools)
	if err != nil {
		return nil, fmt.Errorf("encode Gateway Task runtime allowedTools: %w", err)
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_events
		SET task_runtime_allowed_tools_json = ?, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?
		  AND claim_until > ? AND expires_at > ?
		  AND (task_runtime_allowed_tools_json IS NULL OR task_runtime_allowed_tools_json = ?)`,
		string(encoded), now, namespace, id, store.GatewayEventDispatching, owner,
		now, now, string(encoded),
	)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated == 0 {
		return nil, store.ErrConflict
	}
	event, err := s.GetGatewayEvent(ctx, namespace, id)
	if err != nil {
		return nil, err
	}
	if !event.TaskPolicyFrozen || !slices.Equal(event.TaskAllowedTools, allowedTools) {
		return nil, store.ErrConflict
	}
	return event, nil
}

// MarkGatewayEventTaskCreated links a claimed event to its deterministic Task.
func (s *Store) MarkGatewayEventTaskCreated(ctx context.Context, namespace, id, taskName, taskUID, owner string, now time.Time) error {
	if strings.TrimSpace(taskName) == "" || strings.TrimSpace(taskUID) == "" || strings.TrimSpace(owner) == "" {
		return store.ValidationErrorf("taskName, taskUID, and owner are required")
	}
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	event, err := getGatewayEventQuery(ctx, tx, namespace, id)
	if err != nil {
		return err
	}
	if event.TaskName == "" || event.TaskName != taskName {
		return store.ErrConflict
	}
	if event.State == store.GatewayEventTaskCreated {
		if event.TaskUID != taskUID {
			return store.ErrConflict
		}
		return nil
	}
	if event.State != store.GatewayEventDispatching || event.ClaimOwner != owner {
		return store.ErrConflict
	}
	var activeTask, activeTaskUID string
	if err := tx.QueryRowContext(ctx, `SELECT active_task, active_task_uid FROM sessions
		WHERE namespace = ? AND name = ?`, namespace, event.SessionName).Scan(&activeTask, &activeTaskUID); err != nil {
		return err
	}
	if activeTask != taskName || (activeTaskUID != "" && activeTaskUID != taskUID) {
		return store.ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE gateway_events SET state = ?, task_uid = ?,
		claim_owner = '', claim_until = NULL, state_message = '', updated_at = ? WHERE namespace = ? AND id = ?
		AND state = ? AND claim_owner = ? AND task_name = ?`,
		store.GatewayEventTaskCreated, taskUID, now, namespace, id,
		store.GatewayEventDispatching, owner, taskName,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrConflict
	}
	lockResult, err := tx.ExecContext(ctx, `UPDATE sessions SET active_task_uid = ?, updated_at = ?
		WHERE namespace = ? AND name = ? AND active_task = ? AND (active_task_uid = '' OR active_task_uid = ?)`,
		taskUID, now, namespace, event.SessionName, taskName, taskUID)
	if err != nil {
		return err
	}
	locked, err := lockResult.RowsAffected()
	if err != nil {
		return err
	}
	if locked == 0 {
		return store.ErrConflict
	}
	return tx.Commit()
}

// RetryGatewayEvent returns a failed dispatch to the FIFO queue and releases its reservation.
func (s *Store) RetryGatewayEvent(ctx context.Context, namespace, id, owner, reason string, nextAttemptAt time.Time) error {
	reason = sanitizeGatewayStoreText(reason, 1024)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	event, err := getGatewayEventQuery(ctx, tx, namespace, id)
	if err != nil {
		return err
	}
	updateResult, err := tx.ExecContext(ctx, `UPDATE gateway_events SET state = ?, state_message = ?, next_attempt_at = ?,
		claim_owner = '', claim_until = NULL, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?`,
		store.GatewayEventQueued, reason, nextAttemptAt.UTC(), time.Now().UTC(), namespace, id,
		store.GatewayEventDispatching, owner)
	if err != nil {
		return err
	}
	updated, err := updateResult.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 0 {
		return store.ErrConflict
	}
	if event.SessionName != "" && event.TaskName != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET active_task = '', active_task_uid = '', updated_at = ?
			WHERE namespace = ? AND name = ? AND active_task = ?
			  AND active_task_uid = ?`,
			time.Now().UTC(), namespace, event.SessionName, event.TaskName, event.TaskUID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeferGatewayEventProjection rotates a nonterminal TaskCreated event behind other due projection work.
func (s *Store) DeferGatewayEventProjection(
	ctx context.Context,
	namespace, id string,
	nextAttemptAt time.Time,
) error {
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_events SET next_attempt_at = ?, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ?`,
		nextAttemptAt.UTC(), time.Now().UTC(), namespace, id, store.GatewayEventTaskCreated)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrConflict
	}
	return nil
}

// ExpireGatewayEvent terminally expires stale work, appends one visible error, and releases the Session.
func (s *Store) ExpireGatewayEvent(
	ctx context.Context,
	namespace, id, owner, reason string,
	now time.Time,
) error {
	reason = sanitizeGatewayStoreText(reason, 1024)
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	event, err := getGatewayEventQuery(ctx, tx, namespace, id)
	if err != nil {
		return err
	}
	if err := expireGatewayEventTx(ctx, tx, event, owner, reason, now); err != nil {
		return err
	}
	return tx.Commit()
}

// ExpireGatewayEventWithDelivery atomically expires stale work and creates the
// corresponding adapter outbox record.
func (s *Store) ExpireGatewayEventWithDelivery(
	ctx context.Context,
	projection store.GatewayExpiryProjection,
) (*store.GatewayDelivery, bool, error) {
	if projection.EventID == "" || projection.Delivery.EventID != projection.EventID {
		return nil, false, store.ValidationErrorf("event ID and delivery event ID must match")
	}
	if err := validateGatewayDelivery(&projection.Delivery); err != nil {
		return nil, false, err
	}
	reason := sanitizeGatewayStoreText(projection.Reason, 1024)
	now := projection.CompletedAt.UTC()
	if now.IsZero() {
		return nil, false, store.ValidationErrorf("completion time is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback() //nolint:errcheck
	event, err := getGatewayEventQuery(ctx, tx, projection.Delivery.Namespace, projection.EventID)
	if err != nil {
		return nil, false, err
	}
	if event.State == store.GatewayEventExpired {
		existing, getErr := getGatewayDeliveryByEventQuery(ctx, tx, event.Namespace, event.ID)
		if errors.Is(getErr, store.ErrNotFound) {
			delivery, created, createErr := createGatewayDeliveryTx(ctx, tx, &projection.Delivery)
			if createErr != nil {
				return nil, false, createErr
			}
			if err := tx.Commit(); err != nil {
				return nil, false, err
			}
			return delivery, created, nil
		}
		if getErr != nil {
			return nil, false, getErr
		}
		if existing.ID != projection.Delivery.ID || existing.IdempotencyID != projection.Delivery.IdempotencyID ||
			existing.EventID != projection.Delivery.EventID || existing.Namespace != projection.Delivery.Namespace {
			return nil, false, store.ErrDuplicateMismatch
		}
		if _, err := tx.ExecContext(ctx, `UPDATE gateway_events SET delivery_id = ?, updated_at = ?
			WHERE namespace = ? AND id = ? AND delivery_id = ''`,
			existing.ID, now, event.Namespace, event.ID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}
	if err := expireGatewayEventTx(ctx, tx, event, projection.Owner, reason, now); err != nil {
		return nil, false, err
	}
	delivery, created, err := createGatewayDeliveryTx(ctx, tx, &projection.Delivery)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return delivery, created, nil
}

// MarkExpiredGatewayEventDeadLettered releases an unrecoverable legacy expiry
// from repair ordering without fabricating immutable delivery identity.
func (s *Store) MarkExpiredGatewayEventDeadLettered(
	ctx context.Context, namespace, id, reason string, now time.Time,
) error {
	reason = sanitizeGatewayStoreText(reason, 1024)
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_events SET state = ?, state_message = ?, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ? AND delivery_id = ''`,
		store.GatewayEventDeadLettered, reason, now.UTC(), namespace, id, store.GatewayEventExpired)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 0 {
		return store.ErrConflict
	}
	return nil
}

func expireGatewayEventTx(
	ctx context.Context,
	tx *sql.Tx,
	event *store.GatewayEvent,
	owner, reason string,
	now time.Time,
) error {
	if event == nil {
		return store.ValidationErrorf("gateway event is required")
	}
	switch event.State {
	case store.GatewayEventAccepted, store.GatewayEventQueued, store.GatewayEventTaskCreated:
	case store.GatewayEventDispatching:
		if owner == "" || event.ClaimOwner != owner {
			return store.ErrConflict
		}
	default:
		return store.ErrConflict
	}
	if event.SessionName != "" && event.TranscriptOrder > 0 {
		metadataJSON, err := marshalStringMap(map[string]string{
			"gateway": event.GatewayName, "binding": event.BindingName, "eventId": event.ID,
		})
		if err != nil {
			return err
		}
		messageResult, err := tx.ExecContext(ctx, `INSERT INTO session_messages
			(namespace, session_name, message_id, sort_order, role, content, source_type, source_ref, metadata_json, created_at)
			VALUES (?, ?, ?, ?, 'assistant', ?, 'gateway-event', ?, ?, ?)
			ON CONFLICT(namespace, session_name, message_id) WHERE message_id <> '' DO NOTHING`,
			event.Namespace, event.SessionName, store.GatewayErrorMessageID(event.ID), event.TranscriptOrder+1,
			reason, event.ID, metadataJSON, now,
		)
		if err != nil {
			return err
		}
		inserted, err := messageResult.RowsAffected()
		if err != nil {
			return err
		}
		if inserted > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE sessions SET message_count = message_count + 1, updated_at = ?
				WHERE namespace = ? AND name = ?`, now, event.Namespace, event.SessionName); err != nil {
				return err
			}
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE gateway_events SET state = ?, state_message = ?, completed_at = ?,
		claim_owner = '', claim_until = NULL, updated_at = ? WHERE namespace = ? AND id = ?`,
		store.GatewayEventExpired, reason, now, now, event.Namespace, event.ID)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 0 {
		return store.ErrConflict
	}
	if event.SessionName != "" && event.TaskName != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET active_task = '', active_task_uid = '', updated_at = ?
			WHERE namespace = ? AND name = ? AND active_task = ?
			  AND active_task_uid = ?`,
			now, event.Namespace, event.SessionName, event.TaskName, event.TaskUID); err != nil {
			return err
		}
	}
	return nil
}

// ProjectGatewayTerminal atomically appends one terminal message, creates one outbox row,
// completes the event, and releases the Session for the next queued event.
func (s *Store) ProjectGatewayTerminal(ctx context.Context, projection store.GatewayTerminalProjection) (*store.GatewayDelivery, bool, error) {
	if projection.EventID == "" || projection.Message.ID == "" {
		return nil, false, store.ValidationErrorf("event ID and stable message ID are required")
	}
	if err := validateGatewayDelivery(&projection.Delivery); err != nil {
		return nil, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback() //nolint:errcheck
	event, err := getGatewayEventQuery(ctx, tx, projection.Delivery.Namespace, projection.EventID)
	if err != nil {
		return nil, false, err
	}
	if err := validateGatewayTerminalProjection(event, &projection); err != nil {
		return nil, false, err
	}
	if event.State == store.GatewayEventCompleted {
		delivery, err := getGatewayDeliveryByEventQuery(ctx, tx, event.Namespace, event.ID)
		if err != nil {
			return nil, false, err
		}
		return delivery, false, nil
	}
	if event.State != store.GatewayEventTaskCreated {
		return nil, false, store.ErrConflict
	}

	messageMetadataJSON, err := marshalStringMap(projection.Message.Metadata)
	if err != nil {
		return nil, false, err
	}
	messageTime := projection.Message.Timestamp.UTC()
	if messageTime.IsZero() {
		messageTime = projection.CompletedAt.UTC()
	}
	messageResult, err := tx.ExecContext(ctx, `INSERT INTO session_messages
		(namespace, session_name, message_id, sort_order, role, content, name, source_type, source_ref, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(namespace, session_name, message_id) WHERE message_id <> '' DO NOTHING`,
		event.Namespace, event.SessionName, projection.Message.ID, event.TranscriptOrder+1, projection.Message.Role,
		projection.Message.Content, nilIfEmpty(projection.Message.Name), projection.Message.SourceType,
		projection.Message.SourceRef, messageMetadataJSON, messageTime,
	)
	if err != nil {
		return nil, false, err
	}
	insertedMessage, err := messageResult.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if insertedMessage > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET message_count = message_count + 1, updated_at = ?
			WHERE namespace = ? AND name = ?`, projection.CompletedAt.UTC(), event.Namespace, event.SessionName); err != nil {
			return nil, false, err
		}
	}

	delivery, created, err := createGatewayDeliveryTx(ctx, tx, &projection.Delivery)
	if err != nil {
		return nil, false, err
	}
	completedAt := projection.CompletedAt.UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE gateway_events SET state = ?, state_message = '', delivery_id = ?, completed_at = ?,
		claim_owner = '', claim_until = NULL, updated_at = ? WHERE namespace = ? AND id = ?`,
		store.GatewayEventCompleted, delivery.ID, completedAt, completedAt, event.Namespace, event.ID); err != nil {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET active_task = '', active_task_uid = '', updated_at = ?
		WHERE namespace = ? AND name = ? AND active_task = ?
		  AND active_task_uid = ?`,
		completedAt, event.Namespace, event.SessionName, event.TaskName, event.TaskUID); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return delivery, created, nil
}

func validateGatewayTerminalProjection(event *store.GatewayEvent, projection *store.GatewayTerminalProjection) error {
	if event == nil || projection == nil {
		return store.ValidationErrorf("gateway terminal projection is required")
	}
	delivery := &projection.Delivery
	for name, matched := range map[string]bool{
		"eventId":           projection.EventID == event.ID && delivery.EventID == event.ID,
		"namespace":         delivery.Namespace == event.Namespace,
		"namespaceUid":      delivery.NamespaceUID == event.NamespaceUID,
		"gatewayUid":        delivery.GatewayUID == event.GatewayUID,
		"gatewayGeneration": delivery.GatewayGeneration == event.GatewayGeneration,
		"gatewayName":       delivery.GatewayName == event.GatewayName,
		"bindingName":       delivery.BindingName == event.BindingName,
		"taskName":          event.TaskName != "" && delivery.TaskName == event.TaskName,
		"sessionName":       delivery.SessionName == event.SessionName,
		"accountId":         delivery.AccountID == event.AccountID,
		"contextId":         delivery.ContextID == event.ContextID,
		"threadId":          delivery.ThreadID == event.ThreadID,
	} {
		if !matched {
			return store.ValidationErrorf("gateway terminal projection %s does not match admitted event", name)
		}
	}
	expectedReplyTarget := strings.TrimSpace(event.ReplyTarget)
	if expectedReplyTarget == "" {
		expectedReplyTarget = event.ContextID
	}
	if delivery.ReplyTarget != expectedReplyTarget {
		return store.ValidationErrorf("gateway terminal projection replyTarget does not match admitted event")
	}
	if delivery.State != store.GatewayDeliveryPending {
		return store.ValidationErrorf("gateway terminal projection delivery must be Pending")
	}
	expectedMessageID := store.GatewayErrorMessageID(event.ID)
	if delivery.Kind == gatewayDeliveryKindFinal {
		expectedMessageID = store.GatewayAssistantMessageID(event.ID)
	}
	if projection.Message.ID != expectedMessageID || projection.Message.Role != "assistant" ||
		projection.Message.Content != delivery.Text {
		return store.ValidationErrorf("gateway terminal projection message identity does not match admitted event")
	}
	if projection.Message.SourceType != "" && projection.Message.SourceType != "gateway-task" {
		return store.ValidationErrorf("gateway terminal projection message sourceType does not match admitted event")
	}
	if projection.Message.SourceRef != "" && projection.Message.SourceRef != event.TaskName {
		return store.ValidationErrorf("gateway terminal projection message sourceRef does not match admitted event")
	}
	return nil
}

// CreateGatewayDelivery idempotently inserts one outbox row.
func (s *Store) CreateGatewayDelivery(ctx context.Context, delivery *store.GatewayDelivery) (*store.GatewayDelivery, bool, error) {
	if err := validateGatewayDelivery(delivery); err != nil {
		return nil, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback() //nolint:errcheck
	got, created, err := createGatewayDeliveryTx(ctx, tx, delivery)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return got, created, nil
}

// GetGatewayDelivery returns one delivery by stable ID.
func (s *Store) GetGatewayDelivery(ctx context.Context, namespace, id string) (*store.GatewayDelivery, error) {
	return getGatewayDeliveryQuery(ctx, s.db, namespace, id)
}

// ListGatewayDeliveries lists newest-first delivery records.
func (s *Store) ListGatewayDeliveries(ctx context.Context, filter store.GatewayDeliveryFilter) ([]store.GatewayDelivery, error) {
	if strings.TrimSpace(filter.Namespace) == "" {
		return nil, store.ValidationErrorf("namespace is required")
	}
	query := `SELECT ` + gatewayDeliveryColumns + ` FROM gateway_deliveries WHERE namespace = ?`
	args := []any{filter.Namespace}
	query, args = appendGatewayStringFilter(query, args, "namespace_uid", filter.NamespaceUID)
	query, args = appendGatewayStringFilter(query, args, "gateway_uid", filter.GatewayUID)
	query, args = appendGatewayStringSetFilter(query, args, "gateway_uid", filter.GatewayUIDs)
	query, args = appendGatewayStringFilter(query, args, "id", filter.ID)
	query, args = appendGatewayStringFilter(query, args, "gateway_name", filter.GatewayName)
	query, args = appendGatewayStringFilter(query, args, "binding_name", filter.BindingName)
	query, args = appendGatewayStringFilter(query, args, "event_id", filter.EventID)
	query, args = appendGatewayStringFilter(query, args, "session_name", filter.SessionName)
	query, args = appendGatewayStringFilter(query, args, "task_name", filter.TaskName)
	if filter.BeforeCreatedAt != nil && filter.BeforeID != "" {
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, filter.BeforeCreatedAt.UTC(), filter.BeforeCreatedAt.UTC(), filter.BeforeID)
	}
	if len(filter.States) > 0 {
		placeholders := make([]string, 0, len(filter.States))
		for _, state := range filter.States {
			if !store.IsValidGatewayDeliveryState(state) {
				return nil, store.ValidationErrorf("unsupported gateway delivery state %q", state)
			}
			placeholders = append(placeholders, "?")
			args = append(args, state)
		}
		query += ` AND state IN (` + strings.Join(placeholders, ",") + `)`
	}
	query += gatewayListOrder
	query += ` LIMIT ?`
	args = append(args, boundedGatewayLimit(filter.Limit))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var deliveries []store.GatewayDelivery
	for rows.Next() {
		delivery, err := scanGatewayDelivery(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, *delivery)
	}
	return deliveries, rows.Err()
}

// ClaimNextGatewayDelivery leases the oldest due outbox row.
func (s *Store) ClaimNextGatewayDelivery(ctx context.Context, namespace, owner string, now time.Time, lease time.Duration) (*store.GatewayDelivery, error) {
	if strings.TrimSpace(owner) == "" || lease <= 0 {
		return nil, store.ValidationErrorf("claim owner and positive lease are required")
	}
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, last_error = ?,
		claim_owner = '', claim_until = NULL, updated_at = ?
		WHERE (? = '' OR namespace = ?) AND attempt_count >= max_attempts AND (state IN (?, ?) OR
		  (state = ? AND (claim_until IS NULL OR claim_until <= ?)))`,
		store.GatewayDeliveryDeadLettered, "delivery attempts exhausted", now, namespace, namespace,
		store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending, now,
	); err != nil {
		return nil, err
	}
	row := tx.QueryRowContext(ctx, `SELECT `+prefixedColumns("delivery", gatewayDeliveryColumns)+` FROM gateway_deliveries delivery
		LEFT JOIN gateway_events delivery_event
		  ON delivery_event.namespace = delivery.namespace AND delivery_event.id = delivery.event_id
		WHERE (? = '' OR delivery.namespace = ?) AND delivery.expires_at > ? AND delivery.next_attempt_at <= ?
		  AND delivery.attempt_count < delivery.max_attempts
		  AND (delivery.state IN (?, ?) OR (delivery.state = ? AND (delivery.claim_until IS NULL OR delivery.claim_until <= ?)))
		  AND (delivery.session_name = '' OR NOT EXISTS (
			SELECT 1 FROM gateway_deliveries earlier
			LEFT JOIN gateway_events earlier_event
			  ON earlier_event.namespace = earlier.namespace AND earlier_event.id = earlier.event_id
			WHERE earlier.namespace = delivery.namespace AND earlier.session_name = delivery.session_name
			  AND (
			    (COALESCE(earlier_event.transcript_order, 0) > 0 AND COALESCE(delivery_event.transcript_order, 0) > 0 AND
			      (earlier_event.transcript_order < delivery_event.transcript_order OR
			       (earlier_event.transcript_order = delivery_event.transcript_order AND
			        (earlier.created_at < delivery.created_at OR (earlier.created_at = delivery.created_at AND earlier.id < delivery.id)))))
			    OR
			    ((COALESCE(earlier_event.transcript_order, 0) <= 0 OR COALESCE(delivery_event.transcript_order, 0) <= 0) AND
			      (earlier.created_at < delivery.created_at OR (earlier.created_at = delivery.created_at AND earlier.id < delivery.id)))
			  )
			  AND earlier.state IN (?, ?, ?)
		  ))
		  AND (delivery.session_name = '' OR NOT EXISTS (
			SELECT 1 FROM gateway_events missing
			WHERE missing.namespace = delivery.namespace AND missing.session_name = delivery.session_name
			  AND missing.state = ? AND missing.delivery_id = ''
			  AND missing.transcript_order > 0
			  AND (COALESCE(delivery_event.transcript_order, 0) <= 0 OR missing.transcript_order < delivery_event.transcript_order)
		  ))
		ORDER BY delivery.created_at ASC, delivery.id ASC LIMIT 1`,
		namespace, namespace, now, now, store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending, now,
		store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending,
		store.GatewayEventExpired,
	)
	delivery, err := scanGatewayDelivery(row)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, store.ErrNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	claimUntil := now.Add(lease)
	result, err := tx.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, claim_owner = ?, claim_until = ?,
		attempt_count = attempt_count + 1, updated_at = ? WHERE namespace = ? AND id = ?
		AND attempt_count < max_attempts
		AND (state IN (?, ?) OR (state = ? AND (claim_until IS NULL OR claim_until <= ?)))`,
		store.GatewayDeliverySending, owner, claimUntil, now, delivery.Namespace, delivery.ID,
		store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending, now,
	)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated == 0 {
		return nil, store.ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	delivery.State = store.GatewayDeliverySending
	delivery.ClaimOwner = owner
	delivery.ClaimUntil = &claimUntil
	delivery.AttemptCount++
	delivery.UpdatedAt = now
	return delivery, nil
}

// MarkGatewayDeliveryDelivered records one successful provider send.
func (s *Store) MarkGatewayDeliveryDelivered(ctx context.Context, namespace, id, owner, providerMessageID string, now time.Time) error {
	providerMessageID = sanitizeGatewayStoreText(providerMessageID, 256)
	now = now.UTC()
	s.executionEventMu.Lock()
	defer s.executionEventMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	delivery, err := getGatewayDeliveryQuery(ctx, tx, namespace, id)
	if err != nil {
		return err
	}
	if delivery.State == store.GatewayDeliveryDelivered {
		if delivery.ProviderMessageID != providerMessageID {
			return store.ErrConflict
		}
		return nil
	}
	if delivery.State != store.GatewayDeliverySending || delivery.ClaimOwner != owner {
		return store.ErrConflict
	}
	event, err := getGatewayEventQuery(ctx, tx, namespace, delivery.EventID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, provider_message_id = ?, last_error = '',
		claim_owner = '', claim_until = NULL, delivered_at = ?, updated_at = ?
		WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?`,
		store.GatewayDeliveryDelivered, providerMessageID, now, now, namespace, id,
		store.GatewayDeliverySending, owner,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gateway_events SET delivery_id = ?, provider_message_id = ?, updated_at = ?
		WHERE namespace = ? AND id = ?`, delivery.ID, providerMessageID, now, namespace, delivery.EventID); err != nil {
		return err
	}
	if delivery.SessionName != "" {
		messageID := "gateway:" + delivery.EventID + ":error"
		if delivery.Kind == gatewayDeliveryKindFinal {
			messageID = "gateway:" + delivery.EventID + ":assistant"
		}
		var metadataJSON string
		err := tx.QueryRowContext(ctx, `SELECT metadata_json FROM session_messages
			WHERE namespace = ? AND session_name = ? AND message_id = ?`,
			namespace, delivery.SessionName, messageID,
		).Scan(&metadataJSON)
		if err == nil {
			metadata := map[string]string{}
			if err := unmarshalStringMap(metadataJSON, &metadata); err != nil {
				return err
			}
			metadata["deliveryId"] = delivery.ID
			if providerMessageID != "" {
				metadata["providerMessageId"] = providerMessageID
			}
			metadataJSON, err = marshalStringMap(metadata)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE session_messages SET metadata_json = ?
				WHERE namespace = ? AND session_name = ? AND message_id = ?`,
				metadataJSON, namespace, delivery.SessionName, messageID,
			); err != nil {
				return err
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if delivery.TaskName != "" {
		contentJSON, err := json.Marshal(map[string]string{
			"gatewayEventId":    delivery.EventID,
			"deliveryId":        delivery.ID,
			"providerMessageId": providerMessageID,
		})
		if err != nil {
			return err
		}
		var latestSeq int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM execution_events
			WHERE namespace = ? AND stream_type = ? AND stream_id = ?`,
			namespace, store.ExecutionEventStreamTypeTask, delivery.TaskName,
		).Scan(&latestSeq); err != nil {
			return err
		}
		seq := latestSeq + 1
		sessionSeq, err := nextSQLiteSessionExecutionEventSeq(ctx, tx, namespace, delivery.SessionName)
		if err != nil {
			return err
		}
		agentName := ""
		if event != nil {
			agentName = event.AgentName
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO execution_events
			(id, namespace, stream_type, stream_id, seq, session_seq, type, severity, task_name, session_name,
			 agent_name, summary, content_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			executionEventID(namespace, store.ExecutionEventStreamTypeTask, delivery.TaskName, seq),
			namespace, store.ExecutionEventStreamTypeTask, delivery.TaskName, seq, sessionSeq,
			executionevents.ExecutionEventTypeGatewayDeliveryCompleted, executionevents.ExecutionEventSeverityInfo,
			delivery.TaskName, delivery.SessionName, agentName, "Gateway delivery completed", string(contentJSON), now,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ScheduleGatewayDeliveryRetry records a retryable adapter result.
func (s *Store) ScheduleGatewayDeliveryRetry(ctx context.Context, namespace, id, owner, reason string, nextAttemptAt time.Time) error {
	reason = sanitizeGatewayStoreText(reason, 1024)
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, last_error = ?, next_attempt_at = ?,
		claim_owner = '', claim_until = NULL, updated_at = ? WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?`,
		store.GatewayDeliveryRetryScheduled, reason, nextAttemptAt.UTC(), time.Now().UTC(), namespace, id,
		store.GatewayDeliverySending, owner,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrConflict
	}
	return nil
}

// MarkGatewayDeliveryTerminal records a permanent, dead-lettered, or expired result.
func (s *Store) MarkGatewayDeliveryTerminal(ctx context.Context, namespace, id, owner string, state store.GatewayDeliveryState, reason string, now time.Time) error {
	reason = sanitizeGatewayStoreText(reason, 1024)
	switch state {
	case store.GatewayDeliveryFailed, store.GatewayDeliveryDeadLettered, store.GatewayDeliveryExpired:
	default:
		return store.ValidationErrorf("unsupported terminal delivery state %q", state)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, last_error = ?, claim_owner = '',
		claim_until = NULL, updated_at = ? WHERE namespace = ? AND id = ? AND state = ? AND claim_owner = ?`,
		state, reason, now.UTC(), namespace, id, store.GatewayDeliverySending, owner)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return store.ErrNotFound
	}
	return nil
}

// RetryGatewayDelivery manually requeues one dead-lettered or failed delivery.
func (s *Store) RetryGatewayDelivery(ctx context.Context, namespace, id string, now, expiresAt time.Time) (*store.GatewayDelivery, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, attempt_count = 0,
		manual_retry_count = manual_retry_count + 1, next_attempt_at = ?, expires_at = ?, last_error = '',
		claim_owner = '', claim_until = NULL, updated_at = ?
		WHERE namespace = ? AND id = ? AND state IN (?, ?)`,
		store.GatewayDeliveryPending, now.UTC(), expiresAt.UTC(), now.UTC(), namespace, id,
		store.GatewayDeliveryDeadLettered, store.GatewayDeliveryFailed,
	)
	if err != nil {
		return nil, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		if _, err := s.GetGatewayDelivery(ctx, namespace, id); err != nil {
			return nil, err
		}
		return nil, store.ErrConflict
	}
	return s.GetGatewayDelivery(ctx, namespace, id)
}

// GetGatewayQueueStats returns exact queue depth and oldest-age inputs without identity labels.
func (s *Store) GetGatewayQueueStats(ctx context.Context, namespace string) (store.GatewayQueueStats, error) {
	var stats store.GatewayQueueStats
	var oldestEvent, oldestDelivery sqliteNullableTime
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(received_at) FROM gateway_events
		WHERE (? = '' OR namespace = ?) AND state IN (?, ?, ?, ?)`,
		namespace, namespace, store.GatewayEventAccepted, store.GatewayEventQueued,
		store.GatewayEventDispatching, store.GatewayEventTaskCreated,
	).Scan(&stats.PendingEvents, &oldestEvent); err != nil {
		return stats, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(next_attempt_at) FROM gateway_deliveries
		WHERE (? = '' OR namespace = ?) AND state IN (?, ?, ?)`,
		namespace, namespace, store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending,
	).Scan(&stats.PendingDeliveries, &oldestDelivery); err != nil {
		return stats, err
	}
	stats.OldestEventReceived = oldestEvent.Ptr()
	stats.OldestDeliveryDue = oldestDelivery.Ptr()
	return stats, nil
}

// MaintainGatewayRecords expires pending deliveries, compacts terminal event identities into
// bounded tombstones, prunes their transcript messages, and removes empty gateway Sessions.
// Event expiry stays in the gateway service so it can atomically create a visible error delivery.
func (s *Store) MaintainGatewayRecords(ctx context.Context, namespace string, now, terminalCutoff time.Time) (store.GatewayMaintenanceResult, error) {
	var result store.GatewayMaintenanceResult
	now = now.UTC()
	terminalCutoff = terminalCutoff.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback() //nolint:errcheck

	tombstoneDelete, err := tx.ExecContext(ctx, `DELETE FROM gateway_event_tombstones
		WHERE (? = '' OR namespace = ?) AND expires_at <= ?`, namespace, namespace, now)
	if err != nil {
		return result, err
	}
	if count, rowsErr := tombstoneDelete.RowsAffected(); rowsErr == nil {
		result.DeletedTombstones = int(count)
	}

	deliveryExpiry, err := tx.ExecContext(ctx, `UPDATE gateway_deliveries SET state = ?, last_error = 'delivery expired',
		claim_owner = '', claim_until = NULL, updated_at = ?
		WHERE (? = '' OR namespace = ?) AND expires_at <= ? AND (state IN (?, ?) OR
		  (state = ? AND (claim_until IS NULL OR claim_until <= ?)))`,
		store.GatewayDeliveryExpired, now, namespace, namespace, now,
		store.GatewayDeliveryPending, store.GatewayDeliveryRetryScheduled, store.GatewayDeliverySending, now)
	if err != nil {
		return result, err
	}
	if count, rowsErr := deliveryExpiry.RowsAffected(); rowsErr == nil {
		result.ExpiredDeliveries = int(count)
	}

	deliveryDelete, err := tx.ExecContext(ctx, `DELETE FROM gateway_deliveries WHERE (? = '' OR namespace = ?)
		AND updated_at < ? AND state IN (?, ?, ?, ?)`, namespace, namespace, terminalCutoff,
		store.GatewayDeliveryDelivered, store.GatewayDeliveryFailed, store.GatewayDeliveryDeadLettered, store.GatewayDeliveryExpired)
	if err != nil {
		return result, err
	}
	if count, rowsErr := deliveryDelete.RowsAffected(); rowsErr == nil {
		result.DeletedDeliveries = int(count)
	}

	terminalEvents, err := listGatewayEventsForMaintenance(ctx, tx, namespace, terminalCutoff)
	if err != nil {
		return result, err
	}
	retentionWindow := max(now.Sub(terminalCutoff), 0)
	tombstoneExpiresAt := now.Add(retentionWindow)
	affectedSessions := map[gatewaySessionKey]struct{}{}
	for i := range terminalEvents {
		event := &terminalEvents[i]
		upsert, upsertErr := tx.ExecContext(ctx, `INSERT INTO gateway_event_tombstones (
			namespace, gateway_uid, external_event_id, event_id, task_name, task_uid, envelope_digest, session_name, transcript_order, expires_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(namespace, gateway_uid, external_event_id) DO UPDATE SET
			event_id = excluded.event_id, task_name = excluded.task_name, task_uid = excluded.task_uid,
			envelope_digest = excluded.envelope_digest, session_name = excluded.session_name,
			transcript_order = excluded.transcript_order, expires_at = excluded.expires_at, created_at = excluded.created_at`,
			event.Namespace, event.GatewayUID, event.ExternalEventID, event.ID, event.TaskName, event.TaskUID,
			store.GatewayEventEnvelopeDigest(event), event.SessionName, event.TranscriptOrder,
			tombstoneExpiresAt, now)
		if upsertErr != nil {
			return result, upsertErr
		}
		if count, rowsErr := upsert.RowsAffected(); rowsErr == nil {
			result.UpsertedTombstones += int(count)
		}
		if event.SessionName == "" {
			continue
		}
		messageDelete, deleteErr := tx.ExecContext(ctx, `DELETE FROM session_messages
			WHERE namespace = ? AND session_name = ?
			  AND (message_id IN (?, ?, ?) OR (source_type = 'gateway-event' AND source_ref = ?))`,
			event.Namespace, event.SessionName,
			store.GatewayUserMessageID(event.ID), store.GatewayAssistantMessageID(event.ID), store.GatewayErrorMessageID(event.ID),
			event.ID)
		if deleteErr != nil {
			return result, deleteErr
		}
		if count, rowsErr := messageDelete.RowsAffected(); rowsErr == nil {
			result.DeletedSessionMessages += int(count)
		}
		affectedSessions[gatewaySessionKey{Namespace: event.Namespace, Name: event.SessionName}] = struct{}{}
	}

	eventDelete, err := tx.ExecContext(ctx, `DELETE FROM gateway_events AS event WHERE (? = '' OR event.namespace = ?)
		AND event.updated_at < ?
		AND event.state IN (?, ?, ?, ?)
		AND (event.state <> ? OR event.delivery_id <> '')
		AND NOT EXISTS (SELECT 1 FROM gateway_deliveries delivery
			WHERE delivery.namespace = event.namespace AND delivery.event_id = event.id)`, namespace, namespace, terminalCutoff,
		store.GatewayEventCompleted, store.GatewayEventRejected, store.GatewayEventDeadLettered, store.GatewayEventExpired,
		store.GatewayEventExpired)
	if err != nil {
		return result, err
	}
	if count, rowsErr := eventDelete.RowsAffected(); rowsErr == nil {
		result.DeletedEvents = int(count)
	}

	for session := range affectedSessions {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET message_count = (
			SELECT COUNT(*) FROM session_messages WHERE namespace = ? AND session_name = ?
		) WHERE namespace = ? AND name = ? AND session_type = ?`,
			session.Namespace, session.Name, session.Namespace, session.Name, store.SessionTypeGateway); err != nil {
			return result, err
		}
		sessionDelete, err := tx.ExecContext(ctx, `DELETE FROM sessions AS session
			WHERE session.namespace = ? AND session.name = ? AND session.session_type = ?
			  AND session.active_task = '' AND session.updated_at < ?
			  AND NOT EXISTS (SELECT 1 FROM session_messages message
				WHERE message.namespace = session.namespace AND message.session_name = session.name)
			  AND NOT EXISTS (SELECT 1 FROM gateway_events event
				WHERE event.namespace = session.namespace AND event.session_name = session.name)
			  AND NOT EXISTS (SELECT 1 FROM gateway_deliveries delivery
				WHERE delivery.namespace = session.namespace AND delivery.session_name = session.name)`,
			session.Namespace, session.Name, store.SessionTypeGateway, terminalCutoff)
		if err != nil {
			return result, err
		}
		deleted, rowsErr := sessionDelete.RowsAffected()
		if rowsErr != nil {
			return result, rowsErr
		}
		if deleted == 0 {
			continue
		}
		result.DeletedSessions += int(deleted)
		if _, err := tx.ExecContext(ctx, `UPDATE execution_events SET session_name = '', session_seq = 0
			WHERE namespace = ? AND session_name = ?`, session.Namespace, session.Name); err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM execution_event_session_sequences
			WHERE namespace = ? AND session_name = ?`, session.Namespace, session.Name); err != nil {
			return result, err
		}
	}

	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

type gatewaySessionKey struct {
	Namespace string
	Name      string
}

func listGatewayEventsForMaintenance(
	ctx context.Context, tx *sql.Tx, namespace string, terminalCutoff time.Time,
) ([]store.GatewayEvent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+gatewayEventColumns+` FROM gateway_events AS event
		WHERE (? = '' OR event.namespace = ?) AND event.updated_at < ?
		  AND event.state IN (?, ?, ?, ?)
		  AND (event.state <> ? OR event.delivery_id <> '')
		  AND NOT EXISTS (SELECT 1 FROM gateway_deliveries delivery
			WHERE delivery.namespace = event.namespace AND delivery.event_id = event.id)
		ORDER BY event.updated_at, event.id`, namespace, namespace, terminalCutoff,
		store.GatewayEventCompleted, store.GatewayEventRejected, store.GatewayEventDeadLettered, store.GatewayEventExpired,
		store.GatewayEventExpired)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	gatewayEvents := []store.GatewayEvent{}
	for rows.Next() {
		event, scanErr := scanGatewayEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		gatewayEvents = append(gatewayEvents, *event)
	}
	return gatewayEvents, rows.Err()
}

func createGatewayDeliveryTx(ctx context.Context, tx *sql.Tx, delivery *store.GatewayDelivery) (*store.GatewayDelivery, bool, error) {
	if existing, err := getGatewayDeliveryQuery(ctx, tx, delivery.Namespace, delivery.ID); err == nil {
		if !gatewayDeliveriesHaveSameOperation(existing, delivery) {
			return nil, false, store.ErrDuplicateMismatch
		}
		return existing, false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	metadataJSON, err := marshalStringMap(delivery.Metadata)
	if err != nil {
		return nil, false, err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO gateway_deliveries (
		id, idempotency_id, namespace, namespace_uid, gateway_uid, gateway_generation, gateway_name, binding_name, event_id, task_name,
		session_name, kind, state, account_id, context_id, thread_id, reply_target, text, metadata_json,
		attempt_count, max_attempts, manual_retry_count, next_attempt_at, expires_at, provider_message_id, trace_parent, trace_state,
		last_error, claim_owner, claim_until, created_at, updated_at, delivered_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		delivery.ID, delivery.IdempotencyID, delivery.Namespace, delivery.NamespaceUID, delivery.GatewayUID, delivery.GatewayGeneration, delivery.GatewayName,
		delivery.BindingName, delivery.EventID, delivery.TaskName, delivery.SessionName, delivery.Kind,
		delivery.State, delivery.AccountID, delivery.ContextID, delivery.ThreadID, delivery.ReplyTarget,
		delivery.Text, metadataJSON, delivery.AttemptCount, delivery.MaxAttempts, delivery.ManualRetryCount,
		delivery.NextAttemptAt, delivery.ExpiresAt, delivery.ProviderMessageID, delivery.TraceParent, delivery.TraceState, delivery.LastError,
		delivery.ClaimOwner, nullableTime(delivery.ClaimUntil), delivery.CreatedAt, delivery.UpdatedAt,
		nullableTime(delivery.DeliveredAt),
	)
	if err != nil {
		return nil, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if rows == 0 {
		existing, err := getGatewayDeliveryByIdempotencyQuery(
			ctx, tx, delivery.Namespace, delivery.IdempotencyID,
		)
		if err == nil && !gatewayDeliveriesHaveSameOperation(existing, delivery) {
			return nil, false, store.ErrDuplicateMismatch
		}
		return existing, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gateway_events SET delivery_id = ?, updated_at = ?
		WHERE namespace = ? AND id = ?`, delivery.ID, delivery.UpdatedAt.UTC(), delivery.Namespace, delivery.EventID); err != nil {
		return nil, false, err
	}
	copy := *delivery
	return &copy, true, nil
}

func gatewayDeliveriesHaveSameOperation(left, right *store.GatewayDelivery) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.ID == right.ID && left.IdempotencyID == right.IdempotencyID &&
		left.Namespace == right.Namespace && left.NamespaceUID == right.NamespaceUID &&
		left.GatewayUID == right.GatewayUID && left.GatewayGeneration == right.GatewayGeneration &&
		left.GatewayName == right.GatewayName && left.BindingName == right.BindingName &&
		left.EventID == right.EventID && left.TaskName == right.TaskName && left.SessionName == right.SessionName &&
		left.Kind == right.Kind && left.AccountID == right.AccountID && left.ContextID == right.ContextID &&
		left.ThreadID == right.ThreadID && left.ReplyTarget == right.ReplyTarget && left.Text == right.Text &&
		maps.Equal(left.Metadata, right.Metadata) && left.MaxAttempts == right.MaxAttempts &&
		left.ExpiresAt.Equal(right.ExpiresAt) && left.TraceParent == right.TraceParent && left.TraceState == right.TraceState
}

func getGatewayEventQuery(ctx context.Context, q queryRower, namespace, id string) (*store.GatewayEvent, error) {
	row := q.QueryRowContext(ctx, `SELECT `+gatewayEventColumns+` FROM gateway_events WHERE namespace = ? AND id = ?`, namespace, id)
	event, err := scanGatewayEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return event, err
}

type gatewayEventTombstone struct {
	EventID         string
	EnvelopeDigest  string
	SessionName     string
	TranscriptOrder int64
}

func getGatewayEventTombstoneQuery(
	ctx context.Context, q queryRower, namespace, gatewayUID, externalID string, now time.Time,
) (*gatewayEventTombstone, error) {
	tombstone := &gatewayEventTombstone{}
	err := q.QueryRowContext(ctx, `SELECT event_id, envelope_digest, session_name, transcript_order
		FROM gateway_event_tombstones
		WHERE namespace = ? AND gateway_uid = ? AND external_event_id = ? AND expires_at > ?`,
		namespace, gatewayUID, externalID, now.UTC()).Scan(
		&tombstone.EventID, &tombstone.EnvelopeDigest, &tombstone.SessionName, &tombstone.TranscriptOrder,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return tombstone, err
}

func getGatewayEventByExternalIDQuery(ctx context.Context, q queryRower, namespace, gatewayUID, externalID string) (*store.GatewayEvent, error) {
	row := q.QueryRowContext(ctx, `SELECT `+gatewayEventColumns+` FROM gateway_events
		WHERE namespace = ? AND gateway_uid = ? AND external_event_id = ?`, namespace, gatewayUID, externalID)
	event, err := scanGatewayEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return event, err
}

func getGatewayDeliveryQuery(ctx context.Context, q queryRower, namespace, id string) (*store.GatewayDelivery, error) {
	row := q.QueryRowContext(ctx, `SELECT `+gatewayDeliveryColumns+` FROM gateway_deliveries WHERE namespace = ? AND id = ?`, namespace, id)
	delivery, err := scanGatewayDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return delivery, err
}

func getGatewayDeliveryByIdempotencyQuery(
	ctx context.Context,
	q queryRower,
	namespace, idempotencyID string,
) (*store.GatewayDelivery, error) {
	row := q.QueryRowContext(ctx, `SELECT `+gatewayDeliveryColumns+` FROM gateway_deliveries
		WHERE namespace = ? AND idempotency_id = ?`, namespace, idempotencyID)
	delivery, err := scanGatewayDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return delivery, err
}

func getGatewayDeliveryByEventQuery(ctx context.Context, q queryRower, namespace, eventID string) (*store.GatewayDelivery, error) {
	row := q.QueryRowContext(ctx, `SELECT `+gatewayDeliveryColumns+` FROM gateway_deliveries
		WHERE namespace = ? AND event_id = ? ORDER BY created_at, id LIMIT 1`, namespace, eventID)
	delivery, err := scanGatewayDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return delivery, err
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type gatewayRowScanner interface {
	Scan(...any) error
}

func scanGatewayEvent(row gatewayRowScanner) (*store.GatewayEvent, error) {
	var event store.GatewayEvent
	var metadataJSON string
	var taskRuntimeAllowedToolsJSON sql.NullString
	var claimUntil, occurredAt, completedAt sql.NullTime
	if err := row.Scan(
		&event.ID, &event.Namespace, &event.NamespaceUID, &event.GatewayUID, &event.GatewayGeneration, &event.GatewayName, &event.BindingName,
		&event.BindingUID, &event.BindingGeneration, &event.AgentName, &event.AgentUID, &event.ExternalEventID, &event.ProtocolVersion, &event.EventType, &event.State, &event.StateMessage,
		&event.AccountID, &event.ContextID, &event.ThreadID, &event.SenderID, &event.SenderDisplayName,
		&event.Text, &event.ReplyTarget, &metadataJSON, &event.SessionName, &event.TaskName, &event.TaskUID, &taskRuntimeAllowedToolsJSON,
		&event.DeliveryID, &event.ProviderMessageID, &event.TraceParent, &event.TraceState,
		&event.TranscriptOrder, &event.AttemptCount, &event.ClaimOwner, &claimUntil, &event.NextAttemptAt, &occurredAt,
		&event.ReceivedAt, &event.ExpiresAt, &event.CreatedAt, &event.UpdatedAt, &completedAt,
	); err != nil {
		return nil, err
	}
	if err := unmarshalStringMap(metadataJSON, &event.Metadata); err != nil {
		return nil, err
	}
	if taskRuntimeAllowedToolsJSON.Valid {
		if err := json.Unmarshal([]byte(taskRuntimeAllowedToolsJSON.String), &event.TaskAllowedTools); err != nil {
			return nil, fmt.Errorf("decode Gateway Task runtime allowedTools: %w", err)
		}
		if event.TaskAllowedTools == nil {
			return nil, fmt.Errorf("decode Gateway Task runtime allowedTools: frozen value must be an explicit list")
		}
		event.TaskPolicyFrozen = true
	}
	event.ClaimUntil = timePtr(claimUntil)
	event.OccurredAt = timePtr(occurredAt)
	event.CompletedAt = timePtr(completedAt)
	return &event, nil
}

func scanGatewayDelivery(row gatewayRowScanner) (*store.GatewayDelivery, error) {
	var delivery store.GatewayDelivery
	var metadataJSON string
	var claimUntil, deliveredAt sql.NullTime
	if err := row.Scan(
		&delivery.ID, &delivery.IdempotencyID, &delivery.Namespace, &delivery.NamespaceUID, &delivery.GatewayUID, &delivery.GatewayGeneration,
		&delivery.GatewayName, &delivery.BindingName, &delivery.EventID, &delivery.TaskName,
		&delivery.SessionName, &delivery.Kind, &delivery.State, &delivery.AccountID, &delivery.ContextID,
		&delivery.ThreadID, &delivery.ReplyTarget, &delivery.Text, &metadataJSON, &delivery.AttemptCount,
		&delivery.MaxAttempts, &delivery.ManualRetryCount, &delivery.NextAttemptAt, &delivery.ExpiresAt,
		&delivery.ProviderMessageID, &delivery.TraceParent, &delivery.TraceState, &delivery.LastError, &delivery.ClaimOwner, &claimUntil,
		&delivery.CreatedAt, &delivery.UpdatedAt, &deliveredAt,
	); err != nil {
		return nil, err
	}
	if err := unmarshalStringMap(metadataJSON, &delivery.Metadata); err != nil {
		return nil, err
	}
	delivery.ClaimUntil = timePtr(claimUntil)
	delivery.DeliveredAt = timePtr(deliveredAt)
	return &delivery, nil
}

func validateGatewayEvent(event *store.GatewayEvent) error {
	if event == nil {
		return store.ValidationErrorf("gateway event is required")
	}
	for name, value := range map[string]string{
		"id":              event.ID,
		"namespace":       event.Namespace,
		"namespaceUid":    event.NamespaceUID,
		"gatewayUid":      event.GatewayUID,
		"gatewayName":     event.GatewayName,
		"externalEventId": event.ExternalEventID,
		"protocolVersion": event.ProtocolVersion,
		"eventType":       event.EventType,
		"accountId":       event.AccountID,
		"contextId":       event.ContextID,
		"senderId":        event.SenderID,
	} {
		if strings.TrimSpace(value) == "" {
			return store.ValidationErrorf("gateway event %s is required", name)
		}
	}
	if event.GatewayGeneration <= 0 {
		return store.ValidationErrorf("gateway event gatewayGeneration must be positive")
	}
	return nil
}

func validateGatewayDelivery(delivery *store.GatewayDelivery) error {
	if delivery == nil {
		return store.ValidationErrorf("gateway delivery is required")
	}
	for name, value := range map[string]string{
		"id":            delivery.ID,
		"idempotencyId": delivery.IdempotencyID,
		"namespace":     delivery.Namespace,
		"namespaceUid":  delivery.NamespaceUID,
		"gatewayUid":    delivery.GatewayUID,
		"gatewayName":   delivery.GatewayName,
		"eventId":       delivery.EventID,
		"kind":          delivery.Kind,
		"accountId":     delivery.AccountID,
		"contextId":     delivery.ContextID,
		"replyTarget":   delivery.ReplyTarget,
		"text":          delivery.Text,
	} {
		if strings.TrimSpace(value) == "" {
			return store.ValidationErrorf("gateway delivery %s is required", name)
		}
	}
	if delivery.GatewayGeneration <= 0 {
		return store.ValidationErrorf("gateway delivery gatewayGeneration must be positive")
	}
	delivery.LastError = sanitizeGatewayStoreText(delivery.LastError, 1024)
	delivery.ProviderMessageID = sanitizeGatewayStoreText(delivery.ProviderMessageID, 256)
	if delivery.State == "" {
		delivery.State = store.GatewayDeliveryPending
	}
	if !store.IsValidGatewayDeliveryState(delivery.State) {
		return store.ValidationErrorf("unsupported gateway delivery state %q", delivery.State)
	}
	if delivery.Kind != gatewayDeliveryKindFinal && delivery.Kind != gatewayDeliveryKindError {
		return store.ValidationErrorf("unsupported gateway delivery kind %q", delivery.Kind)
	}
	delivery.Metadata = normalizeGatewayStoreMetadata(delivery.Metadata)
	if delivery.MaxAttempts <= 0 {
		delivery.MaxAttempts = 10
	}
	now := delivery.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
		delivery.CreatedAt = now
	}
	if delivery.UpdatedAt.IsZero() {
		delivery.UpdatedAt = now
	}
	if delivery.NextAttemptAt.IsZero() {
		delivery.NextAttemptAt = now
	}
	if delivery.ExpiresAt.IsZero() {
		return store.ValidationErrorf("gateway delivery expiresAt is required")
	}
	return nil
}

func marshalStringMap(value map[string]string) (string, error) {
	if len(value) == 0 {
		return "{}", nil
	}
	safe := make(map[string]string, len(value))
	for key, item := range value {
		safe[key] = sanitizeGatewayStoreText(item, 256)
	}
	data, err := json.Marshal(safe)
	if err != nil {
		return "", fmt.Errorf("marshal metadata: %w", err)
	}
	return string(data), nil
}

func unmarshalStringMap(value string, target *map[string]string) error {
	if value == "" || value == "{}" {
		return nil
	}
	if err := json.Unmarshal([]byte(value), target); err != nil {
		return fmt.Errorf("unmarshal metadata: %w", err)
	}
	return nil
}

func cloneStringMap(value map[string]string) map[string]string {
	clone := make(map[string]string, len(value)+3)
	maps.Copy(clone, value)
	return clone
}

func gatewayUserMessageID(eventID string) string {
	return store.GatewayUserMessageID(eventID)
}

func appendGatewayStringFilter(query string, args []any, column, value string) (string, []any) {
	if strings.TrimSpace(value) != "" {
		query += ` AND ` + column + ` = ?`
		args = append(args, value)
	}
	return query, args
}

func appendGatewayStringSetFilter(query string, args []any, column string, values []string) (string, []any) {
	values = compactStrings(values)
	if len(values) == 0 {
		return query, args
	}
	placeholders := make([]string, 0, len(values))
	for _, value := range values {
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}
	return query + ` AND ` + column + ` IN (` + strings.Join(placeholders, ",") + `)`, args
}

func boundedGatewayLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func sanitizeGatewayStoreText(value string, limit int) string {
	value = redact.SensitiveText(strings.TrimSpace(value))
	if limit > 0 && len(value) > limit {
		value = value[:limit]
	}
	return value
}

func nullableTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return value.UTC()
}

func timePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

type sqliteNullableTime struct {
	value *time.Time
}

func (value *sqliteNullableTime) Scan(src any) error {
	switch typed := src.(type) {
	case nil:
		value.value = nil
		return nil
	case time.Time:
		parsed := typed.UTC()
		value.value = &parsed
		return nil
	case string:
		return value.parse(typed)
	case []byte:
		return value.parse(string(typed))
	default:
		return fmt.Errorf("unsupported SQLite timestamp type %T", src)
	}
}

func (value *sqliteNullableTime) parse(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		value.value = nil
		return nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			parsed = parsed.UTC()
			value.value = &parsed
			return nil
		}
	}
	return fmt.Errorf("invalid SQLite timestamp %q", sanitizeGatewayStoreText(raw, 128))
}

func (value sqliteNullableTime) Ptr() *time.Time {
	if value.value == nil {
		return nil
	}
	copy := *value.value
	return &copy
}

func prefixedColumns(prefix, columns string) string {
	parts := strings.Split(columns, ",")
	for i, part := range parts {
		parts[i] = prefix + "." + strings.TrimSpace(part)
	}
	return strings.Join(parts, ", ")
}
