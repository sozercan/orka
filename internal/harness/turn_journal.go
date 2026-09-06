/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package harness

import (
	"context"
	"fmt"

	"github.com/orka-agents/orka/internal/store"
)

// TurnJournal owns the persisted-frame side of harness turn integration: loading
// durable frame identities, deduplicating mapped frames, and appending new frame
// events to Orka's execution event stream. Controllers remain responsible for
// task status, retry policy, and result storage.
type TurnJournal struct {
	EventStore store.ExecutionEventStore
	MapContext EventMapContext
}

// TurnJournalState is the mutable append state for a single journal pass. It
// hides the invariant that callers must load persisted keys once and then mutate
// that same key set as replayed frames are appended.
type TurnJournalState struct {
	journal TurnJournal
	keys    map[string]struct{}
}

// Open loads the durable mapped-frame keys for the journal's task stream.
func (j TurnJournal) Open(ctx context.Context) (*TurnJournalState, error) {
	keys, err := j.ExistingFrameKeys(ctx)
	if err != nil {
		return nil, err
	}
	return &TurnJournalState{journal: j, keys: keys}, nil
}

// ExistingFrameKeys returns the persisted mapped-frame identity keys for the
// journal's task stream.
func (j TurnJournal) ExistingFrameKeys(ctx context.Context) (map[string]struct{}, error) {
	keys := map[string]struct{}{}
	if err := j.visitMappedFrameIdentities(ctx, func(identity MappedFrameIdentity) (bool, error) {
		keys[identity.Key()] = struct{}{}
		return true, nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

// AppendFrameIfNew maps and appends frame unless this journal pass already saw
// its persisted identity.
func (s *TurnJournalState) AppendFrameIfNew(ctx context.Context, frame HarnessEventFrame) (*store.ExecutionEvent, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("turn journal state is required")
	}
	return s.journal.appendFrameIfNew(ctx, frame, s.keys)
}

func (j TurnJournal) appendFrameIfNew(
	ctx context.Context,
	frame HarnessEventFrame,
	keys map[string]struct{},
) (*store.ExecutionEvent, bool, error) {
	if j.EventStore == nil {
		return nil, false, fmt.Errorf("execution event store is required")
	}
	key := MappedFrameKey(frame)
	if keys != nil {
		if _, ok := keys[key]; ok {
			return nil, false, nil
		}
	}
	mapped, err := MapFrameToExecutionEvent(frame, j.MapContext)
	if err != nil {
		return nil, false, err
	}
	appended, err := j.EventStore.AppendExecutionEvent(ctx, mapped)
	if err != nil {
		return nil, false, fmt.Errorf("append mapped harness event: %w", err)
	}
	if keys != nil {
		keys[key] = struct{}{}
	}
	return appended, true, nil
}

func (j TurnJournal) visitMappedFrameIdentities(
	ctx context.Context,
	visit func(MappedFrameIdentity) (bool, error),
) error {
	if j.EventStore == nil {
		return nil
	}
	if visit == nil {
		return fmt.Errorf("visit callback is required")
	}
	mapCtx, err := j.taskStreamContext()
	if err != nil {
		return err
	}
	var afterSeq int64
	for {
		eventsList, err := j.EventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
			Namespace:  mapCtx.Namespace,
			StreamType: store.ExecutionEventStreamTypeTask,
			StreamID:   mapCtx.StreamID,
			AfterSeq:   afterSeq,
			Limit:      store.MaxExecutionEventLimit,
		})
		if err != nil {
			return fmt.Errorf("list mapped harness events: %w", err)
		}
		if len(eventsList) == 0 {
			return nil
		}
		for _, event := range eventsList {
			if event.Seq > afterSeq {
				afterSeq = event.Seq
			}
			identity, ok := MappedFrameIdentityFromEvent(event)
			if !ok {
				continue
			}
			more, err := visit(identity)
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
		if len(eventsList) < store.MaxExecutionEventLimit {
			return nil
		}
	}
}

func (j TurnJournal) taskStreamContext() (EventMapContext, error) {
	if err := j.MapContext.validate(); err != nil {
		return EventMapContext{}, err
	}
	return j.MapContext.normalized(), nil
}
