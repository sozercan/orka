package api

import (
	"context"
	"errors"

	"github.com/orka-agents/orka/internal/events"
	forkcontext "github.com/orka-agents/orka/internal/fork"
	"github.com/orka-agents/orka/internal/store"
)

var errTaskTimelineReadLimitExceeded = errors.New("task timeline read limit exceeded")

const taskTimelineContextCompatibilityScanLimit = store.MaxExecutionEventLimit

type taskTimelineReader struct {
	eventStore store.ExecutionEventStore
	namespace  string
	taskName   string
}

func newTaskTimelineReader(eventStore store.ExecutionEventStore, namespace, taskName string) taskTimelineReader {
	return taskTimelineReader{eventStore: eventStore, namespace: namespace, taskName: taskName}
}

func (r taskTimelineReader) listMatching(ctx context.Context, eventTypes []string) ([]store.ExecutionEvent, error) {
	out := []store.ExecutionEvent{}
	var after int64
	for {
		batch, err := r.list(ctx, after, store.MaxExecutionEventLimit, eventTypes)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		out = append(out, batch...)
		after = batch[len(batch)-1].Seq
		if len(batch) < store.MaxExecutionEventLimit {
			break
		}
	}
	return out, nil
}

func (r taskTimelineReader) listThrough(ctx context.Context, throughSeq int64, maxEvents int) ([]store.ExecutionEvent, error) {
	if throughSeq == 0 {
		return nil, nil
	}
	if maxEvents <= 0 {
		return nil, errTaskTimelineReadLimitExceeded
	}
	var out []store.ExecutionEvent
	var after int64
	for {
		batch, err := r.list(ctx, after, store.MaxExecutionEventLimit, nil)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, event := range batch {
			if event.Seq > throughSeq {
				return out, nil
			}
			if len(out) >= maxEvents {
				return nil, errTaskTimelineReadLimitExceeded
			}
			out = append(out, event)
			after = event.Seq
			if after >= throughSeq {
				return out, nil
			}
		}
		if len(batch) < store.MaxExecutionEventLimit {
			break
		}
	}
	return out, nil
}

func (r taskTimelineReader) listRecentContextThrough(ctx context.Context, throughSeq int64, maxEvents int) ([]store.ExecutionEvent, bool, error) {
	if throughSeq == 0 || maxEvents <= 0 {
		return nil, false, nil
	}
	out := make([]store.ExecutionEvent, 0, maxEvents)
	scanLimit := max(maxEvents, taskTimelineContextCompatibilityScanLimit)
	initialAfter := max(throughSeq-int64(scanLimit), 0)
	after := initialAfter
	var firstScanned store.ExecutionEvent
	haveFirstScanned := false
	boundaryModelOpen := false
	var boundaryModelLastSeq int64
	for {
		batch, err := r.list(ctx, after, store.MaxExecutionEventLimit, nil)
		if err != nil {
			return nil, false, err
		}
		if len(batch) == 0 {
			break
		}
		reachedCutoff := false
		for _, event := range batch {
			if event.Seq > throughSeq {
				reachedCutoff = true
				break
			}
			if !haveFirstScanned {
				firstScanned = event
				haveFirstScanned = true
				boundaryModelOpen = event.Type == events.ExecutionEventTypeModelMessage
				if boundaryModelOpen {
					boundaryModelLastSeq = event.Seq
				}
			} else if boundaryModelOpen {
				if forkcontext.SameHarnessV2ModelMessage(firstScanned, event) {
					boundaryModelLastSeq = event.Seq
				} else {
					boundaryModelOpen = false
				}
			}
			out = append(out, event)
			after = event.Seq
		}
		out = forkcontext.CoalesceAdjacentModelMessages(out)
		if len(out) > maxEvents {
			out = append([]store.ExecutionEvent(nil), out[len(out)-maxEvents:]...)
		}
		if reachedCutoff || after >= throughSeq || len(batch) < store.MaxExecutionEventLimit {
			break
		}
	}
	scanTruncated := false
	if initialAfter > 0 {
		earliest, err := r.list(ctx, 0, 1, nil)
		if err != nil {
			return nil, false, err
		}
		scanTruncated = len(earliest) > 0 && earliest[0].Seq <= initialAfter
	}
	if scanTruncated && haveFirstScanned && boundaryModelLastSeq > 0 && len(out) > 0 &&
		out[0].Seq == boundaryModelLastSeq {
		previousAfter := max(firstScanned.Seq-2, 0)
		previous, err := r.list(ctx, previousAfter, 1, nil)
		if err != nil {
			return nil, false, err
		}
		if len(previous) == 1 && previous[0].Seq+1 == firstScanned.Seq &&
			forkcontext.SameHarnessV2ModelMessage(previous[0], firstScanned) {
			out = out[1:]
		}
	}
	return out, scanTruncated, nil
}

func (r taskTimelineReader) seqExists(ctx context.Context, seq, latestSeq int64) (bool, error) {
	if seq == 0 || seq == latestSeq {
		return true, nil
	}
	if seq < 0 || seq > latestSeq {
		return false, nil
	}
	listed, err := r.list(ctx, seq-1, 1, nil)
	if err != nil {
		return false, err
	}
	return len(listed) == 1 && listed[0].Seq == seq, nil
}

func (r taskTimelineReader) terminalThroughCursor(ctx context.Context, cursor int64) (store.ExecutionEvent, bool, error) {
	if cursor <= 0 {
		return store.ExecutionEvent{}, false, nil
	}
	after := int64(0)
	for {
		batch, err := r.list(ctx, after, store.MaxExecutionEventLimit, events.TerminalTaskEventTypes())
		if err != nil {
			return store.ExecutionEvent{}, false, err
		}
		if len(batch) == 0 {
			return store.ExecutionEvent{}, false, nil
		}
		for _, event := range batch {
			if event.Seq > cursor {
				return store.ExecutionEvent{}, false, nil
			}
			if event.Seq > after {
				after = event.Seq
			}
			if events.IsTerminalTaskEventType(event.Type) {
				return event, true, nil
			}
		}
		if len(batch) < store.MaxExecutionEventLimit {
			return store.ExecutionEvent{}, false, nil
		}
	}
}

func (r taskTimelineReader) terminalForCompletion(ctx context.Context, cursor int64) (store.ExecutionEvent, bool, int64, error) {
	after := cursor
	if after > 0 {
		after--
	}
	scannedThrough := cursor
	for {
		batch, err := r.list(ctx, after, store.MaxExecutionEventLimit, nil)
		if err != nil {
			return store.ExecutionEvent{}, false, scannedThrough, err
		}
		if len(batch) == 0 {
			return store.ExecutionEvent{}, false, scannedThrough, nil
		}
		for _, event := range batch {
			if event.Seq > after {
				after = event.Seq
			}
			scannedThrough = max(scannedThrough, event.Seq)
			if events.IsTerminalTaskEventType(event.Type) {
				return event, true, scannedThrough, nil
			}
		}
		if len(batch) < store.MaxExecutionEventLimit {
			return store.ExecutionEvent{}, false, scannedThrough, nil
		}
	}
}

func (r taskTimelineReader) list(ctx context.Context, afterSeq int64, limit int, eventTypes []string) ([]store.ExecutionEvent, error) {
	return r.eventStore.ListExecutionEvents(ctx, store.ExecutionEventFilter{
		Namespace:  r.namespace,
		StreamType: events.ExecutionEventStreamTypeTask,
		StreamID:   r.taskName,
		EventTypes: eventTypes,
		AfterSeq:   afterSeq,
		Limit:      limit,
	})
}
