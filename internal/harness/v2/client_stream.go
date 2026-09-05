package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// PromptStream is one non-reconnectable prompt response. Decode must be called
// until io.EOF so the client can prove that the stream contained accepted and
// exactly one terminal event with no trailing frames. A transport disconnect is
// never converted into a reconnect or replay.
type PromptStream struct {
	client     *Client
	ctx        context.Context
	cancel     context.CancelFunc
	body       io.ReadCloser
	decoder    *EventDecoder
	trace      *requestTrace
	capability string

	decodeActive atomic.Bool
	closeOnce    sync.Once
	stateMu      sync.Mutex
	closed       bool
	finished     bool
	accepted     bool
	eventCount   uint64
	terminal     *Event
}

// PromptStreamSummary is a stable snapshot after or during stream processing.
type PromptStreamSummary struct {
	Accepted      bool
	EventCount    uint64
	Terminal      *Event
	WriteEvidence RequestWriteEvidence
}

// StartPrompt opens a single v2 NDJSON prompt stream. It deliberately has no
// reconnect parameter and performs no automatic retry.
func (c *Client) StartPrompt(ctx context.Context, sessionID RuntimeSessionID, request StartPromptRequest) (*PromptStream, error) {
	const operation = "start_prompt"
	if err := c.requireMutationAuth(operation); err != nil {
		return nil, err
	}
	limits := c.protocolLimits()
	now := time.Now().UTC()
	minLease := time.Duration(limits.MinPromptLeaseMillis) * time.Millisecond
	maxLease := time.Duration(limits.MaxPromptLeaseMillis) * time.Millisecond
	if err := request.ValidateAt(now, minLease, maxLease); err != nil {
		return nil, c.validationError(operation, err)
	}
	relative, err := PromptPath(sessionID, request.Metadata.PromptID)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	if err := c.validateBeforeMutation(ctx, operation, request.Metadata.ExpiresAt); err != nil {
		if clientErr, ok := errors.AsType[*ClientError](err); ok {
			copy := *clientErr
			copy.WriteEvidence = RequestWriteEvidence{State: RequestWriteZeroBytes}
			err = &copy
		}
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, c.validationError(operation, fmt.Errorf("encode request: %w", err))
	}
	if len(payload) > limits.MaxRequestBytes {
		return nil, c.validationError(operation, fmt.Errorf("request body is %d bytes, limit %d", len(payload), limits.MaxRequestBytes))
	}
	capability, err := SignOperationCapability(c.capabilitySecret, ClaimsForMutation(request.Metadata))
	if err != nil {
		return nil, c.validationError(operation, fmt.Errorf("sign operation capability: %w", err))
	}
	endpoint, err := c.endpoint(relative)
	if err != nil {
		return nil, c.validationError(operation, err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamContext, cancel := context.WithCancel(ctx)
	tracker := newRequestTrace(c.traceReliable)
	httpRequest, err := newTrackedRequest(streamContext, http.MethodPut, endpoint.String(), payload, tracker)
	if err != nil {
		cancel()
		return nil, c.validationError(operation, err)
	}
	setCommonHeaders(httpRequest, NDJSONMediaType+", application/json")
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+c.controllerBearer)
	httpRequest.Header.Set(OperationCapabilityHeader, capability)

	response, err := c.httpClient.Do(httpRequest)
	if err != nil {
		cancel()
		return nil, c.transportError(operation, streamContext, err, tracker, capability)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close() //nolint:errcheck
		defer cancel()
		return nil, c.decodeHTTPError(operation, response, tracker, capability)
	}

	mediaType, mediaErr := responseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil {
		response.Body.Close() //nolint:errcheck
		cancel()
		return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, mediaErr, tracker.evidence(), capability)
	}
	switch mediaType {
	case NDJSONMediaType:
		eventLimits := EventStreamLimits{
			MaxLineBytes:             limits.MaxEventLineBytes,
			MaxTerminalResultBytes:   limits.MaxTerminalResultBytes,
			MaxBufferedEvents:        limits.MaxBufferedEvents,
			MaxUpdateEventsPerSecond: limits.MaxUpdateEventsPerSecond,
		}
		decoder, err := NewEventDecoder(response.Body, eventLimits, EventExpectationFromMetadata(request.Metadata))
		if err != nil {
			response.Body.Close() //nolint:errcheck
			cancel()
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, err, tracker.evidence(), capability)
		}
		return &PromptStream{
			client: c, ctx: streamContext, cancel: cancel, body: response.Body,
			decoder: decoder, trace: tracker, capability: capability,
		}, nil
	case "application/json":
		defer response.Body.Close() //nolint:errcheck
		defer cancel()
		body, err := readBoundedResponseBody(response, c.maxErrorBodyBytes)
		if err != nil {
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, err, tracker.evidence(), capability)
		}
		if errorResponse, ok, err := decodeErrorEnvelope(body); err != nil {
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, err, tracker.evidence(), capability)
		} else if ok {
			return nil, c.httpError(operation, response.StatusCode, errorResponse, tracker.evidence(), capability)
		}
		var admission PromptAdmissionResponse
		if err := decodeSuccessJSON(body, &admission); err != nil {
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, err, tracker.evidence(), capability)
		}
		if err := admission.Validate(); err != nil {
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, err, tracker.evidence(), capability)
		}
		if admission.Classification.Class == RequestClassificationFresh {
			return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, fmt.Errorf("fresh prompt admission returned JSON instead of NDJSON stream"), tracker.evidence(), capability)
		}
		errorResponse := ErrorResponse{
			Protocol:       ProtocolVersion,
			Code:           classificationCode(admission.Classification.Class),
			Message:        string(admission.Classification.Class),
			Classification: &admission.Classification,
			Retryable:      false,
		}
		return nil, c.httpError(operation, response.StatusCode, errorResponse, tracker.evidence(), capability)
	default:
		response.Body.Close() //nolint:errcheck
		cancel()
		return nil, c.protocolErrorWithEvidence(operation, response.StatusCode, fmt.Errorf("response Content-Type %q is unsupported; want %q", mediaType, NDJSONMediaType), tracker.evidence(), capability)
	}
}

// StreamPrompt consumes a complete prompt stream and invokes emit for each
// validated event. It returns only after clean EOF following exactly one
// terminal event.
func (c *Client) StreamPrompt(
	ctx context.Context,
	sessionID RuntimeSessionID,
	request StartPromptRequest,
	emit func(Event) error,
) (PromptStreamSummary, error) {
	if emit == nil {
		return PromptStreamSummary{}, c.validationError("stream_prompt", fmt.Errorf("emit callback is required"))
	}
	stream, err := c.StartPrompt(ctx, sessionID, request)
	if err != nil {
		if clientErr, ok := errors.AsType[*ClientError](err); ok {
			return PromptStreamSummary{WriteEvidence: clientErr.WriteEvidence}, err
		}
		return PromptStreamSummary{}, err
	}
	defer stream.Close() //nolint:errcheck
	for {
		event, err := stream.Decode()
		if errors.Is(err, io.EOF) {
			return stream.Summary(), nil
		}
		if err != nil {
			return stream.Summary(), err
		}
		if err := emit(event); err != nil {
			return stream.Summary(), err
		}
	}
}

// Decode returns the next validated event. After the body reaches EOF it calls
// EventStreamValidator.Finish, so EOF is successful only after accepted and one
// terminal event were observed.
func (s *PromptStream) Decode() (Event, error) {
	if s == nil || s.decoder == nil {
		return Event{}, clientError("prompt_stream", ClientErrorConfiguration, "prompt stream is not initialized", ErrClientConfiguration)
	}
	if !s.decodeActive.CompareAndSwap(false, true) {
		return Event{}, clientError("prompt_stream", ClientErrorValidation, "concurrent Decode calls are not supported", ErrClientValidation)
	}
	defer s.decodeActive.Store(false)

	s.stateMu.Lock()
	if s.finished || s.closed {
		s.stateMu.Unlock()
		return Event{}, io.EOF
	}
	s.stateMu.Unlock()

	event, err := s.decoder.Decode()
	if err == nil {
		s.stateMu.Lock()
		s.eventCount++
		if event.Type == EventAccepted {
			s.accepted = true
		}
		if event.Type.IsTerminal() {
			terminal := event
			s.terminal = &terminal
		}
		s.stateMu.Unlock()
		return event, nil
	}
	if errors.Is(err, io.EOF) {
		finishErr := s.decoder.validator.Finish()
		if finishErr != nil {
			s.finish()
			return Event{}, s.streamError(finishErr)
		}
		s.finish()
		return Event{}, io.EOF
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		contextErr := s.ctx.Err()
		s.finish()
		return Event{}, s.streamError(contextErr)
	}
	s.finish()
	if IsPoisoningStreamError(err) {
		return Event{}, s.streamError(err)
	}
	return Event{}, s.streamError(ErrPromptStreamDisconnected)
}

func (s *PromptStream) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.stateMu.Unlock()
		if s.cancel != nil {
			s.cancel()
		}
		if s.body != nil {
			closeErr = s.body.Close()
		}
	})
	return closeErr
}

func (s *PromptStream) Summary() PromptStreamSummary {
	if s == nil {
		return PromptStreamSummary{}
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	var terminal *Event
	if s.terminal != nil {
		copy := *s.terminal
		terminal = &copy
	}
	evidence := RequestWriteEvidence{State: RequestWriteUnknown}
	if s.trace != nil {
		evidence = s.trace.evidence()
	}
	return PromptStreamSummary{
		Accepted: s.accepted, EventCount: s.eventCount, Terminal: terminal, WriteEvidence: evidence,
	}
}

func (s *PromptStream) finish() {
	s.stateMu.Lock()
	s.finished = true
	s.stateMu.Unlock()
	_ = s.Close()
}

func (s *PromptStream) streamError(cause error) error {
	message := cause.Error()
	if s.client != nil {
		message = s.client.redact(message, s.capability)
	}
	evidence := RequestWriteEvidence{State: RequestWriteUnknown}
	if s.trace != nil {
		evidence = s.trace.evidence()
	}
	return &ClientError{
		Operation: "prompt_stream", Kind: ClientErrorStream, StatusCode: http.StatusOK,
		Message: message, WriteEvidence: evidence, cause: cause,
	}
}

func responseMediaType(header string) (string, error) {
	for _, expected := range []string{NDJSONMediaType, "application/json"} {
		if requireMediaType(header, expected) == nil {
			return expected, nil
		}
	}
	return "", fmt.Errorf("response Content-Type %q is unsupported", header)
}
