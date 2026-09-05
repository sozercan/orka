package v2

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientPromptNDJSONStreamCompletesWithoutReconnect(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "stream-success-op")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var decoded StartPromptRequest
		clientTestDecodeMutation(t, r, &decoded, true)
		writeClientTestNDJSON(w,
			clientTestAcceptedEvent(request, now.Add(time.Millisecond)),
			clientTestCompletedEvent(request, now.Add(2*time.Millisecond)),
		)
	}))
	defer server.Close()

	var eventTypes []EventType
	summary, err := clientTestClient(t, server.URL).StreamPrompt(context.Background(), "runtime-session-1", request, func(event Event) error {
		eventTypes = append(eventTypes, event.Type)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamPrompt() error = %v", err)
	}
	if got, want := eventTypes, []EventType{EventAccepted, EventCompleted}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("event types = %v, want %v", got, want)
	}
	if !summary.Accepted || summary.EventCount != 2 || summary.Terminal == nil || summary.Terminal.Type != EventCompleted {
		t.Fatalf("summary = %#v", summary)
	}
	if summary.WriteEvidence.State != RequestWriteComplete {
		t.Fatalf("write evidence = %#v", summary.WriteEvidence)
	}
}

func TestClientStartPromptPreMutationFailureReportsZeroWriteEvidence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "preflight-zero-write-op")
	transportCalled := false
	client := clientTestClient(t, "http://runtime.invalid",
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			transportCalled = true
			return nil, errors.New("prompt transport must not be called")
		})}),
		WithBeforeMutation(func(context.Context, string) error {
			return errors.New("external runtime capabilities unavailable")
		}),
	)

	_, err := client.StartPrompt(context.Background(), "runtime-session-1", request)
	var clientErr *ClientError
	if !errors.As(err, &clientErr) || !errors.Is(err, ErrClientValidation) {
		t.Fatalf("StartPrompt() error = %v, want validation *ClientError", err)
	}
	if clientErr.WriteEvidence.State != RequestWriteZeroBytes || !clientErr.WriteEvidence.SafeToResendSameIdentity() {
		t.Fatalf("write evidence = %#v, want zero bytes written", clientErr.WriteEvidence)
	}
	if transportCalled {
		t.Fatal("prompt transport was called after pre-mutation validation failed")
	}
}

func TestClientStreamPromptPreservesPreMutationWriteEvidence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "stream-preflight-zero-write-op")
	emitCalled := false
	client := clientTestClient(t, "http://runtime.invalid",
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("prompt transport must not be called")
		})}),
		WithBeforeMutation(func(context.Context, string) error {
			return errors.New("external runtime status unavailable")
		}),
	)

	summary, err := client.StreamPrompt(context.Background(), "runtime-session-1", request, func(Event) error {
		emitCalled = true
		return nil
	})
	var clientErr *ClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("StreamPrompt() error = %v, want *ClientError", err)
	}
	if clientErr.WriteEvidence.State != RequestWriteZeroBytes {
		t.Fatalf("error write evidence = %#v, want zero bytes written", clientErr.WriteEvidence)
	}
	if summary.WriteEvidence.State != RequestWriteZeroBytes || !summary.WriteEvidence.SafeToResendSameIdentity() {
		t.Fatalf("summary write evidence = %#v, want zero bytes written", summary.WriteEvidence)
	}
	if emitCalled {
		t.Fatal("StreamPrompt() called emit after pre-mutation validation failed")
	}
}

func TestClientRejectsMalformedAndOversizedPromptStreams(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "hostile-stream-op")
	tests := []struct {
		name   string
		body   string
		limits ProtocolLimits
		want   error
	}{
		{name: "malformed", body: "{not-json}\n", limits: DefaultProtocolLimits(), want: ErrMalformedEvent},
		{name: "oversized", body: strings.Repeat("x", 129) + "\n", limits: clientTestSmallStreamLimits(), want: ErrEventLineTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", NDJSONMediaType)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client := clientTestClient(t, server.URL, WithProtocolLimits(test.limits))
			stream, err := client.StartPrompt(context.Background(), "runtime-session-1", request)
			if err != nil {
				t.Fatalf("StartPrompt() error = %v", err)
			}
			defer stream.Close() //nolint:errcheck
			_, err = stream.Decode()
			if !errors.Is(err, test.want) {
				t.Fatalf("Decode() error = %v, want %v", err, test.want)
			}
			if !errors.Is(err, ErrClientStream) || !IsPoisoningStreamError(err) {
				t.Fatalf("Decode() error = %v, want poisoning client stream error", err)
			}
		})
	}
}

func TestClientDetectsMidstreamDisconnectAfterAcceptance(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "midstream-op")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", NDJSONMediaType)
		_, _ = io.WriteString(w, mustClientTestJSON(clientTestAcceptedEvent(request, now.Add(time.Millisecond)))+"\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Returning closes the body without the required terminal event.
	}))
	defer server.Close()

	stream, err := clientTestClient(t, server.URL).StartPrompt(context.Background(), "runtime-session-1", request)
	if err != nil {
		t.Fatalf("StartPrompt() error = %v", err)
	}
	defer stream.Close() //nolint:errcheck
	if event, err := stream.Decode(); err != nil || event.Type != EventAccepted {
		t.Fatalf("first Decode() = (%q, %v)", event.Type, err)
	}
	_, err = stream.Decode()
	if !errors.Is(err, ErrMissingTerminalEvent) || !errors.Is(err, ErrClientStream) {
		t.Fatalf("second Decode() error = %v, want missing terminal", err)
	}
	if !IsPoisoningStreamError(err) {
		t.Fatalf("midstream error is not poisoning: %v", err)
	}
	summary := stream.Summary()
	if !summary.Accepted || summary.Terminal != nil {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestClientPromptStreamContextCancellation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestStartPromptRequest(t, now, "context-op")
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		w.Header().Set("Content-Type", NDJSONMediaType)
		_, _ = io.WriteString(w, mustClientTestJSON(clientTestAcceptedEvent(request, now.Add(time.Millisecond)))+"\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := clientTestClient(t, server.URL).StartPrompt(ctx, "runtime-session-1", request)
	if err != nil {
		cancel()
		t.Fatalf("StartPrompt() error = %v", err)
	}
	defer stream.Close() //nolint:errcheck
	if event, err := stream.Decode(); err != nil || event.Type != EventAccepted {
		cancel()
		t.Fatalf("first Decode() = (%q, %v)", event.Type, err)
	}

	result := make(chan error, 1)
	go func() {
		_, decodeErr := stream.Decode()
		result <- decodeErr
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrClientStream) {
			t.Fatalf("Decode() error = %v, want context.Canceled client stream error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Decode() did not unblock after context cancellation")
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not observe context cancellation")
	}
}

func clientTestSmallStreamLimits() ProtocolLimits {
	limits := DefaultProtocolLimits()
	limits.MaxEventLineBytes = 128
	limits.MaxTerminalResultBytes = 64
	return limits
}
