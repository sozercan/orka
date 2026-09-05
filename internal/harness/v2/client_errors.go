package v2

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrClientConfiguration marks an invalid client configuration or a missing
	// credential required for an authenticated operation.
	ErrClientConfiguration = errors.New("harness v2 client configuration error")
	// ErrClientValidation marks a locally rejected request or identifier.
	ErrClientValidation = errors.New("harness v2 client validation error")
	// ErrClientTransport marks an HTTP transport failure before a response was
	// available.
	ErrClientTransport = errors.New("harness v2 client transport error")
	// ErrClientProtocol marks a malformed, oversized, version-skewed, or otherwise
	// contract-incompatible HTTP response.
	ErrClientProtocol = errors.New("harness v2 client protocol error")
	// ErrClientStream marks a prompt stream failure after the response was opened.
	ErrClientStream = errors.New("harness v2 client stream error")
	// ErrResponseBodyTooLarge marks a bounded JSON or error body overflow.
	ErrResponseBodyTooLarge = errors.New("harness v2 response body too large")
	// ErrPromptStreamDisconnected marks a non-EOF transport failure while reading
	// the non-reconnectable prompt stream.
	ErrPromptStreamDisconnected = errors.New("harness v2 prompt stream disconnected")
)

type ClientErrorKind string

const (
	ClientErrorConfiguration ClientErrorKind = "configuration"
	ClientErrorValidation    ClientErrorKind = "validation"
	ClientErrorTransport     ClientErrorKind = "transport"
	ClientErrorHTTP          ClientErrorKind = "http"
	ClientErrorProtocol      ClientErrorKind = "protocol"
	ClientErrorStream        ClientErrorKind = "stream"
)

// RequestWriteState is conservative transport evidence for a mutating request.
// It never treats an unknown/custom RoundTripper as proof that a request was not
// accepted.
type RequestWriteState string

const (
	RequestWriteUnknown   RequestWriteState = "unknown"
	RequestWriteZeroBytes RequestWriteState = "zero_bytes_written"
	RequestWriteComplete  RequestWriteState = "request_written"
	RequestWriteAmbiguous RequestWriteState = "request_write_ambiguous"
)

// RequestWriteEvidence captures only observations exposed by net/http and
// httptrace. ZeroBytes is reported only for the standard net/http transport when
// no headers were reported written, no body bytes were consumed, and the full
// request was not reported written. All other incomplete writes are ambiguous.
type RequestWriteEvidence struct {
	State                RequestWriteState
	RequestBodyBytesRead int64
	WroteHeaders         bool
	WroteRequest         bool
	WroteRequestError    bool
	GotFirstResponseByte bool
}

// SafeToResendSameIdentity reports whether the transport proved that no request
// bytes were written. It never authorizes allocating a new prompt identity.
func (e RequestWriteEvidence) SafeToResendSameIdentity() bool {
	return e.State == RequestWriteZeroBytes
}

// ClientError is the exact typed error returned by the v2 client. HTTP protocol
// errors retain the validated v2 code and replay classification. Authentication
// values are never included in Message or Error().
type ClientError struct {
	Operation      string
	Kind           ClientErrorKind
	StatusCode     int
	Code           ErrorCode
	Classification *Classification
	Retryable      bool
	Message        string
	WriteEvidence  RequestWriteEvidence
	cause          error
}

func (e *ClientError) Error() string {
	if e == nil {
		return "<nil>"
	}
	parts := []string{"harness v2 " + e.Operation + " failed"}
	if e.Kind != "" {
		parts = append(parts, "kind="+string(e.Kind))
	}
	if e.StatusCode != 0 {
		parts = append(parts, fmt.Sprintf("status=%d", e.StatusCode))
	}
	if e.Code != "" {
		parts = append(parts, "code="+string(e.Code))
	}
	if e.Classification != nil {
		parts = append(parts, "classification="+string(e.Classification.Class))
	}
	if e.WriteEvidence.State != "" && e.WriteEvidence.State != RequestWriteUnknown {
		parts = append(parts, "write="+string(e.WriteEvidence.State))
	}
	prefix := strings.Join(parts, " ")
	if strings.TrimSpace(e.Message) == "" {
		return prefix
	}
	return prefix + ": " + e.Message
}

func (e *ClientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *ClientError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrClientConfiguration:
		return e.Kind == ClientErrorConfiguration
	case ErrClientValidation:
		return e.Kind == ClientErrorValidation
	case ErrClientTransport:
		return e.Kind == ClientErrorTransport
	case ErrClientProtocol:
		return e.Kind == ClientErrorProtocol
	case ErrClientStream:
		return e.Kind == ClientErrorStream
	}
	return errors.Is(e.cause, target)
}

func clientError(operation string, kind ClientErrorKind, message string, cause error) *ClientError {
	return &ClientError{Operation: operation, Kind: kind, Message: message, cause: cause}
}

type retryablePreMutationError struct{ err error }

func (e *retryablePreMutationError) Error() string { return e.err.Error() }
func (e *retryablePreMutationError) Unwrap() error { return e.err }

// MarkPreMutationRetryable marks a before-mutation validation failure as
// transient. The client still rejects the mutation locally and reports that no
// request bytes were written; callers may retry the same sealed identity while
// its deadline remains valid.
func MarkPreMutationRetryable(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*retryablePreMutationError](err); ok {
		return err
	}
	return &retryablePreMutationError{err: err}
}

func preMutationRetryable(err error) bool {
	var marked *retryablePreMutationError
	return errors.As(err, &marked)
}

func classificationCode(class RequestClassification) ErrorCode {
	switch class {
	case RequestClassificationStaleFence:
		return ErrorCodeStaleFence
	case RequestClassificationExpired:
		return ErrorCodeExpired
	case RequestClassificationDigestConflict:
		return ErrorCodeDigestConflict
	case RequestClassificationAlreadyAccepted:
		return ErrorCodeAlreadyAccepted
	case RequestClassificationSettled:
		return ErrorCodeSettled
	default:
		return ErrorCodeInvalidRequest
	}
}
