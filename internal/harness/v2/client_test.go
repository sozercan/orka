package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func WithResponseBodyLimit(maxBytes int64) ClientOption {
	return func(c *Client) error {
		if maxBytes <= 0 || maxBytes > int64(MaxCanonicalJSONBytes) {
			return fmt.Errorf("JSON response limit must be in range 1..%d", MaxCanonicalJSONBytes)
		}
		c.maxJSONResponseBytes = maxBytes
		return nil
	}
}

const clientTestBearer = "controller-bearer-token-0123456789abcdef"

var clientTestCapabilitySecret = []byte("capability-secret-0123456789abcdef")

func TestNewClientDefaultsToProxylessTransport(t *testing.T) {
	client, err := NewClient("http://10.0.0.1:8443")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default client transport = %T, want *http.Transport", client.httpClient.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("default supervisor transport must not resolve environment proxies: authenticated exact-Pod control traffic would traverse HTTP_PROXY")
	}
}

func TestClientControlSurfaceAndAuthentication(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	createRequest := clientTestCreateSessionRequest(t, now, "create-op")
	leaseRequest := clientTestLeaseRequest(t, now, "lease-op")
	permissionRequest := clientTestPermissionRequest(t, now, "permission-op")
	cancelRequest := clientTestCancelRequest(t, now, "cancel-op")
	deltaRequest := clientTestDeltaRequest(t, now, "delta-op")
	publicationFinalizationRequest := clientTestPublicationFinalizationRequest(t, now, "publication-finalization-op")
	drainRequest := clientTestDrainRequest(t, now, "drain-op")
	deleteRequest := clientTestDeleteRequest(t, now, "delete-op")

	var mu sync.Mutex
	seen := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		switch r.Method + " " + r.URL.Path {
		case http.MethodGet + " " + HealthPath:
			if got := r.Header.Get("Authorization"); got != "" {
				t.Errorf("health Authorization = %q, want empty", got)
			}
			writeClientTestJSON(w, http.StatusOK, HealthResponse{Protocol: ProtocolVersion, Status: HealthStatusOK, Timestamp: now})
		case http.MethodGet + " " + CapabilitiesPath:
			if got := r.Header.Get("Authorization"); got != "" {
				t.Errorf("capabilities Authorization = %q, want empty", got)
			}
			writeClientTestJSON(w, http.StatusOK, clientTestCapabilities(t))
		case http.MethodGet + " " + StatusPath:
			clientTestRequireBearer(t, r)
			writeClientTestJSON(w, http.StatusOK, clientTestStatus(t, now))
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1":
			var request CreateRuntimeSessionRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, clientTestCreateSessionResponse(request, now, RequestClassificationFresh))
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1/prompts/prompt-1/lease":
			var request RenewPromptLeaseRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, PromptLeaseResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh}, Lease: request.Lease,
			})
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1/prompts/prompt-1/permissions/permission-1":
			var request ResolvePermissionRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, PermissionResolutionResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				State: PermissionResolutionApplied, Decision: request.Decision, ResolvedAt: now,
			})
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1/prompts/prompt-1/cancel":
			var request CancelPromptRequest
			clientTestDecodeMutation(t, r, &request, true)
			settlement := PromptSettlement{
				TerminalEvent: EventCancelled, Outcome: PromptOutcomeCancelled,
				StopReason: ACPStopReasonCancelled, SettledAt: now,
			}
			writeClientTestJSON(w, http.StatusOK, CancelPromptResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				BarrierState: CancellationBarrierSettled, SettlementProven: true, Settlement: settlement,
			})
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1/workspace-deltas/delta-1":
			var request CreateWorkspaceDeltaRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, CreateWorkspaceDeltaResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				Delta: WorkspaceDeltaDescriptor{
					DeltaID: request.DeltaID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
					SessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration, State: WorkspaceDeltaNoChange,
					Intent: request.Intent, VerifiedBaseline: request.VerifiedBaseline,
					NoFollowVerified: true, PublicationSafe: true, FrozenAt: now,
				},
			})
		case http.MethodPut + " " + "/v2/runtime-sessions/runtime-session-1/publication-finalization":
			var request FinalizeRuntimeSessionPublicationRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, FinalizeRuntimeSessionPublicationResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				Session: RuntimeSessionDescriptor{
					RuntimeSessionID: "runtime-session-1", RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
					Generation: request.Metadata.Fence.RuntimeSessionGeneration, RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID,
					SupervisorBootID: request.Metadata.Fence.SupervisorBootID, RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest,
					State: RuntimeSessionStateFinalizing, ProviderSessionID: "provider-session-1", WorkspaceBaseline: testWorkspaceBaseline(),
					CreatedAt: now, LastTransitionAt: now,
				},
				Finalization: PublicationFinalizationReceipt{
					WorkspaceDeltaID: request.WorkspaceDeltaID, PublicationID: request.PublicationID,
					PublicationGeneration: request.PublicationGeneration, PublicationVersion: request.PublicationVersion,
					TerminalState: request.TerminalState, TerminalReceiptDigest: request.TerminalReceiptDigest, AppliedAt: now,
				},
			})
		case http.MethodPut + " " + DrainPath:
			var request DrainRequest
			clientTestDecodeMutation(t, r, &request, false)
			writeClientTestJSON(w, http.StatusOK, DrainResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				Drain: DrainStatus{AcceptingNewSessions: false, Requested: true, RequestedAt: now, Reason: request.Reason},
			})
		case http.MethodDelete + " " + "/v2/runtime-sessions/runtime-session-1":
			var request DeleteRuntimeSessionRequest
			clientTestDecodeMutation(t, r, &request, true)
			writeClientTestJSON(w, http.StatusOK, DeleteRuntimeSessionResponse{
				Protocol: ProtocolVersion, Classification: Classification{Class: RequestClassificationFresh},
				State: RuntimeSessionStateDeleted,
				Tombstone: RuntimeSessionTombstone{
					RuntimeSessionUID:        request.Metadata.Fence.RuntimeSessionUID,
					RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
					RuntimeProfileDigest:     request.Metadata.Fence.RuntimeProfileDigest, DeletedAt: now,
					Operations: []OperationRecord{{
						OperationID: request.Metadata.OperationID, RequestDigest: request.Metadata.RequestDigest,
						Phase: OperationPhaseDeleted, RecordedAt: now, UpdatedAt: now,
					}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := clientTestClient(t, server.URL)
	ctx := context.Background()
	if _, err := client.Health(ctx); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if _, err := client.Capabilities(ctx); err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if _, err := client.Status(ctx); err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if _, err := client.CreateRuntimeSession(ctx, createRequest); err != nil {
		t.Fatalf("CreateRuntimeSession() error = %v", err)
	}
	if _, err := client.RenewPromptLease(ctx, "runtime-session-1", leaseRequest); err != nil {
		t.Fatalf("RenewPromptLease() error = %v", err)
	}
	if _, err := client.ResolvePermission(ctx, "runtime-session-1", permissionRequest); err != nil {
		t.Fatalf("ResolvePermission() error = %v", err)
	}
	if _, err := client.CancelPrompt(ctx, "runtime-session-1", cancelRequest); err != nil {
		t.Fatalf("CancelPrompt() error = %v", err)
	}
	if _, err := client.CreateWorkspaceDelta(ctx, "runtime-session-1", deltaRequest); err != nil {
		t.Fatalf("CreateWorkspaceDelta() error = %v", err)
	}
	if _, err := client.FinalizeRuntimeSessionPublication(ctx, "runtime-session-1", publicationFinalizationRequest); err != nil {
		t.Fatalf("FinalizeRuntimeSessionPublication() error = %v", err)
	}
	if _, err := client.Drain(ctx, drainRequest); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if _, err := client.DeleteRuntimeSession(ctx, "runtime-session-1", deleteRequest); err != nil {
		t.Fatalf("DeleteRuntimeSession() error = %v", err)
	}

	for _, route := range []string{
		http.MethodGet + " " + HealthPath,
		http.MethodGet + " " + CapabilitiesPath,
		http.MethodGet + " " + StatusPath,
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1",
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1/prompts/prompt-1/lease",
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1/prompts/prompt-1/permissions/permission-1",
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1/prompts/prompt-1/cancel",
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1/workspace-deltas/delta-1",
		http.MethodPut + " /v2/runtime-sessions/runtime-session-1/publication-finalization",
		http.MethodPut + " " + DrainPath,
		http.MethodDelete + " /v2/runtime-sessions/runtime-session-1",
	} {
		mu.Lock()
		count := seen[route]
		mu.Unlock()
		if count != 1 {
			t.Errorf("route %q count = %d, want 1", route, count)
		}
	}
}

func TestClientRejectsVersionSkewAndOversizedJSON(t *testing.T) {
	t.Run("version skew", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeClientTestJSON(w, http.StatusOK, HealthResponse{Protocol: "orka.harness.v3", Status: HealthStatusOK, Timestamp: time.Now().UTC()})
		}))
		defer server.Close()
		client, err := NewClient(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Health(context.Background())
		if !errors.Is(err, ErrClientProtocol) {
			t.Fatalf("Health() error = %v, want ErrClientProtocol", err)
		}
	})

	t.Run("bounded response body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"padding":"`+strings.Repeat("x", 1024)+`"}`)
		}))
		defer server.Close()
		client, err := NewClient(server.URL, WithResponseBodyLimit(128))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Health(context.Background())
		if !errors.Is(err, ErrResponseBodyTooLarge) {
			t.Fatalf("Health() error = %v, want ErrResponseBodyTooLarge", err)
		}
	})
}

func TestClientMapsStaleFenceDigestConflictAndAlreadyAccepted(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "classification-op")
	tests := []struct {
		name      string
		status    int
		response  ErrorResponse
		wantCode  ErrorCode
		wantClass RequestClassification
		wantFence FenceMismatch
	}{
		{
			name: "stale fence", status: http.StatusGone, wantCode: ErrorCodeStaleFence,
			wantClass: RequestClassificationStaleFence, wantFence: FenceMismatchControllerEpoch,
			response: ErrorResponse{
				Protocol: ProtocolVersion, Code: ErrorCodeStaleFence, Message: "stale controller epoch",
				Classification: &Classification{Class: RequestClassificationStaleFence, FenceMismatch: FenceMismatchControllerEpoch},
			},
		},
		{
			name: "digest conflict", status: http.StatusConflict, wantCode: ErrorCodeDigestConflict,
			wantClass: RequestClassificationDigestConflict,
			response: ErrorResponse{
				Protocol: ProtocolVersion, Code: ErrorCodeDigestConflict, Message: "digest differs",
				Classification: &Classification{Class: RequestClassificationDigestConflict, Phase: OperationPhaseRecorded},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeClientTestJSON(w, test.status, test.response)
			}))
			defer server.Close()
			client := clientTestClient(t, server.URL)
			_, err := client.CreateRuntimeSession(context.Background(), request)
			var clientErr *ClientError
			if !errors.As(err, &clientErr) {
				t.Fatalf("error = %v, want *ClientError", err)
			}
			if clientErr.Kind != ClientErrorHTTP || clientErr.StatusCode != test.status || clientErr.Code != test.wantCode {
				t.Fatalf("ClientError = %#v", clientErr)
			}
			if clientErr.Classification == nil || clientErr.Classification.Class != test.wantClass || clientErr.Classification.FenceMismatch != test.wantFence {
				t.Fatalf("classification = %#v", clientErr.Classification)
			}
		})
	}

	t.Run("workspace resume loss is typed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeClientTestJSON(w, http.StatusConflict, ErrorResponse{
				Protocol: ProtocolVersion,
				Code:     ErrorCodeWorkspaceResumeLost,
				Message:  "runtime session failed during durable resume verification",
			})
		}))
		defer server.Close()
		_, err := clientTestClient(t, server.URL).CreateRuntimeSession(context.Background(), request)
		var clientErr *ClientError
		if !errors.As(err, &clientErr) || clientErr.Kind != ClientErrorHTTP ||
			clientErr.StatusCode != http.StatusConflict || clientErr.Code != ErrorCodeWorkspaceResumeLost ||
			clientErr.Retryable {
			t.Fatalf("CreateRuntimeSession() error = %#v", err)
		}
	})

	t.Run("duplicate create is typed success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeClientTestJSON(w, http.StatusOK, clientTestCreateSessionResponse(request, now, RequestClassificationDuplicate))
		}))
		defer server.Close()
		response, err := clientTestClient(t, server.URL).CreateRuntimeSession(context.Background(), request)
		if err != nil {
			t.Fatalf("CreateRuntimeSession() error = %v", err)
		}
		if response.Classification.Class != RequestClassificationDuplicate {
			t.Fatalf("classification = %q", response.Classification.Class)
		}
	})

	t.Run("already accepted prompt never reconnects", func(t *testing.T) {
		promptRequest := clientTestStartPromptRequest(t, now, "already-op")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeClientTestJSON(w, http.StatusOK, ErrorResponse{
				Protocol: ProtocolVersion, Code: ErrorCodeAlreadyAccepted, Message: "already accepted",
				Classification: &Classification{Class: RequestClassificationAlreadyAccepted, Phase: OperationPhaseAccepted},
			})
		}))
		defer server.Close()
		_, err := clientTestClient(t, server.URL).StartPrompt(context.Background(), "runtime-session-1", promptRequest)
		var clientErr *ClientError
		if !errors.As(err, &clientErr) || clientErr.Code != ErrorCodeAlreadyAccepted || clientErr.Classification == nil || clientErr.Classification.Class != RequestClassificationAlreadyAccepted {
			t.Fatalf("StartPrompt() error = %#v", err)
		}
		if clientErr.StatusCode != http.StatusOK {
			t.Fatalf("already accepted status = %d, want 200", clientErr.StatusCode)
		}
	})
}

func TestClientRedactsAuthenticationValues(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "redaction-op")
	var echoedCapability string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		echoedCapability = r.Header.Get(OperationCapabilityHeader)
		writeClientTestJSON(w, http.StatusForbidden, ErrorResponse{
			Protocol: ProtocolVersion, Code: ErrorCodeForbidden,
			Message: "authorization=Bearer " + clientTestBearer + " capability=" + echoedCapability,
		})
	}))
	defer server.Close()

	_, err := clientTestClient(t, server.URL).CreateRuntimeSession(context.Background(), request)
	var clientErr *ClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("error = %v, want *ClientError", err)
	}
	if echoedCapability == "" {
		t.Fatal("server did not receive operation capability")
	}
	for _, secret := range []string{clientTestBearer, echoedCapability} {
		if strings.Contains(err.Error(), secret) || strings.Contains(clientErr.Message, secret) {
			t.Fatalf("error leaked credential %q: %v", secret, err)
		}
	}
	if strings.Count(err.Error(), "[REDACTED]") < 2 {
		t.Fatalf("redacted error = %v", err)
	}
}

func TestClientStrictPathBuildingRejectsTraversalBeforeTransport(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "path-op")
	request.RuntimeSessionID = "../status"
	sealRequest(t, request, &request.Metadata.RequestDigest)
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer server.Close()
	_, err := clientTestClient(t, server.URL).CreateRuntimeSession(context.Background(), request)
	if !errors.Is(err, ErrClientValidation) {
		t.Fatalf("CreateRuntimeSession() error = %v, want validation error", err)
	}
	if called {
		t.Fatal("transport was called for traversal path")
	}
}

func TestClientBeforeMutationReservesRequestValidityWindow(t *testing.T) {
	tests := []struct {
		name             string
		metadataLifetime time.Duration
		callerDeadline   time.Duration
	}{
		{name: "metadata expiry", metadataLifetime: time.Second},
		{name: "caller deadline", metadataLifetime: 5 * time.Second, callerDeadline: time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Millisecond)
			request := clientTestCreateSessionRequest(t, now, "authority-budget-op")
			request.Metadata.ExpiresAt = time.Now().UTC().Add(test.metadataLifetime)
			sealRequest(t, request, &request.Metadata.RequestDigest)
			originalDigest := request.Metadata.RequestDigest

			ctx := context.Background()
			effectiveDeadline := request.Metadata.ExpiresAt
			if test.callerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.callerDeadline)
				defer cancel()
				effectiveDeadline, _ = ctx.Deadline()
			}
			started := time.Now()
			transportCalled := false
			httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				transportCalled = true
				return nil, errors.New("transport must not be called")
			})}
			var validationDeadline time.Time
			client := clientTestClient(t, "http://runtime.invalid", WithHTTPClient(httpClient), WithBeforeMutation(func(ctx context.Context, _ string) error {
				var ok bool
				validationDeadline, ok = ctx.Deadline()
				if !ok {
					t.Fatal("pre-mutation authority context has no deadline")
				}
				<-ctx.Done()
				return nil
			}))

			_, err := client.CreateRuntimeSession(ctx, request)
			if !errors.Is(err, ErrClientValidation) || !strings.Contains(err.Error(), "reserved deadline") {
				t.Fatalf("CreateRuntimeSession() error = %v, want reserved-deadline validation error", err)
			}
			if transportCalled {
				t.Fatal("mutation transport was called after authority validation overran its deadline")
			}
			if !validationDeadline.Before(effectiveDeadline) {
				t.Fatalf("authority deadline = %s, effective mutation deadline = %s", validationDeadline, effectiveDeadline)
			}
			if reserved, window := effectiveDeadline.Sub(validationDeadline), effectiveDeadline.Sub(started); reserved < window/3 {
				t.Fatalf("reserved mutation window = %s, effective window = %s", reserved, window)
			}
			if !time.Now().Before(effectiveDeadline) {
				t.Fatal("authority validation consumed the reserved mutation window")
			}
			if request.Metadata.RequestDigest != originalDigest {
				t.Fatalf("request digest changed from %q to %q", originalDigest, request.Metadata.RequestDigest)
			}
		})
	}
}

func TestClientMutationPreflightFailureReportsZeroWriteEvidence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "preflight-zero-write-op")
	transportCalled := false
	client := clientTestClient(t, "http://runtime.invalid",
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			transportCalled = true
			return nil, errors.New("mutation transport must not be called")
		})}),
		WithBeforeMutation(func(context.Context, string) error {
			return errors.New("external runtime status unavailable")
		}),
	)

	_, err := client.CreateRuntimeSession(context.Background(), request)
	var clientErr *ClientError
	if !errors.As(err, &clientErr) || !errors.Is(err, ErrClientValidation) {
		t.Fatalf("CreateRuntimeSession() error = %v, want validation *ClientError", err)
	}
	if clientErr.WriteEvidence.State != RequestWriteZeroBytes || !clientErr.WriteEvidence.SafeToResendSameIdentity() {
		t.Fatalf("write evidence = %#v, want zero bytes written", clientErr.WriteEvidence)
	}
	if transportCalled {
		t.Fatal("mutation transport was called after pre-mutation validation failed")
	}
}

func TestClientMutationRetryablePreflightFailurePreservesClassification(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "retryable-preflight-op")
	client := clientTestClient(t, "http://runtime.invalid",
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("mutation transport must not be called")
		})}),
		WithBeforeMutation(func(context.Context, string) error {
			return MarkPreMutationRetryable(context.DeadlineExceeded)
		}),
	)

	_, err := client.CreateRuntimeSession(context.Background(), request)
	var clientErr *ClientError
	if !errors.As(err, &clientErr) || !clientErr.Retryable ||
		clientErr.WriteEvidence.State != RequestWriteZeroBytes {
		t.Fatalf("CreateRuntimeSession() error = %#v, want retryable zero-write validation", err)
	}
}

func TestClientTransportWriteEvidence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := clientTestCreateSessionRequest(t, now, "transport-op")

	t.Run("zero bytes exposed by net/http", func(t *testing.T) {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("dial refused")
		}
		httpClient := &http.Client{Transport: transport}
		client, err := NewClient("http://runtime.invalid", WithHTTPClient(httpClient), WithControllerBearerToken(clientTestBearer), WithOperationCapabilitySecret(clientTestCapabilitySecret))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.CreateRuntimeSession(context.Background(), request)
		var clientErr *ClientError
		if !errors.As(err, &clientErr) {
			t.Fatalf("error = %v, want *ClientError", err)
		}
		if clientErr.WriteEvidence.State != RequestWriteZeroBytes || !clientErr.WriteEvidence.SafeToResendSameIdentity() {
			t.Fatalf("write evidence = %#v", clientErr.WriteEvidence)
		}
	})

	t.Run("body consumption is ambiguous", func(t *testing.T) {
		roundTripper := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			buffer := make([]byte, 1)
			_, _ = request.Body.Read(buffer)
			return nil, errors.New("connection dropped after write")
		})
		client, err := NewClient("http://runtime.invalid", WithHTTPClient(&http.Client{Transport: roundTripper}), WithControllerBearerToken(clientTestBearer), WithOperationCapabilitySecret(clientTestCapabilitySecret))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.CreateRuntimeSession(context.Background(), request)
		var clientErr *ClientError
		if !errors.As(err, &clientErr) {
			t.Fatalf("error = %v, want *ClientError", err)
		}
		if clientErr.WriteEvidence.State != RequestWriteAmbiguous || clientErr.WriteEvidence.RequestBodyBytesRead != 1 || clientErr.WriteEvidence.SafeToResendSameIdentity() {
			t.Fatalf("write evidence = %#v", clientErr.WriteEvidence)
		}
	})
}

func clientTestClient(t *testing.T, baseURL string, extra ...ClientOption) *Client {
	t.Helper()
	options := []ClientOption{
		WithControllerBearerToken(clientTestBearer), WithOperationCapabilitySecret(clientTestCapabilitySecret),
		WithStatusCapabilityBinding(StatusCapabilityBinding{RuntimeProfileDigest: testFence(t).RuntimeProfileDigest}),
	}
	options = append(options, extra...)
	client, err := NewClient(baseURL, options...)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func clientTestCapabilities(t *testing.T) CapabilitiesResponse {
	t.Helper()
	return CapabilitiesResponse{
		Protocol: ProtocolVersion, Transport: "http+ndjson", ACPVersion: ACPProfileV1,
		RuntimeProfileDigest: testFence(t).RuntimeProfileDigest, ProfileDigestSchemaVersion: ProfileDigestSchemaVersion,
		AdapterDigests: map[string]string{"codex-acp": testSHA256("adapter")}, Limits: DefaultProtocolLimits(),
		Provider: ProviderCapabilities{
			ProviderKinds: []string{"codex"}, Models: []string{"test-model"},
			SupportsPermissions: true, SupportsCancel: true, SupportsTools: true,
		},
		WorkspaceGovernance: StrictWorkspaceGovernanceCapabilities(),
		SupportsDrain:       true,
	}
}

func clientTestStatus(t *testing.T, now time.Time) StatusResponse {
	t.Helper()
	fence := testFence(t)
	fence.RuntimeSessionUID = ""
	fence.RuntimeSessionGeneration = 0
	return StatusResponse{
		Protocol: ProtocolVersion, Fence: fence, Lifecycle: SupervisorLifecycleReady,
		Drain: DrainStatus{AcceptingNewSessions: true}, Sessions: []RuntimeSessionStatus{},
		ActivePrompts: []ActivePromptStatus{}, PendingPermissions: []PendingPermissionStatus{},
		Pressure: PressureMetadata{}, Timestamp: now,
	}
}

func clientTestMetadata(t *testing.T, now time.Time, operation OperationID, prompt bool) MutationMetadata {
	t.Helper()
	metadata := MutationMetadata{
		Fence: testFence(t), TaskUID: "task-uid-1", TaskAttempt: 1, OperationID: operation,
		RequestDigestSchemaVersion: RequestDigestSchemaVersion, ExpiresAt: now.Add(45 * time.Second),
	}
	if prompt {
		metadata.PromptID = "prompt-1"
	}
	return metadata
}

func clientTestCreateSessionRequest(t *testing.T, now time.Time, operation OperationID) CreateRuntimeSessionRequest {
	t.Helper()
	request := CreateRuntimeSessionRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, false),
		RuntimeSessionID: "runtime-session-1", Profile: testRuntimeProfile(),
		AgentConfiguration: testAgentSessionConfigurationPointer(),
		MCPConfiguration:   testMCPPolicyConfiguration(),
		Workspace:          WorkspaceSpec{Intent: WorkspaceIntentWrite, Baseline: testWorkspaceBaseline()},
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestCreateSessionResponse(request CreateRuntimeSessionRequest, now time.Time, class RequestClassification) CreateRuntimeSessionResponse {
	classification := Classification{Class: class}
	if class == RequestClassificationDuplicate {
		classification.Phase = OperationPhaseApplied
	}
	return CreateRuntimeSessionResponse{
		Protocol: ProtocolVersion, Classification: classification,
		Session: RuntimeSessionDescriptor{
			RuntimeSessionID: request.RuntimeSessionID, RuntimeSessionUID: request.Metadata.Fence.RuntimeSessionUID,
			Generation:        request.Metadata.Fence.RuntimeSessionGeneration,
			RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID, SupervisorBootID: request.Metadata.Fence.SupervisorBootID,
			RuntimeProfileDigest: request.Metadata.Fence.RuntimeProfileDigest, State: RuntimeSessionStateIdle,
			ProviderSessionID: "provider-session-1", WorkspaceBaseline: request.Workspace.Baseline,
			CreatedAt: now, LastTransitionAt: now,
		},
	}
}

func clientTestStartPromptRequest(t *testing.T, now time.Time, operation OperationID) StartPromptRequest {
	t.Helper()
	metadata := clientTestMetadata(t, now, operation, true)
	lease := PromptLease{Generation: 1, IssuedAt: now, ExpiresAt: now.Add(60 * time.Second)}
	request := StartPromptRequest{
		Protocol: ProtocolVersion, Metadata: metadata, Lease: lease,
		MCPAuthorization: testPromptMCPAuthorizationAt(metadata, lease, now.Add(45*time.Second)),
		Input:            PromptInput{Content: []ContentBlock{{Type: ContentBlockText, Text: "test prompt"}}},
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestLeaseRequest(t *testing.T, now time.Time, operation OperationID) RenewPromptLeaseRequest {
	t.Helper()
	metadata := clientTestMetadata(t, now, operation, true)
	lease := PromptLease{Generation: 2, IssuedAt: now, ExpiresAt: now.Add(60 * time.Second)}
	request := RenewPromptLeaseRequest{
		Protocol: ProtocolVersion, Metadata: metadata, ExpectedLeaseGeneration: 1, Lease: lease,
		MCPAuthorization: testPromptMCPAuthorizationAt(metadata, lease, now.Add(45*time.Second)),
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestPermissionRequest(t *testing.T, now time.Time, operation OperationID) ResolvePermissionRequest {
	t.Helper()
	request := ResolvePermissionRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, true), RequestID: "permission-1",
		Decision: PermissionDecision{Outcome: PermissionDecisionSelected, OptionID: "allow-once"},
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestCancelRequest(t *testing.T, now time.Time, operation OperationID) CancelPromptRequest {
	t.Helper()
	request := CancelPromptRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, true),
		Reason: CancelReasonUserRequested, SettlementDeadline: now.Add(30 * time.Second),
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestDeltaRequest(t *testing.T, now time.Time, operation OperationID) CreateWorkspaceDeltaRequest {
	t.Helper()
	request := CreateWorkspaceDeltaRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, true), DeltaID: "delta-1",
		Intent: WorkspaceIntentWrite, VerifiedBaseline: testWorkspaceBaseline(),
		PromptSettlementDigest: testSHA256("settlement"), Limits: WorkspaceDeltaLimits{MaxBytes: 1 << 20, MaxEntries: 100},
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestDrainRequest(t *testing.T, now time.Time, operation OperationID) DrainRequest {
	t.Helper()
	fence := testFence(t)
	fence.RuntimeSessionUID = ""
	fence.RuntimeSessionGeneration = 0
	request := DrainRequest{
		Protocol: ProtocolVersion,
		Metadata: MutationMetadata{
			Fence: fence, OperationID: operation, RequestDigestSchemaVersion: RequestDigestSchemaVersion,
			ExpiresAt: now.Add(45 * time.Second),
		},
		Reason: "rollout",
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestPublicationFinalizationRequest(t *testing.T, now time.Time, operation OperationID) FinalizeRuntimeSessionPublicationRequest {
	t.Helper()
	request := FinalizeRuntimeSessionPublicationRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, true),
		WorkspaceDeltaID: "delta-1", PublicationID: "publication-1", PublicationGeneration: 1, PublicationVersion: 7,
		TerminalState: PublicationTerminalVerifiedExact, TerminalReceiptDigest: testSHA256("publication-receipt"),
	}
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestDeleteRequest(t *testing.T, now time.Time, operation OperationID) DeleteRuntimeSessionRequest {
	t.Helper()
	request := DeleteRuntimeSessionRequest{
		Protocol: ProtocolVersion, Metadata: clientTestMetadata(t, now, operation, false), Reason: "idle expiry",
	}
	request.Metadata.TaskUID = ""
	request.Metadata.TaskAttempt = 0
	sealRequest(t, request, &request.Metadata.RequestDigest)
	return request
}

func clientTestDecodeMutation(t *testing.T, request *http.Request, target any, requireSession bool) {
	t.Helper()
	clientTestRequireBearer(t, request)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return
	}
	if err := json.Unmarshal(body, target); err != nil {
		t.Errorf("decode request body: %v", err)
		return
	}
	var envelope struct {
		Metadata MutationMetadata `json:"metadata"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Errorf("decode request metadata: %v", err)
		return
	}
	if err := VerifyOperationCapability(clientTestCapabilitySecret, request.Header.Get(OperationCapabilityHeader), envelope.Metadata, requireSession, time.Now().UTC()); err != nil {
		t.Errorf("operation capability: %v", err)
	}
}

func clientTestRequireBearer(t *testing.T, request *http.Request) {
	t.Helper()
	if got, want := request.Header.Get("Authorization"), "Bearer "+clientTestBearer; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

func writeClientTestJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func clientTestEventIdentity(request StartPromptRequest, sequence uint64, at time.Time) EventIdentity {
	return EventIdentity{
		RuntimeInstanceID: request.Metadata.Fence.RuntimeInstanceID, SupervisorBootID: request.Metadata.Fence.SupervisorBootID,
		RuntimeSessionUID:        request.Metadata.Fence.RuntimeSessionUID,
		RuntimeSessionGeneration: request.Metadata.Fence.RuntimeSessionGeneration,
		TaskUID:                  request.Metadata.TaskUID, TaskAttempt: request.Metadata.TaskAttempt,
		PromptID: request.Metadata.PromptID, Sequence: sequence, RequestDigest: request.Metadata.RequestDigest, Timestamp: at,
	}
}

func clientTestAcceptedEvent(request StartPromptRequest, at time.Time) Event {
	return Event{
		Protocol: ProtocolVersion, Type: EventAccepted, Identity: clientTestEventIdentity(request, 1, at),
		Accepted: &AcceptedEvent{AcceptedAt: at, Lease: request.Lease, ACPVersion: ACPProfileV1},
	}
}

func clientTestCompletedEvent(request StartPromptRequest, at time.Time) Event {
	return Event{
		Protocol: ProtocolVersion, Type: EventCompleted, Identity: clientTestEventIdentity(request, 2, at),
		Completed: &CompletedEvent{
			StopReason: ACPStopReasonEndTurn,
			Result:     PromptResult{Content: []ContentBlock{{Type: ContentBlockText, Text: "done"}}, Model: "test-model"},
		},
	}
}

func writeClientTestNDJSON(w http.ResponseWriter, events ...Event) {
	w.Header().Set("Content-Type", NDJSONMediaType)
	for _, event := range events {
		encoded, _ := json.Marshal(event)
		_, _ = w.Write(append(encoded, '\n'))
	}
}

func mustClientTestJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal test JSON: %v", err))
	}
	return string(encoded)
}
