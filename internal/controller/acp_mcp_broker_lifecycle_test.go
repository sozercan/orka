package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/store"
)

func TestACPMCPBrokerCancelsInFlightToolWhenPromptAuthorityEnds(t *testing.T) {
	for _, test := range []struct {
		name   string
		effect harnessv2.MCPToolEffect
		remove bool
	}{
		{name: "read-only prompt settles", effect: harnessv2.MCPToolEffectReadOnly},
		{name: "read-only attempt removed", effect: harnessv2.MCPToolEffectReadOnly, remove: true},
		{name: "consequential prompt settles", effect: harnessv2.MCPToolEffectConsequential},
	} {
		t.Run(test.name, func(t *testing.T) {
			effects, fence := newMCPBrokerControlStore(t)
			request, profile := testMCPBrokerRequest(t, test.effect)
			bearer := strings.Repeat("b", 32)
			capability := []byte(strings.Repeat("c", 32))
			attempt := &store.PromptAttempt{
				Key: store.PromptAttemptKey{
					Namespace: request.Namespace, TaskUID: string(request.Metadata.TaskUID),
					Attempt: int64(request.Metadata.TaskAttempt), PromptID: string(request.Metadata.PromptID),
				},
				SessionUID: string(request.Authorization.RuntimeSessionUID), RuntimeInstanceID: string(request.Metadata.Fence.RuntimeInstanceID),
				ControllerEpoch: int64(request.Metadata.Fence.ControllerEpoch), ExecutionState: store.PromptExecutionRunning,
			}
			attempt.ID, _ = attempt.Key.CanonicalID()
			attempts := &liveMCPPromptAttemptStore{}
			attempts.current.Store(attempt)

			entered := make(chan struct{})
			cancelled := make(chan struct{})
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-r.Context().Done():
					close(cancelled)
				case <-release:
					_, _ = w.Write([]byte(acpMCPTestOKBody))
				}
			}))
			defer upstream.Close()
			defer close(release)
			var executions atomic.Int32
			broker := &ACPMCPBroker{
				Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
					return ACPMCPBrokerCredentials{
						ControllerBearerToken: bearer, CapabilitySecret: capability,
						ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile, ControllerFence: fence,
					}, nil
				}),
				Prompts: DurableACPMCPPromptAuthorizer{Attempts: attempts},
				Executor: ACPMCPToolExecutorFunc(func(ctx context.Context, _ harnessv2.MCPBrokerCallRequest, _ harnessv2.MCPToolDescriptor) (json.RawMessage, error) {
					executions.Add(1)
					outbound, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
					if err != nil {
						return nil, err
					}
					response, err := upstream.Client().Do(outbound)
					if err != nil {
						return nil, err
					}
					defer response.Body.Close() //nolint:errcheck
					return io.ReadAll(response.Body)
				}),
				Effects: effects,
			}
			// Exercise the production HTTP adapter. Its incoming request context
			// does not cancel on client disconnect, so durable prompt authority
			// must cancel the downstream request independently.
			app := fiber.New()
			app.Post(harnessv2.MCPBrokerCallPath, adaptor.HTTPHandler(broker))
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			token, err := harnessv2.SignOperationCapability(capability, harnessv2.ClaimsForMutation(request.Metadata))
			if err != nil {
				t.Fatal(err)
			}
			incoming := httptest.NewRequest(http.MethodPost, harnessv2.MCPBrokerCallPath, bytes.NewReader(body))
			incoming.Header.Set("Content-Type", "application/json")
			incoming.Header.Set("Authorization", "Bearer "+bearer)
			incoming.Header.Set(harnessv2.OperationCapabilityHeader, token)
			type callResult struct {
				code int
				err  error
			}
			done := make(chan callResult, 1)
			go func() {
				response, err := app.Test(incoming, fiber.TestConfig{Timeout: 5 * time.Second})
				result := callResult{err: err}
				if response != nil {
					result.code = response.StatusCode
					_ = response.Body.Close()
				}
				done <- result
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("tool did not start")
			}
			if test.remove {
				attempts.current.Store(nil)
			} else {
				settling := *attempt
				settling.ExecutionState = store.PromptExecutionSettling
				attempts.current.Store(&settling)
			}
			select {
			case <-cancelled:
			case <-time.After(3 * time.Second):
				t.Fatal("tool HTTP request remained active after prompt authority ended")
			}
			select {
			case result := <-done:
				if result.err != nil || result.code != http.StatusBadGateway {
					t.Fatalf("cancelled broker call = %#v, want HTTP 502", result)
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled broker handler did not finish")
			}
			if test.effect == harnessv2.MCPToolEffectConsequential {
				identity := store.ExternalEffectIdentity{
					Kind: "acp-mcp-tool", Namespace: request.Namespace,
					AggregateID: string(request.Authorization.RuntimeSessionUID), OperationID: string(request.Metadata.OperationID),
				}
				id, err := identity.CanonicalID()
				if err != nil {
					t.Fatal(err)
				}
				effect, err := effects.GetExternalEffect(context.Background(), id)
				if err != nil || effect.State != store.ExternalEffectOutcomeUnknown {
					t.Fatalf("cancelled consequential effect = %#v, %v; want OutcomeUnknown", effect, err)
				}
				attempts.current.Store(attempt)
				replay := performMCPBrokerCall(t, broker, request, bearer, capability)
				if replay.Code == http.StatusOK || executions.Load() != 1 {
					t.Fatalf("cancelled effect replay status=%d executions=%d", replay.Code, executions.Load())
				}
			}
		})
	}
}

type liveMCPPromptAttemptStore struct {
	staticPromptAttemptStore
	current atomic.Pointer[store.PromptAttempt]
}

func (s *liveMCPPromptAttemptStore) GetPromptAttempt(_ context.Context, id string) (*store.PromptAttempt, error) {
	attempt := s.current.Load()
	if attempt == nil || attempt.ID != id {
		return nil, store.ErrNotFound
	}
	copy := *attempt
	return &copy, nil
}
