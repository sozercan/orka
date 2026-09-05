package supervisor

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orka-agents/orka/internal/harness/v2/conformance"
)

func TestSupervisorAgentKitPassesCompletedPromptConformance(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("supervisor workspace verification requires Linux")
	}
	// Uses the production supervisor and the existing immediate ACP child.
	server, cfg, profile := newTestAgentKitServer(t)
	parent := filepath.Dir(cfg.SessionBaseDir)
	for _, dir := range []string{parent, filepath.Dir(parent)} {
		if err := os.Chmod(dir, 0711); err != nil {
			t.Fatal(err)
		}
	}
	var replayObservedSettlement atomic.Bool
	handler := server.Handler()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A valid scheduler/network ordering: the prompt finishes before the
		// controller's duplicate admission request arrives at the supervisor.
		if r.Method == http.MethodPut &&
			r.Header.Get("Accept") == "application/json" &&
			strings.Contains(r.URL.Path, "/prompts/") &&
			!strings.Contains(r.URL.Path, "conformance-session-workspace-") {
			deadline := time.Now().Add(5 * time.Second)
			for {
				settled := false
				server.mu.Lock()
				for id, state := range server.sessions {
					if strings.Contains(r.URL.Path, "/"+string(id)+"/prompts/") &&
						state.prompt != nil && state.prompt.settlement != nil {
						settled = true
					}
				}
				server.mu.Unlock()
				if settled {
					replayObservedSettlement.Store(true)
					break
				}
				if time.Now().After(deadline) || r.Context().Err() != nil {
					http.Error(w, "conformance prompt did not settle", http.StatusGatewayTimeout)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	policy := testServerMCPPolicyConfiguration(profile)
	result := conformance.Check(t.Context(), conformance.Target{
		BaseURL: endpoint.URL, ControllerBearerToken: cfg.ControllerBearerToken,
		OperationCapabilitySecret: cfg.CapabilitySecret, ControlTimeout: 20 * time.Second,
		ExpectedRuntimeInstanceID: cfg.Fence.RuntimeInstanceID, ExpectedControllerEpoch: cfg.Fence.ControllerEpoch,
		Profile: profile, ToolPolicy: policy.ToolPolicy, ApprovalPolicy: policy.ApprovalPolicy,
		Limits: cfg.Capabilities.Limits, SupportsDrain: cfg.Capabilities.SupportsDrain,
		SupportsPublicationFinalization: cfg.Capabilities.SupportsPublicationFinalization,
		WorkspaceGovernance:             cfg.Capabilities.WorkspaceGovernance, ProbeLifecycle: true,
	})
	t.Logf("replay arrived after settlement=%v, conformance passed=%v, message=%s", replayObservedSettlement.Load(), result.Passed, result.Message)
	if !replayObservedSettlement.Load() {
		t.Fatal("conformance did not exercise completion before replay")
	}
	if !result.Passed || !result.LifecycleProbeExecuted {
		t.Fatalf("completed prompt lifecycle failed conformance: %s", result.Message)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.sessions) != 0 {
		t.Fatalf("conformance leaked %d runtime sessions", len(server.sessions))
	}
}
