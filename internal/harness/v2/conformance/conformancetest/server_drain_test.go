package conformancetest

import (
	"testing"
	"time"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
)

func TestServerDrainClosesAdmissionAndReportsQuiescence(t *testing.T) {
	profile, err := DeterministicProfile("fixture-runtime")
	if err != nil {
		t.Fatal(err)
	}
	controllerToken := "controller-token-for-drain-test-0001"
	capabilitySecret := []byte("capability-secret-for-drain-test-01")
	server, err := NewServer(Config{
		ControllerBearerToken:     controllerToken,
		OperationCapabilitySecret: capabilitySecret,
		Profile:                   profile,
		SupportsDrain:             true,
		WorkspaceGovernance:       harnessv2.StrictWorkspaceGovernanceCapabilities(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := harnessv2.NewClient(
		server.URL(),
		harnessv2.WithControllerBearerToken(controllerToken),
		harnessv2.WithOperationCapabilitySecret(capabilitySecret),
		harnessv2.WithStatusCapabilityBinding(harnessv2.StatusCapabilityBinding{
			RuntimeProfileDigest: server.Fence().RuntimeProfileDigest,
			RuntimeInstanceID:    server.Fence().RuntimeInstanceID,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := harnessv2.DrainRequest{
		Protocol: harnessv2.ProtocolVersion,
		Metadata: harnessv2.MutationMetadata{
			Fence:                      server.Fence(),
			OperationID:                "fixture-drain-1",
			RequestDigestSchemaVersion: harnessv2.RequestDigestSchemaVersion,
			ExpiresAt:                  time.Now().UTC().Add(time.Minute),
		},
		Reason: "test_cleanup",
	}
	requestDigest, err := harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata.RequestDigest = requestDigest

	response, err := client.Drain(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Classification.Class != harnessv2.RequestClassificationFresh ||
		!response.Drain.Requested || response.Drain.AcceptingNewSessions {
		t.Fatalf("fresh drain response = %#v", response)
	}
	replay, err := client.Drain(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Classification.Class != harnessv2.RequestClassificationDuplicate {
		t.Fatalf("replayed drain classification = %q, want %q", replay.Classification.Class, harnessv2.RequestClassificationDuplicate)
	}
	status, err := client.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Lifecycle != harnessv2.SupervisorLifecycleDraining ||
		!status.Drain.Requested || status.Drain.AcceptingNewSessions ||
		status.Drain.Reason != request.Reason {
		t.Fatalf("status after drain = %#v", status)
	}
}
