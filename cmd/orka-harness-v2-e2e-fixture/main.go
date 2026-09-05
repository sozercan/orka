package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
)

func main() {
	runtimeName := requiredEnv("ORKA_E2E_RUNTIME_NAME")
	epochValue := requiredEnv("ORKA_E2E_CONTROLLER_EPOCH")
	epoch, err := strconv.ParseUint(epochValue, 10, 64)
	if err != nil || epoch == 0 {
		log.Fatalf("invalid ORKA_E2E_CONTROLLER_EPOCH %q", epochValue)
	}

	profile, err := conformancetest.DeterministicProfile(runtimeName)
	if err != nil {
		log.Fatal(err)
	}
	server, err := conformancetest.NewServer(conformancetest.Config{
		ListenAddress:                 ":8080",
		ControllerBearerToken:         requiredEnv("ORKA_E2E_CONTROLLER_TOKEN"),
		OperationCapabilitySecret:     []byte(requiredEnv("ORKA_E2E_CAPABILITY_SECRET")),
		ControllerEpoch:               epoch,
		RuntimeInstanceID:             harnessv2.RuntimeInstanceID(runtimeName),
		SupervisorBootID:              "fixture-boot-1",
		RuntimePoolUID:                harnessv2.RuntimePoolUID(runtimeName + "-pool"),
		Profile:                       profile,
		Limits:                        harnessv2.DefaultProtocolLimits(),
		SupportsDrain:                 true,
		WorkspaceGovernance:           harnessv2.StrictWorkspaceGovernanceCapabilities(),
		CompleteNonConformancePrompts: true,
		PromptResultText:              conformancetest.DeterministicPromptResult,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	log.Printf("harness-v2 E2E fixture listening for runtime %q at epoch %d", runtimeName, epoch)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
