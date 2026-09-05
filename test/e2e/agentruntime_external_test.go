//go:build e2e
// +build e2e

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
	"github.com/orka-agents/orka/test/utils"
)

const (
	externalV2RuntimeName           = "external-v2-runtime"
	externalV2RuntimeDeploymentName = "external-v2-runtime"
	externalV2RuntimeServiceName    = "external-v2-runtime"
	externalV2RuntimeAuthName       = "external-v2-runtime-auth"
	externalV2ResultAPIPort         = 18121
)

var _ = Describe("AgentRuntime external dispatch", func() {
	const (
		agentName = "e2e-external-v2-agent"
		taskName  = "e2e-external-v2-task"
	)

	AfterEach(func() {
		for _, resource := range []struct{ kind, name string }{
			{"task", taskName},
			{"agent", agentName},
			{"agentruntime", externalV2RuntimeName},
			{"service", externalV2RuntimeServiceName},
			{"deployment", externalV2RuntimeDeploymentName},
			{"secret", externalV2RuntimeAuthName},
		} {
			cmd := exec.Command("kubectl", "delete", resource.kind, resource.name,
				"-n", namespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		}
	})

	It("rejects orka.harness.v1 registrations in a harness-v2 namespace", func() {
		manifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "AgentRuntime",
			"metadata": {"name": "e2e-v1-rejected", "namespace": %q},
			"spec": {
				"contractVersion": "orka.harness.v1",
				"clientAuth": {
					"bearerTokenSecretRef": {"name": "runtime-auth", "key": "token"}
				},
				"deployment": {"mode": "external-endpoint", "endpoint": "https://runtime.example.com"}
			}
		}`, namespace)
		cmd := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
		cmd.Stdin = stringReader(manifest)
		output, err := utils.Run(cmd)
		Expect(err).To(HaveOccurred())
		Expect(output).To(ContainSubstring(
			`AgentRuntime contractVersion must match namespace execution mode "harness-v2"`,
		))
	})

	It("dispatches a Task through a conformant external v2 runtime", func() {
		Expect(deployHarnessV2Fixture(
			externalV2RuntimeName,
			externalV2RuntimeDeploymentName,
			externalV2RuntimeServiceName,
			externalV2RuntimeAuthName,
		)).To(Succeed())

		agentManifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Agent",
			"metadata": {"name": %q, "namespace": %q},
			"spec": {
				"runtime": {"runtimeRef": {"name": %q}}
			}
		}`, agentName, namespace, externalV2RuntimeName)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = stringReader(agentManifest)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		taskManifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Task",
			"metadata": {"name": %q, "namespace": %q},
			"spec": {
				"type": "agent",
				"agentRef": {"name": %q},
				"agentRuntime": {"allowedTools": []},
				"prompt": "Prepare a governed change.",
				"workspace": {"intent": "read"}
			}
		}`, taskName, namespace, agentName)
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = stringReader(taskManifest)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		waitForTaskPhase(taskName, "Succeeded", 3*time.Minute)
		verifyNoJobForTask(taskName, 5*time.Second)

		By("verifying the frozen external execution identity")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "task", taskName, "-n", namespace,
				"-o", "jsonpath={.status.execution.agentRuntimeName}{\"/\"}{.status.execution.runtimePoolName}{\"/\"}{.status.execution.runtimeInstanceID}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(Equal(externalV2RuntimeName + "//" + externalV2RuntimeName))
		}, time.Minute, time.Second).Should(Succeed())

		By("verifying the deterministic external result")
		verifyResultAvailable(taskName)
		apiBaseURL, cancelPortForward, portForwardCmd, err := startControllerAPIPortForward(externalV2ResultAPIPort)
		Expect(err).NotTo(HaveOccurred())
		defer stopPortForward(cancelPortForward, portForwardCmd)
		apiToken, err := serviceAccountToken()
		Expect(err).NotTo(HaveOccurred())
		result := fetchTaskResultViaAPI(apiBaseURL, apiToken, taskName)
		Expect(strings.TrimSpace(result)).To(Equal(conformancetest.DeterministicPromptResult))
	})
})
