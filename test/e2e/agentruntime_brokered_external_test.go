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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/orka-agents/orka/test/utils"
)

var _ = Describe("AgentRuntime v2 broker boundary", func() {
	const (
		runtimeName    = "external-v2-broker-runtime"
		deploymentName = "external-v2-broker-runtime"
		serviceName    = "external-v2-broker-runtime"
		authSecretName = "external-v2-broker-runtime-auth"
		agentName      = "e2e-external-broker-agent"
		taskName       = "e2e-external-broker-task"
	)

	AfterEach(func() {
		for _, resource := range []struct{ kind, name string }{
			{"task", taskName},
			{"agent", agentName},
			{"agentruntime", runtimeName},
			{"service", serviceName},
			{"deployment", deploymentName},
			{"secret", authSecretName},
		} {
			cmd := exec.Command("kubectl", "delete", resource.kind, resource.name,
				"-n", namespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		}
	})

	It("rejects strict read tasks before any external v2 tool or prompt dispatch", func() {
		Expect(deployHarnessV2Fixture(runtimeName, deploymentName, serviceName, authSecretName)).To(Succeed())

		agentManifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Agent",
			"metadata": {"name": %q, "namespace": %q},
			"spec": {"runtime": {"runtimeRef": {"name": %q}}}
		}`, agentName, namespace, runtimeName)
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
				"agentRuntime": {"allowedTools": ["read-evidence"]},
				"prompt": "Read evidence through the Orka broker.",
				"workspace": {"intent": "read"}
			}
		}`, taskName, namespace, agentName)
		cmd = exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = stringReader(taskManifest)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "task", taskName, "-n", namespace,
				"-o", "jsonpath={.status.message}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(ContainSubstring("task allowedTools do not exactly match the registered external AgentRuntime MCP policy"))
		}, 2*time.Minute, time.Second).Should(Succeed())
		waitForTaskPhase(taskName, "Failed", 2*time.Minute)
		verifyNoJobForTask(taskName, 5*time.Second)
	})
})
