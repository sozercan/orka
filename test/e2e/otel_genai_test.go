//go:build e2e
// +build e2e

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/orka-agents/orka/test/utils"
)

const (
	otelCollectorName               = "e2e-otel-collector"
	otelFakeOpenAIName              = "e2e-otel-openai"
	otelProviderName                = "e2e-otel-provider"
	otelProviderAuthRefName         = "e2e-otel-openai-auth"
	otelTaskName                    = "e2e-otel-genai-task"
	otelTopologyProviderName        = "e2e-otel-topology-provider"
	otelTopologyProviderAuthRefName = "e2e-otel-topology-openai-auth"
	otelTopologyTaskName            = "e2e-otel-topology-task"
	otelTopologyCoordAgent          = "e2e-otel-topology-coord"
	otelTopologyWorkerAgent         = "e2e-otel-topology-worker"
	otelModelName                   = "e2e-otel-model"
	otelFakeOpenAIImage             = "python:3.14-slim"
)

func otelCollectorServiceAddr() string {
	return fmt.Sprintf("http://%s.%s.svc:4317", otelCollectorName, namespace)
}

var _ = Describe("OpenTelemetry GenAI export", Ordered, Serial, func() {
	var controllerSnapshot otelControllerSnapshot

	BeforeAll(func() {
		By("snapshotting controller telemetry settings")
		controllerSnapshot = captureOTelControllerSnapshot()

		By("deploying a local OTLP collector")
		applyOTelManifest(otelCollectorManifest())
		waitForOTelDeploymentAvailable(otelCollectorName, 2*time.Minute)

		By("deploying a fake OpenAI-compatible endpoint")
		applyOTelManifest(otelFakeOpenAIManifest())
		waitForOTelDeploymentAvailable(otelFakeOpenAIName, 2*time.Minute)

		By("enabling controller telemetry against the local collector")
		enableControllerTelemetryForE2E(controllerSnapshot)
	})

	AfterAll(func() {
		By("cleaning up OpenTelemetry GenAI test resources")
		cmd := exec.Command("kubectl", "delete", "tasks", "-l", fmt.Sprintf("orka.ai/parent-task=%s", otelTopologyTaskName), "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
		for _, resource := range []struct {
			kind string
			name string
		}{
			{kind: "task", name: otelTaskName},
			{kind: "task", name: otelTopologyTaskName},
			{kind: "agent", name: otelTopologyCoordAgent},
			{kind: "agent", name: otelTopologyWorkerAgent},
			{kind: "provider", name: otelTopologyProviderName},
			{kind: "secret", name: otelTopologyProviderAuthRefName},
			{kind: "provider", name: otelProviderName},
			{kind: "secret", name: otelProviderAuthRefName},
			{kind: "deployment", name: otelCollectorName},
			{kind: "service", name: otelCollectorName},
			{kind: "configmap", name: otelCollectorName + "-config"},
			{kind: "deployment", name: otelFakeOpenAIName},
			{kind: "service", name: otelFakeOpenAIName},
			{kind: "configmap", name: otelFakeOpenAIName + "-server"},
		} {
			cmd := exec.Command("kubectl", "delete", resource.kind, resource.name, "-n", namespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		}

		By("restoring controller telemetry settings")
		restoreOTelControllerSnapshot(controllerSnapshot)
	})

	AfterEach(func() {
		dumpDebugInfo(otelTaskName)
		dumpDebugInfo(otelTopologyTaskName)
		dumpOTelCollectorLogsForDiagnostics()
	})

	It("exports GenAI model and tool spans and metrics from an AI worker", func() {
		By("creating a Provider CRD that points to the fake OpenAI endpoint")
		createOTelSecretOrFail(otelProviderAuthRefName, map[string]string{"token": "placeholder"})
		createProviderCRD(
			otelProviderName,
			"openai",
			otelProviderAuthRefName,
			"token",
			fmt.Sprintf("http://%s.%s.svc:8080", otelFakeOpenAIName, namespace),
			otelModelName,
		)

		By("creating an AI task that exercises the model loop and file_write tool")
		taskManifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Task",
			"metadata": {
				"name": "%s",
				"namespace": "%s"
			},
			"spec": {
				"type": "ai",
				"ai": {
					"prompt": "Use file_write once, then reply otel e2e complete.",
					"model": "%s",
					"providerRef": {
						"name": "%s"
					},
					"tools": ["file_write"]
				}
			}
		}`, otelTaskName, namespace, otelModelName, otelProviderName)

		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = stringReader(taskManifest)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create OpenTelemetry GenAI task")

		By("waiting for the AI task to complete successfully")
		phase := waitForTaskCompletion(otelTaskName, 5*time.Minute)
		Expect(phase).To(Equal("Succeeded"), "OpenTelemetry GenAI task should succeed")
		verifyResultAvailable(otelTaskName)

		By("asserting the worker Job received telemetry enablement and OTLP settings")
		verifyOTelTelemetryEnvForTaskJob(otelTaskName)

		By("asserting the collector received GenAI spans and metrics")
		assertOTelCollectorLogsContain([]string{
			"service.name: Str(orka-ai-worker)",
			"chat " + otelModelName,
			"gen_ai.operation.name: Str(chat)",
			"gen_ai.provider.name: Str(openai)",
			"gen_ai.request.model: Str(" + otelModelName + ")",
			"execute_tool file_write",
			"gen_ai.operation.name: Str(execute_tool)",
			"gen_ai.tool.name: Str(file_write)",
			"gen_ai.client.operation.duration",
			"gen_ai.client.token.usage",
			"gen_ai.execute_tool.duration",
		}, 3*time.Minute)
	})

	It("exports one trace for a delegated parent and child task topology", func() {
		By("cleaning up prior topology resources from earlier runs")
		cleanupOTelTopologyResources()

		By("creating a Provider CRD that points to the fake OpenAI endpoint")
		createOTelSecretOrFail(otelTopologyProviderAuthRefName, map[string]string{"token": "placeholder"})
		createProviderCRD(
			otelTopologyProviderName,
			"openai",
			otelTopologyProviderAuthRefName,
			"token",
			fmt.Sprintf("http://%s.%s.svc:8080", otelFakeOpenAIName, namespace),
			otelModelName,
		)

		By("creating a worker agent with a child tool and a coordinator agent with delegation enabled")
		applyOTelManifest(fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Agent",
			"metadata": {
				"name": "%s",
				"namespace": "%s"
			},
			"spec": {
				"providerRef": {"name": "%s"},
				"model": {"name": "%s"},
				"tools": [{"name": "file_write"}]
			}
		}`, otelTopologyWorkerAgent, namespace, otelTopologyProviderName, otelModelName))

		applyOTelManifest(fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Agent",
			"metadata": {
				"name": "%s",
				"namespace": "%s"
			},
			"spec": {
				"providerRef": {"name": "%s"},
				"model": {"name": "%s"},
				"coordination": {
					"enabled": true,
					"maxDepth": 2,
					"maxConcurrentChildren": 1,
					"allowedAgents": [{"name": "%s"}]
				}
			}
		}`, otelTopologyCoordAgent, namespace, otelTopologyProviderName, otelModelName, otelTopologyWorkerAgent))

		By("creating a coordinator task that delegates to the worker")
		taskManifest := fmt.Sprintf(`{
			"apiVersion": "core.orka.ai/v1alpha1",
			"kind": "Task",
			"metadata": {
				"name": "%s",
				"namespace": "%s"
			},
			"spec": {
				"type": "ai",
				"agentRef": {"name": "%s"},
				"ai": {
					"prompt": "OTEL_TOPOLOGY_PARENT: delegate exactly one child task, wait for it, then reply otel topology complete.",
					"model": "%s",
					"providerRef": {"name": "%s"}
				}
			}
		}`, otelTopologyTaskName, namespace, otelTopologyCoordAgent, otelModelName, otelTopologyProviderName)

		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = stringReader(taskManifest)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create OpenTelemetry topology task")

		By("waiting for the delegated child task to be created")
		childTaskName := waitForOTelTopologyChildTask(otelTopologyTaskName, 5*time.Minute)

		By("waiting for child and parent tasks to complete successfully")
		Expect(waitForTaskCompletion(childTaskName, 5*time.Minute)).To(Equal("Succeeded"), "delegated child task should succeed")
		Expect(waitForTaskCompletion(otelTopologyTaskName, 7*time.Minute)).To(Equal("Succeeded"), "topology parent task should succeed")
		verifyResultAvailable(childTaskName)
		verifyResultAvailable(otelTopologyTaskName)

		By("asserting parent and child Jobs received telemetry enablement and OTLP settings")
		verifyOTelTelemetryEnvForTaskJob(otelTopologyTaskName)
		verifyOTelTelemetryEnvForTaskJob(childTaskName)

		By("asserting the collector received the delegated topology in one trace")
		assertOTelCollectorHasDelegatedTopology(otelTopologyTaskName, childTaskName, 3*time.Minute)
	})
})

func cleanupOTelTopologyResources() {
	cmd := exec.Command("kubectl", "delete", "tasks", "-l", fmt.Sprintf("orka.ai/parent-task=%s", otelTopologyTaskName), "-n", namespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
	for _, resource := range []struct {
		kind string
		name string
	}{
		{kind: "task", name: otelTopologyTaskName},
		{kind: "agent", name: otelTopologyCoordAgent},
		{kind: "agent", name: otelTopologyWorkerAgent},
		{kind: "provider", name: otelTopologyProviderName},
		{kind: "secret", name: otelTopologyProviderAuthRefName},
	} {
		cmd = exec.Command("kubectl", "delete", resource.kind, resource.name, "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	}
}

func applyOTelManifest(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = stringReader(manifest)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

func waitForOTelDeploymentAvailable(name string, timeout time.Duration) {
	cmd := exec.Command("kubectl", "rollout", "status", "deployment/"+name, "-n", namespace, "--timeout="+timeout.String())
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "deployment %s did not become available", name)
}

func createOTelSecretOrFail(name string, data map[string]string) {
	cmd := exec.Command("kubectl", "delete", "secret", name, "-n", namespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
	err := createK8sSecret(name, namespace, data)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to create secret %s", name)
}

func enableControllerTelemetryForE2E(snapshot otelControllerSnapshot) {
	ExpectWithOffset(1, snapshot.Captured).To(BeTrue(), "controller telemetry snapshot must be captured before patching")

	args := append([]string(nil), snapshot.Args...)
	env := append([]otelEnvVar(nil), snapshot.Env...)
	if !slices.Contains(args, "--enable-telemetry") {
		args = append(args, "--enable-telemetry")
	}
	env = upsertOTelEnvVars(env, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT":        otelCollectorServiceAddr(),
		"OTEL_EXPORTER_OTLP_INSECURE":        "true",
		"OTEL_EXPORTER_OTLP_PROTOCOL":        "grpc",
		"OTEL_EXPORTER_OTLP_TIMEOUT":         "5000",
		"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT": "5000",
		"OTEL_RESOURCE_ATTRIBUTES":           "orka.e2e.test=otel-genai",
	})

	patchOTelControllerManager(snapshot.DeploymentName, snapshot.ContainerIndex, args, env, "failed to patch controller-manager telemetry settings")
}

type otelControllerSnapshot struct {
	DeploymentName string
	ContainerIndex int
	Args           []string
	Env            []otelEnvVar
	Captured       bool
}

func captureOTelControllerSnapshot() otelControllerSnapshot {
	deploymentName, err := controllerManagerDeploymentName()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	containerIndex, args, env := otelControllerManagerArgsAndEnv(deploymentName)
	return otelControllerSnapshot{
		DeploymentName: deploymentName,
		ContainerIndex: containerIndex,
		Args:           append([]string(nil), args...),
		Env:            append([]otelEnvVar(nil), env...),
		Captured:       true,
	}
}

func restoreOTelControllerSnapshot(snapshot otelControllerSnapshot) {
	if !snapshot.Captured {
		return
	}
	patchOTelControllerManager(
		snapshot.DeploymentName,
		snapshot.ContainerIndex,
		append([]string(nil), snapshot.Args...),
		append([]otelEnvVar(nil), snapshot.Env...),
		"failed to restore controller-manager telemetry settings",
	)
}

func patchOTelControllerManager(deploymentName string, containerIndex int, args []string, env []otelEnvVar, failureMessage string) {
	if args == nil {
		args = []string{}
	}
	if env == nil {
		env = []otelEnvVar{}
	}
	containerPath := fmt.Sprintf("/spec/template/spec/containers/%d", containerIndex)
	patch := []map[string]any{
		{
			"op":    "add",
			"path":  containerPath + "/args",
			"value": args,
		},
		{
			"op":    "add",
			"path":  containerPath + "/env",
			"value": env,
		},
	}
	patchBytes, err := json.Marshal(patch)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	cmd := exec.Command("kubectl", "patch", "deployment", deploymentName, "-n", namespace, "--type=json", "-p", string(patchBytes))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), failureMessage)

	cmd = exec.Command("kubectl", "rollout", "status", "deployment/"+deploymentName, "-n", namespace, "--timeout=5m")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed waiting for controller-manager rollout")
}

type otelEnvVar struct {
	Name      string `json:"name"`
	Value     string `json:"value,omitempty"`
	ValueFrom any    `json:"valueFrom,omitempty"`
}

type otelControllerContainer struct {
	Name string       `json:"name"`
	Args []string     `json:"args"`
	Env  []otelEnvVar `json:"env"`
}

func otelControllerManagerArgsAndEnv(deploymentName string) (int, []string, []otelEnvVar) {
	cmd := exec.Command("kubectl", "get", "deployment", deploymentName, "-n", namespace, "-o", "json")
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to read controller-manager deployment")

	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []otelControllerContainer `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	err = json.Unmarshal([]byte(output), &deployment)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	for i, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "manager" {
			return i, append([]string(nil), container.Args...), append([]otelEnvVar(nil), container.Env...)
		}
	}
	Fail("controller-manager deployment did not contain manager container")
	return 0, nil, nil
}

func upsertOTelEnvVars(env []otelEnvVar, values map[string]string) []otelEnvVar {
	for name, value := range values {
		updated := false
		for i := range env {
			if env[i].Name == name {
				env[i].Value = value
				env[i].ValueFrom = nil
				updated = true
				break
			}
		}
		if !updated {
			env = append(env, otelEnvVar{Name: name, Value: value})
		}
	}
	return env
}

func verifyOTelTelemetryEnvForTaskJob(taskName string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "jobs", "-l", fmt.Sprintf("orka.ai/task=%s", taskName),
			"-o", "jsonpath={.items[0].spec.template.spec.containers[0].env}", "-n", namespace)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(ContainSubstring("ORKA_ENABLE_TELEMETRY"))
		g.Expect(output).To(ContainSubstring(otelCollectorServiceAddr()))
		g.Expect(output).To(ContainSubstring("OTEL_EXPORTER_OTLP_INSECURE"))
		g.Expect(output).To(ContainSubstring("OTEL_EXPORTER_OTLP_PROTOCOL"))
	}, 30*time.Second, time.Second).Should(Succeed())
}

func assertOTelCollectorLogsContain(needles []string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		logs := otelCollectorLogs()
		for _, needle := range needles {
			g.Expect(logs).To(ContainSubstring(needle), "collector logs should contain %q", needle)
		}
	}, timeout, 2*time.Second).Should(Succeed())
}

func waitForOTelTopologyChildTask(parentTaskName string, timeout time.Duration) string {
	var childTaskName string
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "tasks",
			"-l", fmt.Sprintf("orka.ai/parent-task=%s", parentTaskName),
			"-o", "jsonpath={.items[0].metadata.name}",
			"-n", namespace,
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty(), "delegated child task should be created")
		childTaskName = strings.TrimSpace(output)
	}, timeout, 2*time.Second).Should(Succeed())
	return childTaskName
}

type otelDebugSpan struct {
	Name     string
	TraceID  string
	SpanID   string
	ParentID string
	Attrs    map[string]string
}

func assertOTelCollectorHasDelegatedTopology(parentTaskName, childTaskName string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		logs := otelCollectorLogs()
		spans := parseOTelDebugSpans(logs)

		childRun := requireOTelSpan(g, spans, "task.run", "orka.task.id", childTaskName)
		delegateSpan := requireOTelSpan(g, spans, "execute_tool delegate_task", "orka.child_task.id", childTaskName)
		g.Expect(delegateSpan.TraceID).To(Equal(childRun.TraceID), "delegate_task span should share current child TraceID")

		traceID := childRun.TraceID
		parentRun := requireOTelSpanWithTrace(g, spans, "task.run", "orka.task.id", parentTaskName, traceID)

		parentSteps := otelSpansNamedWithAttrAndTrace(spans, "agent.step", "orka.task.id", parentTaskName, traceID)
		childSteps := otelSpansNamedWithAttrAndTrace(spans, "agent.step", "orka.task.id", childTaskName, traceID)
		g.Expect(parentSteps).NotTo(BeEmpty(), "parent agent.step should be exported")
		g.Expect(childSteps).NotTo(BeEmpty(), "child agent.step should be exported")
		g.Expect(otelAllTraceIDs(parentSteps)).To(ConsistOf(traceID), "parent agent.step spans should share current TraceID")
		g.Expect(otelAllTraceIDs(childSteps)).To(ConsistOf(traceID), "child agent.step spans should share current TraceID")
		g.Expect(otelAllParents(parentSteps)).To(ConsistOf(parentRun.SpanID), "parent agent.step spans should be children of parent task.run")
		g.Expect(otelAllParents(childSteps)).To(ConsistOf(childRun.SpanID), "child agent.step spans should be children of child task.run")

		g.Expect(delegateSpan.Attrs).To(HaveKeyWithValue("orka.parent_task.id", parentTaskName))
		g.Expect(delegateSpan.Attrs).To(HaveKeyWithValue("orka.tool.name", "delegate_task"))
		g.Expect(delegateSpan.Attrs).To(HaveKeyWithValue("orka.tool.kind", "delegate"))
		g.Expect(otelSpanIDs(parentSteps)).To(ContainElement(delegateSpan.ParentID), "delegate_task should be a child of a parent agent.step")
		g.Expect(childRun.ParentID).To(Equal(delegateSpan.SpanID), "child task.run should be a child of delegate_task")

		childTool := requireOTelSpanWithTrace(g, spans, "execute_tool file_write", "orka.task.id", childTaskName, traceID)
		g.Expect(childTool.Attrs).To(HaveKeyWithValue("orka.tool.name", "file_write"))
		g.Expect(otelSpanIDs(childSteps)).To(ContainElement(childTool.ParentID), "child file_write should be a child of a child agent.step")

		chatSpans := otelSpansNamed(spans, "chat "+otelModelName)
		g.Expect(otelHasSpanWithTraceAndParent(chatSpans, traceID, otelSpanIDs(parentSteps))).To(BeTrue(), "parent chat span should be a child of a parent agent.step")
		g.Expect(otelHasSpanWithTraceAndParent(chatSpans, traceID, otelSpanIDs(childSteps))).To(BeTrue(), "child chat span should be a child of a child agent.step")

		g.Expect(logs).NotTo(ContainSubstring("OTEL_TOPOLOGY_PARENT"), "collector logs should not include raw parent prompt content")
		g.Expect(logs).NotTo(ContainSubstring("OTEL_TOPOLOGY_CHILD"), "collector logs should not include raw child prompt content")
	}, timeout, 2*time.Second).Should(Succeed())
}

func parseOTelDebugSpans(logs string) []otelDebugSpan {
	spans := []otelDebugSpan{}
	var current *otelDebugSpan
	finish := func() {
		if current != nil && current.Name != "" && current.TraceID != "" {
			spans = append(spans, *current)
		}
		current = nil
	}

	for _, line := range strings.Split(logs, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "Span #"):
			finish()
			current = &otelDebugSpan{Attrs: map[string]string{}}
			continue
		case strings.HasPrefix(trimmed, "ResourceSpans #"), strings.HasPrefix(trimmed, "ScopeSpans #"), strings.HasPrefix(trimmed, "Metric #"):
			finish()
			continue
		case current == nil:
			continue
		}

		if value, ok := otelDebugLineValue(trimmed, "Trace ID"); ok {
			current.TraceID = value
			continue
		}
		if value, ok := otelDebugLineValue(trimmed, "Parent ID"); ok {
			current.ParentID = value
			continue
		}
		if value, ok := otelDebugLineValue(trimmed, "Span ID"); ok {
			current.SpanID = value
			continue
		}
		if value, ok := otelDebugLineValue(trimmed, "ID"); ok {
			current.SpanID = value
			continue
		}
		if value, ok := otelDebugLineValue(trimmed, "Name"); ok {
			current.Name = value
			continue
		}
		if strings.HasPrefix(trimmed, "->") {
			keyValue := strings.TrimSpace(strings.TrimPrefix(trimmed, "->"))
			key, raw, ok := strings.Cut(keyValue, ":")
			if ok {
				current.Attrs[strings.TrimSpace(key)] = otelDebugAttributeValue(raw)
			}
		}
	}
	finish()
	return spans
}

func otelDebugLineValue(line, key string) (string, bool) {
	left, right, ok := strings.Cut(line, ":")
	if !ok || strings.TrimSpace(left) != key {
		return "", false
	}
	return strings.TrimSpace(right), true
}

func otelDebugAttributeValue(raw string) string {
	value := strings.TrimSpace(raw)
	open := strings.Index(value, "(")
	if open >= 0 && strings.HasSuffix(value, ")") {
		return value[open+1 : len(value)-1]
	}
	return value
}

func requireOTelSpan(g Gomega, spans []otelDebugSpan, name, attrKey, attrValue string) otelDebugSpan {
	matches := otelSpansNamedWithAttr(spans, name, attrKey, attrValue)
	g.Expect(matches).NotTo(BeEmpty(), "expected span %q with %s=%s", name, attrKey, attrValue)
	if len(matches) == 0 {
		return otelDebugSpan{}
	}
	return matches[0]
}

func requireOTelSpanWithTrace(g Gomega, spans []otelDebugSpan, name, attrKey, attrValue, traceID string) otelDebugSpan {
	matches := otelSpansNamedWithAttrAndTrace(spans, name, attrKey, attrValue, traceID)
	g.Expect(matches).NotTo(BeEmpty(), "expected span %q with %s=%s in trace %s", name, attrKey, attrValue, traceID)
	if len(matches) == 0 {
		return otelDebugSpan{}
	}
	return matches[0]
}

func otelSpansNamed(spans []otelDebugSpan, name string) []otelDebugSpan {
	matches := []otelDebugSpan{}
	for _, span := range spans {
		if span.Name == name {
			matches = append(matches, span)
		}
	}
	return matches
}

func otelSpansNamedWithAttr(spans []otelDebugSpan, name, attrKey, attrValue string) []otelDebugSpan {
	matches := []otelDebugSpan{}
	for _, span := range spans {
		if span.Name == name && span.Attrs[attrKey] == attrValue {
			matches = append(matches, span)
		}
	}
	return matches
}

func otelSpansNamedWithAttrAndTrace(spans []otelDebugSpan, name, attrKey, attrValue, traceID string) []otelDebugSpan {
	matches := []otelDebugSpan{}
	for _, span := range spans {
		if span.TraceID == traceID && span.Name == name && span.Attrs[attrKey] == attrValue {
			matches = append(matches, span)
		}
	}
	return matches
}

func otelSpanIDs(spans []otelDebugSpan) []string {
	ids := make([]string, 0, len(spans))
	for _, span := range spans {
		if span.SpanID != "" {
			ids = append(ids, span.SpanID)
		}
	}
	return ids
}

func otelAllTraceIDs(spans []otelDebugSpan) []string {
	return sortedUniqueOTelSpanValues(spans, func(span otelDebugSpan) string { return span.TraceID })
}

func otelAllParents(spans []otelDebugSpan) []string {
	return sortedUniqueOTelSpanValues(spans, func(span otelDebugSpan) string { return span.ParentID })
}

func sortedUniqueOTelSpanValues(spans []otelDebugSpan, valueFn func(otelDebugSpan) string) []string {
	seen := map[string]struct{}{}
	for _, span := range spans {
		if value := valueFn(span); value != "" {
			seen[value] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func otelHasSpanWithTraceAndParent(spans []otelDebugSpan, traceID string, parentIDs []string) bool {
	parents := map[string]struct{}{}
	for _, id := range parentIDs {
		parents[id] = struct{}{}
	}
	for _, span := range spans {
		if span.TraceID != traceID {
			continue
		}
		if _, ok := parents[span.ParentID]; ok {
			return true
		}
	}
	return false
}

func otelCollectorLogs() string {
	cmd := exec.Command("kubectl", "logs", "deployment/"+otelCollectorName, "-n", namespace, "--tail=5000")
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to read collector logs")
	return output
}

func dumpOTelCollectorLogsForDiagnostics() {
	if !CurrentSpecReport().Failed() {
		return
	}

	cmd := exec.Command("kubectl", "logs", "deployment/"+otelCollectorName, "-n", namespace, "--tail=200")
	output, err := utils.Run(cmd)
	if err == nil && strings.TrimSpace(output) != "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "\n--- %s logs ---\n%s\n", otelCollectorName, output)
	}
}

func otelCollectorManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s-config
  namespace: %[2]s
data:
  collector.yaml: |
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
          http:
            endpoint: 0.0.0.0:4318
    processors:
      batch:
        timeout: 1s
    exporters:
      debug:
        verbosity: detailed
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [debug]
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: [debug]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: %[1]s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: %[1]s
    spec:
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: collector
          image: otel/opentelemetry-collector:0.111.0
          args:
            - --config=/etc/otelcol/collector.yaml
          ports:
            - name: otlp-grpc
              containerPort: 4317
            - name: otlp-http
              containerPort: 4318
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /etc/otelcol
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: %[1]s-config
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app.kubernetes.io/name: %[1]s
  ports:
    - name: otlp-grpc
      port: 4317
      targetPort: otlp-grpc
    - name: otlp-http
      port: 4318
      targetPort: otlp-http
`, otelCollectorName, namespace)
}

func otelFakeOpenAIManifest() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s-server
  namespace: %[2]s
data:
  server.py: |
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import json

    TOOL_ARGS = json.dumps({
        "path": "otel-e2e.txt",
        "content": "otel-e2e",
        "mode": "write",
        "create_dirs": True,
    })
    TOPOLOGY_CHILD_TOOL_ARGS = json.dumps({
        "path": "otel-topology-child.txt",
        "content": "otel topology child output",
        "mode": "write",
        "create_dirs": True,
    })
    TOPOLOGY_WORKER_AGENT = "%[5]s"

    def message_content_contains(messages, marker):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, str) and marker in content:
                return True
        return False

    def tool_message_contents(messages):
        return [
            m.get("content") or ""
            for m in messages
            if isinstance(m, dict) and m.get("role") == "tool"
        ]

    def delegated_task_name(messages):
        for content in tool_message_contents(messages):
            try:
                payload = json.loads(content)
            except Exception:
                continue
            if isinstance(payload, dict) and payload.get("taskName"):
                return payload["taskName"]
        return ""

    def has_wait_for_tasks_result(messages):
        for content in tool_message_contents(messages):
            try:
                payload = json.loads(content)
            except Exception:
                continue
            if isinstance(payload, dict) and "completed" in payload and "results" in payload:
                return True
        return False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            return

        def _json(self, status, payload):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/readyz":
                self._json(200, {"ok": True})
                return
            if self.path.endswith("/models"):
                self._json(200, {"data": [{"id": "%[3]s", "object": "model"}]})
                return
            self._json(404, {"error": {"message": "not found", "code": "not_found"}})

        def do_POST(self):
            length = int(self.headers.get("content-length", "0") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                request = json.loads(raw.decode("utf-8"))
            except Exception:
                request = {}

            if self.path.endswith("/responses"):
                self._json(404, {"error": {"message": "Not Found", "type": "invalid_request_error", "code": "invalid_url"}})
                return

            if self.path.endswith("/chat/completions"):
                model = request.get("model") or "%[3]s"
                messages = request.get("messages") or []
                is_topology_parent = message_content_contains(messages, "OTEL_TOPOLOGY_PARENT")
                is_topology_child = message_content_contains(messages, "OTEL_TOPOLOGY_CHILD")

                if is_topology_parent:
                    child_task = delegated_task_name(messages)
                    if not child_task:
                        delegate_args = json.dumps({
                            "agent": TOPOLOGY_WORKER_AGENT,
                            "prompt": "OTEL_TOPOLOGY_CHILD: use file_write once, then reply otel topology child complete.",
                            "timeout": "5m",
                        })
                        self._json(200, {
                            "id": "chatcmpl-otel-topology-delegate",
                            "object": "chat.completion",
                            "model": model,
                            "choices": [{
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": "",
                                    "tool_calls": [{
                                        "id": "call_otel_delegate_task",
                                        "type": "function",
                                        "function": {"name": "delegate_task", "arguments": delegate_args},
                                    }],
                                },
                                "finish_reason": "tool_calls",
                            }],
                            "usage": {"prompt_tokens": 19, "completion_tokens": 13, "total_tokens": 32},
                        })
                        return

                    if not has_wait_for_tasks_result(messages):
                        wait_args = json.dumps({"tasks": [child_task], "timeout": "5m"})
                        self._json(200, {
                            "id": "chatcmpl-otel-topology-wait",
                            "object": "chat.completion",
                            "model": model,
                            "choices": [{
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": "",
                                    "tool_calls": [{
                                        "id": "call_otel_wait_for_tasks",
                                        "type": "function",
                                        "function": {"name": "wait_for_tasks", "arguments": wait_args},
                                    }],
                                },
                                "finish_reason": "tool_calls",
                            }],
                            "usage": {"prompt_tokens": 23, "completion_tokens": 9, "total_tokens": 32},
                        })
                        return

                    self._json(200, {
                        "id": "chatcmpl-otel-topology-parent-final",
                        "object": "chat.completion",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": "otel topology complete"},
                            "finish_reason": "stop",
                        }],
                        "usage": {"prompt_tokens": 31, "completion_tokens": 4, "total_tokens": 35},
                    })
                    return

                if is_topology_child:
                    saw_child_tool_result = any(tool_message_contents(messages))
                    if not saw_child_tool_result:
                        self._json(200, {
                            "id": "chatcmpl-otel-topology-child-tool-call",
                            "object": "chat.completion",
                            "model": model,
                            "choices": [{
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": "",
                                    "tool_calls": [{
                                        "id": "call_otel_topology_child_file_write",
                                        "type": "function",
                                        "function": {"name": "file_write", "arguments": TOPOLOGY_CHILD_TOOL_ARGS},
                                    }],
                                },
                                "finish_reason": "tool_calls",
                            }],
                            "usage": {"prompt_tokens": 17, "completion_tokens": 8, "total_tokens": 25},
                        })
                        return

                    self._json(200, {
                        "id": "chatcmpl-otel-topology-child-final",
                        "object": "chat.completion",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": "otel topology child complete"},
                            "finish_reason": "stop",
                        }],
                        "usage": {"prompt_tokens": 21, "completion_tokens": 4, "total_tokens": 25},
                    })
                    return

                saw_tool_result = any(m.get("role") == "tool" for m in messages if isinstance(m, dict))
                if not saw_tool_result:
                    self._json(200, {
                        "id": "chatcmpl-otel-tool-call",
                        "object": "chat.completion",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "",
                                "tool_calls": [{
                                    "id": "call_otel_file_write",
                                    "type": "function",
                                    "function": {"name": "file_write", "arguments": TOOL_ARGS},
                                }],
                            },
                            "finish_reason": "tool_calls",
                        }],
                        "usage": {"prompt_tokens": 17, "completion_tokens": 11, "total_tokens": 28},
                    })
                    return

                self._json(200, {
                    "id": "chatcmpl-otel-final",
                    "object": "chat.completion",
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": "otel e2e complete"},
                        "finish_reason": "stop",
                    }],
                    "usage": {"prompt_tokens": 23, "completion_tokens": 5, "total_tokens": 28},
                })
                return

            self._json(404, {"error": {"message": "not found", "code": "not_found"}})

    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: %[1]s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: %[1]s
    spec:
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: fake-openai
          image: %[4]s
          imagePullPolicy: IfNotPresent
          command: ["python", "/etc/fake-openai/server.py"]
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
            periodSeconds: 2
            failureThreshold: 30
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: server
              mountPath: /etc/fake-openai
              readOnly: true
      volumes:
        - name: server
          configMap:
            name: %[1]s-server
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app.kubernetes.io/name: %[1]s
  ports:
    - name: http
      port: 8080
      targetPort: http
    `, otelFakeOpenAIName, namespace, otelModelName, otelFakeOpenAIImage, otelTopologyWorkerAgent)
}
