//go:build e2e
// +build e2e

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/orka-agents/orka/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "ghcr.io/orka-agents/orka:latest"

	// Worker, ACP runtime, clean-room publisher, and focused fixture images used by the e2e cluster.
	aiWorkerImage                = "ghcr.io/orka-agents/orka/ai-worker:latest"
	generalWorkerImage           = "ghcr.io/orka-agents/orka/general-worker:latest"
	acpCodexRuntimeImage         = "ghcr.io/orka-agents/orka/acp-codex-runtime:e2e"
	acpClaudeRuntimeImage        = "ghcr.io/orka-agents/orka/acp-claude-runtime:e2e"
	acpCopilotRuntimeImage       = "ghcr.io/orka-agents/orka/acp-copilot-runtime:e2e"
	acpOpencodeRuntimeImage      = "ghcr.io/orka-agents/orka/acp-opencode-runtime:e2e"
	workspacePublisherImage      = "ghcr.io/orka-agents/orka/workspace-publisher:e2e"
	gatewayReferenceAdapterImage = "ghcr.io/orka-agents/orka/gateway-reference-adapter:e2e"
	harnessV2FixtureImage        = "ghcr.io/orka-agents/orka/harness-v2-e2e-fixture:e2e"
	gatewayE2EEnvVar             = "E2E_GATEWAY"
	e2eEphemeralClusterEnvVar    = "E2E_EPHEMERAL_CLUSTER"
	managerRef                   string
	acpCodexRuntimeRef           string
	acpClaudeRuntimeRef          string
	acpCopilotRuntimeRef         string
	acpOpencodeRuntimeRef        string
	workspacePublisherRef        string
	e2eRegistryContainerName     string

	// E2E environment configuration (loaded from .env or environment)
	e2eOpenAIAPIKey            string
	e2eOpenAIBaseURL           string
	e2eOpenAIModel             string
	e2eAnthropicAPIKey         string
	e2eAnthropicBaseURL        string
	e2eAnthropicModel          string
	e2eGitHubToken             string
	e2eLiveCopilotProxyBaseURL string
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting orka e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("loading e2e environment file")
	earlyEnvProjectDir, _ := utils.GetProjectDir()
	loadEnvFile(filepath.Join(earlyEnvProjectDir, "test", "e2e", ".env"))

	By("building all Docker images")
	cmd := exec.Command("make", "docker-build-all",
		fmt.Sprintf("IMG=%s", managerImage),
		fmt.Sprintf("AI_WORKER_IMG=%s", aiWorkerImage),
		fmt.Sprintf("GENERAL_WORKER_IMG=%s", generalWorkerImage),
		fmt.Sprintf("ACP_CODEX_RUNTIME_IMG=%s", acpCodexRuntimeImage),
		fmt.Sprintf("ACP_CLAUDE_RUNTIME_IMG=%s", acpClaudeRuntimeImage),
		fmt.Sprintf("ACP_COPILOT_RUNTIME_IMG=%s", acpCopilotRuntimeImage),
		fmt.Sprintf("ACP_OPENCODE_RUNTIME_IMG=%s", acpOpencodeRuntimeImage),
		fmt.Sprintf("WORKSPACE_PUBLISHER_IMG=%s", workspacePublisherImage),
	)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build Docker images")

	By("building the harness-v2 E2E fixture image")
	cmd = exec.Command("docker", "build", "-t", harnessV2FixtureImage,
		"-f", "cmd/orka-harness-v2-e2e-fixture/Dockerfile", ".")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build harness-v2 E2E fixture image")

	if gatewayE2EEnabled() {
		By("building the Gateway reference adapter Docker image")
		cmd = exec.Command("docker", "build", "-t", gatewayReferenceAdapterImage,
			"-f", "cmd/orka-gateway-reference-adapter/Dockerfile", ".")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build Gateway reference adapter image")
	}

	By("loading all images into Kind cluster")

	images := []string{
		managerImage,
		aiWorkerImage,
		generalWorkerImage,
		harnessV2FixtureImage,
	}
	if gatewayE2EEnabled() {
		images = append(images, gatewayReferenceAdapterImage)
	}
	for _, img := range images {
		err = utils.LoadImageToKindClusterWithName(img)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to load image %s into Kind", img))
	}

	By("publishing digest-pinned ACP and publisher images to a Kind-local registry")
	registry, registryContainer, err := prepareKindLocalRegistry()
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to prepare Kind-local registry")
	e2eRegistryContainerName = registryContainer
	managerRef, err = pushImageToLocalRegistry(registry, managerImage, "orka/controller:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish controller image")
	acpCodexRuntimeRef, err = pushImageToLocalRegistry(registry, acpCodexRuntimeImage, "orka/acp-codex-runtime:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish Codex ACP runtime image")
	acpClaudeRuntimeRef, err = pushImageToLocalRegistry(registry, acpClaudeRuntimeImage, "orka/acp-claude-runtime:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish Claude ACP runtime image")
	acpCopilotRuntimeRef, err = pushImageToLocalRegistry(registry, acpCopilotRuntimeImage, "orka/acp-copilot-runtime:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish Copilot ACP runtime image")
	acpOpencodeRuntimeRef, err = pushImageToLocalRegistry(registry, acpOpencodeRuntimeImage, "orka/acp-opencode-runtime:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish OpenCode ACP runtime image")
	workspacePublisherRef, err = pushImageToLocalRegistry(registry, workspacePublisherImage, "orka/workspace-publisher:e2e")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish workspace publisher image")

	By("loading e2e environment configuration")
	projectDir, _ := utils.GetProjectDir()
	loadEnvFile(filepath.Join(projectDir, "test", "e2e", ".env"))
	e2eOpenAIAPIKey = os.Getenv("E2E_OPENAI_API_KEY")
	e2eOpenAIBaseURL = os.Getenv("E2E_OPENAI_BASE_URL")
	e2eOpenAIModel = os.Getenv("E2E_OPENAI_MODEL")
	e2eAnthropicAPIKey = os.Getenv("E2E_ANTHROPIC_API_KEY")
	e2eAnthropicBaseURL = os.Getenv("E2E_ANTHROPIC_BASE_URL")
	e2eAnthropicModel = os.Getenv("E2E_ANTHROPIC_MODEL")
	e2eGitHubToken = os.Getenv("E2E_GITHUB_TOKEN")
	e2eLiveCopilotProxyBaseURL = firstSetEnv(
		"E2E_LIVE_COPILOT_PROXY_BASE_URL",
		"E2E_COPILOT_PROXY_BASE_URL",
		"COPILOT_PROXY_BASE_URL",
	)

	By("bootstrapping manager namespace identity")
	cmd = exec.Command("bash", filepath.Join(projectDir, "scripts", "lib", "ensure-static-mode-namespace.sh"),
		"kubectl", namespace, "harness-v2")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to bootstrap manager namespace identity")

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("creating the Vekil namespace required by the production ingress policy")
	cmd = exec.Command("kubectl", "create", "namespace", "vekil-system", "--dry-run=client", "-o", "yaml")
	vekilNamespace, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to render the Vekil namespace")
	cmd = exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(vekilNamespace)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create the Vekil namespace")

	By("creating e2e K8s secrets from environment variables")
	if e2eOpenAIAPIKey != "" {
		err = createK8sSecret("e2e-openai-secret", namespace, map[string]string{"api-key": e2eOpenAIAPIKey})
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create OpenAI secret")
		_, _ = fmt.Fprintf(GinkgoWriter, "Created e2e-openai-secret\n")
	}
	if e2eAnthropicAPIKey != "" {
		anthropicSecretData := map[string]string{"ANTHROPIC_API_KEY": e2eAnthropicAPIKey}
		if e2eAnthropicBaseURL != "" {
			anthropicSecretData["ANTHROPIC_BASE_URL"] = e2eAnthropicBaseURL
		}
		err = createK8sSecret("e2e-anthropic-secret", namespace, anthropicSecretData)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create Anthropic secret")
		_, _ = fmt.Fprintf(GinkgoWriter, "Created e2e-anthropic-secret\n")
	}
	if e2eGitHubToken != "" {
		err = createK8sSecret("e2e-github-secret", namespace, map[string]string{"GITHUB_TOKEN": e2eGitHubToken})
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create GitHub secret")
		_, _ = fmt.Fprintf(GinkgoWriter, "Created e2e-github-secret\n")
	}

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("waiting for CRDs to be established")
	requiredCRDs := []string{
		"tasks.core.orka.ai",
		"agents.core.orka.ai",
		"agentruntimes.core.orka.ai",
		"runtimepools.core.orka.ai",
		"tools.core.orka.ai",
		"providers.core.orka.ai",
		"skills.core.orka.ai",
		"repositoryscans.core.orka.ai",
		"repositorymonitors.core.orka.ai",
		"substrateactorpools.core.orka.ai",
	}
	if gatewayE2EEnabled() {
		requiredCRDs = append(requiredCRDs,
			"gatewayclasses.gateway.orka.ai",
			"gateways.gateway.orka.ai",
			"gatewaybindings.gateway.orka.ai",
		)
	}
	Eventually(func(g Gomega) {
		for _, crd := range requiredCRDs {

			cmd := exec.Command("kubectl", "wait", "--for=condition=Established",
				"crd/"+crd, "--timeout=30s")
			_, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred(), "CRD %s not established", crd)
		}
	}, 60*time.Second, time.Second).Should(Succeed())

	By("bootstrapping test-only admission TLS")
	cmd = exec.Command("bash", filepath.Join(projectDir, "scripts", "lib", "e2e-admission-tls.sh"))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to bootstrap admission TLS")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy",
		fmt.Sprintf("IMG=%s", managerRef),
		fmt.Sprintf("ACP_CODEX_RUNTIME_IMG=%s", acpCodexRuntimeRef),
		fmt.Sprintf("ACP_CLAUDE_RUNTIME_IMG=%s", acpClaudeRuntimeRef),
		fmt.Sprintf("ACP_COPILOT_RUNTIME_IMG=%s", acpCopilotRuntimeRef),
		fmt.Sprintf("ACP_OPENCODE_RUNTIME_IMG=%s", acpOpencodeRuntimeRef),
		fmt.Sprintf("WORKSPACE_PUBLISHER_IMG=%s", workspacePublisherRef),
	)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

	By("resolving the controller-manager deployment name")
	var controllerManagerDeployment string
	Eventually(func(g Gomega) {
		name, err := controllerManagerDeploymentName()
		g.Expect(err).NotTo(HaveOccurred(), "Failed to resolve the controller-manager deployment")
		g.Expect(name).NotTo(BeEmpty(), "controller-manager deployment name was empty")
		controllerManagerDeployment = name
	}, 30*time.Second, time.Second).Should(Succeed())
	_, _ = fmt.Fprintf(GinkgoWriter, "Resolved controller-manager deployment: %s\n", controllerManagerDeployment)

	By("patching the controller-manager deployment to use kind-loaded images")
	cmd = exec.Command(
		"kubectl", "patch", "deployment", controllerManagerDeployment, "-n", namespace, "--type=strategic",
		"-p", `{"spec":{"template":{"spec":{"containers":[{"name":"manager","imagePullPolicy":"IfNotPresent","env":[{"name":"ORKA_ACP_IDLE_POOL_TTL","value":"2m"}]}]}}}}`,
	)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to patch controller-manager imagePullPolicy")

	By("waiting for controller-manager to be ready")
	cmd = exec.Command("kubectl", "rollout", "status", "deployment/"+controllerManagerDeployment,
		"-n", namespace, "--timeout=5m")
	_, err = utils.Run(cmd)
	if err != nil {
		dumpControllerManagerDiagnostics()
	}
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed waiting for the controller-manager rollout")
})

var _ = AfterSuite(func() {
	if e2eEphemeralClusterEnabled() {
		By("skipping resource cleanup for the ephemeral E2E cluster")
		return
	}

	By("cleaning up the curl pod for metrics")
	runBoundedE2ECleanup(30*time.Second, "kubectl", "delete", "pod", "curl-metrics",
		"-n", namespace, "--ignore-not-found", "--wait=true", "--timeout=20s")

	By("deleting Tasks before their ACP RuntimePools and other finalizer owners")
	deleteAllE2EResources("tasks.core.orka.ai", true, 90*time.Second)

	By("deleting Tools before execution workspaces and actor pools")
	deleteAllE2EResources("tools.core.orka.ai", true, 45*time.Second)

	By("deleting execution workspaces before workspace classes and providers")
	deleteAllE2EResources("executionworkspaces.workspace.orka.ai", true, 45*time.Second)

	By("deleting controller-owned ACP RuntimePools while the controller is still running")
	deleteAllE2EResources("runtimepools.core.orka.ai", true, 60*time.Second)

	By("deleting remaining namespaced finalizer owners")
	for _, resource := range []string{
		"substrateactorpools.core.orka.ai",
		"outboundaccesspolicies.core.orka.ai",
		"executionworkspacepools.workspace.orka.ai",
		"executionworkspaceclasses.workspace.orka.ai",
	} {
		deleteAllE2EResources(resource, true, 45*time.Second)
	}

	By("deleting cluster-scoped execution workspace providers last")
	deleteAllE2EResources("executionworkspaceproviders.workspace.orka.ai", false, 45*time.Second)

	By("cleaning up e2e secrets")
	for _, s := range []string{"e2e-openai-secret", "e2e-anthropic-secret", "e2e-github-secret"} {
		runBoundedE2ECleanup(30*time.Second, "kubectl", "delete", "secret", s,
			"-n", namespace, "--ignore-not-found", "--wait=true", "--timeout=20s")
	}

	By("undeploying the controller-manager")
	runBoundedE2ECleanup(2*time.Minute, "make", "undeploy", "ignore-not-found=true")

	By("uninstalling CRDs")
	runBoundedE2ECleanup(2*time.Minute, "make", "uninstall", "ignore-not-found=true")

	By("removing manager namespace")
	runBoundedE2ECleanup(60*time.Second, "kubectl", "delete", "ns", namespace,
		"--ignore-not-found", "--wait=true", "--timeout=45s")

	if e2eRegistryContainerName != "" {
		By("removing the Kind-local image registry")
		runBoundedE2ECleanup(30*time.Second, "docker", "rm", "-f", e2eRegistryContainerName)
	}
})

func deleteAllE2EResources(resource string, namespaced bool, timeout time.Duration) {
	args := []string{
		"delete", resource, "--all", "--ignore-not-found", "--wait=true", "--timeout=" + timeout.String(),
	}
	if namespaced {
		args = append(args, "-n", namespace)
	}
	runBoundedE2ECleanup(timeout+10*time.Second, "kubectl", args...)
}

func runBoundedE2ECleanup(timeout time.Duration, name string, args ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	// CommandContext kills only the direct process. A timed-out make recipe
	// can leave kubectl holding the output pipes open, so Wait would otherwise
	// outlive this helper's deadline and eventually trip the suite timeout.
	cmd.WaitDelay = time.Second
	if _, err := utils.Run(cmd); err != nil {
		if ctx.Err() != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "cleanup command timed out after %s: %s %s\n",
				timeout, name, strings.Join(args, " "))
			return
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup command failed: %s %s: %v\n",
			name, strings.Join(args, " "), err)
	}
}

// loadEnvFile reads a .env file and sets environment variables that are not already set.
func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "No .env file at %s (using environment directly)\n", path)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

func dumpControllerManagerDiagnostics() {
	By("dumping controller-manager diagnostics")

	for _, args := range [][]string{
		{"get", "pods", "-l", "control-plane=controller-manager", "-n", namespace, "-o", "wide"},
		{"describe", "pods", "-l", "control-plane=controller-manager", "-n", namespace},
		{"logs", "-l", "control-plane=controller-manager", "-n", namespace, "--all-containers=true", "--prefix=true", "--tail=500"},
		{"get", "lease", "03b49a10.orka.ai", "-n", namespace, "-o", "yaml"},
		{"get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
		{"get", "deployments", "-l", "control-plane=controller-manager", "-n", namespace, "-o", "yaml"},
	} {
		cmd := exec.Command("kubectl", args...)
		output, err := utils.Run(cmd)
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "diagnostic command failed: kubectl %s\n%v\n", strings.Join(args, " "), err)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "diagnostic output: kubectl %s\n%s\n", strings.Join(args, " "), output)
	}
}

func controllerManagerDeploymentName() (string, error) {
	cmd := exec.Command("kubectl", "get", "deployments", "-l", "control-plane=controller-manager",
		"-n", namespace, "-o", "jsonpath={.items[0].metadata.name}")
	output, err := utils.Run(cmd)
	if err != nil {
		return "", err
	}

	name := strings.TrimSpace(output)
	if name == "" {
		return "", fmt.Errorf("no controller-manager deployment found")
	}

	return name, nil
}

// createK8sSecret creates a Kubernetes Secret with the given key-value data.
func createK8sSecret(name, ns string, data map[string]string) error {
	secret := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]string{
			"name":      name,
			"namespace": ns,
		},
		"type":       "Opaque",
		"stringData": data,
	}
	manifest, err := json.Marshal(secret)
	if err != nil {
		return fmt.Errorf("failed to marshal secret: %w", err)
	}

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(string(manifest))
	_, err = utils.Run(cmd)
	return err
}

func prepareKindLocalRegistry() (string, string, error) {
	cluster := strings.TrimSpace(os.Getenv("KIND_CLUSTER"))
	if cluster == "" {
		cluster = "kind"
	}
	containerName := sanitizeDockerName(cluster + "-e2e-registry")
	_, _ = utils.Run(exec.Command("docker", "rm", "-f", containerName))
	if output, err := utils.Run(exec.Command(
		"docker", "run", "-d", "--name", containerName, "--network", "kind", "-p", "127.0.0.1::5000", "registry:2",
	)); err != nil {
		return "", "", fmt.Errorf("start local registry: %s: %w", strings.TrimSpace(output), err)
	}

	output, err := utils.Run(exec.Command("docker", "port", containerName, "5000/tcp"))
	if err != nil {
		return "", "", err
	}
	hostPort := strings.TrimSpace(output)
	if comma := strings.IndexByte(hostPort, ','); comma >= 0 {
		hostPort = hostPort[:comma]
	}
	colon := strings.LastIndexByte(hostPort, ':')
	if colon < 0 || colon == len(hostPort)-1 {
		return "", "", fmt.Errorf("unexpected registry port mapping %q", hostPort)
	}
	registry := "localhost:" + hostPort[colon+1:]
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, requestErr := client.Get("http://" + registry + "/v2/")
		if requestErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			return "", "", fmt.Errorf("local registry %s did not become ready", registry)
		}
		time.Sleep(250 * time.Millisecond)
	}

	kindBinary := firstSetEnv("KIND")
	if kindBinary == "" {
		kindBinary = "kind"
	}
	nodesOutput, err := utils.Run(exec.Command(kindBinary, "get", "nodes", "--name", cluster))
	if err != nil {
		return "", "", err
	}
	hosts := fmt.Sprintf(`server = "http://%s"
[host."http://%s:5000"]
  capabilities = ["pull", "resolve"]
`, registry, containerName)
	for _, node := range utils.GetNonEmptyLines(nodesOutput) {
		dir := "/etc/containerd/certs.d/" + registry
		if _, err := utils.Run(exec.Command("docker", "exec", node, "mkdir", "-p", dir)); err != nil {
			return "", "", err
		}
		cmd := exec.Command("docker", "exec", "-i", node, "sh", "-c", "cat > "+dir+"/hosts.toml")
		cmd.Stdin = strings.NewReader(hosts)
		if _, err := utils.Run(cmd); err != nil {
			return "", "", err
		}
	}
	return registry, containerName, nil
}

func pushImageToLocalRegistry(registry, sourceImage, targetPath string) (string, error) {
	target := registry + "/" + targetPath
	if _, err := utils.Run(exec.Command("docker", "tag", sourceImage, target)); err != nil {
		return "", err
	}
	if _, err := utils.Run(exec.Command("docker", "push", target)); err != nil {
		return "", err
	}
	output, err := utils.Run(exec.Command("docker", "image", "inspect", "--format", `{{range .RepoDigests}}{{println .}}{{end}}`, target))
	if err != nil {
		return "", err
	}
	prefix := strings.TrimSuffix(target, ":e2e") + "@sha256:"
	for _, line := range utils.GetNonEmptyLines(output) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return line, nil
		}
	}
	return "", fmt.Errorf("no digest reference found for %s", target)
}

func sanitizeDockerName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(value) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-_.")

}

func gatewayE2EEnabled() bool {
	return e2eFlagEnabled(gatewayE2EEnvVar)
}

func e2eEphemeralClusterEnabled() bool {
	return e2eFlagEnabled(e2eEphemeralClusterEnvVar)
}

func e2eFlagEnabled(name string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes"

}

func firstSetEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
