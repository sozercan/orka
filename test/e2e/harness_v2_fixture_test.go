//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/gomega"

	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
	"github.com/orka-agents/orka/test/utils"
)

const (
	harnessV2FixtureControllerToken  = "controller-token-for-e2e-only-000001"
	harnessV2FixtureCapabilitySecret = "capability-secret-for-e2e-only-0001"
)

func deployHarnessV2Fixture(runtimeName, deploymentName, serviceName, authSecretName string) error {
	controllerEpoch, err := currentControllerEpoch()
	if err != nil {
		return err
	}
	manifest, err := harnessV2FixtureManifest(runtimeName, deploymentName, serviceName, authSecretName, controllerEpoch)
	if err != nil {
		return err
	}
	items, ok := manifest["items"].([]any)
	if !ok || len(items) != 4 {
		return fmt.Errorf("harness-v2 fixture manifest has invalid item shape")
	}
	if err := applyManifestJSON(map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items":      items[:3],
	}); err != nil {
		return err
	}
	if err := gatewayE2EWaitForDeployment(deploymentName, 2*time.Minute); err != nil {
		return err
	}
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "endpointslice", "-n", namespace,
			"-l", "kubernetes.io/service-name="+serviceName,
			"-o", `jsonpath={range .items[*].endpoints[?(@.conditions.ready==true)]}{.addresses[0]}{"\n"}{end}`)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty())
	}, time.Minute, time.Second).Should(Succeed())
	runtimeManifest, ok := items[3].(map[string]any)
	if !ok {
		return fmt.Errorf("harness-v2 fixture AgentRuntime manifest has invalid shape")
	}
	if err := applyManifestJSON(runtimeManifest); err != nil {
		return err
	}
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "agentruntime", runtimeName, "-n", namespace,
			"-o", "jsonpath={.status.ready}{\"/\"}{.metadata.generation}{\"/\"}{.status.observedGeneration}{\"/\"}{.status.conditions[?(@.type==\"Ready\")].reason}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		parts := strings.Split(output, "/")
		g.Expect(parts).To(HaveLen(4), output)
		g.Expect(parts[0]).To(Equal("true"), output)
		g.Expect(parts[2]).To(Equal(parts[1]), output)
		g.Expect(parts[3]).To(Equal("ConformancePassed"), output)
	}, 3*time.Minute, time.Second).Should(Succeed())
	return nil
}

func currentControllerEpoch() (uint64, error) {
	cmd := exec.Command("kubectl", "get", "controllerepoch", "-n", namespace,
		"-o", `jsonpath={.items[?(@.spec.name=="orka-controller")].status.epoch}`)
	output, err := utils.Run(cmd)
	if err != nil {
		return 0, fmt.Errorf("get current controller epoch: %w", err)
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(output), 10, 64)
	if err != nil || epoch == 0 {
		return 0, fmt.Errorf("parse current controller epoch %q", strings.TrimSpace(output))
	}
	return epoch, nil
}

func harnessV2FixtureManifest(runtimeName, deploymentName, serviceName, authSecretName string, controllerEpoch uint64) (map[string]any, error) {
	profile, err := conformancetest.DeterministicProfile(runtimeName)
	if err != nil {
		return nil, err
	}
	profileDigest, err := harnessv2.CanonicalProfileDigest(profile)
	if err != nil {
		return nil, fmt.Errorf("canonicalize harness-v2 fixture profile: %w", err)
	}
	limits := harnessv2.DefaultProtocolLimits()
	governance := harnessv2.StrictWorkspaceGovernanceCapabilities()
	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", serviceName, namespace, gatewayE2ERuntimePort)
	labels := map[string]any{"app.kubernetes.io/name": deploymentName}

	return map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items": []any{
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata": map[string]any{
					"name":      authSecretName,
					"namespace": namespace,
					"labels": map[string]any{
						"orka.ai/agent-runtime-auth": "true",
						"orka.ai/agent-runtime-name": runtimeName,
					},
					"annotations": map[string]any{"orka.ai/agent-runtime-endpoint": endpoint},
				},
				"type": "Opaque",
				"stringData": map[string]any{
					"controller-token":  harnessV2FixtureControllerToken,
					"capability-secret": harnessV2FixtureCapabilitySecret,
				},
			},
			map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"name":      deploymentName,
					"namespace": namespace,
				},
				"spec": map[string]any{
					"replicas": 1,
					"selector": map[string]any{"matchLabels": labels},
					"template": map[string]any{
						"metadata": map[string]any{"labels": labels},
						"spec": map[string]any{
							"automountServiceAccountToken": false,
							"securityContext": map[string]any{
								"runAsNonRoot":   true,
								"runAsUser":      65532,
								"runAsGroup":     65532,
								"seccompProfile": map[string]any{"type": "RuntimeDefault"},
							},
							"containers": []any{map[string]any{
								"name":            "runtime",
								"image":           harnessV2FixtureImage,
								"imagePullPolicy": "IfNotPresent",
								"env": []any{
									map[string]any{"name": "ORKA_E2E_RUNTIME_NAME", "value": runtimeName},
									map[string]any{"name": "ORKA_E2E_CONTROLLER_EPOCH", "value": strconv.FormatUint(controllerEpoch, 10)},
									map[string]any{"name": "ORKA_E2E_CONTROLLER_TOKEN", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": authSecretName, "key": "controller-token"}}},
									map[string]any{"name": "ORKA_E2E_CAPABILITY_SECRET", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": authSecretName, "key": "capability-secret"}}},
								},
								"ports": []any{map[string]any{"name": "http", "containerPort": gatewayE2ERuntimePort}},
								"readinessProbe": map[string]any{
									"httpGet":       map[string]any{"path": harnessv2.HealthPath, "port": "http"},
									"periodSeconds": 2,
								},
								"securityContext": map[string]any{
									"allowPrivilegeEscalation": false,
									"readOnlyRootFilesystem":   true,
									"capabilities":             map[string]any{"drop": []any{"ALL"}},
								},
							}},
						},
					},
				},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata":   map[string]any{"name": serviceName, "namespace": namespace},
				"spec": map[string]any{
					"selector": labels,
					"ports":    []any{map[string]any{"name": "http", "port": gatewayE2ERuntimePort, "targetPort": "http"}},
				},
			},
			map[string]any{
				"apiVersion": "core.orka.ai/v1alpha1",
				"kind":       "AgentRuntime",
				"metadata":   map[string]any{"name": runtimeName, "namespace": namespace},
				"spec": map[string]any{
					"contractVersion": "orka.harness.v2",
					"deployment":      map[string]any{"mode": "external-endpoint", "endpoint": endpoint},
					"clientAuth": map[string]any{
						"controllerBearerTokenSecretRef": map[string]any{"name": authSecretName, "key": "controller-token"},
						"operationCapabilitySecretRef":   map[string]any{"name": authSecretName, "key": "capability-secret"},
					},
					"capabilities": map[string]any{
						"runtimeInstanceID": runtimeName,
						"profile": map[string]any{
							"digest":                   string(profileDigest),
							"digestSchemaVersion":      int32(harnessv2.ProfileDigestSchemaVersion),
							"acpProfile":               profile.ACPProfile,
							"adapterName":              runtimeName,
							"adapterDigest":            profile.AdapterDigests[runtimeName],
							"providerKind":             profile.ProviderKind,
							"model":                    profile.Model,
							"agentConfigurationDigest": profile.AgentConfigurationDigest,
							"toolPolicyDigest":         profile.ToolPolicyDigest,
							"approvalPolicyDigest":     profile.ApprovalPolicyDigest,
							"mcpConfigurationDigest":   profile.MCPConfigurationDigest,
							"workspaceIntent":          string(profile.WorkspaceIntent),
							"proxyCredentialRole":      profile.ProxyCredentialRole,
							"proxyCredentialScope":     profile.ProxyCredentialScope,
							"resourceClass":            profile.ResourceClass,
						},
						"mcpPolicy": map[string]any{
							"allowedTools":          []any{},
							"disallowedTools":       []any{},
							"allowBash":             false,
							"approvalRequiredTools": []any{},
						},
						"limits": map[string]any{
							"maxResidentSessions":      limits.MaxResidentSessions,
							"maxConcurrentPrompts":     limits.MaxConcurrentPrompts,
							"maxRequestBytes":          limits.MaxRequestBytes,
							"maxEventLineBytes":        limits.MaxEventLineBytes,
							"maxTerminalResultBytes":   limits.MaxTerminalResultBytes,
							"maxBufferedEvents":        limits.MaxBufferedEvents,
							"maxUpdateEventsPerSecond": limits.MaxUpdateEventsPerSecond,
							"minPromptLeaseMillis":     limits.MinPromptLeaseMillis,
							"maxPromptLeaseMillis":     limits.MaxPromptLeaseMillis,
							"maxPendingPermissions":    limits.MaxPendingPermissions,
							"maxWorkspaceDeltaBytes":   limits.MaxWorkspaceDeltaBytes,
						},
						"supportsDrain":                   true,
						"supportsPublicationFinalization": false,
						"workspaceGovernance": map[string]any{
							"mode":                            string(governance.Mode),
							"trusted":                         governance.Trusted,
							"orkaOwnedWorkspaceDeltas":        governance.OrkaOwnedWorkspaceDeltas,
							"promptScopedBrokerAuthorization": governance.PromptScopedBrokerAuthorization,
							"noDirectSCMPublication":          governance.NoDirectSCMPublication,
							"orkaOwnedCleanRoomPublication":   governance.OrkaOwnedCleanRoomPublication,
							"exactInstanceFencing":            governance.ExactInstanceFencing,
							"duplicateSafeMutations":          governance.DuplicateSafeMutations,
							"cancellationSettlement":          governance.CancellationSettlement,
						},
					},
				},
			},
		},
	}, nil
}
