package main

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	canonicalProductionControllerUsername = "system:serviceaccount:orka-system:orka-controller-manager"
	staticChartTestNamespace              = "orka-test"
	webhookPortName                       = "webhook"
	sharedAdmissionVariant                = "shared"
	releaseLocalAdmissionVariant          = "release local"
)

func TestControllerWebhooksAreReleaseLocalAndModeScoped(t *testing.T) {
	digest := "sha256:" + strings.Repeat("3", 64)
	for _, mode := range []string{"harness-v1", "harness-v2"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{
				"--set-string", "controller.mode=" + mode,
				"--show-only", "templates/controller-validating-webhook.yaml",
			}
			if mode == "harness-v1" {
				args = append(args,
					"--set-string", "harnessV1.image.digest="+digest,
					"--set-string", "harnessV1.auth.existingSecret=harness-wrapper-auth",
					"--set-string", "harnessV1.tls.existingSecret=harness-wrapper-tls",
				)
			}

			rendered := requireHelmRender(t, args...)
			configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
			if err := yaml.Unmarshal([]byte(rendered), &configuration); err != nil {
				t.Fatalf("decode controller validating webhook configuration: %v", err)
			}
			if configuration.Name != "test-orka-controller" {
				t.Fatalf("controller webhook name = %q, want test-orka-controller", configuration.Name)
			}

			webhooks := make(map[string]admissionregistrationv1.ValidatingWebhook, len(configuration.Webhooks))
			for _, webhook := range configuration.Webhooks {
				webhooks[webhook.Name] = webhook
				if !strings.HasSuffix(webhook.Name, "."+mode+".orka.ai") {
					t.Errorf("webhook name %q is not scoped to mode %q", webhook.Name, mode)
				}
				if webhook.FailurePolicy == nil || *webhook.FailurePolicy != admissionregistrationv1.Fail {
					t.Errorf("%s failurePolicy = %v, want Fail", webhook.Name, webhook.FailurePolicy)
				}
				service := webhook.ClientConfig.Service
				if service == nil || service.Name != "test-orka-webhook" || service.Namespace != staticChartTestNamespace ||
					service.Port == nil || *service.Port != 443 {
					t.Errorf("%s service = %#v, want test-orka-webhook:443 in orka-test", webhook.Name, service)
				}
				selector := webhook.NamespaceSelector
				if strings.HasPrefix(webhook.Name, "namespace-mode.") {
					selector = webhook.ObjectSelector
				}
				if selector == nil || selector.MatchLabels["orka.ai/controller-mode"] != mode {
					t.Errorf("%s execution-mode selector = %#v, want %q", webhook.Name, selector, mode)
				}
				if selector == nil || selector.MatchLabels["kubernetes.io/metadata.name"] != staticChartTestNamespace {
					t.Errorf("%s namespace selector = %#v, want orka-test", webhook.Name, selector)
				}
			}

			_, hasTaskWorkspace := webhooks["task-workspace-class."+mode+".orka.ai"]
			_, hasToolWorkspace := webhooks["tool-workspace-class."+mode+".orka.ai"]
			_, hasAttachmentSecret := webhooks["workspace-attachment-secret."+mode+".orka.ai"]
			_, hasSuspendQuotaLease := webhooks["acp-suspend-quota-lease."+mode+".orka.ai"]
			wantWorkspace := mode == "harness-v2"
			if hasTaskWorkspace != wantWorkspace || hasToolWorkspace != wantWorkspace ||
				hasAttachmentSecret != wantWorkspace || hasSuspendQuotaLease != wantWorkspace {
				t.Fatalf("workspace webhooks present = task:%t tool:%t attachment Secret:%t suspend quota Lease:%t, want %t",
					hasTaskWorkspace, hasToolWorkspace, hasAttachmentSecret, hasSuspendQuotaLease, wantWorkspace)
			}
		})
	}
}

func TestAttachmentSecretWebhooksRouteProtectedIntegrityWrites(t *testing.T) {
	sharedPath := filepath.Join("..", "..", "..", "config", "orka-admission-webhooks", "validating_webhook.yaml")
	sharedManifest, err := os.ReadFile(sharedPath)
	if err != nil {
		t.Fatalf("read standalone admission webhooks: %v", err)
	}
	chartManifest := []byte(requireHelmRender(t,
		"--set-string", "controller.mode=harness-v2",
		"--show-only", "templates/controller-validating-webhook.yaml",
	))

	for _, test := range []struct {
		name        string
		manifest    []byte
		webhookName string
	}{
		{name: sharedAdmissionVariant, manifest: sharedManifest, webhookName: "workspaceattachmentsecret.core.orka.ai"},
		{
			name: releaseLocalAdmissionVariant, manifest: chartManifest,
			webhookName: "workspace-attachment-secret.harness-v2.orka.ai",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
			if err := yaml.Unmarshal(test.manifest, &configuration); err != nil {
				t.Fatalf("decode validating webhook configuration: %v", err)
			}
			var attachmentWebhook *admissionregistrationv1.ValidatingWebhook
			for i := range configuration.Webhooks {
				if configuration.Webhooks[i].Name == test.webhookName {
					attachmentWebhook = &configuration.Webhooks[i]
					break
				}
			}
			if attachmentWebhook == nil {
				t.Fatalf("%s is missing", test.webhookName)
			}
			if attachmentWebhook.FailurePolicy == nil || *attachmentWebhook.FailurePolicy != admissionregistrationv1.Fail {
				t.Fatalf("failurePolicy = %v, want Fail", attachmentWebhook.FailurePolicy)
			}
			if attachmentWebhook.ClientConfig.Service == nil || attachmentWebhook.ClientConfig.Service.Path == nil ||
				*attachmentWebhook.ClientConfig.Service.Path != "/validate-v1-secret-workspace-attachment" {
				t.Fatalf("client service = %#v, want workspace attachment Secret handler", attachmentWebhook.ClientConfig.Service)
			}
			if len(attachmentWebhook.Rules) != 1 {
				t.Fatalf("rules = %#v, want one Secret rule", attachmentWebhook.Rules)
			}
			rule := attachmentWebhook.Rules[0]
			wantOperations := []admissionregistrationv1.OperationType{
				admissionregistrationv1.Create, admissionregistrationv1.Update, admissionregistrationv1.Delete,
			}
			if !slices.Equal(rule.Operations, wantOperations) {
				t.Errorf("operations = %#v, want %#v", rule.Operations, wantOperations)
			}
			if !slices.Equal(rule.APIGroups, []string{""}) ||
				!slices.Equal(rule.APIVersions, []string{"v1"}) ||
				!slices.Equal(rule.Resources, []string{"secrets"}) {
				t.Errorf("rule = %#v, want core/v1 Secrets", rule.Rule)
			}
			selector := attachmentWebhook.ObjectSelector
			if selector == nil || len(selector.MatchExpressions) != 1 ||
				selector.MatchExpressions[0].Key != "workspace.orka.ai/attachment-for" ||
				selector.MatchExpressions[0].Operator != metav1.LabelSelectorOpExists {
				t.Fatalf("objectSelector = %#v, want attachment label Exists", selector)
			}
		})
	}
}

func TestSuspendQuotaLeaseWebhooksRouteProtectedWrites(t *testing.T) {
	sharedPath := filepath.Join("..", "..", "..", "config", "orka-admission-webhooks", "validating_webhook.yaml")
	sharedManifest, err := os.ReadFile(sharedPath)
	if err != nil {
		t.Fatalf("read standalone admission webhooks: %v", err)
	}
	chartManifest := []byte(requireHelmRender(t,
		"--set-string", "controller.mode=harness-v2",
		"--show-only", "templates/controller-validating-webhook.yaml",
	))

	for _, test := range []struct {
		name        string
		manifest    []byte
		webhookName string
	}{
		{name: sharedAdmissionVariant, manifest: sharedManifest, webhookName: "acpsuspendquotalease.core.orka.ai"},
		{
			name: releaseLocalAdmissionVariant, manifest: chartManifest,
			webhookName: "acp-suspend-quota-lease.harness-v2.orka.ai",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
			if err := yaml.Unmarshal(test.manifest, &configuration); err != nil {
				t.Fatalf("decode validating webhook configuration: %v", err)
			}
			var quotaWebhook *admissionregistrationv1.ValidatingWebhook
			for i := range configuration.Webhooks {
				if configuration.Webhooks[i].Name == test.webhookName {
					quotaWebhook = &configuration.Webhooks[i]
					break
				}
			}
			if quotaWebhook == nil {
				t.Fatalf("%s is missing", test.webhookName)
			}
			if quotaWebhook.FailurePolicy == nil || *quotaWebhook.FailurePolicy != admissionregistrationv1.Fail {
				t.Fatalf("failurePolicy = %v, want Fail", quotaWebhook.FailurePolicy)
			}
			if quotaWebhook.ClientConfig.Service == nil || quotaWebhook.ClientConfig.Service.Path == nil ||
				*quotaWebhook.ClientConfig.Service.Path != "/validate-coordination-k8s-io-v1-acp-suspend-quota-lease" {
				t.Fatalf("client service = %#v, want suspension quota Lease handler", quotaWebhook.ClientConfig.Service)
			}
			if len(quotaWebhook.Rules) != 1 {
				t.Fatalf("rules = %#v, want one Lease rule", quotaWebhook.Rules)
			}
			rule := quotaWebhook.Rules[0]
			wantOperations := []admissionregistrationv1.OperationType{
				admissionregistrationv1.Create, admissionregistrationv1.Update, admissionregistrationv1.Delete,
			}
			if !slices.Equal(rule.Operations, wantOperations) {
				t.Errorf("operations = %#v, want %#v", rule.Operations, wantOperations)
			}
			if !slices.Equal(rule.APIGroups, []string{"coordination.k8s.io"}) ||
				!slices.Equal(rule.APIVersions, []string{"v1"}) ||
				!slices.Equal(rule.Resources, []string{"leases"}) {
				t.Errorf("rule = %#v, want coordination.k8s.io/v1 Leases", rule.Rule)
			}
			wantNamespace := staticChartTestNamespace
			if test.name == sharedAdmissionVariant {
				wantNamespace = "orka-system"
			}
			selector := quotaWebhook.NamespaceSelector
			if selector == nil || selector.MatchLabels["kubernetes.io/metadata.name"] != wantNamespace {
				t.Fatalf("namespaceSelector = %#v, want namespace %q", selector, wantNamespace)
			}
			expectedExpression := "request.?name.orValue('').startsWith('acp-suspend-quota-') || " +
				"request.?name.orValue('').startsWith('acp-retention-fence-') || " +
				"(request.operation == 'CREATE' && " +
				"(object.metadata.?generateName.orValue('').startsWith('acp-suspend-quota-') || " +
				"object.metadata.?generateName.orValue('').startsWith('acp-retention-fence-')))"
			if len(quotaWebhook.MatchConditions) != 1 ||
				quotaWebhook.MatchConditions[0].Name != "reserved-acp-workspace-lease-name" ||
				quotaWebhook.MatchConditions[0].Expression != expectedExpression {
				t.Fatalf("matchConditions = %#v, want reserved ACP workspace Lease prefixes", quotaWebhook.MatchConditions)
			}
		})
	}
}

func TestTaskProvenanceWebhooksRouteStatusMetadataWrites(t *testing.T) {
	sharedPath := filepath.Join("..", "..", "..", "config", "orka-admission-webhooks", "validating_webhook.yaml")
	sharedManifest, err := os.ReadFile(sharedPath)
	if err != nil {
		t.Fatalf("read standalone admission webhooks: %v", err)
	}
	chartManifest := []byte(requireHelmRender(t,
		"--set-string", "controller.mode=harness-v2",
		"--show-only", "templates/controller-validating-webhook.yaml",
	))

	for _, test := range []struct {
		name        string
		manifest    []byte
		webhookName string
	}{
		{name: sharedAdmissionVariant, manifest: sharedManifest, webhookName: "taskprovenance.core.orka.ai"},
		{name: releaseLocalAdmissionVariant, manifest: chartManifest, webhookName: "task-provenance.harness-v2.orka.ai"},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
			if err := yaml.Unmarshal(test.manifest, &configuration); err != nil {
				t.Fatalf("decode validating webhook configuration: %v", err)
			}
			var provenanceWebhook *admissionregistrationv1.ValidatingWebhook
			for i := range configuration.Webhooks {
				if configuration.Webhooks[i].Name == test.webhookName {
					provenanceWebhook = &configuration.Webhooks[i]
					break
				}
			}
			if provenanceWebhook == nil {
				t.Fatalf("%s is missing", test.webhookName)
			}
			if len(provenanceWebhook.Rules) != 1 {
				t.Fatalf("rules = %#v, want one Task rule", provenanceWebhook.Rules)
			}
			if !slices.Equal(provenanceWebhook.Rules[0].Resources, []string{"tasks", "tasks/status"}) {
				t.Fatalf("resources = %#v, want Task writes and status metadata writes", provenanceWebhook.Rules[0].Resources)
			}
		})
	}
}

func TestWorkspaceCoreAdmissionPolicyRoutesStatusMetadataWrites(t *testing.T) {
	sharedPath := filepath.Join("..", "..", "..", "config", "policy", "workspace_core_admission_policy.yaml")
	sharedManifest, err := os.ReadFile(sharedPath)
	if err != nil {
		t.Fatalf("read standalone workspace core admission policy: %v", err)
	}
	chartManifest := []byte(requireHelmRender(t, "--show-only", "templates/workspace-core-admission-policy.yaml"))

	for _, test := range []struct {
		name     string
		manifest []byte
	}{
		{name: sharedAdmissionVariant, manifest: sharedManifest},
		{name: releaseLocalAdmissionVariant, manifest: chartManifest},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := admissionregistrationv1.ValidatingAdmissionPolicy{}
			if err := yaml.Unmarshal(test.manifest, &policy); err != nil {
				t.Fatalf("decode workspace core admission policy: %v", err)
			}
			if policy.Spec.MatchConstraints == nil || len(policy.Spec.MatchConstraints.ResourceRules) != 1 {
				t.Fatalf("match constraints = %#v, want one ExecutionWorkspace rule", policy.Spec.MatchConstraints)
			}
			resources := policy.Spec.MatchConstraints.ResourceRules[0].Resources
			if !slices.Equal(resources, []string{"executionworkspaces", "executionworkspaces/status"}) {
				t.Fatalf("resources = %#v, want workspace writes and status metadata writes", resources)
			}
			foundAttachmentVariable := false
			for _, variable := range policy.Spec.Variables {
				if variable.Name == "attachmentIntentUnchanged" &&
					strings.Contains(variable.Expression, "object.spec.attachment") &&
					strings.Contains(variable.Expression, "object.spec.attachmentEpoch") {
					foundAttachmentVariable = true
					break
				}
			}
			if !foundAttachmentVariable {
				t.Fatal("policy does not compare both controller-owned attachment intent fields")
			}
			foundMarkerFence := false
			foundAttachmentFence := false
			for _, validation := range policy.Spec.Validations {
				if strings.Contains(validation.Expression, "variables.acpMarkersUnchanged") {
					foundMarkerFence = true
				}
				if strings.Contains(validation.Expression, "variables.attachmentIntentUnchanged") {
					foundAttachmentFence = true
				}
			}
			if !foundMarkerFence {
				t.Fatal("status-routed policy does not enforce unchanged ACP materialization markers")
			}
			if !foundAttachmentFence {
				t.Fatal("policy does not reserve attachment intent for the Orka core controller")
			}
		})
	}
}

func TestControllerWebhooksDoNotOverlapSameModeReleasesInDifferentNamespaces(t *testing.T) {
	const mode = "harness-v2"
	selectorsByNamespace := make(map[string]map[string]map[string]string)
	for _, namespace := range []string{"orka-one", "orka-two"} {
		output := requireHelmRender(t,
			"--namespace", namespace,
			"--set-string", "controller.mode="+mode,
			"--set-string", "controller.watchNamespace="+namespace,
			"--show-only", "templates/controller-validating-webhook.yaml",
		)

		configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
		if err := yaml.Unmarshal([]byte(output), &configuration); err != nil {
			t.Fatalf("decode controller validating webhook configuration in namespace %q: %v", namespace, err)
		}
		selectorsByNamespace[namespace] = make(map[string]map[string]string, len(configuration.Webhooks))
		for _, webhook := range configuration.Webhooks {
			selector := webhook.NamespaceSelector
			if strings.HasPrefix(webhook.Name, "namespace-mode.") {
				selector = webhook.ObjectSelector
			}
			if selector == nil {
				t.Fatalf("%s selector in namespace %q is nil", webhook.Name, namespace)
			}
			selectorsByNamespace[namespace][webhook.Name] = selector.MatchLabels
			if selector.MatchLabels["orka.ai/controller-mode"] != mode ||
				selector.MatchLabels["kubernetes.io/metadata.name"] != namespace {
				t.Fatalf("%s selector in namespace %q = %#v", webhook.Name, namespace, selector.MatchLabels)
			}
		}
	}

	for webhookName, first := range selectorsByNamespace["orka-one"] {
		second := selectorsByNamespace["orka-two"][webhookName]
		if reflect.DeepEqual(first, second) {
			t.Errorf("%s has overlapping selectors across namespaces: %#v", webhookName, selectorsByNamespace)
		}
	}
}

func TestControllerWebhookServiceIsIsolatedFromExternalService(t *testing.T) {
	rendered := requireHelmRender(t,
		"--set", "service.type=LoadBalancer",
		"--show-only", "templates/service.yaml",
	)
	controllerService := corev1.Service{}
	if err := yaml.Unmarshal([]byte(rendered), &controllerService); err != nil {
		t.Fatalf("decode controller Service: %v", err)
	}
	if controllerService.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("controller Service type = %q, want LoadBalancer", controllerService.Spec.Type)
	}
	for _, port := range controllerService.Spec.Ports {
		if port.Name == webhookPortName || port.TargetPort.String() == webhookPortName || port.Port == 443 {
			t.Fatalf("external controller Service exposes webhook port: %#v", port)
		}
	}

	rendered = requireHelmRender(t,
		"--set", "service.type=LoadBalancer",
		"--show-only", "templates/controller-webhook-service.yaml",
	)
	webhookService := corev1.Service{}
	if err := yaml.Unmarshal([]byte(rendered), &webhookService); err != nil {
		t.Fatalf("decode controller webhook Service: %v", err)
	}
	if webhookService.Name != "test-orka-webhook" {
		t.Fatalf("controller webhook Service name = %q, want test-orka-webhook", webhookService.Name)
	}
	if webhookService.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("controller webhook Service type = %q, want ClusterIP", webhookService.Spec.Type)
	}
	if len(webhookService.Spec.Ports) != 1 {
		t.Fatalf("controller webhook Service ports = %#v, want one", webhookService.Spec.Ports)
	}
	port := webhookService.Spec.Ports[0]
	if port.Name != webhookPortName || port.Port != 443 || port.TargetPort.String() != webhookPortName {
		t.Fatalf("controller webhook Service port = %#v, want webhook 443 -> webhook", port)
	}

	rendered = requireHelmRender(t,
		"--set", "service.type=LoadBalancer",
		"--show-only", "templates/controller-validating-webhook.yaml",
	)
	configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := yaml.Unmarshal([]byte(rendered), &configuration); err != nil {
		t.Fatalf("decode controller validating webhook configuration: %v", err)
	}
	for _, webhook := range configuration.Webhooks {
		service := webhook.ClientConfig.Service
		if service == nil || service.Name != webhookService.Name || service.Namespace != staticChartTestNamespace {
			t.Errorf("%s service = %#v, want %s in orka-test", webhook.Name, service, webhookService.Name)
		}
	}
}

func TestControllerDeploymentEnablesReleaseLocalAdmission(t *testing.T) {
	rendered := requireHelmRender(t, "--show-only", "templates/deployment.yaml")
	deployment := appsv1.Deployment{}
	if err := yaml.Unmarshal([]byte(rendered), &deployment); err != nil {
		t.Fatalf("decode controller Deployment: %v", err)
	}

	var args []string
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "controller" {
			args = container.Args
			break
		}
	}
	for _, want := range []string{
		"--task-provenance-admission-enabled=true",
		"--workspace-class-use-admission-enabled=true",
		"--webhook-cert-path=/var/run/orka/webhook/tls",
	} {
		if !containsString(args, want) {
			t.Errorf("controller args do not contain %q: %#v", want, args)
		}
	}
}

func TestSharedAdmissionAuthorizesCanonicalProductionController(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config", "orka-admission", "deployment.yaml")
	manifest, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read standalone admission Deployment: %v", err)
	}
	deployment := appsv1.Deployment{}
	if err := yaml.Unmarshal(manifest, &deployment); err != nil {
		t.Fatalf("decode standalone admission Deployment: %v", err)
	}

	var args []string
	var lifecycle *corev1.Lifecycle
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "admission" {
			args = container.Args
			lifecycle = container.Lifecycle
			break
		}
	}
	if lifecycle == nil || lifecycle.PreStop == nil || lifecycle.PreStop.Exec == nil ||
		!slices.Equal(lifecycle.PreStop.Exec.Command, []string{"/orka-admission", "--pre-stop-delay=5s"}) {
		t.Fatalf("admission preStop lifecycle = %#v, want bounded endpoint-removal delay", lifecycle)
	}
	for _, prefix := range []string{"--controller-usernames=", "--task-provenance-trusted-users="} {
		if !commaListArgumentContains(args, prefix, canonicalProductionControllerUsername) {
			t.Errorf("admission args do not authorize %q in %s: %#v",
				canonicalProductionControllerUsername, prefix, args)
		}
	}
	for _, serviceAccount := range []string{"orka-ai-worker", "orka-vendor-worker"} {
		if !commaListArgumentContains(args, "--task-provenance-trusted-service-accounts=", serviceAccount) {
			t.Errorf("admission args do not authorize canonical worker %q: %#v", serviceAccount, args)
		}
	}
}

//nolint:gocyclo // This contract intentionally validates the complete multi-resource HA rollout shape.
func TestSharedAdmissionRolloutPreservesReadyEndpoints(t *testing.T) {
	deploymentPath := filepath.Join("..", "..", "..", "config", "orka-admission", "deployment.yaml")
	manifest, err := os.ReadFile(deploymentPath)
	if err != nil {
		t.Fatalf("read standalone admission Deployment: %v", err)
	}
	deployment := appsv1.Deployment{}
	if err := yaml.Unmarshal(manifest, &deployment); err != nil {
		t.Fatalf("decode standalone admission Deployment: %v", err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 2 {
		t.Fatalf("admission replicas = %v, want at least two", deployment.Spec.Replicas)
	}
	rollingUpdate := deployment.Spec.Strategy.RollingUpdate
	if deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || rollingUpdate == nil ||
		rollingUpdate.MaxUnavailable == nil || rollingUpdate.MaxUnavailable.IntValue() != 0 ||
		rollingUpdate.MaxSurge == nil || rollingUpdate.MaxSurge.IntValue() < 1 {
		t.Fatalf(
			"admission rollout strategy = %#v, want zero-unavailable rolling update with surge",
			deployment.Spec.Strategy,
		)
	}
	if deployment.Spec.Template.Spec.TerminationGracePeriodSeconds == nil ||
		*deployment.Spec.Template.Spec.TerminationGracePeriodSeconds <= 5 {
		t.Fatalf("admission termination grace = %v, want longer than endpoint-removal delay",
			deployment.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	var admissionContainer *corev1.Container
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		if container.Name == "admission" {
			admissionContainer = container
			break
		}
	}
	if admissionContainer == nil {
		t.Fatal("standalone admission container is missing")
	}
	if admissionContainer.ReadinessProbe == nil || admissionContainer.ReadinessProbe.HTTPGet == nil ||
		admissionContainer.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Fatalf("admission readiness probe = %#v, want /readyz", admissionContainer.ReadinessProbe)
	}
	if admissionContainer.Lifecycle == nil || admissionContainer.Lifecycle.PreStop == nil ||
		admissionContainer.Lifecycle.PreStop.Exec == nil ||
		!slices.Equal(admissionContainer.Lifecycle.PreStop.Exec.Command, []string{"/orka-admission", "--pre-stop-delay=5s"}) {
		t.Fatalf("admission preStop lifecycle = %#v, want bounded endpoint-removal delay", admissionContainer.Lifecycle)
	}

	servicePath := filepath.Join("..", "..", "..", "config", "orka-admission", "service.yaml")
	manifest, err = os.ReadFile(servicePath)
	if err != nil {
		t.Fatalf("read standalone admission Service: %v", err)
	}
	service := corev1.Service{}
	if err := yaml.Unmarshal(manifest, &service); err != nil {
		t.Fatalf("decode standalone admission Service: %v", err)
	}
	if service.Spec.PublishNotReadyAddresses {
		t.Fatal("admission Service publishes unready addresses; terminating Pods could remain routable")
	}
	if !reflect.DeepEqual(service.Spec.Selector, deployment.Spec.Selector.MatchLabels) {
		t.Fatalf("admission Service selector = %#v, want Deployment selector %#v",
			service.Spec.Selector, deployment.Spec.Selector.MatchLabels)
	}

	pdbPath := filepath.Join("..", "..", "..", "config", "orka-admission", "poddisruptionbudget.yaml")
	manifest, err = os.ReadFile(pdbPath)
	if err != nil {
		t.Fatalf("read standalone admission PodDisruptionBudget: %v", err)
	}
	pdb := policyv1.PodDisruptionBudget{}
	if err := yaml.Unmarshal(manifest, &pdb); err != nil {
		t.Fatalf("decode standalone admission PodDisruptionBudget: %v", err)
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() < 1 || pdb.Spec.Selector == nil ||
		!reflect.DeepEqual(pdb.Spec.Selector.MatchLabels, deployment.Spec.Selector.MatchLabels) {
		t.Fatalf("admission disruption budget = %#v, want at least one matching Pod available", pdb.Spec)
	}
}

func TestSharedTaskWebhooksBypassExactControllerCleanup(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config", "orka-admission-webhooks", "validating_webhook.yaml")
	manifest, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read standalone admission webhooks: %v", err)
	}
	assertTaskWebhooksBypassExactControllerCleanup(t, manifest)
}

func assertTaskWebhooksBypassExactControllerCleanup(t *testing.T, manifest []byte) {
	t.Helper()

	configuration := admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := yaml.Unmarshal(manifest, &configuration); err != nil {
		t.Fatalf("decode validating webhook configuration: %v", err)
	}
	webhooks := make(map[string]admissionregistrationv1.ValidatingWebhook, len(configuration.Webhooks))
	for _, webhook := range configuration.Webhooks {
		webhooks[webhook.Name] = webhook
	}

	authority, ok := webhooks["taskexecutionauthority.core.orka.ai"]
	if !ok {
		t.Fatal("task execution authority webhook is missing")
	}
	if len(authority.MatchConditions) != 1 ||
		authority.MatchConditions[0].Name != "route-unless-controller-cleanup-safe" {
		t.Fatalf("task execution authority cleanup-safe condition = %#v", authority.MatchConditions)
	}
	condition := authority.MatchConditions[0]
	for _, marker := range []string{
		"request.userInfo.username == '" + canonicalProductionControllerUsername + "'",
		"request.operation == 'UPDATE'",
		"has(oldObject.metadata.deletionTimestamp)",
		"oldObject.metadata.?finalizers.orValue([]).exists(f, f == 'orka.ai/cleanup')",
		"oldObject.metadata.?finalizers.orValue([]).filter(f, f != 'orka.ai/cleanup')",
		"object.spec == oldObject.spec",
		"object.?status.orValue({}) == oldObject.?status.orValue({})",
	} {
		if !strings.Contains(condition.Expression, marker) {
			t.Fatalf("cleanup-safe condition is missing %q:\n%s", marker, condition.Expression)
		}
	}

	for _, name := range []string{
		"taskprovenance.core.orka.ai",
		"taskworkspaceclassuse.core.orka.ai",
	} {
		webhook, ok := webhooks[name]
		if !ok {
			t.Fatalf("%s webhook is missing", name)
		}
		if webhook.FailurePolicy == nil || *webhook.FailurePolicy != admissionregistrationv1.Fail {
			t.Fatalf("%s failurePolicy = %v, want Fail", name, webhook.FailurePolicy)
		}
		if !reflect.DeepEqual(webhook.MatchConditions, authority.MatchConditions) {
			t.Fatalf("%s cleanup-safe conditions = %#v, want %#v", name, webhook.MatchConditions, authority.MatchConditions)
		}
	}
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}

func commaListArgumentContains(args []string, prefix, want string) bool {
	for _, arg := range args {
		value, ok := strings.CutPrefix(arg, prefix)
		if !ok {
			continue
		}
		if slices.Contains(strings.Split(value, ","), want) {
			return true
		}
	}
	return false
}
