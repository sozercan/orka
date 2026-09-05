//go:build e2e
// +build e2e

/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	"github.com/orka-agents/orka/internal/gateway/protocol"
	"github.com/orka-agents/orka/internal/harness/v2/conformance/conformancetest"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/test/utils"
)

const (
	gatewayE2EClassName                = "gateway-e2e"
	gatewayE2EName                     = "gateway-e2e"
	gatewayE2EBindingName              = "gateway-e2e-binding"
	gatewayE2EAdapterName              = "gateway-e2e-adapter"
	gatewayE2EAdapterTLSResourceName   = "gateway-e2e-adapter-tls"
	gatewayE2ECAConfigMapName          = "gateway-e2e-ca"
	gatewayE2EInboundAuthResourceName  = "gateway-e2e-inbound-auth"
	gatewayE2EOutboundAuthResourceName = "gateway-e2e-outbound-auth"
	gatewayE2ERuntimeName              = "gateway-e2e-runtime"
	gatewayE2ERuntimeAuthResourceName  = "gateway-e2e-runtime-auth"
	gatewayE2ERuntimeDeploymentName    = "gateway-e2e-runtime"
	gatewayE2ERuntimeServiceName       = "gateway-e2e-runtime"
	gatewayE2EAgentName                = "gateway-e2e-agent"
	gatewayE2EManagerCAMountName       = "gateway-e2e-ca"
	gatewayE2EManagerCAMountPath       = "/var/run/orka/gateway-e2e-ca"
	gatewayE2EAPIPort                  = 18120
	gatewayE2EAdapterPort              = 8443
	gatewayE2ERuntimePort              = 8080
)

var _ = Describe("Gateway live E2E", Ordered, func() {
	var (
		testActive            bool
		managerDeploymentName string
		managerCAConfigured   bool
		apiBaseURL            string
		apiToken              string
		cancelAPIPortForward  context.CancelFunc
		apiPortForwardCmd     *exec.Cmd
		eventID               string
		taskName              string
	)

	BeforeAll(func() {
		if !gatewayE2EEnabled() {
			Skip(fmt.Sprintf("Skipping Gateway live E2E: %s is not enabled", gatewayE2EEnvVar))
		}
		testActive = true
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpGatewayE2EDiagnostics(eventID, taskName)
		}
	})

	AfterAll(func() {
		if !testActive {
			return
		}

		if taskName != "" && apiBaseURL != "" && apiToken != "" {
			By("deleting the Gateway-owned Task through the controller API")
			if err := gatewayE2EDeleteTaskViaAPI(apiBaseURL, apiToken, taskName); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "failed to delete Gateway E2E Task through the controller API: %v\n", err)
			} else if err := gatewayE2EWaitForTaskDeletion(taskName, 2*time.Minute); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Gateway E2E Task deletion did not complete: %v\n", err)
			}
		}
		stopPortForward(cancelAPIPortForward, apiPortForwardCmd)

		for _, resource := range []struct {
			kind string
			name string
		}{
			{"gatewaybinding", gatewayE2EBindingName},
			{"gateway", gatewayE2EName},
			{"agent", gatewayE2EAgentName},
			{"agentruntime", gatewayE2ERuntimeName},
			{"service", gatewayE2ERuntimeServiceName},
			{"deployment", gatewayE2ERuntimeDeploymentName},
			{"service", gatewayE2EAdapterName},
			{"deployment", gatewayE2EAdapterName},
			{"secret", gatewayE2ERuntimeAuthResourceName},
			{"secret", gatewayE2EInboundAuthResourceName},
			{"secret", gatewayE2EOutboundAuthResourceName},
			{"secret", gatewayE2EAdapterTLSResourceName},
		} {
			gatewayE2EDelete(resource.kind, resource.name)
		}
		gatewayE2EDelete("gatewayclass", gatewayE2EClassName)

		if managerCAConfigured && managerDeploymentName != "" {
			By("removing Gateway E2E CA trust from the manager")
			if err := gatewayE2ERemoveManagerCATrust(managerDeploymentName); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "failed to remove Gateway E2E manager CA trust: %v\n", err)
			} else if err := gatewayE2EWaitForDeployment(managerDeploymentName, 5*time.Minute); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "manager rollout after Gateway E2E CA cleanup failed: %v\n", err)
			} else {
				managerCAConfigured = false
			}
		}
		if !managerCAConfigured {
			gatewayE2EDelete("configmap", gatewayE2ECAConfigMapName)
		}
	})

	It("runs authenticated ingress through an external v2 runtime and delivers the result", func() {
		adapterDNSName := fmt.Sprintf("%s.%s.svc", gatewayE2EAdapterName, namespace)
		adapterEndpoint := fmt.Sprintf("https://%s:%d", adapterDNSName, gatewayE2EAdapterPort)
		By("generating ephemeral Gateway authentication and TLS material")
		inboundBearer, err := gatewayE2ERandomBearer()
		Expect(err).NotTo(HaveOccurred())
		outboundBearer, err := gatewayE2ERandomBearer()
		Expect(err).NotTo(HaveOccurred())
		caPEM, serverCertPEM, serverKeyPEM, err := gatewayE2EGenerateTLS(adapterDNSName)
		Expect(err).NotTo(HaveOccurred())

		By("creating the Gateway TLS, CA, and bearer resources")
		Expect(applyManifestJSON(gatewayE2ETLSSecret(serverCertPEM, serverKeyPEM))).To(Succeed())
		Expect(applyManifestJSON(gatewayE2ECAConfigMap(caPEM))).To(Succeed())
		Expect(applyManifestJSON(gatewayE2EAuthSecret(
			gatewayE2EInboundAuthResourceName,
			gatewayruntime.GatewayInboundAuthLabel,
			inboundBearer,
			"",
		))).To(Succeed())
		Expect(applyManifestJSON(gatewayE2EAuthSecret(
			gatewayE2EOutboundAuthResourceName,
			gatewayruntime.GatewayOutboundAuthLabel,
			outboundBearer,
			adapterEndpoint,
		))).To(Succeed())

		By("patching the manager to trust the ephemeral Gateway CA")
		managerDeploymentName, err = controllerManagerDeploymentName()
		Expect(err).NotTo(HaveOccurred())
		Expect(gatewayE2EConfigureManagerCATrust(managerDeploymentName)).To(Succeed())
		managerCAConfigured = true
		Expect(gatewayE2EWaitForDeployment(managerDeploymentName, 5*time.Minute)).To(Succeed())

		By("deploying the TLS reference adapter")
		Expect(applyManifestJSON(gatewayE2EAdapterManifest())).To(Succeed())
		Expect(gatewayE2EWaitForDeployment(gatewayE2EAdapterName, 2*time.Minute)).To(Succeed())

		By("deploying and registering a conformant external v2 runtime")
		Expect(deployHarnessV2Fixture(
			gatewayE2ERuntimeName,
			gatewayE2ERuntimeDeploymentName,
			gatewayE2ERuntimeServiceName,
			gatewayE2ERuntimeAuthResourceName,
		)).To(Succeed())

		By("creating an Agent that references the external v2 runtime")
		Expect(applyManifestJSON(gatewayE2EAgentManifest())).To(Succeed())

		By("creating the GatewayClass, Gateway, and GatewayBinding")
		Expect(applyManifestJSON(gatewayE2EClassManifest())).To(Succeed())
		Expect(applyManifestJSON(gatewayE2EGatewayManifest())).To(Succeed())
		Expect(applyManifestJSON(gatewayE2EBindingManifest())).To(Succeed())
		waitForGatewayE2EReadiness(adapterEndpoint)

		By("port-forwarding the controller API")
		apiBaseURL, cancelAPIPortForward, apiPortForwardCmd, err = startControllerAPIPortForward(gatewayE2EAPIPort)
		Expect(err).NotTo(HaveOccurred())
		apiToken, err = serviceAccountToken()
		Expect(err).NotTo(HaveOccurred())
		Expect(apiToken).NotTo(BeEmpty())

		envelope := protocol.EventEnvelope{
			ProtocolVersion: protocol.Version,
			ExternalEventID: "gateway-live-e2e-event",
			EventType:       protocol.EventTypeText,
			AccountID:       "acct-ci",
			ContextID:       "room-ci",
			ThreadID:        "thread-ci",
			Sender: protocol.Sender{
				ID:          "sender-ci",
				DisplayName: "CI Sender",
			},
			Text:        "Return the deterministic gateway result.",
			ReplyTarget: "reply-ci",
			Metadata:    map[string]string{"testCase": "happy-path"},
		}
		requestBody, err := json.Marshal(envelope)
		Expect(err).NotTo(HaveOccurred())
		ingressURL := fmt.Sprintf(
			"%s/api/v1/gateways/%s/%s/events",
			strings.TrimRight(apiBaseURL, "/"),
			namespace,
			gatewayE2EName,
		)

		By("rejecting an invalid inbound bearer token")
		body, statusCode, err := doAuthorizedJSONRequest(
			http.MethodPost,
			ingressURL,
			"wrong-gateway-e2e-token",
			string(requestBody),
			"",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(statusCode).To(Equal(http.StatusUnauthorized), "unexpected invalid-token response: %s", strings.TrimSpace(body))

		By("accepting a valid normalized event")
		body, statusCode, err = doAuthorizedJSONRequest(
			http.MethodPost,
			ingressURL,
			inboundBearer,
			string(requestBody),
			"",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(statusCode).To(Equal(http.StatusAccepted), "unexpected ingress response: %s", strings.TrimSpace(body))
		var accepted protocol.IngressResponse
		Expect(json.Unmarshal([]byte(body), &accepted)).To(Succeed())
		Expect(accepted.Status).To(Equal("accepted"))
		Expect(accepted.EventID).NotTo(BeEmpty())
		Expect(accepted.State).To(Equal(string(store.GatewayEventQueued)))
		eventID = accepted.EventID

		By("acknowledging an exact duplicate with the same durable event ID")
		body, statusCode, err = doAuthorizedJSONRequest(
			http.MethodPost,
			ingressURL,
			inboundBearer,
			string(requestBody),
			"",
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(statusCode).To(Equal(http.StatusAccepted), "unexpected duplicate response: %s", strings.TrimSpace(body))
		var duplicate protocol.IngressResponse
		Expect(json.Unmarshal([]byte(body), &duplicate)).To(Succeed())
		Expect(duplicate.Status).To(Equal("duplicate"))
		Expect(duplicate.EventID).To(Equal(eventID))

		By("waiting for the Gateway-created runtimeRef Task")
		tasks := waitForGatewayE2ETasks(eventID, 1, 3*time.Minute)
		task := tasks[0]
		taskName = task.Name
		Expect(task.Spec.Type).To(Equal(corev1alpha1.TaskTypeAgent))
		Expect(task.Spec.AgentRef).NotTo(BeNil())
		Expect(task.Spec.AgentRef.Name).To(Equal(gatewayE2EAgentName))
		Expect(task.Spec.Prompt).To(BeEmpty())
		Expect(task.Spec.SessionRef).NotTo(BeNil())
		Expect(task.Spec.SessionRef.PromptIncluded).To(BeTrue())
		Expect(task.Spec.SessionRef.ThroughMessageID).To(Equal(store.GatewayUserMessageID(eventID)))
		Expect(task.Spec.RequestedBy).NotTo(BeNil())
		Expect(task.Spec.RequestedBy.Subject).To(Equal(envelope.Sender.ID))
		Expect(task.Labels).To(HaveKeyWithValue(gatewayruntime.TaskGatewayEventLabel, eventID))
		Expect(task.Annotations).To(HaveKeyWithValue(gatewayruntime.TaskGatewayEventAnnotation, eventID))
		Expect(task.Annotations).To(HaveKeyWithValue(gatewayruntime.TaskGatewayNameAnnotation, gatewayE2EName))
		Expect(task.Annotations).To(HaveKeyWithValue(gatewayruntime.TaskGatewayBindingAnnotation, gatewayE2EBindingName))

		waitForTaskPhase(taskName, "Succeeded", 3*time.Minute)
		verifyNoJobForTask(taskName, 5*time.Second)
		completedTask, err := gatewayE2EGetTask(taskName)
		Expect(err).NotTo(HaveOccurred())
		Expect(completedTask.Status.Execution).NotTo(BeNil())
		Expect(completedTask.Status.Execution.AgentRuntimeName).To(Equal(gatewayE2ERuntimeName))
		Expect(completedTask.Status.Execution.RuntimePoolName).To(BeEmpty())
		Expect(completedTask.Status.Execution.RuntimeInstanceID).To(Equal(gatewayE2ERuntimeName))

		By("waiting for durable completion projection and outbound delivery")
		event := waitForGatewayE2ECompletedEvent(apiBaseURL, apiToken, eventID, 4*time.Minute)
		Expect(event.GatewayName).To(Equal(gatewayE2EName))
		Expect(event.BindingName).To(Equal(gatewayE2EBindingName))
		Expect(event.AgentName).To(Equal(gatewayE2EAgentName))
		Expect(event.TaskName).To(Equal(taskName))
		Expect(event.SessionName).NotTo(BeEmpty())
		Expect(task.Spec.SessionRef.Name).To(Equal(event.SessionName))

		delivery := waitForGatewayE2EDelivery(apiBaseURL, apiToken, eventID, 4*time.Minute)
		Expect(delivery.ID).NotTo(BeEmpty())
		Expect(delivery.EventID).To(Equal(eventID))
		Expect(delivery.TaskName).To(Equal(taskName))
		Expect(delivery.SessionName).To(Equal(event.SessionName))
		Expect(delivery.Kind).To(Equal(protocol.DeliveryKindFinal))
		Expect(delivery.Text).To(Equal(conformancetest.DeterministicPromptResult))
		Expect(delivery.IdempotencyID).To(Equal(delivery.ID))
		Expect(delivery.AttemptCount).To(Equal(1))
		Expect(delivery.ProviderMessageID).To(Equal("reference:" + delivery.ID))
		Expect(event.DeliveryID).To(Equal(delivery.ID))
		Expect(event.ProviderMessageID).To(Equal(delivery.ProviderMessageID))

		By("verifying delivery correlation on the Task")
		Eventually(func(g Gomega) {
			current, err := gatewayE2EGetTask(taskName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(current.Annotations).To(HaveKeyWithValue(gatewayruntime.TaskGatewayDelivery, delivery.ID))
			g.Expect(current.Annotations).To(HaveKeyWithValue(
				gatewayruntime.TaskGatewayProviderMessage,
				delivery.ProviderMessageID,
			))
		}, time.Minute, time.Second).Should(Succeed())

		By("verifying duplicate safety and binding activity projection")
		Expect(waitForGatewayE2ETasks(eventID, 1, time.Minute)).To(HaveLen(1))
		Expect(listGatewayE2EDeliveries(apiBaseURL, apiToken, eventID)).To(HaveLen(1))
		Eventually(func(g Gomega) {
			binding := &gatewayv1alpha1.GatewayBinding{}
			g.Expect(gatewayE2EGetKubernetesJSON("gatewaybinding", gatewayE2EBindingName, true, binding)).To(Succeed())
			g.Expect(binding.Status.LastInboundActivity).NotTo(BeNil())
			g.Expect(binding.Status.LastOutboundActivity).NotTo(BeNil())
		}, time.Minute, time.Second).Should(Succeed())
	})
})

func gatewayE2ERandomBearer() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func gatewayE2EGenerateTLS(dnsName string) (string, string, string, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	caSerial, err := gatewayE2ERandomSerial()
	if err != nil {
		return "", "", "", err
	}
	serverSerial, err := gatewayE2ERandomSerial()
	if err != nil {
		return "", "", "", err
	}

	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "orka-gateway-e2e-ca"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(2 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return "", "", "", err
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(2 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			dnsName,
			gatewayE2EAdapterName,
			fmt.Sprintf("%s.%s", gatewayE2EAdapterName, namespace),
		},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return "", "", "", err
	}
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return "", "", "", err
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER})
	if len(caPEM) == 0 || len(serverCertPEM) == 0 || len(serverKeyPEM) == 0 {
		return "", "", "", fmt.Errorf("failed to encode Gateway E2E TLS material")
	}
	return string(caPEM), string(serverCertPEM), string(serverKeyPEM), nil
}

func gatewayE2ERandomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func gatewayE2ETLSSecret(certPEM, keyPEM string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      gatewayE2EAdapterTLSResourceName,
			"namespace": namespace,
		},
		"type": "kubernetes.io/tls",
		"stringData": map[string]any{
			"tls.crt": certPEM,
			"tls.key": keyPEM,
		},
	}
}

func gatewayE2ECAConfigMap(caPEM string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      gatewayE2ECAConfigMapName,
			"namespace": namespace,
		},
		"data": map[string]any{"ca.crt": caPEM},
	}
}

func gatewayE2EAuthSecret(name, directionLabel, token, endpoint string) map[string]any {
	annotations := map[string]any{gatewayruntime.GatewayAuthNameAnnotation: gatewayE2EName}
	if endpoint != "" {
		annotations[gatewayruntime.GatewayAuthEndpointAnnotation] = endpoint
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels": map[string]any{
				directionLabel:                      gatewayruntime.GatewayAuthEnabledValue,
				gatewayruntime.GatewayAuthNameLabel: gatewayE2EName,
			},
			"annotations": annotations,
		},
		"type":       "Opaque",
		"stringData": map[string]any{"token": token},
	}
}

func gatewayE2EAdapterManifest() map[string]any {
	labels := map[string]any{"app.kubernetes.io/name": gatewayE2EAdapterName}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items": []any{
			map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"name":      gatewayE2EAdapterName,
					"namespace": namespace,
					"labels":    labels,
				},
				"spec": map[string]any{
					"replicas": 1,
					"selector": map[string]any{"matchLabels": labels},
					"template": map[string]any{
						"metadata": map[string]any{"labels": labels},
						"spec": map[string]any{
							"automountServiceAccountToken": false,
							"securityContext": map[string]any{
								"runAsNonRoot": true,
								"runAsUser":    65532,
								"runAsGroup":   65532,
								"seccompProfile": map[string]any{
									"type": "RuntimeDefault",
								},
							},
							"containers": []any{
								map[string]any{
									"name":            "adapter",
									"image":           gatewayReferenceAdapterImage,
									"imagePullPolicy": "IfNotPresent",
									"args": []any{
										fmt.Sprintf("--listen=:%d", gatewayE2EAdapterPort),
										"--tls-cert-file=/var/run/orka/gateway/tls/tls.crt",
										"--tls-key-file=/var/run/orka/gateway/tls/tls.key",
									},
									"env": []any{map[string]any{
										"name": "ORKA_GATEWAY_BEARER_TOKEN",
										"valueFrom": map[string]any{"secretKeyRef": map[string]any{
											"name": gatewayE2EOutboundAuthResourceName,
											"key":  "token",
										}},
									}},
									"ports": []any{map[string]any{
										"name":          "https",
										"containerPort": gatewayE2EAdapterPort,
									}},
									"readinessProbe": map[string]any{
										"tcpSocket":     map[string]any{"port": "https"},
										"periodSeconds": 2,
									},
									"securityContext": map[string]any{
										"allowPrivilegeEscalation": false,
										"readOnlyRootFilesystem":   true,
										"capabilities": map[string]any{
											"drop": []any{"ALL"},
										},
									},
									"volumeMounts": []any{map[string]any{
										"name":      "tls",
										"mountPath": "/var/run/orka/gateway/tls",
										"readOnly":  true,
									}},
								},
							},
							"volumes": []any{map[string]any{
								"name": "tls",
								"secret": map[string]any{
									"secretName": gatewayE2EAdapterTLSResourceName,
								},
							}},
						},
					},
				},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata": map[string]any{
					"name":      gatewayE2EAdapterName,
					"namespace": namespace,
				},
				"spec": map[string]any{
					"selector": labels,
					"ports": []any{map[string]any{
						"name":       "https",
						"port":       gatewayE2EAdapterPort,
						"targetPort": "https",
					}},
				},
			},
		},
	}
}

func gatewayE2EAgentManifest() map[string]any {
	return map[string]any{
		"apiVersion": "core.orka.ai/v1alpha1",
		"kind":       "Agent",
		"metadata": map[string]any{
			"name":      gatewayE2EAgentName,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"runtime": map[string]any{
				"runtimeRef": map[string]any{"name": gatewayE2ERuntimeName},
			},
		},
	}
}

func gatewayE2EClassManifest() map[string]any {
	return map[string]any{
		"apiVersion": "gateway.orka.ai/v1alpha1",
		"kind":       "GatewayClass",
		"metadata":   map[string]any{"name": gatewayE2EClassName},
		"spec": map[string]any{
			"contractVersion": protocol.Version,
			"category":        "chat",
			"capabilities": map[string]any{
				"inboundText":        true,
				"outboundText":       true,
				"senderIdentity":     true,
				"idempotentDelivery": true,
			},
			"allowedMetadataKeys": []any{"testCase"},
		},
	}
}

func gatewayE2EGatewayManifest() map[string]any {
	return map[string]any{
		"apiVersion": "gateway.orka.ai/v1alpha1",
		"kind":       "Gateway",
		"metadata": map[string]any{
			"name":      gatewayE2EName,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"gatewayClassName": gatewayE2EClassName,
			"adapter": map[string]any{"serviceRef": map[string]any{
				"name": gatewayE2EAdapterName,
				"port": gatewayE2EAdapterPort,
			}},
			"inboundAuthRef": map[string]any{
				"name": gatewayE2EInboundAuthResourceName,
				"key":  "token",
			},
			"outboundAuthRef": map[string]any{
				"name": gatewayE2EOutboundAuthResourceName,
				"key":  "token",
			},
		},
	}
}

func gatewayE2EBindingManifest() map[string]any {
	return map[string]any{
		"apiVersion": "gateway.orka.ai/v1alpha1",
		"kind":       "GatewayBinding",
		"metadata": map[string]any{
			"name":      gatewayE2EBindingName,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"gatewayRef": map[string]any{"name": gatewayE2EName},
			"agentRef":   map[string]any{"name": gatewayE2EAgentName},
			"match": map[string]any{
				"accountId": "acct-ci",
				"contextId": "room-ci",
			},
			"senderPolicy": map[string]any{
				"mode":             "allowlist",
				"allowedSenderIds": []any{"sender-ci"},
			},
			"session":            map[string]any{"mode": "context"},
			"activeTurnBehavior": "queue",
		},
	}
}

func gatewayE2EConfigureManagerCATrust(deploymentName string) error {
	patch := map[string]any{
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "manager",
				"env": []any{map[string]any{
					"name":  "SSL_CERT_DIR",
					"value": gatewayE2EManagerCAMountPath,
				}},
				"volumeMounts": []any{map[string]any{
					"name":      gatewayE2EManagerCAMountName,
					"mountPath": gatewayE2EManagerCAMountPath,
					"readOnly":  true,
				}},
			}},
			"volumes": []any{map[string]any{
				"name": gatewayE2EManagerCAMountName,
				"configMap": map[string]any{
					"name": gatewayE2ECAConfigMapName,
				},
			}},
		}}},
	}
	return gatewayE2EPatchDeployment(deploymentName, patch)
}

func gatewayE2ERemoveManagerCATrust(deploymentName string) error {
	patch := map[string]any{
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "manager",
				"env": []any{map[string]any{
					"name":   "SSL_CERT_DIR",
					"$patch": "delete",
				}},
				"volumeMounts": []any{map[string]any{
					"name":      gatewayE2EManagerCAMountName,
					"mountPath": gatewayE2EManagerCAMountPath,
					"$patch":    "delete",
				}},
			}},
			"volumes": []any{map[string]any{
				"name":   gatewayE2EManagerCAMountName,
				"$patch": "delete",
			}},
		}}},
	}
	return gatewayE2EPatchDeployment(deploymentName, patch)
}

func gatewayE2EPatchDeployment(deploymentName string, patch map[string]any) error {
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	cmd := exec.Command(
		"kubectl", "patch", "deployment", deploymentName,
		"-n", namespace,
		"--type=strategic",
		"-p", string(payload),
	)
	_, err = utils.Run(cmd)
	return err
}

func gatewayE2EWaitForDeployment(name string, timeout time.Duration) error {
	cmd := exec.Command(
		"kubectl", "rollout", "status", "deployment/"+name,
		"-n", namespace,
		"--timeout="+timeout.String(),
	)
	_, err := utils.Run(cmd)
	return err
}

func waitForGatewayE2EReadiness(expectedEndpoint string) {
	Eventually(func(g Gomega) {
		class := &gatewayv1alpha1.GatewayClass{}
		g.Expect(gatewayE2EGetKubernetesJSON("gatewayclass", gatewayE2EClassName, false, class)).To(Succeed())
		g.Expect(class.Status.Accepted).To(BeTrue(), class.Status.Message)
		g.Expect(class.Status.ObservedGeneration).To(Equal(class.Generation))
	}, 2*time.Minute, time.Second).Should(Succeed())

	Eventually(func(g Gomega) {
		gateway := &gatewayv1alpha1.Gateway{}
		g.Expect(gatewayE2EGetKubernetesJSON("gateway", gatewayE2EName, true, gateway)).To(Succeed())
		g.Expect(gateway.Status.Accepted).To(BeTrue(), gateway.Status.Message)
		g.Expect(gateway.Status.ResolvedRefs).To(BeTrue(), gateway.Status.Message)
		g.Expect(gateway.Status.Connected).To(BeTrue(), gateway.Status.Message)
		g.Expect(gateway.Status.Ready).To(BeTrue(), gateway.Status.Message)
		g.Expect(gateway.Status.ObservedGeneration).To(Equal(gateway.Generation))
		g.Expect(gateway.Status.ResolvedEndpoint).To(Equal(expectedEndpoint))
		g.Expect(gateway.Status.ObservedCapabilities).NotTo(BeNil())
		g.Expect(gateway.Status.ObservedCapabilities.AdapterName).To(Equal("orka-reference-adapter"))
		g.Expect(gateway.Status.ObservedCapabilities.ContractVersion).To(Equal(protocol.Version))
	}, 3*time.Minute, 2*time.Second).Should(Succeed())

	Eventually(func(g Gomega) {
		binding := &gatewayv1alpha1.GatewayBinding{}
		g.Expect(gatewayE2EGetKubernetesJSON("gatewaybinding", gatewayE2EBindingName, true, binding)).To(Succeed())
		g.Expect(binding.Status.Accepted).To(BeTrue(), binding.Status.Message)
		g.Expect(binding.Status.ResolvedRefs).To(BeTrue(), binding.Status.Message)
		g.Expect(binding.Status.Programmed).To(BeTrue(), binding.Status.Message)
		g.Expect(binding.Status.Ready).To(BeTrue(), binding.Status.Message)
		g.Expect(binding.Status.ObservedGeneration).To(Equal(binding.Generation))
	}, 3*time.Minute, time.Second).Should(Succeed())
}

func gatewayE2EGetKubernetesJSON(resource, name string, namespaced bool, target any) error {
	args := []string{"get", resource, name}
	if namespaced {
		args = append(args, "-n", namespace)
	}
	args = append(args, "-o", "json")
	output, err := utils.Run(exec.Command("kubectl", args...))
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(output), target)
}

func waitForGatewayE2ETasks(eventID string, count int, timeout time.Duration) []corev1alpha1.Task {
	var tasks []corev1alpha1.Task
	Eventually(func(g Gomega) {
		current, err := gatewayE2ETasks(eventID)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(current).To(HaveLen(count))
		tasks = current
	}, timeout, time.Second).Should(Succeed())
	return tasks
}

func gatewayE2ETasks(eventID string) ([]corev1alpha1.Task, error) {
	cmd := exec.Command(
		"kubectl", "get", "tasks",
		"-n", namespace,
		"-l", gatewayruntime.TaskGatewayEventLabel+"="+eventID,
		"-o", "json",
	)
	output, err := utils.Run(cmd)
	if err != nil {
		return nil, err
	}
	var list corev1alpha1.TaskList
	if err := json.Unmarshal([]byte(output), &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func gatewayE2EGetTask(name string) (*corev1alpha1.Task, error) {
	task := &corev1alpha1.Task{}
	if err := gatewayE2EGetKubernetesJSON("task", name, true, task); err != nil {
		return nil, err
	}
	return task, nil
}

func gatewayE2EDeleteTaskViaAPI(apiBaseURL, bearer, taskName string) error {
	endpoint := fmt.Sprintf(
		"%s/api/v1/tasks/%s?namespace=%s",
		strings.TrimRight(apiBaseURL, "/"),
		url.PathEscape(taskName),
		url.QueryEscape(namespace),
	)
	body, statusCode, err := doAuthorizedJSONRequest(http.MethodDelete, endpoint, bearer, "", "")
	if err != nil {
		return err
	}
	if statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
		return fmt.Errorf("delete Gateway Task API returned %d: %s", statusCode, strings.TrimSpace(body))
	}
	return nil
}

func gatewayE2EWaitForTaskDeletion(taskName string, timeout time.Duration) error {
	cmd := exec.Command(
		"kubectl", "wait", "--for=delete", "task/"+taskName,
		"-n", namespace, "--timeout="+timeout.String(),
	)
	_, err := utils.Run(cmd)
	return err
}

func waitForGatewayE2ECompletedEvent(apiBaseURL, token, eventID string, timeout time.Duration) store.GatewayEvent {
	var event store.GatewayEvent
	Eventually(func(g Gomega) {
		current, err := getGatewayE2EEvent(apiBaseURL, token, eventID)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(current.State).To(Equal(store.GatewayEventCompleted), current.StateMessage)
		g.Expect(current.TaskName).NotTo(BeEmpty())
		g.Expect(current.SessionName).NotTo(BeEmpty())
		g.Expect(current.DeliveryID).NotTo(BeEmpty())
		g.Expect(current.ProviderMessageID).NotTo(BeEmpty())
		event = current
	}, timeout, 2*time.Second).Should(Succeed())
	return event
}

func getGatewayE2EEvent(apiBaseURL, token, eventID string) (store.GatewayEvent, error) {
	endpoint := fmt.Sprintf(
		"%s/api/v1/gateway-events/%s?namespace=%s",
		strings.TrimRight(apiBaseURL, "/"),
		url.PathEscape(eventID),
		url.QueryEscape(namespace),
	)
	body, statusCode, err := doAuthorizedJSONRequest(http.MethodGet, endpoint, token, "", "")
	if err != nil {
		return store.GatewayEvent{}, err
	}
	if statusCode != http.StatusOK {
		return store.GatewayEvent{}, fmt.Errorf("gateway event API returned %d: %s", statusCode, strings.TrimSpace(body))
	}
	var event store.GatewayEvent
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		return store.GatewayEvent{}, err
	}
	return event, nil
}

func waitForGatewayE2EDelivery(apiBaseURL, token, eventID string, timeout time.Duration) store.GatewayDelivery {
	var delivery store.GatewayDelivery
	Eventually(func(g Gomega) {
		deliveries, err := getGatewayE2EDeliveries(apiBaseURL, token, eventID)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(deliveries).To(HaveLen(1))
		g.Expect(deliveries[0].State).To(Equal(store.GatewayDeliveryDelivered), deliveries[0].LastError)
		delivery = deliveries[0]
	}, timeout, 2*time.Second).Should(Succeed())
	return delivery
}

func listGatewayE2EDeliveries(apiBaseURL, token, eventID string) []store.GatewayDelivery {
	deliveries, err := getGatewayE2EDeliveries(apiBaseURL, token, eventID)
	Expect(err).NotTo(HaveOccurred())
	return deliveries
}

func getGatewayE2EDeliveries(apiBaseURL, token, eventID string) ([]store.GatewayDelivery, error) {
	values := url.Values{}
	values.Set("namespace", namespace)
	values.Set("gateway", gatewayE2EName)
	values.Set("event", eventID)
	endpoint := fmt.Sprintf(
		"%s/api/v1/gateway-deliveries?%s",
		strings.TrimRight(apiBaseURL, "/"),
		values.Encode(),
	)
	body, statusCode, err := doAuthorizedJSONRequest(http.MethodGet, endpoint, token, "", "")
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway delivery API returned %d: %s", statusCode, strings.TrimSpace(body))
	}
	var response struct {
		Items []store.GatewayDelivery `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func gatewayE2EDelete(kind string, args ...string) {
	commandArgs := []string{"delete", kind}
	commandArgs = append(commandArgs, args...)
	commandArgs = append(commandArgs, "-n", namespace, "--ignore-not-found", "--timeout=2m")
	if kind == "gatewayclass" {
		commandArgs = []string{"delete", kind}
		commandArgs = append(commandArgs, args...)
		commandArgs = append(commandArgs, "--ignore-not-found", "--timeout=2m")
	}
	if _, err := utils.Run(exec.Command("kubectl", commandArgs...)); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup failed: kubectl %s: %v\n", strings.Join(commandArgs, " "), err)
	}
}

func dumpGatewayE2EDiagnostics(eventID, taskName string) {
	By("collecting Gateway live E2E diagnostics")
	commands := [][]string{
		{"get", "gatewayclass", gatewayE2EClassName, "-o", "yaml"},
		{"get", "gateway", gatewayE2EName, "-n", namespace, "-o", "yaml"},
		{"get", "gatewaybinding", gatewayE2EBindingName, "-n", namespace, "-o", "yaml"},
		{"get", "agentruntime", gatewayE2ERuntimeName, "-n", namespace, "-o", "yaml"},
		{"get", "pods", "-n", namespace, "-o", "wide"},
		{"get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
		{"logs", "deployment/" + gatewayE2EAdapterName, "-n", namespace, "--tail=200"},
		{"logs", "deployment/" + gatewayE2ERuntimeDeploymentName, "-n", namespace, "--tail=200"},
	}
	if eventID != "" {
		commands = append(commands, []string{
			"get", "tasks", "-n", namespace,
			"-l", gatewayruntime.TaskGatewayEventLabel + "=" + eventID,
			"-o", "yaml",
		})
	} else if taskName != "" {
		commands = append(commands, []string{"get", "task", taskName, "-n", namespace, "-o", "yaml"})
	}
	for _, args := range commands {
		output, err := utils.Run(exec.Command("kubectl", args...))
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "diagnostic failed: kubectl %s: %v\n", strings.Join(args, " "), err)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "diagnostic: kubectl %s\n%s\n", strings.Join(args, " "), output)
	}
	dumpControllerManagerDiagnostics()
}
