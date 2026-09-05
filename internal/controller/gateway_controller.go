/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/agentruntimepolicy"
	"github.com/orka-agents/orka/internal/events"
	gatewayruntime "github.com/orka-agents/orka/internal/gateway"
	gatewayconformance "github.com/orka-agents/orka/internal/gateway/conformance"
	"github.com/orka-agents/orka/internal/gateway/protocol"
)

const (
	gatewayRequeueInterval = 30 * time.Second
	gatewayProbeTimeout    = 15 * time.Second
)

var gatewayMetadataKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

// GatewayClassReconciler validates administrator-owned adapter profiles.
type GatewayClassReconciler struct {
	client.Client
	Scheme *k8sruntime.Scheme
}

// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewayclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewayclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewayclasses/finalizers,verbs=update

// Reconcile validates one GatewayClass.
func (r *GatewayClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	object := &gatewayv1alpha1.GatewayClass{}
	if err := r.Get(ctx, req.NamespacedName, object); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	message := "GatewayClass is accepted"
	accepted := true
	if err := validateGatewayClass(object); err != nil {
		accepted = false
		message = err.Error()
	}
	now := metav1.Now()
	object.Status.Accepted = accepted
	object.Status.ObservedGeneration = object.Generation
	object.Status.Message = sanitizeGatewayStatusMessage(message)
	setGatewayCondition(&object.Status.Conditions, "Accepted", accepted, "ValidationSucceeded", "ValidationFailed", object.Generation, object.Status.Message, now)
	if err := r.Status().Update(ctx, object); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the GatewayClass controller.
func (r *GatewayClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1alpha1.GatewayClass{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("gatewayclass").
		Complete(r)
}

// GatewayReconciler resolves adapter references and probes orka.gateway.v1.
type GatewayReconciler struct {
	client.Client
	Scheme                *k8sruntime.Scheme
	HTTPClient            *http.Client
	AllowInsecureLoopback bool
}

// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gateways,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gateways/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gateways/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewayclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch

// Reconcile validates references, Secret boundaries, health, and capabilities.
func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	object := &gatewayv1alpha1.Gateway{}
	if err := r.Get(ctx, req.NamespacedName, object); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	logger := log.FromContext(ctx)
	logger.Info("Reconciling Gateway", "gateway", object.Name, "class", object.Spec.GatewayClassName)

	accepted, resolved, connected, ready := true, false, false, false
	message := "Gateway is ready"
	var resolvedEndpoint, safeEndpoint string
	var inboundVersion, outboundVersion string
	var observed *gatewayv1alpha1.GatewayObservedCapabilities

	if err := validateGatewaySpec(object); err != nil {
		accepted = false
		message = err.Error()
	} else {
		class := &gatewayv1alpha1.GatewayClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: object.Spec.GatewayClassName}, class); err != nil {
			if apierrors.IsNotFound(err) {
				message = fmt.Sprintf("GatewayClass %q not found", object.Spec.GatewayClassName)
			} else {
				return ctrl.Result{}, err
			}
		} else if !class.Status.Accepted || class.Status.ObservedGeneration != class.Generation {
			message = fmt.Sprintf("GatewayClass %q is not accepted", class.Name)
		} else {
			resolver := gatewayruntime.EndpointResolver{Client: r.Client, AllowInsecureLoopback: r.AllowInsecureLoopback}
			var err error
			resolvedEndpoint, safeEndpoint, err = resolver.Resolve(ctx, object)
			if err != nil {
				message = err.Error()
			} else {
				inbound, inboundErr := gatewayruntime.ReadBearerSecret(ctx, r.Client, object, gatewayruntime.AuthDirectionInbound, resolvedEndpoint)
				outbound, outboundErr := gatewayruntime.ReadBearerSecret(ctx, r.Client, object, gatewayruntime.AuthDirectionOutbound, resolvedEndpoint)
				switch {
				case inboundErr != nil:
					message = inboundErr.Error()
				case outboundErr != nil:
					message = outboundErr.Error()
				default:
					resolved = true
					inboundVersion = inbound.ResourceVersion
					outboundVersion = outbound.ResourceVersion
					probeClient, clientErr := gatewayruntime.NewAdapterHTTPClient(
						r.HTTPClient, gatewayProbeTimeout, object.Spec.Adapter.ServiceRef != nil, r.AllowInsecureLoopback,
					)
					if clientErr != nil {
						message = "adapter HTTP client is unsafe"
					} else {
						probeCtx, cancel := context.WithTimeout(ctx, gatewayProbeTimeout)
						probe := gatewayconformance.Probe(probeCtx, gatewayconformance.Target{
							BaseURL: resolvedEndpoint, AuthorizationValue: outbound.Token, HTTPClient: probeClient,
							Timeout: gatewayProbeTimeout,
						})
						cancel()
						if !probe.Passed {
							message = probe.Message
						} else {
							connected = true
							observed = observedGatewayCapabilities(probe.Capabilities)
							if err := validateRequiredGatewayCapabilities(class.Spec.Capabilities, observed.Capabilities); err != nil {
								message = err.Error()
							} else {
								ready = true
							}
						}
					}
				}
			}
		}
	}

	now := metav1.Now()
	object.Status.Accepted = accepted
	object.Status.ResolvedRefs = resolved
	object.Status.Connected = connected
	object.Status.Ready = ready
	object.Status.ObservedGeneration = object.Generation
	object.Status.ResolvedEndpoint = safeEndpoint
	object.Status.ObservedCapabilities = observed
	object.Status.ObservedInboundAuthRefVersion = inboundVersion
	object.Status.ObservedOutboundAuthRefVersion = outboundVersion
	object.Status.Message = sanitizeGatewayStatusMessage(message)
	if ready {
		object.Status.LastSuccessfulProbe = &now
	}
	setGatewayCondition(&object.Status.Conditions, "Accepted", accepted, "ValidationSucceeded", "ValidationFailed", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "ResolvedRefs", resolved, "ReferencesResolved", "ReferencesNotResolved", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "Connected", connected, "ProbeSucceeded", "ProbeFailed", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "Ready", ready, "Ready", "NotReady", object.Generation, object.Status.Message, now)
	if err := r.Status().Update(ctx, object); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: gatewayRequeueInterval}, nil
}

// SetupWithManager registers Gateway watches, including Secret and Service rotation.
func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1alpha1.Gateway{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForSecret)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForService)).
		Watches(&gatewayv1alpha1.GatewayClass{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForClass)).
		Named("gateway").
		Complete(r)
}

func (r *GatewayReconciler) gatewaysForSecret(ctx context.Context, object client.Object) []reconcile.Request {
	secret, ok := object.(*corev1.Secret)
	if !ok {
		return nil
	}
	list := &gatewayv1alpha1.GatewayList{}
	if err := r.List(ctx, list, client.InNamespace(secret.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.InboundAuthRef.Name == secret.Name || item.Spec.OutboundAuthRef.Name == secret.Name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(item)})
		}
	}
	return requests
}

func (r *GatewayReconciler) gatewaysForService(ctx context.Context, object client.Object) []reconcile.Request {
	service, ok := object.(*corev1.Service)
	if !ok {
		return nil
	}
	list := &gatewayv1alpha1.GatewayList{}
	if err := r.List(ctx, list, client.InNamespace(service.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.Adapter.ServiceRef != nil && item.Spec.Adapter.ServiceRef.Name == service.Name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(item)})
		}
	}
	return requests
}

func (r *GatewayReconciler) gatewaysForClass(ctx context.Context, object client.Object) []reconcile.Request {
	class, ok := object.(*gatewayv1alpha1.GatewayClass)
	if !ok {
		return nil
	}
	list := &gatewayv1alpha1.GatewayList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.GatewayClassName == class.Name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(item)})
		}
	}
	return requests
}

// GatewayBindingReconciler validates semantic routing and target references.
type GatewayBindingReconciler struct {
	client.Client
	Scheme *k8sruntime.Scheme
}

// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewaybindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewaybindings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gatewaybindings/finalizers,verbs=update
// +kubebuilder:rbac:groups=gateway.orka.ai,resources=gateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.orka.ai,resources=agents,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.orka.ai,resources=agentruntimes,verbs=get;list;watch

// Reconcile validates one semantic GatewayBinding.
func (r *GatewayBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	object := &gatewayv1alpha1.GatewayBinding{}
	if err := r.Get(ctx, req.NamespacedName, object); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	accepted, resolved, programmed, ready := true, false, false, false
	message := "GatewayBinding is ready"
	var capabilities *gatewayv1alpha1.GatewayCapabilities
	if err := validateGatewayBindingSpec(object); err != nil {
		accepted = false
		message = err.Error()
	} else {
		gatewayObject := &gatewayv1alpha1.Gateway{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: object.Namespace, Name: object.Spec.GatewayRef.Name}, gatewayObject); err != nil {
			if apierrors.IsNotFound(err) {
				message = fmt.Sprintf("Gateway %q not found", object.Spec.GatewayRef.Name)
			} else {
				return ctrl.Result{}, err
			}
		} else if !gatewayObject.Status.Ready || gatewayObject.Status.ObservedGeneration != gatewayObject.Generation || gatewayObject.Status.ObservedCapabilities == nil {
			message = fmt.Sprintf("Gateway %q is not ready", gatewayObject.Name)
		} else {
			agent := &corev1alpha1.Agent{}
			if err := r.Get(ctx, client.ObjectKey{Namespace: object.Namespace, Name: object.Spec.AgentRef.Name}, agent); err != nil {
				if apierrors.IsNotFound(err) {
					message = fmt.Sprintf("Agent %q not found", object.Spec.AgentRef.Name)
				} else {
					return ctrl.Result{}, err
				}
			} else {
				resolved = true
				copy := gatewayObject.Status.ObservedCapabilities.Capabilities
				capabilities = &copy
				runtimeMessage, err := r.validateBindingAgentRuntimeDefaults(ctx, object, agent)
				if err != nil {
					return ctrl.Result{}, err
				}
				if runtimeMessage != "" {
					message = runtimeMessage
				} else if err := validateBindingCapabilities(object, copy); err != nil {
					message = err.Error()
				} else if conflict, err := r.findAmbiguousGatewayBinding(ctx, object, copy); err != nil {
					return ctrl.Result{}, err
				} else if conflict != "" {
					message = fmt.Sprintf("equal-priority match overlaps GatewayBinding %q", conflict)
				} else {
					programmed = true
					ready = true
				}
			}
		}
	}

	now := metav1.Now()
	object.Status.Accepted = accepted
	object.Status.ResolvedRefs = resolved
	object.Status.Programmed = programmed
	object.Status.Ready = ready
	object.Status.ObservedGeneration = object.Generation
	object.Status.ResolvedCapabilities = capabilities
	object.Status.Message = sanitizeGatewayStatusMessage(message)
	setGatewayCondition(&object.Status.Conditions, "Accepted", accepted, "ValidationSucceeded", "ValidationFailed", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "ResolvedRefs", resolved, "ReferencesResolved", "ReferencesNotResolved", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "Programmed", programmed, "Programmed", "AmbiguousOrUnsupported", object.Generation, object.Status.Message, now)
	setGatewayCondition(&object.Status.Conditions, "Ready", ready, "Ready", "NotReady", object.Generation, object.Status.Message, now)
	if err := r.Status().Update(ctx, object); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers binding watches.
func (r *GatewayBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1alpha1.GatewayBinding{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&gatewayv1alpha1.GatewayBinding{},
			handler.EnqueueRequestsFromMapFunc(r.bindingsForPeer),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Watches(&gatewayv1alpha1.Gateway{}, handler.EnqueueRequestsFromMapFunc(r.bindingsForGateway)).
		Watches(&corev1alpha1.Agent{}, handler.EnqueueRequestsFromMapFunc(r.bindingsForAgent)).
		Watches(&corev1alpha1.AgentRuntime{}, handler.EnqueueRequestsFromMapFunc(r.bindingsForAgentRuntime)).
		Named("gatewaybinding").
		Complete(r)
}

func (r *GatewayBindingReconciler) bindingsForPeer(ctx context.Context, object client.Object) []reconcile.Request {
	binding, ok := object.(*gatewayv1alpha1.GatewayBinding)
	if !ok {
		return nil
	}
	return r.bindingsForReference(ctx, binding.Namespace, func(*gatewayv1alpha1.GatewayBinding) bool { return true })
}

func (r *GatewayBindingReconciler) bindingsForGateway(ctx context.Context, object client.Object) []reconcile.Request {
	gatewayObject, ok := object.(*gatewayv1alpha1.Gateway)
	if !ok {
		return nil
	}
	return r.bindingsForReference(ctx, gatewayObject.Namespace, func(binding *gatewayv1alpha1.GatewayBinding) bool {
		return binding.Spec.GatewayRef.Name == gatewayObject.Name
	})
}

func (r *GatewayBindingReconciler) bindingsForAgent(ctx context.Context, object client.Object) []reconcile.Request {
	agent, ok := object.(*corev1alpha1.Agent)
	if !ok {
		return nil
	}
	return r.bindingsForAgentNames(ctx, agent.Namespace, map[string]struct{}{agent.Name: {}})
}

func (r *GatewayBindingReconciler) bindingsForAgentRuntime(ctx context.Context, object client.Object) []reconcile.Request {
	runtimeObject, ok := object.(*corev1alpha1.AgentRuntime)
	if !ok {
		return nil
	}
	agents := &corev1alpha1.AgentList{}
	if err := r.List(ctx, agents, client.InNamespace(runtimeObject.Namespace)); err != nil {
		return nil
	}
	names := make(map[string]struct{})
	for i := range agents.Items {
		agent := &agents.Items[i]
		if agent.Spec.Runtime != nil && agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) == runtimeObject.Name {
			names[agent.Name] = struct{}{}
		}
	}
	return r.bindingsForAgentNames(ctx, runtimeObject.Namespace, names)
}

func (r *GatewayBindingReconciler) bindingsForAgentNames(ctx context.Context, namespace string, agentNames map[string]struct{}) []reconcile.Request {
	if len(agentNames) == 0 {
		return nil
	}
	list := &gatewayv1alpha1.GatewayBindingList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil
	}
	references := make([]*gatewayv1alpha1.GatewayBinding, 0)
	for i := range list.Items {
		binding := &list.Items[i]
		if _, ok := agentNames[binding.Spec.AgentRef.Name]; ok {
			references = append(references, binding)
		}
	}
	requests := make([]reconcile.Request, 0)
	for i := range list.Items {
		binding := &list.Items[i]
		for _, reference := range references {
			if binding.Name == reference.Name ||
				(binding.Spec.Priority == reference.Spec.Priority && gatewayBindingsOverlap(binding, reference)) {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(binding)})
				break
			}
		}
	}
	return requests
}

func (r *GatewayBindingReconciler) bindingsForReference(ctx context.Context, namespace string, matches func(*gatewayv1alpha1.GatewayBinding) bool) []reconcile.Request {
	list := &gatewayv1alpha1.GatewayBindingList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range list.Items {
		item := &list.Items[i]
		if matches(item) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(item)})
		}
	}
	return requests
}

func (r *GatewayBindingReconciler) findAmbiguousGatewayBinding(
	ctx context.Context,
	current *gatewayv1alpha1.GatewayBinding,
	capabilities gatewayv1alpha1.GatewayCapabilities,
) (string, error) {
	list := &gatewayv1alpha1.GatewayBindingList{}
	if err := r.List(ctx, list, client.InNamespace(current.Namespace)); err != nil {
		return "", err
	}
	conflict := ""
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == current.Name || other.Spec.Priority != current.Spec.Priority {
			continue
		}
		if !gatewayBindingsOverlap(current, other) {
			continue
		}
		// Overlap requires the same GatewayRef, so the current Gateway's observed
		// capabilities are also the peer binding's capabilities.
		programmable, err := r.gatewayBindingCanBeProgrammed(ctx, other, capabilities)
		if err != nil {
			return "", err
		}
		if programmable && (conflict == "" || other.Name < conflict) {
			conflict = other.Name
		}
	}
	return conflict, nil
}

func (r *GatewayBindingReconciler) gatewayBindingCanBeProgrammed(
	ctx context.Context,
	binding *gatewayv1alpha1.GatewayBinding,
	capabilities gatewayv1alpha1.GatewayCapabilities,
) (bool, error) {
	if validateGatewayBindingSpec(binding) != nil || validateBindingCapabilities(binding, capabilities) != nil {
		return false, nil
	}
	agent := &corev1alpha1.Agent{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: binding.Namespace, Name: binding.Spec.AgentRef.Name}, agent); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	message, err := r.validateBindingAgentRuntimeDefaults(ctx, binding, agent)
	if err != nil {
		return false, err
	}
	if message != "" {
		return false, nil
	}
	return true, nil
}

func (r *GatewayBindingReconciler) validateBindingAgentRuntimeDefaults(
	ctx context.Context,
	binding *gatewayv1alpha1.GatewayBinding,
	agent *corev1alpha1.Agent,
) (string, error) {
	if binding == nil || agent == nil || agent.Spec.Runtime == nil || agent.Spec.Runtime.RuntimeRef == nil {
		return "", nil
	}
	runtimeName := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
	if runtimeName == "" {
		return fmt.Sprintf("Agent %q runtimeRef.name is required", agent.Name), nil
	}
	runtimeObject := &corev1alpha1.AgentRuntime{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: agent.Namespace, Name: runtimeName}, runtimeObject); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("AgentRuntime %q referenced by Agent %q not found", runtimeName, agent.Name), nil
		}
		return "", err
	}
	switch runtimeObject.RegisteredContractVersion() {
	case corev1alpha1.AgentRuntimeContractHarnessV1:
		return "", nil
	case corev1alpha1.AgentRuntimeContractHarnessV2:
		if binding.Spec.TaskDefaults.AgentRuntimeMaxTurns != nil {
			return fmt.Sprintf("taskDefaults.agentRuntimeMaxTurns is not supported by external %s AgentRuntime %q", corev1alpha1.AgentRuntimeContractHarnessV2, runtimeName), nil
		}
		if retry := binding.Spec.TaskDefaults.RetryPolicy; retry != nil && retry.MaxRetries > 0 {
			return fmt.Sprintf("taskDefaults.retryPolicy.maxRetries must be 0 for external %s AgentRuntime %q", corev1alpha1.AgentRuntimeContractHarnessV2, runtimeName), nil
		}
		policy, err := agentruntimepolicy.PolicyForRuntime(runtimeObject)
		if err != nil {
			return err.Error(), nil
		}
		if policy.WorkspaceIntent != corev1alpha1.WorkspaceIntentRead {
			return fmt.Sprintf("external %s AgentRuntime %q profile workspace intent %q does not match Gateway Task intent %q", corev1alpha1.AgentRuntimeContractHarnessV2, runtimeName, policy.WorkspaceIntent, corev1alpha1.WorkspaceIntentRead), nil
		}
		return "", nil
	default:
		return fmt.Sprintf("AgentRuntime %q referenced by Agent %q has no supported contractVersion", runtimeName, agent.Name), nil
	}
}

func validateGatewayClass(object *gatewayv1alpha1.GatewayClass) error {
	if object == nil {
		return fmt.Errorf("GatewayClass is required")
	}
	if object.Spec.ContractVersion != gatewayv1alpha1.ContractVersionV1 {
		return fmt.Errorf("unsupported contractVersion %q", object.Spec.ContractVersion)
	}
	switch object.Spec.Category {
	case gatewayv1alpha1.GatewayCategoryChat, gatewayv1alpha1.GatewayCategoryWebhook,
		gatewayv1alpha1.GatewayCategoryHTTP, gatewayv1alpha1.GatewayCategoryEvent, gatewayv1alpha1.GatewayCategoryInternal:
	default:
		return fmt.Errorf("unsupported category %q", object.Spec.Category)
	}
	seen := map[string]struct{}{}
	for _, key := range object.Spec.AllowedMetadataKeys {
		if len(key) == 0 || len(key) > protocol.MaxMetadataKeyBytes || !gatewayMetadataKeyPattern.MatchString(key) {
			return fmt.Errorf("allowedMetadataKeys contains invalid key")
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("allowedMetadataKeys contains duplicate key %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateGatewaySpec(object *gatewayv1alpha1.Gateway) error {
	if object == nil {
		return fmt.Errorf("gateway is required")
	}
	if strings.TrimSpace(object.Spec.GatewayClassName) == "" {
		return fmt.Errorf("gatewayClassName is required")
	}
	inboundName := strings.TrimSpace(object.Spec.InboundAuthRef.Name)
	outboundName := strings.TrimSpace(object.Spec.OutboundAuthRef.Name)
	if inboundName == outboundName {
		return fmt.Errorf("inboundAuthRef and outboundAuthRef must use separate Secrets")
	}
	for direction, ref := range map[string]gatewayv1alpha1.GatewayBearerAuthReference{
		"inboundAuthRef": object.Spec.InboundAuthRef, "outboundAuthRef": object.Spec.OutboundAuthRef,
	} {
		canonicalName := strings.TrimSpace(ref.Name)
		if canonicalName == "" || strings.TrimSpace(ref.Key) == "" {
			return fmt.Errorf("%s name and key are required", direction)
		}
		if ref.Name != canonicalName {
			return fmt.Errorf("%s name must not contain surrounding whitespace", direction)
		}
	}
	if len(object.Spec.Metadata) > protocol.MaxMetadataEntries {
		return fmt.Errorf("metadata exceeds %d entries", protocol.MaxMetadataEntries)
	}
	for key, value := range object.Spec.Metadata {
		if len(key) == 0 || len(key) > protocol.MaxMetadataKeyBytes || len(value) > protocol.MaxMetadataValueBytes {
			return fmt.Errorf("metadata exceeds safe bounds")
		}
	}
	return nil
}

func validateGatewayBindingSpec(object *gatewayv1alpha1.GatewayBinding) error {
	if object == nil {
		return fmt.Errorf("gateway binding is required")
	}
	if strings.TrimSpace(object.Spec.GatewayRef.Name) == "" || strings.TrimSpace(object.Spec.AgentRef.Name) == "" {
		return fmt.Errorf("gatewayRef.name and agentRef.name are required")
	}
	if err := validateGatewayBindingMatch(object.Spec.Match); err != nil {
		return err
	}
	if err := validateGatewaySenderPolicy(object.Spec.Match, object.Spec.SenderPolicy); err != nil {
		return err
	}
	if err := validateGatewaySessionSpec(object.Spec.Session); err != nil {
		return err
	}
	if behavior := object.Spec.ActiveTurnBehavior; behavior != "" && behavior != gatewayv1alpha1.GatewayActiveTurnQueue {
		return fmt.Errorf("activeTurnBehavior must be queue")
	}
	if err := validateGatewayTaskDefaults(object.Spec.TaskDefaults); err != nil {
		return err
	}
	return nil
}

func validateGatewayBindingMatch(match gatewayv1alpha1.GatewayBindingMatch) error {
	for name, value := range map[string]string{
		"match.accountId": match.AccountID,
		"match.contextId": match.ContextID,
	} {
		if !isNormalizedGatewayIdentity(value, true) {
			return fmt.Errorf("%s must be a normalized identity", name)
		}
	}
	for name, value := range map[string]string{
		"match.threadId": match.ThreadID,
		"match.senderId": match.SenderID,
	} {
		if !isNormalizedGatewayIdentity(value, false) {
			return fmt.Errorf("%s must be a normalized identity", name)
		}
	}
	return nil
}

func validateGatewaySenderPolicy(match gatewayv1alpha1.GatewayBindingMatch, policy gatewayv1alpha1.GatewaySenderPolicy) error {
	mode := policy.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySenderPolicyAllowlist
	}
	if mode != gatewayv1alpha1.GatewaySenderPolicyAllowlist && mode != gatewayv1alpha1.GatewaySenderPolicyAll {
		return fmt.Errorf("unsupported senderPolicy.mode %q", mode)
	}
	if mode == gatewayv1alpha1.GatewaySenderPolicyAllowlist && len(policy.AllowedSenderIDs) == 0 && match.SenderID == "" {
		return fmt.Errorf("allowlist sender policy requires allowedSenderIds or match.senderId")
	}
	seen := map[string]struct{}{}
	for _, sender := range policy.AllowedSenderIDs {
		if !isNormalizedGatewayIdentity(sender, true) {
			return fmt.Errorf("allowedSenderIds contains a non-normalized identity")
		}
		if _, duplicate := seen[sender]; duplicate {
			return fmt.Errorf("allowedSenderIds contains duplicate identity %q", sender)
		}
		seen[sender] = struct{}{}
	}
	return nil
}

func validateGatewaySessionSpec(session gatewayv1alpha1.GatewaySessionSpec) error {
	mode := session.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySessionContext
	}
	switch mode {
	case gatewayv1alpha1.GatewaySessionEphemeral, gatewayv1alpha1.GatewaySessionContext,
		gatewayv1alpha1.GatewaySessionThread, gatewayv1alpha1.GatewaySessionSender,
		gatewayv1alpha1.GatewaySessionContextSender, gatewayv1alpha1.GatewaySessionThreadSender,
		gatewayv1alpha1.GatewaySessionExplicit:
	default:
		return fmt.Errorf("unsupported session.mode %q", mode)
	}
	if mode == gatewayv1alpha1.GatewaySessionExplicit && strings.TrimSpace(session.Name) == "" {
		return fmt.Errorf("session.name is required for explicit mode")
	}
	if len(session.Name) > 253 || strings.ContainsFunc(session.Name, unicode.IsControl) {
		return fmt.Errorf("session.name exceeds safe bounds")
	}
	if mode != gatewayv1alpha1.GatewaySessionExplicit && strings.TrimSpace(session.Name) != "" {
		return fmt.Errorf("session.name is allowed only for explicit mode")
	}
	return nil
}

func validateGatewayTaskDefaults(defaults gatewayv1alpha1.GatewayTaskDefaults) error {
	if defaults.Priority != nil && (*defaults.Priority < 0 || *defaults.Priority > 1000) {
		return fmt.Errorf("taskDefaults.priority must be between 0 and 1000")
	}
	if defaults.Timeout != nil && (defaults.Timeout.Duration <= 0 || defaults.Timeout.Duration > 24*time.Hour) {
		return fmt.Errorf("taskDefaults.timeout must be positive and no greater than 24h")
	}
	if defaults.AgentRuntimeMaxTurns != nil && (*defaults.AgentRuntimeMaxTurns < 1 || *defaults.AgentRuntimeMaxTurns > 1000) {
		return fmt.Errorf("taskDefaults.agentRuntimeMaxTurns must be between 1 and 1000")
	}
	if retry := defaults.RetryPolicy; retry != nil {
		if retry.MaxRetries < 0 || retry.MaxRetries > 10 {
			return fmt.Errorf("taskDefaults.retryPolicy.maxRetries must be between 0 and 10")
		}
		if retry.BackoffMultiplier != 0 && (retry.BackoffMultiplier < 1 || retry.BackoffMultiplier > 10) {
			return fmt.Errorf("taskDefaults.retryPolicy.backoffMultiplier must be between 1 and 10")
		}
		if retry.InitialDelay != nil && (retry.InitialDelay.Duration <= 0 || retry.InitialDelay.Duration > time.Hour) {
			return fmt.Errorf("taskDefaults.retryPolicy.initialDelay must be positive and no greater than 1h")
		}
	}
	return nil
}

func isNormalizedGatewayIdentity(value string, required bool) bool {
	if required && strings.TrimSpace(value) == "" {
		return false
	}
	return strings.TrimSpace(value) == value && len(value) <= protocol.MaxIdentityBytes &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validateRequiredGatewayCapabilities(required, observed gatewayv1alpha1.GatewayCapabilities) error {
	checks := []struct {
		name string
		want bool
		got  bool
	}{
		{"inboundText", required.InboundText, observed.InboundText},
		{"outboundText", required.OutboundText, observed.OutboundText},
		{"threads", required.Threads, observed.Threads},
		{"senderIdentity", required.SenderIdentity, observed.SenderIdentity},
		{"explicitSessions", required.ExplicitSessions, observed.ExplicitSessions},
		{"idempotentDelivery", required.IdempotentDelivery, observed.IdempotentDelivery},
	}
	for _, check := range checks {
		if check.want && !check.got {
			return fmt.Errorf("adapter does not advertise required %s capability", check.name)
		}
	}
	return nil
}

func validateBindingCapabilities(binding *gatewayv1alpha1.GatewayBinding, capabilities gatewayv1alpha1.GatewayCapabilities) error {
	if !capabilities.InboundText || !capabilities.OutboundText || !capabilities.IdempotentDelivery {
		return fmt.Errorf("gateway lacks required V1 text or idempotent delivery capabilities")
	}
	mode := binding.Spec.Session.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySessionContext
	}
	if (binding.Spec.Match.ThreadID != "" || mode == gatewayv1alpha1.GatewaySessionThread || mode == gatewayv1alpha1.GatewaySessionThreadSender) && !capabilities.Threads {
		return fmt.Errorf("binding requires threads but Gateway does not advertise threads")
	}
	requiresSenderIdentity := binding.Spec.Match.SenderID != "" ||
		len(binding.Spec.SenderPolicy.AllowedSenderIDs) > 0 ||
		mode == gatewayv1alpha1.GatewaySessionSender ||
		mode == gatewayv1alpha1.GatewaySessionContextSender ||
		mode == gatewayv1alpha1.GatewaySessionThreadSender
	if requiresSenderIdentity && !capabilities.SenderIdentity {
		return fmt.Errorf("binding sender matching or session mode requires senderIdentity capability")
	}
	if mode == gatewayv1alpha1.GatewaySessionExplicit && !capabilities.ExplicitSessions {
		return fmt.Errorf("binding requires explicitSessions capability")
	}
	return nil
}

func observedGatewayCapabilities(response *protocol.CapabilitiesResponse) *gatewayv1alpha1.GatewayObservedCapabilities {
	if response == nil {
		return nil
	}
	return &gatewayv1alpha1.GatewayObservedCapabilities{
		ContractVersion: sanitizeGatewayCapability(response.ProtocolVersion),
		AdapterName:     sanitizeGatewayCapability(response.AdapterName),
		AdapterVersion:  sanitizeGatewayCapability(response.AdapterVersion),
		Capabilities: gatewayv1alpha1.GatewayCapabilities{
			InboundText: response.Capabilities.InboundText, OutboundText: response.Capabilities.OutboundText,
			Threads: response.Capabilities.Threads, SenderIdentity: response.Capabilities.SenderIdentity,
			ExplicitSessions: response.Capabilities.ExplicitSessions, IdempotentDelivery: response.Capabilities.IdempotentDelivery,
		},
	}
}

func gatewayBindingsOverlap(left, right *gatewayv1alpha1.GatewayBinding) bool {
	if left.Spec.GatewayRef.Name != right.Spec.GatewayRef.Name ||
		left.Spec.Match.AccountID != right.Spec.Match.AccountID || left.Spec.Match.ContextID != right.Spec.Match.ContextID {
		return false
	}
	if left.Spec.Match.ThreadID != "" && right.Spec.Match.ThreadID != "" && left.Spec.Match.ThreadID != right.Spec.Match.ThreadID {
		return false
	}
	leftSenders, leftAll := gatewayBindingSenderSet(left)
	rightSenders, rightAll := gatewayBindingSenderSet(right)
	if leftAll || rightAll {
		return true
	}
	for sender := range leftSenders {
		if _, ok := rightSenders[sender]; ok {
			return true
		}
	}
	return false
}

func gatewayBindingSenderSet(binding *gatewayv1alpha1.GatewayBinding) (map[string]struct{}, bool) {
	exactSender := strings.TrimSpace(binding.Spec.Match.SenderID)
	mode := binding.Spec.SenderPolicy.Mode
	if mode == "" {
		mode = gatewayv1alpha1.GatewaySenderPolicyAllowlist
	}
	if mode == gatewayv1alpha1.GatewaySenderPolicyAll && exactSender == "" {
		return nil, true
	}
	if exactSender != "" {
		if mode == gatewayv1alpha1.GatewaySenderPolicyAll || len(binding.Spec.SenderPolicy.AllowedSenderIDs) == 0 ||
			slices.Contains(binding.Spec.SenderPolicy.AllowedSenderIDs, exactSender) {
			return map[string]struct{}{exactSender: {}}, false
		}
		return map[string]struct{}{}, false
	}
	values := append([]string(nil), binding.Spec.SenderPolicy.AllowedSenderIDs...)
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set, false
}

func setGatewayCondition(conditions *[]metav1.Condition, conditionType string, value bool, trueReason, falseReason string, generation int64, message string, now metav1.Time) {
	condition := metav1.Condition{
		Type: conditionType, ObservedGeneration: generation, LastTransitionTime: now, Message: message,
	}
	if value {
		condition.Status = metav1.ConditionTrue
		condition.Reason = trueReason
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Reason = falseReason
	}
	meta.SetStatusCondition(conditions, condition)
}

func sanitizeGatewayStatusMessage(message string) string {
	message = events.RedactExecutionEventText(strings.TrimSpace(message))
	return truncateUTF8(strings.ToValidUTF8(message, "�"), 1024)
}

func sanitizeGatewayCapability(value string) string {
	value = events.RedactExecutionEventText(strings.TrimSpace(value))
	return truncateUTF8(strings.ToValidUTF8(value, "�"), protocol.MaxIdentityBytes)
}
