/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

// Package admission contains Kubernetes admission handlers for Orka CRDs.
package admission

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	// TaskProvenanceWebhookPath is the validating admission path for Task provenance fields.
	TaskProvenanceWebhookPath = "/validate-core-orka-ai-v1alpha1-task-provenance"

	fieldSpecRequestedBy     = "spec.requestedBy"
	fieldSpecTransaction     = "spec.transaction"
	fieldMetadataLabels      = "metadata.labels"
	fieldMetadataAnnotations = "metadata.annotations"

	// managedWorkspaceMetadataPrefix protects the controller-owned ACP
	// workspace settlement metadata (the settled marker, the workspace link
	// label, and the incarnation pin): forging them through direct Kubernetes
	// writes would skip controller-owned revocation and detach actions.
	managedWorkspaceMetadataPrefix = "acp.workspace.orka.ai/"
)

var (
	defaultTrustedWorkerServiceAccounts = []string{
		"orka-ai-worker",
		"orka-vendor-worker",
	}

	managedTransactionLabelKeys = []string{
		labels.LabelTransactionID,
		labels.LabelAuthProfile,
	}

	managedTransactionAnnotationKeys = []string{
		labels.AnnotationTransactionID,
		labels.AnnotationContextTokenProfile,
		labels.AnnotationTransactionIssuer,
		labels.AnnotationTransactionSubject,
		labels.AnnotationTransactionRequestingWorkload,
		labels.AnnotationTransactionScope,
		labels.AnnotationTransactionContextDigest,
		labels.AnnotationRequesterContextDigest,
		labels.AnnotationTransactionTokenSecret,
		labels.AnnotationTransactionTokenPending,
		labels.AnnotationTransactionTokenPendingSince,
		labels.AnnotationTraceParent,
		labels.AnnotationTraceState,
		labels.AnnotationTraceBaggage,
	}

	controllerManagedTaskAnnotationKeys = []string{
		labels.AnnotationParentTaskUID,
	}
)

// TaskProvenanceConfig configures direct Kubernetes admission protection for
// Orka-managed Task provenance fields.
type TaskProvenanceConfig struct {
	Enabled bool
	// ControllerUsernames are the controller identities allowed to write
	// EVERY Orka-managed field, including the reserved
	// acp.workspace.orka.ai/ settlement metadata. An explicit
	// --execution-mode-controller-usernames list is exclusive; the
	// namespace-derived default ServiceAccount usernames are used only when
	// no explicit controller identities were configured. The trusted-users
	// flag never extends this set.
	ControllerUsernames []string
	// TrustedUsernames (the --task-provenance-admission-trusted-users flag)
	// grants only the provenance-field allowance; workspace settlement
	// metadata stays reserved to ControllerUsernames.
	TrustedUsernames           []string
	TrustedServiceAccountNames []string
}

// NewTaskProvenanceConfig builds Task provenance admission config.
// controllerUsernames carries the deployment's exact controller identities
// (the --execution-mode-controller-usernames value); when supplied, ONLY that
// list receives full controller trust, and the namespace-derived
// ServiceAccount defaults apply solely to installations that configured no
// explicit identities.
func NewTaskProvenanceConfig(
	enabled bool,
	controllerUsernames, trustedUsernames, trustedServiceAccountNames, controllerNamespace string,
) TaskProvenanceConfig {
	cfg := TaskProvenanceConfig{Enabled: enabled}
	cfg.ControllerUsernames = workerenv.SplitCSV(controllerUsernames)
	if len(cfg.ControllerUsernames) == 0 {
		// The namespace-derived ServiceAccount identities are a FALLBACK for
		// installations that configured no explicit controller identities.
		// When the operator supplied an exact list, only that list receives
		// controller-only settlement trust - appending the defaults would
		// silently widen authorization to any same-named ServiceAccount in
		// the namespace (for example a release-specific Helm identity
		// coexisting with a generic controller-manager account).
		cfg.ControllerUsernames = defaultControllerServiceAccountUsernames(controllerNamespace)
	}
	cfg.TrustedUsernames = workerenv.SplitCSV(trustedUsernames)
	if len(cfg.TrustedUsernames) == 0 {
		cfg.TrustedUsernames = defaultControllerServiceAccountUsernames(controllerNamespace)
	}
	cfg.TrustedServiceAccountNames = workerenv.SplitCSV(trustedServiceAccountNames)
	if len(cfg.TrustedServiceAccountNames) == 0 {
		cfg.TrustedServiceAccountNames = append([]string{}, defaultTrustedWorkerServiceAccounts...)
	}
	return cfg
}

// RegisterTaskProvenanceWebhook registers the Task provenance validating webhook
// when enabled by configuration.
func RegisterTaskProvenanceWebhook(server webhook.Server, scheme *runtime.Scheme, cfg TaskProvenanceConfig) {
	if !cfg.Enabled {
		return
	}
	server.Register(TaskProvenanceWebhookPath, &ctrladmission.Webhook{
		Handler: NewTaskProvenanceValidator(scheme, cfg),
	})
}

// TaskProvenanceValidator rejects untrusted direct Kubernetes writes that set or
// modify Orka-managed provenance fields.
type TaskProvenanceValidator struct {
	decoder ctrladmission.Decoder
	config  TaskProvenanceConfig
}

// NewTaskProvenanceValidator creates a Task provenance admission handler.
func NewTaskProvenanceValidator(scheme *runtime.Scheme, cfg TaskProvenanceConfig) *TaskProvenanceValidator {
	return &TaskProvenanceValidator{
		decoder: ctrladmission.NewDecoder(scheme),
		config:  cfg,
	}
}

// Handle implements admission.Handler.
func (v *TaskProvenanceValidator) Handle(_ context.Context, req ctrladmission.Request) ctrladmission.Response {
	if (req.SubResource != "" && req.SubResource != statusSubresource) ||
		(req.Operation != admissionv1.Create && req.Operation != admissionv1.Update) {
		return ctrladmission.Allowed("not a Task provenance write")
	}
	if isTrustedControllerProvenanceUser(v.config, req.UserInfo) {
		return ctrladmission.Allowed("trusted Task provenance writer")
	}
	workerTrusted := isTrustedWorkerProvenanceUser(v.config, req.UserInfo, req.Namespace)

	task := &corev1alpha1.Task{}
	if err := v.decoder.Decode(req, task); err != nil {
		return ctrladmission.Errored(http.StatusBadRequest, fmt.Errorf("decode Task: %w", err))
	}

	switch req.Operation {
	case admissionv1.Create:
		fields := presentTaskProvenanceFields(task, workerTrusted)
		if len(fields) > 0 {
			return ctrladmission.Denied("direct Task create cannot set Orka-managed provenance fields: " + strings.Join(fields, ", "))
		}
	case admissionv1.Update:
		oldTask := &corev1alpha1.Task{}
		if err := v.decoder.DecodeRaw(req.OldObject, oldTask); err != nil {
			return ctrladmission.Errored(http.StatusBadRequest, fmt.Errorf("decode old Task: %w", err))
		}
		fields := changedTaskProvenanceFields(oldTask, task, workerTrusted)
		if len(fields) > 0 {
			return ctrladmission.Denied("direct Task update cannot modify Orka-managed provenance fields: " + strings.Join(fields, ", "))
		}
	}

	return ctrladmission.Allowed("Task provenance fields unchanged")
}

// presentTaskProvenanceFields lists Orka-managed fields present on a created
// Task. A trusted worker keeps its provenance-field allowance, but
// controller-authenticated lineage and workspace settlement metadata stay
// reserved to controller identities.
func presentTaskProvenanceFields(task *corev1alpha1.Task, workerTrusted bool) []string {
	fields := []string{}
	if !workerTrusted {
		if task.Spec.RequestedBy != nil {
			fields = append(fields, fieldSpecRequestedBy)
		}
		if task.Spec.Transaction != nil {
			fields = append(fields, fieldSpecTransaction)
		}
		fields = append(fields, presentManagedMapFields(fieldMetadataLabels, task.Labels, managedTransactionLabelKeys)...)
		fields = append(fields, presentManagedMapFields(fieldMetadataAnnotations, task.Annotations, managedTransactionAnnotationKeys)...)
	}
	fields = append(fields, presentManagedMapFields(fieldMetadataAnnotations, task.Annotations, controllerManagedTaskAnnotationKeys)...)
	fields = append(fields, presentManagedPrefixFields(fieldMetadataLabels, task.Labels)...)
	fields = append(fields, presentManagedPrefixFields(fieldMetadataAnnotations, task.Annotations)...)
	return fields
}

func changedTaskProvenanceFields(oldTask, newTask *corev1alpha1.Task, workerTrusted bool) []string {
	fields := []string{}
	if !workerTrusted {
		if !reflect.DeepEqual(oldTask.Spec.RequestedBy, newTask.Spec.RequestedBy) {
			fields = append(fields, fieldSpecRequestedBy)
		}
		if !reflect.DeepEqual(oldTask.Spec.Transaction, newTask.Spec.Transaction) {
			fields = append(fields, fieldSpecTransaction)
		}
		fields = append(fields, changedManagedMapFields(fieldMetadataLabels, oldTask.Labels, newTask.Labels, managedTransactionLabelKeys)...)
		fields = append(fields, changedManagedMapFields(fieldMetadataAnnotations, oldTask.Annotations, newTask.Annotations, managedTransactionAnnotationKeys)...)
	}
	fields = append(fields, changedManagedMapFields(fieldMetadataAnnotations, oldTask.Annotations, newTask.Annotations, controllerManagedTaskAnnotationKeys)...)
	fields = append(fields, changedManagedPrefixFields(fieldMetadataLabels, oldTask.Labels, newTask.Labels)...)
	fields = append(fields, changedManagedPrefixFields(fieldMetadataAnnotations, oldTask.Annotations, newTask.Annotations)...)
	return fields
}

func presentManagedPrefixFields(prefix string, values map[string]string) []string {
	fields := []string{}
	for key := range values {
		if strings.HasPrefix(key, managedWorkspaceMetadataPrefix) {
			fields = append(fields, prefix+"["+key+"]")
		}
	}
	slices.Sort(fields)
	return fields
}

func changedManagedPrefixFields(prefix string, oldValues, newValues map[string]string) []string {
	fields := []string{}
	keys := map[string]struct{}{}
	for key := range oldValues {
		if strings.HasPrefix(key, managedWorkspaceMetadataPrefix) {
			keys[key] = struct{}{}
		}
	}
	for key := range newValues {
		if strings.HasPrefix(key, managedWorkspaceMetadataPrefix) {
			keys[key] = struct{}{}
		}
	}
	for key := range keys {
		oldValue, oldOK := oldValues[key]
		newValue, newOK := newValues[key]
		if oldOK != newOK || oldValue != newValue {
			fields = append(fields, prefix+"["+key+"]")
		}
	}
	slices.Sort(fields)
	return fields
}

func presentManagedMapFields(prefix string, values map[string]string, keys []string) []string {
	fields := []string{}
	for _, key := range keys {
		if _, ok := values[key]; ok {
			fields = append(fields, prefix+"["+key+"]")
		}
	}
	return fields
}

func changedManagedMapFields(prefix string, oldValues, newValues map[string]string, keys []string) []string {
	fields := []string{}
	for _, key := range keys {
		oldValue, oldOK := oldValues[key]
		newValue, newOK := newValues[key]
		if oldOK != newOK || oldValue != newValue {
			fields = append(fields, prefix+"["+key+"]")
		}
	}
	return fields
}

// isTrustedControllerProvenanceUser reports a controller identity: the only
// writers allowed to touch every Orka-managed field, including the reserved
// workspace settlement metadata. Flag-supplied trusted users deliberately do
// NOT qualify — they keep only the provenance-field allowance.
func isTrustedControllerProvenanceUser(cfg TaskProvenanceConfig, user authenticationv1.UserInfo) bool {
	username := strings.TrimSpace(user.Username)
	return username != "" && slices.Contains(cfg.ControllerUsernames, username)
}

// isTrustedWorkerProvenanceUser reports a provenance-trusted principal — a
// trusted worker ServiceAccount or a flag-listed trusted user: it keeps the
// provenance-field allowance but never the workspace settlement metadata
// reservation.
func isTrustedWorkerProvenanceUser(cfg TaskProvenanceConfig, user authenticationv1.UserInfo, namespace string) bool {
	username := strings.TrimSpace(user.Username)
	if username == "" {
		return false
	}
	if slices.Contains(cfg.TrustedUsernames, username) {
		return true
	}
	for _, serviceAccountName := range cfg.TrustedServiceAccountNames {
		if username == serviceAccountUsername(namespace, serviceAccountName) {
			return true
		}
	}
	return false
}

func defaultControllerServiceAccountUsernames(namespace string) []string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil
	}
	return []string{
		serviceAccountUsername(namespace, "orka-controller-manager"),
		serviceAccountUsername(namespace, "controller-manager"),
	}
}

func serviceAccountUsername(namespace, name string) string {
	return "system:serviceaccount:" + strings.TrimSpace(namespace) + ":" + strings.TrimSpace(name)
}
