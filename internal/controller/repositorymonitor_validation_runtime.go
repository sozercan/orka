/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	distributionref "github.com/distribution/reference"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	repositoryMonitorValidationNetworkGateVolume      = "validation-network-gate"
	repositoryMonitorValidationNetworkSandboxVolume   = "validation-network-sandbox"
	repositoryMonitorValidationCommandSourceVolume    = "validation-command-source"
	repositoryMonitorValidationCommandVolume          = "validation-command"
	repositoryMonitorValidationNetworkProbeContainer  = "probe-validation-network-access"
	repositoryMonitorValidationCommandContainer       = "materialize-validation-command"
	repositoryMonitorValidationNetworkGateContainer   = "await-validation-network-policy"
	repositoryMonitorValidationNetworkGateMount       = "/var/run/orka/validation-network"
	repositoryMonitorValidationNetworkSandboxMount    = "/var/run/orka/validation-sandbox"
	repositoryMonitorValidationCommandSourceMount     = "/var/run/orka/validation-command-source"
	repositoryMonitorValidationCommandMount           = "/var/run/orka/validation-command"
	repositoryMonitorValidationNetworkSandboxBinary   = "worker"
	repositoryMonitorValidationCommandFile            = "command"
	repositoryMonitorValidationNetworkGateKey         = "ready"
	repositoryMonitorValidationNetworkGatePending     = "false"
	repositoryMonitorValidationNetworkGateReady       = "true"
	repositoryMonitorValidationNetworkProbeWorkerMode = "--wait-for-validation-network-access"
	repositoryMonitorValidationCommandWorkerMode      = "--materialize-validation-command"
	repositoryMonitorValidationNetworkGateWorkerMode  = "--wait-for-validation-network-policy"
	repositoryMonitorValidationSandboxWorkerMode      = "--run-validation-network-sandbox"
	repositoryMonitorValidationWorkerContainer        = "worker"
)

var (
	errRepositoryMonitorValidationConfinement               = errors.New("repository validation confinement failed")
	errRepositoryMonitorValidationClassificationUnavailable = errors.New("repository validation classification unavailable")
)

func isRepositoryMonitorValidationTask(task *corev1alpha1.Task) bool {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeContainer ||
		task.Labels[labels.LabelCreatedBy] != repositoryMonitorTaskCreatedBy ||
		task.Labels[labels.LabelPurpose] != repositoryMonitorValidationPurpose ||
		strings.TrimSpace(task.Annotations[labels.AnnotationRepositoryValidationImage]) == "" ||
		strings.TrimSpace(task.Spec.Image) != strings.TrimSpace(task.Annotations[labels.AnnotationRepositoryValidationImage]) {
		return false
	}
	monitorName := strings.TrimSpace(task.Annotations[labels.AnnotationRepositoryMonitorName])
	parentName := labels.ParentTaskName(task.Labels, task.Annotations)
	parentUID := strings.TrimSpace(task.Annotations[labels.AnnotationParentTaskUID])
	owner := metav1.GetControllerOf(task)
	return monitorName != "" && parentName != "" && parentUID != "" && owner != nil &&
		owner.APIVersion == corev1alpha1.GroupVersion.String() &&
		owner.Kind == taskResourceKind && owner.Name == parentName && string(owner.UID) == parentUID
}

func mayBeRepositoryMonitorValidationTask(task *corev1alpha1.Task) bool {
	return task != nil && strings.HasSuffix(task.Name, "-validation")
}

func (r *TaskReconciler) repositoryMonitorValidationSafetyTask(ctx context.Context, task *corev1alpha1.Task) bool {
	if isRepositoryMonitorValidationTask(task) {
		return true
	}
	if !mayBeRepositoryMonitorValidationTask(task) {
		return false
	}
	if r.RepositoryValidationBindings == nil {
		return true
	}
	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.RepositoryValidationBindings, task.Namespace, task.Name)
	return err != nil || binding != nil
}

func (r *TaskReconciler) repositoryMonitorValidationResultTask(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if isRepositoryMonitorValidationTask(task) {
		return true, nil
	}
	if !mayBeRepositoryMonitorValidationTask(task) {
		return false, nil
	}
	if r.RepositoryValidationBindings == nil {
		return false, errRepositoryMonitorValidationClassificationUnavailable
	}
	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.RepositoryValidationBindings, task.Namespace, task.Name)
	if err != nil {
		if tools.IsRepositoryValidationCommandBindingInvalid(err) {
			return true, nil
		}
		return false, fmt.Errorf("%w: %v", errRepositoryMonitorValidationClassificationUnavailable, err)
	}
	return binding != nil, nil
}

func (r *TaskReconciler) repositoryMonitorValidationTask(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	if !mayBeRepositoryMonitorValidationTask(task) {
		return false, nil
	}
	if r.RepositoryValidationBindings == nil {
		return true, fmt.Errorf("%w: durable binding store is unavailable", errRepositoryMonitorValidationClassificationUnavailable)
	}

	binding, err := tools.FindRepositoryValidationCommandBinding(ctx, r.RepositoryValidationBindings, task.Namespace, task.Name)
	if err != nil {
		if tools.IsRepositoryValidationCommandBindingInvalid(err) {
			return true, repositoryMonitorValidationConfinementErrorf("load immutable validation provenance: %v", err)
		}
		return true, fmt.Errorf("load immutable validation provenance: %w", err)
	}
	if binding == nil {
		if isRepositoryMonitorValidationTask(task) {
			return true, repositoryMonitorValidationConfinementErrorf("immutable validation provenance is missing")
		}
		return false, nil
	}

	reader := r.validationResourceReader()
	monitor := &corev1alpha1.RepositoryMonitor{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: binding.MonitorNamespace, Name: binding.MonitorName}, monitor); err != nil {
		if apierrors.IsNotFound(err) {
			return true, repositoryMonitorValidationConfinementErrorf("bound RepositoryMonitor is missing")
		}
		return true, fmt.Errorf("load bound RepositoryMonitor: %w", err)
	}
	if string(monitor.UID) != binding.MonitorUID {
		return true, repositoryMonitorValidationConfinementErrorf("bound RepositoryMonitor identity changed")
	}
	parent := &corev1alpha1.Task{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: binding.MonitorNamespace, Name: binding.ReviewTaskName}, parent); err != nil {
		if apierrors.IsNotFound(err) {
			return true, repositoryMonitorValidationConfinementErrorf("bound review Task is missing")
		}
		return true, fmt.Errorf("load bound review Task: %w", err)
	}
	if string(parent.UID) != binding.ReviewTaskUID || !binding.MatchesReview(parent, monitor, binding.Image, binding.HeadSHA) {
		return true, repositoryMonitorValidationConfinementErrorf("immutable validation provenance does not match its review Task")
	}
	if err := validateRepositoryMonitorValidationTask(monitor, parent, task, binding.Image); err != nil {
		return true, repositoryMonitorValidationConfinementErrorf("validation Task no longer matches immutable provenance: %v", err)
	}
	commandSecret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{
		Namespace: task.Namespace,
		Name:      tools.RepositoryValidationCommandSecretName(task.Name),
	}, commandSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return true, repositoryMonitorValidationConfinementErrorf("immutable validation command Secret is missing")
		}
		return true, fmt.Errorf("load immutable validation command Secret: %w", err)
	}
	if err := tools.ValidateRepositoryValidationCommandSecret(parent, task, commandSecret, binding); err != nil {
		return true, repositoryMonitorValidationConfinementErrorf("immutable validation command Secret does not match its binding")
	}
	return true, nil
}

func repositoryMonitorValidationResourceLabels(task *corev1alpha1.Task) map[string]string {
	return map[string]string{
		labels.LabelManaged: managedLabelValue,
		labels.LabelPurpose: repositoryMonitorValidationPurpose,
		labels.LabelTask:    labels.SelectorValue(task.Name),
	}
}

func repositoryMonitorValidationOwnerReference(task *corev1alpha1.Task) metav1.OwnerReference {
	return *metav1.NewControllerRef(task, corev1alpha1.GroupVersion.WithKind("Task"))
}

func repositoryMonitorValidationGateConfigMap(task *corev1alpha1.Task) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            task.Name,
			Namespace:       task.Namespace,
			Labels:          repositoryMonitorValidationResourceLabels(task),
			OwnerReferences: []metav1.OwnerReference{repositoryMonitorValidationOwnerReference(task)},
		},
		Data: map[string]string{repositoryMonitorValidationNetworkGateKey: repositoryMonitorValidationNetworkGatePending},
	}
}

func repositoryMonitorValidationNetworkPolicy(task *corev1alpha1.Task) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:            task.Name,
			Namespace:       task.Namespace,
			Labels:          repositoryMonitorValidationResourceLabels(task),
			OwnerReferences: []metav1.OwnerReference{repositoryMonitorValidationOwnerReference(task)},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{
				labels.LabelTask: labels.SelectorValue(task.Name),
			}},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
		},
	}
}

func repositoryMonitorValidationConfinementErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errRepositoryMonitorValidationConfinement, fmt.Sprintf(format, args...))
}

func (r *TaskReconciler) validationResourceReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *TaskReconciler) ensureRepositoryMonitorValidationNetworkGate(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	validationTask, err := r.repositoryMonitorValidationTask(ctx, task)
	if err != nil {
		return false, err
	}
	return r.ensureRepositoryMonitorValidationNetworkGateForTask(ctx, task, validationTask)
}

func (r *TaskReconciler) ensureRepositoryMonitorValidationNetworkGateForTask(ctx context.Context, task *corev1alpha1.Task, validationTask bool) (bool, error) {
	if !validationTask {
		return true, nil
	}
	current := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: task.Name, Namespace: task.Namespace}
	if err := r.validationResourceReader().Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		if err := r.Create(ctx, repositoryMonitorValidationGateConfigMap(task)); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		return false, nil
	}
	if err := validateRepositoryMonitorValidationGateConfigMap(task, current); err != nil {
		return false, err
	}
	if current.Data[repositoryMonitorValidationNetworkGateKey] != repositoryMonitorValidationNetworkGatePending {
		return false, repositoryMonitorValidationConfinementErrorf("network gate was released before validation Job creation")
	}
	return true, nil
}

func validateRepositoryMonitorValidationGateConfigMap(task *corev1alpha1.Task, gate *corev1.ConfigMap) error {
	if gate == nil || !metav1.IsControlledBy(gate, task) {
		return repositoryMonitorValidationConfinementErrorf("network gate is not controlled by validation Task %s/%s", task.Namespace, task.Name)
	}
	for key, value := range repositoryMonitorValidationResourceLabels(task) {
		if gate.Labels[key] != value {
			return repositoryMonitorValidationConfinementErrorf("network gate label %s does not match validation Task", key)
		}
	}
	state := gate.Data[repositoryMonitorValidationNetworkGateKey]
	if state != repositoryMonitorValidationNetworkGatePending && state != repositoryMonitorValidationNetworkGateReady {
		return repositoryMonitorValidationConfinementErrorf("network gate state is invalid")
	}
	return nil
}

func validateRepositoryMonitorValidationNetworkPolicy(task *corev1alpha1.Task, policy *networkingv1.NetworkPolicy) error {
	if policy == nil || !metav1.IsControlledBy(policy, task) {
		return repositoryMonitorValidationConfinementErrorf("NetworkPolicy is not controlled by validation Task %s/%s", task.Namespace, task.Name)
	}
	for key, value := range repositoryMonitorValidationResourceLabels(task) {
		if policy.Labels[key] != value {
			return repositoryMonitorValidationConfinementErrorf("NetworkPolicy label %s does not match validation Task", key)
		}
	}
	if !reflect.DeepEqual(policy.Spec, repositoryMonitorValidationNetworkPolicy(task).Spec) {
		return repositoryMonitorValidationConfinementErrorf("NetworkPolicy no longer denies all validation Pod ingress and egress")
	}
	return nil
}

func (r *TaskReconciler) reconcileRepositoryMonitorValidationConfinement(ctx context.Context, task *corev1alpha1.Task, job *batchv1.Job) error {
	validationTask, err := r.repositoryMonitorValidationTask(ctx, task)
	if err != nil {
		return err
	}
	if !validationTask {
		return nil
	}
	if err := r.validateRepositoryMonitorValidationJob(ctx, task, job); err != nil {
		return err
	}
	gate := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: task.Name, Namespace: task.Namespace}
	if err := r.validationResourceReader().Get(ctx, key, gate); err != nil {
		if apierrors.IsNotFound(err) {
			return repositoryMonitorValidationConfinementErrorf("network gate is missing after validation Job creation")
		}
		return err
	}
	if err := validateRepositoryMonitorValidationGateConfigMap(task, gate); err != nil {
		return err
	}

	pod, err := r.repositoryMonitorValidationPod(ctx, task, job)
	if err != nil {
		return err
	}
	gateState := gate.Data[repositoryMonitorValidationNetworkGateKey]
	if pod == nil {
		if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
			return repositoryMonitorValidationConfinementErrorf("validation Job reached a terminal state without its exact Pod")
		}
		if gateState == repositoryMonitorValidationNetworkGateReady {
			return repositoryMonitorValidationConfinementErrorf("validation Pod disappeared after network gate release")
		}
		return nil
	}
	prepared, err := repositoryMonitorValidationPreconditionsCompleted(pod)
	if err != nil {
		return err
	}
	if !prepared {
		if gateState == repositoryMonitorValidationNetworkGateReady {
			return repositoryMonitorValidationConfinementErrorf("network gate was released before workspace preparation completed")
		}
		return nil
	}

	policy := &networkingv1.NetworkPolicy{}
	policyErr := r.validationResourceReader().Get(ctx, key, policy)
	if gateState == repositoryMonitorValidationNetworkGateReady {
		if apierrors.IsNotFound(policyErr) {
			return repositoryMonitorValidationConfinementErrorf("NetworkPolicy disappeared after validation command release")
		}
		if policyErr != nil {
			return policyErr
		}
		return validateRepositoryMonitorValidationNetworkPolicy(task, policy)
	}
	if policyErr != nil {
		if !apierrors.IsNotFound(policyErr) {
			return policyErr
		}
		if err := r.Create(ctx, repositoryMonitorValidationNetworkPolicy(task)); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		// A later reconcile marks the policy ready. The Pod gate then proves the
		// repository endpoint is blocked from the Pod network namespace before
		// the validation container can start.
		return nil
	}
	if err := validateRepositoryMonitorValidationNetworkPolicy(task, policy); err != nil {
		return err
	}
	patch := client.MergeFrom(gate.DeepCopy())
	gate.Data[repositoryMonitorValidationNetworkGateKey] = repositoryMonitorValidationNetworkGateReady
	return r.Patch(ctx, gate, patch)
}

func (r *TaskReconciler) validateRepositoryMonitorValidationJob(ctx context.Context, task *corev1alpha1.Task, actual *batchv1.Job) error {
	if r.JobBuilder == nil || r.Scheme == nil {
		return repositoryMonitorValidationConfinementErrorf("validation Job renderer is unavailable")
	}
	renderTask := task.DeepCopy()
	if renderTask.Status.Attempts > 0 {
		renderTask.Status.Attempts--
	}
	expected, err := r.JobBuilder.BuildWithOptions(ctx, renderTask, nil, nil, JobBuildOptions{RepositoryMonitorValidation: true})
	if err != nil {
		return repositoryMonitorValidationConfinementErrorf("render expected validation Job: %v", err)
	}
	if err := controllerutil.SetControllerReference(task, expected, r.Scheme); err != nil {
		return repositoryMonitorValidationConfinementErrorf("bind expected validation Job: %v", err)
	}
	return validateRepositoryMonitorValidationJobAgainstExpected(task, actual, expected)
}

func validateRepositoryMonitorValidationJobAgainstExpected(task *corev1alpha1.Task, actual, expected *batchv1.Job) error {
	if task == nil || actual == nil || expected == nil || actual.Name != expected.Name || actual.Namespace != expected.Namespace {
		return repositoryMonitorValidationConfinementErrorf("validation Job identity does not match the Task")
	}
	if actual.UID == "" || !metav1.IsControlledBy(actual, task) {
		return repositoryMonitorValidationConfinementErrorf("validation Job is not controlled by validation Task %s/%s", task.Namespace, task.Name)
	}

	normalizedExpected := expected.DeepCopy()
	normalizedActual := actual.DeepCopy()
	if err := defaultExpectedRepositoryMonitorValidationJob(normalizedExpected, normalizedActual); err != nil {
		return err
	}
	if err := normalizeRepositoryMonitorValidationJobAPIDefaults(normalizedActual, normalizedActual); err != nil {
		return err
	}
	if !apiequality.Semantic.DeepEqual(normalizedExpected.Spec, normalizedActual.Spec) {
		return repositoryMonitorValidationConfinementErrorf("validation Job spec does not match the controller-rendered execution contract")
	}
	return nil
}

func defaultExpectedRepositoryMonitorValidationJob(expected, actual *batchv1.Job) error {
	if err := normalizeRepositoryMonitorValidationJobAPIDefaults(expected, actual); err != nil {
		return err
	}

	jobUID := string(actual.UID)
	if expected.Spec.Template.Labels == nil {
		expected.Spec.Template.Labels = map[string]string{}
	}
	expected.Spec.Template.Labels["job-name"] = expected.Name
	expected.Spec.Template.Labels[batchv1.JobNameLabel] = expected.Name
	expected.Spec.Template.Labels["controller-uid"] = jobUID
	expected.Spec.Template.Labels[batchv1.ControllerUidLabel] = jobUID
	expected.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
		batchv1.ControllerUidLabel: jobUID,
	}}
	return nil
}

func normalizeRepositoryMonitorValidationJobAPIDefaults(expected, actual *batchv1.Job) error {
	one := int32(1)
	if expected.Spec.Completions == nil && expected.Spec.Parallelism == nil {
		expected.Spec.Completions = &one
		expected.Spec.Parallelism = &one
	}
	if expected.Spec.Parallelism == nil {
		expected.Spec.Parallelism = &one
	}
	if expected.Spec.CompletionMode == nil {
		mode := batchv1.NonIndexedCompletion
		expected.Spec.CompletionMode = &mode
	}
	if expected.Spec.Suspend == nil {
		expected.Spec.Suspend = new(bool)
	}
	if expected.Spec.ManualSelector == nil {
		expected.Spec.ManualSelector = new(bool)
	}
	if actual.Spec.PodReplacementPolicy != nil {
		if *actual.Spec.PodReplacementPolicy != batchv1.TerminatingOrFailed {
			return repositoryMonitorValidationConfinementErrorf("validation Job pod replacement policy does not match the API default")
		}
		policy := *actual.Spec.PodReplacementPolicy
		expected.Spec.PodReplacementPolicy = &policy
	}
	defaultRepositoryMonitorValidationPodSpec(&expected.Spec.Template.Spec)
	return nil
}

func defaultRepositoryMonitorValidationPodSpec(spec *corev1.PodSpec) {
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.TerminationGracePeriodSeconds == nil {
		seconds := int64(corev1.DefaultTerminationGracePeriodSeconds)
		spec.TerminationGracePeriodSeconds = &seconds
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	if spec.EnableServiceLinks == nil {
		enabled := corev1.DefaultEnableServiceLinks
		spec.EnableServiceLinks = &enabled
	}
	for i := range spec.Volumes {
		volume := &spec.Volumes[i]
		switch {
		case volume.Secret != nil && volume.Secret.DefaultMode == nil:
			mode := corev1.SecretVolumeSourceDefaultMode
			volume.Secret.DefaultMode = &mode
		case volume.ConfigMap != nil && volume.ConfigMap.DefaultMode == nil:
			mode := corev1.ConfigMapVolumeSourceDefaultMode
			volume.ConfigMap.DefaultMode = &mode
		case volume.Projected != nil && volume.Projected.DefaultMode == nil:
			mode := corev1.ProjectedVolumeSourceDefaultMode
			volume.Projected.DefaultMode = &mode
		case volume.DownwardAPI != nil && volume.DownwardAPI.DefaultMode == nil:
			mode := corev1.DownwardAPIVolumeSourceDefaultMode
			volume.DownwardAPI.DefaultMode = &mode
		}
	}
	for i := range spec.InitContainers {
		defaultRepositoryMonitorValidationContainer(&spec.InitContainers[i])
	}
	for i := range spec.Containers {
		defaultRepositoryMonitorValidationContainer(&spec.Containers[i])
	}
}

func defaultRepositoryMonitorValidationContainer(container *corev1.Container) {
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = corev1.PullIfNotPresent
		if imageUsesLatestTag(container.Image) {
			container.ImagePullPolicy = corev1.PullAlways
		}
	}
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
}

func imageUsesLatestTag(image string) bool {
	named, err := distributionref.ParseNormalizedNamed(strings.TrimSpace(image))
	if err != nil {
		return false
	}
	tagged, ok := distributionref.TagNameOnly(named).(distributionref.Tagged)
	return ok && tagged.Tag() == "latest"
}

func (r *TaskReconciler) repositoryMonitorValidationPod(ctx context.Context, task *corev1alpha1.Task, job *batchv1.Job) (*corev1.Pod, error) {
	if job == nil {
		return nil, repositoryMonitorValidationConfinementErrorf("validation Job is missing")
	}
	var pods corev1.PodList
	if err := r.validationResourceReader().List(ctx, &pods,
		client.InNamespace(task.Namespace),
		client.MatchingLabels{labels.LabelTask: labels.SelectorValue(task.Name)},
	); err != nil {
		return nil, err
	}
	var matched *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		if err := validateRepositoryMonitorValidationPodAgainstJob(pod, job); err != nil {
			return nil, err
		}
		if matched != nil {
			return nil, repositoryMonitorValidationConfinementErrorf("multiple Pods belong to validation Job %s/%s", job.Namespace, job.Name)
		}
		matched = pod
	}
	return matched, nil
}

func (r *TaskReconciler) repositoryMonitorValidationCommandFailed(ctx context.Context, task *corev1alpha1.Task, job *batchv1.Job) (bool, error) {
	pod, err := r.repositoryMonitorValidationPod(ctx, task, job)
	if err != nil || pod == nil {
		return false, err
	}
	if repositoryMonitorValidationPodDisrupted(pod) {
		return false, nil
	}
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != repositoryMonitorValidationWorkerContainer {
			continue
		}
		terminated := status.State.Terminated
		if terminated == nil {
			terminated = status.LastTerminationState.Terminated
		}
		if terminated == nil || terminated.ExitCode == 0 || !repositoryMonitorValidationTerminationExecuted(terminated) {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func repositoryMonitorValidationPodDisrupted(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(pod.Status.Reason), "Evicted") {
		return true
	}
	for i := range pod.Status.Conditions {
		condition := &pod.Status.Conditions[i]
		if condition.Status == corev1.ConditionTrue && condition.Type == corev1.PodConditionType("DisruptionTarget") {
			return true
		}
	}
	return false
}

func repositoryMonitorValidationTerminationExecuted(terminated *corev1.ContainerStateTerminated) bool {
	if terminated == nil || terminated.StartedAt.IsZero() ||
		terminated.ExitCode == int32(workerenv.RepositoryValidationUnavailableExitCode) {
		return false
	}
	switch terminated.Reason {
	case "ContainerCannotRun", "StartError":
		return false
	default:
		return true
	}
}

func (r *TaskReconciler) repositoryMonitorValidationCommandStarted(ctx context.Context, task *corev1alpha1.Task) (bool, error) {
	validationTask, err := r.repositoryMonitorValidationTask(ctx, task)
	if err != nil || !validationTask || strings.TrimSpace(task.Status.JobName) == "" {
		return false, err
	}
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Status.JobName, Namespace: task.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if err := r.reconcileRepositoryMonitorValidationConfinement(ctx, task, job); err != nil {
		return false, err
	}
	pod, err := r.repositoryMonitorValidationPod(ctx, task, job)
	if err != nil || pod == nil {
		return false, err
	}
	if repositoryMonitorValidationPodDisrupted(pod) {
		return false, nil
	}
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != repositoryMonitorValidationWorkerContainer {
			continue
		}
		return status.State.Running != nil && !status.State.Running.StartedAt.IsZero() ||
			repositoryMonitorValidationTerminationExecuted(status.State.Terminated) ||
			repositoryMonitorValidationTerminationExecuted(status.LastTerminationState.Terminated), nil
	}
	return false, nil
}

func validateRepositoryMonitorValidationPodAgainstJob(pod *corev1.Pod, job *batchv1.Job) error {
	if pod == nil || job == nil || pod.Namespace != job.Namespace {
		return repositoryMonitorValidationConfinementErrorf("validation Pod identity does not match the Job")
	}
	if !repositoryMonitorValidationMetadataContains(pod.Labels, job.Spec.Template.Labels) ||
		!repositoryMonitorValidationMetadataContains(pod.Annotations, job.Spec.Template.Annotations) {
		return repositoryMonitorValidationConfinementErrorf("validation Pod metadata does not match the rendered Job template")
	}
	if !apiequality.Semantic.DeepEqual(pod.Spec.InitContainers, job.Spec.Template.Spec.InitContainers) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.Containers, job.Spec.Template.Spec.Containers) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.EphemeralContainers, job.Spec.Template.Spec.EphemeralContainers) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.Volumes, job.Spec.Template.Spec.Volumes) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.SecurityContext, job.Spec.Template.Spec.SecurityContext) ||
		!reflect.DeepEqual(pod.Spec.AutomountServiceAccountToken, job.Spec.Template.Spec.AutomountServiceAccountToken) ||
		pod.Spec.ServiceAccountName != job.Spec.Template.Spec.ServiceAccountName ||
		pod.Spec.RestartPolicy != job.Spec.Template.Spec.RestartPolicy ||
		pod.Spec.DNSPolicy != job.Spec.Template.Spec.DNSPolicy ||
		!apiequality.Semantic.DeepEqual(pod.Spec.DNSConfig, job.Spec.Template.Spec.DNSConfig) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.HostAliases, job.Spec.Template.Spec.HostAliases) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.NodeSelector, job.Spec.Template.Spec.NodeSelector) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.Affinity, job.Spec.Template.Spec.Affinity) ||
		!apiequality.Semantic.DeepEqual(pod.Spec.Tolerations, job.Spec.Template.Spec.Tolerations) ||
		!reflect.DeepEqual(pod.Spec.RuntimeClassName, job.Spec.Template.Spec.RuntimeClassName) ||
		pod.Spec.HostNetwork != job.Spec.Template.Spec.HostNetwork || pod.Spec.HostPID != job.Spec.Template.Spec.HostPID || pod.Spec.HostIPC != job.Spec.Template.Spec.HostIPC ||
		!reflect.DeepEqual(pod.Spec.ShareProcessNamespace, job.Spec.Template.Spec.ShareProcessNamespace) ||
		!reflect.DeepEqual(pod.Spec.HostUsers, job.Spec.Template.Spec.HostUsers) {
		return repositoryMonitorValidationConfinementErrorf("validation Pod execution contract does not match the rendered Job template")
	}
	return nil
}

func repositoryMonitorValidationMetadataContains(actual, expected map[string]string) bool {
	for key, expectedValue := range expected {
		actualValue, ok := actual[key]
		if !ok || actualValue != expectedValue {
			return false
		}
	}
	return true
}

func repositoryMonitorValidationPreconditionsCompleted(pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, nil
	}
	foundWorkspace := false
	foundCommand := false
	foundProbe := false
	for i := range pod.Spec.InitContainers {
		switch pod.Spec.InitContainers[i].Name {
		case workspacePreparationInitContainerName:
			foundWorkspace = true
		case repositoryMonitorValidationCommandContainer:
			foundCommand = true
		case repositoryMonitorValidationNetworkProbeContainer:
			foundProbe = true
		}
	}
	if !foundWorkspace {
		return false, repositoryMonitorValidationConfinementErrorf("validation Pod is missing the workspace preparation init container")
	}
	if !foundCommand {
		return false, repositoryMonitorValidationConfinementErrorf("validation Pod is missing the command materializer init container")
	}
	if !foundProbe {
		return false, repositoryMonitorValidationConfinementErrorf("validation Pod is missing the pre-policy network probe init container")
	}

	workspacePrepared := false
	commandMaterialized := false
	networkBaselineEstablished := false
	for i := range pod.Status.InitContainerStatuses {
		status := &pod.Status.InitContainerStatuses[i]
		if status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 {
			continue
		}
		switch status.Name {
		case workspacePreparationInitContainerName:
			workspacePrepared = true
		case repositoryMonitorValidationCommandContainer:
			commandMaterialized = true
		case repositoryMonitorValidationNetworkProbeContainer:
			networkBaselineEstablished = true
		}
	}
	return workspacePrepared && commandMaterialized && networkBaselineEstablished, nil
}
