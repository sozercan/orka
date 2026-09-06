/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/executionmode"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/metrics"
	"github.com/orka-agents/orka/internal/taskmeta"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	// DefaultAIWorkerImage is the default image for AI tasks
	DefaultAIWorkerImage = "ghcr.io/orka-agents/orka/ai-worker:latest"

	// DefaultGeneralWorkerImage is the default image for container tasks
	DefaultGeneralWorkerImage = "ghcr.io/orka-agents/orka/general-worker:latest"

	// DefaultInitImage is the default image for init containers
	DefaultInitImage = "busybox:1.37"

	// AIWorkerServiceAccount is the ServiceAccount used by trusted AI task workers.
	AIWorkerServiceAccount = "orka-ai-worker"

	// VendorWorkerServiceAccount is the ServiceAccount used by untrusted vendor/agent task workers.
	VendorWorkerServiceAccount = "orka-vendor-worker"

	// ContainerWorkerServiceAccount is the ServiceAccount used by untrusted container task workers.
	ContainerWorkerServiceAccount = "orka-container-worker"

	// directProviderSecretsEnvVar restores legacy direct provider API key/base URL injection for untrusted container pods.
	directProviderSecretsEnvVar = "ORKA_AGENT_DIRECT_PROVIDER_SECRETS"

	// directSecretMountsEnvVar restores legacy direct task/agent secret injection for untrusted container pods.
	directSecretMountsEnvVar = "ORKA_AGENT_DIRECT_SECRET_MOUNTS"

	// directGitCredentialsEnvVar restores legacy direct Git credential mounts for untrusted custom container pods.
	directGitCredentialsEnvVar = "ORKA_AGENT_DIRECT_GIT_CREDENTIALS"

	// ResultEndpointEnvVar is the env var for the result submission URL
	ResultEndpointEnvVar = workerenv.ResultEndpoint

	// ControllerURLEnvVar is the env var for the controller base URL
	ControllerURLEnvVar = workerenv.ControllerURL

	// TaskNameEnvVar is the env var for the task name
	TaskNameEnvVar = workerenv.TaskName

	// TaskNamespaceEnvVar is the env var for the task namespace
	TaskNamespaceEnvVar = workerenv.TaskNamespace

	sessionTranscriptURLEnv         = "ORKA_SESSION_TRANSCRIPT_URL"
	sessionTranscriptRequiredEnv    = "ORKA_SESSION_TRANSCRIPT_REQUIRED"
	sessionTranscriptMaxAttemptsEnv = "ORKA_SESSION_TRANSCRIPT_MAX_ATTEMPTS"

	// defaultSecretKey is the default key name in provider secrets
	defaultSecretKey    = "api-key"
	taskWorkspaceVolume = "workspace"

	// Kubernetes Job names end up mirrored into pod labels like `job-name`,
	// which are capped at 63 characters.
	maxJobNameLength = 63

	workspacePreparationInitContainerName = "prepare-workspace"

	repositoryMonitorValidationUIDBase = int64(1_000_000_000)
	repositoryMonitorValidationUIDSpan = uint32(1_000_000_000)
)

// JobBuilder builds Kubernetes Jobs for Tasks
type JobBuilder struct {
	client.Client
	AIWorkerImage                              string
	GeneralWorkerImage                         string
	InitImage                                  string
	AIWorkerServiceAccountName                 string
	ContainerWorkerServiceAccountName          string
	ControllerURL                              string // e.g. http://orka-controller.orka-system.svc:8080
	ControllerMode                             executionmode.Mode
	ContextTokenTTSEndpoint                    string
	ContextTokenTTSAudience                    string
	ContextTokenTTSTimeout                     string
	ContextTokenTTSTokenSource                 string
	ContextTokenSubjectTokenType               string
	ContextTokenChildScope                     string
	ContextTokenOutboundScope                  string
	ContextTokenChildTokenTTL                  string
	ContextTokenToolTokenTTL                   string
	EnforceTransactionCredentialAuth           bool
	TransactionCredentialReadScopes            []string
	OutboundAccessTrustedGatewayServices       string
	OutboundAccessTrustedTokenEndpointServices string
	EnableTelemetry                            bool
	directSecrets                              directRuntimeSecretPolicy
}

type directRuntimeSecretPolicy struct {
	providerSecrets bool
	secretMounts    bool
	gitCredentials  bool
}

// NewJobBuilder creates a new JobBuilder
func NewJobBuilder(c client.Client) *JobBuilder {
	return &JobBuilder{
		Client:                            c,
		AIWorkerImage:                     DefaultAIWorkerImage,
		GeneralWorkerImage:                DefaultGeneralWorkerImage,
		InitImage:                         DefaultInitImage,
		AIWorkerServiceAccountName:        AIWorkerServiceAccount,
		ContainerWorkerServiceAccountName: ContainerWorkerServiceAccount,
		directSecrets: directRuntimeSecretPolicy{
			providerSecrets: envFlagEnabled(directProviderSecretsEnvVar),
			secretMounts:    envFlagEnabled(directSecretMountsEnvVar),
			gitCredentials:  envFlagEnabled(directGitCredentialsEnvVar),
		},
	}
}

func workerServiceAccountName(configured, fallback string) string {
	if configured != "" {
		return configured
	}
	return fallback
}

func (b *JobBuilder) workerServiceAccountForTask(task *corev1alpha1.Task) string {
	if task == nil {
		return workerServiceAccountName(b.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount)
	}
	if taskRequestsRuntimeAuthOnly(task) {
		return workerServiceAccountName(b.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount)
	}

	switch task.Spec.Type {
	case corev1alpha1.TaskTypeAI:
		return workerServiceAccountName(b.AIWorkerServiceAccountName, AIWorkerServiceAccount)
	case corev1alpha1.TaskTypeContainer:
		return workerServiceAccountName(b.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount)
	default:
		return workerServiceAccountName(b.ContainerWorkerServiceAccountName, ContainerWorkerServiceAccount)
	}
}

func workerAutomountServiceAccountToken(task *corev1alpha1.Task) *bool {
	return new(podShouldAutomountServiceAccountToken(task))
}

func podShouldAutomountServiceAccountToken(task *corev1alpha1.Task) bool {
	if taskRequestsReadOnlyAgent(task) {
		return false
	}
	if task == nil || !isUntrustedComputeTask(task) {
		return true
	}

	return taskUsesManagedOrkaWorker(task)
}

func taskUsesManagedOrkaWorker(task *corev1alpha1.Task) bool {
	if task == nil {
		return false
	}

	switch task.Spec.Type {
	case corev1alpha1.TaskTypeAI:
		return true
	case corev1alpha1.TaskTypeContainer:
		return task.Spec.Image == ""
	default:
		return false
	}
}

func isUntrustedComputeTask(task *corev1alpha1.Task) bool {
	return task != nil && task.Spec.Type == corev1alpha1.TaskTypeContainer
}

func (b *JobBuilder) directProviderSecretsAllowed(task *corev1alpha1.Task) bool {
	return taskAllowsDirectRuntimeSecrets(task) || (b != nil && b.directSecrets.providerSecrets)
}

func (b *JobBuilder) directSecretMountsAllowed(task *corev1alpha1.Task) bool {
	if taskRequestsReadOnlyAgent(task) {
		return false
	}
	return taskAllowsDirectRuntimeSecrets(task) || (b != nil && b.directSecrets.secretMounts)
}

func taskAllowsDirectRuntimeSecrets(task *corev1alpha1.Task) bool {
	return !isUntrustedComputeTask(task)
}

func mainContainerNeedsGitCredentials(task *corev1alpha1.Task) bool {
	return taskUsesManagedOrkaWorker(task) && !taskRequestsReadOnlyAgent(task)
}

func (b *JobBuilder) directGitCredentialsAllowed(task *corev1alpha1.Task) bool {
	return !isUntrustedComputeTask(task) || mainContainerNeedsGitCredentials(task) || (b != nil && b.directSecrets.gitCredentials)
}

func envFlagEnabled(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if enabled, err := strconv.ParseBool(value); err == nil {
		return enabled
	}

	switch strings.ToLower(value) {
	case "y", "yes", "on":
		return true
	case "n", "no", "off":
		return false
	default:
		return false
	}
}

func agentHasFallbackProviders(agent *corev1alpha1.Agent) bool {
	return agent != nil && agent.Spec.Model != nil && len(agent.Spec.Model.Fallbacks) > 0
}

var (
	defaultTaskResourceCPURequest      = *resource.NewMilliQuantity(100, resource.DecimalSI)
	defaultTaskResourceMemoryRequest   = *resource.NewQuantity(512*1024*1024, resource.BinarySI)
	defaultTaskResourceCPULimit        = *resource.NewQuantity(1, resource.DecimalSI)
	defaultTaskResourceMemoryLimit     = *resource.NewQuantity(2*1024*1024*1024, resource.BinarySI)
	repositoryValidationStorageRequest = resource.MustParse("256Mi")
	repositoryValidationStorageLimit   = resource.MustParse("4Gi")
	repositoryValidationTmpSizeLimit   = resource.MustParse("2Gi")
	repositoryValidationHomeSizeLimit  = resource.MustParse("2Gi")
	repositoryValidationWorkspaceLimit = resource.MustParse("4Gi")
	repositoryValidationCommandLimit   = resource.MustParse("16Ki")
)

var repositoryMonitorValidationShellWrapper = fmt.Sprintf(
	`exec >/dev/null 2>&1; /bin/sh "$0"; status=$?; if [ "$status" -eq %d ]; then exit 1; fi; exit "$status"`,
	workerenv.RepositoryValidationUnavailableExitCode,
)

func repositoryMonitorValidationRunAsUser(task *corev1alpha1.Task) (int64, error) {
	if task == nil || strings.TrimSpace(string(task.UID)) == "" {
		return 0, fmt.Errorf("repository validation task UID is required for process isolation")
	}
	digest := sha256.Sum256([]byte(task.UID))
	return repositoryMonitorValidationUIDBase + int64(binary.BigEndian.Uint32(digest[:4])%repositoryMonitorValidationUIDSpan), nil
}

func applyRepositoryMonitorValidationProcessLimit(job *batchv1.Job, task *corev1alpha1.Task) error {
	if job == nil {
		return fmt.Errorf("repository validation Job is required for process isolation")
	}
	runtimeUID, err := repositoryMonitorValidationRunAsUser(task)
	if err != nil {
		return err
	}
	if job.Spec.Template.Annotations == nil {
		job.Spec.Template.Annotations = map[string]string{}
	}
	// The shipped worker enforces RLIMIT_NPROC. A high Task-derived UID keeps
	// Linux's per-real-UID accounting isolated from ordinary worker Pods. A
	// hash collision only shares the same bound; it cannot raise the limit.
	job.Spec.Template.Annotations[runtimePoolPIDsAnnotation] = strconv.Itoa(workerenv.RepositoryValidationMaxProcesses)
	if job.Spec.Template.Spec.SecurityContext == nil {
		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	job.Spec.Template.Spec.SecurityContext.RunAsUser = &runtimeUID
	job.Spec.Template.Spec.SecurityContext.RunAsGroup = &runtimeUID
	job.Spec.Template.Spec.SecurityContext.FSGroup = &runtimeUID
	applyUID := func(container *corev1.Container) {
		if container.SecurityContext == nil {
			container.SecurityContext = &corev1.SecurityContext{}
		}
		container.SecurityContext.RunAsUser = &runtimeUID
		container.SecurityContext.RunAsGroup = &runtimeUID
	}
	for i := range job.Spec.Template.Spec.InitContainers {
		applyUID(&job.Spec.Template.Spec.InitContainers[i])
	}
	for i := range job.Spec.Template.Spec.Containers {
		applyUID(&job.Spec.Template.Spec.Containers[i])
	}
	return nil
}

func applyRepositoryMonitorValidationDefaultTolerations(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	seconds := int64(300)
	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		if repositoryMonitorValidationToleratesNoExecute(spec.Tolerations, key) {
			continue
		}
		spec.Tolerations = append(spec.Tolerations, corev1.Toleration{
			Key:               key,
			Operator:          corev1.TolerationOpExists,
			Effect:            corev1.TaintEffectNoExecute,
			TolerationSeconds: new(seconds),
		})
	}
}

func repositoryMonitorValidationToleratesNoExecute(tolerations []corev1.Toleration, key string) bool {
	for i := range tolerations {
		toleration := &tolerations[i]
		if (toleration.Key == key || toleration.Key == "") &&
			(toleration.Effect == corev1.TaintEffectNoExecute || toleration.Effect == "") {
			return true
		}
	}
	return false
}

func defaultTaskResourceRequirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    defaultTaskResourceCPURequest,
			corev1.ResourceMemory: defaultTaskResourceMemoryRequest,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    defaultTaskResourceCPULimit,
			corev1.ResourceMemory: defaultTaskResourceMemoryLimit,
		},
	}
}

func repositoryMonitorValidationEmptyDir(validationTask bool, limit resource.Quantity) *corev1.EmptyDirVolumeSource {
	emptyDir := &corev1.EmptyDirVolumeSource{}
	if validationTask {
		copy := limit.DeepCopy()
		emptyDir.SizeLimit = &copy
	}
	return emptyDir
}

func applyRepositoryMonitorValidationStorageBounds(job *batchv1.Job) {
	if job == nil {
		return
	}
	for i := range job.Spec.Template.Spec.InitContainers {
		applyRepositoryMonitorValidationContainerStorageBounds(&job.Spec.Template.Spec.InitContainers[i])
	}
	for i := range job.Spec.Template.Spec.Containers {
		applyRepositoryMonitorValidationContainerStorageBounds(&job.Spec.Template.Spec.Containers[i])
	}
}

func applyRepositoryMonitorValidationContainerStorageBounds(container *corev1.Container) {
	if container == nil {
		return
	}
	if container.Resources.Requests == nil {
		container.Resources.Requests = corev1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	container.Resources.Requests[corev1.ResourceEphemeralStorage] = repositoryValidationStorageRequest.DeepCopy()
	container.Resources.Limits[corev1.ResourceEphemeralStorage] = repositoryValidationStorageLimit.DeepCopy()
}

func (b *JobBuilder) needsSecretVolumes(task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) bool {
	if b.directSecretMountsAllowed(task) {
		if task != nil && task.Spec.SecretRef != nil {
			return true
		}
		if agent != nil && agent.Spec.SecretRef != nil {
			return true
		}
	}

	return b.directProviderSecretsAllowed(task) && (provider != nil || agentHasFallbackProviders(agent))
}

func buildTaskJobName(task *corev1alpha1.Task) string {
	uidPrefix := string(task.UID)
	if len(uidPrefix) > 8 {
		uidPrefix = uidPrefix[:8]
	}
	suffix := fmt.Sprintf("-job-%s-%d", uidPrefix, task.Status.Attempts)
	maxPrefixLength := max(1, maxJobNameLength-len(suffix))

	prefix := task.Name
	if len(prefix) > maxPrefixLength {
		prefix = strings.Trim(prefix[:maxPrefixLength], "-")
		if prefix == "" {
			prefix = "task"
		}
	}

	return prefix + suffix
}

// JobBuildOptions carries optional inputs that affect Job rendering while keeping
// the historical Build signature stable.
type JobBuildOptions struct {
	ResolvedApprovalsJSON       string
	RepositoryMonitorValidation bool
}

// Build creates a Job for the given Task.
func (b *JobBuilder) Build(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) (*batchv1.Job, error) {
	return b.BuildWithOptions(ctx, task, agent, provider, JobBuildOptions{})
}

// BuildWithOptions creates a Job for the given Task using additional resolved options.
func (b *JobBuilder) BuildWithOptions(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider, opts JobBuildOptions) (*batchv1.Job, error) {
	if err := validateContainerPublicationWorkspace(task); err != nil {
		return nil, err
	}
	if err := b.validateContainerDeliveredPromptSize(ctx, task, agent); err != nil {
		return nil, err
	}

	validationTask := opts.RepositoryMonitorValidation || isRepositoryMonitorValidationTask(task)
	jobName := buildTaskJobName(task)
	execution := resolveExecution(task, agent)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: task.Namespace,
			Labels: map[string]string{
				labels.LabelTask:     labels.SelectorValue(task.Name),
				labels.LabelTaskType: string(task.Spec.Type),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32(0)), // No retries at Job level, we handle retries in the controller
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						labels.LabelTask:     labels.SelectorValue(task.Name),
						labels.LabelTaskType: string(task.Spec.Type),
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           b.workerServiceAccountForTask(task),
					AutomountServiceAccountToken: workerAutomountServiceAccountToken(task),
					SecurityContext:              b.buildPodSecurityContext(),
					Containers: []corev1.Container{
						b.buildContainerWithOptions(ctx, task, agent, provider, opts),
					},
				},
			},
		},
	}

	taskmeta.ApplyTransactionMetadata(&job.ObjectMeta, task.Spec.Transaction)
	taskmeta.ApplyTransactionMetadata(&job.Spec.Template.ObjectMeta, task.Spec.Transaction)

	applyExecution(job, execution)

	// Always add tmp volume for read-only root filesystem
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name:         runtimePoolTempVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: repositoryMonitorValidationEmptyDir(validationTask, repositoryValidationTmpSizeLimit)},
	})

	transactionTokenTask := task
	if validationTask && task != nil && strings.TrimSpace(task.Annotations[labels.AnnotationTransactionTokenSecret]) != "" {
		transactionTokenTask = task.DeepCopy()
		delete(transactionTokenTask.Annotations, labels.AnnotationTransactionTokenSecret)
	}
	b.addTransactionTokenSecret(job, transactionTokenTask)

	// Add workspace/home volumes for tasks that need a git workspace.
	if taskNeedsWorkspace(task) {
		b.addWorkspaceVolumes(job, task, validationTask)
	}

	if taskNeedsWorkspaceInitContainer(task) {
		b.addWorkspaceInitContainer(job, task, validationTask)
	}
	if validationTask {
		applyRepositoryMonitorValidationDefaultTolerations(&job.Spec.Template.Spec)
		if err := b.addRepositoryMonitorValidationCommand(job, task); err != nil {
			return nil, err
		}
		if err := b.addRepositoryMonitorValidationNetworkGate(job, task); err != nil {
			return nil, err
		}
		if err := applyRepositoryMonitorValidationProcessLimit(job, task); err != nil {
			return nil, err
		}
		applyRepositoryMonitorValidationStorageBounds(job)
	}

	// Add skill volumes — read Skill CRs, create ConfigMap, mount at /workspace/.skills/
	if err := b.addSkillVolumes(ctx, job, task, agent); err != nil {
		return nil, fmt.Errorf("failed to add skill volumes: %w", err)
	}

	// Add secret volumes if needed
	if b.needsSecretVolumes(task, agent, provider) {
		b.addSecretVolumes(ctx, job, task, agent, provider)
	}

	// Add session volume if needed
	if task.Spec.SessionRef != nil {
		b.addSessionVolume(job, task)
	}

	// Set active deadline if timeout is specified
	if task.Spec.Timeout != nil {
		seconds := int64(task.Spec.Timeout.Seconds())
		job.Spec.ActiveDeadlineSeconds = &seconds
	}

	return job, nil
}

// buildPodSecurityContext builds a secure pod security context
func (b *JobBuilder) buildPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: new(true),
		RunAsUser:    new(int64(1000)),
		RunAsGroup:   new(int64(1000)),
		FSGroup:      new(int64(1000)),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// buildContainerSecurityContext builds a secure container security context
func (b *JobBuilder) buildContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		ReadOnlyRootFilesystem:   new(true),
		RunAsNonRoot:             new(true),
		RunAsUser:                new(int64(1000)),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

// buildContainerWithOptions builds the main container for the Job.
func (b *JobBuilder) buildContainerWithOptions(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider, opts JobBuildOptions) corev1.Container {
	container := corev1.Container{
		Name:            "worker",
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Resources:       b.buildResources(task, agent),
		Env:             b.buildEnvVarsWithOptions(ctx, task, agent, provider, opts),
		VolumeMounts:    []corev1.VolumeMount{},
	}

	// Set image and command based on task type
	switch task.Spec.Type {
	case corev1alpha1.TaskTypeAI:
		container.Image = b.AIWorkerImage
		container.Command = []string{"/worker"}
		container.Args = []string{"--mode=ai"}
	case corev1alpha1.TaskTypeContainer:
		if task.Spec.Image != "" {
			container.Image = task.Spec.Image
			if effectiveWorkspace(task) != nil {
				container.WorkingDir = workspaceWorkingDir(task)
				if !envVarExists(container.Env, "HOME") {
					container.Env = append(container.Env, corev1.EnvVar{Name: "HOME", Value: "/home/worker"})
				}
			}
			if len(task.Spec.Command) > 0 {
				container.Command = task.Spec.Command
			}
			if len(task.Spec.Args) > 0 {
				container.Args = task.Spec.Args
			}
			if opts.RepositoryMonitorValidation || isRepositoryMonitorValidationTask(task) {
				container.Command = []string{path.Join(repositoryMonitorValidationNetworkSandboxMount, repositoryMonitorValidationNetworkSandboxBinary)}
				container.Args = []string{
					repositoryMonitorValidationSandboxWorkerMode,
					"/bin/sh",
					"-c",
					repositoryMonitorValidationShellWrapper,
					path.Join(repositoryMonitorValidationCommandMount, repositoryMonitorValidationCommandFile),
				}
				container.TerminationMessagePath = "/dev/null"
				container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}
		} else {
			container.Image = b.GeneralWorkerImage
			container.Command = []string{"/worker"}
			// Pass the user command as args to the worker binary
			workerArgs := make([]string, 0, len(task.Spec.Command)+len(task.Spec.Args))
			workerArgs = append(workerArgs, task.Spec.Command...)
			workerArgs = append(workerArgs, task.Spec.Args...)
			container.Args = workerArgs
		}
	}

	// Add tmp volume mount for read-only root filesystem
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      runtimePoolTempVolume,
		MountPath: "/tmp",
	})

	return container
}

func resolveExecution(task *corev1alpha1.Task, agent *corev1alpha1.Agent) *corev1alpha1.ExecutionSpec {
	var effective corev1alpha1.ExecutionSpec

	if agent != nil && agent.Spec.Execution != nil {
		effective.RuntimeClassName = agent.Spec.Execution.RuntimeClassName
		effective.NodeSelector = copyNodeSelector(agent.Spec.Execution.NodeSelector)
		effective.Tolerations = copyTolerations(agent.Spec.Execution.Tolerations)
		if agent.Spec.Execution.Affinity != nil {
			effective.Affinity = agent.Spec.Execution.Affinity.DeepCopy()
		}
	}

	if task != nil && task.Spec.Execution != nil {
		if task.Spec.Execution.RuntimeClassName != "" {
			effective.RuntimeClassName = task.Spec.Execution.RuntimeClassName
		}
		if task.Spec.Execution.NodeSelector != nil {
			effective.NodeSelector = copyNodeSelector(task.Spec.Execution.NodeSelector)
		}
		if task.Spec.Execution.Tolerations != nil {
			effective.Tolerations = copyTolerations(task.Spec.Execution.Tolerations)
		}
		if task.Spec.Execution.Affinity != nil {
			effective.Affinity = task.Spec.Execution.Affinity.DeepCopy()
		}
	}

	if effective.RuntimeClassName == "" && len(effective.NodeSelector) == 0 && len(effective.Tolerations) == 0 && effective.Affinity == nil {
		return nil
	}

	return &effective
}

func applyExecution(job *batchv1.Job, execution *corev1alpha1.ExecutionSpec) {
	if job == nil || execution == nil {
		return
	}

	if execution.RuntimeClassName != "" {
		job.Spec.Template.Spec.RuntimeClassName = new(execution.RuntimeClassName)
	}
	if len(execution.NodeSelector) > 0 {
		job.Spec.Template.Spec.NodeSelector = copyNodeSelector(execution.NodeSelector)
	}
	if len(execution.Tolerations) > 0 {
		job.Spec.Template.Spec.Tolerations = copyTolerations(execution.Tolerations)
	}
	if execution.Affinity != nil {
		job.Spec.Template.Spec.Affinity = execution.Affinity.DeepCopy()
	}
}

func copyNodeSelector(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	return maps.Clone(in)
}

func copyTolerations(in []corev1.Toleration) []corev1.Toleration {
	if in == nil {
		return nil
	}

	out := make([]corev1.Toleration, len(in))
	copy(out, in)

	return out
}

// buildResources builds the resource requirements
func (b *JobBuilder) buildResources(task *corev1alpha1.Task, agent *corev1alpha1.Agent) corev1.ResourceRequirements {
	// Use task resources if specified
	if task.Spec.Resources.Limits != nil || task.Spec.Resources.Requests != nil {
		return task.Spec.Resources
	}

	// Use agent resources if specified
	if agent != nil && (agent.Spec.Resources.Limits != nil || agent.Spec.Resources.Requests != nil) {
		return agent.Spec.Resources
	}

	// Default resources. Memory limit is sized for real Go/Node/Python
	// test suites — 512Mi was too small for `go test ./...` on medium repos
	// and silently OOMKilled workers. Agents/tasks can still override via
	// agent.spec.resources or task.spec.resources (checked above).
	return defaultTaskResourceRequirements()
}

// buildEnvVarsWithOptions builds the environment variables for the container using additional options.
func (b *JobBuilder) buildEnvVarsWithOptions(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider, opts JobBuildOptions) []corev1.EnvVar {
	baseEnv := workerenv.BaseEnv{
		TaskName:       task.Name,
		TaskNamespace:  task.Namespace,
		TaskUID:        string(task.UID),
		ResultEndpoint: fmt.Sprintf("%s/internal/v1/results/%s/%s", b.ControllerURL, task.Namespace, task.Name),
		ControllerURL:  b.ControllerURL,
	}
	if agent != nil {
		baseEnv.AgentName = agent.Name
	}
	envVars := baseEnv.EnvVars()
	if taskRequestsReadOnlyAgent(task) {
		envVars = setControllerEnv(envVars, workerenv.ResultStdout, scheduledRunLabelValue)
	}
	envVars = b.addTelemetryEnvVars(envVars, task)

	// Add task-level env vars. AI worker telemetry env vars are reserved for
	// controller injection so workload authors cannot bypass default-off telemetry
	// policy or redirect GenAI metadata to arbitrary collectors. Restore
	// controller-owned identity and approval env vars so task authors cannot
	// spoof execution identity or approval state.
	envVars = appendTaskEnvVars(envVars, task)
	envVars = setControllerEnv(envVars, workerenv.TaskName, task.Name)
	envVars = setControllerEnv(envVars, workerenv.TaskNamespace, task.Namespace)
	envVars = setControllerEnv(envVars, workerenv.TaskUID, string(task.UID))
	if agent != nil {
		envVars = setControllerEnv(envVars, workerenv.AgentName, agent.Name)
	}
	envVars = setControllerEnv(envVars, workerenv.ResultEndpoint, fmt.Sprintf("%s/internal/v1/results/%s/%s", b.ControllerURL, task.Namespace, task.Name))
	envVars = setControllerEnv(envVars, workerenv.ControllerURL, b.ControllerURL)
	envVars = setControllerEnvValue(envVars, workerenv.AITools, "")
	envVars = setControllerEnvValue(envVars, workerenv.CoordinationEnabled, "")
	envVars = setControllerEnvValue(envVars, workerenv.AutonomousMode, "")
	envVars = setControllerEnvValue(envVars, workerenv.ResolvedApprovals, "")
	envVars = setControllerEnvValue(envVars, workerenv.ApprovalRequiredTools, "")
	envVars = setTransactionCredentialAuthorizationEnv(
		envVars,
		task.Spec.Transaction,
		b.EnforceTransactionCredentialAuth,
	)
	envVars = addTransactionEnvVars(
		envVars,
		task.Spec.Transaction,
		b.TransactionCredentialReadScopes,
	)

	// Add prior task env vars for iterative coordination
	if task.Spec.PriorTaskRef != nil {
		envVars = append(envVars,
			corev1.EnvVar{Name: workerenv.PriorTask, Value: task.Spec.PriorTaskRef.Name},
		)
		priorNS := task.Spec.PriorTaskRef.Namespace
		if priorNS == "" {
			priorNS = task.Namespace
		}
		envVars = append(envVars,
			corev1.EnvVar{Name: workerenv.PriorTaskNamespace, Value: priorNS},
		)
	}

	// Add parent task env var for inter-agent messaging
	if parentTask := labels.ParentTaskName(task.Labels, task.Annotations); parentTask != "" {
		envVars = append(envVars,
			corev1.EnvVar{Name: workerenv.ParentTask, Value: parentTask},
		)
	}

	// Add AI-specific env vars
	if task.Spec.Type == corev1alpha1.TaskTypeAI {
		envVars = b.addAIEnvVars(ctx, envVars, task, agent, provider)
	}

	if task.Spec.Type == corev1alpha1.TaskTypeContainer {
		envVars = b.addWorkspaceEnvVars(envVars, task)
	}
	envVars = setControllerEnvValue(envVars, workerenv.ResolvedApprovals, opts.ResolvedApprovalsJSON)
	if taskRequestsReadOnlyAgent(task) {
		envVars = setControllerEnv(envVars, workerenv.AgentReadOnly, scheduledRunLabelValue)
		envVars = setControllerEnv(envVars, workerenv.ResultStdout, scheduledRunLabelValue)
	}

	return envVars
}

func (b *JobBuilder) addTelemetryEnvVars(envVars []corev1.EnvVar, task *corev1alpha1.Task) []corev1.EnvVar {
	if task != nil && task.Annotations != nil {
		envVars = setControllerEnv(envVars, workerenv.TraceParent, task.Annotations[labels.AnnotationTraceParent])
		envVars = setControllerEnv(envVars, workerenv.TraceState, task.Annotations[labels.AnnotationTraceState])
	}
	if !b.EnableTelemetry || task == nil || task.Spec.Type != corev1alpha1.TaskTypeAI {
		return envVars
	}
	if !workerReachableOTLPEndpointConfigured(os.Getenv) {
		return envVars
	}
	envVars = setControllerEnv(envVars, workerenv.EnableTelemetry, scheduledRunLabelValue)
	unreachableSignalOverrides := unreachableWorkerOTLPSignalEndpoints(os.Getenv)
	// Copy only non-secret scalar OTLP settings. Header env vars carry
	// credentials, and certificate env vars are file paths whose source files are
	// not mounted into worker Pods by the controller.
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
		"OTEL_EXPORTER_OTLP_METRICS_INSECURE",
		"OTEL_EXPORTER_OTLP_TIMEOUT",
		"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
		"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
		"OTEL_EXPORTER_OTLP_COMPRESSION",
		"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
		"OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
	} {
		if signal := otlpSignalFromEnvName(name); signal != "" && unreachableSignalOverrides[signal] {
			continue
		}
		value := os.Getenv(name)
		if strings.HasSuffix(name, "_ENDPOINT") && !isWorkerReachableOTLPEndpoint(value) {
			value = ""
		}
		envVars = setControllerEnv(envVars, name, safeWorkerOTLPEnvValue(name, value))
	}
	return envVars
}

func unreachableWorkerOTLPSignalEndpoints(getenv func(string) string) map[string]bool {
	out := map[string]bool{}
	for _, signal := range []string{"TRACES", "METRICS"} {
		name := "OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT"
		if strings.TrimSpace(getenv(name)) != "" && !isWorkerReachableOTLPEndpoint(getenv(name)) {
			out[signal] = true
		}
	}
	return out
}

func otlpSignalFromEnvName(name string) string {
	for _, signal := range []string{"TRACES", "METRICS"} {
		if strings.HasPrefix(name, "OTEL_EXPORTER_OTLP_"+signal+"_") {
			return signal
		}
	}
	return ""
}

func appendTaskEnvVars(envVars []corev1.EnvVar, task *corev1alpha1.Task) []corev1.EnvVar {
	if task == nil {
		return envVars
	}
	for _, envVar := range task.Spec.Env {
		if isReservedTaskTelemetryEnv(task, envVar.Name) {
			continue
		}
		envVars = append(envVars, envVar)
	}
	return envVars
}

func isReservedTaskTelemetryEnv(task *corev1alpha1.Task, name string) bool {
	if task == nil {
		return false
	}
	switch task.Spec.Type {
	case corev1alpha1.TaskTypeAI:
		return isReservedAIWorkerTelemetryEnv(name)
	default:
		return false
	}
}

func isReservedAIWorkerTelemetryEnv(name string) bool {
	if isReservedTraceContextEnv(name) {
		return true
	}
	switch name {
	case workerenv.EnableTelemetry, "OTEL_RESOURCE_ATTRIBUTES":
		return true
	default:
		return strings.HasPrefix(name, "OTEL_EXPORTER_OTLP")
	}
}

func isReservedTraceContextEnv(name string) bool {
	switch name {
	case workerenv.TraceParent, workerenv.TraceState, workerenv.TraceBaggage:
		return true
	default:
		return false
	}
}

func addTransactionEnvVars(
	envVars []corev1.EnvVar,
	tx *corev1alpha1.TaskTransaction,
	credentialReadScopes []string,
) []corev1.EnvVar {
	if tx == nil {
		return envVars
	}
	envVars = setControllerEnv(envVars, workerenv.TransactionID, tx.ID)
	envVars = setControllerEnv(envVars, workerenv.TransactionProfile, tx.Profile)
	envVars = setControllerEnv(envVars, workerenv.TransactionIssuer, tx.Issuer)
	envVars = setControllerEnv(envVars, workerenv.TransactionSubject, tx.Subject)
	envVars = setControllerEnv(envVars, workerenv.TransactionRequestingWorkload, tx.RequestingWorkload)
	envVars = setControllerEnv(envVars, workerenv.TransactionScope, tx.Scope)
	envVars = setControllerEnv(envVars, workerenv.TransactionScopes, workerenv.JoinCSV(tx.Scopes))
	envVars = setControllerEnv(envVars, workerenv.TransactionContextDigest, tx.ContextDigest)
	envVars = setControllerEnv(envVars, workerenv.TransactionRequesterContextDigest, tx.RequesterContextDigest)
	envVars = setControllerEnv(envVars, workerenv.TransactionCredentialSecret, tx.Context["secret"])
	envVars = setControllerEnv(
		envVars,
		workerenv.TransactionCredentialReadScopes,
		workerenv.JoinCSV(credentialReadScopes),
	)
	return envVars
}

func setTransactionCredentialAuthorizationEnv(
	envVars []corev1.EnvVar,
	tx *corev1alpha1.TaskTransaction,
	enforced bool,
) []corev1.EnvVar {
	return setControllerEnvValue(
		envVars,
		workerenv.TransactionCredentialAuthorizationEnforced,
		strconv.FormatBool(tx != nil && enforced),
	)
}

// aiConfig holds resolved AI configuration from provider, agent, and task.
type aiConfig struct {
	providerType    string
	model           string
	prompt          string
	systemPrompt    string
	baseURL         string
	azureAPIVersion string
	tools           []string
}

// resolveAIConfig merges AI configuration from provider, agent, and task (in priority order).
func resolveAIConfig(task *corev1alpha1.Task, agent *corev1alpha1.Agent, providerCRD *corev1alpha1.Provider) aiConfig {
	var cfg aiConfig

	// Get values from Provider CRD if present (lowest priority - defaults)
	if providerCRD != nil {
		cfg.providerType = string(providerCRD.Spec.Type)
		cfg.model = providerCRD.Spec.DefaultModel
		cfg.baseURL = providerCRD.Spec.BaseURL
		if providerCRD.Spec.Azure != nil {
			cfg.azureAPIVersion = providerCRD.Spec.Azure.APIVersion
		}
	}

	// Get values from agent if present (overrides provider defaults)
	if agent != nil {
		if agent.Spec.Model != nil {
			if agent.Spec.Model.Provider != "" {
				cfg.providerType = agent.Spec.Model.Provider
			}
			if agent.Spec.Model.Name != "" {
				cfg.model = agent.Spec.Model.Name
			}
		}
		if agent.Spec.SystemPrompt != nil {
			cfg.systemPrompt = agent.Spec.SystemPrompt.Inline
		}
		for _, t := range agent.Spec.Tools {
			if t.Enabled == nil || *t.Enabled {
				cfg.tools = append(cfg.tools, t.Name)
			}
		}
	}

	// Override with task values if present (highest priority)
	if task.Spec.AI != nil {
		if task.Spec.AI.Provider != "" {
			cfg.providerType = task.Spec.AI.Provider
		}
		if task.Spec.AI.Model != "" {
			cfg.model = task.Spec.AI.Model
		}
		if task.Spec.AI.Prompt != "" {
			cfg.prompt = task.Spec.AI.Prompt
		}
		if task.Spec.AI.SystemPrompt != "" {
			cfg.systemPrompt = task.Spec.AI.SystemPrompt
		}
		if len(task.Spec.AI.Tools) > 0 {
			cfg.tools = append(cfg.tools, task.Spec.AI.Tools...)
		}
	}

	// Check task.Spec.Prompt (used with agentRef pattern)
	if cfg.prompt == "" && task.Spec.Prompt != "" {
		cfg.prompt = task.Spec.Prompt
	}

	// Provider CRD type is authoritative when resolved via providerRef.
	// model.provider and task AI provider are hints for when no Provider CRD exists.
	if providerCRD != nil {
		cfg.providerType = string(providerCRD.Spec.Type)
	}

	return cfg
}

// addCoordinationEnvVars appends coordination-related environment variables.
func (b *JobBuilder) addCoordinationEnvVars(envVars []corev1.EnvVar, task *corev1alpha1.Task, agent *corev1alpha1.Agent) []corev1.EnvVar {
	agentNames := make([]string, 0, len(agent.Spec.Coordination.AllowedAgents))
	for _, a := range agent.Spec.Coordination.AllowedAgents {
		agentNames = append(agentNames, a.Name)
	}

	// Current depth (0 for top-level coordinator)
	depth := "0"
	if d, ok := task.Annotations[labels.AnnotationCoordinationDepth]; ok {
		depth = d
	}

	for _, envVar := range (workerenv.CoordinationEnv{
		Enabled:                 true,
		MaxDepth:                int(agent.Spec.Coordination.MaxDepth),
		MaxChildren:             int(agent.Spec.Coordination.MaxConcurrentChildren),
		AllowedAgents:           agentNames,
		Depth:                   depth,
		AutonomousMode:          agent.Spec.Coordination.Autonomous,
		AutonomousIteration:     int(task.Status.Iteration),
		AutonomousMaxIterations: int(agent.Spec.Coordination.MaxIterations),
	}).EnvVars() {
		envVars = setControllerEnvValue(envVars, envVar.Name, envVar.Value)
	}
	return setControllerEnvValue(envVars, workerenv.ApprovalRequiredTools, workerenv.JoinCSV(agent.Spec.Coordination.ApprovalRequiredTools))
}

// addAIEnvVars adds AI-specific environment variables
func (b *JobBuilder) addAIEnvVars(ctx context.Context, //nolint:gocyclo
	envVars []corev1.EnvVar, task *corev1alpha1.Task, agent *corev1alpha1.Agent, providerCRD *corev1alpha1.Provider) []corev1.EnvVar {
	cfg := resolveAIConfig(task, agent, providerCRD)

	// Resolve system prompt from ConfigMapRef if not already set inline
	if cfg.systemPrompt == "" && agent != nil && agent.Spec.SystemPrompt != nil && agent.Spec.SystemPrompt.ConfigMapRef != nil {
		cfg.systemPrompt = b.resolveConfigMapValue(ctx, agent.Namespace, agent.Spec.SystemPrompt.ConfigMapRef)
	}

	envVars = append(envVars, workerenv.AIWorkerEnv{
		Provider:        cfg.providerType,
		Model:           cfg.model,
		Prompt:          cfg.prompt,
		SystemPrompt:    cfg.systemPrompt,
		BaseURL:         cfg.baseURL,
		AzureAPIVersion: cfg.azureAPIVersion,
		ControllerMode:  string(b.ControllerMode),
	}.EnvVars()...)

	disableCoordinationToolInjection := task.Annotations[labels.AnnotationDisableCoordinationToolInject] == scheduledRunLabelValue

	// Auto-inject coordination tools when coordination is enabled, unless the
	// task deliberately supplies a narrower explicit tool set.
	if agent != nil && agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled && !disableCoordinationToolInjection {
		for _, ct := range []string{
			"delegate_task",
			"wait_for_tasks",
			"create_container_task",
			"cancel_task",
			"send_message",
			"check_messages",
			"recall_memory",
			"remember",
			"propose_memory",
			"search_transcript",
			"create_pull_request",
			"list_pull_requests",
			"check_pr_review_marker",
			"check_pull_request_ci",
			"merge_pull_request",
			"auto_merge_pull_request",
			"review_pull_request",
			"post_review_comment",
			"create_agent",
			"delete_agent",
			"update_plan",
		} {
			if !slices.Contains(cfg.tools, ct) {
				cfg.tools = append(cfg.tools, ct)
			}
		}
		if agent.Spec.Coordination.Autonomous && !slices.Contains(cfg.tools, "request_approval") {
			cfg.tools = append(cfg.tools, "request_approval")
		}
	}

	// Auto-inject messaging tools for child tasks (tasks delegated by a coordinator)
	// so they can communicate with sibling tasks via send_message/check_messages
	_, isChildTask := task.Labels[labels.LabelParentTask]
	if isChildTask && !disableCoordinationToolInjection {
		for _, ct := range []string{"send_message", "check_messages"} {
			if !slices.Contains(cfg.tools, ct) {
				cfg.tools = append(cfg.tools, ct)
			}
		}
	}

	if len(cfg.tools) > 0 {
		envVars = setControllerEnvValue(envVars, workerenv.AITools, strings.Join(cfg.tools, ","))
	}

	if agent != nil && agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled {
		envVars = b.addCoordinationEnvVars(envVars, task, agent)
	}

	// Enable coordination in worker for child tasks so messaging tools are registered
	if isChildTask && (agent == nil || agent.Spec.Coordination == nil || !agent.Spec.Coordination.Enabled) {
		envVars = append(envVars, corev1.EnvVar{Name: workerenv.CoordinationEnabled, Value: scheduledRunLabelValue})
	}

	// Add fallback provider environment variables
	if agent != nil && agent.Spec.Model != nil && len(agent.Spec.Model.Fallbacks) > 0 {
		envVars = append(envVars, corev1.EnvVar{
			Name:  workerenv.AIFallbackCount,
			Value: fmt.Sprintf("%d", len(agent.Spec.Model.Fallbacks)),
		})
		for i, fb := range agent.Spec.Model.Fallbacks {
			// Resolve the fallback Provider CRD
			fbProvider := &corev1alpha1.Provider{}
			if err := b.Get(ctx, client.ObjectKey{
				Namespace: task.Namespace,
				Name:      fb.ProviderRef,
			}, fbProvider); err != nil {
				continue // skip unresolvable fallbacks
			}
			fallbackEnv := workerenv.FallbackProviderEnv{
				Provider: string(fbProvider.Spec.Type),
				Model:    fb.Model,
				BaseURL:  fbProvider.Spec.BaseURL,
			}
			if fbProvider.Spec.Azure != nil {
				fallbackEnv.AzureAPIVersion = fbProvider.Spec.Azure.APIVersion
			}
			envVars = append(envVars, fallbackEnv.EnvVars(i)...)

		}
	}

	// AllowBash: task override > agent default > true
	allowBash := true
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.DefaultAllowBash != nil {
		allowBash = *agent.Spec.Runtime.DefaultAllowBash
	}
	if task.Spec.AgentRuntime != nil && task.Spec.AgentRuntime.AllowBash != nil {
		allowBash = *task.Spec.AgentRuntime.AllowBash
	}
	if allowBash {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.AllowBash, Value: scheduledRunLabelValue,
		})
	}

	return envVars
}

func (b *JobBuilder) addTransactionTokenSecret(job *batchv1.Job, task *corev1alpha1.Task) {
	if job == nil || len(job.Spec.Template.Spec.Containers) == 0 {
		return
	}
	secretName := ""
	if task != nil && task.Annotations != nil {
		secretName = strings.TrimSpace(task.Annotations[labels.AnnotationTransactionTokenSecret])
	}
	injectTTS := b.shouldInjectContextTokenTTS(task, secretName)
	for i := range job.Spec.Template.Spec.Containers {
		container := &job.Spec.Template.Spec.Containers[i]
		container.Env = setControllerEnv(container.Env, workerenv.OutboundAccessTrustedGatewayServices, b.OutboundAccessTrustedGatewayServices)
		container.Env = setControllerEnv(container.Env, workerenv.OutboundAccessTrustedTokenEndpointServices, b.OutboundAccessTrustedTokenEndpointServices)
		if injectTTS {
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenTTSEndpoint, b.ContextTokenTTSEndpoint)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenTTSAudience, b.ContextTokenTTSAudience)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenTTSTimeout, b.ContextTokenTTSTimeout)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenTTSTokenSource, b.ContextTokenTTSTokenSource)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenSubjectTokenType, b.ContextTokenSubjectTokenType)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenChildScope, b.ContextTokenChildScope)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenOutboundScope, b.ContextTokenOutboundScope)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenChildTokenTTL, b.ContextTokenChildTokenTTL)
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenToolTokenTTL, b.ContextTokenToolTokenTTL)
		} else {
			for _, name := range contextTokenTTSEnvNames() {
				container.Env = removeControllerEnv(container.Env, name)
			}
		}
		container.Env = removeControllerEnv(container.Env, workerenv.TransactionTokenFile)
		container.Env = removeControllerEnv(container.Env, workerenv.ContextTokenSubjectTokenFile)
	}

	if secretName == "" {
		return
	}
	const (
		volumeName = "transaction-token"
		mountPath  = "/var/run/orka/transaction-token"
		tokenPath  = mountPath + "/token"
	)
	defaultMode := int32(0400)
	// The child transaction token is delegation authority. Add the Secret as a
	// pod volume and expose the mounted token-file path to every workload
	// container in the pod so secondary containers can make TTS-mediated calls.
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  secretName,
				DefaultMode: &defaultMode,
				Items: []corev1.KeyToPath{{
					Key:  "token",
					Path: "token",
				}},
			},
		},
	})
	for i := range job.Spec.Template.Spec.Containers {
		container := &job.Spec.Template.Spec.Containers[i]
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
		container.Env = setControllerEnv(container.Env, workerenv.TransactionTokenFile, tokenPath)
		if b.ContextTokenTTSTokenSource == contexttoken.TTSTokenSourceIncoming {
			container.Env = setControllerEnv(container.Env, workerenv.ContextTokenSubjectTokenFile, tokenPath)
		} else {
			container.Env = removeControllerEnv(container.Env, workerenv.ContextTokenSubjectTokenFile)
		}
	}
}

func (b *JobBuilder) shouldInjectContextTokenTTS(task *corev1alpha1.Task, secretName string) bool {
	if b.ContextTokenTTSEndpoint == "" {
		return false
	}
	if secretName != "" {
		return true
	}
	if task == nil || task.Spec.Transaction == nil {
		return false
	}
	return b.ContextTokenTTSTokenSource != contexttoken.TTSTokenSourceIncoming
}

func contextTokenTTSEnvNames() []string {
	return []string{
		workerenv.ContextTokenTTSEndpoint,
		workerenv.ContextTokenTTSAudience,
		workerenv.ContextTokenTTSTimeout,
		workerenv.ContextTokenTTSTokenSource,
		workerenv.ContextTokenSubjectTokenType,
		workerenv.ContextTokenChildScope,
		workerenv.ContextTokenOutboundScope,
		workerenv.ContextTokenChildTokenTTL,
		workerenv.ContextTokenToolTokenTTL,
	}
}

// addSecretVolumes adds secret volumes to the Job
func (b *JobBuilder) addSecretVolumes(ctx context.Context, job *batchv1.Job, task *corev1alpha1.Task, agent *corev1alpha1.Agent, provider *corev1alpha1.Provider) {
	allowDirectProviderSecrets := b.directProviderSecretsAllowed(task)
	allowDirectSecretMounts := b.directSecretMountsAllowed(task)

	// Add provider secret (mounted as environment variable source)
	if allowDirectProviderSecrets && provider != nil {
		secretName := provider.Spec.SecretRef.Name
		secretKey := provider.Spec.SecretRef.Key
		if secretKey == "" {
			secretKey = defaultSecretKey
		}

		// Determine the env var name based on provider type
		envVarName := workerenv.AnthropicAPIKey
		if provider.Spec.Type == corev1alpha1.ProviderTypeOpenAI || provider.Spec.Type == corev1alpha1.ProviderTypeAzureOpenAI {
			envVarName = workerenv.OpenAIAPIKey
		}

		// Add API key as environment variable from secret
		job.Spec.Template.Spec.Containers[0].Env = append(
			job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{
				Name: envVarName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: secretName,
						},
						Key: secretKey,
					},
				},
			},
		)

		// Set base URL so agent CLIs route through the provider's upstream
		if provider.Spec.BaseURL != "" {
			baseURLEnvVar := workerenv.AnthropicBaseURL
			if provider.Spec.Type == corev1alpha1.ProviderTypeOpenAI || provider.Spec.Type == corev1alpha1.ProviderTypeAzureOpenAI {
				baseURLEnvVar = workerenv.OpenAIBaseURL
			}
			job.Spec.Template.Spec.Containers[0].Env = append(
				job.Spec.Template.Spec.Containers[0].Env,
				corev1.EnvVar{Name: baseURLEnvVar, Value: provider.Spec.BaseURL},
			)
		}
	}

	// Add fallback provider secrets
	if allowDirectProviderSecrets && agentHasFallbackProviders(agent) {
		for i, fb := range agent.Spec.Model.Fallbacks {
			fbProvider := &corev1alpha1.Provider{}
			if err := b.Get(ctx, client.ObjectKey{
				Namespace: task.Namespace,
				Name:      fb.ProviderRef,
			}, fbProvider); err != nil {
				continue
			}
			secretName := fbProvider.Spec.SecretRef.Name
			secretKey := fbProvider.Spec.SecretRef.Key
			if secretKey == "" {
				secretKey = defaultSecretKey
			}
			envVarName := workerenv.FallbackAPIKeyKey(i)
			job.Spec.Template.Spec.Containers[0].Env = append(
				job.Spec.Template.Spec.Containers[0].Env,
				corev1.EnvVar{
					Name: envVarName,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: secretName,
							},
							Key: secretKey,
						},
					},
				},
			)
		}
	}

	// Add task secret
	if allowDirectSecretMounts && !taskRequestsRuntimeAuthOnly(task) && task.Spec.SecretRef != nil {
		secretName := task.Spec.SecretRef.Name
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: "task-secrets",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
				},
			},
		})
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      "task-secrets",
				MountPath: "/secrets/task",
				ReadOnly:  true,
			},
		)
	}

	// Add agent secret
	if allowDirectSecretMounts && !taskRequestsRuntimeAuthOnly(task) && agent != nil && agent.Spec.SecretRef != nil {
		secretName := agent.Spec.SecretRef.Name
		// Inject all secret keys as environment variables
		job.Spec.Template.Spec.Containers[0].EnvFrom = append(
			job.Spec.Template.Spec.Containers[0].EnvFrom,
			corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				},
			},
		)
		if task.Spec.Type == corev1alpha1.TaskTypeAI {
			job.Spec.Template.Spec.Containers[0].Env = reserveAIWorkerTelemetryEnvFromKeys(job.Spec.Template.Spec.Containers[0].Env)
		}
		// Also mount as files for tools that read from filesystem
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: "agent-secrets",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
				},
			},
		})
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      "agent-secrets",
				MountPath: "/secrets/agent",
				ReadOnly:  true,
			},
		)
	}
}

func reserveAIWorkerTelemetryEnvFromKeys(envVars []corev1.EnvVar) []corev1.EnvVar {
	for _, name := range reservedAIWorkerTelemetryEnvNames() {
		if !envVarExists(envVars, name) {
			envVars = append(envVars, corev1.EnvVar{Name: name})
		}
	}
	return envVars
}

func reservedAIWorkerTelemetryEnvNames() []string {
	return []string{
		workerenv.EnableTelemetry,
		workerenv.TraceParent,
		workerenv.TraceState,
		workerenv.TraceBaggage,
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
		"OTEL_EXPORTER_OTLP_METRICS_INSECURE",
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_METRICS_HEADERS",
		"OTEL_EXPORTER_OTLP_TIMEOUT",
		"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
		"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
		"OTEL_EXPORTER_OTLP_COMPRESSION",
		"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
		"OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
		"OTEL_EXPORTER_OTLP_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY",
		"OTEL_RESOURCE_ATTRIBUTES",
	}
}

func scopedAgentRuntimeSecretCoordinates(task *corev1alpha1.Task, agent *corev1alpha1.Agent) (namespace, name string, err error) {
	if task == nil {
		return "", "", nil
	}
	if repositoryMonitorTaskUsesPinnedRuntimeAuth(task) {
		namespace = strings.TrimSpace(task.Spec.SecretRef.Namespace)
		if namespace == "" {
			namespace = task.Namespace
		}
		if namespace != task.Namespace {
			return "", "", fmt.Errorf("runtime-auth-only task secretRef namespace %q does not match task namespace %q", namespace, task.Namespace)
		}
		return namespace, strings.TrimSpace(task.Spec.SecretRef.Name), nil
	}
	if agent == nil || agent.Spec.SecretRef == nil || strings.TrimSpace(agent.Spec.SecretRef.Name) == "" {
		return "", "", nil
	}
	return task.Namespace, strings.TrimSpace(agent.Spec.SecretRef.Name), nil
}

func repositoryMonitorTaskUsesPinnedRuntimeAuth(task *corev1alpha1.Task) bool {
	return task != nil && taskRequestsRuntimeAuthOnly(task) && task.Spec.SecretRef != nil &&
		strings.TrimSpace(task.Spec.SecretRef.Name) != "" &&
		strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationActionKind]) == repositoryMonitorIssueActionImplementation &&
		strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationRuntimeAgentGeneration]) != "" &&
		strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationRuntimeAuthFields]) != ""
}

func validateScopedRuntimeSecretBinding(task *corev1alpha1.Task, secret *corev1.Secret) error {
	if task == nil || !taskRequestsRuntimeAuthOnly(task) {
		return nil
	}
	expectedUID := strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationRuntimeAuthUID])
	pinnedFields := strings.TrimSpace(task.Annotations[repositoryMonitorIssueAnnotationRuntimeAuthFields])
	if expectedUID == "" && pinnedFields == "" {
		return nil
	}
	if secret == nil {
		return fmt.Errorf("%w: runtime auth snapshot is missing", errRepositoryMonitorRuntimeAuthBindingInvalid)
	}
	if expectedUID != "" && string(secret.UID) != expectedUID {
		return fmt.Errorf("%w: runtime credential Secret UID changed", errRepositoryMonitorRuntimeAuthBindingInvalid)
	}
	if secret.Immutable == nil || !*secret.Immutable {
		return fmt.Errorf("%w: runtime credential Secret is not immutable", errRepositoryMonitorRuntimeAuthBindingInvalid)
	}
	return nil
}

func repositoryMonitorPinnedRuntimeAuthFields(task *corev1alpha1.Task) []string {
	if task == nil {
		return nil
	}
	var keys []string
	for key := range strings.SplitSeq(task.Annotations[repositoryMonitorIssueAnnotationRuntimeAuthFields], ",") {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func scopedAgentRuntimeSecretKeys(agent *corev1alpha1.Agent) (keys, credentialKeys []string, err error) {
	if agent != nil && agent.Spec.Runtime != nil && agent.Spec.Runtime.RuntimeRef != nil && strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name) != "" {
		return nil, nil, fmt.Errorf("scoped agent runtime credentials do not support external runtimeRef %q", agent.Spec.Runtime.RuntimeRef.Name)
	}
	switch readOnlyAgentRuntimeType(agent) {
	case corev1alpha1.AgentRuntimeCodex:
		return []string{workerenv.OpenAIAPIKey, workerenv.CodexAPIKey, workerenv.OpenAIBaseURL}, []string{workerenv.OpenAIAPIKey, workerenv.CodexAPIKey}, nil
	case corev1alpha1.AgentRuntimeClaude:
		return []string{workerenv.AnthropicAPIKey, workerenv.AnthropicBaseURL}, []string{workerenv.AnthropicAPIKey}, nil
	case corev1alpha1.AgentRuntimeCopilot:
		return nil, nil, fmt.Errorf("scoped agent runtime credentials do not support copilot because %s can mutate GitHub", workerenv.GitHubToken)
	default:
		return nil, nil, fmt.Errorf("scoped agent runtime credentials do not support runtime %q", readOnlyAgentRuntimeType(agent))
	}
}

func readOnlyAgentRuntimeSecretKeys(agent *corev1alpha1.Agent) ([]string, error) {
	switch readOnlyAgentRuntimeType(agent) {
	case corev1alpha1.AgentRuntimeCodex:
		return []string{workerenv.OpenAIAPIKey, workerenv.CodexAPIKey, workerenv.OpenAIBaseURL}, nil
	case corev1alpha1.AgentRuntimeClaude:
		return []string{
			workerenv.AnthropicAPIKey,
			workerenv.AnthropicBaseURL,
			"CLAUDE_CODE_USE_FOUNDRY",
			"ANTHROPIC_FOUNDRY_API_KEY",
			workerenv.AnthropicFoundryBaseURL,
			"ANTHROPIC_FOUNDRY_RESOURCE",
			"ANTHROPIC_DEFAULT_SONNET_MODEL",
			"ANTHROPIC_DEFAULT_HAIKU_MODEL",
			"ANTHROPIC_DEFAULT_OPUS_MODEL",
		}, nil
	case corev1alpha1.AgentRuntimeCopilot:
		return nil, fmt.Errorf("read-only agent tasks do not support copilot runtime credentials because GITHUB_TOKEN can mutate GitHub")
	case corev1alpha1.AgentRuntimeOpencode:
		return nil, nil
	default:
		return nil, nil
	}
}

func readOnlyAgentRuntimeType(agent *corev1alpha1.Agent) corev1alpha1.AgentRuntimeType {
	if agent == nil || agent.Spec.Runtime == nil {
		return corev1alpha1.AgentRuntimeClaude
	}
	switch agent.Spec.Runtime.Type {
	case corev1alpha1.AgentRuntimeCodex, corev1alpha1.AgentRuntimeClaude, corev1alpha1.AgentRuntimeCopilot,
		corev1alpha1.AgentRuntimeOpencode:
		return agent.Spec.Runtime.Type
	default:
		return corev1alpha1.AgentRuntimeClaude
	}
}

// addSessionVolume adds a session emptyDir volume and init container to the Job.
// The init container fetches the session transcript from the controller via HTTP
// and writes it to /session/transcript.jsonl for the main worker container.
func (b *JobBuilder) addSessionVolume(job *batchv1.Job, task *corev1alpha1.Task) {
	sessionName := task.Spec.SessionRef.Name

	// Add shared emptyDir volume for session data
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "session-data",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	// Mount in the main worker container
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      "session-data",
			MountPath: "/session",
			ReadOnly:  true,
		},
	)

	// Build the transcript fetch URL
	transcriptURL := fmt.Sprintf("%s/internal/v1/sessions/%s/%s/transcript?taskName=%s",
		b.ControllerURL, url.PathEscape(task.Namespace), url.PathEscape(sessionName), url.QueryEscape(task.Name))

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "session-data",
			MountPath: "/session",
		},
	}
	// Always project a short-lived token exclusively into the trusted init container.
	// This keeps transcript loading available even when the main pod disables automount.
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "session-token",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{
						ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
							Path:              "token",
							ExpirationSeconds: new(int64(3600)),
						},
					},
				},
			},
		},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "session-token",
		MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
		ReadOnly:  true,
	})

	// Add init container that fetches the transcript via HTTP
	initContainer := corev1.Container{
		Name:            "fetch-session",
		Image:           b.InitImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Command:         []string{"sh", "-c", sessionTranscriptFetchCommand()},
		Env: []corev1.EnvVar{
			{Name: sessionTranscriptURLEnv, Value: transcriptURL},
			{Name: sessionTranscriptRequiredEnv, Value: strconv.FormatBool(task.Spec.SessionRef.PromptIncluded)},
			{Name: sessionTranscriptMaxAttemptsEnv, Value: sessionTranscriptMaxAttempts(task.Spec.SessionRef.PromptIncluded, task.Spec.Timeout)},
		},
		VolumeMounts: volumeMounts,
	}

	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, initContainer)

	// Add session env vars
	job.Spec.Template.Spec.Containers[0].Env = append(
		job.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: workerenv.SessionName, Value: sessionName},
		corev1.EnvVar{Name: workerenv.SessionPromptIncluded, Value: strconv.FormatBool(task.Spec.SessionRef.PromptIncluded)},
	)
}

func sessionTranscriptMaxAttempts(promptIncluded bool, timeout *metav1.Duration) string {
	attempts := 5
	if promptIncluded {
		// Required transcripts must keep retrying through the Task startup window.
		// Worker authorization intentionally waits for controller-persisted JobName;
		// extending this retry budget preserves that security boundary without a
		// pre-status identity fallback.
		attempts = 300
	}
	if timeout != nil && timeout.Duration > 0 {
		deadlineBudget := max(1, int(timeout.Duration/time.Second)-1)
		if promptIncluded {
			attempts = deadlineBudget
		} else {
			attempts = min(attempts, deadlineBudget)
		}
	}
	return strconv.Itoa(attempts)
}

func sessionTranscriptFetchCommand() string {
	return `set -eu
TOKEN_FILE=/var/run/secrets/kubernetes.io/serviceaccount/token
TMP=/session/transcript.jsonl.tmp
FINAL=/session/transcript.jsonl
: "${ORKA_SESSION_TRANSCRIPT_URL:?}"
: "${ORKA_SESSION_TRANSCRIPT_REQUIRED:?}"
: "${ORKA_SESSION_TRANSCRIPT_MAX_ATTEMPTS:?}"
rm -f "$TMP"
attempt=0
while true; do
  SA_JWT=$(cat "$TOKEN_FILE")
  if wget --header="Authorization: Bearer $SA_JWT" -q -O "$TMP" "$ORKA_SESSION_TRANSCRIPT_URL"; then
    mv "$TMP" "$FINAL"
    exit 0
  fi
  rm -f "$TMP"
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$ORKA_SESSION_TRANSCRIPT_MAX_ATTEMPTS" ]; then
    if [ "$ORKA_SESSION_TRANSCRIPT_REQUIRED" = "true" ]; then
      exit 1
    fi
    : > "$TMP"
    mv "$TMP" "$FINAL"
    exit 0
  fi
  sleep 1
done`
}

func workerReachableOTLPEndpointConfigured(getenv func(string) string) bool {
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		if isWorkerReachableOTLPEndpoint(getenv(name)) {
			return true
		}
	}
	return false
}

func isWorkerReachableOTLPEndpoint(value string) bool {
	host := otlpEndpointHost(value)
	if host == "" {
		return false
	}
	if host == "localhost" {
		return false
	}
	hostWithoutZone, _, _ := strings.Cut(host, "%")
	if ip, err := netip.ParseAddr(hostWithoutZone); err == nil {
		ip = ip.Unmap()
		return !ip.IsLoopback() && !ip.IsUnspecified()
	}
	return true
}

func otlpEndpointHost(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parseValue := value
	if !strings.Contains(value, "://") {
		parseValue = "//" + value
	}
	parsed, err := url.Parse(parseValue)
	if err != nil {
		return ""
	}
	host := parsed.Hostname()
	return strings.ToLower(strings.Trim(host, "[]"))
}

func safeWorkerOTLPEnvValue(name, value string) string {
	if !strings.HasSuffix(name, "_ENDPOINT") {
		return value
	}
	value = strings.TrimSpace(value)
	parseValue := value
	schemeLess := !strings.Contains(value, "://")
	if schemeLess {
		parseValue = "//" + value
	}
	parsed, err := url.Parse(parseValue)
	if err != nil {
		return value
	}
	if parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" {
		return value
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	sanitized := parsed.String()
	if schemeLess {
		return strings.TrimPrefix(sanitized, "//")
	}
	return sanitized
}

func setControllerEnvValue(envVars []corev1.EnvVar, name, value string) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(envVars)+1)
	set := false
	for _, envVar := range envVars {
		if envVar.Name != name {
			out = append(out, envVar)
			continue
		}
		if !set {
			out = append(out, corev1.EnvVar{Name: name, Value: value})
			set = true
		}
	}
	if !set {
		out = append(out, corev1.EnvVar{Name: name, Value: value})
	}
	return out
}

func setControllerEnv(envVars []corev1.EnvVar, name, value string) []corev1.EnvVar {
	if value == "" {
		return removeControllerEnv(envVars, name)
	}
	out := make([]corev1.EnvVar, 0, len(envVars)+1)
	set := false
	for _, envVar := range envVars {
		if envVar.Name != name {
			out = append(out, envVar)
			continue
		}
		if !set {
			out = append(out, corev1.EnvVar{Name: name, Value: value})
			set = true
		}
	}
	if !set {
		out = append(out, corev1.EnvVar{Name: name, Value: value})
	}
	return out
}

func removeControllerEnv(envVars []corev1.EnvVar, name string) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(envVars))
	for _, envVar := range envVars {
		if envVar.Name != name {
			out = append(out, envVar)
		}
	}
	return out
}

func envVarExists(envVars []corev1.EnvVar, name string) bool {
	for _, envVar := range envVars {
		if envVar.Name == name {
			return true
		}
	}
	return false
}

// resolveConfigMapValue reads a value from a ConfigMap key.
func (b *JobBuilder) resolveConfigMapValue(ctx context.Context, namespace string, ref *corev1alpha1.ConfigMapKeySelector) string {
	cm := &corev1.ConfigMap{}
	if err := b.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: namespace}, cm); err != nil {
		log.FromContext(ctx).Error(err, "failed to resolve ConfigMap for system prompt",
			"configMap", ref.Name, "namespace", namespace, "key", ref.Key)
		return ""
	}
	return cm.Data[ref.Key]
}

// addWorkspaceEnvVars adds workspace-related env vars from the task.
func (b *JobBuilder) addWorkspaceEnvVars(
	envVars []corev1.EnvVar,
	task *corev1alpha1.Task,
) []corev1.EnvVar {
	ws := effectiveWorkspace(task)
	if ws == nil {
		return envVars
	}
	if ws.GitRepo != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.GitRepo, Value: ws.GitRepo,
		})
	}
	envVars = append(envVars,
		corev1.EnvVar{Name: workerenv.GitConfigCount, Value: "1"},
		corev1.EnvVar{Name: workerenv.GitConfigKey0, Value: "safe.directory"},
		corev1.EnvVar{Name: workerenv.GitConfigValue0, Value: "/workspace"},
	)
	if ws.Branch != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.GitBranch, Value: ws.Branch,
		})
	}
	if ws.Ref != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.GitRef, Value: ws.Ref,
		})
	}
	if ws.SubPath != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.WorkspaceSubpath, Value: ws.SubPath,
		})
	}
	if ws.PublicationGitRepo != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.ForkRepo, Value: ws.PublicationGitRepo,
		})
	}
	if ws.PRBaseBranch != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.PRBaseBranch, Value: ws.PRBaseBranch,
		})
	}
	if ws.PushBranch != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.PushBranch, Value: ws.PushBranch,
		})
		envVars = append(envVars, corev1.EnvVar{
			Name: workerenv.RequirePushBranch, Value: scheduledRunLabelValue,
		})
	}
	return envVars
}

// addWorkspaceVolumes adds workspace-specific volumes to the Job (workspace, home)
func (b *JobBuilder) addWorkspaceVolumes(job *batchv1.Job, task *corev1alpha1.Task, validationTask bool) {
	// /workspace emptyDir for git clone target
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name:         taskWorkspaceVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: repositoryMonitorValidationEmptyDir(validationTask, repositoryValidationWorkspaceLimit)},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      taskWorkspaceVolume,
			MountPath: "/workspace",
			ReadOnly:  validationTask,
		},
	)

	// /home/worker emptyDir for writable home (CLI config/cache)
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name:         runtimePoolHomeVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: repositoryMonitorValidationEmptyDir(validationTask, repositoryValidationHomeSizeLimit)},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      runtimePoolHomeVolume,
			MountPath: "/home/worker",
		},
	)

	ws := effectiveWorkspace(task)
	if ws == nil {
		return
	}
	if ws.ReadCredentialRef != nil {
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, workspaceCredentialVolume("git-read-credentials", ws.ReadCredentialRef.Name, ws.ReadCredentialRef.Key))
	}
	if ws.PublicationCredentialRef != nil {
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, workspaceCredentialVolume("git-publication-credentials", ws.PublicationCredentialRef.Name, ws.PublicationCredentialRef.Key))
	}
	if b.directGitCredentialsAllowed(task) && !taskUsesWorkspaceInitContainer(task) {
		if volumeName := mainWorkspaceCredentialVolume(task, ws); volumeName != "" {
			job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
				job.Spec.Template.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{Name: volumeName, MountPath: "/secrets/git", ReadOnly: true},
			)
		}
	}
}

func workspaceCredentialVolume(name, secretName, key string) corev1.Volume {
	key = strings.TrimSpace(key)
	if key == "" {
		key = defaultACPWorkspaceCredentialKey
	}
	volume := corev1.Volume{
		Name:         name,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName}},
	}
	volume.Secret.Items = []corev1.KeyToPath{{Key: key, Path: defaultACPWorkspaceCredentialKey}}
	return volume
}

func mainWorkspaceCredentialVolume(task *corev1alpha1.Task, workspace *corev1alpha1.WorkspaceConfig) string {
	if workspace == nil {
		return ""
	}
	if mainContainerNeedsGitCredentials(task) && workspace.PushBranch != "" {
		if workspace.PublicationCredentialRef != nil {
			return "git-publication-credentials"
		}
		return ""
	}
	if workspace.ReadCredentialRef != nil {
		return "git-read-credentials"
	}
	return ""
}

func taskNeedsWorkspace(task *corev1alpha1.Task) bool {
	return task != nil && effectiveWorkspace(task) != nil
}

func validateContainerPublicationWorkspace(task *corev1alpha1.Task) error {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeContainer {
		return nil
	}
	workspace := effectiveWorkspace(task)
	if workspace == nil {
		return nil
	}

	if strings.TrimSpace(workspace.ExpectedRemoteSHA) != "" {
		return fmt.Errorf("container Tasks do not support workspace.expectedRemoteSHA")
	}
	if workspace.CreatePR {
		return fmt.Errorf("container Tasks do not support workspace.createPR")
	}
	if field := unsupportedContainerWorkspacePolicyField(workspace); field != "" {
		return fmt.Errorf("container Tasks do not support clean-room workspace publication policy field %s", field)
	}
	if strings.TrimSpace(workspace.PushBranch) == "" {
		return nil
	}
	if strings.TrimSpace(task.Spec.Image) != "" {
		return fmt.Errorf("custom-image container Tasks do not support workspace.pushBranch publication")
	}
	if workspace.PublicationCredentialRef == nil || strings.TrimSpace(workspace.PublicationCredentialRef.Name) == "" {
		return fmt.Errorf("container workspace pushBranch requires publicationCredentialRef")
	}
	return nil
}

func unsupportedContainerWorkspacePolicyField(workspace *corev1alpha1.WorkspaceConfig) string {
	switch {
	case workspace == nil:
		return ""
	case workspace.MaxChangedFiles != nil:
		return "workspace.maxChangedFiles"
	case len(workspace.AllowedPaths) > 0:
		return "workspace.allowedPaths"
	case workspace.DenyRepositoryControlPaths:
		return "workspace.denyRepositoryControlPaths"
	case workspace.RejectBinaryFiles:
		return "workspace.rejectBinaryFiles"
	case workspace.RejectSecretLikeContent:
		return "workspace.rejectSecretLikeContent"
	default:
		return ""
	}
}

func taskNeedsWorkspaceInitContainer(task *corev1alpha1.Task) bool {
	workspace := effectiveWorkspace(task)
	if workspace == nil {
		return false
	}
	if taskUsesWorkspaceInitContainer(task) {
		return true
	}
	return task != nil && task.Spec.Type == corev1alpha1.TaskTypeContainer && (task.Spec.Image != "" || workspace.PushBranch != "")
}

func taskUsesWorkspaceInitContainer(task *corev1alpha1.Task) bool {
	return task != nil && task.Annotations[labels.AnnotationWorkspaceInitContainer] == scheduledRunLabelValue
}

func taskRequestsReadOnlyAgent(task *corev1alpha1.Task) bool {
	return task != nil && task.Annotations[labels.AnnotationAgentReadOnly] == scheduledRunLabelValue
}

func taskRequestsRuntimeAuthOnly(task *corev1alpha1.Task) bool {
	return task != nil && task.Annotations[labels.AnnotationAgentRuntimeAuthOnly] == scheduledRunLabelValue
}

func readOnlyAgentAllowedTools() []string {
	return []string{
		"Read(/workspace/**)",
		"Glob(/workspace/**)",
		"Grep(/workspace/**)",
		"LS(/workspace/**)",
	}
}

func effectiveWorkspace(task *corev1alpha1.Task) *corev1alpha1.WorkspaceConfig {
	if task == nil {
		return nil
	}
	return task.Spec.Workspace
}

func (b *JobBuilder) addWorkspaceInitContainer(job *batchv1.Job, task *corev1alpha1.Task, validationTask bool) {
	initContainer := corev1.Container{
		Name:            workspacePreparationInitContainerName,
		Image:           b.GeneralWorkerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Command:         []string{"/worker"},
		Args:            []string{"--prepare-workspace-only"},
		Env:             b.workspaceInitEnvVars(task, validationTask),
		VolumeMounts: []corev1.VolumeMount{
			{Name: taskWorkspaceVolume, MountPath: "/workspace"},
			{Name: runtimePoolHomeVolume, MountPath: "/home/worker"},
			{Name: runtimePoolTempVolume, MountPath: "/tmp"},
		},
	}
	if workspace := effectiveWorkspace(task); workspace != nil && workspace.ReadCredentialRef != nil {
		initContainer.VolumeMounts = append(initContainer.VolumeMounts, corev1.VolumeMount{
			Name:      "git-read-credentials",
			MountPath: "/secrets/git",
			ReadOnly:  true,
		})
	}
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, initContainer)
}

func (b *JobBuilder) addRepositoryMonitorValidationNetworkGate(job *batchv1.Job, task *corev1alpha1.Task) error {
	probeAddress, err := repositoryMonitorValidationProbeAddress(task)
	if err != nil {
		return err
	}
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, corev1.Container{
		Name:            repositoryMonitorValidationNetworkProbeContainer,
		Image:           b.GeneralWorkerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Command:         []string{"/worker"},
		Args: []string{
			repositoryMonitorValidationNetworkProbeWorkerMode,
			probeAddress,
		},
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: repositoryMonitorValidationNetworkGateVolume,
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: task.Name},
			Items: []corev1.KeyToPath{{
				Key:  repositoryMonitorValidationNetworkGateKey,
				Path: repositoryMonitorValidationNetworkGateKey,
			}},
		}},
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name:         repositoryMonitorValidationNetworkSandboxVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, corev1.Container{
		Name:            repositoryMonitorValidationNetworkGateContainer,
		Image:           b.GeneralWorkerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Command:         []string{"/worker"},
		Args: []string{
			repositoryMonitorValidationNetworkGateWorkerMode,
			path.Join(repositoryMonitorValidationNetworkGateMount, repositoryMonitorValidationNetworkGateKey),
			path.Join(repositoryMonitorValidationNetworkSandboxMount, repositoryMonitorValidationNetworkSandboxBinary),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      repositoryMonitorValidationNetworkGateVolume,
				MountPath: repositoryMonitorValidationNetworkGateMount,
				ReadOnly:  true,
			},
			{
				Name:      repositoryMonitorValidationNetworkSandboxVolume,
				MountPath: repositoryMonitorValidationNetworkSandboxMount,
			},
		},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      repositoryMonitorValidationNetworkSandboxVolume,
		MountPath: repositoryMonitorValidationNetworkSandboxMount,
		ReadOnly:  true,
	})
	return nil
}

func (b *JobBuilder) addRepositoryMonitorValidationCommand(job *batchv1.Job, task *corev1alpha1.Task) error {
	commandDigest := strings.TrimSpace(task.Annotations[labels.AnnotationRepositoryValidationCommandDigest])
	if commandDigest == "" {
		return fmt.Errorf("repository validation command digest is required")
	}
	mode := int32(0o400)
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: repositoryMonitorValidationCommandSourceVolume,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  tools.RepositoryValidationCommandSecretName(task.Name),
			DefaultMode: &mode,
			Items: []corev1.KeyToPath{{
				Key:  tools.RepositoryValidationCommandSecretKey,
				Path: repositoryMonitorValidationCommandFile,
			}},
		}},
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: repositoryMonitorValidationCommandVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: repositoryMonitorValidationEmptyDir(
			true,
			repositoryValidationCommandLimit,
		)},
	})
	sourcePath := path.Join(repositoryMonitorValidationCommandSourceMount, repositoryMonitorValidationCommandFile)
	destinationPath := path.Join(repositoryMonitorValidationCommandMount, repositoryMonitorValidationCommandFile)
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, corev1.Container{
		Name:            repositoryMonitorValidationCommandContainer,
		Image:           b.GeneralWorkerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: b.buildContainerSecurityContext(),
		Command:         []string{"/worker"},
		Args: []string{
			repositoryMonitorValidationCommandWorkerMode,
			sourcePath,
			destinationPath,
			commandDigest,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: repositoryMonitorValidationCommandSourceVolume, MountPath: repositoryMonitorValidationCommandSourceMount, ReadOnly: true},
			{Name: repositoryMonitorValidationCommandVolume, MountPath: repositoryMonitorValidationCommandMount},
		},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      repositoryMonitorValidationCommandVolume,
		MountPath: repositoryMonitorValidationCommandMount,
		ReadOnly:  true,
	})
	return nil
}

func repositoryMonitorValidationProbeAddress(task *corev1alpha1.Task) (string, error) {
	workspace := effectiveWorkspace(task)
	if workspace == nil {
		return "", fmt.Errorf("repository validation requires a workspace")
	}
	parsed, err := url.Parse(strings.TrimSpace(workspace.GitRepo))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" {
		return "", fmt.Errorf("repository validation requires an HTTPS repository endpoint for network-policy enforcement")
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	return net.JoinHostPort(parsed.Hostname(), port), nil
}

func (b *JobBuilder) workspaceInitEnvVars(task *corev1alpha1.Task, validationTask bool) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		{Name: TaskNameEnvVar, Value: task.Name},
		{Name: TaskNamespaceEnvVar, Value: task.Namespace},
		{Name: ControllerURLEnvVar, Value: b.ControllerURL},
	}
	envVars = append(envVars, task.Spec.Env...)
	if task.Spec.PriorTaskRef != nil {
		envVars = append(envVars, corev1.EnvVar{Name: workerenv.PriorTask, Value: task.Spec.PriorTaskRef.Name})
		priorNS := task.Spec.PriorTaskRef.Namespace
		if priorNS == "" {
			priorNS = task.Namespace
		}
		envVars = append(envVars, corev1.EnvVar{Name: workerenv.PriorTaskNamespace, Value: priorNS})
	}
	envVars = b.addWorkspaceEnvVars(envVars, task)
	if validationTask {
		envVars = append(envVars, corev1.EnvVar{Name: workerenv.GitRefShallow, Value: scheduledRunLabelValue})
	}
	return envVars
}

func workspaceWorkingDir(task *corev1alpha1.Task) string {
	ws := effectiveWorkspace(task)
	if ws != nil && ws.SubPath != "" {
		return path.Join("/workspace", ws.SubPath)
	}
	return "/workspace"
}

// addSkillVolumes reads Skill CRs referenced by the agent and task, creates a ConfigMap
// with concatenated skill content, and mounts it at /workspace/.skills/.
func (b *JobBuilder) addSkillVolumes(ctx context.Context, job *batchv1.Job, task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	logger := log.FromContext(ctx)

	// Collect skill references: agent-level first, then task-level
	var skillRefs []corev1alpha1.SkillReference
	if agent != nil {
		skillRefs = append(skillRefs, agent.Spec.Skills...)
	}
	if task.Spec.AI != nil {
		skillRefs = append(skillRefs, task.Spec.AI.Skills...)
	}

	// Deduplicate skill references by resolved identifier
	seen := make(map[string]bool)
	deduped := make([]corev1alpha1.SkillReference, 0, len(skillRefs))
	for _, ref := range skillRefs {
		var key string
		switch {
		case ref.Name != "":
			key = "skill:" + ref.Name
		case ref.ConfigMapRef != nil:
			key = "configmap:" + ref.ConfigMapRef.Name + "/" + ref.ConfigMapRef.Key
		}
		if key != "" && !seen[key] {
			seen[key] = true
			deduped = append(deduped, ref)
		}
	}
	skillRefs = deduped

	if len(skillRefs) == 0 {
		return nil
	}

	// Read Skill CRs and build ConfigMap data.
	// "PROMPT.md" is the only file injected into the model system prompt.
	cmData := make(map[string]string)
	items := make([]corev1.KeyToPath, 0, len(skillRefs)+1)
	promptParts := make([]string, 0, len(skillRefs))

	for idx, ref := range skillRefs {
		switch {
		case ref.Name != "":
			skill := &corev1alpha1.Skill{}
			skillName := ref.Name
			if err := b.Get(ctx, client.ObjectKey{Name: skillName, Namespace: task.Namespace}, skill); err != nil {
				return fmt.Errorf("failed to get Skill %q: %w", skillName, err)
			}

			metrics.SkillsLoaded.WithLabelValues(skill.Name, task.Namespace).Inc()

			promptParts = append(promptParts, strings.TrimSpace(skill.Spec.Content.Inline))

			inlineKey := fmt.Sprintf("skill-%d-inline", idx)
			cmData[inlineKey] = skill.Spec.Content.Inline
			items = append(items, corev1.KeyToPath{
				Key:  inlineKey,
				Path: path.Join(skillName, "SKILL.md"),
			})

			filePaths := make([]string, 0, len(skill.Spec.Content.Files))
			for filePath := range skill.Spec.Content.Files {
				filePaths = append(filePaths, filePath)
			}
			sort.Strings(filePaths)
			for i, filePath := range filePaths {
				fileKey := fmt.Sprintf("skill-%d-file-%d", idx, i)
				cmData[fileKey] = skill.Spec.Content.Files[filePath]
				items = append(items, corev1.KeyToPath{
					Key:  fileKey,
					Path: path.Join(skillName, filePath),
				})
			}
		case ref.ConfigMapRef != nil:
			cm := &corev1.ConfigMap{}
			if err := b.Get(ctx, client.ObjectKey{Name: ref.ConfigMapRef.Name, Namespace: task.Namespace}, cm); err != nil {
				return fmt.Errorf("failed to get skill ConfigMap %q: %w", ref.ConfigMapRef.Name, err)
			}
			content, ok := cm.Data[ref.ConfigMapRef.Key]
			if !ok {
				return fmt.Errorf("key %q not found in skill ConfigMap %q", ref.ConfigMapRef.Key, ref.ConfigMapRef.Name)
			}

			metrics.SkillsLoaded.WithLabelValues(ref.ConfigMapRef.Name, task.Namespace).Inc()

			promptParts = append(promptParts, strings.TrimSpace(content))

			inlineKey := fmt.Sprintf("skill-%d-inline", idx)
			cmData[inlineKey] = content
			items = append(items, corev1.KeyToPath{
				Key:  inlineKey,
				Path: path.Join(ref.ConfigMapRef.Name+"-"+ref.ConfigMapRef.Key, "SKILL.md"),
			})
		default:
			return fmt.Errorf("skill reference must set either name or configMapRef")
		}
	}

	prompt := strings.TrimSpace(strings.Join(promptParts, "\n\n"))
	if prompt == "" {
		return nil
	}
	cmData["system-prompt"] = prompt
	items = append(items, corev1.KeyToPath{Key: "system-prompt", Path: "PROMPT.md"})

	// Create a ConfigMap for skill content owned by the Job
	skillCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + "-skills",
			Namespace: job.Namespace,
			Labels: map[string]string{
				labels.LabelTask:    labels.SelectorValue(task.Name),
				labels.LabelPurpose: "skills",
				labels.LabelManaged: scheduledRunLabelValue,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(task, corev1alpha1.GroupVersion.WithKind("Task")),
			},
		},
		Data: cmData,
	}

	if err := b.Create(ctx, skillCM); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create skill ConfigMap: %w", err)
		}
		existing := &corev1.ConfigMap{}
		if getErr := b.Get(ctx, client.ObjectKey{Name: skillCM.Name, Namespace: skillCM.Namespace}, existing); getErr != nil {
			return fmt.Errorf("failed to get existing skill ConfigMap: %w", getErr)
		}
		if !reflect.DeepEqual(existing.Data, cmData) {
			existing.Data = cmData
			if updateErr := b.Update(ctx, existing); updateErr != nil {
				return fmt.Errorf("failed to update existing skill ConfigMap: %w", updateErr)
			}
		}
	} else {
		logger.Info("Created skill ConfigMap", "configmap", skillCM.Name, "skills", len(skillRefs))
	}

	// Mount the ConfigMap into the worker pod
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "skills",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: skillCM.Name,
				},
				Items: items,
			},
		},
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{
			Name:      "skills",
			MountPath: "/workspace/.skills",
			ReadOnly:  true,
		},
	)

	return nil
}

// maxContainerDeliveredPromptBytes bounds the prompt and system prompt a
// worker Job can carry: both travel as environment variables, and Linux
// rejects any single execve argument or environment string over
// MAX_ARG_STRLEN (128 KiB) with the opaque "argument list too long" exec
// failure. Guard well below that so the Task fails with an actionable
// message instead of a dead container.
const maxContainerDeliveredPromptBytes = 110 * 1024

func (b *JobBuilder) validateContainerDeliveredPromptSize(ctx context.Context, task *corev1alpha1.Task, agent *corev1alpha1.Agent) error {
	if task == nil || task.Spec.Type != corev1alpha1.TaskTypeAI {
		// Only AI worker Jobs export prompts through the process
		// environment; a container Task's unused optional prompt fields must
		// not make an otherwise runnable container fail this guard.
		return nil
	}
	// Mirror resolveAIConfig's precedence exactly — spec.ai.prompt over
	// spec.prompt — and resolve a ConfigMap-backed Agent system prompt before
	// measuring: the guard must see the values that actually reach the
	// environment.
	prompt := ""
	if task.Spec.AI != nil {
		prompt = task.Spec.AI.Prompt
	}
	if prompt == "" {
		prompt = task.Spec.Prompt
	}
	systemPrompt := ""
	if task.Spec.AI != nil {
		systemPrompt = task.Spec.AI.SystemPrompt
	}
	if systemPrompt == "" && agent != nil && agent.Spec.SystemPrompt != nil {
		systemPrompt = agent.Spec.SystemPrompt.Inline
		if systemPrompt == "" && agent.Spec.SystemPrompt.ConfigMapRef != nil {
			systemPrompt = b.resolveConfigMapValue(ctx, agent.Namespace, agent.Spec.SystemPrompt.ConfigMapRef)
		}
	}
	for name, value := range map[string]string{"prompt": prompt, "system prompt": systemPrompt} {
		if len(value) > maxContainerDeliveredPromptBytes {
			return fmt.Errorf(
				"%s is %d bytes; container-delivered prompts are limited to %d bytes because they are passed as process environment (Linux MAX_ARG_STRLEN). Shorten the %s or supply the content through the workspace instead",
				name, len(value), maxContainerDeliveredPromptBytes, name)
		}
	}
	return nil
}
