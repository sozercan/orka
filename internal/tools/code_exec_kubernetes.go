/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orka-agents/orka/internal/workerenv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	codeExecKubernetesContainerName = "code-exec"
	codeExecKubernetesJobPrefix     = "orka-code-exec-"
	codeExecKubernetesLabelTool     = "orka.ai/tool"
	codeExecKubernetesLabelJob      = "orka.ai/code-exec-job"

	codeExecKubernetesAnnotationRunID                = "orka.ai/code-exec-run-id"
	codeExecKubernetesAnnotationInputHash            = "orka.ai/code-exec-input-hash"
	codeExecKubernetesAnnotationResultVersion        = "orka.ai/code-exec-result-version"
	codeExecKubernetesResultVersion                  = "code_exec_result_v1"
	codeExecKubernetesResultKey                      = "result.json"
	codeExecKubernetesStoreResultTimeout             = 15 * time.Second
	codeExecKubernetesStoredResultRetention          = 24 * time.Hour
	codeExecKubernetesStoredResultCleanupMinInterval = 10 * time.Minute

	codeExecKubernetesCodeVolumeName = "code"
	codeExecKubernetesCodeMountPath  = "/orka-code"
	codeExecKubernetesCodePath       = codeExecKubernetesCodeMountPath + "/code"

	codeExecKubernetesImageEnv       = workerenv.CodeExecKubernetesImage
	codeExecKubernetesPythonImageEnv = workerenv.CodeExecKubernetesPythonImage
	codeExecKubernetesNodeImageEnv   = workerenv.CodeExecKubernetesNodeImage
	codeExecKubernetesBashImageEnv   = workerenv.CodeExecKubernetesBashImage

	codeExecKubernetesCPURequestEnv       = workerenv.CodeExecKubernetesCPURequest
	codeExecKubernetesCPULimitEnv         = workerenv.CodeExecKubernetesCPULimit
	codeExecKubernetesMemoryRequestEnv    = workerenv.CodeExecKubernetesMemoryRequest
	codeExecKubernetesMemoryLimitEnv      = workerenv.CodeExecKubernetesMemoryLimit
	codeExecKubernetesNetworkPolicyEnv    = workerenv.CodeExecKubernetesNetworkPolicy
	codeExecKubernetesRuntimeClassNameEnv = workerenv.CodeExecKubernetesRuntimeClass
	codeExecKubernetesAppArmorProfileEnv  = workerenv.CodeExecKubernetesAppArmor

	// Keep completed Jobs long enough for the executor to observe status and stream logs.
	// The executor still deletes all resources after each run; this TTL is only a
	// fallback for completed Jobs if the worker exits before cleanup. A zero TTL can
	// race the Job TTL controller in live clusters and delete Jobs before results are read.
	codeExecKubernetesFinishedTTLSeconds = int32(60)
)

// KubernetesJobCodeExecutor runs code in a short-lived Kubernetes Job.
type KubernetesJobCodeExecutor struct {
	resolveClients func(context.Context) (kubernetesCodeExecClients, error)
	logStreamer    podLogStreamer
	pollInterval   time.Duration
	randomSuffix   func() string
	cleanupState   kubernetesCodeExecStoredResultCleanupState
}

var _ SandboxClient = (*KubernetesJobCodeExecutor)(nil)

type kubernetesCodeExecStoredResultCleanupState struct {
	sync.Mutex
	lastByNamespace map[string]time.Time
}

func (s *kubernetesCodeExecStoredResultCleanupState) shouldRun(namespace string, now time.Time) bool {
	s.Lock()
	defer s.Unlock()

	if s.lastByNamespace == nil {
		s.lastByNamespace = map[string]time.Time{}
	}

	last := s.lastByNamespace[namespace]
	return last.IsZero() || now.Sub(last) >= codeExecKubernetesStoredResultCleanupMinInterval
}

func (s *kubernetesCodeExecStoredResultCleanupState) record(namespace string, now time.Time) {
	s.Lock()
	defer s.Unlock()

	if s.lastByNamespace == nil {
		s.lastByNamespace = map[string]time.Time{}
	}
	s.pruneLocked(now)
	s.lastByNamespace[namespace] = now
}

func (s *kubernetesCodeExecStoredResultCleanupState) pruneLocked(now time.Time) {
	for ns, last := range s.lastByNamespace {
		if last.IsZero() || now.Sub(last) > 2*codeExecKubernetesStoredResultCleanupMinInterval {
			delete(s.lastByNamespace, ns)
		}
	}
}

type kubernetesCodeExecClients struct {
	client           crclient.Client
	kubeClient       kubernetes.Interface
	namespace        string
	authorizePodLogs func(context.Context, string, string) error
}

type kubernetesCodeExecResources struct {
	job            *batchv1.Job
	secret         *corev1.Secret
	serviceAccount *corev1.ServiceAccount
	networkPolicy  *networkingv1.NetworkPolicy
}

type kubernetesCodeExecLogMarkers struct {
	stdoutStart     string
	stdoutEnd       string
	stdoutTruncated string
	stderrStart     string
	stderrEnd       string
	stderrTruncated string
}

type kubernetesCodeExecLogOutput struct {
	stdout          string
	stderr          string
	stdoutTruncated bool
	stderrTruncated bool
}

type kubernetesCodeExecStoredResult struct {
	Version   string         `json:"version"`
	RunID     string         `json:"run_id"`
	InputHash string         `json:"input_hash"`
	Result    CodeExecResult `json:"result"`
}

type podLogStreamer interface {
	Stream(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
}

type kubeClientPodLogStreamer struct {
	client kubernetes.Interface
}

func (s kubeClientPodLogStreamer) Stream(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	return s.client.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
}

// Run executes a sandbox request with the Kubernetes Job backend.
func (e *KubernetesJobCodeExecutor) Run(ctx context.Context, req SandboxRunRequest) SandboxRunResult {
	return sandboxRunResultFromCodeExecResult(e.Execute(ctx, codeExecutionRequestFromSandboxRunRequest(req)))
}

// Execute runs the request in Kubernetes.
func (e *KubernetesJobCodeExecutor) Execute(ctx context.Context, req CodeExecutionRequest) CodeExecResult {
	start := time.Now()
	result := CodeExecResult{ExitCode: -1}

	if req.Backend == "" {
		req.Backend = codeExecBackendKubernetes
	}
	if req.Timeout <= 0 {
		req.Timeout = defaultCodeExecTimeout
	}
	if req.OutputLimitBytes <= 0 {
		req.OutputLimitBytes = defaultCodeExecOutputLimitBytes
	}
	if err := populateCodeExecRequestResourceAudit(&req); err != nil {
		result.Error = fmt.Sprintf("failed to configure kubernetes code_exec resources: %v", err)
		return result
	}
	ensureCodeExecRequestInputHash(&req)

	execCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()
	ctx = execCtx

	defer func() {
		auditCodeExec(ctx, req, result, time.Since(start))
	}()

	if req.Language == codeLanguageBash || req.Language == codeLanguageShell {
		if msg := checkDenyPatterns(req.Code, req.DenyPatterns); msg != "" {
			result.Error = msg
			return result
		}
	}

	clients, err := e.kubernetesClients(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("kubernetes code_exec backend unavailable: %v", err)
		return result
	}
	if err := e.validateClients(clients); err != nil {
		result.Error = fmt.Sprintf("kubernetes code_exec backend unavailable: %v", err)
		return result
	}

	jobName := e.jobNameForRequest(req)
	if storedResult, found, err := e.loadStoredResult(ctx, clients.client, clients.namespace, jobName, req); err != nil {
		result.Error = fmt.Sprintf("failed to read persisted kubernetes code_exec result: %v", err)
		return result
	} else if found {
		result = storedResult
		return result
	}

	resources, err := e.buildResourcesWithJobName(clients.namespace, req, jobName)
	if err != nil {
		result.Error = fmt.Sprintf("failed to configure kubernetes code_exec resources: %v", err)
		return result
	}

	createdResources, err := e.createResources(ctx, clients.client, resources)
	if err != nil {
		e.cleanupResources(clients.client, createdResources)
		result.Error = fmt.Sprintf("failed to create kubernetes code_exec resources: %v", err)
		return result
	}
	defer e.cleanupResources(clients.client, createdResources)

	result = e.waitForJob(ctx, clients, resources.job.Name, req)
	if codeExecKubernetesShouldPersistObservedResult(result) {
		if err := e.storeResult(ctx, clients.client, clients.namespace, resources.job.Name, req, result); err != nil {
			result.Error = appendCodeExecError(result.Error, fmt.Sprintf("failed to persist kubernetes code_exec result: %v", err))
		}
	}
	return result
}

func (e *KubernetesJobCodeExecutor) validateClients(clients kubernetesCodeExecClients) error {
	if clients.client == nil {
		return fmt.Errorf("controller-runtime Kubernetes client is not configured")
	}
	if clients.kubeClient == nil && e.logStreamer == nil {
		return fmt.Errorf("client-go Kubernetes clientset is not configured")
	}
	if strings.TrimSpace(clients.namespace) == "" {
		return fmt.Errorf("namespace is not configured")
	}
	return nil
}

func (e *KubernetesJobCodeExecutor) kubernetesClients(ctx context.Context) (kubernetesCodeExecClients, error) {
	if e.resolveClients != nil {
		return e.resolveClients(ctx)
	}

	clients := kubernetesCodeExecClients{namespace: defaultCodeExecNamespace()}
	if tc := GetToolContext(ctx); tc != nil {
		clients.client = tc.Client
		clients.kubeClient = tc.KubeClient
		clients.authorizePodLogs = tc.AuthorizePodLogs
		if strings.TrimSpace(tc.Namespace) != "" {
			clients.namespace = tc.Namespace
		}
	}

	needsKubeClient := e.logStreamer == nil
	if clients.client != nil && (!needsKubeClient || clients.kubeClient != nil) {
		return clients, nil
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		if clients.client != nil && !needsKubeClient {
			return clients, nil
		}
		return clients, fmt.Errorf("no complete ToolContext Kubernetes clients and in-cluster config is unavailable: %w", err)
	}

	if clients.client == nil {
		clients.client, err = crclient.New(restConfig, crclient.Options{Scheme: clientgoscheme.Scheme})
		if err != nil {
			return clients, fmt.Errorf("failed to create controller-runtime Kubernetes client: %w", err)
		}
	}
	if clients.kubeClient == nil && needsKubeClient {
		clients.kubeClient, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return clients, fmt.Errorf("failed to create client-go Kubernetes clientset: %w", err)
		}
	}

	return clients, nil
}

//nolint:unparam // Tests use a fixed namespace while keeping the helper explicit about resource scope.
func (e *KubernetesJobCodeExecutor) buildResourcesWithJobName(namespace string, req CodeExecutionRequest, jobName string) (*kubernetesCodeExecResources, error) {
	image, err := codeExecKubernetesImageForRequest(req)
	if err != nil {
		return nil, err
	}
	resources, err := codeExecKubernetesResourcesForRequest(req)
	if err != nil {
		return nil, err
	}

	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return nil, fmt.Errorf("kubernetes code_exec job name is required")
	}
	command, err := codeExecKubernetesCommand(req.Language, req.OutputLimitBytes, jobName)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{
		"app.kubernetes.io/name":       "orka",
		codeExecKubernetesLabelTool:    codeExecToolName,
		codeExecKubernetesLabelJob:     jobName,
		"batch.kubernetes.io/job-name": jobName,
	}
	annotations := codeExecKubernetesIdentityAnnotations(req)

	backoffLimit := int32(0)
	deadlineSeconds := codeExecDeadlineSeconds(req.Timeout)
	ttlSeconds := codeExecKubernetesFinishedTTLSeconds
	terminationGracePeriodSeconds := int64(1)
	runAsNonRoot := true
	runAsUser := int64(65532)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	automountServiceAccountToken := false
	fsGroup := int64(65532)
	seccompType := corev1.SeccompProfileTypeRuntimeDefault
	appArmorProfile, err := codeExecKubernetesAppArmorProfileForRequest(req)
	if err != nil {
		return nil, err
	}
	tmpSizeLimit := resource.MustParse("16Mi")
	codeDefaultMode := int32(0444)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   namespace,
			Labels:      cloneKubernetesCodeExecStringMap(labels),
			Annotations: cloneKubernetesCodeExecStringMap(annotations),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			codeExecKubernetesCodeVolumeName: []byte(req.Code),
		},
	}

	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   namespace,
			Labels:      cloneKubernetesCodeExecStringMap(labels),
			Annotations: cloneKubernetesCodeExecStringMap(annotations),
		},
		AutomountServiceAccountToken: &automountServiceAccountToken,
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   namespace,
			Labels:      cloneKubernetesCodeExecStringMap(labels),
			Annotations: cloneKubernetesCodeExecStringMap(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &deadlineSeconds,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      cloneKubernetesCodeExecStringMap(labels),
					Annotations: cloneKubernetesCodeExecStringMap(annotations),
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					ServiceAccountName:            serviceAccount.Name,
					AutomountServiceAccountToken:  &automountServiceAccountToken,
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:    &runAsNonRoot,
						RunAsUser:       &runAsUser,
						FSGroup:         &fsGroup,
						SeccompProfile:  &corev1.SeccompProfile{Type: seccompType},
						AppArmorProfile: appArmorProfile,
					},
					Containers: []corev1.Container{
						{
							Name:            codeExecKubernetesContainerName,
							Image:           image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         command,
							Env: []corev1.EnvVar{
								{Name: "PATH", Value: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
								{Name: "HOME", Value: tempDirPath},
								{Name: "TMPDIR", Value: tempDirPath},
								{Name: "TMP", Value: tempDirPath},
								{Name: "TEMP", Value: tempDirPath},
								{Name: "LANG", Value: cUTF8Locale},
								{Name: "LC_ALL", Value: cUTF8Locale},
								{Name: "LC_CTYPE", Value: cUTF8Locale},
								{Name: "TERM", Value: "dumb"},
							},
							Resources: resources,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "tmp", MountPath: tempDirPath},
								{Name: codeExecKubernetesCodeVolumeName, MountPath: codeExecKubernetesCodeMountPath, ReadOnly: true},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             &runAsNonRoot,
								RunAsUser:                &runAsUser,
								ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
								AllowPrivilegeEscalation: &allowPrivilegeEscalation,
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
								SeccompProfile:  &corev1.SeccompProfile{Type: seccompType},
								AppArmorProfile: appArmorProfile,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmpSizeLimit},
							},
						},
						{
							Name: codeExecKubernetesCodeVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  secret.Name,
									DefaultMode: &codeDefaultMode,
									Items: []corev1.KeyToPath{
										{Key: codeExecKubernetesCodeVolumeName, Path: codeExecKubernetesCodeVolumeName, Mode: &codeDefaultMode},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if runtimeClassName := codeExecKubernetesRuntimeClassNameForRequest(req); runtimeClassName != "" {
		job.Spec.Template.Spec.RuntimeClassName = &runtimeClassName
	}

	resourcesToCreate := &kubernetesCodeExecResources{
		job:            job,
		secret:         secret,
		serviceAccount: serviceAccount,
	}
	if codeExecKubernetesNetworkPolicyEnabledForRequest(req) {
		resourcesToCreate.networkPolicy = buildKubernetesCodeExecNetworkPolicy(namespace, jobName, labels, annotations)
	}

	return resourcesToCreate, nil
}

func buildKubernetesCodeExecNetworkPolicy(namespace, jobName string, labels, annotations map[string]string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   namespace,
			Labels:      cloneKubernetesCodeExecStringMap(labels),
			Annotations: cloneKubernetesCodeExecStringMap(annotations),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{codeExecKubernetesLabelJob: jobName}},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
		},
	}
}

func (e *KubernetesJobCodeExecutor) createResources(ctx context.Context, c crclient.Client, resources *kubernetesCodeExecResources) (*kubernetesCodeExecResources, error) {
	created := &kubernetesCodeExecResources{}
	if c == nil {
		return created, fmt.Errorf("kubernetes client is not configured")
	}
	if resources == nil || resources.job == nil || resources.secret == nil || resources.serviceAccount == nil {
		return created, fmt.Errorf("required Kubernetes resources are not configured")
	}
	if tc := GetToolContext(ctx); tc != nil && tc.AuthorizeCodeExecResources != nil {
		objects := make([]crclient.Object, 0, 4)
		objects = append(objects, resources.secret, resources.serviceAccount, resources.job)
		if resources.networkPolicy != nil {
			objects = append(objects, resources.networkPolicy)
		}
		if err := tc.AuthorizeCodeExecResources(ctx, objects); err != nil {
			return created, fmt.Errorf("resource authorization: %w", err)
		}
	}

	if wasCreated, err := createKubernetesCodeExecObject(ctx, c, resources.secret); err != nil {
		return created, fmt.Errorf("secret: %w", err)
	} else if wasCreated {
		created.secret = resources.secret
	}

	if wasCreated, err := createKubernetesCodeExecObject(ctx, c, resources.serviceAccount); err != nil {
		return created, fmt.Errorf("service account: %w", err)
	} else if wasCreated {
		created.serviceAccount = resources.serviceAccount
	}

	if resources.networkPolicy != nil {
		if wasCreated, err := createKubernetesCodeExecObject(ctx, c, resources.networkPolicy); err != nil {
			return created, fmt.Errorf("network policy: %w", err)
		} else if wasCreated {
			created.networkPolicy = resources.networkPolicy
		}
	}

	if wasCreated, err := createKubernetesCodeExecObject(ctx, c, resources.job); err != nil {
		return created, fmt.Errorf("job: %w", err)
	} else if wasCreated {
		created.job = resources.job
	}

	return created, nil
}

func createKubernetesCodeExecObject(ctx context.Context, c crclient.Client, obj crclient.Object) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("kubernetes client is not configured")
	}
	if obj == nil {
		return false, fmt.Errorf("kubernetes object is not configured")
	}
	if err := c.Create(ctx, obj); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		existing := newKubernetesCodeExecObjectLike(obj)
		if existing == nil {
			return false, fmt.Errorf("existing %T cannot be checked for code_exec reuse", obj)
		}
		if getErr := c.Get(ctx, crclient.ObjectKeyFromObject(obj), existing); getErr != nil {
			return false, getErr
		}
		if err := validateKubernetesCodeExecReusableObject(obj, existing); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func newKubernetesCodeExecObjectLike(obj crclient.Object) crclient.Object {
	switch obj.(type) {
	case *corev1.Secret:
		return &corev1.Secret{}
	case *corev1.ServiceAccount:
		return &corev1.ServiceAccount{}
	case *corev1.ConfigMap:
		return &corev1.ConfigMap{}
	case *batchv1.Job:
		return &batchv1.Job{}
	case *networkingv1.NetworkPolicy:
		return &networkingv1.NetworkPolicy{}
	default:
		if copied, ok := obj.DeepCopyObject().(crclient.Object); ok {
			return copied
		}
		return nil
	}
}

func validateKubernetesCodeExecReusableObject(expected, existing crclient.Object) error {
	if expected == nil || existing == nil {
		return fmt.Errorf("kubernetes object is not configured")
	}
	expectedAnnotations := expected.GetAnnotations()
	expectedRunID := strings.TrimSpace(expectedAnnotations[codeExecKubernetesAnnotationRunID])
	if expectedRunID == "" {
		return fmt.Errorf("existing %T %s/%s cannot be reused without a code_exec run id annotation", existing, existing.GetNamespace(), existing.GetName())
	}

	actualAnnotations := existing.GetAnnotations()
	for _, key := range []string{
		codeExecKubernetesAnnotationRunID,
		codeExecKubernetesAnnotationInputHash,
		codeExecKubernetesAnnotationResultVersion,
	} {
		expectedValue := strings.TrimSpace(expectedAnnotations[key])
		if expectedValue == "" {
			continue
		}
		if strings.TrimSpace(actualAnnotations[key]) != expectedValue {
			return fmt.Errorf("existing %T %s/%s has mismatched %s annotation", existing, existing.GetNamespace(), existing.GetName(), key)
		}
	}
	return nil
}

func codeExecKubernetesIdentityAnnotations(req CodeExecutionRequest) map[string]string {
	annotations := map[string]string{}
	if runID := strings.TrimSpace(req.RunID); runID != "" {
		annotations[codeExecKubernetesAnnotationRunID] = runID
	}
	if inputHash := strings.TrimSpace(req.InputHash); inputHash != "" {
		annotations[codeExecKubernetesAnnotationInputHash] = inputHash
	}
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}

func cloneKubernetesCodeExecStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	clone := make(map[string]string, len(values))
	maps.Copy(clone, values)
	return clone
}

func codeExecKubernetesShouldPersistResult(req CodeExecutionRequest) bool {
	return strings.TrimSpace(req.RunID) != "" && strings.TrimSpace(req.InputHash) != ""
}

// Container exit codes are non-negative; this executor reserves -1 for infrastructure failures.
func codeExecKubernetesShouldPersistObservedResult(result CodeExecResult) bool {
	return !result.TimedOut && result.ExitCode != -1
}

func (e *KubernetesJobCodeExecutor) loadStoredResult(ctx context.Context, c crclient.Client, namespace, jobName string, req CodeExecutionRequest) (CodeExecResult, bool, error) {
	if !codeExecKubernetesShouldPersistResult(req) {
		return CodeExecResult{}, false, nil
	}
	if c == nil {
		return CodeExecResult{}, false, fmt.Errorf("kubernetes client is not configured")
	}
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(jobName) == "" {
		return CodeExecResult{}, false, fmt.Errorf("namespace and job name are required")
	}

	stored := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, stored); err != nil {
		if apierrors.IsNotFound(err) {
			return CodeExecResult{}, false, nil
		}
		return CodeExecResult{}, false, err
	}
	if !kubernetesCodeExecStoredResultIdentityMatches(stored, req) {
		return CodeExecResult{}, false, nil
	}
	if !kubernetesCodeExecStoredResultVersionMatches(stored) {
		_ = c.Delete(ctx, stored)
		return CodeExecResult{}, false, nil
	}

	raw := stored.Data[codeExecKubernetesResultKey]
	if raw == "" {
		_ = c.Delete(ctx, stored)
		return CodeExecResult{}, false, nil
	}
	var storedResult kubernetesCodeExecStoredResult
	if err := json.Unmarshal([]byte(raw), &storedResult); err != nil {
		_ = c.Delete(ctx, stored)
		return CodeExecResult{}, false, nil
	}
	if storedResult.Version != codeExecKubernetesResultVersion {
		_ = c.Delete(ctx, stored)
		return CodeExecResult{}, false, nil
	}
	if strings.TrimSpace(storedResult.RunID) != strings.TrimSpace(req.RunID) || strings.TrimSpace(storedResult.InputHash) != strings.TrimSpace(req.InputHash) {
		_ = c.Delete(ctx, stored)
		return CodeExecResult{}, false, nil
	}
	return storedResult.Result, true, nil
}

func (e *KubernetesJobCodeExecutor) storeResult(ctx context.Context, c crclient.Client, namespace, jobName string, req CodeExecutionRequest, result CodeExecResult) error {
	if !codeExecKubernetesShouldPersistResult(req) {
		return nil
	}
	if !codeExecKubernetesShouldPersistObservedResult(result) {
		return nil
	}
	if c == nil {
		return fmt.Errorf("kubernetes client is not configured")
	}
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(jobName) == "" {
		return fmt.Errorf("namespace and job name are required")
	}

	stored := kubernetesCodeExecStoredResult{
		Version:   codeExecKubernetesResultVersion,
		RunID:     strings.TrimSpace(req.RunID),
		InputHash: strings.TrimSpace(req.InputHash),
		Result:    result,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	annotations := codeExecKubernetesIdentityAnnotations(req)
	annotations[codeExecKubernetesAnnotationResultVersion] = codeExecKubernetesResultVersion
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "orka",
				codeExecKubernetesLabelTool: codeExecToolName,
				codeExecKubernetesLabelJob:  jobName,
			},
			Annotations: annotations,
		},
		Data: map[string]string{codeExecKubernetesResultKey: string(data)},
	}

	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), codeExecKubernetesStoreResultTimeout)
	defer cancel()
	created, err := createKubernetesCodeExecObject(storeCtx, c, configMap)
	if err != nil {
		if apierrors.IsForbidden(err) {
			return nil
		}
		if retryStored, retryErr := e.retryStoreResultAfterInvalidCache(storeCtx, c, namespace, jobName, req, configMap); retryErr == nil {
			created = retryStored
		} else {
			return err
		}
	}
	if !created {
		retryStored, err := e.retryStoreResultAfterInvalidCache(storeCtx, c, namespace, jobName, req, configMap)
		if err != nil {
			return err
		}
		if !retryStored {
			return fmt.Errorf("existing stored result %s/%s did not persist replacement", namespace, jobName)
		}
	}
	// The result is already durable; cleanup must not turn a successful store
	// into a failed execution result.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), codeExecKubernetesStoreResultTimeout)
	defer cleanupCancel()
	_ = e.cleanupExpiredStoredResultsIfDue(cleanupCtx, c, namespace, time.Now())
	return nil
}

func (e *KubernetesJobCodeExecutor) retryStoreResultAfterInvalidCache(ctx context.Context, c crclient.Client, namespace, jobName string, req CodeExecutionRequest, configMap *corev1.ConfigMap) (bool, error) {
	_, found, err := e.loadStoredResult(ctx, c, namespace, jobName, req)
	if err != nil {
		return false, err
	}
	if found {
		return true, nil
	}

	created, err := createKubernetesCodeExecObject(ctx, c, configMap)
	if err != nil {
		if apierrors.IsForbidden(err) {
			return true, nil
		}
		return false, err
	}
	return created, nil
}

func (e *KubernetesJobCodeExecutor) cleanupExpiredStoredResultsIfDue(ctx context.Context, c crclient.Client, namespace string, now time.Time) error {
	if strings.TrimSpace(namespace) == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}

	if !e.cleanupState.shouldRun(namespace, now) {
		return nil
	}

	err := e.cleanupExpiredStoredResults(ctx, c, namespace, now)
	e.cleanupState.record(namespace, now)
	return err
}

func (e *KubernetesJobCodeExecutor) cleanupExpiredStoredResults(ctx context.Context, c crclient.Client, namespace string, now time.Time) error {
	if c == nil || strings.TrimSpace(namespace) == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.Add(-codeExecKubernetesStoredResultRetention)

	var storedResults corev1.ConfigMapList
	err := c.List(ctx, &storedResults,
		crclient.InNamespace(namespace),
		crclient.MatchingLabels{codeExecKubernetesLabelTool: codeExecToolName},
	)
	if err != nil {
		if apierrors.IsForbidden(err) {
			return nil
		}
		return fmt.Errorf("listing stored code_exec results: %w", err)
	}

	var deleteErrs []error
	for i := range storedResults.Items {
		configMap := &storedResults.Items[i]
		if configMap.Labels[codeExecKubernetesLabelJob] == "" || configMap.CreationTimestamp.IsZero() {
			continue
		}
		if !configMap.CreationTimestamp.Time.Before(cutoff) {
			continue
		}
		if err := c.Delete(ctx, configMap); err != nil && !apierrors.IsNotFound(err) {
			if apierrors.IsForbidden(err) {
				return errors.Join(deleteErrs...)
			}
			deleteErrs = append(deleteErrs, fmt.Errorf("deleting expired stored code_exec result %s/%s: %w", namespace, configMap.Name, err))
		}
	}
	return errors.Join(deleteErrs...)
}

func kubernetesCodeExecStoredResultIdentityMatches(stored crclient.Object, req CodeExecutionRequest) bool {
	if stored == nil || !codeExecKubernetesShouldPersistResult(req) {
		return false
	}
	annotations := stored.GetAnnotations()
	return strings.TrimSpace(annotations[codeExecKubernetesAnnotationRunID]) == strings.TrimSpace(req.RunID) &&
		strings.TrimSpace(annotations[codeExecKubernetesAnnotationInputHash]) == strings.TrimSpace(req.InputHash)
}

func kubernetesCodeExecStoredResultVersionMatches(stored crclient.Object) bool {
	if stored == nil {
		return false
	}
	annotations := stored.GetAnnotations()
	return strings.TrimSpace(annotations[codeExecKubernetesAnnotationResultVersion]) == codeExecKubernetesResultVersion
}

func (e *KubernetesJobCodeExecutor) jobNameForRequest(req CodeExecutionRequest) string {
	runID := strings.TrimSpace(req.RunID)
	if runID != "" {
		return codeExecKubernetesJobNameForRunID(runID)
	}
	return e.newJobName()
}

func codeExecKubernetesJobNameForRunID(runID string) string {
	suffix := strings.Trim(strings.ToLower(sanitizeKubernetesNamePart(runID)), "-")
	if suffix == "" {
		suffix = "run-" + codeExecSHA256HexString(runID)[:16]
	}

	maxSuffixLen := 63 - len(codeExecKubernetesJobPrefix)
	if maxSuffixLen < 1 {
		name := strings.TrimRight(codeExecKubernetesJobPrefix[:min(len(codeExecKubernetesJobPrefix), 63)], "-")
		if name == "" {
			return "orka-code-exec"
		}
		return name
	}
	if len(suffix) > maxSuffixLen {
		digest := codeExecSHA256HexString(runID)
		shortDigest := digest[:16]
		prefixLen := maxSuffixLen - len(shortDigest) - 1
		if prefixLen < 1 {
			return codeExecKubernetesJobPrefix + digest[:min(maxSuffixLen, len(digest))]
		}
		suffix = strings.TrimRight(suffix[:prefixLen], "-") + "-" + shortDigest
	}
	suffix = strings.Trim(suffix, "-")
	if suffix == "" {
		suffix = "run-" + codeExecSHA256HexString(runID)[:16]
	}
	return codeExecKubernetesJobPrefix + suffix
}

func (e *KubernetesJobCodeExecutor) newJobName() string {
	suffix := ""
	if e.randomSuffix != nil {
		suffix = e.randomSuffix()
	} else {
		suffix = randomCodeExecSuffix()
	}
	suffix = strings.Trim(strings.ToLower(sanitizeKubernetesNamePart(suffix)), "-")
	if suffix == "" {
		suffix = randomCodeExecSuffix()
	}
	name := codeExecKubernetesJobPrefix + suffix
	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimRight(name, "-")
	}
	return name
}

func randomCodeExecSuffix() string {
	buf := make([]byte, 5)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func sanitizeKubernetesNamePart(value string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return b.String()
}

func codeExecDeadlineSeconds(timeout time.Duration) int64 {
	if timeout <= 0 {
		timeout = defaultCodeExecTimeout
	}
	seconds := int64(timeout / time.Second)
	if timeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

func codeExecKubernetesImageForRequest(req CodeExecutionRequest) (string, error) {
	return codeExecKubernetesImageForScope(req.Language, codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesImageForScope(language, provider, tenant string) (string, error) {
	if image := codeExecScopedEnv(codeExecKubernetesImageEnv, provider, tenant, ""); image != "" {
		return image, nil
	}
	switch language {
	case codeLanguagePython, python3BinaryName:
		return codeExecScopedEnv(codeExecKubernetesPythonImageEnv, provider, tenant, "python:3.12-alpine"), nil
	case codeLanguageJavaScript, codeLanguageNode:
		return codeExecScopedEnv(codeExecKubernetesNodeImageEnv, provider, tenant, "node:22-alpine"), nil
	case codeLanguageBash, codeLanguageShell:
		return codeExecScopedEnv(codeExecKubernetesBashImageEnv, provider, tenant, "bash:5.2"), nil
	default:
		return "", fmt.Errorf("unsupported language: %s", language)
	}
}

func codeExecKubernetesCommand(language string, outputLimitBytes int64, jobName string) ([]string, error) {
	runner, err := codeExecKubernetesRunner(language)
	if err != nil {
		return nil, err
	}
	return []string{"/bin/sh", "-c", codeExecKubernetesWrapperScript(runner, outputLimitBytes, jobName), "orka-code-exec", codeExecKubernetesCodePath}, nil
}

func codeExecKubernetesRunner(language string) (string, error) {
	switch language {
	case codeLanguagePython, python3BinaryName:
		return python3BinaryName, nil
	case codeLanguageJavaScript, codeLanguageNode:
		return codeLanguageNode, nil
	case codeLanguageBash:
		return codeLanguageBash, nil
	case codeLanguageShell:
		return codeLanguageShell, nil
	default:
		return "", fmt.Errorf("unsupported language: %s", language)
	}
}

func codeExecKubernetesWrapperScript(runner string, outputLimitBytes int64, jobName string) string {
	if outputLimitBytes <= 0 {
		outputLimitBytes = defaultCodeExecOutputLimitBytes
	}
	markers := codeExecKubernetesLogMarkers(jobName)
	stdoutPath := tempDirPath + "/orka-code-exec-stdout-" + sanitizeKubernetesNamePart(jobName)
	stderrPath := tempDirPath + "/orka-code-exec-stderr-" + sanitizeKubernetesNamePart(jobName)

	return fmt.Sprintf(`set +e
stdout_file=%s
stderr_file=%s
limit=%d
%s "$1" >"$stdout_file" 2>"$stderr_file"
status=$?
emit_stream() {
	start_marker="$1"
	end_marker="$2"
	truncated_marker="$3"
	file="$4"
	printf '%%s\n' "$start_marker"
	if [ -f "$file" ]; then
		size=$(wc -c < "$file" 2>/dev/null | tr -d '[:space:]')
		case "$size" in ''|*[!0-9]*) size=0 ;; esac
		if [ "$size" -gt "$limit" ]; then
			head -c "$limit" "$file" 2>/dev/null || true
			omitted=$((size - limit))
			printf '\n%%s %%s %%s\n' "$truncated_marker" "$limit" "$omitted"
		else
			cat "$file" 2>/dev/null || true
		fi
	fi
	printf '\n%%s\n' "$end_marker"
}
emit_stream %s %s %s "$stdout_file"
emit_stream %s %s %s "$stderr_file"
rm -f "$stdout_file" "$stderr_file" 2>/dev/null || true
exit "$status"
`,
		codeExecKubernetesShellQuote(stdoutPath),
		codeExecKubernetesShellQuote(stderrPath),
		outputLimitBytes,
		codeExecKubernetesShellQuote(runner),
		codeExecKubernetesShellQuote(markers.stdoutStart),
		codeExecKubernetesShellQuote(markers.stdoutEnd),
		codeExecKubernetesShellQuote(markers.stdoutTruncated),
		codeExecKubernetesShellQuote(markers.stderrStart),
		codeExecKubernetesShellQuote(markers.stderrEnd),
		codeExecKubernetesShellQuote(markers.stderrTruncated),
	)
}

func codeExecKubernetesShellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func codeExecKubernetesLogMarkers(jobName string) kubernetesCodeExecLogMarkers {
	markerName := strings.ToUpper(strings.ReplaceAll(sanitizeKubernetesNamePart(jobName), "-", "_"))
	if markerName == "" {
		markerName = "UNKNOWN"
	}
	prefix := "__ORKA_CODE_EXEC_" + markerName + "_"
	return kubernetesCodeExecLogMarkers{
		stdoutStart:     prefix + "STDOUT_BEGIN__",
		stdoutEnd:       prefix + "STDOUT_END__",
		stdoutTruncated: prefix + "STDOUT_TRUNCATED__",
		stderrStart:     prefix + "STDERR_BEGIN__",
		stderrEnd:       prefix + "STDERR_END__",
		stderrTruncated: prefix + "STDERR_TRUNCATED__",
	}
}

func codeExecKubernetesPodLogLimitBytes(outputLimitBytes int64, jobName string) int64 {
	if outputLimitBytes <= 0 {
		outputLimitBytes = defaultCodeExecOutputLimitBytes
	}
	markers := codeExecKubernetesLogMarkers(jobName)
	markerOverhead := int64(len(markers.stdoutStart)+len(markers.stdoutEnd)+len(markers.stdoutTruncated)+len(markers.stderrStart)+len(markers.stderrEnd)+len(markers.stderrTruncated)) + 4096
	maxLimitBytes := int64(1 << 62)
	if outputLimitBytes > (maxLimitBytes-markerOverhead)/2 {
		return maxLimitBytes
	}
	return outputLimitBytes*2 + markerOverhead
}

func readKubernetesCodeExecRawLogs(reader io.Reader, limitBytes int64) (string, bool, error) {
	if limitBytes <= 0 {
		limitBytes = defaultCodeExecOutputLimitBytes + 1
	}
	var buf strings.Builder
	limitedReader := &io.LimitedReader{R: reader, N: limitBytes + 1}
	_, err := io.Copy(&buf, limitedReader)
	raw := buf.String()
	truncated := int64(len(raw)) > limitBytes
	if truncated {
		raw = raw[:limitBytes]
	}
	return raw, truncated, err
}

func parseKubernetesCodeExecLogs(rawLogs, jobName string, outputLimitBytes int64) kubernetesCodeExecLogOutput {
	markers := codeExecKubernetesLogMarkers(jobName)
	stdout, stdoutOK, stdoutTruncated := extractKubernetesCodeExecLogSection(rawLogs, markers.stdoutStart, markers.stdoutEnd, markers.stdoutTruncated, outputLimitBytes)
	stderr, stderrOK, stderrTruncated := extractKubernetesCodeExecLogSection(rawLogs, markers.stderrStart, markers.stderrEnd, markers.stderrTruncated, outputLimitBytes)
	if !stdoutOK && !stderrOK {
		buf := newCappedBuffer(outputLimitBytes)
		_, _ = buf.Write([]byte(rawLogs))
		return kubernetesCodeExecLogOutput{stdout: buf.String(), stdoutTruncated: buf.Truncated()}
	}
	return kubernetesCodeExecLogOutput{
		stdout:          stdout,
		stderr:          stderr,
		stdoutTruncated: stdoutTruncated,
		stderrTruncated: stderrTruncated,
	}
}

func extractKubernetesCodeExecLogSection(rawLogs, startMarker, endMarker, truncatedMarker string, outputLimitBytes int64) (string, bool, bool) {
	startNeedle := startMarker + "\n"
	startIndex := strings.Index(rawLogs, startNeedle)
	if startIndex < 0 {
		return "", false, false
	}
	sectionStart := startIndex + len(startNeedle)
	endNeedle := "\n" + endMarker
	endOffset := strings.Index(rawLogs[sectionStart:], endNeedle)
	if endOffset < 0 {
		return "", false, false
	}
	section := rawLogs[sectionStart : sectionStart+endOffset]
	section, wrapperTruncated := stripKubernetesCodeExecTruncationMarker(section, truncatedMarker, outputLimitBytes)
	if wrapperTruncated.truncated {
		return section + formatCodeExecTruncationMessage(wrapperTruncated.limit, wrapperTruncated.omitted), true, true
	}
	buf := newCappedBuffer(outputLimitBytes)
	_, _ = buf.Write([]byte(section))
	return buf.String(), true, buf.Truncated()
}

type kubernetesCodeExecTruncation struct {
	truncated bool
	limit     int64
	omitted   int64
}

func stripKubernetesCodeExecTruncationMarker(section, truncatedMarker string, outputLimitBytes int64) (string, kubernetesCodeExecTruncation) {
	lastLineStart := strings.LastIndex(section, "\n")
	lastLine := section
	content := ""
	if lastLineStart >= 0 {
		lastLine = section[lastLineStart+1:]
		content = section[:lastLineStart]
	}
	if !strings.HasPrefix(lastLine, truncatedMarker+" ") {
		return section, kubernetesCodeExecTruncation{}
	}
	fields := strings.Fields(lastLine)
	if len(fields) != 3 || fields[0] != truncatedMarker {
		return section, kubernetesCodeExecTruncation{}
	}
	limit, limitErr := strconv.ParseInt(fields[1], 10, 64)
	omitted, omittedErr := strconv.ParseInt(fields[2], 10, 64)
	if limitErr != nil || omittedErr != nil || limit < 0 || omitted < 0 {
		return section, kubernetesCodeExecTruncation{}
	}
	if limit == 0 && outputLimitBytes > 0 {
		limit = outputLimitBytes
	}
	return content, kubernetesCodeExecTruncation{truncated: true, limit: limit, omitted: omitted}
}

func formatCodeExecTruncationMessage(limit, omitted int64) string {
	return fmt.Sprintf("\n[truncated after %d bytes; %d bytes omitted]", limit, omitted)
}

func codeExecKubernetesResourcesForRequest(req CodeExecutionRequest) (corev1.ResourceRequirements, error) {
	return codeExecKubernetesResourcesForScope(codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesResourcesForScope(provider, tenant string) (corev1.ResourceRequirements, error) {
	cpuRequest, err := codeExecQuantityForScope(codeExecKubernetesCPURequestEnv, provider, tenant, "50m")
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	cpuLimit, err := codeExecQuantityForScope(codeExecKubernetesCPULimitEnv, provider, tenant, "500m")
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	memoryRequest, err := codeExecQuantityForScope(codeExecKubernetesMemoryRequestEnv, provider, tenant, "64Mi")
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	memoryLimit, err := codeExecQuantityForScope(codeExecKubernetesMemoryLimitEnv, provider, tenant, "256Mi")
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}

	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuRequest,
			corev1.ResourceMemory: memoryRequest,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuLimit,
			corev1.ResourceMemory: memoryLimit,
		},
	}, nil
}

func codeExecKubernetesResourceAuditForRequest(req CodeExecutionRequest) (map[string]string, error) {
	return codeExecKubernetesResourceAuditForScope(codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesResourceAuditForScope(provider, tenant string) (map[string]string, error) {
	resources, err := codeExecKubernetesResourcesForScope(provider, tenant)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"cpu_request":    resources.Requests.Cpu().String(),
		"cpu_limit":      resources.Limits.Cpu().String(),
		"memory_request": resources.Requests.Memory().String(),
		"memory_limit":   resources.Limits.Memory().String(),
	}, nil
}

func codeExecKubernetesRuntimeClassNameForRequest(req CodeExecutionRequest) string {
	return codeExecKubernetesRuntimeClassNameForScope(codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesRuntimeClassNameForScope(provider, tenant string) string {
	return codeExecScopedEnv(codeExecKubernetesRuntimeClassNameEnv, provider, tenant, "")
}

func codeExecKubernetesAppArmorProfileForRequest(req CodeExecutionRequest) (*corev1.AppArmorProfile, error) {
	return codeExecKubernetesAppArmorProfileForScope(codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesAppArmorProfileForScope(provider, tenant string) (*corev1.AppArmorProfile, error) {
	value, sourceEnv := codeExecScopedEnvValue(codeExecKubernetesAppArmorProfileEnv, provider, tenant)
	value = strings.TrimSpace(value)
	if sourceEnv == "" {
		sourceEnv = codeExecKubernetesAppArmorProfileEnv
	}
	normalized := strings.ToLower(value)
	switch normalized {
	case "", "0", falseStr, "no", "off", "disabled", "disable", "none":
		return nil, nil
	case "runtime/default", "runtime-default", "runtimedefault", "default":
		return &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}, nil
	case "unconfined":
		return &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}, nil
	}

	if strings.HasPrefix(normalized, "localhost/") {
		profile := strings.TrimSpace(value[len("localhost/"):])
		if profile == "" {
			return nil, fmt.Errorf("%s=%q is invalid: localhost profile name is required", sourceEnv, value)
		}
		return &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeLocalhost, LocalhostProfile: &profile}, nil
	}

	return nil, fmt.Errorf("%s=%q is invalid: expected empty/disabled/none/off, runtime/default, runtime-default, default, unconfined, or localhost/<profile>", sourceEnv, value)
}

func codeExecKubernetesNetworkPolicyEnabledForRequest(req CodeExecutionRequest) bool {
	return codeExecKubernetesNetworkPolicyEnabledForScope(codeExecRequestProviderScope(req), req.Tenant)
}

func codeExecKubernetesNetworkPolicyEnabledForScope(provider, tenant string) bool {
	value := strings.TrimSpace(strings.ToLower(codeExecScopedEnv(codeExecKubernetesNetworkPolicyEnv, provider, tenant, "")))
	if value == "" {
		return true
	}
	switch value {
	case "0", falseStr, "no", "off", "disabled":
		return false
	case "1", trueStr, "yes", "on", enabledString:
		return true
	default:
		return true
	}
}

func codeExecQuantityForScope(envName, provider, tenant, fallback string) (resource.Quantity, error) {
	value, sourceEnv := codeExecScopedEnvValue(envName, provider, tenant)
	if value == "" {
		value = fallback
		sourceEnv = envName
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%s=%q is invalid: %w", sourceEnv, value, err)
	}
	return quantity, nil
}

func defaultCodeExecNamespace() string {
	for _, envName := range []string{workerenv.PodNamespace, workerenv.OrkaNamespace, workerenv.Namespace} {
		if namespace := strings.TrimSpace(os.Getenv(envName)); namespace != "" {
			return namespace
		}
	}
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace
		}
	}
	return defaultNamespace
}

func (e *KubernetesJobCodeExecutor) waitForJob(ctx context.Context, clients kubernetesCodeExecClients, jobName string, req CodeExecutionRequest) CodeExecResult {
	pollInterval := e.pollInterval
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	jobObserved := false
	for {
		result, done, observed := e.checkJob(ctx, clients, jobName, req, jobObserved)
		jobObserved = jobObserved || observed
		if done {
			return result
		}

		select {
		case <-ctx.Done():
			return e.timeoutResult(clients, jobName, req.OutputLimitBytes)
		case <-ticker.C:
		}
	}
}

func (e *KubernetesJobCodeExecutor) checkJob(ctx context.Context, clients kubernetesCodeExecClients, jobName string, req CodeExecutionRequest, jobObserved bool) (CodeExecResult, bool, bool) {
	outputLimitBytes := req.OutputLimitBytes
	job := &batchv1.Job{}
	if err := clients.client.Get(ctx, types.NamespacedName{Namespace: clients.namespace, Name: jobName}, job); err != nil {
		if ctx.Err() != nil {
			return CodeExecResult{}, false, false
		}
		if apierrors.IsNotFound(err) && codeExecKubernetesShouldPersistResult(req) {
			stored, found, loadErr := e.loadStoredResult(ctx, clients.client, clients.namespace, jobName, req)
			if loadErr != nil {
				return CodeExecResult{Error: fmt.Sprintf("failed to read persisted kubernetes code_exec result: %v", loadErr), ExitCode: -1}, true, false
			}
			if found {
				return stored, true, false
			}
			if jobObserved {
				return CodeExecResult{Error: fmt.Sprintf("kubernetes code_exec job %s/%s disappeared before result was persisted", clients.namespace, jobName), ExitCode: -1}, true, false
			}
			return CodeExecResult{}, false, false
		}
		return CodeExecResult{Error: fmt.Sprintf("failed to get kubernetes code_exec job: %v", err), ExitCode: -1}, true, false
	}

	if job.Status.Succeeded > 0 || hasJobCondition(job, batchv1.JobComplete) {
		logs, err := e.readJobLogs(ctx, clients, jobName, outputLimitBytes)
		result := CodeExecResult{
			Output:          logs.stdout,
			Error:           logs.stderr,
			ExitCode:        0,
			OutputTruncated: logs.stdoutTruncated,
			ErrorTruncated:  logs.stderrTruncated,
		}
		if err != nil {
			result.Error = appendCodeExecError(result.Error, fmt.Sprintf("failed to read kubernetes code_exec logs: %v", err))
			result.ExitCode = -1
		}
		return result, true, true
	}

	if isKubernetesJobDeadlineExceeded(job) {
		return e.timeoutResult(clients, jobName, outputLimitBytes), true, true
	}

	if hasJobCondition(job, batchv1.JobFailed) || (job.Status.Failed > 0 && job.Status.Active == 0) {
		if ctx.Err() != nil {
			return e.timeoutResult(clients, jobName, outputLimitBytes), true, true
		}

		logs, logErr := e.readJobLogs(ctx, clients, jobName, outputLimitBytes)
		exitCode := e.containerExitCode(ctx, clients, jobName)
		if exitCode == 0 {
			exitCode = 1
		}
		result := CodeExecResult{
			Output:          logs.stdout,
			Error:           logs.stderr,
			ExitCode:        exitCode,
			OutputTruncated: logs.stdoutTruncated,
			ErrorTruncated:  logs.stderrTruncated,
		}
		failureMessage := kubernetesJobFailureMessage(job)
		if failureMessage == "" {
			failureMessage = "execution failed"
		}
		result.Error = appendCodeExecError(result.Error, failureMessage)
		if logErr != nil {
			result.Error = appendCodeExecError(result.Error, fmt.Sprintf("failed to read kubernetes code_exec logs: %v", logErr))
		}
		return result, true, true
	}

	return CodeExecResult{}, false, true
}

func (e *KubernetesJobCodeExecutor) timeoutResult(clients kubernetesCodeExecClients, jobName string, outputLimitBytes int64) CodeExecResult {
	logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logs, err := e.readJobLogs(logCtx, clients, jobName, outputLimitBytes)
	result := CodeExecResult{
		Output:          logs.stdout,
		Error:           appendCodeExecError(logs.stderr, "execution timed out"),
		ExitCode:        -1,
		TimedOut:        true,
		OutputTruncated: logs.stdoutTruncated,
		ErrorTruncated:  logs.stderrTruncated,
	}
	if err != nil {
		result.Error = appendCodeExecError(result.Error, fmt.Sprintf("failed to read kubernetes code_exec logs: %v", err))
	}
	return result
}

func (e *KubernetesJobCodeExecutor) readJobLogs(ctx context.Context, clients kubernetesCodeExecClients, jobName string, outputLimitBytes int64) (kubernetesCodeExecLogOutput, error) {
	pod, ok, err := e.firstJobPod(ctx, clients, jobName)
	if err != nil || !ok {
		return kubernetesCodeExecLogOutput{}, err
	}
	if clients.authorizePodLogs != nil {
		if err := clients.authorizePodLogs(ctx, clients.namespace, pod.Name); err != nil {
			return kubernetesCodeExecLogOutput{}, err
		}
	}

	streamer := e.logStreamer
	if streamer == nil {
		streamer = kubeClientPodLogStreamer{client: clients.kubeClient}
	}
	limitBytes := codeExecKubernetesPodLogLimitBytes(outputLimitBytes, jobName)
	reader, err := streamer.Stream(ctx, clients.namespace, pod.Name, &corev1.PodLogOptions{
		Container:  codeExecKubernetesContainerName,
		LimitBytes: &limitBytes,
	})
	if err != nil {
		return kubernetesCodeExecLogOutput{}, err
	}
	defer reader.Close() //nolint:errcheck

	rawLogs, _, err := readKubernetesCodeExecRawLogs(reader, limitBytes)
	logs := parseKubernetesCodeExecLogs(rawLogs, jobName, outputLimitBytes)
	return logs, err
}

func (e *KubernetesJobCodeExecutor) containerExitCode(ctx context.Context, clients kubernetesCodeExecClients, jobName string) int {
	pod, ok, err := e.firstJobPod(ctx, clients, jobName)
	if err != nil || !ok {
		return 1
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != codeExecKubernetesContainerName || status.State.Terminated == nil {
			continue
		}
		return int(status.State.Terminated.ExitCode)
	}
	return 1
}

func (e *KubernetesJobCodeExecutor) firstJobPod(ctx context.Context, clients kubernetesCodeExecClients, jobName string) (corev1.Pod, bool, error) {
	pods := &corev1.PodList{}
	if err := clients.client.List(ctx, pods, crclient.InNamespace(clients.namespace), crclient.MatchingLabels{codeExecKubernetesLabelJob: jobName}); err != nil {
		return corev1.Pod{}, false, err
	}
	if len(pods.Items) == 0 {
		return corev1.Pod{}, false, nil
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		left := pods.Items[i]
		right := pods.Items[j]
		if !left.CreationTimestamp.Equal(&right.CreationTimestamp) {
			return left.CreationTimestamp.Before(&right.CreationTimestamp)
		}
		return left.Name < right.Name
	})
	return pods.Items[0], true, nil
}

func hasJobCondition(job *batchv1.Job, conditionType batchv1.JobConditionType) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isKubernetesJobDeadlineExceeded(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		reason := strings.ToLower(strings.TrimSpace(condition.Reason))
		message := strings.ToLower(condition.Message)
		if reason == "deadlineexceeded" || strings.Contains(message, "active deadline") {
			return true
		}
	}
	return false
}

func kubernetesJobFailureMessage(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			if condition.Message != "" {
				return condition.Message
			}
			if condition.Reason != "" {
				return condition.Reason
			}
		}
	}
	return ""
}

func (e *KubernetesJobCodeExecutor) cleanupResources(c crclient.Client, resources *kubernetesCodeExecResources) {
	if c == nil || resources == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	deleteKubernetesCodeExecJob(cleanupCtx, c, resources.job)
	if resources.secret != nil {
		deleteKubernetesCodeExecObject(cleanupCtx, c, resources.secret)
	}
	if resources.serviceAccount != nil {
		deleteKubernetesCodeExecObject(cleanupCtx, c, resources.serviceAccount)
	}
	if resources.networkPolicy != nil {
		deleteKubernetesCodeExecObject(cleanupCtx, c, resources.networkPolicy)
	}
}

func deleteKubernetesCodeExecJob(ctx context.Context, c crclient.Client, job *batchv1.Job) {
	if job == nil {
		return
	}
	propagationPolicy := metav1.DeletePropagationBackground
	if err := c.Delete(ctx, job, &crclient.DeleteOptions{PropagationPolicy: &propagationPolicy}); err != nil && !apierrors.IsNotFound(err) {
		return
	}
}

func deleteKubernetesCodeExecObject(ctx context.Context, c crclient.Client, obj crclient.Object) {
	if obj == nil {
		return
	}
	if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return
	}
}

var _ CodeExecutor = (*KubernetesJobCodeExecutor)(nil)
