/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	gorruntime "runtime"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
	"github.com/orka-agents/orka/internal/workerenv"
)

func TestRepositoryMonitorValidationShellWrapperSuppressesOutputWithoutBreakingCommand(t *testing.T) {
	payload := `printf visible; printf hidden >&2`
	if gorruntime.GOOS == "linux" {
		payload += `; printf parent-stdout >"/proc/$PPID/fd/1"; printf parent-stderr >"/proc/$PPID/fd/2"`
	}
	commandFile := path.Join(t.TempDir(), "command.sh")
	if err := os.WriteFile(commandFile, []byte(payload), 0o400); err != nil {
		t.Fatalf("write validation command file: %v", err)
	}
	command := exec.Command(
		"/bin/sh", "-c", repositoryMonitorValidationShellWrapper,
		commandFile,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("validation shell wrapper changed command success: %v", err)
	}
	if len(output) != 0 {
		t.Fatalf("validation shell wrapper emitted suppressed output: %q", output)
	}

	unavailableCommandFile := path.Join(t.TempDir(), "unavailable.sh")
	if err := os.WriteFile(unavailableCommandFile, []byte(fmt.Sprintf("exit %d", workerenv.RepositoryValidationUnavailableExitCode)), 0o400); err != nil {
		t.Fatalf("write unavailable validation command file: %v", err)
	}
	command = exec.Command(
		"/bin/sh", "-c", repositoryMonitorValidationShellWrapper,
		unavailableCommandFile,
	)
	err = command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("reserved validation exit code was not remapped: %v", err)
	}
}

func TestRepositoryMonitorValidationJobIsReadOnlyAndNetworkGated(t *testing.T) {
	task := repositoryMonitorValidationRuntimeTask()
	task.Annotations[labels.AnnotationTransactionTokenSecret] = "injected-token-secret"
	job, err := setupJobBuilder().Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	assertRepositoryMonitorValidationWorkspaceMount(t, job)
	gate := assertRepositoryMonitorValidationInitContainers(t, job)
	assertRepositoryMonitorValidationVolumes(t, job, task)
	assertRepositoryMonitorValidationProcessIsolation(t, job, task)
	assertRepositoryMonitorValidationNetworkAndCredentialIsolation(t, job, gate)
	assertRepositoryMonitorValidationOutputAndStorage(t, job)
}

func TestRepositoryMonitorValidationJobRequiresTaskUIDForProcessIsolation(t *testing.T) {
	task := repositoryMonitorValidationRuntimeTask()
	task.UID = ""
	if _, err := setupJobBuilder().Build(context.Background(), task, nil, nil); err == nil || !strings.Contains(err.Error(), "task UID is required") {
		t.Fatalf("Build() error = %v, want missing Task UID rejection", err)
	}
}

func TestRepositoryMonitorValidationRunAsUserIsTaskScoped(t *testing.T) {
	first := repositoryMonitorValidationRuntimeTask()
	second := repositoryMonitorValidationRuntimeTask()
	second.UID = types.UID("different-validation-task-uid")
	firstUID, err := repositoryMonitorValidationRunAsUser(first)
	if err != nil {
		t.Fatal(err)
	}
	secondUID, err := repositoryMonitorValidationRunAsUser(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstUID == secondUID || firstUID < repositoryMonitorValidationUIDBase || secondUID < repositoryMonitorValidationUIDBase {
		t.Fatalf("validation runtime UIDs = %d and %d, want distinct high UIDs", firstUID, secondUID)
	}
}

func assertRepositoryMonitorValidationWorkspaceMount(t *testing.T, job *batchv1.Job) {
	t.Helper()
	workspace, ok := findVolumeMount(job.Spec.Template.Spec.Containers[0].VolumeMounts, "workspace")
	if !ok || !workspace.ReadOnly {
		t.Fatalf("validation workspace mount = %#v, want read-only", workspace)
	}
}

func assertRepositoryMonitorValidationInitContainers(t *testing.T, job *batchv1.Job) corev1.Container {
	t.Helper()
	if len(job.Spec.Template.Spec.InitContainers) != 4 {
		t.Fatalf("init containers = %#v, want workspace preparation, command materializer, network probe, and network gate", job.Spec.Template.Spec.InitContainers)
	}
	if job.Spec.Template.Spec.InitContainers[0].Name != workspacePreparationInitContainerName ||
		job.Spec.Template.Spec.InitContainers[1].Name != repositoryMonitorValidationCommandContainer ||
		job.Spec.Template.Spec.InitContainers[2].Name != repositoryMonitorValidationNetworkProbeContainer ||
		job.Spec.Template.Spec.InitContainers[3].Name != repositoryMonitorValidationNetworkGateContainer {
		t.Fatalf("init container order = %q, %q, %q, %q", job.Spec.Template.Spec.InitContainers[0].Name, job.Spec.Template.Spec.InitContainers[1].Name, job.Spec.Template.Spec.InitContainers[2].Name, job.Spec.Template.Spec.InitContainers[3].Name)
	}
	workspaceInit := job.Spec.Template.Spec.InitContainers[0]
	assertRepositoryMonitorValidationWorkspaceInitEnv(t, workspaceInit)
	materializer := job.Spec.Template.Spec.InitContainers[1]
	wantMaterializerArgs := []string{
		repositoryMonitorValidationCommandWorkerMode,
		path.Join(repositoryMonitorValidationCommandSourceMount, repositoryMonitorValidationCommandFile),
		path.Join(repositoryMonitorValidationCommandMount, repositoryMonitorValidationCommandFile),
		repositoryMonitorValidationTestCommandDigest,
	}
	if materializer.Image != setupJobBuilder().GeneralWorkerImage || !slices.Equal(materializer.Command, []string{"/worker"}) || !slices.Equal(materializer.Args, wantMaterializerArgs) {
		t.Fatalf("command materializer image/command/args = %q/%#v/%#v", materializer.Image, materializer.Command, materializer.Args)
	}
	if mount, ok := findVolumeMount(materializer.VolumeMounts, repositoryMonitorValidationCommandSourceVolume); !ok || !mount.ReadOnly {
		t.Fatalf("command materializer source mount = %#v, want read-only Secret mount", mount)
	}
	if mount, ok := findVolumeMount(materializer.VolumeMounts, repositoryMonitorValidationCommandVolume); !ok || mount.ReadOnly {
		t.Fatalf("command materializer destination mount = %#v, want writable EmptyDir mount", mount)
	}
	probe := job.Spec.Template.Spec.InitContainers[2]
	if probe.Image != setupJobBuilder().GeneralWorkerImage || len(probe.Command) != 1 || probe.Command[0] != "/worker" ||
		len(probe.Args) != 2 || probe.Args[0] != repositoryMonitorValidationNetworkProbeWorkerMode || probe.Args[1] != "github.com:443" {
		t.Fatalf("network probe image/command/args = %q/%#v/%#v", probe.Image, probe.Command, probe.Args)
	}
	gate := job.Spec.Template.Spec.InitContainers[3]
	if gate.Image != setupJobBuilder().GeneralWorkerImage || len(gate.Command) != 1 || gate.Command[0] != "/worker" {
		t.Fatalf("network gate image/command = %q/%#v", gate.Image, gate.Command)
	}
	if mount, ok := findVolumeMount(gate.VolumeMounts, repositoryMonitorValidationNetworkGateVolume); !ok || !mount.ReadOnly {
		t.Fatalf("network gate mount = %#v, want read-only ConfigMap mount", mount)
	}
	sandboxBinary := path.Join(repositoryMonitorValidationNetworkSandboxMount, repositoryMonitorValidationNetworkSandboxBinary)
	if len(gate.Args) != 3 || gate.Args[0] != repositoryMonitorValidationNetworkGateWorkerMode ||
		!strings.Contains(gate.Args[1], repositoryMonitorValidationNetworkGateKey) || gate.Args[2] != sandboxBinary {
		t.Fatalf("network gate args = %#v", gate.Args)
	}
	if mount, ok := findVolumeMount(gate.VolumeMounts, repositoryMonitorValidationNetworkSandboxVolume); !ok || mount.ReadOnly {
		t.Fatalf("network gate sandbox mount = %#v, want writable EmptyDir mount", mount)
	}
	return gate
}

func assertRepositoryMonitorValidationWorkspaceInitEnv(t *testing.T, workspaceInit corev1.Container) {
	t.Helper()
	if !slices.ContainsFunc(workspaceInit.Env, func(env corev1.EnvVar) bool {
		return env.Name == workerenv.GitRefShallow && env.Value == scheduledRunLabelValue
	}) {
		t.Fatalf("workspace init env = %#v, want controller-owned shallow ref marker", workspaceInit.Env)
	}
}

func assertRepositoryMonitorValidationVolumes(t *testing.T, job *batchv1.Job, task *corev1alpha1.Task) {
	t.Helper()
	foundGateVolume, foundSandboxVolume := false, false
	foundCommandSourceVolume, foundCommandVolume := false, false
	for i := range job.Spec.Template.Spec.Volumes {
		volume := &job.Spec.Template.Spec.Volumes[i]
		switch volume.Name {
		case repositoryMonitorValidationNetworkGateVolume:
			foundGateVolume = volume.ConfigMap != nil && volume.ConfigMap.Name == task.Name
		case repositoryMonitorValidationNetworkSandboxVolume:
			foundSandboxVolume = volume.EmptyDir != nil
		case repositoryMonitorValidationCommandSourceVolume:
			foundCommandSourceVolume = volume.Secret != nil &&
				volume.Secret.SecretName == tools.RepositoryValidationCommandSecretName(task.Name) &&
				len(volume.Secret.Items) == 1 && volume.Secret.Items[0].Key == tools.RepositoryValidationCommandSecretKey &&
				volume.Secret.Items[0].Path == repositoryMonitorValidationCommandFile
		case repositoryMonitorValidationCommandVolume:
			foundCommandVolume = volume.EmptyDir != nil && volume.EmptyDir.SizeLimit != nil &&
				volume.EmptyDir.SizeLimit.Cmp(repositoryValidationCommandLimit) == 0
		}
	}
	if !foundGateVolume || !foundSandboxVolume || !foundCommandSourceVolume || !foundCommandVolume {
		t.Fatalf("validation Job gate/sandbox/command-source/command volumes = %v/%v/%v/%v, want all", foundGateVolume, foundSandboxVolume, foundCommandSourceVolume, foundCommandVolume)
	}
	if job.Spec.Template.Spec.AutomountServiceAccountToken == nil || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatal("validation Pod must not automount a service account token")
	}
}

func assertRepositoryMonitorValidationProcessIsolation(t *testing.T, job *batchv1.Job, task *corev1alpha1.Task) {
	t.Helper()
	if job.Spec.Template.Spec.HostUsers != nil {
		t.Fatalf("validation Pod hostUsers = %v, want no user-namespace requirement", job.Spec.Template.Spec.HostUsers)
	}
	runtimeUID, err := repositoryMonitorValidationRunAsUser(task)
	if err != nil {
		t.Fatal(err)
	}
	if got := job.Spec.Template.Annotations[runtimePoolPIDsAnnotation]; got != "512" {
		t.Fatalf("validation PID limit annotation = %q, want 512", got)
	}
	podSecurity := job.Spec.Template.Spec.SecurityContext
	if podSecurity == nil || podSecurity.RunAsUser == nil || *podSecurity.RunAsUser != runtimeUID ||
		podSecurity.RunAsGroup == nil || *podSecurity.RunAsGroup != runtimeUID ||
		podSecurity.FSGroup == nil || *podSecurity.FSGroup != runtimeUID {
		t.Fatalf("validation Pod security context = %#v, want Task-scoped UID/GID %d", podSecurity, runtimeUID)
	}
	for _, container := range append(append([]corev1.Container{}, job.Spec.Template.Spec.InitContainers...), job.Spec.Template.Spec.Containers...) {
		if container.SecurityContext == nil || container.SecurityContext.RunAsUser == nil || *container.SecurityContext.RunAsUser != runtimeUID ||
			container.SecurityContext.RunAsGroup == nil || *container.SecurityContext.RunAsGroup != runtimeUID {
			t.Fatalf("container %q security context = %#v, want Task-scoped UID/GID %d", container.Name, container.SecurityContext, runtimeUID)
		}
	}
}

func assertRepositoryMonitorValidationNetworkAndCredentialIsolation(t *testing.T, job *batchv1.Job, gate corev1.Container) {
	t.Helper()
	wantTolerations := []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(300))},
	}
	if !slices.EqualFunc(job.Spec.Template.Spec.Tolerations, wantTolerations, func(a, b corev1.Toleration) bool {
		return a.Key == b.Key && a.Operator == b.Operator && a.Effect == b.Effect &&
			a.TolerationSeconds != nil && b.TolerationSeconds != nil && *a.TolerationSeconds == *b.TolerationSeconds
	}) {
		t.Fatalf("validation Pod tolerations = %#v, want explicit Kubernetes defaults", job.Spec.Template.Spec.Tolerations)
	}
	if gate.SecurityContext == nil || gate.SecurityContext.RunAsNonRoot == nil || !*gate.SecurityContext.RunAsNonRoot ||
		gate.SecurityContext.Capabilities == nil || len(gate.SecurityContext.Capabilities.Add) != 0 ||
		!slices.Contains(gate.SecurityContext.Capabilities.Drop, corev1.Capability("ALL")) {
		t.Fatalf("network gate security context = %#v, want baseline-compatible non-root container with all capabilities dropped", gate.SecurityContext)
	}
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Secret != nil && job.Spec.Template.Spec.Volumes[i].Secret.SecretName == "injected-token-secret" {
			t.Fatal("validation Pod mounted an injected transaction-token Secret")
		}
	}
	worker := job.Spec.Template.Spec.Containers[0]
	if worker.SecurityContext == nil || worker.SecurityContext.Capabilities == nil || len(worker.SecurityContext.Capabilities.Add) != 0 {
		t.Fatalf("validation worker security context = %#v, want no added capabilities", worker.SecurityContext)
	}
	if mount, ok := findVolumeMount(worker.VolumeMounts, repositoryMonitorValidationNetworkSandboxVolume); !ok || !mount.ReadOnly {
		t.Fatalf("validation worker sandbox mount = %#v, want read-only executable mount", mount)
	}
	if mount, ok := findVolumeMount(worker.VolumeMounts, repositoryMonitorValidationCommandVolume); !ok || !mount.ReadOnly || mount.MountPath != repositoryMonitorValidationCommandMount {
		t.Fatalf("validation worker command mount = %#v, want read-only verified command mount", mount)
	}
	if mount, ok := findVolumeMount(worker.VolumeMounts, repositoryMonitorValidationCommandSourceVolume); ok {
		t.Fatalf("validation worker mounted unverified command Secret: %#v", mount)
	}
}

func assertRepositoryMonitorValidationOutputAndStorage(t *testing.T, job *batchv1.Job) {
	t.Helper()
	worker := job.Spec.Template.Spec.Containers[0]
	wantCommand := []string{path.Join(repositoryMonitorValidationNetworkSandboxMount, repositoryMonitorValidationNetworkSandboxBinary)}
	wantArgs := []string{
		repositoryMonitorValidationSandboxWorkerMode,
		"/bin/sh",
		"-c",
		repositoryMonitorValidationShellWrapper,
		path.Join(repositoryMonitorValidationCommandMount, repositoryMonitorValidationCommandFile),
	}
	if !slices.Equal(worker.Command, wantCommand) || !slices.Equal(worker.Args, wantArgs) {
		t.Fatalf("validation worker command/args = %#v/%#v, want sandbox wrapper and fixed command file", worker.Command, worker.Args)
	}
	if strings.Contains(strings.Join(worker.Args, " "), repositoryMonitorValidationTestCommand) {
		t.Fatalf("validation worker args exposed the repository-selected command: %#v", worker.Args)
	}
	if worker.TerminationMessagePath != "/dev/null" {
		t.Fatalf("validation termination message path = %q, want /dev/null", worker.TerminationMessagePath)
	}
	for _, container := range append(append([]corev1.Container{}, job.Spec.Template.Spec.InitContainers...), job.Spec.Template.Spec.Containers...) {
		if got := container.Resources.Requests[corev1.ResourceEphemeralStorage]; got.Cmp(repositoryValidationStorageRequest) != 0 {
			t.Fatalf("container %q ephemeral request = %s, want %s", container.Name, got.String(), repositoryValidationStorageRequest.String())
		}
		if got := container.Resources.Limits[corev1.ResourceEphemeralStorage]; got.Cmp(repositoryValidationStorageLimit) != 0 {
			t.Fatalf("container %q ephemeral limit = %s, want %s", container.Name, got.String(), repositoryValidationStorageLimit.String())
		}
	}
	wantVolumeLimits := map[string]string{"tmp": "2Gi", "home": "2Gi", "workspace": "4Gi", repositoryMonitorValidationCommandVolume: "16Ki"}
	for i := range job.Spec.Template.Spec.Volumes {
		volume := &job.Spec.Template.Spec.Volumes[i]
		want, ok := wantVolumeLimits[volume.Name]
		if !ok {
			continue
		}
		if volume.EmptyDir == nil || volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.String() != want {
			t.Fatalf("volume %q size limit = %#v, want %s", volume.Name, volume.EmptyDir, want)
		}
		delete(wantVolumeLimits, volume.Name)
	}
	if len(wantVolumeLimits) != 0 {
		t.Fatalf("validation Job is missing bounded volumes: %#v", wantVolumeLimits)
	}
}

func TestRepositoryMonitorValidationUsesDurableProvenanceAfterMetadataMutation(t *testing.T) {
	ctx := context.Background()
	bindingStore := setupControllerSQLiteStore(t)
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-provenance")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-provenance-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	seedRepositoryMonitorValidationBindingForTest(t, ctx, bindingStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	validationTask.Spec.Type = corev1alpha1.TaskTypeAI
	validationTask.Spec.Image = ""
	validationTask.Spec.Workspace = nil
	delete(validationTask.Labels, labels.LabelPurpose)
	delete(validationTask.Annotations, labels.AnnotationRepositoryValidationImage)

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask, commandSecret).Build()
	reconciler := &TaskReconciler{
		Client:                       k8sClient,
		APIReader:                    k8sClient,
		RepositoryValidationBindings: bindingStore,
	}
	validation, err := reconciler.repositoryMonitorValidationTask(ctx, validationTask)
	if !validation || !errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("repositoryMonitorValidationTask() = (%v, %v), want durable classification and confinement failure", validation, err)
	}
}

func TestRepositoryMonitorValidationBoundOwnerReadErrorsRemainRetryable(t *testing.T) {
	for _, target := range []string{"monitor", "review-task"} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			bindingStore := setupControllerSQLiteStore(t)
			scheme := repositoryMonitorValidationRuntimeScheme(t)
			monitor := repositoryMonitorReviewIngestTestMonitor("validation-owner-read-" + target)
			monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
			reviewTask := repositoryMonitorReviewIngestTestTask("validation-owner-read-review-"+target, monitor.Name, 1, repositoryMonitorTestHeadSHA)
			repositoryMonitorBindValidationForTest(reviewTask)
			validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
			commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)
			seedRepositoryMonitorValidationBindingForTest(t, ctx, bindingStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)

			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, validationTask, commandSecret).Build()
			transient := errors.New("temporary API read failure")
			reader := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					switch target {
					case "monitor":
						if _, ok := obj.(*corev1alpha1.RepositoryMonitor); ok && key.Name == monitor.Name {
							return transient
						}
					case "review-task":
						if _, ok := obj.(*corev1alpha1.Task); ok && key.Name == reviewTask.Name {
							return transient
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			reconciler := &TaskReconciler{
				Client: base, APIReader: reader,
				RepositoryValidationBindings: bindingStore,
			}

			validation, err := reconciler.repositoryMonitorValidationTask(ctx, validationTask)
			if !validation || !errors.Is(err, transient) || errors.Is(err, errRepositoryMonitorValidationConfinement) {
				t.Fatalf("repositoryMonitorValidationTask() = (%v, %v), want retryable owner-read failure", validation, err)
			}
		})
	}
}

func TestRepositoryMonitorValidationHeuristicLookupErrorRemainsRetryable(t *testing.T) {
	ctx := context.Background()
	bindingErr := errors.New("validation binding store unavailable")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "integration-validation", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentRead,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(task).
		WithStatusSubresource(&corev1alpha1.Task{}).
		Build()
	reconciler := &TaskReconciler{
		Client: k8sClient,
		RepositoryValidationBindings: repositoryMonitorValidationBindingErrorStore{
			err: bindingErr,
		},
	}

	if _, err := reconciler.createTaskJob(ctx, task.DeepCopy(), nil, nil); !errors.Is(err, bindingErr) || errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("createTaskJob() error = %v, want retryable binding-store error", err)
	}
	current := &corev1alpha1.Task{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Fatalf("ordinary Task phase = %q, want Pending after retryable lookup failure", current.Status.Phase)
	}
}

func TestRepositoryMonitorValidationBypassesNamespaceTaskLimit(t *testing.T) {
	ctx := context.Background()
	bindingStore := setupControllerSQLiteStore(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-namespace-limit")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-namespace-limit-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	reviewTask.Status = corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning}
	validationTask := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	validationTask.Status.ResultRef = nil
	seedRepositoryMonitorValidationBindingForTest(t, ctx, bindingStore, monitor, reviewTask, validationTask, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, validationTask, repositoryMonitorValidationTestCommand)

	scheme := repositoryMonitorValidationRuntimeScheme(t)
	reconciler := newUnitReconciler(scheme, monitor, reviewTask, validationTask, commandSecret)
	reconciler.APIReader = reconciler.Client
	reconciler.RepositoryValidationBindings = bindingStore
	reconciler.MaxTasksPerNamespace = 1

	result, err := reconciler.handlePending(ctx, validationTask.DeepCopy())
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("handlePending() RequeueAfter = %v, want validation network gate requeue instead of namespace limit", result.RequeueAfter)
	}
}

func TestRepositoryMonitorValidationClassificationOutageRetriesOrdinaryTaskCompletion(t *testing.T) {
	ctx := context.Background()
	bindingErr := errors.New("validation binding store unavailable")
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ordinary-validation",
			Namespace: "default",
			UID:       types.UID("ordinary-validation-uid"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeContainer,
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent: corev1alpha1.WorkspaceIntentRead,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:    corev1alpha1.TaskPhaseRunning,
			Attempts: 1,
			JobName:  "ordinary-validation-job",
		},
	}
	reconciler := newUnitReconciler(repositoryMonitorValidationRuntimeScheme(t), task)
	reconciler.RepositoryValidationBindings = repositoryMonitorValidationBindingErrorStore{err: bindingErr}
	if err := reconciler.ResultStore.SaveResult(ctx, task.Namespace, task.Name, []byte("ordinary output")); err != nil {
		t.Fatal(err)
	}

	if _, err := reconciler.completeExecutedTask(ctx, task.DeepCopy(), corev1alpha1.TaskPhaseSucceeded, "completed"); !errors.Is(err, errRepositoryMonitorValidationClassificationUnavailable) {
		t.Fatalf("completeExecutedTask() error = %v, want retryable validation classification error", err)
	}
	current := &corev1alpha1.Task{}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseRunning || current.Status.ResultRef != nil {
		t.Fatalf("Task status after classification outage = phase %q result %#v, want Running without terminal result", current.Status.Phase, current.Status.ResultRef)
	}

	reconciler.RepositoryValidationBindings = setupControllerSQLiteStore(t)
	if _, err := reconciler.completeExecutedTask(ctx, current, corev1alpha1.TaskPhaseSucceeded, "completed"); err != nil {
		t.Fatalf("completeExecutedTask() after recovery error = %v", err)
	}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseSucceeded || current.Status.ResultRef == nil || !current.Status.ResultRef.Available {
		t.Fatalf("Task status after classification recovery = phase %q result %#v, want Succeeded with available result", current.Status.Phase, current.Status.ResultRef)
	}
}

func TestRepositoryMonitorValidationDeletionWaitsForForegroundJobCleanup(t *testing.T) {
	ctx := context.Background()
	task := repositoryMonitorValidationRuntimeTask()
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	job := repositoryMonitorValidationRuntimeJob(t, task, scheme, setupJobBuilder())
	task.Status.JobName = job.Name
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task, job).Build()
	var propagation *metav1.DeletionPropagation
	k8sClient := interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			if _, ok := object.(*batchv1.Job); ok {
				propagation = (&client.DeleteOptions{}).ApplyOptions(options).PropagationPolicy
			}
			return delegate.Delete(ctx, object, options...)
		},
	})
	reconciler := &TaskReconciler{Client: k8sClient}
	waiting, err := reconciler.cleanupDeletedTaskJob(ctx, task)
	if err != nil {
		t.Fatalf("cleanupDeletedTaskJob() error = %v", err)
	}
	if !waiting || propagation == nil || *propagation != metav1.DeletePropagationForeground {
		t.Fatalf("cleanupDeletedTaskJob() waiting/propagation = %v/%v, want true/Foreground", waiting, propagation)
	}
}

func TestRepositoryMonitorValidationResultCollectionSuppressesOutput(t *testing.T) {
	task := repositoryMonitorValidationRuntimeTask()
	resultStore := &repositoryMonitorValidationResultStoreProbe{}
	reconciler := &TaskReconciler{ResultStore: resultStore}
	if err := reconciler.collectResult(context.Background(), task); err != nil {
		t.Fatalf("collectResult() error = %v", err)
	}
	if resultStore.gets != 0 || resultStore.saves != 0 || resultStore.deletes != 0 || task.Status.ResultRef != nil {
		t.Fatalf("validation result collection touched durable output: gets=%d saves=%d deletes=%d resultRef=%#v", resultStore.gets, resultStore.saves, resultStore.deletes, task.Status.ResultRef)
	}
}

type repositoryMonitorValidationResultStoreProbe struct {
	gets    int
	saves   int
	deletes int
}

func (s *repositoryMonitorValidationResultStoreProbe) SaveResult(context.Context, string, string, []byte) error {
	s.saves++
	return nil
}

func (s *repositoryMonitorValidationResultStoreProbe) GetResult(context.Context, string, string) ([]byte, error) {
	s.gets++
	return nil, store.ErrNotFound
}

func (s *repositoryMonitorValidationResultStoreProbe) DeleteResult(context.Context, string, string) error {
	s.deletes++
	return nil
}

func TestRepositoryMonitorValidationJobRequiresExactOwnerAndSpec(t *testing.T) {
	task := repositoryMonitorValidationRuntimeTask()
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	builder := setupJobBuilder()
	expected, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(task, expected, scheme); err != nil {
		t.Fatal(err)
	}
	actual := expected.DeepCopy()
	actual.UID = types.UID("validation-job-uid")
	if err := defaultExpectedRepositoryMonitorValidationJob(actual, actual); err != nil {
		t.Fatal(err)
	}
	if err := validateRepositoryMonitorValidationJobAgainstExpected(task, actual, expected); err != nil {
		t.Fatalf("valid API-defaulted Job rejected: %v", err)
	}

	foreign := actual.DeepCopy()
	foreign.OwnerReferences[0].UID = types.UID("foreign-task-uid")
	if err := validateRepositoryMonitorValidationJobAgainstExpected(task, foreign, expected); !errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("foreign Job error = %v, want confinement failure", err)
	}

	mutated := actual.DeepCopy()
	mutated.Spec.Template.Spec.InitContainers[2].Args[1] = "attacker.example:443"
	if err := validateRepositoryMonitorValidationJobAgainstExpected(task, mutated, expected); !errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("mutated Job error = %v, want confinement failure", err)
	}

	mutatedSelector := actual.DeepCopy()
	mutatedSelector.Spec.Selector.MatchLabels[batchv1.ControllerUidLabel] = "foreign-job-uid"
	if err := validateRepositoryMonitorValidationJobAgainstExpected(task, mutatedSelector, expected); !errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("mutated Job selector error = %v, want confinement failure", err)
	}

	mutatedTemplateLabel := actual.DeepCopy()
	mutatedTemplateLabel.Spec.Template.Labels[batchv1.ControllerUidLabel] = "foreign-job-uid"
	if err := validateRepositoryMonitorValidationJobAgainstExpected(task, mutatedTemplateLabel, expected); !errors.Is(err, errRepositoryMonitorValidationConfinement) {
		t.Fatalf("mutated Job template label error = %v, want confinement failure", err)
	}
}

func TestRepositoryMonitorValidationPodRequiresExactJobOwnerAndSpec(t *testing.T) {
	task := repositoryMonitorValidationRuntimeTask()
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	job := repositoryMonitorValidationRuntimeJob(t, task, scheme, setupJobBuilder())

	for _, tt := range []struct {
		name      string
		mutate    func(*corev1.Pod)
		wantMatch bool
		wantErr   bool
	}{
		{name: "exact pod", wantMatch: true},
		{name: "system metadata", mutate: func(pod *corev1.Pod) {
			pod.Labels["cni.example.io/ready"] = "true"
			if pod.Annotations == nil {
				pod.Annotations = map[string]string{}
			}
			pod.Annotations["k8s.v1.cni.cncf.io/network-status"] = `[{"name":"default"}]`
		}, wantMatch: true},
		{name: "foreign owner", mutate: func(pod *corev1.Pod) {
			pod.OwnerReferences[0].UID = types.UID("foreign-job-uid")
		}},
		{name: "mutated template label", mutate: func(pod *corev1.Pod) {
			pod.Labels[labels.LabelTaskType] = string(corev1alpha1.TaskTypeAgent)
		}, wantErr: true},
		{name: "user namespace mutation", mutate: func(pod *corev1.Pod) {
			pod.Spec.HostUsers = new(false)
		}, wantErr: true},
		{name: "mutated gate", mutate: func(pod *corev1.Pod) {
			pod.Spec.InitContainers[3].Command = []string{"/bin/true"}
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := repositoryMonitorValidationRuntimePod(job)
			if tt.mutate != nil {
				tt.mutate(pod)
			}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task, pod).Build()
			reconciler := &TaskReconciler{Client: k8sClient, Scheme: scheme}
			matched, err := reconciler.repositoryMonitorValidationPod(context.Background(), task, job)
			if tt.wantErr && !errors.Is(err, errRepositoryMonitorValidationConfinement) {
				t.Fatalf("mutated Pod error = %v, want confinement failure", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("repositoryMonitorValidationPod() error = %v", err)
			}
			if got := matched != nil; got != tt.wantMatch {
				t.Fatalf("matched Pod = %#v, presence=%v want=%v", matched, got, tt.wantMatch)
			}
		})
	}

	t.Run("unrelated pod does not hide exact pod", func(t *testing.T) {
		exact := repositoryMonitorValidationRuntimePod(job)
		unrelated := exact.DeepCopy()
		unrelated.Name = job.Name + "-unrelated"
		unrelated.OwnerReferences[0].UID = types.UID("foreign-job-uid")
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task, unrelated, exact).Build()
		reconciler := &TaskReconciler{Client: k8sClient, Scheme: scheme}
		matched, err := reconciler.repositoryMonitorValidationPod(context.Background(), task, job)
		if err != nil || matched == nil || matched.Name != exact.Name {
			t.Fatalf("matched Pod = %#v, error = %v, want exact Job-owned Pod", matched, err)
		}
	})
}

func TestRepositoryMonitorValidationConfinementLifecycle(t *testing.T) {
	ctx := context.Background()
	bindingStore := setupControllerSQLiteStore(t)
	monitor := repositoryMonitorReviewIngestTestMonitor("validation-confinement")
	monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
	reviewTask := repositoryMonitorReviewIngestTestTask("validation-confinement-review", monitor.Name, 1, repositoryMonitorTestHeadSHA)
	repositoryMonitorBindValidationForTest(reviewTask)
	task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhasePending, repositoryMonitorTestHeadSHA)
	task.UID = types.UID("validation-confinement-task-uid")
	task.Annotations[labels.AnnotationWorkspaceInitContainer] = scheduledRunLabelValue
	seedRepositoryMonitorValidationBindingForTest(t, ctx, bindingStore, monitor, reviewTask, task, repositoryMonitorValidationTestCommand)
	commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, task, repositoryMonitorValidationTestCommand)
	scheme := repositoryMonitorValidationRuntimeScheme(t)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor, reviewTask, task, commandSecret).Build()
	builder := setupJobBuilder()
	builder.Client = k8sClient
	reconciler := &TaskReconciler{
		Client: k8sClient, APIReader: k8sClient, Scheme: scheme, JobBuilder: builder,
		RepositoryValidationBindings: bindingStore,
	}

	ready, err := reconciler.ensureRepositoryMonitorValidationNetworkGate(ctx, task)
	if err != nil || ready {
		t.Fatalf("first gate reconcile = (%v, %v), want created and pending", ready, err)
	}
	ready, err = reconciler.ensureRepositoryMonitorValidationNetworkGate(ctx, task)
	if err != nil || !ready {
		t.Fatalf("second gate reconcile = (%v, %v), want ready for Job creation", ready, err)
	}

	job := repositoryMonitorValidationRuntimeJob(t, task, scheme, builder)
	task.Status.Attempts = 1
	pod := repositoryMonitorValidationRuntimePod(job)
	pod.Status = corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
		Name:  workspacePreparationInitContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
	}}}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}

	if err := reconciler.reconcileRepositoryMonitorValidationConfinement(ctx, task, job); err != nil {
		t.Fatalf("wait for pre-policy network probe: %v", err)
	}
	policy := &networkingv1.NetworkPolicy{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, policy); !apierrors.IsNotFound(err) {
		t.Fatalf("NetworkPolicy before successful baseline probe error = %v, want not found", err)
	}
	pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses,
		corev1.ContainerStatus{
			Name:  repositoryMonitorValidationCommandContainer,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
		},
		corev1.ContainerStatus{
			Name:  repositoryMonitorValidationNetworkProbeContainer,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
		},
	)
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}

	if err := reconciler.reconcileRepositoryMonitorValidationConfinement(ctx, task, job); err != nil {
		t.Fatalf("create NetworkPolicy: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Spec.Ingress) != 0 || len(policy.Spec.Egress) != 0 || len(policy.Spec.PolicyTypes) != 2 {
		t.Fatalf("NetworkPolicy spec = %#v, want deny-all ingress and egress", policy.Spec)
	}
	gate := &corev1.ConfigMap{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, gate); err != nil {
		t.Fatal(err)
	}
	if gate.Data[repositoryMonitorValidationNetworkGateKey] != repositoryMonitorValidationNetworkGatePending {
		t.Fatalf("gate released in same reconcile as NetworkPolicy creation: %#v", gate.Data)
	}

	if err := reconciler.reconcileRepositoryMonitorValidationConfinement(ctx, task, job); err != nil {
		t.Fatalf("release network gate: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, gate); err != nil {
		t.Fatal(err)
	}
	if gate.Data[repositoryMonitorValidationNetworkGateKey] != repositoryMonitorValidationNetworkGateReady {
		t.Fatalf("gate state = %#v, want released", gate.Data)
	}

	if err := k8sClient.Delete(ctx, policy); err != nil {
		t.Fatal(err)
	}
	err = reconciler.reconcileRepositoryMonitorValidationConfinement(ctx, task, job)
	if !errors.Is(err, errRepositoryMonitorValidationConfinement) || !strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("missing released NetworkPolicy error = %v", err)
	}
}

func TestRepositoryMonitorValidationFailureOutcomeRequiresStartedWorker(t *testing.T) {
	for _, tt := range []struct {
		name              string
		workerFailed      bool
		workerUnavailable bool
		workerEvicted     bool
		workerTimedOut    bool
		wantExecutionInfo bool
	}{
		{name: "command materializer failure remains unavailable"},
		{name: "wrapper startup failure remains unavailable", workerUnavailable: true},
		{name: "evicted worker remains unavailable", workerEvicted: true},
		{name: "validation command failure records execution", workerFailed: true, wantExecutionInfo: true},
		{name: "validation command timeout records execution", workerTimedOut: true, wantExecutionInfo: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bindingStore := setupControllerSQLiteStore(t)
			monitor := repositoryMonitorReviewIngestTestMonitor("runtime-" + repositoryMonitorShortHash(tt.name))
			monitor.Spec.Validation.Image = repositoryMonitorValidationTestImage
			reviewTask := repositoryMonitorReviewIngestTestTask("review-"+repositoryMonitorShortHash(tt.name), monitor.Name, 1, repositoryMonitorTestHeadSHA)
			repositoryMonitorBindValidationForTest(reviewTask)
			reviewTask.Status.Phase = corev1alpha1.TaskPhaseRunning
			task := repositoryMonitorValidationTaskForTest(monitor, reviewTask, corev1alpha1.TaskPhaseRunning, repositoryMonitorTestHeadSHA)
			task.UID = types.UID("validation-task-" + repositoryMonitorShortHash(tt.name))
			task.Annotations[labels.AnnotationWorkspaceInitContainer] = scheduledRunLabelValue
			seedRepositoryMonitorValidationBindingForTest(t, ctx, bindingStore, monitor, reviewTask, task, repositoryMonitorValidationTestCommand)
			commandSecret := repositoryMonitorValidationCommandSecretForTest(reviewTask, task, repositoryMonitorValidationTestCommand)
			if tt.workerTimedOut {
				taskStarted := metav1.NewTime(time.Now().Add(-tools.RepositoryValidationTimeout - time.Minute))
				task.Status.StartTime = &taskStarted
			}
			scheme := repositoryMonitorValidationRuntimeScheme(t)
			builder := setupJobBuilder()
			job := repositoryMonitorValidationRuntimeJob(t, task, scheme, builder)
			task.Status.Phase = corev1alpha1.TaskPhaseRunning
			task.Status.Attempts = 1
			task.Status.JobName = job.Name

			pod := repositoryMonitorValidationRuntimePod(job)
			gate := repositoryMonitorValidationGateConfigMap(task)
			objects := []client.Object{monitor, reviewTask, task, commandSecret, job, pod, gate}
			if tt.workerFailed || tt.workerUnavailable || tt.workerEvicted || tt.workerTimedOut {
				startedAt := metav1.NewTime(time.Now().Add(-time.Minute))
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{Name: workspacePreparationInitContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
					{Name: repositoryMonitorValidationCommandContainer, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
					{Name: repositoryMonitorValidationNetworkProbeContainer, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
					{Name: repositoryMonitorValidationNetworkGateContainer, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
				}
				if tt.workerFailed || tt.workerUnavailable || tt.workerEvicted {
					exitCode := int32(1)
					if tt.workerUnavailable {
						exitCode = int32(workerenv.RepositoryValidationUnavailableExitCode)
					}
					job.Status.Failed = 1
					if tt.workerEvicted {
						pod.Status.Reason = "Evicted"
					}
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
						Name: "worker",
						State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							ExitCode: exitCode, Reason: "Error", StartedAt: startedAt,
						}},
					}}
				} else {
					job.Status.Active = 1
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
						Name: "worker", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: startedAt}},
					}}
				}
				gate.Data[repositoryMonitorValidationNetworkGateKey] = repositoryMonitorValidationNetworkGateReady
				objects = append(objects, repositoryMonitorValidationNetworkPolicy(task))
			} else {
				job.Status.Failed = 1
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{
						Name:  workspacePreparationInitContainerName,
						State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}},
					},
					{
						Name: repositoryMonitorValidationCommandContainer,
						State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							ExitCode: int32(workerenv.RepositoryValidationUnavailableExitCode), Reason: "Error",
						}},
					},
				}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name:  "worker",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
				}}
			}

			reconciler := newUnitReconciler(scheme, objects...)
			builder.Client = reconciler.Client
			reconciler.JobBuilder = builder
			reconciler.APIReader = reconciler.Client
			reconciler.RepositoryValidationBindings = bindingStore
			if _, err := reconciler.handleRunning(ctx, task); err != nil {
				t.Fatalf("handleRunning() error = %v", err)
			}
			updated := &corev1alpha1.Task{}
			if err := reconciler.Get(ctx, client.ObjectKeyFromObject(task), updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
				t.Fatalf("phase = %q, want Failed", updated.Status.Phase)
			}
			if got := updated.Status.ExecutionOutcome != nil; got != tt.wantExecutionInfo {
				t.Fatalf("execution outcome = %#v, presence = %v, want %v", updated.Status.ExecutionOutcome, got, tt.wantExecutionInfo)
			}
		})
	}
}

func repositoryMonitorValidationRuntimeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"batch":        batchv1.AddToScheme,
		"coordination": coordinationv1.AddToScheme,
		"core":         corev1.AddToScheme,
		"networking":   networkingv1.AddToScheme,
		"orka":         corev1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	return scheme
}

func repositoryMonitorValidationRuntimeJob(t *testing.T, task *corev1alpha1.Task, scheme *runtime.Scheme, builder *JobBuilder) *batchv1.Job {
	t.Helper()
	job, err := builder.Build(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(task, job, scheme); err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("validation-job-uid")
	if err := defaultExpectedRepositoryMonitorValidationJob(job, job); err != nil {
		t.Fatal(err)
	}
	return job
}

func repositoryMonitorValidationRuntimePod(job *batchv1.Job) *corev1.Pod {
	template := job.Spec.Template.DeepCopy()
	pod := &corev1.Pod{ObjectMeta: template.ObjectMeta, Spec: template.Spec}
	pod.Name = job.Name + "-pod"
	pod.Namespace = job.Namespace
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job"))}
	return pod
}

func repositoryMonitorValidationRuntimeTask() *corev1alpha1.Task {
	controller := true
	const monitorName = "repository-monitor"
	const parentName = "repository-review"
	parentUID := types.UID("review-task-uid")
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "repository-review-validation",
			Namespace: "default",
			UID:       types.UID("validation-task-uid"),
			Labels: map[string]string{
				labels.LabelCreatedBy: repositoryMonitorTaskCreatedBy,
				labels.LabelPurpose:   repositoryMonitorValidationPurpose,
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName:                    parentName,
				labels.AnnotationParentTaskUID:                     string(parentUID),
				labels.AnnotationRepositoryMonitorName:             monitorName,
				labels.AnnotationRepositoryValidationImage:         repositoryMonitorValidationTestImage,
				labels.AnnotationRepositoryValidationCommandDigest: repositoryMonitorValidationTestCommandDigest,
				labels.AnnotationWorkspaceInitContainer:            scheduledRunLabelValue,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: corev1alpha1.GroupVersion.String(), Kind: "Task",
				Name: parentName, UID: parentUID, Controller: &controller,
			}},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   repositoryMonitorValidationTestImage,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{"exit 125"},
			Workspace: &corev1alpha1.WorkspaceConfig{
				Intent:  corev1alpha1.WorkspaceIntentRead,
				GitRepo: "https://github.com/example/repository.git",
				Ref:     strings.Repeat("a", 40),
			},
		},
	}
}
