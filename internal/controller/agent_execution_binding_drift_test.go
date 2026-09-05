/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
)

type externalRuntimeGetErrorReader struct {
	client.Reader
	err error
}

func (r externalRuntimeGetErrorReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*corev1alpha1.AgentRuntime); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestEnsureAgentExecutionBindingFrozenExternalDriftBeforeQueueIsTerminal(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	bound := bindExternalTaskBeforeQueue(t, fixture, "external-prequeue-runtime-drift", "runtime drift")

	runtime := &corev1alpha1.AgentRuntime{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(fixture.runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Generation++
	if err := fixture.client.Update(fixture.ctx, runtime); err != nil {
		t.Fatal(err)
	}

	result, err, handled := fixture.reconciler.ensureAgentExecutionBinding(fixture.ctx, bound, nil)
	if err != nil || !handled || result.RequeueAfter != 0 {
		t.Fatalf("ensureAgentExecutionBinding() = result=%#v handled=%v err=%v, want terminal handling", result, handled, err)
	}
	assertExternalBindingDriftFailedBeforeQueue(t, fixture, bound, "identity or generation changed after binding")
}

func TestHandleBoundAgentTaskPendingFrozenExternalSecretDriftBeforeQueueIsTerminal(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	bound := bindExternalTaskBeforeQueue(t, fixture, "external-prequeue-secret-drift", "Secret drift")

	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Namespace: defaultNS,
		Name:      fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name,
	}
	if err := fixture.client.Get(fixture.ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.Delete(fixture.ctx, secret); err != nil {
		t.Fatal(err)
	}
	secret.ResourceVersion = ""
	secret.UID = types.UID("replacement-prequeue-runtime-auth-uid")
	if err := fixture.client.Create(fixture.ctx, secret); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.reconciler.handleBoundAgentTaskPending(fixture.ctx, bound)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatalf("handleBoundAgentTaskPending() = result=%#v err=%v, want terminal handling", result, err)
	}
	assertExternalBindingDriftFailedBeforeQueue(t, fixture, bound, "authentication authority changed after binding")
}

func TestEnsureAgentExecutionBindingRetriesTransientExternalRuntimeRead(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	bound := bindExternalTaskBeforeQueue(t, fixture, "external-prequeue-read-retry", "retry read")
	fixture.reconciler.APIReader = externalRuntimeGetErrorReader{
		Reader: fixture.client,
		err:    errors.New("temporary AgentRuntime read failure"),
	}

	result, err, handled := fixture.reconciler.ensureAgentExecutionBinding(fixture.ctx, bound, nil)
	if err != nil || !handled || result.RequeueAfter != 5*time.Second {
		t.Fatalf("ensureAgentExecutionBinding() = result=%#v handled=%v err=%v, want transient retry", result, handled, err)
	}

	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(bound), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhasePending || current.Status.Execution != nil {
		t.Fatalf("transient read failure changed Task status: %#v", current.Status)
	}
}

func bindExternalTaskBeforeQueue(
	t *testing.T,
	fixture *externalACPDispatchFixture,
	name string,
	prompt string,
) *corev1alpha1.Task {
	t.Helper()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  defaultNS,
			Name:       name,
			UID:        types.UID(name + "-uid"),
			Generation: 1,
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:         corev1alpha1.TaskTypeAgent,
			AgentRef:     &corev1alpha1.AgentReference{Name: fixture.agent.Name},
			Prompt:       prompt,
			AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: append([]string{}, fixture.mcpPolicy.AllowedTools...)},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	if err := fixture.client.Create(fixture.ctx, task); err != nil {
		t.Fatal(err)
	}
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if result, err, handled := fixture.reconciler.ensureAgentExecutionBinding(fixture.ctx, current, fixture.agent); err != nil || handled {
		t.Fatalf("initial ensureAgentExecutionBinding() = result=%#v handled=%v err=%v", result, handled, err)
	}
	bound := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), bound); err != nil {
		t.Fatal(err)
	}
	if bound.Status.AgentExecutionBinding == nil ||
		bound.Status.AgentExecutionBinding.Backend != corev1alpha1.AgentExecutionBackendExternalEndpoint ||
		bound.Status.Execution != nil {
		t.Fatalf("pre-queue external binding = %#v; Task status = %#v", bound.Status.AgentExecutionBinding, bound.Status)
	}
	return bound
}

func assertExternalBindingDriftFailedBeforeQueue(
	t *testing.T,
	fixture *externalACPDispatchFixture,
	task *corev1alpha1.Task,
	wantMessage string,
) {
	t.Helper()
	current := &corev1alpha1.Task{}
	if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != corev1alpha1.TaskPhaseFailed || current.Status.Execution == nil ||
		current.Status.Execution.State != corev1alpha1.TaskExecutionStateFailed ||
		current.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("InvalidRuntimeProfile") ||
		current.Status.Execution.Attempt != 0 || current.Status.Execution.PromptID != "" ||
		current.Status.Execution.RequestDigest != "" ||
		!strings.Contains(current.Status.Execution.Message, wantMessage) {
		t.Fatalf("pre-queue binding drift Task settlement = %#v", current.Status)
	}
	if fixture.createCalls.Load() != 0 {
		t.Fatalf("external runtime received %d mutating requests", fixture.createCalls.Load())
	}

	attemptKey := store.PromptAttemptKey{
		Namespace: current.Namespace,
		TaskUID:   string(current.UID),
		Attempt:   1,
		PromptID:  fmt.Sprintf("prompt-%s-1", current.UID),
	}
	attemptID, err := attemptKey.CanonicalID()
	if err != nil {
		t.Fatal(err)
	}
	if attempt, err := fixture.controlStore.GetPromptAttempt(fixture.ctx, attemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pre-queue binding drift PromptAttempt = %#v, err=%v, want not found", attempt, err)
	}
}
