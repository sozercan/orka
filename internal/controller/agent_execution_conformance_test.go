/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func TestEnsureAgentExecutionBindingClassifiesExternalConformanceFailures(t *testing.T) {
	for _, tt := range []struct {
		name             string
		intent           corev1alpha1.WorkspaceIntent
		mutateRuntime    func(*corev1alpha1.AgentRuntime)
		removeAuthSecret bool
		permanent        bool
		wantMessage      string
	}{
		{
			name: "workspace intent mismatch", intent: corev1alpha1.WorkspaceIntentWrite, permanent: true,
			wantMessage: `profile workspace intent "read" does not match Task intent "write"`,
		},
		{
			name: "runtime not ready", wantMessage: "current-generation v2 conformance",
			mutateRuntime: func(runtime *corev1alpha1.AgentRuntime) { runtime.Status.Ready = false },
		},
		{
			name: "conformance generation stale", wantMessage: "current-generation v2 conformance",
			mutateRuntime: func(runtime *corev1alpha1.AgentRuntime) { runtime.Status.ObservedGeneration-- },
		},
		{
			name: "observed profile stale", wantMessage: "exact observed runtime identity/profile",
			mutateRuntime: func(runtime *corev1alpha1.AgentRuntime) {
				runtime.Status.ObservedCapabilities.RuntimeProfileDigest = ""
			},
		},
		{
			name: "authentication Secret not ready", removeAuthSecret: true, wantMessage: "not found",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newExternalACPDispatchFixture(t)
			task := bindingTestTask()
			task.Spec.AgentRef.Name = fixture.agent.Name
			task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: append([]string{}, fixture.mcpPolicy.AllowedTools...)}
			task.Status.Phase = corev1alpha1.TaskPhasePending
			if _, err := fixture.reconciler.resolveAgentExecutionCandidate(fixture.ctx, task, fixture.agent); err != nil {
				t.Fatalf("unmodified external runtime fixture is not admissible: %v", err)
			}
			if tt.intent != "" {
				task.Spec.Workspace.Intent = tt.intent
			}
			if err := fixture.client.Create(fixture.ctx, task); err != nil {
				t.Fatal(err)
			}
			if tt.mutateRuntime != nil {
				runtime := fixture.runtime.DeepCopy()
				tt.mutateRuntime(runtime)
				if err := fixture.client.Status().Update(fixture.ctx, runtime); err != nil {
					t.Fatal(err)
				}
			}
			if tt.removeAuthSecret {
				secret := &corev1.Secret{}
				key := client.ObjectKey{Namespace: task.Namespace, Name: fixture.runtime.Spec.ClientAuth.ControllerBearerTokenSecretRef.Name}
				if err := fixture.client.Get(fixture.ctx, key, secret); err != nil {
					t.Fatal(err)
				}
				if err := fixture.client.Delete(fixture.ctx, secret); err != nil {
					t.Fatal(err)
				}
			}
			_, candidateErr := fixture.reconciler.resolveAgentExecutionCandidate(fixture.ctx, task, fixture.agent)
			if candidateErr == nil || !strings.Contains(candidateErr.Error(), tt.wantMessage) {
				t.Fatalf("candidate error = %v, want %q", candidateErr, tt.wantMessage)
			}
			result, err, handled := fixture.reconciler.ensureAgentExecutionBinding(fixture.ctx, task, fixture.agent)
			if err != nil || !handled {
				t.Fatalf("ensureAgentExecutionBinding() = result=%#v handled=%v err=%v", result, handled, err)
			}
			current := &corev1alpha1.Task{}
			if err := fixture.client.Get(fixture.ctx, client.ObjectKeyFromObject(task), current); err != nil {
				t.Fatal(err)
			}
			if current.Status.AgentExecutionBinding != nil || fixture.createCalls.Load() != 0 || fixture.deleteCalls.Load() != 0 {
				t.Fatal("inadmissible Task obtained an execution binding or mutated the runtime")
			}
			if tt.permanent {
				if result.RequeueAfter != 0 {
					t.Fatalf("permanent mismatch requeued for %v instead of failing immediately", result.RequeueAfter)
				}
				assertExternalBindingDriftFailedBeforeQueue(t, fixture, current, tt.wantMessage)
			} else if result.RequeueAfter != 5*time.Second || current.Status.Phase != corev1alpha1.TaskPhasePending || current.Status.Execution != nil {
				t.Fatalf("transient conformance failure did not remain Pending with a retry: result=%#v status=%#v", result, current.Status)
			}
			if isPermanentACPAgentConfigurationError(candidateErr) != tt.permanent {
				t.Fatalf("candidate error permanent = %v, want %v", isPermanentACPAgentConfigurationError(candidateErr), tt.permanent)
			}
		})
	}
}
