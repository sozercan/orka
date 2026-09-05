/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	sandboxextv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	workspacev1alpha1 "github.com/orka-agents/orka/api/workspace/v1alpha1"
	"github.com/orka-agents/orka/internal/events"
	"github.com/orka-agents/orka/internal/harness"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/outboundaccess"
	"github.com/orka-agents/orka/internal/store"
	storekube "github.com/orka-agents/orka/internal/store/kube"
	"github.com/orka-agents/orka/internal/store/sqlite"
	storetest "github.com/orka-agents/orka/internal/store/storetest"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/testutil"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	staleResourceLabelKey   = "stale"
	staleResourceLabelValue = scheduledRunLabelValue
	testSubstrateActorID    = "actor-1"
)

// newTestScheme creates a scheme with all types needed for unit tests.
func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(s)
	_ = workspacev1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = batchv1.AddToScheme(s)
	_ = coordinationv1.AddToScheme(s)
	_ = rbacv1.AddToScheme(s)
	_ = sandboxextv1beta1.AddToScheme(s)
	return s
}

// newUnitReconciler builds a TaskReconciler backed by a fake client.
func newUnitReconciler(scheme *runtime.Scheme, objs ...client.Object) *TaskReconciler {
	fb := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&corev1alpha1.Task{}, &corev1alpha1.Agent{}, &corev1alpha1.AgentRuntime{},
			&corev1alpha1.ControllerEpoch{}, &corev1alpha1.PromptAttempt{}, &corev1alpha1.RuntimeSessionControl{},
			&corev1alpha1.BranchClaim{}, &corev1alpha1.Publication{}, &corev1alpha1.ExternalEffect{},
		).
		WithIndex(&corev1.Event{}, eventInvolvedObjectNameField, eventInvolvedObjectNameIndex).
		WithIndex(&corev1.Event{}, eventReasonField, eventReasonIndex)
	if len(objs) > 0 {
		fb = fb.WithObjects(objs...)
	}
	fc := fb.Build()

	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		panic(err)
	}
	ss := sqlite.NewStore(db, ":memory:")
	sessionManager := NewSessionManager(ss)
	sessionManager.SetGatewayEventStore(ss)
	return &TaskReconciler{
		Client:              fc,
		Scheme:              scheme,
		JobBuilder:          NewJobBuilder(fc),
		SessionManager:      sessionManager,
		Recorder:            record.NewFakeRecorder(100),
		ResultStore:         ss,
		MessageStore:        ss,
		PlanStore:           ss,
		ExecutionEventStore: ss,
	}
}

type failingGetSessionStore struct {
	store.SessionStore
	err error
}

func (s failingGetSessionStore) GetSession(context.Context, string, string) (*store.SessionRecord, error) {
	return nil, s.err
}

type failingDeletePlanStore struct {
	store.PlanStore
	err error
}

func (s failingDeletePlanStore) DeletePlan(context.Context, string, string) error {
	return s.err
}

type failingExecutionEventStore struct {
	err error
}

func (s failingExecutionEventStore) AppendExecutionEvent(context.Context, *store.ExecutionEvent) (*store.ExecutionEvent, error) {
	return nil, s.err
}

func (s failingExecutionEventStore) ListExecutionEvents(context.Context, store.ExecutionEventFilter) ([]store.ExecutionEvent, error) {
	return nil, s.err
}

func (s failingExecutionEventStore) ListSessionExecutionEvents(context.Context, store.SessionExecutionEventFilter) ([]store.SessionExecutionEvent, int64, error) {
	return nil, 0, s.err
}

func (s failingExecutionEventStore) GetLatestExecutionEventSeq(context.Context, string, string, string) (int64, error) {
	return 0, s.err
}

func (s failingExecutionEventStore) DeleteExecutionEvents(context.Context, string, string, string) error {
	return s.err
}

// ---------------------------------------------------------------------------
// isAutonomousTask
// ---------------------------------------------------------------------------

func TestIsAutonomousTask_NoAgentRef(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	if r.isAutonomousTask(context.Background(), task) {
		t.Error("expected false when agentRef is nil")
	}
}

func TestIsAutonomousTask_AgentNotFound(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "missing"},
		},
	}
	if r.isAutonomousTask(context.Background(), task) {
		t.Error("expected false when agent does not exist")
	}
}

func TestIsAutonomousTask_CoordinationNil(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "default"},
	}
	r := newUnitReconciler(scheme, agent)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1"},
		},
	}
	if r.isAutonomousTask(context.Background(), task) {
		t.Error("expected false when coordination is nil")
	}
}

func TestIsAutonomousTask_AutonomousTrue(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous: true,
			},
		},
	}
	r := newUnitReconciler(scheme, agent)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1"},
		},
	}
	if !r.isAutonomousTask(context.Background(), task) {
		t.Error("expected true when autonomous is enabled")
	}
}

func TestIsAutonomousTask_CrossNamespace(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "other"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous: true,
			},
		},
	}
	r := newUnitReconciler(scheme, agent)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1", Namespace: "other"},
		},
	}
	if !r.isAutonomousTask(context.Background(), task) {
		t.Error("expected true when cross-namespace agent has autonomous enabled")
	}
}

// ---------------------------------------------------------------------------
// resolveAgent
// ---------------------------------------------------------------------------

func TestResolveAgent_NilRef(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	agent, err := r.resolveAgent(context.Background(), task)
	if err != nil || agent != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", agent, err)
	}
}

func TestResolveAgent_Found(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "default"},
	}
	r := newUnitReconciler(scheme, agent)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1"},
		},
	}
	got, err := r.resolveAgent(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "a1" {
		t.Errorf("expected agent name a1, got %s", got.Name)
	}
}

func TestResolveAgent_NotFound(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "missing"},
		},
	}
	_, err := r.resolveAgent(context.Background(), task)
	if err == nil {
		t.Error("expected error when agent not found")
	}
}

func TestResolveAgent_NamespaceIsolation(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "other"},
	}
	r := newUnitReconciler(scheme, agent)
	r.EnforceNamespaceIsolation = true
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1", Namespace: "other"},
		},
	}
	_, err := r.resolveAgent(context.Background(), task)
	if err == nil {
		t.Error("expected error for cross-namespace agent with isolation enforced")
	}
}

func TestResolveAgent_CrossNamespaceAllowed(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "other"},
	}
	r := newUnitReconciler(scheme, agent)
	r.EnforceNamespaceIsolation = false
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a1", Namespace: "other"},
		},
	}
	got, err := r.resolveAgent(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "a1" {
		t.Errorf("expected a1, got %s", got.Name)
	}
}

// ---------------------------------------------------------------------------
// resolveProviderRef (pure logic, no client needed)
// ---------------------------------------------------------------------------

func TestResolveProviderRef_AgentTask(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent}}
	if ref := r.resolveProviderRef(task, nil); ref != nil {
		t.Error("expected nil for agent tasks")
	}
}

func TestResolveProviderRef_TaskAI(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "task-provider"},
			},
		},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			ProviderRef: &corev1alpha1.ProviderReference{Name: "agent-provider"},
		},
	}
	ref := r.resolveProviderRef(task, agent)
	if ref == nil || ref.Name != "task-provider" {
		t.Errorf("expected task-level provider, got %v", ref)
	}
}

func TestResolveProviderRef_AgentFallback(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			ProviderRef: &corev1alpha1.ProviderReference{Name: "agent-provider"},
		},
	}
	ref := r.resolveProviderRef(task, agent)
	if ref == nil || ref.Name != "agent-provider" {
		t.Errorf("expected agent-level provider, got %v", ref)
	}
}

func TestResolveProviderRef_NilEverything(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	if ref := r.resolveProviderRef(task, nil); ref != nil {
		t.Errorf("expected nil, got %v", ref)
	}
}

// ---------------------------------------------------------------------------
// validateTaskAgentCompatibility (pure logic)
// ---------------------------------------------------------------------------

func TestValidateTaskAgentCompatibility_AgentTaskNoAgent(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	if err := r.validateTaskAgentCompatibility(task, nil); err == nil {
		t.Error("expected error for agent task without agentRef")
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskNoRuntime(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil {
		t.Error("expected error when agent has no runtime")
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskCopilotRuntime(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot}}}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want Copilot accepted", err)
	}
}
func TestValidateTaskAgentCompatibility_AgentTaskOpencodeRuntime(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Model: testOpenCodeModelConfig(),
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeOpencode,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want OpenCode accepted", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskOpencodeRejectsSecretRef(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{
		Model: testOpenCodeModelConfig(),
		Runtime: &corev1alpha1.AgentCLIRuntime{
			Type:            corev1alpha1.AgentRuntimeOpencode,
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		},
		SecretRef: &corev1.LocalObjectReference{Name: "legacy-opencode-secret"},
	}}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "does not support agent secretRef") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want OpenCode secretRef rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskOpencodeRejectsReasoningEffort(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{
		Model: testOpenCodeModelConfig(),
		Runtime: &corev1alpha1.AgentCLIRuntime{
			Type:                   corev1alpha1.AgentRuntimeOpencode,
			ContractVersion:        new(corev1alpha1.AgentRuntimeContractHarnessV2),
			DefaultReasoningEffort: agentReasoningEffortHigh,
		},
	}}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "does not support spec.runtime.defaultReasoningEffort") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want OpenCode reasoning-effort rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskOpencodeRejectsSubstitutionModel(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{
		Model: &corev1alpha1.ModelConfig{Name: "{file:/proc/self/environ}"},
		Runtime: &corev1alpha1.AgentCLIRuntime{
			Type:            corev1alpha1.AgentRuntimeOpencode,
			ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		},
	}}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "substitution braces") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want substitution rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskOpencodeRuntimeRequiresModel(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
		Type:            corev1alpha1.AgentRuntimeOpencode,
		ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
	}}}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "opencode runtime requires spec.model.name") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want missing model rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_ReadOnlyCopilotRejected(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{labels.AnnotationAgentReadOnly: scheduledRunLabelValue}},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot}}}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "GITHUB_TOKEN can mutate GitHub") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want read-only credential rejection", err)
	}
}
func TestValidateTaskAgentCompatibility_ReadOnlyOpencodeAccepted(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{labels.AnnotationAgentReadOnly: scheduledRunLabelValue}},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "review"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Model:   testOpenCodeModelConfig(),
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeOpencode},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want read-only OpenCode accepted", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRejectsApprovalRequiredTools(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "approval-runtime-agent"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeClaude},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:               true,
				Autonomous:            true,
				ApprovalRequiredTools: []string{"dispatch_work_order"},
			},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil ||
		!strings.Contains(err.Error(), "only supported for type: ai") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want runtime approval rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_BuiltInRuntimeRejectsCredentialSecretRefs(t *testing.T) {
	for _, runtimeType := range []corev1alpha1.AgentRuntimeType{
		corev1alpha1.AgentRuntimeCodex,
		corev1alpha1.AgentRuntimeClaude,
		corev1alpha1.AgentRuntimeCopilot,
	} {
		for _, refOwner := range []string{"agent", "task"} {
			t.Run(string(runtimeType)+"/"+refOwner, func(t *testing.T) {
				r := &TaskReconciler{}
				task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
				agent := &corev1alpha1.Agent{
					ObjectMeta: metav1.ObjectMeta{Name: "a1"},
					Spec: corev1alpha1.AgentSpec{
						Runtime: &corev1alpha1.AgentCLIRuntime{Type: runtimeType},
					},
				}
				switch refOwner {
				case "agent":
					agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: "agent-creds"}
				case "task":
					task.Spec.SecretRef = &corev1alpha1.SecretReference{Name: "task-creds"}
				}

				err := r.validateTaskAgentCompatibility(task, agent)
				wantError := fmt.Sprintf("does not support %s secretRef", refOwner)
				if err == nil || !strings.Contains(err.Error(), wantError) {
					t.Fatalf("validateTaskAgentCompatibility() error = %v, want %q", err, wantError)
				}
			})
		}
	}
}

func TestValidateTaskAgentCompatibility_ProviderBackedCredentialSecretRefsRemainValid(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Type:      corev1alpha1.TaskTypeAI,
			Prompt:    "do stuff",
			SecretRef: &corev1alpha1.SecretReference{Name: "task-creds"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			ProviderRef: &corev1alpha1.ProviderReference{Name: "provider"},
			SecretRef:   &corev1.LocalObjectReference{Name: "agent-creds"},
		},
	}

	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v", err)
	}
}

func TestValidateTaskAgentCompatibility_RuntimeRefRejectsCredentialSecretRefs(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*corev1alpha1.Task, *corev1alpha1.Agent)
		wantError string
	}{
		{
			name: "agent secretRef",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.SecretRef = &corev1.LocalObjectReference{Name: "agent-creds"}
			},
			wantError: "agent secretRef",
		},
		{
			name: "task secretRef",
			mutate: func(task *corev1alpha1.Task, _ *corev1alpha1.Agent) {
				task.Spec.SecretRef = &corev1alpha1.SecretReference{Name: "task-creds"}
			},
			wantError: "task secretRef",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &TaskReconciler{}
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "a1"},
				Spec: corev1alpha1.AgentSpec{
					Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
				},
			}
			tt.mutate(task, agent)
			err := r.validateTaskAgentCompatibility(task, agent)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateTaskAgentCompatibility() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateTaskAgentCompatibility_RuntimeRefRejectsLegacyRestrictions(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*corev1alpha1.Task, *corev1alpha1.Agent)
		wantError string
	}{
		{
			name: "agent defaultAllowedTools",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowedTools = []string{"read_incident"}
			},
			wantError: "defaultAllowedTools",
		},
		{
			name: "agent explicitly empty defaultAllowedTools",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowedTools = []string{}
			},
			wantError: "defaultAllowedTools",
		},
		{
			name: "agent defaultAllowBash",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				allow := false
				agent.Spec.Runtime.DefaultAllowBash = &allow
			},
			wantError: "defaultAllowBash",
		},
		{
			name: "agent defaultReasoningEffort",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultReasoningEffort = "high"
			},
			wantError: "defaultReasoningEffort",
		},
		{
			name: "task disallowedTools",
			mutate: func(task *corev1alpha1.Task, _ *corev1alpha1.Agent) {
				task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{DisallowedTools: []string{"write"}}
			},
			wantError: "disallowedTools",
		},
		{
			name: "task allowBash",
			mutate: func(task *corev1alpha1.Task, _ *corev1alpha1.Agent) {
				allow := false
				task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowBash: &allow}
			},
			wantError: "allowBash",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &TaskReconciler{}
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "a1"},
				Spec: corev1alpha1.AgentSpec{
					Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
				},
			}
			tt.mutate(task, agent)
			err := r.validateTaskAgentCompatibility(task, agent)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateTaskAgentCompatibility() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateHarnessV2RuntimeRefAgentTaskRestrictionsRejectsUnsupportedOverrides(t *testing.T) {
	disabled := false
	tests := []struct {
		name      string
		mutate    func(*corev1alpha1.Task, *corev1alpha1.Agent)
		wantError string
	}{
		{
			name: "agent model object",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Model = &corev1alpha1.ModelConfig{}
			},
			wantError: "Agent.spec.model",
		},
		{
			name: "agent system prompt",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: "ignored prompt"}
			},
			wantError: "Agent.spec.systemPrompt",
		},
		{
			name: "agent skills",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Skills = []corev1alpha1.SkillReference{{Name: "ignored-skill"}}
			},
			wantError: "Agent.spec.skills",
		},
		{
			name: "agent enabled tool",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Tools = []corev1alpha1.ToolReference{{Name: "ignored-tool"}, {Name: "disabled-tool", Enabled: &disabled}}
			},
			wantError: "enabled Agent.spec.tools",
		},
		{
			name: "agent defaultMaxTurns",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				maxTurns := int32(20)
				agent.Spec.Runtime.DefaultMaxTurns = &maxTurns
			},
			wantError: "defaultMaxTurns",
		},
		{
			name: "agent explicitly empty defaultAllowedTools",
			mutate: func(_ *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				agent.Spec.Runtime.DefaultAllowedTools = []string{}
			},
			wantError: "defaultAllowedTools",
		},
		{
			name: "task maxTurns",
			mutate: func(task *corev1alpha1.Task, _ *corev1alpha1.Agent) {
				maxTurns := int32(20)
				task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: &maxTurns}
			},
			wantError: "maxTurns",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"}}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "a1"},
				Spec: corev1alpha1.AgentSpec{
					Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
				},
			}
			tt.mutate(task, agent)
			err := validateHarnessV2RuntimeRefAgentTaskRestrictions(task, agent)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateHarnessV2RuntimeRefAgentTaskRestrictions() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateHarnessV2RuntimeRefAgentTaskRestrictionsAcceptsPersistedHistoricalDefault(t *testing.T) {
	defaultMaxTurns := int32(50)
	agent := &corev1alpha1.Agent{
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				RuntimeRef:      &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"},
				DefaultMaxTurns: &defaultMaxTurns,
			},
		},
	}

	if err := validateHarnessV2RuntimeRefAgentTaskRestrictions(nil, agent); err != nil {
		t.Fatalf("validateHarnessV2RuntimeRefAgentTaskRestrictions() error = %v, want persisted historical default accepted", err)
	}
}

func TestValidatePlannedRuntimeRefAgentTaskRestrictionsUsesResolvedContract(t *testing.T) {
	tests := []struct {
		name      string
		contract  corev1alpha1.AgentRuntimeContractVersion
		wantPath  agentExecutionPath
		wantError string
		mutate    func(*corev1alpha1.Task, *corev1alpha1.Agent)
	}{
		{
			name:     "harness v1 preserves legacy overrides",
			contract: corev1alpha1.AgentRuntimeContractHarnessV1,
			wantPath: agentExecutionPathHarnessV1,
			mutate: func(task *corev1alpha1.Task, agent *corev1alpha1.Agent) {
				defaultMaxTurns := int32(50)
				taskMaxTurns := int32(7)
				agent.Spec.SystemPrompt = &corev1alpha1.PromptSource{Inline: "frozen system prompt"}
				agent.Spec.Skills = []corev1alpha1.SkillReference{{Name: "legacy-skill"}}
				agent.Spec.Tools = []corev1alpha1.ToolReference{{Name: "legacy-tool"}}
				agent.Spec.Runtime.DefaultMaxTurns = &defaultMaxTurns
				task.Spec.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{MaxTurns: &taskMaxTurns}
			},
		},
		{
			name:      "harness v2 rejects model override",
			contract:  corev1alpha1.AgentRuntimeContractHarnessV2,
			wantPath:  agentExecutionPathExternal,
			wantError: "Agent.spec.model",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := plannerExternalRuntime()
			runtime.Name = "custom-runtime"
			contract := tt.contract
			runtime.Spec.ContractVersion = &contract
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Namespace: defaultNS},
				Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: defaultNS},
				Spec: corev1alpha1.AgentSpec{
					Model:   &corev1alpha1.ModelConfig{Name: "configured-model"},
					Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtime.Name}},
				},
			}
			if tt.mutate != nil {
				tt.mutate(task, agent)
			}
			r := newUnitReconciler(newTestScheme(), runtime)
			r.ACPRuntimeEnabled = true
			r.HarnessV1Enabled = true

			if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
				t.Fatalf("validateTaskAgentCompatibility() error = %v", err)
			}
			plan := r.planAgentExecution(context.Background(), task, agent)
			if plan.path != tt.wantPath {
				t.Fatalf("plan path = %q, want %q (plan=%#v)", plan.path, tt.wantPath, plan)
			}
			err := validatePlannedRuntimeRefAgentTaskRestrictions(task, agent, plan)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validatePlannedRuntimeRefAgentTaskRestrictions() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validatePlannedRuntimeRefAgentTaskRestrictions() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateTaskAgentCompatibility_RuntimeRefAllowsBrokeredAllowedToolsAndDisabledAgentTools(t *testing.T) {
	disabled := false
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type:         corev1alpha1.TaskTypeAgent,
		Prompt:       "do stuff",
		AgentRuntime: &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"read_incident"}},
	}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Tools:   []corev1alpha1.ToolReference{{Name: "disabled-tool", Enabled: &disabled}},
			Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v", err)
	}
}

func TestValidateTaskAgentCompatibility_RuntimeRefRejectsPriorTaskRef(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{
			Type:         corev1alpha1.TaskTypeAgent,
			Prompt:       "do stuff",
			PriorTaskRef: &corev1alpha1.PriorTaskReference{Name: "prior"},
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
		},
	}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "priorTaskRef") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want priorTaskRef rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_ReadOnlyRuntimeRefRejected(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{labels.AnnotationAgentReadOnly: scheduledRunLabelValue}},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "review"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want conformant runtimeRef compatibility", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRuntimeRefValid(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"}},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want nil for runtimeRef", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRuntimeTypeAndRefRejected(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:       corev1alpha1.AgentRuntimeCodex,
				RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "custom-runtime"},
			},
		},
	}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "both runtime.type and runtime.runtimeRef") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want type/runtimeRef conflict", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRuntimeNeitherTypeNorRefRejected(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{},
		},
	}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil || !strings.Contains(err.Error(), "exactly one of type or runtimeRef") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want missing type/runtimeRef rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRuntimeAndProvider(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime:     &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
			ProviderRef: &corev1alpha1.ProviderReference{Name: "p1"},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil {
		t.Error("expected error when runtime and providerRef are both set")
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskAgentExecutionWorkspace(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true},
			},
		},
	}
	err := r.validateTaskAgentCompatibility(task, agent)
	if err == nil {
		t.Fatal("expected error when agent execution workspace is enabled")
	}
	if !strings.Contains(err.Error(), "Task.spec.execution.workspace") {
		t.Fatalf("expected Task.spec.execution.workspace guidance, got %q", err.Error())
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskRuntimeAndModelProvider(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do stuff"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
			Model:   &corev1alpha1.ModelConfig{Provider: "openai"},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil {
		t.Error("expected error when runtime and model.provider are both set")
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskNoPrompt(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil {
		t.Error("expected error when prompt is empty for agent task")
	}
}

func TestValidateTaskAgentCompatibility_AgentTaskValid(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "do it"},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestValidateTaskAgentCompatibility_AITaskWithRuntime(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "a1"},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: "copilot"},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil {
		t.Error("expected error for AI task referencing agent with runtime")
	}
}

func TestValidateTaskAgentCompatibility_RequestApprovalToolRequiresAutonomous(t *testing.T) {
	for _, tt := range []struct {
		name  string
		task  *corev1alpha1.Task
		agent *corev1alpha1.Agent
	}{
		{
			name: "agent tool",
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI}},
			agent: &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"},
				Spec: corev1alpha1.AgentSpec{
					Tools: []corev1alpha1.ToolReference{{Name: "request_approval"}},
				},
			},
		},
		{
			name: "task tool",
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAI,
				AI:   &corev1alpha1.AISpec{Tools: []string{"request_approval"}},
			}},
			agent: &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &TaskReconciler{}
			if err := r.validateTaskAgentCompatibility(tt.task, tt.agent); err == nil ||
				!strings.Contains(err.Error(), "enabled autonomous") {
				t.Fatalf("validateTaskAgentCompatibility() error = %v, want autonomous request_approval rejection", err)
			}
		})
	}
}

func TestValidateTaskAgentCompatibility_RequestApprovalAllowedForAutonomous(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI}}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"},
		Spec: corev1alpha1.AgentSpec{
			Tools: []corev1alpha1.ToolReference{{Name: "request_approval"}},
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:    true,
				Autonomous: true,
			},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err != nil {
		t.Fatalf("validateTaskAgentCompatibility() error = %v", err)
	}
}

func TestValidateTaskAgentCompatibility_ApprovalRequiredToolsRequireAutonomous(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:               true,
				ApprovalRequiredTools: []string{"dispatch_work_order"},
			},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil ||
		!strings.Contains(err.Error(), "enabled autonomous") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want autonomous approval rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_ApprovalRequiredToolsRequireCoordinationEnabled(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:            true,
				ApprovalRequiredTools: []string{"dispatch_work_order"},
			},
		},
	}
	if err := r.validateTaskAgentCompatibility(task, agent); err == nil ||
		!strings.Contains(err.Error(), "enabled autonomous") {
		t.Fatalf("validateTaskAgentCompatibility() error = %v, want enabled autonomous approval rejection", err)
	}
}

func TestValidateTaskAgentCompatibility_ApprovalRequiredToolsRejectBuiltIns(t *testing.T) {
	for _, toolName := range []string{"request_approval", "create_container_task", "web_search", "file_read", "web_fetch", "list_issues", "get_issue", "list_pull_requests", "recall_memory", "search_transcript", "delegate_task", "send_message", "check_messages", "post_review_comment", "check_pr_review_marker", "comment_on_issue", "update_agent"} {
		t.Run(toolName, func(t *testing.T) {
			r := &TaskReconciler{}
			task := &corev1alpha1.Task{
				Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "approval-agent"},
				Spec: corev1alpha1.AgentSpec{
					Coordination: &corev1alpha1.CoordinationConfig{
						Enabled:               true,
						Autonomous:            true,
						ApprovalRequiredTools: []string{toolName},
					},
				},
			}
			if err := r.validateTaskAgentCompatibility(task, agent); err == nil ||
				!strings.Contains(err.Error(), "cannot include built-in tool") {
				t.Fatalf("validateTaskAgentCompatibility() error = %v, want built-in rejection", err)
			}
		})
	}
}

func TestValidateTaskAgentCompatibility_ContainerTask(t *testing.T) {
	r := &TaskReconciler{}
	task := &corev1alpha1.Task{
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	if err := r.validateTaskAgentCompatibility(task, nil); err != nil {
		t.Errorf("expected no error for container task, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// validateExecutionWorkspace (pure logic)
// ---------------------------------------------------------------------------

func TestValidateExecutionWorkspace(t *testing.T) {
	executionWorkspace := func(mutators ...func(*corev1alpha1.ExecutionWorkspaceSpec)) *corev1alpha1.ExecutionWorkspaceSpec {
		// ACP RuntimeSessions run in controller-rendered sandbox templates, so a
		// valid request omits templateRef entirely.
		ws := &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}
		for _, mutate := range mutators {
			mutate(ws)
		}
		return ws
	}
	substrateTemplateRef := func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
		ws.TemplateRef = &corev1alpha1.WorkspaceTemplateReference{Name: "default"}
	}
	_ = substrateTemplateRef

	tests := []struct {
		name                        string
		agentSandboxEnabled         bool
		substrateEnabled            bool
		acpWorkspaceDispatchEnabled bool
		workspaceProviderAPIEnabled bool
		task                        *corev1alpha1.Task
		agentSandboxConfig          AgentSandboxConfig
		substrateConfig             SubstrateConfig
		wantErr                     string
	}{
		{
			name: "nil execution",
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
			}},
		},
		{
			name: "workspace disabled",
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: false},
				},
			}},
		},
		{
			name: "classRef workspace provider API disabled",
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					ClassRef: &corev1alpha1.WorkspaceClassReference{Name: "coding-v1"},
				}},
			}},
			wantErr: "requires the workspace provider API",
		},
		{
			name:                        "classRef admitted for agent tasks",
			workspaceProviderAPIEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					ClassRef: &corev1alpha1.WorkspaceClassReference{Name: "coding-v1"},
				}},
			}},
		},
		{
			name:                        "classRef rejected for non-agent tasks",
			workspaceProviderAPIEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAI,
				Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					ClassRef: &corev1alpha1.WorkspaceClassReference{Name: "coding-v1"},
				}},
			}},
			wantErr: "only supported for type: agent tasks",
		},
		{
			name:                "non-agent task",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAI,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(),
				},
			}},
			wantErr: "only supported for type: agent",
		},
		{
			name:                "unsupported reusePolicy",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicy("forever")
					}),
				},
			}},
			wantErr: "unsupported execution workspace reusePolicy",
		},
		{
			name:                "unsupported cleanupPolicy",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicy("archive")
					}),
				},
			}},
			wantErr: "unsupported execution workspace cleanupPolicy",
		},
		{
			name:             "substrate Task validation does not require legacy bootstrap secret before dispatch gate",
			substrateEnabled: true,
			substrateConfig: SubstrateConfig{
				APIInsecureSkipVerify: true,
			},
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(substrateTemplateRef, func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
					}),
				},
			}},
		},
		{
			name:             "substrate poolRef accepted",
			substrateEnabled: true,
			substrateConfig: SubstrateConfig{
				APIInsecureSkipVerify: true,
				BootstrapSecretName:   testSubstrateBootstrapSecretName,
				BootstrapSecretKey:    testSubstrateBootstrapSecretKey,
			},
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(substrateTemplateRef, func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.Provider = corev1alpha1.WorkspaceProviderSubstrate
						ws.PoolRef = &corev1alpha1.SubstrateActorPoolReference{Name: "codex-pool"}
					}),
				},
			}},
		},
		{
			name:                "session reuse without sessionRef",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
					}),
				},
			}},
			wantErr: acpWorkspaceTestSessionReferenceRequiredError,
		},
		{
			name:                "session reuse with empty sessionRef name",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type:       corev1alpha1.TaskTypeAgent,
				SessionRef: &corev1alpha1.SessionReference{Name: ""},
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
					}),
				},
			}},
			wantErr: acpWorkspaceTestSessionReferenceRequiredError,
		},
		{
			name:                "valid defaults",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type: corev1alpha1.TaskTypeAgent,
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(),
				},
			}},
		},
		{
			name:                "valid session reuse",
			agentSandboxEnabled: true,
			task: &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
				Type:       corev1alpha1.TaskTypeAgent,
				SessionRef: &corev1alpha1.SessionReference{Name: "session-1"},
				Execution: &corev1alpha1.ExecutionSpec{
					Workspace: executionWorkspace(func(ws *corev1alpha1.ExecutionWorkspaceSpec) {
						ws.ReusePolicy = corev1alpha1.WorkspaceReusePolicySession
						ws.CleanupPolicy = corev1alpha1.WorkspaceCleanupPolicyRetain
					}),
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &TaskReconciler{
				AgentSandboxEnabled:         tt.agentSandboxEnabled,
				SubstrateEnabled:            tt.substrateEnabled,
				ACPWorkspaceDispatchEnabled: tt.acpWorkspaceDispatchEnabled,
				WorkspaceProviderAPIEnabled: tt.workspaceProviderAPIEnabled,
				AgentSandboxConfig:          tt.agentSandboxConfig,
				SubstrateConfig:             tt.substrateConfig,
			}

			err := r.validateExecutionWorkspace(tt.task)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestValidateExecutionWorkspaceDefersACPProviderChecksUntilContractRouting(t *testing.T) {
	task := &corev1alpha1.Task{Spec: corev1alpha1.TaskSpec{
		Type: corev1alpha1.TaskTypeAgent,
		Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
			Enabled: true,
			TemplateRef: &corev1alpha1.WorkspaceTemplateReference{
				Name: "legacy-harness-template",
			},
		}},
	}}
	r := &TaskReconciler{AgentSandboxEnabled: true}

	if err := r.validateExecutionWorkspace(task); err != nil {
		t.Fatalf("validateExecutionWorkspace() error = %v, want provider checks deferred to planAgentExecution", err)
	}
}

// ---------------------------------------------------------------------------
// shouldRetry / calculateRetryDelay
// ---------------------------------------------------------------------------

func TestShouldRetry(t *testing.T) {
	r := &TaskReconciler{}
	tests := []struct {
		name   string
		task   *corev1alpha1.Task
		expect bool
	}{
		{
			name:   "no retry policy",
			task:   &corev1alpha1.Task{},
			expect: false,
		},
		{
			name: "attempts < maxRetries",
			task: &corev1alpha1.Task{
				Spec:   corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 3}},
				Status: corev1alpha1.TaskStatus{Attempts: 1},
			},
			expect: true,
		},
		{
			name: "attempts == maxRetries allows final retry",
			task: &corev1alpha1.Task{
				Spec:   corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 3}},
				Status: corev1alpha1.TaskStatus{Attempts: 3},
			},
			expect: true,
		},
		{
			name: "attempts > maxRetries",
			task: &corev1alpha1.Task{
				Spec:   corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 2}},
				Status: corev1alpha1.TaskStatus{Attempts: 5},
			},
			expect: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.shouldRetry(tc.task); got != tc.expect {
				t.Errorf("shouldRetry = %v, want %v", got, tc.expect)
			}
		})
	}
}

func TestCalculateRetryDelay(t *testing.T) {
	r := &TaskReconciler{}

	t.Run("no retry policy returns default", func(t *testing.T) {
		task := &corev1alpha1.Task{}
		if d := r.calculateRetryDelay(task); d != 10*time.Second {
			t.Errorf("expected 10s, got %v", d)
		}
	})

	t.Run("nil initialDelay returns default", func(t *testing.T) {
		task := &corev1alpha1.Task{
			Spec: corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 3}},
		}
		if d := r.calculateRetryDelay(task); d != 10*time.Second {
			t.Errorf("expected 10s, got %v", d)
		}
	})

	t.Run("first attempt uses initial delay", func(t *testing.T) {
		task := &corev1alpha1.Task{
			Spec: corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{
				MaxRetries:   3,
				InitialDelay: &metav1.Duration{Duration: 5 * time.Second},
			}},
			Status: corev1alpha1.TaskStatus{Attempts: 1},
		}
		if d := r.calculateRetryDelay(task); d != 5*time.Second {
			t.Errorf("expected 5s, got %v", d)
		}
	})

	t.Run("exponential backoff", func(t *testing.T) {
		task := &corev1alpha1.Task{
			Spec: corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{
				MaxRetries:        5,
				BackoffMultiplier: 2,
				InitialDelay:      &metav1.Duration{Duration: 1 * time.Second},
			}},
			Status: corev1alpha1.TaskStatus{Attempts: 3},
		}
		// 1s * 2 * 2 = 4s
		if d := r.calculateRetryDelay(task); d != 4*time.Second {
			t.Errorf("expected 4s, got %v", d)
		}
	})

	t.Run("capped at 5 minutes", func(t *testing.T) {
		task := &corev1alpha1.Task{
			Spec: corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{
				MaxRetries:        20,
				BackoffMultiplier: 10,
				InitialDelay:      &metav1.Duration{Duration: 1 * time.Minute},
			}},
			Status: corev1alpha1.TaskStatus{Attempts: 10},
		}
		if d := r.calculateRetryDelay(task); d != 5*time.Minute {
			t.Errorf("expected 5m cap, got %v", d)
		}
	})

	t.Run("zero multiplier defaults to 2", func(t *testing.T) {
		task := &corev1alpha1.Task{
			Spec: corev1alpha1.TaskSpec{RetryPolicy: &corev1alpha1.RetryPolicy{
				MaxRetries:        5,
				BackoffMultiplier: 0,
				InitialDelay:      &metav1.Duration{Duration: 2 * time.Second},
			}},
			Status: corev1alpha1.TaskStatus{Attempts: 2},
		}
		// 2s * 2 = 4s (multiplier 0 defaults to 2)
		if d := r.calculateRetryDelay(task); d != 4*time.Second {
			t.Errorf("expected 4s, got %v", d)
		}
	})
}

// ---------------------------------------------------------------------------
// enforceHistoryLimits
// ---------------------------------------------------------------------------

func TestEnforceHistoryLimits_DefaultLimits(t *testing.T) {
	scheme := newTestScheme()

	// Parent task
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}

	// Create 5 succeeded + 3 failed child tasks
	objs := make([]client.Object, 0, 10) //nolint:prealloc
	objs = append(objs, parent)
	for i := range 5 {
		objs = append(objs, &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "child-s" + time.Now().Add(time.Duration(i)*time.Hour).Format("150405"),
				Namespace:         "default",
				Labels:            map[string]string{labels.LabelParentTask: "parent"},
				CreationTimestamp: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Hour)),
			},
			Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
		})
	}
	for i := range 3 {
		objs = append(objs, &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "child-f" + time.Now().Add(time.Duration(i)*time.Hour).Format("150405"),
				Namespace:         "default",
				Labels:            map[string]string{labels.LabelParentTask: "parent"},
				CreationTimestamp: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Hour)),
			},
			Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
		})
	}

	r := newUnitReconciler(scheme, objs...)
	err := r.enforceHistoryLimits(context.Background(), parent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Defaults: successLimit=3, failedLimit=1
	// Should have deleted 2 succeeded and 2 failed
	var remaining corev1alpha1.TaskList
	_ = r.List(context.Background(), &remaining, client.InNamespace("default"),
		client.MatchingLabels{labels.LabelParentTask: "parent"})

	succeeded, failed := 0, 0
	for _, task := range remaining.Items {
		switch task.Status.Phase {
		case corev1alpha1.TaskPhaseSucceeded:
			succeeded++
		case corev1alpha1.TaskPhaseFailed:
			failed++
		}
	}
	if succeeded != 3 {
		t.Errorf("expected 3 succeeded remaining, got %d", succeeded)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed remaining, got %d", failed)
	}
}

func TestEnforceHistoryLimits_CustomLimits(t *testing.T) {
	scheme := newTestScheme()

	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:                       corev1alpha1.TaskTypeAI,
			SuccessfulRunsHistoryLimit: new(int32(1)),
			FailedRunsHistoryLimit:     new(int32(0)),
		},
	}

	objs := make([]client.Object, 0, 10) //nolint:prealloc
	objs = append(objs, parent)
	for i := range 4 {
		objs = append(objs, &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "cs-" + time.Now().Add(time.Duration(i)*time.Minute).Format("150405"),
				Namespace:         "default",
				Labels:            map[string]string{labels.LabelParentTask: "parent"},
				CreationTimestamp: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Minute)),
			},
			Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
		})
	}
	for i := range 2 {
		objs = append(objs, &corev1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "cf-" + time.Now().Add(time.Duration(i)*time.Minute).Format("150405"),
				Namespace:         "default",
				Labels:            map[string]string{labels.LabelParentTask: "parent"},
				CreationTimestamp: metav1.NewTime(time.Now().Add(time.Duration(i) * time.Minute)),
			},
			Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
		})
	}

	r := newUnitReconciler(scheme, objs...)
	if err := r.enforceHistoryLimits(context.Background(), parent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var remaining corev1alpha1.TaskList
	_ = r.List(context.Background(), &remaining, client.InNamespace("default"),
		client.MatchingLabels{labels.LabelParentTask: "parent"})

	succeeded, failed := 0, 0
	for _, task := range remaining.Items {
		switch task.Status.Phase {
		case corev1alpha1.TaskPhaseSucceeded:
			succeeded++
		case corev1alpha1.TaskPhaseFailed:
			failed++
		}
	}
	if succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", succeeded)
	}
	if failed != 0 {
		t.Errorf("expected 0 failed, got %d", failed)
	}
}

func TestEnforceHistoryLimits_LongParentNameUsesSelectorValue(t *testing.T) {
	scheme := newTestScheme()

	parentName := "very-long-parent-task-name-that-exceeds-kubernetes-label-limits-1234567890"
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: parentName, Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
		},
	}

	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-1",
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelParentTask: labels.SelectorValue(parentName),
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName: parentName,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}

	r := newUnitReconciler(scheme, parent, child)
	if err := r.enforceHistoryLimits(context.Background(), parent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var remaining corev1alpha1.TaskList
	if err := r.List(context.Background(), &remaining, client.InNamespace("default"),
		client.MatchingLabels{labels.LabelParentTask: labels.SelectorValue(parentName)}); err != nil {
		t.Fatalf("listing child tasks: %v", err)
	}
	if len(remaining.Items) != 1 {
		t.Fatalf("expected 1 child task, got %d", len(remaining.Items))
	}
	if labels.ParentTaskName(remaining.Items[0].Labels, remaining.Items[0].Annotations) != parentName {
		t.Fatalf("ParentTaskName() = %q, want %q", labels.ParentTaskName(remaining.Items[0].Labels, remaining.Items[0].Annotations), parentName)
	}
}

func TestEnforceHistoryLimits_NoChildTasks(t *testing.T) {
	scheme := newTestScheme()
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, parent)
	if err := r.enforceHistoryLimits(context.Background(), parent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// validateCoordinationConstraints
// ---------------------------------------------------------------------------

func TestValidateCoordinationConstraints_NoAnnotation(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
	}
	_, _, done := r.validateCoordinationConstraints(context.Background(), task)
	if done {
		t.Error("expected done=false when no coordination-depth annotation")
	}
}

func TestValidateCoordinationConstraints_CoordinationDisabled(t *testing.T) {
	scheme := newTestScheme()
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
		},
	}
	parentAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{Enabled: false},
		},
	}
	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "1",
			},
			Labels: map[string]string{
				labels.LabelParentTask: "parent",
			},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}

	r := newUnitReconciler(scheme, parentTask, parentAgent, childTask)
	_, _, done := r.validateCoordinationConstraints(context.Background(), childTask)
	if !done {
		t.Error("expected done=true when coordination is disabled")
	}
}

func TestValidateCoordinationConstraints_MaxDepthExceeded(t *testing.T) {
	scheme := newTestScheme()
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
		},
	}
	parentAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:  true,
				MaxDepth: 2,
			},
		},
	}
	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "3",
			},
			Labels: map[string]string{
				labels.LabelParentTask: "parent",
			},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}

	r := newUnitReconciler(scheme, parentTask, parentAgent, childTask)
	_, _, done := r.validateCoordinationConstraints(context.Background(), childTask)
	if !done {
		t.Error("expected done=true when max depth exceeded")
	}
}

func TestValidateCoordinationConstraints_AllowedAgentPass(t *testing.T) {
	scheme := newTestScheme()
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
		},
	}
	parentAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:       true,
				AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "child-agent"}},
			},
		},
	}
	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "1",
			},
			Labels: map[string]string{
				labels.LabelParentTask: "parent",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "child-agent"},
		},
	}

	r := newUnitReconciler(scheme, parentTask, parentAgent, childTask)
	_, _, done := r.validateCoordinationConstraints(context.Background(), childTask)
	if done {
		t.Error("expected done=false when agent is in allowed list")
	}
}

func TestValidateCoordinationConstraints_AgentNotAllowed(t *testing.T) {
	scheme := newTestScheme()
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
		},
	}
	parentAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:       true,
				AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "other-agent"}},
			},
		},
	}
	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "1",
			},
			Labels: map[string]string{
				labels.LabelParentTask: "parent",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "unauthorized-agent"},
		},
	}

	r := newUnitReconciler(scheme, parentTask, parentAgent, childTask)
	_, _, done := r.validateCoordinationConstraints(context.Background(), childTask)
	if !done {
		t.Error("expected done=true when agent is not in allowed list")
	}
}

func TestValidateCoordinationConstraints_DynamicallyCreatedAgent(t *testing.T) {
	scheme := newTestScheme()
	parentTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
		},
	}
	parentAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Coordination: &corev1alpha1.CoordinationConfig{
				Enabled:       true,
				AllowedAgents: []corev1alpha1.AllowedAgent{{Name: "other-agent"}},
			},
		},
	}
	// Dynamically created agent by create_agent tool
	dynamicAgent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dynamic-agent",
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelCreatedBy:  "create_agent",
				labels.LabelParentTask: "parent",
			},
		},
	}
	childTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationCoordinationDepth: "1",
			},
			Labels: map[string]string{
				labels.LabelParentTask: "parent",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "dynamic-agent"},
		},
	}

	r := newUnitReconciler(scheme, parentTask, parentAgent, dynamicAgent, childTask)
	_, _, done := r.validateCoordinationConstraints(context.Background(), childTask)
	if done {
		t.Error("expected done=false for dynamically created agent with matching parent")
	}
}

func TestValidateCoordinationConstraints_ConcurrencyLimit(t *testing.T) {
	tests := []struct {
		phase       corev1alpha1.TaskPhase
		wantLimited bool
	}{
		{phase: corev1alpha1.TaskPhaseRunning, wantLimited: true},
		{phase: corev1alpha1.TaskPhaseFinalizing, wantLimited: true},
		{phase: corev1alpha1.TaskPhaseSucceeded, wantLimited: false},
		{phase: corev1alpha1.TaskPhaseFailed, wantLimited: false},
		{phase: corev1alpha1.TaskPhaseCancelled, wantLimited: false},
	}

	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			scheme := newTestScheme()
			parentTask := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "default"},
				Spec: corev1alpha1.TaskSpec{
					Type:     corev1alpha1.TaskTypeAI,
					AgentRef: &corev1alpha1.AgentReference{Name: "parent-agent"},
				},
			}
			parentAgent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "parent-agent", Namespace: "default"},
				Spec: corev1alpha1.AgentSpec{
					Coordination: &corev1alpha1.CoordinationConfig{
						Enabled:               true,
						MaxConcurrentChildren: 1,
					},
				},
			}
			sibling := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sibling",
					Namespace: "default",
					Labels:    map[string]string{labels.LabelParentTask: "parent"},
				},
				Status: corev1alpha1.TaskStatus{Phase: tt.phase},
			}
			childTask := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "child",
					Namespace: "default",
					Annotations: map[string]string{
						labels.AnnotationCoordinationDepth: "1",
					},
					Labels: map[string]string{
						labels.LabelParentTask: "parent",
					},
				},
				Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
			}

			r := newUnitReconciler(scheme, parentTask, parentAgent, sibling, childTask)
			result, err, done := r.validateCoordinationConstraints(context.Background(), childTask)
			if err != nil {
				t.Fatalf("validateCoordinationConstraints() error = %v", err)
			}
			if done != tt.wantLimited {
				t.Fatalf("done = %t, want %t", done, tt.wantLimited)
			}
			if tt.wantLimited && result.RequeueAfter != 10*time.Second {
				t.Errorf("expected 10s requeue, got %v", result.RequeueAfter)
			}
			if !tt.wantLimited && result.RequeueAfter != 0 {
				t.Errorf("unexpected requeue for terminal sibling: %v", result.RequeueAfter)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ensureWorkerRBAC
// ---------------------------------------------------------------------------

func TestEnsureWorkerRBAC_CreatesNamespacedResources(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)

	err := r.ensureWorkerRBAC(context.Background(), testNS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []struct {
		serviceAccount string
		roleBinding    string
		clusterRole    string
	}{
		{AIWorkerServiceAccount, "orka-ai-worker-test-ns", DefaultAIWorkerClusterRoleName},
		{VendorWorkerServiceAccount, "orka-vendor-worker-test-ns", DefaultVendorWorkerClusterRoleName},
		{ContainerWorkerServiceAccount, "orka-container-worker-test-ns", DefaultContainerWorkerClusterRoleName},
	}

	for _, tt := range expected {
		t.Run(tt.serviceAccount, func(t *testing.T) {
			// Verify ServiceAccount was created.
			sa := &corev1.ServiceAccount{}
			if err := r.Get(context.Background(), types.NamespacedName{
				Name: tt.serviceAccount, Namespace: testNS,
			}, sa); err != nil {
				t.Fatalf("expected SA %s to exist: %v", tt.serviceAccount, err)
			}

			// Verify only a namespaced binding to the worker ClusterRole was created.
			rb := &rbacv1.RoleBinding{}
			if err := r.Get(context.Background(), types.NamespacedName{
				Name: tt.roleBinding, Namespace: testNS,
			}, rb); err != nil {
				t.Fatalf("expected RoleBinding %s/%s to exist: %v", testNS, tt.roleBinding, err)
			}
			if rb.RoleRef.Kind != "ClusterRole" || rb.RoleRef.Name != tt.clusterRole {
				t.Errorf("expected ClusterRole roleRef %s, got %#v", tt.clusterRole, rb.RoleRef)
			}
			if len(rb.Subjects) != 1 {
				t.Fatalf("expected 1 subject, got %d", len(rb.Subjects))
			}
			subject := rb.Subjects[0]
			if subject.Kind != rbacv1.ServiceAccountKind || subject.Name != tt.serviceAccount || subject.Namespace != testNS {
				t.Errorf("unexpected subject: %#v", subject)
			}
			crb := &rbacv1.ClusterRoleBinding{}
			if err := r.Get(context.Background(), types.NamespacedName{Name: tt.roleBinding}, crb); !apierrors.IsNotFound(err) {
				t.Fatalf("expected no ClusterRoleBinding %s, got err %v and object %#v", tt.roleBinding, err, crb)
			}
		})
	}
}

func TestEnsureWorkerRBAC_UsesConfiguredServiceAccountNames(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	r.AIWorkerServiceAccountName = testAIWorkerServiceAccountName
	r.VendorWorkerServiceAccountName = testVendorWorkerServiceAccountName
	r.ContainerWorkerServiceAccountName = testContainerWorkerServiceAccountName

	if err := r.ensureWorkerRBAC(context.Background(), testNS); err != nil {
		t.Fatalf("ensureWorkerRBAC() error = %v", err)
	}

	expected := []struct {
		serviceAccount string
		roleBinding    string
	}{
		{serviceAccount: testAIWorkerServiceAccountName, roleBinding: "orka-ai-worker-test-ns"},
		{serviceAccount: testVendorWorkerServiceAccountName, roleBinding: "orka-vendor-worker-test-ns"},
		{serviceAccount: testContainerWorkerServiceAccountName, roleBinding: "orka-container-worker-test-ns"},
	}

	for _, tt := range expected {
		t.Run(tt.serviceAccount, func(t *testing.T) {
			sa := &corev1.ServiceAccount{}
			if err := r.Get(context.Background(), types.NamespacedName{Name: tt.serviceAccount, Namespace: testNS}, sa); err != nil {
				t.Fatalf("expected ServiceAccount %s/%s to exist: %v", testNS, tt.serviceAccount, err)
			}

			rb := &rbacv1.RoleBinding{}
			if err := r.Get(context.Background(), types.NamespacedName{Name: tt.roleBinding, Namespace: testNS}, rb); err != nil {
				t.Fatalf("expected RoleBinding %s/%s to exist: %v", testNS, tt.roleBinding, err)
			}
			if len(rb.Subjects) != 1 {
				t.Fatalf("RoleBinding %s/%s subjects = %#v, want one subject", testNS, tt.roleBinding, rb.Subjects)
			}
			if got := rb.Subjects[0]; got.Kind != rbacv1.ServiceAccountKind || got.Name != tt.serviceAccount || got.Namespace != testNS {
				t.Fatalf("RoleBinding %s/%s subject = %#v, want ServiceAccount %s/%s", testNS, tt.roleBinding, got, testNS, tt.serviceAccount)
			}
		})
	}

	for _, name := range []string{AIWorkerServiceAccount, VendorWorkerServiceAccount, ContainerWorkerServiceAccount} {
		sa := &corev1.ServiceAccount{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: testNS}, sa); !apierrors.IsNotFound(err) {
			t.Fatalf("default ServiceAccount %s/%s should not be created when a custom name is configured; err = %v", testNS, name, err)
		}
	}
}

func TestEnsureWorkerRBAC_DoesNotMigrateLegacyClusterRoleBindings(t *testing.T) {
	scheme := newTestScheme()
	legacy := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "orka-ai-worker-test-ns", Labels: map[string]string{managedByLabelKey: managedByLabelValue}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "old-ai-worker-role"},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: testNS},
			{Kind: rbacv1.ServiceAccountKind, Name: "extra-worker", Namespace: testNS},
		},
	}
	r := newUnitReconciler(scheme, legacy)

	if err := r.ensureWorkerRBAC(context.Background(), testNS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: legacy.Name}, crb); err != nil {
		t.Fatalf("legacy ClusterRoleBinding was touched: %v", err)
	}
	if !reflect.DeepEqual(crb.Subjects, legacy.Subjects) || crb.RoleRef != legacy.RoleRef {
		t.Fatalf("legacy ClusterRoleBinding was mutated: %#v", crb)
	}

	rb := &rbacv1.RoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: legacy.Name, Namespace: testNS}, rb); err != nil {
		t.Fatalf("expected independent namespaced RoleBinding to exist: %v", err)
	}
}

func TestEnsureWorkerRBAC_UsesRoleBindingPrefix(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	r.WorkerRoleBindingNamePrefix = "orka-dev"
	ctx := context.Background()

	if err := r.ensureWorkerRBAC(ctx, testNS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []struct {
		serviceAccount string
		roleBinding    string
		clusterRole    string
	}{
		{AIWorkerServiceAccount, "orka-dev-ai-worker-test-ns", DefaultAIWorkerClusterRoleName},
		{VendorWorkerServiceAccount, "orka-dev-vendor-worker-test-ns", DefaultVendorWorkerClusterRoleName},
		{ContainerWorkerServiceAccount, "orka-dev-container-worker-test-ns", DefaultContainerWorkerClusterRoleName},
	}

	for _, tt := range expected {
		t.Run(tt.roleBinding, func(t *testing.T) {
			rb := &rbacv1.RoleBinding{}
			if err := r.Get(ctx, types.NamespacedName{Name: tt.roleBinding, Namespace: testNS}, rb); err != nil {
				t.Fatalf("expected prefixed RoleBinding %s/%s to exist: %v", testNS, tt.roleBinding, err)
			}
			if rb.RoleRef.Name != tt.clusterRole {
				t.Fatalf("expected roleRef %s, got %s", tt.clusterRole, rb.RoleRef.Name)
			}
			if len(rb.Subjects) != 1 {
				t.Fatalf("expected 1 subject, got %d", len(rb.Subjects))
			}
			subject := rb.Subjects[0]
			if subject.Kind != rbacv1.ServiceAccountKind || subject.Name != tt.serviceAccount || subject.Namespace != testNS {
				t.Fatalf("unexpected subject: %#v", subject)
			}
		})
	}
}

func TestWorkerRoleBindingNameTruncatesLongNames(t *testing.T) {
	prefix := strings.Repeat("p", 230)
	namespace := strings.Repeat("n", 80)

	got := workerRoleBindingName(prefix, "container", namespace)
	if len(got) != maxWorkerRoleBindingNameLength {
		t.Fatalf("expected name length %d, got %d", maxWorkerRoleBindingNameLength, len(got))
	}
	if got != workerRoleBindingName(prefix, "container", namespace) {
		t.Fatal("expected truncated name to be stable")
	}
	if got == workerRoleBindingName(prefix, "vendor", namespace) {
		t.Fatal("expected hash suffix to distinguish names that share a truncated prefix")
	}
}

func TestEnsureWorkerRBAC_Idempotent(t *testing.T) {
	scheme := newTestScheme()
	// Pre-create all SAs and namespaced RoleBindings.
	expected := []struct {
		serviceAccount string
		roleBinding    string
		clusterRole    string
	}{
		{AIWorkerServiceAccount, "orka-ai-worker-test-ns", DefaultAIWorkerClusterRoleName},
		{VendorWorkerServiceAccount, "orka-vendor-worker-test-ns", DefaultVendorWorkerClusterRoleName},
		{ContainerWorkerServiceAccount, "orka-container-worker-test-ns", DefaultContainerWorkerClusterRoleName},
	}

	objects := make([]client.Object, 0, len(expected)*2)
	for _, tt := range expected {
		objects = append(objects,
			&corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: tt.serviceAccount, Namespace: testNS},
			},
			&rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: tt.roleBinding, Namespace: testNS},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     tt.clusterRole,
				},
				Subjects: []rbacv1.Subject{{
					Kind: rbacv1.ServiceAccountKind, Name: tt.serviceAccount, Namespace: testNS,
				}},
			},
		)
	}
	r := newUnitReconciler(scheme, objects...)

	// Should not fail when resources already exist.
	if err := r.ensureWorkerRBAC(context.Background(), testNS); err != nil {
		t.Fatalf("unexpected error on idempotent call: %v", err)
	}
}

func TestEnsureWorkerServiceAccountPreservesAppManagedByLabel(t *testing.T) {
	scheme := newTestScheme()
	ctx := context.Background()
	namespace := testNS
	appManagedBy := "Helm"
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AIWorkerServiceAccount,
			Namespace: namespace,
			Labels: map[string]string{
				managedByLabelKey: appManagedBy,
				"custom":          "keep",
			},
		},
	}
	r := newUnitReconciler(scheme, existing)

	if err := r.ensureWorkerServiceAccount(ctx, namespace, AIWorkerServiceAccount); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := &corev1.ServiceAccount{}
	if err := r.Get(ctx, types.NamespacedName{Name: AIWorkerServiceAccount, Namespace: namespace}, got); err != nil {
		t.Fatalf("getting ServiceAccount: %v", err)
	}
	if got.Labels[managedByLabelKey] != appManagedBy {
		t.Fatalf("expected app managed-by label to be preserved, got labels %#v", got.Labels)
	}
	if got.Labels[orkaManagedByLabelKey] != managedByLabelValue {
		t.Fatalf("expected Orka managed-by label to be reconciled, got labels %#v", got.Labels)
	}
	if got.Labels["custom"] != "keep" {
		t.Fatalf("expected existing labels to be preserved, got labels %#v", got.Labels)
	}
}

func TestEnsureWorkerRoleBindingAlreadyExistsRaceUpdatesExistingBinding(t *testing.T) {
	scheme := newTestScheme()
	ctx := context.Background()
	namespace := "race-ns"
	spec := workerRBACSpec{
		serviceAccountName: AIWorkerServiceAccount,
		clusterRoleName:    DefaultAIWorkerClusterRoleName,
		roleBindingName:    fmt.Sprintf("orka-ai-worker-%s", namespace),
	}

	interceptedCreate := false
	fc := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() != spec.roleBindingName || obj.GetNamespace() != namespace {
				return c.Create(ctx, obj, opts...)
			}
			if _, ok := obj.(*rbacv1.RoleBinding); !ok {
				return c.Create(ctx, obj, opts...)
			}

			interceptedCreate = true
			existing := &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      spec.roleBindingName,
					Namespace: namespace,
					Labels:    map[string]string{staleResourceLabelKey: staleResourceLabelValue},
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     spec.clusterRoleName,
				},
				Subjects: []rbacv1.Subject{{
					Kind:      rbacv1.ServiceAccountKind,
					Name:      "stale-worker",
					Namespace: namespace,
				}},
			}
			if err := c.Create(ctx, existing); err != nil {
				t.Fatalf("creating raced RoleBinding fixture: %v", err)
			}

			return apierrors.NewAlreadyExists(
				schema.GroupResource{Group: rbacv1.GroupName, Resource: "rolebindings"},
				spec.roleBindingName,
			)
		},
	}).Build()

	r := newUnitReconciler(scheme)
	r.Client = fc
	r.JobBuilder = NewJobBuilder(fc)

	if err := r.ensureWorkerRoleBinding(ctx, namespace, spec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !interceptedCreate {
		t.Fatal("expected create to be intercepted")
	}

	got := &rbacv1.RoleBinding{}
	if err := fc.Get(ctx, types.NamespacedName{Name: spec.roleBindingName, Namespace: namespace}, got); err != nil {
		t.Fatalf("expected raced RoleBinding to exist: %v", err)
	}
	if got.Labels[managedByLabelKey] != managedByLabelValue {
		t.Fatalf("expected managed-by label to be reconciled, got labels %#v", got.Labels)
	}
	if len(got.Subjects) != 1 {
		t.Fatalf("expected 1 subject, got %d", len(got.Subjects))
	}
	subject := got.Subjects[0]
	if subject.Kind != rbacv1.ServiceAccountKind || subject.Name != spec.serviceAccountName || subject.Namespace != namespace {
		t.Fatalf("expected desired subject to be reconciled, got %#v", subject)
	}
}

func TestEnsureWorkerRoleBindingRecreatesStaleRoleRef(t *testing.T) {
	scheme := newTestScheme()
	ctx := context.Background()
	namespace := "stale-ns"
	spec := workerRBACSpec{
		serviceAccountName: AIWorkerServiceAccount,
		clusterRoleName:    DefaultAIWorkerClusterRoleName,
		roleBindingName:    fmt.Sprintf("orka-ai-worker-%s", namespace),
	}

	stale := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.roleBindingName,
			Namespace: namespace,
			Labels: map[string]string{
				staleResourceLabelKey: staleResourceLabelValue,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "stale-worker-role",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      "stale-worker",
			Namespace: "old-ns",
		}},
	}
	r := newUnitReconciler(scheme, stale)

	if err := r.ensureWorkerRoleBinding(ctx, namespace, spec); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, types.NamespacedName{Name: spec.roleBindingName, Namespace: namespace}, got); err != nil {
		t.Fatalf("expected RoleBinding to exist after remediation: %v", err)
	}
	wantRoleRef := rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "ClusterRole",
		Name:     spec.clusterRoleName,
	}
	if got.RoleRef != wantRoleRef {
		t.Fatalf("expected stale RoleRef to be remediated to %#v, got %#v", wantRoleRef, got.RoleRef)
	}
	if got.Labels[managedByLabelKey] != managedByLabelValue {
		t.Fatalf("expected managed-by label on remediated binding, got labels %#v", got.Labels)
	}
	if got.Labels[staleResourceLabelKey] == staleResourceLabelValue {
		t.Fatalf("expected stale binding to be recreated, got stale labels %#v", got.Labels)
	}
	if len(got.Subjects) != 1 {
		t.Fatalf("expected 1 subject, got %d", len(got.Subjects))
	}
	subject := got.Subjects[0]
	if subject.Kind != rbacv1.ServiceAccountKind || subject.Name != spec.serviceAccountName || subject.Namespace != namespace {
		t.Fatalf("expected desired subject to be reconciled, got %#v", subject)
	}
}

// ---------------------------------------------------------------------------
// handleScheduledTask
// ---------------------------------------------------------------------------

func TestHandleScheduledTask_ValidCron(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "*/5 * * * *",
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduledTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter")
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseScheduled {
		t.Errorf("expected phase Scheduled, got %s", task.Status.Phase)
	}
	if task.Status.NextScheduleTime == nil {
		t.Error("expected NextScheduleTime to be set")
	}
}

func TestHandleScheduledTask_InvalidCron(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched2", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "not-a-cron",
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleScheduledTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected phase Failed for invalid cron, got %s", task.Status.Phase)
	}
}

func TestHandleScheduledTask_WithTimeZone(t *testing.T) {
	scheme := newTestScheme()
	tz := "America/New_York"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched3", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "0 12 * * *",
			TimeZone: &tz,
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduledTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter")
	}
}

// ---------------------------------------------------------------------------
// collectResult
// ---------------------------------------------------------------------------

func TestCollectResult_ResultAlreadyExists(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)

	// Pre-save a result
	_ = r.ResultStore.SaveResult(context.Background(), "default", "t1", []byte("result data"))

	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.ResultRef == nil || !task.Status.ResultRef.Available {
		t.Error("expected ResultRef.Available to be true")
	}
}

func TestCollectResult_NoResultNoKubeClient(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t2", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	r := newUnitReconciler(scheme, task)
	// KubeClient is nil by default in unit reconciler

	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No result and no kube client — should return nil
}

func TestCollectResult_ContainerWithoutJobDoesNotReadPodLogs(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "prejob-failure", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{JobName: ""},
	}
	r := newUnitReconciler(scheme, task)
	r.KubeClient = k8sfake.NewSimpleClientset()

	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.ResultRef != nil {
		t.Fatalf("expected ResultRef to remain nil without a job, got %#v", task.Status.ResultRef)
	}
}

func TestCollectResult_AITaskNoResult(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t3", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)

	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// AI task without result in store, no kube client — should not fail
}

func TestCollectResult_NilResultStore_DoesNotPanic(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "nil-store-result", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)
	r.ResultStore = nil

	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.ResultRef != nil {
		t.Fatalf("expected ResultRef to remain nil when result store is nil, got %#v", task.Status.ResultRef)
	}
}

func TestExtractStdoutTaskResult(t *testing.T) {
	first := base64.StdEncoding.EncodeToString([]byte("first"))
	second := base64.StdEncoding.EncodeToString([]byte("second"))
	logs := strings.Join([]string{
		"Worker started",
		workerenv.ResultStdoutPrefix + first,
		"Task completed",
		workerenv.ResultStdoutPrefix + second,
	}, "\n")

	got, ok, err := extractStdoutTaskResult(logs)
	if err != nil {
		t.Fatalf("extractStdoutTaskResult() error = %v", err)
	}
	if !ok {
		t.Fatal("extractStdoutTaskResult() ok = false, want true")
	}
	if string(got) != "second" {
		t.Fatalf("extractStdoutTaskResult() = %q, want second", string(got))
	}
}

func TestExtractStdoutTaskResultMissingMarker(t *testing.T) {
	got, ok, err := extractStdoutTaskResult("Worker started\nTask completed")
	if err != nil {
		t.Fatalf("extractStdoutTaskResult() error = %v", err)
	}
	if ok {
		t.Fatal("extractStdoutTaskResult() ok = true, want false")
	}
	if got != nil {
		t.Fatalf("extractStdoutTaskResult() = %#v, want nil", got)
	}
}

func TestExtractStdoutTaskResultInvalidBase64(t *testing.T) {
	_, ok, err := extractStdoutTaskResult(workerenv.ResultStdoutPrefix + "not base64")
	if err == nil {
		t.Fatal("extractStdoutTaskResult() error = nil, want error")
	}
	if !ok {
		t.Fatal("extractStdoutTaskResult() ok = false, want true")
	}
}

func TestStdoutResultPodLogOptionsReadsBoundedFullLog(t *testing.T) {
	opts := stdoutResultPodLogOptions()
	if opts.TailLines != nil {
		t.Fatalf("TailLines = %#v, want nil so stdout markers are not tailed away", opts.TailLines)
	}
	if opts.LimitBytes == nil || *opts.LimitBytes != stdoutResultLogLimitBytes {
		t.Fatalf("LimitBytes = %#v, want %d", opts.LimitBytes, stdoutResultLogLimitBytes)
	}
	if stdoutResultLogLimitBytes <= podLogLimitBytes {
		t.Fatalf("stdoutResultLogLimitBytes = %d, want greater than podLogLimitBytes %d", stdoutResultLogLimitBytes, podLogLimitBytes)
	}
}

// ---------------------------------------------------------------------------
// readPodLogs — requires KubeClient; we test the error path (no pods found)
// ---------------------------------------------------------------------------

func TestReadPodLogs_NoPods(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{JobName: "j1"},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.readPodLogs(context.Background(), task)
	if err == nil {
		t.Error("expected error when no pods found")
	}
}

// ---------------------------------------------------------------------------
// Helpers used by handleScheduled — tested indirectly above but we also
// verify the suspend path.
// ---------------------------------------------------------------------------

func TestHandleScheduled_Suspended(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched-susp", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "*/5 * * * *",
			Suspend:  new(true),
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseScheduled},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != time.Minute {
		t.Errorf("expected 1m requeue for suspended task, got %v", result.RequeueAfter)
	}
}

func TestHandleScheduled_InvalidCron(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched-bad", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "invalid",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseScheduled},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected Failed phase, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// resolveProvider (with fake client)
// ---------------------------------------------------------------------------

func TestResolveProvider_NilProviderRef(t *testing.T) {
	r := newUnitReconciler(newTestScheme())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	provider, err := r.resolveProvider(context.Background(), task, nil)
	if err != nil || provider != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", provider, err)
	}
}

func TestResolveProvider_NamespaceIsolation(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	r.EnforceNamespaceIsolation = true
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "p1", Namespace: "other"},
			},
		},
	}
	_, err := r.resolveProvider(context.Background(), task, nil)
	if err == nil {
		t.Error("expected error for cross-namespace provider with isolation")
	}
}

// ---------------------------------------------------------------------------
// handleDeletion
// ---------------------------------------------------------------------------

func TestHandleDeletion_NoFinalizer(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-no-fin",
			Namespace: "default",
		},
	}
	r := newUnitReconciler(scheme, task)
	// Set deletion timestamp after creation (can't pass it to fake.NewClientBuilder)
	task.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	result, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleDeletionRemovesFinalizerWithMetadataOnlyPatch(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "del-agent-metadata",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeAgent,
			Timeout: &metav1.Duration{Duration: 12 * time.Minute},
		},
	}
	r := newUnitReconciler(scheme, task)
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	patchInspected := false
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			if _, ok := object.(*corev1alpha1.Task); ok {
				return errors.New("full Task update is forbidden while removing the finalizer")
			}
			return delegate.Update(ctx, object, options...)
		},
		Patch: func(ctx context.Context, delegate client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			if _, ok := object.(*corev1alpha1.Task); ok {
				data, err := patch.Data(object)
				if err != nil {
					return err
				}
				var body map[string]any
				if err := json.Unmarshal(data, &body); err != nil {
					return err
				}
				if _, present := body["spec"]; present {
					return fmt.Errorf("finalizer patch unexpectedly contains spec: %s", data)
				}
				metadata, ok := body["metadata"].(map[string]any)
				if !ok {
					return fmt.Errorf("finalizer patch has no metadata: %s", data)
				}
				if _, present := metadata["finalizers"]; !present {
					return fmt.Errorf("finalizer patch has no finalizers: %s", data)
				}
				patchInspected = true
			}
			return delegate.Patch(ctx, object, patch, options...)
		},
	})

	if _, err := r.handleDeletion(context.Background(), task); err != nil {
		t.Fatalf("handleDeletion() error = %v", err)
	}
	if !patchInspected {
		t.Fatal("finalizer removal patch was not inspected")
	}
}

func TestHandleDeletion_WithPersistedResultWithoutResultRef(t *testing.T) {

	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-result",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
	}
	r := newUnitReconciler(scheme, task)
	_ = r.ResultStore.SaveResult(context.Background(), "default", "del-result", []byte("data"))
	// handleDeletion calls r.Update to remove finalizer — this works when DeletionTimestamp
	// is not set on the local copy (the fake client rejects changes to DeletionTimestamp).
	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Result should have been cleaned up
	_, getErr := r.ResultStore.GetResult(context.Background(), "default", "del-result")
	if getErr == nil {
		t.Error("expected result to be deleted")
	}
}

func TestHandleDeletionDeletesExecutionEvents(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-events",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
	}
	r := newUnitReconciler(scheme, task)
	if _, err := r.ExecutionEventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "del-events",
		TaskName:   "del-events",
		Type:       events.ExecutionEventTypeTaskStarted,
		Severity:   events.ExecutionEventSeverityInfo,
	}); err != nil {
		t.Fatalf("AppendExecutionEvent task: %v", err)
	}
	if _, err := r.ExecutionEventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "other-task",
		TaskName:   "other-task",
		Type:       events.ExecutionEventTypeTaskStarted,
		Severity:   events.ExecutionEventSeverityInfo,
	}); err != nil {
		t.Fatalf("AppendExecutionEvent other task: %v", err)
	}

	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("handleDeletion() error = %v", err)
	}
	deletedEvents, err := r.ExecutionEventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "del-events",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents deleted task: %v", err)
	}
	if len(deletedEvents) != 0 {
		t.Fatalf("deleted task events len = %d, want 0", len(deletedEvents))
	}
	otherEvents, err := r.ExecutionEventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "other-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents other task: %v", err)
	}
	if len(otherEvents) != 1 {
		t.Fatalf("other task events len = %d, want 1", len(otherEvents))
	}
}

func TestHandleDeletionKeepsFinalizerWhenExecutionEventCleanupFails(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-events-fail",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
	}
	r := newUnitReconciler(scheme, task)
	r.ExecutionEventStore = failingExecutionEventStore{err: errors.New("store unavailable")}

	_, err := r.handleDeletion(context.Background(), task)
	if err == nil {
		t.Fatal("handleDeletion() error = nil, want execution event cleanup error")
	}
	if !controllerutil.ContainsFinalizer(task, labels.TaskFinalizer) {
		t.Fatal("task finalizer was removed after execution event cleanup failed")
	}
}

func TestHandleDeletionKeepsFinalizerWhenPlanCleanupFails(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-plan-fail",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
	}
	r := newUnitReconciler(scheme, task)
	r.PlanStore = failingDeletePlanStore{PlanStore: r.PlanStore, err: errors.New("store unavailable")}

	_, err := r.handleDeletion(context.Background(), task)
	if err == nil || !strings.Contains(err.Error(), "delete plan state") {
		t.Fatalf("handleDeletion() error = %v, want plan cleanup error", err)
	}
	if !controllerutil.ContainsFinalizer(task, labels.TaskFinalizer) {
		t.Fatal("task finalizer was removed after plan cleanup failed")
	}
}

func TestHandleDeletion_WithResultRefNilResultStore(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-result-no-store",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Status: corev1alpha1.TaskStatus{
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	r := newUnitReconciler(scheme, task)
	r.ResultStore = nil

	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleDeletion_WithSessionRef(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-sess",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			SessionRef: &corev1alpha1.SessionReference{Name: "sess1"},
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleDeletion_WithJobName(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job1", Namespace: "default"},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-job",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Status: corev1alpha1.TaskStatus{JobName: "job1"},
	}
	r := newUnitReconciler(scheme, task, job)
	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleDeletion_WithMessageStore(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "del-msg",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
	}
	r := newUnitReconciler(scheme, task)
	db, _ := sqlite.NewDB(":memory:")
	ss := sqlite.NewStore(db, ":memory:")
	r.MessageStore = ss
	_, err := r.handleDeletion(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// handleCompleted
// ---------------------------------------------------------------------------

func TestHandleCompleted_NoWebhook(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleCompleted_WebhookAlreadyDelivered(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp2", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			WebhookURL: "http://example.com/hook",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseSucceeded,
			WebhookDelivered: true,
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleCompleted_CancelledDeletesJob(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "cancel-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cancel-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseCancelled,
			JobName: "cancel-job",
		},
	}
	r := newUnitReconciler(scheme, task, job)

	_, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := r.Get(context.Background(), types.NamespacedName{Name: "cancel-job", Namespace: "default"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected cancelled task Job to be deleted, got %v", err)
	}
}

func TestHandleCompleted_FailedActiveJobDeletesJob(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-active-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-active-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseFailed,
			JobName: "failed-active-job",
		},
	}
	r := newUnitReconciler(scheme, task, job)

	_, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := r.Get(context.Background(), types.NamespacedName{Name: "failed-active-job", Namespace: "default"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected active failed task Job to be deleted, got %v", err)
	}
}

func TestHandleCompleted_FailedInactiveJobRetainsJob(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-inactive-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-inactive-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseFailed,
			JobName: "failed-inactive-job",
		},
	}
	r := newUnitReconciler(scheme, task, job)

	_, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := r.Get(context.Background(), types.NamespacedName{Name: "failed-inactive-job", Namespace: "default"}, &batchv1.Job{}); err != nil {
		t.Fatalf("expected inactive failed task Job to be retained, got %v", err)
	}
}

func TestHandleCompleted_EnforcesScheduledTaskHistoryLimit(t *testing.T) {
	scheme := newTestScheme()
	parent := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "sched-parent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:                       corev1alpha1.TaskTypeContainer,
			SuccessfulRunsHistoryLimit: new(int32(1)),
		},
	}
	oldChild := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-child-old",
			Namespace:         "default",
			Labels:            map[string]string{labels.LabelParentTask: "sched-parent", labels.LabelScheduledRun: scheduledRunLabelValue},
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	currentChild := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-child-current",
			Namespace:         "default",
			Labels:            map[string]string{labels.LabelParentTask: "sched-parent", labels.LabelScheduledRun: scheduledRunLabelValue},
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}

	r := newUnitReconciler(scheme, parent, oldChild, currentChild)
	result, err := r.handleCompleted(context.Background(), currentChild)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}

	remaining := &corev1alpha1.TaskList{}
	if err := r.List(context.Background(), remaining, client.InNamespace("default"),
		client.MatchingLabels{labels.LabelParentTask: "sched-parent"}); err != nil {
		t.Fatalf("listing child tasks: %v", err)
	}

	if len(remaining.Items) != 1 {
		t.Fatalf("expected 1 child task to remain, got %d", len(remaining.Items))
	}
	if remaining.Items[0].Name != "sched-child-current" {
		t.Fatalf("expected newest child to remain, got %s", remaining.Items[0].Name)
	}
}

// ---------------------------------------------------------------------------
// handleRunning
// ---------------------------------------------------------------------------

func TestHandleRunning_Timeout(t *testing.T) {
	scheme := newTestScheme()
	timeout := metav1.Duration{Duration: 1 * time.Second}
	startTime := metav1.NewTime(time.Now().Add(-10 * time.Second))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-timeout", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeAI,
			Timeout: &timeout,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &startTime,
			JobName:   "job1",
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected phase Failed, got %s", task.Status.Phase)
	}
}

func TestHandleRunning_JobNotFound(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-nojob", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "missing-job",
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected phase Failed for missing job, got %s", task.Status.Phase)
	}
}

func TestHandleRunning_JobNotFoundWithRetryPolicy(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-nojob-retry", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:        corev1alpha1.TaskTypeAI,
			RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 2},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:    corev1alpha1.TaskPhaseRunning,
			JobName:  "missing-job",
			Attempts: 1,
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected positive requeue after scheduling retry, got %v", result.RequeueAfter)
	}
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected phase Pending after scheduling retry, got %s", task.Status.Phase)
	}
	if task.Status.JobName != "" {
		t.Errorf("expected JobName to be cleared for retry, got %q", task.Status.JobName)
	}
}

func TestHandleRunning_JobSucceeded(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-job-ok", Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-ok", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "run-job-ok",
		},
	}
	r := newUnitReconciler(scheme, task, job)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Errorf("expected phase Succeeded, got %s", task.Status.Phase)
	}
}

func TestHandleRunning_JobFailed_NoRetry(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-job-fail", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-fail", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "run-job-fail",
		},
	}
	r := newUnitReconciler(scheme, task, job)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected phase Failed, got %s", task.Status.Phase)
	}
}

func TestHandleRunning_JobFailed_WithRetry(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-job-retry", Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-retry", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:        corev1alpha1.TaskTypeAI,
			RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 3},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:    corev1alpha1.TaskPhaseRunning,
			JobName:  "run-job-retry",
			Attempts: 1,
		},
	}
	r := newUnitReconciler(scheme, task, job)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected phase Pending for retry, got %s", task.Status.Phase)
	}
}

func TestHandleRunning_PodFailedMountFailsTask(t *testing.T) {
	scheme := newTestScheme()
	now := time.Now()
	startTime := metav1.NewTime(now.Add(-3 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-failed-mount", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &startTime,
			JobName:   "run-failed-mount-job",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-failed-mount-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-failed-mount-pod",
			Namespace: "default",
			UID:       "pod-uid",
			Labels: map[string]string{
				labels.LabelTask: labels.SelectorValue(task.Name),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "prepare-workspace",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "worker",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
		},
	}
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-mount", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: pod.Name,
			UID:  pod.UID,
		},
		Reason:        "FailedMount",
		Message:       `MountVolume.SetUp failed for volume "git-credentials": secret "missing" not found`,
		LastTimestamp: metav1.NewTime(now.Add(-30 * time.Second)),
	}
	r := newUnitReconciler(scheme, task, job, pod, event)

	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", task.Status.Phase)
	}
	if !strings.Contains(task.Status.Message, "secret") {
		t.Fatalf("message = %q, want failed mount detail", task.Status.Message)
	}
}

func TestHandleRunning_StalePodFailedMountEventRequeues(t *testing.T) {
	scheme := newTestScheme()
	now := time.Now()
	startTime := metav1.NewTime(now.Add(-4 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-stale-failed-mount", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &startTime,
			JobName:   "run-stale-failed-mount-job",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-stale-failed-mount-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-stale-failed-mount-pod",
			Namespace: "default",
			UID:       "pod-uid",
			Labels: map[string]string{
				labels.LabelTask: labels.SelectorValue(task.Name),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "worker",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
		},
	}
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-failed-mount", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: pod.Name,
			UID:  pod.UID,
		},
		Reason:        "FailedMount",
		Message:       `MountVolume.SetUp failed for volume "git-credentials": secret "missing" not found`,
		LastTimestamp: metav1.NewTime(now.Add(-3 * time.Minute)),
	}
	r := newUnitReconciler(scheme, task, job, pod, event)

	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Fatalf("phase = %s, want Running", task.Status.Phase)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %s, want positive duration", result.RequeueAfter)
	}
}

func TestHandleRunning_PodFailedMountSeriesFailsTask(t *testing.T) {
	scheme := newTestScheme()
	now := time.Now()
	startTime := metav1.NewTime(now.Add(-4 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-series-failed-mount", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &startTime,
			JobName:   "run-series-failed-mount-job",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-series-failed-mount-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-series-failed-mount-pod",
			Namespace: "default",
			UID:       "pod-uid",
			Labels: map[string]string{
				labels.LabelTask: labels.SelectorValue(task.Name),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "worker",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
		},
	}
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "series-failed-mount", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: pod.Name,
			UID:  pod.UID,
		},
		Reason:         "FailedMount",
		Message:        `MountVolume.SetUp failed for volume "git-credentials": secret "missing" not found`,
		LastTimestamp:  metav1.NewTime(now.Add(-3 * time.Minute)),
		FirstTimestamp: metav1.NewTime(now.Add(-4 * time.Minute)),
		Series: &corev1.EventSeries{
			Count:            3,
			LastObservedTime: metav1.MicroTime{Time: now.Add(-30 * time.Second)},
		},
	}
	r := newUnitReconciler(scheme, task, job, pod, event)

	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", task.Status.Phase)
	}
	if !strings.Contains(task.Status.Message, "secret") {
		t.Fatalf("message = %q, want failed mount detail", task.Status.Message)
	}
}

func TestHandleRunning_PodInitializingWithoutFailedMountRequeues(t *testing.T) {
	scheme := newTestScheme()
	startTime := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-pod-initializing", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			StartTime: &startTime,
			JobName:   "run-pod-initializing-job",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-pod-initializing-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-pod-initializing-pod",
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelTask: labels.SelectorValue(task.Name),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "prepare-workspace",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "worker",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
		},
	}
	r := newUnitReconciler(scheme, task, job, pod)

	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Fatalf("phase = %s, want Running", task.Status.Phase)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %s, want positive duration", result.RequeueAfter)
	}
}

func TestHandleRunning_JobStillRunning(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-job-active", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "run-active", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "run-job-active",
		},
	}
	r := newUnitReconciler(scheme, task, job)
	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleRunning_ChildTaskStatuses(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "run-job-parent", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-task",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-run"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "child-agent"},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-run", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "run-job-parent",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Errorf("expected 1 child task status, got %d", len(task.Status.ChildTasks))
	}
}

// ---------------------------------------------------------------------------
// createTaskJob
// ---------------------------------------------------------------------------

func TestCreateTaskJob_ContainerTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-job",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hello"},
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.createTaskJob(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Errorf("expected phase Running, got %s", task.Status.Phase)
	}
	if task.Status.JobName == "" {
		t.Error("expected JobName to be set")
	}
	if task.Status.Attempts != 1 {
		t.Errorf("expected Attempts=1, got %d", task.Status.Attempts)
	}
}

func TestCreateTaskJob_AITaskWithAgent(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-ai-job",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "ai-agent"},
			AI:       &corev1alpha1.AISpec{Prompt: "hello"},
		},
	}
	r := newUnitReconciler(scheme, task, agent)
	_, err := r.createTaskJob(context.Background(), task, agent, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Errorf("expected phase Running, got %s", task.Status.Phase)
	}
}

func TestCreateTaskJob_RBACReconcileFailureEmitsWarningAndContinues(t *testing.T) {
	scheme := newTestScheme()
	ctx := context.Background()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-rbac-warn",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hello"},
		},
	}

	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1alpha1.Task{}, &corev1alpha1.Agent{}, &corev1alpha1.AgentRuntime{}).
		WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ServiceAccount); ok {
					return apierrors.NewForbidden(
						schema.GroupResource{Resource: "serviceaccounts"},
						obj.GetName(),
						errors.New("injected serviceaccount create failure"),
					)
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(100)
	r := newUnitReconciler(scheme)
	r.Client = fc
	r.JobBuilder = NewJobBuilder(fc)
	r.Recorder = recorder

	result, err := r.createTaskJob(ctx, task, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Errorf("expected phase Running, got %s", task.Status.Phase)
	}
	if task.Status.JobName == "" {
		t.Error("expected JobName to be set")
	}

	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: task.Status.JobName, Namespace: task.Namespace}, job); err != nil {
		t.Fatalf("expected Job to be created despite RBAC warning: %v", err)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, corev1.EventTypeWarning) || !strings.Contains(event, workerRBACReconcileFailedReason) {
			t.Fatalf("expected %s Warning event, got %q", workerRBACReconcileFailedReason, event)
		}
	default:
		t.Fatalf("expected %s Warning event", workerRBACReconcileFailedReason)
	}
}

func TestCreateTaskJob_WithProvider(t *testing.T) {
	scheme := newTestScheme()
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "prov1", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type: corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{
				Name: "prov-secret",
			},
		},
		Status: corev1alpha1.ProviderStatus{Ready: true},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-prov-job",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				Prompt:      "hello",
				ProviderRef: &corev1alpha1.ProviderReference{Name: "prov1"},
			},
		},
	}
	r := newUnitReconciler(scheme, task, provider)
	_, err := r.createTaskJob(context.Background(), task, nil, provider)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Errorf("expected phase Running, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// completeTask
// ---------------------------------------------------------------------------

func TestCompleteTask_Succeeded(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-succ", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("expected 1s requeue, got %v", result.RequeueAfter)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Errorf("expected Succeeded, got %s", task.Status.Phase)
	}
	if task.Status.CompletionTime == nil {
		t.Error("expected CompletionTime to be set")
	}
}

func TestCompleteTask_Failed(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-fail", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseFailed, "failed")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected Failed, got %s", task.Status.Phase)
	}
}

func TestCompleteTask_Cancelled(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-cancel", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseCancelled, "cancelled")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseCancelled {
		t.Errorf("expected Cancelled, got %s", task.Status.Phase)
	}
}

func TestCompleteTask_WithSessionRef(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-sess", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name:   "sess1",
				Append: true,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// retryTask
// ---------------------------------------------------------------------------

func TestRetryTask(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-job", Namespace: "default"},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-t", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:        corev1alpha1.TaskTypeAI,
			RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 3},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:    corev1alpha1.TaskPhaseRunning,
			JobName:  "retry-job",
			Attempts: 1,
		},
	}
	r := newUnitReconciler(scheme, task, job)
	result, err := r.retryTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive requeue delay")
	}
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected Pending, got %s", task.Status.Phase)
	}
	if task.Status.JobName != "" {
		t.Error("expected JobName to be cleared")
	}
}

func TestRetryTask_NoExistingJob(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-nojob", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "nonexistent",
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.retryTask(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected Pending, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// acquireSessionLock
// ---------------------------------------------------------------------------

func TestAcquireSessionLock_NoSessionRef(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lock-none", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)
	result, err, locked := r.acquireSessionLock(context.Background(), task)
	if locked {
		t.Error("expected locked=false when no sessionRef")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestAcquireSessionLock_SessionNotExist_CreateTrue(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lock-create", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name:   "new-sess",
				Create: true,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err, locked := r.acquireSessionLock(context.Background(), task)
	if locked {
		t.Error("expected locked=false after acquiring lock on new session")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAcquireSessionLock_SessionNotExist_CreateFalse(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lock-nocreat", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name:   "nonexist-sess",
				Create: false,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err, locked := r.acquireSessionLock(context.Background(), task)
	if !locked {
		t.Error("expected locked=true after terminal failure")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "session nonexist-sess not found and create=false") {
		t.Fatalf("message = %q, want missing session failure", updated.Status.Message)
	}
}

func TestAcquireSessionLock_GatewayOwnershipMismatchFailsTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "forked-gateway-task", Namespace: "default", UID: "forked-task-uid"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "gateway-session", Create: false, Append: false,
				MaxMessages:      store.GatewayTranscriptMessageLimit,
				ThroughMessageID: store.GatewayUserMessageID("gateway-event-pending"),
				PromptIncluded:   true,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	sqliteStore, ok := r.ResultStore.(*sqlite.Store)
	if !ok {
		t.Fatalf("ResultStore = %T, want *sqlite.Store", r.ResultStore)
	}
	now := time.Now().UTC().Truncate(time.Second)
	_, _, err := sqliteStore.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{
		Event: store.GatewayEvent{
			ID: "gateway-event", Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
			BindingName: "room", BindingUID: "binding-uid", ExternalEventID: "external-event",
			ProtocolVersion: "orka.gateway.v1", EventType: "text", AccountID: "acct", ContextID: "room",
			SenderID: "sender", Text: "hello", ReplyTarget: "room", SessionName: "gateway-session",
			TaskName: "admitted-gateway-task", ReceivedAt: now, NextAttemptAt: now,
			ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
		},
		AppendUserMessage: true,
		PendingLimit:      100,
	})
	if err != nil {
		t.Fatalf("AdmitGatewayEvent: %v", err)
	}

	_, err, locked := r.acquireSessionLock(context.Background(), task)
	if !locked {
		t.Error("expected locked=true after terminal ownership failure")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "not the admitted owner of gateway session") {
		t.Fatalf("message = %q, want gateway ownership failure", updated.Status.Message)
	}
}

func TestAcquireSessionLock_ModifiedGatewaySessionPolicyFailsTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "gateway-policy-task", Namespace: "default", UID: "gateway-policy-uid"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "mutated-session", Create: true, Append: true, MaxMessages: 1,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	sqliteStore := r.ResultStore.(*sqlite.Store)
	now := time.Now().UTC().Truncate(time.Second)
	event := store.GatewayEvent{
		ID: "gateway-policy-event", Namespace: "default", NamespaceUID: "namespace-uid",
		GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat", BindingName: "room", BindingUID: "binding-uid",
		ExternalEventID: "gateway-policy-external", ProtocolVersion: "orka.gateway.v1", EventType: "text",
		State: store.GatewayEventTaskCreated, AccountID: "acct", ContextID: "room", SenderID: "sender",
		Text: "hello", ReplyTarget: "room", SessionName: "canonical-session", TaskName: task.Name,
		TaskUID: string(task.UID), ReceivedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{Event: event}); err != nil {
		t.Fatal(err)
	}
	_, err, locked := r.acquireSessionLock(context.Background(), task)
	if !locked || err != nil {
		t.Fatalf("modified gateway policy acquire = (err=%v, locked=%v)", err, locked)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed || !strings.Contains(updated.Status.Message, "session policy was modified") {
		t.Fatalf("updated task status = %#v", updated.Status)
	}
}

func TestAcquireSessionLock_GatewayOwnershipPendingRequeues(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "admitted-gateway-task", Namespace: "default", UID: "task-uid"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{
				Name: "gateway-session", Create: false, Append: false,
				MaxMessages:      store.GatewayTranscriptMessageLimit,
				ThroughMessageID: store.GatewayUserMessageID("gateway-event-pending"),
				PromptIncluded:   true,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	sqliteStore, ok := r.ResultStore.(*sqlite.Store)
	if !ok {
		t.Fatalf("ResultStore = %T, want *sqlite.Store", r.ResultStore)
	}
	now := time.Now().UTC().Truncate(time.Second)
	event := store.GatewayEvent{
		ID: "gateway-event-pending", Namespace: "default", NamespaceUID: "namespace-uid", GatewayUID: "gateway-uid", GatewayGeneration: 1, GatewayName: "chat",
		BindingName: "room", BindingUID: "binding-uid", ExternalEventID: "external-event-pending",
		ProtocolVersion: "orka.gateway.v1", EventType: "text", AccountID: "acct", ContextID: "room",
		SenderID: "sender", Text: "hello", ReplyTarget: "room", SessionName: "gateway-session",
		TaskName: task.Name, ReceivedAt: now, NextAttemptAt: now,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := sqliteStore.AdmitGatewayEvent(context.Background(), store.GatewayEventAdmission{
		Event: event, AppendUserMessage: true, PendingLimit: 100,
	}); err != nil {
		t.Fatalf("AdmitGatewayEvent: %v", err)
	}
	if _, err := sqliteStore.ClaimNextGatewayEvent(context.Background(), "", "dispatcher", now, time.Minute); err != nil {
		t.Fatalf("ClaimNextGatewayEvent: %v", err)
	}

	result, err, locked := r.acquireSessionLock(context.Background(), task)
	if !locked || err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("pending ownership acquire = (result=%+v, err=%v, locked=%v)", result, err, locked)
	}
	current := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, current); err != nil {
		t.Fatalf("Get current task: %v", err)
	}
	if current.Status.Phase == corev1alpha1.TaskPhaseFailed {
		t.Fatalf("task failed while ownership linkage was pending: %#v", current.Status)
	}
	if err := sqliteStore.MarkGatewayEventTaskCreated(
		context.Background(), event.Namespace, event.ID, event.TaskName, string(task.UID), "dispatcher", now,
	); err != nil {
		t.Fatalf("MarkGatewayEventTaskCreated: %v", err)
	}
	if result, err, locked := r.acquireSessionLock(context.Background(), task); locked || err != nil {
		t.Fatalf("linked ownership acquire = (result=%+v, err=%v, locked=%v)", result, err, locked)
	}
}

// ---------------------------------------------------------------------------
// resolveProvider
// ---------------------------------------------------------------------------

func TestResolveProvider_Found(t *testing.T) {
	scheme := newTestScheme()
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:      corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{Name: "sec1"},
		},
		Status: corev1alpha1.ProviderStatus{Ready: true},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-prov", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "p1"},
			},
		},
	}
	r := newUnitReconciler(scheme, provider, task)
	got, err := r.resolveProvider(context.Background(), task, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.Name != "p1" {
		t.Errorf("expected provider p1, got %v", got)
	}
}

func TestResolveProvider_NotReady(t *testing.T) {
	scheme := newTestScheme()
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "p-notready", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:      corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{Name: "sec1"},
		},
		Status: corev1alpha1.ProviderStatus{Ready: false, Message: "not configured"},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-prov-nr", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "p-notready"},
			},
		},
	}
	r := newUnitReconciler(scheme, provider, task)
	_, err := r.resolveProvider(context.Background(), task, nil)
	if err == nil {
		t.Error("expected error for not-ready provider")
	}
}

func TestResolveProvider_NotFound(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-prov-miss", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "nonexistent"},
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.resolveProvider(context.Background(), task, nil)
	if err == nil {
		t.Error("expected error for missing provider")
	}
}

// ---------------------------------------------------------------------------
// handleScheduled — additional paths
// ---------------------------------------------------------------------------

func TestHandleScheduled_NotYetTime(t *testing.T) {
	scheme := newTestScheme()
	// Use a schedule far in the future
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-future",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "0 0 1 1 *", // Jan 1 only
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseScheduled},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter for future schedule")
	}
}

func TestHandleScheduled_WithTimeZone(t *testing.T) {
	scheme := newTestScheme()
	tz := "America/New_York"
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-tz",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "0 0 1 1 *",
			TimeZone: &tz,
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseScheduled},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter")
	}
}

func TestHandleScheduled_MissedDeadline(t *testing.T) {
	scheme := newTestScheme()
	deadline := int64(1)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-missed",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeContainer,
			Schedule:                "* * * * *", // every minute
			StartingDeadlineSeconds: &deadline,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: new(metav1.NewTime(time.Now().Add(-24 * time.Hour))),
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter after missed deadline")
	}
}

func TestHandleScheduled_MissedDeadlineReturnsStatusUpdateError(t *testing.T) {
	scheme := newTestScheme()
	deadline := int64(1)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-missed-status-error",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeContainer,
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: &deadline,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: new(metav1.NewTime(time.Now().Add(-24 * time.Hour))),
		},
	}
	r := newUnitReconciler(scheme, task)
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	statusErr := errors.New("injected schedule status update failure")
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, delegate client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subResourceName == "status" {
				if current, ok := obj.(*corev1alpha1.Task); ok && current.Name == task.Name {
					return statusErr
				}
			}
			return delegate.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	})

	_, err := r.handleScheduled(context.Background(), task)
	if !errors.Is(err, statusErr) {
		t.Fatalf("handleScheduled() error = %v, want status update error", err)
	}
}

func TestHandleScheduled_ConcurrencyForbid(t *testing.T) {
	tests := []struct {
		phase       corev1alpha1.TaskPhase
		wantBlocked bool
	}{
		{phase: corev1alpha1.TaskPhaseRunning, wantBlocked: true},
		{phase: corev1alpha1.TaskPhaseFinalizing, wantBlocked: true},
		{phase: corev1alpha1.TaskPhaseSucceeded, wantBlocked: false},
	}

	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			scheme := newTestScheme()
			parentName := "sched-concur-" + strings.ToLower(string(tt.phase))
			activeChild := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      parentName + "-child",
					Namespace: "default",
					Labels:    map[string]string{labels.LabelParentTask: parentName},
				},
				Status: corev1alpha1.TaskStatus{Phase: tt.phase},
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:              parentName,
					Namespace:         "default",
					UID:               types.UID("uid-" + strings.ToLower(string(tt.phase))),
					CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
				},
				Spec: corev1alpha1.TaskSpec{
					Type:                    corev1alpha1.TaskTypeContainer,
					Schedule:                "* * * * *",
					ConcurrencyPolicy:       corev1alpha1.ForbidConcurrent,
					StartingDeadlineSeconds: new(int64(300)),
				},
				Status: corev1alpha1.TaskStatus{
					Phase:            corev1alpha1.TaskPhaseScheduled,
					LastScheduleTime: new(metav1.NewTime(time.Now().Add(-2 * time.Minute))),
				},
			}
			r := newUnitReconciler(scheme, task, activeChild)
			result, err := r.handleScheduled(context.Background(), task)
			if err != nil {
				t.Fatalf("handleScheduled() error = %v", err)
			}
			if result.RequeueAfter <= 0 {
				t.Error("expected positive RequeueAfter")
			}

			children := &corev1alpha1.TaskList{}
			if err := r.List(context.Background(), children,
				client.InNamespace(task.Namespace),
				client.MatchingLabels{labels.LabelParentTask: labels.SelectorValue(task.Name)},
			); err != nil {
				t.Fatalf("list scheduled children: %v", err)
			}
			wantChildren := 2
			if tt.wantBlocked {
				wantChildren = 1
			}
			if len(children.Items) != wantChildren {
				t.Fatalf("scheduled children = %d, want %d", len(children.Items), wantChildren)
			}
		})
	}
}

func TestHandleScheduled_CreateChildTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-create",
			Namespace:         "default",
			UID:               "12345678-abcd-efgh-ijkl-1234567890ab",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeContainer,
			Image:                   "busybox:latest",
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: new(int64(300)),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: new(metav1.NewTime(time.Now().Add(-2 * time.Minute))),
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter after creating child task")
	}
	if task.Status.LastScheduleTime == nil {
		t.Error("expected LastScheduleTime to be updated")
	}
}

func TestHandleScheduled_RefreshesRuntimeRefPolicyFromAPIReader(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name         string
		allowedTools []string
	}{
		{name: "current policy", allowedTools: []string{"read_current"}},
		{name: "explicit deny all", allowedTools: []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			lastSchedule := metav1.NewTime(time.Now().Add(-2 * time.Minute))
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "sched-runtime-policy",
					Namespace:         "default",
					UID:               types.UID("scheduled-runtime-policy"),
					CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
				},
				Spec: corev1alpha1.TaskSpec{
					Type:                    corev1alpha1.TaskTypeAgent,
					AgentRef:                &corev1alpha1.AgentReference{Name: "external-agent"},
					AgentRuntime:            &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"stale_tool"}},
					Schedule:                "* * * * *",
					StartingDeadlineSeconds: new(int64(300)),
				},
				Status: corev1alpha1.TaskStatus{
					Phase:            corev1alpha1.TaskPhaseScheduled,
					LastScheduleTime: &lastSchedule,
				},
			}
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: task.Namespace},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
				}},
			}
			staleRuntime := scheduledTestAgentRuntime(contract, []string{"stale_tool"})
			currentRuntime := scheduledTestAgentRuntime(contract, tt.allowedTools)

			r := newUnitReconciler(scheme, task, agent.DeepCopy(), staleRuntime)
			r.APIReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, currentRuntime).Build()

			if _, err := r.handleScheduled(context.Background(), task); err != nil {
				t.Fatalf("handleScheduled() error = %v", err)
			}

			children := &corev1alpha1.TaskList{}
			if err := r.List(context.Background(), children, client.InNamespace(task.Namespace), client.MatchingLabels{
				labels.LabelParentTask: labels.SelectorValue(task.Name),
			}); err != nil {
				t.Fatalf("list scheduled children: %v", err)
			}
			if len(children.Items) != 1 {
				t.Fatalf("scheduled children = %d, want 1", len(children.Items))
			}
			got := children.Items[0].Spec.AgentRuntime
			if got == nil || got.AllowedTools == nil || !slices.Equal(got.AllowedTools, tt.allowedTools) {
				t.Fatalf("child allowedTools = %#v, want %#v", got, tt.allowedTools)
			}
			if task.Spec.AgentRuntime == nil || !slices.Equal(task.Spec.AgentRuntime.AllowedTools, []string{"stale_tool"}) {
				t.Fatalf("parent allowedTools = %#v, want unchanged stale policy", task.Spec.AgentRuntime)
			}
		})
	}
}

func TestHandleScheduled_RuntimeRefPolicyFailureHasNoSideEffects(t *testing.T) {
	scheme := newTestScheme()
	lastSchedule := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-runtime-policy-failure",
			Namespace:         "default",
			UID:               types.UID("scheduled-runtime-policy-failure"),
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeAgent,
			AgentRef:                &corev1alpha1.AgentReference{Name: "external-agent"},
			AgentRuntime:            &corev1alpha1.AgentRuntimeSpec{AllowedTools: []string{"stale_tool"}},
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: new(int64(300)),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: &lastSchedule,
		},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: task.Namespace},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "missing-runtime"},
		}},
	}
	r := newUnitReconciler(scheme, task)
	r.APIReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent).Build()

	_, err := r.handleScheduled(context.Background(), task)
	if err == nil || !strings.Contains(err.Error(), "refreshing scheduled child AgentRuntime policy") {
		t.Fatalf("handleScheduled() error = %v, want policy refresh failure", err)
	}
	children := &corev1alpha1.TaskList{}
	if err := r.List(context.Background(), children, client.InNamespace(task.Namespace), client.MatchingLabels{
		labels.LabelParentTask: labels.SelectorValue(task.Name),
	}); err != nil {
		t.Fatalf("list scheduled children: %v", err)
	}
	if len(children.Items) != 0 {
		t.Fatalf("scheduled children = %d, want 0", len(children.Items))
	}
	if task.Status.LastScheduleTime == nil || !task.Status.LastScheduleTime.Equal(&lastSchedule) {
		t.Fatalf("LastScheduleTime = %v, want unchanged %v", task.Status.LastScheduleTime, lastSchedule)
	}
}

func scheduledTestAgentRuntime(
	contract corev1alpha1.AgentRuntimeContractVersion,
	allowedTools []string,
) *corev1alpha1.AgentRuntime {
	return &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: "default"},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind:    "codex",
					Model:           "gpt-5.6",
					WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:    append([]string{}, allowedTools...),
					DisallowedTools: []string{},
				},
			},
		},
	}
}

func TestHandleScheduled_CopiesCoordinationToolInjectionDisableAnnotation(t *testing.T) {
	scheme := newTestScheme()
	lastSchedule := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-pr-monitor",
			Namespace:         "default",
			UID:               "12345678-abcd-efgh-ijkl-1234567890ad",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
			Annotations: map[string]string{
				labels.AnnotationDisableCoordinationToolInject: scheduledRunLabelValue,
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeAI,
			Prompt:                  "review this PR",
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: new(int64(300)),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: &lastSchedule,
		},
	}

	ctx := context.Background()
	r := newUnitReconciler(scheme, task)
	if _, err := r.handleScheduled(ctx, task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var childList corev1alpha1.TaskList
	if err := r.List(ctx, &childList, client.InNamespace(task.Namespace), client.MatchingLabels{
		labels.LabelParentTask: labels.SelectorValue(task.Name),
	}); err != nil {
		t.Fatalf("list child tasks: %v", err)
	}
	if len(childList.Items) != 1 {
		t.Fatalf("expected 1 scheduled child task, got %d", len(childList.Items))
	}
	child := childList.Items[0]
	if child.Annotations[labels.AnnotationDisableCoordinationToolInject] != scheduledRunLabelValue {
		t.Fatalf("child coordination injection disable annotation = %q, want %q",
			child.Annotations[labels.AnnotationDisableCoordinationToolInject], scheduledRunLabelValue)
	}
	if child.Annotations[labels.AnnotationParentTaskName] != task.Name {
		t.Fatalf("child parent task annotation = %q, want %q", child.Annotations[labels.AnnotationParentTaskName], task.Name)
	}
}

func TestHandleScheduled_StampsChildWithSchedulerTrace(t *testing.T) {
	if shutdown, err := orkatracing.Init("test", false); err == nil {
		t.Cleanup(func() { _ = shutdown(context.Background()) })
	} else {
		t.Fatalf("init tracing: %v", err)
	}
	testutil.NewSpanHarness(t)
	ctx, span := orkatracing.Tracer("test").Start(context.Background(), "scheduler")
	defer span.End()

	scheme := newTestScheme()
	lastSchedule := metav1.NewTime(time.Now().Add(-2 * time.Minute).UTC())
	startingDeadlineSeconds := int64(300)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-trace",
			Namespace:         "default",
			UID:               "12345678-abcd-efgh-ijkl-1234567890ab",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour).UTC()),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeAI,
			Prompt:                  "hello",
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: &startingDeadlineSeconds,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: &lastSchedule,
		},
	}
	r := newUnitReconciler(scheme, task)

	if _, err := r.handleScheduled(ctx, task); err != nil {
		t.Fatalf("handleScheduled() error = %v", err)
	}

	var childList corev1alpha1.TaskList
	if err := r.List(ctx, &childList, client.InNamespace(task.Namespace), client.MatchingLabels{
		labels.LabelParentTask: labels.SelectorValue(task.Name),
	}); err != nil {
		t.Fatalf("list child tasks: %v", err)
	}
	if len(childList.Items) != 1 {
		t.Fatalf("expected 1 scheduled child task, got %d", len(childList.Items))
	}
	if got := childList.Items[0].Annotations[labels.AnnotationTraceParent]; got == "" {
		t.Fatalf("scheduled child missing %s annotation", labels.AnnotationTraceParent)
	}
}

func TestHandleScheduled_ExistingChildTaskStillUpdatesScheduleStatus(t *testing.T) {
	scheme := newTestScheme()
	lastSchedule := metav1.NewTime(time.Now().Add(-2 * time.Minute).UTC())
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sched-existing-child",
			Namespace:         "default",
			UID:               "12345678-abcd-efgh-ijkl-1234567890ac",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour).UTC()),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:                    corev1alpha1.TaskTypeContainer,
			Image:                   "busybox:latest",
			Schedule:                "* * * * *",
			StartingDeadlineSeconds: new(int64(300)),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseScheduled,
			LastScheduleTime: &lastSchedule,
		},
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(task.Spec.Schedule)
	if err != nil {
		t.Fatalf("parse cron: %v", err)
	}
	scheduledTime := schedule.Next(lastSchedule.Time)
	childName := fmt.Sprintf("%s-%d", task.Name, scheduledTime.Unix())
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName,
			Namespace: "default",
			Labels: map[string]string{
				labels.LabelParentTask:   labels.SelectorValue(task.Name),
				labels.LabelScheduledRun: scheduledRunLabelValue,
			},
			Annotations: map[string]string{
				labels.AnnotationParentTaskName: task.Name,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}

	r := newUnitReconciler(scheme, task, child)
	result, err := r.handleScheduled(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive RequeueAfter after handling existing child")
	}
	if task.Status.LastScheduleTime == nil || !task.Status.LastScheduleTime.Time.Equal(scheduledTime) {
		t.Fatalf("expected LastScheduleTime %s, got %v", scheduledTime.Format(time.RFC3339), task.Status.LastScheduleTime)
	}
	if task.Status.NextScheduleTime == nil {
		t.Fatal("expected NextScheduleTime to be updated")
	}
}

// ---------------------------------------------------------------------------
// handleAutonomousIteration
// ---------------------------------------------------------------------------

func TestHandleAutonomousIteration_GoalComplete(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:    true,
				MaxIterations: 10,
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "auto-agent"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			Iteration: 2,
		},
	}
	r := newUnitReconciler(scheme, task, agent)
	// Save plan state indicating goal is complete
	_ = r.PlanStore.SavePlan(context.Background(), "default", "auto-task", &store.PlanState{
		GoalComplete: true,
		Summary:      "All tasks done",
	})
	_, err := r.handleAutonomousIteration(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Errorf("expected Succeeded, got %s", task.Status.Phase)
	}
}

func TestHandleAutonomousIteration_MaxIterations(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-agent2", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:    true,
				MaxIterations: 3,
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-max", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "auto-agent2"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			Iteration: 2,
		},
	}
	r := newUnitReconciler(scheme, task, agent)
	_, err := r.handleAutonomousIteration(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Errorf("expected Succeeded at max iterations, got %s", task.Status.Phase)
	}
}

func TestHandleAutonomousIteration_Continue(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-agent3", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:    true,
				MaxIterations: 10,
			},
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-job", Namespace: "default"},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-cont", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "auto-agent3"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			Iteration: 1,
			JobName:   "auto-job",
		},
	}
	r := newUnitReconciler(scheme, task, agent, job)
	result, err := r.handleAutonomousIteration(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected Pending for next iteration, got %s", task.Status.Phase)
	}
	if task.Status.Iteration != 2 {
		t.Errorf("expected iteration 2, got %d", task.Status.Iteration)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleAutonomousIteration_Suspended(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-agent4", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:    true,
				MaxIterations: 10,
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-susp", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "auto-agent4"},
			Suspend:  new(true),
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			Iteration: 1,
		},
	}
	r := newUnitReconciler(scheme, task, agent)
	result, err := r.handleAutonomousIteration(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Errorf("expected 30s requeue for suspended, got %v", result.RequeueAfter)
	}
}

func TestHandleAutonomousIteration_AgentNotFound(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-noagent", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "missing-agent"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			Iteration: 0,
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.handleAutonomousIteration(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Errorf("expected Failed, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

func TestReconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestReconcile_AddFinalizer(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "rec-fin", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-fin", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("expected 1s requeue after adding finalizer, got %v", result.RequeueAfter)
	}
}

func TestReconcile_AddFinalizerUsesMetadataOnlyPatch(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "rec-fin-metadata", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeAgent,
			Timeout: &metav1.Duration{Duration: 12 * time.Minute},
		},
	}
	r := newUnitReconciler(scheme, task)
	base, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("fake client does not implement client.WithWatch")
	}
	patchInspected := false
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, delegate client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			if _, ok := object.(*corev1alpha1.Task); ok {
				return errors.New("full Task update is forbidden while adding the finalizer")
			}
			return delegate.Update(ctx, object, options...)
		},
		Patch: func(ctx context.Context, delegate client.WithWatch, object client.Object, patch client.Patch, options ...client.PatchOption) error {
			if _, ok := object.(*corev1alpha1.Task); ok {
				data, err := patch.Data(object)
				if err != nil {
					return err
				}
				var body map[string]any
				if err := json.Unmarshal(data, &body); err != nil {
					return err
				}
				if _, present := body["spec"]; present {
					return fmt.Errorf("finalizer patch unexpectedly contains spec: %s", data)
				}
				metadata, ok := body["metadata"].(map[string]any)
				if !ok {
					return fmt.Errorf("finalizer patch has no metadata: %s", data)
				}
				if _, present := metadata["finalizers"]; !present {
					return fmt.Errorf("finalizer patch has no finalizers: %s", data)
				}
				patchInspected = true
			}
			return delegate.Patch(ctx, object, patch, options...)
		},
	})

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: task.Name, Namespace: task.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %s, want 1s", result.RequeueAfter)
	}
	if !patchInspected {
		t.Fatal("finalizer patch was not inspected")
	}
	var updated corev1alpha1.Task
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, &updated); err != nil {
		t.Fatalf("get updated Task: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&updated, labels.TaskFinalizer) {
		t.Fatal("Task finalizer was not added")
	}
	if updated.Spec.Timeout == nil || updated.Spec.Timeout.Duration != 12*time.Minute {
		t.Fatalf("Task timeout = %#v, want 12m", updated.Spec.Timeout)
	}
}

func TestReconcile_InitializeStatus(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-init",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-init", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("expected 1s requeue after initializing status, got %v", result.RequeueAfter)
	}
}

func TestReconcile_CompletedPhase(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-comp",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-comp", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// ---------------------------------------------------------------------------
// handlePending — transaction token pending
// ---------------------------------------------------------------------------

func TestHandlePending_TransactionTokenPendingRequeuesWithoutJob(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-token",
			Namespace: "default",
			Annotations: map[string]string{
				labels.AnnotationTransactionTokenPending: "true",
			},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("expected 1s requeue while transaction token is pending, got %v", result.RequeueAfter)
	}

	jobs := &batchv1.JobList{}
	if err := r.List(context.Background(), jobs, client.InNamespace(task.Namespace)); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected no Job to be created while transaction token is pending, got %d", len(jobs.Items))
	}
}

func TestHandlePending_BuiltInAgentRuntimeFailsClosedWhenACPDisabled(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-no-secret", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent)

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "no fallback execution path") {
		t.Fatalf("message = %q, want fail-closed ACP-disabled error", updated.Status.Message)
	}
	assertNoJobsForTask(t, r, task)
}

func TestHandlePending_ExternalRuntimeRefQueuesDurableAttempt(t *testing.T) {
	fixture := newExternalACPDispatchFixture(t)
	fixture.queueTask(t, "external-task", types.UID("external-task-uid"), "do work", nil)
}

func TestHandlePending_AgentRuntimeWithResourcesFailsBeforeJobBackend(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-with-resources", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent)
	r.ACPRuntimeEnabled = true

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "custom Kubernetes resources") {
		t.Fatalf("message = %q, want resource unsupported failure", updated.Status.Message)
	}
	assertNoJobsForTask(t, r, task)
}

func TestHandlePending_AgentRuntimeUnsupportedPlannerFeaturesFailBeforeJobBackend(t *testing.T) {
	tests := []struct {
		name       string
		mutateTask func(*corev1alpha1.Task)
		want       string
	}{
		{
			name: "transaction",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.Transaction = &corev1alpha1.TaskTransaction{ID: "txn-1"}
			},
			want: "transaction token delegation",
		},
		{
			name: "execution placement",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.Execution = &corev1alpha1.ExecutionSpec{RuntimeClassName: "kata"}
			},
			want: "execution placement",
		},
		{
			name: "cross namespace prior task",
			mutateTask: func(task *corev1alpha1.Task) {
				task.Spec.PriorTaskRef = &corev1alpha1.PriorTaskReference{Name: "prior", Namespace: "other"}
			},
			want: "use sessionRef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
				Spec: corev1alpha1.AgentSpec{
					Runtime: &corev1alpha1.AgentCLIRuntime{
						Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
					},
				},
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "agent-" + strings.ReplaceAll(tt.name, " ", "-"), Namespace: defaultNS},
				Spec: corev1alpha1.TaskSpec{
					Type:     corev1alpha1.TaskTypeAgent,
					AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
					Prompt:   "do work",
				},
				Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
			}
			tt.mutateTask(task)
			r := newUnitReconciler(scheme, task, agent)
			r.ACPRuntimeEnabled = true
			r.EnforceNamespaceIsolation = true

			result, err := r.handlePending(context.Background(), task)
			if err != nil {
				t.Fatalf("handlePending() error = %v", err)
			}
			if result.RequeueAfter != time.Second {
				t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
			}

			updated := &corev1alpha1.Task{}
			if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
				t.Fatalf("Get updated task: %v", err)
			}
			if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
				t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
			}
			if !strings.Contains(updated.Status.Message, tt.want) {
				t.Fatalf("message = %q, want %q", updated.Status.Message, tt.want)
			}
			assertNoJobsForTask(t, r, task)
		})
	}
}

func TestHandlePending_AgentRuntimeValidWorkspaceFailsBeforeJobBackend(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	template := &sandboxextv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: runtimePoolSandboxTemplateSuffix, Namespace: defaultNS},
	}
	warmPool := &sandboxextv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{Name: template.Name, Namespace: defaultNS},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workspace-valid-but-unsupported",
			Namespace: defaultNS,
			UID:       "task-uid-workspace-template-ref",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					Enabled:  true,
					Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
					TemplateRef: &corev1alpha1.WorkspaceTemplateReference{
						Name: template.Name,
					},
				},
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent, template, warmPool)
	r.AgentSandboxEnabled = true
	r.ACPRuntimeEnabled = true

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, acpWorkspaceTestTemplateRefForbiddenError) {
		t.Fatalf("message = %q, want templateRef rejection", updated.Status.Message)
	}
	assertExecutionWorkspaceValidationFailedStatus(
		t,
		updated.Status.ExecutionWorkspace,
		corev1alpha1.WorkspaceProviderAgentSandbox,
		template.Name,
		acpWorkspaceTestTemplateRefForbiddenError,
	)
	assertNoJobsForTask(t, r, task)
}

func TestHandlePending_ExecutionWorkspaceValidationFailureSetsWorkspaceStatus(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workspace-validation-fails",
			Namespace: defaultNS,
			UID:       "task-uid-workspace-validation",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					Enabled:  true,
					Provider: corev1alpha1.WorkspaceProviderSubstrate,
					TemplateRef: &corev1alpha1.WorkspaceTemplateReference{
						Name: "orka-codex",
					},
				},
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent)
	r.ACPRuntimeEnabled = true

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	assertExecutionWorkspaceValidationFailedStatus(t, updated.Status.ExecutionWorkspace, corev1alpha1.WorkspaceProviderSubstrate, "orka-codex", "provider substrate is disabled")
	assertNoJobsForTask(t, r, task)
}

func TestHandlePending_ExecutionWorkspaceUnsupportedProviderStatusOmitsProviderDetails(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCodex},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace-unsupported-provider", Namespace: defaultNS},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					Enabled:  true,
					Provider: corev1alpha1.WorkspaceProvider("provider-native"),
				},
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent)

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	status := updated.Status.ExecutionWorkspace
	if status == nil {
		t.Fatal("ExecutionWorkspace status is nil")
	}
	if status.Provider != "" || status.TemplateRef != nil {
		t.Fatalf("unsupported provider status provider=%q template=%#v, want provider-neutral empty details", status.Provider, status.TemplateRef)
	}
	if status.Phase != corev1alpha1.ExecutionWorkspacePhaseFailed || status.Reason != corev1alpha1.ExecutionWorkspaceReasonValidationFailed {
		t.Fatalf("workspace status phase/reason = %q/%q, want Failed/WorkspaceValidationFailed", status.Phase, status.Reason)
	}
	if !strings.Contains(status.Message, "unsupported execution workspace provider") {
		t.Fatalf("workspace status message = %q, want unsupported provider", status.Message)
	}
	assertNoJobsForTask(t, r, task)
}

func TestHandlePending_ExecutionWorkspaceDispatchDisabledFailsClosed(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: defaultNS},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:            corev1alpha1.AgentRuntimeCodex,
				ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
			},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workspace-dispatch-disabled",
			Namespace: defaultNS,
			UID:       "task-uid-workspace-dispatch",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: agent.Name},
			Prompt:   "do work",
			Execution: &corev1alpha1.ExecutionSpec{
				Workspace: &corev1alpha1.ExecutionWorkspaceSpec{
					Enabled:  true,
					Provider: corev1alpha1.WorkspaceProviderAgentSandbox,
				},
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent)
	r.AgentSandboxEnabled = true
	r.ACPRuntimeEnabled = true

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "acp-workspace-dispatch-enabled") {
		t.Fatalf("message = %q, want workspace dispatch disabled failure", updated.Status.Message)
	}
	status := updated.Status.ExecutionWorkspace
	if status == nil {
		t.Fatal("ExecutionWorkspace status is nil")
	}
	if status.Phase != corev1alpha1.ExecutionWorkspacePhaseFailed || status.Reason != corev1alpha1.ExecutionWorkspaceReasonValidationFailed {
		t.Fatalf("workspace status phase/reason = %q/%q, want Failed/WorkspaceValidationFailed", status.Phase, status.Reason)
	}
	if !strings.Contains(status.Message, "acp-workspace-dispatch-enabled") {
		t.Fatalf("workspace status message = %q, want dispatch disabled", status.Message)
	}
	assertNoJobsForTask(t, r, task)
}

func assertExecutionWorkspaceValidationFailedStatus(t *testing.T, status *corev1alpha1.ExecutionWorkspaceStatus, provider corev1alpha1.WorkspaceProvider, templateName, messageSubstring string) {
	t.Helper()
	if status == nil {
		t.Fatal("ExecutionWorkspace status is nil")
	}
	if status.Provider != provider {
		t.Fatalf("workspace provider = %q, want %q", status.Provider, provider)
	}
	if status.Phase != corev1alpha1.ExecutionWorkspacePhaseFailed {
		t.Fatalf("workspace phase = %q, want %q", status.Phase, corev1alpha1.ExecutionWorkspacePhaseFailed)
	}
	if status.Reason != corev1alpha1.ExecutionWorkspaceReasonValidationFailed {
		t.Fatalf("workspace reason = %q, want %q", status.Reason, corev1alpha1.ExecutionWorkspaceReasonValidationFailed)
	}
	if status.TemplateRef == nil || status.TemplateRef.Name != templateName || status.TemplateRef.Namespace != defaultNS {
		t.Fatalf("workspace templateRef = %#v, want default/%s", status.TemplateRef, templateName)
	}
	if status.ReusePolicy != corev1alpha1.WorkspaceReusePolicyNone {
		t.Fatalf("workspace reusePolicy = %q, want %q", status.ReusePolicy, corev1alpha1.WorkspaceReusePolicyNone)
	}
	if status.CleanupPolicy != corev1alpha1.WorkspaceCleanupPolicyDelete {
		t.Fatalf("workspace cleanupPolicy = %q, want %q", status.CleanupPolicy, corev1alpha1.WorkspaceCleanupPolicyDelete)
	}
	if !strings.Contains(status.Message, messageSubstring) {
		t.Fatalf("workspace message = %q, want substring %q", status.Message, messageSubstring)
	}
	if status.LastUpdateTime == nil {
		t.Fatal("workspace LastUpdateTime is nil")
	}
}

func assertNoJobsForTask(t *testing.T, r *TaskReconciler, task *corev1alpha1.Task) {
	t.Helper()
	jobs := &batchv1.JobList{}
	if err := r.List(context.Background(), jobs, client.InNamespace(task.Namespace)); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected no Jobs to be created, got %d", len(jobs.Items))
	}
}

// ---------------------------------------------------------------------------
// handlePending — namespace task limit
// ---------------------------------------------------------------------------

func TestTaskPhaseCountsTowardConcurrency(t *testing.T) {
	tests := []struct {
		phase corev1alpha1.TaskPhase
		want  bool
	}{
		{phase: "", want: false},
		{phase: corev1alpha1.TaskPhasePending, want: true},
		{phase: corev1alpha1.TaskPhaseScheduled, want: false},
		{phase: corev1alpha1.TaskPhaseRunning, want: true},
		{phase: corev1alpha1.TaskPhaseFinalizing, want: true},
		{phase: corev1alpha1.TaskPhaseSucceeded, want: false},
		{phase: corev1alpha1.TaskPhaseFailed, want: false},
		{phase: corev1alpha1.TaskPhaseCancelled, want: false},
	}

	for _, tt := range tests {
		name := string(tt.phase)
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			if got := taskPhaseCountsTowardConcurrency(tt.phase); got != tt.want {
				t.Fatalf("taskPhaseCountsTowardConcurrency(%q) = %t, want %t", tt.phase, got, tt.want)
			}
		})
	}
}

func TestHandlePending_NamespaceTaskLimit(t *testing.T) {
	scheme := newTestScheme()
	// Create active tasks to hit the limit
	active1 := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "active1", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	active2 := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "active2", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pending-limit",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, active1, active2)
	r.MaxTasksPerNamespace = 2
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Errorf("expected 10s requeue at limit, got %v", result.RequeueAfter)
	}
}

func TestHandlePending_NamespaceTaskLimitCountsFinalizing(t *testing.T) {
	scheme := newTestScheme()
	finalizing := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalizing", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFinalizing},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pending-finalizing-limit",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, finalizing)
	r.MaxTasksPerNamespace = 1

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("RequeueAfter = %v, want 10s", result.RequeueAfter)
	}
	assertNoJobsForTask(t, r, task)
}

// ---------------------------------------------------------------------------
// readPodLogs — pods found but no KubeClient causes panic (guarded by caller)
// The readPodLogs method is called by collectResult which checks KubeClient != nil.
// We test this indirectly through collectResult to avoid the nil dereference.
// ---------------------------------------------------------------------------

func TestCollectResult_ContainerTask_NoKubeClient_NoPods(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-collect", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{JobName: "job-collect"},
	}
	r := newUnitReconciler(scheme, task)
	// KubeClient is nil — collectResult should return nil early
	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("expected nil error when KubeClient is nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// handleCompleted — webhook paths
// ---------------------------------------------------------------------------

func TestHandleCompleted_WebhookNotConfigured(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-nowh", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestHandleCompleted_WebhookFails(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-whfail", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			WebhookURL: "http://invalid.nonexistent.local:9999/hook",
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseSucceeded,
			WebhookDelivered: false,
		},
	}
	r := newUnitReconciler(scheme, task)
	r.WebhookNotifier = NewWebhookNotifier()
	result, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should requeue for webhook retry
	if result.RequeueAfter != 30*time.Second {
		t.Errorf("expected 30s requeue for webhook retry, got %v", result.RequeueAfter)
	}
}

// ---------------------------------------------------------------------------
// Reconcile — additional phase paths
// ---------------------------------------------------------------------------

func TestReconcile_ScheduledPhase(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "rec-sched",
			Namespace:         "default",
			Finalizers:        []string{labels.TaskFinalizer},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "0 0 1 1 *",
			Suspend:  new(true),
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseScheduled},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-sched", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive requeue for scheduled task")
	}
}

func TestReconcile_RunningPhase_JobNotFound(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-run",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "nonexistent-job",
		},
	}
	r := newUnitReconciler(scheme, task)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-run", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReconcile_RunningPhase_JobNotFoundWithRetryPolicy(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-run-retry",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:        corev1alpha1.TaskTypeAI,
			RetryPolicy: &corev1alpha1.RetryPolicy{MaxRetries: 2},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:    corev1alpha1.TaskPhaseRunning,
			JobName:  "nonexistent-job",
			Attempts: 1,
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-run-retry", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected positive requeue after scheduling retry, got %v", result.RequeueAfter)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rec-run-retry", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to fetch updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Fatalf("expected phase Pending after retry scheduling, got %s", updated.Status.Phase)
	}
	if updated.Status.JobName != "" {
		t.Fatalf("expected JobName to be cleared after retry scheduling, got %q", updated.Status.JobName)
	}
}

func TestReconcile_FailedPhase(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-fail",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseFailed},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-fail", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestReconcile_CancelledPhase(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rec-cancel",
			Namespace:  "default",
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseCancelled},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rec-cancel", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// ---------------------------------------------------------------------------
// handlePending — scheduled task path
// ---------------------------------------------------------------------------

func TestHandlePending_ScheduledTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pend-sched", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeContainer,
			Schedule: "*/5 * * * *",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected positive requeue for scheduled task")
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseScheduled {
		t.Errorf("expected Scheduled phase, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// handleRunning — autonomous task path
// ---------------------------------------------------------------------------

func TestHandleRunning_AutonomousTask(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-run-agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			Model: &corev1alpha1.ModelConfig{Provider: "openai", Name: "gpt-4"},
			Coordination: &corev1alpha1.CoordinationConfig{
				Autonomous:    true,
				MaxIterations: 5,
			},
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-run-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-run-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "auto-run-agent"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseRunning,
			JobName:   "auto-run-job",
			Iteration: 0,
		},
	}
	r := newUnitReconciler(scheme, task, agent, job)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should advance to next iteration (Pending)
	if task.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected Pending for next iteration, got %s", task.Status.Phase)
	}
}

// ---------------------------------------------------------------------------
// handleRunning — child task with result
// ---------------------------------------------------------------------------

func TestHandleRunning_ChildTaskWithResult(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job2", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-with-result",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-result"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-result", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job2",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	// Save child result
	_ = r.ResultStore.SaveResult(context.Background(), "default", "child-with-result", []byte("child output"))
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Fatalf("expected 1 child status, got %d", len(task.Status.ChildTasks))
	}
	if task.Status.ChildTasks[0].Result == "" {
		t.Error("expected child task result to be populated")
	}
}

func TestHandleRunning_ChildTasksSortedByName(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job-sorted", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	childZ := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "z-child",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-sorted"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "z-agent"},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseSucceeded},
	}
	childA := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "a-child",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-sorted"},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			AgentRef: &corev1alpha1.AgentReference{Name: "a-agent"},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-sorted", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job-sorted",
		},
	}
	r := newUnitReconciler(scheme, task, job, childZ, childA)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 2 {
		t.Fatalf("expected 2 child statuses, got %d", len(task.Status.ChildTasks))
	}
	if task.Status.ChildTasks[0].Name != "a-child" || task.Status.ChildTasks[1].Name != "z-child" {
		t.Fatalf("expected child statuses to be sorted by name, got %#v", task.Status.ChildTasks)
	}
}

// ---------------------------------------------------------------------------
// completeTask — with plan store cleanup
// ---------------------------------------------------------------------------

func TestHandleCompletedRecordsMissingCancelledExecutionEvent(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cancelled-event-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseCancelled,
			Message: "cancelled by tool",
		},
	}
	r := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	r.ExecutionEventStore = eventStore

	_, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("handleCompleted() error = %v", err)
	}

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "cancelled-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 ||
		listed[0].Type != events.ExecutionEventTypeTaskCancelled ||
		listed[0].Summary != "cancelled by tool" {
		t.Fatalf("terminal events = %#v, want TaskCancelled with summary", listed)
	}
}

func TestCompleteTaskRecordsTerminalExecutionEvent(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "terminal-event-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	r.ExecutionEventStore = eventStore

	_, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("completeTask() error = %v", err)
	}

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "terminal-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 ||
		listed[0].Type != events.ExecutionEventTypeTaskSucceeded ||
		listed[0].Summary != "done" {
		t.Fatalf("terminal events = %#v, want TaskSucceeded with summary", listed)
	}
}

func TestHandleCompletedRecordsCancelledExecutionEventOnce(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cancelled-event-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseCancelled,
			Message: "cancelled by parent task",
		},
	}
	r := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	r.ExecutionEventStore = eventStore

	for range 2 {
		if _, err := r.handleCompleted(context.Background(), task); err != nil {
			t.Fatalf("handleCompleted() error = %v", err)
		}
	}

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "cancelled-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 ||
		listed[0].Type != events.ExecutionEventTypeTaskCancelled ||
		listed[0].Summary != "cancelled by parent task" {
		t.Fatalf("terminal events = %#v, want one TaskCancelled event", listed)
	}
}

func TestCompleteTask_WithPlanStore(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-plan", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	r := newUnitReconciler(scheme, task)
	// Save a plan
	_ = r.PlanStore.SavePlan(context.Background(), "default", "comp-plan", &store.PlanState{
		Summary: "test plan",
	})
	_, err := r.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Plan should be cleaned up
	_, planErr := r.PlanStore.GetPlan(context.Background(), "default", "comp-plan")
	if planErr == nil {
		t.Error("expected plan to be deleted on completion")
	}
}

// ---------------------------------------------------------------------------
// handleCompleted — webhook success path
// ---------------------------------------------------------------------------

func TestHandleCompleted_WebhookSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-whok", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			WebhookURL: srv.URL,
		},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseSucceeded,
			WebhookDelivered: false,
		},
	}
	r := newUnitReconciler(scheme, task)
	notifier := NewWebhookNotifier()
	notifier.skipURLValidation = true
	r.WebhookNotifier = notifier
	result, err := r.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue after successful webhook, got %v", result.RequeueAfter)
	}
	if !task.Status.WebhookDelivered {
		t.Error("expected WebhookDelivered to be true")
	}
}

// ---------------------------------------------------------------------------
// collectResult — result already saved by worker (AI task)
// ---------------------------------------------------------------------------

func TestCollectResult_ResultSavedByWorker(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "collect-saved", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)
	_ = r.ResultStore.SaveResult(context.Background(), "default", "collect-saved", []byte("worker result"))
	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.Status.ResultRef == nil || !task.Status.ResultRef.Available {
		t.Error("expected ResultRef.Available=true when result exists")
	}
}

func TestCollectResult_AITask_NoResult(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "collect-ai-none", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, task)
	err := r.collectResult(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// AI task without result — should return nil (no attempt to read logs)
}

// ---------------------------------------------------------------------------
// createTaskJob — job already exists
// ---------------------------------------------------------------------------

func TestCreateTaskJob_JobAlreadyExists(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-exist",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hello"},
		},
	}
	r := newUnitReconciler(scheme, task)
	// First call succeeds
	_, err := r.createTaskJob(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on first call: %v", err)
	}
	jobName := task.Status.JobName

	// Reset task status to simulate second reconcile
	task.Status.Phase = corev1alpha1.TaskPhasePending
	task.Status.JobName = ""
	task.Status.Attempts = 0
	task.Status.StartTime = nil
	task.Status.Conditions = nil

	// Second call should handle AlreadyExists
	_, err = r.createTaskJob(context.Background(), task, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on second call (AlreadyExists): %v", err)
	}
	if task.Status.Phase != corev1alpha1.TaskPhaseRunning {
		t.Errorf("expected phase Running, got %s", task.Status.Phase)
	}
	_ = jobName
}

func TestCreateTaskJob_DoesNotOverwriteCancelledStatus(t *testing.T) {
	scheme := newTestScheme()
	current := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "create-cancelled",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"sleep"},
			Args:    []string{"600"},
		},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseCancelled,
			Message: "cancelled by caller",
		},
	}
	stale := current.DeepCopy()
	stale.Status = corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending}

	r := newUnitReconciler(scheme, current)
	eventStore := storetest.NewFakeExecutionEventStore()
	r.ExecutionEventStore = eventStore
	result, err := r.createTaskJob(context.Background(), stale, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for cancelled task, got %v", result.RequeueAfter)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: current.Name, Namespace: current.Namespace}, updated); err != nil {
		t.Fatalf("failed to get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseCancelled {
		t.Fatalf("phase = %s, want Cancelled", updated.Status.Phase)
	}

	jobs := &batchv1.JobList{}
	if err := r.List(context.Background(), jobs, client.InNamespace(current.Namespace)); err != nil {
		t.Fatalf("failed to list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected no jobs to be created, got %d", len(jobs.Items))
	}
	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   current.Name,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("start events = %#v, want none for cancelled task", listed)
	}
}

// ---------------------------------------------------------------------------
// handlePending — with session ref
// ---------------------------------------------------------------------------

func TestHandlePending_WithSessionRef(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pend-sess",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hi"},
			SessionRef: &corev1alpha1.SessionReference{
				Name:   "pend-session",
				Create: true,
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
}

func TestHandlePending_AgentSessionWaitExpiresAtAbsoluteDeadline(t *testing.T) {
	scheme := newTestScheme()
	timeout := metav1.Duration{Duration: time.Minute}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pend-expired-session", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890ad",
			CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, Prompt: "expired before session lock",
			Timeout: &timeout, SessionRef: &corev1alpha1.SessionReference{Name: "busy-session", Create: true, Append: true},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want terminal result", result)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseCancelled || updated.Status.Execution == nil ||
		updated.Status.Execution.State != corev1alpha1.TaskExecutionStateCancelled ||
		updated.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeCancelled ||
		updated.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("TaskTimeout") ||
		updated.Status.Execution.Attempt != 0 || updated.Status.Execution.PromptID != "" ||
		updated.Status.Delivery == nil || updated.Status.Delivery.State != corev1alpha1.TaskDeliveryStateNotRequested {
		t.Fatalf("expired pending Task status = %#v", updated.Status)
	}
}

func TestHandlePending_BoundV2AgentExpiresAtDefaultDeadlineBeforeQueue(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bound-v2-default-timeout", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890b0",
			CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-defaultACPTaskTimeout - time.Minute)),
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "expired before ACP queueing"},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhasePending,
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV2,
			},
		},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want terminal result", result)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseCancelled || updated.Status.Execution == nil ||
		updated.Status.Execution.State != corev1alpha1.TaskExecutionStateCancelled ||
		updated.Status.Execution.Outcome != corev1alpha1.TaskExecutionOutcomeCancelled ||
		updated.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("TaskTimeout") {
		t.Fatalf("expired bound v2 Task status = %#v", updated.Status)
	}
}

func TestHandlePending_UnboundV2AgentExpiresAtDefaultDeadlineAtNamespaceLimit(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV2),
		}},
	}
	active := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "unbound-v2-default-timeout", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890b1",
			CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-defaultACPTaskTimeout - time.Minute)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: agent.Name}, Prompt: "expired at namespace limit",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent, active)
	r.ACPRuntimeEnabled = true
	r.MaxTasksPerNamespace = 1

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("result = %#v, want terminal result", result)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseCancelled || updated.Status.Execution == nil ||
		updated.Status.Execution.Reason != corev1alpha1.TaskExecutionReason("TaskTimeout") {
		t.Fatalf("expired unbound v2 Task status = %#v", updated.Status)
	}
}

func TestHandlePending_UnboundV1AgentRetainsBindingRelativeDefaultAtNamespaceLimit(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			Type: corev1alpha1.AgentRuntimeCodex, ContractVersion: new(corev1alpha1.AgentRuntimeContractHarnessV1),
		}},
	}
	active := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status:     corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhaseRunning},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "unbound-v1-default-timeout", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890b2",
			CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-defaultACPTaskTimeout - time.Minute)),
		},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent, AgentRef: &corev1alpha1.AgentReference{Name: agent.Name}, Prompt: "wait at namespace limit",
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task, agent, active)
	r.HarnessV1Enabled = true
	r.MaxTasksPerNamespace = 1

	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("RequeueAfter = %v, want 10s", result.RequeueAfter)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhasePending || updated.Status.Execution != nil {
		t.Fatalf("queued unbound v1 Task status = %#v", updated.Status)
	}
}

func TestHandlePending_ExpiredAgentSettlesDurableAttemptBeforeStatusBinding(t *testing.T) {
	scheme := newTestScheme()
	timeout := metav1.Duration{Duration: time.Minute}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "expired-durable-attempt", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890ae",
			CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute)),
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, Prompt: "expired after attempt create", Timeout: &timeout},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	controlStore := sqlite.NewStore(db, "expired-attempt-test")
	epochs := NewControllerEpochManager(controlStore, "expired-attempt-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	promptID := fmt.Sprintf("prompt-%s-1", task.UID)
	attemptKey := store.PromptAttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1, PromptID: promptID}
	attempt, err := controlStore.CreatePromptAttempt(ctx, boundPromptAttemptForTest(&store.PromptAttempt{
		Key: attemptKey, RequestDigest: testControlDigestForDispatcher("expired-durable-attempt"),
		ExecutionState: store.PromptExecutionQueued, DeliveryState: store.PromptDeliveryNotRequested,
	}), fence)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	r.DurableControlStore = controlStore
	r.ControllerEpochManager = epochs
	r.APIReader = r.Client

	result, err := r.handlePending(ctx, task.DeepCopy())
	if err != nil {
		cancelEpoch()
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		cancelEpoch()
		t.Fatalf("result = %#v, want terminal result", result)
	}
	settled, err := controlStore.GetPromptAttempt(ctx, attempt.ID)
	if err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if settled.ExecutionState != store.PromptExecutionCancelled {
		cancelEpoch()
		t.Fatalf("attempt state = %s, want %s", settled.ExecutionState, store.PromptExecutionCancelled)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), updated); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if updated.Status.Execution == nil || updated.Status.Execution.Attempt != 1 || updated.Status.Execution.PromptID != promptID ||
		updated.Status.Execution.RequestDigest != attempt.RequestDigest || updated.Status.Execution.State != corev1alpha1.TaskExecutionStateCancelled ||
		!acpTaskRequiresAuthoritativeAttemptDiscovery(updated) {
		cancelEpoch()
		t.Fatalf("settled Task status = %#v", updated.Status)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestCancelACPTaskBeforeDurableAttemptDoesNotOverwriteFreshExecutionStatus(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh-status", Namespace: "default", UID: "12345678-abcd-efgh-ijkl-1234567890af"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending, Execution: &corev1alpha1.TaskExecutionStatus{
			State: corev1alpha1.TaskExecutionStateQueued, Attempt: 1, PromptID: "prompt-fresh-status-1",
		}},
	}
	r := newUnitReconciler(scheme, task)
	stale := task.DeepCopy()
	stale.Status.Execution = nil
	result, err := r.cancelACPTaskBeforeDurableAttempt(context.Background(), stale, "expired")
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != time.Second {
		t.Fatalf("result = %#v, want one-second retry", result)
	}
	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Execution == nil || updated.Status.Execution.State != corev1alpha1.TaskExecutionStateQueued || updated.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Fatalf("fresh status was overwritten: %#v", updated.Status)
	}
}

func TestHandlePending_WithMissingSessionCreateFalseFailsTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pend-missing-session",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ac",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hi"},
			SessionRef: &corev1alpha1.SessionReference{
				Name: "missing-session",
			},
		},
		Status: corev1alpha1.TaskStatus{Phase: corev1alpha1.TaskPhasePending},
	}
	r := newUnitReconciler(scheme, task)
	result, err := r.handlePending(context.Background(), task)
	if err != nil {
		t.Fatalf("handlePending() error = %v", err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, time.Second)
	}

	updated := &corev1alpha1.Task{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: task.Name, Namespace: task.Namespace}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed", updated.Status.Phase)
	}
	if updated.Status.JobName != "" {
		t.Fatalf("JobName = %q, want no job", updated.Status.JobName)
	}
	if !strings.Contains(updated.Status.Message, "session missing-session not found and create=false") {
		t.Fatalf("message = %q, want missing session failure", updated.Status.Message)
	}
}

// ---------------------------------------------------------------------------
// handleRunning — child task with empty phase
// ---------------------------------------------------------------------------

func TestHandleRunning_ChildWithEmptyPhase(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job3", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-empty-phase",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-empty"},
		},
		Spec:   corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{Phase: ""},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-empty", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job3",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Fatalf("expected 1 child, got %d", len(task.Status.ChildTasks))
	}
	if task.Status.ChildTasks[0].Phase != corev1alpha1.TaskPhasePending {
		t.Errorf("expected empty phase to default to Pending, got %s", task.Status.ChildTasks[0].Phase)
	}
}

// ---------------------------------------------------------------------------
// handleRunning — child task result fetch error
// ---------------------------------------------------------------------------

func TestHandleRunning_ChildResultFetchError(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job4", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-err-result",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-err"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-err", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job4",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	// Don't save the child's result — fetch will fail
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Fatalf("expected 1 child, got %d", len(task.Status.ChildTasks))
	}
	if task.Status.ChildTasks[0].Result != "(result fetch error)" {
		t.Errorf("expected error message in result, got %q", task.Status.ChildTasks[0].Result)
	}
}

func TestHandleRunning_ChildTaskNilResultStore(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job-no-store", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-result-no-store",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-no-store"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-no-store", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job-no-store",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	r.ResultStore = nil

	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Fatalf("expected 1 child, got %d", len(task.Status.ChildTasks))
	}
	if task.Status.ChildTasks[0].Result != "" {
		t.Errorf("expected empty result when result store is nil, got %q", task.Status.ChildTasks[0].Result)
	}
}

// ---------------------------------------------------------------------------
// handleRunning — child task result truncation
// ---------------------------------------------------------------------------

func TestHandleRunning_ChildResultTruncated(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-job5", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	child := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-trunc",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "parent-trunc"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:     corev1alpha1.TaskPhaseSucceeded,
			ResultRef: &corev1alpha1.ResultReference{Available: true},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-trunc", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "parent-job5",
		},
	}
	r := newUnitReconciler(scheme, task, job, child)
	// Save a large result > 4096 bytes
	largeResult := make([]byte, 5000)
	for i := range largeResult {
		largeResult[i] = 'x'
	}
	_ = r.ResultStore.SaveResult(context.Background(), "default", "child-trunc", largeResult)
	_, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(task.Status.ChildTasks) != 1 {
		t.Fatalf("expected 1 child, got %d", len(task.Status.ChildTasks))
	}
	if len(task.Status.ChildTasks[0].Result) > 4200 {
		t.Error("expected result to be truncated")
	}
}

// ---------------------------------------------------------------------------
// handleRunning — is child task (skip child status aggregation)
// ---------------------------------------------------------------------------

func TestHandleRunning_IsChildTask(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "child-job", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-self",
			Namespace: "default",
			Labels:    map[string]string{labels.LabelParentTask: "some-parent"},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "child-job",
		},
	}
	r := newUnitReconciler(scheme, task, job)
	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
	// Child tasks should not aggregate child statuses
	if len(task.Status.ChildTasks) != 0 {
		t.Error("child task should not have child statuses")
	}
}

// ---------------------------------------------------------------------------
// handleRunning — no timeout (nil fields)
// ---------------------------------------------------------------------------

func TestHandleRunning_NoTimeoutFields(t *testing.T) {
	scheme := newTestScheme()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job-notimeout", Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "no-timeout", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseRunning,
			JobName: "job-notimeout",
		},
	}
	r := newUnitReconciler(scheme, task, job)
	result, err := r.handleRunning(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 5*time.Second {
		t.Errorf("expected 5s requeue, got %v", result.RequeueAfter)
	}
}

// ---------------------------------------------------------------------------
// resolveProvider — cross-namespace enforcement
// ---------------------------------------------------------------------------

func TestResolveProvider_CrossNamespaceEnforced(t *testing.T) {
	scheme := newTestScheme()
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "cross-prov", Namespace: "other-ns"},
		Spec: corev1alpha1.ProviderSpec{
			Type:      corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{Name: "sec"},
		},
		Status: corev1alpha1.ProviderStatus{Ready: true},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-cross", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAI,
			AI: &corev1alpha1.AISpec{
				ProviderRef: &corev1alpha1.ProviderReference{Name: "cross-prov", Namespace: "other-ns"},
			},
		},
	}
	r := newUnitReconciler(scheme, provider, task)
	r.EnforceNamespaceIsolation = true
	_, err := r.resolveProvider(context.Background(), task, nil)
	if err == nil {
		t.Error("expected error for cross-namespace provider with isolation")
	}
}

// ---------------------------------------------------------------------------
// resolveProvider — agent fallback
// ---------------------------------------------------------------------------

func TestResolveProvider_AgentFallback(t *testing.T) {
	scheme := newTestScheme()
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-prov", Namespace: "default"},
		Spec: corev1alpha1.ProviderSpec{
			Type:      corev1alpha1.ProviderTypeOpenAI,
			SecretRef: corev1alpha1.ProviderSecretRef{Name: "sec"},
		},
		Status: corev1alpha1.ProviderStatus{Ready: true},
	}
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-with-prov", Namespace: "default"},
		Spec: corev1alpha1.AgentSpec{
			ProviderRef: &corev1alpha1.ProviderReference{Name: "agent-prov"},
			Model:       &corev1alpha1.ModelConfig{Provider: "openai"},
		},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-agent-prov", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAI},
	}
	r := newUnitReconciler(scheme, provider, task, agent)
	got, err := r.resolveProvider(context.Background(), task, agent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || got.Name != "agent-prov" {
		t.Errorf("expected agent-prov, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// ensureWorkerRBAC — error paths
// ---------------------------------------------------------------------------

func TestEnsureWorkerRBAC_SAExistsButRoleBindingsMissing(t *testing.T) {
	scheme := newTestScheme()
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: AIWorkerServiceAccount, Namespace: "test-ns2"},
	}
	r := newUnitReconciler(scheme, sa)
	err := r.ensureWorkerRBAC(context.Background(), "test-ns2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedBindings := []string{
		fmt.Sprintf("orka-ai-worker-%s", "test-ns2"),
		fmt.Sprintf("orka-vendor-worker-%s", "test-ns2"),
		fmt.Sprintf("orka-container-worker-%s", "test-ns2"),
	}
	for _, bindingName := range expectedBindings {
		// The installation may only grant worker access inside its watched
		// namespace; no cluster-wide binding is created.
		rb := &rbacv1.RoleBinding{}
		if err := r.Get(context.Background(), types.NamespacedName{
			Name: bindingName, Namespace: "test-ns2",
		}, rb); err != nil {
			t.Errorf("expected RoleBinding test-ns2/%s to be created: %v", bindingName, err)
		}
		crb := &rbacv1.ClusterRoleBinding{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: bindingName}, crb); !apierrors.IsNotFound(err) {
			t.Errorf("expected no ClusterRoleBinding %s, got err %v and object %#v", bindingName, err, crb)
		}
	}
}

// ---------------------------------------------------------------------------
// Verify existing Ginkgo tests are unaffected (build check only)
// ---------------------------------------------------------------------------

// These standard Go tests live alongside the Ginkgo test file.
// They deliberately avoid TestMain or any Ginkgo bootstrap to stay independent.

// Ensure the store.ErrNotFound sentinel is used correctly in tests above.
var _ = store.ErrNotFound

type failingTaskExecutionEventStore struct{}

func (failingTaskExecutionEventStore) AppendExecutionEvent(context.Context, *store.ExecutionEvent) (*store.ExecutionEvent, error) {
	return nil, errors.New("execution event append failed")
}

func (failingTaskExecutionEventStore) ListExecutionEvents(context.Context, store.ExecutionEventFilter) ([]store.ExecutionEvent, error) {
	return nil, errors.New("not implemented")
}

func (failingTaskExecutionEventStore) ListSessionExecutionEvents(
	context.Context,
	store.SessionExecutionEventFilter,
) ([]store.SessionExecutionEvent, int64, error) {
	return nil, 0, errors.New("not implemented")
}

func (failingTaskExecutionEventStore) GetLatestExecutionEventSeq(context.Context, string, string, string) (int64, error) {
	return 0, errors.New("not implemented")
}

func (failingTaskExecutionEventStore) DeleteExecutionEvents(context.Context, string, string, string) error {
	return errors.New("not implemented")
}

func TestTaskReconcilerRecordsTaskCreatedEventOnStatusInitialization(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "event-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	controllerutil.AddFinalizer(task, labels.TaskFinalizer)
	reconciler := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "event-task"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "event-task"}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Fatalf("phase = %s, want Pending", updated.Status.Phase)
	}

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 || listed[0].Type != events.ExecutionEventTypeTaskCreated || listed[0].Seq != 1 || listed[0].TaskName != "event-task" {
		t.Fatalf("listed events = %#v, want one TaskCreated event", listed)
	}
}

func TestTaskControllerLifecycleEvents(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lifecycle-task",
			Namespace: "default",
			UID:       "12345678-abcd-efgh-ijkl-1234567890ab",
		},
		Spec: corev1alpha1.TaskSpec{
			Type:    corev1alpha1.TaskTypeContainer,
			Image:   "busybox:latest",
			Command: []string{"echo", "hello"},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore

	if _, err := reconciler.createTaskJob(context.Background(), task, nil, nil); err != nil {
		t.Fatalf("createTaskJob() error = %v", err)
	}
	if _, err := reconciler.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "task completed"); err != nil {
		t.Fatalf("completeTask() error = %v", err)
	}

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "lifecycle-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	wantTypes := map[string]bool{
		events.ExecutionEventTypeTaskJobCreated: false,
		events.ExecutionEventTypeTaskStarted:    false,
		events.ExecutionEventTypeTaskSucceeded:  false,
	}
	var previousSeq int64
	for _, event := range listed {
		if event.Seq <= previousSeq {
			t.Fatalf("events are not strictly increasing: %#v", listed)
		}
		previousSeq = event.Seq
		if _, ok := wantTypes[event.Type]; ok {
			wantTypes[event.Type] = true
		}
	}
	for typ, seen := range wantTypes {
		if !seen {
			t.Fatalf("lifecycle events missing %s: %#v", typ, listed)
		}
	}
	afterFirst, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "lifecycle-task",
		AfterSeq:   listed[0].Seq,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents after first seq: %v", err)
	}
	for _, event := range afterFirst {
		if event.Seq <= listed[0].Seq {
			t.Fatalf("after query returned old seq <= %d: %#v", listed[0].Seq, afterFirst)
		}
	}
}

func TestTaskLifecycleEventOmitsMissingSessionName(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-session-event-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{Name: "deleted-session"},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore

	_ = reconciler.recordTaskLifecycleEvent(
		context.Background(),
		task,
		events.ExecutionEventTypeTaskSucceeded,
		events.ExecutionEventSeverityInfo,
		"task completed",
	)

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "missing-session-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 || listed[0].SessionName != "" {
		t.Fatalf("listed events = %#v, want lifecycle event without deleted session name", listed)
	}
}

func TestTaskLifecycleEventKeepsSessionNameOnLookupFailure(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lookup-failure-session-event-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{Name: "session-a"},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	reconciler.SessionManager = NewSessionManager(failingGetSessionStore{err: errors.New("session store unavailable")})
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore

	_ = reconciler.recordTaskLifecycleEvent(
		context.Background(),
		task,
		events.ExecutionEventTypeTaskSucceeded,
		events.ExecutionEventSeverityInfo,
		"task completed",
	)

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "lookup-failure-session-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 || listed[0].SessionName != "session-a" {
		t.Fatalf("listed events = %#v, want lifecycle event to keep session name on ambiguous lookup failure", listed)
	}
}

func TestTaskLifecycleEventKeepsExistingSessionName(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-session-event-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:       corev1alpha1.TaskTypeAI,
			SessionRef: &corev1alpha1.SessionReference{Name: "session-a"},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	now := time.Now()
	if err := reconciler.SessionManager.store.CreateSession(context.Background(), &store.SessionRecord{
		Namespace:   "default",
		Name:        "session-a",
		SessionType: "task",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore

	_ = reconciler.recordTaskLifecycleEvent(
		context.Background(),
		task,
		events.ExecutionEventTypeTaskSucceeded,
		events.ExecutionEventSeverityInfo,
		"task completed",
	)

	listed, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "existing-session-event-task",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents: %v", err)
	}
	if len(listed) != 1 || listed[0].SessionName != "session-a" {
		t.Fatalf("listed events = %#v, want lifecycle event with existing session name", listed)
	}
}

func TestTaskDeletionDeletesExecutionEvents(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "delete-events-task",
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	reconciler := newUnitReconciler(scheme, task)
	eventStore := storetest.NewFakeExecutionEventStore()
	reconciler.ExecutionEventStore = eventStore
	if _, err := eventStore.AppendExecutionEvent(context.Background(), &store.ExecutionEvent{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "delete-events-task",
		TaskName:   "delete-events-task",
		Type:       events.ExecutionEventTypeTaskStarted,
	}); err != nil {
		t.Fatalf("AppendExecutionEvent: %v", err)
	}

	if _, err := reconciler.handleDeletion(context.Background(), task); err != nil {
		t.Fatalf("handleDeletion() error = %v", err)
	}
	remaining, err := eventStore.ListExecutionEvents(context.Background(), store.ExecutionEventFilter{
		Namespace:  "default",
		StreamType: store.ExecutionEventStreamTypeTask,
		StreamID:   "delete-events-task",
	})
	if err != nil {
		t.Fatalf("ListExecutionEvents after deletion: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining events = %#v, want none after task deletion", remaining)
	}
}

func TestHandleCompletedCleansJobWhenTerminalEventAppendFails(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "terminal-cleanup-event-failure-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseCancelled,
			JobName: "terminal-cleanup-event-failure-job",
		},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "terminal-cleanup-event-failure-job", Namespace: "default"}}
	reconciler := newUnitReconciler(scheme, task, job)
	reconciler.ExecutionEventStore = failingTaskExecutionEventStore{}
	result, err := reconciler.handleCompleted(context.Background(), task)
	if err != nil {
		t.Fatalf("handleCompleted() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("handleCompleted() result = %#v, want requeue after terminal event append failure", result)
	}
	remaining := &batchv1.Job{}
	err = reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "terminal-cleanup-event-failure-job"}, remaining)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Get job after handleCompleted() error = %v, want NotFound", err)
	}
}

func TestCompleteExecutedTaskBeginsFinalizingUntilWorkspaceAuthorityIsRevoked(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-after-execution", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}}},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseRunning,
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				AttachedEpoch: 2,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	result, err := reconciler.completeExecutedTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "execution complete")
	if err != nil {
		t.Fatalf("completeExecutedTask() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("completeExecutedTask() result = %#v, want finalization requeue", result)
	}
	updated := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseFinalizing || updated.Status.CompletionTime != nil {
		t.Fatalf("status = %#v, want Finalizing without completion time", updated.Status)
	}
	if updated.Status.ExecutionOutcome == nil || updated.Status.ExecutionOutcome.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("execution outcome = %#v, want immutable Succeeded outcome", updated.Status.ExecutionOutcome)
	}
	complete := meta.FindStatusCondition(updated.Status.Conditions, ConditionTypeComplete)
	if complete == nil || complete.Status != metav1.ConditionFalse || complete.Reason != "TaskFinalizing" {
		t.Fatalf("complete condition = %#v, want TaskFinalizing false", complete)
	}
}

func TestHandleFinalizingBeginsWorkspaceAttachmentRevocation(t *testing.T) {
	scheme := newTestScheme()
	epoch := int64(2)
	workspaceObject := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workspace-finalize", Namespace: "default", UID: types.UID("workspace-uid"),
			Labels: map[string]string{workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			AttachmentEpoch: epoch,
			Attachment:      &workspacev1alpha1.ExecutionWorkspaceAttachment{Epoch: epoch},
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{AttachedEpoch: epoch},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-revoke", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}}},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1},
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				WorkspaceRef:  &corev1alpha1.WorkspaceObjectReference{Name: workspaceObject.Name, UID: string(workspaceObject.UID)},
				AttachedEpoch: epoch,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task, workspaceObject)
	result, err := reconciler.handleFinalizing(context.Background(), task)
	if err != nil {
		t.Fatalf("handleFinalizing() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("handleFinalizing() result = %#v, want requeue", result)
	}
	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(workspaceObject), current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Attachment != nil || current.Spec.AttachmentEpoch != epoch {
		t.Fatalf("workspace attachment intent = %#v epoch=%d, want revoked at epoch %d", current.Spec.Attachment, current.Spec.AttachmentEpoch, epoch)
	}
	updatedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updatedTask); err != nil {
		t.Fatal(err)
	}
	if got := acpTaskRecordedAttachmentEpoch(updatedTask); got != epoch {
		t.Fatalf("recorded attachment epoch = %d, want %d before revocation", got, epoch)
	}
}

func TestHandleFinalizingRecoversRotatedACPAttachmentEpoch(t *testing.T) {
	scheme := newTestScheme()
	projectedEpoch := int64(2)
	liveEpoch := projectedEpoch + 1
	taskUID := types.UID("rotated-finalizing-task-uid")
	workspaceObject := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workspace-finalize-rotated", Namespace: "default", UID: types.UID("workspace-rotated-uid"),
			Labels: map[string]string{workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue},
		},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			AttachmentEpoch: liveEpoch,
			Attachment: &workspacev1alpha1.ExecutionWorkspaceAttachment{
				TaskRef: workspacev1alpha1.ObjectIdentityReference{UID: taskUID},
				Epoch:   liveEpoch,
			},
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{AttachedEpoch: liveEpoch},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "finalize-revoke-rotated", Namespace: "default", UID: taskUID,
			Annotations: map[string]string{acpTaskAttachmentEpochAnnotation: strconv.FormatInt(projectedEpoch, 10)},
		},
		Spec: corev1alpha1.TaskSpec{Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}}},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1},
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				WorkspaceRef:  &corev1alpha1.WorkspaceObjectReference{Name: workspaceObject.Name, UID: string(workspaceObject.UID)},
				AttachedEpoch: projectedEpoch,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task, workspaceObject)

	for attempt := range 2 {
		result, err := reconciler.handleFinalizing(context.Background(), task)
		if err != nil {
			t.Fatalf("handleFinalizing() attempt %d error = %v", attempt, err)
		}
		if result.RequeueAfter <= 0 {
			t.Fatalf("handleFinalizing() attempt %d result = %#v, want requeue", attempt, result)
		}
	}

	current := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(workspaceObject), current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Attachment != nil || current.Spec.AttachmentEpoch != liveEpoch {
		t.Fatalf("workspace attachment intent = %#v epoch=%d, want revoked rotated epoch %d",
			current.Spec.Attachment, current.Spec.AttachmentEpoch, liveEpoch)
	}
	stampedEpoch, _, ok := parseACPWorkspaceRevocationStamp(current.Annotations[acpWorkspaceRevocationStartedAnnotation])
	if !ok || stampedEpoch != liveEpoch {
		t.Fatalf("revocation stamp = %q, want rotated epoch %d",
			current.Annotations[acpWorkspaceRevocationStartedAnnotation], liveEpoch)
	}
	updatedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updatedTask); err != nil {
		t.Fatal(err)
	}
	if got := acpTaskRecordedAttachmentEpoch(updatedTask); got != liveEpoch {
		t.Fatalf("recorded attachment epoch = %d, want rotated live epoch %d", got, liveEpoch)
	}
}

func TestHandleFinalizingCompletesAfterProjectedRevocationUsingHighWaterEpoch(t *testing.T) {
	scheme := newTestScheme()
	epoch := int64(3)
	workspaceObject := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace-detached", Namespace: "default", UID: types.UID("workspace-detached-uid")},
		Spec:       workspacev1alpha1.ExecutionWorkspaceSpec{AttachmentEpoch: epoch},
		Status:     workspacev1alpha1.ExecutionWorkspaceStatus{AttachedEpoch: 0, Conditions: []metav1.Condition{{Type: "Attached", Status: metav1.ConditionFalse}}},
	}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-detached", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}}},
		Status: corev1alpha1.TaskStatus{
			Phase:            corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1, Message: "done"},
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				WorkspaceRef:  &corev1alpha1.WorkspaceObjectReference{Name: workspaceObject.Name, UID: string(workspaceObject.UID)},
				AttachedEpoch: epoch,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionFalse}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task, workspaceObject)
	if _, err := reconciler.handleFinalizing(context.Background(), task); err != nil {
		t.Fatalf("handleFinalizing() error = %v", err)
	}
	updated := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("phase = %s, want Succeeded", updated.Status.Phase)
	}
}

func TestHandleFinalizingCompletesRecordedOutcome(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalizing-complete", Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{
				Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1, RecordedAt: metav1.Now(), Message: "done",
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	if _, err := reconciler.handleFinalizing(context.Background(), task); err != nil {
		t.Fatalf("handleFinalizing() error = %v", err)
	}
	updated := &corev1alpha1.Task{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := reconciler.Get(context.Background(), key, updated); err != nil {
		t.Fatalf("get finalizing task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("phase = %s, want Succeeded", updated.Status.Phase)
	}
}

func TestHandleFinalizingWaitsForAttachmentRevocation(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalizing-attached", Namespace: defaultNS},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{
				Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1, RecordedAt: metav1.Now(), Message: "done",
			},
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				AttachedEpoch: 2,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task)
	result, err := reconciler.handleFinalizing(context.Background(), task)
	if err != nil {
		t.Fatalf("handleFinalizing() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("result = %#v, want requeue", result)
	}
}

func TestCompleteTaskRequeuesWhenTerminalEventAppendFails(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "terminal-event-failure-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	reconciler := newUnitReconciler(scheme, task)
	reconciler.ExecutionEventStore = failingTaskExecutionEventStore{}
	result, err := reconciler.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("completeTask() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("completeTask() result = %#v, want requeue after terminal event append failure", result)
	}
	updated := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "terminal-event-failure-task"}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhaseSucceeded {
		t.Fatalf("phase = %s, want Succeeded despite event write failure", updated.Status.Phase)
	}
}

func TestCompleteTaskDoesNotRecordOutcomeBeforeExecutionAttempt(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pre-execution-failure", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	reconciler := newUnitReconciler(scheme, task)
	if _, err := reconciler.completeTask(
		context.Background(), task, corev1alpha1.TaskPhaseFailed, "validation failed",
	); err != nil {
		t.Fatalf("completeTask() error = %v", err)
	}
	updated := &corev1alpha1.Task{}
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := reconciler.Get(context.Background(), key, updated); err != nil {
		t.Fatalf("get completed task: %v", err)
	}
	if updated.Status.ExecutionOutcome != nil {
		t.Fatalf("pre-execution failure recorded outcome %#v", updated.Status.ExecutionOutcome)
	}
}

func TestCompleteTaskUpdatesAgentLastUsedDespiteTerminalEventAppendFailure(t *testing.T) {
	scheme := newTestScheme()
	agent := &corev1alpha1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "agent-a", Namespace: "default"}}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-terminal-event-failure-task", Namespace: "default"},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAgent,
			AgentRef: &corev1alpha1.AgentReference{Name: "agent-a"},
		},
	}
	reconciler := newUnitReconciler(scheme, task, agent)
	reconciler.ExecutionEventStore = failingTaskExecutionEventStore{}
	result, err := reconciler.completeTask(context.Background(), task, corev1alpha1.TaskPhaseSucceeded, "done")
	if err != nil {
		t.Fatalf("completeTask() error = %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("completeTask() result = %#v, want requeue after terminal event append failure", result)
	}
	updated := &corev1alpha1.Agent{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "agent-a"}, updated); err != nil {
		t.Fatalf("Get updated agent: %v", err)
	}
	if updated.Status.LastUsed == nil {
		t.Fatalf("agent LastUsed was not updated after terminal event append failure")
	}
}

func TestTaskEventWriteFailureDoesNotBreakStatusUpdate(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "event-failure-task", Namespace: "default"},
		Spec:       corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeContainer},
	}
	controllerutil.AddFinalizer(task, labels.TaskFinalizer)
	reconciler := newUnitReconciler(scheme, task)
	reconciler.ExecutionEventStore = failingTaskExecutionEventStore{}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "event-failure-task"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	updated := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "event-failure-task"}, updated); err != nil {
		t.Fatalf("Get updated task: %v", err)
	}
	if updated.Status.Phase != corev1alpha1.TaskPhasePending {
		t.Fatalf("phase = %s, want Pending despite event write failure", updated.Status.Phase)
	}
}

func TestEnsureWorkerRBACCreatesExactTrustedServiceReadBinding(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	r.AIWorkerServiceAccountName = testAIWorkerServiceAccountName
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureWorkerRBAC(context.Background(), testNS); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(testNS, "infra", "gateway", testAIWorkerServiceAccountName)
	role := &rbacv1.Role{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, role); err != nil {
		t.Fatal(err)
	}
	if len(role.Rules) != 1 || !slices.Equal(role.Rules[0].ResourceNames, []string{"gateway"}) || !slices.Equal(role.Rules[0].Verbs, []string{"get"}) {
		t.Fatalf("rules=%#v", role.Rules)
	}
	if role.Labels[trustedServiceReaderLabelKey] != trustedServiceReaderLabelValue || role.Labels[trustedServiceReaderTaskNamespaceLabelKey] != testNS {
		t.Fatalf("role labels=%#v", role.Labels)
	}
	rb := &rbacv1.RoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, rb); err != nil {
		t.Fatal(err)
	}
	if len(rb.Subjects) != 1 || rb.Subjects[0].Namespace != testNS || rb.Subjects[0].Name != r.AIWorkerServiceAccountName {
		t.Fatalf("subjects=%#v", rb.Subjects)
	}
	if rb.Labels[trustedServiceReaderLabelKey] != trustedServiceReaderLabelValue || rb.Labels[trustedServiceReaderTaskNamespaceLabelKey] != testNS {
		t.Fatalf("RoleBinding labels=%#v", rb.Labels)
	}
}

func TestEnsureTrustedServiceReadBindingsRejectsRoleCollision(t *testing.T) {
	scheme := newTestScheme()
	name := trustedServiceReadBindingName(testNS, "infra", "gateway")
	collision := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"}}},
	}
	r := newUnitReconciler(scheme, collision)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), testNS); err == nil || !strings.Contains(err.Error(), "unexpected ownership or permissions") {
		t.Fatalf("ensureTrustedServiceReadBindings() error = %v", err)
	}
	current := &rbacv1.Role{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, current); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Rules, collision.Rules) {
		t.Fatalf("colliding Role was rewritten: %#v", current.Rules)
	}
}

func TestEnsureTrustedServiceReadBindingsRejectsRoleBindingCollision(t *testing.T) {
	scheme := newTestScheme()
	name := trustedServiceReadBindingName(testNS, "infra", "gateway")
	objectLabels := trustedServiceReadBindingLabels(testNS)
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: []string{"gateway"}, Verbs: []string{"get"},
		}},
	}
	collision := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: testNS},
			{Kind: rbacv1.ServiceAccountKind, Name: "attacker", Namespace: testNS},
		},
	}
	r := newUnitReconciler(scheme, role, collision)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), testNS); err == nil || !strings.Contains(err.Error(), "unexpected ownership or subjects") {
		t.Fatalf("ensureTrustedServiceReadBindings() error = %v", err)
	}
	current := &rbacv1.RoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, current); err != nil {
		t.Fatal(err)
	}
	if len(current.Subjects) != 2 {
		t.Fatalf("colliding RoleBinding was rewritten: %#v", current.Subjects)
	}
}

func TestEnsureTrustedServiceReadBindingsUsesAPIReaderForCrossNamespaceRBAC(t *testing.T) {
	scheme := newTestScheme()
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	cachedRBACRead := false
	restrictedCache := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			switch obj.(type) {
			case *rbacv1.Role, *rbacv1.RoleBinding:
				cachedRBACRead = true
				return errors.New("cross-namespace RBAC read attempted through restricted cache")
			default:
				return c.Get(ctx, key, obj, opts...)
			}
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *rbacv1.RoleList, *rbacv1.RoleBindingList:
				cachedRBACRead = true
				return errors.New("cross-namespace RBAC list attempted through restricted cache")
			default:
				return c.List(ctx, list, opts...)
			}
		},
	})
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r := &TaskReconciler{
		Client:              restrictedCache,
		APIReader:           base,
		OutboundAccessTrust: outboundaccess.TrustConfig{Gateways: trusted},
	}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), testNS); err != nil {
		t.Fatal(err)
	}
	if cachedRBACRead {
		t.Fatal("trusted Service RBAC used the restricted cached reader")
	}
	name := trustedServiceReadBindingName(testNS, "infra", "gateway")
	if err := base.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); err != nil {
		t.Fatalf("uncached reader did not resolve created Role: %v", err)
	}
	if err := base.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); err != nil {
		t.Fatalf("uncached reader did not resolve created RoleBinding: %v", err)
	}
}

func startTrustedServiceReadCleanupRunnableForTest(
	t *testing.T,
	runnable *trustedServiceReadCleanupRunnable,
	condition func() bool,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runnable.Start(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !condition() {
		cancel()
		t.Fatal("startup cleanup condition did not become true")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("startup cleanup runnable returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup cleanup runnable did not stop after cancellation")
	}
}

func TestTrustedServiceReadCleanupAfterFinalTaskRemovalDeletesGrant(t *testing.T) {
	scheme := newTestScheme()
	const namespace = "final-task-tenant"
	removed := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "final-task", Namespace: namespace}}
	r := newUnitReconciler(scheme, removed)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(namespace, "infra", "gateway")
	if err := r.Delete(context.Background(), removed); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupTrustedServiceReadBindingsAfterTaskRemoval(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); !apierrors.IsNotFound(err) {
		t.Fatalf("final Task removal left trusted Service Role: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); !apierrors.IsNotFound(err) {
		t.Fatalf("final Task removal left trusted Service RoleBinding: %v", err)
	}
}

func TestTrustedServiceReadCleanupAfterTaskRemovalPreservesGrantForRemainingTask(t *testing.T) {
	scheme := newTestScheme()
	const namespace = "multi-task-tenant"
	removed := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "removed-task", Namespace: namespace}}
	remaining := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "remaining-task", Namespace: namespace}}
	r := newUnitReconciler(scheme, removed, remaining)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(namespace, "infra", "gateway")
	if err := r.Delete(context.Background(), removed); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupTrustedServiceReadBindingsAfterTaskRemoval(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); err != nil {
		t.Fatalf("remaining Task lost trusted Service Role: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); err != nil {
		t.Fatalf("remaining Task lost trusted Service RoleBinding: %v", err)
	}
}

func TestTrustedServiceReadCleanupPreservesSameNameReplacementTaskGrant(t *testing.T) {
	scheme := newTestScheme()
	const namespace = "replacement-task-tenant"
	oldTask := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "replaceable-task", Namespace: namespace, UID: types.UID("old-task-uid")},
	}
	r := newUnitReconciler(scheme, oldTask)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(namespace, "infra", "gateway")
	if err := r.Delete(context.Background(), oldTask); err != nil {
		t.Fatal(err)
	}
	replacement := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: oldTask.Name, Namespace: namespace, UID: types.UID("replacement-task-uid")},
	}
	if err := r.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupTrustedServiceReadBindingsAfterTaskRemoval(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); err != nil {
		t.Fatalf("same-name replacement Task lost trusted Service Role: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); err != nil {
		t.Fatalf("same-name replacement Task lost trusted Service RoleBinding: %v", err)
	}
}

func TestTaskReconcileNotFoundCleansTrustedServiceGrant(t *testing.T) {
	scheme := newTestScheme()
	const namespace = "not-found-tenant"
	r := newUnitReconciler(scheme)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureTrustedServiceReadBindings(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(namespace, "infra", "gateway")
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: namespace,
		Name:      "already-deleted-task",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); !apierrors.IsNotFound(err) {
		t.Fatalf("NotFound reconciliation left trusted Service Role: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); !apierrors.IsNotFound(err) {
		t.Fatalf("NotFound reconciliation left trusted Service RoleBinding: %v", err)
	}
}

func TestTrustedServiceReadStartupCleanupSerializesFreshGrantReconciliation(t *testing.T) {
	scheme := newTestScheme()
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	taskSnapshotDone := make(chan struct{})
	cleanupAtBindings := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var taskSnapshotOnce sync.Once
	var bindingListOnce sync.Once
	intercepted := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *corev1alpha1.TaskList:
				err := c.List(ctx, list, opts...)
				if err == nil {
					taskSnapshotOnce.Do(func() { close(taskSnapshotDone) })
				}
				return err
			case *rbacv1.RoleBindingList:
				bindingListOnce.Do(func() {
					close(cleanupAtBindings)
					<-releaseCleanup
				})
			}
			return c.List(ctx, list, opts...)
		},
	})
	r := &TaskReconciler{Client: intercepted}
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- r.pruneTrustedServiceReadBindingsOnce(context.Background()) }()
	select {
	case <-taskSnapshotDone:
	case <-time.After(2 * time.Second):
		t.Fatal("startup cleanup did not snapshot Task namespaces")
	}
	select {
	case <-cleanupAtBindings:
	case <-time.After(2 * time.Second):
		t.Fatal("startup cleanup did not reach RoleBinding discovery")
	}

	const newNamespace = "new-tenant"
	if err := base.Create(context.Background(), &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "new-task", Namespace: newNamespace},
	}); err != nil {
		t.Fatal(err)
	}
	grantDone := make(chan error, 1)
	go func() { grantDone <- r.ensureTrustedServiceReadBindings(context.Background(), newNamespace) }()
	select {
	case err := <-grantDone:
		t.Fatalf("grant reconciliation bypassed startup cleanup lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCleanup)
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
	if err := <-grantDone; err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(newNamespace, "infra", "gateway")
	if err := base.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); err != nil {
		t.Fatalf("fresh trusted Service Role missing after startup cleanup: %v", err)
	}
	if err := base.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); err != nil {
		t.Fatalf("fresh trusted Service RoleBinding missing after startup cleanup: %v", err)
	}
}

func TestTrustedServiceReadCleanupRunnablePrunesWithoutTaskReconciliation(t *testing.T) {
	scheme := newTestScheme()
	const inactiveNamespace = "inactive-no-task"
	name := trustedServiceReadBindingName(inactiveNamespace, "infra", "gateway")
	objectLabels := trustedServiceReadBindingLabels(inactiveNamespace)
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: []string{"gateway"}, Verbs: []string{"get"},
		}},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: inactiveNamespace,
		}},
	}
	r := newUnitReconciler(scheme, role, binding)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	runnable := &trustedServiceReadCleanupRunnable{reconciler: r}
	if !runnable.NeedLeaderElection() {
		t.Fatal("startup cleanup must run only on the elected manager")
	}
	startTrustedServiceReadCleanupRunnableForTest(t, runnable, func() bool {
		roleErr := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{})
		bindingErr := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{})
		return apierrors.IsNotFound(roleErr) && apierrors.IsNotFound(bindingErr)
	})
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); !apierrors.IsNotFound(err) {
		t.Fatalf("startup cleanup left stale Role without any Task reconciliation: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); !apierrors.IsNotFound(err) {
		t.Fatalf("startup cleanup left stale RoleBinding without any Task reconciliation: %v", err)
	}
}

func TestTrustedServiceReadCleanupRunnablePreservesActiveTaskNamespaceGrant(t *testing.T) {
	scheme := newTestScheme()
	const activeNamespace = "active-tenant"
	name := trustedServiceReadBindingName(activeNamespace, "infra", "gateway")
	objectLabels := trustedServiceReadBindingLabels(activeNamespace)
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: []string{"gateway"}, Verbs: []string{"get"},
		}},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "infra", Labels: maps.Clone(objectLabels)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: activeNamespace,
		}},
	}
	task := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "active-task", Namespace: activeNamespace}}
	r := newUnitReconciler(scheme, role, binding, task)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	startTrustedServiceReadCleanupRunnableForTest(t, &trustedServiceReadCleanupRunnable{reconciler: r}, func() bool {
		r.trustedServiceCleanupMu.RLock()
		cleanupDone := r.trustedServiceCleanupDone
		r.trustedServiceCleanupMu.RUnlock()
		if !cleanupDone {
			return false
		}
		roleErr := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{})
		bindingErr := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{})
		return roleErr == nil && bindingErr == nil
	})
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.Role{}); err != nil {
		t.Fatalf("startup cleanup removed active namespace Role: %v", err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, &rbacv1.RoleBinding{}); err != nil {
		t.Fatalf("startup cleanup removed active namespace RoleBinding: %v", err)
	}
}

func TestEnsureWorkerRBACStartupPrunesManagedTrustedServiceGrantForInactiveNamespace(t *testing.T) {
	scheme := newTestScheme()
	r := newUnitReconciler(scheme)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	const inactiveNamespace = "inactive-tenant"
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureWorkerRBAC(context.Background(), inactiveNamespace); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(inactiveNamespace, "infra", "gateway")

	restarted := &TaskReconciler{Client: r.Client, OutboundAccessTrust: outboundaccess.TrustConfig{}}
	if err := restarted.pruneTrustedServiceReadBindingsOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, role); !apierrors.IsNotFound(err) {
		t.Fatalf("inactive namespace stale Role still exists: %v", err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, binding); !apierrors.IsNotFound(err) {
		t.Fatalf("inactive namespace stale RoleBinding still exists: %v", err)
	}
}

func TestEnsureWorkerRBACPrunesRemovedTrustedServiceReadBindingAfterRestart(t *testing.T) {
	scheme := newTestScheme()
	activeTask := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "active-task", Namespace: testNS}}
	r := newUnitReconciler(scheme, activeTask)
	trusted, err := outboundaccess.ParseTrustedServiceReferences("infra/gateway:8080")
	if err != nil {
		t.Fatal(err)
	}
	r.OutboundAccessTrust = outboundaccess.TrustConfig{Gateways: trusted}
	if err := r.ensureWorkerRBAC(context.Background(), testNS); err != nil {
		t.Fatal(err)
	}
	name := trustedServiceReadBindingName(testNS, "infra", "gateway")
	legacyRole := &rbacv1.Role{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, legacyRole); err != nil {
		t.Fatal(err)
	}
	legacyRole.Labels = nil
	if err := r.Update(context.Background(), legacyRole); err != nil {
		t.Fatal(err)
	}
	legacyBinding := &rbacv1.RoleBinding{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, legacyBinding); err != nil {
		t.Fatal(err)
	}
	legacyBinding.Labels = nil
	if err := r.Update(context.Background(), legacyBinding); err != nil {
		t.Fatal(err)
	}
	unrelatedName := "orka-outbound-service-unrelated"
	unrelatedRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: unrelatedName, Namespace: "infra"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"services"}, ResourceNames: []string{"unrelated"}, Verbs: []string{"get"},
		}},
	}
	if err := r.Create(context.Background(), unrelatedRole); err != nil {
		t.Fatal(err)
	}
	unrelatedBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: unrelatedName, Namespace: "infra"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: unrelatedName},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: testNS,
		}},
	}
	if err := r.Create(context.Background(), unrelatedBinding); err != nil {
		t.Fatal(err)
	}
	missingName := trustedServiceReadBindingName(testNS, "infra", "missing")
	missingBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: missingName, Namespace: "infra"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: missingName},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: testNS,
		}},
	}
	if err := r.Create(context.Background(), missingBinding); err != nil {
		t.Fatal(err)
	}
	driftedName := trustedServiceReadBindingName(testNS, "infra", "drifted")
	driftedRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: driftedName, Namespace: "infra"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"services", "secrets"}, Verbs: []string{"get"},
		}},
	}
	if err := r.Create(context.Background(), driftedRole); err != nil {
		t.Fatal(err)
	}
	driftedBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: driftedName, Namespace: "infra"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: driftedName},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.ServiceAccountKind, Name: AIWorkerServiceAccount, Namespace: testNS,
		}},
	}
	if err := r.Create(context.Background(), driftedBinding); err != nil {
		t.Fatal(err)
	}

	restarted := &TaskReconciler{Client: r.Client, OutboundAccessTrust: outboundaccess.TrustConfig{}}
	if err := restarted.pruneTrustedServiceReadBindingsOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, role); !apierrors.IsNotFound(err) {
		t.Fatalf("stale Role still exists: %v", err)
	}
	rb := &rbacv1.RoleBinding{}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "infra"}, rb); !apierrors.IsNotFound(err) {
		t.Fatalf("stale RoleBinding still exists: %v", err)
	}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: missingName, Namespace: "infra"}, missingBinding); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy RoleBinding with missing Role still exists: %v", err)
	}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: driftedName, Namespace: "infra"}, driftedBinding); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy RoleBinding with drifted Role still exists: %v", err)
	}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: driftedName, Namespace: "infra"}, driftedRole); err != nil {
		t.Fatalf("drifted legacy Role was removed: %v", err)
	}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: unrelatedName, Namespace: "infra"}, unrelatedRole); err != nil {
		t.Fatalf("unrelated prefixed Role was removed: %v", err)
	}
	if err := restarted.Get(context.Background(), types.NamespacedName{Name: unrelatedName, Namespace: "infra"}, unrelatedBinding); err != nil {
		t.Fatalf("unrelated prefixed RoleBinding was removed: %v", err)
	}
}

func TestHandleFinalizingUsesACPWorkspaceDetachTimeout(t *testing.T) {
	t.Parallel()
	const epoch int64 = 4
	tests := []struct {
		name          string
		detachTimeout time.Duration
		revocationAge time.Duration
		outcomeAge    time.Duration
		wantPhase     corev1alpha1.TaskPhase
		wantState     workspacev1alpha1.ExecutionWorkspaceDesiredState
	}{
		{
			name: "short-class-timeout", detachTimeout: time.Minute, revocationAge: 2 * time.Minute,
			outcomeAge: 30 * time.Second, wantPhase: corev1alpha1.TaskPhaseFailed,
			wantState: workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined,
		},
		{
			name: "long-class-timeout", detachTimeout: 10 * time.Minute, revocationAge: 6 * time.Minute,
			outcomeAge: 6 * time.Minute, wantPhase: corev1alpha1.TaskPhaseFinalizing,
			wantState: workspacev1alpha1.ExecutionWorkspaceDesiredReady,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now().UTC()
			workspaceName := "workspace-" + test.name
			workspaceObject := &workspacev1alpha1.ExecutionWorkspace{
				ObjectMeta: metav1.ObjectMeta{
					Name: workspaceName, Namespace: defaultNS, UID: types.UID(workspaceName + "-uid"),
					Labels: map[string]string{workspacev1alpha1.ProviderControllerLabel: acpWorkspaceControllerLabelValue},
					Annotations: map[string]string{
						acpWorkspaceRevocationStartedAnnotation: fmt.Sprintf(
							"%d %s", epoch, now.Add(-test.revocationAge).Format(time.RFC3339Nano),
						),
					},
				},
				Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
					DesiredState:    workspacev1alpha1.ExecutionWorkspaceDesiredReady,
					AttachmentEpoch: epoch,
					Lifecycle: workspacev1alpha1.ExecutionWorkspaceLifecycle{
						DetachTimeout: metav1.Duration{Duration: test.detachTimeout},
					},
				},
				Status: workspacev1alpha1.ExecutionWorkspaceStatus{AttachedEpoch: epoch},
			}
			task := &corev1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name: "finalize-" + test.name, Namespace: defaultNS, UID: types.UID("task-" + test.name + "-uid"),
				},
				Spec: corev1alpha1.TaskSpec{
					Type: corev1alpha1.TaskTypeAgent,
					Execution: &corev1alpha1.ExecutionSpec{
						Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true},
					},
				},
				Status: corev1alpha1.TaskStatus{
					Phase: corev1alpha1.TaskPhaseFinalizing,
					ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{
						Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1,
						RecordedAt: metav1.NewTime(now.Add(-test.outcomeAge)),
					},
					ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
						WorkspaceRef: &corev1alpha1.WorkspaceObjectReference{
							Name: workspaceObject.Name, UID: string(workspaceObject.UID),
						},
						AttachedEpoch: epoch,
						Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
					},
				},
			}
			reconciler := newUnitReconciler(newTestScheme(), task, workspaceObject)
			result, err := reconciler.handleFinalizing(context.Background(), task)
			if err != nil {
				t.Fatalf("handleFinalizing() error = %v", err)
			}
			if test.wantPhase == corev1alpha1.TaskPhaseFinalizing && result.RequeueAfter <= 0 {
				t.Fatalf("handleFinalizing() result = %#v, want a pending-finalization requeue", result)
			}
			updatedTask := &corev1alpha1.Task{}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updatedTask); err != nil {
				t.Fatal(err)
			}
			if updatedTask.Status.Phase != test.wantPhase {
				t.Fatalf("phase = %s, want %s", updatedTask.Status.Phase, test.wantPhase)
			}
			updatedWorkspace := &workspacev1alpha1.ExecutionWorkspace{}
			if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(workspaceObject), updatedWorkspace); err != nil {
				t.Fatal(err)
			}
			if updatedWorkspace.Spec.DesiredState != test.wantState {
				t.Fatalf("desired state = %s, want %s", updatedWorkspace.Spec.DesiredState, test.wantState)
			}
		})
	}
}

func TestHandleFinalizingQuarantinesWorkspaceAfterTimeout(t *testing.T) {
	scheme := newTestScheme()
	epoch := int64(4)
	workspaceObject := &workspacev1alpha1.ExecutionWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace-timeout", Namespace: "default", UID: types.UID("workspace-timeout-uid")},
		Spec: workspacev1alpha1.ExecutionWorkspaceSpec{
			AttachmentEpoch: epoch,
			Attachment:      &workspacev1alpha1.ExecutionWorkspaceAttachment{Epoch: epoch},
		},
		Status: workspacev1alpha1.ExecutionWorkspaceStatus{AttachedEpoch: epoch},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: attachmentSecretName(workspaceObject.Name, epoch), Namespace: "default"}}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: attachmentLeaseName(workspaceObject.Name), Namespace: "default"}}
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-timeout", Namespace: "default", UID: types.UID("task-timeout-uid")},
		Spec:       corev1alpha1.TaskSpec{Execution: &corev1alpha1.ExecutionSpec{Workspace: &corev1alpha1.ExecutionWorkspaceSpec{Enabled: true}}},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseFinalizing,
			ExecutionOutcome: &corev1alpha1.TaskWorkloadExecutionOutcome{
				Phase: corev1alpha1.TaskPhaseSucceeded, Attempt: 1,
				RecordedAt: metav1.NewTime(time.Now().Add(-workspaceFinalizationTimeout - time.Minute)),
			},
			ExecutionWorkspace: &corev1alpha1.ExecutionWorkspaceStatus{
				WorkspaceRef:  &corev1alpha1.WorkspaceObjectReference{Name: workspaceObject.Name, UID: string(workspaceObject.UID)},
				AttachedEpoch: epoch,
				Conditions:    []metav1.Condition{{Type: "Attached", Status: metav1.ConditionTrue}},
			},
		},
	}
	reconciler := newUnitReconciler(scheme, task, workspaceObject, secret, lease)
	if _, err := reconciler.handleFinalizing(context.Background(), task); err != nil {
		t.Fatalf("handleFinalizing() error = %v", err)
	}
	updatedTask := &corev1alpha1.Task{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(task), updatedTask); err != nil {
		t.Fatal(err)
	}
	if updatedTask.Status.Phase != corev1alpha1.TaskPhaseFailed {
		t.Fatalf("phase = %s, want Failed quarantine settlement", updatedTask.Status.Phase)
	}
	updatedWorkspace := &workspacev1alpha1.ExecutionWorkspace{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(workspaceObject), updatedWorkspace); err != nil {
		t.Fatal(err)
	}
	if updatedWorkspace.Spec.DesiredState != workspacev1alpha1.ExecutionWorkspaceDesiredQuarantined || updatedWorkspace.Spec.Attachment != nil {
		t.Fatalf("workspace quarantine = %#v", updatedWorkspace.Spec)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment secret still exists: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(lease), &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment lease still exists: %v", err)
	}
}

func TestHandleDeletionReclaimsNoAttemptAgentTask(t *testing.T) {
	scheme := newTestScheme()
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "no-attempt-delete", Namespace: "default", UID: types.UID("12345678-abcd-efgh-ijkl-1234567890bb"),
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
		Status: corev1alpha1.TaskStatus{
			Phase:   corev1alpha1.TaskPhaseFailed,
			Message: "unsupported runtime",
		},
	}
	r := newUnitReconciler(scheme, task)
	persistDB, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer persistDB.Close() //nolint:errcheck
	controlClient := withControllerEpochLeaseUIDs(t, r.Client)
	controlStore, err := storekube.NewComposite(controlClient, "default", sqlite.NewStore(persistDB, "reclaim-test"))
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, "reclaim-controller")
	epochCtx, cancelEpoch := context.WithCancel(context.Background())
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := epochs.CurrentFence(ctx); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	r.DurableControlStore = controlStore
	r.ControllerEpochManager = epochs
	r.APIReader = r.Client

	current := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if err := r.Delete(ctx, current); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	if _, err := r.handleDeletion(ctx, current); err != nil {
		cancelEpoch()
		t.Fatal(err)
	}
	got := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), got); err == nil {
		if controllerutil.ContainsFinalizer(got, labels.TaskFinalizer) {
			cancelEpoch()
			t.Fatalf("Task finalizer remained after no-attempt reclamation: %#v", got.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		cancelEpoch()
		t.Fatal(err)
	}
	cancelEpoch()
	if err := <-epochDone; err != nil {
		t.Fatal(err)
	}
}

func TestHandleDeletionWaitsForHarnessV1AttemptReclamation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scheme := newTestScheme()
	bindingDigest := "sha256:" + strings.Repeat("a", 64)
	snapshotDigest := "sha256:" + strings.Repeat("b", 64)
	requestDigest := "sha256:" + strings.Repeat("c", 64)
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1-active-delete", Namespace: "default", UID: types.UID("v1-active-delete-uid"),
			Finalizers: []string{labels.TaskFinalizer},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent},
		Status: corev1alpha1.TaskStatus{AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
			ContractVersion: corev1alpha1.AgentRuntimeContractHarnessV1,
			BindingDigest:   bindingDigest,
			Snapshot:        corev1alpha1.AgentExecutionSnapshotRef{Digest: snapshotDigest},
		}},
	}
	r := newUnitReconciler(scheme, task)
	db, err := sqlite.NewDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	durable := sqlite.NewStore(db, "v1-finalizer-test")
	controlClient := withControllerEpochLeaseUIDs(t, r.Client)
	controlStore, err := storekube.NewComposite(controlClient, "default", durable)
	if err != nil {
		t.Fatal(err)
	}
	epochs := NewControllerEpochManager(controlStore, "v1-finalizer-controller").WithMirror(durable)
	epochCtx, cancelEpoch := context.WithCancel(ctx)
	epochDone := make(chan error, 1)
	go func() { epochDone <- epochs.Start(epochCtx) }()
	defer func() {
		cancelEpoch()
		if err := <-epochDone; err != nil {
			t.Errorf("stop epoch manager: %v", err)
		}
	}()
	fence, err := epochs.CurrentFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	attempt := &store.HarnessV1Attempt{
		Namespace: task.Namespace, TaskName: task.Name, TaskUID: string(task.UID), Attempt: 1,
		BindingDigest: bindingDigest, SnapshotDigest: snapshotDigest, RequestDigest: requestDigest,
		TurnID: "turn-v1-active-delete", RuntimeSessionID: "runtime-v1-active-delete",
		State:      store.HarnessV1AttemptPrepared,
		RetryClass: store.HarnessV1RetryClassNone,
	}
	runtimeSession := &harness.RuntimeSession{
		ID: harness.RuntimeSessionID(attempt.RuntimeSessionID),
		Owner: harness.RuntimeSessionOwner{
			Namespace: task.Namespace, SessionName: "task-runtime", ActiveTask: task.Name,
			Provider: harness.ProviderKindKubernetesService,
		},
		State: harness.RuntimeSessionStateReady, CleanupPolicy: harness.RuntimeCleanupPolicyDelete,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := durable.CreateRuntimeSession(ctx, runtimeSession); err != nil {
		t.Fatal(err)
	}
	if err := durable.CreateHarnessV1Attempt(ctx, attempt, fence); err != nil {
		t.Fatal(err)
	}
	r.HarnessV1Attempts = durable
	r.HarnessV1SettlementAcknowledger = &recordingHarnessV1SettlementAcknowledger{}
	r.ControllerEpochManager = epochs
	r.APIReader = r.Client

	current := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), current); err != nil {
		t.Fatal(err)
	}
	result, err := r.handleDeletion(ctx, current)
	if err != nil {
		t.Fatalf("handleDeletion() with active v1 attempt: %v", err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("active v1 deletion requeue = %v, want 2s", result.RequeueAfter)
	}
	retained := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), retained); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(retained, labels.TaskFinalizer) {
		t.Fatal("active harness v1 attempt did not retain the Task finalizer")
	}
	persisted, err := durable.GetHarnessV1Attempt(ctx, store.HarnessV1AttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	reason := "BackendDisabled"
	if _, err := durable.TransitionHarnessV1Attempt(ctx, store.HarnessV1AttemptTransition{
		Key:             store.HarnessV1AttemptKey{Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1},
		ExpectedVersion: persisted.Version,
		ExpectedState:   store.HarnessV1AttemptPrepared,
		TargetState:     store.HarnessV1AttemptRejected,
		OperationID:     "reject-before-delete",
		OperationDigest: store.CanonicalAgentExecutionSnapshotDigest([]byte("reject-before-delete")),
		Fence:           fence,
		Updates:         store.HarnessV1AttemptUpdates{TerminalReason: &reason},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := r.handleDeletion(ctx, retained); err != nil {
		t.Fatalf("handleDeletion() after terminal v1 attempt: %v", err)
	}
	if _, err := durable.GetHarnessV1Attempt(ctx, store.HarnessV1AttemptKey{
		Namespace: task.Namespace, TaskUID: string(task.UID), Attempt: 1,
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reclaimed harness v1 attempt error = %v, want not found", err)
	}
	if _, err := durable.GetRuntimeSession(ctx, task.Namespace, runtimeSession.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reclaimed harness v1 runtime session error = %v, want not found", err)
	}
	deleted := &corev1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), deleted); err == nil {
		if controllerutil.ContainsFinalizer(deleted, labels.TaskFinalizer) {
			t.Fatalf("Task finalizer remained after terminal v1 reclamation: %#v", deleted.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}
