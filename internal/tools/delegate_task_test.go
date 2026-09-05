/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/contexttoken"
	"github.com/orka-agents/orka/internal/labels"
	orkatracing "github.com/orka-agents/orka/internal/tracing"
	"github.com/orka-agents/orka/internal/tracing/genai"
	"github.com/orka-agents/orka/internal/tracing/testutil"
	"github.com/orka-agents/orka/internal/transactiontoken"
	txtest "github.com/orka-agents/orka/internal/transactiontoken/testutil"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	parentTaskName         = "parent-task"
	parentTransactionID    = "txn-parent"
	parentTransactionHash  = "sha256:parent-context"
	parentTransactionToken = "parent-tx-token"
	childTransactionScope  = "orka:agents:run"
)

const testAgentCatalogNS = "catalog"

func researcherAgent() *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testResearcherAgentName,
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{},
	}
}

func parentTask() *corev1alpha1.Task {
	priority := int32(500)
	return &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      parentTaskName,
			Namespace: defaultNamespace,
			UID:       apitypes.UID("parent-uid-1234"),
		},
		Spec: corev1alpha1.TaskSpec{
			Type:     corev1alpha1.TaskTypeAI,
			Priority: &priority,
			RequestedBy: &corev1alpha1.RequestedBy{
				Subject: "parent-subject",
				Issuer:  "https://issuer.example.test",
				Roles:   []string{"orka:agents:delegate", childTransactionScope},
			},
			Transaction: &corev1alpha1.TaskTransaction{
				Profile:            transactiontoken.ProfileName,
				ID:                 parentTransactionID,
				Issuer:             "https://issuer.example.test",
				Subject:            "parent-subject",
				RequestingWorkload: "spiffe://example.test/ns/default/sa/parent",
				Scope:              "orka:agents:delegate orka:agents:run orka:secrets:credentials:read",
				Scopes:             []string{"orka:agents:delegate", childTransactionScope, "orka:secrets:credentials:read"},
				ContextDigest:      parentTransactionHash,
			},
		},
	}
}

func expectInheritedTaskProvenance(t *testing.T, task *corev1alpha1.Task) {
	t.Helper()
	if task.Spec.RequestedBy == nil || task.Spec.RequestedBy.Subject != "parent-subject" {
		t.Fatalf("spec.requestedBy = %#v, want parent requester", task.Spec.RequestedBy)
	}
	if task.Spec.Transaction == nil || task.Spec.Transaction.ID != parentTransactionID {
		t.Fatalf("spec.transaction = %#v, want parent transaction", task.Spec.Transaction)
	}
	if task.Labels[labels.LabelTransactionID] != labels.SelectorValue(parentTransactionID) {
		t.Fatalf("transaction label = %q, want %q", task.Labels[labels.LabelTransactionID], labels.SelectorValue(parentTransactionID))
	}
	if task.Annotations[labels.AnnotationTransactionContextDigest] != parentTransactionHash {
		t.Fatalf("transaction context digest annotation = %q, want %q", task.Annotations[labels.AnnotationTransactionContextDigest], parentTransactionHash)
	}
	if len(task.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences = %#v, want parent task owner", task.OwnerReferences)
	}
	ownerRef := task.OwnerReferences[0]
	if ownerRef.Name != parentTaskName {
		t.Fatalf("ownerRef.Name = %q, want %q", ownerRef.Name, parentTaskName)
	}
	if ownerRef.UID != apitypes.UID("parent-uid-1234") {
		t.Fatalf("ownerRef.UID = %q, want %q", ownerRef.UID, apitypes.UID("parent-uid-1234"))
	}
	if ownerRef.Controller == nil || !*ownerRef.Controller {
		t.Fatalf("ownerRef.Controller = %#v, want true", ownerRef.Controller)
	}
	if ownerRef.BlockOwnerDeletion != nil {
		t.Fatalf("ownerRef.BlockOwnerDeletion = %#v, want nil", ownerRef.BlockOwnerDeletion)
	}
}

func TestDelegateTaskTool_Name(t *testing.T) {
	tool := NewDelegateTaskTool(newFakeClient())
	if got := tool.Name(); got != delegateTaskToolName {
		t.Errorf("Name() = %v, want %v", got, delegateTaskToolName)
	}
}

func TestDelegateTaskTool_Description(t *testing.T) {
	tool := NewDelegateTaskTool(newFakeClient())
	if got := tool.Description(); got == "" {
		t.Error("Description() returned empty string")
	}
}

func TestDelegateTaskTool_Parameters(t *testing.T) {
	tool := NewDelegateTaskTool(newFakeClient())
	params := tool.Parameters()
	if params == nil {
		t.Fatal("Parameters() returned nil")
	}

	var parametersSchema map[string]any
	if err := json.Unmarshal(params, &parametersSchema); err != nil {
		t.Errorf("Parameters() returned invalid JSON: %v", err)
	}
	if parametersSchema[jsonSchemaTypeField] != typeObject {
		t.Error("Parameters schema should have type: object")
	}
	properties, ok := parametersSchema[jsonSchemaPropertiesField].(map[string]any)
	if !ok {
		t.Fatal("Parameters schema is missing properties")
	}
	workspaceSchema, ok := properties[workspaceField].(map[string]any)
	if !ok {
		t.Fatal("workspace schema is not an object")
	}
	workspaceProperties, ok := workspaceSchema[jsonSchemaPropertiesField].(map[string]any)
	if !ok {
		t.Fatal("workspace schema is missing properties")
	}
	for _, key := range []string{"publicationReadCredentialRef", "publicationCredentialRef", "forgeCredentialRef"} {
		if _, ok := workspaceProperties[key]; !ok {
			t.Errorf("workspace schema missing %s property", key)
		}
	}
}

func TestDelegateTaskTool_Execute(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		args        json.RawMessage
		wantErr     bool
		wantErrMsg  string
		checkResult bool
		wantStatus  string
	}{
		{
			name: "successful delegation",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: "researcher,coder",
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:        json.RawMessage(`{"agent": "researcher", "prompt": "Research the topic"}`),
			wantErr:     false,
			checkResult: true,
			wantStatus:  GitHubPullRequestStatusCreated,
		},
		{
			name: "agent not allowed",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: "researcher,coder",
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:       json.RawMessage(`{"agent": "unauthorized-agent", "prompt": "Do something"}`),
			wantErr:    true,
			wantErrMsg: "not in the allowed agents list",
		},
		{
			name: "depth exceeded",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "3",
				envOrkaCoordinationAllowedAgents: testResearcherAgentName,
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:       json.RawMessage(`{"agent": "researcher", "prompt": "Research the topic"}`),
			wantErr:    true,
			wantErrMsg: "coordination depth exceeded",
		},
		{
			name: "missing agent arg",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: testResearcherAgentName,
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:       json.RawMessage(`{"prompt": "Research the topic"}`),
			wantErr:    true,
			wantErrMsg: "agent is required",
		},
		{
			name: "missing prompt arg",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: testResearcherAgentName,
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:       json.RawMessage(`{"agent": "researcher"}`),
			wantErr:    true,
			wantErrMsg: "prompt is required",
		},
		{
			name: invalidJSONArgsCaseName,
			envVars: map[string]string{
				envOrkaTaskName:      parentTaskName,
				envOrkaTaskNamespace: defaultNamespace,
			},
			args:       json.RawMessage(invalidJSONText),
			wantErr:    true,
			wantErrMsg: invalidArgumentsMessage,
		},
		{
			name: "custom priority",
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: testResearcherAgentName,
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:        json.RawMessage(`{"agent": "researcher", "prompt": "Research", "priority": 800}`),
			wantErr:     false,
			checkResult: true,
			wantStatus:  GitHubPullRequestStatusCreated,
		},
		{
			name: testCustomNamespaceCaseName,
			envVars: map[string]string{
				envOrkaTaskName:                  parentTaskName,
				envOrkaTaskNamespace:             defaultNamespace,
				envOrkaCoordinationDepth:         "0",
				envOrkaCoordinationAllowedAgents: testResearcherAgentName,
				envOrkaCoordinationMaxDepth:      "3",
			},
			args:    json.RawMessage(`{"agent": "researcher", "prompt": "Research", "namespace": "other-ns"}`),
			wantErr: true, // parent task not found in other-ns
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			// Create fake client with parent task and agent
			k8sClient := newFakeClient(parentTask(), researcherAgent())
			tool := NewDelegateTaskTool(k8sClient)

			result, err := tool.Execute(context.Background(), tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.wantErrMsg != "" {
				if err == nil || !contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("Execute() error = %v, want error containing %q", err, tt.wantErrMsg)
				}
				return
			}

			if tt.checkResult {
				var delegateResult DelegateTaskResult
				if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}
				if delegateResult.Status != tt.wantStatus {
					t.Errorf("Execute() status = %q, want %q", delegateResult.Status, tt.wantStatus)
				}
				if delegateResult.TaskName == "" {
					t.Error("Execute() returned empty task name")
				}
			}
		})
	}
}

func TestDelegateTaskTool_CrossNamespaceAgentKeepsChildInParentNamespace(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testAgentCatalogNS+"/researcher")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	catalogAgent := researcherAgent()
	catalogAgent.Namespace = testAgentCatalogNS
	k8sClient := newFakeClient(parentTask(), catalogAgent)
	tool := NewDelegateTaskTool(k8sClient)
	result, err := tool.Execute(context.Background(), json.RawMessage(fmt.Sprintf(`{
		"agent":"researcher",
		"agentNamespace":%q,
		"prompt":"Research cross namespace"
	}`, testAgentCatalogNS)))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var parsed DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	child := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{Name: parsed.TaskName, Namespace: defaultNamespace}, child); err != nil {
		t.Fatalf("get child task in parent namespace: %v", err)
	}
	if child.Spec.AgentRef == nil || child.Spec.AgentRef.Name != "researcher" || child.Spec.AgentRef.Namespace != testAgentCatalogNS {
		t.Fatalf("child AgentRef = %#v, want %s/researcher", child.Spec.AgentRef, testAgentCatalogNS)
	}
}

func TestDelegateTaskTool_CrossNamespaceAgentDeniedWhenAllowedAgentsLacksNamespace(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "default/researcher")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	catalogAgent := researcherAgent()
	catalogAgent.Namespace = testAgentCatalogNS
	k8sClient := newFakeClient(parentTask(), catalogAgent)
	_, err := NewDelegateTaskTool(k8sClient).Execute(context.Background(), json.RawMessage(fmt.Sprintf(`{
		"agent":"researcher",
		"agentNamespace":%q,
		"prompt":"Research cross namespace"
	}`, testAgentCatalogNS)))
	if err == nil || !strings.Contains(err.Error(), "allowed agents") {
		t.Fatalf("Execute() error = %v, want allowed agents denial", err)
	}
}

func TestDelegateTaskTool_Execute_CleansUpChildTaskWhenTokenSecretAdoptionFails(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	subjectPath := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(subjectPath, []byte(parentTransactionToken), 0600); err != nil {
		t.Fatalf("failed to write subject token fixture: %v", err)
	}
	ttsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token":      "child-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:txn_token",
			"token_type":        "N_A",
		})
	}))
	defer ttsServer.Close()

	t.Setenv(workerenv.ContextTokenTTSEndpoint, ttsServer.URL+"/token_endpoint")
	t.Setenv(workerenv.ContextTokenTTSTokenSource, contexttoken.TTSTokenSourceIncoming)
	t.Setenv(workerenv.ContextTokenSubjectTokenFile, subjectPath)
	t.Setenv(workerenv.ContextTokenChildScope, childTransactionScope)

	forcedErr := errors.New("forced secret adoption update failure")
	k8sClient := newFakeClientWithInterceptorFuncs(interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*corev1.Secret); ok {
				for _, owner := range obj.GetOwnerReferences() {
					if owner.Name != parentTaskName {
						return forcedErr
					}
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	}, parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	_, err := tool.Execute(context.Background(), json.RawMessage(`{"agent":"researcher","prompt":"Research with token"}`))
	if err == nil || !strings.Contains(err.Error(), "adopting child transaction token secret") || !strings.Contains(err.Error(), forcedErr.Error()) {
		t.Fatalf("Execute() error = %v, want forced secret adoption update failure", err)
	}

	tasks := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), tasks, client.InNamespace(defaultNamespace)); err != nil {
		t.Fatalf("failed to list tasks: %v", err)
	}
	for _, task := range tasks.Items {
		if task.Name != parentTaskName {
			t.Fatalf("unexpected child task left after adoption failure: %#v", task)
		}
	}

	secrets := &corev1.SecretList{}
	if err := k8sClient.List(context.Background(), secrets, client.InNamespace(defaultNamespace)); err != nil {
		t.Fatalf("failed to list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("unexpected child transaction token secrets after adoption failure cleanup: %#v", secrets.Items)
	}
}

func TestDelegateTaskTool_Execute_WithTTSChildToken(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	subjectPath := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(subjectPath, []byte(parentTransactionToken), 0600); err != nil {
		t.Fatalf("failed to write subject token fixture: %v", err)
	}

	issuer := txtest.NewIssuer(t)
	jwksServer := httptest.NewServer(issuer.JWKSHandler())
	defer jwksServer.Close()
	childToken := issuer.Sign(t, transactiontoken.Claims{
		Issuer:             "https://tts.example.test",
		Audience:           "child.example.test",
		TransactionID:      parentTransactionID,
		Subject:            "spiffe://example.test/ns/default/sa/child",
		Scope:              childTransactionScope,
		RequestingWorkload: "spiffe://example.test/ns/default/sa/orka-worker",
	}, time.Minute)

	var requestDetails map[string]any
	ttsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		if got := r.FormValue("subject_token"); got != parentTransactionToken {
			t.Fatalf("subject_token = %q, want %s", got, parentTransactionToken)
		}
		if got := r.FormValue("scope"); got != childTransactionScope {
			t.Fatalf("scope = %q, want orka:agents:run", got)
		}
		if err := json.Unmarshal([]byte(r.FormValue("request_details")), &requestDetails); err != nil {
			t.Fatalf("request_details JSON error = %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token":      childToken,
			"issued_token_type": "urn:ietf:params:oauth:token-type:txn_token",
			"token_type":        "N_A",
		})
	}))
	defer ttsServer.Close()

	t.Setenv(workerenv.ContextTokenTTSEndpoint, ttsServer.URL+"/token_endpoint")
	t.Setenv(workerenv.ContextTokenTTSTokenSource, contexttoken.TTSTokenSourceIncoming)
	t.Setenv(workerenv.ContextTokenSubjectTokenFile, subjectPath)
	t.Setenv(workerenv.ContextTokenChildScope, childTransactionScope)

	var patchCalled atomic.Bool
	k8sClient := newFakeClientWithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			data, err := patch.Data(obj)
			if err != nil {
				t.Fatalf("patch.Data() error = %v", err)
			}
			patchCalled.Store(true)
			if bytes.Contains(data, []byte(`"spec"`)) || bytes.Contains(data, []byte(`"transaction"`)) {
				t.Fatalf("child token metadata patch unexpectedly includes spec/transaction: %s", data)
			}
			if !bytes.Contains(data, []byte(labels.AnnotationTransactionTokenPending)) {
				t.Fatalf("child token metadata patch %s does not include pending annotation", data)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}, parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"agent":"researcher","prompt":"Research with token"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if delegateResult.TaskName == "" {
		t.Fatal("Execute() returned empty task name")
	}
	if !patchCalled.Load() {
		t.Fatal("expected child token metadata patch to be called")
	}
	if requestDetails["operation"] != "delegateTask" || requestDetails["agent"] != testResearcherAgentName || requestDetails["txn"] != parentTransactionID {
		t.Fatalf("request_details = %#v", requestDetails)
	}

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{Name: delegateResult.TaskName, Namespace: defaultNamespace}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}
	expectInheritedTaskProvenance(t, childTask)
	secretName := childTask.Annotations[labels.AnnotationTransactionTokenSecret]
	if secretName == "" {
		t.Fatal("expected child transaction token secret annotation")
	}
	if _, ok := childTask.Annotations[labels.AnnotationTransactionTokenPending]; ok {
		t.Fatalf("child transaction token pending annotation was not removed: %#v", childTask.Annotations)
	}
	secret := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{Name: secretName, Namespace: defaultNamespace}, secret); err != nil {
		t.Fatalf("failed to get child token secret: %v", err)
	}
	if len(secret.OwnerReferences) != 1 {
		t.Fatalf("secret ownerReferences = %#v, want child task owner", secret.OwnerReferences)
	}
	owner := secret.OwnerReferences[0]
	if owner.Name != childTask.Name {
		t.Fatalf("secret owner name = %q, want child task name %q", owner.Name, childTask.Name)
	}
	if owner.UID != childTask.UID {
		t.Fatalf("secret owner UID = %q, want child task UID %q", owner.UID, childTask.UID)
	}
	if owner.BlockOwnerDeletion != nil {
		t.Fatalf("secret owner BlockOwnerDeletion = %#v, want nil", owner.BlockOwnerDeletion)
	}
	claims, err := txtest.Verify(context.Background(), jwksServer.URL, "child.example.test", string(secret.Data["token"]))
	if err != nil {
		t.Fatalf("failed to verify child TxToken from secret: %v", err)
	}
	if claims.TransactionID != parentTransactionID {
		t.Fatalf("child token txn = %q, want %q", claims.TransactionID, parentTransactionID)
	}
	if claims.Scope != childTransactionScope {
		t.Fatalf("child token scope = %q, want orka:agents:run", claims.Scope)
	}
}

func TestDelegateTaskTool_Execute_CleansUpPreparedChildTokenWhenTaskCreateFails(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	subjectPath := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(subjectPath, []byte(parentTransactionToken), 0600); err != nil {
		t.Fatalf("failed to write subject token fixture: %v", err)
	}
	var ttsCalled atomic.Bool
	ttsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ttsCalled.Store(true)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token":      "child-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:txn_token",
			"token_type":        "N_A",
		})
	}))
	defer ttsServer.Close()

	t.Setenv(workerenv.ContextTokenTTSEndpoint, ttsServer.URL+"/token_endpoint")
	t.Setenv(workerenv.ContextTokenTTSTokenSource, contexttoken.TTSTokenSourceIncoming)
	t.Setenv(workerenv.ContextTokenSubjectTokenFile, subjectPath)
	t.Setenv(workerenv.ContextTokenChildScope, childTransactionScope)

	var secretCreateCalled atomic.Bool
	k8sClient := newFakeClientWithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			switch typed := obj.(type) {
			case *corev1alpha1.Task:
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: "tasks"},
					typed.GenerateName+"collision",
				)
			case *corev1.Secret:
				secretCreateCalled.Store(true)
				return c.Create(ctx, typed, opts...)
			default:
				return assignFakeUIDOnCreate(ctx, c, obj, opts...)
			}
		},
	}, parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	_, err := tool.Execute(context.Background(), json.RawMessage(`{"agent":"researcher","prompt":"Research with token"}`))
	if err == nil || !strings.Contains(err.Error(), "failed to create child task") || !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Execute() error = %v, want child task AlreadyExists", err)
	}
	if !ttsCalled.Load() {
		t.Fatal("child token exchange was not attempted before child task create")
	}
	if !secretCreateCalled.Load() {
		t.Fatal("child token secret was not prepared before child task create")
	}

	secrets := &corev1.SecretList{}
	if err := k8sClient.List(context.Background(), secrets, client.InNamespace(defaultNamespace)); err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("expected prepared child token secret to be cleaned up, got %d secrets", len(secrets.Items))
	}
}

func TestDelegateTaskTool_Execute_DoesNotExchangeChildTokenForNonTransactionalParent(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	ttsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("TTS should not be called for non-transactional parent task")
	}))
	defer ttsServer.Close()

	t.Setenv(workerenv.ContextTokenTTSEndpoint, ttsServer.URL+"/token_endpoint")
	t.Setenv(workerenv.ContextTokenTTSTokenSource, contexttoken.TTSTokenSourceIncoming)
	t.Setenv(workerenv.ContextTokenChildScope, childTransactionScope)

	parent := parentTask()
	parent.Spec.Transaction = nil
	k8sClient := newFakeClient(parent, researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	result, err := tool.Execute(context.Background(), json.RawMessage(`{"agent":"researcher","prompt":"Research without token"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if delegateResult.TaskName == "" {
		t.Fatal("Execute() returned empty task name")
	}

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{Name: delegateResult.TaskName, Namespace: defaultNamespace}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}
	if _, ok := childTask.Annotations[labels.AnnotationTransactionTokenSecret]; ok {
		t.Fatalf("unexpected child transaction token secret annotation: %#v", childTask.Annotations)
	}
	if _, ok := childTask.Annotations[labels.AnnotationTransactionTokenPending]; ok {
		t.Fatalf("unexpected child transaction token pending annotation: %#v", childTask.Annotations)
	}

	secrets := &corev1.SecretList{}
	if err := k8sClient.List(context.Background(), secrets, client.InNamespace(defaultNamespace)); err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("expected no child token secrets to be created, got %d", len(secrets.Items))
	}
}

func TestDelegateTaskTool_Execute_ChildTaskFields(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "1")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "5")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)
	shutdown, err := orkatracing.Init("test", false)
	if err != nil {
		t.Fatalf("tracing init: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
	spanHarness := testutil.NewSpanHarness(t)
	ctx, span := orkatracing.Tracer("test").Start(context.Background(), "parent-tool")
	defer span.End()

	args := json.RawMessage(`{"agent": "researcher", "prompt": "Investigate this"}`)
	result, err := tool.Execute(ctx, args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	// Fetch the created child task to verify fields
	taskList := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), taskList); err != nil {
		t.Fatalf("failed to list tasks: %v", err)
	}

	// Find the child task (not the parent)
	var childTask *corev1alpha1.Task
	for i := range taskList.Items {
		if taskList.Items[i].Name != parentTaskName {
			childTask = &taskList.Items[i]
			break
		}
	}

	if childTask == nil {
		t.Fatal("child task not found")
	}

	// Verify labels
	if childTask.Labels[labels.LabelParentTask] != labels.SelectorValue(parentTaskName) {
		t.Errorf("label orka.ai/parent-task = %q, want %q", childTask.Labels[labels.LabelParentTask], labels.SelectorValue(parentTaskName))
	}
	if childTask.Labels[labels.LabelCoordinator] != trueStr {
		t.Errorf("label orka.ai/coordinator = %q, want %q", childTask.Labels[labels.LabelCoordinator], trueStr)
	}
	if childTask.Labels[labels.LabelDelegatedAgent] != testResearcherAgentName {
		t.Errorf("label orka.ai/delegated-agent = %q, want %q", childTask.Labels[labels.LabelDelegatedAgent], testResearcherAgentName)
	}

	// Verify annotations
	if childTask.Annotations[labels.AnnotationParentTaskName] != parentTaskName {
		t.Errorf("annotation orka.ai/parent-task-name = %q, want %q", childTask.Annotations[labels.AnnotationParentTaskName], parentTaskName)
	}
	if childTask.Annotations[labels.AnnotationCoordinationDepth] != "2" {
		t.Errorf("annotation orka.ai/coordination-depth = %q, want %q", childTask.Annotations[labels.AnnotationCoordinationDepth], "2")
	}
	if childTask.Annotations[labels.AnnotationTraceParent] == "" {
		t.Fatalf("missing %s annotation on delegated child task", labels.AnnotationTraceParent)
	}
	extracted := orkatracing.ExtractTaskTraceContext(context.Background(), childTask)
	_, childSpan := orkatracing.Tracer("test").Start(extracted, "child-task")
	childSpan.End()
	ended := spanHarness.Recorder.Ended()
	if len(ended) == 0 || ended[len(ended)-1].Parent().TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("delegated child trace parent was not propagated")
	}

	// Verify spec
	if childTask.Spec.Type != corev1alpha1.TaskTypeAI {
		t.Errorf("spec.type = %q, want %q", childTask.Spec.Type, corev1alpha1.TaskTypeAI)
	}
	if childTask.Spec.AgentRef == nil || childTask.Spec.AgentRef.Name != testResearcherAgentName {
		t.Errorf("spec.agentRef.name = %v, want %q", childTask.Spec.AgentRef, testResearcherAgentName)
	}
	if childTask.Spec.Prompt != "Investigate this" {
		t.Errorf("spec.prompt = %q, want %q", childTask.Spec.Prompt, "Investigate this")
	}
	if childTask.Spec.AI == nil || childTask.Spec.AI.Prompt != childTask.Spec.Prompt {
		t.Fatalf("spec.ai = %#v, want only delegated prompt %q", childTask.Spec.AI, childTask.Spec.Prompt)
	}
	expectInheritedTaskProvenance(t, childTask)

	// Verify owner reference
	if len(childTask.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(childTask.OwnerReferences))
	}
	ownerRef := childTask.OwnerReferences[0]
	if ownerRef.Name != parentTaskName {
		t.Errorf("ownerRef.Name = %q, want %q", ownerRef.Name, parentTaskName)
	}
	if ownerRef.UID != apitypes.UID("parent-uid-1234") {
		t.Errorf("ownerRef.UID = %q, want %q", ownerRef.UID, "parent-uid-1234")
	}
	if ownerRef.Controller == nil || !*ownerRef.Controller {
		t.Error("ownerRef.Controller should be true")
	}
	if ownerRef.BlockOwnerDeletion != nil {
		t.Errorf("ownerRef.BlockOwnerDeletion = %#v, want nil", ownerRef.BlockOwnerDeletion)
	}

	// Verify priority inherited from parent
	if childTask.Spec.Priority == nil || *childTask.Spec.Priority != 500 {
		t.Errorf("spec.priority = %v, want 500", childTask.Spec.Priority)
	}
}

func TestDelegateTaskTool_Execute_AgentType(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testClaudeCoderName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	maxTurns := int32(100)
	agentTask := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClaudeCoderName,
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type:             runtimeTypeClaude,
				DefaultMaxTurns:  &maxTurns,
				DefaultAllowBash: new(true),
			},
		},
	}

	k8sClient := newFakeClient(parentTask(), agentTask)
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{
		"agent": "claude-coder",
		"prompt": "Fix the auth module",
		"workspace": {
			"gitRepo": "https://github.com/myorg/myrepo.git",
			"branch": "main"
		},
		"timeout": "20m",
		"maxTurns": 50,
		"allowBash": true
	}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if delegateResult.Status != GitHubPullRequestStatusCreated {
		t.Errorf("status = %q, want %q", delegateResult.Status, GitHubPullRequestStatusCreated)
	}

	// Fetch the child task
	taskList := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), taskList); err != nil {
		t.Fatalf("failed to list tasks: %v", err)
	}

	var childTask *corev1alpha1.Task
	for i := range taskList.Items {
		if taskList.Items[i].Name != parentTaskName {
			childTask = &taskList.Items[i]
			break
		}
	}
	if childTask == nil {
		t.Fatal("child task not found")
	}

	// Verify task type is agent
	if childTask.Spec.Type != corev1alpha1.TaskTypeAgent {
		t.Errorf("spec.type = %q, want %q", childTask.Spec.Type, corev1alpha1.TaskTypeAgent)
	}
	if childTask.Spec.AI != nil {
		t.Fatalf("spec.ai = %#v, want nil for an explicit runtime Agent", childTask.Spec.AI)
	}

	// Verify agent runtime config
	if childTask.Spec.AgentRuntime == nil {
		t.Fatal("spec.agentRuntime is nil")
	}
	if childTask.Spec.Workspace == nil {
		t.Fatal("spec.agentRuntime.workspace is nil")
	}
	if childTask.Spec.Workspace.GitRepo != "https://github.com/myorg/myrepo" {
		t.Errorf("workspace.gitRepo = %q, want %q", childTask.Spec.Workspace.GitRepo, "https://github.com/myorg/myrepo")
	}
	if childTask.Spec.Workspace.Branch != testBranch {
		t.Errorf("workspace.branch = %q, want %q", childTask.Spec.Workspace.Branch, testBranch)
	}
	if childTask.Spec.AgentRuntime.MaxTurns == nil || *childTask.Spec.AgentRuntime.MaxTurns != 50 {
		t.Errorf("agentRuntime.maxTurns = %v, want 50", childTask.Spec.AgentRuntime.MaxTurns)
	}
	if childTask.Spec.AgentRuntime.AllowBash == nil || !*childTask.Spec.AgentRuntime.AllowBash {
		t.Error("agentRuntime.allowBash should be true")
	}
	if childTask.Spec.Timeout == nil || childTask.Spec.Timeout.Duration != 20*time.Minute {
		t.Errorf("spec.timeout = %v, want 20m", childTask.Spec.Timeout)
	}
}

func TestDelegateTaskTool_Execute_InvalidTimeout(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testClaudeCoderName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentTask := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClaudeCoderName,
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: runtimeTypeClaude},
		},
	}

	k8sClient := newFakeClient(parentTask(), agentTask)
	tool := NewDelegateTaskTool(k8sClient)

	_, err := tool.Execute(context.Background(), json.RawMessage(`{
		"agent": "claude-coder",
		"prompt": "Fix the auth module",
		"timeout": "eventually"
	}`))
	if err == nil {
		t.Fatal("Execute() expected error for invalid timeout")
	}
	if !contains(err.Error(), invalidTimeoutCaseName) {
		t.Errorf("Execute() error = %v, want error containing %q", err, invalidTimeoutCaseName)
	}
}

func TestDelegateTaskTool_Execute_AgentNotFound(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "nonexistent-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	// No agent registered in the fake client
	k8sClient := newFakeClient(parentTask())
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{"agent": "nonexistent-agent", "prompt": "Do something"}`)
	_, err := tool.Execute(context.Background(), args)
	if err == nil {
		t.Fatal("Execute() expected error for nonexistent agent")
	}
	if !contains(err.Error(), "failed to get agent") {
		t.Errorf("Execute() error = %v, want error containing %q", err, "failed to get agent")
	}
}

func TestDelegateTaskTool_Execute_AgentTypeNoWorkspace(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testClaudeCoderName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClaudeCoderName,
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: runtimeTypeClaude,
			},
		},
	}

	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	// No workspace, maxTurns, or allowBash args
	args := json.RawMessage(`{"agent": "claude-coder", "prompt": "Fix bugs"}`)
	_, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	taskList := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), taskList); err != nil {
		t.Fatalf("failed to list tasks: %v", err)
	}

	var childTask *corev1alpha1.Task
	for i := range taskList.Items {
		if taskList.Items[i].Name != parentTaskName {
			childTask = &taskList.Items[i]
			break
		}
	}
	if childTask == nil {
		t.Fatal("child task not found")
	}

	if childTask.Spec.Type != corev1alpha1.TaskTypeAgent {
		t.Errorf("spec.type = %q, want %q", childTask.Spec.Type, corev1alpha1.TaskTypeAgent)
	}
	if childTask.Spec.AgentRuntime == nil {
		t.Fatal("spec.agentRuntime should not be nil for agent-type tasks")
	}
	if childTask.Spec.Workspace != nil {
		t.Error("spec.agentRuntime.workspace should be nil when not provided")
	}
	if childTask.Spec.AgentRuntime.MaxTurns != nil {
		t.Error("spec.agentRuntime.maxTurns should be nil when not provided")
	}
	if childTask.Spec.AgentRuntime.AllowBash != nil {
		t.Error("spec.agentRuntime.allowBash should be nil when not provided")
	}
}

func TestDelegateTaskTool_Execute_MaterializesRuntimeRefAllowedTools(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name    string
		allowed []string
	}{
		{name: "nonempty allowlist", allowed: []string{"check_messages", "send_message"}},
		{name: "explicit deny all", allowed: []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envOrkaTaskName, parentTaskName)
			t.Setenv(envOrkaTaskNamespace, defaultNamespace)
			t.Setenv(envOrkaCoordinationDepth, "0")
			t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
			t.Setenv(envOrkaCoordinationMaxDepth, "3")

			const runtimeName = "external-runtime"
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
				}},
			}
			runtime := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{
							ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
						},
						MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
							AllowedTools:          append([]string{}, tt.allowed...),
							DisallowedTools:       []string{},
							ApprovalRequiredTools: []string{},
						},
					},
				},
			}
			parent := parentTask()
			parent.Spec.Transaction = nil
			k8sClient := newFakeClient(parent, agent, runtime)
			result, err := NewDelegateTaskTool(k8sClient).Execute(
				context.Background(),
				json.RawMessage(`{"agent":"external-agent","prompt":"work"}`),
			)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			var response DelegateTaskResult
			if err := json.Unmarshal([]byte(result), &response); err != nil {
				t.Fatal(err)
			}
			child := &corev1alpha1.Task{}
			if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
				Name: response.TaskName, Namespace: defaultNamespace,
			}, child); err != nil {
				t.Fatal(err)
			}
			if child.Spec.AgentRuntime == nil {
				t.Fatal("agentRuntime = nil, want materialized runtime policy")
			}
			if !slices.Equal(child.Spec.AgentRuntime.AllowedTools, tt.allowed) {
				t.Fatalf("allowedTools = %#v, want %#v", child.Spec.AgentRuntime.AllowedTools, tt.allowed)
			}
			if child.Spec.AgentRuntime.AllowedTools == nil {
				t.Fatal("allowedTools = nil, want explicit list")
			}
		})
	}
}

func TestDelegateTaskTool_Execute_UsesRuntimePolicyReader(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	cachedAgent, cachedRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	liveAgent, liveRuntime := externalRuntimePolicyFixtures([]string{"Write"})
	parent := parentTask()
	parent.Spec.Transaction = nil
	cachedClient := newFakeClient(parent, cachedAgent, cachedRuntime)
	liveReader := newFakeClient(parent, liveAgent, liveRuntime)
	ctx := WithToolContext(context.Background(), &ToolContext{PolicyReader: liveReader})

	result, err := NewDelegateTaskTool(cachedClient).Execute(
		ctx,
		json.RawMessage(`{"agent":"external-agent","prompt":"work"}`),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var response DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		t.Fatal(err)
	}
	child := &corev1alpha1.Task{}
	if err := cachedClient.Get(context.Background(), apitypes.NamespacedName{
		Name: response.TaskName, Namespace: defaultNamespace,
	}, child); err != nil {
		t.Fatal(err)
	}
	if child.Spec.AgentRuntime == nil || !slices.Equal(child.Spec.AgentRuntime.AllowedTools, []string{"Write"}) {
		t.Fatalf("agentRuntime = %#v, want current live policy", child.Spec.AgentRuntime)
	}
}

func TestDelegateTaskTool_Execute_UsesRuntimePolicyReaderForTransactionValidation(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	cachedAgent, cachedRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	liveAgent, liveRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	cachedRuntime.Spec.Capabilities.Profile.ProviderKind = "cached-provider"
	cachedRuntime.Spec.Capabilities.Profile.Model = "cached-model"
	liveRuntime.Spec.Capabilities.Profile.ProviderKind = "live-provider"
	liveRuntime.Spec.Capabilities.Profile.Model = "live-model"
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["external-agent"]`,
		"provider":      "cached-provider",
		"model":         "cached-model",
		"allowedTools":  `["Read"]`,
	}
	cachedClient := newFakeClient(parent, cachedAgent, cachedRuntime)
	liveReader := newFakeClient(parent, liveAgent, liveRuntime)
	ctx := WithToolContext(context.Background(), &ToolContext{PolicyReader: liveReader})

	_, err := NewDelegateTaskTool(cachedClient).Execute(
		ctx,
		json.RawMessage(`{"agent":"external-agent","prompt":"work"}`),
	)
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("Execute() error = %v, want live provider policy denial", err)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := cachedClient.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 1 || tasks.Items[0].Name != parentTaskName {
		t.Fatalf("Tasks = %#v, want only parent Task after transaction denial", tasks.Items)
	}
}

func TestDelegateTaskTool_Execute_FailsClosedWhenPolicyReaderCannotResolveFallbackProvider(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	cachedAgent, cachedRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	liveAgent, liveRuntime := externalRuntimePolicyFixtures([]string{"Read"})
	fallback := corev1alpha1.ModelFallback{ProviderRef: "fallback-provider", Model: "claude-sonnet-4"}
	cachedAgent.Spec.Model = &corev1alpha1.ModelConfig{Fallbacks: []corev1alpha1.ModelFallback{fallback}}
	liveAgent.Spec.Model = &corev1alpha1.ModelConfig{Fallbacks: []corev1alpha1.ModelFallback{fallback}}
	fallbackProvider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: fallback.ProviderRef, Namespace: defaultNamespace},
		Spec: corev1alpha1.ProviderSpec{
			Type:         corev1alpha1.ProviderTypeAnthropic,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: "fallback-secret"},
			DefaultModel: fallback.Model,
		},
	}
	parent := parentTask()
	parent.Spec.Transaction.Context = map[string]string{
		"namespace":     defaultNamespace,
		"allowedAgents": `["external-agent"]`,
		"provider":      "codex",
		"model":         "gpt-5.6",
		"allowedTools":  `["Read"]`,
	}
	cachedClient := newFakeClient(parent, cachedAgent, cachedRuntime, fallbackProvider)
	liveReader := newFakeClientWithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == fallback.ProviderRef {
				if _, ok := obj.(*corev1alpha1.Provider); ok {
					return errors.New("authoritative fallback provider read failed")
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, parent, liveAgent, liveRuntime, fallbackProvider)
	ctx := WithToolContext(context.Background(), &ToolContext{PolicyReader: liveReader})

	_, err := NewDelegateTaskTool(cachedClient).Execute(
		ctx,
		json.RawMessage(`{"agent":"external-agent","prompt":"work"}`),
	)
	if err == nil || !strings.Contains(err.Error(), "authoritative fallback provider read failed") {
		t.Fatalf("Execute() error = %v, want authoritative fallback provider read failure", err)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := cachedClient.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 1 || tasks.Items[0].Name != parentTaskName {
		t.Fatalf("Tasks = %#v, want only parent Task after fallback provider read failure", tasks.Items)
	}
}

func TestDelegateTaskTool_Execute_RejectsRuntimeRefOverrides(t *testing.T) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	for _, tt := range []struct {
		name string
		args string
		want string
	}{
		{name: "maxTurns", args: `{"agent":"external-agent","prompt":"work","maxTurns":10}`, want: "do not support maxTurns"},
		{name: "allowBash", args: `{"agent":"external-agent","prompt":"work","allowBash":false}`, want: "do not support allowBash"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envOrkaTaskName, parentTaskName)
			t.Setenv(envOrkaTaskNamespace, defaultNamespace)
			t.Setenv(envOrkaCoordinationDepth, "0")
			t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
			t.Setenv(envOrkaCoordinationMaxDepth, "3")

			const runtimeName = "external-runtime"
			agent := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
					RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: runtimeName},
				}},
			}
			runtime := &corev1alpha1.AgentRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: runtimeName, Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentRuntimeRegistrySpec{
					ContractVersion: &contract,
					Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
						Profile: &corev1alpha1.AgentRuntimeProfileSpec{
							ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
						},
						MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
							AllowedTools:          []string{},
							DisallowedTools:       []string{},
							ApprovalRequiredTools: []string{},
						},
					},
				},
			}
			parent := parentTask()
			parent.Spec.Transaction = nil
			k8sClient := newFakeClient(parent, agent, runtime)
			_, err := NewDelegateTaskTool(k8sClient).Execute(context.Background(), json.RawMessage(tt.args))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Execute() error = %v, want %q", err, tt.want)
			}
			tasks := &corev1alpha1.TaskList{}
			if err := k8sClient.List(context.Background(), tasks); err != nil {
				t.Fatal(err)
			}
			if len(tasks.Items) != 1 || tasks.Items[0].Name != parentTaskName {
				t.Fatalf("Tasks = %#v, want only parent Task after unsupported override", tasks.Items)
			}
		})
	}
}

func TestDelegateTaskTool_Execute_RejectsRuntimeRefPriorTaskBeforeCreate(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "external-agent")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	agent := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
		}},
	}
	runtime := &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          []string{},
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
	prior := &corev1alpha1.Task{ObjectMeta: metav1.ObjectMeta{
		Name: testPriorTaskName, Namespace: defaultNamespace,
	}}
	parent := parentTask()
	parent.Spec.Transaction = nil
	k8sClient := newFakeClient(parent, prior, agent, runtime)

	_, err := NewDelegateTaskTool(k8sClient).Execute(context.Background(), json.RawMessage(
		`{"agent":"external-agent","prompt":"work","prior_task":"prior-task-1"}`,
	))
	if err == nil || !strings.Contains(err.Error(), "do not support priorTaskRef workspace handoff") {
		t.Fatalf("Execute() error = %v, want priorTaskRef rejection", err)
	}
	tasks := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Items) != 2 {
		t.Fatalf("Tasks = %#v, want only parent and prior Tasks", tasks.Items)
	}
	for _, task := range tasks.Items {
		if task.Name != parentTaskName && task.Name != testPriorTaskName {
			t.Fatalf("unexpected child Task %q created", task.Name)
		}
	}
}

func TestDelegateTaskTool_Execute_AITypeNoRuntime(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{"agent": "researcher", "prompt": "Research the topic"}`)
	_, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	taskList := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), taskList); err != nil {
		t.Fatalf("failed to list tasks: %v", err)
	}

	var childTask *corev1alpha1.Task
	for i := range taskList.Items {
		if taskList.Items[i].Name != parentTaskName {
			childTask = &taskList.Items[i]
			break
		}
	}
	if childTask == nil {
		t.Fatal("child task not found")
	}

	if childTask.Spec.Type != corev1alpha1.TaskTypeAI {
		t.Errorf("spec.type = %q, want %q", childTask.Spec.Type, corev1alpha1.TaskTypeAI)
	}
	if childTask.Spec.AgentRuntime != nil {
		t.Error("spec.agentRuntime should be nil for AI-type tasks")
	}
	if childTask.Spec.AgentRef == nil || childTask.Spec.AgentRef.Name != testResearcherAgentName {
		t.Fatalf("spec.agentRef = %#v, want %q", childTask.Spec.AgentRef, testResearcherAgentName)
	}
	if childTask.Spec.AI == nil {
		t.Fatal("spec.ai is nil for a native AI child")
	}
	if childTask.Spec.AI.Prompt != "Research the topic" {
		t.Fatalf("spec.ai.prompt = %q, want %q", childTask.Spec.AI.Prompt, "Research the topic")
	}
	if childTask.Spec.AI.ProviderRef != nil || childTask.Spec.AI.Provider != "" ||
		childTask.Spec.AI.Model != "" || childTask.Spec.AI.SystemPrompt != "" ||
		childTask.Spec.AI.Temperature != nil || childTask.Spec.AI.MaxTokens != nil ||
		len(childTask.Spec.AI.Skills) != 0 || len(childTask.Spec.AI.Tools) != 0 {
		t.Fatalf("spec.ai copied Agent configuration: %#v", childTask.Spec.AI)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDelegateTaskTool_Execute_PriorTask(t *testing.T) {
	// Create a prior task in the fake client
	prior := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPriorTaskName,
			Namespace: defaultNamespace,
			UID:       "prior-uid",
			Labels: map[string]string{
				labels.LabelIteration:      "1",
				labels.LabelIterationGroup: "group-abc",
			},
		},
		Spec: corev1alpha1.TaskSpec{
			Type:   corev1alpha1.TaskTypeAgent,
			Prompt: "original prompt",
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo: "https://github.com/example/repo",
				Branch:  testBranch,
			},
		},
		Status: corev1alpha1.TaskStatus{
			Phase: corev1alpha1.TaskPhaseSucceeded,
		},
	}

	parent := parentTask()
	agent := researcherAgent()

	fakeClient := newFakeClient(parent, agent, prior)
	tool := NewDelegateTaskTool(fakeClient)

	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)

	args, _ := json.Marshal(map[string]any{
		"agent": testResearcherAgentName, promptField: "fix the bug", priorTaskField: testPriorTaskName, "feedback": "Add error handling",
	})

	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if delegateResult.Status != GitHubPullRequestStatusCreated {
		t.Errorf("expected status 'created', got %q", delegateResult.Status)
	}

	// Verify the child task was created with PriorTaskRef
	childTask := &corev1alpha1.Task{}
	if err := fakeClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("get child task: %v", err)
	}

	if childTask.Spec.PriorTaskRef == nil {
		t.Fatal("expected PriorTaskRef to be set")
	}
	if childTask.Spec.PriorTaskRef.Name != testPriorTaskName {
		t.Errorf("expected PriorTaskRef.Name 'prior-task-1', got %q", childTask.Spec.PriorTaskRef.Name)
	}

	// Verify feedback was prepended to prompt
	if !strings.Contains(childTask.Spec.Prompt, "FEEDBACK FROM REVIEW") {
		t.Errorf("expected prompt to contain feedback, got %q", childTask.Spec.Prompt)
	}
	if !strings.Contains(childTask.Spec.Prompt, "Add error handling") {
		t.Errorf("expected prompt to contain feedback text")
	}

	// Verify iteration labels
	if childTask.Labels[labels.LabelIteration] != "2" {
		t.Errorf("expected iteration=2, got %q", childTask.Labels[labels.LabelIteration])
	}
	if childTask.Labels[labels.LabelIterationGroup] != "group-abc" {
		t.Errorf("expected iteration-group=group-abc, got %q", childTask.Labels[labels.LabelIterationGroup])
	}

	// Verify workspace was copied from prior task
	if childTask.Spec.Workspace == nil {
		t.Fatal("expected workspace to be copied from prior task")
	}
	if childTask.Spec.Workspace.GitRepo != "https://github.com/example/repo" {
		t.Errorf("expected git repo from prior task, got %q", childTask.Spec.Workspace.GitRepo)
	}
}

func TestDelegateTaskTool_Execute_FeedbackOnly(t *testing.T) {
	parent := parentTask()
	agent := researcherAgent()

	fakeClient := newFakeClient(parent, agent)
	tool := NewDelegateTaskTool(fakeClient)

	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)

	args, _ := json.Marshal(map[string]any{
		"agent": testResearcherAgentName, promptField: "implement feature", "feedback": "Use dependency injection",
	})

	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	var delegateResult DelegateTaskResult
	_ = json.Unmarshal([]byte(result), &delegateResult)

	// Verify the child task was created with feedback in prompt
	childTask := &corev1alpha1.Task{}
	_ = fakeClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask)

	if !strings.Contains(childTask.Spec.Prompt, "FEEDBACK FROM REVIEW") {
		t.Errorf("expected feedback in prompt, got %q", childTask.Spec.Prompt)
	}
	if childTask.Spec.AI == nil || childTask.Spec.AI.Prompt != childTask.Spec.Prompt {
		t.Fatalf("spec.ai = %#v, want feedback-adjusted prompt %q", childTask.Spec.AI, childTask.Spec.Prompt)
	}
	// PriorTaskRef should NOT be set
	if childTask.Spec.PriorTaskRef != nil {
		t.Errorf("expected PriorTaskRef to be nil when prior_task not specified")
	}
}

func TestDelegateTaskTool_Execute_PushBranch(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testClaudeCoderName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClaudeCoderName,
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{
				Type: runtimeTypeClaude,
			},
		},
	}

	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{
		"agent": "claude-coder",
		"prompt": "Implement feature",
		"workspace": {
			"gitRepo": "https://github.com/sozercan/ayna",
			"branch": "main",
			"readCredentialRef": "git-credentials",
			"publicationReadCredentialRef": "git-target-read",
			"publicationCredentialRef": "git-target-write",
			"forgeCredentialRef": "git-forge",
			"pushBranch": "feature/edit-message"
		}
	}`)

	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	_ = json.Unmarshal([]byte(result), &delegateResult)

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}

	if childTask.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	ws := childTask.Spec.Workspace
	if ws.PushBranch != "feature/edit-message" {
		t.Errorf("pushBranch = %q, want %q", ws.PushBranch, "feature/edit-message")
	}
	if ws.GitRepo != testSozercanAynaRepoURL {
		t.Errorf("gitRepo = %q, want %q", ws.GitRepo, testSozercanAynaRepoURL)
	}
	if ws.ReadCredentialRef == nil || ws.ReadCredentialRef.Name != "git-credentials" {
		t.Errorf("readCredentialRef = %v, want git-credentials", ws.ReadCredentialRef)
	}
	if ws.PublicationReadCredentialRef == nil || ws.PublicationReadCredentialRef.Name != "git-target-read" {
		t.Errorf("publicationReadCredentialRef = %v, want git-target-read", ws.PublicationReadCredentialRef)
	}
	if ws.PublicationCredentialRef == nil || ws.PublicationCredentialRef.Name != "git-target-write" {
		t.Errorf("publicationCredentialRef = %v, want git-target-write", ws.PublicationCredentialRef)
	}
	if ws.ForgeCredentialRef == nil || ws.ForgeCredentialRef.Name != "git-forge" {
		t.Errorf("forgeCredentialRef = %v, want git-forge", ws.ForgeCredentialRef)
	}
}

func TestDelegateTaskTool_Execute_RequiresExplicitPublicationCredentialForWrite(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testClaudeCoderName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: testClaudeCoderName, Namespace: defaultNamespace},
		Spec:       corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{Type: runtimeTypeClaude}},
	}
	tool := NewDelegateTaskTool(newFakeClient(parentTask(), agentWithRuntime))
	_, err := tool.Execute(context.Background(), json.RawMessage(`{
		"agent":"claude-coder",
		"prompt":"Implement feature",
		"workspace":{"gitRepo":"https://github.com/sozercan/ayna","intent":"write","readCredentialRef":"git-credentials"}
	}`))
	if err == nil || !strings.Contains(err.Error(), "publicationCredentialRef is required") {
		t.Fatalf("Execute() error = %v, want explicit publication credential denial", err)
	}
}

func TestDelegateTaskTool_Execute_AutoDiscoversReadCredentialRef(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "copilot-coder")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "copilot-coder",
			Namespace: defaultNamespace,
		},
		Spec: corev1alpha1.AgentSpec{
			Runtime:   &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
			SecretRef: &corev1.LocalObjectReference{Name: testCustomCopilotSecretName},
		},
	}

	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{
		"agent": "copilot-coder",
		"prompt": "Implement feature",
		"workspace": {
			"gitRepo": "https://github.com/sozercan/ayna",
			"branch": "main",
			"publicationCredentialRef": "git-target-write",
			"pushBranch": "feature/auto-secret"
		}
	}`)

	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	_ = json.Unmarshal([]byte(result), &delegateResult)

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}

	if childTask.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if childTask.Spec.Workspace.ReadCredentialRef == nil {
		t.Fatal("expected readCredentialRef to be auto-populated")
	}
	if childTask.Spec.Workspace.ReadCredentialRef.Name != testCustomCopilotSecretName {
		t.Errorf("readCredentialRef = %q, want %q", childTask.Spec.Workspace.ReadCredentialRef.Name, testCustomCopilotSecretName)
	}
}

func TestDelegateTaskTool_Execute_RepositoryFreeWorkspaceSkipsReadCredentialDiscovery(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "copilot-coder")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	// The agent carries a discoverable secret, but the controller workspace
	// preflight rejects readCredentialRef without gitRepo — auto-discovery must
	// not doom a repository-free workspace.
	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "copilot-coder", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{
			Runtime:   &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
			SecretRef: &corev1.LocalObjectReference{Name: testCustomCopilotSecretName},
		},
	}
	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	result, err := tool.Execute(context.Background(), json.RawMessage(`{
		"agent": "copilot-coder",
		"prompt": "Summarize the design",
		"workspace": {"intent": "read"}
	}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatal(err)
	}
	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}
	if childTask.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if childTask.Spec.Workspace.ReadCredentialRef != nil {
		t.Fatalf("readCredentialRef = %#v, want nil without gitRepo", childTask.Spec.Workspace.ReadCredentialRef)
	}
}

func TestDelegateTaskTool_Execute_ExplicitReadCredentialRequiresGitRepo(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "copilot-coder")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "copilot-coder", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
		},
	}
	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	_, err := tool.Execute(context.Background(), json.RawMessage(`{
		"agent": "copilot-coder",
		"prompt": "Summarize the design",
		"workspace": {"readCredentialRef": "my-secret"}
	}`))
	if err == nil || !strings.Contains(err.Error(), "readCredentialRef requires workspace.gitRepo") {
		t.Fatalf("Execute() error = %v, want readCredentialRef requires workspace.gitRepo", err)
	}
	taskList := &corev1alpha1.TaskList{}
	if err := k8sClient.List(context.Background(), taskList); err != nil {
		t.Fatal(err)
	}
	for _, task := range taskList.Items {
		if task.Name != parentTaskName {
			t.Fatalf("unexpected child Task %q created", task.Name)
		}
	}
}

func TestDelegateTaskTool_Execute_CanonicalizesSSHWorkspaceRepository(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "copilot-coder")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	agentWithRuntime := &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "copilot-coder", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{
			Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
		},
	}
	k8sClient := newFakeClient(parentTask(), agentWithRuntime)
	tool := NewDelegateTaskTool(k8sClient)

	result, err := tool.Execute(context.Background(), json.RawMessage(`{
		"agent": "copilot-coder",
		"prompt": "Summarize the design",
		"workspace": {"gitRepo": "git@github.com:myorg/myrepo.git"}
	}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatal(err)
	}
	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("failed to get child task: %v", err)
	}
	if childTask.Spec.Workspace == nil {
		t.Fatal("expected workspace to be set")
	}
	if got, want := childTask.Spec.Workspace.GitRepo, "https://github.com/myorg/myrepo"; got != want {
		t.Fatalf("workspace.gitRepo = %q, want canonical %q", got, want)
	}
}

func TestDelegateTaskTool_Execute_RejectsDoomedWriteWorkspaces(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, "copilot-coder")
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	tests := []struct {
		name      string
		workspace string
		want      string
	}{
		{
			name:      "write intent without gitRepo",
			workspace: `{"intent": "write", "publicationCredentialRef": "repo-write"}`,
			want:      "gitRepo is required for write intent",
		},
		{
			name:      "createPR without prBaseBranch",
			workspace: `{"gitRepo": "https://github.com/myorg/myrepo", "createPR": true, "publicationCredentialRef": "repo-write", "forgeCredentialRef": "repo-forge"}`,
			want:      "createPR requires workspace.prBaseBranch",
		},
		{
			name:      "createPR without forgeCredentialRef",
			workspace: `{"gitRepo": "https://github.com/myorg/myrepo", "createPR": true, "prBaseBranch": "main", "publicationCredentialRef": "repo-write"}`,
			want:      "createPR requires workspace.forgeCredentialRef",
		},
		{
			name:      "non-HTTPS repository URL",
			workspace: `{"gitRepo": "http://github.com/myorg/myrepo"}`,
			want:      "credential-free HTTPS URL",
		},
		{
			name:      "unsupported source ref namespace",
			workspace: `{"gitRepo": "https://github.com/myorg/myrepo", "ref": "refs/remotes/origin/main"}`,
			want:      "workspace.ref is invalid",
		},
		{
			name:      "malformed source branch",
			workspace: `{"gitRepo": "https://github.com/myorg/myrepo", "branch": "bad..branch"}`,
			want:      "workspace.branch is invalid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agentWithRuntime := &corev1alpha1.Agent{
				ObjectMeta: metav1.ObjectMeta{Name: "copilot-coder", Namespace: defaultNamespace},
				Spec: corev1alpha1.AgentSpec{
					Runtime: &corev1alpha1.AgentCLIRuntime{Type: corev1alpha1.AgentRuntimeCopilot},
				},
			}
			k8sClient := newFakeClient(parentTask(), agentWithRuntime)
			tool := NewDelegateTaskTool(k8sClient)

			_, err := tool.Execute(context.Background(), json.RawMessage(`{
				"agent": "copilot-coder",
				"prompt": "Make the change",
				"workspace": `+tt.workspace+`
			}`))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Execute() error = %v, want %q", err, tt.want)
			}
			taskList := &corev1alpha1.TaskList{}
			if err := k8sClient.List(context.Background(), taskList); err != nil {
				t.Fatal(err)
			}
			for _, task := range taskList.Items {
				if task.Name != parentTaskName {
					t.Fatalf("unexpected child Task %q created", task.Name)
				}
			}
		})
	}
}

func TestDelegateTaskTool_Execute_AutoRetry(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{"agent": "researcher", "prompt": "Do the work", "auto_retry": true, "max_retries": 3}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	// Fetch child task and verify annotations
	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("get child task: %v", err)
	}

	if childTask.Annotations[labels.AnnotationAutoRetry] != trueStr {
		t.Errorf("expected auto-retry=true, got %q", childTask.Annotations[labels.AnnotationAutoRetry])
	}
	if childTask.Annotations[labels.AnnotationMaxRetries] != "3" {
		t.Errorf("expected max-retries=3, got %q", childTask.Annotations[labels.AnnotationMaxRetries])
	}
	if childTask.Annotations[labels.AnnotationRetryCount] != "0" {
		t.Errorf("expected retry-count=0, got %q", childTask.Annotations[labels.AnnotationRetryCount])
	}
	if childTask.Annotations[labels.AnnotationOriginalPrompt] != "Do the work" {
		t.Errorf("expected original-prompt stored, got %q", childTask.Annotations[labels.AnnotationOriginalPrompt])
	}
}

func TestDelegateTaskTool_Execute_AutoRetryDefault(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	// auto_retry without max_retries should default to 2
	args := json.RawMessage(`{"agent": "researcher", "prompt": "Do work", "auto_retry": true}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("get child task: %v", err)
	}

	if childTask.Annotations[labels.AnnotationMaxRetries] != "2" {
		t.Errorf("expected default max-retries=2, got %q", childTask.Annotations[labels.AnnotationMaxRetries])
	}
}

func TestDelegateTaskTool_Execute_NoAutoRetry(t *testing.T) {
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	tool := NewDelegateTaskTool(k8sClient)

	args := json.RawMessage(`{"agent": "researcher", "prompt": "Do work"}`)
	result, err := tool.Execute(context.Background(), args)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{
		Name: delegateResult.TaskName, Namespace: defaultNamespace,
	}, childTask); err != nil {
		t.Fatalf("get child task: %v", err)
	}

	// When auto_retry is not set, no retry annotations should be present
	if _, ok := childTask.Annotations[labels.AnnotationAutoRetry]; ok {
		t.Error("expected no auto-retry annotation when auto_retry is false")
	}
}

func TestDelegateTaskToolExecuteStampsTraceContextAndSpanAttributes(t *testing.T) {
	if _, err := orkatracing.Init("test", false); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	spans := testutil.NewSpanHarness(t)
	t.Setenv(envOrkaTaskName, parentTaskName)
	t.Setenv(envOrkaTaskNamespace, defaultNamespace)
	t.Setenv(envOrkaCoordinationDepth, "0")
	t.Setenv(envOrkaCoordinationAllowedAgents, testResearcherAgentName)
	t.Setenv(envOrkaCoordinationMaxDepth, "3")

	k8sClient := newFakeClient(parentTask(), researcherAgent())
	registry := NewRegistry()
	registry.Register(NewDelegateTaskTool(k8sClient))

	rootCtx, rootSpan := orkatracing.Tracer("test").Start(context.Background(), "task.run")
	stepCtx, stepSpan := orkatracing.Tracer("test").Start(rootCtx, "agent.step")
	toolCtx := WithToolContext(stepCtx, &ToolContext{TaskID: parentTaskName, Namespace: defaultNamespace, Tenant: defaultNamespace})
	result, err := registry.Execute(toolCtx, delegateTaskToolName, json.RawMessage(`{"agent":"researcher","prompt":"Research"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	stepSpan.End()
	rootSpan.End()

	var delegateResult DelegateTaskResult
	if err := json.Unmarshal([]byte(result), &delegateResult); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	childTask := &corev1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), apitypes.NamespacedName{Name: delegateResult.TaskName, Namespace: defaultNamespace}, childTask); err != nil {
		t.Fatalf("get child task: %v", err)
	}
	if childTask.Annotations[labels.AnnotationTraceParent] == "" {
		t.Fatalf("missing %s annotation", labels.AnnotationTraceParent)
	}

	delegateSpan := testutil.SpanNamed(spans.Recorder.Ended(), "execute_tool delegate_task")
	if delegateSpan == nil {
		t.Fatal("missing delegate tool span")
	}
	attrs := testutil.AttributeMap(delegateSpan)
	if got := attrs[orkatracing.AttrToolKind].AsString(); got != orkatracing.ToolKindDelegate {
		t.Fatalf("%s = %q", orkatracing.AttrToolKind, got)
	}
	if got := attrs[orkatracing.AttrParentTaskID].AsString(); got != parentTaskName {
		t.Fatalf("%s = %q", orkatracing.AttrParentTaskID, got)
	}
	if got := attrs[orkatracing.AttrChildTaskID].AsString(); got != delegateResult.TaskName {
		t.Fatalf("%s = %q", orkatracing.AttrChildTaskID, got)
	}

	childCtx := orkatracing.ExtractTaskTraceContext(context.Background(), childTask)
	childTaskCtx, childRunSpan := orkatracing.Tracer("test").Start(childCtx, "child task.run")
	childStepCtx, childStepSpan := orkatracing.Tracer("test").Start(childTaskCtx, "child agent.step")
	_, childModelSpan := orkatracing.GenAITracer(genai.InstrumentationName).Start(childStepCtx, "chat test-model")
	childModelSpan.End()
	childStepSpan.End()
	childRunSpan.End()
	childRun := testutil.SpanNamed(spans.Recorder.Ended(), "child task.run")
	if childRun == nil {
		t.Fatal("missing child task.run span")
	}
	if got, want := childRun.SpanContext().TraceID(), delegateSpan.SpanContext().TraceID(); got != want {
		t.Fatalf("child trace id = %s, want %s", got, want)
	}
	if got, want := childRun.Parent().SpanID(), delegateSpan.SpanContext().SpanID(); got != want {
		t.Fatalf("child parent span id = %s, want delegate span %s", got, want)
	}
	childStep := testutil.SpanNamed(spans.Recorder.Ended(), "child agent.step")
	childModel := testutil.SpanNamed(spans.Recorder.Ended(), "chat test-model")
	if childStep == nil || childModel == nil {
		t.Fatalf("missing child step/model spans: step=%v model=%v", childStep != nil, childModel != nil)
	}
	if got, want := childModel.SpanContext().TraceID(), delegateSpan.SpanContext().TraceID(); got != want {
		t.Fatalf("child model trace id = %s, want %s", got, want)
	}
	if got, want := childModel.Parent().SpanID(), childStep.SpanContext().SpanID(); got != want {
		t.Fatalf("child model parent = %s, want child step %s", got, want)
	}
}
