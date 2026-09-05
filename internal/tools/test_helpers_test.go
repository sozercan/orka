/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func newFakeClient(objs ...client.Object) client.Client {
	return newFakeClientWithInterceptorFuncs(interceptor.Funcs{}, objs...)
}

func newFakeClientWithInterceptorFuncs(funcs interceptor.Funcs, objs ...client.Object) client.Client {
	if funcs.Create == nil {
		funcs.Create = assignFakeUIDOnCreate
	}
	return fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&corev1alpha1.Task{}).
		WithInterceptorFuncs(funcs).
		Build()
}

func assignFakeUIDOnCreate(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if err := c.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if obj.GetUID() != "" {
		return nil
	}
	obj.SetUID(apitypes.UID(fmt.Sprintf("test-uid-%s-%s", obj.GetNamespace(), obj.GetName())))
	return c.Update(ctx, obj)
}

func newFakeClientWithAgents(agents []*corev1alpha1.Agent) client.Client {
	objs := make([]client.Object, len(agents))
	for i, a := range agents {
		objs[i] = a
	}
	return newFakeClient(objs...)
}

func newFakeClientWithTools(tools []*corev1alpha1.Tool) client.Client {
	objs := make([]client.Object, len(tools))
	for i, t := range tools {
		objs[i] = t
	}
	return newFakeClient(objs...)
}

func externalRuntimePolicyFixtures(allowedTools []string) (*corev1alpha1.Agent, *corev1alpha1.AgentRuntime) {
	contract := corev1alpha1.AgentRuntimeContractHarnessV2
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "external-agent", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentSpec{Runtime: &corev1alpha1.AgentCLIRuntime{
			RuntimeRef: &corev1alpha1.AgentRuntimeReference{Name: "external-runtime"},
		}},
	}, &corev1alpha1.AgentRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "external-runtime", Namespace: defaultNamespace},
		Spec: corev1alpha1.AgentRuntimeRegistrySpec{
			ContractVersion: &contract,
			Capabilities: &corev1alpha1.AgentRuntimeCapabilitiesSpec{
				Profile: &corev1alpha1.AgentRuntimeProfileSpec{
					ProviderKind: "codex", Model: "gpt-5.6", WorkspaceIntent: corev1alpha1.WorkspaceIntentRead,
				},
				MCPPolicy: &corev1alpha1.AgentRuntimeMCPPolicySpec{
					AllowedTools:          append([]string{}, allowedTools...),
					DisallowedTools:       []string{},
					ApprovalRequiredTools: []string{},
				},
			},
		},
	}
}

func githubRepoTaskWithSecret(repoURL string) (*corev1alpha1.Task, *corev1.Secret) {
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: testCoderTaskName, Namespace: defaultNamespace},
		Spec: corev1alpha1.TaskSpec{
			Type: corev1alpha1.TaskTypeAgent,
			Workspace: &corev1alpha1.WorkspaceConfig{
				GitRepo:           repoURL,
				ReadCredentialRef: &corev1alpha1.WorkspaceCredentialReference{Name: testGitCredsSecretName},
				ForgeCredentialRef: &corev1alpha1.WorkspaceCredentialReference{
					Name: testGitCredsSecretName,
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testGitCredsSecretName, Namespace: defaultNamespace},
		Data:       map[string][]byte{tokenKey: []byte(testGitHubToken)},
	}
	return task, secret
}

func contextWithTaskScope() context.Context {
	return WithToolContext(context.Background(), &ToolContext{
		TaskID:    testCoderTaskName,
		Namespace: defaultNamespace,
	})
}
