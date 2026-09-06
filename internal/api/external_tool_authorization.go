/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const externalToolTaskResource = "tasks"

// authorizeExternalToolContext keeps API tool calls on the caller's resource
// permissions. Controller and worker clients are never changed.
func authorizeExternalToolContext(tc *tools.ToolContext, userInfo *UserInfo, events store.GatewayEventStore) {
	authorizedClient := newExternalToolClient(tc.Client, tc.KubeClient, userInfo, tc.Namespace, tc.WatchNamespace, tc.EnforceNamespaceIsolation, events)
	authorization := authorizedClient.authorization
	tc.Client = authorizedClient
	if tc.ResultStore != nil {
		tc.ResultStore = &externalToolResultReader{reader: tc.ResultStore, client: authorizedClient}
	}
	if tc.SessionDeleter != nil {
		tc.SessionDeleter = &externalToolSessionDeleter{deleter: tc.SessionDeleter, authorization: authorization}
	}
	tc.AuthorizePodLogs = func(ctx context.Context, namespace, podName string) error {
		return authorization.authorize(ctx, namespace, "get", "", "pods/log", podName)
	}
	if userInfo != nil && userInfo.AuthType == AuthTypeTokenReview {
		tc.RequireGitHubTaskCredentials = true
		tc.AuthorizeCodeExecResources = func(ctx context.Context, objects []client.Object) error {
			for _, obj := range objects {
				if err := authorizedClient.authorize(ctx, obj, obj.GetNamespace(), "create", ""); err != nil {
					return err
				}
				if err := authorizedClient.authorize(ctx, obj, obj.GetNamespace(), "delete", obj.GetName()); err != nil {
					return err
				}
			}
			return nil
		}
		tc.AuthorizeAgentInitialTask = func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			for _, resource := range []string{"agents", externalToolTaskResource} {
				if err := authorization.authorize(ctx, agent.Namespace, "create", corev1alpha1.GroupVersion.Group, resource, ""); err != nil {
					return &tools.ChatToolError{Type: "permission_denied", Message: err.Error(), Suggestion: "Check RBAC permissions"}
				}
			}
			return nil
		}
	}
}

// The proxy's GitHub tools retain their constructor client. Bind those two
// instances to this request without modifying the shared registry.
func registryForExternalToolCall(tc *tools.ToolContext, name string) *tools.Registry {
	if tc == nil {
		return tools.DefaultRegistry
	}
	if _, ok := tc.Client.(*externalToolClient); !ok {
		return tools.DefaultRegistry
	}
	tool, _ := tools.DefaultRegistry.Get(name)
	var requestTool tools.Tool
	switch tool.(type) {
	case *tools.CreatePullRequestTool:
		requestTool = tools.NewCreatePullRequestTool(tc.Client)
	case *tools.CheckPullRequestCITool:
		requestTool = tools.NewCheckPullRequestCITool(tc.Client)
	default:
		return tools.DefaultRegistry
	}
	registry := tools.NewRegistry()
	registry.Register(requestTool)
	return registry
}

type externalToolAuthorization struct {
	kubeClient                kubernetes.Interface
	userInfo                  *UserInfo
	namespace                 string
	watchNamespace            string
	enforceNamespaceIsolation bool
	gatewayEventStore         store.GatewayEventStore
}

func (a *externalToolAuthorization) authorize(ctx context.Context, namespace, verb, group, resource, name string) error {
	if strings.TrimSpace(namespace) == "" || blockedNamespaces[namespace] ||
		(a.watchNamespace != "" && namespace != a.watchNamespace) ||
		(a.enforceNamespaceIsolation && namespace != a.namespace) {
		return externalToolForbidden(group, resource, name)
	}
	if err := authorizeKubernetesResourceAction(ctx, a.kubeClient, a.userInfo, namespace, verb, group, resource, name); err != nil {
		return externalToolForbidden(group, resource, name)
	}
	return nil
}

func externalToolForbidden(group, resource, name string) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: group, Resource: resource}, name, errors.New("not authorized"))
}

// externalToolClient authorizes the actual client operation, after each tool
// resolves its arguments and defaults. It deliberately does not expose Watch.
type externalToolClient struct {
	client.Client
	authorization *externalToolAuthorization
}

func newExternalToolClient(c client.Client, kubeClient kubernetes.Interface, userInfo *UserInfo, namespace, watchNamespace string, enforceNamespaceIsolation bool, events store.GatewayEventStore) *externalToolClient {
	return &externalToolClient{Client: c, authorization: &externalToolAuthorization{
		kubeClient: kubeClient, userInfo: userInfo, namespace: namespace,
		watchNamespace: watchNamespace, enforceNamespaceIsolation: enforceNamespaceIsolation,
		gatewayEventStore: events,
	}}
}

// Discovery omits unavailable metadata while keeping ordinary chat usable.
// Executable tools receive the strict client and still return authorization errors.
type externalToolDiscoveryClient struct {
	client.Client
}

func (c externalToolDiscoveryClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := c.Client.List(ctx, list, opts...)
	if apierrors.IsForbidden(err) {
		return meta.SetList(list, nil)
	}
	return err
}

func (c *externalToolClient) authorize(ctx context.Context, obj runtime.Object, namespace, verb, name string) error {
	if c.Client == nil || (reflect.ValueOf(c.Client).Kind() == reflect.Pointer && reflect.ValueOf(c.Client).IsNil()) {
		return externalToolForbidden("", "resources", name)
	}
	resource, ok := externalToolResource(obj)
	if !ok {
		return externalToolForbidden("", "resources", name)
	}
	return c.authorization.authorize(ctx, namespace, verb, resource.Group, resource.Resource, name)
}

// These are the concrete resources used by management tools and code_exec.
// New resource access must be assigned an explicit permission here.
func externalToolResource(obj runtime.Object) (schema.GroupResource, bool) {
	group := corev1alpha1.GroupVersion.Group
	resource := ""
	switch obj.(type) {
	case *corev1alpha1.Task, *corev1alpha1.TaskList:
		resource = externalToolTaskResource
	case *corev1alpha1.Agent, *corev1alpha1.AgentList:
		resource = "agents"
	case *corev1alpha1.Tool, *corev1alpha1.ToolList:
		resource = "tools"
	case *corev1alpha1.Provider, *corev1alpha1.ProviderList:
		resource = "providers"
	case *corev1alpha1.Skill, *corev1alpha1.SkillList:
		resource = "skills"
	case *corev1.Secret, *corev1.SecretList:
		group, resource = "", "secrets"
	case *corev1.ConfigMap, *corev1.ConfigMapList:
		group, resource = "", "configmaps"
	case *corev1.ServiceAccount, *corev1.ServiceAccountList:
		group, resource = "", "serviceaccounts"
	case *corev1.Pod, *corev1.PodList:
		group, resource = "", "pods"
	case *batchv1.Job, *batchv1.JobList:
		group, resource = batchv1.GroupName, "jobs"
	case *networkingv1.NetworkPolicy, *networkingv1.NetworkPolicyList:
		group, resource = networkingv1.GroupName, "networkpolicies"
	default:
		return schema.GroupResource{}, false
	}
	return schema.GroupResource{Group: group, Resource: resource}, true
}

func (c *externalToolClient) authorizeTask(ctx context.Context, task *corev1alpha1.Task) error {
	_, gatewayOwned, err := gatewayTaskIdentity(ctx, c.authorization.gatewayEventStore, task)
	if err != nil {
		return err
	}
	// Gateway Tasks require the dedicated Gateway read/operate API checks.
	// Generic chat tools cannot act on them or reveal their transcripts.
	if gatewayOwned {
		return apierrors.NewNotFound(schema.GroupResource{Group: corev1alpha1.GroupVersion.Group, Resource: externalToolTaskResource}, task.Name)
	}
	return nil
}

func (c *externalToolClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.authorize(ctx, obj, key.Namespace, "get", key.Name); err != nil {
		return err
	}
	if task, ok := obj.(*corev1alpha1.Task); ok {
		loaded := &corev1alpha1.Task{}
		if err := c.Client.Get(ctx, key, loaded, opts...); err != nil {
			return err
		}
		if err := c.authorizeTask(ctx, loaded); err != nil {
			return err
		}
		*task = *loaded
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *externalToolClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	if err := c.authorize(ctx, list, options.Namespace, "list", ""); err != nil {
		return err
	}
	if tasks, ok := list.(*corev1alpha1.TaskList); ok {
		loaded := &corev1alpha1.TaskList{}
		if err := c.Client.List(ctx, loaded, opts...); err != nil {
			return err
		}
		visible := make([]corev1alpha1.Task, 0, len(loaded.Items))
		for i := range loaded.Items {
			if err := c.authorizeTask(ctx, &loaded.Items[i]); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			visible = append(visible, loaded.Items[i])
		}
		loaded.Items = visible
		*tasks = *loaded
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *externalToolClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.authorize(ctx, obj, obj.GetNamespace(), "create", ""); err != nil {
		return err
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *externalToolClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := c.authorize(ctx, obj, obj.GetNamespace(), "delete", obj.GetName()); err != nil {
		return err
	}
	if _, ok := obj.(*corev1alpha1.Task); ok {
		current := &corev1alpha1.Task{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			return err
		}
		if err := c.authorizeTask(ctx, current); err != nil {
			return err
		}
		// Fence the loaded ownership decision against replacement or mutation.
		opts = append(opts, client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion})
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *externalToolClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.authorize(ctx, obj, obj.GetNamespace(), "update", obj.GetName()); err != nil {
		return err
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *externalToolClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := c.authorize(ctx, obj, obj.GetNamespace(), "patch", obj.GetName()); err != nil {
		return err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// No external tool needs apply, collection deletion, or subresource mutation.
// Keep those entry points closed instead of inheriting the controller client.
func (c *externalToolClient) Apply(context.Context, runtime.ApplyConfiguration, ...client.ApplyOption) error {
	return externalToolForbidden("", "resources", "")
}

func (c *externalToolClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	return externalToolForbidden("", "resources", "")
}

func (c *externalToolClient) Status() client.SubResourceWriter {
	return deniedExternalToolSubResource{}
}

func (c *externalToolClient) SubResource(string) client.SubResourceClient {
	return deniedExternalToolSubResource{}
}

type deniedExternalToolSubResource struct{}

func (deniedExternalToolSubResource) Get(context.Context, client.Object, client.Object, ...client.SubResourceGetOption) error {
	return externalToolForbidden("", "resources", "")
}

func (deniedExternalToolSubResource) Create(context.Context, client.Object, client.Object, ...client.SubResourceCreateOption) error {
	return externalToolForbidden("", "resources", "")
}

func (deniedExternalToolSubResource) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return externalToolForbidden("", "resources", "")
}

func (deniedExternalToolSubResource) Patch(context.Context, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
	return externalToolForbidden("", "resources", "")
}

func (deniedExternalToolSubResource) Apply(context.Context, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
	return externalToolForbidden("", "resources", "")
}

type externalToolResultReader struct {
	reader interface {
		GetResult(context.Context, string, string) ([]byte, error)
	}
	client client.Client
}

func (r *externalToolResultReader) GetResult(ctx context.Context, namespace, taskName string) ([]byte, error) {
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: taskName}, &corev1alpha1.Task{}); err != nil {
		return nil, err
	}
	return r.reader.GetResult(ctx, namespace, taskName)
}

type externalToolSessionDeleter struct {
	deleter interface {
		DeleteSession(context.Context, string, string) error
	}
	authorization *externalToolAuthorization
}

func (d *externalToolSessionDeleter) DeleteSession(ctx context.Context, namespace, sessionID string) error {
	if err := d.authorization.authorize(ctx, namespace, "delete", corev1alpha1.GroupVersion.Group, "sessions", sessionID); err != nil {
		return err
	}
	if d.deleter == nil || (reflect.ValueOf(d.deleter).Kind() == reflect.Pointer && reflect.ValueOf(d.deleter).IsNil()) {
		return fmt.Errorf("session manager is unavailable")
	}
	return d.deleter.DeleteSession(ctx, namespace, sessionID)
}
