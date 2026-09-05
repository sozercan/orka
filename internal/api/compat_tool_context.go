/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/tools"
)

const maxProxyCreatedTasks = 20

type compatProxyToolContextProfile struct {
	SourceLabel       string
	TaskCreateAction  string
	TaskDeleteAction  string
	AgentCreateAction string
	AgentUpdateAction string
	AgentDeleteAction string
	SecretReadAction  string
}

var openAICompatProxyToolContextProfile = compatProxyToolContextProfile{
	SourceLabel:       "openai-proxy",
	TaskCreateAction:  "openAIToolCreateTask",
	TaskDeleteAction:  "openAIToolDeleteTask",
	AgentCreateAction: "openAIToolCreateAgent",
	AgentUpdateAction: "openAIToolUpdateAgent",
	AgentDeleteAction: "openAIToolDeleteAgent",
	SecretReadAction:  "openAIToolReadSecret",
}

var anthropicCompatProxyToolContextProfile = compatProxyToolContextProfile{
	SourceLabel:       "anthropic-proxy",
	TaskCreateAction:  "anthropicToolCreateTask",
	AgentCreateAction: "anthropicToolCreateAgent",
	SecretReadAction:  "anthropicToolReadSecret",
}

type compatProxyToolContextConfig struct {
	Client                    client.Client
	AuthorizationReader       client.Reader
	KubeClient                kubernetes.Interface
	Namespace                 string
	Provider                  ProviderResolutionInfo
	WatchNamespace            string
	EnforceNamespaceIsolation bool
	ResultStore               store.ResultStore
	GenerateTaskName          func() string
	Profile                   compatProxyToolContextProfile
	AuthContext               *ContextToken
	AuthorizationConfig       ContextTokenAuthorizationConfig
	UserInfo                  *UserInfo
}

func newCompatProxyToolContext(cfg compatProxyToolContextConfig) *tools.ToolContext {
	tasksCreated := 0
	authorizationReader := cfg.AuthorizationReader
	if authorizationReader == nil {
		authorizationReader = cfg.Client
	}
	toolCtx := &tools.ToolContext{
		Client:                    cfg.Client,
		PolicyReader:              authorizationReader,
		KubeClient:                cfg.KubeClient,
		Namespace:                 cfg.Namespace,
		Tenant:                    cfg.Namespace,
		Provider:                  cfg.Provider.Name,
		ProviderType:              cfg.Provider.Type,
		WatchNamespace:            cfg.WatchNamespace,
		EnforceNamespaceIsolation: cfg.EnforceNamespaceIsolation,
		ResultStore:               cfg.ResultStore,
		GenerateTaskName:          cfg.GenerateTaskName,
		TaskLabels:                func() map[string]string { return map[string]string{"orka.ai/source": cfg.Profile.SourceLabel} },
		CheckTaskLimit: func() *tools.ChatToolError {
			if tasksCreated >= maxProxyCreatedTasks {
				return &tools.ChatToolError{Type: "limit_reached", Message: fmt.Sprintf("task creation limit reached (max %d)", maxProxyCreatedTasks), Suggestion: "Wait for existing tasks to complete"}
			}
			return nil
		},
		IncrementTasks: func() { tasksCreated++ },
	}
	if cfg.Profile.TaskCreateAction != "" {
		toolCtx.AuthorizeTaskCreate = func(ctx context.Context, task *corev1alpha1.Task) *tools.ChatToolError {
			authorize := func(ctx context.Context, task *corev1alpha1.Task) error {
				return authorizeAndStampToolTaskCreate(ctx, authorizationReader, cfg.KubeClient, cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.TaskCreateAction, cfg.UserInfo, task)
			}
			return chatToolAuthorizationError(authorize, ctx, task, "Use a task configuration authorized by the context token")
		}
	}
	if cfg.Profile.TaskDeleteAction != "" {
		toolCtx.AuthorizeTaskDelete = func(ctx context.Context, task *corev1alpha1.Task) *tools.ChatToolError {
			authorize := func(ctx context.Context, task *corev1alpha1.Task) error {
				return authorizeContextTokenTaskDeleteObject(ctx, authorizationReader, cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.TaskDeleteAction, task)
			}
			return chatToolAuthorizationError(authorize, ctx, task, "Use a task authorized by the context token")
		}
	}
	if cfg.Profile.AgentCreateAction != "" {
		toolCtx.AuthorizeAgentCreate = func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			authorize := func(ctx context.Context, agent *corev1alpha1.Agent) error {
				return authorizeContextTokenToolAgentCreate(ctx, authorizationReader, cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.AgentCreateAction, agent)
			}
			return chatToolAuthorizationError(authorize, ctx, agent, "Use an agent configuration authorized by the context token")
		}
	}
	if cfg.Profile.AgentUpdateAction != "" {
		toolCtx.AuthorizeAgentUpdate = func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			authorize := func(ctx context.Context, agent *corev1alpha1.Agent) error {
				return authorizeContextTokenToolAgentUpdate(ctx, authorizationReader, cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.AgentUpdateAction, agent)
			}
			return chatToolAuthorizationError(authorize, ctx, agent, "Use an agent update authorized by the context token")
		}
	}
	if cfg.Profile.AgentDeleteAction != "" {
		toolCtx.AuthorizeAgentDelete = func(ctx context.Context, agent *corev1alpha1.Agent) *tools.ChatToolError {
			authorize := func(ctx context.Context, agent *corev1alpha1.Agent) error {
				return authorizeContextTokenToolAgentDelete(cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.AgentDeleteAction, agent)
			}
			return chatToolAuthorizationError(authorize, ctx, agent, "Use an agent authorized by the context token")
		}
	}
	if cfg.Profile.SecretReadAction != "" {
		toolCtx.AuthorizeSecretRead = func(ctx context.Context, namespace, secretName string) *tools.ChatToolError {
			if err := authorizeContextTokenSecretRead(cfg.AuthContext, cfg.AuthorizationConfig, cfg.Profile.SecretReadAction, namespace, secretName); err != nil {
				return &tools.ChatToolError{
					Type:       "authorization_failed",
					Message:    err.Error(),
					Suggestion: "Use a context token authorized to read the git credential secret",
				}
			}
			return nil
		}
		toolCtx.RequireSecretReadAuthorization = true
	}
	return toolCtx
}
