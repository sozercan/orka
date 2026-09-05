/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/cli/client"
)

const (
	cliTaskTypeAI    = "ai"
	cliTaskTypeAgent = "agent"
	cliTaskTypeCont  = "container"
)

func newTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Manage tasks",
	}
	cmd.AddCommand(newTaskCreateCmd())
	cmd.AddCommand(newTaskListCmd())
	cmd.AddCommand(newTaskGetCmd())
	cmd.AddCommand(newTaskRuntimeStatusCmd())
	cmd.AddCommand(newTaskLogsCmd())
	cmd.AddCommand(newTaskEventsCmd())
	cmd.AddCommand(newTaskFollowCmd())
	cmd.AddCommand(newTaskTraceCmd())
	cmd.AddCommand(newTaskApprovalsCmd())
	cmd.AddCommand(newTaskApprovalDecisionCmd("approve", "Approve a pending task approval", "approve"))
	cmd.AddCommand(newTaskApprovalDecisionCmd("decline", "Decline a pending task approval", "decline"))
	cmd.AddCommand(newTaskForkCmd())
	cmd.AddCommand(newTaskResultCmd())
	cmd.AddCommand(newTaskPlanCmd())
	cmd.AddCommand(newTaskChildrenCmd())
	cmd.AddCommand(newTaskWaitCmd())
	cmd.AddCommand(newTaskDeleteCmd())
	cmd.AddCommand(newTaskArtifactsCmd())
	cmd.AddCommand(newTaskDownloadCmd())
	return cmd
}

func newTaskCreateCmd() *cobra.Command {
	var taskType, taskName, agent, provider, model, timeout, image, schedule, timezone, file string
	var commandVals, argVals, envVals []string
	var priority int32
	var suspend bool
	var workspaceOptions taskWorkspaceCreateOptions

	cmd := &cobra.Command{
		Use:   "create <prompt>",
		Short: "Create a new task",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)

			if file != "" {
				body, err := manifestWithNamespaceJSON(cmd, file, c.Namespace)
				if err != nil {
					return err
				}
				result, err := c.CreateTaskRaw(context.Background(), body)
				if err != nil {
					return err
				}
				name := client.StringField(*result, "metadata", "name")
				fmt.Printf("Task created: %s\n", name)
				return nil
			}

			prompt := strings.Join(args, " ")
			// The default type is ai, but an explicit --image is an
			// unambiguous request for a container task. --agent is not:
			// it also names native AI Agents, so the referenced Agent's
			// spec decides whether this is an agent task. Only an explicit
			// --type overrides the inference.
			if !cmd.Flags().Changed("type") {
				if strings.TrimSpace(image) != "" && strings.TrimSpace(agent) != "" {
					return fmt.Errorf("--image and --agent are ambiguous without --type: pass --type container or --type ai|agent explicitly")
				}
				switch {
				case strings.TrimSpace(image) != "":
					taskType = cliTaskTypeCont
				case strings.TrimSpace(agent) != "":
					resolved, err := resolveAgentTaskType(context.Background(), c, agent)
					if err != nil {
						return err
					}
					taskType = resolved
				}
			}
			if taskType == "" {
				taskType = cliTaskTypeAI
			}
			if (taskType == cliTaskTypeAI || taskType == cliTaskTypeAgent) && strings.TrimSpace(prompt) == "" {
				return fmt.Errorf("prompt is required for ai and agent tasks unless --file is used")
			}
			if taskName == "" {
				b := make([]byte, 4)
				_, _ = rand.Read(b)
				taskName = "task-" + hex.EncodeToString(b)
			}

			req := client.CreateTaskRequest{
				Name:      taskName,
				Namespace: c.Namespace,
				Type:      taskType,
				Image:     image,
				Command:   commandVals,
				Args:      argVals,
				Prompt:    prompt,
				Timeout:   timeout,
				Schedule:  schedule,
			}
			if timezone != "" {
				req.TimeZone = &timezone
			}
			if cmd.Flags().Changed("suspend") {
				req.Suspend = &suspend
			}
			if cmd.Flags().Changed("priority") {
				req.Priority = &priority
			}
			if len(envVals) > 0 {
				for _, env := range envVals {
					key, value, ok := strings.Cut(env, "=")
					if !ok || key == "" {
						return fmt.Errorf("invalid --env %q, expected KEY=VALUE", env)
					}
					req.Env = append(req.Env, struct {
						Name  string `json:"name"`
						Value string `json:"value,omitempty"`
					}{Name: key, Value: value})
				}
			}

			if agent != "" {
				req.AgentRef = &struct {
					Name string `json:"name"`
				}{Name: agent}
			}

			if taskType == cliTaskTypeAI {
				if strings.TrimSpace(provider) == "" && agent == "" {
					// No Provider named: pick the namespace's only ready
					// Provider, or explain what is available, instead of
					// assuming one named "default" exists.
					resolved, err := resolveDefaultProviderName(cmd.Context(), c)
					if err != nil {
						return err
					}
					provider = resolved
				}
				req.AI = &struct {
					ProviderRef *struct {
						Name string `json:"name"`
					} `json:"providerRef,omitempty"`
					Model  string `json:"model,omitempty"`
					Prompt string `json:"prompt,omitempty"`
				}{
					Model:  model,
					Prompt: prompt,
				}
				if strings.TrimSpace(provider) != "" {
					req.AI.ProviderRef = &struct {
						Name string `json:"name"`
					}{Name: strings.TrimSpace(provider)}
				}
			}

			workspace, err := workspaceOptions.build(cmd, taskType)
			if err != nil {
				return err
			}
			var externalRuntimePolicy *resolvedAgentRuntimePolicy
			if agent != "" && taskType == cliTaskTypeAgent {
				policy, err := resolveAgentRuntimePolicy(cmd.Context(), c, agent)
				if err != nil {
					return err
				}
				if policy != nil {
					req.AgentRuntime = &corev1alpha1.AgentRuntimeSpec{AllowedTools: policy.allowedTools}
					externalRuntimePolicy = policy
				}
			}
			if externalRuntimePolicy != nil && externalRuntimePolicy.requireWorkspaceIntentMatch {
				taskIntent := corev1alpha1.WorkspaceIntentRead
				if workspace != nil {
					intent, ok := workspace["intent"].(string)
					if !ok {
						return fmt.Errorf("prepare external AgentRuntime workspace intent: workspace intent is missing")
					}
					taskIntent = corev1alpha1.WorkspaceIntent(intent)
				}
				if externalRuntimePolicy.workspaceIntent != taskIntent {
					return fmt.Errorf(
						"AgentRuntime %q profile workspace intent %q does not match Task intent %q",
						externalRuntimePolicy.runtimeName,
						externalRuntimePolicy.workspaceIntent,
						taskIntent,
					)
				}
			}
			body, err := json.Marshal(req)
			if err != nil {
				return fmt.Errorf("marshal task request: %w", err)
			}
			if workspace != nil {
				var payload map[string]any
				if err := json.Unmarshal(body, &payload); err != nil {
					return fmt.Errorf("prepare task workspace request: %w", err)
				}
				payload["workspace"] = workspace
				body, err = json.Marshal(payload)
				if err != nil {
					return fmt.Errorf("marshal task workspace request: %w", err)
				}
			}
			result, err := c.CreateTaskRaw(context.Background(), body)
			if err != nil {
				return err
			}

			name := client.StringField(*result, "metadata", "name")
			fmt.Printf("Task created: %s\n", name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&file, "file", "f", "", "Path to task YAML/JSON manifest")
	cmd.Flags().StringVar(&taskName, "name", "", "Task name (default: generated)")
	cmd.Flags().StringVar(
		&taskType,
		"type",
		cliTaskTypeAI,
		"Task type: "+cliTaskTypeAI+", "+cliTaskTypeCont+", "+cliTaskTypeAgent,
	)
	cmd.Flags().StringVar(&image, "image", "", "Container image")
	cmd.Flags().StringArrayVar(&commandVals, "command", nil, "Command entry to run (repeat for multiple entries)")
	cmd.Flags().StringArrayVar(&argVals, "arg", nil, "Command argument (repeat for multiple arguments)")
	cmd.Flags().StringArrayVar(&envVals, "env", nil, "Environment variable KEY=VALUE (repeatable)")
	cmd.Flags().Int32Var(&priority, "priority", 0, "Task priority (0-1000)")
	cmd.Flags().StringVar(&agent, "agent", "", "Agent reference name")
	cmd.Flags().StringVar(&provider, "provider", "", "Provider reference name for ai tasks (default: a ready Provider named \"default\", else the namespace's only ready Provider)")
	cmd.Flags().StringVar(&model, "model", "", "Model name for AI tasks")
	cmd.Flags().StringVar(&timeout, "timeout", "", "Task timeout (e.g., \"5m\", \"1h\")")
	cmd.Flags().StringVar(&schedule, "schedule", "", "Cron schedule for recurring tasks")
	cmd.Flags().StringVar(&timezone, "timezone", "", "IANA time zone for scheduled tasks")
	cmd.Flags().BoolVar(&suspend, "suspend", false, "Suspend scheduled task runs")
	workspaceOptions.bindFlags(cmd)

	return cmd
}

func newTaskListCmd() *cobra.Command {
	var status string
	var transactionID string
	var limit int
	var continueToken string

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List tasks",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := newClientFromCmd(cmd)
			var tasks []client.TaskSummary
			if status != "" || transactionID != "" {
				var truncated bool
				var err error
				tasks, truncated, err = listFilteredTasks(
					context.Background(),
					c,
					c.Namespace,
					limit,
					func(t client.TaskSummary) bool {
						if status != "" && !strings.EqualFold(t.Phase, status) {
							return false
						}
						if transactionID != "" && t.TransactionID != transactionID {
							return false
						}
						return true
					},
				)
				if err != nil {
					return err
				}
				if truncated {
					warnFilteredTaskOutputLimited(limit)
				}
			} else {
				var err error
				tasks, err = c.ListTasks(context.Background(), client.ListTasksOptions{
					Namespace: c.Namespace,
					Limit:     limit,
					Continue:  continueToken,
				})
				if err != nil {
					return err
				}
			}

			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			if format != outputTable {
				return printStructured(cmd, tasks)
			}

			if len(tasks) == 0 {
				fmt.Println("No tasks found.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tTYPE\tSTATUS\tAGE") //nolint:errcheck
			for _, t := range tasks {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", t.Name, t.Type, t.Phase, formatAge(t.Age)) //nolint:errcheck
			}
			w.Flush() //nolint:errcheck
			return nil
		},
	}

	cmd.Flags().StringVar(&status, "status", "", "Filter by status (client-side scan; may page through many tasks)")
	cmd.Flags().StringVar(&transactionID, "transaction", "", "Filter by transaction ID (client-side scan)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Maximum number of results")
	cmd.Flags().StringVar(&continueToken, "continue", "", "Continue token for the next page")
	cmd.Flags().StringVar(&continueToken, "cursor", "", "Cursor token for the next page")
	addOutputFlag(cmd, outputTable)

	return cmd
}

func newTaskGetCmd() *cobra.Command {
	var showTransaction bool

	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get task details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			detail, err := c.GetTask(context.Background(), args[0], client.GetOptions{
				Namespace: c.Namespace,
			})
			if err != nil {
				return err
			}

			if showTransaction {
				transaction, ok := taskTransaction(*detail)
				if !ok {
					fmt.Println("No transaction metadata found.")
					return nil
				}
				out, err := json.MarshalIndent(transaction, "", "  ")
				if err != nil {
					return fmt.Errorf("formatting transaction output: %w", err)
				}
				fmt.Println(string(out))
				return nil
			}

			return printStructured(cmd, detail)
		},
	}

	cmd.Flags().BoolVar(&showTransaction, "show-transaction", false, "Show only transaction metadata")
	addOutputFlag(cmd, outputJSON)
	return cmd
}

func taskTransaction(detail client.TaskDetail) (map[string]any, bool) {
	spec, ok := detail["spec"].(map[string]any)
	if !ok {
		return nil, false
	}
	transaction, ok := spec["transaction"].(map[string]any)
	if !ok || len(transaction) == 0 {
		return nil, false
	}
	return transaction, true
}

func newTaskLogsCmd() *cobra.Command {
	var follow bool

	cmd := &cobra.Command{
		Use:   "logs <name>",
		Short: "Get task logs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)

			if follow {
				ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
				defer cancel()

				err := c.StreamTaskLogs(ctx, args[0], client.StreamLogsOptions{
					Namespace: c.Namespace,
					Writer:    os.Stdout,
				})
				if err != nil && ctx.Err() == nil {
					return err
				}
				return nil
			}

			logsResp, err := c.GetTaskLogs(context.Background(), args[0], client.GetOptions{
				Namespace: c.Namespace,
			})
			if err != nil {
				return err
			}

			if logsResp.Logs != "" {
				fmt.Print(logsResp.Logs)
			} else if logsResp.Message != "" {
				fmt.Println(logsResp.Message)
			}
			return nil
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Stream logs in real time")

	return cmd
}

func newTaskResultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "result <name>",
		Short: "Get task result",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			result, err := c.GetTaskResult(context.Background(), args[0], client.GetOptions{Namespace: c.Namespace})
			if err != nil {
				return err
			}
			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			if format == outputTable {
				fmt.Fprint(cmd.OutOrStdout(), result.Result) //nolint:errcheck
				if !strings.HasSuffix(result.Result, "\n") {
					fmt.Fprintln(cmd.OutOrStdout()) //nolint:errcheck
				}
				return nil
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	return cmd
}

func newTaskPlanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plan <name>",
		Short: "Get task autonomous plan state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			result, err := c.GetTaskPlan(context.Background(), args[0], client.GetOptions{Namespace: c.Namespace})
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputJSON)
	return cmd
}

func newTaskChildrenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "children <name>",
		Short: "List child tasks",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			result, err := c.GetTaskChildren(context.Background(), args[0], client.GetOptions{Namespace: c.Namespace})
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	return cmd
}

func newTaskWaitCmd() *cobra.Command {
	var timeout string
	cmd := &cobra.Command{
		Use:   "wait <name>",
		Short: "Wait for a task to complete",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var deadline <-chan time.Time
			if timeout != "" {
				d, err := time.ParseDuration(timeout)
				if err != nil {
					return fmt.Errorf("invalid timeout: %w", err)
				}
				deadline = time.After(d)
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			c := newClientFromCmd(cmd)
			return waitForTaskPhase(
				ctx,
				args[0],
				deadline,
				2*time.Second,
				func(ctx context.Context) (string, error) {
					detail, err := c.GetTask(ctx, args[0], client.GetOptions{Namespace: c.Namespace})
					if err != nil {
						return "", err
					}
					return client.StringField(*detail, "status", "phase"), nil
				},
				cmd.OutOrStdout(),
			)
		},
	}
	cmd.Flags().StringVar(&timeout, "timeout", "", "Maximum time to wait (e.g. 5m)")
	return cmd
}

func waitForTaskPhase(
	ctx context.Context,
	taskName string,
	deadline <-chan time.Time,
	pollInterval time.Duration,
	getPhase func(context.Context) (string, error),
	out io.Writer,
) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		phase, err := getPhase(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		switch strings.ToLower(phase) {
		case "succeeded":
			fmt.Fprintf(out, "Task %s succeeded.\n", taskName) //nolint:errcheck
			return nil
		case "failed", "cancelled":
			return fmt.Errorf("task %s finished with phase %s", taskName, phase)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for task %s", taskName)
		case <-ticker.C:
		}
	}
}

func newTaskDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm"},
		Short:   "Delete a task",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			if err := c.DeleteTask(context.Background(), args[0], client.GetOptions{
				Namespace: c.Namespace,
			}); err != nil {
				return err
			}
			fmt.Printf("Task deleted: %s\n", args[0])
			return nil
		},
	}
}

func formatAge(timestamp string) string {
	if timestamp == "" {
		return "<unknown>"
	}
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return timestamp
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

const (
	cliProvidersAPIPath     = "/api/v1/providers"
	cliAgentRuntimesAPIPath = "/api/v1/agent-runtimes"
	cliNamespaceQuery       = "namespace"
)

type resolvedAgentRuntimePolicy struct {
	runtimeName                 string
	allowedTools                []string
	workspaceIntent             corev1alpha1.WorkspaceIntent
	requireWorkspaceIntentMatch bool
}

// resolveAgentRuntimePolicy loads the task policy for a runtimeRef Agent.
// Harness v1 requires an explicit empty task allowlist, while harness v2 uses
// the registered MCP policy and workspace intent. A nil result means the Agent
// is missing, built-in, or has no classified harness contract. An explicit
// empty allowedTools result is a deny-all policy and must be serialized as [].
func resolveAgentRuntimePolicy(ctx context.Context, c *client.Client, agentName string) (*resolvedAgentRuntimePolicy, error) {
	agentPath := "/api/v1/agents/" + url.PathEscape(strings.TrimSpace(agentName))
	body, _, err := c.GetRaw(ctx, agentPath, map[string]string{cliNamespaceQuery: c.Namespace})
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q: %w", agentName, err)
	}
	var agent *corev1alpha1.Agent
	if err := json.Unmarshal(body, &agent); err != nil || agent == nil || strings.TrimSpace(agent.Name) == "" {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q: invalid Agent response", agentName)
	}
	if agent.Spec.Runtime == nil || agent.Spec.Runtime.RuntimeRef == nil {
		return nil, nil
	}
	runtimeName := strings.TrimSpace(agent.Spec.Runtime.RuntimeRef.Name)
	if runtimeName == "" {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q: runtimeRef.name is required", agentName)
	}

	runtimePath := cliAgentRuntimesAPIPath + "/" + url.PathEscape(runtimeName)
	body, _, err = c.GetRaw(ctx, runtimePath, map[string]string{cliNamespaceQuery: c.Namespace})
	if err != nil {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: %w", agentName, runtimeName, err)
	}
	var runtime *corev1alpha1.AgentRuntime
	if err := json.Unmarshal(body, &runtime); err != nil || runtime == nil || strings.TrimSpace(runtime.Name) == "" {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: invalid AgentRuntime response", agentName, runtimeName)
	}
	switch runtime.RegisteredContractVersion() {
	case corev1alpha1.AgentRuntimeContractHarnessV1:
		return &resolvedAgentRuntimePolicy{
			runtimeName:  runtimeName,
			allowedTools: []string{},
		}, nil
	case corev1alpha1.AgentRuntimeContractHarnessV2:
	default:
		return nil, nil
	}
	if runtime.Spec.Capabilities == nil || runtime.Spec.Capabilities.MCPPolicy == nil {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: capabilities.mcpPolicy is required", agentName, runtimeName)
	}
	allowedTools := runtime.Spec.Capabilities.MCPPolicy.AllowedTools
	if allowedTools == nil {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: capabilities.mcpPolicy.allowedTools must be an explicit list", agentName, runtimeName)
	}
	if runtime.Spec.Capabilities.Profile == nil {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: capabilities.profile is required", agentName, runtimeName)
	}
	workspaceIntent := runtime.Spec.Capabilities.Profile.WorkspaceIntent
	if workspaceIntent != corev1alpha1.WorkspaceIntentRead && workspaceIntent != corev1alpha1.WorkspaceIntentWrite {
		return nil, fmt.Errorf("resolve AgentRuntime policy for --agent %q from %q: capabilities.profile.workspaceIntent must be read or write", agentName, runtimeName)
	}
	return &resolvedAgentRuntimePolicy{
		runtimeName:                 runtimeName,
		allowedTools:                append([]string{}, allowedTools...),
		workspaceIntent:             workspaceIntent,
		requireWorkspaceIntentMatch: true,
	}, nil
}

// resolveAgentTaskType decides whether --agent names an ACP runtime Agent
// (task type "agent") or a native AI Agent (task type "ai") by reading the
// Agent's spec. A missing Agent (404) keeps the historical "ai" default —
// creation surfaces the real problem server-side — but any other read
// failure (for example a token without agents read permission) is surfaced
// instead of silently guessing: a wrong guess would submit an AI Task for a
// runtime Agent, or vice versa.
func resolveAgentTaskType(ctx context.Context, c *client.Client, agent string) (string, error) {
	body, _, err := c.GetRaw(ctx, "/api/v1/agents/"+url.PathEscape(strings.TrimSpace(agent)), map[string]string{cliNamespaceQuery: c.Namespace})
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return cliTaskTypeAI, nil
		}
		return "", fmt.Errorf("cannot infer the task type for --agent %q (%v); pass --type ai or --type agent explicitly", agent, err)
	}
	var object *struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Runtime json.RawMessage `json:"runtime"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &object); err != nil || object == nil || strings.TrimSpace(object.Metadata.Name) == "" {
		return "", fmt.Errorf("cannot infer the task type for --agent %q from an invalid Agent response; pass --type ai or --type agent explicitly", agent)
	}
	if len(object.Spec.Runtime) > 0 && string(object.Spec.Runtime) != "null" {
		return cliTaskTypeAgent, nil
	}
	return cliTaskTypeAI, nil
}

// resolveDefaultProviderName selects the Provider for an ai task when none was
// named: the namespace's only ready Provider is used; otherwise the error
// lists what exists so the user can pass --provider. The list is paged through
// completely so the decision never rests on a truncated first page.
func resolveDefaultProviderName(ctx context.Context, c *client.Client) (string, error) {
	type providerItem struct {
		Name     string `json:"name"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Ready  bool `json:"ready"`
		Status struct {
			Ready bool `json:"ready"`
		} `json:"status"`
	}
	var items []providerItem
	seenCursor := map[string]struct{}{}
	continueToken := ""
	for {
		query := map[string]string{cliNamespaceQuery: c.Namespace}
		if continueToken != "" {
			query["continue"] = continueToken
		}
		body, _, err := c.GetRaw(ctx, cliProvidersAPIPath, query)
		if err != nil {
			return "", fmt.Errorf("no --provider given and Providers could not be listed: %w", err)
		}
		var list struct {
			Items    []providerItem `json:"items"`
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(body, &list); err != nil {
			return "", fmt.Errorf("no --provider given and the Provider list could not be parsed: %w", err)
		}
		items = append(items, list.Items...)
		next := strings.TrimSpace(list.Metadata.Continue)
		if next == "" {
			break
		}
		if _, repeated := seenCursor[next]; repeated {
			return "", fmt.Errorf("no --provider given and the Provider list repeated a continuation cursor")
		}
		seenCursor[next] = struct{}{}
		continueToken = next
	}
	names := make([]string, 0, len(items))
	ready := make([]string, 0, len(items))
	for _, item := range items {
		name := item.Name
		if name == "" {
			name = item.Metadata.Name
		}
		if name == "" {
			continue
		}
		names = append(names, name)
		if item.Ready || item.Status.Ready {
			ready = append(ready, name)
		}
	}
	// Mirror the server resolver's precedence: a ready Provider named
	// "default" wins outright, then a sole ready Provider.
	for _, name := range ready {
		if name == "default" {
			return name, nil
		}
	}
	if len(ready) == 1 {
		return ready[0], nil
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no --provider given and namespace %q has no Providers; create one first", c.Namespace)
	}
	sort.Strings(names)
	return "", fmt.Errorf("no --provider given and namespace %q has %d Providers (%s); pass --provider", c.Namespace, len(names), strings.Join(names, ", "))
}
