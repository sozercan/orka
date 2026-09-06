//nolint:lll
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newMonitorCmd() *cobra.Command {
	cmd := newCRUDResourceCmd(crudResourceSpec{
		Use:      "monitor",
		Short:    "Manage repository monitors",
		BasePath: "/api/v1/monitors/repositories",
		Name:     "repository monitor",
	})
	cmd.AddCommand(newMonitorRunCmd())
	cmd.AddCommand(newMonitorRunsCmd())
	cmd.AddCommand(newMonitorItemsCmd())
	cmd.AddCommand(newMonitorIssuesCmd())
	cmd.AddCommand(newMonitorCommandsCmd())
	cmd.AddCommand(newMonitorActionsCmd())
	cmd.AddCommand(newMonitorWorkActionsCmd())
	cmd.AddCommand(newMonitorImplementationJobsCmd())
	cmd.AddCommand(newMonitorMutationsCmd())
	cmd.AddCommand(newMonitorIssueWorkflowCmd())
	cmd.AddCommand(newMonitorPRWorkflowCmd())
	cmd.AddCommand(newMonitorDoctorCmd())
	cmd.AddCommand(newMonitorWatchCmd())
	cmd.AddCommand(newMonitorTriggerLabelsCmd())
	cmd.AddCommand(newMonitorEventsCmd())
	return cmd
}

func newMonitorRunCmd() *cobra.Command {
	var targetKind, targetSHA string
	var targetNumber int64
	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Trigger a manual repository monitor run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body, _ := json.Marshal(map[string]any{
				"targetKind":   targetKind,
				"targetNumber": targetNumber,
				"targetSHA":    targetSHA,
			})
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/runs"
			result, err := c.DoJSON(context.Background(), http.MethodPost, path, nil, body)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Repository monitor run created: %s\n", metadataName(result)) //nolint:errcheck
			return nil
		},
	}
	cmd.Flags().StringVar(&targetKind, "target-kind", "", "Target kind (pull_request or issue)")
	cmd.Flags().Int64Var(&targetNumber, "target-number", 0, "Target number")
	cmd.Flags().StringVar(&targetSHA, "target-sha", "", "Target commit SHA")
	return cmd
}

func newMonitorRunsCmd() *cobra.Command {
	var limit int
	var cursor, trigger, targetKind string
	cmd := &cobra.Command{
		Use:   "runs <name>",
		Short: "List repository monitor runs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(
				map[string]string{},
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"trigger", trigger,
				"targetKind", targetKind,
			)
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/runs"
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 20, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&trigger, "trigger", "", "Filter by trigger")
	cmd.Flags().StringVar(&targetKind, "target-kind", "", "Filter by target kind")
	return cmd
}

func newMonitorItemsCmd() *cobra.Command {
	var limit int
	var cursor, kind, state, verdict, repairState, automergeState string
	var number int64
	cmd := &cobra.Command{
		Use:   "items <name>",
		Short: "List repository monitor items",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(
				map[string]string{},
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"kind", kind,
				"number", func() string {
					if number > 0 {
						return fmt.Sprintf("%d", number)
					}
					return ""
				}(),
				"state", state,
				"verdict", verdict,
				"repairState", repairState,
				"automergeState", automergeState,
			)
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/items"
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&kind, "kind", "", "Filter by item kind")
	cmd.Flags().Int64Var(&number, "number", 0, "Filter by item number")
	cmd.Flags().StringVar(&state, "state", "", "Filter by state")
	cmd.Flags().StringVar(&verdict, "verdict", "", "Filter by review verdict")
	cmd.Flags().StringVar(&repairState, "repair-state", "", "Filter by repair state")
	cmd.Flags().StringVar(&automergeState, "automerge-state", "", "Filter by automerge state")
	return cmd
}

func newMonitorIssuesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "issues",
		Short: "Inspect repository monitor issue inventory",
	}
	cmd.AddCommand(newMonitorIssuesListCmd())
	cmd.AddCommand(newMonitorIssuesGetCmd())
	return cmd
}

func newMonitorIssuesListCmd() *cobra.Command {
	var limit int
	var cursor, state string
	cmd := &cobra.Command{
		Use:   "list <name>",
		Short: "List repository monitor issues",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(
				map[string]string{},
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"kind", "issue",
				"state", state,
			)
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/items"
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&state, "state", "", "Filter by state")
	return cmd
}

func newMonitorCommandsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commands",
		Short: "Inspect repository monitor command events",
	}
	cmd.AddCommand(newMonitorCommandsListCmd())
	cmd.AddCommand(newMonitorCommandsGetCmd())
	cmd.AddCommand(newMonitorCommandsCreateCmd())
	return cmd
}

//nolint:dupl // Command and action list commands intentionally share CLI plumbing with different filters.
func newMonitorCommandsListCmd() *cobra.Command {
	var limit int
	var cursor, kind, intent, status string
	var number int64
	cmd := &cobra.Command{
		Use:   "list <name>",
		Short: "List repository monitor commands",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(
				map[string]string{},
				"name", args[0],
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"kind", kind,
				"intent", intent,
				"status", status,
			)
			if number > 0 {
				q["number"] = fmt.Sprintf("%d", number)
			}
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/commands", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&kind, "kind", "", "Filter by target kind")
	cmd.Flags().Int64Var(&number, "number", 0, "Filter by target number")
	cmd.Flags().StringVar(&intent, "intent", "", "Filter by command intent")
	cmd.Flags().StringVar(&status, "status", "", "Filter by command status")
	return cmd
}

func newMonitorCommandsGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <command-id>",
		Short: "Get a repository monitor command",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/commands/" + url.PathEscape(args[0])
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, nil, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorCommandsCreateCmd() *cobra.Command {
	var kind, intent, targetSHA string
	var number int64
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a repository monitor command",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := repositoryMonitorCommandRequestBody(kind, number, intent, targetSHA)
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/commands"
			result, err := c.DoJSON(context.Background(), http.MethodPost, path, nil, body)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputYAML)
	cmd.Flags().StringVar(&kind, "kind", "issue", "Target kind (issue or pull_request)")
	cmd.Flags().Int64Var(&number, "number", 0, "Target issue or pull request number")
	cmd.Flags().StringVar(&intent, "intent", "", "Command intent")
	cmd.Flags().StringVar(&targetSHA, "target-sha", "", "Target head SHA for pull request commands")
	_ = cmd.MarkFlagRequired("number")
	_ = cmd.MarkFlagRequired("intent")
	return cmd
}

func repositoryMonitorCommandRequestBody(kind string, number int64, intent, targetSHA string) []byte {
	body := fmt.Sprintf(`{"kind":%s,"number":%d,"intent":%s`, strconv.Quote(kind), number, strconv.Quote(intent))
	if targetSHA != "" {
		body += fmt.Sprintf(`,"targetSHA":%s`, strconv.Quote(targetSHA))
	}
	body += "}"
	return []byte(body)
}

func newMonitorActionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "actions",
		Short: "Inspect repository monitor action records",
	}
	cmd.AddCommand(newMonitorActionsListCmd())
	cmd.AddCommand(newMonitorActionsGetCmd())
	return cmd
}

//nolint:dupl // Command and action list commands intentionally share CLI plumbing with different filters.
func newMonitorActionsListCmd() *cobra.Command {
	var limit int
	var cursor, kind, actionKind, taskName string
	var number int64
	cmd := &cobra.Command{
		Use:   "list <name>",
		Short: "List repository monitor actions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(
				map[string]string{},
				"name", args[0],
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"kind", kind,
				"actionKind", actionKind,
				"taskName", taskName,
			)
			if number > 0 {
				q["number"] = fmt.Sprintf("%d", number)
			}
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/actions", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&kind, "kind", "", "Filter by target kind")
	cmd.Flags().Int64Var(&number, "number", 0, "Filter by target number")
	cmd.Flags().StringVar(&actionKind, "action-kind", "", "Filter by action kind")
	cmd.Flags().StringVar(&taskName, "task-name", "", "Filter by task name")
	return cmd
}

func newMonitorActionsGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <action-id>",
		Short: "Get a repository monitor action",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/actions/" + url.PathEscape(args[0])
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, nil, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorEventsCmd() *cobra.Command {
	var monitorName, runID, itemKind, eventType, cursor string
	var itemNumber int64
	var limit int
	cmd := &cobra.Command{
		Use:   "events [name]",
		Short: "List repository monitor events",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				monitorName = args[0]
			}
			if monitorName == "" {
				return fmt.Errorf("monitor name is required")
			}
			q := mergeQuery(
				map[string]string{},
				"name", monitorName,
				"limit", fmt.Sprintf("%d", limit),
				"cursor", cursor,
				"continue", cursor,
				"runID", runID,
				"itemKind", itemKind,
				"eventType", eventType,
			)
			if itemNumber > 0 {
				q["itemNumber"] = fmt.Sprintf("%d", itemNumber)
			}
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/events", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().StringVar(&monitorName, "name", "", "Repository monitor name")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&runID, "run-id", "", "Filter by run ID")
	cmd.Flags().StringVar(&itemKind, "item-kind", "", "Filter by item kind")
	cmd.Flags().Int64Var(&itemNumber, "item-number", 0, "Filter by item number")
	cmd.Flags().StringVar(&eventType, "event-type", "", "Filter by event type")
	return cmd
}

func newMonitorIssuesGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name> <number>",
		Short: "Get a repository monitor issue item",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(map[string]string{}, "kind", "issue", "number", args[1], "limit", "1")
			c := newClientFromCmd(cmd)
			path := "/api/v1/monitors/repositories/" + url.PathEscape(args[0]) + "/items"
			result, err := c.DoJSON(context.Background(), http.MethodGet, path, q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorWorkActionsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "work-actions", Short: "Inspect repository monitor workflow actions"}
	cmd.AddCommand(newMonitorWorkActionsListCmd())
	cmd.AddCommand(newMonitorWorkActionsGetCmd())
	return cmd
}

func newMonitorWorkActionsListCmd() *cobra.Command {
	var limit int
	var cursor, kind, intent, desiredAction, status, taskName string
	var number int64
	cmd := &cobra.Command{Use: "list <name>", Short: "List repository monitor workflow actions", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		q := mergeQuery(map[string]string{}, "name", args[0], "limit", fmt.Sprintf("%d", limit), "cursor", cursor, "continue", cursor, "kind", kind, "intent", intent, "desiredAction", desiredAction, "status", status, "taskName", taskName)
		if number > 0 {
			q["number"] = fmt.Sprintf("%d", number)
		}
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/work-actions", q, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&kind, "kind", "", "Filter by target kind")
	cmd.Flags().Int64Var(&number, "number", 0, "Filter by target number")
	cmd.Flags().StringVar(&intent, "intent", "", "Filter by command intent")
	cmd.Flags().StringVar(&desiredAction, "desired-action", "", "Filter by desired workflow action")
	cmd.Flags().StringVar(&status, "status", "", "Filter by workflow status")
	cmd.Flags().StringVar(&taskName, "task-name", "", "Filter by task name")
	return cmd
}

func newMonitorWorkActionsGetCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "get <action-id>", Short: "Get a repository monitor workflow action", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/work-actions/"+url.PathEscape(args[0]), nil, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorImplementationJobsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "implementations", Short: "Inspect repository monitor implementation jobs"}
	cmd.AddCommand(newMonitorImplementationJobsListCmd())
	cmd.AddCommand(newMonitorImplementationJobsGetCmd())
	return cmd
}

func newMonitorImplementationJobsListCmd() *cobra.Command {
	var limit int
	var cursor, phase, taskName string
	var issueNumber int64
	cmd := &cobra.Command{Use: "list <name>", Short: "List repository monitor implementation jobs", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		q := mergeQuery(map[string]string{}, "name", args[0], "limit", fmt.Sprintf("%d", limit), "cursor", cursor, "continue", cursor, "phase", phase, "taskName", taskName)
		if issueNumber > 0 {
			q["issueNumber"] = fmt.Sprintf("%d", issueNumber)
		}
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/implementation-jobs", q, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().Int64Var(&issueNumber, "issue-number", 0, "Filter by issue number")
	cmd.Flags().StringVar(&phase, "phase", "", "Filter by phase")
	cmd.Flags().StringVar(&taskName, "task-name", "", "Filter by implementation task name")
	return cmd
}

func newMonitorImplementationJobsGetCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "get <job-id>", Short: "Get a repository monitor implementation job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/implementation-jobs/"+url.PathEscape(args[0]), nil, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorMutationsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "mutations", Short: "Inspect controller-owned GitHub mutation records"}
	cmd.AddCommand(newMonitorMutationsListCmd())
	cmd.AddCommand(newMonitorMutationsGetCmd())
	return cmd
}

//nolint:dupl
func newMonitorMutationsListCmd() *cobra.Command {
	var limit int
	var cursor, kind, operation, status string
	var number int64
	cmd := &cobra.Command{Use: "list <name>", Short: "List repository monitor GitHub mutation records", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		q := mergeQuery(map[string]string{}, "name", args[0], "limit", fmt.Sprintf("%d", limit), "cursor", cursor, "continue", cursor, "kind", kind, "operation", operation, "status", status)
		if number > 0 {
			q["number"] = fmt.Sprintf("%d", number)
		}
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/mutations", q, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputTable)
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Cursor token")
	cmd.Flags().StringVar(&cursor, "continue", "", "Continue token")
	cmd.Flags().StringVar(&kind, "kind", "", "Filter by target kind")
	cmd.Flags().Int64Var(&number, "number", 0, "Filter by target number")
	cmd.Flags().StringVar(&operation, "operation", "", "Filter by GitHub operation")
	cmd.Flags().StringVar(&status, "status", "", "Filter by status")
	return cmd
}

func newMonitorMutationsGetCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "get <mutation-id>", Short: "Get a repository monitor GitHub mutation record", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/mutations/"+url.PathEscape(args[0]), nil, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorIssueWorkflowCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "issue", Short: "Control a repository monitor issue workflow"}
	for _, spec := range []struct{ use, short, intent string }{
		{"triage <name> <number>", "Queue issue triage", "triage"},
		{"research <name> <number>", "Queue issue research", "research"},
		{"plan <name> <number>", "Queue issue planning", "plan"},
		{"approve-plan <name> <number>", "Approve the current issue plan", "approve_plan"},
		{"implement <name> <number>", "Queue issue implementation", "implement"},
		{"decompose <name> <number>", "Queue issue decomposition", "decompose"},
		{"stop <name> <number>", "Stop issue automation", "stop"},
		{"resume <name> <number>", "Resume issue automation", "resume"},
	} {
		cmd.AddCommand(newMonitorCommandIntentCmd(spec.use, spec.short, "issue", spec.intent))
	}
	cmd.AddCommand(newMonitorIssueStatusCmd())
	cmd.AddCommand(newMonitorIssueImplementationGetCmd())
	cmd.AddCommand(newMonitorIssuePatchPreviewCmd())
	return cmd
}

func newMonitorCommandIntentCmd(use, short, kind, intent string) *cobra.Command {
	var targetSHA string
	cmd := &cobra.Command{Use: use, Short: short, Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		number, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || number <= 0 {
			return fmt.Errorf("target number must be a positive integer")
		}
		body := repositoryMonitorCommandRequestBody(kind, number, intent, targetSHA)
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodPost, "/api/v1/monitors/repositories/"+url.PathEscape(args[0])+"/commands", nil, body)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	if kind == "pull_request" {
		cmd.Flags().StringVar(&targetSHA, "target-sha", "", "Current pull request head SHA for head-bound commands")
		if intent != "stop" && intent != "resume" {
			_ = cmd.MarkFlagRequired("target-sha")
		}
	}
	return cmd
}

func newMonitorIssueStatusCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "status <name> <number>", Short: "Show issue workflow status", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		q := mergeQuery(map[string]string{}, "kind", "issue", "number", args[1], "limit", "1")
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0])+"/items", q, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorIssueImplementationGetCmd() *cobra.Command {
	parent := &cobra.Command{Use: "implementation", Short: "Inspect issue implementation jobs"}
	cmd := &cobra.Command{
		Use:   "get <name> <number>",
		Short: "Show latest implementation jobs for an issue",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(map[string]string{}, "name", args[0], "issueNumber", args[1], "limit", "5")
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/implementation-jobs", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(cmd, outputYAML)
	parent.AddCommand(cmd)
	return parent
}

func newMonitorIssuePatchPreviewCmd() *cobra.Command {
	parent := &cobra.Command{Use: "patch", Short: "Inspect issue patch artifacts"}
	cmd := &cobra.Command{
		Use:   "preview <name> <number>",
		Short: "Show safe patch artifact metadata for an issue",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(map[string]string{}, "name", args[0], "issueNumber", args[1], "limit", "1")
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/implementation-jobs", q, nil)
			if err != nil {
				return err
			}
			jobID, err := monitorFirstListItemID(result)
			if err != nil {
				return err
			}
			preview, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/implementation-jobs/"+url.PathEscape(jobID)+"/patch-preview", nil, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, preview)
		},
	}
	addOutputFlag(cmd, outputYAML)
	parent.AddCommand(cmd)
	return parent
}

func monitorFirstListItemID(result any) (string, error) {
	root, ok := result.(map[string]any)
	if !ok {
		return "", fmt.Errorf("unexpected list response shape")
	}
	items, ok := root["items"].([]any)
	if !ok || len(items) == 0 {
		return "", fmt.Errorf("no implementation jobs found")
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		return "", fmt.Errorf("unexpected implementation job shape")
	}
	id, _ := item["id"].(string)
	if id == "" {
		return "", fmt.Errorf("implementation job is missing id")
	}
	return id, nil
}

func newMonitorPRWorkflowCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "pr", Short: "Control a repository monitor pull request workflow"}
	for _, spec := range []struct{ use, short, intent string }{
		{"review <name> <number>", "Queue exact-head PR review", "review"},
		{"fix <name> <number>", "Queue PR finding repair", "fix"},
		{"fix-ci <name> <number>", "Queue PR CI repair", "fix_ci"},
		{"update-branch <name> <number>", "Queue PR branch update", "update_branch"},
		{"automerge <name> <number>", "Request head-bound automerge", "automerge"},
		{"stop <name> <number>", "Stop PR automation", "stop"},
		{"resume <name> <number>", "Resume PR automation", "resume"},
	} {
		cmd.AddCommand(newMonitorCommandIntentCmd(spec.use, spec.short, "pull_request", spec.intent))
	}
	cmd.AddCommand(newMonitorPRStatusCmd())
	cmd.AddCommand(newMonitorPRRepairsCmd())
	cmd.AddCommand(newMonitorPRReadyCmd())
	return cmd
}

func newMonitorPRStatusCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "status <name> <number>", Short: "Show PR workflow status", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		q := mergeQuery(map[string]string{}, "kind", "pull_request", "number", args[1], "limit", "1")
		c := newClientFromCmd(cmd)
		result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0])+"/items", q, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, result)
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

func newMonitorPRRepairsCmd() *cobra.Command {
	parent := &cobra.Command{Use: "repairs", Short: "Inspect PR repair jobs"}
	cmd := &cobra.Command{
		Use:   "list <name> <number>",
		Short: "List repair jobs for a PR",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			filtered := []any{}
			cursor := ""
			for {
				q := mergeQuery(map[string]string{}, "name", args[0], "kind", "pull_request", "number", args[1], "limit", "100", "continue", cursor)
				result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/work-actions", q, nil)
				if err != nil {
					return err
				}
				body, ok := result.(map[string]any)
				if !ok {
					return fmt.Errorf("unexpected work-action response shape")
				}
				items, _ := body["items"].([]any)
				for _, raw := range items {
					item, _ := raw.(map[string]any)
					switch fmt.Sprint(item["desiredAction"]) {
					case "repair", "fix_ci", "update_branch":
						filtered = append(filtered, raw)
					}
				}
				metadata, _ := body["metadata"].(map[string]any)
				cursor = strings.TrimSpace(fmt.Sprint(metadata["continue"]))
				if cursor == "" || cursor == "<nil>" {
					break
				}
			}
			return printStructured(cmd, map[string]any{"items": filtered, "metadata": map[string]any{"continue": ""}})
		},
	}
	addOutputFlag(cmd, outputTable)
	parent.AddCommand(cmd)
	return parent
}

func newMonitorPRReadyCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "ready", Short: "Inspect merge-ready PRs"}
	listCmd := &cobra.Command{
		Use:   "list <name>",
		Short: "List merge-ready pull requests",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(map[string]string{}, "kind", "pull_request", "automergeState", "merge_ready")
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0])+"/items", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(listCmd, outputTable)
	readinessCmd := &cobra.Command{
		Use:   "readiness <name> <number>",
		Short: "Show readiness state for a pull request",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			q := mergeQuery(map[string]string{}, "kind", "pull_request", "number", args[1], "limit", "1")
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0])+"/items", q, nil)
			if err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(readinessCmd, outputYAML)
	cmd.AddCommand(listCmd, readinessCmd)
	return cmd
}

func validateMonitorTriggerLabels(result any) error {
	body, ok := result.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected monitor response shape")
	}
	spec, _ := body["spec"].(map[string]any)
	triggers, _ := spec["triggers"].(map[string]any)
	github, _ := triggers["github"].(map[string]any)
	labels, _ := github["labels"].(map[string]any)
	groups := map[string][]struct{ field, intent string }{
		"issues":       {{"triage", "triage"}, {"research", "research"}, {"plan", "plan"}, {"approvePlan", "approve_plan"}, {"implement", "implement"}, {"decompose", "decompose"}, {"stop", "stop"}, {"resume", "resume"}},
		"pullRequests": {{"review", "review"}, {"fix", "fix"}, {"fixCI", "fix_ci"}, {"updateBranch", "update_branch"}, {"automerge", "automerge"}, {"stop", "stop"}, {"resume", "resume"}},
	}
	for groupName, entries := range groups {
		seen := map[string]string{}
		group, _ := labels[groupName].(map[string]any)
		for _, entry := range entries {
			configured, _ := group[entry.field].(string)
			label := strings.ToLower(strings.TrimSpace(configured))
			if label == "" {
				label = defaultMonitorCommandLabel(entry.intent)
			}
			key := groupName + "." + entry.field
			if previous := seen[label]; previous != "" {
				return fmt.Errorf("trigger label %q is configured for both %s and %s", label, previous, key)
			}
			seen[label] = key
		}
	}
	return nil
}

func defaultMonitorCommandLabel(intent string) string {
	switch intent {
	case "approve_plan":
		return "orka:approve-plan"
	case "fix_ci":
		return "orka:fix-ci"
	case "update_branch":
		return "orka:update-branch"
	case "decompose":
		return "orka:to-issues"
	default:
		return "orka:" + strings.ReplaceAll(intent, "_", "-")
	}
}

func newMonitorDoctorCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "doctor <name>", Short: "Summarize monitor workflow health", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c := newClientFromCmd(cmd)
		monitor, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0]), nil, nil)
		if err != nil {
			return err
		}
		return printStructured(cmd, monitorDoctorSummary(monitor))
	}}
	addOutputFlag(cmd, outputYAML)
	return cmd
}

const (
	monitorSummaryKeyName     = "name"
	monitorSummaryKeyMetadata = "metadata"
	monitorSummaryKeyStatus   = "status"
	monitorSummaryKeyType     = "type"
)

// monitorDoctorSummary reduces a RepositoryMonitor resource to the fields an
// operator needs to judge workflow health: identity, repository, phase, run
// timestamps, inventory counts, and conditions. The full resource (including
// managed fields) remains available through "orka monitor get".
func monitorDoctorSummary(monitor any) map[string]any {
	root, _ := monitor.(map[string]any)
	if root == nil {
		return map[string]any{}
	}
	spec, _ := root["spec"].(map[string]any)
	status, _ := root[monitorSummaryKeyStatus].(map[string]any)
	summary := map[string]any{
		monitorSummaryKeyName: nestedString(root, monitorSummaryKeyMetadata, monitorSummaryKeyName),
		cliNamespaceQuery:     nestedString(root, monitorSummaryKeyMetadata, cliNamespaceQuery),
	}
	if spec != nil {
		summary["repository"] = firstString(spec, "repoURL", "repositoryURL", "repository")
		summary["branch"] = firstString(spec, "branch")
	}
	if status != nil {
		for _, key := range []string{"phase", "lastRunID", "lastRunTime", "lastSuccessfulRunTime", "observedGeneration",
			"openIssues", "openPullRequests", "blockedIssues", "blockedItems",
			"pendingReviews", "activeRepairs", "mergeReadyItems", "pendingIssueActions"} {
			if value, ok := status[key]; ok {
				summary[key] = value
			}
		}
		if conditions, ok := status["conditions"].([]any); ok {
			trimmed := make([]map[string]any, 0, len(conditions))
			for _, raw := range conditions {
				condition, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				entry := map[string]any{}
				for _, key := range []string{monitorSummaryKeyType, monitorSummaryKeyStatus, "reason", "message", "lastTransitionTime"} {
					if value, ok := condition[key]; ok {
						entry[key] = value
					}
				}
				trimmed = append(trimmed, entry)
			}
			summary["conditions"] = trimmed
		}
	}
	return summary
}

func newMonitorWatchCmd() *cobra.Command {
	var interval time.Duration
	cmd := &cobra.Command{Use: "watch <name>", Short: "Watch monitor status", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if interval <= 0 {
			return fmt.Errorf("watch interval must be greater than zero")
		}
		c := newClientFromCmd(cmd)
		for {
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0]), nil, nil)
			if err != nil {
				return err
			}
			if err := printStructured(cmd, result); err != nil {
				return err
			}
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(interval):
			}
		}
	}}
	addOutputFlag(cmd, outputYAML)
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Watch refresh interval")
	return cmd
}

func newMonitorTriggerLabelsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "trigger-labels", Short: "Validate monitor label trigger configuration"}
	validateCmd := &cobra.Command{
		Use:   "validate <name>",
		Short: "Validate monitor label trigger configuration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClientFromCmd(cmd)
			result, err := c.DoJSON(context.Background(), http.MethodGet, "/api/v1/monitors/repositories/"+url.PathEscape(args[0]), nil, nil)
			if err != nil {
				return err
			}
			if err := validateMonitorTriggerLabels(result); err != nil {
				return err
			}
			return printStructured(cmd, result)
		},
	}
	addOutputFlag(validateCmd, outputYAML)
	cmd.AddCommand(validateCmd)
	return cmd
}
