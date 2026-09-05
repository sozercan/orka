---
slug: /multi-agent-coordination
description: "How one agent delegates to another: tool schemas, controller guardrails, and the child task lifecycle."
---

# Multi-agent coordination

Coordinator agents delegate subtasks to specialist agents at runtime. The LLM decides
what to delegate and how to combine results. The controller enforces allowed agents,
depth, and concurrency limits. Each delegation creates a child Task. Native AI children
run in worker Jobs; children targeting built-in ACP runtimes use RuntimeSessions and
RuntimePools.

## Workflow

This example uses a native `type: ai` coordinator. Its children can target native AI
Agents or built-in ACP runtime Agents.

```
User creates Task (agentRef: coordinator-agent, prompt: "Refactor auth")
  │
  ▼
Controller: Pending → resolves Agent → coordination.enabled=true → creates Job
  │
  ▼
AI Worker pod starts, LLM receives system prompt + delegate_task/wait_for_tasks tools
  │
  ▼
LLM calls delegate_task(agent: "backend-dev", prompt: "Refactor API layer")
LLM calls delegate_task(agent: "frontend-dev", prompt: "Update UI auth")
  │
  ▼
Worker creates child Tasks via K8s API with:
  - ownerRef → parent Task
  - label: orka.ai/parent-task = <parent>
  - label: orka.ai/delegated-agent = <agent>
  - annotation: orka.ai/coordination-depth = <parent_depth + 1>
  │
  ▼
Controller reconciles child Tasks:
  - Checks allowedAgents, maxDepth, and maxConcurrentChildren
  - Native AI Agent → type: ai → worker Job
  - Built-in runtime Agent → type: agent → RuntimeSession on a RuntimePool
  - Children run in parallel within the configured limits
  - Updates parent's status.childTasks[]
  │
  ▼
LLM calls wait_for_tasks(tasks: ["child-1", "child-2"])
  - Worker polls child Task status via K8s API
  - Blocks until all reach terminal phase
  - Reads results from SQLite (via controller's internal API), returns aggregated results
  │
  ▼
LLM synthesizes final answer from child results → writes result via HTTP POST to controller
  │
  ▼
Controller: parent Job succeeded → parent Task → Succeeded
  - ChildTaskStatus fully populated
  - Owner references cascade-delete children when parent is deleted
```

## Components

### Coordination Tools

Located in `internal/tools/`. Coordination tools include `delegate_task`, `wait_for_tasks`, `create_container_task`, `cancel_task`, `send_message`, `check_messages`, memory tools (`recall_memory`, `remember`, `propose_memory`, `search_transcript`), PR tools (`create_pull_request`, `list_pull_requests`, `check_pr_review_marker`, `check_pull_request_ci`, `merge_pull_request`, `auto_merge_pull_request`, `review_pull_request`, `post_review_comment`), issue tools (`list_issues`, `get_issue`, `comment_on_issue`), agent management tools (`create_agent`, `delete_agent`), and `update_plan` (autonomous mode). These are registered via `RegisterCoordinationTools(k8sClient)` in `internal/tools/registry.go` when `ORKA_COORDINATION_ENABLED=true`. The `create_pr_monitor` compatibility tool is exposed through chat/management tooling and creates a scheduled monitor Task with its own narrow PR review tool set.

#### `delegate_task` Tool

LLM-visible parameter schema:
```json
{
  "type": "object",
  "properties": {
    "agent":       {"type": "string", "description": "Name of the agent to delegate to"},
    "prompt":      {"type": "string", "description": "The task prompt for the agent"},
    "namespace":   {"type": "string", "description": "Namespace (defaults to current)"},
    "priority":    {"type": "integer", "description": "Priority 0-1000 (defaults to parent priority)"},
    "workspace":   {"type": "object", "description": "Git workspace configuration for agent runtime tasks",
      "properties": {
        "intent":                   {"type": "string", "enum": ["read", "write"]},
        "gitRepo":                  {"type": "string", "description": "Credential-free source repository URL"},
        "branch":                   {"type": "string", "description": "Source branch name"},
        "ref":                      {"type": "string", "description": "Source ref, commit SHA, or tag"},
        "readCredentialRef":        {"type": "string", "description": "Secret used only for clean-room clone/read"},
        "publicationGitRepo":       {"type": "string", "description": "Credential-free publication repository URL"},
        "publicationCredentialRef": {"type": "string", "description": "Secret used only by the Workspace/Publisher"},
        "pushBranch":               {"type": "string", "description": "Publication branch"},
        "prBaseBranch":             {"type": "string", "description": "Pull-request base branch"},
        "createPR":                 {"type": "boolean", "description": "Request PR reconciliation after verified publication"}
      }
    },
    "maxTurns":    {"type": "integer", "description": "Maximum number of turns for the agent"},
    "allowBash":   {"type": "boolean", "description": "Whether to allow bash execution in the agent"},
    "prior_task":  {"type": "string", "description": "Name of a previously completed task whose diff should be applied to the workspace before this task starts. Used for iterative workflows."},
    "feedback":    {"type": "string", "description": "Review feedback to prepend to the task prompt. Used with prior_task for iterative code review workflows."},
    "auto_retry":  {"type": "boolean", "description": "Include structured retry metadata in failure reports. The coordinator decides whether to retry — wait_for_tasks does not auto-retry."},
    "max_retries": {"type": "integer", "description": "Maximum retry budget for coordinator reference (default: 2). Only used when auto_retry is true."}
  },
  "required": ["agent", "prompt"]
}
```

Implementation (`internal/tools/delegate_task.go`):
1. Reads `ORKA_TASK_NAME`, `ORKA_TASK_NAMESPACE`, `ORKA_COORDINATION_DEPTH` from env
2. Reads `ORKA_COORDINATION_ALLOWED_AGENTS` and validates the target agent is allowed
3. Checks depth + 1 does not exceed `ORKA_COORDINATION_MAX_DEPTH`
4. Creates child Task via K8s API with:
   - `GenerateName: {parentName}-child-`
   - `Labels: orka.ai/parent-task, orka.ai/coordinator, orka.ai/delegated-agent`
   - `Annotations: orka.ai/coordination-depth`
   - `OwnerReferences` pointing to parent Task
   - `Spec.AgentRef.Name: <agent>`, `Spec.Prompt: <prompt>`
   - `Spec.Type: agent` when the target Agent has `spec.runtime`, otherwise `ai`
5. If `auto_retry: true`, stores retry config as annotations: `orka.ai/auto-retry`, `orka.ai/max-retries`, `orka.ai/retry-count`, `orka.ai/original-prompt`
6. Inherits safe transaction metadata from the parent Task. When `ORKA_CONTEXT_TOKEN_TTS_ENDPOINT`, `ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE`, and `ORKA_CONTEXT_TOKEN_CHILD_SCOPE` are set in the worker, exchanges the mounted subject token for a child TxToken whose scope must be a subset of the parent transaction scopes. The raw child token is stored only in an owner-referenced Secret and mounted into the child worker.
7. Returns `{"taskName": "<name>", "status": "created"}` to LLM

### TxToken-aware delegation

When a coordinator Task has transaction metadata, delegated Tasks inherit the same safe transaction identity (`spec.transaction`, transaction labels, and annotations) so parent and child work can be audited by transaction ID. If worker-side transaction-token TTS exchange is configured, `delegate_task` and `create_container_task` request a narrower child TxToken before creating the child Task.

Required worker environment for child-token exchange:

```bash
ORKA_CONTEXT_TOKEN_TTS_ENDPOINT=https://transaction-token-tts.transaction-token-system.svc/oauth/token
ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE=/var/run/orka/transaction-token/token
ORKA_CONTEXT_TOKEN_CHILD_SCOPE=orka:agents:run
```

The requested child scope must already be present in the parent transaction scopes; broadening attempts fail before the child Task is created. The returned raw TxToken is stored in a short-lived, owner-referenced Secret and mounted into the child worker as `ORKA_TRANSACTION_TOKEN_FILE` / `ORKA_CONTEXT_TOKEN_SUBJECT_TOKEN_FILE`.

#### `wait_for_tasks` Tool

LLM-visible parameter schema:
```json
{
  "type": "object",
  "properties": {
    "tasks":   {"type": "array", "items": {"type": "string"}, "description": "Child task names to wait for"},
    "timeout": {"type": "string", "description": "Max wait duration, e.g. '5m' (default: '10m')"}
  },
  "required": ["tasks"]
}
```

Implementation (`internal/tools/wait_for_tasks.go`):
1. Parses timeout (default 10m)
2. Poll loop (5s interval):
   - Gets each child Task by name via K8s API
   - Checks if all are in a terminal phase (Succeeded, Failed, or Cancelled)
   - Returns failed children with retry metadata when `orka.ai/auto-retry=true`; the coordinator decides whether to submit another Task
   - If timeout exceeded, returns partial results with timeout flag
   - Respects context cancellation
3. For each completed child, reads result via the controller's result API
4. Returns aggregated JSON:
   ```json
   {
     "completed": true,
     "results": [
       {"task": "name", "agent": "agent", "phase": "Succeeded", "result": "..."},
       {"task": "name", "agent": "agent", "phase": "Failed", "result": "error: ..."}
     ]
   }
   ```

#### `cancel_task` Tool

LLM-visible parameter schema:
```json
{
  "type": "object",
  "properties": {
    "task_name":  {"type": "string", "description": "Name of the child task to cancel"},
    "namespace":  {"type": "string", "description": "Namespace (defaults to current)"},
    "reason":     {"type": "string", "description": "Reason for cancellation"}
  },
  "required": ["task_name"]
}
```

Implementation (`internal/tools/cancel_task.go`):
1. Reads `ORKA_TASK_NAME`, `ORKA_TASK_NAMESPACE` from env
2. Validates the target task is a child of the calling task via `orka.ai/parent-task` label
3. Only cancels tasks in `Pending` or `Running` phase
4. Sets the task phase to `Cancelled` via status subresource update
5. Returns confirmation with task name and cancellation reason

#### `send_message` Tool

LLM-visible parameter schema:
```json
{
  "type": "object",
  "properties": {
    "to_task": {"type": "string", "description": "Name of the sibling task to message, or \"*\" to broadcast"},
    "content": {"type": "string", "description": "Message content to send"}
  },
  "required": ["to_task", "content"]
}
```

Implementation (`internal/tools/send_message.go`):
1. Reads `ORKA_TASK_NAME`, `ORKA_TASK_NAMESPACE`, `ORKA_PARENT_TASK`, `ORKA_CONTROLLER_URL` from env
2. Posts message to controller's internal messaging API
3. Messages are scoped to siblings (same parent task) — agents can only message tasks that share the same coordinator
4. Use `to_task="*"` to broadcast to all siblings

#### `check_messages` Tool

LLM-visible parameter schema:
```json
{
  "type": "object",
  "properties": {
    "mark_read": {"type": "boolean", "description": "Whether to mark messages as read (default: true)"}
  }
}
```

Implementation (`internal/tools/check_messages.go`):
1. Reads `ORKA_TASK_NAME`, `ORKA_TASK_NAMESPACE`, `ORKA_PARENT_TASK`, `ORKA_CONTROLLER_URL` from env
2. Fetches unread messages from controller's internal messaging API
3. Returns all unread messages addressed to this task or broadcast to all siblings (same parent)
4. Messages are marked as read by default to avoid re-delivery

### Inter-agent messaging

Sibling tasks (children of the same coordinator) can exchange messages to coordinate work, share findings, and avoid duplicated effort. Messages flow through the controller's internal API — no direct pod-to-pod communication.

**Architecture:**
- Messages stored in SQLite via `MessageStore` interface (`internal/store/store.go`)
- Internal endpoints: `POST /internal/v1/messages/{namespace}` and `GET /internal/v1/messages/{namespace}/{taskName}`
- Scoped to siblings only: messages filtered by `parent_task` column
- Broadcast: `to_task="*"` sends to all siblings (sender excluded from own broadcasts)
- Cleanup: messages are automatically deleted when tasks or parent coordinators are deleted

**Environment Variables:**
- `ORKA_PARENT_TASK`: Set automatically by the job builder for child tasks (from `orka.ai/parent-task` label)
- `ORKA_CONTROLLER_URL`, `ORKA_TASK_NAME`, `ORKA_TASK_NAMESPACE`: Already available for all workers

### Controller enforcement

Located in `internal/controller/task_controller.go`.

#### `handlePending` — coordination validation

After agent resolution, before dispatch, the controller validates coordination constraints for child tasks (identified by `orka.ai/coordination-depth` annotation):

```go
if depthStr := task.Annotations["orka.ai/coordination-depth"]; depthStr != "" {
    parentName := task.Labels["orka.ai/parent-task"]
    depthInt, _ := strconv.Atoi(depthStr)

    // Look up parent task to find its agent's coordination config
    parentTask := &corev1alpha1.Task{}
    r.Get(ctx, types.NamespacedName{Name: parentName, Namespace: task.Namespace}, parentTask)

    parentAgent := &corev1alpha1.Agent{}
    r.Get(ctx, types.NamespacedName{Name: parentTask.Spec.AgentRef.Name, Namespace: task.Namespace}, parentAgent)

    coord := parentAgent.Spec.Coordination
    if coord == nil || !coord.Enabled {
        return r.failTask(ctx, task, "parent agent does not have coordination enabled")
    }

    // 1. Enforce maxDepth
    if int32(depthInt) > coord.MaxDepth {
        return r.failTask(ctx, task, fmt.Sprintf("coordination depth %d exceeds max %d", depthInt, coord.MaxDepth))
    }

    // 2. Enforce allowedAgents
    allowed := false
    for _, a := range coord.AllowedAgents {
        if a.Name == task.Spec.AgentRef.Name {
            allowed = true
            break
        }
    }
    if !allowed {
        return r.failTask(ctx, task, fmt.Sprintf("agent %q not in parent's allowedAgents", task.Spec.AgentRef.Name))
    }

    // 3. Enforce maxConcurrentChildren (requeue if at limit)
    var siblings corev1alpha1.TaskList
    r.List(ctx, &siblings, client.InNamespace(task.Namespace),
        client.MatchingLabels{"orka.ai/parent-task": parentName})
    active := 0
    for _, s := range siblings.Items {
        if s.Status.Phase == corev1alpha1.TaskPhasePending || s.Status.Phase == corev1alpha1.TaskPhaseRunning {
            active++
        }
    }
    if int32(active) >= coord.MaxConcurrentChildren {
        return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
    }
}
```

#### `handleRunning` — ChildTaskStatus population

For coordinator tasks (those without a `orka.ai/parent-task` label), the controller lists child tasks and populates `status.childTasks[]` with name, agent, phase, and truncated result (max 500 chars):

```go
if _, hasChildren := task.Labels["orka.ai/parent-task"]; !hasChildren {
    // This task might be a coordinator — check for children
    var children corev1alpha1.TaskList
    if err := r.List(ctx, &children, client.InNamespace(task.Namespace),
        client.MatchingLabels{"orka.ai/parent-task": task.Name}); err == nil && len(children.Items) > 0 {
        task.Status.ChildTasks = make([]corev1alpha1.ChildTaskStatus, 0, len(children.Items))
        for _, child := range children.Items {
            cs := corev1alpha1.ChildTaskStatus{
                Name:  child.Name,
                Phase: child.Status.Phase,
            }
            if child.Spec.AgentRef != nil {
                cs.Agent = child.Spec.AgentRef.Name
            }
            // Include truncated result summary from SQLite store
            if child.Status.Phase == corev1alpha1.TaskPhaseSucceeded {
                data, err := r.resultStore.GetResult(ctx, task.Namespace, child.Name)
                if err == nil {
                    result := string(data)
                    if len(result) > 500 {
                        result = result[:500] + "..."
                    }
                    cs.Result = result
                }
            }
            task.Status.ChildTasks = append(task.Status.ChildTasks, cs)
        }
    }
}
```

### Job builder — coordination config

Located in `internal/controller/job_builder.go`. In `addAIEnvVars`, when an agent has coordination enabled, the following env vars are injected:

```go
if agent != nil && agent.Spec.Coordination != nil && agent.Spec.Coordination.Enabled {
    envVars = append(envVars,
        corev1.EnvVar{Name: "ORKA_COORDINATION_ENABLED", Value: "true"},
        corev1.EnvVar{Name: "ORKA_COORDINATION_MAX_DEPTH",
            Value: fmt.Sprintf("%d", agent.Spec.Coordination.MaxDepth)},
        corev1.EnvVar{Name: "ORKA_COORDINATION_MAX_CHILDREN",
            Value: fmt.Sprintf("%d", agent.Spec.Coordination.MaxConcurrentChildren)},
    )

    var agentNames []string
    for _, a := range agent.Spec.Coordination.AllowedAgents {
        agentNames = append(agentNames, a.Name)
    }
    envVars = append(envVars,
        corev1.EnvVar{Name: "ORKA_COORDINATION_ALLOWED_AGENTS",
            Value: strings.Join(agentNames, ",")},
    )

    // Current depth (0 for top-level coordinator)
    depth := "0"
    if d, ok := task.Annotations["orka.ai/coordination-depth"]; ok {
        depth = d
    }
    envVars = append(envVars,
        corev1.EnvVar{Name: "ORKA_COORDINATION_DEPTH", Value: depth},
    )
}
```

Coordination, memory, PR, dynamic-agent, and plan tools are automatically injected into the tools list when coordination is enabled. This includes `delegate_task`, `wait_for_tasks`, `create_container_task`, sibling messaging tools, memory tools (`recall_memory`, `remember`, `propose_memory`, `search_transcript`), PR workflow tools, dynamic agent management, and `update_plan`.

### AI worker wiring

Located in `workers/ai/main.go`. When `ORKA_COORDINATION_ENABLED=true`:

- Coordination tools are registered into `DefaultRegistry` via `tools.RegisterCoordinationTools(k8sClient)`
- Memory tools are registered when controller context is present and can be used for durable recall, proposal creation, and transcript search
- `maxIterations` is increased from 10 to 50 for coordinator agents

## RBAC

Worker pods use trust-tiered ServiceAccounts and ClusterRoles defined in `config/rbac/worker_role.yaml`. The raw RBAC manifests use unprefixed names (`ai-worker`, `ai-worker-role`, etc.); the default install renders them through `config/default`, whose kustomize `namePrefix: orka-` produces the `orka-*` names shown below:

- AI tasks use `orka-ai-worker` / `orka-ai-worker-role`.
- Agent-provider tasks use `orka-vendor-worker` / `orka-vendor-worker-role`.
- Container tasks use `orka-container-worker` / `orka-container-worker-role`.

The controller ensures the worker ServiceAccounts and namespace-specific ClusterRoleBindings exist in each task namespace. General Orka worker images mount the ServiceAccount token so they can submit authenticated results to the controller; custom container images run with token automount disabled and are collected from pod logs instead.

The worker ClusterRoles include the coordination permissions needed to delegate tasks and manage agents:

```yaml
- apiGroups: ["core.orka.ai"]
  resources: ["tasks"]
  verbs: ["get", "list", "watch", "create"]
- apiGroups: ["core.orka.ai"]
  resources: ["agents"]
  verbs: ["get", "create", "update", "delete"]
```

## Example YAML

### Coordinator Agent
```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: project-coordinator
spec:
  providerRef:
    name: anthropic-provider
  model:
    name: claude-sonnet-4-20250514
  systemPrompt:
    inline: |
      You are a project coordinator. Your job is to break complex tasks into
      subtasks and delegate them to specialist agents using the delegate_task
      tool. Once all subtasks complete, use wait_for_tasks to collect results
      and synthesize a final summary.

      Available specialist agents and their capabilities will be provided.
      Delegate appropriately based on the task requirements.
  coordination:
    enabled: true
    maxConcurrentChildren: 3
    maxDepth: 2
    allowedAgents:
      - name: backend-dev
      - name: frontend-dev
      - name: test-writer
  secretRef:
    name: llm-credentials
```

### Specialist Agents
```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: backend-dev
spec:
  providerRef:
    name: anthropic-provider
  model:
    name: claude-sonnet-4-20250514
  systemPrompt:
    inline: "You are a backend developer specializing in Go and Kubernetes."
  tools:
    - name: code_exec
    - name: file_read
  secretRef:
    name: llm-credentials
---
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: frontend-dev
spec:
  providerRef:
    name: anthropic-provider
  model:
    name: claude-sonnet-4-20250514
  systemPrompt:
    inline: "You are a frontend developer specializing in React and TypeScript."
  tools:
    - name: code_exec
    - name: file_read
  secretRef:
    name: llm-credentials
```

### Coordination Task
```yaml
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: refactor-auth
spec:
  type: ai
  agentRef:
    name: project-coordinator
  prompt: |
    Refactor the authentication module for better security and testability.
    Break this into backend API changes and frontend UI updates.
    Ensure all changes are tested.
  timeout: 30m
  priority: 800
```

### Expected Task status (while running)
```yaml
status:
  phase: Running
  startTime: "2026-02-06T10:00:00Z"
  jobName: refactor-auth-job-abc12345
  childTasks:
    - name: refactor-auth-child-xyz1
      agent: backend-dev
      phase: Running
    - name: refactor-auth-child-xyz2
      agent: frontend-dev
      phase: Succeeded
      result: "Updated 12 components to use new auth context..."
```

## File summary

| File | Description |
|---|---|
| `internal/tools/delegate_task.go` | delegate_task tool implementation |
| `internal/tools/delegate_task_test.go` | Unit tests with fake K8s client |
| `internal/tools/wait_for_tasks.go` | wait_for_tasks tool implementation |
| `internal/tools/wait_for_tasks_test.go` | Unit tests with fake K8s client |
| `internal/tools/create_pull_request.go` | create_pull_request tool implementation |
| `internal/tools/create_pull_request_test.go` | Unit tests with fake K8s client |
| `internal/tools/check_pull_request_ci.go` | check_pull_request_ci tool implementation |
| `internal/tools/check_pull_request_ci_test.go` | Unit tests with fake K8s client |
| `internal/tools/merge_pull_request.go` | merge_pull_request tool implementation |
| `internal/tools/merge_pull_request_test.go` | Unit tests with fake K8s client |
| `internal/tools/auto_merge_pull_request.go` | auto_merge_pull_request tool implementation |
| `internal/tools/auto_merge_pull_request_test.go` | Unit tests with fake K8s client |
| `internal/tools/review_pull_request.go` | review_pull_request tool implementation |
| `internal/tools/review_pull_request_test.go` | Unit tests with fake K8s client |
| `internal/tools/post_review_comment.go` | post_review_comment tool implementation |
| `internal/tools/post_review_comment_test.go` | Unit tests with fake K8s client |
| `internal/tools/create_agent.go` | create_agent tool implementation |
| `internal/tools/create_agent_test.go` | Unit tests with fake K8s client |
| `internal/tools/delete_agent.go` | delete_agent tool implementation |
| `internal/tools/delete_agent_test.go` | Unit tests with fake K8s client |
| `internal/tools/registry.go` | `RegisterCoordinationTools()` function |
| `internal/controller/task_controller.go` | Coordination validation + ChildTaskStatus population |
| `internal/controller/task_controller_test.go` | Tests for coordination enforcement |
| `internal/controller/job_builder.go` | Coordination env vars for worker pods |
| `internal/controller/job_builder_test.go` | Tests for coordination env vars |
| `workers/ai/main.go` | Register coordination and memory tools, increase maxIterations |
| `config/rbac/worker_role.yaml` | Task create + ConfigMap list permissions |

## Testing

1. **Unit tests** for `delegate_task` and `wait_for_tasks` tools using `sigs.k8s.io/controller-runtime/pkg/client/fake`
2. **Controller tests** for coordination validation (depth, allowedAgents, concurrency) using envtest
3. **Job builder tests** verifying coordination env vars and auto-injected memory tools are set correctly

## Retry decisions

`auto_retry: true` enables structured failure metadata. `wait_for_tasks` returns the
failed child with `phase: Failed`; it does not create a replacement Task or advance
the retry count.

The coordinator must explicitly call `delegate_task` again to retry, choosing any
prompt changes and tracking its attempt budget. `max_retries` defaults to 2 and is
metadata for that decision; the tools do not enforce an automatic retry loop. Each
new delegation with `auto_retry: true` starts with `retry-count: "0"`.

### Example

```json
{
  "agent": "coder",
  "prompt": "Implement the feature",
  "auto_retry": true,
  "max_retries": 3
}
```

### Result format

For the request above, a failed child's result includes:
```json
{
  "task": "parent-task-child-abc",
  "phase": "Failed",
  "failureDetails": {
    "message": "out of memory",
    "retryCount": 0,
    "maxRetries": 3
  }
}
```

## Iterative code review workflows

Orka supports iterative multi-agent workflows where a coordinator orchestrates coding, review, and feedback loops until code is approved.

The current ACP core runtime does not accept `priorTaskRef`; use `sessionRef` for runtime continuity in direct ACP manifests. Existing `prior_task` coordination examples describe the older diff-handoff loop and are not an ACP v2 release gate.

### Flow

```
COORDINATOR (AI worker)
  │
  ├── delegate_task(agent="coder", prompt="Implement feature",
  │                 workspace={intent: "write",
  │                            gitRepo: "https://github.com/example/repo.git",
  │                            readCredentialRef: "repository-read",
  │                            publicationGitRepo: "https://github.com/example/repo.git",
  │                            publicationCredentialRef: "repository-publish",
  │                            pushBranch: "feature/my-change"})
  │     Workspace boundary clones; coder edits only; Orka validates and
  │     clean-room publishes the claimed branch with a delivery receipt
  │
  ├── wait_for_tasks → gets {summary, files, pushBranch} (diff stripped)
  │
  ├── delegate_task(agent="reviewer", prompt="Review changes",
  │                 prior_task="coder-task-xyz")
  │     Reviewer: clones repo → applies coder's diff → reviews
  │               → result: {verdict: "CHANGES_NEEDED", feedback: "Add tests"}
  │
  ├── wait_for_tasks → gets {verdict, feedback}
  │
  ├── delegate_task(agent="coder", prompt="Fix: add tests",
  │                 prior_task="coder-task-xyz", feedback="Add tests")
  │     Coder iter 2: receives the reviewed workspace context and fixes files;
  │                   Orka owns the next publication attempt
  │
  ├── (review loop repeats until APPROVED or max iterations)
  │
  ├── create_pull_request(task_name="coder-task-xyz",
  │                       head_branch="feature/my-change",
  │                       base_branch="main", title="feat: my change")
  │     → creates PR via GitHub API using workspace.publicationCredentialRef
  │
  ├── review_pull_request(task_name="coder-task-xyz", pr_number=42)
  │     → fetches PR diff and file changes for analysis
  │
  ├── post_review_comment(task_name="coder-task-xyz", pr_number=42,
  │                       body="LGTM", event="APPROVE")
  │     → posts review with verdict and optional line comments
  │
  ├── check_pull_request_ci(task_name="coder-task-xyz", pr_number=42)
  │     → checks GitHub CI status without merging
  │
  ├── merge_pull_request(task_name="coder-task-xyz", pr_number=42)
  │     → verifies CI checks pass, then merges the PR (instant, fails if CI not green)
  │
  ├── auto_merge_pull_request(task_name="coder-task-xyz", pr_number=42)
  │     → polls CI checks every 30s and auto-merges when green (blocks up to timeout)
  │
  └── COMPLETE with merged PR
```

### delegate_task iteration parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `prior_task` | string | Name of a previously completed task whose diff to apply before starting |
| `feedback` | string | Review feedback prepended to the prompt as `FEEDBACK FROM REVIEW: ...` |
| `workspace.pushBranch` | string | Publication branch reconciled by the clean-room Workspace/Publisher after validation |

When `prior_task` is set:
- `PriorTaskRef` is set on the child Task, triggering diff application in the worker
- Workspace config is copied from the prior task if not explicitly provided
- Iteration labels are tracked: `orka.ai/iteration` (incremented) and `orka.ai/iteration-group` (shared ID)

### Structured result format

Workers with git workspaces produce structured results:

```json
{
  "version": 1,
  "summary": "Implemented JWT authentication middleware",
  "baseSHA": "abc123def456",
  "diff": "diff --git a/auth.go ...",
  "verdict": "APPROVED",
  "feedback": "Looks good, minor nit on line 42",
  "files": ["auth.go", "middleware.go"],
  "pushBranch": "feature/jwt-auth",
  "metadata": {}
}
```

- `diff` is always **cumulative** (`git diff --binary --full-index` from clean checkout)
- `verdict`: `APPROVED` or `CHANGES_NEEDED`
- `wait_for_tasks` **strips the diff** from results returned to the coordinator to prevent context bloat
- Plain-text results remain backward compatible

### Iteration labels

| Label | Description |
|-------|-------------|
| `orka.ai/iteration` | Iteration number (1, 2, 3...) |
| `orka.ai/iteration-group` | Shared ID grouping all iterations of the same logical task |

### Recommended coordinator system prompt

```
You are a coordinator agent. Follow this protocol:

1. DELEGATE implementation to the coder agent.
2. WAIT for the coder's result.
3. DELEGATE review to the reviewer agent (workspace.intent = read on the
   PUBLISHED head: publicationGitRepo — or gitRepo when publication targets the
   same repository — at branch = pushBranch). The coder's verified changes
   live only on pushBranch; reviewing the original branch would inspect the
   unchanged base. The ACP core runtime does not accept prior_task — the
   reviewer reads the published head rather than replaying a diff.
4. WAIT for the reviewer's verdict.
5. IF verdict == "CHANGES_NEEDED" AND iteration < 3:
   DELEGATE fix to the coder agent again (workspace.intent = write) with the
   reviewer feedback in the prompt.
   Go to step 2.
6. IF verdict == "APPROVED":
   Report final result.
7. Report final result.
```

### ACP workspace and delivery data

ACP children do not receive Git publication environment variables or credentials. The delegated child Task stores source/publication policy in top-level `spec.workspace`, and Orka records the authoritative outcome in `status.delivery`.

Require a terminal verified delivery receipt before creating or merging a pull request. A model result that says files were changed or pushed is not publication evidence.

### create_pull_request Tool

Creates a GitHub pull request from a branch that Orka independently verified for a completed agent Task.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | yes | Name of the completed child Task whose workspace and delivery receipt identify the publication repository/branch |
| `head_branch` | string | yes | Branch containing the changes (the `pushBranch` from the coder task) |
| `base_branch` | string | yes | Target branch to merge into (e.g. `main`) |
| `title` | string | yes | Pull request title |
| `body` | string | no | Pull request body in Markdown |

The tool resolves the child Task's `workspace.publicationCredentialRef` and calls the GitHub API for the verified publication repository. The publication credential is distinct from `readCredentialRef` and is never delivered to the ACP runtime.

### create_pr_monitor Tool

Creates a scheduled prompt-orchestrated pull request monitor Task for one GitHub repository. This is the compatibility path for scheduled AI monitor Tasks; use the `RepositoryMonitor` CRD for durable monitor runs, PR queue state, review records, and dashboard visibility.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | yes | Human-readable monitor name. The created Task receives an Orka-generated name. |
| `repo_url` | string | yes | Credential-free GitHub repository root URL to monitor, such as `https://github.com/owner/repo`, `https://github.com/owner/repo.git`, or `git@github.com:owner/repo.git`. The created Task stores it in `spec.workspace.gitRepo`. |
| `schedule` | string | yes | Cron schedule for the monitor Task. |
| `agent_ref` | string | yes | AI Agent name for the scheduled monitor Task. The Agent must have coordination enabled and autonomous coordination disabled. |
| `namespace` | string | no | Namespace for the monitor Task. Defaults to the current task namespace. |
| `provider_ref` | string | no | Optional Provider reference for the scheduled AI Task. |
| `gitSecretRef` | string | no | Compatibility parameter for the monitor API. The selected Secret is stored on the created Task as `spec.workspace.readCredentialRef`; it is not the deprecated Task workspace field. |
| `per_page` | integer | no | Maximum open PRs to scan per run. Defaults to `30`, maximum `100`. |
| `review_event` | string | no | Review event to post after analysis: `COMMENT`, `APPROVE`, or `REQUEST_CHANGES`. Defaults to `COMMENT`. |
| `prompt` | string | no | Additional instructions appended to the generated monitor prompt. |

Pull request, issue, branch/tree, blob/file, commit, query-string, fragment, non-GitHub, HTTP, and embedded-credential URLs are rejected before Orka creates the monitor Task.

If the monitor tool's `gitSecretRef` parameter is omitted, Orka searches `git-credentials`, `github-credentials`, `copilot-token`, `github-token`, and `git-token`. The selected Secret must exist in the target namespace and contain a non-empty `token`, `password`, or `GITHUB_TOKEN` key; Orka maps it to the Task read-credential role. The created Task receives a narrow tool set: `list_pull_requests`, `check_pr_review_marker`, `check_pull_request_ci`, `review_pull_request`, and `post_review_comment`. The generated prompt tells the Task to pass the same `repo_url` to each PR tool call. Those explicit repository URLs are scope-checked against the Task workspace or signed transaction repository context before Orka resolves credentials or calls GitHub.

### check_pull_request_ci Tool

Checks GitHub CI status for a pull request without merging it. Use this after creating or updating a PR to decide whether the branch is ready or needs a focused repair task.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pr_number` | integer | yes | GitHub pull request number to inspect |
| `task_name` | string | no | Name of the child task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `wait_timeout` | string | no | Maximum time to wait for pending checks, e.g. `30m`. Empty means one immediate check |
| `poll_interval` | string | no | Delay between checks while waiting, e.g. `30s`. Defaults to `30s` when waiting |

Returns:
```json
{
  "status": "failed",
  "pr_number": 42,
  "head_sha": "abc123",
  "checks_passed": false,
  "checks_failed": true,
  "checks_pending": false,
  "checks_details": "lint (conclusion=failure)",
  "wait_timed_out": false,
  "attempts": 1,
  "message": "one or more CI checks failed"
}
```

Possible `status` values: `passed`, `failed`, `pending`, `no_checks`, `closed`, `merged`, and `unknown`.

### review_pull_request Tool

Fetches a GitHub pull request's diff, file changes, and metadata for code review. Returns the full unified diff, individual file patches with change statistics, and PR metadata (title, body, author, branches).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of the child task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `pr_number` | integer | yes | GitHub pull request number to review |

Returns:
```json
{
  "pr_title": "feat: add auth middleware",
  "pr_body": "Adds JWT-based authentication...",
  "pr_author": "dev-bot",
  "base_branch": "main",
  "head_branch": "feature/jwt-auth",
  "diff": "diff --git a/auth.go ...",
  "files": [
    {"filename": "auth.go", "status": "added", "additions": 42, "deletions": 0, "patch": "..."}
  ],
  "status": "fetched"
}
```

Usage: Call `review_pull_request` after `create_pull_request` to fetch the PR diff, then analyze the changes and submit feedback via `post_review_comment`.

### post_review_comment Tool

Posts a review on a GitHub pull request with a verdict (APPROVE, REQUEST_CHANGES, or COMMENT) and optional line-level comments.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of the child task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `pr_number` | integer | yes | GitHub pull request number |
| `body` | string | yes | Top-level review body text |
| `event` | string | yes | Review verdict: `APPROVE`, `REQUEST_CHANGES`, or `COMMENT` |
| `comments` | array | no | Line-level review comments (each with `path`, `line`, `body`) |

Each item in `comments`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | yes | File path relative to repo root |
| `line` | integer | yes | Line number in the diff (new file line number) |
| `body` | string | yes | Comment text |

Returns:
```json
{
  "review_id": 12345,
  "status": "submitted",
  "html_url": "https://github.com/owner/repo/pull/1#pullrequestreview-12345"
}
```

### merge_pull_request Tool

Merges a GitHub pull request after verifying all CI checks have passed. Supports merge, squash, and rebase merge methods.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | yes | Name of the child task whose workspace config has the repo and git credentials |
| `pr_number` | integer | yes | GitHub pull request number to merge |
| `merge_method` | string | no | Merge method: `merge`, `squash`, or `rebase` (defaults to `squash`) |
| `commit_title` | string | no | Custom merge commit title |
| `commit_message` | string | no | Custom merge commit message |

Returns:
```json
{
  "sha": "abc123def456",
  "merged": true,
  "checks_passed": true,
  "message": "Pull request merged successfully"
}
```

If CI checks have not passed, returns `{"merged": false, "checks_passed": false, "message": "CI checks have not all passed: ..."}` without failing.

### auto_merge_pull_request Tool

Polls GitHub CI checks and automatically merges a pull request when all checks pass. Unlike `merge_pull_request` which checks CI once and fails if not green, this tool blocks with a configurable timeout (default 30 minutes), polling every 30 seconds until CI passes, fails, or the PR is closed/merged externally. Resilient to transient GitHub API errors (429/5xx).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | yes | Name of the child task whose workspace config has the repo and git credentials |
| `pr_number` | integer | yes | GitHub pull request number to merge |
| `merge_method` | string | no | Merge method: `merge`, `squash`, or `rebase` (defaults to `squash`) |
| `commit_title` | string | no | Custom merge commit title |
| `commit_message` | string | no | Custom merge commit message |
| `timeout` | string | no | Maximum wait duration, e.g. `30m`, `1h` (defaults to `30m`) |

Returns:
```json
{
  "merged": true,
  "checks_passed": true,
  "sha": "abc123def456",
  "message": "Pull request merged successfully",
  "outcome": "merged"
}
```

Possible `outcome` values:
- `"merged"` — CI passed and PR was merged successfully
- `"ci_failed"` — A CI check completed with failure; returns immediately with `checks_details`
- `"closed"` — PR was closed externally during polling
- `"already_merged"` — PR was already merged when checked
- `"timeout"` — Timeout exceeded while CI checks were still pending

### list_issues Tool

Lists open GitHub issues in a repository. By default only returns unassigned issues. Returns issue number, title, body (truncated to 500 chars), labels, and author.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of a task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `unassigned_only` | boolean | no | If true, only return issues with no assignee (default: `true`) |
| `per_page` | integer | no | Number of results per page (default: 30, max: 100) |
| `page` | integer | no | Page number for pagination (default: 1) |

At least one repository source must be available: `task_name`, the current task context, or `ORKA_GIT_REPO`. Explicit `repo_url` is only accepted when it matches the task's permitted repository scope.

### list_pull_requests Tool

Lists open pull requests in a GitHub repository. Returns PR numbers, titles, authors, branches, labels, and URLs.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of a task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `per_page` | integer | no | Number of results per page (default: 30, max: 100) |
| `page` | integer | no | Page number for pagination (default: 1) |

### check_pr_review_marker Tool

Checks whether a pull request already has an Orka review marker for the current head SHA. PR monitor tasks use this before review to avoid duplicate GitHub reviews on the same head.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pr_number` | integer | yes | GitHub pull request number to inspect |
| `task_name` | string | no | Name of a task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `head_sha` | string | no | PR head SHA to check. If omitted, the tool fetches the current PR head SHA. |

The tool returns a hidden review marker that should be included unchanged in the later `post_review_comment` body. The marker binds `repo`, `pr`, and `head_sha`, and includes a stable signature:

```html
<!-- orka:pr-review repo=owner/repo pr=123 head_sha=abc123 sig=... -->
```

Marker signatures are not derived from the live GitHub token. For stronger verification, set `ORKA_PR_REVIEW_MARKER_SECRET` in the worker Task environment. During rotation, keep old marker keys in comma-separated `ORKA_PR_REVIEW_MARKER_PREVIOUS_SECRETS`. Orka also accepts legacy markers from a trusted review author; set `ORKA_PR_REVIEW_MARKER_TRUSTED_AUTHOR` to the bot login, or omit it to use the authenticated GitHub login for the Task credential.

### get_issue Tool

Fetches full details of a specific GitHub issue by number, including title, body, labels, assignees, state, and the first page of comments.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of a task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `issue_number` | integer | yes | GitHub issue number to fetch |

### comment_on_issue Tool

Posts a comment on a GitHub issue. Use for status updates, progress reports, or agent activity notes.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `task_name` | string | no | Name of a task whose workspace config has the repo and git credentials |
| `repo_url` | string | no | GitHub repository URL. When task context is present, it must match that task's workspace or transaction repository scope. |
| `issue_number` | integer | yes | GitHub issue number to comment on |
| `body` | string | yes | Comment text (Markdown supported) |

### update_plan Tool

Updates the autonomous execution plan state. Must be called at least once per iteration in autonomous mode.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `summary` | string | yes | Brief human-readable summary of current progress (1-2 sentences) |
| `progress_pct` | integer | no | Estimated progress percentage (0-100) |
| `goal_complete` | boolean | no | Set to `true` when the overall goal has been fully achieved |
| `plan_document` | string | yes | Full markdown plan document. Replaces the previous plan document entirely |

### Recommended coordinator system prompt (with PR workflow)

```
You are a coordinator agent. Follow this protocol:

1. DELEGATE implementation to the coder agent with workspace.intent = write,
   separate readCredentialRef/publicationCredentialRef, publicationGitRepo,
   pushBranch, and prBaseBranch. Tell the coder to edit only.
2. WAIT for the coder's result and require a verified delivery receipt.
3. DELEGATE review to the reviewer agent (workspace.intent = read on the
   PUBLISHED head: publicationGitRepo — or gitRepo for same-repository
   publication — at branch = pushBranch, with a readCredentialRef that can
   read it). The coder's verified changes live only on pushBranch; the ACP
   core runtime does not accept prior_task — the reviewer reads the published
   head rather than replaying a diff.
4. WAIT for the reviewer's verdict.
5. IF verdict == "CHANGES_NEEDED" AND iteration < 3:
   DELEGATE fix to the coder agent again (workspace.intent = write) with the
   reviewer feedback in the prompt.
   Go to step 2.
6. IF verdict == "APPROVED":
   Call create_pull_request with the coder's task name and pushBranch.
   Call review_pull_request to fetch the PR diff for final review.
   Call post_review_comment to approve the PR.
   Call check_pull_request_ci to verify CI status.
   If CI failed, delegate a focused repair task to the coder on the PR branch,
   then check CI again before reporting the PR as ready.
   Call merge_pull_request or auto_merge_pull_request only if the user asked to merge.
7. Report final result with the PR URL, review result, and CI status.
```

## Autonomous mode

For long-running goals that require multiple planning and execution cycles, enable autonomous mode on the coordinator agent. See [Autonomous Task execution](../guides/autonomous-tasks.md) for details.

When `coordination.autonomous: true` is set, the controller runs the coordinator in a loop — each iteration gets the accumulated plan state, delegates sub-tasks, and updates the plan. The loop continues until the goal is complete, max iterations are reached, or the task is suspended.

## Roadmap

Planned and in-progress extensions to the coordination model:

- **TaskGroup CRD** — Declarative DAG-based workflows (Layer 2)
- **Agent-to-Agent Messaging** — Real-time inter-agent communication (Layer 3)
- **Cost tracking** — Per-task/child token usage aggregation
- **UI support** — Tree view of coordinator → child tasks in Web UI
- **Recursive delegation** — Child agents that are themselves coordinators (works with maxDepth, but needs testing)
