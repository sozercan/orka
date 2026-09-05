---
slug: /cli-commands
description: "Generated help output for every orka CLI command."
---

# CLI command reference

This page is generated from `orka --help` output. Do not edit it by hand; run:

```bash
make docs-cli
```

For workflow-oriented examples and coverage notes, see [CLI reference](./cli.md).

## `orka`

```text
Orka CLI — Kubernetes-native task execution platform

Usage:
  orka [command]

Available Commands:
  agent         Manage agents
  agent-runtime Manage external orka.harness.v2 AgentRuntime registrations
  audit         Inspect audit and transaction traces
  auth          Inspect authentication
  completion    Generate the autocompletion script for the specified shell
  config        Manage CLI configuration
  gateway       Inspect generic gateway resources and durable event delivery
  help          Help about any command
  login         Authenticate with the Orka dashboard
  memory        Manage durable memory
  models        List compatible model IDs
  monitor       Manage repository monitors
  provider      Manage providers
  run           Chat with the Orka AI assistant
  runtime-pool  Manage controller-owned ACP runtime pools
  secret        Inspect secret metadata
  security      Manage repository security scans
  session       Manage sessions
  skill         Manage skills
  status        Show system overview (health, tasks, agents)
  substrate     Inspect and manage substrate resources
  task          Manage tasks
  tool          Manage tools
  workspace     Inspect task workspace status

Flags:
  -h, --help                    help for orka
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
  -v, --version                 version for orka

Use "orka [command] --help" for more information about a command.
```

## `orka agent`

```text
Manage agents

Usage:
  orka agent [command]

Available Commands:
  create      Create an agent from a manifest
  delete      Delete an agent
  get         Get agent details
  list        List agents
  update      Update an agent resource from a manifest

Flags:
  -h, --help   help for agent

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka agent [command] --help" for more information about a command.
```

## `orka agent create`

```text
Create an agent from a manifest

Usage:
  orka agent create -f <file> [flags]

Flags:
  -f, --file string   Path to agent YAML/JSON manifest
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent delete`

```text
Delete an agent

Usage:
  orka agent delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent get`

```text
Get agent details

Usage:
  orka agent get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent list`

```text
List agents

Usage:
  orka agent list [flags]

Flags:
  -h, --help            help for list
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent update`

```text
Update an agent resource from a manifest

Usage:
  orka agent update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent-runtime`

```text
Manage external orka.harness.v2 AgentRuntime registrations

Usage:
  orka agent-runtime [command]

Available Commands:
  create      Create an agent runtime resource from a manifest
  delete      Delete an agent runtime resource
  get         Get an agent runtime resource
  list        List agent runtime resources
  update      Update an agent runtime resource from a manifest

Flags:
  -h, --help   help for agent-runtime

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka agent-runtime [command] --help" for more information about a command.
```

## `orka agent-runtime create`

```text
Create an agent runtime resource from a manifest

Usage:
  orka agent-runtime create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent-runtime delete`

```text
Delete an agent runtime resource

Usage:
  orka agent-runtime delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent-runtime get`

```text
Get an agent runtime resource

Usage:
  orka agent-runtime get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent-runtime list`

```text
List agent runtime resources

Usage:
  orka agent-runtime list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka agent-runtime update`

```text
Update an agent runtime resource from a manifest

Usage:
  orka agent-runtime update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka audit`

```text
Inspect audit and transaction traces

Usage:
  orka audit [command]

Available Commands:
  trace       Show tasks correlated by transaction ID

Flags:
  -h, --help   help for audit

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka audit [command] --help" for more information about a command.
```

## `orka audit trace`

```text
Show tasks correlated by transaction ID

Usage:
  orka audit trace <transaction-id> [flags]

Flags:
  -h, --help        help for trace
      --limit int   Maximum number of matching tasks to show (default 100)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka auth`

```text
Inspect authentication

Usage:
  orka auth [command]

Available Commands:
  validate    Validate current credentials
  whoami      Show sanitized authenticated identity

Flags:
  -h, --help   help for auth

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka auth [command] --help" for more information about a command.
```

## `orka auth validate`

```text
Validate current credentials

Usage:
  orka auth validate [flags]

Flags:
  -h, --help            help for validate
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka auth whoami`

```text
Show sanitized authenticated identity

Usage:
  orka auth whoami [flags]

Flags:
  -h, --help            help for whoami
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka completion`

```text
Generate the autocompletion script for orka for the specified shell.
See each sub-command's help for details on how to use the generated script.

Usage:
  orka completion [command]

Available Commands:
  bash        Generate the autocompletion script for bash
  fish        Generate the autocompletion script for fish
  powershell  Generate the autocompletion script for powershell
  zsh         Generate the autocompletion script for zsh

Flags:
  -h, --help   help for completion

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka completion [command] --help" for more information about a command.
```

## `orka config`

```text
Manage CLI configuration

Usage:
  orka config [command]

Available Commands:
  set-namespace Set the default Kubernetes namespace
  set-server    Set the default Orka server URL
  set-token     Set the default authentication token
  view          Show current configuration

Flags:
  -h, --help   help for config

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka config [command] --help" for more information about a command.
```

## `orka config set-namespace`

```text
Set the default Kubernetes namespace

Usage:
  orka config set-namespace <namespace> [flags]

Flags:
  -h, --help   help for set-namespace

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka config set-server`

```text
Set the default Orka server URL

Usage:
  orka config set-server <url> [flags]

Flags:
  -h, --help   help for set-server

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka config set-token`

```text
Set the default authentication token

Usage:
  orka config set-token [token] [flags]

Flags:
  -f, --file string   Read token from file (use - for stdin)
  -h, --help          help for set-token

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka config view`

```text
Show current configuration

Usage:
  orka config view [flags]

Flags:
  -h, --help   help for view

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway`

```text
Inspect generic gateway resources and durable event delivery

Usage:
  orka gateway [command]

Available Commands:
  binding     Inspect GatewayBinding routes
  class       Inspect cluster-scoped GatewayClass profiles
  deliveries  Inspect and retry durable gateway deliveries
  events      Inspect durable normalized gateway ingress events
  get         Get a gateway resource
  list        List gateway resources

Flags:
  -h, --help   help for gateway

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka gateway [command] --help" for more information about a command.
```

## `orka gateway binding`

```text
Inspect GatewayBinding routes

Usage:
  orka gateway binding [command]

Available Commands:
  get         Get a gateway binding resource
  list        List gateway binding resources

Flags:
  -h, --help   help for binding

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka gateway binding [command] --help" for more information about a command.
```

## `orka gateway binding get`

```text
Get a gateway binding resource

Usage:
  orka gateway binding get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway binding list`

```text
List gateway binding resources

Usage:
  orka gateway binding list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway class`

```text
Inspect cluster-scoped GatewayClass profiles

Usage:
  orka gateway class [command]

Available Commands:
  get         Get a gateway class resource
  list        List gateway class resources

Flags:
  -h, --help   help for class

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka gateway class [command] --help" for more information about a command.
```

## `orka gateway class get`

```text
Get a gateway class resource

Usage:
  orka gateway class get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway class list`

```text
List gateway class resources

Usage:
  orka gateway class list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway deliveries`

```text
Inspect and retry durable gateway deliveries

Usage:
  orka gateway deliveries [command]

Available Commands:
  get         Get a gateway delivery resource
  list        List gateway delivery resources
  retry       Retry a dead-lettered gateway delivery

Flags:
  -h, --help   help for deliveries

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka gateway deliveries [command] --help" for more information about a command.
```

## `orka gateway deliveries get`

```text
Get a gateway delivery resource

Usage:
  orka gateway deliveries get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway deliveries list`

```text
List gateway delivery resources

Usage:
  orka gateway deliveries list [flags]

Flags:
      --binding string    Filter by GatewayBinding name
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
      --event string      Filter by gateway event ID
      --gateway string    Filter by Gateway name
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")
      --session string    Filter by Session name
      --state string      Filter by comma-separated delivery state
      --task string       Filter by Task name

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway deliveries retry`

```text
Retry a dead-lettered gateway delivery

Usage:
  orka gateway deliveries retry <delivery-id> [flags]

Flags:
  -h, --help            help for retry
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway events`

```text
Inspect durable normalized gateway ingress events

Usage:
  orka gateway events [command]

Available Commands:
  get         Get a gateway event resource
  list        List gateway event resources

Flags:
  -h, --help   help for events

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka gateway events [command] --help" for more information about a command.
```

## `orka gateway events get`

```text
Get a gateway event resource

Usage:
  orka gateway events get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway events list`

```text
List gateway event resources

Usage:
  orka gateway events list [flags]

Flags:
      --binding string    Filter by GatewayBinding name
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
      --gateway string    Filter by Gateway name
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")
      --session string    Filter by Session name
      --state string      Filter by comma-separated event state
      --task string       Filter by Task name

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway get`

```text
Get a gateway resource

Usage:
  orka gateway get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka gateway list`

```text
List gateway resources

Usage:
  orka gateway list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka login`

```text
Generate a ServiceAccount token and open the Orka dashboard in your browser.

Usage:
  orka login [flags]

Flags:
  -h, --help                     help for login
      --no-open                  Print the login URL without opening a browser
      --redact-token             Redact the token in printed output while preserving browser login
      --service-account string   ServiceAccount name (default "default")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory`

```text
Manage durable memory

Usage:
  orka memory [command]

Available Commands:
  create      Create a memory
  delete      Delete a memory
  disable     Disable a memory
  enable      Enable a memory
  get         Get a memory
  list        List memories
  proposal    Manage memory proposals
  update      Update a memory

Flags:
  -h, --help   help for memory

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka memory [command] --help" for more information about a command.
```

## `orka memory create`

```text
Create a memory

Usage:
  orka memory create [flags]

Flags:
      --content string   Memory content
  -f, --file string      Path to memory JSON/YAML body
  -h, --help             help for create
      --source string    Memory source (default "cli")
      --tags string      Comma-separated tags

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory delete`

```text
Delete a memory

Usage:
  orka memory delete <id> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory disable`

```text
Disable a memory

Usage:
  orka memory disable <id> [flags]

Flags:
  -h, --help   help for disable

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory enable`

```text
Enable a memory

Usage:
  orka memory enable <id> [flags]

Flags:
  -h, --help   help for enable

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory get`

```text
Get a memory

Usage:
  orka memory get <id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory list`

```text
List memories

Usage:
  orka memory list [flags]

Flags:
      --agentName string     Filter by agentName
  -h, --help                 help for list
      --ids string           Filter by ids
      --include-deleted      Include deleted memories
      --include-disabled     Include disabled memories
      --limit int            Maximum number of results (default 100)
  -o, --output string        Output format: table, json, yaml (default "table")
      --parentTask string    Filter by parentTask
      --query string         Filter by query
      --sessionName string   Filter by sessionName
      --source string        Filter by source
      --tags string          Filter by tags
      --taskName string      Filter by taskName

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory proposal`

```text
Manage memory proposals

Usage:
  orka memory proposal [command]

Available Commands:
  apply       Apply an accepted memory proposal
  archive     Archive a memory proposal
  get         Get a memory proposal
  list        List memory proposals
  review      Review a memory proposal

Flags:
  -h, --help   help for proposal

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka memory proposal [command] --help" for more information about a command.
```

## `orka memory proposal apply`

```text
Apply an accepted memory proposal

Usage:
  orka memory proposal apply <id> [flags]

Flags:
      --applied-by string   Reviewer applying the proposal
  -h, --help                help for apply

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory proposal archive`

```text
Archive a memory proposal

Usage:
  orka memory proposal archive <id> [flags]

Flags:
  -h, --help   help for archive

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory proposal get`

```text
Get a memory proposal

Usage:
  orka memory proposal get <id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory proposal list`

```text
List memory proposals

Usage:
  orka memory proposal list [flags]

Flags:
      --agent-name string   Filter by agent name
  -h, --help                help for list
      --limit int           Maximum number of results (default 100)
  -o, --output string       Output format: table, json, yaml (default "table")
      --query string        Search query
      --status string       Filter by proposal status
      --task-name string    Filter by task name
      --type string         Filter by proposal type

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory proposal review`

```text
Review a memory proposal

Usage:
  orka memory proposal review <id> [flags]

Flags:
  -h, --help              help for review
      --note string       Review note
      --reviewer string   Reviewer name
      --status string     Review status (accepted or rejected)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka memory update`

```text
Update a memory

Usage:
  orka memory update <id> [flags]

Flags:
      --content string   Memory content
  -f, --file string      Path to memory JSON/YAML body
  -h, --help             help for update
      --source string    Memory source
      --tags string      Comma-separated tags

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka models`

```text
List compatible model IDs

Usage:
  orka models [command]

Available Commands:
  list        List model IDs

Flags:
  -h, --help   help for models

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka models [command] --help" for more information about a command.
```

## `orka models list`

```text
List model IDs

Usage:
  orka models list [flags]

Flags:
      --compat string   Compatibility surface: openai or anthropic (default "openai")
  -h, --help            help for list
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor`

```text
Manage repository monitors

Usage:
  orka monitor [command]

Available Commands:
  actions         Inspect repository monitor action records
  commands        Inspect repository monitor command events
  create          Create a repository monitor resource from a manifest
  delete          Delete a repository monitor resource
  doctor          Summarize monitor workflow health
  events          List repository monitor events
  get             Get a repository monitor resource
  implementations Inspect repository monitor implementation jobs
  issue           Control a repository monitor issue workflow
  issues          Inspect repository monitor issue inventory
  items           List repository monitor items
  list            List repository monitor resources
  mutations       Inspect controller-owned GitHub mutation records
  pr              Control a repository monitor pull request workflow
  run             Trigger a manual repository monitor run
  runs            List repository monitor runs
  trigger-labels  Validate monitor label trigger configuration
  update          Update a repository monitor resource from a manifest
  watch           Watch monitor status
  work-actions    Inspect repository monitor workflow actions

Flags:
  -h, --help   help for monitor

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor [command] --help" for more information about a command.
```

## `orka monitor actions`

```text
Inspect repository monitor action records

Usage:
  orka monitor actions [command]

Available Commands:
  get         Get a repository monitor action
  list        List repository monitor actions

Flags:
  -h, --help   help for actions

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor actions [command] --help" for more information about a command.
```

## `orka monitor actions get`

```text
Get a repository monitor action

Usage:
  orka monitor actions get <action-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor actions list`

```text
List repository monitor actions

Usage:
  orka monitor actions list <name> [flags]

Flags:
      --action-kind string   Filter by action kind
      --continue string      Continue token
      --cursor string        Cursor token
  -h, --help                 help for list
      --kind string          Filter by target kind
      --limit int            Maximum number of results (default 50)
      --number int           Filter by target number
  -o, --output string        Output format: table, json, yaml (default "table")
      --task-name string     Filter by task name

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor commands`

```text
Inspect repository monitor command events

Usage:
  orka monitor commands [command]

Available Commands:
  create      Create a repository monitor command
  get         Get a repository monitor command
  list        List repository monitor commands

Flags:
  -h, --help   help for commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor commands [command] --help" for more information about a command.
```

## `orka monitor commands create`

```text
Create a repository monitor command

Usage:
  orka monitor commands create <name> [flags]

Flags:
  -h, --help                help for create
      --intent string       Command intent
      --kind string         Target kind (issue or pull_request) (default "issue")
      --number int          Target issue or pull request number
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Target head SHA for pull request commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor commands get`

```text
Get a repository monitor command

Usage:
  orka monitor commands get <command-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor commands list`

```text
List repository monitor commands

Usage:
  orka monitor commands list <name> [flags]

Flags:
      --continue string   Continue token
      --cursor string     Cursor token
  -h, --help              help for list
      --intent string     Filter by command intent
      --kind string       Filter by target kind
      --limit int         Maximum number of results (default 50)
      --number int        Filter by target number
  -o, --output string     Output format: table, json, yaml (default "table")
      --status string     Filter by command status

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor create`

```text
Create a repository monitor resource from a manifest

Usage:
  orka monitor create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor delete`

```text
Delete a repository monitor resource

Usage:
  orka monitor delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor doctor`

```text
Summarize monitor workflow health

Usage:
  orka monitor doctor <name> [flags]

Flags:
  -h, --help            help for doctor
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor events`

```text
List repository monitor events

Usage:
  orka monitor events [name] [flags]

Flags:
      --continue string     Continue token
      --cursor string       Cursor token
      --event-type string   Filter by event type
  -h, --help                help for events
      --item-kind string    Filter by item kind
      --item-number int     Filter by item number
      --limit int           Maximum number of results (default 50)
      --name string         Repository monitor name
  -o, --output string       Output format: table, json, yaml (default "table")
      --run-id string       Filter by run ID

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor get`

```text
Get a repository monitor resource

Usage:
  orka monitor get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor implementations`

```text
Inspect repository monitor implementation jobs

Usage:
  orka monitor implementations [command]

Available Commands:
  get         Get a repository monitor implementation job
  list        List repository monitor implementation jobs

Flags:
  -h, --help   help for implementations

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor implementations [command] --help" for more information about a command.
```

## `orka monitor implementations get`

```text
Get a repository monitor implementation job

Usage:
  orka monitor implementations get <job-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor implementations list`

```text
List repository monitor implementation jobs

Usage:
  orka monitor implementations list <name> [flags]

Flags:
      --continue string    Continue token
      --cursor string      Cursor token
  -h, --help               help for list
      --issue-number int   Filter by issue number
      --limit int          Maximum number of results (default 50)
  -o, --output string      Output format: table, json, yaml (default "table")
      --phase string       Filter by phase
      --task-name string   Filter by implementation task name

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue`

```text
Control a repository monitor issue workflow

Usage:
  orka monitor issue [command]

Available Commands:
  approve-plan   Approve the current issue plan
  decompose      Queue issue decomposition
  implement      Queue issue implementation
  implementation Inspect issue implementation jobs
  patch          Inspect issue patch artifacts
  plan           Queue issue planning
  research       Queue issue research
  resume         Resume issue automation
  status         Show issue workflow status
  stop           Stop issue automation
  triage         Queue issue triage

Flags:
  -h, --help   help for issue

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor issue [command] --help" for more information about a command.
```

## `orka monitor issue approve-plan`

```text
Approve the current issue plan

Usage:
  orka monitor issue approve-plan <name> <number> [flags]

Flags:
  -h, --help            help for approve-plan
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue decompose`

```text
Queue issue decomposition

Usage:
  orka monitor issue decompose <name> <number> [flags]

Flags:
  -h, --help            help for decompose
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue implement`

```text
Queue issue implementation

Usage:
  orka monitor issue implement <name> <number> [flags]

Flags:
  -h, --help            help for implement
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue implementation`

```text
Inspect issue implementation jobs

Usage:
  orka monitor issue implementation [command]

Available Commands:
  get         Show latest implementation jobs for an issue

Flags:
  -h, --help   help for implementation

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor issue implementation [command] --help" for more information about a command.
```

## `orka monitor issue implementation get`

```text
Show latest implementation jobs for an issue

Usage:
  orka monitor issue implementation get <name> <number> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue patch`

```text
Inspect issue patch artifacts

Usage:
  orka monitor issue patch [command]

Available Commands:
  preview     Show safe patch artifact metadata for an issue

Flags:
  -h, --help   help for patch

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor issue patch [command] --help" for more information about a command.
```

## `orka monitor issue patch preview`

```text
Show safe patch artifact metadata for an issue

Usage:
  orka monitor issue patch preview <name> <number> [flags]

Flags:
  -h, --help            help for preview
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue plan`

```text
Queue issue planning

Usage:
  orka monitor issue plan <name> <number> [flags]

Flags:
  -h, --help            help for plan
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue research`

```text
Queue issue research

Usage:
  orka monitor issue research <name> <number> [flags]

Flags:
  -h, --help            help for research
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue resume`

```text
Resume issue automation

Usage:
  orka monitor issue resume <name> <number> [flags]

Flags:
  -h, --help            help for resume
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue status`

```text
Show issue workflow status

Usage:
  orka monitor issue status <name> <number> [flags]

Flags:
  -h, --help            help for status
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue stop`

```text
Stop issue automation

Usage:
  orka monitor issue stop <name> <number> [flags]

Flags:
  -h, --help            help for stop
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issue triage`

```text
Queue issue triage

Usage:
  orka monitor issue triage <name> <number> [flags]

Flags:
  -h, --help            help for triage
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issues`

```text
Inspect repository monitor issue inventory

Usage:
  orka monitor issues [command]

Available Commands:
  get         Get a repository monitor issue item
  list        List repository monitor issues

Flags:
  -h, --help   help for issues

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor issues [command] --help" for more information about a command.
```

## `orka monitor issues get`

```text
Get a repository monitor issue item

Usage:
  orka monitor issues get <name> <number> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor issues list`

```text
List repository monitor issues

Usage:
  orka monitor issues list <name> [flags]

Flags:
      --continue string   Continue token
      --cursor string     Cursor token
  -h, --help              help for list
      --limit int         Maximum number of results (default 50)
  -o, --output string     Output format: table, json, yaml (default "table")
      --state string      Filter by state

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor items`

```text
List repository monitor items

Usage:
  orka monitor items <name> [flags]

Flags:
      --automerge-state string   Filter by automerge state
      --continue string          Continue token
      --cursor string            Cursor token
  -h, --help                     help for items
      --kind string              Filter by item kind
      --limit int                Maximum number of results (default 50)
      --number int               Filter by item number
  -o, --output string            Output format: table, json, yaml (default "table")
      --repair-state string      Filter by repair state
      --state string             Filter by state
      --verdict string           Filter by review verdict

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor list`

```text
List repository monitor resources

Usage:
  orka monitor list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor mutations`

```text
Inspect controller-owned GitHub mutation records

Usage:
  orka monitor mutations [command]

Available Commands:
  get         Get a repository monitor GitHub mutation record
  list        List repository monitor GitHub mutation records

Flags:
  -h, --help   help for mutations

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor mutations [command] --help" for more information about a command.
```

## `orka monitor mutations get`

```text
Get a repository monitor GitHub mutation record

Usage:
  orka monitor mutations get <mutation-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor mutations list`

```text
List repository monitor GitHub mutation records

Usage:
  orka monitor mutations list <name> [flags]

Flags:
      --continue string    Continue token
      --cursor string      Cursor token
  -h, --help               help for list
      --kind string        Filter by target kind
      --limit int          Maximum number of results (default 50)
      --number int         Filter by target number
      --operation string   Filter by GitHub operation
  -o, --output string      Output format: table, json, yaml (default "table")
      --status string      Filter by status

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr`

```text
Control a repository monitor pull request workflow

Usage:
  orka monitor pr [command]

Available Commands:
  automerge     Request head-bound automerge
  fix           Queue PR finding repair
  fix-ci        Queue PR CI repair
  ready         Inspect merge-ready PRs
  repairs       Inspect PR repair jobs
  resume        Resume PR automation
  review        Queue exact-head PR review
  status        Show PR workflow status
  stop          Stop PR automation
  update-branch Queue PR branch update

Flags:
  -h, --help   help for pr

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor pr [command] --help" for more information about a command.
```

## `orka monitor pr automerge`

```text
Request head-bound automerge

Usage:
  orka monitor pr automerge <name> <number> [flags]

Flags:
  -h, --help                help for automerge
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr fix`

```text
Queue PR finding repair

Usage:
  orka monitor pr fix <name> <number> [flags]

Flags:
  -h, --help                help for fix
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr fix-ci`

```text
Queue PR CI repair

Usage:
  orka monitor pr fix-ci <name> <number> [flags]

Flags:
  -h, --help                help for fix-ci
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr ready`

```text
Inspect merge-ready PRs

Usage:
  orka monitor pr ready [command]

Available Commands:
  list        List merge-ready pull requests
  readiness   Show readiness state for a pull request

Flags:
  -h, --help   help for ready

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor pr ready [command] --help" for more information about a command.
```

## `orka monitor pr ready list`

```text
List merge-ready pull requests

Usage:
  orka monitor pr ready list <name> [flags]

Flags:
  -h, --help            help for list
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr ready readiness`

```text
Show readiness state for a pull request

Usage:
  orka monitor pr ready readiness <name> <number> [flags]

Flags:
  -h, --help            help for readiness
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr repairs`

```text
Inspect PR repair jobs

Usage:
  orka monitor pr repairs [command]

Available Commands:
  list        List repair jobs for a PR

Flags:
  -h, --help   help for repairs

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor pr repairs [command] --help" for more information about a command.
```

## `orka monitor pr repairs list`

```text
List repair jobs for a PR

Usage:
  orka monitor pr repairs list <name> <number> [flags]

Flags:
  -h, --help            help for list
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr resume`

```text
Resume PR automation

Usage:
  orka monitor pr resume <name> <number> [flags]

Flags:
  -h, --help                help for resume
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr review`

```text
Queue exact-head PR review

Usage:
  orka monitor pr review <name> <number> [flags]

Flags:
  -h, --help                help for review
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr status`

```text
Show PR workflow status

Usage:
  orka monitor pr status <name> <number> [flags]

Flags:
  -h, --help            help for status
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr stop`

```text
Stop PR automation

Usage:
  orka monitor pr stop <name> <number> [flags]

Flags:
  -h, --help                help for stop
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor pr update-branch`

```text
Queue PR branch update

Usage:
  orka monitor pr update-branch <name> <number> [flags]

Flags:
  -h, --help                help for update-branch
  -o, --output string       Output format: table, json, yaml (default "yaml")
      --target-sha string   Current pull request head SHA for head-bound commands

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor run`

```text
Trigger a manual repository monitor run

Usage:
  orka monitor run <name> [flags]

Flags:
  -h, --help                 help for run
      --target-kind string   Target kind (pull_request or issue)
      --target-number int    Target number
      --target-sha string    Target commit SHA

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor runs`

```text
List repository monitor runs

Usage:
  orka monitor runs <name> [flags]

Flags:
      --continue string      Continue token
      --cursor string        Cursor token
  -h, --help                 help for runs
      --limit int            Maximum number of results (default 20)
  -o, --output string        Output format: table, json, yaml (default "table")
      --target-kind string   Filter by target kind
      --trigger string       Filter by trigger

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor trigger-labels`

```text
Validate monitor label trigger configuration

Usage:
  orka monitor trigger-labels [command]

Available Commands:
  validate    Validate monitor label trigger configuration

Flags:
  -h, --help   help for trigger-labels

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor trigger-labels [command] --help" for more information about a command.
```

## `orka monitor trigger-labels validate`

```text
Validate monitor label trigger configuration

Usage:
  orka monitor trigger-labels validate <name> [flags]

Flags:
  -h, --help            help for validate
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor update`

```text
Update a repository monitor resource from a manifest

Usage:
  orka monitor update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor watch`

```text
Watch monitor status

Usage:
  orka monitor watch <name> [flags]

Flags:
  -h, --help                help for watch
      --interval duration   Watch refresh interval (default 5s)
  -o, --output string       Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor work-actions`

```text
Inspect repository monitor workflow actions

Usage:
  orka monitor work-actions [command]

Available Commands:
  get         Get a repository monitor workflow action
  list        List repository monitor workflow actions

Flags:
  -h, --help   help for work-actions

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka monitor work-actions [command] --help" for more information about a command.
```

## `orka monitor work-actions get`

```text
Get a repository monitor workflow action

Usage:
  orka monitor work-actions get <action-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "yaml")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka monitor work-actions list`

```text
List repository monitor workflow actions

Usage:
  orka monitor work-actions list <name> [flags]

Flags:
      --continue string         Continue token
      --cursor string           Cursor token
      --desired-action string   Filter by desired workflow action
  -h, --help                    help for list
      --intent string           Filter by command intent
      --kind string             Filter by target kind
      --limit int               Maximum number of results (default 50)
      --number int              Filter by target number
  -o, --output string           Output format: table, json, yaml (default "table")
      --status string           Filter by workflow status
      --task-name string        Filter by task name

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka provider`

```text
Manage providers

Usage:
  orka provider [command]

Available Commands:
  create      Create a provider resource from a manifest
  delete      Delete a provider resource
  get         Get a provider resource
  list        List provider resources
  update      Update a provider resource from a manifest

Flags:
  -h, --help   help for provider

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka provider [command] --help" for more information about a command.
```

## `orka provider create`

```text
Create a provider resource from a manifest

Usage:
  orka provider create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka provider delete`

```text
Delete a provider resource

Usage:
  orka provider delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka provider get`

```text
Get a provider resource

Usage:
  orka provider get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka provider list`

```text
List provider resources

Usage:
  orka provider list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka provider update`

```text
Update a provider resource from a manifest

Usage:
  orka provider update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka run`

```text
Chat interface backed by Orka chat and provider configuration.

  One-shot:    orka run "explain kubernetes pods"
  Interactive: orka run
  Piped:       echo "fix bugs" | orka run

Usage:
  orka run [prompt] [flags]

Flags:
      --agent string      Agent to use for the task
  -h, --help              help for run
      --model string      Model to use
      --provider string   Chat Provider to use (also selects the coordinator Provider for a runtime --agent)
      --session string    Resume a specific session
  -v, --verbose count     Verbosity level (-v, -vv)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka runtime-pool`

```text
Manage controller-owned ACP runtime pools

Usage:
  orka runtime-pool [command]

Available Commands:
  get         Get a runtime pool resource
  list        List runtime pool resources

Flags:
  -h, --help   help for runtime-pool

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka runtime-pool [command] --help" for more information about a command.
```

## `orka runtime-pool get`

```text
Get a runtime pool resource

Usage:
  orka runtime-pool get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka runtime-pool list`

```text
List runtime pool resources

Usage:
  orka runtime-pool list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka secret`

```text
Inspect secret metadata

Usage:
  orka secret [command]

Available Commands:
  list        List secret resources

Flags:
  -h, --help   help for secret

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka secret [command] --help" for more information about a command.
```

## `orka secret list`

```text
List secret resources

Usage:
  orka secret list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security`

```text
Manage repository security scans

Usage:
  orka security [command]

Available Commands:
  dropped-findings Inspect dropped findings
  finding          Manage security findings
  repo             Manage security repository scan configs
  scan             Run and list security scan runs
  slice            Inspect security review slices
  threat-model     Manage security threat models

Flags:
  -h, --help   help for security

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security [command] --help" for more information about a command.
```

## `orka security dropped-findings`

```text
Inspect dropped findings

Usage:
  orka security dropped-findings [command]

Available Commands:
  list        List dropped security findings

Flags:
  -h, --help   help for dropped-findings

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security dropped-findings [command] --help" for more information about a command.
```

## `orka security dropped-findings list`

```text
List dropped security findings

Usage:
  orka security dropped-findings list <repo> [flags]

Flags:
      --continue string      Continue token
      --cursor string        Cursor token
  -h, --help                 help for list
      --layer string         Filter by dropped-finding layer (validation, filter, cap)
      --limit int            Maximum number of results (default 50)
  -o, --output string        Output format: table, json, yaml (default "table")
      --reason string        Filter by exact reason or contains=<text>
      --scan-run-id string   Filter by scan run ID
      --slice-id string      Filter by review slice ID

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding`

```text
Manage security findings

Usage:
  orka security finding [command]

Available Commands:
  dismiss     dismiss a security finding
  get         Get a security finding
  list        List security findings
  patch       patch a security finding
  patches     List security patch proposals
  pr          Create a pull request for the latest patch proposal
  reopen      reopen a security finding
  validate    validate a security finding

Flags:
  -h, --help   help for finding

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security finding [command] --help" for more information about a command.
```

## `orka security finding dismiss`

```text
dismiss a security finding

Usage:
  orka security finding dismiss <id> [flags]

Flags:
  -h, --help   help for dismiss

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding get`

```text
Get a security finding

Usage:
  orka security finding get <id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding list`

```text
List security findings

Usage:
  orka security finding list <repo> [flags]

Flags:
      --category string            Filter by category
      --continue string            Continue token
      --cursor string              Cursor token
  -h, --help                       help for list
      --limit int                  Maximum number of results (default 50)
  -o, --output string              Output format: table, json, yaml (default "table")
      --recommended                Only recommended findings
      --severity string            Filter by severity
      --slice-id string            Filter by slice ID
      --state string               Filter by state
      --validation-status string   Filter by validation status

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding patch`

```text
patch a security finding

Usage:
  orka security finding patch <id> [flags]

Flags:
  -h, --help            help for patch
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding patches`

```text
List security patch proposals

Usage:
  orka security finding patches <id> [flags]

Flags:
  -h, --help            help for patches
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding pr`

```text
Create a pull request for the latest patch proposal

Usage:
  orka security finding pr <id> [flags]

Flags:
  -h, --help            help for pr
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding reopen`

```text
reopen a security finding

Usage:
  orka security finding reopen <id> [flags]

Flags:
  -h, --help   help for reopen

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security finding validate`

```text
validate a security finding

Usage:
  orka security finding validate <id> [flags]

Flags:
  -h, --help   help for validate

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security repo`

```text
Manage security repository scan configs

Usage:
  orka security repo [command]

Available Commands:
  create      Create a repository scan resource from a manifest
  delete      Delete a repository scan resource
  get         Get a repository scan resource
  list        List repository scan resources
  update      Update a repository scan resource from a manifest

Flags:
  -h, --help   help for repo

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security repo [command] --help" for more information about a command.
```

## `orka security repo create`

```text
Create a repository scan resource from a manifest

Usage:
  orka security repo create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security repo delete`

```text
Delete a repository scan resource

Usage:
  orka security repo delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security repo get`

```text
Get a repository scan resource

Usage:
  orka security repo get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security repo list`

```text
List repository scan resources

Usage:
  orka security repo list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security repo update`

```text
Update a repository scan resource from a manifest

Usage:
  orka security repo update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security scan`

```text
Run and list security scan runs

Usage:
  orka security scan [command]

Available Commands:
  list        List security scan runs
  run         Run a manual security scan

Flags:
  -h, --help   help for scan

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security scan [command] --help" for more information about a command.
```

## `orka security scan list`

```text
List security scan runs

Usage:
  orka security scan list <repo> [flags]

Flags:
      --continue string   Continue token
      --cursor string     Cursor token
  -h, --help              help for list
      --limit int         Maximum number of results (default 20)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security scan run`

```text
Run a manual security scan

Usage:
  orka security scan run <repo> [flags]

Flags:
  -h, --help   help for run

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security slice`

```text
Inspect security review slices

Usage:
  orka security slice [command]

Available Commands:
  get         Get a security review slice
  list        List security review slices

Flags:
  -h, --help   help for slice

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security slice [command] --help" for more information about a command.
```

## `orka security slice get`

```text
Get a security review slice

Usage:
  orka security slice get <repo> <slice-id> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security slice list`

```text
List security review slices

Usage:
  orka security slice list <repo> [flags]

Flags:
      --continue string   Continue token
      --cursor string     Cursor token
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")
      --status string     Filter by status

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security threat-model`

```text
Manage security threat models

Usage:
  orka security threat-model [command]

Available Commands:
  get         Get latest threat model
  update      Update threat model

Flags:
  -h, --help   help for threat-model

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka security threat-model [command] --help" for more information about a command.
```

## `orka security threat-model get`

```text
Get latest threat model

Usage:
  orka security threat-model get <repo> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka security threat-model update`

```text
Update threat model

Usage:
  orka security threat-model update <repo> [flags]

Flags:
      --content string   Threat model content
  -f, --file string      Path to threat model content
  -h, --help             help for update
      --source string    Threat model source (default "edited")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka session`

```text
Manage sessions

Usage:
  orka session [command]

Available Commands:
  delete      Delete a session resource
  events      List session execution events
  follow      Follow session execution events
  get         Get a session resource
  list        List session resources

Flags:
  -h, --help   help for session

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka session [command] --help" for more information about a command.
```

## `orka session delete`

```text
Delete a session resource

Usage:
  orka session delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka session events`

```text
List session execution events

Usage:
  orka session events <session> [flags]

Flags:
      --after int          Only return events after this sequence
  -h, --help               help for events
      --limit int          Maximum events to return (default 100)
  -o, --output string      Output format: table, json, yaml (default "table")
      --type stringArray   Filter by event type (repeatable; streaming supports repeats)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka session follow`

```text
Follow session execution events

Usage:
  orka session follow <session> [flags]

Flags:
      --after int          Resume after this sequence
  -h, --help               help for follow
      --type stringArray   Filter by event type (repeatable)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka session get`

```text
Get a session resource

Usage:
  orka session get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka session list`

```text
List session resources

Usage:
  orka session list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill`

```text
Manage skills

Usage:
  orka skill [command]

Available Commands:
  content     Print raw skill content
  create      Create a skill from a YAML manifest
  delete      Delete a skill
  get         Get skill details
  import      Create a skill from a local SKILL.md file
  init        Initialize a local SKILL.md template
  list        List skills
  update      Update a skill resource from a manifest
  validate    Validate a local skill manifest or SKILL.md file

Flags:
  -h, --help   help for skill

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka skill [command] --help" for more information about a command.
```

## `orka skill content`

```text
Print raw skill content

Usage:
  orka skill content <name> [flags]

Flags:
  -h, --help   help for content

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill create`

```text
Create a skill from a YAML manifest

Usage:
  orka skill create -f <file> [flags]

Flags:
  -f, --file string   Path to skill YAML manifest
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill delete`

```text
Delete a skill

Usage:
  orka skill delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill get`

```text
Get skill details

Usage:
  orka skill get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill import`

```text
Create a skill from a local SKILL.md file

Usage:
  orka skill import <path/to/SKILL.md> [flags]

Flags:
      --description string   Override skill description (default: the SKILL.md "## Description" section)
  -h, --help                 help for import
      --name string          Override skill name (default: the SKILL.md H1 heading, then the parent directory of a SKILL.md, then the filename)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill init`

```text
Initialize a local SKILL.md template

Usage:
  orka skill init [dir] [flags]

Flags:
      --description string   Skill description for the template
      --force                Overwrite an existing SKILL.md
  -h, --help                 help for init
      --name string          Skill name for the template

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill list`

```text
List skills

Usage:
  orka skill list [flags]

Flags:
  -h, --help            help for list
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill update`

```text
Update a skill resource from a manifest

Usage:
  orka skill update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka skill validate`

```text
Validate a local skill manifest or SKILL.md file

Usage:
  orka skill validate [-f manifest.yaml] [SKILL.md] [flags]

Flags:
  -f, --file string   Path to skill YAML/JSON manifest
  -h, --help          help for validate

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka status`

```text
Show system overview (health, tasks, agents)

Usage:
  orka status [flags]

Flags:
  -h, --help   help for status

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka substrate`

```text
Inspect and manage substrate resources

Usage:
  orka substrate [command]

Available Commands:
  pool        Manage substrate actor pools

Flags:
  -h, --help   help for substrate

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka substrate [command] --help" for more information about a command.
```

## `orka substrate pool`

```text
Manage substrate actor pools

Usage:
  orka substrate pool [command]

Available Commands:
  create      Create a substrate pool resource from a manifest
  delete      Delete a substrate pool resource
  get         Get a substrate pool resource
  list        List substrate pool resources
  update      Update a substrate pool resource from a manifest

Flags:
  -h, --help   help for pool

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka substrate pool [command] --help" for more information about a command.
```

## `orka substrate pool create`

```text
Create a substrate pool resource from a manifest

Usage:
  orka substrate pool create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka substrate pool delete`

```text
Delete a substrate pool resource

Usage:
  orka substrate pool delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka substrate pool get`

```text
Get a substrate pool resource

Usage:
  orka substrate pool get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka substrate pool list`

```text
List substrate pool resources

Usage:
  orka substrate pool list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka substrate pool update`

```text
Update a substrate pool resource from a manifest

Usage:
  orka substrate pool update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task`

```text
Manage tasks

Usage:
  orka task [command]

Available Commands:
  approvals   List task approvals
  approve     Approve a pending task approval
  artifacts   List artifacts for a task
  children    List child tasks
  create      Create a new task
  decline     Decline a pending task approval
  delete      Delete a task
  download    Download task artifacts
  events      List task execution events
  follow      Follow task execution events
  fork        Fork a task from an execution event checkpoint
  get         Get task details
  list        List tasks
  logs        Get task logs
  plan        Get task autonomous plan state
  result      Get task result
  status      Show durable execution, delivery, and runtime-pool status
  trace       Show a task trace summary
  wait        Wait for a task to complete

Flags:
  -h, --help   help for task

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka task [command] --help" for more information about a command.
```

## `orka task approvals`

```text
List task approvals

Usage:
  orka task approvals <task> [flags]

Flags:
  -h, --help            help for approvals
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task approve`

```text
Approve a pending task approval

Usage:
  orka task approve <task> <approvalID> [flags]

Flags:
  -h, --help            help for approve
  -o, --output string   Output format: table, json, yaml (default "json")
      --reason string   Decision reason

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task artifacts`

```text
List artifacts for a task

Usage:
  orka task artifacts <task-name> [flags]

Flags:
  -h, --help   help for artifacts

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task children`

```text
List child tasks

Usage:
  orka task children <name> [flags]

Flags:
  -h, --help            help for children
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task create`

```text
Create a new task

Usage:
  orka task create <prompt> [flags]

Flags:
      --agent string                             Agent reference name
      --arg stringArray                          Command argument (repeat for multiple arguments)
      --branch string                            Source branch
      --command stringArray                      Command entry to run (repeat for multiple entries)
      --create-pr                                Reconcile a pull request after verified publication
      --env stringArray                          Environment variable KEY=VALUE (repeatable)
  -f, --file string                              Path to task YAML/JSON manifest
      --forge-credential string                  Secret name for forge API credentials used to reconcile pull requests
      --forge-credential-key string              Secret key for forge API credentials (default: token)
      --git-repo string                          Source repository URL (credentials must not be embedded)
  -h, --help                                     help for create
      --image string                             Container image
      --model string                             Model name for AI tasks
      --name string                              Task name (default: generated)
      --pr-base-branch string                    Pull request base branch
      --priority int32                           Task priority (0-1000)
      --provider string                          Provider reference name for ai tasks (default: a ready Provider named "default", else the namespace's only ready Provider)
      --publication-credential string            Secret name for publication write credentials
      --publication-credential-key string        Secret key for publication write credentials (default: token)
      --publication-git-repo string              Publication repository URL
      --publication-read-credential string       Secret name for publication preflight and verification credentials
      --publication-read-credential-key string   Secret key for publication preflight and verification credentials (default: token)
      --publication-repository-id string         Canonical publication repository ID
      --publication-repository-provider string   Canonical publication repository provider
      --push-branch string                       Publication branch (default: controller-derived full-entropy branch)
      --read-credential string                   Secret name for source clone/read credentials
      --read-credential-key string               Secret key for source clone/read credentials (default: token)
      --ref string                               Source commit, tag, or ref
      --schedule string                          Cron schedule for recurring tasks
      --source-repository-id string              Canonical source repository ID
      --source-repository-provider string        Canonical source repository provider
      --sub-path string                          Subdirectory within the source repository
      --suspend                                  Suspend scheduled task runs
      --timeout string                           Task timeout (e.g., "5m", "1h")
      --timezone string                          IANA time zone for scheduled tasks
      --type string                              Task type: ai, container, agent (default "ai")
      --workspace-intent string                  Agent workspace intent: read or write (default "read")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task decline`

```text
Decline a pending task approval

Usage:
  orka task decline <task> <approvalID> [flags]

Flags:
  -h, --help            help for decline
  -o, --output string   Output format: table, json, yaml (default "json")
      --reason string   Decision reason

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task delete`

```text
Delete a task

Usage:
  orka task delete <name> [flags]

Aliases:
  delete, rm

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task download`

```text
Download task artifacts

Usage:
  orka task download <task-name> [filename] [flags]

Flags:
  -h, --help            help for download
  -o, --output string   output file path

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task events`

```text
List task execution events

Usage:
  orka task events <task> [flags]

Flags:
      --after int          Only return events after this sequence
  -h, --help               help for events
      --limit int          Maximum events to return (default 100)
  -o, --output string      Output format: table, json, yaml (default "table")
      --type stringArray   Filter by event type (repeatable; streaming supports repeats)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task follow`

```text
Follow task execution events

Usage:
  orka task follow <task> [flags]

Flags:
      --after int          Resume after this sequence
  -h, --help               help for follow
      --type stringArray   Filter by event type (repeatable)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task fork`

```text
Fork a task from an execution event checkpoint

Usage:
  orka task fork <task> [flags]

Flags:
      --after int       Checkpoint sequence (default: latest) (default -1)
      --agent string    Override agent reference
  -h, --help            help for fork
      --name string     Forked task name
  -o, --output string   Output format: table, json, yaml (default "table")
      --prompt string   Override prompt

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task get`

```text
Get task details

Usage:
  orka task get <name> [flags]

Flags:
  -h, --help               help for get
  -o, --output string      Output format: table, json, yaml (default "json")
      --show-transaction   Show only transaction metadata

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task list`

```text
List tasks

Usage:
  orka task list [flags]

Aliases:
  list, ls

Flags:
      --continue string      Continue token for the next page
      --cursor string        Cursor token for the next page
  -h, --help                 help for list
      --limit int            Maximum number of results (default 20)
  -o, --output string        Output format: table, json, yaml (default "table")
      --status string        Filter by status (client-side scan; may page through many tasks)
      --transaction string   Filter by transaction ID (client-side scan)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task logs`

```text
Get task logs

Usage:
  orka task logs <name> [flags]

Flags:
  -f, --follow   Stream logs in real time
  -h, --help     help for logs

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task plan`

```text
Get task autonomous plan state

Usage:
  orka task plan <name> [flags]

Flags:
  -h, --help            help for plan
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task result`

```text
Get task result

Usage:
  orka task result <name> [flags]

Flags:
  -h, --help            help for result
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task status`

```text
Show durable execution, delivery, and runtime-pool status

Usage:
  orka task status <name> [flags]

Flags:
  -h, --help            help for status
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task trace`

```text
Show a task trace summary

Usage:
  orka task trace <task> [flags]

Flags:
  -h, --help            help for trace
  -o, --output string   Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka task wait`

```text
Wait for a task to complete

Usage:
  orka task wait <name> [flags]

Flags:
  -h, --help             help for wait
      --timeout string   Maximum time to wait (e.g. 5m)

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka tool`

```text
Manage tools

Usage:
  orka tool [command]

Available Commands:
  create      Create a tool resource from a manifest
  delete      Delete a tool resource
  get         Get a tool resource
  list        List tool resources
  update      Update a tool resource from a manifest

Flags:
  -h, --help   help for tool

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka tool [command] --help" for more information about a command.
```

## `orka tool create`

```text
Create a tool resource from a manifest

Usage:
  orka tool create -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for create

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka tool delete`

```text
Delete a tool resource

Usage:
  orka tool delete <name> [flags]

Flags:
  -h, --help   help for delete

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka tool get`

```text
Get a tool resource

Usage:
  orka tool get <name> [flags]

Flags:
  -h, --help            help for get
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka tool list`

```text
List tool resources

Usage:
  orka tool list [flags]

Flags:
      --continue string   Continue/cursor token for the next page
      --cursor string     Cursor token for the next page
  -h, --help              help for list
      --limit int         Maximum number of results (default 100)
  -o, --output string     Output format: table, json, yaml (default "table")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka tool update`

```text
Update a tool resource from a manifest

Usage:
  orka tool update <name> -f <file> [flags]

Flags:
  -f, --file string   Path to YAML/JSON manifest (use - for stdin)
  -h, --help          help for update

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

## `orka workspace`

```text
Inspect task workspace status

Usage:
  orka workspace [command]

Available Commands:
  status      Show safe workspace status fields

Flags:
  -h, --help   help for workspace

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)

Use "orka workspace [command] --help" for more information about a command.
```

## `orka workspace status`

```text
Show safe workspace status fields

Usage:
  orka workspace status <task> [flags]

Flags:
  -h, --help            help for status
  -o, --output string   Output format: table, json, yaml (default "json")

Global Flags:
      --kubeconfig string       Path to kubeconfig file
  -n, --namespace string        Kubernetes namespace (default "default")
  -s, --server string           Orka server URL (default "http://localhost:8080")
  -t, --token string            Bearer token for authentication
      --txn-token string        Transaction token to send via Txn-Token header
      --txn-token-file string   Path to file containing a Transaction token (use - for stdin)
```

