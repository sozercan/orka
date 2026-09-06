#!/usr/bin/env bash

set -Eeuo pipefail

demo_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
demo_dir="$(cd "${demo_lib_dir}/.." && pwd)"
repo_root="$(cd "${demo_dir}/../.." && pwd)"

# shellcheck source=hack/demos/lib/style.sh
. "${demo_lib_dir}/style.sh"

: "${ORKA_NAMESPACE:=orka-system}"
: "${DEMO_NAMESPACE:=orka-system}"
: "${ORKA_TOKEN_NAMESPACE:=default}"
: "${ORKA_API_BASE:=http://127.0.0.1:8080}"
: "${ORKA_API_SERVICE_NAME:=orka-api}"
: "${ORKA_TOKEN_SERVICE_ACCOUNT:=orka-client}"
: "${ORKA_TOKEN_COMMAND:=}"
: "${ORKA_TOKEN_DURATION:=12h}"
: "${DEMO_WORKDIR:=/tmp/orka-demo}"
: "${DEMO_AUTO_PORT_FORWARD:=1}"

__DEMO_CLEANUP_DIRS=()

demo_register_cleanup_dir() {
  local dir="$1"
  [[ -n "${dir}" ]] || return 0
  __DEMO_CLEANUP_DIRS+=("${dir}")
}

demo_run_exit_cleanups() {
  local dir
  for dir in "${__DEMO_CLEANUP_DIRS[@]:-}"; do
    [[ -n "${dir}" ]] || continue
    rm -rf "${dir}" 2>/dev/null || true
  done
}

trap 'demo_run_exit_cleanups' EXIT

: "${DEMO_PROVIDER_TYPE:=openai}"
: "${DEMO_PROVIDER_SECRET_REF:=}"
: "${DEMO_PROVIDER_SECRET_KEY:=api-key}"
: "${DEMO_PROVIDER_BASE_URL:=}"

: "${DEMO_CHAT_CLIENT:=claude-code}"
: "${DEMO_CLAUDE_BIN:=claude}"
: "${DEMO_CLAUDE_MODEL:=}"
: "${DEMO_CLAUDE_SETTING_SOURCES:=}"
: "${DEMO_CLAUDE_PERMISSION_MODE:=dontAsk}"
: "${DEMO_CLAUDE_TOOLS:=}"
: "${DEMO_CLAUDE_OUTPUT_FORMAT:=json}"
: "${DEMO_SHOW_CHAT_CLIENT_RESULT:=0}"
: "${DEMO_SHOW_FULL_PROMPT:=1}"
: "${DEMO_SHOW_FULL_MANIFEST:=1}"
: "${DEMO_REQUEST_FILE:=}"
: "${DEMO_CHAT_REQUEST_FILE:=${DEMO_REQUEST_FILE}}"
: "${DEMO_MANUAL_REQUEST_FILE:=${DEMO_REQUEST_FILE}}"

: "${DEMO_LABEL_VALUE:=demo-magic}"

: "${DEMO_AGENT_CPU_REQUEST:=100m}"
: "${DEMO_AGENT_CPU_LIMIT:=1}"
: "${DEMO_AGENT_MEMORY_REQUEST:=512Mi}"
: "${DEMO_AGENT_MEMORY_LIMIT:=2Gi}"

: "${DEMO_GIT_REPO:=https://github.com/sozercan/vekil.git}"
: "${DEMO_GIT_BRANCH:=main}"

: "${DEMO_PR_COORDINATOR_NAME:=demo-pr-coordinator}"
: "${DEMO_CODER_AGENT_NAME:=demo-coder}"
: "${DEMO_SECURITY_REVIEWER_NAME:=demo-reviewer-security}"
: "${DEMO_QUALITY_REVIEWER_NAME:=demo-reviewer-quality}"

: "${DEMO_RUN_ID:=$(date +%Y%m%d%H%M%S)}"

: "${DEMO_MANUAL_TASK_NAME:=demo-manual-pr}"
: "${DEMO_CHAT_SESSION_PREFIX:=demo-magic-chat-pr-}"
: "${DEMO_CHAT_SESSION:=${DEMO_CHAT_SESSION_PREFIX}${DEMO_RUN_ID}}"
: "${DEMO_CHAT_PUSH_BRANCH:=demo/vekil-metrics-chat-${DEMO_RUN_ID}}"
: "${DEMO_MANUAL_PUSH_BRANCH:=demo/vekil-metrics-yaml-${DEMO_RUN_ID}}"
: "${DEMO_PR_WORKFLOW_TIMEOUT:=300m}"
: "${DEMO_VALIDATION_REPAIR_LIMIT:=6}"
: "${DEMO_REVIEW_REPAIR_LIMIT:=8}"
: "${DEMO_CI_REPAIR_LIMIT:=3}"

: "${DEMO_CRON_AGENT_NAME:=demo-cron-reporter}"
: "${DEMO_CRON_TASK_NAME:=demo-cron-report}"
: "${DEMO_CRON_SCHEDULE:=*/2 * * * *}"

: "${DEMO_SECURITY_ANALYSIS_AGENT_NAME:=demo-security-analysis}"
: "${DEMO_SECURITY_PATCH_AGENT_NAME:=demo-security-patch}"
: "${DEMO_SECURITY_SCAN_PREFIX:=demo-security-repository}"
: "${DEMO_SECURITY_SCAN_NAME:=${DEMO_SECURITY_SCAN_PREFIX}-${DEMO_RUN_ID}}"
: "${DEMO_SECURITY_SCHEDULE:=}"

: "${DEMO_SECURITY_GIT_REPO:=https://github.com/sozercan/nodejs-goof.git}"
: "${DEMO_SECURITY_GIT_BRANCH:=main}"
: "${DEMO_SECURITY_GIT_SECRET_REF:=${DEMO_GIT_SECRET_REF:-}}"
# nodejs-goof IS the fork — leave DEMO_SECURITY_GIT_FORK_REPO unset by default.
: "${DEMO_SECURITY_GIT_FORK_REPO:=${DEMO_GIT_FORK_REPO:-}}"
: "${DEMO_SECURITY_PR_BASE_BRANCH:=${DEMO_SECURITY_GIT_BRANCH}}"
: "${DEMO_SECURITY_GIT_SUB_PATH:=${DEMO_GIT_SUB_PATH:-}}"

: "${DEMO_VEKIL_METRICS_REQUEST:=Implement GitHub issue sozercan/vekil#77: add a Prometheus-compatible /metrics endpoint for Vekil. Mount /metrics on the existing server and keep it enabled by default, with a --metrics/--no-metrics flag or the closest existing flag style. Use github.com/prometheus/client_golang/prometheus and promhttp. Instrument the chat, responses, messages, and Gemini handler paths where applicable with bounded labels: provider, public_model, endpoint, status, direction, reason, and code as appropriate. Include vekil_requests_total, vekil_request_duration_seconds, vekil_tokens_total for prompt/completion usage, vekil_retries_total, vekil_upstream_errors_total, vekil_inflight_requests, vekil_build_info from the existing version ldflags, and standard Go runtime metrics. Treat vekil_endpoint_healthy as optional if endpoint health state is not available yet, and document any defer. Add focused tests, document histogram buckets and the metrics flag in docs/configuration.md, and add an example Grafana dashboard JSON under docs/ or assets/. Acceptance: curl localhost:1337/metrics returns valid Prometheus exposition after a request, handlers increment the relevant counters with correct labels, no user or key labels are added, and no secrets are logged or exposed. Do not implement OpenTelemetry, Pushgateway support, virtual-key dimensions, or unrelated selector work. Keep the diff focused and easy to review.}"
: "${DEMO_VEKIL_METRICS_SLICE_REQUEST:=Implement GitHub issue sozercan/vekil#77. Important: for Gemini countTokens fallback handling, do not remove or bypass metrics observation for the first probe wholesale. Filter only the expected max_completion_tokens fallback 400; preserve vekil_retries_total and vekil_upstream_errors_total for real first-probe 429/5xx/timeout outcomes, and add regression coverage for a 429-then-success countTokens flow.}"

# ---------------------------------------------------------------------------
# Demo request presets.
#
# DEMO_REQUEST_PRESET selects the chat/manual prompt. Default is `quiet-flag`
# because it's short, real, fits on screen, and finishes in under a minute —
# ideal for recordings. The longer presets stay available for live demos
# where the full audit story matters.
#
# An explicitly-set DEMO_CHAT_REQUEST / DEMO_MANUAL_REQUEST env var or
# DEMO_CHAT_REQUEST_FILE / DEMO_MANUAL_REQUEST_FILE always wins.
# ---------------------------------------------------------------------------
: "${DEMO_QUIET_FLAG_REQUEST:=Add a --quiet flag to vekil that suppresses non-error output when set. Add a test that exercises the flag.}"
: "${DEMO_README_FIX_REQUEST:=Fix the broken link to docs/configuration.md in the README.}"

: "${DEMO_REQUEST_PRESET:=quiet-flag}"
case "${DEMO_REQUEST_PRESET}" in
  quiet-flag)    __DEMO_PRESET_REQUEST="${DEMO_QUIET_FLAG_REQUEST}" ;;
  readme-fix)    __DEMO_PRESET_REQUEST="${DEMO_README_FIX_REQUEST}" ;;
  vekil-metrics) __DEMO_PRESET_REQUEST="${DEMO_VEKIL_METRICS_REQUEST}" ;;
  vekil-metrics-slice) __DEMO_PRESET_REQUEST="${DEMO_VEKIL_METRICS_SLICE_REQUEST}" ;;
  *)
    printf 'error: DEMO_REQUEST_PRESET=%s is not one of quiet-flag|readme-fix|vekil-metrics|vekil-metrics-slice\n' \
      "${DEMO_REQUEST_PRESET}" >&2
    exit 1
    ;;
esac

: "${DEMO_CHAT_REQUEST:=${__DEMO_PRESET_REQUEST}}"
: "${DEMO_MANUAL_REQUEST:=${__DEMO_PRESET_REQUEST}}"
: "${DEMO_CRON_REQUEST:=Triage stale pull requests on github.com/sozercan/vekil.

Use curl against api.github.com (no gh CLI is installed). The GitHub token is
available in the GH_TOKEN environment variable; reference it inside your curl
commands as the value of an 'Authorization: Bearer ...' header. Never print,
echo, or include the token value in your output. Also send
'Accept: application/vnd.github+json' on every request.

Steps:
1. List open PRs: GET /repos/sozercan/vekil/pulls?state=open&per_page=20
2. For each PR idle more than 3 days (computed from updated_at vs the current
   UTC time), fetch its check-runs status and its last review comments so you
   can understand WHY it is stuck.
3. Classify each into exactly one blocker bucket from this set:
   awaiting-review | ci-broken | merge-conflict | author-needs-respond |
   discussion-stalled | dependabot
4. Produce a markdown report in this exact shape (no other commentary):

## Stale PR triage — <current UTC timestamp>
Open: N  |  Idle >3 days: M

| #   | Title (truncated to 50 chars) | Days idle | Blocker | Suggested next action |
|-----|-------------------------------|-----------|---------|-----------------------|
| 162 | Add prometheus metrics endpoint | 12 | author-needs-respond | Ping author about reviewer feedback |
| ... |                                 |    |                       |                                 |

### Top 3 priorities
1. PR #X — <one-line reason a human reviewer should look here first>
2. PR #Y — <one-line reason>
3. PR #Z — <one-line reason>

Output ONLY the markdown report. Do not commit, push, open a PR, comment on
any PR, or modify any file in the workspace.}"

# ---------------------------------------------------------------------------
# Recording profile.
#
# Controls pacing/verbosity of the visual helpers in lib/style.sh:
#   presenter (default) — full transparency, typewriter on
#   docs                — typewriter off, narration cues printed
#   social              — typewriter off, chapters 1..3 only
#   hero                — typewriter off, no chapters
# ---------------------------------------------------------------------------
: "${DEMO_RECORD_PROFILE:=presenter}"
case "${DEMO_RECORD_PROFILE}" in
  presenter|docs|social|hero) ;;
  *)
    printf 'error: DEMO_RECORD_PROFILE=%s is not one of presenter|docs|social|hero\n' \
      "${DEMO_RECORD_PROFILE}" >&2
    exit 1
    ;;
esac

if [[ -n "${DEMO_CHAT_REQUEST_FILE}" ]]; then
  [[ -f "${DEMO_CHAT_REQUEST_FILE}" ]] || { printf 'error: DEMO_CHAT_REQUEST_FILE does not exist: %s\n' "${DEMO_CHAT_REQUEST_FILE}" >&2; exit 1; }
  DEMO_CHAT_REQUEST="$(cat "${DEMO_CHAT_REQUEST_FILE}")"
fi
if [[ -n "${DEMO_MANUAL_REQUEST_FILE}" ]]; then
  [[ -f "${DEMO_MANUAL_REQUEST_FILE}" ]] || { printf 'error: DEMO_MANUAL_REQUEST_FILE does not exist: %s\n' "${DEMO_MANUAL_REQUEST_FILE}" >&2; exit 1; }
  DEMO_MANUAL_REQUEST="$(cat "${DEMO_MANUAL_REQUEST_FILE}")"
fi

ORKA_TOKEN_CACHE=""

log() {
  printf '==> %s\n' "$*" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

require_file() {
  [[ -f "$1" ]] || die "missing required file: $1"
}

require_vars() {
  local name
  for name in "$@"; do
    [[ -n "${!name:-}" ]] || die "missing required environment variable: $name"
  done
}

print_demo_provider_setup_hint() {
  local provider_ref="${1:-${DEMO_PROVIDER_REF:-}}"
  local namespace="${2:-${DEMO_NAMESPACE:-}}"
  local provider_type="${DEMO_PROVIDER_TYPE:-openai}"
  local provider_secret_ref="${DEMO_PROVIDER_SECRET_REF:-<provider-secret-name>}"
  local provider_secret_key="${DEMO_PROVIDER_SECRET_KEY:-api-key}"
  local provider_model="${DEMO_PROVIDER_DEFAULT_MODEL:-${DEMO_AI_MODEL:-<model-name>}}"

  cat >&2 <<EOF

Fix options:
  1. If an existing Provider should be used, point the demo at that exact name:
       kubectl get provider -n ${namespace}
       export DEMO_PROVIDER_REF="<existing-provider-name>"

  2. Or create the missing Provider. First create the referenced API-key Secret
     (replace the placeholder value; do not commit or print real tokens):
       kubectl -n ${namespace} create secret generic '${provider_secret_ref}' \\
         --from-literal='${provider_secret_key}=<provider-api-key-or-proxy-placeholder>' \\
         --dry-run=client -o yaml | kubectl apply -f -

     Then apply a Provider CR that references that Secret:
       cat <<'YAML' | kubectl apply -f -
EOF

  cat >&2 <<EOF
apiVersion: core.orka.ai/v1alpha1
kind: Provider
metadata:
  name: ${provider_ref}
  namespace: ${namespace}
spec:
  type: ${provider_type}
  secretRef:
    name: ${provider_secret_ref}
    key: ${provider_secret_key}
EOF
  if [[ -n "${DEMO_PROVIDER_BASE_URL:-}" ]]; then
    printf '  baseURL: %s\n' "${DEMO_PROVIDER_BASE_URL}" >&2
  else
    printf '  # baseURL: http://<openai-compatible-proxy>.<namespace>.svc.cluster.local:<port>/v1\n' >&2
  fi
  cat >&2 <<EOF
  defaultModel: ${provider_model}
YAML

EOF
}

require_demo_provider() {
  local provider_ref="${1:-${DEMO_PROVIDER_REF:-}}"
  local namespace="${2:-${DEMO_NAMESPACE:-}}"
  local output ready message

  [[ -n "${provider_ref}" ]] || die "missing required environment variable: DEMO_PROVIDER_REF"
  [[ -n "${namespace}" ]] || die "missing required environment variable: DEMO_NAMESPACE"

  if output="$(kubectl get provider "${provider_ref}" -n "${namespace}" 2>&1)"; then
    printf '%s\n' "${output}"
    ready="$(kubectl get provider "${provider_ref}" -n "${namespace}" -o jsonpath='{.status.ready}' 2>/dev/null || true)"
    if [[ "${ready}" == "true" ]]; then
      return 0
    fi

    message="$(kubectl get provider "${provider_ref}" -n "${namespace}" -o jsonpath='{.status.message}' 2>/dev/null || true)"
    printf 'error: Provider %q exists in namespace %q but is not Ready (status.ready=%s).\n' "${provider_ref}" "${namespace}" "${ready:-<empty>}" >&2
    if [[ -n "${message}" ]]; then
      printf 'status.message: %s\n' "${message}" >&2
    fi
    printf 'Check the Provider spec.secretRef.name/key and the referenced Secret in namespace %q.\n' "${namespace}" >&2
    return 1
  fi

  printf 'error: Provider %q was not found in namespace %q.\n' "${provider_ref}" "${namespace}" >&2
  printf 'kubectl said: %s\n' "${output}" >&2
  printf '\nProviders visible in namespace %s:\n' "${namespace}" >&2
  kubectl get provider -n "${namespace}" >&2 || true
  printf '\nProviders visible in all namespaces:\n' >&2
  kubectl get provider -A >&2 || true
  print_demo_provider_setup_hint "${provider_ref}" "${namespace}"
  return 1
}

require_demo_secret() {
  local secret_name="${1:-}"
  local description="${2:-Secret}"
  local namespace="${3:-${DEMO_NAMESPACE:-}}"
  local output key_hint

  [[ -n "${secret_name}" ]] || die "missing secret name for ${description}"
  [[ -n "${namespace}" ]] || die "missing required environment variable: DEMO_NAMESPACE"

  if output="$(kubectl get secret "${secret_name}" -n "${namespace}" 2>&1)"; then
    printf '%s\n' "${output}"
    return 0
  fi

  printf 'error: %s Secret %q was not found in namespace %q.\n' "${description}" "${secret_name}" "${namespace}" >&2
  printf 'kubectl said: %s\n' "${output}" >&2
  printf '\nSecrets visible in namespace %s (data values are not shown):\n' "${namespace}" >&2
  kubectl get secret -n "${namespace}" >&2 || true
  printf '\nCreate the missing Secret with placeholder values replaced locally. Do not commit or print real tokens.\n' >&2

  case "${description}" in
    *runtime*)
      case "${DEMO_RUNTIME_TYPE:-}" in
        codex)
          key_hint="OPENAI_API_KEY"
          ;;
        claude)
          key_hint="ANTHROPIC_API_KEY"
          ;;
        copilot)
          key_hint="GITHUB_TOKEN"
          ;;
        *)
          key_hint="<runtime-secret-key>"
          ;;
      esac
      cat >&2 <<EOF
Example:
  kubectl -n ${namespace} create secret generic '${secret_name}' \\
    --from-literal='${key_hint}=<runtime-token>' \\
    --dry-run=client -o yaml | kubectl apply -f -
EOF
      ;;
    *git*)
      cat >&2 <<EOF
Example:
  kubectl -n ${namespace} create secret generic '${secret_name}' \\
    --from-literal='username=<git-username-or-oauth2>' \\
    --from-literal='password=<git-token>' \\
    --dry-run=client -o yaml | kubectl apply -f -
EOF
      ;;
    *)
      cat >&2 <<EOF
Example:
  kubectl -n ${namespace} create secret generic '${secret_name}' \\
    --from-literal='<key>=<value>' \\
    --dry-run=client -o yaml | kubectl apply -f -
EOF
      ;;
  esac
  return 1
}

resolve_demo_magic_path() {
  if [[ -n "${DEMO_MAGIC_PATH:-}" ]]; then
    if [[ -f "${DEMO_MAGIC_PATH}" ]]; then
      printf '%s\n' "${DEMO_MAGIC_PATH}"
      return 0
    fi
    log "Ignoring DEMO_MAGIC_PATH=${DEMO_MAGIC_PATH}; file does not exist"
  fi

  local candidate
  for candidate in \
    "${demo_lib_dir}/demo-magic.sh" \
    "${demo_dir}/demo-magic.sh" \
    "${repo_root}/hack/demo-magic.sh" \
    "${repo_root}/bin/demo-magic.sh" \
    "${repo_root}/demo-magic.sh" \
    "${HOME}/demo-magic.sh" \
    "${HOME}/demo-magic/demo-magic.sh" \
    "${HOME}/src/demo-magic/demo-magic.sh"; do
    if [[ -f "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

source_demo_magic() {
  local path
  local args=("$@")
  local restore_nounset=0
  local has_pv=1
  path="$(resolve_demo_magic_path)" || die "demo-magic.sh not found; set DEMO_MAGIC_PATH=/path/to/demo-magic.sh"
  if ! command -v pv >/dev/null 2>&1; then
    has_pv=0
    if ((${#args[@]})); then
      args=("-d" "${args[@]}")
    else
      args=("-d")
    fi
  fi
  if [[ $- == *u* ]]; then
    restore_nounset=1
    set +u
  fi
  # shellcheck disable=SC1090
  . "${path}" "${args[@]}"
  if [[ ! -t 0 || ! -t 1 ]]; then
    run_cmd() {
      eval "$@"
    }
  fi
  if (( ! has_pv )); then
    TYPE_SPEED=""
  fi
  if (( restore_nounset )); then
    set -u
  fi
}

configure_demo_magic() {
  if command -v pv >/dev/null 2>&1; then
    TYPE_SPEED="${TYPE_SPEED:-40}"
  else
    TYPE_SPEED=""
  fi
  # Terminal-style prompt: a single ">" with no path/host noise. Keeps the
  # cast looking like a real shell session rather than a branded demo.
  # Unconditional overwrite — demo-magic.sh sets its own default at source
  # time, so a `:-` fallback here would be a no-op. Callers wanting a
  # different prompt can set DEMO_PROMPT after calling configure_demo_magic.
  DEMO_PROMPT="${BOLD}${CYAN}> ${COLOR_RESET}"
  PROMPT_TIMEOUT="${PROMPT_TIMEOUT:-0}"
}

ensure_demo_workdir() {
  mkdir -p "${DEMO_WORKDIR}"
}

prepare_api_env() {
  export ORKA_SERVER="${ORKA_API_BASE}"
  if [[ -z "${ORKA_TOKEN:-}" || "${ORKA_TOKEN_MANAGED:-0}" == "1" ]]; then
    export ORKA_TOKEN_MANAGED=1
  fi
  export ORKA_TOKEN="$(get_orka_token)"
}

orka_api_base_host_port() {
  local without_scheme="${ORKA_API_BASE#*://}"
  printf '%s\n' "${without_scheme%%/*}"
}

orka_api_base_host() {
  local host_port
  host_port="$(orka_api_base_host_port)"
  if [[ "${host_port}" == *:* ]]; then
    printf '%s\n' "${host_port%:*}"
  else
    printf '%s\n' "${host_port}"
  fi
}

orka_api_base_port() {
  local host_port
  host_port="$(orka_api_base_host_port)"
  if [[ "${host_port}" == *:* ]]; then
    printf '%s\n' "${host_port##*:}"
  elif [[ "${ORKA_API_BASE}" == https://* ]]; then
    printf '%s\n' "443"
  else
    printf '%s\n' "80"
  fi
}

orka_api_base_is_local() {
  local host
  host="$(orka_api_base_host)"
  [[ "${host}" == "127.0.0.1" || "${host}" == "localhost" ]]
}

orka_api_port_forward_command() {
  printf 'kubectl -n %s port-forward svc/%s %s:8080\n' \
    "${ORKA_NAMESPACE}" \
    "${ORKA_API_SERVICE_NAME}" \
    "$(orka_api_base_port)"
}

orka_api_port_forward_pid_file() {
  printf '%s/orka-api-port-forward.pid\n' "${DEMO_WORKDIR}"
}

orka_api_port_forward_log_file() {
  printf '%s/orka-api-port-forward.log\n' "${DEMO_WORKDIR}"
}

wait_for_orka_api_reachable() {
  local timeout_seconds="${1:-10}"
  local deadline
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if curl -fsS --max-time "${DEMO_API_CHECK_TIMEOUT:-2}" "${ORKA_SERVER}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  return 1
}

start_orka_api_port_forward() {
  local pid_file log_file pid local_port
  ensure_demo_workdir
  pid_file="$(orka_api_port_forward_pid_file)"
  log_file="$(orka_api_port_forward_log_file)"
  local_port="$(orka_api_base_port)"

  if [[ -s "${pid_file}" ]]; then
    pid="$(cat "${pid_file}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      return 0
    fi
  fi

  log "Starting Orka API port-forward on localhost:${local_port}"
  nohup kubectl -n "${ORKA_NAMESPACE}" port-forward "svc/${ORKA_API_SERVICE_NAME}" "${local_port}:8080" >"${log_file}" 2>&1 </dev/null &
  pid="$!"
  printf '%s\n' "${pid}" >"${pid_file}"
}

stop_orka_api_port_forward() {
  local pid_file pid
  pid_file="$(orka_api_port_forward_pid_file)"
  if [[ ! -s "${pid_file}" ]]; then
    return 0
  fi
  pid="$(cat "${pid_file}")"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${pid_file}"
}

require_orka_api_reachable() {
  if curl -fsS --max-time "${DEMO_API_CHECK_TIMEOUT:-2}" "${ORKA_SERVER}/healthz" >/dev/null 2>&1; then
    return 0
  fi

  if orka_api_base_is_local && [[ "${DEMO_AUTO_PORT_FORWARD}" == "1" ]]; then
    start_orka_api_port_forward
    if wait_for_orka_api_reachable "${DEMO_PORT_FORWARD_TIMEOUT:-12}"; then
      printf 'Orka API is reachable at %s\n' "${ORKA_SERVER}"
      printf 'port-forward pid: %s\n' "$(cat "$(orka_api_port_forward_pid_file)")"
      printf 'port-forward log: %s\n' "$(orka_api_port_forward_log_file)"
      return 0
    fi

    stop_orka_api_port_forward
    start_orka_api_port_forward
    if wait_for_orka_api_reachable "${DEMO_PORT_FORWARD_TIMEOUT:-12}"; then
      printf 'Orka API is reachable at %s\n' "${ORKA_SERVER}"
      printf 'port-forward pid: %s\n' "$(cat "$(orka_api_port_forward_pid_file)")"
      printf 'port-forward log: %s\n' "$(orka_api_port_forward_log_file)"
      return 0
    fi
  fi

  printf 'error: Orka API is not reachable at %s\n' "${ORKA_SERVER}" >&2
  if orka_api_base_is_local; then
    printf 'Start a local tunnel in another terminal, then rerun this demo:\n  %s\n' "$(orka_api_port_forward_command)" >&2
    if [[ -f "$(orka_api_port_forward_log_file)" ]]; then
      printf 'Last port-forward log:\n' >&2
      tail -20 "$(orka_api_port_forward_log_file)" >&2 || true
    fi
  else
    printf 'Set ORKA_API_BASE to a reachable Orka API URL.\n' >&2
  fi
  return 1
}

create_orka_service_account_token() {
  local namespace="$1"
  local token

  if [[ -n "${ORKA_TOKEN_DURATION:-}" && "${ORKA_TOKEN_DURATION}" != "0" && "${ORKA_TOKEN_DURATION}" != "0s" ]]; then
    if token="$(kubectl create token "${ORKA_TOKEN_SERVICE_ACCOUNT}" -n "${namespace}" --duration="${ORKA_TOKEN_DURATION}" 2>/dev/null)"; then
      printf '%s' "${token}"
      return 0
    fi
  fi

  kubectl create token "${ORKA_TOKEN_SERVICE_ACCOUNT}" -n "${namespace}"
}

get_orka_token() {
  if [[ -n "${ORKA_TOKEN:-}" && "${ORKA_TOKEN_MANAGED:-0}" != "1" ]]; then
    # Defend against a stale ORKA_TOKEN carried over from a previous shell
    # session (e.g., a token minted against a different cluster). We probe
    # the API once per process; on rejection we forget the pre-set token
    # and fall through to fresh minting.
    if [[ "${ORKA_TOKEN_VALIDATED:-0}" != "1" ]]; then
      local probe_url="${ORKA_SERVER:-${ORKA_API_BASE}}"
      local probe_code
      probe_code="$(curl -sS -o /dev/null -w '%{http_code}' \
        --max-time "${DEMO_API_CHECK_TIMEOUT:-3}" \
        -H "Authorization: Bearer ${ORKA_TOKEN}" \
        "${probe_url}/api/v1/version" 2>/dev/null || printf '000')"
      case "${probe_code}" in
        2*|3*)
          export ORKA_TOKEN_VALIDATED=1
          printf '%s' "${ORKA_TOKEN}"
          return 0
          ;;
        401|403)
          printf 'warning: pre-set ORKA_TOKEN rejected by %s (HTTP %s); minting a fresh token\n' \
            "${probe_url}" "${probe_code}" >&2
          unset ORKA_TOKEN
          ;;
        *)
          # Network error / API down. Return the caller's token for this attempt,
          # but do NOT mark it validated: require_orka_api_reachable may start the
          # local port-forward immediately after this, and the next call should
          # re-probe so stale tokens are rejected before real API calls run.
          printf '%s' "${ORKA_TOKEN}"
          return 0
          ;;
      esac
    else
      printf '%s' "${ORKA_TOKEN}"
      return 0
    fi
  fi
  if [[ -z "${ORKA_TOKEN_CACHE:-}" ]]; then
    if [[ -n "${ORKA_TOKEN_COMMAND:-}" ]]; then
      ORKA_TOKEN_CACHE="$(eval "${ORKA_TOKEN_COMMAND}")"
    else
      local token_namespace candidate
      token_namespace="${ORKA_TOKEN_NAMESPACE:-default}"
      if kubectl get serviceaccount "${ORKA_TOKEN_SERVICE_ACCOUNT}" -n "${token_namespace}" >/dev/null 2>&1; then
        ORKA_TOKEN_CACHE="$(create_orka_service_account_token "${token_namespace}")"
      else
        for candidate in default "${DEMO_NAMESPACE}" "${ORKA_NAMESPACE}"; do
          [[ -n "${candidate}" ]] || continue
          if kubectl get serviceaccount "${ORKA_TOKEN_SERVICE_ACCOUNT}" -n "${candidate}" >/dev/null 2>&1; then
            ORKA_TOKEN_CACHE="$(create_orka_service_account_token "${candidate}")"
            break
          fi
        done
      fi
      if [[ -z "${ORKA_TOKEN_CACHE:-}" ]]; then
        printf 'error: failed to create token for serviceaccount %s; set ORKA_TOKEN or ORKA_TOKEN_COMMAND\n' "${ORKA_TOKEN_SERVICE_ACCOUNT}" >&2
        return 1
      fi
    fi
  fi
  printf '%s' "${ORKA_TOKEN_CACHE}"
}

refresh_orka_token() {
  if [[ "${ORKA_TOKEN_MANAGED:-0}" == "1" ]]; then
    ORKA_TOKEN_CACHE=""
  fi
  export ORKA_TOKEN="$(get_orka_token)"
}



sandbox_session_claim_name() {
  local session_name="$1"
  local claim_namespace="${DEMO_SANDBOX_CLAIM_NAMESPACE:-${DEMO_NAMESPACE}}"
  local task_namespace="${DEMO_NAMESPACE}"
  local template_namespace="${DEMO_SANDBOX_TEMPLATE_NAMESPACE:-${DEMO_NAMESPACE}}"
  local template_ref="${DEMO_SANDBOX_TEMPLATE_REF:-orka-live-template}"
  local digest

  if command -v shasum >/dev/null 2>&1; then
    digest="$(
      printf '%s\0%s\0%s\0%s\0%s' \
        "${claim_namespace}" \
        "${task_namespace}" \
        "${template_namespace}" \
        "${template_ref}" \
        "${session_name}" \
        | shasum -a 256 | awk '{print $1}'
    )"
  elif command -v sha256sum >/dev/null 2>&1; then
    digest="$(
      printf '%s\0%s\0%s\0%s\0%s' \
        "${claim_namespace}" \
        "${task_namespace}" \
        "${template_namespace}" \
        "${template_ref}" \
        "${session_name}" \
        | sha256sum | awk '{print $1}'
    )"
  else
    die "shasum or sha256sum is required to compute the demo SandboxClaim name"
  fi

  printf 'orka-session-%.32s\n' "${digest}"
}

demo_anthropic_base_url() {
  printf '%s/anthropic\n' "${ORKA_API_BASE%/}"
}

demo_anthropic_model() {
  local claude_model="${DEMO_CLAUDE_MODEL:-}"
  local ai_model="${DEMO_AI_MODEL:-}"
  local provider_ref="${DEMO_PROVIDER_REF:-}"

  if [[ -n "${claude_model}" ]]; then
    printf '%s\n' "${claude_model}"
  elif [[ -z "${ai_model}" ]]; then
    printf '\n'
  elif [[ "${ai_model}" == */* || -z "${provider_ref}" ]]; then
    printf '%s\n' "${ai_model}"
  else
    printf '%s/%s\n' "${provider_ref}" "${ai_model}"
  fi
}

run_demo_chat_client() {
  case "${DEMO_CHAT_CLIENT}" in
    claude-code)
      run_demo_chat_client_claude_code
      ;;
    *)
      die "unsupported DEMO_CHAT_CLIENT=${DEMO_CHAT_CLIENT}; add a run_demo_chat_client implementation in hack/demos/lib/common.sh"
      ;;
  esac
}

run_demo_chat_request_file() {
  local request_file="$1"
  local output_file="${2:-${DEMO_WORKDIR}/chat-client-result.json}"

  DEMO_CLAUDE_OUTPUT_FILE="${output_file}" run_demo_chat_client < "${request_file}"
}

chat_client_result_is_blocked() {
  local result="$1"
  local lower
  lower="$(printf '%s' "${result}" | tr '[:upper:]' '[:lower:]')"

  case "${lower}" in
    *"can't complete"*|*"cannot complete"*|*"do not have"*|*"don't have"*|*"not available"*)
      return 0
      ;;
  esac

  [[ "${lower}" == *"unable to"* && "${lower}" == *"tool"* ]]
}

print_chat_client_json_summary() {
  local output_file="$1"
  local result session_id

  session_id="$(jq -r '.session_id // empty' "${output_file}")"
  result="$(jq -r '.result // empty' "${output_file}")"

  # Presenter audience wants the full audit trail (session id + on-disk path
  # for post-run inspection). Recording profiles get the human-readable
  # result body only — no UUIDs, no /tmp paths.
  if demo_profile_is presenter; then
    if [[ -n "${session_id}" ]]; then
      printf 'chat client session: %s\n' "${session_id}"
    fi
    printf 'debug response file: %s\n' "${output_file}"
  fi

  if [[ "${DEMO_SHOW_CHAT_CLIENT_RESULT}" == "1" && -n "${result}" ]]; then
    printf 'chat client summary:\n%s\n' "${result}"
  fi
}

run_demo_chat_client_claude_code() {
  local token model base_url
  token="$(get_orka_token)"
  model="$(demo_anthropic_model)"
  base_url="$(demo_anthropic_base_url)"

  # Claude Code merges user-level settings.json AFTER --settings, so an
  # `env.ANTHROPIC_BASE_URL` in ~/.claude/settings.json silently wins
  # over what we pass via flag (and over shell env). Workaround: point
  # the whole config dir at a per-run scratch dir via CLAUDE_CONFIG_DIR.
  # That dir holds ONLY our settings.json, so user-level env can't shadow.
  local settings_dir
  settings_dir="$(mktemp -d -t orka-demo-claude-cfg.XXXXXX)"
  chmod 700 "${settings_dir}"
  demo_register_cleanup_dir "${settings_dir}"
  # No secrets logged: the file is written 0600 below and removed when done.
  umask 077
  cat >"${settings_dir}/settings.json" <<JSON
{"permissions": {"defaultMode": "${DEMO_CLAUDE_PERMISSION_MODE}"},
 "env": {"ANTHROPIC_BASE_URL": "${base_url}", "ANTHROPIC_API_KEY": "${token}"}}
JSON

  local cmd=(
    "${DEMO_CLAUDE_BIN}"
    --bare
    -p
    --model "${model}"
    --no-session-persistence
    --permission-mode "${DEMO_CLAUDE_PERMISSION_MODE}"
    --output-format "${DEMO_CLAUDE_OUTPUT_FORMAT}"
  )

  if [[ -n "${DEMO_CLAUDE_TOOLS}" ]]; then
    cmd+=(--tools "${DEMO_CLAUDE_TOOLS}")
  fi

  local output_file remove_output status had_errexit
  output_file="${DEMO_CLAUDE_OUTPUT_FILE:-}"
  remove_output=0
  if [[ -z "${output_file}" ]]; then
    output_file="$(mktemp)"
    remove_output=1
  fi

  if [[ "${DEMO_CLAUDE_OUTPUT_FORMAT}" == "json" ]]; then
    had_errexit=0
    if [[ $- == *e* ]]; then
      had_errexit=1
      set +e
    fi
    CLAUDE_CONFIG_DIR="${settings_dir}" \
      ANTHROPIC_BASE_URL="${base_url}" ANTHROPIC_API_KEY="${token}" "${cmd[@]}" >"${output_file}"
    status="$?"
    if (( had_errexit )); then
      set -e
    fi
  else
    had_errexit=0
    if [[ $- == *e* ]]; then
      had_errexit=1
      set +e
    fi
    CLAUDE_CONFIG_DIR="${settings_dir}" \
      ANTHROPIC_BASE_URL="${base_url}" ANTHROPIC_API_KEY="${token}" "${cmd[@]}" | tee "${output_file}"
    status="${PIPESTATUS[0]}"
    if (( had_errexit )); then
      set -e
    fi
  fi

  # Scratch config dir held the per-run token; remove it now.
  rm -rf "${settings_dir}"

  if (( status != 0 )); then
    printf 'error: Claude Code exited with status %s\n' "${status}" >&2
    printf 'debug response file: %s\n' "${output_file}" >&2
    if (( remove_output )); then
      rm -f "${output_file}"
    fi
    return "${status}"
  fi

  if [[ "${DEMO_CLAUDE_OUTPUT_FORMAT}" == "json" && -s "${output_file}" ]] &&
    jq -e '(.is_error // false) == true' "${output_file}" >/dev/null 2>&1; then
    local result
    result="$(jq -r '.result // "Claude Code returned is_error=true"' "${output_file}")"
    printf 'error: Claude Code request failed: %s\n' "${result}" >&2
    if [[ "${result}" == *"ConnectionRefused"* || "${result}" == *"Unable to connect"* ]]; then
      printf 'Check ORKA_API_BASE=%s and ANTHROPIC_BASE_URL=%s.\n' "${ORKA_SERVER}" "${base_url}" >&2
      if orka_api_base_is_local; then
        printf 'For the default local setup, keep this running in another terminal:\n  %s\n' "$(orka_api_port_forward_command)" >&2
      fi
    fi
    if (( remove_output )); then
      rm -f "${output_file}"
    fi
    return 1
  fi

  if [[ "${DEMO_CLAUDE_OUTPUT_FORMAT}" == "json" && -s "${output_file}" ]]; then
    local result
    result="$(jq -r '.result // empty' "${output_file}")"
    if chat_client_result_is_blocked "${result}"; then
      printf 'error: chat client did not start the workflow. Raw response saved for debugging: %s\n' "${output_file}" >&2
      if (( remove_output )); then
        rm -f "${output_file}"
      fi
      return 1
    fi
    print_chat_client_json_summary "${output_file}"
  fi

  if (( remove_output )); then
    rm -f "${output_file}"
  fi
  return "${status}"
}

orka_api() {
  local method="$1"
  local path="$2"
  local body_file="${3:-}"
  local token status attempt

  for attempt in 1 2 3; do
    token="$(get_orka_token)"
    local args=(
      -fsS
      -X "${method}"
      -H "Authorization: Bearer ${token}"
      -H "Accept: application/json"
    )
    if [[ -n "${body_file}" ]]; then
      args+=(-H "Content-Type: application/json" --data-binary "@${body_file}")
    fi

    if curl "${args[@]}" "${ORKA_API_BASE}${path}"; then
      return 0
    fi
    status="$?"

    if [[ "${status}" == "22" && "${attempt}" -lt 3 && "${ORKA_TOKEN_MANAGED:-0}" == "1" ]]; then
      refresh_orka_token
      continue
    fi

    # curl exit 7 = couldn't connect: on long runs the auto port-forward can die
    # (idle timeout / kube-apiserver hiccup), which would spuriously fail an
    # otherwise-successful demo's final assertion. Re-establish the tunnel and
    # retry before giving up. Only meaningful for the local-tunnel path.
    if [[ "${status}" == "7" && "${attempt}" -lt 3 ]] \
        && declare -f require_orka_api_reachable >/dev/null 2>&1; then
      printf 'orka_api: connection failed; re-establishing API tunnel and retrying…\n' >&2
      require_orka_api_reachable >/dev/null 2>&1 || true
      continue
    fi

    return "${status}"
  done
}


summarize_preflight() {
  jq -n \
    --arg namespace "${DEMO_NAMESPACE}" \
    --arg controllerNamespace "${ORKA_NAMESPACE}" \
    --arg api "${ORKA_SERVER}" \
    --arg anthropic "$(demo_anthropic_base_url)" \
    --arg model "$(demo_anthropic_model)" \
    --arg workdir "${DEMO_WORKDIR}" \
    '{
      namespace: $namespace,
      controllerNamespace: $controllerNamespace,
      api: $api,
      anthropicEndpoint: $anthropic,
      model: $model,
      workdir: $workdir,
      status: "ready"
    }'
}


assert_real_pr_result() {
  local task_name="$1"
  local task_json result_json children_json result pr_url failed_children task_phase

  # Chat-session sentinel: the chat-driven coordinator lives in the chat
  # tool loop, not a Task. Validate from the chat-client-result.json file.
  if [[ "${task_name}" == "chat-session" ]]; then
    local chat_file="${DEMO_WORKDIR}/chat-client-result.json"
    if [[ ! -s "${chat_file}" ]]; then
      printf 'error: chat client result file missing or empty: %s\n' "${chat_file}" >&2
      return 1
    fi
    local is_error
    is_error="$(jq -r '.is_error // false' "${chat_file}" 2>/dev/null || printf 'true')"
    result="$(jq -r '.result // ""' "${chat_file}" 2>/dev/null || printf '')"
    if [[ "${is_error}" == "true" ]]; then
      printf 'error: chat session ended with an error result:\n%s\n' "${result}" >&2
      return 1
    fi
    if [[ -z "${result}" ]]; then
      printf 'error: chat session produced an empty result\n' >&2
      return 1
    fi
    if printf '%s\n' "${result}" | grep -Eiq 'implementation failed|did not create a pull request|did not open a pull request|pull request[^\n]*(not created|not opened)|PR:[[:space:]]*not created|not create a pull request|no pull request|VALIDATION_CONFIG_BLOCKED|VALIDATION_BLOCKED|REVIEW_BLOCKED|CI_BLOCKED|CI_PENDING|CI_NO_CHECKS|CI_CLOSED|CI_UNKNOWN|status[=:][[:space:]]*(no_checks|closed|unknown)|CI:[[:space:]]*(no_checks|closed|unknown)'; then
      printf 'error: chat session result is not a successful PR handoff:\n%s\n' "${result}" >&2
      return 1
    fi
    if printf '%s\n' "${result}" | grep -Eq '(^|[^[:alnum:]_])(FAILED|BLOCKED)([^[:alnum:]_]|$)'; then
      printf 'error: chat session result contains a failure/blocker marker:\n%s\n' "${result}" >&2
      return 1
    fi
    pr_url="$(printf '%s\n' "${result}" | grep -Eo "https://github[.]com/[^[:space:])\"'<>]+/[^[:space:])\"'<>]+/pull/[0-9]+" | head -n 1 || true)"
    if [[ -z "${pr_url}" ]]; then
      printf 'error: chat session result does not contain a GitHub pull request URL:\n%s\n' "${result}" >&2
      return 1
    fi
    children_json="$(kubectl get tasks -n "${DEMO_NAMESPACE}" -l 'orka.ai/source=anthropic-proxy' -o json 2>/dev/null || printf '{"items":[]}')"
    failed_children="$(jq -r --arg started "${DEMO_CHAT_STARTED_AT:-}" '
      (.items // [])[]?
      | select($started == "" or ((.metadata.creationTimestamp // "") >= $started))
      | (.status.phase // "Unknown") as $phase
      | select(["Failed", "Cancelled", "Canceled", "Error"] | index($phase))
      | "\(.metadata.name) agent=\(.spec.agentRef.name // "-") phase=\($phase)"
    ' <<<"${children_json}")"
    if [[ -n "${failed_children}" ]]; then
      if printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?Validation([[:space:]]+status)?):[[:space:]]*(PASS(ED)?|GREEN)([^[:alnum:]_]|$)' \
        && printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?Review(ers)?([[:space:]]+status)?):[[:space:]]*([0-9]+[[:space:]]+)?(LGTM|APPROVED)([^[:alnum:]_]|$)' \
        && printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?CI([[:space:]]+status)?):[[:space:]]*(PASS(ED)?|SUCCESS|GREEN)([^[:alnum:]_]|$)'; then
        local failed_count
        failed_count="$(printf '%s\n' "${failed_children}" | grep -c . || true)"
        printf '🔁 self-healed %d intermediate proxy child failure(s) before success\n' "${failed_count}" >&2
      else
        printf 'error: chat session has failed proxy child tasks without final pass evidence; refusing to treat result as demo success:\n%s\n' "${failed_children}" >&2
        return 1
      fi
    fi
    printf 'validated pull request handoff: %s\n' "${pr_url}"
    return 0
  fi

  task_json="$(orka_api GET "/api/v1/tasks/${task_name}?namespace=${DEMO_NAMESPACE}")" || {
    printf 'error: failed to fetch task %s\n' "${task_name}" >&2
    return 1
  }
  task_phase="$(jq -r '.status.phase // "Unknown"' <<<"${task_json}")"
  if [[ "${task_phase}" != "Succeeded" ]]; then
    printf 'error: task %s is %s; expected Succeeded before accepting a PR handoff\n' "${task_name}" "${task_phase}" >&2
    return 1
  fi

  result_json="$(orka_api GET "/api/v1/tasks/${task_name}/result?namespace=${DEMO_NAMESPACE}")" || {
    printf 'error: failed to fetch result for task %s\n' "${task_name}" >&2
    return 1
  }
  result="$(jq -r '(.result // "") | tostring' <<<"${result_json}")"
  if [[ -z "${result}" ]]; then
    printf 'error: task %s has an empty result; expected a GitHub PR handoff\n' "${task_name}" >&2
    return 1
  fi

  if printf '%s\n' "${result}" | grep -Eiq 'implementation failed|did not create a pull request|did not open a pull request|pull request[^\n]*(not created|not opened)|PR:[[:space:]]*not created|not create a pull request|no pull request|VALIDATION_CONFIG_BLOCKED|VALIDATION_BLOCKED|REVIEW_BLOCKED|CI_BLOCKED|CI_PENDING|CI_NO_CHECKS|CI_CLOSED|CI_UNKNOWN|status[=:][[:space:]]*(no_checks|closed|unknown)|CI:[[:space:]]*(no_checks|closed|unknown)'; then
    printf 'error: task %s result is not a successful PR handoff:\n%s\n' "${task_name}" "${result}" >&2
    return 1
  fi
  if printf '%s\n' "${result}" | grep -Eq '(^|[^[:alnum:]_])(FAILED|BLOCKED)([^[:alnum:]_]|$)'; then
    printf 'error: task %s result contains a failure/blocker marker:\n%s\n' "${task_name}" "${result}" >&2
    return 1
  fi

  pr_url="$(printf '%s\n' "${result}" | grep -Eo "https://github[.]com/[^[:space:])\"'<>]+/[^[:space:])\"'<>]+/pull/[0-9]+" | head -n 1 || true)"
  if [[ -z "${pr_url}" ]]; then
    printf 'error: task %s result does not contain a GitHub pull request URL:\n%s\n' "${task_name}" "${result}" >&2
    return 1
  fi

  children_json="$(orka_api GET "/api/v1/tasks/${task_name}/children?namespace=${DEMO_NAMESPACE}" 2>/dev/null || printf '{"items":[]}')"
  failed_children="$(jq -r '
    (.items // [])[]?
    | (.status.phase // "Unknown") as $phase
    | select(["Failed", "Cancelled", "Canceled", "Error"] | index($phase))
    | "\(.metadata.name) agent=\(.spec.agentRef.name // "-") phase=\($phase)"
  ' <<<"${children_json}")"
  if [[ -n "${failed_children}" ]]; then
    # Auto-retry keeps historical failed children, so accept them only when the
    # terminal parent result has explicit final pass evidence. Support both the
    # older "Final validation status: PASSED" format and the current handoff
    # format:
    #   Final status:
    #   - Validation: PASSED
    #   - Review: APPROVED ...
    #   - CI: PASSED | NO_CHECKS | NOT_APPLICABLE | NONE
    #
    # CI may legitimately report no checks if the target repo's CI workflow is
    # absent or hasn't run yet — that doesn't invalidate a Validation/Review
    # pass, so accept the NO_CHECKS family as final-pass evidence too.
    if printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?Validation([[:space:]]+status)?):[[:space:]]*PASS(ED)?([^[:alnum:]_]|$)' \
      && printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?Review([[:space:]]+status)?):[[:space:]]*APPROVED([^[:alnum:]_]|$)' \
      && printf '%s\n' "${result}" | grep -Eiq '(^|[[:space:]-])((Final[[:space:]]+)?CI([[:space:]]+status)?):[[:space:]]*(PASS(ED)?|NO_CHECKS|NOT_APPLICABLE|NONE)([^[:alnum:]_]|$)'; then
      # Viewer-friendly note: present the recovery as "self-healed", with
      # the gory child names only when the presenter wants them.
      local failed_count
      failed_count="$(printf '%s\n' "${failed_children}" | grep -c . || true)"
      if declare -F demo_profile_is >/dev/null 2>&1 && demo_profile_is presenter; then
        printf 'note: task %s had recovered intermediate child failures:\n%s\n' "${task_name}" "${failed_children}" >&2
      else
        printf '🔁 self-healed %d intermediate child failure(s) before success\n' "${failed_count}" >&2
      fi
    else
      printf 'error: task %s has failed child tasks without final pass evidence; refusing to treat result as demo success:\n%s\n' "${task_name}" "${failed_children}" >&2
      return 1
    fi
  fi

  printf 'validated pull request handoff: %s\n' "${pr_url}"
}

summarize_task_run() {
  local task_name="$1"
  local demo_name="${2:-task-run}"
  local task_json children_json result_json

  # Chat-session sentinel: emit a summary derived from the chat result file
  # and live cluster Agents/Tasks. payoff_card_pr extracts the PR URL from
  # .result; the rest is best-effort context for the audit-mode viewer.
  if [[ "${task_name}" == "chat-session" ]]; then
    local chat_file="${DEMO_WORKDIR}/chat-client-result.json"
    local chat_json="{}"
    if [[ -s "${chat_file}" ]]; then
      chat_json="$(cat "${chat_file}")"
    fi
    children_json="$(kubectl get tasks -n "${DEMO_NAMESPACE}" \
                      -l "orka.ai/source=anthropic-proxy" -o json 2>/dev/null \
                      || printf '{"items":[]}')"
    jq -n \
      --arg demo "${demo_name}" \
      --arg started_at "${DEMO_CHAT_STARTED_AT:-}" \
      --argjson chat "${chat_json}" \
      --argjson children "${children_json}" \
      '{
        demo: $demo,
        task: "chat-session",
        phase: (if ($chat.is_error // false) then "Failed" else "Succeeded" end),
        agent: "chat-coordinator",
        job: null,
        childTasks: (
          ($children.items // [])
          | map(select(($started_at == "") or (.metadata.creationTimestamp >= $started_at)))
          | length
        ),
        children: (
          ($children.items // [])
          | map(select(($started_at == "") or (.metadata.creationTimestamp >= $started_at)))
          | map({
              name: .metadata.name,
              agent: (.spec.agentRef.name // null),
              phase: (.status.phase // "Unknown")
            })
        ),
        result: (($chat.result // "") | tostring | .[0:1200])
      }'
    return 0
  fi

  task_json="$(kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o json)"
  children_json="$(orka_api GET "/api/v1/tasks/${task_name}/children?namespace=${DEMO_NAMESPACE}" 2>/dev/null || printf '{"items":[]}')"
  result_json="$(orka_api GET "/api/v1/tasks/${task_name}/result?namespace=${DEMO_NAMESPACE}" 2>/dev/null || printf '{}')"

  jq -n \
    --arg demo "${demo_name}" \
    --argjson task "${task_json}" \
    --argjson children "${children_json}" \
    --argjson result "${result_json}" \
    '{
      demo: $demo,
      task: $task.metadata.name,
      phase: ($task.status.phase // "Unknown"),
      agent: ($task.spec.agentRef.name // null),
      job: ($task.status.jobName // null),
      childTasks: (($children.items // []) | length),
      children: (($children.items // []) | map({
        name: .metadata.name,
        agent: (.spec.agentRef.name // null),
        phase: (.status.phase // "Unknown")
      })),
      result: (($result.result // "") | tostring | .[0:1200])
    }'
}

summarize_security_run() {
  local finding_id="$1"
  local pr_file="${2:-}"
  local repo_json patches_json pr_json

  repo_json="$(orka_api GET "/api/v1/security/repositories/${DEMO_SECURITY_SCAN_NAME}?namespace=${DEMO_NAMESPACE}" 2>/dev/null || printf '{}')"
  patches_json="$(orka_api GET "/api/v1/security/findings/${finding_id}/patches?namespace=${DEMO_NAMESPACE}" 2>/dev/null || printf '{"items":[]}')"
  if [[ -n "${pr_file}" && -s "${pr_file}" ]]; then
    pr_json="$(cat "${pr_file}")"
  else
    pr_json="{}"
  fi

  jq -n \
    --arg finding "${finding_id}" \
    --argjson repo "${repo_json}" \
    --argjson patches "${patches_json}" \
    --argjson pr "${pr_json}" \
    '{
      demo: "security-remediation",
      repositoryScan: ($repo.metadata.name // null),
      scanPhase: ($repo.status.phase // null),
      finding: $finding,
      patches: (($patches.items // []) | map({
        id,
        status,
        branch,
        taskName
      })),
      pullRequest: {
        status: ($pr.status // null),
        number: ($pr.prNumber // $pr.number // null),
        url: ($pr.prURL // $pr.html_url // null)
      }
    }'
}

demo_label_selector() {
  printf 'demo.orka.ai/name=%s' "${DEMO_LABEL_VALUE}"
}

task_exists() {
  kubectl get task "$1" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1
}

agent_exists() {
  kubectl get agent "$1" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1
}

repository_scan_exists() {
  kubectl get repositoryscan "$1" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1
}

delete_task_if_exists() {
  kubectl delete task "$1" -n "${DEMO_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
}

delete_agent_if_exists() {
  kubectl delete agent "$1" -n "${DEMO_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
}

# delete_demo_chat_agents_if_present removes Agents that the chat-driven
# coordinator created in a prior run of demo 10. The chat path no longer
# pre-creates Agents with fixed demo names — the coordinator invents them —
# so we delete by label (orka.ai/created-by=chat) and by the fixed names
# from prior versions of the demo, ignoring missing resources.
delete_demo_chat_agents_if_present() {
  # New shape: Agents created via the chat `create_agent` tool get this label.
  local names
  names="$(
    kubectl get agents -n "${DEMO_NAMESPACE}" \
      -l 'orka.ai/created-by=chat' \
      -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || printf ''
  )"
  if [[ -n "${names}" ]]; then
    # shellcheck disable=SC2086
    kubectl delete agent -n "${DEMO_NAMESPACE}" ${names} --ignore-not-found >/dev/null 2>&1 || true
  fi
  # Legacy shape: Agents the old demo pre-created by name.
  delete_agent_if_exists "${DEMO_PR_COORDINATOR_NAME}"
  delete_agent_if_exists "${DEMO_CODER_AGENT_NAME}"
  delete_agent_if_exists "${DEMO_SECURITY_REVIEWER_NAME}"
  delete_agent_if_exists "${DEMO_QUALITY_REVIEWER_NAME}"
}

delete_repository_scan_if_exists() {
  kubectl delete repositoryscan "$1" -n "${DEMO_NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
}

delete_repository_scans_by_name_prefix() {
  local prefix="$1"
  local names
  names="$(
    kubectl get repositoryscans -n "${DEMO_NAMESPACE}" -o json 2>/dev/null \
      | jq -r --arg prefix "${prefix}" '.items[] | select(.metadata.name | startswith($prefix)) | .metadata.name'
  )"
  if [[ -n "${names}" ]]; then
    while IFS= read -r name; do
      [[ -n "${name}" ]] || continue
      kubectl delete repositoryscan "${name}" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1 || true
    done <<< "${names}"
  fi
}

delete_tasks_by_selector() {
  local selector="$1"
  local names
  names="$(kubectl get tasks -n "${DEMO_NAMESPACE}" -l "${selector}" -o name 2>/dev/null || true)"
  if [[ -n "${names}" ]]; then
    kubectl delete -n "${DEMO_NAMESPACE}" ${names} >/dev/null 2>&1 || true
  fi
}

delete_tasks_by_name_prefix() {
  local prefix="$1"
  local names
  names="$(
    kubectl get tasks -n "${DEMO_NAMESPACE}" -o json 2>/dev/null \
      | jq -r --arg prefix "${prefix}" '.items[] | select(.metadata.name | startswith($prefix)) | .metadata.name'
  )"
  if [[ -n "${names}" ]]; then
    while IFS= read -r name; do
      [[ -n "${name}" ]] || continue
      kubectl delete task "${name}" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1 || true
    done <<< "${names}"
  fi
}

latest_task_by_selector() {
  local selector="$1"
  kubectl get tasks -n "${DEMO_NAMESPACE}" -l "${selector}" -o json 2>/dev/null \
    | jq -r '.items | sort_by(.metadata.creationTimestamp) | last? | .metadata.name // empty'
}

delete_tasks_by_session_ref_prefix() {
  local prefix="$1"
  local names
  names="$(
    kubectl get tasks -n "${DEMO_NAMESPACE}" -o json 2>/dev/null |
      jq -r --arg prefix "${prefix}" '.items[] | select((.spec.sessionRef.name // "") | startswith($prefix)) | .metadata.name'
  )"
  if [[ -n "${names}" ]]; then
    while IFS= read -r name; do
      [[ -n "${name}" ]] || continue
      kubectl delete task "${name}" -n "${DEMO_NAMESPACE}" >/dev/null 2>&1 || true
    done <<< "${names}"
  fi
}

delete_chat_session_tasks() {
  delete_tasks_by_selector "orka.ai/chat-session=${DEMO_CHAT_SESSION}"
  delete_tasks_by_session_ref_prefix "${DEMO_CHAT_SESSION}"
}

latest_chat_parent_task() {
  local started_at="${1:-}"
  local parent

  # New chat-coordinator flow: the "coordinator" lives in the chat session
  # itself (the proxy's tool loop), not as a Kubernetes Task. When the chat
  # client has produced its result file, treat the chat session as the
  # parent — assert_real_pr_result / payoff_card_pr / wait_for_task_succeeded
  # / wait_for_task_result_available all special-case the "chat-session"
  # sentinel and read from chat-client-result.json.
  if [[ -n "${DEMO_WORKDIR:-}" && -s "${DEMO_WORKDIR}/chat-client-result.json" ]]; then
    printf 'chat-session\n'
    return 0
  fi

  parent="$(
    kubectl get tasks -n "${DEMO_NAMESPACE}" -l "orka.ai/source=anthropic-proxy" -o json 2>/dev/null |
      jq -r \
        --arg session "${DEMO_CHAT_SESSION}" \
        --arg agent "${DEMO_PR_COORDINATOR_NAME}" \
        '.items
         | map(select((.spec.sessionRef.name // "") == $session)
               | select((.spec.agentRef.name // "") == $agent))
         | sort_by(.metadata.creationTimestamp)
         | last?
         | .metadata.name // empty'
  )"
  if [[ -n "${parent}" ]]; then
    printf '%s\n' "${parent}"
    return 0
  fi

  kubectl get tasks -n "${DEMO_NAMESPACE}" -l "orka.ai/source=anthropic-proxy" -o json 2>/dev/null |
    jq -r \
      --arg started_at "${started_at}" \
      --arg agent "${DEMO_PR_COORDINATOR_NAME}" \
      '.items
       | map(select(($started_at == "") or (.metadata.creationTimestamp >= $started_at))
             | select((.spec.agentRef.name // "") == $agent))
       | sort_by(.metadata.creationTimestamp)
       | last?
       | .metadata.name // empty'
}

wait_for_chat_parent_task() {
  local timeout_seconds="${1:-120}"
  local started_at="${2:-${DEMO_CHAT_STARTED_AT:-}}"
  local deadline parent
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    parent="$(latest_chat_parent_task "${started_at}")"
    if [[ -n "${parent}" ]]; then
      printf '%s\n' "${parent}"
      return 0
    fi
    sleep 2
  done

  return 1
}

latest_scheduled_child_task() {
  latest_task_by_selector "orka.ai/parent-task=${DEMO_CRON_TASK_NAME},orka.ai/scheduled-run=true"
}

__demo_wait_emit_status() {
  # Emit a one-line "phase=X children=N latest=child/Phase elapsed=Zs" status
  # to stderr so callers using $() capture on stdout still see clean output.
  # Uses \r in-place rewrite when stderr is a tty; falls back to newlines
  # so log scrapers and non-tty runs still get readable output.
  #
  # If DEMO_WAIT_STATUS_HOOK is set to a defined function name, that hook
  # is called instead (with task_name, elapsed). The hook is expected to
  # use demo_announce_once / __demo_heartbeat itself. This lets each demo
  # narrate state transitions in domain-specific terms (threat-model →
  # discovery → validation in demo 40, parent → child fan-out in demo 10,
  # etc) instead of the generic phase=X children=N line.
  local task_name="$1"
  local elapsed="$2"
  if [[ "${DEMO_WAIT_QUIET:-0}" == "1" ]] || demo_profile_is hero; then
    return 0
  fi
  if [[ -n "${DEMO_WAIT_STATUS_HOOK:-}" ]] && declare -F "${DEMO_WAIT_STATUS_HOOK}" >/dev/null 2>&1; then
    "${DEMO_WAIT_STATUS_HOOK}" "${task_name}" "${elapsed}"
    return 0
  fi
  local phase children latest_child latest_phase latest_label
  phase="$(kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [[ -z "${phase}" ]] && phase="Pending"
  children="$(kubectl get tasks -n "${DEMO_NAMESPACE}" -l "orka.ai/parent-task=${task_name}" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  latest_child="$(kubectl get tasks -n "${DEMO_NAMESPACE}" \
                    -l "orka.ai/parent-task=${task_name}" \
                    --sort-by=.metadata.creationTimestamp \
                    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)"
  latest_phase="$(kubectl get tasks -n "${DEMO_NAMESPACE}" \
                    -l "orka.ai/parent-task=${task_name}" \
                    --sort-by=.metadata.creationTimestamp \
                    -o jsonpath='{.items[-1:].status.phase}' 2>/dev/null || true)"
  latest_label=""
  if [[ -n "${latest_child}" ]]; then
    latest_label=" latest=${latest_child}/${latest_phase:-Pending}"
  fi
  local children_field=""
  if (( children > 0 )); then
    children_field=" children=${children}"
  fi
  __demo_heartbeat '%s phase=%s%s%s elapsed=%ss' \
    "${task_name}" "${phase}" "${children_field}" "${latest_label}" "${elapsed}"
}

wait_for_task_terminal() {
  local task_name="$1"
  local timeout_seconds="${2:-900}"
  local deadline phase elapsed start
  start="${SECONDS}"
  deadline=$((SECONDS + timeout_seconds))
  # Adaptive tick: tight cadence for the first minute so viewers see the
  # workflow start, then back off to 30s so long coordinator runs don't
  # spam the cast with 100+ heartbeat lines. DEMO_WAIT_TICK_SECONDS still
  # overrides if the caller pins a value.
  local tick_initial tick_slow
  tick_initial="${DEMO_WAIT_TICK_SECONDS:-5}"
  tick_slow="${DEMO_WAIT_SLOW_TICK_SECONDS:-30}"
  (( tick_initial < 1 )) && tick_initial=1
  (( tick_slow < tick_initial )) && tick_slow="${tick_initial}"

  while (( SECONDS < deadline )); do
    phase="$(kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Succeeded|Failed|Cancelled)
        # Newline so the next log line lands on a fresh row after \r updates.
        if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
          printf '\n' >&2
        fi
        printf '%s\n' "${phase}"
        return 0
        ;;
    esac
    elapsed=$(( SECONDS - start ))
    __demo_wait_emit_status "${task_name}" "${elapsed}"
    if (( elapsed < 60 )); then
      sleep "${tick_initial}"
    else
      sleep "${tick_slow}"
    fi
  done

  if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
    printf '\n' >&2
  fi
  return 1
}

wait_for_task_succeeded() {
  local task_name="$1"
  local timeout_seconds="${2:-900}"
  local phase

  if [[ "${task_name}" == "chat-session" ]]; then
    local chat_file="${DEMO_WORKDIR}/chat-client-result.json"
    if [[ -s "${chat_file}" ]]; then
      local is_error
      is_error="$(jq -r '.is_error // false' "${chat_file}" 2>/dev/null || printf 'true')"
      if [[ "${is_error}" != "true" ]]; then
        return 0
      fi
      printf 'chat session ended with is_error=true\n' >&2
      return 1
    fi
    printf 'chat session result file missing: %s\n' "${chat_file}" >&2
    return 1
  fi

  phase="$(wait_for_task_terminal "${task_name}" "${timeout_seconds}")" || return 1
  if [[ "${phase}" == "Succeeded" ]]; then
    return 0
  fi

  printf 'task %s finished with phase %s\n' "${task_name}" "${phase}" >&2
  return 1
}

wait_for_first_scheduled_child() {
  local timeout_seconds="${1:-180}"
  local deadline child
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    child="$(latest_scheduled_child_task)"
    if [[ -n "${child}" ]]; then
      printf '%s\n' "${child}"
      return 0
    fi
    sleep 5
  done

  return 1
}

follow_task_logs_when_ready() {
  local task_name="$1"
  local timeout_seconds="${2:-180}"
  local deadline job_name pod_phase status
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    job_name="$(kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o jsonpath='{.status.jobName}' 2>/dev/null || true)"
    status="$(kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"

    if [[ -n "${job_name}" ]]; then
      pod_phase="$(kubectl get pods -n "${DEMO_NAMESPACE}" -l "job-name=${job_name}" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)"
      case "${pod_phase}" in
        Running|Succeeded|Failed)
          local pod_name
          pod_name="$(kubectl get pods -n "${DEMO_NAMESPACE}" -l "job-name=${job_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
          if [[ -z "${pod_name}" ]]; then
            break
          fi
          kubectl logs -n "${DEMO_NAMESPACE}" -f "pod/${pod_name}" --all-containers=true
          return $?
          ;;
      esac
    fi

    if [[ "${status}" == "Failed" || "${status}" == "Cancelled" ]]; then
      if [[ -n "${job_name}" ]]; then
        local failed_pod
        failed_pod="$(kubectl get pods -n "${DEMO_NAMESPACE}" -l "job-name=${job_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
        if [[ -n "${failed_pod}" ]]; then
          kubectl logs -n "${DEMO_NAMESPACE}" "pod/${failed_pod}" --all-containers=true >&2 || true
        fi
      fi
      return 1
    fi

    sleep 2
  done

  printf '%s\n' "timed out waiting to stream logs for task ${task_name}" >&2
  return 1
}

task_has_kubernetes_result() {
  local task_name="$1"

  kubectl get task "${task_name}" -n "${DEMO_NAMESPACE}" -o json 2>/dev/null |
    jq -e '
      (.status.resultRef.available == true)
      or
      ((.status.phase == "Succeeded") and ((.status.result // "") | tostring | length > 0))
    ' >/dev/null
}

wait_for_task_result_available() {
  local task_name="$1"
  local timeout_seconds="${2:-120}"
  local deadline
  deadline=$((SECONDS + timeout_seconds))

  if [[ "${task_name}" == "chat-session" ]]; then
    local chat_file="${DEMO_WORKDIR}/chat-client-result.json"
    while (( SECONDS < deadline )); do
      if [[ -s "${chat_file}" ]] && jq -e '.result // empty' "${chat_file}" >/dev/null 2>&1; then
        return 0
      fi
      sleep 1
    done
    return 1
  fi

  while (( SECONDS < deadline )); do
    if orka_api GET "/api/v1/tasks/${task_name}/result?namespace=${DEMO_NAMESPACE}" >/dev/null 2>&1; then
      return 0
    fi
    if task_has_kubernetes_result "${task_name}"; then
      return 0
    fi
    sleep 2
  done

  return 1
}

wait_for_repository_scan_ready() {
  local scan_name="$1"
  local timeout_seconds="${2:-1800}"
  local deadline phase start elapsed
  start="${SECONDS}"
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    phase="$(kubectl get repositoryscan "${scan_name}" -n "${DEMO_NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Ready)
        if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
          printf '\n' >&2
        fi
        return 0
        ;;
      Error)
        if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
          printf '\n' >&2
        fi
        return 1
        ;;
    esac
    elapsed=$(( SECONDS - start ))
    if [[ -n "${DEMO_WAIT_STATUS_HOOK:-}" ]] && declare -F "${DEMO_WAIT_STATUS_HOOK}" >/dev/null 2>&1; then
      "${DEMO_WAIT_STATUS_HOOK}" "${scan_name}" "${elapsed}"
    else
      __demo_heartbeat 'scan/%s phase=%s elapsed=%ss' \
        "${scan_name}" "${phase:-Pending}" "${elapsed}"
    fi
    sleep 10
  done

  if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
    printf '\n' >&2
  fi
  return 1
}

first_security_finding_id() {
  orka_api GET "/api/v1/security/repositories/${DEMO_SECURITY_SCAN_NAME}/findings?namespace=${DEMO_NAMESPACE}&state=open&limit=100" \
    | jq -r '
        def severity_rank:
          (. // "" | ascii_downcase) as $severity
          | if $severity == "critical" then 4
            elif $severity == "high" then 3
            elif $severity == "medium" then 2
            elif $severity == "low" then 1
            else 0
            end;
        (.items // [])
        | sort_by([(.severity | severity_rank), .id])
        | (last? | .id) // empty
      '
}

wait_for_first_security_finding() {
  local timeout_seconds="${1:-1800}"
  local deadline finding_id start elapsed
  start="${SECONDS}"
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    finding_id="$(first_security_finding_id)"
    if [[ -n "${finding_id}" ]]; then
      if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
        printf '\n' >&2
      fi
      printf '%s\n' "${finding_id}"
      return 0
    fi
    elapsed=$(( SECONDS - start ))
    __demo_heartbeat 'awaiting first finding from %s scan elapsed=%ss' \
      "${DEMO_SECURITY_SCAN_NAME:-scan}" "${elapsed}"
    sleep 10
  done

  if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
    printf '\n' >&2
  fi
  return 1
}

# wait_for_job_with_progress <job-name> <namespace> <timeout-seconds> <expect>
# expect = "complete"   → return 0 on Complete=True, 1 on Failed=True
# expect = "fail"       → return 0 on Failed=True,   1 on Complete=True
# Emits a per-tick status line to stderr (in-place on tty, newlines off-tty)
# showing the latest Pod phase so the viewer can see what's happening.
# Honors DEMO_WAIT_QUIET and hero profile.
wait_for_job_with_progress() {
  local job_name="$1"
  local job_ns="$2"
  local timeout_seconds="${3:-120}"
  local expect="${4:-complete}"
  local deadline start elapsed complete failed pod_phase line
  start="${SECONDS}"
  deadline=$((SECONDS + timeout_seconds))
  local tick_interval="${DEMO_WAIT_TICK_SECONDS:-3}"
  (( tick_interval < 1 )) && tick_interval=1

  while (( SECONDS < deadline )); do
    complete="$(kubectl get job "${job_name}" -n "${job_ns}" \
      -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
    failed="$(kubectl get job "${job_name}" -n "${job_ns}" \
      -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"

    if [[ "${complete}" == "True" ]]; then
      if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
        printf '\n' >&2
      fi
      [[ "${expect}" == "complete" ]] && return 0 || return 1
    fi
    if [[ "${failed}" == "True" ]]; then
      if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
        printf '\n' >&2
      fi
      [[ "${expect}" == "fail" ]] && return 0 || return 1
    fi

    if [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
      pod_phase="$(kubectl get pods -n "${job_ns}" -l "job-name=${job_name}" \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1:].status.phase}' 2>/dev/null || true)"
      [[ -z "${pod_phase}" ]] && pod_phase="Pending"
      elapsed=$(( SECONDS - start ))
      line="$(printf '[%s] ⏳  job/%s pod=%s elapsed=%ss' \
              "$(__demo_log_ts)" "${job_name}" "${pod_phase}" "${elapsed}")"
      if [[ -t 2 ]]; then
        printf '\r\033[2K%b%s%b' "${DIM}" "${line}" "${COLOR_RESET}" >&2
      else
        printf '%s\n' "${line}" >&2
      fi
    fi
    sleep "${tick_interval}"
  done

  if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
    printf '\n' >&2
  fi
  return 1
}

wait_for_patch_proposal_ready() {
  local finding_id="$1"
  local timeout_seconds="${2:-1200}"
  local deadline status start elapsed line
  start="${SECONDS}"
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    status="$(orka_api GET "/api/v1/security/findings/${finding_id}/patches?namespace=${DEMO_NAMESPACE}" \
      | jq -r '.items[0].status // empty')"
    case "${status}" in
      succeeded|pr_opened)
        if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
          printf '\n' >&2
        fi
        return 0
        ;;
      failed)
        if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
          printf '\n' >&2
        fi
        return 1
        ;;
    esac
    if [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
      elapsed=$(( SECONDS - start ))
      line="$(printf '[%s] ⏳  patch %s status=%s elapsed=%ss' \
              "$(__demo_log_ts)" "${finding_id}" "${status:-pending}" "${elapsed}")"
      if [[ -t 2 ]]; then
        printf '\r\033[2K%b%s%b' "${DIM}" "${line}" "${COLOR_RESET}" >&2
      else
        printf '%s\n' "${line}" >&2
      fi
    fi
    sleep 10
  done

  if [[ -t 2 ]] && [[ "${DEMO_WAIT_QUIET:-0}" != "1" ]] && ! demo_profile_is hero; then
    printf '\n' >&2
  fi
  return 1
}

wait_for_security_pull_request() {
  local finding_id="$1"
  local timeout_seconds="${2:-300}"
  local deadline http_code token url body_file
  deadline=$((SECONDS + timeout_seconds))
  token="$(get_orka_token)"
  url="${ORKA_API_BASE}/api/v1/security/findings/${finding_id}/pull-request?namespace=${DEMO_NAMESPACE}"
  body_file="$(mktemp)"
  trap 'rm -f "${body_file}"' RETURN

  while (( SECONDS < deadline )); do
    http_code="$(
      curl -sS \
        -o "${body_file}" \
        -w '%{http_code}' \
        -X POST \
        -H "Authorization: Bearer ${token}" \
        -H "Accept: application/json" \
        "${url}" \
        || true
    )"

    case "${http_code}" in
      200|201)
        cat "${body_file}"
        return 0
        ;;
      409|422|500)
        ;;
      *)
        if [[ -s "${body_file}" ]]; then
          cat "${body_file}" >&2
        fi
        return 1
        ;;
    esac

    sleep 5
  done

  if [[ -s "${body_file}" ]]; then
    cat "${body_file}" >&2
  fi
  return 1
}

require_demo_base() {
  require_cmd kubectl
  require_cmd curl
  require_cmd jq
}

require_chat_client() {
  case "${DEMO_CHAT_CLIENT}" in
    claude-code)
      require_cmd "${DEMO_CLAUDE_BIN}"
      ;;
    *)
      die "unsupported DEMO_CHAT_CLIENT=${DEMO_CHAT_CLIENT}; supported: claude-code"
      ;;
  esac
}

require_pr_demo_env() {
  require_vars \
    DEMO_PROVIDER_REF \
    DEMO_AI_MODEL \
    DEMO_RUNTIME_TYPE \
    DEMO_RUNTIME_MODEL \
    DEMO_RUNTIME_SECRET_REF \
    DEMO_GIT_REPO \
    DEMO_GIT_BRANCH \
    DEMO_GIT_SECRET_REF
}

require_cron_demo_env() {
  require_vars \
    DEMO_RUNTIME_TYPE \
    DEMO_RUNTIME_MODEL \
    DEMO_RUNTIME_SECRET_REF \
    DEMO_GIT_REPO \
    DEMO_GIT_BRANCH
}

require_security_demo_env() {
  require_vars \
    DEMO_RUNTIME_TYPE \
    DEMO_RUNTIME_MODEL \
    DEMO_RUNTIME_SECRET_REF \
    DEMO_SECURITY_GIT_REPO \
    DEMO_SECURITY_GIT_BRANCH \
    DEMO_SECURITY_GIT_SECRET_REF
}

require_substrate_demo_env() {
  # Demo 70 runs a real codex agent that opens a PR. We validate the coordinates
  # a user is most likely to override so a typo fails fast instead of mid-run.
  # The model + git Secrets are created by install-substrate.sh.
  require_vars \
    DEMO_SUBSTRATE_AGENT \
    DEMO_SUBSTRATE_TEMPLATE_NAME \
    DEMO_SUBSTRATE_TEMPLATE_NAMESPACE \
    DEMO_SUBSTRATE_SESSION \
    DEMO_SUBSTRATE_PR_REPO \
    DEMO_SUBSTRATE_PUSH_BRANCH
}

open_url() {
  local url="$1"
  if command -v open >/dev/null 2>&1; then
    open "${url}" >/dev/null 2>&1 || true
    return 0
  fi
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${url}" >/dev/null 2>&1 || true
    return 0
  fi
  return 1
}
