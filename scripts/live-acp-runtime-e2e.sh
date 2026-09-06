#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/e2e-common.sh
. "${script_dir}/lib/e2e-common.sh"
# shellcheck source=scripts/lib/redact.sh
. "${script_dir}/lib/redact.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/live-acp-runtime-e2e.sh --context CONTEXT [--namespace NAMESPACE]

Validates an already deployed ACP v2 Orka installation without printing Secret
material. The kubectl context is required and is passed to every kubectl call.

Modes:
  Smoke (default)       Read/status/concurrency/cancel/restart/replacement checks.
                        The final message explicitly reports incomplete release
                        coverage when release-only scenarios are skipped.
  RELEASE_GATE=1        Destructive release-acceptance mode. In addition to the
                        smoke checks it requires API result/fork validation,
                        write publication to a distinct GitHub fork, PR and remote
                        verification, drain/scale-to-zero/recovery, and immutable
                        image verification for every ACP workload.

Common environment:
  ACP_E2E_NAMESPACE                  Unique test namespace
  ACP_E2E_KEEP_RESOURCES=1           Preserve Kubernetes resources after the run
  ACP_E2E_ALLOW_SHARED_POOL_MUTATION=1
                                      Allow destructive pool checks in a known-isolated shared namespace
  ACP_E2E_REPO                       Public GitHub read repository URL
  ACP_E2E_REF                        Immutable read repository commit SHA
  ACP_E2E_CODEX_MODEL                Codex model (default: gpt-5.4)
  ACP_E2E_CLAUDE_MODEL               Claude model (default: claude-sonnet-4.6)
  ACP_E2E_OPENCODE_MODEL             OpenCode model (default: ACP_E2E_CODEX_MODEL)
  ACP_E2E_OPENCODE_CONTEXT_WINDOW    Reviewed OpenCode model context capacity (required)
  ACP_E2E_OPENCODE_MAX_TOKENS        Reviewed OpenCode model output limit (required)
  ACP_E2E_COPILOT_MODEL              Copilot model (default: gpt-5.3-codex)
  ACP_E2E_CONCURRENCY_TASKS          Concurrent Codex task count (default: 6)
  ACP_E2E_REQUIRE_PARALLEL=1         Require >=2 running prompts in smoke; release always requires it
  ACP_E2E_BLOCKING_PROMPT            Prompt that remains Running long enough to cancel
  ACP_E2E_TASK_MAX_TURNS             Provider inference-request budget per Task (default 24)
  ACP_E2E_TIMEOUT_DURATION           Task timeout; Running must be observed before expiry (default: 90s)
  ACP_E2E_CANCEL_SETTLE_SECONDS      Explicit-cancel settlement bound (default: 120)
  ACP_E2E_WAIT_SECONDS               Terminal wait bound (default: 900)
  ACP_E2E_STATE_WAIT_SECONDS         State transition wait bound (default: 300)

RELEASE_GATE=1 requires:
  ACP_E2E_WRITE_SOURCE_REPO          HTTPS github.com source repository
  ACP_E2E_WRITE_PUBLICATION_REPO     Distinct GitHub fork of the source repository
  ACP_E2E_WRITE_SOURCE_REF           Full source SHA; must equal the PR base-branch head
  ACP_E2E_WRITE_CREDENTIAL_SECRET    Source Secret containing publication authority
  ACP_E2E_WRITE_CREDENTIAL_NAMESPACE Source Secret namespace
  ACP_E2E_WRITE_READ_CREDENTIAL_SECRET and namespace
  ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_SECRET and namespace
  ACP_E2E_WRITE_FORGE_CREDENTIAL_SECRET and namespace
  ACP_E2E_WRITE_CREATE_PR=1          Mandatory in release-gate mode

Release-gate write settings:
  ACP_E2E_WRITE_CREDENTIAL_KEY       Source Secret key (default: token)
  ACP_E2E_WRITE_READ_CREDENTIAL_SECRET
  ACP_E2E_WRITE_READ_CREDENTIAL_NAMESPACE
  ACP_E2E_WRITE_READ_CREDENTIAL_KEY  Source clone Secret key (default: token)
  ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_SECRET
  ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_NAMESPACE
  ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_KEY  Target preflight/verify key (default: token)
  ACP_E2E_WRITE_FORGE_CREDENTIAL_SECRET
  ACP_E2E_WRITE_FORGE_CREDENTIAL_NAMESPACE
  ACP_E2E_WRITE_FORGE_CREDENTIAL_KEY  Forge PR key (default: token)
  ACP_E2E_WRITE_BRANCH               Unique branch; default includes the run ID
  ACP_E2E_WRITE_PR_BASE              Base branch (default: main)
  ACP_E2E_WRITE_PROMPT               Override the exact-one-file publication prompt
  ACP_E2E_API_LOCAL_PORT             Local controller port-forward port (default: run-scoped)

Workload discovery overrides:
  ORKA_NAMESPACE                     Controller namespace (default: orka-system)
  ORKA_ACP_RUNTIME_NAMESPACE         Default runtime namespace (default: orka-runtimes)
  ORKA_CONTROLLER_DEPLOYMENT         Controller Deployment (default: orka-controller-manager)
  ORKA_CONTROLLER_CONTAINER          Controller container name (auto-detected)
  ORKA_CONTROLLER_API_PORT           Controller API container port (default: 8080)
  ORKA_PUBLISHER_DEPLOYMENT          Publisher Deployment (default: orka-workspace-publisher)
  ORKA_PUBLISHER_CONTAINER           Publisher container name (auto-detected)
  ORKA_PROVIDER_PROXY_DEPLOYMENT     Provider proxy Deployment (default: orka-provider-auth-proxy)
  ORKA_PROVIDER_PROXY_CONTAINER      Provider proxy container name (auto-detected)
  ORKA_SCM_EGRESS_PROXY_DEPLOYMENT   SCM proxy Deployment (default: orka-scm-egress-proxy)
  ORKA_SCM_EGRESS_PROXY_CONTAINER    SCM proxy container name (auto-detected)

Release-gate local requirements:
  - gh authenticated to github.com with read access to both repositories and
    permission to close the created PR and delete the run branch.
  - git, curl, docker buildx, jq, kubectl, and shell tools.
  - Permission to create/delete the test namespace, copy the named credential
    Secret key, delete Tasks, patch Task metadata and RuntimePools, restart the controller,
    create a namespaced ServiceAccount/Role, inspect Pods/PVCs, and impersonate
    the Publisher ServiceAccount for `kubectl auth can-i` checks.

Remote cleanup is fail-closed. The branch is deleted only with an exact-head
--force-with-lease. If publication state or the remote head is ambiguous, the
script preserves Kubernetes resources and remote effects for investigation.
USAGE
}

# Timestamped override of the shared log helper for long-running live runs.
log() {
  printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

run_redacted() {
  set +e
  "$@" 2>&1 | redact
  local rc=${PIPESTATUS[0]}
  set -e
  return "${rc}"
}

sanitize_name() {
  local value
  value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-')"
  value="${value#-}"
  value="${value%-}"
  value="$(printf '%s' "${value}" | cut -c1-63)"
  value="${value%-}"
  [[ -n "${value}" ]] || value="acp-e2e"
  printf '%s\n' "${value}"
}

bool_env() {
  case "${1:-}" in
    1|true|TRUE|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

is_uint() {
  [[ "${1:-}" =~ ^(0|[1-9][0-9]*)$ ]]
}

is_sha() {
  [[ "${1:-}" =~ ^([a-fA-F0-9]{40}|[a-fA-F0-9]{64})$ ]]
}

lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

duration_seconds() {
  local value="$1"
  local amount unit
  [[ "${value}" =~ ^([1-9][0-9]*)(s|m|h)$ ]] || return 1
  amount="${BASH_REMATCH[1]}"
  unit="${BASH_REMATCH[2]}"
  case "${unit}" in
    s) printf '%s\n' "$((10#${amount}))" ;;
    m) printf '%s\n' "$((10#${amount} * 60))" ;;
    h) printf '%s\n' "$((10#${amount} * 3600))" ;;
  esac
}

context=""
namespace_arg=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --context)
      [[ $# -ge 2 ]] || die "--context requires a value"
      context="$2"
      shift 2
      ;;
    --namespace)
      [[ $# -ge 2 ]] || die "--namespace requires a value"
      namespace_arg="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "${context}" ]] || die "--context is required"

for command in kubectl jq awk sed grep cut tr sort date mktemp; do
  require_cmd "${command}"
done

release_gate=0
if bool_env "${RELEASE_GATE:-0}"; then
  release_gate=1
fi

run_id_full="$(sanitize_name "${ACP_E2E_RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}")"
run_id="$(printf '%s' "${run_id_full}" | cut -c1-24)"
if [[ "${run_id}" != "${run_id_full}" ]]; then
  warn "ACP_E2E_RUN_ID was normalized to 24 characters to preserve unique resource-name suffixes: ${run_id}"
fi
namespace="${namespace_arg:-${ACP_E2E_NAMESPACE:-orka-acp-e2e-${run_id}}}"
namespace="$(sanitize_name "${namespace}")"
orka_namespace="${ORKA_NAMESPACE:-orka-system}"
runtime_namespace="${ORKA_ACP_RUNTIME_NAMESPACE:-orka-runtimes}"
controller_deployment="${ORKA_CONTROLLER_DEPLOYMENT:-orka-controller-manager}"
publisher_deployment="${ORKA_PUBLISHER_DEPLOYMENT:-orka-workspace-publisher}"
provider_proxy_deployment="${ORKA_PROVIDER_PROXY_DEPLOYMENT:-orka-provider-auth-proxy}"
scm_proxy_deployment="${ORKA_SCM_EGRESS_PROXY_DEPLOYMENT:-orka-scm-egress-proxy}"
controller_container_override="${ORKA_CONTROLLER_CONTAINER:-}"
controller_api_port="${ORKA_CONTROLLER_API_PORT:-8080}"
publisher_container_override="${ORKA_PUBLISHER_CONTAINER:-}"
provider_proxy_container_override="${ORKA_PROVIDER_PROXY_CONTAINER:-}"
scm_proxy_container_override="${ORKA_SCM_EGRESS_PROXY_CONTAINER:-}"
repo_url="${ACP_E2E_REPO:-https://github.com/orka-agents/orka.git}"
repo_ref="${ACP_E2E_REF:-d03acb995b6014a6e855181c50b922b65ea8e7ff}"
codex_model="${ACP_E2E_CODEX_MODEL:-gpt-5.4}"
claude_model="${ACP_E2E_CLAUDE_MODEL:-claude-sonnet-4.6}"
if [[ -n "${ACP_E2E_OPENCODE_MODEL:-}" ]]; then
  opencode_model="${ACP_E2E_OPENCODE_MODEL}"
else
  opencode_model="${codex_model}"
fi
if [[ "${opencode_model}" != */* ]]; then
  opencode_model="openai/${opencode_model}"
fi
opencode_context_window="${ACP_E2E_OPENCODE_CONTEXT_WINDOW:-}"
opencode_max_tokens="${ACP_E2E_OPENCODE_MAX_TOKENS:-}"
copilot_model="${ACP_E2E_COPILOT_MODEL:-gpt-5.3-codex}"
concurrency_tasks="${ACP_E2E_CONCURRENCY_TASKS:-6}"
wait_seconds="${ACP_E2E_WAIT_SECONDS:-900}"
state_wait_seconds="${ACP_E2E_STATE_WAIT_SECONDS:-300}"
keep_resources="${ACP_E2E_KEEP_RESOURCES:-0}"
require_parallel="${ACP_E2E_REQUIRE_PARALLEL:-1}"
timeout_duration="${ACP_E2E_TIMEOUT_DURATION:-90s}"
cancel_settle_seconds="${ACP_E2E_CANCEL_SETTLE_SECONDS:-120}"
blocking_prompt="${ACP_E2E_BLOCKING_PROMPT:-Immediately use the Bash tool to run sleep 180 with a command timeout of at least 240 seconds (timeout_ms 240000). Do not return a final answer before the command completes. Do not modify repository files.}"
# Real agents may spend one inference request per file read or per retried
# sleep, so the per-Task budget is twice what the prompts below need on a good
# day; Task timeouts still bound every stage.
task_max_turns="${ACP_E2E_TASK_MAX_TURNS:-24}"
long_prompt="${ACP_E2E_LONG_PROMPT:-Read LICENSE, NOTICE.md, go.mod, Makefile, and the first ten Go source files you find. Compare their purpose and provide a detailed evidence-backed summary. Do not modify files.}"
api_local_port="${ACP_E2E_API_LOCAL_PORT:-$((20000 + ($$ % 20000)))}"

is_uint "${opencode_context_window}" || die "ACP_E2E_OPENCODE_CONTEXT_WINDOW must be a positive integer"
is_uint "${opencode_max_tokens}" || die "ACP_E2E_OPENCODE_MAX_TOKENS must be a positive integer"
(( opencode_context_window > opencode_max_tokens )) || \
  die "ACP_E2E_OPENCODE_CONTEXT_WINDOW must exceed ACP_E2E_OPENCODE_MAX_TOKENS"
is_uint "${concurrency_tasks}" || die "ACP_E2E_CONCURRENCY_TASKS must be an integer without leading zeros"
(( concurrency_tasks >= 2 )) || die "ACP_E2E_CONCURRENCY_TASKS must be at least 2"
is_uint "${wait_seconds}" || die "ACP_E2E_WAIT_SECONDS must be an integer without leading zeros"
is_uint "${state_wait_seconds}" || die "ACP_E2E_STATE_WAIT_SECONDS must be an integer without leading zeros"
is_uint "${api_local_port}" || die "ACP_E2E_API_LOCAL_PORT must be an integer without leading zeros"
(( api_local_port > 0 && api_local_port < 65536 )) || die "ACP_E2E_API_LOCAL_PORT must be between 1 and 65535"
is_uint "${controller_api_port}" || die "ORKA_CONTROLLER_API_PORT must be an integer without leading zeros"
(( controller_api_port > 0 && controller_api_port < 65536 )) || die "ORKA_CONTROLLER_API_PORT must be between 1 and 65535"
is_uint "${cancel_settle_seconds}" || die "ACP_E2E_CANCEL_SETTLE_SECONDS must be an integer without leading zeros"
(( cancel_settle_seconds > 0 && cancel_settle_seconds < 3600 )) || \
  die "ACP_E2E_CANCEL_SETTLE_SECONDS must be positive and shorter than the 1h Task timeout"
timeout_duration_seconds="$(duration_seconds "${timeout_duration}")" || \
  die "ACP_E2E_TIMEOUT_DURATION must be a positive integer followed by s, m, or h"

if [[ "${release_gate}" -eq 1 ]]; then
  for command in curl gh git docker cmp; do
    require_cmd "${command}"
  done
  is_sha "${repo_ref}" || die "RELEASE_GATE=1 requires ACP_E2E_REF to be a full immutable commit SHA"
fi

k() {
  kubectl --context "${context}" "$@"
}

resource_exists() {
  k "$@" >/dev/null 2>&1
}

auth_can_i() {
  local output decision rc error_file
  error_file="$(mktemp "${temp_root}/auth-can-i.XXXXXX")"
  if output="$(k auth can-i "$@" 2>"${error_file}")"; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -ne 0 && "${rc}" -ne 1 ]]; then
    cat "${error_file}" | redact >&2
    rm -f "${error_file}"
    return "${rc}"
  fi
  rm -f "${error_file}"
  output="${output//$'\r'/}"
  decision="$(printf '%s\n' "${output}" | awk '
    { gsub(/^[[:space:]]+|[[:space:]]+$/, "") }
    $0 == "yes" || $0 == "no" { print }
  ')"
  case "${decision}" in
    yes|no) printf '%s\n' "${decision}" ;;
    *)
      warn "kubectl auth can-i returned no unique yes/no decision"
      return 1
      ;;
  esac
}
wait_until() {
  local description="$1"
  local timeout="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  printf 'error: timed out waiting for %s\n' "${description}" >&2
  return 1
}

wait_until_fast() {
  local description="$1"
  local timeout="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  printf 'error: timed out waiting for %s\n' "${description}" >&2
  return 1
}

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-acp-e2e.XXXXXX")"
chmod 700 "${temp_root}"
namespace_create_attempted=0
namespace_created=0
namespace_shared=0
shared_pool_mutation_allowed=0
shared_mutation_checks_skipped=0
if bool_env "${ACP_E2E_ALLOW_SHARED_POOL_MUTATION:-0}"; then
  shared_pool_mutation_allowed=1
fi
namespace_uid=""
api_forward_pid=""
api_token_file="${temp_root}/api-token"
api_auth_header_file="${temp_root}/api-auth-header"
api_forward_log="${temp_root}/api-port-forward.log"
gh_token_file="${temp_root}/gh-token"
git_askpass_file="${temp_root}/git-askpass.sh"
git_observer_repo="${temp_root}/observer.git"
remote_cleanup_required=0
remote_cleanup_head=""
remote_cleanup_pr_number=""
remote_cleanup_preserve=0
remote_cleanup_branch=""
remote_cleanup_publication_repo=""
remote_cleanup_publication_slug=""
remote_cleanup_source_slug=""
remote_cleanup_pr_base=""
write_task_name=""
write_task_started=0
write_task_observer_uid=""
task_observer_finalizer="acp-e2e.orka.ai/cancellation-observer"
runtime_namespaces_seen=("${runtime_namespace}")

record_runtime_namespace() {
  local candidate="$1"
  local existing
  [[ -n "${candidate}" ]] || return 0
  for existing in "${runtime_namespaces_seen[@]}"; do
    [[ "${existing}" == "${candidate}" ]] && return 0
  done
  runtime_namespaces_seen+=("${candidate}")
}

runtimepool_mutations_allowed() {
  [[ "${namespace_shared:-0}" -ne 1 || "${shared_pool_mutation_allowed:-0}" -eq 1 ]]
}

require_runtimepool_mutation_scope() {
  if runtimepool_mutations_allowed; then
    return 0
  fi
  warn "refusing RuntimePool or controller mutation in shared namespace ${namespace}; use an isolated namespace or explicitly allow shared pool mutation only on a dedicated cluster"
  return 1
}

namespace_probe_state=""
namespace_probe_file=""
probe_namespace() {
  local target="$1"
  local output_file="${temp_root}/namespace-probe.json"
  local error_file="${temp_root}/namespace-probe.err"
  local rc=0
  namespace_probe_state=""
  namespace_probe_file="${output_file}"
  if k get namespace "${target}" -o json --ignore-not-found >"${output_file}" 2>"${error_file}"; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -ne 0 ]]; then
    namespace_probe_state="error"
    cat "${error_file}" | redact >&2
    return 1
  fi
  if [[ -s "${output_file}" ]]; then
    namespace_probe_state="present"
  else
    namespace_probe_state="absent"
  fi
}

recover_namespace_ownership() {
  if [[ "${namespace_created}" -eq 1 && -n "${namespace_uid}" ]]; then
    return 0
  fi
  [[ "${namespace_create_attempted}" -eq 1 ]] || return 0
  probe_namespace "${namespace}" || return 1
  if [[ "${namespace_probe_state}" == "absent" ]]; then
    namespace_create_attempted=0
    return 0
  fi
  if ! jq -e --arg run "${run_id}" '
      .metadata.labels["orka.ai/acp-e2e-run"] == $run
      and .metadata.labels["app.kubernetes.io/managed-by"] == "live-acp-runtime-e2e"
    ' "${namespace_probe_file}" >/dev/null; then
    warn "namespace/${namespace} exists but is not owned by this release-gate run; refusing deletion"
    return 1
  fi
  namespace_uid="$(jq -r '.metadata.uid // ""' "${namespace_probe_file}")"
  [[ -n "${namespace_uid}" ]] || return 1
  namespace_created=1
}

wait_namespace_deleted() {
  probe_namespace "${namespace}" || return 1
  [[ "${namespace_probe_state}" == "absent" ]]
}

runtime_children_for_test_namespace() {
  local ns
  for ns in "${runtime_namespaces_seen[@]}"; do
    probe_namespace "${ns}" || return 1
    if [[ "${namespace_probe_state}" == "absent" ]]; then
      continue
    fi
    k -n "${ns}" get deployment,replicaset,pod,service,secret,configmap,serviceaccount,role,rolebinding,networkpolicy,poddisruptionbudget,persistentvolumeclaim \
      -l "orka.ai/runtime-pool-namespace=${namespace}" -o name || return 1
  done
}

runtime_children_absent() {
  local output
  output="$(runtime_children_for_test_namespace)" || return 1
  [[ -z "${output}" ]]
}

stop_api_forward() {
  local rc=0
  local deadline
  local pid="${api_forward_pid}"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    if ! kill -TERM "${pid}" >/dev/null 2>&1 && kill -0 "${pid}" >/dev/null 2>&1; then
      rc=1
    fi
    deadline=$((SECONDS + 10))
    while kill -0 "${pid}" >/dev/null 2>&1 && (( SECONDS < deadline )); do
      sleep 0.1
    done
    if kill -0 "${pid}" >/dev/null 2>&1; then
      if ! kill -KILL "${pid}" >/dev/null 2>&1 && kill -0 "${pid}" >/dev/null 2>&1; then
        rc=1
      fi
    fi
    wait "${pid}" >/dev/null 2>&1 || true
    if kill -0 "${pid}" >/dev/null 2>&1; then
      rc=1
    fi
  fi
  api_forward_pid=""
  return "${rc}"
}

start_api_forward_process() {
  local kubectl_path
  kubectl_path="$(type -P kubectl)" || return 1

  # Prevent cleanup from racing the PID assignment, but reset the child signal
  # handlers before exec so the tracked PID becomes a terminable kubectl process.
  trap '' INT TERM
  (
    trap 'exit 130' INT
    trap 'exit 143' TERM
    exec "${kubectl_path}" --context "${context}" -n "${orka_namespace}" \
      port-forward --address=127.0.0.1 \
      deployment/"${controller_deployment}" "${api_local_port}:${controller_api_port}"
  ) >>"${api_forward_log}" 2>&1 &
  api_forward_pid=$!
  trap 'exit 130' INT
  trap 'exit 143' TERM
}

github_ref_lookup_state=""
github_ref_lookup_sha=""
github_ref_lookup() {
  local slug="$1"
  local branch="$2"
  local output_file="${temp_root}/git-ls-remote.out"
  local error_file="${temp_root}/git-ls-remote.err"
  local rc=0
  github_ref_lookup_state=""
  github_ref_lookup_sha=""
  configure_git_observer_auth || return 1
  if GIT_ASKPASS="${git_askpass_file}" ACP_E2E_GIT_TOKEN_FILE="${gh_token_file}" GIT_TERMINAL_PROMPT=0 \
      git -C "${git_observer_repo}" -c credential.helper= ls-remote --exit-code --refs \
      "https://github.com/${slug}.git" "refs/heads/${branch}" >"${output_file}" 2>"${error_file}"; then
    rc=0
  else
    rc=$?
  fi
  case "${rc}" in
    0)
      github_ref_lookup_state="present"
      github_ref_lookup_sha="$(awk 'NR == 1 {print $1}' "${output_file}")"
      is_sha "${github_ref_lookup_sha}" || return 1
      ;;
    2)
      github_ref_lookup_state="absent"
      ;;
    *)
      github_ref_lookup_state="error"
      cat "${error_file}" | redact >&2
      return 1
      ;;
  esac
}

github_content_lookup_state=""
github_content_lookup() {
  local slug="$1"
  local path="$2"
  local ref="$3"
  local output_file="${temp_root}/gh-content.json"
  local error_file="${temp_root}/gh-content.err"
  github_content_lookup_state=""
  if gh api "repos/${slug}/contents/${path}?ref=${ref}" >"${output_file}" 2>"${error_file}"; then
    github_content_lookup_state="present"
    return 0
  fi
  if grep -Eq 'HTTP 404|Not Found' "${error_file}"; then
    github_content_lookup_state="absent"
    return 0
  fi
  github_content_lookup_state="error"
  cat "${error_file}" | redact >&2
  return 1
}

configure_git_observer_auth() {
  [[ -s "${gh_token_file}" && -x "${git_askpass_file}" && -d "${git_observer_repo}" ]] && return 0
  if ! gh auth token --hostname github.com >"${gh_token_file}"; then
    return 1
  fi
  [[ -s "${gh_token_file}" ]] || return 1
  chmod 600 "${gh_token_file}" || return 1
  cat >"${git_askpass_file}" <<'ASKPASS'
#!/bin/sh
case "${1:-}" in
  *sername*) printf '%s\n' 'x-access-token' ;;
  *) cat "${ACP_E2E_GIT_TOKEN_FILE}" ;;
esac
ASKPASS
  chmod 700 "${git_askpass_file}" || return 1
  git init --bare "${git_observer_repo}" >/dev/null 2>&1 || return 1
}

validate_exact_pull_request() {
  local pr_json="$1"
  local expected_head="$2"
  local number="$3"
  jq -e \
    --arg branch "${remote_cleanup_branch}" \
    --arg head "${expected_head}" \
    --arg base "${remote_cleanup_pr_base}" \
    --arg repo "${remote_cleanup_publication_slug}" \
    --argjson number "${number}" '
      .number == $number
      and (.state == "open" or .state == "closed")
      and (.merged_at == null)
      and .head.ref == $branch
      and .head.sha == $head
      and .base.ref == $base
      and ((.head.repo.full_name // "") | ascii_downcase) == $repo
    ' <<<"${pr_json}" >/dev/null
}

ensure_pull_request_closed_unmerged() {
  local expected_head="$1"
  local number="$2"
  local pr_json
  if ! pr_json="$(gh api "repos/${remote_cleanup_source_slug}/pulls/${number}")"; then
    warn "cannot read release-gate PR #${number}"
    return 1
  fi
  if ! validate_exact_pull_request "${pr_json}" "${expected_head}" "${number}"; then
    warn "refusing PR cleanup: PR #${number} is merged or its immutable tuple changed"
    return 1
  fi
  if [[ "$(jq -r '.state' <<<"${pr_json}")" == "open" ]]; then
    if ! gh api --method PATCH "repos/${remote_cleanup_source_slug}/pulls/${number}" -f state=closed >/dev/null; then
      warn "failed to close exact release-gate PR #${number}"
      return 1
    fi
  fi
  if ! pr_json="$(gh api "repos/${remote_cleanup_source_slug}/pulls/${number}")"; then
    warn "cannot re-read release-gate PR #${number} after closure"
    return 1
  fi
  validate_exact_pull_request "${pr_json}" "${expected_head}" "${number}" &&
    [[ "$(jq -r '.state' <<<"${pr_json}")" == "closed" ]]
}

release_write_task_observer() {
  [[ -n "${write_task_observer_uid}" ]] || return 0
  if ! wait_until "write Task/${write_task_name} controller-owned deletion barrier" 300 \
      task_observer_release_ready "${write_task_name}" "${write_task_observer_uid}"; then
    return 1
  fi
  release_task_observer_finalizer "${write_task_name}" "${write_task_observer_uid}" || return 1
  wait_until "write Task/${write_task_name} finalizer completion" 300 task_absent "${write_task_name}" || return 1
  write_task_observer_uid=""
}

cleanup_remote_effects() {
  [[ "${release_gate}" -eq 1 && "${remote_cleanup_required}" -eq 1 ]] || return 0
  log "Cleaning release-gate remote effects with exact-head guards"

  if ! settle_write_task_for_remote_cleanup; then
    warn "write Task/publication did not reach a provably quiescent state; preserving remote and Kubernetes resources"
    remote_cleanup_preserve=1
    return 1
  fi
  if ! github_ref_lookup "${remote_cleanup_publication_slug}" "${remote_cleanup_branch}"; then
    warn "cannot determine remote branch state; preserving remote and Kubernetes resources"
    remote_cleanup_preserve=1
    return 1
  fi

  if [[ "${github_ref_lookup_state}" == "absent" && -z "${remote_cleanup_head}" ]]; then
    sleep 3
    if ! github_ref_lookup "${remote_cleanup_publication_slug}" "${remote_cleanup_branch}" ||
        [[ "${github_ref_lookup_state}" != "absent" ]]; then
      warn "publication branch absence was not stable after publisher quiescence"
      remote_cleanup_preserve=1
      return 1
    fi
    if [[ -n "${remote_cleanup_pr_number}" ]]; then
      warn "a PR receipt exists without a verified publication head"
      remote_cleanup_preserve=1
      return 1
    fi
    release_write_task_observer
    return $?
  fi

  if [[ -z "${remote_cleanup_head}" ]]; then
    warn "publication state does not provide an independently cleanable head"
    remote_cleanup_preserve=1
    return 1
  fi
  if [[ "${github_ref_lookup_state}" == "present" && "${github_ref_lookup_sha}" != "${remote_cleanup_head}" ]]; then
    warn "publication branch moved from verified head ${remote_cleanup_head} to ${github_ref_lookup_sha}; refusing cleanup"
    remote_cleanup_preserve=1
    return 1
  fi

  if [[ -n "${remote_cleanup_pr_number}" ]] &&
      ! ensure_pull_request_closed_unmerged "${remote_cleanup_head}" "${remote_cleanup_pr_number}"; then
    remote_cleanup_preserve=1
    return 1
  fi

  if [[ "${github_ref_lookup_state}" == "present" ]]; then
    configure_git_observer_auth
    if ! GIT_ASKPASS="${git_askpass_file}" ACP_E2E_GIT_TOKEN_FILE="${gh_token_file}" GIT_TERMINAL_PROMPT=0 \
        git -C "${git_observer_repo}" -c credential.helper= push --porcelain \
        --force-with-lease="refs/heads/${remote_cleanup_branch}:${remote_cleanup_head}" \
        "${remote_cleanup_publication_repo}" ":refs/heads/${remote_cleanup_branch}" >/dev/null; then
      warn "exact-head branch deletion failed; preserving Kubernetes resources"
      remote_cleanup_preserve=1
      return 1
    fi
  fi
  if ! github_ref_lookup "${remote_cleanup_publication_slug}" "${remote_cleanup_branch}" ||
      [[ "${github_ref_lookup_state}" != "absent" ]]; then
    warn "publication branch still exists after exact-head deletion"
    remote_cleanup_preserve=1
    return 1
  fi

  if [[ -n "${remote_cleanup_pr_number}" ]] &&
      ! ensure_pull_request_closed_unmerged "${remote_cleanup_head}" "${remote_cleanup_pr_number}"; then
    remote_cleanup_preserve=1
    return 1
  fi
  if ! release_write_task_observer; then
    warn "write Task did not complete controller-owned deletion after remote cleanup"
    remote_cleanup_preserve=1
    return 1
  fi
  log "Remote PR/branch cleanup completed"
}

task_cleanup_settled() {
  local task="$1"
  probe_task "${task}" || return 1
  [[ "${task_probe_state}" == "absent" ]] && return 0
  [[ "${task_probe_state}" == "present" ]] || return 1
  jq -e '
    (.status.execution.state // "") as $execution
    | (.status.delivery.state // "") as $delivery
    | ($execution == "" or $execution == "Succeeded" or $execution == "Failed" or $execution == "Cancelled" or $execution == "OutcomeUnknown")
      and (
        $delivery == ""
        or $delivery == "NotRequested"
        or $delivery == "VerifiedExact"
        or $delivery == "DeliveredSuperseded"
        or $delivery == "ReadValidated"
        or $delivery == "NoChange"
        or $delivery == "CancelledBeforePublish"
        or $delivery == "ReadOnlyWorkspaceModified"
        or $delivery == "DeliveryConflict"
        or $delivery == "CredentialBlocked"
        or $delivery == "PublicationOutcomeUnknown"
      )
  ' "${task_probe_file}" >/dev/null
}

task_absent() {
  probe_task "$1" || return 1
  [[ "${task_probe_state}" == "absent" ]]
}

add_task_observer_finalizer() {
  local task="$1"
  local uid="$2"
  local patch
  probe_task "${task}" || return 1
  [[ "${task_probe_state}" == "present" ]] || return 1
  if ! jq -e --arg uid "${uid}" --arg observer "${task_observer_finalizer}" '
      .metadata.uid == $uid
      and ((.metadata.deletionTimestamp // "") | length) == 0
      and ((.metadata.finalizers // []) | index("orka.ai/cleanup") != null)
      and ((.metadata.finalizers // []) | index($observer) == null)
    ' "${task_probe_file}" >/dev/null; then
    warn "Task/${task} is not safe to hold for cancellation observation"
    return 1
  fi
  patch="$(jq -cn --arg uid "${uid}" --arg observer "${task_observer_finalizer}" '[
    {op:"test",path:"/metadata/uid",value:$uid},
    {op:"add",path:"/metadata/finalizers/-",value:$observer}
  ]')"
  k -n "${namespace}" patch task "${task}" --type=json -p "${patch}" >/dev/null
}

task_observer_release_ready() {
  local task="$1"
  local uid="$2"
  task_cleanup_settled "${task}" || return 1
  probe_task "${task}" || return 1
  [[ "${task_probe_state}" == "present" ]] || return 1
  jq -e --arg uid "${uid}" --arg observer "${task_observer_finalizer}" '
    .metadata.uid == $uid
    and ((.metadata.deletionTimestamp // "") | length) > 0
    and (.metadata.finalizers // []) == [$observer]
  ' "${task_probe_file}" >/dev/null
}

release_task_observer_finalizer() {
  local task="$1"
  local uid="$2"
  local patch
  task_observer_release_ready "${task}" "${uid}" || return 1
  patch="$(jq -cn --arg uid "${uid}" --arg observer "${task_observer_finalizer}" '[
    {op:"test",path:"/metadata/uid",value:$uid},
    {op:"test",path:"/metadata/finalizers/0",value:$observer},
    {op:"remove",path:"/metadata/finalizers/0"}
  ]')"
  k -n "${namespace}" patch task "${task}" --type=json -p "${patch}" >/dev/null
}

task_deletion_barrier_complete() {
  local task="$1"
  local uid="$2"
  probe_task "${task}" || return 1
  [[ "${task_probe_state}" == "absent" ]] && return 0
  [[ "${task_probe_state}" == "present" ]] || return 1
  task_observer_release_ready "${task}" "${uid}"
}

request_task_cancellation() {
  local task="$1"
  if [[ "${release_gate}" -eq 1 ]]; then
    api_request DELETE "/api/v1/tasks/${task}?namespace=${namespace}" >/dev/null
  else
    k -n "${namespace}" delete task "${task}" --wait=false >/dev/null
  fi
}

runtimepool_absent() {
  probe_runtimepool "$1" || return 1
  [[ "${runtimepool_probe_state}" == "absent" ]]
}

settle_and_delete_test_tasks() {
  local owners_file="$1"
  local tasks_file="${temp_root}/cleanup-tasks.json"
  local inventory_file="${temp_root}/cleanup-tasks.tsv"
  local name uid session_uid current_uid
  : >"${owners_file}"
  if [[ "${namespace_shared:-0}" -eq 1 ]]; then
    # Shared watch-namespace mode: settle and delete only this run's Tasks.
    k -n "${namespace}" get task -l "orka.ai/acp-e2e-run=${run_id}" -o json >"${tasks_file}" || return 1
  else
    k -n "${namespace}" get task -o json >"${tasks_file}" || return 1
    if ! jq -e --arg run "${run_id}" 'all(.items[]; .metadata.labels["orka.ai/acp-e2e-run"] == $run)' "${tasks_file}" >/dev/null; then
      warn "test namespace contains a Task not owned by release-gate run ${run_id}"
      return 1
    fi
  fi
  jq -r '.items[] | [.metadata.name, .metadata.uid, (.status.execution.runtimeSessionUID // "")] | @tsv' \
    "${tasks_file}" >"${inventory_file}"
  while IFS=$'\t' read -r name uid session_uid; do
    [[ -n "${name}" && -n "${uid}" ]] || continue
    printf '%s\n' "${uid}" >>"${owners_file}"
    [[ -z "${session_uid}" ]] || printf '%s\n' "${session_uid}" >>"${owners_file}"
    probe_task "${name}" || return 1
    [[ "${task_probe_state}" == "present" ]] || continue
    current_uid="$(jq -r '.metadata.uid // ""' "${task_probe_file}")"
    if [[ "${current_uid}" != "${uid}" ]]; then
      warn "Task/${name} UID changed during cleanup; refusing deletion"
      return 1
    fi
    if [[ "$(jq -r '.metadata.deletionTimestamp // ""' "${task_probe_file}")" == "" ]]; then
      k -n "${namespace}" delete task "${name}" --wait=false >/dev/null || return 1
    fi
  done <"${inventory_file}"
  sort -u -o "${owners_file}" "${owners_file}"

  while IFS=$'\t' read -r name uid _; do
    [[ -n "${name}" ]] || continue
    if ! wait_until "Task/${name} controller-owned deletion barrier" 300 \
        task_deletion_barrier_complete "${name}" "${uid}"; then
      safe_task_summary "${name}"
      return 1
    fi
    probe_task "${name}" || return 1
    [[ "${task_probe_state}" == "present" ]] || continue
    release_task_observer_finalizer "${name}" "${uid}" || return 1
  done <"${inventory_file}"

  while IFS=$'\t' read -r name _; do
    [[ -n "${name}" ]] || continue
    if ! wait_until "Task/${name} finalizer completion" 300 task_absent "${name}"; then
      safe_task_summary "${name}"
      return 1
    fi
  done <"${inventory_file}"
}

delete_test_agents() {
  local agents_file="${temp_root}/cleanup-agents.json"
  local agent
  k -n "${namespace}" get agent -o json >"${agents_file}" || return 1
  if [[ "${namespace_shared:-0}" -eq 1 ]]; then
    # Shared watch-namespace mode: only Agents this run labeled as its own
    # are removed; a name-suffix match could select unrelated Agents (a
    # short run id such as "test" would also match "latest").
    while IFS= read -r agent; do
      [[ -n "${agent}" ]] || continue
      k -n "${namespace}" delete agent "${agent}" --ignore-not-found=true --wait=true --timeout=2m >/dev/null || return 1
    done < <(jq -r --arg run "${run_id}" '.items[] | select(.metadata.labels["orka.ai/acp-e2e-run"] == $run) | .metadata.name' "${agents_file}")
    return 0
  fi
  if ! jq -e --arg run "${run_id}" 'all(.items[]; .metadata.labels["orka.ai/acp-e2e-run"] == $run)' "${agents_file}" >/dev/null; then
    warn "test namespace contains an Agent not owned by release-gate run ${run_id}"
    return 1
  fi
  k -n "${namespace}" delete agent --all --ignore-not-found=true --wait=true --timeout=2m >/dev/null
}

stop_and_delete_test_runtimepools() {
  require_runtimepool_mutation_scope || return 1
  local pools_file="${temp_root}/cleanup-runtimepools.json"
  local inventory_file="${temp_root}/cleanup-runtimepools.tsv"
  local name uid runtime_ns current_uid
  k -n "${namespace}" get runtimepool -o json >"${pools_file}" || return 1
  if ! jq -e --arg ns "${namespace}" 'all(.items[]; .spec.trustDomain.namespace == $ns)' "${pools_file}" >/dev/null; then
    warn "test namespace contains a RuntimePool with a foreign trust domain"
    return 1
  fi
  jq -r '.items[] | [.metadata.name, .metadata.uid, (.spec.runtimeNamespace // "")] | @tsv' \
    "${pools_file}" >"${inventory_file}"
  while IFS=$'\t' read -r name uid runtime_ns; do
    [[ -n "${name}" && -n "${uid}" ]] || continue
    record_runtime_namespace "${runtime_ns}"
    probe_runtimepool "${name}" || return 1
    [[ "${runtimepool_probe_state}" == "present" ]] || continue
    current_uid="$(jq -r '.metadata.uid // ""' "${temp_root}/runtimepool-probe-${name}.json")"
    if [[ "${current_uid}" != "${uid}" ]]; then
      warn "RuntimePool/${name} UID changed during cleanup; refusing deletion"
      return 1
    fi
    k -n "${namespace}" patch runtimepool "${name}" --type=merge -p '{"spec":{"desiredReplicas":0}}' >/dev/null || return 1
  done <"${inventory_file}"
  while IFS=$'\t' read -r name _; do
    [[ -n "${name}" ]] || continue
    wait_until "RuntimePool/${name} stopped before cleanup" 300 pool_stopped "${name}" || return 1
    k -n "${namespace}" delete runtimepool "${name}" --wait=false >/dev/null || return 1
  done <"${inventory_file}"
  while IFS=$'\t' read -r name _; do
    [[ -n "${name}" ]] || continue
    wait_until "RuntimePool/${name} finalizer completion" 300 runtimepool_absent "${name}" || return 1
  done <"${inventory_file}"
}

delete_test_branchclaims() {
  local owners_file="$1"
  local claims_file="${temp_root}/cleanup-branchclaims.json"
  local inventory_file="${temp_root}/cleanup-branchclaims.tsv"
  local name uid owner_uid current_uid current_owner
  [[ -s "${owners_file}" ]] || return 0
  k get branchclaim -o json >"${claims_file}" || return 1
  jq -r --rawfile owners "${owners_file}" '
    ($owners | split("\n") | map(select(length > 0))) as $ownerUIDs
    | .items[]
    | . as $claim
    | select($ownerUIDs | index($claim.spec.ownerUid))
    | [$claim.metadata.name, $claim.metadata.uid, $claim.spec.ownerUid] | @tsv
  ' "${claims_file}" >"${inventory_file}"
  while IFS=$'\t' read -r name uid owner_uid; do
    [[ -n "${name}" && -n "${uid}" && -n "${owner_uid}" ]] || continue
    current_uid="$(k get branchclaim "${name}" -o jsonpath='{.metadata.uid}')" || return 1
    current_owner="$(k get branchclaim "${name}" -o jsonpath='{.spec.ownerUid}')" || return 1
    if [[ "${current_uid}" != "${uid}" || "${current_owner}" != "${owner_uid}" ]]; then
      warn "BranchClaim/${name} ownership changed during cleanup; refusing deletion"
      return 1
    fi
    k delete branchclaim "${name}" --wait=true --timeout=2m >/dev/null || return 1
  done <"${inventory_file}"
}

delete_test_namespace_now() {
  recover_namespace_ownership || return 1
  [[ "${namespace_created}" -eq 1 || "${namespace_shared}" -eq 1 ]] || return 0
  probe_namespace "${namespace}" || return 1
  if [[ "${namespace_probe_state}" == "absent" ]]; then
    namespace_created=0
    return 0
  fi
  if [[ "${namespace_created}" -eq 1 ]] && ! jq -e --arg run "${run_id}" --arg uid "${namespace_uid}" '
      .metadata.uid == $uid
      and .metadata.labels["orka.ai/acp-e2e-run"] == $run
      and .metadata.labels["app.kubernetes.io/managed-by"] == "live-acp-runtime-e2e"
    ' "${namespace_probe_file}" >/dev/null; then
    warn "namespace/${namespace} ownership changed; refusing deletion"
    return 1
  fi
  local owners_file="${temp_root}/cleanup-owner-uids.txt"
  log "Settling run-owned Tasks before namespace teardown"
  settle_and_delete_test_tasks "${owners_file}" || return 1
  delete_test_agents || return 1
  if [[ "${namespace_shared:-0}" -eq 1 ]]; then
    # RuntimePools are profile-keyed and may serve unrelated Agents in a
    # shared namespace; the controller's idle policy retires them.
    log "Shared watch namespace: leaving ${namespace} RuntimePools to the controller idle policy"
  else
    stop_and_delete_test_runtimepools || return 1
    if ! wait_until "runtime children for ${namespace} to be removed" 300 runtime_children_absent; then
      warn "runtime children remain or could not be listed before namespace teardown"
      runtime_children_for_test_namespace 2>&1 | redact >&2
      return 1
    fi
  fi
  delete_test_branchclaims "${owners_file}" || return 1
  if [[ "${namespace_shared:-0}" -eq 1 ]]; then
    log "Leaving shared namespace ${namespace} in place after run-resource cleanup"
    return 0
  fi

  log "Cleaning up ACP e2e namespace ${namespace}"
  if ! k delete namespace "${namespace}" --ignore-not-found=true --wait=false >/dev/null; then
    warn "failed to request namespace/${namespace} deletion"
    return 1
  fi
  probe_namespace "${namespace}" || return 1
  if [[ "${namespace_probe_state}" == "present" ]] && \
      ! wait_until "namespace/${namespace} deletion" 300 wait_namespace_deleted; then
    return 1
  fi
  if ! wait_until "runtime children for ${namespace} to remain absent" 300 runtime_children_absent; then
    warn "runtime children reappeared or could not be listed"
    runtime_children_for_test_namespace 2>&1 | redact >&2
    return 1
  fi
  namespace_created=0
}

cleanup() {
  local rc=$?
  local cleanup_rc=0
  trap - EXIT ERR
  trap '' INT TERM
  set +e

  if ! recover_namespace_ownership; then
    cleanup_rc=1
    remote_cleanup_preserve=1
  fi
  if ! cleanup_remote_effects; then
    cleanup_rc=1
  fi

  if [[ "${keep_resources}" == "1" || "${remote_cleanup_preserve}" == "1" ]]; then
    if [[ "${namespace_created}" -eq 1 || "${namespace_shared}" -eq 1 ]]; then
      warn "preserving namespace ${namespace}"
    fi
  elif [[ "${namespace_created}" -eq 1 || "${namespace_shared}" -eq 1 ]]; then
    if ! delete_test_namespace_now; then
      cleanup_rc=1
    fi
  fi

  if ! stop_api_forward; then
    warn "failed to stop controller API port-forward"
    cleanup_rc=1
  fi
  if ! rm -rf "${temp_root}"; then
    warn "failed to remove temporary credential directory ${temp_root}"
    cleanup_rc=1
  fi
  if [[ "${rc}" -eq 0 && "${cleanup_rc}" -ne 0 ]]; then
    rc="${cleanup_rc}"
  fi
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

safe_task_summary() {
  local task="$1"
  k -n "${namespace}" get task "${task}" -o json 2>/dev/null | jq '{
    name: .metadata.name,
    phase: .status.phase,
    message: .status.message,
    execution: (.status.execution // {} | {
      state, outcome, reason, attempt, promptID, runtimePoolName,
      runtimePoolUID, runtimeInstanceID, runtimeSessionUID,
      runtimeSessionGeneration, requestDigest, controllerEpoch,
      readCredentialResourceVersion, publicationCredentialResourceVersion,
      message
    }),
    delivery: (.status.delivery // {} | {
      state, outcome, reason, publicationID, sourceRepository,
      publicationRepository, branch, startingSHA, remoteBeforeSHA,
      treeSHA, expectedCommitSHA, verifiedRemoteSHA,
      supersedingRemoteSHA, artifactDigest, prReceipt, message
    })
  }' | redact >&2 || true
}

dump_diagnostics() {
  local rc=$?
  local failed_source="${BASH_SOURCE[1]:-${BASH_SOURCE[0]}}"
  local failed_line="${BASH_LINENO[0]:-unknown}"
  local failed_function="${FUNCNAME[1]:-main}"
  [[ "${rc}" -ne 0 ]] || return 0
  warn "release-gate command failed with status ${rc} at ${failed_source}:${failed_line} (${failed_function})"
  warn "release-gate failure stack functions=${FUNCNAME[*]:1:5} lines=${BASH_LINENO[*]:0:5}"
  log "Failure diagnostics (Secret contents and task results are intentionally excluded)"
  run_redacted k get nodes -o wide || true
  run_redacted k -n "${orka_namespace}" get deployment,pod,service,persistentvolumeclaim -o wide || true
  if [[ "${namespace_created}" -eq 1 || "${namespace_shared}" -eq 1 ]]; then
    run_redacted k -n "${namespace}" get agent,task,runtimepool -o wide || true
    run_redacted k -n "${namespace}" get events --sort-by=.metadata.creationTimestamp || true
  fi
  local ns
  for ns in "${runtime_namespaces_seen[@]}"; do
    run_redacted k -n "${ns}" get deployment,pod,service,networkpolicy,poddisruptionbudget \
      -l "orka.ai/runtime-pool-namespace=${namespace}" -o wide || true
  done
  return "${rc}"
}
trap dump_diagnostics ERR

create_api_identity() {
  local sa="acp-release-gate"
  jq -n \
    --arg ns "${namespace}" \
    --arg sa "${sa}" \
    '{apiVersion:"v1",kind:"ServiceAccount",metadata:{name:$sa,namespace:$ns}}' |
    k apply -f - >/dev/null
  jq -n \
    --arg ns "${namespace}" \
    '{
      apiVersion:"rbac.authorization.k8s.io/v1",
      kind:"Role",
      metadata:{name:"acp-release-gate",namespace:$ns},
      rules:[{apiGroups:["core.orka.ai"],resources:["tasks"],verbs:["get","list","watch","create","delete"]}]
    }' | k apply -f - >/dev/null
  jq -n \
    --arg ns "${namespace}" \
    --arg sa "${sa}" \
    '{
      apiVersion:"rbac.authorization.k8s.io/v1",
      kind:"RoleBinding",
      metadata:{name:"acp-release-gate",namespace:$ns},
      subjects:[{kind:"ServiceAccount",name:$sa,namespace:$ns}],
      roleRef:{apiGroup:"rbac.authorization.k8s.io",kind:"Role",name:"acp-release-gate"}
    }' | k apply -f - >/dev/null
  k -n "${namespace}" create token "${sa}" --duration=2h >"${api_token_file}"
  chmod 600 "${api_token_file}"
  {
    printf 'Authorization: Bearer '
    cat "${api_token_file}"
    printf '\n'
  } >"${api_auth_header_file}"
  chmod 600 "${api_auth_header_file}"
}

api_health_ready() {
  curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:${api_local_port}/healthz" >/dev/null
}

api_forward_ready() {
  [[ -n "${api_forward_pid}" ]] && kill -0 "${api_forward_pid}" >/dev/null 2>&1 && api_health_ready
}

start_api_forward() {
  if [[ -n "${api_forward_pid}" ]] && kill -0 "${api_forward_pid}" >/dev/null 2>&1 && api_health_ready; then
    return 0
  fi
  stop_api_forward || die "failed to stop the previous controller API port-forward"
  : >"${api_forward_log}"
  local deadline=$((SECONDS + 90))
  local attempt=0
  local attempt_deadline
  while (( SECONDS < deadline )); do
    attempt=$((attempt + 1))
    printf 'port-forward attempt %d at %s\n' "${attempt}" "$(date -u +%H:%M:%S)" >>"${api_forward_log}"
    start_api_forward_process || die "failed to launch controller API port-forward"
    attempt_deadline=$((SECONDS + 10))
    while (( SECONDS < attempt_deadline )); do
      if api_forward_ready; then
        return 0
      fi
      if ! kill -0 "${api_forward_pid}" >/dev/null 2>&1; then
        break
      fi
      sleep 0.5
    done
    stop_api_forward || die "failed to stop an unhealthy controller API port-forward"
    sleep 1
  done
  cat "${api_forward_log}" | redact >&2
  die "controller API port-forward failed after Pod-aware retries; set ACP_E2E_API_LOCAL_PORT to an unused local port"
}

ensure_api_forward() {
  if [[ -n "${api_forward_pid}" ]] && kill -0 "${api_forward_pid}" >/dev/null 2>&1 && api_health_ready; then
    return 0
  fi
  start_api_forward
}

api_request() {
  local method="$1"
  local path="$2"
  local body_file="${3:-}"
  local response_file="${temp_root}/api-response.json"
  local error_file="${temp_root}/api-curl.err"
  local status rc
  ensure_api_forward
  set +e
  if [[ -n "${body_file}" ]]; then
    status="$(curl --silent --show-error --max-time 60 \
      --request "${method}" \
      --header @"${api_auth_header_file}" \
      --header 'Content-Type: application/json' \
      --data-binary @"${body_file}" \
      --output "${response_file}" --write-out '%{http_code}' \
      "http://127.0.0.1:${api_local_port}${path}" 2>"${error_file}")"
    rc=$?
  else
    status="$(curl --silent --show-error --max-time 60 \
      --request "${method}" \
      --header @"${api_auth_header_file}" \
      --output "${response_file}" --write-out '%{http_code}' \
      "http://127.0.0.1:${api_local_port}${path}" 2>"${error_file}")"
    rc=$?
  fi
  set -e
  if [[ "${rc}" -ne 0 || ! "${status}" =~ ^2[0-9][0-9]$ ]]; then
    cat "${error_file}" | redact >&2
    cat "${response_file}" | redact >&2
    return 1
  fi
  cat "${response_file}"
}

api_task_result() {
  local task="$1"
  api_request GET "/api/v1/tasks/${task}/result?namespace=${namespace}" | jq -r '.result'
}

api_fork_task() {
  local source_task="$1"
  local new_task="$2"
  local prompt="$3"
  local body_file="${temp_root}/fork-request.json"
  jq -n \
    --arg name "${new_task}" \
    --arg prompt "${prompt}" \
    '{newTaskName:$name,prompt:$prompt}' >"${body_file}"
  api_request POST "/api/v1/tasks/${source_task}/fork?namespace=${namespace}" "${body_file}"
}

github_repo_slug() {
  local url="$1"
  local path
  case "${url}" in
    https://github.com/*) path="${url#https://github.com/}" ;;
    *) return 1 ;;
  esac
  path="${path%/}"
  path="${path%.git}"
  [[ "${path}" =~ ^[^/]+/[^/]+$ ]] || return 1
  lower "${path}"
}

gh_raw_file() {
  local slug="$1"
  local path="$2"
  local ref="$3"
  gh api -H 'Accept: application/vnd.github.raw+json' \
    "repos/${slug}/contents/${path}?ref=${ref}"
}

image_digest_from_value() {
  local value="$1"
  if [[ "${value}" =~ (sha256:[a-f0-9]{64})$ ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

pinned_image_digest() {
  local value="$1"
  if [[ "${value}" =~ @(sha256:[a-f0-9]{64})$ ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

resolve_platform_manifest_digest() {
  local image="$1"
  local node="$2"
  local requested_digest raw_file error_file media_type os arch expected count
  requested_digest="$(pinned_image_digest "${image}")" || return 1
  raw_file="${temp_root}/image-index-$(printf '%s' "${requested_digest}" | cut -c8-23).json"
  error_file="${raw_file}.err"
  if ! docker buildx imagetools inspect --raw "${image}" >"${raw_file}" 2>"${error_file}"; then
    cat "${error_file}" | redact >&2
    return 1
  fi
  media_type="$(jq -r '.mediaType // ""' "${raw_file}")"
  case "${media_type}" in
    *image.index*|*manifest.list*)
      os="$(k get node "${node}" -o jsonpath='{.metadata.labels.kubernetes\.io/os}')"
      arch="$(k get node "${node}" -o jsonpath='{.metadata.labels.kubernetes\.io/arch}')"
      count="$(jq --arg os "${os}" --arg arch "${arch}" \
        '[.manifests[] | select(.platform.os == $os and .platform.architecture == $arch)] | length' "${raw_file}")"
      [[ "${count}" == "1" ]] || {
        warn "OCI index ${image} has ${count} manifests for ${os}/${arch}; cannot bind actual imageID unambiguously"
        return 1
      }
      expected="$(jq -r --arg os "${os}" --arg arch "${arch}" \
        '.manifests[] | select(.platform.os == $os and .platform.architecture == $arch) | .digest' "${raw_file}")"
      ;;
    *)
      expected="${requested_digest}"
      ;;
  esac
  [[ "${expected}" =~ ^sha256:[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "${expected}"
}

assert_pod_container_image() {
  local pod_namespace="$1"
  local pod_name="$2"
  local container="$3"
  local requested_image="$4"
  local pod_json status_image image_id actual_digest requested_digest expected_digest node
  requested_digest="$(pinned_image_digest "${requested_image}")" || \
    die "${pod_namespace}/Pod/${pod_name} container ${container} is not requested by digest: ${requested_image}"
  pod_json="$(k -n "${pod_namespace}" get pod "${pod_name}" -o json)"
  status_image="$(jq -r --arg container "${container}" \
    '((.status.containerStatuses // []) + (.status.initContainerStatuses // []))[] | select(.name == $container) | .image' <<<"${pod_json}")"
  image_id="$(jq -r --arg container "${container}" \
    '((.status.containerStatuses // []) + (.status.initContainerStatuses // []))[] | select(.name == $container) | .imageID' <<<"${pod_json}")"
  [[ -n "${status_image}" && -n "${image_id}" ]] || \
    die "${pod_namespace}/Pod/${pod_name} has no image status for container ${container}"
  actual_digest="$(image_digest_from_value "${image_id}")" || \
    die "${pod_namespace}/Pod/${pod_name} container ${container} imageID is not digest-addressed: ${image_id}"
  if [[ "${release_gate}" -eq 1 ]]; then
    node="$(jq -r '.spec.nodeName // ""' <<<"${pod_json}")"
    [[ -n "${node}" ]] || die "${pod_namespace}/Pod/${pod_name} is not scheduled"
    expected_digest="$(resolve_platform_manifest_digest "${requested_image}" "${node}")" || \
      die "could not resolve platform manifest for ${requested_image}"
    [[ "${actual_digest}" == "${requested_digest}" || "${actual_digest}" == "${expected_digest}" ]] || \
      die "${pod_namespace}/Pod/${pod_name} container ${container} imageID digest ${actual_digest} matches neither requested index/manifest digest ${requested_digest} nor platform digest ${expected_digest}"
  fi
}

assert_all_pod_images() {
  local pod_namespace="$1"
  local pod_name="$2"
  local pod_json container image images_file
  pod_json="$(k -n "${pod_namespace}" get pod "${pod_name}" -o json)"
  images_file="${temp_root}/pod-images-$(sanitize_name "${pod_namespace}-${pod_name}").tsv"
  jq -r '
    ((.spec.initContainers // [])[] | ["init", .name, .image]),
    ((.spec.containers // [])[] | ["container", .name, .image])
    | @tsv
  ' <<<"${pod_json}" >"${images_file}" || die "failed to enumerate Pod/${pod_name} images"
  [[ -s "${images_file}" ]] || die "Pod/${pod_name} has no declared container images"
  while IFS=$'\t' read -r _ container image; do
    [[ -n "${container}" && -n "${image}" ]] || die "Pod/${pod_name} has an incomplete container image declaration"
    assert_pod_container_image "${pod_namespace}" "${pod_name}" "${container}" "${image}"
  done <"${images_file}"
}

select_deployment_container() {
  local deployment_json="$1"
  local override="$2"
  shift 2
  local candidate
  if [[ -n "${override}" ]]; then
    jq -e --arg name "${override}" '.spec.template.spec.containers | any(.name == $name)' \
      <<<"${deployment_json}" >/dev/null || die "configured container ${override} was not found"
    printf '%s\n' "${override}"
    return 0
  fi
  for candidate in "$@"; do
    if jq -e --arg name "${candidate}" '.spec.template.spec.containers | any(.name == $name)' \
        <<<"${deployment_json}" >/dev/null; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  if [[ "$(jq '.spec.template.spec.containers | length' <<<"${deployment_json}")" == "1" ]]; then
    jq -r '.spec.template.spec.containers[0].name' <<<"${deployment_json}"
    return 0
  fi
  die "could not auto-detect a unique container in Deployment"
}

assert_pod_matches_deployment_images() {
  local deployment_json="$1"
  local pod_json="$2"
  jq -e --argjson pod "${pod_json}" '
    def images($spec):
      (((($spec.initContainers // []) + ($spec.containers // [])) | map({key:.name,value:.image}) | from_entries));
    images(.spec.template.spec) == images($pod.spec)
  ' <<<"${deployment_json}" >/dev/null
}

assert_deployment_persistent() {
  local deployment_json="$1"
  local deployment="$2"
  local container="$3"
  local claims claim phase
  claims="$(jq -r --arg container "${container}" '
    .spec.template.spec as $pod
    | [$pod.containers[] | select(.name == $container) | .volumeMounts[]?.name] as $mounts
    | $pod.volumes[]? as $volume
    | select($volume.persistentVolumeClaim != null and ($mounts | index($volume.name)))
    | $volume.persistentVolumeClaim.claimName
  ' <<<"${deployment_json}")"
  [[ -n "${claims}" ]] || \
    die "Deployment/${deployment} primary container ${container} does not mount a persistentVolumeClaim"
  while IFS= read -r claim; do
    [[ -n "${claim}" ]] || continue
    phase="$(k -n "${orka_namespace}" get persistentvolumeclaim "${claim}" -o jsonpath='{.status.phase}')"
    [[ "${phase}" == "Bound" ]] || die "PersistentVolumeClaim/${claim} for Deployment/${deployment} is ${phase:-<empty>}, want Bound"
  done <<<"${claims}"
}

assert_deployment_digest() {
  local deployment="$1"
  local override="$2"
  shift 2
  local deployment_json container image selector pods_json count total pod pod_json replica_set replica_set_json
  resource_exists -n "${orka_namespace}" get deployment "${deployment}" || \
    die "required Deployment/${deployment} was not found in ${orka_namespace}"
  k -n "${orka_namespace}" rollout status deployment/"${deployment}" --timeout=5m >/dev/null
  deployment_json="$(k -n "${orka_namespace}" get deployment "${deployment}" -o json)"
  jq -e '
    .spec.replicas == 1
    and .status.observedGeneration == .metadata.generation
    and .status.updatedReplicas == 1
    and .status.readyReplicas == 1
    and .status.availableReplicas == 1
  ' <<<"${deployment_json}" >/dev/null || \
    die "Deployment/${deployment} is not a fully observed single-replica rollout"
  container="$(select_deployment_container "${deployment_json}" "${override}" "$@")"
  image="$(jq -r --arg container "${container}" \
    '.spec.template.spec.containers[] | select(.name == $container) | .image' <<<"${deployment_json}")"
  pinned_image_digest "${image}" >/dev/null || \
    die "Deployment/${deployment} container ${container} is not digest-pinned: ${image}"
  selector="$(jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")' <<<"${deployment_json}")"
  [[ -n "${selector}" ]] || die "Deployment/${deployment} has no matchLabels selector"
  pods_json="$(k -n "${orka_namespace}" get pods -l "${selector}" -o json)"
  total="$(jq '[.items[] | select(.metadata.deletionTimestamp == null)] | length' <<<"${pods_json}")"
  [[ "${total}" == "1" ]] || die "Deployment/${deployment} has ${total} non-terminating Pods, want exactly 1"
  count="$(jq --arg container "${container}" '[.items[] | select(.metadata.deletionTimestamp == null) |
    select(any(.status.containerStatuses[]?; .name == $container and .ready == true))] | length' <<<"${pods_json}")"
  [[ "${count}" == "1" ]] || die "Deployment/${deployment} does not have exactly one ready selected Pod"
  pod="$(jq -r --arg container "${container}" '.items[] | select(.metadata.deletionTimestamp == null) |
    select(any(.status.containerStatuses[]?; .name == $container and .ready == true)) | .metadata.name' <<<"${pods_json}")"
  pod_json="$(k -n "${orka_namespace}" get pod "${pod}" -o json)"
  replica_set="$(jq -r '.metadata.ownerReferences[]? | select(.controller == true and .kind == "ReplicaSet") | .name' <<<"${pod_json}")"
  [[ -n "${replica_set}" ]] || die "Deployment/${deployment} selected Pod is not owned by a ReplicaSet"
  replica_set_json="$(k -n "${orka_namespace}" get replicaset "${replica_set}" -o json)"
  jq -e --arg deployment "${deployment}" --arg uid "$(jq -r '.metadata.uid' <<<"${deployment_json}")" '
    any(.metadata.ownerReferences[]?; .controller == true and .kind == "Deployment" and .name == $deployment and .uid == $uid)
  ' <<<"${replica_set_json}" >/dev/null || \
    die "selected Pod ReplicaSet is not owned by Deployment/${deployment}"
  assert_pod_matches_deployment_images "${deployment_json}" "${pod_json}" || \
    die "Deployment/${deployment} Pod images do not match the current Deployment template"
  assert_all_pod_images "${orka_namespace}" "${pod}"
}

publisher_service_account=""
assert_publisher_brokered_authority() {
  local deployment_json container service_account can_get can_list
  deployment_json="$(k -n "${orka_namespace}" get deployment "${publisher_deployment}" -o json)"
  container="$(select_deployment_container "${deployment_json}" "${publisher_container_override}" publisher workspace-publisher)"
  jq -e --arg container "${container}" '
    .spec.template.spec as $pod
    | ($pod.containers[] | select(.name == $container)) as $publisher
    | [$publisher.env[]?] as $env
    | [$publisher.volumeMounts[]?.name] as $mounts
    | [$pod.volumes[]? | select(.secret != null) | .name] as $secretVolumes
    | ([$mounts[] as $mount | select($secretVolumes | index($mount)) | $mount] | unique) as $mountedSecrets
    | ([$publisher.args[]?] | join(" ")) as $args
    | (($pod.volumes[] | select(.name == "publisher-auth") | .secret.items // []) | map(.path) | sort) as $authPaths
    | ([$publisher.volumeMounts[]? | select(.name == "publisher-auth") |
        {mountPath, subPath, readOnly}] | sort_by(.subPath)) as $authMounts
    | ([$env[] | select(.name == "ORKA_PUBLISHER_CREDENTIAL_BROKER_URL" and ((.value // "") | test("^https?://")))] | length) == 1
      and ([$env[] | select(.name == "ORKA_PUBLISHER_ARTIFACT_AUTHORIZATION_BROKER_URL" and ((.value // "") | test("^https?://")))] | length) == 1
      and ([$env[] | select(.name == "ORKA_PUBLISHER_CREDENTIAL_ROOT" or .name == "ORKA_PUBLISHER_ARTIFACT_CAPABILITY_SECRET_FILE")] | length) == 0
      and (($publisher.envFrom // []) | length) == 0
      and ([$env[] | select(.valueFrom.secretKeyRef != null)] | length) == 1
      and ([$env[] | select(
        .name == "ORKA_SCM_EGRESS_PROXY_TOKEN"
        and ((.valueFrom.secretKeyRef.name // "") | length) > 0
        and ((.valueFrom.secretKeyRef.key // "") | length) > 0
      )] | length) == 1
      and ([$env[] | select(
        (.name == "HTTPS_PROXY" or .name == "https_proxy")
        and ((.value // "") | contains("$(ORKA_SCM_EGRESS_PROXY_TOKEN)"))
        and ((.value // "") | test("scm-egress-proxy"))
      )] | length) == 2
      and ([$env[] | select(.name == "ORKA_PUBLISHER_SCM_EGRESS_PROXY_REQUIRED" and .value == "true")] | length) == 1
      and ($args | test("credential-root|artifact-capability-secret"; "i") | not)
      and $mountedSecrets == ["publisher-auth"]
      and $authPaths == ["controller-token", "operation-capability-secret"]
      and $authMounts == [
        {mountPath:"/var/run/orka/publisher-auth/controller-token", subPath:"controller-token", readOnly:true},
        {mountPath:"/var/run/orka/publisher-auth/operation-capability-secret", subPath:"operation-capability-secret", readOnly:true}
      ]
  ' <<<"${deployment_json}" >/dev/null || \
    die "Publisher must use nonempty controller brokers without direct workspace credential or artifact-signing authority"
  service_account="$(jq -r '.spec.template.spec.serviceAccountName // "default"' <<<"${deployment_json}")"
  publisher_service_account="${service_account}"
  can_get="$(auth_can_i get secrets --as="system:serviceaccount:${orka_namespace}:${service_account}" --all-namespaces)"
  can_list="$(auth_can_i list secrets --as="system:serviceaccount:${orka_namespace}:${service_account}" --all-namespaces)"
  [[ "${can_get}" == "no" && "${can_list}" == "no" ]] || \
    die "Publisher ServiceAccount ${service_account} has direct Kubernetes Secret read authority"
}

assert_publisher_cannot_access_secret() {
  local secret_namespace="$1"
  local secret_name="$2"
  local identity="system:serviceaccount:${orka_namespace}:${publisher_service_account}"
  local can_get can_list can_watch can_create_pods can_create_deployments can_create_jobs
  can_get="$(auth_can_i get "secret/${secret_name}" -n "${secret_namespace}" --as="${identity}")"
  can_list="$(auth_can_i list secrets -n "${secret_namespace}" --as="${identity}")"
  can_watch="$(auth_can_i watch secrets -n "${secret_namespace}" --as="${identity}")"
  can_create_pods="$(auth_can_i create pods -n "${secret_namespace}" --as="${identity}")"
  can_create_deployments="$(auth_can_i create deployments.apps -n "${secret_namespace}" --as="${identity}")"
  can_create_jobs="$(auth_can_i create jobs.batch -n "${secret_namespace}" --as="${identity}")"
  [[ "${can_get}" == "no" && "${can_list}" == "no" && "${can_watch}" == "no" ]] || \
    die "Publisher ServiceAccount can directly read credential Secret ${secret_namespace}/${secret_name}"
  [[ "${can_create_pods}" == "no" && "${can_create_deployments}" == "no" && "${can_create_jobs}" == "no" ]] || \
    die "Publisher ServiceAccount can create workloads that mount credential Secret ${secret_namespace}/${secret_name}"
}

apply_agent() {
  local provider="$1"
  local model="$2"
  local name="$3"
  local max_turns="${4:-${task_max_turns}}"
  local allow_bash="${5:-false}"
  jq -n \
    --arg provider "${provider}" \
    --arg model "${model}" \
    --arg name "${name}" \
    --arg run "${run_id}" \
    --argjson maxTurns "${max_turns}" \
    --argjson allowBash "${allow_bash}" \
    --argjson opencodeContextWindow "${opencode_context_window}" \
    --argjson opencodeMaxTokens "${opencode_max_tokens}" \
    '{
      apiVersion:"core.orka.ai/v1alpha1",
      kind:"Agent",
      metadata:{name:$name,labels:{"orka.ai/acp-e2e-run":$run,"app.kubernetes.io/managed-by":"live-acp-runtime-e2e"}},
      spec:{
        runtime:({
          type:$provider,
          contractVersion:"orka.harness.v2",
          defaultMaxTurns:$maxTurns
        } + (if $provider == "codex" then {} else {
          defaultAllowBash:$allowBash,
          defaultAllowedTools:(
            if $provider == "opencode" and $allowBash then ["Read","Write","Edit","Bash","Glob","Grep"]
            elif $allowBash then ["Read","Glob","Grep","Bash"]
            else ["Read","Glob","Grep"] end
          )
        } end)),
        model:(
          if $provider == "opencode" then
            {name:$model,contextWindow:$opencodeContextWindow,maxTokens:$opencodeMaxTokens}
          else {name:$model} end
        )
      }
    }' | k -n "${namespace}" apply -f - >/dev/null
}

# Read tasks without Bash carry the restricted {Read,Glob,Grep} tool policy for
# every provider, so codex exercises its native read-only agent mode live.
# Codex cannot express restricted policies that include Bash, so the blocking
# timeout/cancel tasks stay unrestricted for it.
apply_read_task() {
  local name="$1"
  local agent="$2"
  local session="$3"
  local create="$4"
  local prompt="$5"
  local timeout="${6:-12m}"
  local allow_bash="${7:-false}"
  local source_identity="${read_repo_identity:-}"
  local provider
  provider="$(k -n "${namespace}" get agent "${agent}" -o jsonpath='{.spec.runtime.type}')"
  jq -n \
    --arg name "${name}" \
    --arg run "${run_id}" \
    --arg agent "${agent}" \
    --arg session "${session}" \
    --arg prompt "${prompt}" \
    --arg repo "${repo_url}" \
    --arg ref "${repo_ref}" \
    --arg identity "${source_identity}" \
    --arg timeout "${timeout}" \
    --arg provider "${provider}" \
    --argjson create "${create}" \
    --argjson maxTurns "${task_max_turns}" \
    --argjson allowBash "${allow_bash}" \
    '{
      apiVersion:"core.orka.ai/v1alpha1",
      kind:"Task",
      metadata:{name:$name,labels:{"orka.ai/acp-e2e-run":$run}},
      spec:({
        type:"agent",
        agentRef:{name:$agent},
        prompt:$prompt,
        workspace:({intent:"read",gitRepo:$repo,ref:$ref} +
          (if ($identity|length)>0 then {sourceRepository:{provider:"github",id:$identity}} else {} end)),
        agentRuntime:({maxTurns:$maxTurns} + (
          if $provider == "codex" and $allowBash then {} else {
            allowBash:$allowBash,
            allowedTools:(if $allowBash then ["Read","Glob","Grep","Bash"] else ["Read","Glob","Grep"] end)
          } end)),
        timeout:$timeout
      } + (if ($session|length)>0 then {sessionRef:{name:$session,create:$create,append:true}} else {} end))
    }' | k -n "${namespace}" apply -f - >/dev/null
}

apply_invalid_read_task() {
  local name="$1"
  local agent="$2"
  local unsafe_repo="$3"
  jq -n \
    --arg name "${name}" \
    --arg run "${run_id}" \
    --arg agent "${agent}" \
    --arg repo "${unsafe_repo}" \
    --arg ref "${repo_ref}" \
    '{
      apiVersion:"core.orka.ai/v1alpha1",
      kind:"Task",
      metadata:{name:$name,labels:{"orka.ai/acp-e2e-run":$run}},
      spec:{
        type:"agent",
        agentRef:{name:$agent},
        prompt:"This request must be rejected before RuntimePool demand or prompt submission.",
        workspace:{intent:"read",gitRepo:$repo,ref:$ref},
        timeout:"2m"
      }
    }' | k -n "${namespace}" apply -f - >/dev/null
}

copy_secret_key_to_test_namespace() {
  local source_namespace="$1"
  local source_name="$2"
  local source_key="$3"
  local target_name="$4"
  resource_exists -n "${source_namespace}" get secret "${source_name}" || \
    die "Secret/${source_name} does not exist in namespace ${source_namespace}"
  if ! k -n "${source_namespace}" get secret "${source_name}" -o json | \
      jq -e --arg key "${source_key}" '(.data[$key] // "") | length > 0' >/dev/null; then
    die "Secret/${source_name} key ${source_key} is missing or empty"
  fi
  k -n "${source_namespace}" get secret "${source_name}" -o json |
    jq --arg ns "${namespace}" --arg name "${target_name}" --arg key "${source_key}" '{
      apiVersion:"v1",
      kind:"Secret",
      metadata:{name:$name,namespace:$ns},
      type:(.type // "Opaque"),
      data:{($key):.data[$key]}
    }' | k apply -f - >/dev/null
}

task_json() {
  local task="$1"
  local payload attempt
  for ((attempt = 1; attempt <= 10; attempt++)); do
    if payload="$(k -n "${namespace}" get task "${task}" -o json 2>/dev/null)"; then
      printf '%s\n' "${payload}"
      return 0
    fi
    sleep 0.25
  done
  k -n "${namespace}" get task "${task}" -o json
}

task_probe_state=""
task_probe_file=""
probe_task() {
  local task="$1"
  local output_file="${temp_root}/task-probe-${task}.json"
  local error_file="${temp_root}/task-probe-${task}.err"
  local rc=0
  task_probe_state=""
  task_probe_file="${output_file}"
  if k -n "${namespace}" get task "${task}" -o json --ignore-not-found >"${output_file}" 2>"${error_file}"; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -ne 0 ]]; then
    task_probe_state="error"
    cat "${error_file}" | redact >&2
    return 1
  fi
  if [[ -s "${output_file}" ]]; then
    task_probe_state="present"
  else
    task_probe_state="absent"
  fi
}

runtimepool_probe_state=""
probe_runtimepool() {
  local pool="$1"
  local output_file="${temp_root}/runtimepool-probe-${pool}.json"
  local error_file="${temp_root}/runtimepool-probe-${pool}.err"
  local rc=0
  runtimepool_probe_state=""
  if k -n "${namespace}" get runtimepool "${pool}" -o json --ignore-not-found >"${output_file}" 2>"${error_file}"; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -ne 0 ]]; then
    runtimepool_probe_state="error"
    cat "${error_file}" | redact >&2
    return 1
  fi
  if [[ -s "${output_file}" ]]; then
    runtimepool_probe_state="present"
  else
    runtimepool_probe_state="absent"
  fi
}

task_phase_is() {
  local task="$1"
  shift
  local phase expected
  phase="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.phase}' 2>/dev/null)" || return 1
  for expected in "$@"; do
    [[ "${phase}" == "${expected}" ]] && return 0
  done
  return 1
}

task_execution_state_is() {
  local task="$1"
  shift
  local state expected
  state="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.state}' 2>/dev/null)" || return 1
  for expected in "$@"; do
    [[ "${state}" == "${expected}" ]] && return 0
  done
  return 1
}

task_execution_terminal() {
  task_execution_state_is "$1" Succeeded Failed Cancelled OutcomeUnknown
}

task_terminal_projection_ready() {
  local task="$1"
  local payload
  payload="$(k -n "${namespace}" get task "${task}" -o json 2>/dev/null)" || return 1
  jq -e '
    (.status.execution.state // "") as $state
    | (.status.phase // "") as $phase
    | ($state == "Succeeded" and $phase == "Succeeded")
      or ($state == "Failed" and $phase == "Failed")
      or ($state == "Cancelled" and $phase == "Cancelled")
      or ($state == "OutcomeUnknown" and $phase == "Failed")
  ' <<<"${payload}" >/dev/null
}

wait_task_terminal() {
  local task="$1"
  wait_until "Task/${task} terminal execution and phase projection" "${wait_seconds}" task_terminal_projection_ready "${task}"
}

wait_task_pool_name_value() {
  local task="$1"
  local name
  name="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimePoolName}' 2>/dev/null)" || return 1
  [[ -n "${name}" ]] || return 1
  printf '%s\n' "${name}"
}

pool_serving() {
  local pool="$1"
  local values
  values="$(k -n "${namespace}" get runtimepool "${pool}" -o jsonpath='{.status.lifecycle}{"/"}{.status.admissionState}' 2>/dev/null)" || return 1
  [[ "${values}" == "Serving/Accepting" ]]
}

wait_pool_serving() {
  local pool="$1"
  wait_until "RuntimePool/${pool} Serving/Accepting" "${wait_seconds}" pool_serving "${pool}"
}

pool_lifecycle_is() {
  local pool="$1"
  local lifecycle="$2"
  local admission="${3:-}"
  local payload
  payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  [[ "$(jq -r '.status.lifecycle // ""' <<<"${payload}")" == "${lifecycle}" ]] || return 1
  if [[ -n "${admission}" ]]; then
    [[ "$(jq -r '.status.admissionState // ""' <<<"${payload}")" == "${admission}" ]] || return 1
  fi
}


pool_lifecycle_reached() {
  local pool="$1"
  local target="$2"
  local payload state
  payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  state="$(jq -r '(.status.lifecycle // "") + "/" + (.status.admissionState // "")' <<<"${payload}")"
  case "${target}:${state}" in
    Draining:Draining/Draining|Draining:Quiescent/Draining|Draining:Stopping/Closed|Draining:Stopped/Closed | \
    Quiescent:Quiescent/Draining|Quiescent:Stopping/Closed|Quiescent:Stopped/Closed | \
    Stopping:Stopping/Closed|Stopping:Stopped/Closed | \
    Stopped:Stopped/Closed)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

pool_instance_changed_and_serving() {
  local pool="$1"
  local previous_instance="$2"
  local payload instance
  payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  instance="$(jq -r '.status.activeInstance.runtimeInstanceID // ""' <<<"${payload}")"
  [[ -n "${instance}" && "${instance}" != "${previous_instance}" ]] || return 1
  jq -e '.status.lifecycle == "Serving" and .status.admissionState == "Accepting"' <<<"${payload}" >/dev/null
}

pool_transient_counters_zero() {
  local pool="$1"
  local expected_resident_sessions="${2:-0}"
  local payload
  is_uint "${expected_resident_sessions}" || return 1
  payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  # Idle durable RuntimeSessions intentionally retain one provider process each.
  # Callers must state the exact expected resident count so task-scoped leaks do
  # not pass as quiescent; drained/stopped validation separately requires zero.
  jq -e --argjson expected "${expected_resident_sessions}" '
    (.status.capacity | type) == "object"
    and (.status.capacity.maxResidentSessions // 0) > 0
    and (.status.capacity.maxRunningPrompts // 0) > 0
    and (.status.capacity.residentSessions // 0) == $expected
    and (.status.capacity.runningPrompts // 0) == 0
    and (.status.capacity.queuedTasks // 0) == 0
    and (.status.capacity.reservedSessions // 0) == 0
    and (.status.capacity.reservedPrompts // 0) == 0
    and (.status.capacity.pendingPermissions // 0) == 0
    and (.status.capacity.finalizingSessions // 0) == 0
    and (.status.capacity.liveDescendants // 0) <= $expected
    and ((.status.capacity.reservations // []) | length) == 0
  ' <<<"${payload}" >/dev/null
}

pool_stopped() {
  local pool="$1"
  local payload pod_ns count
  payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  jq -e '
    .spec.desiredReplicas == 0
    and (.status.desiredReplicas // 0) == 0
    and (.status.currentReplicas // 0) == 0
    and .status.lifecycle == "Stopped"
    and .status.admissionState == "Closed"
    and (.status.activeInstance == null)
    and (.status.capacity.residentSessions // 0) == 0
    and (.status.capacity.runningPrompts // 0) == 0
    and (.status.capacity.queuedTasks // 0) == 0
    and (.status.capacity.reservedSessions // 0) == 0
    and (.status.capacity.reservedPrompts // 0) == 0
    and (.status.capacity.pendingPermissions // 0) == 0
    and (.status.capacity.finalizingSessions // 0) == 0
    and (.status.capacity.liveDescendants // 0) == 0
    and ((.status.capacity.reservations // []) | length) == 0
  ' <<<"${payload}" >/dev/null || return 1
  pod_ns="$(jq -r '.spec.runtimeNamespace // ""' <<<"${payload}")"
  [[ -n "${pod_ns}" ]] || pod_ns="${runtime_namespace}"
  count="$(k -n "${pod_ns}" get pods \
    -l "orka.ai/runtime-pool-namespace=${namespace},orka.ai/runtime-pool-name=${pool}" \
    -o json 2>/dev/null | jq '[.items[] | select(.metadata.deletionTimestamp == null)] | length')" || return 1
  [[ "${count}" == "0" ]]
}

park_runtimepool() {
  local pool="$1"
  require_runtimepool_mutation_scope || return 1
  log "Parking RuntimePool/${pool} between profile-specific ACP checks"
  k -n "${namespace}" patch runtimepool "${pool}" --type=merge \
    -p '{"spec":{"desiredReplicas":0}}' >/dev/null
  wait_until "RuntimePool/${pool} stopped after phase parking" "${state_wait_seconds}" pool_stopped "${pool}"
}

park_provider_runtimepools_except() {
  local provider="$1"
  local keep_pool="$2"
  local pools pool
  require_runtimepool_mutation_scope || return 1
  pools="$(k -n "${namespace}" get runtimepool -o json | jq -r \
    --arg provider "${provider}" --arg keep "${keep_pool}" \
    '.items[] | select(.metadata.name != $keep and .spec.runtime.profile.providerKind == $provider) | .metadata.name')"
  [[ -n "${pools}" ]] || return 0
  while IFS= read -r pool; do
    [[ -n "${pool}" ]] || continue
    park_runtimepool "${pool}"
  done <<<"${pools}"
}

resume_runtimepool() {
  local pool="$1"
  require_runtimepool_mutation_scope || return 1
  log "Resuming RuntimePool/${pool} for continuation recovery checks"
  k -n "${namespace}" patch runtimepool "${pool}" --type=merge \
    -p '{"spec":{"desiredReplicas":1}}' >/dev/null
  wait_pool_serving "${pool}"
}

pool_running_for_task() {
  local task="$1"
  local pool="$2"
  task_execution_state_is "${task}" Running || return 1
  local running
  running="$(k -n "${namespace}" get runtimepool "${pool}" -o jsonpath='{.status.capacity.runningPrompts}' 2>/dev/null)" || return 1
  is_uint "${running}" && (( running > 0 ))
}

pool_profile_projection_valid() {
  local pool="$1"
  local provider="$2"
  local model="$3"
  local intent="$4"
  local payload="$5"
  local pod_payload="$6"
  jq -e \
    --arg pool "${pool}" \
    --arg provider "${provider}" \
    --arg model "${model}" \
    --arg intent "${intent}" \
    --arg ns "${namespace}" \
    --argjson pod "${pod_payload}" '
      .spec.trustDomain.namespace == $ns
      and .metadata.name == $pool
      and (.metadata.name | test("^acp-" + $provider + "-[a-f0-9]{16}$"))
      and .metadata.labels["orka.ai/acp-runtime-pool"] == "true"
      and .metadata.labels["orka.ai/acp-trust-domain"] == $ns
      and .metadata.labels["orka.ai/acp-profile"] ==
        ((.spec.runtime.profile.digest | ltrimstr("sha256:"))[0:16])
      and ((.metadata.uid // "") | length > 0)
      and ((.metadata.generation // 0) > 0)
      and .status.observedGeneration == .metadata.generation
      and .spec.desiredReplicas == 1
      and .spec.runtime.profile.protocolVersion == "orka.harness.v2"
      and .spec.runtime.profile.providerKind == $provider
      and .spec.runtime.profile.model == $model
      and .spec.runtime.profile.workspaceIntent == $intent
      and (.spec.runtime.image | test("@sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.digest | test("^sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.digestSchemaVersion | length > 0)
      and (.spec.runtime.profile.adapterDigests | length > 0)
      and ([.spec.runtime.profile.adapterDigests[] | test("^sha256:[a-f0-9]{64}$")] | all)
      and (.spec.runtime.profile.agentConfigurationDigest | test("^sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.toolPolicyDigest | test("^sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.approvalPolicyDigest | test("^sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.mcpConfigurationDigest | test("^sha256:[a-f0-9]{64}$"))
      and (.spec.runtime.profile.proxyCredentialRole | length > 0)
      and (.spec.runtime.profile.proxyCredentialScope | length > 0)
      and .status.lifecycle == "Serving"
      and .status.admissionState == "Accepting"
      and .status.currentReplicas == 1
      and .status.desiredReplicas == 1
      and .status.controllerEpoch > 0
      and .status.activeInstance.controllerEpoch == .status.controllerEpoch
      and .status.activeInstance.protocolVersion == "orka.harness.v2"
      and .status.activeInstance.profileDigest == .spec.runtime.profile.digest
      and .status.activeInstance.profileDigestSchemaVersion == .spec.runtime.profile.digestSchemaVersion
      and ((.status.activeInstance.providerTokenGeneration // "") | test("^[a-f0-9]{16}$"))
      and (.status.activeInstance.bootID | length > 0)
      and (.status.activeInstance.podNamespace | length > 0)
      and (.status.activeInstance.podName | length > 0)
      and (.status.activeInstance.podUID | length > 0)
      and .status.activeInstance.runtimeInstanceID ==
        (.status.activeInstance.podUID + "." + .status.activeInstance.bootID)
      and (.status.activeInstance.podAddress | length > 0)
      and ((.status.activeInstance.lastObservedTime // "") | length > 0)
      and $pod.metadata.namespace == .status.activeInstance.podNamespace
      and $pod.metadata.name == .status.activeInstance.podName
      and $pod.metadata.uid == .status.activeInstance.podUID
      and (($pod.metadata.annotations["orka.ai/provider-token-generation"] // "") |
        test("^[a-f0-9]{16}$"))
      and $pod.metadata.annotations["orka.ai/provider-token-generation"] ==
        .status.activeInstance.providerTokenGeneration
      and .status.capacity.maxResidentSessions >= 1
      and .status.capacity.maxRunningPrompts >= 1
      and .status.capacity.maxRunningPrompts <= .status.capacity.maxResidentSessions
    ' <<<"${payload}" >/dev/null
}

pool_profile_projection_fingerprint() {
  local payload="$1"
  local pod_payload="$2"
  jq -cS --argjson pod "${pod_payload}" '{
    poolUID:.metadata.uid,
    poolName:.metadata.name,
    poolGeneration:.metadata.generation,
    desiredReplicas:.spec.desiredReplicas,
    trustDomain:.spec.trustDomain,
    runtime:{
      image:.spec.runtime.image,
      profile:.spec.runtime.profile
    },
    status:{
      lifecycle:.status.lifecycle,
      admissionState:.status.admissionState,
      currentReplicas:.status.currentReplicas,
      desiredReplicas:.status.desiredReplicas,
      observedGeneration:.status.observedGeneration,
      controllerEpoch:.status.controllerEpoch,
      activeInstance:{
        controllerEpoch:.status.activeInstance.controllerEpoch,
        protocolVersion:.status.activeInstance.protocolVersion,
        profileDigest:.status.activeInstance.profileDigest,
        profileDigestSchemaVersion:.status.activeInstance.profileDigestSchemaVersion,
        providerTokenGeneration:.status.activeInstance.providerTokenGeneration,
        runtimeInstanceID:.status.activeInstance.runtimeInstanceID,
        bootID:.status.activeInstance.bootID,
        podNamespace:.status.activeInstance.podNamespace,
        podName:.status.activeInstance.podName,
        podUID:.status.activeInstance.podUID,
        podAddress:.status.activeInstance.podAddress
      },
      capacity:{
        maxResidentSessions:.status.capacity.maxResidentSessions,
        maxRunningPrompts:.status.capacity.maxRunningPrompts
      }
    },
    selectedPod:{
      namespace:$pod.metadata.namespace,
      name:$pod.metadata.name,
      uid:$pod.metadata.uid,
      providerTokenGeneration:$pod.metadata.annotations["orka.ai/provider-token-generation"]
    }
  }' <<<"${payload}"
}

pool_profile_projection_stable() {
  local pool="$1"
  local provider="$2"
  local model="$3"
  local intent="$4"
  local output_file="$5"
  local first_payload first_pod_payload first_pod_ns first_pod_name
  local second_payload second_pod_payload second_pod_ns second_pod_name
  local first_fingerprint second_fingerprint candidate_file

  first_payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  first_pod_ns="$(jq -r '.status.activeInstance.podNamespace // ""' <<<"${first_payload}")" || return 1
  first_pod_name="$(jq -r '.status.activeInstance.podName // ""' <<<"${first_payload}")" || return 1
  [[ -n "${first_pod_ns}" && -n "${first_pod_name}" ]] || return 1
  first_pod_payload="$(k -n "${first_pod_ns}" get pod "${first_pod_name}" -o json 2>/dev/null)" || return 1
  pool_profile_projection_valid \
    "${pool}" "${provider}" "${model}" "${intent}" "${first_payload}" "${first_pod_payload}" || return 1
  first_fingerprint="$(pool_profile_projection_fingerprint "${first_payload}" "${first_pod_payload}")" || return 1

  sleep 0.25

  second_payload="$(k -n "${namespace}" get runtimepool "${pool}" -o json 2>/dev/null)" || return 1
  second_pod_ns="$(jq -r '.status.activeInstance.podNamespace // ""' <<<"${second_payload}")" || return 1
  second_pod_name="$(jq -r '.status.activeInstance.podName // ""' <<<"${second_payload}")" || return 1
  [[ -n "${second_pod_ns}" && -n "${second_pod_name}" ]] || return 1
  second_pod_payload="$(k -n "${second_pod_ns}" get pod "${second_pod_name}" -o json 2>/dev/null)" || return 1
  pool_profile_projection_valid \
    "${pool}" "${provider}" "${model}" "${intent}" "${second_payload}" "${second_pod_payload}" || return 1
  second_fingerprint="$(pool_profile_projection_fingerprint "${second_payload}" "${second_pod_payload}")" || return 1
  [[ "${first_fingerprint}" == "${second_fingerprint}" ]] || return 1

  candidate_file="${output_file}.candidate.$$"
  printf '%s\n' "${second_payload}" >"${candidate_file}"
  mv "${candidate_file}" "${output_file}"
}

wait_pool_profile_projection() {
  local pool="$1"
  local provider="$2"
  local model="$3"
  local intent="$4"
  local output_file="$5"
  wait_until_fast "RuntimePool/${pool} complete stable ACP v2 ${provider}/${intent} profile projection" \
    "${wait_seconds}" pool_profile_projection_stable \
    "${pool}" "${provider}" "${model}" "${intent}" "${output_file}" || \
    die "RuntimePool/${pool} did not reach the expected stable ACP v2 ${provider}/${intent} profile projection"
}

capture_pool_snapshot() {
  local pool="$1"
  local provider="$2"
  local model="$3"
  local intent="$4"
  local output_file="$5"
  local pool_file pod_file pod_ns pod_name pool_uid pod_uid pod_ip pod_address image pod_runtime_image pod_count
  pool_file="${temp_root}/pool-${pool}.json"
  wait_pool_profile_projection "${pool}" "${provider}" "${model}" "${intent}" "${pool_file}"
  pool_uid="$(jq -r '.metadata.uid // ""' "${pool_file}")"
  pod_ns="$(jq -r '.status.activeInstance.podNamespace // ""' "${pool_file}")"
  pod_name="$(jq -r '.status.activeInstance.podName // ""' "${pool_file}")"
  pod_uid="$(jq -r '.status.activeInstance.podUID // ""' "${pool_file}")"
  pod_address="$(jq -r '.status.activeInstance.podAddress // ""' "${pool_file}")"
  image="$(jq -r '.spec.runtime.image // ""' "${pool_file}")"
  [[ -n "${pool_uid}" && -n "${pod_ns}" && -n "${pod_name}" && -n "${pod_uid}" && -n "${pod_address}" ]] || \
    die "RuntimePool/${pool} has an incomplete active-instance fence"
  record_runtime_namespace "${pod_ns}"
  pod_file="${temp_root}/pod-${pool}.json"
  k -n "${pod_ns}" get pod "${pod_name}" -o json >"${pod_file}"
  pod_ip="$(jq -r '.status.podIP // ""' "${pod_file}")"
  pod_runtime_image="$(jq -r '.spec.containers[] | select(.name == "runtime") | .image' "${pod_file}")"
  [[ "${pod_runtime_image}" == "${image}" ]] || \
    die "RuntimePool/${pool} selected Pod runtime image does not match spec.runtime.image"
  [[ "$(jq -r '.metadata.uid // ""' "${pod_file}")" == "${pod_uid}" ]] || \
    die "RuntimePool/${pool} activeInstance.podUID does not match Pod/${pod_name} metadata.uid"
  [[ "${pod_ip}" == "${pod_address}" ]] || \
    die "RuntimePool/${pool} podAddress ${pod_address} is not the exact Pod IP ${pod_ip}"
  pod_count="$(k -n "${pod_ns}" get pods \
    -l "orka.ai/runtime-pool-namespace=${namespace},orka.ai/runtime-pool-name=${pool}" -o json |
    jq '[.items[] | select(.metadata.deletionTimestamp == null)] | length')"
  [[ "${pod_count}" == "1" ]] || die "RuntimePool/${pool} has ${pod_count} live Pods, want exactly 1"
  assert_all_pod_images "${pod_ns}" "${pod_name}"
  jq -n --slurpfile pool "${pool_file}" --slurpfile pod "${pod_file}" '{
    poolName:$pool[0].metadata.name,
    poolUID:$pool[0].metadata.uid,
    controllerEpoch:$pool[0].status.controllerEpoch,
    profileDigest:$pool[0].spec.runtime.profile.digest,
    profileDigestSchemaVersion:$pool[0].spec.runtime.profile.digestSchemaVersion,
    runtimeImage:$pool[0].spec.runtime.image,
    runtimeInstanceID:$pool[0].status.activeInstance.runtimeInstanceID,
    supervisorBootID:$pool[0].status.activeInstance.bootID,
    podNamespace:$pool[0].status.activeInstance.podNamespace,
    podName:$pool[0].status.activeInstance.podName,
    podUID:$pool[0].status.activeInstance.podUID,
    podAddress:$pool[0].status.activeInstance.podAddress,
    podImageID:($pod[0].status.containerStatuses[] | select(.name == "runtime") | .imageID)
  }' >"${output_file}"
}

assert_task_fence() {
  local task="$1"
  local snapshot="$2"
  local payload_file="${temp_root}/task-${task}.json"
  task_json "${task}" >"${payload_file}"
  if ! jq -e --slurpfile snapshot "${snapshot}" '
    $snapshot[0] as $s
    | .status.execution as $e
    | ($e.runtimePoolName == $s.poolName)
      and (.metadata.labels["orka.ai/runtime-pool"] == $s.poolName)
      and ($e.runtimePoolUID == $s.poolUID)
      and ($e.runtimeInstanceID == $s.runtimeInstanceID)
      and ($e.controllerEpoch == $s.controllerEpoch)
      and (($e.promptID // "") | length > 0)
      and (($e.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
      and (($e.runtimeSessionUID // "") | length > 0)
      and (($e.runtimeSessionGeneration // 0) >= 1)
      and (($e.attempt // 0) == 1)
  ' "${payload_file}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} execution fence does not match the exact RuntimePool/Pod/profile snapshot"
  fi
}


assert_restart_task_fence() {
  local task="$1"
  local snapshot="$2"
  local payload_file="${temp_root}/task-${task}.json"
  task_json "${task}" >"${payload_file}"
  if ! jq -e --slurpfile snapshot "${snapshot}" '
    $snapshot[0] as $s
    | .status.execution as $e
    | ($e.runtimePoolName == $s.poolName)
      and (.metadata.labels["orka.ai/runtime-pool"] == $s.poolName)
      and ($e.runtimePoolUID == $s.poolUID)
      and ($e.runtimeInstanceID == $s.runtimeInstanceID)
      and (($e.controllerEpoch | type) == "number")
      and (($s.controllerEpoch | type) == "number")
      and (($e.controllerEpoch | floor) == $e.controllerEpoch)
      and (($s.controllerEpoch | floor) == $s.controllerEpoch)
      and ($e.controllerEpoch >= $s.controllerEpoch)
      and (($e.promptID // "") | length > 0)
      and (($e.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
      and (($e.runtimeSessionUID // "") | length > 0)
      and (($e.runtimeSessionGeneration // 0) >= 1)
      and (($e.attempt // 0) == 1)
  ' "${payload_file}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} restart recovery fence does not preserve the exact pre-restart runtime identity"
  fi
}

mark_task_validated() {
  local task="$1"
  if ! k -n "${namespace}" patch task "${task}" --type=merge \
      -p '{"metadata":{"labels":{"orka.ai/acp-e2e-validated":"true"}}}' >/dev/null; then
    die "failed to mark Task/${task} as release-gate validated"
  fi
}

assert_task_succeeded_read() {
  local task="$1"
  local snapshot="$2"
  local expected_sha="$3"
  local expected_nonce="${4:-}"
  local expected_text="${5:-}"
  local payload result
  wait_task_terminal "${task}"
  payload="$(task_json "${task}")"
  if ! jq -e --arg sha "${expected_sha}" '
    .status.phase == "Succeeded"
    and .status.execution.state == "Succeeded"
    and .status.execution.outcome == "Succeeded"
    and .status.delivery.state == "ReadValidated"
    and .status.delivery.outcome == "ReadValidated"
    and .status.delivery.startingSHA == $sha
    and (.status.resultRef.available == true)
    and ((.status.jobName // "") == "")
  ' <<<"${payload}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} did not produce a successful ReadValidated result"
  fi
  assert_task_fence "${task}" "${snapshot}"
  if [[ "${release_gate}" -eq 1 && ( -n "${expected_nonce}" || -n "${expected_text}" ) ]]; then
    result="$(api_task_result "${task}")" || die "failed to fetch Task/${task} result from the authenticated API"
    if [[ -n "${expected_nonce}" ]] && ! grep -Fq -- "${expected_nonce}" <<<"${result}"; then
      die "Task/${task} result did not contain the release-gate nonce"
    fi
    if [[ -n "${expected_text}" ]] && ! grep -Fq -- "${expected_text}" <<<"${result}"; then
      die "Task/${task} result did not contain independently observed repository content"
    fi
    # Provider CLIs have leaked startup diagnostics into the agent message
    # stream before (Codex WebSocket fallback, Copilot tool exclusions); the
    # supervisor must keep them out of the result text.
    if grep -Eq 'Info: (Disabled tools|Unknown tool name in the tool excludedlist)|Warning: Falling back from WebSockets' <<<"${result}"; then
      die "Task/${task} result contains provider CLI diagnostics"
    fi
  fi
  mark_task_validated "${task}"
}

# assert_cancelled_task requires a controller-owned Cancelled settlement. The
# execution reason distinguishes why: an explicit cancellation request settles
# with reason "Cancelled", while a Task deadline settles with reason
# "TaskTimeout" (message "task deadline cancellation settled").
assert_cancelled_task() {
  local task="$1"
  local snapshot="$2"
  local expected_reason="${3:-Cancelled}"
  wait_task_terminal "${task}"
  local payload
  payload="$(task_json "${task}")"
  if ! jq -e --arg reason "${expected_reason}" '
    .status.phase == "Cancelled"
    and .status.execution.state == "Cancelled"
    and .status.execution.outcome == "Cancelled"
    and .status.execution.reason == $reason
    and ((.status.jobName // "") == "")
  ' <<<"${payload}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} cancellation did not settle as controller-owned Cancelled with reason ${expected_reason}"
  fi
  assert_task_fence "${task}" "${snapshot}"
  mark_task_validated "${task}"
}

assert_restart_task_settled() {
  local task="$1"
  local snapshot="$2"
  wait_task_terminal "${task}"
  local payload
  payload="$(task_json "${task}")"
  if ! jq -e '
    (
      .status.phase == "Succeeded"
      and .status.execution.state == "Succeeded"
      and .status.execution.outcome == "Succeeded"
      and .status.delivery.state == "ReadValidated"
      and .status.delivery.outcome == "ReadValidated"
    )
    or (
      .status.phase == "Cancelled"
      and .status.execution.state == "Cancelled"
      and .status.execution.outcome == "Cancelled"
    )
    or (
      .status.phase == "Failed"
      and .status.execution.state == "OutcomeUnknown"
      and .status.execution.outcome == "OutcomeUnknown"
    )
  ' <<<"${payload}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} did not settle safely across controller restart"
  fi
  [[ "$(jq -r '.status.execution.attempt // 0' <<<"${payload}")" == "1" ]] || \
    die "Task/${task} was replayed after controller restart"
  assert_restart_task_fence "${task}" "${snapshot}"
  mark_task_validated "${task}"
}

# assert_all_tasks_validated requires every Task this run created to have
# passed exact fence/result validation. It is scoped to run-owned Tasks so a
# shared watch namespace (adopted harness-v2 mode) that also hosts unrelated
# Tasks does not fail the release gate.
assert_all_tasks_validated() {
  local unvalidated
  unvalidated="$(k -n "${namespace}" get tasks -l "orka.ai/acp-e2e-run=${run_id}" -o json | jq -r '
    .items[]
    | select((.status.execution.state // "") != "" or (.status.phase // "") != "")
    | select(.metadata.labels["orka.ai/acp-e2e-validated"] != "true")
    | .metadata.name
  ')"
  if [[ -n "${unvalidated}" ]]; then
    printf '%s\n' "${unvalidated}" >&2
    die "one or more Tasks reached controller processing without exact fence/result validation"
  fi
}

read_smoke_pool=""
read_smoke_session_uid=""
read_smoke_session_generation=""
run_read_smoke() {
  local provider="$1"
  local model="$2"
  local agent="$3"
  local task="$4"
  local session="$5"
  local nonce="$6"
  local expected_line="$7"
  local continuation fork_task pool snapshot continuation_snapshot fork_snapshot fork_response fork_after_seq
  apply_agent "${provider}" "${model}" "${agent}"
  apply_read_task "${task}" "${agent}" "${session}" true \
    "Read LICENSE. Remember the session nonce ${nonce}. Report the nonce and the first non-empty LICENSE line exactly. Do not modify any file." \
    "12m" false
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  snapshot="${temp_root}/${task}-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${snapshot}"
  assert_task_succeeded_read "${task}" "${snapshot}" "${read_repo_commit}" "${nonce}" "${expected_line}"

  continuation="$(sanitize_name "${task}-continue")"
  [[ "${continuation}" != "${task}" ]] || die "continuation Task name collided with source Task name"
  apply_read_task "${continuation}" "${agent}" "${session}" false \
    "Continue the existing session. Return the session nonce from the prior turn and no repository-derived replacement. Do not modify files." \
    "12m" false
  wait_until "Task/${continuation} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${continuation}"
  [[ "$(wait_task_pool_name_value "${continuation}")" == "${pool}" ]] || \
    die "continuation Task/${continuation} selected a different RuntimePool"
  continuation_snapshot="${temp_root}/${continuation}-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${continuation_snapshot}"
  assert_task_succeeded_read "${continuation}" "${continuation_snapshot}" "${read_repo_commit}" "${nonce}" ""

  local first_uid first_generation continued_uid continued_generation
  first_uid="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  first_generation="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  continued_uid="$(k -n "${namespace}" get task "${continuation}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  continued_generation="$(k -n "${namespace}" get task "${continuation}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ -n "${first_uid}" && "${first_uid}" == "${continued_uid}" ]] || \
    die "create:false continuation did not preserve RuntimeSession UID"
  [[ "${first_generation}" == "${continued_generation}" ]] || \
    die "create:false continuation unexpectedly changed RuntimeSession generation without replacement"

  if [[ "${release_gate}" -eq 1 ]]; then
    fork_task="$(sanitize_name "${task}-fork")"
    [[ "${fork_task}" != "${task}" && "${fork_task}" != "${continuation}" ]] || \
      die "forked Task name collided with source or continuation Task name"
    fork_response="$(api_fork_task "${continuation}" "${fork_task}" \
      "Return the session nonce represented in the source task timeline. Do not modify files.")" || \
      die "Task fork API request failed"
    jq -e --arg source "${continuation}" --arg fork "${fork_task}" --arg ns "${namespace}" '
      .namespace == $ns and .sourceTaskName == $source and .newTaskName == $fork and .afterSeq >= 1
    ' <<<"${fork_response}" >/dev/null || die "Task fork API response did not identify the requested checkpoint fork"
    fork_after_seq="$(jq -r '.afterSeq' <<<"${fork_response}")"
    if ! k -n "${namespace}" patch task "${fork_task}" --type=merge \
        -p "$(jq -cn --arg run "${run_id}" '{metadata:{labels:{"orka.ai/acp-e2e-run":$run}}}')" >/dev/null; then
      die "failed to mark forked Task/${fork_task} as owned by this release-gate run"
    fi
    wait_until "forked Task/${fork_task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${fork_task}"
    [[ "$(wait_task_pool_name_value "${fork_task}")" == "${pool}" ]] || \
      die "forked Task/${fork_task} selected a different RuntimePool"
    fork_snapshot="${temp_root}/${fork_task}-pool.json"
    capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${fork_snapshot}"
    assert_task_succeeded_read "${fork_task}" "${fork_snapshot}" "${read_repo_commit}" "${nonce}" ""
    jq -e --arg source "${continuation}" --arg seq "${fork_after_seq}" '
      .metadata.annotations["orka.ai/fork-source-task"] == $source
      and .metadata.annotations["orka.ai/fork-source-seq"] == $seq
    ' < <(task_json "${fork_task}") >/dev/null || die "forked Task/${fork_task} does not bind to the API checkpoint sequence"
  fi

  read_smoke_pool="${pool}"
  read_smoke_session_uid="${continued_uid}"
  read_smoke_session_generation="${continued_generation}"
}

run_opencode_read_policy_check() {
  local agent="$1"
  local model="$2"
  local task session pool snapshot
  task="$(sanitize_name "acp-opencode-policy-${run_id}")"
  session="$(sanitize_name "acp-opencode-policy-session-${run_id}")"
  apply_read_task "${task}" "${agent}" "${session}" true \
    "Attempt to use Bash and a file-mutation tool to create SHOULD_NOT_EXIST.txt. Those tools must be unavailable in this read-intent session. Then read LICENSE and report its first non-empty line without modifying any file." \
    "12m" true
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  snapshot="${temp_root}/${task}-pool.json"
  capture_pool_snapshot "${pool}" opencode "${model}" read "${snapshot}"
  assert_task_succeeded_read "${task}" "${snapshot}" "${read_repo_commit}" "" "${expected_license_line}"
}

server_dry_run_manifest() {
  k apply --dry-run=server -f - >/dev/null
}

assert_dry_run_rejected() {
  local manifest_file="$1"
  local field="$2"
  local expected_message="$3"
  local output_file="${temp_root}/unsafe-output.txt"
  local rc
  if k apply --dry-run=server -f "${manifest_file}" >"${output_file}" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -eq 0 ]]; then
    die "unsafe workspace manifest ${manifest_file} was accepted"
  fi
  if ! grep -Fq -- "${field}" "${output_file}" || ! grep -Fq -- "${expected_message}" "${output_file}"; then
    cat "${output_file}" | redact >&2
    die "unsafe workspace rejection did not identify ${field} with the exact validation reason"
  fi
}

build_read_manifest() {
  local name="$1"
  local agent="$2"
  local git_repo="$3"
  local output_file="$4"
  jq -n \
    --arg ns "${namespace}" --arg name "${name}" --arg agent "${agent}" \
    --arg repo "${git_repo}" --arg ref "${repo_ref}" '{
      apiVersion:"core.orka.ai/v1alpha1",kind:"Task",metadata:{name:$name,namespace:$ns},
      spec:{type:"agent",agentRef:{name:$agent},prompt:"dry-run URL validation",workspace:{intent:"read",gitRepo:$repo,ref:$ref}}
    }' >"${output_file}"
}

build_write_manifest() {
  local name="$1"
  local agent="$2"
  local source_repo="$3"
  local publication_repo="$4"
  local output_file="$5"
  jq -n \
    --arg ns "${namespace}" --arg name "${name}" --arg agent "${agent}" \
    --arg source "${source_repo}" --arg publication "${publication_repo}" \
    --arg ref "${write_source_commit:-${repo_ref}}" --arg credential "${write_credential_target:-dry-run-credential}" '{
      apiVersion:"core.orka.ai/v1alpha1",kind:"Task",metadata:{name:$name,namespace:$ns},
      spec:{
        type:"agent",agentRef:{name:$agent},prompt:"dry-run publication URL validation",
        workspace:{
          intent:"write",gitRepo:$source,ref:$ref,publicationGitRepo:$publication,
          publicationCredentialRef:{name:$credential},pushBranch:"dry-run",prBaseBranch:"main",createPR:false
        }
      }
    }' >"${output_file}"
}

assert_controller_rejected_before_demand() {
  local agent="$1"
  local unsafe_repo="$2"
  local suffix="$3"
  local task
  task="$(sanitize_name "acp-unsafe-${suffix}-${run_id}")"
  apply_invalid_read_task "${task}" "${agent}" "${unsafe_repo}"
  wait_task_terminal "${task}"
  local payload
  payload="$(task_json "${task}")"
  if ! jq -e '
    .status.phase == "Failed"
    and .status.execution.state == "Failed"
    and .status.execution.outcome == "Failed"
    and .status.execution.reason == "InvalidWorkspace"
    and .status.delivery.state == "NotRequested"
    and .status.delivery.outcome == "NotRequested"
    and ((.status.execution.runtimePoolName // "") == "")
    and ((.status.execution.runtimePoolUID // "") == "")
    and ((.status.execution.runtimeInstanceID // "") == "")
    and ((.status.execution.promptID // "") == "")
  ' <<<"${payload}" >/dev/null; then
    safe_task_summary "${task}"
    die "Task/${task} was not rejected before RuntimePool demand and prompt fencing"
  fi
  mark_task_validated "${task}"
}

assert_unsafe_workspace_rejected() {
  local agent="$1"
  local safe_read="${temp_root}/safe-read.json"
  local unsafe_file="${temp_root}/unsafe.json"
  local safe_write="${temp_root}/safe-write.json"
  local message='gitRepo must not contain embedded credentials, query parameters, or fragments'
  local publication_message='publicationGitRepo must not contain embedded credentials, query parameters, or fragments'
  log "Validating safe positive controls and unsafe workspace URL rejection"

  build_read_manifest "$(sanitize_name "acp-safe-read-${run_id}")" "${agent}" "${repo_url}" "${safe_read}"
  server_dry_run_manifest <"${safe_read}" || die "safe read workspace positive control was rejected"
  log "Safe read workspace URL control passed"

  build_read_manifest "$(sanitize_name "acp-unsafe-query-${run_id}")" "${agent}" "${repo_url}?unexpected=query" "${unsafe_file}"
  assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${message}"
  build_read_manifest "$(sanitize_name "acp-unsafe-fragment-${run_id}")" "${agent}" "${repo_url}#fragment" "${unsafe_file}"
  assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${message}"
  build_read_manifest "$(sanitize_name "acp-unsafe-user-${run_id}")" "${agent}" "https://user@github.com/orka-agents/orka.git" "${unsafe_file}"
  assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${message}"
  log "Unsafe read workspace URL controls passed"

  if [[ "${release_gate}" -eq 1 ]]; then
    build_write_manifest "$(sanitize_name "acp-safe-write-${run_id}")" "${agent}" \
      "${write_source_repo}" "${write_publication_repo}" "${safe_write}"
    server_dry_run_manifest <"${safe_write}" || die "safe write workspace positive control was rejected"
    log "Safe write workspace URL control passed"
    build_write_manifest "$(sanitize_name "acp-unsafe-publication-query-${run_id}")" "${agent}" \
      "${write_source_repo}" "${write_publication_repo}?unexpected=query" "${unsafe_file}"
    assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${publication_message}"
    build_write_manifest "$(sanitize_name "acp-unsafe-publication-fragment-${run_id}")" "${agent}" \
      "${write_source_repo}" "${write_publication_repo}#fragment" "${unsafe_file}"
    assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${publication_message}"
    build_write_manifest "$(sanitize_name "acp-unsafe-publication-user-${run_id}")" "${agent}" \
      "${write_source_repo}" "https://user@github.com/${write_publication_slug}.git" "${unsafe_file}"
    assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${publication_message}"
    log "Unsafe publication workspace URL controls passed"

    build_read_manifest "$(sanitize_name "acp-unsafe-ssh-${run_id}")" "${agent}" \
      "ssh://git@github.com/orka-agents/orka.git" "${unsafe_file}"
    assert_dry_run_rejected "${unsafe_file}" "spec.workspace" "${message}"
    log "Unsafe SSH workspace URL control passed"
    assert_controller_rejected_before_demand "${agent}" "ext::https://github.com/orka-agents/orka.git" helper
    log "Unsafe remote-helper workspace URL control passed"
  fi
}

run_concurrency_check() {
  local agent="$1"
  local provider="$2"
  local model="$3"
  local expected_pool="$4"
  local snapshot="${temp_root}/concurrency-pool.json"
  local tasks_file="${temp_root}/concurrency-tasks.txt"
  local i task deadline max_seen queued_seen all_terminal running queued pool_name phase terminal_count limit
  : >"${tasks_file}"
  capture_pool_snapshot "${expected_pool}" "${provider}" "${model}" read "${snapshot}"
  log "Submitting ${concurrency_tasks} task-scoped Codex Tasks to one exact RuntimePool instance"
  for ((i = 1; i <= concurrency_tasks; i++)); do
    task="$(sanitize_name "acp-concurrent-${i}-${run_id}")"
    printf '%s\n' "${task}" >>"${tasks_file}"
    apply_read_task "${task}" "${agent}" "" false "${long_prompt}" "12m" false
  done

  deadline=$((SECONDS + wait_seconds))
  max_seen=0
  queued_seen=0
  all_terminal=0
  while (( SECONDS < deadline )); do
    running="$(k -n "${namespace}" get runtimepool "${expected_pool}" -o jsonpath='{.status.capacity.runningPrompts}' 2>/dev/null || printf '0')"
    queued="$(k -n "${namespace}" get runtimepool "${expected_pool}" -o jsonpath='{.status.capacity.queuedTasks}' 2>/dev/null || printf '0')"
    is_uint "${running}" || running=0
    is_uint "${queued}" || queued=0
    (( running > max_seen )) && max_seen="${running}"
    (( queued > queued_seen )) && queued_seen="${queued}"
    terminal_count=0
    while IFS= read -r task; do
      pool_name="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimePoolName}' 2>/dev/null || true)"
      if [[ -n "${pool_name}" && "${pool_name}" != "${expected_pool}" ]]; then
        die "Task/${task} selected RuntimePool/${pool_name}, want ${expected_pool}"
      fi
      phase="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.state}' 2>/dev/null || true)"
      case "${phase}" in
        Succeeded|Failed|Cancelled|OutcomeUnknown) terminal_count=$((terminal_count + 1)) ;;
      esac
    done <"${tasks_file}"
    if (( terminal_count == concurrency_tasks )); then
      all_terminal=1
      break
    fi
    sleep 1
  done
  (( all_terminal == 1 )) || die "concurrent Tasks did not all reach terminal execution states"

  limit="$(k -n "${namespace}" get runtimepool "${expected_pool}" -o jsonpath='{.status.capacity.maxRunningPrompts}')"
  is_uint "${limit}" || die "RuntimePool/${expected_pool} reported invalid maxRunningPrompts=${limit}"
  (( max_seen <= limit )) || die "observed ${max_seen} running prompts above configured limit ${limit}"
  if (( concurrency_tasks > limit && queued_seen < 1 )); then
    die "capacity burst never exposed durable queued demand"
  fi
  if [[ "${release_gate}" -eq 1 && "${max_seen}" -lt 2 ]]; then
    die "release gate did not observe at least two concurrent running prompts"
  fi
  if [[ "${release_gate}" -eq 0 ]] && bool_env "${require_parallel}" && (( max_seen < 2 )); then
    die "parallel prompt execution was not observed (max=${max_seen})"
  fi

  local uid_file="${temp_root}/concurrency-uids.txt"
  : >"${uid_file}"
  while IFS= read -r task; do
    assert_task_succeeded_read "${task}" "${snapshot}" "${read_repo_commit}" "" ""
    k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionUID}' >>"${uid_file}"
    printf '\n' >>"${uid_file}"
  done <"${tasks_file}"
  [[ "$(grep -c . "${uid_file}")" == "${concurrency_tasks}" ]] || die "concurrent Tasks have empty RuntimeSession UIDs"
  [[ "$(sort -u "${uid_file}" | grep -c .)" == "${concurrency_tasks}" ]] || \
    die "concurrent Tasks did not receive distinct task-scoped RuntimeSession UIDs"
  wait_until "RuntimePool/${expected_pool} transient counters to settle" "${state_wait_seconds}" pool_transient_counters_zero "${expected_pool}" 1
  log "Shared-pool concurrency passed (maxRunning=${max_seen}, queued=${queued_seen}, limit=${limit})"
}

tool_profile_pool=""
run_timeout_check() {
  local provider="$1"
  local model="$2"
  local agent="$3"
  local task pool snapshot start_seconds elapsed_seconds
  task="$(sanitize_name "acp-timeout-${run_id}")"
  log "Validating Task timeout after first proving the prompt reached Running"
  start_seconds="${SECONDS}"
  apply_read_task "${task}" "${agent}" "" false "${blocking_prompt}" "${timeout_duration}" true
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  tool_profile_pool="${pool}"
  wait_pool_serving "${pool}"
  wait_until "Task/${task} active Running prompt" "${state_wait_seconds}" pool_running_for_task "${task}" "${pool}"
  snapshot="${temp_root}/${task}-running-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${snapshot}"
  assert_cancelled_task "${task}" "${snapshot}" "TaskTimeout"
  elapsed_seconds=$((SECONDS - start_seconds))
  (( elapsed_seconds >= timeout_duration_seconds )) || \
    die "timeout Task/${task} cancelled before its configured ${timeout_duration} deadline"
  wait_until "RuntimePool/${pool} zero transient counters after timeout" "${state_wait_seconds}" pool_transient_counters_zero "${pool}"
}

run_explicit_cancel_check() {
  local provider="$1"
  local model="$2"
  local agent="$3"
  local task pool snapshot uid cancel_requested_at
  task="$(sanitize_name "acp-cancel-${run_id}")"
  log "Validating explicit cancellation after an active Running prompt"
  apply_read_task "${task}" "${agent}" "" false "${blocking_prompt}" "1h" true
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  [[ -n "${tool_profile_pool}" && "${pool}" == "${tool_profile_pool}" ]] || \
    die "explicit-cancel check selected RuntimePool/${pool}, want shared tool RuntimePool/${tool_profile_pool}"
  wait_pool_serving "${pool}"
  wait_until "Task/${task} active Running prompt" "${state_wait_seconds}" pool_running_for_task "${task}" "${pool}"
  snapshot="${temp_root}/${task}-running-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${snapshot}"
  pool_running_for_task "${task}" "${pool}" || die "Task/${task} was no longer Running immediately before cancellation"
  uid="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.metadata.uid}')"
  [[ -n "${uid}" ]] || die "Task/${task} UID is unavailable before cancellation"
  add_task_observer_finalizer "${task}" "${uid}" || \
    die "failed to retain Task/${task} for cancellation settlement observation"
  cancel_requested_at="${SECONDS}"
  request_task_cancellation "${task}" || die "failed to request Task/${task} cancellation"
  wait_until "Task/${task} explicit cancellation settlement" "${cancel_settle_seconds}" task_execution_terminal "${task}" || \
    die "Task/${task} did not settle within the explicit cancellation bound"
  (( SECONDS - cancel_requested_at <= cancel_settle_seconds + 2 )) || \
    die "Task/${task} exceeded the explicit cancellation settlement bound"
  assert_cancelled_task "${task}" "${snapshot}"
  wait_until "Task/${task} controller-owned deletion barrier" "${state_wait_seconds}" \
    task_observer_release_ready "${task}" "${uid}" || \
    die "Task/${task} controller cleanup did not reach its finalizer barrier"
  release_task_observer_finalizer "${task}" "${uid}" || \
    die "failed to release Task/${task} cancellation observer"
  wait_until "Task/${task} deletion after observed cancellation" "${state_wait_seconds}" task_absent "${task}" || \
    die "Task/${task} remained after releasing the cancellation observer"
  wait_until "RuntimePool/${pool} zero transient counters after explicit cancel" "${state_wait_seconds}" pool_transient_counters_zero "${pool}"
}

run_controller_restart_check() {
  require_runtimepool_mutation_scope || return 1
  local provider="$1"
  local model="$2"
  local agent="$3"
  local nonce="$4"
  local task continuation session pool old_snapshot new_snapshot old_instance old_epoch old_uid old_generation new_uid new_generation
  task="$(sanitize_name "acp-restart-${run_id}")"
  continuation="$(sanitize_name "acp-post-restart-${run_id}")"
  session="$(sanitize_name "acp-restart-session-${run_id}")"
  log "Validating controller restart while an ACP prompt is actively Running"
  apply_read_task "${task}" "${agent}" "${session}" true \
    "Remember restart nonce ${nonce}. ${blocking_prompt}" "1h" true
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  [[ -n "${tool_profile_pool}" && "${pool}" == "${tool_profile_pool}" ]] || \
    die "controller-restart check selected RuntimePool/${pool}, want shared tool RuntimePool/${tool_profile_pool}"
  wait_pool_serving "${pool}"
  wait_until "Task/${task} active Running prompt" "${state_wait_seconds}" pool_running_for_task "${task}" "${pool}"
  old_snapshot="${temp_root}/${task}-pre-restart-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${old_snapshot}"
  old_instance="$(jq -r '.runtimeInstanceID' "${old_snapshot}")"
  old_epoch="$(jq -r '.controllerEpoch' "${old_snapshot}")"
  old_uid="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  old_generation="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  pool_running_for_task "${task}" "${pool}" || die "Task/${task} was no longer Running immediately before controller restart"

  stop_api_forward || die "failed to stop controller API port-forward before restart"
  k -n "${orka_namespace}" rollout restart deployment/"${controller_deployment}" >/dev/null
  k -n "${orka_namespace}" rollout status deployment/"${controller_deployment}" --timeout=10m >/dev/null
  if [[ "${release_gate}" -eq 1 ]]; then
    assert_deployment_digest "${controller_deployment}" "${controller_container_override}" manager controller
    start_api_forward
  fi
  assert_restart_task_settled "${task}" "${old_snapshot}"
  resume_runtimepool "${pool}"
  wait_until "RuntimePool/${pool} replacement after controller epoch change" "${wait_seconds}" \
    pool_instance_changed_and_serving "${pool}" "${old_instance}"
  new_snapshot="${temp_root}/${task}-post-restart-pool.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${new_snapshot}"
  (( $(jq -r '.controllerEpoch' "${new_snapshot}") > old_epoch )) || \
    die "controller epoch did not advance across restart"

  apply_read_task "${continuation}" "${agent}" "${session}" false \
    "Continue the existing session after controller replacement. Return the restart nonce from the prior user turn. Do not modify files." \
    "12m" true
  wait_until "Task/${continuation} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${continuation}"
  [[ "$(wait_task_pool_name_value "${continuation}")" == "${pool}" ]] || \
    die "post-restart continuation selected a different RuntimePool"
  assert_task_succeeded_read "${continuation}" "${new_snapshot}" "${read_repo_commit}" "${nonce}" ""
  new_uid="$(k -n "${namespace}" get task "${continuation}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  new_generation="$(k -n "${namespace}" get task "${continuation}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ -n "${old_uid}" && "${new_uid}" == "${old_uid}" ]] || \
    die "controller restart continuation did not preserve the logical RuntimeSession UID"
  (( new_generation > old_generation )) || \
    die "controller restart continuation did not advance RuntimeSession generation"
}

replacement_session_uid=""
replacement_session_generation=""
run_pool_replacement_check() {
  require_runtimepool_mutation_scope || return 1
  local provider="$1"
  local model="$2"
  local agent="$3"
  local pool="$4"
  local session="$5"
  local nonce="$6"
  local before_snapshot after_snapshot before_instance before_uid before_generation pod_ns pod_name task after_uid after_generation
  log "Validating exact-Pod RuntimePool replacement and create:false recovery"
  before_snapshot="${temp_root}/pool-replacement-before.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${before_snapshot}"
  before_instance="$(jq -r '.runtimeInstanceID' "${before_snapshot}")"
  before_uid="${read_smoke_session_uid}"
  before_generation="${read_smoke_session_generation}"
  pod_ns="$(jq -r '.podNamespace' "${before_snapshot}")"
  pod_name="$(jq -r '.podName' "${before_snapshot}")"
  k -n "${pod_ns}" delete pod "${pod_name}" --wait=false >/dev/null
  wait_until "RuntimePool/${pool} exact-instance replacement" "${wait_seconds}" \
    pool_instance_changed_and_serving "${pool}" "${before_instance}"
  after_snapshot="${temp_root}/pool-replacement-after.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${after_snapshot}"
  [[ "$(jq -r '.podUID' "${after_snapshot}")" != "$(jq -r '.podUID' "${before_snapshot}")" ]] || \
    die "RuntimePool/${pool} Pod UID did not change"
  [[ "$(jq -r '.supervisorBootID' "${after_snapshot}")" != "$(jq -r '.supervisorBootID' "${before_snapshot}")" ]] || \
    die "RuntimePool/${pool} supervisor boot ID did not change"

  task="$(sanitize_name "acp-post-replacement-${run_id}")"
  apply_read_task "${task}" "${agent}" "${session}" false \
    "Continue the existing session after exact Pod replacement. Return the original session nonce from the prior transcript. Do not modify files." \
    "12m" false
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  assert_task_succeeded_read "${task}" "${after_snapshot}" "${read_repo_commit}" "${nonce}" ""
  after_uid="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  after_generation="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${after_uid}" == "${before_uid}" ]] || die "Pod replacement changed logical RuntimeSession UID"
  (( after_generation > before_generation )) || die "Pod replacement did not advance RuntimeSession generation"
  replacement_session_uid="${after_uid}"
  replacement_session_generation="${after_generation}"
}

run_scale_to_zero_recovery_check() {
  require_runtimepool_mutation_scope || return 1
  local provider="$1"
  local model="$2"
  local agent="$3"
  local pool="$4"
  local session="$5"
  local nonce="$6"
  local before_snapshot after_snapshot before_instance before_pool_uid task after_uid after_generation
  log "Validating authenticated drain: Serving -> Draining -> Quiescent -> Stopping -> Stopped"
  before_snapshot="${temp_root}/scale-zero-before.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${before_snapshot}"
  before_instance="$(jq -r '.runtimeInstanceID' "${before_snapshot}")"
  before_pool_uid="$(jq -r '.poolUID' "${before_snapshot}")"

  k -n "${namespace}" patch runtimepool "${pool}" --type=merge \
    -p '{"spec":{"desiredReplicas":0}}' >/dev/null
  wait_until_fast "RuntimePool/${pool} Draining or later" "${state_wait_seconds}" pool_lifecycle_reached "${pool}" Draining
  wait_until_fast "RuntimePool/${pool} Quiescent or later" "${state_wait_seconds}" pool_lifecycle_reached "${pool}" Quiescent
  wait_until_fast "RuntimePool/${pool} Stopping or later" "${state_wait_seconds}" pool_lifecycle_reached "${pool}" Stopping
  wait_until "RuntimePool/${pool} Stopped with zero authenticated/controller counters" "${wait_seconds}" pool_stopped "${pool}"

  log "Validating 0 -> 1 recovery of the same logical Session"
  task="$(sanitize_name "acp-scale-zero-recovery-${run_id}")"
  apply_read_task "${task}" "${agent}" "${session}" false \
    "Continue the existing session after scale-to-zero. Return the original session nonce from the durable transcript. Do not modify files." \
    "12m" false
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  [[ "$(wait_task_pool_name_value "${task}")" == "${pool}" ]] || \
    die "scale-to-zero recovery selected a different logical RuntimePool"
  after_snapshot="${temp_root}/scale-zero-after.json"
  capture_pool_snapshot "${pool}" "${provider}" "${model}" read "${after_snapshot}"
  [[ "$(jq -r '.poolUID' "${after_snapshot}")" == "${before_pool_uid}" ]] || \
    die "0 -> 1 recovery replaced the logical RuntimePool object"
  [[ "$(jq -r '.runtimeInstanceID' "${after_snapshot}")" != "${before_instance}" ]] || \
    die "0 -> 1 recovery reused the stopped runtime instance"
  assert_task_succeeded_read "${task}" "${after_snapshot}" "${read_repo_commit}" "${nonce}" ""
  after_uid="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionUID}')"
  after_generation="$(k -n "${namespace}" get task "${task}" -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${after_uid}" == "${replacement_session_uid}" ]] || \
    die "scale-to-zero recovery changed the logical RuntimeSession UID"
  (( after_generation > replacement_session_generation )) || \
    die "scale-to-zero recovery did not advance RuntimeSession generation"
}

write_source_repo=""
write_publication_repo=""
write_source_slug=""
write_publication_slug=""
write_source_commit=""
write_source_identity=""
write_publication_identity=""
write_source_database_id=""
write_credential_source=""
write_credential_source_namespace=""
write_credential_key=""
write_credential_target=""
write_read_credential_source=""
write_read_credential_source_namespace=""
write_read_credential_key=""
write_read_credential_target=""
write_target_read_credential_source=""
write_target_read_credential_source_namespace=""
write_target_read_credential_key=""
write_target_read_credential_target=""
write_forge_credential_source=""
write_forge_credential_source_namespace=""
write_forge_credential_key=""
write_forge_credential_target=""
write_branch=""
write_pr_base=""
write_expected_file=""
write_expected_content=""
write_prompt=""

prepare_release_gate_environment() {
  [[ "${release_gate}" -eq 1 ]] || return 0
  write_source_repo="${ACP_E2E_WRITE_SOURCE_REPO:-}"
  write_publication_repo="${ACP_E2E_WRITE_PUBLICATION_REPO:-}"
  write_source_commit="${ACP_E2E_WRITE_SOURCE_REF:-}"
  write_credential_source="${ACP_E2E_WRITE_CREDENTIAL_SECRET:-}"
  write_credential_source_namespace="${ACP_E2E_WRITE_CREDENTIAL_NAMESPACE:-}"
  write_credential_key="${ACP_E2E_WRITE_CREDENTIAL_KEY:-token}"
  write_read_credential_source="${ACP_E2E_WRITE_READ_CREDENTIAL_SECRET:-}"
  write_read_credential_source_namespace="${ACP_E2E_WRITE_READ_CREDENTIAL_NAMESPACE:-${write_credential_source_namespace}}"
  write_read_credential_key="${ACP_E2E_WRITE_READ_CREDENTIAL_KEY:-token}"
  write_target_read_credential_source="${ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_SECRET:-}"
  write_target_read_credential_source_namespace="${ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_NAMESPACE:-}"
  write_target_read_credential_key="${ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_KEY:-token}"
  write_forge_credential_source="${ACP_E2E_WRITE_FORGE_CREDENTIAL_SECRET:-}"
  write_forge_credential_source_namespace="${ACP_E2E_WRITE_FORGE_CREDENTIAL_NAMESPACE:-}"
  write_forge_credential_key="${ACP_E2E_WRITE_FORGE_CREDENTIAL_KEY:-token}"
  write_branch="${ACP_E2E_WRITE_BRANCH:-orka/acp-release-gate-${run_id}}"
  write_pr_base="${ACP_E2E_WRITE_PR_BASE:-main}"
  write_expected_file="orka-acp-release-gate-${run_id}.txt"
  write_expected_content="ACP v2 release gate ${run_id}"
  write_prompt="${ACP_E2E_WRITE_PROMPT:-Create exactly one file named ${write_expected_file}. Its entire content must be one line: ${write_expected_content}. Do not modify, rename, or delete any other file.}"

  [[ -n "${write_source_repo}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_SOURCE_REPO"
  [[ -n "${write_publication_repo}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_PUBLICATION_REPO"
  [[ -n "${write_source_commit}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_SOURCE_REF"
  [[ -n "${write_credential_source}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_CREDENTIAL_SECRET"
  [[ -n "${write_credential_source_namespace}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_CREDENTIAL_NAMESPACE"
  [[ -n "${write_read_credential_source}" && -n "${write_read_credential_source_namespace}" ]] || \
    die "RELEASE_GATE=1 requires ACP_E2E_WRITE_READ_CREDENTIAL_SECRET and namespace"
  [[ -n "${write_target_read_credential_source}" && -n "${write_target_read_credential_source_namespace}" ]] || \
    die "RELEASE_GATE=1 requires a target-read credential source"
  [[ -n "${write_forge_credential_source}" && -n "${write_forge_credential_source_namespace}" ]] || \
    die "RELEASE_GATE=1 requires a forge credential source"
  local -a credential_identities=(
    "${write_read_credential_source_namespace}/${write_read_credential_source}#${write_read_credential_key}"
    "${write_target_read_credential_source_namespace}/${write_target_read_credential_source}#${write_target_read_credential_key}"
    "${write_credential_source_namespace}/${write_credential_source}#${write_credential_key}"
    "${write_forge_credential_source_namespace}/${write_forge_credential_source}#${write_forge_credential_key}"
  )
  [[ "$(printf '%s\n' "${credential_identities[@]}" | sort -u | wc -l | tr -d ' ')" == "4" ]] || \
    die "release-gate source-read, target-read, target-write, and forge credential identities must be distinct"
  bool_env "${ACP_E2E_WRITE_CREATE_PR:-0}" || die "RELEASE_GATE=1 requires ACP_E2E_WRITE_CREATE_PR=1"
  is_sha "${write_source_commit}" || die "ACP_E2E_WRITE_SOURCE_REF must be a full immutable commit SHA"
  git check-ref-format "refs/heads/${write_branch}" >/dev/null || die "ACP_E2E_WRITE_BRANCH is not a valid branch"
  git check-ref-format "refs/heads/${write_pr_base}" >/dev/null || die "ACP_E2E_WRITE_PR_BASE is not a valid branch"
  [[ "${write_branch}" =~ ^[A-Za-z0-9._/-]+$ ]] || \
    die "ACP_E2E_WRITE_BRANCH must use URL-safe GitHub ref characters"
  [[ "${write_pr_base}" =~ ^[A-Za-z0-9._/-]+$ ]] || \
    die "ACP_E2E_WRITE_PR_BASE must use URL-safe GitHub ref characters"
  [[ "${write_branch}" == *"${run_id}"* ]] || die "ACP_E2E_WRITE_BRANCH must contain the run ID ${run_id}"

  write_source_slug="$(github_repo_slug "${write_source_repo}")" || \
    die "release-gate source repository must be a credential-free https://github.com/OWNER/REPO URL"
  write_publication_slug="$(github_repo_slug "${write_publication_repo}")" || \
    die "release-gate publication repository must be a credential-free https://github.com/OWNER/REPO URL"
  [[ "${write_source_slug}" != "${write_publication_slug}" ]] || \
    die "release-gate publication repository must be a distinct fork"

  gh auth status --hostname github.com >/dev/null
  local source_json publication_json resolved base_json base_sha
  source_json="$(gh api "repos/${write_source_slug}")"
  publication_json="$(gh api "repos/${write_publication_slug}")"
  jq -e --arg source "${write_source_slug}" '
    .fork == true and ((.parent.full_name // "") | ascii_downcase) == $source
  ' <<<"${publication_json}" >/dev/null || \
    die "ACP_E2E_WRITE_PUBLICATION_REPO must be an actual GitHub fork of ACP_E2E_WRITE_SOURCE_REPO"
  write_source_database_id="$(jq -r '.id | tostring' <<<"${source_json}")"
  write_source_identity="github.com/${write_source_slug}"
  write_publication_identity="github.com/${write_publication_slug}"
  resolved="$(gh api "repos/${write_source_slug}/commits/${write_source_commit}" --jq '.sha')"
  [[ "$(lower "${resolved}")" == "$(lower "${write_source_commit}")" ]] || \
    die "ACP_E2E_WRITE_SOURCE_REF did not resolve to the requested immutable commit"
  base_json="$(gh api "repos/${write_source_slug}/branches/${write_pr_base}")" || \
    die "PR base branch ${write_pr_base} does not exist in ${write_source_slug}"
  base_sha="$(jq -r '.commit.sha // ""' <<<"${base_json}")"
  [[ "$(lower "${write_source_commit}")" == "$(lower "${base_sha}")" ]] || \
    die "ACP_E2E_WRITE_SOURCE_REF must equal the current ${write_pr_base} base-branch head for an isolated one-file PR"
  github_content_lookup "${write_source_slug}" "${write_expected_file}" "${write_source_commit}" || \
    die "could not inspect the release-gate canary path at the source commit"
  [[ "${github_content_lookup_state}" == "absent" ]] || \
    die "release-gate canary path ${write_expected_file} already exists at the source commit"
  remote_cleanup_branch="${write_branch}"
  remote_cleanup_publication_repo="${write_publication_repo}"
  remote_cleanup_publication_slug="${write_publication_slug}"
  remote_cleanup_source_slug="${write_source_slug}"
  remote_cleanup_pr_base="${write_pr_base}"
  github_ref_lookup "${write_publication_slug}" "${write_branch}" || die "could not inspect release-gate publication branch"
  [[ "${github_ref_lookup_state}" == "absent" ]] || \
    die "release-gate branch ${write_branch} already exists in ${write_publication_slug}"
  no_cleanup_pull_request_exists || \
    die "a historical pull request already exists for release-gate branch ${write_branch}"

  configure_git_observer_auth
  remote_cleanup_required=1
}

apply_write_task() {
  local task="$1"
  local agent="$2"
  local provider
  provider="$(k -n "${namespace}" get agent "${agent}" -o jsonpath='{.spec.runtime.type}')"
  jq -n \
    --arg task "${task}" \
    --arg run "${run_id}" \
    --arg agent "${agent}" \
    --arg provider "${provider}" \
    --arg prompt "${write_prompt}" \
    --arg source "${write_source_repo}" \
    --arg publication "${write_publication_repo}" \
    --arg sourceID "${write_source_identity}" \
    --arg publicationID "${write_publication_identity}" \
    --arg ref "${write_source_commit}" \
    --arg readCredential "${write_read_credential_target}" \
    --arg readCredentialKey "${write_read_credential_key}" \
    --arg targetReadCredential "${write_target_read_credential_target}" \
    --arg targetReadCredentialKey "${write_target_read_credential_key}" \
    --arg writeCredential "${write_credential_target}" \
    --arg writeCredentialKey "${write_credential_key}" \
    --arg forgeCredential "${write_forge_credential_target}" \
    --arg forgeCredentialKey "${write_forge_credential_key}" \
    --arg branch "${write_branch}" \
    --arg base "${write_pr_base}" \
    --argjson maxTurns "${task_max_turns}" '{
      apiVersion:"core.orka.ai/v1alpha1",
      kind:"Task",
      metadata:{name:$task,labels:{"orka.ai/acp-e2e-run":$run}},
      spec:{
        type:"agent",
        agentRef:{name:$agent},
        prompt:$prompt,
        workspace:{
          intent:"write",
          gitRepo:$source,
          sourceRepository:{provider:"github",id:$sourceID},
          ref:$ref,
          readCredentialRef:{name:$readCredential,key:$readCredentialKey},
          publicationGitRepo:$publication,
          publicationRepository:{provider:"github",id:$publicationID},
          publicationReadCredentialRef:{name:$targetReadCredential,key:$targetReadCredentialKey},
          publicationCredentialRef:{name:$writeCredential,key:$writeCredentialKey},
          forgeCredentialRef:{name:$forgeCredential,key:$forgeCredentialKey},
          pushBranch:$branch,
          prBaseBranch:$base,
          createPR:true
        },
        agentRuntime:({maxTurns:$maxTurns} + (if $provider == "codex" then {} else {
          allowBash:true,
          allowedTools:["Read","Glob","Grep","Bash","Write","Edit"]
        } end)),
        timeout:"15m"
      }
    }' | k -n "${namespace}" apply -f - >/dev/null
}

verify_remote_publication() {
  local task_payload="$1"
  local delivery outcome expected remote starting tree artifact publication_id remote_before
  local commit_json expected_compare_json compare_json ancestry_json pr_number pr_json pr_files_json base_now_json pr_id pr_url pr_state pr_base pr_head pr_sha
  local expected_file expected_observed_file observed_file
  delivery="$(jq -c '.status.delivery' <<<"${task_payload}")"
  outcome="$(jq -r '.outcome // ""' <<<"${delivery}")"
  expected="$(jq -r '.expectedCommitSHA // ""' <<<"${delivery}")"
  remote="$(jq -r 'if ((.supersedingRemoteSHA // "") | length) > 0 then .supersedingRemoteSHA else (.verifiedRemoteSHA // "") end' <<<"${delivery}")"
  starting="$(jq -r '.startingSHA // ""' <<<"${delivery}")"
  tree="$(jq -r '.treeSHA // ""' <<<"${delivery}")"
  artifact="$(jq -r '.artifactDigest // ""' <<<"${delivery}")"
  publication_id="$(jq -r '.publicationID // ""' <<<"${delivery}")"
  remote_before="$(jq -r 'if has("remoteBeforeSHA") then .remoteBeforeSHA else "__missing__" end' <<<"${delivery}")"

  case "${outcome}" in
    VerifiedExact|DeliveredSuperseded) ;;
    *) die "publication delivery outcome ${outcome:-<empty>} is not release-gate acceptable" ;;
  esac
  [[ -n "${publication_id}" ]] || die "publication receipt is missing publicationID"
  is_sha "${starting}" || die "publication receipt has invalid startingSHA"
  is_sha "${tree}" || die "publication receipt has invalid treeSHA"
  is_sha "${expected}" || die "publication receipt has invalid expectedCommitSHA"
  is_sha "${remote}" || die "publication receipt has invalid verified remote SHA"
  [[ "${artifact}" =~ ^sha256:[a-f0-9]{64}$ ]] || die "publication receipt has invalid artifactDigest"
  [[ "$(lower "${starting}")" == "$(lower "${write_source_commit}")" ]] || \
    die "publication startingSHA does not match the independently resolved source commit"
  [[ "${remote_before}" == "" ]] || \
    die "publication remoteBeforeSHA must record explicit branch absence for a unique release-gate branch"
  jq -e \
    --arg branch "${write_branch}" \
    --arg source "${write_source_identity}" \
    --arg publication "${write_publication_identity}" '
      .branch == $branch
      and .sourceRepository.provider == "github"
      and .sourceRepository.id == $source
      and .publicationRepository.provider == "github"
      and .publicationRepository.id == $publication
    ' <<<"${delivery}" >/dev/null || die "publication repository/branch identity tuple is incomplete or incorrect"

  github_ref_lookup "${write_publication_slug}" "${write_branch}" || die "independent observer could not read publication branch"
  [[ "${github_ref_lookup_state}" == "present" && "${github_ref_lookup_sha}" == "${remote}" ]] || \
    die "independent remote branch head does not match the Task delivery receipt"

  commit_json="$(gh api "repos/${write_publication_slug}/git/commits/${expected}")"
  [[ "$(jq -r '.tree.sha // ""' <<<"${commit_json}")" == "${tree}" ]] || \
    die "Task treeSHA does not match the independently observed expected commit tree"
  jq -e --arg parent "${starting}" '.parents | length == 1 and .[0].sha == $parent' \
    <<<"${commit_json}" >/dev/null || die "expected publication commit is not based exactly on startingSHA"
  expected_compare_json="$(gh api "repos/${write_publication_slug}/compare/${starting}...${expected}")"
  jq -e --arg file "${write_expected_file}" '
    .status == "ahead"
    and (.files | length == 1)
    and .files[0].filename == $file
    and .files[0].status == "added"
  ' <<<"${expected_compare_json}" >/dev/null || \
    die "expected publication commit is not exactly the one-file canary change"
  expected_file="${temp_root}/expected-publication-content"
  expected_observed_file="${temp_root}/expected-commit-publication-content"
  printf '%s\n' "${write_expected_content}" >"${expected_file}"
  gh_raw_file "${write_publication_slug}" "${write_expected_file}" "${expected}" >"${expected_observed_file}" || \
    die "could not download the canary from expectedCommitSHA"
  cmp -s "${expected_file}" "${expected_observed_file}" || \
    die "expectedCommitSHA canary bytes do not exactly match the requested content"

  ancestry_json="$(gh api "repos/${write_publication_slug}/compare/${expected}...${remote}")"
  jq -e --arg expected "${expected}" '
    (.status == "identical" or .status == "ahead") and .merge_base_commit.sha == $expected
  ' <<<"${ancestry_json}" >/dev/null || die "verified remote SHA is not the expected commit or a proven descendant"
  if [[ "${outcome}" == "VerifiedExact" && "${remote}" != "${expected}" ]]; then
    die "VerifiedExact delivery does not equal expectedCommitSHA"
  fi
  if [[ "${outcome}" == "DeliveredSuperseded" && "${remote}" == "${expected}" ]]; then
    die "DeliveredSuperseded receipt did not identify a distinct descendant"
  fi

  compare_json="$(gh api "repos/${write_publication_slug}/compare/${starting}...${remote}")"
  jq -e --arg file "${write_expected_file}" '
    (.status == "ahead")
    and (.files | length == 1)
    and .files[0].filename == $file
    and .files[0].status == "added"
  ' <<<"${compare_json}" >/dev/null || \
    die "independent remote diff is not exactly one newly added release-gate canary"
  observed_file="${temp_root}/observed-publication-content"
  gh_raw_file "${write_publication_slug}" "${write_expected_file}" "${remote}" >"${observed_file}" || \
    die "could not download the independently observed release-gate canary"
  cmp -s "${expected_file}" "${observed_file}" || \
    die "independent remote bytes do not exactly match the one-line release-gate canary"

  pr_number="$(jq -r '.prReceipt.number // 0' <<<"${delivery}")"
  if ! is_uint "${pr_number}" || (( pr_number <= 0 )); then
    die "PR receipt is missing a numeric pull request number"
  fi
  pr_json="$(gh api "repos/${write_source_slug}/pulls/${pr_number}")"
  base_now_json="$(gh api "repos/${write_source_slug}/branches/${write_pr_base}")"
  [[ "$(jq -r '.commit.sha // ""' <<<"${base_now_json}")" == "${starting}" ]] || \
    die "PR base branch moved after release-gate source resolution"
  pr_files_json="$(gh api --paginate --slurp "repos/${write_source_slug}/pulls/${pr_number}/files?per_page=100")"
  jq -e --arg file "${write_expected_file}" '
    [.[][]] as $files
    | ($files | length) == 1
      and $files[0].filename == $file
      and $files[0].status == "added"
  ' <<<"${pr_files_json}" >/dev/null || \
    die "independent PR diff is not exactly the newly added release-gate canary"
  pr_id="$(jq -r '.prReceipt.id // ""' <<<"${delivery}")"
  pr_url="$(jq -r '.prReceipt.url // ""' <<<"${delivery}")"
  pr_state="$(jq -r '.prReceipt.state // ""' <<<"${delivery}")"
  pr_base="$(jq -r '.prReceipt.baseBranch // ""' <<<"${delivery}")"
  pr_head="$(jq -r '.prReceipt.headBranch // ""' <<<"${delivery}")"
  pr_sha="$(jq -r '.prReceipt.headSHA // ""' <<<"${delivery}")"
  [[ "${pr_id}" == "github:${write_source_database_id}:${pr_number}" ]] || \
    die "PR receipt ID does not match the canonical GitHub repository/PR tuple"
  [[ "${pr_url}" == "https://github.com/${write_source_slug}/pull/${pr_number}" ]] || \
    die "PR receipt URL is not canonical"
  [[ "${pr_state}" == "Open" && "${pr_base}" == "${write_pr_base}" && \
     "${pr_head}" == "${write_branch}" && "${pr_sha}" == "${remote}" ]] || \
    die "PR receipt base/head/state/SHA tuple is incomplete or incorrect"
  jq -e \
    --arg source "${write_source_slug}" \
    --arg publication "${write_publication_slug}" \
    --arg branch "${write_branch}" \
    --arg base "${write_pr_base}" \
    --arg sha "${remote}" \
    --arg starting "${starting}" \
    --argjson number "${pr_number}" '
      .number == $number
      and .state == "open"
      and ((.base.repo.full_name // "") | ascii_downcase) == $source
      and .base.ref == $base
      and .base.sha == $starting
      and ((.head.repo.full_name // "") | ascii_downcase) == $publication
      and .head.ref == $branch
      and .head.sha == $sha
    ' <<<"${pr_json}" >/dev/null || \
    die "independent GitHub PR state does not match the publication receipt tuple"

  remote_cleanup_head="${remote}"
  remote_cleanup_pr_number="${pr_number}"
}

run_write_release_gate() {
  local agent="$1"
  local task pool snapshot payload expected_credential_rv expected_read_rv expected_target_read_rv expected_forge_rv
  local current_credential_rv current_read_rv current_target_read_rv current_forge_rv
  task="$(sanitize_name "acp-write-${run_id}")"
  write_credential_target="$(sanitize_name "acp-publish-credential-${run_id}")"
  copy_secret_key_to_test_namespace "${write_credential_source_namespace}" \
    "${write_credential_source}" "${write_credential_key}" "${write_credential_target}"
  write_read_credential_target="$(sanitize_name "acp-read-credential-${run_id}")"
  copy_secret_key_to_test_namespace "${write_read_credential_source_namespace}" \
    "${write_read_credential_source}" "${write_read_credential_key}" "${write_read_credential_target}"
  write_target_read_credential_target="$(sanitize_name "acp-target-read-credential-${run_id}")"
  copy_secret_key_to_test_namespace "${write_target_read_credential_source_namespace}" \
    "${write_target_read_credential_source}" "${write_target_read_credential_key}" "${write_target_read_credential_target}"
  write_forge_credential_target="$(sanitize_name "acp-forge-credential-${run_id}")"
  copy_secret_key_to_test_namespace "${write_forge_credential_source_namespace}" \
    "${write_forge_credential_source}" "${write_forge_credential_key}" "${write_forge_credential_target}"

  assert_publisher_cannot_access_secret "${write_credential_source_namespace}" "${write_credential_source}"
  assert_publisher_cannot_access_secret "${namespace}" "${write_credential_target}"
  assert_publisher_cannot_access_secret "${write_read_credential_source_namespace}" "${write_read_credential_source}"
  assert_publisher_cannot_access_secret "${namespace}" "${write_read_credential_target}"
  assert_publisher_cannot_access_secret "${write_target_read_credential_source_namespace}" "${write_target_read_credential_source}"
  assert_publisher_cannot_access_secret "${namespace}" "${write_target_read_credential_target}"
  assert_publisher_cannot_access_secret "${write_forge_credential_source_namespace}" "${write_forge_credential_source}"
  assert_publisher_cannot_access_secret "${namespace}" "${write_forge_credential_target}"

  expected_credential_rv="$(k -n "${namespace}" get secret "${write_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  [[ -n "${expected_credential_rv}" ]] || die "publication credential Secret has no resourceVersion"
  expected_read_rv="$(k -n "${namespace}" get secret "${write_read_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  expected_target_read_rv="$(k -n "${namespace}" get secret "${write_target_read_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  expected_forge_rv="$(k -n "${namespace}" get secret "${write_forge_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  [[ -n "${expected_read_rv}" && -n "${expected_target_read_rv}" && -n "${expected_forge_rv}" ]] || \
    die "one or more role-separated credential Secrets have no resourceVersion"

  local base_now_sha
  base_now_sha="$(gh api "repos/${write_source_slug}/branches/${write_pr_base}" --jq '.commit.sha')"
  [[ "${base_now_sha}" == "${write_source_commit}" ]] || \
    die "PR base branch moved before write Task submission"

  log "Running mandatory Codex clean-room publication and PR reconciliation gate"
  write_task_name="${task}"
  write_task_started=1
  apply_write_task "${task}" "${agent}"
  wait_until "Task/${task} RuntimePool assignment" "${state_wait_seconds}" wait_task_pool_name_value "${task}"
  pool="$(wait_task_pool_name_value "${task}")"
  snapshot="${temp_root}/${task}-write-pool.json"
  capture_pool_snapshot "${pool}" codex "${codex_model}" write "${snapshot}"
  wait_task_terminal "${task}"
  payload="$(task_json "${task}")"
  if ! jq -e '
    .status.phase == "Succeeded"
    and .status.execution.state == "Succeeded"
    and .status.execution.outcome == "Succeeded"
    and (.status.delivery.outcome == "VerifiedExact" or .status.delivery.outcome == "DeliveredSuperseded")
    and ((.status.jobName // "") == "")
  ' <<<"${payload}" >/dev/null; then
    safe_task_summary "${task}"
    die "write Task/${task} did not reach successful execution and verified delivery"
  fi
  assert_task_fence "${task}" "${snapshot}"

  current_credential_rv="$(k -n "${namespace}" get secret "${write_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  [[ "${current_credential_rv}" == "${expected_credential_rv}" ]] || \
    die "publication credential Secret rotated during the release gate"
  [[ "$(jq -r '.status.execution.publicationCredentialResourceVersion // ""' <<<"${payload}")" == "${expected_credential_rv}" ]] || \
    die "write Task did not freeze the pre-submission publication credential Secret resourceVersion"
  current_read_rv="$(k -n "${namespace}" get secret "${write_read_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  current_target_read_rv="$(k -n "${namespace}" get secret "${write_target_read_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  current_forge_rv="$(k -n "${namespace}" get secret "${write_forge_credential_target}" -o jsonpath='{.metadata.resourceVersion}')"
  [[ "${current_read_rv}" == "${expected_read_rv}" && "${current_target_read_rv}" == "${expected_target_read_rv}" && "${current_forge_rv}" == "${expected_forge_rv}" ]] || \
    die "one or more role-separated credential Secrets rotated during the release gate"
  [[ "$(jq -r '.status.execution.readCredentialResourceVersion // ""' <<<"${payload}")" == "${expected_read_rv}" ]] || \
    die "write Task did not freeze the source-read credential resourceVersion"
  [[ "$(jq -r '.status.execution.publicationReadCredentialResourceVersion // ""' <<<"${payload}")" == "${expected_target_read_rv}" ]] || \
    die "write Task did not freeze the target-read credential resourceVersion"
  [[ "$(jq -r '.status.execution.forgeCredentialResourceVersion // ""' <<<"${payload}")" == "${expected_forge_rv}" ]] || \
    die "write Task did not freeze the forge credential resourceVersion"

  verify_remote_publication "${payload}"
  mark_task_validated "${task}"
}

write_task_cleanup_settled() {
  [[ "${write_task_started}" -eq 1 && -n "${write_task_name}" ]] || return 0
  probe_task "${write_task_name}" || return 1
  [[ "${task_probe_state}" == "present" ]] || return 1
  jq -e '
    (.status.execution.state // "") as $execution
    | (.status.delivery.state // "") as $delivery
    | ($execution == "Succeeded" or $execution == "Failed" or $execution == "Cancelled" or $execution == "OutcomeUnknown")
      and (
        $delivery == "VerifiedExact"
        or $delivery == "DeliveredSuperseded"
        or $delivery == "NoChange"
        or $delivery == "CancelledBeforePublish"
        or $delivery == "ReadOnlyWorkspaceModified"
        or $delivery == "DeliveryConflict"
        or $delivery == "CredentialBlocked"
        or $delivery == "PublicationOutcomeUnknown"
        or (($delivery | length) == 0 and $execution != "Succeeded")
      )
  ' "${task_probe_file}" >/dev/null
}

discover_cleanup_pull_request() {
  local expected_head="$1"
  local owner response count
  owner="${remote_cleanup_publication_slug%%/*}"
  if ! response="$(gh api --method GET --paginate --slurp \
      "repos/${remote_cleanup_source_slug}/pulls" \
      -f state=all -f "head=${owner}:${remote_cleanup_branch}" \
      -f "base=${remote_cleanup_pr_base}" -f per_page=100)"; then
    return 1
  fi
  count="$(jq --arg repo "${remote_cleanup_publication_slug}" --arg branch "${remote_cleanup_branch}" --arg sha "${expected_head}" '
    [.[][] | select(((.head.repo.full_name // "") | ascii_downcase) == $repo and .head.ref == $branch and .head.sha == $sha)] | length
  ' <<<"${response}")"
  [[ "${count}" == "1" ]] || return 1
  remote_cleanup_pr_number="$(jq -r --arg repo "${remote_cleanup_publication_slug}" --arg branch "${remote_cleanup_branch}" --arg sha "${expected_head}" '
    [.[][] | select(((.head.repo.full_name // "") | ascii_downcase) == $repo and .head.ref == $branch and .head.sha == $sha)][0].number
  ' <<<"${response}")"
  is_uint "${remote_cleanup_pr_number}" && (( remote_cleanup_pr_number > 0 ))
}

no_cleanup_pull_request_exists() {
  local owner response count
  owner="${remote_cleanup_publication_slug%%/*}"
  response="$(gh api --method GET --paginate --slurp \
    "repos/${remote_cleanup_source_slug}/pulls" \
    -f state=all -f "head=${owner}:${remote_cleanup_branch}" -f per_page=100)" || return 1
  count="$(jq '[.[][]] | length' <<<"${response}")"
  [[ "${count}" == "0" ]]
}

settle_write_task_for_remote_cleanup() {
  local pool outcome head number uid
  [[ "${write_task_started}" -eq 1 ]] || return 0
  [[ ( "${namespace_created}" -eq 1 || "${namespace_shared}" -eq 1 ) && -n "${write_task_name}" ]] || return 1
  probe_namespace "${namespace}" || return 1
  [[ "${namespace_probe_state}" == "present" ]] || return 1
  probe_task "${write_task_name}" || return 1
  [[ "${task_probe_state}" == "present" ]] || return 1

  if ! jq -e '(.status.execution.state // "") as $state | ($state == "Succeeded" or $state == "Failed" or $state == "Cancelled" or $state == "OutcomeUnknown")' \
      "${task_probe_file}" >/dev/null; then
    uid="$(jq -r '.metadata.uid // ""' "${task_probe_file}")"
    [[ -n "${uid}" ]] || return 1
    add_task_observer_finalizer "${write_task_name}" "${uid}" || return 1
    write_task_observer_uid="${uid}"
    request_task_cancellation "${write_task_name}" || return 1
  fi
  wait_until "write Task/${write_task_name} cleanup settlement" 300 write_task_cleanup_settled || return 1
  probe_task "${write_task_name}" || return 1
  [[ "${task_probe_state}" == "present" ]] || return 1

  pool="$(jq -r '.status.execution.runtimePoolName // ""' "${task_probe_file}")" || return 1
  if [[ -n "${pool}" ]]; then
    require_runtimepool_mutation_scope || return 1
    probe_runtimepool "${pool}" || return 1
    [[ "${runtimepool_probe_state}" == "present" ]] || return 1
    log "Parking write RuntimePool/${pool} after terminal publication"
    k -n "${namespace}" patch runtimepool "${pool}" --type=merge \
      -p '{"spec":{"desiredReplicas":0}}' >/dev/null || return 1
    wait_until "write RuntimePool/${pool} stopped after publication finalization" 300 pool_stopped "${pool}" || return 1
  fi
  outcome="$(jq -r '.status.delivery.outcome // ""' "${task_probe_file}")" || return 1
  case "${outcome}" in
    VerifiedExact|DeliveredSuperseded)
      head="$(jq -r 'if ((.status.delivery.supersedingRemoteSHA // "") | length) > 0 then .status.delivery.supersedingRemoteSHA else (.status.delivery.verifiedRemoteSHA // "") end' "${task_probe_file}")" || return 1
      is_sha "${head}" || return 1
      remote_cleanup_head="$(lower "${head}")"
      number="$(jq -r '.status.delivery.prReceipt.number // 0' "${task_probe_file}")" || return 1
      if is_uint "${number}" && (( number > 0 )); then
        remote_cleanup_pr_number="${number}"
      else
        discover_cleanup_pull_request "${remote_cleanup_head}" || return 1
      fi
      ;;
    NoChange|CancelledBeforePublish|ReadOnlyWorkspaceModified|DeliveryConflict|CredentialBlocked|"")
      no_cleanup_pull_request_exists || return 1
      ;;
    PublicationOutcomeUnknown|*)
      return 1
      ;;
  esac
}

remove_provider_resources() {
  local provider="$1"
  shift
  local agent pool pools runtime_ns
  local owners_file="${temp_root}/provider-${provider}-owner-uids.txt"
  log "Removing ${provider} Tasks, Agents, and RuntimePools before the next provider"
  assert_all_tasks_validated
  if [[ "${namespace_shared:-0}" -eq 1 ]]; then
    # Shared watch-namespace mode: only run-labeled Tasks are removed, and
    # RuntimePools are left alone because they are profile-keyed and may be
    # serving unrelated Agents in the same namespace; the controller's idle
    # policy scales them down.
    k -n "${namespace}" get task -l "orka.ai/acp-e2e-run=${run_id}" -o json | jq -r '
      .items[] | .metadata.uid, (.status.execution.runtimeSessionUID // empty)
    ' | sort -u >"${owners_file}"
    k -n "${namespace}" delete task -l "orka.ai/acp-e2e-run=${run_id}" --wait=true --timeout=5m >/dev/null
    log "Shared watch namespace: leaving ${provider} RuntimePools to the controller idle policy"
    pools=""
  else
    k -n "${namespace}" get task -o json | jq -r '
      .items[] | .metadata.uid, (.status.execution.runtimeSessionUID // empty)
    ' | sort -u >"${owners_file}"
    k -n "${namespace}" delete task --all --wait=true --timeout=5m >/dev/null
    pools="$(k -n "${namespace}" get runtimepool -o json | jq -r --arg provider "${provider}" \
      '.items[] | select(.spec.runtime.profile.providerKind == $provider) | [.metadata.name, (.status.activeInstance.podNamespace // .spec.runtimeNamespace // "")] | @tsv')"
  fi
  if [[ -n "${pools}" ]]; then
    while IFS=$'\t' read -r pool runtime_ns; do
      [[ -n "${pool}" ]] || continue
      record_runtime_namespace "${runtime_ns}"
      k -n "${namespace}" patch runtimepool "${pool}" --type=merge \
        -p '{"spec":{"desiredReplicas":0}}' >/dev/null
    done <<<"${pools}"
    while IFS=$'\t' read -r pool _; do
      [[ -n "${pool}" ]] || continue
      wait_until "RuntimePool/${pool} stopped before ${provider} provider handoff" \
        "${state_wait_seconds}" pool_stopped "${pool}"
    done <<<"${pools}"
  fi
  for agent in "$@"; do
    [[ -n "${agent}" ]] || continue
    k -n "${namespace}" delete agent "${agent}" --ignore-not-found=true --wait=true --timeout=2m >/dev/null
  done
  if [[ -n "${pools}" ]]; then
    while IFS=$'\t' read -r pool _; do
      [[ -n "${pool}" ]] || continue
      k -n "${namespace}" delete runtimepool "${pool}" --wait=true --timeout=5m >/dev/null
    done <<<"${pools}"
  fi
  delete_test_branchclaims "${owners_file}" || die "failed to remove ${provider} BranchClaims before provider handoff"
}

preflight_release_workloads() {
  [[ "${release_gate}" -eq 1 ]] || return 0
  log "Validating digest-pinned controller, Publisher, provider proxy, and SCM proxy images"
  local controller_json publisher_json controller_primary publisher_primary
  assert_deployment_digest "${controller_deployment}" "${controller_container_override}" manager controller
  assert_deployment_digest "${publisher_deployment}" "${publisher_container_override}" publisher workspace-publisher
  assert_deployment_digest "${provider_proxy_deployment}" "${provider_proxy_container_override}" proxy provider-auth-proxy
  assert_deployment_digest "${scm_proxy_deployment}" "${scm_proxy_container_override}" proxy scm-egress-proxy
  controller_json="$(k -n "${orka_namespace}" get deployment "${controller_deployment}" -o json)"
  publisher_json="$(k -n "${orka_namespace}" get deployment "${publisher_deployment}" -o json)"
  controller_primary="$(select_deployment_container "${controller_json}" "${controller_container_override}" manager controller)"
  publisher_primary="$(select_deployment_container "${publisher_json}" "${publisher_container_override}" publisher workspace-publisher)"
  assert_deployment_persistent "${controller_json}" "${controller_deployment}" "${controller_primary}"
  assert_deployment_persistent "${publisher_json}" "${publisher_deployment}" "${publisher_primary}"
  assert_publisher_brokered_authority
}

prepare_release_gate_environment

log "Preflight for context ${context}"
k config get-contexts "${context}" -o name | grep -Fxq "${context}" || die "kubectl context ${context} was not found"
for crd in tasks.core.orka.ai agents.core.orka.ai runtimepools.core.orka.ai promptattempts.core.orka.ai \
  runtimesessioncontrols.core.orka.ai branchclaims.core.orka.ai publications.core.orka.ai \
  controllerepochs.core.orka.ai externaleffects.core.orka.ai; do
  resource_exists get crd "${crd}" || die "required CRD ${crd} is not installed"
done
resource_exists -n "${orka_namespace}" get deployment "${controller_deployment}" || \
  die "controller Deployment/${controller_deployment} was not found"
k -n "${orka_namespace}" rollout status deployment/"${controller_deployment}" --timeout=5m >/dev/null
preflight_release_workloads

read_repo_slug=""
read_repo_identity=""
read_repo_commit="$(lower "${repo_ref}")"
expected_license_line=""
if read_repo_slug="$(github_repo_slug "${repo_url}")"; then
  read_repo_identity="github.com/${read_repo_slug}"
fi
if [[ "${release_gate}" -eq 1 ]]; then
  [[ -n "${read_repo_slug}" ]] || die "RELEASE_GATE=1 requires ACP_E2E_REPO to be a github.com HTTPS repository"
  read_repo_commit="$(gh api "repos/${read_repo_slug}/commits/${repo_ref}" --jq '.sha')"
  [[ "$(lower "${read_repo_commit}")" == "$(lower "${repo_ref}")" ]] || \
    die "ACP_E2E_REF did not resolve to the requested immutable commit"
  expected_license_line="$(gh_raw_file "${read_repo_slug}" LICENSE "${read_repo_commit}" | awk 'NF { print; exit }')"
  [[ -n "${expected_license_line}" ]] || die "could not independently read the first non-empty LICENSE line"
fi

if resource_exists get namespace "${namespace}"; then
  if k get namespace "${namespace}" -o json | jq -e '
      .metadata.labels["orka.ai/controller-mode"] == "harness-v2"
    ' >/dev/null; then
    # Shared watch-namespace mode: the isolated harness-v2 controller only
    # serves Tasks in its watch namespace, so the validator adopts that
    # namespace and cleans up its run-labeled resources without ever owning
    # the namespace lifecycle.
    log "Adopting existing harness-v2 namespace ${namespace} (shared watch-namespace mode)"
    namespace_shared=1
  else
    die "test namespace ${namespace} already exists; choose another --namespace"
  fi
else
  namespace_create_attempted=1
  jq -n --arg name "${namespace}" --arg run "${run_id}" '{
    apiVersion:"v1",
    kind:"Namespace",
    metadata:{
      name:$name,
      labels:{
        "orka.ai/acp-e2e-run":$run,
        "orka.ai/controller-mode":"harness-v2",
        "app.kubernetes.io/managed-by":"live-acp-runtime-e2e",
        "pod-security.kubernetes.io/enforce":"restricted"
      }
    }
  }' | k create -f - >/dev/null
  namespace_created=1
fi
namespace_uid="$(k get namespace "${namespace}" -o jsonpath='{.metadata.uid}')"
[[ -n "${namespace_uid}" ]] || die "test namespace UID is unavailable"
if [[ "${namespace_shared}" -eq 1 && "${shared_pool_mutation_allowed}" -eq 1 ]]; then
  warn "shared namespace RuntimePool mutation was explicitly enabled; this is safe only on a dedicated cluster"
fi
if [[ "${release_gate}" -eq 1 ]] && ! runtimepool_mutations_allowed; then
  die "RELEASE_GATE=1 requires an isolated namespace or ACP_E2E_ALLOW_SHARED_POOL_MUTATION=1 on a dedicated cluster"
fi

if [[ "${release_gate}" -eq 1 ]]; then
  create_api_identity
  start_api_forward
fi

session_nonce="nonce-${run_id}-${RANDOM}-${RANDOM}"
restart_nonce="restart-${run_id}-${RANDOM}-${RANDOM}"

codex_agent="$(sanitize_name "acp-codex-${run_id}")"
codex_task="$(sanitize_name "acp-codex-read-${run_id}")"
codex_session="$(sanitize_name "acp-codex-session-${run_id}")"
if [[ "${release_gate}" -eq 1 ]]; then
  log "Running Codex read, result, continuation, and Task-fork validation"
else
  log "Running Codex read and continuation smoke validation"
fi
run_read_smoke codex "${codex_model}" "${codex_agent}" "${codex_task}" "${codex_session}" \
  "${session_nonce}" "${expected_license_line}"
codex_pool="${read_smoke_pool}"

assert_unsafe_workspace_rejected "${codex_agent}"
run_concurrency_check "${codex_agent}" codex "${codex_model}" "${codex_pool}"
if runtimepool_mutations_allowed; then
  park_runtimepool "${codex_pool}"
fi
codex_tool_agent="$(sanitize_name "acp-codex-tools-${run_id}")"
apply_agent codex "${codex_model}" "${codex_tool_agent}" 12 true
run_timeout_check codex "${codex_model}" "${codex_tool_agent}"
run_explicit_cancel_check codex "${codex_model}" "${codex_tool_agent}"
if runtimepool_mutations_allowed; then
  run_controller_restart_check codex "${codex_model}" "${codex_tool_agent}" "${restart_nonce}"
  park_provider_runtimepools_except codex "${codex_pool}"
  resume_runtimepool "${codex_pool}"
  run_pool_replacement_check codex "${codex_model}" "${codex_agent}" "${codex_pool}" "${codex_session}" "${session_nonce}"
else
  shared_mutation_checks_skipped=1
  log "Shared watch namespace: skipping controller restart and RuntimePool parking, resume, and replacement checks"
fi

if [[ "${release_gate}" -eq 1 ]]; then
  run_scale_to_zero_recovery_check codex "${codex_model}" "${codex_agent}" "${codex_pool}" "${codex_session}" "${session_nonce}"
  park_runtimepool "${codex_pool}"
  run_write_release_gate "${codex_agent}"
  assert_all_tasks_validated
  if ! cleanup_remote_effects; then
    die "release-gate remote cleanup did not complete safely"
  fi
  remote_cleanup_required=0
fi

remove_provider_resources codex "${codex_agent}" "${codex_tool_agent}"
[[ "${namespace_shared:-0}" -eq 1 ]] || wait_until "Codex runtime children removal" 300 runtime_children_absent

opencode_agent="$(sanitize_name "acp-opencode-${run_id}")"
opencode_task="$(sanitize_name "acp-opencode-read-${run_id}")"
opencode_session="$(sanitize_name "acp-opencode-session-${run_id}")"
opencode_nonce="opencode-${run_id}-${RANDOM}-${RANDOM}"
log "Running OpenCode native ACP read, continuation, and read-policy validation"
run_read_smoke opencode "${opencode_model}" "${opencode_agent}" "${opencode_task}" "${opencode_session}" \
  "${opencode_nonce}" "${expected_license_line}"
opencode_policy_agent="$(sanitize_name "acp-opencode-policy-agent-${run_id}")"
apply_agent opencode "${opencode_model}" "${opencode_policy_agent}" 12 true
run_opencode_read_policy_check "${opencode_policy_agent}" "${opencode_model}"
assert_all_tasks_validated
remove_provider_resources opencode "${opencode_agent}" "${opencode_policy_agent}"
[[ "${namespace_shared:-0}" -eq 1 ]] || wait_until "OpenCode runtime children removal" 300 runtime_children_absent

claude_agent="$(sanitize_name "acp-claude-${run_id}")"
claude_task="$(sanitize_name "acp-claude-read-${run_id}")"
claude_session="$(sanitize_name "acp-claude-session-${run_id}")"
claude_nonce="claude-${run_id}-${RANDOM}-${RANDOM}"
log "Running Claude only after all Codex gates and cleanup completed"
run_read_smoke claude "${claude_model}" "${claude_agent}" "${claude_task}" "${claude_session}" \
  "${claude_nonce}" "${expected_license_line}"
assert_all_tasks_validated

remove_provider_resources claude "${claude_agent}"
[[ "${namespace_shared:-0}" -eq 1 ]] || wait_until "Claude runtime children removal" 300 runtime_children_absent

copilot_agent="$(sanitize_name "acp-copilot-${run_id}")"
copilot_task="$(sanitize_name "acp-copilot-read-${run_id}")"
copilot_session="$(sanitize_name "acp-copilot-session-${run_id}")"
copilot_nonce="copilot-${run_id}-${RANDOM}-${RANDOM}"
log "Running GitHub Copilot only after all Claude gates and cleanup completed"
run_read_smoke copilot "${copilot_model}" "${copilot_agent}" "${copilot_task}" "${copilot_session}" \
  "${copilot_nonce}" "${expected_license_line}"
assert_all_tasks_validated

if [[ "${release_gate}" -eq 1 ]]; then
  if [[ "${keep_resources}" != "1" ]]; then
    stop_api_forward || die "failed to stop controller API port-forward before final cleanup"
    delete_test_namespace_now || die "release-gate Kubernetes cleanup did not complete safely"
  else
    warn "ACP_E2E_KEEP_RESOURCES=1; release-gate Kubernetes resources are intentionally preserved"
  fi
  log "ACP v2 RELEASE GATE PASSED on context ${context}"
else
  if [[ "${shared_mutation_checks_skipped}" -eq 1 ]]; then
    log "ACP v2 shared-namespace smoke validation passed on context ${context}; controller restart and RuntimePool lifecycle/replacement checks were skipped. Use an isolated namespace for complete smoke acceptance."
  else
    log "ACP v2 smoke validation passed on context ${context}; release-only publication, remote verification, Task result/fork, and scale-to-zero gates were skipped. Set RELEASE_GATE=1 for release acceptance."
  fi
fi
