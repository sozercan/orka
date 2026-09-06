#!/usr/bin/env bash
set -Eeuo pipefail

# scripts/tests suites rely on 'set -e' stopping on failed (( )) arithmetic,
# which macOS's stock bash 3.2 does not honor; failures would be silently
# masked there. Require a modern bash (for example: brew install bash).
if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "error: this test suite requires bash >= 4; found ${BASH_VERSION}" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
security_script="${root}/scripts/security-scan-e2e.sh"
security_workflow="${root}/.github/workflows/security-scan-e2e.yml"

for command in cmp jq; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "required command not found: ${command}" >&2
    exit 1
  }
done

test_root="$(mktemp -d "${TMPDIR:-/tmp}/security-scan-e2e-test.XXXXXX")"
cleanup() {
  rm -rf -- "${test_root}"
}
trap cleanup EXIT

export RUNNER_TEMP="${test_root}"
export ORKA_SECURITY_SCAN_NAME="security-goof"
export ORKA_SECURITY_BAD_SCAN_NAME="security-goof-malformed-result"
export ORKA_SECURITY_SCAN_AGENT="security-scan-e2e-agent"
export ORKA_SECURITY_SCAN_TARGET_REPO="https://github.com/sozercan/vekil"
export ORKA_SECURITY_SCAN_TARGET_BRANCH="main"
export ORKA_SECURITY_SCAN_TARGET_REF="add14ba59e98240d9e00a235dd7d42cd61ae9912"
# shellcheck source=scripts/security-scan-e2e.sh
source "${security_script}"

[[ "${target_owner}" == "sozercan" ]]
[[ "${target_repository}" == "vekil" ]]
[[ "${authority_tool_name}" == "authority-probe" ]]

manifest_capture="${work_dir}/manifest.yaml"
kubectl() {
  [[ "$*" == "apply -f -" ]] || return 1
  cat >"${manifest_capture}"
}

apply_agent
grep -F 'defaultAllowBash: true' "${manifest_capture}" >/dev/null
if grep -F 'defaultAllowBash: false' "${manifest_capture}" >/dev/null; then
  echo "SecurityScan fixture requested an unenforceable Codex tool restriction" >&2
  exit 1
fi

apply_repository_scan "${scan_name}"
grep -F 'repoURL: https://github.com/sozercan/vekil' "${manifest_capture}" >/dev/null
grep -F 'owner: sozercan' "${manifest_capture}" >/dev/null
grep -F 'repository: vekil' "${manifest_capture}" >/dev/null

apply_authority_resources
grep -F 'kind: OutboundAccessPolicy' "${manifest_capture}" >/dev/null
grep -F "name: ${authority_policy_name}" "${manifest_capture}" >/dev/null
grep -F 'gateway:' "${manifest_capture}" >/dev/null
grep -F "name: ${authority_observer_name}" "${manifest_capture}" >/dev/null
grep -F 'port: 8080' "${manifest_capture}" >/dev/null
grep -F 'kind: Tool' "${manifest_capture}" >/dev/null
grep -F 'name: authority-probe' "${manifest_capture}" >/dev/null
grep -F 'brokeredToolClass: read' "${manifest_capture}" >/dev/null
grep -F 'url: https://example.com/tool' "${manifest_capture}" >/dev/null
grep -F 'outboundAccessPolicyRef:' "${manifest_capture}" >/dev/null
grep -F 'defaultAllowedTools:' "${manifest_capture}" >/dev/null
grep -F -- '      - Glob' "${manifest_capture}" >/dev/null
grep -F -- '      - Grep' "${manifest_capture}" >/dev/null
grep -F -- '      - Read' "${manifest_capture}" >/dev/null
grep -F -- "      - ${authority_tool_name}" "${manifest_capture}" >/dev/null
grep -F 'defaultAllowBash: false' "${manifest_capture}" >/dev/null

create_authority_task "${authority_incoming_task}"
grep -F 'kind: Task' "${manifest_capture}" >/dev/null
grep -F "name: ${authority_incoming_task}" "${manifest_capture}" >/dev/null
grep -F 'prompt: orka-authority-probe' "${manifest_capture}" >/dev/null
grep -F -- '      - Glob' "${manifest_capture}" >/dev/null
grep -F -- '      - Grep' "${manifest_capture}" >/dev/null
grep -F -- '      - Read' "${manifest_capture}" >/dev/null
grep -F -- "      - ${authority_tool_name}" "${manifest_capture}" >/dev/null
if grep -F '  transaction:' "${manifest_capture}" >/dev/null; then
  echo "ACP v2 authority fixture declared unsupported transaction delegation" >&2
  exit 1
fi

api_role_capture="${work_dir}/api-role.json"
kubectl() {
  if [[ "$*" == "apply -f -" ]]; then
    local manifest
    manifest="$(cat)"
    if jq -e '.kind == "Role"' <<<"${manifest}" >/dev/null; then
      printf '%s\n' "${manifest}" >"${api_role_capture}"
    fi
    return 0
  fi
  if [[ "$*" == "-n ${test_namespace} create token ${api_identity_name} --duration=2h" ]]; then
    printf '%s' 'fixture-only-token'
    return 0
  fi
  return 1
}
create_api_identity
for resource in repositoryscans/scans repositoryscans/slices repositoryscans/findings repositoryscans/droppedfindings; do
  jq -e --arg resource "${resource}" --arg ns "${test_namespace}" --arg scan "${scan_name}" --arg badScan "${bad_scan_name}" '
    .metadata.namespace == $ns and any(.rules[];
      .apiGroups == ["core.orka.ai"] and (.resources | index($resource)) != null and
      .resourceNames == [$scan,$badScan] and .verbs == ["list"])
  ' "${api_role_capture}" >/dev/null
done
jq -e --arg scan "${scan_name}" --arg badScan "${bad_scan_name}" '
  any(.rules[]; .apiGroups == ["core.orka.ai"] and .resources == ["repositoryscans/threatmodel"] and
    .resourceNames == [$scan,$badScan] and .verbs == ["get"]) and
  all(.rules[].verbs[]; . == "get" or . == "list" or . == "watch")
' "${api_role_capture}" >/dev/null

wait_calls="${work_dir}/authority-wait-calls"
kubectl() {
  printf '%s\n' "$*" >>"${wait_calls}"
}
wait_authority_resources
grep -F "condition=Accepted=true outboundaccesspolicy/${authority_policy_name}" "${wait_calls}" >/dev/null
grep -F "condition=ResolvedRefs=true outboundaccesspolicy/${authority_policy_name}" "${wait_calls}" >/dev/null
grep -F "jsonpath={.status.available}=true tool/${authority_tool_name}" "${wait_calls}" >/dev/null

authority_observer_stats() {
  jq -n '{ttsCalls:0,toolCalls:1,subjectTokenDigest:"",transactionTokenDigest:""}'
}
assert_transactionless_authority_stats incoming

authority_observer_stats() {
  jq -n '{ttsCalls:1,toolCalls:1,subjectTokenDigest:"sha256:unexpected",transactionTokenDigest:""}'
}
if (assert_transactionless_authority_stats incoming >/dev/null 2>&1); then
  echo "transactionless authority gate accepted a TTS call" >&2
  exit 1
fi

repository_phase="Error"
kubectl() {
  if [[ "$*" == *"get repositoryscan ${scan_name} -o jsonpath={.status.phase}"* ]]; then
    printf '%s' "${repository_phase}"
    return 0
  fi
  if [[ "$*" == *"get repositoryscan ${scan_name} -o json"* ]]; then
    jq -n '{status:{conditions:[{type:"Ready",message:"runtime pool degraded"}]}}'
    return 0
  fi
  return 1
}
set +e
terminal_wait_error="$(wait_repo_phase "${scan_name}" "Ready" 2>&1)"
terminal_wait_status=$?
set -e
[[ "${terminal_wait_status}" -eq 1 ]]
grep -Fq 'entered terminal phase Error while waiting for Ready: runtime pool degraded' <<<"${terminal_wait_error}"

repository_phase="Ready"
wait_repo_phase "${scan_name}" "Ready"

positive="${work_dir}/positive"
malformed="${work_dir}/malformed"
scan_id="scan_fixture"
policy_digest="sha256:$(printf 'a%.0s' {1..64})"
context_digest="sha256:$(printf 'c%.0s' {1..64})"
idempotency_key="scanidem:$(printf 'b%.0s' {1..64})"

jq -n \
  --arg ref "${target_ref}" \
  --arg scanID "${scan_id}" '{
  status: {
    phase: "Ready",
    lastScanID: $scanID,
    lastScanTaskName: "threat-task",
    lastScanAt: "2026-08-09T00:00:00Z",
    lastSuccessfulScanAt: "2026-08-09T00:00:00Z",
    lastProcessedCommit: $ref,
    lastObservedHeadSHA: $ref,
    threatModelVersion: 1,
    findingCounts: {total: 1, high: 1},
    conditions: [{type: "Ready", status: "True", reason: "ScanSucceeded"}]
  }
}' >"${positive}-scan.json"

jq -n \
  --arg agent "${agent_name}" \
  --arg branch "${target_branch}" \
  --arg ref "${target_ref}" \
  --arg repo "${target_repo}" \
  --arg scanID "${scan_id}" \
  --arg scanName "${scan_name}" '
  def labels($stage; $slice): {
    "orka.ai/security-target": $scanName,
    "orka.ai/security-scan-id": $scanID,
    "orka.ai/security-scan-mode": "initial",
    "orka.ai/security-stage": $stage
  } + (if $slice == "" then {} else {"orka.ai/security-slice-id": $slice} end);
  def owner: [{kind: "RepositoryScan", name: $scanName, controller: true}];
  def agentSpec: {
    type: "agent", agentRef: {name: $agent},
    workspace: {intent: "read", gitRepo: $repo, branch: $branch, ref: $ref}
  };
  def agentStatus: {
    phase: "Succeeded", resultRef: {available: true},
    agentExecutionBinding: {contractVersion: "orka.harness.v2", backend: "runtime-pool"},
    execution: {state: "Succeeded", outcome: "Succeeded"},
    delivery: {state: "ReadValidated", outcome: "ReadValidated"}
  };
  {items: [
    {metadata: {name: "threat-task", labels: labels("threat-model"; ""), ownerReferences: owner}, spec: agentSpec, status: agentStatus},
    {metadata: {name: "mapper-task", labels: labels("mapper"; ""), ownerReferences: owner},
     spec: {type: "container", command: ["--security-mapper"], env: [{name: "ORKA_SECURITY_REPOSITORY_SCAN", value: $scanName}]},
     status: {phase: "Succeeded"}},
    {metadata: {name: "review-task", labels: labels("review"; "slice_api"), ownerReferences: owner}, spec: agentSpec, status: agentStatus}
  ]}
' >"${positive}-tasks.json"

jq -n \
  --arg idempotency "${idempotency_key}" \
  --arg policy "${policy_digest}" \
  --arg scanID "${scan_id}" \
  --arg scanName "${scan_name}" '{items: [{
    id: $scanID, repositoryScan: $scanName, taskName: "threat-task", mode: "initial", phase: "succeeded",
    completedAt: "2026-08-09T00:00:00Z", policyDigest: $policy, idempotencyKey: $idempotency,
    sliceCount: 1, reviewedSliceCount: 1, skippedSliceCount: 0, acceptedFindings: 1, droppedFindings: 1
  }]}' >"${positive}-runs.json"
jq -n --arg context "${context_digest}" --arg scanID "${scan_id}" '{items: [{
  id: "slice_api", status: "reviewed", lastScanRunID: $scanID, reviewContextHash: $context
}]}' >"${positive}-slices.json"
jq -n --arg scanID "${scan_id}" --arg scanName "${scan_name}" '{items: [{
  id: "fnd_fixture", repositoryScan: $scanName, scanRunID: $scanID, category: "authorization", severity: "high",
  evidence: [{path: "server.js", startLine: 17, endLine: 17}]
}]}' >"${positive}-findings.json"
jq -n --arg scanID "${scan_id}" --arg scanName "${scan_name}" '{items: [{
  id: "drop_fixture", repositoryScan: $scanName, scanRunID: $scanID, taskName: "review-task", sliceID: "slice_api",
  layer: "validation", reason: "evidence path is outside the trusted review context"
}]}' >"${positive}-dropped.json"
jq -n --arg scanID "${scan_id}" '{source: "generated", generatedByScan: $scanID, version: 1, content: "# Threat Model"}' \
  >"${positive}-threat-model.json"

assert_positive_scan_snapshot "${positive}"
positive_scan_idempotency_snapshot "${positive}" >"${work_dir}/snapshot-a.json"
positive_scan_idempotency_snapshot "${positive}" >"${work_dir}/snapshot-b.json"
cmp -s "${work_dir}/snapshot-a.json" "${work_dir}/snapshot-b.json"

jq '.items[0].spec.env = [{name: "UNTRUSTED", value: "forbidden"}]' "${positive}-tasks.json" >"${work_dir}/positive-env.json"
cp "${positive}-tasks.json" "${work_dir}/positive-tasks-original.json"
cp "${work_dir}/positive-env.json" "${positive}-tasks.json"
if (assert_positive_scan_snapshot "${positive}" >/dev/null 2>&1); then
  echo "positive gate accepted an agent Task with arbitrary env" >&2
  exit 1
fi
cp "${work_dir}/positive-tasks-original.json" "${positive}-tasks.json"

bad_scan_id="scan_bad_fixture"
expected_error="threat model terminal result is missing or invalid: security result scanId does not match task run"
jq -n --arg scanID "${bad_scan_id}" --arg expected "${expected_error}" '{status: {
  phase: "Error", lastScanID: $scanID, lastScanTaskName: "bad-threat-task", lastScanAt: "2026-08-09T00:01:00Z",
  conditions: [{type: "Ready", status: "False", reason: "ScanFailed", message: $expected}]
}}' >"${malformed}-scan.json"
jq -n \
  --arg agent "${agent_name}" \
  --arg scanID "${bad_scan_id}" \
  --arg scanName "${bad_scan_name}" '{items: [{
    metadata: {name: "bad-threat-task", labels: {
      "orka.ai/security-target": $scanName, "orka.ai/security-scan-id": $scanID,
      "orka.ai/security-scan-mode": "initial", "orka.ai/security-stage": "threat-model"
    }},
    spec: {type: "agent", agentRef: {name: $agent}},
    status: {
      phase: "Succeeded", resultRef: {available: true},
      agentExecutionBinding: {contractVersion: "orka.harness.v2", backend: "runtime-pool"},
      execution: {state: "Succeeded", outcome: "Succeeded"},
      delivery: {state: "ReadValidated", outcome: "ReadValidated"}
    }
  }]}' >"${malformed}-tasks.json"
jq -n \
  --arg expected "${expected_error}" \
  --arg scanID "${bad_scan_id}" \
  --arg scanName "${bad_scan_name}" '{items: [{
    id: $scanID, repositoryScan: $scanName, phase: "failed", errorMessage: $expected,
    sliceCount: 0, acceptedFindings: 0, droppedFindings: 0
  }]}' >"${malformed}-runs.json"
for suffix in slices findings dropped; do
  jq -n '{items: []}' >"${malformed}-${suffix}.json"
done
assert_malformed_scan_snapshot "${malformed}"

jq '.items += [{metadata:{labels:{"orka.ai/security-scan-id":"scan_bad_fixture","orka.ai/security-stage":"mapper"}}}]' \
  "${malformed}-tasks.json" >"${work_dir}/malformed-extra-task.json"
cp "${malformed}-tasks.json" "${work_dir}/malformed-tasks-original.json"
cp "${work_dir}/malformed-extra-task.json" "${malformed}-tasks.json"
if (assert_malformed_scan_snapshot "${malformed}" >/dev/null 2>&1); then
  echo "malformed-result gate accepted downstream work" >&2
  exit 1
fi

# shellcheck disable=SC2016 # The harness source must contain this literal variable reference.
grep -Fq 'ACP_CODEX_RUNTIME_IMG="${fake_runtime_ref}"' "${security_script}"
grep -Fq 'patch_controller_images serviceAccount' "${security_script}"
grep -Fq '.ttsCalls == 0 and .toolCalls == 1' "${security_script}"
grep -Fq 'Creating transactionless ACP v2 authority Task/' "${security_script}"
if grep -Fq 'tasks do not support arbitrary task env' "${security_script}"; then
  echo "legacy negative-only compatibility gate remains" >&2
  exit 1
fi

kind_calls="${work_dir}/kind-calls"
kind_mode="collision"
kind() {
  if [[ "$1 $2" == "get clusters" ]]; then
    if [[ "${kind_mode}" == "collision" || "${kind_mode}" == "appeared-after-create-failure" ]]; then
      printf '%s\n' "${kind_cluster}"
    fi
    return 0
  fi
  printf '%q ' "$@" >>"${kind_calls}"
  printf '\n' >>"${kind_calls}"
  if [[ "$1 $2" == "create cluster" && "${kind_mode}" == "create-fail" ]]; then
    kind_mode="appeared-after-create-failure"
    return 1
  fi
  return 0
}

kind_cluster="security-scan-collision"
kind_cleanup_armed="0"
if (setup_kind_cluster >/dev/null 2>&1); then
  echo "SecurityScan harness reused a pre-existing Kind cluster" >&2
  exit 1
fi
[[ ! -s "${kind_calls}" ]]

kind_mode="fresh"
kind_cluster="security-scan-fresh"
kind_config="${work_dir}/isolated-kind.yaml"
kubeconfig_file="${work_dir}/isolated-kubeconfig"
unset ORKA_SECURITY_SCAN_KIND_CONFIG
initialize_isolated_kubeconfig
[[ "${KUBECONFIG}" == "${kubeconfig_file}" ]]
[[ "$(stat -c '%a' "${kubeconfig_file}" 2>/dev/null || stat -f '%Lp' "${kubeconfig_file}")" == "600" ]]
setup_kind_cluster >/dev/null 2>&1
[[ "${kind_cleanup_armed}" == "1" ]]
grep -Fq -- "create cluster --name ${kind_cluster} --config ${kind_config} --kubeconfig ${kubeconfig_file}" "${kind_calls}"

kind_mode="create-fail"
kind_cluster="security-scan-create-fail"
kind_config="${work_dir}/failed-kind.yaml"
kubeconfig_file="${work_dir}/failed-kubeconfig"
kind_cleanup_armed="0"
initialize_isolated_kubeconfig
if setup_kind_cluster >/dev/null 2>&1; then
  echo "failed Kind creation unexpectedly succeeded" >&2
  exit 1
fi
[[ "${kind_cleanup_armed}" == "0" ]] || {
  echo "failed Kind creation armed destructive cleanup without ownership" >&2
  exit 1
}
[[ "${kind_mode}" == "appeared-after-create-failure" ]] || {
  echo "failed Kind creation fixture did not expose the competing cluster" >&2
  exit 1
}
kind_failure_cleanup_dir="${work_dir}/kind-failure-cleanup"
mkdir -p "${kind_failure_cleanup_dir}"
set +e
(
  work_dir="${kind_failure_cleanup_dir}"
  registry_cleanup_armed="0"
  kind_lock_held="0"
  on_exit 1 >/dev/null 2>&1
)
kind_failure_cleanup_status=$?
set -e
[[ "${kind_failure_cleanup_status}" == "1" ]]
[[ ! -e "${kind_failure_cleanup_dir}" ]]
if grep -Fq -- "delete cluster --name ${kind_cluster}" "${kind_calls}"; then
  echo "failed Kind creation deleted a cluster that appeared after the ownership check" >&2
  exit 1
fi
kind_cluster_exists || {
  echo "competing Kind cluster disappeared during unarmed cleanup" >&2
  exit 1
}

kind_cluster="security-scan-lock"
kind_lock_dir=""
kind_lock_held="0"
acquire_kind_cluster_lock
[[ "${kind_lock_held}" == "1" && -d "${kind_lock_dir}" ]]
if (kind_lock_held="0"; acquire_kind_cluster_lock >/dev/null 2>&1); then
  echo "SecurityScan harness allowed concurrent ownership of one Kind cluster" >&2
  exit 1
fi
rmdir "${kind_lock_dir}"
kind_lock_held="0"

[[ "$(security_scan_exit_status 7 1)" == "7" ]]
[[ "$(security_scan_exit_status 0 1)" == "1" ]]
[[ "$(security_scan_exit_status 0 0)" == "0" ]]

registry_state="${work_dir}/registry-state"
registry_error="${work_dir}/registry-error"
docker_mode="ok"
docker() {
  local command="$1"
  shift
  case "${command}" in
    run)
      local owner=""
      while (( $# > 0 )); do
        if [[ "$1" == "--label" ]]; then
          owner="${2#io.orka.test.owner=}"
          shift 2
        else
          shift
        fi
      done
      if [[ "${docker_mode}" == "start-collision" ]]; then
        printf '%s\n' 'foreign-owner' >"${registry_state}"
        return 1
      fi
      if [[ -s "${registry_state}" ]]; then
        return 1
      fi
      printf '%s\n' "${owner}" >"${registry_state}"
      ;;
    rm)
      : >"${registry_state}"
      ;;
    port)
      printf '127.0.0.1:50001\n'
      ;;
    exec)
      ;;
    container)
      local subcommand="$1"
      shift
      case "${subcommand}" in
        ls)
          [[ "${docker_mode}" != "error" ]] || return 1
          [[ ! -s "${registry_state}" ]] || printf 'fixture-container-id\n'
          ;;
        inspect)
          [[ -s "${registry_state}" ]] || return 1
          cat "${registry_state}"
          ;;
        rm)
          : >"${registry_state}"
          ;;
        *) return 1 ;;
      esac
      ;;
    *) return 1 ;;
  esac
}
curl() { return 0; }
kind() {
  [[ "$1 $2" == "get nodes" ]] || return 1
}

: >"${registry_state}"
ORKA_KIND_REGISTRY_NAME=""
printf '%s\n' 'foreign-owner' >"${registry_state}"
if orka_kind_registry_start "security-scan-owned" "owner-a" >/dev/null 2>&1; then
  echo "owned registry startup replaced a foreign container" >&2
  exit 1
fi
[[ "$(<"${registry_state}")" == "foreign-owner" ]]

: >"${registry_state}"
ORKA_KIND_REGISTRY_NAME=""
docker_mode="start-collision"
if orka_kind_registry_start "security-scan-owned" "owner-a" >/dev/null 2>&1; then
  echo "owned registry startup ignored a name collision" >&2
  exit 1
fi
[[ "$(<"${registry_state}")" == "foreign-owner" ]]
registry_collision_cleanup_dir="${work_dir}/registry-collision-cleanup"
mkdir -p "${registry_collision_cleanup_dir}"
set +e
(
  work_dir="${registry_collision_cleanup_dir}"
  registry_cleanup_armed="1"
  registry_owner="owner-a"
  kind_cleanup_armed="0"
  kind_lock_held="0"
  on_exit 1 >/dev/null 2>&1
)
registry_collision_cleanup_status=$?
set -e
[[ "${registry_collision_cleanup_status}" == "1" ]]
[[ ! -e "${registry_collision_cleanup_dir}" ]]
[[ "$(<"${registry_state}")" == "foreign-owner" ]] || {
  echo "armed cleanup removed the foreign registry after a start collision" >&2
  exit 1
}

: >"${registry_state}"
ORKA_KIND_REGISTRY_NAME=""
docker_mode="ok"
orka_kind_registry_start "security-scan-owned" "owner-a"
[[ "$(<"${registry_state}")" == "owner-a" ]]
if orka_kind_registry_stop "security-scan-owned" "owner-b" 2>"${registry_error}"; then
  echo "owned registry cleanup accepted the wrong owner" >&2
  exit 1
fi
[[ "$(<"${registry_state}")" == "owner-a" ]]
grep -Fq 'without matching ownership' "${registry_error}"
orka_kind_registry_stop "security-scan-owned" "owner-a"
[[ ! -s "${registry_state}" ]]

printf '%s\n' 'owner-a' >"${registry_state}"
docker_mode="error"
if orka_kind_registry_stop "security-scan-owned" "owner-a" >/dev/null 2>&1; then
  echo "registry cleanup treated a Docker transport error as absence" >&2
  exit 1
fi

cleanup_transport_dir="${work_dir}/cleanup-transport-success"
mkdir -p "${cleanup_transport_dir}"
set +e
(
  work_dir="${cleanup_transport_dir}"
  registry_cleanup_armed="1"
  registry_owner="owner-a"
  kind_cleanup_armed="0"
  kind_lock_held="0"
  on_exit 0 >/dev/null 2>&1
)
cleanup_transport_status=$?
set -e
[[ "${cleanup_transport_status}" == "1" ]] || {
  echo "successful SecurityScan run ignored a cleanup transport failure" >&2
  exit 1
}
[[ ! -e "${cleanup_transport_dir}" ]]

cleanup_precedence_dir="${work_dir}/cleanup-transport-original-failure"
mkdir -p "${cleanup_precedence_dir}"
set +e
(
  work_dir="${cleanup_precedence_dir}"
  registry_cleanup_armed="1"
  registry_owner="owner-a"
  kind_cleanup_armed="0"
  kind_lock_held="0"
  on_exit 7 >/dev/null 2>&1
)
cleanup_precedence_status=$?
set -e
[[ "${cleanup_precedence_status}" == "7" ]] || {
  echo "cleanup failure replaced the original SecurityScan failure status" >&2
  exit 1
}
[[ ! -e "${cleanup_precedence_dir}" ]]

diagnostics_root="${test_root}/diagnostics"
diagnostics_dir="${diagnostics_root}/fixture-run"
diagnostics_collected="0"
api_forward_log="${test_root}/diagnostic-api-forward.log"
printf '%s\n' 'Authorization: Bearer diagnostic-secret' >"${api_forward_log}"
kubectl() {
  case "$*" in
    *"get repositoryscans -o json"*)
      jq -n '{items:[{
        metadata:{name:"security-goof",namespace:"orka-system",uid:"scan-uid",generation:1,annotations:{debug:"must-not-be-captured"}},
        spec:{repoURL:"https://github.com/sozercan/vekil",branch:"main",ref:"abc",analysisAgentRef:{name:"fixture-agent"},readCredentialRef:{name:"must-not-be-captured"}},
        status:{phase:"Error",conditions:[{type:"Ready",message:"failed safely"}]}
      }]}'
      ;;
    *"get tasks -o json"*)
      jq -n '{items:[{
        metadata:{name:"threat-task",namespace:"orka-system",uid:"task-uid",labels:{"orka.ai/security-target":"security-goof"},annotations:{debug:"must-not-be-captured"}},
        spec:{type:"agent",prompt:"sensitive prompt",env:[{name:"API_KEY",value:"diagnostic-secret"}],agentRef:{name:"fixture-agent"},workspace:{intent:"read",gitRepo:"https://github.com/sozercan/vekil",branch:"main",ref:"abc",readCredentialRef:{name:"must-not-be-captured"}}},
        status:{phase:"Failed",message:"runtime failed"}
      },{
        metadata:{name:"authority-incoming",namespace:"orka-system",uid:"authority-task-uid",labels:{"orka.ai/security-scan-authority-e2e":"true"}},
        spec:{type:"agent",prompt:"sensitive authority prompt",agentRef:{name:"security-scan-authority-agent"},workspace:{intent:"read",gitRepo:"https://github.com/sozercan/vekil",branch:"main",ref:"abc"}},
        status:{phase:"Failed",message:"codex ACP runtime cannot exactly enforce provider-native tool restrictions"}
      }]}'
      ;;
    *"get agents -o json"*)
      jq -n '{items:[]}'
      ;;
    *"get runtimepools -o jsonpath="*)
      printf '%s\n' 'orka-runtimes'
      ;;
    *"get runtimepools -o json"*)
      jq -n '{items:[{
        metadata:{name:"pool",namespace:"orka-system",uid:"pool-uid"},
        spec:{
          trustDomain:{namespace:"orka-system",identity:"fixture"},
          runtimeNamespace:"orka-runtimes",
          runtime:{image:"example.invalid/runtime@sha256:test",profile:{protocolVersion:"orka.harness.v2",digest:"sha256:test",providerKind:"codex",model:"fixture"}},
          desiredReplicas:1,
          capacity:{maxResidentSessions:10,maxRunningPrompts:4},
          credential:"must-not-be-captured"
        },
        status:{lifecycle:"Degraded"}
      }]}'
      ;;
    *"get promptattempts -o json"*)
      jq -n '{items:[{metadata:{name:"attempt"},spec:{id:"attempt-id",taskUid:"task-uid",attempt:1,promptId:"prompt-id",requestDigest:"sha256:test",credentialBindings:[{secretName:"must-not-be-captured",secretKey:"token"}]},status:{executionState:"Failed"}}]}'
      ;;
    *"get externaleffects -o json"*)
      jq -n '{items:[{
        metadata:{name:"effect",namespace:"orka-system",uid:"effect-uid"},
        spec:{id:"effect-id",kind:"workspace.prepare",identityNamespace:"orka-system",aggregateId:"task-uid",operationId:"workspace-prepare-prompt-id",requestDigest:"sha256:test",response:{capability:"must-not-be-captured"}},
        status:{state:"OutcomeUnknown",attempts:1,controllerEpochName:"orka-controller",controllerEpoch:2,version:3,response:{capability:"must-not-be-captured"}}
      }]}'
      ;;
    *"get runtimesessioncontrols -o json"*)
      jq -n '{items:[{
        metadata:{name:"session",namespace:"orka-system",uid:"session-object-uid"},
        spec:{sessionName:"must-not-be-captured",sessionUid:"session-uid",requestDigest:"sha256:test",owner:{kind:"Task",uid:"task-uid"},runtimePoolRef:"pool",credential:"must-not-be-captured"},
        status:{lifecycle:"Poisoned",availability:"ReconciliationBlocked",blockedReason:"fixture blocked",version:2}
      }]}'
      ;;
    *"get jobs -o jsonpath="*)
      printf '%s\n' 'mapper-job'
      ;;
    *"get pods -o jsonpath="*)
      printf '%s\n' 'runtime-pod'
      ;;
    *"logs "*)
      printf '%s\n' 'Authorization: Bearer diagnostic-secret'
      ;;
    *)
      printf '%s\n' 'diagnostic fixture output'
      ;;
  esac
}

collect_security_scan_diagnostics
for expected_file in \
  metadata.json repository-scans.json tasks.json agents.json runtime-pools.json prompt-attempts.json \
  external-effects.json runtime-session-controls.json cluster-resources.txt namespace-resources.txt events.txt controller.log \
  controller-describe.txt api-port-forward.log jobs/mapper-job.log runtime/orka-runtimes-runtime-pod.log; do
  [[ -f "${diagnostics_dir}/${expected_file}" ]] || {
    echo "SecurityScan diagnostics omitted ${expected_file}" >&2
    exit 1
  }
done
if grep -R -Fq 'diagnostic-secret' "${diagnostics_dir}"; then
  echo "SecurityScan diagnostics retained a credential value" >&2
  exit 1
fi
jq -e '
  .items[0].status.phase == "Failed" and
  (.items[0].metadata | has("annotations") | not) and
  (.items[0].spec | has("prompt") | not) and
  (.items[0].spec | has("env") | not) and
  (.items[0].spec.workspace | has("readCredentialRef") | not)
' "${diagnostics_dir}/tasks.json" >/dev/null
jq -e '
  .items[] | select(.metadata.name == "authority-incoming") |
  .status.phase == "Failed" and
  .status.message == "codex ACP runtime cannot exactly enforce provider-native tool restrictions" and
  (.spec | has("prompt") | not)
' "${diagnostics_dir}/tasks.json" >/dev/null
jq -e '
  (.items[0].metadata | has("annotations") | not) and
  (.items[0].spec | has("readCredentialRef") | not)
' "${diagnostics_dir}/repository-scans.json" >/dev/null
jq -e '
  .items[0].spec.runtimeNamespace == "orka-runtimes" and
  .items[0].spec.runtime.profile.providerKind == "codex" and
  (.items[0].spec | has("credential") | not)
' "${diagnostics_dir}/runtime-pools.json" >/dev/null
jq -e '(.items[0].spec | has("credentialBindings") | not)' \
  "${diagnostics_dir}/prompt-attempts.json" >/dev/null
jq -e '
  .items[0].spec.kind == "workspace.prepare" and
  .items[0].status.state == "OutcomeUnknown" and
  (.items[0].spec | has("response") | not) and
  (.items[0].status | has("response") | not)
' "${diagnostics_dir}/external-effects.json" >/dev/null
jq -e '
  .items[0].spec.sessionUid == "session-uid" and
  .items[0].status.lifecycle == "Poisoned" and
  (.items[0].spec | has("sessionName") | not) and
  (.items[0].spec | has("credential") | not)
' "${diagnostics_dir}/runtime-session-controls.json" >/dev/null

grep -Fq 'ORKA_SECURITY_SCAN_DIAGNOSTICS_DIR: ${{ runner.temp }}/security-scan-e2e-diagnostics' "${security_workflow}"
grep -Fq 'if: ${{ failure() }}' "${security_workflow}"
grep -Fq 'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02' "${security_workflow}"
grep -Fq 'path: ${{ runner.temp }}/security-scan-e2e-diagnostics' "${security_workflow}"

preflight_root="${test_root}/preflight-cleanup"
preflight_bin="${test_root}/preflight-bin"
preflight_error="${test_root}/preflight-error"
mkdir -p "${preflight_root}" "${preflight_bin}"
for command in chmod date dirname mktemp rm tr; do
  ln -s "$(command -v "${command}")" "${preflight_bin}/${command}"
done
true_binary="/usr/bin/true"
[[ -x "${true_binary}" ]] || true_binary="/bin/true"
ln -s "${true_binary}" "${preflight_bin}/make"
if RUNNER_TEMP="${preflight_root}" PATH="${preflight_bin}" /bin/bash "${security_script}" \
  >/dev/null 2>"${preflight_error}"; then
  echo "missing prerequisite unexpectedly passed SecurityScan preflight" >&2
  exit 1
fi
grep -Fq 'missing required command: go' "${preflight_error}"
if find "${preflight_root}" -mindepth 1 -maxdepth 1 -name 'security-scan-e2e.*' -print -quit | grep -q .; then
  echo "SecurityScan preflight failure leaked its work directory" >&2
  exit 1
fi

printf '%s\n' 'ok - SecurityScan harness-v2 positive ingestion and malformed-result assertions are exact'
