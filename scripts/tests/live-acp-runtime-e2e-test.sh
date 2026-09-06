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
script="${root}/scripts/live-acp-runtime-e2e.sh"
export ACP_E2E_OPENCODE_CONTEXT_WINDOW=32768
export ACP_E2E_OPENCODE_MAX_TOKENS=4096
body="$(awk '/^delete_test_namespace_now\(\) \{/,/^\}$/' "${script}")"

line_of() {
  local pattern="$1"
  grep -nF -- "${pattern}" <<<"${body}" | head -1 | cut -d: -f1
}

tasks_line="$(line_of 'settle_and_delete_test_tasks')"
agents_line="$(line_of 'delete_test_agents')"
pools_line="$(line_of 'stop_and_delete_test_runtimepools')"
children_line="$(line_of "runtime children for \${namespace} to be removed")"
claims_line="$(line_of 'delete_test_branchclaims')"
namespace_line="$(line_of "delete namespace \"\${namespace}\"")"

for value in "${tasks_line}" "${agents_line}" "${pools_line}" "${children_line}" "${claims_line}" "${namespace_line}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "cleanup ordering assertion could not locate every required barrier" >&2
    exit 1
  }
done

(( tasks_line < agents_line ))
(( agents_line < pools_line ))
(( pools_line < children_line ))
(( children_line < claims_line ))
(( claims_line < namespace_line ))

if grep -F -- '--subresource=status' "${script}" >/dev/null; then
  echo 'live ACP validator still mutates controller-owned Task status' >&2
  exit 1
fi
grep -F 'task_observer_finalizer="acp-e2e.orka.ai/cancellation-observer"' "${script}" >/dev/null
grep -F 'Task/${name} controller-owned deletion barrier' "${script}" >/dev/null
grep -F 'release_task_observer_finalizer "${name}" "${uid}"' "${script}" >/dev/null
grep -F "Task/\${name} finalizer completion" "${script}" >/dev/null
grep -F "patch runtimepool \"\${name}\" --type=merge" "${script}" >/dev/null
grep -F '.spec.ownerUid' "${script}" >/dev/null

printf '%s\n' 'ok - release-gate cleanup uses deletion and controller-owned settlement before namespace teardown'


dry_run_body="$(awk '/^assert_dry_run_rejected\(\) \{/,/^\}$/' "${script}")"
eval "${dry_run_body}"
temp_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-acp-url-test.XXXXXX")"
trap 'rm -rf "${temp_root}"' EXIT
manifest_file="${temp_root}/manifest.json"
printf '{}\n' >"${manifest_file}"
k() {
  printf '%s\n' 'The Task is invalid: spec.workspace: gitRepo must not contain embedded credentials, query parameters, or fragments'
  return 1
}
die() {
  printf 'unexpected die: %s\n' "$*" >&2
  return 1
}
redact() { cat; }
trap 'echo "unexpected ERR trap from expected dry-run rejection" >&2; exit 99' ERR
assert_dry_run_rejected "${manifest_file}" 'spec.workspace' 'gitRepo must not contain embedded credentials, query parameters, or fragments'
trap - ERR

printf '%s\n' 'ok - expected server dry-run rejection does not fire the global ERR trap'


opencode_model_body="$(awk '
  /^if \[\[ -n "\$\{ACP_E2E_OPENCODE_MODEL:-\}" \]\]; then$/ { capture=1 }
  capture && /^concurrency_tasks=/ { exit }
  capture { print }
' "${script}")"
[[ -n "${opencode_model_body}" ]] || {
  echo "OpenCode model normalization block is missing" >&2
  exit 1
}
(
  ACP_E2E_OPENCODE_MODEL="gpt-test"
  codex_model="ignored"
  eval "${opencode_model_body}"
  [[ "${opencode_model}" == "openai/gpt-test" ]]
)
(
  ACP_E2E_OPENCODE_MODEL="anthropic/claude-test"
  codex_model="ignored"
  eval "${opencode_model_body}"
  [[ "${opencode_model}" == "anthropic/claude-test" ]]
)

printf '%s\n' 'ok - OpenCode model overrides normalize bare values and preserve provider-qualified values'

grep -F 'ACP_E2E_OPENCODE_CONTEXT_WINDOW must be a positive integer' "${script}" >/dev/null
grep -F 'ACP_E2E_OPENCODE_MAX_TOKENS must be a positive integer' "${script}" >/dev/null
grep -F '{name:$model,contextWindow:$opencodeContextWindow,maxTokens:$opencodeMaxTokens}' "${script}" >/dev/null
printf '%s
' 'ok - OpenCode live Agents pin reviewed context and output limits'



is_uint_body="$(awk '/^is_uint\(\) \{/,/^\}$/' "${script}")"
transient_body="$(awk '/^pool_transient_counters_zero\(\) \{/,/^\}$/' "${script}")"
eval "${is_uint_body}"
eval "${transient_body}"
namespace="test-namespace"
pool_payload=''
k() {
  printf '%s\n' "${pool_payload}"
}

pool_payload='{"status":{"capacity":{"maxResidentSessions":10,"maxRunningPrompts":4,"residentSessions":1,"liveDescendants":1}}}'
pool_transient_counters_zero test-pool 1

pool_payload='{"status":{"capacity":{"maxResidentSessions":10,"maxRunningPrompts":4,"residentSessions":1,"liveDescendants":2}}}'
if pool_transient_counters_zero test-pool 1; then
  echo "transient quiescence accepted descendants not bounded by resident Sessions" >&2
  exit 1
fi

pool_payload='{"status":{"capacity":{"maxResidentSessions":10,"maxRunningPrompts":4,"residentSessions":1,"liveDescendants":1,"runningPrompts":1}}}'
if pool_transient_counters_zero test-pool 1; then
  echo "transient quiescence accepted a running prompt" >&2
  exit 1
fi

printf '%s\n' 'ok - transient quiescence permits only resident-session descendants'


write_cleanup_body="$(awk '/^settle_write_task_for_remote_cleanup\(\) \{/,/^\}/' "${script}")"
write_cleanup_park_line="$(grep -nF 'patch runtimepool "${pool}" --type=merge' <<<"${write_cleanup_body}" | head -1 | cut -d: -f1)"
write_cleanup_payload_line="$(grep -nF '"desiredReplicas":0' <<<"${write_cleanup_body}" | head -1 | cut -d: -f1)"
write_cleanup_stop_line="$(grep -nF 'write RuntimePool/${pool} stopped after publication finalization' <<<"${write_cleanup_body}" | head -1 | cut -d: -f1)"
write_cleanup_outcome_line="$(grep -nF 'outcome="$(jq -r' <<<"${write_cleanup_body}" | head -1 | cut -d: -f1)"
for value in "${write_cleanup_park_line}" "${write_cleanup_payload_line}" "${write_cleanup_stop_line}" "${write_cleanup_outcome_line}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "write cleanup parking assertion could not locate every required barrier" >&2
    exit 1
  }
done
(( write_cleanup_park_line < write_cleanup_payload_line ))
(( write_cleanup_payload_line < write_cleanup_stop_line ))
(( write_cleanup_stop_line < write_cleanup_outcome_line ))

printf '%s\n' 'ok - terminal write cleanup drains and stops its RuntimePool before remote effect deletion'


line_of_script() {
  local pattern="$1"
  grep -nF -- "${pattern}" "${script}" | tail -1 | cut -d: -f1
}

concurrency_line="$(line_of_script 'run_concurrency_check "${codex_agent}" codex "${codex_model}" "${codex_pool}"')"
park_read_line="$(grep -nF 'park_runtimepool "${codex_pool}"' "${script}" | head -1 | cut -d: -f1)"
timeout_line="$(line_of_script 'run_timeout_check codex "${codex_model}" "${codex_tool_agent}"')"
cancel_line="$(line_of_script 'run_explicit_cancel_check codex "${codex_model}" "${codex_tool_agent}"')"
restart_line="$(line_of_script 'run_controller_restart_check codex "${codex_model}" "${codex_tool_agent}" "${restart_nonce}"')"
park_aux_line="$(line_of_script 'park_provider_runtimepools_except codex "${codex_pool}"')"
resume_read_line="$(line_of_script 'resume_runtimepool "${codex_pool}"')"
replacement_line="$(line_of_script 'run_pool_replacement_check codex "${codex_model}"')"

for value in "${concurrency_line}" "${park_read_line}" "${timeout_line}" "${cancel_line}" "${restart_line}" "${park_aux_line}" "${resume_read_line}" "${replacement_line}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "profile-phase parking assertion could not locate every required step" >&2
    exit 1
  }
done

(( concurrency_line < park_read_line ))
(( park_read_line < timeout_line ))
(( timeout_line < cancel_line ))
(( cancel_line < restart_line ))
(( restart_line < park_aux_line ))
(( park_aux_line < resume_read_line ))
(( resume_read_line < replacement_line ))

grep -F 'wait_until "RuntimePool/${pool} stopped after phase parking" "${state_wait_seconds}" pool_stopped "${pool}"' "${script}" >/dev/null
grep -F 'wait_pool_serving "${pool}"' "${script}" >/dev/null

printf '%s\n' 'ok - profile-specific RuntimePools are parked and resumed in capacity-safe order'

mutation_allowed_body="$(awk '/^runtimepool_mutations_allowed\(\) \{/,/^\}$/' "${script}")"
require_mutation_body="$(awk '/^require_runtimepool_mutation_scope\(\) \{/,/^\}$/' "${script}")"
[[ -n "${mutation_allowed_body}" && -n "${require_mutation_body}" ]] || {
  echo "shared RuntimePool mutation guards are missing" >&2
  exit 1
}
eval "${mutation_allowed_body}"
eval "${require_mutation_body}"
namespace="shared-namespace"
namespace_shared=1
shared_pool_mutation_allowed=0
warn() { :; }
if runtimepool_mutations_allowed || require_runtimepool_mutation_scope; then
  echo "shared namespace permitted RuntimePool mutation without an isolated-cluster opt-in" >&2
  exit 1
fi
shared_pool_mutation_allowed=1
runtimepool_mutations_allowed
require_runtimepool_mutation_scope
namespace_shared=0
shared_pool_mutation_allowed=0
runtimepool_mutations_allowed
require_runtimepool_mutation_scope
grep -F 'Shared watch namespace: skipping controller restart and RuntimePool parking, resume, and replacement checks' "${script}" >/dev/null
grep -F 'RELEASE_GATE=1 requires an isolated namespace or ACP_E2E_ALLOW_SHARED_POOL_MUTATION=1 on a dedicated cluster' "${script}" >/dev/null

printf '%s\n' 'ok - shared namespaces fail closed for RuntimePool mutations unless a dedicated cluster is explicit'


profile_valid_body="$(awk '/^pool_profile_projection_valid\(\) \{/,/^\}$/' "${script}")"
profile_fingerprint_body="$(awk '/^pool_profile_projection_fingerprint\(\) \{/,/^\}$/' "${script}")"
profile_stable_body="$(awk '/^pool_profile_projection_stable\(\) \{/,/^\}$/' "${script}")"
wait_fast_body="$(awk '/^wait_until_fast\(\) \{/,/^\}$/' "${script}")"
wait_profile_body="$(awk '/^wait_pool_profile_projection\(\) \{/,/^\}$/' "${script}")"
for function_body in "${profile_valid_body}" "${profile_fingerprint_body}" "${profile_stable_body}" \
    "${wait_fast_body}" "${wait_profile_body}"; do
  [[ -n "${function_body}" ]] || {
    echo "stable RuntimePool profile projection helper is missing" >&2
    exit 1
  }
  eval "${function_body}"
done

profile_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-acp-profile-test.XXXXXX")"
profile_output="${profile_root}/stable.json"
profile_count_file="${profile_root}/count"
namespace="test-namespace"
valid_profile_payload='{
  "metadata":{
    "name":"acp-codex-1111111111111111",
    "uid":"pool-uid",
    "generation":3,
    "labels":{
      "orka.ai/acp-runtime-pool":"true",
      "orka.ai/acp-trust-domain":"test-namespace",
      "orka.ai/acp-profile":"aaaaaaaaaaaaaaaa"
    }
  },
  "spec":{
    "desiredReplicas":1,
    "trustDomain":{"namespace":"test-namespace"},
    "runtime":{
      "image":"example.invalid/runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111",
      "profile":{
        "protocolVersion":"orka.harness.v2",
        "providerKind":"codex",
        "model":"gpt-test",
        "workspaceIntent":"read",
        "digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "digestSchemaVersion":"v1",
        "adapterDigests":["sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
        "agentConfigurationDigest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "toolPolicyDigest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        "approvalPolicyDigest":"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "mcpConfigurationDigest":"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "proxyCredentialRole":"provider-read",
        "proxyCredentialScope":"test"
      }
    }
  },
  "status":{
    "observedGeneration":3,
    "lifecycle":"Serving",
    "admissionState":"Accepting",
    "currentReplicas":1,
    "desiredReplicas":1,
    "controllerEpoch":7,
    "activeInstance":{
      "controllerEpoch":7,
      "protocolVersion":"orka.harness.v2",
      "profileDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "profileDigestSchemaVersion":"v1",
      "providerTokenGeneration":"0123456789abcdef",
      "runtimeInstanceID":"pod-uid.boot-id",
      "bootID":"boot-id",
      "podNamespace":"orka-runtimes",
      "podName":"runtime-pod",
      "podUID":"pod-uid",
      "podAddress":"10.0.0.8",
      "lastObservedTime":"2026-08-01T00:00:00Z"
    },
    "capacity":{"maxResidentSessions":10,"maxRunningPrompts":4}
  }
}'
valid_profile_pod_payload='{
  "metadata":{
    "namespace":"orka-runtimes",
    "name":"runtime-pod",
    "uid":"pod-uid",
    "annotations":{"orka.ai/provider-token-generation":"0123456789abcdef"}
  }
}'
profile_first_payload="${valid_profile_payload}"
profile_second_payload="${valid_profile_payload}"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="${valid_profile_pod_payload}"
k() {
  local count
  case "${4:-}" in
    runtimepool)
      count="$(cat "${profile_count_file}")"
      count=$((count + 1))
      printf '%s\n' "${count}" >"${profile_count_file}"
      if (( count == 1 )); then
        printf '%s\n' "${profile_first_payload}"
      else
        printf '%s\n' "${profile_second_payload}"
      fi
      ;;
    pod)
      count="$(cat "${profile_count_file}")"
      if (( count == 1 )); then
        printf '%s\n' "${profile_first_pod_payload}"
      else
        printf '%s\n' "${profile_second_pod_payload}"
      fi
      ;;
    *)
      return 1
      ;;
  esac
}

assert_profile_projection_rejected() {
  local description="$1"
  local pool_payload="$2"
  local pod_payload="$3"
  if pool_profile_projection_valid \
    acp-codex-1111111111111111 codex gpt-test read "${pool_payload}" "${pod_payload}"; then
    echo "profile projection accepted ${description}" >&2
    exit 1
  fi
}

assert_profile_projection_rejected \
  'status from a stale RuntimePool generation' \
  "$(jq '.status.observedGeneration = 2' <<<"${valid_profile_payload}")" \
  "${valid_profile_pod_payload}"
if ! pool_profile_projection_valid \
    acp-codex-1111111111111111 codex gpt-test read \
    "${valid_profile_payload}" "${valid_profile_pod_payload}"; then
  echo "profile projection rejected a pool identity name distinct from its profile digest" >&2
  exit 1
fi
if pool_profile_projection_valid \
    acp-codex-2222222222222222 codex gpt-test read \
    "${valid_profile_payload}" "${valid_profile_pod_payload}"; then
  echo "profile projection accepted a payload for a different requested RuntimePool" >&2
  exit 1
fi
assert_profile_projection_rejected \
  'a profile label that does not bind the canonical profile digest' \
  "$(jq '.metadata.labels["orka.ai/acp-profile"] = "bbbbbbbbbbbbbbbb"' <<<"${valid_profile_payload}")" \
  "${valid_profile_pod_payload}"
assert_profile_projection_rejected \
  'a missing runtime-pool ownership label' \
  "$(jq 'del(.metadata.labels["orka.ai/acp-runtime-pool"])' <<<"${valid_profile_payload}")" \
  "${valid_profile_pod_payload}"
assert_profile_projection_rejected \
  'a trust-domain label that does not bind the RuntimePool namespace' \
  "$(jq '.metadata.labels["orka.ai/acp-trust-domain"] = "other-namespace"' <<<"${valid_profile_payload}")" \
  "${valid_profile_pod_payload}"

assert_profile_projection_rejected \
  'a provider token generation that does not match the selected Pod annotation' \
  "${valid_profile_payload}" \
  "$(jq '.metadata.annotations["orka.ai/provider-token-generation"] = "fedcba9876543210"' \
    <<<"${valid_profile_pod_payload}")"
assert_profile_projection_rejected \
  'a runtimeInstanceID not derived from podUID and bootID' \
  "$(jq '.status.activeInstance.runtimeInstanceID = "malformed-instance-id"' <<<"${valid_profile_payload}")" \
  "${valid_profile_pod_payload}"

printf '0\n' >"${profile_count_file}"
profile_first_payload="$(jq 'del(.status.activeInstance.podUID)' <<<"${valid_profile_payload}")"
profile_second_payload="${valid_profile_payload}"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="${valid_profile_pod_payload}"
# shellcheck disable=SC2034 # Consumed by wait_pool_profile_projection from the evaluated script body.
wait_seconds=3
wait_pool_profile_projection acp-codex-1111111111111111 codex gpt-test read "${profile_output}"
[[ "$(cat "${profile_count_file}")" == "3" ]] || {
  echo "profile wait did not retry the incomplete projection before accepting two stable observations" >&2
  exit 1
}
jq -e '
  .metadata.generation == 3
  and .status.observedGeneration == 3
  and .status.activeInstance.providerTokenGeneration == "0123456789abcdef"
  and .status.activeInstance.runtimeInstanceID == "pod-uid.boot-id"
' "${profile_output}" >/dev/null

printf '0\n' >"${profile_count_file}"
profile_first_payload="${valid_profile_payload}"
profile_second_payload="$(jq '.metadata.generation = 4 | .status.observedGeneration = 4' \
  <<<"${valid_profile_payload}")"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="${valid_profile_pod_payload}"
if pool_profile_projection_stable acp-codex-1111111111111111 codex gpt-test read "${profile_output}"; then
  echo "stable profile predicate ignored a RuntimePool generation change" >&2
  exit 1
fi

printf '0\n' >"${profile_count_file}"
profile_first_payload="${valid_profile_payload}"
profile_second_payload="$(jq '.status.activeInstance.providerTokenGeneration = "fedcba9876543210"' \
  <<<"${valid_profile_payload}")"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="$(jq '.metadata.annotations["orka.ai/provider-token-generation"] = "fedcba9876543210"' \
  <<<"${valid_profile_pod_payload}")"
if pool_profile_projection_stable acp-codex-1111111111111111 codex gpt-test read "${profile_output}"; then
  echo "stable profile predicate ignored a provider token generation change" >&2
  exit 1
fi

printf '0\n' >"${profile_count_file}"
profile_first_payload="${valid_profile_payload}"
profile_second_payload="$(jq '
  .status.activeInstance.podUID = "replacement-pod-uid"
  | .status.activeInstance.runtimeInstanceID = "replacement-pod-uid.boot-id"
' <<<"${valid_profile_payload}")"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="$(jq '.metadata.uid = "replacement-pod-uid"' <<<"${valid_profile_pod_payload}")"
if pool_profile_projection_stable acp-codex-1111111111111111 codex gpt-test read "${profile_output}"; then
  echo "stable profile predicate accepted two different complete instance fences" >&2
  exit 1
fi

printf '0\n' >"${profile_count_file}"
profile_first_payload="${valid_profile_payload}"
profile_second_payload="${valid_profile_payload}"
profile_first_pod_payload="${valid_profile_pod_payload}"
profile_second_pod_payload="${valid_profile_pod_payload}"
pool_profile_projection_stable acp-codex-1111111111111111 codex gpt-test read "${profile_output}"
jq -e '
  .metadata.generation == .status.observedGeneration
  and .status.activeInstance.providerTokenGeneration == "0123456789abcdef"
  and .status.activeInstance.podUID == "pod-uid"
  and .status.activeInstance.bootID == "boot-id"
  and .status.activeInstance.runtimeInstanceID == "pod-uid.boot-id"
' "${profile_output}" >/dev/null

grep -F 'wait_until_fast "RuntimePool/${pool} complete stable ACP v2 ${provider}/${intent} profile projection"' \
  "${script}" >/dev/null
grep -F 'wait_pool_profile_projection "${pool}" "${provider}" "${model}" "${intent}" "${pool_file}"' \
  "${script}" >/dev/null

rm -rf "${profile_root}"
printf '%s\n' 'ok - complete RuntimePool profile projection retries until two stable fenced observations'


grep -F 'for ((attempt = 1; attempt <= 10; attempt++)); do' "${script}" >/dev/null
grep -F 'patch task "${task}" --type=merge' "${script}" >/dev/null
grep -F 'orka.ai/acp-e2e-validated":"true"' "${script}" >/dev/null
grep -F 'patch task "${fork_task}" --type=merge' "${script}" >/dev/null
grep -F 'release-gate command failed with status ${rc} at ${failed_source}:${failed_line} (${failed_function})' "${script}" >/dev/null
if grep -F 'label task "${task}"' "${script}" >/dev/null; then
  echo "validated Task metadata still uses conflict-prone kubectl label" >&2
  exit 1
fi

printf '%s\n' 'ok - terminal validation uses retried reads, atomic metadata patches, and safe failure locations'


grep -F 'apply_agent codex "${codex_model}" "${codex_tool_agent}" 12 true' "${script}" >/dev/null
[[ "$(grep -Fc 'apply_agent codex "${codex_model}" "${codex_tool_agent}" 12 true' "${script}")" == "1" ]]
grep -F 'explicit-cancel check selected RuntimePool/${pool}, want shared tool RuntimePool/${tool_profile_pool}' "${script}" >/dev/null
grep -F 'controller-restart check selected RuntimePool/${pool}, want shared tool RuntimePool/${tool_profile_pool}' "${script}" >/dev/null
grep -F 'verbs:["get","list","watch","create","delete"]' "${script}" >/dev/null
cancel_body="$(awk '/^run_explicit_cancel_check\(\) \{/,/^\}/' "${script}")"
cancel_hold_line="$(grep -nF 'add_task_observer_finalizer "${task}" "${uid}"' <<<"${cancel_body}" | cut -d: -f1)"
cancel_request_line="$(grep -nF 'request_task_cancellation "${task}"' <<<"${cancel_body}" | cut -d: -f1)"
cancel_assert_line="$(grep -nF 'assert_cancelled_task "${task}" "${snapshot}"' <<<"${cancel_body}" | cut -d: -f1)"
cancel_release_line="$(grep -nF 'release_task_observer_finalizer "${task}" "${uid}"' <<<"${cancel_body}" | cut -d: -f1)"
for value in "${cancel_hold_line}" "${cancel_request_line}" "${cancel_assert_line}" "${cancel_release_line}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "explicit cancellation assertion could not locate every required barrier" >&2
    exit 1
  }
done
(( cancel_hold_line < cancel_request_line ))
(( cancel_request_line < cancel_assert_line ))
(( cancel_assert_line < cancel_release_line ))
grep -F 'api_request DELETE "/api/v1/tasks/${task}?namespace=${namespace}"' "${script}" >/dev/null
write_cleanup_body="$(awk '/^settle_write_task_for_remote_cleanup\(\) \{/,/^\}/' "${script}")"
grep -F 'request_task_cancellation "${write_task_name}"' <<<"${write_cleanup_body}" >/dev/null
if grep -Eq 'acp-(timeout|cancel|restart)-agent-' "${script}"; then
  echo "profile-specific checks still create distinct Agents" >&2
  exit 1
fi

printf '%s\n' 'ok - timeout, authenticated deletion cancellation, and restart share one immutable tool profile'

grep -F 'Running OpenCode native ACP read, continuation, and read-policy validation' "${script}" >/dev/null
grep -F 'run_opencode_read_policy_check "${opencode_policy_agent}" "${opencode_model}"' "${script}" >/dev/null
grep -F 'Attempt to use Bash and a file-mutation tool to create SHOULD_NOT_EXIST.txt' "${script}" >/dev/null
printf '%s
' 'ok - OpenCode live gate covers native ACP, continuation, and read-intent tool denial'


grep -F 'local deadline=$((SECONDS + 90))' "${script}" >/dev/null
grep -F 'while (( SECONDS < deadline )); do' "${script}" >/dev/null
grep -F 'port-forward attempt %d at %s' "${script}" >/dev/null
grep -F 'controller API port-forward failed after Pod-aware retries' "${script}" >/dev/null
grep -F 'release-gate failure stack functions=${FUNCNAME[*]:1:5} lines=${BASH_LINENO[*]:0:5}' "${script}" >/dev/null

printf '%s\n' 'ok - controller API forwarding retries across controller Pod replacement'


grep -F 'rollout status deployment/"${controller_deployment}" --timeout=10m' "${script}" >/dev/null
restart_settle_line="$(line_of_script 'assert_restart_task_settled "${task}" "${old_snapshot}"')"
restart_resume_line="$(line_of_script 'resume_runtimepool "${pool}"')"
restart_replace_line="$(line_of_script 'RuntimePool/${pool} replacement after controller epoch change')"
(( restart_settle_line < restart_resume_line ))
(( restart_resume_line < restart_replace_line ))
grep -F 'provider-${provider}-owner-uids.txt' "${script}" >/dev/null
grep -F 'delete_test_branchclaims "${owners_file}" || die "failed to remove ${provider} BranchClaims before provider handoff"' "${script}" >/dev/null

printf '%s\n' 'ok - restart drain resumes its pool and provider handoff retains BranchClaim owners'


provider_handoff_body="$(awk '/^remove_provider_resources\(\) \{/,/^\}$/' "${script}")"
[[ -n "${provider_handoff_body}" ]] || {
  echo "provider handoff cleanup function is missing" >&2
  exit 1
}
(
  eval "${provider_handoff_body}"
  handoff_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-provider-handoff-test.XXXXXX")"
  trap 'rm -rf "${handoff_root}"' EXIT
  temp_root="${handoff_root}"
  namespace="test-namespace"
  # shellcheck disable=SC2034 # Consumed by remove_provider_resources from the evaluated script body.
  run_id="test-run"
  # shellcheck disable=SC2034 # Consumed by remove_provider_resources from the evaluated script body.
  state_wait_seconds=37
  events_file="${handoff_root}/events"
  expected_file="${handoff_root}/expected"
  : >"${events_file}"

  log() { :; }
  assert_all_tasks_validated() { :; }
  record_runtime_namespace() { printf 'record-runtime-namespace:%s\n' "$1" >>"${events_file}"; }
  pool_stopped() { printf 'pool-stopped:%s\n' "$1" >>"${events_file}"; }
  wait_until() {
    local description="$1"
    local timeout="$2"
    shift 2
    printf 'wait:%s|%s|%s\n' "${description}" "${timeout}" "$*" >>"${events_file}"
    "$@"
  }
  delete_test_branchclaims() { printf 'delete-branchclaims:%s\n' "$1" >>"${events_file}"; }
  die() {
    printf 'unexpected die: %s\n' "$*" >&2
    return 1
  }
  k() {
    if [[ "$*" == "-n ${namespace} get task -o json" ]]; then
      printf '%s\n' '{"items":[{"metadata":{"uid":"task-uid"},"status":{"execution":{"runtimeSessionUID":"session-uid"}}}]}'
      return 0
    fi
    if [[ "$*" == "-n ${namespace} get runtimepool -o json" ]]; then
      cat <<'JSON'
{"items":[
  {"metadata":{"name":"codex-a"},"spec":{"runtime":{"profile":{"providerKind":"codex"}},"runtimeNamespace":"runtime-a"},"status":{"activeInstance":{"podNamespace":"active-a"}}},
  {"metadata":{"name":"claude-a"},"spec":{"runtime":{"profile":{"providerKind":"claude"}},"runtimeNamespace":"runtime-claude"}},
  {"metadata":{"name":"codex-b"},"spec":{"runtime":{"profile":{"providerKind":"codex"}},"runtimeNamespace":"runtime-b"}}
]}
JSON
      return 0
    fi
    printf 'kubectl:%s\n' "$*" >>"${events_file}"
  }

  remove_provider_resources codex codex-agent

  cat >"${expected_file}" <<EOF
kubectl:-n test-namespace delete task --all --wait=true --timeout=5m
record-runtime-namespace:active-a
kubectl:-n test-namespace patch runtimepool codex-a --type=merge -p {"spec":{"desiredReplicas":0}}
record-runtime-namespace:runtime-b
kubectl:-n test-namespace patch runtimepool codex-b --type=merge -p {"spec":{"desiredReplicas":0}}
wait:RuntimePool/codex-a stopped before codex provider handoff|37|pool_stopped codex-a
pool-stopped:codex-a
wait:RuntimePool/codex-b stopped before codex provider handoff|37|pool_stopped codex-b
pool-stopped:codex-b
kubectl:-n test-namespace delete agent codex-agent --ignore-not-found=true --wait=true --timeout=2m
kubectl:-n test-namespace delete runtimepool codex-a --wait=true --timeout=5m
kubectl:-n test-namespace delete runtimepool codex-b --wait=true --timeout=5m
delete-branchclaims:${handoff_root}/provider-codex-owner-uids.txt
EOF
  if ! cmp -s "${expected_file}" "${events_file}"; then
    echo "provider handoff did not scale every matching pool to zero and wait for Stopped before deletion" >&2
    diff -u "${expected_file}" "${events_file}" >&2 || true
    exit 1
  fi
)

(
  eval "${provider_handoff_body}"
  handoff_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-provider-handoff-shared-test.XXXXXX")"
  trap 'rm -rf "${handoff_root}"' EXIT
  temp_root="${handoff_root}"
  namespace="test-namespace"
  # shellcheck disable=SC2034 # Consumed by remove_provider_resources from the evaluated script body.
  run_id="test-run"
  # shellcheck disable=SC2034 # Consumed by remove_provider_resources from the evaluated script body.
  state_wait_seconds=37
  # shellcheck disable=SC2034 # Shared watch-namespace mode is the branch under test.
  namespace_shared=1
  events_file="${handoff_root}/events"
  expected_file="${handoff_root}/expected"
  : >"${events_file}"

  log() { printf 'log:%s\n' "$1" >>"${events_file}"; }
  assert_all_tasks_validated() { :; }
  record_runtime_namespace() { printf 'record-runtime-namespace:%s\n' "$1" >>"${events_file}"; }
  pool_stopped() { printf 'pool-stopped:%s\n' "$1" >>"${events_file}"; }
  wait_until() { printf 'wait:%s\n' "$1" >>"${events_file}"; }
  delete_test_branchclaims() { printf 'delete-branchclaims:%s\n' "$1" >>"${events_file}"; }
  die() {
    printf 'unexpected die: %s\n' "$*" >&2
    return 1
  }
  k() {
    if [[ "$*" == "-n ${namespace} get task -l orka.ai/acp-e2e-run=test-run -o json" ]]; then
      printf '%s\n' '{"items":[{"metadata":{"uid":"run-task-uid"},"status":{"execution":{"runtimeSessionUID":"run-session-uid"}}}]}'
      return 0
    fi
    if [[ "$*" == "-n ${namespace} get task -o json" || "$*" == "-n ${namespace} get runtimepool -o json" ]]; then
      printf 'unexpected namespace-wide read: %s\n' "$*" >&2
      return 1
    fi
    printf 'kubectl:%s\n' "$*" >>"${events_file}"
  }

  remove_provider_resources codex codex-agent

  cat >"${expected_file}" <<EOF
log:Removing codex Tasks, Agents, and RuntimePools before the next provider
kubectl:-n test-namespace delete task -l orka.ai/acp-e2e-run=test-run --wait=true --timeout=5m
log:Shared watch namespace: leaving codex RuntimePools to the controller idle policy
kubectl:-n test-namespace delete agent codex-agent --ignore-not-found=true --wait=true --timeout=2m
delete-branchclaims:${handoff_root}/provider-codex-owner-uids.txt
EOF
  if ! cmp -s "${expected_file}" "${events_file}"; then
    echo "shared-namespace provider handoff must delete only run-labeled Tasks and leave RuntimePools alone" >&2
    diff -u "${expected_file}" "${events_file}" >&2 || true
    exit 1
  fi
  if ! grep -q 'run-task-uid' "${handoff_root}/provider-codex-owner-uids.txt"; then
    echo "shared-namespace provider handoff must record run-owned Task owners for BranchClaim cleanup" >&2
    exit 1
  fi
)

printf '%s\n' 'ok - shared watch-namespace provider handoff removes only run-owned Tasks and Agents'

provider_remove_line="$(line_of_script 'remove_provider_resources codex "${codex_agent}" "${codex_tool_agent}"')"
provider_children_line="$(line_of_script 'wait_until "Codex runtime children removal" 300 runtime_children_absent')"
opencode_start_line="$(line_of_script 'run_read_smoke opencode "${opencode_model}"')"
opencode_policy_line="$(line_of_script 'run_opencode_read_policy_check "${opencode_policy_agent}" "${opencode_model}"')"
opencode_remove_line="$(line_of_script 'remove_provider_resources opencode "${opencode_agent}" "${opencode_policy_agent}"')"
opencode_children_line="$(line_of_script 'wait_until "OpenCode runtime children removal" 300 runtime_children_absent')"
claude_start_line="$(line_of_script 'run_read_smoke claude "${claude_model}"')"
(( provider_remove_line < provider_children_line ))
(( provider_children_line < opencode_start_line ))
(( opencode_start_line < opencode_policy_line ))
(( opencode_policy_line < opencode_remove_line ))
(( opencode_remove_line < opencode_children_line ))
(( opencode_children_line < claude_start_line ))

printf '%s\n' 'ok - provider handoff scales all matching RuntimePools to zero, waits for Stopped, and deletes them before the next provider'


grep -F 'ACP_E2E_COPILOT_MODEL              Copilot model (default: gpt-5.3-codex)' "${script}" >/dev/null
grep -F 'copilot_model="${ACP_E2E_COPILOT_MODEL:-gpt-5.3-codex}"' "${script}" >/dev/null
claude_remove_line="$(line_of_script 'remove_provider_resources claude "${claude_agent}"')"
claude_children_line="$(line_of_script 'wait_until "Claude runtime children removal" 300 runtime_children_absent')"
copilot_start_line="$(line_of_script 'run_read_smoke copilot "${copilot_model}"')"
copilot_validated_line="$(awk -v start="${copilot_start_line}" '
  NR > start && $0 == "assert_all_tasks_validated" { print NR; exit }
' "${script}")"
for value in "${claude_remove_line}" "${claude_children_line}" "${copilot_start_line}" "${copilot_validated_line}"; do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "Copilot execution assertion could not locate every required step" >&2
    exit 1
  }
done
(( claude_start_line < claude_remove_line ))
(( claude_remove_line < claude_children_line ))
(( claude_children_line < copilot_start_line ))
(( copilot_start_line < copilot_validated_line ))
grep -F 'copilot_agent="$(sanitize_name "acp-copilot-${run_id}")"' "${script}" >/dev/null
grep -F 'copilot_task="$(sanitize_name "acp-copilot-read-${run_id}")"' "${script}" >/dev/null
grep -F 'copilot_session="$(sanitize_name "acp-copilot-session-${run_id}")"' "${script}" >/dev/null
grep -F 'copilot_nonce="copilot-${run_id}-${RANDOM}-${RANDOM}"' "${script}" >/dev/null

printf '%s\n' 'ok - GitHub Copilot executes the canonical read and continuation scenario after a safe Claude provider handoff'


owners_fixture="$(mktemp "${TMPDIR:-/tmp}/orka-branchclaim-owners.XXXXXX")"
printf '%s\n' owner-match >"${owners_fixture}"
matched_claims="$(jq -r --rawfile owners "${owners_fixture}" '
  ($owners | split("\n") | map(select(length > 0))) as $ownerUIDs
  | .items[]
  | . as $claim
  | select($ownerUIDs | index($claim.spec.ownerUid))
  | $claim.metadata.name
' <<'JSON'
{"items":[
  {"metadata":{"name":"matching"},"spec":{"ownerUid":"owner-match"}},
  {"metadata":{"name":"other"},"spec":{"ownerUid":"owner-other"}}
]}
JSON
)"
[[ "${matched_claims}" == "matching" ]]
rm -f "${owners_fixture}"

printf '%s\n' 'ok - BranchClaim owner matching selects only retained provider owners'


restart_fence_body="$(awk '/^assert_restart_task_fence\(\) \{/,/^\}$/' "${script}")"
[[ -n "${restart_fence_body}" ]] || {
  echo "restart recovery fence assertion is missing" >&2
  exit 1
}
eval "${restart_fence_body}"
restart_fence_root="$(mktemp -d "${TMPDIR:-/tmp}/orka-restart-fence-test.XXXXXX")"
temp_root="${restart_fence_root}"
snapshot_file="${restart_fence_root}/snapshot.json"
cat >"${snapshot_file}" <<'JSON'
{"poolName":"pool-a","poolUID":"pool-uid","controllerEpoch":52,"runtimeInstanceID":"pod-uid.boot-id"}
JSON
task_payload='{"metadata":{"labels":{"orka.ai/runtime-pool":"pool-a"}},"status":{"execution":{"runtimePoolName":"pool-a","runtimePoolUID":"pool-uid","runtimeInstanceID":"pod-uid.boot-id","controllerEpoch":53,"promptID":"prompt-1","requestDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","runtimeSessionUID":"session-1","runtimeSessionGeneration":1,"attempt":1}}}'
valid_task_payload="${task_payload}"
task_json() { printf '%s\n' "${task_payload}"; }
safe_task_summary() { :; }
die() { return 1; }
assert_restart_task_fence restart-task "${snapshot_file}"

task_payload="$(jq '.status.execution.controllerEpoch = 51' <<<"${valid_task_payload}")"
if assert_restart_task_fence restart-task "${snapshot_file}"; then
  echo "restart fence accepted a regressed controller epoch" >&2
  exit 1
fi

task_payload="$(jq '.status.execution.controllerEpoch = 53 | .status.execution.runtimeInstanceID = "other.boot"' <<<"${valid_task_payload}")"
if assert_restart_task_fence restart-task "${snapshot_file}"; then
  echo "restart fence accepted a different runtime instance" >&2
  exit 1
fi

task_payload="$(jq '.status.execution.controllerEpoch = "53"' <<<"${valid_task_payload}")"
if assert_restart_task_fence restart-task "${snapshot_file}"; then
  echo "restart fence accepted a nonnumeric Task controller epoch" >&2
  exit 1
fi

bad_snapshot_file="${restart_fence_root}/bad-snapshot.json"
jq '.controllerEpoch = "52"' "${snapshot_file}" >"${bad_snapshot_file}"
task_payload="${valid_task_payload}"
if assert_restart_task_fence restart-task "${bad_snapshot_file}"; then
  echo "restart fence accepted a nonnumeric snapshot controller epoch" >&2
  exit 1
fi

rm -rf "${restart_fence_root}"
printf '%s\n' 'ok - restart recovery fence preserves the old runtime instance and permits a newer controller epoch'


lifecycle_reached_body="$(awk '/^pool_lifecycle_reached\(\) \{/,/^\}$/' "${script}")"
[[ -n "${lifecycle_reached_body}" ]] || {
  echo "monotonic RuntimePool lifecycle assertion is missing" >&2
  exit 1
}
eval "${lifecycle_reached_body}"
namespace="test-namespace"
pool_payload=''
k() { printf '%s\n' "${pool_payload}"; }

pool_payload='{"status":{"lifecycle":"Stopped","admissionState":"Closed"}}'
pool_lifecycle_reached test-pool Draining
pool_lifecycle_reached test-pool Quiescent
pool_lifecycle_reached test-pool Stopping
pool_lifecycle_reached test-pool Stopped

pool_payload='{"status":{"lifecycle":"Draining","admissionState":"Draining"}}'
if pool_lifecycle_reached test-pool Quiescent; then
  echo "monotonic lifecycle accepted a state before Quiescent" >&2
  exit 1
fi

pool_payload='{"status":{"lifecycle":"Stopping","admissionState":"Draining"}}'
if pool_lifecycle_reached test-pool Stopping; then
  echo "monotonic lifecycle accepted an invalid Stopping admission state" >&2
  exit 1
fi

printf '%s\n' 'ok - scale-to-zero lifecycle assertions accept valid later states without accepting regressions'


park_count="$(grep -Fc 'park_runtimepool "${codex_pool}"' "${script}")"
[[ "${park_count}" == "2" ]] || {
  echo "Codex read RuntimePool must be parked before both tool checks and write publication" >&2
  exit 1
}
scale_zero_line="$(grep -nF 'run_scale_to_zero_recovery_check codex "${codex_model}"' "${script}" | tail -1 | cut -d: -f1)"
park_before_write_line="$(grep -nF 'park_runtimepool "${codex_pool}"' "${script}" | tail -1 | cut -d: -f1)"
write_gate_line="$(grep -nF 'run_write_release_gate "${codex_agent}"' "${script}" | tail -1 | cut -d: -f1)"
(( scale_zero_line < park_before_write_line ))
(( park_before_write_line < write_gate_line ))

printf '%s\n' 'ok - release gate parks the recovered read pool before creating the write profile pool'


terminal_projection_body="$(awk '/^task_terminal_projection_ready\(\) \{/,/^\}$/' "${script}")"
[[ -n "${terminal_projection_body}" ]] || {
  echo "atomic Task terminal projection predicate is missing" >&2
  exit 1
}
eval "${terminal_projection_body}"
namespace="test-namespace"
task_payload=''
k() { printf '%s\n' "${task_payload}"; }

task_payload='{"status":{"execution":{"state":"Succeeded"},"phase":"Pending"}}'
if task_terminal_projection_ready test-task; then
  echo "terminal predicate accepted execution before phase projection" >&2
  exit 1
fi

task_payload='{"status":{"execution":{"state":"Succeeded"},"phase":"Succeeded"}}'
task_terminal_projection_ready test-task

task_payload='{"status":{"execution":{"state":"Failed"},"phase":"Failed"}}'
task_terminal_projection_ready test-task

task_payload='{"status":{"execution":{"state":"Running"},"phase":"Succeeded"}}'
if task_terminal_projection_ready test-task; then
  echo "terminal predicate accepted phase before terminal execution" >&2
  exit 1
fi

printf '%s\n' 'ok - Task terminal waits require execution and phase projection from one observation'
