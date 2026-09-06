#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIND_CLUSTER="${KIND_CLUSTER:-orka-agent-substrate-e2e}"
ORKA_NAMESPACE="${ORKA_NAMESPACE:-orka-system}"
ACP_RUNTIME_NAMESPACE="${ORKA_ACP_RUNTIME_NAMESPACE:-orka-runtimes}"
KIND_REGISTRY_NAME="${KIND_REGISTRY_NAME:-kind-registry}"
KIND_REGISTRY_PORT="${KIND_REGISTRY_PORT:-5001}"
SUBSTRATE_REPO="${SUBSTRATE_REPO:-https://github.com/agent-substrate/substrate.git}"
SUBSTRATE_REF="${SUBSTRATE_REF:-b80031d260959b1fc5c6f61e3099fe2a6d368af1}"
# Git blob IDs for the reviewed source files at the default pin. Every local
# evaluation patch verifies these immutable upstream objects before applying.
SUBSTRATE_ATELET_OCI_BLOB="a2ae14c0a264d8ff2fdc9527f5894901d913c0a4"
SUBSTRATE_ATENET_EXTPROC_IN_BLOB="317511845fef40b7602861383f7664e915215a69"
SUBSTRATE_ATENET_EXTPROC_IN_TEST_BLOB="09bb9a4c4e7d4f5c8185c41535ebcc40fc8ff57b"
SUBSTRATE_ATENET_ENVOY_RUNNER_BLOB="8d38be29f09a7ce23886b71a051586354c8413e5"
SUBSTRATE_ATENET_MANIFEST_BLOB="e309cad0a2e8435d1ed8dfd51ce347ab4f5a7521"
SUBSTRATE_ATENET_XDS_BLOB="20ce920de816c885e4614c4f723e75a6c2b74d8d"
SUBSTRATE_ATENET_XDS_TEST_BLOB="189914c245d13eeec19293d08258fcd8c27676e7"
SUBSTRATE_ATEOM_GVISOR_BLOB="7d79dd0a26709599223ed848d1b8f1ea19641cf6"
SUBSTRATE_ATEOM_RUNSC_BLOB="6db499a549f2b6987a867b144e8d6b3828cad9ff"
SUBSTRATE_ATELET_CAPABILITY_PATCH="${ROOT_DIR}/hack/agent-substrate/atelet-root-supervisor-capabilities.patch"
SUBSTRATE_ATENET_REDACTION_PATCH="${ROOT_DIR}/hack/agent-substrate/atenet-router-authorization-redaction.patch"
SUBSTRATE_ATEOM_DELETE_RECOVERY_PATCH="${ROOT_DIR}/hack/agent-substrate/ateom-runsc-delete-recovery.patch"
IMAGE_TAG="${IMAGE_TAG:-agent-substrate-ci}"
# The workspace-backed ACP Task smoke builds the real Codex runtime image and
# proves Substrate-backed RuntimePools live: derived ActorTemplate rendering,
# actor boot under gVisor, and the authenticated Serving probe through the
# router, then completes a real prompt through the authenticated provider
# proxy and the local Responses-compatible fixture. Set to 0 to skip the
# runtime image build and smoke.
SUBSTRATE_E2E_ACP_TASK_SMOKE="${SUBSTRATE_E2E_ACP_TASK_SMOKE:-1}"
# Class-backed data-only suspend/cold-resume conformance (issue #425). Runs a
# session-scoped classRef Task through suspension and continuation against the
# local Responses fixture; requires the workspace provider API.
# The pinned Substrate release supports only snapshotsConfig.location: the
# ActorTemplate CRD prunes the onPause/onCommit/onResume policy fields and
# SuspendActor takes no snapshot scope, so the data-only suspension contract
# (ADR 0027) cannot be expressed against this provider version and the
# controller fails suspend-capable pools closed. Enable this gate only when the
# Substrate pin provides both the per-template snapshot-scope policy API and a
# control protocol that atomically binds data-only resume to the verified actor
# UID/version and immutable Data snapshot UID/version.
SUBSTRATE_E2E_SUSPEND_RESUME="${SUBSTRATE_E2E_SUSPEND_RESUME:-0}"
SUBSTRATE_E2E_LIFECYCLE="${SUBSTRATE_E2E_LIFECYCLE:-1}"
LIFECYCLE_AMBIGUITY_MARKER="ORKA_E2E_WS_LC_AMBIGUOUS_OK"
FIXTURE_LOCAL_PORT="${FIXTURE_LOCAL_PORT:-18337}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TASK_TIMEOUT_SECONDS="${TASK_TIMEOUT_SECONDS:-900}"
SUBSTRATE_E2E_EXTENDED="${SUBSTRATE_E2E_EXTENDED:-0}"
MCP_TOOL_EXEC_ATTEMPTS="${MCP_TOOL_EXEC_ATTEMPTS:-3}"
MCP_TOOL_EXEC_RETRY_DELAY_SECONDS="${MCP_TOOL_EXEC_RETRY_DELAY_SECONDS:-15}"
SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_NAME="${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_NAME:-orka-substrate-bootstrap}"
SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_KEY="${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_KEY:-token}"
if [[ "${SUBSTRATE_BOOTSTRAP_TOKEN+x}" != "x" || -z "${SUBSTRATE_BOOTSTRAP_TOKEN}" ]]; then
  printf -v SUBSTRATE_BOOTSTRAP_TOKEN 'bootstrap-ci-%s-%s' "$(date +%s%N)" "${RANDOM}"
fi

SUBSTRATE_DIR=""
TMP_ROOT=""
DOCKER_CONFIG_DIR=""
PORT_FORWARD_PID=""
ORKA_API_PORT_FORWARD_PID=""
FIXTURE_PORT_FORWARD_PID=""
ORKA_API_LOCAL_PORT="${ORKA_API_LOCAL_PORT:-18084}"
ORKA_API_CLIENT_SERVICE_ACCOUNT="${ORKA_API_CLIENT_SERVICE_ACCOUNT:-orka-client}"
RUNSC_DELETE_INJECTION_NODE=""
RUNSC_DELETE_INJECTION_PATH=""

log() {
  printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

# Shared redaction; the bootstrap token literal is substituted at call time.
# shellcheck source=scripts/lib/redact.sh
. "${ROOT_DIR}/scripts/lib/redact.sh"
# shellcheck source=scripts/lib/e2e-admission-tls.sh
. "${ROOT_DIR}/scripts/lib/e2e-admission-tls.sh"
ORKA_REDACT_SECRET_VARS=(SUBSTRATE_BOOTSTRAP_TOKEN)

run_redacted() {
  set +e
  "$@" 2>&1 | redact
  local rc=${PIPESTATUS[0]}
  set -e
  return "${rc}"
}

stop_port_forward() {
  local pid="$1"
  [[ -n "${pid}" ]] || return 0
  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
  fi
  wait "${pid}" 2>/dev/null || true
}

kubectl_ate() {
  "${TMP_ROOT}/kubectl-ate" --context "kind-${KIND_CLUSTER}" "$@"
}

restore_runsc_delete_injector() {
  local node="${RUNSC_DELETE_INJECTION_NODE:-}"
  local path="${RUNSC_DELETE_INJECTION_PATH:-}"
  if [[ -z "${node}" || -z "${path}" ]]; then
    return 0
  fi

  if ! docker exec "${node}" /bin/sh -ceu '
    path="$1"
    if [ -e "${path}.orka-real" ]; then
      rm -f "${path}"
      mv "${path}.orka-real" "${path}"
    fi
    rm -f "${path}.orka-delete-failure-observed"
  ' sh "${path}"; then
    return 1
  fi
  RUNSC_DELETE_INJECTION_NODE=""
  RUNSC_DELETE_INJECTION_PATH=""
}

dump_diagnostics() {
  local rc=$?
  if [[ "${rc}" -eq 0 ]]; then
    return 0
  fi

  log "Failure diagnostics"
  run_redacted kubectl get pods -A -o wide || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get deployment,pods,agents,tasks,jobs -o wide || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get events --sort-by=.metadata.creationTimestamp || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get tasks -o yaml || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" logs deployment/orka-controller-manager --all-containers --tail=-1 || true

  for job in $(kubectl -n "${ORKA_NAMESPACE}" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true); do
    log "Logs for job/${job}"
    run_redacted kubectl -n "${ORKA_NAMESPACE}" logs "job/${job}" --all-containers --tail=-1 || true
  done
  run_redacted kubectl get executionworkspaceproviders,runtimeproviderconfigs -o yaml || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get executionworkspaceclasses,executionworkspaces,runtimeworkspaceprofiles,runtimepools -o yaml || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get substrateactorpools,tools,leases -o wide || true
  run_redacted kubectl -n "${ORKA_NAMESPACE}" get substrateactorpools,tools,leases -o yaml || true

  run_redacted kubectl -n ate-system get pods,svc,deploy,daemonset,statefulset -o wide || true
  run_redacted kubectl -n ate-system logs deployment/ate-api-server-deployment --all-containers --tail=400 || true
  run_redacted kubectl -n ate-system logs deployment/ate-controller --all-containers --tail=400 || true
  run_redacted kubectl -n ate-system logs deployment/atenet-router --all-containers --tail=400 || true
  run_redacted kubectl -n ate-system logs daemonset/atelet --all-containers --tail=400 || true

  if [[ -x "${TMP_ROOT}/kubectl-ate" ]]; then
    run_redacted kubectl_ate get actors -o table || true
    run_redacted kubectl_ate get workers -o table || true
  fi

  return "${rc}"
}

cleanup() {
  stop_port_forward "${PORT_FORWARD_PID}"
  stop_port_forward "${FIXTURE_PORT_FORWARD_PID}"
  stop_port_forward "${ORKA_API_PORT_FORWARD_PID}"
  restore_runsc_delete_injector >/dev/null 2>&1 || true
  if [[ "${KEEP_CLUSTER}" != "1" ]]; then
    kind delete cluster --name "${KIND_CLUSTER}" >/dev/null 2>&1 || true
  else
    log "KEEP_CLUSTER=1, leaving kind cluster ${KIND_CLUSTER}"
  fi
  if [[ -n "${DOCKER_CONFIG_DIR}" ]]; then
    rm -rf "${DOCKER_CONFIG_DIR}"
  fi
  if [[ -n "${TMP_ROOT}" && "${KEEP_CLUSTER}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
}

trap dump_diagnostics ERR
trap cleanup EXIT

require_command() {
  local command="$1"
  command -v "${command}" >/dev/null 2>&1 || {
    echo "missing required command: ${command}" >&2
    exit 1
  }
}

wait_for_rollouts() {
  log "Waiting for Substrate control plane"
  kubectl -n ate-system rollout status deployment/ate-api-server-deployment --timeout=10m
  kubectl -n ate-system rollout status deployment/ate-controller --timeout=10m
  kubectl -n ate-system rollout status deployment/atenet-router --timeout=10m
  kubectl -n ate-system rollout status daemonset/atelet --timeout=10m
  kubectl -n ate-system rollout status statefulset/valkey-cluster --timeout=10m
  if kubectl -n ate-system get deployment/rustfs >/dev/null 2>&1; then
    kubectl -n ate-system rollout status deployment/rustfs --timeout=10m
  fi
}

ensure_snapshot_bucket() {
  log "Ensuring local Substrate snapshot bucket"
  kubectl -n ate-system delete pod/rustfs-bucket-init --ignore-not-found --wait=true >/dev/null
  kubectl -n ate-system run rustfs-bucket-init \
    --image=amazon/aws-cli:2.32.3 \
    --restart=Never \
    --env=AWS_ACCESS_KEY_ID=rustfsadmin \
    --env=AWS_SECRET_ACCESS_KEY=rustfsadmin \
    --env=AWS_DEFAULT_REGION=us-east-1 \
    --command -- /bin/sh -c \
    'aws --endpoint-url http://rustfs.ate-system.svc:9000 s3api head-bucket --bucket ate-snapshots >/dev/null 2>&1 || aws --endpoint-url http://rustfs.ate-system.svc:9000 s3api create-bucket --bucket ate-snapshots >/dev/null'
  kubectl -n ate-system wait --for=jsonpath='{.status.phase}'=Succeeded pod/rustfs-bucket-init --timeout=2m
  run_redacted kubectl -n ate-system logs pod/rustfs-bucket-init --tail=-1 || true
  kubectl -n ate-system delete pod/rustfs-bucket-init --ignore-not-found --wait=true >/dev/null
}

wait_jsonpath_equals() {
  local description="$1"
  local command="$2"
  local expected="$3"
  local timeout_seconds="$4"
  local started now value
  started="$(date +%s)"

  while true; do
    set +e
    value="$(eval "${command}" 2>/dev/null)"
    local rc=$?
    set -e
    if [[ "${rc}" -eq 0 && "${value}" == "${expected}" ]]; then
      log "${description}: ${expected}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for ${description}; expected ${expected}, got ${value:-<empty>}" >&2
      return 1
    fi
    sleep 5
  done
}

start_orka_api_port_forward() {
  if [[ -n "${ORKA_API_PORT_FORWARD_PID}" ]] &&
    kill -0 "${ORKA_API_PORT_FORWARD_PID}" >/dev/null 2>&1; then
    return 0
  fi
  stop_port_forward "${ORKA_API_PORT_FORWARD_PID}"
  ORKA_API_PORT_FORWARD_PID=""
  kubectl -n "${ORKA_NAMESPACE}" port-forward svc/orka-api \
    "${ORKA_API_LOCAL_PORT}:8080" >"${TMP_ROOT}/orka-api-port-forward.log" 2>&1 &
  ORKA_API_PORT_FORWARD_PID="$!"

  local attempts_remaining=60
  while (( attempts_remaining > 0 )); do
    if curl -fsS --connect-timeout 5 --max-time 10 \
      "http://127.0.0.1:${ORKA_API_LOCAL_PORT}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${ORKA_API_PORT_FORWARD_PID}" >/dev/null 2>&1; then
      echo "Orka API port-forward exited before becoming ready" >&2
      return 1
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done
  echo "Orka API port-forward did not become ready" >&2
  return 1
}

ensure_orka_api_client_identity() {
  log "Creating scoped Orka API client identity ${ORKA_NAMESPACE}/${ORKA_API_CLIENT_SERVICE_ACCOUNT}"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${ORKA_API_CLIENT_SERVICE_ACCOUNT}
  namespace: ${ORKA_NAMESPACE}
automountServiceAccountToken: false
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${ORKA_API_CLIENT_SERVICE_ACCOUNT}
  namespace: ${ORKA_NAMESPACE}
rules:
  - apiGroups: ["core.orka.ai"]
    resources: ["tasks"]
    verbs: ["get"]
  - apiGroups: ["core.orka.ai"]
    resources: ["sessions"]
    verbs: ["get", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${ORKA_API_CLIENT_SERVICE_ACCOUNT}
  namespace: ${ORKA_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${ORKA_API_CLIENT_SERVICE_ACCOUNT}
subjects:
  - kind: ServiceAccount
    name: ${ORKA_API_CLIENT_SERVICE_ACCOUNT}
    namespace: ${ORKA_NAMESPACE}
YAML
}

assert_orka_task_result_contains() {
  local namespace_arg="$1"
  local task_name="$2"
  local expected_marker="$3"
  local api_token result_file status attempts_remaining

  start_orka_api_port_forward
  api_token="$(kubectl -n "${ORKA_NAMESPACE}" create token "${ORKA_API_CLIENT_SERVICE_ACCOUNT}")"
  result_file="${TMP_ROOT}/${task_name}-result.json"
  attempts_remaining=15
  while (( attempts_remaining > 0 )); do
    status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
      --header "Authorization: Bearer ${api_token}" \
      --output "${result_file}" --write-out '%{http_code}' \
      "http://127.0.0.1:${ORKA_API_LOCAL_PORT}/api/v1/tasks/${task_name}/result?namespace=${namespace_arg}" \
      2>>"${TMP_ROOT}/orka-api-port-forward.log" || true)"
    if [[ "${status}" == "200" ]] &&
      jq -er '.result' "${result_file}" | grep -Fq "${expected_marker}"; then
      log "Task/${task_name} result contains ${expected_marker}"
      return 0
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done

  echo "Task/${task_name} result did not contain the expected marker ${expected_marker} (last HTTP status: ${status:-none})" >&2
  return 1
}

restart_session_name_if_deletable() {
  jq -r '
    (.spec.sessionRef.name // "") as $name
    | .status as $status
    | if (($name | length) > 0) and (
        ($status.phase == "Succeeded"
          and $status.execution.state == "Succeeded"
          and $status.execution.outcome == "Succeeded")
        or
        ($status.phase == "Cancelled"
          and $status.execution.state == "Cancelled"
          and $status.execution.outcome == "Cancelled"
          and (($status.execution.reason == "Cancelled") or ($status.execution.reason == "TaskTimeout")))
      )
      then $name
      else empty
      end
  '
}

# delete_fixed_session removes a fixed-name Session from the PVC-backed API
# store and reads it back as 404. This keeps lifecycle reruns isolated and
# proves cleanup removed the durable record rather than only its RuntimePool.
delete_fixed_session() {
  local session_name="$1"
  local api_token delete_status get_status started now

  start_orka_api_port_forward
  api_token="$(kubectl -n "${ORKA_NAMESPACE}" create token "${ORKA_API_CLIENT_SERVICE_ACCOUNT}")"
  started="$(date +%s)"
  while true; do
    delete_status="$(curl --silent --connect-timeout 5 --max-time 30 -X DELETE \
      --header "Authorization: Bearer ${api_token}" \
      --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${ORKA_API_LOCAL_PORT}/api/v1/sessions/${session_name}?namespace=${ORKA_NAMESPACE}" \
      2>>"${TMP_ROOT}/orka-api-port-forward.log" || true)"
    case "${delete_status}" in
      200|202|204|404) break ;;
      409)
        now="$(date +%s)"
        if (( now - started >= 120 )); then
          echo "fixed Session ${session_name} remained conflicted 120s during cleanup" >&2
          return 1
        fi
        sleep 2
        ;;
      *)
        echo "failed to delete fixed Session ${session_name} during cleanup (HTTP ${delete_status:-none})" >&2
        return 1
        ;;
    esac
  done
  started="$(date +%s)"
  while true; do
    get_status="$(curl --silent --connect-timeout 5 --max-time 30 \
      --header "Authorization: Bearer ${api_token}" \
      --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${ORKA_API_LOCAL_PORT}/api/v1/sessions/${session_name}?namespace=${ORKA_NAMESPACE}" \
      2>>"${TMP_ROOT}/orka-api-port-forward.log" || true)"
    [[ "${get_status}" == "404" ]] && return 0
    if [[ "${get_status}" != "200" ]]; then
      echo "failed to verify fixed Session ${session_name} deletion (HTTP ${get_status:-none})" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started >= 60 )); then
      echo "fixed Session ${session_name} remained readable 60s after deletion" >&2
      return 1
    fi
    sleep 2
  done
}

# record_lc_pool persists a lifecycle pool name so a rerun can remove pools
# whose Tasks were deleted before an interrupted cleanup completed.
record_lc_pool() {
  local pool="$1"
  [[ -n "${pool}" ]] || return 0
  local existing merged
  existing="$(kubectl -n "${ORKA_NAMESPACE}" get configmap orka-ws-lc-pools \
    -o jsonpath='{.data.pools}' 2>/dev/null || true)"
  case " ${existing} " in
    *" ${pool} "*) return 0 ;;
  esac
  merged="${existing:+${existing} }${pool}"
  kubectl -n "${ORKA_NAMESPACE}" create configmap orka-ws-lc-pools \
    --from-literal=pools="${merged}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

wait_jsonpath_int_at_least() {
  local description="$1"
  local command="$2"
  local minimum="$3"
  local timeout_seconds="$4"
  local started now value
  started="$(date +%s)"

  while true; do
    set +e
    value="$(eval "${command}" 2>/dev/null)"
    local rc=$?
    set -e
    if [[ "${rc}" -eq 0 && "${value}" =~ ^[0-9]+$ && "${value}" -ge "${minimum}" ]]; then
      log "${description}: ${value}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for ${description}; expected >= ${minimum}, got ${value:-<empty>}" >&2
      return 1
    fi
    sleep 5
  done
}

wait_actor_status() {
  local actor_name="$1"
  local expected="$2"
  local timeout_seconds="$3"
  local started now status
  started="$(date +%s)"

  while true; do
    status="$(kubectl_ate get actor "${actor_name}" -o json 2>/dev/null | jq -r '.actors[0].status // empty')"
    if [[ "${status}" == "${expected}" ]]; then
      log "actor/${actor_name}: ${expected}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for actor/${actor_name}; expected ${expected}, got ${status:-<empty>}" >&2
      return 1
    fi
    sleep 5
  done
}

wait_actor_absent() {
  local actor_name="$1"
  local timeout_seconds="$2"
  local started now output count rc observation
  started="$(date +%s)"
  observation="not checked"

  while true; do
    # Actor absence is reported as a non-zero NotFound. Clear the inherited
    # ERR trap inside this command-substitution subshell so the expected result
    # does not launch the full failure diagnostics before we can inspect it.
    if output="$(trap - ERR; kubectl_ate get actor "${actor_name}" -o json 2>&1)"; then
      rc=0
    else
      rc=$?
    fi
    if [[ "${rc}" -ne 0 ]] && grep -Fq -- "code = NotFound desc = Actor ${actor_name} not found" <<<"${output}"; then
      log "actor/${actor_name}: absent"
      return 0
    fi
    if [[ "${rc}" -eq 0 ]]; then
      if count="$(jq -er '
        if type != "object" then
          error("actor response is not an object")
        elif has("actors") then
          if (.actors | type) == "array" then (.actors | length) else error("actors is not an array") end
        elif length == 0 then
          0
        else
          error("missing actors array")
        end
      ' <<<"${output}" 2>/dev/null)"; then
        if [[ "${count}" == "0" ]]; then
          log "actor/${actor_name}: absent"
          return 0
        fi
        observation="actor query succeeded with ${count} result(s)"
      else
        observation="actor query succeeded with an invalid response"
      fi
    else
      observation="kubectl-ate failed with exit ${rc} without an actor NotFound response"
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for actor/${actor_name} to be absent; ${observation}" >&2
      return 1
    fi
    sleep 5
  done
}

sha256_hex() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

substrate_actor_pool_prefix() {
  local namespace="$1"
  local name="$2"
  local hash
  hash="$(printf '%s\0%s' "${namespace}" "${name}" | sha256_hex)"
  printf 'orka-p-%s' "${hash:0:24}"
}

wait_worker_absent() {
  local worker_name="$1"
  local timeout_seconds="$2"
  local started now count
  started="$(date +%s)"

  while true; do
    count="$(kubectl_ate get workers -o json 2>/dev/null | jq --arg worker "${worker_name}" '[.workers[]? | select(.workerPod == $worker)] | length' 2>/dev/null || true)"
    if [[ "${count}" == "0" ]]; then
      log "worker/${worker_name}: absent from Substrate store"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for worker/${worker_name} to leave the Substrate store" >&2
      return 1
    fi
    sleep 2
  done
}

wait_worker_count_at_least() {
  local minimum="$1"
  local timeout_seconds="$2"
  local started now count
  started="$(date +%s)"

  while true; do
    count="$(kubectl_ate get workers -o json 2>/dev/null | jq '[.workers[]? | select(.workerPool == "orka-workers")] | length' 2>/dev/null || true)"
    if [[ "${count}" =~ ^[0-9]+$ && "${count}" -ge "${minimum}" ]]; then
      log "Substrate worker count: ${count}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for at least ${minimum} registered Substrate workers; got ${count:-<empty>}" >&2
      return 1
    fi
    sleep 2
  done
}

assert_no_suspending_actors() {
  local count
  count="$(kubectl_ate get actors -o json | jq '[.actors[]? | select(.status == "STATUS_SUSPENDING")] | length')"
  if [[ "${count}" != "0" ]]; then
    echo "found ${count} Actor(s) stuck in STATUS_SUSPENDING" >&2
    return 1
  fi
  log "No Actors are stuck in STATUS_SUSPENDING"
}

wait_resource_absent() {
  local namespace="$1"
  local resource="$2"
  local name="$3"
  local timeout_seconds="$4"
  local started now output rc observation
  started="$(date +%s)"
  observation="not checked"

  while true; do
    # Kubernetes absence is also a non-zero NotFound. Handle it here without
    # invoking the script-wide ERR diagnostics from the subshell.
    if output="$(trap - ERR; kubectl -n "${namespace}" get "${resource}" "${name}" 2>&1)"; then
      observation="resource still exists"
    else
      rc=$?
      if grep -Eiq '\(NotFound\)| not found' <<<"${output}"; then
        log "${resource}/${name}: absent"
        return 0
      fi
      observation="kubectl failed with exit ${rc} without a NotFound response"
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for ${resource}/${name} in namespace ${namespace} to be absent; ${observation}" >&2
      return 1
    fi
    sleep 5
  done
}

wait_job_succeeded() {
  local job_name="$1"
  local timeout_seconds="$2"
  local started now succeeded failed
  started="$(date +%s)"

  while true; do
    succeeded="$(kubectl -n "${ORKA_NAMESPACE}" get "job/${job_name}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    failed="$(kubectl -n "${ORKA_NAMESPACE}" get "job/${job_name}" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
    if [[ "${succeeded}" =~ ^[1-9][0-9]*$ ]]; then
      log "job/${job_name}: Complete"
      return 0
    fi
    if [[ "${failed}" =~ ^[1-9][0-9]*$ ]]; then
      echo "job/${job_name} failed" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for job/${job_name} to complete" >&2
      return 1
    fi
    sleep 5
  done
}

patch_substrate_kind_registry_script() {
  local script="${SUBSTRATE_DIR}/hack/create-kind-cluster.sh"
  sed -i.bak \
    -e 's|reg_name="kind-registry"|reg_name="${KIND_REGISTRY_NAME:-kind-registry}"|' \
    -e 's|reg_port="5001"|reg_port="${KIND_REGISTRY_PORT:-5001}"|' \
    "${script}"
  rm -f "${script}.bak"
  if ! grep -q "KIND_REGISTRY_PORT" "${script}"; then
    echo "failed to patch Substrate kind registry script for registry override" >&2
    exit 1
  fi
}

verify_substrate_source_blob() {
  local target="$1"
  local expected_blob="$2"
  local patch_context="$3"
  local actual_blob

  if ! actual_blob="$(git -C "${SUBSTRATE_DIR}" rev-parse "HEAD:${target}" 2>/dev/null)"; then
    echo "Substrate ref ${SUBSTRATE_REF} does not contain expected source ${target}" >&2
    exit 1
  fi
  if [[ "${actual_blob}" != "${expected_blob}" ]]; then
    echo "Substrate ref ${SUBSTRATE_REF} has unreviewed ${patch_context} context in ${target}" >&2
    echo "expected blob ${expected_blob}, got ${actual_blob}; review the provider contract before updating the patch" >&2
    exit 1
  fi
}

verify_patch_paths() {
  local patch_file="$1"
  local expected_paths="$2"
  local patch_paths

  patch_paths="$(git -C "${SUBSTRATE_DIR}" apply --numstat "${patch_file}" | cut -f3- | LC_ALL=C sort)"
  if [[ "${patch_paths}" != "${expected_paths}" ]]; then
    echo "reviewed patch ${patch_file} changes unexpected files: ${patch_paths:-<none>}" >&2
    exit 1
  fi
}

apply_reviewed_substrate_patch() {
  local label="$1"
  local patch_file="$2"
  local expected_paths="$3"
  local -a paths=()
  local path

  if [[ ! -f "${patch_file}" ]]; then
    echo "missing reviewed Substrate patch: ${patch_file}" >&2
    exit 1
  fi
  verify_patch_paths "${patch_file}" "${expected_paths}"
  if ! git -C "${SUBSTRATE_DIR}" apply --check --whitespace=error-all "${patch_file}"; then
    echo "reviewed Substrate ${label} patch no longer applies cleanly" >&2
    exit 1
  fi

  git -C "${SUBSTRATE_DIR}" apply --whitespace=error-all "${patch_file}"

  if ! git -C "${SUBSTRATE_DIR}" apply --reverse --check "${patch_file}"; then
    echo "failed to verify the applied Substrate ${label} patch" >&2
    exit 1
  fi
  while IFS= read -r path; do
    [[ -n "${path}" ]] && paths+=("${path}")
  done <<< "${expected_paths}"
  if ! git -C "${SUBSTRATE_DIR}" diff --check -- "${paths[@]}"; then
    echo "applied Substrate ${label} patch introduced an invalid diff" >&2
    exit 1
  fi
}

apply_substrate_workspace_agent_capability_patch() {
  local target="cmd/servers/atelet/oci.go"
  local changed_files checkout_status

  if [[ ! -f "${SUBSTRATE_ATELET_CAPABILITY_PATCH}" ]]; then
    echo "missing reviewed Substrate compatibility patch: ${SUBSTRATE_ATELET_CAPABILITY_PATCH}" >&2
    exit 1
  fi
  checkout_status="$(git -C "${SUBSTRATE_DIR}" status --porcelain --untracked-files=all)"
  if [[ -n "${checkout_status}" ]]; then
    echo "refusing to patch a dirty Substrate checkout" >&2
    exit 1
  fi
  verify_substrate_source_blob "${target}" "${SUBSTRATE_ATELET_OCI_BLOB}" "OCI capability"
  if ! git -C "${SUBSTRATE_DIR}" apply --check --whitespace=error-all "${SUBSTRATE_ATELET_CAPABILITY_PATCH}"; then
    echo "reviewed Substrate workspace-agent capability patch no longer applies cleanly" >&2
    exit 1
  fi

  git -C "${SUBSTRATE_DIR}" apply --whitespace=error-all "${SUBSTRATE_ATELET_CAPABILITY_PATCH}"

  if ! git -C "${SUBSTRATE_DIR}" apply --reverse --check "${SUBSTRATE_ATELET_CAPABILITY_PATCH}"; then
    echo "failed to verify the applied Substrate workspace-agent capability patch" >&2
    exit 1
  fi
  if ! git -C "${SUBSTRATE_DIR}" diff --check -- "${target}"; then
    echo "applied Substrate workspace-agent capability patch introduced an invalid diff" >&2
    exit 1
  fi
  changed_files="$(git -C "${SUBSTRATE_DIR}" diff --name-only)"
  if [[ "${changed_files}" != "${target}" ]]; then
    echo "Substrate capability patch changed unexpected files: ${changed_files:-<none>}" >&2
    exit 1
  fi
  # CAP_SETGID/CAP_SETUID are scoped to exactly the two root supervisors
  # (workspace agent and ACP runtime); CAP_CHOWN only to the ACP runtime,
  # which assigns session trees to per-session identities.
  for capability in CAP_SETGID CAP_SETUID; do
    if [[ "$(grep -Fc "\"${capability}\"" "${SUBSTRATE_DIR}/${target}" || true)" -ne 2 ]]; then
      echo "Substrate capability patch did not scope ${capability} to the two root supervisors" >&2
      exit 1
    fi
  done
  if [[ "$(grep -Fc '"CAP_CHOWN"' "${SUBSTRATE_DIR}/${target}" || true)" -ne 1 ]]; then
    echo "Substrate capability patch did not scope CAP_CHOWN to the ACP runtime supervisor" >&2
    exit 1
  fi
  if [[ "$(grep -Fc '"/usr/local/bin/orka-acp-runtime"' "${SUBSTRATE_DIR}/${target}" || true)" -ne 2 ]]; then
    echo "Substrate capability patch did not scope the ACP runtime grants to its exact entrypoint" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'os.Chmod(rootPath, 0o755)' "${SUBSTRATE_DIR}/${target}" || true)" -ne 1 ]]; then
    echo "Substrate capability patch did not make the supervisor rootfs traversable after credential drop" >&2
    exit 1
  fi

  log "Applied reviewed Substrate root-supervisor capability compatibility patch"
}

apply_substrate_atenet_authorization_redaction_patch() {
  local source="cmd/servers/atenet/app/router/extproc_in.go"
  local source_test="cmd/servers/atenet/app/router/extproc_in_test.go"
  local envoy_runner="cmd/servers/atenet/app/router/envoyrunner.go"
  local install_manifest="manifests/ate-install/atenet-router.yaml"
  local xds_source="cmd/servers/atenet/app/router/xds.go"
  local xds_test="cmd/servers/atenet/app/router/xds_test.go"
  local expected_paths
  expected_paths="$(printf '%s\n' \
    "${envoy_runner}" "${source}" "${source_test}" "${install_manifest}" \
    "${xds_source}" "${xds_test}" | LC_ALL=C sort)"

  verify_substrate_source_blob "${source}" "${SUBSTRATE_ATENET_EXTPROC_IN_BLOB}" "atenet request-metadata"
  verify_substrate_source_blob "${source_test}" "${SUBSTRATE_ATENET_EXTPROC_IN_TEST_BLOB}" "atenet request-metadata test"
  verify_substrate_source_blob "${envoy_runner}" "${SUBSTRATE_ATENET_ENVOY_RUNNER_BLOB}" "atenet Envoy runner logging"
  verify_substrate_source_blob "${install_manifest}" "${SUBSTRATE_ATENET_MANIFEST_BLOB}" "atenet install manifest logging"
  verify_substrate_source_blob "${xds_source}" "${SUBSTRATE_ATENET_XDS_BLOB}" "atenet xDS routes"
  verify_substrate_source_blob "${xds_test}" "${SUBSTRATE_ATENET_XDS_TEST_BLOB}" "atenet xDS route test"
  apply_reviewed_substrate_patch "atenet router hardening" "${SUBSTRATE_ATENET_REDACTION_PATCH}" "${expected_paths}"

  if [[ "$(grep -Fc 'case ":method", ":path", ":authority", "host", "x-request-id":' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not install the reviewed request-metadata allowlist" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'if !isSafeRequestMetadataHeader(k) {' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch does not reject headers before retaining their values" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'url.ParseRequestURI(value)' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not sanitize the request target before logging it" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'if requestURI.Opaque != "" {' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ||
        "$(grep -Fc 'requestURI, err = url.Parse(value)' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not preserve absolute-form paths while discarding authority credentials" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func sanitizeRequestAuthority(value string) string {' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ||
        "$(grep -Fc 'authority.User != nil' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not reject credential-bearing authority values before logging" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func digestRequestID(value string) string {' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ||
        "$(grep -Fc 'val = digestRequestID(val)' "${SUBSTRATE_DIR}/${source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not replace raw request IDs with audit digests" >&2
    exit 1
  fi
  if grep -Eq 'redactedHeaderValue|sanitizeRequestHeaderValue|case "authorization"' "${SUBSTRATE_DIR}/${source}"; then
    echo "Substrate atenet patch retained denylist-based request logging" >&2
    exit 1
  fi
  for target in "${envoy_runner}" "${install_manifest}"; do
    if grep -Fq 'upstream:debug,router:debug,ext_proc:debug' "${SUBSTRATE_DIR}/${target}"; then
      echo "Substrate atenet patch retained credential-bearing Envoy debug logging in ${target}" >&2
      exit 1
    fi
    grep -Fq 'upstream:info,router:info,ext_proc:info' "${SUBSTRATE_DIR}/${target}" || {
      echo "Substrate atenet patch did not install bounded Envoy component logging in ${target}" >&2
      exit 1
    }
  done
  if [[ "$(grep -Fc 'Timeout: durationpb.New(0), // Disable route timeout for streaming responses.' "${SUBSTRATE_DIR}/${xds_source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not disable the fixed route timeout for streaming responses" >&2
    exit 1
  fi
  if grep -Fq 'Timeout: durationpb.New(10 * time.Second),' "${SUBSTRATE_DIR}/${xds_source}"; then
    echo "Substrate atenet patch retained the fixed 10-second streaming route timeout" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'Expected streaming route timeout to be disabled' "${SUBSTRATE_DIR}/${xds_test}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not test the disabled streaming route timeout" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'headerFreeAccessLog  = ' "${SUBSTRATE_DIR}/${xds_source}" || true)" -ne 1 ||
        "$(grep -Fc 'AccessLogFormat: &streamaccesslogv3.StdoutAccessLog_LogFormat{' "${SUBSTRATE_DIR}/${xds_source}" || true)" -ne 1 ]]; then
    echo "Substrate atenet patch did not install an explicit header-free Envoy access log" >&2
    exit 1
  fi
  if grep -Eq '%(REQ|REQ_WITHOUT_QUERY|RESP|TRAILER)\(' "${SUBSTRATE_DIR}/${xds_source}"; then
    echo "Substrate atenet patch retained an Envoy request, response, or trailer header formatter" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'assertHeaderFreeAccessLogs(t, listenersMap)' "${SUBSTRATE_DIR}/${xds_test}" || true)" -ne 2 ]]; then
    echo "Substrate atenet patch did not test every generated HTTP connection manager access log" >&2
    exit 1
  fi

  log "Applied reviewed Substrate atenet router-hardening patch"
}

apply_substrate_ateom_delete_recovery_patch() {
  local service_source="cmd/servers/ateom-gvisor/ateom-gvisor.go"
  local runsc_source="cmd/servers/ateom-gvisor/runsc.go"
  local runsc_test="cmd/servers/ateom-gvisor/runsc_test.go"
  local expected_paths
  expected_paths="$(printf '%s\n' "${service_source}" "${runsc_source}" "${runsc_test}" | LC_ALL=C sort)"

  verify_substrate_source_blob "${service_source}" "${SUBSTRATE_ATEOM_GVISOR_BLOB}" "ateom checkpoint"
  verify_substrate_source_blob "${runsc_source}" "${SUBSTRATE_ATEOM_RUNSC_BLOB}" "runsc delete"
  if git -C "${SUBSTRATE_DIR}" cat-file -e "HEAD:${runsc_test}" 2>/dev/null; then
    echo "Substrate ref ${SUBSTRATE_REF} unexpectedly contains ${runsc_test}; review the local recovery patch" >&2
    exit 1
  fi
  apply_reviewed_substrate_patch "ateom runsc-delete recovery" "${SUBSTRATE_ATEOM_DELETE_RECOVERY_PATCH}" "${expected_paths}"

  if [[ "$(grep -Fc 'runscDeleteAttempts   = 4' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not install bounded runsc delete retries" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'failed closed while verifying container absence' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not retain the fail-closed absence postcondition" >&2
    exit 1
  fi
  local prepare_line checkpoint_line validate_line commit_line restore_prepare_line restore_network_move_line restore_line delete_line
  prepare_line="$(grep -n 'prepareCheckpointRecovery(checkpointPath, recoveryPath, expectedContainers)' "${SUBSTRATE_DIR}/${service_source}" | cut -d: -f1)"
  checkpoint_line="$(grep -n 'rcmd.cmdCheckpoint(ctx, "pause", checkpointPath)' "${SUBSTRATE_DIR}/${service_source}" | cut -d: -f1)"
  validate_line="$(grep -n 'rcmd.cmdValidateCheckpoint(ctx, recoveryPath)' "${SUBSTRATE_DIR}/${service_source}" | tail -n1 | cut -d: -f1)"
  commit_line="$(grep -n 'commitCheckpointRecovery(recoveryPath, expectedContainers)' "${SUBSTRATE_DIR}/${service_source}" | tail -n1 | cut -d: -f1)"
  if [[ -z "${prepare_line}" || -z "${checkpoint_line}" || -z "${validate_line}" || -z "${commit_line}" ||
        "${prepare_line}" -ge "${checkpoint_line}" || "${checkpoint_line}" -ge "${validate_line}" ||
        "${validate_line}" -ge "${commit_line}" ]]; then
    echo "Substrate ateom patch did not enforce prepared -> checkpointed -> validated -> committed recovery ordering" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'checkpointRecoveryCommitName' "${SUBSTRATE_DIR}/${service_source}" || true)" -lt 3 ]]; then
    echo "Substrate ateom patch did not install an explicit checkpoint commit record" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'checkpointRecoveryArtifact' "${SUBSTRATE_DIR}/${service_source}" || true)" -lt 4 ]]; then
    echo "Substrate ateom patch did not inventory checkpoint artifacts" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func materializeCheckpointTransport(checkpointPath, recoveryPath string) error {' "${SUBSTRATE_DIR}/${service_source}" || true)" -ne 1 ||
        "$(grep -Fc 'compatibilityArtifacts := map[string]string{' "${SUBSTRATE_DIR}/${service_source}" || true)" -ne 1 ||
        "$(grep -Fc 'for artifact, marker := range compatibilityArtifacts {' "${SUBSTRATE_DIR}/${service_source}" || true)" -ne 1 ||
        "$(grep -Fc 'materializeCheckpointTransport(checkpointPath, recoveryPath)' "${SUBSTRATE_DIR}/${service_source}" || true)" -lt 2 ]]; then
    echo "Substrate ateom patch did not preserve the legacy three-object checkpoint transport view" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func prepareCheckpointRestore(checkpointDir string) error {' "${SUBSTRATE_DIR}/${service_source}" || true)" -ne 1 ||
        "$(grep -Fc 'checkpointPagesCompatibilityMarker' "${SUBSTRATE_DIR}/${service_source}" || true)" -lt 3 ||
        "$(grep -Fc 'checkpointPagesMetadataCompatibilityMarker' "${SUBSTRATE_DIR}/${service_source}" || true)" -lt 3 ]]; then
    echo "Substrate ateom patch did not identify marked transport placeholders before compressed restore" >&2
    exit 1
  fi
  restore_prepare_line="$(grep -n 'prepareCheckpointRestore(checkpointDir)' "${SUBSTRATE_DIR}/${service_source}" | cut -d: -f1)"
  restore_network_move_line="$(grep -n 'netlink.LinkSetNsFd(eth0Link, int(s.interiorNetNS))' "${SUBSTRATE_DIR}/${service_source}" | tail -n1 | cut -d: -f1)"
  if [[ -z "${restore_prepare_line}" || -z "${restore_network_move_line}" ||
        "${restore_prepare_line}" -ge "${restore_network_move_line}" ]]; then
    echo "Substrate ateom patch did not validate restore artifacts before moving worker networking" >&2
    exit 1
  fi
  if [[ "$(grep -Fc '"-compression=flate-best-speed"' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not pin the validated single-file checkpoint format" >&2
    exit 1
  fi
  if grep -Fq '"-leave-running"' "${SUBSTRATE_DIR}/${runsc_source}"; then
    echo "Substrate ateom patch resumed the sandbox before checkpoint commit" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func (r *runsc) cmdValidateCheckpoint' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not validate the stopped runsc statefile" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'func (r *runsc) containerNamesLocked' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ||
        "$(grep -Fc 'return r.containerNamesLocked(ctx)' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not guard direct runsc list children from the PID 1 reaper" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'filepath.Dir(recoveryPath),' "${SUBSTRATE_DIR}/${service_source}" || true)" -ne 1 ]]; then
    echo "Substrate ateom patch did not stage commit temporaries outside the inventoried recovery directory" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'json.Unmarshal(stdout.Bytes(), &state)' "${SUBSTRATE_DIR}/${runsc_source}" || true)" -ne 1 ]] ||
     grep -Fq 'cmd.Stderr = &stdout' "${SUBSTRATE_DIR}/${runsc_source}"; then
    echo "Substrate ateom patch did not isolate runsc state JSON from stderr diagnostics" >&2
    exit 1
  fi
  if grep -Fq 'os.Rename(checkpointPath, recoveryPath)' "${SUBSTRATE_DIR}/${service_source}"; then
    echo "Substrate ateom patch retained the post-checkpoint rename crash window" >&2
    exit 1
  fi
  local recovery_test
  local recovery_tests=(
    TestContainerNamesWaitsForReaperReadLock
    TestContainerStatusIgnoresStderrDiagnostics
    TestCmdCheckpointUsesStoppedSingleFileProtocol
    TestCheckpointRecoveryRejectsUnexpectedOrCorruptArtifacts
    TestCheckpointRecoveryReconcilesPreparationBeforeCheckpoint
    TestCheckpointRecoveryReconcilesUncommittedSuccessfulCheckpoint
    TestCheckpointRecoveryReconcilesInterruptedWrite
    TestPrepareCheckpointRestoreRemovesMarkedCompatibilityFiles
    TestPrepareCheckpointRestoreRecoversPartialCompatibilityCleanup
    TestPrepareCheckpointRestorePreservesNativeMultiFileSnapshot
    TestPrepareCheckpointRestoreRejectsPartialNativeState
  )
  for recovery_test in "${recovery_tests[@]}"; do
    if [[ "$(grep -Fc "func ${recovery_test}" "${SUBSTRATE_DIR}/${runsc_test}" || true)" -ne 1 ]]; then
      echo "Substrate ateom patch did not cover ${recovery_test}" >&2
      exit 1
    fi
  done

  restore_line="$(grep -n 'Restore the worker Pod network before fallible runsc cleanup' "${SUBSTRATE_DIR}/${service_source}" | cut -d: -f1)"
  delete_line="$(grep -n 'Delete all application containers' "${SUBSTRATE_DIR}/${service_source}" | cut -d: -f1)"
  if [[ -z "${restore_line}" || -z "${delete_line}" || "${restore_line}" -ge "${delete_line}" ]]; then
    echo "Substrate ateom patch did not restore worker networking before fallible delete cleanup" >&2
    exit 1
  fi

  log "Applied reviewed Substrate ateom runsc-delete recovery patch"
}

verify_reviewed_substrate_patch_set() {
  local expected_files changed_files
  expected_files="$(printf '%s\n' \
    cmd/servers/atelet/oci.go \
    cmd/servers/atenet/app/router/envoyrunner.go \
    cmd/servers/atenet/app/router/extproc_in.go \
    cmd/servers/atenet/app/router/extproc_in_test.go \
    cmd/servers/atenet/app/router/xds.go \
    cmd/servers/atenet/app/router/xds_test.go \
    cmd/servers/ateom-gvisor/ateom-gvisor.go \
    cmd/servers/ateom-gvisor/runsc.go \
    cmd/servers/ateom-gvisor/runsc_test.go \
    manifests/ate-install/atenet-router.yaml | LC_ALL=C sort)"
  changed_files="$(git -C "${SUBSTRATE_DIR}" status --short | sed -E 's/^.. //' | LC_ALL=C sort)"
  if [[ "${changed_files}" != "${expected_files}" ]]; then
    echo "reviewed Substrate patch set changed unexpected files: ${changed_files:-<none>}" >&2
    exit 1
  fi

  log "Running focused tests for the reviewed Substrate patches"
  (
    cd "${SUBSTRATE_DIR}"
    go test ./cmd/servers/atelet ./cmd/servers/atenet/app/router -count=1
    if [[ "$(go env GOOS)" == "linux" ]]; then
      go test ./cmd/servers/ateom-gvisor -count=1
    else
      # The pinned netlink dependency does not expose Linux family constants on
      # non-Linux hosts. Cross-compile the package here; Linux CI/live E2E runs
      # the injected delete-recovery tests.
      GOOS=linux GOARCH="$(go env GOARCH)" go test -c \
        -o "${TMP_ROOT}/ateom-gvisor-patch-tests" ./cmd/servers/ateom-gvisor
    fi
  )
}

publish_ateom_image() {
  local published
  published="$(
    cd "${SUBSTRATE_DIR}"
    export DOCKER_CONFIG="${DOCKER_CONFIG_DIR}"
    export KO_DOCKER_REPO="localhost:${KIND_REGISTRY_PORT}"
    ko publish ./cmd/servers/ateom-gvisor
  )"
  published="$(printf '%s\n' "${published}" | tail -n1)"
  if [[ -z "${published}" ]]; then
    echo "ko did not return an ateom-gvisor image reference" >&2
    exit 1
  fi
  printf '%s' "${published}"
}

deploy_responses_fixture() {
  local image="$1"

  log "Deploying local Responses-compatible provider fixture"
  kubectl create namespace vekil-system --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n vekil-system apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vekil
  labels:
    app.kubernetes.io/name: vekil
    app.kubernetes.io/component: responses-fixture
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vekil
      app.kubernetes.io/component: responses-fixture
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vekil
        app.kubernetes.io/component: responses-fixture
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: responses
          image: ${image}
          imagePullPolicy: Always
          ports:
            - name: http
              containerPort: 1337
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
---
apiVersion: v1
kind: Service
metadata:
  name: vekil
  labels:
    app.kubernetes.io/name: vekil
spec:
  selector:
    app.kubernetes.io/name: vekil
    app.kubernetes.io/component: responses-fixture
  ports:
    - name: http
      port: 1337
      targetPort: http
YAML
  # The fixture stores request counts in memory. Force a new Pod even when a
  # reused cluster receives the same fixed image and Pod template.
  kubectl -n vekil-system rollout restart deployment/vekil
  kubectl -n vekil-system rollout status deployment/vekil --timeout=2m
}

create_substrate_resources() {
  local ateom_image="$1"
  local workspace_actor_image="$2"
  local mcp_actor_image="$3"

  log "Creating Substrate WorkerPool and ActorTemplate"
  kubectl create namespace ate-demo --dry-run=client -o yaml | kubectl apply -f -
  bash "${ROOT_DIR}/scripts/lib/ensure-static-mode-namespace.sh" \
    kubectl "${ORKA_NAMESPACE}" harness-v2
  for ns in ate-demo "${ORKA_NAMESPACE}"; do
    kubectl -n "${ns}" create secret generic "${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_NAME}" \
      "--from-literal=${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_KEY}=${SUBSTRATE_BOOTSTRAP_TOKEN}" \
      --dry-run=client -o yaml | kubectl apply -f -
  done

  # Operator-provided template-namespace grant: Substrate-backed RuntimePools
  # render their derived ActorTemplate in the infrastructure template's
  # namespace. Nothing secret is created there: pool credentials stay in the
  # controller's runtime namespace and are seeded post-boot over the
  # nonce-gated credential bootstrap endpoint.
  kubectl apply -f - <<'YAML'
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: orka-substrate-template-writer
  namespace: ate-demo
rules:
- apiGroups: ["ate.dev"]
  resources: ["actortemplates"]
  verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
# Credential-safe actor teardown destroys a live workload's memory by
# deleting its single-workload worker Pod before settling the actor.
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["delete", "get", "list"]
# The ACP runtime shares the provider worker Pod network namespace. Orka owns
# egress-only default-deny and DNS/controller/provider-proxy allowlists that
# select this dedicated WorkerPool before any Actor receives credentials.
- apiGroups: ["networking.k8s.io"]
  resources: ["networkpolicies"]
  verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: orka-substrate-template-writer
  namespace: ate-demo
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: orka-substrate-template-writer
subjects:
- kind: ServiceAccount
  name: orka-controller-manager
  namespace: orka-system
YAML
  # The workspace-backed ACP smoke keeps its infrastructure and derived
  # ActorTemplates in the tenant namespace, distinct from orka-runtimes where
  # pool Secrets live. Grant only the ActorTemplate writes needed there.
  kubectl apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: orka-substrate-runtime-template-writer
  namespace: ${ORKA_NAMESPACE}
rules:
- apiGroups: ["ate.dev"]
  resources: ["actortemplates"]
  verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: orka-substrate-runtime-template-writer
  namespace: ${ORKA_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: orka-substrate-runtime-template-writer
subjects:
- kind: ServiceAccount
  name: orka-controller-manager
  namespace: ${ORKA_NAMESPACE}
YAML
  kubectl apply -f - <<YAML
apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: orka-workers
  namespace: ate-demo
spec:
  replicas: 6
  ateomImage: ${ateom_image}
---
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: orka-codex-ci
  namespace: ate-demo
  labels:
    orka.ai/execution-workspace: "true"
    orka.ai/workspace-provider: substrate
  annotations:
    orka.ai/agent-runtimes: codex
    orka.ai/workspace-daemon-port: "80"
    orka.ai/workspace-protocol: http-json-v1
    orka.ai/workspace-staging-root: /app
spec:
  pauseImage: registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4
  containers:
  - name: workspace
    image: ${workspace_actor_image}
    command:
      - /orka-workspace-agent
    env:
      - name: ORKA_WORKSPACE_AGENT_LISTEN_ADDR
        value: ":80"
      - name: ORKA_WORKSPACE_HANDOFF_TOKEN_FILE
        value: /app/orka-workspace-handoff-token
      - name: ORKA_WORKSPACE_BOOTSTRAP_TOKEN
        value: "${SUBSTRATE_BOOTSTRAP_TOKEN}"
    ports:
      - containerPort: 80
  workerPoolRef:
    name: orka-workers
    namespace: ate-demo
  snapshotsConfig:
    location: gs://ate-snapshots/orka-codex-ci/
  runsc:
    amd64:
      url: gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc
      sha256Hash: a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63
    arm64:
      url: gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc
      sha256Hash: 1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9
---
# Infrastructure base template for workspace-backed ACP RuntimePools: Orka
# copies its placement fields (workerPoolRef, runsc, snapshotsConfig) into a
# derived, controller-owned ActorTemplate; the container below is never
# executed by ACP pools.
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: orka-acp-infra
  namespace: ${ORKA_NAMESPACE}
spec:
  pauseImage: registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4
  containers:
  - name: infra-placeholder
    image: ${workspace_actor_image}
    command:
      - /orka-workspace-agent
    ports:
      - containerPort: 80
  workerPoolRef:
    name: orka-workers
    namespace: ate-demo
  snapshotsConfig:
    location: gs://ate-snapshots/orka-acp/
  runsc:
    amd64:
      url: gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc
      sha256Hash: a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63
    arm64:
      url: gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc
      sha256Hash: 1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9
YAML

  wait_jsonpath_equals \
    "actortemplate/orka-codex-ci readiness" \
    "kubectl -n ate-demo get actortemplate orka-codex-ci -o jsonpath='{.status.phase}'" \
    "Ready" \
    900

  log "Creating Substrate MCP ActorTemplate"
  kubectl apply -f - <<YAML
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: orka-mcp-ci
  namespace: ${ORKA_NAMESPACE}
  labels:
    orka.ai/execution-workspace: "true"
    orka.ai/workspace-provider: substrate
  annotations:
    orka.ai/workspace-daemon-port: "80"
    orka.ai/workspace-protocol: http-json-v1
    orka.ai/workspace-staging-root: /app
spec:
  pauseImage: registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4
  containers:
  - name: workspace
    image: ${mcp_actor_image}
    command:
      - /orka-mcp-e2e-server
    env:
      - name: ORKA_WORKSPACE_AGENT_LISTEN_ADDR
        value: ":80"
      - name: ORKA_WORKSPACE_BOOTSTRAP_TOKEN
        valueFrom:
          secretKeyRef:
            name: ${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_NAME}
            key: ${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_KEY}
    ports:
      - containerPort: 80
  workerPoolRef:
    name: orka-workers
    namespace: ate-demo
  snapshotsConfig:
    location: gs://ate-snapshots/orka-mcp-ci/
  runsc:
    amd64:
      url: gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc
      sha256Hash: a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63
    arm64:
      url: gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc
      sha256Hash: 1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9
YAML

  wait_jsonpath_equals \
    "actortemplate/orka-mcp-ci readiness" \
    "kubectl -n ${ORKA_NAMESPACE} get actortemplate orka-mcp-ci -o jsonpath='{.status.phase}'" \
    "Ready" \
    900
}

deploy_orka() {
  local controller_image="$1"
  local codex_runtime_actor_ref="${2:-}"
  local tmp_config
  tmp_config="$(mktemp -d "${TMP_ROOT}/orka-config.XXXXXX")"

  log "Regenerating manifests and installing Orka CRDs"
  make -C "${ROOT_DIR}" manifests generate
  make -C "${ROOT_DIR}" install
  make -C "${ROOT_DIR}" kustomize
  if [[ "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" ]]; then
    log "Bootstrapping test-only admission TLS"
    orka_e2e_remove_admission_webhooks
    orka_e2e_bootstrap_admission_tls kubectl "${ORKA_NAMESPACE}"
  fi

  cp -R "${ROOT_DIR}/config" "${tmp_config}/config"
  (cd "${tmp_config}/config/manager" && "${ROOT_DIR}/bin/kustomize" edit set image "controller=${controller_image}")
  (cd "${tmp_config}/config/provider-proxy" && "${ROOT_DIR}/bin/kustomize" edit set image "controller=${controller_image}")
  # Agent Substrate validation exercises the workspace provider directly. Omit
  # the unrelated clean-room publisher and SCM proxy workloads, but retain the
  # authenticated provider proxy used by the real Codex prompt smoke.
  (
    cd "${tmp_config}/config/acp-workload"
    "${ROOT_DIR}/bin/kustomize" edit remove resource ../publisher
    "${ROOT_DIR}/bin/kustomize" edit remove resource ../scm-egress-proxy
  )
  local placeholder_digest codex_runtime_image
  placeholder_digest="sha256:$(printf '0%.0s' {1..64})"
  codex_runtime_image="${codex_runtime_actor_ref:-example.invalid/orka/acp-codex@${placeholder_digest}}"
  kubectl -n orka-system create configmap acp-runtime-images \
    --from-literal="ORKA_ACP_CODEX_RUNTIME_IMAGE=${codex_runtime_image}" \
    --from-literal="ORKA_ACP_CLAUDE_RUNTIME_IMAGE=example.invalid/orka/acp-claude@${placeholder_digest}" \
    --from-literal="ORKA_ACP_COPILOT_RUNTIME_IMAGE=example.invalid/orka/acp-copilot@${placeholder_digest}" \
    --from-literal="ORKA_ACP_OPENCODE_RUNTIME_IMAGE=example.invalid/orka/acp-opencode@${placeholder_digest}" \
    --dry-run=client -o yaml | kubectl apply -f -

  local capability_dir snapshot_key_field artifact_capability_field publisher_controller_field publisher_operation_field provider_field
  capability_dir="$(mktemp -d "${TMP_ROOT}/acp-capabilities.XXXXXX")"
  snapshot_key_field="snapshot-key"
  artifact_capability_field="capability-secret"
  publisher_controller_field="controller-token"
  publisher_operation_field="operation-capability-secret"
  provider_field="token"
  chmod 0700 "${capability_dir}"
  dd if=/dev/urandom bs=32 count=1 2>/dev/null >"${capability_dir}/snapshot-key"
  dd if=/dev/urandom bs=32 count=1 2>/dev/null >"${capability_dir}/artifact-capability"
  dd if=/dev/urandom bs=32 count=1 2>/dev/null >"${capability_dir}/publisher-token"
  dd if=/dev/urandom bs=32 count=1 2>/dev/null >"${capability_dir}/publisher-capability"
  # The RuntimePool provider token must be printable (it is compared and
  # copied into pool Secrets); raw random bytes are rejected.
  openssl rand -hex 32 >"${capability_dir}/provider-token"
  chmod 0600 "${capability_dir}"/*
  kubectl -n orka-system create secret generic agent-execution-snapshot-key \
    --from-file="${snapshot_key_field}=${capability_dir}/snapshot-key" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n orka-system create secret generic acp-artifact-capability \
    --from-file="${artifact_capability_field}=${capability_dir}/artifact-capability" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n orka-system create secret generic workspace-publisher-auth \
    --from-file="${publisher_controller_field}=${capability_dir}/publisher-token" \
    --from-file="${publisher_operation_field}=${capability_dir}/publisher-capability" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n orka-system create secret generic provider-auth-proxy \
    --from-file="${provider_field}=${capability_dir}/provider-token" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -rf "${capability_dir}"

  "${ROOT_DIR}/bin/kustomize" build "${tmp_config}/config/acp-workload" | kubectl apply -f -
  if [[ "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" ]]; then
    log "Deploying the dedicated fail-closed admission runtime"
    orka_e2e_deploy_admission "${controller_image}" kubectl "${ORKA_NAMESPACE}"
  fi
  ensure_orka_api_client_identity
  # Substrate actor traffic originates from its single-workload WorkerPool Pod
  # in ate-demo rather than a native orka-runtimes Pod. Keep the same
  # authenticated proxy boundary while allowing only that provider namespace.
  kubectl -n orka-system patch networkpolicy orka-provider-auth-proxy --type=json -p '[
    {
      "op": "add",
      "path": "/spec/ingress/-",
      "value": {
        "from": [
          {
            "namespaceSelector": {
              "matchLabels": {
                "kubernetes.io/metadata.name": "ate-demo"
              }
            }
          }
        ],
        "ports": [
          {
            "protocol": "TCP",
            "port": 8080
          }
        ]
      }
    }
  ]'
  kubectl -n orka-system rollout status deployment/orka-provider-auth-proxy --timeout=5m

  local patch
  local workspace_dispatch="false"
  if [[ "${SUBSTRATE_E2E_ACP_TASK_SMOKE}" == "1" || "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" || "${SUBSTRATE_E2E_LIFECYCLE}" == "1" ]]; then
    workspace_dispatch="true"
  fi
  local workspace_api="false"
  if [[ "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" ]]; then
    workspace_api="true"
    # The dedicated admission runtime above is the API server boundary. These
    # controller flags also register equivalent local handlers, so give the
    # manager webhook server a certificate even though no Service routes to it.
    local webhook_cert_dir
    webhook_cert_dir="$(mktemp -d "${TMP_ROOT}/webhook-certs.XXXXXX")"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
      -keyout "${webhook_cert_dir}/tls.key" -out "${webhook_cert_dir}/tls.crt" \
      -subj "/CN=orka-controller-manager.orka-system.svc" >/dev/null 2>&1
    kubectl -n orka-system create secret tls orka-webhook-serving-certs \
      --cert="${webhook_cert_dir}/tls.crt" --key="${webhook_cert_dir}/tls.key" \
      --dry-run=client -o yaml | kubectl apply -f -
    rm -rf "${webhook_cert_dir}"
  fi
  patch="$(jq -cn \
    --arg bootstrap_secret_name "${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_NAME}" \
    --arg bootstrap_secret_key "${SUBSTRATE_BOOTSTRAP_TOKEN_SECRET_KEY}" \
    --arg workspaceDispatch "${workspace_dispatch}" \
    --arg workspaceAPI "${workspace_api}" \
    --arg ambiguityMarker "${LIFECYCLE_AMBIGUITY_MARKER}" \
    '{
      spec: {
        template: {
          spec: {
            containers: [
              {
                name: "manager",
                # The Substrate-only deployment removes the Publisher workload.
                # Disable the client explicitly so fail-closed capability
                # negotiation does not target a Service that is intentionally
                # absent from this direct provider evaluation cluster.
                env: [
                  {
                    name: "ORKA_WORKSPACE_PUBLISHER_URL",
                    "$patch": "delete"
                  }
                ],
                imagePullPolicy: "IfNotPresent",
                resources: {
                  requests: { cpu: "250m", memory: "256Mi" },
                  limits: { cpu: "2", memory: "1Gi" }
                },
                livenessProbe: {
                  httpGet: { path: "/healthz", port: 8081 },
                  initialDelaySeconds: 30,
                  periodSeconds: 20,
                  timeoutSeconds: 5,
                  failureThreshold: 6
                },
                readinessProbe: {
                  httpGet: { path: "/readyz", port: 8081 },
                  initialDelaySeconds: 10,
                  periodSeconds: 10,
                  timeoutSeconds: 5,
                  failureThreshold: 6
                },
                args: ([
                  "--leader-elect",
                  "--health-probe-bind-address=:8081",
                  "--agent-execution-snapshot-key-file=/var/run/orka/agent-execution-snapshot/key",
                  "--controller-url=http://orka-api.orka-system.svc:8080",
                  "--controller-mode=harness-v2",
                  "--watch-namespace=orka-system",
                  "--enforce-namespace-isolation=true",
                  "--execution-mode-controller-usernames=system:serviceaccount:orka-system:orka-controller-manager",
                  "--execution-workspace-default-provider=substrate",
                  "--agent-sandbox-enabled=false",
                  "--substrate-enabled=true",
                  "--substrate-api-endpoint=api.ate-system.svc:443",
                  "--substrate-api-insecure-skip-verify=true",
                  "--substrate-router-url=http://atenet-router.ate-system.svc",
                  "--substrate-actor-dns-suffix=actors.resources.substrate.ate.dev",
                  "--substrate-default-template=orka-codex-ci",
                  "--substrate-default-template-namespace=ate-demo",
                  "--substrate-bootstrap-token-secret-name=" + $bootstrap_secret_name,
                  "--substrate-bootstrap-token-secret-key=" + $bootstrap_secret_key,
                  "--substrate-claim-timeout=2m",
                  "--substrate-command-timeout=10m",
                  "--substrate-cleanup-policy=delete",
                  "--acp-workspace-dispatch-enabled=" + $workspaceDispatch,
                  "--acp-e2e-prompt-write-ambiguity-marker=" + $ambiguityMarker,
                  # RuntimePool reconciliation and prompt execution use the
                  # authenticated provider-proxy boundary deployed above; the
                  # token Secret is created above and mounted by the base manifest.
                  "--acp-provider-proxy-base-url=http://orka-provider-auth-proxy.orka-system.svc:8080",
                  "--acp-provider-proxy-namespace=orka-system",
                  "--acp-provider-proxy-pod-labels=orka.ai/network-role=provider-auth-proxy",
                  "--acp-provider-proxy-token-file=/var/run/orka/provider-auth/token"
                ] + (if $workspaceAPI == "true" then [
                  "--enable-workspace-provider-api=true",
                  "--workspace-class-use-admission-enabled=true",
                  "--task-provenance-admission-enabled=true"
                ] else [] end)),
                volumeMounts: (if $workspaceAPI == "true" then [
                  {
                    name: "webhook-serving-certs",
                    mountPath: "/tmp/k8s-webhook-server/serving-certs",
                    readOnly: true
                  }
                ] else [] end)
              }
            ],
            volumes: (if $workspaceAPI == "true" then [
              {
                name: "webhook-serving-certs",
                secret: { secretName: "orka-webhook-serving-certs" }
              }
            ] else [] end)
          }
        }
      }
    }')"
  kubectl -n orka-system patch deployment orka-controller-manager --type=strategic -p "${patch}"
  kubectl -n orka-system rollout status deployment/orka-controller-manager --timeout=5m
}

create_substrate_actor_pools() {
  log "Creating Orka SubstrateActorPools"
  kubectl -n "${ORKA_NAMESPACE}" apply -f - <<'YAML'
apiVersion: core.orka.ai/v1alpha1
kind: SubstrateActorPool
metadata:
  name: mcp-substrate-pool-ci
spec:
  templateRef:
    name: orka-mcp-ci
  workerPoolRef:
    name: orka-workers
    namespace: ate-demo
  targetActors: 2
  precreateActors: true
YAML

  wait_jsonpath_equals \
    "substrateactorpool/mcp-substrate-pool-ci readiness" \
    "kubectl -n ${ORKA_NAMESPACE} get substrateactorpool mcp-substrate-pool-ci -o jsonpath='{.status.phase}'" \
    "Ready" \
    600
  wait_jsonpath_int_at_least \
    "substrateactorpool/mcp-substrate-pool-ci actor count" \
    "kubectl -n ${ORKA_NAMESPACE} get substrateactorpool mcp-substrate-pool-ci -o jsonpath='{.status.actorCount}'" \
    2 \
    600
}

create_mcp_tool() {
  log "Creating pooled MCP Tool"
  kubectl -n "${ORKA_NAMESPACE}" apply -f - <<'YAML'
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: mcp-ci
spec:
  description: E2E MCP tool backed by a durable Substrate actor.
  parameters:
    type: object
    properties:
      message:
        type: string
    required:
      - message
  mcp:
    path: /mcp
    substrateActor:
      templateRef:
        name: orka-mcp-ci
      poolRef:
        name: mcp-substrate-pool-ci
      boot: true
YAML

  wait_jsonpath_equals \
    "tool/mcp-ci availability" \
    "kubectl -n ${ORKA_NAMESPACE} get tool mcp-ci -o jsonpath='{.status.available}'" \
    "true" \
    600
  wait_jsonpath_equals \
    "tool/mcp-ci actor provider" \
    "kubectl -n ${ORKA_NAMESPACE} get tool mcp-ci -o jsonpath='{.status.actor.provider}'" \
    "substrate" \
    60
  wait_jsonpath_equals \
    "tool/mcp-ci poolRef" \
    "kubectl -n ${ORKA_NAMESPACE} get tool mcp-ci -o jsonpath='{.status.actor.poolRef.name}'" \
    "mcp-substrate-pool-ci" \
    60
}

run_mcp_tool_client_job() {
  local tool_client_image="$1"
  local job_name="${2:-mcp-tool-exec-ci}"
  local message="${3:-ci}"
  local expected attempt
  local args_json

  if [[ ! "${MCP_TOOL_EXEC_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "MCP_TOOL_EXEC_ATTEMPTS must be a positive integer, got ${MCP_TOOL_EXEC_ATTEMPTS}" >&2
    return 1
  fi
  if [[ ! "${MCP_TOOL_EXEC_RETRY_DELAY_SECONDS}" =~ ^[0-9]+$ ]]; then
    echo "MCP_TOOL_EXEC_RETRY_DELAY_SECONDS must be a non-negative integer, got ${MCP_TOOL_EXEC_RETRY_DELAY_SECONDS}" >&2
    return 1
  fi

  if [[ "$#" -ge 4 ]]; then
    expected="$4"
  else
    expected="mcp-e2e-ok:mcp-ci:${message}"
  fi
  args_json="$(jq -cn --arg message "${message}" '{message: $message}')"

  for ((attempt = 1; attempt <= MCP_TOOL_EXEC_ATTEMPTS; attempt++)); do
    log "Executing MCP Tool through worker ToolExecutor (attempt ${attempt}/${MCP_TOOL_EXEC_ATTEMPTS})"
    kubectl -n "${ORKA_NAMESPACE}" delete "job/${job_name}" --ignore-not-found --wait=true >/dev/null
    kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${job_name}
  namespace: ${ORKA_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${job_name}
  namespace: ${ORKA_NAMESPACE}
rules:
  - apiGroups:
      - core.orka.ai
    resources:
      - tools
    verbs:
      - get
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${job_name}
  namespace: ${ORKA_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${job_name}
subjects:
  - kind: ServiceAccount
    name: ${job_name}
    namespace: ${ORKA_NAMESPACE}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${ORKA_NAMESPACE}
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: ${job_name}
      restartPolicy: Never
      containers:
        - name: tool-client
          image: ${tool_client_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: ORKA_TOOL_NAMESPACE
              value: ${ORKA_NAMESPACE}
            - name: ORKA_TOOL_NAME
              value: mcp-ci
            - name: ORKA_TOOL_ARGS
              value: '${args_json}'
            - name: ORKA_TOOL_EXPECT_RESULT
              value: '${expected}'
YAML
    if wait_job_succeeded "${job_name}" 300; then
      run_redacted kubectl -n "${ORKA_NAMESPACE}" logs "job/${job_name}" --all-containers --tail=-1
      return 0
    fi

    run_redacted kubectl -n "${ORKA_NAMESPACE}" logs "job/${job_name}" --all-containers --tail=-1 || true
    if (( attempt == MCP_TOOL_EXEC_ATTEMPTS )); then
      echo "job/${job_name} did not complete after ${MCP_TOOL_EXEC_ATTEMPTS} attempts" >&2
      return 1
    fi
    log "Retrying MCP Tool execution after ${MCP_TOOL_EXEC_RETRY_DELAY_SECONDS}s"
    sleep "${MCP_TOOL_EXEC_RETRY_DELAY_SECONDS}"
  done
}

mcp_tool_client_result() {
  local job_name="$1"
  kubectl -n "${ORKA_NAMESPACE}" logs "job/${job_name}" --all-containers --tail=-1 | redact | tail -n1
}

verify_mcp_tool_boots_actor_once() {
  local tool_client_image="$1"
  local actor_id booted_actor_id generation before after before_started before_count after_started after_count

  log "Verifying MCP Tool actor is booted once across forced reconcile"
  actor_id="$(kubectl -n "${ORKA_NAMESPACE}" get tool mcp-ci -o jsonpath='{.status.actor.actorID}')"
  booted_actor_id="$(kubectl -n "${ORKA_NAMESPACE}" get tool mcp-ci -o json | jq -r '.metadata.annotations["orka.ai/substrate-mcp-tool-booted-id"] // ""')"
  if [[ -z "${actor_id}" || "${booted_actor_id}" != "${actor_id}" ]]; then
    echo "tool/mcp-ci booted actor annotation = ${booted_actor_id:-<empty>}, want ${actor_id:-<empty>}" >&2
    exit 1
  fi

  run_mcp_tool_client_job "${tool_client_image}" "mcp-tool-state-before-ci" "boot-state" ""
  before="$(mcp_tool_client_result mcp-tool-state-before-ci)"
  if [[ ! "${before}" =~ ^mcp-e2e-state:mcp-ci:([0-9]+):([0-9]+)$ ]]; then
    echo "unexpected pre-reconcile MCP state response: ${before}" >&2
    exit 1
  fi
  before_started="${BASH_REMATCH[1]}"
  before_count="${BASH_REMATCH[2]}"

  generation="$(
    kubectl -n "${ORKA_NAMESPACE}" patch tool mcp-ci --type=merge \
      -p '{"spec":{"description":"E2E MCP tool backed by a durable Substrate actor after forced reconcile."}}' \
      -o json | jq -r '.metadata.generation'
  )"
  wait_jsonpath_equals \
    "tool/mcp-ci forced reconcile observed generation" \
    "kubectl -n ${ORKA_NAMESPACE} get tool mcp-ci -o json | jq -r '.status.conditions[]? | select(.type == \"Available\") | .observedGeneration'" \
    "${generation}" \
    120

  booted_actor_id="$(kubectl -n "${ORKA_NAMESPACE}" get tool mcp-ci -o json | jq -r '.metadata.annotations["orka.ai/substrate-mcp-tool-booted-id"] // ""')"
  if [[ "${booted_actor_id}" != "${actor_id}" ]]; then
    echo "tool/mcp-ci booted actor annotation after reconcile = ${booted_actor_id:-<empty>}, want ${actor_id}" >&2
    exit 1
  fi

  run_mcp_tool_client_job "${tool_client_image}" "mcp-tool-state-after-ci" "boot-state" ""
  after="$(mcp_tool_client_result mcp-tool-state-after-ci)"
  if [[ ! "${after}" =~ ^mcp-e2e-state:mcp-ci:([0-9]+):([0-9]+)$ ]]; then
    echo "unexpected post-reconcile MCP state response: ${after}" >&2
    exit 1
  fi
  after_started="${BASH_REMATCH[1]}"
  after_count="${BASH_REMATCH[2]}"
  if [[ "${after_started}" != "${before_started}" ]]; then
    echo "tool/mcp-ci actor process restarted across forced reconcile: before=${before}, after=${after}" >&2
    exit 1
  fi
  if (( after_count <= before_count )); then
    echo "tool/mcp-ci actor state did not advance across forced reconcile: before=${before}, after=${after}" >&2
    exit 1
  fi
  log "tool/mcp-ci retained MCP actor state across forced reconcile"
}

verify_mcp_tool_cleanup() {
  local actor_id pool_name generation pool_prefix pool_actor_0 pool_actor_1

  log "Verifying MCP Tool deletion and non-precreating pool scale-down prune actors"
  actor_id="$(kubectl -n "${ORKA_NAMESPACE}" get tool mcp-ci -o jsonpath='{.status.actor.actorID}')"
  pool_name="$(kubectl -n "${ORKA_NAMESPACE}" get tool mcp-ci -o jsonpath='{.status.actor.poolRef.name}')"
  if [[ -z "${actor_id}" ]]; then
    echo "tool/mcp-ci missing status.actor.actorID before cleanup" >&2
    exit 1
  fi
  if [[ -z "${pool_name}" ]]; then
    echo "tool/mcp-ci missing status.actor.poolRef.name before cleanup" >&2
    exit 1
  fi
  pool_prefix="$(substrate_actor_pool_prefix "${ORKA_NAMESPACE}" "${pool_name}")"
  pool_actor_0="${pool_prefix}-00000"
  pool_actor_1="${pool_prefix}-00001"
  kubectl -n "${ORKA_NAMESPACE}" get lease "${actor_id}" >/dev/null

  generation="$(
    kubectl -n "${ORKA_NAMESPACE}" patch substrateactorpool "${pool_name}" --type=merge \
      -p '{"spec":{"targetActors":0,"precreateActors":false}}' \
      -o json | jq -r '.metadata.generation'
  )"
  wait_jsonpath_equals \
    "substrateactorpool/${pool_name} scale-down observed generation" \
    "kubectl -n ${ORKA_NAMESPACE} get substrateactorpool ${pool_name} -o jsonpath='{.status.observedGeneration}'" \
    "${generation}" \
    120

  kubectl -n "${ORKA_NAMESPACE}" delete tool mcp-ci --wait=false
  wait_resource_absent "${ORKA_NAMESPACE}" tool mcp-ci 300
  wait_resource_absent "${ORKA_NAMESPACE}" lease "${actor_id}" 300
  wait_actor_absent "${actor_id}" 300
  wait_jsonpath_equals \
    "substrateactorpool/${pool_name} non-precreate scale-down readiness" \
    "kubectl -n ${ORKA_NAMESPACE} get substrateactorpool ${pool_name} -o jsonpath='{.status.phase}'" \
    "Ready" \
    300
  wait_jsonpath_equals \
    "substrateactorpool/${pool_name} actor count after non-precreate prune" \
    "kubectl -n ${ORKA_NAMESPACE} get substrateactorpool ${pool_name} -o json | jq -r '.status.actorCount // 0'" \
    "0" \
    300
  wait_actor_absent "${pool_actor_0}" 300
  wait_actor_absent "${pool_actor_1}" 300
  log "tool/mcp-ci cleanup removed actor ${actor_id}, its pool lease, and scaled down pool actors"
}

exercise_orka_tasks() {
  local tool_client_image="$1"

  create_substrate_actor_pools

  create_mcp_tool
  run_mcp_tool_client_job "${tool_client_image}"
  verify_mcp_tool_boots_actor_once "${tool_client_image}"
  verify_mcp_tool_cleanup

}

wait_http_ok() {
  local url="$1"
  local host_header="$2"
  local auth_header="${3:-}"
  local timeout_seconds="$4"
  local started now
  started="$(date +%s)"

  while true; do
    if [[ -n "${auth_header}" ]]; then
      if curl -fsS -H "Host: ${host_header}" -H "${auth_header}" "${url}" >/dev/null 2>&1; then
        return 0
      fi
    elif curl -fsS -H "Host: ${host_header}" "${url}" >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for ${url} via Host ${host_header}" >&2
      return 1
    fi
    sleep 5
  done
}

write_workspace_handoff_token() {
  local url="$1"
  local host_header="$2"
  local token_b64="$3"
  local timeout_seconds="$4"
  local started now remaining attempt_timeout
  started="$(date +%s)"

  while true; do
    now="$(date +%s)"
    remaining=$((timeout_seconds - (now - started)))
    if (( remaining <= 0 )); then
      echo "timed out installing workspace handoff token via Host ${host_header}" >&2
      return 1
    fi
    attempt_timeout="${remaining}"
    if (( attempt_timeout > 5 )); then
      attempt_timeout=5
    fi
    # STATUS_RUNNING can precede the router's upstream connection becoming
    # usable after worker replacement or runsc recovery. Keep credentials and
    # response bodies private while retrying that bounded readiness window.
    if curl -fsS \
      --connect-timeout "${attempt_timeout}" \
      --max-time "${attempt_timeout}" \
      -H "Host: ${host_header}" \
      -H "Authorization: Bearer ${SUBSTRATE_BOOTSTRAP_TOKEN}" \
      -H "Content-Type: application/json" \
      -X PUT \
      -d "{\"files\":[{\"path\":\"/app/orka-workspace-handoff-token\",\"data\":\"${token_b64}\",\"mode\":384}]}" \
      "${url}" >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out installing workspace handoff token via Host ${host_header}" >&2
      return 1
    fi
    sleep 2
  done
}

run_idempotent_workspace_exec() {
  local url="$1"
  local host_header="$2"
  local handoff_token="$3"
  local request_id="$4"
  local timeout_seconds="$5"
  local started now remaining attempt_timeout response
  started="$(date +%s)"

  while true; do
    now="$(date +%s)"
    remaining=$((timeout_seconds - (now - started)))
    if (( remaining <= 0 )); then
      echo "timed out executing idempotent workspace probe via Host ${host_header}" >&2
      return 1
    fi
    attempt_timeout="${remaining}"
    if (( attempt_timeout > 5 )); then
      attempt_timeout=5
    fi
    # This exact command is deliberately idempotent. Retry only the live
    # routing probe, never arbitrary user commands, while a replacement
    # worker's Envoy upstream converges.
    if response="$(curl -fsS \
      --connect-timeout "${attempt_timeout}" \
      --max-time "${attempt_timeout}" \
      -H "Host: ${host_header}" \
      -H "Authorization: Bearer ${handoff_token}" \
      -H "X-Request-ID: ${request_id}" \
      -H "Content-Type: application/json" \
      -d '{"command":["/bin/sh","-lc","printf direct-ok"]}' \
      "${url}" 2>/dev/null)"; then
      printf '%s\n' "${response}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "timed out executing idempotent workspace probe via Host ${host_header}" >&2
      return 1
    fi
    sleep 2
  done
}

verify_router_request_metadata_allowlist() {
  local handoff_token="$1"
  local request_id="$2"
  local timeout_seconds="${3:-30}"
  local started now raw_log_file request_id_digest
  started="$(date +%s)"
  raw_log_file="${TMP_ROOT}/atenet-router-raw-${request_id}.log"
  request_id_digest="sha256:$(printf '%s' "${request_id}" | sha256_hex)"

  while true; do
    # Keep the provider output private and inspect it before any presentation
    # redaction. Otherwise the test could erase the exact token leak it is meant
    # to detect and then falsely pass on the safe request-ID evidence.
    kubectl -n ate-system logs deployment/atenet-router --all-containers --since=2m       >"${raw_log_file}" 2>/dev/null || :
    if grep -Fq -- "${SUBSTRATE_BOOTSTRAP_TOKEN}" "${raw_log_file}" ||
       grep -Fq -- "${handoff_token}" "${raw_log_file}"; then
      echo "atenet-router leaked an Authorization credential in its logs" >&2
      grep -F -- "${request_id_digest}" "${raw_log_file}" | redact >&2 || true
      return 1
    fi
    if grep -Fq -- "${request_id}" "${raw_log_file}"; then
      echo "atenet-router leaked a raw request ID in its logs" >&2
      grep -F -- "${request_id}" "${raw_log_file}" | redact >&2 || true
      return 1
    fi
    if grep -Fq -- "${request_id_digest}" "${raw_log_file}"; then
      log "atenet-router logs retain a request audit digest while omitting raw request metadata and credentials"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started > timeout_seconds )); then
      echo "timed out waiting for atenet-router request-metadata allowlist evidence" >&2
      return 1
    fi
    sleep 2
  done
}

run_direct_actor_lifecycle() {
  local actor_name="$1"
  local host_header="${actor_name}.actors.resources.substrate.ate.dev"
  local token token_b64 request_id response

  kubectl_ate create actor "${actor_name}" --template ate-demo/orka-codex-ci
  kubectl_ate resume actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_RUNNING" 300
  wait_http_ok "http://127.0.0.1:18082/healthz" "${host_header}" "" 300

  printf -v token 'ci-token-%s' "$(date +%s%N)"
  token_b64="$(printf '%s' "${token}" | base64 | tr -d '\n')"
  write_workspace_handoff_token "http://127.0.0.1:18082/v1/files" "${host_header}" "${token_b64}" 60

  wait_http_ok "http://127.0.0.1:18082/healthz" "${host_header}" "Authorization: Bearer ${token}" 60
  printf -v request_id 'orka-router-log-%s' "$(date +%s%N)"
  response="$(run_idempotent_workspace_exec \
    "http://127.0.0.1:18082/v1/exec" "${host_header}" "${token}" "${request_id}" 60)"
  if [[ "$(jq -r '.exitCode' <<< "${response}")" != "0" || "$(jq -r '.stdout' <<< "${response}")" != "direct-ok" ]]; then
    echo "unexpected direct exec response for actor/${actor_name}" >&2
    jq -c '{exitCode,stdout,stderr}' <<< "${response}" | redact >&2
    return 1
  fi
  verify_router_request_metadata_allowlist "${token}" "${request_id}" 30

  kubectl_ate suspend actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_SUSPENDED" 300
  kubectl_ate delete actor "${actor_name}"
  wait_actor_absent "${actor_name}" 120
}

# The ateom image is distroless, so the live fault injector must be a static
# executable rather than a shell wrapper. It fails one delete before delegating
# every later invocation to the checksum-verified runsc binary beside it.
build_runsc_delete_failure_injector() {
  local output_path="$1"
  local architecture="$2"
  local target_os="${3:-linux}"
  local source_path="${TMP_ROOT}/runsc-delete-failure-injector.go"

  cat >"${source_path}" <<'GO'
package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func main() {
	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve runsc injector path: %v\n", err)
		os.Exit(87)
	}

	isDelete := false
	for _, arg := range os.Args[1:] {
		if arg == "delete" {
			isDelete = true
			break
		}
	}
	if isDelete {
		marker := executable + ".orka-delete-failure-observed"
		file, markerErr := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if markerErr == nil {
			_ = file.Close()
			fmt.Fprintln(os.Stderr, "injected one runsc delete failure before container removal")
			os.Exit(86)
		}
		if !errors.Is(markerErr, os.ErrExist) {
			fmt.Fprintf(os.Stderr, "create runsc delete injection marker: %v\n", markerErr)
			os.Exit(87)
		}
	}

	realPath := executable + ".orka-real"
	argv := append([]string{realPath}, os.Args[1:]...)
	if err := syscall.Exec(realPath, argv, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "exec real runsc: %v\n", err)
		os.Exit(87)
	}
}
GO

  CGO_ENABLED=0 GOOS="${target_os}" GOARCH="${architecture}" go build -trimpath -o "${output_path}" "${source_path}"
}

install_runsc_delete_failure_injector() {
  local worker_name="$1"
  local node architecture runsc_hash runsc_path injector_path node_injector_path

  node="$(kubectl -n ate-demo get pod "${worker_name}" -o jsonpath='{.spec.nodeName}')"
  architecture="$(kubectl get node "${node}" -o jsonpath='{.status.nodeInfo.architecture}')"
  case "${architecture}" in
    amd64 | arm64) ;;
    *)
      echo "unsupported kind node architecture for runsc delete injection: ${architecture:-<empty>}" >&2
      return 1
      ;;
  esac
  runsc_hash="$(kubectl -n ate-demo get actortemplate orka-codex-ci -o "jsonpath={.spec.runsc.${architecture}.sha256Hash}")"
  if [[ -z "${node}" || -z "${runsc_hash}" ]]; then
    echo "could not resolve the assigned worker node or runsc digest for delete injection" >&2
    return 1
  fi

  runsc_path="/run/ateom-gvisor/static-files/runsc-${runsc_hash}"
  injector_path="${TMP_ROOT}/runsc-delete-failure-injector-${architecture}"
  node_injector_path="/root/orka-runsc-delete-failure-injector-$$-${RANDOM}"
  build_runsc_delete_failure_injector "${injector_path}" "${architecture}"
  docker cp "${injector_path}" "${node}:${node_injector_path}"

  RUNSC_DELETE_INJECTION_NODE="${node}"
  RUNSC_DELETE_INJECTION_PATH="${runsc_path}"
  if ! docker exec "${node}" /bin/sh -ceu '
    path="$1"
    incoming="$2"
    test -f "${path}"
    test ! -e "${path}.orka-real"
    rm -f "${path}.orka-delete-failure-observed"
    mv "${path}" "${path}.orka-real"
    if ! cp "${incoming}" "${path}" || ! chmod 0755 "${path}"; then
      rm -f "${path}"
      mv "${path}.orka-real" "${path}"
      rm -f "${incoming}"
      exit 1
    fi
    rm -f "${incoming}"
  ' sh "${runsc_path}" "${node_injector_path}"; then
    restore_runsc_delete_injector >/dev/null 2>&1 || true
    return 1
  fi
}

runsc_delete_failure_was_injected() {
  [[ -n "${RUNSC_DELETE_INJECTION_NODE}" && -n "${RUNSC_DELETE_INJECTION_PATH}" ]] || return 1
  docker exec "${RUNSC_DELETE_INJECTION_NODE}" /bin/sh -ceu \
    'test -f "${1}.orka-delete-failure-observed"' sh "${RUNSC_DELETE_INJECTION_PATH}"
}

exercise_runsc_delete_retry_recovery() {
  local actor_name="orka-delete-retry-ci"
  local host_header="${actor_name}.actors.resources.substrate.ate.dev"
  local actor_json worker_name worker_logs

  log "Validating an injected runsc delete failure is retried on the live worker"
  kubectl_ate create actor "${actor_name}" --template ate-demo/orka-codex-ci
  kubectl_ate resume actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_RUNNING" 300
  wait_http_ok "http://127.0.0.1:18082/healthz" "${host_header}" "" 300

  actor_json="$(kubectl_ate get actor "${actor_name}" -o json)"
  worker_name="$(jq -r '.actors[0].ateomPodName // empty' <<<"${actor_json}")"
  if [[ -z "${worker_name}" ]]; then
    echo "actor/${actor_name} did not expose its assigned worker before delete injection" >&2
    return 1
  fi

  install_runsc_delete_failure_injector "${worker_name}"
  kubectl_ate suspend actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_SUSPENDED" 300
  if ! runsc_delete_failure_was_injected; then
    restore_runsc_delete_injector >/dev/null 2>&1 || true
    echo "runsc delete failure injector was not invoked for actor/${actor_name}" >&2
    return 1
  fi
  restore_runsc_delete_injector

  worker_logs="$(kubectl -n ate-demo logs "pod/${worker_name}" -c ateom --since=5m 2>&1)"
  if ! grep -Fq 'runsc delete did not remove the container; retrying' <<<"${worker_logs}"; then
    echo "actor/${actor_name} suspended without live evidence from the patched runsc delete retry path" >&2
    return 1
  fi
  log "actor/${actor_name}: observed injected failure, verified-presence retry, and successful suspension"

  kubectl -n ate-demo wait --for=condition=Ready "pod/${worker_name}" --timeout=2m
  kubectl_ate delete actor "${actor_name}"
  wait_actor_absent "${actor_name}" 120

  # A fresh lifecycle after restoring the original binary proves that cleanup
  # left the worker fleet routable rather than merely settling actor metadata.
  run_direct_actor_lifecycle "orka-post-delete-retry-ci"
  assert_no_suspending_actors
}

exercise_worker_replacement_recovery() {
  local actor_name="orka-worker-loss-ci"
  local host_header="${actor_name}.actors.resources.substrate.ate.dev"
  local actor_json worker_name replacement_name

  log "Validating worker-loss settlement and post-replacement direct routing"
  kubectl_ate create actor "${actor_name}" --template ate-demo/orka-codex-ci
  kubectl_ate resume actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_RUNNING" 300
  wait_http_ok "http://127.0.0.1:18082/healthz" "${host_header}" "" 300

  actor_json="$(kubectl_ate get actor "${actor_name}" -o json)"
  worker_name="$(jq -r '.actors[0].ateomPodName // empty' <<< "${actor_json}")"
  if [[ -z "${worker_name}" ]]; then
    echo "actor/${actor_name} did not expose its assigned worker before replacement" >&2
    return 1
  fi

  kubectl -n ate-demo delete pod "${worker_name}" --wait=true
  wait_worker_absent "${worker_name}" 120
  kubectl -n ate-demo rollout status deployment/orka-workers-deployment --timeout=5m
  wait_worker_count_at_least 3 180

  kubectl_ate suspend actor "${actor_name}"
  wait_actor_status "${actor_name}" "STATUS_SUSPENDED" 120
  kubectl_ate delete actor "${actor_name}"
  wait_actor_absent "${actor_name}" 120

  replacement_name="orka-post-worker-loss-ci"
  run_direct_actor_lifecycle "${replacement_name}"

  # The dangling-worker path above cannot invoke CheckpointWorkload because the
  # assigned Pod is gone. Inject one live runsc failure on the replacement fleet
  # so the extended E2E proves the reviewed retry and verified-absence path.
  exercise_runsc_delete_retry_recovery
  assert_no_suspending_actors
}

exercise_direct_substrate() {
  log "Running direct Substrate workspace-agent smoke"
  kubectl -n ate-system port-forward svc/atenet-router 18082:80 >/tmp/orka-atenet-router-port-forward.log 2>&1 &
  PORT_FORWARD_PID="$!"
  sleep 3

  run_direct_actor_lifecycle "orka-direct-ci"
  if [[ "${SUBSTRATE_E2E_EXTENDED}" == "1" ]]; then
    exercise_worker_replacement_recovery
  fi

  kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  PORT_FORWARD_PID=""
}

# exercise_workspace_backed_acp_task proves the Phase-2 Substrate adapter live:
# a Task.spec.execution.workspace substrate Task binds a dedicated acp-ws-*
# RuntimePool, the controller renders a derived ActorTemplate from the
# operator infrastructure template, the actor boots the immutable Codex
# runtime supervisor under gVisor, and the authenticated exact-instance fence
# probe reaches Serving through the atenet-router, and a real Codex prompt
# completes through the authenticated provider proxy.
exercise_workspace_backed_acp_task() {
  log "Running workspace-backed ACP Task (Substrate) prompt smoke"

  # Earlier phases dirty ateom workers: a worker that hosted any workload
  # (including provider golden-snapshot builds) loses its eth0 to the gVisor
  # sandbox netns and fails later RunWorkload calls with "eth0: Link not
  # found". Recycle the fleet so the smoke's golden-build instance and real
  # actor both land on fresh workers.
  log "Recycling the Substrate worker fleet for fresh workers"
  local worker_pods
  worker_pods="$(kubectl -n ate-demo get pods -o name | grep '^pod/orka-workers-deployment-' || true)"
  if [[ -n "${worker_pods}" ]]; then
    # shellcheck disable=SC2086
    kubectl -n ate-demo delete ${worker_pods} --wait=true --timeout=5m
  fi
  kubectl -n ate-demo rollout status deployment/orka-workers-deployment --timeout=5m
  wait_worker_count_at_least 4 300

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: orka-ws-substrate-agent
  namespace: ${ORKA_NAMESPACE}
spec:
  runtime:
    type: codex
    contractVersion: orka.harness.v2
    defaultMaxTurns: 1
  model:
    name: gpt-5.5
---
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-substrate-smoke
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-substrate-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: none
      cleanupPolicy: delete
  prompt: "Reply exactly: ORKA_WS_SUBSTRATE_OK"
YAML

  wait_jsonpath_equals     "workspace-backed Task workspace provider"     "kubectl -n orka-system get task orka-ws-substrate-smoke -o jsonpath='{.status.executionWorkspace.provider}'"     "substrate" 120

  local pool_name
  pool_name="$(kubectl -n orka-system get task orka-ws-substrate-smoke     -o jsonpath='{.status.execution.runtimePoolName}')"
  if [[ "${pool_name}" != acp-ws-codex-* ]]; then
    echo "runtime pool ${pool_name:-<empty>} is not a workspace-backed pool" >&2
    kubectl -n orka-system get task orka-ws-substrate-smoke -o yaml >&2 || true
    return 1
  fi
  log "Workspace-backed Task bound RuntimePool ${pool_name}"

  local derived_template derived_template_name started now
  started="$(date +%s)"
  while true; do
    derived_template_name="$(kubectl -n "${ORKA_NAMESPACE}" get actortemplates \
      -l "orka.ai/runtime-pool-name=${pool_name}" \
      -o json 2>/dev/null | jq -r 'if (.items | length) == 1 then .items[0].metadata.name else "" end' || true)"
    if [[ -n "${derived_template_name}" ]]; then
      derived_template="actortemplate.ate.dev/${derived_template_name}"
      break
    fi
    now="$(date +%s)"
    if (( now - started >= 240 )); then
      echo "controller did not render a derived ActorTemplate in ${ORKA_NAMESPACE}" >&2
      echo "=== workspace-backed RuntimePool ===" >&2
      kubectl -n orka-system get runtimepools -o yaml >&2 || true
      echo "=== orka controller logs (runtimepool) ===" >&2
      kubectl -n orka-system logs deployment/orka-controller-manager --tail=2000 2>/dev/null | grep -iE "runtimepool|substrate|actor" | tail -80 >&2 || true
      return 1
    fi
    sleep 3
  done
  log "Controller rendered derived ${derived_template}"
  # The provider requires snapshotsConfig and golden-snapshots the template by
  # booting one instance; that is safe only because the rendered container is
  # completely credential-free (awaiting-bootstrap supervisor + public nonce).
  if ! kubectl -n "${ORKA_NAMESPACE}" get "${derived_template}" -o jsonpath='{.spec.snapshotsConfig.location}' | grep -q .; then
    echo "derived ActorTemplate lost the operator snapshotsConfig" >&2
    return 1
  fi
  local derived_env
  derived_env="$(kubectl -n "${ORKA_NAMESPACE}" get "${derived_template}" -o jsonpath='{.spec.containers[0].env}')"
  if grep -q "valueFrom" <<<"${derived_env}"; then
    echo "derived ActorTemplate resolves Secrets via valueFrom; provider workloads must be credential-free" >&2
    return 1
  fi
  if ! grep -q "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE" <<<"${derived_env}"; then
    echo "derived ActorTemplate is missing the credential bootstrap nonce env" >&2
    return 1
  fi
  if kubectl -n "${ORKA_NAMESPACE}" get secrets -l orka.ai/runtime-pool-uid -o name 2>/dev/null | grep -q .; then
    echo "pool Secrets leaked into the template namespace; credentials must stay in the runtime namespace" >&2
    kubectl -n "${ORKA_NAMESPACE}" get secrets -l orka.ai/runtime-pool-uid >&2 || true
    return 1
  fi

  log "Waiting for the Substrate-backed pool to reach Serving (actor boot under gVisor)"
  if ! wait_jsonpath_equals     "Substrate-backed RuntimePool lifecycle"     "kubectl -n orka-system get runtimepool ${pool_name} -o jsonpath='{.status.lifecycle}'"     "Serving" 600; then
    kubectl -n orka-system get runtimepool "${pool_name}" -o yaml >&2 || true
    "${TMP_ROOT}/kubectl-ate" get actors >&2 || true
    return 1
  fi

  local actors_json actor_count actor_id worker_pod
  actors_json="$("${TMP_ROOT}/kubectl-ate" get actors -o json)"
  actor_id="${derived_template_name%-template}"
  actor_count="$(jq -r \
    --arg namespace "${ORKA_NAMESPACE}" \
    --arg template "${derived_template_name}" \
    --arg actor "${actor_id}" \
    '[.actors[]? | select(.actorTemplateNamespace == $namespace and .actorTemplateName == $template and .actorId == $actor)] | length' \
    <<<"${actors_json}")"
  if [[ "${actor_count}" != "1" ]]; then
    echo "expected exact runtime actor ${actor_id} for ${ORKA_NAMESPACE}/${derived_template_name}, found ${actor_count}" >&2
    "${TMP_ROOT}/kubectl-ate" get actors >&2 || true
    return 1
  fi
  worker_pod="$(jq -r \
    --arg namespace "${ORKA_NAMESPACE}" \
    --arg template "${derived_template_name}" \
    --arg actor "${actor_id}" \
    '.actors[] | select(.actorTemplateNamespace == $namespace and .actorTemplateName == $template and .actorId == $actor) | .ateomPodName // empty' \
    <<<"${actors_json}")"

  log "Waiting for the workspace-backed Task to succeed"
  local task_json task_yaml phase execution_state execution_outcome result_available
  started="$(date +%s)"
  while true; do
    task_json="$(kubectl -n orka-system get task orka-ws-substrate-smoke -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${task_json}")"
    execution_state="$(jq -r '.status.execution.state // ""' <<<"${task_json}")"
    execution_outcome="$(jq -r '.status.execution.outcome // ""' <<<"${task_json}")"
    result_available="$(jq -r '.status.resultRef.available // false' <<<"${task_json}")"
    if [[ "${phase}" == "Succeeded" && "${execution_state}" == "Succeeded" &&
          "${execution_outcome}" == "Succeeded" && "${result_available}" == "true" ]]; then
      break
    fi
    if [[ "${phase}" == "Failed" || "${phase}" == "Cancelled" ||
          "${execution_state}" == "Failed" || "${execution_state}" == "Cancelled" ]]; then
      kubectl -n orka-system get task orka-ws-substrate-smoke -o yaml >&2 || true
      kubectl -n orka-system logs deployment/orka-controller-manager --tail=400 2>/dev/null | grep -iE "session|dispatch|substrate" | tail -60 >&2 || true
      echo "workspace-backed Task reached terminal failure (phase=${phase:-<empty>}, state=${execution_state:-<empty>}, outcome=${execution_outcome:-<empty>})" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started >= 300 )); then
      echo "workspace-backed Task did not succeed (phase=${phase:-<empty>}, state=${execution_state:-<empty>}, outcome=${execution_outcome:-<empty>}, resultAvailable=${result_available})" >&2
      kubectl -n orka-system get task orka-ws-substrate-smoke -o yaml >&2 || true
      kubectl -n orka-system get runtimepool "${pool_name}" -o yaml >&2 || true
      return 1
    fi
    sleep 3
  done
  log "Workspace-backed Task reached Succeeded/Succeeded with an available result"
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-substrate-smoke" "ORKA_WS_SUBSTRATE_OK"

  # Provider-native identifiers must never enter final public Task status: the
  # raw actor ID, actor route host (DNS suffix), assigned worker Pod, and
  # snapshot URIs. The public exact-instance fence is an opaque Orka identity.
  task_yaml="$(kubectl -n orka-system get task orka-ws-substrate-smoke -o yaml)"
  if ! jq -e '.status.execution.runtimeInstanceID | test("^workspace:[a-f0-9]{64}\\.[a-f0-9-]{36}$")' \
      <<<"${task_json}" >/dev/null; then
    echo "public Task status did not use an opaque workspace runtime identity" >&2
    return 1
  fi
  if grep -Fq "${actor_id}" <<<"${task_yaml}"; then
    echo "public Task status leaked the provider actor ID" >&2
    return 1
  fi
  if grep -q "actors.resources.substrate.ate.dev" <<<"${task_yaml}"; then
    echo "public Task status leaked a provider actor route host" >&2
    return 1
  fi
  if grep -q "gs://" <<<"${task_yaml}"; then
    echo "public Task status leaked a provider snapshot URI" >&2
    return 1
  fi
  if [[ -n "${worker_pod}" ]] && grep -q "${worker_pod}" <<<"${task_yaml}"; then
    echo "public Task status leaked the provider worker placement ${worker_pod}" >&2
    return 1
  fi

  log "Cleaning up the workspace-backed Task and pool"
  kubectl -n orka-system delete task orka-ws-substrate-smoke --wait=true --timeout=4m
  kubectl -n orka-system delete runtimepool "${pool_name}" --wait=true --timeout=5m
  if kubectl -n "${ORKA_NAMESPACE}" get "${derived_template}" >/dev/null 2>&1; then
    echo "pool finalization left the derived ActorTemplate behind" >&2
    return 1
  fi
  kubectl -n orka-system delete agent orka-ws-substrate-agent --ignore-not-found=true
  log "Workspace-backed ACP Task (Substrate) prompt smoke passed"
}


# exercise_workspace_suspend_resume_acp_task proves class-backed data-only
# suspension and cold resume live (issue #425): a session-scoped classRef Task
# suspends its workspace on detach (the pool drains to Stopped and the exact
# actor is consensually suspended with only DurableDir data checkpointed), a
# continuation Task cold-resumes the same actor with rotated bootstrap
# material, and explicit deletion removes the workspace, pool, and actor
# exactly.
exercise_workspace_suspend_resume_acp_task() {
  log "Running class-backed suspend/cold-resume conformance (Substrate)"

  log "Recycling the Substrate worker fleet for fresh workers"
  local worker_pods
  worker_pods="$(kubectl -n ate-demo get pods -o name | grep '^pod/orka-workers-deployment-' || true)"
  if [[ -n "${worker_pods}" ]]; then
    # shellcheck disable=SC2086
    kubectl -n ate-demo delete ${worker_pods} --wait=true --timeout=5m
  fi
  kubectl -n ate-demo rollout status deployment/orka-workers-deployment --timeout=5m
  wait_worker_count_at_least 4 300

  kubectl apply -f - <<YAML
apiVersion: acp.workspace.orka.ai/v1alpha1
kind: RuntimeProviderConfig
metadata:
  name: acp-substrate-e2e
spec:
  backend: substrate
---
apiVersion: workspace.orka.ai/v1alpha1
kind: ExecutionWorkspaceProvider
metadata:
  name: acp-substrate-e2e
spec:
  controllerName: acp.workspace.orka.ai/runtime-pool
  parametersRef:
    group: acp.workspace.orka.ai
    kind: RuntimeProviderConfig
    name: acp-substrate-e2e
  lifecycleState: Active
  requiredContracts:
    - workspace.orka.ai/v1
---
apiVersion: acp.workspace.orka.ai/v1alpha1
kind: RuntimeWorkspaceProfile
metadata:
  name: acp-substrate-suspend
  namespace: ${ORKA_NAMESPACE}
spec:
  substrate:
    templateRef:
      namespace: ${ORKA_NAMESPACE}
      name: orka-acp-infra
    suspend:
      mode: DataOnly
---
apiVersion: workspace.orka.ai/v1alpha1
kind: ExecutionWorkspaceClass
metadata:
  name: acp-substrate-suspend
  namespace: ${ORKA_NAMESPACE}
spec:
  providerRef:
    name: acp-substrate-e2e
  parametersRef:
    group: acp.workspace.orka.ai
    kind: RuntimeWorkspaceProfile
    name: acp-substrate-suspend
  mode: Interactive
  allowedReuseScopes:
    - Session
  lifecycle:
    defaultOnDetach: Suspend
    allowedOnDetach:
      - Suspend
      - Delete
    detachTimeout: 2m
    maxLifetime: 2h
    deletionPolicy:
      providerResources: Delete
      persistentVolumes: Delete
      checkpoints: Delete
YAML

  wait_jsonpath_equals \
    "suspendable workspace class readiness" \
    "kubectl -n ${ORKA_NAMESPACE} get executionworkspaceclass acp-substrate-suspend -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'" \
    "True" 180

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: orka-ws-suspend-agent
  namespace: ${ORKA_NAMESPACE}
spec:
  runtime:
    type: codex
    contractVersion: orka.harness.v2
    defaultMaxTurns: 1
  model:
    name: gpt-5.5
---
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-suspend-first
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-suspend-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-suspend-session
    create: true
  execution:
    workspace:
      classRef:
        name: acp-substrate-suspend
      reusePolicy: session
  prompt: "Reply exactly: ORKA_WS_SUSPEND_FIRST_OK"
YAML

  wait_jsonpath_equals \
    "first suspendable Task phase" \
    "kubectl -n orka-system get task orka-ws-suspend-first -o jsonpath='{.status.phase}'" \
    "Succeeded" 600
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-suspend-first" "ORKA_WS_SUSPEND_FIRST_OK"

  local workspace_name pool_name actor_id
  workspace_name="$(kubectl -n orka-system get task orka-ws-suspend-first \
    -o jsonpath='{.metadata.labels.acp\.workspace\.orka\.ai/execution-workspace}')"
  pool_name="$(kubectl -n orka-system get task orka-ws-suspend-first \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  if [[ -z "${workspace_name}" || "${pool_name}" != acp-ws-session-* ]]; then
    echo "class-backed Task did not bind a session workspace (workspace=${workspace_name:-<empty>} pool=${pool_name:-<empty>})" >&2
    kubectl -n orka-system get task orka-ws-suspend-first -o yaml >&2 || true
    return 1
  fi
  log "Class-backed Task bound workspace ${workspace_name} on pool ${pool_name}"

  log "Waiting for the detach-time data-only suspension to settle"
  wait_jsonpath_equals \
    "workspace desired state after detach" \
    "kubectl -n orka-system get executionworkspace ${workspace_name} -o jsonpath='{.spec.desiredState}'" \
    "Suspended" 240
  wait_jsonpath_equals \
    "workspace observed suspension" \
    "kubectl -n orka-system get executionworkspace ${workspace_name} -o jsonpath='{.status.state}'" \
    "Suspended" 600
  wait_jsonpath_equals \
    "suspended pool lifecycle" \
    "kubectl -n orka-system get runtimepool ${pool_name} -o jsonpath='{.status.lifecycle}'" \
    "Stopped" 240
  actor_id="$(kubectl -n orka-system get runtimepool "${pool_name}" \
    -o jsonpath='{.metadata.annotations.orka\.ai/substrate-actor-suspended}')"
  if [[ -z "${actor_id}" ]]; then
    echo "suspended pool carries no consensual checkpoint record" >&2
    kubectl -n orka-system get runtimepool "${pool_name}" -o yaml >&2 || true
    return 1
  fi
  local actor_state
  actor_state="$("${TMP_ROOT}/kubectl-ate" get actors -o json | jq -r \
    --arg actor "${actor_id}" '[.actors[]? | select(.actorId == $actor)][0].status // empty')"
  if [[ "${actor_state}" != "STATUS_SUSPENDED" ]]; then
    echo "provider actor ${actor_id} is not suspended (status=${actor_state:-<absent>})" >&2
    "${TMP_ROOT}/kubectl-ate" get actors >&2 || true
    return 1
  fi
  log "Actor ${actor_id} is consensually suspended with a data-only checkpoint"

  log "Continuing the session to cold-resume the suspended workspace"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-suspend-second
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-suspend-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-suspend-session
    create: false
  execution:
    workspace:
      classRef:
        name: acp-substrate-suspend
      reusePolicy: session
  prompt: "Reply exactly: ORKA_WS_SUSPEND_SECOND_OK"
YAML

  wait_jsonpath_equals \
    "continuation Task phase" \
    "kubectl -n orka-system get task orka-ws-suspend-second -o jsonpath='{.status.phase}'" \
    "Succeeded" 900
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-suspend-second" "ORKA_WS_SUSPEND_SECOND_OK"

  local second_workspace resumed_actor
  second_workspace="$(kubectl -n orka-system get task orka-ws-suspend-second \
    -o jsonpath='{.metadata.labels.acp\.workspace\.orka\.ai/execution-workspace}')"
  if [[ "${second_workspace}" != "${workspace_name}" ]]; then
    echo "continuation bound workspace ${second_workspace:-<empty>}, want the resumed ${workspace_name}" >&2
    return 1
  fi
  resumed_actor="$("${TMP_ROOT}/kubectl-ate" get actors -o json | jq -r \
    --arg actor "${actor_id}" '[.actors[]? | select(.actorId == $actor)] | length')"
  if [[ "${resumed_actor}" != "1" ]]; then
    echo "resume did not preserve the exact actor ${actor_id}" >&2
    "${TMP_ROOT}/kubectl-ate" get actors >&2 || true
    return 1
  fi
  log "Continuation cold-resumed the same logical session on actor ${actor_id}"

  # The controller must have stamped the resumed lineage onto the workspace:
  # that marker is what arms the supervisor's expectDurableResume assertion,
  # under which an empty or foreign DurableDir fails session creation closed
  # instead of silently starting a fresh baseline. Its presence after a
  # SUCCESSFUL continuation proves the resume path ran authenticated.
  local resumed_lineage
  resumed_lineage="$(kubectl -n orka-system get executionworkspace "${workspace_name}" \
    -o jsonpath='{.metadata.annotations.acp\.workspace\.orka\.ai/resumed-lineage}')"
  if [[ "${resumed_lineage}" != "true" ]]; then
    echo "resumed workspace does not carry resumed-lineage=true (got ${resumed_lineage:-<absent>})" >&2
    kubectl -n orka-system get executionworkspace "${workspace_name}" -o yaml >&2 || true
    return 1
  fi
  log "Workspace ${workspace_name} carries resumed-lineage=true; the durable checkpoint resume was authenticated"

  log "Deleting the suspended workspace and asserting exact cleanup"
  # Let the continuation's detach settle back into suspension first.
  wait_jsonpath_equals \
    "workspace re-suspension after continuation" \
    "kubectl -n orka-system get executionworkspace ${workspace_name} -o jsonpath='{.status.state}'" \
    "Suspended" 600
  kubectl -n orka-system delete task orka-ws-suspend-first orka-ws-suspend-second --wait=true --timeout=4m
  kubectl -n orka-system delete executionworkspace "${workspace_name}" --wait=true --timeout=6m
  wait_resource_absent "orka-system" "runtimepool" "${pool_name}" 300
  local remaining
  remaining="$("${TMP_ROOT}/kubectl-ate" get actors -o json | jq -r \
    --arg actor "${actor_id}" '[.actors[]? | select(.actorId == $actor)] | length')"
  if [[ "${remaining}" != "0" ]]; then
    echo "workspace deletion left provider actor ${actor_id} behind" >&2
    "${TMP_ROOT}/kubectl-ate" get actors >&2 || true
    return 1
  fi
  kubectl -n orka-system delete agent orka-ws-suspend-agent --ignore-not-found=true
  kubectl -n orka-system delete executionworkspaceclass acp-substrate-suspend --ignore-not-found=true
  kubectl -n orka-system delete runtimeworkspaceprofile acp-substrate-suspend --ignore-not-found=true
  kubectl delete executionworkspaceprovider acp-substrate-e2e --ignore-not-found=true
  kubectl delete runtimeproviderconfig acp-substrate-e2e --ignore-not-found=true
  log "Class-backed suspend/cold-resume conformance (Substrate) passed"
}


start_fixture_port_forward() {
  if [[ -n "${FIXTURE_PORT_FORWARD_PID}" ]] &&
    kill -0 "${FIXTURE_PORT_FORWARD_PID}" >/dev/null 2>&1; then
    return 0
  fi
  kubectl -n vekil-system port-forward svc/vekil \
    "${FIXTURE_LOCAL_PORT}:1337" >"${TMP_ROOT}/fixture-port-forward.log" 2>&1 &
  FIXTURE_PORT_FORWARD_PID="$!"
  local attempts_remaining=30
  while (( attempts_remaining > 0 )); do
    if curl -fsS --connect-timeout 2 --max-time 5 \
      "http://127.0.0.1:${FIXTURE_LOCAL_PORT}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done
  echo "fixture port-forward did not become ready" >&2
  return 1
}

# fixture_marker_count prints how many /responses requests resolved to the
# marker — the fixture-side proof that a prompt was sent exactly once (no
# replay across cancellation or controller restart).
# fixture_marker_key reduces a marker to the fixture's 8-byte SHA-256 digest
# key: the unauthenticated endpoints never expose raw prompt-derived markers.
fixture_marker_key() {
  printf '%s' "$1" | { sha256sum 2>/dev/null || shasum -a 256; } | awk '{print substr($1, 1, 16)}'
}

fixture_marker_count() {
  local marker
  marker="$(fixture_marker_key "$1")"
  start_fixture_port_forward || return 1
  curl -fsS --connect-timeout 2 --max-time 10 \
    "http://127.0.0.1:${FIXTURE_LOCAL_PORT}/fixture/marker-counts" |
    jq -r --arg marker "${marker}" '.[$marker] // 0'
}

# fixture_marker_saw_history reports whether a request resolving to the marker
# replayed prior conversation turns - the fixture-side proof that a
# continuation actually carried the session history.
fixture_marker_saw_history() {
  local marker
  marker="$(fixture_marker_key "$1")"
  start_fixture_port_forward || return 1
  curl -fsS --connect-timeout 2 --max-time 10 \
    "http://127.0.0.1:${FIXTURE_LOCAL_PORT}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" '.[$marker].sawHistory // false'
}

# fixture_marker_disconnects reports how many held requests for the marker
# observed a client disconnect before their hold elapsed - the proof that a
# cancellation closed the in-flight provider stream.
# fixture_marker_history_contains reports whether the requests resolving the
# FIRST marker replayed the SECOND marker in their history: the proof that a
# continuation preserved the EXPECTED earlier turn, not just any transcript
# with an assistant item.
fixture_marker_history_contains() {
  local marker expected
  marker="$(fixture_marker_key "$1")"
  expected="$(fixture_marker_key "$2")"
  start_fixture_port_forward || return 1
  curl -fsS --connect-timeout 2 --max-time 10 \
    "http://127.0.0.1:${FIXTURE_LOCAL_PORT}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" --arg expected "${expected}" \
      '(.[$marker].historyMarkers // []) | index($expected) != null'
}

fixture_marker_disconnects() {
  local marker
  marker="$(fixture_marker_key "$1")"
  start_fixture_port_forward || return 1
  curl -fsS --connect-timeout 2 --max-time 10 \
    "http://127.0.0.1:${FIXTURE_LOCAL_PORT}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" '.[$marker].disconnects // 0'
}

# assert_lc_task_success_tuple requires the COMPLETE canonical successful
# settlement: the controller-owned Succeeded/Succeeded/Succeeded tuple, a
# ReadValidated delivery projection, an available stored result, and no legacy
# Job projection (built-in ACP runtimes are v2-only).
assert_lc_task_success_tuple() {
  local task="$1"
  kubectl -n orka-system get task "${task}" -o json |
    jq -e '
      .status.phase == "Succeeded"
      and .status.execution.state == "Succeeded"
      and .status.execution.outcome == "Succeeded"
      and .status.delivery.state == "ReadValidated"
      and .status.delivery.outcome == "ReadValidated"
      and (.status.resultRef.available == true)
      and ((.status.jobName // "") == "")
    ' >/dev/null || {
    kubectl -n orka-system get task "${task}" -o yaml >&2 || true
    echo "Task ${task} did not settle the complete successful tuple (Succeeded/Succeeded/Succeeded + ReadValidated delivery + available result, no legacy Job)" >&2
    return 1
  }
}

# assert_lc_task_success_fence applies the complete canonical execution fence
# (mirroring assert_task_fence in live-acp-runtime-e2e) to a successful turn:
# the Task's projected pool label and identity must match the RuntimePool's
# OWN name/UID/instance/epoch exactly, with a complete prompt identity and
# exactly one attempt - a self-consistent but incomplete or mis-projected
# fence must fail the lane. The pool can legitimately drain its active
# instance moments after a turn completes (observed live: the settled pool's
# activeInstance was already cleared when this ran), so the instance
# cross-check applies only while the pool still projects one; pool
# name/UID/epoch and the complete Task-side fence are required regardless.
assert_lc_task_success_fence() {
  local task="$1"
  local fence_file="${WORK_DIR:-/tmp}/lc-success-fence-${task}.json"
  local pool_file="${WORK_DIR:-/tmp}/lc-success-pool-${task}.json"
  local fence_pool
  kubectl -n orka-system get task "${task}" -o json >"${fence_file}"
  fence_pool="$(jq -r '.status.execution.runtimePoolName // ""' "${fence_file}")"
  if [[ -z "${fence_pool}" ]]; then
    echo "Task ${task} exposes no runtimePoolName for its success fence" >&2
    return 1
  fi
  kubectl -n orka-system get runtimepool "${fence_pool}" -o json |
    jq '{poolName: .metadata.name, poolUID: .metadata.uid,
         controllerEpoch: .status.controllerEpoch,
         runtimeInstanceID: .status.activeInstance.runtimeInstanceID}' >"${pool_file}"
  jq -e --slurpfile snap "${pool_file}" '
    $snap[0] as $s
    | .status.execution as $e
    | ($s.poolUID // "" | length > 0)
      and (($s.controllerEpoch | type) == "number")
      and ($e.runtimePoolName == $s.poolName)
      and (.metadata.labels["orka.ai/runtime-pool"] == $s.poolName)
      and ($e.runtimePoolUID == $s.poolUID)
      and (($e.runtimeInstanceID // "") | length > 0)
      and (($s.runtimeInstanceID // "" | length == 0)
        or ($e.runtimeInstanceID == $s.runtimeInstanceID))
      and (($e.controllerEpoch | type) == "number")
      and ($e.controllerEpoch == $s.controllerEpoch)
      and (($e.promptID // "") | length > 0)
      and (($e.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
      and (($e.runtimeSessionUID // "") | length > 0)
      and (($e.runtimeSessionGeneration // 0) >= 1)
      and (($e.attempt // 0) == 1)
  ' "${fence_file}" >/dev/null || {
    kubectl -n orka-system get task "${task}" -o yaml >&2 || true
    cat "${pool_file}" >&2 || true
    echo "Task ${task} execution fence does not match the RuntimePool's own identity" >&2
    return 1
  }
}

apply_substrate_lifecycle_task() {
  local name="$1" session="$2" create="$3" prompt="$4" timeout="${5:-15m0s}"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: ${name}
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: ${timeout}
  sessionRef:
    name: ${session}
    create: ${create}
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "${prompt}"
YAML
}

# Start the watch before changing desiredReplicas so even short-lived barriers
# are recorded. Success requires every exact lifecycle/admission pair in order.
drain_lc_pool_to_zero() {
  local pool="$1" timeout_seconds="$2"
  local events_file="${TMP_ROOT}/${pool}-drain-events.tsv"
  local watch_log="${TMP_ROOT}/${pool}-drain-watch.log"
  local watch_pid started now
  : >"${events_file}"
  : >"${watch_log}"
  (
    kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" \
      --watch --output-watch-events --request-timeout="${timeout_seconds}s" -o json |
      jq --unbuffered -r '
        (.object // .) as $pool
        | [
            $pool.metadata.resourceVersion,
            ($pool.spec.desiredReplicas // ""),
            (($pool.status.lifecycle // "") + "/" + ($pool.status.admissionState // ""))
          ]
        | @tsv
      '
  ) >"${events_file}" 2>"${watch_log}" &
  watch_pid=$!
  started="$(date +%s)"

  while ! awk -F '\t' 'NF >= 3 { found = 1 } END { exit(found ? 0 : 1) }' "${events_file}"; do
    if ! kill -0 "${watch_pid}" 2>/dev/null; then
      wait "${watch_pid}" 2>/dev/null || true
      cat "${watch_log}" >&2 || true
      echo "RuntimePool/${pool} lifecycle watch exited before its initial snapshot" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started >= 30 )); then
      kill "${watch_pid}" 2>/dev/null || true
      wait "${watch_pid}" 2>/dev/null || true
      cat "${watch_log}" >&2 || true
      echo "timed out establishing the RuntimePool/${pool} lifecycle watch" >&2
      return 1
    fi
    sleep 1
  done

  if ! kubectl -n "${ORKA_NAMESPACE}" patch runtimepool "${pool}" --type=merge \
    -p '{"spec":{"desiredReplicas":0}}'; then
    kill "${watch_pid}" 2>/dev/null || true
    wait "${watch_pid}" 2>/dev/null || true
    return 1
  fi

  while ! awk -F '\t' '
    $2 != "0" { next }
    step == 0 && $3 == "Draining/Draining" { step = 1; next }
    step == 1 && $3 == "Quiescent/Draining" { step = 2; next }
    step == 2 && $3 == "Stopping/Closed" { step = 3; next }
    step == 3 && $3 == "Stopped/Closed" { step = 4; exit }
    END { exit(step == 4 ? 0 : 1) }
  ' "${events_file}"; do
    if ! kill -0 "${watch_pid}" 2>/dev/null; then
      wait "${watch_pid}" 2>/dev/null || true
      cat "${watch_log}" >&2 || true
      cat "${events_file}" >&2 || true
      echo "RuntimePool/${pool} lifecycle watch ended before the exact drain sequence completed" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      kill "${watch_pid}" 2>/dev/null || true
      wait "${watch_pid}" 2>/dev/null || true
      cat "${events_file}" >&2 || true
      kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o yaml >&2 || true
      echo "RuntimePool/${pool} did not traverse the exact drain sequence within ${timeout_seconds}s" >&2
      return 1
    fi
    sleep 1
  done

  kill "${watch_pid}" 2>/dev/null || true
  wait "${watch_pid}" 2>/dev/null || true
  log "RuntimePool/${pool} traversed Draining/Draining, Quiescent/Draining, Stopping/Closed, and Stopped/Closed"
}

wait_for_lc_pool_stopped() {
  local pool="$1" timeout_seconds="$2"
  local started now payload
  started="$(date +%s)"
  while true; do
    payload="$(kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o json 2>/dev/null || true)"
    if jq -e '
      .metadata.deletionTimestamp == null
      and .status.observedGeneration == .metadata.generation
      and .spec.desiredReplicas == 0
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
    ' <<<"${payload}" >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o yaml >&2 || true
      echo "RuntimePool/${pool} did not reach the exact stopped state within ${timeout_seconds}s" >&2
      return 1
    fi
    sleep 3
  done
}

wait_for_lc_substrate_runtime_zero() {
  local pool="$1" timeout_seconds="$2"
  local started now actor_json actor_ids actor_count
  started="$(date +%s)"
  while true; do
    if actor_json="$(kubectl_ate get actors -o json 2>&1)" &&
      actor_ids="$(jq -r '.actors[]?.actorId // empty' <<<"${actor_json}" 2>&1)"; then
      actor_count="$(grep -Fc "${pool}" <<<"${actor_ids}" || true)"
      if [[ "${actor_count}" == "0" ]]; then
        return 0
      fi
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      echo "RuntimePool/${pool} stopped but still has ${actor_count:-unknown} provider actor(s)" >&2
      return 1
    fi
    sleep 5
  done
}

capture_lc_running_fence() {
  local task="$1" fence_file="$2" pool_file="$3"
  local pool
  kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o json |
    jq '{poolName: .status.execution.runtimePoolName,
         poolLabel: (.metadata.labels["orka.ai/runtime-pool"] // ""),
         poolUID: .status.execution.runtimePoolUID,
         runtimeInstanceID: .status.execution.runtimeInstanceID,
         controllerEpoch: .status.execution.controllerEpoch,
         promptID: .status.execution.promptID,
         requestDigest: .status.execution.requestDigest,
         runtimeSessionUID: .status.execution.runtimeSessionUID,
         runtimeSessionGeneration: .status.execution.runtimeSessionGeneration,
         attempt: .status.execution.attempt,
         state: .status.execution.state}' >"${fence_file}"
  jq -e '
    .state == "Running"
    and (.poolName // "" | length > 0)
    and (.poolLabel == .poolName)
    and (.poolUID // "" | length > 0)
    and (.runtimeInstanceID // "" | length > 0)
    and ((.controllerEpoch | type) == "number")
    and (.promptID // "" | length > 0)
    and ((.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
    and (.runtimeSessionUID // "" | length > 0)
    and ((.runtimeSessionGeneration // 0) >= 1)
    and (.attempt == 1)
  ' "${fence_file}" >/dev/null || {
    echo "Task ${task} carries an incomplete Running execution fence" >&2
    return 1
  }
  pool="$(jq -r '.poolName' "${fence_file}")"
  kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o json |
    jq '{poolName: .metadata.name, poolUID: .metadata.uid,
         controllerEpoch: .status.controllerEpoch,
         runtimeInstanceID: .status.activeInstance.runtimeInstanceID}' >"${pool_file}"
  jq -e --slurpfile fence "${fence_file}" '
    $fence[0] as $f
    | (.poolUID // "" | length > 0)
      and (.runtimeInstanceID // "" | length > 0)
      and ((.controllerEpoch | type) == "number")
      and (.poolName == $f.poolName)
      and (.poolName == $f.poolLabel)
      and (.poolUID == $f.poolUID)
      and (.runtimeInstanceID == $f.runtimeInstanceID)
      and (.controllerEpoch == $f.controllerEpoch)
  ' "${pool_file}" >/dev/null || {
    echo "Task ${task} Running fence does not match the RuntimePool identity" >&2
    return 1
  }
}

assert_lc_timeout_from_fence() {
  local task="$1" fence_file="$2" pool_file="$3"
  kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o json |
    jq -e --slurpfile fence "${fence_file}" --slurpfile pool "${pool_file}" '
      $fence[0] as $f
      | $pool[0] as $p
      | .status.execution as $e
      | .status.phase == "Cancelled"
        and $e.state == "Cancelled"
        and $e.outcome == "Cancelled"
        and $e.reason == "TaskTimeout"
        and (($e.attempt // 0) == 1)
        and ((.status.jobName // "") == "")
        and (.metadata.labels["orka.ai/runtime-pool"] == $p.poolName)
        and ($e.runtimePoolName == $p.poolName)
        and ($e.runtimePoolUID == $p.poolUID)
        and ($e.runtimeInstanceID == $p.runtimeInstanceID)
        and ($e.controllerEpoch == $p.controllerEpoch)
        and ($e.runtimePoolName == $f.poolName)
        and ($e.runtimePoolUID == $f.poolUID)
        and ($e.runtimeInstanceID == $f.runtimeInstanceID)
        and ($e.controllerEpoch == $f.controllerEpoch)
        and ($e.promptID == $f.promptID)
        and ($e.requestDigest == $f.requestDigest)
        and ($e.runtimeSessionUID == $f.runtimeSessionUID)
        and ($e.runtimeSessionGeneration == $f.runtimeSessionGeneration)
    ' >/dev/null || {
    kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o yaml >&2 || true
    echo "Task ${task} did not settle as controller-owned TaskTimeout cancellation with its exact Running fence" >&2
    return 1
  }
}

assert_lc_ambiguous_write_outcome() {
  local task="$1" marker="$2"
  local pool_file="${TMP_ROOT}/lc-ambiguous-pool-${task}.json"
  local started now payload phase state pool pool_payload count_before count_after
  # Capture the provider-independent pool identity before waiting for terminal
  # settlement. The Task can release its live binding as soon as ambiguity is
  # projected, so a later RuntimePool read is not a reliable fence source.
  started="$(date +%s)"
  while true; do
    payload="$(kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${payload}")"
    state="$(jq -r '.status.execution.state // ""' <<<"${payload}")"
    pool="$(jq -r '.status.execution.runtimePoolName // ""' <<<"${payload}")"
    if [[ -n "${pool}" ]] &&
      pool_payload="$(kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o json 2>/dev/null)"; then
      if jq '{poolName: .metadata.name, poolUID: .metadata.uid,
              controllerEpoch: .status.controllerEpoch,
              runtimeInstanceID: .status.activeInstance.runtimeInstanceID}' \
        <<<"${pool_payload}" >"${pool_file}" &&
        jq -e '
          (.poolName // "" | length > 0)
          and (.poolUID // "" | length > 0)
          and (.runtimeInstanceID // "" | length > 0)
          and ((.controllerEpoch | type) == "number")
        ' "${pool_file}" >/dev/null; then
        break
      fi
    fi
    if [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" || "${phase}" == "Cancelled" ]]; then
      kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o yaml >&2 || true
      [[ ! -s "${pool_file}" ]] || cat "${pool_file}" >&2
      echo "ambiguous-write Task settled before its independent RuntimePool identity was captured" >&2
      return 1
    fi
    now="$(date +%s)"
    if (( now - started >= 300 )); then
      kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o yaml >&2 || true
      echo "ambiguous-write Task exposed no complete RuntimePool identity (phase=${phase:-<empty>}, state=${state:-<empty>})" >&2
      return 1
    fi
    sleep 3
  done
  started="$(date +%s)"
  while true; do
    payload="$(kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${payload}")"
    state="$(jq -r '.status.execution.state // ""' <<<"${payload}")"
    if [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" || "${phase}" == "Cancelled" ]]; then
      break
    fi
    now="$(date +%s)"
    if (( now - started >= 300 )); then
      kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o yaml >&2 || true
      echo "ambiguous-write Task did not reach a terminal state (phase=${phase:-<empty>}, state=${state:-<empty>})" >&2
      return 1
    fi
    sleep 3
  done
  jq -e --slurpfile pool "${pool_file}" '
    $pool[0] as $p
    | .status.execution as $e
    | .status.phase == "Failed"
    and $e.state == "OutcomeUnknown"
    and $e.outcome == "OutcomeUnknown"
    and $e.reason == "RuntimeLost"
    and (($e.attempt // 0) == 1)
    and ((.status.jobName // "") == "")
    and (.metadata.labels["orka.ai/runtime-pool"] == $p.poolName)
    and ($e.runtimePoolName == $p.poolName)
    and ($e.runtimePoolUID == $p.poolUID)
    and ($e.runtimeInstanceID == $p.runtimeInstanceID)
    and (($e.controllerEpoch | type) == "number")
    and ($e.controllerEpoch == $p.controllerEpoch)
    and (($e.promptID // "") | length > 0)
    and (($e.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
    and (($e.runtimeSessionUID // "") | length > 0)
    and (($e.runtimeSessionGeneration // 0) >= 1)
  ' <<<"${payload}" >/dev/null || {
    kubectl -n "${ORKA_NAMESPACE}" get task "${task}" -o yaml >&2 || true
    cat "${pool_file}" >&2 || true
    echo "ambiguous-write Task did not settle once as durable OutcomeUnknown" >&2
    return 1
  }
  count_before="$(fixture_marker_count "${marker}")"
  if [[ "${count_before}" != "0" ]]; then
    echo "ambiguous-write prompt reached the provider fixture ${count_before} time(s); want zero before acknowledgement" >&2
    return 1
  fi
  sleep 10
  count_after="$(fixture_marker_count "${marker}")"
  if [[ "${count_after}" != "0" ]]; then
    echo "ambiguous-write prompt was replayed to the provider fixture (${count_before} -> ${count_after})" >&2
    return 1
  fi
}

assert_lc_substrate_replacement_identity() {
  local pool="$1" pool_uid="$2" task_instance="$3" prior_instance="$4"
  local pool_file="${TMP_ROOT}/orka-ws-lc-replaced-pool.json"
  local actors_file="${TMP_ROOT}/orka-ws-lc-replaced-actors.json"
  local template_file="${TMP_ROOT}/orka-ws-lc-replaced-template.json"
  local worker_file="${TMP_ROOT}/orka-ws-lc-replaced-worker.json"
  local actor_id template_namespace template_name worker_namespace worker_name

  kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o json >"${pool_file}"
  actor_id="$(jq -r '.metadata.annotations["orka.ai/substrate-actor-booted"] // ""' "${pool_file}")"
  worker_namespace="$(jq -r '
    (.metadata.annotations["orka.ai/substrate-actor-worker-pod-fence"] // "") as $raw
    | (try ($raw | fromjson) catch {})
    | .namespace // ""
  ' "${pool_file}")"
  worker_name="$(jq -r '
    (.metadata.annotations["orka.ai/substrate-actor-worker-pod-fence"] // "") as $raw
    | (try ($raw | fromjson) catch {})
    | .name // ""
  ' "${pool_file}")"
  [[ -n "${actor_id}" && -n "${worker_namespace}" && -n "${worker_name}" ]] || {
    echo "replacement pool ${pool} carries no complete Actor and worker Pod fence" >&2
    return 1
  }

  kubectl_ate get actors -o json >"${actors_file}"
  template_namespace="$(jq -r --arg actor "${actor_id}" '
    [.actors[]? | select(.actorId == $actor)]
    | if length == 1 then .[0].actorTemplateNamespace // "" else "" end
  ' "${actors_file}")"
  template_name="$(jq -r --arg actor "${actor_id}" '
    [.actors[]? | select(.actorId == $actor)]
    | if length == 1 then .[0].actorTemplateName // "" else "" end
  ' "${actors_file}")"
  [[ -n "${template_namespace}" && -n "${template_name}" ]] || {
    echo "replacement pool ${pool} does not have exactly one live provider Actor ${actor_id}" >&2
    return 1
  }

  kubectl -n "${template_namespace}" get actortemplates.ate.dev "${template_name}" \
    -o json >"${template_file}"
  kubectl -n "${worker_namespace}" get pod "${worker_name}" -o json >"${worker_file}"

  jq -e -n \
    --arg pool "${pool}" \
    --arg poolUID "${pool_uid}" \
    --arg actorID "${actor_id}" \
    --arg taskInstance "${task_instance}" \
    --arg priorInstance "${prior_instance}" \
    --arg routeSuffix ".actors.resources.substrate.ate.dev" \
    --slurpfile pools "${pool_file}" \
    --slurpfile actors "${actors_file}" \
    --slurpfile templates "${template_file}" \
    --slurpfile workers "${worker_file}" '
      ($pools[0]) as $runtimePool
      | (($runtimePool.metadata.annotations["orka.ai/substrate-actor-worker-pod-fence"] // "") | fromjson) as $workerFence
      | (($runtimePool.metadata.annotations["orka.ai/substrate-actor-worker-placement"] // "") | fromjson) as $placement
      | ($actors[0].actors | map(select(.actorId == $actorID))) as $matchingActors
      | ($matchingActors[0]) as $actor
      | ($templates[0]) as $template
      | ($workers[0]) as $worker
      | ($runtimePool.status.activeInstance) as $active
      | ($matchingActors | length) == 1
        and $runtimePool.metadata.name == $pool
        and $runtimePool.metadata.uid == $poolUID
        and $runtimePool.metadata.annotations["orka.ai/substrate-actor-booted"] == $actorID
        and $runtimePool.metadata.annotations["orka.ai/substrate-actor-credential-seeded"] == $actorID
        and (($runtimePool.metadata.annotations["orka.ai/substrate-actor-recycling"] // "") == "")
        and (($runtimePool.metadata.annotations["orka.ai/substrate-actor-replacement-worker-pod-fence"] // "") == "")
        and $runtimePool.status.lifecycle == "Serving"
        and (
          $runtimePool.status.admissionState == "Accepting"
          or (
            $runtimePool.status.admissionState == "Closed"
            and ($runtimePool.status.capacity.residentSessions // 0)
              >= $runtimePool.spec.capacity.maxResidentSessions
          )
        )
        and $actor.actorTemplateNamespace == $template.metadata.namespace
        and $actor.actorTemplateName == $template.metadata.name
        and $actor.ateomPodNamespace == $workerFence.namespace
        and $actor.ateomPodName == $workerFence.name
        and $workerFence.actorID == $actorID
        and $workerFence.uid == $worker.metadata.uid
        and $workerFence.namespace == $worker.metadata.namespace
        and $workerFence.name == $worker.metadata.name
        and $placement.namespace == $worker.metadata.namespace
        and ($placement.workerPool // "" | length > 0)
        and $worker.metadata.labels["ate.dev/worker-pool"] == $placement.workerPool
        and $worker.metadata.deletionTimestamp == null
        and $template.metadata.labels["orka.ai/runtime-pool-name"] == $pool
        and $template.metadata.labels["orka.ai/runtime-pool-uid"] == $poolUID
        and ($template.metadata.uid // "" | length > 0)
        and $active.podName == $worker.metadata.name
        and $active.podAddress == ($actorID + $routeSuffix)
        and ($active.podUID | test("^workspace:[a-f0-9]{64}$"))
        and ($active.bootID // "" | length > 0)
        and $active.runtimeInstanceID == ($active.podUID + "." + $active.bootID)
        and $active.runtimeInstanceID == $taskInstance
        and $active.runtimeInstanceID != $priorInstance
    ' >/dev/null || {
    kubectl -n "${ORKA_NAMESPACE}" get runtimepool "${pool}" -o yaml >&2 || true
    echo "replacement pool ${pool} is not fenced to its exact provider Actor, route, worker placement, and new boot identity" >&2
    return 1
  }
}

# exercise_workspace_lifecycle_acp_task proves issue #411 through Substrate:
# continuation, authenticated drain and recovery from zero, timeout and
# explicit cancellation of Running prompts, controller restart without replay,
# physical RuntimePool replacement, and exact cleanup while preserving the
# logical Session across every continuation.
exercise_workspace_lifecycle_acp_task() {
  log "Running workspace-backed lifecycle/recovery conformance (Substrate)"

  # OutcomeUnknown deliberately makes a Session non-deletable because prompt
  # delivery cannot be proven absent. Use fresh Sessions for those cases on
  # each run and exclude them from fixed-name reset cleanup.
  local outcome_unknown_session_suffix="$(date -u +%s)-${RANDOM}"
  local ambiguous_session="orka-ws-lc-ambiguous-${outcome_unknown_session_suffix}"
  local restart_session="orka-ws-lc-restart-${outcome_unknown_session_suffix}"
  local restart_session_deletable=0

  # Marker observations live in the fixture process. A reused cluster and
  # fixed image tag can otherwise retain both stale counters and old code.
  log "Restarting the Responses fixture to reset marker observations"
  kubectl -n vekil-system set image deployment/vekil "responses=${responses_fixture_image}"
  kubectl -n vekil-system rollout restart deployment/vekil
  kubectl -n vekil-system rollout status deployment/vekil --timeout=3m
  if [[ -n "${FIXTURE_PORT_FORWARD_PID}" ]]; then
    stop_port_forward "${FIXTURE_PORT_FORWARD_PID}"
    FIXTURE_PORT_FORWARD_PID=""
  fi

  log "Recycling the Substrate worker fleet for fresh workers"
  local worker_pods
  worker_pods="$(kubectl -n ate-demo get pods -o name | grep '^pod/orka-workers-deployment-' || true)"
  if [[ -n "${worker_pods}" ]]; then
    # shellcheck disable=SC2086
    kubectl -n ate-demo delete ${worker_pods} --wait=true --timeout=5m
  fi
  kubectl -n ate-demo rollout status deployment/orka-workers-deployment --timeout=5m
  wait_worker_count_at_least 4 300
  start_fixture_port_forward

  # Capture every pool still discoverable from fixed lifecycle Tasks plus the
  # persisted ledger before deleting Tasks. Cancellation and final cleanup
  # intentionally delete Tasks before pools, so either source may be the only
  # remaining pool identity after an interrupted run.
  local reset_restart_json reset_restart_session=""
  reset_restart_json="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-restart -o json 2>/dev/null || true)"
  if [[ -n "${reset_restart_json}" ]]; then
    reset_restart_session="$(restart_session_name_if_deletable <<<"${reset_restart_json}")"
  fi
  local reset_lc_task reset_lc_pool reset_lc_pools=""
  for reset_lc_task in orka-ws-lc-first orka-ws-lc-second orka-ws-lc-drained \
    orka-ws-lc-timeout orka-ws-lc-cancel orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced; do
    reset_lc_pool="$(kubectl -n "${ORKA_NAMESPACE}" get task "${reset_lc_task}" \
      -o jsonpath='{.status.execution.runtimePoolName}' 2>/dev/null || true)"
    if [[ -n "${reset_lc_pool}" && " ${reset_lc_pools} " != *" ${reset_lc_pool} "* ]]; then
      reset_lc_pools="${reset_lc_pools} ${reset_lc_pool}"
    fi
  done
  local reset_recorded_pools
  reset_recorded_pools="$(kubectl -n "${ORKA_NAMESPACE}" get configmap orka-ws-lc-pools \
    -o jsonpath='{.data.pools}' 2>/dev/null || true)"
  for reset_lc_pool in ${reset_recorded_pools}; do
    if [[ " ${reset_lc_pools} " != *" ${reset_lc_pool} "* ]]; then
      reset_lc_pools="${reset_lc_pools} ${reset_lc_pool}"
    fi
  done

  # No controller owns the test-only observer finalizer. Strip a stale copy
  # before the waited Task deletion so reset cannot hang on an interrupted
  # cancellation assertion.
  if kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-cancel >/dev/null 2>&1; then
    local reset_cancel_json reset_cancel_index reset_cancel_attempt reset_cancel_ok
    reset_cancel_ok=0
    for reset_cancel_attempt in 1 2 3 4 5; do
      reset_cancel_json="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-cancel -o json 2>/dev/null || true)"
      [[ -n "${reset_cancel_json}" ]] || { reset_cancel_ok=1; break; }
      reset_cancel_index="$(jq -r '(.metadata.finalizers // []) | index("acp-e2e.orka.ai/lifecycle-observer") // empty' <<<"${reset_cancel_json}")"
      if [[ -z "${reset_cancel_index}" ]]; then
        reset_cancel_ok=1
        break
      fi
      if kubectl -n "${ORKA_NAMESPACE}" patch task orka-ws-lc-cancel --type=json \
        -p "[{\"op\":\"test\",\"path\":\"/metadata/finalizers/${reset_cancel_index}\",\"value\":\"acp-e2e.orka.ai/lifecycle-observer\"},{\"op\":\"remove\",\"path\":\"/metadata/finalizers/${reset_cancel_index}\"}]" >/dev/null; then
        reset_cancel_ok=1
        break
      fi
      sleep 2
    done
    if [[ "${reset_cancel_ok}" != "1" ]]; then
      echo "could not strip the stale lifecycle-observer finalizer from orka-ws-lc-cancel" >&2
      return 1
    fi
  fi
  kubectl -n "${ORKA_NAMESPACE}" delete task \
    orka-ws-lc-first orka-ws-lc-second orka-ws-lc-drained orka-ws-lc-timeout \
    orka-ws-lc-cancel orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced \
    --ignore-not-found=true --wait=true --timeout=4m
  kubectl -n "${ORKA_NAMESPACE}" delete agent orka-ws-lc-agent \
    --ignore-not-found=true --wait=true --timeout=1m
  for reset_lc_pool in ${reset_lc_pools}; do
    kubectl -n "${ORKA_NAMESPACE}" delete runtimepool "${reset_lc_pool}" \
      --ignore-not-found=true --wait=true --timeout=5m
  done
  kubectl -n "${ORKA_NAMESPACE}" delete configmap orka-ws-lc-pools \
    --ignore-not-found=true >/dev/null 2>&1 || true

  local reset_lc_session
  for reset_lc_session in orka-ws-lc-session orka-ws-lc-timeout-session \
    orka-ws-lc-cancel-session; do
    delete_fixed_session "${reset_lc_session}"
  done
  if [[ -n "${reset_restart_session}" ]]; then
    delete_fixed_session "${reset_restart_session}"
  fi

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: orka-ws-lc-agent
  namespace: ${ORKA_NAMESPACE}
spec:
  runtime:
    type: codex
    contractVersion: orka.harness.v2
    defaultMaxTurns: 1
  model:
    name: gpt-5.5
---
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-lc-first
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-lc-session
    create: true
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "Reply exactly: ORKA_WS_LC_FIRST_OK"
YAML

  wait_jsonpath_equals \
    "lifecycle first Task phase" \
    "kubectl -n orka-system get task orka-ws-lc-first -o jsonpath='{.status.phase}'" \
    "Succeeded" 600
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-lc-first" "ORKA_WS_LC_FIRST_OK"
  assert_lc_task_success_tuple orka-ws-lc-first
  assert_lc_task_success_fence orka-ws-lc-first
  # Every lifecycle turn must reach the provider EXACTLY once: sticky
  # history/disconnect observations would otherwise hide duplicate prompt
  # delivery outside the cancellation/restart scenarios.
  if [[ "$(fixture_marker_count "ORKA_WS_LC_FIRST_OK")" != "1" ]]; then
    echo "first lifecycle turn was delivered $(fixture_marker_count "ORKA_WS_LC_FIRST_OK") times; want exactly one" >&2
    return 1
  fi
  if [[ "$(fixture_marker_saw_history "ORKA_WS_LC_FIRST_OK")" != "false" ]]; then
    echo "first lifecycle turn unexpectedly carried prior session history" >&2
    return 1
  fi

  local pool_name pool_uid session_uid first_instance
  pool_name="$(kubectl -n orka-system get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${pool_name}"
  pool_uid="$(kubectl -n orka-system get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  session_uid="$(kubectl -n orka-system get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  first_instance="$(kubectl -n orka-system get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  if [[ "${pool_name}" != acp-ws-session-* || -z "${pool_uid}" || -z "${session_uid}" || -z "${first_instance}" ]]; then
    kubectl -n orka-system get task orka-ws-lc-first -o yaml >&2 || true
    echo "lifecycle Task did not bind a session workspace pool with runtime identities (pool=${pool_name:-<empty>})" >&2
    return 1
  fi

  log "Continuing the Session on the same physical runtime"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-lc-second
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-lc-session
    create: false
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "Reply exactly: ORKA_WS_LC_SECOND_OK"
YAML
  wait_jsonpath_equals \
    "lifecycle continuation Task phase" \
    "kubectl -n orka-system get task orka-ws-lc-second -o jsonpath='{.status.phase}'" \
    "Succeeded" 600
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-lc-second" "ORKA_WS_LC_SECOND_OK"
  assert_lc_task_success_tuple orka-ws-lc-second
  assert_lc_task_success_fence orka-ws-lc-second
  local second_session second_instance second_pool second_pool_uid
  second_session="$(kubectl -n orka-system get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  second_instance="$(kubectl -n orka-system get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  second_pool="$(kubectl -n orka-system get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  second_pool_uid="$(kubectl -n orka-system get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  [[ "${second_session}" == "${session_uid}" ]] || {
    echo "continuation changed the RuntimeSession UID (${second_session:-<empty>} != ${session_uid})" >&2
    return 1
  }
  # Session reuse must retain the SAME dedicated workspace pool: a second
  # Task selecting or creating another pool would both duplicate the
  # workspace and leak an untracked pool.
  [[ "${second_pool}" == "${pool_name}" && "${second_pool_uid}" == "${pool_uid}" ]] || {
    echo "continuation moved to a different workspace pool (${second_pool:-<empty>}/${second_pool_uid:-<empty>} != ${pool_name}/${pool_uid})" >&2
    return 1
  }
  # Semantic continuation proof: the fixture must have seen the replayed
  # session history in the continuation request, not just a fresh prompt that
  # happens to carry its own marker.
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_SECOND_OK")" == "true" ]] || {
    echo "continuation request carried no prior session history; the runtime silently started a fresh session" >&2
    return 1
  }
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_SECOND_OK" "ORKA_WS_LC_FIRST_OK")" == "true" ]] || {
    echo "continuation history did not replay the expected first turn; an unrelated or truncated transcript was accepted" >&2
    return 1
  }
  if [[ "$(fixture_marker_count "ORKA_WS_LC_SECOND_OK")" != "1" ]]; then
    echo "continuation turn was delivered $(fixture_marker_count "ORKA_WS_LC_SECOND_OK") times; want exactly one" >&2
    return 1
  fi
  # The physical instance may legitimately change between turns (the pool can
  # scale to zero while idle); the contract requires the logical Session to
  # survive, which the UID equality above proves. The session generation
  # fences the provider RuntimeSession, not the physical runtime. Transparent
  # reuse preserves it, an in-place RuntimeSession recreation may advance it,
  # and a fresh physical runtime must advance it.
  local first_generation second_generation
  first_generation="$(kubectl -n orka-system get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  second_generation="$(kubectl -n orka-system get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  if [[ ! "${first_generation}" =~ ^[0-9]+$ || ! "${second_generation}" =~ ^[0-9]+$ ]]; then
    echo "lifecycle turns carry no valid runtimeSessionGeneration (first=${first_generation:-<empty>} second=${second_generation:-<empty>})" >&2
    return 1
  fi
  if [[ "${second_instance}" == "${first_instance}" ]]; then
    if (( second_generation < first_generation )); then
      echo "continuation on the same instance regressed the session generation (${first_generation} -> ${second_generation})" >&2
      return 1
    fi
    if [[ "${second_generation}" == "${first_generation}" ]]; then
      log "Continuation reused the RuntimeSession on the same physical runtime instance"
    else
      log "Continuation recreated the RuntimeSession on the same physical runtime instance (${first_generation} -> ${second_generation})"
    fi
  else
    if (( second_generation <= first_generation )); then
      echo "recovery on a fresh instance did not advance the session generation (${first_generation} -> ${second_generation})" >&2
      return 1
    fi
    log "Continuation recovered the Session on a fresh physical runtime instance"
  fi

  log "Draining the Session pool to zero through every authenticated lifecycle barrier"
  drain_lc_pool_to_zero "${pool_name}" 600
  wait_for_lc_pool_stopped "${pool_name}" 600
  wait_for_lc_substrate_runtime_zero "${pool_name}" 300

  log "Recovering the same logical Session from the stopped pool"
  apply_substrate_lifecycle_task orka-ws-lc-drained orka-ws-lc-session false \
    "Reply exactly: ORKA_WS_LC_DRAINED_OK"
  wait_jsonpath_equals \
    "post-drain continuation phase" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-drained -o jsonpath='{.status.phase}'" \
    "Succeeded" 900
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-lc-drained" "ORKA_WS_LC_DRAINED_OK"
  assert_lc_task_success_tuple orka-ws-lc-drained
  assert_lc_task_success_fence orka-ws-lc-drained
  local drained_session drained_instance drained_pool drained_pool_uid drained_generation
  drained_session="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  drained_instance="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  drained_pool="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  drained_pool_uid="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  drained_generation="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${drained_session}" == "${session_uid}" ]] || {
    echo "scale-to-zero recovery changed the RuntimeSession UID" >&2
    return 1
  }
  [[ "${drained_pool}" == "${pool_name}" && "${drained_pool_uid}" == "${pool_uid}" ]] || {
    echo "scale-to-zero recovery replaced the logical RuntimePool" >&2
    return 1
  }
  [[ -n "${drained_instance}" && "${drained_instance}" != "${second_instance}" ]] || {
    echo "scale-to-zero recovery reused the stopped runtime instance" >&2
    return 1
  }
  if ! [[ "${drained_generation}" =~ ^[0-9]+$ && "${drained_generation}" -gt "${second_generation}" ]]; then
    echo "scale-to-zero recovery did not advance the session generation" >&2
    return 1
  fi
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_DRAINED_OK")" == "true" ]] || {
    echo "scale-to-zero continuation carried no prior session history" >&2
    return 1
  }
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_DRAINED_OK" "ORKA_WS_LC_SECOND_OK")" == "true" ]] || {
    echo "scale-to-zero history did not replay the expected prior turn" >&2
    return 1
  }
  if [[ "$(fixture_marker_count "ORKA_WS_LC_DRAINED_OK")" != "1" ]]; then
    echo "scale-to-zero turn did not reach the provider exactly once" >&2
    return 1
  fi

  log "Timing out a Running prompt only after its configured deadline"
  local timeout_started timeout_pool timeout_fence_file timeout_pool_snapshot
  local timeout_hold_started timeout_hold_now timeout_hold_count timeout_elapsed
  timeout_started="$(date +%s)"
  apply_substrate_lifecycle_task orka-ws-lc-timeout orka-ws-lc-timeout-session true \
    "ORKA_HOLD_300S Reply exactly: ORKA_WS_LC_TIMEOUT_OK" "4m0s"
  wait_jsonpath_equals \
    "timeout Task Running state" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.execution.state}'" \
    "Running" 600
  timeout_pool="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-timeout \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${timeout_pool}"
  timeout_fence_file="${TMP_ROOT}/orka-ws-lc-timeout-fence.json"
  timeout_pool_snapshot="${TMP_ROOT}/orka-ws-lc-timeout-pool.json"
  capture_lc_running_fence orka-ws-lc-timeout "${timeout_fence_file}" "${timeout_pool_snapshot}"
  timeout_hold_started="$(date +%s)"
  while true; do
    timeout_hold_count="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
    [[ "${timeout_hold_count}" =~ ^[0-9]+$ && "${timeout_hold_count}" -ge 1 ]] && break
    timeout_hold_now="$(date +%s)"
    if (( timeout_hold_now - timeout_hold_started >= 180 )); then
      echo "held timeout prompt never reached the provider fixture" >&2
      return 1
    fi
    sleep 3
  done
  [[ "${timeout_hold_count}" == "1" ]] || {
    echo "held timeout prompt was delivered ${timeout_hold_count} times before timeout; want exactly one" >&2
    return 1
  }
  wait_jsonpath_equals \
    "timed-out Task phase" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.phase}'" \
    "Cancelled" 420
  wait_jsonpath_equals \
    "timed-out Task execution state" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.execution.state}'" \
    "Cancelled" 120
  wait_jsonpath_equals \
    "timed-out Task execution outcome" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.execution.outcome}'" \
    "Cancelled" 120
  wait_jsonpath_equals \
    "timed-out Task execution reason" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.execution.reason}'" \
    "TaskTimeout" 120
  wait_jsonpath_equals \
    "timed-out Task execution attempt" \
    "kubectl -n ${ORKA_NAMESPACE} get task orka-ws-lc-timeout -o jsonpath='{.status.execution.attempt}'" \
    "1" 60
  assert_lc_timeout_from_fence orka-ws-lc-timeout "${timeout_fence_file}" "${timeout_pool_snapshot}"
  timeout_elapsed=$(( $(date +%s) - timeout_started ))
  if (( timeout_elapsed < 240 )); then
    echo "timeout Task cancelled after ${timeout_elapsed}s, before its configured 4m0s deadline" >&2
    return 1
  fi
  local timeout_disconnect_started timeout_disconnect_now timeout_disconnects
  timeout_disconnect_started="$(date +%s)"
  while true; do
    timeout_disconnects="$(fixture_marker_disconnects "ORKA_WS_LC_TIMEOUT_OK")"
    if [[ "${timeout_disconnects}" =~ ^[0-9]+$ && "${timeout_disconnects}" -ge 1 ]]; then
      break
    fi
    timeout_disconnect_now="$(date +%s)"
    if (( timeout_disconnect_now - timeout_disconnect_started >= 120 )); then
      echo "timeout never closed the in-flight provider stream" >&2
      return 1
    fi
    sleep 3
  done
  local timeout_count_settled timeout_count_later
  timeout_count_settled="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
  [[ "${timeout_count_settled}" == "1" ]] || {
    echo "timed-out prompt did not reach the provider exactly once" >&2
    return 1
  }
  sleep 20
  timeout_count_later="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
  [[ "${timeout_count_later}" == "1" ]] || {
    echo "timed-out prompt was replayed after settlement" >&2
    return 1
  }
  log "Timed-out prompt settled after ${timeout_elapsed}s with no replay and a closed provider stream"

  log "Cancelling a Running prompt in a dedicated Session"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-lc-cancel
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-lc-cancel-session
    create: true
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "ORKA_HOLD_180S Reply exactly: ORKA_WS_LC_CANCEL_OK"
YAML
  wait_jsonpath_equals \
    "cancellation Task Running state" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.execution.state}'" \
    "Running" 480
  local cancel_pool
  cancel_pool="$(kubectl -n orka-system get task orka-ws-lc-cancel \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${cancel_pool}"
  # Cancel only once the held model request is actually in flight at the
  # fixture: an accepted-but-not-yet-issued prompt would make the no-replay
  # count vacuously zero.
  local hold_started hold_now hold_count
  hold_started="$(date +%s)"
  while true; do
    hold_count="$(fixture_marker_count "ORKA_WS_LC_CANCEL_OK")"
    [[ "${hold_count}" =~ ^[0-9]+$ && "${hold_count}" -ge 1 ]] && break
    hold_now="$(date +%s)"
    if (( hold_now - hold_started >= 180 )); then
      echo "held cancellation prompt never reached the provider fixture" >&2
      return 1
    fi
    sleep 3
  done
  [[ "${hold_count}" == "1" ]] || {
    echo "held cancellation prompt was delivered ${hold_count} times before cancellation; want exactly one" >&2
    return 1
  }
  # Hold the object visible through settlement so the terminal projection is
  # observable after deletion-triggered cancellation.
  kubectl -n orka-system patch task orka-ws-lc-cancel --type=json \
    -p '[{"op":"add","path":"/metadata/finalizers/-","value":"acp-e2e.orka.ai/lifecycle-observer"}]'
  # Capture the complete pre-delete execution fence so settlement can be
  # proven to preserve the exact controller-owned identity, not just the
  # terminal state tuple.
  local cancel_fence_file="${WORK_DIR:-/tmp}/orka-ws-lc-cancel-fence.json"
  kubectl -n orka-system get task orka-ws-lc-cancel -o json |
    jq '{poolName: .status.execution.runtimePoolName,
         poolLabel: (.metadata.labels["orka.ai/runtime-pool"] // ""),
         poolUID: .status.execution.runtimePoolUID,
         runtimeInstanceID: .status.execution.runtimeInstanceID, controllerEpoch: .status.execution.controllerEpoch,
         promptID: .status.execution.promptID, requestDigest: .status.execution.requestDigest,
         runtimeSessionUID: .status.execution.runtimeSessionUID,
         runtimeSessionGeneration: .status.execution.runtimeSessionGeneration,
         state: .status.execution.state,
         attempt: .status.execution.attempt}' >"${cancel_fence_file}"
  jq -e '
    .state == "Running"
    and (.attempt == 1)
    and (.poolName // "" | length > 0)
    and (.poolLabel // "" | length > 0)
    and (.poolUID // "" | length > 0)
    and (.runtimeInstanceID // "" | length > 0)
    and ((.controllerEpoch | type) == "number")
    and (.promptID // "" | length > 0)
    and ((.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
    and (.runtimeSessionUID // "" | length > 0)
    and ((.runtimeSessionGeneration // 0) >= 1)
  ' "${cancel_fence_file}" >/dev/null || {
    echo "cancellation Task carries an incomplete execution fence before deletion" >&2
    cat "${cancel_fence_file}" >&2 || true
    return 1
  }
  # The Task-side fence alone would let an incorrectly projected identity
  # self-compare into a pass: snapshot the RuntimePool's own identity as the
  # independent source and require the Task fence to match it BEFORE the
  # deletion; the settled Task is then compared against this snapshot too.
  local cancel_pool_snapshot="${WORK_DIR:-/tmp}/orka-ws-lc-cancel-pool.json"
  local cancel_fence_pool
  cancel_fence_pool="$(jq -r '.poolName' "${cancel_fence_file}")"
  kubectl -n orka-system get runtimepool "${cancel_fence_pool}" -o json |
    jq '{poolName: .metadata.name, poolUID: .metadata.uid,
         controllerEpoch: .status.controllerEpoch,
         runtimeInstanceID: .status.activeInstance.runtimeInstanceID}' >"${cancel_pool_snapshot}"
  jq -e --slurpfile fence "${cancel_fence_file}" '
    $fence[0] as $f
    | (.poolUID // "" | length > 0)
      and (.runtimeInstanceID // "" | length > 0)
      and ((.controllerEpoch | type) == "number")
      and (.poolName == $f.poolName)
      and (.poolName == $f.poolLabel)
      and (.poolUID == $f.poolUID)
      and (.runtimeInstanceID == $f.runtimeInstanceID)
      and ($f.controllerEpoch == .controllerEpoch)
  ' "${cancel_pool_snapshot}" >/dev/null || {
    echo "pre-cancellation Task fence does not match the RuntimePool's own identity" >&2
    return 1
  }
  kubectl -n orka-system get task orka-ws-lc-cancel -o json |
    jq -e --slurpfile fence "${cancel_fence_file}" '
      $fence[0] as $f
      | .status.execution as $e
      | $e.state == "Running"
        and $e.attempt == 1
        and ($e.runtimePoolName == $f.poolName)
        and ($e.runtimePoolUID == $f.poolUID)
        and ($e.runtimeInstanceID == $f.runtimeInstanceID)
        and ($e.promptID == $f.promptID)
        and ($e.requestDigest == $f.requestDigest)
    ' >/dev/null || {
    echo "cancellation Task left its first Running attempt before deletion" >&2
    return 1
  }
  kubectl -n orka-system delete task orka-ws-lc-cancel --wait=false
  wait_jsonpath_equals \
    "cancelled Task phase" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.phase}'" \
    "Cancelled" 240
  wait_jsonpath_equals \
    "cancelled Task execution state" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.execution.state}'" \
    "Cancelled" 120
  # The canonical cancellation contract (live-acp-runtime-e2e) requires the
  # full controller-owned settlement tuple, not just phase and state.
  wait_jsonpath_equals \
    "cancelled Task execution outcome" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.execution.outcome}'" \
    "Cancelled" 120
  wait_jsonpath_equals \
    "cancelled Task execution reason" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.execution.reason}'" \
    "Cancelled" 120
  # The canonical cancellation fence also requires exactly one execution
  # attempt: a rejected controller-side replay leaves the fixture count at
  # one while replay still happened.
  wait_jsonpath_equals \
    "cancelled Task execution attempt" \
    "kubectl -n orka-system get task orka-ws-lc-cancel -o jsonpath='{.status.execution.attempt}'" \
    "1" 60
  # And the complete pre-delete execution identity must be preserved,
  # validated against BOTH the Task-side fence and the independent
  # RuntimePool snapshot.
  kubectl -n orka-system get task orka-ws-lc-cancel -o json |
    jq -e --slurpfile snap "${cancel_pool_snapshot}" '
      $snap[0] as $s
      | .status.execution as $e
      | (.metadata.labels["orka.ai/runtime-pool"] == $s.poolName)
        and ($e.runtimePoolName == $s.poolName)
        and ($e.runtimePoolUID == $s.poolUID)
        and ($e.runtimeInstanceID == $s.runtimeInstanceID)
        and (($e.controllerEpoch | type) == "number")
        and ($e.controllerEpoch == $s.controllerEpoch)
        and ((.status.jobName // "") == "")
    ' >/dev/null || {
    echo "cancellation settlement does not match the independent RuntimePool identity" >&2
    kubectl -n orka-system get task orka-ws-lc-cancel -o yaml >&2 || true
    return 1
  }
  kubectl -n orka-system get task orka-ws-lc-cancel -o json |
    jq -e --slurpfile fence "${cancel_fence_file}" '
      $fence[0] as $f
      | .status.execution as $e
      | (.metadata.labels["orka.ai/runtime-pool"] == $f.poolLabel)
        and ($e.runtimePoolName == $f.poolName)
        and ($e.runtimePoolUID == $f.poolUID)
        and ($e.runtimeInstanceID == $f.runtimeInstanceID)
        and (($e.controllerEpoch | type) == "number")
        and ($e.controllerEpoch == $f.controllerEpoch)
        and ($e.promptID == $f.promptID)
        and ($e.requestDigest == $f.requestDigest)
        and ($e.runtimeSessionUID == $f.runtimeSessionUID)
      and (($e.runtimeSessionGeneration // 0) >= 1)
      and ($e.runtimeSessionGeneration == $f.runtimeSessionGeneration)
    ' >/dev/null || {
    echo "cancellation settlement did not preserve the exact pre-delete execution fence" >&2
    kubectl -n orka-system get task orka-ws-lc-cancel -o yaml >&2 || true
    return 1
  }
  # Release the observer only after the controller's own cleanup finalizer has
  # completed and removed itself, so cancellation cleanup is never skipped.
  # kubectl's jsonpath stringifies arrays with fmt (no quotes), so the
  # finalizer list is parsed from -o json with jq like the canonical
  # task_observer_release_ready helper does.
  local release_started release_now cancel_task_json
  release_started="$(date +%s)"
  while true; do
    if ! cancel_task_json="$(kubectl -n orka-system get task orka-ws-lc-cancel -o json 2>/dev/null)"; then
      break
    fi
    if jq -e '(.metadata.finalizers // []) | length == 0' <<<"${cancel_task_json}" >/dev/null; then
      break
    fi
    if jq -e '(.metadata.finalizers // []) == ["acp-e2e.orka.ai/lifecycle-observer"]' <<<"${cancel_task_json}" >/dev/null; then
      # Leave the loop only after the release actually applied: a transient
      # patch failure would otherwise break out with the observer finalizer
      # still held, and the following absence wait would time out even
      # though controller cleanup behaved correctly.
      if kubectl -n orka-system patch task orka-ws-lc-cancel --type=json \
        -p '[{"op":"test","path":"/metadata/finalizers/0","value":"acp-e2e.orka.ai/lifecycle-observer"},{"op":"remove","path":"/metadata/finalizers/0"}]' >/dev/null; then
        break
      fi
    fi
    release_now="$(date +%s)"
    if (( release_now - release_started >= 300 )); then
      echo "controller cleanup did not settle for the cancelled Task (finalizers=$(jq -c '.metadata.finalizers // []' <<<"${cancel_task_json}"))" >&2
      return 1
    fi
    sleep 3
  done
  wait_resource_absent "orka-system" "task" "orka-ws-lc-cancel" 240
  # No-replay proof: the fixture request count for the cancelled prompt must
  # not grow after settlement (a replay would re-deliver the prompt and issue
  # a fresh provider request).
  local cancel_count_settled cancel_count_later
  cancel_count_settled="$(fixture_marker_count "ORKA_WS_LC_CANCEL_OK")"
  [[ "${cancel_count_settled}" == "1" ]] || {
    echo "cancelled prompt must reach the provider fixture exactly once (count=${cancel_count_settled:-<empty>})" >&2
    return 1
  }
  sleep 20
  cancel_count_later="$(fixture_marker_count "ORKA_WS_LC_CANCEL_OK")"
  [[ "${cancel_count_later}" == "1" ]] || {
    echo "cancelled prompt was replayed after settlement (${cancel_count_settled} -> ${cancel_count_later})" >&2
    return 1
  }
  # Stream-closure proof: the fixture's held request must have observed the
  # client disconnect - a cancelled turn that leaves its provider HTTP stream
  # open would report zero disconnects while every other check passes.
  local cancel_disconnect_started cancel_disconnect_now cancel_disconnects
  cancel_disconnect_started="$(date +%s)"
  while true; do
    cancel_disconnects="$(fixture_marker_disconnects "ORKA_WS_LC_CANCEL_OK")"
    [[ "${cancel_disconnects}" =~ ^[0-9]+$ && "${cancel_disconnects}" -ge 1 ]] && break
    cancel_disconnect_now="$(date +%s)"
    if (( cancel_disconnect_now - cancel_disconnect_started >= 120 )); then
      echo "cancellation never closed the in-flight provider stream (fixture disconnects=${cancel_disconnects:-0})" >&2
      return 1
    fi
    sleep 3
  done
  log "Cancelled prompt settled with no replay and a closed provider stream (fixture requests: ${cancel_count_settled})"
  if [[ -n "${cancel_pool}" ]]; then
    kubectl -n orka-system delete runtimepool "${cancel_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi

  log "Forcing the prompt request-write/ack boundary into an ambiguous state"
  apply_substrate_lifecycle_task orka-ws-lc-ambiguous "${ambiguous_session}" true \
    "Reply exactly: ${LIFECYCLE_AMBIGUITY_MARKER}"
  assert_lc_ambiguous_write_outcome orka-ws-lc-ambiguous "${LIFECYCLE_AMBIGUITY_MARKER}"
  local ambiguous_pool
  ambiguous_pool="$(kubectl -n "${ORKA_NAMESPACE}" get task orka-ws-lc-ambiguous \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  if [[ -z "${ambiguous_pool}" ]]; then
    echo "ambiguous-write Task carries no RuntimePool identity" >&2
    return 1
  fi
  record_lc_pool "${ambiguous_pool}"
  log "Ambiguous prompt write settled durably as OutcomeUnknown without provider delivery"

  log "Restarting the controller while a prompt is Running"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-lc-restart
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: ${restart_session}
    create: true
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "ORKA_HOLD_90S Reply exactly: ORKA_WS_LC_RESTART_OK"
YAML
  wait_jsonpath_equals \
    "restart Task Running state" \
    "kubectl -n orka-system get task orka-ws-lc-restart -o jsonpath='{.status.execution.state}'" \
    "Running" 480
  # Restart only once the held model request is in flight so the
  # before/after fixture counts prove the accepted request was not replayed.
  local restart_count_before restart_count_after restart_hold_started restart_hold_now
  restart_hold_started="$(date +%s)"
  while true; do
    restart_count_before="$(fixture_marker_count "ORKA_WS_LC_RESTART_OK")"
    [[ "${restart_count_before}" =~ ^[0-9]+$ && "${restart_count_before}" -ge 1 ]] && break
    restart_hold_now="$(date +%s)"
    if (( restart_hold_now - restart_hold_started >= 180 )); then
      echo "held restart prompt never reached the provider fixture" >&2
      return 1
    fi
    sleep 3
  done
  [[ "${restart_count_before}" == "1" ]] || {
    echo "held restart prompt was delivered ${restart_count_before} times before the restart; want exactly one" >&2
    return 1
  }
  # Capture the execution fence before the restart: takeover must settle the
  # SAME prompt against the SAME pool, instance, and RuntimeSession identity.
  local restart_fence_file="${WORK_DIR:-/tmp}/orka-ws-lc-restart-fence.json"
  kubectl -n orka-system get task orka-ws-lc-restart -o json |
    jq '{poolName: .status.execution.runtimePoolName,
         poolLabel: (.metadata.labels["orka.ai/runtime-pool"] // ""),
         poolUID: .status.execution.runtimePoolUID,
         runtimeInstanceID: .status.execution.runtimeInstanceID, controllerEpoch: .status.execution.controllerEpoch,
         promptID: .status.execution.promptID, requestDigest: .status.execution.requestDigest,
         runtimeSessionUID: .status.execution.runtimeSessionUID,
         runtimeSessionGeneration: .status.execution.runtimeSessionGeneration,
         attempt: .status.execution.attempt,
         state: .status.execution.state}' >"${restart_fence_file}"
  jq -e '
    .state == "Running"
    and (.poolName // "" | length > 0)
    and (.poolLabel == .poolName)
    and (.poolUID // "" | length > 0)
    and (.runtimeInstanceID // "" | length > 0)
    and ((.controllerEpoch | type) == "number")
    and (.promptID // "" | length > 0)
    and ((.requestDigest // "") | test("^sha256:[a-f0-9]{64}$"))
    and (.runtimeSessionUID // "" | length > 0)
    and ((.runtimeSessionGeneration // 0) >= 1)
    and (.attempt == 1)
  ' "${restart_fence_file}" >/dev/null || {
    echo "restart Task carries an incomplete execution fence before the restart" >&2
    return 1
  }
  # The Task-side fence alone would let an incorrectly projected identity
  # self-compare into a pass: snapshot the RuntimePool's own identity as the
  # independent source (mirroring assert_restart_task_fence in
  # live-acp-runtime-e2e) and require the Task fence to match it BEFORE the
  # restart; the settled Task is then compared against this snapshot too.
  local restart_pool_snapshot="${WORK_DIR:-/tmp}/orka-ws-lc-restart-pool.json"
  local restart_fence_pool
  restart_fence_pool="$(jq -r '.poolName' "${restart_fence_file}")"
  kubectl -n orka-system get runtimepool "${restart_fence_pool}" -o json |
    jq '{poolName: .metadata.name, poolUID: .metadata.uid,
         controllerEpoch: .status.controllerEpoch,
         runtimeInstanceID: .status.activeInstance.runtimeInstanceID}' >"${restart_pool_snapshot}"
  jq -e --slurpfile fence "${restart_fence_file}" '
    $fence[0] as $f
    | (.poolUID // "" | length > 0)
      and (.runtimeInstanceID // "" | length > 0)
      and ((.controllerEpoch | type) == "number")
      and (.poolName == $f.poolName)
      and (.poolUID == $f.poolUID)
      and (.runtimeInstanceID == $f.runtimeInstanceID)
      and ($f.controllerEpoch == .controllerEpoch)
  ' "${restart_pool_snapshot}" >/dev/null || {
    echo "pre-restart Task fence does not match the RuntimePool's own identity" >&2
    return 1
  }
  kubectl -n orka-system get task orka-ws-lc-restart -o json |
    jq -e --slurpfile fence "${restart_fence_file}" '
      $fence[0] as $f
      | .status.execution as $e
      | $e.state == "Running"
        and $e.attempt == 1
        and ($e.runtimePoolName == $f.poolName)
        and ($e.runtimePoolUID == $f.poolUID)
        and ($e.runtimeInstanceID == $f.runtimeInstanceID)
        and ($e.promptID == $f.promptID)
        and ($e.requestDigest == $f.requestDigest)
    ' >/dev/null || {
    echo "restart Task left its first Running attempt before the controller restart" >&2
    return 1
  }
  # Force an UNPLANNED restart: a graceful rollout runs the manager's preStop
  # ACP upgrade drain, which waits out the held prompt before the old
  # controller exits and never exercises takeover of an interrupted Running
  # prompt. Killing the Pod without its preStop hook does.
  kubectl -n orka-system delete pod -l control-plane=controller-manager \
    --grace-period=0 --force --wait=true
  kubectl -n orka-system rollout status deployment/orka-controller-manager --timeout=5m
  # The controller restart severs the Orka API port-forward; drop it so the
  # next result assertion re-establishes a live tunnel.
  stop_port_forward "${ORKA_API_PORT_FORWARD_PID}"
  ORKA_API_PORT_FORWARD_PID=""
  # The canonical restart contract (live-acp-runtime-e2e) accepts an adopted
  # completion, a clean cancellation, or a conservative Failed/OutcomeUnknown
  # settlement; the invariant is bounded settlement without replay, not
  # guaranteed completion. This provider lane additionally proves a cancelled
  # prompt's model stream disconnected.
  local restart_started restart_now restart_json restart_phase restart_state restart_outcome restart_reason restart_attempt
  restart_started="$(date +%s)"
  while true; do
    restart_json="$(kubectl -n orka-system get task orka-ws-lc-restart -o json 2>/dev/null || true)"
    restart_phase="$(jq -r '.status.phase // ""' <<<"${restart_json}")"
    [[ "${restart_phase}" == "Succeeded" || "${restart_phase}" == "Failed" || "${restart_phase}" == "Cancelled" ]] && break
    restart_now="$(date +%s)"
    if (( restart_now - restart_started >= 600 )); then
      echo "restart Task did not settle after the controller restart (phase=${restart_phase:-<empty>})" >&2
      kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
      return 1
    fi
    sleep 5
  done
  restart_state="$(jq -r '.status.execution.state // ""' <<<"${restart_json}")"
  restart_outcome="$(jq -r '.status.execution.outcome // ""' <<<"${restart_json}")"
  restart_reason="$(jq -r '.status.execution.reason // ""' <<<"${restart_json}")"
  # The canonical restart contract also requires exactly one execution
  # attempt: a second controller-side attempt can be rejected before the
  # provider, leaving the fixture count at one while replay still happened.
  restart_attempt="$(jq -r '.status.execution.attempt // 0' <<<"${restart_json}")"
  [[ "${restart_attempt}" == "1" ]] || {
    kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
    echo "restart Task recorded ${restart_attempt} execution attempts; the restart contract requires exactly 1" >&2
    return 1
  }
  # Whatever the terminal tuple, takeover must have preserved the exact
  # pre-restart execution identity (canonical assert_restart_task_fence).
  jq -e --slurpfile fence "${restart_fence_file}" '
    $fence[0] as $f
    | .status.execution as $e
    | ($e.runtimePoolName == $f.poolName)
      and ($e.runtimePoolUID == $f.poolUID)
      and ($e.runtimeInstanceID == $f.runtimeInstanceID)
      and (($e.controllerEpoch | type) == "number")
      and ($e.controllerEpoch >= $f.controllerEpoch)
      and ($e.promptID == $f.promptID)
      and ($e.requestDigest == $f.requestDigest)
      and ($e.runtimeSessionUID == $f.runtimeSessionUID)
      and (($e.runtimeSessionGeneration // 0) >= 1)
      and ($e.runtimeSessionGeneration == $f.runtimeSessionGeneration)
  ' <<<"${restart_json}" >/dev/null || {
    kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
    echo "restart takeover did not preserve the exact pre-restart execution fence" >&2
    return 1
  }
  # The settled identity must also match the independent pre-restart
  # RuntimePool snapshot, not only the Task's own earlier projection.
  jq -e --slurpfile snap "${restart_pool_snapshot}" '
    $snap[0] as $s
    | .status.execution as $e
    | ($e.runtimePoolName == $s.poolName)
      and (.metadata.labels["orka.ai/runtime-pool"] == $s.poolName)
      and ($e.runtimePoolUID == $s.poolUID)
      and ($e.runtimeInstanceID == $s.runtimeInstanceID)
      and (($e.controllerEpoch | type) == "number")
      and ($e.controllerEpoch >= $s.controllerEpoch)
      and ((.status.jobName // "") == "")
  ' <<<"${restart_json}" >/dev/null || {
    kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
    echo "restart settlement does not match the independent pre-restart RuntimePool identity" >&2
    return 1
  }
  if [[ "${restart_phase}" == "Succeeded" && "${restart_state}" == "Succeeded" && "${restart_outcome}" == "Succeeded" ]]; then
    # The canonical restart contract also requires a ReadValidated delivery
    # projection with the successful tuple.
    jq -e '.status.delivery.state == "ReadValidated" and .status.delivery.outcome == "ReadValidated"' \
      <<<"${restart_json}" >/dev/null || {
      kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
      echo "restart Task succeeded without a ReadValidated delivery projection" >&2
      return 1
    }
    assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-lc-restart" "ORKA_WS_LC_RESTART_OK"
    restart_session_deletable=1
    log "Restart Task completed after adoption by the new controller epoch"
  elif [[ "${restart_phase}" == "Failed" && "${restart_state}" == "OutcomeUnknown" && "${restart_outcome}" == "OutcomeUnknown" && "${restart_reason}" == "RuntimeLost" ]]; then
    log "Restart Task settled conservatively as OutcomeUnknown under the new controller epoch"
  elif [[ "${restart_phase}" == "Cancelled" && "${restart_state}" == "Cancelled" && "${restart_outcome}" == "Cancelled" ]] &&
    [[ "${restart_reason}" == "Cancelled" || "${restart_reason}" == "TaskTimeout" ]]; then
    # The canonical restart contract (assert_restart_task_settled in
    # live-acp-runtime-e2e) accepts a clean cancellation of the interrupted
    # prompt as a safe settlement - but only a terminated one: the surviving
    # runtime's held provider stream must have observed the client
    # disconnect, or the interrupted prompt merely continued to completion
    # after terminal settlement while the fixture count stayed at one.
    local restart_disconnects restart_disconnect_started restart_disconnect_now
    restart_disconnect_started="$(date +%s)"
    while true; do
      restart_disconnects="$(fixture_marker_disconnects "ORKA_WS_LC_RESTART_OK")"
      if [[ "${restart_disconnects}" =~ ^[0-9]+$ && "${restart_disconnects}" -ge 1 ]]; then
        break
      fi
      restart_disconnect_now="$(date +%s)"
      if (( restart_disconnect_now - restart_disconnect_started >= 120 )); then
        echo "cancelled restart settlement never closed the in-flight provider stream (fixture disconnects=${restart_disconnects:-0})" >&2
        return 1
      fi
      sleep 3
    done
    restart_session_deletable=1
    log "Restart Task settled as a clean cancellation under the new controller epoch with a closed provider stream"
  else
    echo "restart Task settled outside the restart contract (phase=${restart_phase} state=${restart_state} outcome=${restart_outcome} reason=${restart_reason})" >&2
    kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
    return 1
  fi
  sleep 10
  restart_count_after="$(fixture_marker_count "ORKA_WS_LC_RESTART_OK")"
  [[ "${restart_count_after}" == "1" ]] || {
    echo "accepted prompt was replayed across the controller restart (${restart_count_before:-<empty>} -> ${restart_count_after:-<empty>})" >&2
    return 1
  }
  log "Accepted prompt survived the controller restart with no replay (fixture requests: ${restart_count_before})"
  # Operation fencing must actually advance across the forced restart: read
  # the RuntimePool AFTER the replacement manager took over and require its
  # controller epoch to be strictly greater than the pre-restart snapshot
  # (canonical live-acp-runtime-e2e restart check). A regressed epoch
  # rotation would otherwise pass every >= comparison above.
  local epoch_advance_started epoch_advance_now takeover_pool_json takeover_epoch
  epoch_advance_started="$(date +%s)"
  while true; do
    takeover_pool_json="$(kubectl -n orka-system get runtimepool "${restart_fence_pool}" -o json 2>/dev/null || true)"
    if jq -e --slurpfile snap "${restart_pool_snapshot}" '
      ((.status.controllerEpoch | type) == "number")
      and (.metadata.uid == $snap[0].poolUID)
      and (.status.controllerEpoch > $snap[0].controllerEpoch)
    ' <<<"${takeover_pool_json}" >/dev/null 2>&1; then
      break
    fi
    epoch_advance_now="$(date +%s)"
    if (( epoch_advance_now - epoch_advance_started >= 120 )); then
      echo "the RuntimePool controller epoch did not advance across the forced restart" >&2
      return 1
    fi
    sleep 3
  done
  takeover_epoch="$(jq -r '.status.controllerEpoch' <<<"${takeover_pool_json}")"
  restart_json="$(kubectl -n orka-system get task orka-ws-lc-restart -o json)"
  if ! jq -e --argjson takeoverEpoch "${takeover_epoch}" '
    (.status.execution.controllerEpoch | type) == "number"
    and .status.execution.controllerEpoch == $takeoverEpoch
  ' <<<"${restart_json}" >/dev/null; then
    kubectl -n orka-system get task orka-ws-lc-restart -o yaml >&2 || true
    echo "restart Task controller epoch does not match the takeover RuntimePool epoch" >&2
    return 1
  fi
  local restart_pool
  restart_pool="$(kubectl -n orka-system get task orka-ws-lc-restart \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${restart_pool}"

  log "Replacing the physical runtime and recovering the Session from zero"
  # The session generation is part of the authorization fence and must
  # advance monotonically across a pool replacement (canonical
  # live-acp-runtime-e2e replacement check).
  local pre_replacement_generation
  pre_replacement_generation="$(kubectl -n orka-system get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  if ! [[ "${pre_replacement_generation}" =~ ^[0-9]+$ && "${pre_replacement_generation}" -ge 1 ]]; then
    echo "post-drain continuation carries no valid runtimeSessionGeneration" >&2
    return 1
  fi
  local replacement_pool_json replacement_template_json replacement_template_count
  local replacement_template_namespace replacement_template_name replacement_actor_id
  local replacement_active_instance replacement_worker_fence replacement_worker_actor_id
  local replacement_worker_namespace replacement_worker_name replacement_worker_uid
  replacement_pool_json="$(kubectl -n orka-system get runtimepool "${pool_name}" -o json)"
  replacement_template_namespace="${ORKA_NAMESPACE}"
  replacement_template_json="$(kubectl -n "${replacement_template_namespace}" get actortemplates.ate.dev \
    -l "orka.ai/runtime-pool-name=${pool_name},orka.ai/runtime-pool-uid=${pool_uid}" -o json)"
  replacement_template_count="$(jq -r '.items | length' <<<"${replacement_template_json}")"
  if [[ "${replacement_template_count}" != "1" ]]; then
    echo "old pool ${pool_name}/${pool_uid} has ${replacement_template_count} derived ActorTemplates before replacement; want exactly one" >&2
    return 1
  fi
  replacement_template_name="$(jq -r '.items[0].metadata.name // ""' <<<"${replacement_template_json}")"
  if [[ "${replacement_template_name}" != *-actor-template ]]; then
    echo "old pool ${pool_name}/${pool_uid} has unexpected derived ActorTemplate name ${replacement_template_name:-<empty>}" >&2
    return 1
  fi
  replacement_actor_id="${replacement_template_name%-template}"
  replacement_active_instance="$(jq -r '.status.activeInstance.runtimeInstanceID // ""' <<<"${replacement_pool_json}")"
  replacement_worker_fence="$(jq -r '.metadata.annotations["orka.ai/substrate-actor-worker-pod-fence"] // ""' <<<"${replacement_pool_json}")"
  replacement_worker_actor_id="$(jq -Rr 'try (fromjson | .actorID // "") catch ""' <<<"${replacement_worker_fence}")"
  replacement_worker_namespace="$(jq -Rr 'try (fromjson | .namespace // "") catch ""' <<<"${replacement_worker_fence}")"
  replacement_worker_name="$(jq -Rr 'try (fromjson | .name // "") catch ""' <<<"${replacement_worker_fence}")"
  replacement_worker_uid="$(jq -Rr 'try (fromjson | .uid // "") catch ""' <<<"${replacement_worker_fence}")"
  [[ -n "${replacement_template_name}" && -n "${replacement_actor_id}" ]] || {
    echo "old pool ${pool_name}/${pool_uid} has no exact derived ActorTemplate identity before replacement" >&2
    return 1
  }
  if [[ -n "${replacement_active_instance}" && -z "${replacement_worker_fence}" ]]; then
    echo "active old pool ${pool_name}/${pool_uid} has no worker Pod fence before replacement" >&2
    return 1
  fi
  if [[ -n "${replacement_worker_fence}" ]] &&
    [[ -z "${replacement_worker_actor_id}" || -z "${replacement_worker_namespace}" ||
      -z "${replacement_worker_name}" || -z "${replacement_worker_uid}" ]]; then
    echo "old pool ${pool_name}/${pool_uid} has an incomplete worker Pod fence before replacement" >&2
    return 1
  fi
  if [[ -n "${replacement_worker_actor_id}" && "${replacement_worker_actor_id}" != "${replacement_actor_id}" ]]; then
    echo "old pool worker Pod fence belongs to actor ${replacement_worker_actor_id}, want ${replacement_actor_id}" >&2
    return 1
  fi
  kubectl -n orka-system delete runtimepool "${pool_name}" --wait=true --timeout=5m
  # RuntimePool finalization does not wait for the provider actor teardown to
  # finish; recreating the deterministic pool while the old Actor, its fenced
  # worker Pod, or the derived ActorTemplate still exists would overlap pool
  # incarnations and let the recovery-from-zero check pass without observing
  # physical zero.
  wait_actor_absent "${replacement_actor_id}" 300
  if [[ -n "${replacement_worker_name}" ]]; then
    wait_resource_absent "${replacement_worker_namespace}" pod "${replacement_worker_name}" 300
  fi
  wait_resource_absent "${replacement_template_namespace}" actortemplates.ate.dev "${replacement_template_name}" 300
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-lc-replaced
  namespace: ${ORKA_NAMESPACE}
spec:
  type: agent
  agentRef:
    name: orka-ws-lc-agent
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: orka-ws-lc-session
    create: false
  execution:
    workspace:
      enabled: true
      provider: substrate
      templateRef:
        namespace: ${ORKA_NAMESPACE}
        name: orka-acp-infra
      reusePolicy: session
  prompt: "Reply exactly: ORKA_WS_LC_REPLACED_OK"
YAML
  wait_jsonpath_equals \
    "post-replacement continuation phase" \
    "kubectl -n orka-system get task orka-ws-lc-replaced -o jsonpath='{.status.phase}'" \
    "Succeeded" 900
  assert_orka_task_result_contains "${ORKA_NAMESPACE}" "orka-ws-lc-replaced" "ORKA_WS_LC_REPLACED_OK"
  assert_lc_task_success_tuple orka-ws-lc-replaced
  assert_lc_task_success_fence orka-ws-lc-replaced
  local replaced_session replaced_instance replaced_pool replaced_pool_uid
  replaced_session="$(kubectl -n orka-system get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  replaced_instance="$(kubectl -n orka-system get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  replaced_pool="$(kubectl -n orka-system get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  replaced_pool_uid="$(kubectl -n orka-system get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  [[ "${replaced_session}" == "${session_uid}" ]] || {
    echo "physical replacement changed the logical RuntimeSession UID" >&2
    return 1
  }
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_REPLACED_OK")" == "true" ]] || {
    echo "post-replacement continuation carried no prior session history; the recovered session lost its transcript" >&2
    return 1
  }
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_REPLACED_OK" "ORKA_WS_LC_DRAINED_OK")" == "true" ]] || {
    echo "post-replacement history did not replay the expected prior turn; the recovered transcript is not this session's" >&2
    return 1
  }
  # Physical replacement must recreate the SAME logical workspace pool as a
  # new incarnation: the pool name is retained while its UID rotates. A
  # matching UID would mean the pool was never actually replaced; a new pool
  # name would mean the workspace binding silently moved.
  [[ "${replaced_pool}" == "${pool_name}" && -n "${replaced_pool_uid}" && "${replaced_pool_uid}" != "${pool_uid}" ]] || {
    echo "replacement did not recreate the workspace pool as a new incarnation (${replaced_pool:-<empty>}/${replaced_pool_uid:-<empty>} vs ${pool_name}/${pool_uid})" >&2
    return 1
  }
  [[ -n "${replaced_instance}" && "${replaced_instance}" != "${drained_instance}" ]] || {
    echo "physical replacement did not produce a new runtime instance identity" >&2
    return 1
  }
  local replaced_generation
  replaced_generation="$(kubectl -n orka-system get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  if ! [[ "${replaced_generation}" =~ ^[0-9]+$ && "${replaced_generation}" -gt "${pre_replacement_generation}" ]]; then
    echo "replacement did not advance the session generation (${replaced_generation:-<empty>} <= ${pre_replacement_generation})" >&2
    return 1
  fi
  assert_lc_substrate_replacement_identity \
    "${replaced_pool}" "${replaced_pool_uid}" "${replaced_instance}" "${drained_instance}"
  if [[ "$(fixture_marker_count "ORKA_WS_LC_REPLACED_OK")" != "1" ]]; then
    echo "post-replacement turn was delivered $(fixture_marker_count "ORKA_WS_LC_REPLACED_OK") times; want exactly one" >&2
    return 1
  fi

  local timeout_pool_uid cancel_pool_uid ambiguous_pool_uid restart_pool_uid
  timeout_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${timeout_pool_snapshot}")"
  cancel_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${cancel_pool_snapshot}")"
  ambiguous_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' \
    "${TMP_ROOT}/lc-ambiguous-pool-orka-ws-lc-ambiguous.json")"
  restart_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${restart_pool_snapshot}")"

  if [[ "${restart_session_deletable}" == "1" ]]; then
    delete_fixed_session "${restart_session}"
  fi
  log "Cleaning up lifecycle Tasks and pools"
  kubectl -n orka-system delete task orka-ws-lc-first orka-ws-lc-second \
    orka-ws-lc-drained orka-ws-lc-timeout orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced \
    --wait=true --timeout=4m
  kubectl -n orka-system delete runtimepool "${pool_name}" --ignore-not-found=true --wait=true --timeout=5m
  if [[ -n "${timeout_pool}" ]]; then
    kubectl -n orka-system delete runtimepool "${timeout_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  if [[ -n "${restart_pool}" ]]; then
    kubectl -n orka-system delete runtimepool "${restart_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  if [[ -n "${ambiguous_pool}" ]]; then
    kubectl -n orka-system delete runtimepool "${ambiguous_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  kubectl -n orka-system delete agent orka-ws-lc-agent --ignore-not-found=true
  # Exact-cleanup proof covers every scenario pool - continuation, cancel,
  # and restart are distinct session-scoped pools - and queries the provider
  # actors directly, not just the Orka-side objects.
  # Provider garbage collection can still be draining dependents after the
  # RuntimePool deletion returns; poll each pool's dependents to zero with a
  # bounded timeout instead of failing an otherwise correct run.
  local -a cleanup_pool_specs=(
    "${pool_name}|${pool_uid}"
    "${replaced_pool}|${replaced_pool_uid}"
    "${timeout_pool}|${timeout_pool_uid}"
    "${cancel_pool}|${cancel_pool_uid}"
    "${ambiguous_pool}|${ambiguous_pool_uid}"
    "${restart_pool}|${restart_pool_uid}"
  )
  local cleanup_pool_spec leftover_pool leftover_pool_uid pool_selector
  local actor_leftovers cleanup_poll_started cleanup_poll_now
  for cleanup_pool_spec in "${cleanup_pool_specs[@]}"; do
    IFS='|' read -r leftover_pool leftover_pool_uid <<<"${cleanup_pool_spec}"
    [[ -n "${leftover_pool}" && -n "${leftover_pool_uid}" ]] || {
      echo "lifecycle cleanup is missing an exact RuntimePool identity (${cleanup_pool_spec})" >&2
      return 1
    }
    pool_selector="orka.ai/runtime-pool-namespace=${ORKA_NAMESPACE},orka.ai/runtime-pool-name=${leftover_pool},orka.ai/runtime-pool-uid=${leftover_pool_uid}"
    if kubectl -n orka-system get runtimepool "${leftover_pool}" >/dev/null 2>&1; then
      echo "lifecycle cleanup left RuntimePool ${leftover_pool}" >&2
      return 1
    fi
    cleanup_poll_started="$(date +%s)"
    while true; do
    # kubectl-ate supports table, json, and yaml output; -o name exits
    # without producing names. The query and its JSON must themselves
    # succeed: a transient provider or CRD error would otherwise produce an
    # empty stream and a false zero count.
    local actor_json actor_ids
    # A failed or unparseable provider query is retried within the bounded
    # poll: the atenet CLI can emit truncated JSON while actors are being
    # torn down concurrently, and one transient glitch must not fail an
    # otherwise correct cleanup. Only a glitch persisting past the deadline
    # fails the lane.
    if ! actor_json="$(kubectl_ate get actors -o json 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not query provider actors: ${actor_json}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if ! actor_ids="$(jq -r '.actors[]?.actorId // empty' <<<"${actor_json}" 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not parse provider actors: ${actor_ids}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    actor_leftovers="$(grep -c "${leftover_pool}" <<<"${actor_ids}" || true)"
    # The controller-derived ActorTemplate is a distinct pool-owned child that
    # Substrate finalization deletes explicitly; exact cleanup must observe
    # its absence too or a finalization regression that leaks only the
    # template would pass. The ownership labels are stamped on every derived
    # template, and the query itself must succeed for the zero to count.
    local template_json template_count
    if ! template_json="$(kubectl -n "${ORKA_NAMESPACE}" get actortemplates.ate.dev \
      -l "${pool_selector}" -o json 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not query derived ActorTemplates: ${template_json}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if ! template_count="$(jq -r '.items | length' <<<"${template_json}" 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not parse derived ActorTemplates: ${template_count}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    local secret_json secret_count policy_json policy_count
    if ! secret_json="$(kubectl -n "${ACP_RUNTIME_NAMESPACE}" get secrets \
      -l "${pool_selector}" -o json 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not query RuntimePool Secrets: ${secret_json}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if ! secret_count="$(jq -r '.items | length' <<<"${secret_json}" 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not parse RuntimePool Secrets: ${secret_count}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if ! policy_json="$(kubectl get networkpolicies -A \
      -l "${pool_selector}" -o json 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not query RuntimePool NetworkPolicies: ${policy_json}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if ! policy_count="$(jq -r '.items | length' <<<"${policy_json}" 2>&1)"; then
      cleanup_poll_now="$(date +%s)"
      if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
        echo "exact-cleanup verification could not parse RuntimePool NetworkPolicies: ${policy_count}" >&2
        return 1
      fi
      sleep 5
      continue
    fi
    if [[ "${actor_leftovers}" == "0" && "${template_count}" == "0" &&
      "${secret_count}" == "0" && "${policy_count}" == "0" ]]; then
      break
    fi
    cleanup_poll_now="$(date +%s)"
    if (( cleanup_poll_now - cleanup_poll_started >= 300 )); then
      echo "lifecycle cleanup left ${actor_leftovers} provider actor(s), ${template_count} derived ActorTemplate(s), ${secret_count} Secret(s), and ${policy_count} NetworkPolicy object(s) for ${leftover_pool}/${leftover_pool_uid} after 300s" >&2
      kubectl -n "${ORKA_NAMESPACE}" get actortemplates.ate.dev \
        -l "${pool_selector}" >&2 || true
      kubectl -n "${ACP_RUNTIME_NAMESPACE}" get secrets \
        -l "${pool_selector}" >&2 || true
      kubectl get networkpolicies -A -l "${pool_selector}" >&2 || true
      return 1
    fi
    sleep 5
    done
  done
  for reset_lc_session in orka-ws-lc-session orka-ws-lc-timeout-session \
    orka-ws-lc-cancel-session; do
    delete_fixed_session "${reset_lc_session}"
  done
  kubectl -n "${ORKA_NAMESPACE}" delete configmap orka-ws-lc-pools \
    --ignore-not-found=true >/dev/null 2>&1 || true
  log "Workspace-backed lifecycle/recovery conformance (Substrate) passed"
}

main() {
  require_command bash
  require_command curl
  require_command docker
  require_command git
  require_command go
  require_command jq
  require_command kind
  require_command ko
  require_command kubectl
  [[ "${ORKA_NAMESPACE}" == "orka-system" ]] || {
    echo "ORKA_NAMESPACE must be orka-system for the canonical config/acp-workload deployment" >&2
    exit 1
  }

  TMP_ROOT="$(mktemp -d)"
  export KUBECONFIG="${TMP_ROOT}/kubeconfig"
  DOCKER_CONFIG_DIR="$(mktemp -d)"
  printf '{"auths":{}}\n' > "${DOCKER_CONFIG_DIR}/config.json"
  SUBSTRATE_DIR="${TMP_ROOT}/substrate"

  log "Cloning Substrate ${SUBSTRATE_REF}"
  git clone --quiet "${SUBSTRATE_REPO}" "${SUBSTRATE_DIR}"
  git -C "${SUBSTRATE_DIR}" checkout --quiet "${SUBSTRATE_REF}"
  apply_substrate_workspace_agent_capability_patch
  apply_substrate_atenet_authorization_redaction_patch
  apply_substrate_ateom_delete_recovery_patch
  verify_reviewed_substrate_patch_set
  patch_substrate_kind_registry_script

  log "Creating kind cluster and installing Substrate"
  (
    cd "${SUBSTRATE_DIR}"
    export DOCKER_CONFIG="${DOCKER_CONFIG_DIR}"
    export KIND_CLUSTER_NAME="${KIND_CLUSTER}"
    export KIND_REGISTRY_NAME="${KIND_REGISTRY_NAME}"
    export KIND_REGISTRY_PORT="${KIND_REGISTRY_PORT}"
    export KO_DOCKER_REPO="localhost:${KIND_REGISTRY_PORT}"
    hack/create-kind-cluster.sh
    hack/install-ate-kind.sh --deploy-ate-system
  )
  kubectl config use-context "kind-${KIND_CLUSTER}"
  wait_for_rollouts
  ensure_snapshot_bucket

  log "Building kubectl-ate"
  (cd "${SUBSTRATE_DIR}" && go build -o "${TMP_ROOT}/kubectl-ate" ./cmd/kubectl-ate)

  local registry_ip registry_addr controller_image workspace_push_image workspace_actor_image mcp_push_image mcp_actor_image tool_client_image responses_fixture_image ateom_image
  registry_ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "${KIND_REGISTRY_NAME}")"
  if [[ -z "${registry_ip}" ]]; then
    registry_ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}' "${KIND_REGISTRY_NAME}" | head -n1)"
  fi
  if [[ -z "${registry_ip}" ]]; then
    echo "could not determine registry IP for ${KIND_REGISTRY_NAME}" >&2
    exit 1
  fi
  registry_addr="localhost:${KIND_REGISTRY_PORT}"
  controller_image="${registry_addr}/orka/controller:${IMAGE_TAG}"
  workspace_push_image="${registry_addr}/orka/workspace-agent-root:${IMAGE_TAG}"
  workspace_actor_image="${registry_ip}:5000/orka/workspace-agent-root:${IMAGE_TAG}"
  mcp_push_image="${registry_addr}/orka/mcp-e2e-server:${IMAGE_TAG}"
  mcp_actor_image="${registry_ip}:5000/orka/mcp-e2e-server:${IMAGE_TAG}"
  tool_client_image="${registry_addr}/orka/tool-e2e-client:${IMAGE_TAG}"
  responses_fixture_image="${registry_addr}/orka/openai-responses-fixture:${IMAGE_TAG}"

  local acp_codex_push_image acp_codex_actor_ref
  acp_codex_push_image="${registry_addr}/orka/acp-codex-runtime:${IMAGE_TAG}"
  acp_codex_actor_ref=""

  log "Building and pushing Orka images"
  docker build -t "${controller_image}" -f "${ROOT_DIR}/Dockerfile" "${ROOT_DIR}"
  docker build -t "${workspace_push_image}" -f "${ROOT_DIR}/cmd/orka-workspace-agent/Dockerfile" "${ROOT_DIR}"
  docker build -t "${mcp_push_image}" -f "${ROOT_DIR}/cmd/orka-mcp-e2e-server/Dockerfile" "${ROOT_DIR}"
  docker build -t "${tool_client_image}" -f "${ROOT_DIR}/cmd/orka-tool-e2e-client/Dockerfile" "${ROOT_DIR}"
  docker push "${controller_image}"
  docker push "${workspace_push_image}"
  docker push "${mcp_push_image}"
  docker push "${tool_client_image}"
  if [[ "${SUBSTRATE_E2E_ACP_TASK_SMOKE}" == "1" || "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" || "${SUBSTRATE_E2E_LIFECYCLE}" == "1" ]]; then
    docker build -t "${responses_fixture_image}" -f "${ROOT_DIR}/scripts/fixtures/openai-responses/Dockerfile" "${ROOT_DIR}"
    docker push "${responses_fixture_image}"
    log "Building immutable Codex ACP runtime image for the workspace-backed Task smoke"
    make -C "${ROOT_DIR}" docker-build-acp-codex-runtime ACP_CODEX_RUNTIME_IMG="${acp_codex_push_image}"
    docker push "${acp_codex_push_image}"
    local acp_codex_digest
    acp_codex_digest="$(docker inspect --format '{{index .RepoDigests 0}}' "${acp_codex_push_image}" | awk -F'@' '{print $2}')"
    [[ -n "${acp_codex_digest}" ]] || { echo "could not resolve Codex runtime image digest" >&2; exit 1; }
    acp_codex_actor_ref="${registry_ip}:5000/orka/acp-codex-runtime@${acp_codex_digest}"
    log "Codex runtime actor image: ${acp_codex_actor_ref}"
  fi

  log "Publishing Substrate ateom-gvisor image"
  ateom_image="$(publish_ateom_image)"
  create_substrate_resources "${ateom_image}" "${workspace_actor_image}" "${mcp_actor_image}"
  if [[ "${SUBSTRATE_E2E_ACP_TASK_SMOKE}" == "1" || "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" || "${SUBSTRATE_E2E_LIFECYCLE}" == "1" ]]; then
    deploy_responses_fixture "${responses_fixture_image}"
  fi
  deploy_orka "${controller_image}" "${acp_codex_actor_ref}"
  exercise_direct_substrate
  exercise_orka_tasks "${tool_client_image}"
  if [[ "${SUBSTRATE_E2E_ACP_TASK_SMOKE}" == "1" ]]; then
    exercise_workspace_backed_acp_task
  else
    log "Skipping workspace-backed ACP Task smoke (SUBSTRATE_E2E_ACP_TASK_SMOKE=0)"
  fi
  if [[ "${SUBSTRATE_E2E_SUSPEND_RESUME}" == "1" ]]; then
    exercise_workspace_suspend_resume_acp_task
  else
    log "Skipping class-backed suspend/cold-resume conformance (SUBSTRATE_E2E_SUSPEND_RESUME=0; the pinned Substrate release cannot express the data-only snapshot policy)"
  fi
  if [[ "${SUBSTRATE_E2E_LIFECYCLE}" == "1" ]]; then
    exercise_workspace_lifecycle_acp_task
  else
    log "Skipping workspace-backed lifecycle/recovery conformance (SUBSTRATE_E2E_LIFECYCLE=0)"
  fi
  assert_no_suspending_actors

  log "Agent Substrate E2E passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
