#!/usr/bin/env bash
# Live harness v1/v2 coexistence E2E.
#
# Proves the isolated-coexistence contract from
# website/docs/operations/harness-modes.md on one kind cluster, model-free and
# secret-free:
#
#   1. One platform-owned CRD wave (scripts/apply-helm-crds.sh), then two
#      isolated Helm releases: orka-v1 (controller.mode=harness-v1 plus the
#      wrapper data plane) in orka-v1-system and orka-v2
#      (controller.mode=harness-v2) in orka-v2-system, both installed with
#      --skip-crds into pre-claimed mode-labeled namespaces.
#   2. Admission in both directions: an orka.harness.v2-contract AgentRuntime
#      and Agent are rejected in the v1 namespace, and orka.harness.v1-contract
#      registrations are rejected in the v2 namespace.
#   3. A real orka.harness.v1 wrapper Task executes end to end through the
#      production wrapper image driven by a deterministic fake agent CLI
#      (scripts/fixtures/coexistence-fake-agent.sh via the wrapper's generic
#      adapter) and reaches terminal Succeeded with its result persisted.
#   4. The v1 controller is restarted while a wrapper turn is actively held
#      open and provably past durable wrapper admission (the fake agent's hold
#      marker can only appear after the wrapper records TurnAccepted in its
#      ledger); ledger-backed recovery settles the Task to Succeeded afterwards
#      and the v2 controller stays Ready throughout.
#   5. Isolation matrix: each installation's controller ServiceAccount is
#      denied reads and writes in the other watch namespace (kubectl auth
#      can-i plus a real impersonated request), leader Leases live in their
#      own namespaces, and NetworkPolicies exist and select the expected pods
#      in both namespaces.
#   6. scripts/check-legacy-wrapper-resources.sh passes its COEXISTENCE=1
#      branch while the wrapper exists, fails its default v2-only branch for
#      the same inventory, and passes the default branch again after the v1
#      release is uninstalled.
#   7. Uninstalling the v1 release leaves the v2 installation Ready and the
#      shared CRDs installed.
#
# Scope notes:
#   - The fake agent CLI uses the wrapper's generic adapter
#     (ORKA_HARNESS_WRAPPER_RUNTIME=generic + ORKA_HARNESS_WRAPPER_COMMAND),
#     wired through a test-only Deployment patch, because the shipped multi
#     adapter only routes to real provider CLIs. Everything else about the
#     wrapper (image, TLS, bearer auth, admission ledger, HTTP contract,
#     dispatcher) is the production path.
#   - NetworkPolicy assertions cover existence and pod selection. kind's
#     default CNI does not enforce NetworkPolicy, so enforcement itself is not
#     asserted; RBAC denial is asserted with real requests.
set -Eeuo pipefail

check_docker_ready() {
  if ! docker info >/dev/null 2>&1; then
    die "Docker daemon is not reachable; start Docker before running the live coexistence E2E"
  fi
}

parse_args() {
  while (( $# > 0 )); do
    case "$1" in
      --preflight-only)
        preflight_only=1
        shift
        ;;
      -h|--help)
        cat <<EOF_HELP
Usage: $0 [--preflight-only]

Runs the live harness v1/v2 coexistence E2E against a local kind cluster.

Options:
  --preflight-only  Validate local prerequisites and configuration, then exit before cluster changes.

Environment:
  KIND_CLUSTER                  kind cluster name (default: orka-coexistence-live-e2e)
  COEXISTENCE_SKIP_IMAGE_BUILD  Set to 1 to reuse already-built local images.
  COEXISTENCE_KEEP_CLUSTER      Set to 1 to keep the kind cluster after the run.
EOF_HELP
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=scripts/lib/e2e-common.sh
. "${script_dir}/lib/e2e-common.sh"
# shellcheck source=scripts/lib/redact.sh
. "${script_dir}/lib/redact.sh"
# shellcheck source=scripts/lib/kind-local-registry.sh
. "${script_dir}/lib/kind-local-registry.sh"

kind_cluster="${KIND_CLUSTER:-orka-coexistence-live-e2e}"
chart_dir="${repo_root}/manifest_staging/charts/orka"
fake_agent_fixture="${repo_root}/scripts/fixtures/coexistence-fake-agent.sh"

v1_namespace="orka-v1-system"
v2_namespace="orka-v2-system"
v2_runtime_namespace="orka-v2-runtimes"
v1_release="orka-v1"
v2_release="orka-v2"
v1_controller_deployment="orka-v1-controller"
v2_controller_deployment="orka-v2-controller"
wrapper_deployment="orka-v1-agent-harness-wrapper"
v1_controller_sa="system:serviceaccount:${v1_namespace}:${v1_release}"
v2_controller_sa="system:serviceaccount:${v2_namespace}:${v2_release}"
leader_lease="03b49a10.orka.ai"

manager_image="${ORKA_MANAGER_IMAGE:-orka-controller:coexistence-e2e}"
wrapper_image="${ORKA_HARNESS_WRAPPER_IMAGE:-orka-harness-wrapper:coexistence-e2e}"
publisher_image="${ORKA_WORKSPACE_PUBLISHER_IMAGE:-orka-workspace-publisher:coexistence-e2e}"
skip_image_build="${COEXISTENCE_SKIP_IMAGE_BUILD:-0}"
keep_cluster="${COEXISTENCE_KEEP_CLUSTER:-0}"
rollout_timeout="${COEXISTENCE_ROLLOUT_TIMEOUT:-5m}"
task_wait_seconds="${COEXISTENCE_TASK_WAIT_SECONDS:-300}"

wrapper_token=""
api_token=""
api_pf_pid=""
preflight_only=0
cluster_created_by_run=0
work_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/coexistence-live-e2e.XXXXXX")"
# Per-run registry ownership label (derived from the unique temp directory) so
# registry start/stop can never touch another run's registry container.
registry_owner="coexistence-live-e2e-$(basename "${work_dir}" | tr -c 'a-zA-Z0-9.-' '-')"
registry_owner="${registry_owner%-}"
api_pf_log="${work_dir}/api-port-forward.log"
api_local_port="${COEXISTENCE_API_LOCAL_PORT:-18093}"
v2_readiness_monitor_pid=""
v2_readiness_violations="${work_dir}/v2-readiness-violations.log"

# All kubectl/helm/kind interactions use an isolated kubeconfig inside the
# run's temp directory; the user's global kubeconfig and current-context are
# never read or mutated.
export KUBECONFIG="${work_dir}/kubeconfig"

# The shared redact() (scripts/lib/redact.sh) substitutes the current values of
# these variables at call time; both are test-only credentials.
ORKA_REDACT_SECRET_VARS=(wrapper_token api_token)

run() {
  printf '+ ' >&2
  printf '%q ' "$@" >&2
  printf '\n' >&2
  "$@"
}

wait_until() {
  local description="$1"
  local timeout_seconds="$2"
  shift 2
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  die "timed out after ${timeout_seconds}s waiting for ${description}"
}

cleanup_port_forward() {
  if [[ -n "${api_pf_pid}" ]]; then
    if kill -0 "${api_pf_pid}" 2>/dev/null; then
      kill "${api_pf_pid}" 2>/dev/null || true
    fi
    wait "${api_pf_pid}" 2>/dev/null || true
    api_pf_pid=""
  fi
}

# Continuously sample the v2 controller Deployment's readiness (1s cadence)
# into a violations file so a transient v2 outage during the v1 controller
# restart cannot escape a single post-restart check. A failed kubectl read is
# retried once immediately so one apiserver blip is not misclassified; two
# consecutive read failures are recorded fail-closed as a violation.
start_v2_readiness_monitor() {
  : >"${v2_readiness_violations}"
  (
    local deployment_json
    while true; do
      deployment_json="$(kubectl -n "${v2_namespace}" get deployment "${v2_controller_deployment}" -o json 2>/dev/null)" || deployment_json=""
      if [[ -z "${deployment_json}" ]]; then
        sleep 1
        deployment_json="$(kubectl -n "${v2_namespace}" get deployment "${v2_controller_deployment}" -o json 2>/dev/null)" || deployment_json=""
      fi
      if ! jq -e \
        '.status.observedGeneration == .metadata.generation
          and (.status.updatedReplicas // 0) == .spec.replicas
          and (.status.readyReplicas // 0) == .spec.replicas
          and (.status.availableReplicas // 0) == .spec.replicas' \
        >/dev/null 2>&1 <<<"${deployment_json}"; then
        printf '%s v2 controller deployment was not fully Ready\n' \
          "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >>"${v2_readiness_violations}"
      fi
      sleep 1
    done
  ) &
  v2_readiness_monitor_pid=$!
}

stop_v2_readiness_monitor() {
  if [[ -n "${v2_readiness_monitor_pid}" ]]; then
    if kill -0 "${v2_readiness_monitor_pid}" 2>/dev/null; then
      kill "${v2_readiness_monitor_pid}" 2>/dev/null || true
    fi
    wait "${v2_readiness_monitor_pid}" 2>/dev/null || true
    v2_readiness_monitor_pid=""
  fi
}

dump_diagnostics() {
  log "Collecting redacted diagnostics"
  {
    echo "=== Current Kubernetes Context ==="
    kubectl config current-context 2>/dev/null || true
    echo
    echo "=== Helm Releases ==="
    helm list -A 2>/dev/null || true
    for ns in "${v1_namespace}" "${v2_namespace}" "${v2_runtime_namespace}"; do
      echo
      echo "=== Namespace ${ns} Resources ==="
      kubectl get pods,svc,deploy,pvc,networkpolicies -n "${ns}" -o wide 2>/dev/null || true
      kubectl get agents,tasks,agentruntimes -n "${ns}" -o wide 2>/dev/null || true
      echo
      echo "=== Namespace ${ns} Events ==="
      kubectl get events -n "${ns}" --sort-by=.lastTimestamp 2>/dev/null | tail -40 || true
    done
    echo
    echo "=== v1 Controller Logs ==="
    kubectl logs "deployment/${v1_controller_deployment}" -n "${v1_namespace}" --tail=150 2>/dev/null || true
    echo
    echo "=== v1 Wrapper Logs ==="
    kubectl logs "deployment/${wrapper_deployment}" -n "${v1_namespace}" --tail=100 2>/dev/null || true
    echo
    echo "=== v2 Controller Logs ==="
    kubectl logs "deployment/${v2_controller_deployment}" -n "${v2_namespace}" --tail=150 2>/dev/null || true
    echo
    echo "=== Tasks (v1 namespace, full) ==="
    kubectl get tasks -n "${v1_namespace}" -o yaml 2>/dev/null || true
    echo
    echo "=== API Port-forward Log ==="
    if [[ -f "${api_pf_log}" ]]; then
      cat "${api_pf_log}" 2>/dev/null || true
    fi
  } | redact >&2
}

on_exit() {
  local status="$1"
  set +e

  if [[ "${status}" -ne 0 ]]; then
    dump_diagnostics
  fi

  stop_v2_readiness_monitor
  cleanup_port_forward
  # Remove the owned registry only with a cluster this invocation is also
  # removing. Retained/reused clusters keep digest-pinned workloads that still
  # depend on this registry after node cache eviction or replacement.
  if [[ "${cluster_created_by_run}" == "1" && "${keep_cluster}" != "1" ]]; then
    orka_kind_registry_stop "${kind_cluster}" "${registry_owner}" >/dev/null 2>&1 || true
    kind delete cluster --name "${kind_cluster}" >/dev/null 2>&1 || true
  else
    log "Retaining the owned kind registry because cluster ${kind_cluster} is retained"
  fi
  rm -rf "${work_dir}" >/dev/null 2>&1 || true

  if [[ "${status}" -ne 0 ]]; then
    log "Live coexistence e2e failed"
  fi
}

# Generate a throwaway CA plus serving certificate for one in-cluster Service
# DNS name and leave ca.crt/tls.crt/tls.key in the given output directory.
# Modeled on scripts/lib/e2e-admission-tls.sh; test-only, 7-day validity.
generate_service_tls() {
  local service_dns="$1"
  local out_dir="$2"
  mkdir -p "${out_dir}"

  cat >"${out_dir}/ca.conf" <<'EOF_CA_CONFIG'
[req]
prompt = no
distinguished_name = ca_name
x509_extensions = ca_extensions

[ca_name]
CN = Orka coexistence E2E CA

[ca_extensions]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
EOF_CA_CONFIG

  cat >"${out_dir}/serving.conf" <<EOF_SERVING_CONFIG
[req]
prompt = no
distinguished_name = serving_name
req_extensions = serving_extensions

[serving_name]
CN = ${service_dns}

[serving_extensions]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:${service_dns},DNS:${service_dns}.cluster.local
subjectKeyIdentifier = hash
EOF_SERVING_CONFIG

  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 7 \
    -config "${out_dir}/ca.conf" \
    -keyout "${out_dir}/ca.key" \
    -out "${out_dir}/ca.crt" >/dev/null 2>&1
  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -config "${out_dir}/serving.conf" \
    -keyout "${out_dir}/tls.key" \
    -out "${out_dir}/tls.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -days 7 \
    -in "${out_dir}/tls.csr" \
    -CA "${out_dir}/ca.crt" \
    -CAkey "${out_dir}/ca.key" \
    -CAcreateserial \
    -extfile "${out_dir}/serving.conf" \
    -extensions serving_extensions \
    -out "${out_dir}/tls.crt" >/dev/null 2>&1
  openssl verify -CAfile "${out_dir}/ca.crt" "${out_dir}/tls.crt" >/dev/null
}

base64_no_wrap() {
  openssl base64 -A -in "$1"
}

# Apply the manifest on stdin with server-side dry-run and require the request
# to be denied with the expected admission message. Transient webhook transport
# errors (endpoint propagation or a webhook socket that is not accepting yet
# right after rollout) surface as "failed calling webhook"; only those are
# retried. An admitted request or a denial with the wrong message fails
# immediately.
expect_admission_denied() {
  local description="$1"
  local expected="$2"
  local manifest_file="$3"
  local err_file="${work_dir}/admission-denied.err"
  local attempts_remaining=20

  while (( attempts_remaining > 0 )); do
    if kubectl apply --dry-run=server -f "${manifest_file}" >"${err_file}" 2>&1; then
      {
        echo "expected admission denial for ${description}, but the request was admitted:"
        cat "${err_file}" 2>/dev/null || true
      } | redact >&2
      return 1
    fi
    if grep -Fq "${expected}" "${err_file}"; then
      log "Denied as expected: ${description}"
      return 0
    fi
    if ! grep -Eq 'failed calling webhook|no endpoints available' "${err_file}"; then
      break
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 3
  done
  {
    echo "admission denial for ${description} did not contain ${expected}:"
    cat "${err_file}" 2>/dev/null || true
  } | redact >&2
  return 1
}

expect_can_i() {
  local expected="$1"
  local subject="$2"
  local verb="$3"
  local resource="$4"
  local namespace="$5"
  local answer
  answer="$(kubectl auth can-i "${verb}" "${resource}" -n "${namespace}" --as="${subject}" 2>/dev/null || true)"
  if [[ "${answer}" != "${expected}" ]]; then
    die "expected 'kubectl auth can-i ${verb} ${resource} -n ${namespace} --as=${subject}' to answer ${expected}, got '${answer}'"
  fi
}

deployment_ready() {
  local namespace="$1"
  local deployment="$2"
  kubectl -n "${namespace}" get deployment "${deployment}" -o json 2>/dev/null | jq -e \
    '.status.observedGeneration == .metadata.generation
      and (.status.updatedReplicas // 0) == .spec.replicas
      and (.status.readyReplicas // 0) == .spec.replicas
      and (.status.availableReplicas // 0) == .spec.replicas' >/dev/null
}

task_phase_is() {
  local task="$1"
  local phase="$2"
  [[ "$(kubectl -n "${v1_namespace}" get task "${task}" -o jsonpath='{.status.phase}' 2>/dev/null)" == "${phase}" ]]
}

task_harness_state_in() {
  local task="$1"
  shift
  local state candidate
  state="$(kubectl -n "${v1_namespace}" get task "${task}" -o jsonpath='{.status.harnessRuntime.state}' 2>/dev/null)"
  for candidate in "$@"; do
    if [[ "${state}" == "${candidate}" ]]; then
      return 0
    fi
  done
  return 1
}

assert_task_succeeded() {
  local task="$1"
  local task_json="${work_dir}/${task}.json"
  kubectl -n "${v1_namespace}" get task "${task}" -o json >"${task_json}"
  jq -e '
    .status.phase == "Succeeded"
    and .status.harnessRuntime.state == "Succeeded"
    and .status.harnessRuntime.outcome == "Succeeded"
    and .status.resultRef.available == true
  ' "${task_json}" >/dev/null || die "Task/${task} did not record a terminal harness v1 success"
}

start_api_port_forward() {
  kubectl -n "${v1_namespace}" port-forward "svc/${v1_release}" \
    "${api_local_port}:8080" >>"${api_pf_log}" 2>&1 &
  echo $!
}

fetch_task_result() {
  local task="$1"
  local output_file="$2"
  local attempts_remaining=30
  local status

  if [[ -z "${api_pf_pid}" ]] || ! kill -0 "${api_pf_pid}" 2>/dev/null; then
    api_pf_pid="$(start_api_port_forward)"
  fi
  while (( attempts_remaining > 0 )); do
    status="$(curl -sS --connect-timeout 5 --max-time 30 \
      -o "${output_file}" -w '%{http_code}' \
      -H "Authorization: Bearer ${api_token}" \
      "http://127.0.0.1:${api_local_port}/api/v1/tasks/${task}/result?namespace=${v1_namespace}" \
      2>>"${api_pf_log}" || true)"
    if [[ "${status}" == "200" ]]; then
      return 0
    fi
    if [[ -n "${api_pf_pid}" ]] && ! kill -0 "${api_pf_pid}" 2>/dev/null; then
      wait "${api_pf_pid}" 2>/dev/null || true
      api_pf_pid="$(start_api_port_forward)"
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done
  die "failed to fetch result for Task/${task} through the v1 controller API (last HTTP status: ${status:-none})"
}

assert_result_contains() {
  local task="$1"
  local expected="$2"
  local result_file="${work_dir}/${task}-result.json"
  fetch_task_result "${task}" "${result_file}"
  jq -er '.result' "${result_file}" | grep -Fq "${expected}" || \
    die "Task/${task} result did not contain the expected marker ${expected}"
}

split_image_repository() {
  printf '%s' "${1%@*}"
}

split_image_digest() {
  printf '%s' "${1##*@}"
}

# Fail-closed reuse guard: a pre-existing cluster may only be reused when it
# contains no Orka CRDs, mode-labeled namespaces, fixed coexistence namespaces,
# or fixed coexistence Helm releases. The shared CRD wave uses server-side
# force-conflicts, so even an Orka installation under unrelated release and
# namespace names would be unsafe to adopt. Runs before any cluster write; a
# cluster this invocation created is fresh and needs no check.
assert_reused_cluster_is_unclaimed() {
  local conflicts=() crd crd_inventory mode_namespace mode_namespace_inventory
  local ns entry release release_ns

  crd_inventory="$(kubectl get customresourcedefinitions.apiextensions.k8s.io -o name 2>/dev/null)" || \
    die "unable to inspect CRDs before reusing kind cluster ${kind_cluster}"
  while IFS= read -r crd; do
    [[ -n "${crd}" && "${crd}" == *.orka.ai ]] || continue
    conflicts+=("${crd}")
  done <<<"${crd_inventory}"

  mode_namespace_inventory="$(kubectl get namespaces -l 'orka.ai/controller-mode' -o name 2>/dev/null)" || \
    die "unable to inspect mode-labeled namespaces before reusing kind cluster ${kind_cluster}"
  while IFS= read -r mode_namespace; do
    [[ -n "${mode_namespace}" ]] || continue
    conflicts+=("${mode_namespace}")
  done <<<"${mode_namespace_inventory}"

  for ns in "${v1_namespace}" "${v2_namespace}" "${v2_runtime_namespace}"; do
    if kubectl get namespace "${ns}" >/dev/null 2>&1; then
      conflicts+=("namespace/${ns}")
    fi
  done
  for entry in "${v1_release}:${v1_namespace}" "${v2_release}:${v2_namespace}"; do
    release="${entry%%:*}"
    release_ns="${entry#*:}"
    if helm status "${release}" --namespace "${release_ns}" >/dev/null 2>&1; then
      conflicts+=("helm-release/${release_ns}/${release}")
    fi
  done
  if (( ${#conflicts[@]} > 0 )); then
    die "refusing to reuse pre-existing kind cluster ${kind_cluster}: found ${conflicts[*]}; delete those namespaces/releases (or the cluster) or run with a dedicated KIND_CLUSTER name"
  fi
  log "Reused cluster contains no coexistence namespaces or releases; proceeding"
}

create_namespace_secrets() {
  local namespace="$1"
  local webhook_service="$2"
  local tls_dir="${work_dir}/${namespace}-webhook-tls"

  kubectl -n "${namespace}" create secret generic orka-agent-snapshot-key \
    --from-literal="key=$(openssl rand -base64 32)" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  generate_service_tls "${webhook_service}" "${tls_dir}"
  kubectl -n "${namespace}" create secret tls orka-webhook-tls \
    --cert="${tls_dir}/tls.crt" \
    --key="${tls_dir}/tls.key" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

main() {
  parse_args "$@"

  require_cmd make
  require_cmd docker
  require_cmd kind
  require_cmd kubectl
  require_cmd helm
  require_cmd jq
  require_cmd openssl
  require_cmd curl
  check_docker_ready
  [[ -d "${chart_dir}" ]] || die "missing staged Helm chart: ${chart_dir}"
  [[ -f "${fake_agent_fixture}" ]] || die "missing fake agent fixture: ${fake_agent_fixture}"

  cd "${repo_root}"
  if (( preflight_only )); then
    log "Live coexistence E2E preflight passed"
    rm -rf "${work_dir}" >/dev/null 2>&1 || true
    exit 0
  fi

  trap 'status=$?; on_exit "${status}"; exit "${status}"' EXIT

  log "Creating or reusing kind cluster ${kind_cluster}"
  if ! kind get clusters 2>/dev/null | grep -qx "${kind_cluster}"; then
    run kind create cluster --name "${kind_cluster}" --kubeconfig "${KUBECONFIG}"
    cluster_created_by_run=1
  else
    warn "reusing pre-existing kind cluster ${kind_cluster}; it will not be deleted on exit"
    run kind export kubeconfig --name "${kind_cluster}" --kubeconfig "${KUBECONFIG}"
  fi
  run kubectl config use-context "kind-${kind_cluster}"

  if [[ "${cluster_created_by_run}" != "1" ]]; then
    log "Verifying the reused cluster holds no existing coexistence installation"
    assert_reused_cluster_is_unclaimed
  fi

  # Start the local registry with a per-run ownership label. The helper's
  # unowned form force-removes any same-named container, which could destroy
  # another run's registry when a pre-existing cluster is reused; the owned
  # form never removes, so a foreign registry surfaces as a clear refusal
  # instead of a docker name-conflict error.
  local registry_name
  registry_name="$(orka_kind_registry_name "${kind_cluster}")"
  if docker container ls --all --filter "name=^/${registry_name}$" --format '{{.ID}}' | grep -q .; then
    die "registry container ${registry_name} already exists and is not owned by this run; remove it or point KIND_CLUSTER at a dedicated cluster name"
  fi
  orka_kind_registry_start "${kind_cluster}" "${registry_owner}"

  if [[ "${skip_image_build}" != "1" ]]; then
    log "Building controller image ${manager_image}"
    run make docker-build IMG="${manager_image}"
    log "Building harness wrapper image ${wrapper_image}"
    run make docker-build-harness-wrapper HARNESS_WRAPPER_IMG="${wrapper_image}"
    log "Building workspace publisher image ${publisher_image}"
    run make docker-build-workspace-publisher WORKSPACE_PUBLISHER_IMG="${publisher_image}"
  else
    log "COEXISTENCE_SKIP_IMAGE_BUILD=1; reusing local images"
  fi

  log "Publishing immutable digest-pinned images to the kind-local registry"
  local manager_ref wrapper_ref publisher_ref
  manager_ref="$(orka_kind_registry_push "${manager_image}" "orka/controller")"
  wrapper_ref="$(orka_kind_registry_push "${wrapper_image}" "orka/agent-harness-wrapper")"
  publisher_ref="$(orka_kind_registry_push "${publisher_image}" "orka/workspace-publisher")"

  log "Applying the platform-owned shared CRD wave once for both installations"
  run bash "${script_dir}/apply-helm-crds.sh" "${chart_dir}" "kind-${kind_cluster}"

  log "Claiming mode-labeled namespaces before any workload write"
  run bash "${script_dir}/lib/ensure-static-mode-namespace.sh" kubectl "${v1_namespace}" harness-v1
  run bash "${script_dir}/lib/ensure-static-mode-namespace.sh" kubectl "${v2_namespace}" harness-v2
  # Stub namespace required by the v2 release's pinned Vekil ingress policy.
  kubectl create namespace vekil-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  log "Provisioning per-release test-only secrets"
  create_namespace_secrets "${v1_namespace}" "${v1_release}-webhook.${v1_namespace}.svc"
  create_namespace_secrets "${v2_namespace}" "${v2_release}-webhook.${v2_namespace}.svc"

  wrapper_token="$(openssl rand -hex 32)"
  kubectl -n "${v1_namespace}" create secret generic orka-wrapper-auth \
    --from-literal="token=${wrapper_token}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  local wrapper_tls_dir="${work_dir}/${v1_namespace}-wrapper-tls"
  generate_service_tls "${wrapper_deployment}.${v1_namespace}.svc" "${wrapper_tls_dir}"
  kubectl -n "${v1_namespace}" create secret generic orka-wrapper-tls \
    --type=kubernetes.io/tls \
    --from-file="tls.crt=${wrapper_tls_dir}/tls.crt" \
    --from-file="tls.key=${wrapper_tls_dir}/tls.key" \
    --from-file="ca.crt=${wrapper_tls_dir}/ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  log "Installing the harness-v1 release ${v1_release}"
  run helm install "${v1_release}" "${chart_dir}" \
    --namespace "${v1_namespace}" \
    --skip-crds \
    --set controller.mode=harness-v1 \
    --set "controller.watchNamespace=${v1_namespace}" \
    --set "controller.image.repository=$(split_image_repository "${manager_ref}")" \
    --set "controller.image.digest=$(split_image_digest "${manager_ref}")" \
    --set controller.agentExecutionSnapshot.existingSecret=orka-agent-snapshot-key \
    --set controller.agentExecutionSnapshot.key=key \
    --set webhooks.tls.existingSecret=orka-webhook-tls \
    --set "webhooks.caBundle=$(base64_no_wrap "${work_dir}/${v1_namespace}-webhook-tls/ca.crt")" \
    --set "harnessV1.image.repository=$(split_image_repository "${wrapper_ref}")" \
    --set "harnessV1.image.digest=$(split_image_digest "${wrapper_ref}")" \
    --set harnessV1.auth.existingSecret=orka-wrapper-auth \
    --set harnessV1.tls.existingSecret=orka-wrapper-tls

  log "Installing the harness-v2 release ${v2_release}"
  run helm install "${v2_release}" "${chart_dir}" \
    --namespace "${v2_namespace}" \
    --skip-crds \
    --set controller.mode=harness-v2 \
    --set "controller.watchNamespace=${v2_namespace}" \
    --set "controller.acpRuntime.namespace=${v2_runtime_namespace}" \
    --set "controller.image.repository=$(split_image_repository "${manager_ref}")" \
    --set "controller.image.digest=$(split_image_digest "${manager_ref}")" \
    --set controller.agentExecutionSnapshot.existingSecret=orka-agent-snapshot-key \
    --set controller.agentExecutionSnapshot.key=key \
    --set webhooks.tls.existingSecret=orka-webhook-tls \
    --set "webhooks.caBundle=$(base64_no_wrap "${work_dir}/${v2_namespace}-webhook-tls/ca.crt")" \
    --set "publisher.image.repository=$(split_image_repository "${publisher_ref}")" \
    --set "publisher.image.digest=$(split_image_digest "${publisher_ref}")" \
    --set providerProxy.enabled=true

  log "Wiring the deterministic fake agent CLI into the wrapper (test-only)"
  kubectl -n "${v1_namespace}" create configmap coexistence-fake-agent \
    --from-file="fake-agent.sh=${fake_agent_fixture}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  run kubectl -n "${v1_namespace}" patch deployment "${wrapper_deployment}" \
    --type=strategic \
    -p '{"spec":{"template":{"spec":{"containers":[{"name":"wrapper","env":[{"name":"ORKA_HARNESS_WRAPPER_RUNTIME","value":"generic"},{"name":"ORKA_HARNESS_WRAPPER_COMMAND","value":"/opt/orka-fake-agent/fake-agent.sh"}],"volumeMounts":[{"name":"fake-agent","mountPath":"/opt/orka-fake-agent","readOnly":true}]}],"volumes":[{"name":"fake-agent","configMap":{"name":"coexistence-fake-agent","defaultMode":365}}]}}}}'

  log "Waiting for both installations to become Ready"
  run kubectl -n "${v1_namespace}" rollout status "deployment/${v1_controller_deployment}" --timeout="${rollout_timeout}"
  run kubectl -n "${v1_namespace}" rollout status "deployment/${wrapper_deployment}" --timeout="${rollout_timeout}"
  run kubectl -n "${v2_namespace}" rollout status "deployment/${v2_controller_deployment}" --timeout="${rollout_timeout}"

  log "Asserting each controller declares its static mode and watch namespace"
  kubectl -n "${v1_namespace}" get deployment "${v1_controller_deployment}" -o json | jq -e \
    '.spec.template.spec.containers[0].args | (index("--controller-mode=harness-v1") != null) and (index("--watch-namespace=orka-v1-system") != null)' >/dev/null || \
    die "v1 controller does not declare --controller-mode=harness-v1 with its own watch namespace"
  kubectl -n "${v2_namespace}" get deployment "${v2_controller_deployment}" -o json | jq -e \
    '.spec.template.spec.containers[0].args | (index("--controller-mode=harness-v2") != null) and (index("--watch-namespace=orka-v2-system") != null)' >/dev/null || \
    die "v2 controller does not declare --controller-mode=harness-v2 with its own watch namespace"

  log "Asserting leader Leases live in their own watched namespaces"
  wait_until "v1 leader Lease" 120 kubectl -n "${v1_namespace}" get lease "${leader_lease}"
  wait_until "v2 leader Lease" 120 kubectl -n "${v2_namespace}" get lease "${leader_lease}"

  log "Proving admission direction: v2 contracts rejected in the v1 namespace"
  local zero_digest
  zero_digest="sha256:$(printf '0%.0s' {1..64})"
  cat >"${work_dir}/v2-runtime-in-v1.yaml" <<EOF_V2_RUNTIME
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: coexistence-v2-runtime-rejected, namespace: ${v1_namespace}}
spec:
  contractVersion: orka.harness.v2
  deployment: {mode: external-endpoint, endpoint: "https://runtime.example.com"}
  clientAuth:
    controllerBearerTokenSecretRef: {name: auth, key: controller}
    operationCapabilitySecretRef: {name: auth, key: capability}
  capabilities:
    runtimeInstanceID: instance-1
    profile:
      digest: ${zero_digest}
      digestSchemaVersion: 1
      acpProfile: acp.v1
      adapterName: adapter
      adapterDigest: ${zero_digest}
      providerKind: codex
      model: gpt-5.2-codex
      agentConfigurationDigest: ${zero_digest}
      toolPolicyDigest: ${zero_digest}
      approvalPolicyDigest: ${zero_digest}
      mcpConfigurationDigest: ${zero_digest}
      workspaceIntent: read
      proxyCredentialRole: provider-inference
      proxyCredentialScope: "model:gpt-5.2-codex"
      resourceClass: standard
    mcpPolicy:
      allowedTools: []
      disallowedTools: []
      allowBash: false
      approvalRequiredTools: []
    limits:
      maxResidentSessions: 10
      maxConcurrentPrompts: 4
      maxRequestBytes: 1048576
      maxEventLineBytes: 1048576
      maxTerminalResultBytes: 1048576
      maxBufferedEvents: 256
      maxUpdateEventsPerSecond: 100
      minPromptLeaseMillis: 5000
      maxPromptLeaseMillis: 120000
      maxPendingPermissions: 32
      maxWorkspaceDeltaBytes: 536870912
    supportsDrain: true
    workspaceGovernance:
      mode: strict-governed
      trusted: false
      orkaOwnedWorkspaceDeltas: true
      promptScopedBrokerAuthorization: true
      noDirectSCMPublication: true
      orkaOwnedCleanRoomPublication: true
      exactInstanceFencing: true
      duplicateSafeMutations: true
      cancellationSettlement: true
EOF_V2_RUNTIME
  expect_admission_denied "orka.harness.v2 AgentRuntime in the harness-v1 namespace" \
    'AgentRuntime contractVersion must match namespace execution mode "harness-v1"' \
    "${work_dir}/v2-runtime-in-v1.yaml"

  cat >"${work_dir}/v2-agent-in-v1.yaml" <<EOF_V2_AGENT
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: coexistence-v2-agent-rejected, namespace: ${v1_namespace}}
spec:
  runtime: {type: codex, contractVersion: orka.harness.v2}
  model: {name: gpt-5.2-codex}
EOF_V2_AGENT
  expect_admission_denied "orka.harness.v2 Agent in the harness-v1 namespace" \
    'Agent contractVersion must match namespace execution mode "harness-v1"' \
    "${work_dir}/v2-agent-in-v1.yaml"

  log "Proving admission direction: v1 contracts rejected in the v2 namespace"
  cat >"${work_dir}/v1-runtime-in-v2.yaml" <<EOF_V1_RUNTIME
apiVersion: core.orka.ai/v1alpha1
kind: AgentRuntime
metadata: {name: coexistence-v1-runtime-rejected, namespace: ${v2_namespace}}
spec:
  contractVersion: orka.harness.v1
  deployment: {mode: external-endpoint, endpoint: "https://runtime.example.com"}
  clientAuth:
    bearerTokenSecretRef: {name: runtime-auth, key: token}
EOF_V1_RUNTIME
  expect_admission_denied "orka.harness.v1 AgentRuntime in the harness-v2 namespace" \
    'AgentRuntime contractVersion must match namespace execution mode "harness-v2"' \
    "${work_dir}/v1-runtime-in-v2.yaml"

  cat >"${work_dir}/v1-agent-in-v2.yaml" <<EOF_V1_AGENT
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata: {name: coexistence-v1-agent-rejected, namespace: ${v2_namespace}}
spec:
  runtime: {type: codex, contractVersion: orka.harness.v1}
  model: {name: gpt-5.2-codex}
EOF_V1_AGENT
  expect_admission_denied "orka.harness.v1 Agent in the harness-v2 namespace" \
    'Agent contractVersion must match namespace execution mode "harness-v2"' \
    "${work_dir}/v1-agent-in-v2.yaml"

  log "Executing a real harness v1 wrapper Task end to end (model-free)"
  cat <<EOF_AGENT | kubectl apply -f -
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: coexistence-v1-agent
  namespace: ${v1_namespace}
spec:
  systemPrompt:
    inline: "Deterministic coexistence test agent."
  runtime:
    contractVersion: orka.harness.v1
    type: codex
    defaultMaxTurns: 1
    defaultAllowBash: false
    defaultAllowedTools: []
EOF_AGENT

  local run_id nonce_one nonce_two
  run_id="${GITHUB_RUN_ID:-local}-$(date +%s)"
  nonce_one="coexistence-turn-${run_id}-${RANDOM}"
  cat <<EOF_TASK_ONE | kubectl apply -f -
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: coexistence-v1-task
  namespace: ${v1_namespace}
spec:
  type: agent
  agentRef:
    name: coexistence-v1-agent
  prompt: "Echo the coexistence nonce ${nonce_one}."
  timeout: "5m"
EOF_TASK_ONE
  wait_until "Task/coexistence-v1-task terminal success" "${task_wait_seconds}" \
    task_phase_is coexistence-v1-task Succeeded
  assert_task_succeeded coexistence-v1-task

  log "Fetching the persisted v1 result through the v1 controller API"
  api_token="$(kubectl -n "${v1_namespace}" create token orka-client)"
  assert_result_contains coexistence-v1-task "${nonce_one}"

  log "Restarting the v1 controller while a wrapper turn is actively held open"
  nonce_two="coexistence-restart-${run_id}-${RANDOM}"
  cat <<EOF_TASK_TWO | kubectl apply -f -
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: coexistence-v1-restart-task
  namespace: ${v1_namespace}
spec:
  type: agent
  agentRef:
    name: coexistence-v1-agent
  prompt: "COEXISTENCE_HOLD_TURN then echo ${nonce_two}."
  timeout: "10m"
EOF_TASK_TWO
  # The dispatcher projects "Submitting" while it blocks on the live wrapper
  # turn stream; Accepted/Running remain internal durable-store states until a
  # terminal or admission-closed projection.
  wait_until "Task/coexistence-v1-restart-task active attempt" "${task_wait_seconds}" \
    task_harness_state_in coexistence-v1-restart-task Submitting SubmittedUnknown Accepted Running
  # Controller-side "Submitting" is stamped before StartTurn reaches the
  # wrapper, so additionally wait for the wrapper-side hold marker. The wrapper
  # starts the fake agent child only after durably recording TurnAccepted in
  # its admission ledger (StartTurn flow in
  # workers/harness/cliwrapper/server.go), so the marker file the fake agent
  # writes proves the wrapper durably admitted the held turn before we restart.
  wait_until "wrapper-side durable admission of the held turn" "${task_wait_seconds}" \
    kubectl -n "${v1_namespace}" exec "deployment/${wrapper_deployment}" -- \
    test -f /tmp/coexistence-hold-turn-active
  cleanup_port_forward
  local pre_restart_pod post_restart_pod
  pre_restart_pod="$(kubectl -n "${v1_namespace}" get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')"
  log "Sampling v2 controller readiness continuously across the v1 restart"
  start_v2_readiness_monitor
  run kubectl -n "${v1_namespace}" rollout restart "deployment/${v1_controller_deployment}"
  run kubectl -n "${v1_namespace}" rollout status "deployment/${v1_controller_deployment}" --timeout="${rollout_timeout}"
  post_restart_pod="$(kubectl -n "${v1_namespace}" get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "${post_restart_pod}" && "${post_restart_pod}" != "${pre_restart_pod}" ]] || \
    die "v1 controller Pod was not replaced by the restart"
  stop_v2_readiness_monitor
  if [[ -s "${v2_readiness_violations}" ]]; then
    cat "${v2_readiness_violations}" | redact >&2
    die "v2 controller lost readiness during the v1 controller restart"
  fi

  log "Asserting ledger-backed recovery settles the in-flight v1 Task"
  wait_until "Task/coexistence-v1-restart-task settlement after controller restart" "${task_wait_seconds}" \
    task_phase_is coexistence-v1-restart-task Succeeded
  assert_task_succeeded coexistence-v1-restart-task
  api_token="$(kubectl -n "${v1_namespace}" create token orka-client)"
  assert_result_contains coexistence-v1-restart-task "${nonce_two}"
  cleanup_port_forward
  deployment_ready "${v2_namespace}" "${v2_controller_deployment}" || \
    die "v2 controller was not Ready after the restarted v1 Task settled"

  log "Proving the isolation matrix: cross-namespace RBAC denial"
  expect_can_i yes "${v1_controller_sa}" get tasks.core.orka.ai "${v1_namespace}"
  expect_can_i yes "${v2_controller_sa}" get tasks.core.orka.ai "${v2_namespace}"
  local verb resource
  for verb in get list create update delete; do
    for resource in tasks.core.orka.ai agents.core.orka.ai sessions.core.orka.ai; do
      expect_can_i no "${v1_controller_sa}" "${verb}" "${resource}" "${v2_namespace}"
      expect_can_i no "${v2_controller_sa}" "${verb}" "${resource}" "${v1_namespace}"
    done
  done
  expect_can_i no "${v1_controller_sa}" get secrets "${v2_namespace}"
  expect_can_i no "${v2_controller_sa}" get secrets "${v1_namespace}"

  local forbidden_err="${work_dir}/forbidden.err"
  if kubectl get tasks -n "${v2_namespace}" --as="${v1_controller_sa}" >"${forbidden_err}" 2>&1; then
    die "v1 controller ServiceAccount unexpectedly read Tasks in ${v2_namespace}"
  fi
  grep -Fq "forbidden" "${forbidden_err}" || die "cross-namespace v1->v2 Task read was not denied with Forbidden"
  if kubectl get tasks -n "${v1_namespace}" --as="${v2_controller_sa}" >"${forbidden_err}" 2>&1; then
    die "v2 controller ServiceAccount unexpectedly read Tasks in ${v1_namespace}"
  fi
  grep -Fq "forbidden" "${forbidden_err}" || die "cross-namespace v2->v1 Task read was not denied with Forbidden"

  log "Proving the isolation matrix: NetworkPolicies exist and select the expected pods"
  local entry policy_namespace netpol component selector
  for entry in \
    "${v1_namespace}/${wrapper_deployment}/agent-harness-wrapper" \
    "${v2_namespace}/${v2_release}-provider-auth-proxy/provider-auth-proxy" \
    "${v2_namespace}/${v2_release}-workspace-publisher/workspace-publisher" \
    "${v2_namespace}/${v2_release}-scm-egress-proxy/scm-egress-proxy"; do
    IFS=/ read -r policy_namespace netpol component <<<"${entry}"
    selector="$(kubectl -n "${policy_namespace}" get networkpolicy "${netpol}" -o jsonpath='{.spec.podSelector.matchLabels.app\.kubernetes\.io/component}')" || \
      die "expected NetworkPolicy ${netpol} in ${policy_namespace}"
    [[ "${selector}" == "${component}" ]] || \
      die "NetworkPolicy ${netpol} in ${policy_namespace} selects component '${selector}', want '${component}'"
    kubectl -n "${policy_namespace}" get pods -l "app.kubernetes.io/component=${component}" -o name | grep -q pod/ || \
      die "NetworkPolicy ${netpol} in ${policy_namespace} selects no running pods (component=${component})"
  done

  log "Running scripts/check-legacy-wrapper-resources.sh in its coexistence branch"
  local legacy_out="${work_dir}/check-legacy.out"
  if ! COEXISTENCE=1 bash "${script_dir}/check-legacy-wrapper-resources.sh" >"${legacy_out}" 2>&1; then
    cat "${legacy_out}" | redact >&2
    die "check-legacy-wrapper-resources.sh COEXISTENCE=1 failed while a compliant wrapper exists"
  fi
  grep -Fq "coexistence mode: wrapper resources are allowed" "${legacy_out}" || \
    die "check-legacy-wrapper-resources.sh COEXISTENCE=1 did not report the coexistence verdict"

  log "Asserting the default v2-only branch fails while the wrapper exists"
  if bash "${script_dir}/check-legacy-wrapper-resources.sh" >"${legacy_out}" 2>&1; then
    die "check-legacy-wrapper-resources.sh default branch passed despite live wrapper resources"
  fi
  grep -Fq "legacy harness-wrapper resources remain" "${legacy_out}" || \
    die "check-legacy-wrapper-resources.sh default branch did not report the wrapper inventory"

  log "Deleting test-created v1 Tasks and Agent through the API before retirement"
  # Wait for the v1 controller and wrapper to settle each orka.ai/cleanup
  # finalizer before retiring either component. This proves the production
  # deletion path instead of bypassing its wrapper-settlement barrier.
  run kubectl -n "${v1_namespace}" delete task coexistence-v1-task coexistence-v1-restart-task \
    --ignore-not-found --wait=true --timeout=120s
  run kubectl -n "${v1_namespace}" delete agent coexistence-v1-agent \
    --ignore-not-found --timeout=120s

  log "Uninstalling the v1 release and asserting the v2 installation is unaffected"
  run helm uninstall "${v1_release}" --namespace "${v1_namespace}" --timeout 5m
  wait_until "wrapper Deployment removal" 120 bash -c \
    "! kubectl -n '${v1_namespace}' get deployment '${wrapper_deployment}' >/dev/null 2>&1"

  # helm uninstall removes only release-managed objects. The script-created
  # Secrets/ConfigMap use custom names the legacy-resource check does not
  # match, and the release's ledger PVC carries helm.sh/resource-policy: keep,
  # so on a retained/reused cluster all of them would silently survive v1
  # retirement. Delete every remaining test-created object explicitly, then
  # prove nothing test-owned is left before asserting retirement.
  log "Deleting test-created v1 Secrets, ConfigMap, and the kept ledger PVC"
  run kubectl -n "${v1_namespace}" delete secret \
    orka-agent-snapshot-key orka-webhook-tls orka-wrapper-auth orka-wrapper-tls \
    --ignore-not-found
  run kubectl -n "${v1_namespace}" delete configmap coexistence-fake-agent --ignore-not-found
  run kubectl -n "${v1_namespace}" delete pvc "${v1_release}-harness-v1-ledger" \
    --ignore-not-found --timeout=120s

  log "Asserting no test-created objects survive v1 retirement"
  local leftover
  leftover="$(kubectl -n "${v1_namespace}" get \
    secret/orka-agent-snapshot-key secret/orka-webhook-tls \
    secret/orka-wrapper-auth secret/orka-wrapper-tls \
    configmap/coexistence-fake-agent \
    "pvc/${v1_release}-harness-v1-ledger" \
    task/coexistence-v1-task task/coexistence-v1-restart-task \
    agent/coexistence-v1-agent \
    --ignore-not-found -o name 2>/dev/null)" || leftover="query-failed"
  [[ -z "${leftover}" ]] || die "test-created objects survived v1 retirement: ${leftover}"
  deployment_ready "${v2_namespace}" "${v2_controller_deployment}" || \
    die "v2 controller lost readiness after the v1 release uninstall"
  kubectl get crd tasks.core.orka.ai >/dev/null || \
    die "shared Task CRD disappeared with the v1 release uninstall"
  kubectl get crd agentruntimes.core.orka.ai >/dev/null || \
    die "shared AgentRuntime CRD disappeared with the v1 release uninstall"

  log "Asserting the default v2-only branch passes after v1 retirement"
  if ! bash "${script_dir}/check-legacy-wrapper-resources.sh" >"${legacy_out}" 2>&1; then
    cat "${legacy_out}" | redact >&2
    die "check-legacy-wrapper-resources.sh default branch failed after the v1 release uninstall"
  fi

  log "Live coexistence e2e passed"
}

main "$@"
