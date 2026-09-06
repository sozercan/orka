#!/usr/bin/env bash

set -Eeuo pipefail

sanitize_image_tag() {
  printf '%s' "$1" | LC_ALL=C tr -c 'A-Za-z0-9_.-' '-'
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=scripts/lib/e2e-common.sh
. "${script_dir}/lib/e2e-common.sh"
# shellcheck source=scripts/lib/kind-local-registry.sh
. "${script_dir}/lib/kind-local-registry.sh"
# shellcheck source=scripts/lib/e2e-admission-tls.sh
. "${script_dir}/lib/e2e-admission-tls.sh"

agent_sandbox_version="${AGENT_SANDBOX_VERSION:-v1.0.0}"
kind_cluster="${KIND_CLUSTER:-orka-live-agent-sandbox-e2e}"
orka_namespace="${ORKA_NAMESPACE:-orka-system}"
orka_controller_deployment="${ORKA_CONTROLLER_DEPLOYMENT:-orka-controller-manager}"
orka_api_service="${ORKA_API_SERVICE:-orka-api}"
orka_api_service_port="${ORKA_API_SERVICE_PORT:-8080}"
orka_api_local_port="${ORKA_API_LOCAL_PORT:-18084}"
orka_api_client_service_account="${ORKA_API_CLIENT_SERVICE_ACCOUNT:-orka-client}"
router_api_local_port="${ORKA_AGENT_SANDBOX_ROUTER_LOCAL_PORT:-18085}"
e2e_run_id="$(sanitize_image_tag "${ORKA_AGENT_SANDBOX_RUN_ID:-${GITHUB_RUN_ID:-manual}-$(date -u +%Y%m%d%H%M%S)}")"
manager_image="${ORKA_MANAGER_IMAGE:-orka-controller:live-agent-sandbox-e2e-${e2e_run_id}}"
publisher_image="${ORKA_WORKSPACE_PUBLISHER_IMAGE:-orka-workspace-publisher:live-agent-sandbox-e2e-${e2e_run_id}}"
sandbox_fixture_image="${ORKA_AGENT_SANDBOX_FIXTURE_IMAGE:-orka-agent-sandbox-fixture:live-agent-sandbox-e2e-${e2e_run_id}}"
sandbox_router_image="${ORKA_AGENT_SANDBOX_ROUTER_IMAGE:-orka-agent-sandbox-router:live-agent-sandbox-e2e-${e2e_run_id}}"
responses_fixture_image="${ORKA_RESPONSES_FIXTURE_IMAGE:-orka-openai-responses-fixture:live-agent-sandbox-e2e-${e2e_run_id}}"
sandbox_template_name="${ORKA_AGENT_SANDBOX_TEMPLATE:-orka-agent-sandbox-e2e-template}"
smoke_claim_name="${ORKA_AGENT_SANDBOX_SMOKE_CLAIM:-orka-agent-sandbox-e2e-retained-smoke}"
# The workspace-backed ACP Task smoke builds the real Codex runtime image and
# proves the workspace-provider-backed RuntimePool path live against upstream
# agent-sandbox (admission, claim materialization, authenticated Serving, and
# cleanup), then completes a real prompt through the authenticated provider
# proxy and the local Responses-compatible fixture. Set to 0 to skip the
# runtime image build and smoke.
acp_task_smoke_enabled="${ORKA_AGENT_SANDBOX_ACP_TASK_SMOKE:-1}"
# The class-backed suspend/cold-resume conformance (issue #425) enables the
# workspace provider API, provisions a suspendable ExecutionWorkspaceClass, and
# proves PVC-backed data-only suspension plus exact-Sandbox cold resume live.
# It reuses the Codex runtime image and Responses fixture from the smoke.
suspend_resume_enabled="${ORKA_AGENT_SANDBOX_SUSPEND_RESUME:-1}"
# The lifecycle/recovery conformance (issue #411) proves Session continuation,
# explicit cancellation, controller restart, and physical replacement through
# a workspace-provider-backed RuntimePool, using the fixture's hold markers
# and request counters for exactly-once delivery proof.
lifecycle_enabled="${ORKA_AGENT_SANDBOX_LIFECYCLE:-1}"
lifecycle_ambiguity_marker="ORKA_E2E_WS_LC_AMBIGUOUS_OK"
fixture_local_port="${ORKA_RESPONSES_FIXTURE_LOCAL_PORT:-18337}"
acp_suspend_agent_name="orka-ws-suspend-agent"
acp_suspend_class_name="acp-sandbox-suspend"
acp_suspend_session_name="orka-ws-suspend-session"
acp_codex_runtime_image="${ORKA_ACP_CODEX_RUNTIME_IMAGE:-orka-acp-codex-runtime:live-agent-sandbox-e2e-${e2e_run_id}}"
acp_runtime_namespace="${ORKA_ACP_RUNTIME_NAMESPACE:-orka-runtimes}"
acp_task_namespace="${ORKA_AGENT_SANDBOX_ACP_TASK_NAMESPACE:-${orka_namespace}}"
acp_task_name="orka-ws-sandbox-smoke"
acp_agent_name="orka-ws-sandbox-agent"
api_pf_pid=""
router_pf_pid=""
fixture_pf_pid=""
router_namespace=""
created_kind_cluster="0"
agent_sandbox_module_cache=""
work_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/live-agent-sandbox-e2e.XXXXXX")"
e2e_kubeconfig="${work_dir}/kubeconfig"
export KUBECONFIG="${e2e_kubeconfig}"
kind_config="${ORKA_AGENT_SANDBOX_KIND_CONFIG:-${work_dir}/kind-config.yaml}"
fixture_dockerfile="${work_dir}/Dockerfile.sandbox-fixture"
api_pf_log="${work_dir}/api-port-forward.log"
router_pf_log="${work_dir}/router-port-forward.log"
smoke_go_dir="${repo_root}/.tmp-live-agent-sandbox-smoke-${e2e_run_id}"
manager_kustomization="${repo_root}/config/manager/kustomization.yaml"
manager_kustomization_backup="${work_dir}/manager-kustomization.yaml.bak"

if [[ "${agent_sandbox_version}" != "v1.0.0" ]]; then
  die "this e2e is pinned to agent-sandbox v1.0.0 to match go.mod"
fi

cleanup_one_port_forward() {
  local pid="$1"
  if [[ -n "${pid}" ]]; then
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
    wait "${pid}" 2>/dev/null || true
  fi
}

cleanup_port_forward() {
  cleanup_one_port_forward "${api_pf_pid}"
  api_pf_pid=""
  cleanup_one_port_forward "${router_pf_pid}"
  router_pf_pid=""
  cleanup_one_port_forward "${fixture_pf_pid}"
  fixture_pf_pid=""
}

restore_manager_kustomization() {
  if [[ -f "${manager_kustomization_backup}" ]]; then
    cp "${manager_kustomization_backup}" "${manager_kustomization}" || true
  fi
}

dump_diagnostics() {
  log "Collecting diagnostics"

  {
    echo "=== Current Kubernetes Context ==="
    kubectl config current-context 2>/dev/null || true
    echo
    echo "=== Orka Namespace Resources ==="
    kubectl get pods,svc,deploy,jobs,tasks,agents,sandboxclaims,sandboxes,sandboxtemplates,sandboxwarmpools -n "${orka_namespace}" -o wide 2>/dev/null || true
    echo
    echo "=== Agent Sandbox Resources ==="
    kubectl get pods,svc,deploy,sandboxclaims,sandboxes,sandboxtemplates,sandboxwarmpools -A -o wide 2>/dev/null || true
    echo
    echo "=== Workspace-backed RuntimePools ==="
    kubectl get runtimepools -A -o wide 2>/dev/null || true
    kubectl get runtimepools -A -o yaml 2>/dev/null || true
    echo
    echo "=== ACP Runtime Namespace Resources ==="
    kubectl get pods,secrets,sandboxclaims,sandboxes,sandboxtemplates,sandboxwarmpools -n "${acp_runtime_namespace}" -o wide 2>/dev/null || true
    echo
    echo "=== Responses Fixture ==="
    # Diagnostics are limited to the fixture's own fixed-name Tasks, with the
    # prompt and result bodies stripped: a reused cluster or customized task
    # namespace can hold unrelated Tasks whose prompts and results may carry
    # credentials or other sensitive user input that must never land in logs.
    kubectl -n "${acp_task_namespace}" get tasks -o wide 2>/dev/null || true
    local diag_task
    for diag_task in "${acp_task_name}" orka-ws-suspend-first orka-ws-suspend-second; do
      echo "--- task/${diag_task} (prompt and result redacted) ---"
      kubectl -n "${acp_task_namespace}" get task "${diag_task}" -o json 2>/dev/null |
        jq 'del(.spec.prompt) | del(.status.result) | del(.status.execution.result)' 2>/dev/null || true
    done
    kubectl -n "${acp_task_namespace}" get events --sort-by=.metadata.creationTimestamp 2>/dev/null | tail -80 || true
    kubectl -n "${acp_runtime_namespace}" get events --sort-by=.metadata.creationTimestamp 2>/dev/null | tail -40 || true
    echo
    echo "=== Durability Probe Pods ==="
    local diag_probe
    for diag_probe in orka-ws-durability-writer orka-ws-durability-reader; do
      echo "--- pod/${diag_probe} logs ---"
      kubectl -n "${acp_runtime_namespace}" logs "${diag_probe}" --all-containers 2>/dev/null || true
      kubectl -n "${acp_runtime_namespace}" get pod "${diag_probe}" -o jsonpath='{.status.containerStatuses[*].state}' 2>/dev/null || true
      echo
    done
    kubectl get executionworkspaceproviders,runtimeproviderconfigs -o yaml 2>/dev/null || true
    kubectl -n "${acp_task_namespace}" get executionworkspaceclasses,executionworkspaces,runtimeworkspaceprofiles,runtimepools -o yaml 2>/dev/null || true
    kubectl get pods,svc,deploy -n vekil-system -o wide 2>/dev/null || true
    kubectl logs deployment/vekil -n vekil-system --tail=300 2>/dev/null || true
    echo
    echo "=== Workspace-backed ACP Task ==="
    kubectl -n "${acp_task_namespace}" get task "${acp_task_name}" -o yaml 2>/dev/null || true
    echo
    echo "=== Orka Namespace Events ==="
    kubectl get events -n "${orka_namespace}" --sort-by=.lastTimestamp 2>/dev/null || true
    echo
    echo "=== Agent Sandbox System Events ==="
    kubectl get events -n agent-sandbox-system --sort-by=.lastTimestamp 2>/dev/null || true
    echo
    echo "=== Controller Logs ==="
    kubectl logs deployment/"${orka_controller_deployment}" -n "${orka_namespace}" -c manager --tail=300 2>/dev/null || true
    echo
    echo "=== Agent Sandbox Controller Logs ==="
    kubectl logs deployment/agent-sandbox-controller -n agent-sandbox-system --tail=300 2>/dev/null || true
    echo
    echo "=== Sandbox Router Logs ==="
    if [[ -n "${router_namespace}" ]]; then
      kubectl logs deployment/sandbox-router-deployment -n "${router_namespace}" --tail=300 2>/dev/null || true
    fi
    echo
    echo "=== API Port-forward Log ==="
    if [[ -f "${api_pf_log}" ]]; then
      cat "${api_pf_log}" 2>/dev/null || true
    fi
    echo
    echo "=== Router Port-forward Log ==="
    if [[ -f "${router_pf_log}" ]]; then
      cat "${router_pf_log}" 2>/dev/null || true
    fi
  } >&2
}

on_exit() {
  local status="$1"
  set +e

  if [[ "${status}" -ne 0 ]]; then
    if [[ "$(kubectl config current-context 2>/dev/null || true)" == "kind-${kind_cluster}" ]]; then
      dump_diagnostics
    else
      warn "skipping Kubernetes diagnostics because the current context is not kind-${kind_cluster}"
    fi
  fi

  cleanup_port_forward
  restore_manager_kustomization
  orka_kind_registry_stop
  if [[ "${created_kind_cluster}" == "1" ]]; then
    kind delete cluster --name "${kind_cluster}" >/dev/null 2>&1 || true
  fi
  rm -rf "${smoke_go_dir}" >/dev/null 2>&1 || true
  rm -rf "${work_dir}" >/dev/null 2>&1 || true

  if [[ "${status}" -ne 0 ]]; then
    log "Live agent-sandbox e2e failed"
  fi
}

run() {
  printf '+ ' >&2
  printf '%q ' "$@" >&2
  printf '\n' >&2
  "$@"
}

write_pod_file_as_directory_owner() {
  local pod_name="$1"
  local directory="$2"
  local file_path="$3"
  local contents="$4"

  run kubectl -n "${acp_runtime_namespace}" exec "${pod_name}" -- node -e '
const fs = require("node:fs");
const directory = process.argv[1];
const filePath = process.argv[2];
const contents = process.argv[3];
const owner = fs.statSync(directory);

process.setgroups([]);
process.setgid(owner.gid);
process.setuid(owner.uid);

const fd = fs.openSync(filePath, "w");
try {
  fs.writeFileSync(fd, contents);
  fs.fsyncSync(fd);
} finally {
  fs.closeSync(fd);
}
' "${directory}" "${file_path}" "${contents}"
}

wait_for_pod_directory() {
  local pod_name="$1"
  local directory="$2"
  local attempts_remaining="${3:-120}"

  while (( attempts_remaining > 0 )); do
    if kubectl -n "${acp_runtime_namespace}" exec "${pod_name}" -- node -e '
const fs = require("node:fs");
const stat = fs.statSync(process.argv[1]);
if (!stat.isDirectory()) {
  process.exit(1);
}
' "${directory}" >/dev/null 2>&1; then
      return 0
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 1
  done

  die "runtime Pod ${pod_name} never created session directory ${directory}"
}

read_pod_file_as_directory_owner() {
  local pod_name="$1"
  local directory="$2"
  local file_path="$3"

  kubectl -n "${acp_runtime_namespace}" exec "${pod_name}" -- node -e '
const fs = require("node:fs");
const directory = process.argv[1];
const filePath = process.argv[2];
const owner = fs.statSync(directory);

process.setgroups([]);
process.setgid(owner.gid);
process.setuid(owner.uid);
process.stdout.write(fs.readFileSync(filePath, "utf8"));
' "${directory}" "${file_path}"
}

remove_pod_file_as_directory_owner() {
  local pod_name="$1"
  local directory="$2"
  local file_path="$3"

  run kubectl -n "${acp_runtime_namespace}" exec "${pod_name}" -- node -e '
const fs = require("node:fs");
const directory = process.argv[1];
const filePath = process.argv[2];
const owner = fs.statSync(directory);

process.setgroups([]);
process.setgid(owner.gid);
process.setuid(owner.uid);
fs.unlinkSync(filePath);

const directoryFd = fs.openSync(directory, "r");
try {
  fs.fsyncSync(directoryFd);
} finally {
  fs.closeSync(directoryFd);
}
' "${directory}" "${file_path}"
}

kind_cluster_exists() {
  kind get clusters | grep -Fxq "${kind_cluster}"
}

write_default_kind_config() {
  cat >"${kind_config}" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
YAML
}

setup_kind_cluster() {
  if kind_cluster_exists; then
    log "Kind cluster ${kind_cluster} already exists; reusing it"
    run kind export kubeconfig --name "${kind_cluster}" --kubeconfig "${e2e_kubeconfig}"
    return
  fi

  if [[ -z "${ORKA_AGENT_SANDBOX_KIND_CONFIG:-}" ]]; then
    write_default_kind_config
  fi
  [[ -f "${kind_config}" ]] || die "Kind config not found: ${kind_config}"

  log "Creating Kind cluster ${kind_cluster}"
  run kind create cluster --name "${kind_cluster}" --config "${kind_config}" --kubeconfig "${e2e_kubeconfig}"
  created_kind_cluster="1"
}

start_port_forward() {
  local namespace_arg="$1"
  local resource="$2"
  local local_port="$3"
  local remote_port="$4"
  local logfile="$5"

  kubectl -n "${namespace_arg}" port-forward "${resource}" "${local_port}:${remote_port}" >>"${logfile}" 2>&1 &
  echo $!
}

wait_for_http() {
  local url="$1"
  local description="$2"
  local attempts_remaining=90

  while (( attempts_remaining > 0 )); do
    if curl -fsS --connect-timeout 5 --max-time 10 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "${api_pf_pid}" ]] && ! kill -0 "${api_pf_pid}" 2>/dev/null; then
      warn "API port-forward exited while waiting for ${description}; restarting"
      wait "${api_pf_pid}" 2>/dev/null || true
      api_pf_pid="$(start_port_forward "${orka_namespace}" "svc/${orka_api_service}" "${orka_api_local_port}" "${orka_api_service_port}" "${api_pf_log}")"
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done

  die "${description} never became available at ${url}"
}

assert_task_result_contains() {
  local namespace_arg="$1"
  local task_name="$2"
  local expected_marker="$3"
  local api_base="http://127.0.0.1:${orka_api_local_port}"
  local api_token result_file status attempts_remaining

  wait_for_http "${api_base}/readyz" "Orka API /readyz"
  api_token="$(kubectl -n "${namespace_arg}" create token "${orka_api_client_service_account}")"
  result_file="${work_dir}/${task_name}-result.json"
  attempts_remaining=15
  while (( attempts_remaining > 0 )); do
    status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
      --header "Authorization: Bearer ${api_token}" \
      --output "${result_file}" --write-out '%{http_code}' \
      "${api_base}/api/v1/tasks/${task_name}/result?namespace=${namespace_arg}" \
      2>>"${api_pf_log}" || true)"
    if [[ "${status}" == "200" ]] &&
      jq -er '.result' "${result_file}" | grep -Fq "${expected_marker}"; then
      log "Task/${task_name} result contains ${expected_marker}"
      return 0
    fi
    attempts_remaining=$((attempts_remaining - 1))
    sleep 2
  done

  die "Task/${task_name} result did not contain the expected marker ${expected_marker} (last HTTP status: ${status:-none})"
}

assert_fixture_marker_count() {
  local marker="$1"
  local expected_count="$2"
  local marker_digest counts_payload observed_count

  marker_digest="$(printf '%s' "${marker}" | openssl dgst -sha256 | awk '{print substr($NF, 1, 16)}')"
  [[ "${#marker_digest}" == "16" ]] || die "could not digest fixture marker ${marker}"
  counts_payload="$(kubectl get --raw \
    '/api/v1/namespaces/vekil-system/services/http:vekil:1337/proxy/fixture/marker-counts')" ||
    die "could not read Responses fixture marker counts"
  observed_count="$(jq -er --arg digest "${marker_digest}" '.[$digest] // 0' <<<"${counts_payload}")" ||
    die "Responses fixture returned invalid marker counts"
  [[ "${observed_count}" == "${expected_count}" ]] ||
    die "Responses fixture observed marker ${marker} ${observed_count} times, want ${expected_count}"
  log "Responses fixture observed marker ${marker} exactly ${expected_count} time(s)"
}

delete_session_if_present() {
  local namespace_arg="$1"
  local session_name="$2"
  local api_base="http://127.0.0.1:${orka_api_local_port}"
  local api_token status attempts_remaining

  wait_for_http "${api_base}/readyz" "Orka API /readyz"
  api_token="$(kubectl -n "${namespace_arg}" create token "${orka_api_client_service_account}")"
  attempts_remaining=30
  while (( attempts_remaining > 0 )); do
    status="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
      --request DELETE \
      --header "Authorization: Bearer ${api_token}" \
      --output /dev/null --write-out '%{http_code}' \
      "${api_base}/api/v1/sessions/${session_name}?namespace=${namespace_arg}" \
      2>>"${api_pf_log}" || true)"
    case "${status}" in
    204 | 404)
      log "Session/${session_name} is absent"
      return 0
      ;;
    409 | "")
      attempts_remaining=$((attempts_remaining - 1))
      sleep 2
      ;;
    *)
      die "delete Session/${session_name} returned HTTP ${status}"
      ;;
    esac
  done

  die "Session/${session_name} remained active or unsettled during cleanup"
}

deploy_responses_fixture() {
  log "Deploying local Responses-compatible provider fixture"
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
          image: ${responses_fixture_image}
          imagePullPolicy: IfNotPresent
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
  run kubectl -n vekil-system rollout restart deployment/vekil
  run kubectl -n vekil-system rollout status deployment/vekil --timeout=2m
}

ensure_api_client_identity() {
  log "Creating scoped Orka API client identity ${acp_task_namespace}/${orka_api_client_service_account}"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${orka_api_client_service_account}
  namespace: ${acp_task_namespace}
automountServiceAccountToken: false
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${orka_api_client_service_account}
  namespace: ${acp_task_namespace}
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
  name: ${orka_api_client_service_account}
  namespace: ${acp_task_namespace}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ${orka_api_client_service_account}
subjects:
  - kind: ServiceAccount
    name: ${orka_api_client_service_account}
    namespace: ${acp_task_namespace}
YAML
}

write_sandbox_fixture_dockerfile() {
  cat >"${fixture_dockerfile}" <<'DOCKERFILE'
FROM --platform=$BUILDPLATFORM golang:1.27.0 AS builder

ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN cat >/tmp/sandbox-runtime.go <<'GO'
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const appRoot = "/app"

type executeRequest struct {
	Command string `json:"command"`
}

type executeResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

type listEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

func main() {
	if err := os.MkdirAll(appRoot, 0o755); err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", health)
	mux.HandleFunc("/execute", execute)
	mux.HandleFunc("/upload", upload)
	mux.HandleFunc("/download/", download)
	mux.HandleFunc("/list/", list)
	mux.HandleFunc("/exists/", exists)
	if err := http.ListenAndServe(":8888", mux); err != nil {
		panic(err)
	}
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func execute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, executeResponse{Stderr: err.Error(), ExitCode: 1})
		return
	}
	cmd := exec.Command("/bin/sh", "-c", req.Command)
	cmd.Dir = appRoot
	out, err := cmd.Output()
	resp := executeResponse{Stdout: string(out)}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.Stderr = string(exitErr.Stderr)
			resp.ExitCode = exitErr.ExitCode()
		} else {
			resp.Stderr = err.Error()
			resp.ExitCode = 1
		}
	}
	writeJSON(w, resp)
}

func upload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(256 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	target, err := safePath(header.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if err := writeMultipartFile(target, file); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"message": "uploaded"})
}

func download(w http.ResponseWriter, r *http.Request) {
	target, err := safePath(strings.TrimPrefix(r.URL.Path, "/download/"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	http.ServeFile(w, r, target)
}

func list(w http.ResponseWriter, r *http.Request) {
	target, err := safePath(strings.TrimPrefix(r.URL.Path, "/list/"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]listEntry, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, listEntry{Name: entry.Name(), IsDir: entry.IsDir(), Size: info.Size()})
	}
	writeJSON(w, out)
}

func exists(w http.ResponseWriter, r *http.Request) {
	target, err := safePath(strings.TrimPrefix(r.URL.Path, "/exists/"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	_, err = os.Stat(target)
	writeJSON(w, map[string]bool{"exists": err == nil})
}

func safePath(name string) (string, error) {
	clean := filepath.Clean(strings.TrimLeft(name, "/"))
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid path %q", name)
	}
	target := filepath.Join(appRoot, clean)
	rel, err := filepath.Rel(appRoot, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("path escapes app root")
	}
	return target, nil
}

func writeMultipartFile(target string, file multipart.File) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, file)
	return err
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
GO

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -o /out/sandbox-runtime /tmp/sandbox-runtime.go

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/sandbox-runtime /usr/local/bin/sandbox-runtime

RUN chmod 0755 /usr/local/bin/sandbox-runtime

RUN groupadd -g 1000 worker \
    && useradd -u 1000 -g worker -m worker \
    && mkdir -p /workspace /app /tmp \
    && chown -R 1000:1000 /workspace /app /home/worker /tmp

USER 1000:1000
ENV HOME=/home/worker
ENTRYPOINT ["/usr/local/bin/sandbox-runtime"]
DOCKERFILE
}

install_agent_sandbox() {
  log "Installing upstream agent-sandbox ${agent_sandbox_version}"
  run kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${agent_sandbox_version}/sandbox.yaml"
  run kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${agent_sandbox_version}/extensions.yaml"

  for crd in \
    sandboxes.agents.x-k8s.io \
    sandboxclaims.extensions.agents.x-k8s.io \
    sandboxtemplates.extensions.agents.x-k8s.io \
    sandboxwarmpools.extensions.agents.x-k8s.io; do
    run kubectl wait --for=condition=Established "crd/${crd}" --timeout=90s
  done

  run kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller --timeout=5m
}

agent_sandbox_module_dir() {
  local module_dir

  if [[ -n "${agent_sandbox_module_cache}" ]]; then
    printf '%s\n' "${agent_sandbox_module_cache}"
    return
  fi

  module_dir="$(go list -m -f '{{.Dir}}' sigs.k8s.io/agent-sandbox)"
  if [[ -z "${module_dir}" ]]; then
    log "Downloading agent-sandbox module source"
    run go mod download sigs.k8s.io/agent-sandbox
    module_dir="$(go list -m -f '{{.Dir}}' sigs.k8s.io/agent-sandbox)"
  fi

  [[ -n "${module_dir}" ]] || die "failed to resolve agent-sandbox module directory"
  agent_sandbox_module_cache="${module_dir}"
  printf '%s\n' "${module_dir}"
}

build_sandbox_router_image() {
  local module_dir router_dir
  module_dir="$(agent_sandbox_module_dir)"
  router_dir="${module_dir}/clients/python/agentic-sandbox-client/sandbox-router"
  [[ -d "${router_dir}" ]] || die "agent-sandbox router source not found: ${router_dir}"

  log "Building upstream sandbox router image ${sandbox_router_image}"
  run docker build -t "${sandbox_router_image}" "${router_dir}"
}

deploy_sandbox_router() {
  local module_dir router_yaml
  module_dir="$(agent_sandbox_module_dir)"
  router_yaml="${module_dir}/clients/python/agentic-sandbox-client/sandbox-router/sandbox_router.yaml"
  [[ -f "${router_yaml}" ]] || die "agent-sandbox router manifest not found: ${router_yaml}"

  router_namespace="${orka_namespace}"
  log "Deploying upstream sandbox router into ${router_namespace}"
  awk -v image="${sandbox_router_image}" '
    {
      gsub(/\$\{ROUTER_IMAGE\}/, image)
      if ($0 ~ /name: ALLOW_UNAUTHENTICATED_ROUTER/) { allow = 1 }
      if (allow == 1 && $0 ~ /value: "false"/) {
        sub(/value: "false"/, "value: \"true\"")
        allow = 0
      }
      print
    }
  ' "${router_yaml}" | kubectl -n "${router_namespace}" apply -f -
  run kubectl -n "${router_namespace}" rollout status deployment/sandbox-router-deployment --timeout=5m
}

patch_controller_for_agent_sandbox() {
  local router_url rollout_id
  router_url="http://sandbox-router-svc.${router_namespace}.svc.cluster.local:8080"
  rollout_id="${e2e_run_id}"

  local workspace_api="false"
  if [[ "${suspend_resume_enabled}" == "1" ]]; then
    workspace_api="true"
    # The dedicated admission runtime below is the API server boundary. These
    # controller flags also register equivalent local handlers, so give the
    # manager webhook server a certificate even though no Service routes to it.
    local webhook_cert_dir
    webhook_cert_dir="$(mktemp -d "${work_dir}/webhook-certs.XXXXXX")"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
      -keyout "${webhook_cert_dir}/tls.key" -out "${webhook_cert_dir}/tls.crt" \
      -subj "/CN=${orka_controller_deployment}.${orka_namespace}.svc" >/dev/null 2>&1
    kubectl -n "${orka_namespace}" create secret tls orka-webhook-serving-certs \
      --cert="${webhook_cert_dir}/tls.crt" --key="${webhook_cert_dir}/tls.key" \
      --dry-run=client -o yaml | kubectl apply -f -
    rm -rf "${webhook_cert_dir}"
  fi

  log "Configuring Orka controller for agent-sandbox"
  kubectl -n "${orka_namespace}" get deployment "${orka_controller_deployment}" -o json |
    jq \
      --arg routerURL "${router_url}" \
      --arg rolloutID "${rollout_id}" \
      --arg workspaceAPI "${workspace_api}" \
      --arg ambiguityMarker "${lifecycle_ambiguity_marker}" \
      --arg template "${sandbox_template_name}" '
      def upsert_arg($name; $value):
        . as $args
        | if any($args[]?; startswith($name + "=")) then
            map(if startswith($name + "=") then $name + "=" + $value else . end)
          else
            $args + [$name + "=" + $value]
          end;
      .spec.template.metadata.annotations = ((.spec.template.metadata.annotations // {}) + {
        "orka.ai/live-agent-sandbox-e2e-run": $rolloutID
      })
      |
      .spec.template.spec.containers |= map(
        if .name == "manager" then
          .imagePullPolicy = "IfNotPresent"
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-enabled"; "true"))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-router-url"; $routerURL))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-default-template"; $template))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-warm-pool-policy"; "disabled"))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-namespace-strategy"; "task"))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-claim-timeout"; "3m"))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-command-timeout"; "5m"))
          | .args = ((.args // []) | upsert_arg("--agent-sandbox-cleanup-policy"; "delete"))
          | .args = ((.args // []) | upsert_arg("--acp-workspace-dispatch-enabled"; "true"))
          | .args = ((.args // []) | upsert_arg("--acp-e2e-prompt-write-ambiguity-marker"; $ambiguityMarker))
          | (if $workspaceAPI == "true" then
              .args = ((.args // [])
                | upsert_arg("--enable-workspace-provider-api"; "true")
                | upsert_arg("--workspace-class-use-admission-enabled"; "true")
                | upsert_arg("--task-provenance-admission-enabled"; "true"))
              | .volumeMounts = (((.volumeMounts // []) | map(select(.name != "webhook-serving-certs"))) + [{
                  "name": "webhook-serving-certs",
                  "mountPath": "/tmp/k8s-webhook-server/serving-certs",
                  "readOnly": true
                }])
            else . end)
        else . end
      )
      | (if $workspaceAPI == "true" then
          .spec.template.spec.volumes = (((.spec.template.spec.volumes // []) | map(select(.name != "webhook-serving-certs"))) + [{
            "name": "webhook-serving-certs",
            "secret": { "secretName": "orka-webhook-serving-certs" }
          }])
        else . end)
    ' | kubectl apply -f -

  run kubectl -n "${orka_namespace}" rollout status deployment/"${orka_controller_deployment}" --timeout=5m
}

apply_sandbox_template() {
  log "Creating agent-sandbox template and warm pool ${sandbox_template_name}"
  kubectl apply -f - <<YAML
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: ${sandbox_template_name}
  namespace: ${orka_namespace}
spec:
  networkPolicyManagement: Unmanaged
  service: true
  podTemplate:
    spec:
      dnsPolicy: ClusterFirst
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
        runAsNonRoot: true
      containers:
        - name: agent
          image: ${sandbox_fixture_image}
          imagePullPolicy: IfNotPresent
          command: ["/usr/local/bin/sandbox-runtime"]
          ports:
            - containerPort: 8888
              protocol: TCP
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: app
              mountPath: /app
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: workspace
          emptyDir: {}
        - name: app
          emptyDir: {}
        - name: tmp
          emptyDir: {}
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: ${sandbox_template_name}
  namespace: ${orka_namespace}
spec:
  replicas: 0
  sandboxTemplateRef:
    name: ${sandbox_template_name}
YAML
}

write_workspace_smoke_go() {
  rm -rf "${smoke_go_dir}"
  mkdir -p "${smoke_go_dir}"
  cat >"${smoke_go_dir}/main.go" <<'GO'
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandbox "sigs.k8s.io/agent-sandbox/clients/go/sandbox"

	"github.com/orka-agents/orka/internal/workspace"
)

func main() {
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintln(os.Stderr, recovered)
			os.Exit(1)
		}
	}()

	namespace := mustEnv("ORKA_NAMESPACE")
	warmPool := mustEnv("ORKA_AGENT_SANDBOX_TEMPLATE")
	routerURL := mustEnv("ORKA_AGENT_SANDBOX_ROUTER_URL")
	retainedClaim := mustEnv("ORKA_AGENT_SANDBOX_SMOKE_CLAIM")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	helper, err := sandbox.NewK8sHelper(nil, logr.Discard())
	must("create Kubernetes helper", err)

	executor := workspace.NewAgentSandboxExecutor(workspace.WithAgentSandboxAPIURL(routerURL))
	claim, err := executor.Claim(ctx, workspace.ClaimRequest{
		Namespace:         namespace,
		TaskName:          "live-agent-sandbox-delete-smoke",
		Template:          workspace.TemplateRef{Name: warmPool},
		Timeout:           3 * time.Minute,
		MaxRequestTimeout: 5 * time.Minute,
	})
	must("claim delete workspace", err)
	deleteRef := claim.Ref
	cleanupDelete := true
	defer func() {
		if cleanupDelete {
			cleanupWorkspace(executor, deleteRef)
		}
	}()
	verifyWarmPoolRef(ctx, helper, namespace, deleteRef.ClaimName, warmPool)
	execContains(ctx, executor, deleteRef, "delete workspace exec", "test \"$ORKA_SMOKE_ENV\" = env-ok && printf delete-smoke-ok", "delete-smoke-ok")
	_, err = executor.Delete(ctx, workspace.DeleteRequest{Ref: deleteRef, Reason: "live smoke delete cleanup", Timeout: 2 * time.Minute})
	must("delete delete workspace", err)
	cleanupDelete = false
	waitClaimDeleted(ctx, helper, namespace, deleteRef.ClaimName)
	fmt.Printf("delete workspace claim %s executed and deleted\n", deleteRef.ClaimName)

	retainedExecutor := workspace.NewAgentSandboxExecutor(workspace.WithAgentSandboxAPIURL(routerURL))
	retained, err := retainedExecutor.Claim(ctx, workspace.ClaimRequest{
		Namespace:         namespace,
		TaskName:          "live-agent-sandbox-retain-smoke",
		ClaimName:         retainedClaim,
		CreateIfMissing:   true,
		Template:          workspace.TemplateRef{Name: warmPool},
		ReuseKey:          "live-smoke-session",
		Timeout:           3 * time.Minute,
		MaxRequestTimeout: 5 * time.Minute,
	})
	must("claim retained workspace", err)
	retainedRef := retained.Ref
	cleanupRetained := true
	defer func() {
		if cleanupRetained {
			cleanupWorkspace(retainedExecutor, retainedRef)
		}
	}()
	verifyWarmPoolRef(ctx, helper, namespace, retainedRef.ClaimName, warmPool)
	execContains(ctx, retainedExecutor, retainedRef, "retained workspace marker write", "printf retained-smoke-ok > retained-marker.txt && cat retained-marker.txt", "retained-smoke-ok")
	_, err = retainedExecutor.Release(ctx, workspace.ReleaseRequest{Ref: retainedRef, Retain: true, Reason: "live smoke retain", Timeout: 2 * time.Minute})
	must("release retained workspace", err)
	verifyWarmPoolRef(ctx, helper, namespace, retainedRef.ClaimName, warmPool)

	reuseExecutor := workspace.NewAgentSandboxExecutor(workspace.WithAgentSandboxAPIURL(routerURL))
	reused, err := reuseExecutor.Claim(ctx, workspace.ClaimRequest{
		Namespace:         namespace,
		TaskName:          "live-agent-sandbox-reuse-smoke",
		ClaimName:         retainedClaim,
		CreateIfMissing:   true,
		Template:          workspace.TemplateRef{Name: warmPool},
		ReuseKey:          "live-smoke-session",
		Timeout:           3 * time.Minute,
		MaxRequestTimeout: 5 * time.Minute,
	})
	must("reattach retained workspace", err)
	if !reused.Reused {
		fatalf("reattach retained workspace: Reused=%v, want true", reused.Reused)
	}
	retainedExecutor = reuseExecutor
	retainedRef = reused.Ref
	verifyWarmPoolRef(ctx, helper, namespace, retainedRef.ClaimName, warmPool)
	execContains(ctx, reuseExecutor, retainedRef, "retained workspace marker read", "cat retained-marker.txt", "retained-smoke-ok")
	_, err = reuseExecutor.Delete(ctx, workspace.DeleteRequest{Ref: retainedRef, Reason: "live smoke retained cleanup", Timeout: 2 * time.Minute})
	must("delete retained workspace", err)
	cleanupRetained = false
	waitClaimDeleted(ctx, helper, namespace, retainedRef.ClaimName)
	fmt.Printf("retained workspace claim %s reused and deleted\n", retainedRef.ClaimName)
	fmt.Println("agent-sandbox workspace adapter smoke passed")
}

func mustEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		fatalf("%s is required", name)
	}
	return value
}

func execContains(ctx context.Context, executor *workspace.AgentSandboxExecutor, ref workspace.WorkspaceRef, label, command, expected string) {
	result, err := executor.Exec(ctx, workspace.ExecRequest{
		Ref:     ref,
		Command: []string{"sh", "-c", command},
		Env:     map[string]string{"ORKA_SMOKE_ENV": "env-ok"},
		WorkDir: "/workspace",
		Timeout: 90 * time.Second,
	})
	must(label, err)
	if !strings.Contains(result.Stdout, expected) {
		fatalf("%s stdout = %q, want substring %q (stderr=%q)", label, result.Stdout, expected, result.Stderr)
	}
}

func verifyWarmPoolRef(ctx context.Context, helper *sandbox.K8sHelper, namespace, claimName, warmPool string) {
	// agent-sandbox v1 K8sHelper uses the extensions v1beta1 client, where
	// SandboxClaimSpec requires warmPoolRef. This compile-checks that Orka's
	// adapter creates the v1beta1 claim shape expected by the upgraded SDK.
	claim, err := helper.ExtensionsClient.SandboxClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	must("get SandboxClaim "+claimName, err)
	if claim.Spec.WarmPoolRef.Name != warmPool {
		fatalf("SandboxClaim/%s warmPoolRef.name = %q, want %q", claimName, claim.Spec.WarmPoolRef.Name, warmPool)
	}
	if strings.TrimSpace(claim.Status.SandboxStatus.Name) == "" {
		fatalf("SandboxClaim/%s has empty status.sandbox.name", claimName)
	}
}

func waitClaimDeleted(ctx context.Context, helper *sandbox.K8sHelper, namespace, claimName string) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err := helper.ExtensionsClient.SandboxClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return
		}
		if err != nil {
			must("wait for SandboxClaim deletion", err)
		}
		if time.Now().After(deadline) {
			fatalf("SandboxClaim/%s was not deleted within timeout", claimName)
		}
		time.Sleep(2 * time.Second)
	}
}

func cleanupWorkspace(executor *workspace.AgentSandboxExecutor, ref workspace.WorkspaceRef) {
	if executor == nil || ref.IsZero() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, _ = executor.Delete(ctx, workspace.DeleteRequest{Ref: ref, Reason: "live smoke deferred cleanup", Timeout: 2 * time.Minute})
}

func must(label string, err error) {
	if err != nil {
		fatalf("%s: %v", label, err)
	}
}

func fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
GO
}

run_workspace_smoke() {
  local router_url="$1"
  log "Running live agent-sandbox workspace adapter smoke"
  write_workspace_smoke_go
  (cd "${repo_root}" && \
    ORKA_NAMESPACE="${orka_namespace}" \
    ORKA_AGENT_SANDBOX_TEMPLATE="${sandbox_template_name}" \
    ORKA_AGENT_SANDBOX_ROUTER_URL="${router_url}" \
    ORKA_AGENT_SANDBOX_SMOKE_CLAIM="${smoke_claim_name}" \
    go run "./$(basename "${smoke_go_dir}")")
}

reset_e2e_resources() {
  log "Resetting fixed-name agent-sandbox e2e resources"
  run kubectl -n "${acp_task_namespace}" delete task "${acp_task_name}"     --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${acp_task_namespace}" delete agent "${acp_agent_name}"     --ignore-not-found=true --wait=true --timeout=1m
  # Suspend/cold-resume conformance leftovers from a failed prior run: a
  # terminal fixed-name Task is never restarted by kubectl apply, and a stale
  # suspended workspace, class, or provider registration would bind the rerun
  # to old state or fail admission instead of exercising a fresh cycle.
  run kubectl -n "${acp_task_namespace}" delete task orka-ws-suspend-first orka-ws-suspend-second \
    --ignore-not-found=true --wait=true --timeout=4m
  run kubectl -n "${acp_task_namespace}" delete agent "${acp_suspend_agent_name}" --ignore-not-found=true --wait=true --timeout=1m
  run kubectl -n "${acp_runtime_namespace}" delete pod orka-ws-durability-writer orka-ws-durability-reader \
    --ignore-not-found=true --wait=true --timeout=2m
  # Delete only this scenario's ACP workspaces: an ACP-wide selector on a
  # reused cluster could destroy unrelated experiments' active workspaces and
  # their retained data. The suspend scenario binds the fixed session name;
  # per-Task workspaces die with the fixed-name Tasks deleted above.
  local reset_workspace
  for reset_workspace in $(kubectl -n "${acp_task_namespace}" get executionworkspaces -o json 2>/dev/null |
    jq -r --arg session_name "${acp_suspend_session_name}" \
      '.items[] | select((.spec.sessionRef.name // "") == $session_name) | .metadata.name'); do
    run kubectl -n "${acp_task_namespace}" delete executionworkspace "${reset_workspace}" \
      --ignore-not-found=true --wait=true --timeout=6m
  done
  delete_session_if_present "${acp_task_namespace}" "${acp_suspend_session_name}"
  run kubectl -n "${acp_task_namespace}" delete executionworkspaceclass "${acp_suspend_class_name}" --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${acp_task_namespace}" delete runtimeworkspaceprofile "${acp_suspend_class_name}" --ignore-not-found=true --wait=true --timeout=1m
  run kubectl delete executionworkspaceprovider acp-sandbox-e2e --ignore-not-found=true --wait=true --timeout=1m
  run kubectl delete runtimeproviderconfig acp-sandbox-e2e --ignore-not-found=true --wait=true --timeout=1m
  # Lifecycle-conformance leftovers: terminal fixed-name Tasks are never
  # restarted by kubectl apply, and stale session pools would bind a rerun to
  # old state or fail lifecycle checks against fresh fixture counters. The
  # pool sweep deletes ONLY pools the fixed-name lifecycle Tasks actually
  # bound (captured before their Tasks are deleted), so unrelated workspace
  # experiments on a reused cluster keep their pools and data.
  local reset_restart_json reset_restart_session=""
  reset_restart_json="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o json 2>/dev/null || true)"
  if [[ -n "${reset_restart_json}" ]]; then
    reset_restart_session="$(restart_session_name_if_deletable <<<"${reset_restart_json}")"
  fi
  local reset_lc_task reset_lc_pool reset_lc_pools=""
  for reset_lc_task in orka-ws-lc-first orka-ws-lc-second orka-ws-lc-drained \
    orka-ws-lc-timeout orka-ws-lc-cancel orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced; do
    reset_lc_pool="$(kubectl -n "${acp_task_namespace}" get task "${reset_lc_task}" \
      -o jsonpath='{.status.execution.runtimePoolName}' 2>/dev/null || true)"
    if [[ -n "${reset_lc_pool}" && " ${reset_lc_pools} " != *" ${reset_lc_pool} "* ]]; then
      reset_lc_pools="${reset_lc_pools} ${reset_lc_pool}"
    fi
  done
  # Both cancellation and final cleanup delete Tasks BEFORE pools, so an
  # interrupted run can leave pools whose Tasks are already gone. The
  # scenario persists every bound lifecycle pool in a ConfigMap; recover
  # those identities too.
  local reset_recorded_pools
  reset_recorded_pools="$(kubectl -n "${acp_task_namespace}" get configmap orka-ws-lc-pools \
    -o jsonpath='{.data.pools}' 2>/dev/null || true)"
  for reset_lc_pool in ${reset_recorded_pools}; do
    if [[ " ${reset_lc_pools} " != *" ${reset_lc_pool} "* ]]; then
      reset_lc_pools="${reset_lc_pools} ${reset_lc_pool}"
    fi
  done
  # An interrupted earlier run can leave orka-ws-lc-cancel protected by the
  # test-only observer finalizer that no controller owns; strip it before the
  # waited delete or every rerun stalls to the four-minute timeout and fails.
  if kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel >/dev/null 2>&1; then
    # The patch is retried with a re-read index: a transient failure or a
    # concurrent finalizer-list change would otherwise leave the observer
    # finalizer held and stall the waited delete below to its full timeout.
    local reset_cancel_json reset_cancel_index reset_cancel_attempt reset_cancel_ok
    reset_cancel_ok=0
    for reset_cancel_attempt in 1 2 3 4 5; do
      reset_cancel_json="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json 2>/dev/null || true)"
      [[ -n "${reset_cancel_json}" ]] || { reset_cancel_ok=1; break; }
      reset_cancel_index="$(jq -r '(.metadata.finalizers // []) | index("acp-e2e.orka.ai/lifecycle-observer") // empty' <<<"${reset_cancel_json}")"
      if [[ -z "${reset_cancel_index}" ]]; then
        reset_cancel_ok=1
        break
      fi
      if kubectl -n "${acp_task_namespace}" patch task orka-ws-lc-cancel --type=json \
        -p "[{\"op\":\"test\",\"path\":\"/metadata/finalizers/${reset_cancel_index}\",\"value\":\"acp-e2e.orka.ai/lifecycle-observer\"},{\"op\":\"remove\",\"path\":\"/metadata/finalizers/${reset_cancel_index}\"}]" >/dev/null; then
        reset_cancel_ok=1
        break
      fi
      sleep 2
    done
    [[ "${reset_cancel_ok}" == "1" ]] ||
      die "could not strip the stale lifecycle-observer finalizer from orka-ws-lc-cancel; the waited reset delete would stall"
  fi
  run kubectl -n "${acp_task_namespace}" delete task \
    orka-ws-lc-first orka-ws-lc-second orka-ws-lc-drained orka-ws-lc-timeout \
    orka-ws-lc-cancel orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced \
    --ignore-not-found=true --wait=true --timeout=4m
  run kubectl -n "${acp_task_namespace}" delete agent orka-ws-lc-agent --ignore-not-found=true --wait=true --timeout=1m
  for reset_lc_pool in ${reset_lc_pools}; do
    run kubectl -n "${acp_task_namespace}" delete runtimepool "${reset_lc_pool}" --ignore-not-found=true --wait=true --timeout=5m
  done
  kubectl -n "${acp_task_namespace}" delete configmap orka-ws-lc-pools --ignore-not-found=true >/dev/null 2>&1 || true
  if [[ -n "${reset_restart_session}" ]]; then
    delete_fixed_session "${reset_restart_session}"
  fi
  run kubectl -n "${orka_namespace}" delete sandboxclaim "${smoke_claim_name}" \
    --ignore-not-found=true \
    --wait=true \
    --timeout=2m
  run kubectl -n "${orka_namespace}" delete sandboxwarmpool "${sandbox_template_name}" \
    --ignore-not-found=true \
    --wait=true \
    --timeout=2m
  run kubectl -n "${orka_namespace}" delete sandboxtemplate "${sandbox_template_name}" \
    --ignore-not-found=true \
    --wait=true \
    --timeout=2m
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

# delete_fixed_session removes one fixed-name ACP Session through the API and
# waits until readback returns 404. A rerun on a reused cluster must not load
# the previous UID or transcript from the PVC-backed manager store.
delete_fixed_session() {
  local session_name="$1"
  local api_base="http://127.0.0.1:${orka_api_local_port}"
  local api_token delete_status get_status started now
  api_token="$(kubectl -n "${acp_task_namespace}" create token "${orka_api_client_service_account}")"
  started="$(date +%s)"
  while true; do
    delete_status="$(curl --silent --connect-timeout 5 --max-time 30 -X DELETE \
      --header "Authorization: Bearer ${api_token}" \
      --output /dev/null --write-out '%{http_code}' \
      "${api_base}/api/v1/sessions/${session_name}?namespace=${acp_task_namespace}" \
      2>>"${api_pf_log}" || true)"
    case "${delete_status}" in
      200|202|204|404) break ;;
      409)
        now="$(date +%s)"
        (( now - started >= 120 )) &&
          die "fixed Session ${session_name} remained conflicted 120s during cleanup"
        sleep 2
        ;;
      *) die "failed to delete fixed Session ${session_name} during cleanup (HTTP ${delete_status:-none})" ;;
    esac
  done
  started="$(date +%s)"
  while true; do
    get_status="$(curl --silent --connect-timeout 5 --max-time 30 \
      --header "Authorization: Bearer ${api_token}" \
      --output /dev/null --write-out '%{http_code}' \
      "${api_base}/api/v1/sessions/${session_name}?namespace=${acp_task_namespace}" \
      2>>"${api_pf_log}" || true)"
    [[ "${get_status}" == "404" ]] && return 0
    [[ "${get_status}" == "200" ]] ||
      die "failed to verify fixed Session ${session_name} deletion (HTTP ${get_status:-none})"
    now="$(date +%s)"
    (( now - started >= 60 )) &&
      die "fixed Session ${session_name} remained readable 60s after deletion"
    sleep 2
  done
}

# record_lc_pool persists a lifecycle pool name in a ConfigMap so a reset on
# a reused cluster can recover pools whose Tasks were already deleted (both
# cancellation and final cleanup delete Tasks before pools).
record_lc_pool() {
  local pool="$1"
  [[ -n "${pool}" ]] || return 0
  local existing merged
  existing="$(kubectl -n "${acp_task_namespace}" get configmap orka-ws-lc-pools     -o jsonpath='{.data.pools}' 2>/dev/null || true)"
  case " ${existing} " in
    *" ${pool} "*) return 0 ;;
  esac
  merged="$(printf '%s %s' "${existing}" "${pool}" | sed 's/^ //')"
  kubectl -n "${acp_task_namespace}" create configmap orka-ws-lc-pools     --from-literal=pools="${merged}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

wait_for_jsonpath() {
  local kind="$1" namespace="$2" name="$3" path="$4" want="$5" timeout_seconds="$6"
  local started now value
  started="$(date +%s)"
  while true; do
    value="$(kubectl -n "${namespace}" get "${kind}" "${name}" -o jsonpath="${path}" 2>/dev/null || true)"
    if [[ "${value}" == "${want}" ]]; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      die "timed out waiting for ${kind}/${name} ${path}=${want} (last: ${value:-<empty>})"
    fi
    sleep 3
  done
}

wait_for_nonempty_jsonpath() {
  local kind="$1" namespace="$2" name="$3" path="$4" timeout_seconds="$5"
  local started now value
  started="$(date +%s)"
  while true; do
    value="$(kubectl -n "${namespace}" get "${kind}" "${name}" -o jsonpath="${path}" 2>/dev/null || true)"
    if [[ -n "${value}" ]]; then
      printf '%s' "${value}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      die "timed out waiting for ${kind}/${name} ${path} to be set"
    fi
    sleep 3
  done
}

# run_workspace_backed_acp_task_smoke proves the Phase-1 workspace-provider
# adapter live against upstream agent-sandbox:
#   1. a Task.spec.execution.workspace agent Task is admitted (not rejected
#      with WorkspaceValidationFailed) and binds a dedicated acp-ws-* pool;
#   2. the pool materializes a controller-rendered SandboxTemplate, a
#      zero-replica SandboxWarmPool, and one SandboxClaim through the real
#      provider controller, and the sandbox Pod runs the immutable Codex
#      runtime image;
#   3. the authenticated exact-instance fence probe reaches Serving/Accepting;
#   4. a real Codex prompt succeeds through the authenticated provider proxy;
#   5. Task status stays provider-neutral (no claim identifiers);
#   6. pool deletion removes the claim, warm pool, and template.
run_workspace_backed_acp_task_smoke() {
  log "Running workspace-backed ACP Task infrastructure smoke"

  bash "${repo_root}/scripts/lib/ensure-static-mode-namespace.sh" \
    kubectl "${acp_task_namespace}" harness-v2

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: ${acp_agent_name}
  namespace: ${acp_task_namespace}
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
  name: ${acp_task_name}
  namespace: ${acp_task_namespace}
spec:
  type: agent
  agentRef:
    name: ${acp_agent_name}
  agentRuntime:
    maxTurns: 1
  timeout: 10m0s
  execution:
    workspace:
      enabled: true
      provider: agent-sandbox
      reusePolicy: none
      cleanupPolicy: delete
  prompt: "Reply exactly: ORKA_WS_SANDBOX_OK"
YAML

  local pool_name
  pool_name="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" "${acp_task_name}"     '{.status.execution.runtimePoolName}' 120)"
  log "Workspace-backed Task bound RuntimePool ${pool_name}"
  [[ "${pool_name}" == acp-ws-codex-* ]] ||
    die "runtime pool ${pool_name} is not a workspace-backed pool"

  local workspace_provider workspace_reason
  workspace_provider="$(kubectl -n "${acp_task_namespace}" get task "${acp_task_name}"     -o jsonpath='{.status.executionWorkspace.provider}')"
  workspace_reason="$(kubectl -n "${acp_task_namespace}" get task "${acp_task_name}"     -o jsonpath='{.status.executionWorkspace.reason}')"
  [[ "${workspace_provider}" == "agent-sandbox" ]] ||
    die "workspace status provider ${workspace_provider}, want agent-sandbox"
  [[ "${workspace_reason}" != "WorkspaceValidationFailed" ]] ||
    die "workspace-backed Task was rejected with WorkspaceValidationFailed"

  log "Waiting for workspace-backed RuntimePool ${pool_name} to reach Serving"
  wait_for_jsonpath runtimepool "${acp_task_namespace}" "${pool_name}"     '{.status.lifecycle}' "Serving" 480

  local active_pod_uid
  active_pod_uid="$(kubectl -n "${acp_task_namespace}" get runtimepool "${pool_name}"     -o jsonpath='{.status.activeInstance.podUID}')"
  [[ -n "${active_pod_uid}" ]] || die "Serving pool has no active instance"

  local claim_count claim_name
  claim_count="$(kubectl get sandboxclaims -A     -l "orka.ai/runtime-pool-name=${pool_name}" -o name | wc -l | tr -d ' ')"
  [[ "${claim_count}" == "1" ]] ||
    die "expected exactly one SandboxClaim for ${pool_name}, found ${claim_count}"
  claim_name="$(kubectl get sandboxclaims -n "${acp_runtime_namespace}"     -l "orka.ai/runtime-pool-name=${pool_name}" -o jsonpath='{.items[0].metadata.name}')"
  log "Workspace-backed pool is Serving through SandboxClaim ${claim_name}"

  local sandbox_pod_image
  sandbox_pod_image="$(kubectl get pods -n "${acp_runtime_namespace}"     -l "orka.ai/runtime-pool-name=${pool_name}"     -o jsonpath='{.items[0].spec.containers[0].image}')"
  [[ "${sandbox_pod_image}" == *"acp-codex"* ]] ||
    die "sandbox Pod image ${sandbox_pod_image} is not the immutable Codex runtime image"

  if kubectl -n "${acp_task_namespace}" get task "${acp_task_name}" -o yaml | grep -q "${claim_name}"; then
    die "public Task status leaked the provider claim identifier ${claim_name}"
  fi

  log "Waiting for the workspace-backed Task to succeed"
  local started now task_payload phase execution_state execution_outcome result_available
  started="$(date +%s)"
  while true; do
    task_payload="$(kubectl -n "${acp_task_namespace}" get task "${acp_task_name}" -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${task_payload}")"
    execution_state="$(jq -r '.status.execution.state // ""' <<<"${task_payload}")"
    execution_outcome="$(jq -r '.status.execution.outcome // ""' <<<"${task_payload}")"
    result_available="$(jq -r '.status.resultRef.available // false' <<<"${task_payload}")"
    if [[ "${phase}" == "Succeeded" && "${execution_state}" == "Succeeded" &&
          "${execution_outcome}" == "Succeeded" && "${result_available}" == "true" ]]; then
      break
    fi
    if [[ "${phase}" == "Failed" || "${phase}" == "Cancelled" ||
          "${execution_state}" == "Failed" || "${execution_state}" == "Cancelled" ]]; then
      kubectl -n "${acp_task_namespace}" get task "${acp_task_name}" -o yaml >&2 || true
      die "workspace-backed Task reached terminal failure (phase=${phase:-<empty>}, state=${execution_state:-<empty>}, outcome=${execution_outcome:-<empty>})"
    fi
    now="$(date +%s)"
    if (( now - started >= 300 )); then
      kubectl -n "${acp_task_namespace}" get task "${acp_task_name}" -o yaml >&2 || true
      die "workspace-backed Task did not succeed (phase=${phase:-<empty>}, state=${execution_state:-<empty>}, outcome=${execution_outcome:-<empty>}, resultAvailable=${result_available})"
    fi
    sleep 3
  done
  log "Workspace-backed Task reached Succeeded/Succeeded with an available result"
  assert_task_result_contains "${acp_task_namespace}" "${acp_task_name}" "ORKA_WS_SANDBOX_OK"

  workspace_reason="$(kubectl -n "${acp_task_namespace}" get task "${acp_task_name}"     -o jsonpath='{.status.executionWorkspace.reason}')"
  [[ "${workspace_reason}" != "WorkspaceValidationFailed" ]] ||
    die "workspace-backed Task regressed to WorkspaceValidationFailed after dispatch"

  log "Cleaning up the workspace-backed Task and pool"
  run kubectl -n "${acp_task_namespace}" delete task "${acp_task_name}" --wait=true --timeout=3m
  run kubectl -n "${acp_task_namespace}" delete runtimepool "${pool_name}" --wait=true --timeout=4m
  local remaining
  remaining="$(kubectl get sandboxclaims,sandboxwarmpools,sandboxtemplates -n "${acp_runtime_namespace}"     -l "orka.ai/runtime-pool-name=${pool_name}" -o name | wc -l | tr -d ' ')"
  [[ "${remaining}" == "0" ]] ||
    die "pool finalization left ${remaining} provider objects for ${pool_name}"
  log "Workspace-backed ACP Task infrastructure smoke passed"
}


# run_workspace_suspend_resume_acp_task proves class-backed PVC-backed
# suspension and cold resume live against upstream agent-sandbox (issue #425):
# a session-scoped classRef Task suspends its workspace on detach (the pool
# drains to Stopped, the exact Sandbox is consensually suspended through
# operatingMode: Suspended, its Pod disappears, and the injected durable
# workspace PVC stays Bound), a continuation Task cold-resumes the same
# Sandbox, and explicit deletion removes the workspace, pool, claim, Sandbox,
# and PVC.
run_workspace_suspend_resume_acp_task() {
  log "Running class-backed suspend/cold-resume conformance (agent-sandbox)"

  bash "${repo_root}/scripts/lib/ensure-static-mode-namespace.sh" \
    kubectl "${acp_task_namespace}" harness-v2

  kubectl apply -f - <<YAML
apiVersion: acp.workspace.orka.ai/v1alpha1
kind: RuntimeProviderConfig
metadata:
  name: acp-sandbox-e2e
spec:
  backend: agent-sandbox
---
apiVersion: workspace.orka.ai/v1alpha1
kind: ExecutionWorkspaceProvider
metadata:
  name: acp-sandbox-e2e
spec:
  controllerName: acp.workspace.orka.ai/runtime-pool
  parametersRef:
    group: acp.workspace.orka.ai
    kind: RuntimeProviderConfig
    name: acp-sandbox-e2e
  lifecycleState: Active
  requiredContracts:
    - workspace.orka.ai/v1
---
apiVersion: acp.workspace.orka.ai/v1alpha1
kind: RuntimeWorkspaceProfile
metadata:
  name: ${acp_suspend_class_name}
  namespace: ${acp_task_namespace}
spec:
  agentSandbox:
    suspend:
      mode: DataOnly
      volume:
        capacity: 1Gi
---
apiVersion: workspace.orka.ai/v1alpha1
kind: ExecutionWorkspaceClass
metadata:
  name: ${acp_suspend_class_name}
  namespace: ${acp_task_namespace}
spec:
  providerRef:
    name: acp-sandbox-e2e
  parametersRef:
    group: acp.workspace.orka.ai
    kind: RuntimeWorkspaceProfile
    name: ${acp_suspend_class_name}
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

  wait_for_jsonpath executionworkspaceclass "${acp_task_namespace}" "${acp_suspend_class_name}" \
    '{.status.conditions[?(@.type=="Ready")].status}' "True" 180

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: ${acp_suspend_agent_name}
  namespace: ${acp_task_namespace}
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
  namespace: ${acp_task_namespace}
spec:
  type: agent
  agentRef:
    name: ${acp_suspend_agent_name}
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: ${acp_suspend_session_name}
    create: true
  execution:
    workspace:
      classRef:
        name: ${acp_suspend_class_name}
      reusePolicy: session
  prompt: "ORKA_HOLD_60S then reply ORKA_WS_SUSPEND_FIRST_OK"
YAML

  local workspace_name pool_name first_runtime_instance first_pod_uid
  local claim_payload claim_count claim_name claim_uid warm_pool_name sandbox_template_name
  local durability_marker durable_session_uid durable_volume_directory
  local durable_session_relative_path durable_session_directory durable_marker_relative_path durable_marker_path
  workspace_name="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" orka-ws-suspend-first \
    '{.metadata.labels.acp\.workspace\.orka\.ai/execution-workspace}' 240)"
  pool_name="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" orka-ws-suspend-first \
    '{.status.execution.runtimePoolName}' 240)"
  if [[ -z "${workspace_name}" || "${pool_name}" != acp-ws-session-* ]]; then
    kubectl -n "${acp_task_namespace}" get task orka-ws-suspend-first -o yaml >&2 || true
    die "class-backed Task did not bind a session workspace (workspace=${workspace_name:-<empty>} pool=${pool_name:-<empty>})"
  fi
  first_runtime_instance="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" orka-ws-suspend-first \
    '{.status.execution.runtimeInstanceID}' 240)"
  first_pod_uid="${first_runtime_instance%%.*}"
  if [[ -z "${first_runtime_instance}" || "${first_runtime_instance}" == "${first_pod_uid}" ]]; then
    die "first suspend Task carries no Pod-scoped runtime instance (got ${first_runtime_instance:-<empty>})"
  fi
  durable_session_uid="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" orka-ws-suspend-first \
    '{.status.execution.runtimeSessionUID}' 240)"
  durable_volume_directory="/durable/orka-workspace"
  durable_session_relative_path="ws-${durable_session_uid}"
  durable_session_directory="${durable_volume_directory}/${durable_session_relative_path}"
  durable_marker_relative_path="${durable_session_relative_path}/e2e-durability-marker-${durable_session_uid}"
  durable_marker_path="${durable_volume_directory}/${durable_marker_relative_path}"
  durability_marker="ORKA_E2E_DURABLE_MARKER_${e2e_run_id}"
  claim_payload="$(kubectl -n "${acp_runtime_namespace}" get sandboxclaims \
    -l "orka.ai/runtime-pool-name=${pool_name}" -o json)"
  claim_count="$(jq -r '.items | length' <<<"${claim_payload}")"
  [[ "${claim_count}" == "1" ]] ||
    die "expected exactly one SandboxClaim for ${pool_name}, found ${claim_count}"
  claim_name="$(jq -r '.items[0].metadata.name // empty' <<<"${claim_payload}")"
  claim_uid="$(jq -r '.items[0].metadata.uid // empty' <<<"${claim_payload}")"
  [[ -n "${claim_name}" && -n "${claim_uid}" ]] ||
    die "SandboxClaim for ${pool_name} has no exact name/UID identity"
  warm_pool_name="$(jq -r '.items[0].spec.warmPoolRef.name // empty' <<<"${claim_payload}")"
  [[ -n "${warm_pool_name}" ]] ||
    die "SandboxClaim ${claim_name} has no SandboxWarmPool reference"
  sandbox_template_name="$(kubectl -n "${acp_runtime_namespace}" get sandboxwarmpools.extensions.agents.x-k8s.io "${warm_pool_name}" \
    -o jsonpath='{.spec.sandboxTemplateRef.name}')"
  [[ -n "${sandbox_template_name}" ]] ||
    die "SandboxWarmPool ${warm_pool_name} has no SandboxTemplate reference"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-suspend-first '{.status.phase}' "Succeeded" 900
  assert_task_result_contains "${acp_task_namespace}" orka-ws-suspend-first "ORKA_WS_SUSPEND_FIRST_OK"
  log "Class-backed Task bound workspace ${workspace_name} on pool ${pool_name} through SandboxClaim ${claim_name}"

  log "Waiting for the detach-time PVC-backed suspension to settle"
  wait_for_jsonpath executionworkspace "${acp_task_namespace}" "${workspace_name}" \
    '{.spec.desiredState}' "Suspended" 240
  wait_for_jsonpath executionworkspace "${acp_task_namespace}" "${workspace_name}" \
    '{.status.state}' "Suspended" 600
  wait_for_jsonpath runtimepool "${acp_task_namespace}" "${pool_name}" \
    '{.status.lifecycle}' "Stopped" 240

  local suspend_record sandbox_name sandbox_uid
  suspend_record="$(kubectl -n "${acp_task_namespace}" get runtimepool "${pool_name}" -o json |
    jq -r '.metadata.annotations["orka.ai/sandbox-suspended"] // empty')"
  [[ -n "${suspend_record}" ]] || die "suspended pool carries no consensual Sandbox checkpoint record"
  sandbox_name="$(jq -r '.name // empty' <<<"${suspend_record}")"
  sandbox_uid="$(jq -r '.uid // empty' <<<"${suspend_record}")"
  [[ -n "${sandbox_name}" && -n "${sandbox_uid}" ]] ||
    die "consensual checkpoint record does not pin an exact Sandbox: ${suspend_record}"

  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.spec.operatingMode}' "Suspended" 120
  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.status.conditions[?(@.type=="Suspended")].status}' "True" 300
  local observed_uid observed_claim_uid claim_sandbox_name
  observed_uid="$(kubectl -n "${acp_runtime_namespace}" get sandboxes.agents.x-k8s.io "${sandbox_name}" \
    -o jsonpath='{.metadata.uid}')"
  [[ "${observed_uid}" == "${sandbox_uid}" ]] ||
    die "suspended Sandbox UID ${observed_uid} does not match the consensual record ${sandbox_uid}"
  observed_claim_uid="$(kubectl -n "${acp_runtime_namespace}" get sandboxclaims "${claim_name}" \
    -o jsonpath='{.metadata.uid}')"
  [[ "${observed_claim_uid}" == "${claim_uid}" ]] ||
    die "SandboxClaim ${claim_name} UID changed from ${claim_uid} to ${observed_claim_uid}"
  claim_sandbox_name="$(kubectl -n "${acp_runtime_namespace}" get sandboxclaims "${claim_name}" \
    -o jsonpath='{.status.sandbox.name}')"
  [[ "${claim_sandbox_name}" == "${sandbox_name}" ]] ||
    die "SandboxClaim ${claim_name} selected Sandbox ${claim_sandbox_name:-<empty>}, want ${sandbox_name}"

  local durable_pvc pvc_phase pod_count
  durable_pvc="orka-workspace-${sandbox_name}"
  pvc_phase="$(kubectl -n "${acp_runtime_namespace}" get pvc "${durable_pvc}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [[ "${pvc_phase}" == "Bound" ]] ||
    die "durable workspace PVC ${durable_pvc} is not Bound during suspension (phase=${pvc_phase:-<absent>})"
  local pod_started pod_now
  pod_started="$(date +%s)"
  while true; do
    pod_count="$(kubectl -n "${acp_runtime_namespace}" get pods -o json 2>/dev/null |
      jq -r --arg sandbox_name "${sandbox_name}" --arg sandbox_uid "${sandbox_uid}" '
        [.items[]? |
          select(any(.metadata.ownerReferences[]?;
            .controller == true and .kind == "Sandbox" and
            .name == $sandbox_name and .uid == $sandbox_uid))] |
        length
      ')"
    [[ "${pod_count}" == "0" ]] && break
    pod_now="$(date +%s)"
    (( pod_now - pod_started >= 180 )) &&
      die "suspension left ${pod_count} runtime Pod(s) for ${pool_name}"
    sleep 3
  done
  log "Sandbox ${sandbox_name} is consensually suspended; PVC ${durable_pvc} is retained and no runtime Pod remains"

  # Seed known bytes only after the first turn's read-only validation and
  # suspension have completed. The helper mounts the retained PVC while no
  # runtime Pod owns it and writes inside the exact session tree. The
  # continuation removes the probe before its own read-only validation.
  log "Writing a durability marker into ${durable_session_relative_path} on retained PVC ${durable_pvc}"
  kubectl -n "${acp_runtime_namespace}" delete pod orka-ws-durability-writer \
    --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
  kubectl -n "${acp_runtime_namespace}" apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: orka-ws-durability-writer
spec:
  restartPolicy: Never
  # The private session tree is owned by the ACP child's distinct UID. The
  # local-path volume receives no useful fsGroup, so this proof pod runs as
  # root with only DAC_OVERRIDE to cross that ownership boundary.
  securityContext:
    runAsUser: 0
    runAsGroup: 0
  containers:
    - name: writer
      image: busybox:1.36
      command:
        - /bin/sh
        - -c
        - "test -d '/data/${durable_session_relative_path}' && printf '%s' '${durability_marker}' > '/data/${durable_marker_relative_path}' && sync"
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
          add:
            - DAC_OVERRIDE
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${durable_pvc}
YAML
  wait_for_jsonpath pod "${acp_runtime_namespace}" orka-ws-durability-writer '{.status.phase}' "Succeeded" 240
  run kubectl -n "${acp_runtime_namespace}" delete pod orka-ws-durability-writer --wait=true --timeout=2m

  log "Continuing the session to cold-resume the suspended workspace"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: orka-ws-suspend-second
  namespace: ${acp_task_namespace}
spec:
  type: agent
  agentRef:
    name: ${acp_suspend_agent_name}
  agentRuntime:
    maxTurns: 1
  timeout: 15m0s
  sessionRef:
    name: ${acp_suspend_session_name}
    create: false
  execution:
    workspace:
      classRef:
        name: ${acp_suspend_class_name}
      reusePolicy: session
  prompt: "ORKA_HOLD_60S then reply ORKA_WS_SUSPEND_SECOND_OK"
YAML

  local second_pool_name resumed_uid resumed_pod_json resumed_pod resumed_pod_uid
  second_pool_name="$(wait_for_nonempty_jsonpath task "${acp_task_namespace}" orka-ws-suspend-second \
    '{.status.execution.runtimePoolName}' 240)"
  [[ "${second_pool_name}" == "${pool_name}" ]] ||
    die "continuation selected RuntimePool ${second_pool_name:-<empty>}, want the original ${pool_name}"

  resumed_uid="$(kubectl -n "${acp_runtime_namespace}" get sandboxes.agents.x-k8s.io "${sandbox_name}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  [[ "${resumed_uid}" == "${sandbox_uid}" ]] ||
    die "resume did not preserve the exact Sandbox ${sandbox_name} (uid ${resumed_uid:-<absent>}, want ${sandbox_uid})"
  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.spec.operatingMode}' "Running" 300
  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.status.conditions[?(@.type=="Ready")].status}' "True" 600

  local resume_started resume_now
  resume_started="$(date +%s)"
  while true; do
    resumed_pod_json="$(kubectl -n "${acp_runtime_namespace}" get pods \
      -l "orka.ai/runtime-pool-name=${pool_name}" -o json 2>/dev/null |
      jq -c --arg sandbox_name "${sandbox_name}" --arg sandbox_uid "${sandbox_uid}" '
        [.items[]? |
          select(.status.phase == "Running") |
          select(any(.metadata.ownerReferences[]?;
            .controller == true and .kind == "Sandbox" and
            .name == $sandbox_name and .uid == $sandbox_uid))] |
        if length == 1 then .[0] else empty end
      ' || true)"
    if [[ -n "${resumed_pod_json}" ]]; then
      resumed_pod="$(jq -r '.metadata.name' <<<"${resumed_pod_json}")"
      resumed_pod_uid="$(jq -r '.metadata.uid' <<<"${resumed_pod_json}")"
      break
    fi
    resume_now="$(date +%s)"
    (( resume_now - resume_started >= 300 )) &&
      die "exact Sandbox ${sandbox_name} did not produce one Running controller-owned Pod for ${pool_name}"
    sleep 3
  done
  [[ "${resumed_pod_uid}" != "${first_pod_uid}" ]] ||
    die "cold resume reused the first runtime Pod UID ${first_pod_uid} instead of replacing the process"
  log "Exact Sandbox ${sandbox_name} returned Ready on replacement Pod ${resumed_pod}"

  log "Verifying the replacement runtime Pod mounted the preserved workspace"
  local marker_content
  marker_content="$(read_pod_file_as_directory_owner \
    "${resumed_pod}" \
    "${durable_session_directory}" \
    "${durable_marker_path}")"
  [[ "${marker_content}" == "${durability_marker}" ]] ||
    die "replacement runtime Pod ${resumed_pod} did not read the pre-resume durability marker"
  log "Replacement runtime Pod ${resumed_pod} reads the pre-resume durability marker from the preserved PVC"
  remove_pod_file_as_directory_owner \
    "${resumed_pod}" \
    "${durable_session_directory}" \
    "${durable_marker_path}"
  log "Removed the durability probe before read-only workspace validation"

  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-suspend-second '{.status.phase}' "Succeeded" 900
  assert_task_result_contains "${acp_task_namespace}" orka-ws-suspend-second "ORKA_WS_SUSPEND_SECOND_OK"
  assert_fixture_marker_count "ORKA_WS_SUSPEND_FIRST_OK" 1
  assert_fixture_marker_count "ORKA_WS_SUSPEND_SECOND_OK" 1

  local second_workspace second_runtime_instance
  second_workspace="$(kubectl -n "${acp_task_namespace}" get task orka-ws-suspend-second \
    -o jsonpath='{.metadata.labels.acp\.workspace\.orka\.ai/execution-workspace}')"
  [[ "${second_workspace}" == "${workspace_name}" ]] ||
    die "continuation bound workspace ${second_workspace:-<empty>}, want the resumed ${workspace_name}"
  second_runtime_instance="$(kubectl -n "${acp_task_namespace}" get task orka-ws-suspend-second \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  [[ "${second_runtime_instance}" == "${resumed_pod_uid}."* ]] ||
    die "continuation runtime instance ${second_runtime_instance:-<empty>} does not belong to resumed Pod UID ${resumed_pod_uid}"
  log "Continuation cold-resumed the same Sandbox ${sandbox_name} on its replacement runtime Pod"

  log "Deleting the suspended workspace and asserting exact cleanup"
  wait_for_jsonpath executionworkspace "${acp_task_namespace}" "${workspace_name}" \
    '{.spec.desiredState}' "Suspended" 240
  wait_for_jsonpath executionworkspace "${acp_task_namespace}" "${workspace_name}" \
    '{.status.state}' "Suspended" 600
  wait_for_jsonpath runtimepool "${acp_task_namespace}" "${pool_name}" \
    '{.status.lifecycle}' "Stopped" 240
  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.spec.operatingMode}' "Suspended" 120
  wait_for_jsonpath sandboxes.agents.x-k8s.io "${acp_runtime_namespace}" "${sandbox_name}" \
    '{.status.conditions[?(@.type=="Suspended")].status}' "True" 300
  pod_started="$(date +%s)"
  while true; do
    pod_count="$(kubectl -n "${acp_runtime_namespace}" get pods -o json 2>/dev/null |
      jq -r --arg sandbox_name "${sandbox_name}" --arg sandbox_uid "${sandbox_uid}" '
        [.items[]? |
          select(any(.metadata.ownerReferences[]?;
            .controller == true and .kind == "Sandbox" and
            .name == $sandbox_name and .uid == $sandbox_uid))] |
        length
      ')"
    [[ "${pod_count}" == "0" ]] && break
    pod_now="$(date +%s)"
    (( pod_now - pod_started >= 180 )) &&
      die "continuation re-suspension left ${pod_count} runtime Pod(s) for ${pool_name}"
    sleep 3
  done
  run kubectl -n "${acp_task_namespace}" delete task orka-ws-suspend-first orka-ws-suspend-second --wait=true --timeout=4m
  run kubectl -n "${acp_task_namespace}" delete executionworkspace "${workspace_name}" --wait=true --timeout=6m

  local cleanup_started cleanup_now remaining current_claim_uid
  cleanup_started="$(date +%s)"
  while true; do
    remaining=0
    kubectl -n "${acp_task_namespace}" get runtimepool "${pool_name}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    kubectl -n "${acp_runtime_namespace}" get sandboxes.agents.x-k8s.io "${sandbox_name}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    kubectl -n "${acp_runtime_namespace}" get sandboxwarmpools.extensions.agents.x-k8s.io "${warm_pool_name}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    kubectl -n "${acp_runtime_namespace}" get sandboxtemplates.extensions.agents.x-k8s.io "${sandbox_template_name}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    kubectl -n "${acp_runtime_namespace}" get pvc "${durable_pvc}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    current_claim_uid="$(kubectl -n "${acp_runtime_namespace}" get sandboxclaims "${claim_name}" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    if [[ -n "${current_claim_uid}" ]]; then
      [[ "${current_claim_uid}" == "${claim_uid}" ]] ||
        die "SandboxClaim ${claim_name} was replaced during cleanup (uid ${current_claim_uid}, want ${claim_uid})"
      remaining=$((remaining + 1))
    fi
    [[ "${remaining}" == "0" ]] && break
    cleanup_now="$(date +%s)"
    (( cleanup_now - cleanup_started >= 300 )) &&
      die "workspace deletion left ${remaining} provider object(s) for ${pool_name}/${sandbox_name}"
    sleep 5
  done

  run kubectl -n "${acp_task_namespace}" delete agent "${acp_suspend_agent_name}" \
    --ignore-not-found=true --wait=true --timeout=1m
  run kubectl -n "${acp_task_namespace}" delete executionworkspaceclass "${acp_suspend_class_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${acp_task_namespace}" delete runtimeworkspaceprofile "${acp_suspend_class_name}" \
    --ignore-not-found=true --wait=true --timeout=1m
  run kubectl delete executionworkspaceprovider acp-sandbox-e2e \
    --ignore-not-found=true --wait=true --timeout=1m
  run kubectl delete runtimeproviderconfig acp-sandbox-e2e \
    --ignore-not-found=true --wait=true --timeout=1m
  delete_session_if_present "${acp_task_namespace}" "${acp_suspend_session_name}"
  log "Class-backed suspend/cold-resume conformance (agent-sandbox) passed"
}


start_fixture_port_forward() {
  if [[ -n "${fixture_pf_pid}" ]] && kill -0 "${fixture_pf_pid}" >/dev/null 2>&1; then
    return 0
  fi
  fixture_pf_pid="$(start_port_forward vekil-system "svc/vekil" "${fixture_local_port}" 1337 "${work_dir}/fixture-port-forward.log")"
  wait_for_http "http://127.0.0.1:${fixture_local_port}/healthz" "Responses fixture /healthz"
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
    "http://127.0.0.1:${fixture_local_port}/fixture/marker-counts" |
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
    "http://127.0.0.1:${fixture_local_port}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" '.[$marker].sawHistory // false'
}

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
    "http://127.0.0.1:${fixture_local_port}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" --arg expected "${expected}" \
      '(.[$marker].historyMarkers // []) | index($expected) != null'
}

# fixture_marker_disconnects reports how many held requests for the marker
# observed a client disconnect before their hold elapsed - the proof that a
# cancellation closed the in-flight provider stream.
fixture_marker_disconnects() {
  local marker
  marker="$(fixture_marker_key "$1")"
  start_fixture_port_forward || return 1
  curl -fsS --connect-timeout 2 --max-time 10 \
    "http://127.0.0.1:${fixture_local_port}/fixture/marker-observations" |
    jq -r --arg marker "${marker}" '.[$marker].disconnects // 0'
}

apply_lifecycle_task() {
  local name="$1" session="$2" create="$3" prompt="$4" timeout="${5:-15m0s}"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: ${name}
  namespace: ${acp_task_namespace}
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
      provider: agent-sandbox
      reusePolicy: session
  prompt: "${prompt}"
YAML
}

# assert_lc_task_success_tuple requires the COMPLETE canonical successful
# settlement: the controller-owned Succeeded/Succeeded/Succeeded tuple, a
# ReadValidated delivery projection, an available stored result, and no legacy
# Job projection (built-in ACP runtimes are v2-only).
assert_lc_task_success_tuple() {
  local task="$1"
  kubectl -n "${acp_task_namespace}" get task "${task}" -o json |
    jq -e '
      .status.phase == "Succeeded"
      and .status.execution.state == "Succeeded"
      and .status.execution.outcome == "Succeeded"
      and .status.delivery.state == "ReadValidated"
      and .status.delivery.outcome == "ReadValidated"
      and (.status.resultRef.available == true)
      and ((.status.jobName // "") == "")
    ' >/dev/null || {
    kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
    die "Task ${task} did not settle the complete successful tuple (Succeeded/Succeeded/Succeeded + ReadValidated delivery + available result, no legacy Job)"
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
  local fence_file="${work_dir}/lc-success-fence-${task}.json"
  local pool_file="${work_dir}/lc-success-pool-${task}.json"
  local fence_pool
  kubectl -n "${acp_task_namespace}" get task "${task}" -o json >"${fence_file}"
  fence_pool="$(jq -r '.status.execution.runtimePoolName // ""' "${fence_file}")"
  [[ -n "${fence_pool}" ]] || die "Task ${task} exposes no runtimePoolName for its success fence"
  kubectl -n "${acp_task_namespace}" get runtimepool "${fence_pool}" -o json |
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
    kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
    cat "${pool_file}" >&2 || true
    die "Task ${task} execution fence does not match the RuntimePool's own identity"
  }
}

# Start the watch before changing desiredReplicas so even short-lived barriers
# are recorded. Success requires every exact lifecycle/admission pair in order.
drain_lc_pool_to_zero() {
  local pool="$1" timeout_seconds="$2"
  local events_file="${work_dir}/${pool}-drain-events.tsv"
  local watch_log="${work_dir}/${pool}-drain-watch.log"
  local watch_pid started now
  : >"${events_file}"
  : >"${watch_log}"
  (
    kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" \
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
      die "RuntimePool/${pool} lifecycle watch exited before its initial snapshot"
    fi
    now="$(date +%s)"
    if (( now - started >= 30 )); then
      kill "${watch_pid}" 2>/dev/null || true
      wait "${watch_pid}" 2>/dev/null || true
      cat "${watch_log}" >&2 || true
      die "timed out establishing the RuntimePool/${pool} lifecycle watch"
    fi
    sleep 1
  done

  if ! run kubectl -n "${acp_task_namespace}" patch runtimepool "${pool}" --type=merge \
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
      die "RuntimePool/${pool} lifecycle watch ended before the exact drain sequence completed"
    fi
    now="$(date +%s)"
    if (( now - started >= timeout_seconds )); then
      kill "${watch_pid}" 2>/dev/null || true
      wait "${watch_pid}" 2>/dev/null || true
      cat "${events_file}" >&2 || true
      kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o yaml >&2 || true
      die "RuntimePool/${pool} did not traverse the exact drain sequence within ${timeout_seconds}s"
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
    payload="$(kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o json 2>/dev/null || true)"
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
    (( now - started >= timeout_seconds )) && {
      kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o yaml >&2 || true
      die "RuntimePool/${pool} did not reach the exact stopped state within ${timeout_seconds}s"
    }
    sleep 3
  done
}

wait_for_lc_sandbox_runtime_zero() {
  local pool="$1" timeout_seconds="$2"
  local started now claim_json pod_json sandbox_names
  local claim_count pod_count sandbox_count
  started="$(date +%s)"
  while true; do
    if claim_json="$(kubectl -n "${acp_runtime_namespace}" get sandboxclaims \
      -l "orka.ai/runtime-pool-name=${pool}" -o json 2>&1)" &&
      pod_json="$(kubectl -n "${acp_runtime_namespace}" get pods \
        -l "orka.ai/runtime-pool-name=${pool}" -o json 2>&1)" &&
      sandbox_names="$(kubectl -n "${acp_runtime_namespace}" get sandboxes.agents.x-k8s.io \
        -o name 2>&1)" &&
      claim_count="$(jq -r '.items | length' <<<"${claim_json}" 2>&1)" &&
      pod_count="$(jq -r '.items | length' <<<"${pod_json}" 2>&1)"; then
      sandbox_count="$(grep -c "/${pool}-" <<<"${sandbox_names}" || true)"
      if [[ "${claim_count}" == "0" && "${pod_count}" == "0" && "${sandbox_count}" == "0" ]]; then
        return 0
      fi
    fi
    now="$(date +%s)"
    (( now - started >= timeout_seconds )) &&
      die "RuntimePool/${pool} stopped but still has ${claim_count:-unknown} claim(s), ${pod_count:-unknown} Pod(s), and ${sandbox_count:-unknown} Sandbox(es)"
    sleep 5
  done
}

capture_lc_running_fence() {
  local task="$1" fence_file="$2" pool_file="$3"
  local pool
  kubectl -n "${acp_task_namespace}" get task "${task}" -o json |
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
  ' "${fence_file}" >/dev/null || die "Task ${task} carries an incomplete Running execution fence"
  pool="$(jq -r '.poolName' "${fence_file}")"
  kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o json |
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
  ' "${pool_file}" >/dev/null || die "Task ${task} Running fence does not match the RuntimePool identity"
}

assert_lc_timeout_from_fence() {
  local task="$1" fence_file="$2" pool_file="$3"
  kubectl -n "${acp_task_namespace}" get task "${task}" -o json |
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
    kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
    die "Task ${task} did not settle as controller-owned TaskTimeout cancellation with its exact Running fence"
  }
}

assert_lc_ambiguous_write_outcome() {
  local task="$1" marker="$2"
  local pool_file="${work_dir}/lc-ambiguous-pool-${task}.json"
  local started now payload phase state pool pool_payload count_before count_after
  # Capture the provider-independent pool identity before waiting for terminal
  # settlement. The Task can release its live binding as soon as ambiguity is
  # projected, so a later RuntimePool read is not a reliable fence source.
  started="$(date +%s)"
  while true; do
    payload="$(kubectl -n "${acp_task_namespace}" get task "${task}" -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${payload}")"
    state="$(jq -r '.status.execution.state // ""' <<<"${payload}")"
    pool="$(jq -r '.status.execution.runtimePoolName // ""' <<<"${payload}")"
    if [[ -n "${pool}" ]] &&
      pool_payload="$(kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o json 2>/dev/null)"; then
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
      kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
      [[ ! -s "${pool_file}" ]] || cat "${pool_file}" >&2
      die "ambiguous-write Task settled before its independent RuntimePool identity was captured"
    fi
    now="$(date +%s)"
    (( now - started >= 300 )) && {
      kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
      die "ambiguous-write Task exposed no complete RuntimePool identity (phase=${phase:-<empty>}, state=${state:-<empty>})"
    }
    sleep 3
  done
  started="$(date +%s)"
  while true; do
    payload="$(kubectl -n "${acp_task_namespace}" get task "${task}" -o json 2>/dev/null || true)"
    phase="$(jq -r '.status.phase // ""' <<<"${payload}")"
    state="$(jq -r '.status.execution.state // ""' <<<"${payload}")"
    if [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" || "${phase}" == "Cancelled" ]]; then
      break
    fi
    now="$(date +%s)"
    (( now - started >= 300 )) && {
      kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
      die "ambiguous-write Task did not reach a terminal state (phase=${phase:-<empty>}, state=${state:-<empty>})"
    }
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
    kubectl -n "${acp_task_namespace}" get task "${task}" -o yaml >&2 || true
    cat "${pool_file}" >&2 || true
    die "ambiguous-write Task did not settle once as durable OutcomeUnknown"
  }
  count_before="$(fixture_marker_count "${marker}")"
  [[ "${count_before}" == "0" ]] ||
    die "ambiguous-write prompt reached the provider fixture ${count_before} time(s); want zero before acknowledgement"
  sleep 10
  count_after="$(fixture_marker_count "${marker}")"
  [[ "${count_after}" == "0" ]] ||
    die "ambiguous-write prompt was replayed to the provider fixture (${count_before} -> ${count_after})"
}

assert_lc_sandbox_replacement_identity() {
  local pool="$1" pool_uid="$2" task_instance="$3" prior_instance="$4"
  local claim_file="${work_dir}/orka-ws-lc-replaced-claims.json"
  local sandbox_file="${work_dir}/orka-ws-lc-replaced-sandbox.json"
  local pods_file="${work_dir}/orka-ws-lc-replaced-pods.json"
  local pool_file="${work_dir}/orka-ws-lc-replaced-pool.json"
  local claim_name sandbox_name

  kubectl -n "${acp_runtime_namespace}" get sandboxclaims \
    -l "orka.ai/runtime-pool-name=${pool},orka.ai/runtime-pool-uid=${pool_uid}" \
    -o json >"${claim_file}"
  claim_name="$(jq -r '
    [.items[] | select(.metadata.deletionTimestamp == null)]
    | if length == 1 then .[0].metadata.name else "" end
  ' "${claim_file}")"
  [[ -n "${claim_name}" ]] ||
    die "replacement pool ${pool} does not have exactly one live SandboxClaim for UID ${pool_uid}"
  sandbox_name="$(jq -r '
    [.items[] | select(.metadata.deletionTimestamp == null)][0]
    | (.metadata.annotations["agents.x-k8s.io/sandbox-name"] // "") as $assigned
    | (.status.sandbox.name // "") as $selected
    | select($selected != "")
    | select($assigned == "" or $assigned == $selected)
    | $selected
  ' "${claim_file}")"
  [[ -n "${sandbox_name}" ]] ||
    die "replacement SandboxClaim ${claim_name} has no selected Sandbox or a conflicting assignment annotation"

  kubectl -n "${acp_runtime_namespace}" get sandboxes.agents.x-k8s.io "${sandbox_name}" \
    -o json >"${sandbox_file}"
  kubectl -n "${acp_runtime_namespace}" get pods \
    -l "orka.ai/runtime-pool-name=${pool},orka.ai/runtime-pool-uid=${pool_uid}" \
    -o json >"${pods_file}"
  kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o json >"${pool_file}"

  jq -e -n \
    --arg pool "${pool}" \
    --arg poolUID "${pool_uid}" \
    --arg claimName "${claim_name}" \
    --arg sandboxName "${sandbox_name}" \
    --arg taskInstance "${task_instance}" \
    --arg priorInstance "${prior_instance}" \
    --slurpfile claims "${claim_file}" \
    --slurpfile sandboxes "${sandbox_file}" \
    --slurpfile pods "${pods_file}" \
    --slurpfile pools "${pool_file}" '
      ($claims[0].items | map(select(.metadata.deletionTimestamp == null))) as $liveClaims
      | ($pods[0].items | map(select(
          .metadata.deletionTimestamp == null
          and .status.phase != "Succeeded"
          and .status.phase != "Failed"
        ))) as $livePods
      | ($liveClaims[0]) as $claim
      | ($sandboxes[0]) as $sandbox
      | ($livePods[0]) as $pod
      | ($pools[0]) as $runtimePool
      | ($runtimePool.status.activeInstance) as $active
      | ($liveClaims | length) == 1
        and ($livePods | length) == 1
        and $claim.metadata.name == $claimName
        and ($claim.metadata.uid // "" | length > 0)
        and $claim.metadata.labels["orka.ai/runtime-pool-name"] == $pool
        and $claim.metadata.labels["orka.ai/runtime-pool-uid"] == $poolUID
        and (
          ($claim.metadata.annotations["agents.x-k8s.io/sandbox-name"] // "") as $assigned
          | ($assigned == "" or $assigned == $sandboxName)
        )
        and ($claim.status.sandbox.name // "") == $sandboxName
        and $sandbox.metadata.name == $sandboxName
        and ($sandbox.metadata.uid // "" | length > 0)
        and $sandbox.metadata.deletionTimestamp == null
        and $sandbox.metadata.labels["agents.x-k8s.io/claim-uid"] == $claim.metadata.uid
        and any($sandbox.metadata.ownerReferences[]?;
          .kind == "SandboxClaim"
          and .name == $claim.metadata.name
          and .uid == $claim.metadata.uid
          and .controller == true)
        and $pod.status.phase == "Running"
        and $pod.metadata.labels["orka.ai/runtime-pool-name"] == $pool
        and $pod.metadata.labels["orka.ai/runtime-pool-uid"] == $poolUID
        and any($pod.metadata.ownerReferences[]?;
          .kind == "Sandbox"
          and .name == $sandbox.metadata.name
          and .uid == $sandbox.metadata.uid
          and .controller == true)
        and $runtimePool.metadata.name == $pool
        and $runtimePool.metadata.uid == $poolUID
        and $runtimePool.status.lifecycle == "Serving"
        and (
          $runtimePool.status.admissionState == "Accepting"
          or (
            $runtimePool.status.admissionState == "Closed"
            and ($runtimePool.status.capacity.residentSessions // 0)
              >= $runtimePool.spec.capacity.maxResidentSessions
          )
        )
        and $active.podName == $pod.metadata.name
        and $active.podUID == $pod.metadata.uid
        and ($active.bootID // "" | length > 0)
        and $active.runtimeInstanceID == ($active.podUID + "." + $active.bootID)
        and $active.runtimeInstanceID == $taskInstance
        and $active.runtimeInstanceID != $priorInstance
    ' >/dev/null || {
    kubectl -n "${acp_task_namespace}" get runtimepool "${pool}" -o yaml >&2 || true
    die "replacement pool ${pool} is not fenced to its exact SandboxClaim, Sandbox, Pod, and new boot identity"
  }
}

# run_workspace_lifecycle_acp_task proves issue #411 through agent-sandbox:
# continuation, authenticated drain and recovery from zero, timeout and
# explicit cancellation of Running prompts, controller restart without replay,
# physical RuntimePool replacement, and exact cleanup while preserving the
# logical Session across every continuation.
run_workspace_lifecycle_acp_task() {
  log "Running workspace-backed lifecycle/recovery conformance (agent-sandbox)"
  # OutcomeUnknown deliberately makes a Session non-deletable because prompt
  # delivery cannot be proven absent. Use fresh Sessions for those cases on
  # each run and exclude them from fixed-name reset cleanup.
  local outcome_unknown_session_suffix="$(date -u +%s)-${RANDOM}"
  local ambiguous_session="orka-ws-lc-ambiguous-${outcome_unknown_session_suffix}"
  local restart_session="orka-ws-lc-restart-${outcome_unknown_session_suffix}"
  local restart_session_deletable=0
  # Marker counts and disconnect/history observations are package state in
  # the fixture process: on a reused cluster (stable run id or fixture
  # image), applying an unchanged Deployment does not restart the Pod, and
  # stale counts could satisfy - or fail - this run's exactly-once
  # assertions. Restart the fixture so every lifecycle run starts from zero.
  log "Restarting the Responses fixture to reset marker observations"
  kubectl -n vekil-system rollout restart deployment/vekil
  run kubectl -n vekil-system rollout status deployment/vekil --timeout=3m
  if [[ -n "${fixture_pf_pid}" ]]; then
    cleanup_one_port_forward "${fixture_pf_pid}"
    fixture_pf_pid=""
  fi

  bash "${repo_root}/scripts/lib/ensure-static-mode-namespace.sh" \
    kubectl "${acp_task_namespace}" harness-v2
  start_fixture_port_forward

  # The manager store is PVC-backed: on a reused cluster the fixed-name
  # Sessions from a previous (or interrupted) run would hand the create:true
  # Tasks the OLD Session UID and transcript. Reset them through the API so
  # every run exercises a clean Session.
  local reset_lc_session
  for reset_lc_session in orka-ws-lc-session orka-ws-lc-timeout-session \
    orka-ws-lc-cancel-session; do
    delete_fixed_session "${reset_lc_session}"
  done

  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: orka-ws-lc-agent
  namespace: ${acp_task_namespace}
spec:
  runtime:
    type: codex
    contractVersion: orka.harness.v2
    defaultMaxTurns: 1
  model:
    name: gpt-5.5
YAML
  apply_lifecycle_task orka-ws-lc-first orka-ws-lc-session true "Reply exactly: ORKA_WS_LC_FIRST_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-first '{.status.phase}' "Succeeded" 900
  assert_task_result_contains "${acp_task_namespace}" orka-ws-lc-first "ORKA_WS_LC_FIRST_OK"
  assert_lc_task_success_tuple orka-ws-lc-first
  assert_lc_task_success_fence orka-ws-lc-first
  # Every lifecycle turn must reach the provider EXACTLY once: sticky
  # history/disconnect observations would otherwise hide duplicate prompt
  # delivery outside the cancellation/restart scenarios.
  [[ "$(fixture_marker_count "ORKA_WS_LC_FIRST_OK")" == "1" ]] ||
    die "first lifecycle turn was delivered $(fixture_marker_count "ORKA_WS_LC_FIRST_OK") times; want exactly one"
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_FIRST_OK")" == "false" ]] ||
    die "first lifecycle turn unexpectedly carried prior session history"

  local pool_name pool_uid session_uid first_instance
  pool_name="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  pool_uid="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  record_lc_pool "${pool_name}"
  session_uid="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  first_instance="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  [[ "${pool_name}" == acp-ws-session-* && -n "${pool_uid}" && -n "${session_uid}" && -n "${first_instance}" ]] ||
    die "lifecycle Task did not bind a session workspace pool with runtime identities (pool=${pool_name:-<empty>} uid=${pool_uid:-<empty>})"

  log "Continuing the Session on the same physical runtime"
  apply_lifecycle_task orka-ws-lc-second orka-ws-lc-session false "Reply exactly: ORKA_WS_LC_SECOND_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-second '{.status.phase}' "Succeeded" 600
  assert_task_result_contains "${acp_task_namespace}" orka-ws-lc-second "ORKA_WS_LC_SECOND_OK"
  assert_lc_task_success_tuple orka-ws-lc-second
  assert_lc_task_success_fence orka-ws-lc-second
  local second_session second_instance second_pool second_pool_uid
  second_session="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  second_instance="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  second_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  second_pool_uid="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  [[ "${second_session}" == "${session_uid}" ]] ||
    die "continuation changed the RuntimeSession UID (${second_session:-<empty>} != ${session_uid})"
  # Session reuse must retain the SAME dedicated workspace pool: a second
  # Task selecting or creating another pool would both duplicate the
  # workspace and leak an untracked pool.
  [[ "${second_pool}" == "${pool_name}" && "${second_pool_uid}" == "${pool_uid}" ]] ||
    die "continuation moved to a different workspace pool (${second_pool:-<empty>}/${second_pool_uid:-<empty>} != ${pool_name}/${pool_uid})"
  # Semantic continuation proof: the fixture must have seen the replayed
  # session history in the continuation request, not just a fresh prompt that
  # happens to carry its own marker.
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_SECOND_OK")" == "true" ]] ||
    die "continuation request carried no prior session history; the runtime silently started a fresh session"
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_SECOND_OK" "ORKA_WS_LC_FIRST_OK")" == "true" ]] ||
    die "continuation history did not replay the expected first turn; an unrelated or truncated transcript was accepted"
  [[ "$(fixture_marker_count "ORKA_WS_LC_SECOND_OK")" == "1" ]] ||
    die "continuation turn was delivered $(fixture_marker_count "ORKA_WS_LC_SECOND_OK") times; want exactly one"
  # The physical instance may legitimately change between turns (the pool can
  # scale to zero while idle); the contract requires the logical Session to
  # survive, which the UID equality above proves. The session generation
  # fences the provider RuntimeSession, not the physical runtime. Transparent
  # reuse preserves it, an in-place RuntimeSession recreation may advance it,
  # and a scale-to-zero recovery onto a fresh runtime must advance it.
  local first_generation second_generation
  first_generation="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-first \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  second_generation="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-second \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${first_generation}" =~ ^[0-9]+$ && "${second_generation}" =~ ^[0-9]+$ ]] ||
    die "lifecycle turns carry no valid runtimeSessionGeneration (first=${first_generation:-<empty>} second=${second_generation:-<empty>})"
  if [[ "${second_instance}" == "${first_instance}" ]]; then
    (( second_generation >= first_generation )) ||
      die "continuation on the same instance regressed the session generation (${first_generation} -> ${second_generation})"
    if [[ "${second_generation}" == "${first_generation}" ]]; then
      log "Continuation reused the RuntimeSession on the same physical runtime instance"
    else
      log "Continuation recreated the RuntimeSession on the same physical runtime instance (${first_generation} -> ${second_generation})"
    fi
  else
    (( second_generation > first_generation )) ||
      die "recovery on a fresh instance did not advance the session generation (${first_generation} -> ${second_generation})"
    log "Continuation recovered the Session on a fresh physical runtime instance"
  fi

  log "Draining the Session pool to zero through every authenticated lifecycle barrier"
  drain_lc_pool_to_zero "${pool_name}" 600
  wait_for_lc_pool_stopped "${pool_name}" 600
  wait_for_lc_sandbox_runtime_zero "${pool_name}" 300

  log "Recovering the same logical Session from the stopped pool"
  apply_lifecycle_task orka-ws-lc-drained orka-ws-lc-session false \
    "Reply exactly: ORKA_WS_LC_DRAINED_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-drained '{.status.phase}' "Succeeded" 900
  assert_task_result_contains "${acp_task_namespace}" orka-ws-lc-drained "ORKA_WS_LC_DRAINED_OK"
  assert_lc_task_success_tuple orka-ws-lc-drained
  assert_lc_task_success_fence orka-ws-lc-drained
  local drained_session drained_instance drained_pool drained_pool_uid drained_generation
  drained_session="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  drained_instance="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  drained_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  drained_pool_uid="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  drained_generation="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${drained_session}" == "${session_uid}" ]] ||
    die "scale-to-zero recovery changed the RuntimeSession UID"
  [[ "${drained_pool}" == "${pool_name}" && "${drained_pool_uid}" == "${pool_uid}" ]] ||
    die "scale-to-zero recovery replaced the logical RuntimePool (${drained_pool:-<empty>}/${drained_pool_uid:-<empty>} != ${pool_name}/${pool_uid})"
  [[ -n "${drained_instance}" && "${drained_instance}" != "${second_instance}" ]] ||
    die "scale-to-zero recovery reused the stopped runtime instance"
  [[ "${drained_generation}" =~ ^[0-9]+$ && "${drained_generation}" -gt "${second_generation}" ]] ||
    die "scale-to-zero recovery did not advance the session generation (${drained_generation:-<empty>} <= ${second_generation})"
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_DRAINED_OK")" == "true" ]] ||
    die "scale-to-zero continuation carried no prior session history"
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_DRAINED_OK" "ORKA_WS_LC_SECOND_OK")" == "true" ]] ||
    die "scale-to-zero history did not replay the expected prior turn"
  [[ "$(fixture_marker_count "ORKA_WS_LC_DRAINED_OK")" == "1" ]] ||
    die "scale-to-zero turn was delivered $(fixture_marker_count "ORKA_WS_LC_DRAINED_OK") times; want exactly one"

  log "Timing out a Running prompt only after its configured deadline"
  local timeout_started timeout_pool timeout_fence_file timeout_pool_snapshot
  local timeout_hold_started timeout_hold_now timeout_hold_count timeout_elapsed
  timeout_started="$(date +%s)"
  apply_lifecycle_task orka-ws-lc-timeout orka-ws-lc-timeout-session true \
    "ORKA_HOLD_300S Reply exactly: ORKA_WS_LC_TIMEOUT_OK" "4m0s"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout \
    '{.status.execution.state}' "Running" 600
  timeout_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-timeout \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${timeout_pool}"
  timeout_fence_file="${work_dir}/orka-ws-lc-timeout-fence.json"
  timeout_pool_snapshot="${work_dir}/orka-ws-lc-timeout-pool.json"
  capture_lc_running_fence orka-ws-lc-timeout "${timeout_fence_file}" "${timeout_pool_snapshot}"
  timeout_hold_started="$(date +%s)"
  while true; do
    timeout_hold_count="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
    [[ "${timeout_hold_count}" =~ ^[0-9]+$ && "${timeout_hold_count}" -ge 1 ]] && break
    timeout_hold_now="$(date +%s)"
    (( timeout_hold_now - timeout_hold_started >= 180 )) &&
      die "held timeout prompt never reached the provider fixture"
    sleep 3
  done
  [[ "${timeout_hold_count}" == "1" ]] ||
    die "held timeout prompt was delivered ${timeout_hold_count} times before timeout; want exactly one"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout '{.status.phase}' "Cancelled" 420
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout \
    '{.status.execution.state}' "Cancelled" 120
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout \
    '{.status.execution.outcome}' "Cancelled" 120
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout \
    '{.status.execution.reason}' "TaskTimeout" 120
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-timeout \
    '{.status.execution.attempt}' "1" 60
  assert_lc_timeout_from_fence orka-ws-lc-timeout "${timeout_fence_file}" "${timeout_pool_snapshot}"
  timeout_elapsed=$(( $(date +%s) - timeout_started ))
  (( timeout_elapsed >= 240 )) ||
    die "timeout Task cancelled after ${timeout_elapsed}s, before its configured 4m0s deadline"
  local timeout_disconnect_started timeout_disconnect_now timeout_disconnects
  timeout_disconnect_started="$(date +%s)"
  while true; do
    timeout_disconnects="$(fixture_marker_disconnects "ORKA_WS_LC_TIMEOUT_OK")"
    [[ "${timeout_disconnects}" =~ ^[0-9]+$ && "${timeout_disconnects}" -ge 1 ]] && break
    timeout_disconnect_now="$(date +%s)"
    (( timeout_disconnect_now - timeout_disconnect_started >= 120 )) &&
      die "timeout never closed the in-flight provider stream (fixture disconnects=${timeout_disconnects:-0})"
    sleep 3
  done
  local timeout_count_settled timeout_count_later
  timeout_count_settled="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
  [[ "${timeout_count_settled}" == "1" ]] ||
    die "timed-out prompt must reach the provider fixture exactly once (count=${timeout_count_settled:-<empty>})"
  sleep 20
  timeout_count_later="$(fixture_marker_count "ORKA_WS_LC_TIMEOUT_OK")"
  [[ "${timeout_count_later}" == "1" ]] ||
    die "timed-out prompt was replayed after settlement (${timeout_count_settled} -> ${timeout_count_later})"
  log "Timed-out prompt settled after ${timeout_elapsed}s with no replay and a closed provider stream"

  log "Cancelling a Running prompt in a dedicated Session"
  apply_lifecycle_task orka-ws-lc-cancel orka-ws-lc-cancel-session true "ORKA_HOLD_180S Reply exactly: ORKA_WS_LC_CANCEL_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.execution.state}' "Running" 600
  local cancel_pool
  cancel_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel \
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
      die "held cancellation prompt never reached the provider fixture"
    fi
    sleep 3
  done
  [[ "${hold_count}" == "1" ]] ||
    die "held cancellation prompt was delivered ${hold_count} times before cancellation; want exactly one"
  # Hold the object visible through settlement so the terminal projection is
  # observable after deletion-triggered cancellation.
  kubectl -n "${acp_task_namespace}" patch task orka-ws-lc-cancel --type=json \
    -p '[{"op":"add","path":"/metadata/finalizers/-","value":"acp-e2e.orka.ai/lifecycle-observer"}]'
  # Capture the execution fence before the deletion-triggered cancellation:
  # settlement must preserve the exact identity, never a re-projection.
  local cancel_fence_file="${work_dir}/orka-ws-lc-cancel-fence.json"
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json |
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
  ' "${cancel_fence_file}" >/dev/null ||
    die "cancellation Task carries an incomplete execution fence before deletion"
  # The Task-side fence alone would let an incorrectly projected identity
  # self-compare into a pass: snapshot the RuntimePool's own identity as the
  # independent source and require the Task fence to match it BEFORE the
  # deletion; the settled Task is then compared against this snapshot too.
  local cancel_pool_snapshot="${work_dir}/orka-ws-lc-cancel-pool.json"
  local cancel_fence_pool
  cancel_fence_pool="$(jq -r '.poolName' "${cancel_fence_file}")"
  kubectl -n "${acp_task_namespace}" get runtimepool "${cancel_fence_pool}" -o json |
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
  ' "${cancel_pool_snapshot}" >/dev/null ||
    die "pre-cancellation Task fence does not match the RuntimePool's own identity"
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json |
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
    ' >/dev/null ||
    die "cancellation Task left its first Running attempt before deletion"
  kubectl -n "${acp_task_namespace}" delete task orka-ws-lc-cancel --wait=false
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.phase}' "Cancelled" 240
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.execution.state}' "Cancelled" 120
  # The canonical cancellation contract (live-acp-runtime-e2e) requires the
  # full controller-owned settlement tuple, not just phase and state.
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.execution.outcome}' "Cancelled" 120
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.execution.reason}' "Cancelled" 120
  # The canonical cancellation fence also requires exactly one execution
  # attempt: a rejected controller-side replay leaves the fixture count at
  # one while replay still happened.
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-cancel '{.status.execution.attempt}' "1" 60
  # And the complete pre-delete execution identity must be preserved,
  # validated against BOTH the Task-side fence and the independent
  # RuntimePool snapshot.
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json |
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
    ' >/dev/null ||
    die "cancellation settlement does not match the independent RuntimePool identity"
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json |
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
    ' >/dev/null ||
    die "cancellation settlement did not preserve the exact pre-delete execution fence"
  # Release the observer only after the controller's own cleanup finalizer has
  # completed and removed itself, so cancellation cleanup is never skipped.
  # kubectl's jsonpath stringifies arrays with fmt (no quotes), so the
  # finalizer list is parsed from -o json with jq like the canonical
  # task_observer_release_ready helper does.
  local release_started release_now cancel_task_json
  release_started="$(date +%s)"
  while true; do
    if ! cancel_task_json="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel -o json 2>/dev/null)"; then
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
      if kubectl -n "${acp_task_namespace}" patch task orka-ws-lc-cancel --type=json \
        -p '[{"op":"test","path":"/metadata/finalizers/0","value":"acp-e2e.orka.ai/lifecycle-observer"},{"op":"remove","path":"/metadata/finalizers/0"}]' >/dev/null; then
        break
      fi
    fi
    release_now="$(date +%s)"
    (( release_now - release_started >= 300 )) &&
      die "controller cleanup did not settle for the cancelled Task (finalizers=$(jq -c '.metadata.finalizers // []' <<<"${cancel_task_json}"))"
    sleep 3
  done
  local absent_started absent_now
  absent_started="$(date +%s)"
  while kubectl -n "${acp_task_namespace}" get task orka-ws-lc-cancel >/dev/null 2>&1; do
    absent_now="$(date +%s)"
    (( absent_now - absent_started >= 240 )) && die "cancelled Task was not removed after observer release"
    sleep 3
  done
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
    (( cancel_disconnect_now - cancel_disconnect_started >= 120 )) &&
      die "cancellation never closed the in-flight provider stream (fixture disconnects=${cancel_disconnects:-0})"
    sleep 3
  done
  log "Cancelled prompt settled with no replay and a closed provider stream (fixture requests: ${cancel_count_settled})"
  if [[ -n "${cancel_pool}" ]]; then
    kubectl -n "${acp_task_namespace}" delete runtimepool "${cancel_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi

  log "Forcing the prompt request-write/ack boundary into an ambiguous state"
  apply_lifecycle_task orka-ws-lc-ambiguous "${ambiguous_session}" true \
    "Reply exactly: ${lifecycle_ambiguity_marker}"
  assert_lc_ambiguous_write_outcome orka-ws-lc-ambiguous "${lifecycle_ambiguity_marker}"
  local ambiguous_pool
  ambiguous_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-ambiguous \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  [[ -n "${ambiguous_pool}" ]] || die "ambiguous-write Task carries no RuntimePool identity"
  record_lc_pool "${ambiguous_pool}"
  log "Ambiguous prompt write settled durably as OutcomeUnknown without provider delivery"

  log "Restarting the controller while a prompt is Running"
  apply_lifecycle_task orka-ws-lc-restart "${restart_session}" true "ORKA_HOLD_90S Reply exactly: ORKA_WS_LC_RESTART_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-restart '{.status.execution.state}' "Running" 600
  # Restart only once the held model request is in flight so the
  # before/after fixture counts prove the accepted request was not replayed.
  local restart_count_before restart_count_after restart_hold_started restart_hold_now
  restart_hold_started="$(date +%s)"
  while true; do
    restart_count_before="$(fixture_marker_count "ORKA_WS_LC_RESTART_OK")"
    [[ "${restart_count_before}" =~ ^[0-9]+$ && "${restart_count_before}" -ge 1 ]] && break
    restart_hold_now="$(date +%s)"
    if (( restart_hold_now - restart_hold_started >= 180 )); then
      die "held restart prompt never reached the provider fixture"
    fi
    sleep 3
  done
  [[ "${restart_count_before}" == "1" ]] ||
    die "held restart prompt was delivered ${restart_count_before} times before the restart; want exactly one"
  # Capture the execution fence before the restart: takeover must settle the
  # SAME prompt against the SAME pool, instance, and RuntimeSession identity,
  # never a re-projection under different infrastructure.
  local restart_fence_file="${work_dir}/orka-ws-lc-restart-fence.json"
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o json |
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
  ' "${restart_fence_file}" >/dev/null ||
    die "restart Task carries an incomplete execution fence before the restart"
  # The Task-side fence alone would let an incorrectly projected identity
  # self-compare into a pass: snapshot the RuntimePool's own identity as the
  # independent source (mirroring assert_restart_task_fence in
  # live-acp-runtime-e2e) and require the Task fence to match it BEFORE the
  # restart; the settled Task is then compared against this snapshot too.
  local restart_pool_snapshot="${work_dir}/orka-ws-lc-restart-pool.json"
  local restart_fence_pool
  restart_fence_pool="$(jq -r '.poolName' "${restart_fence_file}")"
  kubectl -n "${acp_task_namespace}" get runtimepool "${restart_fence_pool}" -o json |
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
  ' "${restart_pool_snapshot}" >/dev/null ||
    die "pre-restart Task fence does not match the RuntimePool's own identity"
  kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o json |
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
    ' >/dev/null ||
    die "restart Task left its first Running attempt before the controller restart"
  # Force an UNPLANNED restart: a graceful rollout runs the manager's preStop
  # ACP upgrade drain, which waits out the held prompt before the old
  # controller exits and never exercises takeover of an interrupted Running
  # prompt. Killing the Pod without its preStop hook does.
  kubectl -n "${orka_namespace}" delete pod -l control-plane=controller-manager \
    --grace-period=0 --force --wait=true
  run kubectl -n "${orka_namespace}" rollout status deployment/"${orka_controller_deployment}" --timeout=5m
  # The controller restart severs the Orka API port-forward; re-establish it
  # so later result assertions reach a live tunnel.
  cleanup_one_port_forward "${api_pf_pid}"
  api_pf_pid="$(start_port_forward "${orka_namespace}" "svc/${orka_api_service}" "${orka_api_local_port}" "${orka_api_service_port}" "${api_pf_log}")"
  wait_for_http "http://127.0.0.1:${orka_api_local_port}/readyz" "Orka API /readyz after controller restart"
  # The canonical restart contract (live-acp-runtime-e2e) accepts an adopted
  # completion, a clean cancellation, or a conservative Failed/OutcomeUnknown
  # settlement; the invariant is bounded settlement without replay, not
  # guaranteed completion. This provider lane additionally proves a cancelled
  # prompt's model stream disconnected.
  local restart_started restart_now restart_json restart_phase restart_state restart_outcome restart_reason restart_attempt
  restart_started="$(date +%s)"
  while true; do
    restart_json="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o json 2>/dev/null || true)"
    restart_phase="$(jq -r '.status.phase // ""' <<<"${restart_json}")"
    [[ "${restart_phase}" == "Succeeded" || "${restart_phase}" == "Failed" || "${restart_phase}" == "Cancelled" ]] && break
    restart_now="$(date +%s)"
    if (( restart_now - restart_started >= 600 )); then
      kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
      die "restart Task did not settle after the controller restart (phase=${restart_phase:-<empty>})"
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
    kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
    die "restart Task recorded ${restart_attempt} execution attempts; the restart contract requires exactly 1"
  }
  # Whatever the terminal tuple, takeover must have preserved the exact
  # pre-restart execution identity, checked against BOTH the Task-side fence
  # and the independent RuntimePool snapshot (canonical
  # assert_restart_task_fence).
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
    kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
    die "restart settlement does not match the independent pre-restart RuntimePool identity"
  }
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
    kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
    die "restart takeover did not preserve the exact pre-restart execution fence"
  }
  if [[ "${restart_phase}" == "Succeeded" && "${restart_state}" == "Succeeded" && "${restart_outcome}" == "Succeeded" ]]; then
    # The canonical restart contract also requires a ReadValidated delivery
    # projection with the successful tuple; a restored result without it is
    # incomplete post-restart settlement.
    jq -e '.status.delivery.state == "ReadValidated" and .status.delivery.outcome == "ReadValidated"' \
      <<<"${restart_json}" >/dev/null || {
      kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
      die "restart Task succeeded without a ReadValidated delivery projection"
    }
    assert_task_result_contains "${acp_task_namespace}" orka-ws-lc-restart "ORKA_WS_LC_RESTART_OK"
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
      [[ "${restart_disconnects}" =~ ^[0-9]+$ && "${restart_disconnects}" -ge 1 ]] && break
      restart_disconnect_now="$(date +%s)"
      (( restart_disconnect_now - restart_disconnect_started >= 120 )) &&
        die "cancelled restart settlement never closed the in-flight provider stream (fixture disconnects=${restart_disconnects:-0})"
      sleep 3
    done
    restart_session_deletable=1
    log "Restart Task settled as a clean cancellation under the new controller epoch with a closed provider stream"
  else
    kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
    die "restart Task settled outside the restart contract (phase=${restart_phase} state=${restart_state} outcome=${restart_outcome} reason=${restart_reason})"
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
    takeover_pool_json="$(kubectl -n "${acp_task_namespace}" get runtimepool "${restart_fence_pool}" -o json 2>/dev/null || true)"
    if jq -e --slurpfile snap "${restart_pool_snapshot}" '
      ((.status.controllerEpoch | type) == "number")
      and (.metadata.uid == $snap[0].poolUID)
      and (.status.controllerEpoch > $snap[0].controllerEpoch)
    ' <<<"${takeover_pool_json}" >/dev/null 2>&1; then
      break
    fi
    epoch_advance_now="$(date +%s)"
    (( epoch_advance_now - epoch_advance_started >= 120 )) &&
      die "the RuntimePool controller epoch did not advance across the forced restart"
    sleep 3
  done
  takeover_epoch="$(jq -r '.status.controllerEpoch' <<<"${takeover_pool_json}")"
  restart_json="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o json)"
  jq -e --argjson takeoverEpoch "${takeover_epoch}" '
    (.status.execution.controllerEpoch | type) == "number"
    and .status.execution.controllerEpoch == $takeoverEpoch
  ' <<<"${restart_json}" >/dev/null || {
    kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart -o yaml >&2 || true
    die "restart Task controller epoch does not match the takeover RuntimePool epoch"
  }
  local restart_pool
  restart_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-restart \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  record_lc_pool "${restart_pool}"

  log "Replacing the physical runtime and recovering the Session from zero"
  # The session generation is part of the authorization fence and must
  # advance monotonically across a pool replacement (canonical
  # live-acp-runtime-e2e replacement check).
  local pre_replacement_generation
  pre_replacement_generation="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-drained \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${pre_replacement_generation}" =~ ^[0-9]+$ && "${pre_replacement_generation}" -ge 1 ]] ||
    die "post-drain continuation carries no valid runtimeSessionGeneration (${pre_replacement_generation:-<empty>})"
  run kubectl -n "${acp_task_namespace}" delete runtimepool "${pool_name}" --wait=true --timeout=5m
  # RuntimePool finalization does not wait for provider-created Sandbox and
  # Pod dependents; recreating the deterministic pool while garbage
  # collection still drains them would overlap the incarnations and let the
  # recovery-from-zero check pass without ever observing zero. Poll the old
  # pool's provider resources to zero first.
  local replacement_barrier_started replacement_barrier_now replacement_leftovers replacement_sandboxes
  replacement_barrier_started="$(date +%s)"
  while true; do
    replacement_leftovers="$(kubectl get sandboxclaims,sandboxwarmpools,sandboxtemplates,pods -n "${acp_runtime_namespace}" \
      -l "orka.ai/runtime-pool-name=${pool_name}" -o name | wc -l | tr -d ' ')"
    if ! replacement_sandboxes="$(kubectl get sandboxes.agents.x-k8s.io -n "${acp_runtime_namespace}" -o name 2>&1)"; then
      die "replacement barrier could not query provider Sandboxes: ${replacement_sandboxes}"
    fi
    if [[ "${replacement_leftovers}" == "0" && "$(grep -c "/${pool_name}-" <<<"${replacement_sandboxes}" || true)" == "0" ]]; then
      break
    fi
    replacement_barrier_now="$(date +%s)"
    (( replacement_barrier_now - replacement_barrier_started >= 300 )) &&
      die "old pool ${pool_name} still has provider dependents after 300s; recovery-from-zero cannot start"
    sleep 5
  done
  apply_lifecycle_task orka-ws-lc-replaced orka-ws-lc-session false "Reply exactly: ORKA_WS_LC_REPLACED_OK"
  wait_for_jsonpath task "${acp_task_namespace}" orka-ws-lc-replaced '{.status.phase}' "Succeeded" 900
  assert_task_result_contains "${acp_task_namespace}" orka-ws-lc-replaced "ORKA_WS_LC_REPLACED_OK"
  assert_lc_task_success_tuple orka-ws-lc-replaced
  assert_lc_task_success_fence orka-ws-lc-replaced
  local replaced_session replaced_instance replaced_pool replaced_pool_uid
  replaced_session="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeSessionUID}')"
  replaced_instance="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeInstanceID}')"
  replaced_pool="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimePoolName}')"
  replaced_pool_uid="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimePoolUID}')"
  [[ "${replaced_session}" == "${session_uid}" ]] ||
    die "physical replacement changed the logical RuntimeSession UID"
  # Recovery must recreate the SAME deterministic dedicated session pool as a
  # NEW incarnation, not dispatch through a plain or unrelated pool.
  [[ "${replaced_pool}" == "${pool_name}" ]] ||
    die "replacement dispatched through pool ${replaced_pool:-<empty>}, want the recreated ${pool_name}"
  [[ -n "${replaced_pool_uid}" && "${replaced_pool_uid}" != "${pool_uid}" ]] ||
    die "replacement did not recreate the session workspace pool as a new incarnation (uid ${replaced_pool_uid:-<empty>})"
  [[ "$(fixture_marker_saw_history "ORKA_WS_LC_REPLACED_OK")" == "true" ]] ||
    die "post-replacement continuation carried no prior session history; the recovered session lost its transcript"
  [[ "$(fixture_marker_history_contains "ORKA_WS_LC_REPLACED_OK" "ORKA_WS_LC_DRAINED_OK")" == "true" ]] ||
    die "post-replacement history did not replay the expected prior turn; the recovered transcript is not this session's"
  [[ -n "${replaced_instance}" && "${replaced_instance}" != "${drained_instance}" ]] ||
    die "physical replacement did not produce a new runtime instance identity"
  local replaced_generation
  replaced_generation="$(kubectl -n "${acp_task_namespace}" get task orka-ws-lc-replaced \
    -o jsonpath='{.status.execution.runtimeSessionGeneration}')"
  [[ "${replaced_generation}" =~ ^[0-9]+$ && "${replaced_generation}" -gt "${pre_replacement_generation}" ]] ||
    die "replacement did not advance the session generation (${replaced_generation:-<empty>} <= ${pre_replacement_generation})"
  assert_lc_sandbox_replacement_identity \
    "${replaced_pool}" "${replaced_pool_uid}" "${replaced_instance}" "${drained_instance}"
  [[ "$(fixture_marker_count "ORKA_WS_LC_REPLACED_OK")" == "1" ]] ||
    die "post-replacement turn was delivered $(fixture_marker_count "ORKA_WS_LC_REPLACED_OK") times; want exactly one"

  local timeout_pool_uid cancel_pool_uid ambiguous_pool_uid restart_pool_uid
  timeout_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${timeout_pool_snapshot}")"
  cancel_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${cancel_pool_snapshot}")"
  ambiguous_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' \
    "${work_dir}/lc-ambiguous-pool-orka-ws-lc-ambiguous.json")"
  restart_pool_uid="$(jq -er '.poolUID | select(type == "string" and length > 0)' "${restart_pool_snapshot}")"

  if [[ "${restart_session_deletable}" == "1" ]]; then
    delete_fixed_session "${restart_session}"
  fi
  log "Cleaning up lifecycle Tasks and pools"
  run kubectl -n "${acp_task_namespace}" delete task orka-ws-lc-first orka-ws-lc-second \
    orka-ws-lc-drained orka-ws-lc-timeout orka-ws-lc-ambiguous orka-ws-lc-restart orka-ws-lc-replaced \
    --wait=true --timeout=4m
  kubectl -n "${acp_task_namespace}" delete runtimepool "${pool_name}" --ignore-not-found=true --wait=true --timeout=5m
  if [[ -n "${timeout_pool}" ]]; then
    kubectl -n "${acp_task_namespace}" delete runtimepool "${timeout_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  if [[ -n "${restart_pool}" ]]; then
    kubectl -n "${acp_task_namespace}" delete runtimepool "${restart_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  if [[ -n "${ambiguous_pool}" ]]; then
    kubectl -n "${acp_task_namespace}" delete runtimepool "${ambiguous_pool}" --ignore-not-found=true --wait=true --timeout=5m
  fi
  kubectl -n "${acp_task_namespace}" delete agent orka-ws-lc-agent --ignore-not-found=true
  # Exact-cleanup proof covers every scenario pool - continuation, cancel,
  # and restart are distinct session-scoped pools - and also queries the
  # realized Sandboxes, Pods, pool Secrets, and NetworkPolicies, not just
  # claim-side objects.
  # RuntimePool finalization deletes its controller-owned children with
  # background propagation and does not wait for provider-created Sandbox and
  # Pod dependents; garbage collection can still be draining them after
  # `kubectl delete runtimepool --wait` returns. Poll each pool's dependents
  # to zero with a bounded timeout instead of failing an otherwise correct
  # run on an immediate count.
  local -a cleanup_pool_specs=(
    "${pool_name}|${pool_uid}"
    "${replaced_pool}|${replaced_pool_uid}"
    "${timeout_pool}|${timeout_pool_uid}"
    "${cancel_pool}|${cancel_pool_uid}"
    "${ambiguous_pool}|${ambiguous_pool_uid}"
    "${restart_pool}|${restart_pool_uid}"
  )
  local cleanup_pool_spec leftover_pool leftover_pool_uid pool_selector
  local leftovers sandbox_leftovers policy_leftovers cleanup_poll_started cleanup_poll_now
  for cleanup_pool_spec in "${cleanup_pool_specs[@]}"; do
    IFS='|' read -r leftover_pool leftover_pool_uid <<<"${cleanup_pool_spec}"
    [[ -n "${leftover_pool}" && -n "${leftover_pool_uid}" ]] ||
      die "lifecycle cleanup is missing an exact RuntimePool identity (${cleanup_pool_spec})"
    pool_selector="orka.ai/runtime-pool-namespace=${acp_task_namespace},orka.ai/runtime-pool-name=${leftover_pool},orka.ai/runtime-pool-uid=${leftover_pool_uid}"
    cleanup_poll_started="$(date +%s)"
    while true; do
      leftovers="$(kubectl get sandboxclaims,sandboxwarmpools,sandboxtemplates,pods,secrets -n "${acp_runtime_namespace}" \
        -l "${pool_selector}" -o name | wc -l | tr -d ' ')"
      # The provider query itself must succeed: a transient API or CRD error
      # would otherwise produce an empty stream and a false zero count.
      local sandbox_names
      if ! sandbox_names="$(kubectl get sandboxes.agents.x-k8s.io -n "${acp_runtime_namespace}" -o name 2>&1)"; then
        die "exact-cleanup verification could not query provider Sandboxes: ${sandbox_names}"
      fi
      sandbox_leftovers="$(grep -c "/${leftover_pool}-" <<<"${sandbox_names}" || true)"
      local policy_names
      if ! policy_names="$(kubectl get networkpolicies -A \
        -l "${pool_selector}" -o name 2>&1)"; then
        die "exact-cleanup verification could not query RuntimePool NetworkPolicies: ${policy_names}"
      fi
      policy_leftovers="$(grep -c . <<<"${policy_names}" || true)"
      [[ "${leftovers}" == "0" && "${sandbox_leftovers}" == "0" && "${policy_leftovers}" == "0" ]] && break
      cleanup_poll_now="$(date +%s)"
      (( cleanup_poll_now - cleanup_poll_started >= 300 )) &&
        die "lifecycle cleanup left ${leftovers} namespaced pool object(s), ${sandbox_leftovers} Sandbox(es), and ${policy_leftovers} NetworkPolicy object(s) for ${leftover_pool}/${leftover_pool_uid} after 300s"
      sleep 5
    done
  done
  for reset_lc_session in orka-ws-lc-session orka-ws-lc-timeout-session \
    orka-ws-lc-cancel-session; do
    delete_fixed_session "${reset_lc_session}"
  done
  kubectl -n "${acp_task_namespace}" delete configmap orka-ws-lc-pools --ignore-not-found=true >/dev/null 2>&1 || true
  log "Workspace-backed lifecycle/recovery conformance (agent-sandbox) passed"
}

main() {
  require_cmd make
  require_cmd go
  require_cmd docker
  require_cmd kind
  require_cmd kubectl
  require_cmd curl
  require_cmd jq
  require_cmd openssl

  cd "${repo_root}"
  [[ -f "${manager_kustomization}" ]] || die "missing ${manager_kustomization}"
  cp "${manager_kustomization}" "${manager_kustomization_backup}"

  trap 'status=$?; on_exit "${status}"; exit "${status}"' EXIT

  setup_kind_cluster
  log "Installing current Orka CRDs into the test cluster"
  run make install
  log "Creating the Vekil namespace required by the production ingress policy"
  kubectl create namespace vekil-system --dry-run=client -o yaml | kubectl apply -f -
  orka_kind_registry_start "${kind_cluster}"

  install_agent_sandbox

  log "Building manager image ${manager_image}"
  run make docker-build IMG="${manager_image}"
  log "Building workspace publisher image ${publisher_image}"
  run make docker-build-workspace-publisher WORKSPACE_PUBLISHER_IMG="${publisher_image}"
  if [[ "${acp_task_smoke_enabled}" == "1" || "${suspend_resume_enabled}" == "1" || "${lifecycle_enabled}" == "1" ]]; then
    log "Building immutable Codex ACP runtime image ${acp_codex_runtime_image} for the workspace-backed Task smoke"
    run make docker-build-acp-codex-runtime ACP_CODEX_RUNTIME_IMG="${acp_codex_runtime_image}"
    log "Building local Responses-compatible provider fixture image ${responses_fixture_image}"
    run docker build -t "${responses_fixture_image}" -f scripts/fixtures/openai-responses/Dockerfile .
  fi

  write_sandbox_fixture_dockerfile
  log "Building agent-sandbox HTTP fixture image ${sandbox_fixture_image}"
  run docker build -t "${sandbox_fixture_image}" -f "${fixture_dockerfile}" .
  build_sandbox_router_image

  log "Loading images into Kind cluster ${kind_cluster}"
  run kind load docker-image "${manager_image}" --name "${kind_cluster}"
  run kind load docker-image "${sandbox_fixture_image}" --name "${kind_cluster}"
  run kind load docker-image "${sandbox_router_image}" --name "${kind_cluster}"
  if [[ "${acp_task_smoke_enabled}" == "1" || "${suspend_resume_enabled}" == "1" || "${lifecycle_enabled}" == "1" ]]; then
    run kind load docker-image "${responses_fixture_image}" --name "${kind_cluster}"
  fi

  local manager_ref publisher_ref
  manager_ref="$(orka_kind_registry_push "${manager_image}" "orka/controller")"
  publisher_ref="$(orka_kind_registry_push "${publisher_image}" "orka/workspace-publisher")"
  local placeholder_digest codex_runtime_ref
  placeholder_digest="sha256:$(printf '0%.0s' {1..64})"
  codex_runtime_ref="example.invalid/orka/acp-codex@${placeholder_digest}"
  if [[ "${acp_task_smoke_enabled}" == "1" || "${suspend_resume_enabled}" == "1" || "${lifecycle_enabled}" == "1" ]]; then
    codex_runtime_ref="$(orka_kind_registry_push "${acp_codex_runtime_image}" "orka/acp-codex-runtime")"
  fi

  log "Bootstrapping test-only admission TLS"
  orka_e2e_remove_admission_webhooks
  orka_e2e_bootstrap_admission_tls kubectl "${orka_namespace}"

  if [[ "${acp_task_smoke_enabled}" == "1" || "${suspend_resume_enabled}" == "1" || "${lifecycle_enabled}" == "1" ]]; then
    deploy_responses_fixture
  fi

  log "Deploying Orka manager (Codex runtime image real when the workspace-backed Task smoke is enabled; other runtimes inert)"
  run make deploy \
    IMG="${manager_ref}" \
    WORKSPACE_PUBLISHER_IMG="${publisher_ref}" \
    ACP_CODEX_RUNTIME_IMG="${codex_runtime_ref}" \
    ACP_CLAUDE_RUNTIME_IMG="example.invalid/orka/acp-claude@${placeholder_digest}" \
    ACP_COPILOT_RUNTIME_IMG="example.invalid/orka/acp-copilot@${placeholder_digest}" \
    ACP_OPENCODE_RUNTIME_IMG="example.invalid/orka/acp-opencode@${placeholder_digest}"
  run kubectl wait --for=condition=Established crd/tasks.core.orka.ai --timeout=60s
  log "Deploying fail-closed Orka admission with the controller image under test"
  orka_e2e_deploy_admission "${manager_ref}" kubectl "${orka_namespace}"
  ensure_api_client_identity
  deploy_sandbox_router
  patch_controller_for_agent_sandbox

  log "Port-forwarding Orka API service"
  api_pf_pid="$(start_port_forward "${orka_namespace}" "svc/${orka_api_service}" "${orka_api_local_port}" "${orka_api_service_port}" "${api_pf_log}")"
  local api_base
  api_base="http://127.0.0.1:${orka_api_local_port}"
  wait_for_http "${api_base}/readyz" "Orka API /readyz"

  reset_e2e_resources
  apply_sandbox_template

  log "Port-forwarding sandbox router service"
  router_pf_pid="$(start_port_forward "${router_namespace}" "svc/sandbox-router-svc" "${router_api_local_port}" "8080" "${router_pf_log}")"
  local router_base
  router_base="http://127.0.0.1:${router_api_local_port}"
  wait_for_http "${router_base}/healthz" "sandbox router /healthz"

  run_workspace_smoke "${router_base}"

  if [[ "${acp_task_smoke_enabled}" == "1" ]]; then
    run_workspace_backed_acp_task_smoke
  else
    log "Skipping workspace-backed ACP Task smoke (ORKA_AGENT_SANDBOX_ACP_TASK_SMOKE=0)"
  fi
  if [[ "${suspend_resume_enabled}" == "1" ]]; then
    run_workspace_suspend_resume_acp_task
  else
    log "Skipping class-backed suspend/cold-resume conformance (ORKA_AGENT_SANDBOX_SUSPEND_RESUME=0)"
  fi
  if [[ "${lifecycle_enabled}" == "1" ]]; then
    run_workspace_lifecycle_acp_task
  else
    log "Skipping workspace-backed lifecycle/recovery conformance (ORKA_AGENT_SANDBOX_LIFECYCLE=0)"
  fi
  log "Live agent-sandbox installation/configuration/workspace-adapter e2e passed"
}

main "$@"
