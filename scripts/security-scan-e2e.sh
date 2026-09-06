#!/usr/bin/env bash

set -Eeuo pipefail

sanitize_image_tag() {
  printf '%s' "$1" | LC_ALL=C tr -c 'A-Za-z0-9_.-' '-'
}

parse_github_repository_identity() {
  local repo_url="${1%/}"
  repo_url="${repo_url%.git}"
  if [[ ! "${repo_url}" =~ ^https://github\.com/([^/]+)/([^/]+)$ ]]; then
    return 1
  fi
  printf '%s\t%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=scripts/lib/e2e-common.sh
. "${script_dir}/lib/e2e-common.sh"
# shellcheck source=scripts/lib/redact.sh
. "${script_dir}/lib/redact.sh"
# shellcheck source=scripts/lib/kind-local-registry.sh
. "${script_dir}/lib/kind-local-registry.sh"
# shellcheck source=scripts/lib/e2e-admission-tls.sh
. "${script_dir}/lib/e2e-admission-tls.sh"

e2e_run_id="$(sanitize_image_tag "${ORKA_SECURITY_SCAN_RUN_ID:-${GITHUB_RUN_ID:-manual}-$(date -u +%Y%m%d%H%M%S)}")"
default_kind_suffix="${e2e_run_id:0:32}"
kind_cluster="${KIND_CLUSTER:-orka-security-scan-${default_kind_suffix}}"
orka_namespace="${ORKA_NAMESPACE:-orka-system}"
test_namespace="${ORKA_SECURITY_SCAN_E2E_NAMESPACE:-${orka_namespace}}"
orka_controller_deployment="${ORKA_CONTROLLER_DEPLOYMENT:-orka-controller-manager}"
wait_timeout="${ORKA_SECURITY_SCAN_WAIT_TIMEOUT:-25m}"
target_repo="${ORKA_SECURITY_SCAN_TARGET_REPO:-https://github.com/sozercan/nodejs-goof}"
target_branch="${ORKA_SECURITY_SCAN_TARGET_BRANCH:-main}"
target_ref="${ORKA_SECURITY_SCAN_TARGET_REF:-add14ba59e98240d9e00a235dd7d42cd61ae9912}"
read -r target_owner target_repository < <(parse_github_repository_identity "${target_repo}") ||
  die "ORKA_SECURITY_SCAN_TARGET_REPO must be a credential-free HTTPS github.com repository URL"
agent_name="${ORKA_SECURITY_SCAN_AGENT:-security-scan-e2e-agent}"
scan_name="${ORKA_SECURITY_SCAN_NAME:-security-goof}"
bad_scan_name="${ORKA_SECURITY_BAD_SCAN_NAME:-security-goof-malformed-result}"
authority_observer_name="${ORKA_SECURITY_SCAN_AUTHORITY_OBSERVER_NAME:-security-scan-authority-observer}"
authority_agent_name="${ORKA_SECURITY_SCAN_AUTHORITY_AGENT:-security-scan-authority-agent}"
# The deterministic ACP fixture advertises and calls this exact protocol name.
authority_tool_name="authority-probe"
authority_policy_name="${ORKA_SECURITY_SCAN_AUTHORITY_POLICY:-authority-observer-gateway}"
authority_incoming_task="${ORKA_SECURITY_SCAN_AUTHORITY_INCOMING_TASK:-authority-incoming}"
authority_service_account_task="${ORKA_SECURITY_SCAN_AUTHORITY_SERVICE_ACCOUNT_TASK:-authority-service-account}"
authority_scope="${ORKA_SECURITY_SCAN_AUTHORITY_SCOPE:-authority.execute}"
api_identity_name="${ORKA_SECURITY_SCAN_API_IDENTITY:-security-scan-e2e}"
api_local_port="${ORKA_SECURITY_SCAN_API_LOCAL_PORT:-18086}"
keep_cluster="${KEEP_CLUSTER:-0}"
kind_cleanup_armed="0"
registry_cleanup_armed="0"
kind_lock_held="0"
api_forward_pid=""

manager_image="${ORKA_MANAGER_IMAGE:-orka-controller:security-scan-e2e-${e2e_run_id}}"
publisher_image="${ORKA_WORKSPACE_PUBLISHER_IMAGE:-orka-workspace-publisher:security-scan-e2e-${e2e_run_id}}"
general_worker_image="${ORKA_GENERAL_WORKER_IMAGE:-orka-general-worker:security-scan-e2e-${e2e_run_id}}"
fake_runtime_image="${ORKA_SECURITY_SCAN_FAKE_RUNTIME_IMAGE:-orka-acp-security-fixture:security-scan-e2e-${e2e_run_id}}"

work_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/security-scan-e2e.XXXXXX")"
diagnostics_root="${ORKA_SECURITY_SCAN_DIAGNOSTICS_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/security-scan-e2e-diagnostics}"
diagnostics_dir="${diagnostics_root}/${e2e_run_id}"
kind_config="${ORKA_SECURITY_SCAN_KIND_CONFIG:-${work_dir}/kind-config.yaml}"
manager_kustomization="${repo_root}/config/manager/kustomization.yaml"
manager_kustomization_backup="${work_dir}/manager-kustomization.yaml.bak"
api_forward_log="${work_dir}/api-port-forward.log"
api_token_file="${work_dir}/api-token"
api_auth_header_file="${work_dir}/api-auth-header"
kubeconfig_file="${work_dir}/kubeconfig"
kind_lock_dir=""
registry_owner="security-scan-e2e-${e2e_run_id}"
diagnostics_collected="0"

run() {
  printf '+ ' >&2
  printf '%q ' "$@" >&2
  printf '\n' >&2
  "$@"
}

run_redacted() {
  set +e
  "$@" 2>&1 | redact
  local rc=${PIPESTATUS[0]}
  set -e
  return "${rc}"
}

restore_manager_kustomization() {
  if [[ -f "${manager_kustomization_backup}" ]]; then
    cp "${manager_kustomization_backup}" "${manager_kustomization}"
  fi
}

capture_redacted() {
  local output_file="$1"
  shift
  "$@" 2>&1 | redact >"${output_file}" || true
}

collect_security_scan_diagnostics() {
  if [[ "${diagnostics_collected}" == "1" ]]; then
    return 0
  fi
  diagnostics_collected="1"

  mkdir -p "${diagnostics_dir}/jobs" "${diagnostics_dir}/runtime"
  chmod 700 "${diagnostics_root}" "${diagnostics_dir}" "${diagnostics_dir}/jobs" "${diagnostics_dir}/runtime" 2>/dev/null || true
  log "Collecting redacted SecurityScan diagnostics in ${diagnostics_dir}"

  jq -n \
    --arg collectedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg runID "${e2e_run_id}" \
    --arg cluster "${kind_cluster}" \
    --arg namespace "${test_namespace}" \
    '{collectedAt:$collectedAt,runID:$runID,cluster:$cluster,namespace:$namespace}' \
    >"${diagnostics_dir}/metadata.json"

  kubectl -n "${test_namespace}" get repositoryscans -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {
        name: .metadata.name,
        namespace: .metadata.namespace,
        uid: .metadata.uid,
        generation: .metadata.generation,
        creationTimestamp: .metadata.creationTimestamp,
        labels: .metadata.labels
      },
      spec: {
        provider: .spec.provider,
        repoURL: .spec.repoURL,
        owner: .spec.owner,
        repository: .spec.repository,
        branch: .spec.branch,
        ref: .spec.ref,
        subPath: .spec.subPath,
        analysisAgentRef: .spec.analysisAgentRef
      },
      status: .status
    }]}' | redact >"${diagnostics_dir}/repository-scans.json" || true

  kubectl -n "${test_namespace}" get tasks -o json 2>/dev/null |
    jq \
      --arg scanName "${scan_name}" \
      --arg badScanName "${bad_scan_name}" '
      {items: [.items[] |
      select(
        .metadata.labels["orka.ai/security-target"] == $scanName or
        .metadata.labels["orka.ai/security-target"] == $badScanName or
        .metadata.labels["orka.ai/security-scan-authority-e2e"] == "true"
      ) | {
      metadata: {
        name: .metadata.name,
        namespace: .metadata.namespace,
        uid: .metadata.uid,
        generation: .metadata.generation,
        creationTimestamp: .metadata.creationTimestamp,
        labels: .metadata.labels,
        ownerReferences: .metadata.ownerReferences
      },
      spec: {
        type: .spec.type,
        image: .spec.image,
        command: .spec.command,
        timeout: .spec.timeout,
        priority: .spec.priority,
        agentRef: .spec.agentRef,
        workspace: (if .spec.workspace == null then null else {
          intent: .spec.workspace.intent,
          gitRepo: .spec.workspace.gitRepo,
          branch: .spec.workspace.branch,
          ref: .spec.workspace.ref
        } end)
      },
      status: .status
    }]}' | redact >"${diagnostics_dir}/tasks.json" || true

  kubectl -n "${test_namespace}" get agents -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid, generation: .metadata.generation},
      spec: {runtime: .spec.runtime, model: .spec.model},
      status: .status
    }]}' | redact >"${diagnostics_dir}/agents.json" || true

  kubectl -n "${test_namespace}" get runtimepools -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {
        name: .metadata.name,
        namespace: .metadata.namespace,
        uid: .metadata.uid,
        generation: .metadata.generation,
        creationTimestamp: .metadata.creationTimestamp,
        labels: .metadata.labels,
        ownerReferences: .metadata.ownerReferences
      },
      spec: {
        trustDomain: .spec.trustDomain,
        runtimeNamespace: .spec.runtimeNamespace,
        runtime: (if .spec.runtime == null then null else {
          image: .spec.runtime.image,
          profile: (if .spec.runtime.profile == null then null else {
            protocolVersion: .spec.runtime.profile.protocolVersion,
            digest: .spec.runtime.profile.digest,
            digestSchemaVersion: .spec.runtime.profile.digestSchemaVersion,
            acpProfile: .spec.runtime.profile.acpProfile,
            providerKind: .spec.runtime.profile.providerKind,
            model: .spec.runtime.profile.model,
            modelLimits: .spec.runtime.profile.modelLimits,
            agentConfigurationDigest: .spec.runtime.profile.agentConfigurationDigest,
            toolPolicyDigest: .spec.runtime.profile.toolPolicyDigest,
            approvalPolicyDigest: .spec.runtime.profile.approvalPolicyDigest,
            mcpConfigurationDigest: .spec.runtime.profile.mcpConfigurationDigest,
            workspaceIntent: .spec.runtime.profile.workspaceIntent,
            proxyCredentialRole: .spec.runtime.profile.proxyCredentialRole,
            proxyCredentialScope: .spec.runtime.profile.proxyCredentialScope,
            resourceClass: .spec.runtime.profile.resourceClass
          } end)
        } end),
        desiredReplicas: .spec.desiredReplicas,
        capacity: .spec.capacity,
        coldStartTimeoutSeconds: .spec.coldStartTimeoutSeconds
      },
      status: .status
    }]}' | redact >"${diagnostics_dir}/runtime-pools.json" || true

  kubectl -n "${test_namespace}" get promptattempts -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid, labels: .metadata.labels},
      spec: {
        id: .spec.id,
        taskUid: .spec.taskUid,
        attempt: .spec.attempt,
        promptId: .spec.promptId,
        requestDigest: .spec.requestDigest,
        bindingDigest: .spec.bindingDigest,
        snapshotDigest: .spec.snapshotDigest
      },
      status: .status
    }]}' | redact >"${diagnostics_dir}/prompt-attempts.json" || true

  kubectl -n "${test_namespace}" get externaleffects -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {
        name: .metadata.name,
        namespace: .metadata.namespace,
        uid: .metadata.uid,
        creationTimestamp: .metadata.creationTimestamp,
        labels: .metadata.labels
      },
      spec: {
        id: .spec.id,
        kind: .spec.kind,
        identityNamespace: .spec.identityNamespace,
        aggregateId: .spec.aggregateId,
        operationId: .spec.operationId,
        requestDigest: .spec.requestDigest
      },
      status: {
        state: .status.state,
        responseDigest: .status.responseDigest,
        leaseOwner: .status.leaseOwner,
        leaseExpiresAt: .status.leaseExpiresAt,
        attempts: .status.attempts,
        controllerEpochName: .status.controllerEpochName,
        controllerEpoch: .status.controllerEpoch,
        controllerEpochLeaseResourceVersion: .status.controllerEpochLeaseResourceVersion,
        version: .status.version,
        createdAt: .status.createdAt,
        updatedAt: .status.updatedAt
      }
    }]}' | redact >"${diagnostics_dir}/external-effects.json" || true

  kubectl -n "${test_namespace}" get runtimesessioncontrols -o json 2>/dev/null |
    jq '{items: [.items[] | {
      metadata: {name: .metadata.name, namespace: .metadata.namespace, uid: .metadata.uid, labels: .metadata.labels},
      spec: {
        sessionUid: .spec.sessionUid,
        requestDigest: .spec.requestDigest,
        owner: .spec.owner,
        runtimePoolRef: .spec.runtimePoolRef,
        runtimePoolUid: .spec.runtimePoolUid,
        runtimeProfileDigest: .spec.runtimeProfileDigest,
        profileDigestSchemaVersion: .spec.profileDigestSchemaVersion
      },
      status: {
        generation: .status.generation,
        lifecycle: .status.lifecycle,
        availability: .status.availability,
        mutationLeaseGeneration: .status.mutationLeaseGeneration,
        mutationLease: .status.mutationLease,
        blockedReason: .status.blockedReason,
        relatedPromptAttemptId: .status.relatedPromptAttemptId,
        relatedPublicationId: .status.relatedPublicationId,
        lineage: .status.lineage,
        controllerEpochName: .status.controllerEpochName,
        controllerEpoch: .status.controllerEpoch,
        controllerEpochLeaseResourceVersion: .status.controllerEpochLeaseResourceVersion,
        lastOperationId: .status.lastOperationId,
        lastOperationDigest: .status.lastOperationDigest,
        version: .status.version,
        createdAt: .status.createdAt,
        updatedAt: .status.updatedAt
      }
    }]}' | redact >"${diagnostics_dir}/runtime-session-controls.json" || true

  capture_redacted "${diagnostics_dir}/cluster-resources.txt" kubectl get nodes,pods -A -o wide
  capture_redacted "${diagnostics_dir}/namespace-resources.txt" kubectl -n "${test_namespace}" \
    get deployments,pods,services,jobs,agents,tasks,runtimepools,promptattempts,externaleffects,runtimesessioncontrols -o wide
  capture_redacted "${diagnostics_dir}/events.txt" kubectl -n "${test_namespace}" \
    get events --sort-by=.metadata.creationTimestamp
  capture_redacted "${diagnostics_dir}/controller.log" kubectl -n "${orka_namespace}" \
    logs deployment/"${orka_controller_deployment}" --all-containers=true --timestamps=true --tail=2000
  capture_redacted "${diagnostics_dir}/controller-describe.txt" kubectl -n "${orka_namespace}" \
    describe deployment/"${orka_controller_deployment}"

  local job_name
  while IFS= read -r job_name; do
    [[ -n "${job_name}" ]] || continue
    capture_redacted "${diagnostics_dir}/jobs/${job_name}.log" kubectl -n "${test_namespace}" \
      logs job/"${job_name}" --all-containers=true --timestamps=true --tail=2000
    capture_redacted "${diagnostics_dir}/jobs/${job_name}.describe.txt" kubectl -n "${test_namespace}" \
      describe job/"${job_name}"
  done < <(kubectl -n "${test_namespace}" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)

  local runtime_namespace runtime_pod
  while IFS= read -r runtime_namespace; do
    [[ -n "${runtime_namespace}" ]] || continue
    capture_redacted "${diagnostics_dir}/runtime/${runtime_namespace}-resources.txt" kubectl -n "${runtime_namespace}" \
      get deployments,pods,services,networkpolicies,poddisruptionbudgets -o wide
    capture_redacted "${diagnostics_dir}/runtime/${runtime_namespace}-events.txt" kubectl -n "${runtime_namespace}" \
      get events --sort-by=.metadata.creationTimestamp
    while IFS= read -r runtime_pod; do
      [[ -n "${runtime_pod}" ]] || continue
      capture_redacted "${diagnostics_dir}/runtime/${runtime_namespace}-${runtime_pod}.log" kubectl -n "${runtime_namespace}" \
        logs pod/"${runtime_pod}" --all-containers=true --timestamps=true --tail=2000
      capture_redacted "${diagnostics_dir}/runtime/${runtime_namespace}-${runtime_pod}.previous.log" kubectl -n "${runtime_namespace}" \
        logs pod/"${runtime_pod}" --all-containers=true --timestamps=true --tail=2000 --previous
      capture_redacted "${diagnostics_dir}/runtime/${runtime_namespace}-${runtime_pod}.describe.txt" kubectl -n "${runtime_namespace}" \
        describe pod/"${runtime_pod}"
    done < <(kubectl -n "${runtime_namespace}" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
  done < <(
    {
      printf '%s\n' "${orka_namespace}" "orka-runtimes"
      kubectl -n "${test_namespace}" get runtimepools \
        -o jsonpath='{range .items[*]}{.spec.runtimeNamespace}{"\n"}{end}' 2>/dev/null || true
    } | LC_ALL=C sort -u
  )

  if [[ -f "${api_forward_log}" ]]; then
    redact <"${api_forward_log}" >"${diagnostics_dir}/api-port-forward.log"
  fi
  log "Redacted SecurityScan diagnostics are ready at ${diagnostics_dir}"
}

on_exit() {
  local status="$1"
  local cleanup_status=0
  set +e
  if [[ "${status}" -ne 0 ]] &&
    [[ "$(kubectl config current-context 2>/dev/null || true)" == "kind-${kind_cluster}" ]]; then
    collect_security_scan_diagnostics || warn "SecurityScan diagnostic collection was incomplete"
  fi
  stop_api_forward
  if [[ "$(kubectl config current-context 2>/dev/null || true)" == "kind-${kind_cluster}" ]]; then
    kubectl -n "${test_namespace}" delete serviceaccount "${api_identity_name}" \
      --ignore-not-found=true --wait=false >/dev/null 2>&1 || cleanup_status=1
    kubectl -n "${test_namespace}" delete role "${api_identity_name}" \
      --ignore-not-found=true --wait=false >/dev/null 2>&1 || cleanup_status=1
    kubectl -n "${test_namespace}" delete rolebinding "${api_identity_name}" \
      --ignore-not-found=true --wait=false >/dev/null 2>&1 || cleanup_status=1
  fi
  restore_manager_kustomization || cleanup_status=1
  if [[ "${registry_cleanup_armed}" == "1" ]]; then
    orka_kind_registry_stop "${kind_cluster}" "${registry_owner}" || cleanup_status=1
  fi
  if [[ "${kind_cleanup_armed}" == "1" ]]; then
    kind delete cluster --name "${kind_cluster}" >/dev/null 2>&1 || cleanup_status=1
    if kind_cluster_exists; then
      cleanup_status=1
    fi
  fi
  rm -rf "${work_dir}" >/dev/null 2>&1 || cleanup_status=1
  if [[ "${kind_lock_held}" == "1" ]]; then
    rmdir "${kind_lock_dir}" >/dev/null 2>&1 || cleanup_status=1
    [[ ! -e "${kind_lock_dir}" ]] || cleanup_status=1
  fi
  if [[ "${status}" -ne 0 ]]; then
    log "Security scan e2e failed"
  fi
  status="$(security_scan_exit_status "${status}" "${cleanup_status}")"
  return "${status}"
}

security_scan_exit_status() {
  local original_status="$1"
  local cleanup_status="$2"
  if [[ "${original_status}" -ne 0 ]]; then
    printf '%s\n' "${original_status}"
  else
    printf '%s\n' "${cleanup_status}"
  fi
}

duration_to_seconds() {
  local value="$1"
  local rest="$1"
  local total=0
  local number unit amount

  if [[ "${value}" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "${value}"
    return
  fi

  while [[ -n "${rest}" ]]; do
    if [[ ! "${rest}" =~ ^([0-9]+)([hms])(.*)$ ]]; then
      die "unsupported duration ${value}; use digits with h, m, or s units"
    fi
    number="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    rest="${BASH_REMATCH[3]}"
    amount=$((10#${number}))
    case "${unit}" in
      h) total=$((total + amount * 3600)) ;;
      m) total=$((total + amount * 60)) ;;
      s) total=$((total + amount)) ;;
    esac
  done

  [[ "${total}" -gt 0 ]] || die "duration ${value} must be positive"
  printf '%s\n' "${total}"
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
    die "refusing to reuse existing Kind cluster ${kind_cluster}"
  fi

  if [[ -z "${ORKA_SECURITY_SCAN_KIND_CONFIG:-}" ]]; then
    write_default_kind_config
  fi
  [[ -f "${kind_config}" ]] || die "Kind config not found: ${kind_config}"

  log "Creating Kind cluster ${kind_cluster}"
  if ! run kind create cluster --name "${kind_cluster}" --config "${kind_config}" --kubeconfig "${kubeconfig_file}"; then
    return 1
  fi
  kind_cleanup_armed="1"
}

initialize_isolated_kubeconfig() {
  : >"${kubeconfig_file}"
  chmod 600 "${kubeconfig_file}"
  export KUBECONFIG="${kubeconfig_file}"
}

acquire_kind_cluster_lock() {
  kind_lock_dir="${TMPDIR:-/tmp}/orka-security-scan-kind-${kind_cluster}.lock"
  mkdir "${kind_lock_dir}" 2>/dev/null || die "another SecurityScan gate owns Kind cluster ${kind_cluster}"
  kind_lock_held="1"
}

stop_api_forward() {
  if [[ -z "${api_forward_pid}" ]]; then
    return 0
  fi
  if kill -0 "${api_forward_pid}" >/dev/null 2>&1; then
    kill "${api_forward_pid}" >/dev/null 2>&1 || true
    wait "${api_forward_pid}" >/dev/null 2>&1 || true
  fi
  api_forward_pid=""
}

api_health_ready() {
  curl --fail --silent --show-error --max-time 2 \
    "http://127.0.0.1:${api_local_port}/healthz" >/dev/null
}

start_api_forward() {
  stop_api_forward
  : >"${api_forward_log}"
  kubectl -n "${orka_namespace}" port-forward service/orka-api \
    "${api_local_port}:8080" >"${api_forward_log}" 2>&1 &
  api_forward_pid="$!"

  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if api_health_ready; then
      return 0
    fi
    if ! kill -0 "${api_forward_pid}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  cat "${api_forward_log}" | redact >&2
  die "controller API port-forward did not become ready"
}

create_api_identity() {
  log "Creating namespace-scoped API identity ${api_identity_name}"
  jq -n \
    --arg ns "${test_namespace}" \
    --arg name "${api_identity_name}" \
    '{apiVersion:"v1",kind:"ServiceAccount",metadata:{name:$name,namespace:$ns}}' |
    kubectl apply -f - >/dev/null
  jq -n \
    --arg ns "${test_namespace}" \
    --arg name "${api_identity_name}" \
    --arg scan "${scan_name}" \
    --arg badScan "${bad_scan_name}" '
    {
      apiVersion:"rbac.authorization.k8s.io/v1",
      kind:"Role",
      metadata:{name:$name,namespace:$ns},
      rules:[{
        apiGroups:["core.orka.ai"],
        resources:["agents","repositoryscans","tasks"],
        verbs:["get","list","watch"]
      },{
        apiGroups:["core.orka.ai"],
        resources:["repositoryscans/scans","repositoryscans/slices","repositoryscans/findings","repositoryscans/droppedfindings"],
        resourceNames:[$scan,$badScan],
        verbs:["list"]
      },{
        apiGroups:["core.orka.ai"],
        resources:["repositoryscans/threatmodel"],
        resourceNames:[$scan,$badScan],
        verbs:["get"]
      }]
    }' | kubectl apply -f - >/dev/null
  jq -n \
    --arg ns "${test_namespace}" \
    --arg name "${api_identity_name}" '
    {
      apiVersion:"rbac.authorization.k8s.io/v1",
      kind:"RoleBinding",
      metadata:{name:$name,namespace:$ns},
      subjects:[{kind:"ServiceAccount",name:$name,namespace:$ns}],
      roleRef:{apiGroup:"rbac.authorization.k8s.io",kind:"Role",name:$name}
    }' | kubectl apply -f - >/dev/null
  kubectl -n "${test_namespace}" create token "${api_identity_name}" --duration=2h >"${api_token_file}"
  chmod 600 "${api_token_file}"
  {
    printf 'Authorization: Bearer '
    tr -d '\r\n' <"${api_token_file}"
    printf '\n'
  } >"${api_auth_header_file}"
  chmod 600 "${api_auth_header_file}"
}

api_get() {
  local path="$1"
  local output_file="$2"
  local error_file="${output_file}.curl-error"
  local status rc

  set +e
  status="$(curl --silent --show-error --max-time 60 \
    --request GET \
    --header @"${api_auth_header_file}" \
    --output "${output_file}" \
    --write-out '%{http_code}' \
    "http://127.0.0.1:${api_local_port}${path}" 2>"${error_file}")"
  rc=$?
  set -e
  if [[ "${rc}" -ne 0 || ! "${status}" =~ ^2[0-9][0-9]$ ]]; then
    cat "${error_file}" | redact >&2
    cat "${output_file}" | redact >&2
    die "API GET ${path} failed with status ${status:-unavailable}"
  fi
}

build_fake_runtime() {
  local dockerfile="${work_dir}/security-scan-fake-runtime.Dockerfile"
  cat >"${dockerfile}" <<'DOCKERFILE'
# syntax=docker/dockerfile:1.7.1@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.0-bookworm@sha256:484ef6066fa69acb059fdfeda7ba2b8f7391f2ef6abc6f9b8411e669ebd56466 AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked go mod download
COPY . .
RUN set -eu; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/orka-acp-runtime ./cmd/orka-acp-runtime; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/orka-acp-exec-helper ./cmd/orka-acp-exec-helper; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -buildvcs=false -trimpath -ldflags='-s -w' -o /out/node ./scripts/fixtures/security-scan-fake-acp

FROM --platform=$TARGETPLATFORM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd
LABEL org.opencontainers.image.title="Orka SecurityScan deterministic ACP fixture" \
      io.orka.test.fixture="security-scan-harness-v2"
ENV HOME=/root \
    ORKA_ACP_PROVIDER=codex \
    PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN set -eu; \
    mkdir -p /sessions /opt/codex-acp/dist; \
    chmod 0711 /sessions
COPY --from=builder /out/orka-acp-runtime /usr/local/bin/orka-acp-runtime
COPY --from=builder /out/orka-acp-exec-helper /usr/local/bin/orka-acp-exec-helper
COPY --from=builder /out/node /usr/bin/node
WORKDIR /
USER 0:0
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/orka-acp-runtime"]
DOCKERFILE

  log "Building deterministic ACP v2 runtime ${fake_runtime_image}"
  run docker build -t "${fake_runtime_image}" -f "${dockerfile}" .
}

apply_authority_observer() {
  local image="$1"
  log "Deploying deterministic TTS and Tool authority observer ${authority_observer_name}"
  kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${authority_observer_name}
  namespace: ${test_namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${authority_observer_name}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${authority_observer_name}
    spec:
      containers:
        - name: observer
          image: ${image}
          imagePullPolicy: IfNotPresent
          command: ["/usr/bin/node"]
          env:
            - name: ORKA_SECURITY_SCAN_AUTHORITY_OBSERVER
              value: "1"
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet:
              path: /stats
              port: http
            initialDelaySeconds: 1
            periodSeconds: 1
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 250m
              memory: 64Mi
---
apiVersion: v1
kind: Service
metadata:
  name: ${authority_observer_name}
  namespace: ${test_namespace}
spec:
  selector:
    app.kubernetes.io/name: ${authority_observer_name}
  ports:
    - name: http
      port: 8080
      targetPort: http
YAML
  run kubectl -n "${test_namespace}" rollout status deployment/"${authority_observer_name}" --timeout=2m
}

patch_controller_images() {
  local token_source="${1:-incoming}"
  local rollout_id="${e2e_run_id}-${token_source}"
  local tts_endpoint="http://${authority_observer_name}.${test_namespace}.svc.cluster.local:8080/token"

  log "Configuring Orka controller worker images and ${token_source} brokered TTS authority"
  kubectl -n "${orka_namespace}" get deployment "${orka_controller_deployment}" -o json |
    jq \
      --arg generalImage "${general_worker_image}" \
      --arg rolloutID "${rollout_id}" \
      --arg outboundScope "${authority_scope}" \
      --arg tokenSource "${token_source}" \
      --arg ttsEndpoint "${tts_endpoint}" '
      def upsert_arg($name; $value):
        . as $args
        | if any($args[]?; startswith($name + "=")) then
            map(if startswith($name + "=") then $name + "=" + $value else . end)
          else
            $args + [$name + "=" + $value]
          end;
      .spec.template.metadata.annotations = ((.spec.template.metadata.annotations // {}) + {
        "orka.ai/security-scan-e2e-run": $rolloutID,
        "orka.ai/security-scan-e2e-tts-source": $tokenSource
      })
      |
      .spec.template.spec.containers |= map(
        if .name == "manager" then
          .imagePullPolicy = "IfNotPresent"
          | .args = ((.args // [])
            | upsert_arg("--general-worker-image"; $generalImage)
            | upsert_arg("--context-token-tts-endpoint"; $ttsEndpoint)
            | upsert_arg("--context-token-tts-token-source"; $tokenSource)
            | upsert_arg("--context-token-outbound-scope"; $outboundScope))
        else . end
      )
    ' | kubectl apply -f -

  run kubectl -n "${orka_namespace}" rollout status deployment/"${orka_controller_deployment}" --timeout=5m
}

reset_e2e_resources() {
  log "Resetting security scan e2e resources"
  run kubectl -n "${test_namespace}" delete repositoryscan "${scan_name}" "${bad_scan_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete task \
    -l "orka.ai/security-target=${scan_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete task \
    -l "orka.ai/security-target=${bad_scan_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete task "${authority_incoming_task}" "${authority_service_account_task}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete agent "${agent_name}" "${authority_agent_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete tool "${authority_tool_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
  run kubectl -n "${test_namespace}" delete outboundaccesspolicy "${authority_policy_name}" \
    --ignore-not-found=true --wait=true --timeout=2m
}

apply_authority_resources() {
  log "Creating deterministic ACP v2 custom Tool authority fixtures"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: OutboundAccessPolicy
metadata:
  name: ${authority_policy_name}
  namespace: ${test_namespace}
spec:
  gateway:
    serviceRef:
      name: ${authority_observer_name}
      port: 8080
    scheme: http
---
apiVersion: core.orka.ai/v1alpha1
kind: Tool
metadata:
  name: ${authority_tool_name}
  namespace: ${test_namespace}
spec:
  description: Deterministic ACP v2 transaction-authority probe
  brokeredToolClass: read
  parameters:
    type: object
    properties:
      probe:
        type: string
    required: ["probe"]
    additionalProperties: false
  http:
    url: https://example.com/tool
    method: POST
    timeout: 15s
    outboundAccessPolicyRef:
      name: ${authority_policy_name}
---
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: ${authority_agent_name}
  namespace: ${test_namespace}
spec:
  runtime:
    contractVersion: orka.harness.v2
    type: codex
    defaultMaxTurns: 1
    defaultAllowedTools:
      - Glob
      - Grep
      - Read
      - ${authority_tool_name}
    defaultAllowBash: false
  model:
    name: gpt-5.4
YAML
}

wait_authority_resources() {
  log "Waiting for ACP v2 authority policy and Tool readiness"
  run kubectl -n "${test_namespace}" wait \
    --for=condition=Accepted=true "outboundaccesspolicy/${authority_policy_name}" --timeout=2m
  run kubectl -n "${test_namespace}" wait \
    --for=condition=ResolvedRefs=true "outboundaccesspolicy/${authority_policy_name}" --timeout=2m
  run kubectl -n "${test_namespace}" wait \
    --for=jsonpath='{.status.available}'=true "tool/${authority_tool_name}" --timeout=2m
}

authority_observer_proxy_path() {
  printf '/api/v1/namespaces/%s/services/http:%s:8080/proxy' "${test_namespace}" "${authority_observer_name}"
}

authority_observer_stats() {
  kubectl get --raw="$(authority_observer_proxy_path)/stats"
}

reset_authority_observer() {
  printf '{}\n' | kubectl create --raw="$(authority_observer_proxy_path)/reset" -f - >/dev/null
  authority_observer_stats | jq -e '
    .ttsCalls == 0 and .toolCalls == 0 and
    .subjectTokenDigest == "" and .transactionTokenDigest == ""
  ' >/dev/null || die "authority observer did not reset to zero"
}

create_authority_task() {
  local task_name="$1"

  log "Creating transactionless ACP v2 authority Task/${task_name}"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Task
metadata:
  name: ${task_name}
  namespace: ${test_namespace}
  labels:
    orka.ai/security-scan-authority-e2e: "true"
spec:
  type: agent
  agentRef:
    name: ${authority_agent_name}
  prompt: orka-authority-probe
  workspace:
    intent: read
    gitRepo: ${target_repo}
    branch: ${target_branch}
    ref: ${target_ref}
  agentRuntime:
    maxTurns: 1
    allowedTools:
      - Glob
      - Grep
      - Read
      - ${authority_tool_name}
    allowBash: false
  timeout: 8m
YAML
}

wait_authority_task_phase() {
  local task_name="$1"
  local expected="$2"
  local timeout_seconds
  timeout_seconds="$(duration_to_seconds "${wait_timeout}")"
  local deadline=$((SECONDS + timeout_seconds))
  local phase=""

  log "Waiting for authority Task/${task_name} phase ${expected}"
  while ((SECONDS < deadline)); do
    phase="$(kubectl -n "${test_namespace}" get task "${task_name}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "${phase}" == "${expected}" ]]; then
      return 0
    fi
    case "${phase}" in
      Succeeded|Failed|Cancelled)
        die "authority Task/${task_name} entered phase ${phase} while waiting for ${expected}"
        ;;
    esac
    sleep 3
  done
  die "authority Task/${task_name} did not reach phase ${expected}; current phase ${phase:-<empty>}"
}

assert_transactionless_authority_task() {
  local task_name="$1"
  kubectl -n "${test_namespace}" get task "${task_name}" -o json | jq -e '
    .status.phase == "Succeeded" and
    .status.agentExecutionBinding.contractVersion == "orka.harness.v2" and
    .status.agentExecutionBinding.backend == "runtime-pool" and
    .status.execution.state == "Succeeded" and
    (.status.execution.runtimeSessionUID | length) > 0 and
    (.status.execution.promptID | length) > 0 and
    .status.resultRef.available == true
  ' >/dev/null || die "transactionless authority Task/${task_name} did not complete through ACP v2"
}

assert_transactionless_authority_stats() {
  local stats mode="$1"
  stats="$(authority_observer_stats)" || die "could not read authority observer stats"
  if ! jq -e '
    .ttsCalls == 0 and .toolCalls == 1 and
    .subjectTokenDigest == "" and .transactionTokenDigest == ""
  ' <<<"${stats}" >/dev/null; then
    printf '%s\n' "${stats}" >"${work_dir}/authority-${mode}-stats.json"
    die "transactionless ${mode} authority path did not reach the Tool exactly once without TTS or a transaction token"
  fi
}

run_acp_authority_gate() {
  apply_authority_resources
  wait_authority_resources
  reset_authority_observer
  create_authority_task "${authority_incoming_task}"
  wait_authority_task_phase "${authority_incoming_task}" "Succeeded"
  assert_transactionless_authority_task "${authority_incoming_task}"
  assert_transactionless_authority_stats incoming

  reset_authority_observer
  patch_controller_images serviceAccount

  create_authority_task "${authority_service_account_task}"
  wait_authority_task_phase "${authority_service_account_task}" "Succeeded"
  assert_transactionless_authority_task "${authority_service_account_task}"
  assert_transactionless_authority_stats service-account
  log "ACP v2 custom Tool transactionless TTS-isolation gate passed"
}

apply_agent() {
  log "Creating ACP Codex Agent fixture ${agent_name}"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: Agent
metadata:
  name: ${agent_name}
  namespace: ${test_namespace}
spec:
  runtime:
    contractVersion: orka.harness.v2
    type: codex
    defaultMaxTurns: 1
    defaultAllowBash: true
  model:
    name: gpt-5.4
YAML
}

apply_repository_scan() {
  local name="$1"
  log "Creating RepositoryScan ${name} for ${target_repo}"
  kubectl apply -f - <<YAML
apiVersion: core.orka.ai/v1alpha1
kind: RepositoryScan
metadata:
  name: ${name}
  namespace: ${test_namespace}
spec:
  provider: github
  repoURL: ${target_repo}
  owner: ${target_owner}
  repository: ${target_repository}
  branch: ${target_branch}
  ref: ${target_ref}
  validationMode: "off"
  maxFindingsPerRun: 20
  analysisAgentRef:
    name: ${agent_name}
YAML
}

wait_repo_phase() {
  local name="$1"
  local expected="$2"
  local timeout_seconds
  timeout_seconds="$(duration_to_seconds "${wait_timeout}")"
  local deadline=$((SECONDS + timeout_seconds))
  local phase message

  log "Waiting for RepositoryScan/${name} phase ${expected}"
  while (( SECONDS < deadline )); do
    phase="$(kubectl -n "${test_namespace}" get repositoryscan "${name}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "${phase}" == "${expected}" ]]; then
      return 0
    fi
    case "${phase}" in
      Ready|Error|Suspended)
        message="$(
          kubectl -n "${test_namespace}" get repositoryscan "${name}" -o json 2>/dev/null |
            jq -r '[.status.conditions[]? | select(.type == "Ready")][-1].message // empty' |
            redact
        )"
        die "RepositoryScan/${name} entered terminal phase ${phase} while waiting for ${expected}${message:+: ${message}}"
        ;;
    esac
    sleep 5
  done
  die "RepositoryScan/${name} did not reach phase ${expected}; current phase ${phase:-<empty>}"
}

collect_scan_evidence() {
  local name="$1"
  local prefix="$2"
  run kubectl -n "${test_namespace}" get repositoryscan "${name}" -o json >"${prefix}-scan.json"
  run kubectl -n "${test_namespace}" get tasks -l "orka.ai/security-target=${name}" -o json >"${prefix}-tasks.json"
  api_get "/api/v1/security/repositories/${name}/scans?namespace=${test_namespace}&limit=100" "${prefix}-runs.json"
  api_get "/api/v1/security/repositories/${name}/slices?namespace=${test_namespace}&limit=1000" "${prefix}-slices.json"
  api_get "/api/v1/security/repositories/${name}/findings?namespace=${test_namespace}&limit=1000" "${prefix}-findings.json"
  api_get "/api/v1/security/repositories/${name}/dropped-findings?namespace=${test_namespace}&limit=1000" "${prefix}-dropped.json"
}

assert_positive_scan_snapshot() {
  local prefix="$1"
  if ! jq -e \
    --arg agent "${agent_name}" \
    --arg branch "${target_branch}" \
    --arg ref "${target_ref}" \
    --arg repo "${target_repo}" \
    --arg scanName "${scan_name}" \
    --slurpfile tasks "${prefix}-tasks.json" \
    --slurpfile runs "${prefix}-runs.json" \
    --slurpfile slices "${prefix}-slices.json" \
    --slurpfile findings "${prefix}-findings.json" \
    --slurpfile dropped "${prefix}-dropped.json" '
    .status.lastScanID as $scanID |
    ($tasks[0].items | map(select(.metadata.labels["orka.ai/security-scan-id"] == $scanID))) as $runTasks |
    ($runTasks | map(select(.metadata.labels["orka.ai/security-stage"] == "threat-model"))) as $threatTasks |
    ($runTasks | map(select(.metadata.labels["orka.ai/security-stage"] == "mapper"))) as $mapperTasks |
    ($runTasks | map(select(.metadata.labels["orka.ai/security-stage"] == "review"))) as $reviewTasks |
    ($runs[0].items | map(select(.id == $scanID))) as $runItems |
    ($slices[0].items | map(select(.lastScanRunID == $scanID))) as $sliceItems |
    ($findings[0].items | map(select(.scanRunID == $scanID))) as $findingItems |
    ($dropped[0].items | map(select(.scanRunID == $scanID))) as $dropItems |
    .status.phase == "Ready" and
    (.status.lastScanTaskName | length > 0) and
    (.status.lastScanAt | length > 0) and
    (.status.lastSuccessfulScanAt | length > 0) and
    .status.lastProcessedCommit == $ref and
    .status.lastObservedHeadSHA == $ref and
    (.status.threatModelVersion // 0) > 0 and
    any(.status.conditions[]?; .type == "Ready" and .status == "True" and .reason == "ScanSucceeded") and
    ($runItems | length) == 1 and
    ($runItems[0] as $run |
      $run.repositoryScan == $scanName and
      $run.taskName == .status.lastScanTaskName and
      $run.mode == "initial" and
      $run.phase == "succeeded" and
      ($run.completedAt | length > 0) and
      ($run.policyDigest | test("^sha256:[0-9a-f]{64}$")) and
      ($run.idempotencyKey | test("^scanidem:[0-9a-f]{64}$")) and
      $run.sliceCount > 0 and
      $run.reviewedSliceCount == $run.sliceCount and
      $run.skippedSliceCount == 0 and
      $run.acceptedFindings == ($findingItems | length) and
      $run.droppedFindings == ($dropItems | length) and
      $run.acceptedFindings > 0 and
      $run.droppedFindings > 0 and
      ($threatTasks | length) == 1 and
      ($mapperTasks | length) == 1 and
      ($reviewTasks | length) == $run.sliceCount
    ) and
    all($runTasks[];
      .metadata.labels["orka.ai/security-target"] == $scanName and
      .metadata.labels["orka.ai/security-scan-mode"] == "initial" and
      any(.metadata.ownerReferences[]?; .kind == "RepositoryScan" and .name == $scanName and .controller == true) and
      .status.phase == "Succeeded"
    ) and
    all(($threatTasks + $reviewTasks)[];
      .spec.type == "agent" and
      .spec.agentRef.name == $agent and
      ((.spec.env // []) | length) == 0 and
      .spec.workspace.intent == "read" and
      .spec.workspace.gitRepo == $repo and
      .spec.workspace.branch == $branch and
      .spec.workspace.ref == $ref and
      .status.resultRef.available == true and
      .status.agentExecutionBinding.contractVersion == "orka.harness.v2" and
      .status.agentExecutionBinding.backend == "runtime-pool" and
      .status.execution.state == "Succeeded" and
      .status.execution.outcome == "Succeeded" and
      .status.delivery.state == "ReadValidated" and
      .status.delivery.outcome == "ReadValidated"
    ) and
    ($mapperTasks[0].spec.type == "container") and
    ($mapperTasks[0].spec.command == ["--security-mapper"]) and
    (($mapperTasks[0].spec.env // []) | map(.name) | index("ORKA_SECURITY_REPOSITORY_SCAN") != null) and
    ($sliceItems | length) == $runItems[0].sliceCount and
    all($sliceItems[]; .status == "reviewed" and (.reviewContextHash | test("^sha256:[0-9a-f]{64}$"))) and
    any($findingItems[]; .category == "authorization" and .severity == "high" and (.evidence | length) > 0) and
    all($findingItems[]; .repositoryScan == $scanName and .scanRunID == $scanID and (.id | length) > 0) and
    any($dropItems[]; .layer == "validation" and (.reason | contains("review context"))) and
    all($dropItems[];
      .repositoryScan == $scanName and .scanRunID == $scanID and
      (.id | test("^drop_")) and (.taskName | length) > 0 and
      (.sliceID | length) > 0 and (.reason | length) > 0
    ) and
    (.status.findingCounts.total // 0) == ($findingItems | length) and
    (.status.findingCounts.high // 0) == ($findingItems | map(select(.severity == "high")) | length) and
    (.status.findingCounts.critical // 0) == ($findingItems | map(select(.severity == "critical")) | length) and
    (.status.findingCounts.medium // 0) == ($findingItems | map(select(.severity == "medium")) | length) and
    (.status.findingCounts.low // 0) == ($findingItems | map(select(.severity == "low")) | length)
  ' "${prefix}-scan.json" >/dev/null; then
    die "positive SecurityScan snapshot did not satisfy the harness-v2 ingestion contract"
  fi
}

positive_scan_idempotency_snapshot() {
  local prefix="$1"
  jq -S \
    --slurpfile tasks "${prefix}-tasks.json" \
    --slurpfile runs "${prefix}-runs.json" \
    --slurpfile slices "${prefix}-slices.json" \
    --slurpfile findings "${prefix}-findings.json" \
    --slurpfile dropped "${prefix}-dropped.json" '
    .status.lastScanID as $scanID |
    {
      scanID:$scanID,
      lastScanTaskName:.status.lastScanTaskName,
      threatModelVersion:.status.threatModelVersion,
      tasks:($tasks[0].items | map(select(.metadata.labels["orka.ai/security-scan-id"] == $scanID) | {
        name:.metadata.name, stage:.metadata.labels["orka.ai/security-stage"], slice:.metadata.labels["orka.ai/security-slice-id"]
      }) | sort_by(.name)),
      run:($runs[0].items | map(select(.id == $scanID) | {
        id,phase,taskName,sliceCount,reviewedSliceCount,skippedSliceCount,acceptedFindings,droppedFindings,policyDigest,idempotencyKey
      })),
      slices:($slices[0].items | map(select(.lastScanRunID == $scanID) | {id,status,lastScanRunID,reviewContextHash}) | sort_by(.id)),
      findings:($findings[0].items | map(select(.scanRunID == $scanID) | .id) | sort),
      dropped:($dropped[0].items | map(select(.scanRunID == $scanID) | .id) | sort)
    }
  ' "${prefix}-scan.json"
}

assert_malformed_scan_snapshot() {
  local prefix="$1"
  local expected="threat model terminal result is missing or invalid: security result scanId does not match task run"
  if ! jq -e \
    --arg agent "${agent_name}" \
    --arg expected "${expected}" \
    --arg scanName "${bad_scan_name}" \
    --slurpfile tasks "${prefix}-tasks.json" \
    --slurpfile runs "${prefix}-runs.json" \
    --slurpfile slices "${prefix}-slices.json" \
    --slurpfile findings "${prefix}-findings.json" \
    --slurpfile dropped "${prefix}-dropped.json" '
    .status.lastScanID as $scanID |
    ($tasks[0].items | map(select(.metadata.labels["orka.ai/security-scan-id"] == $scanID))) as $runTasks |
    ($runs[0].items | map(select(.id == $scanID))) as $runItems |
    .status.phase == "Error" and
    (.status.lastScanTaskName | length > 0) and
    (.status.lastScanAt | length > 0) and
    (.status | has("lastSuccessfulScanAt") | not) and
    (.status.threatModelVersion // 0) == 0 and
    (.status.findingCounts.total // 0) == 0 and
    any(.status.conditions[]?;
      .type == "Ready" and .status == "False" and .reason == "ScanFailed" and (.message | startswith($expected))
    ) and
    ($runTasks | length) == 1 and
    ($runTasks[0] as $task |
      $task.metadata.labels["orka.ai/security-stage"] == "threat-model" and
      $task.spec.type == "agent" and $task.spec.agentRef.name == $agent and
      (($task.spec.env // []) | length) == 0 and
      $task.status.phase == "Succeeded" and $task.status.resultRef.available == true and
      $task.status.agentExecutionBinding.contractVersion == "orka.harness.v2" and
      $task.status.agentExecutionBinding.backend == "runtime-pool" and
      $task.status.execution.outcome == "Succeeded" and
      $task.status.delivery.outcome == "ReadValidated"
    ) and
    ($runItems | length) == 1 and
    $runItems[0].phase == "failed" and
    ($runItems[0].errorMessage | startswith($expected)) and
    $runItems[0].sliceCount == 0 and $runItems[0].acceptedFindings == 0 and $runItems[0].droppedFindings == 0 and
    ($slices[0].items | length) == 0 and
    ($findings[0].items | length) == 0 and
    ($dropped[0].items | length) == 0
  ' "${prefix}-scan.json" >/dev/null; then
    die "malformed SecurityScan result did not fail closed after successful ACP execution"
  fi
}

assert_positive_scan_gate() {
  local before="${work_dir}/${scan_name}-before"
  local after="${work_dir}/${scan_name}-after"
  collect_scan_evidence "${scan_name}" "${before}"
  api_get "/api/v1/security/repositories/${scan_name}/threat-model?namespace=${test_namespace}" "${before}-threat-model.json"
  assert_positive_scan_snapshot "${before}"
  jq -e --slurpfile scan "${before}-scan.json" '
    .source == "generated" and .generatedByScan == $scan[0].status.lastScanID and
    .version == $scan[0].status.threatModelVersion and (.content | startswith("#"))
  ' "${before}-threat-model.json" >/dev/null || die "generated threat model was not durably ingested"
  positive_scan_idempotency_snapshot "${before}" >"${work_dir}/positive-before.json"

  run kubectl -n "${test_namespace}" annotate repositoryscan "${scan_name}" \
    "orka.ai/security-scan-e2e-reconcile=${e2e_run_id}" --overwrite
  wait_repo_phase "${scan_name}" "Ready"
  collect_scan_evidence "${scan_name}" "${after}"
  assert_positive_scan_snapshot "${after}"
  positive_scan_idempotency_snapshot "${after}" >"${work_dir}/positive-after.json"
  cmp -s "${work_dir}/positive-before.json" "${work_dir}/positive-after.json" ||
    die "RepositoryScan reconciliation changed durable run, task, finding, slice, or drop identities"
}

assert_malformed_result_gate() {
  local prefix="${work_dir}/${bad_scan_name}"
  collect_scan_evidence "${bad_scan_name}" "${prefix}"
  assert_malformed_scan_snapshot "${prefix}"
}

main() {
  require_cmd make
  require_cmd go
  require_cmd docker
  require_cmd kind
  require_cmd kubectl
  require_cmd jq
  require_cmd openssl
  require_cmd curl
  require_cmd cmp

  [[ "${orka_namespace}" == "orka-system" ]] || die "ORKA_NAMESPACE must be orka-system for the canonical make deploy path"
  [[ "${test_namespace}" == "${orka_namespace}" ]] || die "ORKA_SECURITY_SCAN_E2E_NAMESPACE must match ORKA_NAMESPACE for an isolated controller"
  [[ "${keep_cluster}" == "0" ]] || die "KEEP_CLUSTER is forbidden for the isolated SecurityScan gate"
  [[ "${kind_cluster}" =~ ^[a-z0-9][a-z0-9.-]{0,62}$ ]] || die "KIND_CLUSTER must be a valid lowercase Kind cluster name of at most 63 characters"

  cd "${repo_root}"
  initialize_isolated_kubeconfig
  [[ -f "${manager_kustomization}" ]] || die "missing ${manager_kustomization}"
  cp "${manager_kustomization}" "${manager_kustomization_backup}"

  acquire_kind_cluster_lock
  setup_kind_cluster
  run kubectl config use-context "kind-${kind_cluster}"
  log "Installing current Orka CRDs into the test cluster"
  run make install
  log "Creating the Vekil namespace required by the production ingress policy"
  kubectl create namespace vekil-system --dry-run=client -o yaml | kubectl apply -f -
  registry_cleanup_armed="1"
  orka_kind_registry_start "${kind_cluster}" "${registry_owner}"

  log "Building manager image ${manager_image}"
  run make docker-build IMG="${manager_image}"
  log "Building workspace publisher image ${publisher_image}"
  run make docker-build-workspace-publisher WORKSPACE_PUBLISHER_IMG="${publisher_image}"

  log "Building general worker image ${general_worker_image}"
  run docker build -t "${general_worker_image}" -f workers/general/Dockerfile .
  build_fake_runtime

  log "Loading images into Kind cluster ${kind_cluster}"
  run kind load docker-image "${manager_image}" --name "${kind_cluster}"
  run kind load docker-image "${general_worker_image}" --name "${kind_cluster}"

  local manager_ref publisher_ref fake_runtime_ref
  manager_ref="$(orka_kind_registry_push "${manager_image}" "orka/controller")"
  publisher_ref="$(orka_kind_registry_push "${publisher_image}" "orka/workspace-publisher")"
  fake_runtime_ref="$(orka_kind_registry_push "${fake_runtime_image}" "orka/acp-security-fixture")"

  log "Bootstrapping test-only admission TLS"
  orka_e2e_bootstrap_admission_tls

  log "Deploying Orka manager with the deterministic digest-pinned ACP v2 fixture"
  local placeholder_digest
  placeholder_digest="sha256:$(printf '0%.0s' {1..64})"
  run make deploy \
    IMG="${manager_ref}" \
    WORKSPACE_PUBLISHER_IMG="${publisher_ref}" \
    ACP_CODEX_RUNTIME_IMG="${fake_runtime_ref}" \
    ACP_CLAUDE_RUNTIME_IMG="example.invalid/orka/acp-claude@${placeholder_digest}" \
    ACP_COPILOT_RUNTIME_IMG="example.invalid/orka/acp-copilot@${placeholder_digest}" \
    ACP_OPENCODE_RUNTIME_IMG="example.invalid/orka/acp-opencode@${placeholder_digest}"
  run kubectl wait --for=condition=Established crd/repositoryscans.core.orka.ai --timeout=60s
  run kubectl -n "${orka_namespace}" rollout status deployment/"${orka_controller_deployment}" --timeout=5m
  apply_authority_observer "${fake_runtime_ref}"
  patch_controller_images incoming
  reset_e2e_resources
  run_acp_authority_gate

  create_api_identity
  start_api_forward
  apply_agent

  apply_repository_scan "${scan_name}"
  wait_repo_phase "${scan_name}" "Ready"
  log "Verifying positive harness-v2 execution and durable ResultStore ingestion"
  assert_positive_scan_gate

  apply_repository_scan "${bad_scan_name}"
  wait_repo_phase "${bad_scan_name}" "Error"
  log "Verifying malformed terminal results fail closed after successful ACP execution"
  assert_malformed_result_gate
  log "SecurityScan and ACP v2 custom Tool authority gates passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  trap 'status=$?; set +e; on_exit "${status}"; exit $?' EXIT
  main "$@"
fi
