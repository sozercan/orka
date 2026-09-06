#!/usr/bin/env bash
# shellcheck disable=SC2016 # acp_report_update arguments are jq programs.
# Shared, source-only bootstrap for the live ACP runtime validator on Kind.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "error: source scripts/lib/live-acp-runtime-kind-bootstrap.sh; do not execute it directly" >&2
  exit 2
fi

live_acp_kind_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/e2e-admission-tls.sh
. "${live_acp_kind_lib_dir}/e2e-admission-tls.sh"
# shellcheck source=scripts/lib/live-acp-release-report.sh
. "${live_acp_kind_lib_dir}/live-acp-release-report.sh"
unset live_acp_kind_lib_dir

# Internal port-forward state is process-local. Reset inherited values so a
# caller-controlled environment can never make cleanup signal or unlink an
# unrelated resource.
LIVE_ACP_VEKIL_PORT_FORWARD_PID=""
LIVE_ACP_VEKIL_PORT_FORWARD_LOG=""
LIVE_ACP_VEKIL_BASE_URL=""
LIVE_ACP_VEKIL_PORT_FORWARD_OWNED=0

live_acp_kind_log() {
  printf '==> %s\n' "$*" >&2
}

live_acp_kind_die() {
  printf 'error: %s\n' "$*" >&2
  return 1
}

live_acp_kind_enabled() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

live_acp_kind_require_cmd() {
  command -v "$1" >/dev/null 2>&1 || live_acp_kind_die "missing required command: $1"
}

live_acp_kind_run() {
  printf '+ ' >&2
  printf '%q ' "$@" >&2
  printf '\n' >&2
  "$@"
}

live_acp_kind_preflight() {
  local command
  for command in curl docker git go jq kind kubectl make openssl python3; do
    live_acp_kind_require_cmd "${command}" || return 1
  done
  if live_acp_kind_enabled "${RELEASE_GATE:-0}"; then
    live_acp_kind_require_cmd gh || return 1
    local token_var
    for token_var in \
      ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN \
      ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN \
      ACP_E2E_WRITE_CREDENTIAL_TOKEN \
      ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN; do
      if [[ -z "${!token_var:-}" ]]; then
        acp_report_update '.failure = {reason:"missing_credential", credential:$name}' --arg name "${token_var}" || return 1
        live_acp_kind_die "${token_var} is required when RELEASE_GATE=1"
        return 1
      fi
    done
    local left right
    local -a role_tokens=("${ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN}" "${ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN}"
      "${ACP_E2E_WRITE_CREDENTIAL_TOKEN}" "${ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN}")
    for ((left=0; left<4; left++)); do
      for ((right=left+1; right<4; right++)); do
        [[ "${role_tokens[$left]}" != "${role_tokens[$right]}" ]] || \
          live_acp_kind_die "release-gate credentials must use four distinct values" || return 1
      done
    done
  fi
  [[ -x "${LIVE_ACP_KINDCTL_BIN}" ]] || live_acp_kind_die "kindctl is not executable: ${LIVE_ACP_KINDCTL_BIN}" || return 1
  [[ -x "${LIVE_ACP_VEKIL_DEPLOY_SCRIPT}" ]] || live_acp_kind_die "Vekil deploy script is not executable: ${LIVE_ACP_VEKIL_DEPLOY_SCRIPT}" || return 1
  [[ -x "${LIVE_ACP_VALIDATOR_SCRIPT}" ]] || live_acp_kind_die "live ACP validator is not executable: ${LIVE_ACP_VALIDATOR_SCRIPT}" || return 1
  if [[ -n "${LIVE_ACP_VEKIL_LOCAL_IMAGE:-}" ]]; then
    # A locally built Vekil is published through the run's own registry and
    # digest-pinned there, so development builds stay immutable end to end.
    docker image inspect "${LIVE_ACP_VEKIL_LOCAL_IMAGE}" >/dev/null 2>&1 || \
      live_acp_kind_die "LIVE_ACP_VEKIL_LOCAL_IMAGE is not a local Docker image: ${LIVE_ACP_VEKIL_LOCAL_IMAGE}" || return 1
  else
    [[ "${LIVE_ACP_VEKIL_IMAGE}" =~ @sha256:[0-9a-f]{64}$ ]] || \
      live_acp_kind_die "LIVE_ACP_VEKIL_IMAGE must be digest-pinned" || return 1
  fi
  [[ -n "${COPILOT_GITHUB_TOKEN:-}" ]] || \
    live_acp_kind_die "COPILOT_GITHUB_TOKEN is required; this bootstrap is noninteractive and never starts device-code login" || return 1
  [[ "${ACP_E2E_OPENCODE_CONTEXT_WINDOW:-}" =~ ^[1-9][0-9]*$ ]] || \
    live_acp_kind_die "ACP_E2E_OPENCODE_CONTEXT_WINDOW must be a positive integer" || return 1
  [[ "${ACP_E2E_OPENCODE_MAX_TOKENS:-}" =~ ^[1-9][0-9]*$ ]] || \
    live_acp_kind_die "ACP_E2E_OPENCODE_MAX_TOKENS must be a positive integer" || return 1
  (( ACP_E2E_OPENCODE_CONTEXT_WINDOW > ACP_E2E_OPENCODE_MAX_TOKENS )) || \
    live_acp_kind_die "ACP_E2E_OPENCODE_CONTEXT_WINDOW must exceed ACP_E2E_OPENCODE_MAX_TOKENS" || return 1
  docker info >/dev/null 2>&1 || live_acp_kind_die "Docker daemon is not reachable" || return 1
}

live_acp_kind_create_cluster() {
  local -a args=(create --tag "${LIVE_ACP_KIND_TAG}")
  if [[ -n "${LIVE_ACP_KIND_CONFIG:-}" ]]; then
    [[ -f "${LIVE_ACP_KIND_CONFIG}" ]] || live_acp_kind_die "Kind config not found: ${LIVE_ACP_KIND_CONFIG}" || return 1
    args+=(--config "${LIVE_ACP_KIND_CONFIG}")
  fi

  # Arm exact-tag deletion before create: Kind can leave a partial cluster even
  # when its create command returns non-zero.
  LIVE_ACP_KIND_CREATED=1
  acp_report_update '.cluster = {tag:$tag, creationAttempted:true, registryStarted:false}' \
    --arg tag "${LIVE_ACP_KIND_TAG}" || return 1
  live_acp_kind_run "${LIVE_ACP_KINDCTL_BIN}" "${args[@]}" || return $?
  LIVE_ACP_KUBECONFIG="$("${LIVE_ACP_KINDCTL_BIN}" path --tag "${LIVE_ACP_KIND_TAG}")"
  export KUBECONFIG="${LIVE_ACP_KUBECONFIG}"
  LIVE_ACP_CONTEXT="$(kubectl config current-context)"
  [[ "${LIVE_ACP_CONTEXT}" == kind-* ]] || live_acp_kind_die "scoped context is not a Kind context: ${LIVE_ACP_CONTEXT}" || return 1
  LIVE_ACP_KIND_CLUSTER="${LIVE_ACP_CONTEXT#kind-}"
  export LIVE_ACP_CONTEXT LIVE_ACP_KIND_CLUSTER LIVE_ACP_KUBECONFIG
  acp_report_update '.cluster.name = $name' --arg name "${LIVE_ACP_KIND_CLUSTER}"
}

live_acp_kind_start_registry() {
  # shellcheck source=scripts/lib/kind-local-registry.sh
  . "${LIVE_ACP_REPO_ROOT}/scripts/lib/kind-local-registry.sh"

  LIVE_ACP_REGISTRY_STARTED=1
  acp_report_update '.cluster.registryStarted = true'
  orka_kind_registry_start "${LIVE_ACP_KIND_CLUSTER}"

  if [[ -n "${LIVE_ACP_VEKIL_LOCAL_IMAGE:-}" ]]; then
    # Publish the development Vekil before the Vekil deploy step so the
    # deployment references the digest-pinned copy in the run's registry.
    LIVE_ACP_VEKIL_IMAGE="$(orka_kind_registry_push "${LIVE_ACP_VEKIL_LOCAL_IMAGE}" vekil/vekil)"
    export LIVE_ACP_VEKIL_IMAGE
  fi
}

live_acp_kind_build_and_publish_images() {
  live_acp_kind_run make -C "${LIVE_ACP_REPO_ROOT}" \
    IMG="${LIVE_ACP_CONTROLLER_IMAGE}" \
    ACP_CODEX_RUNTIME_IMG="${LIVE_ACP_CODEX_IMAGE}" \
    ACP_CLAUDE_RUNTIME_IMG="${LIVE_ACP_CLAUDE_IMAGE}" \
    ACP_COPILOT_RUNTIME_IMG="${LIVE_ACP_COPILOT_IMAGE}" \
    ACP_OPENCODE_RUNTIME_IMG="${LIVE_ACP_OPENCODE_IMAGE}" \
    WORKSPACE_PUBLISHER_IMG="${LIVE_ACP_PUBLISHER_IMAGE}" \
    GENERAL_WORKER_IMG="${LIVE_ACP_GENERAL_WORKER_IMAGE}" \
    docker-build docker-build-acp-codex-runtime docker-build-acp-claude-runtime \
    docker-build-acp-copilot-runtime docker-build-acp-opencode-runtime \
    docker-build-workspace-publisher docker-build-general-worker

  LIVE_ACP_CONTROLLER_REF="$(orka_kind_registry_push "${LIVE_ACP_CONTROLLER_IMAGE}" orka/controller)"
  LIVE_ACP_CODEX_REF="$(orka_kind_registry_push "${LIVE_ACP_CODEX_IMAGE}" orka/acp-codex-runtime)"
  LIVE_ACP_CLAUDE_REF="$(orka_kind_registry_push "${LIVE_ACP_CLAUDE_IMAGE}" orka/acp-claude-runtime)"
  LIVE_ACP_COPILOT_REF="$(orka_kind_registry_push "${LIVE_ACP_COPILOT_IMAGE}" orka/acp-copilot-runtime)"
  LIVE_ACP_OPENCODE_REF="$(orka_kind_registry_push "${LIVE_ACP_OPENCODE_IMAGE}" orka/acp-opencode-runtime)"
  LIVE_ACP_PUBLISHER_REF="$(orka_kind_registry_push "${LIVE_ACP_PUBLISHER_IMAGE}" orka/workspace-publisher)"
  LIVE_ACP_GENERAL_WORKER_REF="$(orka_kind_registry_push "${LIVE_ACP_GENERAL_WORKER_IMAGE}" orka/general-worker)"
  export LIVE_ACP_CONTROLLER_REF LIVE_ACP_CODEX_REF LIVE_ACP_CLAUDE_REF LIVE_ACP_COPILOT_REF
  export LIVE_ACP_OPENCODE_REF LIVE_ACP_PUBLISHER_REF LIVE_ACP_GENERAL_WORKER_REF
  acp_report_update '.builtImages = {controller:$controller, publisher:$publisher,
    codex:$codex, claude:$claude, copilot:$copilot, opencode:$opencode}' \
    --arg controller "${LIVE_ACP_CONTROLLER_REF}" --arg publisher "${LIVE_ACP_PUBLISHER_REF}" \
    --arg codex "${LIVE_ACP_CODEX_REF}" --arg claude "${LIVE_ACP_CLAUDE_REF}" \
    --arg copilot "${LIVE_ACP_COPILOT_REF}" --arg opencode "${LIVE_ACP_OPENCODE_REF}"
}

live_acp_kind_catalog_model_supports_endpoint() {
  local models_file="$1"
  local model="$2"
  local endpoint="$3"
  jq -e --arg model "${model}" --arg endpoint "${endpoint}" '
    def model_entries:
      (if (.data | type) == "array" then .data else [] end)
      + (if (.models | type) == "array" then .models else [] end);
    def model_id:
      if type == "string" then .
      elif type == "object" then (.id // .slug // .name // "")
      else ""
      end;
    [
      model_entries[]
      | select(model_id == $model)
      | select(type == "object" and (.supported_endpoints | type) == "array")
      | .supported_endpoints[]
      | strings
      | select(ascii_downcase == ($endpoint | ascii_downcase))
    ]
    | length > 0
  ' "${models_file}" >/dev/null
}

live_acp_kind_require_model_endpoint() {
  local models_file="$1"
  local provider="$2"
  local model="$3"
  local endpoint="$4"
  live_acp_kind_catalog_model_supports_endpoint "${models_file}" "${model}" "${endpoint}" || \
    live_acp_kind_die "Vekil model ${model} for ${provider} does not advertise required endpoint ${endpoint}"
}

live_acp_kind_validate_vekil_catalog() {
  local models_file="$1"
  local codex_model="${ACP_E2E_CODEX_MODEL:-gpt-5.4}"
  local claude_model="${ACP_E2E_CLAUDE_MODEL:-claude-sonnet-4.6}"
  local copilot_model="${ACP_E2E_COPILOT_MODEL:-gpt-5.3-codex}"
  local opencode_model="${ACP_E2E_OPENCODE_MODEL:-${ACP_E2E_CODEX_MODEL:-gpt-5.4}}"
  opencode_model="${opencode_model#*/}"

  live_acp_kind_require_model_endpoint "${models_file}" Codex "${codex_model}" /responses || return 1
  if ! live_acp_kind_catalog_model_supports_endpoint "${models_file}" "${opencode_model}" /chat/completions && \
      ! live_acp_kind_catalog_model_supports_endpoint "${models_file}" "${opencode_model}" /responses; then
    live_acp_kind_die \
      "Vekil model ${opencode_model} for OpenCode does not advertise required endpoint /chat/completions or compatible /responses"
    return 1
  fi
  live_acp_kind_require_model_endpoint "${models_file}" Claude "${claude_model}" /v1/messages || return 1
  live_acp_kind_require_model_endpoint "${models_file}" Copilot "${copilot_model}" /responses || return 1
}

live_acp_kind_unused_local_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
}

live_acp_kind_stop_vekil_port_forward() {
  local pid="${LIVE_ACP_VEKIL_PORT_FORWARD_PID:-}"
  local log_file="${LIVE_ACP_VEKIL_PORT_FORWARD_LOG:-}"
  local deadline owned_pid=0 running_child=0

  if [[ "${LIVE_ACP_VEKIL_PORT_FORWARD_OWNED:-0}" == "1" && "${pid}" =~ ^[1-9][0-9]*$ ]] && \
      (( pid > 1 )); then
    owned_pid=1
    if jobs -pr | grep -Fx -- "${pid}" >/dev/null 2>&1; then
      running_child=1
    fi
  fi
  if [[ "${running_child}" == "1" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    deadline=$((SECONDS + 5))
    while kill -0 "${pid}" >/dev/null 2>&1 && (( SECONDS < deadline )); do
      sleep 0.1
    done
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -KILL "${pid}" >/dev/null 2>&1 || true
    fi
    wait "${pid}" >/dev/null 2>&1 || true
  elif [[ "${owned_pid}" == "1" ]]; then
    wait "${pid}" >/dev/null 2>&1 || true
  fi

  case "${log_file}" in
    "${LIVE_ACP_SECRET_DIR}"/vekil-port-forward.*) rm -f "${log_file}" ;;
  esac
  LIVE_ACP_VEKIL_PORT_FORWARD_PID=""
  LIVE_ACP_VEKIL_PORT_FORWARD_LOG=""
  LIVE_ACP_VEKIL_BASE_URL=""
  LIVE_ACP_VEKIL_PORT_FORWARD_OWNED=0
}

live_acp_kind_start_vekil_port_forward() {
  local port deadline

  [[ -z "${LIVE_ACP_VEKIL_PORT_FORWARD_PID:-}" ]] || \
    live_acp_kind_die "Vekil port-forward is already running" || return 1
  port="$(live_acp_kind_unused_local_port)" || return 1
  [[ "${port}" =~ ^[1-9][0-9]*$ ]] || live_acp_kind_die "failed to allocate a local Vekil probe port" || return 1

  LIVE_ACP_VEKIL_PORT_FORWARD_LOG="$(mktemp "${LIVE_ACP_SECRET_DIR}/vekil-port-forward.XXXXXX")"
  LIVE_ACP_VEKIL_BASE_URL="http://127.0.0.1:${port}"
  kubectl --context "${LIVE_ACP_CONTEXT}" -n vekil-system port-forward \
    --address=127.0.0.1 service/vekil "${port}:1337" \
    >"${LIVE_ACP_VEKIL_PORT_FORWARD_LOG}" 2>&1 &
  LIVE_ACP_VEKIL_PORT_FORWARD_PID=$!
  LIVE_ACP_VEKIL_PORT_FORWARD_OWNED=1

  deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if ! kill -0 "${LIVE_ACP_VEKIL_PORT_FORWARD_PID}" >/dev/null 2>&1; then
      live_acp_kind_stop_vekil_port_forward
      live_acp_kind_die "Vekil port-forward exited before becoming ready"
      return 1
    fi
    if curl --fail --silent --show-error --connect-timeout 1 --max-time 2 \
        -o /dev/null "${LIVE_ACP_VEKIL_BASE_URL}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done

  live_acp_kind_stop_vekil_port_forward
  live_acp_kind_die "timed out waiting for the bounded Vekil port-forward readiness probe"
  return 1
}

live_acp_kind_probe_vekil_wire_path() {
  local provider="$1"
  local model="$2"
  local endpoint="$3"
  local payload payload_file response_file error_file url http_code probe_value="live-acp-preflight"
  local authorization_header="Authori""zation" api_key_header="x-api-""key"
  local -a headers

  [[ -n "${LIVE_ACP_VEKIL_BASE_URL:-}" ]] || \
    live_acp_kind_die "Vekil port-forward is not running for the live wire probe" || return 1

  case "${endpoint}" in
    /responses)
      payload="$(jq -cn --arg model "${model}" '{
        model:$model,
        input:"Reply with exactly OK.",
        max_output_tokens:1024,
        stream:true
      }')"
      headers=(
        --header 'Content-Type: application/json'
        --header "${authorization_header}: Bearer ${probe_value}"
      )
      ;;
    /messages)
      payload="$(jq -cn --arg model "${model}" '{
        model:$model,
        max_tokens:16,
        stream:true,
        messages:[{role:"user",content:"Reply with exactly OK."}]
      }')"
      headers=(
        --header 'Content-Type: application/json'
        --header "${api_key_header}: ${probe_value}"
        --header 'anthropic-version: 2023-06-01'
      )
      ;;
    /chat/completions)
      # Reasoning-family chat models reject the deprecated max_tokens
      # parameter, so the probe uses max_completion_tokens.
      payload="$(jq -cn --arg model "${model}" '{
        model:$model,
        max_completion_tokens:16,
        stream:true,
        messages:[{role:"user",content:"Reply with exactly OK."}]
      }')"
      headers=(
        --header 'Content-Type: application/json'
        --header "${authorization_header}: Bearer ${probe_value}"
      )
      ;;
    *)
      live_acp_kind_die "unsupported Vekil preflight endpoint: ${endpoint}"
      return 1
      ;;
  esac

  url="${LIVE_ACP_VEKIL_BASE_URL}/v1${endpoint}"
  payload_file="$(mktemp "${LIVE_ACP_SECRET_DIR}/vekil-wire-payload.XXXXXX")"
  response_file="$(mktemp "${LIVE_ACP_SECRET_DIR}/vekil-wire-response.XXXXXX")"
  error_file="$(mktemp "${LIVE_ACP_SECRET_DIR}/vekil-wire-probe.XXXXXX")"
  printf '%s\n' "${payload}" >"${payload_file}"
  if ! http_code="$(curl --silent --show-error --no-buffer --connect-timeout 5 --max-time 120 \
      -o "${response_file}" --write-out '%{http_code}' "${headers[@]}" \
      --data-binary "@${payload_file}" "${url}" 2>"${error_file}")"; then
    rm -f "${payload_file}" "${response_file}" "${error_file}"
    live_acp_kind_die "Vekil ${endpoint} live probe rejected configured ${provider} model ${model}"
    return 1
  fi
  case "${http_code}" in
    2??) ;;
    *)
      rm -f "${payload_file}" "${response_file}" "${error_file}"
      live_acp_kind_die "Vekil ${endpoint} live probe rejected configured ${provider} model ${model}"
      return 1
      ;;
  esac
  case "${endpoint}" in
    /responses)
      if grep -Eq '^event: (error|response\.failed|response\.incomplete)$|"type":"(error|response\.failed|response\.incomplete)"' \
          "${response_file}" || \
          ! grep -Eq '^event: response\.completed$|"type":"response\.completed"' "${response_file}"; then
        rm -f "${payload_file}" "${response_file}" "${error_file}"
        live_acp_kind_die "Vekil ${endpoint} live probe did not complete configured ${provider} model ${model} successfully"
        return 1
      fi
      ;;
    /messages)
      if grep -Eq '^event: error$|"type":"error"' "${response_file}" || \
          ! grep -Eq '^event: message_stop$|"type":"message_stop"' "${response_file}"; then
        rm -f "${payload_file}" "${response_file}" "${error_file}"
        live_acp_kind_die "Vekil ${endpoint} live probe did not complete configured ${provider} model ${model} successfully"
        return 1
      fi
      ;;
    /chat/completions)
      if grep -Eq '^data: .*"error"' "${response_file}" || \
          ! grep -Fx 'data: [DONE]' "${response_file}" >/dev/null; then
        rm -f "${payload_file}" "${response_file}" "${error_file}"
        live_acp_kind_die "Vekil ${endpoint} live probe did not complete configured ${provider} model ${model} successfully"
        return 1
      fi
      ;;
  esac
  rm -f "${payload_file}" "${response_file}" "${error_file}"
}

live_acp_kind_probe_configured_models() {
  local codex_model="${ACP_E2E_CODEX_MODEL:-gpt-5.4}"
  local claude_model="${ACP_E2E_CLAUDE_MODEL:-claude-sonnet-4.6}"
  local copilot_model="${ACP_E2E_COPILOT_MODEL:-gpt-5.3-codex}"
  local opencode_model="${ACP_E2E_OPENCODE_MODEL:-${ACP_E2E_CODEX_MODEL:-gpt-5.4}}"
  opencode_model="${opencode_model#*/}"
  local status=0

  live_acp_kind_log "Probing configured provider models through their live Vekil streaming wire paths"
  live_acp_kind_start_vekil_port_forward || return 1
  if ! live_acp_kind_probe_vekil_wire_path Codex "${codex_model}" /responses; then
    status=1
  elif ! live_acp_kind_probe_vekil_wire_path OpenCode "${opencode_model}" /chat/completions; then
    status=1
  elif ! live_acp_kind_probe_vekil_wire_path Claude "${claude_model}" /messages; then
    status=1
  elif [[ "${copilot_model}" != "${codex_model}" ]] && \
      ! live_acp_kind_probe_vekil_wire_path Copilot "${copilot_model}" /responses; then
    status=1
  fi
  live_acp_kind_stop_vekil_port_forward
  return "${status}"
}

live_acp_kind_deploy_vekil() {
  live_acp_kind_log "Deploying digest-pinned Vekil with noninteractive Secret-backed Copilot auth"
  "${LIVE_ACP_VEKIL_DEPLOY_SCRIPT}" \
    --context "${LIVE_ACP_CONTEXT}" \
    --namespace vekil-system \
    --name vekil \
    --image "${LIVE_ACP_VEKIL_IMAGE}" \
    --image-pull-policy IfNotPresent \
    --create-copilot-token-secret live-acp-runtime-copilot:token \
    --timeout "${LIVE_ACP_ROLLOUT_TIMEOUT}"

  local models_file="${LIVE_ACP_SECRET_DIR}/vekil-models.json"
  live_acp_kind_log "Validating configured provider models and required endpoints through the Vekil Service"
  kubectl --context "${LIVE_ACP_CONTEXT}" get --raw \
    /api/v1/namespaces/vekil-system/services/http:vekil:1337/proxy/v1/models >"${models_file}"
  live_acp_kind_validate_vekil_catalog "${models_file}" || return 1
  live_acp_kind_probe_configured_models
}

live_acp_kind_deploy_orka() {
  live_acp_kind_log "Installing current CRDs"
  live_acp_kind_run make -C "${LIVE_ACP_REPO_ROOT}" install

  live_acp_kind_run kubectl wait --for=condition=Established \
    crd/runtimepools.core.orka.ai crd/promptattempts.core.orka.ai \
    crd/runtimesessioncontrols.core.orka.ai crd/branchclaims.core.orka.ai \
    crd/publications.core.orka.ai crd/controllerepochs.core.orka.ai \
    crd/externaleffects.core.orka.ai --timeout="${LIVE_ACP_ROLLOUT_TIMEOUT}"

  live_acp_kind_log "Bootstrapping test-only admission TLS"
  orka_e2e_bootstrap_admission_tls

  live_acp_kind_log "Deploying digest-pinned Orka ACP workloads"
  live_acp_kind_run make -C "${LIVE_ACP_REPO_ROOT}" deploy \
    IMG="${LIVE_ACP_CONTROLLER_REF}" \
    ACP_CODEX_RUNTIME_IMG="${LIVE_ACP_CODEX_REF}" \
    ACP_CLAUDE_RUNTIME_IMG="${LIVE_ACP_CLAUDE_REF}" \
    ACP_COPILOT_RUNTIME_IMG="${LIVE_ACP_COPILOT_REF}" \
    ACP_OPENCODE_RUNTIME_IMG="${LIVE_ACP_OPENCODE_REF}" \
    WORKSPACE_PUBLISHER_IMG="${LIVE_ACP_PUBLISHER_REF}" \
    GENERAL_WORKER_IMG="${LIVE_ACP_GENERAL_WORKER_REF}"

  local deployment
  for deployment in orka-controller-manager orka-workspace-publisher orka-provider-auth-proxy orka-scm-egress-proxy; do
    live_acp_kind_run kubectl -n orka-system rollout status "deployment/${deployment}" --timeout="${LIVE_ACP_ROLLOUT_TIMEOUT}"
  done
}


live_acp_kind_create_release_credentials() {
  live_acp_kind_enabled "${RELEASE_GATE:-0}" || return 0

  local namespace="orka-system" key="token" role env_name secret_name token_file
  local -a roles=(read target-read write forge)
  local -a env_names=(
    ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN
    ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN
    ACP_E2E_WRITE_CREDENTIAL_TOKEN
    ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN
  )
  local -a secret_names=(
    live-acp-source-read
    live-acp-target-read
    live-acp-target-write
    live-acp-forge
  )

  live_acp_kind_log "Creating four role-separated release-gate credential Secrets"
  for role in "${!roles[@]}"; do
    env_name="${env_names[$role]}"
    secret_name="${secret_names[$role]}"
    token_file="${LIVE_ACP_SECRET_DIR}/${secret_name}"
    printf '%s' "${!env_name}" >"${token_file}"
    chmod 600 "${token_file}"
    kubectl -n "${namespace}" create secret generic "${secret_name}" \
      --from-file="${key}=${token_file}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    rm -f "${token_file}"
  done

  printf -v ACP_E2E_WRITE_READ_CREDENTIAL_SECRET '%s' "${secret_names[0]}"
  export ACP_E2E_WRITE_READ_CREDENTIAL_NAMESPACE="${namespace}"
  printf -v ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_SECRET '%s' "${secret_names[1]}"
  export ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_NAMESPACE="${namespace}"
  printf -v ACP_E2E_WRITE_CREDENTIAL_SECRET '%s' "${secret_names[2]}"
  export ACP_E2E_WRITE_CREDENTIAL_NAMESPACE="${namespace}"
  printf -v ACP_E2E_WRITE_FORGE_CREDENTIAL_SECRET '%s' "${secret_names[3]}"
  export ACP_E2E_WRITE_FORGE_CREDENTIAL_NAMESPACE="${namespace}"
  export ACP_E2E_WRITE_READ_CREDENTIAL_SECRET ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_SECRET
  export ACP_E2E_WRITE_CREDENTIAL_SECRET ACP_E2E_WRITE_FORGE_CREDENTIAL_SECRET
}

live_acp_kind_bootstrap() {
  live_acp_kind_create_cluster
  live_acp_kind_start_registry
  live_acp_kind_deploy_vekil
  live_acp_kind_build_and_publish_images
  live_acp_kind_deploy_orka
  live_acp_kind_create_release_credentials
}

live_acp_kind_cleanup() {
  local status="${1:-0}"
  local cleanup_status=0
  set +e
  live_acp_kind_stop_vekil_port_forward >/dev/null 2>&1 || cleanup_status=1
  if [[ -n "${LIVE_ACP_SECRET_DIR:-}" ]]; then
    rm -rf -- "${LIVE_ACP_SECRET_DIR}" >/dev/null 2>&1 || cleanup_status=1
  fi
  if (( cleanup_status == 0 )); then
    acp_report_update '.cleanup.bootstrapCredentials = "passed"' || cleanup_status=1
  else
    acp_report_update '.cleanup.bootstrapCredentials = "failed"' || true
  fi
  if acp_report_enabled && jq -e '.preserved != null' "${ACP_E2E_REPORT_FILE}" >/dev/null; then
    live_acp_kind_log "Preserving Kind resources because remote cleanup could not be proven safe"
    acp_report_update '.cleanup.cluster = "preserved" | .cleanup.registry = "preserved"' || true
    return 1
  fi
  if [[ "${LIVE_ACP_KEEP_CLUSTER:-0}" == "1" ]]; then
    live_acp_kind_log "Keeping Kind cluster ${LIVE_ACP_KIND_CLUSTER:-<not-created>} (ACP_E2E_KEEP_CLUSTER=1)"
    acp_report_update '.cleanup.cluster = "pending" | .cleanup.registry = "pending"' || cleanup_status=1
    if (( status != 0 )); then return "${status}"; fi
    return "${cleanup_status}"
  fi
  live_acp_kind_delete_cluster || cleanup_status=1
  if (( status != 0 )); then
    return "${status}"
  fi
  return "${cleanup_status}"
}

# CI calls this after uploading the report, using only the recorded run tag and
# cluster name. It does not repeat or overwrite credential cleanup results.
live_acp_kind_delete_cluster() {
  local cleanup_status=0
  if acp_report_enabled && jq -e '.preserved != null' "${ACP_E2E_REPORT_FILE}" >/dev/null; then
    acp_report_update '.cleanup.cluster = "preserved" | .cleanup.registry = "preserved"' || true
    return 1
  fi
  if [[ "${LIVE_ACP_REGISTRY_STARTED:-0}" == "1" ]]; then
    # shellcheck source=scripts/lib/kind-local-registry.sh
    . "${LIVE_ACP_REPO_ROOT}/scripts/lib/kind-local-registry.sh"
    if orka_kind_registry_stop "${LIVE_ACP_KIND_CLUSTER:-}"; then
      acp_report_update '.cleanup.registry = "passed"' || cleanup_status=1
    else
      cleanup_status=1
      acp_report_update '.cleanup.registry = "failed"' || true
    fi
  fi
  if [[ "${LIVE_ACP_KIND_CREATED:-0}" == "1" ]]; then
    if "${LIVE_ACP_KINDCTL_BIN}" delete --tag "${LIVE_ACP_KIND_TAG}"; then
      acp_report_update '.cleanup.cluster = "passed"' || cleanup_status=1
    else
      cleanup_status=1
      acp_report_update '.cleanup.cluster = "failed"' || true
    fi
  fi
  return "${cleanup_status}"
}
