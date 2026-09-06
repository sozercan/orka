#!/usr/bin/env bash
# shellcheck disable=SC2016 # This test intentionally matches literal shell expressions.
set -Eeuo pipefail

# scripts/tests suites rely on 'set -e' stopping on failed (( )) arithmetic,
# which macOS's stock bash 3.2 does not honor; failures would be silently
# masked there. Require a modern bash (for example: brew install bash).
if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "error: this test suite requires bash >= 4; found ${BASH_VERSION}" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
wrapper="${root}/scripts/live-acp-runtime-kind-e2e.sh"
bootstrap="${root}/scripts/lib/live-acp-runtime-kind-bootstrap.sh"

export ACP_E2E_OPENCODE_CONTEXT_WINDOW=32768
export ACP_E2E_OPENCODE_MAX_TOKENS=4096

help="$(${wrapper} --help)"
grep -F 'intentionally noninteractive' <<<"${help}" >/dev/null
grep -F 'COPILOT_GITHUB_TOKEN is required' <<<"${help}" >/dev/null

grep -F -- '--create-copilot-token-secret live-acp-runtime-copilot:token' "${bootstrap}" >/dev/null
grep -F '/api/v1/namespaces/vekil-system/services/http:vekil:1337/proxy/v1/models' "${bootstrap}" >/dev/null
grep -F 'ACP_E2E_COPILOT_MODEL:-gpt-5.3-codex' "${bootstrap}" >/dev/null
grep -F 'LIVE_ACP_OPENCODE_IMAGE="orka-acp-opencode:live-acp-${image_tag}"' "${wrapper}" >/dev/null
grep -F 'LIVE_ACP_GENERAL_WORKER_IMAGE="orka-general-worker:live-acp-${image_tag}"' "${wrapper}" >/dev/null
grep -F 'ACP_OPENCODE_RUNTIME_IMG="${LIVE_ACP_OPENCODE_IMAGE}"' "${bootstrap}" >/dev/null
grep -F 'GENERAL_WORKER_IMG="${LIVE_ACP_GENERAL_WORKER_IMAGE}"' "${bootstrap}" >/dev/null
grep -F 'docker-build-acp-opencode-runtime' "${bootstrap}" >/dev/null
grep -F 'docker-build-general-worker' "${bootstrap}" >/dev/null
grep -F 'orka/acp-opencode-runtime' "${bootstrap}" >/dev/null
grep -F 'orka/general-worker' "${bootstrap}" >/dev/null
grep -F 'ACP_OPENCODE_RUNTIME_IMG="${LIVE_ACP_OPENCODE_REF}"' "${bootstrap}" >/dev/null
grep -F 'GENERAL_WORKER_IMG="${LIVE_ACP_GENERAL_WORKER_REF}"' "${bootstrap}" >/dev/null
grep -F 'opencode_model="${ACP_E2E_OPENCODE_MODEL:-${ACP_E2E_CODEX_MODEL:-gpt-5.4}}"' "${bootstrap}" >/dev/null
grep -F 'opencode_model="${opencode_model#*/}"' "${bootstrap}" >/dev/null
grep -F 'ACP_E2E_OPENCODE_CONTEXT_WINDOW must be a positive integer' "${bootstrap}" >/dev/null
grep -F 'ACP_E2E_OPENCODE_MAX_TOKENS must be a positive integer' "${bootstrap}" >/dev/null
# shellcheck disable=SC2016 # Match the literal catalog-validation expression.
grep -F 'live_acp_kind_validate_vekil_catalog "${models_file}"' "${bootstrap}" >/dev/null
grep -F 'live_acp_kind_probe_configured_models' "${bootstrap}" >/dev/null
# shellcheck disable=SC2016 # Match the literal delegation expression in the wrapper.
grep -F '"${LIVE_ACP_VALIDATOR_SCRIPT}" "${validator_args[@]}"' "${wrapper}" >/dev/null
grep -F 'export ACP_E2E_ALLOW_SHARED_POOL_MUTATION="${ACP_E2E_ALLOW_SHARED_POOL_MUTATION:-1}"' "${wrapper}" >/dev/null
grep -F 'ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN' "${bootstrap}" >/dev/null
grep -F 'Creating four role-separated release-gate credential Secrets' "${bootstrap}" >/dev/null
if grep -Eq 'set +-x|echo .*COPILOT_GITHUB_TOKEN' "${bootstrap}" "${wrapper}"; then
  echo 'bootstrap must not enable tracing or print the provider credential' >&2
  exit 1
fi

# shellcheck source=scripts/lib/live-acp-runtime-kind-bootstrap.sh
. "${bootstrap}"
inherited_state="$(
  LIVE_ACP_VEKIL_PORT_FORWARD_PID=123 \
    LIVE_ACP_VEKIL_PORT_FORWARD_LOG=/tmp/untrusted-port-forward.log \
    LIVE_ACP_VEKIL_PORT_FORWARD_OWNED=1 \
    bash -c '. "$1"; printf "%s|%s|%s" "${LIVE_ACP_VEKIL_PORT_FORWARD_PID}" "${LIVE_ACP_VEKIL_PORT_FORWARD_LOG}" "${LIVE_ACP_VEKIL_PORT_FORWARD_OWNED}"' \
      _ "${bootstrap}"
)"
[[ "${inherited_state}" == '||0' ]] || {
  echo 'bootstrap retained inherited port-forward cleanup state' >&2
  exit 1
}

unowned_root="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-unowned-forward.XXXXXX")"
mkdir -p "${unowned_root}/owned"
unowned_log="${unowned_root}/vekil-port-forward.unowned"
printf 'keep\n' >"${unowned_log}"
LIVE_ACP_SECRET_DIR="${unowned_root}/owned"
LIVE_ACP_VEKIL_PORT_FORWARD_PID="$$"
LIVE_ACP_VEKIL_PORT_FORWARD_LOG="${unowned_log}"
LIVE_ACP_VEKIL_PORT_FORWARD_OWNED=0
live_acp_kind_stop_vekil_port_forward
[[ -f "${unowned_log}" ]] || {
  echo 'cleanup removed an unowned port-forward log path' >&2
  exit 1
}
rm -rf "${unowned_root}"

catalog_root="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-catalog-test.XXXXXX")"
catalog_file="${catalog_root}/models.json"
cat >"${catalog_file}" <<'JSON'
{
  "data": [
    {"id":"gpt-codex-test","supported_endpoints":["/responses"]},
    {"id":"gpt-opencode-test","supported_endpoints":["/responses"]},
    {"id":"claude-test","supported_endpoints":["/v1/messages"]}
  ],
  "models": [
    {"id":"gpt-copilot-test","supported_endpoints":["/responses"]}
  ]
}
JSON
export ACP_E2E_CODEX_MODEL='gpt-codex-test'
export ACP_E2E_OPENCODE_MODEL='openai/gpt-opencode-test'
export ACP_E2E_CLAUDE_MODEL='claude-test'
export ACP_E2E_COPILOT_MODEL='gpt-copilot-test'
live_acp_kind_validate_vekil_catalog "${catalog_file}"

jq '(.models[] | select(.id == "gpt-copilot-test") | .supported_endpoints) = ["/chat/completions"]' \
  "${catalog_file}" >"${catalog_root}/wrong-endpoint.json"
if catalog_error="$(live_acp_kind_validate_vekil_catalog "${catalog_root}/wrong-endpoint.json" 2>&1)"; then
  echo 'catalog validation accepted a present model without its required wire endpoint' >&2
  exit 1
fi
grep -F 'does not advertise required endpoint /responses' <<<"${catalog_error}" >/dev/null

jq '(.data[] | select(.id == "gpt-opencode-test") | .supported_endpoints) = ["/v1/messages"]' \
  "${catalog_file}" >"${catalog_root}/wrong-opencode-endpoint.json"
if catalog_error="$(live_acp_kind_validate_vekil_catalog "${catalog_root}/wrong-opencode-endpoint.json" 2>&1)"; then
  echo 'catalog validation accepted an OpenCode model without Chat or Responses compatibility' >&2
  exit 1
fi
grep -F 'does not advertise required endpoint /chat/completions or compatible /responses' \
  <<<"${catalog_error}" >/dev/null

LIVE_ACP_CONTEXT='kind-live-acp-test'
LIVE_ACP_SECRET_DIR="${catalog_root}"
wire_log="${catalog_root}/wire.log"
port_forward_command_log="${catalog_root}/port-forward-command.log"
port_forward_pid_file="${catalog_root}/port-forward.pid"
probe_bin="${catalog_root}/bin"
mkdir -p "${probe_bin}"
export WIRE_LOG="${wire_log}"
export PORT_FORWARD_COMMAND_LOG="${port_forward_command_log}"
export PORT_FORWARD_PID_FILE="${port_forward_pid_file}"
export WIRE_FAIL_MESSAGES=0
export WIRE_STREAM_FAILURE=0
provider_sentinel='must-not-appear-in-wire-probe'
export COPILOT_GITHUB_TOKEN="${provider_sentinel}"
cat >"${probe_bin}/kubectl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" >>"${PORT_FORWARD_COMMAND_LOG}"
if [[ "$*" == *' port-forward '* ]]; then
  printf '%s\n' "$$" >"${PORT_FORWARD_PID_FILE}"
  trap 'exit 0' TERM INT
  while :; do
    sleep 1
  done
fi
exit 1
STUB
chmod +x "${probe_bin}/kubectl"
cat >"${probe_bin}/curl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" >>"${WIRE_LOG}"
output_file=""
previous=""
for argument in "$@"; do
  if [[ "${previous}" == "-o" ]]; then
    output_file="${argument}"
  fi
  case "${argument}" in
    @*) cat "${argument#@}" >>"${WIRE_LOG}" ;;
  esac
  previous="${argument}"
done
if [[ "$*" == *'/readyz'* && ! -s "${PORT_FORWARD_PID_FILE}" ]]; then
  exit 7
fi
if [[ "${WIRE_FAIL_MESSAGES:-0}" == "1" && "$*" == *'/v1/messages'* ]]; then
  printf '%s\n' 'raw-upstream-error-must-remain-redacted' >&2
  exit 22
fi
if [[ -n "${output_file}" && "${output_file}" != "/dev/null" ]]; then
  if [[ "${WIRE_STREAM_FAILURE:-0}" == "1" && "$*" == *'/v1/responses'* ]]; then
    printf '%s\n' 'event: response.failed' 'data: {"type":"response.failed"}' >"${output_file}"
  elif [[ "$*" == *'/v1/responses'* ]]; then
    printf '%s\n' 'event: response.completed' 'data: {"type":"response.completed"}' >"${output_file}"
  elif [[ "$*" == *'/v1/messages'* ]]; then
    printf '%s\n' 'event: message_stop' 'data: {"type":"message_stop"}' >"${output_file}"
  elif [[ "$*" == *'/v1/chat/completions'* ]]; then
    printf '%s\n' 'data: {"choices":[{"delta":{"content":"OK"}}]}' 'data: [DONE]' >"${output_file}"
  fi
fi
printf '200'
exit 0
STUB
chmod +x "${probe_bin}/curl"

PATH="${probe_bin}:${PATH}" live_acp_kind_probe_configured_models
grep -F 'port-forward' "${port_forward_command_log}" >/dev/null
grep -F -- '--address=127.0.0.1' "${port_forward_command_log}" >/dev/null
grep -F 'service/vekil' "${port_forward_command_log}" >/dev/null
wire_probe_source="$(sed -n '/^live_acp_kind_probe_vekil_wire_path()/,/^}/p' "${bootstrap}")"
if grep -F 'exec' "${port_forward_command_log}" >/dev/null || grep -F 'wget' <<<"${wire_probe_source}" >/dev/null; then
  echo 'Vekil wire probe still depends on an in-container executable' >&2
  exit 1
fi
port_forward_pid="$(cat "${port_forward_pid_file}")"
if kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
  echo 'successful Vekil wire probes left the kubectl port-forward running' >&2
  exit 1
fi
[[ -z "${LIVE_ACP_VEKIL_PORT_FORWARD_PID:-}" ]]
[[ -z "${LIVE_ACP_VEKIL_PORT_FORWARD_LOG:-}" ]]
[[ -z "${LIVE_ACP_VEKIL_BASE_URL:-}" ]]
grep -F '/v1/responses' "${wire_log}" >/dev/null
grep -F '/v1/messages' "${wire_log}" >/dev/null
grep -F '/v1/chat/completions' "${wire_log}" >/dev/null
grep -F '"model":"gpt-codex-test"' "${wire_log}" >/dev/null
grep -F '"model":"gpt-opencode-test"' "${wire_log}" >/dev/null
grep -F '"model":"claude-test"' "${wire_log}" >/dev/null
grep -F '"model":"gpt-copilot-test"' "${wire_log}" >/dev/null
grep -F '"stream":true' "${wire_log}" >/dev/null
grep -F 'anthropic-version: 2023-06-01' "${wire_log}" >/dev/null
grep -F '%{http_code}' "${wire_log}" >/dev/null
if grep -F "${provider_sentinel}" "${wire_log}" >/dev/null; then
  echo 'wire probe leaked COPILOT_GITHUB_TOKEN into the probe command' >&2
  exit 1
fi

export WIRE_FAIL_MESSAGES=1
if wire_error="$(PATH="${probe_bin}:${PATH}" live_acp_kind_probe_configured_models 2>&1)"; then
  echo 'wire probe accepted a rejected configured model request' >&2
  exit 1
fi
grep -F 'Vekil /messages live probe rejected configured Claude model claude-test' <<<"${wire_error}" >/dev/null
if grep -F 'raw-upstream-error-must-remain-redacted' <<<"${wire_error}" >/dev/null; then
  echo 'wire probe exposed the raw upstream failure body' >&2
  exit 1
fi
port_forward_pid="$(cat "${port_forward_pid_file}")"
if kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
  echo 'failed Vekil wire probes left the kubectl port-forward running' >&2
  exit 1
fi
if find "${catalog_root}" -maxdepth 1 \( -name 'vekil-port-forward.*' -o -name 'vekil-wire-*.??????' \) \
    -print | grep -q .; then
  echo 'Vekil wire probes left temporary payload, error, or port-forward log files behind' >&2
  exit 1
fi

export WIRE_FAIL_MESSAGES=0
export WIRE_STREAM_FAILURE=1
if stream_error="$(PATH="${probe_bin}:${PATH}" live_acp_kind_probe_configured_models 2>&1)"; then
  echo 'wire probe accepted an HTTP 200 stream with a terminal failure event' >&2
  exit 1
fi
grep -F 'Vekil /responses live probe did not complete configured Codex model gpt-codex-test successfully' \
  <<<"${stream_error}" >/dev/null
port_forward_pid="$(cat "${port_forward_pid_file}")"
if kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
  echo 'terminal stream failure left the kubectl port-forward running' >&2
  exit 1
fi
if find "${catalog_root}" -maxdepth 1 \( -name 'vekil-port-forward.*' -o -name 'vekil-wire-*.??????' \) \
    -print | grep -q .; then
  echo 'terminal stream failure left temporary probe files behind' >&2
  exit 1
fi

rm -rf "${catalog_root}"
printf '%s\n' 'ok - Vekil preflight requires endpoint metadata and live streaming wire compatibility without credential disclosure'

fake_bin="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-kind-test.XXXXXX")"
trap 'rm -rf "${fake_bin}"' EXIT
for command in curl gh git go jq kind kubectl make python3; do
  cat >"${fake_bin}/${command}" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  chmod +x "${fake_bin}/${command}"
done
cat >"${fake_bin}/docker" <<'STUB'
#!/usr/bin/env bash
[[ "${1:-}" == info ]]
STUB
chmod +x "${fake_bin}/docker"

cat >"${fake_bin}/failing-kindctl" <<'STUB'
#!/usr/bin/env bash
[[ "${1:-}" == delete ]] || exit 2
exit 1
STUB
chmod +x "${fake_bin}/failing-kindctl"

partial_create_log="${fake_bin}/partial-create.log"
export PARTIAL_CREATE_LOG="${partial_create_log}"
cat >"${fake_bin}/partial-kindctl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${PARTIAL_CREATE_LOG}"
case "${1:-}" in
  create) exit 23 ;;
  delete) exit 0 ;;
  *) exit 97 ;;
esac
STUB
chmod +x "${fake_bin}/partial-kindctl"
partial_secret_dir="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-partial-create.XXXXXX")"
LIVE_ACP_SECRET_DIR="${partial_secret_dir}"
LIVE_ACP_KEEP_CLUSTER=0
LIVE_ACP_REGISTRY_STARTED=0
LIVE_ACP_KIND_CREATED=0
LIVE_ACP_KINDCTL_BIN="${fake_bin}/partial-kindctl"
LIVE_ACP_KIND_TAG="partial-create-test"
LIVE_ACP_KIND_CONFIG=""
set +e
live_acp_kind_create_cluster >/dev/null 2>&1
partial_create_status=$?
set -e
[[ "${partial_create_status}" -eq 23 && "${LIVE_ACP_KIND_CREATED}" -eq 1 ]] || {
  echo 'Kind create failure did not preserve status and arm exact cleanup' >&2
  exit 1
}
set +e
live_acp_kind_cleanup "${partial_create_status}" >/dev/null 2>&1
partial_cleanup_status=$?
set -e
[[ "${partial_cleanup_status}" -eq 23 ]] || {
  echo 'partial Kind cleanup masked the create failure' >&2
  exit 1
}
grep -Fx 'create --tag partial-create-test' "${partial_create_log}" >/dev/null
grep -Fx 'delete --tag partial-create-test' "${partial_create_log}" >/dev/null
[[ ! -e "${partial_secret_dir}" ]] || {
  echo 'partial Kind cleanup retained its secret directory' >&2
  exit 1
}
printf '%s\n' 'ok - partial Kind create failures exact-clean by tag'

cleanup_secret_dir="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-cleanup-test.XXXXXX")"
LIVE_ACP_SECRET_DIR="${cleanup_secret_dir}"
LIVE_ACP_KEEP_CLUSTER=0
LIVE_ACP_REGISTRY_STARTED=0
LIVE_ACP_KIND_CREATED=1
LIVE_ACP_KINDCTL_BIN="${fake_bin}/failing-kindctl"
LIVE_ACP_KIND_TAG="cleanup-test"
if live_acp_kind_cleanup 0; then
  echo 'successful validation unexpectedly masked Kind cluster deletion failure' >&2
  exit 1
fi
if [[ -d "${cleanup_secret_dir}" ]]; then
  echo 'Kind cleanup did not remove its secret directory after deletion failure' >&2
  exit 1
fi
cleanup_secret_dir="$(mktemp -d "${TMPDIR:-/tmp}/live-acp-cleanup-test.XXXXXX")"
LIVE_ACP_SECRET_DIR="${cleanup_secret_dir}"
set +e
live_acp_kind_cleanup 23
cleanup_failure_status=$?
set -e
if [[ "${cleanup_failure_status}" -ne 23 ]]; then
  echo "Kind cleanup replaced validator failure 23 with ${cleanup_failure_status}" >&2
  exit 1
fi
printf '%s\n' 'ok - Kind cleanup failure fails successful validation without masking validator failures'

provider_sentinel='must-not-appear-in-output'
output="$(PATH="${fake_bin}:${PATH}" COPILOT_GITHUB_TOKEN="${provider_sentinel}" "${wrapper}" --preflight-only 2>&1)"
grep -F 'preflight passed' <<<"${output}" >/dev/null
if grep -F "${provider_sentinel}" <<<"${output}" >/dev/null; then
  echo 'preflight leaked COPILOT_GITHUB_TOKEN' >&2
  exit 1
fi

if PATH="${fake_bin}:${PATH}" COPILOT_GITHUB_TOKEN='' "${wrapper}" --preflight-only >"${fake_bin}/missing.out" 2>&1; then
  echo 'preflight unexpectedly accepted missing COPILOT_GITHUB_TOKEN' >&2
  exit 1
fi
grep -F 'noninteractive and never starts device-code login' "${fake_bin}/missing.out" >/dev/null

if PATH="${fake_bin}:${PATH}" RELEASE_GATE=1 COPILOT_GITHUB_TOKEN="${provider_sentinel}" \
    ACP_E2E_WRITE_CREATE_PR=1 \
    ACP_E2E_WRITE_PUBLICATION_REPO='https://github.com/example/orka.git' \
    ACP_E2E_WRITE_SOURCE_REPO='https://github.com/orka-agents/orka.git' \
    ACP_E2E_WRITE_SOURCE_REF='0123456789abcdef0123456789abcdef01234567' \
    "${wrapper}" --preflight-only >"${fake_bin}/release-missing.out" 2>&1; then
  echo 'release preflight unexpectedly accepted missing role-separated credentials' >&2
  exit 1
fi
grep -F 'ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN is required when RELEASE_GATE=1' \
  "${fake_bin}/release-missing.out" >/dev/null

source_read_sentinel='source-read-value'
target_read_sentinel='target-read-value'
target_write_sentinel='target-write-value'
forge_sentinel='forge-value'
printf -v ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN '%s' "${source_read_sentinel}"
printf -v ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN '%s' "${target_read_sentinel}"
printf -v ACP_E2E_WRITE_CREDENTIAL_TOKEN '%s' "${target_write_sentinel}"
printf -v ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN '%s' "${forge_sentinel}"
export ACP_E2E_WRITE_READ_CREDENTIAL_TOKEN ACP_E2E_WRITE_TARGET_READ_CREDENTIAL_TOKEN
export ACP_E2E_WRITE_CREDENTIAL_TOKEN ACP_E2E_WRITE_FORGE_CREDENTIAL_TOKEN
release_output="$(
  PATH="${fake_bin}:${PATH}" \
    RELEASE_GATE=1 \
    COPILOT_GITHUB_TOKEN="${provider_sentinel}" \
    ACP_E2E_WRITE_CREATE_PR=1 \
    ACP_E2E_WRITE_PUBLICATION_REPO='https://github.com/example/orka.git' \
    ACP_E2E_WRITE_SOURCE_REPO='https://github.com/orka-agents/orka.git' \
    ACP_E2E_WRITE_SOURCE_REF='0123456789abcdef0123456789abcdef01234567' \
    "${wrapper}" --preflight-only 2>&1
)"
grep -F 'preflight passed' <<<"${release_output}" >/dev/null
for value in "${provider_sentinel}" "${source_read_sentinel}" "${target_read_sentinel}" \
    "${target_write_sentinel}" "${forge_sentinel}"; do
  if grep -F "${value}" <<<"${release_output}" >/dev/null; then
    echo 'release preflight leaked a provider or publication credential' >&2
    exit 1
  fi
done

if PATH="${fake_bin}:${PATH}" RELEASE_GATE=1 COPILOT_GITHUB_TOKEN="${provider_sentinel}" \
    ACP_E2E_REPO='https://github.com/example/other.git' \
    ACP_E2E_WRITE_CREATE_PR=1 \
    ACP_E2E_WRITE_PUBLICATION_REPO='https://github.com/example/orka.git' \
    ACP_E2E_WRITE_SOURCE_REPO='https://github.com/orka-agents/orka.git' \
    ACP_E2E_WRITE_SOURCE_REF='0123456789abcdef0123456789abcdef01234567' \
    "${wrapper}" --preflight-only >"${fake_bin}/release-mismatch.out" 2>&1; then
  echo 'release preflight unexpectedly accepted a different read repository' >&2
  exit 1
fi
grep -F 'ACP_E2E_REPO must equal ACP_E2E_WRITE_SOURCE_REPO' \
  "${fake_bin}/release-mismatch.out" >/dev/null

printf '%s\n' 'ok - live ACP Kind bootstrap is noninteractive, secret-safe, and delegates to the canonical validator'
