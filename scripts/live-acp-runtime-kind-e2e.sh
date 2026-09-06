#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage: scripts/live-acp-runtime-kind-e2e.sh [--namespace NAME] [--keep-cluster] [--preflight-only]

Builds current Orka ACP images, creates a repo-scoped Kind cluster, deploys a
pinned Vekil proxy and Orka, then runs scripts/live-acp-runtime-e2e.sh.

This wrapper is intentionally noninteractive: COPILOT_GITHUB_TOKEN is required
and is written only through the Vekil deploy helper into a Kubernetes Secret.
It never prints the token or starts GitHub device-code login. CI uses the same
path as local runs.

Environment:
  ACP_E2E_KIND_TAG        kindctl tag (default: run-scoped live-acp-runtime)
  ACP_E2E_KIND_CONFIG     optional Kind config path
  ACP_E2E_KEEP_CLUSTER=1  keep the cluster and local registry after the run
  ACP_E2E_VEKIL_IMAGE     digest-pinned Vekil image override
  ACP_E2E_VEKIL_LOCAL_IMAGE local Docker image published through the run's
                          registry and digest-pinned there (development builds)
  ACP_E2E_OPENCODE_MODEL  reviewed OpenCode provider/model identifier
  ACP_E2E_OPENCODE_CONTEXT_WINDOW reviewed OpenCode context capacity (required)
  ACP_E2E_OPENCODE_MAX_TOKENS reviewed OpenCode output limit (required)
  ACP_E2E_ROLLOUT_TIMEOUT rollout timeout (default: 10m)
  RELEASE_GATE=1          forwarded to the canonical validator
USAGE
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=scripts/lib/live-acp-runtime-kind-bootstrap.sh
. "${script_dir}/lib/live-acp-runtime-kind-bootstrap.sh"

namespace=""
preflight_only=0
while (( $# > 0 )); do
  case "$1" in
    --namespace)
      [[ $# -ge 2 ]] || { echo "error: --namespace requires a value" >&2; exit 2; }
      namespace="$2"
      shift 2
      ;;
    --keep-cluster)
      export ACP_E2E_KEEP_CLUSTER=1
      shift
      ;;
    --preflight-only)
      preflight_only=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

run_id="${ACP_E2E_RUN_ID:-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$(date -u +%Y%m%d%H%M%S)-$$}"
image_tag="$(printf '%s' "${run_id}" | LC_ALL=C tr -c 'A-Za-z0-9_.-' '-' | cut -c1-80)"
export ACP_E2E_RUN_ID="${run_id}"

LIVE_ACP_REPO_ROOT="${repo_root}"
LIVE_ACP_KINDCTL_BIN="${ACP_E2E_KINDCTL_BIN:-${repo_root}/.agents/skills/kindctl/bin/kindctl}"
LIVE_ACP_VEKIL_DEPLOY_SCRIPT="${ACP_E2E_VEKIL_DEPLOY_SCRIPT:-${repo_root}/.agents/skills/vekil-reverse-proxy-deploy/scripts/deploy_vekil_reverse_proxy.sh}"
LIVE_ACP_VALIDATOR_SCRIPT="${ACP_E2E_VALIDATOR_SCRIPT:-${repo_root}/scripts/live-acp-runtime-e2e.sh}"
LIVE_ACP_KIND_TAG="${ACP_E2E_KIND_TAG:-live-acp-runtime-${image_tag}}"
LIVE_ACP_KIND_CONFIG="${ACP_E2E_KIND_CONFIG:-}"
LIVE_ACP_KEEP_CLUSTER="${ACP_E2E_KEEP_CLUSTER:-0}"
LIVE_ACP_VEKIL_IMAGE="${ACP_E2E_VEKIL_IMAGE:-ghcr.io/sozercan/vekil:v0.14.1@sha256:2fa0558f6304cc6ed1fb5b0135f62f12f28f1cdd0a8c057c4283414bceac1362}"
LIVE_ACP_VEKIL_LOCAL_IMAGE="${ACP_E2E_VEKIL_LOCAL_IMAGE:-}"
export LIVE_ACP_VEKIL_LOCAL_IMAGE
LIVE_ACP_ROLLOUT_TIMEOUT="${ACP_E2E_ROLLOUT_TIMEOUT:-10m}"
LIVE_ACP_CONTROLLER_IMAGE="orka-controller:live-acp-${image_tag}"
LIVE_ACP_CODEX_IMAGE="orka-acp-codex:live-acp-${image_tag}"
LIVE_ACP_CLAUDE_IMAGE="orka-acp-claude:live-acp-${image_tag}"
LIVE_ACP_COPILOT_IMAGE="orka-acp-copilot:live-acp-${image_tag}"
LIVE_ACP_OPENCODE_IMAGE="orka-acp-opencode:live-acp-${image_tag}"
LIVE_ACP_PUBLISHER_IMAGE="orka-workspace-publisher:live-acp-${image_tag}"
LIVE_ACP_GENERAL_WORKER_IMAGE="orka-general-worker:live-acp-${image_tag}"
LIVE_ACP_KIND_CREATED=0
LIVE_ACP_REGISTRY_STARTED=0
LIVE_ACP_SECRET_DIR=""
export LIVE_ACP_REPO_ROOT LIVE_ACP_KINDCTL_BIN LIVE_ACP_VEKIL_DEPLOY_SCRIPT LIVE_ACP_VALIDATOR_SCRIPT
export LIVE_ACP_KIND_TAG LIVE_ACP_KIND_CONFIG LIVE_ACP_KEEP_CLUSTER LIVE_ACP_VEKIL_IMAGE LIVE_ACP_ROLLOUT_TIMEOUT
export LIVE_ACP_CONTROLLER_IMAGE LIVE_ACP_CODEX_IMAGE LIVE_ACP_CLAUDE_IMAGE LIVE_ACP_COPILOT_IMAGE
export LIVE_ACP_OPENCODE_IMAGE LIVE_ACP_PUBLISHER_IMAGE LIVE_ACP_GENERAL_WORKER_IMAGE
export LIVE_ACP_SECRET_DIR

if live_acp_kind_enabled "${RELEASE_GATE:-0}"; then
  [[ -n "${ACP_E2E_WRITE_SOURCE_REPO:-}" ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_SOURCE_REPO is required when RELEASE_GATE=1"
  [[ -n "${ACP_E2E_WRITE_PUBLICATION_REPO:-}" ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_PUBLICATION_REPO is required when RELEASE_GATE=1"
  [[ -n "${ACP_E2E_WRITE_SOURCE_REF:-}" ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_SOURCE_REF is required when RELEASE_GATE=1"
  [[ "${ACP_E2E_WRITE_SOURCE_REPO}" =~ ^https://github\.com/[^/]+/[^/]+(\.git)?$ ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_SOURCE_REPO must be an HTTPS github.com repository URL"
  [[ "${ACP_E2E_WRITE_PUBLICATION_REPO}" =~ ^https://github\.com/[^/]+/[^/]+(\.git)?$ ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_PUBLICATION_REPO must be an HTTPS github.com repository URL"
  source_identity="${ACP_E2E_WRITE_SOURCE_REPO%.git}"
  publication_identity="${ACP_E2E_WRITE_PUBLICATION_REPO%.git}"
  source_identity_lower="$(printf '%s' "${source_identity}" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  publication_identity_lower="$(printf '%s' "${publication_identity}" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  [[ "${source_identity_lower}" != "${publication_identity_lower}" ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_PUBLICATION_REPO must be a distinct repository"
  [[ "${ACP_E2E_WRITE_SOURCE_REF}" =~ ^[0-9a-fA-F]{40}$ ]] || \
    live_acp_kind_die "ACP_E2E_WRITE_SOURCE_REF must be a full 40-character commit SHA"
  live_acp_kind_enabled "${ACP_E2E_WRITE_CREATE_PR:-0}" || \
    live_acp_kind_die "ACP_E2E_WRITE_CREATE_PR=1 is required when RELEASE_GATE=1"
  if [[ -n "${ACP_E2E_REPO:-}" ]]; then
    read_identity="${ACP_E2E_REPO%.git}"
  else
    read_identity=""
  fi
  read_identity_lower="$(printf '%s' "${read_identity}" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  if [[ -n "${read_identity}" && "${read_identity_lower}" != "${source_identity_lower}" ]]; then
    live_acp_kind_die "ACP_E2E_REPO must equal ACP_E2E_WRITE_SOURCE_REPO when RELEASE_GATE=1"
  fi
  read_ref_lower="$(printf '%s' "${ACP_E2E_REF:-}" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  write_ref_lower="$(printf '%s' "${ACP_E2E_WRITE_SOURCE_REF}" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
  if [[ -n "${ACP_E2E_REF:-}" && "${read_ref_lower}" != "${write_ref_lower}" ]]; then
    live_acp_kind_die "ACP_E2E_REF must equal ACP_E2E_WRITE_SOURCE_REF when RELEASE_GATE=1"
  fi
  export ACP_E2E_REPO="${ACP_E2E_WRITE_SOURCE_REPO}"
  export ACP_E2E_REF="${ACP_E2E_WRITE_SOURCE_REF}"
fi

live_acp_kind_preflight
if (( preflight_only )); then
  live_acp_kind_log "Live ACP Kind E2E preflight passed"
  exit 0
fi

LIVE_ACP_SECRET_DIR="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/live-acp-kind-secrets.XXXXXX")"
export LIVE_ACP_SECRET_DIR
trap 'status=$?; live_acp_kind_cleanup "${status}"; exit "${status}"' EXIT
live_acp_kind_bootstrap

validator_args=(--context "${LIVE_ACP_CONTEXT}")
if [[ -z "${namespace}" ]]; then
  # The isolated harness-v2 controller only serves Tasks in its watch
  # namespace, so the validator runs there in shared watch-namespace mode.
  namespace="orka-system"
fi
validator_args+=(--namespace "${namespace}")
# This wrapper owns a dedicated Kind cluster, so the shared controller watch
# namespace cannot contain unrelated RuntimePool consumers.
export ACP_E2E_ALLOW_SHARED_POOL_MUTATION="${ACP_E2E_ALLOW_SHARED_POOL_MUTATION:-1}"
live_acp_kind_log "Running canonical live ACP runtime validator"
"${LIVE_ACP_VALIDATOR_SCRIPT}" "${validator_args[@]}"
