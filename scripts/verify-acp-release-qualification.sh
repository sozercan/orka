#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ $# != 2 || ! "$1" =~ ^[a-fA-F0-9]{40}$ || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
  echo 'Usage: scripts/verify-acp-release-qualification.sh FULL_CANDIDATE_SHA WORKFLOW_RUN_ID' >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/live-acp-release-report.sh
. "${script_dir}/lib/live-acp-release-report.sh"
candidate="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
run_id="$2"
repository=orka-agents/orka
report_dir="$(mktemp -d "${TMPDIR:-/tmp}/acp-release-qualification.XXXXXX")"
trap 'rm -rf "${report_dir}"' EXIT

default_branch="$(gh api "repos/${repository}" --jq '.default_branch')"
gh api "repos/${repository}/actions/runs/${run_id}" >"${report_dir}/run.json"
if ! jq -e --arg sha "${candidate}" --arg branch "${default_branch}" --arg repo "${repository}" '
    .status == "completed" and .conclusion == "success"
    and .event == "workflow_dispatch" and .head_sha == $sha and .head_branch == $branch
    and .repository.full_name == $repo and .head_repository.full_name == $repo
    and .path == ".github/workflows/live-acp-release-gate.yml"
    and (.run_attempt | type == "number" and . > 0)
  ' "${report_dir}/run.json" >/dev/null; then
  echo 'Not qualified: require a successful default-branch Live ACP Release Gate run for this exact candidate.' >&2
  exit 1
fi
attempt="$(jq -r '.run_attempt' "${report_dir}/run.json")"
artifact="live-acp-release-acceptance-${run_id}-${attempt}"
gh api --paginate --slurp "repos/${repository}/actions/runs/${run_id}/artifacts?per_page=100" >"${report_dir}/artifacts.json"
if ! jq -e --arg name "${artifact}" '
    [.[] | .artifacts[] | select(.name == $name and .expired == false)] | length == 1
  ' "${report_dir}/artifacts.json" >/dev/null; then
  echo 'Not qualified: the current workflow attempt has no unique, unexpired acceptance report.' >&2
  exit 1
fi
gh run download "${run_id}" --repo "${repository}" --name "${artifact}" --dir "${report_dir}/download"
report="${report_dir}/download/acceptance.json"
if ! acp_report_qualified "${report}" || ! jq -e \
    --arg sha "${candidate}" --arg run "${run_id}" --arg attempt "${attempt}" \
    --arg repo "${repository}" --arg ref "refs/heads/${default_branch}" '
      .result == "qualified" and .candidateSHA == $sha and .checkoutSHA == $sha
      and .workflow.sha == $sha and .workflow.repository == $repo and .workflow.ref == $ref
      and .workflow.runID == $run and .workflow.runAttempt == $attempt
      and .workflow.event == "workflow_dispatch"
    ' "${report}" >/dev/null; then
  echo 'Not qualified: the report is incomplete or does not match the trusted candidate and workflow attempt.' >&2
  exit 1
fi

destination="${script_dir}/../bin/acp-release-qualification-${run_id}-${attempt}"
mkdir -p "${destination}"
cp "${report}" "${destination}/acceptance.json"
printf 'Qualified %s using https://github.com/%s/actions/runs/%s/attempts/%s\n' \
  "${candidate}" "${repository}" "${run_id}" "${attempt}"
printf 'Acceptance report: %s/acceptance.json\n' "${destination}"
