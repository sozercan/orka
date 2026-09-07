#!/usr/bin/env bash
# shellcheck disable=SC2034 # Variables are consumed by the extracted validator functions.
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${root}/scripts/live-acp-runtime-e2e.sh"
# shellcheck source=scripts/lib/live-acp-release-report.sh
. "${root}/scripts/lib/live-acp-release-report.sh"
for function in is_sha is_uint lower verify_remote_publication cleanup_remote_effects \
  validate_exact_pull_request ensure_pull_request_closed_unmerged; do
  eval "$(awk -v name="${function}" '$0 == name "() {" {copy=1} copy {print} copy && /^}$/ {exit}' "${script}")"
done

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/acp-publication-test.XXXXXX")"
trap 'rm -rf "${temp_root}"' EXIT
export RELEASE_GATE=1 ACP_E2E_REPORT_FILE="${temp_root}/acceptance.json"
ACP_E2E_WRITE_SOURCE_REF="$(git -C "${root}" rev-parse HEAD)"
export ACP_E2E_WRITE_SOURCE_REF
export ACP_E2E_WRITE_SOURCE_REPO=https://github.com/orka-agents/orka.git
export ACP_E2E_WRITE_PUBLICATION_REPO=https://github.com/sozercan/orka-acp-release-gate.git
write_source_slug=orka-agents/orka
write_publication_slug=sozercan/orka-acp-release-gate
write_source_identity="github.com/${write_source_slug}"
write_publication_identity="github.com/${write_publication_slug}"
write_source_database_id=123
write_source_commit="${ACP_E2E_WRITE_SOURCE_REF}"
write_branch=orka/acp-release-gate-test
write_pr_base=main
write_expected_file=canary-test.txt
write_expected_content='ACP v2 release gate test'
head=1111111111111111111111111111111111111111
tree=2222222222222222222222222222222222222222
other=3333333333333333333333333333333333333333
fault=""
printf 'open\n' >"${temp_root}/pr-state"

die() { printf '%s\n' "$*" >&2; exit 1; }
warn() { printf '%s\n' "$*" >&2; }
log() { :; }
github_ref_lookup() {
  [[ "${fault}" != read_failure ]] || return 1
  github_ref_lookup_state=present
  github_ref_lookup_sha="${head}"
  if [[ -f "${temp_root}/ref-state" ]]; then
    github_ref_lookup_state="$(cat "${temp_root}/ref-state")"
  fi
  [[ "${fault}" != wrong_head ]] || github_ref_lookup_sha="${other}"
}
gh_raw_file() {
  if [[ "${fault}" == wrong_content ]]; then
    printf 'incorrect canary bytes\n'
  else
    printf '%s\n' "${write_expected_content}"
  fi
}
gh() {
  local endpoint="${*: -1}" parent="${write_source_commit}" observed_tree="${tree}"
  local observed_commit="${head}"
  local base="${write_source_commit}" branch="${write_branch}" number=42 merged=null
  if [[ "$*" == *'--method PATCH'* ]]; then
    printf 'close_pr\n' >>"${temp_root}/events"
    printf 'closed\n' >"${temp_root}/pr-state"
    return 0
  fi
  case "${endpoint}" in
    "repos/${write_publication_slug}/git/commits/${head}")
      [[ "${fault}" != wrong_commit ]] || observed_commit="${other}"
      [[ "${fault}" != wrong_parent ]] || parent="${other}"
      [[ "${fault}" != wrong_tree ]] || observed_tree="${other}"
      jq -n --arg sha "${observed_commit}" --arg parent "${parent}" --arg tree "${observed_tree}" \
        '{sha:$sha,parents:[{sha:$parent}],tree:{sha:$tree}}'
      ;;
    "repos/${write_publication_slug}/compare/${write_source_commit}...${head}")
      jq -n --arg path "${write_expected_file}" '{status:"ahead",files:[{filename:$path,status:"added"}]}'
      ;;
    "repos/${write_publication_slug}/compare/${head}...${head}")
      jq -n --arg sha "${head}" '{status:"identical",merge_base_commit:{sha:$sha}}'
      ;;
    "repos/${write_source_slug}/branches/${write_pr_base}")
      [[ "${fault}" != moved_base ]] || base="${other}"
      jq -n --arg sha "${base}" '{commit:{sha:$sha}}'
      ;;
    "repos/${write_source_slug}/pulls/42/files?per_page=100")
      jq -n --arg path "${write_expected_file}" '[[{filename:$path,status:"added"}]]'
      ;;
    "repos/${write_source_slug}/pulls/42")
      [[ "${fault}" != wrong_pr ]] || number=43
      [[ "${fault}" != wrong_branch ]] || branch=someone-elses-branch
      [[ "${fault}" != merged_pr ]] || merged='"2026-01-01T00:00:00Z"'
      jq -n --argjson number "${number}" --arg state "$(cat "${temp_root}/pr-state")" \
        --arg base "${base}" --arg branch "${branch}" --arg head "${head}" --argjson merged "${merged}" \
        --arg source "${write_source_slug}" --arg publication "${write_publication_slug}" '{
          number:$number,state:$state,merged_at:$merged,
          base:{sha:$base,ref:"main",repo:{full_name:$source}},
          head:{sha:$head,ref:$branch,repo:{full_name:$publication}}}'
      ;;
    *) printf 'unexpected GitHub fixture endpoint\n' >&2; return 97 ;;
  esac
}
payload="$(jq -n --arg base "${write_source_commit}" --arg head "${head}" --arg tree "${tree}" \
  --arg branch "${write_branch}" --arg source "${write_source_identity}" --arg publication "${write_publication_identity}" '{
    status:{delivery:{outcome:"VerifiedExact",publicationID:"pub-id",startingSHA:$base,treeSHA:$tree,
      expectedCommitSHA:$head,verifiedRemoteSHA:$head,remoteBeforeSHA:"",
      artifactDigest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      branch:$branch,sourceRepository:{provider:"github",id:$source},
      publicationRepository:{provider:"github",id:$publication},
      prReceipt:{number:42,id:"github:123:42",url:"https://github.com/orka-agents/orka/pull/42",
        state:"Open",baseBranch:"main",headBranch:$branch,headSHA:$head}}}
  }')"
acp_report_init "${root}"
verify_remote_publication "${payload}"
jq -e --arg sha "${head}" --arg tree "${tree}" '
  .checks.publication == true and .canary.remoteBytesMatched == true
  and .observations.expectedCommit == {sha:$sha,treeSHA:$tree}
' "${ACP_E2E_REPORT_FILE}" >/dev/null
for fault in wrong_content wrong_commit wrong_parent wrong_tree wrong_head wrong_branch wrong_pr moved_base read_failure; do
  acp_report_init "${root}"
  if (verify_remote_publication "${payload}") >"${temp_root}/failure.log" 2>&1; then
    printf 'publication accepted invalid independent evidence: %s\n' "${fault}" >&2
    exit 1
  fi
  jq -e '.checks.publication == false and .result == "not_qualified"' "${ACP_E2E_REPORT_FILE}" >/dev/null
done
printf '%s\n' 'ok - deployed publication verification rejects wrong bytes, parent, tree, head, branch, PR, moved base and failed reads'

release_gate=1
remote_cleanup_required=1
remote_cleanup_branch="${write_branch}"
remote_cleanup_publication_slug="${write_publication_slug}"
remote_cleanup_publication_repo="${ACP_E2E_WRITE_PUBLICATION_REPO}"
remote_cleanup_source_slug="${write_source_slug}"
remote_cleanup_pr_base=main
remote_cleanup_head="${head}"
remote_cleanup_pr_number=42
git_askpass_file="${temp_root}/unused-askpass"
gh_token_file="${temp_root}/unused-token"
git_observer_repo="${temp_root}/unused-observer"
settle_write_task_for_remote_cleanup() {
  printf 'settle\n' >>"${temp_root}/events"
  [[ "${fault}" != active_writer ]]
}
configure_git_observer_auth() { :; }
release_write_task_observer() { printf 'release_observer\n' >>"${temp_root}/events"; }
git() {
  if [[ "$*" == *'rev-parse HEAD'* ]]; then command git "$@"; return; fi
  [[ "$*" == *"--force-with-lease=refs/heads/${write_branch}:${head}"* ]] || return 98
  [[ "$*" == *":refs/heads/${write_branch}"* ]] || return 98
  printf 'delete_branch\n' >>"${temp_root}/events"
  [[ "${fault}" != failed_lease ]] || return 1
  printf 'absent\n' >"${temp_root}/ref-state"
}
for fault in none active_writer wrong_head wrong_branch wrong_pr merged_pr read_failure failed_lease; do
  acp_report_init "${root}"
  remote_cleanup_preserve=0
  : >"${temp_root}/events"
  printf 'present\n' >"${temp_root}/ref-state"
  printf 'open\n' >"${temp_root}/pr-state"
  if cleanup_remote_effects >"${temp_root}/cleanup.log" 2>&1; then
    [[ "${fault}" == none ]] || { echo "unsafe cleanup accepted ${fault}" >&2; exit 1; }
    printf 'settle\nclose_pr\ndelete_branch\nrelease_observer\n' >"${temp_root}/expected-events"
    cmp "${temp_root}/events" "${temp_root}/expected-events"
    jq -e '.cleanup.remote == "passed"' "${ACP_E2E_REPORT_FILE}" >/dev/null
  else
    [[ "${fault}" != none && "${remote_cleanup_preserve}" == 1 ]] || exit 1
    [[ "$(cat "${temp_root}/ref-state")" == present ]] || exit 1
    if [[ "${fault}" != failed_lease ]]; then
      [[ "$(cat "${temp_root}/events")" == settle ]] || {
        echo "cleanup mutated a PR or branch without safe evidence: ${fault}" >&2
        exit 1
      }
    fi
  fi
done
printf '%s\n' 'ok - cleanup settles writers before PR closure, uses an exact-head lease and preserves ambiguous remote effects'
