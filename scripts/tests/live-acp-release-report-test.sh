#!/usr/bin/env bash
# shellcheck disable=SC2016 # acp_report_update arguments are jq programs.
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/lib/live-acp-release-report.sh
. "${root}/scripts/lib/live-acp-release-report.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/acp-release-report-test.XXXXXX")"
trap 'rm -rf "${fixture}"' EXIT
export RELEASE_GATE=1 ACP_E2E_REPORT_FILE="${fixture}/acceptance.json"
ACP_E2E_WRITE_SOURCE_REF="$(git -C "${root}" rev-parse HEAD)"
export ACP_E2E_WRITE_SOURCE_REF
export ACP_E2E_WRITE_SOURCE_REPO=https://github.com/orka-agents/orka.git
export ACP_E2E_WRITE_PUBLICATION_REPO=https://github.com/sozercan/orka-acp-release-gate.git
export ACP_E2E_WRITE_PR_BASE=main ACP_E2E_RUN_ID=report-test
export GITHUB_REPOSITORY=orka-agents/orka GITHUB_REF=refs/heads/main
export GITHUB_SHA="${ACP_E2E_WRITE_SOURCE_REF}" GITHUB_RUN_ID="9999$$"
export GITHUB_RUN_ATTEMPT=1 GITHUB_EVENT_NAME=workflow_dispatch
acp_report_init "${root}"
if acp_report_finish 2>/dev/null; then
  echo 'an unexecuted release gate qualified' >&2
  exit 1
fi

sentinel='excluded-private-evidence'
commit=1111111111111111111111111111111111111111
tree=2222222222222222222222222222222222222222
digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
task_payload="$(jq -n --arg sha "${GITHUB_SHA}" --arg commit "${commit}" --arg tree "${tree}" \
  --arg digest "${digest}" --arg sentinel "${sentinel}" '{
    metadata:{namespace:"test",name:"write-test",uid:"task-uid",annotations:{private:$sentinel}},
    spec:{prompt:$sentinel},
    status:{phase:"Succeeded",message:$sentinel,result:$sentinel,
      execution:{state:"Succeeded",outcome:"Succeeded",attempt:1,promptID:"prompt-id",
        runtimePoolName:"write-pool",runtimePoolUID:"pool-uid",runtimeInstanceID:"instance-id",
        runtimeSessionUID:"session-uid",runtimeSessionGeneration:1,requestDigest:$digest,
        controllerEpoch:1,readCredentialResourceVersion:"1",publicationReadCredentialResourceVersion:"2",
        publicationCredentialResourceVersion:"3",forgeCredentialResourceVersion:"4",message:$sentinel},
      delivery:{state:"VerifiedExact",outcome:"VerifiedExact",publicationID:"publication-id",
        sourceRepository:{provider:"github",id:"github.com/orka-agents/orka",private:$sentinel},
        publicationRepository:{provider:"github",id:"github.com/sozercan/orka-acp-release-gate"},
        branch:"orka/acp-release-gate-test",startingSHA:$sha,remoteBeforeSHA:"",treeSHA:$tree,
        expectedCommitSHA:$commit,verifiedRemoteSHA:$commit,artifactDigest:$digest,message:$sentinel,
        prReceipt:{id:"github:123:42",number:42,url:"https://github.com/orka-agents/orka/pull/42",
          state:"Open",baseBranch:"main",headBranch:"orka/acp-release-gate-test",headSHA:$commit,
          private:$sentinel}}}
  }')"
acp_report_task "${task_payload}"
for role in controller publisher codex opencode claude copilot; do
  ref="registry.example/orka/${role}@${digest}"
  pod="$(jq -n --arg role "${role}" --arg ref "${ref}" --arg sentinel "${sentinel}" '{
    metadata:{namespace:"test",name:$role,uid:($role + "-uid"),annotations:{private:$sentinel}},
    spec:{containers:[{name:"runtime",image:$ref,env:[{name:"PRIVATE",value:$sentinel}]}]},
    status:{containerStatuses:[{name:"runtime",imageID:($ref | sub("registry.example";"docker-pullable://registry.example"))}]}
  }')"
  acp_report_image "${role}" "${pod}" runtime
  acp_report_update '.builtImages[$role] = $ref' --arg role "${role}" --arg ref "${ref}"
done
acp_report_update '
  .validation = "passed" | .validatorExitCode = 0 | .bootstrapExitCode = 0
  | .checks |= with_entries(.value = true)
  | .canary = {path:"canary.txt",expectedCommitBytesMatched:true,remoteBytesMatched:true,singleAddedFile:true}
  | .cleanup |= with_entries(.value = "passed")
  | .credentials = [
    {role:"sourceRead",namespace:"test",name:"source",resourceVersion:"1"},
    {role:"targetRead",namespace:"test",name:"target",resourceVersion:"2"},
    {role:"targetWrite",namespace:"test",name:"write",resourceVersion:"3"},
    {role:"forge",namespace:"test",name:"forge",resourceVersion:"4"}]
  | .observations = {
    baseHeads:{preflight:.candidateSHA,submission:.candidateSHA,publication:.candidateSHA,completion:.candidateSHA},
    remoteHead:$head,
    pullRequest:{number:42,state:"open",headSHA:$head,baseSHA:.candidateSHA,
      baseBranch:"main",headBranch:.task.delivery.branch,
      sourceRepository:.sourceRepository,publicationRepository:.publicationRepository}}
' --arg head "${commit}"
if grep -F "${sentinel}" "${ACP_E2E_REPORT_FILE}" >/dev/null; then
  echo 'acceptance report copied excluded Task or Pod content' >&2
  exit 1
fi
acp_report_finish
jq -e '.result == "qualified" and .finishedAt != null' "${ACP_E2E_REPORT_FILE}" >/dev/null
cp "${ACP_E2E_REPORT_FILE}" "${fixture}/qualified.json"
printf '%s\n' 'ok - complete publication evidence qualifies without Task content or credentials'

while IFS= read -r mutation; do
  jq "${mutation}" "${fixture}/qualified.json" >"${ACP_E2E_REPORT_FILE}"
  if acp_report_finish 2>/dev/null; then
    printf 'incomplete or inconsistent report qualified: %s\n' "${mutation}" >&2
    exit 1
  fi
  jq -e '.result == "not_qualified"' "${ACP_E2E_REPORT_FILE}" >/dev/null
done <<'MUTATIONS'
.mode = "smoke"
.candidateSHA = "0000000000000000000000000000000000000000"
.sourceRepository = .publicationRepository
.validation = "not_started"
.validatorExitCode = 1
.bootstrapExitCode = 1
.checks.publication = false
.canary.remoteBytesMatched = false
.canary.expectedCommitBytesMatched = false
.canary.singleAddedFile = false
.checks.publisherSecretReadDenied = false
.observations.baseHeads.submission = "0000000000000000000000000000000000000000"
.observations.baseHeads.completion = "0000000000000000000000000000000000000000"
.observations.remoteHead = "0000000000000000000000000000000000000000"
.observations.pullRequest.number = 43
.observations.pullRequest.headBranch = "somebody-elses-branch"
.observations.pullRequest.sourceRepository = .publicationRepository
.task.delivery.prReceipt = null
.task.delivery.artifactDigest = null
.task.execution.attempt = 2
.task.execution.forgeCredentialResourceVersion = "rotated"
.credentials = []
.credentials[1].name = .credentials[0].name
.images.publisher.imageID = null | .images.publisher.actualDigest = null
.images.codex.verified = false
del(.images.copilot)
.builtImages.controller = "registry.example/unqualified@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
.cleanup.remote = "preserved"
.cleanup.kubernetes = "failed"
.cleanup.validatorCredentials = "failed"
.cleanup.bootstrapCredentials = "failed"
.cleanup.cluster = "pending"
.cleanup.registry = "failed"
.preserved = {branch:"inspect-me"}
MUTATIONS
printf '%s\n' 'ok - skipped publication, moved base, inconsistent receipts, missing images and incomplete cleanup fail qualification'

ACP_E2E_WRITE_SOURCE_REPO="https://${sentinel}@github.com/orka-agents/orka" \
  ACP_E2E_WRITE_SOURCE_REF="${sentinel}" acp_report_init "${root}"
jq -e '.candidateSHA == null and .sourceRepository == null' "${ACP_E2E_REPORT_FILE}" >/dev/null
if grep -F "${sentinel}" "${ACP_E2E_REPORT_FILE}" >/dev/null; then
  echo 'invalid inputs leaked into the failure report' >&2
  exit 1
fi
printf '%s\n' 'ok - invalid source URLs and candidate inputs are omitted from failure evidence'

mkdir "${fixture}/bin"
export QUALIFICATION_FIXTURE="${fixture}"
jq -n --arg sha "${GITHUB_SHA}" '{status:"completed",conclusion:"success",event:"workflow_dispatch",
  head_sha:$sha,head_branch:"main",repository:{full_name:"orka-agents/orka"},
  head_repository:{full_name:"orka-agents/orka"},path:".github/workflows/live-acp-release-gate.yml",run_attempt:1}' \
  >"${fixture}/run-original.json"
jq -n --arg name "live-acp-release-acceptance-${GITHUB_RUN_ID}-1" \
  '[{artifacts:[{name:$name,expired:false}]}]' >"${fixture}/artifacts.json"
cat >"${fixture}/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "$1" == api ]]; then
  case "$*" in
    *'/artifacts?'*) cat "${QUALIFICATION_FIXTURE}/artifacts.json" ;;
    *'/actions/runs/'*) cat "${QUALIFICATION_FIXTURE}/run.json" ;;
    *) printf 'main\n' ;;
  esac
elif [[ "$1 $2" == 'run download' ]]; then
  while [[ "$1" != --dir ]]; do shift; done
  mkdir -p "$2"
  cp "${QUALIFICATION_FIXTURE}/qualified.json" "$2/acceptance.json"
else
  exit 99
fi
STUB
chmod +x "${fixture}/bin/gh"
cp "${fixture}/run-original.json" "${fixture}/run.json"
PATH="${fixture}/bin:${PATH}" bash "${root}/scripts/verify-acp-release-qualification.sh" \
  "${GITHUB_SHA}" "${GITHUB_RUN_ID}" >/dev/null
rm -rf "${root}/bin/acp-release-qualification-${GITHUB_RUN_ID}-1"
while IFS= read -r mutation; do
  jq "${mutation}" "${fixture}/run-original.json" >"${fixture}/run.json"
  if PATH="${fixture}/bin:${PATH}" bash "${root}/scripts/verify-acp-release-qualification.sh" \
      "${GITHUB_SHA}" "${GITHUB_RUN_ID}" >/dev/null 2>&1; then
    printf 'untrusted workflow metadata qualified: %s\n' "${mutation}" >&2
    exit 1
  fi
done <<'MUTATIONS'
.event = "pull_request"
.event = "schedule"
.head_branch = "topic"
.head_sha = "0000000000000000000000000000000000000000"
.head_repository.full_name = "external/orka"
.path = ".github/workflows/live-acp-runtime-e2e.yml"
.conclusion = "failure"
.status = "in_progress"
.run_attempt = 2
MUTATIONS
printf '%s\n' 'ok - qualification requires the exact trusted workflow, commit, successful run and current attempt artifact'
