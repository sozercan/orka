#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

usage() {
  echo "Usage: $0 OVERLAY_DIR KUSTOMIZE KUBECTL" >&2
}

[[ $# -eq 3 ]] || {
  usage
  exit 2
}

overlay_dir="$1"
kustomize="$2"
kubectl="$3"

for command in "${kustomize}" "${kubectl}" base64 cmp curl dd jq openssl sleep tr wc; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "required command not found: ${command}" >&2
    exit 1
  }
done
[[ -d "${overlay_dir}" && ! -L "${overlay_dir}" ]] || {
  echo "ACP production overlay must be a real directory: ${overlay_dir}" >&2
  exit 1
}
admission_webhooks_dir="${overlay_dir}/../orka-admission-webhooks"
[[ -d "${admission_webhooks_dir}" && ! -L "${admission_webhooks_dir}" ]] || {
  echo "admission webhook base must be a real sibling directory: ${admission_webhooks_dir}" >&2
  exit 1
}

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/apply-acp-production.XXXXXX")"
admission_proxy_pid=""
stop_admission_proxy() {
  if [[ -n "${admission_proxy_pid}" ]]; then
    kill "${admission_proxy_pid}" 2>/dev/null || true
    wait "${admission_proxy_pid}" 2>/dev/null || true
    admission_proxy_pid=""
  fi
}
cleanup() {
  stop_admission_proxy
  rm -rf "${work_dir}"
}
trap cleanup EXIT

manifest="${work_dir}/manifest.yaml"
rendered_json="${work_dir}/rendered.json"
namespace_resource="${work_dir}/namespace.json"
namespace_metadata_patch="${work_dir}/namespace-metadata-patch.json"
existing_namespace="${work_dir}/existing-namespace.json"
existing_controller="${work_dir}/existing-controller.json"
runtime_config="${work_dir}/runtime-images-configmap.json"
admission_runtime_manifest="${work_dir}/admission-runtime-manifest.json"
workload_manifest="${work_dir}/workload-manifest.json"
workload_prerequisite_manifest="${work_dir}/workload-prerequisite-manifest.json"
controller_manifest="${work_dir}/controller-manifest.json"
workload_dependency_endpoints="${work_dir}/workload-dependency-endpoints.json"
snapshot_secret="${work_dir}/agent-execution-snapshot-key.json"
snapshot_key="${work_dir}/snapshot-key"
snapshot_key_data="${work_dir}/snapshot-key-data"
snapshot_key_encoded="${work_dir}/snapshot-key-encoded"
snapshot_key_decoded="${work_dir}/snapshot-key-decoded"
admission_tls_secret="${work_dir}/orka-admission-tls.json"
admission_tls_cert="${work_dir}/orka-admission-tls.crt"
admission_tls_key="${work_dir}/orka-admission-tls.key"
admission_ca_cert="${work_dir}/orka-admission-ca.crt"
admission_tls_cert_public_key="${work_dir}/orka-admission-tls-cert.pub"
admission_tls_key_public_key="${work_dir}/orka-admission-tls-key.pub"
admission_endpoints="${work_dir}/orka-admission-endpoints.json"
admission_webhooks_source="${work_dir}/orka-admission-webhooks.yaml"
admission_webhooks_rendered="${work_dir}/orka-admission-webhooks-rendered.json"
admission_webhooks_manifest="${work_dir}/orka-admission-webhooks.json"
admission_smoke_request="${work_dir}/orka-admission-smoke-request.json"
admission_smoke_response="${work_dir}/orka-admission-smoke-response.json"
admission_proxy_log="${work_dir}/kubectl-proxy.log"
admission_proxy_port=""
"${kustomize}" build "${overlay_dir}" >"${manifest}"
"${kubectl}" create --dry-run=client --validate=false -f "${manifest}" -o json >"${rendered_json}"
"${kustomize}" build "${admission_webhooks_dir}" >"${admission_webhooks_source}"
"${kubectl}" create --dry-run=client --validate=false -f "${admission_webhooks_source}" -o json >"${admission_webhooks_rendered}"

jq -sc '
  [.[] | if .kind == "List" then .items[] else . end]
  | map(select(.kind == "Namespace" and .metadata.name == "orka-system"))
  | if length == 1 and .[0].metadata.labels["orka.ai/controller-mode"] == "harness-v2"
    then .[0]
    else error("expected exactly one orka-system Namespace claimed by harness-v2")
    end
' "${rendered_json}" >"${namespace_resource}"
jq -e '
  def json_pointer_escape: gsub("~"; "~0") | gsub("/"; "~1");
  .metadata.labels as $labels
  | if ($labels | type) != "object" or $labels["orka.ai/controller-mode"] != "harness-v2"
    then error("namespace metadata patch requires the harness-v2 mode claim")
    else [
      {
        op: "test",
        path: "/metadata/labels/orka.ai~1controller-mode",
        value: "harness-v2"
      },
      ($labels | to_entries[]
        | select(.key != "orka.ai/controller-mode")
        | {
            op: "add",
            path: ("/metadata/labels/" + (.key | json_pointer_escape)),
            value: .value
          })
    ]
    end
' "${namespace_resource}" >"${namespace_metadata_patch}"
jq -sc '
  [.[] | if .kind == "List" then .items[] else . end]
  | map(select(.kind == "ConfigMap" and .metadata.labels["orka.ai/acp-runtime-images"] == "true"))
  | if length == 1 then .[0] else error("expected exactly one generated ACP runtime image ConfigMap") end
' "${rendered_json}" >"${runtime_config}"
jq -sc '
  [.[] | if .kind == "List" then .items[] else . end]
  | {
      apiVersion: "v1",
      kind: "List",
      items: map(select(.metadata.labels["app.kubernetes.io/component"] == "admission"))
    }
' "${rendered_json}" >"${admission_runtime_manifest}"
jq -sc '
  [.[] | if .kind == "List" then .items[] else . end]
  | {
      apiVersion: "v1",
      kind: "List",
      items: map(select(
        .metadata.labels["app.kubernetes.io/component"] != "admission" and
        (.kind != "Namespace" or .metadata.name != "orka-system") and
        (.kind != "ConfigMap" or .metadata.labels["orka.ai/acp-runtime-images"] != "true")
      ))
    }
' "${rendered_json}" >"${workload_manifest}"
jq '
  {
    apiVersion: "v1",
    kind: "List",
    items: [.items[] | select((
      .apiVersion == "apps/v1" and
      .kind == "Deployment" and
      .metadata.namespace == "orka-system" and
      .metadata.name == "orka-controller-manager"
    ) | not)]
  }
' "${workload_manifest}" >"${workload_prerequisite_manifest}"
jq '
  [.items[] | select(
    .apiVersion == "apps/v1" and
    .kind == "Deployment" and
    .metadata.namespace == "orka-system" and
    .metadata.name == "orka-controller-manager"
  )]
  | if length == 1 then
      {apiVersion: "v1", kind: "List", items: .}
    else
      error("expected exactly one harness-v2 controller Deployment")
    end
' "${workload_manifest}" >"${controller_manifest}"
jq -e '
  ([.items[] | select(
    .apiVersion == "apps/v1" and
    .kind == "Deployment" and
    .metadata.namespace == "orka-system" and
    (.metadata.name == "orka-provider-auth-proxy" or
     .metadata.name == "orka-scm-egress-proxy" or
     .metadata.name == "orka-workspace-publisher")
  ) | .metadata.name] | sort) == [
    "orka-provider-auth-proxy",
    "orka-scm-egress-proxy",
    "orka-workspace-publisher"
  ] and
  ([.items[] | select(
    .apiVersion == "apps/v1" and
    .kind == "Deployment" and
    .metadata.namespace == "orka-system" and
    .metadata.name == "orka-controller-manager"
  )] | length) == 0
' "${workload_prerequisite_manifest}" >/dev/null || {
  echo "workload prerequisite wave must contain each publisher/proxy Deployment exactly once and no controller Deployment" >&2
  exit 1
}
jq -esc '
  [.[] | if .kind == "List" then .items[] else . end] as $items
  | ($items | map(select(.apiVersion == "apps/v1" and .kind == "Deployment" and
      .metadata.namespace == "orka-system" and .metadata.name == "orka-controller-manager"))) as $controllers
  | ($items | map(select(.apiVersion == "apps/v1" and .kind == "Deployment" and
      .metadata.namespace == "orka-system" and .metadata.name == "orka-admission"))) as $deployments
  | ($items | map(select(.apiVersion == "v1" and .kind == "Service" and
      .metadata.namespace == "orka-system" and .metadata.name == "orka-admission"))) as $services
  | ($controllers[0].spec.template.spec.containers[]? | select(.name == "manager") | .args) as $args
  | ($controllers | length) == 1 and
    ($controllers[0].spec.template.spec.containers | map(select(.name == "manager")) | length) == 1 and
    ([$args[] | select(startswith("--controller-mode="))] | length) == 1 and
    ([$args[] | select(. == "--controller-mode=harness-v2")] | length) == 1 and
    ([$args[] | select(startswith("--watch-namespace="))] | length) == 1 and
    ([$args[] | select(. == "--watch-namespace=orka-system")] | length) == 1 and
    ([$args[] | select(. == "--leader-elect" or . == "--leader-elect=true")] | length) == 1 and
    ([$args[] | select(startswith("--harness-v1-"))] | length) == 0 and
    ($controllers[0].spec.template.spec.containers | any(
      .name == "manager" and
      (.image | test("@sha256:[a-f0-9]{64}$"))
    )) and
    ($items | map(select(
      .metadata.labels["app.kubernetes.io/component"] == "agent-harness-wrapper" or
      (.kind == "Deployment" and (.metadata.name | endswith("agent-harness-wrapper")))
    )) | length) == 0 and
    ($deployments | length) == 1 and
    ($services | length) == 1 and
    ($deployments[0].spec.replicas >= 2) and
    ($deployments[0].spec.strategy.type == "RollingUpdate") and
    ($deployments[0].spec.strategy.rollingUpdate.maxUnavailable == 0) and
    ($deployments[0].spec.template.spec.containers | any(
      .name == "admission" and
      (.image | test("@sha256:[a-f0-9]{64}$"))
    ))
' "${rendered_json}" >/dev/null || {
  echo "production overlay must contain one digest-pinned static harness-v2 controller, no v1 wrapper path, and one digest-pinned, zero-unavailable, replicated orka-admission runtime and Service" >&2
  exit 1
}

validate_existing_controller_identity() {
  : >"${existing_namespace}"
  if ! "${kubectl}" get namespace orka-system --ignore-not-found -o json >"${existing_namespace}"; then
    echo "unable to inspect the existing orka-system Namespace" >&2
    return 1
  fi
  : >"${existing_controller}"
  if ! "${kubectl}" -n orka-system get deployment orka-controller-manager \
    --ignore-not-found -o json >"${existing_controller}"; then
    echo "unable to inspect the existing orka-system/orka-controller-manager Deployment" >&2
    return 1
  fi

  if [[ ! -s "${existing_namespace}" && ! -s "${existing_controller}" ]]; then
    return 0
  fi
  if [[ ! -s "${existing_namespace}" ]] || ! jq -e '
    .apiVersion == "v1" and
    .kind == "Namespace" and
    .metadata.name == "orka-system" and
    .metadata.labels["orka.ai/controller-mode"] == "harness-v2"
  ' "${existing_namespace}" >/dev/null; then
    echo "existing namespace orka-system must already claim orka.ai/controller-mode=harness-v2; unlabeled, implicit, legacy, or opposite-mode namespaces cannot be adopted in place" >&2
    return 1
  fi
  [[ -s "${existing_controller}" ]] || return 0

  if ! jq -e '
    .apiVersion == "apps/v1" and
    .kind == "Deployment" and
    .metadata.namespace == "orka-system" and
    .metadata.name == "orka-controller-manager" and
    ([.spec.template.spec.containers[] | select(.name == "manager")] | length) == 1 and
    ([.spec.template.spec.containers[] | select(.name == "manager") | .args[]? |
      select(startswith("--controller-mode="))] == ["--controller-mode=harness-v2"]) and
    ([.spec.template.spec.containers[] | select(.name == "manager") | .args[]? |
      select(startswith("--watch-namespace="))] == ["--watch-namespace=orka-system"])
  ' "${existing_controller}" >/dev/null; then
    echo "implicit, legacy, differently scoped, or opposite-mode controllers cannot be upgraded in place; settle or retire the existing installation and deploy harness-v2 in a fresh namespace" >&2
    return 1
  fi
}

validate_snapshot_secret() {
  if ! jq -er '.data["snapshot-key"] | select(type == "string" and length > 0)' "${snapshot_secret}" \
    | base64 -d >"${snapshot_key_data}" 2>/dev/null; then
    echo "agent-execution-snapshot-key Secret must contain snapshot-key" >&2
    return 1
  fi

  local encoded_key key_size
  key_size="$(wc -c <"${snapshot_key_data}" | tr -d '[:space:]')"
  if [[ "${key_size}" == "32" ]]; then
    return 0
  fi

  # Match the controller parser: raw input is accepted only at exactly 32
  # bytes. Otherwise trim surrounding whitespace from base64 text, while
  # retaining Go's allowance for embedded CR/LF line wrapping.
  encoded_key="$(<"${snapshot_key_data}")"
  while [[ -n "${encoded_key}" && "${encoded_key}" == [[:space:]]* ]]; do
    encoded_key="${encoded_key:1}"
  done
  while [[ -n "${encoded_key}" && "${encoded_key}" == *[[:space:]] ]]; do
    encoded_key="${encoded_key:0:${#encoded_key}-1}"
  done
  encoded_key="${encoded_key//$'\r'/}"
  encoded_key="${encoded_key//$'\n'/}"
  if [[ "${encoded_key}" =~ ^[A-Za-z0-9+/]{43}=$ ]]; then
    printf '%s' "${encoded_key}" >"${snapshot_key_encoded}"
  else
    : >"${snapshot_key_encoded}"
  fi
  if [[ "$(wc -c <"${snapshot_key_encoded}" | tr -d '[:space:]')" == "44" ]] \
    && base64 -d <"${snapshot_key_encoded}" >"${snapshot_key_decoded}" 2>/dev/null \
    && [[ "$(wc -c <"${snapshot_key_decoded}" | tr -d '[:space:]')" == "32" ]]; then
    return 0
  fi

  echo "agent-execution-snapshot-key/snapshot-key must contain exactly 32 raw bytes or their base64 encoding" >&2
  return 1
}

ensure_snapshot_secret() {
  if ! "${kubectl}" -n orka-system get secret agent-execution-snapshot-key --ignore-not-found -o json >"${snapshot_secret}"; then
    echo "unable to inspect orka-system/agent-execution-snapshot-key" >&2
    return 1
  fi
  if [[ ! -s "${snapshot_secret}" ]]; then
    umask 077
    dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\r\n' >"${snapshot_key}"
    "${kubectl}" -n orka-system create secret generic agent-execution-snapshot-key \
      --from-file="snapshot-key=${snapshot_key}" >/dev/null
    "${kubectl}" -n orka-system get secret agent-execution-snapshot-key -o json >"${snapshot_secret}"
  fi
  validate_snapshot_secret
}

validate_admission_tls_secret() {
  local service_dns="orka-admission.orka-system.svc"
  if ! "${kubectl}" -n orka-system get secret orka-admission-tls -o json >"${admission_tls_secret}"; then
    echo "orka-system/orka-admission-tls is required before admission rollout" >&2
    return 1
  fi
  if ! jq -e '
    .type == "kubernetes.io/tls" and
    (.data["tls.crt"] | type == "string" and length > 0) and
    (.data["tls.key"] | type == "string" and length > 0) and
    (.data["ca.crt"] | type == "string" and length > 0)
  ' "${admission_tls_secret}" >/dev/null; then
    echo "orka-system/orka-admission-tls must be a TLS Secret containing tls.crt, tls.key, and ca.crt" >&2
    return 1
  fi
  local key output san_extension
  for key in tls.crt tls.key ca.crt; do
    case "${key}" in
      tls.crt) output="${admission_tls_cert}" ;;
      tls.key) output="${admission_tls_key}" ;;
      ca.crt) output="${admission_ca_cert}" ;;
    esac
    if ! jq -er --arg key "${key}" '.data[$key]' "${admission_tls_secret}" \
      | base64 -d >"${output}" 2>/dev/null || [[ ! -s "${output}" ]]; then
      echo "orka-system/orka-admission-tls contains invalid ${key} data" >&2
      return 1
    fi
  done

  if ! openssl x509 -in "${admission_tls_cert}" -noout >/dev/null 2>&1 \
    || ! openssl pkey -in "${admission_tls_key}" -passin pass: -noout >/dev/null 2>&1 \
    || ! openssl x509 -in "${admission_ca_cert}" -noout >/dev/null 2>&1; then
    echo "orka-system/orka-admission-tls must contain parseable PEM certificate, private-key, and CA material" >&2
    return 1
  fi
  if ! openssl x509 -in "${admission_tls_cert}" -pubkey -noout \
      >"${admission_tls_cert_public_key}" 2>/dev/null \
    || ! openssl pkey -in "${admission_tls_key}" -passin pass: -pubout \
      >"${admission_tls_key_public_key}" 2>/dev/null \
    || ! cmp -s "${admission_tls_cert_public_key}" "${admission_tls_key_public_key}"; then
    echo "orka-system/orka-admission-tls tls.crt and tls.key do not form a key pair" >&2
    return 1
  fi
  if ! san_extension="$(openssl x509 -in "${admission_tls_cert}" -noout -ext subjectAltName 2>/dev/null)" \
    || [[ "${san_extension}" != *"X509v3 Subject Alternative Name"* ]] \
    || ! openssl x509 -in "${admission_tls_cert}" -noout -checkhost "${service_dns}" >/dev/null 2>&1; then
    echo "orka-system/orka-admission-tls tls.crt must contain a subjectAltName for ${service_dns}" >&2
    return 1
  fi
  if ! openssl verify -x509_strict -purpose sslserver -verify_hostname "${service_dns}" \
      -CAfile "${admission_ca_cert}" -untrusted "${admission_tls_cert}" \
      "${admission_tls_cert}" >/dev/null 2>&1; then
    echo "orka-system/orka-admission-tls tls.crt must be currently valid for ${service_dns} and chain to ca.crt" >&2
    return 1
  fi
}

render_admission_webhooks() {
  jq -sc --slurpfile tls "${admission_tls_secret}" '
    ($tls[0].data["ca.crt"]) as $ca
    | [.[] | if .kind == "List" then .items[] else . end]
    | {
        apiVersion: "v1",
        kind: "List",
        items: map(
          if .apiVersion == "admissionregistration.k8s.io/v1" and .kind == "ValidatingWebhookConfiguration"
          then
            .metadata.annotations = ((.metadata.annotations // {}) | del(."cert-manager.io/inject-ca-from-secret"))
            | .webhooks |= map(.clientConfig.caBundle = $ca)
          else . end
        )
      }
  ' "${admission_webhooks_rendered}" >"${admission_webhooks_manifest}"

  jq -e --slurpfile tls "${admission_tls_secret}" '
    ($tls[0].data["ca.crt"]) as $ca |
    ([.items[] | select(.kind == "ValidatingAdmissionPolicy")] | length) == 0 and
    ([.items[] | select(.kind == "ValidatingAdmissionPolicyBinding")] | length) == 0 and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration")] | length) == 1 and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration") | .webhooks[]] | length) == 9 and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration") | .webhooks[].name] | unique | length) == 9 and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration") | .webhooks[] |
      (.failurePolicy == "Fail" and
       .sideEffects == "None" and
       .clientConfig.caBundle == $ca and
       .clientConfig.service.name == "orka-admission" and
       .clientConfig.service.namespace == "orka-system" and
       (.clientConfig.service.path | type == "string" and length > 0))] | all)
    and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration") | .webhooks[] |
      select(.name == "workspaceattachmentsecret.core.orka.ai" and
             .clientConfig.service.path == "/validate-v1-secret-workspace-attachment" and
             .rules == [{"operations":["CREATE","UPDATE","DELETE"],"apiGroups":[""],"apiVersions":["v1"],"resources":["secrets"],"scope":"Namespaced"}] and
             .objectSelector.matchExpressions == [{"key":"workspace.orka.ai/attachment-for","operator":"Exists"}])] | length) == 1 and
    ([.items[] | select(.kind == "ValidatingWebhookConfiguration") | .webhooks[] |
      select(.name == "acpsuspendquotalease.core.orka.ai" and
             .clientConfig.service.path == "/validate-coordination-k8s-io-v1-acp-suspend-quota-lease" and
             .rules == [{"operations":["CREATE","UPDATE","DELETE"],"apiGroups":["coordination.k8s.io"],"apiVersions":["v1"],"resources":["leases"],"scope":"Namespaced"}] and
             .matchConditions == [{"name":"reserved-acp-workspace-lease-name","expression":"request.?name.orValue(\u0027\u0027).startsWith(\u0027acp-suspend-quota-\u0027) || request.?name.orValue(\u0027\u0027).startsWith(\u0027acp-retention-fence-\u0027) || (request.operation == \u0027CREATE\u0027 && (object.metadata.?generateName.orValue(\u0027\u0027).startsWith(\u0027acp-suspend-quota-\u0027) || object.metadata.?generateName.orValue(\u0027\u0027).startsWith(\u0027acp-retention-fence-\u0027)))"}])] | length) == 1
  ' "${admission_webhooks_manifest}" >/dev/null || {
    echo "admission wave must contain exactly nine unique, fail-closed, CA-pinned orka-admission webhooks, including attachment Secret and ACP workspace coordination Lease protection, and no legacy coexistence policies" >&2
    return 1
  }
}

wait_for_admission_endpoints() {
  "${kubectl}" -n orka-system rollout status deployment/orka-admission --timeout=2m >/dev/null
  if ! "${kubectl}" -n orka-system get endpoints orka-admission -o json >"${admission_endpoints}" \
    || ! jq -e '[.subsets[]?.addresses[]?.ip] | unique | length >= 2' "${admission_endpoints}" >/dev/null; then
    echo "orka-admission Service must expose at least two ready endpoints" >&2
    return 1
  fi
}

wait_for_workload_dependencies() {
  local deployment service attempt ready
  local -a dependencies=(
    orka-provider-auth-proxy
    orka-scm-egress-proxy
    orka-workspace-publisher
  )

  for deployment in "${dependencies[@]}"; do
    "${kubectl}" -n orka-system rollout status "deployment/${deployment}" --timeout=2m >/dev/null
  done

  for service in "${dependencies[@]}"; do
    ready=0
    for attempt in {1..50}; do
      if "${kubectl}" -n orka-system get endpoints "${service}" -o json >"${workload_dependency_endpoints}" \
        && jq -e '[.subsets[]?.addresses[]?.ip] | unique | length >= 1' \
          "${workload_dependency_endpoints}" >/dev/null; then
        ready=1
        break
      fi
      sleep 0.2
    done
    if (( ready == 0 )); then
      echo "${service} Service must expose at least one ready endpoint before controller rollout after ${attempt} attempts" >&2
      return 1
    fi
  done
}

start_admission_proxy() {
  local attempt line
  admission_proxy_port=""
  : >"${admission_proxy_log}"
  "${kubectl}" proxy \
    --address=127.0.0.1 \
    --accept-hosts='^127\.0\.0\.1$' \
    --port=0 \
    >"${admission_proxy_log}" 2>&1 &
  admission_proxy_pid=$!

  for attempt in {1..50}; do
    while IFS= read -r line; do
      if [[ "${line}" =~ ^Starting[[:space:]]+to[[:space:]]+serve[[:space:]]+on[[:space:]]+127\.0\.0\.1:([0-9]+)$ ]]; then
        admission_proxy_port="${BASH_REMATCH[1]}"
        return 0
      fi
    done <"${admission_proxy_log}"
    if ! kill -0 "${admission_proxy_pid}" 2>/dev/null; then
      echo "kubectl proxy exited before the admission smoke endpoint was ready" >&2
      return 1
    fi
    sleep 0.1
  done

  echo "kubectl proxy did not publish an admission smoke endpoint" >&2
  return 1
}

smoke_admission_handlers() {
  local handler group version kind resource uid
  local service_proxy
  if ! start_admission_proxy; then
    return 1
  fi
  service_proxy="http://127.0.0.1:${admission_proxy_port}/api/v1/namespaces/orka-system/services/https:orka-admission:443/proxy"
  while IFS='|' read -r handler group version kind resource; do
    [[ -n "${handler}" ]] || continue
    uid="orka-admission-smoke-${resource}"
    jq -n \
      --arg uid "${uid}" \
      --arg group "${group}" \
      --arg version "${version}" \
      --arg kind "${kind}" \
      --arg resource "${resource}" '
      {
        apiVersion: "admission.k8s.io/v1",
        kind: "AdmissionReview",
        request: {
          uid: $uid,
          kind: {group: $group, version: $version, kind: $kind},
          resource: {group: $group, version: $version, resource: $resource},
          requestKind: {group: $group, version: $version, kind: $kind},
          requestResource: {group: $group, version: $version, resource: $resource},
          name: "orka-admission-smoke",
          namespace: (if $kind == "Namespace" then "" else "orka-system" end),
          operation: "CREATE",
          userInfo: {username: "system:admin", groups: ["system:masters"]},
          object: {
            apiVersion: (if $group == "" then $version else ($group + "/" + $version) end),
            kind: $kind,
            metadata: ({name: "orka-admission-smoke"} +
              (if $kind == "Namespace" then {labels: {"orka.ai/controller-mode": "harness-v2"}}
               elif $kind == "Secret" then
                 {namespace: "orka-system", labels: {"workspace.orka.ai/attachment-for": "smoke-workspace-uid"}}
               else {namespace: "orka-system"} end))
          },
          oldObject: null,
          dryRun: true,
          options: {apiVersion: "meta.k8s.io/v1", kind: "CreateOptions"}
        }
      }
    ' >"${admission_smoke_request}"
    if ! curl \
      --fail \
      --silent \
      --show-error \
      --noproxy '*' \
      --max-time 15 \
      --header 'Content-Type: application/json' \
      --data-binary "@${admission_smoke_request}" \
      "${service_proxy}${handler}" \
      >"${admission_smoke_response}" \
      || ! jq -e --arg uid "${uid}" '
        .apiVersion == "admission.k8s.io/v1" and
        .kind == "AdmissionReview" and
        .response.uid == $uid and
        (.response.allowed | type == "boolean")
      ' "${admission_smoke_response}" >/dev/null; then
      echo "orka-admission handler smoke failed: ${handler}" >&2
      stop_admission_proxy
      return 1
    fi
  done <<'EOF_ADMISSION_HANDLERS'
/validate-v1-namespace-execution-mode||v1|Namespace|namespaces
/validate-v1-secret-workspace-attachment||v1|Secret|secrets
/validate-coordination-k8s-io-v1-acp-suspend-quota-lease|coordination.k8s.io|v1|Lease|leases
/validate-core-orka-ai-v1alpha1-task-provenance|core.orka.ai|v1alpha1|Task|tasks
/validate-core-orka-ai-v1alpha1-task-workspace-class-use|core.orka.ai|v1alpha1|Task|tasks-workspace-class-use
/validate-core-orka-ai-v1alpha1-tool-workspace-class-use|core.orka.ai|v1alpha1|Tool|tools
/validate-core-orka-ai-v1alpha1-agent-contract|core.orka.ai|v1alpha1|Agent|agents
/validate-core-orka-ai-v1alpha1-agentruntime-contract|core.orka.ai|v1alpha1|AgentRuntime|agentruntimes
/validate-core-orka-ai-v1alpha1-task-execution-authority|core.orka.ai|v1alpha1|Task|tasks-execution-authority
EOF_ADMISSION_HANDLERS
  stop_admission_proxy
}

# Establish every workload prerequisite, then activate the independent
# admission plane before rolling the static harness-v2 controller. Every retry repeats
# these idempotent phases, so interruption after any apply still converges on the
# desired generation without rotating an existing snapshot key.
bash "${script_dir}/lib/ensure-static-mode-namespace.sh" "${kubectl}" orka-system harness-v2
validate_existing_controller_identity
# Preserve the immutable mode claim as an atomic precondition while converging
# the remaining platform labels. This cannot relabel or adopt a namespace that
# changes identity after the preflight.
"${kubectl}" patch namespace orka-system --type=json --patch-file "${namespace_metadata_patch}" >/dev/null
validate_admission_tls_secret
ensure_snapshot_secret
"${kubectl}" apply -f "${runtime_config}"
"${kubectl}" apply -f "${admission_runtime_manifest}"
wait_for_admission_endpoints
smoke_admission_handlers
render_admission_webhooks
"${kubectl}" apply -f "${admission_webhooks_manifest}"
"${kubectl}" apply -f "${workload_prerequisite_manifest}"
wait_for_workload_dependencies
"${kubectl}" apply -f "${controller_manifest}"
"${kubectl}" -n orka-system rollout status deployment/orka-controller-manager --timeout=2m >/dev/null
