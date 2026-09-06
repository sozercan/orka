#!/usr/bin/env bash
# Shared helpers for publishing immutable test images to a registry reachable by
# both the host and nodes in one Kind cluster.

orka_kind_registry_name() {
  local cluster="$1"
  local sanitized
  sanitized="$(printf '%s' "${cluster}" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_.-' '-')"
  sanitized="${sanitized#[-_.]}"
  sanitized="${sanitized%[-_.]}"
  printf '%s-orka-registry\n' "${sanitized}"
}

orka_kind_registry_start() {
  local cluster="$1"
  local owner="${2:-}"
  local kind_bin="${KIND:-kind}"
  ORKA_KIND_REGISTRY_NAME="$(orka_kind_registry_name "${cluster}")"

  if [[ -n "${owner}" ]]; then
    docker run -d --name "${ORKA_KIND_REGISTRY_NAME}" --network kind \
      --label "io.orka.test.owner=${owner}" \
      -p 127.0.0.1::5000 registry:2 >/dev/null || return 1
  elif [[ "$(docker inspect -f '{{.State.Running}}' "${ORKA_KIND_REGISTRY_NAME}" 2>/dev/null)" == "true" ]]; then
    # Reuse the running registry: recreating it would change the host port
    # and discard every image a previous step already pinned by digest
    # (cluster-up.sh, install-agent-sandbox.sh, and demo-images run in
    # separate invocations against the same cluster).
    :
  else
    docker rm -f "${ORKA_KIND_REGISTRY_NAME}" >/dev/null 2>&1 || true
    docker run -d --name "${ORKA_KIND_REGISTRY_NAME}" --network kind \
      -p 127.0.0.1::5000 registry:2 >/dev/null || return 1
  fi

  local port_output host_port
  port_output="$(docker port "${ORKA_KIND_REGISTRY_NAME}" 5000/tcp)"
  port_output="${port_output%%,*}"
  host_port="${port_output##*:}"
  [[ "${host_port}" =~ ^[0-9]+$ ]] || {
    echo "unexpected Kind registry port mapping: ${port_output}" >&2
    return 1
  }
  # Keep the host-side reference on IPv4. Docker may resolve localhost to ::1
  # first, while the registry port is intentionally bound only to 127.0.0.1.
  ORKA_KIND_REGISTRY_ADDR="127.0.0.1:${host_port}"

  local deadline=$((SECONDS + 30))
  until curl -fsS "http://${ORKA_KIND_REGISTRY_ADDR}/v2/" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Kind registry ${ORKA_KIND_REGISTRY_ADDR} did not become ready" >&2
      return 1
    fi
    sleep 1
  done

  local node registry_dir
  while IFS= read -r node; do
    [[ -n "${node}" ]] || continue
    registry_dir="/etc/containerd/certs.d/${ORKA_KIND_REGISTRY_ADDR}"
    docker exec "${node}" mkdir -p "${registry_dir}"
    docker exec -i "${node}" sh -c "cat > '${registry_dir}/hosts.toml'" <<HOSTS
server = "http://${ORKA_KIND_REGISTRY_ADDR}"
[host."http://${ORKA_KIND_REGISTRY_NAME}:5000"]
  capabilities = ["pull", "resolve"]
HOSTS
  done < <("${kind_bin}" get nodes --name "${cluster}")

  export ORKA_KIND_REGISTRY_NAME ORKA_KIND_REGISTRY_ADDR
}

orka_kind_registry_push() {
  local source_image="$1"
  local repository="$2"
  local target="${ORKA_KIND_REGISTRY_ADDR}/${repository}:e2e"
  if command -v skopeo >/dev/null 2>&1; then
    if ! skopeo copy --dest-tls-verify=false \
      "docker-daemon:${source_image}" "docker://${target}" >/dev/null; then
      echo "failed to copy ${source_image} to the Kind registry as ${target}" >&2
      return 1
    fi
  else
    docker tag "${source_image}" "${target}"
    if ! docker push "${target}" >/dev/null; then
      echo "failed to push ${target} to the Kind registry" >&2
      return 1
    fi
  fi

  local digest
  digest="$(curl -fsSI \
    -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://${ORKA_KIND_REGISTRY_ADDR}/v2/${repository}/manifests/e2e" | \
    tr -d '\r' | awk 'BEGIN { IGNORECASE=1 } /^Docker-Content-Digest:/ { print $2; exit }')"
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "no immutable digest found after pushing ${target}" >&2
    return 1
  }
  printf '%s/%s@%s\n' "${ORKA_KIND_REGISTRY_ADDR}" "${repository}" "${digest}"
}

orka_kind_registry_stop() {
  local name="${ORKA_KIND_REGISTRY_NAME:-}"
  local owner="${2:-}"
  local container_ids actual_owner
  if [[ -z "${name}" && $# -gt 0 ]]; then
    name="$(orka_kind_registry_name "$1")"
  fi
  [[ -n "${name}" ]] || return 0

  container_ids="$(docker container ls --all --filter "name=^/${name}$" --format '{{.ID}}')" || return 1
  [[ -n "${container_ids}" ]] || return 0
  [[ "${container_ids}" != *$'\n'* ]] || {
    echo "multiple containers matched Kind registry ${name}" >&2
    return 1
  }

  if [[ -n "${owner}" ]]; then
    actual_owner="$(docker container inspect --format '{{ index .Config.Labels "io.orka.test.owner" }}' "${name}")" || return 1
    [[ "${actual_owner}" == "${owner}" ]] || {
      echo "refusing to remove Kind registry ${name} without matching ownership" >&2
      return 1
    }
  fi

  docker container rm --force "${name}" >/dev/null || return 1
  container_ids="$(docker container ls --all --filter "name=^/${name}$" --format '{{.ID}}')" || return 1
  [[ -z "${container_ids}" ]] || {
    echo "Kind registry ${name} remains after cleanup" >&2
    return 1
  }
}
