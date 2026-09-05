# syntax=docker/dockerfile:1.7.1@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG AGENTKIT_RUNTIME_IMAGE
ARG AGENTKIT_ADAPTER_DIGEST

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.0-bookworm@sha256:484ef6066fa69acb059fdfeda7ba2b8f7391f2ef6abc6f9b8411e669ebd56466 AS supervisor-builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked go mod download
COPY . .
RUN set -eu; \
    test "$TARGETOS" = linux; \
    case "$TARGETARCH" in amd64|arm64) ;; *) exit 1 ;; esac; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -buildvcs=false -trimpath -ldflags='-s -w' \
      -o /out/orka-acp-runtime ./cmd/orka-acp-runtime; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -buildvcs=false -trimpath -ldflags='-s -w' \
      -o /out/orka-acp-exec-helper ./cmd/orka-acp-exec-helper

# The source image is one AgentKit-built agent. It supplies the Python runtime,
# /opt/agentkit/bin/agentkit-serve, and the exact /agent/agent.yaml registered
# with Orka. The Orka layer adds only its static supervisor processes.
FROM --platform=$TARGETPLATFORM ${AGENTKIT_RUNTIME_IMAGE}
ARG AGENTKIT_RUNTIME_IMAGE
ARG AGENTKIT_ADAPTER_DIGEST
LABEL org.opencontainers.image.title="Orka ACP AgentKit runtime" \
      org.opencontainers.image.description="Orka harness v2 ACP supervisor layered onto a frozen AgentKit agent" \
      org.opencontainers.image.source="https://github.com/orka-agents/orka" \
      io.orka.acp.provider="agentkit" \
      io.orka.acp.adapter.name="agentkit-serve-acp"
ENV ORKA_ACP_PROVIDER=agentkit \
    ORKA_ACP_AGENTKIT_ADAPTER_DIGEST=${AGENTKIT_ADAPTER_DIGEST}
USER 0:0
RUN set -eu; \
    case "$AGENTKIT_RUNTIME_IMAGE" in *@sha256:*) ;; *) echo "AGENTKIT_RUNTIME_IMAGE must be digest-pinned" >&2; exit 1 ;; esac; \
    digest="${AGENTKIT_RUNTIME_IMAGE##*@sha256:}"; \
    test "${#digest}" -eq 64; \
    case "$digest" in *[!0-9a-f]*) echo "AGENTKIT_RUNTIME_IMAGE must use a lowercase sha256 digest" >&2; exit 1 ;; esac; \
    case "$AGENTKIT_ADAPTER_DIGEST" in sha256:*) ;; *) echo "AGENTKIT_ADAPTER_DIGEST must be a sha256 digest" >&2; exit 1 ;; esac; \
    adapter_digest="${AGENTKIT_ADAPTER_DIGEST#sha256:}"; \
    test "${#adapter_digest}" -eq 64; \
    case "$adapter_digest" in *[!0-9a-f]*) echo "AGENTKIT_ADAPTER_DIGEST must use a lowercase sha256 digest" >&2; exit 1 ;; esac; \
    test "$AGENTKIT_ADAPTER_DIGEST" = "sha256:$digest" || { echo "AGENTKIT_ADAPTER_DIGEST must equal the digest-pinned AgentKit source image" >&2; exit 1; }; \
    test -x /opt/agentkit/bin/agentkit-serve; \
    test -s /agent/agent.yaml; \
    mkdir -p /sessions /usr/share/licenses/orka; \
    chown -R 0:0 /opt/agentkit; \
    chmod -R a+rX,go-w /opt/agentkit; \
    chown 0:0 /agent /agent/agent.yaml; \
    chmod 0555 /agent; \
    chmod 0711 /sessions; \
    chmod 0444 /agent/agent.yaml
COPY --from=supervisor-builder /out/orka-acp-runtime /usr/local/bin/orka-acp-runtime
COPY --from=supervisor-builder /out/orka-acp-exec-helper /usr/local/bin/orka-acp-exec-helper
COPY LICENSE /usr/share/licenses/orka/LICENSE
COPY NOTICE.md /usr/share/licenses/orka/NOTICE.md
WORKDIR /
# The supervisor starts as root so it can allocate a unique UID/GID for each
# child and prove descendant cleanup. The Pod security context drops every
# capability except the existing identity and process-management set.
# trivy:ignore:DS-0002
USER 0:0
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD []
ENTRYPOINT ["/usr/local/bin/orka-acp-runtime"]
