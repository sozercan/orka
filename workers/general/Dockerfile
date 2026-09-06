# Build stage
FROM golang:1.27.1 AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -o worker ./workers/general

# Runtime stage
FROM alpine:3.24

RUN apk add --no-cache ca-certificates git \
    && addgroup -g 1000 worker \
    && adduser -D -u 1000 -G worker worker \
    && mkdir -p /workspace /home/worker /tmp \
    && chown -R 1000:1000 /workspace /home/worker /tmp \
    && printf '#!/bin/sh\necho "$GIT_TOKEN"\n' > /bin/echo-token \
    && chmod +x /bin/echo-token

WORKDIR /

COPY --from=builder /workspace/worker /worker

USER 1000:1000
ENV HOME=/home/worker

ENTRYPOINT ["/worker"]
