# Build stage
FROM golang:1.27.1 AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build the worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -o worker ./workers/ai

# Runtime stage - using python slim for code_exec tool support
FROM python:3.14-slim

# Install Node.js for JavaScript execution
RUN apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -u 65532 -m nonroot

WORKDIR /

COPY --from=builder /workspace/worker /worker

# Create work directory for code execution
RUN mkdir -p /tmp/orka-exec && chown nonroot:nonroot /tmp/orka-exec

USER 65532:65532

ENTRYPOINT ["/worker"]
