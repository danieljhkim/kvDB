# Go 1.24.13 on Alpine 3.23 (pinned 2026-08-22).
FROM golang:1.24.13-alpine3.23@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS builder

WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the CLI binary
RUN go build -o kv

# ------------------------
# Final runtime image
# ------------------------

# Alpine 3.24.1 runtime (pinned 2026-08-22).
FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

WORKDIR /cli

# Copy the built binary from builder stage
COPY --from=builder /app/kv .

COPY config.yaml .
# Optional: make sure it's executable
RUN chmod +x kv

# Default command when container starts
ENTRYPOINT ["./kv"]
