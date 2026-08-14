FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install git and certificates
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/bin/barnacles-server ./cmd/barnacles-server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/bin/barnacles-agent ./cmd/barnacles-agent

# Runtime image
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 10001 -S barnacles && \
    adduser -u 10001 -S barnacles -G barnacles

WORKDIR /app

# Copy binaries and web assets
COPY --from=builder /build/bin/barnacles-server /app/barnacles-server
COPY --from=builder /build/bin/barnacles-agent /app/barnacles-agent
COPY --from=builder /build/web /app/web
COPY --from=builder /build/config /app/config

# Create data directories and set permissions
RUN mkdir -p /app/data/logs /app/data/agent-spool && \
    chown -R barnacles:barnacles /app

USER barnacles:barnacles

EXPOSE 8080 9090

ENTRYPOINT ["/app/barnacles-server"]
CMD ["-config", "/app/config/server.yaml"]
