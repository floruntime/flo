# Multi-stage Dockerfile for Flo
# Stage 1: Build web dashboard (runs on builder's native arch — only produces static assets)
FROM --platform=$BUILDPLATFORM node:20-alpine AS web-builder

WORKDIR /build/web
COPY web/package*.json ./
RUN npm ci

COPY web/ ./
RUN npm run build

# Stage 2: Build Zig application
# Always runs on the builder's native arch — Zig cross-compiles for the target.
# This avoids slow QEMU emulation for arm64 builds on amd64 runners.
FROM --platform=$BUILDPLATFORM alpine:3.19 AS zig-builder

# Install build dependencies
RUN apk add --no-cache \
    curl \
    xz \
    tar \
    gzip \
    build-base \
    linux-headers

# Install Zig for the BUILD platform (native, never emulated)
ARG ZIG_VERSION=0.16.0
ARG BUILDPLATFORM
RUN case "${BUILDPLATFORM}" in \
      */arm64) ZIG_ARCH="aarch64" ;; \
      *)       ZIG_ARCH="x86_64"  ;; \
    esac && \
    curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" | tar -xJ -C /usr/local && \
    ln -s "/usr/local/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig

WORKDIR /build

# Copy source code
COPY build.zig build.zig.zon ./
COPY src/ ./src/

# Copy built web dashboard from previous stage
RUN mkdir -p src/node/dashboard/dist
COPY --from=web-builder /build/web/dist/ ./src/node/dashboard/dist/

# Cross-compile for the TARGET platform using Zig's native cross-compilation.
# When BUILDPLATFORM == TARGETPLATFORM, this is a native build (no -Dtarget needed).
# When they differ (e.g. building arm64 on amd64), Zig cross-compiles without QEMU.
ARG TARGETPLATFORM
ARG FLO_VERSION=dev
RUN case "${TARGETPLATFORM}" in \
      linux/arm64)  ZIG_TARGET="aarch64-linux-musl" ;; \
      linux/amd64)  ZIG_TARGET="x86_64-linux-musl"  ;; \
      *)            ZIG_TARGET="" ;; \
    esac && \
    if [ -n "${ZIG_TARGET}" ]; then \
      zig build -Drelease -Dtarget=${ZIG_TARGET} -Dversion=${FLO_VERSION}; \
    else \
      zig build -Drelease -Dversion=${FLO_VERSION}; \
    fi

# Stage 3: Runtime image
FROM alpine:3.19

# Install runtime dependencies (curl for healthcheck; nc is built into busybox)
RUN apk add --no-cache \
    ca-certificates \
    curl

# Create data directory
RUN mkdir -p /data/flo

# Copy the binary and default config from builder
COPY --from=zig-builder /build/zig-out/bin/flo /usr/local/bin/flo
COPY examples/docker-compose/flo.toml /etc/flo/flo.toml

# Expose ports: 9000=API, 9001=Metrics (port+1), 9002=Dashboard (port+2)
# Raft (port+500) and Gossip (port+600) only needed for clustering
EXPOSE 9000 9001 9002

# Health check: try dashboard /health first (rich JSON), fall back to TCP
# connect on the main port (works even when dashboard is disabled).
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD curl -sf http://localhost:9002/health \
     || nc -z localhost 9000 \
     || exit 1

# Run as non-root user
RUN addgroup -g 1000 flo && \
    adduser -D -u 1000 -G flo flo && \
    chown -R flo:flo /data/flo /etc/flo

USER flo

VOLUME ["/data/flo"]

ENTRYPOINT ["/usr/local/bin/flo"]
CMD ["server", "start", "-c", "/etc/flo/flo.toml"]
