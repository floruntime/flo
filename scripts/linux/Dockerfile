# Linux build/test environment for flo, matching the Zig version in CI.
#
# Exists because macOS never compiles flo's Linux-only code paths (io_uring
# reactor, /proc host stats), so that breakage is invisible on a Mac.
FROM debian:bookworm-slim

ARG ZIG_VERSION=0.16.0
ARG ZIG_ARCH=aarch64

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates xz-utils git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
      -o /tmp/zig.tar.xz \
 && mkdir -p /opt/zig \
 && tar -xJf /tmp/zig.tar.xz -C /opt/zig --strip-components=1 \
 && rm /tmp/zig.tar.xz \
 && ln -s /opt/zig/zig /usr/local/bin/zig

# The repo is bind-mounted; keep the Zig cache out of it so Linux artifacts
# never collide with the host's macOS cache.
ENV ZIG_LOCAL_CACHE_DIR=/zigcache
ENV ZIG_GLOBAL_CACHE_DIR=/zigcache/global
WORKDIR /w
CMD ["zig", "version"]
