#!/usr/bin/env bash
# Build and test flo on Linux from a macOS host.
#
# Zig analyses lazily, so a macOS build never compiles flo's Linux-only code —
# the io_uring reactor, the /proc host stats. Whole classes of breakage are
# therefore invisible on a Mac, and some failures reproduce only on Linux.
# This gives a local Linux environment instead of a CI round trip.
#
#   scripts/linux/run.sh build
#   scripts/linux/run.sh test-e2e -Dtest-filter="e2e/kv/cluster"
#   scripts/linux/run.sh shell
#
# Notes:
#   * --security-opt seccomp=unconfined is REQUIRED: Docker's default profile
#     blocks the io_uring syscalls and flo cannot start without them.
#   * The Zig cache lives in a named volume so Linux artifacts never collide
#     with the host's macOS .zig-cache.
#   * -Dcpu=baseline+crc is needed on aarch64: checksum_hw.zig emits a crc
#     instruction that the aarch64 baseline target does not enable.
set -euo pipefail

IMAGE=flo-linux:0.16.0
VOLUME=flo-zigcache
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARCH="$(uname -m)"; [ "$ARCH" = "arm64" ] && ARCH=aarch64

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "==> building $IMAGE"
  docker build --build-arg ZIG_ARCH="$ARCH" -t "$IMAGE" "$REPO_ROOT/scripts/linux"
fi
if ! docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  docker volume create "$VOLUME" >/dev/null
  docker run --rm -v "$VOLUME:/zigcache" "$IMAGE" chown -R "$(id -u):$(id -g)" /zigcache
fi

CPU_ARG=()
[ "$ARCH" = "aarch64" ] && CPU_ARG=(-Dcpu=baseline+crc)

case "${1:-build}" in
  shell) shift; CMD=(bash "$@") ;;
  *)     CMD=(zig build "$@" "${CPU_ARG[@]}") ;;
esac

exec docker run --rm -it \
  --security-opt seccomp=unconfined \
  --user "$(id -u):$(id -g)" \
  -v "$REPO_ROOT:/w" -v "$VOLUME:/zigcache" -w /w \
  "$IMAGE" "${CMD[@]}"
