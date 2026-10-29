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
#   * -Dcpu=baseline+crc on aarch64 builds for the release CPU floor (ARMv8
#     with CRC), so log and peer checksums take the hardware path.
#   * The Zig version is build.zig.zon's minimum_zig_version, and the image
#     is tagged with it, so a toolchain bump builds a new image.
#   * e2e data dirs (.zig-cache/tmp) get a tmpfs of their own: Docker Desktop
#     doesn't carry flock between processes on a folder shared from the Mac,
#     so on the bind mount a second server would take a data dir in use. The
#     tmpfs goes with the container. A fresh volume would be root-owned and
#     unwritable as --user. It needs exec: some tests run scripts from there,
#     and a tmpfs mount is noexec by default.
set -euo pipefail

ZIG_VERSION=$(sed -n 's/.*minimum_zig_version = "\([^"]*\)".*/\1/p' "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/build.zig.zon")
[ -n "$ZIG_VERSION" ] || { echo "run.sh: no minimum_zig_version in build.zig.zon" >&2; exit 1; }
IMAGE=flo-linux:$ZIG_VERSION
VOLUME=flo-zigcache
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARCH="$(uname -m)"; [ "$ARCH" = "arm64" ] && ARCH=aarch64

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "==> building $IMAGE"
  docker build --build-arg ZIG_ARCH="$ARCH" --build-arg ZIG_VERSION="$ZIG_VERSION" -t "$IMAGE" "$REPO_ROOT/scripts/linux"
fi
if ! docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  docker volume create "$VOLUME" >/dev/null
  docker run --rm -v "$VOLUME:/zigcache" "$IMAGE" chown -R "$(id -u):$(id -g)" /zigcache
fi

CPU_ARG=()
[ "$ARCH" = "aarch64" ] && CPU_ARG=(-Dcpu=baseline+crc)

case "${1:-build}" in
  shell) shift; CMD=(bash "$@") ;;
  *)     CMD=(zig build "$@" ${CPU_ARG[@]+"${CPU_ARG[@]}"}) ;;
esac

# A terminal only when there is one: agents and CI run this without a TTY.
TTY=()
[ -t 0 ] && [ -t 1 ] && TTY=(-it)

exec docker run --rm ${TTY[@]+"${TTY[@]}"} \
  --security-opt seccomp=unconfined \
  --user "$(id -u):$(id -g)" \
  -v "$REPO_ROOT:/w" -v "$VOLUME:/zigcache" -w /w \
  --tmpfs /w/.zig-cache/tmp:rw,exec,mode=1777 \
  "$IMAGE" "${CMD[@]}"
