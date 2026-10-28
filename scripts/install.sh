#!/bin/sh
# Flo install script
# Downloads a pre-built binary from GitHub Releases and installs it.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/dev/scripts/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/dev/scripts/install.sh | sh -s -- --version v0.1.0
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/dev/scripts/install.sh | sh -s -- --dir ~/.local/bin

set -e

REPO="floruntime/flo"
BINARY="flo"
VERSION=""
INSTALL_DIR=""
SKIP_CHECKSUM=0

# --- Parse arguments ---
while [ $# -gt 0 ]; do
  case "$1" in
    --version|-v) VERSION="$2"; shift 2 ;;
    --dir|-d)     INSTALL_DIR="$2"; shift 2 ;;
    --insecure-skip-checksum) SKIP_CHECKSUM=1; shift ;;
    --help|-h)
      echo "Usage: install.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --version, -v VERSION   Install a specific version (e.g. v0.1.0)"
      echo "  --dir, -d DIR           Install to a custom directory"
      echo "  --insecure-skip-checksum"
      echo "                          Install even if the download can't be verified"
      echo "  --help, -h              Show this help"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# --- Detect platform ---
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
  linux)  ;;
  darwin) OS="macos" ;;
  *)      echo "Error: Unsupported OS: $OS"; exit 1 ;;
esac

case "$ARCH" in
  x86_64)         ARCH="x86_64" ;;
  aarch64|arm64)  ARCH="aarch64" ;;
  *)              echo "Error: Unsupported architecture: $ARCH"; exit 1 ;;
esac

# The macOS binaries are built for macOS 15 and won't start on older systems.
if [ "$OS" = "macos" ]; then
  MACOS_VERSION=$(sw_vers -productVersion 2>/dev/null || true)
  MACOS_MAJOR=${MACOS_VERSION%%.*}
  case "$MACOS_MAJOR" in
    ''|*[!0-9]*)
      echo "Warning: could not read the macOS version; flo needs macOS 15 or later." >&2
      ;;
    *)
      if [ "$MACOS_MAJOR" -lt 15 ]; then
        echo "Error: flo needs macOS 15 or later (this is macOS $MACOS_VERSION)."
        echo "Upgrade macOS, or build from source: https://github.com/$REPO#building-from-source"
        exit 1
      fi
      ;;
  esac
fi

# The release binaries need hardware CRC32C: x86-64-v2, or ARMv8 with CRC.
# Below that they stop with an illegal instruction, so say why first.
# Only this arch's line is read: under emulation (an amd64 container on an
# arm64 host) cpuinfo describes the host, and then there's nothing to judge.
# The same check, with the same flag lists, is in
# packaging/scripts/preinstall.sh and the Homebrew formula
# (scripts/check-cpu-floor.sh compares them).
if [ "$OS" = "linux" ]; then
  case "$ARCH" in
    x86_64)  CPU_KEY="flags";    NEEDED="cx16 lahf_lm pni popcnt sse4_1 sse4_2 ssse3" ;;
    aarch64) CPU_KEY="Features"; NEEDED="crc32" ;;
  esac
  CPU_FLAGS=$(grep -m1 -E "^${CPU_KEY}[[:space:]]*:" /proc/cpuinfo 2>/dev/null || true)
  if [ -z "$CPU_FLAGS" ]; then
    echo "Warning: could not read this CPU's $CPU_KEY from /proc/cpuinfo; flo needs an x86-64-v2 CPU or ARMv8 with CRC." >&2
  else
    MISSING=""
    for f in $NEEDED; do
      case " ${CPU_FLAGS#*:} " in
        *" $f "*) ;;
        *) MISSING="$MISSING $f" ;;
      esac
    done
    if [ -n "$MISSING" ]; then
      echo "Error: flo needs an x86-64-v2 CPU or ARMv8 with CRC; this CPU lacks:$MISSING."
      echo "Build from source for this CPU: https://github.com/$REPO#building-from-source"
      exit 1
    fi
  fi
fi

# --- Default install directory ---
if [ -z "$INSTALL_DIR" ]; then
  if [ "$OS" = "macos" ]; then
    INSTALL_DIR="/opt/homebrew/bin"
  else
    INSTALL_DIR="/usr/local/bin"
  fi
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# --- Resolve version ---
if [ -z "$VERSION" ]; then
  echo "Fetching latest release..."
  # The newest release, prereleases included: releases/latest skips those,
  # and every flo release so far is one.
  API_CODE=$(curl -sL -D "$TMP_DIR/api-headers" -o "$TMP_DIR/api-body" -w '%{http_code}' \
    "https://api.github.com/repos/$REPO/releases?per_page=1" || true)
  if [ "$API_CODE" = "200" ]; then
    VERSION=$(grep '"tag_name"' "$TMP_DIR/api-body" | head -1 | cut -d'"' -f4)
  fi
  if [ -z "$VERSION" ]; then
    if { [ "$API_CODE" = "403" ] || [ "$API_CODE" = "429" ]; } &&
      grep -qi '^x-ratelimit-remaining: *0[^0-9]*$' "$TMP_DIR/api-headers" 2>/dev/null; then
      echo "Error: GitHub API rate limit reached. Retry later, or pass --version vX.Y.Z"
      echo "(releases: https://github.com/$REPO/releases)."
    else
      echo "Error: Could not determine latest version (GitHub API HTTP ${API_CODE:-000})."
      echo "Check https://github.com/$REPO/releases or specify --version"
    fi
    exit 1
  fi
fi

ARTIFACT="flo-${ARCH}-${OS}"
URL="https://github.com/$REPO/releases/download/$VERSION/${ARTIFACT}.tar.gz"
CHECKSUM_URL="${URL}.sha256"

echo "Installing flo $VERSION ($ARCH-$OS) to $INSTALL_DIR..."

# --- Download ---
TARBALL="${ARTIFACT}.tar.gz"

echo "Downloading $URL..."
HTTP_CODE=$(curl -sL -w '%{http_code}' -o "$TMP_DIR/$TARBALL" "$URL")
if [ "$HTTP_CODE" != "200" ]; then
  echo "Error: Download failed (HTTP $HTTP_CODE)"
  echo "Check that version $VERSION exists: https://github.com/$REPO/releases"
  exit 1
fi

# --- Verify checksum ---
if [ "$SKIP_CHECKSUM" = 1 ]; then
  echo "Warning: installing without verifying the checksum (--insecure-skip-checksum)." >&2
else
  echo "Verifying checksum..."
  SKIP_HINT="Re-run with --insecure-skip-checksum to install anyway."
  SUM_CODE=$(curl -sL -w '%{http_code}' -o "$TMP_DIR/${TARBALL}.sha256" "$CHECKSUM_URL" || true)
  if [ "$SUM_CODE" != "200" ]; then
    echo "Error: could not download the checksum (HTTP ${SUM_CODE:-000}) from $CHECKSUM_URL."
    echo "Not installing an unverified binary. $SKIP_HINT"
    exit 1
  fi
  EXPECTED=$(awk '{ print tolower($1); exit }' "$TMP_DIR/${TARBALL}.sha256")
  case "$EXPECTED" in
    *[!0-9a-f]*) EXPECTED="" ;;
  esac
  if [ ${#EXPECTED} -ne 64 ]; then
    echo "Error: $CHECKSUM_URL doesn't hold a SHA-256 hash."
    echo "Not installing an unverified binary. $SKIP_HINT"
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "$TMP_DIR/$TARBALL" | awk '{ print $1 }')
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL=$(shasum -a 256 "$TMP_DIR/$TARBALL" | awk '{ print $1 }')
  else
    echo "Error: no SHA-256 tool found (looked for sha256sum and shasum)."
    echo "Not installing an unverified binary. $SKIP_HINT"
    exit 1
  fi
  if [ "$ACTUAL" != "$EXPECTED" ]; then
    echo "Error: checksum mismatch for $TARBALL."
    echo "  expected $EXPECTED"
    echo "  got      $ACTUAL"
    exit 1
  fi
  echo "Checksum OK ($EXPECTED)"
fi

# --- Extract ---
tar -xzf "$TMP_DIR/$TARBALL" -C "$TMP_DIR"

if [ ! -f "$TMP_DIR/$BINARY" ]; then
  echo "Error: Binary not found in archive"
  exit 1
fi

# --- Install ---
mkdir -p "$INSTALL_DIR" 2>/dev/null || true

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
  chmod +x "$INSTALL_DIR/$BINARY"
else
  echo "Installing to $INSTALL_DIR requires sudo..."
  sudo mkdir -p "$INSTALL_DIR"
  sudo mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
  sudo chmod +x "$INSTALL_DIR/$BINARY"
fi

echo ""
echo "  flo $VERSION installed to $INSTALL_DIR/$BINARY"
echo ""
echo "  Get started:"
echo "    flo server start            # Start the server"
echo "    flo kv set hello world      # Store a value"
echo "    flo kv get hello            # Read it back"
echo ""
echo "  Dashboard: http://localhost:9002"
echo ""
