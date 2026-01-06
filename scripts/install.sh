#!/bin/sh
# Flo install script
# Downloads a pre-built binary from GitHub Releases and installs it.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh -s -- --version v0.1.0
#   curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh -s -- --dir ~/.local/bin

set -e

REPO="floruntime/flo"
BINARY="flo"
VERSION=""
INSTALL_DIR=""

# --- Parse arguments ---
while [ $# -gt 0 ]; do
  case "$1" in
    --version|-v) VERSION="$2"; shift 2 ;;
    --dir|-d)     INSTALL_DIR="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: install.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --version, -v VERSION   Install a specific version (e.g. v0.1.0)"
      echo "  --dir, -d DIR           Install to a custom directory"
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

# --- Default install directory ---
if [ -z "$INSTALL_DIR" ]; then
  if [ "$OS" = "macos" ]; then
    INSTALL_DIR="/opt/homebrew/bin"
  else
    INSTALL_DIR="/usr/local/bin"
  fi
fi

# --- Resolve version ---
if [ -z "$VERSION" ]; then
  echo "Fetching latest release..."
  VERSION=$(curl -sfL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d'"' -f4)
  if [ -z "$VERSION" ]; then
    echo "Error: Could not determine latest version."
    echo "Check https://github.com/$REPO/releases or specify --version"
    exit 1
  fi
fi

ARTIFACT="flo-${ARCH}-${OS}"
URL="https://github.com/$REPO/releases/download/$VERSION/${ARTIFACT}.tar.gz"
CHECKSUM_URL="${URL}.sha256"

echo "Installing flo $VERSION ($ARCH-$OS) to $INSTALL_DIR..."

# --- Download ---
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading $URL..."
HTTP_CODE=$(curl -sL -w '%{http_code}' -o "$TMP_DIR/flo.tar.gz" "$URL")
if [ "$HTTP_CODE" != "200" ]; then
  echo "Error: Download failed (HTTP $HTTP_CODE)"
  echo "Check that version $VERSION exists: https://github.com/$REPO/releases"
  exit 1
fi

# --- Verify checksum ---
echo "Verifying checksum..."
if curl -sfL -o "$TMP_DIR/flo.tar.gz.sha256" "$CHECKSUM_URL" 2>/dev/null; then
  (
    cd "$TMP_DIR"
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum -c flo.tar.gz.sha256 >/dev/null
    elif command -v shasum >/dev/null 2>&1; then
      shasum -a 256 -c flo.tar.gz.sha256 >/dev/null
    else
      echo "Warning: No SHA256 tool found, skipping verification"
    fi
  )
  echo "Checksum OK"
else
  echo "Warning: No checksum file available, skipping verification"
fi

# --- Extract ---
tar -xzf "$TMP_DIR/flo.tar.gz" -C "$TMP_DIR"

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
