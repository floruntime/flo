#!/usr/bin/env bash
# Build the Console v2 web app and embed it into the binary's dashboard assets.
#
# This MUST run before any release-mode `zig build`: the binary embeds whatever
# is in src/node/dashboard/dist (build.zig regenerates assets.zig from it), so a
# stale dist ships a stale dashboard. The release zake tasks call this script.
#
# Usage: scripts/build-dashboard.sh   (run from anywhere; resolves the repo root)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "Building web dashboard (Console v2)…"
( cd web && npm install && npm run build )

echo "Embedding dashboard assets into src/node/dashboard/dist/…"
rm -rf src/node/dashboard/dist
mkdir -p src/node/dashboard/dist
cp -r web/dist/* src/node/dashboard/dist/

echo "✓ Dashboard build complete — re-run \`zig build\` to regenerate assets.zig"
