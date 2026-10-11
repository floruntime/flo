#!/bin/bash
# Script to run a 3-node Flo cluster locally for testing
#
# Usage: ./scripts/run-cluster.sh
#
# Config files: tests/cluster/e2e/configs/node{1,2,3}.toml
#
# Port layout (10-port gap avoids collisions with derived ports):
#   Node 1: listen=4441, metrics=4442, dashboard=4443, peer=4941
#   Node 2: listen=4451, metrics=4452, dashboard=4453, peer=4951
#   Node 3: listen=4461, metrics=4462, dashboard=4463, peer=4961

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_DIR="$PROJECT_DIR/tests/cluster/e2e/configs"
DATA_DIR="/tmp/flo-cluster-test"
LOG_DIR="$DATA_DIR"
FLO_BIN="$PROJECT_DIR/zig-out/bin/flo"

# Stop the flo servers started from this checkout's own binary, and only
# those: a pattern any flo matches would kill other checkouts' test runs and
# hand-started servers too. The path is regex-escaped and anchored, matching
# how the servers are launched ("$FLO_BIN" server start ...).
kill_own_servers() {
    local pattern
    pattern="^$(printf '%s' "$FLO_BIN" | sed 's/[][\.*^$|?+(){}]/\\&/g') server "
    pkill -f "$pattern" 2>/dev/null || true
}

# Build if needed
if [[ ! -f "$FLO_BIN" ]]; then
    echo "Building Flo..."
    cd "$PROJECT_DIR" && zig build -Drelease
fi

echo "🚀 Starting 3-node Flo cluster..."

# Stop servers an earlier run of this checkout left running
kill_own_servers
sleep 1

# Clean data directories
rm -rf "$DATA_DIR"/{node1,node2,node3}
mkdir -p "$DATA_DIR"/{node1,node2,node3}

# Start Node 1 (listen=4441, dashboard=4443)
echo "Starting Node 1..."
"$FLO_BIN" server start \
  --config "$CONFIG_DIR/node1.toml" \
  > "$LOG_DIR/node1.log" 2>&1 &
NODE1_PID=$!
echo "Node 1 PID: $NODE1_PID"

sleep 2

# Start Node 2 (listen=4451, dashboard=4453)
echo "Starting Node 2..."
"$FLO_BIN" server start \
  --config "$CONFIG_DIR/node2.toml" \
  > "$LOG_DIR/node2.log" 2>&1 &
NODE2_PID=$!
echo "Node 2 PID: $NODE2_PID"

sleep 2

# Start Node 3 (listen=4461, dashboard=4463)
echo "Starting Node 3..."
"$FLO_BIN" server start \
  --config "$CONFIG_DIR/node3.toml" \
  > "$LOG_DIR/node3.log" 2>&1 &
NODE3_PID=$!
echo "Node 3 PID: $NODE3_PID"

sleep 3

echo ""
echo "✅ Cluster started!"
echo ""
echo "Node 1: listen=4441  dashboard=http://localhost:4443  PID=$NODE1_PID"
echo "Node 2: listen=4451  dashboard=http://localhost:4453  PID=$NODE2_PID"
echo "Node 3: listen=4461  dashboard=http://localhost:4463  PID=$NODE3_PID"
echo ""
echo "Logs:"
echo "  tail -f $LOG_DIR/node1.log"
echo "  tail -f $LOG_DIR/node2.log"
echo "  tail -f $LOG_DIR/node3.log"
echo ""
echo "To stop cluster: kill $NODE1_PID $NODE2_PID $NODE3_PID"
echo ""

# The first member leads at once; wait for the two joins.
echo "Waiting for the joins (5s)..."
sleep 5

# Test basic connectivity (health endpoint is on dashboard port: listen + 2)
echo ""
echo "🧪 Testing cluster connectivity..."
echo ""

for port in 4443 4453 4463; do
    echo -n "Health check on :$port... "
    curl -sf "http://localhost:$port/health" && echo "✓" || echo "❌ unreachable"
done

echo ""
echo "Press Ctrl+C to stop cluster..."
wait
