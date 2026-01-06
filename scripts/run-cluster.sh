#!/bin/bash
# Script to run a 3-node Flo cluster locally for testing
#
# Usage: ./scripts/run-cluster.sh [start|stop|status|logs]
#
# Config files: tests/cluster/e2e/configs/node{1,2,3}.toml

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_DIR="$PROJECT_DIR/tests/cluster/e2e/configs"
DATA_DIR="/tmp/flo-cluster-test"
LOG_DIR="$DATA_DIR"

echo "🚀 Starting 3-node Flo cluster..."

# Kill any existing flo processes
pkill -f "flo server" || true
sleep 1

# Clean data directories
rm -rf $DATA_DIR/{node1,node2,node3}
mkdir -p $DATA_DIR/{node1,node2,node3}

# Start Node 1 (port 4441, raft 9501)
echo "Starting Node 1..."
"$PROJECT_DIR/zig-out/bin/flo" server start \
  --config "$CONFIG_DIR/node1.toml" \
  > "$LOG_DIR/node1.log" 2>&1 &
NODE1_PID=$!
echo "Node 1 PID: $NODE1_PID"

sleep 2

# Start Node 2 (port 4442, raft 9502)
echo "Starting Node 2..."
"$PROJECT_DIR/zig-out/bin/flo" server start \
  --config "$CONFIG_DIR/node2.toml" \
  > "$LOG_DIR/node2.log" 2>&1 &
NODE2_PID=$!
echo "Node 2 PID: $NODE2_PID"

sleep 2

# Start Node 3 (port 4443, raft 9503)
echo "Starting Node 3..."
"$PROJECT_DIR/zig-out/bin/flo" server start \
  --config "$CONFIG_DIR/node3.toml" \
  > "$LOG_DIR/node3.log" 2>&1 &
NODE3_PID=$!
echo "Node 3 PID: $NODE3_PID"

sleep 3

echo ""
echo "✅ Cluster started!"
echo ""
echo "Node 1: http://localhost:4441 (Raft: 9501) - PID: $NODE1_PID"
echo "Node 2: http://localhost:4442 (Raft: 9502) - PID: $NODE2_PID"
echo "Node 3: http://localhost:4443 (Raft: 9503) - PID: $NODE3_PID"
echo ""
echo "Logs:"
echo "  tail -f $LOG_DIR/node1.log"
echo "  tail -f $LOG_DIR/node2.log"
echo "  tail -f $LOG_DIR/node3.log"
echo ""
echo "To stop cluster: kill $NODE1_PID $NODE2_PID $NODE3_PID"
echo ""

# Wait for leader election
echo "Waiting for leader election (5s)..."
sleep 5

# Test basic connectivity
echo ""
echo "🧪 Testing cluster connectivity..."
echo ""

echo "Node 1 health:"
curl -s http://localhost:4441/health || echo "❌ Node 1 unreachable"
echo ""

echo "Node 2 health:"
curl -s http://localhost:4442/health || echo "❌ Node 2 unreachable"
echo ""

echo "Node 3 health:"
curl -s http://localhost:4443/health || echo "❌ Node 3 unreachable"
echo ""

echo ""
echo "Press Ctrl+C to stop cluster..."
wait
