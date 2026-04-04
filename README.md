<p align="center">
  <h1 align="center">Flo</h1>
  <p align="center">The stream processor with built-in state.</p>
  <p align="center">
    <a href="https://github.com/floruntime/flo/actions"><img alt="CI" src="https://github.com/floruntime/flo/actions/workflows/release.yml/badge.svg"></a>
    <a href="https://github.com/floruntime/flo/releases"><img alt="Release" src="https://img.shields.io/github/v/release/floruntime/flo?include_prereleases"></a>
    <a href="https://github.com/floruntime/flo/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
    <a href="https://ziglang.org"><img alt="Built with Zig" src="https://img.shields.io/badge/built%20with-Zig-F7A41D.svg"></a>
  </p>
</p>

---

Flo is a **stream processor with built-in state**. Define processing pipelines in YAML, deploy with one command, and enrich from KV, dead-letter to queues, and write derived metrics to time-series — all in one binary, one memory space, zero serialization hops. No JVM. No cluster. No glue.

```
┌──────────────────────────────────────────────────────────────┐
│  Source (Flo Streams, Kafka, …)                              │
│    ↓                                                         │
│  Operators — filter, map, enrich, route, aggregate, window   │
│    ↓                                                         │
│  Built-in state (same process, same shard, pointer speed)    │
│    ├─ KV    — enrichment lookups, no Redis                   │
│    ├─ Queue — dead-letter & retry, no SQS                    │
│    ├─ TS    — derived metrics, no InfluxDB                   │
│    └─ Stream — processed output                              │
│                                                              │
│  One binary. One YAML file. One command.                     │
└──────────────────────────────────────────────────────────────┘
```

## Why Flo?

Every stream processor needs state. You enrich events from Redis. Dead-letter failures to SQS. Write derived metrics to InfluxDB. Orchestrate multi-step reactions in Temporal. Each integration is a network hop, a serialization boundary, a failure mode, and an ops burden.

```
The status quo:                         With Flo:
┌────────┐  ┌────────┐  ┌───────┐      ┌──────────────────────────────┐
│ Kafka  │→ │ Flink  │→ │ Redis │      │           Flo                │
│ (pipe) │  │(process)│  │(state)│      │                              │
└───┬────┘  └───┬────┘  └───────┘      │  stream → process → KV       │
    │           │        ┌───────┐      │                   → queue    │
    │           └──────→ │  SQS  │      │                   → TS       │
    │                    │ (DLQ) │      │                              │
    │           ┌──────→ ┌───────┐      │  Zero serialization.         │
    │           │        │Influx │      │  One binary.                 │
    │           │        │ (TS)  │      │                              │
    └───────────┘        └───────┘      └──────────────────────────────┘

4 serialization boundaries.             0 serialization boundaries.
4 failure modes. 3+ clusters.           1 process. 1 binary.
```

In Flo, the KV lookup is a pointer dereference. The dead-letter queue is a function call. The metrics write is a memory copy. Processing and state live in the same process, on the same shard, in the same memory space.

**Already on Kafka?** Keep it. Flo's Source interface is pluggable — point Flo at your topics and get processing with built-in state in minutes, not weeks.

## Features

- **Stream Processing** — YAML-defined pipelines with filter, map, enrich, route, aggregate, and window operators. Checkpointing, watermarks, exactly-once. No separate cluster
- **KV Store** — Sub-microsecond enrichment lookups from the same shard. CAS, TTL, blocking gets, prefix scans. No Redis sidecar
- **Queues** — Dead-letter, retry, and task dispatch as pipeline sinks. Priority, leases, competing consumers. No SQS
- **Time-Series** — Derived metrics written by pipelines. InfluxDB line protocol, FloQL queries. No InfluxDB
- **Streams** — Raft-replicated commit log with consumer groups and exactly-once delivery. The default data plane for pipelines
- **Actions** — External system triggers with retries, timeouts, and dead-letter. Webhooks, notifications, API calls from pipelines
- **Workflows** — Multi-step orchestration with YAML definitions, signals, timers, and circuit breakers. No Temporal
- **Shard-per-Core** — Shared-nothing architecture with io_uring/kqueue per core. No locks, no GC pauses
- **Raft Consensus** — Linearizable writes, tiered storage (hot RAM → warm disk → cold remote), automatic leader election
- **Built-in Dashboard** — Real-time web UI for monitoring pipelines, streams, keys, queues, and cluster health

## Quick Start

### Install

```bash
# One-line install (macOS / Linux)
curl -fsSL https://raw.githubusercontent.com/floruntime/flo/master/scripts/install.sh | sh

# Homebrew (macOS)
brew tap floruntime/tap
brew install flo
```

### Start the Server

```bash
flo server start
```

Flo starts on port `9000` (client API), `9001` (metrics), and `9002` (dashboard).

### Docker

```bash
docker run -p 9000:9000 -p 9001:9001 -p 9002:9002 ghcr.io/floruntime/flo:latest
```

Or with Docker Compose:

```yaml
services:
  flo:
    image: ghcr.io/floruntime/flo:latest
    ports:
      - "9000:9000"
      - "9001:9001"
      - "9002:9002"
    volumes:
      - flo-data:/data/flo
    restart: unless-stopped

volumes:
  flo-data:
```

```bash
docker compose up -d
```

### Try It Out

```bash
# Deploy a processing pipeline
cat <<EOF > enrich-payments.yaml
name: enrich-payments
sources:
  - type: stream
    stream: raw-payments
operators:
  - type: kv_lookup
    key: "$.merchant_id"
    namespace: merchants
  - type: classify
    rules:
      - name: high-value
        condition: "$.amount > 10000"
sinks:
  - type: stream
    stream: enriched-payments
    tag: high-value
  - type: ts
    measurement: payment-throughput
EOF

flo processing submit -f enrich-payments.yaml
# ✓ Pipeline deployed: enrich-payments

# Streams
flo stream append raw-payments '{"merchant_id": "acme", "amount": 15000}'
flo stream read enriched-payments --last 10

# KV (enrichment state)
flo kv set merchants:acme '{"name": "ACME Corp", "tier": "gold"}'
flo kv get merchants:acme

# Queues
flo queue push jobs '{"task": "send-welcome-email", "to": "alice"}'
flo queue pop jobs

# Time-Series
flo ts write cpu host=web-01 usage=82.5
flo ts query "cpu{host=web-01}[1h] | avg(5m)"
```

## Architecture

Flo is built on a **shard-per-core** architecture. Every CPU core runs an independent shard with its own event loop, processing pipelines, state projections, and partitions — zero cross-thread contention. When a pipeline does a KV enrichment lookup, it's a pointer dereference on the same shard. When it dead-letters to a queue, it's a function call. No serialization. No network.

```
┌───────────────────────────────────────────────────────────────┐
│                           NODE                                │
│                                                               │
│  Acceptor Thread     TCP accept → peek → route → hand-off     │
│                                                               │
│  Shard 0 Thread      Reactor → Dispatcher → Partitions        │
│  Shard 1 Thread      Reactor → Dispatcher → Partitions        │
│  Shard N Thread      Reactor → Dispatcher → Partitions        │
│                                                               │
│  Dashboard Thread    HTTP REST API → Inbox queries            │
│  Metrics Thread      Prometheus /metrics → per-shard agg      │
└───────────────────────────────────────────────────────────────┘
```

### Write Path

```
Client → Acceptor → Shard (correct one) → Dispatcher
  → Handler parses proto.Request
  → Raft propose (if write)
  → Raft commit → UAL append
  → ProjectionRouter fans out to KV/Queue/TS/Stream Projection
  → Response to client
```

### Read Path

```
Client → Shard → Dispatcher → Handler
  → Query Projection directly (no Raft needed for reads)
  → If cross-shard: Inbox message → remote Shard → Inbox response
  → Response to client
```

### Key Components

| Component | Role |
|-----------|------|
| **Acceptor** | Dedicated thread. Accepts TCP, peeks first request, resolves routing key → shard, hands off fd via pipe |
| **Reactor** | Single unified event loop per shard (kqueue/io_uring). Handles all I/O, timers, and inbox draining |
| **Dispatcher** | Table-driven opcode→handler routing (~300 lines). Handlers self-register |
| **Router** | Hash-based routing: `hash(key) → partition → shard` |
| **Inbox** | MPSC ring buffer with 32-byte envelopes for cross-shard communication |
| **Slab** | Per-shard slab allocator for cross-shard payload lifetimes |
| **UAL** | Unified Append Log — hot ring buffer (mmap) + disk segments. The Raft log IS the UAL |
| **Projections** | Derived views rebuilt from UAL on recovery: KV (hash+MVCC), Queue (heaps+leases), Stream (offsets+groups), TS (columnar blocks) |

### Storage: "Log is Data"

All writes go through the **Unified Append Log (UAL)**. The Raft consensus log and the storage log are the same thing — no separate WAL.

| Tier | Medium | Latency | Purpose |
|------|--------|---------|---------|
| Hot | RAM ring buffer | ~µs | Recent entries, active reads |
| Warm | Local disk segments | ~ms | Historical data, memory-mapped I/O |
| Cold | Remote (S3/GCS) | ~100ms | Long-term archival |

### Performance

Benchmarked on the new architecture (Apple M-series, single core, ReleaseFast):

| Benchmark | Throughput | Latency |
|-----------|-----------|--------|
| UAL append | 12.9M ops/sec | 77 ns/op |
| KV put | 4.4M ops/sec | 225 ns/op |
| KV get | 11.9M ops/sec | 84 ns/op |
| KV scan | 6.0M ops/sec | 165 ns/op |
| Inbox SPSC | 28.5M msg/sec | 35 ns/msg |

## Stream Processing

The core of Flo. Define pipelines in YAML, deploy with one command. Flo handles checkpointing, watermarks, and state management — with KV enrichment, queue dead-letter, and TS metrics all built in. No Flink cluster. No Redis sidecar. No SQS subscription.

```yaml
kind: ProcessingJob
name: user-spend-alerts
version: "1.0.0"

sources:
  - name: transactions
    stream: payment-events
    start: latest
    watermark:
      strategy: bounded-out-of-order
      max_delay_ms: 5000

operators:
  - name: by-user
    type: keyBy
    input: transactions
    key: "$.user_id"

  - name: spend-per-hour
    type: window
    input: by-user
    window: { type: tumbling, size: 1h }
    aggregate: { type: sum, field: "$.amount" }
    trigger: event-time

  - name: high-spenders
    type: filter
    input: spend-per-hour
    condition: "$.value > 10000"

sinks:
  - name: alerts
    input: high-spenders
    stream: high-spend-alerts

checkpointing:
  interval: 30s
  mode: exactly-once
```

```bash
flo processing submit --file jobs/user-spend-alerts.yaml
flo processing status user-spend-alerts
flo processing savepoint user-spend-alerts
flo processing stop user-spend-alerts
```

## Actions & Workers

Actions provide durable execution of external business logic. Register a handler, and Flo manages dispatch, retries, timeouts, and dead-letter handling.

```typescript
import { FloClient } from '@floruntime/node';

const client = new FloClient('localhost:9000');
await client.connect();

const worker = client.newWorker({ concurrency: 5 });

worker.registerAction('send-email', async (task) => {
  const { to, subject, body } = task.input;
  await sendEmail(to, subject, body);
  return { status: 'sent', timestamp: Date.now() };
});

await worker.start();
```

Invoke actions from anywhere:

```bash
flo action invoke send-email '{"to": "alice@example.com", "subject": "Welcome!"}'
flo action status <run-id>
```

## Workflows

Workflows orchestrate multi-step business processes using YAML definitions. They compose Actions with built-in resilience: circuit breakers, health-weighted routing, retries, and signals.

```yaml
kind: Workflow
name: process-order
version: "1.0.0"

plans:
  payment:
    selection: health-weighted
    executors:
      - name: stripe
        run: "@actions/charge-stripe"
        priority: 100
        breaker: { failureThreshold: 5, cooldownMs: 60000 }
      - name: braintree
        run: "@actions/charge-braintree"
        priority: 90

start:
  run: "@actions/validate-order"
  transitions:
    success: charge
    failure: flo.Failed

steps:
  charge:
    run: "@plan/payment"
    transitions:
      success: ship
      exhausted: manual_review

  ship:
    run: "@actions/create-shipment"
    transitions:
      success: flo.Completed

  manual_review:
    run: flo.WaitForSignal
    signal: manager_approval
    timeout: 24h
    transitions:
      approved: charge
      rejected: flo.Failed
      timeout: flo.Failed
```

```bash
flo workflow create --file order-workflow.yaml
flo workflow start process-order '{"order_id": "ORD-123", "amount": 99.99}'
flo workflow signal <run-id> manager_approval '{"decision": "approved"}'
flo workflow status <run-id>
```

## Configuration

Flo is configured via `flo.toml`:

```toml
[server]
port = 9000
bind = "0.0.0.0"
data_dir = "/data/flo"
shards = 0            # 0 = auto-detect CPU count

[storage]
durability = "async_flush"
hot_buffer_capacity = 67108864

[logging]
level = "info"        # debug, info, warn, error

[metrics]
enabled = true

[dashboard]
enabled = true
bind = "0.0.0.0"
```

Environment variables override config values:

| Variable | Default | Description |
|---|---|---|
| `FLO_DATA_DIR` | `/data/flo` | Data directory |
| `FLO_LISTEN_ADDR` | `0.0.0.0:9000` | Client API address |
| `FLO_METRICS_ADDR` | `0.0.0.0:9001` | Metrics endpoint |
| `FLO_NODE_ID` | - | Node ID (required for clustering) |
| `FLO_LOG_LEVEL` | `info` | Log level |
| `FLO_LOG_FORMAT` | `text` | Log format (`text` or `json`) |

## Module Structure

```
src/
├── main.zig                 # Entry point
├── lib.zig                  # Public API for tests
├── stdx.zig                 # Standard library extensions
├── log.zig                  # Structured logging
│
├── protocol/                # Wire protocol (binary + RESP)
│   ├── proto.zig            #   OpCode enum, headers, TLV encoding
│   ├── request_builder.zig  #   Request construction helper
│   ├── result.zig           #   CommandResult response types
│   └── resp.zig             #   RESP Redis protocol parser
│
├── node/                    # Node layer (shard-per-core)
│   ├── acceptor.zig         #   TCP accept + routing hand-off
│   ├── reactor.zig          #   Unified event loop (kqueue/io_uring)
│   ├── shard.zig            #   Shard — owns everything on its core
│   ├── dispatcher.zig       #   Opcode → handler routing table
│   ├── router.zig           #   hash → partition → shard
│   ├── inbox.zig            #   MPSC ring for cross-shard messages
│   ├── slab.zig             #   Slab allocator for payloads
│   ├── connection.zig       #   Connection + protocol detection
│   ├── shard_walker.zig     #   ShardWalker(T) for list/scan
│   ├── runtime.zig          #   Boot sequence, thread spawning
│   └── task_scheduler.zig   #   Cooperative background tasks
│
├── storage/                 # Storage layer
│   ├── ual/                 #   Unified Append Log
│   │   ├── entry.zig        #     Entry format (CRC, term, index, opcode, key, value)
│   │   ├── ual.zig          #     Hot ring buffer
│   │   ├── segment.zig      #     Disk segment format
│   │   ├── writer.zig       #     Segment writer
│   │   └── reader.zig       #     Segment reader
│   ├── partition.zig        #   Partition (Raft + UAL + Projections)
│   ├── snapshot.zig         #   Snapshot manager
│   └── memory.zig           #   Shard memory controller
│
├── raft/                    # Raft consensus
│   ├── node.zig             #   State machine (Leader/Follower/Candidate)
│   ├── log.zig              #   Raft log backed by UAL
│   ├── election.zig         #   Leader election + pre-vote
│   ├── replication.zig      #   Log replication (pipelined)
│   ├── transport.zig        #   Per-shard TCP transport
│   └── snapshot.zig         #   InstallSnapshot RPC
│
├── projection/              # Projection engines (derived views from UAL)
│   ├── router.zig           #   Fans committed entries to projections
│   ├── kv.zig               #   KVProjection (hash table + MVCC)
│   ├── queue.zig            #   QueueProjection (heaps + leases)
│   ├── stream.zig           #   StreamProjection (offsets + groups)
│   └── ts.zig               #   TSProjection (columnar blocks)
│
├── kv/handler.zig           # KV opcodes → KVProjection
├── stream/handler.zig       # Stream opcodes → StreamProjection
├── queue/handler.zig        # Queue opcodes → QueueProjection
├── ts/handler.zig           # TS opcodes → TSProjection
├── actions/handler.zig      # Actions + WASM execution
├── workflow/handler.zig     # Workflow orchestration
├── processing/              # Stream processing engine
│
├── cluster/                 # Clustering
│   ├── coordinator.zig      #   Controller Raft on Shard 0
│   ├── partition_table.zig  #   Partition → Node mapping
│   ├── forwarder.zig        #   Cross-node request forwarding
│   ├── gossip.zig           #   SWIM protocol
│   └── membership.zig       #   Join/leave/fail state machine
│
├── cli/                     # CLI client
├── config/                  # Configuration (flo.toml)
├── metrics/                 # Prometheus metrics
├── dashboard/               # REST API for web UI
└── testing/                 # Test infrastructure

web/                         # React dashboard
tests/                       # E2E and integration tests
bench/                       # Benchmarks
```

## Building from Source

Requires [Zig 0.15.2+](https://ziglang.org/download/).

```bash
git clone https://github.com/floruntime/flo.git
cd flo

# Build
zig build                              # Debug build
zig build -Doptimize=ReleaseFast       # Release build

# Run
./zig-out/bin/flo server start

# Test
zig build test --summary all               # Unit tests (1040 tests)
zig build test-integration --summary all   # Integration tests
zig build test-e2e --summary all           # E2E acceptance tests

# Benchmarks
zig build bench                        # Build benchmarks (ReleaseFast)
./zig-out/bin/bench-ual                # UAL throughput
./zig-out/bin/bench-kv                 # KV projection ops/sec
./zig-out/bin/bench-inbox              # Inbox MPSC throughput
```

## SDKs

| Language | Package | Status |
|---|---|---|
| **TypeScript/JS** | [`@floruntime/node`](https://github.com/floruntime/flo-js) / [`@floruntime/web`](https://github.com/floruntime/flo-js) | ✅ Stable |
| **Python** | [`flo-python`](https://github.com/floruntime/flo-python) | ✅ Stable |
| **Go** | [`flo-go`](https://github.com/floruntime/flo-go) | ✅ Stable |
| **Zig** | [`flo-zig`](https://github.com/floruntime/flo-zig) | 🚧 In Progress |

## Documentation

- [Architecture Overview](docs/architecture/OVERVIEW.md) — Shard-per-core design, data flow, storage model
- [Node & Network Design](docs/architecture/NODE_NETWORK_DESIGN.md) — Acceptor, Reactor, Dispatcher, Inbox
- [Unified Storage Design](docs/architecture/UNIFIED_STORAGE_DESIGN.md) — UAL, Projections, Memory Controller
- [Primitives Design](docs/architecture/PRIMITIVES_DESIGN.md) — Streams, KV, Queues internals
- [Actions Design](docs/architecture/ACTIONS_DESIGN.md) — Durable execution layer
- [Processing Design](docs/architecture/PROCESSING_DESIGN.md) — Stream processing engine
- [Workflow Design](docs/architecture/WORKFLOW_DESIGN.md) — Orchestration and resilience

## Project Status

Flo is in **active development**. The stream processing engine and all built-in state primitives are complete.

| Component | Status |
|---|---|
| Stream Processing (operators, windows, checkpoints) | ✅ Complete |
| Shard-per-core runtime (Acceptor, Reactor, Dispatcher) | ✅ Complete |
| Unified Append Log (UAL) | ✅ Complete |
| Raft consensus (election, replication, snapshots) | ✅ Complete |
| Projection engines (KV, Queue, Stream, TS) | ✅ Complete |
| Streams (append, read, consumer groups) | ✅ Complete |
| Key-Value (get, set, delete, scan, CAS, blocking get) | ✅ Complete |
| Queues (push, pop, ack, dead-letter) | ✅ Complete |
| Time-Series (write, query, FloQL) | ✅ Complete |
| Actions (register, invoke, WASM) | ✅ Complete |
| Workflows (YAML, signals, timers) | ✅ Complete |
| Web Dashboard | ✅ Complete |
| SWIM gossip + cluster membership | ✅ Complete |
| Cross-node request forwarding | ✅ Complete |
| Kafka Source | 🚧 In Progress |
| Cold Storage (S3/GCS) | 🔲 Local backend complete, remote planned |

## Contributing

Contributions are welcome! Please see the [architecture docs](docs/architecture/) to understand the codebase structure before diving in.

```bash
# Run tests
zig build test --summary all

# Run benchmarks
zig build bench && ./zig-out/bin/bench-ual

# Format code
zig fmt src/
```

## License

MIT
