<p align="center">
  <h1 align="center">Flo</h1>
  <p align="center">The Universal Distributed Runtime</p>
  <p align="center">
    <a href="https://github.com/floruntime/flo/actions"><img alt="CI" src="https://github.com/floruntime/flo/actions/workflows/release.yml/badge.svg"></a>
    <a href="https://github.com/floruntime/flo/releases"><img alt="Release" src="https://img.shields.io/github/v/release/floruntime/flo?include_prereleases"></a>
    <a href="https://github.com/floruntime/flo/blob/dev/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
    <a href="https://ziglang.org"><img alt="Built with Zig" src="https://img.shields.io/badge/built%20with-Zig-F7A41D.svg"></a>
  </p>
</p>

---

Flo is a distributed runtime that unifies **streams**, **key-value storage**, **queues**, **durable actions**, and **workflow orchestration** in a single binary. Instead of stitching together separate systems for each concern, Flo provides all five as primitives over one Raft-replicated log — with a single connection, consistent durability, and zero integration overhead.

```
┌─────────────────────────────────────────────────────────────┐
│             Layer 3: Orchestration                          │
│   Workflows · Plans · Signals · Timers                      │
├─────────────────────────────────────────────────────────────┤
│             Layer 2: Durable Execution + Processing         │
│   Actions · Workers · Stream Processing · WASM Operators    │
├─────────────────────────────────────────────────────────────┤
│             Layer 1: Core Primitives                        │
│   Streams · KV · Queues · Time-Series                       │
├─────────────────────────────────────────────────────────────┤
│             Layer 0: Unified Append Log + Raft Consensus    │
│   The log IS the database                                   │
└─────────────────────────────────────────────────────────────┘
```

## Why Flo?

Modern backends tend to accumulate a separate system for every concern — an event log here, a cache there, a job queue, a workflow engine. Each one brings its own connection pool, failure mode, and consistency model. Developers end up spending as much time wiring infrastructure together as building the actual product.

Flo takes a different approach. All five primitives share one Raft consensus log as their storage engine. Streams expose the log directly. KV and Queues are deterministic state machines that consume it. Because everything lives in the same replication path, there are no dual-write race conditions, no cross-system sync issues, and one fewer thing to operate.

## Features

- **Streams** — Partitioned, append-only commit log with consumer groups, configurable storage tiers, and exactly-once delivery
- **Key-Value** — Strongly consistent, versioned storage with compare-and-swap (CAS), TTL, blocking gets, and prefix scans
- **Queues** — Priority queues with competing consumers and dead-letter support; delivery is currently at-most-once
- **Time-Series** — Columnar write buffers with block index, InfluxDB line protocol ingest, and FloQL query language
- **Actions** — Durable execution of external business logic with automatic retries, timeouts, and dead-letter handling
- **Stream Processing** — Continuous YAML pipelines (filter, map, keyed aggregates, KV enrichment) over streams and time-series — no separate cluster needed
- **Workflows** — Multi-step orchestration with YAML definitions, signals, timers, circuit breakers, and health-weighted routing
- **Thread-per-Core** — Shared-nothing architecture with io_uring/kqueue per core. No locks, no GC pauses
- **Raft Consensus** — Linearizable writes, tiered storage (hot RAM → warm disk), automatic leader election
- **Built-in Dashboard** — Real-time web UI for monitoring streams, keys, queues, and cluster health

## Quick Start

### Install

```bash
# One-line install (macOS / Linux)
curl -fsSL https://raw.githubusercontent.com/floruntime/flo/dev/scripts/install.sh | sh
```

Or download a tarball for your platform from
[Releases](https://github.com/floruntime/flo/releases); each has a `.sha256` beside it.

The macOS binaries need macOS 15 or later.

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

> **Note:** The image's `ENTRYPOINT` is a small shim that fixes ownership on a
> freshly-mounted `/data/flo` (root-owned by default on new ext4/xfs volumes),
> then drops to uid 1000 via `su-exec`. Don't override it from your orchestrator
> — use `command:` / `CMD` for arguments, not `entrypoint:`. Overriding it
> bypasses the chown and either traps you on `error.AccessDenied` or leaves flo
> running as root.

### Try It Out (CLI)

```bash
# Key-Value
flo kv set user:alice '{"name": "Alice", "role": "admin"}'
flo kv get user:alice

# Streams
flo stream append events '{"type": "signup", "user": "alice"}'
flo stream read events --limit 10

# Queues
flo queue enqueue jobs '{"task": "send-welcome-email", "to": "alice"}'
flo queue dequeue jobs

# Time-Series
flo ts write cpu --tags host=web-01 --value 82.5
flo ts floql 'cpu{host=web-01}[1h] | window(5m) | avg()'
```

## Architecture

Flo is built on a **log-native, shard-per-core** architecture. Every CPU core runs an independent shard with its own event loop, memory, and partitions — zero cross-thread contention.

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
  → If cross-shard: request on the owner's inbox → answer on the asker's reply ring
  → Response to client
```

### Key Components

| Component | Role |
|-----------|------|
| **Acceptor** | Dedicated thread. Accepts TCP, peeks first request, resolves routing key → shard, hands off fd via pipe |
| **Reactor** | Single unified event loop per shard (kqueue/io_uring). Handles all I/O, timers, and inbox draining |
| **Dispatcher** | Table-driven opcode→handler routing (~300 lines). Handlers self-register |
| **Router** | Hash-based routing: `hash(key) → partition → shard` |
| **Mailbox** | Per shard: an inbox for other shards' requests, a reply ring for answers to its own, and wake flags; MPSC rings of 32-byte envelopes |
| **UAL** | Unified Append Log — hot ring buffer (mmap) + disk segments. The Raft log IS the UAL |
| **Projections** | Derived views rebuilt from UAL on recovery: KV (hash+MVCC), Queue (heaps+leases), Stream (offsets+groups), TS (columnar blocks) |

### Storage: "Log is Data"

All writes go through the **Unified Append Log (UAL)**. The Raft consensus log and the storage log are the same thing — no separate WAL.

| Tier | Medium | Latency | Purpose |
|------|--------|---------|---------|
| Hot | RAM ring buffer | ~µs | Recent entries, active reads |
| Warm | Local disk segments | ~ms | Historical data, memory-mapped I/O |

### Performance

Benchmarked on the new architecture (Apple M-series, single core, ReleaseFast):

| Benchmark | Throughput | Latency |
|-----------|-----------|--------|
| UAL append | 12.9M ops/sec | 77 ns/op |
| KV put | 4.4M ops/sec | 225 ns/op |
| KV get | 11.9M ops/sec | 84 ns/op |
| KV scan | 6.0M ops/sec | 165 ns/op |
| Inbox SPSC | 28.5M msg/sec | 35 ns/msg |

## SDK Examples

### TypeScript / JavaScript

```bash
npm install @floruntime/node
```

```typescript
import { FloClient } from '@floruntime/node';

const client = new FloClient('localhost:9000');
await client.connect();

// Key-Value
await client.kv.set('user:alice', Buffer.from('{"role": "admin"}'));
const result = await client.kv.get('user:alice');

// Streams
await client.stream.append('events', Buffer.from('{"type": "signup"}'));
const records = await client.stream.read('events', { maxRecords: 10 });

// Queues
await client.queue.push('jobs', Buffer.from('send-welcome-email'));
const msg = await client.queue.pop('jobs');

await client.close();
```

### Python

```bash
pip install flo-python
```

```python
import asyncio
from flo import FloClient

async def main():
    client = FloClient("localhost:9000")
    await client.connect()

    # Key-Value
    await client.kv.set("user:alice", b'{"role": "admin"}')
    result = await client.kv.get("user:alice")

    # Streams
    await client.stream.append("events", b'{"type": "signup"}')
    records = await client.stream.read("events", max_records=10)

    # Queues
    await client.queue.push("jobs", b"send-welcome-email")
    msg = await client.queue.pop("jobs")

    await client.close()

asyncio.run(main())
```

### Go

```bash
go get github.com/floruntime/flo-go
```

```go
package main

import (
    "context"
    "fmt"
    flo "github.com/floruntime/flo-go"
)

func main() {
    client, _ := flo.NewClient("localhost:9000")
    defer client.Close()
    ctx := context.Background()

    // Key-Value
    client.KV.Set(ctx, "user:alice", []byte(`{"role": "admin"}`), nil)
    result, _ := client.KV.Get(ctx, "user:alice", nil)
    fmt.Println(string(result.Value))

    // Streams
    client.Stream.Append(ctx, "events", []byte(`{"type": "signup"}`), nil)
    records, _ := client.Stream.Read(ctx, "events", nil)

    // Queues
    client.Queue.Push(ctx, "jobs", []byte("send-welcome-email"), nil)
    msg, _ := client.Queue.Pop(ctx, "jobs", nil)
}
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

## Stream Processing

Flo includes a built-in stream processing engine for continuous pipelines over streams and time-series. A job is a YAML definition — sources, a chain of operators, sinks — and runs inside the node that owns its source; the source cursor is persisted, so a restarted job resumes where it stopped.

```yaml
kind: Processing
name: high-spenders

sources:
  - name: transactions
    stream:
      name: payment-events

operators:
  - type: keyby
    name: by-user
    key_expression: "$.user_id"
  - type: aggregate              # running total per user
    name: spend
    function: sum
    field: "$.amount"
  - type: filter
    name: over-limit
    condition: "json:$.value>10000"

sinks:
  - name: alerts
    stream:
      name: high-spend-alerts
```

```bash
flo processing submit high-spenders.yaml   # prints the job id
flo processing list
flo processing status <job-id>
flo processing stop <job-id>
```

Operators: `filter`, `map`, `flatmap`, `keyby`, `aggregate` (running, or per `window: count`), `kv_lookup`, `classify`, `passthrough`.

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
        breaker: { failure_threshold: 5, cooldown_ms: 60000 }
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
# bind = "127.0.0.1"   # loopback by default: the dashboard authenticates no one yet
# hosts = "flo.example.internal, 10.0.1.5"   # names/addresses you browse to it by (besides localhost)
```

The server takes its settings from `flo.toml` and its flags. The one
environment variable it reads is `FLO_CLUSTER_SECRET`, the cluster's shared
secret. The CLI reads `FLO_ENDPOINT`, `FLO_PORT` and `FLO_NAMESPACE` when
`--endpoint`, `--port` or `--namespace` isn't given.

## Module Structure

```
src/
├── main.zig                 # Entry point
├── lib.zig                  # Public API for tests
├── stdx.zig                 # Standard library extensions
├── log.zig                  # Structured logging
│
├── protocol/                # Wire protocol (binary)
│   ├── proto.zig            #   OpCode enum, headers, TLV encoding
│   ├── request_builder.zig  #   Request construction helper
│   └── result.zig           #   CommandResult response types
│
├── node/                    # Node layer (shard-per-core)
│   ├── acceptor.zig         #   TCP accept + routing hand-off
│   ├── reactor.zig          #   Unified event loop (kqueue/io_uring)
│   ├── shard.zig            #   Shard — owns everything on its core
│   ├── dispatcher.zig       #   Opcode → handler routing table
│   ├── router.zig           #   hash → partition → shard
│   ├── inbox.zig            #   MPSC ring for cross-shard messages
│   ├── mailbox.zig          #   Inbox, reply ring and wake flags per shard
│   ├── reply_pool.zig       #   Room for answers to requests sent to other shards
│   ├── slab.zig             #   Slab allocator (not yet used for cross-shard payloads)
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
│   └── partition_table.zig  #   Partition → Node mapping (not yet used for routing)
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

Requires [Zig 0.17.x](https://ziglang.org/download/); `build.zig` refuses any other version.

```bash
git clone https://github.com/floruntime/flo.git
cd flo

# Build
zig build                              # Debug build
zig build -Doptimize=ReleaseFast       # Release build

# Run
./zig-out/bin/flo server start

# Test
zig build test --summary all               # Unit tests
zig build test-integration --summary all   # Integration tests
zig build test-e2e --summary all           # E2E acceptance tests

# Only tests whose name contains a substring; repeat to run several.
# A filter that matches no test in the step fails it; `|` is refused.
zig build test-e2e -Dtest-filter=kv -Dtest-filter=queue

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

Flo is in **active development**. The core runtime has been rewritten from the ground up with a shard-per-core architecture.

| Component | Status |
|---|---|
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
| Stream Processing (operators, keyed and count-window aggregates) | ✅ Complete |
| Web Dashboard | ✅ Complete |
| Cluster membership by the replicated log | ✅ Complete |
| Writes forwarded to the leader | ✅ Complete |

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

Apache-2.0 — see [LICENSE](./LICENSE).
