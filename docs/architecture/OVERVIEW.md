# Flo Architecture Overview

> Shard-per-core distributed runtime with unified append log storage.

## Design Principles

1. **The log is the database.** All writes go through the Unified Append Log (UAL). The Raft consensus log and the storage log are the same thing — no separate WAL.
2. **Thread-per-core, shared-nothing.** Each CPU core runs an independent shard. Shards never share mutable state; they communicate via lock-free MPSC inboxes.
3. **Projections, not tables.** KV, Queue, Stream, and Time-Series are deterministic projections derived from the UAL. On recovery, projections are rebuilt by replaying the log.
4. **Handler self-registration.** The Dispatcher is a ~300-line opcode→handler lookup table. Each domain subsystem registers its own handlers — the Dispatcher imports nothing.

## System Architecture

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

### Acceptor

Dedicated thread running a TCP accept loop. On each new connection:

1. Accept the socket
2. Peek at the first bytes to detect the protocol (binary, RESP, HTTP, WebSocket)
3. Parse the routing key from the first request
4. Hash the routing key → partition → shard
5. Hand off the file descriptor to the target shard via a pipe

Connections have **shard affinity** — once assigned, a connection stays on its shard to avoid cross-core cache thrashing.

### Shard

Each shard is a CPU-pinned OS thread that owns:

- **Reactor** — Single kqueue/io_uring event loop handling all I/O, timers, and inbox draining
- **Dispatcher** — Opcode→handler table for request routing
- **ConnectionPool** — All connections assigned to this shard
- **Partitions** — Raft groups with UAL + Projections (partition_id % shard_count = shard_id)
- **Inbox** — MPSC ring buffer for receiving cross-shard messages
- **Slab allocator** — For cross-shard payload lifetimes

The shard's main loop:

```
loop {
    reactor.poll()                  // I/O events
    processEvents()                 // handle ready connections
    drainInbox()                    // cross-shard messages
    waiter_pool.expireTimeouts()    // blocking operation expiry
    taskScheduler.tick()            // background tasks
}
```

### Dispatcher

Table-driven routing with a flat 256-slot handler array:

```zig
pub const Dispatcher = struct {
    handlers: [256]?HandlerFn = [_]?HandlerFn{null} ** 256,

    pub fn dispatch(self, shard, conn, req) void {
        if (self.handlers[@intFromEnum(req.header.opcode)]) |handler| {
            handler(shard, conn, req);
        } else {
            conn.sendError(req.header.request_id, .unknown_opcode);
        }
    }
};
```

Each subsystem (KV, Stream, Queue, etc.) calls `dispatcher.register(.opcode, handler)` during initialization — the Dispatcher never imports domain code.

### Router

Hash-based partition assignment:

```
hash(key) → partition_id
partition_id % shard_count → shard_id
```

If a request lands on the wrong shard, the handler detects the mismatch and forwards via the Inbox.

## Storage Model

### Unified Append Log (UAL)

The UAL is the single source of truth. Every mutation (KV put, queue enqueue, stream append) is an entry in this log.

```
Entry format:
┌──────┬──────┬───────┬────────┬─────────────┬─────┬───────┐
│ CRC  │ Term │ Index │ OpCode │ Namespace   │ Key │ Value │
│ u32  │ u64  │ u64   │ u16    │ u64 (hash)  │ var │ var   │
└──────┴──────┴───────┴────────┴─────────────┴─────┴───────┘
```

Storage tiers:

| Tier | Implementation | Purpose |
|------|---------------|---------|
| **Hot** | mmap'd ring buffer in RAM | Recent entries for active reads |
| **Warm** | Disk segment files | Historical data, memory-mapped |
| **Cold** | S3/GCS/Azure (planned) | Long-term archival |

The Raft log IS the UAL — there is no separate write-ahead log.

### Projections

Projections are deterministic state machines that consume UAL entries and maintain specialized indexes:

| Projection | Data Structure | Purpose |
|-----------|---------------|---------|
| **KVProjection** | Hash table + MVCC version chain | Key-value lookups with versioning |
| **QueueProjection** | Priority heaps + lease tracker + DLQ | Ordered delivery with leases |
| **StreamProjection** | Offset index + consumer group state | Append-only reads with groups |
| **TSProjection** | Columnar write buffers + block index | Time-series aggregation queries |

On recovery, projections are rebuilt by replaying UAL entries from the last snapshot. This means the UAL is the only thing that needs to be durable — projections are ephemeral derived state.

### Partition

A partition binds together:

- **Raft node** — Leader/Follower/Candidate state machine
- **UAL instance** — The log for this partition
- **Projection router** — Fans committed entries to the appropriate projection

Assignment: `partition_id % shard_count = shard_id`

## Write Path

```
1. Client sends request to Acceptor
2. Acceptor routes to correct Shard
3. Dispatcher maps opcode to Handler
4. Handler calls Raft.propose(entry)
5. Raft replicates to quorum
6. On commit: UAL.append(entry)
7. ProjectionRouter.apply(entry) → updates KV/Queue/Stream/TS
8. Response sent to client
```

Write latency: ~1 RTT to quorum + local UAL write.

## Read Path

```
1. Client sends request to Shard
2. Dispatcher maps opcode to Handler
3. Handler queries Projection directly (no Raft needed)
4. If cross-shard: Inbox message → remote Shard → response
5. Response sent to client
```

Read latency: 0 RTT for local reads on the leader.

## Cross-Shard Communication

Shards communicate via 32-byte envelopes on lock-free MPSC ring buffers:

```zig
const Envelope = extern struct {
    tag: u8,              // message type
    src_shard: u8,        // sender shard ID
    partition_id: u16,    // target partition
    payload_ptr: *anyopaque, // slab-allocated payload
    payload_len: u32,
    sequence: u64,        // for response matching
    padding: [8]u8,
};
```

Payloads are allocated from the slab allocator, not embedded in the envelope. This keeps the MPSC ring's cache line footprint minimal.

## Clustering

- **SWIM gossip** — failure detection and membership protocol
- **Controller Raft** — runs on Shard 0, manages partition table
- **Partition Table** — maps partition → node, updated on membership changes
- **Forwarder** — routes requests to the correct node when partitions aren't local

## Performance

Benchmarked on the new architecture (single-core, debug build):

| Benchmark | Throughput |
|-----------|-----------|
| UAL append | 10M+ ops/sec |
| KV put | 5.9M ops/sec |
| KV get | 20M+ ops/sec |
| Inbox SPSC | 16M+ msgs/sec |

See [bench/RESULTS.md](../../bench/RESULTS.md) for detailed results and comparisons with the old architecture.
