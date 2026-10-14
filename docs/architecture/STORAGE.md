# Storage Internals

> How Flo stores data, and why it works the way it does.

This document is a companion to the [Architecture Overview](OVERVIEW.md). Where that document covers the full system — Acceptor, Shards, Dispatcher, clustering — this one zooms in on the storage layer: the Unified Append Log, projection engines, and snapshots.

If you're trying to understand why a KV put eventually lands on disk, or how a queue's priority heap survives a crash, this is the right document.

---

## Table of Contents

- [Philosophy: The Log Is the Database](#philosophy-the-log-is-the-database)
- [Unified Append Log (UAL)](#unified-append-log-ual)
  - [Entry Format](#entry-format)
  - [Two-Tier Storage](#two-tier-storage)
  - [Segment Format](#segment-format)
- [Projections](#projections)
  - [KV Projection](#kv-projection)
  - [Queue Projection](#queue-projection)
  - [Stream Projection](#stream-projection)
  - [Time-Series Projection](#time-series-projection)
  - [Projection Router](#projection-router)
- [Snapshots](#snapshots)
  - [The Problem with RAM-Only State](#the-problem-with-ram-only-state)
  - [Snapshot Format](#snapshot-format)
  - [Lifecycle](#lifecycle)
  - [Crash Safety](#crash-safety)
- [Recovery](#recovery)
- [Directory Layout](#directory-layout)
- [What's Not Here Yet](#whats-not-here-yet)

---

## Philosophy: The Log Is the Database

Most databases maintain two copies of every write: a write-ahead log (WAL) for durability and a primary data structure (B-tree, LSM tree, heap file) for queries. The WAL exists solely so that the database can recover the primary structure after a crash. Once the primary structure is flushed to disk, the WAL entries are discarded.

Flo doesn't do this. The log *is* the primary data structure.

The Unified Append Log captures every mutation — KV puts, queue enqueues, stream appends, time-series writes — as a sequenced, typed entry. The Raft consensus log and the storage log are the same thing. There is no separate WAL.

Everything else — the KV hash table, the queue's priority heap, the time-series columnar buffers — is a **projection**: a derived view rebuilt deterministically from the log. If you deleted every projection and replayed the log from the beginning, you'd get back to exactly the same state. The projections exist for performance (you don't want to scan the entire log to answer a KV get), but they are not the source of truth.

This has a few consequences that might feel strange at first:

1. **Stream data has no projection at all.** Stream reads go directly to the UAL. The log entries *are* the stream records — there's nothing to project.

2. **Snapshots don't capture the log.** They capture projection state. On recovery, you load the snapshot (to skip replaying ancient history) and then replay recent log entries to bring projections up to date.

---

## Unified Append Log (UAL)

The UAL is a sequence of entries, each identified by a monotonically increasing 64-bit index. In a running system the Raft leader assigns indices; in a single-node setup, the partition just increments.

### Entry Format

Every UAL entry has a 40-byte header followed by a variable-length payload:

```
┌──────────┬──────────┬───────┬───────────┬────────────┬─────────────┬───────┐
│ CRC32C   │ entry_   │ flags │ raft_term │ raft_index │ timestamp   │ pay-  │
│ (4B)     │ type(2B) │ (2B)  │ (8B)      │ (8B)       │ _ns (8B)    │ load  │
└──────────┴──────────┴───────┴───────────┴────────────┴─────────────┴───────┘
```

The `entry_type` field tells you what kind of mutation this is: `kv_put`, `queue_enqueue`, `stream_append`, `ts_write`, `raft_noop`, and so on — around 30 types total, with room for more. The CRC covers the header and payload together.

Payload layout varies by entry type. For KV operations, the payload is a `CommandPayload` with key length, value length, namespace hash, and then the key and value bytes inline. For queue operations, it includes priority and delay metadata. The projections know how to parse their own payloads.

### Two-Tier Storage

Entries live in one of two places, depending on age:

**Hot tier** — an mmap'd ring buffer in RAM. This is where writes land. The ring has a fixed capacity (default 64 MB) and evicts the oldest entries when full. Recent reads — the common case — hit this tier. The ring buffer is the fastest path: O(1) by index, no syscalls, no copies.

**Warm tier** — sealed disk segments (`.flseg` files). When entries age out of the hot ring, they've typically already been flushed to a segment file. These are memory-mapped for read access. The warm tier is bounded by local disk space and a configurable segment count.

The warm tier also maintains an in-process `warm_store` hash map: when the hot ring evicts an entry, a copy of its payload is kept in the warm store so reads don't have to go to disk immediately. The warm store is bounded (default 32 MB) and evicts lowest-index entries first when over budget.

### Segment Format

Each sealed segment is a `.flseg` file with this layout:

```
[SegmentHeader]
  magic:       "FLOSEG\0\0"
  version:     u16
  segment_id:  u64
  first_index: u64
  last_index:  u64
  entry_count: u32
  data_size:   u64

[Entry 0]
[Entry 1]
...
[Entry N]

[SparseIndex]
  (sampled every ~256 entries: index → file offset)

[SegmentFooter]
  index_offset: u64
  index_count:  u32
  crc32c:       u32
```

The sparse index lets you binary-search into the segment by UAL index without scanning every entry. The CRC in the footer covers the entire segment — header, entries, and index — so corruption is detected on read.

---

## Projections

A projection is a specialized data structure that consumes UAL entries and maintains queryable state. Each projection implements a common interface:

```zig
const ProjectionVTable = struct {
    applyFn: *const fn (ctx: *anyopaque, entry: *const Entry) !void,
    memoryUsageFn: *const fn (ctx: *anyopaque) usize,
};
```

Every committed entry goes through the **Projection Router**, which dispatches by entry type to the appropriate projection. The router tracks an `applied_index` for idempotency — replaying an entry the router has already seen is a silent no-op. This is critical for crash recovery, where you might replay entries that were already applied before the crash.

### KV Projection

The KV projection maintains a hash table mapping keys to values, plus MVCC version tracking and TTL expiry management.

When a `kv_put` entry is applied, the projection parses the `CommandPayload` to extract the key, value, namespace hash, and optional expiry timestamp. It upserts into the hash table and pushes the previous value onto a version chain (bounded ring buffer, default 8 versions per key). If the entry includes a TTL, the key is registered in the TTL tracker for future expiry.

A `kv_delete` entry removes the key from the hash table and records a tombstone version.

KV reads are O(1) hash table lookups. No Raft round-trip needed. Historical version lookups walk the version chain. Prefix scans aren't supported by the hash table (a B-tree index is planned for a future phase).

Serialization for snapshots writes every key-value pair, including metadata (LSN, expiry, version chains), into a flat byte buffer with length-prefixed fields. Deserialization rebuilds the hash table from that buffer.

### Queue Projection

Queues in the old system stored each message as 8 separate KV entries — message body, metadata, index entries, consumer state. At ~180 bytes of overhead per KV entry, that's roughly 1.5 KB per message before you even count the payload. Not great for a queue that might hold millions of messages.

The new queue projection uses native data structures:

- **Ready heap** — a min-heap ordered by priority, holding `(sequence, ual_index)` pairs. Dequeue pops the top. Since the actual message payload stays in the UAL (or warm store), the heap node is just 16 bytes.
- **Lease tracker** — maps sequence numbers to lease expiry timestamps. When a consumer dequeues, the message is leased, not removed. If the consumer doesn't ACK before the lease expires, the message goes back to the ready heap.
- **DLQ state** — messages that exceed the retry limit are moved to a dead-letter queue. The projection tracks attempt counts per message.

On `queue_enqueue`, the projection stores a lightweight metadata record and pushes the sequence onto the ready heap (or delay wheel, if a delivery delay was specified). On `queue_ack`, it removes the message from the lease tracker. On `queue_requeue`, it pushes the message back onto the ready heap.

This brings per-message overhead down to around 64 bytes — a 23× reduction from the old KV-based design.

Serialization writes the queue name, last sequence, and each message's metadata. It does *not* copy message payloads — those live in the UAL. Deserialization rebuilds the heaps and trackers from the serialized metadata.

### Stream Projection

Streams are the odd one out: there is no stream projection in the traditional sense.

Stream records are UAL entries with `entry_type = stream_append`. Reading a stream is just reading a range of UAL entries and filtering by type. The UAL's index *is* the stream offset. Zero copy, no derived state, no overhead.

Consumer group offsets and metadata are stored as KV entries (keys prefixed with `cg:`), so the KV projection handles that state.

The stream "projection" object exists in code, but it's mostly a bookkeeping wrapper — tracking high water marks and consumer group cursors. It doesn't maintain a separate copy of the data.

Serialization captures the high water mark and consumer group state. That's it — there's no message data to snapshot because the messages are the UAL entries themselves.

### Time-Series Projection

Time-series data has different access patterns from everything else: writes are append-only (timestamped points), and reads are range scans that need to be fast over millions of points. Storing individual points as UAL entries would make range scans unbearably slow.

The TS projection maintains:

- **Write buffers** — per-series, per-field in-memory buffers that accumulate incoming points. When a buffer fills up (1024 points or 10 seconds, whichever comes first), it's flushed to a columnar block file.
- **Block index** — maps `(series_hash, field_hash, time_range)` → `(file_id, offset, size)`. This is how range queries find the right blocks without scanning every file.

On `ts_write`, the projection routes the point to the appropriate write buffer by series and field hash. When a buffer is flushed, the data is written column-wise (all timestamps together, then all values together) to a `.floc` file, and a block index entry is added. This columnar layout enables compression and vectorized aggregation.

Series metadata (measurement names, tag sets, field names) lives in the KV projection under `_ts:meta:*` keys. The TS query path typically resolves series metadata from KV, then scans the block index and write buffers to answer aggregation queries.

Serialization captures the block index and any unflushed write buffers. The `.floc` files on disk are not part of the snapshot — they're derived artifacts that can be rebuilt.

### Projection Router

The router is the single fan-out point between the UAL and the projections. When a Partition calls `router.apply(entry)`, the router checks:

1. Is `entry.index <= applied_index`? If yes, skip (idempotent replay guard).
2. What's the entry type? Route to the appropriate projection:
   - `kv_put`, `kv_delete`, `kv_batch`, `cg_*` → KV Projection
   - `queue_enqueue`, `queue_ack`, `queue_requeue` → Queue Projection
   - `ts_write`, `ts_write_batch` → TS Projection
   - `stream_append` → Nothing (data stays in UAL)
   - `raft_noop`, `raft_config` → Nothing (consensus layer)
3. Update `applied_index`.

The router doesn't implement any business logic. It's a switch statement with accounting.

---

## Snapshots

### The Problem with RAM-Only State

Projections live in RAM. Without snapshots, the only way to rebuild projection state after a crash is to replay the entire UAL from the beginning. For a partition with millions of entries, that could take minutes.

Worse, if the UAL has been compacted (old segments deleted because they're no longer needed for replication), and the snapshot was only in RAM, then the data those segments contained is simply gone. You can't replay entries you don't have, and you can't restore a snapshot that wasn't persisted.

Persistent snapshots solve both problems: fast recovery (load snapshot + replay recent delta) and safe compaction (old segments can be deleted once the snapshot covers their entries).

### Snapshot Format

Each `.fsnap` file is a self-contained snapshot of all projection state at a specific UAL index:

```
┌─────────────────────────────────┐
│ Header (64 bytes)               │
│   magic: "FLO_SNP\0"           │
│   version, partition_id         │
│   ual_index (snapshot point)    │
│   raft_term, timestamp          │
│   section_count, total_size     │
├─────────────────────────────────┤
│ Section: KV (type=0x01)         │
│   [section_header 12B]          │
│   [serialized KV state]         │
├─────────────────────────────────┤
│ Section: Queue (type=0x02)      │
│   [section_header 12B]          │
│   [serialized Queue state]      │
├─────────────────────────────────┤
│ Section: Stream (type=0x04)     │
│   [section_header 12B]          │
│   [serialized Stream state]     │
├─────────────────────────────────┤
│ Section: TS (type=0x03)         │
│   [section_header 12B]          │
│   [serialized TS state]         │
├─────────────────────────────────┤
│ Footer (16 bytes)               │
│   crc32c (of everything above)  │
│   magic: "FLO_SNE"             │
└─────────────────────────────────┘
```

The CRC covers the entire file from byte 0 through the end of the last section, so any corruption — partial writes, bit flips, truncated files — is caught on load.

### Lifecycle

Snapshots are taken periodically (every N applied entries, configurable). The process:

1. Record `snapshot_index = current applied_index`
2. Serialize all four projections (KV, Queue, Stream, TS) into their section formats
3. Build the `.fsnap` in memory using `SnapshotBuilder`
4. Write to `snap-{index}-{timestamp}.fsnap.tmp`
5. `fdatasync` the temp file
6. Atomic rename: `.fsnap.tmp` → `.fsnap`
7. Update `MANIFEST` (same atomic rename pattern)
8. Old segments with indices ≤ `snapshot_index` can now be safely compacted

The atomic rename in step 6 is the key safety property. If the process crashes at any point before step 6, the temp file is incomplete and the previous snapshot remains valid. If it crashes after step 6 but before step 7, the new snapshot file exists and can be discovered by scanning the directory.

### Crash Safety

| What happens | What you lose | Why it's OK |
|---|---|---|
| Crash during UAL append | The uncommitted entry | It wasn't acknowledged to the client |
| Crash during snapshot write | The in-progress snapshot | Previous snapshot is still valid |
| Crash after snapshot, before compaction | Nothing | Snapshot is valid; extra UAL entries are harmless |
| Crash after compaction | Nothing | Snapshot covers all compacted entries |
| Power loss under `async_flush` | Up to about a second of acknowledged entries | Segments flush once a second; use `sync` durability where that isn't acceptable |

---

## Recovery

When a node restarts, each partition recovers in this order:

```
1. Load snapshot
   ├─ Read MANIFEST → find latest .fsnap
   ├─ Validate CRC
   ├─ Deserialize KV, Queue, Stream, TS projections
   └─ Set replay_from = snapshot's ual_index

2. Open UAL
   ├─ Discover warm segment files on disk
   └─ Rebuild segment index

3. Replay UAL from snapshot_index + 1
   ├─ Feed each entry through ProjectionRouter.apply()
   └─ Same code path as normal operation

4. Ready for traffic
```

Step 3 is identical to the normal write path — the projections don't know or care whether they're processing a live entry or a replayed one. The idempotency guard in the router (`applied_index`) ensures replaying an already-applied entry is a no-op.

If no snapshot exists (first boot, or snapshot was deleted), recovery replays the entire UAL from the first available segment. Slow, but correct.

If the UAL has been compacted and there's no snapshot covering the compacted range... that data is gone. This is why snapshot persistence matters, and why compaction only happens after a successful snapshot.

---

## Directory Layout

```
{data_dir}/
├── flo.lock                    # Held while a server uses the directory
├── SYSTEM                      # Shard and partition counts, checked at every boot
├── 00000/                      # One directory per shard
│   ├── MANIFEST                # Names the latest snapshot
│   ├── HARDSTATE               # Raft term and vote
│   ├── segs/
│   │   └── 0000000001.flseg    # Sealed log segments, named by first index
│   └── snaps/
│       └── {index}-{ts}.fsnap  # Snapshots
└── 00001/
```

Each shard's files are its own: no two shards write the same file.

---

## What's Not Here Yet

This document describes what's implemented. A few things from the design are planned but not yet built:

- **B-tree disk spillover for KV** — currently the KV hash table is RAM-only. For datasets that exceed memory, a B-tree index with on-disk pages is planned.
- **Queue message disk spillover** — same idea. For now, queue metadata lives entirely in memory.
- **Chunked snapshot serialization** — large snapshots (>100 MB) should be serialized in chunks with Reactor yields between them, to avoid blocking Raft heartbeats. For typical partition sizes this isn't an issue, but it matters at scale.
- **Incremental snapshots** — instead of serializing all projection state every time, only serialize what changed since the last snapshot. Planned for when full snapshots become a bottleneck.
- **entry_type_bitmap in segment sparse index** — allows O(1) filtering of segments that don't contain the entry type you're looking for (e.g., skip segments with no `stream_append` entries during a stream read).
- **TS block compaction** — merging small `.floc` files into larger ones, and downsampling older data.

These are tracked in the migration plan (Phases 3–5) and will be added as the rewrite progresses.
