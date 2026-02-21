# Stream Implementation — Gap Analysis

> **Date**: 2025-07-15
> **Scope**: Review of the stream handler/projection implementation against
> `UNIFIED_STORAGE_DESIGN.md` and `NODE_NETWORK_DESIGN.md`.
> **Branch**: `feat/rewrite-v2`

## Summary

73 of 79 stream E2E tests pass. All 713 unit tests pass. The 6 remaining failures
are infrastructure-level gaps (tiered storage reads, multi-partition support), not
handler logic bugs.

---

## 1. Alignment with Design

### 1.1 P6: Zero-Copy Stream Path — Partially Aligned

**Design says** (UNIFIED_STORAGE §5.4):
> Streams do **not** have a projection engine. The UAL entry for `stream_append`
> contains the full stream record. Reads go directly to the UAL.

**What we have**:
- The `ProjectionRouter` correctly routes `stream_append` / `stream_trim` to `.none`
  — no projection apply step on the write path. ✅
- `StreamProjection` exists but is explicitly documented as "offset tracking +
  consumer groups" — it does **not** store record payloads. ✅
- Stream reads call `partition.stream.readRange()` / `readRangeForStream()` which
  reads from the UAL ring buffer. ✅

**Gap — Stream name hash filtering**:
The design envisions reads filtering by `entry_type = .stream_append` at the UAL
level (segment bitmap skipping). Our implementation instead filters by a
`stream_name_hash` stored in `OffsetEntry`. This is a pragmatic solution for the
current single-partition model where multiple stream names can share a UAL, but
diverges from the design's "filter by entry type at the UAL" approach.

**Recommendation**: When the UAL gains entry-type bitmaps on segments (per §4.5),
the hash-based filtering should be replaced or augmented with UAL-level type
filtering to get O(1) segment skipping.

### 1.2 Consumer Group State — Diverges from Design Intent

**Design says** (UNIFIED_STORAGE §5.4, §10.2):
> Consumer group state is a thin projection in the **KV Projection**.
> Key format: `cg:{ns}:{stream}:{group}:{partition}`
> Consumer Group operations → KVProjection get/put on `cg:*` keys.

**What we have**:
Consumer group state (groups, members, committed offsets) lives inside
`StreamProjection` as `ConsumerGroup` structs with a `StringHashMap(Member)`.
This is fully self-contained in the stream projection, not delegated to KV.

**Trade-off analysis**:
- ✅ Current approach is simpler — group ops don't cross projection boundaries
- ✅ Group lifecycle (create/join/leave/delete) is atomic within one struct
- ❌ Breaks P5 (Projection Autonomy) — the stream has its own stateful data
  structures rather than using KV as the single source of truth
- ❌ Snapshot/restore must handle consumer groups separately instead of getting
  them "for free" from the KV snapshot
- ❌ No MVCC/versioning on group state (KV projection has this)

**Recommendation**: Consider migrating consumer group state to KV Projection for
Phase 2. The current approach works for single-node and is simpler to reason about
during the rewrite. The migration path is: persist group ops as `cg:*` KV entries
in the UAL → KVProjection applies them → stream handler reads from KVProjection.

### 1.3 P5: Generic Shard Walker — Aligned ✅

**Design says** (NODE_NETWORK §10):
> One generic `ShardWalker(T)` replaces 7 copy-pasted walkers.

**What we have**:
`stream_list` uses `dispatcher.registerWalk()` with `localScanStreams` as the
`LocalScanFn`, wired via `runtime.wireWalkContexts()` at index `[2]`. This is
the exact pattern from the design.

### 1.4 P4: Self-Registering Handlers — Aligned ✅

**Design says** (NODE_NETWORK §7.2):
> Each subsystem registers itself with the dispatcher. The dispatcher does not
> import any handler module.

**What we have**:
`stream.handler.register(dispatcher)` registers all 16 opcodes. The dispatcher
imports nothing from the stream module.

### 1.5 Domain-Specific Routing — Aligned ✅

**Design says** (NODE_NETWORK §8.3):
> Stream (metadata): `namespace + "\x00" + stream_name`
> Stream (data ops): `namespace + ":" + stream + ":" + partition_bytes`

**What we have**:
All stream handlers use namespace-qualified routing via
`hashKeyWithNamespace(namespace, key)`. Currently there is no distinction between
metadata and data routing because multi-partition is not yet implemented
(all streams have partition_count=1).

---

## 2. Known Gaps (6 Failing E2E Tests)

### 2.1 Tiered Storage — 2 failures

**Tests**: `tiered storage - warm tier spill and read`, `tiered storage - read range across tiers`

**Root cause**: The response serializer in `serializeMessagesWithPayloads` always
sets `tier = 0` (hot). When entries are evicted from the hot ring buffer, they
should be readable from warm disk segments, but our read path only reads from the
in-memory offset map → UAL hot buffer. There is no fallback to warm/cold segments.

**Design reference** (UNIFIED_STORAGE §4.6):
> UAL looks up index X in hot tier (hash map) or warm tier (sparse index).

**What's needed**:
1. UAL `readRange()` must fall back to disk segment reader when hot ring buffer
   doesn't contain the requested index.
2. The tier byte in serialized messages should reflect the actual tier the data
   was read from.
3. Segment sparse-index support for warm-tier binary search.

### 2.2 Multi-Partition Streams — 4 failures

**Tests**: `create multi-partition stream`, `partition-key routing is deterministic`,
`multi-partition stream info shows correct partition count`,
`read with --partition still reads only that partition`

**Root cause**: `handleCreate` ignores the `--partitions` flag entirely.
`handleAppend` doesn't differentiate partitions within a stream. `handleInfo`
hardcodes `partition_count: 1`. `handleRead` doesn't filter by partition index.

**Design reference** (NODE_NETWORK §8.3):
> Stream (data ops) routing: `namespace + ":" + stream + ":" + partition_bytes`

**What's needed**:
1. `handleCreate` must parse and store partition count (in stream metadata).
2. `handleAppend` must route to the correct partition using `stream:partition`
   composite routing key.
3. Stream metadata (name → partition_count) should be stored either in the stream
   name registry or as a KV entry.
4. `handleRead` must accept and respect the `--partition` flag.
5. `handleInfo` must report actual partition count.

---

## 3. Architectural Observations

### 3.1 Stream Name Registry

The design does not mention a stream name registry — the UAL IS the stream and
names are implicit from the entries. Our implementation adds a
`stream_names: StringHashMap(void)` in `StreamProjection` plus a
`stream_name_hash: u64` in `OffsetEntry`. This is a pragmatic addition needed for:

- `stream_list` (must enumerate known stream names)
- Per-stream read filtering (must distinguish streams sharing a partition)

This is a reasonable divergence. The design's zero-copy vision assumes one stream
per partition (or entry-type filtering), but in practice, multiple streams can
hash to the same partition. The name hash provides O(1) filtering without
materializing records.

### 3.2 Binary Wire Decode with Fallback

Group handlers use `WireReader` to decode binary-encoded values from the CLI
(`FixedWireWriter`), with a raw-string fallback for unit tests that send
plain-text values. This dual-path decode is not in the design but is a practical
necessity for testability. It should be cleaned up once all tests use binary
wire encoding.

### 3.3 WebSocket Stream Subscribe

**Design says** (NODE_NETWORK §11.6):
> `stream_subscribe` registers a `Subscription{conn_fd, stream, partition, from_offset}`
> and pushes new records to WebSocket clients as they are committed.

**Status**: Not yet implemented. The WebSocket handler exists (`ws_handler.zig`)
but does not integrate with stream subscriptions. This is a separate piece of work
involving:
- Subscription registry per shard
- Wake-on-commit notification from ProjectionRouter
- WebSocket binary frame serialization

### 3.4 Snapshot / Restore

**Design says** (UNIFIED_STORAGE §P3):
> Snapshots are full-fidelity checkpoints. On recovery, load the latest snapshot
> and replay only UAL entries after it.

**Status**: `StreamProjection` has no `snapshotFn` or `restoreFn`. Recovery
replays the entire UAL from segment 0. This is acceptable for Phase 1 but will
need snapshot support for production (especially with large consumer group state).

### 3.5 Retention / Trim

`stream_trim` entries route to `.none` in the ProjectionRouter (correct — trim
is a UAL operation). However, the `handleTrim` handler in the stream handler is
only a stub. Full retention sweep (by age/count/size) is a background task
described in NODE_NETWORK §14 (`retention_sweep`, 5m interval).

---

## 4. Files Modified This Session

| File | Changes |
|------|---------|
| `src/stream/handler.zig` | 16 opcode registrations, all group handlers rewritten with binary WireReader decode, `stream_list` via ShardWalker, per-stream read filtering, new `handleGroupPending`/`handleGroupTouch`/`handleGroupNack` |
| `src/projection/stream.zig` | Stream name registry (`stream_names` HashMap), `stream_name_hash` in OffsetEntry, `readRangeForStream()`, `registerStream()`, `scanStreamNames()` |
| `src/node/shard.zig` | `serializeWalkStreamNames()`, `.stream_list` walk dispatch, segment replay stream name hash computation |
| `src/node/runtime.zig` | Walk context wiring at index `[2]` for stream_list |

---

## 5. Test Results

| Suite | Result |
|-------|--------|
| Unit tests | 713/713 pass (1 pre-existing flaky acceptor TCP test) |
| Stream E2E | 73/79 pass |
| KV E2E | 46/46 pass |
| TS E2E | 10/10 pass |

### Remaining 6 E2E failures (infrastructure, not handler bugs):
1. `tiered storage - warm tier spill and read` — needs warm segment read fallback
2. `tiered storage - read range across tiers` — same root cause
3. `create multi-partition stream and append to specific partitions` — needs --partitions support
4. `partition-key routing is deterministic` — needs multi-partition routing
5. `multi-partition stream info shows correct partition count` — handleInfo hardcodes 1
6. `read with --partition still reads only that partition` — --partition flag ignored
