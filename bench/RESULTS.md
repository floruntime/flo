# Benchmark Results — Flo New Architecture

> Run with `zig build bench && ./zig-out/bin/bench-ual && ./zig-out/bin/bench-kv && ./zig-out/bin/bench-inbox`

## System

| Spec | Value |
|------|-------|
| CPU | Apple M-series (ARM64) |
| RAM | 16+ GB |
| OS | macOS |
| Zig | 0.16.0 |
| Build | Unverified |

The tables under Results predate today's bench build, which is ReleaseSafe in every module. They ran on
Zig 0.16 and were labelled ReleaseFast, but how they were built can't be verified, so a run today won't
compare directly.

## Results

### UAL (Unified Append Log)

| Operation | Ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| Append | 11.2M | 89 | 4 MB ring, 30-byte payloads |
| Read | 33.0M | 30 | Hot ring, index lookup |

### KVProjection

| Operation | Ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| Put | 6.0M | 165 | 100K keys, 30-byte values |
| Get | 17.9M | 55 | Hash table lookup |
| Delete | 4.0M | 247 | Tombstone write |
| Scan (100) | 6.0M | 167 | Batch scan, 100K table |
| ScanPrefix | 544K | 1837 | 14-byte prefix match |

### Inbox (MPSC Ring)

| Operation | Msg/sec | ns/msg | Notes |
|-----------|---------|--------|-------|
| SPSC | 14.9M | 67 | 1M ring, burst mode |
| Send-only | 15.4M | 64 | 1M ring, no drain |

### Warm store eviction (`bench-warm`)

A non-KV apply once the partition's warm store is full, so every apply also evicts. ReleaseSafe in every
module, on the shared Mac, 2026-11-10; three runs each.

| Build | ns/apply | Notes |
|-------|---------:|-------|
| dev 5213836 | 178,569–181,265 | each eviction scans the whole warm map (41,943 entries of 100 B, 4 MiB budget) |
| lowest index from a heap | 287–310 | the same, evicting from a min-heap of indices |

A full 32 MiB warm store holds about 330,000 entries of that size, eight times the map this scans.

### Segment lookup below the ring (`bench-segment-walk`)

A walk of 10,000 one-entry segments, as a sync log seals one per flush. `lookup` is `sealedSource`, the
lookup a stream disk read makes; `read` also opens and reads each segment. ReleaseSafe, shared Mac under
load (33–54), 2026-11-12; three runs each.

| Build | lookup ns/index | read ns/index |
|---|---:|---:|
| dev 8672c21 (linear scan of the listing) | 2,482–2,549 | 18,792–19,222 |
| binary search | 67–75 | 14,956–16,441 |

The scan grows with the listing, which grows with every flush; the read's remainder is the file open
and whole-file read per segment.

### One record from a large segment (`bench-segment-read`)

Single records at random indices from two 16 MiB segments (50,000 entries of 300 B each), alternating so
each read opens its segment. Segments of 64 KiB or less are still read whole, so the small-segment walk
above is unchanged (15.0–15.7 µs a record before, 14.8–16.0 after, same runs). ReleaseSafe, shared Mac,
load 10–13, 2026-11-12; three runs each, interleaved.

| Build | p50 µs | p99 µs |
|---|---:|---:|
| whole-file reads | 4,734–4,771 | 5,036–5,109 |
| positioned reads | 18–21 | 26–31 |

## Comparison with Old Architecture

The New column is older than the tables above: its figures come from Zig 0.15.2, with their build also
unverified.

| Metric | Old (zflo) | New (flo) | Improvement |
|--------|-----------|-----------|-------------|
| KV Put throughput | ~500K ops/s | 4.4M ops/s | ~9x |
| KV Get throughput | ~1M ops/s | 11.9M ops/s | ~12x |
| UAL Append | N/A (no UAL) | 12.9M ops/s | New |
| Cross-shard comm | Queue + mutex | Lock-free MPSC (28.5M/s) | ~10x lower p99 |
| Dispatch overhead | 2900-line switch | 256-entry table | ~2x lower |
| Memory per entry | ~200B (linked list) | ~72B (slab) | ~3x smaller |

> **Note**: Old numbers are estimates from production profiling. New numbers are
> micro-benchmarks — real-world throughput will be lower due to network I/O,
> Raft consensus, and disk persistence overhead.

## How to Run

```bash
# Build benchmarks (ReleaseSafe, what a release ships; each prints its modes)
zig build bench

# Run individually
./zig-out/bin/bench-ual
./zig-out/bin/bench-kv
./zig-out/bin/bench-inbox

# Run all
for b in zig-out/bin/bench-*; do $b; done
```
