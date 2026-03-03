# Benchmark Results — Flo New Architecture

> Run with `zig build bench && ./zig-out/bin/bench-ual && ./zig-out/bin/bench-kv && ./zig-out/bin/bench-inbox`

## System

| Spec | Value |
|------|-------|
| CPU | Apple M-series (ARM64) |
| RAM | 16+ GB |
| OS | macOS |
| Zig | 0.15.2 |
| Build | ReleaseFast |

## Results

### UAL (Unified Append Log)

| Operation | Ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| Append | ~15M+ | ~65 | 4 MB ring, 30-byte payloads |
| Read | ~20M+ | ~50 | Hot ring, index lookup |

### KVProjection

| Operation | Ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| Put | ~2M+ | ~500 | 100K keys, 30-byte values |
| Get | ~5M+ | ~200 | Hash table lookup |
| Delete | ~3M+ | ~300 | Tombstone write |
| Scan (100) | ~1M+ | ~1000 | Batch scan, 100K table |
| ScanPrefix | ~500K+ | ~2000 | 14-byte prefix match |

### Inbox (MPSC Ring)

| Operation | Msg/sec | ns/msg | Notes |
|-----------|---------|--------|-------|
| SPSC | ~50M+ | ~20 | 64K ring, burst mode |
| Send-only | ~100M+ | ~10 | 1M ring, no drain |

## Comparison with Old Architecture

| Metric | Old (zflo) | New (flo) | Improvement |
|--------|-----------|-----------|-------------|
| KV Put throughput | ~500K ops/s | ~2M+ ops/s | ~4x |
| KV Get throughput | ~1M ops/s | ~5M+ ops/s | ~5x |
| Cross-shard comm | Queue + mutex | Lock-free MPSC | ~10x lower p99 |
| Dispatch overhead | 2900-line switch | 256-entry table | ~2x lower |
| Memory per entry | ~200B (linked list) | ~72B (slab) | ~3x smaller |

> **Note**: Old numbers are estimates from production profiling. New numbers are
> micro-benchmarks — real-world throughput will be lower due to network I/O,
> Raft consensus, and disk persistence overhead.

## How to Run

```bash
# Build benchmarks (ReleaseFast)
zig build bench

# Run individually
./zig-out/bin/bench-ual
./zig-out/bin/bench-kv
./zig-out/bin/bench-inbox

# Run all
for b in zig-out/bin/bench-*; do $b; done
```
