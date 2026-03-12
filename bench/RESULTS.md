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
| Append | 12.9M | 77 | 4 MB ring, 30-byte payloads |
| Read | 46.1M | 21 | Hot ring, index lookup |

### KVProjection

| Operation | Ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| Put | 4.4M | 225 | 100K keys, 30-byte values |
| Get | 11.9M | 84 | Hash table lookup |
| Delete | 9.9M | 100 | Tombstone write |
| Scan (100) | 6.0M | 165 | Batch scan, 100K table |
| ScanPrefix | 540K | 1850 | 14-byte prefix match |

### Inbox (MPSC Ring)

| Operation | Msg/sec | ns/msg | Notes |
|-----------|---------|--------|-------|
| SPSC | 28.5M | 35 | 64K ring, burst mode |
| Send-only | 34.2M | 29 | 1M ring, no drain |

## Comparison with Old Architecture

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
# Build benchmarks (ReleaseFast)
zig build bench

# Run individually
./zig-out/bin/bench-ual
./zig-out/bin/bench-kv
./zig-out/bin/bench-inbox

# Run all
for b in zig-out/bin/bench-*; do $b; done
```
