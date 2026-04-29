//! Benchmark: Inbox (MPSC ring) throughput
//!
//! Measures single-producer/single-consumer message passing rate.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const Inbox = src.node.inbox.Inbox;
const Message = src.node.inbox.Message;
const Tag = src.node.inbox.Tag;

const BENCH_ITERS = 1_000_000;

fn makeMsg(seq: u64) Message {
    return .{
        .tag = .forward_request,
        .src_shard = 0,
        .partition_id = 0,
        .payload_len = 0,
        .sequence = seq,
        .payload_ptr = null,
        ._padding = .{0} ** 8,
    };
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Inbox Benchmark ===\n\n", .{});

    // ── Send + Drain (SPSC) ──
    {
        var inbox = try Inbox.init(allocator, 65536);
        defer inbox.deinit();

        var batch: [256]Message = undefined;

        const start = stdx.time.nanoTimestamp();

        var sent: u64 = 0;
        var received: u64 = 0;
        while (sent < BENCH_ITERS) {
            // Send a burst
            const burst = @min(@as(u64, 1024), BENCH_ITERS - sent);
            var i: u64 = 0;
            while (i < burst) : (i += 1) {
                if (!inbox.send(makeMsg(sent + i))) {
                    break;
                }
            }
            sent += i;

            // Drain what's available
            while (true) {
                const n = inbox.drain(&batch);
                if (n == 0) break;
                received += n;
            }
        }

        // Drain remaining
        while (received < sent) {
            const n = inbox.drain(&batch);
            if (n == 0) break;
            received += n;
        }

        const elapsed_ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
        const ops = sent * 1_000_000_000 / elapsed_ns;
        const ns_per_op = elapsed_ns / sent;

        std.debug.print("  Inbox SPSC:   {d:>12} msg/sec  ({d} ns/msg, {d} messages)\n", .{
            ops, ns_per_op, sent,
        });
        std.debug.print("  Inbox Drain:  {d:>12} received ({d} sent)\n", .{
            received, sent,
        });
    }

    // ── Send-only throughput (fill a large ring) ──
    {
        var inbox = try Inbox.init(allocator, 1024 * 1024);
        defer inbox.deinit();

        const start = stdx.time.nanoTimestamp();
        var count: u64 = 0;
        while (count < BENCH_ITERS) : (count += 1) {
            if (!inbox.send(makeMsg(count))) break;
        }
        const elapsed_ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
        const ops = count * 1_000_000_000 / elapsed_ns;

        std.debug.print("  Inbox Send:   {d:>12} msg/sec  ({d} ns/msg, ring=1M)\n", .{
            ops, elapsed_ns / @max(count, 1),
        });
    }

    std.debug.print("\n", .{});
}
