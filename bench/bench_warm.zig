//! Benchmark: a non-KV apply's cost once the partition's warm store is full,
//! where every apply also evicts. Run against dev and a change to the warm
//! store's eviction.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const Partition = src.storage.partition.Partition;
const entry_mod = src.storage.ual.entry;

/// About 40,000 entries of 100 bytes held, as a full 32 MiB store holds
/// about 330,000 of that size.
const WARM_BUDGET = 4 * 1024 * 1024;
const PAYLOAD = 100;
const FILL = 60_000;
const MEASURED = 20_000;

pub fn main() !void {
    mode.print();
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var part = try Partition.init(allocator, 0, 16 * 1024 * 1024, 0);
    defer part.deinit();
    part.wireProjections();
    part.setWarmBudget(WARM_BUDGET);

    const payload: [PAYLOAD]u8 = @splat('w');
    var index: u64 = 1;
    for (0..FILL) |_| {
        var e = entry_mod.buildEntry(.stream_append, 0, 1, index, @intCast(index * 1000), &payload);
        e.header.crc32c = e.computeCrc();
        _ = part.apply(&e) catch {};
        index += 1;
    }
    const held = part.warmCount();
    const t0 = stdx.time.monotonicNs();
    for (0..MEASURED) |_| {
        var e = entry_mod.buildEntry(.stream_append, 0, 1, index, @intCast(index * 1000), &payload);
        e.header.crc32c = e.computeCrc();
        _ = part.apply(&e) catch {};
        index += 1;
    }
    const ns = stdx.time.monotonicNs() - t0;
    std.debug.print("warm store full ({d} entries of {d} B, {d} MiB budget): {d} applies, {d} ns/apply\n", .{ held, PAYLOAD, WARM_BUDGET / (1024 * 1024), MEASURED, ns / MEASURED });
}
