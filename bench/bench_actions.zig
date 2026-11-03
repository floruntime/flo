//! Benchmark: the actions machine's apply, entry by entry.
//!
//! A run's life on a store that already holds many: invoke, deliver,
//! settle and a one-lease expire, then an expire of a full 256 leases, the
//! costliest apply there is: it declares three keys a lease, and the key
//! set's cover check scans them.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const contract = src.contract;
const entry_mod = src.storage.ual.entry;
const persistence = src.storage.persistence;
const ae = src.protocol.action_entries;
const machine = src.actions.machine;

/// Runs on the store when timing starts: a shard with a backlog.
const RUNS = 100_000;
const LEASE_MS = 30_000;

const Bench = struct {
    store: contract.store.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,
    buf: []u8,

    fn apply(self: *Bench, comptime et: entry_mod.EntryType, comptime P: type, v: P.Value, stamp: u64) !void {
        self.index += 1;
        const payload = try P.encodeInto(self.buf, v);
        const e = entry_mod.buildEntry(et, entry_mod.Flags.NONE, 1, self.index, stamp, payload);
        var out: persistence.Result = .{};
        out.reset(&e);
        try contract.machine.applyEntry(&machine.machine, &self.store, &self.scratch, &e, &out);
        if (out.refused()) |r| {
            std.debug.print("{t} refused: {s}\n", .{ et, r.refusal.message });
            return error.Refused;
        }
    }
};

fn runId(buf: []u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "run-{d:0>12}", .{i}) catch unreachable;
}

fn report(name: []const u8, n: u64, start: i128) void {
    const ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
    std.debug.print("  {s:<32} {d:>8} ns/op  ({d} ops)\n", .{ name, ns / n, n });
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;
    var b: Bench = .{ .store = contract.store.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator), .buf = try allocator.alloc(u8, ae.Expire.max_size) };
    defer b.store.deinit();
    defer b.scratch.deinit();
    std.debug.print("\n=== Actions apply ({d} runs on the store) ===\n\n", .{RUNS});

    try b.apply(.action_register, ae.Register, .{ .ns = "default", .name = "act", .owner = "", .lease_ms = LEASE_MS, .max_attempts = 3 }, 1);
    var rb: [32]u8 = undefined;
    // Stamps a second apart, so every run is due by the next phase.
    const s = std.time.ns_per_s;
    const due0: u64 = 10 * s;
    var start = stdx.time.nanoTimestamp();
    for (0..RUNS) |i| try b.apply(.action_invoke, ae.Invoke, .{ .ns = "default", .action = "act", .run = runId(&rb, i), .input = "{\"order\":42}", .labels = null, .source = .direct, .caller_run = null, .caller_workflow = null, .caller_shard = null }, due0 + i);
    report("invoke", RUNS, start);

    const deliver_at = due0 + RUNS + s;
    const first_deliver = b.index + 1;
    start = stdx.time.nanoTimestamp();
    for (0..RUNS) |i| try b.apply(.action_deliver, ae.Deliver, .{ .ns = "default", .action = "act", .run = runId(&rb, i), .due_ns = due0 + i, .holder = "worker-1", .request_key = null, .lease_ms = LEASE_MS }, deliver_at + i);
    report("deliver", RUNS, start);

    // Half settled; the rest left to expire.
    const settled = RUNS / 2;
    start = stdx.time.nanoTimestamp();
    for (0..settled) |i| try b.apply(.action_settle, ae.Settle, .{ .ns = "default", .action = "act", .run = runId(&rb, i), .holder = "worker-1", .request_key = null, .deliveries = 1, .token = .{ .term = 1, .index = first_deliver + i }, .expiry_ns = deliver_at + i + LEASE_MS * std.time.ns_per_ms, .outcome = "", .result = "{\"ok\":true}" }, deliver_at + RUNS + i);
    report("settle", settled, start);

    const expire_at = deliver_at + RUNS + LEASE_MS * std.time.ns_per_ms + s;
    var items: std.ArrayListUnmanaged(u8) = .empty;
    defer items.deinit(allocator);
    var ib: [ae.ExpireItem.max_size]u8 = undefined;
    const singles = (RUNS - settled) / 2;
    start = stdx.time.nanoTimestamp();
    for (settled..settled + singles) |i| {
        const item = try ae.ExpireItem.encodeInto(&ib, .{ .ns = "default", .action = "act", .run = runId(&rb, i), .expiry_ns = deliver_at + i + LEASE_MS * std.time.ns_per_ms, .holder = "worker-1", .request_key = null });
        try b.apply(.action_expire, ae.Expire, .{ .items = .{ .bytes = item, .count = 1 } }, expire_at + i);
    }
    report("expire, 1 lease", singles, start);

    // The rest, many a time: an apply's cost per lease grows with how many
    // it ends, as the key set's cover check scans every key it declares.
    var i: usize = settled + singles;
    for ([_]u32{ 16, 64, ae.MAX_EXPIRE_ITEMS }) |per| {
        const share = (RUNS - settled - singles) / 3;
        const end = i + share;
        var entries: u64 = 0;
        start = stdx.time.nanoTimestamp();
        while (i + per <= end) : (i += per) {
            items.clearRetainingCapacity();
            for (i..i + per) |j| try items.appendSlice(allocator, try ae.ExpireItem.encodeInto(&ib, .{ .ns = "default", .action = "act", .run = runId(&rb, j), .expiry_ns = deliver_at + j + LEASE_MS * std.time.ns_per_ms, .holder = "worker-1", .request_key = null }));
            try b.apply(.action_expire, ae.Expire, .{ .items = .{ .bytes = items.items, .count = per } }, expire_at + RUNS + i);
            entries += 1;
        }
        var name: [32]u8 = undefined;
        report(try std.fmt.bufPrint(&name, "expire, {d} leases", .{per}), entries, start);
        i = end;
    }
    std.debug.print("\n", .{});
}
