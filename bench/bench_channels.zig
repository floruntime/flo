//! Benchmark: what a channel message costs, between two groups in one
//! process, with the test machine as sender and receiver.
//!
//! - send: the apply of an entry that sends one message (the probe's
//!   apply, staging the send and writing it to the outbox in its commit);
//! - inbound: the receiver's apply of one message, in batches of up to 64;
//! - round: everything per message once sent, with nothing lost or
//!   delayed: the batch, the inbound entry's apply, both acks back, and
//!   the trim's apply on the sender.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const contract = src.contract;
const ce = src.protocol.channel_entries;
const entry_mod = src.storage.ual.entry;
const persistence = src.storage.persistence;
const channels = contract.channels;
const driver_mod = contract.channel_driver;
const probe = contract.channel_probe;

const Probed = channels.Channels(&.{probe.machine});
const machines = [_]contract.machine.Machine{ probe.machine, Probed.machine };

const MESSAGES = 100_000;

const Group = struct {
    id: u16,
    store: contract.store.Store,
    scratch: std.heap.ArenaAllocator,
    driver: driver_mod.Driver,
    index: u64 = 0,
    /// What its driver proposed, applied in order.
    log: std.ArrayListUnmanaged(struct { etype: entry_mod.EntryType, bytes: []u8 }) = .empty,
    inbound_messages: u64 = 0,
    inbound_ns: u64 = 0,
};

const Net = struct {
    allocator: std.mem.Allocator,
    groups: *[2]Group,
    frames: std.ArrayListUnmanaged(struct { from: u16, to: u16, bytes: []u8 }) = .empty,
};

const Io = struct {
    net: *Net,
    me: u16,

    pub fn sendFrame(io: Io, to: u16, bytes: []const u8) bool {
        const owned = io.net.allocator.dupe(u8, bytes) catch return false;
        io.net.frames.append(io.net.allocator, .{ .from = io.me, .to = to, .bytes = owned }) catch return false;
        return true;
    }

    pub fn propose(io: Io, etype: entry_mod.EntryType, payload: []const u8) bool {
        const g = &io.net.groups[io.me];
        const owned = io.net.allocator.dupe(u8, payload) catch return false;
        g.log.append(io.net.allocator, .{ .etype = etype, .bytes = owned }) catch return false;
        return true;
    }
};

fn apply(g: *Group, etype: entry_mod.EntryType, payload: []const u8) !void {
    g.index += 1;
    const e = entry_mod.buildEntry(etype, entry_mod.Flags.NONE, 1, g.index, g.index, payload);
    var out: persistence.Result = .{};
    out.reset(&e);
    try contract.machine.applyEntry(contract.machine.ownerOf(&machines, etype).?, &g.store, &g.scratch, &e, &out);
}

fn report(name: []const u8, n: u64, ns: u64) void {
    std.debug.print("  {s:<28} {d:>6} ns/msg  ({d} msgs)\n", .{ name, ns / n, n });
}

pub fn main() !void {
    mode.print();
    const allocator = std.heap.c_allocator;
    std.debug.print("\n=== Channel messages ===\n\n", .{});
    var groups: [2]Group = undefined;
    for (&groups, 0..) |*g, i| {
        g.* = .{
            .id = @intCast(i),
            .store = contract.store.Store.init(allocator),
            .scratch = std.heap.ArenaAllocator.init(allocator),
            .driver = try driver_mod.Driver.init(allocator, @intCast(i), 2),
        };
        g.store.place = .{ .group = @intCast(i), .count = 2 };
        g.driver.reset(1);
    }
    var net: Net = .{ .allocator = allocator, .groups = &groups };
    const a = &groups[0];
    const b = &groups[1];

    // Send: each probe goes to slot 1, group 1's.
    var buf: [128]u8 = undefined;
    var body: [16 + 48]u8 = @splat('x');
    var start = stdx.time.nanoTimestamp();
    for (0..MESSAGES) |i| {
        _ = std.fmt.bufPrint(body[0..16], "m-{d:0>14}", .{i}) catch unreachable;
        try apply(a, .channel_probe, try ce.Probe.encodeInto(&buf, .{ .slot = 1, .body = &body }));
    }
    report("send (apply + outbox)", MESSAGES, @intCast(stdx.time.nanoTimestamp() - start));

    // The round: tick, deliver, apply what was proposed, until every
    // message is applied and trimmed.
    start = stdx.time.nanoTimestamp();
    var now: u64 = 1;
    while (channels.metaOf(.{ .store = &a.store }, 1, .main).acked < MESSAGES) : (now += 1) {
        for (&groups) |*g| g.driver.tick(now, .{ .store = &g.store }, g.index, Io{ .net = &net, .me = g.id });
        while (net.frames.items.len > 0) {
            const f = net.frames.orderedRemove(0);
            defer allocator.free(f.bytes);
            const g = &groups[f.to];
            g.driver.onFrame(f.from, f.bytes, now, .{ .store = &g.store }, g.index, false, Io{ .net = &net, .me = g.id });
            for (&groups) |*h| while (h.log.items.len > 0) {
                const p = h.log.orderedRemove(0);
                defer allocator.free(p.bytes);
                if (p.etype == .channel_inbound) {
                    var diag: src.protocol.schema.Diagnostic = .{};
                    const n = (try ce.Inbound.decodeAll(p.bytes, &diag)).messages.count;
                    const t = stdx.time.nanoTimestamp();
                    try apply(h, p.etype, p.bytes);
                    h.inbound_ns += @intCast(stdx.time.nanoTimestamp() - t);
                    h.inbound_messages += n;
                } else try apply(h, p.etype, p.bytes);
            };
        }
        // The trims wait 100 ms apart: let time pass for them.
        now += driver_mod.TRIM_EVERY_MS;
    }
    report("round (deliver..trim)", MESSAGES, @intCast(stdx.time.nanoTimestamp() - start));
    report("inbound apply", b.inbound_messages, b.inbound_ns);
    std.debug.print("\n", .{});
}
