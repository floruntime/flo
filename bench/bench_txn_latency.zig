//! Benchmark: what large KV transactions cost the small requests on their
//! shard, against a running server at FLO_BENCH_ENDPOINT (host:port).
//!
//! Small gets and puts run at a steady rate on connections of their own. A
//! request's latency runs from when it was due, if it is sent late because the
//! one before it was still waiting, so a stall isn't hidden by the load
//! generator waiting it out (coordinated omission). Beside them, one connection
//! commits transactions at a steady rate.
//!
//! Environment: FLO_BENCH_ENDPOINT, FLO_SMALL_RATE (requests/s, all clients),
//! FLO_TXN_RATE (0 = none), FLO_TXN_OPS, FLO_TXN_BYTES, FLO_SECONDS, FLO_WARMUP.
//! Prints its modes, then one line of `name=value`.
//!
//! Read the probe first. In a VM, a lightly loaded server lets the vCPUs sleep,
//! and waking them stalled everything in the VM by 20-27 ms, the probe included.
//! For a one-node figure there, run an idle three-node cluster beside the server,
//! taking no traffic: its heartbeats keep the vCPUs awake.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");
const kv_ops = src.protocol.kv_ops;
const client = src.cli_client;

const SMALL_CLIENTS = 8; // half gets, half puts
const KEYS = 1024;
const VALUE = "small-value-padding-padding-padding";

fn envInt(name: [*:0]const u8, default: u64) u64 {
    const v = std.c.getenv(name) orelse return default;
    return std.fmt.parseInt(u64, std.mem.span(v), 10) catch default;
}

/// Monotonic: a wall clock stepping back inside a VM would read as a negative
/// latency.
fn now() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn sleepUntil(at: u64) void {
    const t = now();
    if (at > t) stdx.time.sleep(at - t);
}

/// How late a thread that only sleeps wakes: what the host costs everything
/// here, the server included. A latency no worse than this says nothing about
/// the server.
const Probe = struct {
    record_from: u64,
    stop_at: u64,
    samples: std.ArrayListUnmanaged(u32) = .empty,

    fn run(self: *Probe) void {
        var due = self.record_from;
        while (due < self.stop_at) : (due += std.time.ns_per_ms) {
            sleepUntil(due);
            const late = now() -| due;
            self.samples.append(std.heap.c_allocator, @intCast(@min(late, std.math.maxInt(u32)))) catch {};
        }
    }
};

/// A request that took over 5 ms: when, from the run's start, and how long.
const Slow = struct { at_ms: u32, us: u32 };

const Small = struct {
    id: usize,
    is_get: bool,
    endpoint: []const u8,
    interval_ns: u64,
    record_from: u64,
    stop_at: u64,
    samples: std.ArrayListUnmanaged(u32) = .empty,
    slow: std.ArrayListUnmanaged(Slow) = .empty,
    errors: u64 = 0,

    fn run(self: *Small) void {
        const allocator = std.heap.c_allocator;
        var conn = client.Client.init(allocator, self.endpoint);
        defer conn.deinit();
        conn.connect() catch {
            self.errors += 1;
            return;
        };
        var kbuf: [32]u8 = undefined;
        // Spread the clients across one interval.
        var due = self.record_from -| (self.interval_ns * (SMALL_CLIENTS - self.id) / SMALL_CLIENTS);
        var i: usize = self.id * 131;
        while (due < self.stop_at) : (due += self.interval_ns) {
            const before = now();
            sleepUntil(due);
            const woke = now();
            // On time: from the send, leaving out how late the timer woke.
            // Late: from when it was due, which the server's stall cost it.
            const start = if (before > due) due else woke;
            const k = std.fmt.bufPrint(&kbuf, "s-{d:0>6}", .{i % KEYS}) catch unreachable;
            i += 1;
            var r = (if (self.is_get)
                client.kv.get(&conn, "", k, .{ .wait_ms = 0, .wait_for = .exists, .txn = null })
            else
                client.kv.set(&conn, "", k, .{ .value = VALUE, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = null })) catch {
                self.errors += 1;
                return;
            };
            const done = now();
            if (r.status != .ok) self.errors += 1;
            r.deinit();
            if (start >= self.record_from) {
                self.samples.append(allocator, @intCast(@min(done - start, std.math.maxInt(u32)))) catch {};
                if (done - start > 5 * std.time.ns_per_ms) self.slow.append(allocator, .{ .at_ms = @intCast((start - self.record_from) / std.time.ns_per_ms), .us = @intCast((done - start) / 1000) }) catch {};
            }
        }
    }
};

const Txns = struct {
    endpoint: []const u8,
    rate: u64,
    ops: usize,
    bytes: usize,
    record_from: u64,
    stop_at: u64,
    committed: u64 = 0,
    errors: u64 = 0,
    commit_samples: std.ArrayListUnmanaged(u32) = .empty,

    fn run(self: *Txns) void {
        if (self.rate == 0) return;
        self.loop() catch {
            self.errors += 1;
        };
    }

    fn loop(self: *Txns) !void {
        const allocator = std.heap.c_allocator;
        var conn = client.Client.init(allocator, self.endpoint);
        defer conn.deinit();
        try conn.connect();
        var kbuf: [32]u8 = undefined;
        // Equal shares of keys and values, namespace prefix included.
        const value = try allocator.alloc(u8, (self.bytes / self.ops) -| 18);
        defer allocator.free(value);
        @memset(value, 'x');
        const interval = std.time.ns_per_s / self.rate;
        var due = self.record_from;
        var n: usize = 0;
        while (due < self.stop_at) : (due += interval) {
            sleepUntil(due);
            var b = try client.kv.call(&conn, "kv.txn.begin", "", "{r}", .{});
            if (b.status != .ok) {
                b.deinit();
                self.errors += 1;
                continue;
            }
            var diag: src.protocol.schema.Diagnostic = .{};
            const txn = (try kv_ops.op("kv.txn.begin").response.decodeAll(b.data, &diag)).txn;
            b.deinit();
            for (0..self.ops) |i| {
                const k = std.fmt.bufPrint(&kbuf, "{{r}}t-{d:0>6}", .{(n * self.ops + i) % KEYS}) catch unreachable;
                var r = try client.kv.set(&conn, "", k, .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = txn });
                if (r.status != .ok) self.errors += 1;
                r.deinit();
            }
            const t0 = now();
            var c = try client.kv.call(&conn, "kv.txn.commit", "", "", .{ .txn = txn });
            const t1 = now();
            if (c.status == .ok) self.committed += 1 else self.errors += 1;
            c.deinit();
            self.commit_samples.append(allocator, @intCast(@min(t1 - t0, std.math.maxInt(u32)))) catch {};
            n += 1;
        }
    }
};

fn pct(sorted: []const u32, p: f64) f64 {
    if (sorted.len == 0) return 0;
    const at: usize = @intFromFloat(@as(f64, @floatFromInt(sorted.len - 1)) * p);
    return @as(f64, @floatFromInt(sorted[at])) / 1000;
}

fn report(name: []const u8, s: []u32) void {
    std.mem.sort(u32, s, {}, std.sort.asc(u32));
    std.debug.print(" {s}_n={d} {s}_p50={d:.1} {s}_p99={d:.1} {s}_p999={d:.1} {s}_max={d:.1}", .{ name, s.len, name, pct(s, 0.5), name, pct(s, 0.99), name, pct(s, 0.999), name, pct(s, 1.0) });
}

pub fn main() !void {
    mode.printForClient();
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    const rate = envInt("FLO_SMALL_RATE", 2000);
    const seconds = envInt("FLO_SECONDS", 15);
    const warmup = envInt("FLO_WARMUP", 3);

    // Every key a get asks for exists, in a store with something in it.
    {
        var conn = client.Client.init(allocator, endpoint);
        defer conn.deinit();
        try conn.connect();
        var kbuf: [32]u8 = undefined;
        for (0..KEYS) |i| {
            const k = try std.fmt.bufPrint(&kbuf, "s-{d:0>6}", .{i});
            var r = try client.kv.set(&conn, "", k, .{ .value = VALUE, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = null });
            if (r.status != .ok) return error.PrefillRefused;
            r.deinit();
        }
    }

    const record_from = now() + warmup * std.time.ns_per_s;
    const stop_at = record_from + seconds * std.time.ns_per_s;
    const interval = std.time.ns_per_s * SMALL_CLIENTS / rate;
    var smalls: [SMALL_CLIENTS]Small = undefined;
    var threads: [SMALL_CLIENTS]std.Thread = undefined;
    for (&smalls, &threads, 0..) |*s, *t, i| {
        s.* = .{ .id = i, .is_get = i % 2 == 0, .endpoint = endpoint, .interval_ns = interval, .record_from = record_from, .stop_at = stop_at };
        t.* = try std.Thread.spawn(.{}, Small.run, .{s});
    }
    var txns: Txns = .{ .endpoint = endpoint, .rate = envInt("FLO_TXN_RATE", 0), .ops = envInt("FLO_TXN_OPS", 32), .bytes = envInt("FLO_TXN_BYTES", 256 * 1024), .record_from = record_from, .stop_at = stop_at };
    const txn_thread = try std.Thread.spawn(.{}, Txns.run, .{&txns});
    var probe: Probe = .{ .record_from = record_from, .stop_at = stop_at };
    const probe_thread = try std.Thread.spawn(.{}, Probe.run, .{&probe});
    for (&threads) |*t| t.join();
    txn_thread.join();
    probe_thread.join();

    var gets: std.ArrayListUnmanaged(u32) = .empty;
    var puts: std.ArrayListUnmanaged(u32) = .empty;
    var errors: u64 = txns.errors;
    for (&smalls) |*s| {
        try (if (s.is_get) &gets else &puts).appendSlice(allocator, s.samples.items);
        errors += s.errors;
    }
    std.debug.print("errors={d} txns={d} txn_rate={d:.1}", .{ errors, txns.committed, @as(f64, @floatFromInt(txns.committed)) / @as(f64, @floatFromInt(seconds)) });
    report("get", gets.items);
    report("put", puts.items);
    report("commit", txns.commit_samples.items);
    report("probe", probe.samples.items);
    // Stalls: slow requests within 20 ms of each other are one.
    var slow: std.ArrayListUnmanaged(Slow) = .empty;
    for (&smalls) |*s| try slow.appendSlice(allocator, s.slow.items);
    std.mem.sort(Slow, slow.items, {}, struct {
        fn lt(_: void, a: Slow, b: Slow) bool {
            return a.at_ms < b.at_ms;
        }
    }.lt);
    std.debug.print(" stalls=", .{});
    var i: usize = 0;
    while (i < slow.items.len) {
        var worst: u32 = slow.items[i].us;
        var j = i + 1;
        while (j < slow.items.len and slow.items[j].at_ms -| slow.items[j - 1].at_ms <= 20) : (j += 1) worst = @max(worst, slow.items[j].us);
        std.debug.print("{d}ms:{d}us,", .{ slow.items[i].at_ms, worst });
        i = j;
    }
    std.debug.print("\n", .{});
}
