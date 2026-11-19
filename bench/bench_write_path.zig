//! Benchmark: the shard write path, request in → response queued.
//!
//! Drives a real Shard (Raft node, projections, handlers, dispatcher) with
//! kv_put, stream_append and queue_enqueue requests over an in-memory
//! connection, exactly as a client on the wire would, minus the socket.
//! bench_kv.zig measures the projections alone; this one includes the Raft
//! log and the apply loop.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const Shard = src.node.shard.Shard;
const Partition = src.storage.partition.Partition;
const proto = src.protocol.proto;

const WARMUP = 2_000;
const ITERS = 200_000;
/// Enqueues are never dequeued here, and the queue projection's cost grows
/// with the backlog, so the queue case runs shorter to measure the write
/// path rather than the backlog.
const QUEUE_ITERS = 20_000;

fn request(op: proto.OpCode, id: u64, namespace: []const u8, key: []const u8, value: []const u8) proto.Request {
    const header = proto.RequestHeader.init(op, id);
    return .{ .header = header, .namespace = namespace, .key = key, .value = value };
}

const Run = struct {
    name: []const u8,
    ops: u64,
    ns: u64,

    fn print(self: Run) void {
        const per_op = @as(f64, @floatFromInt(self.ns)) / @as(f64, @floatFromInt(self.ops));
        const ops_s = 1e9 / per_op;
        std.debug.print("  {s:<16} {d:>9.0} ops/s   {d:>8.0} ns/op   ({d} ops)\n", .{ self.name, ops_s, per_op, self.ops });
    }
};

/// The body a client sends for `op`: kv.set's request, a stream append's
/// one-record batch, a time-series point's value, an enqueue's payload.
fn bodyOf(buf: []u8, op: proto.OpCode, value: []const u8) ![]const u8 {
    return switch (op) {
        .kv_put => try src.protocol.kv_ops.op("kv.set").request.encodeInto(buf, .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = null }),
        // [count:u32][len:u32][payload][header_count:u16]
        .stream_append => blk: {
            std.mem.writeInt(u32, buf[0..4], 1, .little);
            std.mem.writeInt(u32, buf[4..8], @intCast(value.len), .little);
            @memcpy(buf[8..][0..value.len], value);
            std.mem.writeInt(u16, buf[8 + value.len ..][0..2], 0, .little);
            break :blk buf[0 .. 10 + value.len];
        },
        // An f64, little-endian.
        .ts_write => blk: {
            std.mem.writeInt(u64, buf[0..8], @bitCast(@as(f64, @floatFromInt(value.len))), .little);
            break :blk buf[0..8];
        },
        else => value,
    };
}

/// `iters` requests, each answered before the next: the shard applies what
/// committed as its loop would, so its answer is queued. Any answer but ok
/// fails the bench: one that refused every request would otherwise time
/// the refusal.
fn drive(shard: *Shard, conn: *src.node.connection.Connection, op: proto.OpCode, key_prefix: []const u8, iters: usize, start_id: u64) !u64 {
    var kbuf: [48]u8 = undefined;
    var vbuf: [128]u8 = undefined;
    var bbuf: [256]u8 = undefined;
    var answer: [256]u8 = undefined;
    const t0 = stdx.time.nanoTimestamp();
    for (0..iters) |i| {
        const id = start_id + i;
        // Keys cycle over 1024 names so the projections stay small and the
        // measurement is the write path, not hash-map growth.
        const key = try std.fmt.bufPrint(&kbuf, "{s}-{d:0>4}", .{ key_prefix, i % 1024 });
        const value = try std.fmt.bufPrint(&vbuf, "value-{d:0>8}-padding-padding-padding-padding-padding-padding-padding", .{id});
        shard.runRequest(conn, request(op, id, "", key, try bodyOf(&bbuf, op, value)), .client);
        _ = shard.applyCommitted();
        const pending = conn.write_buf.readable();
        if (pending == 0) return error.NotAnswered;
        const n = conn.write_buf.read(answer[0..@min(answer.len, pending)]);
        const r = try proto.Response.parse(answer[0..n]);
        if (r.header.status != @backingInt(proto.StatusCode.ok)) {
            std.debug.print("  {s}: refused, status {d}: {s}\n", .{ @tagName(op), r.header.status, r.data[0..@min(r.data.len, 160)] });
            return error.Refused;
        }
        // Drop the rest so the connection buffer never fills.
        const rest = conn.write_buf.readable();
        if (rest > 0) conn.write_buf.consume(rest);
    }
    return @intCast(stdx.time.nanoTimestamp() - t0);
}

pub fn main() !void {
    mode.print();
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const pipe_fds = try stdx.io.pipe();
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(allocator, 0, 1, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();

    const conn_pipe = try stdx.io.pipe();
    defer _ = std.c.close(conn_pipe[1]);
    const conn = try shard.addConnection(conn_pipe[0]);

    std.debug.print("\n=== Shard write path (after {d} warm-up ops) ===\n\n", .{WARMUP});

    const cases = [_]struct { name: []const u8, op: proto.OpCode, prefix: []const u8, iters: usize }{
        .{ .name = "kv_put", .op = .kv_put, .prefix = "bench-kv", .iters = ITERS },
        .{ .name = "stream_append", .op = .stream_append, .prefix = "bench-stream", .iters = ITERS },
        .{ .name = "queue_enqueue", .op = .queue_enqueue, .prefix = "bench-queue", .iters = QUEUE_ITERS },
        .{ .name = "ts_write", .op = .ts_write, .prefix = "bench-ts", .iters = QUEUE_ITERS },
    };

    var next_id: u64 = 1;
    for (cases) |c| {
        _ = try drive(&shard, conn, c.op, c.prefix, WARMUP, next_id);
        next_id += WARMUP;
        const ns = try drive(&shard, conn, c.op, c.prefix, c.iters, next_id);
        next_id += c.iters;
        (Run{ .name = c.name, .ops = c.iters, .ns = ns }).print();
    }

    std.debug.print("\n=== Applying stream appends ===\n\n", .{});
    try streamApply(&shard, 20_000);
    std.debug.print("\n=== A committed backlog, applied in passes ===\n\n", .{});
    try backlog(&shard, "kv_put", 10_000, 1);
    try backlog(&shard, "kv_batch of 32", 1_000, 32);
    try backlog(&shard, "kv_batch of 256", 100, 256);
    std.debug.print("\n", .{});
}

/// A pass that applied nothing stopped at a snapshot bar, whose window ends
/// over the shard's ticks, which this bench doesn't run: it measures apply,
/// not snapshots, so the window closes and apply goes on. Anything else
/// that stops apply fails the bench rather than spinning.
fn passBar(shard: *Shard) !void {
    if (shard.snapshot_window == null) return error.ApplyStopped;
    shard.snapshot_window = null;
}

/// `n` entries of `ops` puts each in the log with nothing applied, as a
/// cold boot or a follower far behind meets its log, then drained one pass
/// at a time. The worst pass is what the shard's other work waits on.
fn backlog(shard: *Shard, name: []const u8, n: usize, ops: u16) !void {
    const kv_entries = src.protocol.kv_entries;
    const raft = shard.raft_node;
    var payload: [64 * 1024]u8 = undefined;
    var kbuf: [256][16]u8 = undefined;
    var items: [256]kv_entries.BatchOp.Value = undefined;
    // Stamps rise from now, as a leader's do.
    const stamp0 = raft.now();
    for (0..n) |i| {
        const body = if (ops == 1) try kv_entries.Put.encodeInto(&payload, .{ .key = try std.fmt.bufPrint(&kbuf[0], "bl-{d:0>6}", .{i % 4096}), .value = "value-padding-padding", .ttl_ms = null, .condition = .always, .cas_version = null }) else b: {
            for (items[0..ops], 0..) |*it, k| it.* = .{ .kind = .put, .key = try std.fmt.bufPrint(&kbuf[k], "bl-{d:0>6}", .{(i * ops + k) % 4096}), .value = "value-padding-padding", .ttl_ms = null, .delta = null, .condition = .always, .cas_version = null };
            var w = src.protocol.schema.Writer{ .buf = &payload };
            // In the default namespace, id 1.
            try kv_entries.encodeBatch(&w, 1, items[0..ops]);
            break :b w.written();
        };
        // Into the log only, as replication hands a follower its log: a
        // proposal would stop at the cap on entries not yet applied. The
        // apply below is the one being measured.
        var e = src.storage.ual.entry.buildEntry(if (ops == 1) .kv_put else .kv_batch, 0, raft.current_term, raft.log.lastIndex() + 1, stamp0 + i, body);
        e.header.crc32c = e.computeCrc();
        _ = try raft.log.append(&e);
    }
    raft.commit_index = raft.log.lastIndex();
    var passes: u64 = 0;
    var worst: u64 = 0;
    const t0 = stdx.time.nanoTimestamp();
    while (raft.last_applied < raft.commit_index) : (passes += 1) {
        const before = raft.last_applied;
        const p0 = stdx.time.nanoTimestamp();
        _ = shard.applyCommitted();
        worst = @max(worst, @as(u64, @intCast(stdx.time.nanoTimestamp() - p0)));
        if (raft.last_applied == before) try passBar(shard);
    }
    const total: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
    std.debug.print("  {s:<16} {d:>6} entries  {d:>4} passes   worst pass {d:>6} µs   total {d:>6} µs\n", .{ name, n, passes, worst / 1000, total / 1000 });
}

/// `n` stream appends over 1024 streams, proposed a chunk at a time with
/// nothing applied, each chunk then applied: what apply costs an append.
/// Chunks stay under the shard's cap on proposals not yet applied.
fn streamApply(shard: *Shard, n: usize) !void {
    const entry_mod = src.storage.ual.entry;
    const raft = shard.raft_node;
    // Partition 0, then one record: [count:u32][len:u32][payload][header_count:u16].
    const record = "value-padding-padding-padding";
    var buf: [64]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 0, .little);
    std.mem.writeInt(u32, buf[4..8], 1, .little);
    std.mem.writeInt(u32, buf[8..12], record.len, .little);
    @memcpy(buf[12..][0..record.len], record);
    std.mem.writeInt(u16, buf[12 + record.len ..][0..2], 0, .little);
    const value = buf[0 .. 14 + record.len];
    const CHUNK = 128;
    var kbuf: [32]u8 = undefined;
    var payload: [512]u8 = undefined;
    var applying: u64 = 0;
    var i: usize = 0;
    while (i < n) {
        const end = @min(n, i + CHUNK);
        while (i < end) : (i += 1) {
            const key = try std.fmt.bufPrint(&kbuf, "bs-{d:0>4}", .{i % 1024});
            const cmd: entry_mod.CommandPayload = .{ .namespace_id = .default, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
            const len = cmd.serialize(&payload) orelse return error.TooLarge;
            shard.applying = true;
            _ = try raft.propose(.stream_append, 0, payload[0..len]);
            shard.applying = false;
        }
        raft.commit_index = raft.log.lastIndex();
        const t0 = stdx.time.nanoTimestamp();
        while (raft.last_applied < raft.commit_index) _ = shard.applyCommitted();
        applying += @intCast(stdx.time.nanoTimestamp() - t0);
    }
    std.debug.print("  stream_append    {d:>6} entries applied   {d:>6} ns/entry\n", .{ n, applying / n });
}
