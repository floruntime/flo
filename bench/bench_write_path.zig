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
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.op_code = @intFromEnum(op);
    header.request_id = id;
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

fn drive(shard: *Shard, conn: *src.node.connection.Connection, op: proto.OpCode, key_prefix: []const u8, iters: usize, start_id: u64) !u64 {
    var kbuf: [48]u8 = undefined;
    var vbuf: [128]u8 = undefined;
    const t0 = stdx.time.nanoTimestamp();
    for (0..iters) |i| {
        const id = start_id + i;
        // Keys cycle over 1024 names so the projections stay small and the
        // measurement is the write path, not hash-map growth.
        const key = try std.fmt.bufPrint(&kbuf, "{s}-{d:0>4}", .{ key_prefix, i % 1024 });
        const value = try std.fmt.bufPrint(&vbuf, "value-{d:0>8}-padding-padding-padding-padding-padding-padding-padding", .{id});
        shard.dispatchRequest(conn, request(op, id, "", key, value));
        // Drop the queued response so the connection buffer never fills.
        const pending = conn.write_buf.readable();
        if (pending > 0) conn.write_buf.consume(pending);
    }
    return @intCast(stdx.time.nanoTimestamp() - t0);
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const pipe_fds = try stdx.io.pipe();
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
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
    };

    var next_id: u64 = 1;
    for (cases) |c| {
        _ = try drive(&shard, conn, c.op, c.prefix, WARMUP, next_id);
        next_id += WARMUP;
        const ns = try drive(&shard, conn, c.op, c.prefix, c.iters, next_id);
        next_id += c.iters;
        (Run{ .name = c.name, .ops = c.iters, .ns = ns }).print();
    }
    std.debug.print("\n", .{});
}
