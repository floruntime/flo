//! Worker End-to-End Tests
//!
//! Tests the worker management lifecycle:
//!   register → list → info → drain → heartbeat
//!
//! Also tests the REST API endpoints:
//!   GET /api/v1/workers
//!   GET /api/v1/workers/:id
//!
//! NOTE: Task dispatch tests (await, complete, fail) live in action_test.zig
//! since they test the action→worker flow together.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Worker Registration
// =============================================================================

test "e2e/worker: register and list" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register two workers
    try ctx.exec(&.{ "worker", "register", "list-w1", "task_a" });
    try ctx.exec(&.{ "worker", "register", "list-w2", "task_b" });

    // List workers
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);

    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(output.len > 0);
    try testing.expect(std.mem.indexOf(u8, output, "list-w1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "list-w2") != null);
}

test "e2e/worker: register with multiple task types" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register a worker that handles multiple task types
    try ctx.exec(&.{ "worker", "register", "multi-worker", "task_x", "task_y", "task_z" });

    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, "multi-worker") != null);

    // Every type given is registered, not just the first.
    var info = try ctx.cli.run(&.{ "worker", "info", "multi-worker" });
    defer info.deinit();
    try testing.expectEqual(@as(u8, 0), info.exit_code);
    try testing.expect(info.stdoutContains("Processes (3):"));
    inline for (.{ "task_x", "task_y", "task_z" }) |t| try testing.expect(info.stdoutContains(t));
}

// =============================================================================
// Worker Info
// =============================================================================

test "e2e/worker: info shows the worker's row, processes and metadata" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "info-w1", "resize", "encode", "--labels", "{\"gpu\":true}" });

    var result = try ctx.cli.run(&.{ "worker", "info", "info-w1" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    // Times vary; every other line is exact.
    var lines = std.mem.splitScalar(u8, std.mem.trim(u8, result.stdout, &std.ascii.whitespace), '\n');
    for ([_][]const u8{
        "Worker ID:       info-w1",
        "Status:          active",
        "Type:            action",
        "Current Load:    0 / 10",
        "Tasks Completed: 0",
        "Tasks Failed:    0",
        "Registered:      ",
        "Last Heartbeat:  ",
        "",
        "Processes (2):",
        "  resize (action) — runs: 0, fails: 0, last_run: 0",
        "  encode (action) — runs: 0, fails: 0, last_run: 0",
        "Metadata:        {\"gpu\":true}",
    }) |want| {
        const line = lines.next() orelse return error.TooFewLines;
        if (std.mem.endsWith(u8, want, ":  ") or std.mem.endsWith(u8, want, ":      ")) {
            try testing.expect(std.mem.startsWith(u8, line, want));
            _ = try std.fmt.parseInt(i64, line[want.len..], 10);
        } else try testing.expectEqualStrings(want, line);
    }
    try testing.expectEqual(@as(?[]const u8, null), lines.next());
}

test "e2e/worker: info on non-existent worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "worker", "info", "ghost-worker" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 1), result.exit_code);
    try testing.expectEqualStrings("Error: worker not found [not_found/not_found]", std.mem.trim(u8, result.stderr, &std.ascii.whitespace));
}

// =============================================================================
// Worker Drain
// =============================================================================

test "e2e/worker: drain sets worker to draining" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const worker_id = "drain-w1";

    // Register a worker
    try ctx.exec(&.{ "worker", "register", worker_id, "process" });

    // Drain the worker
    var drain_result = try ctx.cli.run(&.{ "worker", "drain", worker_id });
    defer drain_result.deinit();

    try stdx.testing.assertSucceeded(drain_result);

    // Info should reflect draining status
    var info_result = try ctx.cli.run(&.{ "worker", "info", worker_id });
    defer info_result.deinit();

    try stdx.testing.assertSucceeded(info_result);
    const output = std.mem.trim(u8, info_result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, "drain") != null);
}

test "e2e/worker: drain non-existent worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "worker", "drain", "no-such-worker" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 1), result.exit_code);
}

// =============================================================================
// Worker List Edge Cases
// =============================================================================

test "e2e/worker: list with no workers" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Fresh server — no workers registered
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();
    try testing.expectEqualStrings("(no workers)", std.mem.trim(u8, result.stdout, &std.ascii.whitespace));

    var json = try ctx.cli.run(&.{ "worker", "list", "-o", "json" });
    defer json.deinit();
    try testing.expectEqualStrings("[]", std.mem.trim(u8, json.stdout, &std.ascii.whitespace));
}

test "e2e/worker: list with limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register several workers
    try ctx.exec(&.{ "worker", "register", "lim-w1", "task_a" });
    try ctx.exec(&.{ "worker", "register", "lim-w2", "task_b" });
    try ctx.exec(&.{ "worker", "register", "lim-w3", "task_c" });

    // The first two by id.
    var result = try ctx.cli.run(&.{ "worker", "list", "--limit", "2", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqualStrings("lim-w1\nlim-w2", std.mem.trim(u8, result.stdout, &std.ascii.whitespace));
}

test "e2e/worker: list on a node of several shards shows every shard's workers once, in id order" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // Registrations land on their workers' shards; enough ids to reach all four.
    var name: [8]u8 = undefined;
    for (0..12) |i| try ctx.exec(&.{ "worker", "register", try std.fmt.bufPrint(&name, "sh-w{d:0>2}", .{i}), "task_a" });

    var want: std.ArrayList(u8) = .empty;
    defer want.deinit(testing.allocator);
    for (0..12) |i| try want.print(testing.allocator, "sh-w{d:0>2}\n", .{i});

    var all = try ctx.cli.run(&.{ "worker", "list", "-o", "raw" });
    defer all.deinit();
    try testing.expectEqualStrings(want.items, all.stdout);

    var json = try ctx.cli.run(&.{ "worker", "list", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(usize, 12), json.stdoutCount("\"worker_id\":\"sh-w"));
    try testing.expectEqual(@as(usize, 12), json.stdoutCount("\"status\":\"active\""));
}

// =============================================================================
// REST API — Worker Endpoints
// =============================================================================

test "e2e/http/worker: GET /workers with none registered is one empty page" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqualStrings("{\"workers\":[],\"has_more\":false,\"cursor\":null}", resp.bodyTrimmed());
}

test "e2e/http/worker: GET /workers pages every shard's workers once, in id order, and only the namespace's" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    const ids = [_][]const u8{ "w-a", "w-b", "w-c", "w-d", "w-e" };
    for (ids) |id| try ctx.exec(&.{ "worker", "register", id, "process" });
    try ctx.exec(&.{ "worker", "register", "w-other", "process", "-n", "other" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var seen: std.ArrayList([]u8) = .empty;
    defer {
        for (seen.items) |id| testing.allocator.free(id);
        seen.deinit(testing.allocator);
    }
    var path_buf: [128]u8 = undefined;
    var path: []const u8 = "/api/v1/workers?limit=2";
    var cursor: ?[]u8 = null;
    defer if (cursor) |c| testing.allocator.free(c);
    var pages: usize = 0;
    while (true) : (pages += 1) {
        try testing.expect(pages < 10);
        var resp = try http.get(path);
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.bodyTrimmed(), .{});
        defer parsed.deinit();
        const root = parsed.value.object;
        const workers = root.get("workers").?.array.items;
        try testing.expect(workers.len <= 2);
        for (workers) |w| {
            try seen.append(testing.allocator, try testing.allocator.dupe(u8, w.object.get("worker_id").?.string));
            try testing.expectEqualStrings("default", w.object.get("namespace").?.string);
            try testing.expectEqualStrings("action", w.object.get("worker_type").?.string);
            try testing.expectEqualStrings("active", w.object.get("status").?.string);
        }
        try testing.expectEqual(root.get("has_more").?.bool, root.get("cursor").? == .string);
        if (root.get("cursor").? != .string) break;
        if (cursor) |c| testing.allocator.free(c);
        cursor = try testing.allocator.dupe(u8, root.get("cursor").?.string);
        path = try std.fmt.bufPrint(&path_buf, "/api/v1/workers?limit=2&cursor={s}", .{cursor.?});
    }
    try testing.expectEqual(ids.len, seen.items.len);
    for (ids, seen.items) |want, got| try testing.expectEqualStrings(want, got);
    try testing.expect(pages >= 2);
}

/// `s` percent-encoded as encodeURIComponent does it: all but A-Z a-z 0-9 - _ . ! ~ * ' ( ).
fn encodeComponent(buf: []u8, s: []const u8) []const u8 {
    var n: usize = 0;
    for (s) |c| {
        if (std.ascii.isAlphanumeric(c) or std.mem.indexOfScalar(u8, "-_.!~*'()", c) != null) {
            buf[n] = c;
            n += 1;
        } else {
            _ = std.fmt.bufPrint(buf[n..][0..3], "%{X:0>2}", .{c}) catch unreachable;
            n += 3;
        }
    }
    return buf[0..n];
}

test "e2e/http/worker: GET /workers pages through ids that need percent-encoding, and caps the limit at a page" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    // In id order. Each is a cursor in turn, sent encoded as the console sends it.
    const ids = [_][]const u8{ "w a", "w%ird", "w&x=1", "w+/y" };
    for (ids) |id| try ctx.exec(&.{ "worker", "register", id, "process" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var path_buf: [256]u8 = undefined;
    var enc_buf: [64]u8 = undefined;
    var path: []const u8 = "/api/v1/workers?limit=1";
    for (ids, 0..) |want, i| {
        var resp = try http.get(path);
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.bodyTrimmed(), .{});
        defer parsed.deinit();
        const workers = parsed.value.object.get("workers").?.array.items;
        try testing.expectEqual(@as(usize, 1), workers.len);
        try testing.expectEqualStrings(want, workers[0].object.get("worker_id").?.string);
        const more = i + 1 < ids.len;
        try testing.expectEqual(more, parsed.value.object.get("has_more").?.bool);
        if (!more) break;
        path = try std.fmt.bufPrint(&path_buf, "/api/v1/workers?limit=1&cursor={s}", .{encodeComponent(&enc_buf, want)});
    }

    // A limit past a page's worth is a page's worth, not a refusal.
    var capped = try http.get("/api/v1/workers?limit=5000");
    defer capped.deinit();
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, capped.bodyTrimmed(), .{});
    defer parsed.deinit();
    try testing.expectEqual(ids.len, parsed.value.object.get("workers").?.array.items.len);
}

test "e2e/http/worker: GET /workers/:id returns worker detail" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Register a worker via CLI
    try ctx.exec(&.{ "worker", "register", "http-detail-w1", "analyze" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers/http-detail-w1");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    try testing.expect(resp.bodyContains("http-detail-w1"));
    try testing.expect(resp.bodyContains("\"worker_type\""));
    try testing.expect(resp.bodyContains("\"current_load\""));
    try testing.expect(resp.bodyContains("\"processes\""));
}

test "e2e/http/worker: GET /workers/:id not found" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers/nonexistent-worker-xyz");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    // Should contain error message
    try testing.expect(resp.bodyContains("not found") or resp.bodyContains("error") or resp.bodyContains("Worker not found"));
}

// =============================================================================
// REST API — Workers in Action Detail
// =============================================================================

test "e2e/http/worker: action detail includes workers array" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Register an action and a worker
    try ctx.exec(&.{ "action", "register", "http-act-with-workers" });
    try ctx.exec(&.{ "worker", "register", "act-detail-w1", "http-act-with-workers" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/actions/http-act-with-workers");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    try testing.expect(resp.bodyContains("\"workers\""));
    try testing.expect(resp.bodyContains("act-detail-w1"));
}

// =============================================================================
// Worker Re-registration
// =============================================================================

test "e2e/worker: re-register updates worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const worker_id = "rereg-w1";

    // Register once
    try ctx.exec(&.{ "worker", "register", worker_id, "task_a" });

    // Register again (same ID, different task type — should update)
    try ctx.exec(&.{ "worker", "register", worker_id, "task_b" });

    // List should show the worker
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, worker_id) != null);
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/worker: different namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register workers in different namespaces
    try ctx.exec(&.{ "worker", "register", "ns-w1", "process", "--namespace", "ns-alpha" });
    try ctx.exec(&.{ "worker", "register", "ns-w2", "process", "--namespace", "ns-beta" });

    // Both should register without conflict
    var result1 = try ctx.cli.run(&.{ "worker", "list", "--namespace", "ns-alpha" });
    defer result1.deinit();
    try stdx.testing.assertSucceeded(result1);

    var result2 = try ctx.cli.run(&.{ "worker", "list", "--namespace", "ns-beta" });
    defer result2.deinit();
    try stdx.testing.assertSucceeded(result2);
}

// =============================================================================
// Worker Expiry
// =============================================================================

/// Poll `worker info` until `done` holds for its result, or `within_ms`
/// pass: a deadline, not a count, since each poll spawns the CLI and takes
/// longer on a loaded machine.
fn pollWorkerInfo(ctx: *stdx.testing.TestContext, id: []const u8, within_ms: u64, comptime done: fn (stdx.testing.CommandResult) bool) !bool {
    const deadline = stdx.time.monotonicMs() + within_ms;
    while (stdx.time.monotonicMs() < deadline) {
        var info = try ctx.cli.run(&.{ "worker", "info", id });
        defer info.deinit();
        if (done(info)) return true;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return false;
}

// Each test asserts one transition: removal is far off while unhealthy is
// checked, so a slow poll can't step past it.
test "e2e/worker: a worker that stops heartbeating is marked unhealthy and refused work" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{
        .extra_config = "[workers]\nheartbeat_timeout_ms = 1000\nremove_after_ms = 60000\n",
    } });
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "silent-w", "process" });

    const Unhealthy = struct {
        fn done(r: stdx.testing.CommandResult) bool {
            return r.exit_code == 0 and r.stdoutContains("Status:          unhealthy");
        }
    };
    try testing.expect(try pollWorkerInfo(ctx, "silent-w", 20_000, Unhealthy.done));

    var awaited = try ctx.cli.run(&.{ "worker", "await", "process", "-w", "silent-w", "-b", "0" });
    defer awaited.deinit();
    try testing.expectEqual(@as(u8, 3), awaited.exit_code);
    try testing.expectEqualStrings("Error: worker missed its heartbeats; send one, or register again [bad_request/worker_unhealthy]", std.mem.trim(u8, awaited.stderr, &std.ascii.whitespace));
}

test "e2e/worker: a worker silent past remove_after_ms is removed" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{
        .extra_config = "[workers]\nheartbeat_timeout_ms = 1000\nremove_after_ms = 2000\n",
    } });
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "silent-w", "process" });

    const Removed = struct {
        fn done(r: stdx.testing.CommandResult) bool {
            return r.exit_code == 1 and std.mem.eql(u8, std.mem.trim(u8, r.stderr, &std.ascii.whitespace), "Error: worker not found [not_found/not_found]");
        }
    };
    try testing.expect(try pollWorkerInfo(ctx, "silent-w", 20_000, Removed.done));
    var listed = try ctx.cli.run(&.{ "worker", "list" });
    defer listed.deinit();
    try testing.expectEqual(@as(u8, 0), listed.exit_code);
    try testing.expectEqualStrings("(no workers)", std.mem.trim(u8, listed.stdout, &std.ascii.whitespace));
}

// =============================================================================
// Registration limits
// =============================================================================

const proto = @import("src").protocol.proto;

/// Sends one worker_register for `id` with `value` and returns its refusal's
/// status and reason, with the message copied into `msg`.
fn registerRefusal(ctx: *stdx.testing.TestContext, id: []const u8, value: []const u8, msg: []u8) !struct { status: u16, reason: proto.Reason, message: []const u8 } {
    const a = testing.allocator;
    const req: proto.Request = .{ .header = proto.RequestHeader.init(.worker_register, 9), .namespace = "default", .key = id, .value = value, .options = "" };
    const buf = try a.alloc(u8, req.wireSize());
    defer a.free(buf);
    const bytes = try req.serialize(buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    var sent: usize = 0;
    while (sent < bytes.len) {
        const rc = std.c.write(fd, bytes[sent..].ptr, bytes.len - sent);
        if (rc <= 0) return error.ShortWrite;
        sent += @intCast(rc);
    }
    var out: [4096]u8 = undefined;
    var got: usize = 0;
    for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |resp| {
            const refusal = try resp.refused();
            @memcpy(msg[0..refusal.message.len], refusal.message);
            return .{ .status = resp.header.status, .reason = refusal.reason, .message = msg[0..refusal.message.len] };
        } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

test "e2e/worker: a registration past a limit is refused with that limit's reason" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const a = testing.allocator;
    const max_key = 4096; // a qualified key's longest: the longest process name

    var w: std.Io.Writer.Allocating = .init(a);
    defer w.deinit();
    const Case = struct { processes: usize, name_len: usize, machine_id_len: usize = 0, reason: proto.Reason, why: []const u8 };
    for ([_]Case{
        .{ .processes = 1025, .name_len = 0, .reason = .out_of_range, .why = "more than 1024 processes" },
        .{ .processes = 1, .name_len = max_key + 1, .reason = .field_too_long, .why = std.fmt.comptimePrint("process name longer than {d} bytes", .{max_key}) },
        // Each name fits; together they pass what worker.info can answer.
        .{ .processes = 47, .name_len = max_key, .reason = .field_too_long, .why = "processes too long to list together" },
        .{ .processes = 0, .name_len = 0, .machine_id_len = 256, .reason = .field_too_long, .why = "machine id longer than 255 bytes" },
    }) |c| {
        w.clearRetainingCapacity();
        const out = &w.writer;
        try out.writeAll(&.{ 0, 4, 0, 0, 0 });
        try out.writeInt(u16, @intCast(c.processes), .little);
        for (0..c.processes) |_| {
            try out.writeInt(u16, @intCast(c.name_len), .little);
            try out.splatByteAll('p', c.name_len);
            try out.writeByte(0);
        }
        try out.writeByte(0); // no metadata
        if (c.machine_id_len > 0) {
            try out.writeByte(1);
            try out.writeInt(u16, @intCast(c.machine_id_len), .little);
            try out.splatByteAll('m', c.machine_id_len);
        } else try out.writeByte(0);

        var msg: [512]u8 = undefined;
        const r = try registerRefusal(ctx, "limit-w", w.written(), &msg);
        try testing.expectEqual(@as(u16, @backingInt(proto.StatusCode.bad_request)), r.status);
        try testing.expectEqual(c.reason, r.reason);
        try testing.expectEqualStrings(c.why, r.message);
    }
    var info = try ctx.cli.run(&.{ "worker", "info", "limit-w" });
    defer info.deinit();
    try testing.expectEqual(@as(u8, 1), info.exit_code);
}
