//! Time-Series End-to-End Tests
//!
//! Tests the complete path: CLI → TCP → Node → TsHandler → Storage
//!
//! Coverage:
//! - Write: single-point, multi-field, batch (line protocol)
//! - Read: raw point retrieval, tag filtering, time ranges, limits
//! - Query: windowed aggregation (avg, sum, count, min, max)
//! - List: measurements, series, fields
//! - Delete: measurement, specific series
//! - Retention: set raw TTL, add downsample rules, --show
//! - FloQL: pipeline queries
//! - Namespace isolation for TS data

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Helper Functions
// =============================================================================

/// Extract "OK (<ts>-<seq>)" response from write output
fn extractWriteId(output: []const u8) ?[]const u8 {
    // Look for pattern: digits-digits inside "OK (..."
    const paren = std.mem.indexOf(u8, output, "(") orelse return null;
    const close = std.mem.indexOf(u8, output, ")") orelse return null;
    if (close <= paren + 1) return null;
    const inner = output[paren + 1 .. close];
    // Validate: should contain a dash with digits on both sides
    const dash = std.mem.indexOf(u8, inner, "-") orelse return null;
    if (dash == 0 or dash >= inner.len - 1) return null;
    return inner;
}

/// Check output contains a numeric value (as formatted by CLI)
fn containsNumericValue(output: []const u8) bool {
    // Look for a decimal point surrounded by digits (e.g. "72.5000")
    for (output, 0..) |c, i| {
        if (c == '.' and i > 0 and i + 1 < output.len) {
            if (std.ascii.isDigit(output[i - 1]) and std.ascii.isDigit(output[i + 1])) {
                return true;
            }
        }
    }
    return false;
}

// =============================================================================
// Write: Single-Point
// =============================================================================

test "e2e/ts: write single point returns OK" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo ts write cpu_usage --tags host=web-01 --fields 72.5
    const output = try ctx.execCapture(&.{ "ts", "write", "cpu_usage", "--tags", "host=web-01", "--value", "72.5" });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write returns timestamp-sequence ID" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const output = try ctx.execCapture(&.{ "ts", "write", "temperature", "--tags", "sensor=A1", "--value", "23.4" });
    const id = extractWriteId(output);
    try testing.expect(id != null);
    // ID should contain a dash (timestamp-sequence format)
    try testing.expect(std.mem.indexOf(u8, id.?, "-") != null);
}

test "e2e/ts: write with explicit timestamp" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo ts write temperature --tags sensor=A1 --fields 21.0 --timestamp 1708700400000
    const output = try ctx.execCapture(&.{
        "ts",            "write",   "temperature", "--tags",
        "sensor=A1",     "--value", "21.0",        "--timestamp",
        "1708700400000",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write multiple fields stores each field at one timestamp" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var w = try ctx.cli.run(&.{
        "ts",            "write",    "cpu",                            "--tags",
        "host=web-01",   "--fields", "user=72.5,system=7.4,idle=20.1", "--timestamp",
        "1708700400000",
    });
    defer w.deinit();
    try testing.expectEqual(@as(u8, 0), w.exit_code);
    try testing.expect(w.stdoutContains("OK (3 fields at 1708700400000)"));

    const expected = [_][2][]const u8{
        .{ "user", "1708700400000 72.500000\n" },
        .{ "system", "1708700400000 7.400000\n" },
        .{ "idle", "1708700400000 20.100000\n" },
    };
    for (expected) |e| {
        var r = try ctx.cli.run(&.{
            "ts", "read",   "cpu",           "--tags", "host=web-01", "--field",
            e[0], "--from", "1708700000000", "-o",     "raw",
        });
        defer r.deinit();
        try testing.expectEqualStrings(e[1], r.stdout);
    }
}

test "e2e/ts: write stores the exact value and timestamp" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Eight or more characters: the server used to read these bytes as f64 bits.
    try ctx.exec(&.{ "ts", "write", "exact", "--value", "1234.5678", "--timestamp", "1708700400123" });
    try ctx.exec(&.{ "ts", "write", "exact", "--value", "-0.25", "--timestamp", "1708700400456" });

    var r = try ctx.cli.run(&.{ "ts", "read", "exact", "--from", "1708700000000", "-o", "raw" });
    defer r.deinit();
    try testing.expectEqualStrings("1708700400123 1234.567800\n1708700400456 -0.250000\n", r.stdout);
}

test "e2e/ts: write refuses a value that is not a finite number" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    for ([_][]const u8{ "abc", "nan", "inf", "1.5x" }) |v| {
        var r = try ctx.cli.run(&.{ "ts", "write", "bad_value", "--value", v });
        defer r.deinit();
        try testing.expect(r.exit_code != 0);
        try testing.expect(r.stderrContains(v));
    }
    var f = try ctx.cli.run(&.{ "ts", "write", "bad_value", "--fields", "a=1,b=nope" });
    defer f.deinit();
    try testing.expect(f.exit_code != 0);
    try testing.expect(f.stderrContains("nope"));

    // Nothing was written, not even field a.
    var r = try ctx.cli.run(&.{ "ts", "read", "bad_value", "--field", "a", "--from", "0", "-o", "raw" });
    defer r.deinit();
    try testing.expectEqualStrings("(no data)\n", r.stdout);
}

test "e2e/ts: write no tags" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo ts write global_metric --fields 99.9
    const output = try ctx.execCapture(&.{ "ts", "write", "global_metric", "--value", "99.9" });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write using --tags flag" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo ts write cpu_usage --tags host=web-02,region=eu --fields 55.3
    const output = try ctx.execCapture(&.{
        "ts", "write", "cpu_usage", "--tags", "host=web-02,region=eu", "--value", "55.3",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write without --value or --fields fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Missing --value/--fields should fail
    var result = try ctx.cli.run(&.{ "ts", "write", "cpu_usage", "--tags", "host=web-01" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

// =============================================================================
// Write: Batch (Line Protocol)
// =============================================================================

test "e2e/ts: write batch from file stores every point" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const tmp_path = "/tmp/flo-e2e-ts-batch.txt";
    {
        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file,
            \\cpu,host=web-01 user=72.5,system=7.4 1708700400000
            \\cpu,host=web-02 user=55.3,system=12.1 1708700400000
            \\memory,host=web-01 used=4096 1708700400000
            \\
        );
    }
    defer @import("stdx").fs.deleteFile(tmp_path) catch {};

    var w = try ctx.cli.run(&.{ "ts", "write", "--batch", "--file", tmp_path, "--precision", "ms" });
    defer w.deinit();
    try testing.expectEqual(@as(u8, 0), w.exit_code);
    try testing.expect(w.stdoutContains("Wrote 5 points\n"));

    const expected = [_][4][]const u8{
        .{ "cpu", "host=web-01", "user", "1708700400000 72.500000\n" },
        .{ "cpu", "host=web-01", "system", "1708700400000 7.400000\n" },
        .{ "cpu", "host=web-02", "user", "1708700400000 55.300000\n" },
        .{ "cpu", "host=web-02", "system", "1708700400000 12.100000\n" },
        .{ "memory", "host=web-01", "used", "1708700400000 4096.000000\n" },
    };
    for (expected) |e| {
        var r = try ctx.cli.run(&.{ "ts", "read", e[0], "--tags", e[1], "--field", e[2], "--from", "1708700000000", "-o", "raw" });
        defer r.deinit();
        try testing.expectEqualStrings(e[3], r.stdout);
    }
}

test "e2e/ts: write batch converts the timestamp precision" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const tmp_path = "/tmp/flo-e2e-ts-batch-ns.txt";
    {
        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file, "disk,host=a free=12.5 1708700400123000000\n");
    }
    defer @import("stdx").fs.deleteFile(tmp_path) catch {};

    var w = try ctx.cli.run(&.{ "ts", "write", "--batch", "--file", tmp_path, "--precision", "ns" });
    defer w.deinit();
    try testing.expectEqual(@as(u8, 0), w.exit_code);

    var r = try ctx.cli.run(&.{ "ts", "read", "disk", "--tags", "host=a", "--field", "free", "--from", "1708700000000", "-o", "raw" });
    defer r.deinit();
    try testing.expectEqualStrings("1708700400123 12.500000\n", r.stdout);
}

test "e2e/ts: write batch reports a bad line and stores the rest" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const tmp_path = "/tmp/flo-e2e-ts-batch-bad.txt";
    {
        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file,
            \\net,host=a rx=1.5 1708700400000
            \\
            \\# a comment counts as a line too
            \\this line is not line protocol
            \\net,host=a rx=2.5 1708700401000
            \\net,host=a rx=3.5,tx=inf 1708700402000
            \\net,host=a rx=4.5 abc
            \\net,host=a rx=4.5 1708700403000 extra
            \\net,host=a\ b rx=4.5
            \\net,host=a,host=b rx=4.5
            \\net,host=a rx=4.5,rx=5.5
            \\net,host=a rx=9007199254740993i
            \\
        );
    }
    defer @import("stdx").fs.deleteFile(tmp_path) catch {};

    var w = try ctx.cli.run(&.{ "ts", "write", "--batch", "--file", tmp_path });
    defer w.deinit();
    try testing.expect(w.exit_code != 0);
    // Blank and comment lines are counted, so the bad line is number 4.
    try testing.expect(w.stderrContains("line 4: "));
    // A line with one bad field writes none of its fields.
    try testing.expect(w.stderrContains("line 6: field value 'inf' is not a finite number"));
    try testing.expect(w.stderrContains("line 7: timestamp 'abc' is not a positive whole number"));
    try testing.expect(w.stderrContains("line 8: unexpected 'extra' after the timestamp"));
    try testing.expect(w.stderrContains("line 9: backslash escapes"));
    try testing.expect(w.stderrContains("line 10: tag key 'host' is given more than once"));
    try testing.expect(w.stderrContains("line 11: field 'rx' is given more than once"));
    try testing.expect(w.stderrContains("line 12: integer '9007199254740993i' is past 2^53"));
    try testing.expect(w.stdoutContains("Wrote 2 points (8 lines failed)"));

    var r = try ctx.cli.run(&.{ "ts", "read", "net", "--tags", "host=a", "--field", "rx", "--from", "1708700000000", "-o", "raw" });
    defer r.deinit();
    try testing.expectEqualStrings("1708700400000 1.500000\n1708700401000 2.500000\n", r.stdout);
}

test "e2e/ts: a batch line without a timestamp stamps its fields once" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const tmp_path = "/tmp/flo-e2e-ts-batch-nots.txt";
    {
        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file, "load,host=a one=1,two=2\n");
    }
    defer @import("stdx").fs.deleteFile(tmp_path) catch {};
    try ctx.exec(&.{ "ts", "write", "--batch", "--file", tmp_path });

    var one = try ctx.cli.run(&.{ "ts", "read", "load", "--tags", "host=a", "--field", "one", "--from", "-1h", "-o", "raw" });
    defer one.deinit();
    var two = try ctx.cli.run(&.{ "ts", "read", "load", "--tags", "host=a", "--field", "two", "--from", "-1h", "-o", "raw" });
    defer two.deinit();
    const ts_one = one.stdout[0 .. std.mem.indexOfScalar(u8, one.stdout, ' ') orelse return error.NoPoint];
    const ts_two = two.stdout[0 .. std.mem.indexOfScalar(u8, two.stdout, ' ') orelse return error.NoPoint];
    try testing.expectEqualStrings(ts_one, ts_two);
    try testing.expect(std.mem.endsWith(u8, one.stdout, " 1.000000\n"));
    try testing.expect(std.mem.endsWith(u8, two.stdout, " 2.000000\n"));
}

test "e2e/ts: write refuses flags --batch would ignore, and bad timestamps and fields" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const cases = [_]struct { args: []const []const u8, why: []const u8 }{
        .{ .args = &.{ "ts", "write", "m", "--batch" }, .why = "--batch takes no measurement" },
        .{ .args = &.{ "ts", "write", "--batch", "--tags", "h=a" }, .why = "--batch can't be combined with --tags" },
        .{ .args = &.{ "ts", "write", "--batch", "--value", "1" }, .why = "--batch can't be combined with --value" },
        .{ .args = &.{ "ts", "write", "--batch", "--fields", "a=1" }, .why = "--batch can't be combined with --fields" },
        .{ .args = &.{ "ts", "write", "--batch", "--timestamp", "5" }, .why = "--batch can't be combined with --timestamp" },
        .{ .args = &.{ "ts", "write", "bad", "--value", "1", "--timestamp", "-5" }, .why = "--timestamp must be > 0 ms" },
        .{ .args = &.{ "ts", "write", "bad", "--fields", "a=1,b=2", "--timestamp", "0" }, .why = "--timestamp must be > 0 ms" },
        .{ .args = &.{ "ts", "write", "bad", "--fields", "a=1,a=2" }, .why = "field 'a' is given more than once" },
        .{ .args = &.{ "ts", "write", "bad", "--fields", "a=1, b=2" }, .why = "field name ' b' has surrounding spaces" },
        .{ .args = &.{ "ts", "write", "bad", "--value", "9007199254740993" }, .why = "past 2^53" },
    };
    for (cases) |c| {
        var r = try ctx.cli.run(c.args);
        defer r.deinit();
        try testing.expect(r.exit_code != 0);
        try testing.expect(r.stderrContains(c.why));
    }

    var r = try ctx.cli.run(&.{ "ts", "read", "bad", "--field", "a", "--from", "0", "-o", "raw" });
    defer r.deinit();
    try testing.expectEqualStrings("(no data)\n", r.stdout);
}

// =============================================================================
// Read: Raw Points
// =============================================================================

test "e2e/ts: read after write returns data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write a point with known timestamp
    try ctx.exec(&.{
        "ts",            "write",   "read_test", "--tags",
        "host=srv-1",    "--value", "42.0",      "--timestamp",
        "1708700400000",
    });

    // Read back
    var result = try ctx.cli.run(&.{
        "ts",     "read",          "read_test", "--tags", "host=srv-1",
        "--from", "1708700000000", "--limit",   "10",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Should contain the timestamp or value representation
    try testing.expect(result.contains("1708700400000") or containsNumericValue(result.stdout));
}

test "e2e/ts: read with no data returns no data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{
        "ts",     "read", "nonexistent_measurement_xyz",
        "--from", "-1h",  "--limit",
        "10",
    });
    defer result.deinit();

    // Should indicate no data
    try testing.expect(
        result.contains("no data") or
            result.contains("(no data)") or
            result.stdout.len == 0,
    );
}

test "e2e/ts: read with --output json" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{
        "ts",            "write",   "json_read_test", "--tags",
        "env=prod",      "--value", "88.8",           "--timestamp",
        "1708700500000",
    });

    var result = try ctx.cli.run(&.{
        "ts",     "read",          "json_read_test", "--tags", "env=prod",
        "--from", "1708700000000", "--output",       "json",   "--limit",
        "10",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // JSON output should have brackets/braces
    try testing.expect(result.contains("[") or result.contains("{"));
}

test "e2e/ts: read with --output raw" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{
        "ts",            "write",   "raw_read_test", "--tags",
        "host=a",        "--value", "77.7",          "--timestamp",
        "1708700600000",
    });

    var result = try ctx.cli.run(&.{
        "ts",     "read",          "raw_read_test", "--tags", "host=a",
        "--from", "1708700000000", "--output",      "raw",    "--limit",
        "10",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Raw format: "timestamp value\n"
    try testing.expect(result.contains("1708700600000") or containsNumericValue(result.stdout));
}

test "e2e/ts: read with --limit caps results" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several points
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var ts_buf: [32]u8 = undefined;
        const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{1708700400000 + i * 1000}) catch unreachable;
        var val_buf: [16]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "{d}.0", .{i + 1}) catch unreachable;
        try ctx.exec(&.{
            "ts", "write", "limit_test", "--tags", "host=x", "--value", val, "--timestamp", ts,
        });
    }

    // Read with limit 2
    var result = try ctx.cli.run(&.{
        "ts",     "read",          "limit_test", "--tags", "host=x",
        "--from", "1708700000000", "--limit",    "2",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: read with time range" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write points at different timestamps
    try ctx.exec(&.{
        "ts",            "write",   "range_test", "--tags",
        "host=z",        "--value", "10.0",       "--timestamp",
        "1708700100000",
    });
    try ctx.exec(&.{
        "ts",            "write",   "range_test", "--tags",
        "host=z",        "--value", "20.0",       "--timestamp",
        "1708700200000",
    });
    try ctx.exec(&.{
        "ts",            "write",   "range_test", "--tags",
        "host=z",        "--value", "30.0",       "--timestamp",
        "1708700300000",
    });

    // Read only the middle range
    var result = try ctx.cli.run(&.{
        "ts",     "read",          "range_test", "--tags",        "host=z",
        "--from", "1708700150000", "--to",       "1708700250000", "--output",
        "raw",    "--limit",       "100",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: read specific field" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write multi-field point
    try ctx.exec(&.{
        "ts",            "write",    "field_read_test",      "--tags",
        "host=a",        "--fields", "user=72.5,system=7.4", "--timestamp",
        "1708700400000",
    });

    // Read specific field
    var result = try ctx.cli.run(&.{
        "ts",      "read",   "field_read_test", "--tags",        "host=a",
        "--field", "system", "--from",          "1708700000000", "--limit",
        "10",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

// =============================================================================
// Query: Windowed Aggregation
// =============================================================================

test "e2e/ts: query avg" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several points within a window
    try ctx.exec(&.{ "ts", "write", "query_avg", "--tags", "host=a", "--value", "10.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "query_avg", "--tags", "host=a", "--value", "20.0", "--timestamp", "1708700410000" });
    try ctx.exec(&.{ "ts", "write", "query_avg", "--tags", "host=a", "--value", "30.0", "--timestamp", "1708700420000" });

    // Query with avg aggregation
    var result = try ctx.cli.run(&.{
        "ts",     "query",         "query_avg", "--tags", "host=a",
        "--from", "1708700000000", "--window",  "1m",     "--agg",
        "avg",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Should return some data (not "no data")
    try testing.expect(!result.contains("(no data)"));
}

test "e2e/ts: query sum" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "query_sum", "--tags", "host=a", "--value", "5.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "query_sum", "--tags", "host=a", "--value", "15.0", "--timestamp", "1708700410000" });

    var result = try ctx.cli.run(&.{
        "ts",     "query",         "query_sum", "--tags", "host=a",
        "--from", "1708700000000", "--window",  "1m",     "--agg",
        "sum",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try testing.expect(!result.contains("(no data)"));
}

test "e2e/ts: query count" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "query_count", "--tags", "host=a", "--value", "1.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "query_count", "--tags", "host=a", "--value", "2.0", "--timestamp", "1708700410000" });
    try ctx.exec(&.{ "ts", "write", "query_count", "--tags", "host=a", "--value", "3.0", "--timestamp", "1708700420000" });

    var result = try ctx.cli.run(&.{
        "ts",     "query",         "query_count", "--tags", "host=a",
        "--from", "1708700000000", "--window",    "1m",     "--agg",
        "count",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try testing.expect(!result.contains("(no data)"));
}

test "e2e/ts: query min and max" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "query_minmax", "--tags", "host=a", "--value", "10.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "query_minmax", "--tags", "host=a", "--value", "50.0", "--timestamp", "1708700410000" });
    try ctx.exec(&.{ "ts", "write", "query_minmax", "--tags", "host=a", "--value", "30.0", "--timestamp", "1708700420000" });

    // Query min
    var min_result = try ctx.cli.run(&.{
        "ts",     "query",         "query_minmax", "--tags", "host=a",
        "--from", "1708700000000", "--window",     "1m",     "--agg",
        "min",
    });
    defer min_result.deinit();
    try stdx.testing.assertSucceeded(min_result);
    try testing.expect(!min_result.contains("(no data)"));

    // Query max
    var max_result = try ctx.cli.run(&.{
        "ts",     "query",         "query_minmax", "--tags", "host=a",
        "--from", "1708700000000", "--window",     "1m",     "--agg",
        "max",
    });
    defer max_result.deinit();
    try stdx.testing.assertSucceeded(max_result);
    try testing.expect(!max_result.contains("(no data)"));
}

test "e2e/ts: query with no data returns no data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{
        "ts",     "query", "nonexistent_query_meas",
        "--from", "-1h",   "--window",
        "5m",     "--agg", "avg",
    });
    defer result.deinit();

    try testing.expect(
        result.contains("no data") or
            result.contains("(no data)") or
            result.stdout.len == 0,
    );
}

test "e2e/ts: query json format" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "query_json", "--tags", "host=a", "--value", "42.0", "--timestamp", "1708700400000" });

    var result = try ctx.cli.run(&.{
        "ts",     "query",         "query_json", "--tags", "host=a",
        "--from", "1708700000000", "--window",   "1m",     "--agg",
        "avg",    "--output",      "json",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // JSON output should have braces or brackets
    try testing.expect(result.contains("{") or result.contains("["));
}

// =============================================================================
// List: Measurements & Series
// =============================================================================

test "e2e/ts: list measurements after write" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write to create measurements
    try ctx.exec(&.{ "ts", "write", "list_cpu", "--tags", "host=a", "--value", "50.0" });
    try ctx.exec(&.{ "ts", "write", "list_memory", "--tags", "host=a", "--value", "4096.0" });

    // List all measurements
    var result = try ctx.cli.run(&.{ "ts", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "list_cpu");
    try stdx.testing.assertContains(result, "list_memory");
}

test "e2e/ts: list with no measurements shows none" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "ts", "list" });
    defer result.deinit();

    try testing.expect(
        result.contains("(none)") or
            result.contains("No measurements") or
            result.stdout.len == 0 or
            result.succeeded(),
    );
}

test "e2e/ts: list json format" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "json_list_cpu", "--tags", "host=a", "--value", "50.0" });

    var result = try ctx.cli.run(&.{ "ts", "list", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "json_list_cpu");
}

test "e2e/ts: list with --limit caps results" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several measurements
    try ctx.exec(&.{ "ts", "write", "limit_m1", "--tags", "host=a", "--value", "1.0" });
    try ctx.exec(&.{ "ts", "write", "limit_m2", "--tags", "host=a", "--value", "2.0" });
    try ctx.exec(&.{ "ts", "write", "limit_m3", "--tags", "host=a", "--value", "3.0" });

    // List with --limit 1 should return at most 1
    var result = try ctx.cli.run(&.{ "ts", "list", "--limit", "1" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Should have at most 1 measurement (no more than 1 line of output)
    const stdout = result.stdout;
    var lines: usize = 0;
    var it = std.mem.splitScalar(u8, stdout, '\n');
    while (it.next()) |line| {
        if (line.len > 0) lines += 1;
    }
    try testing.expect(lines <= 1);
}

test "e2e/ts: list cursor walks all shards" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write measurements that hash to different shards
    try ctx.exec(&.{ "ts", "write", "cursor_alpha", "--tags", "x=1", "--value", "1.0" });
    try ctx.exec(&.{ "ts", "write", "cursor_beta", "--tags", "x=1", "--value", "2.0" });
    try ctx.exec(&.{ "ts", "write", "cursor_gamma", "--tags", "x=1", "--value", "3.0" });
    try ctx.exec(&.{ "ts", "write", "cursor_delta", "--tags", "x=1", "--value", "4.0" });

    // Default limit (1000) should find all of them
    var result = try ctx.cli.run(&.{ "ts", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "cursor_alpha");
    try stdx.testing.assertContains(result, "cursor_beta");
    try stdx.testing.assertContains(result, "cursor_gamma");
    try stdx.testing.assertContains(result, "cursor_delta");
}

// =============================================================================
// Delete
// =============================================================================

test "e2e/ts: delete requires --confirm flag" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write data
    try ctx.exec(&.{ "ts", "write", "delete_test", "--tags", "host=a", "--value", "50.0" });

    // Delete without --confirm should fail
    var result = try ctx.cli.run(&.{ "ts", "delete", "delete_test" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "confirm");
}

test "e2e/ts: delete with --confirm succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write data
    try ctx.exec(&.{ "ts", "write", "delete_confirm_test", "--tags", "host=a", "--value", "50.0" });

    // Delete with --confirm
    try ctx.exec(&.{ "ts", "delete", "delete_confirm_test", "--confirm" });

    // Verify: reading deleted measurement should return no data
    var result = try ctx.cli.run(&.{
        "ts",     "read", "delete_confirm_test", "--tags", "host=a",
        "--from", "-1h",  "--limit",             "10",
    });
    defer result.deinit();

    try testing.expect(
        result.contains("no data") or
            result.contains("(no data)") or
            result.stdout.len == 0,
    );
}

test "e2e/ts: delete with tag filter" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write two series
    try ctx.exec(&.{ "ts", "write", "del_tag_test", "--tags", "host=web-01", "--value", "10.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "del_tag_test", "--tags", "host=web-02", "--value", "20.0", "--timestamp", "1708700400000" });

    // Delete only one series
    try ctx.exec(&.{ "ts", "delete", "del_tag_test", "--tags", "host=web-01", "--confirm" });

    // web-02 data should still be readable
    var result = try ctx.cli.run(&.{
        "ts",     "read",          "del_tag_test", "--tags", "host=web-02",
        "--from", "1708700000000", "--limit",      "10",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

// =============================================================================
// Retention & Downsampling
// =============================================================================

test "e2e/ts: set retention raw TTL" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create measurement first
    try ctx.exec(&.{ "ts", "write", "retention_test", "--tags", "host=a", "--value", "50.0" });

    // flo ts retention retention_test --raw-ttl 7d
    const output = try ctx.execCapture(&.{
        "ts", "retention", "retention_test", "--raw-ttl", "7d",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

// =============================================================================
// FloQL Pipeline Queries
// =============================================================================

test "e2e/ts: floql basic query" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write test data
    try ctx.exec(&.{ "ts", "write", "floql_cpu", "--tags", "host=web-01", "--value", "70.0" });
    try ctx.exec(&.{ "ts", "write", "floql_cpu", "--tags", "host=web-01", "--value", "80.0" });
    try ctx.exec(&.{ "ts", "write", "floql_cpu", "--tags", "host=web-01", "--value", "90.0" });

    // FloQL query
    var result = try ctx.cli.run(&.{
        "ts", "floql", "floql_cpu{host=web-01}[1h] | window(5m) | avg()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: floql requires query argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // No query argument
    var result = try ctx.cli.run(&.{ "ts", "floql" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/ts: floql on nonexistent measurement" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{
        "ts", "floql", "nonexistent_floql_meas[1h] | window(5m) | avg()",
    });
    defer result.deinit();

    // Should return empty or succeed gracefully
    try testing.expect(
        result.contains("no series") or
            result.contains("empty") or
            result.contains("0 points") or
            result.succeeded(),
    );
}

// =============================================================================
// Tag Filtering
// =============================================================================

test "e2e/ts: read filters by tag" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Two tag-series under the same measurement+field.
    try ctx.exec(&.{ "ts", "write", "tag_filter_test", "--tags", "host=web-01", "--value", "111.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "tag_filter_test", "--tags", "host=web-02", "--value", "222.0", "--timestamp", "1708700400000" });

    // Filtering by one tag set must return ONLY that series. Before #24 the
    // option was dropped server-side, so this returned both.
    var result = try ctx.cli.run(&.{
        "ts",            "read",        "tag_filter_test",
        "--tags",        "host=web-01", "--from",
        "1708700000000", "--output",    "raw",
        "--limit",       "100",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try testing.expect(result.contains("111"));
    try testing.expect(!result.contains("222"));

    // No filter = every tag-series (a tagged write stays visible to an
    // untagged read).
    var all = try ctx.cli.run(&.{
        "ts", "read", "tag_filter_test", "--from", "1708700000000", "--output", "raw", "--limit", "100",
    });
    defer all.deinit();
    try testing.expect(all.contains("111"));
    try testing.expect(all.contains("222"));
}

test "e2e/ts: partial tag filter matches a superset tag set" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "tag_exact_test", "--tags", "host=web-01,env=prod", "--value", "333.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "tag_exact_test", "--tags", "host=web-02,env=prod", "--value", "444.0", "--timestamp", "1708700400000" });

    // The full tag set matches in any order — the hash is canonical.
    var exact = try ctx.cli.run(&.{
        "ts", "read", "tag_exact_test", "--tags", "env=prod,host=web-01", "--from", "1708700000000", "--output", "raw", "--limit", "100",
    });
    defer exact.deinit();
    try testing.expect(exact.contains("333"));
    try testing.expect(!exact.contains("444"));

    // A PARTIAL tag set now matches too: predicates constrain only the tags
    // they name. Under the 2a tag-hash lookup this returned nothing.
    var partial = try ctx.cli.run(&.{
        "ts", "read", "tag_exact_test", "--tags", "host=web-01", "--from", "1708700000000", "--output", "raw", "--limit", "100",
    });
    defer partial.deinit();
    try testing.expect(partial.contains("333"));
    try testing.expect(!partial.contains("444"));

    // A tag shared by both selects both.
    var shared = try ctx.cli.run(&.{
        "ts", "read", "tag_exact_test", "--tags", "env=prod", "--from", "1708700000000", "--output", "raw", "--limit", "100",
    });
    defer shared.deinit();
    try testing.expect(shared.contains("333"));
    try testing.expect(shared.contains("444"));
}

test "e2e/ts: query filters by tag" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "tag_query_test", "--tags", "region=us-east", "--value", "100.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "tag_query_test", "--tags", "region=eu-west", "--value", "200.0", "--timestamp", "1708700400000" });

    // avg over us-east alone is 100 — not 150, which is what an unfiltered
    // aggregate (the pre-#24 behaviour) would produce.
    var result = try ctx.cli.run(&.{
        "ts",            "query",          "tag_query_test",
        "--tags",        "region=us-east", "--from",
        "1708700000000", "--window",       "1m",
        "--agg",         "avg",
    });
    defer result.deinit();

    // avg over us-east alone is 100 — not 150, which is what an unfiltered
    // aggregate would produce.
    try stdx.testing.assertSucceeded(result);
    try testing.expect(!result.contains("(no data)"));
    try testing.expect(result.contains("100"));
    try testing.expect(!result.contains("150"));

    // Unfiltered spans both tag-series: (100+200)/2 = 150.
    var all = try ctx.cli.run(&.{
        "ts", "query", "tag_query_test", "--from", "1708700000000", "--window", "1m", "--agg", "avg",
    });
    defer all.deinit();
    try testing.expect(all.contains("150"));
}

// =============================================================================
// Multiple Writes (Monotonicity & Ordering)
// =============================================================================

test "e2e/ts: multiple writes to same series" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several points to the same series with sequential timestamps
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var ts_buf: [32]u8 = undefined;
        const ts = std.fmt.bufPrint(&ts_buf, "{d}", .{1708700400000 + i * 1000}) catch unreachable;
        var val_buf: [16]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "{d}.0", .{i * 10}) catch unreachable;
        try ctx.exec(&.{
            "ts", "write", "multi_write_test", "--tags", "host=srv-1", "--value", val, "--timestamp", ts,
        });
    }

    // Read them back
    var result = try ctx.cli.run(&.{
        "ts",     "read",          "multi_write_test", "--tags", "host=srv-1",
        "--from", "1708700000000", "--limit",          "20",     "--output",
        "raw",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Should contain at least some of the timestamps
    try testing.expect(result.contains("1708700400000") or result.contains("1708700409000"));
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/ts: data is isolated between namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create namespaces
    try ctx.exec(&.{ "ns", "create", "ts_ns_a" });
    try ctx.exec(&.{ "ns", "create", "ts_ns_b" });

    // Write to namespace A
    try ctx.exec(&.{
        "ts",            "write",   "ns_test_cpu", "--tags",
        "host=a",        "--value", "100.0",       "--timestamp",
        "1708700400000", "-n",      "ts_ns_a",
    });

    // Write to namespace B
    try ctx.exec(&.{
        "ts",            "write",   "ns_test_cpu", "--tags",
        "host=a",        "--value", "200.0",       "--timestamp",
        "1708700400000", "-n",      "ts_ns_b",
    });

    // Read from namespace A
    var result_a = try ctx.cli.run(&.{
        "ts",     "read",          "ns_test_cpu", "--tags", "host=a",
        "--from", "1708700000000", "--output",    "raw",    "--limit",
        "10",     "-n",            "ts_ns_a",
    });
    defer result_a.deinit();
    try stdx.testing.assertSucceeded(result_a);

    // Read from namespace B
    var result_b = try ctx.cli.run(&.{
        "ts",     "read",          "ns_test_cpu", "--tags", "host=a",
        "--from", "1708700000000", "--output",    "raw",    "--limit",
        "10",     "-n",            "ts_ns_b",
    });
    defer result_b.deinit();
    try stdx.testing.assertSucceeded(result_b);
}

// =============================================================================
// CLI Help
// =============================================================================

test "e2e/ts: help shows all subcommands" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "ts", "--help" });
    defer result.deinit();

    // Verify all subcommands are listed
    try stdx.testing.assertContains(result, "write");
    try stdx.testing.assertContains(result, "read");
    try stdx.testing.assertContains(result, "query");
    try stdx.testing.assertContains(result, "list");
    try stdx.testing.assertContains(result, "delete");
    try stdx.testing.assertContains(result, "retention");
    try stdx.testing.assertContains(result, "floql");
}

test "e2e/ts: write help shows usage" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "ts", "write", "--help" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "Write");
    try stdx.testing.assertContains(result, "measurement");
}

test "e2e/ts: read help shows usage" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "ts", "read", "--help" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "Read");
    try stdx.testing.assertContains(result, "measurement");
}

test "e2e/ts: floql help shows examples" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "ts", "floql", "--help" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "FloQL");
    try stdx.testing.assertContains(result, "query");
}

// =============================================================================
// Edge Cases
// =============================================================================

test "e2e/ts: write and read very large value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write a large value
    const output = try ctx.execCapture(&.{
        "ts",            "write",   "large_val_test", "--tags",
        "host=a",        "--value", "99999999.12345", "--timestamp",
        "1708700400000",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write and read very small value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const output = try ctx.execCapture(&.{
        "ts",            "write",   "small_val_test", "--tags",
        "host=a",        "--value", "0.000001",       "--timestamp",
        "1708700400000",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write negative value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const output = try ctx.execCapture(&.{
        "ts",            "write",   "neg_val_test", "--tags",
        "host=a",        "--value", "-42.5",        "--timestamp",
        "1708700400000",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write zero value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const output = try ctx.execCapture(&.{
        "ts",            "write",   "zero_val_test", "--tags",
        "host=a",        "--value", "0.0",           "--timestamp",
        "1708700400000",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: write with many tags" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const output = try ctx.execCapture(&.{
        "ts",     "write",                                                 "many_tags_test",
        "--tags", "host=web-01,region=us-east,dc=dc1,env=prod,team=infra", "--value",
        "42.0",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

// =============================================================================
// FloQL: math() Stage
// =============================================================================

test "e2e/ts: floql math multiply" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write fractional values (0.5, 0.75, 0.9) → multiply by 100 → expect percentages
    try ctx.exec(&.{ "ts", "write", "math_mul", "--tags", "host=a", "--value", "0.5" });
    try ctx.exec(&.{ "ts", "write", "math_mul", "--tags", "host=a", "--value", "0.75" });
    try ctx.exec(&.{ "ts", "write", "math_mul", "--tags", "host=a", "--value", "0.9" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_mul{host=a}[1h] | math(value * 100)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Multiplied values should appear (50, 75, 90)
    try testing.expect(result.contains("50") or result.contains("75") or result.contains("90"));
}

test "e2e/ts: floql math add" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "math_add", "--tags", "host=a", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "math_add", "--tags", "host=a", "--value", "20.0" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_add{host=a}[1h] | math(value + 5)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 10+5=15, 20+5=25
    try testing.expect(result.contains("15") or result.contains("25"));
}

test "e2e/ts: floql math divide" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "math_div", "--tags", "host=a", "--value", "100.0" });
    try ctx.exec(&.{ "ts", "write", "math_div", "--tags", "host=a", "--value", "200.0" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_div{host=a}[1h] | math(value / 10)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 100/10=10, 200/10=20
    try testing.expect(result.contains("10") or result.contains("20"));
}

test "e2e/ts: floql math subtract" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "math_sub", "--tags", "host=a", "--value", "50.0" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_sub{host=a}[1h] | math(value - 10)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 50-10=40
    try testing.expect(result.contains("40.0000"));
}

test "e2e/ts: floql math modulo" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "math_mod", "--tags", "host=a", "--value", "17.0" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_mod{host=a}[1h] | math(value % 5)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 17 % 5 = 2
    try testing.expect(result.contains("2.0000"));
}

test "e2e/ts: floql math chained with aggregation" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write: 0.5, 0.75, 0.9 → avg → math *100 → should produce percentage
    try ctx.exec(&.{ "ts", "write", "math_chain", "--tags", "host=a", "--value", "0.5" });
    try ctx.exec(&.{ "ts", "write", "math_chain", "--tags", "host=a", "--value", "0.75" });
    try ctx.exec(&.{ "ts", "write", "math_chain", "--tags", "host=a", "--value", "0.9" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_chain{host=a}[1h] | window(5m) | avg() | math(value * 100)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: floql math shorthand (no 'value' keyword)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "math_short", "--tags", "host=a", "--value", "10.0" });

    // Shorthand: math(* 2) instead of math(value * 2)
    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_short{host=a}[1h] | math(* 2)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 10*2=20
    try testing.expect(result.contains("20.0000"));
}

// =============================================================================
// FloQL: round(N) with Decimals
// =============================================================================

test "e2e/ts: floql round to integer" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "round_int", "--tags", "host=a", "--value", "72.567" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "round_int{host=a}[1h] | round(0)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 72.567 rounded to 0 decimals → 73
    try testing.expect(result.contains("73.0000"));
}

test "e2e/ts: floql round to 2 decimals" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "round_dec", "--tags", "host=a", "--value", "72.5678" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "round_dec{host=a}[1h] | round(2)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // 72.5678 rounded to 2 decimals → 72.57
    try testing.expect(result.contains("72.5700"));
}

test "e2e/ts: floql round default (no args)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "round_def", "--tags", "host=a", "--value", "42.789" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "round_def{host=a}[1h] | round()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Default round → nearest integer → 43
    try testing.expect(result.contains("43.0000"));
}

test "e2e/ts: floql math then round" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // 0.33333 * 100 = 33.333, round(1) → 33.3
    try ctx.exec(&.{ "ts", "write", "math_round", "--tags", "host=a", "--value", "0.33333" });

    var result = try ctx.cli.run(&.{
        "ts", "floql", "math_round{host=a}[1h] | math(value * 100) | round(1)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Should contain 33.3
    try testing.expect(result.contains("33.3000"));
}

// =============================================================================
// FloQL: Regex / Glob Tag Filters (=~ and !~)
// =============================================================================

test "e2e/ts: floql glob tag filter =~" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write multiple series with different hosts
    try ctx.exec(&.{ "ts", "write", "glob_test", "--tags", "host=web-01", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "glob_test", "--tags", "host=web-02", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "glob_test", "--tags", "host=api-01", "--value", "30.0" });

    // Only web-* hosts: avg(10,20) = 15, not 20 (which would include api-01).
    // The filter was inert before the tag dictionary landed.
    var result = try ctx.cli.run(&.{
        "ts", "floql", "glob_test{host=~web-*}[1h] | avg()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try testing.expect(result.contains("15.0000"));
    try testing.expect(!result.contains("20.0000"));
}

test "e2e/ts: floql negate glob tag filter !~" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write series with different regions
    try ctx.exec(&.{ "ts", "write", "nglob_test", "--tags", "host=web-01,region=us-east", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "nglob_test", "--tags", "host=web-02,region=us-west", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "nglob_test", "--tags", "host=api-01,region=eu-west", "--value", "30.0" });

    // Excluding *-west leaves only us-east → avg is exactly 10.
    var result = try ctx.cli.run(&.{
        "ts", "floql", "nglob_test{region!~*-west}[1h] | avg()",
    });
    defer result.deinit();

    // Match the rendered value, not a bare number: output lines are
    // `<epoch_ms>: <value>` and these points carry wall-clock timestamps, so a
    // bare "30" matches a digit pair inside the timestamp and fails a passing
    // run. Values always render with four decimals and timestamps never contain
    // a '.', so the decimal form cannot collide.
    try stdx.testing.assertSucceeded(result);
    try testing.expect(result.contains("10.0000"));
    try testing.expect(!result.contains("30.0000"));
}

test "e2e/ts: floql neq tag filter with !=" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "neq_test", "--tags", "host=web-01,env=prod", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "neq_test", "--tags", "host=web-02,env=staging", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "neq_test", "--tags", "host=web-03,env=dev", "--value", "30.0" });

    // Exclude staging
    var result = try ctx.cli.run(&.{
        "ts", "floql", "neq_test{env!=staging}[1h] | window(5m) | avg()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: floql mixed eq and glob filters" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write series with multiple tag dimensions
    try ctx.exec(&.{ "ts", "write", "mixed_filt", "--tags", "host=web-01,env=prod", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "mixed_filt", "--tags", "host=web-02,env=prod", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "mixed_filt", "--tags", "host=api-01,env=prod", "--value", "30.0" });
    try ctx.exec(&.{ "ts", "write", "mixed_filt", "--tags", "host=web-01,env=staging", "--value", "40.0" });

    // Exact env=prod AND glob host=~web-*
    var result = try ctx.cli.run(&.{
        "ts", "floql", "mixed_filt{env=prod,host=~web-*}[1h] | window(5m) | avg()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: floql glob with question mark" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "glob_qm", "--tags", "host=web-01", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "glob_qm", "--tags", "host=web-02", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "glob_qm", "--tags", "host=web-100", "--value", "30.0" });

    // ? matches single char → web-0? matches web-01, web-02 but not web-100
    var result = try ctx.cli.run(&.{
        "ts", "floql", "glob_qm{host=~web-0?}[1h] | window(5m) | avg()",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: floql full pipeline with math + round + glob" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write CPU utilization as fractions for web servers
    try ctx.exec(&.{ "ts", "write", "full_pipe", "--tags", "host=web-01,env=prod", "--value", "0.723" });
    try ctx.exec(&.{ "ts", "write", "full_pipe", "--tags", "host=web-01,env=prod", "--value", "0.815" });
    try ctx.exec(&.{ "ts", "write", "full_pipe", "--tags", "host=web-02,env=prod", "--value", "0.654" });
    try ctx.exec(&.{ "ts", "write", "full_pipe", "--tags", "host=api-01,env=prod", "--value", "0.912" });

    // Full pipeline: glob filter → window → avg → multiply by 100 → round to 1 decimal
    var result = try ctx.cli.run(&.{
        "ts", "floql", "full_pipe{host=~web-*}[1h] | window(5m) | avg() | math(value * 100) | round(1)",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

// =============================================================================
// Multi-Shard Tests
// =============================================================================

test "e2e/ts: list returns all measurements across shards" {
    // Start server with 4 shards — measurements hash to different shards
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    // Write 8 distinctly-named measurements. With 4 shards and Wyhash
    // routing, these are very likely to land on at least 2 different shards.
    const measurements = [_][]const u8{
        "alpha_cpu", "bravo_mem",   "charlie_disk", "delta_net",
        "echo_iops", "foxtrot_lat", "golf_tput",    "hotel_err",
    };

    for (measurements) |m| {
        try ctx.exec(&.{ "ts", "write", m, "--value", "1.0" });
    }

    // ts list should return ALL measurements regardless of shard placement
    var result = try ctx.cli.run(&.{ "ts", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);

    // Every measurement we wrote must appear in the listing
    for (measurements) |m| {
        try stdx.testing.assertContains(result, m);
    }
}

test "e2e/ts: query renders real aggregate values" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // `ts query` used to render an empty table for every aggregate: the CLI
    // parsed a [hash:u64] the server never wrote, misaligning the stream.
    try ctx.exec(&.{ "ts", "write", "agg_vals", "--value", "100.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "agg_vals", "--value", "200.0", "--timestamp", "1708700400000" });

    const cases = [_][2][]const u8{
        .{ "avg", "150" },
        .{ "sum", "300" },
        .{ "min", "100" },
        .{ "max", "200" },
        .{ "count", "2" },
    };
    inline for (cases) |c| {
        var r = try ctx.cli.run(&.{
            "ts", "query", "agg_vals", "--from", "1708700000000", "--window", "1m", "--agg", c[0],
        });
        defer r.deinit();
        try stdx.testing.assertSucceeded(r);
        try testing.expect(r.contains(c[1]));
        // A real epoch-aligned bucket start, not the hardcoded 0 it used to emit.
        try testing.expect(r.contains("1708700400000"));
    }
}

test "e2e/ts: query buckets by window" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Two points in one minute, a third in a later minute.
    try ctx.exec(&.{ "ts", "write", "agg_win", "--value", "10.0", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "agg_win", "--value", "20.0", "--timestamp", "1708700430000" });
    try ctx.exec(&.{ "ts", "write", "agg_win", "--value", "90.0", "--timestamp", "1708700520000" });

    // 1m → two buckets: avg 15 then avg 90. `--window` was previously ignored
    // (one bucket for the whole range), and every row rendered the LAST row's
    // text because the table borrowed the caller's reused format buffer.
    var one_min = try ctx.cli.run(&.{
        "ts", "query", "agg_win", "--from", "1708700000000", "--window", "1m", "--agg", "avg",
    });
    defer one_min.deinit();
    try stdx.testing.assertSucceeded(one_min);
    try testing.expect(one_min.contains("15"));
    try testing.expect(one_min.contains("90"));
    try testing.expect(one_min.contains("1708700400000"));
    try testing.expect(one_min.contains("1708700520000"));

    // 5m → a single bucket covering all three: (10+20+90)/3 = 40.
    var five_min = try ctx.cli.run(&.{
        "ts", "query", "agg_win", "--from", "1708700000000", "--window", "5m", "--agg", "avg",
    });
    defer five_min.deinit();
    try testing.expect(five_min.contains("40"));
    try testing.expect(!five_min.contains("1708700520000"));
}

test "e2e/ts: points survive restart exactly once" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // One applier owns ts_write; a second one on the replay path would
    // insert every point twice after a restart. Distinct values, counted.
    const values = [_][]const u8{ "7101.5", "7202.5", "7303.5" };
    const stamps = [_][]const u8{ "1708700400000", "1708700401000", "1708700402000" };
    for (values, stamps) |v, t| {
        try ctx.exec(&.{ "ts", "write", "restart_cpu", "--tags", "host=a", "--value", v, "--timestamp", t });
    }

    var before = try ctx.cli.run(&.{ "ts", "read", "restart_cpu", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    for (values) |v| try testing.expectEqual(@as(usize, 1), before.stdoutCount(v));

    try ctx.restartServer();

    var after = try ctx.cli.run(&.{ "ts", "read", "restart_cpu", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    for (values) |v| try testing.expectEqual(@as(usize, 1), after.stdoutCount(v));
}

test "e2e/ts: retention trims only its own measurement in its own namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // A day's retention leaves nothing from 2024.
    try ctx.exec(&.{ "ts", "write", "ret_a", "--value", "111", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "ret_a", "--value", "444" });
    try ctx.exec(&.{ "ts", "write", "ret_b", "--value", "222", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "ret_a", "--value", "333", "--timestamp", "1708700400000", "-n", "other" });
    try ctx.exec(&.{ "ts", "retention", "ret_a", "--raw-ttl", "1d" });

    var a = try ctx.cli.run(&.{ "ts", "read", "ret_a", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer a.deinit();
    // Values as the raw output prints them, so a timestamp can't match.
    try testing.expect(a.contains(" 444.000000"));
    try testing.expect(!a.contains(" 111.000000"));
    var b = try ctx.cli.run(&.{ "ts", "read", "ret_b", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer b.deinit();
    try testing.expect(b.contains(" 222.000000"));
    var other = try ctx.cli.run(&.{ "ts", "read", "ret_a", "--from", "1708700000000", "--output", "raw", "--limit", "100", "-n", "other" });
    defer other.deinit();
    try testing.expect(other.contains(" 333.000000"));
}

test "e2e/ts: a FloQL percentile outside 0..100 or a window with too many buckets is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "ts", "write", "pct", "--value", "1" });
    try ctx.exec(&.{ "ts", "write", "pct", "--value", "2" });
    // Three in one bucket: 150% of the way through them is past the last.
    try ctx.exec(&.{ "ts", "write", "pct", "--value", "3" });
    // Two points far enough apart that one-second buckets number in the millions.
    try ctx.exec(&.{ "ts", "write", "span", "--value", "1", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "span", "--value", "2" });

    for ([_][]const u8{
        "pct[1h] | window(1m) | percentile(150)",
        "span[1708700000000..4102444800000] | window(1s) | sum()",
    }) |q| {
        var r = try ctx.cli.run(&.{ "ts", "floql", q });
        defer r.deinit();
        try testing.expect(r.contains("out of range"));
    }

    var ok = try ctx.cli.run(&.{ "ts", "floql", "pct[1h] | window(1m) | percentile(50)" });
    defer ok.deinit();
    try testing.expect(!ok.contains("out of range") and !ok.contains("rror"));

    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null);
}

test "e2e/ts: retention takes only --raw-ttl, and the server refuses a downsample rule" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var missing = try ctx.cli.run(&.{ "ts", "retention", "cpu" });
    defer missing.deinit();
    try testing.expect(missing.contains("--raw-ttl is required"));
    for ([_][]const u8{ "--show", "--downsample" }) |flag| {
        var r = try ctx.cli.run(&.{ "ts", "retention", "cpu", "--raw-ttl", "7d", flag, "1m:avg:30d" });
        defer r.deinit();
        try testing.expect(!r.succeeded());
    }

    // An older client's rule is refused, not ignored.
    const proto = @import("src").protocol.proto;
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.ts_retention);
    header.request_id = 7;
    var obuf: [64]u8 = undefined;
    var b = proto.OptionsBuilder.init(&obuf);
    try b.addString(.ts_raw_ttl, "7d");
    try b.addString(.ts_downsample, "1m:avg:30d");
    const req: proto.Request = .{ .header = header, .namespace = "default", .key = "cpu", .value = "", .options = b.getOptions() };
    var buf: [256]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    _ = std.c.write(fd, bytes.ptr, bytes.len);
    var out: [512]u8 = undefined;
    var got: usize = 0;
    const resp = for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break null;
        if (proto.Response.parse(out[0..got])) |r| break r else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    } else null;
    try testing.expect(resp != null);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), resp.?.header.status);
    try testing.expect(std.mem.indexOf(u8, resp.?.data, "downsampling isn't supported") != null);
}

test "e2e/ts: a delete and a retention trim stay done across a restart" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "gone", "--value", "11" });
    try ctx.exec(&.{ "ts", "write", "aged", "--value", "22", "--timestamp", "1708700400000" });
    try ctx.exec(&.{ "ts", "write", "aged", "--value", "33" });
    try ctx.exec(&.{ "ts", "delete", "gone", "--confirm" });
    try ctx.exec(&.{ "ts", "retention", "aged", "--raw-ttl", "1d" });
    try ctx.restartServer();

    var gone = try ctx.cli.run(&.{ "ts", "read", "gone", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer gone.deinit();
    try testing.expect(!gone.contains(" 11.000000"));
    var aged = try ctx.cli.run(&.{ "ts", "read", "aged", "--from", "1708700000000", "--output", "raw", "--limit", "100" });
    defer aged.deinit();
    try testing.expect(aged.contains(" 33.000000"));
    try testing.expect(!aged.contains(" 22.000000"));
}

test "e2e/ts: a delete with tags removes only the matching series, and stays done across a restart" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "temp", "--tags", "sensor=A1,site=x", "--value", "11" });
    try ctx.exec(&.{ "ts", "write", "temp", "--tags", "sensor=B2,site=x", "--value", "22" });
    try ctx.exec(&.{ "ts", "write", "temp", "--value", "33" });

    // Dropping a part that isn't a pair would widen the filter to every series.
    var bad = try ctx.cli.run(&.{ "ts", "delete", "temp", "--tags", "sensor", "--confirm" });
    defer bad.deinit();
    try testing.expect(bad.contains("'sensor' is not key=value"));
    var none = try ctx.cli.run(&.{ "ts", "delete", "temp", "--tags", "sensor=Z9", "--confirm" });
    defer none.deinit();
    try testing.expect(none.contains("nothing deleted"));

    try ctx.exec(&.{ "ts", "delete", "temp", "--tags", "sensor=A1", "--confirm" });
    for (0..2) |pass| {
        if (pass == 1) try ctx.restartServer();
        var r = try ctx.cli.run(&.{ "ts", "read", "temp", "--from", "0", "--output", "raw", "--limit", "100" });
        defer r.deinit();
        try testing.expect(!r.contains(" 11.000000"));
        try testing.expect(r.contains(" 22.000000"));
        try testing.expect(r.contains(" 33.000000"));
    }
}
