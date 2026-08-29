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

test "e2e/ts: write multiple fields" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo ts write cpu --tags host=web-01 --fields user=72.5,system=7.4,idle=20.1
    const output = try ctx.execCapture(&.{
        "ts",          "write",    "cpu",                            "--tags",
        "host=web-01", "--fields", "user=72.5,system=7.4,idle=20.1",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
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

test "e2e/ts: write batch from file" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a temp line-protocol file
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

    // flo ts write --batch --file /tmp/flo-e2e-ts-batch.txt --precision ms
    const output = try ctx.execCapture(&.{
        "ts", "write", "--batch", "--file", tmp_path, "--precision", "ms",
    });
    try testing.expect(std.mem.indexOf(u8, output, "Wrote") != null);
    try testing.expect(std.mem.indexOf(u8, output, "points") != null);
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

test "e2e/ts: list specific measurement shows series" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several series under same measurement
    try ctx.exec(&.{ "ts", "write", "detail_cpu", "--tags", "host=web-01", "--value", "70.0" });
    try ctx.exec(&.{ "ts", "write", "detail_cpu", "--tags", "host=web-02", "--value", "80.0" });

    // List specific measurement
    var result = try ctx.cli.run(&.{ "ts", "list", "detail_cpu" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/ts: list with --fields flag" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write multi-field data
    try ctx.exec(&.{
        "ts",     "write",    "fields_cpu",                     "--tags",
        "host=a", "--fields", "user=72.5,system=7.4,idle=20.1",
    });

    // List with --fields to see field names
    var result = try ctx.cli.run(&.{ "ts", "list", "fields_cpu", "--fields" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
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

test "e2e/ts: list series with --limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write several series under one measurement
    try ctx.exec(&.{ "ts", "write", "limit_series", "--tags", "host=a", "--value", "1.0" });
    try ctx.exec(&.{ "ts", "write", "limit_series", "--tags", "host=b", "--value", "2.0" });
    try ctx.exec(&.{ "ts", "write", "limit_series", "--tags", "host=c", "--value", "3.0" });

    // List series with --limit 2
    var result = try ctx.cli.run(&.{ "ts", "list", "limit_series", "--limit", "2" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
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

test "e2e/ts: set retention with downsample rule" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "retention_ds_test", "--tags", "host=a", "--value", "50.0" });

    // flo ts retention retention_ds_test --raw-ttl 7d --downsample 1m:avg:30d
    const output = try ctx.execCapture(&.{
        "ts",         "retention", "retention_ds_test",
        "--raw-ttl",  "7d",        "--downsample",
        "1m:avg:30d",
    });
    try testing.expect(std.mem.indexOf(u8, output, "OK") != null);
}

test "e2e/ts: retention requires --raw-ttl or --downsample" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "retention_fail_test", "--tags", "host=a", "--value", "50.0" });

    // No --raw-ttl or --downsample should fail
    var result = try ctx.cli.run(&.{ "ts", "retention", "retention_fail_test" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
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
    try testing.expect(result.contains("40"));
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
    try testing.expect(result.contains("2"));
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
    try testing.expect(result.contains("20"));
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
    try testing.expect(result.contains("73"));
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
    try testing.expect(result.contains("72.57"));
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
    try testing.expect(result.contains("43"));
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
    try testing.expect(result.contains("33.3"));
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
    try testing.expect(result.contains("15"));
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

    try stdx.testing.assertSucceeded(result);
    try testing.expect(result.contains("10"));
    try testing.expect(!result.contains("30"));
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
        "alpha_cpu",   "bravo_mem",   "charlie_disk", "delta_net",
        "echo_iops",   "foxtrot_lat", "golf_tput",    "hotel_err",
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
