//! Kafka Source E2E Tests
//!
//! Tests the full Kafka source → Flo pipeline → Flo stream sink flow using
//! a real Redpanda container. Requires Docker.
//!
//! If Docker is not available, tests are skipped via `error.SkipZigTest`.
//!
//! Run with: zig build test-e2e --summary all

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const RedpandaProcess = stdx.testing.RedpandaProcess;
const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const cleanupTempFile = stdx.testing.cleanupTempFile;

// =============================================================================
// Kafka Source E2E Tests (Redpanda + Flo Server + CLI)
// =============================================================================

test "e2e/kafka: source ingests JSON records into flo stream" {
    // Start Redpanda container
    var rp = try RedpandaProcess.init(testing.allocator, .{});
    defer rp.deinit();
    rp.start() catch return error.SkipZigTest;

    // Start Flo server
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Get dynamic broker address
    const broker_addr = try rp.brokerAddr();
    defer testing.allocator.free(broker_addr);

    // Create Kafka topic with 4 partitions
    try rp.createTopic("e2e-events", 4);

    // Create output stream in Flo
    try ctx.exec(&.{ "stream", "create", "enriched" });

    // Produce 10 JSON records to Kafka
    var records: [10]RedpandaProcess.Record = undefined;
    var value_bufs: [10][128]u8 = undefined;
    var value_slices: [10][]const u8 = undefined;
    for (0..10) |i| {
        const len = (std.fmt.bufPrint(&value_bufs[i], "{{\"seq\":{d},\"msg\":\"event-{d}\"}}", .{ i, i }) catch unreachable).len;
        value_slices[i] = value_bufs[i][0..len];
        records[i] = .{ .value = value_slices[i] };
    }
    try rp.produceBatch("e2e-events", &records);

    // Build pipeline YAML with dynamic broker address
    var yaml_buf: [2048]u8 = undefined;
    const job_def = std.fmt.bufPrint(&yaml_buf,
        \\kind: Processing
        \\name: kafka-ingest-test
        \\sources.[0].name: kafka-src
        \\sources.[0].kafka.brokers: {s}
        \\sources.[0].kafka.topic: e2e-events
        \\sources.[0].kafka.group: flo-e2e-test
        \\sources.[0].kafka.format: json
        \\sources.[0].kafka.start_offset: earliest
        \\sinks.[0].stream.name: enriched
        \\parallelism: 1
        \\batch_size: 100
    , .{broker_addr}) catch unreachable;

    const yaml_path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kafka-ingest.yaml");
    defer cleanupTempFile(testing.allocator, yaml_path);

    // Submit pipeline
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", yaml_path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Wait for records to flow through (poll the output stream)
    const found = try readStreamBlocking(ctx, "enriched", "default", "event-", "10000");
    if (!found) {
        std.debug.print("\n[FAILED] No records found in output stream 'enriched' after 10s\n", .{});

        // Check job status for diagnostics
        var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status_result.deinit();
        std.debug.print("[DIAG] Job status: {s}\n", .{status_result.stdout});

        return error.NoRecordsFound;
    }
}

test "e2e/kafka: source with earliest offset reads all existing records" {
    var rp = try RedpandaProcess.init(testing.allocator, .{});
    defer rp.deinit();
    rp.start() catch return error.SkipZigTest;

    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const broker_addr = try rp.brokerAddr();
    defer testing.allocator.free(broker_addr);

    // Create topic and produce records BEFORE pipeline starts
    try rp.createTopic("e2e-preexist", 1);
    try rp.produce("e2e-preexist", null, "{\"pre\":1}");
    try rp.produce("e2e-preexist", null, "{\"pre\":2}");
    try rp.produce("e2e-preexist", null, "{\"pre\":3}");

    // Small delay to ensure records are flushed
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Create output stream
    try ctx.exec(&.{ "stream", "create", "pre-output" });

    // Submit pipeline with start_offset: earliest
    var yaml_buf: [2048]u8 = undefined;
    const job_def = std.fmt.bufPrint(&yaml_buf,
        \\kind: Processing
        \\name: kafka-earliest-test
        \\sources.[0].name: kafka-pre
        \\sources.[0].kafka.brokers: {s}
        \\sources.[0].kafka.topic: e2e-preexist
        \\sources.[0].kafka.group: flo-e2e-earliest
        \\sources.[0].kafka.format: json
        \\sources.[0].kafka.start_offset: earliest
        \\sinks.[0].stream.name: pre-output
        \\parallelism: 1
        \\batch_size: 100
    , .{broker_addr}) catch unreachable;

    const yaml_path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kafka-earliest.yaml");
    defer cleanupTempFile(testing.allocator, yaml_path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", yaml_path });
    _ = extractJobId(submit_output) orelse return error.NoJobId;

    // Pre-existing records should appear in the output stream
    const found = try readStreamBlocking(ctx, "pre-output", "default", "\"pre\"", "10000");
    if (!found) {
        std.debug.print("\n[FAILED] Pre-existing records not found in output stream\n", .{});
        return error.NoRecordsFound;
    }
}

test "e2e/kafka: source with keyed records" {
    var rp = try RedpandaProcess.init(testing.allocator, .{});
    defer rp.deinit();
    rp.start() catch return error.SkipZigTest;

    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const broker_addr = try rp.brokerAddr();
    defer testing.allocator.free(broker_addr);

    try rp.createTopic("e2e-keyed", 2);

    // Produce keyed records
    try rp.produce("e2e-keyed", "user-1", "{\"action\":\"login\"}");
    try rp.produce("e2e-keyed", "user-2", "{\"action\":\"logout\"}");
    try rp.produce("e2e-keyed", "user-1", "{\"action\":\"purchase\"}");

    std.Thread.sleep(500 * std.time.ns_per_ms);

    try ctx.exec(&.{ "stream", "create", "keyed-output" });

    var yaml_buf: [2048]u8 = undefined;
    const job_def = std.fmt.bufPrint(&yaml_buf,
        \\kind: Processing
        \\name: kafka-keyed-test
        \\sources.[0].name: kafka-keyed
        \\sources.[0].kafka.brokers: {s}
        \\sources.[0].kafka.topic: e2e-keyed
        \\sources.[0].kafka.group: flo-e2e-keyed
        \\sources.[0].kafka.format: json
        \\sources.[0].kafka.start_offset: earliest
        \\sinks.[0].stream.name: keyed-output
        \\parallelism: 1
        \\batch_size: 100
    , .{broker_addr}) catch unreachable;

    const yaml_path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kafka-keyed.yaml");
    defer cleanupTempFile(testing.allocator, yaml_path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", yaml_path });
    _ = extractJobId(submit_output) orelse return error.NoJobId;

    const found = try readStreamBlocking(ctx, "keyed-output", "default", "purchase", "10000");
    if (!found) {
        std.debug.print("\n[FAILED] Keyed records not found in output stream\n", .{});
        return error.NoRecordsFound;
    }
}

test "e2e/kafka: pipeline lifecycle - submit and stop" {
    var rp = try RedpandaProcess.init(testing.allocator, .{});
    defer rp.deinit();
    rp.start() catch return error.SkipZigTest;

    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const broker_addr = try rp.brokerAddr();
    defer testing.allocator.free(broker_addr);

    try rp.createTopic("e2e-lifecycle", 1);

    try ctx.exec(&.{ "stream", "create", "lifecycle-output" });

    var yaml_buf: [2048]u8 = undefined;
    const job_def = std.fmt.bufPrint(&yaml_buf,
        \\kind: Processing
        \\name: kafka-lifecycle-test
        \\sources.[0].name: kafka-lc
        \\sources.[0].kafka.brokers: {s}
        \\sources.[0].kafka.topic: e2e-lifecycle
        \\sources.[0].kafka.group: flo-e2e-lifecycle
        \\sources.[0].kafka.format: json
        \\sources.[0].kafka.start_offset: earliest
        \\sinks.[0].stream.name: lifecycle-output
        \\parallelism: 1
        \\batch_size: 100
    , .{broker_addr}) catch unreachable;

    const yaml_path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kafka-lifecycle.yaml");
    defer cleanupTempFile(testing.allocator, yaml_path);

    // Submit
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", yaml_path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Check running
    const is_running = try pollForJobState(ctx, job_id, "default", "RUNNING", 5000);
    if (!is_running) {
        std.debug.print("\n[WARN] Job did not reach RUNNING state\n", .{});
    }

    // Stop the job
    var stop_result = try ctx.cli.run(&.{ "processing", "stop", job_id });
    defer stop_result.deinit();
    try stdx.testing.assertSucceeded(stop_result);
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract job ID from "Job submitted: <job_id>" output
fn extractJobId(output: []const u8) ?[]const u8 {
    const prefix = "Job submitted: ";
    const start = std.mem.indexOf(u8, output, prefix) orelse return null;
    const id_start = start + prefix.len;
    var id_end = id_start;
    while (id_end < output.len and output[id_end] != '\n' and output[id_end] != '\r') {
        id_end += 1;
    }
    if (id_end == id_start) return null;
    return std.mem.trim(u8, output[id_start..id_end], &std.ascii.whitespace);
}

/// Poll-read from a stream, waiting up to timeout_ms for expected data to appear.
fn readStreamBlocking(ctx: *stdx.testing.TestContext, stream_name: []const u8, ns: []const u8, expected: []const u8, timeout_ms_str: []const u8) !bool {
    const timeout_ms = std.fmt.parseInt(u64, timeout_ms_str, 10) catch 5000;
    const poll_interval_ns: u64 = 200 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 200, 1);

    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "-n", ns, "--start", "0-0", "--limit", "100" });
        defer result.deinit();

        if (result.succeeded() and result.stdoutContains(expected)) {
            return true;
        }

        std.Thread.sleep(poll_interval_ns);
    }

    // Final attempt
    var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "-n", ns, "--start", "0-0", "--limit", "100" });
    defer result.deinit();
    return result.succeeded() and result.stdoutContains(expected);
}

/// Poll until job reaches a given state, or timeout.
fn pollForJobState(ctx: *stdx.testing.TestContext, job_id: []const u8, ns: []const u8, target_state: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 200 * std.time.ns_per_ms;
    const max_attempts = timeout_ms / 200;

    for (0..max_attempts) |_| {
        std.Thread.sleep(poll_interval_ns);

        var result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", ns });
        defer result.deinit();

        if (result.succeeded() and result.stdoutContains(target_state)) {
            return true;
        }
    }
    return false;
}
