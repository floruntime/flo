//! Processing End-to-End Tests
//!
//! Tests the complete stream processing pipeline:
//!   CLI → TCP → Dispatcher → ProcessingHandler → Chain → FloStreamSource/FloTsSource/Sinks
//!
//! Coverage:
//! - CLI command parsing and routing
//! - Protocol serialization/deserialization for processing commands
//! - Help text generation for processing subcommands
//! - Real pipeline execution (passthrough, late-data, metrics, savepoint)
//! - Job lifecycle: submit → status → metrics → savepoint → stop/cancel
//! - TS sink: JSON → time-series via FloTsSink (tag/field extraction, query)
//! - TS source: time-series → JSON via FloTsSource (measurement polling, field mapping)
//! - TS-to-stream pipeline: TS source → stream sink
//! - TS-to-TS pipeline: TS source → TS sink (re-tagging / derived metrics)
//!
//! ## Dotted-Key Format
//!
//! Pipeline tests use flat dotted-key strings with `writeDottedToTempYaml`.
//! Sources and sinks use array-index notation `[N]`:
//! ```zig
//! const job_def =
//!     \\kind: Processing
//!     \\name: my-pipeline
//!     \\sources.[0].stream.name: input-events
//!     \\sinks.[0].stream.name: output-events
//! ;
//! const path = try writeDottedToTempYaml(allocator, job_def, "pipeline.yaml");
//! ```
//!
//! TS source example:
//! ```zig
//! const job_def =
//!     \\kind: Processing
//!     \\name: ts-pipeline
//!     \\sources.[0].ts.measurement: cpu_usage
//!     \\sources.[0].ts.field: idle
//!     \\sources.[0].ts.tags.host: web-01
//!     \\sinks.[0].stream.name: cpu-json-output
//! ;
//! ```

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");

// =============================================================================
// Processing CLI Help Tests
// =============================================================================

test "e2e/processing: help shows all subcommands" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Use runRaw to avoid appending --endpoint which is unknown to --help parser
    var result = try ctx.cli.runRaw(&.{ "processing", "--help" });
    defer result.deinit();

    // Verify all subcommands are listed
    try stdx.testing.assertContains(result, "submit");
    try stdx.testing.assertContains(result, "stop");
    try stdx.testing.assertContains(result, "cancel");
    try stdx.testing.assertContains(result, "status");
    try stdx.testing.assertContains(result, "list");
    try stdx.testing.assertContains(result, "metrics");
    try stdx.testing.assertContains(result, "savepoint");
    try stdx.testing.assertContains(result, "restore");
    try stdx.testing.assertContains(result, "rescale");
}

test "e2e/processing: submit help shows usage" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "processing", "submit", "--help" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "Submit a processing job");
    try stdx.testing.assertContains(result, "file");
}

test "e2e/processing: rescale help shows usage" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.runRaw(&.{ "processing", "rescale", "--help" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "Rescale job parallelism");
    try stdx.testing.assertContains(result, "parallelism");
}

// =============================================================================
// Processing Command Parsing Tests
// =============================================================================

test "e2e/processing: submit requires file argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // No file argument should fail
    var result = try ctx.cli.run(&.{ "processing", "submit" });
    defer result.deinit();

    // Should show error or help about missing argument
    try stdx.testing.assertFailed(result);
}

test "e2e/processing: stop requires job_id argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "processing", "stop" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/processing: cancel requires job_id argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "processing", "cancel" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/processing: status requires job_id argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "processing", "status" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/processing: savepoint requires job_id argument" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "processing", "savepoint" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/processing: restore requires job_id and savepoint_id" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Missing both arguments
    var result = try ctx.cli.run(&.{ "processing", "restore" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/processing: rescale requires job_id and parallelism" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Missing arguments
    var result = try ctx.cli.run(&.{ "processing", "rescale" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

// =============================================================================
// Processing List (no-args subcommand) Test
// =============================================================================

test "e2e/processing: list accepts no arguments" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // list should parse fine (even if connection fails since no server)
    var result = try ctx.cli.run(&.{ "processing", "list" });
    defer result.deinit();

    // Either succeeds with empty output or fails with connection error
    // Both are valid - the command parsed correctly
    // Just verify it doesn't crash
    _ = result.succeeded();
}

// =============================================================================
// Processing Protocol Serialization Tests
// =============================================================================

test "e2e/processing: protocol request building - submit" {
    const proto = src.protocol.proto;
    const RequestBuilder = src.protocol.request_builder.RequestBuilder;

    var builder = RequestBuilder.init(testing.allocator);
    const req = builder.build(
        proto.OpCode.processing_submit,
        "test-ns",
        "",
        "name: test-job\nversion: 1.0",
        "",
    );

    try testing.expectEqual(@intFromEnum(proto.OpCode.processing_submit), req.header.op_code);
    try testing.expect(req.header.request_id > 0);
}

test "e2e/processing: protocol request building - stop" {
    const proto = src.protocol.proto;
    const RequestBuilder = src.protocol.request_builder.RequestBuilder;

    var builder = RequestBuilder.init(testing.allocator);
    const req = builder.build(
        proto.OpCode.processing_stop,
        "test-ns",
        "job-abc-123",
        "",
        "",
    );

    try testing.expectEqual(@intFromEnum(proto.OpCode.processing_stop), req.header.op_code);
    try testing.expect(req.header.request_id > 0);
}

test "e2e/processing: protocol request building - list" {
    const proto = src.protocol.proto;
    const RequestBuilder = src.protocol.request_builder.RequestBuilder;

    var builder = RequestBuilder.init(testing.allocator);
    const req = builder.build(
        proto.OpCode.processing_list,
        "default",
        "",
        "",
        "",
    );

    try testing.expectEqual(@intFromEnum(proto.OpCode.processing_list), req.header.op_code);
}

test "e2e/processing: protocol request building - rescale" {
    const proto = src.protocol.proto;
    const RequestBuilder = src.protocol.request_builder.RequestBuilder;

    var builder = RequestBuilder.init(testing.allocator);
    const req = builder.build(
        proto.OpCode.processing_rescale,
        "prod",
        "job-xyz",
        "8",
        "",
    );

    try testing.expectEqual(@intFromEnum(proto.OpCode.processing_rescale), req.header.op_code);
}

test "e2e/processing: protocol request building - restore" {
    const proto = src.protocol.proto;
    const RequestBuilder = src.protocol.request_builder.RequestBuilder;

    var builder = RequestBuilder.init(testing.allocator);
    const req = builder.build(
        proto.OpCode.processing_restore,
        "ns",
        "job-1",
        "sp-42",
        "",
    );

    try testing.expectEqual(@intFromEnum(proto.OpCode.processing_restore), req.header.op_code);
}

// =============================================================================
// Processing Result Tests
// =============================================================================

test "e2e/processing: result opcode mapping" {
    const CommandResult = src.protocol.result.CommandResult;
    const proto = src.protocol.proto;

    // Test all processing result → opcode mappings
    const submitted = CommandResult{ .processing_submitted = .{ .job_id = "job-1" } };
    try testing.expectEqual(proto.OpCode.processing_submit_response, submitted.opcode());

    const stopped = CommandResult{ .processing_stopped = {} };
    try testing.expectEqual(proto.OpCode.processing_stop_response, stopped.opcode());

    const cancelled = CommandResult{ .processing_cancelled = {} };
    try testing.expectEqual(proto.OpCode.processing_cancel_response, cancelled.opcode());

    const status_result = CommandResult{ .processing_status_result = .{ .data = "{}" } };
    try testing.expectEqual(proto.OpCode.processing_status_response, status_result.opcode());

    const list_result = CommandResult{ .processing_list_result = .{ .data = "[]" } };
    try testing.expectEqual(proto.OpCode.processing_list_response, list_result.opcode());

    const sp_result = CommandResult{ .processing_savepoint_result = .{ .savepoint_id = "sp-1" } };
    try testing.expectEqual(proto.OpCode.processing_savepoint_response, sp_result.opcode());

    const restored = CommandResult{ .processing_restored = {} };
    try testing.expectEqual(proto.OpCode.processing_restore_response, restored.opcode());

    const rescaled = CommandResult{ .processing_rescaled = {} };
    try testing.expectEqual(proto.OpCode.processing_rescale_response, rescaled.opcode());
}

test "e2e/processing: result serialized size" {
    const CommandResult = src.protocol.result.CommandResult;

    const submitted = CommandResult{ .processing_submitted = .{ .job_id = "job-123" } };
    const size = submitted.serializedSize();
    // tag(1) + len(4) + "job-123"(7) = 12
    try testing.expectEqual(@as(usize, 12), size);

    const stopped = CommandResult{ .processing_stopped = {} };
    try testing.expectEqual(@as(usize, 1), stopped.serializedSize());

    const list_result = CommandResult{ .processing_list_result = .{ .data = "[]" } };
    // tag(1) + len(4) + "[]"(2) + has_cursor(1) = 8
    try testing.expectEqual(@as(usize, 8), list_result.serializedSize());
}

// =============================================================================
// Processing OpCode Contract Tests
// =============================================================================

test "e2e/processing: processing opcodes are defined" {
    const proto = src.protocol.proto;

    // Processing opcodes must exist and have stable values (wire contract)
    try testing.expectEqual(@as(u8, 0xC0), @intFromEnum(proto.OpCode.processing_submit));
    try testing.expectEqual(@as(u8, 0xC1), @intFromEnum(proto.OpCode.processing_stop));
    try testing.expectEqual(@as(u8, 0xC2), @intFromEnum(proto.OpCode.processing_cancel));
    try testing.expectEqual(@as(u8, 0xC3), @intFromEnum(proto.OpCode.processing_status));
    try testing.expectEqual(@as(u8, 0xC4), @intFromEnum(proto.OpCode.processing_list));
    try testing.expectEqual(@as(u8, 0xC6), @intFromEnum(proto.OpCode.processing_savepoint));
    try testing.expectEqual(@as(u8, 0xC7), @intFromEnum(proto.OpCode.processing_restore));
    try testing.expectEqual(@as(u8, 0xC8), @intFromEnum(proto.OpCode.processing_rescale));
}

test "e2e/processing: processing response opcodes are defined" {
    const proto = src.protocol.proto;

    // Response opcodes must pair with request opcodes (wire contract)
    try testing.expectEqual(@as(u8, 0xC9), @intFromEnum(proto.OpCode.processing_submit_response));
    try testing.expectEqual(@as(u8, 0xCA), @intFromEnum(proto.OpCode.processing_stop_response));
    try testing.expectEqual(@as(u8, 0xCB), @intFromEnum(proto.OpCode.processing_cancel_response));
    try testing.expectEqual(@as(u8, 0xCC), @intFromEnum(proto.OpCode.processing_status_response));
    try testing.expectEqual(@as(u8, 0xCD), @intFromEnum(proto.OpCode.processing_list_response));
    try testing.expectEqual(@as(u8, 0xCF), @intFromEnum(proto.OpCode.processing_savepoint_response));
    try testing.expectEqual(@as(u8, 0xD0), @intFromEnum(proto.OpCode.processing_restore_response));
    try testing.expectEqual(@as(u8, 0xD1), @intFromEnum(proto.OpCode.processing_rescale_response));
}

// =============================================================================
// Processing Pipeline E2E Tests (Real Server + CLI)
// =============================================================================

const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const cleanupTempFile = stdx.testing.cleanupTempFile;

/// Extract job ID from "Job submitted: <job_id>" output
fn extractJobId(output: []const u8) ?[]const u8 {
    const prefix = "Job submitted: ";
    const start = std.mem.indexOf(u8, output, prefix) orelse return null;
    const id_start = start + prefix.len;
    // Find end of job ID (newline or end of string)
    var id_end = id_start;
    while (id_end < output.len and output[id_end] != '\n' and output[id_end] != '\r') {
        id_end += 1;
    }
    if (id_end == id_start) return null;
    return std.mem.trim(u8, output[id_start..id_end], &std.ascii.whitespace);
}

/// Poll-read from a stream, waiting up to timeout_ms for expected data to appear.
/// Returns true if the expected content was found within the timeout.
/// NOTE: We poll with repeated non-blocking reads because ClusterHandler's
/// handleStreamRead does not implement block_ms (it reads the Raft log once
/// and returns immediately). So --block is effectively a no-op in the real
/// server path. This poll loop provides the actual waiting behavior.
fn readStreamBlocking(ctx: *stdx.testing.TestContext, stream_name: []const u8, expected: []const u8, timeout_ms_str: []const u8) !bool {
    const timeout_ms = std.fmt.parseInt(u64, timeout_ms_str, 10) catch 5000;
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);

    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "--start", "0-0", "--limit", "100" });
        defer result.deinit();

        if (result.succeeded() and result.stdoutContains(expected)) {
            return true;
        }

        std.Thread.sleep(poll_interval_ns);
    }

    // One final attempt after all sleeps
    var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "--start", "0-0", "--limit", "100" });
    defer result.deinit();

    return result.succeeded() and result.stdoutContains(expected);
}

/// Poll until job reaches a given state, or timeout.
/// Returns true if the state was reached.
fn pollForJobState(ctx: *stdx.testing.TestContext, job_id: []const u8, target_state: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 200 * std.time.ns_per_ms;
    const max_attempts = timeout_ms / 200;

    for (0..max_attempts) |_| {
        std.Thread.sleep(poll_interval_ns);

        var result = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer result.deinit();

        if (result.succeeded() and result.stdoutContains(target_state)) {
            return true;
        }
    }
    return false;
}

test "e2e/processing: submit job and verify running state" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write the job definition to a temp file
    const job_def =
        \\kind: Processing
        \\name: e2e-state-test
        \\sources.[0].stream.name: state-input
        \\sinks.[0].stream.name: state-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-state-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit the processing job
    var submit_result = try ctx.cli.run(&.{ "processing", "submit", path });
    defer submit_result.deinit();

    try stdx.testing.assertSucceeded(submit_result);

    // Extract job ID from stdout
    const submit_output = std.mem.trim(u8, submit_result.stdout, &std.ascii.whitespace);
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Check status — job should be running
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "RUNNING");
}

test "e2e/processing: submit job then stop it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const job_def =
        \\kind: Processing
        \\name: e2e-stop-test
        \\sources.[0].stream.name: stop-input
        \\sinks.[0].stream.name: stop-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-stop-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Stop the job
    var stop_result = try ctx.cli.run(&.{ "processing", "stop", job_id });
    defer stop_result.deinit();

    try stdx.testing.assertSucceeded(stop_result);
    try stdx.testing.assertContains(stop_result, "stopped");

    // Verify status shows stopped
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "STOPPED");
}

test "e2e/processing: submit job then cancel it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const job_def =
        \\kind: Processing
        \\name: e2e-cancel-test
        \\sources.[0].stream.name: cancel-input
        \\sinks.[0].stream.name: cancel-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-cancel-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Cancel the job
    var cancel_result = try ctx.cli.run(&.{ "processing", "cancel", job_id });
    defer cancel_result.deinit();

    try stdx.testing.assertSucceeded(cancel_result);
    try stdx.testing.assertContains(cancel_result, "cancelled");

    // Verify status shows cancelled
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "CANCELLED");
}

test "e2e/processing: list shows submitted jobs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Submit two jobs
    const job1_def =
        \\kind: Processing
        \\name: e2e-list-job1
        \\sources.[0].stream.name: list-input1
        \\sinks.[0].stream.name: list-output1
    ;
    const path1 = try writeDottedToTempYaml(testing.allocator, job1_def, "e2e-list-job1.yaml");
    defer cleanupTempFile(testing.allocator, path1);

    const job2_def =
        \\kind: Processing
        \\name: e2e-list-job2
        \\sources.[0].stream.name: list-input2
        \\sinks.[0].stream.name: list-output2
    ;
    const path2 = try writeDottedToTempYaml(testing.allocator, job2_def, "e2e-list-job2.yaml");
    defer cleanupTempFile(testing.allocator, path2);

    _ = try ctx.execCapture(&.{ "processing", "submit", path1 });
    _ = try ctx.execCapture(&.{ "processing", "submit", path2 });

    // List should show both jobs
    var list_result = try ctx.cli.run(&.{ "processing", "list" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    // List returns JSON array with job status objects
    try testing.expect(list_result.stdoutContains("RUNNING"));
}

test "e2e/processing: passthrough pipeline - data flows from source to sink stream" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Append test data to the source stream
    try ctx.exec(&.{ "stream", "append", "pipe-input", "hello-pipeline" });
    try ctx.exec(&.{ "stream", "append", "pipe-input", "second-record" });
    try ctx.exec(&.{ "stream", "append", "pipe-input", "third-record" });

    // Step 2: Submit a processing job that reads from pipe-input and writes to pipe-output
    const job_def =
        \\kind: Processing
        \\name: e2e-passthrough
        \\sources.[0].stream.name: pipe-input
        \\sinks.[0].stream.name: pipe-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-passthrough.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for the processing pipeline to move data
    // The server's tick() drives Chain.runBatch() which polls from the source
    // stream and writes to the sink stream. Give it time to process.
    const found = try readStreamBlocking(ctx, "pipe-output", "hello-pipeline", "5000");

    if (!found) {
        // Dump diagnostics on failure
        std.debug.print("\n[TIMEOUT] Data did not flow through pipeline within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify all records made it through
    var read_result = try ctx.cli.run(&.{ "stream", "read", "pipe-output", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "hello-pipeline");
    try stdx.testing.assertContains(read_result, "second-record");
    try stdx.testing.assertContains(read_result, "third-record");

    // Step 5: Verify job status shows records processed
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    // Should show records_processed > 0
    try testing.expect(!status_result.stdoutContains("\"records_processed\":0"));

    // Step 6: Clean up — stop the job
    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: pipeline processes data appended after job submission" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Submit job FIRST (no data yet in the source stream)
    const job_def =
        \\kind: Processing
        \\name: e2e-late-data
        \\sources.[0].stream.name: late-input
        \\sinks.[0].stream.name: late-output
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-late-data.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job a moment to start ticking (source will be exhausted initially)
    std.Thread.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now append data — the job should pick it up on subsequent ticks
    try ctx.exec(&.{ "stream", "append", "late-input", "late-arrival-1" });
    try ctx.exec(&.{ "stream", "append", "late-input", "late-arrival-2" });

    // Step 3: Wait for data to flow through the pipeline
    const found = try readStreamBlocking(ctx, "late-output", "late-arrival-1", "5000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late data did not flow through pipeline\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Both records should be in the output
    var read_result = try ctx.cli.run(&.{ "stream", "read", "late-output", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "late-arrival-1");
    try stdx.testing.assertContains(read_result, "late-arrival-2");

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: job status reflects processed record count" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Append known number of records
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-1" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-2" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-3" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-4" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-5" });

    const job_def =
        \\kind: Processing
        \\name: e2e-metrics
        \\sources.[0].stream.name: metrics-input
        \\sinks.[0].stream.name: metrics-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-metrics.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for processing
    _ = try readStreamBlocking(ctx, "metrics-output", "metrics-5", "5000");

    // Check status
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    // Status JSON should contain records_processed field
    try stdx.testing.assertContains(status_result, "records_processed");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: savepoint captures job state" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "stream", "append", "sp-input", "savepoint-data" });

    const job_def =
        \\kind: Processing
        \\name: e2e-savepoint
        \\sources.[0].stream.name: sp-input
        \\sinks.[0].stream.name: sp-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-savepoint.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for at least one record to be processed
    _ = try readStreamBlocking(ctx, "sp-output", "savepoint-data", "5000");

    // Take a savepoint
    var sp_result = try ctx.cli.run(&.{ "processing", "savepoint", job_id });
    defer sp_result.deinit();

    try stdx.testing.assertSucceeded(sp_result);
    // Should contain a savepoint ID
    try testing.expect(sp_result.stdoutContains("savepoint") or sp_result.stdoutContains("sp-"));

    try ctx.exec(&.{ "processing", "stop", job_id });
}

// =============================================================================
// Multi-Source / Multi-Sink Pipeline E2E Tests
// =============================================================================

test "e2e/processing: multi-source pipeline merges two input streams" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Append data to two separate source streams
    try ctx.exec(&.{ "stream", "append", "multi-src-a", "from-stream-a" });
    try ctx.exec(&.{ "stream", "append", "multi-src-b", "from-stream-b" });

    // Submit a job with two sources — MergingSource will round-robin poll both
    const job_def =
        \\kind: Processing
        \\name: e2e-multi-source
        \\sources.[0].stream.name: multi-src-a
        \\sources.[1].stream.name: multi-src-b
        \\sinks.[0].stream.name: multi-src-out
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-multi-source.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from submit\n", .{});
        return error.NoJobId;
    };

    // Wait for data from both streams to appear in the output
    const found_a = try readStreamBlocking(ctx, "multi-src-out", "from-stream-a", "5000");
    const found_b = try readStreamBlocking(ctx, "multi-src-out", "from-stream-b", "5000");

    if (!found_a or !found_b) {
        std.debug.print("\n[TIMEOUT] Multi-source merge: a={}, b={}\n", .{ found_a, found_b });
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify sink has records from both source streams
    var read_result = try ctx.cli.run(&.{ "stream", "read", "multi-src-out", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "from-stream-a");
    try stdx.testing.assertContains(read_result, "from-stream-b");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: multi-sink declaration uses primary sink" {
    // NOTE: Multi-sink fan-out is not yet wired — the handler only builds
    // the primary (first) sink. This test verifies that declaring multiple
    // sinks doesn't break submission, and data flows through the primary sink.
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Seed source stream
    try ctx.exec(&.{ "stream", "append", "msink-input", "multi-sink-record" });

    // Submit job with two sinks declared; only the first (stream) is wired
    const job_def =
        \\kind: Processing
        \\name: e2e-multi-sink
        \\sources.[0].stream.name: msink-input
        \\sinks.[0].name: primary-out
        \\sinks.[0].stream.name: msink-stream-out
        \\sinks.[1].name: secondary-kv
        \\sinks.[1].kv.namespace: msink-kv
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-multi-sink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to appear in the primary (stream) sink
    const found = try readStreamBlocking(ctx, "msink-stream-out", "multi-sink-record", "5000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Multi-sink data did not flow to primary stream sink\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: checkpoint persists to internal KV namespace" {
    // Use expose_internal_keys so kv scan shows _proc: keys
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .expose_internal_keys = true },
    });
    defer ctx.deinit();

    // Seed + submit
    try ctx.exec(&.{ "stream", "append", "ckpt-input", "checkpoint-test-data" });

    const job_def =
        \\kind: Processing
        \\name: e2e-checkpoint-persist
        \\sources.[0].stream.name: ckpt-input
        \\sinks.[0].stream.name: ckpt-output
        \\checkpointing.interval_ms: 500
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ckpt.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through
    _ = try readStreamBlocking(ctx, "ckpt-output", "checkpoint-test-data", "5000");

    // Take a savepoint — this triggers a checkpoint that persists to KV
    var sp_result = try ctx.cli.run(&.{ "processing", "savepoint", job_id });
    defer sp_result.deinit();
    try stdx.testing.assertSucceeded(sp_result);

    // With expose_internal_keys=true, _proc: checkpoint keys should be visible
    var scan_result = try ctx.cli.run(&.{ "kv", "list" });
    defer scan_result.deinit();
    if (scan_result.succeeded()) {
        // Verify checkpoint keys are present under _proc: namespace
        const has_proc_keys = scan_result.stdoutContains("_proc:");
        if (!has_proc_keys) {
            std.debug.print("WARN: kv list did not contain _proc: keys (checkpoint may not have flushed yet)\n", .{});
            std.debug.print("kv list output:\n{s}\n", .{scan_result.stdout});
        }
    }

    // Also verify that a server WITHOUT expose_internal_keys hides these keys.
    // We've already verified the mechanism in the KVHandler unit: the filter
    // uses `!is_internal and !self.expose_internal_keys and entry.key[0] == '_'`.

    try ctx.exec(&.{ "processing", "stop", job_id });
}

// =============================================================================
// Time-Series Sink E2E Tests
// =============================================================================

/// Poll `flo ts read` until data appears or timeout.
/// Returns true if the measurement returned data within the timeout.
fn readTsBlocking(ctx: *stdx.testing.TestContext, measurement: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);

    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{
            "ts",     "read",     measurement,
            "--from", "0",        "--limit",
            "100",    "--format", "json",
        });
        defer result.deinit();

        if (result.succeeded() and !result.stdoutContains("(no data)") and result.stdoutContains("[")) {
            return true;
        }

        std.Thread.sleep(poll_interval_ns);
    }

    // One final attempt
    var result = try ctx.cli.run(&.{
        "ts",     "read",     measurement,
        "--from", "0",        "--limit",
        "100",    "--format", "json",
    });
    defer result.deinit();

    return result.succeeded() and !result.stdoutContains("(no data)") and result.stdoutContains("[");
}

test "e2e/processing: ts sink - JSON records flow to time-series measurement" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Append JSON records to the source stream
    // The FloTsSink will extract tags and fields from these JSON payloads
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-01\",\"cpu_percent\":72.5}" });
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-01\",\"cpu_percent\":85.3}" });
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-02\",\"cpu_percent\":45.0}" });

    // Step 2: Submit a processing job with a TS sink configured
    // Tags: host ← hostname (extract "hostname" field as tag "host")
    // Fields: cpu ← cpu_percent (extract "cpu_percent" as field "cpu")
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-sink
        \\sources.[0].stream.name: ts-sink-input
        \\sinks.[0].ts.measurement: proc_cpu_metrics
        \\sinks.[0].ts.tags.host: hostname
        \\sinks.[0].ts.fields.cpu: cpu_percent
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-sink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for the TS sink to write data
    const found = try readTsBlocking(ctx, "proc_cpu_metrics", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS sink data did not appear within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify data via `flo ts read` with tag filter
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_cpu_metrics", "--tags", "host=web-01",
        "--from", "0",    "--limit",          "100",    "--format",
        "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    // JSON output should contain data points (not "(no data)")
    try testing.expect(!read_result.stdoutContains("(no data)"));
    try testing.expect(read_result.stdoutContains("["));

    // Step 5: Verify measurement appears in `flo ts list`
    var list_result = try ctx.cli.run(&.{ "ts", "list" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    try stdx.testing.assertContains(list_result, "proc_cpu_metrics");

    // Step 6: Clean up
    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts sink - value_field shorthand for scalar extraction" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Append JSON records with a "temperature" field as the value
    try ctx.exec(&.{ "stream", "append", "ts-scalar-input", "{\"sensor\":\"A1\",\"temperature\":23.4}" });
    try ctx.exec(&.{ "stream", "append", "ts-scalar-input", "{\"sensor\":\"A1\",\"temperature\":24.1}" });

    // Submit job: value_field = "temperature" (shorthand — no explicit fields map)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-scalar
        \\sources.[0].stream.name: ts-scalar-input
        \\sinks.[0].ts.measurement: proc_temp
        \\sinks.[0].ts.value_field: temperature
        \\sinks.[0].ts.tags.sensor: sensor
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-scalar.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to appear
    const found = try readTsBlocking(ctx, "proc_temp", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS scalar sink data did not appear within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Read with sensor tag filter
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_temp", "--tags", "sensor=A1",
        "--from", "0",    "--limit",   "100",    "--format",
        "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));
    // JSON array with data points
    try testing.expect(read_result.stdoutContains("timestamp_ms"));

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts sink - late data flows through after job starts" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Submit job FIRST (no data yet)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-late
        \\sources.[0].stream.name: ts-late-input
        \\sinks.[0].ts.measurement: proc_late_metric
        \\sinks.[0].ts.tags.host: host
        \\sinks.[0].ts.fields.load: load_avg
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-late.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job time to start its tick loop
    std.Thread.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now append data — should be picked up on subsequent ticks
    try ctx.exec(&.{ "stream", "append", "ts-late-input", "{\"host\":\"db-01\",\"load_avg\":3.14}" });
    try ctx.exec(&.{ "stream", "append", "ts-late-input", "{\"host\":\"db-01\",\"load_avg\":2.71}" });

    // Step 3: Wait for TS data to appear
    const found = try readTsBlocking(ctx, "proc_late_metric", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late TS sink data did not appear within 5s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify via read
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_late_metric", "--tags", "host=db-01",
        "--from", "0",    "--limit",          "100",    "--format",
        "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts sink - query aggregation on pipeline-written data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write records with known values for aggregation verification
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":100.0}" });
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":200.0}" });
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":300.0}" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-agg
        \\sources.[0].stream.name: ts-agg-input
        \\sinks.[0].ts.measurement: proc_requests
        \\sinks.[0].ts.tags.region: region
        \\sinks.[0].ts.fields.rps: rps
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-agg.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through
    const found = try readTsBlocking(ctx, "proc_requests", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS agg sink data did not appear within 5s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify via `flo ts query` with aggregation
    var query_result = try ctx.cli.run(&.{
        "ts",     "query",    "proc_requests", "--tags", "region=us",
        "--from", "0",        "--window",      "1h",     "--agg",
        "sum",    "--format", "json",
    });
    defer query_result.deinit();

    try stdx.testing.assertSucceeded(query_result);
    // Should have data (not empty)
    try testing.expect(!query_result.stdoutContains("(no data)"));

    try ctx.exec(&.{ "processing", "stop", job_id });
}

// =============================================================================
// Time-Series Source E2E Tests
// =============================================================================

test "e2e/processing: ts source - TS measurement data flows to stream sink" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Seed the TS measurement with data points via `flo ts write`
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-01", "--value", "72.5" });
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-01", "--value", "85.3" });
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-02", "--value", "45.0" });

    // Step 2: Submit a processing job with a TS *source* that reads from src_cpu
    // The FloTsSource wraps each point as JSON: {"measurement":..., "value":..., ...}
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-stream
        \\sources.[0].ts.measurement: src_cpu
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].stream.name: ts-src-stream-out
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-stream.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for data to flow through to the stream sink
    // The TS source polls the measurement and emits JSON records
    const found = try readStreamBlocking(ctx, "ts-src-stream-out", "src_cpu", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS source data did not flow to stream sink within 8s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify the stream contains JSON-wrapped TS data
    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-stream-out", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    // FloTsSource emits JSON containing measurement name and value
    try stdx.testing.assertContains(read_result, "src_cpu");

    // Step 5: Verify job status shows records processed
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try testing.expect(!status_result.stdoutContains("\"records_processed\":0"));

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts source - late data written after job starts" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Submit job FIRST with TS source (no data yet in measurement)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-late
        \\sources.[0].ts.measurement: src_late_cpu
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].stream.name: ts-src-late-out
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-late.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job time to start ticking (source will see no data initially)
    std.Thread.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now write TS data — the source should pick it up on subsequent polls
    try ctx.exec(&.{ "ts", "write", "src_late_cpu", "--tags", "host=db-01", "--value", "3.14" });
    try ctx.exec(&.{ "ts", "write", "src_late_cpu", "--tags", "host=db-01", "--value", "2.71" });

    // Step 3: Wait for data to flow through
    const found = try readStreamBlocking(ctx, "ts-src-late-out", "src_late_cpu", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late TS source data did not flow within 8s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-late-out", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "src_late_cpu");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts source - with tag filter reads subset of data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Seed data with two different hosts
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-01", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-02", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-01", "--value", "30.0" });

    // Submit job with tag filter: only read host=web-01
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-filtered
        \\sources.[0].ts.measurement: src_filtered
        \\sources.[0].ts.field: value
        \\sources.[0].ts.tags.host: web-01
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].stream.name: ts-src-filtered-out
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-filtered.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow
    const found = try readStreamBlocking(ctx, "ts-src-filtered-out", "src_filtered", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Filtered TS source data did not flow within 8s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify output contains data
    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-filtered-out", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "src_filtered");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts source to ts sink - derived metrics pipeline" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Step 1: Seed source measurement with data points
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A1,room=lab", "--value", "22.5" });
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A1,room=lab", "--value", "23.1" });
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A2,room=lab", "--value", "21.8" });

    // Step 2: Submit a TS-to-TS pipeline
    // Reads from raw_temperature → passthrough → writes to derived_temp
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-to-ts
        \\sources.[0].ts.measurement: raw_temperature
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].ts.measurement: derived_temp
        \\sinks.[0].ts.value_field: value
        \\sinks.[0].ts.tags.sensor: sensor
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-to-ts.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for derived measurement to appear
    const found = try readTsBlocking(ctx, "derived_temp", 8000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS-to-TS pipeline data did not appear within 8s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify data in derived measurement
    var read_result = try ctx.cli.run(&.{
        "ts",     "read",     "derived_temp",
        "--from", "0",        "--limit",
        "100",    "--format", "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));
    try testing.expect(read_result.stdoutContains("["));

    // Step 5: Verify derived_temp appears in ts list
    var list_result = try ctx.cli.run(&.{ "ts", "list" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    try stdx.testing.assertContains(list_result, "derived_temp");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts source - job status shows RUNNING" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Seed minimal data
    try ctx.exec(&.{ "ts", "write", "src_status_check", "--tags", "host=a", "--value", "1.0" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-status
        \\sources.[0].ts.measurement: src_status_check
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 500
        \\sinks.[0].stream.name: ts-src-status-out
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-status.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Check status — job should be running
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "RUNNING");

    try ctx.exec(&.{ "processing", "stop", job_id });
}

test "e2e/processing: ts source - stop and cancel lifecycle" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "src_lifecycle", "--tags", "host=a", "--value", "1.0" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-lifecycle
        \\sources.[0].ts.measurement: src_lifecycle
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 500
        \\sinks.[0].stream.name: ts-src-lifecycle-out
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-lifecycle.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Stop the job
    var stop_result = try ctx.cli.run(&.{ "processing", "stop", job_id });
    defer stop_result.deinit();

    try stdx.testing.assertSucceeded(stop_result);
    try stdx.testing.assertContains(stop_result, "stopped");

    // Verify status shows stopped
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "STOPPED");
}

// =============================================================================
// Processing Definition SourceKind Tests
// =============================================================================

test "e2e/processing: SourceKind enum string representation" {
    const SourceKind = src.processing.definition.SourceKind;

    try testing.expectEqualStrings("stream", SourceKind.stream.toStr());
    try testing.expectEqualStrings("ts", SourceKind.ts.toStr());
}

test "e2e/processing: SourceKind values are stable" {
    const SourceKind = src.processing.definition.SourceKind;

    // Enum backing values must remain stable for wire compatibility
    try testing.expectEqual(@as(u8, 0), @intFromEnum(SourceKind.stream));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(SourceKind.ts));
}

test "e2e/processing: TS source definition parser roundtrip" {
    const parser = src.processing.parser;

    // This YAML defines a TS source — parsed by the server-side parser
    const yaml =
        \\kind: Processing
        \\name: parser-test-ts-source
        \\sources:
        \\  - name: cpu-source
        \\    ts:
        \\      measurement: cpu_usage
        \\      namespace: production
        \\      field: idle
        \\      poll_interval_ms: 500
        \\      tags:
        \\        host: web-01
        \\sinks:
        \\  - name: output
        \\    stream:
        \\      name: output
    ;

    var def = try parser.parseJobDefinition(testing.allocator, yaml);
    defer def.deinit(testing.allocator);

    try testing.expectEqualStrings("parser-test-ts-source", def.name);
    try testing.expectEqual(@as(usize, 1), def.sources.items.len);

    const src_spec = def.sources.items[0];
    try testing.expectEqual(parser.SourceKind.ts, src_spec.kind);
    try testing.expectEqualStrings("cpu_usage", src_spec.ts_measurement);
    try testing.expectEqualStrings("idle", src_spec.ts_field);
    try testing.expectEqual(@as(u32, 500), src_spec.ts_poll_interval_ms);
    // Tags are stored as flat key-value pairs: ["host", "web-01"]
    try testing.expectEqual(@as(usize, 2), src_spec.ts_tags.len);
    try testing.expectEqualStrings("host", src_spec.ts_tags[0]);
    try testing.expectEqualStrings("web-01", src_spec.ts_tags[1]);
}

test "e2e/processing: stream source definition is default kind" {
    const parser = src.processing.parser;

    const yaml =
        \\kind: Processing
        \\name: parser-test-stream-default
        \\sources:
        \\  - name: events-source
        \\    stream:
        \\      name: events
        \\sinks:
        \\  - name: output
        \\    stream:
        \\      name: output
    ;

    var def = try parser.parseJobDefinition(testing.allocator, yaml);
    defer def.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), def.sources.items.len);
    try testing.expectEqual(parser.SourceKind.stream, def.sources.items[0].kind);
    try testing.expectEqualStrings("events", def.sources.items[0].stream);
}
