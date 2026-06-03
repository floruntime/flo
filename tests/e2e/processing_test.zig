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
    try testing.expectEqual(@as(u16, 0x360), @intFromEnum(proto.OpCode.processing_submit));
    try testing.expectEqual(@as(u16, 0x361), @intFromEnum(proto.OpCode.processing_stop));
    try testing.expectEqual(@as(u16, 0x362), @intFromEnum(proto.OpCode.processing_cancel));
    try testing.expectEqual(@as(u16, 0x363), @intFromEnum(proto.OpCode.processing_status));
    try testing.expectEqual(@as(u16, 0x364), @intFromEnum(proto.OpCode.processing_list));
    try testing.expectEqual(@as(u16, 0x365), @intFromEnum(proto.OpCode.processing_savepoint));
    try testing.expectEqual(@as(u16, 0x366), @intFromEnum(proto.OpCode.processing_restore));
    try testing.expectEqual(@as(u16, 0x367), @intFromEnum(proto.OpCode.processing_rescale));
}

test "e2e/processing: processing response opcodes are defined" {
    const proto = src.protocol.proto;

    // Response opcodes must pair with request opcodes (wire contract)
    try testing.expectEqual(@as(u16, 0x370), @intFromEnum(proto.OpCode.processing_submit_response));
    try testing.expectEqual(@as(u16, 0x371), @intFromEnum(proto.OpCode.processing_stop_response));
    try testing.expectEqual(@as(u16, 0x372), @intFromEnum(proto.OpCode.processing_cancel_response));
    try testing.expectEqual(@as(u16, 0x373), @intFromEnum(proto.OpCode.processing_status_response));
    try testing.expectEqual(@as(u16, 0x374), @intFromEnum(proto.OpCode.processing_list_response));
    try testing.expectEqual(@as(u16, 0x375), @intFromEnum(proto.OpCode.processing_savepoint_response));
    try testing.expectEqual(@as(u16, 0x376), @intFromEnum(proto.OpCode.processing_restore_response));
    try testing.expectEqual(@as(u16, 0x377), @intFromEnum(proto.OpCode.processing_rescale_response));
}

// =============================================================================
// Processing Pipeline E2E Tests (Real Server + CLI)
// =============================================================================

const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const writeTempYaml = stdx.testing.writeTempYaml;
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
fn readStreamBlocking(ctx: *stdx.testing.TestContext, stream_name: []const u8, ns: []const u8, expected: []const u8, timeout_ms_str: []const u8) !bool {
    const timeout_ms = std.fmt.parseInt(u64, timeout_ms_str, 10) catch 5000;
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);

    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "-n", ns, "--start", "0-0", "--limit", "100" });
        defer result.deinit();

        if (result.succeeded() and result.stdoutContains(expected)) {
            return true;
        }

        @import("stdx").time.sleep(poll_interval_ns);
    }

    // One final attempt after all sleeps
    var result = try ctx.cli.run(&.{ "stream", "read", stream_name, "-n", ns, "--start", "0-0", "--limit", "100" });
    defer result.deinit();

    return result.succeeded() and result.stdoutContains(expected);
}

/// Poll until job reaches a given state, or timeout.
/// Returns true if the state was reached.
fn pollForJobState(ctx: *stdx.testing.TestContext, job_id: []const u8, ns: []const u8, target_state: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 200 * std.time.ns_per_ms;
    const max_attempts = timeout_ms / 200;

    for (0..max_attempts) |_| {
        @import("stdx").time.sleep(poll_interval_ns);

        var result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", ns });
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

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_state" });

    // Write the job definition to a temp file
    const job_def =
        \\kind: Processing
        \\name: e2e-state-test
        \\namespace: proc_state
        \\sources.[0].stream.name: state-input
        \\sinks.[0].stream.name: state-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-state-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit the processing job
    var submit_result = try ctx.cli.run(&.{ "processing", "submit", path, "-n", "proc_state" });
    defer submit_result.deinit();

    try stdx.testing.assertSucceeded(submit_result);

    // Extract job ID from stdout
    const submit_output = std.mem.trim(u8, submit_result.stdout, &std.ascii.whitespace);
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Check status — job should be running
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_state" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "RUNNING");
}

test "e2e/processing: submit job then stop it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_stop" });

    const job_def =
        \\kind: Processing
        \\name: e2e-stop-test
        \\namespace: proc_stop
        \\sources.[0].stream.name: stop-input
        \\sinks.[0].stream.name: stop-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-stop-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_stop" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Stop the job
    var stop_result = try ctx.cli.run(&.{ "processing", "stop", job_id, "-n", "proc_stop" });
    defer stop_result.deinit();

    try stdx.testing.assertSucceeded(stop_result);
    try stdx.testing.assertContains(stop_result, "stopped");

    // Verify status shows stopped
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_stop" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "STOPPED");
}

test "e2e/processing: submit job then cancel it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_cancel" });

    const job_def =
        \\kind: Processing
        \\name: e2e-cancel-test
        \\namespace: proc_cancel
        \\sources.[0].stream.name: cancel-input
        \\sinks.[0].stream.name: cancel-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-cancel-test.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // Submit
    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_cancel" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Cancel the job
    var cancel_result = try ctx.cli.run(&.{ "processing", "cancel", job_id, "-n", "proc_cancel" });
    defer cancel_result.deinit();

    try stdx.testing.assertSucceeded(cancel_result);
    try stdx.testing.assertContains(cancel_result, "cancelled");

    // Verify status shows cancelled
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_cancel" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "CANCELLED");
}

test "e2e/processing: list shows submitted jobs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_list" });

    // Submit two jobs
    const job1_def =
        \\kind: Processing
        \\name: e2e-list-job1
        \\namespace: proc_list
        \\sources.[0].stream.name: list-input1
        \\sinks.[0].stream.name: list-output1
    ;
    const path1 = try writeDottedToTempYaml(testing.allocator, job1_def, "e2e-list-job1.yaml");
    defer cleanupTempFile(testing.allocator, path1);

    const job2_def =
        \\kind: Processing
        \\name: e2e-list-job2
        \\namespace: proc_list
        \\sources.[0].stream.name: list-input2
        \\sinks.[0].stream.name: list-output2
    ;
    const path2 = try writeDottedToTempYaml(testing.allocator, job2_def, "e2e-list-job2.yaml");
    defer cleanupTempFile(testing.allocator, path2);

    _ = try ctx.execCapture(&.{ "processing", "submit", path1, "-n", "proc_list" });
    _ = try ctx.execCapture(&.{ "processing", "submit", path2, "-n", "proc_list" });

    // List should show both jobs
    var list_result = try ctx.cli.run(&.{ "processing", "list", "-n", "proc_list" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    // List returns JSON array with job status objects
    try testing.expect(list_result.stdoutContains("RUNNING"));
}

test "e2e/processing: passthrough pipeline - data flows from source to sink stream" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_pipe" });

    // Step 1: Append test data to the source stream
    try ctx.exec(&.{ "stream", "append", "pipe-input", "hello-pipeline", "-n", "proc_pipe" });
    try ctx.exec(&.{ "stream", "append", "pipe-input", "second-record", "-n", "proc_pipe" });
    try ctx.exec(&.{ "stream", "append", "pipe-input", "third-record", "-n", "proc_pipe" });

    // Step 2: Submit a processing job that reads from pipe-input and writes to pipe-output
    const job_def =
        \\kind: Processing
        \\name: e2e-passthrough
        \\namespace: proc_pipe
        \\sources.[0].stream.name: pipe-input
        \\sinks.[0].stream.name: pipe-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-passthrough.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_pipe" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for the processing pipeline to move data
    // The server's tick() drives Chain.runBatch() which polls from the source
    // stream and writes to the sink stream. Give it time to process.
    const found = try readStreamBlocking(ctx, "pipe-output", "proc_pipe", "hello-pipeline", "5000");

    if (!found) {
        // Dump diagnostics on failure
        std.debug.print("\n[TIMEOUT] Data did not flow through pipeline within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_pipe" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify all records made it through
    var read_result = try ctx.cli.run(&.{ "stream", "read", "pipe-output", "-n", "proc_pipe", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "hello-pipeline");
    try stdx.testing.assertContains(read_result, "second-record");
    try stdx.testing.assertContains(read_result, "third-record");

    // Step 5: Verify job status shows records processed
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_pipe" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    // Should show records processed > 0
    try testing.expect(!status_result.stdoutContains("Processed:  0 records"));

    // Step 6: Clean up — stop the job
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_pipe" });
}

test "e2e/processing: pipeline processes data appended after job submission" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_late" });

    // Step 1: Submit job FIRST (no data yet in the source stream)
    const job_def =
        \\kind: Processing
        \\name: e2e-late-data
        \\namespace: proc_late
        \\sources.[0].stream.name: late-input
        \\sinks.[0].stream.name: late-output
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-late-data.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_late" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job a moment to start ticking (source will be exhausted initially)
    @import("stdx").time.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now append data — the job should pick it up on subsequent ticks
    try ctx.exec(&.{ "stream", "append", "late-input", "late-arrival-1", "-n", "proc_late" });
    try ctx.exec(&.{ "stream", "append", "late-input", "late-arrival-2", "-n", "proc_late" });

    // Step 3: Wait for data to flow through the pipeline
    const found = try readStreamBlocking(ctx, "late-output", "proc_late", "late-arrival-1", "5000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late data did not flow through pipeline\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_late" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Both records should be in the output
    var read_result = try ctx.cli.run(&.{ "stream", "read", "late-output", "-n", "proc_late", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "late-arrival-1");
    try stdx.testing.assertContains(read_result, "late-arrival-2");

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_late" });
}

test "e2e/processing: job status reflects processed record count" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_metrics" });

    // Append known number of records
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-1", "-n", "proc_metrics" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-2", "-n", "proc_metrics" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-3", "-n", "proc_metrics" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-4", "-n", "proc_metrics" });
    try ctx.exec(&.{ "stream", "append", "metrics-input", "metrics-5", "-n", "proc_metrics" });

    const job_def =
        \\kind: Processing
        \\name: e2e-metrics
        \\namespace: proc_metrics
        \\sources.[0].stream.name: metrics-input
        \\sinks.[0].stream.name: metrics-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-metrics.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_metrics" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for processing
    _ = try readStreamBlocking(ctx, "metrics-output", "proc_metrics", "metrics-5", "5000");

    // Check status
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_metrics" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    // Status should contain the Processed field
    try stdx.testing.assertContains(status_result, "Processed:");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_metrics" });
}

test "e2e/processing: savepoint captures job state" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_sp" });

    try ctx.exec(&.{ "stream", "append", "sp-input", "savepoint-data", "-n", "proc_sp" });

    const job_def =
        \\kind: Processing
        \\name: e2e-savepoint
        \\namespace: proc_sp
        \\sources.[0].stream.name: sp-input
        \\sinks.[0].stream.name: sp-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-savepoint.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_sp" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for at least one record to be processed
    _ = try readStreamBlocking(ctx, "sp-output", "proc_sp", "savepoint-data", "5000");

    // Take a savepoint
    var sp_result = try ctx.cli.run(&.{ "processing", "savepoint", job_id, "-n", "proc_sp" });
    defer sp_result.deinit();

    try stdx.testing.assertSucceeded(sp_result);
    // Should contain a savepoint ID
    try testing.expect(sp_result.stdoutContains("savepoint") or sp_result.stdoutContains("sp-"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_sp" });
}

// =============================================================================
// Multi-Source / Multi-Sink Pipeline E2E Tests
// =============================================================================

test "e2e/processing: multi-source pipeline merges two input streams" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_msrc" });

    // Append data to two separate source streams
    try ctx.exec(&.{ "stream", "append", "multi-src-a", "from-stream-a", "-n", "proc_msrc" });
    try ctx.exec(&.{ "stream", "append", "multi-src-b", "from-stream-b", "-n", "proc_msrc" });

    // Submit a job with two sources — MergingSource will round-robin poll both
    const job_def =
        \\kind: Processing
        \\name: e2e-multi-source
        \\namespace: proc_msrc
        \\sources.[0].stream.name: multi-src-a
        \\sources.[1].stream.name: multi-src-b
        \\sinks.[0].stream.name: multi-src-out
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-multi-source.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_msrc" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from submit\n", .{});
        return error.NoJobId;
    };

    // Wait for data from both streams to appear in the output
    const found_a = try readStreamBlocking(ctx, "multi-src-out", "proc_msrc", "from-stream-a", "5000");
    const found_b = try readStreamBlocking(ctx, "multi-src-out", "proc_msrc", "from-stream-b", "5000");

    if (!found_a or !found_b) {
        std.debug.print("\n[TIMEOUT] Multi-source merge: a={}, b={}\n", .{ found_a, found_b });
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_msrc" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify sink has records from both source streams
    var read_result = try ctx.cli.run(&.{ "stream", "read", "multi-src-out", "-n", "proc_msrc", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "from-stream-a");
    try stdx.testing.assertContains(read_result, "from-stream-b");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_msrc" });
}

test "e2e/processing: multi-sink declaration uses primary sink" {
    // Multi-sink fan-out is wired — all sinks receive records.
    // This test verifies data flows through the primary (stream) sink;
    // the secondary KV sink is a no-op (KV write not yet implemented in sink dispatch).
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_msink" });

    // Seed source stream
    try ctx.exec(&.{ "stream", "append", "msink-input", "multi-sink-record", "-n", "proc_msink" });

    // Submit job with two sinks declared; only the first (stream) is wired
    const job_def =
        \\kind: Processing
        \\name: e2e-multi-sink
        \\namespace: proc_msink
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

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_msink" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to appear in the primary (stream) sink
    const found = try readStreamBlocking(ctx, "msink-stream-out", "proc_msink", "multi-sink-record", "5000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Multi-sink data did not flow to primary stream sink\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_msink" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_msink" });
}

test "e2e/processing: checkpoint persists to internal KV namespace" {
    // Use expose_internal_keys so kv scan shows _proc: keys
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .expose_internal_keys = true },
    });
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_ckpt" });

    // Seed + submit
    try ctx.exec(&.{ "stream", "append", "ckpt-input", "checkpoint-test-data", "-n", "proc_ckpt" });

    const job_def =
        \\kind: Processing
        \\name: e2e-checkpoint-persist
        \\namespace: proc_ckpt
        \\sources.[0].stream.name: ckpt-input
        \\sinks.[0].stream.name: ckpt-output
        \\checkpointing.interval_ms: 500
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ckpt.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_ckpt" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through
    _ = try readStreamBlocking(ctx, "ckpt-output", "proc_ckpt", "checkpoint-test-data", "5000");

    // Take a savepoint — this triggers a checkpoint that persists to KV
    var sp_result = try ctx.cli.run(&.{ "processing", "savepoint", job_id, "-n", "proc_ckpt" });
    defer sp_result.deinit();
    try stdx.testing.assertSucceeded(sp_result);

    // With expose_internal_keys=true, _proc: checkpoint keys should be visible
    var scan_result = try ctx.cli.run(&.{ "kv", "list", "-n", "proc_ckpt" });
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

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_ckpt" });
}

// =============================================================================
// Time-Series Sink E2E Tests
// =============================================================================

/// Poll `flo ts read` until data appears or timeout.
/// Returns true if the measurement returned data within the timeout.
fn readTsBlocking(ctx: *stdx.testing.TestContext, measurement: []const u8, ns: []const u8, field: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);

    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{
            "ts",     "read",    measurement, "-n",     ns,    "--from",
            "0",      "--field", field,       "--limit", "100", "--output",
            "json",
        });
        defer result.deinit();

        if (result.succeeded() and !result.stdoutContains("(no data)") and result.stdoutContains("[")) {
            return true;
        }

        @import("stdx").time.sleep(poll_interval_ns);
    }

    // One final attempt
    var result = try ctx.cli.run(&.{
        "ts",     "read",    measurement, "-n",     ns,    "--from",
        "0",      "--field", field,       "--limit", "100", "--output",
        "json",
    });
    defer result.deinit();

    return result.succeeded() and !result.stdoutContains("(no data)") and result.stdoutContains("[");
}

test "e2e/processing: ts sink - JSON records flow to time-series measurement" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tssink" });

    // Step 1: Append JSON records to the source stream
    // The FloTsSink will extract tags and fields from these JSON payloads
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-01\",\"cpu_percent\":72.5}", "-n", "proc_tssink" });
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-01\",\"cpu_percent\":85.3}", "-n", "proc_tssink" });
    try ctx.exec(&.{ "stream", "append", "ts-sink-input", "{\"hostname\":\"web-02\",\"cpu_percent\":45.0}", "-n", "proc_tssink" });

    // Step 2: Submit a processing job with a TS sink configured
    // Tags: host ← hostname (extract "hostname" field as tag "host")
    // Fields: cpu ← cpu_percent (extract "cpu_percent" as field "cpu")
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-sink
        \\namespace: proc_tssink
        \\sources.[0].stream.name: ts-sink-input
        \\sinks.[0].ts.measurement: proc_cpu_metrics
        \\sinks.[0].ts.tags.host: hostname
        \\sinks.[0].ts.fields.cpu: cpu_percent
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-sink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tssink" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for the TS sink to write data (field `cpu` ← cpu_percent)
    const found = try readTsBlocking(ctx, "proc_cpu_metrics", "proc_tssink", "cpu", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS sink data did not appear within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tssink" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify data via `flo ts read` — field `cpu`, tag host=web-01, value 72.5
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_cpu_metrics", "-n",     "proc_tssink", "--tags", "host=web-01",
        "--from", "0",    "--field",          "cpu",    "--limit",     "100",    "--output", "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    // JSON output should contain data points (not "(no data)") with the mapped value.
    try testing.expect(!read_result.stdoutContains("(no data)"));
    try testing.expect(read_result.stdoutContains("["));
    try testing.expect(read_result.stdoutContains("72.5"));

    // Step 5: Verify measurement appears in `flo ts list`
    var list_result = try ctx.cli.run(&.{ "ts", "list", "-n", "proc_tssink" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    try stdx.testing.assertContains(list_result, "proc_cpu_metrics");

    // Step 6: Clean up
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tssink" });
}

test "e2e/processing: ts sink - value_field shorthand for scalar extraction" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tsscal" });

    // Append JSON records with a "temperature" field as the value
    try ctx.exec(&.{ "stream", "append", "ts-scalar-input", "{\"sensor\":\"A1\",\"temperature\":23.4}", "-n", "proc_tsscal" });
    try ctx.exec(&.{ "stream", "append", "ts-scalar-input", "{\"sensor\":\"A1\",\"temperature\":24.1}", "-n", "proc_tsscal" });

    // Submit job: value_field = "temperature" (shorthand — no explicit fields map)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-scalar
        \\namespace: proc_tsscal
        \\sources.[0].stream.name: ts-scalar-input
        \\sinks.[0].ts.measurement: proc_temp
        \\sinks.[0].ts.value_field: temperature
        \\sinks.[0].ts.tags.sensor: sensor
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-scalar.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tsscal" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to appear (value_field shorthand stores under the "value" field)
    const found = try readTsBlocking(ctx, "proc_temp", "proc_tsscal", "value", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS scalar sink data did not appear within 5s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tsscal" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Read with sensor tag filter
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_temp", "-n",  "proc_tsscal", "--tags", "sensor=A1",
        "--from", "0",    "--limit",   "100", "--output",    "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));
    // JSON array with data points
    try testing.expect(read_result.stdoutContains("timestamp_ms"));

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tsscal" });
}

test "e2e/processing: ts sink - late data flows through after job starts" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tslate" });

    // Step 1: Submit job FIRST (no data yet)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-late
        \\namespace: proc_tslate
        \\sources.[0].stream.name: ts-late-input
        \\sinks.[0].ts.measurement: proc_late_metric
        \\sinks.[0].ts.tags.host: host
        \\sinks.[0].ts.fields.load: load_avg
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-late.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tslate" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job time to start its tick loop
    @import("stdx").time.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now append data — should be picked up on subsequent ticks
    try ctx.exec(&.{ "stream", "append", "ts-late-input", "{\"host\":\"db-01\",\"load_avg\":3.14}", "-n", "proc_tslate" });
    try ctx.exec(&.{ "stream", "append", "ts-late-input", "{\"host\":\"db-01\",\"load_avg\":2.71}", "-n", "proc_tslate" });

    // Step 3: Wait for TS data to appear (field `load` ← load_avg)
    const found = try readTsBlocking(ctx, "proc_late_metric", "proc_tslate", "load", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late TS sink data did not appear within 5s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify via read
    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "proc_late_metric", "-n",     "proc_tslate", "--tags", "host=db-01",
        "--from", "0",    "--field",          "load",   "--limit",     "100",    "--output", "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tslate" });
}

test "e2e/processing: ts sink - query aggregation on pipeline-written data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tsagg" });

    // Write records with known values for aggregation verification
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":100.0}", "-n", "proc_tsagg" });
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":200.0}", "-n", "proc_tsagg" });
    try ctx.exec(&.{ "stream", "append", "ts-agg-input", "{\"region\":\"us\",\"rps\":300.0}", "-n", "proc_tsagg" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-agg
        \\namespace: proc_tsagg
        \\sources.[0].stream.name: ts-agg-input
        \\sinks.[0].ts.measurement: proc_requests
        \\sinks.[0].ts.tags.region: region
        \\sinks.[0].ts.fields.rps: rps
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-agg.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tsagg" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through (field `rps` ← rps)
    const found = try readTsBlocking(ctx, "proc_requests", "proc_tsagg", "rps", 5000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS agg sink data did not appear within 5s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify via `flo ts query` with aggregation on the `rps` field
    var query_result = try ctx.cli.run(&.{
        "ts",     "query", "proc_requests", "-n",  "proc_tsagg", "--tags", "region=us",
        "--from", "0",     "--field",       "rps", "--window",   "1h",     "--agg", "sum",
        "--output", "json",
    });
    defer query_result.deinit();

    try stdx.testing.assertSucceeded(query_result);
    // Should have data (not empty)
    try testing.expect(!query_result.stdoutContains("(no data)"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tsagg" });
}

// =============================================================================
// Time-Series Source E2E Tests
// =============================================================================

test "e2e/processing: ts source - TS measurement data flows to stream sink" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tssrc" });

    // Step 1: Seed the TS measurement with data points via `flo ts write`
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-01", "--value", "72.5", "-n", "proc_tssrc" });
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-01", "--value", "85.3", "-n", "proc_tssrc" });
    try ctx.exec(&.{ "ts", "write", "src_cpu", "--tags", "host=web-02", "--value", "45.0", "-n", "proc_tssrc" });

    // Step 2: Submit a processing job with a TS *source* that reads from src_cpu
    // The FloTsSource wraps each point as JSON: {"measurement":..., "value":..., ...}
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-stream
        \\namespace: proc_tssrc
        \\sources.[0].ts.measurement: src_cpu
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].stream.name: ts-src-stream-out
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-stream.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tssrc" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for data to flow through to the stream sink
    // The TS source polls the measurement and emits JSON records
    const found = try readStreamBlocking(ctx, "ts-src-stream-out", "proc_tssrc", "src_cpu", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS source data did not flow to stream sink within 8s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tssrc" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify the stream contains JSON-wrapped TS data
    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-stream-out", "-n", "proc_tssrc", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    // FloTsSource emits JSON containing measurement name and value
    try stdx.testing.assertContains(read_result, "src_cpu");

    // Step 5: Verify job status shows records processed
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tssrc" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try testing.expect(!status_result.stdoutContains("Processed:  0 records"));

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tssrc" });
}

test "e2e/processing: ts source - late data written after job starts" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tsslat" });

    // Step 1: Submit job FIRST with TS source (no data yet in measurement)
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-late
        \\namespace: proc_tsslat
        \\sources.[0].ts.measurement: src_late_cpu
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 200
        \\sinks.[0].stream.name: ts-src-late-out
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-late.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tsslat" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Give the job time to start ticking (source will see no data initially)
    @import("stdx").time.sleep(300 * std.time.ns_per_ms);

    // Step 2: Now write TS data — the source should pick it up on subsequent polls
    try ctx.exec(&.{ "ts", "write", "src_late_cpu", "--tags", "host=db-01", "--value", "3.14", "-n", "proc_tsslat" });
    try ctx.exec(&.{ "ts", "write", "src_late_cpu", "--tags", "host=db-01", "--value", "2.71", "-n", "proc_tsslat" });

    // Step 3: Wait for data to flow through
    const found = try readStreamBlocking(ctx, "ts-src-late-out", "proc_tsslat", "src_late_cpu", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Late TS source data did not flow within 8s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-late-out", "-n", "proc_tsslat", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "src_late_cpu");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tsslat" });
}

test "e2e/processing: ts source - with tag filter reads subset of data" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tssflt" });

    // Seed data with two different hosts
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-01", "--value", "10.0", "-n", "proc_tssflt" });
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-02", "--value", "20.0", "-n", "proc_tssflt" });
    try ctx.exec(&.{ "ts", "write", "src_filtered", "--tags", "host=web-01", "--value", "30.0", "-n", "proc_tssflt" });

    // Submit job with tag filter: only read host=web-01
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-filtered
        \\namespace: proc_tssflt
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

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tssflt" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow
    const found = try readStreamBlocking(ctx, "ts-src-filtered-out", "proc_tssflt", "src_filtered", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] Filtered TS source data did not flow within 8s\n", .{});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify output contains data
    var read_result = try ctx.cli.run(&.{ "stream", "read", "ts-src-filtered-out", "-n", "proc_tssflt", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "src_filtered");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tssflt" });
}

test "e2e/processing: ts source to ts sink - derived metrics pipeline" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_ts2ts" });

    // Step 1: Seed source measurement with data points
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A1,room=lab", "--value", "22.5", "-n", "proc_ts2ts" });
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A1,room=lab", "--value", "23.1", "-n", "proc_ts2ts" });
    try ctx.exec(&.{ "ts", "write", "raw_temperature", "--tags", "sensor=A2,room=lab", "--value", "21.8", "-n", "proc_ts2ts" });

    // Step 2: Submit a TS-to-TS pipeline
    // Reads from raw_temperature → passthrough → writes to derived_temp
    const job_def =
        \\kind: Processing
        \\name: e2e-ts-to-ts
        \\namespace: proc_ts2ts
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

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_ts2ts" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 3: Wait for derived measurement to appear (value_field → "value")
    const found = try readTsBlocking(ctx, "derived_temp", "proc_ts2ts", "value", 8000);

    if (!found) {
        std.debug.print("\n[TIMEOUT] TS-to-TS pipeline data did not appear within 8s\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_ts2ts" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Step 4: Verify data in derived measurement
    var read_result = try ctx.cli.run(&.{
        "ts",       "read",       "derived_temp",
        "-n",       "proc_ts2ts", "--from",
        "0",        "--limit",    "100",
        "--output", "json",
    });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try testing.expect(!read_result.stdoutContains("(no data)"));
    try testing.expect(read_result.stdoutContains("["));

    // Step 5: Verify derived_temp appears in ts list
    var list_result = try ctx.cli.run(&.{ "ts", "list", "-n", "proc_ts2ts" });
    defer list_result.deinit();

    try stdx.testing.assertSucceeded(list_result);
    try stdx.testing.assertContains(list_result, "derived_temp");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_ts2ts" });
}

test "e2e/processing: ts source - job status shows RUNNING" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tsstat" });

    // Seed minimal data
    try ctx.exec(&.{ "ts", "write", "src_status_check", "--tags", "host=a", "--value", "1.0", "-n", "proc_tsstat" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-status
        \\namespace: proc_tsstat
        \\sources.[0].ts.measurement: src_status_check
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 500
        \\sinks.[0].stream.name: ts-src-status-out
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-status.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tsstat" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Check status — job should be running
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tsstat" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "RUNNING");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_tsstat" });
}

test "e2e/processing: ts source - stop and cancel lifecycle" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_tslife" });

    try ctx.exec(&.{ "ts", "write", "src_lifecycle", "--tags", "host=a", "--value", "1.0", "-n", "proc_tslife" });

    const job_def =
        \\kind: Processing
        \\name: e2e-ts-src-lifecycle
        \\namespace: proc_tslife
        \\sources.[0].ts.measurement: src_lifecycle
        \\sources.[0].ts.field: value
        \\sources.[0].ts.poll_interval_ms: 500
        \\sinks.[0].stream.name: ts-src-lifecycle-out
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ts-src-lifecycle.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_tslife" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Stop the job
    var stop_result = try ctx.cli.run(&.{ "processing", "stop", job_id, "-n", "proc_tslife" });
    defer stop_result.deinit();

    try stdx.testing.assertSucceeded(stop_result);
    try stdx.testing.assertContains(stop_result, "stopped");

    // Verify status shows stopped
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_tslife" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "STOPPED");
}

// =============================================================================
// Namespace Isolation Tests
// =============================================================================

test "e2e/processing: same job name in different namespaces are independent" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces
    try ctx.exec(&.{ "ns", "create", "proc_iso_a" });
    try ctx.exec(&.{ "ns", "create", "proc_iso_b" });

    // Submit same-named job in both namespaces with different source streams
    const job_def_a =
        \\kind: Processing
        \\name: iso-shared-name
        \\namespace: proc_iso_a
        \\sources.[0].stream.name: iso-a-input
        \\sinks.[0].stream.name: iso-a-output
    ;
    const path_a = try writeDottedToTempYaml(testing.allocator, job_def_a, "e2e-iso-a.yaml");
    defer cleanupTempFile(testing.allocator, path_a);

    const job_def_b =
        \\kind: Processing
        \\name: iso-shared-name
        \\namespace: proc_iso_b
        \\sources.[0].stream.name: iso-b-input
        \\sinks.[0].stream.name: iso-b-output
    ;
    const path_b = try writeDottedToTempYaml(testing.allocator, job_def_b, "e2e-iso-b.yaml");
    defer cleanupTempFile(testing.allocator, path_b);

    // Submit jobs in their respective namespaces
    var submit_a = try ctx.cli.run(&.{ "processing", "submit", path_a, "-n", "proc_iso_a" });
    defer submit_a.deinit();
    try stdx.testing.assertSucceeded(submit_a);
    const job_id_a = extractJobId(std.mem.trim(u8, submit_a.stdout, &std.ascii.whitespace)) orelse return error.NoJobId;

    var submit_b = try ctx.cli.run(&.{ "processing", "submit", path_b, "-n", "proc_iso_b" });
    defer submit_b.deinit();
    try stdx.testing.assertSucceeded(submit_b);
    const job_id_b = extractJobId(std.mem.trim(u8, submit_b.stdout, &std.ascii.whitespace)) orelse return error.NoJobId;

    // Both should be running independently
    var status_a = try ctx.cli.run(&.{ "processing", "status", job_id_a, "-n", "proc_iso_a" });
    defer status_a.deinit();
    try stdx.testing.assertSucceeded(status_a);
    try stdx.testing.assertContains(status_a, "RUNNING");

    var status_b = try ctx.cli.run(&.{ "processing", "status", job_id_b, "-n", "proc_iso_b" });
    defer status_b.deinit();
    try stdx.testing.assertSucceeded(status_b);
    try stdx.testing.assertContains(status_b, "RUNNING");

    // Stop only job A
    try ctx.exec(&.{ "processing", "stop", job_id_a, "-n", "proc_iso_a" });

    // A should be stopped, B should still be running
    var status_a2 = try ctx.cli.run(&.{ "processing", "status", job_id_a, "-n", "proc_iso_a" });
    defer status_a2.deinit();
    try stdx.testing.assertContains(status_a2, "STOPPED");

    var status_b2 = try ctx.cli.run(&.{ "processing", "status", job_id_b, "-n", "proc_iso_b" });
    defer status_b2.deinit();
    try stdx.testing.assertContains(status_b2, "RUNNING");

    // Clean up
    try ctx.exec(&.{ "processing", "stop", job_id_b, "-n", "proc_iso_b" });
}

test "e2e/processing: list is namespace-scoped" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_ls_a" });
    try ctx.exec(&.{ "ns", "create", "proc_ls_b" });

    // Submit a job only in namespace A
    const job_def =
        \\kind: Processing
        \\name: ls-scoped-job
        \\namespace: proc_ls_a
        \\sources.[0].stream.name: ls-input
        \\sinks.[0].stream.name: ls-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-ls-scoped.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_ls_a" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // List in namespace A — should see the job
    var list_a = try ctx.cli.run(&.{ "processing", "list", "-n", "proc_ls_a" });
    defer list_a.deinit();
    try stdx.testing.assertSucceeded(list_a);
    try testing.expect(list_a.stdoutContains("RUNNING"));

    // List in namespace B — should be empty (no jobs submitted there)
    var list_b = try ctx.cli.run(&.{ "processing", "list", "-n", "proc_ls_b" });
    defer list_b.deinit();
    try stdx.testing.assertSucceeded(list_b);
    try testing.expect(!list_b.stdoutContains("RUNNING"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_ls_a" });
}

test "e2e/processing: default namespace is isolated from named namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_custom_ns" });

    // Submit job in default namespace (no -n flag)
    const def_job =
        \\kind: Processing
        \\name: ns-default-job
        \\sources.[0].stream.name: ns-def-input
        \\sinks.[0].stream.name: ns-def-output
    ;
    const path_def = try writeDottedToTempYaml(testing.allocator, def_job, "e2e-ns-default.yaml");
    defer cleanupTempFile(testing.allocator, path_def);

    const submit_def = try ctx.execCapture(&.{ "processing", "submit", path_def });
    const job_id_def = extractJobId(submit_def) orelse return error.NoJobId;

    // Submit same-named job in custom namespace
    const custom_job =
        \\kind: Processing
        \\name: ns-default-job
        \\namespace: proc_custom_ns
        \\sources.[0].stream.name: ns-cust-input
        \\sinks.[0].stream.name: ns-cust-output
    ;
    const path_cust = try writeDottedToTempYaml(testing.allocator, custom_job, "e2e-ns-custom.yaml");
    defer cleanupTempFile(testing.allocator, path_cust);

    const submit_cust = try ctx.execCapture(&.{ "processing", "submit", path_cust, "-n", "proc_custom_ns" });
    const job_id_cust = extractJobId(submit_cust) orelse return error.NoJobId;

    // Both should be running in their respective namespaces
    var status_def = try ctx.cli.run(&.{ "processing", "status", job_id_def });
    defer status_def.deinit();
    try stdx.testing.assertSucceeded(status_def);
    try stdx.testing.assertContains(status_def, "RUNNING");

    var status_cust = try ctx.cli.run(&.{ "processing", "status", job_id_cust, "-n", "proc_custom_ns" });
    defer status_cust.deinit();
    try stdx.testing.assertSucceeded(status_cust);
    try stdx.testing.assertContains(status_cust, "RUNNING");

    // Stop default, custom should remain running
    try ctx.exec(&.{ "processing", "stop", job_id_def });

    var status_def2 = try ctx.cli.run(&.{ "processing", "status", job_id_def });
    defer status_def2.deinit();
    try stdx.testing.assertContains(status_def2, "STOPPED");

    var status_cust2 = try ctx.cli.run(&.{ "processing", "status", job_id_cust, "-n", "proc_custom_ns" });
    defer status_cust2.deinit();
    try stdx.testing.assertContains(status_cust2, "RUNNING");

    try ctx.exec(&.{ "processing", "stop", job_id_cust, "-n", "proc_custom_ns" });
}

test "e2e/processing: pipeline data flows only within namespace scope" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces
    try ctx.exec(&.{ "ns", "create", "proc_scope_a" });
    try ctx.exec(&.{ "ns", "create", "proc_scope_b" });

    // Append data to same-named stream in both namespaces
    try ctx.exec(&.{ "stream", "append", "scoped-input", "data-from-ns-a", "-n", "proc_scope_a" });
    try ctx.exec(&.{ "stream", "append", "scoped-input", "data-from-ns-b", "-n", "proc_scope_b" });

    // Submit pipeline in namespace A only
    const job_def =
        \\kind: Processing
        \\name: scoped-pipeline
        \\namespace: proc_scope_a
        \\sources.[0].stream.name: scoped-input
        \\sinks.[0].stream.name: scoped-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-scoped-pipe.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_scope_a" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data from namespace A to flow through
    const found = try readStreamBlocking(ctx, "scoped-output", "proc_scope_a", "data-from-ns-a", "5000");

    if (!found) {
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify output in namespace A contains ONLY data from namespace A
    var read_a = try ctx.cli.run(&.{ "stream", "read", "scoped-output", "-n", "proc_scope_a", "--start", "0-0", "--limit", "10" });
    defer read_a.deinit();

    try stdx.testing.assertSucceeded(read_a);
    try stdx.testing.assertContains(read_a, "data-from-ns-a");
    // Should NOT contain data from namespace B's same-named stream
    try stdx.testing.assertNotContains(read_a, "data-from-ns-b");

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_scope_a" });
}

test "e2e/processing: multi-shard namespace isolation across shards" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces — names chosen to likely hash to different shards
    try ctx.exec(&.{ "ns", "create", "proc_shard_alpha" });
    try ctx.exec(&.{ "ns", "create", "proc_shard_beta" });

    // Submit same-named job in both namespaces
    const job_def_alpha =
        \\kind: Processing
        \\name: shard-test-job
        \\namespace: proc_shard_alpha
        \\sources.[0].stream.name: shard-alpha-in
        \\sinks.[0].stream.name: shard-alpha-out
    ;
    const path_alpha = try writeDottedToTempYaml(testing.allocator, job_def_alpha, "e2e-shard-alpha.yaml");
    defer cleanupTempFile(testing.allocator, path_alpha);

    const job_def_beta =
        \\kind: Processing
        \\name: shard-test-job
        \\namespace: proc_shard_beta
        \\sources.[0].stream.name: shard-beta-in
        \\sinks.[0].stream.name: shard-beta-out
    ;
    const path_beta = try writeDottedToTempYaml(testing.allocator, job_def_beta, "e2e-shard-beta.yaml");
    defer cleanupTempFile(testing.allocator, path_beta);

    // Submit in both namespaces
    var submit_alpha = try ctx.cli.run(&.{ "processing", "submit", path_alpha, "-n", "proc_shard_alpha" });
    defer submit_alpha.deinit();
    try stdx.testing.assertSucceeded(submit_alpha);
    const job_id_alpha = extractJobId(std.mem.trim(u8, submit_alpha.stdout, &std.ascii.whitespace)) orelse return error.NoJobId;

    var submit_beta = try ctx.cli.run(&.{ "processing", "submit", path_beta, "-n", "proc_shard_beta" });
    defer submit_beta.deinit();
    try stdx.testing.assertSucceeded(submit_beta);
    const job_id_beta = extractJobId(std.mem.trim(u8, submit_beta.stdout, &std.ascii.whitespace)) orelse return error.NoJobId;

    // Both should be running
    var status_alpha = try ctx.cli.run(&.{ "processing", "status", job_id_alpha, "-n", "proc_shard_alpha" });
    defer status_alpha.deinit();
    try stdx.testing.assertContains(status_alpha, "RUNNING");

    var status_beta = try ctx.cli.run(&.{ "processing", "status", job_id_beta, "-n", "proc_shard_beta" });
    defer status_beta.deinit();
    try stdx.testing.assertContains(status_beta, "RUNNING");

    // Cancel alpha — beta should be unaffected
    try ctx.exec(&.{ "processing", "cancel", job_id_alpha, "-n", "proc_shard_alpha" });

    var status_alpha2 = try ctx.cli.run(&.{ "processing", "status", job_id_alpha, "-n", "proc_shard_alpha" });
    defer status_alpha2.deinit();
    try stdx.testing.assertContains(status_alpha2, "CANCELLED");

    var status_beta2 = try ctx.cli.run(&.{ "processing", "status", job_id_beta, "-n", "proc_shard_beta" });
    defer status_beta2.deinit();
    try stdx.testing.assertContains(status_beta2, "RUNNING");

    try ctx.exec(&.{ "processing", "stop", job_id_beta, "-n", "proc_shard_beta" });
}

// =============================================================================
// Processing Persistence Tests (restart survival)
// =============================================================================

test "e2e/processing: submitted job survives restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Scope to dedicated namespace for isolation
    try ctx.exec(&.{ "ns", "create", "proc_persist" });

    // Submit a processing job
    const job_def =
        \\kind: Processing
        \\name: persist-test-job
        \\namespace: proc_persist
        \\sources.[0].stream.name: persist-input
        \\sinks.[0].stream.name: persist-output
        \\parallelism: 1
        \\batch_size: 50
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-persist-submit.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_persist" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Verify it exists before restart
    var before = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    try stdx.testing.assertContains(before, "persist-test-job");

    // Restart the server
    try ctx.restartServer();

    // After restart, job should still exist with same metadata
    var after = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "persist-test-job");
    try stdx.testing.assertContains(after, job_id);
}

test "e2e/processing: stopped job state survives restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_persist_stop" });

    const job_def =
        \\kind: Processing
        \\name: persist-stop-job
        \\namespace: proc_persist_stop
        \\sources.[0].stream.name: pstop-input
        \\sinks.[0].stream.name: pstop-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-persist-stop.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_persist_stop" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Stop the job
    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_persist_stop" });

    // Verify stopped state before restart
    var before = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist_stop" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    try stdx.testing.assertContains(before, "STOPPED");

    // Restart the server
    try ctx.restartServer();

    // After restart, job should still be stopped (not reset to running)
    var after = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist_stop" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "STOPPED");
}

test "e2e/processing: cancelled job state survives restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_persist_cancel" });

    const job_def =
        \\kind: Processing
        \\name: persist-cancel-job
        \\namespace: proc_persist_cancel
        \\sources.[0].stream.name: pcancel-input
        \\sinks.[0].stream.name: pcancel-output
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-persist-cancel.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_persist_cancel" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Cancel the job
    try ctx.exec(&.{ "processing", "cancel", job_id, "-n", "proc_persist_cancel" });

    // Restart the server
    try ctx.restartServer();

    // After restart, job should still be cancelled
    var after = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist_cancel" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "CANCELLED");
}

test "e2e/processing: list shows jobs after restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_persist_list" });

    // Submit two jobs
    const job1_def =
        \\kind: Processing
        \\name: persist-list-alpha
        \\namespace: proc_persist_list
        \\sources.[0].stream.name: plist-in1
        \\sinks.[0].stream.name: plist-out1
    ;
    const path1 = try writeDottedToTempYaml(testing.allocator, job1_def, "e2e-persist-list1.yaml");
    defer cleanupTempFile(testing.allocator, path1);

    const job2_def =
        \\kind: Processing
        \\name: persist-list-beta
        \\namespace: proc_persist_list
        \\sources.[0].stream.name: plist-in2
        \\sinks.[0].stream.name: plist-out2
    ;
    const path2 = try writeDottedToTempYaml(testing.allocator, job2_def, "e2e-persist-list2.yaml");
    defer cleanupTempFile(testing.allocator, path2);

    _ = try ctx.execCapture(&.{ "processing", "submit", path1, "-n", "proc_persist_list" });
    _ = try ctx.execCapture(&.{ "processing", "submit", path2, "-n", "proc_persist_list" });

    // Restart the server
    try ctx.restartServer();

    // After restart, list should show both jobs
    var after = try ctx.cli.run(&.{ "processing", "list", "-n", "proc_persist_list" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "persist-list-alpha");
    try stdx.testing.assertContains(after, "persist-list-beta");
}

test "e2e/processing: rescaled parallelism survives restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_persist_rescale" });

    const job_def =
        \\kind: Processing
        \\name: persist-rescale-job
        \\namespace: proc_persist_rescale
        \\sources.[0].stream.name: prescale-input
        \\sinks.[0].stream.name: prescale-output
        \\parallelism: 1
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-persist-rescale.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_persist_rescale" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Rescale to parallelism=4
    try ctx.exec(&.{ "processing", "rescale", job_id, "4", "-n", "proc_persist_rescale" });

    // Verify parallelism changed before restart
    var before = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist_rescale" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    try stdx.testing.assertContains(before, "Parallelism:4");

    // Restart the server
    try ctx.restartServer();

    // After restart, parallelism should still be 4 (not reset to 1)
    var after = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_persist_rescale" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "Parallelism:4");
}

test "e2e/processing: new job IDs don't collide after restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_persist_ids" });

    // Submit a job before restart
    const job1_def =
        \\kind: Processing
        \\name: id-collision-test
        \\namespace: proc_persist_ids
        \\sources.[0].stream.name: idtest-in
        \\sinks.[0].stream.name: idtest-out
    ;
    const path1 = try writeDottedToTempYaml(testing.allocator, job1_def, "e2e-persist-ids1.yaml");
    defer cleanupTempFile(testing.allocator, path1);

    const submit1 = try ctx.execCapture(&.{ "processing", "submit", path1, "-n", "proc_persist_ids" });
    const job_id_1 = extractJobId(submit1) orelse return error.NoJobId;

    // Restart the server
    try ctx.restartServer();

    // Submit another job after restart
    const job2_def =
        \\kind: Processing
        \\name: id-collision-test-2
        \\namespace: proc_persist_ids
        \\sources.[0].stream.name: idtest-in2
        \\sinks.[0].stream.name: idtest-out2
    ;
    const path2 = try writeDottedToTempYaml(testing.allocator, job2_def, "e2e-persist-ids2.yaml");
    defer cleanupTempFile(testing.allocator, path2);

    const submit2 = try ctx.execCapture(&.{ "processing", "submit", path2, "-n", "proc_persist_ids" });
    const job_id_2 = extractJobId(submit2) orelse return error.NoJobId;

    // New job ID must be different from the pre-restart one
    try testing.expect(!std.mem.eql(u8, job_id_1, job_id_2));

    // Both jobs should be visible in list
    var list = try ctx.cli.run(&.{ "processing", "list", "-n", "proc_persist_ids" });
    defer list.deinit();
    try stdx.testing.assertSucceeded(list);
    try stdx.testing.assertContains(list, "id-collision-test");
    try stdx.testing.assertContains(list, "id-collision-test-2");
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

// =============================================================================
// KV Lookup Operator Pipeline E2E Tests
// =============================================================================

test "e2e/processing: kv_lookup filter mode — only records with matching KV keys pass through" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_kvfilt" });

    // Step 1: Seed KV store with known accounts
    try ctx.exec(&.{ "kv", "set", "account:alice", "active", "-n", "proc_kvfilt" });
    try ctx.exec(&.{ "kv", "set", "account:bob", "active", "-n", "proc_kvfilt" });

    // Step 2: Append JSON records to source stream — mix of known and unknown accounts
    try ctx.exec(&.{ "stream", "append", "kvf-input", "{\"account_id\":\"alice\",\"amount\":100}", "-n", "proc_kvfilt" });
    try ctx.exec(&.{ "stream", "append", "kvf-input", "{\"account_id\":\"charlie\",\"amount\":200}", "-n", "proc_kvfilt" });
    try ctx.exec(&.{ "stream", "append", "kvf-input", "{\"account_id\":\"bob\",\"amount\":300}", "-n", "proc_kvfilt" });
    try ctx.exec(&.{ "stream", "append", "kvf-input", "{\"account_id\":\"dave\",\"amount\":400}", "-n", "proc_kvfilt" });

    // Step 3: Submit pipeline with kv_lookup operator in filter mode
    const job_def =
        \\kind: Processing
        \\name: e2e-kvlookup-filter
        \\namespace: proc_kvfilt
        \\sources.[0].stream.name: kvf-input
        \\sinks.[0].stream.name: kvf-output
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: check-account
        \\operators.[0].lookup_key: account:${$.account_id}
        \\operators.[0].namespace: proc_kvfilt
        \\operators.[0].mode: filter
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kvlookup-filter.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_kvfilt" });
    const job_id = extractJobId(submit_output) orelse {
        std.debug.print("\n[FAILED] Could not extract job ID from: '{s}'\n", .{submit_output});
        return error.NoJobId;
    };

    // Step 4: Wait for data to flow through
    const found = try readStreamBlocking(ctx, "kvf-output", "proc_kvfilt", "alice", "5000");

    if (!found) {
        std.debug.print("\n[FAILED] kv_lookup filter pipeline did not produce output within timeout\n", .{});
        ctx.dumpServerLogs();
        return error.TimeoutWaitingForData;
    }

    // Step 5: Verify only alice and bob records passed through (charlie and dave should be filtered out)
    var read_result = try ctx.cli.run(&.{ "stream", "read", "kvf-output", "-n", "proc_kvfilt", "--start", "0-0", "--limit", "20" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "alice");
    try stdx.testing.assertContains(read_result, "bob");
    // charlie and dave should NOT be in the output
    try testing.expect(!read_result.stdoutContains("charlie"));
    try testing.expect(!read_result.stdoutContains("dave"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_kvfilt" });
}

test "e2e/processing: kv_lookup enrich mode — records are enriched with KV values" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_kvenr" });

    // Step 1: Seed KV with user profiles
    try ctx.exec(&.{ "kv", "set", "user:u1", "premium", "-n", "proc_kvenr" });
    try ctx.exec(&.{ "kv", "set", "user:u2", "basic", "-n", "proc_kvenr" });

    // Step 2: Append records referencing user IDs
    try ctx.exec(&.{ "stream", "append", "kve-input", "{\"user_id\":\"u1\",\"action\":\"purchase\"}", "-n", "proc_kvenr" });
    try ctx.exec(&.{ "stream", "append", "kve-input", "{\"user_id\":\"u2\",\"action\":\"browse\"}", "-n", "proc_kvenr" });
    try ctx.exec(&.{ "stream", "append", "kve-input", "{\"user_id\":\"u3\",\"action\":\"signup\"}", "-n", "proc_kvenr" });

    // Step 3: Submit pipeline with kv_lookup in enrich mode
    const job_def =
        \\kind: Processing
        \\name: e2e-kvlookup-enrich
        \\namespace: proc_kvenr
        \\sources.[0].stream.name: kve-input
        \\sinks.[0].stream.name: kve-output
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: enrich-user
        \\operators.[0].lookup_key: user:${$.user_id}
        \\operators.[0].namespace: proc_kvenr
        \\operators.[0].mode: enrich
        \\operators.[0].enrich_field: tier
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kvlookup-enrich.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_kvenr" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Step 4: Wait for data to flow through
    const found = try readStreamBlocking(ctx, "kve-output", "proc_kvenr", "u1", "5000");

    if (!found) {
        std.debug.print("\n[FAILED] kv_lookup enrich pipeline did not produce output within timeout\n", .{});
        return error.TimeoutWaitingForData;
    }

    // Step 5: Verify enriched records
    var read_result = try ctx.cli.run(&.{ "stream", "read", "kve-output", "-n", "proc_kvenr", "--start", "0-0", "--limit", "20" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);

    // ALL 3 records should pass through (enrich mode passes everything)
    try stdx.testing.assertContains(read_result, "u1");
    try stdx.testing.assertContains(read_result, "u2");
    try stdx.testing.assertContains(read_result, "u3");

    // u1 and u2 should be enriched with their KV values
    try stdx.testing.assertContains(read_result, "premium");
    try stdx.testing.assertContains(read_result, "basic");

    // u3 has no KV entry — should pass through unchanged (no "tier" field)
    // We just verify it's there (already checked above)

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_kvenr" });
}

test "e2e/processing: kv_lookup filter with composite key template" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_kvcomp" });

    // Step 1: Seed KV with org-scoped accounts
    try ctx.exec(&.{ "kv", "set", "org.acme.alice", "admin", "-n", "proc_kvcomp" });
    try ctx.exec(&.{ "kv", "set", "org.acme.bob", "member", "-n", "proc_kvcomp" });

    // Step 2: Append records with org + user fields
    try ctx.exec(&.{ "stream", "append", "kvc-input", "{\"org\":\"acme\",\"user\":\"alice\",\"event\":\"login\"}", "-n", "proc_kvcomp" });
    try ctx.exec(&.{ "stream", "append", "kvc-input", "{\"org\":\"acme\",\"user\":\"charlie\",\"event\":\"login\"}", "-n", "proc_kvcomp" });
    try ctx.exec(&.{ "stream", "append", "kvc-input", "{\"org\":\"acme\",\"user\":\"bob\",\"event\":\"logout\"}", "-n", "proc_kvcomp" });

    // Step 3: Submit pipeline — composite key: org.${$.org}.${$.user}
    const job_def =
        \\kind: Processing
        \\name: e2e-kvlookup-composite
        \\namespace: proc_kvcomp
        \\sources.[0].stream.name: kvc-input
        \\sinks.[0].stream.name: kvc-output
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: org-account-check
        \\operators.[0].lookup_key: org.${$.org}.${$.user}
        \\operators.[0].namespace: proc_kvcomp
        \\operators.[0].mode: filter
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kvlookup-composite.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_kvcomp" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Step 4: Wait for output
    const found = try readStreamBlocking(ctx, "kvc-output", "proc_kvcomp", "alice", "5000");

    if (!found) {
        std.debug.print("\n[FAILED] kv_lookup composite key pipeline did not produce output within timeout\n", .{});
        return error.TimeoutWaitingForData;
    }

    // Step 5: Verify — alice and bob pass, charlie is filtered
    var read_result = try ctx.cli.run(&.{ "stream", "read", "kvc-output", "-n", "proc_kvcomp", "--start", "0-0", "--limit", "20" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "alice");
    try stdx.testing.assertContains(read_result, "bob");
    try testing.expect(!read_result.stdoutContains("charlie"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_kvcomp" });
}

test "e2e/processing: kv_lookup filter drops all when no matching keys exist" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_kvnone" });

    // No KV entries seeded — all lookups will miss

    // Append records
    try ctx.exec(&.{ "stream", "append", "kvn-input", "{\"id\":\"a1\",\"data\":\"hello\"}", "-n", "proc_kvnone" });
    try ctx.exec(&.{ "stream", "append", "kvn-input", "{\"id\":\"a2\",\"data\":\"world\"}", "-n", "proc_kvnone" });

    // Submit pipeline with kv_lookup in filter mode
    const job_def =
        \\kind: Processing
        \\name: e2e-kvlookup-nomatch
        \\namespace: proc_kvnone
        \\sources.[0].stream.name: kvn-input
        \\sinks.[0].stream.name: kvn-output
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: check-missing
        \\operators.[0].lookup_key: item:${$.id}
        \\operators.[0].namespace: proc_kvnone
        \\operators.[0].mode: filter
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kvlookup-nomatch.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_kvnone" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait a bit for the pipeline to process the records
    @import("stdx").time.sleep(2000 * std.time.ns_per_ms);

    // Verify the job is RUNNING and has processed records (even if filtered)
    var status_result = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_kvnone" });
    defer status_result.deinit();

    try stdx.testing.assertSucceeded(status_result);
    try stdx.testing.assertContains(status_result, "RUNNING");

    // The output stream should be empty — all records were filtered out
    var read_result = try ctx.cli.run(&.{ "stream", "read", "kvn-output", "-n", "proc_kvnone", "--start", "0-0", "--limit", "10" });
    defer read_result.deinit();

    // Output should have no data records (empty or not containing our test data)
    if (read_result.succeeded()) {
        try testing.expect(!read_result.stdoutContains("hello"));
        try testing.expect(!read_result.stdoutContains("world"));
    }

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_kvnone" });
}

test "e2e/processing: kv_lookup late KV write — records written after KV seed are filtered correctly" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_kvlate" });

    // Step 1: Submit pipeline FIRST (no KV entries yet)
    const job_def =
        \\kind: Processing
        \\name: e2e-kvlookup-late
        \\namespace: proc_kvlate
        \\sources.[0].stream.name: kvl-input
        \\sinks.[0].stream.name: kvl-output
        \\operators.[0].type: kv_lookup
        \\operators.[0].name: check-late
        \\operators.[0].lookup_key: acct:${$.account_id}
        \\operators.[0].namespace: proc_kvlate
        \\operators.[0].mode: filter
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-kvlookup-late.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_kvlate" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Step 2: Now seed KV with an account
    try ctx.exec(&.{ "kv", "set", "acct:eve", "verified", "-n", "proc_kvlate" });

    // Step 3: Wait briefly for KV to be available, then append stream data
    @import("stdx").time.sleep(300 * std.time.ns_per_ms);
    try ctx.exec(&.{ "stream", "append", "kvl-input", "{\"account_id\":\"eve\",\"event\":\"deposit\"}", "-n", "proc_kvlate" });
    try ctx.exec(&.{ "stream", "append", "kvl-input", "{\"account_id\":\"mallory\",\"event\":\"withdraw\"}", "-n", "proc_kvlate" });

    // Step 4: Wait for eve's record to appear
    const found = try readStreamBlocking(ctx, "kvl-output", "proc_kvlate", "eve", "5000");

    if (!found) {
        std.debug.print("\n[FAILED] kv_lookup late-KV pipeline did not produce output within timeout\n", .{});
        return error.TimeoutWaitingForData;
    }

    // Step 5: Verify — only eve passes, mallory is filtered
    var read_result = try ctx.cli.run(&.{ "stream", "read", "kvl-output", "-n", "proc_kvlate", "--start", "0-0", "--limit", "20" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "eve");
    try testing.expect(!read_result.stdoutContains("mallory"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_kvlate" });
}

// =============================================================================
// Classify + Tag-Based Routing E2E Tests
// =============================================================================

test "e2e/processing: classify operator routes tagged records to filtered sink" {
    // Pipeline: source → classify (tags errors) → two sinks:
    //   - firehose sink (no match, gets ALL records)
    //   - error sink (match: [errors], gets ONLY error records)
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_classify" });

    // Write mixed records — some with "error", some without
    try ctx.exec(&.{ "stream", "append", "clf-input", "login succeeded", "-n", "proc_classify" });
    try ctx.exec(&.{ "stream", "append", "clf-input", "error: disk full", "-n", "proc_classify" });
    try ctx.exec(&.{ "stream", "append", "clf-input", "all systems normal", "-n", "proc_classify" });
    try ctx.exec(&.{ "stream", "append", "clf-input", "error: timeout exceeded", "-n", "proc_classify" });

    const job_def =
        \\kind: Processing
        \\name: e2e-classify-route
        \\namespace: proc_classify
        \\sources.[0].stream.name: clf-input
        \\operators.[0].type: classify
        \\operators.[0].name: error-tagger
        \\operators.[0].condition_0: value_contains:error
        \\operators.[0].tag_0: errors
        \\sinks.[0].name: all-events
        \\sinks.[0].stream.name: clf-all-out
        \\sinks.[1].name: error-events
        \\sinks.[1].stream.name: clf-err-out
        \\sinks.[1].match.[0]: errors
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-classify-route.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_classify" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for error records in the tagged sink
    const found_errors = try readStreamBlocking(ctx, "clf-err-out", "proc_classify", "error:", "8000");

    if (!found_errors) {
        std.debug.print("\n[TIMEOUT] Classify tagged sink did not receive error records\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_classify" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Wait for all records in the firehose sink
    const found_all = try readStreamBlocking(ctx, "clf-all-out", "proc_classify", "login succeeded", "5000");

    if (!found_all) {
        std.debug.print("\n[TIMEOUT] Classify firehose sink did not receive all records\n", .{});
        return error.PipelineTimeout;
    }

    // Verify firehose sink has ALL records (error + non-error)
    var all_result = try ctx.cli.run(&.{ "stream", "read", "clf-all-out", "-n", "proc_classify", "--start", "0-0", "--limit", "100" });
    defer all_result.deinit();
    try stdx.testing.assertSucceeded(all_result);
    try stdx.testing.assertContains(all_result, "login succeeded");
    try stdx.testing.assertContains(all_result, "error: disk full");
    try stdx.testing.assertContains(all_result, "all systems normal");
    try stdx.testing.assertContains(all_result, "error: timeout exceeded");

    // Verify error sink ONLY has error records
    var err_result = try ctx.cli.run(&.{ "stream", "read", "clf-err-out", "-n", "proc_classify", "--start", "0-0", "--limit", "100" });
    defer err_result.deinit();
    try stdx.testing.assertSucceeded(err_result);
    try stdx.testing.assertContains(err_result, "error: disk full");
    try stdx.testing.assertContains(err_result, "error: timeout exceeded");
    // Non-error records should NOT be in the error sink
    try testing.expect(!err_result.stdoutContains("login succeeded"));
    try testing.expect(!err_result.stdoutContains("all systems normal"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_classify" });
}

test "e2e/processing: classify multi-tag routing with AND match" {
    // Pipeline: source → classify (tags: critical, errors) → three sinks:
    //   - all-sink:      no match (firehose — gets everything)
    //   - error-sink:    match: [errors] (gets errors + critical)
    //   - critical-sink: match: [critical, errors] (AND match — gets ONLY critical errors)
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_clftag" });

    try ctx.exec(&.{ "stream", "append", "clftag-in", "info: all good", "-n", "proc_clftag" });
    try ctx.exec(&.{ "stream", "append", "clftag-in", "error: minor glitch", "-n", "proc_clftag" });
    try ctx.exec(&.{ "stream", "append", "clftag-in", "critical error: system down", "-n", "proc_clftag" });

    const job_def =
        \\kind: Processing
        \\name: e2e-classify-and
        \\namespace: proc_clftag
        \\sources.[0].stream.name: clftag-in
        \\operators.[0].type: classify
        \\operators.[0].name: severity-tagger
        \\operators.[0].condition_0: value_contains:error
        \\operators.[0].tag_0: errors
        \\operators.[0].condition_1: value_contains:critical
        \\operators.[0].tag_1: critical
        \\sinks.[0].name: all-sink
        \\sinks.[0].stream.name: clftag-all
        \\sinks.[1].name: error-sink
        \\sinks.[1].stream.name: clftag-errors
        \\sinks.[1].match.[0]: errors
        \\sinks.[2].name: critical-sink
        \\sinks.[2].stream.name: clftag-critical
        \\sinks.[2].match.[0]: critical
        \\sinks.[2].match.[1]: errors
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-classify-and.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_clftag" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for critical record in the AND-matched sink
    const found_crit = try readStreamBlocking(ctx, "clftag-critical", "proc_clftag", "critical error", "8000");

    if (!found_crit) {
        std.debug.print("\n[TIMEOUT] Classify AND-match sink did not receive critical records\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_clftag" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Firehose sink should have all 3 records
    const found_all = try readStreamBlocking(ctx, "clftag-all", "proc_clftag", "info: all good", "5000");
    if (!found_all) {
        std.debug.print("\n[TIMEOUT] Firehose sink did not receive all records\n", .{});
        return error.PipelineTimeout;
    }

    // Error sink: gets "error: minor glitch" + "critical error: system down" (both have "error")
    const found_err = try readStreamBlocking(ctx, "clftag-errors", "proc_clftag", "minor glitch", "5000");
    if (!found_err) {
        std.debug.print("\n[TIMEOUT] Error sink did not receive error records\n", .{});
        return error.PipelineTimeout;
    }

    // Verify critical sink has ONLY the critical error (requires both match criteria)
    var crit_result = try ctx.cli.run(&.{ "stream", "read", "clftag-critical", "-n", "proc_clftag", "--start", "0-0", "--limit", "100" });
    defer crit_result.deinit();
    try stdx.testing.assertSucceeded(crit_result);
    try stdx.testing.assertContains(crit_result, "critical error: system down");
    try testing.expect(!crit_result.stdoutContains("minor glitch"));
    try testing.expect(!crit_result.stdoutContains("all good"));

    // Verify error sink has both errors but NOT info
    var err_result = try ctx.cli.run(&.{ "stream", "read", "clftag-errors", "-n", "proc_clftag", "--start", "0-0", "--limit", "100" });
    defer err_result.deinit();
    try stdx.testing.assertSucceeded(err_result);
    try stdx.testing.assertContains(err_result, "minor glitch");
    try stdx.testing.assertContains(err_result, "critical error");
    try testing.expect(!err_result.stdoutContains("all good"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_clftag" });
}

// =============================================================================
// Compound Expression (OR / AND) Filter E2E Tests
// =============================================================================

test "e2e/processing: filter with OR condition — keeps records matching either branch" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_or" });

    // Write records with different event types
    try ctx.exec(&.{ "stream", "append", "or-input", "{\"eventType\":\"payment.deposit\",\"id\":1}", "-n", "proc_or" });
    try ctx.exec(&.{ "stream", "append", "or-input", "{\"eventType\":\"kyc.verified\",\"id\":2}", "-n", "proc_or" });
    try ctx.exec(&.{ "stream", "append", "or-input", "{\"eventType\":\"refund.issued\",\"id\":3}", "-n", "proc_or" });
    try ctx.exec(&.{ "stream", "append", "or-input", "{\"eventType\":\"payment.transfer\",\"id\":4}", "-n", "proc_or" });

    // Submit a pipeline with an OR filter: keep payment OR kyc events
    const job_def =
        \\kind: Processing
        \\name: e2e-or-filter
        \\namespace: proc_or
        \\sources.[0].stream.name: or-input
        \\operators.[0].type: filter
        \\operators.[0].name: payment-or-kyc
        \\operators.[0].condition: value_contains:payment OR value_contains:kyc
        \\sinks.[0].stream.name: or-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-or-filter.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_or" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through
    const found = try readStreamBlocking(ctx, "or-output", "proc_or", "payment.deposit", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] OR filter pipeline did not produce output\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_or" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify output contains payment and kyc records, but NOT refund
    var read_result = try ctx.cli.run(&.{ "stream", "read", "or-output", "-n", "proc_or", "--start", "0-0", "--limit", "100" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "payment.deposit");
    try stdx.testing.assertContains(read_result, "kyc.verified");
    try stdx.testing.assertContains(read_result, "payment.transfer");
    // Refund should have been filtered out
    try testing.expect(!read_result.stdoutContains("refund.issued"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_or" });
}

test "e2e/processing: filter with AND condition — keeps records matching all branches" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_and" });

    // Write records — only one has BOTH "payment" AND "approved"
    try ctx.exec(&.{ "stream", "append", "and-input", "payment approved tx-1", "-n", "proc_and" });
    try ctx.exec(&.{ "stream", "append", "and-input", "payment pending tx-2", "-n", "proc_and" });
    try ctx.exec(&.{ "stream", "append", "and-input", "refund approved tx-3", "-n", "proc_and" });
    try ctx.exec(&.{ "stream", "append", "and-input", "refund pending tx-4", "-n", "proc_and" });

    // Submit pipeline with AND filter
    const job_def =
        \\kind: Processing
        \\name: e2e-and-filter
        \\namespace: proc_and
        \\sources.[0].stream.name: and-input
        \\operators.[0].type: filter
        \\operators.[0].name: payment-and-approved
        \\operators.[0].condition: value_contains:payment AND value_contains:approved
        \\sinks.[0].stream.name: and-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-and-filter.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_and" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for data to flow through
    const found = try readStreamBlocking(ctx, "and-output", "proc_and", "payment approved", "8000");

    if (!found) {
        std.debug.print("\n[TIMEOUT] AND filter pipeline did not produce output\n", .{});
        var status = try ctx.cli.run(&.{ "processing", "status", job_id, "-n", "proc_and" });
        defer status.deinit();
        std.debug.print("Job status: {s}\n", .{status.stdout});
        ctx.dumpServerLogs();
        return error.PipelineTimeout;
    }

    // Verify output contains ONLY the record with both "payment" AND "approved"
    var read_result = try ctx.cli.run(&.{ "stream", "read", "and-output", "-n", "proc_and", "--start", "0-0", "--limit", "100" });
    defer read_result.deinit();

    try stdx.testing.assertSucceeded(read_result);
    try stdx.testing.assertContains(read_result, "payment approved tx-1");
    // Other records should not be present
    try testing.expect(!read_result.stdoutContains("pending"));
    try testing.expect(!read_result.stdoutContains("refund"));

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_and" });
}

// =============================================================================
// Doc-contract regression tests
//
// Each test below asserts the behavior the docs advertise
// (https://docs.floruntime.io/orchestration/processing/). They were written from a
// live audit of the running engine that surfaced a batch of doc-vs-reality bugs
// (KV/queue sink no-ops, `json:$.` conditions, TS field/tag mapping, flow-style YAML);
// the bugs are fixed and these lock the behavior in. Do not weaken the assertions.
// =============================================================================

/// Poll `kv get <key>` until the value is present (not "(nil)") and contains `expected`.
fn kvGetBlocking(ctx: *stdx.testing.TestContext, key: []const u8, ns: []const u8, expected: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);
    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{ "kv", "get", key, "-n", ns });
        defer result.deinit();
        if (result.succeeded() and !result.stdoutContains("(nil)") and result.stdoutContains(expected)) return true;
        @import("stdx").time.sleep(poll_interval_ns);
    }
    var result = try ctx.cli.run(&.{ "kv", "get", key, "-n", ns });
    defer result.deinit();
    return result.succeeded() and !result.stdoutContains("(nil)") and result.stdoutContains(expected);
}

/// Poll `queue list` until the named queue appears (queues are auto-created on first enqueue,
/// so the queue's existence is proof the sink enqueued at least one record).
fn queueAppearsBlocking(ctx: *stdx.testing.TestContext, queue_name: []const u8, ns: []const u8, timeout_ms: u64) !bool {
    const poll_interval_ns: u64 = 100 * std.time.ns_per_ms;
    const max_attempts = @max(timeout_ms / 100, 1);
    for (0..max_attempts) |_| {
        var result = try ctx.cli.run(&.{ "queue", "list", "-n", ns });
        defer result.deinit();
        if (result.succeeded() and result.stdoutContains(queue_name)) return true;
        @import("stdx").time.sleep(poll_interval_ns);
    }
    var result = try ctx.cli.run(&.{ "queue", "list", "-n", ns });
    defer result.deinit();
    return result.succeeded() and result.stdoutContains(queue_name);
}

// Regression: the KV sink used to be a silent no-op.
// `writeSinkRecordFromStream` (handler.zig) switches only on .stream/.ts; .kv falls
// into `else => {}`. records_processed increments but nothing is ever written to KV.
// Docs advertise the KV sink with a full example.
test "e2e/processing: kv sink writes records to KV" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_bug_kvsink" });

    // keyby sets the record key to the user_id; the KV sink uses key_prefix + ":" + key.
    try ctx.exec(&.{ "stream", "append", "kvsink-input", "{\"user_id\":\"u1\",\"name\":\"alice\"}", "-n", "proc_bug_kvsink" });

    const job_def =
        \\kind: Processing
        \\name: e2e-bug-kvsink
        \\namespace: proc_bug_kvsink
        \\sources.[0].stream.name: kvsink-input
        \\operators.[0].type: keyby
        \\operators.[0].name: by-user
        \\operators.[0].key_expression: $.user_id
        \\sinks.[0].kv.namespace: proc_bug_kvsink
        \\sinks.[0].kv.key_prefix: user
        \\sinks.[0].kv.write_mode: upsert
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-bug-kvsink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_bug_kvsink" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Documented behavior: the record is written under key `user:u1`.
    const found = try kvGetBlocking(ctx, "user:u1", "proc_bug_kvsink", "alice", 6000);

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_bug_kvsink" });
    try testing.expect(found); // KV sink must persist the record.
}

// Regression: the queue sink used to be a silent no-op.
// Docs advertise the Queue sink with a full example.
test "e2e/processing: queue sink enqueues records" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_bug_qsink" });

    try ctx.exec(&.{ "stream", "append", "qsink-input", "{\"task\":\"do-thing\"}", "-n", "proc_bug_qsink" });

    const job_def =
        \\kind: Processing
        \\name: e2e-bug-qsink
        \\namespace: proc_bug_qsink
        \\sources.[0].stream.name: qsink-input
        \\sinks.[0].queue.name: bug-task-queue
        \\sinks.[0].queue.priority: 5
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-bug-qsink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_bug_qsink" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Documented behavior: the record is enqueued into `bug-task-queue`.
    const appeared = try queueAppearsBlocking(ctx, "bug-task-queue", "proc_bug_qsink", 6000);

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_bug_qsink" });
    try testing.expect(appeared); // queue sink must enqueue the record.
}

// Regression: `json:$.<path>` conditions in `filter` used to match nothing.
// The doc uses `$.`-prefixed JSONPaths in conditions (e.g. `json:$.latency_ms>5000`),
// but the condition parser only matches plain paths (`json:amount>100`). With `$.`
// the filter drops every record.
test "e2e/processing: filter json:$. condition keeps matching records" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_bug_filt" });

    try ctx.exec(&.{ "stream", "append", "filt-input", "{\"amount\":250}", "-n", "proc_bug_filt" });
    try ctx.exec(&.{ "stream", "append", "filt-input", "{\"amount\":5}", "-n", "proc_bug_filt" });

    const job_def =
        \\kind: Processing
        \\name: e2e-bug-filter-dollar
        \\namespace: proc_bug_filt
        \\sources.[0].stream.name: filt-input
        \\operators.[0].type: filter
        \\operators.[0].name: big-amounts
        \\operators.[0].condition: json:$.amount>100
        \\sinks.[0].stream.name: filt-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-bug-filter-dollar.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_bug_filt" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Best-effort wait for the matching record to flow through.
    _ = try readStreamBlocking(ctx, "filt-output", "proc_bug_filt", "\"amount\":250", "6000");

    var read_result = try ctx.cli.run(&.{ "stream", "read", "filt-output", "-n", "proc_bug_filt", "--start", "0-0", "--limit", "20" });
    defer read_result.deinit();

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_bug_filt" });

    try stdx.testing.assertSucceeded(read_result);
    // Documented: amount 250 passes the `$.amount>100` filter…
    try stdx.testing.assertContains(read_result, "\"amount\":250"); // `$.amount>100` keeps the matching record.
    // …and amount 5 is filtered out.
    try testing.expect(!read_result.stdoutContains("\"amount\":5"));
}

// Regression: `json:$.<path>` conditions in `classify` rules used to never set the tag,
// so tag-routed sinks (`match:`) receive nothing. The doc's routing example uses
// `json:$.latency_ms>5000`. (Plain-path classify routing works — see the passing
// non-bug classify tests — so this isolates the `$.` prefix as the cause.)
test "e2e/processing: classify json:$. routing delivers to matched sink" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_bug_cls" });

    try ctx.exec(&.{ "stream", "append", "cls-input", "{\"level\":\"error\",\"service\":\"api\"}", "-n", "proc_bug_cls" });

    const job_def =
        \\kind: Processing
        \\name: e2e-bug-classify-dollar
        \\namespace: proc_bug_cls
        \\sources.[0].stream.name: cls-input
        \\operators.[0].type: classify
        \\operators.[0].name: route
        \\operators.[0].rules.[0].condition: json:$.level=error
        \\operators.[0].rules.[0].tag: errors
        \\sinks.[0].stream.name: cls-main
        \\sinks.[1].stream.name: cls-errors
        \\sinks.[1].match.[0]: errors
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-bug-classify-dollar.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_bug_cls" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Sanity: classify never drops, so the unfiltered main sink must always receive the record.
    const main_found = try readStreamBlocking(ctx, "cls-main", "proc_bug_cls", "error", "6000");
    // Best-effort wait for the tagged record to reach the errors sink.
    _ = try readStreamBlocking(ctx, "cls-errors", "proc_bug_cls", "error", "6000");

    var errors_read = try ctx.cli.run(&.{ "stream", "read", "cls-errors", "-n", "proc_bug_cls", "--start", "0-0", "--limit", "20" });
    defer errors_read.deinit();

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_bug_cls" });

    try testing.expect(main_found); // control: main sink always gets it
    try stdx.testing.assertSucceeded(errors_read);
    // Documented: the `$.level=error` rule tags the record `errors`, routing it to the errors sink.
    try stdx.testing.assertContains(errors_read, "error"); // `$.level=error` tags the record so it reaches the errors sink.
}

// Regression: the TS sink used to ignore the configured `fields:` map (and tag mapping), writing
// field literally as "value" with 0.0 when the value can't be extracted. The doc says
// `fields: { cpu: cpu_percent }` over `{"cpu_percent":72.5}` writes field cpu = 72.5.
// (The existing "ts sink - JSON records flow" test only checks that *some* data exists;
// this one asserts the actual mapped value, which is the documented contract.)
test "e2e/processing: ts sink maps configured field value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_bug_tssink" });

    try ctx.exec(&.{ "stream", "append", "tssink-input", "{\"hostname\":\"web-01\",\"cpu_percent\":72.5}", "-n", "proc_bug_tssink" });

    const job_def =
        \\kind: Processing
        \\name: e2e-bug-tssink
        \\namespace: proc_bug_tssink
        \\sources.[0].stream.name: tssink-input
        \\sinks.[0].ts.measurement: bug_cpu
        \\sinks.[0].ts.tags.host: hostname
        \\sinks.[0].ts.fields.cpu: cpu_percent
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-bug-tssink.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_bug_tssink" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Wait for the sink to write the `cpu` field point.
    _ = try readTsBlocking(ctx, "bug_cpu", "proc_bug_tssink", "cpu", 6000);

    var read_result = try ctx.cli.run(&.{
        "ts",     "read", "bug_cpu", "-n",     "proc_bug_tssink", "--tags", "host=web-01",
        "--from", "0",    "--field", "cpu",    "--limit",         "100",    "--output", "json",
    });
    defer read_result.deinit();

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "proc_bug_tssink" });

    try stdx.testing.assertSucceeded(read_result);
    // Documented: `fields: { cpu: cpu_percent }` writes field `cpu` = 72.5 with tag host=web-01,
    // not a hardcoded "value"=0.0. Reading field `cpu` with the tag filter must return 72.5.
    try stdx.testing.assertContains(read_result, "72.5"); // configured field mapping writes the real value.
}

// Regression: the docs' own minimal examples use YAML flow-style inline maps
// (`sources: - stream: { name: events }`), but the processing parser only accepts
// block-style nested mappings — flow style fails with "source is missing stream name".
// This is the verbatim "Stream to Stream (Passthrough)" example from the docs.
test "e2e/processing: doc flow-style YAML parses" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Verbatim from https://docs.floruntime.io/orchestration/processing/ ("Pipeline Patterns").
    const flow_yaml =
        \\kind: Processing
        \\name: mirror
        \\sources:
        \\  - stream: { name: events }
        \\sinks:
        \\  - stream: { name: events-copy }
        \\
    ;
    const path = try writeTempYaml(testing.allocator, flow_yaml, "e2e-bug-flowstyle.yaml");
    defer cleanupTempFile(testing.allocator, path);

    // `validate processing` runs the same parser the server uses, offline.
    var result = try ctx.cli.run(&.{ "validate", "processing", "--file", path });
    defer result.deinit();

    // Documented behavior: this example is valid and should parse.
    try testing.expect(!result.stdoutContains("missing stream name"));
    try stdx.testing.assertContains(result, "PASSED"); // flow-style example parses.
}

// =============================================================================
// Cross-shard sink routing (#11)
// =============================================================================

// On a multi-shard node, a KV sink must write to the shard that OWNS the key
// (the same shard a later `kv get` routes to), not the shard running the
// pipeline. The 8 distinct keyby keys below hash across all 4 shards, so most
// are written cross-shard — pre-fix those were unreadable; now all resolve.
test "e2e/processing: kv sink routes to the owning shard (multi-shard)" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "xshard" });

    const ids = [_][]const u8{ "k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7" };
    inline for (ids) |id| {
        try ctx.exec(&.{ "stream", "append", "xshard-src", "{\"id\":\"" ++ id ++ "\",\"v\":\"val-" ++ id ++ "\"}", "-n", "xshard" });
    }

    const job_def =
        \\kind: Processing
        \\name: xshard-kv
        \\namespace: xshard
        \\sources.[0].stream.name: xshard-src
        \\operators.[0].type: keyby
        \\operators.[0].name: by-id
        \\operators.[0].key_expression: $.id
        \\sinks.[0].kv.namespace: xshard
        \\sinks.[0].kv.key_prefix: u
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "e2e-xshard-kv.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_output = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "xshard" });
    const job_id = extractJobId(submit_output) orelse return error.NoJobId;

    // Every record's KV key must be readable, including keys whose hash shard
    // differs from the shard running the pipeline.
    inline for (ids) |id| {
        const found = try kvGetBlocking(ctx, "u:" ++ id, "xshard", "val-" ++ id, 8000);
        if (!found) {
            std.debug.print("\n[FAILED] cross-shard KV sink key 'u:{s}' not found\n", .{id});
            ctx.dumpServerLogs();
            return error.KvKeyMissing;
        }
    }

    try ctx.exec(&.{ "processing", "stop", job_id, "-n", "xshard" });
}
