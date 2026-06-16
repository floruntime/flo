//! Dashboard Processing API E2E Tests (issue #27)
//!
//! Verifies the dashboard surfaces REAL per-job processing metrics
//! (throughput, input/output, latency, watermark, saturation) instead of only
//! `records_processed`.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const cleanupTempFile = stdx.testing.cleanupTempFile;

fn extractJobId(output: []const u8) ?[]const u8 {
    const prefix = "Job submitted: ";
    const idx = std.mem.indexOf(u8, output, prefix) orelse return null;
    const start = idx + prefix.len;
    var end = start;
    while (end < output.len and output[end] != '\n' and output[end] != '\r') end += 1;
    return if (end > start) output[start..end] else null;
}

test "e2e/dashboard: processing job surfaces real runtime metrics" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "proc_metrics" });

    const job_def =
        \\kind: Processing
        \\name: metrics-job
        \\namespace: proc_metrics
        \\sources.[0].stream.name: m-input
        \\sinks.[0].stream.name: m-output
        \\parallelism: 1
        \\batch_size: 100
    ;
    const path = try writeDottedToTempYaml(testing.allocator, job_def, "metrics-job.yaml");
    defer cleanupTempFile(testing.allocator, path);

    const submit_out = try ctx.execCapture(&.{ "processing", "submit", path, "-n", "proc_metrics" });
    const job_id = extractJobId(std.mem.trim(u8, submit_out, &std.ascii.whitespace)) orelse return error.NoJobId;

    // Feed the source stream.
    for (0..20) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "{{\"v\":{d}}}", .{i}) catch unreachable;
        try ctx.exec(&.{ "stream", "append", "m-input", msg, "-n", "proc_metrics" });
    }

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    const url = try std.fmt.allocPrint(testing.allocator, "/api/v1/processing/jobs/{s}", .{job_id});
    defer testing.allocator.free(url);

    // Poll until the pipeline has consumed at least one record (metrics go live).
    var processed = false;
    var attempt: usize = 0;
    while (attempt < 40) : (attempt += 1) {
        @import("stdx").time.sleep(200 * std.time.ns_per_ms);
        var resp = try http.get(url);
        defer resp.deinit();
        if (resp.status == 200 and std.mem.indexOf(u8, resp.body, "\"records_processed\":0") == null and
            std.mem.indexOf(u8, resp.body, "\"records_processed\"") != null)
        {
            processed = true;
            break;
        }
    }
    try testing.expect(processed);

    // Final read: the metrics object is present and reflects the running job.
    var detail = try http.get(url);
    defer detail.deinit();
    try testing.expectEqual(@as(u16, 200), detail.status);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"metrics\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"running\":true") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"throughput_per_sec\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"output_per_sec\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"latency_avg_ms\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"watermark_ms\"") != null);
    // Records actually flowed end to end (input consumed, output emitted).
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"records_in\":0") == null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"records_out\":0") == null);
}
