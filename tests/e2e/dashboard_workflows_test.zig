//! Dashboard Workflow API E2E Tests (issue #25)
//!
//! The dashboard's create-definition and signal endpoints existed as routes
//! but returned a `"status":"not_wired"` stub. These verify they now perform
//! the real loopback writes.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

const dottedToYaml = stdx.testing.dottedToYaml;
const writeDottedToTempYaml = stdx.testing.writeDottedToTempYaml;
const cleanupTempFile = stdx.testing.cleanupTempFile;

test "e2e/dashboard: workflow create defines a workflow" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Real nested YAML (the dashboard receives the raw body).
    const def =
        \\kind: Workflow
        \\name: dash-wf
        \\version: 1.0.0
        \\start.run: @actions/validate
        \\start.transition.success: flo.Completed
        \\start.transition.failure: flo.Failed
    ;
    const yaml = try dottedToYaml(testing.allocator, def);
    defer testing.allocator.free(yaml);

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // POST the definition, then confirm it actually lands in the definitions
    // list. Retried because this branch predates the dashboard body-read fix
    // (#33): a cold-connection POST can drop the body.
    var defined = false;
    var attempt: usize = 0;
    while (attempt < 8) : (attempt += 1) {
        var r = try http.post("/api/v1/workflow/definitions", yaml);
        r.deinit();
        var list = try http.get("/api/v1/workflow/definitions");
        defer list.deinit();
        if (std.mem.indexOf(u8, list.body, "dash-wf") != null) {
            defined = true;
            break;
        }
        @import("stdx").time.sleep(100 * std.time.ns_per_ms);
    }
    try testing.expect(defined);
}

test "e2e/dashboard: workflow signal reaches a running run" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Define + start a run via the CLI (reliable). The start action has no
    // worker, so the run stays RUNNING and is signalable.
    const def =
        \\kind: Workflow
        \\name: sig-wf
        \\version: 1.0.0
        \\start.run: @actions/approval-action
        \\start.transition.success: flo.Completed
        \\start.transition.failure: flo.Failed
    ;
    const path = try writeDottedToTempYaml(testing.allocator, def, "sig-wf.yaml");
    defer cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "workflow", "create", "-f", path });
    try ctx.exec(&.{ "workflow", "start", "sig-wf", "{}", "--run-id", "sig-run-1" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // Signal type via query, no request body → not subject to the body-read
    // race, so a single call is reliable. Was a "not_wired" stub.
    var resp = try http.post("/api/v1/workflow/runs/sig-run-1/signal?signal=approval", "");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"ok\":true") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "not_wired") == null);
}
