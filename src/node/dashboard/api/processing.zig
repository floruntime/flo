//! Dashboard API — Processing Endpoints
//!
//! All processing job REST API endpoints, mirroring the 8 binary protocol
//! commands (submit, stop, cancel, status, list, savepoint, restore, rescale).
//!
//! Endpoints:
//!   GET    /processing/jobs                    — List processing jobs
//!   GET    /processing/jobs/:id                — Get job detail
//!   POST   /processing/jobs                    — Submit new job (YAML body)
//!   POST   /processing/jobs/:id/stop           — Stop a running job
//!   POST   /processing/jobs/:id/cancel         — Cancel a job
//!   POST   /processing/jobs/:id/savepoint      — Create a savepoint
//!   POST   /processing/jobs/:id/restore        — Restore from savepoint
//!   POST   /processing/jobs/:id/rescale        — Change parallelism

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const h = @import("helpers.zig");
const Dispatcher = h.Dispatcher;
const Method = @import("../../../util/http/mod.zig").Method;

// =============================================================================
// Router
// =============================================================================

/// Handle all `/processing/...` requests.
///
/// Path is the portion after "processing/" — e.g. "jobs", "jobs/proc-001",
/// "jobs/proc-001/stop".
pub fn handleProcessingRequest(
    allocator: Allocator,
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    body: []const u8,
    dispatchers: []*Dispatcher,
) ![]const u8 {
    _ = query_string;

    // GET/POST /processing/jobs
    if (std.mem.eql(u8, path, "jobs")) {
        if (method == .POST) {
            return try submitJob(allocator, body, dispatchers);
        }
        return try listJobs(allocator, dispatchers);
    }

    // /processing/jobs/... — extract job_id and optional sub-resource
    if (std.mem.startsWith(u8, path, "jobs/")) {
        const rest = path["jobs/".len..];

        if (std.mem.indexOf(u8, rest, "/")) |slash_idx| {
            const job_id = rest[0..slash_idx];
            const sub = rest[slash_idx + 1 ..];

            if (std.mem.eql(u8, sub, "stop")) {
                return try stopJob(allocator, job_id, dispatchers);
            } else if (std.mem.eql(u8, sub, "cancel")) {
                return try cancelJob(allocator, job_id, dispatchers);
            } else if (std.mem.eql(u8, sub, "savepoint")) {
                return try createSavepoint(allocator, job_id, dispatchers);
            } else if (std.mem.eql(u8, sub, "restore")) {
                return try restoreJob(allocator, job_id, body, dispatchers);
            } else if (std.mem.eql(u8, sub, "rescale")) {
                return try rescaleJob(allocator, job_id, body, dispatchers);
            } else {
                return try h.jsonError(allocator, "Unknown processing sub-resource");
            }
        } else {
            // GET /processing/jobs/:id
            return try getJobDetail(allocator, rest, dispatchers);
        }
    }

    return try h.jsonError(allocator, "Unknown processing endpoint");
}

// =============================================================================
// GET /processing/jobs — list all processing jobs
// =============================================================================

fn listJobs(allocator: Allocator, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    const result = dispatchers[0].dispatch(.{ .processing_list = .{
        .namespace = "default",
        .limit = 100,
        .cursor = null,
    } }, 0, 0, null) catch {
        return try allocator.dupe(u8, "[]");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_list_result => |jobs_data| {
                return try allocator.dupe(u8, jobs_data.data);
            },
            .err => return try allocator.dupe(u8, "[]"),
            else => return try allocator.dupe(u8, "[]"),
        }
    }
    return try allocator.dupe(u8, "[]");
}

// =============================================================================
// GET /processing/jobs/:id — get full job detail
// =============================================================================

fn getJobDetail(allocator: Allocator, job_id: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const result = dispatchers[0].dispatch(.{ .processing_status = .{
        .namespace = "default",
        .job_id = job_id,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to get job status");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_status_result => |status_data| {
                return try allocator.dupe(u8, status_data.data);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "Job not found");
}

// =============================================================================
// POST /processing/jobs — submit a new processing job
// =============================================================================

fn submitJob(allocator: Allocator, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");
    if (body.len == 0) return try h.jsonError(allocator, "Request body (YAML definition) is required");

    const result = dispatchers[0].dispatch(.{ .processing_submit = .{
        .namespace = "default",
        .definition_yaml = body,
        .job_id = null,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to submit processing job");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_submitted => |submitted| {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"job_id":"{s}","state":"CREATED"}}
                , .{submitted.job_id});
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}

// =============================================================================
// POST /processing/jobs/:id/stop — stop a running job
// =============================================================================

fn stopJob(allocator: Allocator, job_id: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const result = dispatchers[0].dispatch(.{ .processing_stop = .{
        .namespace = "default",
        .job_id = job_id,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to stop job");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_stopped => {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"ok":true,"job_id":"{s}","state":"STOPPED"}}
                , .{job_id});
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}

// =============================================================================
// POST /processing/jobs/:id/cancel — cancel a job
// =============================================================================

fn cancelJob(allocator: Allocator, job_id: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const result = dispatchers[0].dispatch(.{ .processing_cancel = .{
        .namespace = "default",
        .job_id = job_id,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to cancel job");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_cancelled => {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"ok":true,"job_id":"{s}","state":"CANCELLED"}}
                , .{job_id});
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}

// =============================================================================
// POST /processing/jobs/:id/savepoint — create a savepoint
// =============================================================================

fn createSavepoint(allocator: Allocator, job_id: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const result = dispatchers[0].dispatch(.{ .processing_savepoint = .{
        .namespace = "default",
        .job_id = job_id,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to create savepoint");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_savepoint_result => |sp| {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"ok":true,"job_id":"{s}","savepoint_id":"{s}"}}
                , .{ job_id, sp.savepoint_id });
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}

// =============================================================================
// POST /processing/jobs/:id/restore — restore from savepoint
// =============================================================================

fn restoreJob(allocator: Allocator, job_id: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    // Parse optional savepoint_id from JSON body
    var savepoint_id: []const u8 = "";
    if (body.len > 0) {
        // Simple extraction: look for "savepoint_id":"..."
        if (std.mem.indexOf(u8, body, "\"savepoint_id\"")) |_| {
            if (std.mem.indexOf(u8, body, ":\"")) |colon_quote| {
                const start = colon_quote + 2;
                if (std.mem.indexOfScalarPos(u8, body, start, '"')) |end_quote| {
                    savepoint_id = body[start..end_quote];
                }
            }
        }
    }

    // Use a fallback savepoint_id if none provided
    if (savepoint_id.len == 0) {
        savepoint_id = "latest";
    }

    const result = dispatchers[0].dispatch(.{ .processing_restore = .{
        .namespace = "default",
        .job_id = job_id,
        .savepoint_id = savepoint_id,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to restore job");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_restored => {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"ok":true,"job_id":"{s}","state":"RUNNING"}}
                , .{job_id});
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}

// =============================================================================
// POST /processing/jobs/:id/rescale — change parallelism
// =============================================================================

fn rescaleJob(allocator: Allocator, job_id: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    // Parse parallelism from JSON body: {"parallelism": N}
    var parallelism: u32 = 0;
    if (body.len > 0) {
        if (std.mem.indexOf(u8, body, "\"parallelism\"")) |_| {
            // Simple number extraction after ":"
            if (std.mem.indexOf(u8, body, "parallelism")) |key_pos| {
                const after_key = body[key_pos + "parallelism".len ..];
                // Skip `":` or `" :`
                var i: usize = 0;
                while (i < after_key.len and (after_key[i] == '"' or after_key[i] == ':' or after_key[i] == ' ')) : (i += 1) {}
                // Parse digits
                var val: u32 = 0;
                while (i < after_key.len and after_key[i] >= '0' and after_key[i] <= '9') : (i += 1) {
                    val = val * 10 + @as(u32, after_key[i] - '0');
                }
                parallelism = val;
            }
        }
    }

    if (parallelism == 0) {
        return try h.jsonError(allocator, "parallelism must be >= 1");
    }

    const result = dispatchers[0].dispatch(.{ .processing_rescale = .{
        .namespace = "default",
        .job_id = job_id,
        .parallelism = parallelism,
    } }, 0, 0, null) catch {
        return try h.jsonError(allocator, "Failed to rescale job");
    };

    if (result) |res| {
        defer res.deinit(allocator);
        switch (res) {
            .processing_rescaled => {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const writer = buf.writer(allocator);
                try writer.print(
                    \\{{"ok":true,"job_id":"{s}","parallelism":{d}}}
                , .{ job_id, parallelism });
                return try buf.toOwnedSlice(allocator);
            },
            .err => |e| return try h.jsonError(allocator, e.message),
            else => return try h.jsonError(allocator, "Unexpected response"),
        }
    }
    return try h.jsonError(allocator, "No response from processing handler");
}
