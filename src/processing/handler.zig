//! Processing Handler — registers stream processing opcodes with Dispatcher.
//!
//! Stream processing is a Layer 2 "Intelligent Layer" that composes Layer 1
//! primitives for continuous data pipelines (Flink-inspired). The handler
//! manages job lifecycle: submit, stop, cancel, status, list, savepoint,
//! restore, and rescale.
//!
//! ## Opcode Range
//!
//!   Commands:   0xC0–0xC8
//!   Responses:  0xC9–0xD1
//!
//! ## Wire Format
//!
//! Each command uses the standard Request format (namespace, key, value):
//!
//! | Command              | key        | value                          |
//! |----------------------|------------|--------------------------------|
//! | processing_submit    | (unused)   | YAML job definition            |
//! | processing_stop      | job_id     | (empty)                        |
//! | processing_cancel    | job_id     | (empty)                        |
//! | processing_status    | job_id     | (empty)                        |
//! | processing_list      | (unused)   | [limit:u32][cursor...]?        |
//! | processing_savepoint | job_id     | (empty)                        |
//! | processing_restore   | job_id     | savepoint_id                   |
//! | processing_rescale   | job_id     | [parallelism:u32]              |

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const parser = @import("parser.zig");

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;

// ═══════════════════════════════════════════════════════════════════════════════
// ProcessingHandler
// ═══════════════════════════════════════════════════════════════════════════════

pub const ProcessingHandler = struct {
    allocator: Allocator,

    /// In-memory job store: job_id → JobRecord.
    jobs: std.StringHashMap(JobRecord),

    /// Monotonic job counter for generating IDs.
    next_job_id: u64,

    /// Savepoint store: savepoint_id → SavepointRecord.
    savepoints: std.StringHashMap(SavepointRecord),

    /// Monotonic savepoint counter.
    next_savepoint_id: u64,

    const MAX_JOBS: usize = 100_000;

    pub const JobStatus = enum(u8) {
        running = 0,
        stopped = 1,
        cancelled = 2,
        failed = 3,
        completed = 4,

        pub fn toString(self: JobStatus) []const u8 {
            return switch (self) {
                .running => "RUNNING",
                .stopped => "STOPPED",
                .cancelled => "CANCELLED",
                .failed => "FAILED",
                .completed => "COMPLETED",
            };
        }
    };

    pub const JobRecord = struct {
        job_id_owned: []const u8,
        name_owned: []const u8,
        namespace_owned: []const u8,
        status: JobStatus,
        parallelism: u32,
        batch_size: u32,
        yaml_owned: []const u8,
        created_at_ms: i64,
        records_processed: u64,
    };

    pub const SavepointRecord = struct {
        savepoint_id_owned: []const u8,
        job_id_owned: []const u8,
        created_at_ms: i64,
        records_at_savepoint: u64,
    };

    pub fn init(allocator: Allocator) ProcessingHandler {
        return .{
            .allocator = allocator,
            .jobs = std.StringHashMap(JobRecord).init(allocator),
            .next_job_id = 1,
            .savepoints = std.StringHashMap(SavepointRecord).init(allocator),
            .next_savepoint_id = 1,
        };
    }

    pub fn deinit(self: *ProcessingHandler) void {
        // Free all job records
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            self.freeJobRecord(entry.value_ptr);
        }
        self.jobs.deinit();

        // Free all savepoint records
        var sit = self.savepoints.iterator();
        while (sit.next()) |entry| {
            self.allocator.free(entry.value_ptr.savepoint_id_owned);
            self.allocator.free(entry.value_ptr.job_id_owned);
        }
        self.savepoints.deinit();
    }

    fn freeJobRecord(self: *ProcessingHandler, job: *JobRecord) void {
        self.allocator.free(job.job_id_owned);
        self.allocator.free(job.name_owned);
        self.allocator.free(job.namespace_owned);
        self.allocator.free(job.yaml_owned);
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.processing_submit, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_stop, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_cancel, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_status, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_list, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_savepoint, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_restore, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_rescale, dispatchProcessing, preRouteByProcessing);
    }

    /// Pre-routing: hash on key (job_id) for deterministic shard assignment.
    fn preRouteByProcessing(req: Request) ?u64 {
        if (req.key.len > 0) {
            return std.hash.Wyhash.hash(0, req.key);
        }
        // For submit/list with empty key, use namespace
        return 0;
    }

    fn dispatchProcessing(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        shard.processing_handler.handleCommand(shard, conn, req);
    }

    // ── Command Routing ─────────────────────────────────────────────────

    pub fn handleCommand(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const op: OpCode = @enumFromInt(req.header.op_code);
        switch (op) {
            .processing_submit => self.handleSubmit(shard, conn, req),
            .processing_stop => self.handleStop(shard, conn, req),
            .processing_cancel => self.handleCancel(shard, conn, req),
            .processing_status => self.handleStatus(shard, conn, req),
            .processing_list => self.handleList(shard, conn, req),
            .processing_savepoint => self.handleSavepoint(shard, conn, req),
            .processing_restore => self.handleRestore(shard, conn, req),
            .processing_rescale => self.handleRescale(shard, conn, req),
            else => {
                shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "unknown processing opcode");
            },
        }
    }

    // ── Submit ───────────────────────────────────────────────────────────

    fn handleSubmit(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const yaml = req.value;
        if (yaml.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job definition is required");
            return;
        }

        // Parse the YAML/JSON definition
        var def = parser.parseJobDefinition(self.allocator, yaml) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "invalid job definition");
            return;
        };
        defer def.deinit(self.allocator);

        // Generate a unique job ID
        const job_id_num = self.next_job_id;
        self.next_job_id += 1;

        var id_buf: [64]u8 = undefined;
        const job_id = std.fmt.bufPrint(&id_buf, "job-{d}", .{job_id_num}) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "id generation failed");
            return;
        };

        // Duplicate all owned data
        const owned_id = self.allocator.dupe(u8, job_id) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_id);

        const owned_name = self.allocator.dupe(u8, def.name) catch {
            self.allocator.free(owned_id);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_name);

        const owned_namespace = self.allocator.dupe(u8, def.namespace) catch {
            self.allocator.free(owned_id);
            self.allocator.free(owned_name);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        errdefer self.allocator.free(owned_namespace);

        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(owned_id);
            self.allocator.free(owned_name);
            self.allocator.free(owned_namespace);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };

        const now = std.time.milliTimestamp();

        const record = JobRecord{
            .job_id_owned = owned_id,
            .name_owned = owned_name,
            .namespace_owned = owned_namespace,
            .status = .running,
            .parallelism = def.parallelism,
            .batch_size = def.batch_size,
            .yaml_owned = owned_yaml,
            .created_at_ms = now,
            .records_processed = 0,
        };

        self.jobs.put(owned_id, record) catch {
            self.allocator.free(owned_id);
            self.allocator.free(owned_name);
            self.allocator.free(owned_namespace);
            self.allocator.free(owned_yaml);
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "storage failed");
            return;
        };

        // Return the job ID
        shard.sendOkResponse(conn, req.header.request_id, job_id);
    }

    // ── Stop ─────────────────────────────────────────────────────────────

    fn handleStop(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.jobs.getPtr(job_id)) |job| {
            job.status = .stopped;
            shard.sendOkResponse(conn, req.header.request_id, "");
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── Cancel ───────────────────────────────────────────────────────────

    fn handleCancel(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.jobs.getPtr(job_id)) |job| {
            job.status = .cancelled;
            shard.sendOkResponse(conn, req.header.request_id, "");
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── Status ───────────────────────────────────────────────────────────

    fn handleStatus(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.jobs.get(job_id)) |job| {
            // Build JSON status response
            var buf: [4096]u8 = undefined;
            const json = std.fmt.bufPrint(&buf,
                \\{{"job_id":"{s}","name":"{s}","status":"{s}","parallelism":{d},"batch_size":{d},"records_processed":{d},"created_at_ms":{d}}}
            , .{
                job.job_id_owned,
                job.name_owned,
                job.status.toString(),
                job.parallelism,
                job.batch_size,
                job.records_processed,
                job.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
                return;
            };
            shard.sendOkResponse(conn, req.header.request_id, json);
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── List ─────────────────────────────────────────────────────────────

    fn handleList(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const data = req.value;

        // Parse limit from wire format: [limit:u32][cursor...]?
        var limit: u32 = 100;
        if (data.len >= 4) {
            limit = std.mem.readInt(u32, data[0..4], .little);
        }
        if (limit == 0) limit = 100;

        // Build JSON array of jobs
        var response_buf: [65536]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&response_buf);
        const writer = fbs.writer();

        writer.writeByte('[') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
            return;
        };

        var count: u32 = 0;
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            if (count >= limit) break;

            if (count > 0) {
                writer.writeByte(',') catch {
                    shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
                    return;
                };
            }

            const job = entry.value_ptr;
            std.fmt.format(writer,
                \\{{"job_id":"{s}","name":"{s}","status":"{s}","parallelism":{d},"created_at_ms":{d}}}
            , .{
                job.job_id_owned,
                job.name_owned,
                job.status.toString(),
                job.parallelism,
                job.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
                return;
            };

            count += 1;
        }

        writer.writeByte(']') catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "serialization failed");
            return;
        };

        const written = fbs.getWritten();
        shard.sendOkResponse(conn, req.header.request_id, written);
    }

    // ── Savepoint ────────────────────────────────────────────────────────

    fn handleSavepoint(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.jobs.get(job_id)) |job| {
            // Generate savepoint ID
            const sp_num = self.next_savepoint_id;
            self.next_savepoint_id += 1;

            var id_buf: [64]u8 = undefined;
            const sp_id = std.fmt.bufPrint(&id_buf, "sp-{d}", .{sp_num}) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "id generation failed");
                return;
            };

            const owned_sp_id = self.allocator.dupe(u8, sp_id) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
            errdefer self.allocator.free(owned_sp_id);

            const owned_job_id = self.allocator.dupe(u8, job_id) catch {
                self.allocator.free(owned_sp_id);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };

            const now = std.time.milliTimestamp();

            self.savepoints.put(owned_sp_id, .{
                .savepoint_id_owned = owned_sp_id,
                .job_id_owned = owned_job_id,
                .created_at_ms = now,
                .records_at_savepoint = job.records_processed,
            }) catch {
                self.allocator.free(owned_sp_id);
                self.allocator.free(owned_job_id);
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "storage failed");
                return;
            };

            shard.sendOkResponse(conn, req.header.request_id, sp_id);
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── Restore ──────────────────────────────────────────────────────────

    fn handleRestore(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        const savepoint_id = req.value;

        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (savepoint_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "savepoint_id is required");
            return;
        }

        // Check job exists
        if (self.jobs.getPtr(job_id)) |job| {
            // Check savepoint exists
            if (self.savepoints.get(savepoint_id)) |sp| {
                // Verify the savepoint belongs to this job
                if (!std.mem.eql(u8, sp.job_id_owned, job_id)) {
                    shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "savepoint does not belong to this job");
                    return;
                }

                // Restore: reset job state to savepoint
                job.status = .running;
                job.records_processed = sp.records_at_savepoint;
                shard.sendOkResponse(conn, req.header.request_id, "");
            } else {
                shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
            }
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── Rescale ──────────────────────────────────────────────────────────

    fn handleRescale(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        const data = req.value;

        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (data.len < 4) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "parallelism value is required");
            return;
        }

        const parallelism = std.mem.readInt(u32, data[0..4], .little);
        if (parallelism == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "parallelism must be at least 1");
            return;
        }

        if (self.jobs.getPtr(job_id)) |job| {
            job.parallelism = parallelism;
            shard.sendOkResponse(conn, req.header.request_id, "");
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Unit Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "ProcessingHandler: all opcodes are registered" {
    const Dispatch = @import("../node/dispatcher.zig").Dispatcher;
    var d = Dispatch.init();
    ProcessingHandler.register(&d);

    // Verify all 8 processing opcodes are registered
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_submit)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_stop)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_cancel)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_status)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_list)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_savepoint)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_restore)] != null);
    try std.testing.expect(d.handlers[@intFromEnum(proto.OpCode.processing_rescale)] != null);
}

test "ProcessingHandler: init and deinit" {
    const handler = ProcessingHandler.init(std.testing.allocator);
    var h = handler;
    h.deinit();
}
