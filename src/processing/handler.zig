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
const definition = @import("definition.zig");
const SourceKind = definition.SourceKind;
const SinkKind = definition.SinkKind;
const ts_mod = @import("../projection/ts.zig");
const StoredPoint = ts_mod.StoredPoint;

const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const native_registry = @import("operators/native_registry.zig");
const operator_mod = @import("operator.zig");
const Operator = operator_mod.Operator;
const collector_mod = @import("collector.zig");
const OutputCollector = collector_mod.OutputCollector;
const context_mod = @import("context.zig");
const OperatorContext = context_mod.OperatorContext;
const OperatorMetrics = context_mod.OperatorMetrics;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

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

    /// Per-job pipeline execution state (keyed by same job_id as jobs map).
    pipelines: std.StringHashMap(PipelineState),

    const MAX_JOBS: usize = 100_000;

    /// Per-job pipeline state: source/sink config + read cursor + operator chain.
    pub const PipelineState = struct {
        // Source configuration
        source_kind: SourceKind,
        src_measurement: []const u8, // owned (TS source: measurement name)
        src_field: []const u8, // owned (TS source: field name, default "value")
        src_tag_hash: u64, // TS source: tag hash for filtering (0 = no filter)
        src_stream: []const u8, // owned (stream source: stream name)
        src_poll_ms: u32, // poll interval in milliseconds

        // Sink configuration
        sink_kind: SinkKind,
        sink_target: []const u8, // owned (stream/queue sink: target name)
        sink_measurement: []const u8, // owned (TS sink: measurement name)
        sink_value_field: []const u8, // owned (TS sink: value field name)

        // Read cursors — track what's been processed
        ts_cursor_ns: u64, // TS source: next timestamp_ns to read from
        stream_cursor: u64, // Stream source: last offset read
        last_poll_ms: i64, // last time this pipeline was ticked

        // Operator chain — instantiated from job definition
        operators: []Operator = &.{},
        operator_backings: []native_registry.CreateResult = &.{},
    };

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
            .pipelines = std.StringHashMap(PipelineState).init(allocator),
        };
    }

    pub fn deinit(self: *ProcessingHandler) void {
        // Free pipeline state
        var pit = self.pipelines.iterator();
        while (pit.next()) |entry| {
            self.freePipelineState(entry.value_ptr);
        }
        self.pipelines.deinit();

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

    fn freePipelineState(self: *ProcessingHandler, pipe: *PipelineState) void {
        // Free operator chain
        for (pipe.operator_backings) |*backing| {
            backing.deinit(self.allocator);
        }
        if (pipe.operator_backings.len > 0) self.allocator.free(pipe.operator_backings);
        if (pipe.operators.len > 0) self.allocator.free(pipe.operators);

        if (pipe.src_measurement.len > 0) self.allocator.free(pipe.src_measurement);
        if (pipe.src_field.len > 0) self.allocator.free(pipe.src_field);
        if (pipe.src_stream.len > 0) self.allocator.free(pipe.src_stream);
        if (pipe.sink_target.len > 0) self.allocator.free(pipe.sink_target);
        if (pipe.sink_measurement.len > 0) self.allocator.free(pipe.sink_measurement);
        if (pipe.sink_value_field.len > 0) self.allocator.free(pipe.sink_value_field);
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
    /// Uses namespace-qualified hash: hash(namespace \0 job_id)
    fn preRouteByProcessing(req: Request) ?u64 {
        if (req.key.len > 0) {
            return router.hashKeyWithNamespace(req.namespace, req.key);
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

        // Create pipeline execution state from parsed definition
        if (def.primarySource()) |src| {
            if (def.primarySink()) |snk| {
                self.createPipeline(owned_id, src, snk, &def);
            }
        }

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

    // ── Pipeline Execution ──────────────────────────────────────────────

    /// Create a pipeline execution state from parsed source/sink specifications.
    /// Instantiates the operator chain from the job definition.
    fn createPipeline(self: *ProcessingHandler, job_id: []const u8, src: *const definition.SourceSpec, snk: *const definition.SinkSpec, def: *const definition.JobDefinition) void {
        // Compute tag hash for TS source filtering (same algorithm as TSHandler)
        const tag_hash: u64 = if (src.ts_tags.len >= 2) blk: {
            var tag_buf: [1024]u8 = undefined;
            var pos: usize = 0;
            var i: usize = 0;
            while (i + 1 < src.ts_tags.len) : (i += 2) {
                if (pos > 0 and pos < tag_buf.len) {
                    tag_buf[pos] = ',';
                    pos += 1;
                }
                const kv = std.fmt.bufPrint(tag_buf[pos..], "{s}={s}", .{ src.ts_tags[i], src.ts_tags[i + 1] }) catch break;
                pos += kv.len;
            }
            break :blk if (pos > 0) std.hash.Wyhash.hash(0, tag_buf[0..pos]) else 0;
        } else 0;

        // Instantiate operator chain from definition
        const op_specs = def.operators.items;
        var ops = self.allocator.alloc(Operator, op_specs.len) catch {
            // Fall through — pipeline will operate with no operators (passthrough)
            self.pipelines.put(job_id, makePipeState(self.allocator, src, snk, tag_hash)) catch {};
            return;
        };
        var backings = self.allocator.alloc(native_registry.CreateResult, op_specs.len) catch {
            self.allocator.free(ops);
            self.pipelines.put(job_id, makePipeState(self.allocator, src, snk, tag_hash)) catch {};
            return;
        };

        var count: usize = 0;
        for (op_specs) |*spec| {
            if (!native_registry.isNativeType(spec.type_name)) continue;
            const result = native_registry.create(self.allocator, spec) catch continue;
            backings[count] = result;
            ops[count] = result.op;
            count += 1;
        }

        var pipe_state = makePipeState(self.allocator, src, snk, tag_hash);
        if (count > 0) {
            pipe_state.operators = ops[0..count];
            pipe_state.operator_backings = backings[0..count];
        } else {
            self.allocator.free(ops);
            self.allocator.free(backings);
        }
        self.pipelines.put(job_id, pipe_state) catch {};
    }

    /// Helper to build a PipelineState with source/sink config (no operators).
    fn makePipeState(allocator: Allocator, src: *const definition.SourceSpec, snk: *const definition.SinkSpec, tag_hash: u64) PipelineState {
        return .{
            .source_kind = src.kind,
            .src_measurement = allocator.dupe(u8, src.ts_measurement) catch "",
            .src_field = allocator.dupe(u8, if (src.ts_field.len > 0) src.ts_field else "value") catch "",
            .src_tag_hash = tag_hash,
            .src_stream = allocator.dupe(u8, src.stream) catch "",
            .src_poll_ms = if (src.ts_poll_interval_ms > 0) src.ts_poll_interval_ms else 1000,
            .sink_kind = snk.kind,
            .sink_target = allocator.dupe(u8, snk.target) catch "",
            .sink_measurement = allocator.dupe(u8, snk.ts_measurement) catch "",
            .sink_value_field = allocator.dupe(u8, if (snk.ts_value_field.len > 0) snk.ts_value_field else "value") catch "",
            .ts_cursor_ns = 0,
            .stream_cursor = 0,
            .last_poll_ms = 0,
        };
    }

    /// Drive all running pipelines: poll sources, push to sinks.
    /// Called from Shard.tick() on each reactor iteration.
    pub fn tickPipelines(self: *ProcessingHandler, shard: *Shard) void {
        const now_ms = std.time.milliTimestamp();

        var it = self.pipelines.iterator();
        while (it.next()) |entry| {
            const job_id = entry.key_ptr.*;
            const pipe = entry.value_ptr;

            // Only run for RUNNING jobs
            const job = self.jobs.getPtr(job_id) orelse continue;
            if (job.status != .running) continue;

            // Enforce poll interval
            const poll_ms: i64 = @intCast(pipe.src_poll_ms);
            if (now_ms - pipe.last_poll_ms < poll_ms) continue;
            pipe.last_poll_ms = now_ms;

            switch (pipe.source_kind) {
                .ts => tickTsSource(pipe, shard, job),
                .stream => tickStreamSource(pipe, shard, job),
            }
        }
    }

    /// Tick a TS source pipeline: read points from TSProjection, apply operators, write to sink.
    fn tickTsSource(pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        var point_buf: [256]StoredPoint = undefined;
        const result = shard.ts_handler.ts.queryRange(
            pipe.src_measurement,
            pipe.src_field,
            pipe.ts_cursor_ns,
            std.math.maxInt(u64),
            &point_buf,
        ) catch return;

        const points = point_buf[0..result.points_in_buffer];
        if (points.len == 0) return;

        for (points) |pt| {
            // Filter by tag hash if specified
            if (pipe.src_tag_hash != 0 and pt.tag_hash != pipe.src_tag_hash) continue;

            // Format TS point as JSON record
            var json_buf: [512]u8 = undefined;
            const ts_ms: i64 = @intCast(pt.timestamp_ns / 1_000_000);
            const json = std.fmt.bufPrint(&json_buf, "{{\"measurement\":\"{s}\",\"value\":{},\"timestamp_ms\":{d}}}", .{ pipe.src_measurement, pt.field_value, ts_ms }) catch continue;

            // Apply operator chain (or pass through directly)
            const output_records = applyOperatorChain(pipe.operators, json, ts_ms, shard.allocator) catch null;

            if (output_records) |records| {
                defer shard.allocator.free(records);
                for (records) |rec| {
                    writeSinkRecord(pipe, shard, rec.value, pt.field_value, pt.timestamp_ns, pt.tag_hash);
                }
            } else {
                // No operators or chain failed — direct passthrough
                writeSinkRecord(pipe, shard, json, pt.field_value, pt.timestamp_ns, pt.tag_hash);
            }

            // Advance cursor past this point
            if (pt.timestamp_ns >= pipe.ts_cursor_ns) {
                pipe.ts_cursor_ns = pt.timestamp_ns + 1;
            }

            job.records_processed += 1;
        }
    }

    /// Tick a stream source pipeline: read payloads from stream, apply operators, write to sink.
    fn tickStreamSource(pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        const payloads = shard.stream_handler.readPayloads(pipe.stream_cursor + 1, 100);
        if (payloads.len == 0) return;
        defer shard.stream_handler.allocator.free(payloads);

        for (payloads) |payload| {
            const output_records = applyOperatorChain(pipe.operators, payload, std.time.milliTimestamp(), shard.allocator) catch null;

            if (output_records) |records| {
                defer shard.allocator.free(records);
                for (records) |rec| {
                    writeSinkRecordFromStream(pipe, shard, rec.value);
                }
            } else {
                writeSinkRecordFromStream(pipe, shard, payload);
            }

            pipe.stream_cursor += 1;
            job.records_processed += 1;
        }
    }

    /// Apply the operator chain to a single input value.
    /// Returns owned slice of output records (caller must free), or null if no operators.
    fn applyOperatorChain(operators: []Operator, value: []const u8, event_time_ms: i64, allocator: Allocator) !?[]const ProcessingRecord {
        if (operators.len == 0) return null;

        // Set up collector + context for the chain
        var collector = OutputCollector.init(allocator);
        defer collector.deinit();
        var metrics = OperatorMetrics{};
        var ctx = OperatorContext{
            .collector = &collector,
            .metrics = &metrics,
            .allocator = allocator,
            .current_processing_time_ms = std.time.milliTimestamp(),
            .current_watermark_ms = event_time_ms,
            .operator_name = "",
        };

        // Feed the initial record into the first operator
        const input_rec = ProcessingRecord.fromValue(value, event_time_ms);
        try operators[0].processElement(input_rec, &ctx);

        // Chain: each operator processes the output of the previous one
        var i: usize = 1;
        while (i < operators.len) : (i += 1) {
            // Snapshot current output before clearing
            const prev_count = collector.count();
            const prev_output = collector.drain();
            // Dupe the records since clear will allow the backing to be reused
            const staged = allocator.dupe(ProcessingRecord, prev_output[0..prev_count]) catch return null;
            defer allocator.free(staged);
            collector.clear();
            for (staged) |rec| {
                try operators[i].processElement(rec, &ctx);
            }
        }

        // Return final output — must dupe since collector.deinit() frees the backing array
        const final = collector.drain();
        if (final.len == 0) {
            return &.{};
        }
        return allocator.dupe(ProcessingRecord, final) catch &.{};
    }

    /// Write a processed record to the configured sink (TS source variant).
    fn writeSinkRecord(pipe: *PipelineState, shard: *Shard, payload: []const u8, field_value: f64, timestamp_ns: u64, tag_hash: u64) void {
        switch (pipe.sink_kind) {
            .stream => {
                _ = shard.stream_handler.appendPayload(payload) catch return;
            },
            .ts => {
                const ual_idx = shard.ts_handler.nextUalIndex();
                shard.ts_handler.ts.insert(
                    pipe.sink_measurement,
                    pipe.sink_value_field,
                    field_value,
                    timestamp_ns,
                    ual_idx,
                    tag_hash,
                ) catch return;
            },
            else => {},
        }
    }

    /// Write a processed record to the configured sink (stream source variant).
    fn writeSinkRecordFromStream(pipe: *PipelineState, shard: *Shard, payload: []const u8) void {
        switch (pipe.sink_kind) {
            .stream => {
                _ = shard.stream_handler.appendPayload(payload) catch return;
            },
            .ts => {
                const value = std.fmt.parseFloat(f64, payload) catch 0.0;
                const ual_idx = shard.ts_handler.nextUalIndex();
                const now_ns: u64 = @intCast(@as(u64, @bitCast(std.time.milliTimestamp())) * 1_000_000);
                shard.ts_handler.ts.insert(
                    pipe.sink_measurement,
                    pipe.sink_value_field,
                    value,
                    now_ns,
                    ual_idx,
                    0,
                ) catch return;
            },
            else => {},
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

test "ProcessingHandler: applyOperatorChain with no operators returns null" {
    const result = try ProcessingHandler.applyOperatorChain(&.{}, "hello", 100, std.testing.allocator);
    try std.testing.expect(result == null);
}

test "ProcessingHandler: applyOperatorChain with passthrough operator" {
    const allocator = std.testing.allocator;

    // Create a passthrough operator via native_registry
    var spec = definition.OperatorSpec{
        .type_name = "passthrough",
        .name = "test-pass",
        .module = "",
        .config = null,
    };
    var create_result = try native_registry.create(allocator, &spec);
    defer create_result.deinit(allocator);

    var ops = [_]Operator{create_result.op};
    const result = try ProcessingHandler.applyOperatorChain(&ops, "test-value", 42, allocator);
    try std.testing.expect(result != null);
    const records = result.?;
    defer allocator.free(records);

    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("test-value", records[0].value);
    try std.testing.expectEqual(@as(i64, 42), records[0].event_time_ms);
}

test "ProcessingHandler: applyOperatorChain with filter operator rejects" {
    const allocator = std.testing.allocator;

    // Create a filter operator with "key_not_empty" condition — since our records
    // have an empty key (fromValue), the filter should reject them.
    const config_entries = [_]definition.OperatorSpec.ConfigEntry{
        .{ .key = "condition", .value = "key_not_empty" },
    };
    var spec = definition.OperatorSpec{
        .type_name = "filter",
        .name = "test-filter",
        .module = "",
        .config = &config_entries,
    };
    var create_result = try native_registry.create(allocator, &spec);
    defer create_result.deinit(allocator);

    // Feed a record with no key — filter should reject it
    var ops = [_]Operator{create_result.op};
    const result = try ProcessingHandler.applyOperatorChain(&ops, "some-data", 0, allocator);
    try std.testing.expect(result != null);
    const records = result.?;
    defer allocator.free(records);
    // key_not_empty filter rejects records with empty key
    try std.testing.expectEqual(@as(usize, 0), records.len);
}
