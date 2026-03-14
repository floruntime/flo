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

const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const EntryType = entry_mod.EntryType;
const Flags = entry_mod.Flags;

const native_registry = @import("operators/native_registry.zig");
const kv_lookup_mod = @import("operators/kv_lookup.zig");
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

    /// Sink configuration for a single sink in the pipeline.
    pub const SinkConfig = struct {
        kind: SinkKind,
        target: []const u8, // owned (stream/queue sink: target name)
        namespace: []const u8, // owned (sink namespace)
        measurement: []const u8, // owned (TS sink: measurement name)
        value_field: []const u8, // owned (TS sink: value field name)
        ts_tag_keys: []const u8, // owned (serialized flat pairs)
        ts_field_keys: []const u8, // owned (serialized flat pairs)
        required_tags: u32, // tag bitmask — 0 = firehose (receives all records)
    };

    /// Per-job pipeline state: source/sink config + read cursor + operator chain.
    pub const PipelineState = struct {
        // Source configuration
        source_kind: SourceKind,
        src_measurement: []const u8, // owned (TS source: measurement name)
        src_field: []const u8, // owned (TS source: field name, default "value")
        src_tag_hash: u64, // TS source: tag hash for filtering (0 = no filter)
        src_stream: []const u8, // owned (stream source: stream name)
        src_namespace: []const u8, // owned (source namespace, default "default")
        src_poll_ms: u32, // poll interval in milliseconds

        // Multi-sink configuration (tag-based routing)
        sinks: []SinkConfig = &.{},

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
        if (pipe.src_namespace.len > 0) self.allocator.free(pipe.src_namespace);

        // Free multi-sink configs
        for (pipe.sinks) |snk| {
            if (snk.target.len > 0) self.allocator.free(snk.target);
            if (snk.namespace.len > 0) self.allocator.free(snk.namespace);
            if (snk.measurement.len > 0) self.allocator.free(snk.measurement);
            if (snk.value_field.len > 0) self.allocator.free(snk.value_field);
            if (snk.ts_tag_keys.len > 0) self.allocator.free(snk.ts_tag_keys);
            if (snk.ts_field_keys.len > 0) self.allocator.free(snk.ts_field_keys);
        }
        if (pipe.sinks.len > 0) self.allocator.free(pipe.sinks);
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.processing_submit, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_stop, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_cancel, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_status, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWalk(.processing_list, dispatchProcessing, localScanJobs);
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

    /// ShardWalker LocalScanFn for processing_list — returns job names
    /// from one shard's ProcessingHandler registry.
    fn localScanJobs(
        ctx: *anyopaque,
        namespace: []const u8,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        const S = struct {
            threadlocal var name_buf: [1024][]const u8 = undefined;
        };

        const req_ns = if (namespace.len > 0) namespace else "default";
        var count: usize = 0;
        var it = handler.jobs.iterator();
        while (it.next()) |entry| {
            if (count >= S.name_buf.len) break;
            const job = entry.value_ptr;
            if (!std.mem.eql(u8, job.namespace_owned, req_ns)) continue;
            S.name_buf[count] = job.name_owned;
            count += 1;
        }

        return .{ .items = S.name_buf[0..count], .next_cursor = null };
    }

    fn dispatchProcessing(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const op: OpCode = @enumFromInt(req.header.op_code);
        shard.processing_handler.handleCommand(shard, conn, req);
        if (op == .processing_submit) {
            shard.namespace_handler.markNamespaceHasData(req.namespace);
        }
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

        // Parse the YAML/JSON definition, using the request namespace as fallback
        const req_ns: ?[]const u8 = if (req.namespace.len > 0) req.namespace else null;
        var def = parser.parseJobDefinitionWithNamespace(self.allocator, yaml, req_ns) catch {
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

        // Persist through Raft → UAL so the job survives restart.
        // Value format: [status:u8][parallelism:u32][batch_size:u32][created_at_ms:i64][yaml...]
        self.persistSubmit(shard, req.namespace, owned_id, &record);

        // Build tag registry from all sinks and classify operators
        var tag_registry = definition.TagRegistry{};
        for (def.sinks.items) |snk| {
            if (snk.match) |tag_list| {
                for (tag_list) |tag| _ = tag_registry.getOrCreate(tag);
            }
        }
        for (def.operators.items) |op_spec| {
            if (std.mem.eql(u8, op_spec.type_name, "classify")) {
                if (op_spec.config) |entries| {
                    for (entries) |entry| {
                        if (std.mem.startsWith(u8, entry.key, "tag_")) {
                            _ = tag_registry.getOrCreate(entry.value);
                        }
                    }
                }
            }
        }

        // Resolve required_tags bitmask for each sink
        for (def.sinks.items) |*snk| {
            if (snk.match) |tag_list| {
                snk.required_tags = tag_registry.buildMask(tag_list);
            }
        }

        // Create pipeline execution state from parsed definition.
        // For multi-source jobs, create one pipeline per source (all sinks per pipeline).
        if (def.sinks.items.len > 0) {
            for (def.sources.items, 0..) |*src, idx| {
                if (idx == 0) {
                    self.createPipeline(owned_id, src, def.sinks.items, &def, &tag_registry, shard);
                } else {
                    // Multi-source: create additional pipeline with suffixed key
                    var key_buf: [256]u8 = undefined;
                    const ms_key = std.fmt.bufPrint(&key_buf, "{s}\x00{d}", .{ owned_id, idx }) catch continue;
                    const ms_owned = self.allocator.dupe(u8, ms_key) catch continue;
                    self.createPipeline(ms_owned, src, def.sinks.items, &def, &tag_registry, shard);
                }
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
            self.persistStatusChange(shard, req.namespace, job_id, .stopped);
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
            self.persistStatusChange(shard, req.namespace, job_id, .cancelled);
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

        // Resolve effective namespace for filtering
        const req_ns = if (req.namespace.len > 0) req.namespace else "default";

        var count: u32 = 0;
        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            if (count >= limit) break;

            // Filter: only include jobs belonging to the requested namespace
            const job_ns = entry.value_ptr.namespace_owned;
            if (!std.mem.eql(u8, job_ns, req_ns)) continue;

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

            // Persist savepoint through Raft → UAL
            self.persistSavepoint(shard, req.namespace, owned_sp_id, job_id, job.records_processed, now);

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
            self.persistRescale(shard, req.namespace, job_id, parallelism);
            shard.sendOkResponse(conn, req.header.request_id, "");
        } else {
            shard.sendErrorResponse(conn, req.header.request_id, .not_found, "");
        }
    }

    // ── UAL Persistence ─────────────────────────────────────────────────

    /// Persist a processing_submit entry. Key = job_id.
    /// Value format: [status:u8][parallelism:u32][batch_size:u32][created_at_ms:i64][ns_len:u16][namespace][yaml...]
    fn persistSubmit(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, job_id: []const u8, job: *const JobRecord) void {
        _ = self;
        var value_buf: [persistence_mod.MAX_PERSIST_PAYLOAD]u8 = undefined;
        var off: usize = 0;

        value_buf[off] = @intFromEnum(job.status);
        off += 1;
        std.mem.writeInt(u32, value_buf[off..][0..4], job.parallelism, .little);
        off += 4;
        std.mem.writeInt(u32, value_buf[off..][0..4], job.batch_size, .little);
        off += 4;
        std.mem.writeInt(i64, value_buf[off..][0..8], job.created_at_ms, .little);
        off += 8;

        // Embed the effective namespace so replay can recover it without re-parsing quirks
        const ns = job.namespace_owned;
        const ns_len: u16 = @intCast(ns.len);
        std.mem.writeInt(u16, value_buf[off..][0..2], ns_len, .little);
        off += 2;
        if (off + ns.len > value_buf.len) return;
        @memcpy(value_buf[off .. off + ns.len], ns);
        off += ns.len;

        const yaml = job.yaml_owned;
        if (off + yaml.len > value_buf.len) return;
        @memcpy(value_buf[off .. off + yaml.len], yaml);
        off += yaml.len;

        _ = persistence_mod.persistEntry(shard, .processing_submit, Flags.NONE, namespace, job_id, value_buf[0..off]) catch {};
    }

    /// Persist a processing_stop or processing_cancel entry. Key = job_id, value = [new_status:u8].
    fn persistStatusChange(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, job_id: []const u8, status: JobStatus) void {
        _ = self;
        const entry_type: EntryType = switch (status) {
            .stopped => .processing_stop,
            .cancelled => .processing_cancel,
            else => return,
        };
        const value = &[_]u8{@intFromEnum(status)};
        _ = persistence_mod.persistEntry(shard, entry_type, Flags.NONE, namespace, job_id, value) catch {};
    }

    /// Persist a processing_savepoint entry. Key = savepoint_id.
    /// Value format: [job_id_len:u16][job_id][records_at:u64][created_at_ms:i64]
    fn persistSavepoint(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, sp_id: []const u8, job_id: []const u8, records_at: u64, created_at_ms: i64) void {
        _ = self;
        var value_buf: [512]u8 = undefined;
        var off: usize = 0;

        std.mem.writeInt(u16, value_buf[off..][0..2], @intCast(job_id.len), .little);
        off += 2;
        @memcpy(value_buf[off .. off + job_id.len], job_id);
        off += job_id.len;
        std.mem.writeInt(u64, value_buf[off..][0..8], records_at, .little);
        off += 8;
        std.mem.writeInt(i64, value_buf[off..][0..8], created_at_ms, .little);
        off += 8;

        _ = persistence_mod.persistEntry(shard, .processing_savepoint, Flags.NONE, namespace, sp_id, value_buf[0..off]) catch {};
    }

    /// Persist a processing_rescale entry. Key = job_id, value = [parallelism:u32].
    fn persistRescale(self: *ProcessingHandler, shard: *Shard, namespace: []const u8, job_id: []const u8, parallelism: u32) void {
        _ = self;
        var value_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, value_buf[0..4], parallelism, .little);
        _ = persistence_mod.persistEntry(shard, .processing_rescale, Flags.NONE, namespace, job_id, &value_buf) catch {};
    }

    // ── Replay ──────────────────────────────────────────────────────────

    /// Register this handler's entry types with the shared ReplayRegistry.
    pub fn registerReplay(self: *ProcessingHandler, registry: *persistence_mod.ReplayRegistry) void {
        registry.register(.processing_submit, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_stop, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_cancel, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_savepoint, @ptrCast(self), replayEntryThunk);
        registry.register(.processing_rescale, @ptrCast(self), replayEntryThunk);
    }

    fn replayEntryThunk(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        self.replayEntry(entry);
    }

    /// Replay a persisted processing entry to rebuild in-memory state.
    pub fn replayEntry(self: *ProcessingHandler, entry: *const entry_mod.Entry) void {
        const etype: EntryType = @enumFromInt(entry.header.entry_type);
        const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;

        switch (etype) {
            .processing_submit => self.replaySubmit(cmd.key, cmd.value),
            .processing_stop => self.replayStatusChange(cmd.key, .stopped),
            .processing_cancel => self.replayStatusChange(cmd.key, .cancelled),
            .processing_savepoint => self.replaySavepoint(cmd.key, cmd.value),
            .processing_rescale => self.replayRescale(cmd.key, cmd.value),
            else => {},
        }
    }

    /// Replay a processing_submit entry. Rebuilds JobRecord from persisted bytes.
    /// Value format: [status:u8][parallelism:u32][batch_size:u32][created_at_ms:i64][ns_len:u16][namespace][yaml...]
    fn replaySubmit(self: *ProcessingHandler, key: []const u8, value: []const u8) void {
        if (value.len < 19) return; // 1+4+4+8+2 minimum
        const job_id = key;

        var off: usize = 0;
        const status: JobStatus = @enumFromInt(value[off]);
        off += 1;
        const parallelism = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        const batch_size = std.mem.readInt(u32, value[off..][0..4], .little);
        off += 4;
        const created_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);
        off += 8;

        // Read embedded namespace
        const ns_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + ns_len > value.len) return;
        const ns_raw = value[off .. off + ns_len];
        off += ns_len;

        const yaml = value[off..];

        // Extract name from YAML by re-parsing
        var def = parser.parseJobDefinition(self.allocator, yaml) catch return;
        const name = self.allocator.dupe(u8, def.name) catch {
            def.deinit(self.allocator);
            return;
        };
        def.deinit(self.allocator);

        // Use the embedded namespace (authoritative from original submit)
        const namespace = self.allocator.dupe(u8, ns_raw) catch {
            self.allocator.free(name);
            return;
        };

        const owned_id = self.allocator.dupe(u8, job_id) catch {
            self.allocator.free(name);
            self.allocator.free(namespace);
            return;
        };
        const owned_yaml = self.allocator.dupe(u8, yaml) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(namespace);
            return;
        };

        // Remove old entry if exists (idempotent replay)
        if (self.jobs.fetchRemove(owned_id)) |old| {
            self.freeJobRecord(@constCast(&old.value));
        }

        self.jobs.put(owned_id, .{
            .job_id_owned = owned_id,
            .name_owned = name,
            .namespace_owned = namespace,
            .status = status,
            .parallelism = parallelism,
            .batch_size = batch_size,
            .yaml_owned = owned_yaml,
            .created_at_ms = created_at_ms,
            .records_processed = 0,
        }) catch {
            self.allocator.free(owned_id);
            self.allocator.free(name);
            self.allocator.free(namespace);
            self.allocator.free(owned_yaml);
            return;
        };

        // Update next_job_id counter to avoid collisions
        // Parse numeric suffix from "job-N"
        if (std.mem.startsWith(u8, job_id, "job-")) {
            const num = std.fmt.parseInt(u64, job_id[4..], 10) catch 0;
            if (num >= self.next_job_id) self.next_job_id = num + 1;
        }
    }

    /// Replay a status change (stop/cancel).
    fn replayStatusChange(self: *ProcessingHandler, key: []const u8, status: JobStatus) void {
        if (self.jobs.getPtr(key)) |job| {
            job.status = status;
        }
    }

    /// Replay a savepoint entry.
    /// Value format: [job_id_len:u16][job_id][records_at:u64][created_at_ms:i64]
    fn replaySavepoint(self: *ProcessingHandler, key: []const u8, value: []const u8) void {
        if (value.len < 2) return;
        var off: usize = 0;

        const job_id_len = std.mem.readInt(u16, value[off..][0..2], .little);
        off += 2;
        if (off + job_id_len + 16 > value.len) return;
        const job_id = value[off .. off + job_id_len];
        off += job_id_len;
        const records_at = std.mem.readInt(u64, value[off..][0..8], .little);
        off += 8;
        const created_at_ms = std.mem.readInt(i64, value[off..][0..8], .little);

        const owned_sp_id = self.allocator.dupe(u8, key) catch return;
        const owned_job_id = self.allocator.dupe(u8, job_id) catch {
            self.allocator.free(owned_sp_id);
            return;
        };

        // Remove old entry if exists (idempotent replay)
        if (self.savepoints.fetchRemove(owned_sp_id)) |old| {
            self.allocator.free(old.value.savepoint_id_owned);
            self.allocator.free(old.value.job_id_owned);
        }

        self.savepoints.put(owned_sp_id, .{
            .savepoint_id_owned = owned_sp_id,
            .job_id_owned = owned_job_id,
            .created_at_ms = created_at_ms,
            .records_at_savepoint = records_at,
        }) catch {
            self.allocator.free(owned_sp_id);
            self.allocator.free(owned_job_id);
        };

        // Update next_savepoint_id to avoid collisions
        if (std.mem.startsWith(u8, key, "sp-")) {
            const num = std.fmt.parseInt(u64, key[3..], 10) catch 0;
            if (num >= self.next_savepoint_id) self.next_savepoint_id = num + 1;
        }
    }

    /// Replay a rescale entry. Value = [parallelism:u32].
    fn replayRescale(self: *ProcessingHandler, key: []const u8, value: []const u8) void {
        if (value.len < 4) return;
        const parallelism = std.mem.readInt(u32, value[0..4], .little);
        if (self.jobs.getPtr(key)) |job| {
            job.parallelism = parallelism;
        }
    }

    // ── Pipeline Execution ──────────────────────────────────────────────

    /// Create a pipeline execution state from parsed source/sink specifications.
    /// Instantiates the operator chain from the job definition.
    fn createPipeline(self: *ProcessingHandler, job_id: []const u8, src: *const definition.SourceSpec, sinks: []const definition.SinkSpec, def: *const definition.JobDefinition, tag_registry: *const definition.TagRegistry, shard: *Shard) void {
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
            self.pipelines.put(job_id, makePipeState(self.allocator, src, sinks, tag_hash)) catch {};
            return;
        };
        var backings = self.allocator.alloc(native_registry.CreateResult, op_specs.len) catch {
            self.allocator.free(ops);
            self.pipelines.put(job_id, makePipeState(self.allocator, src, sinks, tag_hash)) catch {};
            return;
        };

        var count: usize = 0;
        for (op_specs) |*spec| {
            if (!native_registry.isNativeType(spec.type_name)) continue;
            const result = native_registry.create(self.allocator, spec, tag_registry) catch continue;
            // Wire kv_lookup operators to the shard's KV projection
            if (result.backing == .kv_lookup) {
                result.backing.kv_lookup.setLookupFn(shardKvLookup, @ptrCast(shard));
            }
            backings[count] = result;
            ops[count] = result.op;
            count += 1;
        }

        var pipe_state = makePipeState(self.allocator, src, sinks, tag_hash);
        if (count > 0) {
            pipe_state.operators = ops[0..count];
            pipe_state.operator_backings = backings[0..count];
        } else {
            self.allocator.free(ops);
            self.allocator.free(backings);
        }
        self.pipelines.put(job_id, pipe_state) catch {};
    }

    /// KV lookup callback passed to KvLookupOperator.
    /// Resolves a key against the shard's KV projection, namespace-qualified.
    fn shardKvLookup(ctx: *anyopaque, namespace: []const u8, key: []const u8) ?[]const u8 {
        const shard: *Shard = @ptrCast(@alignCast(ctx));
        const ns_handler = @import("../namespace/handler.zig");
        var qbuf: [ns_handler.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = ns_handler.qualifyKey(&qbuf, namespace, key) catch return null;
        const entry = shard.kv_handler.*.kv.get(qkey) orelse return null;
        return entry.value;
    }

    /// Helper to build a PipelineState with source/sink config (no operators).
    fn makePipeState(allocator: Allocator, src: *const definition.SourceSpec, sinks: []const definition.SinkSpec, tag_hash: u64) PipelineState {
        // Build SinkConfig array from all sink specs
        const sink_configs: []SinkConfig = if (sinks.len > 0)
            allocator.alloc(SinkConfig, sinks.len) catch @as([]SinkConfig, &.{})
        else
            @as([]SinkConfig, &.{});

        for (sink_configs, 0..) |*sc, i| {
            const snk = sinks[i];
            sc.* = .{
                .kind = snk.kind,
                .target = allocator.dupe(u8, snk.target) catch "",
                .namespace = allocator.dupe(u8, if (snk.namespace.len > 0) snk.namespace else "default") catch "",
                .measurement = allocator.dupe(u8, snk.ts_measurement) catch "",
                .value_field = allocator.dupe(u8, if (snk.ts_value_field.len > 0) snk.ts_value_field else "value") catch "",
                .ts_tag_keys = serializeFlatPairs(allocator, snk.ts_tag_keys) catch "",
                .ts_field_keys = serializeFlatPairs(allocator, snk.ts_field_keys) catch "",
                .required_tags = snk.required_tags,
            };
        }

        return .{
            .source_kind = src.kind,
            .src_measurement = allocator.dupe(u8, src.ts_measurement) catch "",
            .src_field = allocator.dupe(u8, if (src.ts_field.len > 0) src.ts_field else "value") catch "",
            .src_tag_hash = tag_hash,
            .src_stream = allocator.dupe(u8, src.stream) catch "",
            .src_namespace = allocator.dupe(u8, if (src.namespace.len > 0) src.namespace else "default") catch "",
            .src_poll_ms = if (src.ts_poll_interval_ms > 0) src.ts_poll_interval_ms else 1000,
            .sinks = sink_configs,
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
            const pipeline_key = entry.key_ptr.*;
            const pipe = entry.value_ptr;

            // Resolve base job ID (strip multi-source suffix "\0idx" if present)
            const base_job_id = if (std.mem.indexOfScalar(u8, pipeline_key, 0)) |nul_pos|
                pipeline_key[0..nul_pos]
            else
                pipeline_key;

            // Only run for RUNNING jobs
            const job = self.jobs.getPtr(base_job_id) orelse continue;
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
                    writeSinkRecord(pipe, shard, rec.value, pt.field_value, pt.timestamp_ns, pt.tag_hash, rec.tags);
                }
            } else {
                // No operators or chain failed — direct passthrough
                writeSinkRecord(pipe, shard, json, pt.field_value, pt.timestamp_ns, pt.tag_hash, 0);
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
        // Read from the named source stream using namespace-qualified name-hash filtering
        const result = shard.stream_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, pipe.stream_cursor + 1, 100);
        if (result.payloads.len == 0) {
            // Still advance cursor so we don't re-scan the same empty range
            if (result.next_offset > pipe.stream_cursor + 1) {
                pipe.stream_cursor = result.next_offset - 1;
            }
            return;
        }
        defer shard.stream_handler.allocator.free(result.payloads);

        for (result.payloads) |payload| {
            const output_records = applyOperatorChain(pipe.operators, payload, std.time.milliTimestamp(), shard.allocator) catch null;

            if (output_records) |records| {
                defer shard.allocator.free(records);
                for (records) |rec| {
                    writeSinkRecordFromStream(pipe, shard, rec.value, rec.tags);
                }
            } else {
                writeSinkRecordFromStream(pipe, shard, payload, 0);
            }

            job.records_processed += 1;
        }

        // Advance cursor past the scanned range
        if (result.next_offset > pipe.stream_cursor) {
            pipe.stream_cursor = result.next_offset - 1;
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

    /// Write a processed record to all matching sinks (TS source variant).
    /// Tag filtering: sinks with required_tags=0 (firehose) get all records;
    /// sinks with required_tags set only get records where record_tags matches.
    fn writeSinkRecord(pipe: *PipelineState, shard: *Shard, payload: []const u8, field_value: f64, timestamp_ns: u64, tag_hash: u64, record_tags: u32) void {
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            switch (snk.kind) {
                .stream => {
                    _ = shard.stream_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch continue;
                },
                .ts => {
                    const ual_idx = shard.ts_handler.nextUalIndex();
                    shard.ts_handler.ts.insert(
                        snk.measurement,
                        snk.value_field,
                        field_value,
                        timestamp_ns,
                        ual_idx,
                        tag_hash,
                    ) catch continue;
                },
                else => {},
            }
        }
    }

    /// Write a processed record to all matching sinks (stream source variant).
    fn writeSinkRecordFromStream(pipe: *PipelineState, shard: *Shard, payload: []const u8, record_tags: u32) void {
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            switch (snk.kind) {
                .stream => {
                    _ = shard.stream_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch continue;
                },
                .ts => {
                    // Extract value from JSON payload using the sink's value_field as source key.
                    const value = extractJsonFloat(payload, snk.value_field) orelse
                        std.fmt.parseFloat(f64, payload) catch 0.0;
                    const ual_idx = shard.ts_handler.nextUalIndex();
                    const now_ns: u64 = @intCast(@as(u64, @bitCast(std.time.milliTimestamp())) * 1_000_000);
                    const tag_hash_val = extractJsonTagHash(payload, snk.measurement);
                    shard.ts_handler.ts.insert(
                        snk.measurement,
                        "value",
                        value,
                        now_ns,
                        ual_idx,
                        tag_hash_val,
                    ) catch continue;
                },
                else => {},
            }
        }
    }

    /// Extract a float value from a JSON payload by field name.
    /// Handles the TS sink's value_field extraction (e.g., "cpu_percent" from {"cpu_percent":72.5}).
    fn extractJsonFloat(payload: []const u8, field_name: []const u8) ?f64 {
        if (field_name.len == 0) return null;

        // Build the search pattern: "field_name":
        var search_buf: [256]u8 = undefined;
        const pattern = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{field_name}) catch return null;

        const idx = std.mem.indexOf(u8, payload, pattern) orelse return null;
        const after = payload[idx + pattern.len ..];

        // Skip whitespace
        var start: usize = 0;
        while (start < after.len and (after[start] == ' ' or after[start] == '\t')) : (start += 1) {}
        if (start >= after.len) return null;

        // Find end of number
        var end: usize = start;
        while (end < after.len and (after[end] == '-' or after[end] == '.' or
            (after[end] >= '0' and after[end] <= '9') or after[end] == 'e' or after[end] == 'E' or after[end] == '+')) : (end += 1)
        {}

        if (end <= start) return null;
        return std.fmt.parseFloat(f64, after[start..end]) catch null;
    }

    /// Extract a tag hash from JSON payload for TS sink writes.
    /// This handles the case where sink.ts.tags maps output tag names to JSON fields.
    fn extractJsonTagHash(_: []const u8, _: []const u8) u64 {
        // For now, return 0 — tag extraction will be enhanced in phase 2+
        // The TS source variant already handles tag_hash from the source side
        return 0;
    }

    /// Serialize flat pairs ([]const []const u8) to a single owned string.
    /// Pairs are NUL-separated: "key1\0val1\0key2\0val2\0"
    fn serializeFlatPairs(allocator: Allocator, pairs: []const []const u8) ![]const u8 {
        if (pairs.len == 0) return "";
        var total: usize = 0;
        for (pairs) |s| total += s.len + 1;
        const buf = try allocator.alloc(u8, total);
        var pos: usize = 0;
        for (pairs) |s| {
            @memcpy(buf[pos .. pos + s.len], s);
            pos += s.len;
            buf[pos] = 0;
            pos += 1;
        }
        return buf;
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
    var create_result = try native_registry.create(allocator, &spec, null);
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
    var create_result = try native_registry.create(allocator, &spec, null);
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
