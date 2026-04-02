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
const log = @import("stdx").log;
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
const run_id_mod = @import("../node/run_id.zig");
const stream_handler_mod = @import("../stream/handler.zig");
const kv_handler_mod = @import("../kv/handler.zig");
const StreamID = @import("../projection/stream.zig").StreamID;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;

const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const EntryType = entry_mod.EntryType;
const Flags = entry_mod.Flags;

const KafkaSource = @import("../kafka/source.zig").KafkaSource;
const KafkaSourceConfig = @import("../kafka/source.zig").KafkaSourceConfig;
const KafkaStartOffset = @import("../kafka/source.zig").StartOffset;
const SourceVTable = @import("endpoints/source.zig").Source;
const StreamElement = @import("record.zig").StreamElement;

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

    /// Savepoint store: savepoint_id → SavepointRecord.
    savepoints: std.StringHashMap(SavepointRecord),

    /// Per-job pipeline execution state (keyed by same job_id as jobs map).
    pipelines: std.StringHashMap(PipelineState),

    /// Cross-shard stream handlers — one per shard, wired by Runtime.
    /// Enables pipelines to read/write streams on the shard that owns them.
    peer_stream_handlers: ?[]const *stream_handler_mod.StreamHandler = null,
    peer_shard_count: u16 = 1,
    peer_partition_count: u32 = 0,

    /// Cross-shard KV handlers — one per shard, wired by Runtime.
    /// Enables KV lookup operators to find keys on any shard.
    peer_kv_handlers: ?[]const *kv_handler_mod.KVHandler = null,

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
        stream_cursor_ts: u64, // Stream source: last StreamID timestamp_ms read
        stream_cursor_seq: u64, // Stream source: last StreamID sequence read
        last_poll_ms: i64, // last time this pipeline was ticked

        // Operator chain — instantiated from job definition
        operators: []Operator = &.{},
        operator_backings: []native_registry.CreateResult = &.{},

        // Tag registry — heap-allocated, used by classify operators for tag resolution
        tag_registry: ?*definition.TagRegistry = null,

        // External source (Kafka, etc.) — null for Flo-internal sources
        external_source: ?SourceVTable = null,
        external_source_handle: ?*KafkaSource = null,
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
            .savepoints = std.StringHashMap(SavepointRecord).init(allocator),
            .pipelines = std.StringHashMap(PipelineState).init(allocator),
        };
    }

    pub fn deinit(self: *ProcessingHandler) void {
        // Free pipeline state + multi-source pipeline keys
        var pit = self.pipelines.iterator();
        while (pit.next()) |entry| {
            self.freePipelineState(entry.value_ptr);
            // Multi-source pipeline keys (containing '\0' separator) are independently
            // allocated and must be freed here. The idx==0 key is shared with the jobs
            // map and freed via freeJobRecord below.
            const key = entry.key_ptr.*;
            if (std.mem.indexOfScalar(u8, key, 0) != null) {
                self.allocator.free(key);
            }
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

    /// Wire cross-shard stream handler references.
    /// Called by Runtime after all shards are initialized.
    pub fn setPeerStreamHandlers(self: *ProcessingHandler, handlers: []const *stream_handler_mod.StreamHandler, partition_count: u32, shard_count: u16) void {
        self.peer_stream_handlers = handlers;
        self.peer_partition_count = partition_count;
        self.peer_shard_count = shard_count;
    }

    /// Wire cross-shard KV handler references.
    /// Called by Runtime after all shards are initialized.
    pub fn setPeerKvHandlers(self: *ProcessingHandler, handlers: []const *kv_handler_mod.KVHandler) void {
        self.peer_kv_handlers = handlers;
    }

    /// Resolve the correct StreamHandler for a given stream name + namespace.
    /// Uses hashKeyWithNamespace (namespace + key) to match the Dispatcher's
    /// stream_append routing — the Dispatcher uses preRouteByStream which
    /// hashes with hashKeyWithNamespace(namespace, key), so stream data lives
    /// on the shard selected by that composite hash.
    fn resolveStreamHandler(self: *ProcessingHandler, local: *stream_handler_mod.StreamHandler, stream_name: []const u8, namespace: []const u8) *stream_handler_mod.StreamHandler {
        const peers = self.peer_stream_handlers orelse return local;
        if (peers.len <= 1) return local;
        const hash = router.hashKeyWithNamespace(namespace, stream_name);
        const partition_id: u32 = @intCast(hash % self.peer_partition_count);
        const shard_id: u16 = @intCast(partition_id % self.peer_shard_count);
        return peers[shard_id];
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

        // Free heap-allocated tag registry
        if (pipe.tag_registry) |reg| self.allocator.destroy(reg);

        // Free external source (Kafka, etc.)
        if (pipe.external_source_handle) |ks| ks.deinit();
    }

    // ── Dispatcher Registration ─────────────────────────────────────────

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.processing_submit, dispatchProcessing, preRouteByProcessing);
        dispatcher.registerWithRoute(.processing_stop, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_cancel, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_status, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWalk(.processing_list, dispatchProcessing, localScanJobs);
        dispatcher.registerWithRoute(.processing_savepoint, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_restore, dispatchProcessing, run_id_mod.preRouteByRunId);
        dispatcher.registerWithRoute(.processing_rescale, dispatchProcessing, run_id_mod.preRouteByRunId);
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
            shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
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

        // Generate a unique job ID with embedded partition bits
        const partition_id = shard.router.keyToPartitionNs(def.namespace, def.name);
        var id_buf: [32]u8 = undefined;
        const job_id = shard.run_id_gen.next(.job, partition_id, &id_buf) catch {
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
        // Heap-allocated so classify operators can reference it during creation
        const tag_registry_ptr = self.allocator.create(definition.TagRegistry) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "tag registry alloc failed");
            return;
        };
        tag_registry_ptr.* = definition.TagRegistry{};
        var tag_registry = tag_registry_ptr;
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
                        // Also register default_tag so it can be resolved
                        if (std.mem.eql(u8, entry.key, "default_tag")) {
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
                    self.createPipeline(owned_id, src, def.sinks.items, &def, tag_registry);
                } else {
                    // Multi-source: create additional pipeline with suffixed key
                    var key_buf: [256]u8 = undefined;
                    const ms_key = std.fmt.bufPrint(&key_buf, "{s}\x00{d}", .{ owned_id, idx }) catch continue;
                    const ms_owned = self.allocator.dupe(u8, ms_key) catch continue;
                    self.createPipeline(ms_owned, src, def.sinks.items, &def, tag_registry);
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
            // Binary wire format:
            // [job_id_len:u16][job_id][name_len:u16][name][status:u8]
            // [parallelism:u32][batch_size:u32][records_processed:u64][created_at:i64]
            var buf: [4096]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            const w = fbs.writer();
            w.writeInt(u16, @intCast(job.job_id_owned.len), .little) catch return;
            w.writeAll(job.job_id_owned) catch return;
            w.writeInt(u16, @intCast(job.name_owned.len), .little) catch return;
            w.writeAll(job.name_owned) catch return;
            w.writeByte(@intFromEnum(job.status)) catch return;
            w.writeInt(u32, job.parallelism, .little) catch return;
            w.writeInt(u32, job.batch_size, .little) catch return;
            w.writeInt(u64, job.records_processed, .little) catch return;
            w.writeInt(i64, job.created_at_ms, .little) catch return;
            shard.sendOkResponse(conn, req.header.request_id, fbs.getWritten());
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

        // Resolve effective namespace for filtering
        const req_ns = if (req.namespace.len > 0) req.namespace else "default";

        // Collect matching jobs
        const JobInfo = struct { name: []const u8, job_id: []const u8, status: []const u8, parallelism: u32, created_at: i64 };
        var jobs: std.ArrayListUnmanaged(JobInfo) = .{};
        defer jobs.deinit(self.allocator);

        var jit = self.jobs.iterator();
        while (jit.next()) |entry| {
            if (jobs.items.len >= limit) break;
            const job = entry.value_ptr;
            if (!std.mem.eql(u8, job.namespace_owned, req_ns)) continue;
            jobs.append(self.allocator, .{
                .name = job.name_owned,
                .job_id = job.job_id_owned,
                .status = job.status.toString(),
                .parallelism = job.parallelism,
                .created_at = job.created_at_ms,
            }) catch {
                shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
                return;
            };
        }

        // Serialize to binary wire format:
        // [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
        //  [status_len:u16][status][parallelism:u32][created_at:i64])*
        // [has_more:u8][cursor_len:u16]
        var total: usize = 4; // count
        for (jobs.items) |j| {
            total += 2 + j.name.len + 2 + j.job_id.len + 2 + j.status.len + 4 + 8;
        }
        total += 1 + 2; // trailer

        const buf = self.allocator.alloc(u8, total) catch {
            shard.sendErrorResponse(conn, req.header.request_id, .internal_error, "allocation failed");
            return;
        };
        defer self.allocator.free(buf);

        std.mem.writeInt(u32, buf[0..4], @intCast(jobs.items.len), .little);
        var pos: usize = 4;
        for (jobs.items) |j| {
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.name.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..j.name.len], j.name);
            pos += j.name.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.job_id.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..j.job_id.len], j.job_id);
            pos += j.job_id.len;
            std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.status.len), .little);
            pos += 2;
            @memcpy(buf[pos..][0..j.status.len], j.status);
            pos += j.status.len;
            std.mem.writeInt(u32, buf[pos..][0..4], j.parallelism, .little);
            pos += 4;
            std.mem.writeInt(i64, buf[pos..][0..8], j.created_at, .little);
            pos += 8;
        }
        buf[pos] = 0; // has_more
        pos += 1;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little); // cursor_len

        shard.sendOkResponse(conn, req.header.request_id, buf);
    }

    // ── Savepoint ────────────────────────────────────────────────────────

    fn handleSavepoint(self: *ProcessingHandler, shard: *Shard, conn: *Connection, req: Request) void {
        const job_id = req.key;
        if (job_id.len == 0) {
            shard.sendErrorResponse(conn, req.header.request_id, .bad_request, "job_id is required");
            return;
        }

        if (self.jobs.get(job_id)) |job| {
            // Generate savepoint ID with partition from parent job
            const partition_id = run_id_mod.extractPartition(job_id) orelse 0;
            var id_buf: [32]u8 = undefined;
            const sp_id = shard.run_id_gen.next(.savepoint, partition_id, &id_buf) catch {
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
    fn createPipeline(self: *ProcessingHandler, job_id: []const u8, src: *const definition.SourceSpec, sinks: []const definition.SinkSpec, def: *const definition.JobDefinition, tag_registry: *definition.TagRegistry) void {
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
            var ps = makePipeState(self.allocator, src, sinks, tag_hash);
            self.pipelines.put(job_id, ps) catch {
                self.freePipelineState(&ps);
                return;
            };
            return;
        };
        var backings = self.allocator.alloc(native_registry.CreateResult, op_specs.len) catch {
            self.allocator.free(ops);
            var ps = makePipeState(self.allocator, src, sinks, tag_hash);
            self.pipelines.put(job_id, ps) catch {
                self.freePipelineState(&ps);
                return;
            };
            return;
        };

        var count: usize = 0;
        log.info("createPipeline: {d} operator specs to process", .{op_specs.len});
        for (op_specs) |*spec| {
            log.info("createPipeline: spec type='{s}' name='{s}' has_config={}", .{ spec.type_name, spec.name, spec.config != null });
            if (spec.config) |cfg| {
                for (cfg) |entry| {
                    log.info("createPipeline:   config: {s} = {s}", .{ entry.key, entry.value });
                }
            }
            if (!native_registry.isNativeType(spec.type_name)) {
                log.info("createPipeline: not native type '{s}', skipping", .{spec.type_name});
                continue;
            }
            const result = native_registry.create(self.allocator, spec, tag_registry) catch |err| {
                log.err("createPipeline: native_registry.create failed for '{s}' type='{s}': {}", .{ spec.name, spec.type_name, err });
                continue;
            };
            // Wire kv_lookup operators to route across all shards' KV projections
            if (result.backing == .kv_lookup) {
                result.backing.kv_lookup.setLookupFn(shardKvLookup, @ptrCast(self));
            }
            backings[count] = result;
            ops[count] = result.op;
            count += 1;
        }

        var pipe_state = makePipeState(self.allocator, src, sinks, tag_hash);
        pipe_state.tag_registry = tag_registry;
        log.info("createPipeline: {d} operators loaded (out of {d} specs)", .{ count, op_specs.len });
        if (count > 0) {
            pipe_state.operators = ops[0..count];
            pipe_state.operator_backings = backings[0..count];
        } else {
            self.allocator.free(ops);
            self.allocator.free(backings);
        }

        // Initialize external source for Kafka pipelines
        if (src.kind == .kafka) {
            self.initKafkaSource(&pipe_state, src);
        }

        self.pipelines.put(job_id, pipe_state) catch {
            self.freePipelineState(&pipe_state);
        };
    }

    /// KV lookup callback passed to KvLookupOperator.
    /// Routes the lookup to the correct shard's KV projection via cross-shard peer handlers.
    fn shardKvLookup(ctx: *anyopaque, namespace: []const u8, key: []const u8) ?[]const u8 {
        const self: *ProcessingHandler = @ptrCast(@alignCast(ctx));
        const ns_handler = @import("../namespace/handler.zig");
        var qbuf: [ns_handler.MAX_QUALIFIED_KEY]u8 = undefined;
        const qkey = ns_handler.qualifyKey(&qbuf, namespace, key) catch return null;

        log.info("shardKvLookup: ns='{s}' key='{s}' qkey='{s}' has_peers={} pc={d} sc={d}", .{
            namespace,                     key,                       qkey,
            self.peer_kv_handlers != null, self.peer_partition_count, self.peer_shard_count,
        });

        // Route to the correct shard using the same hash as the KV dispatcher
        if (self.peer_kv_handlers) |handlers| {
            if (self.peer_partition_count > 0 and self.peer_shard_count > 0) {
                const hash = router.hashKeyWithNamespace(namespace, key);
                const partition_id: u32 = @intCast(hash % self.peer_partition_count);
                const shard_id: u16 = @intCast(partition_id % self.peer_shard_count);
                log.info("shardKvLookup: hash={d} part={d} shard={d} nhandlers={d}", .{
                    hash, partition_id, shard_id, handlers.len,
                });
                if (shard_id < handlers.len) {
                    const entry = handlers[shard_id].*.kv.get(qkey) orelse {
                        log.info("shardKvLookup: NOT FOUND on shard {d}", .{shard_id});
                        return null;
                    };
                    log.info("shardKvLookup: FOUND val_len={d}", .{entry.value.len});
                    return entry.value;
                }
            }
        }

        log.info("shardKvLookup: NO PEERS, returning null", .{});
        return null;
    }

    /// Create and attach a KafkaSource to a pipeline state.
    fn initKafkaSource(self: *ProcessingHandler, pipe: *PipelineState, src: *const definition.SourceSpec) void {
        const deser_format: @import("../kafka/deser.zig").Format = switch (src.kafka_format) {
            .raw => .raw,
            .json => .json,
            .string => .string,
            .avro => .avro,
            .protobuf => .protobuf,
        };
        const start_off: KafkaStartOffset = switch (src.kafka_start_offset) {
            .latest => .latest,
            .earliest => .earliest,
            .timestamp => .timestamp,
            .committed => .committed,
        };
        const ks = KafkaSource.init(self.allocator, .{
            .brokers = src.kafka_brokers,
            .topic = src.kafka_topic,
            .group_id = src.kafka_group,
            .format = deser_format,
            .start_offset = start_off,
            .fetch_max_wait_ms = @intCast(src.kafka_fetch_max_wait_ms),
            .fetch_min_bytes = @intCast(src.kafka_fetch_min_bytes),
            .fetch_max_bytes = @intCast(src.kafka_fetch_max_bytes),
            .partition_max_bytes = @intCast(src.kafka_partition_max_bytes),
            .max_poll_records = src.kafka_max_poll_records,
            .metadata_refresh_ms = @intCast(src.kafka_metadata_refresh_ms),
            .isolation_level = @intCast(src.kafka_isolation_level),
            .security_mechanism = if (src.kafka_sasl_mechanism.len > 0) src.kafka_sasl_mechanism else null,
            .sasl_username = if (src.kafka_sasl_username.len > 0) src.kafka_sasl_username else null,
            .sasl_password = if (src.kafka_sasl_password.len > 0) src.kafka_sasl_password else null,
        }) catch |err| {
            log.err("Failed to create KafkaSource: {}", .{err});
            return;
        };
        pipe.external_source = ks.source();
        pipe.external_source_handle = ks;
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
            .stream_cursor_ts = 0,
            .stream_cursor_seq = 0,
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
                .stream => self.tickStreamSource(pipe, shard, job),
                .kafka => self.tickExternalSource(pipe, shard, job),
            }
        }
    }

    /// Tick an external (Kafka) source pipeline: poll for records, apply operators, write to sinks.
    fn tickExternalSource(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        const src = pipe.external_source orelse return;

        var batch_count: u32 = 0;
        const max_batch: u32 = 500; // cap per tick to avoid starvation

        while (batch_count < max_batch) {
            const element = src.poll() catch |err| {
                log.err("Kafka source poll error: {}", .{err});
                break;
            };
            const el = element orelse break;

            switch (el) {
                .record => |rec| {
                    const output = applyOperatorChain(pipe.operators, rec.value, rec.event_time_ms, shard.allocator) catch null;
                    if (output) |records| {
                        defer shard.allocator.free(records);
                        for (records) |out_rec| {
                            self.writeSinkRecordFromStream(pipe, shard, out_rec.value, out_rec.tags);
                        }
                    } else {
                        self.writeSinkRecordFromStream(pipe, shard, rec.value, 0);
                    }
                    job.records_processed += 1;
                    batch_count += 1;
                },
                .watermark => {},
                .barrier => {},
                .end_of_stream => break,
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
    fn tickStreamSource(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, job: *JobRecord) void {
        log.info("TICK_ENTRY: src={s} ns={s} cursor_ts={d} cursor_seq={d}", .{ pipe.src_stream, pipe.src_namespace, pipe.stream_cursor_ts, pipe.stream_cursor_seq });
        // Build cursor from last-read StreamID
        const cursor = StreamID{ .timestamp_ms = pipe.stream_cursor_ts, .sequence = pipe.stream_cursor_seq };

        // Resolve the stream handler that owns the source stream (may be on a different shard)
        const src_handler = self.resolveStreamHandler(shard.stream_handler, pipe.src_stream, pipe.src_namespace);

        // Read from the named source stream using namespace-qualified name-hash filtering
        const result = src_handler.readPayloadsForStream(pipe.src_stream, pipe.src_namespace, cursor, 100);
        log.info("TICK: read {d} payloads, last_id ts={d} seq={d}", .{ result.payloads.len, result.last_id.timestamp_ms, result.last_id.sequence });
        if (result.payloads.len == 0) return;
        defer src_handler.allocator.free(result.payloads);

        for (result.payloads) |payload| {
            log.debug("tickStreamSource: payload len={d} first100='{s}'", .{ payload.len, if (payload.len > 100) payload[0..100] else payload });
            const output_records = applyOperatorChain(pipe.operators, payload, std.time.milliTimestamp(), shard.allocator) catch null;

            if (output_records) |records| {
                defer shard.allocator.free(records);
                log.info("TICK: chain returned {d} records for job", .{records.len});
                for (records) |rec| {
                    log.info("TICK: value_len={d} first50='{s}'", .{ rec.value.len, if (rec.value.len > 50) rec.value[0..50] else rec.value });
                    self.writeSinkRecordFromStream(pipe, shard, rec.value, rec.tags);
                }
            } else {
                log.debug("tickStreamSource: no operators, passthrough", .{});
                self.writeSinkRecordFromStream(pipe, shard, payload, 0);
            }

            job.records_processed += 1;
        }

        // Advance cursor to last StreamID read
        pipe.stream_cursor_ts = result.last_id.timestamp_ms;
        pipe.stream_cursor_seq = result.last_id.sequence;
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
        log.info("applyOperatorChain: calling operator[0] name='{s}'", .{operators[0].getName()});
        try operators[0].processElement(input_rec, &ctx);
        log.info("applyOperatorChain: operator[0] done, collector count={d}", .{collector.count()});

        // Chain: each operator processes the output of the previous one
        var i: usize = 1;
        while (i < operators.len) : (i += 1) {
            // Snapshot current output before clearing
            const prev_count = collector.count();
            const prev_output = collector.drain();
            // Dupe the records since clear will allow the backing to be reused
            const staged = allocator.dupe(ProcessingRecord, prev_output[0..prev_count]) catch return null;
            defer allocator.free(staged);
            // Transfer ownership out of collector BEFORE clear frees owned memory
            for (collector.records.items) |*rec| {
                rec.owns_memory = false;
            }
            collector.clear();
            log.info("applyOperatorChain: calling operator[{d}] name='{s}' with {d} records", .{ i, operators[i].getName(), staged.len });
            for (staged) |rec| {
                try operators[i].processElement(rec, &ctx);
            }
            log.info("applyOperatorChain: operator[{d}] done, collector count={d}", .{ i, collector.count() });
        }

        // Transfer ownership out of collector BEFORE deinit frees owned memory
        for (collector.records.items) |*rec| {
            rec.owns_memory = false;
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
    fn writeSinkRecordFromStream(self: *ProcessingHandler, pipe: *PipelineState, shard: *Shard, payload: []const u8, record_tags: u32) void {
        for (pipe.sinks) |snk| {
            if (snk.required_tags != 0 and (record_tags & snk.required_tags) != snk.required_tags) continue;
            switch (snk.kind) {
                .stream => {
                    const sink_handler = self.resolveStreamHandler(shard.stream_handler, snk.target, snk.namespace);
                    _ = sink_handler.appendPayloadToStream(snk.target, snk.namespace, payload) catch continue;
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
